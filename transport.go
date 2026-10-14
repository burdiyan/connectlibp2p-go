// Package connectlibp2p carries Connect-Go RPCs over authenticated libp2p
// streams, without HTTP, a second multiplexer, or an RPC connection cache.
// It implements all four RPC shapes with protobuf messages and identity
// compression. Its versioned wire protocol is not the Connect HTTP protocol.
// See README.md for the wire contract, limits, and cancellation caveat.
package connectlibp2p

//go:generate mise run generate

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/textproto"
	"strings"
	"sync"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/burdiyan/connectlibp2p-go/internal/wire"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	maxMessage  = 8 << 20
	maxControl  = 64 << 10
	serviceName = "connectlibp2p"
)

const (
	frameOpen = byte(iota)
	frameMessage
	frameHalfClose
	frameHeaders
	frameEnd
)

// ServerOptions controls admission and resource lifetimes. Zero fields use the
// documented defaults. Negative values panic at setup; limits cannot be disabled.
type ServerOptions struct {
	// MaxConcurrentCalls defaults to 64, including admission and finalization.
	MaxConcurrentCalls int
	// MaxConcurrentCallsPerPeer defaults to 16.
	MaxConcurrentCallsPerPeer int
	// OpenTimeout defaults to 5 seconds, bounding initial request admission.
	OpenTimeout time.Duration
	// MaxCallDuration defaults to 5 minutes. A shorter client deadline wins.
	// Long-lived streaming services should explicitly increase this limit.
	MaxCallDuration time.Duration
	// FinishTimeout defaults to 5 seconds, bounding terminal writes and drain.
	FinishTimeout time.Duration
}

type frame struct {
	kind  byte
	data  []byte
	err   error
	scope network.ResourceScope
}

func (f frame) release() {
	if f.scope != nil {
		f.scope.ReleaseMemory(len(f.data))
	}
}

// callScope holds a conservative peak reservation until the stream closes.
// Both directions share it, so overlapping live buffers count together without
// producing resource-manager trace events for every small streaming message.
type callScope struct {
	network.ResourceScope
	mu       sync.Mutex
	used     int
	reserved int
}

// ReserveMemory admits overlapping buffers against the call's peak budget.
func (s *callScope) ReserveMemory(size int, priority uint8) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	used := s.used + size
	if used > s.reserved {
		if err := s.ResourceScope.ReserveMemory(used-s.reserved, priority); err != nil {
			return err
		}
		s.reserved = used
	}
	s.used = used
	return nil
}

// ReleaseMemory records buffer release; libp2p releases the peak at stream teardown.
func (s *callScope) ReleaseMemory(size int) {
	s.mu.Lock()
	s.used -= size
	s.mu.Unlock()
}

type transport struct {
	host     host.Host
	peer     peer.ID
	protocol protocol.ID
}

// NewTransport returns a concurrency-safe Connect transport for a remote peer.
// Each RPC lazily opens one stream; libp2p reuses the underlying secure connection.
// The application owns id; both endpoints must agree on its framing contract.
// An empty ID panics at setup.
func NewTransport(h host.Host, remote peer.ID, id protocol.ID) connect.Transport {
	if id == "" {
		panic("connectlibp2p: protocol ID must not be empty")
	}
	return &transport{host: h, peer: remote, protocol: id}
}

// NewClientStream preserves interceptor metadata until the first operation.
func (t *transport) NewClientStream(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
	info, ok := connect.CallInfoForClientContext(ctx)
	if !ok {
		ctx, info = connect.NewClientContext(ctx)
	}
	info.Spec = spec
	info.PeerAddr = t.peer.String()
	info.Protocol = serviceName
	info.Codec = connect.CodecNameProto
	info.RequestEncoding = connect.CompressionNameIdentity
	info.ResponseEncoding = connect.CompressionNameIdentity
	info.TransportInfo = t.peer
	ctx, cancel := context.WithCancel(ctx)
	return &clientStream{ctx: ctx, cancel: cancel, transport: t, info: info}, nil
}

// Serve exposes a dispatcher whose registration must be complete before setup.
// At most one ServerOptions value may be supplied. The idempotent stop function
// removes the handler, cancels active calls, and joins handlers and reader pumps.
// Handlers must honor their contexts, as with other RPC transports. Admission
// overload resets the stream; application errors are delivered as RPC status.
// The application owns id and must not share its registration with another
// dispatcher. An empty ID panics at setup. Admission limits apply per Serve.
func Serve(ctx context.Context, h host.Host, id protocol.ID, server *connect.Server, options ...ServerOptions) (stop func()) {
	if id == "" {
		panic("connectlibp2p: protocol ID must not be empty")
	}
	o := ServerOptions{64, 16, 5 * time.Second, 5 * time.Minute, 5 * time.Second}
	if len(options) > 1 {
		panic("connectlibp2p: expected at most one ServerOptions")
	}
	if len(options) == 1 {
		v := options[0]
		if v.MaxConcurrentCalls < 0 || v.MaxConcurrentCallsPerPeer < 0 || v.OpenTimeout < 0 || v.MaxCallDuration < 0 || v.FinishTimeout < 0 {
			panic("connectlibp2p: limits must be nonnegative")
		}
		if v.MaxConcurrentCalls != 0 {
			o.MaxConcurrentCalls = v.MaxConcurrentCalls
		}
		if v.MaxConcurrentCallsPerPeer != 0 {
			o.MaxConcurrentCallsPerPeer = v.MaxConcurrentCallsPerPeer
		}
		if v.OpenTimeout != 0 {
			o.OpenTimeout = v.OpenTimeout
		}
		if v.MaxCallDuration != 0 {
			o.MaxCallDuration = v.MaxCallDuration
		}
		if v.FinishTimeout != 0 {
			o.FinishTimeout = v.FinishTimeout
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	var mu sync.Mutex
	var stopped bool
	var active int
	peers := make(map[peer.ID]int)
	var wg sync.WaitGroup
	specs := make(map[string]connect.Spec)
	for spec := range server.Specs() {
		specs[spec.Procedure] = spec
	}
	h.SetStreamHandler(id, func(raw network.Stream) {
		remote := raw.Conn().RemotePeer()
		mu.Lock()
		if stopped || ctx.Err() != nil || active >= o.MaxConcurrentCalls || peers[remote] >= o.MaxConcurrentCallsPerPeer {
			mu.Unlock()
			_ = raw.Reset()
			return
		}
		active++
		peers[remote]++
		wg.Add(1)
		mu.Unlock()
		defer func() {
			mu.Lock()
			active--
			peers[remote]--
			if peers[remote] == 0 {
				delete(peers, remote)
			}
			mu.Unlock()
			wg.Done()
		}()
		if err := raw.Scope().SetService(serviceName); err != nil {
			_ = raw.Reset()
			return
		}
		serveStream(ctx, raw, server, specs, o)
	})
	return func() {
		mu.Lock()
		if !stopped {
			stopped = true
			h.RemoveStreamHandler(id)
			cancel()
		}
		mu.Unlock()
		wg.Wait()
	}
}

func serveStream(parent context.Context, raw network.Stream, server *connect.Server, specs map[string]connect.Spec, o ServerOptions) {
	lifetime, cancelLifetime := context.WithTimeout(parent, o.MaxCallDuration)
	defer cancelLifetime()
	// Administrative cancellation must always unblock I/O, even if a protocol
	// failure has separately canceled the handler context with a coded cause.
	stopReset := context.AfterFunc(lifetime, func() { _ = raw.Reset() })
	defer stopReset()
	defer func() { _ = raw.Close() }()
	_ = raw.SetReadDeadline(time.Now().Add(o.OpenTimeout))
	scope := &callScope{ResourceScope: raw.Scope()}
	f := readFrame(raw, scope, 1<<frameOpen)
	if f.err != nil {
		f.release()
		_ = raw.Reset()
		return
	}
	var open wire.Open
	err := proto.Unmarshal(f.data, &open)
	f.release()
	if err != nil || open.TimeoutNs < 0 || !strings.HasPrefix(open.Procedure, "/") || len(open.Procedure) > 1024 {
		_ = raw.Reset()
		return
	}
	_ = raw.SetReadDeadline(time.Time{})
	if open.TimeoutNs != 0 {
		var cancel context.CancelFunc
		lifetime, cancel = context.WithTimeout(lifetime, time.Duration(open.TimeoutNs))
		defer cancel()
		stopDeadlineReset := context.AfterFunc(lifetime, func() { _ = raw.Reset() })
		defer stopDeadlineReset()
	}
	ctx, cancelCall := context.WithCancelCause(lifetime)
	defer cancelCall(nil)
	spec, ok := specs[open.Procedure]
	if !ok {
		spec = connect.Spec{Procedure: open.Procedure, StreamType: connect.StreamTypeBidi}
	}
	info := &connect.CallInfo{
		Spec: spec, PeerAddr: raw.Conn().RemotePeer().String(),
		Protocol: serviceName, Codec: connect.CodecNameProto,
		RequestEncoding: connect.CompressionNameIdentity, ResponseEncoding: connect.CompressionNameIdentity,
		TransportInfo: raw.Conn().RemotePeer(),
	}
	s := &serverStream{ctx: ctx, raw: raw, scope: scope, info: info, incoming: make(chan frame)}
	if err := setHeaders(info.RequestHeader(), open.Headers); err != nil {
		_ = raw.SetWriteDeadline(time.Now().Add(o.FinishTimeout))
		_ = s.finish(connect.NewError(connect.CodeInvalidArgument, "invalid request metadata").WithCause(err))
		return
	}
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		var hasMessage bool
		var halfClosed bool
		for {
			allowed := uint8(1<<frameMessage | 1<<frameHalfClose)
			if spec.StreamType&connect.StreamTypeClient == 0 && hasMessage {
				allowed = 1 << frameHalfClose
			}
			if halfClosed {
				allowed = 0
			}
			f := readFrame(raw, scope, allowed)
			if f.err == nil {
				switch f.kind {
				case frameMessage:
					hasMessage = true
				case frameHalfClose:
					halfClosed = true
					if spec.StreamType&connect.StreamTypeClient == 0 && !hasMessage {
						f.err = connect.NewError(connect.CodeInvalidArgument, "expected one request")
					}
				}
			}
			if f.err != nil {
				f.release()
				if errors.Is(f.err, io.EOF) && !halfClosed {
					f.err = connect.NewError(connect.CodeInvalidArgument, "missing request half-close")
				}
				cancelCall(f.err)
				return
			}
			select {
			case s.incoming <- f:
			case <-ctx.Done():
				f.release()
				return
			}
		}
	}()
	writeInterrupted := make(chan struct{})
	stopWriteInterrupt := context.AfterFunc(ctx, func() {
		// Protocol cancellation must also unblock a handler's pending Send.
		// Unlike Reset, a write deadline leaves an unwritten response direction
		// available for a terminal verdict. Partial writes are never resumed.
		_ = raw.SetWriteDeadline(time.Now())
		close(writeInterrupted)
	})
	err = func() (err error) {
		defer func() {
			if recover() != nil {
				err = connect.NewError(connect.CodeInternal, "handler panic")
			}
		}()
		return server.Call(ctx, open.Procedure, info, s)
	}()
	if !stopWriteInterrupt() {
		<-writeInterrupted // join before restoring the finalization deadline.
	}
	// A successful single-request handler must not bypass wire cardinality by
	// ignoring its input. The pump also validates frames after logical EOF.
	if err == nil && spec.StreamType&connect.StreamTypeClient == 0 && !s.recvEnded {
		for !s.recvEnded && err == nil {
			select {
			case f := <-s.incoming:
				s.recvEnded = f.kind == frameHalfClose
				f.release()
			case <-ctx.Done():
				err = contextError(ctx)
			}
		}
	}
	if err == nil && ctx.Err() != nil {
		err = contextError(ctx)
	}
	if err == nil && s.sendErr != nil {
		err = s.sendErr
	}
	if err == nil && spec.StreamType&connect.StreamTypeServer == 0 && s.sent != 1 {
		err = connect.NewError(connect.CodeInternal, "expected one response")
	}
	finishCtx, cancelFinish := context.WithTimeout(lifetime, o.FinishTimeout)
	defer cancelFinish()
	stopFinishReset := context.AfterFunc(finishCtx, func() { _ = raw.Reset() })
	defer stopFinishReset()
	if deadline, ok := finishCtx.Deadline(); ok {
		_ = raw.SetDeadline(deadline)
	}
	if s.finish(err) == nil {
		_ = raw.CloseWrite()
		// Drain early rejection requests so QUIC does not abort generated
		// clients' Send before they can read the terminal verdict. Time and
		// memory are bounded; this is not an unbounded background drain.
	drain:
		for {
			select {
			case f := <-s.incoming:
				f.release()
			case <-readerDone:
				break drain
			case <-finishCtx.Done():
				break drain
			}
		}
	}
	cancelCall(nil)
	// Do not reset a completed reader: on QUIC that can discard the terminal
	// response just queued for a malformed request. Reset only if I/O still
	// needs interrupting after a failed write or an expired drain budget.
	select {
	case <-readerDone:
		return
	default:
		_ = raw.Reset()
	}
	<-readerDone
}

type clientStream struct {
	ctx       context.Context
	cancel    context.CancelFunc
	transport *transport
	info      *connect.CallInfo
	open      sync.Once
	openErr   error
	raw       network.Stream
	scope     *callScope
	stopReset func() bool
	sendEnded bool
	sent      int
	headers   bool
	ended     bool
	endErr    error
}

// SendHeaders opens one stream and writes request metadata once.
func (s *clientStream) SendHeaders() error {
	s.open.Do(func() {
		defer func() {
			if s.openErr != nil {
				s.openErr = s.failure(s.openErr)
				s.cancel()
			}
		}()
		open := &wire.Open{Procedure: s.info.Spec.Procedure, Headers: getHeaders(s.info.RequestHeader())}
		if err := setHeaders(new(connect.Header), open.Headers); err != nil {
			s.openErr = connect.NewError(connect.CodeInvalidArgument, "invalid request metadata").WithCause(err)
			return
		}
		if proto.Size(open) > maxControl {
			s.openErr = connect.NewError(connect.CodeResourceExhausted, "metadata too large")
			return
		}
		s.raw, s.openErr = s.transport.host.NewStream(s.ctx, s.transport.peer, s.transport.protocol)
		if s.openErr != nil {
			return
		}
		s.stopReset = context.AfterFunc(s.ctx, func() { _ = s.raw.Reset() })
		if err := s.raw.Scope().SetService(serviceName); err != nil {
			s.openErr = connect.NewError(connect.CodeResourceExhausted, "stream resource limit").WithCause(err)
			return
		}
		s.scope = &callScope{ResourceScope: s.raw.Scope()}
		// Stream establishment consumes the caller's budget. Recompute only
		// after NewStream, immediately before transmitting the prelude.
		if deadline, ok := s.ctx.Deadline(); ok {
			open.TimeoutNs = int64(time.Until(deadline))
			if open.TimeoutNs <= 0 {
				s.openErr = connect.NewError(connect.CodeDeadlineExceeded, "")
				return
			}
		}
		_, s.openErr = writeProto(s.raw, s.scope, frameOpen, open)
	})
	return s.failure(s.openErr)
}

// Send transmits a bounded protobuf request and records payload byte counts.
func (s *clientStream) Send(msg any) error {
	if err := s.SendHeaders(); err != nil {
		return err
	}
	if s.sendEnded {
		return connect.NewError(connect.CodeFailedPrecondition, "request side closed")
	}
	if s.info.Spec.StreamType&connect.StreamTypeClient == 0 && s.sent != 0 {
		s.cancel()
		return connect.NewError(connect.CodeInvalidArgument, "expected one request")
	}
	m, ok := msg.(proto.Message)
	if !ok {
		s.cancel()
		return connect.NewError(connect.CodeInvalidArgument, "expected protobuf request")
	}
	n, err := writeProto(s.raw, s.scope, frameMessage, m)
	s.info.SendStats = connect.MessageStats{Size: n}
	if err != nil {
		err = s.failure(err)
		s.cancel()
	} else {
		s.sent++
	}
	return err
}

// CloseSend sends logical EOF, preserving physical reset detection on the peer.
func (s *clientStream) CloseSend() error {
	if s.sendEnded {
		return nil
	}
	if err := s.SendHeaders(); err != nil {
		return err
	}
	if s.info.Spec.StreamType&connect.StreamTypeClient == 0 && s.sent != 1 {
		s.cancel()
		return connect.NewError(connect.CodeInvalidArgument, "expected one request")
	}
	s.sendEnded = true
	err := writeHalfClose(s.raw)
	if err != nil {
		err = s.failure(err)
		s.cancel()
	}
	return err
}

// Receive verifies terminal status before completing single-response calls.
// Streaming calls expose trailers and EOF only after the terminal frame.
func (s *clientStream) Receive(msg any) error {
	if err := s.SendHeaders(); err != nil {
		return err
	}
	if s.ended {
		return s.endErr
	}
	m, ok := msg.(proto.Message)
	if !ok {
		s.cancel()
		return connect.NewError(connect.CodeInvalidArgument, "expected protobuf response")
	}
	single := s.info.Spec.StreamType&connect.StreamTypeServer == 0
	gotMessage := false
	for {
		allowed := uint8(1 << frameHeaders)
		if s.headers {
			allowed = 1 << frameEnd
			if !gotMessage {
				allowed |= 1 << frameMessage
			}
		}
		f := readFrame(s.raw, s.scope, allowed)
		if f.err != nil {
			err := s.failure(f.err)
			if connect.CodeOf(err) == connect.CodeInvalidArgument {
				err = connect.NewError(connect.CodeDataLoss, "unexpected response frame").WithCause(err)
			}
			if connect.CodeOf(err) == connect.CodeUnavailable && (errors.Is(f.err, io.EOF) || errors.Is(f.err, io.ErrUnexpectedEOF)) {
				err = connect.NewError(connect.CodeDataLoss, "missing terminal status")
			}
			_ = s.Close()
			return err
		}
		var err error
		switch f.kind {
		case frameHeaders:
			var headers wire.Metadata
			if s.headers || proto.Unmarshal(f.data, &headers) != nil || setHeaders(s.info.ResponseHeader(), &headers) != nil {
				err = connect.NewError(connect.CodeDataLoss, "invalid response headers")
			} else {
				s.headers = true
			}
		case frameMessage:
			if !s.headers || gotMessage {
				err = connect.NewError(connect.CodeDataLoss, "unexpected response message")
			} else if decodeErr := proto.Unmarshal(f.data, m); decodeErr != nil {
				err = connect.NewError(connect.CodeDataLoss, "invalid response protobuf").WithCause(decodeErr)
			} else {
				s.info.ReceiveStats = connect.MessageStats{Size: len(f.data)}
				f.release()
				if !single {
					return nil
				}
				gotMessage = true
				continue
			}
		case frameEnd:
			var end wire.End
			if proto.Unmarshal(f.data, &end) != nil || !s.headers || end.Status == nil || end.Status.Code < 0 || end.Status.Code > int32(connect.CodeUnauthenticated) || (end.Status.Code == 0 && (end.Status.Message != "" || len(end.Status.Details) != 0)) || setHeaders(s.info.ResponseTrailer(), end.Trailers) != nil {
				err = connect.NewError(connect.CodeDataLoss, "invalid terminal status")
				break
			}
			s.ended, s.endErr = true, io.EOF
			if end.Status.Code != 0 {
				rpcErr := connect.NewError(connect.Code(end.Status.Code), end.Status.Message).WithRemote()
				for _, detail := range end.Status.Details {
					decoded, _ := connectproto.NewErrorDetail(detail)
					rpcErr = rpcErr.WithDetail(decoded)
				}
				s.endErr = rpcErr
			} else if single && !gotMessage {
				s.endErr = connect.NewError(connect.CodeDataLoss, "missing unary response")
			}
			f.release()
			s.stopReset()
			_ = s.raw.Close()
			s.cancel()
			if single && errors.Is(s.endErr, io.EOF) {
				return nil
			}
			return s.endErr
		default:
			err = connect.NewError(connect.CodeDataLoss, "unexpected response frame")
		}
		f.release()
		if err != nil {
			_ = s.Close()
			return err
		}
	}
}

// Close is idempotent and unblocks concurrent send/receive operations.
func (s *clientStream) Close() error {
	s.cancel()
	return nil
}

func (s *clientStream) failure(err error) error {
	if err == nil {
		return nil
	}
	if rpcErr, ok := errors.AsType[*connect.Error](err); ok && rpcErr != nil {
		return err
	}
	if s.ctx.Err() != nil {
		return contextError(s.ctx)
	}
	if deadline, ok := s.ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return connect.NewError(connect.CodeDeadlineExceeded, "").WithCause(context.DeadlineExceeded)
	}
	return connect.NewError(connect.CodeUnavailable, "").WithCause(err)
}

type serverStream struct {
	ctx       context.Context
	raw       network.Stream
	scope     *callScope
	info      *connect.CallInfo
	incoming  chan frame
	headers   bool
	recvEnded bool
	sent      int
	sendErr   error
}

// Receive decodes one request, enforcing single-request half-close semantics.
func (s *serverStream) Receive(msg any) error {
	if s.recvEnded {
		return io.EOF
	}
	m, ok := msg.(proto.Message)
	if !ok {
		return connect.NewError(connect.CodeInternal, "expected protobuf request")
	}
	select {
	case <-s.ctx.Done():
		return contextError(s.ctx)
	case f := <-s.incoming:
		defer f.release()
		if f.kind == frameHalfClose {
			s.recvEnded = true
			return io.EOF
		}
		if err := proto.Unmarshal(f.data, m); err != nil {
			return connect.NewError(connect.CodeInvalidArgument, "invalid request protobuf").WithCause(err)
		}
		s.info.ReceiveStats = connect.MessageStats{Size: len(f.data)}
		if s.info.Spec.StreamType&connect.StreamTypeClient == 0 {
			select {
			case <-s.ctx.Done():
				return contextError(s.ctx)
			case end := <-s.incoming:
				end.release()
				s.recvEnded = true // the pump has validated HALF_CLOSE and cardinality.
			}
		}
		return nil
	}
}

// SendHeaders flushes response metadata once, before any response message.
func (s *serverStream) SendHeaders() error {
	if s.headers {
		return nil
	}
	headers := getHeaders(s.info.ResponseHeader())
	if err := setHeaders(new(connect.Header), headers); err != nil {
		return connect.NewError(connect.CodeInternal, "invalid response metadata").WithCause(err)
	}
	if _, err := writeProto(s.raw, s.scope, frameHeaders, headers); err != nil {
		return err
	}
	s.headers = true
	return nil
}

// Send transmits a bounded protobuf response and records payload byte counts.
func (s *serverStream) Send(msg any) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	if s.info.Spec.StreamType&connect.StreamTypeServer == 0 && s.sent != 0 {
		s.sendErr = connect.NewError(connect.CodeInternal, "expected one response")
		return s.sendErr
	}
	if s.sendErr = s.SendHeaders(); s.sendErr != nil {
		return s.sendErr
	}
	m, ok := msg.(proto.Message)
	if !ok {
		s.sendErr = connect.NewError(connect.CodeInternal, "expected protobuf response")
		return s.sendErr
	}
	n, err := writeProto(s.raw, s.scope, frameMessage, m)
	s.info.SendStats = connect.MessageStats{Size: n}
	s.sendErr = err
	if err == nil {
		s.sent++
	}
	return err
}

func (s *serverStream) finish(err error) error {
	if s.sendErr != nil {
		if _, coded := errors.AsType[*connect.Error](s.sendErr); !coded {
			return s.sendErr // a partial message write leaves no safe frame boundary.
		}
	}
	if headerErr := s.SendHeaders(); headerErr != nil {
		if code := connect.CodeOf(headerErr); code != connect.CodeResourceExhausted && code != connect.CodeInternal {
			return headerErr // a partial I/O write must not be retried as headers.
		}
		err = headerErr
		if _, writeErr := writeProto(s.raw, s.scope, frameHeaders, &wire.Metadata{}); writeErr != nil {
			return writeErr
		}
	}
	end := &wire.End{Status: &status.Status{}, Trailers: getHeaders(s.info.ResponseTrailer())}
	if metadataErr := setHeaders(new(connect.Header), end.Trailers); metadataErr != nil {
		err = connect.NewError(connect.CodeInternal, "invalid response metadata").WithCause(metadataErr)
		end.Trailers = nil
	}
	// Match Connect HTTP's scrubHandlerError contract. Only locally authored
	// Connect errors keep their code/message/details, before inspecting causes.
	if err != nil {
		code := connect.CodeUnknown
		if rpcErr, ok := errors.AsType[*connect.Error](err); ok && rpcErr != nil {
			if rpcErr.IsRemote() {
				code = connect.CodeInternal
			} else {
				code = rpcErr.Code()
				end.Status.Message = rpcErr.Message()
				for _, detail := range rpcErr.Details() {
					end.Status.Details = append(end.Status.Details, connectproto.ErrorDetailToAny(detail))
				}
			}
		} else if errors.Is(err, context.Canceled) {
			code = connect.CodeCanceled
		} else if errors.Is(err, context.DeadlineExceeded) {
			code = connect.CodeDeadlineExceeded
		}
		end.Status.Code = int32(code) // #nosec G115 -- bounded Connect status codes.
	}
	_, writeErr := writeProto(s.raw, s.scope, frameEnd, end)
	if connect.CodeOf(writeErr) == connect.CodeResourceExhausted {
		_, writeErr = writeProto(s.raw, s.scope, frameEnd, &wire.End{Status: &status.Status{Code: int32(connect.CodeResourceExhausted)}})
	}
	return writeErr
}

func contextError(ctx context.Context) error {
	if err, ok := errors.AsType[*connect.Error](context.Cause(ctx)); ok && err != nil {
		return err
	}
	code := connect.CodeCanceled
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		code = connect.CodeDeadlineExceeded
	}
	return connect.NewError(code, "").WithCause(ctx.Err())
}

func readFrame(r io.Reader, scope network.ResourceScope, allowed uint8) frame {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return frame{err: err}
	}
	kind := header[0]
	size := binary.BigEndian.Uint32(header[1:])
	if kind > frameEnd || allowed&(1<<kind) == 0 || (kind == frameHalfClose && size != 0) {
		return frame{err: connect.NewError(connect.CodeInvalidArgument, "invalid frame header")}
	}
	limit := uint32(maxControl)
	if kind == frameMessage {
		limit = maxMessage
	}
	if size > limit {
		return frame{err: connect.NewError(connect.CodeResourceExhausted, "frame too large")}
	}
	if err := scope.ReserveMemory(int(size), network.ReservationPriorityAlways); err != nil {
		return frame{err: connect.NewError(connect.CodeResourceExhausted, "frame resource limit").WithCause(err)}
	}
	f := frame{kind: kind, data: make([]byte, size), scope: scope}
	if _, err := io.ReadFull(r, f.data); err != nil {
		f.release()
		return frame{err: err}
	}
	return f
}

func writeHalfClose(w io.Writer) error {
	n, err := w.Write([]byte{frameHalfClose, 0, 0, 0, 0})
	if err == nil && n != 5 {
		err = io.ErrShortWrite
	}
	return err
}

func writeProto(raw network.Stream, scope network.ResourceScope, kind byte, m proto.Message) (int, error) {
	size := proto.Size(m)
	limit := maxControl
	if kind == frameMessage {
		limit = maxMessage
	}
	if size > limit {
		return 0, connect.NewError(connect.CodeResourceExhausted, "frame too large")
	}
	if err := scope.ReserveMemory(size+5, network.ReservationPriorityAlways); err != nil {
		return 0, connect.NewError(connect.CodeResourceExhausted, "frame resource limit").WithCause(err)
	}
	defer scope.ReleaseMemory(size + 5)
	// Marshal directly into the framed buffer: no second payload-sized copy.
	buf := make([]byte, 5, size+5)
	buf[0] = kind
	buf, err := proto.MarshalOptions{}.MarshalAppend(buf, m)
	if err != nil {
		return 0, err
	}
	binary.BigEndian.PutUint32(buf[1:5], uint32(len(buf)-5)) // #nosec G115 -- checked above against 8 MiB/64 KiB.
	n, err := raw.Write(buf)
	if err == nil && n != len(buf) {
		err = io.ErrShortWrite
	}
	return len(buf) - 5, err
}

func getHeaders(h *connect.Header) *wire.Metadata {
	values := &wire.Metadata{Entries: make(map[string]*wire.Metadata_Values, h.Len())}
	for key, value := range h.All() {
		values.Entries[key] = &wire.Metadata_Values{Values: value}
	}
	return values
}

func setHeaders(h *connect.Header, values *wire.Metadata) error {
	seen := make(map[string]bool, len(values.GetEntries()))
	for key, value := range values.GetEntries() {
		if key == "" {
			return errors.New("empty metadata key")
		}
		for _, c := range key {
			valid := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)
			if !valid {
				return errors.New("invalid metadata key")
			}
		}
		canonical := textproto.CanonicalMIMEHeaderKey(key)
		if seen[canonical] {
			return errors.New("duplicate metadata case alias")
		}
		seen[canonical] = true
		for _, v := range value.GetValues() {
			if strings.ContainsAny(v, "\r\n\x00") {
				return errors.New("invalid metadata value")
			}
			if strings.HasSuffix(canonical, "-Bin") {
				for _, part := range strings.Split(v, ",") {
					if _, err := connect.DecodeBinaryHeader(strings.TrimSpace(part)); err != nil {
						return err
					}
				}
			}
		}
		h.SetValues(canonical, value.GetValues())
	}
	return nil
}
