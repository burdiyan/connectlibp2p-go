package connectlibp2p_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect/v2"
	"connectrpc.com/connect/v2/connectproto"
	"github.com/burdiyan/connectlibp2p-go"
	p2p "github.com/burdiyan/connectlibp2p-go/internal/testpb"
	"github.com/burdiyan/connectlibp2p-go/internal/wire"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

const procedure = "/prototype.Transport/Call"
const testProtocol protocol.ID = "/test/connectlibp2p/1.0.0"

func hosts(t *testing.T, kind string, options ...libp2p.Option) (host.Host, host.Host) {
	t.Helper()
	addr := "/ip4/127.0.0.1/tcp/0"
	if kind == "quic" {
		addr = "/ip4/127.0.0.1/udp/0/quic-v1"
	}
	options = append([]libp2p.Option{libp2p.ListenAddrStrings(addr), libp2p.DisableRelay(), libp2p.DisableMetrics()}, options...)
	server, err := libp2p.New(options...)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	client, err := libp2p.New(libp2p.NoListenAddrs, libp2p.DisableRelay(), libp2p.DisableMetrics())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, client.Close()) })
	require.NoError(t, client.Connect(t.Context(), peer.AddrInfo{ID: server.ID(), Addrs: server.Addrs()}))
	return server, client
}

// TestProtocolIDs checks application-owned routing and independent shutdown on
// the same host, including rejection instead of fallback for an unsupported ID.
func TestProtocolIDs(t *testing.T) {
	for _, kind := range []string{"tcp", "quic"} {
		t.Run(kind, func(t *testing.T) {
			serverHost, clientHost := hosts(t, kind)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			spec := connect.Spec{Procedure: procedure}
			var clients []*connect.Client
			var stops []func()
			for _, app := range []struct {
				id    protocol.ID
				label string
			}{{"/example/storage/connectlibp2p/1.0.0", "storage"}, {"/example/chat/connectlibp2p/1.0.0", "chat"}} {
				server := connect.NewServer()
				server.Register(connect.Method{Spec: spec, Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
					if err := stream.Receive(new(p2p.ListSpacesRequest)); err != nil {
						return err
					}
					return stream.Send(&p2p.ListSpacesResponse{NextPageToken: app.label})
				}})
				stop := connectlibp2p.Serve(ctx, serverHost, app.id, server)
				t.Cleanup(stop)
				stops = append(stops, stop)
				client := connect.NewClient(connectlibp2p.NewTransport(clientHost, serverHost.ID(), app.id))
				clients = append(clients, client)
				var response p2p.ListSpacesResponse
				require.NoError(t, client.CallUnary(ctx, spec, &p2p.ListSpacesRequest{}, &response))
				require.Equal(t, app.label, response.NextPageToken)
			}
			// Recheck the first dispatcher after registering the second: same-ID
			// replacement would otherwise go unnoticed by the first round.
			var response p2p.ListSpacesResponse
			require.NoError(t, clients[0].CallUnary(ctx, spec, &p2p.ListSpacesRequest{}, &response))
			require.Equal(t, "storage", response.NextPageToken)
			stops[0]()
			require.NoError(t, clients[1].CallUnary(ctx, spec, &p2p.ListSpacesRequest{}, &response))
			require.Equal(t, "chat", response.NextPageToken)
			err := clients[0].CallUnary(ctx, spec, &p2p.ListSpacesRequest{}, &response)
			require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
			unknown := connect.NewClient(connectlibp2p.NewTransport(clientHost, serverHost.ID(), "/example/missing/connectlibp2p/1.0.0"))
			err = unknown.CallUnary(ctx, spec, &p2p.ListSpacesRequest{}, &response)
			require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
		})
	}
	t.Run("empty", func(t *testing.T) {
		require.PanicsWithValue(t, "connectlibp2p: protocol ID must not be empty", func() {
			connectlibp2p.NewTransport(nil, "", "")
		})
		require.PanicsWithValue(t, "connectlibp2p: protocol ID must not be empty", func() {
			connectlibp2p.Serve(t.Context(), nil, "", nil)
		})
	})
}

// TestMetadataAndErrors checks repeated/binary metadata, transport identity,
// client/server interception, public errors, and status after a unary message.
func TestMetadataAndErrors(t *testing.T) {
	for _, kind := range []string{"tcp", "quic"} {
		t.Run(kind, func(t *testing.T) {
			serverHost, clientHost := hosts(t, kind)
			detail, err := connectproto.NewErrorDetail(&p2p.Blob{Cid: []byte{0, 255, 7}, Cursor: "resume"})
			require.NoError(t, err)
			server := connect.NewServer(func(next connect.ServerFunc) connect.ServerFunc {
				return func(ctx context.Context, spec connect.Spec, stream connect.ServerStream) error {
					info, _ := connect.CallInfoForServerContext(ctx)
					if info.RequestHeader().Get("Authorization") != "allow" {
						return connect.NewError(connect.CodeUnauthenticated, "credentials required")
					}
					if info.TransportInfo != clientHost.ID() || info.PeerAddr != clientHost.ID().String() {
						return connect.NewError(connect.CodeInternal, "wrong transport identity")
					}
					info.ResponseHeader().SetValues("X-Repeated", info.RequestHeader().Values("X-Repeated"))
					info.ResponseHeader().Set("Opaque-Bin", info.RequestHeader().Get("Opaque-Bin"))
					info.ResponseTrailer().Set("X-Finished", "yes")
					return next(ctx, spec, stream)
				}
			})
			spec := connect.Spec{Procedure: procedure}
			server.Register(connect.Method{Spec: spec, Handler: func(ctx context.Context, _ connect.Spec, stream connect.ServerStream) error {
				var req p2p.ListSpacesRequest
				if err := stream.Receive(&req); err != nil {
					return err
				}
				if req.PageSize > 0 {
					return connect.NewError(connect.Code(req.PageSize), "public verdict").WithCause(context.DeadlineExceeded) // #nosec G115 -- test supplies only codes 1..16.
				}
				switch req.PageToken {
				case "private":
					return errors.New("private database path")
				case "remote":
					return connect.NewError(connect.CodeNotFound, "private downstream error").WithDetail(detail).WithRemote()
				case "panic":
					panic("private panic details")
				case "oversized-status":
					return connect.NewError(connect.CodeAborted, strings.Repeat("secret", 12000))
				case "opaque-detail":
					return connect.NewError(connect.CodeAborted, "opaque").WithDetail(&connect.ErrorDetail{Type: "example.Unknown", Value: []byte{0, 255, 7}})
				}
				info, _ := connect.CallInfoForServerContext(ctx)
				if err := stream.Send(&p2p.ListSpacesResponse{Spaces: []string{info.PeerAddr}, NextPageToken: req.PageToken + "-next"}); err != nil {
					return err
				}
				if req.PageToken == "late-error" {
					return connect.NewError(connect.CodeAborted, "retry with cursor").WithDetail(detail)
				}
				return nil
			}})
			stop := connectlibp2p.Serve(t.Context(), serverHost, testProtocol, server)
			t.Cleanup(stop)
			client := connect.NewClient(connectlibp2p.NewTransport(clientHost, serverHost.ID(), testProtocol), func(next connect.ClientFunc) connect.ClientFunc {
				return func(ctx context.Context, spec connect.Spec) (connect.ClientStream, error) {
					stream, err := next(ctx, spec)
					info, _ := connect.CallInfoForClientContext(ctx)
					if info.RequestHeader().Get("Authorization") == "" {
						info.RequestHeader().Set("Authorization", "allow")
					}
					return stream, err
				}
			})
			ctx, info := connect.NewClientContext(t.Context())
			info.RequestHeader().SetValues("X-Repeated", []string{"first", "second"})
			info.RequestHeader().Set("Opaque-Bin", connect.EncodeBinaryHeader([]byte{0, 255, 7}))
			info.RequestHeader().Set("X-Peer", "spoofed")
			var response p2p.ListSpacesResponse
			require.NoError(t, client.CallUnary(ctx, spec, &p2p.ListSpacesRequest{PageToken: "start"}, &response))
			require.Equal(t, []string{clientHost.ID().String()}, response.Spaces)
			require.Equal(t, "start-next", response.NextPageToken)
			require.Equal(t, []string{"first", "second"}, info.ResponseHeader().Values("X-Repeated"))
			binaryValue, err := connect.DecodeBinaryHeader(info.ResponseHeader().Get("Opaque-Bin"))
			require.NoError(t, err)
			require.Equal(t, []byte{0, 255, 7}, binaryValue)
			require.Equal(t, "yes", info.ResponseTrailer().Get("X-Finished"))
			require.Equal(t, connect.MessageStats{Size: 7}, info.SendStats)
			require.Equal(t, connect.MessageStats{Size: proto.Size(&response)}, info.ReceiveStats)
			for code := connect.CodeCanceled; code <= connect.CodeUnauthenticated; code++ {
				err := client.CallUnary(ctx, spec, &p2p.ListSpacesRequest{PageSize: int32(code)}, &response) // #nosec G115 -- codes 1..16.
				require.Equal(t, code, connect.CodeOf(err), "a local Connect code takes precedence over its context cause")
			}
			err = client.CallUnary(ctx, spec, &p2p.ListSpacesRequest{PageToken: "opaque-detail"}, &response) // #nosec G101 -- RPC test selector, not a credential.
			rpcErr, ok := errors.AsType[*connect.Error](err)
			require.True(t, ok)
			require.Len(t, rpcErr.Details(), 1)
			require.True(t, proto.Equal(&anypb.Any{TypeUrl: "type.googleapis.com/example.Unknown", Value: []byte{0, 255, 7}}, connectproto.ErrorDetailToAny(rpcErr.Details()[0])))
			for _, tc := range []struct {
				token string
				code  connect.Code
				text  string
			}{
				{"private", connect.CodeUnknown, ""},
				{"remote", connect.CodeInternal, ""},
				{"panic", connect.CodeInternal, "handler panic"},
				{"late-error", connect.CodeAborted, "retry with cursor"},
				{"oversized-status", connect.CodeResourceExhausted, ""},
			} {
				err := client.CallUnary(ctx, spec, &p2p.ListSpacesRequest{PageToken: tc.token}, &response)
				require.Equal(t, tc.code, connect.CodeOf(err), "%s: %v", tc.token, err)
				rpcErr, ok := errors.AsType[*connect.Error](err)
				require.True(t, ok)
				require.True(t, rpcErr.IsRemote())
				require.Equal(t, tc.text, rpcErr.Message())
				if tc.token == "late-error" {
					require.Len(t, rpcErr.Details(), 1)
					decoded, err := connectproto.UnmarshalErrorDetail(rpcErr.Details()[0])
					require.NoError(t, err)
					require.True(t, proto.Equal(&p2p.Blob{Cid: []byte{0, 255, 7}, Cursor: "resume"}, decoded))
				} else {
					require.Empty(t, rpcErr.Details())
				}
			}
			info.RequestHeader().Set("Authorization", "deny")
			for _, token := range []string{"", strings.Repeat("x", 256<<10)} {
				// Rejection must survive a request still in flight, rather than
				// resetting Send before the client can read the error status.
				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				err = client.CallUnary(ctx, spec, &p2p.ListSpacesRequest{PageToken: token}, &response)
				cancel()
				require.Equal(t, connect.CodeUnauthenticated, connect.CodeOf(err))
			}
			err = client.CallUnary(t.Context(), connect.Spec{Procedure: "/unregistered.Local/Private"}, &p2p.ListSpacesRequest{}, &response)
			require.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
		})
	}
}

// TestStreaming exercises client streaming and true full duplex without HTTP,
// including receive-before-send, half-close, and concurrent independent calls.
func TestStreaming(t *testing.T) {
	for _, kind := range []string{"tcp", "quic"} {
		t.Run(kind, func(t *testing.T) {
			serverHost, clientHost := hosts(t, kind)
			server := connect.NewServer()
			for _, shape := range []connect.StreamType{connect.StreamTypeClient, connect.StreamTypeBidi} {
				spec := connect.Spec{Procedure: fmt.Sprintf("%s/%s", procedure, shape), StreamType: shape}
				server.Register(connect.Method{Spec: spec, Handler: func(_ context.Context, spec connect.Spec, stream connect.ServerStream) error {
					var sum int32
					for {
						var req p2p.ListSpacesRequest
						err := stream.Receive(&req)
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							return err
						}
						sum += req.PageSize
						if spec.StreamType == connect.StreamTypeBidi {
							if err := stream.Send(&p2p.ListSpacesResponse{NextPageToken: req.PageToken + "-reply"}); err != nil {
								return err
							}
						}
					}
					if spec.StreamType == connect.StreamTypeClient {
						return stream.Send(&p2p.ListSpacesResponse{NextPageToken: fmt.Sprint(sum)})
					}
					return nil
				}})
			}
			stop := connectlibp2p.Serve(t.Context(), serverHost, testProtocol, server)
			t.Cleanup(stop)
			client := connect.NewClient(connectlibp2p.NewTransport(clientHost, serverHost.ID(), testProtocol))
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			upload, err := client.CallClientStream(ctx, connect.Spec{Procedure: procedure + "/client", StreamType: connect.StreamTypeClient})
			require.NoError(t, err)
			defer func() { _ = upload.Close() }()
			require.NoError(t, upload.Send(&p2p.ListSpacesRequest{PageSize: 7}))
			require.NoError(t, upload.Send(&p2p.ListSpacesRequest{PageSize: 13}))
			require.NoError(t, upload.CloseSend())
			var response p2p.ListSpacesResponse
			require.NoError(t, upload.Receive(&response))
			require.Equal(t, "20", response.NextPageToken)
			require.ErrorIs(t, upload.Receive(&response), io.EOF)
			var wg sync.WaitGroup
			errs := make(chan error, 12)
			for i := range 12 {
				wg.Go(func() {
					stream, err := client.CallClientStream(ctx, connect.Spec{Procedure: procedure + "/bidi", StreamType: connect.StreamTypeBidi})
					if err != nil {
						errs <- err
						return
					}
					defer func() { _ = stream.Close() }()
					// Receive opens headers before Send: these sides must coexist.
					received := make(chan error, 1)
					var response p2p.ListSpacesResponse
					go func() { received <- stream.Receive(&response) }()
					for _, token := range []string{fmt.Sprintf("%d-first", i), fmt.Sprintf("%d-last", i)} {
						if err := stream.Send(&p2p.ListSpacesRequest{PageToken: token}); err != nil {
							errs <- err
							return
						}
						if received != nil {
							err = <-received
							received = nil
						} else {
							err = stream.Receive(&response)
						}
						if err != nil || response.NextPageToken != token+"-reply" {
							errs <- fmt.Errorf("duplex response %q: %w", response.NextPageToken, err)
							return
						}
					}
					if err := stream.CloseSend(); err != nil {
						errs <- err
						return
					}
					err = stream.Receive(&response)
					if !errors.Is(err, io.EOF) {
						errs <- fmt.Errorf("duplex completion: %w", err)
					}
				})
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				require.NoError(t, err)
			}
		})
	}
}

// TestCancellation checks abandonment after request half-close, deadline
// propagation, blocked Receive interruption, and stopping active handlers.
func TestCancellation(t *testing.T) {
	for _, tc := range []struct{ network, action string }{
		{"tcp", "cancel"}, {"tcp", "close"}, {"tcp", "deadline"}, {"tcp", "shutdown"},
		{"quic", "cancel"}, {"quic", "close"}, {"quic", "deadline"}, {"quic", "shutdown"},
	} {
		t.Run(tc.network+"/"+tc.action, func(t *testing.T) {
			action := tc.action
			serverHost, clientHost := hosts(t, tc.network)
			started := make(chan time.Time, 1)
			finished := make(chan error, 1)
			server := connect.NewServer()
			spec := connect.Spec{Procedure: procedure, StreamType: connect.StreamTypeServer}
			server.Register(connect.Method{Spec: spec, Handler: func(ctx context.Context, _ connect.Spec, stream connect.ServerStream) error {
				var req p2p.ListSpacesRequest
				if err := stream.Receive(&req); err != nil {
					return err
				}
				deadline, _ := ctx.Deadline()
				started <- deadline
				<-ctx.Done()
				finished <- ctx.Err()
				return ctx.Err()
			}})
			stop := connectlibp2p.Serve(t.Context(), serverHost, testProtocol, server)
			t.Cleanup(stop)
			client := connect.NewClient(connectlibp2p.NewTransport(clientHost, serverHost.ID(), testProtocol))
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if action == "deadline" {
				ctx, cancel = context.WithTimeout(ctx, 250*time.Millisecond)
				defer cancel()
			}
			stream, err := client.CallServerStream(ctx, spec, &p2p.ListSpacesRequest{})
			require.NoError(t, err)
			defer func() { _ = stream.Close() }()
			var serverDeadline time.Time
			select {
			case serverDeadline = <-started:
			case <-ctx.Done():
				t.Fatal("handler never started")
			}
			received := make(chan error, 1)
			go func() { received <- stream.Receive(new(p2p.ListSpacesResponse)) }()
			switch action {
			case "cancel":
				cancel()
			case "close":
				require.NoError(t, stream.Close())
			case "deadline":
				clientDeadline, _ := ctx.Deadline()
				require.WithinDuration(t, clientDeadline, serverDeadline, 100*time.Millisecond)
			case "shutdown":
				stop()
			}
			select {
			case err := <-received:
				switch action {
				case "deadline":
					require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
				case "shutdown":
					require.Error(t, err)
				default:
					require.Equal(t, connect.CodeCanceled, connect.CodeOf(err))
				}
			case <-time.After(5 * time.Second):
				t.Fatal("blocked Receive did not finish")
			}
			select {
			case err := <-finished:
				require.Error(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("server context was not canceled after request half-close")
			}
		})
	}
}

func appendWire(t testing.TB, dst []byte, kind byte, message proto.Message) []byte {
	t.Helper()
	body, err := proto.Marshal(message)
	require.NoError(t, err)
	dst = binary.BigEndian.AppendUint32(append(dst, kind), uint32(len(body))) // #nosec G115 -- bounded test fixtures.
	return append(dst, body...)
}

func terminal(t testing.TB, raw network.Stream) *wire.End {
	t.Helper()
	for {
		var header [5]byte
		_, err := io.ReadFull(raw, header[:])
		require.NoError(t, err)
		size := binary.BigEndian.Uint32(header[1:])
		require.LessOrEqual(t, size, uint32(64<<10))
		body := make([]byte, size)
		_, err = io.ReadFull(raw, body)
		require.NoError(t, err)
		if header[0] == 4 {
			var end wire.End
			require.NoError(t, proto.Unmarshal(body, &end))
			require.NotNil(t, end.Status)
			return &end
		}
	}
}

// TestRequestContract uses a real malicious peer, bypassing local client checks.
// Empty protobuf messages count, and extra frames after EOF must cancel a
// handler even when it never calls Receive again.
func TestRequestContract(t *testing.T) {
	for _, kind := range []string{"tcp", "quic"} {
		for _, name := range []string{"empty-message", "zero-messages", "two-messages", "after-half-close", "duplicate-open", "bad-half-close", "unknown-frame", "case-aliases", "invalid-binary"} {
			t.Run(kind+"/"+name, func(t *testing.T) {
				serverHost, clientHost := hosts(t, kind)
				server := connect.NewServer()
				server.Register(connect.Method{Spec: connect.Spec{Procedure: procedure}, Handler: func(ctx context.Context, _ connect.Spec, stream connect.ServerStream) error {
					if err := stream.Receive(new(p2p.Blob)); err != nil {
						return err
					}
					if name == "after-half-close" {
						<-ctx.Done()
						return nil
					}
					return stream.Send(&p2p.Blob{})
				}})
				t.Cleanup(connectlibp2p.Serve(t.Context(), serverHost, testProtocol, server))
				open := &wire.Open{Procedure: procedure}
				if name == "case-aliases" {
					open.Headers = &wire.Metadata{Entries: map[string]*wire.Metadata_Values{"X-Key": {Values: []string{"first"}}, "x-key": {Values: []string{"other"}}}}
				}
				if name == "invalid-binary" {
					open.Headers = &wire.Metadata{Entries: map[string]*wire.Metadata_Values{"Opaque-Bin": {Values: []string{"!"}}}}
				}
				buf := appendWire(t, nil, 0, open)
				if name != "zero-messages" {
					buf = appendWire(t, buf, 1, &p2p.Blob{})
				}
				if name == "two-messages" {
					buf = appendWire(t, buf, 1, &p2p.Blob{})
				}
				if name == "duplicate-open" {
					buf = appendWire(t, buf, 0, open)
				}
				switch name {
				case "bad-half-close":
					buf = append(buf, 2, 0, 0, 0, 1, 0)
				case "unknown-frame":
					buf = append(buf, 255, 0, 0, 0, 0)
				default:
					buf = append(buf, 2, 0, 0, 0, 0)
				}
				if name == "after-half-close" {
					buf = appendWire(t, buf, 1, &p2p.Blob{})
				}
				raw, err := clientHost.NewStream(t.Context(), serverHost.ID(), testProtocol)
				require.NoError(t, err)
				defer func() { _ = raw.Close() }()
				require.NoError(t, raw.SetDeadline(time.Now().Add(5*time.Second)))
				_, err = raw.Write(buf)
				require.NoError(t, err)
				end := terminal(t, raw)
				want := connect.CodeInvalidArgument
				if name == "empty-message" {
					want = 0
				}
				require.EqualValues(t, want, end.Status.Code)
			})
		}
	}
}

// TestResponseCardinality checks both too few and too many single responses,
// and distinguishes legitimate empty server/client streams from missing unary
// messages. An ignored Send error must not turn a bad handler into success.
func TestResponseCardinality(t *testing.T) {
	serverHost, clientHost := hosts(t, "tcp")
	server := connect.NewServer()
	for _, shape := range []connect.StreamType{0, connect.StreamTypeClient, connect.StreamTypeServer, connect.StreamTypeBidi} {
		for _, count := range []int{0, 1, 2} {
			spec := connect.Spec{Procedure: fmt.Sprintf("%s/%s/%d", procedure, shape, count), StreamType: shape}
			server.Register(connect.Method{Spec: spec, Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
				for {
					err := stream.Receive(new(p2p.Blob))
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						return err
					}
				}
				for range count {
					_ = stream.Send(&p2p.Blob{})
				}
				return nil
			}})
		}
	}
	t.Cleanup(connectlibp2p.Serve(t.Context(), serverHost, testProtocol, server))
	client := connect.NewClient(connectlibp2p.NewTransport(clientHost, serverHost.ID(), testProtocol))
	for _, shape := range []connect.StreamType{0, connect.StreamTypeClient, connect.StreamTypeServer, connect.StreamTypeBidi} {
		for _, count := range []int{0, 1, 2} {
			spec := connect.Spec{Procedure: fmt.Sprintf("%s/%s/%d", procedure, shape, count), StreamType: shape}
			stream, err := client.CallClientStream(t.Context(), spec)
			require.NoError(t, err)
			if shape&connect.StreamTypeClient == 0 {
				require.NoError(t, stream.Send(&p2p.Blob{}))
			}
			require.NoError(t, stream.CloseSend())
			require.NoError(t, stream.CloseSend())
			if shape&connect.StreamTypeServer == 0 {
				err = stream.Receive(new(p2p.Blob))
				if count == 1 {
					require.NoError(t, err)
					require.ErrorIs(t, stream.Receive(new(p2p.Blob)), io.EOF)
				} else {
					require.Equal(t, connect.CodeInternal, connect.CodeOf(err))
				}
			} else {
				for range count {
					require.NoError(t, stream.Receive(new(p2p.Blob)))
				}
				require.ErrorIs(t, stream.Receive(new(p2p.Blob)), io.EOF)
			}
			require.NoError(t, stream.Close())
		}
	}
}

type joinedHost struct {
	host.Host
	done chan struct{}
}

// SetStreamHandler observes when the real transport has finished cleanup.
func (h joinedHost) SetStreamHandler(id protocol.ID, handler network.StreamHandler) {
	h.Host.SetStreamHandler(id, func(raw network.Stream) { handler(raw); h.done <- struct{}{} })
}

// TestResourceLifetimes verifies admission and finalization release without a
// cooperative peer, including shutdown after a protocol error cancels a handler
// that is blocked writing a response. Timeouts here are tested policies, not
// sleeps used to synchronize a race.
func TestResourceLifetimes(t *testing.T) {
	for _, kind := range []string{"tcp", "quic"} {
		for _, mode := range []string{"slow-open", "reject-drain", "blocked-send", "shutdown-after-protocol-error"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				serverHost, clientHost := hosts(t, kind)
				done := make(chan struct{}, 1)
				canceled := make(chan error, 1)
				server := connect.NewServer()
				server.Register(connect.Method{Spec: connect.Spec{Procedure: procedure, StreamType: connect.StreamTypeServer}, Handler: func(ctx context.Context, _ connect.Spec, stream connect.ServerStream) error {
					if mode == "reject-drain" {
						return connect.NewError(connect.CodeUnauthenticated, "no")
					}
					if err := stream.Receive(new(p2p.Blob)); err != nil {
						return err
					}
					if mode == "shutdown-after-protocol-error" {
						go func() { <-ctx.Done(); canceled <- context.Cause(ctx) }()
					}
					return stream.Send(&p2p.Blob{Cid: make([]byte, 4<<20)})
				}})
				o := connectlibp2p.ServerOptions{OpenTimeout: 100 * time.Millisecond, FinishTimeout: 100 * time.Millisecond, MaxCallDuration: 200 * time.Millisecond}
				if mode == "shutdown-after-protocol-error" {
					o.MaxCallDuration = 10 * time.Second
				}
				stop := connectlibp2p.Serve(t.Context(), joinedHost{serverHost, done}, testProtocol, server, o)
				t.Cleanup(stop)
				raw, err := clientHost.NewStream(t.Context(), serverHost.ID(), testProtocol)
				require.NoError(t, err)
				defer func() { _ = raw.Close() }()
				require.NoError(t, raw.SetDeadline(time.Now().Add(3*time.Second)))
				buf := []byte{0} // partial OPEN to start negotiation/admission.
				if mode != "slow-open" {
					buf = appendWire(t, nil, 0, &wire.Open{Procedure: procedure})
				}
				if mode == "blocked-send" || mode == "shutdown-after-protocol-error" {
					buf = appendWire(t, buf, 1, &p2p.Blob{})
					buf = append(buf, 2, 0, 0, 0, 0)
				}
				if mode == "shutdown-after-protocol-error" {
					buf = appendWire(t, buf, 1, &p2p.Blob{})
				}
				_, err = raw.Write(buf)
				require.NoError(t, err)
				if mode == "reject-drain" {
					require.EqualValues(t, connect.CodeUnauthenticated, terminal(t, raw).Status.Code)
				}
				if mode == "shutdown-after-protocol-error" {
					select {
					case err := <-canceled:
						require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
					case <-time.After(3 * time.Second):
						t.Fatal("malformed input did not cancel the blocked handler")
					}
					stopped := make(chan struct{})
					go func() { stop(); close(stopped) }()
					select {
					case <-stopped:
					case <-time.After(3 * time.Second):
						t.Fatal("shutdown failed to interrupt I/O after coded cancellation")
					}
				}
				select {
				case <-done:
				case <-time.After(3 * time.Second):
					t.Fatal("resource policy did not join the stream handler")
				}
			})
		}
	}
}

// TestAdmission checks both per-peer and global caps, and capacity recovery.
func TestAdmission(t *testing.T) {
	for _, global := range []bool{false, true} {
		t.Run(fmt.Sprint("global=", global), func(t *testing.T) {
			serverHost, clientHost := hosts(t, "tcp")
			other := clientHost
			if global {
				var err error
				other, err = libp2p.New(libp2p.NoListenAddrs, libp2p.DisableRelay(), libp2p.DisableMetrics())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, other.Close()) })
				require.NoError(t, other.Connect(t.Context(), peer.AddrInfo{ID: serverHost.ID(), Addrs: serverHost.Addrs()}))
			}
			entered := make(chan struct{}, 2)
			done := make(chan struct{}, 4)
			server := connect.NewServer()
			server.Register(connect.Method{Spec: connect.Spec{Procedure: procedure, StreamType: connect.StreamTypeBidi}, Handler: func(ctx context.Context, _ connect.Spec, _ connect.ServerStream) error {
				entered <- struct{}{}
				<-ctx.Done()
				return ctx.Err()
			}})
			o := connectlibp2p.ServerOptions{MaxConcurrentCalls: 2, MaxConcurrentCallsPerPeer: 1}
			if global {
				o.MaxConcurrentCalls, o.MaxConcurrentCallsPerPeer = 1, 2
			}
			t.Cleanup(connectlibp2p.Serve(t.Context(), joinedHost{serverHost, done}, testProtocol, server, o))
			open := appendWire(t, nil, 0, &wire.Open{Procedure: procedure})
			first, err := clientHost.NewStream(t.Context(), serverHost.ID(), testProtocol)
			require.NoError(t, err)
			defer func() { _ = first.Reset() }()
			_, err = first.Write(open)
			require.NoError(t, err)
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("first call not admitted")
			}
			rejected, err := other.NewStream(t.Context(), serverHost.ID(), testProtocol)
			if err == nil {
				defer func() { _ = rejected.Reset() }()
				require.NoError(t, rejected.SetDeadline(time.Now().Add(3*time.Second)))
				_, err = rejected.Write(open)
				if err == nil {
					_, err = rejected.Read(make([]byte, 1))
				}
			}
			require.Error(t, err)
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("overload handler did not return")
			}
			require.NoError(t, first.Reset())
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("active handler did not release capacity")
			}
			next, err := clientHost.NewStream(t.Context(), serverHost.ID(), testProtocol)
			require.NoError(t, err)
			defer func() { _ = next.Reset() }()
			_, err = next.Write(open)
			require.NoError(t, err)
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("capacity did not recover")
			}
		})
	}
}

// TestMessageLimits checks the exact existing 8 MiB boundary in both directions.
func TestMessageLimits(t *testing.T) {
	for _, kind := range []string{"tcp", "quic"} {
		t.Run(kind, func(t *testing.T) {
			serverHost, clientHost := hosts(t, kind)
			server := connect.NewServer()
			spec := connect.Spec{Procedure: procedure}
			server.Register(connect.Method{Spec: spec, Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
				var req p2p.Blob
				if err := stream.Receive(&req); err != nil {
					return err
				}
				if req.Cursor == "oversized-response" {
					return stream.Send(&p2p.Blob{Cid: make([]byte, 8<<20)})
				}
				return stream.Send(&req)
			}})
			stop := connectlibp2p.Serve(t.Context(), serverHost, testProtocol, server)
			t.Cleanup(stop)
			client := connect.NewClient(connectlibp2p.NewTransport(clientHost, serverHost.ID(), testProtocol))
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			var response p2p.Blob
			request := &p2p.Blob{Cid: bytes.Repeat([]byte{77}, (8<<20)-5)}
			require.Equal(t, 8<<20, proto.Size(request))
			require.NoError(t, client.CallUnary(ctx, spec, request, &response))
			require.Equal(t, request.Cid, response.Cid)
			request.Cid = append(request.Cid, 78)
			require.Equal(t, (8<<20)+1, proto.Size(request))
			err := client.CallUnary(ctx, spec, request, &response)
			require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
			err = client.CallUnary(ctx, spec, &p2p.Blob{Cursor: "oversized-response"}, &response)
			require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
			ctx, info := connect.NewClientContext(ctx)
			info.RequestHeader().Set("Huge", strings.Repeat("x", 64<<10))
			err = client.CallUnary(ctx, spec, &p2p.Blob{}, &response)
			require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
		})
	}
}

// TestResourceManagerMemory checks the deployment's real service memory cap,
// independently of the transport's 8 MiB message limit.
func TestResourceManagerMemory(t *testing.T) {
	limits := rcmgr.PartialLimitConfig{Service: map[string]rcmgr.ResourceLimits{"connectlibp2p": {Memory: 512 << 10}}}
	manager, err := rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits.Build(rcmgr.DefaultLimits.AutoScale())))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	serverHost, clientHost := hosts(t, "tcp", libp2p.ResourceManager(manager))
	server := connect.NewServer()
	spec := connect.Spec{Procedure: procedure}
	server.Register(connect.Method{Spec: spec, Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
		if err := stream.Receive(new(p2p.Blob)); err != nil {
			return err
		}
		return stream.Send(&p2p.Blob{Cid: make([]byte, 2<<20)})
	}})
	t.Cleanup(connectlibp2p.Serve(t.Context(), serverHost, testProtocol, server))
	client := connect.NewClient(connectlibp2p.NewTransport(clientHost, serverHost.ID(), testProtocol))
	err = client.CallUnary(t.Context(), spec, &p2p.Blob{}, new(p2p.Blob))
	require.Equal(t, connect.CodeResourceExhausted, connect.CodeOf(err))
}

// TestMalformedPeers checks that an oversized declared payload is rejected
// without reading it, and that broken terminal framing cannot report success.
func TestMalformedPeers(t *testing.T) {
	t.Run("oversized-declared-request", func(t *testing.T) {
		serverHost, clientHost := hosts(t, "tcp")
		server := connect.NewServer()
		server.Register(connect.Method{Spec: connect.Spec{Procedure: procedure}, Handler: func(_ context.Context, _ connect.Spec, stream connect.ServerStream) error {
			return stream.Receive(new(p2p.Blob))
		}})
		t.Cleanup(connectlibp2p.Serve(t.Context(), serverHost, testProtocol, server))
		raw, err := clientHost.NewStream(t.Context(), serverHost.ID(), testProtocol)
		require.NoError(t, err)
		defer func() { _ = raw.Reset() }()
		require.NoError(t, raw.SetDeadline(time.Now().Add(5*time.Second)))
		// Wire contract: 0=open, 1=message, 3=headers, 4=terminal. The
		// advertised one-gigabyte body is intentionally never sent.
		open, err := proto.Marshal(&wire.Open{Procedure: procedure, TimeoutNs: int64(10 * time.Second)})
		require.NoError(t, err)
		buf := binary.BigEndian.AppendUint32([]byte{0}, uint32(len(open))) // #nosec G115 -- fixed small fixture.
		buf = append(buf, open...)
		buf = binary.BigEndian.AppendUint32(append(buf, 1), 1<<30)
		_, err = raw.Write(buf)
		require.NoError(t, err)
		for _, wantKind := range []byte{3, 4} {
			var header [5]byte
			_, err := io.ReadFull(raw, header[:])
			require.NoError(t, err)
			require.Equal(t, wantKind, header[0])
			size := binary.BigEndian.Uint32(header[1:])
			require.LessOrEqual(t, size, uint32(64<<10))
			body := make([]byte, size)
			_, err = io.ReadFull(raw, body)
			require.NoError(t, err)
			if wantKind == 4 {
				var end wire.End
				require.NoError(t, proto.Unmarshal(body, &end))
				require.EqualValues(t, connect.CodeResourceExhausted, end.GetStatus().GetCode())
			}
		}
	})
	for _, invalid := range []string{"missing-terminal", "duplicate-message", "truncated-terminal"} {
		t.Run(invalid, func(t *testing.T) {
			serverHost, clientHost := hosts(t, "tcp")
			serverHost.SetStreamHandler(testProtocol, func(raw network.Stream) {
				defer func() { _ = raw.Close() }()
				// Drain the open/message/half-close frames, so completion is
				// not confused with an early transport reset.
				for range 3 {
					var header [5]byte
					if _, err := io.ReadFull(raw, header[:]); err != nil {
						return
					}
					if _, err := io.CopyN(io.Discard, raw, int64(binary.BigEndian.Uint32(header[1:]))); err != nil {
						return
					}
				}
				payload, _ := proto.Marshal(&p2p.ListSpacesResponse{NextPageToken: "not-success"})
				buf := []byte{3, 0, 0, 0, 0}
				buf = binary.BigEndian.AppendUint32(append(buf, 1), uint32(len(payload))) // #nosec G115 -- fixed small fixture.
				buf = append(buf, payload...)
				switch invalid {
				case "duplicate-message":
					buf = binary.BigEndian.AppendUint32(append(buf, 1), uint32(len(payload))) // #nosec G115 -- fixed small fixture.
					buf = append(buf, payload...)
				case "truncated-terminal":
					buf = append(buf, 4, 0, 0)
				}
				_, _ = raw.Write(buf)
			})
			client := connect.NewClient(connectlibp2p.NewTransport(clientHost, serverHost.ID(), testProtocol))
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := client.CallUnary(ctx, connect.Spec{Procedure: procedure}, &p2p.ListSpacesRequest{}, new(p2p.ListSpacesResponse))
			require.Error(t, err)
			require.NotEqual(t, connect.CodeDeadlineExceeded, connect.CodeOf(err), "must detect invalid framing, not just time out")
		})
	}
}
