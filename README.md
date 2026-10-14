# connectlibp2p-go

A [Connect-Go v2](https://connectrpc.com/) transport over native libp2p streams.
Supports unary, client-streaming, server-streaming, and bidirectional RPCs, with
protobuf messages, metadata, trailers, interceptors, and deadlines. Each call opens
one libp2p stream.

## Install

Requires Go 1.26 or newer.

```sh
go get github.com/burdiyan/connectlibp2p-go
```

## Usage

Register a generated Connect service, then give its client the libp2p transport:

```go
import (
    "connectrpc.com/connect/v2"
    "github.com/burdiyan/connectlibp2p-go"
    "github.com/libp2p/go-libp2p/core/protocol"
)

// Use your service's generated Connect package in place of yourserviceconnect.
rpcID := protocol.ID("/myapp/rpc/1.0.0")
server := connect.NewServer()
yourserviceconnect.RegisterYourServiceHandler(server, implementation)
stop := connectlibp2p.Serve(ctx, serverHost, rpcID, server)
defer stop()

client := yourserviceconnect.NewYourServiceClient(connect.NewClient(
    connectlibp2p.NewTransport(clientHost, serverPeerID, rpcID),
))
```

Create and connect the libp2p hosts yourself. Both endpoints must use the same
nonempty protocol ID and compatible services. Register methods before calling
`Serve`, and use only one `Serve` registration per host/protocol ID. Choose a new
ID when making incompatible changes to the framing or application protocol.

The [Echo example](examples/echo/main.go) includes a schema, generated bindings,
and a complete client/server setup:

```sh
go run ./examples/echo
# hello libp2p
```

## Limits and lifecycle

- Protobuf payloads only; no JSON or compression.
- Maximum application message: 8 MiB. Maximum metadata/status frame: 64 KiB.
- Default incoming-call limits: 64 total and 16 per peer, per `Serve` instance.
- Default time budgets: 5 seconds to receive the initial request frame, 5 minutes
  per call, and 5 seconds to write final status and drain requests. Shorter caller
  deadlines take precedence.
- Pass `ServerOptions` to `Serve` to change admission limits and time budgets.
  Zero fields use defaults; negative values panic. Increase `MaxCallDuration`
  explicitly for long-lived streams.
- Frame buffers are accounted against libp2p's resource manager. Configure its
  connection, stream, and memory limits for your deployment; handler allocations
  are not covered by the transport's buffer accounting.
- `CallInfo.TransportInfo` contains the authenticated remote `peer.ID`.
  Authentication is not authorization: enforce access policy in your service or
  interceptors.
- One send and one receive may run concurrently; concurrent sends or concurrent
  receives on the same call are unsupported. Handlers must join their goroutines
  before returning.
- Close abandoned client streams. The stop function returned by `Serve` cancels
  active calls and waits for handlers to exit; handlers must honor context.

**Cancellation caveat:** if a streaming handler stops receiving while an unread
request is waiting for it, the transport cannot observe a later peer reset until
receiving resumes. Keep consuming requests or return. Deadlines and
`MaxCallDuration` bound the call, but cannot stop handler code that ignores context.

## Development

Use [mise](https://mise.jdx.dev/) for the pinned toolchain:

```sh
mise install
mise run test
mise run vet
mise run lint
mise exec -- go run ./examples/echo
mise run generate # after editing .proto files
```

Tests use real TCP and QUIC peers; `mise run test` enables the race detector.
Generation uses Buf with local Go/Connect plugins pinned by `go.mod`. Google API
schemas are pinned in `buf.lock`, not vendored. Generated files are checked in.
Agent guidance is in [AGENTS.md](AGENTS.md).

## License

[Apache-2.0](LICENSE).
