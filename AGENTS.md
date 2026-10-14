# Repository basics

This is a standalone Go transport library for Connect-Go v2 over native libp2p streams.
The module is `github.com/burdiyan/connectlibp2p-go`; the public package is `connectlibp2p`.
Read `README.md` for the public API, framing contract, resource limits, and cancellation caveat.
Keep user-facing documentation in `README.md` and agent guidance in this file.

## Layout

- `transport.go`: public `NewTransport`, `Serve`, and `ServerOptions`, plus framing,
  metadata, status conversion, stream lifetimes, and resource admission.
- `transport_test.go`: external-package regression tests using real TCP and QUIC peers.
- `proto/connectlibp2p/v1/transport.proto`: internal wire-control messages.
- `proto/connectlibp2p/test/v1/messages.proto`: test-only payload messages.
- `internal/wire` and `internal/testpb`: checked-in generated protobuf code.
- `examples/echo`: runnable service, schema, and generated protobuf/Connect bindings.
- `buf.yaml`, `buf.gen.yaml`, and `buf.lock`: schema modules, local generation, and
  pinned Google API schema dependency. No vendored schemas or external `protoc` compiler.
- `mise.toml`: pinned development tools and tasks.
- `.github/workflows/test.yml`: minimum/development Go tests and generation checks.

## Development

Use mise so commands run with the repository's pinned tools:

```sh
mise install
mise run generate
mise run test
mise run vet
mise run lint
mise exec -- go run ./examples/echo
mise exec -- go mod verify
test -z "$(mise exec -- gofmt -l .)"
```

The `test` task runs the race detector three times. CI tests both minimum Go 1.26.0
and development Go 1.27.1; preserve minimum-version compatibility.
Format edited Go files with `mise exec -- gofmt -w <files>`.
When changing dependencies, run `mise exec -- go mod tidy` and review `go.mod`/`go.sum`.

Edit schemas, not generated Go files, then run `mise run generate` and include the
generated changes. `go generate ./...` delegates to the same mise task.
Buf compiles locally and invokes Go plugins at the versions in `go.mod`; do not add
remote generation plugins or separately versioned plugin binaries.
Run `mise exec -- buf dep update` only when intentionally updating schema dependencies,
and include the resulting `buf.lock`. Fresh caches require network access.
Repeat generation and check that it introduces no additional changes.

## Constraints

- Keep this library transport-only. Hosts, discovery, application services, and
  authorization policy belong to callers, not this package.
- Preserve all four RPC shapes, protobuf payloads, identity compression, authenticated
  peer information, bounded resources, and synchronous/idempotent shutdown.
- The framing contract is not Connect HTTP. Application-owned protocol IDs and the
  Go module version are separate; incompatible framing needs a new protocol ID.
- Do not mask the documented cancellation/backpressure limitation with unbounded queues.
- Add regression coverage through the public transport contract, including TCP and
  QUIC where relevant. Keep generated wire types internal.
- Preserve license/copyright notices. Do not publish, tag, push, or rewrite shared
  history unless the user explicitly requests that action.
