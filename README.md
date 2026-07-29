# PeerRPC

> gRPC-compatible RPC over WebRTC DataChannels. Cross-network, NAT-traversing, multi-language.

PeerRPC rebuilds gRPC's **semantic model** on top of WebRTC instead of HTTP/2.

* Same method paths (`/{Package}.{Service}/{Method}`)
* All four RPC types (Unary / Server Streaming / Client Streaming / Bidi)
* Header + Trailer metadata
* `google.rpc.Status` error model, 1:1 with grpc-go / connect-go
* Deadline propagation + cancellation
* Interceptor chain (Unary + Stream)
* Half-close (CloseSend) for client streaming

It does **not** reuse the gRPC HTTP/2 wire format.
PeerRPC has its own frame format defined in `proto/peerrpc/peerrpc.proto`.

## Repository layout

```
peerrpc/
├── proto/                 # protocol definitions (single source of truth)
│   ├── peerrpc/           #   core frame protocol v2
│   └── peerrpc/signaling/v2/  # signaling protocol
├── peerrpc-go/            # Go SDK
├── peerrpc-ts/            # TypeScript SDK
├── peerrpc-rs/            # Rust SDK
├── test/vectors/          # golden protocol vectors
├── buf.yaml
└── buf.gen.yaml
```

## Build & test

Requirements: Go 1.22+, [buf](https://buf.build) (`go install github.com/bufbuild/buf/cmd/buf@latest`).

```bash
# Lint & generate code in all three languages.
buf lint
buf generate

# Regenerate the golden vectors (when proto/schema changes).
go -C peerrpc-go run ./cmd/gen-vectors

# Run the regression test.
go -C peerrpc-go test ./protocol/...
```

The vectors at `test/vectors/*.bin` are the canonical binary encoding of
representative `Frame` / `ResponseFrame` messages. Every SDK (Go, TS, Rust)
MUST decode these byte-for-byte identically and re-encode them to the
exact same bytes.

## License

Apache License 2.0. See `LICENSE`.
