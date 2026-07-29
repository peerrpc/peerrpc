# PeerRPC API Reference

This document describes the public API surface of each SDK.
The canonical reference is the Go SDK; TypeScript and Rust follow
equivalent patterns.

---

## Go SDK (`github.com/peerrpc/go`)

### Package `protocol`

Functions for length-prefixed frame encoding/decoding.

- `LengthPrefix(payload []byte) []byte` -- prepend 4-byte BE length.
- `StripLengthPrefix(b []byte) []byte` -- remove length prefix (best-effort).
- `EncodeFrame(frame *peerrpcpb.Frame) ([]byte, error)`
- `DecodeFrame(raw []byte) (*peerrpcpb.Frame, error)`

### Package `transport`

DataChannel wrapper with backpressure and chunk reassembly.

- `New(dc *webrtc.DataChannel) *Channel` -- wrap an established DC.
- `(*Channel).SendFrame(ctx, proto.Message) error`
- `(*Channel).SendRaw(ctx, []byte) error` -- verbatim (for relay).
- `(*Channel).Recv(ctx) ([]byte, error)` -- returns payload with prefix stripped.
- `(*Channel).Close() error`
- `NewReassembler() *Reassembler`
- `(*Reassembler).Reassemble(seq, total, offset, data) ([]byte, bool)`

Constants: `InlineMax` (16K), `MessageMax` (256K), `ChunkSize` (256K),
`BufferedAmountHigh` (1 MiB).

### Package `peer`

WebRTC PeerConnection lifecycle.

- `New(ctx, role signal.Role, cfg Config) (*Peer, error)`
- `(*Peer).Dial(ctx, sig *signal.Session) (*transport.Channel, error)` -- Offerer.
- `(*Peer).Accept(ctx, sig *signal.Session) (*transport.Channel, error)` -- Answerer.
- `(*Peer).Close() error`

### Package `signal`

Pluggable signaling backends.

- `NewLocal() *Local` -- in-process backend for tests/demos.
- `NewRemote(baseURL string, opts ...connect.ClientOption) *Remote` -- connect-go client.
- `Backend` interface: `Exchange(ctx, roomID, peerID) (*Session, error)`
- `(*Session).Send(ctx, *SignalMessage) error`
- `(*Session).Receive() <-chan *SignalMessage`
- `(*Session).Close() error`

### Package `rpc`

Server and Client for RPC dispatch over a transport.Channel.

- `NewServer(opts ...ServerOption) *Server`
- `(*Server).RegisterService(ServiceDesc)`
- `(*Server).Serve(ctx, *transport.Channel) error`
- `NewClient(ch *transport.Channel, opts ...ClientOption) *Client`
- `(*Client).Attach(ctx) error`
- `(*Client).InvokeUnary(ctx, method, req) (resp, *Status, error)`
- `(*Client).InvokeServerStreaming(ctx, method, req) (*ServerStream, error)`
- `MethodDesc`, `ServiceDesc`, `MethodKind*`, `ServerStream`
- Interceptors: `UnaryServerInterceptor`, `StreamServerInterceptor`, etc.

### Package `relay`

Byte-forwarding relay node.

- `New(cfg Config) (*Server, error)`
- `(*Server).Serve(ctx, roomID, relayPeerID) error`

### Package `grpcbridge`

Bridge PeerRPC to Connect/gRPC services.

- `HTTPHandlerInvoker` -- in-process handler.
- `UnaryHandler(invoker) HandlerFunc`
- `ServerStreamingHandler(invoker) HandlerFunc`
- `MountConnectService(srv, name, methods, invoker)`

### Package `auth`

JWT verification.

- `NewHMACVerifier(secret string) *Verifier`
- `(*Verifier).Verify(token string) (*Claims, error)`

### Package `crypto`

End-to-end encryption (AES-256-GCM with ECDH key exchange + HKDF).

- `GenerateKeyPair() (priv, pub, error)`
- `NewEncryptionSession(priv, peerPub) (*Session, error)`
- `(*Session).Encrypt(plaintext) (ciphertext, error)`
- `(*Session).Decrypt(ciphertext) (plaintext, error)`

### Package `observability`

Logging, Prometheus metrics, and OpenTelemetry tracing interceptors.

- `NewLoggingInterceptor(logger *slog.Logger) rpc.UnaryServerInterceptor`
- `NewMetricsInterceptor(reg *prometheus.Registry) rpc.UnaryServerInterceptor`
- `NewTracingInterceptor(tp *trace.TracerProvider) rpc.UnaryServerInterceptor`

---

## TypeScript SDK (`@peerrpc/*`)

### `@peerrpc/protocol`

- `encodeFrame(frame: Frame): Uint8Array`
- `decodeFrame(data: Uint8Array): Frame`
- `encodeResponseFrame(frame: ResponseFrame): Uint8Array`
- `decodeResponseFrame(data: Uint8Array): ResponseFrame`
- `lengthPrefix(payload: Uint8Array): Uint8Array`

### `@peerrpc/transport`

- `Channel` class wrapping `RTCDataChannel` with:
  - `sendFrame(frame: proto.IMessage): Promise<void>`
  - `recv(): Promise<Uint8Array>`
  - `close(): void`

### `@peerrpc/peer`

- `Peer` class with:
  - `constructor(config: PeerConfig)`
  - `dial(signal: SignalTransport): Promise<Channel>`
  - `accept(signal: SignalTransport): Promise<Channel>`

### `@peerrpc/rpc`

- `Client` class:
  - `invokeUnary(method: string, req: Uint8Array): Promise<UnaryResponse>`
  - `invokeServerStreaming(method: string, req: Uint8Array): AsyncIterable<Uint8Array>`

### `@peerrpc/react`

- `usePeerRPC(config): Peer`
- `useUnary(peer, method, request): UseUnaryResult`
- `useServerStream(peer, method, request): UseStreamResult`
- `useConnected(peer): boolean`

---

## Rust SDK (`peerrpc-rs`)

### `peerrpc-protocol`

- `encode_frame(frame: &Frame) -> Bytes`
- `try_decode_frame(buf: &[u8]) -> Result<Option<(Frame, usize)>, DecodeError>`
- `length_prefix(payload: &[u8]) -> Bytes`
- Constants: `INLINE_MAX`, `MESSAGE_MAX`, `CHUNK_SIZE`, `BUFFERED_AMOUNT_HIGH`

### `peerrpc-transport`

- `Channel` struct:
  - `new(dc: Arc<RTCDataChannel>) -> Arc<Channel>`
  - `send_frame(frame: &Frame) -> Result<(), ChannelError>`
  - `send_raw(payload: &[u8]) -> Result<(), ChannelError>`
  - `recv_raw() -> Result<Bytes, ChannelError>`
  - `recv_frame() -> Result<Frame, ChannelError>`
  - `reassemble(seq, total, offset, data) -> Option<Vec<u8>>`
- `Reassembler` struct

### `peerrpc-peer`

- `Peer` struct implementing `WireTransport`:
  - `create_offer(cfg) -> Result<(Self, String), PeerError>`
  - `accept_offer(cfg, offer_sdp) -> Result<(Self, String), PeerError>`
  - `set_remote_answer(sdp) -> Result<(), PeerError>`
  - `close() -> Result<(), PeerError>`

### `peerrpc-rpc`

- `Client` struct:
  - `new<T: WireTransport>(transport: T) -> Arc<Client>`
  - `invoke_unary(method, req) -> Result<(Vec<u8>, Status), RpcError>`
  - `invoke_server_streaming(method, req) -> Result<ClientStream, RpcError>`
- `ClientStream` struct with `recv() -> Option<Vec<u8>>` and `wait_end() -> Status`
- `WireTransport` trait
