# PeerRPC Getting Started

PeerRPC is a multi-language RPC framework built on WebRTC DataChannels.
This guide walks through the concepts and shows how to build a simple
echo service in Go, TypeScript, and Rust.

## Architecture Overview

```
+----------+   WebRTC DataChannel   +----------+
|  Peer A  |<---------------------->|  Peer B  |
|  (Go SDK)|   (P2P encrypted)      |  (TS SDK)|
+----+-----+                        +----+-----+
     |                                   |
     |  signaling (SDP exchange)         |
     |  via in-process Local or           |
     |  remote signal-server              |
     +------------------+----------------+
                        |
               +--------+--------+
               |  signal-server  |
               |  (optional)     |
               +-----------------+
```

When direct P2P connection is not possible (symmetric NAT, firewalls),
an application-layer **relay** can forward traffic between two peers:

```
+----------+   DC#1   +----------+   DC#2   +----------+
|  Peer A  |<-------->|  Relay   |<-------->|  Peer B  |
+----------+          +----------+          +----------+
```

## Concepts

- **Protocol**: Custom protobuf wire format with Frame/ResponseFrame,
  length-prefixed encoding, and chunk reassembly for large messages.
- **Transport**: Wraps a WebRTC DataChannel with backpressure and
  transparent chunking at 256 KiB boundaries.
- **Peer**: Manages the WebRTC PeerConnection lifecycle (offer/answer
  via signaling).
- **Signaling**: The rendezvous channel (in-process Local or a remote
  WebSocket signal-server) that exchanges SDP offers/answers and ICE
  candidates.
- **RPC**: Server multiplexes streams over one DataChannel; Client
  allocates sequence numbers and dispatches responses.
- **Relay**: Byte-forwarding node that joins a signaling room and pairs
  two DataChannels.
- **gRPC Bridge**: Exposes PeerRPC services by forwarding to a remote
  Connect/gRPC service.

## Go Quickstart

### Prerequisites
- Go 1.25+
- `buf` CLI (for code generation)

### 1. Add the dependency

```bash
go get github.com/peerrpc/go
```

### 2. Define a service

PeerRPC uses raw protobuf bytes at the API boundary. Define your
protobuf service as usual and use the `rpc.Server` to register handlers:

```go
package main

import (
    "context"
    "log"
    "os/signal"
    "syscall"

    "github.com/peerrpc/go/peer"
    "github.com/peerrpc/go/rpc"
    "github.com/peerrpc/go/signal"
)

func main() {
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer stop()

    // 1. Signaling (in-process for localhost dev)
    backend := signal.NewLocal()
    sig, _ := backend.Exchange(ctx, "my-room", "server")

    // 2. Start PeerConnection as Answerer
    p, _ := peer.New(ctx, signal.RoleServer, peer.Config{})
    ch, _ := p.Accept(ctx, sig)

    // 3. Register RPC handler
    srv := rpc.NewServer()
    srv.RegisterService(rpc.ServiceDesc{
        ServiceName: "echo.Echo",
        Methods: []rpc.MethodDesc{
            {Method: "Echo", Kind: rpc.MethodKindUnary, Handler: echoHandler},
        },
    })

    // 4. Serve
    _ = srv.Serve(ctx, ch)
}

func echoHandler(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
    req, _ := s.Recv()
    // req is []byte -- unmarshal with proto.Unmarshal
    s.Send(req) // echo back
    return rpc.OK()
}
```

### Client side

```go
ch, _ := p.Dial(ctx, sig) // instead of Accept
client := rpc.NewClient(ch)
go client.Attach(ctx)

resp, status, _ := client.InvokeUnary(ctx, "/echo.Echo/Echo", reqBytes)
```

See `examples/echo-go/` for a complete working example.

## TypeScript Quickstart

### Prerequisites
- Node.js 20+

### Install

```bash
npm install @peerrpc/protocol @peerrpc/transport @peerrpc/peer \
            @peerrpc/signal @peerrpc/rpc
```

### Usage

```typescript
import { Peer } from '@peerrpc/peer';
import { Client, Server } from '@peerrpc/rpc';

const peer = new Peer({ role: 'answerer' });
await peer.join('my-room', 'http://localhost:8080');

const client = new Client(peer.dataChannel);
// Use client.invokeUnary('/echo.Echo/Echo', requestBytes)
```

### React hooks

```typescript
import { usePeerRPC, useUnary } from '@peerrpc/react';

function App() {
  const peer = usePeerRPC({ role: 'offerer', signalURL: 'http://localhost:8080' });
  const result = useUnary(peer, '/echo.Echo/Echo', requestBytes);
  return <div>{result}</div>;
}
```

## Rust Quickstart

### Prerequisites
- Rust 1.75+

### Add dependencies

```toml
[dependencies]
peerrpc-protocol = "0.1"
peerrpc-transport = "0.1"
peerrpc-peer = "0.1"
peerrpc-rpc = "0.1"
```

### Usage

```rust
use peerrpc_peer::Peer;
use peerrpc_rpc::Client;

let (peer, offer_sdp) = Peer::create_offer(Default::default()).await?;
// Exchange SDP via signaling...

let client = Client::new(peer);
let (resp, status) = client.invoke_unary("/echo.Echo/Echo", &req_bytes).await?;
```

## Running the Signal Server

```bash
cd peerrpc-go
go run ./cmd/peerrpc signal -addr :8080
```

## Running the Relay

```bash
cd peerrpc-go
go run ./cmd/peerrpc relay -service my-service -signal http://localhost:8080
```

Without `-signal`, the relay uses in-process signaling (localhost demos only).

## Running the gRPC Bridge

```bash
cd peerrpc-go
go run ./cmd/peerrpc bridge \
    -room my-room \
    -upstream http://localhost:9090 \
    -service echo.Echo:Echo,Stream
```

## Protocol Golden Vectors

The `test/vectors/` directory contains golden `.bin` files that every
SDK must decode byte-identically. To regenerate:

```bash
make gen-vectors
make test-vectors
```

## Development

```bash
# Lint, generate, test
make all

# Go-specific
make check-go
make tidy
```

## SDK Status

| Language   | Protocol | Transport | Peer | RPC | Signaling | Relay | Bridge | React |
|------------|----------|-----------|------|-----|-----------|-------|--------|-------|
| Go         | Done     | Done      | Done | Done| Done      | Done  | Done   | N/A   |
| TypeScript | Done     | Done      | Done | Done| Done      | N/A   | N/A    | Done  |
| Rust       | Done     | Done      | Done | Done| N/A       | N/A   | N/A    | N/A   |

## Further reading

- **[SCTP max-message-size and Adaptive Chunking](sctp-message-sizing.md)** —
  why each SDK ships a different `CHUNK_SIZE` (60 KiB on the TS
  client, 255 KiB on Go/Rust), how the `a=max-message-size` SDP
  attribute is negotiated end-to-end, and the post-send backpressure
  pattern that makes large-payload RPCs (Large Echo / Large Echo
  Stream) work over WebRTC.
