// Package grpcbridge lets a PeerRPC client invoke a standard
// Connect-RPC service without modifying the service.
//
// The bridge plays the role of a BidiStream forwarder:
//
//	PeerRPC client ── DataChannel ──▶ bridge ── HTTP ──▶ Connect service
//	    (PeerRPC frames)                          (Connect/gRPC)
//
// The bridge is the inverse of the relay: the relay is
// protocol-opaque byte forwarding between two PeerRPC peers; the
// bridge is protocol-translating forwarding between PeerRPC and
// Connect/gRPC.
//
// Two integration shapes are supported:
//
//   1. In-process: the bridge wraps an already-constructed
//      connect-go http.Handler and dispatches incoming PeerRPC
//      frames to it via httptest.NewRecorder + handler.ServeHTTP.
//      Use this when the PeerRPC server and the Connect service
//      live in the same binary.
//
//   2. Out-of-process: the bridge forwards over HTTP to a remote
//      Connect service URL. Use this when the Connect service is a
//      pre-existing deployment the PeerRPC client wants to reach.
//
// v1 ships both shapes: shape 1 via HTTPHandlerInvoker (in-process)
// and shape 2 via HTTPClientInvoker (out-of-process), which the
// `peerrpc bridge` subcommand uses to forward to a remote Connect URL.
package grpcbridge
