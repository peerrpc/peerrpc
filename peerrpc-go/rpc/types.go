// Package rpc is the PeerRPC runtime: server, client, and the stream
// abstraction that carries a single RPC across the multiplexed
// DataChannel.
//
// Layering (top to bottom):
//
//	caller code
//	     │  MethodDesc.Handler / Client.InvokeXXX
//	     ▼
//	rpc.Server / rpc.Client
//	     │  Frame / ResponseFrame (proto)
//	     ▼
//	transport.Channel (shard + backpressure)
//	     ▼
//	peer.DataChannel
//
// A single Server or Client multiplexes many concurrent RPC streams
// over one transport.Channel by Routing.sequence.
package rpc

import (
	"context"
	"errors"
	"time"

	rpcpb "google.golang.org/genproto/googleapis/rpc/status"
)

// MethodKind classifies RPC methods by their streaming shape, matching
// gRPC/connect-go semantics.
type MethodKind int

const (
	MethodKindUnary MethodKind = iota
	MethodKindServerStreaming
	MethodKindClientStreaming
	MethodKindBidiStreaming
)

// MethodDesc registers one RPC method with the Server.
//
// The handler works in raw protobuf bytes so the RPC layer does not
// depend on any service-specific generated code. Service authors wire
// their generated types in via proto.Marshal/Unmarshal at the call
// boundary.
type MethodDesc struct {
	// Method is the bare method name ("Echo", NOT "/echo.Echo/Echo").
	// The Server prepends "/ServiceName/" when registering.
	Method string
	Kind   MethodKind
	// Handler runs once per inbound RPC.
	Handler func(ctx context.Context, stream *ServerStream) *Status
}

// ServiceDesc groups a set of related MethodDesc.
type ServiceDesc struct {
	ServiceName string
	Methods     []MethodDesc
}

// Status mirrors google.rpc.Status for ergonomic in-package use.
// Code values match grpc-go / connect-go (0=OK, 1=CANCELLED, ...).
type Status struct {
	Code    int32
	Message string
}

// OK is the canonical success status.
func OK() *Status { return &Status{Code: 0} }

// Err returns a Status carrying err's message under the given code.
func Err(code int32, err error) *Status {
	if err == nil {
		return OK()
	}
	return &Status{Code: code, Message: err.Error()}
}

// Err returns the Status as a Go error, or nil for OK.
func (s *Status) Err() error {
	if s == nil || s.Code == 0 {
		return nil
	}
	return errors.New(s.Message)
}

// toProto converts to the on-wire google.rpc.Status.
func (s *Status) toProto() *rpcpb.Status {
	if s == nil {
		return &rpcpb.Status{Code: 0}
	}
	return &rpcpb.Status{Code: s.Code, Message: s.Message}
}

// statusFromProto inverts toProto.
func statusFromProto(p *rpcpb.Status) *Status {
	if p == nil {
		return OK()
	}
	return &Status{Code: p.Code, Message: p.Message}
}

// errEOF is the internal sentinel for "stream half-closed by peer".
// It is mapped to io.EOF at the public API boundary.
var errEOF = errors.New("rpc: stream half-closed")

// durationFromMs converts a wire deadline_ms into a Go duration. Zero
// or negative becomes 0 (no timeout).
func durationFromMs(ms int32) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}
