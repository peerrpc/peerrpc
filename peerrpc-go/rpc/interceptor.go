// Interceptor chain for Server and Client, modeled on connect-go's
// `Interceptor` shape so existing middleware (auth, tracing, logging)
// ports over with minimal adjustment.
//
// Two flavors:
//   - Unary interceptors wrap a single request -> single response call.
//   - Stream interceptors wrap a long-lived *ServerStream / *ClientStream.
//
// On the Server, interceptors are installed via RegisterService options
// and form a chain ordered outer-to-inner: the first interceptor in
// the slice runs first.
//
// On the Client, interceptors are passed to NewClient and wrap every
// Invoke* call similarly.
package rpc

import (
	"context"
)

// UnaryHandler is the innermost receiver of a unary RPC after every
// interceptor has run.
type UnaryHandler func(ctx context.Context, req []byte) ([]byte, *Status)

// UnaryServerInterceptor wraps a UnaryHandler. next MUST be invoked
// exactly once to advance the chain.
type UnaryServerInterceptor func(ctx context.Context, method string, req []byte, next UnaryHandler) ([]byte, *Status)

// StreamHandler is the innermost receiver of a streaming RPC after
// every interceptor has run.
type StreamHandler func(ctx context.Context, stream *ServerStream) *Status

// StreamServerInterceptor wraps a StreamHandler.
type StreamServerInterceptor func(ctx context.Context, stream *ServerStream, info *StreamServerInfo, next StreamHandler) *Status

// StreamServerInfo carries metadata the stream interceptor may use
// (method path, kind) to make policy decisions.
type StreamServerInfo struct {
	Method string
	Kind   MethodKind
}

// UnaryClientInterceptor wraps a Client.UnaryInvoker.
type UnaryClientInterceptor func(ctx context.Context, method string, req []byte, invoker UnaryInvoker) ([]byte, *Status)

// UnaryInvoker is the bottom of the client interceptor chain — it
// actually calls the Server.
type UnaryInvoker func(ctx context.Context, method string, req []byte) ([]byte, *Status)

// chainUnaryServer assembles interceptors into a single handler.
// interceptors[0] is outermost. An empty slice returns next verbatim.
func chainUnaryServer(interceptors []UnaryServerInterceptor, info *StreamServerInfo, final UnaryHandler) UnaryHandler {
	if len(interceptors) == 0 {
		return final
	}
	// Build from innermost outward.
	h := final
	for i := len(interceptors) - 1; i >= 0; i-- {
		interceptor := interceptors[i]
		next := h
		h = func(ctx context.Context, req []byte) ([]byte, *Status) {
			return interceptor(ctx, info.Method, req, next)
		}
	}
	return h
}

// chainStreamServer assembles stream interceptors. interceptors[0] is
// outermost.
func chainStreamServer(interceptors []StreamServerInterceptor, info *StreamServerInfo, final StreamHandler) StreamHandler {
	if len(interceptors) == 0 {
		return final
	}
	h := final
	for i := len(interceptors) - 1; i >= 0; i-- {
		interceptor := interceptors[i]
		next := h
		h = func(ctx context.Context, s *ServerStream) *Status {
			return interceptor(ctx, s, info, next)
		}
	}
	return h
}

// chainUnaryClient assembles client interceptors. interceptors[0] is
// outermost.
func chainUnaryClient(interceptors []UnaryClientInterceptor, invoker UnaryInvoker) UnaryInvoker {
	if len(interceptors) == 0 {
		return invoker
	}
	v := invoker
	for i := len(interceptors) - 1; i >= 0; i-- {
		interceptor := interceptors[i]
		next := v
		v = func(ctx context.Context, method string, req []byte) ([]byte, *Status) {
			return interceptor(ctx, method, req, next)
		}
	}
	return v
}
