package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/peerrpc/go/rpc"
)

// LogServerStream returns a stream interceptor that emits one Info
// log line per RPC completion with method, duration, and final
// status code.
//
// The logger defaults to DefaultLogger; pass a configured *slog.Logger
// to route logs to a different sink (JSON handler, file, etc.).
func LogServerStream(logger *slog.Logger) rpc.StreamServerInterceptor {
	if logger == nil {
		logger = DefaultLogger
	}
	return func(ctx context.Context, s *rpc.ServerStream, info *rpc.StreamServerInfo, next rpc.StreamHandler) *rpc.Status {
		start := time.Now()
		st := next(ctx, s)
		logger.InfoContext(ctx, "rpc completed",
			keyMethod, info.Method,
			keyDuration, time.Since(start).Milliseconds(),
			keyStatusCode, statusCode(st),
		)
		return st
	}
}

// LogUnaryClient returns a unary client interceptor that emits one
// log line per invocation with method, duration, and status code.
func LogUnaryClient(logger *slog.Logger) rpc.UnaryClientInterceptor {
	if logger == nil {
		logger = DefaultLogger
	}
	return func(ctx context.Context, method string, req []byte, next rpc.UnaryInvoker) ([]byte, *rpc.Status) {
		start := time.Now()
		resp, st := next(ctx, method, req)
		logger.InfoContext(ctx, "rpc invoked",
			keyMethod, method,
			keyDuration, time.Since(start).Milliseconds(),
			keyStatusCode, statusCode(st),
		)
		return resp, st
	}
}

// statusCode extracts the numeric gRPC code from a Status, returning
// 0 (OK) for nil.
func statusCode(st *rpc.Status) int32 {
	if st == nil {
		return 0
	}
	return st.Code
}
