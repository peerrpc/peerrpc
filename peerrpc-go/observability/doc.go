// Package observability provides PeerRPC's structured logging, Prometheus
// metrics, and OpenTelemetry tracing as ready-to-install RPC interceptors.
//
// The three concerns share the same install pattern:
//
//	srv := rpc.NewServer(
//	    rpc.WithStreamServerInterceptors(
//	        observability.LogServerStream(log),
//	        observability.MetricsServerStream(metrics),
//	        observability.TraceServerStream(tracer),
//	    ),
//	)
//
// Each interceptor is independent: install only what you need. The
// defaults (zero-value Config) are usable; production callers tune
// them via the option-style constructors.
package observability

import (
	"log/slog"
)

// DefaultLogger is the package-level fallback used when callers do
// not pass an explicit *slog.Logger. It writes TextHandler output to
// stderr at Info level.
var DefaultLogger = slog.Default()

// common attribute keys used across log call sites. Centralizing
// them keeps downstream tooling (Loki, Datadog, etc.) querying a
// single canonical field name per concept.
const (
	keyMethod     = "method"
	keySequence   = "sequence"
	keyStatusCode = "status_code"
	keyDuration   = "duration_ms"
	keyPeerID     = "peer_id"
	keyService    = "service"
	keyError      = "err"
)
