package observability

import (
	"context"
	"time"

	"github.com/peerrpc/go/rpc"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics holds the Prometheus collectors PeerRPC exports. Construct
// one per process with NewMetrics; the same instance is safe to
// install in both client and server interceptors.
//
// All collectors are registered against the supplied prometheus.Registerer
// (typically prometheus.DefaultRegisterer in a binary, or a fresh
// prometheus.NewRegistry() in tests). The constructor returns the
// instance so callers can also expose it via the standard
// promhttp.Handler.
type Metrics struct {
	registry prometheus.Registerer

	RPCTotal        *prometheus.CounterVec
	RPCDuration     *prometheus.HistogramVec
	StreamsInFlight *prometheus.GaugeVec
}

// NewMetrics constructs and registers the PeerRPC Prometheus
// collectors against reg. Nil reg falls back to
// prometheus.DefaultRegisterer.
func NewMetrics(reg prometheus.Registerer) *Metrics {
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	m := &Metrics{
		registry: reg,
		RPCTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "peerrpc_rpc_total",
			Help: "Total PeerRPC calls by method and final gRPC status code.",
		}, []string{"method", "status_code"}),
		RPCDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "peerrpc_rpc_duration_seconds",
			Help:    "PeerRPC call duration in seconds, by method.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method"}),
		StreamsInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "peerrpc_streams_in_flight",
			Help: "Number of PeerRPC streams currently being served, by method.",
		}, []string{"method"}),
	}
	reg.MustRegister(m.RPCTotal, m.RPCDuration, m.StreamsInFlight)
	return m
}

// Registry returns the registerer the metrics were installed against.
// Useful for callers that want to expose a custom registry and use
// the same one for promhttp.Handler.
func (m *Metrics) Registry() prometheus.Registerer { return m.registry }

// MetricsServerStream installs Metrics counters around a streaming
// handler. The in-flight gauge is incremented on entry and
// decremented on exit; the total counter and duration histogram
// observe the final status.
func MetricsServerStream(m *Metrics) rpc.StreamServerInterceptor {
	if m == nil {
		return func(ctx context.Context, s *rpc.ServerStream, info *rpc.StreamServerInfo, next rpc.StreamHandler) *rpc.Status {
			return next(ctx, s)
		}
	}
	return func(ctx context.Context, s *rpc.ServerStream, info *rpc.StreamServerInfo, next rpc.StreamHandler) *rpc.Status {
		m.StreamsInFlight.WithLabelValues(info.Method).Inc()
		defer m.StreamsInFlight.WithLabelValues(info.Method).Dec()

		start := time.Now()
		st := next(ctx, s)
		m.RPCTotal.WithLabelValues(info.Method, codeLabel(st)).Inc()
		m.RPCDuration.WithLabelValues(info.Method).Observe(time.Since(start).Seconds())
		return st
	}
}

// MetricsUnaryClient installs Metrics counters around a unary client
// invocation.
func MetricsUnaryClient(m *Metrics) rpc.UnaryClientInterceptor {
	if m == nil {
		return func(ctx context.Context, method string, req []byte, next rpc.UnaryInvoker) ([]byte, *rpc.Status) {
			return next(ctx, method, req)
		}
	}
	return func(ctx context.Context, method string, req []byte, next rpc.UnaryInvoker) ([]byte, *rpc.Status) {
		start := time.Now()
		resp, st := next(ctx, method, req)
		m.RPCTotal.WithLabelValues(method, codeLabel(st)).Inc()
		m.RPCDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())
		return resp, st
	}
}

// codeLabel maps a Status code to the canonical 0-OK / N-NAME label
// Prometheus scrapers expect. We use the numeric form for compactness;
// the dashboards typically join against a code table.
func codeLabel(st *rpc.Status) string {
	if st == nil {
		return "0"
	}
	switch st.Code {
	case 0:
		return "0"
	default:
		return itoa(st.Code)
	}
}

// itoa avoids importing strconv just for one helper.
func itoa(i int32) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [12]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
