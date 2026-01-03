package observability

import (
	"context"

	"github.com/peerrpc/go/rpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// traceMetadataKey is the header name under which OpenTelemetry
// trace context is propagated across PeerRPC metadata. We use the
// standard W3C traceparent format; OTel's TextMapPropagator injects
// and extracts it from a carrier.
//
// PeerRPC collapses every value into a single string per key in the
// inbound direction, so the propagator sees exactly one entry per
// header.
const traceMetadataKey = "traceparent"

// TraceServerStream installs an OpenTelemetry server-side tracer
// around the streaming handler.
//
// On entry the interceptor extracts a SpanContext from the incoming
// ServerStream.Header() (which the client must have populated via
// TraceUnaryClient). It starts a span named "peerrpc.<method>",
// records the final status as the span status, and ends the span on
// handler return.
//
// The global TracerProvider / TextMapPropagator are used so callers
// configure them once at process start via otel.SetTracerProvider
// and otel.SetTextMapPropagator.
func TraceServerStream(tp trace.TracerProvider) rpc.StreamServerInterceptor {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	tracer := tp.Tracer("peerrpc")
	propagator := otel.GetTextMapPropagator()
	if propagator == nil {
		propagator = propagation.TraceContext{}
	}

	return func(ctx context.Context, s *rpc.ServerStream, info *rpc.StreamServerInfo, next rpc.StreamHandler) *rpc.Status {
		// Extract from incoming metadata.
		carrier := headerCarrier(s.IncomingHeader())
		extractedCtx := propagator.Extract(ctx, carrier)

		spanCtx, span := tracer.Start(extractedCtx, "peerrpc."+shortMethod(info.Method),
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("rpc.method", info.Method)),
		)
		defer span.End()

		// Replace the stream's handler context so the handler's
		// trace.SpanFromContext(ctx) sees this span and any child
		// spans it creates land below us in the trace tree.
		s.WithContext(spanCtx)

		st := next(spanCtx, s)
		if st != nil && st.Code != 0 {
			span.SetStatus(codes.Error, st.Message)
			span.SetAttributes(attribute.Int64("rpc.status_code", int64(st.Code)))
		} else {
			span.SetStatus(codes.Ok, "")
		}
		return st
	}
}

// TraceUnaryClient injects the active trace context into outgoing
// PeerRPC metadata so the server-side interceptor can join the trace.
//
// Must run BEFORE the bottom of the chain fills in Call.metadata
// from the context. We attach the trace header via the outgoing
// header plumbing installed by Client.InvokeUnary.
func TraceUnaryClient(tp trace.TracerProvider) rpc.UnaryClientInterceptor {
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	tracer := tp.Tracer("peerrpc")
	propagator := otel.GetTextMapPropagator()
	if propagator == nil {
		propagator = propagation.TraceContext{}
	}

	return func(ctx context.Context, method string, req []byte, next rpc.UnaryInvoker) ([]byte, *rpc.Status) {
		ctx, span := tracer.Start(ctx, "peerrpc."+shortMethod(method),
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(attribute.String("rpc.method", method)),
		)
		defer span.End()

		// Inject the current SpanContext into a carrier and forward
		// it as outgoing PeerRPC metadata. The bottom of the chain
		// picks the metadata up from ctx via WithOutgoingHeader.
		carrier := mapCarrier{}
		propagator.Inject(ctx, carrier)
		outgoing := rpc.OutgoingHeaderFromCtx(ctx)
		if outgoing == nil {
			outgoing = map[string][]string{}
		}
		for k, v := range carrier {
			outgoing[k] = append(outgoing[k], v[0])
		}
		ctx = rpc.WithOutgoingHeader(ctx, outgoing)

		resp, st := next(ctx, method, req)
		if st != nil && st.Code != 0 {
			span.SetStatus(codes.Error, st.Message)
			span.SetAttributes(attribute.Int64("rpc.status_code", int64(st.Code)))
		} else {
			span.SetStatus(codes.Ok, "")
		}
		return resp, st
	}
}

// shortMethod strips the leading "/" from a fully-qualified method
// path so the span name is human-friendly ("/echo.Echo/Echo" ->
// "echo.Echo/Echo").
func shortMethod(method string) string {
	for i := 0; i < len(method); i++ {
		if method[i] == '/' {
			return method[i+1:]
		}
	}
	return method
}

// headerCarrier adapts a PeerRPC metadata map to OTel's
// TextMapCarrier interface for extraction.
type headerCarrier map[string][]string

func (c headerCarrier) Get(key string) string {
	if vs, ok := c[key]; ok && len(vs) > 0 {
		return vs[0]
	}
	return ""
}

func (c headerCarrier) Set(key, value string) { c[key] = []string{value} }

func (c headerCarrier) Keys() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}

// mapCarrier is the inject counterpart; it is identical to
// headerCarrier but exposed as a distinct type so the API is
// symmetric.
type mapCarrier = headerCarrier
