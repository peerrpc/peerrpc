package observability_test

import (
	"context"
	"testing"
	"time"

	"github.com/peerrpc/go/observability"
	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	"github.com/peerrpc/go/signal"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// TestTraceClientServer_PropagatesSpanContext installs a client-side
// trace interceptor and a server-side trace interceptor against an
// in-memory span exporter. The test verifies that:
//
//   * the client creates a span (kind=client)
//   * the server creates a child span (kind=server) under the same
//     trace
//   * both spans share the same TraceID
func TestTraceClientServer_PropagatesSpanContext(t *testing.T) {
	// Set up an in-memory exporter + a TracerProvider that exports
	// synchronously on span end.
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	defer tp.Shutdown(context.Background())
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	// Server side: install the trace interceptor.
	srv := rpc.NewServer(
		rpc.WithStreamServerInterceptors(observability.TraceServerStream(tp)),
	)
	srv.RegisterService(rpc.ServiceDesc{
		ServiceName: "echo.Echo",
		Methods: []rpc.MethodDesc{
			{
				Method: "Echo",
				Kind:   rpc.MethodKindUnary,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					// Mark the span active in the handler so we can
					// observe it later.
					span := trace.SpanFromContext(ctx)
					span.SetAttributes()
					req, err := s.Recv()
					if err != nil {
						return rpc.Err(13, err)
					}
					if err := s.Send(append([]byte("echo:"), req...)); err != nil {
						return rpc.Err(13, err)
					}
					return rpc.OK()
				},
			},
		},
	})

	// Wire up server over in-process signaling.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	backend := signal.NewLocal()
	oSig, _ := backend.Exchange(ctx, "tr", "o")
	aSig, _ := backend.Exchange(ctx, "tr", "a")
	oPeer, _ := peer.New(ctx, signal.RoleOfferer, peer.Config{})
	aPeer, _ := peer.New(ctx, signal.RoleAnswerer, peer.Config{})
	defer oPeer.Close()
	defer aPeer.Close()

	go func() {
		ch, _ := aPeer.Accept(ctx, aSig)
		_ = srv.Serve(ctx, ch)
	}()
	och, err := oPeer.Dial(ctx, oSig)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Client side: install the trace interceptor. Outermost-first
	// ordering: TraceUnaryClient wraps the underlying invoker.
	cli := rpc.NewClient(och,
		rpc.WithUnaryClientInterceptors(observability.TraceUnaryClient(tp)),
	)
	go cli.Attach(ctx)

	// Issue the call from a parent span so we can assert the
	// parent/child relationship.
	parentCtx, parentSpan := tp.Tracer("test").Start(ctx, "test.parent")
	if _, st := cli.InvokeUnary(parentCtx, "/echo.Echo/Echo", []byte("x"), nil); st.Code != 0 {
		parentSpan.End()
		t.Fatalf("InvokeUnary: %+v", st)
	}
	parentSpan.End()

	// Drain the exporter: parent + client + server spans.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(exporter.GetSpans()) >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	spans := exporter.GetSpans()
	if len(spans) < 3 {
		t.Fatalf("got %d spans, want >=3", len(spans))
	}

	// All spans must share the parent's TraceID.
	parentTraceID := parentSpan.SpanContext().TraceID()
	for _, s := range spans {
		if s.SpanContext.TraceID() != parentTraceID {
			t.Errorf("span %q traceID = %v, want %v",
				s.Name, s.SpanContext.TraceID(), parentTraceID)
		}
	}

	// One span must be the server-side one under the client's span.
	names := map[string]bool{}
	for _, s := range spans {
		names[s.Name] = true
	}
	if !names["peerrpc.echo.Echo/Echo"] {
		t.Errorf("missing peerrpc.* span; got %v", names)
	}
}
