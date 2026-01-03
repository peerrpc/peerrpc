package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/peerrpc/go/observability"
	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	"github.com/peerrpc/go/signal"
	"github.com/prometheus/client_golang/prometheus"
)

// newEchoSrv wires a Server with logging + metrics interceptors
// installed around a Unary handler.
func newEchoSrv(t *testing.T, log *slog.Logger, reg prometheus.Registerer) *rpc.Server {
	t.Helper()
	m := observability.NewMetrics(reg)
	srv := rpc.NewServer(
		rpc.WithStreamServerInterceptors(
			observability.LogServerStream(log),
			observability.MetricsServerStream(m),
		),
	)
	srv.RegisterService(rpc.ServiceDesc{
		ServiceName: "echo.Echo",
		Methods: []rpc.MethodDesc{
			{
				Method: "Echo",
				Kind:   rpc.MethodKindUnary,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
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
	return srv
}

// spinUp wires a Server and Client over the in-process signaling
// backend and returns the live Client + teardown.
func spinUp(t *testing.T, srv *rpc.Server) (*rpc.Client, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	backend := signal.NewLocal()

	oSig, _ := backend.Exchange(ctx, "room", "offerer")
	aSig, _ := backend.Exchange(ctx, "room", "answerer")

	oPeer, _ := peer.New(ctx, signal.RoleOfferer, peer.Config{})
	aPeer, _ := peer.New(ctx, signal.RoleAnswerer, peer.Config{})

	go func() {
		ch, err := aPeer.Accept(ctx, aSig)
		if err != nil {
			return
		}
		_ = srv.Serve(ctx, ch)
	}()

	och, err := oPeer.Dial(ctx, oSig)
	if err != nil {
		cancel()
		t.Fatalf("Dial: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	cli := rpc.NewClient(och)
	go cli.Attach(ctx)

	return cli, func() {
		cancel()
		_ = oPeer.Close()
		_ = aPeer.Close()
		_ = oSig.Close()
		_ = aSig.Close()
	}
}

func TestLogServerStream_EmitsOneLinePerRPC(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	srv := newEchoSrv(t, logger, prometheus.NewRegistry())

	cli, teardown := spinUp(t, srv)
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, st := cli.InvokeUnary(ctx, "/echo.Echo/Echo", []byte("hi"), nil); st.Code != 0 {
		t.Fatalf("InvokeUnary: %+v", st)
	}

	// Drain the async log write.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), "rpc completed") {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	var entry map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &entry); err != nil {
		t.Fatalf("decode log: %v\nraw: %s", err, buf.String())
	}
	if entry["msg"] != "rpc completed" {
		t.Errorf("msg: %v", entry["msg"])
	}
	if entry["method"] != "/echo.Echo/Echo" {
		t.Errorf("method: %v", entry["method"])
	}
}

func TestMetricsServerStream_CountsRPCs(t *testing.T) {
	reg := prometheus.NewRegistry()
	srv := newEchoSrv(t, slog.New(slog.NewTextHandler(io.Discard, nil)), reg)

	cli, teardown := spinUp(t, srv)
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := 0; i < 3; i++ {
		if _, st := cli.InvokeUnary(ctx, "/echo.Echo/Echo", []byte("x"), nil); st.Code != 0 {
			t.Fatalf("call %d: %+v", i, st)
		}
	}

	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	var total float64
	for _, mf := range mfs {
		if mf.GetName() != "peerrpc_rpc_total" {
			continue
		}
		for _, m := range mf.GetMetric() {
			total += m.GetCounter().GetValue()
		}
	}
	if total != 3 {
		t.Fatalf("rpc_total = %v, want 3", total)
	}

	// Also assert the histogram has observations.
	for _, mf := range mfs {
		if mf.GetName() != "peerrpc_rpc_duration_seconds" {
			continue
		}
		for _, m := range mf.GetMetric() {
			if m.GetHistogram().GetSampleCount() != 3 {
				t.Errorf("duration count = %v, want 3", m.GetHistogram().GetSampleCount())
			}
		}
	}
}
