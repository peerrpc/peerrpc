// Package bench contains PeerRPC performance benchmarks.
//
// All benchmarks use the in-process signaling backend on localhost
// so results reflect the PeerRPC stack's overhead (framing,
// marshaling, multiplexing, transport) without network latency.
//
// Each benchmark wires a Server + Client through signal.Local,
// waits for the DataChannel to open, then issues RPCs in a tight
// loop under Go's testing.B framework.
package bench

import (
	"context"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	"github.com/peerrpc/go/signal"
)

// echoUnaryHandler reads one request and sends it back prefixed
// with "echo:". It exercises the full Frame encode → transport →
// Frame decode → handler → ResponseFrame encode → transport →
// ResponseFrame decode path.
func echoUnaryHandler(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
	req, err := s.Recv()
	if err != nil {
		return rpc.Err(13, err)
	}
	if err := s.Send(append([]byte("echo:"), req...)); err != nil {
		return rpc.Err(13, err)
	}
	return rpc.OK()
}

// streamHandler emits n response chunks, each of the same size as
// the request.
func streamHandler(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
	req, err := s.Recv()
	if err != nil && err != io.EOF {
		return rpc.Err(13, err)
	}
	for i := 0; i < 100; i++ {
		if err := s.Send(req); err != nil {
			return rpc.Err(13, err)
		}
	}
	return rpc.OK()
}

// setup wires a Server (Answerer) and Client (Offerer) over the
// in-process signaling backend. Returns the live Client and a
// teardown function. Callers MUST defer teardown.
//
// setup takes ~100ms due to ICE gathering; benchmarks amortize this
// across many iterations via b.ResetTimer().
func setup(b *testing.B) (*rpc.Client, func()) {
	b.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	backend := signal.NewLocal()

	oSig, err := backend.Exchange(ctx, "bench", "offerer")
	if err != nil {
		cancel()
		b.Fatalf("offerer sig: %v", err)
	}
	aSig, err := backend.Exchange(ctx, "bench", "answerer")
	if err != nil {
		cancel()
		b.Fatalf("answerer sig: %v", err)
	}

	srv := rpc.NewServer()
	srv.RegisterService(rpc.ServiceDesc{
		ServiceName: "echo.Echo",
		Methods: []rpc.MethodDesc{
			{Method: "Unary", Kind: rpc.MethodKindUnary, Handler: echoUnaryHandler},
			{Method: "Stream", Kind: rpc.MethodKindServerStreaming, Handler: streamHandler},
		},
	})

	oPeer, err := peer.New(ctx, signal.RoleOfferer, peer.Config{})
	if err != nil {
		cancel()
		b.Fatalf("offerer New: %v", err)
	}
	aPeer, err := peer.New(ctx, signal.RoleAnswerer, peer.Config{})
	if err != nil {
		cancel()
		b.Fatalf("answerer New: %v", err)
	}

	// Answerer accepts + serves in the background.
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
		b.Fatalf("offerer Dial: %v", err)
	}

	cli := rpc.NewClient(och)
	go cli.Attach(ctx)

	teardown := func() {
		cancel()
		_ = oPeer.Close()
		_ = aPeer.Close()
		_ = oSig.Close()
		_ = aSig.Close()
	}
	return cli, teardown
}

// makePayload returns a byte slice of the given size filled with a
// repeating pattern. Deterministic so CI results are reproducible.
func makePayload(size int) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = byte(i % 256)
	}
	return b
}

// -----------------------------------------------------------------------
// Unary RPC benchmarks
// -----------------------------------------------------------------------

// BenchmarkUnaryRPC measures end-to-end Unary RPC latency at various
// payload sizes. Each iteration is one complete request → response
// cycle.
func BenchmarkUnaryRPC(b *testing.B) {
	cases := []struct {
		name string
		size int
	}{
		{"1KB", 1024},
		{"16KB", 16 * 1024},
		{"64KB", 64 * 1024},
		{"256KB", 256 * 1024},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			cli, teardown := setup(b)
			defer teardown()

			req := makePayload(c.size)
			ctx := context.Background()

			// Warmup.
			for i := 0; i < 10; i++ {
				_, st := cli.InvokeUnary(ctx, "/echo.Echo/Unary", req, nil)
				if st.Code != 0 {
					b.Fatalf("warmup %d: %+v", i, st)
				}
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				resp, st := cli.InvokeUnary(ctx, "/echo.Echo/Unary", req, nil)
				if st.Code != 0 {
					b.Fatalf("iter %d: %+v", i, st)
				}
				if len(resp) == 0 {
					b.Fatal("empty response")
				}
			}
		})
	}
}

// BenchmarkUnaryRPC_Concurrent measures throughput at varying
// concurrency levels using b.RunParallel.
func BenchmarkUnaryRPC_Concurrent(b *testing.B) {
	cases := []struct {
		name string
	}{
		{"c1"},
		{"c10"},
		{"c100"},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			cli, teardown := setup(b)
			defer teardown()

			req := makePayload(1024)
			ctx := context.Background()

			// Warmup.
			for i := 0; i < 10; i++ {
				_, _ = cli.InvokeUnary(ctx, "/echo.Echo/Unary", req, nil)
			}
			b.ResetTimer()

			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					_, st := cli.InvokeUnary(ctx, "/echo.Echo/Unary", req, nil)
					if st.Code != 0 {
						b.Errorf("status: %+v", st)
					}
				}
			})
		})
	}
}

// -----------------------------------------------------------------------
// Server Streaming benchmarks
// -----------------------------------------------------------------------

// BenchmarkServerStreaming measures the time to receive all chunks
// from a server-streaming RPC that emits 100 response messages.
func BenchmarkServerStreaming(b *testing.B) {
	cases := []struct {
		name string
		size int
	}{
		{"1KB_100chunks", 1024},
		{"16KB_100chunks", 16 * 1024},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			cli, teardown := setup(b)
			defer teardown()

			req := makePayload(c.size)
			ctx := context.Background()

			// Warmup.
			stream, _ := cli.InvokeServerStreaming(ctx, "/echo.Echo/Stream", req, nil)
			for {
				_, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				stream, st := cli.InvokeServerStreaming(ctx, "/echo.Echo/Stream", req, nil)
				if st.Code != 0 {
					b.Fatalf("open: %+v", st)
				}
				var count int
				for {
					msg, err := stream.Recv()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatalf("recv: %v", err)
					}
					if len(msg) != c.size {
						b.Fatalf("size: got %d want %d", len(msg), c.size)
					}
					count++
				}
				if count != 100 {
					b.Fatalf("chunks: got %d want 100", count)
				}
			}
		})
	}
}

// -----------------------------------------------------------------------
// Large payload (chunked) benchmark
// -----------------------------------------------------------------------

// BenchmarkLargePayload measures throughput for messages above the
// 256KB single-frame threshold, exercising the chunk split + reassembly
// path.
func BenchmarkLargePayload(b *testing.B) {
	cases := []struct {
		name string
		size int
	}{
		{"1MB", 1024 * 1024},
	}

	for _, c := range cases {
		b.Run(c.name, func(b *testing.B) {
			cli, teardown := setup(b)
			defer teardown()

			req := makePayload(c.size)
			ctx := context.Background()

			// Warmup.
			for i := 0; i < 3; i++ {
				resp, st := cli.InvokeUnary(ctx, "/echo.Echo/Unary", req, nil)
				if st.Code != 0 {
					b.Fatalf("warmup: %+v", st)
				}
				if len(resp) != c.size+5 {
					b.Fatalf("warmup size: %d", len(resp))
				}
			}
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				resp, st := cli.InvokeUnary(ctx, "/echo.Echo/Unary", req, nil)
				if st.Code != 0 {
					b.Fatalf("iter %d: %+v", i, st)
				}
				if len(resp) != c.size+5 {
					b.Fatalf("size: %d", len(resp))
				}
			}
		})
	}
}

// -----------------------------------------------------------------------
// Connection setup benchmark
// -----------------------------------------------------------------------

// BenchmarkConnectionSetup measures the time to establish a
// PeerConnection + DataChannel via the in-process signaling backend.
// This includes ICE gathering, SDP exchange, and DTLS handshake.
func BenchmarkConnectionSetup(b *testing.B) {
	// Connection setup is expensive (~100ms each); cap iterations.
	if b.N > 50 {
		b.N = 50
	}

	for i := 0; i < b.N; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		backend := signal.NewLocal()

		oSig, _ := backend.Exchange(ctx, fmt.Sprintf("cs-%d", i), "o")
		aSig, _ := backend.Exchange(ctx, fmt.Sprintf("cs-%d", i), "a")

		oPeer, _ := peer.New(ctx, signal.RoleOfferer, peer.Config{})
		aPeer, _ := peer.New(ctx, signal.RoleAnswerer, peer.Config{})

		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = aPeer.Accept(ctx, aSig)
		}()

		_, err := oPeer.Dial(ctx, oSig)
		if err != nil {
			cancel()
			b.Fatalf("Dial: %v", err)
		}
		wg.Wait()

		_ = oPeer.Close()
		_ = aPeer.Close()
		cancel()
	}
}
