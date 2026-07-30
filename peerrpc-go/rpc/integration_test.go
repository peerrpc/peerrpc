package rpc_test

// Integration tests for call-timing and 1-to-many scenarios. These
// exercise the real pion WebRTC stack (via spinUp/spinUpPair) so they
// catch issues the mock-based tests cannot: sequence allocation under
// concurrency, server-streaming cancellation, deadline mid-stream,
// and concurrent fan-in from multiple clients.

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
	"github.com/peerrpc/go/transport"
)

// spinUpPair wires one Server (Answerer) and one Client (Offerer) over
// an isolated in-process signaling backend keyed by service, so many
// pairs can coexist without broadcast cross-talk. Returns the live
// Client and a teardown function.
func spinUpPair(t *testing.T, srv *rpc.Server, service string) (*rpc.Client, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	backend := signal.NewLocal()

	oSig, err := backend.Exchange(ctx, service, "offerer-"+service)
	if err != nil {
		cancel()
		t.Fatalf("offerer sig: %v", err)
	}
	aSig, err := backend.Exchange(ctx, service, "answerer-"+service)
	if err != nil {
		cancel()
		t.Fatalf("answerer sig: %v", err)
	}

	oPeer, err := peer.New(ctx, signal.RoleClient, peer.Config{})
	if err != nil {
		cancel()
		t.Fatalf("offerer New: %v", err)
	}
	aPeer, err := peer.New(ctx, signal.RoleServer, peer.Config{})
	if err != nil {
		cancel()
		t.Fatalf("answerer New: %v", err)
	}

	accepted := make(chan *transport.Channel, 1)
	go func() {
		ch, err := aPeer.Accept(ctx, aSig)
		if err != nil {
			accepted <- nil
			return
		}
		accepted <- ch
		go srv.Serve(ctx, ch)
	}()

	och, err := oPeer.Dial(ctx, oSig)
	if err != nil {
		cancel()
		t.Fatalf("offerer Dial: %v", err)
	}
	select {
	case ach := <-accepted:
		if ach == nil {
			cancel()
			t.Fatalf("answerer Accept failed")
		}
	case <-time.After(10 * time.Second):
		cancel()
		t.Fatalf("answerer Accept never returned")
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

// TestIntegration_ConcurrentUnary verifies that one client can issue
// many concurrent unary RPCs whose responses are matched back to the
// right sequence (no cross-wiring).
func TestIntegration_ConcurrentUnary(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUpPair(t, srv, "concurrent-unary")
	defer teardown()

	const n = 5
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			req := []byte(fmt.Sprintf("req-%d", i))
			resp, status := cli.InvokeUnary(context.Background(), "/echo.Echo/Unary", req, nil)
			if status.Code != 0 {
				t.Errorf("req %d: status %d %s", i, status.Code, status.Message)
				return
			}
			want := []byte("echo:req-" + fmt.Sprint(i))
			if string(resp) != string(want) {
				t.Errorf("req %d: got %q want %q", i, resp, want)
			}
		}()
	}
	wg.Wait()
}

// TestIntegration_StreamCancellation verifies that canceling the client
// context during a server-streaming RPC stops the stream (the client
// observes ctx.Err() and the server stops sending).
func TestIntegration_StreamCancellation(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUpPair(t, srv, "stream-cancel")
	defer teardown()

	ctx, cancel := context.WithCancel(context.Background())
	stream, status := cli.InvokeServerStreaming(ctx, "/echo.Echo/Stream", []byte("flow"), nil)
	if status.Code != 0 {
		t.Fatalf("InvokeServerStreaming status: %d", status.Code)
	}

	// Read one chunk to confirm the stream started, then cancel.
	_, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	cancel()

	// After cancel, Recv should return a ctx error (or EOF) promptly.
	deadline := time.After(3 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("Recv did not return after cancel within 3s")
		default:
		}
		_, err := stream.Recv()
		if err != nil {
			// ctx canceled or EOF — both acceptable.
			break
		}
	}
}

// TestIntegration_DeadlineMidStream verifies that a client deadline
// firing during a server-streaming RPC terminates the stream rather
// than blocking forever.
func TestIntegration_DeadlineMidStream(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUpPair(t, srv, "stream-deadline")
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	stream, status := cli.InvokeServerStreaming(ctx, "/echo.Echo/Stream", []byte("flow"), nil)
	if status.Code != 0 {
		t.Fatalf("InvokeServerStreaming status: %d", status.Code)
	}

	// Drain until the deadline expires; Recv must return an error.
	for {
		_, err := stream.Recv()
		if err != nil {
			break // deadline hit (context.DeadlineExceeded) or transport
		}
	}
}

// TestIntegration_ConcurrentFanIn spins up N independent client-server
// pairs (each on its own signal service, sharing the same *rpc.Server
// registration set) and has them issue unary RPCs concurrently. This
// exercises the server's per-stream goroutine spawning across many
// channels simultaneously.
func TestIntegration_ConcurrentFanIn(t *testing.T) {
	// A single Server registration set, reused across pairs. Each pair
	// runs srv.Serve on its own channel; the multiplexer keys streams
	// by sequence within a channel, so sharing methods is safe.
	srv := newEchoServer()

	const n = 3
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			service := fmt.Sprintf("fanin-%d", i)
			cli, teardown := spinUpPair(t, srv, service)
			defer teardown()

			req := []byte(fmt.Sprintf("client-%d", i))
			resp, status := cli.InvokeUnary(context.Background(), "/echo.Echo/Unary", req, nil)
			if status.Code != 0 {
				t.Errorf("client %d: status %d", i, status.Code)
				return
			}
			want := []byte("echo:client-" + fmt.Sprint(i))
			if string(resp) != string(want) {
				t.Errorf("client %d: got %q want %q", i, resp, want)
			}
		}()
	}
	wg.Wait()
}

// TestIntegration_BidiUnordered verifies that bidi Send/Recv can
// tolerate the server responding before the client's next Send, i.e.
// the stream is not deadlocked by strict ping-pong ordering.
func TestIntegration_BidiUnordered(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUpPair(t, srv, "bidi-unordered")
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, status := cli.InvokeBidiStreaming(ctx, "/echo.Echo/Chat", nil, nil)
	if status.Code != 0 {
		t.Fatalf("InvokeBidiStreaming status: %d", status.Code)
	}

	// Send 3 messages, then drain 3 acks. The server echoes each on
	// receipt, so by the time we finish sending, all acks are buffered.
	for i := 1; i <= 3; i++ {
		if err := stream.Send([]byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	for i := 1; i <= 3; i++ {
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		want := fmt.Sprintf("ack %d: m%d", i, i)
		if string(msg) != want {
			t.Errorf("Recv %d: got %q want %q", i, msg, want)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	// After half-close the server returns OK -> EOF.
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("expected EOF after CloseSend, got %v", err)
	}
}
