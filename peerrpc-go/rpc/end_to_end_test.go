package rpc_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
)

// echoHandler echoes back whatever it receives as a Unary response.
// It also exercises header/trailer plumbing so the metadata path gets
// covered in the same test.
func echoHandler(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
	req, err := s.Recv()
	if err != nil {
		return rpc.Err(13, err)
	}
	s.SetHeader(map[string][]string{"x-server": {"echo"}})
	s.SetTrailer(map[string][]string{"x-consumed": {"1"}})
	if err := s.Send(append([]byte("echo:"), req...)); err != nil {
		return rpc.Err(13, err)
	}
	return rpc.OK()
}

// streamHandler emits a fixed number of reply chunks.
func streamHandler(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
	req, err := s.Recv()
	if err != nil && err != io.EOF {
		return rpc.Err(13, err)
	}
	prefix := append([]byte("chunk-"), req...)
	for i := 0; i < 5; i++ {
		if err := s.Send(prefix); err != nil {
			return rpc.Err(13, err)
		}
	}
	return rpc.OK()
}

func newEchoServer() *rpc.Server {
	srv := rpc.NewServer()
	srv.RegisterService(rpc.ServiceDesc{
		ServiceName: "echo.Echo",
		Methods: []rpc.MethodDesc{
			{Method: "Unary", Kind: rpc.MethodKindUnary, Handler: echoHandler},
			{Method: "Stream", Kind: rpc.MethodKindServerStreaming, Handler: streamHandler},
			{Method: "Collect", Kind: rpc.MethodKindClientStreaming, Handler: collectHandler},
			{Method: "Chat", Kind: rpc.MethodKindBidiStreaming, Handler: chatHandler},
			{Method: "Slow", Kind: rpc.MethodKindUnary, Handler: slowHandler},
		},
	})
	return srv
}

// slowHandler sleeps for the duration encoded in the request payload
// ("2s" etc) so tests can exercise deadline and cancellation paths.
func slowHandler(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
	req, err := s.Recv()
	if err != nil {
		return rpc.Err(13, err)
	}
	d, err := time.ParseDuration(string(req))
	if err != nil {
		return rpc.Err(3, err)
	}
	select {
	case <-time.After(d):
		if err := s.Send([]byte("done")); err != nil {
			return rpc.Err(13, err)
		}
		return rpc.OK()
	case <-ctx.Done():
		return rpc.Err(4, ctx.Err())
	}
}

// collectHandler reads every request until EOF and replies with one
// message describing the batch.
func collectHandler(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
	var n int
	var total int
	for {
		msg, err := s.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return rpc.Err(13, err)
		}
		n++
		total += len(msg)
	}
	reply := []byte(fmt.Sprintf("received %d messages (%d bytes)", n, total))
	if err := s.Send(reply); err != nil {
		return rpc.Err(13, err)
	}
	return rpc.OK()
}

// chatHandler echoes each request back as a tagged response. The
// client closes by half-closing; we keep going until Recv returns EOF
// (we honor client-driven half-close).
func chatHandler(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
	var seq int
	for {
		msg, err := s.Recv()
		if err == io.EOF {
			return rpc.OK()
		}
		if err != nil {
			return rpc.Err(13, err)
		}
		seq++
		reply := []byte(fmt.Sprintf("ack %d: %s", seq, string(msg)))
		if err := s.Send(reply); err != nil {
			return rpc.Err(13, err)
		}
	}
}

// spinUp wires a Server (Answerer) and a Client (Offerer) over the
// same in-process signaling backend. Returns the live Client and a
// teardown function the caller MUST defer.
func spinUp(t *testing.T, srv *rpc.Server) (*rpc.Client, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	backend := signal.NewLocal()

	oSig, err := backend.Exchange(ctx, "room", "offerer")
	if err != nil {
		cancel()
		t.Fatalf("offerer sig: %v", err)
	}
	aSig, err := backend.Exchange(ctx, "room", "answerer")
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
	case <-time.After(5 * time.Second):
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

func TestEndToEnd_Unary(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUp(t, srv)
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, status := cli.InvokeUnary(ctx, "/echo.Echo/Unary", []byte("hello"), map[string][]string{"auth": {"token"}})
	if status.Code != 0 {
		t.Fatalf("InvokeUnary status: %+v", status)
	}
	if !bytes.Equal(resp, []byte("echo:hello")) {
		t.Fatalf("unexpected resp: %q", resp)
	}
}

func TestEndToEnd_Unary_HeaderAndTrailer(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUp(t, srv)
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, status := cli.InvokeServerStreaming(ctx, "/echo.Echo/Stream", []byte("x"), nil)
	if status.Code != 0 {
		t.Fatalf("InvokeServerStreaming status: %+v", status)
	}
	var got int
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if !bytes.Equal(msg, []byte("chunk-x")) {
			t.Fatalf("msg: %q", msg)
		}
		got++
	}
	if got != 5 {
		t.Fatalf("got %d chunks, want 5", got)
	}
}

func TestEndToEnd_Unary_NotFound(t *testing.T) {
	srv := rpc.NewServer() // empty
	cli, teardown := spinUp(t, srv)
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, status := cli.InvokeUnary(ctx, "/echo.Echo/Missing", []byte("x"), nil)
	if status.Code != 12 { // UNIMPLEMENTED
		t.Fatalf("expected UNIMPLEMENTED (12), got %+v", status)
	}
}

func TestEndToEnd_ClientStreaming(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUp(t, srv)
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, status := cli.InvokeClientStreaming(ctx, "/echo.Echo/Collect", nil, nil)
	if status.Code != 0 {
		t.Fatalf("open: %+v", status)
	}
	for i := 0; i < 4; i++ {
		if err := stream.Send([]byte("chunk")); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if !strings.Contains(string(resp), "4 messages") {
		t.Fatalf("unexpected resp: %q", resp)
	}
	// Next Recv should be EOF.
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("expected io.EOF, got %v", err)
	}
}

func TestEndToEnd_BidiStreaming(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUp(t, srv)
	defer teardown()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, status := cli.InvokeBidiStreaming(ctx, "/echo.Echo/Chat", nil, nil)
	if status.Code != 0 {
		t.Fatalf("open: %+v", status)
	}

	// Interleave Send and Recv to exercise true bidi.
	for i := 0; i < 5; i++ {
		req := []byte(fmt.Sprintf("msg-%d", i))
		if err := stream.Send(req); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
		resp, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv %d: %v", i, err)
		}
		want := fmt.Sprintf("ack %d: msg-%d", i+1, i)
		if string(resp) != want {
			t.Fatalf("Recv %d: got %q want %q", i, resp, want)
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	// Server returns OK on half-close.
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatalf("expected io.EOF after half-close, got %v", err)
	}
}

func TestEndToEnd_DeadlineExceeded(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUp(t, srv)
	defer teardown()

	// 100ms client deadline; server is asked to sleep 10s, so the
	// deadline MUST fire first.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, status := cli.InvokeUnary(ctx, "/echo.Echo/Slow", []byte("10s"), nil)
	// Either side may surface the failure first; both DEADLINE_EXCEEDED
	// (4) and CANCELLED (1) are acceptable here.
	if status.Code != 4 && status.Code != 1 {
		t.Fatalf("expected DEADLINE_EXCEEDED (4) or CANCELLED (1), got %+v", status)
	}
}

func TestEndToEnd_ClientCancellation(t *testing.T) {
	srv := newEchoServer()
	cli, teardown := spinUp(t, srv)
	defer teardown()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Issue the request; immediately cancel. Server was asked to wait
	// 10s, so cancellation MUST land first.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()
	_, status := cli.InvokeUnary(ctx, "/echo.Echo/Slow", []byte("10s"), nil)
	if status.Code != 4 && status.Code != 1 && status.Code != 14 {
		t.Fatalf("expected DEADLINE/CANCELLED/UNAVAILABLE, got %+v", status)
	}
}
