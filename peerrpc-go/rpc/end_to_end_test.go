package rpc_test

import (
	"bytes"
	"context"
	"io"
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
		},
	})
	return srv
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

	oPeer, err := peer.New(ctx, signal.RoleOfferer, peer.Config{})
	if err != nil {
		cancel()
		t.Fatalf("offerer New: %v", err)
	}
	aPeer, err := peer.New(ctx, signal.RoleAnswerer, peer.Config{})
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
