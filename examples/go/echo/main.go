// Command peerrpc-echo is the Phase-1 PeerRPC end-to-end demo.
//
// It launches one Server (Answerer) and one Client (Offerer) inside
// the same process, wires them together via the in-process signaling
// backend, registers an EchoService exposing both a Unary and a
// Server-Streaming method, then issues one Unary and one streaming
// call from the Client. Successful execution demonstrates that the
// full Phase-1 stack composes: protocol + transport + peer + signal +
// rpc layers.
//
// Usage:
//
//	go run ./examples/go/echo
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	"github.com/peerrpc/go/signal"
)

// echoService implements both the Unary and Server-Streaming shapes
// for the demo. Real services would generate this from proto; here we
// hand-write it because Phase 1 has no Echo proto yet.
func registerEcho(srv *rpc.Server) {
	srv.RegisterService(rpc.ServiceDesc{
		ServiceName: "echo.Echo",
		Methods: []rpc.MethodDesc{
			{
				Method: "Unary",
				Kind:   rpc.MethodKindUnary,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					req, err := s.Recv()
					if err != nil {
						return rpc.Err(13, err)
					}
					s.SetHeader(map[string][]string{"x-server": {"peerrpc-echo"}})
					s.SetTrailer(map[string][]string{"x-consumed": {"1"}})
					if err := s.Send(append([]byte("echo: "), req...)); err != nil {
						return rpc.Err(13, err)
					}
					return rpc.OK()
				},
			},
			{
				Method: "Stream",
				Kind:   rpc.MethodKindServerStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					req, err := s.Recv()
					if err != nil && err != io.EOF {
						return rpc.Err(13, err)
					}
					for i := 1; i <= 5; i++ {
						msg := []byte(fmt.Sprintf("chunk %d for %q", i, string(req)))
						if err := s.Send(msg); err != nil {
							return rpc.Err(13, err)
						}
					}
					return rpc.OK()
				},
			},
		},
	})
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1. Wire the in-process signaling backend and the two peers.
	backend := signal.NewLocal()
	oSig, err := backend.Exchange(ctx, "demo-room", "offerer")
	must(log, "offerer signaling", err)
	defer oSig.Close()
	aSig, err := backend.Exchange(ctx, "demo-room", "answerer")
	must(log, "answerer signaling", err)
	defer aSig.Close()

	oPeer, err := peer.New(ctx, signal.RoleClient, peer.Config{})
	must(log, "offerer New", err)
	defer oPeer.Close()
	aPeer, err := peer.New(ctx, signal.RoleServer, peer.Config{})
	must(log, "answerer New", err)
	defer aPeer.Close()

	// 2. Server (answerer) registers EchoService and accepts the
	//    connection; once the channel is open it serves forever (or
	//    until ctx cancels).
	srv := rpc.NewServer()
	registerEcho(srv)
	accepted := make(chan error, 1)
	go func() {
		ch, err := aPeer.Accept(ctx, aSig)
		accepted <- err
		if err != nil {
			return
		}
		_ = srv.Serve(ctx, ch)
	}()

	// 3. Client (offerer) dials.
	och, err := oPeer.Dial(ctx, oSig)
	must(log, "offerer Dial", err)

	select {
	case err := <-accepted:
		must(log, "answerer Accept", err)
	case <-time.After(5 * time.Second):
		log.Error("answerer Accept timed out")
		os.Exit(1)
	}

	cli := rpc.NewClient(och)
	go func() { _ = cli.Attach(ctx) }()

	// 4. Exercise Unary.
	resp, status := cli.InvokeUnary(ctx, "/echo.Echo/Unary", []byte("hello, peerrpc"), nil)
	mustStatus(log, "Unary status", status)
	log.Info("Unary OK", "response", string(resp))

	// 5. Exercise Server-Streaming.
	stream, status := cli.InvokeServerStreaming(ctx, "/echo.Echo/Stream", []byte("ping"), nil)
	mustStatus(log, "Stream status", status)
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Error("Stream Recv failed", "err", err)
			os.Exit(1)
		}
		log.Info("Stream chunk", "message", string(msg))
	}
	log.Info("Phase 1 demo complete")
}

func must(log *slog.Logger, what string, err error) {
	if err != nil {
		log.Error(what+" failed", "err", err)
		os.Exit(1)
	}
	log.Info(what + " OK")
}

func mustStatus(log *slog.Logger, what string, s *rpc.Status) {
	if s == nil || s.Code == 0 {
		return
	}
	log.Error(what+" non-OK", "code", s.Code, "message", s.Message)
	os.Exit(1)
}
