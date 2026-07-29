// Command peerrpc-facade-demo is the PeerRPC end-to-end demo using
// the new top-level peerrpc.Dial / peerrpc.Listen facade.
//
// Compared to the v1 examples/go/echo demo (which manually wires
// signal.Backend + peer.Peer + rpc.Server + goroutines), this
// version does the same work in roughly a dozen lines and exposes
// no magic strings (no "demo-room", no "offerer"/"answerer").
//
// Run:
//
//	go run ./examples/go/facade
package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/peerrpc/go/peerrpc"
	"github.com/peerrpc/go/rpc"
)

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
					_ = req
					for i := 1; i <= 5; i++ {
						if err := s.Send([]byte("chunk")); err != nil {
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

	target := "peerrpc+local:///echo.Echo"

	// Server side: one call, returns immediately; Serve blocks in
	// the goroutine until ctx cancels.
	ln, err := peerrpc.Listen(ctx, target)
	if err != nil {
		log.Error("Listen", "err", err)
		os.Exit(1)
	}
	defer ln.Close()
	go func() {
		_ = ln.Serve(ctx, func() *rpc.Server {
			srv := rpc.NewServer()
			registerEcho(srv)
			return srv
		})
	}()

	// Client side: one Dial. Compare with the 11-step v1 dance.
	conn, err := peerrpc.Dial(ctx, target)
	if err != nil {
		log.Error("Dial", "err", err)
		os.Exit(1)
	}
	defer conn.Close()
	log.Info("connected", "peer_id", conn.PeerID())

	// Unary.
	resp, status := conn.InvokeUnary(ctx, "/echo.Echo/Unary", []byte("hello, peerrpc"), nil)
	if status != nil && status.Code != 0 {
		log.Error("Unary status", "code", status.Code, "msg", status.Message)
		os.Exit(1)
	}
	log.Info("Unary OK", "response", string(resp))

	// Server-streaming.
	stream, status := conn.InvokeServerStreaming(ctx, "/echo.Echo/Stream", []byte("ping"), nil)
	if status != nil && status.Code != 0 {
		log.Error("Stream status", "code", status.Code)
		os.Exit(1)
	}
	count := 0
	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Error("Stream Recv", "err", err)
			os.Exit(1)
		}
		count++
	}
	log.Info("Stream OK", "chunks", count)
	log.Info("facade demo complete")
}
