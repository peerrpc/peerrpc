// Command peerrpc-echo-server is a PeerRPC server that connects to a
// standalone signal-server via the Connect scheme and serves the
// echo.Echo service with all four RPC types (Unary / Server-Streaming /
// Client-Streaming / Bidi).
//
// It is the server counterpart to examples/ts/echo (the browser client).
// Run a signal-server first, then this server, then open the browser
// echo page:
//
//	# terminal 1
//	make run-signal
//
//	# terminal 2
//	make run-echo-server
//
//	# terminal 3
//	make run-ts-echo
//
// Then connect from the browser echo page and exercise the RPCs.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/peerrpc/go/peerrpc"
	"github.com/peerrpc/go/rpc"
)

// registerEcho mounts the echo.Echo service with all four RPC types,
// matching the method paths the browser echo demo calls:
//
//	/echo.Echo/Echo    Unary
//	/echo.Echo/Stream  Server-Streaming
//	/echo.Echo/Collect Client-Streaming
//	/echo.Echo/Chat    Bidi-Streaming
func registerEcho(srv *rpc.Server) {
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
			{
				Method: "Collect",
				Kind:   rpc.MethodKindClientStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					var n, total int
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
				},
			},
			{
				Method: "Chat",
				Kind:   rpc.MethodKindBidiStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
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
				},
			},
		},
	})
}

func main() {
	signalAddr := flag.String("signal", "https://localhost:8443", "signal-server base URL (scheme required)")
	service := flag.String("service", "echo.Echo", "rendezvous service key")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Use the builder API so the signal-server URL keeps its scheme.
	// The target URI form (peerrpc+connect://host/svc) strips the
	// https:// prefix, and signal.NewRemote needs the full URL.
	ln, err := peerrpc.ListenContext(ctx).
		SignalAt(*signalAddr).
		Service(*service).
		Over(peerrpc.SchemeConnect).
		Listen()
	if err != nil {
		logger.Error("Listen", "err", err)
		os.Exit(1)
	}
	defer ln.Close()

	logger.Info("echo server listening",
		"signal", *signalAddr,
		"service", *service,
	)

	if err := ln.Serve(ctx, func() *rpc.Server {
		srv := rpc.NewServer()
		registerEcho(srv)
		return srv
	}); err != nil {
		logger.Error("Serve", "err", err)
		os.Exit(1)
	}
}
