// Command peerrpc-interop-ts runs a Go PeerRPC server that serves
// the TypeScript echo demo page + signal-server on the same HTTP
// endpoint. A browser loads the page, joins the signaling room as
// Answerer, and the Go server creates a DataChannel as Offerer.
// Once the DataChannel is open the browser issues Unary + Server
// Streaming RPCs against the Go EchoService.
//
// Usage:
//
//	peerrpc-interop-ts -addr :3000 -static /path/to/examples/ts/echo/dist
//
// Then open http://localhost:3000 in a browser.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	signalingpbconnect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/v1/signalingpbconnect"
	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	signalsdk "github.com/peerrpc/go/signal"
	"github.com/peerrpc/signal-server/server"
	"github.com/peerrpc/signal-server/store"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	staticDir := flag.String("static", "", "path to the TS demo's dist/ directory")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 1. In-process signaling backend.
	backend := signalsdk.NewLocal()

	// 2. EchoService registration.
	srv := rpc.NewServer()
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
					s.SetHeader(map[string][]string{"x-server": {"go-interop"}})
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
					if err != nil && err.Error() != "rpc: stream half-closed" {
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

	// 3. HTTP mux: signaling service + static files + health.
	signalSrv := server.New(store.NewMemory(), server.Config{Logger: logger})

	mux := http.NewServeMux()
	path, handler := signalingpbconnect.NewSignalingServiceHandler(signalSrv)
	mux.Handle(path, handler)

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	if *staticDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(*staticDir)))
		logger.Info("serving static files", "dir", *staticDir)
	}

	httpSrv := &http.Server{
		Addr:    *addr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	// 4. Launch the PeerRPC Offerer in the background. It joins room
	//    "interop" and waits for the browser Answerer.
	go func() {
		if err := runOfferer(context.Background(), backend, srv, logger); err != nil {
			logger.Error("offerer exited", "err", err)
		}
	}()

	// 5. Start HTTP server.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("interop server listening", "addr", *addr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("ListenAndServe", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
}

// runOfferer joins the signaling room as Offerer, waits for the
// browser Answerer, creates a DataChannel, and serves the EchoService
// on it. If the browser disconnects, it loops and waits for the next
// connection.
func runOfferer(ctx context.Context, backend signalsdk.Backend, srv *rpc.Server, logger *slog.Logger) error {
	peerID := "go-offerer-" + randomID()
	for {
		sig, err := backend.Exchange(ctx, "interop", peerID)
		if err != nil {
			return fmt.Errorf("signaling exchange: %w", err)
		}

		p, err := peer.New(ctx, signalsdk.RoleOfferer, peer.Config{
			NegotiationTimeout: 30 * time.Second,
		})
		if err != nil {
			sig.Close()
			return fmt.Errorf("peer.New: %w", err)
		}

		// Accept is concurrent with Dial; here we Dial because we're
		// the Offerer. The browser is the Answerer waiting for our
		// offer via the signaling stream.
		ch, err := p.Dial(ctx, sig)
		if err != nil {
			logger.Warn("offerer Dial failed", "err", err)
			p.Close()
			sig.Close()
			continue
		}

		logger.Info("DataChannel open, serving EchoService")
		if err := srv.Serve(ctx, ch); err != nil {
			logger.Warn("Serve returned", "err", err)
		}
		p.Close()
		sig.Close()
		logger.Info("DataChannel closed, waiting for next connection")
	}
}

func randomID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// touch unused import
var _ = connect.NewError
