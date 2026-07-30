// Command peerrpc-interop-ts runs a Go PeerRPC server that serves
// the TypeScript echo demo page + signal-server on the same HTTP
// endpoint. A browser loads the page, joins the signaling room as
// Answerer, and the Go server creates a DataChannel as Offerer.
// Once the DataChannel is open the browser issues Unary + Server
// Streaming RPCs against the Go EchoService.
//
// Usage:
//
//	peerrpc-interop-ts -addr :3000 -static /path/to/examples/echo-ts/dist
//
// Then open http://localhost:3000 in a browser.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	"github.com/peerrpc/go/signalserver"
	"github.com/peerrpc/go/signalserver/store"
	signalsdk "github.com/peerrpc/go/signal"
)

func main() {
	addr := flag.String("addr", ":3000", "listen address")
	staticDir := flag.String("static", "", "path to the TS demo's dist/ directory")
	tlsCert := flag.String("tls-cert", "", "path to TLS certificate (enables HTTPS for browser Connect streaming)")
	tlsKey := flag.String("tls-key", "", "path to TLS private key")
	autoTLS := flag.Bool("auto-tls", false, "generate an ephemeral self-signed cert at startup (for dev/testing)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 1. In-process signaling backend + SSE hub for browser signaling.
	backend := signalsdk.NewLocal()
	hub := newSignalHub(backend, logger)

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

	// 3. HTTP mux: signaling service + static files + health.
	// 3. HTTP mux: WebSocket signaling + static files + health.
	//
	// The signaling service is served exclusively over WebSocket; the
	// browser connects via WebSocketSignal and the offerer (below) uses
	// the in-process Local backend via the SSE hub. The /ws endpoint is
	// available for any peer that wants to join over the network.
	mem := store.NewMemory()

	mux := http.NewServeMux()
	mux.Handle("/ws", signalserver.WebSocketHandler(mem, signalserver.Config{Logger: logger}))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	if *staticDir != "" {
		mux.Handle("/", http.FileServer(http.Dir(*staticDir)))
		logger.Info("serving static files", "dir", *staticDir)
	}

	// SSE + POST signaling endpoints for the browser.
	mux.HandleFunc("/api/signal/events", hub.handleSSE)
	mux.HandleFunc("/api/signal/send", hub.handleSend)

	// 4. Launch the PeerRPC Offerer in the background. It joins room
	//    "interop" and waits for the browser Answerer. Signaling flows
	//    through the SSE hub: the browser subscribes to /api/signal/events,
	//    POSTs its answer/ICE to /api/signal/send, and the offerer
	//    broadcasts its offer/ICE via the SSE stream.
	go func() {
		if err := runOfferer(context.Background(), backend, hub, srv, logger); err != nil {
			logger.Error("offerer exited", "err", err)
		}
	}()

	// Resolve TLS config. --auto-tls generates an ephemeral cert for
	// dev/testing so the binary is self-contained.
	if *autoTLS && (*tlsCert == "" || *tlsKey == "") {
		cert, key, err := generateSelfSignedCert()
		if err != nil {
			logger.Error("auto-tls cert generation", "err", err)
			os.Exit(1)
		}
		*tlsCert = cert
		*tlsKey = key
		logger.Info("auto-TLS: generated ephemeral self-signed certificate")
	}

	// 5. Start HTTP server. WebSocket upgrades work over plain HTTP/1.1,
	//    so no h2c is needed. TLS still enables wss:// for browsers.
	httpSrv := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("interop server listening",
			"addr", *addr,
			"tls", *tlsCert != "",
		)
		var err error
		if *tlsCert != "" && *tlsKey != "" {
			err = httpSrv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			err = httpSrv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
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
// on it. Signaling flows through the SSE hub: the offerer broadcasts
// its SDP offer + ICE candidates via the SSE stream, and reads the
// browser's answer + ICE candidates from the SSE POST endpoint.
func runOfferer(ctx context.Context, backend signalsdk.Backend, hub *signalHub, srv *rpc.Server, logger *slog.Logger) error {
	peerID := "go-offerer-" + randomID()
	for {
		sig, err := backend.Exchange(ctx, "interop", peerID)
		if err != nil {
			return fmt.Errorf("signaling exchange: %w", err)
		}

		// The Go offerer sends its SDP offer + ICE candidates via
		// sig.Send, which broadcasts to the "browser" virtual peer
		// created by handleSSE. The browser sends its answer + ICE
		// via POST, which handleSend injects into the backend via
		// the browser session's Send. The offerer receives them
		// through its own sig.Receive.

		// Wait for the browser to join the room via SSE before creating
		// the offer. Without this the offer is broadcast before the
		// browser session exists and is lost.
		select {
		case <-hub.waitForBrowser("interop"):
			logger.Info("browser joined signaling room")
		case <-time.After(60 * time.Second):
			logger.Warn("timed out waiting for browser")
			sig.Close()
			continue
		}

		p, err := peer.New(ctx, signalsdk.RoleClient, peer.Config{
			NegotiationTimeout: 30 * time.Second,
		})
		if err != nil {
			sig.Close()
			return fmt.Errorf("peer.New: %w", err)
		}

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
		// Reset the browser-ready channel so the next iteration waits
		// for a fresh browser connection.
		hub.resetBrowserReady("interop")
		logger.Info("DataChannel closed, waiting for next connection")
	}
}

func randomID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}
