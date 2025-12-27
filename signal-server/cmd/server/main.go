// Command peerrpc-signal runs the standalone PeerRPC signaling server.
//
// It exposes peerrpc.signaling.v1.SignalingService over HTTP. Thanks
// to connect-go a single handler transparently serves:
//
//   - Connect clients (connect-go / @connectrpc/connect-web)
//   - gRPC clients (grpc-go)
//   - gRPC-Web clients (browsers, no Envoy required)
//
// Usage:
//
//	peerrpc-signal -addr :8080
//
// For development the binary defaults to no auth; production callers
// should pass --auth-static=token1=alice,token2=bob (subject=peer) to
// require a bearer token on every stream.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	signalingpbconnect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/v1/signalingpbconnect"
	"github.com/peerrpc/signal-server/auth"
	"github.com/peerrpc/signal-server/server"
	"github.com/peerrpc/signal-server/store"

	"connectrpc.com/connect"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	authStatic := flag.String("auth-static", "", "comma-separated token=subject pairs (dev-only; production should use --auth-impl)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	mem := store.NewMemory()
	svc := server.New(mem, server.Config{Logger: logger})

	var opts []connect.HandlerOption
	if *authStatic != "" {
		v, err := parseStaticValidator(*authStatic)
		if err != nil {
			logger.Error("invalid --auth-static", "err", err)
			os.Exit(2)
		}
		opts = append(opts, connect.WithInterceptors(auth.NewInterceptor(v)))
		logger.Info("static auth enabled", "tokens", len(v.Identities))
	}

	mux := http.NewServeMux()
	path, handler := signalingpbconnect.NewSignalingServiceHandler(svc, opts...)
	mux.Handle(path, handler)

	// /healthz for liveness probes; the handler does not need a store
	// round-trip because the binary itself is alive.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		s := mem.Stats()
		logger.Info("stats", "rooms", s.Rooms, "peers", s.Peers)
		w.WriteHeader(http.StatusOK)
	})

	// http2 is required by gRPC clients even without TLS (h2c).
	srv := &http.Server{
		Addr:    *addr,
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("signaling server listening", "addr", *addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("ListenAndServe failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		logger.Error("shutdown failed", "err", err)
	}
}

// parseStaticValidator parses "token1=alice,token2=bob" into a
// StaticValidator. Each token=subject pair becomes one accepted
// bearer token.
func parseStaticValidator(s string) (auth.StaticValidator, error) {
	out := auth.StaticValidator{Identities: map[string]auth.Identity{}}
	if s == "" {
		return out, nil
	}
	for _, pair := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(pair), "=", 2)
		if len(kv) != 2 || kv[0] == "" {
			return out, fmt.Errorf("bad token=subject pair: %q", pair)
		}
		out.Identities[kv[0]] = auth.Identity{Subject: kv[1]}
	}
	return out, nil
}
