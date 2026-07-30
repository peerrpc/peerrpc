// Command peerrpc-signal runs the standalone PeerRPC signaling server.
//
// It serves the signaling rendezvous exclusively over WebSocket
// (wss://host/ws). Both browser clients (WebSocketSignal) and Go
// clients (signal.WS) speak this transport.
//
// Usage:
//
//	peerrpc-signal -addr :8080
//
// For development the binary defaults to no auth; production callers
// should pass --auth-static=token1=alice,token2=bob (subject=peer) to
// require a bearer token on every connection.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	goauth "github.com/peerrpc/go/auth"
	"github.com/peerrpc/go/observability"
	"github.com/peerrpc/signal-server/auth"
	"github.com/peerrpc/signal-server/server"
	"github.com/peerrpc/signal-server/store"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	authStatic := flag.String("auth-static", "", "comma-separated token=subject pairs (dev-only; production should use --jwt-secret)")
	jwtSecret := flag.String("jwt-secret", "", "HMAC-SHA256 secret for JWT verification (production auth)")
	tlsCert := flag.String("tls-cert", "", "path to TLS certificate (enables WSS)")
	tlsKey := flag.String("tls-key", "", "path to TLS private key (enables WSS)")
	autoTLS := flag.Bool("auto-tls", false, "generate an ephemeral self-signed cert at startup (for browser dev/testing)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// --auto-tls: generate an ephemeral self-signed certificate so
	// browsers can reach the WebSocket via wss:// without
	// pre-provisioned cert files.
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

	mem := store.NewMemory()

	var validator auth.TokenValidator
	switch {
	case *jwtSecret != "":
		// Production: HMAC-SHA256 JWT verifier. Tokens are issued
		// elsewhere (typically by the same issuer that knows the
		// secret — e.g. the application's auth service).
		validator = jwtVerifierAdapter{secret: []byte(*jwtSecret)}
		logger.Info("JWT auth enabled")
	case *authStatic != "":
		v, err := parseStaticValidator(*authStatic)
		if err != nil {
			logger.Error("invalid --auth-static", "err", err)
			os.Exit(2)
		}
		validator = v
		logger.Info("static auth enabled", "tokens", len(v.Identities))
	default:
		logger.Warn("no auth configured; production deployments MUST pass --jwt-secret")
	}

	// Metrics: expose Prometheus collectors for /metrics. We use
	// the default registerer so promhttp.Handler sees them; tests
	// can swap in their own via observability.NewMetrics.
	_ = observability.NewMetrics(nil)

	mux := http.NewServeMux()

	// WebSocket signaling endpoint. The signaling service is served
	// exclusively over WebSocket (ws:// cleartext or wss:// over TLS).
	mux.Handle("/ws", server.WebSocketHandler(mem, server.Config{
		Logger:    logger,
		Validator: validator,
	}))

	// /healthz for liveness probes; the handler does not need a store
	// round-trip because the binary itself is alive.
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/stats", func(w http.ResponseWriter, _ *http.Request) {
		s := mem.Stats()
		logger.Info("stats", "services", s.Services, "peers", s.Peers)
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/metrics", promhttp.Handler())

	// TLS: when both cert and key are provided we serve HTTPS (so the
	// WebSocket is wss://). Otherwise plain HTTP/1.1 (ws://) — the
	// WebSocket upgrade works over HTTP/1.1 without h2c.
	srv := &http.Server{
		Addr:    *addr,
		Handler: mux,
	}
	if *tlsCert != "" && *tlsKey != "" {
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("signaling server listening",
			"addr", *addr,
			"tls", *tlsCert != "",
		)
		var err error
		if *tlsCert != "" && *tlsKey != "" {
			err = srv.ListenAndServeTLS(*tlsCert, *tlsKey)
		} else {
			err = srv.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
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

// jwtVerifierAdapter bridges signal-server/auth.TokenValidator to
// peerrpc-go/auth.Verifier. The signal-server package owns its own
// TokenValidator shape (Identity struct); the bridge extracts the
// standard Claims the JWT verifier produces and converts.
type jwtVerifierAdapter struct{ secret []byte }

func (a jwtVerifierAdapter) Validate(_ context.Context, token string) (auth.Identity, error) {
	v := goauth.HS256Verifier{Secret: a.secret}
	claims, err := v.Verify(context.Background(), token)
	if err != nil {
		return auth.Identity{}, err
	}
	return auth.Identity{Subject: claims.Subject, Service: claims.Service}, nil
}

// jwtClaimsAdapter is unused here but exported so callers that wrap
// goauth.HS256Verifier (e.g. relay-server, grpcbridge-server) can
// reuse the same Claims->Identity translation.
type jwtClaimsAdapter = goauth.Claims
