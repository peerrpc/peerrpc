package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	signalingpbconnect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/signalingpbconnect"
	goauth "github.com/peerrpc/go/auth"
	"github.com/peerrpc/go/observability"
	"github.com/peerrpc/signal-server/auth"
	"github.com/peerrpc/signal-server/server"
	"github.com/peerrpc/signal-server/store"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

var signalCmd = &cobra.Command{
	Use:   "signal",
	Short: "Run the signaling server",
	Long: `Start the standalone PeerRPC signaling server.

Exposes peerrpc.signaling.SignalingService over HTTP (Connect,
gRPC, and gRPC-Web via a single handler).`,
	RunE: runSignal,
}

var signalFlags struct {
	addr       string
	authStatic string
	jwtSecret  string
	tlsCert    string
	tlsKey     string
}

func init() {
	f := signalCmd.Flags()
	f.StringVar(&signalFlags.addr, "addr", ":8080", "listen address")
	f.StringVar(&signalFlags.authStatic, "auth-static", "", "comma-separated token=subject pairs (dev-only)")
	f.StringVar(&signalFlags.jwtSecret, "jwt-secret", "", "HMAC-SHA256 secret for JWT verification")
	f.StringVar(&signalFlags.tlsCert, "tls-cert", "", "path to TLS certificate")
	f.StringVar(&signalFlags.tlsKey, "tls-key", "", "path to TLS private key")
}

func runSignal(_ *cobra.Command, _ []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	mem := store.NewMemory()
	svc := server.New(mem, server.Config{Logger: logger})

	var opts []connect.HandlerOption
	switch {
	case signalFlags.jwtSecret != "":
		v := jwtVerifierAdapter{secret: []byte(signalFlags.jwtSecret)}
		opts = append(opts, connect.WithInterceptors(auth.NewInterceptor(v)))
		logger.Info("JWT auth enabled")
	case signalFlags.authStatic != "":
		v, err := parseStaticValidator(signalFlags.authStatic)
		if err != nil {
			logger.Error("invalid --auth-static", "err", err)
			return err
		}
		opts = append(opts, connect.WithInterceptors(auth.NewInterceptor(v)))
		logger.Info("static auth enabled", "tokens", len(v.Identities))
	default:
		logger.Warn("no auth configured; production deployments MUST pass --jwt-secret")
	}

	_ = observability.NewMetrics(nil)

	mux := http.NewServeMux()
	path, handler := signalingpbconnect.NewSignalingServiceHandler(svc, opts...)
	mux.Handle(path, handler)
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

	srv := &http.Server{
		Addr:    signalFlags.addr,
		Handler: mux,
	}
	if signalFlags.tlsCert != "" && signalFlags.tlsKey != "" {
		srv.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	} else {
		srv.Handler = h2c.NewHandler(mux, &http2.Server{})
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		logger.Info("signaling server listening",
			"addr", signalFlags.addr,
			"tls", signalFlags.tlsCert != "",
		)
		var err error
		if signalFlags.tlsCert != "" && signalFlags.tlsKey != "" {
			err = srv.ListenAndServeTLS(signalFlags.tlsCert, signalFlags.tlsKey)
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
	return srv.Shutdown(shutCtx)
}

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

type jwtVerifierAdapter struct{ secret []byte }

func (a jwtVerifierAdapter) Validate(_ context.Context, token string) (auth.Identity, error) {
	v := goauth.HS256Verifier{Secret: a.secret}
	claims, err := v.Verify(context.Background(), token)
	if err != nil {
		return auth.Identity{}, err
	}
	return auth.Identity{Subject: claims.Subject, Service: claims.Service}, nil
}
