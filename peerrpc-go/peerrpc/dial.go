package peerrpc

import (
	"context"
	"fmt"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
)

// Dial is the URL-string entry point. It parses target as a Target
// URI and dials it.
//
//	conn, err := peerrpc.Dial(ctx,
//	    "peerrpc+connect://signal.example.com/echo.Echo",
//	    peerrpc.WithToken(jwt),
//	)
//
// Dial blocks until the WebRTC DataChannel is open (or ctx expires,
// or an error occurs). The returned Conn is ready for Invoke* calls
// immediately.
func Dial(ctx context.Context, target string, opts ...DialOption) (*Conn, error) {
	t, err := ParseTarget(target)
	if err != nil {
		return nil, err
	}
	return DialTarget(ctx, t, opts...)
}

// DialTarget is the typed-Target entry point. It is equivalent to
// Dial but skips the URI parse, which is useful when the application
// constructs the Target programmatically.
//
//	conn, err := peerrpc.DialTarget(ctx, peerrpc.Target{
//	    Scheme:  peerrpc.SchemeConnect,
//	    Signal:  "signal.example.com",
//	    Service: "echo.Echo",
//	})
func DialTarget(ctx context.Context, target Target, opts ...DialOption) (*Conn, error) {
	cfg := dialConfig{target: target}
	for _, opt := range opts {
		opt.applyDial(&cfg)
	}
	// Options override Target fields when both are set.
	if cfg.token != "" {
		cfg.target.Token = cfg.token
	}
	return dialTarget(ctx, cfg)
}

// dialTarget is the single core path shared by Dial / DialTarget /
// DialBuilder.Connect.
func dialTarget(ctx context.Context, cfg dialConfig) (*Conn, error) {
	resolution, err := resolve(ctx, cfg.target)
	if err != nil {
		return nil, err
	}

	peerCfg, err := buildPeerConfig(cfg)
	if err != nil {
		_ = resolution.Session.Close()
		return nil, err
	}

	p, err := peer.New(ctx, resolution.PeerRole, peerCfg)
	if err != nil {
		_ = resolution.Session.Close()
		return nil, fmt.Errorf("peerrpc: peer.New: %w", err)
	}

	// WebRTC negotiation. Role is derived from Target.Role (default
	// Offerer for Dial). On any failure we release the stack so the
	// server doesn't leak a phantom peer.
	var ch *transport.Channel
	switch resolution.PeerRole {
	case signal.RoleOfferer:
		ch, err = p.Dial(ctx, resolution.Session)
		if err != nil {
			return cleanupDial(p, resolution.Session, fmt.Errorf("peerrpc: peer.Dial: %w", err))
		}
	case signal.RoleAnswerer:
		ch, err = p.Accept(ctx, resolution.Session)
		if err != nil {
			return cleanupDial(p, resolution.Session, fmt.Errorf("peerrpc: peer.Accept: %w", err))
		}
	default:
		return cleanupDial(p, resolution.Session, fmt.Errorf("peerrpc: unresolved peer role %d", resolution.PeerRole))
	}

	clientOpts := make([]rpc.ClientOption, 0, len(cfg.interceptors))
	if len(cfg.interceptors) > 0 {
		clientOpts = append(clientOpts, rpc.WithUnaryClientInterceptors(cfg.interceptors...))
	}
	client := rpc.NewClient(ch, clientOpts...)
	conn := newConn(ctx, client, ch, p, resolution.Session, resolution.PeerID)
	return conn, nil
}

// cleanupDial tears down peer + session on a failed dial and returns
// the wrapped error. Centralized so each branch stays one line.
func cleanupDial(p *peer.Peer, s *signal.Session, cause error) (*Conn, error) {
	_ = p.Close()
	_ = s.Close()
	return nil, cause
}
