package peerrpc

import (
	"context"
	"fmt"
	"sync"

	"github.com/google/uuid"
	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
)

// Listener is the long-lived handle returned by Listen. It owns the
// signaling-side Target template; each call to Accept mints a fresh
// peer_id + Session + PeerConnection against that template and
// blocks until a remote Dialer connects.
//
// Listener is goroutine-safe for concurrent Accept calls when the
// underlying signaling backend allows multiple concurrent sessions
// for the same service. signal.Local does (it scopes by peer_id, not
// by service). signal.Remote + the standalone signal-server does
// too, as long as each Accept uses a distinct peer_id (which this
// facade guarantees by auto-suffixing).
type Listener struct {
	target Target
	cfg    listenConfig

	mu     sync.Mutex
	conns  map[string]*ServerConn
	closed chan struct{}
	once   sync.Once
}

// Listen is the URL-string entry point for the server side.
//
//	ln, err := peerrpc.Listen(ctx,
//	    "peerrpc+connect://signal.example.com/echo.Echo",
//	    peerrpc.WithToken(jwt),
//	)
//	defer ln.Close()
//	for {
//	    conn, err := ln.Accept(ctx)
//	    if err != nil { return err }
//	    go serve(conn)
//	}
//
// Listen does NOT block; it parses the target and returns
// immediately. Accept does the blocking.
func Listen(ctx context.Context, target string, opts ...ListenOption) (*Listener, error) {
	t, err := ParseTarget(target)
	if err != nil {
		return nil, err
	}
	return ListenTarget(ctx, t, opts...)
}

// ListenTarget is the typed-Target variant of Listen.
func ListenTarget(ctx context.Context, target Target, opts ...ListenOption) (*Listener, error) {
	cfg := listenConfig{target: target}
	for _, opt := range opts {
		opt.applyListen(&cfg)
	}
	if cfg.token != "" {
		cfg.target.Token = cfg.token
	}
	if cfg.target.Role == "" {
		cfg.target.Role = RoleHintServer
	}
	// WithIdentity pins a stable prefix on the listener's peer_id
	// template. Each Accept still suffixes the template with a
	// short random ID (see Listener.Accept) so concurrent clients
	// get distinct peer_ids, but the prefix is reproducible across
	// restarts when the same key is used.
	if cfg.target.PeerID == "" {
		if id, ok := derivePeerID(cfg.identity); ok {
			cfg.target.PeerID = id
		}
	}
	if _, err := resolveProbe(cfg.target); err != nil {
		return nil, err
	}
	return &Listener{
		target: cfg.target,
		cfg:    cfg,
		conns:  map[string]*ServerConn{},
		closed: make(chan struct{}),
	}, nil
}

// Accept blocks until a remote Dialer announces against the
// listener's service and the WebRTC DataChannel opens. Each call
// uses a distinct auto-generated peer_id so the same listener can
// serve many concurrent clients without conflicting at the
// signal-server.
//
// Returns ctx.Err() when ctx is canceled, or ErrListenerClosed
// when Close has been called.
func (l *Listener) Accept(ctx context.Context) (*ServerConn, error) {
	if l.isClosed() {
		return nil, ErrListenerClosed
	}

	t := l.target
	if t.PeerID == "" {
		t.PeerID = uuid.NewString()
	} else {
		t.PeerID = t.PeerID + "-" + shortID()
	}

	resolution, err := resolve(ctx, t)
	if err != nil {
		return nil, err
	}

	peerCfg, err := buildPeerConfigListen(l.cfg)
	if err != nil {
		_ = resolution.Session.Close()
		return nil, err
	}

	p, err := peer.New(ctx, resolution.PeerRole, peerCfg)
	if err != nil {
		_ = resolution.Session.Close()
		return nil, fmt.Errorf("peerrpc: peer.New: %w", err)
	}

	ch, err := p.Accept(ctx, resolution.Session)
	if err != nil {
		_ = p.Close()
		_ = resolution.Session.Close()
		return nil, fmt.Errorf("peerrpc: peer.Accept: %w", err)
	}

	sc := newServerConn(ch, p, resolution.Session, resolution.PeerID)
	l.track(sc)
	return sc, nil
}

// Serve is a convenience loop: accept one peer at a time, hand its
// channel to a fresh rpc.Server built from factory, and run srv.Serve
// in a goroutine. Returns when ctx is canceled or an Accept fails.
//
// The factory is invoked once per accepted peer so that interceptors
// and any per-connection state are isolated. Callers wanting to
// share a single Server instance across peers (NOT recommended: rpc
// is single-connection) should call Accept + srv.Serve themselves.
func (l *Listener) Serve(ctx context.Context, factory func() *rpc.Server) error {
	for {
		if l.isClosed() {
			return ErrListenerClosed
		}
		sc, err := l.Accept(ctx)
		if err != nil {
			if l.isClosed() {
				return ErrListenerClosed
			}
			return err
		}
		srv := factory()
		go func(s *rpc.Server, c *ServerConn) {
			defer l.untrack(c)
			_ = s.Serve(ctx, c.channel)
		}(srv, sc)
	}
}

// Close stops accepting new peers and tears down any active ones.
// Idempotent.
func (l *Listener) Close() error {
	l.once.Do(func() {
		close(l.closed)
		l.mu.Lock()
		defer l.mu.Unlock()
		for _, c := range l.conns {
			_ = c.Close()
		}
		l.conns = nil
	})
	return nil
}

func (l *Listener) isClosed() bool {
	select {
	case <-l.closed:
		return true
	default:
		return false
	}
}

func (l *Listener) track(c *ServerConn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conns == nil {
		return
	}
	l.conns[c.peerID] = c
}

func (l *Listener) untrack(c *ServerConn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conns == nil {
		return
	}
	delete(l.conns, c.peerID)
}

// ErrListenerClosed is returned by Accept / Serve after Close.
var ErrListenerClosed = fmt.Errorf("peerrpc: listener closed")

// resolveProbe validates that target has a registered resolver
// without opening a session. Used by ListenTarget to fail-fast on
// typos.
func resolveProbe(target Target) (*Resolver, error) {
	defaultRegistry.mu.RLock()
	defer defaultRegistry.mu.RUnlock()
	factory, ok := defaultRegistry.factories[target.Scheme]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedScheme, target.Scheme)
	}
	r, err := factory()
	if err != nil {
		return nil, fmt.Errorf("peerrpc: resolver factory for %q failed: %w", target.Scheme, err)
	}
	return &r, nil
}

// shortID returns the first 8 hex chars of a fresh UUID, enough to
// disambiguate peer_id suffixes within one service.
func shortID() string {
	id := uuid.NewString()
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

// Used by listen.go and listen_builder.go to scope imports.
type (
	peerAlias      = peer.Peer
	sessionAlias   = signal.Session
	transportAlias = transport.Channel
)
