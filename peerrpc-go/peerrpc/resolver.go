package peerrpc

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/peerrpc/go/signal"
)

// Resolver turns a Target into an open signaling Session. The
// interface mirrors gRPC's resolver.Resolver: dialing is decoupled
// from how the address was obtained, so different schemes (local,
// connect, ws, relay, future xds) plug in behind the same Dial /
// Listen entry points.
//
// Resolution does NOT include the WebRTC dial. It returns a Session
// (a bidi signaling stream) plus the peer.Role the dialer/listener
// should adopt for the WebRTC negotiation. The caller (dial.go /
// listen.go) feeds those into peer.New + peer.Dial/Accept.
//
// Implementations MUST be safe for concurrent use across goroutines
// and across Targets; the registry may share one Resolver instance
// for every call.
type Resolver interface {
	// Resolve opens a signaling Session for target. The returned
	// Session MUST be closed by the caller (or have ctx cancel it)
	// to release the underlying stream.
	Resolve(ctx context.Context, target Target) (*Resolution, error)
}

// Resolution is the output of Resolver.Resolve. It carries the open
// signaling session and the resolved peer.Role.
type Resolution struct {
	// Session is the bidi signaling stream positioned AFTER the
	// announce/join message has been sent. The caller may immediately
	// exchange SDP/ICE through it.
	Session *signal.Session

	// PeerRole is the WebRTC role the caller should pass to peer.New.
	// Derived from Target.Role: RoleHintClient → RoleClient,
	// RoleHintServer → RoleServer.
	PeerRole signal.Role

	// PeerID is the peer_id actually used on the wire. Useful for
	// logging when Target.PeerID was empty and the resolver
	// auto-generated one.
	PeerID string
}

// ErrUnsupportedScheme is returned by Resolve when no resolver has
// been registered for the requested scheme. The schemes
// SchemeConnect and SchemeLocal are always available; SchemeWS and
// SchemeRelay need explicit registration (and the Go SDK does not
// ship a WS client resolver — see NewWSResolver).
var ErrUnsupportedScheme = errors.New("peerrpc: unsupported scheme; register one with peerrpc.RegisterResolver")

// resolverRegistry maps scheme → Resolver factory. A factory is
// used (rather than a singleton Resolver) so that schemes which need
// per-Target state (e.g. a remote URL) can construct it on demand.
type resolverRegistry struct {
	mu       sync.RWMutex
	factories map[Scheme]ResolverFactory
}

// ResolverFactory constructs a Resolver. Factories let a scheme
// capture shared state once (e.g. an in-process signal.Local
// instance reused across all SchemeLocal dials).
type ResolverFactory func() (Resolver, error)

var defaultRegistry = resolverRegistry{
	factories: map[Scheme]ResolverFactory{},
}

// RegisterResolver associates scheme with factory. Subsequent calls
// to Resolve / Dial / Listen with that scheme will dispatch through
// factory. Registering the same scheme twice replaces the previous
// registration. Built-in schemes (local, connect) are pre-registered
// at package init; callers may override them.
//
// RegisterResolver is safe for concurrent use.
func RegisterResolver(scheme Scheme, factory ResolverFactory) {
	defaultRegistry.mu.Lock()
	defer defaultRegistry.mu.Unlock()
	defaultRegistry.factories[scheme] = factory
}

// resolve dispatches to the registered factory for target.Scheme.
func resolve(ctx context.Context, target Target) (*Resolution, error) {
	defaultRegistry.mu.RLock()
	factory, ok := defaultRegistry.factories[target.Scheme]
	defaultRegistry.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedScheme, target.Scheme)
	}
	r, err := factory()
	if err != nil {
		return nil, fmt.Errorf("peerrpc: resolver factory for %q failed: %w", target.Scheme, err)
	}
	return r.Resolve(ctx, target)
}

// peerRole derives the WebRTC peer.Role from Target.Role. Client
// (the Dialer side) creates the SDP offer; Server (the Listener
// side) accepts it. The empty default maps to Offerer, the only
// sensible choice for a Dialer that did not pick a side explicitly.
func peerRole(t Target, defaultIfEmpty RoleHint) signal.Role {
	r := t.Role
	if r == "" {
		r = defaultIfEmpty
	}
	if r == RoleHintServer {
		return signal.RoleServer
	}
	return signal.RoleClient
}

func init() {
	// SchemeLocal MUST share one signal.Local across all dialers and
	// listeners in the process. Each call to signal.NewLocal would
	// create an isolated broadcast domain; two peers dialing through
	// different Local instances would never see each other. We cache
	// the singleton once at package init.
	var localOnce sync.Once
	var localBackend *signal.Local
	RegisterResolver(SchemeLocal, func() (Resolver, error) {
		localOnce.Do(func() {
			localBackend = signal.NewLocal()
		})
		return &localResolver{backend: localBackend}, nil
	})
	RegisterResolver(SchemeConnect, func() (Resolver, error) {
		// A new Remote backend is constructed per-Resolve because the
		// URL is Target-specific. The connect client is cheap to
		// construct; pooling is a future optimization.
		return &connectResolver{}, nil
	})
}
