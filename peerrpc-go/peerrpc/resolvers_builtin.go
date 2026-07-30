package peerrpc

import (
	"context"
	"fmt"

	"github.com/peerrpc/go/signal"
	"github.com/google/uuid"
)

// localResolver routes through an in-process signal.Local backend.
// Tests and single-binary demos use it via SchemeLocal.
type localResolver struct {
	backend *signal.Local
}

// Resolve implements Resolver.
func (r *localResolver) Resolve(ctx context.Context, target Target) (*Resolution, error) {
	peerID := target.PeerID
	if peerID == "" {
		// v1.6 of google/uuid ships v4 only. PeerRPC's contract is
		// "globally unique opaque string"; a future bump to v1.7+
		// upgrades this to UUIDv7 (k-sortable) without an API change.
		peerID = uuid.NewString()
	}
	session, err := r.backend.Exchange(ctx, target.Service, peerID)
	if err != nil {
		return nil, fmt.Errorf("peerrpc: local exchange: %w", err)
	}
	return &Resolution{
		Session:  session,
		PeerRole: peerRole(target, RoleHintClient),
		PeerID:   peerID,
	}, nil
}

// wsResolver routes through signal.WS (the WebSocket client). Each
// Resolve constructs a fresh WS backend because the base URL is
// Target-specific; this keeps Resolver stateless and the WS client
// is cheap to instantiate.
type wsResolver struct{}

// Resolve implements Resolver.
func (r *wsResolver) Resolve(ctx context.Context, target Target) (*Resolution, error) {
	if target.Signal == "" {
		return nil, fmt.Errorf("peerrpc: scheme %q requires a non-empty Target.Signal", target.Scheme)
	}
	peerID := target.PeerID
	if peerID == "" {
		peerID = uuid.NewString()
	}
	var opts []signal.WSOption
	if target.Token != "" {
		opts = append(opts, signal.WithToken(target.Token))
	}
	backend := signal.NewWS(target.Signal, opts...)
	session, err := backend.Exchange(ctx, target.Service, peerID)
	if err != nil {
		return nil, fmt.Errorf("peerrpc: ws exchange: %w", err)
	}
	return &Resolution{
		Session:  session,
		PeerRole: peerRole(target, RoleHintClient),
		PeerID:   peerID,
	}, nil
}
