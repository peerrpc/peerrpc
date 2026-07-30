// Package store defines the service/persistence abstraction the
// signaling server sits on.
//
// Two peers wishing to exchange WebRTC SDP announce against the same
// service; the store tracks who is in which service and routes
// messages between them. The interface is small so that backends
// can be swapped (in-memory for tests / single binary, Redis for
// production horizontal scaling) without touching the handler.
package store

import (
	"context"
	"errors"
)

// SignalMessage is the per-message envelope the store exchanges. It
// mirrors the wire SignalMessage shape without dragging the generated
// package into every store caller.
//
// Only Service and Body are used; the wire's `oneof body` is
// preserved as the Body field. Implementations MUST treat messages
// as opaque.
type SignalMessage struct {
	// Service is the rendezvous key the message is addressed to.
	Service string
	// SenderID is the peer that produced the message. The store
	// fills this in from the session and never trusts the wire.
	SenderID string
	// Body is the marshaled SignalMessage.body oneof payload, OR a
	// concrete typed value the store is willing to forward as-is.
	// Stores MUST NOT inspect Body.
	Body any
}

// Peer is one connected signaling client. A peer is identified by an
// arbitrary string chosen by the client (peer_id) and scoped to its
// service.
type Peer struct {
	ID      string
	Service string
}

// ErrPeerAlreadyExists is returned by Join when a peer_id is already
// present in the requested service.
var ErrPeerAlreadyExists = errors.New("store: peer already exists in service")

// ErrServiceFull is returned by Join when the service has reached
// MaxPeers. Two peers per service is the PeerRPC ceiling.
var ErrServiceFull = errors.New("store: service is full")

// Store is the rendezvous backend for the signaling server.
//
// All methods MUST be safe for concurrent use. Implementations are
// expected to be cheap to construct so a handler can hold one per
// process.
type Store interface {
	// Join registers peer in service. The returned Sender is what
	// the handler uses to broadcast messages from this peer into
	// the service; the receiver channel yields every message
	// broadcast by OTHER peers in the service (never this peer's
	// own messages).
	//
	// Join blocks until ctx is canceled or Leave is called for the
	// same peer. Callers MUST pump Receiver until it closes to
	// drain in-flight messages.
	Join(ctx context.Context, peer Peer) (Sender, Receiver, error)

	// Leave removes peer from its service and closes any
	// associated receiver channel. Safe to call multiple times.
	Leave(ctx context.Context, peer Peer) error

	// Stats returns service/peer counts for diagnostics.
	Stats() Stats
}

// Sender is the per-peer outbound handle returned by Join.
type Sender interface {
	// Send broadcasts msg to every other peer currently in the
	// service. Send is non-blocking: if a remote peer's inbox is
	// full, the message is dropped (signaling is best-effort).
	Send(ctx context.Context, msg SignalMessage) error
	// Close releases the sender. Subsequent calls are no-ops.
	Close() error
}

// Receiver yields inbound messages for the peer.
type Receiver interface {
	// Recv blocks until the next message arrives or the channel
	// closes (peer left, ctx canceled, store shut down).
	Recv() <-chan SignalMessage
}

// Stats is a snapshot of the store's current state.
type Stats struct {
	Services     int
	Peers        int
	ServicePeers map[string]int
}
