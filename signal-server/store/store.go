// Package store defines the room/persistence abstraction the
// signaling server sits on.
//
// Two peers wishing to exchange WebRTC SDP join the same room; the
// store tracks who is in which room and routes messages between them.
// The interface is small so that backends can be swapped (in-memory
// for tests / single binary, Redis for production horizontal scaling)
// without touching the handler.
package store

import (
	"context"
	"errors"
)

// SignalMessage is the per-message envelope the store exchanges. It
// mirrors proto/peerrpc/signaling/v1/signaling.proto's SignalMessage
// without dragging the generated package into every store caller.
//
// Only RoomID and Body are used; the wire's `oneof body` is preserved
// as the Body field. Implementations MUST treat messages as opaque.
type SignalMessage struct {
	// RoomID is the room the message is addressed to.
	RoomID string
	// SenderID is the peer that produced the message. The store fills
	// this in from the session and never trusts the wire.
	SenderID string
	// Body is the marshaled SignalMessage.body oneof payload, OR a
	// concrete typed value the store is willing to forward as-is.
	// Stores MUST NOT inspect Body.
	Body any
}

// Peer is one connected signaling client. A peer is identified by an
// arbitrary string chosen by the client (peer_id) and scoped to its
// room.
type Peer struct {
	ID     string
	RoomID string
}

// ErrPeerAlreadyExists is returned by Join when a peer_id is already
// present in the requested room.
var ErrPeerAlreadyExists = errors.New("store: peer already exists in room")

// ErrRoomFull is returned by Join when the room has reached MaxPeers.
// Two peers per room is the PeerRPC v1 ceiling.
var ErrRoomFull = errors.New("store: room is full")

// Store is the rendezvous backend for the signaling server.
//
// All methods MUST be safe for concurrent use. Implementations are
// expected to be cheap to construct so a handler can hold one per
// process.
type Store interface {
	// Join registers peer in room. The returned Sender is what the
	// handler uses to broadcast messages from this peer into the room;
	// the receiver channel yields every message broadcast by OTHER
	// peers in the room (never this peer's own messages).
	//
	// Join blocks until ctx is canceled or Leave is called for the
	// same peer. Callers MUST pump Receiver until it closes to drain
	// in-flight messages.
	Join(ctx context.Context, peer Peer) (Sender, Receiver, error)

	// Leave removes peer from its room and closes any associated
	// receiver channel. Safe to call multiple times.
	Leave(ctx context.Context, peer Peer) error

	// Stats returns room/peer counts for diagnostics. The shape is
	// implementation-defined; tests assert specific fields.
	Stats() Stats
}

// Sender is the per-peer outbound handle returned by Join.
type Sender interface {
	// Send broadcasts msg to every other peer currently in the room.
	// Send is non-blocking: if a remote peer's inbox is full, the
	// message is dropped (signaling is best-effort for v1).
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
	Rooms     int
	Peers     int
	RoomPeers map[string]int
}
