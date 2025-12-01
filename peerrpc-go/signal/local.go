// Package signal provides the PeerRPC signaling client and pluggable
// backends.
//
// Two peers MUST exchange SDP offers/answers and ICE candidates over
// some shared channel before a WebRTC DataConnection can form. That
// channel is the "signaling" path and is intentionally decoupled from
// the WebRTC data path: any rendezvous mechanism works.
//
// Phase 1 ships an in-process backend (`Local`) suitable for tests and
// single-binary demos. Future phases add connect-go network backends
// (Cloudflare Workers, standalone Go server) without changing the
// client surface.
package signal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// SignalMessage mirrors proto/peerrpc/signaling/v1/signaling.proto
// without dragging the generated package into every caller. Backends
// exchange pointers to these values, so they should be treated as
// read-only by receivers.
type SignalMessage struct {
	RoomID string
	Body   SignalBody
}

// SignalBody is a tagged union corresponding to the proto oneof.
// Only one field is populated per message.
type SignalBody struct {
	Join      *JoinRequest
	Offer     *SdpOffer
	Answer    *SdpAnswer
	Candidate *IceCandidate
	Leave     *LeaveRequest
	Ping      *Ping
}

// JoinRequest joins (or creates) a signaling room.
type JoinRequest struct {
	PeerID string
	Role   Role
}

// Role disambiguates which peer initiates the WebRTC offer.
type Role int

const (
	RoleUnspecified Role = 0
	RoleOfferer     Role = 1
	RoleAnswerer    Role = 2
)

// SdpOffer carries a WebRTC offer SDP.
type SdpOffer struct {
	Sdp string
}

// SdpAnswer carries a WebRTC answer SDP.
type SdpAnswer struct {
	Sdp string
}

// IceCandidate carries one ICE candidate.
type IceCandidate struct {
	Candidate    string
	SdpMid       string
	SdpMLineIndex uint32
}

// LeaveRequest signals departure.
type LeaveRequest struct {
	Reason string
}

// Ping is a liveness probe.
type Ping struct {
	TimestampMs int64
}

// Backend is the rendezvous transport. The client runs a bidirectional
// Exchange against it: outbound messages are sent via Send, inbound
// messages arrive on the channel returned by Receive.
//
// Backends MUST guarantee:
//   - Each peer in a room receives every other peer's messages except
//     its own (broadcast to others).
//   - Messages sent before a peer joins are not delivered to it (no
//     buffering for Phase 1; Phase 2 may add room history).
//   - Close is idempotent and also drains the receive channel.
type Backend interface {
	// Exchange connects the caller to roomID under peerID and returns
	// a Session whose channel yields every message broadcast into the
	// room by other peers. The session is closed when ctx is canceled
	// or Close is called.
	Exchange(ctx context.Context, roomID, peerID string) (*Session, error)
}

// Session is a peer's presence in a signaling room.
type Session struct {
	backend *Local
	roomID  string
	peerID  string

	out chan<- *SignalMessage // outbound to room
	in  <-chan *SignalMessage // inbound from room

	done chan struct{}
	once sync.Once
}

// Send broadcasts msg to every other peer in the room.
func (s *Session) Send(ctx context.Context, msg *SignalMessage) error {
	msg.RoomID = s.roomID
	select {
	case s.out <- msg:
		return nil
	case <-s.done:
		return errors.New("signal: session closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Receive returns the inbound channel.
func (s *Session) Receive() <-chan *SignalMessage { return s.in }

// Close leaves the room. Safe to call multiple times.
func (s *Session) Close() error {
	s.once.Do(func() {
		s.backend.leave(s.roomID, s.peerID)
		close(s.done)
	})
	return nil
}

// Local is an in-process Backend. It is the rendezvous for two peers
// running inside the same binary: tests, examples, and `peerrpc serve`
// with embedded signaling.
//
// Local is safe for concurrent use by any number of peers and rooms.
type Local struct {
	mu    sync.Mutex
	rooms map[string]*room
}

// NewLocal constructs an empty in-process backend.
func NewLocal() *Local { return &Local{rooms: make(map[string]*room)} }

type room struct {
	mu    sync.Mutex
	peers map[string]chan *SignalMessage
}

// Exchange implements Backend.
func (l *Local) Exchange(ctx context.Context, roomID, peerID string) (*Session, error) {
	if roomID == "" {
		return nil, errors.New("signal: empty room id")
	}
	if peerID == "" {
		return nil, errors.New("signal: empty peer id")
	}

	l.mu.Lock()
	r, ok := l.rooms[roomID]
	if !ok {
		r = &room{peers: make(map[string]chan *SignalMessage)}
		l.rooms[roomID] = r
	}
	l.mu.Unlock()

	// Buffered so a slow peer does not block broadcasters during a
	// burst. 32 is plenty for signaling (a handful of SDP/ICE frames).
	in := make(chan *SignalMessage, 32)
	out := make(chan *SignalMessage, 32)

	r.mu.Lock()
	if _, exists := r.peers[peerID]; exists {
		r.mu.Unlock()
		return nil, fmt.Errorf("signal: peer %q already in room %q", peerID, roomID)
	}
	r.peers[peerID] = in
	r.mu.Unlock()

	s := &Session{
		backend: l,
		roomID:  roomID,
		peerID:  peerID,
		out:     out,
		in:      in,
		done:    make(chan struct{}),
	}

	// Pump outbound messages from this peer into every other peer's
	// inbox. The pump exits when the session is closed (out drained,
	// done closed) or when ctx is canceled.
	go func() {
		for {
			select {
			case msg := <-out:
				l.broadcast(r, peerID, msg)
			case <-s.done:
				return
			case <-ctx.Done():
				s.Close()
				return
			}
		}
	}()

	return s, nil
}

// broadcast delivers msg to every peer in r except sender.
func (l *Local) broadcast(r *room, sender string, msg *SignalMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for peerID, inbox := range r.peers {
		if peerID == sender {
			continue
		}
		// Non-blocking send: a full inbox means the receiver is stuck
		// or gone. We drop instead of blocking the broadcaster.
		select {
		case inbox <- msg:
		default:
			// Inbox overflow; signaling is best-effort for Phase 1.
		}
	}
}

// leave removes a peer from its room and closes its inbox.
func (l *Local) leave(roomID, peerID string) {
	l.mu.Lock()
	r, ok := l.rooms[roomID]
	l.mu.Unlock()
	if !ok {
		return
	}
	r.mu.Lock()
	in, ok := r.peers[peerID]
	if ok {
		delete(r.peers, peerID)
	}
	r.mu.Unlock()
	if ok {
		close(in)
	}

	// Drop empty rooms.
	l.mu.Lock()
	if r, still := l.rooms[roomID]; still {
		r.mu.Lock()
		empty := len(r.peers) == 0
		r.mu.Unlock()
		if empty {
			delete(l.rooms, roomID)
		}
	}
	l.mu.Unlock()
}

// Stats returns room/peer counts for diagnostics. Used by tests.
func (l *Local) Stats() (rooms, peers int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rooms = len(l.rooms)
	for _, r := range l.rooms {
		r.mu.Lock()
		peers += len(r.peers)
		r.mu.Unlock()
	}
	return
}

// awaitPeerCount is a test helper that waits until the room has the
// given number of peers (or is gone, when want == 0) or timeout
// elapses. Not part of the Backend contract.
func (l *Local) awaitPeerCount(roomID string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		l.mu.Lock()
		r, ok := l.rooms[roomID]
		l.mu.Unlock()
		if !ok {
			if want == 0 {
				return true
			}
		} else {
			r.mu.Lock()
			got := len(r.peers)
			r.mu.Unlock()
			if got >= want {
				return true
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
