// Package relay implements the v1 PeerRPC application-layer relay.
//
// Relay is the fallback for two peers that cannot establish a direct
// P2P connection (symmetric NAT, restrictive firewalls, no common
// TURN server). v1 is intentionally minimal:
//
//   * Single node. No mesh routing, no auto-discovery. The
//     application points the client at the relay's static address.
//   * Pure forwarding. The relay does not interpret or transform
//     PeerRPC frames; it moves bytes between the two DataChannels.
//   * No failover. If the relay dies every in-flight RPC through it
//     fails with UNAVAILABLE. Application-layer retry is the policy.
//
// Architecture:
//
//   peer A  ─── DC#1 ─── ▼                  ▼  ─── DC#2 ───  peer B
//                       relay (one goroutine pair per session)
//
// The relay node participates in the signaling service as Answerer.
// Once a DataChannel is open the relay installs a byte-pump that
// forwards every frame verbatim in both directions.
//
// Wire protocol on top of PeerRPC frames is intentionally absent: a
// relayed RPC is bit-identical to a P2P RPC; the only difference is
// how the DataChannel came into being.
package relay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
)

// Config carries the relay node's tuning knobs.
type Config struct {
	// Peer lets the caller inject a custom peer.Config (e.g. extra
	// ICEServers, mDNS policy). If empty a localhost-friendly default
	// is used; relay nodes typically want a TURN server of their own
	// since they may sit behind restrictive networks.
	Peer peer.Config

	// Signaling is the backend the relay uses to rendezvous with
	// peers. In production this is a connect-go client pointing at
	// the standalone signal-server binary.
	Signaling signal.Backend

	// ForwardBufferSize bounds the per-direction relay queue. 256
	// is plenty for one RPC's frame burst; raise for high-throughput
	// streaming workloads.
	ForwardBufferSize int

	// Logger receives structured events (session open/close, bytes
	// forwarded, errors). nil falls back to slog.Default().
	Logger *slog.Logger
}

// Server is the v1 relay node. It accepts DataChannels from peers
// and pumps frames between paired sessions in the same room.
//
// One Server instance serves one room (the v1 binary is invoked
// per room). v1.1 may lift this to multi-room multiplexing.
type Server struct {
	cfg     Config
	logger  *slog.Logger
	peerCfg peer.Config

	mu       sync.Mutex
	sessions map[string]*session
	closed   chan struct{}
}

// New constructs a relay node.
func New(cfg Config) (*Server, error) {
	if cfg.Signaling == nil {
		return nil, errors.New("relay: Config.Signaling is required")
	}
	if cfg.ForwardBufferSize == 0 {
		cfg.ForwardBufferSize = 256
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{
		cfg:      cfg,
		logger:   logger,
		peerCfg:  cfg.Peer,
		sessions: map[string]*session{},
		closed:   make(chan struct{}),
	}, nil
}

// session is the per-room relay state. Two peers connect to the relay
// (each as its own DataChannel); once both are open the forwarder
// pairs them up.
type session struct {
	roomID string

	mu      sync.Mutex
	peers   [2]*peerSlot
	forward chan struct{}
	once    sync.Once
}

// peerSlot bundles one peer's DataChannel with the outbound byte
// queue and a done signal.
type peerSlot struct {
	ch       *transport.Channel
	outbound chan []byte
	done     chan struct{}
}

// Serve starts the relay for the given room. The relay joins the
// room, accepts two DataChannels, and pumps bytes between them.
//
// Serve blocks until either peer disconnects, ctx is canceled, or
// Close is called.
func (s *Server) Serve(ctx context.Context, roomID, relayPeerID string) error {
	relaySig, err := s.cfg.Signaling.Exchange(ctx, roomID, relayPeerID)
	if err != nil {
		return fmt.Errorf("relay: signaling exchange: %w", err)
	}
	defer relaySig.Close()

	for i := 0; i < 2; i++ {
		if err := s.acceptOne(ctx, roomID, relaySig); err != nil {
			return fmt.Errorf("relay: accept #%d: %w", i+1, err)
		}
	}

	sess := s.session(roomID)
	if sess.peers[0] == nil || sess.peers[1] == nil {
		return errors.New("relay: incomplete session after accept loop")
	}
	s.pairAndPump(ctx, sess)
	return nil
}

// acceptOne accepts a DataChannel from a peer in the room and binds
// it to the next free slot in the session.
func (s *Server) acceptOne(ctx context.Context, roomID string, sig *signal.Session) error {
	p, err := peer.New(ctx, signal.RoleAnswerer, s.peerCfg)
	if err != nil {
		return fmt.Errorf("peer.New: %w", err)
	}

	type acceptResult struct {
		ch  *transport.Channel
		err error
	}
	res := make(chan acceptResult, 1)
	go func() {
		ch, err := p.Accept(ctx, sig)
		res <- acceptResult{ch, err}
	}()

	select {
	case r := <-res:
		if r.err != nil {
			p.Close()
			return r.err
		}
		slot := &peerSlot{
			ch:       r.ch,
			outbound: make(chan []byte, s.cfg.ForwardBufferSize),
			done:     make(chan struct{}),
		}
		s.bindSlot(roomID, slot)
		go func() {
			<-r.ch.Closed()
			close(slot.done)
		}()
		return nil
	case <-ctx.Done():
		p.Close()
		return ctx.Err()
	}
}

// session fetches-or-creates the per-room state.
func (s *Server) session(roomID string) *session {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[roomID]
	if !ok {
		sess = &session{roomID: roomID, forward: make(chan struct{})}
		s.sessions[roomID] = sess
	}
	return sess
}

// bindSlot assigns slot to the next free index in the session.
func (s *Server) bindSlot(roomID string, slot *peerSlot) {
	sess := s.session(roomID)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	for i := range sess.peers {
		if sess.peers[i] == nil {
			sess.peers[i] = slot
			return
		}
	}
	sess.peers[0] = slot
}

// pairAndPump wires the two slots together and pumps bytes between
// them until one side closes.
func (s *Server) pairAndPump(ctx context.Context, sess *session) {
	sess.once.Do(func() { close(sess.forward) })

	a := sess.peers[0]
	b := sess.peers[1]

	s.logger.Info("relay session paired", "room_id", sess.roomID)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); s.forwardBytes(ctx, a, b) }()
	go func() { defer wg.Done(); s.forwardBytes(ctx, b, a) }()

	select {
	case <-a.done:
	case <-b.done:
	case <-ctx.Done():
	}
	wg.Wait()

	s.logger.Info("relay session ended", "room_id", sess.roomID)

	s.mu.Lock()
	delete(s.sessions, sess.roomID)
	s.mu.Unlock()
}

// forwardBytes pumps inbound bytes from src.ch into dst.ch via an
// intermediate outbound queue. The two goroutines per direction
// decouple slow producers from slow consumers without head-of-line
// blocking the other side.
func (s *Server) forwardBytes(ctx context.Context, src, dst *peerSlot) {
	// reader: pull bytes off src, push into dst.outbound.
	go func() {
		for {
			payload, err := src.ch.Recv(ctx)
			if err != nil {
				return
			}
			select {
			case dst.outbound <- payload:
			case <-dst.done:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	// writer: drain dst.outbound onto dst.ch verbatim. The bytes
	// are pre-marshaled frames from the remote peer; SendRaw
	// preserves them without re-encoding.
	for {
		select {
		case payload := <-dst.outbound:
			if err := dst.ch.SendRaw(ctx, payload); err != nil {
				return
			}
		case <-dst.done:
			return
		case <-ctx.Done():
			return
		}
	}
}

// Close stops the relay node. Subsequent Serve calls return
// immediately.
func (s *Server) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return nil
}

// Stats snapshots the relay's current state for diagnostics.
type Stats struct {
	Sessions int
}

// Stats returns the current session count.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{Sessions: len(s.sessions)}
}
