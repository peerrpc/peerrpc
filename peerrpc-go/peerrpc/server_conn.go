package peerrpc

import (
	"sync"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
)

// ServerConn is the server-side analog of Conn. It owns a single
// accepted WebRTC DataChannel plus the signaling Session that
// produced it; the caller hands the underlying transport.Channel to
// an rpc.Server via Serve.
//
// ServerConn deliberately does NOT wrap an rpc.Client: on the server
// side the caller brings their own rpc.Server (with their service
// handlers registered), and rpc.Server.Serve takes a *transport.Channel
// directly.
type ServerConn struct {
	channel *transport.Channel
	peer    *peer.Peer
	session *signal.Session
	peerID  string

	once sync.Once
}

// newServerConn assembles a ServerConn from an accepted channel. The
// caller (Listener.Accept) has already done peer.Accept; we only
// own lifetime.
func newServerConn(ch *transport.Channel, p *peer.Peer, s *signal.Session, peerID string) *ServerConn {
	return &ServerConn{
		channel: ch,
		peer:    p,
		session: s,
		peerID:  peerID,
	}
}

// Channel returns the underlying transport.Channel. Pass this to
// rpc.Server.Serve.
func (c *ServerConn) Channel() *transport.Channel { return c.channel }

// PeerID returns the peer_id this server-side peer used on the wire.
// Useful for logging when the listener auto-suffixed it.
func (c *ServerConn) PeerID() string { return c.peerID }

// Close tears the server-side connection down:
//
//  1. Close the transport.Channel (drains inbound frames).
//  2. Close the WebRTC PeerConnection.
//  3. Close the signaling Session (leaves the service).
//
// Idempotent.
func (c *ServerConn) Close() error {
	c.once.Do(func() {
		_ = c.channel.Close()
		_ = c.peer.Close()
		_ = c.session.Close()
	})
	return nil
}
