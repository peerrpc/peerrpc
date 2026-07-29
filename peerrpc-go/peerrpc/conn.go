package peerrpc

import (
	"context"
	"errors"
	"sync"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
)

// Conn is the long-lived handle returned by Dial. It owns the full
// peer stack: the signaling Session, the WebRTC PeerConnection, the
// transport.Channel, and the rpc.Client pumping frames through it.
//
// Conn is goroutine-safe for Invoke* calls. Close is idempotent.
//
// Conn deliberately does NOT expose the underlying peer.Peer or
// transport.Channel: callers that need low-level access should keep
// using the per-package APIs. The facade is for the common case
// (open a channel, fire RPCs, close it).
type Conn struct {
	// Static, set once at construction.
	client  *rpc.Client
	channel *transport.Channel
	peer    *peer.Peer
	session *signal.Session
	peerID  string

	// Lifecycle.
	cancel context.CancelFunc // cancels the Attach loop
	done   chan struct{}      // closed when Attach returns
	closed chan struct{}      // closed when Close returns
	once   sync.Once
	attachErr error
}

// newConn assembles a Conn from already-connected primitives. The
// caller has already performed peer.Dial/Accept; newConn takes
// ownership of all four objects and starts the Attach pump.
func newConn(
	ctx context.Context,
	client *rpc.Client,
	channel *transport.Channel,
	peer *peer.Peer,
	session *signal.Session,
	peerID string,
) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Conn{
		client:  client,
		channel: channel,
		peer:    peer,
		session: session,
		peerID:  peerID,
		cancel:  cancel,
		done:    make(chan struct{}),
		closed:  make(chan struct{}),
	}
	// Attach is a blocking pump; run it in a goroutine that signals
	// completion on c.done so Close can wait for a clean shutdown.
	go func() {
		defer close(c.done)
		c.attachErr = client.Attach(ctx)
		// Attach returns nil on graceful close; surface a wrapped
		// error otherwise. Conn.Close is the canonical way to drain.
	}()
	return c
}

// PeerID returns the peer_id used on the wire. Useful for logging
// when Target.PeerID was empty and the resolver auto-generated one.
func (c *Conn) PeerID() string { return c.peerID }

// InvokeUnary forwards to rpc.Client.InvokeUnary. See its docs for
// semantics; the signature is repeated here to avoid forcing callers
// to import both packages for the common case.
func (c *Conn) InvokeUnary(ctx context.Context, method string, req []byte, hdr map[string][]string) ([]byte, *rpc.Status) {
	return c.client.InvokeUnary(ctx, method, req, hdr)
}

// InvokeServerStreaming forwards to rpc.Client.InvokeServerStreaming.
func (c *Conn) InvokeServerStreaming(ctx context.Context, method string, req []byte, hdr map[string][]string) (*rpc.ClientStream, *rpc.Status) {
	return c.client.InvokeServerStreaming(ctx, method, req, hdr)
}

// InvokeClientStreaming forwards to rpc.Client.InvokeClientStreaming.
func (c *Conn) InvokeClientStreaming(ctx context.Context, method string, firstReq []byte, hdr map[string][]string) (*rpc.ClientStream, *rpc.Status) {
	return c.client.InvokeClientStreaming(ctx, method, firstReq, hdr)
}

// InvokeBidiStreaming forwards to rpc.Client.InvokeBidiStreaming.
func (c *Conn) InvokeBidiStreaming(ctx context.Context, method string, firstReq []byte, hdr map[string][]string) (*rpc.ClientStream, *rpc.Status) {
	return c.client.InvokeBidiStreaming(ctx, method, firstReq, hdr)
}

// Client exposes the underlying rpc.Client for callers that need to
// install interceptors or access lower-level RPC state after Dial.
// The returned Client is shared; do not call its Close (there is
// none) — close the Conn instead.
func (c *Conn) Client() *rpc.Client { return c.client }

// Close tears the connection down in the strictest order required
// by the underlying stack:
//
//  1. Cancel the Attach pump and wait for it to exit.
//  2. Close the transport.Channel (drains inbound frames).
//  3. Close the WebRTC PeerConnection.
//  4. Close the signaling Session (leaves the service).
//
// Each step is idempotent; subsequent Close calls return nil.
//
// We do not surface the Attach error: by the time Close is called,
// the caller has either observed the failure (RPC returned an err)
// or is shutting down voluntarily.
func (c *Conn) Close() error {
	c.once.Do(func() {
		defer close(c.closed)

		// 1. Stop the Attach pump.
		c.cancel()
		<-c.done

		// 2. Close the transport channel (signals OnClose on the
		//    DataChannel; harmless if already closed).
		_ = c.channel.Close()

		// 3. Tear down the PeerConnection.
		_ = c.peer.Close()

		// 4. Leave the signaling service.
		_ = c.session.Close()
	})
	return nil
}

// Done returns a channel that is closed when the underlying Attach
// pump exits, either because Close was called or because the remote
// peer / transport failed. Useful for watcher loops.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err returns the error that ended the Attach pump, or nil if it
// closed cleanly. Err returns nil before Done is closed.
func (c *Conn) Err() error {
	select {
	case <-c.done:
		if c.attachErr == nil {
			return nil
		}
		return c.attachErr
	default:
		return nil
	}
}

// errClosed is returned by Invoke* after Close has been called.
var errClosed = errors.New("peerrpc: connection closed")
