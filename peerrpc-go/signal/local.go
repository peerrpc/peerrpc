// Package signal provides the PeerRPC signaling client and pluggable
// backends.
//
// Two peers MUST exchange SDP offers/answers and ICE candidates over
// some shared channel before a WebRTC DataConnection can form. That
// channel is the "signaling" path and is intentionally decoupled from
// the WebRTC data path: any rendezvous mechanism works.
//
// The package ships an in-process backend (`Local`) suitable for
// tests and single-binary demos, and a WebSocket network backend
// (`WS`) that speaks the signaling wire format (service /
// AnnounceRequest) to a remote signal-server.
package signal

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// SignalMessage mirrors proto/peerrpc/signaling/signaling.proto
// without dragging the generated package into every caller. Backends
// exchange pointers to these values, so they should be treated as
// read-only by receivers.
type SignalMessage struct {
	Service string
	Body    SignalBody
}

// SignalBody is a tagged union corresponding to the proto oneof.
// Only one field is populated per message.
type SignalBody struct {
	Announce  *AnnounceRequest
	Offer     *SdpOffer
	Answer    *SdpAnswer
	Candidate *IceCandidate
	Leave     *LeaveRequest
	Ping      *Ping
}

// AnnounceRequest is the first message a peer sends to declare its
// presence against a service. The signal-server records (peer_id,
// service) and starts broadcasting subsequent messages to other
// peers in the same service.
type AnnounceRequest struct {
	PeerID string
	// PeerPubkey carries an Ed25519 public key when the peer opts
	// into the strong-identity model. Servers accept this field
	// but do not verify; full verification is planned for a future release.
	PeerPubkey []byte
	Role       Role
}

// Role disambiguates the peer's application-level part. The WebRTC
// offer direction is derived by the SDK from this (Client = Offerer,
// Server = Answerer) and is not a signaling-layer concern.
type Role int

const (
	RoleUnspecified Role = 0
	RoleClient      Role = 1
	RoleServer      Role = 2
	RoleRelay       Role = 3
	RoleBridge      Role = 4
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
	Candidate     string
	SdpMid        string
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

// Backend is the rendezvous transport. The client runs a
// bidirectional Exchange against it: outbound messages are sent via
// Send, inbound messages arrive on the channel returned by Receive.
//
// Backends MUST guarantee:
//   - Each peer in a service receives every other peer's messages
//     except its own (broadcast to others).
//   - Messages sent before a peer joins are not delivered to it (no
//     buffering).
//   - Close is idempotent and also drains the receive channel.
type Backend interface {
	// Exchange connects the caller to service under peerID and
	// returns a Session whose channel yields every message
	// broadcast into the service by other peers. The session is
	// closed when ctx is canceled or Close is called.
	Exchange(ctx context.Context, service, peerID string) (*Session, error)
}

// Session is a peer's presence in a signaling service.
//
// It provides bidirectional communication: Send broadcasts a message
// to every other peer in the service, Receive returns a channel of
// incoming messages from other peers.
type Session struct {
	service string
	peerID  string
	out     chan<- *SignalMessage
	in      <-chan *SignalMessage
	done    chan struct{}
	cleanup func() // called by Close to release backend resources
	once    sync.Once
}

// Send broadcasts msg to every other peer in the service.
func (s *Session) Send(ctx context.Context, msg *SignalMessage) error {
	msg.Service = s.service
	select {
	case s.out <- msg:
		return nil
	case <-s.done:
		return errors.New("signal: session closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Service returns the service id this session announced against.
func (s *Session) Service() string { return s.service }

// PeerID returns the peer_id used on the wire.
func (s *Session) PeerID() string { return s.peerID }

// Receive returns the inbound channel.
func (s *Session) Receive() <-chan *SignalMessage { return s.in }

// Close leaves the service. Safe to call multiple times.
func (s *Session) Close() error {
	s.once.Do(func() {
		if s.cleanup != nil {
			s.cleanup()
		}
		close(s.done)
	})
	return nil
}

// Local is an in-process Backend. It is the default for tests and
// single-binary demos. Safe for concurrent use by any number of
// peers and services. Empty services are garbage-collected.
type Local struct {
	mu       sync.Mutex
	services map[string]*localService
}

// NewLocal constructs an empty in-process backend.
func NewLocal() *Local {
	return &Local{services: make(map[string]*localService)}
}

type localService struct {
	id    string // back-reference for GC of empty entries
	mu    sync.Mutex
	peers map[string]*localPeer
}

type localPeer struct {
	id      string
	inbox   chan *SignalMessage
	closed  bool
	closeMu sync.Mutex
}

// Exchange implements Backend.
func (l *Local) Exchange(_ context.Context, service, peerID string) (*Session, error) {
	if service == "" {
		return nil, errors.New("signal: empty service")
	}
	if peerID == "" {
		return nil, errors.New("signal: empty peer id")
	}

	svc := l.serviceForExchange(service)
	svc.mu.Lock()
	if _, exists := svc.peers[peerID]; exists {
		svc.mu.Unlock()
		return nil, fmt.Errorf("signal: peer %q already in service %q", peerID, service)
	}
	p := &localPeer{
		id:    peerID,
		inbox: make(chan *SignalMessage, 32),
	}
	svc.peers[peerID] = p
	svc.mu.Unlock()

	in := make(chan *SignalMessage, 32)
	out := make(chan *SignalMessage, 32)

	ctx, cancel := context.WithCancel(context.Background())

	// Outbound pump: read from out, broadcast to other peers.
	go func() {
		for {
			select {
			case msg, ok := <-out:
				if !ok {
					return
				}
				l.broadcast(svc, p, msg)
			case <-ctx.Done():
				return
			}
		}
	}()

	// Inbox → in pump: forward messages from p.inbox to in.
	go func() {
		defer close(in)
		for {
			select {
			case m, ok := <-p.inbox:
				if !ok {
					return
				}
				select {
				case in <- m:
				case <-ctx.Done():
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	s := &Session{
		service: service,
		peerID:  peerID,
		out:     out,
		in:      in,
		done:    make(chan struct{}),
		cleanup: func() {
			cancel()
			l.leave(svc, p)
			close(out)
		},
	}
	return s, nil
}

func (l *Local) serviceForExchange(service string) *localService {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.services[service]
	if !ok {
		s = &localService{id: service, peers: make(map[string]*localPeer)}
		l.services[service] = s
	}
	return s
}

func (l *Local) broadcast(svc *localService, from *localPeer, msg *SignalMessage) {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	for id, p := range svc.peers {
		if id == from.id {
			continue
		}
		select {
		case p.inbox <- msg:
		default:
			// Inbox overflow: best-effort.
		}
	}
}

func (l *Local) leave(svc *localService, p *localPeer) {
	svc.mu.Lock()
	delete(svc.peers, p.id)
	empty := len(svc.peers) == 0
	svc.mu.Unlock()

	p.closeMu.Lock()
	if !p.closed {
		close(p.inbox)
		p.closed = true
	}
	p.closeMu.Unlock()

	if empty {
		l.mu.Lock()
		if s, still := l.services[svc.id]; still && len(s.peers) == 0 {
			delete(l.services, svc.id)
		}
		l.mu.Unlock()
	}
}

// Stats returns service/peer counts for diagnostics.
type Stats struct {
	Services     int
	Peers        int
	ServicePeers map[string]int
}

// Stats snapshots the Local's current state.
func (l *Local) Stats() Stats {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := Stats{
		Services:     len(l.services),
		ServicePeers: map[string]int{},
	}
	for id, s := range l.services {
		s.mu.Lock()
		n := len(s.peers)
		s.mu.Unlock()
		out.ServicePeers[id] = n
		out.Peers += n
	}
	return out
}
