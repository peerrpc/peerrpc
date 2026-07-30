package store

import (
	"context"
	"sync"
	"time"
)

// MaxPeersPerService caps how many peers may join the same service.
//
// PeerRPC supports one Server serving multiple Clients in the same
// service. The cap prevents pathological fan-out.
const MaxPeersPerService = 10

// Memory is an in-process Store. It is the default backend for the
// signalserver package and the only backend v1 ships.
//
// Memory is safe for concurrent use by any number of peers and
// services. Empty services are garbage-collected so a server that
// handles many short-lived services does not leak.
type Memory struct {
	mu       sync.Mutex
	services map[string]*memoryService
}

// NewMemory constructs an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{services: make(map[string]*memoryService)}
}

type memoryService struct {
	mu    sync.Mutex
	peers map[string]*memoryPeer
}

type memoryPeer struct {
	id      string
	service string
	inbox   chan SignalMessage
	// inboxClosed avoids double-close panics when Leave races with
	// the service's own teardown.
	inboxClosed bool
	closeMu     sync.Mutex
}

// Join implements Store.
func (m *Memory) Join(ctx context.Context, peer Peer) (Sender, Receiver, error) {
	if peer.Service == "" {
		return nil, nil, ErrPeerAlreadyExists // sentinel: invalid input
	}
	if peer.ID == "" {
		return nil, nil, ErrPeerAlreadyExists
	}

	svc := m.serviceForJoin(peer.Service)

	svc.mu.Lock()
	if _, exists := svc.peers[peer.ID]; exists {
		svc.mu.Unlock()
		return nil, nil, ErrPeerAlreadyExists
	}
	if len(svc.peers) >= MaxPeersPerService {
		svc.mu.Unlock()
		return nil, nil, ErrServiceFull
	}
	mp := &memoryPeer{
		id:      peer.ID,
		service: peer.Service,
		// 32 is plenty: signaling exchanges a handful of SDP/ICE
		// frames; buffered so a slow receiver does not block the
		// broadcaster during a brief burst.
		inbox: make(chan SignalMessage, 32),
	}
	svc.peers[peer.ID] = mp
	svc.mu.Unlock()

	sender := &memorySender{store: m, peer: mp, service: svc}
	receiver := &memoryReceiver{inbox: mp.inbox}
	return sender, receiver, nil
}

// serviceForJoin returns the service for serviceID, creating an
// empty one if needed. Callers MUST take svc.mu themselves before
// mutating peers.
func (m *Memory) serviceForJoin(serviceID string) *memoryService {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.services[serviceID]
	if !ok {
		s = &memoryService{peers: make(map[string]*memoryPeer)}
		m.services[serviceID] = s
	}
	return s
}

// Leave implements Store.
func (m *Memory) Leave(_ context.Context, peer Peer) error {
	m.mu.Lock()
	svc, ok := m.services[peer.Service]
	m.mu.Unlock()
	if !ok {
		return nil
	}

	svc.mu.Lock()
	mp, ok := svc.peers[peer.ID]
	if !ok {
		svc.mu.Unlock()
		return nil
	}
	delete(svc.peers, peer.ID)
	empty := len(svc.peers) == 0
	svc.mu.Unlock()

	mp.closeMu.Lock()
	if !mp.inboxClosed {
		close(mp.inbox)
		mp.inboxClosed = true
	}
	mp.closeMu.Unlock()

	if empty {
		m.mu.Lock()
		// Re-check to avoid a race where another peer joined
		// between our snapshot and the lock acquisition.
		if s, still := m.services[peer.Service]; still && len(s.peers) == 0 {
			delete(m.services, peer.Service)
		}
		m.mu.Unlock()
	}
	return nil
}

// Stats implements Store.
func (m *Memory) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := Stats{
		Services:     len(m.services),
		ServicePeers: map[string]int{},
	}
	for id, s := range m.services {
		s.mu.Lock()
		n := len(s.peers)
		s.mu.Unlock()
		out.ServicePeers[id] = n
		out.Peers += n
	}
	return out
}

// memorySender broadcasts messages from one peer to its service.
type memorySender struct {
	store   *Memory
	peer    *memoryPeer
	service *memoryService
}

// Send implements Sender. Non-blocking on slow receivers; overflow
// drops rather than back-pressures the broadcaster.
func (s *memorySender) Send(ctx context.Context, msg SignalMessage) error {
	msg.SenderID = s.peer.id
	msg.Service = s.peer.service

	s.service.mu.Lock()
	defer s.service.mu.Unlock()
	for id, mp := range s.service.peers {
		if id == s.peer.id {
			continue
		}
		select {
		case mp.inbox <- msg:
		default:
			// Inbox overflow: best-effort.
		}
	}
	return nil
}

// Close implements Sender. The sender does not own any resources
// beyond the peer's inbox, which is closed by Leave.
func (s *memorySender) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.store.Leave(ctx, Peer{ID: s.peer.id, Service: s.peer.service})
}

// memoryReceiver adapts a peer's inbox chan into the Receiver
// interface.
type memoryReceiver struct {
	inbox chan SignalMessage
}

func (r *memoryReceiver) Recv() <-chan SignalMessage { return r.inbox }
