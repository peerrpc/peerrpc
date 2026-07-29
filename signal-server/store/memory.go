package store

import (
	"context"
	"sync"
	"time"
)

// MaxPeersPerRoom caps how many peers may join the same room.
//
// PeerRPC supports one Answerer (server) serving multiple Offerers
// (clients) in the same room. The cap prevents pathological fan-out.
const MaxPeersPerRoom = 10

// Memory is an in-process Store. It is the default for the
// standalone signal-server binary and the only backend v1 ships.
//
// Memory is safe for concurrent use by any number of peers and
// rooms. Empty rooms are garbage-collected so a server that handles
// many short-lived rooms does not leak.
type Memory struct {
	mu    sync.Mutex
	rooms map[string]*memoryRoom
}

// NewMemory constructs an empty in-memory store.
func NewMemory() *Memory {
	return &Memory{rooms: make(map[string]*memoryRoom)}
}

type memoryRoom struct {
	mu    sync.Mutex
	peers map[string]*memoryPeer
}

type memoryPeer struct {
	id     string
	roomID string
	inbox  chan SignalMessage
	// inboxClosed avoids double-close panics when Leave races with
	// the room's own teardown.
	inboxClosed bool
	closeMu     sync.Mutex
}

// Join implements Store.
func (m *Memory) Join(ctx context.Context, peer Peer) (Sender, Receiver, error) {
	if peer.RoomID == "" {
		return nil, nil, ErrPeerAlreadyExists // sentinel: invalid input
	}
	if peer.ID == "" {
		return nil, nil, ErrPeerAlreadyExists
	}

	room := m.roomForJoin(peer.RoomID)

	room.mu.Lock()
	if _, exists := room.peers[peer.ID]; exists {
		room.mu.Unlock()
		return nil, nil, ErrPeerAlreadyExists
	}
	if len(room.peers) >= MaxPeersPerRoom {
		room.mu.Unlock()
		return nil, nil, ErrRoomFull
	}
	mp := &memoryPeer{
		id:     peer.ID,
		roomID: peer.RoomID,
		// 32 is plenty: signaling exchanges a handful of SDP/ICE
		// frames; buffered so a slow receiver does not block the
		// broadcaster during a brief burst.
		inbox: make(chan SignalMessage, 32),
	}
	room.peers[peer.ID] = mp
	room.mu.Unlock()

	sender := &memorySender{store: m, peer: mp, room: room}
	receiver := &memoryReceiver{inbox: mp.inbox}
	return sender, receiver, nil
}

// roomForJoin returns the room for roomID, creating an empty one if
// needed. Callers MUST take room.mu themselves before mutating peers.
func (m *Memory) roomForJoin(roomID string) *memoryRoom {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.rooms[roomID]
	if !ok {
		r = &memoryRoom{peers: make(map[string]*memoryPeer)}
		m.rooms[roomID] = r
	}
	return r
}

// Leave implements Store.
func (m *Memory) Leave(_ context.Context, peer Peer) error {
	m.mu.Lock()
	room, ok := m.rooms[peer.RoomID]
	m.mu.Unlock()
	if !ok {
		return nil
	}

	room.mu.Lock()
	mp, ok := room.peers[peer.ID]
	if !ok {
		room.mu.Unlock()
		return nil
	}
	delete(room.peers, peer.ID)
	empty := len(room.peers) == 0
	room.mu.Unlock()

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
		if r, still := m.rooms[peer.RoomID]; still && len(r.peers) == 0 {
			delete(m.rooms, peer.RoomID)
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
		Rooms:     len(m.rooms),
		RoomPeers: map[string]int{},
	}
	for id, r := range m.rooms {
		r.mu.Lock()
		n := len(r.peers)
		r.mu.Unlock()
		out.RoomPeers[id] = n
		out.Peers += n
	}
	return out
}

// memorySender broadcasts messages from one peer to its room.
type memorySender struct {
	store *Memory
	peer  *memoryPeer
	room  *memoryRoom
}

// Send implements Sender. Non-blocking on slow receivers; overflow
// drops rather than back-pressures the broadcaster.
func (s *memorySender) Send(ctx context.Context, msg SignalMessage) error {
	msg.SenderID = s.peer.id
	msg.RoomID = s.peer.roomID

	s.room.mu.Lock()
	defer s.room.mu.Unlock()
	for id, mp := range s.room.peers {
		if id == s.peer.id {
			continue
		}
		select {
		case mp.inbox <- msg:
		default:
			// Inbox overflow: best-effort for v1.
		}
	}
	return nil
}

// Close implements Sender. The sender does not own any resources
// beyond the peer's inbox, which is closed by Leave.
func (s *memorySender) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.store.Leave(ctx, Peer{ID: s.peer.id, RoomID: s.peer.roomID})
}

// memoryReceiver adapts a peer's inbox chan into the Receiver
// interface.
type memoryReceiver struct {
	inbox chan SignalMessage
}

func (r *memoryReceiver) Recv() <-chan SignalMessage { return r.inbox }
