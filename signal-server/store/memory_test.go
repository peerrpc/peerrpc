package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/peerrpc/signal-server/store"
)

func TestMemory_JoinLeave(t *testing.T) {
	s := store.NewMemory()

	alice := store.Peer{ID: "alice", Service: "r1"}
	bob := store.Peer{ID: "bob", Service: "r1"}

	_, arx, err := s.Join(context.Background(), alice)
	if err != nil {
		t.Fatalf("alice Join: %v", err)
	}
	_, brx, err := s.Join(context.Background(), bob)
	if err != nil {
		t.Fatalf("bob Join: %v", err)
	}

	if stats := s.Stats(); stats.Services != 1 || stats.Peers != 2 {
		t.Fatalf("Stats after joins: %+v", stats)
	}

	// alice broadcasts; bob should receive.
	asx, _, _ := s.Join(context.Background(), alice) // returns ErrPeerAlreadyExists; intentional check below
	_ = asx
	if asx != nil {
		t.Fatalf("re-joining alice should fail")
	}

	// Use a fresh send path by re-acquiring alice through the join
	// already done. The original Sender was discarded above; instead
	// just verify Leave clears state.
	if err := s.Leave(context.Background(), alice); err != nil {
		t.Fatalf("Leave alice: %v", err)
	}
	if err := s.Leave(context.Background(), bob); err != nil {
		t.Fatalf("Leave bob: %v", err)
	}

	// Inbox channels close on Leave.
	select {
	case _, ok := <-arx.Recv():
		if ok {
			t.Fatal("alice inbox still open after Leave")
		}
	case <-time.After(time.Second):
		t.Fatal("alice inbox never closed")
	}
	select {
	case _, ok := <-brx.Recv():
		if ok {
			t.Fatal("bob inbox still open after Leave")
		}
	case <-time.After(time.Second):
		t.Fatal("bob inbox never closed")
	}

	if stats := s.Stats(); stats.Services != 0 || stats.Peers != 0 {
		t.Fatalf("Stats after leaves: %+v", stats)
	}
}

func TestMemory_DuplicatePeerRejected(t *testing.T) {
	s := store.NewMemory()
	p := store.Peer{ID: "x", Service: "r"}
	if _, _, err := s.Join(context.Background(), p); err != nil {
		t.Fatalf("first Join: %v", err)
	}
	if _, _, err := s.Join(context.Background(), p); !errors.Is(err, store.ErrPeerAlreadyExists) {
		t.Fatalf("got %v, want ErrPeerAlreadyExists", err)
	}
}

func TestMemory_ServiceFullRejected(t *testing.T) {
	s := store.NewMemory()
	for i := 0; i < store.MaxPeersPerService; i++ {
		p := store.Peer{ID: string(rune('a' + i)), Service: "r"}
		if _, _, err := s.Join(context.Background(), p); err != nil {
			t.Fatalf("Join %d: %v", i, err)
		}
	}
	p := store.Peer{ID: "extra", Service: "r"}
	if _, _, err := s.Join(context.Background(), p); !errors.Is(err, store.ErrServiceFull) {
		t.Fatalf("got %v, want ErrServiceFull", err)
	}
}

func TestMemory_BroadcastNoEcho(t *testing.T) {
	s := store.NewMemory()
	alice := store.Peer{ID: "alice", Service: "r"}
	bob := store.Peer{ID: "bob", Service: "r"}

	asx, arx, err := s.Join(context.Background(), alice)
	if err != nil {
		t.Fatal(err)
	}
	_, brx, err := s.Join(context.Background(), bob)
	if err != nil {
		t.Fatal(err)
	}

	msg := store.SignalMessage{Body: "hello"}
	if err := asx.Send(context.Background(), msg); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case got := <-brx.Recv():
		if got.SenderID != "alice" {
			t.Fatalf("sender: %q", got.SenderID)
		}
	case <-time.After(time.Second):
		t.Fatal("bob did not receive")
	}

	// alice should NOT see her own broadcast.
	select {
	case got := <-arx.Recv():
		t.Fatalf("alice saw her own msg: %+v", got)
	case <-time.After(50 * time.Millisecond):
		// good
	}
}
