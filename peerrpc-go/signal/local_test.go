package signal

import (
	"context"
	"testing"
	"time"
)

func TestLocal_TwoPeersBroadcast(t *testing.T) {
	backend := NewLocal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	alice, err := backend.Exchange(ctx, "room-1", "alice")
	if err != nil {
		t.Fatalf("alice Exchange: %v", err)
	}
	defer alice.Close()
	bob, err := backend.Exchange(ctx, "room-1", "bob")
	if err != nil {
		t.Fatalf("bob Exchange: %v", err)
	}
	defer bob.Close()

	if !backend.awaitPeerCount("room-1", 2, time.Second) {
		t.Fatal("expected 2 peers in room-1")
	}

	// Alice broadcasts an offer; bob should receive it (but not alice).
	if err := alice.Send(ctx, &SignalMessage{
		RoomID: "room-1",
		Body:   SignalBody{Offer: &SdpOffer{Sdp: "v=0\r\no=- alice"}},
	}); err != nil {
		t.Fatalf("alice Send: %v", err)
	}

	select {
	case got := <-bob.Receive():
		if got.Body.Offer == nil || got.Body.Offer.Sdp != "v=0\r\no=- alice" {
			t.Fatalf("bob got wrong message: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("bob did not receive alice's offer")
	}

	// Alice must not receive her own broadcast.
	select {
	case got := <-alice.Receive():
		t.Fatalf("alice received her own message: %+v", got)
	case <-time.After(50 * time.Millisecond):
		// good
	}
}

func TestLocal_DuplicatePeerRejected(t *testing.T) {
	backend := NewLocal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first, err := backend.Exchange(ctx, "room-2", "carol")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	if _, err := backend.Exchange(ctx, "room-2", "carol"); err == nil {
		t.Fatal("expected duplicate peer to be rejected")
	}
}

func TestLocal_CloseRemovesPeer(t *testing.T) {
	backend := NewLocal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := backend.Exchange(ctx, "room-3", "dan")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !backend.awaitPeerCount("room-3", 0, time.Second) {
		rooms, peers := backend.Stats()
		t.Fatalf("dan was not removed; rooms=%d peers=%d", rooms, peers)
	}
}

func TestLocal_CloseIsIdempotent(t *testing.T) {
	backend := NewLocal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := backend.Exchange(ctx, "room-4", "eve")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
