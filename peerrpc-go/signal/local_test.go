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

	alice, err := backend.Exchange(ctx, "svc-1", "alice")
	if err != nil {
		t.Fatalf("alice Exchange: %v", err)
	}
	defer alice.Close()
	bob, err := backend.Exchange(ctx, "svc-1", "bob")
	if err != nil {
		t.Fatalf("bob Exchange: %v", err)
	}
	defer bob.Close()

	if !backend.awaitPeerCount("svc-1", 2, time.Second) {
		t.Fatal("expected 2 peers in svc-1")
	}

	// Alice broadcasts an offer; bob should receive it (but not alice).
	if err := alice.Send(ctx, &SignalMessage{
		Service: "svc-1",
		Body:    SignalBody{Offer: &SdpOffer{Sdp: "v=0\r\no=- alice"}},
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

	first, err := backend.Exchange(ctx, "svc-2", "carol")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	if _, err := backend.Exchange(ctx, "svc-2", "carol"); err == nil {
		t.Fatal("expected duplicate peer to be rejected")
	}
}

func TestLocal_CloseRemovesPeer(t *testing.T) {
	backend := NewLocal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := backend.Exchange(ctx, "svc-3", "dan")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if !backend.awaitPeerCount("svc-3", 0, time.Second) {
		stats := backend.Stats()
		t.Fatalf("dan was not removed; services=%d peers=%d", stats.Services, stats.Peers)
	}
}

func TestLocal_CloseIsIdempotent(t *testing.T) {
	backend := NewLocal()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	s, err := backend.Exchange(ctx, "svc-4", "eve")
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

// awaitPeerCount polls until service has the wanted number of peers
// or the timeout expires. Local's broadcast is async (pump
// goroutines), so tests need to wait for joins to register.
func (l *Local) awaitPeerCount(service string, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		stats := l.Stats()
		if stats.ServicePeers[service] == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}
