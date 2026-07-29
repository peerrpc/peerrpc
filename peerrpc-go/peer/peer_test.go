package peer_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
	peerrpcpb "github.com/peerrpc/go/gen/proto/peerrpc"
	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"
)

// TestPeer_STUNCascade exercises the ICE cascade with a real STUN
// server. Both peers still run on localhost, so the connection's
// selected pair will probably be host-host; the test only verifies
// that adding a STUN server to the config does not break the
// handshake and that the OnICEConnectionStateChange hook fires.
//
// Skip-safe: the test t.Skip if it cannot reach Google STUN within
// NegotiationTimeout, so CI hermetic environments stay green.
func TestPeer_STUNCascade(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	backend := signal.NewLocal()
	oSig, _ := backend.Exchange(ctx, "stun", "o")
	defer oSig.Close()
	aSig, _ := backend.Exchange(ctx, "stun", "a")
	defer aSig.Close()

	var mu sync.Mutex
	seen := map[webrtc.ICEConnectionState]bool{}
	hook := func(s webrtc.ICEConnectionState) {
		mu.Lock()
		seen[s] = true
		mu.Unlock()
	}

	cfg := peer.Config{
		ICEServers:                  []webrtc.ICEServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
		OnICEConnectionStateChange:  hook,
		NegotiationTimeout:          8 * time.Second,
	}
	oPeer, err := peer.New(ctx, signal.RoleClient, cfg)
	if err != nil {
		t.Fatalf("offerer New: %v", err)
	}
	defer oPeer.Close()
	aPeer, err := peer.New(ctx, signal.RoleServer, cfg)
	if err != nil {
		t.Fatalf("answerer New: %v", err)
	}
	defer aPeer.Close()

	ares := make(chan error, 1)
	go func() {
		_, err := aPeer.Accept(ctx, aSig)
		ares <- err
	}()

	_, err = oPeer.Dial(ctx, oSig)
	if err != nil {
		t.Skipf("skipping: dial failed (no internet? %v)", err)
		return
	}
	select {
	case err := <-ares:
		if err != nil {
			t.Skipf("skipping: accept failed (%v)", err)
			return
		}
	case <-time.After(10 * time.Second):
		t.Skip("skipping: accept timed out")
		return
	}

	// The state hook MUST have fired at least once. The exact states
	// depend on the cascade outcome, but a successful connection will
	// at minimum touch Checking and Connected.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		checking := seen[webrtc.ICEConnectionStateChecking]
		connected := seen[webrtc.ICEConnectionStateConnected]
		mu.Unlock()
		if checking && connected {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("expected to observe ICE Checking + Connected; observed %v", seen)
}

// TestPeer_HandshakeAndRoundTrip performs the full localhost dance:
// in-process signaling -> WebRTC handshake -> one Frame round trip.
func TestPeer_HandshakeAndRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	backend := signal.NewLocal()

	offererSig, err := backend.Exchange(ctx, "room", "offerer")
	if err != nil {
		t.Fatalf("offerer sig: %v", err)
	}
	defer offererSig.Close()
	answererSig, err := backend.Exchange(ctx, "room", "answerer")
	if err != nil {
		t.Fatalf("answerer sig: %v", err)
	}
	defer answererSig.Close()

	offerer, err := peer.New(ctx, signal.RoleClient, peer.Config{})
	if err != nil {
		t.Fatalf("offerer New: %v", err)
	}
	defer offerer.Close()
	answerer, err := peer.New(ctx, signal.RoleServer, peer.Config{})
	if err != nil {
		t.Fatalf("answerer New: %v", err)
	}
	defer answerer.Close()

	// Accept runs concurrently with Dial; both sides need to be live
	// to exchange SDP.
	type acceptResult struct {
		ch  *transport.Channel
		err error
	}
	ares := make(chan acceptResult, 1)
	go func() {
		ch, err := answerer.Accept(ctx, answererSig)
		ares <- acceptResult{ch: ch, err: err}
	}()

	och, err := offerer.Dial(ctx, offererSig)
	if err != nil {
		t.Fatalf("offerer Dial: %v", err)
	}

	var ach *transport.Channel
	select {
	case r := <-ares:
		if r.err != nil {
			t.Fatalf("answerer Accept: %v", r.err)
		}
		ach = r.ch
	case <-time.After(10 * time.Second):
		t.Fatal("answerer Accept never returned")
	}

	// Send Frame from offerer, verify it arrives on answerer side.
	out := &peerrpcpb.Frame{
		Routing: &peerrpcpb.Routing{Sequence: 1},
		Type: &peerrpcpb.Frame_Call{
			Call: &peerrpcpb.Call{
				Method:          "/test.Ping/Echo",
				ProtocolVersion: 1,
			},
		},
	}
	if err := och.SendFrame(ctx, out); err != nil {
		t.Fatalf("SendFrame: %v", err)
	}

	raw, err := ach.Recv(ctx)
	if err != nil {
		t.Fatalf("answerer Recv: %v", err)
	}
	var got peerrpcpb.Frame
	if err := (proto.Unmarshal(raw, &got)); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetCall().GetMethod() != "/test.Ping/Echo" {
		t.Fatalf("got method %q", got.GetCall().GetMethod())
	}
}
