package peer_test

import (
	"context"
	"testing"
	"time"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
	peerrpcpb "github.com/peerrpc/go/gen/proto/peerrpc"
	"google.golang.org/protobuf/proto"
)

// TestPeer_HandshakeAndRoundTrip performs the full Phase-1 localhost
// dance: in-process signaling -> WebRTC handshake -> one Frame round
// trip. It is the integration test that proves the lower three layers
// compose.
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

	offerer, err := peer.New(ctx, signal.RoleOfferer, peer.Config{})
	if err != nil {
		t.Fatalf("offerer New: %v", err)
	}
	defer offerer.Close()
	answerer, err := peer.New(ctx, signal.RoleAnswerer, peer.Config{})
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
