package relay_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/relay"
	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
	peerrpcpb "github.com/peerrpc/go/gen/proto/peerrpc"
	"google.golang.org/protobuf/proto"
)

// TestRelay_TwoPeersForward demonstrates the relay's core promise:
// two peers exchange frames through the relay node, which forwards
// bytes verbatim in both directions.
//
// Topology:
//
//	alice ── room "R" ── relay ── room "R" ── bob
//
// alice and bob both negotiate DataChannels to the relay (the relay
// accepts two). The relay pumps bytes between them. alice sends a
// frame; bob receives the SAME bytes.
func TestRelay_TwoPeersForward(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	backend := signal.NewLocal()
	srv, err := relay.New(relay.Config{
		Signaling: backend,
	})
	if err != nil {
		t.Fatalf("relay.New: %v", err)
	}
	defer srv.Close()

	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.Serve(ctx, "R", "relay") }()

	aliceCh := dialRelayPeer(t, ctx, backend, "R", "alice")
	bobCh := dialRelayPeer(t, ctx, backend, "R", "bob")

	// alice -> bob
	out := &peerrpcpb.Frame{
		Routing: &peerrpcpb.Routing{Sequence: 7},
		Type: &peerrpcpb.Frame_Call{
			Call: &peerrpcpb.Call{
				Method:          "/test.Foo/Bar",
				ProtocolVersion: 1,
				InlineData:      []byte("hello-via-relay"),
			},
		},
	}
	if err := aliceCh.SendFrame(ctx, out); err != nil {
		t.Fatalf("alice SendFrame: %v", err)
	}

	raw, err := bobCh.Recv(ctx)
	if err != nil {
		t.Fatalf("bob Recv: %v", err)
	}
	var got peerrpcpb.Frame
	if err := proto.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.GetCall().GetMethod() != "/test.Foo/Bar" {
		t.Fatalf("method: %q", got.GetCall().GetMethod())
	}
	if !bytes.Equal(got.GetCall().GetInlineData(), []byte("hello-via-relay")) {
		t.Fatalf("inline: %q", got.GetCall().GetInlineData())
	}

	// bob -> alice (reverse direction)
	resp := &peerrpcpb.ResponseFrame{
		Routing: &peerrpcpb.Routing{Sequence: 7},
		Type: &peerrpcpb.ResponseFrame_End{
			End: &peerrpcpb.End{},
		},
	}
	if err := bobCh.SendFrame(ctx, resp); err != nil {
		t.Fatalf("bob SendFrame: %v", err)
	}
	raw2, err := aliceCh.Recv(ctx)
	if err != nil {
		t.Fatalf("alice Recv: %v", err)
	}
	var got2 peerrpcpb.ResponseFrame
	if err := proto.Unmarshal(raw2, &got2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got2.GetEnd() == nil {
		t.Fatal("expected End frame")
	}

	cancel()
	if err := <-serverErr; err != nil && !isCanceled(err) {
		t.Logf("server returned: %v", err)
	}
}

// dialRelayPeer is the test's stand-in for the future relay.Dial
// helper: it joins the room and offers a DataChannel to the relay.
func dialRelayPeer(t *testing.T, ctx context.Context, backend signal.Backend, roomID, peerID string) *transport.Channel {
	t.Helper()
	sig, err := backend.Exchange(ctx, roomID, peerID)
	if err != nil {
		t.Fatalf("%s signaling: %v", peerID, err)
	}
	p, err := peer.New(ctx, signal.RoleOfferer, peer.Config{})
	if err != nil {
		t.Fatalf("%s peer.New: %v", peerID, err)
	}
	ch, err := p.Dial(ctx, sig)
	if err != nil {
		t.Fatalf("%s Dial: %v", peerID, err)
	}
	return ch
}

func isCanceled(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled || err == io.EOF || err == io.ErrClosedPipe {
		return true
	}
	return err.Error() == context.Canceled.Error()
}
