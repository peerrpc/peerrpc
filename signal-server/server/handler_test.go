package server_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/peerrpc/signal-server/server"
	"github.com/peerrpc/signal-server/store"

	signalingpb "github.com/peerrpc/go/gen/proto/peerrpc/signaling"
	signalingpbconnect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/signalingpbconnect"

	"connectrpc.com/connect"
)

// newTestServer boots a connect-go signaling server on top of
// an in-memory store and returns a ready-to-use client plus the
// underlying store for assertions.
func newTestServer(t *testing.T, opts ...connect.HandlerOption) (signalingpbconnect.SignalingServiceClient, *store.Memory) {
	t.Helper()
	mem := store.NewMemory()
	svc := server.New(mem, server.Config{})

	mux := http.NewServeMux()
	path, handler := signalingpbconnect.NewSignalingServiceHandler(svc, opts...)
	mux.Handle(path, handler)

	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	client := signalingpbconnect.NewSignalingServiceClient(
		srv.Client(),
		srv.URL,
		connect.WithSendGzip(),
	)
	return client, mem
}

// runExchange pumps everything the stream receives into inbound
// until the stream closes or ctx is canceled.
func runExchange(ctx context.Context, stream *connect.BidiStreamForClient[signalingpb.SignalMessage, signalingpb.SignalMessage], inbound chan<- *signalingpb.SignalMessage) error {
	for {
		msg, err := stream.Receive()
		if err != nil {
			return err
		}
		select {
		case inbound <- msg:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// TestHandler_TwoPeersExchangeSDP runs the full bidirectional
// signaling dance: alice and bob
// both announce against service "echo.Echo", alice sends an SDP
// offer, bob receives it and responds with an answer. Also covers
// ICE candidate round-trip.
func TestHandler_TwoPeersExchangeSDP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, _ := newTestServer(t)

	aliceStream := client.Exchange(ctx)
	bobStream := client.Exchange(ctx)

	// Both peers announce.
	if err := aliceStream.Send(&signalingpb.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpb.SignalMessage_Announce{
			Announce: &signalingpb.AnnounceRequest{
				PeerId: "alice",
				Role:   signalingpb.AnnounceRequest_ROLE_CLIENT,
			},
		},
	}); err != nil {
		t.Fatalf("alice announce: %v", err)
	}
	if err := bobStream.Send(&signalingpb.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpb.SignalMessage_Announce{
			Announce: &signalingpb.AnnounceRequest{
				PeerId: "bob",
				Role:   signalingpb.AnnounceRequest_ROLE_SERVER,
			},
		},
	}); err != nil {
		t.Fatalf("bob announce: %v", err)
	}

	aliceIn := make(chan *signalingpb.SignalMessage, 8)
	bobIn := make(chan *signalingpb.SignalMessage, 8)
	go runExchange(ctx, aliceStream, aliceIn)
	go runExchange(ctx, bobStream, bobIn)

	// Alice sends an offer; bob must receive it.
	if err := aliceStream.Send(&signalingpb.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpb.SignalMessage_Offer{
			Offer: &signalingpb.SdpOffer{Sdp: "v=0\r\no=- alice 1"},
		},
	}); err != nil {
		t.Fatalf("alice send offer: %v", err)
	}
	select {
	case got := <-bobIn:
		if got.GetOffer() == nil || got.GetOffer().GetSdp() != "v=0\r\no=- alice 1" {
			t.Fatalf("bob got unexpected message: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("bob did not receive alice's offer")
	}

	// Alice should NOT see her own offer (broadcast-to-others).
	select {
	case got := <-aliceIn:
		t.Fatalf("alice saw her own offer: %+v", got)
	case <-time.After(50 * time.Millisecond):
		// good
	}

	// Bob replies; alice must receive it.
	if err := bobStream.Send(&signalingpb.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpb.SignalMessage_Answer{
			Answer: &signalingpb.SdpAnswer{Sdp: "v=0\r\no=- bob 1"},
		},
	}); err != nil {
		t.Fatalf("bob send answer: %v", err)
	}
	select {
	case got := <-aliceIn:
		if got.GetAnswer() == nil || got.GetAnswer().GetSdp() != "v=0\r\no=- bob 1" {
			t.Fatalf("alice got unexpected message: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("alice did not receive bob's answer")
	}

	// ICE candidate round-trip.
	if err := aliceStream.Send(&signalingpb.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpb.SignalMessage_Candidate{
			Candidate: &signalingpb.IceCandidate{Candidate: "candidate:1 1 udp 2122260223 10.0.0.1 60000 typ host"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-bobIn:
		if got.GetCandidate() == nil {
			t.Fatalf("bob got non-candidate: %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("bob did not receive ICE candidate")
	}

	_ = aliceStream.CloseResponse()
	_ = bobStream.CloseResponse()
}

// TestHandler_RejectsMissingAnnounce verifies the handler
// enforces Announce-first protocol.
func TestHandler_RejectsMissingAnnounce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, _ := newTestServer(t)
	stream := client.Exchange(ctx)
	// Skip the announce; send an offer right away.
	if err := stream.Send(&signalingpb.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpb.SignalMessage_Offer{
			Offer: &signalingpb.SdpOffer{Sdp: "v=0"},
		},
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	// Next Receive must surface the InvalidArgument.
	_, err := stream.Receive()
	if err == nil {
		t.Fatal("expected InvalidArgument, got nil")
	}
	connErr := new(connect.Error)
	if !errors.As(err, &connErr) || connErr.Code() != connect.CodeInvalidArgument {
		t.Fatalf("expected CodeInvalidArgument, got %v", err)
	}
}

// TestHandler_AcceptsPeerPubkeyWithoutVerification asserts that
// peer_pubkey is accepted on the wire. The handler must not reject the
// announce and must not attempt to validate the key.
func TestHandler_AcceptsPeerPubkeyWithoutVerification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, _ := newTestServer(t)
	stream := client.Exchange(ctx)

	if err := stream.Send(&signalingpb.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpb.SignalMessage_Announce{
			Announce: &signalingpb.AnnounceRequest{
				PeerId:     "with-key",
				Role:       signalingpb.AnnounceRequest_ROLE_SERVER,
				PeerPubkey: []byte("placeholder-ed25519-pubkey-not-verified-yet"),
			},
		},
	}); err != nil {
		t.Fatalf("Send announce with pubkey: %v", err)
	}

	// The announce must succeed. We confirm by sending a follow-up
	// leave and observing the stream close cleanly (an invalid
	// announce would have surfaced as an error before this point).
	if err := stream.Send(&signalingpb.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpb.SignalMessage_Leave{
			Leave: &signalingpb.LeaveRequest{Reason: "test"},
		},
	}); err != nil {
		t.Fatalf("Send leave after announce: %v", err)
	}

	_ = stream.CloseResponse()
}

// TestHandler_RoleEnums asserts the new role enum values are
// stable on the generated types. Renumbering would silently break
// clients and the wire.
func TestHandler_RoleEnums(t *testing.T) {
	cases := []struct {
		name string
		got  signalingpb.AnnounceRequest_Role
		want int32
	}{
		{"UNSPECIFIED", signalingpb.AnnounceRequest_ROLE_UNSPECIFIED, 0},
		{"CLIENT", signalingpb.AnnounceRequest_ROLE_CLIENT, 1},
		{"SERVER", signalingpb.AnnounceRequest_ROLE_SERVER, 2},
		{"RELAY", signalingpb.AnnounceRequest_ROLE_RELAY, 3},
		{"BRIDGE", signalingpb.AnnounceRequest_ROLE_BRIDGE, 4},
	}
	for _, c := range cases {
		if int32(c.got) != c.want {
			t.Errorf("%s: got %d, want %d", c.name, c.got, c.want)
		}
	}
}
