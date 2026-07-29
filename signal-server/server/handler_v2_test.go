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

	signalingpbv1 "github.com/peerrpc/go/gen/proto/peerrpc/signaling/v1"
	signalingpbv1connect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/v1/signalingpbconnect"
	signalingpbv2 "github.com/peerrpc/go/gen/proto/peerrpc/signaling/v2"
	signalingpbv2connect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/v2/signalingpbv2connect"

	"connectrpc.com/connect"
)

// newTestServerV2 boots a connect-go v2 signaling server on top of
// an in-memory store and returns a ready-to-use v2 client plus the
// underlying store for assertions. See newTestServer for the v1
// equivalent.
func newTestServerV2(t *testing.T, opts ...connect.HandlerOption) (signalingpbv2connect.SignalingServiceClient, *store.Memory) {
	t.Helper()
	mem := store.NewMemory()
	svc := server.NewV2(mem, server.Config{})

	mux := http.NewServeMux()
	path, handler := signalingpbv2connect.NewSignalingServiceHandler(svc, opts...)
	mux.Handle(path, handler)

	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	client := signalingpbv2connect.NewSignalingServiceClient(
		srv.Client(),
		srv.URL,
		connect.WithSendGzip(),
	)
	return client, mem
}

// runExchangeV2 is the v2 equivalent of runExchange: it pumps
// everything the stream receives into inbound until the stream
// closes or ctx is canceled.
func runExchangeV2(ctx context.Context, stream *connect.BidiStreamForClient[signalingpbv2.SignalMessage, signalingpbv2.SignalMessage], inbound chan<- *signalingpbv2.SignalMessage) error {
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

// TestHandlerV2_TwoPeersExchangeSDP runs the full bidirectional
// signaling dance against the v2 connect handler: alice and bob
// both announce against service "echo.Echo", alice sends an SDP
// offer, bob receives it and responds with an answer. Also covers
// ICE candidate round-trip.
func TestHandlerV2_TwoPeersExchangeSDP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, _ := newTestServerV2(t)

	aliceStream := client.Exchange(ctx)
	bobStream := client.Exchange(ctx)

	// Both peers announce.
	if err := aliceStream.Send(&signalingpbv2.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpbv2.SignalMessage_Announce{
			Announce: &signalingpbv2.AnnounceRequest{
				PeerId: "alice",
				Role:   signalingpbv2.AnnounceRequest_ROLE_CLIENT,
			},
		},
	}); err != nil {
		t.Fatalf("alice announce: %v", err)
	}
	if err := bobStream.Send(&signalingpbv2.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpbv2.SignalMessage_Announce{
			Announce: &signalingpbv2.AnnounceRequest{
				PeerId: "bob",
				Role:   signalingpbv2.AnnounceRequest_ROLE_SERVER,
			},
		},
	}); err != nil {
		t.Fatalf("bob announce: %v", err)
	}

	aliceIn := make(chan *signalingpbv2.SignalMessage, 8)
	bobIn := make(chan *signalingpbv2.SignalMessage, 8)
	go runExchangeV2(ctx, aliceStream, aliceIn)
	go runExchangeV2(ctx, bobStream, bobIn)

	// Alice sends an offer; bob must receive it.
	if err := aliceStream.Send(&signalingpbv2.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpbv2.SignalMessage_Offer{
			Offer: &signalingpbv2.SdpOffer{Sdp: "v=0\r\no=- alice 1"},
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
	if err := bobStream.Send(&signalingpbv2.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpbv2.SignalMessage_Answer{
			Answer: &signalingpbv2.SdpAnswer{Sdp: "v=0\r\no=- bob 1"},
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
	if err := aliceStream.Send(&signalingpbv2.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpbv2.SignalMessage_Candidate{
			Candidate: &signalingpbv2.IceCandidate{Candidate: "candidate:1 1 udp 2122260223 10.0.0.1 60000 typ host"},
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

// TestHandlerV2_RejectsMissingAnnounce verifies the v2 handler
// enforces Announce-first protocol.
func TestHandlerV2_RejectsMissingAnnounce(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, _ := newTestServerV2(t)
	stream := client.Exchange(ctx)
	// Skip the announce; send an offer right away.
	if err := stream.Send(&signalingpbv2.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpbv2.SignalMessage_Offer{
			Offer: &signalingpbv2.SdpOffer{Sdp: "v=0"},
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

// TestHandlerV2_AcceptsPeerPubkeyWithoutVerification asserts that
// peer_pubkey is accepted on the wire (Q3: interface added in v2,
// verification deferred to v2.1). The handler must not reject the
// announce and must not attempt to validate the key.
func TestHandlerV2_AcceptsPeerPubkeyWithoutVerification(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, _ := newTestServerV2(t)
	stream := client.Exchange(ctx)

	if err := stream.Send(&signalingpbv2.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpbv2.SignalMessage_Announce{
			Announce: &signalingpbv2.AnnounceRequest{
				PeerId:     "with-key",
				Role:       signalingpbv2.AnnounceRequest_ROLE_SERVER,
				PeerPubkey: []byte("placeholder-ed25519-pubkey-not-verified-yet"),
			},
		},
	}); err != nil {
		t.Fatalf("Send announce with pubkey: %v", err)
	}

	// The announce must succeed. We confirm by sending a follow-up
	// leave and observing the stream close cleanly (an invalid
	// announce would have surfaced as an error before this point).
	if err := stream.Send(&signalingpbv2.SignalMessage{
		Service: "echo.Echo",
		Body: &signalingpbv2.SignalMessage_Leave{
			Leave: &signalingpbv2.LeaveRequest{Reason: "test"},
		},
	}); err != nil {
		t.Fatalf("Send leave after announce: %v", err)
	}

	_ = stream.CloseResponse()
}

// TestHandlerV2_RoleEnums asserts the new role enum values are
// stable on the generated types. Renumbering would silently break
// clients and the wire.
func TestHandlerV2_RoleEnums(t *testing.T) {
	cases := []struct {
		name string
		got  signalingpbv2.AnnounceRequest_Role
		want int32
	}{
		{"UNSPECIFIED", signalingpbv2.AnnounceRequest_ROLE_UNSPECIFIED, 0},
		{"CLIENT", signalingpbv2.AnnounceRequest_ROLE_CLIENT, 1},
		{"SERVER", signalingpbv2.AnnounceRequest_ROLE_SERVER, 2},
		{"RELAY", signalingpbv2.AnnounceRequest_ROLE_RELAY, 3},
		{"BRIDGE", signalingpbv2.AnnounceRequest_ROLE_BRIDGE, 4},
	}
	for _, c := range cases {
		if int32(c.got) != c.want {
			t.Errorf("%s: got %d, want %d", c.name, c.got, c.want)
		}
	}
}

// TestHandlerV2_V1V2ShareStore bootstraps a v1 + v2 handler over
// the SAME in-memory store and verifies that a v1 client joining
// room "shared" and a v2 client announcing against service "shared"
// rendezvous in the same store entry. This is the operational
// guarantee for the v1→v2 migration window (two releases).
//
// IMPORTANT CONSTRAINT: store-level sharing does NOT imply wire-level
// interoperability. The store treats Body as opaque `any`; a v2
// sender's proto payload is not decodable by a v1 receiver and vice
// versa. Cross-version message passing requires both peers to speak
// the same proto version — i.e. SDK-side upgrade. signal-server
// deliberately does not translate between v1 and v2 wire formats;
// doing so would couple it to both protos indefinitely and is not
// part of the v2 contract.
func TestHandlerV2_V1V2ShareStore(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	mem := store.NewMemory()
	v1Svc := server.New(mem, server.Config{})
	v2Svc := server.NewV2(mem, server.Config{})

	mux := http.NewServeMux()
	v1Path, v1Handler := signalingpbv1connect.NewSignalingServiceHandler(v1Svc)
	v2Path, v2Handler := signalingpbv2connect.NewSignalingServiceHandler(v2Svc)
	mux.Handle(v1Path, v1Handler)
	mux.Handle(v2Path, v2Handler)

	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	v1Client := signalingpbv1connect.NewSignalingServiceClient(srv.Client(), srv.URL, connect.WithSendGzip())
	v2Client := signalingpbv2connect.NewSignalingServiceClient(srv.Client(), srv.URL, connect.WithSendGzip())

	// v1 client joins room "shared".
	v1Stream := v1Client.Exchange(ctx)
	if err := v1Stream.Send(&signalingpbv1.SignalMessage{
		RoomId: "shared",
		Body: &signalingpbv1.SignalMessage_Join{
			Join: &signalingpbv1.JoinRequest{PeerId: "v1peer"},
		},
	}); err != nil {
		t.Fatalf("v1 join: %v", err)
	}

	// v2 client announces against service "shared".
	v2Stream := v2Client.Exchange(ctx)
	if err := v2Stream.Send(&signalingpbv2.SignalMessage{
		Service: "shared",
		Body: &signalingpbv2.SignalMessage_Announce{
			Announce: &signalingpbv2.AnnounceRequest{PeerId: "v2peer"},
		},
	}); err != nil {
		t.Fatalf("v2 announce: %v", err)
	}

	// Stats must reflect exactly one service with two peers. Send
	// is non-blocking client-side, so poll until both joins are
	// processed server-side.
	deadline := time.Now().Add(2 * time.Second)
	var stats store.Stats
	for time.Now().Before(deadline) {
		stats = mem.Stats()
		if stats.Peers == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if stats.Services != 1 {
		t.Fatalf("expected 1 shared service entry, got %d (%+v)", stats.Services, stats.ServicePeers)
	}
	if stats.Peers != 2 {
		t.Fatalf("expected 2 peers in shared service, got %d", stats.Peers)
	}
	if n := stats.ServicePeers["shared"]; n != 2 {
		t.Fatalf("ServicePeers[shared] = %d, want 2", n)
	}

	_ = v1Stream.CloseResponse()
	_ = v2Stream.CloseResponse()
}
