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

	signalingpb "github.com/peerrpc/go/gen/proto/peerrpc/signaling/v1"
	signalingpbconnect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/v1/signalingpbconnect"

	"connectrpc.com/connect"
)

// newTestServer boots a connect-go signaling server on top of an
// in-memory store and returns a ready-to-use client plus the
// underlying store for assertions.
//
// The server uses TLS so connect's gRPC and Connect protocols both
// work over real HTTP/2 (httptest's TLS-mode auto-configures h2).
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

	// Client uses the test server's TLS config so it trusts its
	// self-signed certificate.
	client := signalingpbconnect.NewSignalingServiceClient(
		srv.Client(),
		srv.URL,
		connect.WithSendGzip(),
	)
	return client, mem
}

// runExchange opens a bidi stream and pumps msg through it. It
// returns everything the stream receives until the pump goroutine
// closes (which happens when the caller closes the stream).
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

// TestExchange_TwoPeersExchangeSDP runs the full bidirectional
// signaling dance against the connect handler: alice and bob both
// join room "r1", alice sends an SDP offer, bob receives it and
// responds with an answer.
func TestExchange_TwoPeersExchangeSDP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, _ := newTestServer(t)

	aliceStream := client.Exchange(ctx)
	bobStream := client.Exchange(ctx)

	// Both peers join.
	if err := aliceStream.Send(&signalingpb.SignalMessage{
		RoomId: "r1",
		Body: &signalingpb.SignalMessage_Join{
			Join: &signalingpb.JoinRequest{PeerId: "alice", Role: signalingpb.JoinRequest_ROLE_OFFERER},
		},
	}); err != nil {
		t.Fatalf("alice join: %v", err)
	}
	if err := bobStream.Send(&signalingpb.SignalMessage{
		RoomId: "r1",
		Body: &signalingpb.SignalMessage_Join{
			Join: &signalingpb.JoinRequest{PeerId: "bob", Role: signalingpb.JoinRequest_ROLE_ANSWERER},
		},
	}); err != nil {
		t.Fatalf("bob join: %v", err)
	}

	aliceIn := make(chan *signalingpb.SignalMessage, 8)
	bobIn := make(chan *signalingpb.SignalMessage, 8)
	go runExchange(ctx, aliceStream, aliceIn)
	go runExchange(ctx, bobStream, bobIn)

	// Alice sends an offer; bob must receive it.
	offer := &signalingpb.SignalMessage{
		RoomId: "r1",
		Body: &signalingpb.SignalMessage_Offer{
			Offer: &signalingpb.SdpOffer{Sdp: "v=0\r\no=- alice 1"},
		},
	}
	if err := aliceStream.Send(offer); err != nil {
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

	// Alice should NOT see her own offer.
	select {
	case got := <-aliceIn:
		t.Fatalf("alice saw her own offer: %+v", got)
	case <-time.After(50 * time.Millisecond):
		// good
	}

	// Bob replies with an answer; alice must receive it.
	answer := &signalingpb.SignalMessage{
		RoomId: "r1",
		Body: &signalingpb.SignalMessage_Answer{
			Answer: &signalingpb.SdpAnswer{Sdp: "v=0\r\no=- bob 1"},
		},
	}
	if err := bobStream.Send(answer); err != nil {
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
		RoomId: "r1",
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

	if err := aliceStream.CloseResponse(); err != nil && !errors.Is(err, context.Canceled) {
		// CloseResponse closes the receive side; the server will see
		// EOF on the next Receive and tear down its half.
	}
	_ = bobStream.CloseResponse()
}

// TestExchange_RejectsMissingJoin verifies the handler enforces the
// Join-first protocol.
func TestExchange_RejectsMissingJoin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client, _ := newTestServer(t)
	stream := client.Exchange(ctx)
	// Skip the join; send an offer right away.
	if err := stream.Send(&signalingpb.SignalMessage{
		RoomId: "r1",
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

// TestExchange_RoomFull rejects the third joiner.
func TestExchange_RoomFull(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, _ := newTestServer(t)
	for i, peer := range []string{"a", "b"} {
		s := client.Exchange(ctx)
		if err := s.Send(&signalingpb.SignalMessage{
			RoomId: "r",
			Body:   &signalingpb.SignalMessage_Join{Join: &signalingpb.JoinRequest{PeerId: peer}},
		}); err != nil {
			t.Fatalf("join %d: %v", i, err)
		}
	}

	third := client.Exchange(ctx)
	if err := third.Send(&signalingpb.SignalMessage{
		RoomId: "r",
		Body:   &signalingpb.SignalMessage_Join{Join: &signalingpb.JoinRequest{PeerId: "c"}},
	}); err != nil {
		t.Fatalf("send: %v", err)
	}
	_, err := third.Receive()
	if err == nil {
		t.Fatal("expected ResourceExhausted, got nil")
	}
	connErr := new(connect.Error)
	if !errors.As(err, &connErr) || connErr.Code() != connect.CodeResourceExhausted {
		t.Fatalf("expected CodeResourceExhausted, got %v", err)
	}
}
