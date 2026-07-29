// Package signal remote backend: connect-go client that speaks the
// signaling wire format (service / AnnounceRequest) to a remote
// signal-server.

package signal

import (
	"context"
	"errors"
	"fmt"
	"sync"

	signalingpb "github.com/peerrpc/go/gen/proto/peerrpc/signaling"
	signalingpbconnect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/signalingpbconnect"

	"connectrpc.com/connect"
)

// Remote is a Backend that connects to a remote signal-server via
// the Connect protocol (gRPC/gRPC-Web/Connect). The constructor
// takes a base URL and optional connect.ClientOption values.
//
// Usage:
//
//	backend := signal.NewRemote("http://localhost:8080")
//	sig, err := backend.Exchange(ctx, "service-id", "peer-id")
//
// Remote is safe for concurrent use by any number of peers and
// services.
type Remote struct {
	baseURL string
	opts    []connect.ClientOption
}

// NewRemote constructs a Remote backend pointing at the given base URL.
// extraOpts are forwarded to connect.NewSignalingServiceClient.
func NewRemote(baseURL string, extraOpts ...connect.ClientOption) *Remote {
	return &Remote{baseURL: baseURL, opts: extraOpts}
}

// Exchange implements Backend by opening a Connect bidi stream to
// the signal-server. The first message sent on the stream is an
// AnnounceRequest carrying the peer id and role. Subsequent
// outbound messages are forwarded verbatim; inbound messages from
// the server are delivered via the returned Session's Receive
// channel.
//
// The caller MUST close the Session when done (or cancel ctx) to
// release the stream.
func (r *Remote) Exchange(ctx context.Context, service, peerID string) (*Session, error) {
	if service == "" {
		return nil, errors.New("signal: empty service")
	}
	if peerID == "" {
		return nil, errors.New("signal: empty peer id")
	}

	client := signalingpbconnect.NewSignalingServiceClient(
		// Default HTTP client; callers can override via connect options.
		nil,
		r.baseURL,
		r.opts...,
	)

	stream := client.Exchange(ctx)

	// First message MUST be AnnounceRequest.
	if err := stream.Send(&signalingpb.SignalMessage{
		Service: service,
		Body: &signalingpb.SignalMessage_Announce{
			Announce: &signalingpb.AnnounceRequest{
				PeerId: peerID,
				Role:   signalingpb.AnnounceRequest_ROLE_CLIENT,
			},
		},
	}); err != nil {
		return nil, fmt.Errorf("signal: send announce: %w", err)
	}

	in := make(chan *SignalMessage, 32)
	out := make(chan *SignalMessage, 32)

	ctx, cancel := context.WithCancel(ctx)

	// Pump: outbound channel -> stream.Send.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-out:
				if !ok {
					_ = stream.CloseRequest()
					return
				}
				pb, err := toProtoMessage(msg)
				if err != nil {
					continue
				}
				pb.Service = service
				if err := stream.Send(pb); err != nil {
					return
				}
			case <-ctx.Done():
				_ = stream.CloseRequest()
				return
			}
		}
	}()

	// Pump: stream.Receive -> inbound channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			pb, err := stream.Receive()
			if err != nil {
				close(in)
				return
			}
			sm := fromProtoMessage(pb)
			if sm == nil {
				continue
			}
			select {
			case in <- sm:
			default:
				// Inbox full; drop (best-effort signaling).
			}
		}
	}()

	s := &Session{
		service: service,
		peerID:  peerID,
		out:     out,
		in:      in,
		done:    make(chan struct{}),
		cleanup: func() {
			cancel()
			_ = stream.CloseRequest()
		},
	}
	return s, nil
}

// toProtoMessage converts a signal.SignalMessage to the protobuf
// wire type.
func toProtoMessage(msg *SignalMessage) (*signalingpb.SignalMessage, error) {
	pb := &signalingpb.SignalMessage{}
	switch {
	case msg.Body.Announce != nil:
		a := msg.Body.Announce
		req := &signalingpb.AnnounceRequest{
			PeerId: a.PeerID,
			Role:   signalingpb.AnnounceRequest_Role(a.Role),
		}
		if a.PeerPubkey != nil {
			req.PeerPubkey = a.PeerPubkey
		}
		pb.Body = &signalingpb.SignalMessage_Announce{Announce: req}
	case msg.Body.Offer != nil:
		pb.Body = &signalingpb.SignalMessage_Offer{
			Offer: &signalingpb.SdpOffer{Sdp: msg.Body.Offer.Sdp},
		}
	case msg.Body.Answer != nil:
		pb.Body = &signalingpb.SignalMessage_Answer{
			Answer: &signalingpb.SdpAnswer{Sdp: msg.Body.Answer.Sdp},
		}
	case msg.Body.Candidate != nil:
		pb.Body = &signalingpb.SignalMessage_Candidate{
			Candidate: &signalingpb.IceCandidate{
				Candidate:     msg.Body.Candidate.Candidate,
				SdpMid:        msg.Body.Candidate.SdpMid,
				SdpMlineIndex: msg.Body.Candidate.SdpMLineIndex,
			},
		}
	case msg.Body.Leave != nil:
		pb.Body = &signalingpb.SignalMessage_Leave{
			Leave: &signalingpb.LeaveRequest{Reason: msg.Body.Leave.Reason},
		}
	case msg.Body.Ping != nil:
		pb.Body = &signalingpb.SignalMessage_Ping{
			Ping: &signalingpb.Ping{TimestampMs: msg.Body.Ping.TimestampMs},
		}
	default:
		return nil, errors.New("signal: unknown body type")
	}
	return pb, nil
}

// fromProtoMessage converts a protobuf SignalMessage to the
// signal.SignalMessage type. Returns nil for unrecognised bodies.
func fromProtoMessage(pb *signalingpb.SignalMessage) *SignalMessage {
	sm := &SignalMessage{Service: pb.GetService()}
	switch body := pb.GetBody().(type) {
	case *signalingpb.SignalMessage_Announce:
		sm.Body.Announce = &AnnounceRequest{
			PeerID: body.Announce.GetPeerId(),
			Role:   Role(body.Announce.GetRole()),
		}
		if body.Announce.GetPeerPubkey() != nil {
			sm.Body.Announce.PeerPubkey = body.Announce.GetPeerPubkey()
		}
	case *signalingpb.SignalMessage_Offer:
		sm.Body.Offer = &SdpOffer{Sdp: body.Offer.GetSdp()}
	case *signalingpb.SignalMessage_Answer:
		sm.Body.Answer = &SdpAnswer{Sdp: body.Answer.GetSdp()}
	case *signalingpb.SignalMessage_Candidate:
		sm.Body.Candidate = &IceCandidate{
			Candidate:     body.Candidate.GetCandidate(),
			SdpMid:        body.Candidate.GetSdpMid(),
			SdpMLineIndex: body.Candidate.GetSdpMlineIndex(),
		}
	case *signalingpb.SignalMessage_Leave:
		sm.Body.Leave = &LeaveRequest{Reason: body.Leave.GetReason()}
	case *signalingpb.SignalMessage_Ping:
		sm.Body.Ping = &Ping{TimestampMs: body.Ping.GetTimestampMs()}
	default:
		return nil
	}
	return sm
}

// Ensure Remote satisfies Backend at compile time.
var _ Backend = (*Remote)(nil)
