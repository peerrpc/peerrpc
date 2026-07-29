package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	signalingpbv2 "github.com/peerrpc/go/gen/proto/peerrpc/signaling/v2"
	signalingpbv2connect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/v2/signalingpbv2connect"
	"github.com/peerrpc/signal-server/store"

	"connectrpc.com/connect"
)

// HandlerV2 implements peerrpc.signaling.v2.SignalingService on top of
// the same store.Store as the v1 Handler. A single HandlerV2 serves
// any number of concurrent peers and services; construct one per
// process.
//
// Wire format is governed by proto/peerrpc/signaling/v2/signaling.proto
// and the generated SignalingServiceHandler in peerrpc-go.
//
// v2 differences from v1 (Handler):
//   - room_id is replaced by service on the wire and in the store.
//   - JoinRequest is replaced by AnnounceRequest.
//   - ROLE_OFFERER / ROLE_ANSWERER are replaced by ROLE_CLIENT /
//     ROLE_SERVER (WebRTC offer direction is now an SDK concern).
//   - New ROLE_RELAY / ROLE_BRIDGE values let the server distinguish
//     relay and bridge peers.
//   - peer_pubkey is accepted but NOT yet verified; Ed25519
//     verification ships in v2.1.
type HandlerV2 struct {
	store  store.Store
	logger *slog.Logger
}

// NewV2 constructs a HandlerV2 bound to the given store. The caller
// is responsible for passing the resulting handler to
// signalingpbv2connect.NewSignalingServiceHandler and wiring it into
// the HTTP mux.
func NewV2(s store.Store, cfg Config) *HandlerV2 {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &HandlerV2{store: s, logger: logger}
}

// streamGuardV2 serializes access to a connect.BidiStream for v2.
// See streamGuard for the rationale: connect.BidiStream is not safe
// for concurrent Send / Receive.
type streamGuardV2 struct {
	sendMu sync.Mutex
	recvMu sync.Mutex
}

func (g *streamGuardV2) send(stream *connect.BidiStream[signalingpbv2.SignalMessage, signalingpbv2.SignalMessage], msg *signalingpbv2.SignalMessage) error {
	g.sendMu.Lock()
	defer g.sendMu.Unlock()
	return stream.Send(msg)
}

func (g *streamGuardV2) receive(stream *connect.BidiStream[signalingpbv2.SignalMessage, signalingpbv2.SignalMessage]) (*signalingpbv2.SignalMessage, error) {
	g.recvMu.Lock()
	defer g.recvMu.Unlock()
	return stream.Receive()
}

// Exchange implements SignalingService.Exchange (v2).
//
// Protocol:
//   - The peer's first message MUST be an AnnounceRequest carrying
//     its peer_id. The service comes from the message's service
//     field.
//   - After announcing, every subsequent message is broadcast into
//     the service (offer / answer / ICE candidate / ping / leave).
//   - Messages from other peers in the same service are written back
//     to this stream.
//   - On stream close (peer disconnect, ctx cancel, transport error)
//     the peer is removed from the service.
//
// peer_pubkey (if present) is accepted but not verified; v2.1 will
// introduce Ed25519 signature verification and propagate the derived
// PeerID into Identity for downstream authorization.
func (h *HandlerV2) Exchange(ctx context.Context, stream *connect.BidiStream[signalingpbv2.SignalMessage, signalingpbv2.SignalMessage]) error {
	guard := &streamGuardV2{}

	// First message: must be Announce.
	announceMsg, err := guard.receive(stream)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("expected announce as first message: %w", err))
	}
	announce := announceMsg.GetAnnounce()
	if announce == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("first message must be an AnnounceRequest"))
	}
	service := announceMsg.GetService()
	peerID := announce.GetPeerId()
	if service == "" || peerID == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("service and peer_id are required"))
	}

	peer := store.Peer{ID: peerID, Service: service}
	sx, rx, err := h.store.Join(ctx, peer)
	if err != nil {
		if errors.Is(err, store.ErrPeerAlreadyExists) {
			return connect.NewError(connect.CodeAlreadyExists, err)
		}
		if errors.Is(err, store.ErrServiceFull) {
			return connect.NewError(connect.CodeResourceExhausted, err)
		}
		return connect.NewError(connect.CodeInternal, err)
	}
	// Defer cleanup so every return path drains the service state.
	// Leave also closes the inbound fan-in channel, which terminates
	// the goroutine below.
	defer func() {
		_ = h.store.Leave(context.Background(), peer)
	}()

	h.logger.Info("peer announced (v2)",
		"service", service,
		"peer_id", peerID,
		"role", announce.GetRole(),
		"has_pubkey", announce.GetPeerPubkey() != nil,
	)

	// Fan-in: pump service broadcasts back to the client.
	sendErr := make(chan error, 1)
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for inbound := range rx.Recv() {
			out, err := translateOutboundV2(inbound)
			if err != nil {
				sendErr <- err
				return
			}
			if err := guard.send(stream, out); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- nil
	}()

	// Fan-out: read messages from the client and broadcast them.
	for {
		msg, err := guard.receive(stream)
		if err != nil {
			// EOF or transport error: client closed or ctx canceled.
			_ = h.store.Leave(context.Background(), peer)
			<-sendDone
			select {
			case e := <-sendErr:
				if e != nil {
					return e
				}
			default:
			}
			return nil
		}
		msg.Service = service

		sm, err := translateInboundV2(msg)
		if err != nil {
			h.logger.Warn("malformed message from peer",
				"peer_id", peerID,
				"service", service,
				"err", err,
			)
			continue
		}

		if err := sx.Send(ctx, sm); err != nil {
			<-sendDone
			return connect.NewError(connect.CodeInternal, err)
		}
	}
}

// translateInboundV2 wraps a wire v2 SignalMessage into the store's
// opaque envelope. The store does not inspect Body.
func translateInboundV2(msg *signalingpbv2.SignalMessage) (store.SignalMessage, error) {
	if msg == nil || msg.GetBody() == nil {
		return store.SignalMessage{}, fmt.Errorf("empty body")
	}
	return store.SignalMessage{
		Service: msg.GetService(),
		Body:    msg,
	}, nil
}

// translateOutboundV2 reconstructs a wire v2 SignalMessage from a
// store envelope. The handler sends Body verbatim (it is already a
// wire-shaped proto).
func translateOutboundV2(in store.SignalMessage) (*signalingpbv2.SignalMessage, error) {
	wire, ok := in.Body.(*signalingpbv2.SignalMessage)
	if !ok {
		return nil, fmt.Errorf("store body is not a v2 SignalMessage")
	}
	return wire, nil
}

// Assert at compile time that HandlerV2 satisfies the generated v2
// service interface.
var _ signalingpbv2connect.SignalingServiceHandler = (*HandlerV2)(nil)
