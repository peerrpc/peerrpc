package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	signalingpb "github.com/peerrpc/go/gen/proto/peerrpc/signaling"
	signalingpbconnect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/signalingpbconnect"
	"github.com/peerrpc/signal-server/store"

	"connectrpc.com/connect"
)

// Handler implements peerrpc.signaling.SignalingService on top of
// a store.Store. A single Handler serves any number of concurrent
// peers and services; construct one per process.
//
// Wire format is governed by proto/peerrpc/signaling/signaling.proto
// and the generated SignalingServiceHandler in peerrpc-go.
type Handler struct {
	store  store.Store
	logger *slog.Logger
}

// Config carries the handler tuning knobs.
type Config struct {
	// Logger receives structured events (peer announce/leave, errors).
	// Defaults to slog.Default().
	Logger *slog.Logger
}

// New constructs a Handler bound to the given store. The caller
// is responsible for passing the resulting handler to
// signalingpbconnect.NewSignalingServiceHandler and wiring it into
// the HTTP mux.
func New(s store.Store, cfg Config) *Handler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{store: s, logger: logger}
}

// streamGuard serializes access to a connect.BidiStream. connect's
// BidiStream is not safe for concurrent Send / Receive, so the
// fan-in pump and the dispatch loop each take a direction-specific
// mutex.
type streamGuard struct {
	sendMu sync.Mutex
	recvMu sync.Mutex
}

func (g *streamGuard) send(stream *connect.BidiStream[signalingpb.SignalMessage, signalingpb.SignalMessage], msg *signalingpb.SignalMessage) error {
	g.sendMu.Lock()
	defer g.sendMu.Unlock()
	return stream.Send(msg)
}

func (g *streamGuard) receive(stream *connect.BidiStream[signalingpb.SignalMessage, signalingpb.SignalMessage]) (*signalingpb.SignalMessage, error) {
	g.recvMu.Lock()
	defer g.recvMu.Unlock()
	return stream.Receive()
}

// Exchange implements SignalingService.Exchange (signal).
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
// peer_pubkey (if present) is accepted but not verified; a future
// release will introduce Ed25519 signature verification and propagate
// the derived PeerID into Identity for downstream authorization.
func (h *Handler) Exchange(ctx context.Context, stream *connect.BidiStream[signalingpb.SignalMessage, signalingpb.SignalMessage]) error {
	guard := &streamGuard{}

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

	h.logger.Info("peer announced (signal)",
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
			out, err := translateOutbound(inbound)
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

		sm, err := translateInbound(msg)
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

// translateInbound wraps a wire SignalMessage into the store's
// opaque envelope. The store does not inspect Body.
func translateInbound(msg *signalingpb.SignalMessage) (store.SignalMessage, error) {
	if msg == nil || msg.GetBody() == nil {
		return store.SignalMessage{}, fmt.Errorf("empty body")
	}
	return store.SignalMessage{
		Service: msg.GetService(),
		Body:    msg,
	}, nil
}

// translateOutbound reconstructs a wire SignalMessage from a
// store envelope. The handler sends Body verbatim (it is already a
// wire-shaped proto).
func translateOutbound(in store.SignalMessage) (*signalingpb.SignalMessage, error) {
	wire, ok := in.Body.(*signalingpb.SignalMessage)
	if !ok {
		return nil, fmt.Errorf("store body is not a SignalMessage")
	}
	return wire, nil
}

// Assert at compile time that Handler satisfies the generated
// service interface.
var _ signalingpbconnect.SignalingServiceHandler = (*Handler)(nil)
