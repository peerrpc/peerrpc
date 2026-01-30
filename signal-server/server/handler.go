// Package server implements peerrpc.signaling.v1.SignalingService on
// top of a store.Store. The handler runs one bidi stream per peer;
// inbound messages are broadcast into the peer's room, inbound-from-
// other-peer messages are written back to the wire.
//
// Wire format is governed by proto/peerrpc/signaling/v1/signaling.proto
// and the generated SignalingServiceHandler in peerrpc-go. The handler
// is protocol-agnostic: the same handler instance serves Connect,
// gRPC, and gRPC-Web clients thanks to connect-go's unified handler.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	signalingpb "github.com/peerrpc/go/gen/proto/peerrpc/signaling/v1"
	signalingpbconnect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/v1/signalingpbconnect"
	"github.com/peerrpc/signal-server/store"

	"connectrpc.com/connect"
)

// Config carries the handler tuning knobs.
type Config struct {
	// Logger receives structured events (peer join/leave, errors).
	// Defaults to slog.Default().
	Logger *slog.Logger
}

// Handler implements signalingpbconnect.SignalingServiceHandler on
// top of a store.Store. A single Handler serves any number of
// concurrent peers and rooms; construct one per process.
type Handler struct {
	store  store.Store
	logger *slog.Logger
}

// New constructs a Handler. The caller is responsible for passing
// the same Store instance to the connect-go handler that wires this
// into the HTTP mux.
func New(s store.Store, cfg Config) *Handler {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{store: s, logger: logger}
}

// streamGuard serializes access to a connect.BidiStream, which is
// not safe for concurrent Send/Receive calls. The fan-in pump writes
// from one goroutine while the dispatch loop reads from another; the
// guard protects both sides.
//
// Each direction has its own mutex so a slow Receive does not block
// a Send and vice versa.
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

// Exchange implements SignalingService.Exchange.
//
// Protocol:
//   - The peer's first message MUST be a JoinRequest carrying its
//     peer_id. The room comes from the message's room_id field.
//   - After joining, every subsequent message is broadcast into the
//     room (offer / answer / ICE candidate / ping / leave).
//   - Messages from other peers in the same room are written back to
//     this stream.
//   - On stream close (peer disconnect, ctx cancel, transport error)
//     the peer is removed from the room.
func (h *Handler) Exchange(ctx context.Context, stream *connect.BidiStream[signalingpb.SignalMessage, signalingpb.SignalMessage]) error {
	guard := &streamGuard{}

	// First message: must be Join.
	joinMsg, err := guard.receive(stream)
	if err != nil {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("expected join as first message: %w", err))
	}
	join := joinMsg.GetJoin()
	if join == nil {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("first message must be a JoinRequest"))
	}
	roomID := joinMsg.GetRoomId()
	peerID := join.GetPeerId()
	if roomID == "" || peerID == "" {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("room_id and peer_id are required"))
	}

	peer := store.Peer{ID: peerID, RoomID: roomID}
	sx, rx, err := h.store.Join(ctx, peer)
	if err != nil {
		if errors.Is(err, store.ErrPeerAlreadyExists) {
			return connect.NewError(connect.CodeAlreadyExists, err)
		}
		if errors.Is(err, store.ErrRoomFull) {
			return connect.NewError(connect.CodeResourceExhausted, err)
		}
		return connect.NewError(connect.CodeInternal, err)
	}
	// Defer cleanup to drain the room state on every return path
	// (success, error, ctx cancel). Leave also closes the inbound
	// fan-in channel, which terminates the goroutine below.
	defer func() {
		_ = h.store.Leave(context.Background(), peer)
	}()

	h.logger.Info("peer joined",
		"room_id", roomID,
		"peer_id", peerID,
		"role", join.GetRole(),
	)

	// Fan-in: pump room broadcasts back to the client. Runs in its
	// own goroutine so Receive and Send can proceed concurrently.
	// Access to the stream is mediated by guard to satisfy
	// connect.BidiStream's no-concurrent-call contract.
	//
	// The goroutine exits when the store closes the receiver (peer
	// left, ctx canceled) OR when a send fails (transport broke).
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
			// Leave the room so the send pump exits; drain the pump.
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
		msg.RoomId = roomID

		sm, err := translateInbound(msg)
		if err != nil {
			h.logger.Warn("malformed message from peer",
				"peer_id", peerID,
				"room_id", roomID,
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

// translateInbound wraps a wire SignalMessage into the store's opaque
// envelope by carrying the marshaled proto bytes through. The store
// does not inspect Body.
func translateInbound(msg *signalingpb.SignalMessage) (store.SignalMessage, error) {
	if msg == nil || msg.GetBody() == nil {
		return store.SignalMessage{}, fmt.Errorf("empty body")
	}
	return store.SignalMessage{
		RoomID: msg.GetRoomId(),
		Body:   msg,
	}, nil
}

// translateOutbound reconstructs a wire SignalMessage from a store
// envelope. The handler always sends Body verbatim (it is already a
// wire-shaped proto), forcing RoomID to the recipient's room at
// send time is unnecessary because the store guarantees same-room
// delivery.
func translateOutbound(in store.SignalMessage) (*signalingpb.SignalMessage, error) {
	wire, ok := in.Body.(*signalingpb.SignalMessage)
	if !ok {
		return nil, fmt.Errorf("store body is not a SignalMessage")
	}
	return wire, nil
}

// Assert at compile time that Handler satisfies the generated service
// interface. The interface is
//
//	SignalingServiceHandler interface {
//	    Exchange(context.Context, *connect.BidiStream[SignalMessage, SignalMessage]) error
//	}
//
// so Handler (a pointer receiver) must satisfy it.
var _ signalingpbconnect.SignalingServiceHandler = (*Handler)(nil)
