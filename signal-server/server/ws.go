// WebSocket signaling endpoint.
//
// The signaling service is served exclusively over WebSocket. Each
// frame is a 4-byte big-endian length prefix + a marshaled
// peerrpc.signaling.SignalMessage, matching the WebSocketSignal
// client (and the Go signal.WS backend).
//
// Wire protocol:
//   1. Client opens wss://host/ws (or ws:// for cleartext).
//   2. Client's first frame MUST be a SignalMessage with body=Announce
//      (carrying service + peer_id + role).
//   3. Server joins the peer into the store and broadcasts subsequent
//      frames to the other peers in the same service.
//   4. Server pushes frames received from other peers back over the
//      WebSocket until either side closes.
//
// Auth (when a Validator is configured) is enforced BEFORE the
// upgrade: the token is read from the Authorization header or the
// "token" query parameter; an invalid token yields a 401 and no
// WebSocket is opened.
package server

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	signalingpb "github.com/peerrpc/go/gen/proto/peerrpc/signaling"
	"github.com/peerrpc/signal-server/auth"
	"github.com/peerrpc/signal-server/store"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

var upgrader = websocket.Upgrader{
	// Browsers connect from a different origin (the Vite dev server),
	// so allow all origins for the signaling endpoint. Auth (when
	// configured) is enforced by the Validator before the upgrade.
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// Config carries the handler tuning knobs.
type Config struct {
	// Logger receives structured events (peer announce/leave, errors).
	// Defaults to slog.Default().
	Logger *slog.Logger

	// Validator, when non-nil, gates the WebSocket handshake on a
	// valid bearer token (Authorization header or ?token= query).
	Validator auth.TokenValidator
}

// WebSocketHandler returns an http.HandlerFunc that serves the
// signaling protocol over WebSocket on top of the given store.
func WebSocketHandler(s store.Store, cfg Config) http.HandlerFunc {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if _, ok := auth.AuthorizeRequest(w, r, cfg.Validator); !ok {
			// AuthorizeRequest already wrote a 401 response.
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			logger.Warn("ws upgrade", "err", err)
			return
		}
		defer conn.Close()
		serveWebSocket(r.Context(), conn, s, logger)
	}
}

func serveWebSocket(ctx context.Context, conn *websocket.Conn, s store.Store, logger *slog.Logger) {
	// 1. First frame: Announce.
	announceMsg, err := readFrame(conn)
	if err != nil {
		logger.Warn("ws read announce", "err", err)
		return
	}
	announce := announceMsg.GetAnnounce()
	if announce == nil {
		logger.Warn("ws first frame not announce")
		return
	}
	service := announceMsg.GetService()
	peerID := announce.GetPeerId()
	if service == "" || peerID == "" {
		logger.Warn("ws announce missing service/peer_id")
		return
	}

	peer := store.Peer{ID: peerID, Service: service}
	sx, rx, err := s.Join(ctx, peer)
	if err != nil {
		logger.Warn("ws join", "err", err, "service", service, "peer_id", peerID)
		return
	}
	defer func() { _ = s.Leave(context.Background(), peer) }()

	logger.Info("peer announced (ws)",
		"service", service,
		"peer_id", peerID,
		"role", announce.GetRole(),
	)

	// 2. Fan-in: pump store broadcasts to the WebSocket.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for inbound := range rx.Recv() {
			wire, ok := inbound.Body.(*signalingpb.SignalMessage)
			if !ok {
				continue
			}
			if err := writeFrame(conn, wire); err != nil {
				return
			}
		}
	}()

	// 3. Fan-out: read WebSocket frames and broadcast into the store.
	for {
		msg, err := readFrame(conn)
		if err != nil {
			// Normal close or transport error: stop pumping.
			_ = sx.Close()
			<-done
			return
		}
		msg.Service = service
		_ = sx.Send(ctx, store.SignalMessage{
			Service: service,
			SenderID: peerID,
			Body:    msg,
		})
	}
}

// readFrame reads one 4-byte-length-prefixed SignalMessage from the
// WebSocket (BinaryMessage only).
func readFrame(conn *websocket.Conn) (*signalingpb.SignalMessage, error) {
	_, data, err := conn.ReadMessage()
	if err != nil {
		if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			return nil, io.EOF
		}
		return nil, err
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("ws frame too short: %d bytes", len(data))
	}
	length := binary.BigEndian.Uint32(data[:4])
	if uint32(len(data)-4) < length {
		return nil, fmt.Errorf("ws frame truncated: want %d payload bytes, got %d", length, len(data)-4)
	}
	var msg signalingpb.SignalMessage
	if err := proto.Unmarshal(data[4:4+length], &msg); err != nil {
		return nil, fmt.Errorf("ws unmarshal: %w", err)
	}
	return &msg, nil
}

// writeFrame writes one 4-byte-length-prefixed SignalMessage to the
// WebSocket as a BinaryMessage.
func writeFrame(conn *websocket.Conn, msg *signalingpb.SignalMessage) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("ws marshal: %w", err)
	}
	frame := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
	copy(frame[4:], payload)
	return conn.WriteMessage(websocket.BinaryMessage, frame)
}
