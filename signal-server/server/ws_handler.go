package server

import (
	"context"
	"net/http"

	"github.com/peerrpc/signal-server/store"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

type wsJoinMessage struct {
	Type   string `json:"type"`
	RoomID string `json:"roomId"`
	PeerID string `json:"peerId"`
	Role   int    `json:"role"`
}

type wsSignalMessage struct {
	Type      string `json:"type"`
	SDP       string `json:"sdp,omitempty"`
	Candidate string `json:"candidate,omitempty"`
	SDPMid    string `json:"sdpMid,omitempty"`
	SDPMline  int    `json:"sdpMLineIndex,omitempty"`
	To        string `json:"to,omitempty"`
}

func (h *Handler) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("ws upgrade failed", "err", err)
		return
	}
	defer conn.Close()

	var join wsJoinMessage
	if err := conn.ReadJSON(&join); err != nil {
		h.logger.Error("ws read join failed", "err", err)
		return
	}
	if join.Type != "join" || join.RoomID == "" || join.PeerID == "" {
		h.logger.Warn("ws invalid join", "msg", join)
		return
	}

	peer := store.Peer{ID: join.PeerID, RoomID: join.RoomID}
	sx, rx, err := h.store.Join(context.Background(), peer)
	if err != nil {
		h.logger.Error("ws store join failed", "err", err, "room_id", join.RoomID, "peer_id", join.PeerID)
		_ = conn.WriteJSON(map[string]string{"type": "error", "message": err.Error()})
		return
	}

	h.logger.Info("peer joined via ws",
		"room_id", join.RoomID,
		"peer_id", join.PeerID,
		"role", join.Role,
	)

	errChan := make(chan error, 1)

	go func() {
		for inbound := range rx.Recv() {
			ev, ok := inbound.Body.(*wsSignalMessage)
			if !ok {
				continue
			}
			out := map[string]any{
				"type": ev.Type,
				"from": inbound.SenderID,
			}
			if ev.SDP != "" {
				out["sdp"] = ev.SDP
			}
			if ev.Candidate != "" {
				out["candidate"] = ev.Candidate
			}
			if ev.SDPMid != "" {
				out["sdpMid"] = ev.SDPMid
			}
			if ev.SDPMline != 0 {
				out["sdpMLineIndex"] = ev.SDPMline
			}
			if ev.To != "" {
				out["to"] = ev.To
			}
			if err := conn.WriteJSON(out); err != nil {
				errChan <- err
				return
			}
		}
		errChan <- nil
	}()

	for {
		var msg wsSignalMessage
		if err := conn.ReadJSON(&msg); err != nil {
			break
		}

		if err := sx.Send(context.Background(), store.SignalMessage{
			RoomID:   join.RoomID,
			SenderID: join.PeerID,
			Body:     &msg,
		}); err != nil {
			h.logger.Warn("ws broadcast failed", "err", err)
			break
		}
	}

	_ = h.store.Leave(context.Background(), peer)
	<-errChan
}
