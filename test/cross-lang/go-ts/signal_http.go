// SSE + POST signaling endpoints for browser ↔ Go signaling.
//
// The browser subscribes to GET /api/signal/events (SSE) and POSTs
// messages to POST /api/signal/send. The Go server fans messages
// between the two sides via the in-process signal.Local backend.
//
// This avoids the HTTP/2 requirement of Connect bidi streaming (and
// therefore the TLS/self-signed-cert problem in headless Chrome).

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"github.com/peerrpc/go/signal"
)

// signalHub fans messages between the SSE subscriber (browser) and
// the Go offerer's signal.Session. One signalHub per server.
//
// Architecture:
//
//	Go offerer ─── signal.Local ─── browser virtual session
//	    │                                   │
//	    └── hub.pumpFromBackend ──► SSE ──► Browser
//	    ▲                                   │
//	    └── signal.Local ◄── hub.handleSend ◄┘
type signalHub struct {
	backend signal.Backend
	logger  *slog.Logger

	mu          sync.Mutex
	subscribers map[string]chan jsonSignalMsg
	browserSess map[string]*signal.Session
	lastOffer   map[string]jsonSignalMsg
	browserReady map[string]chan struct{} // signaled when browser joins
}

type jsonSignalMsg struct {
	Type       string `json:"type"`
	Sdp        string `json:"sdp,omitempty"`
	Candidate  string `json:"candidate,omitempty"`
	SdpMid     string `json:"sdpMid,omitempty"`
	SdpMLineIx int    `json:"sdpMLineIndex,omitempty"`
}

func newSignalHub(backend signal.Backend, logger *slog.Logger) *signalHub {
	return &signalHub{
		backend:      backend,
		logger:       logger,
		subscribers:  map[string]chan jsonSignalMsg{},
		browserSess:  map[string]*signal.Session{},
		lastOffer:    map[string]jsonSignalMsg{},
		browserReady: map[string]chan struct{}{},
	}
}

// waitForBrowser blocks until a browser subscribes to roomID's SSE.
// Called by the Go offerer before sending its SDP offer.
func (h *signalHub) waitForBrowser(roomID string) <-chan struct{} {
	h.mu.Lock()
	ch, ok := h.browserReady[roomID]
	if !ok {
		ch = make(chan struct{})
		h.browserReady[roomID] = ch
	}
	h.mu.Unlock()
	return ch
}

// signalBrowserReady closes the browser-ready channel for roomID.
func (h *signalHub) signalBrowserReady(roomID string) {
	h.mu.Lock()
	if ch, ok := h.browserReady[roomID]; ok {
		select {
		case <-ch:
		default:
			close(ch)
		}
	}
	h.mu.Unlock()
}

// resetBrowserReady replaces the browser-ready channel for roomID
// with a fresh unclosed one, so the offerer can wait for the NEXT
// browser connection.
func (h *signalHub) resetBrowserReady(roomID string) {
	h.mu.Lock()
	h.browserReady[roomID] = make(chan struct{})
	// Also clear the old browser session so a new one can join.
	delete(h.browserSess, roomID)
	h.mu.Unlock()
}

// subscribe returns a channel that receives every signaling message
// for the given room. The SSE handler drains this channel.
func (h *signalHub) subscribe(roomID string) chan jsonSignalMsg {
	ch := make(chan jsonSignalMsg, 16)
	h.mu.Lock()
	h.subscribers[roomID] = ch
	h.mu.Unlock()
	return ch
}

func (h *signalHub) unsubscribe(roomID string) {
	h.mu.Lock()
	if ch, ok := h.subscribers[roomID]; ok {
		delete(h.subscribers, roomID)
		close(ch)
	}
	h.mu.Unlock()
}

// publish delivers msg to the room's subscriber (the browser).
func (h *signalHub) publish(roomID string, msg jsonSignalMsg) {
	h.mu.Lock()
	ch, ok := h.subscribers[roomID]
	h.mu.Unlock()
	if !ok {
		return
	}
	select {
	case ch <- msg:
	default:
	}
}

// pumpFromBackend drains a signal.Session's inbound messages and
// publishes them to the SSE subscriber. Called in a goroutine by the
// offerer after joining the signal room. Offers are buffered so late
// SSE subscribers (browser joins after the offer was sent) can replay
// them.
func (h *signalHub) pumpFromBackend(ctx context.Context, sig *signal.Session) {
	for {
		select {
		case msg, ok := <-sig.Receive():
			if !ok {
				return
			}
			translated := translateBackendToJSON(msg)
			if translated.Type == "" {
				continue
			}
			// Buffer offers for late SSE subscribers.
			if translated.Type == "offer" {
				h.mu.Lock()
				h.lastOffer[msg.Service] = translated
				h.mu.Unlock()
			}
			h.publish(msg.Service, translated)
		case <-ctx.Done():
			return
		}
	}
}

func translateBackendToJSON(msg *signal.SignalMessage) jsonSignalMsg {
	switch {
	case msg.Body.Offer != nil:
		return jsonSignalMsg{Type: "offer", Sdp: msg.Body.Offer.Sdp}
	case msg.Body.Answer != nil:
		return jsonSignalMsg{Type: "answer", Sdp: msg.Body.Answer.Sdp}
	case msg.Body.Candidate != nil:
		return jsonSignalMsg{
			Type:       "candidate",
			Candidate:  msg.Body.Candidate.Candidate,
			SdpMid:     msg.Body.Candidate.SdpMid,
			SdpMLineIx: int(msg.Body.Candidate.SdpMLineIndex),
		}
	default:
		return jsonSignalMsg{}
	}
}

func translateJSONToBackend(msg jsonSignalMsg, roomID string) *signal.SignalMessage {
	sm := &signal.SignalMessage{Service: roomID}
	switch msg.Type {
	case "offer":
		sm.Body = signal.SignalBody{Offer: &signal.SdpOffer{Sdp: msg.Sdp}}
	case "answer":
		sm.Body = signal.SignalBody{Answer: &signal.SdpAnswer{Sdp: msg.Sdp}}
	case "candidate":
		sm.Body = signal.SignalBody{Candidate: &signal.IceCandidate{
			Candidate:     msg.Candidate,
			SdpMid:        msg.SdpMid,
			SdpMLineIndex: uint32(msg.SdpMLineIx),
		}}
	}
	return sm
}

// handleSSE is the GET /api/signal/events endpoint. The browser
// subscribes here via EventSource. On subscribe, a "browser" virtual
// peer is created in the signal backend so that the Go offerer's
// messages (offer, ICE candidates) reach this SSE stream.
func (h *signalHub) handleSSE(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		http.Error(w, "missing room", 400)
		return
	}

	// Create or reuse the browser virtual peer in the signal backend.
	// This is how the Go offerer's messages reach the browser: the
	// offerer broadcasts into room X via signal.Local, the browser
	// virtual peer receives them, and we pump them into the SSE.
	h.mu.Lock()
	browserSess, ok := h.browserSess[roomID]
	h.mu.Unlock()
	if !ok {
		// Use a background context so the session survives after the
		// SSE HTTP connection closes. The browser POSTs its answer
		// later via handleSend, which reuses this session.
		sess, err := h.backend.Exchange(context.Background(), roomID, "browser")
		if err != nil {
			http.Error(w, fmt.Sprintf("signaling join: %v", err), 500)
			return
		}
		h.mu.Lock()
		h.browserSess[roomID] = sess
		h.mu.Unlock()
		browserSess = sess
		// Signal the Go offerer that the browser is ready.
		h.signalBrowserReady(roomID)
	}

	// Also register as SSE subscriber so POST messages echo back.
	sseCh := make(chan jsonSignalMsg, 16)
	h.mu.Lock()
	h.subscribers[roomID] = sseCh
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.subscribers, roomID)
		h.mu.Unlock()
	}()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", 500)
		return
	}

	// Signal that the subscriber is ready.
	fmt.Fprint(w, "event: ready\ndata: {}\n\n")
	flusher.Flush()

	// Replay buffered offer so late subscribers (browser joined after
	// the Go offerer sent its offer) see it immediately.
	h.mu.Lock()
	if offer, ok := h.lastOffer[roomID]; ok && offer.Type == "offer" {
		data, _ := json.Marshal(offer)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
	h.mu.Unlock()

	ctx := r.Context()
	for {
		select {
		// Messages from the signal backend (Go offerer → browser).
		case msg, ok := <-browserSess.Receive():
			if !ok {
				return
			}
			translated := translateBackendToJSON(msg)
			if translated.Type == "" {
				continue
			}
			h.logger.Info("SSE delivering to browser", "type", translated.Type, "room", roomID)
			data, _ := json.Marshal(translated)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		// Messages from POST echo (browser → browser self-echo).
		case msg := <-sseCh:
			data, _ := json.Marshal(msg)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-ctx.Done():
			return
		}
	}
}

// handleSend is the POST /api/signal/send endpoint. The browser
// POSTs a JSON signaling message here. The hub creates (or reuses)
// a "browser" virtual peer in the signal backend so the message
// reaches the Go offerer via normal signal.Local broadcast.
func (h *signalHub) handleSend(w http.ResponseWriter, r *http.Request) {
	var msg jsonSignalMsg
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "bad json", 400)
		return
	}
	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		http.Error(w, "missing room", 400)
		return
	}

	h.logger.Info("handleSend received", "type", msg.Type, "room", roomID)

	// Also publish to SSE for any other listeners (e.g. multiple
	// browser tabs).
	h.publish(roomID, msg)

	// Forward into the signal backend so the Go offerer picks it up.
	// Create or reuse a "browser" peer session. The session may have
	// been killed when the SSE connection dropped; recreate if needed.
	h.mu.Lock()
	sess, ok := h.browserSess[roomID]
	h.mu.Unlock()
	if !ok {
		ctx2, cancel := context.WithCancel(context.Background())
		defer cancel()
		newSess, err := h.backend.Exchange(ctx2, roomID, "browser-post")
		if err != nil {
			w.WriteHeader(http.StatusOK)
			return
		}
		h.mu.Lock()
		h.browserSess[roomID] = newSess
		h.mu.Unlock()
		sess = newSess
	}

	// Send the message into the backend (broadcasts to the Go offerer).
	sm := translateJSONToBackend(msg, roomID)
	_ = sess.Send(context.Background(), sm)

	w.WriteHeader(http.StatusOK)
}
