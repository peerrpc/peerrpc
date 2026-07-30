// Package signal WebSocket backend: a signaling client that speaks
// the signaling wire format (service / AnnounceRequest) to a remote
// signal-server over a raw WebSocket.
//
// The frame format matches signalserver/ws.go exactly: each
// WebSocket BinaryMessage is a 4-byte big-endian length prefix
// followed by a marshaled peerrpc.signaling.SignalMessage protobuf.
// The first frame the client sends MUST be an AnnounceRequest.

package signal

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	signalingpb "github.com/peerrpc/go/gen/proto/peerrpc/signaling"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

// WSOption configures a WS backend.
type WSOption func(*WS)

// WithToken supplies a bearer token forwarded to the signal-server.
// The token is sent as a ?token= query parameter on the WebSocket
// handshake URL (matching the server's WebSocketHandler) and as an
// Authorization header fallback.
func WithToken(token string) WSOption {
	return func(w *WS) { w.token = token }
}

// WithDialer overrides the default websocket.Dialer (e.g. to set a
// custom TLS config or timeouts).
func WithDialer(d *websocket.Dialer) WSOption {
	return func(w *WS) { w.dialer = d }
}

// WithHandshakeTimeout sets the WebSocket handshake timeout.
func WithHandshakeTimeout(d time.Duration) WSOption {
	return func(w *WS) { w.handshakeTimeout = d }
}

// WS is a Backend that connects to a remote signal-server over a
// WebSocket. The constructor takes a base URL (any of http(s):// or
// ws(s):// forms; a bare host is also accepted) and optional WSOption
// values.
//
// Usage:
//
//	backend := signal.NewWS("ws://localhost:8443")
//	sig, err := backend.Exchange(ctx, "service-id", "peer-id")
//
// WS is safe for concurrent use by any number of peers and services.
type WS struct {
	baseURL          string
	token            string
	dialer           *websocket.Dialer
	handshakeTimeout time.Duration
}

// NewWS constructs a WS backend pointing at the given signal-server.
// The URL is normalized: http(s):// is rewritten to ws(s):// and a
// "/ws" path is appended when absent.
func NewWS(baseURL string, opts ...WSOption) *WS {
	w := &WS{baseURL: baseURL}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// wsURL normalizes the configured base URL into a ws:// or wss://
// URL ending with the "/ws" path, appending the token query param
// when a token is set.
func (w *WS) wsURL() (string, error) {
	raw := strings.TrimSpace(w.baseURL)
	if raw == "" {
		return "", errors.New("signal: empty ws url")
	}

	// Rewrite the scheme: http(s) → ws(s). A bare host (no scheme)
	// defaults to ws://.
	var u *url.URL
	var err error
	switch {
	case strings.HasPrefix(raw, "http://"):
		u, err = url.Parse("ws://" + raw[len("http://"):])
	case strings.HasPrefix(raw, "https://"):
		u, err = url.Parse("wss://" + raw[len("https://"):])
	case strings.HasPrefix(raw, "ws://"), strings.HasPrefix(raw, "wss://"):
		u, err = url.Parse(raw)
	default:
		// Bare host[:port][/path]. Treat as cleartext ws://.
		u, err = url.Parse("ws://" + raw)
	}
	if err != nil {
		return "", fmt.Errorf("signal: parse ws url %q: %w", raw, err)
	}

	// Append the "/ws" path if none (or only "/") is present.
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ws"
	}

	// Carry the token as a query param (the server's WS auth reads it
	// from here first).
	if w.token != "" {
		q := u.Query()
		q.Set("token", w.token)
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}

// Exchange implements Backend by opening a WebSocket to the
// signal-server. The first frame sent is an AnnounceRequest carrying
// the peer id and role. Subsequent outbound messages are forwarded
// verbatim; inbound messages from the server are delivered via the
// returned Session's Receive channel.
//
// The caller MUST close the Session when done (or cancel ctx) to
// release the connection.
func (w *WS) Exchange(ctx context.Context, service, peerID string) (*Session, error) {
	if service == "" {
		return nil, errors.New("signal: empty service")
	}
	if peerID == "" {
		return nil, errors.New("signal: empty peer id")
	}

	wsURL, err := w.wsURL()
	if err != nil {
		return nil, err
	}

	dialer := w.dialer
	if dialer == nil {
		dialer = &websocket.Dialer{
			HandshakeTimeout: w.handshakeTimeout,
		}
		if w.handshakeTimeout == 0 {
			dialer.HandshakeTimeout = 10 * time.Second
		}
	}

	header := http.Header{}
	if w.token != "" {
		header.Set("Authorization", "Bearer "+w.token)
	}

	conn, _, err := dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return nil, fmt.Errorf("signal: ws dial %q: %w", wsURL, err)
	}

	// First message MUST be AnnounceRequest.
	if err := writeFrame(conn, &signalingpb.SignalMessage{
		Service: service,
		Body: &signalingpb.SignalMessage_Announce{
			Announce: &signalingpb.AnnounceRequest{
				PeerId: peerID,
				Role:   signalingpb.AnnounceRequest_ROLE_CLIENT,
			},
		},
	}); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("signal: send announce: %w", err)
	}

	in := make(chan *SignalMessage, 32)
	out := make(chan *SignalMessage, 32)

	ctx, cancel := context.WithCancel(ctx)

	// A dedicated write mutex serializes concurrent frame writes (the
	// gorilla websocket.Conn.WriteMessage is not safe for concurrent
	// callers).
	var writeMu sync.Mutex

	// Pump: outbound channel -> ws.WriteMessage.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case msg, ok := <-out:
				if !ok {
					// Half-close: signal end-of-stream to the server.
					writeMu.Lock()
					_ = conn.WriteMessage(websocket.CloseMessage,
						websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
					writeMu.Unlock()
					return
				}
				pb, err := toProtoMessage(msg)
				if err != nil {
					continue
				}
				pb.Service = service
				writeMu.Lock()
				werr := writeFrame(conn, pb)
				writeMu.Unlock()
				if werr != nil {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Pump: ws.ReadMessage -> inbound channel.
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer cancel()
		for {
			pb, err := readFrame(conn)
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
			_ = conn.Close()
		},
	}
	return s, nil
}

// readFrame reads one 4-byte-length-prefixed SignalMessage from the
// WebSocket (BinaryMessage only).
func readFrame(conn *websocket.Conn) (*signalingpb.SignalMessage, error) {
	_, data, err := conn.ReadMessage()
	if err != nil {
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

// Ensure WS satisfies Backend at compile time.
var _ Backend = (*WS)(nil)
