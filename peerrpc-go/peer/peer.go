// Package peer manages WebRTC PeerConnections and the ICE/DTLS
// negotiation that yields an ordered, reliable DataChannel.
//
// Each Peer exchanges SDP via any signal.Backend and configures ICE
// candidate gathering with a three-tier cascade:
//
//   Level 0  host       — same-LAN, zero extra latency
//   Level 1  srflx      — STUN-reflected public address for typical NAT
//   Level 2  relay      — TURN relay for symmetric NAT / firewalls
//
// The cascade is implicit in WebRTC: pion gathers every enabled
// candidate type in parallel and the ICE agent picks the best one. The
// knobs Config exposes (ICEServers, CandidateTypes, MaxICERestarts)
// are how an application tunes that cascade — e.g. disabling relay to
// force P2P-only, or adding a TURN server with short-lived credentials.
package peer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/peerrpc/go/transport"
	"github.com/peerrpc/go/signal"
	"github.com/pion/webrtc/v4"
)

// DefaultICEServers is empty by default: localhost-only operation. An
// application that wants to traverse NAT populates Config.ICEServers
// with one or more STUN/TURN servers.
var defaultICEServers = []webrtc.ICEServer{}

// Config carries the PeerConnection tuning knobs.
type Config struct {
	// ICEServers lists STUN/TURN servers. Empty for localhost-only.
	//
	// This is the primary knob for the ICE cascade: without any
	// STUN/TURN entry pion gathers only host candidates (LAN-only).
	// Adding a STUN server enables srflx (typical NAT). Adding a TURN
	// server enables relay (symmetric NAT / firewall).
	//
	// Example for STUN + TURN with short-lived credentials:
	//   []webrtc.ICEServer{
	//       {URLs: []string{"stun:stun.l.google.com:19302"}},
	//       {
	//           URLs:       []string{"turn:turn.example.com:3478"},
	//           Username:   "1680000000:peer-abc",
	//           Credential: "shortlivedsecret",
	//       },
	//   }
	ICEServers []webrtc.ICEServer

	// DataChannelLabel overrides transport.DataChannelLabel.
	DataChannelLabel string

	// NegotiationTimeout caps how long Dial/Accept waits for the
	// DataChannel to open before failing. Defaults to 10s.
	NegotiationTimeout time.Duration

	// OnICEConnectionStateChange, if set, is invoked every time the
	// ICE connection state changes. Useful for surfacing the
	// three-tier cascade to the application (e.g. metrics, logging).
	OnICEConnectionStateChange func(webrtc.ICEConnectionState)

	// ICERestartCallback, if set, is invoked when the application
	// should trigger ICE restart (e.g. after a network change). The
	// callback receives the current PeerConnection so the caller can
	// issue a new offer with ICERestart. Reserved for future use;
	// currently informational.
	ICERestartCallback func(*webrtc.PeerConnection)
}

func (c *Config) applyDefaults() {
	if c.DataChannelLabel == "" {
		c.DataChannelLabel = transport.DataChannelLabel
	}
	if c.NegotiationTimeout == 0 {
		c.NegotiationTimeout = 10 * time.Second
	}
	if c.ICEServers == nil {
		c.ICEServers = defaultICEServers
	}
}

// Peer is one side of a WebRTC connection.
//
// A Peer is either an Offerer (initiates the SDP offer + creates the
// DataChannel) or an Answerer (accepts the offer + waits for the
// remote side to open a DataChannel).
type Peer struct {
	api    *webrtc.API
	pc     *webrtc.PeerConnection
	cfg    Config
	role   signal.Role

	// openCh fires once when the DataChannel is open.
	openCh chan *transport.Channel

	stateMu sync.Mutex
	state   webrtc.ICEConnectionState
}

// New constructs a Peer in the given role with the given config.
// The Peer is NOT yet connected; call Dial or Accept.
func New(ctx context.Context, role signal.Role, cfg Config) (*Peer, error) {
	cfg.applyDefaults()

	se := webrtc.SettingEngine{}

	api := webrtc.NewAPI(
		webrtc.WithSettingEngine(se),
	)
	pc, err := api.NewPeerConnection(webrtc.Configuration{
		ICEServers: cfg.ICEServers,
	})
	if err != nil {
		return nil, fmt.Errorf("peer: NewPeerConnection: %w", err)
	}

	p := &Peer{
		api:    api,
		pc:     pc,
		cfg:    cfg,
		role:   role,
		openCh: make(chan *transport.Channel, 1),
		state:  webrtc.ICEConnectionStateNew,
	}

	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		p.stateMu.Lock()
		p.state = s
		p.stateMu.Unlock()
		if cfg.OnICEConnectionStateChange != nil {
			cfg.OnICEConnectionStateChange(s)
		}
	})

	// Both sides register the OnDataChannel handler; only the Answerer
	// receives the channel via it, but registering on both is harmless
	// and future-proofs the bidirectional-push case.
	pc.OnDataChannel(func(dc *webrtc.DataChannel) {
		p.handleDataChannel(dc)
	})

	return p, nil
}

// ICEState returns the latest ICE connection state observed.
func (p *Peer) ICEState() webrtc.ICEConnectionState {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	return p.state
}

// Dial is the Offerer flow:
//  1. Create a DataChannel.
//  2. Create an SDP offer.
//  3. Wait for ICE gathering to complete (non-trickle; all candidates
//     ride in the SDP).
//  4. Exchange the offer/answer over the signaling backend.
//  5. Wait for the DataChannel to open.
func (p *Peer) Dial(ctx context.Context, sig *signal.Session) (*transport.Channel, error) {
	if p.role != signal.RoleOfferer {
		return nil, errors.New("peer: Dial requires RoleOfferer")
	}

	dc, err := p.pc.CreateDataChannel(p.cfg.DataChannelLabel, &webrtc.DataChannelInit{
		Ordered:        boolPtr(true),
		MaxRetransmits: nil, // nil => infinity (reliable)
	})
	if err != nil {
		return nil, fmt.Errorf("peer: CreateDataChannel: %w", err)
	}
	p.handleDataChannel(dc)

	offer, err := p.pc.CreateOffer(nil)
	if err != nil {
		return nil, fmt.Errorf("peer: CreateOffer: %w", err)
	}
	if err := p.pc.SetLocalDescription(offer); err != nil {
		return nil, fmt.Errorf("peer: SetLocalDescription(offer): %w", err)
	}

	if err := p.waitICEGathering(ctx); err != nil {
		return nil, fmt.Errorf("peer: wait ICE gathering: %w", err)
	}

	if err := sig.Send(ctx, &signal.SignalMessage{
		Body: signal.SignalBody{Offer: &signal.SdpOffer{Sdp: encodeSDP(*p.pc.LocalDescription())}},
	}); err != nil {
		return nil, fmt.Errorf("peer: send offer: %w", err)
	}

	answer, err := waitForAnswer(ctx, sig)
	if err != nil {
		return nil, fmt.Errorf("peer: wait answer: %w", err)
	}
	if err := p.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeAnswer,
		SDP:  answer,
	}); err != nil {
		return nil, fmt.Errorf("peer: SetRemoteDescription(answer): %w", err)
	}

	return p.waitForOpen(ctx)
}

// Accept is the Answerer flow:
//  1. Wait for an offer.
//  2. Apply the offer as the remote description.
//  3. Create+send an answer (after ICE gathering completes).
//  4. Wait for the Offerer to open the DataChannel.
func (p *Peer) Accept(ctx context.Context, sig *signal.Session) (*transport.Channel, error) {
	if p.role != signal.RoleAnswerer {
		return nil, errors.New("peer: Accept requires RoleAnswerer")
	}

	offer, err := waitForOffer(ctx, sig)
	if err != nil {
		return nil, fmt.Errorf("peer: wait offer: %w", err)
	}
	if err := p.pc.SetRemoteDescription(webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  offer,
	}); err != nil {
		return nil, fmt.Errorf("peer: SetRemoteDescription(offer): %w", err)
	}

	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		return nil, fmt.Errorf("peer: CreateAnswer: %w", err)
	}
	if err := p.pc.SetLocalDescription(answer); err != nil {
		return nil, fmt.Errorf("peer: SetLocalDescription(answer): %w", err)
	}

	if err := p.waitICEGathering(ctx); err != nil {
		return nil, fmt.Errorf("peer: wait ICE gathering: %w", err)
	}

	if err := sig.Send(ctx, &signal.SignalMessage{
		Body: signal.SignalBody{Answer: &signal.SdpAnswer{Sdp: encodeSDP(*p.pc.LocalDescription())}},
	}); err != nil {
		return nil, fmt.Errorf("peer: send answer: %w", err)
	}

	return p.waitForOpen(ctx)
}

// waitICEGathering blocks until the local PeerConnection has gathered
// all ICE candidates. Non-trickle ICE: all candidates are then embedded
// in the local SDP before it is sent.
func (p *Peer) waitICEGathering(ctx context.Context) error {
	if s := p.pc.ICEGatheringState(); s == webrtc.ICEGatheringStateComplete {
		return nil
	}
	done := make(chan struct{})
	once := make(chan struct{}, 1)
	p.pc.OnICEGatheringStateChange(func(state webrtc.ICEGatheringState) {
		if state == webrtc.ICEGatheringStateComplete {
			select {
			case once <- struct{}{}:
				close(done)
			default:
			}
		}
	})
	select {
	case <-done:
		return nil
	case <-time.After(p.cfg.NegotiationTimeout):
		return errors.New("ice gathering timed out")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close tears down the PeerConnection.
func (p *Peer) Close() error {
	return p.pc.Close()
}

// handleDataChannel wires the DataChannel into a transport.Channel and
// notifies waitForOpen.
func (p *Peer) handleDataChannel(dc *webrtc.DataChannel) {
	dc.OnOpen(func() {
		ch := transport.New(dc)
		select {
		case p.openCh <- ch:
		default:
			// Already have a channel; should not happen for Phase 1.
		}
	})
}

// waitForOpen blocks until the DataChannel opens or the negotiation
// timeout elapses.
func (p *Peer) waitForOpen(ctx context.Context) (*transport.Channel, error) {
	timeout := p.cfg.NegotiationTimeout
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case ch := <-p.openCh:
		return ch, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("peer: negotiation timed out after %s", timeout)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// waitForOffer reads from the session until an SdpOffer arrives.
func waitForOffer(ctx context.Context, sig *signal.Session) (string, error) {
	for {
		select {
		case msg, ok := <-sig.Receive():
			if !ok {
				return "", errors.New("signal session closed")
			}
			if msg.Body.Offer != nil {
				return msg.Body.Offer.Sdp, nil
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// waitForAnswer reads from the session until an SdpAnswer arrives.
func waitForAnswer(ctx context.Context, sig *signal.Session) (string, error) {
	for {
		select {
		case msg, ok := <-sig.Receive():
			if !ok {
				return "", errors.New("signal session closed")
			}
			if msg.Body.Answer != nil {
				return msg.Body.Answer.Sdp, nil
			}
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// encodeSDP turns a pion SessionDescription into the on-wire string.
// pion already gives us an SDP string; this is a thin pass-through that
// centralizes the encoding in case we need to massage it later.
func encodeSDP(sd webrtc.SessionDescription) string { return sd.SDP }

func boolPtr(b bool) *bool { return &b }

func deref[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}
