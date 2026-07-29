package peerrpc

import (
	"crypto/ed25519"
	"time"

	"github.com/peerrpc/go/rpc"
)

// DialOption configures a Dial call. Options are applied in order;
// later options override earlier ones for the same field.
type DialOption interface {
	applyDial(*dialConfig)
}

// ListenOption configures a Listen call. Same precedence rules as
// DialOption apply.
type ListenOption interface {
	applyListen(*listenConfig)
}

// option is a private type that implements both DialOption and
// ListenOption, letting the With* helpers configure both sides with
// a single implementation. Callers see DialOption / ListenOption as
// separate types so a Dial option cannot be passed to Listen by
// accident (and vice versa) for options that only make sense on one
// side.
type option func(*dialConfig, *listenConfig)

func (o option) applyDial(c *dialConfig)    { o(c, nil) }
func (o option) applyListen(c *listenConfig) { o(nil, c) }

// dialConfig is the internal shape assembled from a Target and a
// list of DialOptions. dialTarget consumes it.
type dialConfig struct {
	target        Target
	ICEServers    []peerICEServer
	iceCascade    bool
	negotiationTO time.Duration
	token         string
	identity      ed25519.PrivateKey
	interceptors  []rpc.UnaryClientInterceptor
}

// listenConfig mirrors dialConfig for the server side.
type listenConfig struct {
	target        Target
	ICEServers    []peerICEServer
	iceCascade    bool
	negotiationTO time.Duration
	token         string
	identity      ed25519.PrivateKey
	interceptors  []rpc.UnaryServerInterceptor
	streamInterceptors []rpc.StreamServerInterceptor
}

// peerICEServer is a transport-agnostic ICE server spec. It mirrors
// webrtc.ICEServer but without dragging pion into this package's
// public surface.
type peerICEServer struct {
	URLs       []string
	Username   string
	Credential string
}

// WithToken attaches a bearer token to the signaling stream and the
// underlying HTTP request. Equivalent to setting Target.Token.
func WithToken(token string) option {
	return func(d *dialConfig, l *listenConfig) {
		if d != nil {
			d.token = token
		}
		if l != nil {
			l.token = token
		}
	}
}

// WithIdentity injects an Ed25519 private key whose derived PeerID
// is announced on the signaling stream. Servers accept the key on the
// wire but do NOT yet verify signatures server-side; full
// verification ships in a future release. Until then this option only affects
// the peer_id derivation (hash of the public key) when Target.PeerID
// is empty.
func WithIdentity(priv ed25519.PrivateKey) option {
	return func(d *dialConfig, l *listenConfig) {
		if d != nil {
			d.identity = priv
		}
		if l != nil {
			l.identity = priv
		}
	}
}

// WithICEServers configures the WebRTC ICE servers (STUN/TURN).
// Each entry mirrors a webrtc.ICEServer: a slice of URLs plus
// optional short-lived TURN credentials (RFC 7635).
func WithICEServers(servers ...ICEServer) option {
	conv := make([]peerICEServer, 0, len(servers))
	for _, s := range servers {
		conv = append(conv, peerICEServer{
			URLs:       s.URLs,
			Username:   s.Username,
			Credential: s.Credential,
		})
	}
	return func(d *dialConfig, l *listenConfig) {
		if d != nil {
			d.ICEServers = conv
			d.iceCascade = true
		}
		if l != nil {
			l.ICEServers = conv
			l.iceCascade = true
		}
	}
}

// ICEServer is the public, pion-free ICE server descriptor accepted
// by WithICEServers.
type ICEServer struct {
	URLs       []string
	Username   string
	Credential string
}

// WithNegotiationTimeout caps how long Dial/Listen waits for the
// DataChannel to open (Dial side) or for an offer to arrive (Listen
// side). Defaults to 10s when unset.
func WithNegotiationTimeout(d time.Duration) option {
	return func(dc *dialConfig, lc *listenConfig) {
		if dc != nil {
			dc.negotiationTO = d
		}
		if lc != nil {
			lc.negotiationTO = d
		}
	}
}

// WithUnaryClientInterceptors installs a chain of client-side unary
// interceptors. Order is outermost-first. Dial-only.
func WithUnaryClientInterceptors(is ...rpc.UnaryClientInterceptor) DialOption {
	return option(func(d *dialConfig, _ *listenConfig) {
		if d != nil {
			d.interceptors = append(d.interceptors, is...)
		}
	})
}

// WithUnaryServerInterceptors installs a chain of server-side unary
// interceptors. Listen-only.
func WithUnaryServerInterceptors(is ...rpc.UnaryServerInterceptor) ListenOption {
	return option(func(_ *dialConfig, l *listenConfig) {
		if l != nil {
			l.interceptors = append(l.interceptors, is...)
		}
	})
}

// WithStreamServerInterceptors installs a chain of server-side stream
// interceptors. Listen-only.
func WithStreamServerInterceptors(is ...rpc.StreamServerInterceptor) ListenOption {
	return option(func(_ *dialConfig, l *listenConfig) {
		if l != nil {
			l.streamInterceptors = append(l.streamInterceptors, is...)
		}
	})
}
