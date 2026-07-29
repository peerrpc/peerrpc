package peerrpc

import (
	"github.com/peerrpc/go/peer"
	"github.com/pion/webrtc/v4"
)

// buildPeerConfig translates the dialConfig's transport-related
// fields into a peer.Config. It is shared by Dial and Listen so the
// ICE cascade mapping stays in one place.
//
// WithICEServers wins over Target.Token/Role hints. When no
// WithICEServers option is supplied, peer.Config.applyDefaults keeps
// the empty list (localhost-only operation); this matches the v1
// behavior.
func buildPeerConfig(cfg dialConfig) (peer.Config, error) {
	pc := peer.Config{}
	if cfg.negotiationTO != 0 {
		pc.NegotiationTimeout = cfg.negotiationTO
	}
	if cfg.iceCascade {
		iceServers := make([]webrtc.ICEServer, 0, len(cfg.ICEServers))
		for _, s := range cfg.ICEServers {
			iceServers = append(iceServers, webrtc.ICEServer{
				URLs:       s.URLs,
				Username:   s.Username,
				Credential: s.Credential,
			})
		}
		pc.ICEServers = iceServers
	}
	return pc, nil
}

// buildPeerConfigListen mirrors buildPeerConfig for the server side.
// Kept separate so future server-specific tuning knobs don't pollute
// the client path.
func buildPeerConfigListen(cfg listenConfig) (peer.Config, error) {
	pc := peer.Config{}
	if cfg.negotiationTO != 0 {
		pc.NegotiationTimeout = cfg.negotiationTO
	}
	if cfg.iceCascade {
		iceServers := make([]webrtc.ICEServer, 0, len(cfg.ICEServers))
		for _, s := range cfg.ICEServers {
			iceServers = append(iceServers, webrtc.ICEServer{
				URLs:       s.URLs,
				Username:   s.Username,
				Credential: s.Credential,
			})
		}
		pc.ICEServers = iceServers
	}
	return pc, nil
}
