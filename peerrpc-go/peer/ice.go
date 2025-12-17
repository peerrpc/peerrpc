// ICE cascade helpers.
//
// The three-tier cascade (host -> srflx -> relay) is implicit in
// WebRTC: pion gathers every enabled candidate type in parallel and
// the ICE agent picks the best pair. These helpers exist to make the
// cascade observable to the application and to centralize the
// STUN/TURN wiring so callers don't reinvent it.
package peer

import (
	"fmt"

	"github.com/pion/webrtc/v4"
)

// ICETier names the cascade level a candidate pair reached.
type ICETier int

const (
	ICETierUnknown ICETier = iota
	ICETierHost            // Level 0: same-LAN host candidate
	ICETierSrflx           // Level 1: STUN-reflected public address
	ICETierRelay           // Level 2: TURN relay
)

// String renders the tier as a stable identifier for logs / metrics.
func (t ICETier) String() string {
	switch t {
	case ICETierHost:
		return "host"
	case ICETierSrflx:
		return "srflx"
	case ICETierRelay:
		return "relay"
	default:
		return "unknown"
	}
}

// TierForCandidateType maps a pion candidate type to its cascade tier.
// Unknown types map to ICETierUnknown; the application should treat
// that as "best effort, may not be P2P".
func TierForCandidateType(t webrtc.ICECandidateType) ICETier {
	switch t {
	case webrtc.ICECandidateTypeHost:
		return ICETierHost
	case webrtc.ICECandidateTypeSrflx:
		return ICETierSrflx
	case webrtc.ICECandidateTypeRelay:
		return ICETierRelay
	default:
		// prflx (peer-reflexive) is observed in some NAT scenarios.
		// Treat it the same as srflx for cascade-ranking purposes:
		// the agent learned a public address without a TURN relay.
		if t == webrtc.ICECandidateTypePrflx {
			return ICETierSrflx
		}
		return ICETierUnknown
	}
}

// TierForPair picks the worse of the two candidate types' tiers. This
// is how an observer reports "which tier the connection ended up at":
// a host-to-relay pair is effectively relay from the application's
// perspective (one side cannot be reached directly).
func TierForPair(localType, remoteType webrtc.ICECandidateType) ICETier {
	l := TierForCandidateType(localType)
	r := TierForCandidateType(remoteType)
	if l == ICETierUnknown || r == ICETierUnknown {
		return ICETierUnknown
	}
	if l > r {
		return l
	}
	return r
}

// STUNServer returns an ICEServer entry for a plain STUN URL.
//
//	stun:stun.example.com:3478
func STUNServer(url string) webrtc.ICEServer {
	return webrtc.ICEServer{URLs: []string{url}}
}

// TURNServer returns an ICEServer entry for a TURN URL with the given
// short-lived credentials (RFC 7635).
//
//	turn:turn.example.com:3478
//	tcp:turn.example.com:3478?transport=tcp
//
// The credentials are typically issued by the signaling server and
// scoped to a specific peer_id with a 1-hour expiry.
func TURNServer(url, username, credential string) webrtc.ICEServer {
	return webrtc.ICEServer{
		URLs:       []string{url},
		Username:   username,
		Credential: credential,
	}
}

// ICEServersForCascade builds a server list that enables the cascade
// up to and including the given tier. TierHost yields an empty list
// (host candidates don't need a server); TierSrflx adds the STUN
// server; TierRelay adds both STUN and TURN.
//
// Use this to centralize the cascade wiring in a single call site:
//
//	servers := peer.ICEServersForCascade(
//	    peer.ICETierRelay,
//	    "stun:stun.example.com:3478",
//	    "turn:turn.example.com:3478",
//	    shortLivedUsername, shortLivedCredential,
//	)
//	cfg := peer.Config{ICEServers: servers, /* ... */}
func ICEServersForCascade(tier ICETier, stunURL, turnURL, turnUser, turnCred string) ([]webrtc.ICEServer, error) {
	switch tier {
	case ICETierHost:
		return nil, nil
	case ICETierSrflx:
		if stunURL == "" {
			return nil, fmt.Errorf("peer: srflx tier requires a stun url")
		}
		return []webrtc.ICEServer{STUNServer(stunURL)}, nil
	case ICETierRelay:
		if stunURL == "" || turnURL == "" {
			return nil, fmt.Errorf("peer: relay tier requires both stun and turn urls")
		}
		return []webrtc.ICEServer{
			STUNServer(stunURL),
			TURNServer(turnURL, turnUser, turnCred),
		}, nil
	default:
		return nil, fmt.Errorf("peer: unknown ICE tier %d", tier)
	}
}
