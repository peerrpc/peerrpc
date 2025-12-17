package peer_test

import (
	"testing"

	"github.com/peerrpc/go/peer"
	"github.com/pion/webrtc/v4"
)

func TestTierForCandidateType(t *testing.T) {
	cases := []struct {
		in   webrtc.ICECandidateType
		want peer.ICETier
	}{
		{webrtc.ICECandidateTypeHost, peer.ICETierHost},
		{webrtc.ICECandidateTypeSrflx, peer.ICETierSrflx},
		{webrtc.ICECandidateTypePrflx, peer.ICETierSrflx},
		{webrtc.ICECandidateTypeRelay, peer.ICETierRelay},
	}
	for _, c := range cases {
		if got := peer.TierForCandidateType(c.in); got != c.want {
			t.Errorf("TierForCandidateType(%s) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestTierForPair_PicksWorse(t *testing.T) {
	if got := peer.TierForPair(webrtc.ICECandidateTypeHost, webrtc.ICECandidateTypeHost); got != peer.ICETierHost {
		t.Errorf("host/host = %v, want host", got)
	}
	if got := peer.TierForPair(webrtc.ICECandidateTypeHost, webrtc.ICECandidateTypeSrflx); got != peer.ICETierSrflx {
		t.Errorf("host/srflx = %v, want srflx", got)
	}
	if got := peer.TierForPair(webrtc.ICECandidateTypeSrflx, webrtc.ICECandidateTypeRelay); got != peer.ICETierRelay {
		t.Errorf("srflx/relay = %v, want relay", got)
	}
	if got := peer.TierForPair(webrtc.ICECandidateTypeHost, webrtc.ICECandidateTypeRelay); got != peer.ICETierRelay {
		t.Errorf("host/relay = %v, want relay", got)
	}
}

func TestICEServersForCascade_Host(t *testing.T) {
	s, err := peer.ICEServersForCascade(peer.ICETierHost, "stun:x:3478", "turn:y:3478", "u", "c")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(s) != 0 {
		t.Errorf("expected no ICE servers for host tier, got %v", s)
	}
}

func TestICEServersForCascade_Srflx(t *testing.T) {
	s, err := peer.ICEServersForCascade(peer.ICETierSrflx, "stun:x:3478", "turn:y:3478", "u", "c")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(s) != 1 {
		t.Fatalf("got %d servers, want 1", len(s))
	}
	if s[0].URLs[0] != "stun:x:3478" {
		t.Errorf("server[0]: %v", s[0])
	}
}

func TestICEServersForCascade_Relay(t *testing.T) {
	s, err := peer.ICEServersForCascade(peer.ICETierRelay, "stun:x:3478", "turn:y:3478", "u", "c")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if len(s) != 2 {
		t.Fatalf("got %d servers, want 2 (stun+turn)", len(s))
	}
	if s[1].Username != "u" || s[1].Credential != "c" {
		t.Errorf("turn creds: %+v", s[1])
	}
}

func TestICEServersForCascade_Errors(t *testing.T) {
	if _, err := peer.ICEServersForCascade(peer.ICETierSrflx, "", "", "", ""); err == nil {
		t.Fatal("srflx without stun should error")
	}
	if _, err := peer.ICEServersForCascade(peer.ICETierRelay, "stun:x:3478", "", "", ""); err == nil {
		t.Fatal("relay without turn should error")
	}
}

func TestTierString(t *testing.T) {
	if peer.ICETierHost.String() != "host" {
		t.Fail()
	}
	if peer.ICETierSrflx.String() != "srflx" {
		t.Fail()
	}
	if peer.ICETierRelay.String() != "relay" {
		t.Fail()
	}
}
