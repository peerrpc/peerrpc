// Package peerrpc is the top-level facade for building PeerRPC clients
// and servers. It collapses the five objects callers previously had to
// wire by hand (signal.Backend, signal.Session, peer.Peer,
// transport.Channel, rpc.Client/Server) into a single Dial / Listen
// entry point and a Target URI.
//
// # Quickstart (URL style)
//
//	conn, err := peerrpc.Dial(ctx,
//	    "peerrpc+ws://signal.example.com/echo.Echo",
//	    peerrpc.WithToken(jwt),
//	)
//	defer conn.Close()
//	resp, status := conn.InvokeUnary(ctx, "/echo.Echo/Unary", []byte("hi"), nil)
//
// # Quickstart (Target style)
//
//	conn, err := peerrpc.DialTarget(ctx, peerrpc.Target{
//	    Scheme:  peerrpc.SchemeWS,
//	    Signal:  "signal.example.com",
//	    Service: "echo.Echo",
//	}, peerrpc.WithToken(jwt))
//
// # Quickstart (Builder style)
//
//	conn, err := peerrpc.DialContext(ctx).
//	    SignalAt("signal.example.com").
//	    Service("echo.Echo").
//	    Over(peerrpc.SchemeWS).
//	    Connect()
//
// # Target URI grammar
//
//	peerrpc+<scheme>://<authority>/<service>[?<opts>]
//
//	scheme:
//	  local    → in-process signal backend (signal.Local); authority is ignored
//	  ws       → WebSocket (signal.WS); the default for Go clients
//	  relay    → explicit relay hop (not yet implemented in Go)
//
//	authority:  signal-server host; ignored for local
//	service:    the rendezvous key
//
//	query opts (all optional):
//	  ?as=client|server   role hint; defaults to client for Dial, server for Listen
//	  ?peer=<peer_id>     explicit peer_id; defaults to an auto-generated UUIDv7
//	  ?token=<jwt>        bearer token; may also be supplied via WithToken
//
// Three styles exist for ergonomic reasons (URL string, typed Target
// struct, fluent Builder). They are equivalent: each produces the
// same Target and flows through the same dialTarget / listenTarget
// core.
package peerrpc

import (
	"fmt"
	"net/url"
	"strings"
)

// Scheme names a signaling transport backend. It is the part between
// "peerrpc+" and "://" in a Target URI.
type Scheme string

const (
	// SchemeLocal routes through an in-process signal.Local backend.
	// Useful for tests and single-binary demos. The Target's Signal
	// (authority) field is ignored.
	SchemeLocal Scheme = "local"

	// SchemeWS routes through signal.WS, the WebSocket client. Works
	// over ws:// (cleartext) or wss:// (TLS). This is the default for
	// Go clients and the only network signaling transport shipped by
	// the Go SDK.
	SchemeWS Scheme = "ws"

	// SchemeRelay routes through an explicit relay hop. Not yet implemented.
	SchemeRelay Scheme = "relay"
)

// RoleHint tells the dialer/listener which side it is. The WebRTC
// offer direction is derived from this: the CLIENT (Dialer) side
// initiates the SDP offer.
type RoleHint string

const (
	// RoleHintClient is the Dialer side; it creates the SDP offer.
	RoleHintClient RoleHint = "client"

	// RoleHintServer is the Listener side; it accepts the offer.
	RoleHintServer RoleHint = "server"
)

// Target is the parsed form of a Target URI. All three API styles
// (URL / Target struct / Builder) produce one before dialing.
type Target struct {
	// Scheme selects the signaling transport. See the Scheme*
	// constants.
	Scheme Scheme

	// Signal is the signal-server authority (host[:port]) or empty
	// for SchemeLocal. A bare host is accepted ("signal.example.com");
	// a full URL is also accepted ("https://signal.example.com:443").
	Signal string

	// Service is the rendezvous key. Two peers wishing to exchange
	// SDP MUST announce against the same Service.
	Service string

	// Role is the application-level part. Defaults to Client for
	// Dial and Server for Listen when empty.
	Role RoleHint

	// PeerID is the caller-chosen identifier within the service.
	// When empty, the facade generates a UUIDv7. Override only when
	// you need a stable identity across reconnections.
	PeerID string

	// Token is a bearer token forwarded as `Authorization: Bearer
	// <token>` on the signaling stream. May also be supplied via
	// WithToken.
	Token string
}

// String renders a Target back to its canonical URI form. The output
// round-trips through ParseTarget.
func (t Target) String() string {
	var b strings.Builder
	b.WriteString("peerrpc+")
	b.WriteString(string(t.Scheme))
	b.WriteString("://")
	if t.Signal != "" {
		b.WriteString(t.Signal)
	}
	b.WriteByte('/')
	b.WriteString(t.Service)
	q := make(url.Values)
	if t.Role != "" {
		q.Set("as", string(t.Role))
	}
	if t.PeerID != "" {
		q.Set("peer", t.PeerID)
	}
	if t.Token != "" {
		q.Set("token", t.Token)
	}
	if enc := q.Encode(); enc != "" {
		b.WriteByte('?')
		b.WriteString(enc)
	}
	return b.String()
}

// ParseTarget parses a Target URI into a Target. Accepted forms:
//
//	peerrpc+ws://signal.example.com/echo.Echo
//	peerrpc+ws://signal.example.com:8443/echo.Echo?as=client&peer=alice
//	peerrpc+local:///echo.Echo                              (empty authority)
//	peerrpc+ws://signal.example.com/echo.Echo
//
// The leading "peerrpc+" prefix is REQUIRED; bare schemes such as
// "ws://..." are rejected to keep the namespace unambiguous.
//
// We avoid net/url here because it treats the embedded inner scheme
// (e.g. "ws") as part of the URL scheme and the signal host as
// the path, which would mangle signal.example.com:8443. A manual
// split keeps the grammar in this package and is straightforward.
func ParseTarget(uri string) (Target, error) {
	const prefix = "peerrpc+"
	if !strings.HasPrefix(uri, prefix) {
		return Target{}, fmt.Errorf("peerrpc: target URI must start with %q, got %q", prefix, uri)
	}
	rest := uri[len(prefix):] // e.g. "connect://signal.example.com/echo.Echo?as=client"

	schemeSep := "://"
	sepIdx := strings.Index(rest, schemeSep)
	if sepIdx < 0 {
		return Target{}, fmt.Errorf("peerrpc: target URI %q is missing %q after the scheme", uri, schemeSep)
	}
	scheme := Scheme(rest[:sepIdx])
	afterScheme := rest[sepIdx+len(schemeSep):]

	// Split authority from path+query at the first '/'.
	var authority, pathQuery string
	if slashIdx := strings.Index(afterScheme, "/"); slashIdx < 0 {
		authority = afterScheme
	} else {
		authority = afterScheme[:slashIdx]
		pathQuery = afterScheme[slashIdx+1:]
	}

	// Split service path from query.
	var service, rawQuery string
	if qIdx := strings.Index(pathQuery, "?"); qIdx >= 0 {
		service = pathQuery[:qIdx]
		rawQuery = pathQuery[qIdx+1:]
	} else {
		service = pathQuery
	}
	if service == "" {
		return Target{}, fmt.Errorf("peerrpc: target URI %q is missing the service path (e.g. /echo.Echo)", uri)
	}

	t := Target{
		Scheme:  scheme,
		Signal:  authority,
		Service: service,
	}
	if rawQuery != "" {
		vals, err := url.ParseQuery(rawQuery)
		if err != nil {
			return Target{}, fmt.Errorf("peerrpc: malformed query in %q: %w", uri, err)
		}
		if as := vals.Get("as"); as != "" {
			t.Role = RoleHint(as)
		}
		if p := vals.Get("peer"); p != "" {
			t.PeerID = p
		}
		if tok := vals.Get("token"); tok != "" {
			t.Token = tok
		}
	}
	if t.Signal == "" && t.Scheme != SchemeLocal {
		return Target{}, fmt.Errorf("peerrpc: scheme %q requires a non-empty authority", t.Scheme)
	}
	return t, nil
}
