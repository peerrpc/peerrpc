package peerrpc

import (
	"context"
	"time"
)

// DialBuilder is the fluent entry point. It assembles a Target plus
// DialOptions via chained methods and finalizes with Connect.
//
//	conn, err := peerrpc.DialContext(ctx).
//	    SignalAt("signal.example.com").
//	    Service("echo.Echo").
//	    Over(peerrpc.SchemeWS).
//	    WithToken(jwt).
//	    Connect()
//
// All setters return the same builder so they chain. A nil receiver
// panics on use; build with DialContext, never &DialBuilder{}.
type DialBuilder struct {
	ctx   context.Context
	target Target
	opts  []DialOption
}

// DialContext returns a fresh DialBuilder bound to ctx. ctx is
// propagated through Connect into DialTarget and the underlying
// peer.Dial / Accept.
func DialContext(ctx context.Context) *DialBuilder {
	return &DialBuilder{ctx: ctx}
}

// SignalAt sets Target.Signal (the signal-server authority). Has no
// effect for SchemeLocal.
func (b *DialBuilder) SignalAt(authority string) *DialBuilder {
	b.target.Signal = authority
	return b
}

// Service sets Target.Service (the rendezvous key, e.g. "echo.Echo").
func (b *DialBuilder) Service(name string) *DialBuilder {
	b.target.Service = name
	return b
}

// Over sets Target.Scheme. Defaults to SchemeWS if unset.
func (b *DialBuilder) Over(s Scheme) *DialBuilder {
	b.target.Scheme = s
	return b
}

// As sets Target.Role. Defaults to RoleHintClient for a Dial.
func (b *DialBuilder) As(r RoleHint) *DialBuilder {
	b.target.Role = r
	return b
}

// WithPeerID sets Target.PeerID. When omitted, Connect auto-generates
// a UUIDv4 peer_id (UUIDv7 once google/uuid is bumped past v1.7).
func (b *DialBuilder) WithPeerID(id string) *DialBuilder {
	b.target.PeerID = id
	return b
}

// WithToken sets Target.Token. Equivalent to the WithToken option.
func (b *DialBuilder) WithToken(token string) *DialBuilder {
	b.target.Token = token
	return b
}

// WithICEServers installs ICE servers. Same semantics as the
// standalone WithICEServers option.
func (b *DialBuilder) WithICEServers(servers ...ICEServer) *DialBuilder {
	b.opts = append(b.opts, WithICEServers(servers...))
	return b
}

// WithNegotiationTimeout caps Dial/Accept wait. Default 10s.
func (b *DialBuilder) WithNegotiationTimeout(d time.Duration) *DialBuilder {
	b.opts = append(b.opts, WithNegotiationTimeout(d))
	return b
}

// Apply runs opts against the builder's option list. Use it to share
// a slice of options across multiple dials.
func (b *DialBuilder) Apply(opts ...DialOption) *DialBuilder {
	b.opts = append(b.opts, opts...)
	return b
}

// Connect finalizes the builder and runs DialTarget. Returns the
// same errors DialTarget does.
func (b *DialBuilder) Connect() (*Conn, error) {
	if b.target.Scheme == "" {
		b.target.Scheme = SchemeWS
	}
	return DialTarget(b.ctx, b.target, b.opts...)
}
