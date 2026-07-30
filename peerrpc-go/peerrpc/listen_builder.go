package peerrpc

import (
	"context"
	"time"
)

// ListenBuilder is the fluent entry point for the server side.
//
//	ln, err := peerrpc.ListenContext(ctx).
//	    SignalAt("signal.example.com").
//	    Service("echo.Echo").
//	    Over(peerrpc.SchemeWS).
//	    WithToken(jwt).
//	    Listen()
type ListenBuilder struct {
	ctx    context.Context
	target Target
	opts   []ListenOption
}

// ListenContext returns a fresh ListenBuilder bound to ctx.
func ListenContext(ctx context.Context) *ListenBuilder {
	return &ListenBuilder{ctx: ctx}
}

// SignalAt sets Target.Signal.
func (b *ListenBuilder) SignalAt(authority string) *ListenBuilder {
	b.target.Signal = authority
	return b
}

// Service sets Target.Service.
func (b *ListenBuilder) Service(name string) *ListenBuilder {
	b.target.Service = name
	return b
}

// Over sets Target.Scheme. Defaults to SchemeWS if unset.
func (b *ListenBuilder) Over(s Scheme) *ListenBuilder {
	b.target.Scheme = s
	return b
}

// As sets Target.Role. Defaults to RoleHintServer for Listen.
func (b *ListenBuilder) As(r RoleHint) *ListenBuilder {
	b.target.Role = r
	return b
}

// WithPeerID sets Target.PeerID. When omitted, each Accept
// auto-generates a peer_id; when set, Accept suffixes it with a
// short UUID to keep sessions distinct.
func (b *ListenBuilder) WithPeerID(id string) *ListenBuilder {
	b.target.PeerID = id
	return b
}

// WithToken sets Target.Token.
func (b *ListenBuilder) WithToken(token string) *ListenBuilder {
	b.target.Token = token
	return b
}

// WithICEServers installs ICE servers.
func (b *ListenBuilder) WithICEServers(servers ...ICEServer) *ListenBuilder {
	b.opts = append(b.opts, WithICEServers(servers...))
	return b
}

// WithNegotiationTimeout caps Accept wait. Default 10s.
func (b *ListenBuilder) WithNegotiationTimeout(d time.Duration) *ListenBuilder {
	b.opts = append(b.opts, WithNegotiationTimeout(d))
	return b
}

// Apply runs opts against the builder's option list.
func (b *ListenBuilder) Apply(opts ...ListenOption) *ListenBuilder {
	b.opts = append(b.opts, opts...)
	return b
}

// Listen finalizes the builder and runs ListenTarget.
func (b *ListenBuilder) Listen() (*Listener, error) {
	if b.target.Scheme == "" {
		b.target.Scheme = SchemeWS
	}
	return ListenTarget(b.ctx, b.target, b.opts...)
}
