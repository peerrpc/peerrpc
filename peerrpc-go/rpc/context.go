package rpc

import "context"

// ctxKey is unexported so callers cannot synthesize one outside this
// package. The only path to attach outgoing header metadata to a
// context is via ctxWithOutgoingHeader.
type ctxKey int

const (
	ctxKeyOutgoingHeader ctxKey = iota
)

// ctxWithOutgoingHeader is the unexported backing for
// WithOutgoingHeader. It exists so the rpc package's own callers
// (Invoke*) can attach metadata without inflating their public API.
func ctxWithOutgoingHeader(ctx context.Context, hdr map[string][]string) context.Context {
	return context.WithValue(ctx, ctxKeyOutgoingHeader, hdr)
}

// WithOutgoingHeader is the exported wrapper for external callers
// (observability interceptors, applications that want to attach
// metadata outside an Invoke* call).
func WithOutgoingHeader(ctx context.Context, hdr map[string][]string) context.Context {
	return ctxWithOutgoingHeader(ctx, hdr)
}

// outgoingHeaderFromCtx returns the header previously attached, or
// nil if none was set.
func outgoingHeaderFromCtx(ctx context.Context) map[string][]string {
	return OutgoingHeaderFromCtx(ctx)
}

// OutgoingHeaderFromCtx is the exported wrapper for observability
// interceptors that need to read the existing outgoing header map
// (so they can extend rather than replace it).
func OutgoingHeaderFromCtx(ctx context.Context) map[string][]string {
	if v := ctx.Value(ctxKeyOutgoingHeader); v != nil {
		if h, ok := v.(map[string][]string); ok {
			return h
		}
	}
	return nil
}
