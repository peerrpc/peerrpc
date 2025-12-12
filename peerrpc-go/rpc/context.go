package rpc

import "context"

// ctxKey is unexported so callers cannot synthesize one outside this
// package. The only path to attach outgoing header metadata to a
// context is via ctxWithOutgoingHeader.
type ctxKey int

const (
	ctxKeyOutgoingHeader ctxKey = iota
)

// ctxWithOutgoingHeader attaches a header map to ctx so the bottom of
// the client interceptor chain can fish it out without changing the
// UnaryInvoker signature.
func ctxWithOutgoingHeader(ctx context.Context, hdr map[string][]string) context.Context {
	return context.WithValue(ctx, ctxKeyOutgoingHeader, hdr)
}

// outgoingHeaderFromCtx returns the header previously attached, or
// nil if none was set.
func outgoingHeaderFromCtx(ctx context.Context) map[string][]string {
	if v := ctx.Value(ctxKeyOutgoingHeader); v != nil {
		if h, ok := v.(map[string][]string); ok {
			return h
		}
	}
	return nil
}
