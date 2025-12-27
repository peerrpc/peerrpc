package auth

import "context"

type ctxKey int

const (
	ctxKeyIdentity ctxKey = iota
)

// contextWithIdentity attaches id to ctx. Unexported so callers
// cannot synthesize identities outside this package.
func contextWithIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, ctxKeyIdentity, id)
}

// IdentityFromContext extracts the Identity previously attached by
// the auth interceptor, or returns ok=false if none is set.
func IdentityFromContext(ctx context.Context) (Identity, bool) {
	if v := ctx.Value(ctxKeyIdentity); v != nil {
		if id, ok := v.(Identity); ok {
			return id, true
		}
	}
	return Identity{}, false
}
