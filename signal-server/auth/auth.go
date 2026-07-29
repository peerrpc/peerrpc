// Package auth provides Connect interceptors that gate the signaling
// service on room tokens.
//
// v1 ships a TokenValidatorFunc abstraction and a StaticValidator
// suitable for tests and single-binary deployments. Production
// deployments SHOULD plug in a JWT validator that issues per-peer
// credentials scoped to a specific room and expiry (PLAN.md §9.2).
package auth

import (
	"context"
	"errors"
	"net/http"

	"connectrpc.com/connect"
)

// ErrUnauthenticated is returned by TokenValidator.Validate when the
// presented token is invalid, expired, or otherwise unusable.
var ErrUnauthenticated = errors.New("auth: invalid or expired token")

// Identity is the principal extracted from a valid token. The
// signaling server uses it only for logging and for surfacing the
// caller to handlers via context; it does NOT drive authorization
// decisions in v1 (every valid token is treated equivalently).
type Identity struct {
	Subject string
	Service string
}

// TokenValidator checks a bearer token and returns the Identity it
// represents, or an error wrapped with ErrUnauthenticated.
type TokenValidator interface {
	Validate(ctx context.Context, token string) (Identity, error)
}

// TokenValidatorFunc lets a function satisfy TokenValidator.
type TokenValidatorFunc func(ctx context.Context, token string) (Identity, error)

// Validate implements TokenValidator.
func (f TokenValidatorFunc) Validate(ctx context.Context, token string) (Identity, error) {
	return f(ctx, token)
}

// StaticValidator accepts any of the configured tokens. Useful for
// development and integration tests where each peer has a long-lived
// pre-shared token.
type StaticValidator struct {
	Identities map[string]Identity // token -> identity
}

// Validate implements TokenValidator.
func (s StaticValidator) Validate(_ context.Context, token string) (Identity, error) {
	if id, ok := s.Identities[token]; ok {
		return id, nil
	}
	return Identity{}, ErrUnauthenticated
}

// NewInterceptor returns a connect.UnaryInterceptorFunc-style chain
// entry that gates incoming requests. For the signaling server's
// single Exchange RPC, this runs once when the bidi stream opens.
func NewInterceptor(v TokenValidator) connect.Interceptor {
	return connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			tok := bearerToken(req.Header())
			id, err := v.Validate(ctx, tok)
			if err != nil {
				return nil, connect.NewError(connect.CodeUnauthenticated, err)
			}
			return next(contextWithIdentity(ctx, id), req)
		}
	})
}

// bearerToken extracts the raw token from the Authorization header.
// Accepts "Bearer <token>" or the bare token form.
func bearerToken(h http.Header) string {
	v := h.Get("Authorization")
	const prefix = "Bearer "
	if len(v) > len(prefix) && v[:len(prefix)] == prefix {
		return v[len(prefix):]
	}
	return v
}
