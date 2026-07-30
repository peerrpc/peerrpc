// Package auth provides token validation that gates the signaling
// service on room tokens.
//
// v1 ships a TokenValidatorFunc abstraction and a StaticValidator
// suitable for tests and single-binary deployments. Production
// deployments SHOULD plug in a JWT validator that issues per-peer
// credentials scoped to a specific room and expiry (PLAN.md §9.2).
//
// The signaling server consumes a TokenValidator from both the
// WebSocket path. A token may be presented as the Authorization
// header ("Bearer <token>" or bare token) or the "token" query
// parameter (the latter is the canonical form for WebSocket clients,
// which cannot always set headers during the handshake).
package auth

import (
	"context"
	"errors"
	"net/http"

	"github.com/gorilla/websocket"
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

// tokenFromRequest extracts the bearer token from an HTTP request,
// checking the Authorization header first and falling back to the
// "token" query parameter (used by WebSocket clients).
func tokenFromRequest(r *http.Request) string {
	if tok := bearerToken(r.Header); tok != "" {
		return tok
	}
	return r.URL.Query().Get("token")
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

// AuthorizeRequest validates the token carried by r. On success it
// returns the Identity; on failure it writes a 401 response (and, for
// WebSocket handshakes, a close frame) and returns ok=false.
//
// When v is nil, every request is authorized (no auth configured).
func AuthorizeRequest(w http.ResponseWriter, r *http.Request, v TokenValidator) (Identity, bool) {
	if v == nil {
		return Identity{}, true
	}
	tok := tokenFromRequest(r)
	id, err := v.Validate(r.Context(), tok)
	if err == nil {
		return id, true
	}
	// Reject the WebSocket upgrade before it completes.
	if isUpgradeRequest(r) {
		// gorilla/websocket upgrader will not run our handler on a
		// failed handshake; instead we write a plain 401. Some clients
		// (browsers) hide the body, so a clear status line is enough.
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusUnauthorized)
		return Identity{}, false
	}
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return Identity{}, false
}

// isUpgradeRequest reports whether r is a WebSocket upgrade request.
func isUpgradeRequest(r *http.Request) bool {
	return websocket.IsWebSocketUpgrade(r)
}
