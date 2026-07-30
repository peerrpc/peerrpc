// Package auth provides PeerRPC authentication: a JWT crypto layer
// (HS256Verifier / Claims / IssueHS256 in jwt.go) and an HTTP/WebSocket
// transport layer (TokenValidator / Identity / AuthorizeRequest in
// transport.go) consumed by the signalserver package.
//
// The package uses HMAC-SHA256 (HS256) by default because it is the
// cheapest verifier that does not require an external key store. RSA
// and ECDSA verifiers are pluggable via the Verifier interface; v1
// ships only HS256 since the short-lived token issuer (the signaling
// server) is also the verifier in the simplest deployment shape.
package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Verifier is the credential-checking contract. The signaling server's
// auth.Interceptor wraps any Verifier implementation.
type Verifier interface {
	// Verify checks a raw JWT and returns its claims if valid.
	Verify(ctx context.Context, token string) (Claims, error)
}

// Claims is the subset of JWT claims PeerRPC consumes.
type Claims struct {
	// Subject is the peer_id the token authorizes. The signaling
	// server surfaces this in its handler logging.
	Subject string `json:"sub"`
	// ExpiresAt is the unix-seconds expiry. Verify rejects tokens
	// whose ExpiresAt has passed.
	ExpiresAt int64 `json:"exp"`
	// IssuedAt is the unix-seconds issue time.
	IssuedAt int64 `json:"iat"`
	// Service optionally scopes the token to one service. The
	// signaling server may use this to reject tokens that try to
	// announce against an unrelated service.
	Service string `json:"service,omitempty"`
}

// ErrInvalidToken is the sentinel returned for malformed or
// unverified tokens. Callers SHOULD wrap with ErrUnauthenticated
// at higher layers.
var ErrInvalidToken = errors.New("auth: invalid or unverified token")

// ErrExpired is the sentinel for expired tokens.
var ErrExpired = errors.New("auth: token expired")

// HS256Verifier verifies HMAC-SHA256 JWTs signed with the configured
// secret. Use one Verifier per secret; rotate secrets by
// constructing a new Verifier and replacing the old one in the
// signaling server's auth.NewInterceptor call.
type HS256Verifier struct {
	Secret  []byte
	Now     func() time.Time // injectable for tests; defaults to time.Now
	Leeway  time.Duration    // clock skew tolerance
}

// Verify implements Verifier.
func (v HS256Verifier) Verify(_ context.Context, token string) (Claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return Claims{}, fmt.Errorf("%w: expected 3 segments, got %d", ErrInvalidToken, len(parts))
	}

	// Verify signature BEFORE decoding the payload to avoid trusting
	// any field of a tampered token.
	signingInput := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, v.Secret)
	mac.Write([]byte(signingInput))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(parts[2]), []byte(expected)) {
		return Claims{}, fmt.Errorf("%w: signature mismatch", ErrInvalidToken)
	}

	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Claims{}, fmt.Errorf("%w: bad header encoding: %v", ErrInvalidToken, err)
	}
	var header struct {
		Alg string `json:"alg"`
		Typ string `json:"typ"`
	}
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return Claims{}, fmt.Errorf("%w: bad header json: %v", ErrInvalidToken, err)
	}
	if header.Alg != "HS256" {
		return Claims{}, fmt.Errorf("%w: alg %q not supported", ErrInvalidToken, header.Alg)
	}

	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Claims{}, fmt.Errorf("%w: bad payload encoding: %v", ErrInvalidToken, err)
	}
	var claims Claims
	if err := json.Unmarshal(payloadJSON, &claims); err != nil {
		return Claims{}, fmt.Errorf("%w: bad payload json: %v", ErrInvalidToken, err)
	}

	now := v.now()
	if claims.ExpiresAt > 0 && now.Unix() > claims.ExpiresAt+int64(v.Leeway.Seconds()) {
		return Claims{}, fmt.Errorf("%w: exp %d, now %d", ErrExpired, claims.ExpiresAt, now.Unix())
	}
	return claims, nil
}

// now returns v.Now or time.Now if v.Now is nil.
func (v HS256Verifier) now() time.Time {
	if v.Now != nil {
		return v.Now()
	}
	return time.Now()
}

// IssueHS256 signs a JWT for the given claims. It is provided so the
// signaling server (or its token issuer) and tests can produce
// tokens without depending on a separate JWT library.
func IssueHS256(secret []byte, claims Claims) (string, error) {
	header := map[string]string{"alg": "HS256", "typ": "JWT"}
	headerJSON, _ := json.Marshal(header)
	payloadJSON, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("auth: marshal claims: %w", err)
	}

	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." +
		base64.RawURLEncoding.EncodeToString(payloadJSON)
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(signingInput))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signingInput + "." + sig, nil
}
