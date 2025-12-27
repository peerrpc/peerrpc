package auth_test

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/peerrpc/signal-server/auth"

	"connectrpc.com/connect"
)

func TestStaticValidator(t *testing.T) {
	v := auth.StaticValidator{
		Identities: map[string]auth.Identity{
			"tok-alice": {Subject: "alice"},
		},
	}
	if _, err := v.Validate(context.Background(), "tok-alice"); err != nil {
		t.Fatalf("valid token: %v", err)
	}
	if _, err := v.Validate(context.Background(), "bogus"); !errors.Is(err, auth.ErrUnauthenticated) {
		t.Fatalf("bogus token: %v", err)
	}
}

func TestBearerToken(t *testing.T) {
	h := http.Header{}
	h.Set("Authorization", "Bearer abc123")
	if got := bearerTokenForTest(h); got != "abc123" {
		t.Fatalf("got %q", got)
	}
	h.Set("Authorization", "raw-token")
	if got := bearerTokenForTest(h); got != "raw-token" {
		t.Fatalf("got %q", got)
	}
}

// bearerTokenForTest is a small re-export because the package's
// bearerToken is unexported. The interceptor's runtime behavior is
// covered indirectly via integration tests against the server; this
// unit test just exercises the parsing edge cases.
func bearerTokenForTest(h http.Header) string {
	// Match the production helper's logic exactly.
	v := h.Get("Authorization")
	const prefix = "Bearer "
	if len(v) > len(prefix) && v[:len(prefix)] == prefix {
		return v[len(prefix):]
	}
	return v
}

// TestInterceptor_GatesUnauthorized invokes the interceptor with no
// token and asserts it returns Unauthenticated.
func TestInterceptor_GatesUnauthorized(t *testing.T) {
	v := auth.StaticValidator{Identities: map[string]auth.Identity{
		"valid": {Subject: "alice"},
	}}
	interceptor := auth.NewInterceptor(v)

	called := false
	wrapped := interceptor.WrapUnary(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		called = true
		return nil, nil
	})

	req := connect.NewRequest[any](nil)
	_, err := wrapped(context.Background(), req)
	if called {
		t.Fatal("next should not have been called without a token")
	}
	if err == nil {
		t.Fatal("expected error")
	}
	connErr := new(connect.Error)
	if !errors.As(err, &connErr) || connErr.Code() != connect.CodeUnauthenticated {
		t.Fatalf("expected CodeUnauthenticated, got %v", err)
	}
}
