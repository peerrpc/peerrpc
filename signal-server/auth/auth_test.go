package auth_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/peerrpc/signal-server/auth"
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

func TestAuthorizeRequest_HeaderAndQuery(t *testing.T) {
	v := auth.StaticValidator{Identities: map[string]auth.Identity{
		"valid": {Subject: "alice"},
	}}

	// Bearer header.
	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	req.Header.Set("Authorization", "Bearer valid")
	rec := httptest.NewRecorder()
	if _, ok := auth.AuthorizeRequest(rec, req, v); !ok {
		t.Fatalf("bearer header: expected authorized, got status %d", rec.Code)
	}

	// Query param fallback.
	req = httptest.NewRequest(http.MethodGet, "/ws?token=valid", nil)
	rec = httptest.NewRecorder()
	if _, ok := auth.AuthorizeRequest(rec, req, v); !ok {
		t.Fatalf("query token: expected authorized, got status %d", rec.Code)
	}

	// Missing token.
	req = httptest.NewRequest(http.MethodGet, "/ws", nil)
	rec = httptest.NewRecorder()
	if _, ok := auth.AuthorizeRequest(rec, req, v); ok {
		t.Fatalf("missing token: expected unauthorized")
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token: got status %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	// Nil validator => always authorized (no auth configured).
	req = httptest.NewRequest(http.MethodGet, "/ws", nil)
	rec = httptest.NewRecorder()
	if _, ok := auth.AuthorizeRequest(rec, req, nil); !ok {
		t.Fatalf("nil validator: expected authorized")
	}
}
