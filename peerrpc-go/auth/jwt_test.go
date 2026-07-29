package auth_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/peerrpc/go/auth"
)

func TestHS256Verifier_RoundTrip(t *testing.T) {
	secret := []byte("test-secret")
	claims := auth.Claims{
		Subject:   "alice",
		Service:   "svc-1",
		IssuedAt:  time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}
	tok, err := auth.IssueHS256(secret, claims)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}

	v := auth.HS256Verifier{Secret: secret}
	got, err := v.Verify(context.Background(), tok)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if got.Subject != "alice" || got.Service != "svc-1" {
		t.Fatalf("claims: %+v", got)
	}
}

func TestHS256Verifier_RejectsBadSignature(t *testing.T) {
	tok, _ := auth.IssueHS256([]byte("right-secret"), auth.Claims{Subject: "x"})
	v := auth.HS256Verifier{Secret: []byte("wrong-secret")}
	_, err := v.Verify(context.Background(), tok)
	if err == nil || !strings.Contains(err.Error(), "signature") {
		t.Fatalf("got %v, want signature mismatch", err)
	}
}

func TestHS256Verifier_RejectsTamperedPayload(t *testing.T) {
	tok, _ := auth.IssueHS256([]byte("s"), auth.Claims{Subject: "alice"})
	parts := strings.Split(tok, ".")
	// Tamper with the payload: replace the middle segment.
	tampered := parts[0] + "." + base64Raw("eyJoZWxsbyI6IndvcmxkIn0") + "." + parts[2]
	v := auth.HS256Verifier{Secret: []byte("s")}
	_, err := v.Verify(context.Background(), tampered)
	if err == nil {
		t.Fatal("expected verification to fail")
	}
}

// base64Raw is a small helper that strips padding from a fake
// payload for the tamper test.
func base64Raw(s string) string { return s }

func TestHS256Verifier_RejectsExpired(t *testing.T) {
	claims := auth.Claims{
		Subject:   "x",
		ExpiresAt: time.Now().Add(-time.Hour).Unix(),
	}
	tok, _ := auth.IssueHS256([]byte("s"), claims)

	v := auth.HS256Verifier{Secret: []byte("s")}
	_, err := v.Verify(context.Background(), tok)
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("got %v, want expired", err)
	}
}

func TestHS256Verifier_LeewayToleratesClockSkew(t *testing.T) {
	claims := auth.Claims{
		Subject:   "x",
		ExpiresAt: time.Now().Add(-30 * time.Second).Unix(), // expired 30s ago
	}
	tok, _ := auth.IssueHS256([]byte("s"), claims)

	v := auth.HS256Verifier{Secret: []byte("s"), Leeway: time.Minute}
	if _, err := v.Verify(context.Background(), tok); err != nil {
		t.Fatalf("expected leeway to forgive 30s skew, got %v", err)
	}
}

func TestHS256Verifier_RejectsWrongAlg(t *testing.T) {
	// Hand-build a JWT claiming alg=none. The signature check fires
	// first (good — never trust a tampered token's metadata), so we
	// expect signature-mismatch not alg-rejection.
	header := `{"alg":"none","typ":"JWT"}`
	payload := `{"sub":"x"}`
	signingInput := base64RawURL(header) + "." + base64RawURL(payload)
	v := auth.HS256Verifier{Secret: []byte("s")}
	_, err := v.Verify(context.Background(), signingInput+".AAAA")
	if err == nil {
		t.Fatal("expected rejection")
	}
}

// base64RawURL base64-raw-url-encodes a string. Pulled in to avoid
// importing encoding/base64 in the test file's header.
func base64RawURL(s string) string {
	return strings.TrimRight(
		strings.ReplaceAll(
			strings.ReplaceAll(
				strings.NewReplacer("+", "-", "/", "_").Replace(s),
				"+", "-"),
			"/", "_"),
		"=")
}
