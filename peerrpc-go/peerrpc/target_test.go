package peerrpc_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/peerrpc/go/peerrpc"
)

func TestParseTarget_Basic(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want peerrpc.Target
	}{
		{
			name: "connect with port and query",
			in:   "peerrpc+connect://signal.example.com:443/echo.Echo?as=client&peer=alice&token=jwt",
			want: peerrpc.Target{
				Scheme:  peerrpc.SchemeConnect,
				Signal:  "signal.example.com:443",
				Service: "echo.Echo",
				Role:    peerrpc.RoleHintClient,
				PeerID:  "alice",
				Token:   "jwt",
			},
		},
		{
			name: "local empty authority",
			in:   "peerrpc+local:///echo.Echo",
			want: peerrpc.Target{
				Scheme:  peerrpc.SchemeLocal,
				Signal:  "",
				Service: "echo.Echo",
			},
		},
		{
			name: "ws",
			in:   "peerrpc+ws://signal.example.com/echo.Echo",
			want: peerrpc.Target{
				Scheme:  peerrpc.SchemeWS,
				Signal:  "signal.example.com",
				Service: "echo.Echo",
			},
		},
		{
			name: "bare host no port",
			in:   "peerrpc+connect://signal.example.com/echo.Echo",
			want: peerrpc.Target{
				Scheme:  peerrpc.SchemeConnect,
				Signal:  "signal.example.com",
				Service: "echo.Echo",
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := peerrpc.ParseTarget(c.in)
			if err != nil {
				t.Fatalf("ParseTarget: %v", err)
			}
			if got != c.want {
				t.Errorf("got  %+v\nwant %+v", got, c.want)
			}
		})
	}
}

func TestParseTarget_Errors(t *testing.T) {
	cases := []struct {
		name string
		in   string
	}{
		{"missing prefix", "connect://signal.example.com/echo.Echo"},
		{"missing service", "peerrpc+connect://signal.example.com"},
		{"empty service", "peerrpc+connect://signal.example.com/"},
		{"non-local without authority", "peerrpc+connect:///echo.Echo"},
		{"garbage url", "peerrpc+connect://%%illegal"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := peerrpc.ParseTarget(c.in); err == nil {
				t.Errorf("expected error for %q, got nil", c.in)
			}
		})
	}
}

func TestTarget_StringRoundTrip(t *testing.T) {
	original := peerrpc.Target{
		Scheme:  peerrpc.SchemeConnect,
		Signal:  "signal.example.com:443",
		Service: "echo.Echo",
		Role:    peerrpc.RoleHintClient,
		PeerID:  "alice",
		Token:   "tok",
	}
	s := original.String()
	parsed, err := peerrpc.ParseTarget(s)
	if err != nil {
		t.Fatalf("ParseTarget(round-tripped %q): %v", s, err)
	}
	// url.Values.Encode sorts keys, so the input and round-trip
	// strings may differ in query order. Compare field-wise.
	if parsed != original {
		t.Errorf("round trip mismatch:\n got  %+v\n want %+v", parsed, original)
	}
}

func TestRegisterResolver_UnsupportedScheme(t *testing.T) {
	// Schemes that have no built-in resolver must fail with
	// ErrUnsupportedScheme rather than panicking.
	_, err := peerrpc.DialTarget(context.Background(), peerrpc.Target{
		Scheme:  peerrpc.SchemeRelay,
		Signal:  "relay.example.com",
		Service: "echo.Echo",
	})
	if !errors.Is(err, peerrpc.ErrUnsupportedScheme) {
		t.Fatalf("got %v, want ErrUnsupportedScheme", err)
	}
}

// RegisterResolver override is a public extension point; verify a
// custom scheme dispatches.
func TestRegisterResolver_CustomScheme(t *testing.T) {
	custom := peerrpc.Scheme("custom")
	peerrpc.RegisterResolver(custom, func() (peerrpc.Resolver, error) {
		return nil, errors.New("factory-intentional-failure")
	})
	_, err := peerrpc.DialTarget(context.Background(), peerrpc.Target{
		Scheme:  custom,
		Signal:  "x",
		Service: "y",
	})
	if err == nil || !strings.Contains(err.Error(), "factory-intentional-failure") {
		t.Fatalf("expected factory error to surface, got %v", err)
	}
}
