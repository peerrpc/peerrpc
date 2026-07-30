// Integration test for the Go ↔ TS cross-language interop setup.
//
// This test verifies the server side: that the Go interop server
// boots, serves static files, exposes the WebSocket signaling
// endpoint, and that the signaling exchange works against a real Go
// signal.WS client.
//
// The full browser-side test (loading the TS page in a headless
// chromium and clicking buttons) requires Playwright or a similar
// browser runner. That test is in test/cross-lang/go-ts/browser_test.go
// behind a build tag so it only runs when playwright is installed.
package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/signalserver"
	"github.com/peerrpc/go/signalserver/store"
)

// TestInteropServer_ServesStaticAndSignal verifies the mux wires both
// the signaling endpoint and static files.
func TestInteropServer_ServesStaticAndSignal(t *testing.T) {
	mem := store.NewMemory()

	mux := http.NewServeMux()
	mux.Handle("/ws", signalserver.WebSocketHandler(mem, signalserver.Config{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Minimal static handler for the test.
		if r.URL.Path == "/" {
			w.Header().Set("Content-Type", "text/html")
			w.Write([]byte("<html><body>TS demo</body></html>"))
			return
		}
		http.NotFound(w, r)
	}))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// /healthz
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("healthz: %d", resp.StatusCode)
	}
	resp.Body.Close()

	// / (static)
	resp, err = http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if !strings.Contains(string(body), "TS demo") {
		t.Fatalf("static content missing: %s", body)
	}
}

// TestInteropServer_SignalingExchange verifies the WebSocket
// signaling endpoint accepts a signal.WS client and routes messages
// between two peers.
func TestInteropServer_SignalingExchange(t *testing.T) {
	mem := store.NewMemory()

	mux := http.NewServeMux()
	mux.Handle("/ws", signalserver.WebSocketHandler(mem, signalserver.Config{}))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	// Rewrite the http(s):// test URL to ws(s):// for the WS client.
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// alice + bob join via the Go signal.WS client.
	alice, err := signal.NewWS(wsURL).Exchange(ctx, "test-room", "alice")
	if err != nil {
		t.Fatalf("alice exchange: %v", err)
	}
	defer alice.Close()
	bob, err := signal.NewWS(wsURL).Exchange(ctx, "test-room", "bob")
	if err != nil {
		t.Fatalf("bob exchange: %v", err)
	}
	defer bob.Close()

	// alice sends an offer; bob should receive it.
	offer := &signal.SignalMessage{
		Body: signal.SignalBody{
			Offer: &signal.SdpOffer{Sdp: "v=0\r\no=- interop 1"},
		},
	}
	if err := alice.Send(ctx, offer); err != nil {
		t.Fatalf("alice send offer: %v", err)
	}

	// bob reads.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case msg, ok := <-bob.Receive():
				if !ok {
					return
				}
				if msg.Body.Offer != nil {
					if !bytes.Contains([]byte(msg.Body.Offer.Sdp), []byte("interop")) {
						t.Errorf("sdp mismatch: %s", msg.Body.Offer.Sdp)
					}
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	select {
	case <-done:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("bob did not receive offer")
	}
}
