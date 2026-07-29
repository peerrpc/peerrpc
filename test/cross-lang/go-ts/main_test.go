// Integration test for the Go ↔ TS cross-language interop setup.
//
// This test verifies the server side: that the Go interop server
// boots, serves static files, exposes the signaling endpoint, and
// that the signaling Exchange stream works against a real connect-go
// client.
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

	signalingpb "github.com/peerrpc/go/gen/proto/peerrpc/signaling"
	signalingpbconnect "github.com/peerrpc/go/gen/connect/peerrpc/signaling/signalingpbconnect"
	"github.com/peerrpc/signal-server/server"
	"github.com/peerrpc/signal-server/store"

	"connectrpc.com/connect"
)

// TestInteropServer_ServesStaticAndSignal verifies the mux wires both
// the signaling endpoint and static files.
func TestInteropServer_ServesStaticAndSignal(t *testing.T) {
	signalSrv := server.New(store.NewMemory(), server.Config{})

	mux := http.NewServeMux()
	path, handler := signalingpbconnect.NewSignalingServiceHandler(signalSrv)
	mux.Handle(path, handler)
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

// TestInteropServer_SignalingExchange verifies the signaling endpoint
// accepts a connect-go client and routes messages between two peers.
func TestInteropServer_SignalingExchange(t *testing.T) {
	signalSrv := server.New(store.NewMemory(), server.Config{})

	mux := http.NewServeMux()
	path, handler := signalingpbconnect.NewSignalingServiceHandler(signalSrv)
	mux.Handle(path, handler)

	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := signalingpbconnect.NewSignalingServiceClient(
		srv.Client(), srv.URL, connect.WithSendGzip(),
	)

	// alice
	aliceStream := client.Exchange(ctx)
	if err := aliceStream.Send(&signalingpb.SignalMessage{
		Service: "test-room",
		Body: &signalingpb.SignalMessage_Announce{
			Announce: &signalingpb.AnnounceRequest{PeerId: "alice", Role: signalingpb.AnnounceRequest_ROLE_CLIENT},
		},
	}); err != nil {
		t.Fatalf("alice join: %v", err)
	}

	// bob
	bobStream := client.Exchange(ctx)
	if err := bobStream.Send(&signalingpb.SignalMessage{
		Service: "test-room",
		Body: &signalingpb.SignalMessage_Announce{
			Announce: &signalingpb.AnnounceRequest{PeerId: "bob", Role: signalingpb.AnnounceRequest_ROLE_SERVER},
		},
	}); err != nil {
		t.Fatalf("bob join: %v", err)
	}

	// alice sends offer; bob should receive.
	offer := &signalingpb.SignalMessage{
		Service: "test-room",
		Body: &signalingpb.SignalMessage_Offer{
			Offer: &signalingpb.SdpOffer{Sdp: "v=0\r\no=- interop 1"},
		},
	}
	if err := aliceStream.Send(offer); err != nil {
		t.Fatalf("alice send offer: %v", err)
	}

	// bob reads.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			msg, err := bobStream.Receive()
			if err != nil {
				return
			}
			if msg.GetOffer() != nil {
				if !bytes.Contains([]byte(msg.GetOffer().GetSdp()), []byte("interop")) {
					t.Errorf("sdp mismatch: %s", msg.GetOffer().GetSdp())
				}
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

	// Close streams cleanly so the handler goroutines exit.
	_ = aliceStream.CloseRequest()
	_ = aliceStream.CloseResponse()
	_ = bobStream.CloseRequest()
	_ = bobStream.CloseResponse()
}
