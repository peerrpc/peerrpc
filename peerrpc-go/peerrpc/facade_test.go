package peerrpc_test

import (
	"context"
	"crypto/ed25519"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/peerrpc/go/peerrpc"
	"github.com/peerrpc/go/rpc"
)

// registerEcho installs a tiny unary + server-streaming echo service
// onto srv. Mirrors what examples/local-echo-go does by hand.
func registerEcho(srv *rpc.Server) {
	srv.RegisterService(rpc.ServiceDesc{
		ServiceName: "echo.Echo",
		Methods: []rpc.MethodDesc{
			{
				Method: "Unary",
				Kind:   rpc.MethodKindUnary,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					req, err := s.Recv()
					if err != nil {
						return rpc.Err(13, err)
					}
					if err := s.Send(append([]byte("echo: "), req...)); err != nil {
						return rpc.Err(13, err)
					}
					return rpc.OK()
				},
			},
			{
				Method: "Stream",
				Kind:   rpc.MethodKindServerStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					req, err := s.Recv()
					if err != nil && !errors.Is(err, io.EOF) {
						return rpc.Err(13, err)
					}
					for i := 1; i <= 3; i++ {
						if err := s.Send([]byte("chunk")); err != nil {
							return rpc.Err(13, err)
						}
					}
					_ = req
					return rpc.OK()
				},
			},
		},
	})
}

// newEchoServer is a Listener.Serve factory that registers echo.
func newEchoServer() *rpc.Server {
	srv := rpc.NewServer()
	registerEcho(srv)
	return srv
}

// TestFacade_DialUnary_Local runs the full Dial → InvokeUnary →
// Close dance over SchemeLocal with both sides using the facade.
func TestFacade_DialUnary_Local(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ln, err := peerrpc.Listen(ctx, "peerrpc+local:///echo.Echo")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	// Server: accept one connection, run echo on it.
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- ln.Serve(ctx, newEchoServer)
	}()

	// Client: dial, call unary, close.
	conn, err := peerrpc.Dial(ctx, "peerrpc+local:///echo.Echo")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	resp, status := conn.InvokeUnary(ctx, "/echo.Echo/Unary", []byte("hello"), nil)
	if status != nil && status.Code != 0 {
		t.Fatalf("InvokeUnary status: %+v", status)
	}
	if string(resp) != "echo: hello" {
		t.Fatalf("got %q, want %q", string(resp), "echo: hello")
	}

	// Sanity: PeerID was auto-generated (non-empty, not the v1
	// "offerer"/"answerer" magic strings).
	if conn.PeerID() == "" {
		t.Fatal("PeerID is empty")
	}
}

// TestFacade_DialUnary_TargetStyle exercises the typed-Target entry
// point (DialTarget) end-to-end.
func TestFacade_DialUnary_TargetStyle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ln, err := peerrpc.ListenTarget(ctx, peerrpc.Target{
		Scheme:  peerrpc.SchemeLocal,
		Service: "echo.Echo",
	})
	if err != nil {
		t.Fatalf("ListenTarget: %v", err)
	}
	defer ln.Close()

	go func() { _ = ln.Serve(ctx, newEchoServer) }()

	conn, err := peerrpc.DialTarget(ctx, peerrpc.Target{
		Scheme:  peerrpc.SchemeLocal,
		Service: "echo.Echo",
	})
	if err != nil {
		t.Fatalf("DialTarget: %v", err)
	}
	defer conn.Close()

	resp, status := conn.InvokeUnary(ctx, "/echo.Echo/Unary", []byte("world"), nil)
	if status != nil && status.Code != 0 {
		t.Fatalf("status: %+v", status)
	}
	if string(resp) != "echo: world" {
		t.Fatalf("got %q", string(resp))
	}
}

// TestFacade_DialUnary_BuilderStyle exercises the fluent Builder.
func TestFacade_DialUnary_BuilderStyle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ln, err := peerrpc.ListenContext(ctx).
		Service("echo.Echo").
		Over(peerrpc.SchemeLocal).
		Listen()
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	go func() { _ = ln.Serve(ctx, newEchoServer) }()

	conn, err := peerrpc.DialContext(ctx).
		Service("echo.Echo").
		Over(peerrpc.SchemeLocal).
		Connect()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	if _, status := conn.InvokeUnary(ctx, "/echo.Echo/Unary", []byte("builder"), nil); status != nil && status.Code != 0 {
		t.Fatalf("status: %+v", status)
	}
}

// TestFacade_ServerStreaming exercises a server-streaming RPC.
func TestFacade_ServerStreaming(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	ln, err := peerrpc.Listen(ctx, "peerrpc+local:///echo.Echo")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	go func() { _ = ln.Serve(ctx, newEchoServer) }()

	conn, err := peerrpc.Dial(ctx, "peerrpc+local:///echo.Echo")
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	stream, status := conn.InvokeServerStreaming(ctx, "/echo.Echo/Stream", []byte("ping"), nil)
	if status != nil && status.Code != 0 {
		t.Fatalf("status: %+v", status)
	}

	count := 0
	for {
		_, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		count++
	}
	if count != 3 {
		t.Fatalf("got %d chunks, want 3", count)
	}
}

// TestFacade_MultiClient accepts multiple clients on one Listener
// sequentially. The store's broadcast-to-others semantics make
// simultaneous joins within the same service race on the WebRTC
// handshake (each client's offer gets broadcast to every other
// in-flight peer, including unrelated ones). Concurrent clients
// either need distinct services or v1.1's per-pair room semantics.
// Sequential here exercises Listener.Accept's loop and per-conn
// rpc.Server isolation.
func TestFacade_MultiClient(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ln, err := peerrpc.Listen(ctx, "peerrpc+local:///echo.Echo")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	go func() { _ = ln.Serve(ctx, newEchoServer) }()

	const N = 3
	for i := 0; i < N; i++ {
		conn, err := peerrpc.Dial(ctx, "peerrpc+local:///echo.Echo")
		if err != nil {
			t.Fatalf("client %d Dial: %v", i, err)
		}
		resp, status := conn.InvokeUnary(ctx, "/echo.Echo/Unary", []byte("hi"), nil)
		if status != nil && status.Code != 0 {
			t.Errorf("client %d status: %+v", i, status)
			_ = conn.Close()
			continue
		}
		if string(resp) != "echo: hi" {
			t.Errorf("client %d got %q", i, string(resp))
		}
		_ = conn.Close()
		// Give the server-side Serve loop a beat to recycle before
		// the next client connects.
		time.Sleep(100 * time.Millisecond)
	}
}

// TestFacade_DialUnary_WithIdentity is the end-to-end regression
// test for the WithIdentity-silently-dropped bug. With WithIdentity
// supplied, Conn.PeerID() must start with "ed25519:" and be exactly
// the canonical derivation for that key (prefix + 44-char base58 of
// the 32-byte public key). Without WithIdentity, the resolver's
// UUID fallback must be used and PeerID() must not be prefixed.
//
// The "two Dials with the same key produce the same peer_id"
// property is covered by the unit test TestDerivePeerID_StableAndPrefixed
// in derive_test.go; this end-to-end test focuses on the contract
// surface, because the local resolver (one peer_id per service)
// rejects simultaneous duplicates.
func TestFacade_DialUnary_WithIdentity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	_, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	ln, err := peerrpc.Listen(ctx, "peerrpc+local:///echo.Echo")
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer ln.Close()

	go func() { _ = ln.Serve(ctx, newEchoServer) }()

	// With-identity path: peer_id is derived from the public key.
	withID, err := peerrpc.Dial(ctx, "peerrpc+local:///echo.Echo",
		peerrpc.WithIdentity(priv))
	if err != nil {
		t.Fatalf("Dial WithIdentity: %v", err)
	}
	defer withID.Close()

	// Format: "ed25519:" + 44-char base58 of the 32-byte public key.
	// 32 bytes encode to at most ceil(32*log(256)/log(58)) = 44 chars
	// in the Bitcoin alphabet; for a random key no leading zeros
	// survive so the output is exactly 44 chars.
	if !strings.HasPrefix(withID.PeerID(), "ed25519:") {
		t.Errorf("WithIdentity PeerID %q missing ed25519: prefix", withID.PeerID())
	}
	if got, want := len(withID.PeerID()), len("ed25519:")+44; got != want {
		t.Errorf("WithIdentity PeerID length = %d, want %d (got %q)",
			got, want, withID.PeerID())
	}

	// The body of the peer_id must be valid Bitcoin base58 (0/O/I/l
	// excluded). A future refactor that hashed or truncated the key
	// would change the alphabet set; this check pins it.
	body := strings.TrimPrefix(withID.PeerID(), "ed25519:")
	for i, r := range body {
		if !strings.ContainsRune(
			"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz",
			r) {
			t.Errorf("body[%d] = %q is not a base58 alphabet char", i, r)
			break
		}
	}

	// A different key must produce a different peer_id. (The local
	// resolver allows distinct peer_ids in the same service, so this
	// is just a sanity check that the derivation actually consults
	// the key.)
	_, otherPriv, _ := ed25519.GenerateKey(nil)
	other, err := peerrpc.Dial(ctx, "peerrpc+local:///echo.Echo",
		peerrpc.WithIdentity(otherPriv))
	if err != nil {
		t.Fatalf("Dial WithIdentity (other key): %v", err)
	}
	defer other.Close()
	if other.PeerID() == withID.PeerID() {
		t.Errorf("two distinct keys produced the same peer_id %q", withID.PeerID())
	}
	if !strings.HasPrefix(other.PeerID(), "ed25519:") {
		t.Errorf("other-key PeerID %q missing ed25519: prefix", other.PeerID())
	}

	// No-identity path: resolver's UUID fallback, no "ed25519:" prefix.
	noID, err := peerrpc.Dial(ctx, "peerrpc+local:///echo.Echo")
	if err != nil {
		t.Fatalf("Dial (no identity): %v", err)
	}
	defer noID.Close()
	if noID.PeerID() == "" {
		t.Error("no-identity PeerID is empty")
	}
	if strings.HasPrefix(noID.PeerID(), "ed25519:") {
		t.Errorf("no-identity PeerID %q unexpectedly prefixed", noID.PeerID())
	}
}
