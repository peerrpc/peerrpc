package peerrpc

import (
	"crypto/ed25519"
	"strings"
	"testing"
)

// TestDerivePeerID_StableAndPrefixed exercises the happy path: a
// well-formed key produces a peer_id that (a) starts with the
// "ed25519:" prefix and (b) round-trips back to the public key bytes
// when the prefix and base58 are stripped.
func TestDerivePeerID_StableAndPrefixed(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}

	id1, ok1 := derivePeerID(priv)
	if !ok1 {
		t.Fatal("derivePeerID returned !ok for a valid key")
	}
	id2, ok2 := derivePeerID(priv)
	if !ok2 {
		t.Fatal("derivePeerID returned !ok on second call for the same key")
	}
	if id1 != id2 {
		t.Fatalf("derivePeerID not stable: %q vs %q", id1, id2)
	}
	if !strings.HasPrefix(id1, "ed25519:") {
		t.Fatalf("peer_id %q missing ed25519: prefix", id1)
	}

	// Strip the prefix and base58-decode; the result must equal the
	// 32-byte public key.
	body := strings.TrimPrefix(id1, "ed25519:")
	decoded, err := decodeBase58(body)
	if err != nil {
		t.Fatalf("decodeBase58: %v", err)
	}
	if len(decoded) != ed25519.PublicKeySize {
		t.Fatalf("decoded length = %d, want %d", len(decoded), ed25519.PublicKeySize)
	}
	for i := range decoded {
		if decoded[i] != pub[i] {
			t.Fatalf("decoded[%d] = %x, want %x", i, decoded[i], pub[i])
		}
	}
}

// TestDerivePeerID_WrongLengthIgnored covers the documented fallback:
// any key length other than ed25519.PrivateKeySize (64) returns
// ("", false), so callers fall back to UUID generation rather than
// emitting a bogus peer_id.
func TestDerivePeerID_WrongLengthIgnored(t *testing.T) {
	cases := []struct {
		name string
		priv ed25519.PrivateKey
	}{
		{"empty", ed25519.PrivateKey{}},
		{"32 bytes (pubkey only)", make(ed25519.PrivateKey, ed25519.PublicKeySize)},
		{"63 bytes (one short)", make(ed25519.PrivateKey, ed25519.PrivateKeySize-1)},
		{"65 bytes (one long)", make(ed25519.PrivateKey, ed25519.PrivateKeySize+1)},
		{"128 bytes (over-sized)", make(ed25519.PrivateKey, 128)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, ok := derivePeerID(c.priv)
			if ok {
				t.Fatalf("derivePeerID(%d bytes) ok=true (id=%q), want false", len(c.priv), id)
			}
			if id != "" {
				t.Fatalf("derivePeerID(%d bytes) id=%q, want empty", len(c.priv), id)
			}
		})
	}
}

// TestDerivePeerID_DifferentKeysDifferentIDs guards against an
// accidental collision: two distinct keys must produce two distinct
// peer_ids. (Trivially true for a 1-to-1 function, but a future
// refactor that hashed the key would need this test to stay honest.)
func TestDerivePeerID_DifferentKeysDifferentIDs(t *testing.T) {
	_, priv1, _ := ed25519.GenerateKey(nil)
	_, priv2, _ := ed25519.GenerateKey(nil)

	id1, _ := derivePeerID(priv1)
	id2, _ := derivePeerID(priv2)
	if id1 == id2 {
		t.Fatalf("two distinct keys produced the same peer_id %q", id1)
	}
}
