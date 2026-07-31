package peerrpc

import (
	"bytes"
	"testing"
)

// Test vectors cross-checked against a math/big reference encoder
// (the Bitcoin base58 spec is a pure big-endian integer→base-58
// conversion, so the reference is the spec itself). Keep them
// stable: this is the public contract for peer_id derivation.
var base58Vectors = []struct {
	name    string
	raw     []byte
	encoded string
}{
	// Standard edge cases.
	{"empty", []byte{}, ""},
	{"single zero", []byte{0x00}, "1"},
	{"two zeros", []byte{0x00, 0x00}, "11"},
	// hello world — well-known reference vector.
	{"hello world", []byte("hello world"), "StV1DL6CwTryKyV"},
	// 32-byte shape of an ed25519 public key. 0x00-led version
	// exercises the "leading zeros → leading 1s" rule on the actual
	// size used by derivePeerID.
	{"ed25519-shape: 32x0x01", bytes.Repeat([]byte{0x01}, 32),
		"4vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi"},
	{"ed25519-shape: 32x0xFF", bytes.Repeat([]byte{0xFF}, 32),
		"JEKNVnkbo3jma5nREBBJCDoXFVeKkD56V3xKrvRmWxFG"},
	// Mixed: 4 leading zero bytes + 32 data bytes — the 4 leading
	// zeros must surface as 4 leading '1' digits, then the data
	// bytes encode normally.
	{"4 leading zeros + 32x0x01",
		append(bytes.Repeat([]byte{0x00}, 4), bytes.Repeat([]byte{0x01}, 32)...),
		"11114vJ9JU1bJJE96FWSJKvHsmmFADCg4gpZQff4P3bkLKi"},
}

func TestEncodeBase58(t *testing.T) {
	for _, v := range base58Vectors {
		t.Run(v.name, func(t *testing.T) {
			got := encodeBase58(v.raw)
			if got != v.encoded {
				t.Fatalf("encodeBase58(%x) = %q, want %q", v.raw, got, v.encoded)
			}
		})
	}
}

func TestDecodeBase58(t *testing.T) {
	for _, v := range base58Vectors {
		// Skip empty for decode: the empty-string contract is "nil in,
		// nil out" but the round-trip uses bytes.Equal(nil, []byte{})
		// which is true in Go, so it works either way. Include for
		// coverage.
		t.Run(v.name, func(t *testing.T) {
			got, err := decodeBase58(v.encoded)
			if err != nil {
				t.Fatalf("decodeBase58(%q) error: %v", v.encoded, err)
			}
			if !bytes.Equal(got, v.raw) {
				t.Fatalf("decodeBase58(%q) = %x, want %x", v.encoded, got, v.raw)
			}
		})
	}
}

func TestDecodeBase58_Invalid(t *testing.T) {
	// "0" is not in the Bitcoin base58 alphabet (only "1" represents
	// digit 0). "O" and "I" / "l" are also excluded.
	for _, s := range []string{"0", "O", "I", "l", "abc!def"} {
		if _, err := decodeBase58(s); err == nil {
			t.Errorf("decodeBase58(%q) succeeded, want error", s)
		}
	}
}

func TestBase58RoundTrip_Random(t *testing.T) {
	// Pseudo-random data: not crypto, just exercising the digit
	// distribution. Deterministic seed so failures are reproducible.
	seed := uint64(0x1234567890abcdef)
	for i := 0; i < 64; i++ {
		// xorshift64 — fast, deterministic, no extra deps.
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		buf := make([]byte, 64)
		for j := range buf {
			seed ^= seed << 13
			seed ^= seed >> 7
			seed ^= seed << 17
			buf[j] = byte(seed)
		}
		encoded := encodeBase58(buf)
		decoded, err := decodeBase58(encoded)
		if err != nil {
			t.Fatalf("decode error on round-trip: %v", err)
		}
		if !bytes.Equal(decoded, buf) {
			t.Fatalf("round-trip mismatch for seed=%d", i)
		}
	}
}
