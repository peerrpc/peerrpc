package transport_test

import (
	"context"
	"testing"

	"github.com/peerrpc/go/transport"
)

func TestReassembler_SingleChunk(t *testing.T) {
	r := transport.NewReassembler()
	full := []byte("hello, peerrpc")
	out, ok := r.Reassemble(1, len(full), 0, full)
	if !ok {
		t.Fatal("expected ok=true for single-chunk message")
	}
	if string(out) != string(full) {
		t.Fatalf("got %q want %q", out, full)
	}
}

func TestReassembler_MultiChunk(t *testing.T) {
	r := transport.NewReassembler()
	full := make([]byte, 1024)
	for i := range full {
		full[i] = byte(i % 256)
	}
	// Split into 3 chunks of 256 + 1 final chunk of 256 (4 total).
	var final []byte
	var finalOK bool
	for offset := 0; offset < len(full); offset += 256 {
		var ok bool
		final, ok = r.Reassemble(7, len(full), offset, full[offset:offset+256])
		finalOK = ok
	}
	if !finalOK {
		t.Fatal("final chunk did not complete")
	}
	if len(final) != len(full) {
		t.Fatalf("got len=%d want=%d", len(final), len(full))
	}
	for i := range final {
		if final[i] != full[i] {
			t.Fatalf("byte %d mismatch", i)
		}
	}
}

func TestReassembler_DropsStateOnCompletion(t *testing.T) {
	r := transport.NewReassembler()
	_, _ = r.Reassemble(1, 5, 0, []byte{0, 1, 2, 3, 4})
	// After completion a fresh Reassemble should treat this as a new
	// sequence state.
	out, ok := r.Reassemble(1, 5, 0, []byte{0, 1, 2, 3, 4})
	if !ok {
		t.Fatal("expected ok=true after reassemble")
	}
	if string(out) != "\x00\x01\x02\x03\x04" {
		t.Fatalf("got %v", out)
	}
}

// Unused but referenced; silences the linter.
var _ = context.Background
