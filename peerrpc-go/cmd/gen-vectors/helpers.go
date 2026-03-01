// Helpers for the golden vector generator. Kept in a separate file so
// main.go stays a clean readable table of vectors.
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"

	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
)

// repeatByte returns a byte slice of length n filled with v. Used for
// payload padding so vectors are deterministic.
func repeatByte(v byte, n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = v
	}
	return b
}

// statusRPC is a tiny helper to build a google.rpc.Status with the
// canonical numeric gRPC code. We construct it as the well-known proto
// type so its wire encoding matches connect-go / grpc-go byte-for-byte.
func statusRPC(code int32, msg string, _ []byte) *rpcstatus.Status {
	return &rpcstatus.Status{Code: code, Message: msg}
}

func equalBytes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sha256hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// repoRoot returns the absolute path to relPath resolved against the
// repository root. The generator is location-independent: it walks up
// from the source file to find the buf.yaml marker.
func repoRoot(relPath string) (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errResolve
	}
	dir := filepath.Dir(file)
	for i := 0; i < 12; i++ {
		if _, err := os.Stat(filepath.Join(dir, "buf.yaml")); err == nil {
			return filepath.Join(dir, relPath), nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", errResolve
}

var errResolve = os.ErrNotExist
