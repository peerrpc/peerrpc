// Package crypto provides application-layer end-to-end encryption
// for PeerRPC frames.
//
// WebRTC's built-in DTLS secures the P2P transport path, but when
// traffic traverses a relay node the relay can see plaintext frame
// payloads because it performs byte-level forwarding. This package
// adds a transparent AES-256-GCM encryption layer above the
// transport so that only the two endpoints can decrypt payloads.
//
// Architecture:
//
//	rpc.Server / rpc.Client
//	      │  Frame / ResponseFrame (plaintext protobuf)
//	      ▼
//	crypto.Channel (AES-GCM encrypt/decrypt)
//	      │  encrypted length-prefixed bytes
//	      ▼
//	transport.Channel → DataChannel / relay
//
// Key agreement: the two peers exchange Curve25519 public keys via
// the signaling channel (out-of-band relative to the DataChannel).
// Each side derives a shared secret via ECDH, then expands it via
// HKDF-SHA256 into a 32-byte AES key + 12-byte nonce prefix. The
// remaining nonce bytes come from a per-frame counter to provide
// uniqueness without coordination.
//
// Wire format: every length-prefixed frame sent through the
// encrypted channel carries:
//
//	uint32 BE length | 12-byte nonce tail | AES-GCM ciphertext (+ 16-byte GCM tag)
//
// The length prefix covers the nonce tail + ciphertext. The
// decryptor reads the nonce tail, combines it with the prefix, and
// decrypts.
package crypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"golang.org/x/crypto/hkdf"
)

// NonceSize is the standard GCM nonce length.
const NonceSize = 12

// KeySize is the AES-256 key length.
const KeySize = 32

// EncryptedChannel wraps a reader/writer pair with AES-GCM
// encryption. It is the counterpart of transport.Channel for the
// encrypted path.
//
// The same key MUST be derived on both sides via DeriveKey. A
// mismatched key produces GCM authentication failures on every
// frame.
type EncryptedChannel struct {
	gcm    cipher.AEAD
	prefix [8]byte // fixed nonce prefix (first 8 bytes)
	counter atomic.Uint64
}

// NewEncryptedChannel constructs an encrypted channel from a raw
// 32-byte AES key and an 8-byte nonce prefix. Both values MUST
// match on both endpoints.
func NewEncryptedChannel(key []byte, noncePrefix [8]byte) (*EncryptedChannel, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("crypto: key must be %d bytes, got %d", KeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("crypto: AES init: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("crypto: GCM init: %w", err)
	}
	return &EncryptedChannel{
		gcm:    gcm,
		prefix: noncePrefix,
	}, nil
}

// Encrypt encrypts plaintext and returns a length-prefixed wire
// payload ready to send through a transport.Channel.SendRaw.
//
// Wire format:
//
//	uint32 BE (nonce_tail_len + ciphertext_len)
//	4-byte nonce tail    (counter portion)
//	ciphertext           (plaintext_len + 16-byte GCM tag)
//
// The nonce tail is the last 4 bytes of the 12-byte GCM nonce;
// the first 8 bytes are the fixed prefix.
func (ec *EncryptedChannel) Encrypt(plaintext []byte) ([]byte, error) {
	// Build the full nonce.
	counter := ec.counter.Add(1)
	var nonce [NonceSize]byte
	copy(nonce[:8], ec.prefix[:])
	binary.BigEndian.PutUint32(nonce[8:], uint32(counter))

	ciphertext := ec.gcm.Seal(nil, nonce[:], plaintext, nil)

	// Wire: 4-byte length + 4-byte nonce tail + ciphertext.
	nonceTail := nonce[8:]
	totalLen := len(nonceTail) + len(ciphertext)
	out := make([]byte, 4+totalLen)
	binary.BigEndian.PutUint32(out[:4], uint32(totalLen))
	copy(out[4:8], nonceTail)
	copy(out[8:], ciphertext)
	return out, nil
}

// Decrypt reads a length-prefixed encrypted payload and returns the
// plaintext. The input MUST be a complete frame as produced by
// Encrypt (4-byte length prefix + nonce tail + ciphertext).
func (ec *EncryptedChannel) Decrypt(wire []byte) ([]byte, error) {
	if len(wire) < 4 {
		return nil, errors.New("crypto: wire too short for length prefix")
	}
	totalLen := binary.BigEndian.Uint32(wire[:4])
	if len(wire) < 4+int(totalLen) {
		return nil, errors.New("crypto: wire truncated")
	}

	body := wire[4 : 4+totalLen]
	if len(body) < 4 {
		return nil, errors.New("crypto: body too short for nonce tail")
	}

	nonceTail := body[:4]
	ciphertext := body[4:]

	var nonce [NonceSize]byte
	copy(nonce[:8], ec.prefix[:])
	copy(nonce[8:], nonceTail)

	plaintext, err := ec.gcm.Open(nil, nonce[:], ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("crypto: GCM decrypt failed: %w", err)
	}
	return plaintext, nil
}

// ─── Key derivation ─────────────────────────────────────────

// DeriveKey performs ECDH with the peer's public key and the local
// private key, then expands the shared secret via HKDF-SHA256 into
// a 32-byte AES key and an 8-byte nonce prefix.
//
// Both sides MUST call this with their own private key and the
// other's public key; the HKDF info string must match.
func DeriveKey(
	priv *ecdh.PrivateKey,
	peerPub *ecdh.PublicKey,
	info string,
) (key []byte, noncePrefix [8]byte, err error) {
	shared, err := priv.ECDH(peerPub)
	if err != nil {
		return nil, [8]byte{}, fmt.Errorf("crypto: ECDH: %w", err)
	}

	// HKDF expand: 32 bytes key + 8 bytes nonce prefix = 40 bytes.
	hk := hkdf.New(sha256.New, shared, nil, []byte(info))
	out := make([]byte, KeySize+8)
	if _, err := io.ReadFull(hk, out); err != nil {
		return nil, [8]byte{}, fmt.Errorf("crypto: HKDF: %w", err)
	}
	copy(noncePrefix[:], out[KeySize:])
	return out[:KeySize], noncePrefix, nil
}

// GenerateKeyPair generates a fresh Curve25519 ECDH key pair for
// key exchange. The public key is shared via signaling; the private
// key never leaves the process.
func GenerateKeyPair() (*ecdh.PrivateKey, error) {
	return ecdh.X25519().GenerateKey(rand.Reader)
}

// MarshalPublicKey serializes a Curve25519 public key to raw bytes
// for signaling transport.
func MarshalPublicKey(pub *ecdh.PublicKey) []byte {
	return pub.Bytes()
}

// UnmarshalPublicKey deserializes a Curve25519 public key from raw
// bytes received via signaling.
func UnmarshalPublicKey(data []byte) (*ecdh.PublicKey, error) {
	return ecdh.X25519().NewPublicKey(data)
}
