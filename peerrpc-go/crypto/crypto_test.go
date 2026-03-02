package crypto_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/peerrpc/go/crypto"
)

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	alice, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("alice keygen: %v", err)
	}
	bob, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatalf("bob keygen: %v", err)
	}

	aliceKey, aliceNonce, err := crypto.DeriveKey(alice, bob.PublicKey(), "peerrpc-v1")
	if err != nil {
		t.Fatalf("alice derive: %v", err)
	}
	bobKey, bobNonce, err := crypto.DeriveKey(bob, alice.PublicKey(), "peerrpc-v1")
	if err != nil {
		t.Fatalf("bob derive: %v", err)
	}

	// Both sides derive the same key + nonce prefix.
	if !bytes.Equal(aliceKey, bobKey) {
		t.Fatal("ECDH key mismatch")
	}
	if aliceNonce != bobNonce {
		t.Fatal("nonce prefix mismatch")
	}

	aliceCh, err := crypto.NewEncryptedChannel(aliceKey, aliceNonce)
	if err != nil {
		t.Fatalf("alice channel: %v", err)
	}
	bobCh, err := crypto.NewEncryptedChannel(bobKey, bobNonce)
	if err != nil {
		t.Fatalf("bob channel: %v", err)
	}

	// Encrypt on alice's side, decrypt on bob's side.
	plaintext := []byte("hello, encrypted peerrpc!")
	wire, err := aliceCh.Encrypt(plaintext)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	decrypted, err := bobCh.Decrypt(wire)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if !bytes.Equal(decrypted, plaintext) {
		t.Fatalf("plaintext mismatch: got %q want %q", decrypted, plaintext)
	}
}

func TestEncryptDecrypt_MultipleFrames(t *testing.T) {
	key := bytes.Repeat([]byte{0xAB}, crypto.KeySize)
	var nonce [8]byte
	for i := range nonce {
		nonce[i] = byte(i)
	}

	ch, err := crypto.NewEncryptedChannel(key, nonce)
	if err != nil {
		t.Fatalf("channel: %v", err)
	}

	for i := 0; i < 100; i++ {
		msg := []byte(strings.Repeat("x", i*10))
		wire, err := ch.Encrypt(msg)
		if err != nil {
			t.Fatalf("encrypt %d: %v", i, err)
		}
		dec, err := ch.Decrypt(wire)
		if err != nil {
			t.Fatalf("decrypt %d: %v", i, err)
		}
		if !bytes.Equal(dec, msg) {
			t.Fatalf("frame %d mismatch", i)
		}
	}
}

func TestDecrypt_WrongKey(t *testing.T) {
	alice, _ := crypto.GenerateKeyPair()
	bob, _ := crypto.GenerateKeyPair()
	eve, _ := crypto.GenerateKeyPair()

	aliceKey, aliceNonce, _ := crypto.DeriveKey(alice, bob.PublicKey(), "peerrpc-v1")
	eveKey, eveNonce, _ := crypto.DeriveKey(eve, bob.PublicKey(), "peerrpc-v1")

	aliceCh, _ := crypto.NewEncryptedChannel(aliceKey, aliceNonce)
	eveCh, _ := crypto.NewEncryptedChannel(eveKey, eveNonce)

	wire, _ := aliceCh.Encrypt([]byte("secret"))
	_, err := eveCh.Decrypt(wire)
	if err == nil {
		t.Fatal("expected GCM decrypt failure with wrong key")
	}
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	key := bytes.Repeat([]byte{0xAB}, crypto.KeySize)
	var nonce [8]byte
	ch, _ := crypto.NewEncryptedChannel(key, nonce)

	wire, _ := ch.Encrypt([]byte("original"))

	// Flip one bit in the ciphertext.
	tampered := make([]byte, len(wire))
	copy(tampered, wire)
	tampered[len(tampered)-1] ^= 0x01

	_, err := ch.Decrypt(tampered)
	if err == nil {
		t.Fatal("expected GCM decrypt failure on tampered ciphertext")
	}
}

func TestNonceUniqueness(t *testing.T) {
	key := bytes.Repeat([]byte{0xAB}, crypto.KeySize)
	var nonce [8]byte
	ch, _ := crypto.NewEncryptedChannel(key, nonce)

	// Encrypt the same plaintext 100 times; each must produce a
	// different nonce tail (counter-based).
	seen := make(map[[4]byte]bool)
	for i := 0; i < 100; i++ {
		wire, _ := ch.Encrypt([]byte("same"))
		var tail [4]byte
		copy(tail[:], wire[4:8])
		if seen[tail] {
			t.Fatalf("nonce tail %x seen twice at iteration %d", tail, i)
		}
		seen[tail] = true
	}
}

func TestDeriveKey_DifferentInfoStrings(t *testing.T) {
	alice, _ := crypto.GenerateKeyPair()
	bob, _ := crypto.GenerateKeyPair()

	key1, _, _ := crypto.DeriveKey(alice, bob.PublicKey(), "info-A")
	key2, _, _ := crypto.DeriveKey(alice, bob.PublicKey(), "info-B")

	if bytes.Equal(key1, key2) {
		t.Fatal("different HKDF info strings should produce different keys")
	}
}

func TestMarshalPublicKey_RoundTrip(t *testing.T) {
	_, err := crypto.GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	priv, _ := crypto.GenerateKeyPair()
	pub := priv.PublicKey()

	data := crypto.MarshalPublicKey(pub)
	if len(data) != 32 {
		t.Fatalf("public key should be 32 bytes, got %d", len(data))
	}

	recovered, err := crypto.UnmarshalPublicKey(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !bytes.Equal(recovered.Bytes(), pub.Bytes()) {
		t.Fatal("public key round-trip mismatch")
	}
}
