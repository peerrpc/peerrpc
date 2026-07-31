package peerrpc

import (
	"errors"
	"math/big"
)

// base58Alphabet is the standard Bitcoin base58 alphabet. Picked over
// RFC 4648 base64 because base58 avoids visually ambiguous characters
// (0/O, I/l) — important when peer_id values may be copy-pasted
// between chat windows and shell logs.
const base58Alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

var (
	// base58Index maps each alphabet byte back to its digit (0..57),
	// with -1 for bytes outside the alphabet. Built once at init.
	base58Index [128]int8
	// base58BigZero is a cached *big.Int with value 0, used by decode.
	base58BigZero = big.NewInt(0)
	// base58BigRadix is a cached *big.Int with value 58, used by decode.
	base58BigRadix = big.NewInt(58)
)

// init populates base58Index. We do not validate alphabet membership
// during init; entries default to 0 which is harmless (decode treats
// the value as an invalid digit anyway because we range-check first).
func init() {
	for i := range base58Index {
		base58Index[i] = -1
	}
	for i := 0; i < len(base58Alphabet); i++ {
		base58Index[base58Alphabet[i]] = int8(i)
	}
}

// encodeBase58 encodes b into a base58 string using the Bitcoin
// alphabet. The encoding follows the standard "treat as big-endian
// integer, divide by 58, prepend remainder" recipe, with the leading
// zero-byte → leading '1' rule so the result round-trips.
//
// An empty input returns the empty string. This differs from some
// libraries which return "1" for an empty input; we treat zero-length
// as a degenerate caller error, not as a zero value, so the empty
// case is unambiguous to callers.
func encodeBase58(b []byte) string {
	if len(b) == 0 {
		return ""
	}

	// Count leading zero bytes; they each become a leading '1' in the
	// output (the alphabet entry whose value is 0).
	var zeros int
	for zeros = 0; zeros < len(b) && b[zeros] == 0; zeros++ {
	}

	// Convert the remaining bytes (treated as a big-endian unsigned
	// integer) into base58. We accumulate into a slice prepended with
	// enough room for the worst-case encoded length. The convention
	// here is "encoded[0] is the highest-order digit" — we fill from
	// the back and skip leading zero digits at the end.
	size := (len(b)-zeros)*138/100 + 1
	encoded := make([]byte, size)

	// Iterate over the non-zero tail, multiplying the running
	// big-endian number by 256 and adding the input byte, then
	// converting to base 58 by repeated division. We must always
	// walk the full buffer (no early break) because the carry out of
	// position j feeds position j-1; a premature break leaves the
	// most-significant digit at 0, which the leading-zero skip
	// would then erroneously drop, shortening the output by one.
	for _, byt := range b[zeros:] {
		carry := int(byt)
		for j := size - 1; j >= 0; j-- {
			carry += 256 * int(encoded[j])
			encoded[j] = byte(carry % 58)
			carry /= 58
		}
	}

	// Skip any leading zero digits in the encoded buffer; they will be
	// re-emitted as leading '1' characters (one per leading 0x00 byte
	// in the input) to preserve round-trip semantics.
	leading := 0
	for leading = 0; leading < size && encoded[leading] == 0; leading++ {
	}
	out := make([]byte, zeros+(size-leading))
	for i := 0; i < zeros; i++ {
		out[i] = base58Alphabet[0]
	}
	for i, j := zeros, leading; j < size; i, j = i+1, j+1 {
		out[i] = base58Alphabet[encoded[j]]
	}
	return string(out)
}

// decodeBase58 decodes a base58 string into bytes. The result
// round-trips with encodeBase58: decode(encode(b)) == b for any
// b. Returns an error for non-alphabet characters.
//
// The empty string decodes to a nil slice (also round-trips with
// encodeBase58).
func decodeBase58(s string) ([]byte, error) {
	if len(s) == 0 {
		return nil, nil
	}

	// Count leading '1' characters; they each decode to a leading 0
	// byte in the result.
	var zeros int
	for zeros = 0; zeros < len(s) && s[zeros] == base58Alphabet[0]; zeros++ {
	}

	// Build the decoded integer in *big.Int by treating the digits as
	// a base-58 number, then serialize to big-endian bytes.
	acc := new(big.Int)
	for i := zeros; i < len(s); i++ {
		c := s[i]
		if c >= 128 || base58Index[c] < 0 {
			return nil, errors.New("peerrpc: invalid base58 character")
		}
		acc.Mul(acc, base58BigRadix)
		acc.Add(acc, big.NewInt(int64(base58Index[c])))
	}

	decoded := acc.Bytes()
	out := make([]byte, zeros+len(decoded))
	copy(out[zeros:], decoded)
	return out, nil
}
