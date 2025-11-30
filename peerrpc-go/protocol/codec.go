// Package protocol defines the PeerRPC wire codec.
//
// On the DataChannel frames are written as a length-prefixed series of
// bytes so that the receiver can split a continuous byte stream back
// into discrete frames:
//
//	+---------------------+----------------------+
//	| uint32 BE length    | protobuf Frame bytes |
//	+---------------------+----------------------+
//
// The same prefix format is used for both directions (client->server
// Frame and server->client ResponseFrame).
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// MaxFrameSize is the upper bound on a single length-prefixed wire
// frame. The DataChannel itself has a ~256KB SCTP ceiling, so any
// logical payload above the inline/message thresholds is fragmented by
// the transport layer into Chunk payloads before reaching the wire.
const MaxFrameSize = 256 * 1024

// ErrOversizedFrame is returned when the announced length prefix
// exceeds MaxFrameSize. The reader treats this as fatal: a peer that
// sends such a prefix is either buggy or hostile.
var ErrOversizedFrame = errors.New("peerrpc/protocol: frame exceeds MaxFrameSize")

// WriteFrame writes a single length-prefixed protobuf frame to w.
// The payload must already be a marshaled Frame or ResponseFrame.
func WriteFrame(w io.Writer, payload []byte) error {
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := w.Write(header[:]); err != nil {
		return fmt.Errorf("peerrpc/protocol: write length: %w", err)
	}
	if _, err := w.Write(payload); err != nil {
		return fmt.Errorf("peerrpc/protocol: write payload: %w", err)
	}
	return nil
}

// ReadFrame reads a single length-prefixed frame from r and returns
// its raw protobuf payload. The returned slice is freshly allocated
// each call and is safe to retain.
//
// When r returns io.EOF before any byte is read, ReadFrame returns
// io.EOF transparently so callers can use it in a read loop.
func ReadFrame(r io.Reader) ([]byte, error) {
	var header [4]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, io.ErrUnexpectedEOF
		}
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("peerrpc/protocol: read length: %w", err)
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 {
		return []byte{}, nil
	}
	if size > MaxFrameSize {
		return nil, fmt.Errorf("%w: %d bytes", ErrOversizedFrame, size)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, fmt.Errorf("peerrpc/protocol: read payload: %w", err)
	}
	return buf, nil
}
