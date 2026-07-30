// Package transport wraps a WebRTC DataChannel into a reliable
// length-prefixed Frame reader/writer with built-in sharding for large
// messages and outbound backpressure.
//
// Layering (top to bottom):
//
//	rpc.Server / rpc.Client
//	      │  Frame / ResponseFrame (proto messages)
//	      ▼
//	transport.Channel.Send / Recv
//	      │  transparent Chunk split / reassembly
//	      ▼
//	peer.DataChannel (ordered, reliable)
//
// The transport exposes two channels:
//   - SendFrame(frame proto.Message) blocks under backpressure.
//   - RecvFrame() returns decoded Frame / ResponseFrame.
package transport

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"google.golang.org/protobuf/proto"
)

// SCTP / framing thresholds.
//
// WebRTC DataChannels negotiate an SCTP max-message-size (256 KiB by
// default, per pion's a=max-message-size:262144 and the browser). Each
// dc.send() carries one ENCODED frame = 4-byte length prefix + protobuf
// envelope (Routing + Data tags) + payload. That envelope is ~40 bytes
// for a Chunk frame, so a payload of exactly 256 KiB would push the
// encoded frame just over the limit and the browser rejects it with
// "Trying to send message larger than max-message-size".
//
// We reserve frameOverhead so the encoded frame stays safely under the
// negotiated max. These are transport-layer chunking thresholds only;
// they are NOT carried on the wire (Chunk carries total_size/offset/
// data), so each side may pick its own value without a protocol change.
const (
	// maxFrameBytes is the negotiated SCTP max-message-size. Every
	// encoded frame MUST stay at or below this. peerrpc peers advertise
	// a=max-message-size:262144 in their local SDPs (pion already
	// defaults to 262144; webrtc-rs needs the SDP injection plus
	// can_send=Unbounded on its SettingEngine) so the negotiation
	// reaches 256 KiB. Once both sides advertise ≥256 KiB, chunk sizes
	// of 255 KiB are safe.
	maxFrameBytes = 256 * 1024
	// frameOverhead is the worst-case bytes added by the 4-byte length
	// prefix plus the protobuf Frame/Routing/Data/Chunk envelope. 1 KiB
	// is far larger than the real ~40 bytes, giving a safety margin.
	frameOverhead = 1 * 1024

	// InlineMax is the largest payload that may ride in Call.inline_data
	// or Begin.inline_data. Above this, frames use Data.message or
	// Data.chunk.
	InlineMax = 16 * 1024
	// MessageMax is the largest single Data.message payload. Larger
	// logical payloads MUST be split into Chunk frames. Sized so the
	// encoded frame fits under the SCTP max-message-size.
	MessageMax = maxFrameBytes - frameOverhead
	// ChunkSize is the per-chunk payload size when fragmenting. Sized so
	// the encoded frame fits under the SCTP max-message-size.
	ChunkSize = maxFrameBytes - frameOverhead
	// BufferedAmountHigh is the high-watermark (bytes) above which the
	// channel pauses new Send calls until the DataChannel drains.
	BufferedAmountHigh = 1 << 20 // 1 MiB
)

// DataChannelLabel carries the protocol version on the wire.
const DataChannelLabel = "peerrpc-v1"

// Channel is the transport-level duplex pipe backed by a WebRTC
// DataChannel.
//
// A Channel is goroutine-safe for Send. It is intended for a single
// reader (the multiplexer) but that reader may dispatch the decoded
// frames to many streams.
type Channel struct {
	dc *webrtc.DataChannel

	// outbound backpressure
	bufferedLow       chan struct{}
	bufferedLowActive bool
	bufferedLowMu     sync.Mutex

	// inbound queue
	recv chan []byte

	// chunk reassembly
	reasm *Reassembler

	closeOnce sync.Once
	closeMu   sync.Mutex
	closed    chan struct{}
	closeErr  error
	shutdownDone bool
}

// assembler collects chunks for one logical message on one stream.
type assembler struct {
	buf      []byte
	got      int
	total    int
	lastSeen time.Time
}

// New wraps an established DataChannel. Caller is still responsible
// for the underlying PeerConnection; this constructor only registers
// the OnMessage / OnClose handlers.
//
// The DataChannel MUST be created with ordered=true, reliable delivery
// (maxRetransmits = infinity). New does not verify this because pion's
// API makes it awkward at runtime; the peer package enforces it at
// DataChannel creation time.
func New(dc *webrtc.DataChannel) *Channel {
	c := &Channel{
		dc:          dc,
		bufferedLow: make(chan struct{}, 1),
		recv:        make(chan []byte, 256),
		reasm:       NewReassembler(),
		closed:      make(chan struct{}),
	}

	dc.OnBufferedAmountLow(func() {
		c.bufferedLowMu.Lock()
		c.bufferedLowActive = false
		c.bufferedLowMu.Unlock()
		select {
		case c.bufferedLow <- struct{}{}:
		default:
		}
	})
	dc.OnMessage(func(msg webrtc.DataChannelMessage) {
		select {
		case c.recv <- msg.Data:
		case <-c.closed:
		}
	})
	dc.OnClose(func() {
		c.shutdown(io.ErrClosedPipe)
	})

	// Configure pion to emit OnBufferedAmountLow whenever the buffered
	// amount drops below BufferedAmountThreshold. The threshold is set
	// on the DataChannel; we let the peer layer set it (or default).
	dc.SetBufferedAmountLowThreshold(BufferedAmountHigh)

	return c
}

// SendFrame marshals frame and writes it through the DataChannel
// with a length prefix matching the wire protocol. The frame format is:
//
//	uint32 BE length | protobuf payload
//
// This MUST match the TS transport's encoding so both sides can
// interoperate.
func (c *Channel) SendFrame(ctx context.Context, frame proto.Message) error {
	payload, err := proto.Marshal(frame)
	if err != nil {
		return fmt.Errorf("transport: marshal: %w", err)
	}
	return c.SendRaw(ctx, lengthPrefix(payload))
}

// lengthPrefix prepends a 4-byte big-endian length to payload.
func lengthPrefix(payload []byte) []byte {
	out := make([]byte, 4+len(payload))
	out[0] = byte(len(payload) >> 24)
	out[1] = byte(len(payload) >> 16)
	out[2] = byte(len(payload) >> 8)
	out[3] = byte(len(payload))
	copy(out[4:], payload)
	return out
}

// SendRaw writes pre-marshaled bytes verbatim through the DataChannel.
// It is the relay's primitive: the relay forwards frames without
// inspecting them, so marshaling again would be wasted CPU and would
// risk non-determinism when the wire payload was produced by another
// peer's encoder.
//
// SendRaw applies the same backpressure as SendFrame.
func (c *Channel) SendRaw(ctx context.Context, payload []byte) error {
	select {
	case <-c.closed:
		return fmt.Errorf("transport: channel closed: %w", c.closeErr)
	default:
	}

	if err := c.awaitBufferLow(ctx); err != nil {
		return err
	}
	if err := c.dc.Send(payload); err != nil {
		return fmt.Errorf("transport: send: %w", err)
	}
	return nil
}

// awaitBufferLow blocks until the DataChannel's buffered amount is
// below BufferedAmountHigh or ctx is done.
func (c *Channel) awaitBufferLow(ctx context.Context) error {
	for {
		if c.dc.BufferedAmount() < BufferedAmountHigh {
			return nil
		}
		// Arm the low watermark callback. pion only fires the callback
		// once per crossing; we re-arm each iteration.
		c.bufferedLowMu.Lock()
		if !c.bufferedLowActive {
			c.bufferedLowActive = true
			c.bufferedLowMu.Unlock()
		} else {
			c.bufferedLowMu.Unlock()
		}

		select {
		case <-c.bufferedLow:
			// loop and re-check
		case <-c.closed:
			return fmt.Errorf("transport: channel closed while waiting: %w", c.closeErr)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Recv returns the next decoded frame payload (with the length prefix
// stripped) off the DataChannel. It blocks until a message arrives,
// the channel closes, or ctx is canceled.
func (c *Channel) Recv(ctx context.Context) ([]byte, error) {
	select {
	case b := <-c.recv:
		return stripLengthPrefix(b), nil
	case <-c.closed:
		return nil, fmt.Errorf("transport: channel closed: %w", c.closeErr)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// stripLengthPrefix removes the 4-byte big-endian length prefix from
// an inbound DataChannel message. If the message is too short or the
// prefix claims a different size, the raw bytes are returned as-is
// (backward compatibility with raw-proto senders).
func stripLengthPrefix(b []byte) []byte {
	if len(b) < 4 {
		return b
	}
	length := int(b[0])<<24 | int(b[1])<<16 | int(b[2])<<8 | int(b[3])
	if length <= 0 || 4+length > len(b) {
		// Not a valid length-prefixed message; return raw.
		return b
	}
	return b[4 : 4+length]
}

// Closed returns a channel that is closed when the underlying
// DataChannel closes.
func (c *Channel) Closed() <-chan struct{} { return c.closed }

// Close shuts the DataChannel down with a graceful close code.
// Safe to call multiple times.
func (c *Channel) Close() error {
	// closeOnce serializes the dc.Close call so the underlying SCTP
	// teardown happens exactly once. We do NOT call shutdown from
	// here: shutdown is fired by the DataChannel's OnClose callback
	// registered in New(). Calling shutdown inline used to self-
	// deadlock because shutdown also went through closeOnce.Do.
	c.closeOnce.Do(func() {
		_ = c.dc.Close()
	})
	// Defensive fallback: if for any reason OnClose does not fire
	// (e.g. the DataChannel was never fully open), ensure the
	// shutdown side-effects still run. shutdown is itself
	// idempotent under closeMu.
	c.shutdown(errors.New("channel closed by Close"))
	return nil
}

func (c *Channel) shutdown(reason error) {
	c.closeMu.Lock()
	if c.shutdownDone {
		c.closeMu.Unlock()
		return
	}
	c.shutdownDone = true
	c.closeErr = reason
	c.closeMu.Unlock()
	close(c.closed)
}

// Reassemble collects a Chunk frame into a logical message buffer. When
// all bytes are present (offset 0..total-1 covered) the assembled
// payload is returned and the per-sequence state is dropped.
//
// Reassemble is safe for concurrent use across different sequences but
// only one frame per sequence should be in flight at a time.
func (c *Channel) Reassemble(seq int32, total int, offset int, data []byte) ([]byte, bool) {
	return c.reasm.Reassemble(seq, total, offset, data)
}

// Reassembler collects Chunk frames into complete payloads. It is the
// per-connection (or per-peer) chunk assembler that the rpc.Server and
// rpc.Client both use. Splitting it out from Channel lets the rpc
// layer instantiate one without a live DataChannel (useful in tests).
type Reassembler struct {
	mu     sync.Mutex
	chunks map[int32]*assembler
}

// NewReassembler constructs an empty Reassembler.
func NewReassembler() *Reassembler {
	return &Reassembler{chunks: make(map[int32]*assembler)}
}

// Reassemble folds one chunk into the per-sequence buffer and returns
// the assembled payload plus true when complete.
func (r *Reassembler) Reassemble(seq int32, total int, offset int, data []byte) ([]byte, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	a, ok := r.chunks[seq]
	if !ok || total != a.total {
		a = &assembler{buf: make([]byte, total), total: total}
		r.chunks[seq] = a
	}
	a.lastSeen = time.Now()
	end := offset + len(data)
	if end > len(a.buf) {
		return nil, false
	}
	copy(a.buf[offset:end], data)
	a.got += len(data)
	if a.got >= a.total {
		out := a.buf
		delete(r.chunks, seq)
		return out, true
	}
	return nil, false
}
