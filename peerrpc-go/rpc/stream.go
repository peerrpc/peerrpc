package rpc

import (
	"context"
	"io"
	"sync"

	peerrpcpb "github.com/peerrpc/go/gen/proto/peerrpc"
	"github.com/peerrpc/go/transport"
)

// ServerStream is the per-RPC object a MethodDesc.Handler receives.
//
// It exposes:
//   - Recv to read request messages (one per client Data frame).
//   - Send to write response messages (one Data frame each).
//   - SetHeader / SetTrailer for metadata.
//   - Context for deadline + cancellation propagation.
type ServerStream struct {
	ctx context.Context

	// inbound: raw payload bytes from the client, in arrival order.
	// inline_data from Call is delivered as the first message.
	inbound chan []byte

	// halfClose is closed when the client has sent CloseSend. After
	// this, Recv returns io.EOF.
	halfClose chan struct{}

	method string
	mux    *multiplexer
	seq    int

	hdr      *metadataHolder
	hdrState headerState
	hdrMu    sync.Mutex

	// incoming is the Call.metadata (the client->server header). It
	// is read-only from the handler's perspective; observability
	// interceptors extract trace context from it via IncomingHeader.
	incoming *peerrpcpb.Metadata

	// ctxMu guards ctx against replacement by interceptors that need
	// to extend the handler's context (e.g. attach an OTel span).
	ctxMu sync.Mutex
}

// headerState tracks whether the Begin frame has been flushed yet so
// that SetHeader called after the first Send panics (matches
// grpc-go / connect-go).
type headerState struct {
	headerSent bool
}

// newServerStream constructs a stream owned by the multiplexer.
func newServerStream(ctx context.Context, method string, mux *multiplexer, seq int) *ServerStream {
	return &ServerStream{
		ctx:       ctx,
		inbound:   make(chan []byte, 16),
		halfClose: make(chan struct{}),
		method:    method,
		mux:       mux,
		seq:       seq,
		hdr:       newMetadataHolder(),
	}
}

// Context exposes the per-RPC context. Stream interceptors that
// attach additional values (e.g. an OTel span) replace the context
// via WithContext before calling next.
func (s *ServerStream) Context() context.Context {
	s.ctxMu.Lock()
	defer s.ctxMu.Unlock()
	return s.ctx
}

// WithContext replaces the per-RPC context. Stream interceptors use
// it to attach tracing spans, request-scoped loggers, etc. The
// replacement ctx MUST derive from the original (cancel propagation,
// deadline) — typically via context.WithValue(s.Context(), ...).
func (s *ServerStream) WithContext(ctx context.Context) {
	s.ctxMu.Lock()
	defer s.ctxMu.Unlock()
	s.ctx = ctx
}

// Method returns the fully-qualified method path.
func (s *ServerStream) Method() string { return s.method }

// Recv blocks until the next request message arrives, the client
// half-closes (returns io.EOF), or ctx is canceled.
//
// The non-blocking check first drains any already-queued message
// (inline_data is now delivered synchronously at stream open, so it is
// always present before the handler calls Recv). The blocking select
// then waits for subsequent Data frames or half-close.
func (s *ServerStream) Recv() ([]byte, error) {
	// Non-blocking check first so the inline-payload + CloseSend race
	// resolves in favor of delivering data.
	select {
	case b, ok := <-s.inbound:
		if !ok {
			return nil, io.EOF
		}
		return b, nil
	default:
	}
	select {
	case b, ok := <-s.inbound:
		if !ok {
			return nil, io.EOF
		}
		return b, nil
	case <-s.halfClose:
		return nil, io.EOF
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

// Send queues a response message. It returns once the multiplexer has
// accepted the (last) frame, NOT once the bytes are on the wire.
//
// Payloads larger than transport.MessageMax (256 KiB) are split into
// Data.chunk frames transparently, mirroring the client's sendPayload.
// The receiver reassembles them into a single message, so a large Send
// is observed as one message by the peer's Recv/recv.
func (s *ServerStream) Send(b []byte) error {
	// Begin must precede the first Data frame.
	if err := s.mux.flushBeginOnce(s); err != nil {
		return err
	}
	// Small message: a single Data.message frame.
	if len(b) <= transport.MessageMax {
		frame := &peerrpcpb.ResponseFrame{
			Routing: &peerrpcpb.Routing{Sequence: int32(s.seq)},
			Type: &peerrpcpb.ResponseFrame_Data{
				Data: &peerrpcpb.Data{
					Content: &peerrpcpb.Data_Message{Message: append([]byte(nil), b...)},
				},
			},
		}
		return s.mux.queue(frame)
	}
	// Large message: fragment into Data.chunk frames. total_size lets
	// the peer allocate the buffer up front and detect loss.
	total := int32(len(b))
	for offset := 0; offset < len(b); offset += transport.ChunkSize {
		end := offset + transport.ChunkSize
		if end > len(b) {
			end = len(b)
		}
		frame := &peerrpcpb.ResponseFrame{
			Routing: &peerrpcpb.Routing{Sequence: int32(s.seq)},
			Type: &peerrpcpb.ResponseFrame_Data{Data: &peerrpcpb.Data{
				Content: &peerrpcpb.Data_Chunk{Chunk: &peerrpcpb.Chunk{
					TotalSize: total,
					Offset:    int32(offset),
					Data:      append([]byte(nil), b[offset:end]...),
				}},
			}},
		}
		if err := s.mux.queue(frame); err != nil {
			return err
		}
	}
	return nil
}

// SetHeader attaches header metadata. MUST be called before the first
// Send (the header rides with the Begin frame).
func (s *ServerStream) SetHeader(kv map[string][]string) {
	s.hdrMu.Lock()
	defer s.hdrMu.Unlock()
	if s.hdrState.headerSent {
		panic("rpc: SetHeader called after header was already sent")
	}
	s.hdr.SetHeader(kv)
}

// SetTrailer attaches trailer metadata sent with End.
func (s *ServerStream) SetTrailer(kv map[string][]string) {
	s.hdr.SetTrailer(kv)
}

// Header / Trailer return snapshots of the outgoing header / trailer.
// Used by interceptors and tests.
//
// NOTE: this is the OUTGOING header (set by SetHeader). For the
// INCOMING header (set by the client via Call.metadata) use
// IncomingHeader.
func (s *ServerStream) Header() map[string][]string { return s.hdr.Header() }

// IncomingHeader returns a snapshot of the metadata the client sent
// in its Call frame. Observability interceptors use this to extract
// trace context; auth interceptors can use it to inspect per-call
// credentials.
func (s *ServerStream) IncomingHeader() map[string][]string {
	out := map[string][]string{}
	if s.incoming == nil {
		return out
	}
	for k, vs := range s.incoming.Md {
		out[k] = append([]string(nil), vs.Values...)
	}
	return out
}

func (s *ServerStream) Trailer() map[string][]string { return s.hdr.Trailer() }

// metadataHolder holds one direction's metadata with thread-safety.
type metadataHolder struct {
	mu      sync.Mutex
	md      *peerrpcpb.Metadata
}

func newMetadataHolder() *metadataHolder {
	return &metadataHolder{md: &peerrpcpb.Metadata{Md: map[string]*peerrpcpb.Strings{}}}
}

func (m *metadataHolder) SetHeader(kv map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mergeMetadata(m.md, kv)
}

func (m *metadataHolder) SetTrailer(kv map[string][]string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	mergeMetadata(m.md, kv)
}

func (m *metadataHolder) Snapshot() *peerrpcpb.Metadata {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := &peerrpcpb.Metadata{Md: map[string]*peerrpcpb.Strings{}}
	for k, vs := range m.md.Md {
		out.Md[k] = &peerrpcpb.Strings{Values: append([]string(nil), vs.Values...)}
	}
	return out
}

func (m *metadataHolder) Header() map[string][]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return copyMetadata(m.md)
}

// alias used by tests for Trailer() too.
func (m *metadataHolder) Trailer() map[string][]string { return m.Header() }

func mergeMetadata(dst *peerrpcpb.Metadata, kv map[string][]string) {
	if dst.Md == nil {
		dst.Md = map[string]*peerrpcpb.Strings{}
	}
	for k, vs := range kv {
		existing := dst.Md[k]
		if existing == nil {
			existing = &peerrpcpb.Strings{}
			dst.Md[k] = existing
		}
		existing.Values = append(existing.Values, vs...)
	}
}

func copyMetadata(src *peerrpcpb.Metadata) map[string][]string {
	out := map[string][]string{}
	if src == nil {
		return out
	}
	for k, vs := range src.Md {
		out[k] = append([]string(nil), vs.Values...)
	}
	return out
}
