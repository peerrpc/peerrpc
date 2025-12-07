package rpc

import (
	"context"
	"fmt"
	"io"
	"sync"

	rpcpb "google.golang.org/genproto/googleapis/rpc/status"
	peerrpcpb "github.com/peerrpc/go/gen/proto/peerrpc"
	"github.com/peerrpc/go/transport"
	"google.golang.org/protobuf/proto"
)

// Server is the inbound side of an RPC connection. It reads Frames
// off a transport.Channel, dispatches them to registered handlers,
// and writes the handler's ResponseFrames back out.
//
// A Server is single-connection: attach it to exactly one
// transport.Channel via Serve. To serve multiple peers, instantiate
// multiple Servers.
type Server struct {
	mu      sync.RWMutex
	methods map[string]MethodDesc
}

// NewServer constructs an empty Server.
func NewServer() *Server {
	return &Server{methods: map[string]MethodDesc{}}
}

// RegisterService installs every method of desc under its fully
// qualified path. Panics if a method path collides with an existing
// registration.
func (s *Server) RegisterService(desc ServiceDesc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, m := range desc.Methods {
		path := "/" + desc.ServiceName + "/" + m.Method
		if _, exists := s.methods[path]; exists {
			panic(fmt.Sprintf("rpc: duplicate method registration: %s", path))
		}
		full := m
		full.Method = path
		s.methods[path] = full
	}
}

// findMethod looks up a method descriptor by path.
func (s *Server) findMethod(path string) (MethodDesc, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.methods[path]
	return m, ok
}

// Serve binds this Server to ch until the channel closes or ctx is
// canceled. Returns nil on graceful shutdown, an error otherwise.
//
// Calling Serve twice on the same Server is undefined; create a new
// Server per transport.
func (s *Server) Serve(ctx context.Context, ch *transport.Channel) error {
	mux := newMultiplexer(ctx, ch)
	go mux.runWriter()
	defer mux.shutdown()

	for {
		raw, err := ch.Recv(ctx)
		if err != nil {
			if err == io.EOF || isClosed(err) {
				return nil
			}
			return err
		}
		var frame peerrpcpb.Frame
		if err := proto.Unmarshal(raw, &frame); err != nil {
			// Malformed frame at the dispatcher level: drop and continue.
			continue
		}
		mux.handleFrame(ctx, s, &frame)
	}
}

// multiplexer owns the per-sequence routing for one Server (or one
// Client). It maps Routing.sequence -> active stream state.
type multiplexer struct {
	ctx context.Context
	ch  *transport.Channel
	out chan outboundFrame

	mu      sync.Mutex
	streams map[int]*streamState

	shutdownOnce sync.Once
	done         chan struct{}
}

// streamState is the per-RPC bookkeeping kept by the multiplexer.
type streamState struct {
	method     MethodDesc
	stream     *ServerStream
	cancel     context.CancelFunc
	headerSent bool
	hdrMu      sync.Mutex
}

// outboundFrame is one queued ResponseFrame plus a done signal.
type outboundFrame struct {
	frame *peerrpcpb.ResponseFrame
	done  chan struct{}
}

func newMultiplexer(ctx context.Context, ch *transport.Channel) *multiplexer {
	return &multiplexer{
		ctx:     ctx,
		ch:      ch,
		out:     make(chan outboundFrame, 256),
		streams: map[int]*streamState{},
		done:    make(chan struct{}),
	}
}

// handleFrame dispatches one inbound Frame to the right stream.
func (m *multiplexer) handleFrame(ctx context.Context, s *Server, f *peerrpcpb.Frame) {
	seq := 0
	if f.Routing != nil {
		seq = int(f.Routing.Sequence)
	}

	switch t := f.Type.(type) {
	case *peerrpcpb.Frame_Call:
		m.openStream(ctx, s, seq, t.Call)
	case *peerrpcpb.Frame_Data:
		m.deliverData(seq, t.Data)
	case *peerrpcpb.Frame_End:
		m.handleEnd(seq, t.End)
	}
}

// openStream creates a new stream state and dispatches the handler.
func (m *multiplexer) openStream(ctx context.Context, s *Server, seq int, call *peerrpcpb.Call) {
	if call == nil {
		m.endStream(seq, &peerrpcpb.End{Status: &rpcpb.Status{Code: 13, Message: "rpc: nil Call"}})
		return
	}
	method, ok := s.findMethod(call.Method)
	if !ok {
		m.endStream(seq, &peerrpcpb.End{Status: &rpcpb.Status{Code: 12, Message: fmt.Sprintf("rpc: unimplemented method: %s", call.Method)}})
		return
	}

	streamCtx, cancel := context.WithCancel(ctx)
	if call.DeadlineMs > 0 {
		var dc context.CancelFunc
		streamCtx, dc = context.WithTimeout(streamCtx, durationFromMs(call.DeadlineMs))
		origCancel := cancel
		cancel = func() { origCancel(); dc() }
	}

	stream := newServerStream(streamCtx, call.Method, m, seq)
	state := &streamState{method: method, stream: stream, cancel: cancel}

	m.mu.Lock()
	if _, exists := m.streams[seq]; exists {
		m.mu.Unlock()
		cancel()
		m.endStream(seq, &peerrpcpb.End{Status: &rpcpb.Status{Code: 13, Message: "rpc: duplicate sequence"}})
		return
	}
	m.streams[seq] = state
	m.mu.Unlock()

	if call.InlineData != nil {
		select {
		case stream.inbound <- call.InlineData:
		default:
			go func() { stream.inbound <- call.InlineData }()
		}
	}

	go m.runHandler(seq, state)
}

// runHandler invokes the per-RPC handler and translates its return
// value into a final End frame.
func (m *multiplexer) runHandler(seq int, st *streamState) {
	defer st.cancel()
	status := st.method.Handler(st.stream.ctx, st.stream)
	trailer := st.stream.hdr.Snapshot()
	end := &peerrpcpb.End{Trailer: trailer, Status: status.toProto()}
	m.endStream(seq, end)
}

// writeResponse is unused in the current Send path; the queue is hit
// directly from Send. Kept as a placeholder to make the lifecycle
// obvious when interceptors get wired in (Phase 2).

// flushBeginOnce emits the Begin frame the first time the handler
// sends any response data.
func (m *multiplexer) flushBeginOnce(s *ServerStream) error {
	st := m.lookupStream(s.seq)
	if st == nil {
		return io.ErrClosedPipe
	}
	st.hdrMu.Lock()
	firstSend := !st.headerSent
	st.headerSent = true
	st.hdrMu.Unlock()

	if !firstSend {
		return nil
	}
	hdr := s.hdr.Snapshot()
	begin := &peerrpcpb.ResponseFrame{
		Routing: &peerrpcpb.Routing{Sequence: int32(s.seq)},
		Type: &peerrpcpb.ResponseFrame_Begin{
			Begin: &peerrpcpb.Begin{Header: hdr},
		},
	}
	return m.queue(begin)
}

// queue writes one ResponseFrame onto the transport.
func (m *multiplexer) queue(frame *peerrpcpb.ResponseFrame) error {
	done := make(chan struct{})
	of := outboundFrame{frame: frame, done: done}
	select {
	case m.out <- of:
		select {
		case <-done:
			return nil
		case <-m.ctx.Done():
			return m.ctx.Err()
		}
	case <-m.done:
		return io.ErrClosedPipe
	case <-m.ctx.Done():
		return m.ctx.Err()
	}
}

// endStream emits the final End frame (preceded by Begin if no Send
// ever ran, e.g. for an immediate error) and removes the stream.
func (m *multiplexer) endStream(seq int, end *peerrpcpb.End) {
	st := m.lookupStream(seq)
	if st != nil {
		st.hdrMu.Lock()
		firstSend := !st.headerSent
		st.headerSent = true
		st.hdrMu.Unlock()
		if firstSend {
			hdr := st.stream.hdr.Snapshot()
			begin := &peerrpcpb.ResponseFrame{
				Routing: &peerrpcpb.Routing{Sequence: int32(seq)},
				Type: &peerrpcpb.ResponseFrame_Begin{
					Begin: &peerrpcpb.Begin{Header: hdr},
				},
			}
			_ = m.queue(begin)
		}
	}

	m.mu.Lock()
	delete(m.streams, seq)
	m.mu.Unlock()

	frame := &peerrpcpb.ResponseFrame{
		Routing: &peerrpcpb.Routing{Sequence: int32(seq)},
		Type:    &peerrpcpb.ResponseFrame_End{End: end},
	}
	_ = m.queue(frame)
}

// lookupStream returns the streamState for seq, or nil if absent.
func (m *multiplexer) lookupStream(seq int) *streamState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streams[seq]
}

// deliverData routes a Data frame to its stream's inbound channel.
func (m *multiplexer) deliverData(seq int, data *peerrpcpb.Data) {
	if data == nil {
		return
	}
	switch c := data.Content.(type) {
	case *peerrpcpb.Data_Message:
		m.deliverPayload(seq, c.Message)
	case *peerrpcpb.Data_Chunk:
		full, ok := m.ch.Reassemble(int32(seq), int(c.Chunk.TotalSize), int(c.Chunk.Offset), c.Chunk.Data)
		if ok {
			m.deliverPayload(seq, full)
		}
	}
}

func (m *multiplexer) deliverPayload(seq int, payload []byte) {
	st := m.lookupStream(seq)
	if st == nil {
		return
	}
	select {
	case st.stream.inbound <- payload:
	case <-st.stream.ctx.Done():
	}
}

// handleEnd processes an End frame from the client (half-close or
// cancel).
func (m *multiplexer) handleEnd(seq int, end *peerrpcpb.End) {
	if end == nil {
		return
	}
	st := m.lookupStream(seq)
	if st == nil {
		return
	}
	if end.CloseSend {
		closeOnce(st.stream.halfClose)
		return
	}
	// Cancellation: cancel the handler ctx and remove the stream.
	st.cancel()
	m.mu.Lock()
	delete(m.streams, seq)
	m.mu.Unlock()
}

// runWriter is the single outbound goroutine that puts ResponseFrames
// onto the transport. Concentrating writes in one goroutine keeps
// pion's goroutine-safety boundary simple and gives natural
// backpressure propagation.
func (m *multiplexer) runWriter() {
	for {
		select {
		case of := <-m.out:
			_ = m.ch.SendFrame(m.ctx, of.frame)
			if of.done != nil {
				close(of.done)
			}
		case <-m.done:
			return
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *multiplexer) shutdown() {
	m.shutdownOnce.Do(func() { close(m.done) })
}

// closeOnce closes c if not already closed.
func closeOnce(c chan struct{}) {
	select {
	case <-c:
	default:
		close(c)
	}
}

// isClosed returns true if err is a transport-close error.
func isClosed(err error) bool {
	return err != nil && (err == io.EOF || err == io.ErrClosedPipe)
}
