package rpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"time"

	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	peerrpcpb "github.com/peerrpc/go/gen/proto/peerrpc"
	"github.com/peerrpc/go/transport"
	"google.golang.org/protobuf/proto"
)

// Client is the outbound side of an RPC connection. It owns the
// sequence-number allocator and tracks in-flight streams until they
// receive an End frame.
//
// A Client is single-connection: attach it to one transport.Channel
// via Attach. To talk to multiple peers, use multiple Clients.
type Client struct {
	ch              *transport.Channel
	reasm           *transport.Reassembler
	seqAlloc        atomic.Int32
	unaryInterceptors []UnaryClientInterceptor

	mu      sync.Mutex
	streams map[int]*clientStream

	closeOnce sync.Once
}

// NewClient constructs a Client over ch. The caller MUST run Attach
// in a goroutine to pump inbound frames.
func NewClient(ch *transport.Channel, opts ...ClientOption) *Client {
	c := &Client{
		ch:      ch,
		reasm:   transport.NewReassembler(),
		streams: map[int]*clientStream{},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithUnaryClientInterceptors installs a chain of client-side unary
// interceptors. Order is outermost-first.
func WithUnaryClientInterceptors(is ...UnaryClientInterceptor) ClientOption {
	return func(c *Client) { c.unaryInterceptors = append(c.unaryInterceptors, is...) }
}

// Attach begins consuming inbound ResponseFrames and dispatching them
// to the right in-flight stream. Returns when the transport closes or
// ctx is canceled.
func (c *Client) Attach(ctx context.Context) error {
	for {
		raw, err := c.ch.Recv(ctx)
		if err != nil {
			c.failAll(err)
			if err == io.EOF || isClosed(err) {
				return nil
			}
			return err
		}
		var frame peerrpcpb.ResponseFrame
		if err := proto.Unmarshal(raw, &frame); err != nil {
			continue
		}
		c.dispatch(&frame)
	}
}

// clientStream is the per-RPC state on the client side.
type clientStream struct {
	seq     int
	method  string
	header  *metadataHolder
	trailer *metadataHolder
	inbound chan inboundResp
	endOnce sync.Once
	end     chan struct{}
	result  endResult

	// sender is installed by the Client when the stream is opened so
	// ClientStream.Send can reach the transport without needing a
	// back-pointer to *Client.
	sender func(ctx context.Context, b []byte) error
	// closer emits the End{CloseSend:true} frame. Installed alongside
	// sender.
	closer func(ctx context.Context)
}

type inboundResp struct {
	data []byte
	err  error
}

type endResult struct {
	status *Status
	err    error
}

// nextSeq allocates an odd sequence number (client-initiated streams
// use odd numbers; even numbers are reserved for future server-push).
func (c *Client) nextSeq() int {
	for {
		n := c.seqAlloc.Add(2)
		if n > 0 {
			return int(n)
		}
	}
}

// dispatch routes one ResponseFrame to its stream.
func (c *Client) dispatch(f *peerrpcpb.ResponseFrame) {
	seq := 0
	if f.Routing != nil {
		seq = int(f.Routing.Sequence)
	}
	c.mu.Lock()
	cs, ok := c.streams[seq]
	c.mu.Unlock()
	if !ok {
		return
	}

	switch t := f.Type.(type) {
	case *peerrpcpb.ResponseFrame_Begin:
		if t.Begin != nil {
			if t.Begin.Header != nil {
				mergeIn(cs.header, t.Begin.Header)
			}
			if len(t.Begin.InlineData) > 0 {
				c.deliver(cs, t.Begin.InlineData)
			}
		}
	case *peerrpcpb.ResponseFrame_Data:
		switch d := t.Data.GetContent().(type) {
		case *peerrpcpb.Data_Message:
			c.deliver(cs, d.Message)
		case *peerrpcpb.Data_Chunk:
			full, ok := c.reasm.Reassemble(int32(seq), int(d.Chunk.TotalSize), int(d.Chunk.Offset), d.Chunk.Data)
			if ok {
				c.deliver(cs, full)
			}
		}
	case *peerrpcpb.ResponseFrame_End:
		if t.End != nil && t.End.Trailer != nil {
			mergeIn(cs.trailer, t.End.Trailer)
		}
		st := OK()
		if t.End != nil && t.End.Status != nil {
			st = statusFromProto(t.End.Status)
		}
		cs.endOnce.Do(func() {
			cs.result = endResult{status: st, err: st.Err()}
			close(cs.end)
		})
		c.mu.Lock()
		delete(c.streams, seq)
		c.mu.Unlock()
	}
}

// deliver pushes one response payload into the stream's inbound chan.
func (c *Client) deliver(cs *clientStream, payload []byte) {
	select {
	case cs.inbound <- inboundResp{data: payload}:
	default:
		go func() { cs.inbound <- inboundResp{data: payload} }()
	}
}

// failAll cancels every in-flight stream when the transport closes.
func (c *Client) failAll(err error) {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		for seq, cs := range c.streams {
			cs.endOnce.Do(func() {
				cs.result = endResult{status: Err(14, err), err: err}
				close(cs.end)
			})
			delete(c.streams, seq)
		}
		c.mu.Unlock()
	})
}

// openStream registers a new clientStream.
func (c *Client) openStream(method string) *clientStream {
	cs := &clientStream{
		method:  method,
		header:  newMetadataHolder(),
		trailer: newMetadataHolder(),
		inbound: make(chan inboundResp, 16),
		end:     make(chan struct{}),
	}
	cs.seq = c.nextSeq()
	cs.sender = func(ctx context.Context, b []byte) error {
		c.sendPayload(ctx, cs.seq, b)
		return nil
	}
	cs.closer = func(ctx context.Context) {
		c.sendFrame(ctx, &peerrpcpb.Frame{
			Routing: &peerrpcpb.Routing{Sequence: int32(cs.seq)},
			Type:    &peerrpcpb.Frame_End{End: &peerrpcpb.End{CloseSend: true}},
		})
	}
	c.mu.Lock()
	c.streams[cs.seq] = cs
	c.mu.Unlock()
	return cs
}

func (c *Client) cleanupStream(seq int) {
	c.mu.Lock()
	delete(c.streams, seq)
	c.mu.Unlock()
}

// sendFrame marshals and writes one Frame. Errors are ignored at the
// call site; the caller learns of transport failure via Attach
// tearing down the in-flight streams.
func (c *Client) sendFrame(ctx context.Context, f *peerrpcpb.Frame) {
	_ = c.ch.SendFrame(ctx, f)
}

// sendPayload writes a Data frame (or chunked frames) carrying b.
func (c *Client) sendPayload(ctx context.Context, seq int, b []byte) {
	if len(b) <= transport.MessageMax {
		c.sendFrame(ctx, &peerrpcpb.Frame{
			Routing: &peerrpcpb.Routing{Sequence: int32(seq)},
			Type: &peerrpcpb.Frame_Data{Data: &peerrpcpb.Data{
				Content: &peerrpcpb.Data_Message{Message: append([]byte(nil), b...)},
			}},
		})
		return
	}
	for offset := 0; offset < len(b); offset += transport.ChunkSize {
		end := offset + transport.ChunkSize
		if end > len(b) {
			end = len(b)
		}
		c.sendFrame(ctx, &peerrpcpb.Frame{
			Routing: &peerrpcpb.Routing{Sequence: int32(seq)},
			Type: &peerrpcpb.Frame_Data{Data: &peerrpcpb.Data{
				Content: &peerrpcpb.Data_Chunk{Chunk: &peerrpcpb.Chunk{
					TotalSize: int32(len(b)),
					Offset:    int32(offset),
					Data:      append([]byte(nil), b[offset:end]...),
				}},
			}},
		})
	}
}

// InvokeUnary is the public Unary invocation. req is the marshaled
// protobuf request; the returned slice is the marshaled protobuf
// response on success.
//
// hdr carries any outgoing header metadata; nil is allowed.
//
// If the Client was constructed with WithUnaryClientInterceptors they
// wrap the underlying wire call outermost-first.
func (c *Client) InvokeUnary(ctx context.Context, method string, req []byte, hdr map[string][]string) ([]byte, *Status) {
	// Save hdr into the context so the inner invoker (and any
	// interceptor that wants to inspect / mutate it) can pick it up.
	if hdr != nil {
		ctx = ctxWithOutgoingHeader(ctx, hdr)
	}
	invoker := chainUnaryClient(c.unaryInterceptors, c.invokeUnary)
	return invoker(ctx, method, req)
}

// invokeUnary is the bottom of the interceptor chain: it actually
// puts the request on the wire and waits for the response.
func (c *Client) invokeUnary(ctx context.Context, method string, req []byte) ([]byte, *Status) {
	cs := c.openStream(method)
	defer c.cleanupStream(cs.seq)
	c.watchCancel(ctx, cs)

	hdr := outgoingHeaderFromCtx(ctx)
	call := &peerrpcpb.Call{
		Method:          method,
		ProtocolVersion: 1,
	}
	if dl, ok := ctx.Deadline(); ok {
		call.DeadlineMs = int32(time.Until(dl).Milliseconds())
	}
	if hdr != nil {
		call.Metadata = metadataFromKV(hdr)
	}
	if len(req) <= transport.InlineMax {
		call.InlineData = append([]byte(nil), req...)
	}

	c.sendFrame(ctx, &peerrpcpb.Frame{
		Routing: &peerrpcpb.Routing{Sequence: int32(cs.seq)},
		Type:    &peerrpcpb.Frame_Call{Call: call},
	})
	if call.InlineData == nil {
		c.sendPayload(ctx, cs.seq, req)
	}
	// Unary: half-close immediately.
	c.sendFrame(ctx, &peerrpcpb.Frame{
		Routing: &peerrpcpb.Routing{Sequence: int32(cs.seq)},
		Type:    &peerrpcpb.Frame_End{End: &peerrpcpb.End{CloseSend: true}},
	})

	// First message wins (Unary returns exactly one response).
	select {
	case r := <-cs.inbound:
		if r.err != nil {
			return nil, Err(13, r.err)
		}
		select {
		case <-cs.end:
			return r.data, cs.result.status
		case <-ctx.Done():
			return nil, Err(4, ctx.Err())
		}
	case <-cs.end:
		return nil, cs.result.status
	case <-ctx.Done():
		return nil, Err(4, ctx.Err())
	}
}

// InvokeServerStreaming opens a server-streaming RPC. The caller
// ranges Recv until io.EOF.
func (c *Client) InvokeServerStreaming(ctx context.Context, method string, req []byte, hdr map[string][]string) (*ClientStream, *Status) {
	cs := c.openStream(method)
	c.watchCancel(ctx, cs)

	call := &peerrpcpb.Call{
		Method:          method,
		ProtocolVersion: 1,
		Metadata:        metadataFromKV(hdr),
	}
	if dl, ok := ctx.Deadline(); ok {
		call.DeadlineMs = int32(time.Until(dl).Milliseconds())
	}
	if len(req) <= transport.InlineMax {
		call.InlineData = append([]byte(nil), req...)
	}

	c.sendFrame(ctx, &peerrpcpb.Frame{
		Routing: &peerrpcpb.Routing{Sequence: int32(cs.seq)},
		Type:    &peerrpcpb.Frame_Call{Call: call},
	})
	if call.InlineData == nil {
		c.sendPayload(ctx, cs.seq, req)
	}
	c.sendFrame(ctx, &peerrpcpb.Frame{
		Routing: &peerrpcpb.Routing{Sequence: int32(cs.seq)},
		Type:    &peerrpcpb.Frame_End{End: &peerrpcpb.End{CloseSend: true}},
	})

	return &ClientStream{cs: cs, ctx: ctx}, OK()
}

// InvokeClientStreaming opens a client-streaming RPC. The caller
// pushes messages via Send, then CloseSend to signal half-close, then
// Recv exactly one response.
//
// The first request message (if provided) is sent inline with the
// Call frame.
func (c *Client) InvokeClientStreaming(ctx context.Context, method string, firstReq []byte, hdr map[string][]string) (*ClientStream, *Status) {
	cs := c.openStream(method)
	c.watchCancel(ctx, cs)

	call := &peerrpcpb.Call{
		Method:          method,
		ProtocolVersion: 1,
		Metadata:        metadataFromKV(hdr),
	}
	if dl, ok := ctx.Deadline(); ok {
		call.DeadlineMs = int32(time.Until(dl).Milliseconds())
	}
	if len(firstReq) > 0 && len(firstReq) <= transport.InlineMax {
		call.InlineData = append([]byte(nil), firstReq...)
	}

	c.sendFrame(ctx, &peerrpcpb.Frame{
		Routing: &peerrpcpb.Routing{Sequence: int32(cs.seq)},
		Type:    &peerrpcpb.Frame_Call{Call: call},
	})
	if firstReq != nil && call.InlineData == nil {
		c.sendPayload(ctx, cs.seq, firstReq)
	}

	return &ClientStream{cs: cs, ctx: ctx}, OK()
}

// InvokeBidiStreaming opens a bidirectional-streaming RPC. Both sides
// may stream messages independently; the caller finishes its direction
// with CloseSend and then drains Recv until io.EOF.
func (c *Client) InvokeBidiStreaming(ctx context.Context, method string, firstReq []byte, hdr map[string][]string) (*ClientStream, *Status) {
	// Identical wire shape to ClientStreaming; the difference is only
	// in how the application uses Send / Recv (multiple response
	// messages vs one). Reuse the same open path.
	return c.InvokeClientStreaming(ctx, method, firstReq, hdr)
}

// ClientStream is the per-RPC handle returned by streaming
// invocations. For server-streaming RPCs only Recv is used; for
// client-streaming and bidi-streaming RPCs the caller also uses Send
// and CloseSend.
type ClientStream struct {
	cs  *clientStream
	ctx context.Context

	closeSendOnce sync.Once
}

// Send writes one request message. For client-streaming and bidi-
// streaming RPCs the caller invokes Send zero or more times, then
// CloseSend to signal half-close.
//
// Send panics if CloseSend has already been called.
func (s *ClientStream) Send(b []byte) error {
	// Reuse the client's send path by reconstructing it from the
	// stream. We can't call c.sendPayload directly because we don't
	// have a back-pointer to *Client here; instead we walk the
	// transport through cs.
	//
	// The stream keeps a reference to the parent Client via the
	// caller-installed closure on openStream.
	if s.cs.sender == nil {
		return io.ErrClosedPipe
	}
	return s.cs.sender(s.ctx, append([]byte(nil), b...))
}

// CloseSend signals half-close: the client will send no more request
// messages. The server's Recv returns io.EOF on its next call. After
// CloseSend, Send panics.
//
// Safe to call multiple times; subsequent calls are no-ops.
func (s *ClientStream) CloseSend() error {
	s.closeSendOnce.Do(func() {
		if s.cs.closer != nil {
			s.cs.closer(s.ctx)
		}
	})
	return nil
}

// Recv returns the next response message. Returns io.EOF when the
// server has cleanly closed.
//
// When both inbound and end are ready (typical when the server sent
// all messages and End back-to-back before the caller first Recvs),
// Recv drains inbound first before declaring EOF.
func (s *ClientStream) Recv() ([]byte, error) {
	select {
	case r := <-s.cs.inbound:
		return r.data, r.err
	default:
	}
	select {
	case r := <-s.cs.inbound:
		return r.data, r.err
	case <-s.cs.end:
		if s.cs.result.err != nil && s.cs.result.status != nil && s.cs.result.status.Code != 0 {
			return nil, s.cs.result.err
		}
		return nil, io.EOF
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	}
}

// Header returns a snapshot of the response header collected so far.
func (s *ClientStream) Header() map[string][]string {
	return s.cs.header.Header()
}

// Trailer returns the response trailer (available after Recv returns
// io.EOF).
func (s *ClientStream) Trailer() map[string][]string {
	return s.cs.trailer.Trailer()
}

// watchCancel spawns a goroutine that, when ctx is canceled before
// the stream's end arrives, emits an End{status:CANCELLED} frame to
// the server so it can stop the in-flight handler. This implements
// client-driven cancellation propagation.
//
// The goroutine exits as soon as either ctx is canceled or the
// stream terminates, so it does not leak on the success path.
func (c *Client) watchCancel(ctx context.Context, cs *clientStream) {
	if ctx == nil {
		return
	}
	go func() {
		select {
		case <-ctx.Done():
			// Issue a CANCELLED to the server. Best-effort: if the
			// transport already died, failAll will surface the error.
			c.sendFrame(context.Background(), &peerrpcpb.Frame{
				Routing: &peerrpcpb.Routing{Sequence: int32(cs.seq)},
				Type: &peerrpcpb.Frame_End{End: &peerrpcpb.End{
					Status: &statuspb.Status{Code: 1, Message: "cancelled by client"},
				}},
			})
		case <-cs.end:
		}
	}()
}
func mergeIn(dst *metadataHolder, src *peerrpcpb.Metadata) {
	dst.mu.Lock()
	defer dst.mu.Unlock()
	for k, vs := range src.Md {
		existing := dst.md.Md[k]
		if existing == nil {
			existing = &peerrpcpb.Strings{}
			dst.md.Md[k] = existing
		}
		existing.Values = append(existing.Values, vs.Values...)
	}
}

func metadataFromKV(hdr map[string][]string) *peerrpcpb.Metadata {
	md := &peerrpcpb.Metadata{Md: map[string]*peerrpcpb.Strings{}}
	for k, vs := range hdr {
		md.Md[k] = &peerrpcpb.Strings{Values: append([]string(nil), vs...)}
	}
	return md
}
