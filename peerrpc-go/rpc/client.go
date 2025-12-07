package rpc

import (
	"context"
	"io"
	"sync"
	"sync/atomic"

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
	ch       *transport.Channel
	reasm    *transport.Reassembler
	seqAlloc atomic.Int32

	mu      sync.Mutex
	streams map[int]*clientStream

	closeOnce sync.Once
}

// NewClient constructs a Client over ch. The caller MUST run Attach
// in a goroutine to pump inbound frames.
func NewClient(ch *transport.Channel) *Client {
	return &Client{
		ch:      ch,
		reasm:   transport.NewReassembler(),
		streams: map[int]*clientStream{},
	}
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
func (c *Client) InvokeUnary(ctx context.Context, method string, req []byte, hdr map[string][]string) ([]byte, *Status) {
	cs := c.openStream(method)
	defer c.cleanupStream(cs.seq)

	call := &peerrpcpb.Call{
		Method:          method,
		ProtocolVersion: 1,
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

	call := &peerrpcpb.Call{
		Method:          method,
		ProtocolVersion: 1,
		Metadata:        metadataFromKV(hdr),
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

// ClientStream is the per-RPC read handle returned by streaming
// invocations.
type ClientStream struct {
	cs  *clientStream
	ctx context.Context
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

// mergeIn merges src into dst metadata.
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
