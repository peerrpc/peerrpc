// Command peerrpc-echo-server is a PeerRPC server that connects to a
// standalone signal-server over WebSocket and serves the echo.Echo
// service with all four RPC types (Unary / Server-Streaming /
// Client-Streaming / Bidi).
//
// It is the server counterpart to examples/echo-ts (the browser client).
// Run a signal-server first, then this server, then open the browser
// echo page:
//
//	# terminal 1
//	make run-signal
//
//	# terminal 2
//	make run-echo-server
//
//	# terminal 3
//	make run-echo-ts
//
// Then connect from the browser echo page and exercise the RPCs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/peerrpc/go/peerrpc"
	"github.com/peerrpc/go/rpc"
)

// registerEcho mounts the echo.Echo service with all four RPC types,
// matching the method paths the browser echo demo calls:
//
//	/echo.Echo/Echo          Unary
//	/echo.Echo/Stream        Server-Streaming
//	/echo.Echo/Collect       Client-Streaming
//	/echo.Echo/Chat          Bidi-Streaming
//	/echo.Echo/LargeEcho     Unary (multi-MB round-trip; exercises chunking)
//	/echo.Echo/LargeDownload Server-Streaming (server pushes a multi-MB blob)
func registerEcho(srv *rpc.Server) {
	srv.RegisterService(rpc.ServiceDesc{
		ServiceName: "echo.Echo",
		Methods: []rpc.MethodDesc{
			{
				Method: "Echo",
				Kind:   rpc.MethodKindUnary,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					req, err := s.Recv()
					if err != nil {
						return rpc.Err(13, err)
					}
					if err := s.Send(append([]byte("echo: "), req...)); err != nil {
						return rpc.Err(13, err)
					}
					return rpc.OK()
				},
			},
			{
				Method: "Stream",
				Kind:   rpc.MethodKindServerStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					req, err := s.Recv()
					if err != nil && err != io.EOF {
						return rpc.Err(13, err)
					}
					for i := 1; i <= 5; i++ {
						msg := []byte(fmt.Sprintf("chunk %d for %q", i, string(req)))
						if err := s.Send(msg); err != nil {
							return rpc.Err(13, err)
						}
					}
					return rpc.OK()
				},
			},
			{
				Method: "Collect",
				Kind:   rpc.MethodKindClientStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					var n, total int
					for {
						msg, err := s.Recv()
						if err == io.EOF {
							break
						}
						if err != nil {
							return rpc.Err(13, err)
						}
						n++
						total += len(msg)
					}
					reply := []byte(fmt.Sprintf("received %d messages (%d bytes)", n, total))
					if err := s.Send(reply); err != nil {
						return rpc.Err(13, err)
					}
					return rpc.OK()
				},
			},
			{
				Method: "Chat",
				Kind:   rpc.MethodKindBidiStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					var seq int
					for {
						msg, err := s.Recv()
						if err == io.EOF {
							return rpc.OK()
						}
						if err != nil {
							return rpc.Err(13, err)
						}
						seq++
						reply := []byte(fmt.Sprintf("ack %d: %s", seq, string(msg)))
						if err := s.Send(reply); err != nil {
							return rpc.Err(13, err)
						}
					}
				},
			},
			{
				// LargeEcho echoes the request payload verbatim. When the
				// caller sends a multi-MB payload this exercises both the
				// inbound reassembly (server Recv) and the outbound
				// chunking (server Send). The client verifies integrity.
				Method: "LargeEcho",
				Kind:   rpc.MethodKindUnary,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					req, err := s.Recv()
					if err != nil {
						return rpc.Err(13, err)
					}
					if err := s.Send(req); err != nil {
						return rpc.Err(13, err)
					}
					return rpc.OK()
				},
			},
			{
				// LargeDownload generates and pushes a blob of the
				// caller-chosen size. The first request message carries
				// the desired size as a decimal byte count (e.g. "1048576"
				// for 1 MiB); an empty request defaults to 1 MiB. The
				// blob is filled with a deterministic pattern the client
				// can verify without echoing it back.
				Method: "LargeDownload",
				Kind:   rpc.MethodKindServerStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					req, err := s.Recv()
					if err != nil && err != io.EOF {
						return rpc.Err(13, err)
					}
					size, perr := parseDownloadSize(req)
					if perr != nil {
						return rpc.Err(3, perr) // code 3 = INVALID_ARGUMENT
					}
					blob := makePattern(size)
					if err := s.Send(blob); err != nil {
						return rpc.Err(13, err)
					}
					return rpc.OK()
				},
			},
			{
				// LargeEchoStream is a bidi-streaming echo. The client
				// sends an arbitrary number of messages (each a chunk of
				// a larger logical payload); the server echoes each
				// verbatim. Memory is constant (one chunk in flight), so
				// total transfer size is unbounded — the caller streams
				// terabytes if it wants. The client half-closes to end.
				Method: "LargeEchoStream",
				Kind:   rpc.MethodKindBidiStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					for {
						msg, err := s.Recv()
						if err == io.EOF {
							return rpc.OK()
						}
						if err != nil {
							return rpc.Err(13, err)
						}
						if err := s.Send(msg); err != nil {
							return rpc.Err(13, err)
						}
					}
				},
			},
			{
				// LargeDownloadStream generates and pushes a blob of the
				// caller-chosen size in 16 MiB chunks over a
				// server-streaming RPC. Unlike LargeDownload (single
				// message, capped at ~2 GiB by the int32 Chunk.total_size
				// field), this streams as many messages as needed, so the
				// total size is bounded only by an int64 counter. The
				// first request message carries the size (decimal bytes,
				// optional K/KB/M/MB/G/GB suffix).
				Method: "LargeDownloadStream",
				Kind:   rpc.MethodKindServerStreaming,
				Handler: func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
					req, err := s.Recv()
					if err != nil && err != io.EOF {
						return rpc.Err(13, err)
					}
					size, perr := parseStreamSize(req)
					if perr != nil {
						return rpc.Err(3, perr)
					}
					chunk := make([]byte, 16*1024*1024) // 16 MiB, reused
					var sent int64
					for sent < size {
						end := sent + int64(len(chunk))
						if end > size {
							end = size
						}
						fillPattern(chunk[:end-sent], sent)
						if err := s.Send(chunk[:end-sent]); err != nil {
							return rpc.Err(13, err)
						}
						sent = end
					}
					return rpc.OK()
				},
			},
		},
	})
}

// maxDownloadBytes caps the LargeDownload blob size. The wire
// Chunk.total_size field is a signed int32 (max 2,147,483,647 bytes ≈
// 2047.99 MiB), so the cap is the largest int32 value. In practice a
// browser tab will run out of memory well before this; the client also
// clamps its request size.
const maxDownloadBytes = math.MaxInt32

// parseDownloadSize interprets the request payload as a decimal byte
// count and clamps it to [1, maxDownloadBytes]. An empty/omitted
// request defaults to 1 MiB. A trailing unit suffix (K/KB/M/MB) is
// accepted for convenience.
func parseDownloadSize(req []byte) (int, error) {
	raw := strings.TrimSpace(strings.ToLower(string(req)))
	if raw == "" {
		return 1 << 20, nil // default 1 MiB
	}
	mul := 1
	switch {
	case strings.HasSuffix(raw, "mb"):
		raw, mul = strings.TrimSuffix(raw, "mb"), 1<<20
	case strings.HasSuffix(raw, "m"):
		raw, mul = strings.TrimSuffix(raw, "m"), 1<<20
	case strings.HasSuffix(raw, "kb"):
		raw, mul = strings.TrimSuffix(raw, "kb"), 1<<10
	case strings.HasSuffix(raw, "k"):
		raw, mul = strings.TrimSuffix(raw, "k"), 1<<10
	}
	n, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || n < 0 {
		return 0, errors.New("large-download: size must be a non-negative decimal byte count")
	}
	n *= mul
	if n < 1 {
		n = 1
	}
	if n > maxDownloadBytes {
		n = maxDownloadBytes
	}
	return n, nil
}

// makePattern fills a size-byte slice with a deterministic pattern:
// byte(i % 251). 251 is prime, giving a long non-trivial cycle the
// client can verify byte-for-byte without transmitting the whole blob
// back for comparison.
func makePattern(size int) []byte {
	b := make([]byte, size)
	fillPattern(b, 0)
	return b
}

// fillPattern fills dst with the deterministic byte(i % 251) pattern
// starting at global byte offset base. It writes into an existing
// buffer (no allocation) so callers can reuse one chunk buffer across
// an unbounded stream.
func fillPattern(dst []byte, base int64) {
	for i := range dst {
		dst[i] = byte(int((base + int64(i)) % 251))
	}
}

// maxStreamBytes caps the streaming blob size. Streaming RPCs send
// many messages, so unlike the single-message path there is no int32
// ceiling; the cap is an int64 sanity bound (1 EiB). Real runs are
// limited by bandwidth/time, not by this constant.
const maxStreamBytes = int64(1) << 60

// parseStreamSize interprets the request payload as a decimal byte
// count (int64) with an optional K/KB/M/MB/G/GB suffix and clamps it
// to [1, maxStreamBytes]. An empty/omitted request defaults to 1 MiB.
func parseStreamSize(req []byte) (int64, error) {
	raw := strings.TrimSpace(strings.ToLower(string(req)))
	if raw == "" {
		return 1 << 20, nil // default 1 MiB
	}
	mul := int64(1)
	switch {
	case strings.HasSuffix(raw, "gb"):
		raw, mul = strings.TrimSuffix(raw, "gb"), 1<<30
	case strings.HasSuffix(raw, "g"):
		raw, mul = strings.TrimSuffix(raw, "g"), 1<<30
	case strings.HasSuffix(raw, "mb"):
		raw, mul = strings.TrimSuffix(raw, "mb"), 1<<20
	case strings.HasSuffix(raw, "m"):
		raw, mul = strings.TrimSuffix(raw, "m"), 1<<20
	case strings.HasSuffix(raw, "kb"):
		raw, mul = strings.TrimSuffix(raw, "kb"), 1<<10
	case strings.HasSuffix(raw, "k"):
		raw, mul = strings.TrimSuffix(raw, "k"), 1<<10
	}
	n, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64)
	if err != nil || n < 0 {
		return 0, errors.New("large-download-stream: size must be a non-negative decimal byte count")
	}
	n *= mul
	if n < 1 {
		n = 1
	}
	if n > maxStreamBytes {
		n = maxStreamBytes
	}
	return n, nil
}

func main() {
	signalAddr := flag.String("signal", "ws://localhost:8443", "signal-server base URL (ws:// or wss:// required)")
	service := flag.String("service", "echo.Echo", "rendezvous service key")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Use the builder API. The signal-server URL keeps its scheme;
	// signal.NewWS accepts ws:// or wss:// (and rewrites http(s)://).
	ln, err := peerrpc.ListenContext(ctx).
		SignalAt(*signalAddr).
		Service(*service).
		Over(peerrpc.SchemeWS).
		WithNegotiationTimeout(60 * time.Second).
		Listen()
	if err != nil {
		logger.Error("Listen", "err", err)
		os.Exit(1)
	}
	defer ln.Close()

	logger.Info("echo server listening",
		"signal", *signalAddr,
		"service", *service,
	)

	// Drive Accept ourselves rather than via ln.Serve: a single failed
	// Accept (e.g. a browser that announced but never completed the
	// WebRTC handshake, or a negotiation timeout) must NOT take the
	// whole server down. We log and loop, so the server stays up for
	// the next client.
	for {
		if ctx.Err() != nil {
			break
		}
		sc, aerr := ln.Accept(ctx)
		if aerr != nil {
			if ctx.Err() != nil {
				break
			}
			logger.Warn("Accept failed; continuing", "err", aerr)
			continue
		}
		go func(c *peerrpc.ServerConn) {
			defer c.Close()
			srv := rpc.NewServer()
			registerEcho(srv)
			if serr := srv.Serve(ctx, c.Channel()); serr != nil {
				logger.Warn("Serve returned", "err", serr)
			}
		}(sc)
	}
}
