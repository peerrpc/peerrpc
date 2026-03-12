// Command peerrpc-grpcbridge runs the standalone PeerRPC -> Connect
// bridge. It registers PeerRPC service stubs that forward incoming
// calls to a remote Connect service URL.
//
// Usage:
//
//	peerrpc-grpcbridge \
//	    -signal http://localhost:8080 \
//	    -room my-room \
//	    -peer-id bridge-1 \
//	    -upstream http://localhost:9090 \
//	    -service echo.Echo:Echo,Stream \
//	    -stream-service echo.Echo:Lottery \
//	    -service other.Other:Get,Put
//
// v1 forwards Unary RPCs; v1.1 adds server-streaming support.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/peerrpc/go/grpcbridge"
	"github.com/peerrpc/go/peer"
	"github.com/peerrpc/go/rpc"
	signalsdk "github.com/peerrpc/go/signal"
	"github.com/peerrpc/go/transport"
	"github.com/peerrpc/grpcbridge-server/bridge"

	"log/slog"
)

type serviceList []string

func (s *serviceList) String() string     { return strings.Join(*s, ";") }
func (s *serviceList) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func main() {
	upstreamURL := flag.String("upstream", "", "Connect service base URL (e.g. http://localhost:9090)")
	signalAddr := flag.String("signal", "", "signal server base URL (e.g. http://localhost:8080)")
	roomID := flag.String("room", "", "signaling room id")
	peerID := flag.String("peer-id", "bridge", "peer id the bridge uses in the signaling room")
	role := flag.String("role", "answerer", "peer role (offerer|answerer)")

	var services serviceList
	flag.Var(&services, "service", `unary service spec "Name:Method1,Method2" (repeatable)`)

	var streamServices serviceList
	flag.Var(&streamServices, "stream-service", `server-streaming service spec "Name:Method1,Method2" (repeatable)`)

	flag.Parse()

	if *upstreamURL == "" || *roomID == "" || (len(services) == 0 && len(streamServices) == 0) {
		flag.Usage()
		os.Exit(2)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// 1. Build the PeerRPC server with bridge-mounted services.
	invoker := &httpClientInvokerAdapter{
		base: *upstreamURL,
		c:    &http.Client{Timeout: 30 * time.Second},
	}

	peerrpcSrv := rpc.NewServer()

	// Register unary services.
	for _, spec := range services {
		name, methods, err := bridge.ParseServiceSpec(spec)
		if err != nil {
			logger.Error("bad -service spec", "spec", spec, "err", err)
			os.Exit(2)
		}
		grpcbridge.MountConnectService(peerrpcSrv, name, methods, invoker)
		logger.Info("mounted unary service", "name", name, "methods", methods)
	}

	// Register server-streaming services.
	for _, spec := range streamServices {
		name, methods, err := bridge.ParseServiceSpec(spec)
		if err != nil {
			logger.Error("bad -stream-service spec", "spec", spec, "err", err)
			os.Exit(2)
		}
		grpcbridge.MountConnectServiceWithStreaming(
			peerrpcSrv, name, methods, methods, // all methods are streaming
			invoker, invoker, // invoker handles both unary and streaming
		)
		logger.Info("mounted streaming service", "name", name, "methods", methods)
	}

	// 2. Wire signaling + PeerConnection.
	var backend signalsdk.Backend
	if *signalAddr != "" {
		backend = signalsdk.NewRemote(*signalAddr)
		logger.Info("using remote signaling", "addr", *signalAddr)
	} else {
		backend = signalsdk.NewLocal()
		logger.Info("using in-process signaling")
	}

	sig, err := backend.Exchange(context.Background(), *roomID, *peerID)
	if err != nil {
		logger.Error("signaling exchange", "err", err)
		os.Exit(1)
	}
	defer sig.Close()

	r, err := parseRole(*role)
	if err != nil {
		logger.Error("bad role", "err", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	p, err := peer.New(ctx, r, peer.Config{})
	if err != nil {
		logger.Error("peer.New", "err", err)
		os.Exit(1)
	}
	defer p.Close()

	logger.Info("bridge starting",
		"upstream", *upstreamURL,
		"room_id", *roomID,
		"peer_id", *peerID,
		"role", r,
	)

	var ch *transport.Channel
	if r == signalsdk.RoleOfferer {
		ch, err = p.Dial(ctx, sig)
		if err != nil {
			logger.Error("Dial", "err", err)
			os.Exit(1)
		}
	} else {
		ch, err = p.Accept(ctx, sig)
		if err != nil {
			logger.Error("Accept", "err", err)
			os.Exit(1)
		}
	}
	_ = peerrpcSrv.Serve(ctx, ch)
}

func parseRole(s string) (signalsdk.Role, error) {
	switch s {
	case "offerer":
		return signalsdk.RoleOfferer, nil
	case "answerer":
		return signalsdk.RoleAnswerer, nil
	default:
		return 0, fmt.Errorf("unknown role %q (want offerer|answerer)", s)
	}
}

// httpClientInvokerAdapter forwards Unary RPCs to the upstream via HTTP
// and also implements ConnectStreamInvoker for server-streaming.
type httpClientInvokerAdapter struct {
	base string
	c    *http.Client
}

// Invoke implements ConnectInvoker for Unary RPCs.
func (a *httpClientInvokerAdapter) Invoke(ctx context.Context, procedure string, reqBody []byte, hdr map[string][]string) ([]byte, map[string][]string, *rpc.Status) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+procedure, byteReader(reqBody))
	if err != nil {
		return nil, nil, rpc.Err(13, err)
	}
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Connect-Protocol-Version", "1")
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := a.c.Do(req)
	if err != nil {
		return nil, nil, rpc.Err(14, err)
	}
	defer resp.Body.Close()

	respMD := map[string][]string{}
	for k, vs := range resp.Header {
		lk := strings.ToLower(k)
		respMD[lk] = append(respMD[lk], vs...)
	}

	body, readErr := readAll(resp.Body)
	if readErr != nil {
		return nil, respMD, rpc.Err(13, readErr)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, respMD, &rpc.Status{
			Code:    13,
			Message: fmt.Sprintf("upstream connect http %d", resp.StatusCode),
		}
	}
	return body, respMD, rpc.OK()
}

// InvokeStream implements ConnectStreamInvoker by reading the HTTP
// response body as a stream of length-prefixed Connect messages.
//
// The Connect streaming protocol over HTTP/1.1 uses a single response
// body containing concatenated length-prefixed messages. Each message
// is: 1-byte flag (0x00 = data) + 4-byte big-endian length + payload.
func (a *httpClientInvokerAdapter) InvokeStream(ctx context.Context, procedure string, reqBody []byte, hdr map[string][]string, send func([]byte)) *rpc.Status {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.base+procedure, byteReader(reqBody))
	if err != nil {
		return rpc.Err(13, err)
	}
	req.Header.Set("Content-Type", "application/proto")
	req.Header.Set("Connect-Protocol-Version", "1")
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}

	resp, err := a.c.Do(req)
	if err != nil {
		return rpc.Err(14, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := readAll(resp.Body)
		return decodeConnectErrorFromStatus(resp.StatusCode, body)
	}

	// Read streaming response frames:
	// Each frame: 1-byte flag + 4-byte BE length + payload
	// flag=0x00: data, flag=0x01: error trailer
	headerBuf := make([]byte, 5)
	for {
		if _, err := io.ReadFull(resp.Body, headerBuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return rpc.OK()
			}
			return rpc.Err(13, err)
		}
		flags := headerBuf[0]
		msgLen := int(headerBuf[1])<<24 | int(headerBuf[2])<<16 | int(headerBuf[3])<<8 | int(headerBuf[4])
		if msgLen == 0 {
			continue
		}

		msgBuf := make([]byte, msgLen)
		if _, err := io.ReadFull(resp.Body, msgBuf); err != nil {
			return rpc.Err(13, err)
		}

		if flags == 0x01 {
			// Error trailer: body is JSON-encoded rpc.Status.
			return decodeConnectErrorFromStatus(http.StatusOK, msgBuf)
		}

		send(msgBuf)
	}
}

// decodeConnectErrorFromStatus reads a Connect error body (JSON)
// and returns an rpc.Status.
func decodeConnectErrorFromStatus(httpCode int, body []byte) *rpc.Status {
	if len(body) == 0 {
		return &rpc.Status{Code: 13, Message: fmt.Sprintf("http %d", httpCode)}
	}
	var ce struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &ce); err == nil && ce.Code != 0 {
		return &rpc.Status{Code: int32(ce.Code), Message: ce.Message}
	}
	return &rpc.Status{Code: 13, Message: fmt.Sprintf("upstream error (http %d)", httpCode)}
}

// byteReader wraps a []byte in an io.Reader without copying.
func byteReader(b []byte) *br { return &br{b: b} }

type br struct {
	b   []byte
	off int
}

func (r *br) Read(p []byte) (int, error) {
	if r.off >= len(r.b) {
		return 0, errEOF
	}
	n := copy(p, r.b[r.off:])
	r.off += n
	return n, nil
}

var errEOF = errEOFT{}

type errEOFT struct{}

func (errEOFT) Error() string { return "EOF" }

func readAll(r interface{ Read([]byte) (int, error) }) ([]byte, error) {
	out := make([]byte, 0, 1024)
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if n > 0 {
			out = append(out, buf[:n]...)
		}
		if err != nil {
			if _, isEOF := err.(errEOFT); isEOF || err.Error() == "EOF" {
				return out, nil
			}
			return out, err
		}
	}
}
