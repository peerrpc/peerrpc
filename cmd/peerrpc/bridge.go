package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
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
	"github.com/spf13/cobra"
)

var bridgeCmd = &cobra.Command{
	Use:   "bridge",
	Short: "Run the gRPC-Connect bridge",
	Long: `Start the PeerRPC -> Connect/gRPC bridge.

Registers PeerRPC service stubs that forward incoming calls to a
remote Connect (or gRPC) service URL over HTTP.`,
	RunE: runBridge,
}

var bridgeFlags struct {
	upstream      string
	signalAddr    string
	room          string
	peerID        string
	role          string
	services      []string
	streamServices []string
}

func init() {
	f := bridgeCmd.Flags()
	f.StringVar(&bridgeFlags.upstream, "upstream", "", "Connect service base URL")
	f.StringVar(&bridgeFlags.signalAddr, "signal", "", "signal server base URL")
	// Signaling room id. Named -room (not -service) to avoid colliding
	// with the repeatable -service flag below and to match the
	// standalone grpcbridge-server binary and docs/getting-started.md.
	f.StringVar(&bridgeFlags.room, "room", "", "signaling room id")
	f.StringVar(&bridgeFlags.peerID, "peer-id", "bridge", "peer id")
	f.StringVar(&bridgeFlags.role, "role", "answerer", "peer role (offerer|answerer)")
	f.StringArrayVar(&bridgeFlags.services, "service", nil, `unary service spec "Name:Method1,Method2"`)
	f.StringArrayVar(&bridgeFlags.streamServices, "stream-service", nil, `server-streaming service spec "Name:Method1,Method2"`)
	_ = bridgeCmd.MarkFlagRequired("upstream")
	_ = bridgeCmd.MarkFlagRequired("room")
}

func runBridge(_ *cobra.Command, _ []string) error {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	invoker := &httpClientInvokerAdapter{
		base: bridgeFlags.upstream,
		c:    &http.Client{Timeout: 30 * time.Second},
	}

	peerrpcSrv := rpc.NewServer()

	for _, spec := range bridgeFlags.services {
		name, methods, err := bridge.ParseServiceSpec(spec)
		if err != nil {
			logger.Error("bad --service spec", "spec", spec, "err", err)
			return err
		}
		grpcbridge.MountConnectService(peerrpcSrv, name, methods, invoker)
		logger.Info("mounted unary service", "name", name, "methods", methods)
	}

	for _, spec := range bridgeFlags.streamServices {
		name, methods, err := bridge.ParseServiceSpec(spec)
		if err != nil {
			logger.Error("bad --stream-service spec", "spec", spec, "err", err)
			return err
		}
		grpcbridge.MountConnectServiceWithStreaming(
			peerrpcSrv, name, methods, methods,
			invoker, invoker,
		)
		logger.Info("mounted streaming service", "name", name, "methods", methods)
	}

	var backend signalsdk.Backend
	if bridgeFlags.signalAddr != "" {
		backend = signalsdk.NewRemote(bridgeFlags.signalAddr)
		logger.Info("using remote signaling", "addr", bridgeFlags.signalAddr)
	} else {
		backend = signalsdk.NewLocal()
		logger.Info("using in-process signaling")
	}

	sig, err := backend.Exchange(context.Background(), bridgeFlags.room, bridgeFlags.peerID)
	if err != nil {
		logger.Error("signaling exchange", "err", err)
		return err
	}
	defer sig.Close()

	r, err := parseRole(bridgeFlags.role)
	if err != nil {
		logger.Error("bad role", "err", err)
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	p, err := peer.New(ctx, r, peer.Config{})
	if err != nil {
		logger.Error("peer.New", "err", err)
		return err
	}
	defer p.Close()

	logger.Info("bridge starting",
		"upstream", bridgeFlags.upstream,
		"room", bridgeFlags.room,
		"peer_id", bridgeFlags.peerID,
		"role", r,
	)

	var ch *transport.Channel
	if r == signalsdk.RoleClient {
		ch, err = p.Dial(ctx, sig)
		if err != nil {
			logger.Error("Dial", "err", err)
			return err
		}
	} else {
		ch, err = p.Accept(ctx, sig)
		if err != nil {
			logger.Error("Accept", "err", err)
			return err
		}
	}

	peerrpcSrv.Serve(ctx, ch)
	return nil
}

func parseRole(s string) (signalsdk.Role, error) {
	switch s {
	case "offerer":
		return signalsdk.RoleClient, nil
	case "answerer":
		return signalsdk.RoleServer, nil
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
			return decodeConnectErrorFromStatus(http.StatusOK, msgBuf)
		}

		send(msgBuf)
	}
}

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
