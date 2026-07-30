package grpcbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/peerrpc/go/rpc"
)

// HTTPClientInvoker forwards Unary and server-streaming RPCs to an
// upstream Connect (or gRPC) service over HTTP. It implements both
// ConnectInvoker and ConnectStreamInvoker.
//
// It is the remote-HTTP counterpart of HTTPHandlerInvoker (which
// dispatches against an in-process http.Handler). Use it when the
// upstream Connect service lives in another process.
//
// Base is the upstream base URL (e.g. "http://localhost:9090"); each
// procedure path is appended verbatim. Client defaults to an
// http.Client with a 30s timeout when nil.
type HTTPClientInvoker struct {
	Base   string
	Client *http.Client
}

func (a *HTTPClientInvoker) client() *http.Client {
	if a.Client != nil {
		return a.Client
	}
	return &http.Client{Timeout: 30 * time.Second}
}

// Invoke implements ConnectInvoker for Unary RPCs.
func (a *HTTPClientInvoker) Invoke(ctx context.Context, procedure string, reqBody []byte, hdr map[string][]string) ([]byte, map[string][]string, *rpc.Status) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.Base+procedure, bytes.NewReader(reqBody))
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
	resp, err := a.client().Do(req)
	if err != nil {
		return nil, nil, rpc.Err(14, err)
	}
	defer resp.Body.Close()

	respMD := map[string][]string{}
	for k, vs := range resp.Header {
		lk := strings.ToLower(k)
		respMD[lk] = append(respMD[lk], vs...)
	}

	body, readErr := io.ReadAll(resp.Body)
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
// is: 1-byte flag (0x00 = data, 0x01 = error trailer) + 4-byte
// big-endian length + payload.
func (a *HTTPClientInvoker) InvokeStream(ctx context.Context, procedure string, reqBody []byte, hdr map[string][]string, send func([]byte)) *rpc.Status {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.Base+procedure, bytes.NewReader(reqBody))
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

	resp, err := a.client().Do(req)
	if err != nil {
		return rpc.Err(14, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
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
// and returns an rpc.Status. Unlike the package-level decodeConnectError
// (which maps HTTP codes to gRPC codes), this preserves the upstream
// Connect error code verbatim and falls back to code 13 (INTERNAL).
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

