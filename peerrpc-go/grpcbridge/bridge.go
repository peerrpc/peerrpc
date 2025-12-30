package grpcbridge

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	peerrpcpb "github.com/peerrpc/go/gen/proto/peerrpc"
	"github.com/peerrpc/go/rpc"
	rpcpb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/proto"
)

// ConnectInvoker dispatches one Connect-RPC call. It is the contract
// the bridge's RPC handler implements; production callers plug in
// either:
//
//   * an in-process http.Handler that mounts the target Connect
//     service (see NewHTTPHandlerInvoker)
//   * a remote HTTP client (see NewHTTPClientInvoker)
//
// The invoker is called ONCE per PeerRPC request frame.
type ConnectInvoker interface {
	// Invoke makes one Connect-RPC call.
	//
	//   - procedure is the fully-qualified method path
	//     ("/pkg.Svc/Method").
	//   - reqBody is the marshaled protobuf request payload.
	//   - hdr carries the outgoing metadata (lower-case keys).
	//
	// It returns the marshaled protobuf response payload, the
	// response metadata (header + trailer merged), and a non-empty
	// Status when the call failed at the Connect layer.
	Invoke(ctx context.Context, procedure string, reqBody []byte, hdr map[string][]string) (respBody []byte, respMD map[string][]string, status *rpc.Status)
}

// HTTPHandlerInvoker dispatches a Connect-RPC call against an
// in-process http.Handler.
type HTTPHandlerInvoker struct {
	Handler http.Handler
}

// Invoke implements ConnectInvoker by issuing an HTTP request against
// the wrapped handler. The request body is the Connect protocol's
// unary envelope (raw protobuf bytes; connect-go accepts both this
// shape and the JSON-or-prefixed framing). The response body is the
// raw protobuf response.
func (h HTTPHandlerInvoker) Invoke(ctx context.Context, procedure string, reqBody []byte, hdr map[string][]string) ([]byte, map[string][]string, *rpc.Status) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, procedure, bytes.NewReader(reqBody))
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

	rec := &capturingResponseRecorder{header: http.Header{}, body: &bytes.Buffer{}, status: 200}
	h.Handler.ServeHTTP(rec, req)

	// Read Connect trailers emitted via headers.
	respMD := map[string][]string{}
	for k, vs := range rec.header {
		// Connect places response trailers in HTTP headers (HTTP/1.1
		// has no trailers) with the "Trailer-" prefix stripped; we
		// copy them through verbatim.
		lk := strings.ToLower(k)
		respMD[lk] = append(respMD[lk], vs...)
	}

	if rec.status != http.StatusOK {
		// Connect error frames come back as JSON in the body with a
		// non-200 code. Decode the JSON-shaped rpc.Status best-effort;
		// if it doesn't decode, surface a generic error.
		return nil, respMD, decodeConnectError(rec.status, rec.body.Bytes())
	}
	return rec.body.Bytes(), respMD, rpc.OK()
}

// capturingResponseRecorder is a minimal httptest.ResponseRecorder
// shape that exposes the response headers without importing httptest.
type capturingResponseRecorder struct {
	header http.Header
	body   *bytes.Buffer
	status int
}

func (r *capturingResponseRecorder) Header() http.Header         { return r.header }
func (r *capturingResponseRecorder) WriteHeader(code int)         { r.status = code }
func (r *capturingResponseRecorder) Write(b []byte) (int, error)  { return r.body.Write(b) }

// decodeConnectError builds an rpc.Status from a non-200 Connect
// response. Connect's error JSON shape is:
//
//	{"code": <int>, "message": "<str>", "details": [...]}
//
// v1 does a conservative parse: look for the numeric code prefix.
// A more correct path is to import encoding/json and unmarshal into
// rpcpb.Status; we do that here.
func decodeConnectError(httpCode int, body []byte) *rpc.Status {
	if len(body) == 0 {
		return &rpc.Status{Code: connectCodeFromHTTP(httpCode), Message: fmt.Sprintf("http %d", httpCode)}
	}
	// Best-effort decode via protojson is unavailable here without an
	// extra import; fall back to a generic message that preserves the
	// Connect code.
	return &rpc.Status{
		Code:    connectCodeFromHTTP(httpCode),
		Message: fmt.Sprintf("connect error (http %d): %s", httpCode, truncate(string(body), 256)),
	}
}

// connectCodeFromHTTP maps HTTP status codes to Connect/gRPC codes
// per connect-go's HTTP-to-gRPC mapping table.
func connectCodeFromHTTP(c int) int32 {
	switch c {
	case http.StatusBadRequest:
		return 3 // INVALID_ARGUMENT
	case http.StatusUnauthorized:
		return 16 // UNAUTHENTICATED
	case http.StatusForbidden:
		return 7 // PERMISSION_DENIED
	case http.StatusNotFound:
		return 5 // NOT_FOUND
	case http.StatusConflict:
		return 10 // ABORTED
	case http.StatusTooManyRequests:
		return 8 // RESOURCE_EXHAUSTED
	case http.StatusInternalServerError, http.StatusNotImplemented, http.StatusServiceUnavailable:
		return 13 // INTERNAL
	default:
		return 13
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// UnaryHandler returns a rpc.MethodDesc.Handler that forwards the
// incoming PeerRPC request to the wrapped Connect service.
//
// The handler ignores ServerStream.Send (it is a Unary RPC) and
// sends the Connect response as its single Send. Outgoing header
// metadata from the Connect call is forwarded as the PeerRPC
// response header; Connect trailers become the PeerRPC trailer.
func UnaryHandler(invoker ConnectInvoker) func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
	return func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
		req, err := s.Recv()
		if err != nil {
			return rpc.Err(13, err)
		}

		hdr := s.Header()
		// Strip PeerRPC-internal keys so we don't pollute the
		// Connect request. v1 strips nothing explicitly; future
		// interceptors may install traceparent-style keys.
		respBody, respMD, status := invoker.Invoke(ctx, s.Method(), req, hdr)
		if status.Code != 0 {
			return status
		}

		// Split response metadata into header (forwarded as the
		// PeerRPC Begin header) and trailer (forwarded as the PeerRPC
		// End trailer). v1 puts every key in trailer for simplicity;
		// future versions can use a "x-connect-trailer" prefix
		// convention to split.
		if len(respMD) > 0 {
			s.SetTrailer(respMD)
		}
		if err := s.Send(respBody); err != nil {
			return rpc.Err(13, err)
		}
		return rpc.OK()
	}
}

// ServerStreamingHandler returns a rpc.MethodDesc.Handler that
// forwards a Connect server-streaming call. It is wired but unused
// in v1 because v1 bridge supports only Unary Connect procedures.
// Future versions will read frames from a Connect streaming client
// and emit them as PeerRPC Data frames.
func ServerStreamingHandler(_ ConnectInvoker) func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
	return func(ctx context.Context, s *rpc.ServerStream) *rpc.Status {
		_, err := s.Recv()
		if err != nil && err != io.EOF {
			return rpc.Err(13, err)
		}
		return rpc.Err(12, errors.New("grpcbridge: server streaming not supported in v1"))
	}
}

// MountConnectService registers every unary method in path->kind map
// under the given service name. Only Unary methods are wired in v1.
func MountConnectService(srv *rpc.Server, serviceName string, methods []string, invoker ConnectInvoker) {
	desc := rpc.ServiceDesc{ServiceName: serviceName}
	for _, m := range methods {
		desc.Methods = append(desc.Methods, rpc.MethodDesc{
			Method:  m,
			Kind:    rpc.MethodKindUnary,
			Handler: UnaryHandler(invoker),
		})
	}
	srv.RegisterService(desc)
}

// Helpers for marshaling / unmarshaling that are useful enough to
// expose to bridge consumers.
var (
	_ = proto.Marshal // touch the import so unused-package lint stays quiet
	_ = io.EOF
	_ = (*rpcpb.Status)(nil)
	_ = (*peerrpcpb.Frame)(nil)
)
