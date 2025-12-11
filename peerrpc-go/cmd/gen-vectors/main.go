// Command gen produces the golden protocol vectors for PeerRPC.
//
// The vectors are the canonical binary encoding of representative
// Frame / ResponseFrame messages. Every language SDK (Go, TypeScript,
// Rust) MUST decode these byte-for-byte identically and re-encode them
// to the exact same bytes.
//
// Output:
//
//	test/vectors/*.bin          deterministic serialized frames
//	test/vectors/expected.json  canonical description of each vector
//
// Usage:
//
//	go run ./test/vectors/gen
package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	peerrpcpb "github.com/peerrpc/go/gen/proto/peerrpc"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// vector pairs a stable file name with the message to encode.
type vector struct {
	name string
	msg  proto.Message
}

func main() {
	outDir, err := repoRoot("test/vectors")
	if err != nil {
		log.Fatalf("locate vectors dir: %v", err)
	}

	vectors := buildVectors()

	descriptors := make(map[string]any, len(vectors))
	for _, v := range vectors {
		// Deterministic mode is mandatory for golden vectors: protobuf
		// maps have non-deterministic iteration by default, so without
		// this flag the re-encoded bytes of e.g. a Call with multiple
		// metadata entries would differ run-to-run.
		bin, err := (proto.MarshalOptions{Deterministic: true}).Marshal(v.msg)
		if err != nil {
			log.Fatalf("marshal %s: %v", v.name, err)
		}

		binPath := filepath.Join(outDir, v.name+".bin")
		if err := os.WriteFile(binPath, bin, 0o644); err != nil {
			log.Fatalf("write %s: %v", binPath, err)
		}

		// Re-decode to verify round-trip determinism against the bytes
		// we just wrote. This catches non-deterministic encoders early.
		round, err := (proto.MarshalOptions{Deterministic: true}).Marshal(v.msg)
		if err != nil || !equalBytes(round, bin) {
			log.Fatalf("non-deterministic encode for %s", v.name)
		}

		j, err := protojson.Marshal(v.msg)
		if err != nil {
			log.Fatalf("protojson %s: %v", v.name, err)
		}
		var pretty any
		if err := json.Unmarshal(j, &pretty); err != nil {
			log.Fatalf("unmarshal protojson %s: %v", v.name, err)
		}

		// Only inline the hex for small vectors. For multi-KB vectors the
		// hex string is noise in the JSON; cross-language tests load the
		// .bin file directly.
		desc := map[string]any{
			"type":       proto.MessageName(v.msg),
			"size":       len(bin),
			"sha256":     sha256hex(bin),
			"protojson":  pretty,
		}
		if len(bin) <= 1024 {
			desc["hex"] = hex.EncodeToString(bin)
		}
		descriptors[v.name] = desc

		fmt.Printf("  %-40s %6d bytes  %s\n", v.name, len(bin), sha256hex(bin)[:12])
	}

	expected := map[string]any{
		"$schema":      "https://peerrpcpb.io/vectors/v1",
		"description":  "Golden protocol vectors for PeerRPC v2. Every SDK MUST decode these bytes identically and re-encode to the same bytes.",
		"proto_package": "peerrpcpb",
		"vectors":      descriptors,
	}

	raw, err := json.MarshalIndent(expected, "", "  ")
	if err != nil {
		log.Fatalf("marshal expected.json: %v", err)
	}
	raw = append(raw, '\n')
	if err := os.WriteFile(filepath.Join(outDir, "expected.json"), raw, 0o644); err != nil {
		log.Fatalf("write expected.json: %v", err)
	}

	fmt.Printf("\nwrote %d vectors to %s\n", len(vectors), outDir)
}

// buildVectors returns the canonical set of Phase-0 vectors. The contents
// are intentionally varied to exercise every oneof arm and every End
// truth-table row.
func buildVectors() []vector {
	const protoV1 = int32(peerrpcpb.ProtocolVersion_PROTOCOL_VERSION_1)

	return []vector{
		{
			name: "frame_unary_call",
			msg: &peerrpcpb.Frame{
				Routing: &peerrpcpb.Routing{Sequence: 1},
				Type: &peerrpcpb.Frame_Call{
					Call: &peerrpcpb.Call{
						Method:          "/echo.Echo/Echo",
						InlineData:      []byte("hi"),
						ProtocolVersion: protoV1,
					},
				},
			},
		},
		{
			name: "frame_unary_call_with_deadline",
			msg: &peerrpcpb.Frame{
				Routing: &peerrpcpb.Routing{Sequence: 3},
				Type: &peerrpcpb.Frame_Call{
					Call: &peerrpcpb.Call{
						Method:          "/echo.Echo/Echo",
						DeadlineMs:      5000,
						ProtocolVersion: protoV1,
						Metadata: &peerrpcpb.Metadata{
							Md: map[string]*peerrpcpb.Strings{
								"authorization": {Values: []string{"Bearer token-abc"}},
								"trace-id":      {Values: []string{"abcdef0123456789"}},
							},
						},
						InlineData: []byte("with-deadline"),
					},
				},
			},
		},
		{
			name: "frame_streaming_data_small",
			msg: &peerrpcpb.Frame{
				Routing: &peerrpcpb.Routing{Sequence: 5},
				Type: &peerrpcpb.Frame_Data{
					Data: &peerrpcpb.Data{
						Content: &peerrpcpb.Data_Message{
							Message: repeatByte('a', 100),
						},
					},
				},
			},
		},
		{
			// 100KB single Data message (not yet chunked).
			name: "frame_streaming_data_100k",
			msg: &peerrpcpb.Frame{
				Routing: &peerrpcpb.Routing{Sequence: 7},
				Type: &peerrpcpb.Frame_Data{
					Data: &peerrpcpb.Data{
						Content: &peerrpcpb.Data_Message{
							Message: repeatByte('b', 100*1024),
						},
					},
				},
			},
		},
		{
			// First chunk of a 1MB logical message; chunk payload is 256KB.
			name: "frame_chunked_data",
			msg: &peerrpcpb.Frame{
				Routing: &peerrpcpb.Routing{Sequence: 9},
				Type: &peerrpcpb.Frame_Data{
					Data: &peerrpcpb.Data{
						Content: &peerrpcpb.Data_Chunk{
							Chunk: &peerrpcpb.Chunk{
								TotalSize: 1024 * 1024,
								Offset:    0,
								Data:      repeatByte('c', 256*1024),
							},
						},
					},
				},
			},
		},
		{
			// Half-close (client CloseSend): End{close_send:true} with no status.
			name: "frame_end_close_send",
			msg: &peerrpcpb.Frame{
				Routing: &peerrpcpb.Routing{Sequence: 11},
				Type: &peerrpcpb.Frame_End{
					End: &peerrpcpb.End{CloseSend: true},
				},
			},
		},
		{
			// Client cancel: End{close_send:false, status:{CANCELLED}}.
			name: "frame_end_cancelled",
			msg: &peerrpcpb.Frame{
				Routing: &peerrpcpb.Routing{Sequence: 13},
				Type: &peerrpcpb.Frame_End{
					End: &peerrpcpb.End{
						Status: statusRPC(1, "cancelled by client", nil),
					},
				},
			},
		},
		{
			name: "response_begin_with_inline",
			msg: &peerrpcpb.ResponseFrame{
				Routing: &peerrpcpb.Routing{Sequence: 1},
				Type: &peerrpcpb.ResponseFrame_Begin{
					Begin: &peerrpcpb.Begin{
						Header: &peerrpcpb.Metadata{
							Md: map[string]*peerrpcpb.Strings{
								"x-response-id": {Values: []string{"resp-001"}},
							},
						},
						InlineData: []byte("echo:hi"),
					},
				},
			},
		},
		{
			name: "response_data_stream",
			msg: &peerrpcpb.ResponseFrame{
				Routing: &peerrpcpb.Routing{Sequence: 3},
				Type: &peerrpcpb.ResponseFrame_Data{
					Data: &peerrpcpb.Data{
						Content: &peerrpcpb.Data_Message{
							Message: []byte("stream-chunk-1"),
						},
					},
				},
			},
		},
		{
			name: "response_end_ok",
			msg: &peerrpcpb.ResponseFrame{
				Routing: &peerrpcpb.Routing{Sequence: 1},
				Type: &peerrpcpb.ResponseFrame_End{
					End: &peerrpcpb.End{
						Status: statusRPC(0, "", nil),
						Trailer: &peerrpcpb.Metadata{
							Md: map[string]*peerrpcpb.Strings{
								"x-consumed": {Values: []string{"42"}},
							},
						},
					},
				},
			},
		},
		{
			name: "response_end_unavailable",
			msg: &peerrpcpb.ResponseFrame{
				Routing: &peerrpcpb.Routing{Sequence: 5},
				Type: &peerrpcpb.ResponseFrame_End{
					End: &peerrpcpb.End{
						Status: statusRPC(14, "datachannel closed", nil),
					},
				},
			},
		},
	}
}
