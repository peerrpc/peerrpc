/**
 * @file Frame codec: length-prefixed wire format matching the Go
 *       peerrpc-go/protocol package.
 *
 * Every DataChannel write is:
 *
 *   uint32 BE length  |  protobuf payload bytes
 *
 * This module is the single source of truth for encoding/decoding
 * that wire shape. The transport layer calls writeFrame / readFrame;
 * the RPC layer never touches raw bytes directly.
 */

import { Frame, ResponseFrame } from "./gen/peerrpc/peerrpc_pb.js";

/**
 * SCTP / framing thresholds.
 *
 * WebRTC DataChannels negotiate an SCTP max-message-size (256 KiB by
 * default). Each dc.send() carries one ENCODED frame = 4-byte length
 * prefix + protobuf envelope + payload. That envelope (~40 bytes for a
 * Chunk) means a payload of exactly 256 KiB would push the encoded
 * frame over the limit, so the browser rejects it with "Trying to send
 * message larger than max-message-size". MESSAGE_MAX / CHUNK_SIZE are
 * therefore sized below MAX_FRAME_SIZE to leave headroom. These are
 * transport-layer thresholds only (the wire carries total_size/offset/
 * data on Chunk), so each side may pick its own value.
 */
const MAX_FRAME_BYTES = 256 * 1024;
const FRAME_OVERHEAD = 1 * 1024; // 4-byte prefix + protobuf envelope (real ~40 B)

/** Maximum encoded frame size (the negotiated SCTP max-message-size). */
export const MAX_FRAME_SIZE = MAX_FRAME_BYTES;

/** Inline payload threshold: requests ≤ this ride in Call.inline_data. */
export const INLINE_MAX = 16 * 1024;

/** Single Data.message threshold: payloads > this use Chunk frames. */
export const MESSAGE_MAX = MAX_FRAME_BYTES - FRAME_OVERHEAD;

/** Per-chunk payload size when fragmenting large messages. */
export const CHUNK_SIZE = MAX_FRAME_BYTES - FRAME_OVERHEAD;

/** High-watermark for outbound backpressure (bytes in SCTP buffer). */
export const BUFFERED_AMOUNT_HIGH = 1 << 20; // 1 MiB

/** DataChannel label carrying the protocol version on the wire. */
export const DATACHANNEL_LABEL = "peerrpc-v1";

/**
 * Write a length-prefixed Frame to a DataView-friendly sink.
 * Returns the raw bytes to send through the DataChannel.
 */
export function encodeFrame(frame: Frame): Uint8Array<ArrayBuffer> {
  const payload = frame.toBinary();
  return lengthPrefix(payload);
}

/**
 * Write a length-prefixed ResponseFrame.
 */
export function encodeResponseFrame(frame: ResponseFrame): Uint8Array<ArrayBuffer> {
  const payload = frame.toBinary();
  return lengthPrefix(payload);
}

/**
 * Prepend a 4-byte big-endian length header to payload.
 */
export function lengthPrefix(payload: Uint8Array): Uint8Array<ArrayBuffer> {
  const out = new Uint8Array(4 + payload.length);
  const view = new DataView(out.buffer);
  view.setUint32(0, payload.length, false); // big-endian
  out.set(payload, 4);
  return out;
}

/**
 * Decode a length-prefixed wire frame from a raw byte buffer.
 *
 * Returns the decoded Frame + the number of bytes consumed, or null
 * if the buffer does not contain a complete frame yet.
 *
 * The caller (transport layer) is responsible for buffering partial
 * reads from the DataChannel's OnMessage callback until this function
 * returns a non-null result.
 */
export function tryDecodeFrame(buf: Uint8Array): { frame: Frame; consumed: number } | null {
  return tryDecode(buf, (payload) => new Frame().fromBinary(payload));
}

/**
 * Decode a length-prefixed ResponseFrame.
 */
export function tryDecodeResponseFrame(buf: Uint8Array): { frame: ResponseFrame; consumed: number } | null {
  return tryDecode(buf, (payload) => new ResponseFrame().fromBinary(payload));
}

/**
 * Shared decode logic: read the length prefix, check completeness,
 * decode the payload via the supplied deserializer.
 */
function tryDecode<T>(buf: Uint8Array, decode: (payload: Uint8Array) => T): { frame: T; consumed: number } | null {
  if (buf.length < 4) {
    return null;
  }
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const length = view.getUint32(0, false); // big-endian
  if (length > MAX_FRAME_SIZE) {
    throw new Error(`peerrpc: frame size ${length} exceeds MAX_FRAME_SIZE ${MAX_FRAME_SIZE}`);
  }
  const total = 4 + length;
  if (buf.length < total) {
    return null;
  }
  const payload = buf.subarray(4, total);
  const frame = decode(payload);
  return { frame, consumed: total };
}

/**
 * Create a new Frame with sensible defaults.
 */
export function newFrame(init?: Partial<Frame>): Frame {
  return new Frame(init);
}

/**
 * Create a new ResponseFrame with sensible defaults.
 */
export function newResponseFrame(init?: Partial<ResponseFrame>): ResponseFrame {
  return new ResponseFrame(init);
}

// Re-export all generated types for caller convenience.
export * from "./gen/peerrpc/peerrpc_pb.js";
export * from "./gen/peerrpc/signaling/signaling_pb.js";
