//! PeerRPC wire protocol: length-prefixed frame codec + generated
//! protobuf types.
//!
//! Every DataChannel write is:
//!
//! ```text
//! uint32 BE length  |  protobuf payload bytes
//! ```
//!
//! This crate is the Rust counterpart of `peerrpc-go/protocol` and
//! `peerrpc-ts/packages/peerrpc-protocol`. All three MUST produce
//! byte-identical encodings for the golden test vectors.

// The generated peerrpc.rs references super::google::rpc::Status,
// so the crate root needs a `google` module visible to `gen`.
pub mod google {
    pub mod rpc {
        include!(concat!(env!("OUT_DIR"), "/google.rpc.rs"));
    }
}

pub mod gen {
    // The main peerrpc package.
    include!(concat!(env!("OUT_DIR"), "/peerrpc.rs"));
    // The signaling package.
    pub mod signaling {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/peerrpc.signaling.v1.rs"));
        }
    }
}

// Re-export prost_types so the generated code's `::prost_types` path
// resolves.
pub use prost_types;

// Re-export the most-used types at the crate root for convenience.
pub use gen::{
    Begin, Call, Chunk, Data, End, Frame, Metadata, ResponseFrame, Routing, Strings,
};
pub use gen::signaling::v1::{
    IceCandidate, JoinRequest, SdpAnswer, SdpOffer, SignalMessage,
};
pub use google::rpc::Status;

// ─── Wire thresholds (match Go/TS) ───────────────────────────

/// Inline payload threshold: requests ≤ this ride in Call.inline_data.
pub const INLINE_MAX: usize = 16 * 1024;

/// Single Data.message threshold: payloads > this use Chunk frames.
pub const MESSAGE_MAX: usize = 256 * 1024;

/// Per-chunk payload size when fragmenting large messages.
pub const CHUNK_SIZE: usize = 256 * 1024;

/// Maximum payload size for a single length-prefixed frame.
pub const MAX_FRAME_SIZE: usize = 256 * 1024;

/// High-watermark for outbound backpressure (bytes in SCTP buffer).
pub const BUFFERED_AMOUNT_HIGH: u64 = 1 << 20; // 1 MiB

/// DataChannel label carrying the protocol version on the wire.
pub const DATACHANNEL_LABEL: &str = "peerrpc-v1";

// ─── Length-prefixed codec ───────────────────────────────────

use bytes::{BufMut, Bytes, BytesMut};
use prost::Message;
use thiserror::Error;

/// Encode a protobuf payload with a 4-byte big-endian length prefix.
pub fn length_prefix(payload: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(4 + payload.len());
    out.put_u32(payload.len() as u32);
    out.put_slice(payload);
    out.freeze()
}

/// Encode a Frame into length-prefixed wire bytes.
pub fn encode_frame(frame: &Frame) -> Bytes {
    let mut buf = Vec::new();
    prost::Message::encode(frame, &mut buf).expect("encode frame");
    length_prefix(&buf)
}

/// Encode a ResponseFrame into length-prefixed wire bytes.
pub fn encode_response_frame(frame: &ResponseFrame) -> Bytes {
    let mut buf = Vec::new();
    prost::Message::encode(frame, &mut buf).expect("encode response frame");
    length_prefix(&buf)
}

/// Attempt to decode a length-prefixed Frame from a byte buffer.
/// Returns `Ok(Some((frame, consumed)))` on success, `Ok(None)` if
/// the buffer does not contain a complete frame yet.
pub fn try_decode_frame(buf: &[u8]) -> Result<Option<(Frame, usize)>, DecodeError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if length > MAX_FRAME_SIZE {
        return Err(DecodeError::Oversized { length, max: MAX_FRAME_SIZE });
    }
    let total = 4 + length;
    if buf.len() < total {
        return Ok(None);
    }
    let frame = Frame::decode(&buf[4..total])?;
    Ok(Some((frame, total)))
}

/// Attempt to decode a length-prefixed ResponseFrame.
pub fn try_decode_response_frame(buf: &[u8]) -> Result<Option<(ResponseFrame, usize)>, DecodeError> {
    if buf.len() < 4 {
        return Ok(None);
    }
    let length = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if length > MAX_FRAME_SIZE {
        return Err(DecodeError::Oversized { length, max: MAX_FRAME_SIZE });
    }
    let total = 4 + length;
    if buf.len() < total {
        return Ok(None);
    }
    let frame = ResponseFrame::decode(&buf[4..total])?;
    Ok(Some((frame, total)))
}

/// Errors that can occur during frame decoding.
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("frame size {length} exceeds max {max}")]
    Oversized { length: usize, max: usize },
    #[error("protobuf decode: {0}")]
    Prost(#[from] prost::DecodeError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_length_prefix() {
        let payload = [0x01, 0x02, 0x03];
        let out = length_prefix(&payload);
        assert_eq!(out.len(), 7);
        assert_eq!(&out[..4], &[0, 0, 0, 3]);
        assert_eq!(&out[4..], &payload);
    }

    #[test]
    fn test_encode_decode_frame_roundtrip() {
        let frame = Frame {
            routing: Some(Routing { sequence: 42 }),
            r#type: Some(gen::frame::Type::Call(Call {
                method: "/test.Foo/Bar".into(),
                protocol_version: 1,
                ..Default::default()
            })),
        };
        let encoded = encode_frame(&frame);
        let decoded = try_decode_frame(&encoded).unwrap().unwrap();
        assert_eq!(decoded.1, encoded.len());
        assert_eq!(decoded.0.routing.unwrap().sequence, 42);
    }

    #[test]
    fn test_partial_buffer_returns_none() {
        let frame = Frame {
            routing: Some(Routing { sequence: 1 }),
            r#type: Some(gen::frame::Type::End(End {
                close_send: true,
                ..Default::default()
            })),
        };
        let encoded = encode_frame(&frame);
        let partial = &encoded[..5];
        assert!(try_decode_frame(partial).unwrap().is_none());
    }

    #[test]
    fn test_two_back_to_back_frames() {
        let f1 = Frame {
            routing: Some(Routing { sequence: 1 }),
            r#type: Some(gen::frame::Type::Call(Call {
                method: "/a/B".into(),
                protocol_version: 1,
                ..Default::default()
            })),
        };
        let f2 = Frame {
            routing: Some(Routing { sequence: 3 }),
            r#type: Some(gen::frame::Type::Call(Call {
                method: "/c/D".into(),
                protocol_version: 1,
                ..Default::default()
            })),
        };
        let e1 = encode_frame(&f1);
        let e2 = encode_frame(&f2);
        let mut combined = Vec::with_capacity(e1.len() + e2.len());
        combined.extend_from_slice(&e1);
        combined.extend_from_slice(&e2);

        let (d1, consumed1) = try_decode_frame(&combined).unwrap().unwrap();
        assert_eq!(d1.routing.unwrap().sequence, 1);

        let (d2, _) = try_decode_frame(&combined[consumed1..]).unwrap().unwrap();
        assert_eq!(d2.routing.unwrap().sequence, 3);
    }
}
