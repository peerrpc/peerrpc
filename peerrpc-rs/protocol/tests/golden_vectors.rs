//! Cross-language golden vector validation.
//!
//! This test reads the same .bin files the Go generator produces
//! at ../../test/vectors/*.bin. These files contain raw protobuf
//! payload bytes (NOT length-prefixed) — the Go generator uses
//! proto.MarshalOptions{Deterministic: true}.
//!
//! We decode each vector via prost to prove the Rust SDK can parse
//! every frame the Go/TS SDKs produce. Re-encoding byte-for-byte
//! equality is not asserted for map-containing vectors because
//! protobuf map iteration order is non-deterministic in prost (the
//! Go generator uses Deterministic mode which sorts map keys; prost
//! does not offer that mode). The Go↔Go and TS↔TS tests cover
//! byte-level determinism within each language; the cross-language
//! test covers decode compatibility.

use prost::Message;
use peerrpc_protocol::{Frame, ResponseFrame};

#[test]
fn golden_vectors_decode() {
    let vectors_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test/vectors");

    if !vectors_dir.exists() {
        eprintln!("golden vectors directory not found; skipping");
        return;
    }

    let mut tested = 0;
    for entry in std::fs::read_dir(&vectors_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("bin") {
            continue;
        }
        let name = path.file_stem().unwrap().to_str().unwrap().to_string();
        let raw = std::fs::read(&path).unwrap();

        let is_response = name.starts_with("response_");
        if is_response {
            let frame = ResponseFrame::decode(&raw[..])
                .unwrap_or_else(|e| panic!("decode {} failed: {}", name, e));
            assert!(frame.routing.is_some() || frame.r#type.is_some(),
                "empty frame: {}", name);
        } else {
            let frame = Frame::decode(&raw[..])
                .unwrap_or_else(|e| panic!("decode {} failed: {}", name, e));
            assert!(frame.routing.is_some() || frame.r#type.is_some(),
                "empty frame: {}", name);
        }
        tested += 1;
    }

    assert!(tested >= 10, "expected >= 10 golden vectors, got {}", tested);
}

/// For vectors without map fields (the majority), re-encoding
/// DOES produce byte-identical output. This test covers those.
#[test]
fn golden_vectors_roundtrip_no_maps() {
    let vectors_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test/vectors");

    if !vectors_dir.exists() {
        return;
    }

    // Vectors that contain Metadata maps (non-deterministic order
    // in prost) are excluded from the byte-level check.
    let skip = [
        "frame_unary_call_with_deadline",
        "response_begin_with_inline",
        "response_end_ok",
    ];

    let mut tested = 0;
    for entry in std::fs::read_dir(&vectors_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("bin") {
            continue;
        }
        let name = path.file_stem().unwrap().to_str().unwrap().to_string();
        if skip.contains(&name.as_str()) {
            continue;
        }
        let raw = std::fs::read(&path).unwrap();

        let is_response = name.starts_with("response_");
        let reencoded: Vec<u8>;

        if is_response {
            let frame = ResponseFrame::decode(&raw[..]).unwrap();
            reencoded = frame.encode_to_vec();
        } else {
            let frame = Frame::decode(&raw[..]).unwrap();
            reencoded = frame.encode_to_vec();
        }

        assert_eq!(reencoded, raw, "byte mismatch for {}", name);
        tested += 1;
    }

    assert!(tested >= 5, "expected >= 5 non-map vectors, got {}", tested);
}

#[test]
fn frame_unary_call_decodes() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../test/vectors/frame_unary_call.bin");

    if !path.exists() {
        eprintln!("frame_unary_call.bin not found; skipping");
        return;
    }

    let raw = std::fs::read(&path).unwrap();
    let frame = Frame::decode(&raw[..]).unwrap();

    assert_eq!(frame.routing.as_ref().unwrap().sequence, 1);
    match frame.r#type {
        Some(peerrpc_protocol::gen::frame::Type::Call(ref call)) => {
            assert_eq!(call.method, "/echo.Echo/Echo");
            assert_eq!(call.protocol_version, 1);
        }
        other => panic!("expected Call, got {:?}", other),
    }
}
