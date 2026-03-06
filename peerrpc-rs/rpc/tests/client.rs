//! RPC client tests with an in-memory mock transport.
//!
//! These tests prove the Client's multiplexing, sequence allocation,
//! and response dispatch work correctly without a real WebRTC
//! connection.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use peerrpc_protocol::{
    encode_response_frame, gen, Begin, Data, End, ResponseFrame, Routing,
};
use peerrpc_protocol::google::rpc::Status as WireStatus;
use peerrpc_rpc::{Client, WireTransport, RpcError};
use tokio::sync::Mutex;

/// MockTransport holds an inbound queue of pre-built response frames
/// that the test pumps in. Each recv_frame() call pops one entry.
struct MockTransport {
    inbound: Arc<Mutex<Vec<Bytes>>>,
    sent: Arc<AtomicUsize>,
}

#[async_trait]
impl WireTransport for MockTransport {
    async fn send_frame(&mut self, _frame: Bytes) {
        self.sent.fetch_add(1, Ordering::SeqCst);
    }

    async fn recv_frame(&mut self) -> Option<Bytes> {
        let mut q = self.inbound.lock().await;
        if q.is_empty() {
            // Block forever — the test will push frames before calling.
            std::future::pending::<()>().await;
        }
        q.remove(0).into()
    }
}

/// Helper: build a ResponseFrame with Begin + inline data + End.
fn make_unary_response(seq: i32, data: &[u8]) -> Vec<Bytes> {
    vec![
        encode_response_frame(&ResponseFrame {
            routing: Some(Routing { sequence: seq }),
            r#type: Some(gen::response_frame::Type::Begin(Begin {
                inline_data: Some(data.to_vec()),
                ..Default::default()
            })),
        }),
        encode_response_frame(&ResponseFrame {
            routing: Some(Routing { sequence: seq }),
            r#type: Some(gen::response_frame::Type::End(End {
                status: Some(WireStatus { code: 0, message: "".into(), details: vec![] }),
                ..Default::default()
            })),
        }),
    ]
}

/// Helper: build a ResponseFrame sequence for client streaming:
/// Begin → Data × N → End.
fn make_client_streaming_response(seq: i32, chunks: &[Vec<u8>]) -> Vec<Bytes> {
    let mut frames = vec![
        encode_response_frame(&ResponseFrame {
            routing: Some(Routing { sequence: seq }),
            r#type: Some(gen::response_frame::Type::Begin(Begin::default())),
        }),
    ];
    for chunk in chunks {
        frames.push(encode_response_frame(&ResponseFrame {
            routing: Some(Routing { sequence: seq }),
            r#type: Some(gen::response_frame::Type::Data(Data {
                content: Some(gen::data::Content::Message(chunk.clone())),
            })),
        }));
    }
    frames.push(encode_response_frame(&ResponseFrame {
        routing: Some(Routing { sequence: seq }),
        r#type: Some(gen::response_frame::Type::End(End {
            status: Some(WireStatus { code: 0, message: "".into(), details: vec![] }),
            ..Default::default()
        })),
    }));
    frames
}

fn make_streaming_response(seq: i32, chunks: &[Vec<u8>]) -> Vec<Bytes> {
    let mut frames = vec![
        encode_response_frame(&ResponseFrame {
            routing: Some(Routing { sequence: seq }),
            r#type: Some(gen::response_frame::Type::Begin(Begin::default())),
        }),
    ];
    for chunk in chunks {
        frames.push(encode_response_frame(&ResponseFrame {
            routing: Some(Routing { sequence: seq }),
            r#type: Some(gen::response_frame::Type::Data(Data {
                content: Some(gen::data::Content::Message(chunk.clone())),
            })),
        }));
    }
    frames.push(encode_response_frame(&ResponseFrame {
        routing: Some(Routing { sequence: seq }),
        r#type: Some(gen::response_frame::Type::End(End {
            status: Some(WireStatus { code: 0, message: "".into(), details: vec![] }),
            ..Default::default()
        })),
    }));
    frames
}

#[tokio::test]
async fn test_invoke_unary_success() {
    let inbound = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sent = Arc::new(AtomicUsize::new(0));

    let transport = MockTransport {
        inbound: inbound.clone(),
        sent: sent.clone(),
    };
    let client = Client::new(transport);

    // Push the response frames BEFORE calling invoke_unary so the
    // run loop can deliver them.
    // The sequence will be 1 (first odd number from the allocator).
    let responses = make_unary_response(1, b"echo:hello");
    {
        let mut q = inbound.lock().await;
        q.extend(responses);
    }

    let (resp, status) = client
        .invoke_unary("/echo.Echo/Echo", b"hello")
        .await
        .expect("invoke should succeed");

    assert_eq!(resp, b"echo:hello");
    assert!(status.is_ok());
    // Sent: Call + half-close End = 2 frames.
    assert_eq!(sent.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn test_invoke_unary_error_status() {
    let inbound = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let transport = MockTransport {
        inbound: inbound.clone(),
        sent: Arc::new(AtomicUsize::new(0)),
    };
    let client = Client::new(transport);

    // Push an End with non-OK status and no Begin/Data.
    {
        let mut q = inbound.lock().await;
        q.push(encode_response_frame(&ResponseFrame {
            routing: Some(Routing { sequence: 1 }),
            r#type: Some(gen::response_frame::Type::End(End {
                status: Some(WireStatus {
                    code: 12,
                    message: "unimplemented".into(),
                    details: vec![],
                }),
                ..Default::default()
            })),
        }));
    }

    let result = client.invoke_unary("/missing/Method", b"x").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        RpcError::Status { code, message } => {
            assert_eq!(code, 12);
            assert!(message.contains("unimplemented"));
        }
        other => panic!("expected Status error, got {other:?}"),
    }
}

#[tokio::test]
async fn test_invoke_server_streaming() {
    let inbound = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let transport = MockTransport {
        inbound: inbound.clone(),
        sent: Arc::new(AtomicUsize::new(0)),
    };
    let client = Client::new(transport);

    let chunks: Vec<Vec<u8>> = (0..5).map(|i| format!("chunk-{i}").into_bytes()).collect();
    let responses = make_streaming_response(1, &chunks);
    {
        let mut q = inbound.lock().await;
        q.extend(responses);
    }

    let mut stream = client
        .invoke_server_streaming("/echo.Echo/Stream", b"req")
        .await
        .expect("open stream");

    let mut received = Vec::new();
    while let Some(data) = stream.recv().await {
        received.push(data);
    }
    let status = stream.wait_end().await;
    assert_eq!(received.len(), 5);
    assert_eq!(received[0], b"chunk-0");
    assert_eq!(received[4], b"chunk-4");
    assert!(status.is_ok());
}

#[tokio::test]
async fn test_concurrent_unary_calls() {
    let inbound = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let transport = MockTransport {
        inbound: inbound.clone(),
        sent: Arc::new(AtomicUsize::new(0)),
    };
    let client = Client::new(transport);

    // Pre-push responses for seq 1 and 3 (odd, +2 each).
    {
        let mut q = inbound.lock().await;
        q.extend(make_unary_response(1, b"resp-1"));
        q.extend(make_unary_response(3, b"resp-3"));
    }

    let c1 = client.clone();
    let c2 = client.clone();

    let (r1, r2) = tokio::join!(
        c1.invoke_unary("/test/A", b"req-1"),
        c2.invoke_unary("/test/B", b"req-2"),
    );

    let (resp1, _) = r1.unwrap();
    let (resp2, _) = r2.unwrap();
    assert_eq!(resp1, b"resp-1");
    assert_eq!(resp2, b"resp-3");
}

#[tokio::test]
async fn test_large_payload_chunking() {
    let inbound = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let sent = Arc::new(AtomicUsize::new(0));
    let transport = MockTransport {
        inbound: inbound.clone(),
        sent: sent.clone(),
    };
    let client = Client::new(transport);

    // Push a response.
    {
        let mut q = inbound.lock().await;
        q.extend(make_unary_response(1, b"big-response"));
    }

    // Send a 1 MB payload (forces chunking on the outbound).
    let big_req = vec![0xAA; 1024 * 1024];
    let (resp, status) = client
        .invoke_unary("/test/Big", &big_req)
        .await
        .unwrap();
    assert_eq!(resp, b"big-response");
    assert!(status.is_ok());
    // Sent: Call + N chunks + End. At least 3 frames.
    assert!(sent.load(Ordering::SeqCst) >= 3);
}

#[tokio::test]
async fn test_invoke_client_streaming() {
    let inbound = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let transport = MockTransport {
        inbound: inbound.clone(),
        sent: Arc::new(AtomicUsize::new(0)),
    };
    let client = Client::new(transport);

    // Build server response: one concatenated reply.
    let resp_data = b"received 3 messages (15 bytes)".to_vec();
    let responses = make_client_streaming_response(1, &[resp_data]);
    {
        let mut q = inbound.lock().await;
        q.extend(responses);
    }

    let mut stream = client
        .invoke_client_streaming("/echo.Echo/Collect", Some(b"first"))
        .await
        .expect("open client stream");

    // Send additional chunks.
    stream.send(b"chunk-1").expect("send 1");
    stream.send(b"chunk-2").expect("send 2");
    stream.close_send().expect("close_send");

    let resp = stream.recv().await.expect("should have a response");
    assert_eq!(
        String::from_utf8_lossy(&resp),
        "received 3 messages (15 bytes)"
    );

    // Next recv returns None (EOF).
    assert!(stream.recv().await.is_none());

    let status = stream.wait_end().await;
    assert!(status.is_ok());
}

#[tokio::test]
async fn test_invoke_bidi_streaming() {
    let inbound = Arc::new(Mutex::new(Vec::<Bytes>::new()));
    let transport = MockTransport {
        inbound: inbound.clone(),
        sent: Arc::new(AtomicUsize::new(0)),
    };
    let client = Client::new(transport);

    // Build a sequence of interleaved responses:
    // Begin → Data(ack-1) → Data(ack-2) → Data(ack-3) → End
    let responses = make_client_streaming_response(
        1,
        &[
            b"ack 1: msg-1".to_vec(),
            b"ack 2: msg-2".to_vec(),
            b"ack 3: msg-3".to_vec(),
        ],
    );
    {
        let mut q = inbound.lock().await;
        q.extend(responses);
    }

    let mut stream = client
        .invoke_bidi_streaming("/echo.Echo/Chat")
        .await
        .expect("open bidi stream");

    // Interleave send + recv (true bidi pattern).
    for i in 1..=3 {
        stream.send(format!("msg-{}", i).as_bytes()).expect("send");
        let resp = stream.recv().await.expect("should have a response");
        assert_eq!(
            String::from_utf8_lossy(&resp),
            format!("ack {}: msg-{}", i, i)
        );
    }

    stream.close_send().expect("close_send");

    // After close_send, recv should return None (EOF).
    assert!(stream.recv().await.is_none());

    let status = stream.wait_end().await;
    assert!(status.is_ok());
}
