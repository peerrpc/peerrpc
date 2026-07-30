//! Rust ↔ Go cross-language interop test binary.
//!
//! Connects to the Go interop server's SSE signaling endpoint,
//! accepts the WebRTC offer, establishes a DataChannel via webrtc-rs,
//! then issues Unary + Server Streaming RPCs against the Go
//! EchoService. Exits 0 on success, 1 on failure.
//!
//! Usage:
//!   peerrpc-interop-rs http://localhost:30443
//!
//! The Go server must be running with the SSE signaling endpoints
//! (/api/signal/events, /api/signal/send) and the EchoService
//! registered.

use std::sync::Arc;
use std::time::Duration;

use peerrpc_peer::Peer;
use peerrpc_rpc::Client;
use serde::{Deserialize, Serialize};
use webrtc::peer_connection::configuration::RTCConfiguration;

#[derive(Serialize, Deserialize)]
struct SignalMsg {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    sdp: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidate: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sdp_mid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sdp_mline_index: Option<i32>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:30443".to_string());

    let room = "interop";

    tracing::info!("connecting to Go interop server at {}", url);

    // 1. Connect to SSE and receive the offer from the Go server.
    tracing::info!("subscribing to SSE signaling...");
    let offer_sdp = wait_for_sse_offer(&format!("{}/api/signal/events?room={}", url, room)).await?;
    tracing::info!("received SDP offer ({} bytes)", offer_sdp.len());

    // 2. Create a webrtc-rs PeerConnection as Answerer.
    let cfg = RTCConfiguration::default();
    let (mut peer, answer_sdp) = Peer::accept_offer(cfg, offer_sdp).await?;
    tracing::info!("created SDP answer ({} bytes)", answer_sdp.len());

    // 3. Send the answer via POST (async reqwest, not blocking ureq).
    let msg = SignalMsg {
        msg_type: "answer".into(),
        sdp: Some(answer_sdp),
        candidate: None,
        sdp_mid: None,
        sdp_mline_index: None,
    };
    let post_url = format!("{}/api/signal/send?room={}", url, room);
    let http_client = reqwest::Client::new();
    let resp = http_client
        .post(&post_url)
        .json(&msg)
        .send()
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { format!("POST failed: {}", e).into() })?;
    tracing::info!("sent SDP answer via POST (HTTP {})", resp.status());

    // 4. Wait for the DataChannel to open (now that the answer has
    //    been delivered).
    tracing::info!("waiting for DataChannel...");
    let dc = peer
        .wait_for_data_channel(Duration::from_secs(30))
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { format!("DataChannel: {}", e).into() })?;
    tracing::info!("DataChannel open via {}", dc.label());

    // 5. Create the RPC client over the peer's DataChannel.
    let client = Client::new(peer);
    tracing::info!("RPC client created");

    // 5. Issue Unary RPC.
    let req = b"hello from rust";
    tracing::info!("invoking Unary: /echo.Echo/Echo");
    let (resp, status) = tokio::time::timeout(
        Duration::from_secs(10),
        client.invoke_unary("/echo.Echo/Echo", req),
    )
    .await
    .map_err(|_| -> Box<dyn std::error::Error> { "Unary timeout".into() })??;

    if !status.is_ok() {
        tracing::error!(
            "Unary RPC failed: code={} msg={}",
            status.code,
            status.message
        );
        std::process::exit(1);
    }
    let resp_str = String::from_utf8_lossy(&resp);
    tracing::info!("Unary response: {}", resp_str);
    assert!(
        resp_str.starts_with("echo: "),
        "unexpected response: {}",
        resp_str
    );
    assert!(resp_str.contains("hello from rust"));

    // 6. Issue Server Streaming RPC.
    tracing::info!("invoking Server Streaming: /echo.Echo/Stream");
    let mut stream = tokio::time::timeout(
        Duration::from_secs(10),
        client.invoke_server_streaming("/echo.Echo/Stream", b"stream-from-rust"),
    )
    .await??;

    let mut chunks = 0;
    loop {
        let result = tokio::time::timeout(Duration::from_secs(10), stream.recv()).await;
        match result {
            Ok(Some(chunk)) => {
                chunks += 1;
                let text = String::from_utf8_lossy(&chunk);
                tracing::info!("  chunk {}: {}", chunks, text);
            }
            Ok(None) => break,
            Err(_) => return Err("stream recv timeout".into()),
        }
    }
    let status = stream.wait_end().await;
    assert!(status.is_ok(), "stream ended with non-OK status");
    assert_eq!(chunks, 5, "expected 5 chunks, got {}", chunks);

    // 7. Issue Client Streaming RPC.
    tracing::info!("invoking Client Streaming: /echo.Echo/Collect");
    let mut cstream = tokio::time::timeout(
        Duration::from_secs(10),
        client.invoke_client_streaming("/echo.Echo/Collect", Some(b"first")),
    )
    .await??;

    // Send additional chunks.
    cstream.send(b"chunk-2").map_err(|e| format!("send: {e}"))?;
    cstream.send(b"chunk-3").map_err(|e| format!("send: {e}"))?;
    cstream
        .close_send()
        .map_err(|e| format!("close_send: {e}"))?;

    let resp = tokio::time::timeout(Duration::from_secs(10), cstream.recv())
        .await
        .map_err(|_| "client-streaming recv timeout")?
        .ok_or("no response")?;
    let resp_str = String::from_utf8_lossy(&resp);
    tracing::info!("Client Streaming response: {}", resp_str);
    assert!(resp_str.contains("3 messages"), "unexpected: {}", resp_str);

    let status = cstream.wait_end().await;
    assert!(status.is_ok(), "client-stream ended non-OK");

    // 8. Issue Bidi Streaming RPC.
    tracing::info!("invoking Bidi Streaming: /echo.Echo/Chat");
    let mut bstream = tokio::time::timeout(
        Duration::from_secs(10),
        client.invoke_bidi_streaming("/echo.Echo/Chat"),
    )
    .await??;

    for i in 1..=3 {
        let msg = format!("msg-{}", i);
        bstream
            .send(msg.as_bytes())
            .map_err(|e| format!("bidi send: {e}"))?;
        let resp = tokio::time::timeout(Duration::from_secs(10), bstream.recv())
            .await
            .map_err(|_| "bidi recv timeout")?
            .ok_or("no bidi response")?;
        let resp_str = String::from_utf8_lossy(&resp);
        tracing::info!("  ack {}: {}", i, resp_str);
        assert_eq!(resp_str, format!("ack {}: msg-{}", i, i));
    }

    bstream
        .close_send()
        .map_err(|e| format!("bidi close_send: {e}"))?;

    // After close_send the server returns OK → EOF.
    let after = tokio::time::timeout(Duration::from_secs(5), bstream.recv()).await;
    match after {
        Ok(None) => tracing::info!("bidi stream ended cleanly"),
        Ok(Some(d)) => tracing::warn!("unexpected data after close_send: {:?}", d),
        Err(_) => tracing::warn!("bidi recv after close_send timed out (acceptable)"),
    }

    let status = bstream.wait_end().await;
    assert!(status.is_ok(), "bidi ended non-OK");

    tracing::info!("=== ALL TESTS PASSED ===");
    Ok(())
}

/// Subscribe to the SSE endpoint and wait for an SDP offer message.
async fn wait_for_sse_offer(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    // ureq is synchronous; run it on a blocking thread.
    let url = url.to_string();
    tokio::task::spawn_blocking(
        move || -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
            let resp = ureq::get(&url).timeout(Duration::from_secs(60)).call()?;
            let reader = resp.into_reader();
            use std::io::{BufRead, BufReader};
            let buf = BufReader::new(reader);
            for line in buf.lines() {
                let line = line?;
                if line.starts_with("data: ") {
                    let json = &line[6..];
                    if let Ok(msg) = serde_json::from_str::<SignalMsg>(json) {
                        if msg.msg_type == "offer" {
                            return Ok(msg.sdp.unwrap_or_default());
                        }
                    }
                }
            }
            Err("SSE stream ended without offer".into())
        },
    )
    .await?
    .map_err(
        |e: Box<dyn std::error::Error + Send + Sync>| -> Box<dyn std::error::Error> {
            Box::from(e.to_string())
        },
    )
}
