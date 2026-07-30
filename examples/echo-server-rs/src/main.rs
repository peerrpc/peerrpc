//! Command peerrpc-echo-server is a PeerRPC server (Answerer) that
//! connects to a standalone signal-server over WebSocket and serves
//! the echo.Echo service. It is the Rust counterpart to the Go
//! echo-server (examples/echo-server-go).
//!
//! Run:  make run-signal          (terminal 1)
//!       make run-echo-server-rs  (terminal 2)
//!       make run-echo-ts         (terminal 3, browser client)
//!
//! The server loops: for each client it opens a fresh WebSocket
//! signaling session, accepts the WebRTC offer, and serves RPCs until
//! the DataChannel closes, then waits for the next client. An Accept
//! failure (e.g. a negotiation timeout) is logged and the loop
//! continues, so a single bad client does not take the server down.

use std::sync::Arc;
use std::time::Duration;

use peerrpc_peer::{Peer, PeerConfig};
use peerrpc_rpc::server::{BoxFuture, MethodDesc, MethodKind, Server, ServerStream, ServiceDesc};
use peerrpc_signal::WS;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let signal_addr = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ws://localhost:8443".to_string());
    let service = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "echo.Echo".to_string());

    let backend = WS::new(signal_addr.clone());
    tracing::info!(%signal_addr, %service, "echo server listening");

    // Handlers are Arc (cheaply cloneable); ServiceDesc is not Clone,
    // so rebuild the desc per accepted client from the shared handlers.
    let echo = echo_handler();
    let stream = stream_handler();
    let collect = collect_handler();
    let chat = chat_handler();
    let large_echo = large_echo_handler();
    let large_download = large_download_handler();
    let large_echo_stream = large_echo_stream_handler();
    let large_download_stream = large_download_stream_handler();

    // Serve clients one at a time for the accept handshake, then run
    // serve() detached on its own task so a hung peer (webrtc-rs does
    // not always surface a DataChannel close) cannot block the next
    // client. The main loop blocks in Peer::accept until a dialer sends
    // an SDP offer, so it never busy-loops; once a peer is accepted we
    // hand serve() to a task and immediately re-announce for the next
    // client.
    let mut seq: u64 = 0;
    loop {
        seq += 1;
        let peer_id = format!("echo-server-rs-{seq}");

        let sig = match backend.exchange(&service, &peer_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(%peer_id, "signaling exchange failed; retrying: {e}");
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let cfg = PeerConfig::default();
        // Generous timeout for browser WebRTC setup (ICE gathering on
        // first connect can exceed the SDK's 10s default). accept blocks
        // here until a dialer's offer arrives, so the loop only advances
        // when a real client connects (or the session closes).
        let peer = match Peer::accept(cfg, sig, Duration::from_secs(60)).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(%peer_id, "Accept failed; retrying: {e}");
                continue;
            }
        };

        tracing::info!(%peer_id, "DataChannel open, serving echo.Echo");
        let echo_h = echo.clone();
        let stream_h = stream.clone();
        let collect_h = collect.clone();
        let chat_h = chat.clone();
        let large_echo_h = large_echo.clone();
        let large_download_h = large_download.clone();
        let large_echo_stream_h = large_echo_stream.clone();
        let large_download_stream_h = large_download_stream.clone();
        let pid = peer_id.clone();
        // Detach serve(): a hung peer won't block the next accept.
        tokio::spawn(async move {
            let mut srv = Server::new();
            srv.register_service(ServiceDesc {
                service_name: "echo.Echo".into(),
                methods: vec![
                    MethodDesc {
                        method: "Echo".into(),
                        kind: MethodKind::Unary,
                        handler: echo_h,
                    },
                    MethodDesc {
                        method: "Stream".into(),
                        kind: MethodKind::ServerStreaming,
                        handler: stream_h,
                    },
                    MethodDesc {
                        method: "Collect".into(),
                        kind: MethodKind::ClientStreaming,
                        handler: collect_h,
                    },
                    MethodDesc {
                        method: "Chat".into(),
                        kind: MethodKind::BidiStreaming,
                        handler: chat_h,
                    },
                    MethodDesc {
                        method: "LargeEcho".into(),
                        kind: MethodKind::Unary,
                        handler: large_echo_h,
                    },
                    MethodDesc {
                        method: "LargeDownload".into(),
                        kind: MethodKind::ServerStreaming,
                        handler: large_download_h,
                    },
                    MethodDesc {
                        method: "LargeEchoStream".into(),
                        kind: MethodKind::BidiStreaming,
                        handler: large_echo_stream_h,
                    },
                    MethodDesc {
                        method: "LargeDownloadStream".into(),
                        kind: MethodKind::ServerStreaming,
                        handler: large_download_stream_h,
                    },
                ],
            });
            srv.serve(peer).await;
            tracing::info!(%pid, "DataChannel closed");
        });
    }
}

/// Unary echo: prepend "echo: " to the request payload.
fn echo_handler() -> Arc<dyn Fn(ServerStream) -> BoxFuture<peerrpc_rpc::Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            match s.recv().await {
                Some(req) => {
                    let resp = [b"echo: ", &req[..]].concat();
                    match s.send(resp).await {
                        Ok(()) => peerrpc_rpc::Status::ok(),
                        Err(e) => peerrpc_rpc::Status {
                            code: 13,
                            message: e,
                        },
                    }
                }
                None => peerrpc_rpc::Status::ok(),
            }
        })
    })
}

/// Server-streaming: emit 5 chunks echoing the request label.
fn stream_handler() -> Arc<dyn Fn(ServerStream) -> BoxFuture<peerrpc_rpc::Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            let req = s.recv().await.unwrap_or_default();
            let req_str = String::from_utf8_lossy(&req);
            for i in 1..=5 {
                let msg = format!("chunk {i} for {req_str:?}");
                if let Err(e) = s.send(msg.into_bytes()).await {
                    return peerrpc_rpc::Status {
                        code: 13,
                        message: e,
                    };
                }
            }
            peerrpc_rpc::Status::ok()
        })
    })
}

/// Client-streaming: read every request until the client half-closes
/// (recv() returns None), then reply with a count summary.
fn collect_handler() -> Arc<dyn Fn(ServerStream) -> BoxFuture<peerrpc_rpc::Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            let mut n = 0usize;
            let mut total = 0usize;
            while let Some(msg) = s.recv().await {
                n += 1;
                total += msg.len();
            }
            // recv() returned None => client half-closed (CloseSend).
            let reply = format!("received {n} messages ({total} bytes)").into_bytes();
            match s.send(reply).await {
                Ok(()) => peerrpc_rpc::Status::ok(),
                Err(e) => peerrpc_rpc::Status {
                    code: 13,
                    message: e,
                },
            }
        })
    })
}

/// Bidi-streaming: echo each request back as "ack N: <msg>" until the
/// client half-closes.
fn chat_handler() -> Arc<dyn Fn(ServerStream) -> BoxFuture<peerrpc_rpc::Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            let mut seq = 0;
            loop {
                match s.recv().await {
                    Some(msg) => {
                        seq += 1;
                        let reply =
                            format!("ack {seq}: {}", String::from_utf8_lossy(&msg)).into_bytes();
                        if let Err(e) = s.send(reply).await {
                            return peerrpc_rpc::Status {
                                code: 13,
                                message: e,
                            };
                        }
                    }
                    None => return peerrpc_rpc::Status::ok(),
                }
            }
        })
    })
}

// ── Large-packet handlers + helpers (mirror echo-server-go) ──────

/// Largest single-message blob. The wire Chunk.total_size field is a
/// signed int32, so cap at i32::MAX (~2 GiB). Browsers will OOM well
/// before this; the client also clamps.
const MAX_DOWNLOAD_BYTES: usize = i32::MAX as usize;

/// Largest streaming blob (sanity bound; streaming has no int32 ceiling).
const MAX_STREAM_BYTES: u64 = 1u64 << 60;

/// fill_pattern writes the deterministic byte(i % 251) pattern into dst
/// starting at global byte offset base, reusing a caller buffer (no
/// allocation) so a chunk can be reused across an unbounded stream.
fn fill_pattern(dst: &mut [u8], base: u64) {
    for (i, b) in dst.iter_mut().enumerate() {
        *b = ((base + i as u64) % 251) as u8;
    }
}

/// make_pattern allocates a size-byte buffer filled with the pattern.
fn make_pattern(size: usize) -> Vec<u8> {
    let mut b = vec![0u8; size];
    fill_pattern(&mut b, 0);
    b
}

/// parse_download_size parses a decimal byte count with an optional
/// K/KB/M/MB suffix, clamped to [1, MAX_DOWNLOAD_BYTES]. Empty/None
/// defaults to 1 MiB. Returns None on parse error.
fn parse_download_size(req: Option<&[u8]>) -> Option<usize> {
    let raw = req
        .map(|r| String::from_utf8_lossy(r).trim().to_lowercase())
        .unwrap_or_default();
    if raw.is_empty() {
        return Some(1 << 20); // default 1 MiB
    }
    let (n, mul) = strip_size_suffix(&raw)?;
    let bytes = n.checked_mul(mul)?.min(MAX_DOWNLOAD_BYTES as u64);
    Some(bytes.max(1) as usize)
}

/// parse_stream_size is the int64 equivalent (supports G/GB too),
/// clamped to [1, MAX_STREAM_BYTES]. None on parse error.
fn parse_stream_size(req: Option<&[u8]>) -> Option<u64> {
    let raw = req
        .map(|r| String::from_utf8_lossy(r).trim().to_lowercase())
        .unwrap_or_default();
    if raw.is_empty() {
        return Some(1 << 20); // default 1 MiB
    }
    let mut mul: u64 = 1;
    let n = if let Some(rest) = raw.strip_suffix("gb") {
        mul = 1 << 30;
        rest
    } else if let Some(rest) = raw.strip_suffix('g') {
        mul = 1 << 30;
        rest
    } else if let Some(rest) = raw.strip_suffix("mb") {
        mul = 1 << 20;
        rest
    } else if let Some(rest) = raw.strip_suffix('m') {
        mul = 1 << 20;
        rest
    } else if let Some(rest) = raw.strip_suffix("kb") {
        mul = 1 << 10;
        rest
    } else if let Some(rest) = raw.strip_suffix('k') {
        mul = 1 << 10;
        rest
    } else {
        raw.as_str()
    };
    let v: u64 = n.trim().parse().ok()?;
    let bytes = v.checked_mul(mul)?;
    Some(bytes.clamp(1, MAX_STREAM_BYTES))
}

// strip_size_suffix handles K/KB/M/MB (no G for the single-message path).
// Returns (number, multiplier) or None on parse failure.
fn strip_size_suffix(raw: &str) -> Option<(u64, u64)> {
    let (n, mul) = if let Some(rest) = raw.strip_suffix("mb") {
        (rest, 1u64 << 20)
    } else if let Some(rest) = raw.strip_suffix('m') {
        (rest, 1u64 << 20)
    } else if let Some(rest) = raw.strip_suffix("kb") {
        (rest, 1u64 << 10)
    } else if let Some(rest) = raw.strip_suffix('k') {
        (rest, 1u64 << 10)
    } else {
        (raw, 1)
    };
    let v: u64 = n.trim().parse().ok()?;
    Some((v, mul))
}

/// LargeEcho (Unary): echo the request payload verbatim. Exercises
/// inbound reassembly + outbound chunking for large payloads.
fn large_echo_handler() -> Arc<dyn Fn(ServerStream) -> BoxFuture<peerrpc_rpc::Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            match s.recv().await {
                Some(req) => match s.send(req).await {
                    Ok(()) => peerrpc_rpc::Status::ok(),
                    Err(e) => peerrpc_rpc::Status { code: 13, message: e },
                },
                None => peerrpc_rpc::Status::ok(),
            }
        })
    })
}

/// LargeDownload (Server-Streaming): generate a blob of the caller-chosen
/// size (first message = decimal byte count) and push it as one message.
fn large_download_handler()
-> Arc<dyn Fn(ServerStream) -> BoxFuture<peerrpc_rpc::Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            let req = s.recv().await;
            let size = match parse_download_size(req.as_deref()) {
                Some(n) => n,
                None => {
                    return peerrpc_rpc::Status {
                        code: 3,
                        message: "large-download: size must be a non-negative decimal byte count"
                            .into(),
                    }
                }
            };
            let blob = make_pattern(size);
            match s.send(blob).await {
                Ok(()) => peerrpc_rpc::Status::ok(),
                Err(e) => peerrpc_rpc::Status { code: 13, message: e },
            }
        })
    })
}

/// LargeEchoStream (Bidi): echo each message verbatim. Memory is constant
/// (one message in flight), so total transfer size is unbounded.
fn large_echo_stream_handler()
-> Arc<dyn Fn(ServerStream) -> BoxFuture<peerrpc_rpc::Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            loop {
                match s.recv().await {
                    Some(msg) => match s.send(msg).await {
                        Ok(()) => {}
                        Err(e) => return peerrpc_rpc::Status { code: 13, message: e },
                    },
                    None => return peerrpc_rpc::Status::ok(),
                }
            }
        })
    })
}

/// LargeDownloadStream (Server-Streaming): push the caller-chosen size in
/// 16 MiB chunks (pattern-filled), unbounded total.
fn large_download_stream_handler()
-> Arc<dyn Fn(ServerStream) -> BoxFuture<peerrpc_rpc::Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            let req = s.recv().await;
            let size = match parse_stream_size(req.as_deref()) {
                Some(n) => n,
                None => {
                    return peerrpc_rpc::Status {
                        code: 3,
                        message: "large-download-stream: size must be a non-negative decimal byte count".into(),
                    }
                }
            };
            let mut chunk = vec![0u8; 16 * 1024 * 1024]; // 16 MiB, reused
            let mut sent: u64 = 0;
            while sent < size {
                let end = std::cmp::min(sent + chunk.len() as u64, size);
                let take = (end - sent) as usize;
                fill_pattern(&mut chunk[..take], sent);
                if let Err(e) = s.send(chunk[..take].to_vec()).await {
                    return peerrpc_rpc::Status { code: 13, message: e };
                }
                sent = end;
            }
            peerrpc_rpc::Status::ok()
        })
    })
}
