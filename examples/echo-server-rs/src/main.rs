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
