//! Integration tests wiring a real Client to a real Server through an
//! in-memory bridge transport (no WebRTC). Covers 1-to-1, 1-to-many,
//! and call-timing scenarios the unit tests (client-only or
//! server-only) cannot exercise.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use peerrpc_rpc::server::{MethodDesc, MethodKind, Server, ServerStream, ServiceDesc};
use peerrpc_rpc::{Client, Status, WireTransport};
use tokio::sync::mpsc;

// ─── BridgeTransport ─────────────────────────────────────────

/// One side of an in-memory byte-pipe connecting a Client to a Server.
/// Frames written via send_frame appear on the peer's recv_frame.
struct BridgeTransport {
    outbound: mpsc::UnboundedSender<Bytes>,
    inbound: mpsc::UnboundedReceiver<Bytes>,
}

#[async_trait]
impl WireTransport for BridgeTransport {
    async fn send_frame(&mut self, frame: Bytes) -> Result<(), String> {
        self.outbound
            .send(frame)
            .map_err(|_| "bridge closed".to_string())
    }

    async fn recv_frame(&mut self) -> Option<Bytes> {
        self.inbound.recv().await
    }
}

/// Create a pair of connected transports. The first is the client
/// side (sends Frames, receives ResponseFrames), the second is the
/// server side (receives Frames, sends ResponseFrames).
fn bridge() -> (BridgeTransport, BridgeTransport) {
    let (client_to_server_tx, client_to_server_rx) = mpsc::unbounded_channel();
    let (server_to_client_tx, server_to_client_rx) = mpsc::unbounded_channel();
    (
        BridgeTransport {
            outbound: server_to_client_tx,
            inbound: client_to_server_rx,
        },
        BridgeTransport {
            outbound: client_to_server_tx,
            inbound: server_to_client_rx,
        },
    )
}

// ─── Test echo service ───────────────────────────────────────

fn echo_server() -> Server {
    let mut srv = Server::new();
    srv.register_service(ServiceDesc {
        service_name: "echo.Echo".to_string(),
        methods: vec![
            MethodDesc {
                method: "Unary".to_string(),
                kind: MethodKind::Unary,
                handler: Arc::new(|s: ServerStream| {
                    Box::pin(async move {
                        let Some(req) = s.recv().await else {
                            return Status {
                                code: 13,
                                message: "empty".into(),
                            };
                        };
                        let mut echo = b"echo: ".to_vec();
                        echo.extend_from_slice(&req);
                        let _ = s.send(echo).await;
                        Status::ok()
                    })
                }),
            },
            MethodDesc {
                method: "Stream".to_string(),
                kind: MethodKind::ServerStreaming,
                handler: Arc::new(|s: ServerStream| {
                    Box::pin(async move {
                        let label = s
                            .recv()
                            .await
                            .map(|r| String::from_utf8_lossy(&r).to_string())
                            .unwrap_or_default();
                        for i in 1..=5 {
                            let _ = s.send(format!("chunk {i} for {label}").into_bytes()).await;
                        }
                        Status::ok()
                    })
                }),
            },
            MethodDesc {
                method: "Chat".to_string(),
                kind: MethodKind::BidiStreaming,
                handler: Arc::new(|s: ServerStream| {
                    Box::pin(async move {
                        let mut seq = 0;
                        while let Some(msg) = s.recv().await {
                            seq += 1;
                            let _ = s
                                .send(
                                    format!("ack {seq}: {}", String::from_utf8_lossy(&msg))
                                        .into_bytes(),
                                )
                                .await;
                        }
                        Status::ok()
                    })
                }),
            },
            // LargeEcho: verbatim echo of an arbitrarily large payload,
            // exercising inbound chunk reassembly + outbound chunking.
            MethodDesc {
                method: "LargeEcho".to_string(),
                kind: MethodKind::Unary,
                handler: Arc::new(|s: ServerStream| {
                    Box::pin(async move {
                        match s.recv().await {
                            Some(req) => match s.send(req).await {
                                Ok(()) => Status::ok(),
                                Err(e) => Status { code: 13, message: e },
                            },
                            None => Status::ok(),
                        }
                    })
                }),
            },
            // LargeEchoStream: bidi verbatim echo, one response per request.
            MethodDesc {
                method: "LargeEchoStream".to_string(),
                kind: MethodKind::BidiStreaming,
                handler: Arc::new(|s: ServerStream| {
                    Box::pin(async move {
                        loop {
                            match s.recv().await {
                                Some(msg) => match s.send(msg).await {
                                    Ok(()) => {}
                                    Err(e) => return Status { code: 13, message: e },
                                },
                                None => return Status::ok(),
                            }
                        }
                    })
                }),
            },
        ],
    });
    srv
}

/// Spawn a Server on the server side of a bridge. Returns immediately;
/// the serve task runs until the transport closes.
fn spawn_server(srv: Arc<Server>, transport: BridgeTransport) {
    tokio::spawn(async move {
        srv.serve(transport).await;
    });
}

// ─── Tests ───────────────────────────────────────────────────

#[tokio::test]
async fn test_bridge_unary() {
    let srv = Arc::new(echo_server());
    let (client_t, server_t) = bridge();
    spawn_server(srv, server_t);
    let client = Client::new(client_t);

    let (resp, status) = client
        .invoke_unary("/echo.Echo/Unary", b"hello")
        .await
        .unwrap();
    assert!(status.is_ok(), "status: {status:?}");
    assert_eq!(resp, b"echo: hello");
}

#[tokio::test]
async fn test_bridge_concurrent_unary() {
    let srv = Arc::new(echo_server());
    let (client_t, server_t) = bridge();
    spawn_server(srv, server_t);
    let client = Client::new(client_t);

    // Three concurrent unary calls on the same client.
    let h1 = {
        let c = client.clone();
        tokio::spawn(async move { c.invoke_unary("/echo.Echo/Unary", b"one").await })
    };
    let h2 = {
        let c = client.clone();
        tokio::spawn(async move { c.invoke_unary("/echo.Echo/Unary", b"two").await })
    };
    let h3 = {
        let c = client.clone();
        tokio::spawn(async move { c.invoke_unary("/echo.Echo/Unary", b"three").await })
    };

    let r1 = h1.await.unwrap().unwrap();
    let r2 = h2.await.unwrap().unwrap();
    let r3 = h3.await.unwrap().unwrap();

    assert!(r1.1.is_ok() && r2.1.is_ok() && r3.1.is_ok());
    // Each response echoes its own request; collect and verify all present.
    let mut got: Vec<String> = [r1.0, r2.0, r3.0]
        .iter()
        .map(|b| String::from_utf8_lossy(b).into_owned())
        .collect();
    got.sort();
    assert_eq!(got, vec!["echo: one", "echo: three", "echo: two"]);
}

#[tokio::test]
async fn test_bridge_server_streaming() {
    let srv = Arc::new(echo_server());
    let (client_t, server_t) = bridge();
    spawn_server(srv, server_t);
    let client = Client::new(client_t);

    let mut stream = client
        .invoke_server_streaming("/echo.Echo/Stream", b"flow")
        .await
        .unwrap();
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.recv().await {
        chunks.push(String::from_utf8_lossy(&chunk).into_owned());
    }
    let status = stream.wait_end().await;
    assert!(status.is_ok());
    assert_eq!(chunks.len(), 5);
    assert!(chunks[0].contains("flow"));
}

#[tokio::test]
async fn test_bridge_bidi() {
    let srv = Arc::new(echo_server());
    let (client_t, server_t) = bridge();
    spawn_server(srv, server_t);
    let client = Client::new(client_t);

    let mut stream = client
        .invoke_bidi_streaming("/echo.Echo/Chat")
        .await
        .unwrap();
    for i in 1..=3 {
        let msg = format!("m{i}");
        stream.send(msg.as_bytes()).unwrap();
        let resp = stream.recv().await.expect("missing ack");
        assert_eq!(String::from_utf8_lossy(&resp), format!("ack {i}: {msg}"));
    }
    stream.close_send().unwrap();
    // Server returns OK after close_send → EOF.
    assert!(stream.recv().await.is_none());
    let status = stream.wait_end().await;
    assert!(status.is_ok());
}

#[tokio::test]
async fn test_bridge_multi_client() {
    // One server, three clients — each on its own bridge, with its
    // own serve task. The shared Server (&self) multiplexes handlers
    // by sequence across the independent transports.
    let srv = Arc::new(echo_server());

    let mut handles = Vec::new();
    for i in 0..3 {
        let (client_t, server_t) = bridge();
        let srv = srv.clone();
        tokio::spawn(async move {
            srv.serve(server_t).await;
        });
        let client = Client::new(client_t);
        let req = format!("client-{i}");
        handles.push(tokio::spawn(async move {
            client
                .invoke_unary("/echo.Echo/Unary", req.as_bytes())
                .await
                .unwrap()
        }));
    }

    for (i, h) in handles.into_iter().enumerate() {
        let (resp, status) = h.await.unwrap();
        assert!(status.is_ok(), "client {i} status: {status:?}");
        let want = format!("echo: client-{i}");
        assert_eq!(resp, want.as_bytes(), "client {i} mismatch");
    }
}

#[tokio::test]
async fn test_bridge_client_disconnect_cleans_up() {
    // A blocking handler + a client that disconnects (closes its side
    // of the bridge). The server's serve loop should observe
    // recv_frame() == None and exit cleanly, not hang.
    let mut srv = Server::new();
    srv.register_service(ServiceDesc {
        service_name: "echo.Echo".to_string(),
        methods: vec![MethodDesc {
            method: "Block".to_string(),
            kind: MethodKind::Unary,
            handler: Arc::new(|_s: ServerStream| {
                Box::pin(async move {
                    // Never returns on its own.
                    std::future::pending::<()>().await;
                    Status::ok()
                })
            }),
        }],
    });
    let srv = Arc::new(srv);

    let (client_t, server_t) = bridge();
    let serve_handle = tokio::spawn(async move {
        srv.serve(server_t).await;
    });
    let client = Client::new(client_t);

    // Give the server a moment, then drop the client. Dropping the
    // client closes its bridge sender, so the server's recv_frame
    // returns None and serve() returns.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    drop(client);

    // serve() must return (not hang) within 2s.
    tokio::time::timeout(std::time::Duration::from_secs(2), serve_handle)
        .await
        .expect("server serve loop did not exit after client disconnect")
        .expect("serve task panicked");
}

#[tokio::test]
async fn test_bridge_large_unary_echo() {
    // 1 MiB unary call: the client chunks it on the outbound (4×255 KiB
    // Data.chunk frames), the server reassembles, the handler echoes it
    // back, the client reassembles the response. Verifies the full
    // large-payload round trip stays under the SCTP max-message-size.
    let srv = Arc::new(echo_server());
    let (client_t, server_t) = bridge();
    spawn_server(srv, server_t);
    let client = Client::new(client_t);

    let size = 1024 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

    let (resp, status) = client
        .invoke_unary("/echo.Echo/LargeEcho", &payload)
        .await
        .expect("invoke_unary");
    assert!(status.is_ok(), "status: {status:?}");
    assert_eq!(resp.len(), size, "echo length mismatch");
    assert_eq!(resp, payload, "echo integrity mismatch");
}

#[tokio::test]
async fn test_bridge_large_bidi_echo() {
    // LargeEchoStream: send a 1 MiB message, expect a 1 MiB echo back,
    // then half-close. Mirrors the echo-ts LargeEchoStream harness.
    let srv = Arc::new(echo_server());
    let (client_t, server_t) = bridge();
    spawn_server(srv, server_t);
    let client = Client::new(client_t);

    let size = 1024 * 1024;
    let payload: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();

    let mut stream = client
        .invoke_bidi_streaming("/echo.Echo/LargeEchoStream")
        .await
        .expect("invoke_bidi_streaming");
    stream.send(&payload).expect("send");

    let echo = stream.recv().await.expect("missing echo");
    assert_eq!(echo.len(), size, "echo length mismatch");
    assert_eq!(echo, payload, "echo integrity mismatch");

    stream.close_send().unwrap();
    // Server returns OK after EOF.
    assert!(stream.recv().await.is_none());
    let status = stream.wait_end().await;
    assert!(status.is_ok(), "status: {status:?}");
}
