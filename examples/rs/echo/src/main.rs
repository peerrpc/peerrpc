use std::sync::Arc;
use std::time::Duration;

use peerrpc_peer::{Peer, PeerConfig};
use peerrpc_rpc::server::{BoxFuture, MethodDesc, MethodKind, Server, ServerStream, ServiceDesc};
use peerrpc_rpc::{Client, Status};
use peerrpc_signal::Local;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    match run().await {
        Ok(()) => tracing::info!("echo demo OK"),
        Err(e) => tracing::error!("echo demo failed: {e}"),
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let backend = Local::new();

    let o_sig = backend.exchange("demo-room", "offerer").await?;
    let a_sig = backend.exchange("demo-room", "answerer").await?;

    let cfg = PeerConfig::default();
    let cfg_acceptor = cfg.clone();

    // ─── Server (answerer) ──────────────────────────────────
    let mut srv = Server::new();
    srv.register_service(ServiceDesc {
        service_name: "echo.Echo".into(),
        methods: vec![
            MethodDesc {
                method: "Unary".into(),
                kind: MethodKind::Unary,
                handler: unary_handler(),
            },
            MethodDesc {
                method: "Stream".into(),
                kind: MethodKind::ServerStreaming,
                handler: stream_handler(),
            },
        ],
    });

    let answerer = tokio::spawn(async move {
        let peer = Peer::accept(cfg_acceptor, a_sig, Duration::from_secs(10))
            .await
            .map_err(|e| format!("accept: {e}"))?;
        srv.serve(peer).await;
        Ok::<_, String>(())
    });

    // ─── Client (offerer) ───────────────────────────────────
    let peer = Peer::dial(cfg, o_sig, Duration::from_secs(10)).await?;
    let cli = Client::new(peer);

    // Unary
    let (resp, status) = cli
        .invoke_unary("/echo.Echo/Unary", b"hello, peerrpc")
        .await
        .map_err(|e| format!("unary: {e}"))?;
    if !status.is_ok() {
        return Err(format!("unary status: {} {}", status.code, status.message).into());
    }
    tracing::info!("Unary OK: {}", String::from_utf8_lossy(&resp));

    // Server-streaming
    let mut stream = cli
        .invoke_server_streaming("/echo.Echo/Stream", b"ping")
        .await
        .map_err(|e| format!("stream: {e}"))?;
    let mut count = 0;
    while let Some(chunk) = stream.recv().await {
        tracing::info!("Stream chunk: {}", String::from_utf8_lossy(&chunk));
        count += 1;
    }
    let _ = stream.wait_end().await;
    tracing::info!("Stream complete: {count} chunks");

    // Close client transport to signal the answerer's serve() to stop
    drop(cli);

    // Give the answerer a brief window to detect the close
    tokio::time::timeout(Duration::from_secs(3), answerer)
        .await
        .ok();
    Ok(())
}

fn unary_handler() -> Arc<dyn Fn(ServerStream) -> BoxFuture<Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            // recv() and send() take &self, so the by-value stream
            // can be used directly without `mut` binding or borrow-
            // checker gymnastics. Mirrors Go's *ServerStream shape.
            match s.recv().await {
                Some(req) => {
                    let resp = [b"echo: ", &req[..]].concat();
                    match s.send(resp).await {
                        Ok(()) => Status::ok(),
                        Err(e) => Status {
                            code: 13,
                            message: e,
                        },
                    }
                }
                None => Status::ok(),
            }
        })
    })
}

fn stream_handler() -> Arc<dyn Fn(ServerStream) -> BoxFuture<Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            let req = s.recv().await.unwrap_or_default();
            let req_str = String::from_utf8_lossy(&req);
            for i in 1..=5 {
                let msg = format!("chunk {i} for {req_str:?}");
                if let Err(e) = s.send(msg.into_bytes()).await {
                    return Status {
                        code: 13,
                        message: e,
                    };
                }
            }
            Status::ok()
        })
    })
}
