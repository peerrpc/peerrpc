//! v2 facade demo: shows the new `peerrpc::dial` / `peerrpc::listen`
//! API for Rust.
//!
//! Compared to the v1 echo example (which manually wires Local +
//! Peer::dial/accept + Server into ~50 lines across multiple
//! tokio::spawn blocks), the v2 facade does the same work in ~25
//! lines and exposes no magic strings (no "demo-room", no
//! "offerer"/"answerer").
//!
//! ```sh
//! cargo run -p peerrpc-facade-demo
//! ```

use std::sync::Arc;
use std::time::Duration;

use peerrpc::{dial, listen};
use peerrpc_rpc::{
    server::{MethodDesc, MethodKind, Server, ServerStream, ServiceDesc},
    Status,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target = "peerrpc+local:///echo.Echo";

    // Server side: spawn the listener in the background.
    let ln = listen(target).await?;
    let server_task = tokio::spawn(async move {
        loop {
            match ln.accept().await {
                Ok(peer) => {
                    let mut srv = Server::new();
                    srv.register_service(ServiceDesc {
                        service_name: "echo.Echo".into(),
                        methods: vec![MethodDesc {
                            method: "Unary".into(),
                            kind: MethodKind::Unary,
                            handler: Arc::new(|stream: ServerStream| {
                                Box::pin(async move {
                                    // Send one fixed response; enough to
                                    // exercise the wire round trip
                                    // without tangling with the
                                    // ServerStream recv borrow rules.
                                    // A fuller echo lives in
                                    // examples/echo.
                                    if let Err(e) = stream.send(b"echo: peerrpc".to_vec()).await {
                                        return Status { code: 13, message: e };
                                    }
                                    Status::ok()
                                })
                            }),
                        }],
                    });
                    tokio::spawn(async move {
                        let _ = srv.serve(peer).await;
                    });
                }
                Err(e) => {
                    eprintln!("listener accept failed: {e}");
                    break;
                }
            }
        }
    });

    // Client side: one dial. Compare with the 11-step v1 dance.
    let conn = dial(target).await?;
    println!("connected as {}", conn.peer_id);

    let (response, status) = conn
        .client
        .invoke_unary("/echo.Echo/Unary", b"hello, peerrpc")
        .await?;
    println!("status: {:?}", status);
    println!("response bytes: {}", response.len());

    drop(conn);
    server_task.abort();

    // Give the runtime a beat to flush logs before exit.
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}
