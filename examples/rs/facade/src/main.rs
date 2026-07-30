//! Facade demo: shows the `peerrpc::dial` / `peerrpc::listen`
//! API for Rust.
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
    server::{BoxFuture, MethodDesc, MethodKind, Server, ServerStream, ServiceDesc},
    Status,
};

fn unary_handler() -> Arc<dyn Fn(ServerStream) -> BoxFuture<Status> + Send + Sync> {
    Arc::new(|s: ServerStream| {
        Box::pin(async move {
            // Full echo: recv → prefix → send. Made natural by the
            // &self API on recv/send (no `mut` needed, no borrow-
            // checker fights).
            let req = match s.recv().await {
                Some(b) => b,
                None => return Status { code: 13, message: "empty request".into() },
            };
            let mut resp = Vec::with_capacity(6 + req.len());
            resp.extend_from_slice(b"echo: ");
            resp.extend_from_slice(&req);
            match s.send(resp).await {
                Ok(()) => Status::ok(),
                Err(e) => Status { code: 13, message: e },
            }
        })
    })
}

fn new_echo_server() -> Server {
    let mut srv = Server::new();
    srv.register_service(ServiceDesc {
        service_name: "echo.Echo".into(),
        methods: vec![MethodDesc {
            method: "Unary".into(),
            kind: MethodKind::Unary,
            handler: unary_handler(),
        }],
    });
    srv
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let target = "peerrpc+local:///echo.Echo";

    // Server side: spawn the listener in the background.
    let ln = listen(target).await?;
    let server_task = tokio::spawn(async move {
        loop {
            match ln.accept().await {
                Ok(peer) => {
                    let srv = new_echo_server();
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
    println!("status: {status:?}");
    println!("response: {}", String::from_utf8_lossy(&response));

    drop(conn);
    server_task.abort();

    // Give the runtime a beat to flush logs before exit.
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}
