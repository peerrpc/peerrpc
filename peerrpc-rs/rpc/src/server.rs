use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use peerrpc_protocol::{
    encode_response_frame, try_decode_frame, gen, Begin, Chunk, Data, End, Frame, Routing,
    CHUNK_SIZE, MESSAGE_MAX,
};
use peerrpc_transport::Reassembler;
use tokio::sync::{mpsc, Mutex};

use crate::{Status, WireTransport};

pub type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MethodKind {
    Unary,
    ServerStreaming,
    ClientStreaming,
    BidiStreaming,
}

#[derive(Clone)]
pub struct MethodDesc {
    pub method: String,
    pub kind: MethodKind,
    // Handler takes the ServerStream by value so the resulting future
    // can be 'static (the stream's owned state travels into the
    // task). recv() and send() take &self: ServerStream hides its
    // single mutable receiver behind an async Mutex so the API
    // matches Go's *ServerStream ergonomics — the handler body
    // becomes `|s: ServerStream| { let r = s.recv().await; s.send(r).await; }`
    // without borrow-checker gymnastics.
    pub handler: Arc<dyn Fn(ServerStream) -> BoxFuture<Status> + Send + Sync>,
}

pub struct ServiceDesc {
    pub service_name: String,
    pub methods: Vec<MethodDesc>,
}

pub struct Server {
    methods: HashMap<String, MethodDesc>,
}

impl Server {
    pub fn new() -> Self {
        Self { methods: HashMap::new() }
    }

    pub fn register_service(&mut self, desc: ServiceDesc) {
        for m in desc.methods {
            let path = format!("/{}/{}", desc.service_name, m.method);
            let desc = MethodDesc {
                method: path.clone(),
                kind: m.kind,
                handler: m.handler,
            };
            if self.methods.contains_key(&path) {
                panic!("rpc: duplicate method registration: {path}");
            }
            self.methods.insert(path, desc);
        }
    }

    pub async fn serve<T: WireTransport>(&self, mut transport: T) {
        let (resp_tx, mut resp_rx) = mpsc::unbounded_channel::<RespFrame>();
        let streams: Arc<Mutex<HashMap<i32, StreamState>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let mut buf = Vec::new();
        let mut reasm = Reassembler::new();

        loop {
            tokio::select! {
                result = transport.recv_frame() => {
                    match result {
                        None => break,
                        Some(bytes) => {
                            buf.extend_from_slice(&bytes);
                            loop {
                                match try_decode_frame(&buf) {
                                    Ok(Some((frame, consumed))) => {
                                        buf.drain(..consumed);
                                        Self::handle_frame(frame, &self.methods, &streams, &resp_tx, &mut reasm).await;
                                    }
                                    Ok(None) => break,
                                    Err(e) => {
                                        tracing::warn!("server: decode error: {e}");
                                        buf.clear();
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Some(resp) = resp_rx.recv() => {
                    let bytes = encode_response_frame(&resp.frame);
                    if transport.send_frame(bytes).await.is_err() {
                        break;
                    }
                }
            }
        }
    }

    async fn handle_frame(
        frame: Frame,
        methods: &HashMap<String, MethodDesc>,
        streams: &Arc<Mutex<HashMap<i32, StreamState>>>,
        resp_tx: &mpsc::UnboundedSender<RespFrame>,
        reasm: &mut Reassembler,
    ) {
        use gen::frame::Type as FType;
        let seq = frame.routing.as_ref().map(|r| r.sequence).unwrap_or(0);

        match frame.r#type {
            Some(FType::Call(call)) => {
                let method = match methods.get(&call.method) {
                    Some(m) => m.clone(),
                    None => {
                        Self::end_stream_with_error(seq, resp_tx, "unimplemented method");
                        return;
                    }
                };

                let (inbound_tx, inbound_rx) = mpsc::channel(16);

                if let Some(inline) = call.inline_data {
                    if !inline.is_empty() {
                        let _ = inbound_tx.try_send(inline);
                    }
                }

                {
                    let mut guard = streams.lock().await;
                    if guard.contains_key(&seq) {
                        Self::end_stream_with_error(seq, resp_tx, "duplicate sequence");
                        return;
                    }
                    guard.insert(seq, StreamState {
                        inbound: Some(inbound_tx),
                    });
                }

                let handler = method.handler;
                let tx = resp_tx.clone();
                let stream_resp_tx = resp_tx.clone();
                tokio::spawn(async move {
                    // The ServerStream travels by value into this
                    // task. handler(&mut stream) was the original
                    // plan, but BoxFuture is 'static and can't hold
                    // an &'a mut borrow. We hand the stream by value
                    // to the handler; recv() and send() take &self
                    // so the body looks like Go's *ServerStream.
                    let stream = ServerStream {
                        seq,
                        method: call.method,
                        inbound: Arc::new(tokio::sync::Mutex::new(inbound_rx)),
                        resp_tx: stream_resp_tx,
                        header_sent: AtomicBool::new(false),
                    };
                    let status = handler(stream).await;
                    let end = peerrpc_protocol::ResponseFrame {
                        routing: Some(Routing { sequence: seq }),
                        r#type: Some(gen::response_frame::Type::End(End {
                            close_send: false,
                            status: Some(peerrpc_protocol::Status {
                                code: status.code,
                                message: status.message,
                                details: vec![],
                            }),
                            ..Default::default()
                        })),
                    };
                    let _ = tx.send(RespFrame { frame: end });
                });
            }
            Some(FType::Data(data)) => {
                let payload = match data.content {
                    Some(gen::data::Content::Message(msg)) => Some(msg),
                    Some(gen::data::Content::Chunk(chunk)) => {
                        reasm.reassemble(seq, chunk.total_size as usize, chunk.offset as usize, &chunk.data)
                    }
                    None => None,
                };
                if let Some(payload) = payload {
                    let inbound_tx = {
                        let guard = streams.lock().await;
                        guard.get(&seq).and_then(|s| s.inbound.clone())
                    };
                    if let Some(tx) = inbound_tx {
                        if tx.try_send(payload).is_err() {
                            tracing::warn!("server: inbound queue full, dropping data for seq {seq}");
                        }
                    }
                }
            }
            Some(FType::End(end)) => {
                if end.close_send {
                    // Half-close: drop the sender so the handler's recv()
                    // returns None (EOF) after draining buffered messages.
                    // The stream entry stays until endStream removes it.
                    let mut guard = streams.lock().await;
                    if let Some(state) = guard.get_mut(&seq) {
                        state.inbound.take();
                    }
                } else {
                    let _ = streams.lock().await.remove(&seq);
                }
            }
            None => {}
        }
    }

    fn end_stream_with_error(
        seq: i32,
        resp_tx: &mpsc::UnboundedSender<RespFrame>,
        msg: &str,
    ) {
        let _ = resp_tx.send(RespFrame {
            frame: peerrpc_protocol::ResponseFrame {
                routing: Some(Routing { sequence: seq }),
                r#type: Some(gen::response_frame::Type::End(End {
                    status: Some(peerrpc_protocol::Status {
                        code: 12,
                        message: msg.to_string(),
                        details: vec![],
                    }),
                    ..Default::default()
                })),
            },
        });
    }
}

impl Default for Server {
    fn default() -> Self {
        Self::new()
    }
}

struct StreamState {
    // Option so half-close can take() the sender, which makes the
    // handler's recv() return None (EOF) after draining buffered items.
    inbound: Option<mpsc::Sender<Vec<u8>>>,
}

pub struct ServerStream {
    seq: i32,
    method: String,
    // Arc<Mutex<...>> so recv() can take &self. Without this, the
    // owned mpsc::Receiver would force recv(&mut self), which
    // cascades to handler(&mut ServerStream), which can't be paired
    // with a 'static future (the &'a mut borrow can't escape the
    // function frame).
    inbound: Arc<Mutex<mpsc::Receiver<Vec<u8>>>>,
    resp_tx: mpsc::UnboundedSender<RespFrame>,
    header_sent: AtomicBool,
}

impl ServerStream {
    pub fn method(&self) -> &str {
        &self.method
    }

    pub async fn recv(&self) -> Option<Vec<u8>> {
        // EOF is driven by the sender being dropped on half-close
        // (handle_frame End{close_send} takes the sender). After the
        // sender is dropped, recv() drains buffered messages then
        // returns None. This avoids the racy select! between recv()
        // and a Notify that did not store a permit.
        let mut guard = self.inbound.lock().await;
        guard.recv().await
    }

    pub async fn send(&self, data: Vec<u8>) -> Result<(), String> {
        self.emit_begin()?;
        if data.len() <= MESSAGE_MAX {
            let frame = peerrpc_protocol::ResponseFrame {
                routing: Some(Routing { sequence: self.seq }),
                r#type: Some(gen::response_frame::Type::Data(Data {
                    content: Some(gen::data::Content::Message(data)),
                })),
            };
            self.resp_tx
                .send(RespFrame { frame })
                .map_err(|_| "transport closed".to_string())
        } else {
            let total = data.len();
            for offset in (0..total).step_by(CHUNK_SIZE) {
                let end = (offset + CHUNK_SIZE).min(total);
                let frame = peerrpc_protocol::ResponseFrame {
                    routing: Some(Routing { sequence: self.seq }),
                    r#type: Some(gen::response_frame::Type::Data(Data {
                        content: Some(gen::data::Content::Chunk(Chunk {
                            total_size: total as i32,
                            offset: offset as i32,
                            data: data[offset..end].to_vec(),
                        })),
                    })),
                };
                self.resp_tx
                    .send(RespFrame { frame })
                    .map_err(|_| "transport closed".to_string())?;
            }
            Ok(())
        }
    }

    fn emit_begin(&self) -> Result<(), String> {
        if !self.header_sent.swap(true, Ordering::SeqCst) {
            let frame = peerrpc_protocol::ResponseFrame {
                routing: Some(Routing { sequence: self.seq }),
                r#type: Some(gen::response_frame::Type::Begin(Begin {
                    header: None,
                    inline_data: None,
                })),
            };
            self.resp_tx
                .send(RespFrame { frame })
                .map_err(|_| "transport closed".to_string())?;
        }
        Ok(())
    }
}

struct RespFrame {
    frame: peerrpc_protocol::ResponseFrame,
}
