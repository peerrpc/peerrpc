//! PeerRPC client: multiplexes RPC calls over any byte-stream
//! transport that can send and receive length-prefixed frames.
//!
//! The Client is transport-agnostic: it depends on the
//! [`WireTransport`] trait, which abstracts the raw send/recv of
//! length-prefixed wire bytes. Production callers wire in a
//! webrtc-rs DataChannel adapter; tests use an in-memory mock.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Arc;

use peerrpc_protocol::{
    encode_frame, try_decode_response_frame, gen,
    Call, Chunk, Data, End, Frame, Routing,
    INLINE_MAX, MESSAGE_MAX, CHUNK_SIZE,
};
use peerrpc_transport::Reassembler;
use tokio::sync::{mpsc, Mutex};

// ─── WireTransport trait ─────────────────────────────────────

mod wire;

pub use wire::WireTransport;

// ─── Status ──────────────────────────────────────────────────

/// gRPC-compatible status code (0 = OK).
pub type Code = i32;

/// A status carrying a code and optional message.
#[derive(Debug, Clone, Default)]
pub struct Status {
    pub code: Code,
    pub message: String,
}

impl Status {
    pub fn ok() -> Self { Self::default() }
    pub fn is_ok(&self) -> bool { self.code == 0 }
}

/// Errors returned by Client methods.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
    #[error("rpc: transport closed")]
    TransportClosed,
    #[error("rpc: status {code}: {message}")]
    Status { code: Code, message: String },
    #[error("rpc: {0}")]
    Other(String),
}

// ─── Internal stream state ───────────────────────────────────

struct StreamState {
    inbound: mpsc::Sender<Vec<u8>>,
    end: Option<tokio::sync::oneshot::Sender<Status>>,
}

// ─── Client ──────────────────────────────────────────────────

/// Client multiplexes RPC calls over a [`WireTransport`].
///
/// Construct via [`Client::new`], which spawns an internal run loop
/// that pumps outbound frames and dispatches inbound frames to the
/// right stream. RPC invocations are safe to call concurrently.
pub struct Client {
    seq_alloc: AtomicI32,
    streams: Arc<Mutex<HashMap<i32, StreamState>>>,
    outbound: mpsc::UnboundedSender<bytes::Bytes>,
}

impl Client {
    /// Construct a new Client and spawn its internal run loop.
    ///
    /// The run loop lives for the lifetime of the returned Arc;
    /// dropping all clones does NOT cancel it (the transport's
    /// recv_frame returning None is the natural exit).
    pub fn new<T: WireTransport>(transport: T) -> Arc<Self> {
        let streams = Arc::new(Mutex::new(HashMap::new()));
        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();

        let client = Arc::new(Self {
            seq_alloc: AtomicI32::new(1),
            streams: streams.clone(),
            outbound: outbound_tx,
        });

        tokio::spawn(Self::run_loop(transport, outbound_rx, streams));

        client
    }

    /// Invoke a Unary RPC. Sends the request, half-closes, and
    /// collects exactly one response.
    pub async fn invoke_unary(
        &self,
        method: &str,
        req: &[u8],
    ) -> Result<(Vec<u8>, Status), RpcError> {
        let mut stream = self.open_stream(method, req, true).await?;

        let resp = stream.recv().await;
        let status = stream.wait_end().await;

        match resp {
            Some(data) if status.is_ok() => Ok((data, status)),
            Some(data) if !status.is_ok() => Ok((data, status)),
            Some(_) => Ok((Vec::new(), status)),
            None if status.is_ok() => Err(RpcError::Other("no response before End".into())),
            None => Err(RpcError::Status { code: status.code, message: status.message }),
        }
    }

    /// Invoke a server-streaming RPC.
    pub async fn invoke_server_streaming(
        &self,
        method: &str,
        req: &[u8],
    ) -> Result<ClientStream, RpcError> {
        self.open_stream(method, req, true).await
    }

    // ─── Internal ───────────────────────────────────────────

    async fn open_stream(
        &self,
        method: &str,
        req: &[u8],
        half_close: bool,
    ) -> Result<ClientStream, RpcError> {
        let seq = self.seq_alloc.fetch_add(2, Ordering::SeqCst);
        let (inbound_tx, inbound_rx) = mpsc::channel(16);
        let (end_tx, end_rx) = tokio::sync::oneshot::channel();

        // Build + send Call.
        let mut call = Call {
            method: method.to_string(),
            protocol_version: 1,
            ..Default::default()
        };
        if req.len() <= INLINE_MAX {
            call.inline_data = Some(req.to_vec());
        }

        self.send(Frame {
            routing: Some(Routing { sequence: seq }),
            r#type: Some(gen::frame::Type::Call(call)),
        })?;

        if req.len() > INLINE_MAX {
            self.send_payload(seq, req)?;
        }

        if half_close {
            self.send(Frame {
                routing: Some(Routing { sequence: seq }),
                r#type: Some(gen::frame::Type::End(End { close_send: true, ..Default::default() })),
            })?;
        }

        // Register stream state so the run loop can dispatch.
        self.streams.lock().await.insert(seq, StreamState {
            inbound: inbound_tx,
            end: Some(end_tx),
        });

        Ok(ClientStream {
            seq,
            inbound: inbound_rx,
            end: Some(end_rx),
            streams: self.streams.clone(),
        })
    }

    fn send(&self, frame: Frame) -> Result<(), RpcError> {
        self.outbound
            .send(encode_frame(&frame))
            .map_err(|_| RpcError::TransportClosed)
    }

    fn send_payload(&self, seq: i32, payload: &[u8]) -> Result<(), RpcError> {
        if payload.len() <= MESSAGE_MAX {
            return self.send(Frame {
                routing: Some(Routing { sequence: seq }),
                r#type: Some(gen::frame::Type::Data(Data {
                    content: Some(gen::data::Content::Message(payload.to_vec())),
                })),
            });
        }
        for offset in (0..payload.len()).step_by(CHUNK_SIZE) {
            let end = (offset + CHUNK_SIZE).min(payload.len());
            self.send(Frame {
                routing: Some(Routing { sequence: seq }),
                r#type: Some(gen::frame::Type::Data(Data {
                    content: Some(gen::data::Content::Chunk(Chunk {
                        total_size: payload.len() as i32,
                        offset: offset as i32,
                        data: payload[offset..end].to_vec(),
                    })),
                })),
            })?;
        }
        Ok(())
    }

    async fn run_loop<T: WireTransport>(
        mut transport: T,
        mut outbound: mpsc::UnboundedReceiver<bytes::Bytes>,
        streams: Arc<Mutex<HashMap<i32, StreamState>>>,
    ) {
        let mut reasm = Reassembler::new();
        let mut buf = Vec::new();

        loop {
            tokio::select! {
                // Drain outbound queue → transport.
                Some(frame) = outbound.recv() => {
                    transport.send_frame(frame).await;
                }
                // Read inbound from transport.
                result = transport.recv_frame() => {
                    match result {
                        None => break,
                        Some(bytes) => {
                            buf.extend_from_slice(&bytes);
                            // Decode as many frames as the buffer holds.
                            loop {
                                match try_decode_response_frame(&buf) {
                                    Ok(Some((resp, consumed))) => {
                                        buf.drain(..consumed);
                                        Self::dispatch(resp, &streams, &mut reasm).await;
                                    }
                                    Ok(None) => break, // need more bytes
                                    Err(e) => {
                                        tracing::warn!("decode error: {e}");
                                        buf.clear();
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    async fn dispatch(
        frame: peerrpc_protocol::ResponseFrame,
        streams: &Arc<Mutex<HashMap<i32, StreamState>>>,
        reasm: &mut Reassembler,
    ) {
        use gen::response_frame::Type as RType;

        let seq = frame.routing.map(|r| r.sequence).unwrap_or(0);
        let mut streams_guard = streams.lock().await;
        let Some(state) = streams_guard.get_mut(&seq) else {
            return;
        };

        match frame.r#type {
            Some(RType::Begin(begin)) => {
                if let Some(data) = begin.inline_data {
                    let _ = state.inbound.try_send(data);
                }
            }
            Some(RType::Data(data)) => {
                match data.content {
                    Some(gen::data::Content::Message(msg)) => {
                        let _ = state.inbound.try_send(msg);
                    }
                    Some(gen::data::Content::Chunk(chunk)) => {
                        if let Some(full) = reasm.reassemble(
                            seq,
                            chunk.total_size as usize,
                            chunk.offset as usize,
                            &chunk.data,
                        ) {
                            let _ = state.inbound.try_send(full);
                        }
                    }
                    None => {}
                }
            }
            Some(RType::End(end)) => {
                let status = end.status.map(|s| Status {
                    code: s.code,
                    message: s.message,
                }).unwrap_or_default();
                if let Some(end_tx) = state.end.take() {
                    let _ = end_tx.send(status);
                }
                streams_guard.remove(&seq);
            }
            None => {}
        }
    }
}

// ─── ClientStream ────────────────────────────────────────────

/// Per-RPC stream handle returned by `invoke_server_streaming`.
pub struct ClientStream {
    seq: i32,
    inbound: mpsc::Receiver<Vec<u8>>,
    end: Option<tokio::sync::oneshot::Receiver<Status>>,
    streams: Arc<Mutex<HashMap<i32, StreamState>>>,
}

impl ClientStream {
    /// Receive the next response message. Returns None at EOF.
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.inbound.recv().await
    }

    /// Wait for the End frame and return the final status.
    pub async fn wait_end(&mut self) -> Status {
        if let Some(end_rx) = self.end.take() {
            match end_rx.await {
                Ok(st) => st,
                Err(_) => Status::default(),
            }
        } else {
            Status::default()
        }
    }
}

impl Drop for ClientStream {
    fn drop(&mut self) {
        // Best-effort: remove the stream entry so the run loop does
        // not dispatch to a dropped receiver.
        let seq = self.seq;
        let streams = self.streams.clone();
        tokio::spawn(async move {
            streams.lock().await.remove(&seq);
        });
    }
}
