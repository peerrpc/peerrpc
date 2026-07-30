//! DataChannel wrapper with length-prefixed frame I/O, backpressure,
//! and chunk reassembly.
//!
//! Layering (top to bottom):
//!
//!   rpc::Client / rpc::Server
//!       │  Frame / ResponseFrame (proto messages)
//!       ▼
//!   transport::Channel::send_frame / recv
//!       │  transparent Chunk split / reassembly
//!       ▼
//!   webrtc::data_channel::RTCDataChannel (ordered, reliable)

use std::sync::Arc;

use bytes::Bytes;
use futures_util::FutureExt;
use peerrpc_protocol::{
    try_decode_frame, try_decode_response_frame, DecodeError, Frame, ResponseFrame,
    BUFFERED_AMOUNT_HIGH, MAX_FRAME_SIZE,
};
use prost::Message;
use tokio::sync::{mpsc, Mutex, Notify};
use webrtc::data_channel::RTCDataChannel;

use crate::Reassembler;

/// DataChannel label carrying the protocol version on the wire.
pub const DATACHANNEL_LABEL: &str = "peerrpc-v1";

/// Errors returned by Channel methods.
#[derive(Debug, thiserror::Error)]
pub enum ChannelError {
    #[error("transport: channel closed: {0}")]
    Closed(String),
    #[error("transport: send: {0}")]
    Send(String),
    #[error("transport: receive: {0}")]
    Recv(String),
    #[error("transport: marshal: {0}")]
    Marshal(#[from] prost::EncodeError),
    #[error("transport: decode: {0}")]
    Decode(#[from] DecodeError),
}

/// Channel is the transport-level duplex pipe backed by a WebRTC
/// DataChannel.
///
/// A Channel is safe to share across tasks for sending. It is intended
/// for a single reader (the multiplexer) but that reader may dispatch
/// decoded frames to many streams.
pub struct Channel {
    dc: Arc<RTCDataChannel>,

    /// Outbound backpressure: notified when buffered amount drops below
    /// the threshold.
    buffered_low: Arc<Notify>,

    /// Inbound queue of raw DataChannel messages.
    recv_rx: Mutex<mpsc::Receiver<Bytes>>,
    _recv_tx: mpsc::Sender<Bytes>,

    /// Chunk reassembly for large payloads split across Data frames.
    reasm: Mutex<Reassembler>,

    /// Closed signal.
    closed: Arc<Notify>,
    close_reason: Mutex<Option<String>>,
}

impl Channel {
    /// Wrap an established DataChannel.
    ///
    /// The DataChannel MUST be created with ordered=true, reliable
    /// delivery. This constructor registers OnMessage / OnClose /
    /// OnBufferedAmountLow handlers.
    pub fn new(dc: Arc<RTCDataChannel>) -> Arc<Self> {
        let (recv_tx, recv_rx) = mpsc::channel(256);
        let buffered_low = Arc::new(Notify::new());
        let closed = Arc::new(Notify::new());

        // Wire up on_message: push inbound bytes into the mpsc channel.
        let tx = recv_tx.clone();
        dc.on_message(Box::new(move |msg| {
            let tx = tx.clone();
            Box::pin(async move {
                let _ = tx.send(Bytes::copy_from_slice(&msg.data)).await;
            })
        }));

        // Wire up on_buffered_amount_low: wake any blocked senders.
        // This method is async; we spawn a task to register the handler.
        let bl = buffered_low.clone();
        let dc_clone = dc.clone();
        tokio::spawn(async move {
            dc_clone
                .on_buffered_amount_low(Box::new(move || {
                    let bl = bl.clone();
                    Box::pin(async move {
                        bl.notify_waiters();
                    })
                }))
                .await;
        });

        // Wire up on_close: signal the closed Notify.
        let cl = closed.clone();
        dc.on_close(Box::new(move || {
            let cl = cl.clone();
            Box::pin(async move {
                cl.notify_waiters();
            })
        }));

        // Set the low watermark threshold asynchronously.
        let dc_clone = dc.clone();
        tokio::spawn(async move {
            dc_clone
                .set_buffered_amount_low_threshold(BUFFERED_AMOUNT_HIGH as usize)
                .await;
        });

        Arc::new(Self {
            dc,
            buffered_low,
            recv_rx: Mutex::new(recv_rx),
            _recv_tx: recv_tx,
            reasm: Mutex::new(Reassembler::new()),
            closed,
            close_reason: Mutex::new(None),
        })
    }

    /// Send a protobuf Frame with length prefix.
    pub async fn send_frame(&self, frame: &Frame) -> Result<(), ChannelError> {
        let mut buf = Vec::new();
        Message::encode(frame, &mut buf)?;
        self.send_raw(&buf).await
    }

    /// Send a protobuf ResponseFrame with length prefix.
    pub async fn send_response_frame(&self, frame: &ResponseFrame) -> Result<(), ChannelError> {
        let mut buf = Vec::new();
        Message::encode(frame, &mut buf)?;
        self.send_raw(&buf).await
    }

    /// Send pre-encoded (length-prefixed) bytes verbatim through the
    /// DataChannel. Used by the relay, which forwards frames without
    /// inspecting them.
    ///
    /// `payload` MUST already include the 4-byte length prefix.
    pub async fn send_raw(&self, payload: &[u8]) -> Result<(), ChannelError> {
        // Check closed first (non-blocking poll).
        if self.closed.notified().now_or_never().is_some() {
            let reason = self.close_reason.lock().await.clone();
            return Err(ChannelError::Closed(
                reason.unwrap_or_else(|| "unknown".into()),
            ));
        }

        // Apply backpressure.
        self.await_buffer_low().await?;

        let bytes = Bytes::copy_from_slice(payload);
        self.dc
            .send(&bytes)
            .await
            .map_err(|e| ChannelError::Send(e.to_string()))?;

        Ok(())
    }

    /// Await until the DataChannel's buffered amount is below
    /// BUFFERED_AMOUNT_HIGH (or the channel closes).
    async fn await_buffer_low(&self) -> Result<(), ChannelError> {
        loop {
            let ba = self.dc.buffered_amount().await;
            if ba < BUFFERED_AMOUNT_HIGH as usize {
                return Ok(());
            }

            // Wait for the low watermark notification.
            tokio::select! {
                _ = self.buffered_low.notified() => {
                    // Re-check in the next iteration.
                }
                _ = self.closed.notified() => {
                    let reason = self.close_reason.lock().await.clone();
                    return Err(ChannelError::Closed(
                        reason.unwrap_or_else(|| "closed while waiting for backpressure".into()),
                    ));
                }
            }
        }
    }

    /// Receive the next raw payload (length prefix stripped) from the
    /// DataChannel. Blocks until a message arrives or the channel
    /// closes.
    ///
    /// This is the low-level receive. Higher layers typically decode
    /// the payload into a Frame or ResponseFrame.
    pub async fn recv_raw(&self) -> Result<Bytes, ChannelError> {
        let mut rx = self.recv_rx.lock().await;
        tokio::select! {
            msg = rx.recv() => {
                match msg {
                    Some(b) => Ok(Self::strip_length_prefix(&b)),
                    None => {
                        let reason = self.close_reason.lock().await.clone();
                        Err(ChannelError::Closed(
                            reason.unwrap_or_else(|| "receiver dropped".into()),
                        ))
                    }
                }
            }
            _ = self.closed.notified() => {
                let reason = self.close_reason.lock().await.clone();
                Err(ChannelError::Closed(
                    reason.unwrap_or_else(|| "channel closed".into()),
                ))
            }
        }
    }

    /// Receive and decode the next Frame from the DataChannel.
    pub async fn recv_frame(&self) -> Result<Frame, ChannelError> {
        let raw = self.recv_raw().await?;
        match try_decode_frame(&raw)? {
            Some((frame, _consumed)) => Ok(frame),
            None => Err(ChannelError::Decode(DecodeError::Oversized {
                length: raw.len(),
                max: MAX_FRAME_SIZE,
            })),
        }
    }

    /// Receive and decode the next ResponseFrame from the DataChannel.
    pub async fn recv_response_frame(&self) -> Result<ResponseFrame, ChannelError> {
        let raw = self.recv_raw().await?;
        match try_decode_response_frame(&raw)? {
            Some((frame, _consumed)) => Ok(frame),
            None => Err(ChannelError::Decode(DecodeError::Oversized {
                length: raw.len(),
                max: MAX_FRAME_SIZE,
            })),
        }
    }

    /// Fold one chunk into the per-sequence buffer and return the
    /// assembled payload when complete.
    pub async fn reassemble(
        &self,
        seq: i32,
        total: usize,
        offset: usize,
        data: &[u8],
    ) -> Option<Vec<u8>> {
        self.reasm.lock().await.reassemble(seq, total, offset, data)
    }

    /// Returns a reference to the closed Notify.
    pub fn closed_notify(&self) -> &Arc<Notify> {
        &self.closed
    }

    /// Close the DataChannel.
    pub async fn close(&self) -> Result<(), ChannelError> {
        self.dc
            .close()
            .await
            .map_err(|e| ChannelError::Send(e.to_string()))?;
        self.shutdown("closed by Close()".into()).await;
        Ok(())
    }

    async fn shutdown(&self, reason: String) {
        *self.close_reason.lock().await = Some(reason);
        self.closed.notify_waiters();
    }

    /// Strip the 4-byte big-endian length prefix from an inbound
    /// DataChannel message. If invalid, returns the raw bytes.
    fn strip_length_prefix(b: &[u8]) -> Bytes {
        if b.len() < 4 {
            return Bytes::copy_from_slice(b);
        }
        let length = u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize;
        if length == 0 || 4 + length > b.len() {
            return Bytes::copy_from_slice(b);
        }
        Bytes::copy_from_slice(&b[4..4 + length])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strip_length_prefix() {
        let payload = b"hello";
        let mut prefixed = Vec::with_capacity(4 + payload.len());
        prefixed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        prefixed.extend_from_slice(payload);

        let result = Channel::strip_length_prefix(&prefixed);
        assert_eq!(&result[..], payload);
    }

    #[test]
    fn test_strip_length_prefix_too_short() {
        let result = Channel::strip_length_prefix(b"ab");
        assert_eq!(&result[..], b"ab");
    }

    #[test]
    fn test_strip_length_prefix_invalid_length() {
        let buf = vec![0xFF, 0xFF, 0xFF, 0xFF, 0x01, 0x02];
        let result = Channel::strip_length_prefix(&buf);
        assert_eq!(&result[..], &buf[..]);
    }

    #[test]
    fn test_strip_length_prefix_zero_length() {
        let buf = vec![0x00, 0x00, 0x00, 0x00];
        let result = Channel::strip_length_prefix(&buf);
        assert_eq!(&result[..], &buf[..]);
    }
}
