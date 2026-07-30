//! WireTransport trait: abstracts the raw send/recv of length-
//! prefixed PeerRPC wire bytes.
//!
//! Production callers implement this against a webrtc-rs DataChannel
//! or any other duplex byte stream. Tests use an in-memory mock.

use async_trait::async_trait;
use bytes::Bytes;

/// The byte-stream transport the Client multiplexes over.
#[async_trait]
pub trait WireTransport: Send + 'static {
    /// Send raw length-prefixed wire bytes. Returns Err when the
    /// underlying transport is dead (e.g. DataChannel closed); the
    /// run loop treats this as a fatal condition and stops.
    async fn send_frame(&mut self, frame: Bytes) -> Result<(), String>;

    /// Receive the next inbound payload. Returns None when the
    /// transport is closed.
    async fn recv_frame(&mut self) -> Option<Bytes>;
}
