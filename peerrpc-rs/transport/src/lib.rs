//! PeerRPC transport layer.
//!
//! Provides:
//! - [`Channel`]: DataChannel wrapper with length-prefixed frame I/O,
//!   backpressure, and chunk reassembly.
//! - [`Reassembler`]: collects Chunk frames into complete payloads.

pub mod channel;
pub mod reassembler;

pub use channel::{Channel, ChannelError, DATACHANNEL_LABEL};
pub use reassembler::Reassembler;
