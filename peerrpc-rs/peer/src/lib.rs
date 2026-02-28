//! PeerRPC peer layer: wraps webrtc-rs into a WireTransport.
//!
//! The adapter:
//!   1. Creates an RTCPeerConnection.
//!   2. Creates or accepts a DataChannel (label = "peerrpc-v1").
//!   3. Bridges OnMessage → recv_frame and send_frame → Send.

use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use peerrpc_rpc::WireTransport;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_gathering_state::RTCIceGatheringState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

pub use webrtc::peer_connection::configuration::RTCConfiguration as PeerConfig;

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("peer: {0}")]
    Other(String),
    #[error("peer: webrtc: {0}")]
    WebRTC(#[from] webrtc::Error),
    #[error("peer: timeout: {0}")]
    Timeout(String),
}

/// A connected peer wrapping RTCPeerConnection + DataChannel.
pub struct Peer {
    #[allow(dead_code)]
    pc: Arc<RTCPeerConnection>,
    /// For the Offerer flow, dc is set immediately. For the Answerer
    /// flow, dc_handle is set when on_data_channel fires; the caller
    /// must call wait_for_data_channel() after sending the answer.
    dc: Option<Arc<RTCDataChannel>>,
    dc_handle: Arc<Mutex<Option<Arc<RTCDataChannel>>>>,
    dc_notify: Arc<Notify>,
    inbound_rx: Mutex<mpsc::UnboundedReceiver<Bytes>>,
}

impl Peer {
    /// Offerer flow: create DataChannel + offer.
    /// Returns (Peer, offer_sdp). Caller must then send the offer
    /// via signaling, receive the answer, and call set_remote_answer.
    pub async fn create_offer(cfg: RTCConfiguration) -> Result<(Self, String), PeerError> {
        let api = webrtc::api::APIBuilder::new().build();
        let pc = Arc::new(api.new_peer_connection(cfg).await?);

        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let dc = pc
            .create_data_channel(peerrpc_protocol::DATACHANNEL_LABEL, None)
            .await?;

        setup_on_message(&dc, inbound_tx);

        let offer = pc.create_offer(None).await?;
        pc.set_local_description(offer).await?;
        wait_ice_complete(&pc).await;

        let sdp = pc
            .local_description()
            .await
            .ok_or_else(|| PeerError::Other("no local description".into()))?
            .sdp;

        let peer = Self {
            pc,
            dc: Some(dc),
            dc_handle: Arc::new(Mutex::new(None)),
            dc_notify: Arc::new(Notify::new()),
            inbound_rx: Mutex::new(inbound_rx),
        };
        Ok((peer, sdp))
    }

    /// Answerer flow (two-phase):
    ///   Phase 1: apply offer + create answer.
    ///   Phase 2: caller sends the answer, then waits for DataChannel.
    ///
    /// Returns (Answerer, answer_sdp). The caller MUST then:
    ///   1. Send answer_sdp to the remote via signaling.
    ///   2. Call answerer.wait_for_data_channel().
    pub async fn accept_offer(
        cfg: RTCConfiguration,
        offer_sdp: String,
    ) -> Result<(Self, String), PeerError> {
        let api = webrtc::api::APIBuilder::new().build();
        let pc = Arc::new(api.new_peer_connection(cfg).await?);

        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let dc_handle: Arc<Mutex<Option<Arc<RTCDataChannel>>>> =
            Arc::new(Mutex::new(None));
        let dc_notify = Arc::new(Notify::new());

        let tx_cb = inbound_tx.clone();
        let dc_h = dc_handle.clone();
        let notify = dc_notify.clone();
        pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let tx = tx_cb.clone();
            let h = dc_h.clone();
            let n = notify.clone();
            dc.on_message(Box::new(move |msg: DataChannelMessage| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(Bytes::copy_from_slice(&msg.data));
                })
            }));
            Box::pin(async move {
                *h.lock().await = Some(dc);
                n.notify_one();
            })
        }));

        pc.set_remote_description(RTCSessionDescription::offer(offer_sdp)?)
            .await?;

        let answer = pc.create_answer(None).await?;
        pc.set_local_description(answer).await?;
        wait_ice_complete(&pc).await;

        let sdp = pc
            .local_description()
            .await
            .ok_or_else(|| PeerError::Other("no local description".into()))?
            .sdp;

        let peer = Self {
            pc,
            dc: None, // set by wait_for_data_channel
            dc_handle,
            dc_notify,
            inbound_rx: Mutex::new(inbound_rx),
        };
        Ok((peer, sdp))
    }

    /// Wait for the remote to open the DataChannel. Called AFTER the
    /// answer has been sent via signaling.
    pub async fn wait_for_data_channel(&self, timeout: Duration) -> Result<Arc<RTCDataChannel>, PeerError> {
        tokio::time::timeout(timeout, self.dc_notify.notified())
            .await
            .map_err(|_| PeerError::Timeout("DataChannel open".into()))?;

        self.dc_handle
            .lock()
            .await
            .clone()
            .ok_or_else(|| PeerError::Other("no DC after notify".into()))
    }

    /// Set the remote answer (Offerer side).
    pub async fn set_remote_answer(&self, sdp: String) -> Result<(), PeerError> {
        self.pc
            .set_remote_description(RTCSessionDescription::answer(sdp)?)
            .await?;
        Ok(())
    }

    /// Close the PeerConnection.
    pub async fn close(&self) -> Result<(), PeerError> {
        self.pc.close().await?;
        Ok(())
    }
}

#[async_trait]
impl WireTransport for Peer {
    async fn send_frame(&mut self, frame: Bytes) {
        // For the Offerer flow, dc is set. For the Answerer flow,
        // the caller must call wait_for_data_channel first, which
        // populates dc_handle. We try dc first, then dc_handle.
        if let Some(dc) = &self.dc {
            let _ = dc.send(&frame).await;
            return;
        }
        if let Some(dc) = self.dc_handle.lock().await.as_ref() {
            let _ = dc.send(&frame).await;
        }
    }

    async fn recv_frame(&mut self) -> Option<Bytes> {
        self.inbound_rx.lock().await.recv().await
    }
}

fn setup_on_message(dc: &Arc<RTCDataChannel>, tx: mpsc::UnboundedSender<Bytes>) {
    dc.on_message(Box::new(move |msg: DataChannelMessage| {
        let tx = tx.clone();
        Box::pin(async move {
            let _ = tx.send(Bytes::copy_from_slice(&msg.data));
        })
    }));
}

async fn wait_ice_complete(pc: &Arc<RTCPeerConnection>) {
    let notify = Arc::new(Notify::new());
    let n = notify.clone();

    pc.on_ice_candidate(Box::new(move |c| {
        if c.is_none() {
            n.notify_one();
        }
        Box::pin(async {})
    }));

    if pc.ice_gathering_state() == RTCIceGatheringState::Complete {
        return;
    }

    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        notify.notified(),
    )
    .await;
}
