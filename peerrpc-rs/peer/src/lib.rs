use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use peerrpc_rpc::WireTransport;
use peerrpc_signal::{SdpAnswer, SdpOffer, Session, SignalBody, SignalMessage};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};
use webrtc::api::setting_engine::{SctpMaxMessageSize, SettingEngine};
use webrtc::data_channel::data_channel_message::DataChannelMessage;
use webrtc::data_channel::RTCDataChannel;
use webrtc::ice_transport::ice_gathering_state::RTCIceGatheringState;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;

pub use webrtc::peer_connection::configuration::RTCConfiguration as PeerConfig;

#[derive(Debug, thiserror::Error)]
pub enum PeerError {
    #[error("peer: {0}")]
    Other(String),
    #[error("peer: webrtc: {0}")]
    WebRTC(#[from] webrtc::Error),
    #[error("peer: timeout: {0}")]
    Timeout(String),
    #[error("peer: signal: {0}")]
    Signal(#[from] peerrpc_signal::SignalError),
}

pub struct Peer {
    #[allow(dead_code)]
    pc: Arc<RTCPeerConnection>,
    dc: Option<Arc<RTCDataChannel>>,
    dc_handle: Arc<Mutex<Option<Arc<RTCDataChannel>>>>,
    dc_notify: Arc<Notify>,
    open_notify: Arc<Notify>,
    inbound_rx: Mutex<mpsc::UnboundedReceiver<Bytes>>,
    /// Outbound backpressure: notified when the DataChannel's buffered
    /// amount drops below BUFFERED_AMOUNT_HIGH. send_frame awaits this
    /// before writing so a burst of large frames (e.g. a 1 MiB echo
    /// chunked into 4×255 KiB) does not overflow the SCTP send buffer
    /// and tear down the association.
    buffered_low: Arc<Notify>,
    /// Closed signal for the backpressure path.
    closed: Arc<Notify>,
}

impl Drop for Peer {
    fn drop(&mut self) {
        let pc = self.pc.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                let _ = pc.close().await;
            });
        }
    }
}

/// Build a webrtc-rs API with the peerrpc-recommended SettingEngine.
///
/// The key knob is `sctp_max_message_size_can_send = Unbounded`. webrtc-rs
/// computes the negotiated SCTP max-message-size as
/// `min(remote_cap, can_send)`; the default `can_send` is 64 KiB, which
/// caps the negotiation even when the remote SDP advertises 256 KiB
/// (the munging in `munge_max_message_size`). Setting `can_send =
/// Unbounded` (= 0) makes webrtc-rs pick the remote value directly, so
/// when the peer's SDP carries `a=max-message-size:262144` the
/// negotiation reaches 256 KiB. With pion (Go) the default is already
/// 262144 so the negotiation reaches 256 KiB there too.
fn build_api() -> webrtc::api::API {
    let mut engine = SettingEngine::default();
    engine.set_sctp_max_message_size_can_send(SctpMaxMessageSize::Unbounded);
    webrtc::api::APIBuilder::new()
        .with_setting_engine(engine)
        .build()
}

impl Peer {
    pub async fn create_offer(cfg: RTCConfiguration) -> Result<(Self, String), PeerError> {
        let api = build_api();
        let pc = Arc::new(api.new_peer_connection(cfg).await?);

        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let open_notify = Arc::new(Notify::new());
        let buffered_low = Arc::new(Notify::new());
        let closed = Arc::new(Notify::new());
        let dc = pc
            .create_data_channel(peerrpc_protocol::DATACHANNEL_LABEL, None)
            .await?;

        setup_on_message(&dc, inbound_tx);
        setup_on_open(&dc, open_notify.clone());
        setup_backpressure(&dc, buffered_low.clone(), closed.clone());

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
            open_notify,
            inbound_rx: Mutex::new(inbound_rx),
            buffered_low,
            closed,
        };
        Ok((peer, munge_max_message_size(sdp)))
    }

    pub async fn accept_offer(
        cfg: RTCConfiguration,
        offer_sdp: String,
    ) -> Result<(Self, String), PeerError> {
        let api = build_api();
        let pc = Arc::new(api.new_peer_connection(cfg).await?);

        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let dc_handle: Arc<Mutex<Option<Arc<RTCDataChannel>>>> = Arc::new(Mutex::new(None));
        let dc_notify = Arc::new(Notify::new());
        let open_notify = Arc::new(Notify::new());
        let buffered_low = Arc::new(Notify::new());
        let closed = Arc::new(Notify::new());

        let tx_cb = inbound_tx.clone();
        let dc_h = dc_handle.clone();
        let notify = dc_notify.clone();
        let on = open_notify.clone();
        let bl = buffered_low.clone();
        let cl = closed.clone();
        pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let tx = tx_cb.clone();
            let h = dc_h.clone();
            let n = notify.clone();
            let open = on.clone();
            let bl = bl.clone();
            let cl = cl.clone();
            dc.on_message(Box::new(move |msg: DataChannelMessage| {
                let tx = tx.clone();
                Box::pin(async move {
                    let _ = tx.send(Bytes::copy_from_slice(&msg.data));
                })
            }));
            dc.on_open(Box::new(move || {
                let n = open.clone();
                Box::pin(async move {
                    n.notify_waiters();
                })
            }));
            setup_backpressure(&dc, bl, cl);
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
            dc: None,
            dc_handle,
            dc_notify,
            open_notify,
            inbound_rx: Mutex::new(inbound_rx),
            buffered_low,
            closed,
        };
        Ok((peer, munge_max_message_size(sdp)))
    }

    pub async fn wait_for_data_channel(
        &self,
        timeout: Duration,
    ) -> Result<Arc<RTCDataChannel>, PeerError> {
        tokio::time::timeout(timeout, self.dc_notify.notified())
            .await
            .map_err(|_| PeerError::Timeout("DataChannel open".into()))?;

        self.dc_handle
            .lock()
            .await
            .clone()
            .ok_or_else(|| PeerError::Other("no DC after notify".into()))
    }

    pub async fn set_remote_answer(&self, sdp: String) -> Result<(), PeerError> {
        self.pc
            .set_remote_description(RTCSessionDescription::answer(sdp)?)
            .await?;
        Ok(())
    }

    pub async fn wait_for_open(&self, timeout: Duration) -> Result<(), PeerError> {
        tokio::time::timeout(timeout, self.open_notify.notified())
            .await
            .map_err(|_| PeerError::Timeout("DataChannel open".into()))?;
        Ok(())
    }

    pub async fn close(&self) -> Result<(), PeerError> {
        self.pc.close().await?;
        Ok(())
    }

    pub async fn dial(
        cfg: RTCConfiguration,
        mut sig: Session,
        timeout: Duration,
    ) -> Result<Self, PeerError> {
        let (peer, offer_sdp) = Self::create_offer(cfg).await?;

        sig.send(SignalMessage {
            service: sig.service().to_string(),
            body: SignalBody {
                offer: Some(SdpOffer { sdp: offer_sdp }),
                ..Default::default()
            },
        })?;

        // Vanilla ICE: all candidates are embedded in the offer/answer
        // SDP (create_offer / accept_offer wait for gathering complete
        // before returning the SDP). Trickled ICE candidate forwarding
        // is intentionally omitted here because webrtc-rs's candidate
        // trait-object shape is awkward to plumb through on_ice_candidate
        // without depending on internal types.
        while let Some(msg) = sig.recv().await {
            if let Some(answer) = msg.body.answer {
                peer.set_remote_answer(answer.sdp).await?;
                break;
            }
        }

        peer.wait_for_open(timeout).await?;
        Ok(peer)
    }

    pub async fn accept(
        cfg: RTCConfiguration,
        mut sig: Session,
        timeout: Duration,
    ) -> Result<Self, PeerError> {
        let offer_sdp = loop {
            match sig.recv().await {
                Some(msg) if msg.body.offer.is_some() => {
                    break msg.body.offer.unwrap().sdp;
                }
                Some(_) => continue,
                None => {
                    return Err(PeerError::Other(
                        "signal session closed before offer".into(),
                    ))
                }
            }
        };

        let (peer, answer_sdp) = Self::accept_offer(cfg, offer_sdp).await?;

        sig.send(SignalMessage {
            service: sig.service().to_string(),
            body: SignalBody {
                answer: Some(SdpAnswer { sdp: answer_sdp }),
                ..Default::default()
            },
        })?;

        peer.wait_for_open(timeout).await?;
        peer.wait_for_data_channel(timeout).await?;
        Ok(peer)
    }
}

#[async_trait]
impl WireTransport for Peer {
    async fn send_frame(&mut self, frame: Bytes) -> Result<(), String> {
        let dc = if let Some(dc) = &self.dc {
            dc.clone()
        } else {
            match self.dc_handle.lock().await.clone() {
                Some(dc) => dc,
                None => return Err("peer: no data channel".into()),
            }
        };

        // Apply outbound backpressure before each write: if the SCTP
        // send buffer is above the high watermark, wait for it to drain
        // (or for the channel to close). Without this, a burst of large
        // frames — e.g. a 1 MiB echo chunked into N frames — overflows
        // the buffer and webrtc-rs tears down the association.
        self.await_buffer_low(&dc).await?;

        let res = dc.send(&frame).await.map(|_| ()).map_err(|e| e.to_string());
        if res.is_ok() {
            // We just enqueued a potentially large frame. Wait for it
            // (and any pending frames) to drain below the watermark
            // BEFORE returning, so the next send() call does not stack
            // another big frame on top. Otherwise webrtc-rs 0.17's SCTP
            // stream buffer overflows and tears down the association
            // (the 5×255 KiB chunk pattern of LargeEcho reproduces
            // this without the post-send wait).
            self.await_buffer_low(&dc).await?;
        }
        res
    }

    async fn recv_frame(&mut self) -> Option<Bytes> {
        self.inbound_rx.lock().await.recv().await
    }
}

impl Peer {
    /// Block until the DataChannel's buffered amount is below
    /// BUFFERED_AMOUNT_HIGH, or the channel closes. Mirrors the Go
    /// transport's awaitBufferLow.
    async fn await_buffer_low(&self, dc: &Arc<RTCDataChannel>) -> Result<(), String> {
        loop {
            if dc.buffered_amount().await < peerrpc_protocol::BUFFERED_AMOUNT_HIGH as usize {
                return Ok(());
            }
            // Wait for the low-watermark notification (armed via
            // on_buffered_amount_low in setup_backpressure).
            tokio::select! {
                _ = self.buffered_low.notified() => {}
                _ = self.closed.notified() => {
                    return Err("peer: data channel closed while waiting for backpressure".into());
                }
            }
        }
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

/// Wire outbound backpressure on a DataChannel: emit the low-watermark
/// Notify whenever the buffered amount drops below
/// BUFFERED_AMOUNT_HIGH, and configure that threshold. Mirrors the
/// Go transport's OnBufferedAmountLow handling so a burst of large
/// frames does not overflow the SCTP send buffer.
///
/// `on_buffered_amount_low` and `set_buffered_amount_low_threshold`
/// are async in webrtc-rs 0.17, so they run on spawned tasks; `on_close`
/// is still sync and is registered inline (this fn is called from
/// both async and sync-on_data_channel contexts).
fn setup_backpressure(dc: &Arc<RTCDataChannel>, buffered_low: Arc<Notify>, closed: Arc<Notify>) {
    // Wire up on_buffered_amount_low: wake any blocked senders.
    // This method is async in webrtc-rs 0.17; spawn a task to
    // register the handler (mirrors transport::Channel::new).
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

    // set_buffered_amount_low_threshold is async, so run it on a task.
    let dc_clone = dc.clone();
    tokio::spawn(async move {
        dc_clone
            .set_buffered_amount_low_threshold(peerrpc_protocol::BUFFERED_AMOUNT_HIGH as usize)
            .await;
    });

    // Tear down sends if the DataChannel closes. on_close is sync in
    // webrtc-rs 0.17, so register it inline.
    let cl = closed.clone();
    dc.on_close(Box::new(move || {
        let cl = cl.clone();
        Box::pin(async move {
            cl.notify_waiters();
        })
    }));
}

fn setup_on_open(dc: &Arc<RTCDataChannel>, notify: Arc<Notify>) {
    dc.on_open(Box::new(move || {
        let n = notify.clone();
        Box::pin(async move {
            n.notify_waiters();
        })
    }))
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

    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), notify.notified()).await;
}

/// The largest single RTCDataChannel.send() payload the peer should
/// attempt, in bytes. Matches peerrpc_protocol::MAX_FRAME_SIZE so a
/// chunked frame (255 KiB data + ~40 B envelope) fits when both sides
/// negotiate ≥256 KiB.
const ADVERTISED_MAX_MESSAGE_SIZE: u32 = 262144;

/// Inject `a=max-message-size` into the application media section of
/// the SDP. webrtc-rs does not emit this attribute, so per RFC 8831 the
/// peer (browser) defaults to 65535 (64 KiB) and rejects any larger
/// send with "Trying to send message larger than max-message-size".
/// Advertising a larger value lets the browser send chunk-sized frames.
///
/// If the attribute is already present it is left untouched. Only the
/// `m=application` section is affected. The original SDP's line endings
/// are preserved by inserting the attribute as a raw substring right
/// after the `m=application` line.
fn munge_max_message_size(sdp: String) -> String {
    if sdp.contains("a=max-message-size") {
        return sdp; // already advertised (e.g. round-tripped)
    }
    // Detect the line terminator webrtc-rs used so we match it.
    let nl = if sdp.contains("\r\n") { "\r\n" } else { "\n" };
    let attr = format!("a=max-message-size:{}{}", ADVERTISED_MAX_MESSAGE_SIZE, nl);

    // Find the start of the m=application line and inject the attribute
    // right after that line (beginning of its media section).
    if let Some(idx) = sdp.find("m=application") {
        // End of the m=application line.
        if let Some(line_end) = sdp[idx..].find(nl) {
            let insert_at = idx + line_end + nl.len();
            let mut out = String::with_capacity(sdp.len() + attr.len());
            out.push_str(&sdp[..insert_at]);
            out.push_str(&attr);
            out.push_str(&sdp[insert_at..]);
            return out;
        }
    }
    // No application section (unexpected for a DataChannel SDP); return
    // unchanged rather than risk corrupting the SDP.
    sdp
}
