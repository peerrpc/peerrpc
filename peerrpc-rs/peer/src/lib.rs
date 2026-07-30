use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use peerrpc_rpc::WireTransport;
use peerrpc_signal::{SdpAnswer, SdpOffer, Session, SignalBody, SignalMessage};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, Notify};
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

impl Peer {
    pub async fn create_offer(cfg: RTCConfiguration) -> Result<(Self, String), PeerError> {
        let api = webrtc::api::APIBuilder::new().build();
        let pc = Arc::new(api.new_peer_connection(cfg).await?);

        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let open_notify = Arc::new(Notify::new());
        let dc = pc
            .create_data_channel(peerrpc_protocol::DATACHANNEL_LABEL, None)
            .await?;

        setup_on_message(&dc, inbound_tx);
        setup_on_open(&dc, open_notify.clone());

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
        };
        Ok((peer, munge_max_message_size(sdp)))
    }

    pub async fn accept_offer(
        cfg: RTCConfiguration,
        offer_sdp: String,
    ) -> Result<(Self, String), PeerError> {
        let api = webrtc::api::APIBuilder::new().build();
        let pc = Arc::new(api.new_peer_connection(cfg).await?);

        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let dc_handle: Arc<Mutex<Option<Arc<RTCDataChannel>>>> = Arc::new(Mutex::new(None));
        let dc_notify = Arc::new(Notify::new());
        let open_notify = Arc::new(Notify::new());

        let tx_cb = inbound_tx.clone();
        let dc_h = dc_handle.clone();
        let notify = dc_notify.clone();
        let on = open_notify.clone();
        pc.on_data_channel(Box::new(move |dc: Arc<RTCDataChannel>| {
            let tx = tx_cb.clone();
            let h = dc_h.clone();
            let n = notify.clone();
            let open = on.clone();
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
        if let Some(dc) = &self.dc {
            return dc.send(&frame).await.map(|_| ()).map_err(|e| e.to_string());
        }
        if let Some(dc) = self.dc_handle.lock().await.as_ref() {
            return dc.send(&frame).await.map(|_| ()).map_err(|e| e.to_string());
        }
        Err("peer: no data channel".into())
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

fn setup_on_open(dc: &Arc<RTCDataChannel>, notify: Arc<Notify>) {
    dc.on_open(Box::new(move || {
        let n = notify.clone();
        Box::pin(async move {
            n.notify_waiters();
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

    let _ = tokio::time::timeout(std::time::Duration::from_secs(10), notify.notified()).await;
}

/// The largest single RTCDataChannel.send() payload the peer should
/// attempt, in bytes. Matches the transport chunk threshold so a
/// chunked frame (256 KiB data + ~40 B envelope) fits.
const ADVERTISED_MAX_MESSAGE_SIZE: u32 = 256 * 1024;

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
