use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use peerrpc_rpc::WireTransport;
use peerrpc_signal::{Session, SignalBody, SdpOffer, SdpAnswer, SignalMessage};
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
        Ok((peer, sdp))
    }

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
        Ok((peer, sdp))
    }

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
        // without depending on internal types. v2.1 will add a proper
        // adapter; for now localhost-only and STUN-only deployments
        // work without trickle, and TURN-only environments degrade
        // to longer connection setup (full gathering before signaling).
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
                None => return Err(PeerError::Other("signal session closed before offer".into())),
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
    async fn send_frame(&mut self, frame: Bytes) {
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

    let _ = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        notify.notified(),
    )
    .await;
}
