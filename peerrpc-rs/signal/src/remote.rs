//! Remote backend: tonic-based signaling client that talks to a
//! remote signal-server over the v2 proto.
//!
//! Mirrors Go's `signal.Remote` and TS's `ConnectSignalV2`. The
//! caller hands us a base URL (e.g. "http://signal.example.com:8080")
//! and a (service, peer_id) pair; we open a bidi stream, send the
//! AnnounceRequest as the first message, and pump SDP/ICE envelopes
//! both ways.
//!
//! Only the v2 wire format is spoken here. v1 callers should keep
//! using Local or migrate via the @peerrpc/peerrpc facade (which
//! hides the proto version entirely).

use std::sync::Arc;

use tokio::sync::mpsc;
use tonic::Request;

#[allow(dead_code)]
mod gen {
    include!(concat!(env!("OUT_DIR"), "/peerrpc.signaling.v2.rs"));
}

use gen::{
    signaling_service_client::SignalingServiceClient, AnnounceRequest, SignalMessage as WireMsg,
};

use crate::{IceCandidate, SdpAnswer, SignalBody, SdpOffer, Session, SignalError, SignalMessage};

/// Remote speaks the v2 signaling wire format over HTTP/2 (via tonic).
pub struct Remote {
    base_url: String,
}

impl Remote {
    /// `base_url` is the signal-server root (e.g.
    /// "http://signal.example.com:8080"). A trailing slash is
    /// stripped.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
        }
    }

    /// Open a bidi Exchange stream against (service, peer_id) and
    /// return a Session that ferries SignalMessages both ways.
    pub async fn exchange(
        &self,
        service: &str,
        peer_id: &str,
    ) -> Result<Session, SignalError> {
        if service.is_empty() {
            return Err(SignalError::Other("empty service".into()));
        }
        if peer_id.is_empty() {
            return Err(SignalError::Other("empty peer id".into()));
        }

        let base_url = self.base_url.trim_end_matches('/').to_string();
        let endpoint = tonic::transport::Endpoint::from_shared(base_url.clone())
            .map_err(|e| SignalError::Other(format!("invalid base_url: {e}")))?
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(60));
        // Lazy channel: actual TCP/TLS connect happens on first RPC.
        // Simpler than eagerly awaiting connect(); failure surfaces
        // as a transport error on the first exchange() call.
        let channel = endpoint
            .connect_lazy();

        let mut client = SignalingServiceClient::new(channel);

        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel::<WireMsg>();
        let (inbound_tx, mut inbound_rx_from_stream) = mpsc::unbounded_channel::<WireMsg>();

        // First message: AnnounceRequest.
        let announce = WireMsg {
            service: service.to_string(),
            body: Some(gen::signal_message::Body::Announce(AnnounceRequest {
                peer_id: peer_id.to_string(),
                peer_pubkey: None, // v2 accepts but does not verify; v2.1 wires this.
                role: gen::announce_request::Role::Client as i32,
            })),
        };
        outbound_tx
            .send(announce)
            .map_err(|_| SignalError::Closed)?;

        // Spawn the bidi stream pump.
        let svc = service.to_string();
        let peer = peer_id.to_string();
        tokio::spawn(async move {
            let stream = outbound_to_stream(outbound_rx);
            let exchange_result = client.exchange(Request::new(stream)).await;
            if let Ok(resp) = exchange_result {
                let mut stream = resp.into_inner();
                loop {
                    match stream.message().await {
                        Ok(Some(msg)) => {
                            if inbound_tx.send(msg).is_err() {
                                break;
                            }
                        }
                        Ok(None) => break,
                        Err(_) => break,
                    }
                }
            }
        });

        // Bridge: convert wire ↔ public SignalMessage types.
        let (pub_inbound_tx, pub_inbound_rx) = mpsc::unbounded_channel::<SignalMessage>();
        tokio::spawn(async move {
            while let Some(wire) = inbound_rx_from_stream.recv().await {
                let translated = translate_in(&wire);
                if pub_inbound_tx.send(translated).is_err() {
                    break;
                }
            }
        });

        let (pub_outbound_tx, mut pub_outbound_rx) = mpsc::unbounded_channel::<SignalMessage>();
        let svc_for_bridge = svc.clone();
        tokio::spawn(async move {
            while let Some(msg) = pub_outbound_rx.recv().await {
                let wire = translate_out(&msg, &svc_for_bridge);
                if outbound_tx.send(wire).is_err() {
                    break;
                }
            }
        });

        let done = Arc::new(tokio::sync::Notify::new());
        Ok(Session {
            service: svc,
            peer_id: peer,
            outbound: pub_outbound_tx,
            inbound: pub_inbound_rx,
            done,
            cleanup: None,
        })
    }
}

fn translate_out(msg: &SignalMessage, service: &str) -> WireMsg {
    let body = match &msg.body {
        SignalBody { offer: Some(o), .. } => Some(gen::signal_message::Body::Offer(
            gen::SdpOffer { sdp: o.sdp.clone() },
        )),
        SignalBody { answer: Some(a), .. } => Some(gen::signal_message::Body::Answer(
            gen::SdpAnswer { sdp: a.sdp.clone() },
        )),
        SignalBody {
            candidate: Some(c), ..
        } => Some(gen::signal_message::Body::Candidate(gen::IceCandidate {
            candidate: c.candidate.clone(),
            sdp_mid: c.sdp_mid.clone(),
            sdp_mline_index: c.sdp_m_line_index,
        })),
        _ => None,
    };
    WireMsg {
        service: service.to_string(),
        body,
    }
}

fn translate_in(wire: &WireMsg) -> SignalMessage {
    let body = match &wire.body {
        Some(gen::signal_message::Body::Announce(_)) => SignalBody::default(),
        Some(gen::signal_message::Body::Offer(o)) => SignalBody {
            offer: Some(SdpOffer { sdp: o.sdp.clone() }),
            ..Default::default()
        },
        Some(gen::signal_message::Body::Answer(a)) => SignalBody {
            answer: Some(SdpAnswer { sdp: a.sdp.clone() }),
            ..Default::default()
        },
        Some(gen::signal_message::Body::Candidate(c)) => SignalBody {
            candidate: Some(IceCandidate {
                candidate: c.candidate.clone(),
                sdp_mid: c.sdp_mid.clone(),
                sdp_m_line_index: c.sdp_mline_index,
            }),
            ..Default::default()
        },
        _ => SignalBody::default(),
    };
    SignalMessage {
        service: wire.service.clone(),
        body,
    }
}

// Adapter: turn our UnboundedReceiver<WireMsg> into a tonic
// Stream<WireMsg> the generated client expects. tonic 0.12 client
// streaming wants `Stream<Item = T>` (not `Result<T, _>`); error
// paths in the input stream surface as a closed stream.
fn outbound_to_stream(
    mut rx: mpsc::UnboundedReceiver<WireMsg>,
) -> impl futures_core::Stream<Item = WireMsg> + Send {
    async_stream::stream! {
        while let Some(msg) = rx.recv().await {
            yield msg;
        }
    }
}

// Silence unused imports / types if a refactor drops a path.
// (intentionally empty placeholder kept for future use.)
