//! WebSocket backend: a signaling client that speaks the signaling
//! wire format (service / AnnounceRequest) to a remote signal-server
//! over a raw WebSocket.
//!
//! Mirrors Go's `signal.WS` and TS's `WebSocketSignal`. The server's
//! `/ws` endpoint expects each frame to be a 4-byte big-endian length
//! prefix + a marshaled `peerrpc.signaling.SignalMessage` protobuf, with
//! the first frame an `AnnounceRequest` carrying (service, peer_id, role).

use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use prost::Message as _;
use tokio::sync::{mpsc, oneshot, Notify};
use tokio_tungstenite::tungstenite::Message;

use peerrpc_protocol::gen::signaling::{
    announce_request, signal_message, AnnounceRequest, SignalMessage as WireMsg,
};

use crate::{IceCandidate, SdpAnswer, SdpOffer, Session, SignalBody, SignalError, SignalMessage};

/// `WS` is a signaling backend that connects to a remote signal-server
/// over a WebSocket. Construct with a base URL (any of http(s):// or
/// ws(s):// forms; a bare host is also accepted) and call `exchange`
/// per peer.
///
/// The URL is normalized: http(s):// is rewritten to ws(s):// and a
/// "/ws" path is appended when absent, matching the server's endpoint.
pub struct WS {
    base_url: String,
    token: Option<String>,
}

impl WS {
    /// Construct a WS backend pointing at the given signal-server. The
    /// URL is normalized on each `exchange`.
    pub fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            token: None,
        }
    }

    /// Supply a bearer token forwarded to the signal-server (as a
    /// `?token=` query param on the WebSocket URL).
    pub fn with_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    /// Open a WebSocket to the signal-server, send the AnnounceRequest
    /// as the first frame, and return a Session that ferries
    /// SignalMessages both ways. Each call dials its own WebSocket.
    pub async fn exchange(&self, service: &str, peer_id: &str) -> Result<Session, SignalError> {
        if service.is_empty() {
            return Err(SignalError::Other("empty service".into()));
        }
        if peer_id.is_empty() {
            return Err(SignalError::Other("empty peer id".into()));
        }

        let url = ws_url(&self.base_url, self.token.as_deref())?;

        // Dial the WebSocket; surface the result via a oneshot so a
        // connect failure is not silently swallowed.
        let (connect_tx, connect_rx) = oneshot::channel::<Result<(), String>>();
        let (mut ws_stream, _resp) = match tokio_tungstenite::connect_async(url.clone()).await {
            Ok(pair) => {
                let _ = connect_tx.send(Ok(()));
                pair
            }
            Err(e) => {
                return Err(SignalError::Other(format!("ws dial {url:?}: {e}")));
            }
        };

        // First frame MUST be the AnnounceRequest.
        let announce = WireMsg {
            service: service.to_string(),
            body: Some(signal_message::Body::Announce(AnnounceRequest {
                peer_id: peer_id.to_string(),
                peer_pubkey: None,
                role: announce_request::Role::Server as i32,
            })),
        };
        if let Err(e) = ws_stream.send(Message::Binary(write_frame(&announce))).await {
            return Err(SignalError::Other(format!("ws send announce: {e}")));
        }

        let svc = service.to_string();
        let peer = peer_id.to_string();
        let done = Arc::new(Notify::new());
        let done_for_pump = done.clone();

        // Public channels bridged to the wire.
        let (pub_outbound_tx, mut pub_outbound_rx) = mpsc::unbounded_channel::<SignalMessage>();
        let (pub_inbound_tx, pub_inbound_rx) = mpsc::unbounded_channel::<SignalMessage>();

        // One pump task owns the WebSocket and drives both directions.
        // Outbound: public SignalMessage -> wire frame -> ws.send.
        // Inbound:  ws.next -> wire frame -> public SignalMessage.
        tokio::spawn(async move {
            // connect_tx is already consumed above; drop the stale
            // receiver pattern by moving nothing extra in.
            loop {
                tokio::select! {
                    biased;
                    _ = done_for_pump.notified() => break,
                    msg = pub_outbound_rx.recv() => {
                        let Some(msg) = msg else { break };
                        let wire = translate_out(&msg);
                        let frame = write_frame(&wire);
                        if ws_stream.send(Message::Binary(frame)).await.is_err() {
                            break;
                        }
                    }
                    frame = ws_stream.next() => {
                        match frame {
                            Some(Ok(Message::Binary(data))) => {
                                if let Some(wire) = read_frame(&data) {
                                    let translated = translate_in(&wire);
                                    if pub_inbound_tx.send(translated).is_err() {
                                        break;
                                    }
                                }
                            }
                            Some(Ok(_)) => {
                                // Ignore text/ping/close-control frames.
                            }
                            Some(Err(e)) => {
                                tracing::warn!("signal: ws read error: {e}");
                                break;
                            }
                            None => break, // server closed
                        }
                    }
                }
            }
            let _ = ws_stream.close(None).await;
        });

        // Surface any deferred connect error (currently always Ok since
        // we awaited connect_async above, but kept for symmetry with
        // Remote and future lazy-connect variants).
        match connect_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(msg)) => return Err(SignalError::Other(msg)),
            Err(_) => return Err(SignalError::Other("signal: connect task dropped".into())),
        }

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

// ── URL normalization ──────────────────────────────────────────

/// Normalize the configured base URL into a ws:// or wss:// URL ending
/// with the "/ws" path, appending the token query param when set.
fn ws_url(base: &str, token: Option<&str>) -> Result<String, SignalError> {
    let raw = base.trim();
    if raw.is_empty() {
        return Err(SignalError::Other("empty ws url".into()));
    }
    // Rewrite scheme: http(s) -> ws(s). Bare host defaults to ws://.
    let rewritten = if let Some(rest) = raw.strip_prefix("http://") {
        format!("ws://{rest}")
    } else if let Some(rest) = raw.strip_prefix("https://") {
        format!("wss://{rest}")
    } else if raw.starts_with("ws://") || raw.starts_with("wss://") {
        raw.to_string()
    } else {
        format!("ws://{raw}")
    };
    let mut url: http::Uri = rewritten
        .parse()
        .map_err(|e| SignalError::Other(format!("parse ws url {raw:?}: {e}")))?;

    // Append "/ws" path when none (or only "/") is present.
    let path = url.path();
    if path.is_empty() || path == "/" {
        // Rebuild with /ws path (http::Uri is partially mutable via path().
        let mut parts = url.into_parts();
        parts.path_and_query = Some(format!(
            "/ws{}",
            token
                .map(|t| format!("?token={}", urlenc(t)))
                .unwrap_or_default()
        )
        .parse()
        .map_err(|e| SignalError::Other(format!("rebuild ws url: {e}")))?);
        url = http::Uri::from_parts(parts)
            .map_err(|e| SignalError::Other(format!("rebuild ws url: {e}")))?;
    } else if let Some(t) = token {
        // Path already present; append token as query.
        let existing = url.path_and_query().map(|pq| pq.as_str()).unwrap_or(path);
        let sep = if existing.contains('?') { '&' } else { '?' };
        let with_token = format!("{existing}{sep}token={}", urlenc(t));
        let mut parts = url.into_parts();
        parts.path_and_query = Some(
            with_token
                .parse()
                .map_err(|e| SignalError::Other(format!("rebuild ws url: {e}")))?,
        );
        url = http::Uri::from_parts(parts)
            .map_err(|e| SignalError::Other(format!("rebuild ws url: {e}")))?;
    }
    Ok(url.to_string())
}

fn urlenc(s: &str) -> String {
    // Minimal percent-encoding for token query values.
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'-' | b'.' | b'_' | b'~' | b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

// ── Wire framing (4-byte BE length prefix + protobuf) ──────────

/// Encode a SignalMessage into a 4-byte BE length prefix + protobuf.
fn write_frame(msg: &WireMsg) -> Vec<u8> {
    let payload = msg.encode_to_vec();
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Decode one 4-byte-length-prefixed SignalMessage from a WebSocket
/// Binary frame. Returns None if the frame is malformed.
fn read_frame(data: &[u8]) -> Option<WireMsg> {
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if data.len() - 4 < len {
        return None;
    }
    WireMsg::decode(&data[4..4 + len]).ok()
}

// ── Wire <-> public type translation ───────────────────────────

fn translate_out(msg: &SignalMessage) -> WireMsg {
    let body = match &msg.body {
        SignalBody {
            offer: Some(o), ..
        } => Some(signal_message::Body::Offer(peerrpc_protocol::gen::signaling::SdpOffer {
            sdp: o.sdp.clone(),
        })),
        SignalBody {
            answer: Some(a), ..
        } => Some(signal_message::Body::Answer(peerrpc_protocol::gen::signaling::SdpAnswer {
            sdp: a.sdp.clone(),
        })),
        SignalBody {
            candidate: Some(c), ..
        } => Some(signal_message::Body::Candidate(peerrpc_protocol::gen::signaling::IceCandidate {
            candidate: c.candidate.clone(),
            sdp_mid: c.sdp_mid.clone(),
            sdp_mline_index: c.sdp_m_line_index,
        })),
        _ => None,
    };
    WireMsg {
        service: msg.service.clone(),
        body,
    }
}

fn translate_in(wire: &WireMsg) -> SignalMessage {
    let body = match &wire.body {
        Some(signal_message::Body::Announce(_)) => SignalBody::default(),
        Some(signal_message::Body::Offer(o)) => SignalBody {
            offer: Some(SdpOffer { sdp: o.sdp.clone() }),
            ..Default::default()
        },
        Some(signal_message::Body::Answer(a)) => SignalBody {
            answer: Some(SdpAnswer { sdp: a.sdp.clone() }),
            ..Default::default()
        },
        Some(signal_message::Body::Candidate(c)) => SignalBody {
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
