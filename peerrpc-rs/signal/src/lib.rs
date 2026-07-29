use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, Mutex, Notify};

#[derive(Debug, thiserror::Error)]
pub enum SignalError {
    #[error("signal: {0}")]
    Other(String),
    #[error("signal: session closed")]
    Closed,
}

/// Remote backend (tonic over HTTP/2 to a remote signal-server).
///
/// Behind the `remote` feature (default-on) so that callers without
/// a remote signal-server (e.g. in-process tests) can opt out and
/// drop the tonic dependency.
#[cfg(feature = "remote")]
#[cfg_attr(docsrs, doc(cfg(feature = "remote")))]
pub mod remote;

#[cfg(feature = "remote")]
#[doc(inline)]
pub use remote::Remote;

// ─── Wire types ─────────────────────────────────────────────

/// Per-message signaling envelope. The rendezvous key is `service`.
#[derive(Debug, Clone)]
pub struct SignalMessage {
    pub service: String,
    pub body: SignalBody,
}

#[derive(Debug, Clone, Default)]
pub struct SignalBody {
    pub offer: Option<SdpOffer>,
    pub answer: Option<SdpAnswer>,
    pub candidate: Option<IceCandidate>,
}
#[derive(Debug, Clone)]
pub struct SdpOffer {
    pub sdp: String,
}

#[derive(Debug, Clone)]
pub struct SdpAnswer {
    pub sdp: String,
}

#[derive(Debug, Clone)]
pub struct IceCandidate {
    pub candidate: String,
    pub sdp_mid: String,
    pub sdp_m_line_index: u32,
}

// ─── Session ─────────────────────────────────────────────────

pub struct Session {
    service: String,
    peer_id: String,
    outbound: mpsc::UnboundedSender<SignalMessage>,
    inbound: mpsc::UnboundedReceiver<SignalMessage>,
    done: Arc<Notify>,
    cleanup: Option<Box<dyn FnOnce() + Send>>,
}

impl Session {
    /// Rendezvous key.
    pub fn service(&self) -> &str {
        &self.service
    }

    pub fn peer_id(&self) -> &str {
        &self.peer_id
    }

    pub fn send(&self, msg: SignalMessage) -> Result<(), SignalError> {
        self.outbound
            .send(msg)
            .map_err(|_| SignalError::Closed)
    }

    /// Returns a clonable sender so peer code can pump outbound
    /// messages from async callbacks (e.g. on_ice_candidate) without
    /// taking ownership of the Session.
    pub fn outbound_handle(&self) -> mpsc::UnboundedSender<SignalMessage> {
        self.outbound.clone()
    }

    pub async fn recv(&mut self) -> Option<SignalMessage> {
        self.inbound.recv().await
    }

    pub fn close(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
        self.done.notify_waiters();
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.close();
    }
}

// ─── Local backend ───────────────────────────────────────────

pub struct Local {
    rooms: Arc<Mutex<HashMap<String, Room>>>,
}

struct Room {
    peers: HashMap<String, mpsc::UnboundedSender<SignalMessage>>,
}

impl Local {
    pub fn new() -> Self {
        Self { rooms: Arc::new(Mutex::new(HashMap::new())) }
    }

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

        let mut rooms = self.rooms.lock().await;
        let room = rooms
            .entry(service.to_string())
            .or_insert_with(|| Room { peers: HashMap::new() });

        if room.peers.contains_key(peer_id) {
            return Err(SignalError::Other(format!(
                "peer {peer_id:?} already in service {service:?}"
            )));
        }

        let (inbound_tx, inbound_rx) = mpsc::unbounded_channel();
        let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel();

        room.peers.insert(peer_id.to_string(), inbound_tx);

        let rooms = self.rooms.clone();
        let s_id = service.to_string();
        let p_id = peer_id.to_string();
        let done = Arc::new(Notify::new());
        let d = done.clone();
        let s_id_for_task = s_id.clone();
        let p_id_for_task = p_id.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    msg = outbound_rx.recv() => {
                        match msg {
                            Some(msg) => {
                                broadcast(&rooms, &s_id_for_task, &p_id_for_task, msg).await;
                            }
                            None => break,
                        }
                    }
                    _ = d.notified() => break,
                }
            }
            leave(&rooms, &s_id_for_task, &p_id_for_task).await;
        });

        let session = Session {
            service: s_id,
            peer_id: p_id,
            outbound: outbound_tx,
            inbound: inbound_rx,
            done,
            cleanup: None,
        };

        Ok(session)
    }
}

impl Default for Local {
    fn default() -> Self {
        Self::new()
    }
}

async fn broadcast(
    rooms: &Arc<Mutex<HashMap<String, Room>>>,
    service: &str,
    sender: &str,
    msg: SignalMessage,
) {
    let rooms = rooms.lock().await;
    if let Some(room) = rooms.get(service) {
        for (id, tx) in &room.peers {
            if id != sender {
                let _ = tx.send(msg.clone());
            }
        }
    }
}

async fn leave(
    rooms: &Arc<Mutex<HashMap<String, Room>>>,
    service: &str,
    peer_id: &str,
) {
    let mut rooms = rooms.lock().await;
    if let Some(room) = rooms.get_mut(service) {
        room.peers.remove(peer_id);
        if room.peers.is_empty() {
            rooms.remove(service);
        }
    }
}
