//! PeerRPC facade: collapse signal + peer + rpc into dial / listen.
//!
//! Mirrors the Go `peerrpc` package and TS `@peerrpc/peerrpc`. Three
//! entry styles all funnel into one `dial_target` core:
//!
//! ```no_run
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! // URL form
//! let conn = peerrpc::dial("peerrpc+local:///echo.Echo").await?;
//!
//! // Target form
//! use peerrpc::{dial_target, Target, Scheme};
//! let conn = dial_target(Target {
//!     scheme: Scheme::Local,
//!     signal: String::new(),
//!     service: "echo.Echo".into(),
//!     role: None,
//!     peer_id: None,
//!     token: None,
//! }).await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::Duration;

use thiserror::Error;
use uuid::Uuid;

use peerrpc_peer::{Peer, PeerConfig, PeerError};
use peerrpc_rpc::{Client, RpcError};
use peerrpc_signal::{Local, Remote, Session, SignalError};

pub mod target;
pub use target::{format_target, parse_target, RoleHint, Scheme, Target, TargetParseError};

const DEFAULT_NEGOTIATION_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Error)]
pub enum FacadeError {
    #[error(transparent)]
    Parse(#[from] TargetParseError),
    #[error(transparent)]
    Signal(#[from] SignalError),
    #[error(transparent)]
    Peer(#[from] PeerError),
    #[error(transparent)]
    Rpc(#[from] RpcError),
    #[error("unsupported scheme: {0}")]
    UnsupportedScheme(String),
    #[error("scheme {scheme:?} requires a non-empty authority")]
    EmptyAuthority { scheme: String },
    #[error("listener closed")]
    ListenerClosed,
}

/// A client connection: an rpc::Client bound to one Peer.
///
/// The Client owns the Peer (it implements WireTransport). Closing
/// is implicit on drop — there is no explicit `close` because
/// Rust's Drop idiom covers it more cleanly than Go/TS.
pub struct Conn {
    pub client: Arc<Client>,
    pub peer_id: String,
}

/// Open a client connection to `target`. Blocks until the WebRTC
/// DataChannel is open (or an error occurs).
pub async fn dial(target: &str) -> Result<Conn, FacadeError> {
    let t = parse_target(target)?;
    dial_target(t).await
}

/// Typed-Target variant of [`dial`].
pub async fn dial_target(t: Target) -> Result<Conn, FacadeError> {
    let peer_id = t
        .peer_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let session = open_session(&t, &peer_id).await?;

    let cfg = PeerConfig::default();
    let peer = Peer::dial(cfg, session, DEFAULT_NEGOTIATION_TIMEOUT).await?;

    // rpc::Client::new already returns Arc<Client>.
    let client = Client::new(peer);

    Ok(Conn { client, peer_id })
}

// ----- Listener (server side) -----

pub struct Listener {
    target: Target,
}

impl Listener {
    /// Block until a remote Dialer connects; return an accepted
    /// Peer that the caller hands to `rpc::Server::serve`. Each
    /// call uses a fresh peer_id suffix so multiple sequential
    /// Accepts do not collide at the signal-server.
    pub async fn accept(&self) -> Result<Peer, FacadeError> {
        let peer_id = match &self.target.peer_id {
            Some(p) => format!("{}-{}", p, &Uuid::new_v4().to_string()[..8]),
            None => Uuid::new_v4().to_string(),
        };

        let session = open_session(&self.target, &peer_id).await?;
        let cfg = PeerConfig::default();
        let peer = Peer::accept(cfg, session, DEFAULT_NEGOTIATION_TIMEOUT).await?;
        Ok(peer)
    }
}

pub async fn listen(target: &str) -> Result<Listener, FacadeError> {
    let t = parse_target(target)?;
    listen_target(t).await
}

pub async fn listen_target(t: Target) -> Result<Listener, FacadeError> {
    // Validate the scheme is reachable without opening a session.
    match t.scheme {
        Scheme::Local | Scheme::Connect => Ok(Listener { target: t }),
        Scheme::Ws | Scheme::Relay => Err(FacadeError::UnsupportedScheme(t.scheme.as_str().into())),
    }
}

// ----- resolver: Target → Session -----

async fn open_session(t: &Target, peer_id: &str) -> Result<Session, FacadeError> {
    match t.scheme {
        Scheme::Local => {
            // Per-scheme singleton: every Local-scheme dial in the
            // process must share one Local so peers find each other.
            let local = local_singleton().await;
            local
                .exchange(&t.service, peer_id)
                .await
                .map_err(FacadeError::Signal)
        }
        Scheme::Connect => {
            if t.signal.is_empty() {
                return Err(FacadeError::EmptyAuthority {
                    scheme: t.scheme.as_str().into(),
                });
            }
            let remote = Remote::new(&t.signal);
            remote
                .exchange(&t.service, peer_id)
                .await
                .map_err(FacadeError::Signal)
        }
        Scheme::Ws | Scheme::Relay => Err(FacadeError::UnsupportedScheme(t.scheme.as_str().into())),
    }
}

// ----- Local singleton -----

use tokio::sync::OnceCell;

static LOCAL: OnceCell<Arc<Local>> = OnceCell::const_new();

async fn local_singleton() -> Arc<Local> {
    LOCAL
        .get_or_init(|| async { Arc::new(Local::new()) })
        .await
        .clone()
}
