//! Target URI parsing for the peerrpc facade.
//!
//! Grammar mirrors the Go and TS facades:
//!
//! ```text
//! peerrpc+<scheme>://<authority>/<service>[?<opts>]
//! ```
//!
//! scheme:
//!   local    → in-process signaling (no network)
//!   connect  → tonic over HTTP/2 (the default Remote backend)
//!   ws       → browser WebSocket (not yet wired)
//!   relay    → explicit relay hop (not yet implemented)
//!
//! authority: signal-server host (ignored for local).
//! service:   rendezvous key.
//!
//! query opts (all optional):
//!   ?as=client|server   role hint
//!   ?peer=<id>          peer_id; defaults to an auto-generated UUID
//!   ?token=<jwt>        bearer token (placeholder; tonic interceptor
//!                       wiring ships in a future release)

use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    Local,
    Connect,
    Ws,
    Relay,
}

impl Scheme {
    pub fn as_str(&self) -> &'static str {
        match self {
            Scheme::Local => "local",
            Scheme::Connect => "connect",
            Scheme::Ws => "ws",
            Scheme::Relay => "relay",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleHint {
    Client,
    Server,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub scheme: Scheme,
    /// signal-server authority (host[:port]); empty for local.
    pub signal: String,
    /// Rendezvous key.
    pub service: String,
    pub role: Option<RoleHint>,
    pub peer_id: Option<String>,
    pub token: Option<String>,
}

#[derive(Debug, Error)]
pub enum TargetParseError {
    #[error("target URI must start with {prefix:?}, got {input:?}")]
    MissingPrefix { prefix: &'static str, input: String },
    #[error("target URI missing {separator:?} after scheme")]
    MissingSeparator { separator: &'static str },
    #[error("unknown scheme: {0:?}")]
    UnknownScheme(String),
    #[error("missing service path in {0:?}")]
    MissingService(String),
    #[error("scheme {scheme:?} requires a non-empty authority")]
    EmptyAuthority { scheme: String },
}

const PREFIX: &str = "peerrpc+";
const SEP: &str = "://";

pub fn parse_target(uri: &str) -> Result<Target, TargetParseError> {
    if !uri.starts_with(PREFIX) {
        return Err(TargetParseError::MissingPrefix {
            prefix: PREFIX,
            input: uri.to_string(),
        });
    }
    let rest = &uri[PREFIX.len()..];

    let sep_idx = rest
        .find(SEP)
        .ok_or(TargetParseError::MissingSeparator { separator: SEP })?;
    let scheme_str = &rest[..sep_idx];
    let scheme = match scheme_str {
        "local" => Scheme::Local,
        "connect" => Scheme::Connect,
        "ws" => Scheme::Ws,
        "relay" => Scheme::Relay,
        other => return Err(TargetParseError::UnknownScheme(other.to_string())),
    };

    let after_scheme = &rest[sep_idx + SEP.len()..];

    // authority / path+query split at the first '/'.
    let (authority, path_query) = match after_scheme.find('/') {
        Some(idx) => (&after_scheme[..idx], &after_scheme[idx + 1..]),
        None => (after_scheme, ""),
    };

    // service / query split.
    let (service, raw_query) = match path_query.find('?') {
        Some(idx) => (&path_query[..idx], &path_query[idx + 1..]),
        None => (path_query, ""),
    };

    if service.is_empty() {
        return Err(TargetParseError::MissingService(uri.to_string()));
    }

    let mut t = Target {
        scheme,
        signal: authority.to_string(),
        service: service.to_string(),
        role: None,
        peer_id: None,
        token: None,
    };

    if !raw_query.is_empty() {
        for pair in raw_query.split('&') {
            let eq = match pair.find('=') {
                Some(i) => i,
                None => continue,
            };
            let k = &pair[..eq];
            let v = &pair[eq + 1..];
            let v = percent_decode(v);
            match k {
                "as" => {
                    t.role = match v.as_str() {
                        "client" => Some(RoleHint::Client),
                        "server" => Some(RoleHint::Server),
                        _ => None,
                    };
                }
                "peer" => t.peer_id = Some(v),
                "token" => t.token = Some(v),
                _ => {}
            }
        }
    }

    if t.signal.is_empty() && t.scheme != Scheme::Local {
        return Err(TargetParseError::EmptyAuthority {
            scheme: scheme_str.to_string(),
        });
    }
    Ok(t)
}

fn percent_decode(s: &str) -> String {
    // Minimal percent-decoder: handles %XX byte sequences. Enough
    // for the small alphabet that appears in peerrpc targets; we
    // deliberately don't drag in `percent-encoding` as a dependency
    // for ~30 lines of code.
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = hex_digit(bytes[i + 1]);
            let lo = hex_digit(bytes[i + 2]);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Render a Target back to its canonical URI form.
pub fn format_target(t: &Target) -> String {
    let mut s = String::new();
    s.push_str("peerrpc+");
    s.push_str(t.scheme.as_str());
    s.push_str("://");
    s.push_str(&t.signal);
    s.push('/');
    s.push_str(&t.service);
    let mut first = true;
    let mut add = |key: &str, val: &str, first: &mut bool| {
        if *first {
            s.push('?');
            *first = false;
        } else {
            s.push('&');
        }
        s.push_str(key);
        s.push('=');
        s.push_str(val);
    };
    if let Some(r) = t.role {
        add(
            "as",
            match r {
                RoleHint::Client => "client",
                RoleHint::Server => "server",
            },
            &mut first,
        );
    }
    if let Some(p) = &t.peer_id {
        add("peer", p, &mut first);
    }
    if let Some(tok) = &t.token {
        add("token", tok, &mut first);
    }
    s
}
