// @generated
/// SignalMessage is the single envelope on the Exchange stream.
/// Exactly one field of `body` is set per message.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignalMessage {
    /// service is the rendezvous key. Two peers wishing to establish a
    /// DataChannel MUST announce against the same service.
    #[prost(string, tag="1")]
    pub service: ::prost::alloc::string::String,
    #[prost(oneof="signal_message::Body", tags="2, 3, 4, 5, 6, 7")]
    pub body: ::core::option::Option<signal_message::Body>,
}
/// Nested message and enum types in `SignalMessage`.
pub mod signal_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Body {
        #[prost(message, tag="2")]
        Announce(super::AnnounceRequest),
        #[prost(message, tag="3")]
        Offer(super::SdpOffer),
        #[prost(message, tag="4")]
        Answer(super::SdpAnswer),
        #[prost(message, tag="5")]
        Candidate(super::IceCandidate),
        #[prost(message, tag="6")]
        Leave(super::LeaveRequest),
        #[prost(message, tag="7")]
        Ping(super::Ping),
    }
}
/// AnnounceRequest is the first message a peer sends on the Exchange
/// stream. It registers the peer against the requested service.
///
/// Auth is enforced server-side via a Connect interceptor that
/// validates the Authorization header before the stream is admitted.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnounceRequest {
    /// peer_id is the caller-chosen identifier for itself within the
    /// service. The server MAY reject duplicates.
    ///
    /// When the SDK uses peerrpc.Dial/Listen without an explicit
    /// peer_id, it generates a UUIDv7 here.
    #[prost(string, tag="1")]
    pub peer_id: ::prost::alloc::string::String,
    /// peer_pubkey carries an Ed25519 public key when the peer opts
    /// into the strong-identity model. Servers MUST accept this
    /// field but MAY ignore it; full verification ships in a future
    /// release.
    #[prost(bytes="vec", optional, tag="2")]
    pub peer_pubkey: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    #[prost(enumeration="announce_request::Role", tag="3")]
    pub role: i32,
}
/// Nested message and enum types in `AnnounceRequest`.
pub mod announce_request {
    /// Role disambiguates the peer's application-level part.
    ///
    /// ROLE_RELAY / ROLE_BRIDGE values let the server identify
    /// relay-server and grpcbridge-server peers and apply differentiated
    /// policy (e.g. prevent two relays in the same service).
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Role {
        Unspecified = 0,
        Client = 1,
        Server = 2,
        Relay = 3,
        Bridge = 4,
    }
    impl Role {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Role::Unspecified => "ROLE_UNSPECIFIED",
                Role::Client => "ROLE_CLIENT",
                Role::Server => "ROLE_SERVER",
                Role::Relay => "ROLE_RELAY",
                Role::Bridge => "ROLE_BRIDGE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "ROLE_UNSPECIFIED" => Some(Self::Unspecified),
                "ROLE_CLIENT" => Some(Self::Client),
                "ROLE_SERVER" => Some(Self::Server),
                "ROLE_RELAY" => Some(Self::Relay),
                "ROLE_BRIDGE" => Some(Self::Bridge),
                _ => None,
            }
        }
    }
}
/// SdpOffer carries a WebRTC SessionDescription of type "offer".
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SdpOffer {
    #[prost(string, tag="1")]
    pub sdp: ::prost::alloc::string::String,
}
/// SdpAnswer carries a WebRTC SessionDescription of type "answer".
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SdpAnswer {
    #[prost(string, tag="1")]
    pub sdp: ::prost::alloc::string::String,
}
/// IceCandidate carries a single ICE candidate (serialized per the
/// WebRTC RTCIceCandidateInit dictionary).
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IceCandidate {
    /// candidate is the raw candidate string.
    #[prost(string, tag="1")]
    pub candidate: ::prost::alloc::string::String,
    /// sdp_mid is the media stream identification tag.
    #[prost(string, tag="2")]
    pub sdp_mid: ::prost::alloc::string::String,
    /// sdp_mline_index is the zero-based index of the m-line.
    #[prost(uint32, tag="3")]
    pub sdp_mline_index: u32,
}
/// LeaveRequest signals graceful departure from the service.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaveRequest {
    #[prost(string, tag="1")]
    pub reason: ::prost::alloc::string::String,
}
/// Ping is a liveness probe; the server echoes it back as a Ping
/// with the same payload.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ping {
    #[prost(int64, tag="1")]
    pub timestamp_ms: i64,
}
// @@protoc_insertion_point(module)
