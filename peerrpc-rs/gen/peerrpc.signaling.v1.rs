// @generated
/// SignalMessage is the single envelope on the Exchange stream. Exactly
/// one field of `body` is set per message.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SignalMessage {
    /// room_id is the signaling room identifier. Two peers wishing to
    /// establish a DataChannel MUST join the same room_id.
    #[prost(string, tag="1")]
    pub room_id: ::prost::alloc::string::String,
    #[prost(oneof="signal_message::Body", tags="2, 3, 4, 5, 6, 7")]
    pub body: ::core::option::Option<signal_message::Body>,
}
/// Nested message and enum types in `SignalMessage`.
pub mod signal_message {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Body {
        #[prost(message, tag="2")]
        Join(super::JoinRequest),
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
/// JoinRequest joins (or creates) a signaling room.
/// Auth is enforced server-side via a Connect interceptor that validates
/// the Authorization header before the stream is admitted.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct JoinRequest {
    /// peer_id is the caller-chosen identifier for itself within the room.
    /// The server MAY reject duplicates.
    #[prost(string, tag="1")]
    pub peer_id: ::prost::alloc::string::String,
    #[prost(enumeration="join_request::Role", tag="2")]
    pub role: i32,
}
/// Nested message and enum types in `JoinRequest`.
pub mod join_request {
    /// role disambiguates which peer initiates the WebRTC offer.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Role {
        Unspecified = 0,
        Offerer = 1,
        Answerer = 2,
    }
    impl Role {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Role::Unspecified => "ROLE_UNSPECIFIED",
                Role::Offerer => "ROLE_OFFERER",
                Role::Answerer => "ROLE_ANSWERER",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "ROLE_UNSPECIFIED" => Some(Self::Unspecified),
                "ROLE_OFFERER" => Some(Self::Offerer),
                "ROLE_ANSWERER" => Some(Self::Answerer),
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
/// LeaveRequest signals graceful departure from the room.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LeaveRequest {
    #[prost(string, tag="1")]
    pub reason: ::prost::alloc::string::String,
}
/// Ping is a liveness probe; the server echoes it back as a Ping with
/// the same payload.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Ping {
    #[prost(int64, tag="1")]
    pub timestamp_ms: i64,
}
// @@protoc_insertion_point(module)
