// @generated
/// Routing carries the per-RPC multiplexing sequence number.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Routing {
    /// sequence is the stream id. Odd numbers are client-initiated,
    /// even numbers are server-initiated (for server->client push, future).
    /// For v1 only client-initiated streams are used.
    #[prost(int32, tag="1")]
    pub sequence: i32,
}
/// Strings wraps a repeated string used as metadata values.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Strings {
    #[prost(string, repeated, tag="1")]
    pub values: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
/// Metadata is the multi-valued map used for both gRPC-style headers
/// and trailers. All keys MUST be lower-case ASCII (HTTP/2 convention).
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Metadata {
    #[prost(map="string, message", tag="1")]
    pub md: ::std::collections::HashMap<::prost::alloc::string::String, Strings>,
}
// ════════════════════════════════════════════════════════════
// Client -> server
// ════════════════════════════════════════════════════════════

/// Call opens a new RPC on the given sequence.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Call {
    /// method is the fully-qualified gRPC method path:
    ///    "/{Package}.{Service}/{Method}"
    /// Example: "/echo.Echo/Echo"
    #[prost(string, tag="1")]
    pub method: ::prost::alloc::string::String,
    /// metadata carries request headers (client -> server).
    #[prost(message, optional, tag="2")]
    pub metadata: ::core::option::Option<Metadata>,
    /// deadline_ms is the RPC timeout. 0 = no timeout.
    /// The server SHOULD cancel the handler when this deadline expires.
    #[prost(int32, tag="3")]
    pub deadline_ms: i32,
    /// inline_data is the Unary optimization: when the request payload is
    /// small (<=16KB) it can be embedded directly in the Call frame so the
    /// client does not need a separate Data frame.
    #[prost(bytes="vec", optional, tag="4")]
    pub inline_data: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    /// protocol_version MUST be set to PROTOCOL_VERSION_1 by clients.
    /// Servers MUST reject frames with an unknown version with
    /// End{status:FAILED_PRECONDITION}.
    #[prost(int32, tag="5")]
    pub protocol_version: i32,
}
/// Data carries request/response payload bytes for streaming RPCs,
/// or for large Unary payloads that did not fit inline.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Data {
    #[prost(oneof="data::Content", tags="1, 2")]
    pub content: ::core::option::Option<data::Content>,
}
/// Nested message and enum types in `Data`.
pub mod data {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Content {
        /// message is a single un-fragmented payload (<=256KB).
        #[prost(bytes, tag="1")]
        Message(::prost::alloc::vec::Vec<u8>),
        /// chunk is a fragment of a larger logical message (>256KB).
        /// The transport layer transparently re-assembles chunks; the RPC
        /// layer only sees fully reconstructed messages.
        #[prost(message, tag="2")]
        Chunk(super::Chunk),
    }
}
/// Chunk is one fragment of a large logical message.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Chunk {
    /// total_size is the full size of the logical message in bytes.
    /// Receivers use this to allocate a buffer and to detect loss.
    #[prost(int32, tag="1")]
    pub total_size: i32,
    /// offset is the byte offset of this chunk within the logical message.
    #[prost(int32, tag="2")]
    pub offset: i32,
    /// data is the chunk payload (<=256KB per chunk).
    #[prost(bytes="vec", tag="3")]
    pub data: ::prost::alloc::vec::Vec<u8>,
}
/// End terminates a direction of a stream.
///
/// Truth table (see PLAN.md §3.2):
///
///    direction | close_send | status     | meaning
///    ----------|------------|------------|-------------------------------
///    C->S      | true       | unset      | client CloseSend (half-close)
///    C->S      | false      | CANCELLED  | client cancels the RPC
///    S->C      | false      | OK         | server normal completion
///    S->C      | false      | non-OK     | server error
///
/// proto3 semantics: bool defaults to false, message fields are nullable.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct End {
    /// close_send signals half-close when going C->S without status.
    #[prost(bool, tag="1")]
    pub close_send: bool,
    /// status, when set, marks the final state of the RPC (OK or error).
    /// Use google.rpc.Status to align 1:1 with grpc-go / connect-go.
    #[prost(message, optional, tag="2")]
    pub status: ::core::option::Option<super::google::rpc::Status>,
    /// trailer carries response trailers when going S->C.
    #[prost(message, optional, tag="3")]
    pub trailer: ::core::option::Option<Metadata>,
}
/// Frame is the client -> server envelope.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Frame {
    #[prost(message, optional, tag="1")]
    pub routing: ::core::option::Option<Routing>,
    #[prost(oneof="frame::Type", tags="2, 3, 4")]
    pub r#type: ::core::option::Option<frame::Type>,
}
/// Nested message and enum types in `Frame`.
pub mod frame {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        #[prost(message, tag="2")]
        Call(super::Call),
        #[prost(message, tag="3")]
        Data(super::Data),
        #[prost(message, tag="4")]
        End(super::End),
    }
}
// ════════════════════════════════════════════════════════════
// Server -> client
// ════════════════════════════════════════════════════════════

/// Begin is the first server -> client frame. It carries response headers
/// and optionally inlines the first response payload.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Begin {
    /// header carries response headers (server -> client), set via
    /// ServerStream.SetHeader before the first response message.
    #[prost(message, optional, tag="1")]
    pub header: ::core::option::Option<Metadata>,
    /// inline_data is the Unary optimization: the first (or only) response
    /// payload may be embedded directly in Begin when small (<=16KB).
    #[prost(bytes="vec", optional, tag="2")]
    pub inline_data: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
}
/// ResponseFrame is the server -> client envelope.
#[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResponseFrame {
    #[prost(message, optional, tag="1")]
    pub routing: ::core::option::Option<Routing>,
    #[prost(oneof="response_frame::Type", tags="2, 3, 4")]
    pub r#type: ::core::option::Option<response_frame::Type>,
}
/// Nested message and enum types in `ResponseFrame`.
pub mod response_frame {
    #[allow(clippy::derive_partial_eq_without_eq)]
#[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Type {
        #[prost(message, tag="2")]
        Begin(super::Begin),
        #[prost(message, tag="3")]
        Data(super::Data),
        #[prost(message, tag="4")]
        End(super::End),
    }
}
// PeerRPC wire protocol.
//
// Two top-level envelope frames are used:
//    * Frame          - client  -> server
//    * ResponseFrame  - server  -> client
//
// Both envelopes multiplex many logical RPC streams over a single WebRTC
// DataChannel via Routing.sequence. The DataChannel MUST be created with
// ordered=true and reliable delivery (maxRetransmits = infinity).
//
// Protocol version (carried by Call.protocol_version) is currently 1.
// The wire format itself is versioned via the DataChannel label, e.g.
// "peerrpc-v1".

/// ProtocolVersion is the value carried in Call.protocol_version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum ProtocolVersion {
    Unspecified = 0,
    ProtocolVersion1 = 1,
}
impl ProtocolVersion {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            ProtocolVersion::Unspecified => "PROTOCOL_VERSION_UNSPECIFIED",
            ProtocolVersion::ProtocolVersion1 => "PROTOCOL_VERSION_1",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "PROTOCOL_VERSION_UNSPECIFIED" => Some(Self::Unspecified),
            "PROTOCOL_VERSION_1" => Some(Self::ProtocolVersion1),
            _ => None,
        }
    }
}
// @@protoc_insertion_point(module)
