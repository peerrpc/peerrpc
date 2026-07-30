# SCTP max-message-size and Adaptive Chunking

This document explains how PeerRPC negotiates the DataChannel's SCTP
`max-message-size` and how it adapts chunk sizes per SDK to maximize
bandwidth while staying within the limits each runtime imposes.

## Background: SCTP max-message-size

WebRTC DataChannels carry data over SCTP. Per RFC 8831, the SCTP
`max-message-size` is a per-association limit: any `DataChannel.send()`
larger than the negotiated value is rejected by the SCTP layer and
the stream is torn down. The negotiated value is the **minimum** of:

- `a=max-message-size` in the **remote** SDP (parsed by the local
  SCTP transport)
- the local `SctpMaxMessageSize::can_send` setting (configurable
  via the WebRTC API)

A vanilla browser does **not** emit `a=max-message-size` in its
generated SDP, and a vanilla `RTCPeerConnection` has no public API to
override the value. The result is a 64 KiB ceiling (the default from
RFC 8841 §6.1-4 when the SDP omits the attribute).

## Per-runtime behavior

| Runtime | `a=max-message-size` advertised | Honors remote `a=max-message-size`? | Effective outbound cap |
|---|---|---|---|
| **pion (Go)** | 262144 (default) | Yes | 262144 (256 KiB) |
| **webrtc-rs (Rust) 0.17** | Does **not** advertise by default | Yes, but answerer **cannot raise** above the offerer's value | **64 KiB** unless can_send is raised |
| **Chromium / Firefox** | Does **not** advertise by default | Partially — Chrome raises its outbound cap to the value in the **answer** SDP, but only if the answerer's munged value arrives in time | **64 KiB** unless the offer carries `a=max-message-size:262144` |

Key takeaways:

- **pion (Go) negotiates 256 KiB by default** — both directions work.
- **webrtc-rs (Rust) 0.17** honors the offer's `a=max-message-size`
  for the *receive* limit but its `sctp_max_message_size_can_send`
  default (65536) caps the *send* limit at 64 KiB.
- **Chromium's SCTP layer** honors `a=max-message-size` from the
  **answer** SDP for its outbound cap, but only when the value is
  larger than its own default. In practice, the cleanest fix is for
  the **browser (TS) peer** to inject `a=max-message-size:262144`
  into its offer SDP so the negotiation reaches 256 KiB regardless
  of the answerer's behavior.

## PeerRPC's adaptive negotiation

The peerrpc SDKs use three complementary mechanisms to maximize the
negotiated value on every pairing.

### 1. TS (browser) injects `a=max-message-size:262144` into its offer and answer SDPs

`peerrpc-ts/packages/peerrpc-peer/src/sdpMunge.ts` exports
`injectMaxMessageSize(sdp, bytes)`. The `Peer` class calls it on both
`createOffer()` and `acceptOffer()`. The helper inserts the
attribute right after the `m=application` line, preserving the
SDP's line endings, and is idempotent (leaves the SDP alone if the
attribute is already present).

This works because modern Chromium raises its outbound cap to match
the larger of (its own default, the value it sees in the remote
SDP).

### 2. Rust webrtc-rs sets `sctp_max_message_size_can_send = Unbounded`

`peerrpc-rs/peer/src/lib.rs` builds the webrtc-rs API with a
`SettingEngine` that overrides the default 64 KiB ceiling:

```rust
fn build_api() -> webrtc::api::API {
  let mut engine = SettingEngine::default();
  engine.set_sctp_max_message_size_can_send(SctpMaxMessageSize::Unbounded);
  webrtc::api::APIBuilder::new()
    .with_setting_engine(engine)
    .build()
}
```

`SctpMaxMessageSize::Unbounded.as_u32() == 0`. webrtc-rs's
`calc_message_size(remote, can_send)` returns `remote` when
`can_send == 0` (the "can_send == 0" branch), so the answerer's
send limit becomes the remote value — i.e. whatever the peer's SDP
advertised. This bypasses webrtc-rs's hardcoded 64 KiB ceiling.

### 3. pion (Go) needs no work

pion's `SctpMaxMessageSize` default is 262144 and it emits
`a=max-message-size:262144` in its SDPs. `peerrpc-go/transport/channel.go`
uses 256 KiB chunks (`maxFrameBytes = 256 * 1024`) without further
configuration.

## Asymmetric chunk sizes

Even with the SDP negotiation reaching 256 KiB end-to-end, the
**browser's outbound SCTP layer still caps messages at 65535 bytes
on the wire** in current Chrome / Firefox releases, regardless of
the negotiated value. This is a browser implementation detail, not
an SDP issue: the SDP says "we can send 256 KiB" but the browser's
SCTP implementation says "no, I won't actually queue a frame that
big" and tears the stream down on the first oversized send.

To work around this, PeerRPC ships **asymmetric chunk sizes** that
each SDK can sustain within its own runtime:

| SDK | `CHUNK_SIZE` | Wire cap it achieves | Why |
|---|---|---|---|
| `peerrpc-go` | 255 KiB | 256 KiB (pion default) | pion negotiates 256 KiB in both directions |
| `peerrpc-rs` | 255 KiB | 256 KiB (server side) | webrtc-rs accepts 256 KiB frames; no outbound 64 KiB cap when the server is the answerer |
| `peerrpc-ts` (client) | **60 KiB** | 64 KiB (Chromium cap) | Browser's SCTP layer caps outbound at 64 KiB; 60 KiB gives 4 KiB headroom for the length prefix + envelope |

The wire is **fully compatible**: `Chunk` frames carry `total_size`,
`offset`, and `data`, and each SDK reassembles using its own
`CHUNK_SIZE`. A 1 MiB request from a browser becomes ~17 60 KiB
chunks on the wire; the server reassembles them into the 1 MiB
payload before invoking the handler.

## Backpressure: post-send drain is required

`RTCDataChannel.send()` returns `void` immediately; the bytes are
queued in the browser's SCTP send buffer. The browser exposes
`bufferedAmount` (current queue size) and a `bufferedamountlow`
event that fires when the queue drops below
`bufferedAmountLowThreshold`.

Without **post-send** backpressure, a burst of `send()` calls stacks
frames in the queue past the SCTP stream buffer (~1 MiB internal in
webrtc-rs 0.17), the SCTP layer resets the stream, and the channel
dies with `transport closed`. This is the failure mode that
originally blocked `Large Echo` over browser↔Rust even after the
negotiation reached 256 KiB.

Both SDKs now wait for the queue to drain **after** every send:

- **`peerrpc-ts/packages/peerrpc-transport/src/index.ts`** —
  `Channel.send()` calls `awaitBufferLow()` both **before** the send
  (when `bufferedAmount >= highWatermark`) and **after** the send.
  The `highWatermark` default is **60 KiB** (matches the client
  `CHUNK_SIZE`) so backpressure triggers after each chunk.
- **`peerrpc-rs/peer/src/lib.rs`** — `Peer::send_frame` mirrors the
  same pre- and post-send `await_buffer_low(&dc)` calls around
  `dc.send()`. The threshold is `BUFFERED_AMOUNT_HIGH` (1 MiB, the
  protocol default) — together they ensure no two consecutive
  sends can stack > 1 MiB in the server's outbound SCTP buffer.

The `awaitBufferLow` in the TS transport was also simplified to
**never fall through silently**: a 5s timeout that silently
`resolve()`-ed (the old behavior) is gone. The function now only
resolves when the `bufferedamountlow` event fires; `close` and
`error` events `reject()`.

## `on_buffered_amount_low` registration

In webrtc-rs 0.17, `RTCDataChannel::on_buffered_amount_low` is
**synchronous** (`pub fn`), but
`set_buffered_amount_low_threshold` is **async** (`pub async fn`).
`peerrpc-rs/peer/src/lib.rs::setup_backpressure` registers them
accordingly:

- `on_buffered_amount_low` is registered inline so the callback is
  armed before any `dc.send()` can fire the event.
- `set_buffered_amount_low_threshold` is called from a
  `tokio::spawn` because it's async (and the registration context —
  `on_data_channel` — is sync).

An earlier version wrapped both in `tokio::spawn`, which meant the
low-watermark callback was registered **after** the first burst of
sends, and the SCTP stream reset before the callback could catch
the drain event. The current code splits them: sync callback first,
async threshold set in a task.

## Verified end-to-end

| Scenario | Result |
|---|---|
| 1 MiB LargeEcho over browser↔Rust | integrity PASSED, ~1.4 MiB/s, chunked into ~18 frames (17 × 60 KiB client chunks reassembled on the server + Begin/Data/End) |
| 1 MiB LargeEchoStream (bidi) over browser↔Rust | Full echo stream, no close |
| 1 MiB LargeEcho over Rust↔Rust (bridge test) | integrity PASSED, 4 × 255 KiB frames (no 64 KiB cap in the in-process bridge) |
| pion↔pion, pion↔webrtc-rs | 256 KiB chunks, no cap issues (pion honors the negotiated value in both directions) |

## Tuning for the future

If Chromium's SCTP cap is ever raised (it's been a long-standing
limitation; Chrome's `a=max-message-size` outbound enforcement has
been the subject of WebRTC working-group discussions for years), the
fix is to bump the TS `CHUNK_SIZE` constant in
`peerrpc-ts/packages/peerrpc-protocol/src/index.ts` and the TS
transport `highWatermark` in
`peerrpc-ts/packages/peerrpc-transport/src/index.ts` to the new
limit, in lockstep. The wire is unchanged; only the per-direction
chunk size constants need to move.
