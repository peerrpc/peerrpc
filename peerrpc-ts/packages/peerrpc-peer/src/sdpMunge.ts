/**
 * SDP munging for the WebRTC peer layer.
 *
 * The browser's RTCPeerConnection does not emit `a=max-message-size`
 * in its generated offer (Chrome/Firefox omit it; per RFC 8841 the
 * default is 64 KiB). webrtc-rs's default fallback is also 64 KiB.
 * Without intervention the SCTP layer on both sides caps message
 * sends at 64 KiB and rejects any larger frame, tearing down the
 * DataChannel.
 *
 * Inject `a=max-message-size:N` into the application media section
 * of our local SDPs so the SCTP negotiation reaches the desired
 * ceiling. This mirrors the Rust peer's `munge_max_message_size`.
 *
 * The injected attribute is a session-level SDP hint, not part of
 * the offer/answer protocol, so the remote (which speaks plain
 * WebRTC) parses it like any other session attribute.
 */

const ADVERTISED_MAX_MESSAGE_SIZE = 262144; // 256 KiB

/**
 * Inject `a=max-message-size:N` into the application media section.
 *
 * If the attribute is already present it is left untouched. Only the
 * `m=application` section is affected. The original SDP's line
 * endings are preserved by inserting the attribute as a raw substring
 * right after the `m=application` line.
 */
export function injectMaxMessageSize(
  sdp: string,
  bytes: number = ADVERTISED_MAX_MESSAGE_SIZE,
): string {
  if (sdp.includes("a=max-message-size")) {
    return sdp; // already advertised (e.g. round-tripped)
  }
  // Detect the line terminator the browser used so we match it.
  const nl = sdp.includes("\r\n") ? "\r\n" : "\n";
  const attr = `a=max-message-size:${bytes}${nl}`;

  // Find the start of the m=application line and inject the attribute
  // right after that line (beginning of its media section).
  const idx = sdp.indexOf("m=application");
  if (idx === -1) {
    // No application section (unexpected for a DataChannel SDP);
    // return unchanged rather than risk corrupting the SDP.
    return sdp;
  }
  const lineEnd = sdp.indexOf(nl, idx);
  if (lineEnd === -1) {
    return sdp;
  }
  const insertAt = lineEnd + nl.length;
  return sdp.slice(0, insertAt) + attr + sdp.slice(insertAt);
}
