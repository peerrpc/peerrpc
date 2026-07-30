import { describe, expect, it } from "vitest";
import { injectMaxMessageSize } from "./sdpMunge.js";

const SAMPLE_SDP = [
  "v=0",
  "o=- 1 1 IN IP4 127.0.0.1",
  "s=-",
  "t=0 0",
  "m=application 9 UDP/DTLS/SCTP webrtc-datachannel",
  "c=IN IP4 0.0.0.0",
  "a=ice-ufrag:foo",
  "a=ice-pwd:bar",
  "a=fingerprint:sha-256 00:00",
  "a=setup:actpass",
  "a=mid:0",
  "a=sctp-port:5000",
  "a=candidate:1 1 udp 2122260223 127.0.0.1 12345 typ host",
  "",
].join("\r\n");

describe("injectMaxMessageSize", () => {
  it("inserts a=max-message-size immediately after the m=application line", () => {
    const out = injectMaxMessageSize(SAMPLE_SDP);
    // The new attribute must sit on the line directly after m=application.
    const mLineIdx = out.indexOf("m=application");
    const mLineEnd = out.indexOf("\r\n", mLineIdx);
    expect(out.slice(mLineEnd + 2, mLineEnd + 2 + "a=max-message-size:262144".length))
      .toBe("a=max-message-size:262144");
    // The new attribute must be the first a= line in the m=application
    // section (so it precedes the existing a= attributes).
    const sectionStart = mLineEnd + 2;
    const nextAttr = out.indexOf("\r\n", sectionStart);
    expect(out.slice(sectionStart, nextAttr)).toBe("a=max-message-size:262144");
  });

  it("is idempotent (leaves SDP untouched if already present)", () => {
    const once = injectMaxMessageSize(SAMPLE_SDP);
    const twice = injectMaxMessageSize(once);
    expect(twice).toBe(once);
  });

  it("uses LF line terminator when the SDP has no CRLF", () => {
    const lf = SAMPLE_SDP.replace(/\r\n/g, "\n");
    const out = injectMaxMessageSize(lf);
    expect(out).toContain("a=max-message-size:262144\n");
    expect(out).not.toContain("262144\r\n");
  });

  it("returns SDP unchanged when there is no m=application section", () => {
    const noApp = "v=0\r\nm=audio 9 UDP/TLS/RTP/SAVPF 111\r\n";
    expect(injectMaxMessageSize(noApp)).toBe(noApp);
  });

  it("accepts a custom byte value", () => {
    const out = injectMaxMessageSize(SAMPLE_SDP, 65536);
    expect(out).toContain("a=max-message-size:65536\r\n");
    expect(out).not.toContain("262144");
  });
});
