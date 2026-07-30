/**
 * WebSocket signal client. Speaks protobuf over a raw WebSocket.
 *
 * Wire framing on the WS: each WebSocket message carries exactly one
 * length-prefixed peerrpc.signaling.SignalMessage protobuf.
 * Length prefix is a 4-byte big-endian uint32, matching the PeerRPC
 * RPC-frame convention.
 */

import type { SignalMessage } from "@peerrpc/peer";
import {
  SignalMessage as WireSignalMessage,
  AnnounceRequest,
  SdpOffer,
  SdpAnswer,
  IceCandidate,
} from "@peerrpc/protocol/gen/peerrpc/signaling/signaling_pb.js";
import type { AnnounceRequest_Role } from "@peerrpc/protocol/gen/peerrpc/signaling/signaling_pb.js";

export interface WebSocketSignalConfig {
  /** WebSocket URL (e.g. "wss://signal.example.com/ws"). */
  url: string;
  /** Rendezvous key. */
  service: string;
  /** Caller-chosen peer id within the service. */
  peerId: string;
  /** Application-level role. Defaults to ROLE_CLIENT. */
  role?: AnnounceRequest_Role;
  /** Optional Ed25519 public key. */
  peerPubkey?: Uint8Array;
}

export class WebSocketSignal {
  private cfg: WebSocketSignalConfig;
  private onMessageCb: ((msg: SignalMessage) => void) | null = null;
  private ws: WebSocket | null = null;

  constructor(cfg: WebSocketSignalConfig) {
    this.cfg = cfg;
  }

  async connect(): Promise<void> {
    const ws = new WebSocket(this.cfg.url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    // Browsers do NOT surface a TLS rejection on the WebSocket error
    // event (it carries no detail for security reasons). The close
    // event fires with code 1006 (abnormal closure) instead. Capture
    // it so connect() can report an actionable message pointing at the
    // self-signed cert, rather than the opaque "ws open failed".
    await new Promise<void>((resolve, reject) => {
      let settled = false;
      ws.addEventListener("open", () => {
        settled = true;
        resolve();
      }, { once: true });
      ws.addEventListener("close", (ev) => {
        if (settled) return;
        if (ev.code === 1006) {
          reject(new Error(
            `signal: WebSocket to ${this.cfg.url} closed before opening (code 1006). ` +
            `If using a self-signed cert, open ${this.cfg.url} in a tab and accept it first.`,
          ));
        } else {
          reject(new Error(`signal: WebSocket closed before opening (code ${ev.code}: ${ev.reason})`));
        }
      }, { once: true });
      ws.addEventListener("error", () => {
        // Defer to the close handler, which carries the code. If error
        // fires without a following close (some browsers), reject here.
        setTimeout(() => {
          if (!settled && ws.readyState === WebSocket.CLOSED) {
            reject(new Error(
              `signal: WebSocket to ${this.cfg.url} failed to open. ` +
              `If using a self-signed cert, open ${this.cfg.url} in a tab and accept it first.`,
            ));
          }
        }, 0);
      }, { once: true });
    });

    ws.addEventListener("message", (ev) => {
      const buf = new Uint8Array(ev.data as ArrayBuffer);
      const msg = decodeLengthPrefixed(buf);
      if (!msg) return;
      const translated = translateIncoming(msg);
      if (translated) this.onMessageCb?.(translated);
    });
    ws.addEventListener("close", () => { /* best-effort */ });

    // Send the announce as the first frame.
    const announce = new WireSignalMessage({
      service: this.cfg.service,
      body: {
        case: "announce",
        value: new AnnounceRequest({
          peerId: this.cfg.peerId,
          role: this.cfg.role ?? 1 /* ROLE_CLIENT */,
          ...(this.cfg.peerPubkey ? { peerPubkey: this.cfg.peerPubkey } : {}),
        }),
      },
    });
    this.ws.send(encodeLengthPrefixed(announce));
  }

  onMessage(cb: (msg: SignalMessage) => void): void {
    this.onMessageCb = cb;
  }

  send(msg: SignalMessage): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error("signal: not connected; call connect() first");
    }
    const wire = translateOutgoing(msg);
    wire.service = this.cfg.service;
    this.ws.send(encodeLengthPrefixed(wire));
  }

  close(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}

function translateOutgoing(msg: SignalMessage): WireSignalMessage {
  const wire = new WireSignalMessage();
  switch (msg.type) {
    case "offer":
      wire.body = { case: "offer", value: new SdpOffer({ sdp: msg.sdp ?? "" }) };
      break;
    case "answer":
      wire.body = { case: "answer", value: new SdpAnswer({ sdp: msg.sdp ?? "" }) };
      break;
    case "candidate":
      wire.body = {
        case: "candidate",
        value: new IceCandidate({
          candidate: msg.candidate ?? "",
          sdpMid: msg.sdpMid ?? "",
          sdpMlineIndex: msg.sdpMLineIndex ?? 0,
        }),
      };
      break;
  }
  return wire;
}

function translateIncoming(wire: WireSignalMessage): SignalMessage | null {
  switch (wire.body.case) {
    case "offer":
      return { type: "offer", sdp: wire.body.value.sdp };
    case "answer":
      return { type: "answer", sdp: wire.body.value.sdp };
    case "candidate":
      return {
        type: "candidate",
        candidate: wire.body.value.candidate,
        sdpMid: wire.body.value.sdpMid,
        sdpMLineIndex: wire.body.value.sdpMlineIndex,
      };
    default:
      return null;
  }
}

// ----- length-prefixed protobuf framing (4-byte BE length) -----
//
// Encode/decode use the generated Message instance methods
// (msg.toBinary() / new T().fromBinary(bytes)) rather than the
// @bufbuild/protobuf top-level toBinary/fromBinary, which are not
// exported in v1.x. The functions are synchronous so callers can pass
// the result straight to ws.send without an extra await.

function encodeLengthPrefixed(msg: WireSignalMessage): ArrayBuffer {
  const body = msg.toBinary();
  const out = new Uint8Array(4 + body.length);
  const dv = new DataView(out.buffer);
  dv.setUint32(0, body.length, false /* big-endian */);
  out.set(body, 4);
  return out.buffer;
}

function decodeLengthPrefixed(buf: Uint8Array): WireSignalMessage | null {
  if (buf.length < 4) return null;
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const len = dv.getUint32(0, false);
  if (buf.length < 4 + len) return null;
  const body = buf.subarray(4, 4 + len);
  try {
    return new WireSignalMessage().fromBinary(body);
  } catch {
    return null;
  }
}
