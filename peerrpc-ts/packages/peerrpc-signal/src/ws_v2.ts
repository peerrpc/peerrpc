/**
 * v2 WebSocket signal client. Speaks protobuf over a raw WebSocket
 * instead of the JSON ad-hoc format the v1 WebSocketSignal used.
 *
 * Wire framing on the WS: each WebSocket message carries exactly one
 * length-prefixed peerrpc.signaling.v2.SignalMessage protobuf.
 * Length prefix is a 4-byte big-endian uint32, matching the PeerRPC
 * RPC-frame convention. This keeps the browser path and the Go path
 * on the same wire format, which the v1 JSON variant did not.
 *
 * The signal-server endpoint is `/ws-v2` (vs the v1 `/ws`). The
 * server distinguishes by path because the body encoding differs.
 */

import type { SignalMessage } from "@peerrpc/peer";
import { SignalMessage as WireSignalMessageV2 } from "@peerrpc/protocol/gen/peerrpc/signaling/v2/signaling_pb.js";
import type { AnnounceRequest_Role } from "@peerrpc/protocol/gen/peerrpc/signaling/v2/signaling_pb.js";

export interface WebSocketSignalV2Config {
  /** WebSocket URL (e.g. "wss://signal.example.com/ws-v2"). */
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

export class WebSocketSignalV2 {
  private cfg: WebSocketSignalV2Config;
  private onMessageCb: ((msg: SignalMessage) => void) | null = null;
  private ws: WebSocket | null = null;

  constructor(cfg: WebSocketSignalV2Config) {
    this.cfg = cfg;
  }

  async connect(): Promise<void> {
    const ws = new WebSocket(this.cfg.url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    await new Promise<void>((resolve, reject) => {
      ws.addEventListener("open", () => resolve(), { once: true });
      ws.addEventListener("error", () => reject(new Error("signal: ws open failed")), { once: true });
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
    const announce = new WireSignalMessageV2({
      service: this.cfg.service,
      body: {
        case: "announce",
        value: {
          peerId: this.cfg.peerId,
          role: this.cfg.role ?? 1 /* ROLE_CLIENT */,
          ...(this.cfg.peerPubkey ? { peerPubkey: this.cfg.peerPubkey } : {}),
        },
      },
    });
    ws.send(encodeLengthPrefixed(announce));
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

function translateOutgoing(msg: SignalMessage): WireSignalMessageV2 {
  const wire = new WireSignalMessageV2();
  switch (msg.type) {
    case "offer":
      wire.body = { case: "offer", value: { sdp: msg.sdp ?? "" } };
      break;
    case "answer":
      wire.body = { case: "answer", value: { sdp: msg.sdp ?? "" } };
      break;
    case "candidate":
      wire.body = {
        case: "candidate",
        value: {
          candidate: msg.candidate ?? "",
          sdpMid: msg.sdpMid ?? "",
          sdpMlineIndex: msg.sdpMLineIndex ?? 0,
        },
      };
      break;
  }
  return wire;
}

function translateIncoming(wire: WireSignalMessageV2): SignalMessage | null {
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

async function encodeLengthPrefixed(msg: WireSignalMessageV2): Promise<ArrayBuffer> {
  const { toBinary } = await import("@bufbuild/protobuf");
  const body = toBinary(WireSignalMessageV2, msg);
  const out = new Uint8Array(4 + body.length);
  const dv = new DataView(out.buffer);
  dv.setUint32(0, body.length, false /* big-endian */);
  out.set(body, 4);
  return out.buffer;
}

async function decodeLengthPrefixed(buf: Uint8Array): Promise<WireSignalMessageV2 | null> {
  if (buf.length < 4) return null;
  const dv = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const len = dv.getUint32(0, false);
  if (buf.length < 4 + len) return null;
  const body = buf.subarray(4, 4 + len);
  const { fromBinary } = await import("@bufbuild/protobuf");
  try {
    return fromBinary(WireSignalMessageV2, body);
  } catch {
    return null;
  }
}
