/**
 * Signal layer: connect-web client for the standalone PeerRPC
 * signal-server.
 *
 * The browser uses @connectrpc/connect-web to talk to the
 * signal-server's SignalingService.Exchange bidi stream. This module
 * wraps the stream into the SignalTransport shape the Peer layer
 * expects.
 */

import type { SignalMessage } from "@peerrpc/peer";
import { SignalMessage as WireSignalMessage } from "@peerrpc/protocol/gen/peerrpc/signaling/v1/signaling_pb.js";
import {
  SignalingService,
  type JoinRequest_Role,
} from "@peerrpc/protocol/gen/peerrpc/signaling/v1/signaling_pb.js";

export interface ConnectSignalConfig {
  /** signal-server base URL, e.g. https://signal.example.com */
  url: string;
  /** room id to join. */
  roomId: string;
  /** peer id for this client. */
  peerId: string;
  /** offerer or answerer. */
  role?: JoinRequest_Role;
  /** bearer token for the Authorization header. */
  token?: string;
}

/**
 * ConnectSignal adapts the connect-web SignalingService.Exchange
 * bidi stream to the SignalTransport interface the Peer layer
 * consumes.
 *
 * On construction it joins the configured room; on send it pushes
 * SDP/ICE messages into the stream; onMessage delivers messages from
 * the remote peer.
 */
export class ConnectSignal {
  private cfg: ConnectSignalConfig;
  private onMessageCb: ((msg: SignalMessage) => void) | null = null;

  constructor(cfg: ConnectSignalConfig) {
    this.cfg = cfg;
  }

  /**
   * Connect to the signal-server and join the configured room.
   * Returns when the join is acknowledged.
   *
   * This MUST be called before send / onMessage.
   */
  async connect(): Promise<void> {
    // Dynamic import to avoid pulling connect-web into packages that
    // don't need it (e.g. server-side tests).
    const { createConnectTransport } = await import(
      "@connectrpc/connect-web"
    );
    const { createClient } = await import("@connectrpc/connect");

    const transport = createConnectTransport({
      baseUrl: this.cfg.url,
      ...(this.cfg.token
        ? { interceptors: [authInterceptor(this.cfg.token)] }
        : {}),
    });
    const client = createClient(SignalingService, transport);

    // Start the Exchange bidi stream.
    this.stream = client.call(SignalingService.method.exchange, transport);

    // Send the Join message.
    await this.stream.send({
      roomId: this.cfg.roomId,
      body: {
        case: "join",
        value: {
          peerId: this.cfg.peerId,
          role: this.cfg.role ?? 0,
        },
      },
    });

    // Start the inbound pump.
    this.startPump();
  }

  private stream: any = null;

  onMessage(cb: (msg: SignalMessage) => void): void {
    this.onMessageCb = cb;
  }

  send(msg: SignalMessage): void {
    if (!this.stream) {
      throw new Error("signal: not connected; call connect() first");
    }
    const wire = translateOutgoing(msg);
    wire.roomId = this.cfg.roomId;
    this.stream.send(wire).catch(() => {
      // best-effort; the stream may have closed
    });
  }

  close(): void {
    if (this.stream) {
      this.stream.close().catch(() => {});
    }
  }

  private async startPump(): Promise<void> {
    if (!this.stream) return;
    try {
      for (;;) {
        const msg = await this.stream.receive();
        if (!msg) break;
        const translated = translateIncoming(msg);
        if (translated && this.onMessageCb) {
          this.onMessageCb(translated);
        }
      }
    } catch {
      // stream closed
    }
  }
}

function translateOutgoing(msg: SignalMessage): WireSignalMessage {
  const wire = new WireSignalMessage();
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
          sdpMLineIndex: msg.sdpMLineIndex ?? 0,
        },
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
        sdpMLineIndex: wire.body.value.sdpMLineIndex,
      };
    default:
      return null;
  }
}

function authInterceptor(token: string) {
  return (next: any) => async (req: any) => {
    req.header.set("Authorization", `Bearer ${token}`);
    return next(req);
  };
}
