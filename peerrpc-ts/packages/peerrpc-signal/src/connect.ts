/**
 * Connect-RPC signal client: speaks the v2 signaling wire format
 * (service / AnnounceRequest) to a remote signal-server over HTTP/2.
 */

import type { SignalMessage } from "@peerrpc/peer";
import { SignalMessage as WireSignalMessageV2 } from "@peerrpc/protocol/gen/peerrpc/signaling/v2/signaling_pb.js";
import { SignalingService } from "@peerrpc/protocol/gen/peerrpc/signaling/v2/signaling_connect.js";
import type { AnnounceRequest_Role } from "@peerrpc/protocol/gen/peerrpc/signaling/v2/signaling_pb.js";

export interface ConnectSignalConfig {
  /** Signal-server base URL (e.g. "https://signal.example.com"). */
  url: string;
  /** Rendezvous key. */
  service: string;
  /** Caller-chosen identifier within the service. */
  peerId: string;
  /** Application-level role. Defaults to ROLE_CLIENT. */
  role?: AnnounceRequest_Role;
  /** Optional Ed25519 public key. v2 servers accept but do not verify. */
  peerPubkey?: Uint8Array;
  /** Bearer token injected as Authorization: Bearer <token>. */
  token?: string;
}

class AsyncQueue<T> {
  private items: T[] = [];
  private resolvers: ((value: IteratorResult<T>) => void)[] = [];
  private _closed = false;

  push(item: T): void {
    if (this._closed) throw new Error("signal: queue closed");
    if (this.resolvers.length > 0) {
      this.resolvers.shift()!({ value: item, done: false });
    } else {
      this.items.push(item);
    }
  }

  close(): void {
    this._closed = true;
    for (const resolve of this.resolvers) {
      resolve({ value: undefined as never, done: true });
    }
    this.resolvers = [];
  }

  [Symbol.asyncIterator](): AsyncIterator<T> {
    return {
      next: (): Promise<IteratorResult<T>> => {
        if (this.items.length > 0) {
          return Promise.resolve({ value: this.items.shift()!, done: false });
        }
        if (this._closed) {
          return Promise.resolve({ value: undefined as never, done: true });
        }
        return new Promise<IteratorResult<T>>((resolve) => {
          this.resolvers.push(resolve);
        });
      },
      return: (): Promise<IteratorResult<T>> => {
        this.close();
        return Promise.resolve({ value: undefined as never, done: true });
      },
    };
  }
}

export class ConnectSignal {
  private cfg: ConnectSignalConfig;
  private onMessageCb: ((msg: SignalMessage) => void) | null = null;
  private input: AsyncQueue<WireSignalMessageV2> | null = null;

  constructor(cfg: ConnectSignalConfig) {
    this.cfg = cfg;
  }

  async connect(): Promise<void> {
    const { createConnectTransport } = await import("@connectrpc/connect-web");
    const { createClient } = await import("@connectrpc/connect");

    const transport = createConnectTransport({
      baseUrl: this.cfg.url,
      ...(this.cfg.token ? { interceptors: [authInterceptor(this.cfg.token)] } : {}),
    });
    const client = createClient(SignalingService, transport);

    this.input = new AsyncQueue();

    this.input.push(new WireSignalMessageV2({
      service: this.cfg.service,
      body: {
        case: "announce",
        value: {
          peerId: this.cfg.peerId,
          role: this.cfg.role ?? 1 /* ROLE_CLIENT */,
          ...(this.cfg.peerPubkey ? { peerPubkey: this.cfg.peerPubkey } : {}),
        },
      },
    }));

    const output = client.exchange(this.input);
    this.startPump(output);
  }

  onMessage(cb: (msg: SignalMessage) => void): void {
    this.onMessageCb = cb;
  }

  send(msg: SignalMessage): void {
    if (!this.input) {
      throw new Error("signal: not connected; call connect() first");
    }
    const wire = translateOutgoing(msg);
    wire.service = this.cfg.service;
    this.input.push(wire);
  }

  close(): void {
    if (this.input) {
      this.input.close();
      this.input = null;
    }
  }

  private async startPump(output: AsyncIterable<WireSignalMessageV2>): Promise<void> {
    try {
      for await (const msg of output) {
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

function authInterceptor(token: string) {
  return (next: any) => async (req: any) => {
    req.header.set("Authorization", `Bearer ${token}`);
    return next(req);
  };
}
