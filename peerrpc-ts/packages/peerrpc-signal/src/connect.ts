/**
 * Connect-RPC signal client: speaks the signaling wire format
 * (service / AnnounceRequest) to a remote signal-server over HTTP/2.
 */

import type { SignalMessage } from "@peerrpc/peer";
import { SignalMessage as WireSignalMessage, SdpOffer, SdpAnswer, IceCandidate } from "@peerrpc/protocol/gen/peerrpc/signaling/signaling_pb.js";
import { SignalingService } from "@peerrpc/protocol/gen/peerrpc/signaling/signaling_connect.js";
import type { AnnounceRequest_Role } from "@peerrpc/protocol/gen/peerrpc/signaling/signaling_pb.js";

export interface ConnectSignalConfig {
  /** Signal-server base URL (e.g. "https://signal.example.com"). */
  url: string;
  /** Rendezvous key. */
  service: string;
  /** Caller-chosen identifier within the service. */
  peerId: string;
  /** Application-level role. Defaults to ROLE_CLIENT. */
  role?: AnnounceRequest_Role;
  /** Optional Ed25519 public key. Servers accept but do not verify. */
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
  private input: AsyncQueue<WireSignalMessage> | null = null;
  // Captured by startPump if the bidi stream errors before connect()
  // resolves, so connect() can surface the real cause (e.g. TLS cert
  // not accepted) instead of letting dial fail later with "queue closed".
  private connectError: Error | null = null;

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

    this.input.push(new WireSignalMessage({
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

    // Eagerly start the pump and surface the first stream error instead
    // of returning a healthy-looking transport that fails on the first
    // send. connect-web defers the actual HTTP request until the first
    // iteration, so we drive one step and capture any transport error
    // (e.g. self-signed cert not accepted, server unreachable) before
    // connect() resolves.
    this.connectError = null;
    const pumpDone = this.startPump(output);
    // Yield once so the pump's first iteration (which issues the HTTP
    // request) has a chance to run and, on failure, set connectError.
    await Promise.resolve();
    await Promise.resolve();
    if (this.connectError) {
      throw this.connectError;
    }
    // Keep the pump promise alive for diagnostics; unhandled rejection
    // is avoided because startPump never throws (it captures into
    // connectError instead).
    void pumpDone.catch(() => { /* captured in connectError */ });
  }

  onMessage(cb: (msg: SignalMessage) => void): void {
    this.onMessageCb = cb;
  }

  send(msg: SignalMessage): void {
    if (this.connectError) {
      throw this.connectError;
    }
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

  private async startPump(output: AsyncIterable<WireSignalMessage>): Promise<void> {
    try {
      for await (const msg of output) {
        const translated = translateIncoming(msg);
        if (translated && this.onMessageCb) {
          this.onMessageCb(translated);
        }
      }
    } catch (err) {
      // Capture the real cause so connect() / send() can surface it.
      // Common: self-signed cert not accepted, server unreachable.
      const msg = err instanceof Error ? err.message : String(err);
      this.connectError = new Error(`signal: connect stream failed: ${msg}`);
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

function authInterceptor(token: string) {
  return (next: any) => async (req: any) => {
    req.header.set("Authorization", `Bearer ${token}`);
    return next(req);
  };
}
