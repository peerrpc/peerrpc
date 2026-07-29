import type { SignalMessage } from "@peerrpc/peer";
import { SignalMessage as WireSignalMessage } from "@peerrpc/protocol/gen/peerrpc/signaling/v1/signaling_pb.js";
import { SignalingService } from "@peerrpc/protocol/gen/peerrpc/signaling/v1/signaling_connect.js";
import type { JoinRequest_Role } from "@peerrpc/protocol/gen/peerrpc/signaling/v1/signaling_connect.js";

export interface ConnectSignalConfig {
  url: string;
  roomId: string;
  peerId: string;
  role?: JoinRequest_Role;
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

export { WebSocketSignal } from "./ws-signal.js";
export type { WebSocketSignalConfig } from "./ws-signal.js";

// v2 (service / AnnounceRequest) — preferred for new code.
export { ConnectSignalV2, type ConnectSignalV2Config } from "./v2.js";
export { WebSocketSignalV2, type WebSocketSignalV2Config } from "./ws_v2.js";

// v1 (roomId / JoinRequest) — retained as deprecated aliases until
// the v2 GA + 2-release migration window closes (see Q1). Callers
// should migrate to ConnectSignalV2 / WebSocketSignalV2.
export { ConnectSignal as ConnectSignalV1 } from "./v1.js";
export type { ConnectSignalConfig as ConnectSignalV1Config } from "./v1.js";

export class ConnectSignal {
  private cfg: ConnectSignalConfig;
  private onMessageCb: ((msg: SignalMessage) => void) | null = null;
  private input: AsyncQueue<WireSignalMessage> | null = null;

  constructor(cfg: ConnectSignalConfig) {
    this.cfg = cfg;
  }

  async connect(): Promise<void> {
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

    this.input = new AsyncQueue();

    this.input.push(new WireSignalMessage({
      roomId: this.cfg.roomId,
      body: {
        case: "join",
        value: { peerId: this.cfg.peerId, role: this.cfg.role ?? 0 },
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
    wire.roomId = this.cfg.roomId;
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
