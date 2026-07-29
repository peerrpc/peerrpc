/**
 * RPC server: dispatches inbound PeerRPC calls on a transport.Channel
 * to registered handlers.
 *
 * Matches the Go rpc.Server API: register a method path with a
 * handler, call serve(channel), and the server multiplexes the
 * inbound Frame stream into per-RPC ServerStreams until the channel
 * closes.
 *
 * One Server per transport.Channel. Callers wanting to serve many
 * peers from one process should instantiate one Server per accepted
 * channel (see the @peerrpc/peerrpc Listen facade).
 */

import {
  INLINE_MAX,
  MESSAGE_MAX,
  CHUNK_SIZE,
  ResponseFrame,
} from "@peerrpc/protocol";
import {
  Frame,
  Call,
  End,
  Data,
  Chunk,
  Routing,
  Begin,
} from "@peerrpc/protocol/gen/peerrpc/peerrpc_pb.js";
import type { Channel } from "@peerrpc/transport";
import { Code, type Status } from "./index.js";

export { Code };
export type { Status };

/** Per-RPC stream the handler reads from and writes to. */
export class ServerStream {
  private seq: number;
  private ch: Channel;
  private inbound: Uint8Array[] = [];
  private sendClosed = false;
  private headerSent = false;

  constructor(seq: number, ch: Channel, call: Call) {
    this.seq = seq;
    this.ch = ch;
    if (call.inlineData && call.inlineData.length > 0) {
      this.inbound.push(call.inlineData);
    }
  }

  /** Internal: enqueue an inbound payload received as a Data frame. */
  pushData(data: Uint8Array): void {
    this.inbound.push(data);
  }

  /** Internal: mark the client's send side as closed. */
  closeSend(): void {
    this.sendClosed = true;
  }

  /** Receive the next request message; null on client half-close. */
  async recv(): Promise<Uint8Array | null> {
    if (this.inbound.length > 0) {
      return this.inbound.shift()!;
    }
    if (this.sendClosed) {
      return null;
    }
    return new Promise((resolve) => {
      const poll = () => {
        if (this.inbound.length > 0) {
          resolve(this.inbound.shift()!);
        } else if (this.sendClosed) {
          resolve(null);
        } else {
          setTimeout(poll, 1);
        }
      };
      poll();
    });
  }

  /** Send one response message. The first send auto-emits a Begin. */
  async send(payload: Uint8Array): Promise<void> {
    if (!this.headerSent) {
      this.headerSent = true;
      if (payload.length <= INLINE_MAX) {
        await this.ch.send(new ResponseFrame({
          routing: new Routing({ sequence: this.seq }),
          type: {
            case: "begin",
            value: new Begin({ inlineData: payload }),
          },
        }));
        return;
      }
      // Begin without inline; payload rides as Data below.
      await this.ch.send(new ResponseFrame({
        routing: new Routing({ sequence: this.seq }),
        type: { case: "begin", value: new Begin() },
      }));
    }
    if (payload.length <= MESSAGE_MAX) {
      await this.ch.send(new ResponseFrame({
        routing: new Routing({ sequence: this.seq }),
        type: {
          case: "data",
          value: new Data({
            content: { case: "message", value: payload },
          }),
        },
      }));
      return;
    }
    for (let off = 0; off < payload.length; off += CHUNK_SIZE) {
      const end = Math.min(off + CHUNK_SIZE, payload.length);
      await this.ch.send(new ResponseFrame({
        routing: new Routing({ sequence: this.seq }),
        type: {
          case: "data",
          value: new Data({
            content: {
              case: "chunk",
              value: new Chunk({
                totalSize: payload.length,
                offset: off,
                data: payload.subarray(off, end),
              }),
            },
          }),
        },
      }));
    }
  }

  /** Final status; emits the End frame. */
  async endWith(status: Status): Promise<void> {
    await this.ch.send(new ResponseFrame({
      routing: new Routing({ sequence: this.seq }),
      type: {
        case: "end",
        value: new End({
          status: { code: status.code, message: status.message },
        }),
      },
    }));
  }
}

/** Convenience constructors mirroring Go's rpc.OK / rpc.Err. */
export function ok(): Status {
  return { code: 0, message: "" };
}

export function err(code: number, msg: string): Status {
  return { code, message: msg };
}

export type Handler = (stream: ServerStream) => Promise<Status>;

/** Method kind enum mirroring Go's rpc.MethodKind. */
export enum MethodKind {
  Unary = 0,
  ServerStreaming = 1,
  ClientStreaming = 2,
  BidiStreaming = 3,
}

/** ServiceDesc mirrors Go's rpc.ServiceDesc for generated code. */
export interface ServiceDesc {
  serviceName: string;
  methods: Array<{
    method: string;
    kind: MethodKind;
    handler: Handler;
  }>;
}

/**
 * Server dispatches inbound Frames on a transport.Channel to
 * registered handlers.
 */
export class Server {
  private handlers = new Map<string, Handler>();

  /** Register a fully-qualified method path (e.g. "/echo.Echo/Echo"). */
  register(methodPath: string, handler: Handler): void {
    this.handlers.set(methodPath, handler);
  }

  /** Register every method of a ServiceDesc under its fully-qualified path. */
  registerService(desc: ServiceDesc): void {
    for (const m of desc.methods) {
      const path = "/" + desc.serviceName + "/" + m.method;
      this.register(path, m.handler);
    }
  }

  /**
   * Serve channel until it closes. Each Call frame spawns a fresh
   * ServerStream and runs its handler to completion in its own
   * promise; multiple concurrent RPCs on the same channel are
   * supported.
   */
  async serve(ch: Channel): Promise<void> {
    const streams = new Map<number, ServerStream>();
    let resolveClosed: () => void;
    const closed = new Promise<void>((resolve) => { resolveClosed = resolve; });

    ch.onFrame((frame: Frame) => {
      const seq = frame.routing?.sequence ?? 0;

      switch (frame.type.case) {
        case "call": {
          const call = frame.type.value;
          if (!call) break;
          const handler = this.handlers.get(call.method);
          if (!handler) {
            this.sendEnd(ch, seq, err(Code.UNIMPLEMENTED, `unimplemented: ${call.method}`));
            break;
          }
          const stream = new ServerStream(seq, ch, call);
          streams.set(seq, stream);
          handler(stream).then((status) => stream.endWith(status));
          break;
        }
        case "data": {
          const data = frame.type.value;
          if (!data) break;
          const stream = streams.get(seq);
          if (!stream) break;
          if (data.content.case === "message") {
            stream.pushData(data.content.value);
          } else if (data.content.case === "chunk") {
            const full = ch.reassemble(
              seq,
              data.content.value.totalSize,
              data.content.value.offset,
              data.content.value.data,
            );
            if (full) stream.pushData(full);
          }
          break;
        }
        case "end": {
          const stream = streams.get(seq);
          if (stream) {
            stream.closeSend();
            streams.delete(seq);
          }
          break;
        }
      }
    });

    ch.onClose(() => resolveClosed!());
    await closed;
  }

  private sendEnd(ch: Channel, seq: number, status: Status): void {
    ch.send(new ResponseFrame({
      routing: new Routing({ sequence: seq }),
      type: {
        case: "end",
        value: new End({
          status: { code: status.code, message: status.message },
        }),
      },
    }));
  }
}
