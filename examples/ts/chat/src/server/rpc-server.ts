import {
  INLINE_MAX, MESSAGE_MAX, CHUNK_SIZE,
} from "@peerrpc/protocol";
import {
  Frame, Call, End, Data, Chunk, Routing,
  ResponseFrame, Begin,
} from "@peerrpc/protocol/gen/peerrpc/peerrpc_pb.js";
import type { Channel } from "@peerrpc/transport";

export interface Status {
  code: number;
  message: string;
}

export function ok(): Status {
  return { code: 0, message: "" };
}

export function err(code: number, msg: string): Status {
  return { code, message: msg };
}

const EOF = Symbol("EOF");

export class ServerStream {
  private seq: number;
  private ch: Channel;
  private inbound: Uint8Array[] = [];
  private sendClosed = false;

  constructor(seq: number, ch: Channel, call: Call) {
    this.seq = seq;
    this.ch = ch;
    if (call.inlineData && call.inlineData.length > 0) {
      this.inbound.push(call.inlineData);
    }
  }

  pushData(data: Uint8Array): void {
    this.inbound.push(data);
  }

  closeSend(): void {
    this.sendClosed = true;
  }

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

  async send(payload: Uint8Array): Promise<void> {
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

type Handler = (stream: ServerStream) => Promise<Status>;

export class Server {
  private handlers = new Map<string, Handler>();

  register(methodPath: string, handler: Handler): void {
    this.handlers.set(methodPath, handler);
  }

  async serve(ch: Channel): Promise<void> {
    const streams = new Map<number, ServerStream>();
    let resolveClosed: () => void;
    const closed = new Promise<void>((resolve) => { resolveClosed = resolve; });

    ch.onFrame((frame) => {
      const seq = frame.routing?.sequence ?? 0;

      switch (frame.type.case) {
        case "call": {
          const call = frame.type.value;
          if (!call) break;
          const handler = this.handlers.get(call.method);
          if (!handler) {
            this.sendEnd(ch, seq, err(12, `unimplemented: ${call.method}`));
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
