/**
 * RPC client: multiplexes PeerRPC calls over a transport.Channel.
 *
 * Matches the Go rpc.Client API: InvokeUnary for single-request/
 * single-response, InvokeServerStreaming for server-streaming, and
 * the stream Send/CloseSend/Recv lifecycle for client/bidi streaming.
 *
 * One Client per transport.Channel. The caller attaches the inbound
 * frame handler via the constructor; the Client dispatches decoded
 * ResponseFrames to the right in-flight stream by Routing.sequence.
 */

import {
  INLINE_MAX,
  MESSAGE_MAX,
  CHUNK_SIZE,
  ResponseFrame,
} from "@peerrpc/protocol";
import { Frame, Call, End, Data, Chunk, Routing } from "@peerrpc/protocol/gen/peerrpc/peerrpc_pb.js";
import type { Channel } from "@peerrpc/transport";
import { AsyncQueue } from "./queue.js";

/** gRPC status codes (subset; see google.rpc.Code). */
export enum Code {
  OK = 0,
  CANCELLED = 1,
  UNKNOWN = 2,
  INVALID_ARGUMENT = 3,
  DEADLINE_EXCEEDED = 4,
  NOT_FOUND = 5,
  ALREADY_EXISTS = 6,
  PERMISSION_DENIED = 7,
  RESOURCE_EXHAUSTED = 8,
  FAILED_PRECONDITION = 9,
  ABORTED = 10,
  OUT_OF_RANGE = 11,
  UNIMPLEMENTED = 12,
  INTERNAL = 13,
  UNAVAILABLE = 14,
  DATA_LOSS = 15,
  UNAUTHENTICATED = 16,
}

/** Status mirrors google.rpc.Status. */
export interface Status {
  code: number;
  message: string;
}

/** Header metadata (multi-valued map). */
export type Metadata = Record<string, string[]>;

interface StreamState {
  seq: number;
  inbound: AsyncQueue<Uint8Array>;
  header: Metadata;
  trailer: Metadata;
  done: boolean;
  status: Status | null;
  resolveEnd: (s: Status) => void;
}

/**
 * Client multiplexes RPC calls over one transport.Channel.
 */
export class Client {
  private ch: Channel;
  private streams: Map<number, StreamState> = new Map();
  private seqAlloc: number = 1;

  constructor(ch: Channel) {
    this.ch = ch;
    // The client only handles server->client ResponseFrames; narrow
    // the union type from onFrame (which may also carry a Frame on the
    // server side) so dispatch receives the type it expects.
    ch.onFrame((frame) => {
      if (frame instanceof ResponseFrame) {
        this.dispatch(frame);
      }
    });
    ch.onClose(() => this.failAll());
  }

  /**
   * Invoke a Unary RPC. Returns the single response payload and
   * final status.
   */
  async invokeUnary(
    method: string,
    req: Uint8Array,
    metadata?: Metadata
  ): Promise<{ response: Uint8Array; status: Status }> {
    const stream = this.openStream();

    // Build Call frame.
    const call = new Call({
      method,
      protocolVersion: 1,
    });
    if (metadata) {
      const md: Record<string, { values: string[] }> = {};
      for (const [k, vs] of Object.entries(metadata)) {
        md[k] = { values: vs };
      }
      call.metadata = { md };
    }
    if (req.length <= INLINE_MAX) {
      call.inlineData = req;
    }

    // Send Call.
    await this.ch.send(new Frame({
      routing: new Routing({ sequence: stream.seq }),
      type: { case: "call", value: call },
    }));

    // Send non-inline payload if any.
    if (call.inlineData === undefined && req.length > 0) {
      await this.sendPayload(stream.seq, req);
    }

    // Half-close immediately (Unary).
    await this.ch.send(new Frame({
      routing: new Routing({ sequence: stream.seq }),
      type: { case: "end", value: new End({ closeSend: true }) },
    }));

    // Collect exactly one response.
    return this.collectUnary(stream);
  }

  /**
   * Invoke a server-streaming RPC. Returns a ClientStream whose
   * recv() yields each response chunk until the server ends.
   */
  async invokeServerStreaming(
    method: string,
    req: Uint8Array,
    metadata?: Metadata
  ): Promise<ClientStream> {
    const stream = this.openStream();

    const call = new Call({ method, protocolVersion: 1 });
    if (metadata) {
      const md: Record<string, { values: string[] }> = {};
      for (const [k, vs] of Object.entries(metadata)) {
        md[k] = { values: vs };
      }
      call.metadata = { md };
    }
    if (req.length <= INLINE_MAX) {
      call.inlineData = req;
    }

    await this.ch.send(new Frame({
      routing: new Routing({ sequence: stream.seq }),
      type: { case: "call", value: call },
    }));
    if (call.inlineData === undefined && req.length > 0) {
      await this.sendPayload(stream.seq, req);
    }
    await this.ch.send(new Frame({
      routing: new Routing({ sequence: stream.seq }),
      type: { case: "end", value: new End({ closeSend: true }) },
    }));

    return new ClientStream(stream, this.ch);
  }

  /**
   * Invoke a client-streaming RPC. The caller uses the returned
   * ClientStream's send() + closeSend() + recv().
   *
   * @param method - Fully qualified method path.
   * @param firstReq - Optional initial payload sent inline with the
   *   Call frame (≤INLINE_MAX) or as Data frames.
   * @param metadata - Optional request metadata.
   */
  async invokeClientStreaming(
    method: string,
    firstReq?: Uint8Array,
    metadata?: Metadata
  ): Promise<ClientStream> {
    const stream = this.openStream();
    const call = new Call({ method, protocolVersion: 1 });
    if (metadata) {
      const md: Record<string, { values: string[] }> = {};
      for (const [k, vs] of Object.entries(metadata)) {
        md[k] = { values: vs };
      }
      call.metadata = { md };
    }
    if (firstReq && firstReq.length <= INLINE_MAX) {
      call.inlineData = firstReq;
    }
    await this.ch.send(new Frame({
      routing: new Routing({ sequence: stream.seq }),
      type: { case: "call", value: call },
    }));
    // If firstReq is large, send as Data frames.
    if (firstReq && firstReq.length > INLINE_MAX) {
      await this.sendPayload(stream.seq, firstReq);
    }
    return new ClientStream(stream, this.ch);
  }

  /**
   * Invoke a bidi-streaming RPC. Wire shape is identical to client-
   * streaming; the difference is how the application uses recv().
   *
   * @param method - Fully qualified method path.
   * @param firstReq - Optional initial payload (see invokeClientStreaming).
   * @param metadata - Optional request metadata.
   */
  async invokeBidiStreaming(
    method: string,
    firstReq?: Uint8Array,
    metadata?: Metadata
  ): Promise<ClientStream> {
    return this.invokeClientStreaming(method, firstReq, metadata);
  }

  private openStream(): StreamState {
    const seq = this.seqAlloc;
    this.seqAlloc += 2; // odd = client-initiated
    const stream: StreamState = {
      seq,
      inbound: new AsyncQueue<Uint8Array>(),
      header: {},
      trailer: {},
      done: false,
      status: null,
      resolveEnd: () => {},
    };
    this.streams.set(seq, stream);
    return stream;
  }

  private async sendPayload(seq: number, payload: Uint8Array): Promise<void> {
    if (payload.length <= MESSAGE_MAX) {
      await this.ch.send(new Frame({
        routing: new Routing({ sequence: seq }),
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
      const chunk = payload.subarray(off, end);
      await this.ch.send(new Frame({
        routing: new Routing({ sequence: seq }),
        type: {
          case: "data",
          value: new Data({
            content: {
              case: "chunk",
              value: new Chunk({
                totalSize: payload.length,
                offset: off,
                data: chunk,
              }),
            },
          }),
        },
      }));
    }
  }

  private dispatch(frame: ResponseFrame): void {
    const seq = frame.routing?.sequence ?? 0;
    const stream = this.streams.get(seq);
    if (!stream) return;

    switch (frame.type.case) {
      case "begin": {
        const hdr = frame.type.value?.header;
        if (hdr?.md) {
          for (const [k, v] of Object.entries(hdr.md)) {
            stream.header[k] = v.values.slice();
          }
        }
        const inline = frame.type.value?.inlineData;
        if (inline && inline.length > 0) {
          stream.inbound.push(inline);
        }
        break;
      }
      case "data": {
        const data = frame.type.value;
        if (!data) break;
        if (data.content.case === "message") {
          stream.inbound.push(data.content.value);
        } else if (data.content.case === "chunk") {
          const full = this.ch.reassemble(
            seq,
            data.content.value.totalSize,
            data.content.value.offset,
            data.content.value.data
          );
          if (full) {
            stream.inbound.push(full);
          }
        }
        break;
      }
      case "end": {
        const end = frame.type.value;
        if (end?.trailer?.md) {
          for (const [k, v] of Object.entries(end.trailer.md)) {
            stream.trailer[k] = v.values.slice();
          }
        }
        stream.done = true;
        stream.status = end?.status
          ? { code: end.status.code, message: end.status.message }
          : { code: 0, message: "" };
        stream.inbound.close();
        stream.resolveEnd(stream.status);
        this.streams.delete(seq);
        break;
      }
    }
  }

  private async collectUnary(
    stream: StreamState
  ): Promise<{ response: Uint8Array; status: Status }> {
    // recv() returns the first response or null at EOF. If data arrives
    // before End we await the status; if End arrives first (no response),
    // recv() returns null and we surface the status.
    const resp = await stream.inbound.recv();
    if (resp) {
      // Wait for the End status.
      return new Promise((resolve) => {
        stream.resolveEnd = (s) => resolve({ response: resp, status: s });
      });
    }
    // No response before EOF.
    return {
      response: new Uint8Array(0),
      status: stream.status ?? { code: 0, message: "" },
    };
  }

  private failAll(): void {
    for (const [, s] of this.streams) {
      if (!s.done) {
        s.done = true;
        s.status = { code: Code.UNAVAILABLE, message: "transport closed" };
        s.inbound.close();
        s.resolveEnd(s.status);
      }
    }
    this.streams.clear();
  }
}

/**
 * ClientStream is the per-RPC handle for streaming RPCs. Supports
 * send() / closeSend() on the outbound side and recv() on the
 * inbound side.
 */
export class ClientStream {
  private state: StreamState;
  private ch: Channel;
  private closed = false;

  constructor(state: StreamState, ch: Channel) {
    this.state = state;
    this.ch = ch;
  }

  /** Send one request message. */
  async send(payload: Uint8Array): Promise<void> {
    if (this.closed) throw new Error("stream: closed");
    if (payload.length <= MESSAGE_MAX) {
      await this.ch.send(new Frame({
        routing: new Routing({ sequence: this.state.seq }),
        type: {
          case: "data",
          value: new Data({
            content: { case: "message", value: payload },
          }),
        },
      }));
      return;
    }
    // Chunked send.
    for (let off = 0; off < payload.length; off += CHUNK_SIZE) {
      const end = Math.min(off + CHUNK_SIZE, payload.length);
      await this.ch.send(new Frame({
        routing: new Routing({ sequence: this.state.seq }),
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

  /** Half-close: signal the server no more messages will follow. */
  async closeSend(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    await this.ch.send(new Frame({
      routing: new Routing({ sequence: this.state.seq }),
      type: { case: "end", value: new End({ closeSend: true }) },
    }));
  }

  /**
   * Receive the next response message. Returns null when the server
   * has ended (EOF) or the transport closed.
   */
  async recv(): Promise<Uint8Array | null> {
    return this.state.inbound.recv();
  }

  /** Response header (available after Begin arrives). */
  get header(): Metadata {
    return this.state.header;
  }

  /** Response trailer (available after End arrives). */
  get trailer(): Metadata {
    return this.state.trailer;
  }

  /** Final status (available after End arrives). */
  get status(): Status | null {
    return this.state.status;
  }
}
