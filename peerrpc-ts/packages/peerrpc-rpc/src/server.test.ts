import { describe, it, expect } from "vitest";
import {
  Server,
  ServerStream,
  Code,
  ok,
  err,
  MethodKind,
  type ServiceDesc,
} from "../src/main.js";
import type { Channel } from "@peerrpc/transport";
import { Frame, Call, End, Routing, Data } from "@peerrpc/protocol/gen/peerrpc/peerrpc_pb.js";

/** In-memory Channel implementation for tests. Mirrors the shape
 * the real WebRTC-backed Channel exposes; enough to drive Server
 * through one full RPC. */
class FakeChannel implements Channel {
  private frameCb: ((f: Frame) => void) | null = null;
  private closeCb: (() => void) | null = null;
  public sent: Frame[] = [];
  private chunks: Map<number, { total: number; got: number; buf: Map<number, Uint8Array> }> = new Map();

  onFrame(cb: (f: Frame) => void): void { this.frameCb = cb; }
  onClose(cb: () => void): void { this.closeCb = cb; }
  async send(f: Frame): Promise<void> { this.sent.push(f); }

  /** Test helper: simulate an inbound frame from the client. */
  push(f: Frame): void { this.frameCb?.(f); }
  /** Test helper: simulate the channel closing. */
  close(): void { this.closeCb?.(); }

  reassemble(seq: number, total: number, offset: number, data: Uint8Array): Uint8Array | null {
    let entry = this.chunks.get(seq);
    if (!entry) {
      entry = { total, got: 0, buf: new Map() };
      this.chunks.set(seq, entry);
    }
    entry.buf.set(offset, data);
    entry.got += data.length;
    if (entry.got >= entry.total) {
      const out = new Uint8Array(entry.total);
      for (const [off, d] of entry.buf) out.set(d, off);
      this.chunks.delete(seq);
      return out;
    }
    return null;
  }
}

describe("Server", () => {
  it("registers and dispatches a Unary handler", async () => {
    const srv = new Server();
    srv.register("/echo.Echo/Unary", async (stream) => {
      const req = await stream.recv();
      await stream.send(append(req!, " echo"));
      return ok();
    });

    const ch = new FakeChannel();
    const servePromise = srv.serve(ch);

    // Push a Call + End.
    ch.push(new Frame({
      routing: new Routing({ sequence: 1 }),
      type: {
        case: "call",
        value: new Call({ method: "/echo.Echo/Unary", inlineData: new TextEncoder().encode("hi") }),
      },
    }));
    ch.push(new Frame({
      routing: new Routing({ sequence: 1 }),
      type: { case: "end", value: new End({ closeSend: true }) },
    }));

    // Wait for the server to send Begin + End.
    await waitUntil(() => ch.sent.length >= 2);

    // Begin carries the inline response.
    const begin = ch.sent[0];
    expect(begin.type.case).toBe("begin");
    if (begin.type.case === "begin") {
      expect(new TextDecoder().decode(begin.type.value!.inlineData!)).toBe("hi echo");
    }
    // End carries status OK.
    const end = ch.sent[1];
    expect(end.type.case).toBe("end");
    if (end.type.case === "end") {
      expect(end.type.value!.status?.code).toBe(0);
    }

    ch.close();
    await servePromise;
  });

  it("returns UNIMPLEMENTED for unknown methods", async () => {
    const srv = new Server();
    const ch = new FakeChannel();
    const p = srv.serve(ch);
    ch.push(new Frame({
      routing: new Routing({ sequence: 1 }),
      type: {
        case: "call",
        value: new Call({ method: "/nope/Missing" }),
      },
    }));
    await waitUntil(() => ch.sent.length >= 1);
    const end = ch.sent[0];
    expect(end.type.case).toBe("end");
    if (end.type.case === "end") {
      expect(end.type.value!.status?.code).toBe(Code.UNIMPLEMENTED);
    }
    ch.close();
    await p;
  });

  it("registerService installs every method under its fully-qualified path", async () => {
    const desc: ServiceDesc = {
      serviceName: "echo.Echo",
      methods: [
        { method: "Unary", kind: MethodKind.Unary, handler: async (s) => { await s.send(new Uint8Array([1])); return ok(); } },
        { method: "Stream", kind: MethodKind.ServerStreaming, handler: async (s) => ok() },
      ],
    };
    const srv = new Server();
    srv.registerService(desc);

    // Internal: handlers map is private; verify behaviorally.
    const ch1 = new FakeChannel();
    const p1 = srv.serve(ch1);
    ch1.push(new Frame({
      routing: new Routing({ sequence: 1 }),
      type: { case: "call", value: new Call({ method: "/echo.Echo/Unary" }) },
    }));
    await waitUntil(() => ch1.sent.length >= 1);
    expect(ch1.sent[0].type.case).toBe("begin");
    ch1.close();
    await p1;

    const ch2 = new FakeChannel();
    const p2 = srv.serve(ch2);
    ch2.push(new Frame({
      routing: new Routing({ sequence: 1 }),
      type: { case: "call", value: new Call({ method: "/echo.Echo/Stream" }) },
    }));
    await waitUntil(() => ch2.sent.length >= 1);
    // Stream handler returns ok without sending; expect End only.
    expect(ch2.sent[0].type.case).toBe("end");
    ch2.close();
    await p2;
  });
});

function append(base: Uint8Array, suffix: string): Uint8Array {
  const enc = new TextEncoder().encode(suffix);
  const out = new Uint8Array(base.length + enc.length);
  out.set(base, 0);
  out.set(enc, base.length);
  return out;
}

async function waitUntil(pred: () => boolean, timeoutMs = 500): Promise<void> {
  const start = Date.now();
  while (!pred()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitUntil timed out");
    }
    await new Promise((r) => setTimeout(r, 5));
  }
}
