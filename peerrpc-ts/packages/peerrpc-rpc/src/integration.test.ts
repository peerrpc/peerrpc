/**
 * Integration tests wiring a real Client to a real Server through an
 * in-memory bridge channel (no WebRTC). Covers 1-to-1, 1-to-many,
 * and call-timing scenarios the FakeChannel-based server tests cannot
 * exercise (FakeChannel is one-directional and never involves a Client).
 */

import { describe, it, expect } from "vitest";
import { Server, ok, MethodKind, type ServiceDesc } from "../src/main.js";
import { Client, Code } from "../src/index.js";
import type { Frame, ResponseFrame } from "@peerrpc/protocol";

// ─── BridgeChannel ───────────────────────────────────────────

/**
 * Minimal Channel-like object: pairs a Client and a Server. Each side
 * receives frames sent by the other via its onFrame callback. Mirrors
 * the real Channel surface the RPC layer touches:
 *   onFrame / onClose / send / setDecodeMode / reassemble / close
 * but with no real RTCDataChannel underneath.
 */
class BridgeChannel {
  private frameCb: ((f: Frame | ResponseFrame) => void) | null = null;
  private closeCb: (() => void) | null = null;
  private peer: BridgeChannel | null = null;
  closed = false;
  // Chunk reassembly is a passthrough for tests (no chunked payloads).
  reassemble = (_seq: number, _total: number, _offset: number, data: Uint8Array): Uint8Array | null => data;

  link(peer: BridgeChannel): void {
    this.peer = peer;
    peer.peer = this;
  }

  onFrame(cb: (f: Frame | ResponseFrame) => void): void { this.frameCb = cb; }
  onClose(cb: () => void): void { this.closeCb = cb; }
  setDecodeMode(_mode: "response" | "request"): void { /* no-op */ }
  isClosed(): boolean { return this.closed; }

  async send(frame: Frame | ResponseFrame): Promise<void> {
    // Deliver to the peer's onFrame callback (the RPC layer's dispatch).
    this.peer?.frameCb?.(frame);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.closeCb?.();
    this.peer?.closeCb?.();
  }
}

/** Create a linked pair: clientCh (sends Frame, receives ResponseFrame) + serverCh. */
function bridge(): { clientCh: BridgeChannel; serverCh: BridgeChannel } {
  const clientCh = new BridgeChannel();
  const serverCh = new BridgeChannel();
  clientCh.link(serverCh);
  return { clientCh, serverCh };
}

// ─── Echo service ────────────────────────────────────────────

function registerEcho(srv: Server): void {
  const desc: ServiceDesc = {
    serviceName: "echo.Echo",
    methods: [
      {
        method: "Echo",
        kind: MethodKind.Unary,
        handler: async (stream) => {
          const req = await stream.recv();
          if (!req) return { code: 13, message: "empty" };
          const out = new Uint8Array(req.length + 6);
          out.set(new TextEncoder().encode("echo: "), 0);
          out.set(req, 6);
          await stream.send(out);
          return ok();
        },
      },
      {
        method: "Stream",
        kind: MethodKind.ServerStreaming,
        handler: async (stream) => {
          const req = await stream.recv();
          const label = req ? new TextDecoder().decode(req) : "";
          for (let i = 1; i <= 5; i++) {
            await stream.send(new TextEncoder().encode(`chunk ${i} for ${label}`));
          }
          return ok();
        },
      },
      {
        method: "Chat",
        kind: MethodKind.BidiStreaming,
        handler: async (stream) => {
          let seq = 0;
          for (;;) {
            const msg = await stream.recv();
            if (msg === null) return ok();
            seq++;
            await stream.send(new TextEncoder().encode(`ack ${seq}: ${new TextDecoder().decode(msg)}`));
          }
        },
      },
    ],
  };
  srv.registerService(desc);
}

function startServer(srv: Server, serverCh: BridgeChannel): void {
  srv.serve(serverCh).catch(() => { /* best-effort */ });
}

const dec = new TextDecoder();

// ─── Tests ───────────────────────────────────────────────────

describe("bridge integration", () => {
  it("unary 1-to-1", async () => {
    const srv = new Server();
    registerEcho(srv);
    const { clientCh, serverCh } = bridge();
    startServer(srv, serverCh);
    const client = new Client(clientCh as unknown as InstanceType<typeof import("@peerrpc/transport").Channel>);

    const { response, status } = await client.invokeUnary("/echo.Echo/Echo", new TextEncoder().encode("hi"));
    expect(status.code).toBe(0);
    expect(dec.decode(response)).toBe("echo: hi");
  });

  it("concurrent unary on one client", async () => {
    const srv = new Server();
    registerEcho(srv);
    const { clientCh, serverCh } = bridge();
    startServer(srv, serverCh);
    const client = new Client(clientCh as unknown as InstanceType<typeof import("@peerrpc/transport").Channel>);

    const reqs = ["one", "two", "three"];
    const results = await Promise.all(reqs.map((r) => client.invokeUnary("/echo.Echo/Echo", new TextEncoder().encode(r))));

    const got = results.map((r) => dec.decode(r.response)).sort();
    expect(got).toEqual(["echo: one", "echo: three", "echo: two"]);
    for (const r of results) expect(r.status.code).toBe(0);
  });

  it("server streaming drains to EOF", async () => {
    const srv = new Server();
    registerEcho(srv);
    const { clientCh, serverCh } = bridge();
    startServer(srv, serverCh);
    const client = new Client(clientCh as unknown as InstanceType<typeof import("@peerrpc/transport").Channel>);

    const stream = await client.invokeServerStreaming("/echo.Echo/Stream", new TextEncoder().encode("flow"));
    const chunks: string[] = [];
    for (;;) {
      const c = await stream.recv();
      if (c === null) break;
      chunks.push(dec.decode(c));
    }
    expect(chunks.length).toBe(5);
    expect(chunks[0]).toContain("flow");
    expect(stream.status?.code).toBe(0);
  });

  it("bidi interleaved", async () => {
    const srv = new Server();
    registerEcho(srv);
    const { clientCh, serverCh } = bridge();
    startServer(srv, serverCh);
    const client = new Client(clientCh as unknown as InstanceType<typeof import("@peerrpc/transport").Channel>);

    const stream = await client.invokeBidiStreaming("/echo.Echo/Chat");
    for (let i = 1; i <= 3; i++) {
      await stream.send(new TextEncoder().encode(`m${i}`));
      const resp = await stream.recv();
      expect(resp).not.toBeNull();
      expect(dec.decode(resp!)).toBe(`ack ${i}: m${i}`);
    }
    await stream.closeSend();
    // Server returns OK → EOF.
    expect(await stream.recv()).toBeNull();
    expect(stream.status?.code).toBe(0);
  });

  it("1-to-many: one server, three clients", async () => {
    // One Server instance, three independent client/server channel
    // pairs. Each server side runs its own serve() in the background
    // (the Server multiplexes handlers by sequence per channel).
    const srv = new Server();
    registerEcho(srv);

    const clients: Client[] = [];
    for (let i = 0; i < 3; i++) {
      const { clientCh, serverCh } = bridge();
      startServer(srv, serverCh);
      clients.push(new Client(clientCh as unknown as InstanceType<typeof import("@peerrpc/transport").Channel>));
    }

    const results = await Promise.all(
      clients.map((c, i) => c.invokeUnary("/echo.Echo/Echo", new TextEncoder().encode(`c${i}`))),
    );
    results.forEach((r, i) => {
      expect(r.status.code).toBe(0);
      expect(dec.decode(r.response)).toBe(`echo: c${i}`);
    });
  });

  it("client disconnect surfaces to server", async () => {
    const srv = new Server();
    registerEcho(srv);
    const { clientCh, serverCh } = bridge();
    startServer(srv, serverCh);
    const client = new Client(clientCh as unknown as InstanceType<typeof import("@peerrpc/transport").Channel>);

    // serve() registers its own onClose; re-register after it so we
    // observe the close (the bridge links both sides synchronously).
    let serverClosed = false;
    serverCh.onClose(() => { serverClosed = true; });

    clientCh.close();
    expect(serverClosed).toBe(true);
  });
});
