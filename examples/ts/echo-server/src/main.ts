/**
 * PeerRPC browser echo server.
 *
 * Acts as a PeerRPC server (Answerer) behind a signal-server and
 * serves the echo.Echo service with all RPC types, mirroring the Go
 * echo-server (examples/go/echo-server):
 *
 *   /echo.Echo/Echo                Unary
 *   /echo.Echo/Stream              Server-Streaming
 *   /echo.Echo/Collect             Client-Streaming
 *   /echo.Echo/Chat                Bidi-Streaming
 *   /echo.Echo/LargeEcho           Unary (multi-MB round-trip)
 *   /echo.Echo/LargeDownload       Server-Streaming (single big message)
 *   /echo.Echo/LargeEchoStream     Bidi-Streaming (unbounded echo)
 *   /echo.Echo/LargeDownloadStream Server-Streaming (unbounded push)
 *
 * Run:  make run-signal          (terminal 1)
 *       make run-echo-server-ts  (terminal 2)
 *
 * Then open this page, accept the self-signed cert warning at
 * https://localhost:8443, and click "Start Listening". In another tab
 * open the echo client (make run-echo-ts) to issue RPCs.
 */

import { listen, type Listener } from "@peerrpc/peerrpc";
import { Server, ok, err, MethodKind, type ServiceDesc } from "@peerrpc/rpc";

const enc = new TextEncoder();
const dec = new TextDecoder();

const logEl = document.getElementById("log")!;
const listenBtn = document.getElementById("listen") as HTMLButtonElement;
const statusEl = document.getElementById("status")!;
const signalUrlEl = document.getElementById("signalUrl") as HTMLInputElement;
const serviceEl = document.getElementById("service") as HTMLInputElement;

let listener: Listener | null = null;

function log(msg: string): void {
  logEl.textContent += new Date().toLocaleTimeString() + " " + msg + "\n";
  logEl.scrollTop = logEl.scrollHeight;
}

function setStatus(text: string, cls: string): void {
  statusEl.textContent = text;
  statusEl.className = "status " + cls;
}

function registerEcho(srv: Server): void {
  const echoDesc: ServiceDesc = {
    serviceName: "echo.Echo",
    methods: [
      {
        method: "Echo",
        kind: MethodKind.Unary,
        handler: async (stream) => {
          const req = await stream.recv();
          if (!req) return err(13, "empty request");
          await stream.send(concat(enc.encode("echo: "), req));
          return ok();
        },
      },
      {
        method: "Stream",
        kind: MethodKind.ServerStreaming,
        handler: async (stream) => {
          const req = await stream.recv();
          const label = req ? dec.decode(req) : "";
          for (let i = 1; i <= 5; i++) {
            await stream.send(enc.encode(`chunk ${i} for "${label}"`));
          }
          return ok();
        },
      },
      {
        method: "Collect",
        kind: MethodKind.ClientStreaming,
        handler: async (stream) => {
          let n = 0;
          let total = 0;
          for (;;) {
            const msg = await stream.recv();
            if (msg === null) break;
            n++;
            total += msg.length;
          }
          await stream.send(enc.encode(`received ${n} messages (${total} bytes)`));
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
            await stream.send(enc.encode(`ack ${seq}: ${dec.decode(msg)}`));
          }
        },
      },
      {
        // LargeEcho echoes the request payload verbatim. A multi-MB
        // request exercises inbound reassembly (server recv) and
        // outbound chunking (server send). The client verifies
        // integrity. Bounded by the int32 single-message ceiling
        // (~2 GiB) and tab memory.
        method: "LargeEcho",
        kind: MethodKind.Unary,
        handler: async (stream) => {
          const req = await stream.recv();
          if (!req) return err(13, "empty request");
          await stream.send(req);
          return ok();
        },
      },
      {
        // LargeDownload generates and pushes a blob of the caller-chosen
        // size as a single message. The request carries the size as a
        // decimal byte count (e.g. "1048576" for 1 MiB), optional
        // K/KB/M/MB suffix; empty defaults to 1 MiB. The blob carries
        // a deterministic pattern the client verifies. Single-message,
        // so bounded by the int32 ceiling.
        method: "LargeDownload",
        kind: MethodKind.ServerStreaming,
        handler: async (stream) => {
          const req = await stream.recv();
          const parsed = parseDownloadSize(req);
          if (parsed === null) return err(3, "large-download: size must be a non-negative decimal byte count");
          const blob = makePattern(parsed);
          await stream.send(blob);
          return ok();
        },
      },
      {
        // LargeEchoStream is a bidi-streaming echo. The client sends an
        // arbitrary number of messages (each a chunk of a larger logical
        // payload); the server echoes each verbatim. Memory is constant
        // (one chunk in flight), so total transfer size is unbounded.
        // The client half-closes to end.
        method: "LargeEchoStream",
        kind: MethodKind.BidiStreaming,
        handler: async (stream) => {
          for (;;) {
            const msg = await stream.recv();
            if (msg === null) return ok();
            await stream.send(msg);
          }
        },
      },
      {
        // LargeDownloadStream generates and pushes a blob of the
        // caller-chosen size in 16 MiB chunks. Unlike LargeDownload
        // (single message, ~2 GiB ceiling), this streams as many
        // messages as needed, so the total size is unbounded. The
        // request carries the size (decimal bytes, optional
        // K/KB/M/MB/G/GB suffix).
        method: "LargeDownloadStream",
        kind: MethodKind.ServerStreaming,
        handler: async (stream) => {
          const req = await stream.recv();
          const size = parseStreamSize(req);
          if (size === null) return err(3, "large-download-stream: size must be a non-negative decimal byte count");
          const chunk = new Uint8Array(16 * 1024 * 1024); // 16 MiB, reused
          let sent = 0;
          while (sent < size) {
            const end = Math.min(sent + chunk.length, size);
            fillPattern(chunk.subarray(0, end - sent), sent);
            await stream.send(chunk.subarray(0, end - sent));
            sent = end;
          }
          return ok();
        },
      },
    ],
  };
  srv.registerService(echoDesc);
}

function concat(prefix: Uint8Array, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(prefix.length + body.length);
  out.set(prefix, 0);
  out.set(body, prefix.length);
  return out;
}

// ── Large-payload helpers (mirror the Go echo-server) ─────────────
//
// Deterministic byte(i % 251) pattern so clients can verify integrity
// without echoing the whole blob back. 251 is prime (long cycle).

// MAX_DOWNLOAD_BYTES caps the single-message LargeDownload blob. The
// wire Chunk.total_size is a signed int32 (max ~2.147 GiB); the cap is
// the largest int32-safe value. A tab will OOM well before this.
const MAX_DOWNLOAD_BYTES = 2147483647; // 2^31 - 1

// makePattern allocates a size-byte buffer filled with the pattern.
function makePattern(size: number): Uint8Array {
  const b = new Uint8Array(size);
  fillPattern(b, 0);
  return b;
}

// fillPattern fills dst with byte(i % 251) starting at global byte
// offset base, writing into an existing buffer (no allocation) so a
// chunk buffer can be reused across an unbounded stream.
function fillPattern(dst: Uint8Array, base: number): void {
  for (let i = 0; i < dst.length; i++) {
    dst[i] = (base + i) % 251;
  }
}

// parseDownloadSize interprets the request as a decimal byte count
// with an optional K/KB/M/MB suffix and clamps to
// [1, MAX_DOWNLOAD_BYTES]. null on parse error. Empty defaults to 1 MiB.
function parseDownloadSize(req: Uint8Array | null): number | null {
  const raw = (req ? dec.decode(req) : "").trim().toLowerCase();
  if (raw === "") return 1 << 20; // default 1 MiB
  let mul = 1;
  let n = raw;
  if (n.endsWith("mb")) { mul = 1 << 20; n = n.slice(0, -2); }
  else if (n.endsWith("m")) { mul = 1 << 20; n = n.slice(0, -1); }
  else if (n.endsWith("kb")) { mul = 1 << 10; n = n.slice(0, -2); }
  else if (n.endsWith("k")) { mul = 1 << 10; n = n.slice(0, -1); }
  const v = Number.parseInt(n.trim(), 10);
  if (!Number.isFinite(v) || v < 0) return null;
  let bytes = v * mul;
  if (bytes < 1) bytes = 1;
  if (bytes > MAX_DOWNLOAD_BYTES) bytes = MAX_DOWNLOAD_BYTES;
  return bytes;
}

// MAX_STREAM_BYTES caps the streaming blob. Streaming RPCs send many
// messages, so there is no int32 ceiling; the cap is a sanity bound.
const MAX_STREAM_BYTES = Number(1n << 60n); // 1 EiB

// parseStreamSize interprets the request as a decimal byte count with
// an optional K/KB/M/MB/G/GB suffix (int64-scale) and clamps to
// [1, MAX_STREAM_BYTES]. null on parse error. Empty defaults to 1 MiB.
function parseStreamSize(req: Uint8Array | null): number | null {
  const raw = (req ? dec.decode(req) : "").trim().toLowerCase();
  if (raw === "") return 1 << 20; // default 1 MiB
  let mul = 1;
  let n = raw;
  if (n.endsWith("gb")) { mul = 1 << 30; n = n.slice(0, -2); }
  else if (n.endsWith("g")) { mul = 1 << 30; n = n.slice(0, -1); }
  else if (n.endsWith("mb")) { mul = 1 << 20; n = n.slice(0, -2); }
  else if (n.endsWith("m")) { mul = 1 << 20; n = n.slice(0, -1); }
  else if (n.endsWith("kb")) { mul = 1 << 10; n = n.slice(0, -2); }
  else if (n.endsWith("k")) { mul = 1 << 10; n = n.slice(0, -1); }
  const v = Number.parseInt(n.trim(), 10);
  if (!Number.isFinite(v) || v < 0) return null;
  let bytes = v * mul;
  if (bytes < 1) bytes = 1;
  if (bytes > MAX_STREAM_BYTES) bytes = MAX_STREAM_BYTES;
  return bytes;
}

listenBtn.addEventListener("click", async () => {
  if (listener) {
    await listener.close();
    listener = null;
    listenBtn.textContent = "Start Listening";
    setStatus("stopped", "err");
    log("listener closed");
    return;
  }

  const signalUrl = signalUrlEl.value.replace(/\/$/, "");
  const service = serviceEl.value;
  // Browsers cannot do Connect bidi (fetch lacks streaming request
  // bodies), so use the WebSocket signaling endpoint (wss://host/ws).
  const target = `peerrpc+ws://${new URL(signalUrl).host}/${service}`;

  log(`listening on ${target} ...`);
  setStatus("listening...", "");

  try {
    listener = await listen(target);
  } catch (e) {
    log(`listen failed: ${e}`);
    setStatus("failed", "err");
    return;
  }

  listenBtn.textContent = "Stop Listening";
  setStatus("listening", "ok");
  log("listening; waiting for clients to connect");

  // Serve accepts connections sequentially; each gets a fresh Server
  // with the echo service registered.
  listener.serve(() => {
    const srv = new Server();
    registerEcho(srv);
    return srv;
  }).catch((e) => {
    log(`serve loop exited: ${e}`);
    setStatus("stopped", "err");
  });
});

log("ready. Click Start Listening to register as the echo server.");
