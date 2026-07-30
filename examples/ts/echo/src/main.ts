/**
 * PeerRPC browser echo demo.
 *
 * Connects to a signal-server over WebSocket and exercises
 * all four RPC types (Unary / Server-Streaming / Client-Streaming /
 * Bidi) against the echo.Echo service registered by the Go interop
 * server or any compatible PeerRPC server.
 *
 * Run:  make run-signal   (terminal 1)
 *       make run-echo-ts  (terminal 2)
 *
 * Then open the printed Vite URL, accept the self-signed cert warning
 * for the signal-server (https://localhost:8443), and click Connect.
 */

import { dial, type Conn } from "@peerrpc/peerrpc";
import { CHUNK_SIZE } from "@peerrpc/protocol";

const enc = new TextEncoder();
const dec = new TextDecoder();

const logEl = document.getElementById("log")!;
const connectBtn = document.getElementById("connect") as HTMLButtonElement;
const unaryBtn = document.getElementById("unary") as HTMLButtonElement;
const streamBtn = document.getElementById("stream") as HTMLButtonElement;
const collectBtn = document.getElementById("collect") as HTMLButtonElement;
const chatBtn = document.getElementById("chat") as HTMLButtonElement;
const largeEchoBtn = document.getElementById("largeEcho") as HTMLButtonElement;
const largeDownloadBtn = document.getElementById("largeDownload") as HTMLButtonElement;
const largeEchoStreamBtn = document.getElementById("largeEchoStream") as HTMLButtonElement;
const largeDownloadStreamBtn = document.getElementById("largeDownloadStream") as HTMLButtonElement;
const payloadSizeEl = document.getElementById("payloadSize") as HTMLSelectElement;
const upBar = document.getElementById("upBar") as HTMLProgressElement;
const dlBar = document.getElementById("dlBar") as HTMLProgressElement;
const upStatus = document.getElementById("upStatus")!;
const dlStatus = document.getElementById("dlStatus")!;
const signalUrlEl = document.getElementById("signalUrl") as HTMLInputElement;
const serviceEl = document.getElementById("service") as HTMLInputElement;
const requestEl = document.getElementById("request") as HTMLInputElement;

let conn: Conn | null = null;

function log(msg: string): void {
  logEl.textContent += msg + "\n";
  logEl.scrollTop = logEl.scrollHeight;
}

function setRpcEnabled(enabled: boolean): void {
  unaryBtn.disabled = !enabled;
  streamBtn.disabled = !enabled;
  collectBtn.disabled = !enabled;
  chatBtn.disabled = !enabled;
  largeEchoBtn.disabled = !enabled;
  largeDownloadBtn.disabled = !enabled;
  largeEchoStreamBtn.disabled = !enabled;
  largeDownloadStreamBtn.disabled = !enabled;
}

// ── Large-payload helpers ──────────────────────────────────────────
//
// The wire protocol auto-splits payloads > 256 KiB into 256 KiB chunks
// (transparent to the caller; the peer reassembles them). These helpers
// build a deterministic payload so the client can verify a large
// round-trip without comparing the whole blob byte-for-byte in the UI.

// CHUNK_SIZE is imported from @peerrpc/protocol (the transport-layer
// chunk threshold) so the displayed chunk count matches what actually
// goes on the wire.

// STREAM_CHUNK is the logical message size the streaming transfers send
// per RPC message. Each message is itself auto-split into 256 KiB wire
// frames; keeping messages at 16 MiB balances frame overhead against
// per-message reassembly memory. Streaming sends MANY messages, so the
// total transfer size is unbounded (no int32 ceiling).
const STREAM_CHUNK = 16 * 1024 * 1024;

// payloadSizeBytes resolves the selected value to a byte count. Values
// are either a plain MB count ("16") or a GB form ("64gb"). Streaming
// transfers handle arbitrary sizes (no clamp); the single-message path
// clamps via MAX_PAYLOAD_BYTES in its own handler.
function payloadSizeBytes(): number {
  const v = payloadSizeEl.value;
  if (v.endsWith("gb")) {
    return Math.max(1, Number(v.slice(0, -2))) * 1024 * 1024 * 1024;
  }
  return Math.max(1, Number(v)) * 1024 * 1024;
}

// Hard ceiling for the SINGLE-MESSAGE path only: the wire
// Chunk.total_size field is a signed int32 (max ~2.147 GiB) and the tab
// cannot allocate that much. Streaming transfers are unbounded.
const MAX_PAYLOAD_BYTES = 1024 * 1024 * 1024; // 1 GiB

// makePattern builds a size-byte payload filled with byte(i % 251),
// matching the server's makePattern so both sides agree on the pattern.
// Throws RangeError if the allocation fails (the tab ran out of memory).
function makePattern(size: number): Uint8Array {
  const b = new Uint8Array(size);
  for (let i = 0; i < size; i++) b[i] = i % 251;
  return b;
}

// makePatternChunk fills ONE chunk at the given global byte offset with
// the byte(i % 251) pattern, matching the server's fillPattern. Used by
// the streaming path so the whole blob is never materialized.
function makePatternChunk(globalOffset: number, chunkBytes: number): Uint8Array {
  const b = new Uint8Array(chunkBytes);
  for (let i = 0; i < chunkBytes; i++) b[i] = (globalOffset + i) % 251;
  return b;
}

// verifyPatternChunk checks one chunk against the byte(i % 251) pattern
// at the given global offset.
function verifyPatternChunk(globalOffset: number, buf: Uint8Array): boolean {
  for (let i = 0; i < buf.length; i++) {
    if (buf[i] !== (globalOffset + i) % 251) return false;
  }
  return true;
}

// verifyPattern checks a buffer carries the expected byte(i % 251)
// pattern over its full length.
function verifyPattern(buf: Uint8Array): boolean {
  for (let i = 0; i < buf.length; i++) {
    if (buf[i] !== i % 251) return false;
  }
  return true;
}

// formatBytes renders a byte count as a human-readable string, scaling
// to GiB/TiB for very large values.
function formatBytes(n: number): string {
  const units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
  let v = n;
  let u = 0;
  while (v >= 1024 && u < units.length - 1) {
    v /= 1024;
    u++;
  }
  const scaled = u === 0 ? `${v}` : v.toFixed(2);
  return `${scaled} ${units[u]} (${n} bytes)`;
}

// formatThroughput renders MiB/s for a byte count over a duration (ms).
function formatThroughput(bytes: number, ms: number): string {
  if (ms <= 0) return "n/a";
  return `${((bytes / (1024 * 1024)) / (ms / 1000)).toFixed(2)} MiB/s`;
}

// chunkCount reports how many 256 KiB frames a payload of n bytes splits into.
function chunkCount(n: number): number {
  return Math.max(1, Math.ceil(n / CHUNK_SIZE));
}

// ── Progress-bar helpers ───────────────────────────────────────────

// resetProgress clears both bars to an idle "0%" state. The progress
// row itself is always visible.
function resetProgress(): void {
  setProgress(upBar, upStatus, 0, 1);
  setProgress(dlBar, dlStatus, 0, 1);
}

// setProgress updates a <progress> bar and its status text.
function setProgress(
  bar: HTMLProgressElement,
  status: HTMLElement,
  done: number,
  total: number,
): void {
  const pct = total > 0 ? Math.min(100, (done / total) * 100) : 0;
  bar.value = pct;
  bar.max = 100;
  status.textContent = `${formatBytes(done)} / ${formatBytes(total)} (${pct.toFixed(0)}%)`;
}

connectBtn.addEventListener("click", async () => {
  if (conn) {
    await conn.close();
    conn = null;
    connectBtn.textContent = "Connect";
    setRpcEnabled(false);
    log("disconnected");
    return;
  }
  const signalUrl = signalUrlEl.value.replace(/\/$/, "");
  const service = serviceEl.value;
  // Browsers cannot do Connect bidi (fetch lacks streaming request
  // bodies), so use the WebSocket signaling endpoint (wss://host/ws).
  const target = `peerrpc+ws://${new URL(signalUrl).host}/${service}`;
  log(`dialing ${target} ...`);
  try {
    conn = await dial(target);
    log(`connected as ${conn.peerId}`);
    connectBtn.textContent = "Disconnect";
    setRpcEnabled(true);
  } catch (err) {
    log(`dial failed: ${err}`);
  }
});

// Unary: one request, one response.
unaryBtn.addEventListener("click", async () => {
  if (!conn) return;
  const req = enc.encode(requestEl.value);
  log(`Unary /echo.Echo/Echo: "${requestEl.value}"`);
  const { response, status } = await conn.client.invokeUnary("/echo.Echo/Echo", req);
  if (status.code !== 0) {
    log(`  error: code=${status.code} msg=${status.message}`);
    return;
  }
  log(`  response: "${dec.decode(response)}"`);
});

// Server streaming: one request, many responses.
streamBtn.addEventListener("click", async () => {
  if (!conn) return;
  const req = enc.encode(requestEl.value);
  log(`ServerStream /echo.Echo/Stream: "${requestEl.value}"`);
  const stream = await conn.client.invokeServerStreaming("/echo.Echo/Stream", req);
  for (;;) {
    const chunk = await stream.recv();
    if (chunk === null) break;
    log(`  chunk: "${dec.decode(chunk)}"`);
  }
  const status = stream.status;
  if (status && status.code !== 0) {
    log(`  ended with error: code=${status.code}`);
  }
});

// Client streaming: many requests, one response.
collectBtn.addEventListener("click", async () => {
  if (!conn) return;
  log(`ClientStream /echo.Echo/Collect (3 messages)`);
  const stream = await conn.client.invokeClientStreaming("/echo.Echo/Collect");
  for (const msg of ["first", "second", "third"]) {
    await stream.send(enc.encode(msg));
    log(`  sent: "${msg}"`);
  }
  await stream.closeSend();
  const resp = await stream.recv();
  if (resp === null) {
    log("  no response");
  } else {
    log(`  response: "${dec.decode(resp)}"`);
  }
});

// Bidi streaming: interleaved send/recv.
chatBtn.addEventListener("click", async () => {
  if (!conn) return;
  log(`Bidi /echo.Echo/Chat (3 round-trips)`);
  const stream = await conn.client.invokeBidiStreaming("/echo.Echo/Chat");
  for (let i = 1; i <= 3; i++) {
    const msg = `msg-${i}`;
    await stream.send(enc.encode(msg));
    log(`  sent: "${msg}"`);
    const resp = await stream.recv();
    if (resp === null) {
      log("  stream ended early");
      break;
    }
    log(`  recv: "${dec.decode(resp)}"`);
  }
    await stream.closeSend();
    // Drain any trailing EOF.
    await stream.recv();
  });

// Large echo: upload a multi-MB payload, server echoes it verbatim.
// Exercises both directions' chunk split + reassembly. Shows upload +
// download progress bars driven by the per-chunk callbacks.
largeEchoBtn.addEventListener("click", async () => {
  if (!conn) return;
  // Single-message path is bounded by the int32 wire field (~2 GiB)
  // and tab memory; clamp to MAX_PAYLOAD_BYTES and warn if reduced.
  let size = payloadSizeBytes();
  if (size > MAX_PAYLOAD_BYTES) {
    log(`  note: single-message capped at ${formatBytes(MAX_PAYLOAD_BYTES)} (use Stream for larger)`);
    size = MAX_PAYLOAD_BYTES;
  }
  log(`Large Echo /echo.Echo/LargeEcho: sending ${formatBytes(size)} ...`);
  let payload: Uint8Array;
  try {
    payload = makePattern(size);
  } catch (err) {
    log(`  allocation failed (payload too large for this tab): ${err}`);
    return;
  }
  resetProgress();
  const t0 = performance.now();
  let result;
  try {
    result = await conn.client.invokeUnary("/echo.Echo/LargeEcho", payload, undefined, {
      onUploadProgress: (sent, total) => setProgress(upBar, upStatus, sent, total),
      onDownloadProgress: (received, total) => setProgress(dlBar, dlStatus, received, total),
    });
  } catch (err) {
    log(`  failed: ${err}`);
    resetProgress();
    return;
  }
  const ms = performance.now() - t0;
  const { response, status } = result;
  if (status.code !== 0) {
    resetProgress();
    log(`  error: code=${status.code} msg=${status.message}`);
    return;
  }
  log(`  received ${formatBytes(response.length)} in ${ms.toFixed(0)} ms (${formatThroughput(size, ms)})`);
  if (response.length !== size) {
    log(`  length mismatch: expected ${size}, got ${response.length}`);
    return;
  }
  log(`  integrity: ${verifyPattern(response) ? "PASSED \u2713" : "FAILED \u2717"}`);
  log(`  chunked into ${chunkCount(size)} \u00d7 256 KiB frames`);
});

// Large download: client requests a size, server generates + pushes a
// blob of that size. Exercises server-side chunk split (outbound) and
// shows the download progress bar.
largeDownloadBtn.addEventListener("click", async () => {
  if (!conn) return;
  const size = payloadSizeBytes();
  log(`Large Download /echo.Echo/LargeDownload: requesting ${formatBytes(size)} ...`);
  // Upload is tiny (just the size request); mark it done, track download.
  resetProgress();
  upStatus.textContent = "request sent";
  upBar.value = 100;
  const stream = await conn.client.invokeServerStreaming(
    "/echo.Echo/LargeDownload",
    enc.encode(String(size)),
    undefined,
    {
      onDownloadProgress: (received, total) => setProgress(dlBar, dlStatus, received, total),
    },
  );
  const t0 = performance.now();
  const blob = await stream.recv();
  const ms = performance.now() - t0;
  if (blob === null) {
    log("  no data received");
    const st = stream.status;
    if (st && st.code !== 0) log(`  ended with error: code=${st.code}`);
    resetProgress();
    return;
  }
  log(`  received ${formatBytes(blob.length)} in ${ms.toFixed(0)} ms (${formatThroughput(blob.length, ms)})`);
  log(`  integrity: ${verifyPattern(blob) ? "PASSED \u2713" : "FAILED \u2717"}`);
  log(`  chunked into ${chunkCount(blob.length)} \u00d7 256 KiB frames`);
});

// Large echo (stream): bidi-streaming echo of an unbounded payload.
// The client streams 16 MiB chunks (generated per-chunk, never holding
// the whole blob), the server echoes each, and the client verifies each
// chunk against the global pattern. Total size is limited only by a
// JS number counter (safe to 2^53), so 64 GB+ is supported. Memory is
// constant (a few chunks in flight).
largeEchoStreamBtn.addEventListener("click", async () => {
  if (!conn) return;
  const size = payloadSizeBytes();
  log(`Large Echo Stream /echo.Echo/LargeEchoStream: ${formatBytes(size)} (unbounded) ...`);
  resetProgress();
  const t0 = performance.now();
  const stream = await conn.client.invokeBidiStreaming("/echo.Echo/LargeEchoStream");
  let sent = 0;
  let received = 0;
  let chunks = 0;
  let ok = true;
  try {
    while (sent < size) {
      const n = Math.min(STREAM_CHUNK, size - sent);
      const chunk = makePatternChunk(sent, n);
      await stream.send(chunk);
      sent += n;
      setProgress(upBar, upStatus, sent, size);
      // Receive the echo for this chunk (bidi: one response per send).
      const echo = await stream.recv();
      if (echo === null) {
        log("  stream ended early");
        break;
      }
      if (!verifyPatternChunk(received, echo)) ok = false;
      received += echo.length;
      chunks++;
      setProgress(dlBar, dlStatus, received, size);
    }
    await stream.closeSend();
    await stream.recv(); // drain trailing EOF
  } catch (err) {
    log(`  failed: ${err}`);
    resetProgress();
    return;
  }
  const ms = performance.now() - t0;
  log(`  received ${formatBytes(received)} in ${ms.toFixed(0)} ms (${formatThroughput(received, ms)})`);
  log(`  integrity: ${ok ? "PASSED \u2713" : "FAILED \u2717"} (${chunks} chunks)`);
});

// Large download (stream): server-streaming push of an unbounded blob.
// The server streams 16 MiB chunks; the client receives + verifies each
// against the global pattern, summing bytes in a number. Total size is
// unbounded (e.g. 64 GB); memory is constant.
largeDownloadStreamBtn.addEventListener("click", async () => {
  if (!conn) return;
  const size = payloadSizeBytes();
  log(`Large Download Stream /echo.Echo/LargeDownloadStream: requesting ${formatBytes(size)} (unbounded) ...`);
  resetProgress();
  upStatus.textContent = "request sent";
  upBar.value = 100;
  const t0 = performance.now();
  const stream = await conn.client.invokeServerStreaming(
    "/echo.Echo/LargeDownloadStream",
    enc.encode(String(size)),
  );
  let received = 0;
  let chunks = 0;
  let ok = true;
  try {
    for (;;) {
      const chunk = await stream.recv();
      if (chunk === null) break;
      if (!verifyPatternChunk(received, chunk)) ok = false;
      received += chunk.length;
      chunks++;
      setProgress(dlBar, dlStatus, received, size);
    }
  } catch (err) {
    log(`  failed: ${err}`);
    resetProgress();
    return;
  }
  const ms = performance.now() - t0;
  log(`  received ${formatBytes(received)} in ${ms.toFixed(0)} ms (${formatThroughput(received, ms)})`);
  log(`  integrity: ${ok ? "PASSED \u2713" : "FAILED \u2717"} (${chunks} chunks)`);
});

log("ready. Click Connect to dial the signal-server.");
