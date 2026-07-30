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

const enc = new TextEncoder();
const dec = new TextDecoder();

const logEl = document.getElementById("log")!;
const connectBtn = document.getElementById("connect") as HTMLButtonElement;
const unaryBtn = document.getElementById("unary") as HTMLButtonElement;
const streamBtn = document.getElementById("stream") as HTMLButtonElement;
const collectBtn = document.getElementById("collect") as HTMLButtonElement;
const chatBtn = document.getElementById("chat") as HTMLButtonElement;
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

log("ready. Click Connect to dial the signal-server.");
