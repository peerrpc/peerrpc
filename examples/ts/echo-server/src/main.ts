/**
 * PeerRPC browser echo server.
 *
 * Acts as a PeerRPC server (Answerer) behind a signal-server and
 * serves the echo.Echo service with all four RPC types:
 *
 *   /echo.Echo/Echo     Unary
 *   /echo.Echo/Stream   Server-Streaming
 *   /echo.Echo/Collect  Client-Streaming
 *   /echo.Echo/Chat     Bidi-Streaming
 *
 * Run:  make run-signal         (terminal 1)
 *       make run-ts-echo-server (terminal 2)
 *
 * Then open this page, accept the self-signed cert warning at
 * https://localhost:8443, and click "Start Listening". In another tab
 * open the echo client (make run-ts-echo) to issue RPCs.
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
