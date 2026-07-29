import { Peer } from "@peerrpc/peer";
import type { SignalMessage } from "@peerrpc/peer";
import { createSignal } from "../signal.js";
import { Server, ok, err, type Status } from "./rpc-server.js";

const $ = <T extends HTMLElement>(id: string): T =>
  document.getElementById(id) as T;

const statusEl = $<HTMLDivElement>("status");
const roomInput = $<HTMLInputElement>("room-input");
const startBtn = $<HTMLButtonElement>("start-btn");
const clientListEl = $<HTMLUListElement>("client-list");

function setStatus(text: string, cls: string): void {
  statusEl.textContent = text;
  statusEl.className = `status ${cls}`;
}

interface ClientConn {
  peerId: string;
  peer: Peer;
  remoteDescSet: boolean;
  pendingCandidates: RTCIceCandidateInit[];
}

const clients = new Map<string, ClientConn>();

function updateClientList(): void {
  const ids = [...clients.keys()];
  if (ids.length === 0) {
    clientListEl.innerHTML = "<li>None</li>";
    return;
  }
  clientListEl.innerHTML = ids
    .map((id) => `<li>${id.slice(0, 8)}…</li>`)
    .join("");
}

async function handleOffer(sig: ReturnType<typeof createSignal>, msg: SignalMessage): Promise<void> {
  const fromId = msg.from!;
  if (clients.has(fromId)) return;

  const peer = new Peer({ negotiationTimeout: 30_000 });
  const conn: ClientConn = {
    peerId: fromId,
    peer,
    remoteDescSet: false,
    pendingCandidates: [],
  };
  clients.set(fromId, conn);
  updateClientList();

  peer.onIceCandidate((c) => {
    if (c) {
      sig.send({
        type: "candidate" as const,
        candidate: c.candidate ?? "",
        sdpMid: c.sdpMid ?? "",
        sdpMLineIndex: c.sdpMLineIndex ?? 0,
        to: fromId,
      });
    }
  });

  peer.acceptOffer(msg.sdp!).then((answerSdp) => {
    conn.remoteDescSet = true;
    for (const c of conn.pendingCandidates) {
      peer.addCandidate(c);
    }
    sig.send({ type: "answer" as const, sdp: answerSdp, to: fromId });
  }).catch((err) => {
    console.error("[server] acceptOffer failed for", fromId, err);
    clients.delete(fromId);
    updateClientList();
  });

  peer.waitForChannel().then((ch) => {
    ch.setDecodeMode("request");

    const srv = new Server();
    srv.register("/chat.Chat/Echo", handleEcho);
    srv.register("/chat.Chat/Reverse", handleReverse);
    srv.register("/chat.Chat/Stream", handleStream);

    ch.onClose(() => {
      console.log("[server] client disconnected:", fromId);
      clients.delete(fromId);
      updateClientList();
    });

    srv.serve(ch);
    console.log("[server] serving RPCs for client:", fromId);
  }).catch((err) => {
    console.error("[server] waitForChannel failed for", fromId, err);
    clients.delete(fromId);
    updateClientList();
  });
}

function handleCandidate(msg: SignalMessage): void {
  const fromId = msg.from!;
  const conn = clients.get(fromId);
  if (!conn) return;

  const c: RTCIceCandidateInit = {
    candidate: msg.candidate ?? "",
    sdpMid: msg.sdpMid ?? null,
    sdpMLineIndex: msg.sdpMLineIndex ?? null,
  };
  if (conn.remoteDescSet) {
    conn.peer.addCandidate(c);
  } else {
    conn.pendingCandidates.push(c);
  }
}

async function handleEcho(stream: import("./rpc-server.js").ServerStream): Promise<Status> {
  const req = await stream.recv();
  if (!req) return err(13, "empty request");
  const msg = new TextDecoder().decode(req);
  await stream.send(new TextEncoder().encode(`echo: ${msg}`));
  return ok();
}

async function handleReverse(stream: import("./rpc-server.js").ServerStream): Promise<Status> {
  const req = await stream.recv();
  if (!req) return err(13, "empty request");
  const msg = new TextDecoder().decode(req);
  await stream.send(new TextEncoder().encode(msg.split("").reverse().join("")));
  return ok();
}

async function handleStream(stream: import("./rpc-server.js").ServerStream): Promise<Status> {
  const req = await stream.recv();
  if (!req) return err(13, "empty request");
  const msg = new TextDecoder().decode(req);
  for (let i = 1; i <= 5; i++) {
    await stream.send(new TextEncoder().encode(`chunk ${i} for "${msg}"`));
  }
  return ok();
}

startBtn.addEventListener("click", async () => {
  const roomId = roomInput.value.trim();
  if (!roomId) {
    setStatus("Enter a room ID", "error");
    return;
  }

  startBtn.disabled = true;
  roomInput.disabled = true;
  setStatus("Starting server...", "listening");

  const sig = createSignal(
    window.location.origin,
    roomId,
    crypto.randomUUID(),
    2,
  );

  sig.onMessage((msg) => {
    switch (msg.type) {
      case "offer":
        handleOffer(sig, msg);
        break;
      case "candidate":
        handleCandidate(msg);
        break;
    }
  });

  try {
    await sig.connect();
    setStatus("Listening for clients...", "listening");
  } catch (err) {
    setStatus(`Error: ${err}`, "error");
    startBtn.disabled = false;
    roomInput.disabled = false;
  }
});
