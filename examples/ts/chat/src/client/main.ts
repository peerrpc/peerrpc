import { Peer } from "@peerrpc/peer";
import type { SignalMessage } from "@peerrpc/peer";
import { Client } from "@peerrpc/rpc";
import type { Channel } from "@peerrpc/transport";
import { createSignal } from "../signal.js";

const $ = <T extends HTMLElement>(id: string): T =>
  document.getElementById(id) as T;

const statusEl = $<HTMLDivElement>("status");
const roomInput = $<HTMLInputElement>("room-input");
const connectBtn = $<HTMLButtonElement>("connect-btn");
const rpcSection = $<HTMLDivElement>("rpc-section");
const echoBtn = $<HTMLButtonElement>("echo-btn");
const echoInput = $<HTMLInputElement>("echo-input");
const echoOutput = $<HTMLPreElement>("echo-output");
const reverseBtn = $<HTMLButtonElement>("reverse-btn");
const reverseInput = $<HTMLInputElement>("reverse-input");
const reverseOutput = $<HTMLPreElement>("reverse-output");
const streamBtn = $<HTMLButtonElement>("stream-btn");
const streamInput = $<HTMLInputElement>("stream-input");
const streamOutput = $<HTMLPreElement>("stream-output");

function setStatus(text: string, cls: string): void {
  statusEl.textContent = text;
  statusEl.className = `status ${cls}`;
}

async function connectAsClient(roomId: string): Promise<Channel> {
  const peerId = crypto.randomUUID();
  const peer = new Peer();
  let remoteDescSet = false;
  let answerApplied = false;
  const pendingCandidates: RTCIceCandidateInit[] = [];

  const sig = createSignal(window.location.origin, roomId, peerId, 1);

  sig.onMessage((msg: SignalMessage) => {
    if (msg.to && msg.to !== peerId) return;

    switch (msg.type) {
      case "answer": {
        if (answerApplied) return;
        answerApplied = true;
        peer.acceptAnswer(msg.sdp!).then(() => {
          remoteDescSet = true;
          for (const c of pendingCandidates) {
            peer.addCandidate(c);
          }
        }).catch((err) => console.error("[client] acceptAnswer failed", err));
        break;
      }
      case "candidate": {
        const c: RTCIceCandidateInit = {
          candidate: msg.candidate ?? "",
          sdpMid: msg.sdpMid ?? null,
          sdpMLineIndex: msg.sdpMLineIndex ?? null,
        };
        if (remoteDescSet) {
          peer.addCandidate(c);
        } else {
          pendingCandidates.push(c);
        }
        break;
      }
    }
  });

  peer.onIceCandidate((c) => {
    if (c) {
      sig.send({
        type: "candidate" as const,
        candidate: c.candidate ?? "",
        sdpMid: c.sdpMid ?? "",
        sdpMLineIndex: c.sdpMLineIndex ?? 0,
      });
    }
  });

  await sig.connect();

  const offerSdp = await peer.createOffer();
  sig.send({ type: "offer" as const, sdp: offerSdp });

  return peer.waitForChannel();
}

let client: Client | null = null;

connectBtn.addEventListener("click", async () => {
  const roomId = roomInput.value.trim();
  if (!roomId) {
    setStatus("Enter a room ID", "error");
    return;
  }

  connectBtn.disabled = true;
  roomInput.disabled = true;
  setStatus("Connecting...", "connecting");

  try {
    const ch = await connectAsClient(roomId);
    client = new Client(ch);
    setStatus("Connected", "connected");
    rpcSection.style.display = "block";
  } catch (err) {
    setStatus(`Error: ${err}`, "error");
    connectBtn.disabled = false;
    roomInput.disabled = false;
  }
});

echoBtn.addEventListener("click", async () => {
  if (!client) return;
  echoBtn.disabled = true;
  try {
    const { response, status } = await client.invokeUnary(
      "/chat.Chat/Echo",
      new TextEncoder().encode(echoInput.value),
    );
    if (status.code === 0) {
      echoOutput.textContent = new TextDecoder().decode(response);
    } else {
      echoOutput.textContent = `Error: ${status.code} ${status.message}`;
    }
  } catch (err) {
    echoOutput.textContent = `Exception: ${err}`;
  }
  echoBtn.disabled = false;
});

reverseBtn.addEventListener("click", async () => {
  if (!client) return;
  reverseBtn.disabled = true;
  try {
    const { response, status } = await client.invokeUnary(
      "/chat.Chat/Reverse",
      new TextEncoder().encode(reverseInput.value),
    );
    if (status.code === 0) {
      reverseOutput.textContent = new TextDecoder().decode(response);
    } else {
      reverseOutput.textContent = `Error: ${status.code} ${status.message}`;
    }
  } catch (err) {
    reverseOutput.textContent = `Exception: ${err}`;
  }
  reverseBtn.disabled = false;
});

streamBtn.addEventListener("click", async () => {
  if (!client) return;
  streamBtn.disabled = true;
  streamOutput.textContent = "Receiving chunks...";
  try {
    const stream = await client.invokeServerStreaming(
      "/chat.Chat/Stream",
      new TextEncoder().encode(streamInput.value),
    );
    const lines: string[] = [];
    for (;;) {
      const chunk = await stream.recv();
      if (chunk === null) break;
      lines.push(new TextDecoder().decode(chunk));
    }
    streamOutput.textContent = lines.join("\n");
  } catch (err) {
    streamOutput.textContent = `Exception: ${err}`;
  }
  streamBtn.disabled = false;
});
