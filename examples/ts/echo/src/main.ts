/**
 * Browser echo demo: TypeScript client calling a Go PeerRPC server
 * over WebRTC.
 *
 * Signaling uses SSE (EventSource) for server→browser and POST for
 * browser→server. This works over HTTP/1.1, no TLS needed.
 *
 * Flow:
 *   1. Browser subscribes to GET /api/signal/events?room=interop (SSE).
 *   2. Go server (Offerer) creates a DataChannel + SDP offer; it
 *      arrives via SSE.
 *   3. Browser creates an RTCPeerConnection, applies the offer,
 *      generates an answer, POSTs it to /api/signal/send?room=interop.
 *   4. ICE candidates flow bidirectionally via SSE + POST.
 *   5. On DataChannel open, the browser issues Unary + Server
 *      Streaming RPCs.
 */

import { Peer } from "@peerrpc/peer";
import { Client, Code } from "@peerrpc/rpc";

// --- DOM helpers --------------------------------------------------

const $ = <T extends HTMLElement>(id: string): T =>
  document.getElementById(id) as T;

const statusEl = $<HTMLDivElement>("status");
const rpcSection = $<HTMLDivElement>("rpc-section");

function setStatus(text: string, cls: string): void {
  statusEl.textContent = text;
  statusEl.className = `status ${cls}`;
}

// --- Signaling types -----------------------------------------------

interface SignalMsg {
  type: "offer" | "answer" | "candidate";
  sdp?: string;
  candidate?: string;
  sdpMid?: string;
  sdpMLineIndex?: number;
}

const ROOM = "interop";

/** Send a signaling message to the Go server via POST. */
async function signalSend(msg: SignalMsg): Promise<void> {
  await fetch(`/api/signal/send?room=${ROOM}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(msg),
  });
}

/** Subscribe to SSE for signaling messages from the Go server. */
function signalSubscribe(onMsg: (msg: SignalMsg) => void): EventSource {
  const es = new EventSource(`/api/signal/events?room=${ROOM}`);
  es.onmessage = (ev) => {
    try {
      const msg = JSON.parse(ev.data) as SignalMsg;
      onMsg(msg);
    } catch {
      // ignore parse errors
    }
  };
  return es;
}

// --- Main flow -----------------------------------------------------

const connectBtn = $<HTMLButtonElement>("connect-btn");

let client: Client | null = null;

connectBtn.addEventListener("click", async () => {
  connectBtn.disabled = true;
  setStatus("Connecting...", "connecting");

  try {
    const dc = await connectV2();
    const { Channel } = await import("@peerrpc/transport");
    const transport = new Channel(dc);
    client = new Client(transport);
    setStatus("Connected", "connected");
    rpcSection.style.display = "block";
    wireButtons();
  } catch (err) {
    setStatus(`Error: ${err}`, "error");
    connectBtn.disabled = false;
  }
});

async function connectV2(): Promise<RTCDataChannel> {
  const pc = new RTCPeerConnection();
  const pendingCandidates: RTCIceCandidateInit[] = [];
  let remoteDescSet = false;

  return new Promise<RTCDataChannel>((resolve, reject) => {
    const timeout = setTimeout(
      () => reject(new Error("DataChannel timeout")),
      30000
    );

    const es = signalSubscribe((msg) => {
      switch (msg.type) {
        case "offer":
          pc.setRemoteDescription({ type: "offer", sdp: msg.sdp! })
            .then(async () => {
              remoteDescSet = true;
              for (const c of pendingCandidates) {
                await pc.addIceCandidate(c);
              }
              const answer = await pc.createAnswer();
              await pc.setLocalDescription(answer);
              await waitForIceGathering(pc);
              await signalSend({
                type: "answer",
                sdp: pc.localDescription!.sdp,
              });
            })
            .catch(reject);
          break;
        case "candidate": {
          const c: RTCIceCandidateInit = {
            candidate: msg.candidate!,
            sdpMid: msg.sdpMid ?? null,
            sdpMLineIndex: msg.sdpMLineIndex ?? null,
          };
          if (remoteDescSet) {
            pc.addIceCandidate(c).catch(() => {});
          } else {
            pendingCandidates.push(c);
          }
          break;
        }
      }
    });

    pc.onicecandidate = (ev) => {
      if (ev.candidate) {
        signalSend({
          type: "candidate",
          candidate: ev.candidate.candidate,
          sdpMid: ev.candidate.sdpMid ?? "",
          sdpMLineIndex: ev.candidate.sdpMLineIndex ?? 0,
        }).catch(() => {});
      }
    };

    pc.ondatachannel = (ev) => {
      ev.channel.onopen = () => {
        clearTimeout(timeout);
        es.close();
        resolve(ev.channel);
      };
    };
  });
}

function waitForIceGathering(pc: RTCPeerConnection): Promise<void> {
  if (pc.iceGatheringState === "complete") return Promise.resolve();
  return new Promise((resolve) => {
    const check = () => {
      if (pc.iceGatheringState === "complete") {
        pc.removeEventListener("icegatheringstatechange", check);
        resolve();
      }
    };
    pc.addEventListener("icegatheringstatechange", check);
    setTimeout(resolve, 5000);
  });
}

function wireButtons(): void {
  const unaryBtn = $<HTMLButtonElement>("unary-btn");
  const unaryInput = $<HTMLInputElement>("unary-input");
  const unaryOutput = $<HTMLPreElement>("unary-output");

  unaryBtn.addEventListener("click", async () => {
    if (!client) {
      unaryOutput.textContent = "Not connected";
      return;
    }
    unaryBtn.disabled = true;
    try {
      const { response, status } = await client.invokeUnary(
        "/echo.Echo/Echo",
        new TextEncoder().encode(unaryInput.value)
      );
      if (status.code === 0) {
        unaryOutput.textContent = `Response: ${new TextDecoder().decode(response)}`;
      } else {
        unaryOutput.textContent = `Error: ${status.code} ${status.message}`;
      }
    } catch (err) {
      unaryOutput.textContent = `Exception: ${err}`;
    }
    unaryBtn.disabled = false;
  });

  const streamBtn = $<HTMLButtonElement>("stream-btn");
  const streamInput = $<HTMLInputElement>("stream-input");
  const streamOutput = $<HTMLPreElement>("stream-output");

  streamBtn.addEventListener("click", async () => {
    if (!client) {
      streamOutput.textContent = "Not connected";
      return;
    }
    streamBtn.disabled = true;
    streamOutput.textContent = "Receiving chunks...";
    try {
      const stream = await client.invokeServerStreaming(
        "/echo.Echo/Stream",
        new TextEncoder().encode(streamInput.value)
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

  // --- Client Streaming (Collect) ----------------------------------
  const collectBtn = $<HTMLButtonElement>("collect-btn");
  const collectInput = $<HTMLInputElement>("collect-input");
  const collectOutput = $<HTMLPreElement>("collect-output");

  collectBtn.addEventListener("click", async () => {
    if (!client) {
      collectOutput.textContent = "Not connected";
      return;
    }
    collectBtn.disabled = true;
    collectOutput.textContent = "Sending chunks...";
    try {
      const payload = new TextEncoder().encode(collectInput.value);
      const stream = await client.invokeClientStreaming(
        "/echo.Echo/Collect",
        payload,
      );
      // Send two more chunks after the first (inlined in Call).
      await stream.send(new TextEncoder().encode("chunk-2"));
      await stream.send(new TextEncoder().encode("chunk-3"));
      await stream.closeSend();
      const resp = await stream.recv();
      if (resp) {
        collectOutput.textContent = `Response: ${new TextDecoder().decode(resp)}`;
      } else {
        collectOutput.textContent = "No response (EOF)";
      }
      const s = stream.status;
      if (s && s.code !== 0) {
        collectOutput.textContent += `\nStatus: ${s.code} ${s.message}`;
      }
    } catch (err) {
      collectOutput.textContent = `Exception: ${err}`;
    }
    collectBtn.disabled = false;
  });

  // --- Bidi Streaming (Chat) ---------------------------------------
  const chatBtn = $<HTMLButtonElement>("chat-btn");
  const chatInput = $<HTMLInputElement>("chat-input");
  const chatOutput = $<HTMLPreElement>("chat-output");

  chatBtn.addEventListener("click", async () => {
    if (!client) {
      chatOutput.textContent = "Not connected";
      return;
    }
    chatBtn.disabled = true;
    chatOutput.textContent = "Chatting...";
    try {
      const stream = await client.invokeBidiStreaming(
        "/echo.Echo/Chat",
      );
      const lines: string[] = [];
      for (let i = 1; i <= 3; i++) {
        const msg = `${chatInput.value}-${i}`;
        await stream.send(new TextEncoder().encode(msg));
        const resp = await stream.recv();
        if (resp) {
          lines.push(`ack: ${new TextDecoder().decode(resp)}`);
        } else {
          lines.push(`no ack for msg-${i}`);
          break;
        }
      }
      await stream.closeSend();
      chatOutput.textContent = lines.join("\n");
    } catch (err) {
      chatOutput.textContent = `Exception: ${err}`;
    }
    chatBtn.disabled = false;
  });
}
