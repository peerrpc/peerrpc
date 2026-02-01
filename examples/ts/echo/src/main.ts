/**
 * Browser echo demo: TypeScript client calling a Go PeerRPC server
 * over WebRTC.
 *
 * Flow:
 *   1. Page loads; user clicks "Connect".
 *   2. Browser opens a Connect bidi stream to /Exchange on the same
 *      origin and joins room "interop" as Answerer.
 *   3. Go server (Offerer) creates a DataChannel; its SDP offer
 *      arrives via the signaling stream.
 *   4. Browser creates an RTCPeerConnection, sets the remote offer,
 *      generates an answer, and sends it back.
 *   5. ICE candidates are exchanged bidirectionally.
 *   6. On DataChannel open, the browser issues Unary + Server
 *      Streaming RPCs against the Go EchoService.
 */

import { Peer } from "@peerrpc/peer";
import { Client, Code, type Status } from "@peerrpc/rpc";

// --- DOM helpers --------------------------------------------------

const $ = <T extends HTMLElement>(id: string): T =>
  document.getElementById(id) as T;

const statusEl = $<HTMLDivElement>("status");
const rpcSection = $<HTMLDivElement>("rpc-section");

function setStatus(text: string, cls: string): void {
  statusEl.textContent = text;
  statusEl.className = `status ${cls}`;
}

// --- Connect protocol signaling over fetch -------------------------
//
// The browser talks to the Go signal-server via the Connect protocol's
// streaming endpoint. The simplest browser-compatible approach is a
// fetch() POST with a streaming request body + streaming response.
// Connect uses enveloped framing (1 byte flags + 4 bytes length +
// payload); we wrap each SignalMessage in that envelope.

const CONNECT_ENVELOPE_FLAGS = 0x00; // no compression

function encodeConnectEnvelope(payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(5 + payload.length);
  const view = new DataView(out.buffer);
  view.setUint8(0, CONNECT_ENVELOPE_FLAGS);
  view.setUint32(1, payload.length, false); // big-endian
  out.set(payload, 5);
  return out;
}

function tryDecodeConnectEnvelope(buf: Uint8Array): { payload: Uint8Array; consumed: number } | null {
  if (buf.length < 5) return null;
  const view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  const len = view.getUint32(1, false);
  if (buf.length < 5 + len) return null;
  return { payload: buf.subarray(5, 5 + len), consumed: 5 + len };
}

/**
 * SignalSession wraps the Connect bidi stream into a simple
 * send / onMessage interface.
 */
class SignalSession {
  private writable: WritableStreamDefaultWriter<Uint8Array>;
  private reader: ReadableStreamDefaultReader<Uint8Array>;
  private inboundBuf: Uint8Array = new Uint8Array(0);
  private onMessageCb: ((payload: Uint8Array) => void) | null = null;
  private abortController: AbortController;

  constructor(
    writable: WritableStreamDefaultWriter<Uint8Array>,
    reader: ReadableStreamDefaultReader<Uint8Array>,
    abortController: AbortController
  ) {
    this.writable = writable;
    this.reader = reader;
    this.abortController = abortController;
    this.startPump();
  }

  onMessage(cb: (payload: Uint8Array) => void): void {
    this.onMessageCb = cb;
  }

  async send(payload: Uint8Array): Promise<void> {
    const env = encodeConnectEnvelope(payload);
    await this.writable.write(env);
  }

  close(): void {
    this.abortController.abort();
  }

  private async startPump(): Promise<void> {
    try {
      for (;;) {
        const { done, value } = await this.reader.read();
        if (done) break;
        if (!value) continue;

        // Append to buffer.
        const merged = new Uint8Array(this.inboundBuf.length + value.length);
        merged.set(this.inboundBuf, 0);
        merged.set(value, this.inboundBuf.length);
        this.inboundBuf = merged;

        // Decode envelopes.
        for (;;) {
          const result = tryDecodeConnectEnvelope(this.inboundBuf);
          if (result === null) break;
          this.inboundBuf = this.inboundBuf.subarray(result.consumed);
          if (this.onMessageCb) {
            this.onMessageCb(result.payload);
          }
        }
      }
    } catch {
      // stream closed
    }
  }
}

async function openSignalStream(url: string): Promise<SignalSession> {
  const abortController = new AbortController();

  // Use a TransformStream as the request body so we can write
  // incrementally.
  const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
  const writer = writable.getWriter();

  const resp = await fetch(`${url}/peerrpc.signaling.v1.SignalingService/Exchange`, {
    method: "POST",
    headers: {
      "Content-Type": "application/connect+proto",
      "Connect-Protocol-Version": "1",
    },
    body: readable,
    signal: abortController.signal,
  });

  if (!resp.ok || !resp.body) {
    throw new Error(`signaling HTTP ${resp.status}`);
  }

  const reader = resp.body.getReader();
  return new SignalSession(writer, reader, abortController);
}

// --- Protobuf encode/decode for SignalMessage ----------------------
//
// We use the generated protobuf types from @peerrpc/protocol to
// marshal SignalMessage payloads. The Connect envelope wraps these
// payloads.

import {
  SignalMessage as WireSignalMessage,
} from "@peerrpc/protocol/gen/peerrpc/signaling/v1/signaling_pb.js";
import {
  SignalingService,
} from "@peerrpc/protocol/gen/peerrpc/signaling/v1/signaling_pb.js";

// --- Main flow -----------------------------------------------------

const connectBtn = $<HTMLButtonElement>("connect-btn");

let client: Client | null = null;
let signalSession: SignalSession | null = null;

connectBtn.addEventListener("click", async () => {
  connectBtn.disabled = true;
  setStatus("Connecting...", "connecting");

  try {
    await connect();
    setStatus("Connected", "connected");
    rpcSection.style.display = "block";
    wireButtons();
  } catch (err) {
    setStatus(`Error: ${err}`, "error");
    connectBtn.disabled = false;
  }
});

async function connect(): Promise<void> {
  // 1. Open signaling stream.
  signalSession = await openSignalStream("");

  // 2. Send Join message.
  const joinMsg = new WireSignalMessage({
    roomId: "interop",
    body: {
      case: "join",
      value: { peerId: "browser-" + Date.now(), role: 2 /* ANSWERER */ },
    },
  });
  await signalSession.send(joinMsg.toBinary());

  // 3. Create PeerConnection.
  const pc = new RTCPeerConnection();

  // 4. Pump signaling messages.
  const pendingCandidates: RTCIceCandidateInit[] = [];
  let remoteDescSet = false;

  signalSession.onMessage((payload) => {
    const msg = new WireSignalMessage().fromBinary(payload);
    switch (msg.body.case) {
      case "offer": {
        pc.setRemoteDescription({ type: "offer", sdp: msg.body.value.sdp })
          .then(async () => {
            remoteDescSet = true;
            // Flush buffered candidates.
            for (const c of pendingCandidates) {
              await pc.addIceCandidate(c);
            }
            // Create answer.
            const answer = await pc.createAnswer();
            await pc.setLocalDescription(answer);
            // Wait for ICE gathering.
            await waitForIceGathering(pc);

            const ansMsg = new WireSignalMessage({
              roomId: "interop",
              body: {
                case: "answer",
                value: { sdp: pc.localDescription!.sdp },
              },
            });
            await signalSession!.send(ansMsg.toBinary());
          })
          .catch((err) => setStatus(`Offer error: ${err}`, "error"));
        break;
      }
      case "candidate": {
        const c: RTCIceCandidateInit = {
          candidate: msg.body.value.candidate,
          sdpMid: msg.body.value.sdpMid || null,
          sdpMLineIndex: msg.body.value.sdpMLineIndex ?? null,
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

  // 5. Forward local ICE candidates.
  pc.onicecandidate = (ev) => {
    if (ev.candidate && signalSession) {
      const candMsg = new WireSignalMessage({
        roomId: "interop",
        body: {
          case: "candidate",
          value: {
            candidate: ev.candidate.candidate,
            sdpMid: ev.candidate.sdpMid,
            sdpMLineIndex: ev.candidate.sdpMLineIndex ?? 0,
          },
        },
      });
      signalSession.send(candMsg.toBinary()).catch(() => {});
    }
  };

  // 6. Wait for DataChannel (Go server creates it).
  const channel = await new Promise<RTCDataChannel>((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("DataChannel timeout")), 30000);
    pc.ondatachannel = (ev) => {
      clearTimeout(timeout);
      ev.channel.onopen = () => resolve(ev.channel);
    };
  });

  // 7. Wrap into transport.Channel + rpc.Client.
  const { Channel } = await import("@peerrpc/transport");
  const transport = new Channel(channel);
  client = new Client(transport);
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
        unaryOutput.textContent =
          `Response: ${new TextDecoder().decode(response)}\n` +
          `Header: ${JSON.stringify(client["ch"] ?? {})}`;
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
}
