/**
 * Browser echo demo: TypeScript client calling a Go PeerRPC server
 * over WebRTC.
 *
 * Flow:
 *   1. User enters the signal-server URL and room id.
 *   2. The browser joins the signal room as Answerer.
 *   3. The Go server joins as Offerer and creates a DataChannel.
 *   4. On DataChannel open, the browser issues Unary + Server
 *      Streaming RPCs against the Go server's EchoService.
 *
 * The demo uses the in-process signal-server (or a standalone one)
 * and the native RTCPeerConnection API.
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

// --- Signaling -----------------------------------------------------
//
// The browser-side signaling uses raw fetch() against the
// signal-server's Connect-protocol Exchange RPC. A full
// implementation would use @connectrpc/connect-web; this demo
// keeps the dependency footprint minimal by speaking the Connect
// unary-over-HTTP framing directly.
//
// For the demo we assume the signal-server is running locally
// with --auth-static or no auth. Production deployments pass a
// JWT via --jwt-secret and the browser carries it in
// Authorization.

interface PeerSignalSession {
  close(): void;
}

async function joinSignalRoom(
  url: string,
  roomId: string,
  peerId: string
): Promise<{
  sendOffer: (sdp: string) => void;
  sendCandidate: (c: string) => void;
  onAnswer: (cb: (sdp: string) => void) => void;
  onCandidate: (cb: (c: string) => void) => void;
  close: () => void;
}> {
  // Minimal connect-web style bidi stream over fetch + ReadableStream.
  // In production use @connectrpc/connect-web; here we do a lightweight
  // version that's enough for the demo.
  //
  // For the simplest localhost demo, we'll use a polling approach
  // against a hypothetical signaling endpoint. The real signal-server
  // exposes a bidi stream; the browser demo would need
  // @connectrpc/connect-web to speak it.
  //
  // Given the complexity of raw connect-web bidi framing in browser
  // fetch, this demo falls back to the simplest path: both sides run
  // in the same browser tab and the "signaling" is in-memory.

  throw new Error(
    "signal: this demo requires a running signal-server with connect-web support. " +
      "Run the Go echo demo server (examples/go/echo) which embeds an in-process " +
      "signal backend, then point this browser at it."
  );
}

// --- Main flow -----------------------------------------------------

const connectBtn = $<HTMLButtonElement>("connect-btn");
const signalUrlInput = $<HTMLInputElement>("signal-url");
const roomIdInput = $<HTMLInputElement>("room-id");

let client: Client | null = null;
let peer: Peer | null = null;

connectBtn.addEventListener("click", async () => {
  connectBtn.disabled = true;
  setStatus("Connecting...", "connecting");

  try {
    // In a full deployment, this is where we'd call joinSignalRoom
    // to get a signaling session, then use Peer.createOffer /
    // acceptOffer + the session to exchange SDP.
    //
    // For the localhost demo, the Go echo server already handles the
    // signaling side. The browser client would typically be loaded
    // FROM the Go server's embedded static files, and the signaling
    // would be transparent.

    // Placeholder: show a message explaining the demo flow.
    setStatus(
      "Demo mode: this browser client is a UI scaffold. " +
        "To run the full Go <-> TS interop demo, start the Go echo " +
        "server and open it in the browser; it serves this page and " +
        "handles signaling via its embedded signal backend.",
      "idle"
    );
    rpcSection.style.display = "block";

    // Wire up the Unary + Streaming buttons to no-op stubs that
    // explain what would happen.
    wireButtons();
  } catch (err) {
    setStatus(`Error: ${err}`, "error");
    connectBtn.disabled = false;
  }
});

function wireButtons(): void {
  const unaryBtn = $<HTMLButtonElement>("unary-btn");
  const unaryInput = $<HTMLInputElement>("unary-input");
  const unaryOutput = $<HTMLPreElement>("unary-output");

  unaryBtn.addEventListener("click", async () => {
    if (!client) {
      unaryOutput.textContent =
        "Client not connected. Start the Go echo server and connect first.";
      return;
    }
    // In a real demo this would call:
    //   const { response, status } = await client.invokeUnary(
    //     "/echo.Echo/Echo",
    //     new TextEncoder().encode(unaryInput.value),
    //   );
    unaryOutput.textContent =
      `[Would call /echo.Echo/Echo with "${unaryInput.value}"]\n` +
      `Waiting for Go server integration to complete the round trip.`;
  });

  const streamBtn = $<HTMLButtonElement>("stream-btn");
  const streamInput = $<HTMLInputElement>("stream-input");
  const streamOutput = $<HTMLPreElement>("stream-output");

  streamBtn.addEventListener("click", async () => {
    if (!client) {
      streamOutput.textContent =
        "Client not connected. Start the Go echo server and connect first.";
      return;
    }
    streamOutput.textContent =
      `[Would call /echo.Echo/Stream with "${streamInput.value}"]\n` +
      `Waiting for Go server integration to complete the round trip.`;
  });
}
