/**
 * Peer layer: native RTCPeerConnection management + ICE negotiation.
 *
 * The browser uses the native RTCPeerConnection API. Both the
 * Offerer and Answerer flows are supported; the signaling exchange
 * (SDP offer/answer) happens over a pluggable signal transport.
 */

import { Channel, type TransportConfig } from "@peerrpc/transport";
import { DATACHANNEL_LABEL } from "@peerrpc/protocol";
import { injectMaxMessageSize } from "./sdpMunge.js";

/**
 * SignalMessage carries one signaling envelope between peers.
 * The peer layer does not interpret the body; it just ferries
 * offers/answers/candidates between the two sides.
 */
export interface SignalMessage {
  type: "offer" | "answer" | "candidate";
  sdp?: string;
  candidate?: string;
  sdpMid?: string | null;
  sdpMLineIndex?: number | null;
  from?: string;
  to?: string;
}

/**
 * SignalTransport is the abstraction the peer uses to exchange
 * SDP / ICE candidates. The caller implements send + onMessage.
 *
 * Typical implementations:
 *   - WebSocket client (WebSocketSignal) pointing at signal-server
 *   - a custom in-process transport for tests
 *   - in-memory for tests
 */
export interface SignalTransport {
  send(msg: SignalMessage): void;
  onMessage(cb: (msg: SignalMessage) => void): void;
  close(): void;
}

export interface PeerConfig {
  /** ICE servers (STUN/TURN). */
  iceServers?: RTCIceServer[];
  /** Transport-level tuning. */
  transport?: TransportConfig;
  /** Timeout for ICE gathering + DataChannel open (ms). */
  negotiationTimeout?: number;
}

const DEFAULT_NEGOTIATION_TIMEOUT = 10_000;

/**
 * Peer manages one side of a WebRTC connection.
 */
export class Peer {
  private pc: RTCPeerConnection;
  private cfg: PeerConfig;
  private dc: RTCDataChannel | null = null;
  private channelPromise: Promise<Channel> | null = null;
  private channelResolve: ((ch: Channel) => void) | null = null;
  private channelTimeoutId: ReturnType<typeof setTimeout> | null = null;

  constructor(cfg: PeerConfig = {}) {
    this.cfg = cfg;
    this.pc = new RTCPeerConnection({
      iceServers: cfg.iceServers ?? [],
    });
  }

  /**
   * Offerer flow: create a DataChannel, create an SDP offer, wait for
   * the answer, and open the DataChannel.
   *
   * Returns the offer SDP to send to the Answerer via the signaling
   * transport.
   */
  async createOffer(): Promise<string> {
    this.dc = this.pc.createDataChannel(DATACHANNEL_LABEL, {
      ordered: true,
    });
    this.setupDataChannel(this.dc);

    const offer = await this.pc.createOffer();
    await this.pc.setLocalDescription(offer);
    await this.waitForIceGathering();
    // Advertise a=max-message-size so the SCTP negotiation reaches
    // 256 KiB (browser + vanilla webrtc-rs default to 64 KiB otherwise).
    return injectMaxMessageSize(this.pc.localDescription!.sdp);
  }

  /**
   * Answerer flow: accept the remote offer and create an answer.
   *
   * Returns the answer SDP to send back to the Offerer.
   */
  async acceptOffer(remoteSdp: string): Promise<string> {
    this.pc.ondatachannel = (ev) => {
      this.dc = ev.channel;
      this.setupDataChannel(this.dc);
    };

    await this.pc.setRemoteDescription({ type: "offer", sdp: remoteSdp });
    const answer = await this.pc.createAnswer();
    await this.pc.setLocalDescription(answer);
    await this.waitForIceGathering();
    // Advertise a=max-message-size so the SCTP negotiation reaches
    // 256 KiB (webrtc-rs answerer caps at 64 KiB without this).
    return injectMaxMessageSize(this.pc.localDescription!.sdp);
  }

  /**
   * Apply the remote answer (Offerer side).
   */
  async acceptAnswer(remoteSdp: string): Promise<void> {
    await this.pc.setRemoteDescription({ type: "answer", sdp: remoteSdp });
  }

  /**
   * Add a remote ICE candidate (trickle ICE).
   */
  async addCandidate(candidate: RTCIceCandidateInit): Promise<void> {
    try {
      await this.pc.addIceCandidate(candidate);
    } catch {
      // Late candidates after the connection is established are
      // harmless; ignore.
    }
  }

  /**
   * Wait for the DataChannel to open and return the transport Channel.
   */
  async waitForChannel(): Promise<Channel> {
    if (this.channelPromise) {
      return this.channelPromise;
    }
    if (this.dc && this.dc.readyState === "open") {
      const ch = new Channel(this.dc, this.cfg.transport);
      this.channelPromise = Promise.resolve(ch);
      return this.channelPromise;
    }
    const timeout = this.cfg.negotiationTimeout ?? DEFAULT_NEGOTIATION_TIMEOUT;
    this.channelPromise = new Promise<Channel>((resolve, reject) => {
      this.channelResolve = resolve;
      this.channelTimeoutId = setTimeout(() => {
        if (this.dc && this.dc.readyState === "open") {
          const ch = new Channel(this.dc, this.cfg.transport);
          resolve(ch);
          return;
        }
        reject(new Error(`peer: DataChannel did not open within ${timeout}ms`));
      }, timeout);
    });
    return this.channelPromise;
  }

  /**
   * Close the PeerConnection.
   */
  close(): void {
    try {
      this.pc.close();
    } catch {
      // already closed
    }
  }

  /**
   * Install ICE candidate + connection state callbacks for diagnostics.
   */
  onIceCandidate(cb: (c: RTCIceCandidateInit | null) => void): void {
    this.pc.onicecandidate = (ev) => {
      if (ev.candidate) {
        cb(ev.candidate.toJSON());
      } else {
        cb(null); // gathering complete
      }
    };
  }

  /**
   * Install the ICE connection state callback.
   */
  onIceConnectionStateChange(cb: (state: RTCIceConnectionState) => void): void {
    this.pc.oniceconnectionstatechange = () => {
      cb(this.pc.iceConnectionState);
    };
  }

  private setupDataChannel(dc: RTCDataChannel): void {
    if (dc.readyState === "open") {
      if (this.channelTimeoutId) {
        clearTimeout(this.channelTimeoutId);
        this.channelTimeoutId = null;
      }
      if (this.channelResolve) {
        const ch = new Channel(dc, this.cfg.transport);
        this.channelResolve(ch);
      }
      return;
    }
    dc.onopen = () => {
      if (this.channelTimeoutId) {
        clearTimeout(this.channelTimeoutId);
        this.channelTimeoutId = null;
      }
      if (this.channelResolve) {
        const ch = new Channel(dc, this.cfg.transport);
        this.channelResolve(ch);
      }
    };
  }

  private async waitForIceGathering(): Promise<void> {
    if (this.pc.iceGatheringState === "complete") {
      return;
    }
    return new Promise<void>((resolve) => {
      const check = () => {
        if (this.pc.iceGatheringState === "complete") {
          this.pc.removeEventListener("icegatheringstatechange", check);
          resolve();
        }
      };
      this.pc.addEventListener("icegatheringstatechange", check);
      // Safety timeout: resolve after 5s even if gathering does not
      // complete so localhost-only scenarios don't hang.
      setTimeout(resolve, 5000);
    });
  }
}

/**
 * Convenience: wire two Peers via an in-process signal pipe (for
 * tests). Both sides must be created beforehand; the function then
 * ferries offer/answer between them.
 */
export async function connectPeers(
  offerer: Peer,
  answerer: Peer
): Promise<{ offerer: Channel; answerer: Channel }> {
  const offerSdp = await offerer.createOffer();
  const answerSdp = await answerer.acceptOffer(offerSdp);
  await offerer.acceptAnswer(answerSdp);

  const [och, ach] = await Promise.all([
    offerer.waitForChannel(),
    answerer.waitForChannel(),
  ]);
  return { offerer: och, answerer: ach };
}

/**
 * Dial is the Offerer flow over a SignalTransport: create an offer,
 * pump it through signal, apply the remote answer, drain local ICE
 * candidates through signal, and resolve once the DataChannel opens.
 *
 * The returned Peer must be closed by the caller (or have signal
 * close underneath) to release the RTCPeerConnection.
 *
 * This helper replaces the ~50 lines of manual signal-pump code
 * previously copy-pasted in every TS example (chat client/main.ts,
 * echo/main.ts). It is the building block the top-level
 * @peerrpc/peerrpc facade uses to assemble a full Dial.
 */
export async function dial(
  signal: SignalTransport,
  cfg: PeerConfig = {},
): Promise<{ peer: Peer; channel: Channel }> {
  const peer = new Peer(cfg);

  // Forward local ICE candidates to the remote.
  peer.onIceCandidate((c) => {
    if (c) {
      signal.send({
        type: "candidate",
        candidate: c.candidate,
        sdpMid: c.sdpMid ?? null,
        sdpMLineIndex: c.sdpMLineIndex ?? null,
      });
    }
  });

  // Apply remote ICE candidates, deferring any that arrive before
  // remoteDescription is set. Vanilla ICE would simplify this, but
  // we keep the trickle path so latency-sensitive callers don't
  // regress.
  let remoteDescSet = false;
  // The signal-server broadcasts to EVERY peer in the service, so if
  // more than one Answerer is present (e.g. a stale server peer that
  // never cleanly left) the Offerer may receive several answers. An
  // Offerer applies exactly ONE answer; a second setRemoteDescription
  // would throw InvalidStateError ("wrong state: stable"). Track that
  // we have already accepted an answer and ignore the rest.
  let gotAnswer = false;
  const pending: RTCIceCandidateInit[] = [];

  signal.onMessage(async (msg) => {
    if (msg.type === "answer") {
      if (gotAnswer) return; // already paired with an Answerer
      gotAnswer = true;
      await peer.acceptAnswer(msg.sdp!);
      remoteDescSet = true;
      for (const c of pending) {
        await peer.addCandidate(c);
      }
      pending.length = 0;
    } else if (msg.type === "candidate") {
      const c: RTCIceCandidateInit = {
        candidate: msg.candidate ?? "",
        sdpMid: msg.sdpMid ?? null,
        sdpMLineIndex: msg.sdpMLineIndex ?? null,
      };
      if (!gotAnswer) {
        // Candidate arrived before the answer; buffer it and apply
        // once the remote description is set.
        pending.push(c);
      } else if (remoteDescSet) {
        await peer.addCandidate(c);
      }
    }
  });

  const offerSdp = await peer.createOffer();
  signal.send({ type: "offer", sdp: offerSdp });

  const channel = await peer.waitForChannel();
  return { peer, channel };
}

/**
 * Accept is the Answerer flow over a SignalTransport: wait for an
 * offer, create an answer, pump it through signal, drain local ICE
 * candidates through signal, and resolve once the DataChannel opens.
 *
 * Mirror of dial for the server side.
 */
export async function accept(
  signal: SignalTransport,
  cfg: PeerConfig = {},
): Promise<{ peer: Peer; channel: Channel }> {
  const peer = new Peer(cfg);

  // Wait for the remote offer via signal, then send our answer.
  let gotOffer = false;
  const offerPromise = new Promise<string>((resolve) => {
    signal.onMessage(async (msg) => {
      if (msg.type === "offer" && !gotOffer) {
        gotOffer = true;
        resolve(msg.sdp!);
      } else if (msg.type === "candidate" && gotOffer) {
        // Trickled candidate arriving after we started applying the
        // offer; the acceptOffer call below will have set remote
        // description by the time we reach here.
        await peer.addCandidate({
          candidate: msg.candidate ?? "",
          sdpMid: msg.sdpMid ?? null,
          sdpMLineIndex: msg.sdpMLineIndex ?? null,
        });
      }
    });
  });

  const offerSdp = await offerPromise;
  const answerSdp = await peer.acceptOffer(offerSdp);
  signal.send({ type: "answer", sdp: answerSdp });

  // Forward subsequent local ICE candidates.
  peer.onIceCandidate((c) => {
    if (c) {
      signal.send({
        type: "candidate",
        candidate: c.candidate,
        sdpMid: c.sdpMid ?? null,
        sdpMLineIndex: c.sdpMLineIndex ?? null,
      });
    }
  });

  const channel = await peer.waitForChannel();
  return { peer, channel };
}
