/**
 * Peer layer: native RTCPeerConnection management + ICE negotiation.
 *
 * The browser uses the native RTCPeerConnection API. Both the
 * Offerer and Answerer flows are supported; the signaling exchange
 * (SDP offer/answer) happens over a pluggable signal transport.
 */

import { Channel, type TransportConfig } from "@peerrpc/transport";
import { DATACHANNEL_LABEL } from "@peerrpc/protocol";

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
}

/**
 * SignalTransport is the abstraction the peer uses to exchange
 * SDP / ICE candidates. The caller implements send + onMessage.
 *
 * Typical implementations:
 *   - connect-web client pointing at signal-server
 *   - WebSocket for custom signaling
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
    return this.pc.localDescription!.sdp;
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
    return this.pc.localDescription!.sdp;
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
    const timeout = this.cfg.negotiationTimeout ?? DEFAULT_NEGOTIATION_TIMEOUT;
    this.channelPromise = new Promise<Channel>((resolve, reject) => {
      this.channelResolve = resolve;
      setTimeout(() => {
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
    dc.onopen = () => {
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
