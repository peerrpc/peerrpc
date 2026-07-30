/**
 * Transport layer: wraps an RTCDataChannel into a frame-level duplex
 * pipe with sharding and backpressure, matching the Go transport
 * package.
 *
 * The browser uses the native RTCDataChannel API (not pion). The
 * DataChannel must be created with ordered=true, reliable delivery.
 */

import {
  encodeFrame,
  encodeResponseFrame,
  tryDecodeFrame,
  tryDecodeResponseFrame,
  BUFFERED_AMOUNT_HIGH,
  INLINE_MAX,
  MESSAGE_MAX,
  CHUNK_SIZE,
  type Frame,
  ResponseFrame,
} from "@peerrpc/protocol";

/** Accumulated inbound bytes from the DataChannel; frames may arrive split. */
type FrameQueue = {
  /** Decoded inbound frames pending dispatch to the RPC layer. */
  frames: ResponseFrame[];
  /** Callback invoked when a new frame is decoded. */
  onFrame: (frame: ResponseFrame) => void;
};

export interface TransportConfig {
  /** Override the high-watermark for outbound backpressure. */
  bufferedAmountHigh?: number;
  /**
   * Inbound decode mode: "response" (default) decodes ResponseFrame
   * (server→client), "request" decodes Frame (client→server).
   */
  decodeMode?: "response" | "request";
}

/**
 * Channel wraps an established RTCDataChannel into a frame-level pipe.
 *
 * One Channel per DataChannel. The RPC layer installs its dispatch
 * via `onFrame` and sends outbound via `send`.
 */
export class Channel {
  private dc: RTCDataChannel;
  private highWatermark: number;
  private decodeMode: "response" | "request";
  private inboundBuf: Uint8Array = new Uint8Array(0);
  private onFrameCb: ((frame: ResponseFrame | Frame) => void) | null = null;
  private onCloseCb: (() => void) | null = null;
  private closed = false;

  // Chunk reassembly state keyed by sequence.
  private reasm: Map<number, { total: number; buf: Uint8Array; got: number }> =
    new Map();

  constructor(dc: RTCDataChannel, cfg?: TransportConfig) {
    this.dc = dc;
    this.highWatermark = cfg?.bufferedAmountHigh ?? BUFFERED_AMOUNT_HIGH;
    this.decodeMode = cfg?.decodeMode ?? "response";
    dc.binaryType = "arraybuffer";
    dc.onmessage = (ev) => this.handleMessage(ev);
    dc.onclose = () => this.handleClose();
    dc.onerror = () => this.handleClose();
    dc.bufferedAmountLowThreshold = this.highWatermark;
  }

  /**
   * Install the inbound frame handler. Called exactly once for each
   * decoded ResponseFrame.
   */
  onFrame(cb: (frame: ResponseFrame | Frame) => void): void {
    this.onFrameCb = cb;
  }

  /**
   * Set the inbound decode mode. "response" (default) decodes
   * ResponseFrame (client side), "request" decodes Frame (server side).
   */
  setDecodeMode(mode: "response" | "request"): void {
    this.decodeMode = mode;
  }

  /**
   * Install the close handler. Fires when the DataChannel closes.
   */
  onClose(cb: () => void): void {
    this.onCloseCb = cb;
  }

  /**
   * Send a Frame (client -> server) or ResponseFrame (server -> client)
   * over the DataChannel. The frame is encoded according to its actual
   * type so a server emitting ResponseFrames is not misencoded as a
   * Frame. Applies backpressure by awaiting the `bufferedamountlow`
   * event when the SCTP buffer exceeds the high-watermark.
   */
  async send(frame: Frame | ResponseFrame): Promise<void> {
    if (this.closed) {
      throw new Error("transport: channel closed");
    }
    // Backpressure: wait if the buffer is above the watermark.
    if (this.dc.bufferedAmount >= this.highWatermark) {
      await this.awaitBufferLow();
    }
    // Encode by the frame's actual runtime type. The shared `Frame`
    // import used as a base class would otherwise cause a ResponseFrame
    // to be serialized with Frame's tag layout.
    const encoded = frame instanceof ResponseFrame
      ? encodeResponseFrame(frame as ResponseFrame)
      : encodeFrame(frame as Frame);
    this.dc.send(encoded);
  }

  /**
   * Send raw pre-marshaled bytes (used by the relay for transparent
   * forwarding).
   */
  async sendRaw(payload: Uint8Array): Promise<void> {
    if (this.closed) {
      throw new Error("transport: channel closed");
    }
    if (this.dc.bufferedAmount >= this.highWatermark) {
      await this.awaitBufferLow();
    }
    this.dc.send(payload);
  }

  /**
   * Close the underlying DataChannel.
   */
  close(): void {
    if (!this.closed) {
      this.closed = true;
      try {
        this.dc.close();
      } catch {
        // already closed
      }
    }
  }

  /** Whether the channel has been closed. */
  isClosed(): boolean {
    return this.closed;
  }

  /**
   * Reassemble a Chunk frame into a logical message buffer. When all
   * bytes are present the assembled payload is returned.
   */
  reassemble(
    seq: number,
    total: number,
    offset: number,
    data: Uint8Array
  ): Uint8Array | null {
    let state = this.reasm.get(seq);
    if (!state || state.total !== total) {
      state = { total, buf: new Uint8Array(total), got: 0 };
      this.reasm.set(seq, state);
    }
    const end = offset + data.length;
    if (end > state.buf.length) {
      return null;
    }
    state.buf.set(data, offset);
    state.got += data.length;
    if (state.got >= state.total) {
      const out = state.buf;
      this.reasm.delete(seq);
      return out;
    }
    return null;
  }

  private handleMessage(ev: MessageEvent): void {
    const incoming = new Uint8Array(ev.data as ArrayBuffer);
    // Append to the inbound buffer.
    const merged = new Uint8Array(this.inboundBuf.length + incoming.length);
    merged.set(this.inboundBuf, 0);
    merged.set(incoming, this.inboundBuf.length);
    this.inboundBuf = merged;

    // Try to decode as many frames as the buffer holds.
    for (;;) {
      const result = this.decodeMode === "request"
        ? tryDecodeFrame(this.inboundBuf)
        : tryDecodeResponseFrame(this.inboundBuf);
      if (result === null) {
        break;
      }
      this.inboundBuf = this.inboundBuf.subarray(result.consumed);
      if (this.onFrameCb) {
        this.onFrameCb(result.frame);
      }
    }
  }

  private handleClose(): void {
    if (!this.closed) {
      this.closed = true;
    }
    if (this.onCloseCb) {
      this.onCloseCb();
    }
  }

  private awaitBufferLow(): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.dc.bufferedAmount < this.highWatermark) {
        resolve();
        return;
      }
      const handler = () => {
        this.dc.removeEventListener("bufferedamountlow", handler);
        resolve();
      };
      this.dc.addEventListener("bufferedamountlow", handler);
      // Safety timeout: resolve after 5s even if the event never
      // fires so callers do not hang forever on a dead channel.
      setTimeout(() => {
        this.dc.removeEventListener("bufferedamountlow", handler);
        if (this.closed) {
          reject(new Error("transport: channel closed while waiting for buffer drain"));
        } else {
          resolve();
        }
      }, 5000);
    });
  }
}

// Re-export thresholds for callers.
export { INLINE_MAX, MESSAGE_MAX, CHUNK_SIZE, BUFFERED_AMOUNT_HIGH };
