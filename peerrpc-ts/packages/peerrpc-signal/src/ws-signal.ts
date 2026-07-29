import type { SignalMessage } from "@peerrpc/peer";

export interface WebSocketSignalConfig {
  url: string;
  roomId: string;
  peerId: string;
  role: number;
}

export class WebSocketSignal {
  private cfg: WebSocketSignalConfig;
  private onMessageCb: ((msg: SignalMessage) => void) | null = null;
  private ws: WebSocket | null = null;

  constructor(cfg: WebSocketSignalConfig) {
    this.cfg = cfg;
  }

  async connect(): Promise<void> {
    console.log("[ws-signal] connect()", this.cfg);
    const ws = new WebSocket(this.cfg.url);
    this.ws = ws;

    return new Promise<void>((resolve, reject) => {
      ws.onopen = () => {
        console.log("[ws-signal] onopen, sending join");
        ws.send(JSON.stringify({
          type: "join",
          roomId: this.cfg.roomId,
          peerId: this.cfg.peerId,
          role: this.cfg.role,
        }));
        resolve();
      };

      ws.onerror = (ev) => {
        console.error("[ws-signal] onerror", ev);
        reject(new Error("signal: WebSocket connection failed"));
      };

      ws.onclose = (ev) => {
        console.log("[ws-signal] onclose", ev.code, ev.reason);
        this.ws = null;
      };

      ws.onmessage = (ev) => {
        try {
          const data = JSON.parse(ev.data);
          console.log("[ws-signal] onmessage", data.type, data.sdp ? "sdp=" + data.sdp.slice(0, 40) + "..." : "");
          this.onMessageCb?.(data);
        } catch {
          console.warn("[ws-signal] ignoring invalid JSON", ev.data);
        }
      };
    });
  }

  onMessage(cb: (msg: SignalMessage) => void): void {
    console.log("[ws-signal] onMessage registered");
    this.onMessageCb = cb;
  }

  send(msg: SignalMessage): void {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
      throw new Error("signal: not connected; call connect() first");
    }
    this.ws.send(JSON.stringify({ ...msg, from: this.cfg.peerId }));
  }

  close(): void {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }
}
