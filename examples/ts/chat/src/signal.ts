import { WebSocketSignal } from "@peerrpc/signal";

function toWsUrl(httpUrl: string): string {
  const u = new URL(httpUrl);
  return `${u.protocol === "https:" ? "wss:" : "ws:"}//${u.host}/signal-ws`;
}

export function createSignal(
  url: string,
  roomId: string,
  peerId: string,
  role: number,
): WebSocketSignal {
  return new WebSocketSignal({
    url: toWsUrl(url),
    roomId,
    peerId,
    role,
  });
}
