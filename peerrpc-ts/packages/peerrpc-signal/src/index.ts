/**
 * @peerrpc/signal: signaling clients for the PeerRPC rendezvous
 * service.
 *
 * v2 entry points (preferred):
 *   - ConnectSignalV2       — Connect-RPC over HTTP/2 (server-side / Node)
 *   - WebSocketSignalV2     — protobuf over WebSocket (browser)
 *
 * v1 entry points (deprecated; removed two releases after v2 GA):
 *   - ConnectSignalV1       — v1 Connect-RPC (roomId, JoinRequest)
 *   - WebSocketSignal       — ad-hoc JSON over WebSocket
 */

export { ConnectSignalV2, type ConnectSignalV2Config } from "./v2.js";
export { WebSocketSignalV2, type WebSocketSignalV2Config } from "./ws_v2.js";

// v1 aliases; kept until the migration window closes.
export { ConnectSignal as ConnectSignalV1 } from "./v1.js";
export type { ConnectSignalConfig as ConnectSignalV1Config } from "./v1.js";
export { WebSocketSignal } from "./ws-signal.js";
export type { WebSocketSignalConfig } from "./ws-signal.js";
