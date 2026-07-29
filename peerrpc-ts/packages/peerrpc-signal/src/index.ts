/**
 * @peerrpc/signal: signaling clients for the PeerRPC rendezvous
 * service.
 *
 *   - ConnectSignal     — Connect-RPC over HTTP/2 (server-side / Node)
 *   - WebSocketSignal   — protobuf over WebSocket (browser)
 */

export { ConnectSignal, type ConnectSignalConfig } from "./connect.js";
export { WebSocketSignal, type WebSocketSignalConfig } from "./ws.js";
