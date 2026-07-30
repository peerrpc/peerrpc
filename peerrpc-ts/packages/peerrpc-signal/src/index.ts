/**
 * @peerrpc/signal: signaling client for the PeerRPC rendezvous
 * service.
 *
 *   - WebSocketSignal — protobuf over WebSocket (the sole signaling
 *     transport; works from both browsers and Node)
 */

export { WebSocketSignal, type WebSocketSignalConfig } from "./ws.js";
