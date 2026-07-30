/**
 * @file PeerRPC React Hooks.
 *
 * Public API:
 *   usePeerRPC(config)     — manage WebRTC connection + RPC client
 *   useUnary(method)       — invoke a unary RPC with React state
 *   useServerStream(method) — consume a server-streaming RPC
 *
 * The hooks decouple the React render cycle from the PeerRPC
 * event-driven transport. State updates are batched; effect cleanup
 * tears down streams and listeners.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { Channel } from "@peerrpc/transport";
import { Peer, type PeerConfig, type SignalTransport, type SignalMessage } from "@peerrpc/peer";
import { Client, type Status, type Metadata } from "@peerrpc/rpc";

// ---------------------------------------------------------------------------
// usePeerRPC: connection lifecycle
// ---------------------------------------------------------------------------

export interface PeerRPCConfig {
  /** PeerConnection tuning. */
  peer?: PeerConfig;
  /**
   * Factory that returns a SignalTransport the Peer uses to exchange
   * SDP/ICE. Called once when connect() is invoked.
   */
  createSignal: () => SignalTransport;
}

export interface PeerRPCState {
  /** "idle" | "connecting" | "connected" | "error" */
  status: ConnectionStatus;
  /** The live rpc.Client once connected; null otherwise. */
  client: Client | null;
  /** Error message when status === "error". */
  error: string | null;
  /** Call to initiate the connection. */
  connect: () => Promise<void>;
  /** Call to tear down the connection. */
  disconnect: () => void;
}

export type ConnectionStatus = "idle" | "connecting" | "connected" | "error";

/**
 * usePeerRPC manages the full WebRTC + PeerRPC connection lifecycle.
 *
 * The hook returns a stable object whose fields update as the
 * connection progresses through idle → connecting → connected.
 *
 * The caller MUST provide a `createSignal` factory that returns a
 * SignalTransport implementation (e.g. WebSocketSignal wrapping a
 * signal-server URL, or a custom in-process transport).
 *
 * On unmount the hook automatically calls disconnect().
 */
export function usePeerRPC(config: PeerRPCConfig): PeerRPCState {
  const [status, setStatus] = useState<ConnectionStatus>("idle");
  const [error, setError] = useState<string | null>(null);
  const [client, setClient] = useState<Client | null>(null);

  const peerRef = useRef<Peer | null>(null);
  const channelRef = useRef<Channel | null>(null);
  const configRef = useRef(config);
  configRef.current = config;

  const connect = useCallback(async () => {
    if (status === "connecting" || status === "connected") return;
    setStatus("connecting");
    setError(null);

    try {
      const signal = configRef.current.createSignal();
      const peer = new Peer(configRef.current.peer);
      peerRef.current = peer;

      // Offerer flow: create offer, send via signal, wait for answer.
      const offerSdp = await peer.createOffer();
      signal.send({ type: "offer", sdp: offerSdp });

      // Pump inbound signal messages.
      signal.onMessage((msg: SignalMessage) => {
        switch (msg.type) {
          case "answer":
            peer.acceptAnswer(msg.sdp!).catch(() => {});
            break;
          case "candidate":
            peer.addCandidate({
              candidate: msg.candidate!,
              sdpMid: msg.sdpMid ?? null,
              sdpMLineIndex: msg.sdpMLineIndex ?? null,
            }).catch(() => {});
            break;
        }
      });

      // Forward local ICE candidates via signal.
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
      channelRef.current = channel;
      const rpcClient = new Client(channel);
      setClient(rpcClient);
      setStatus("connected");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setStatus("error");
    }
  }, [status]);

  const disconnect = useCallback(() => {
    channelRef.current?.close();
    channelRef.current = null;
    peerRef.current?.close();
    peerRef.current = null;
    setClient(null);
    setStatus("idle");
  }, []);

  // Cleanup on unmount.
  useEffect(() => {
    return () => {
      channelRef.current?.close();
      peerRef.current?.close();
    };
  }, []);

  return { status, client, error, connect, disconnect };
}

// ---------------------------------------------------------------------------
// useUnary: invoke a unary RPC with React state
// ---------------------------------------------------------------------------

export interface UnaryState<T = Uint8Array> {
  data: T | null;
  loading: boolean;
  error: Status | null;
  /** Invoke the RPC. Returns the response payload + status. */
  invoke: (req: Uint8Array, metadata?: Metadata) => Promise<{ response: Uint8Array; status: Status }>;
  /** Reset to the initial state. */
  reset: () => void;
}

/**
 * useUnary wraps a unary RPC invocation in React state.
 *
 * Pass the rpc.Client from usePeerRPC. On each invoke() the hook
 * sets loading=true, calls client.invokeUnary, and updates data or
 * error based on the result.
 *
 * Use the optional `transform` to decode the raw bytes into a
 * domain type (e.g. fromBinary).
 *
 * @example
 *   const { client } = usePeerRPC({ ... });
 *   const { data, loading, invoke } = useUnary(client, "/echo.Echo/Echo");
 *   ...
 *   <button onClick={() => invoke(new TextEncoder().encode("hi"))}>Send</button>
 *   {loading && <p>Loading...</p>}
 *   {data && <p>{data}</p>}
 */
export function useUnary<T = Uint8Array>(
  client: Client | null,
  method: string,
  transform?: (raw: Uint8Array) => T,
): UnaryState<T> {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Status | null>(null);

  const invoke = useCallback(
    async (req: Uint8Array, metadata?: Metadata) => {
      if (!client) {
        const st: Status = { code: 14, message: "not connected" };
        setError(st);
        return { response: new Uint8Array(0), status: st };
      }
      setLoading(true);
      setError(null);
      try {
        const { response, status } = await client.invokeUnary(method, req, metadata);
        if (status.code === 0) {
          setData((transform ? transform(response) : response) as T);
        } else {
          setError(status);
        }
        return { response, status };
      } catch (err) {
        const st: Status = {
          code: 13,
          message: err instanceof Error ? err.message : String(err),
        };
        setError(st);
        return { response: new Uint8Array(0), status: st };
      } finally {
        setLoading(false);
      }
    },
    [client, method, transform],
  );

  const reset = useCallback(() => {
    setData(null);
    setLoading(false);
    setError(null);
  }, []);

  return { data, loading, error, invoke, reset };
}

// ---------------------------------------------------------------------------
// useServerStream: consume a server-streaming RPC
// ---------------------------------------------------------------------------

export interface ServerStreamState<T = Uint8Array> {
  /** Messages received so far. */
  messages: T[];
  loading: boolean;
  error: Status | null;
  done: boolean;
  /** Start the stream. */
  start: (req: Uint8Array, metadata?: Metadata) => Promise<void>;
  /** Abort the current stream. */
  abort: () => void;
  /** Reset to the initial state. */
  reset: () => void;
}

/**
 * useServerStream wraps a server-streaming RPC in React state.
 *
 * Each chunk is appended to `messages`. The optional `transform`
 * decodes raw bytes into a domain type (e.g. fromBinary).
 *
 * @example
 *   const { messages, loading, start } = useServerStream(
 *     client,
 *     "/chat.Chat/Join",
 *     (raw) => ChatMessage.fromBinary(raw),
 *   );
 */
export function useServerStream<T = Uint8Array>(
  client: Client | null,
  method: string,
  transform?: (raw: Uint8Array) => T,
): ServerStreamState<T> {
  const [messages, setMessages] = useState<T[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Status | null>(null);
  const [done, setDone] = useState(false);
  const abortRef = useRef(false);

  const start = useCallback(
    async (req: Uint8Array, metadata?: Metadata) => {
      if (!client) {
        setError({ code: 14, message: "not connected" });
        return;
      }
      setLoading(true);
      setError(null);
      setDone(false);
      setMessages([]);
      abortRef.current = false;

      try {
        const stream = await client.invokeServerStreaming(method, req, metadata);
        for (;;) {
          if (abortRef.current) break;
          const chunk = await stream.recv();
          if (chunk === null) break;
          const decoded = transform ? transform(chunk) : (chunk as unknown as T);
          setMessages((prev) => [...prev, decoded]);
        }
        setDone(true);
      } catch (err) {
        setError({
          code: 13,
          message: err instanceof Error ? err.message : String(err),
        });
      } finally {
        setLoading(false);
      }
    },
    [client, method, transform],
  );

  const abort = useCallback(() => {
    abortRef.current = true;
  }, []);

  const reset = useCallback(() => {
    setMessages([]);
    setLoading(false);
    setError(null);
    setDone(false);
    abortRef.current = false;
  }, []);

  return { messages, loading, error, done, start, abort, reset };
}

// ---------------------------------------------------------------------------
// useClientStatus: subscribe to connection status from usePeerRPC
// ---------------------------------------------------------------------------

/**
 * Convenience selector that derives a boolean from PeerRPCState.status.
 * Useful for conditional rendering without repeating the union type.
 *
 * @example
 *   const rpc = usePeerRPC({ ... });
 *   const connected = useConnected(rpc);
 *   return connected ? <Chat /> : <Login />;
 */
export function useConnected(state: PeerRPCState): boolean {
  return state.status === "connected";
}
