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
 *
 * usePeerRPC delegates the full connection (signal transport + WebRTC
 * offer/answer/ICE + rpc.Client) to the @peerrpc/peerrpc facade's
 * dial(target), which already guards against multi-answer fan-out.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { dial, type Conn, type DialOptions } from "@peerrpc/peerrpc";
import { Client, type Status, type Metadata } from "@peerrpc/rpc";

// ---------------------------------------------------------------------------
// usePeerRPC: connection lifecycle
// ---------------------------------------------------------------------------

export interface PeerRPCConfig {
  /**
   * Target URI for the @peerrpc/peerrpc facade dial(), e.g.
   * "peerrpc+ws://localhost:8443/echo.Echo". The facade builds the
   * signal transport (WebSocketSignal for ws://) and the rpc.Client.
   */
  target: string;
  /** Optional dial options (peer config, token, explicit peer_id). */
  dialOptions?: DialOptions;
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
 * Connection is delegated to the @peerrpc/peerrpc facade dial(target),
 * which builds the signal transport from the target URI and wires the
 * rpc.Client. The facade's dial already guards against the multi-answer
 * fan-out that occurs when several Answerers are present in the same
 * signaling service.
 *
 * On unmount the hook automatically calls disconnect().
 *
 * @example
 *   const rpc = usePeerRPC({ target: "peerrpc+ws://localhost:8443/echo.Echo" });
 *   ...
 *   <button onClick={() => rpc.connect()}>Connect</button>
 */
export function usePeerRPC(config: PeerRPCConfig): PeerRPCState {
  const [status, setStatus] = useState<ConnectionStatus>("idle");
  const [error, setError] = useState<string | null>(null);
  const [client, setClient] = useState<Client | null>(null);

  const connRef = useRef<Conn | null>(null);
  const configRef = useRef(config);
  configRef.current = config;

  const connect = useCallback(async () => {
    if (status === "connecting" || status === "connected") return;
    setStatus("connecting");
    setError(null);

    try {
      const conn = await dial(configRef.current.target, configRef.current.dialOptions ?? {});
      connRef.current = conn;
      setClient(conn.client);
      setStatus("connected");
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
      setStatus("error");
    }
  }, [status]);

  const disconnect = useCallback(() => {
    // Conn.close releases the peer + signal transport.
    connRef.current?.close().catch(() => {});
    connRef.current = null;
    setClient(null);
    setStatus("idle");
  }, []);

  // Cleanup on unmount.
  useEffect(() => {
    return () => {
      connRef.current?.close().catch(() => {});
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
