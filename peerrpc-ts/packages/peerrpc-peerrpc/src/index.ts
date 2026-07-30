/**
 * PeerRPC facade: collapse signal + peer + rpc into dial / listen.
 *
 *   const conn = await peerrpc.dial("peerrpc+local:///echo.Echo");
 *   const { response, status } =
 *     await conn.client.invokeUnary("/echo.Echo/Unary", req);
 *
 *   const ln = await peerrpc.listen("peerrpc+local:///echo.Echo");
 *   await ln.serve(srv => srv.registerService(echoDesc));
 *
 * Mirrors the Go peerrpc package: three entry styles (URL / Target /
 * Builder) all funnel into one dialTarget / listenTarget core.
 *
 * On the browser, dial defaults to the ws scheme if a host authority
 * is provided; on Node it defaults to connect. The local scheme is
 * always available for tests and single-binary demos.
 */

import { type SignalTransport, dial as peerDial, accept as peerAccept, type PeerConfig } from "@peerrpc/peer";
import { Client, Server, ServerStream, MethodKind, ok, err, type ServiceDesc, type Handler, type Status } from "@peerrpc/rpc";
import type { Channel } from "@peerrpc/transport";
import { ConnectSignal } from "@peerrpc/signal";
import { WebSocketSignal } from "@peerrpc/signal";
import { localBus } from "./local_bus.js";
import { parseTarget, formatTarget, type Target, type Scheme, type RoleHint, TargetParseError } from "./target.js";
import type { DialOptions, ListenOptions } from "./options.js";

export { parseTarget, formatTarget, TargetParseError };
export type { Target, Scheme, RoleHint, DialOptions, ListenOptions };
// Re-export rpc surface so callers of @peerrpc/peerrpc don't have to
// also depend on @peerrpc/rpc for the common types.
export { Client, Server, ServerStream, MethodKind, ok, err };
export type { ServiceDesc, Handler, Status };

// ----- random peer_id -----

function randomPeerId(): string {
  // crypto.randomUUID is available on Node ≥ 19 and on every modern
  // browser. The fallback is only hit on very old runtimes; we
  // prefer a graceful degrade over a hard dependency on a polyfill.
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return "p-" + Math.random().toString(36).slice(2) + Date.now().toString(36);
}

// ----- resolver: Target → SignalTransport -----

function buildSignalTransport(target: Target): { transport: SignalTransport; close: () => void } {
  // Backfill a generated peer_id onto the target so callers (dial)
  // can report it even when the target URI omitted ?peer=.
  if (!target.peerId) {
    target.peerId = randomPeerId();
  }
  const peerId = target.peerId;
  switch (target.scheme) {
    case "local":
      return localBus.join(target.service, peerId);
    case "connect": {
      const sig = new ConnectSignal({
        url: target.signal.startsWith("http") ? target.signal : `https://${target.signal}`,
        service: target.service,
        peerId,
        token: target.token,
      });
      return {
        transport: sig,
        close: () => sig.close(),
      };
    }
    case "ws": {
      // Default to cleartext ws:// for loopback hosts (dev convenience:
      // avoids the self-signed cert dance) and wss:// elsewhere. Callers
      // can force either by passing a full ws://host or wss://host in
      // the target authority.
      const wsScheme = target.signal.startsWith("ws")
        ? ""
        : isLoopbackAuthority(target.signal) ? "ws://" : "wss://";
      const url = target.signal.startsWith("ws")
        ? (target.signal.endsWith("/ws") ? target.signal : `${target.signal}/ws`)
        : `${wsScheme}${target.signal}/ws`;
      const sig = new WebSocketSignal({
        url,
        service: target.service,
        peerId,
      });
      return {
        transport: sig,
        close: () => sig.close(),
      };
    }
    case "relay":
      throw new Error("peerrpc: relay scheme not yet implemented");
    default:
      throw new Error(`peerrpc: unknown scheme ${JSON.stringify(target.scheme)}`);
  }
}

// ----- Conn (client side) -----

export interface Conn {
  /** The RPC client for outbound calls. */
  client: Client;
  /** Auto-generated or explicit peer_id used on the wire. */
  peerId: string;
  /** Tear down peer + signal + transport. Idempotent. */
  close: () => Promise<void>;
}

/**
 * Dial the Offerer side. Returns once the DataChannel is open and
 * the RPC client is ready to issue Invoke* calls.
 *
 * For non-local schemes, this also calls signal.connect() before
 * starting the WebRTC negotiation. The returned close() releases
 * every layer.
 */
export async function dial(target: string | Target, opts: DialOptions = {}): Promise<Conn> {
  const t = typeof target === "string" ? parseTarget(target) : target;
  applyOpts(t, opts);

  const { transport, close: closeTransport } = buildSignalTransport(t);

  // Connect signaling first (only the network backends do anything
  // here; local is a no-op).
  await maybeConnectSignal(transport);

  let peerCfg: PeerConfig | undefined = opts.peer;
  try {
    const { peer, channel } = await peerDial(transport, peerCfg ?? {});
    const client = new Client(channel);
    return {
      client,
      peerId: t.peerId!,
      close: async () => {
        peer.close();
        closeTransport();
      },
    };
  } catch (err) {
    closeTransport();
    throw err;
  }
}

// ----- Listener (server side) -----

export interface ServerConn {
  /** The Channel to hand to an rpc.Server. */
  channel: Channel;
  /** peer_id used on the wire for this accepted peer. */
  peerId: string;
  /** Tear down this single accepted connection. */
  close: () => Promise<void>;
}

export interface Listener {
  /** Block until a remote Dialer connects; return the channel. */
  accept: () => Promise<ServerConn>;
  /** Convenience loop: accept forever, hand each conn to a fresh
   * Server built by factory, run srv.serve in a background promise. */
  serve: (factory: () => import("@peerrpc/rpc").Server) => Promise<void>;
  /** Stop accepting and release signal resources. */
  close: () => Promise<void>;
}

export async function listen(target: string | Target, opts: ListenOptions = {}): Promise<Listener> {
  const t = typeof target === "string" ? parseTarget(target) : target;
  applyOpts(t, opts);

  // Per-Accept closure so each call mints its own peer_id and
  // signal client.
  const acceptOnce = async (): Promise<ServerConn> => {
    const callTarget: Target = {
      ...t,
      peerId: t.peerId ? `${t.peerId}-${shortId()}` : randomPeerId(),
    };
    const { transport, close: closeTransport } = buildSignalTransport(callTarget);
    await maybeConnectSignal(transport);

    try {
      const { peer, channel } = await peerAccept(transport, opts.peer ?? {});
      return {
        channel,
        peerId: callTarget.peerId!,
        close: async () => {
          peer.close();
          closeTransport();
        },
      };
    } catch (err) {
      closeTransport();
      throw err;
    }
  };

  // Track active connections and the serve loop so close() can tear
  // everything down instead of leaving live DataChannels open.
  const activeConns = new Set<ServerConn>();
  let closed = false;

  // The Listener object we hand back. accept() is exposed directly;
  // serve() loops over accept and runs a fresh rpc.Server per conn.
  // We use a self-reference so serve() can call accept() without
  // recursion gymnastics.
  const listener: Listener = {
    accept: async () => {
      if (closed) {
        throw new Error("peerrpc: listener closed");
      }
      const conn = await acceptOnce();
      activeConns.add(conn);
      return conn;
    },
    serve: async (factory) => {
      // Sequential loop; concurrent Accept within one service races
      // on the store's broadcast-to-others semantics.
      while (!closed) {
        let conn: ServerConn;
        try {
          conn = await acceptOnce();
        } catch {
          // acceptOnce rejects when the listener is closed (the
          // underlying signal transport errors out). Stop the loop.
          break;
        }
        activeConns.add(conn);
        const srv = factory();
        // Run in the background; do not block the next Accept. Remove
        // the conn from the active set once serve returns (channel
        // closed) so close() does not try to close it twice.
        srv.serve(conn.channel).catch(() => { /* best-effort */ }).finally(() => {
          activeConns.delete(conn);
        });
      }
    },
    close: async () => {
      if (closed) return;
      closed = true;
      // Tear down every live connection (peer + signal transport).
      // This closes the DataChannel, which unblocks the client's
      // recv() and surfaces the disconnect.
      await Promise.all(
        [...activeConns].map((conn) => conn.close().catch(() => { /* best-effort */ })),
      );
      activeConns.clear();
    },
  };
  return listener;
}

// Eliminate the dead helper functions from the previous draft; the
// logic above is self-contained.

// ----- helpers -----

function applyOpts(t: Target, opts: DialOptions | ListenOptions): void {
  if (opts.token && !t.token) t.token = opts.token;
  if (opts.peerId && !t.peerId) t.peerId = opts.peerId;
}

async function maybeConnectSignal(transport: SignalTransport): Promise<void> {
  // The local scheme's transport is already "connected" (in-process).
  // The network schemes (ConnectSignal, WebSocketSignal) expose
  // connect() via duck-typing.
  const maybe = transport as unknown as { connect?: () => Promise<void> };
  if (typeof maybe.connect === "function") {
    await maybe.connect();
  }
}

function shortId(): string {
  const id = randomPeerId();
  return id.length > 8 ? id.slice(0, 8) : id;
}

// isLoopbackAuthority returns true for localhost / 127.0.0.1 / ::1
// authorities, used to pick ws:// (cleartext) over wss:// for local dev.
function isLoopbackAuthority(authority: string): boolean {
  const host = authority.split(":")[0];
  return host === "localhost" || host === "127.0.0.1" || host === "[::1]" || host === "::1";
}
