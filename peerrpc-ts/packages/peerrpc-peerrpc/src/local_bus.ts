/**
 * In-process signal backend for the local scheme.
 *
 * Mirrors Go's signal.Local: a singleton broadcast bus keyed by
 * service. Two callers that dial the same service rendezvous here
 * without going over any network. Used by tests and single-binary
 * demos.
 *
 * This module owns ONE LocalBus instance exported as `localBus`;
 * all dials/listens on the local scheme share it.
 */

import type { SignalMessage, SignalTransport } from "@peerrpc/peer";

interface PeerEntry {
  peerId: string;
  deliver: (msg: SignalMessage) => void;
}

class LocalBus {
  private services = new Map<string, Set<PeerEntry>>();

  /**
   * Join service under peerId. Returns a SignalTransport whose
   * send() broadcasts to every other peer in the service, plus a
   * leave() that removes the peer from the service.
   */
  join(service: string, peerId: string): {
    transport: SignalTransport;
    leave: () => void;
  } {
    let peers = this.services.get(service);
    if (!peers) {
      peers = new Set();
      this.services.set(service, peers);
    }

    let onMessageCb: ((msg: SignalMessage) => void) | null = null;
    const entry: PeerEntry = {
      peerId,
      deliver: (msg) => onMessageCb?.(msg),
    };
    peers.add(entry);

    const transport: SignalTransport = {
      send: (msg) => {
        // broadcast to others.
        for (const p of peers!) {
          if (p === entry) continue;
          p.deliver({ ...msg, from: peerId });
        }
      },
      onMessage: (cb) => {
        onMessageCb = cb;
      },
      close: () => leave(),
    };

    const leave = () => {
      peers!.delete(entry);
      if (peers!.size === 0) {
        this.services.delete(service);
      }
    };

    return { transport, leave };
  }

  /** Test helper: current peer count for a service. */
  peerCount(service: string): number {
    return this.services.get(service)?.size ?? 0;
  }
}

/** Singleton bus. All local-scheme dials/listens share this. */
export const localBus = new LocalBus();
