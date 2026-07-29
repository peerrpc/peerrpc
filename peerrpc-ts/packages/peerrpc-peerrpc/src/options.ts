/**
 * Options shared by dial and listen.
 *
 * Options are applied in order; later options override earlier ones
 * for the same field.
 */

import type { PeerConfig } from "@peerrpc/peer";

export interface DialOptions {
  /** Override PeerConfig (ICE servers, timeout, ...). */
  peer?: PeerConfig;
  /** Bearer token; also settable via Target.token. */
  token?: string;
  /** Explicit peer_id; otherwise auto-generated. */
  peerId?: string;
}

export interface ListenOptions {
  /** Override PeerConfig (ICE servers, timeout, ...). */
  peer?: PeerConfig;
  /** Bearer token; also settable via Target.token. */
  token?: string;
  /** Explicit peer_id; otherwise auto-generated per Accept. */
  peerId?: string;
}
