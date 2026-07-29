/**
 * @peerrpc/rpc: Client and Server multiplexers over a transport.Channel.
 *
 * Re-exports the Client family (this directory's index.ts) and the
 * Server family (server.ts) so callers can do:
 *
 *   import { Client, Server, Status, ok } from "@peerrpc/rpc";
 *
 * The Client-side code lives in index.ts for historical reasons
 * (it was the only thing the package shipped at v1). The barrel
 * keeps imports stable.
 */

export {
  Client,
  ClientStream,
  Code,
  type Status,
  type Metadata,
} from "./index.js";

export {
  Server,
  ServerStream,
  MethodKind,
  type ServiceDesc,
  type Handler,
  ok,
  err,
} from "./server.js";
