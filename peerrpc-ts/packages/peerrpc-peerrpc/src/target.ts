/**
 * Target URI parsing for peerrpc.
 *
 * Grammar (mirrors the Go facade):
 *
 *   peerrpc+<scheme>://<authority>/<service>[?<opts>]
 *
 * scheme:
 *   local    → in-process signaling (no network)
 *   connect  → Connect-RPC over HTTP/2 (Node / non-browser)
 *   ws       → WebSocket (browser)
 *   relay    → explicit relay hop (not yet implemented)
 *
 * authority: signal-server host (ignored for local).
 * service:   rendezvous key.
 *
 * query opts (all optional):
 *   ?as=client|server   role hint
 *   ?peer=<id>          peer_id; defaults to an auto-generated UUID
 *   ?token=<jwt>        bearer token
 */

export type Scheme = "local" | "connect" | "ws" | "relay";

export type RoleHint = "client" | "server";

export interface Target {
  scheme: Scheme;
  /** signal-server authority (host[:port]); empty for local. */
  signal: string;
  /** rendezvous key. */
  service: string;
  role?: RoleHint;
  peerId?: string;
  token?: string;
}

export class TargetParseError extends Error {
  constructor(msg: string) {
    super(msg);
    this.name = "TargetParseError";
  }
}

export function parseTarget(uri: string): Target {
  const prefix = "peerrpc+";
  if (!uri.startsWith(prefix)) {
    throw new TargetParseError(
      `target URI must start with ${JSON.stringify(prefix)}, got ${JSON.stringify(uri)}`,
    );
  }
  const rest = uri.slice(prefix.length); // "connect://host/service?..."

  const schemeSep = "://";
  const sepIdx = rest.indexOf(schemeSep);
  if (sepIdx < 0) {
    throw new TargetParseError(
      `target URI missing ${JSON.stringify(schemeSep)} after scheme`,
    );
  }
  const scheme = rest.slice(0, sepIdx) as Scheme;
  if (!isScheme(scheme)) {
    throw new TargetParseError(`unknown scheme: ${JSON.stringify(scheme)}`);
  }
  const afterScheme = rest.slice(sepIdx + schemeSep.length);

  // authority / service+query split.
  const slashIdx = afterScheme.indexOf("/");
  let authority: string;
  let pathQuery: string;
  if (slashIdx < 0) {
    authority = afterScheme;
    pathQuery = "";
  } else {
    authority = afterScheme.slice(0, slashIdx);
    pathQuery = afterScheme.slice(slashIdx + 1);
  }

  // service / query split.
  let service: string;
  let rawQuery: string;
  const qIdx = pathQuery.indexOf("?");
  if (qIdx >= 0) {
    service = pathQuery.slice(0, qIdx);
    rawQuery = pathQuery.slice(qIdx + 1);
  } else {
    service = pathQuery;
    rawQuery = "";
  }
  if (!service) {
    throw new TargetParseError(
      `target URI missing service path (e.g. /echo.Echo) in ${JSON.stringify(uri)}`,
    );
  }

  const t: Target = { scheme, signal: authority, service };
  if (rawQuery) {
    for (const pair of rawQuery.split("&")) {
      const eq = pair.indexOf("=");
      if (eq < 0) continue;
      const k = decodeURIComponent(pair.slice(0, eq));
      const v = decodeURIComponent(pair.slice(eq + 1));
      if (k === "as") t.role = v as RoleHint;
      else if (k === "peer") t.peerId = v;
      else if (k === "token") t.token = v;
    }
  }
  if (!t.signal && t.scheme !== "local") {
    throw new TargetParseError(
      `scheme ${JSON.stringify(t.scheme)} requires a non-empty authority`,
    );
  }
  return t;
}

function isScheme(s: string): s is Scheme {
  return s === "local" || s === "connect" || s === "ws" || s === "relay";
}

/** Render a Target back to its canonical URI form. */
export function formatTarget(t: Target): string {
  const parts: string[] = [`peerrpc+${t.scheme}://`];
  if (t.signal) parts.push(t.signal);
  parts.push("/");
  parts.push(t.service);
  const q: string[] = [];
  if (t.role) q.push(`as=${encodeURIComponent(t.role)}`);
  if (t.peerId) q.push(`peer=${encodeURIComponent(t.peerId)}`);
  if (t.token) q.push(`token=${encodeURIComponent(t.token)}`);
  if (q.length > 0) parts.push("?", q.join("&"));
  return parts.join("");
}
