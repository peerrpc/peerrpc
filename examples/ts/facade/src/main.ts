/**
 * v2 facade demo: shows the new peerrpc.dial / peerrpc.listen API
 * for TypeScript.
 *
 * Compared to the v1 chat example (which copy-pastes ~80 lines of
 * signal-pump code per client and another ~100 for the server),
 * the v2 facade does the same work in ~20 lines and exposes no
 * magic strings (no "demo-room", no "offerer"/"answerer").
 *
 * Requires `wrtc` for Node.js RTCPeerConnection (the browser
 * doesn't need a polyfill; see examples/ts/chat for the browser
 * path with @peerrpc/react).
 *
 *   npm i
 *   npm run dev
 */

import { peerrpc } from "@peerrpc/peerrpc";
import { Server, ok, MethodKind } from "@peerrpc/rpc";

function registerEcho(srv: Server): void {
  srv.registerService({
    serviceName: "echo.Echo",
    methods: [
      {
        method: "Unary",
        kind: MethodKind.Unary,
        handler: async (stream) => {
          const req = await stream.recv();
          if (!req) return { code: 13, message: "empty request" };
          await stream.send(concat(new TextEncoder().encode("echo: "), req));
          return ok();
        },
      },
    ],
  });
}

function concat(prefix: Uint8Array, body: Uint8Array): Uint8Array {
  const out = new Uint8Array(prefix.length + body.length);
  out.set(prefix, 0);
  out.set(body, prefix.length);
  return out;
}

async function main(): Promise<void> {
  const target = "peerrpc+local:///echo.Echo";

  // Server side: one call, returns immediately; serve blocks in a
  // background promise until the process exits.
  const ln = await peerrpc.listen(target);
  ln.serve(() => {
    const srv = new Server();
    registerEcho(srv);
    return srv;
  }).catch(() => { /* best-effort */ });

  // Client side: one dial, then issue a Unary RPC.
  const conn = await peerrpc.dial(target);
  console.log("connected as", conn.peerId);

  const req = new TextEncoder().encode("hello, peerrpc");
  const { response, status } = await conn.client.invokeUnary("/echo.Echo/Unary", req);
  if (status.code !== 0) {
    console.error("RPC failed", status);
    process.exit(1);
  }
  console.log("Unary OK:", new TextDecoder().decode(response));

  await conn.close();
  await ln.close();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
