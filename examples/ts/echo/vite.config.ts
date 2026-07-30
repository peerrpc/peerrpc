import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";

// Resolve @peerrpc/* directly to the SDK source so the sample has no
// build-step dependency on peerrpc-ts's tsc output and no fragile
// cross-workspace node_modules linking. Vite compiles the .ts sources
// on the fly. Deep imports like @peerrpc/protocol/gen/peerrpc/x_pb.js
// resolve to the .ts source via the trailing-slash alias + Vite's
// resolve.extensions.
const ROOT = fileURLToPath(new URL("../../../peerrpc-ts/packages/", import.meta.url));

export default defineConfig({
  resolve: {
    alias: [
      { find: /^@peerrpc\/peerrpc$/, replacement: ROOT + "peerrpc-peerrpc/src/index.ts" },
      { find: /^@peerrpc\/rpc$/, replacement: ROOT + "peerrpc-rpc/src/main.ts" },
      { find: /^@peerrpc\/signal$/, replacement: ROOT + "peerrpc-signal/src/index.ts" },
      { find: /^@peerrpc\/peer$/, replacement: ROOT + "peerrpc-peer/src/index.ts" },
      { find: /^@peerrpc\/transport$/, replacement: ROOT + "peerrpc-transport/src/index.ts" },
      { find: /^@peerrpc\/protocol\//, replacement: ROOT + "peerrpc-protocol/src/" },
      { find: /^@peerrpc\/protocol$/, replacement: ROOT + "peerrpc-protocol/src/index.ts" },
    ],
    extensions: [".ts", ".js", ".json"],
  },
  server: {
    fs: {
      // The SDK sources live outside the sample directory; allow Vite
      // to serve them so the source alias works in dev mode.
      allow: ["..", "../..", "../../.."],
    },
  },
  build: {
    outDir: "dist",
  },
});