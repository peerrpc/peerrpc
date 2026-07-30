import { defineConfig } from "vite";
import { fileURLToPath } from "node:url";

const ROOT = fileURLToPath(new URL("../../peerrpc-ts/packages/", import.meta.url));

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
      allow: ["..", "../..", "../../.."],
    },
  },
  build: {
    outDir: "dist",
  },
});