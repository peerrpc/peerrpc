import { defineConfig } from "vite";

export default defineConfig({
  root: ".",
  server: {
    port: 5173,
  },
  resolve: {
    // Monorepo: alias @peerrpc/* packages to the workspace source.
    alias: {
      "@peerrpc/protocol": "../../../peerrpc-ts/packages/peerrpc-protocol/src/index.ts",
      "@peerrpc/transport": "../../../peerrpc-ts/packages/peerrpc-transport/src/index.ts",
      "@peerrpc/peer": "../../../peerrpc-ts/packages/peerrpc-peer/src/index.ts",
      "@peerrpc/rpc": "../../../peerrpc-ts/packages/peerrpc-rpc/src/index.ts",
    },
  },
});
