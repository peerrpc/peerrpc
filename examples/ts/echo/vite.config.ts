import { defineConfig } from "vite";
import { resolve } from "path";
import { fileURLToPath } from "url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  root: ".",
  server: {
    port: 5173,
  },
  resolve: {
    // Monorepo: alias @peerrpc/* packages to the workspace source.
    // We alias both the bare package root AND the /gen subpath so
    // imports like @peerrpc/protocol/gen/... resolve correctly.
    alias: [
      {
        find: /^@peerrpc\/protocol\/gen\/(.*)$/,
        replacement: resolve(__dirname, "../../../peerrpc-ts/packages/peerrpc-protocol/src/gen/$1"),
      },
      {
        find: "@peerrpc/protocol",
        replacement: resolve(__dirname, "../../../peerrpc-ts/packages/peerrpc-protocol/src/index.ts"),
      },
      {
        find: "@peerrpc/transport",
        replacement: resolve(__dirname, "../../../peerrpc-ts/packages/peerrpc-transport/src/index.ts"),
      },
      {
        find: "@peerrpc/peer",
        replacement: resolve(__dirname, "../../../peerrpc-ts/packages/peerrpc-peer/src/index.ts"),
      },
      {
        find: "@peerrpc/rpc",
        replacement: resolve(__dirname, "../../../peerrpc-ts/packages/peerrpc-rpc/src/index.ts"),
      },
    ],
  },
});
