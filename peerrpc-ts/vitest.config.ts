import { defineConfig } from "vitest/config";
import { resolve } from "path";
import { fileURLToPath } from "url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));

export default defineConfig({
  resolve: {
    alias: [
      {
        find: /^@peerrpc\/protocol\/gen\/(.*)$/,
        replacement: resolve(__dirname, "packages/peerrpc-protocol/src/gen/$1"),
      },
      {
        find: "@peerrpc/protocol",
        replacement: resolve(__dirname, "packages/peerrpc-protocol/src/index.ts"),
      },
      {
        find: "@peerrpc/transport",
        replacement: resolve(__dirname, "packages/peerrpc-transport/src/index.ts"),
      },
      {
        find: "@peerrpc/peer",
        replacement: resolve(__dirname, "packages/peerrpc-peer/src/index.ts"),
      },
      {
        find: "@peerrpc/rpc",
        replacement: resolve(__dirname, "packages/peerrpc-rpc/src/index.ts"),
      },
    ],
  },
  test: {
    include: ["packages/**/*.test.ts"],
    environment: "node",
  },
});
