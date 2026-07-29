import { defineConfig } from "vite";
import { resolve } from "path";
import { fileURLToPath } from "url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const root = resolve(__dirname, "../../..");

export default defineConfig({
  root: ".",
  server: {
    port: 5173,
    proxy: {
      "/signal-ws": {
        target: "http://localhost:8080",
        ws: true,
        rewrite: (path) => path.replace("/signal-ws", "/ws"),
      },
    },
  },
  build: {
    rollupOptions: {
      input: {
        server: resolve(__dirname, "server.html"),
        client: resolve(__dirname, "client.html"),
      },
    },
  },
  resolve: {
    alias: [
      {
        find: /^@peerrpc\/protocol\/gen\/(.*)$/,
        replacement: resolve(root, "peerrpc-ts/packages/peerrpc-protocol/src/gen/$1"),
      },
      {
        find: "@peerrpc/protocol",
        replacement: resolve(root, "peerrpc-ts/packages/peerrpc-protocol/src/index.ts"),
      },
      {
        find: "@peerrpc/transport",
        replacement: resolve(root, "peerrpc-ts/packages/peerrpc-transport/src/index.ts"),
      },
      {
        find: "@peerrpc/peer",
        replacement: resolve(root, "peerrpc-ts/packages/peerrpc-peer/src/index.ts"),
      },
      {
        find: "@peerrpc/rpc",
        replacement: resolve(root, "peerrpc-ts/packages/peerrpc-rpc/src/index.ts"),
      },
      {
        find: "@peerrpc/signal",
        replacement: resolve(root, "peerrpc-ts/packages/peerrpc-signal/src/index.ts"),
      },
      {
        find: "@connectrpc/connect-web",
        replacement: resolve(root, "peerrpc-ts/node_modules/@connectrpc/connect-web"),
      },
      {
        find: "@connectrpc/connect",
        replacement: resolve(root, "peerrpc-ts/node_modules/@connectrpc/connect"),
      },
      {
        find: "@bufbuild/protobuf",
        replacement: resolve(root, "peerrpc-ts/node_modules/@bufbuild/protobuf"),
      },
    ],
  },
});
