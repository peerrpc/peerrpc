import { defineConfig } from "vite";

// Allow Vite to resolve @peerrpc/* from the SDK workspace outside this
// project root (the file: deps pull source from ../../../peerrpc-ts).
export default defineConfig({
  server: {
    fs: {
      allow: ["..", "../.."],
    },
  },
  build: {
    outDir: "dist",
  },
});
