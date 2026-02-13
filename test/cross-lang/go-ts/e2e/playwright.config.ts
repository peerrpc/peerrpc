import { defineConfig, devices } from "@playwright/test";

/**
 * Playwright config for Go ↔ TS cross-language interop E2E tests.
 *
 * globalSetup (global-setup.ts) builds the TS demo, generates a
 * self-signed TLS cert, and starts the Go interop server. globalTeardown
 * tears everything down.
 *
 * Each test navigates to the server's HTTPS URL with
 * ignoreHTTPSErrors: true so self-signed certs work.
 */
export default defineConfig({
  testDir: "./tests",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  timeout: 60_000,
  expect: { timeout: 15_000 },
  globalSetup: "./global-setup.ts",
  globalTeardown: "./global-teardown.ts",
  use: {
    baseURL: process.env.PEERRPC_INTEROP_URL ?? "http://localhost:30443",
    headless: true,
    launchOptions: {
      args: [
        "--use-fake-ui-for-media-stream",
        "--no-sandbox",
      ],
    },
  },
  projects: [
    {
      name: "chromium",
      use: {
        ...devices["Desktop Chrome"],
      },
    },
  ],
});
