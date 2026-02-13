/** globalTeardown: stop the Go interop server started by globalSetup. */

import { serverProcess } from "./global-setup.js";

export default async function globalTeardown() {
  if (serverProcess) {
    console.log("[teardown] stopping interop server...");
    serverProcess.kill("SIGTERM");
    await new Promise((r) => setTimeout(r, 1000));
    if (!serverProcess.killed) {
      serverProcess.kill("SIGKILL");
    }
  }
}
