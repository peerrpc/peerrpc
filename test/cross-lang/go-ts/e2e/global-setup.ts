/**
 * globalSetup: build the TS demo, generate a self-signed cert, and
 * start the Go interop server. Exposes the server URL via an
 * environment variable for the tests.
 */

import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdirSync } from "node:fs";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "../../../..");
const TS_DEMO = join(ROOT, "examples/ts/echo");
const GO_INTEROP = resolve(import.meta.dirname, "..");
const TLS_DIR = join(GO_INTEROP, ".tls");
const PORT = 30443;
const URL = `http://localhost:${PORT}`;

let serverProcess: ReturnType<typeof spawn> | null = null;

function exec(cmd: string, cwd: string) {
  const parts = cmd.split(" ");
  const result = spawnSync(parts[0], parts.slice(1), {
    cwd,
    stdio: "pipe",
    env: process.env,
  });
  if (result.status !== 0) {
    const err = result.stderr?.toString() ?? "unknown error";
    throw new Error(`command "${cmd}" failed in ${cwd}: ${err}`);
  }
}

export default async function globalSetup() {
  // 1. Build the TS demo.
  console.log("[setup] building TS demo...");
  exec("npx vite build", TS_DEMO);

  // 2. Build the Go interop server.
  console.log("[setup] building Go interop server...");
  const binary = join(GO_INTEROP, ".bin/peerrpc-interop-ts");
  mkdirSync(join(GO_INTEROP, ".bin"), { recursive: true });
  exec(`go build -o ${binary} .`, GO_INTEROP);

  // 3. Start the Go interop server (HTTP, no TLS — the SSE signaling
  //    path works over HTTP/1.1 so no TLS is needed for the browser).
  console.log(`[setup] starting interop server on ${URL}...`);
  const distDir = join(TS_DEMO, "dist");
  serverProcess = spawn(
    binary,
    ["-addr", `:${PORT}`, "-static", distDir],
    { cwd: GO_INTEROP, stdio: "pipe" }
  );

  serverProcess.stdout?.on("data", (d) => process.stdout.write(`[server] ${d}`));
  serverProcess.stderr?.on("data", (d) => process.stderr.write(`[server] ${d}`));

  // 5. Wait for /healthz to return 200.
  const deadline = Date.now() + 15_000;
  while (Date.now() < deadline) {
    try {
      const resp = await fetch(`${URL}/healthz`);
      if (resp.ok) {
        console.log("[setup] server is ready");
        break;
      }
    } catch {
      // server not ready yet
    }
    await new Promise((r) => setTimeout(r, 500));
  }

  if (serverProcess.killed || serverProcess.exitCode !== null) {
    throw new Error("[setup] server exited before becoming healthy");
  }

  process.env.PEERRPC_INTEROP_URL = URL;
}

// Keep the server process reference for teardown.
export { serverProcess };
