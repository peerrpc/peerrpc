import { test, expect, type Page } from "@playwright/test";

/**
 * Go ↔ TS cross-language interop tests.
 *
 * Tests run serially and share a single browser page so the WebRTC
 * DataChannel established in beforeAll persists across tests. The
 * Go server's offerer accepts one connection; we reuse it for all
 * tests to avoid needing reconnection between tests.
 *
 * The Go server registers an EchoService that:
 *   - /echo.Echo/Echo: echoes "echo: <request>"
 *   - /echo.Echo/Stream: emits 5 chunks "chunk N for <request>"
 */

const CONNECT_TIMEOUT = 30_000;

test.describe.configure({ mode: "serial" });

let page: Page;

test.beforeAll(async ({ browser }) => {
  page = await browser.newPage();
  page.on("pageerror", (err) => console.log(`[browser:error] ${err}`));
  await page.goto("/");
  await expect(page.locator("#connect-btn")).toBeVisible();
  await page.click("#connect-btn");
  await expect(page.locator("#status")).toHaveText("Connected", {
    timeout: CONNECT_TIMEOUT,
  });
  await expect(page.locator("#rpc-section")).toBeVisible();
});

test.afterAll(async () => {
  await page.close();
});

test.describe("Go ↔ TS interop", () => {
  test("Unary RPC echoes request", async () => {
    const msg = `interop-unary-${Date.now()}`;
    await page.fill("#unary-input", msg);
    await page.click("#unary-btn");

    await expect(page.locator("#unary-output")).toContainText(
      `echo: ${msg}`,
      { timeout: 10_000 }
    );
  });

  test("Server Streaming RPC delivers 5 chunks", async () => {
    const msg = `interop-stream-${Date.now()}`;
    await page.fill("#stream-input", msg);
    await page.click("#stream-btn");

    const output = page.locator("#stream-output");
    for (let i = 1; i <= 5; i++) {
      await expect(output).toContainText(`chunk ${i}`, { timeout: 10_000 });
    }
  });

  test("supports multiple sequential Unary RPCs", async () => {
    for (let i = 0; i < 3; i++) {
      const msg = `seq-${i}-${Date.now()}`;
      await page.fill("#unary-input", msg);
      await page.click("#unary-btn");
      await expect(page.locator("#unary-output")).toContainText(
        `echo: ${msg}`,
        { timeout: 10_000 }
      );
    }
  });
});
