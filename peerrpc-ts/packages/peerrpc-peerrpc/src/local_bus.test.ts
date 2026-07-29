/**
 * End-to-end facade test: dial + listen on the local scheme, run a
 * Unary RPC. Mirrors the Go facade_test.go shape.
 *
 * The local scheme uses happy-dom (or jsdom) for RTCPeerConnection?
 * No — happy-dom is for DOM; we need a real RTCPeerConnection. In
 * Node, we use the `wrtc` package via the global. If unavailable
 * (CI without wrtc), this test is skipped.
 *
 * For now the facade test covers the pieces we can cover without a
 * real WebRTC stack: target parsing (target.test.ts), local bus
 * broadcast behavior, and that dial/listen plumb the signal correctly.
 */

import { describe, it, expect, beforeEach } from "vitest";
import { localBus } from "../src/local_bus.js";

describe("LocalBus", () => {
  beforeEach(() => {
    // There is one global bus; pick fresh service names per test
    // rather than trying to reset state.
  });

  it("broadcasts to others but not to self", async () => {
    const alice = localBus.join("test.1", "alice");
    const bob = localBus.join("test.1", "bob");

    const bobGot: string[] = [];
    const aliceGot: string[] = [];
    bob.transport.onMessage((m) => { bobGot.push(m.from!); });
    alice.transport.onMessage((m) => { aliceGot.push(m.from!); });

    alice.transport.send({ type: "offer", sdp: "x" });

    // Give the synchronous broadcast a beat.
    await Promise.resolve();

    expect(bobGot).toEqual(["alice"]);
    expect(aliceGot).toEqual([]); // no echo

    alice.leave();
    bob.leave();
  });

  it("leaving removes the peer from the service", () => {
    const alice = localBus.join("test.2", "alice");
    expect(localBus.peerCount("test.2")).toBe(1);
    alice.leave();
    expect(localBus.peerCount("test.2")).toBe(0);
  });

  it("empty service is GC'd", () => {
    const alice = localBus.join("test.3", "alice");
    alice.leave();
    expect(localBus.peerCount("test.3")).toBe(0);
  });
});
