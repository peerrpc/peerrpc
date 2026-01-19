import { describe, it, expect } from "vitest";
import {
  encodeFrame,
  tryDecodeFrame,
  encodeResponseFrame,
  tryDecodeResponseFrame,
  lengthPrefix,
} from "./index.js";
import { Frame, ResponseFrame } from "./gen/peerrpc/peerrpc_pb.js";

describe("lengthPrefix", () => {
  it("prepends 4-byte big-endian length", () => {
    const payload = new Uint8Array([0x01, 0x02, 0x03]);
    const out = lengthPrefix(payload);
    expect(out.length).toBe(7);
    expect(out[0]).toBe(0);
    expect(out[1]).toBe(0);
    expect(out[2]).toBe(0);
    expect(out[3]).toBe(3);
    expect(out[4]).toBe(1);
    expect(out[5]).toBe(2);
    expect(out[6]).toBe(3);
  });

  it("handles empty payload", () => {
    const out = lengthPrefix(new Uint8Array());
    expect(out.length).toBe(4);
    expect(out[3]).toBe(0);
  });
});

describe("encodeFrame / tryDecodeFrame round-trip", () => {
  it("round-trips a Call frame with inline data", () => {
    const frame = new Frame({
      routing: { sequence: 1 },
      type: {
        case: "call",
        value: {
          method: "/echo.Echo/Echo",
          protocolVersion: 1,
          inlineData: new Uint8Array([0x68, 0x69]),
        },
      },
    });
    const encoded = encodeFrame(frame);
    const result = tryDecodeFrame(encoded);
    expect(result).not.toBeNull();
    expect(result!.consumed).toBe(encoded.length);
    expect(result!.frame.type.case).toBe("call");
    expect(result!.frame.type.value!.method).toBe("/echo.Echo/Echo");
  });

  it("returns null for partial buffer", () => {
    const frame = new Frame({
      routing: { sequence: 2 },
      type: { case: "end", value: { closeSend: true } },
    });
    const encoded = encodeFrame(frame);
    const partial = encoded.subarray(0, 5);
    expect(tryDecodeFrame(partial)).toBeNull();
  });

  it("handles two back-to-back frames", () => {
    const f1 = new Frame({
      routing: { sequence: 1 },
      type: { case: "call", value: { method: "/a/B", protocolVersion: 1 } },
    });
    const f2 = new Frame({
      routing: { sequence: 3 },
      type: { case: "call", value: { method: "/c/D", protocolVersion: 1 } },
    });
    const e1 = encodeFrame(f1);
    const e2 = encodeFrame(f2);
    const combined = new Uint8Array(e1.length + e2.length);
    combined.set(e1, 0);
    combined.set(e2, e1.length);

    const r1 = tryDecodeFrame(combined);
    expect(r1).not.toBeNull();
    expect(r1!.frame.routing!.sequence).toBe(1);

    const remaining = combined.subarray(r1!.consumed);
    const r2 = tryDecodeFrame(remaining);
    expect(r2).not.toBeNull();
    expect(r2!.frame.routing!.sequence).toBe(3);
  });
});

describe("encodeResponseFrame / tryDecodeResponseFrame", () => {
  it("round-trips an End frame with OK status", () => {
    const frame = new ResponseFrame({
      routing: { sequence: 1 },
      type: {
        case: "end",
        value: {
          status: { code: 0, message: "" },
        },
      },
    });
    const encoded = encodeResponseFrame(frame);
    const result = tryDecodeResponseFrame(encoded);
    expect(result).not.toBeNull();
    expect(result!.frame.type.case).toBe("end");
    expect(result!.frame.type.value!.status!.code).toBe(0);
  });

  it("round-trips a Begin frame with header", () => {
    const frame = new ResponseFrame({
      routing: { sequence: 5 },
      type: {
        case: "begin",
        value: {
          header: { md: { "x-trace": { values: ["abc123"] } } },
        },
      },
    });
    const encoded = encodeResponseFrame(frame);
    const result = tryDecodeResponseFrame(encoded);
    expect(result).not.toBeNull();
    expect(result!.frame.type.case).toBe("begin");
    expect(result!.frame.type.value!.header!.md["x-trace"]!.values[0]).toBe("abc123");
  });
});
