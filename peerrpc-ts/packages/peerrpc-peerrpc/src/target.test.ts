import { describe, it, expect } from "vitest";
import { parseTarget, formatTarget, TargetParseError, type Target } from "../src/target.js";

describe("parseTarget", () => {
  it("parses ws with port and query", () => {
    const t = parseTarget(
      "peerrpc+ws://signal.example.com:8443/echo.Echo?as=client&peer=alice&token=jwt",
    );
    expect(t).toEqual<Target>({
      scheme: "ws",
      signal: "signal.example.com:8443",
      service: "echo.Echo",
      role: "client",
      peerId: "alice",
      token: "jwt",
    });
  });

  it("parses local with empty authority", () => {
    expect(parseTarget("peerrpc+local:///echo.Echo")).toEqual<Target>({
      scheme: "local",
      signal: "",
      service: "echo.Echo",
    });
  });

  it("parses ws", () => {
    expect(parseTarget("peerrpc+ws://signal.example.com/echo.Echo")).toEqual<Target>({
      scheme: "ws",
      signal: "signal.example.com",
      service: "echo.Echo",
    });
  });

  it("parses bare host no port", () => {
    expect(parseTarget("peerrpc+ws://signal.example.com/echo.Echo")).toEqual<Target>({
      scheme: "ws",
      signal: "signal.example.com",
      service: "echo.Echo",
    });
  });

  it("rejects missing prefix", () => {
    expect(() => parseTarget("ws://x/y")).toThrow(TargetParseError);
  });

  it("rejects missing service", () => {
    expect(() => parseTarget("peerrpc+ws://signal.example.com")).toThrow(TargetParseError);
    expect(() => parseTarget("peerrpc+ws://signal.example.com/")).toThrow(TargetParseError);
  });

  it("rejects non-local without authority", () => {
    expect(() => parseTarget("peerrpc+ws:///echo.Echo")).toThrow(TargetParseError);
  });

  it("rejects unknown scheme", () => {
    expect(() => parseTarget("peerrpc+bogus://x/y")).toThrow(TargetParseError);
  });

  it("round-trips via formatTarget", () => {
    const original: Target = {
      scheme: "ws",
      signal: "signal.example.com:8443",
      service: "echo.Echo",
      role: "client",
      peerId: "alice",
      token: "tok",
    };
    const s = formatTarget(original);
    const parsed = parseTarget(s);
    expect(parsed).toEqual(original);
  });
});
