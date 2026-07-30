/**
 * PeerRPC React echo client.
 *
 * Uses the @peerrpc/react hooks (usePeerRPC / useUnary / useServerStream)
 * to exercise the echo.Echo service via WebSocket signaling. Demonstrates
 * the idiomatic React integration: connection state in usePeerRPC, unary
 * calls in useUnary, server streams in useServerStream.
 *
 * Run:  make run-signal         (terminal 1)
 *       make run-echo-server    (terminal 2, Go or TS)
 *       make run-ts-echo-react  (terminal 3)
 */

import { useCallback, useState } from "react";
import { createRoot } from "react-dom/client";
import { usePeerRPC, useUnary, useServerStream, useConnected } from "@peerrpc/react";
import { WebSocketSignal } from "@peerrpc/signal";

const enc = new TextEncoder();
const dec = new TextDecoder();

function App(): JSX.Element {
  const [signalUrl, setSignalUrl] = useState("ws://localhost:8443/ws");
  const [service, setService] = useState("echo.Echo");
  const [request, setRequest] = useState("hello, peerrpc");
  const [logLines, setLogLines] = useState<string[]>([]);
  const [bidiLog, setBidiLog] = useState<string[]>([]);
  const [collectResult, setCollectResult] = useState<string | null>(null);

  const log = useCallback((msg: string) => {
    setLogLines((prev) => [...prev, msg]);
  }, []);

  // createSignal returns a connected WebSocketSignal. usePeerRPC calls
  // this inside its async connect(); the subsequent peer.createOffer()
  // gives the WebSocket enough time to open before the first send().
  const rpc = usePeerRPC({
    createSignal: () => {
      const sig = new WebSocketSignal({
        url: signalUrl,
        service,
        peerId: "react-echo-" + Math.random().toString(36).slice(2, 8),
      });
      // Fire-and-forget: usePeerRPC.connect awaits peer.createOffer()
      // which overlaps with the WebSocket handshake.
      sig.connect().catch((e) => log(`signal connect: ${e}`));
      return sig;
    },
  });

  const connected = useConnected(rpc);
  const unary = useUnary(rpc.client, "/echo.Echo/Echo", (raw) => dec.decode(raw));
  const stream = useServerStream(rpc.client, "/echo.Echo/Stream", (raw) => dec.decode(raw));

  const onConnect = useCallback(() => {
    log(`dialing ${signalUrl} (service ${service}) ...`);
    rpc.connect().catch((e) => log(`dial failed: ${e}`));
  }, [rpc, signalUrl, service, log]);

  const onUnary = useCallback(async () => {
    log(`Unary /echo.Echo/Echo: "${request}"`);
    const { status } = await unary.invoke(enc.encode(request));
    if (status.code !== 0) log(`  error: code=${status.code} msg=${status.message}`);
  }, [unary, request, log]);

  const onStream = useCallback(() => {
    log(`ServerStream /echo.Echo/Stream: "${request}"`);
    stream.start(enc.encode(request));
  }, [stream, request, log]);

  // Client streaming: usePeerRPC gives a Client; we call it directly
  // since useUnary/useServerStream don't cover client/bidi streaming.
  const onCollect = useCallback(async () => {
    if (!rpc.client) return;
    log(`ClientStream /echo.Echo/Collect (3 messages)`);
    const s = await rpc.client.invokeClientStreaming("/echo.Echo/Collect");
    for (const m of ["first", "second", "third"]) {
      await s.send(enc.encode(m));
      log(`  sent: "${m}"`);
    }
    await s.closeSend();
    const resp = await s.recv();
    if (resp === null) {
      setCollectResult("(no response)");
    } else {
      setCollectResult(dec.decode(resp));
    }
  }, [rpc.client, log]);

  const onChat = useCallback(async () => {
    if (!rpc.client) return;
    setBidiLog([]);
    const s = await rpc.client.invokeBidiStreaming("/echo.Echo/Chat");
    const lines: string[] = [];
    for (let i = 1; i <= 3; i++) {
      const msg = `msg-${i}`;
      await s.send(enc.encode(msg));
      const resp = await s.recv();
      if (resp === null) {
        lines.push("stream ended early");
        break;
      }
      lines.push(`sent "${msg}" → recv "${dec.decode(resp)}"`);
    }
    await s.closeSend();
    await s.recv();
    setBidiLog(lines);
  }, [rpc.client]);

  const statusClass = rpc.status === "connected" ? "ok"
    : rpc.status === "error" ? "err"
    : rpc.status === "connecting" ? "connecting" : "";

  return (
    <>
      <h1>PeerRPC Echo (React)</h1>
      <p className="info">
        Uses @peerrpc/react hooks. Requires a signal-server (default cleartext
        ws://localhost:8443/ws) and an echo server.
      </p>
      <div className="row">
        <label>WS URL: <input value={signalUrl} onChange={(e) => setSignalUrl(e.target.value)} /></label>
      </div>
      <div className="row">
        <label>Service: <input value={service} onChange={(e) => setService(e.target.value)} /></label>
      </div>
      <div className="row">
        <button onClick={onConnect} disabled={rpc.status === "connecting" || rpc.status === "connected"}>
          {rpc.status === "connected" ? "Connected" : rpc.status === "connecting" ? "Connecting..." : "Connect"}
        </button>
        <button onClick={rpc.disconnect} disabled={!connected}>Disconnect</button>
        <span className={`status ${statusClass}`}> {rpc.status}</span>
        {rpc.error && <span className="err"> {rpc.error}</span>}
      </div>
      <div className="row">
        <button onClick={onUnary} disabled={!connected}>Unary</button>
        <button onClick={onStream} disabled={!connected}>Server Stream</button>
        <button onClick={onCollect} disabled={!connected}>Client Stream</button>
        <button onClick={onChat} disabled={!connected}>Bidi Chat</button>
      </div>
      <div className="row">
        <label>Request: <input value={request} onChange={(e) => setRequest(e.target.value)} /></label>
      </div>

      {unary.data !== null && <div className="row"><strong>Unary:</strong> {unary.data}</div>}
      {unary.loading && <div className="info">Unary loading...</div>}
      {unary.error && <div className="err">Unary error: {unary.error.message}</div>}

      {stream.messages.length > 0 && (
        <div className="row">
          <strong>Stream chunks:</strong>
          <ul>{stream.messages.map((m, i) => <li key={i}>{m}</li>)}</ul>
        </div>
      )}
      {stream.done && <div className="info">Stream done</div>}

      {collectResult !== null && <div className="row"><strong>Collect:</strong> {collectResult}</div>}

      {bidiLog.length > 0 && (
        <div className="row">
          <strong>Bidi:</strong>
          <ul>{bidiLog.map((m, i) => <li key={i}>{m}</li>)}</ul>
        </div>
      )}

      <div id="log">{logLines.join("\n")}</div>
    </>
  );
}

const root = createRoot(document.getElementById("root")!);
root.render(<App />);
