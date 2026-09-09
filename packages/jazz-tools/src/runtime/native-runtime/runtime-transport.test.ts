import { afterEach, describe, expect, it, vi } from "vitest";
import type { WasmSchema } from "../../drivers/types.js";
import { PostcardReader, PostcardWriter } from "./native-codec.js";
import {
  CLIENT_WIRE_FEATURES,
  decodeWebSocketFrameBatch,
  encodeWireClientHello,
  encodeWebSocketPrelude,
  encodeWebSocketFrameBatch,
  isWireHello,
  WIRE_PROTOCOL_VERSION,
} from "./websocket.js";
import { BrowserWorkerTransportPump } from "./browser-worker-transport.js";
import { NativeRuntimeAdapter, type Transport } from "./native-runtime-adapter.js";
import { type TxId, type WriteReceipt } from "../client.js";

const previousWebSocket = globalThis.WebSocket;
const TEST_RUNTIME_AUTHOR = new TextEncoder().encode('["urn:jazz:test","runtime"]');

async function waitForServerPumpTimer(): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, 20));
}

async function waitForFakeWebSocketNegotiation(): Promise<void> {
  for (let turn = 0; turn < 6; turn += 1) await Promise.resolve();
}

async function committedTxId(receipt: WriteReceipt): Promise<TxId> {
  if (receipt.kind !== "committed") throw new Error("expected committed write receipt");
  return await receipt.txId;
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

describe("NativeRuntimeAdapter server transport", () => {
  afterEach(() => {
    globalThis.WebSocket = previousWebSocket;
  });

  it("marks external peer admission as requiring a distinct peer pass", () => {
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => fakeDb({ tick: () => undefined }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const peerWork: Array<boolean | undefined> = [];
    const unsubscribe = runtime.onPeerTransportWork((requiresDistinctPass) =>
      peerWork.push(requiresDistinctPass),
    );

    runtime.notifyPeerTransportActivity();

    expect(peerWork).toEqual([true]);
    unsubscribe();
  });

  it.each([
    [
      "transport mentioning a nested rejection",
      new Error("Protocol: upstream reported WriteRejected: quoted peer diagnostic"),
    ],
    ["not-observed", new Error("NotObserved: transaction is not resident")],
    ["schema", new Error("Schema: invalid authored branch value")],
    ["cancellation", Object.assign(new Error("operation cancelled"), { name: "AbortError" })],
    ["unknown", new Error("unknown lifecycle failure")],
  ])("preserves %s lifecycle errors from native write waits", async (_kind, nativeError) => {
    const write = {
      ...fakeWrite(),
      wait: async () => {
        throw nativeError;
      },
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => fakeDb({ insert: () => write, tick: () => undefined }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const inserted = runtime.insert(
      "todos",
      { title: { type: "Text", value: "lifecycle passthrough" } },
      null,
      "00000000-0000-0000-0000-000000000010",
    );

    await expect(runtime.waitForTransaction(await committedTxId(inserted), "local")).rejects.toBe(
      nativeError,
    );

    const admissionRuntime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            insert: () => {
              throw nativeError;
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    let admissionError: unknown;
    try {
      admissionRuntime.insert(
        "todos",
        { title: { type: "Text", value: "admission passthrough" } },
        null,
        "00000000-0000-0000-0000-000000000012",
      );
    } catch (error) {
      admissionError = error;
    }
    expect(admissionError).toBe(nativeError);
  });

  it("normalizes a terminal native WriteRejected error", async () => {
    const write = {
      ...fakeWrite(),
      wait: async () => {
        throw new Error("WriteRejected: queued write was denied");
      },
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => fakeDb({ insert: () => write, tick: () => undefined }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const inserted = runtime.insert(
      "todos",
      { title: { type: "Text", value: "terminal rejection" } },
      null,
      "00000000-0000-0000-0000-000000000011",
    );

    await expect(
      runtime.waitForTransaction(await committedTxId(inserted), "local"),
    ).rejects.toMatchObject({
      kind: "rejected",
      transactionId: await committedTxId(inserted),
      code: "write_rejected",
      reason: "queued write was denied",
    });
  });

  it("connects the native upstream transport to the scoped websocket endpoint", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([Uint8Array.from([1, 2, 3])]);
    const runtimeAuthor = new TextEncoder().encode('["urn:jazz:test","runtime-user"]');
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      runtimeAuthor,
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await waitForFakeWebSocketNegotiation();
    await waitForServerPumpTimer();

    expect(sockets).toHaveLength(1);
    expect(sockets[0]!.url).toBe("ws://127.0.0.1:4200/apps/app-a/ws");
    expect(sockets[0]!.sent[0]).toEqual(encodeWebSocketPrelude("{}", runtimeAuthor));
    const helloBatch = decodeWebSocketFrameBatch(sockets[0]!.sent[1]! as Uint8Array);
    expect(helloBatch).toHaveLength(1);
    expect(isWireHello(helloBatch[0]!)).toBe(true);
    expect(decodeWebSocketFrameBatch(sockets[0]!.sent[2]! as Uint8Array)).toEqual([
      Uint8Array.from([1, 2, 3]),
    ]);
    expect(transport.closed).toBe(false);

    runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));
    await Promise.resolve();
    await Promise.resolve();

    expect(sockets).toHaveLength(2);
    expect(sockets[0]!.closed).toBe(true);
    expect(JSON.parse(sockets[1]!.sent[0] as string)).toEqual({
      peer_identity: '["urn:jazz:test","runtime-user"]',
      auth: {
        sub: "runtime-user",
        jwt_token: "fresh.jwt",
      },
      sub: "runtime-user",
      jwt_token: "fresh.jwt",
    });

    runtime.disconnect();

    expect(sockets[1]!.closed).toBe(true);
  });

  it("reconnects an established upstream after a retryable server restart without publishing a terminal error", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transports: FakeTransport[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => {
              const transport = new FakeTransport([]);
              transports.push(transport);
              return transport;
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const terminal = vi.fn();
    runtime.onServerTransportError(terminal);
    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    sockets[0]!.emitServerClose();
    await runtime.waitForUpstreamServerConnection();
    expect(sockets).toHaveLength(2);
    expect(transports).toHaveLength(2);
    expect(transports[0]!.closed).toBe(true);
    expect(terminal).not.toHaveBeenCalled();
    await runtime.close();
  });

  it("reconnects after a negotiated Edge reports its account registry temporarily unavailable", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transports: FakeTransport[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => {
              const transport = new FakeTransport([]);
              transports.push(transport);
              return transport;
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const terminal = vi.fn();
    runtime.onServerTransportError(terminal);
    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    const error = new PostcardWriter();
    error.u64(2); // WireFrame::Error
    error.u64(6); // NotReady
    error.u64(3); // Later
    error.string("account registry unavailable");
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([error.finish()]));
    await vi.waitFor(() => expect(sockets).toHaveLength(2));
    await runtime.waitForUpstreamServerConnection();
    expect(sockets).toHaveLength(2);
    expect(transports).toHaveLength(2);
    expect(transports[0]!.closed).toBe(true);
    expect(terminal).not.toHaveBeenCalled();
    await runtime.close();
  });

  it.each(["disconnect", "close"] as const)(
    "cancels an established-network retry on %s",
    async (action) => {
      const sockets: FakeWebSocket[] = [];
      globalThis.WebSocket = class extends FakeWebSocket {
        constructor(url: string) {
          super(url);
          sockets.push(this);
        }
      } as unknown as typeof WebSocket;
      const runtime = new NativeRuntimeAdapter(
        {
          openMemory: () =>
            fakeDb({ connectUpstream: () => new FakeTransport([]), tick: () => undefined }),
          openBrowser: async () => {
            throw new Error("not used");
          },
        } as never,
        testSchema,
        new Uint8Array(16),
        TEST_RUNTIME_AUTHOR,
        1,
        true,
      );
      const terminal = vi.fn();
      runtime.onServerTransportError(terminal);
      runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
      await runtime.waitForUpstreamServerConnection();
      sockets[0]!.emitServerClose();
      await runtime[action]();
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(sockets).toHaveLength(1);
      expect(terminal).not.toHaveBeenCalled();
      await runtime.close();
    },
  );

  it("exhausts network retries and rejects an already armed remote wait", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
        if (sockets.length > 1) queueMicrotask(() => this.emitServerClose());
      }
    } as unknown as typeof WebSocket;
    const armed = deferred<void>();
    const settlement = deferred<void>();
    const write = {
      ...fakeWrite(),
      wait: (tier: string) => {
        if (tier === "local") return Promise.resolve();
        armed.resolve();
        return settlement.promise;
      },
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            insert: () => write,
            connectUpstream: () => new FakeTransport([]),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const terminal = vi.fn();
    runtime.onServerTransportError(terminal);
    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    const txId = await committedTxId(
      runtime.insert(
        "todos",
        { title: { type: "Text", value: "pending during outage" } },
        null,
        "00000000-0000-0000-0000-000000000009",
      ),
    );
    const pending = runtime.waitForTransaction(txId, "edge");
    const rejected = expect(pending).rejects.toThrow("websocket closed");
    await armed.promise;
    sockets[0]!.emitServerClose();
    await expect(runtime.waitForUpstreamServerConnection()).rejects.toThrow("websocket closed");
    await rejected;
    expect(sockets).toHaveLength(11);
    expect(terminal).toHaveBeenCalledTimes(1);
    settlement.resolve();
    await runtime.close();
  }, 15_000);

  it("does not resurrect the previous account transport after replacement", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({ connectUpstream: () => new FakeTransport([]), tick: () => undefined }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    sockets[0]!.emitServerClose();
    runtime.connect("ws://127.0.0.1:4200/apps/app-b/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    await new Promise((resolve) => setTimeout(resolve, 150));
    expect(sockets.map((socket) => socket.url)).toEqual([
      "ws://127.0.0.1:4200/apps/app-a/ws",
      "ws://127.0.0.1:4200/apps/app-b/ws",
    ]);
    await runtime.close();
  });

  it.each([
    "none",
    "auth-refresh",
    "disconnect",
    "authority-replacement",
    "account-replacement",
  ] as const)("settles a strict remote query begun during backoff after %s", async (action) => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => new Uint8Array([0]),
            connectUpstream: () => new FakeTransport([]),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    sockets[0]!.emitServerClose();
    // Bound the historical synchronous-spin regression so it fails this test
    // instead of freezing the test process and preventing the retry timer.
    const internal = runtime as unknown as { hasUpstream(): boolean };
    const hasUpstream = internal.hasUpstream.bind(runtime);
    const entered = deferred<void>();
    let probes = 0;
    internal.hasUpstream = () => {
      if (++probes > 1000) throw new Error("remote gate spun without yielding");
      entered.resolve();
      return hasUpstream();
    };
    const read = runtime.query(JSON.stringify({ table: "todos" }), null, "edge");
    const result =
      action === "disconnect"
        ? expect(read).rejects.toThrow("server transport disconnected")
        : action === "authority-replacement" || action === "account-replacement"
          ? expect(read).rejects.toThrow("server transport scope changed")
          : expect(read).resolves.toEqual([]);
    await entered.promise;
    if (action === "auth-refresh")
      await runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));
    if (action === "disconnect") await runtime.disconnect();
    if (action === "authority-replacement")
      runtime.connect("ws://127.0.0.1:4200/apps/app-b/ws", "{}");
    if (action === "account-replacement")
      await runtime.updateAuth(
        JSON.stringify({
          jwt_token:
            "e30." +
            Buffer.from(JSON.stringify({ iss: "urn:jazz:test", sub: "another" })).toString(
              "base64url",
            ) +
            ".signature",
        }),
      );
    await result;
    expect(sockets).toHaveLength(action === "disconnect" ? 1 : 2);
    await runtime.close();
  });

  it.each([
    [3, 1, "authentication expired"],
    [2, 0, "malformed protocol frame"],
  ] as const)(
    "does not retry a close after terminal wire error %s",
    async (code, retry, message) => {
      const sockets: FakeWebSocket[] = [];
      globalThis.WebSocket = class extends FakeWebSocket {
        constructor(url: string) {
          super(url);
          sockets.push(this);
        }
      } as unknown as typeof WebSocket;
      const runtime = new NativeRuntimeAdapter(
        {
          openMemory: () =>
            fakeDb({ connectUpstream: () => new FakeTransport([]), tick: () => undefined }),
          openBrowser: async () => {
            throw new Error("not used");
          },
        } as never,
        testSchema,
        new Uint8Array(16),
        TEST_RUNTIME_AUTHOR,
        1,
        true,
      );
      const terminal = vi.fn();
      runtime.onServerTransportError(terminal);
      runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
      await runtime.waitForUpstreamServerConnection();
      const frame = new PostcardWriter();
      frame.u64(2);
      frame.u64(code);
      frame.u64(retry);
      frame.string(message);
      sockets[0]!.emitMessage(encodeWebSocketFrameBatch([frame.finish()]));
      await vi.waitFor(() => expect(terminal).toHaveBeenCalledTimes(1));
      sockets[0]!.emitServerClose();
      await new Promise((resolve) => setTimeout(resolve, 150));
      expect(sockets).toHaveLength(1);
      await expect(runtime.waitForUpstreamServerConnection()).rejects.toThrow(message);
      expect(terminal).toHaveBeenCalledTimes(1);
      await runtime.close();
    },
  );

  it("identifies a missing wire mask as a generic native-runtime artifact mismatch", () => {
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => new FakeTransport([]),
            tick: () => undefined,
            wireFeatures: undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    expect(() => runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}")).toThrow(
      "native runtime binding does not expose its wire feature mask; install the matching Jazz native runtime package",
    );
  });

  it("uses the canonical credential author for websocket identity, not the raw runtime host", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({ connectUpstream: () => new FakeTransport([]), tick: () => undefined }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new TextEncoder().encode('["https://jazz.test","local-cache"]'),
      1,
      true,
    );

    // A reserved issuer is valid only once the server verifies this signed
    // first-party credential. The raw runtime host identity must never leak
    // into that authenticated transport assertion.
    const jwt = `header.${btoa(
      JSON.stringify({ iss: "urn:jazz:local-first", sub: "provider-subject" }),
    )}.signature`;
    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", JSON.stringify({ jwt_token: jwt }));
    await waitForFakeWebSocketNegotiation();

    expect(JSON.parse(sockets[0]!.sent[0] as string)).toMatchObject({
      peer_identity: '["urn:jazz:local-first","provider-subject"]',
      auth: { jwt_token: jwt, sub: "provider-subject" },
    });
    runtime.disconnect();
  });

  it("routes auxiliary frames and drains their output without semantic delivery", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    transport.auxiliaryConsumedFirstByte = 99;
    transport.auxiliaryOutgoing.push(Uint8Array.from([88]));
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([Uint8Array.from([99, 1])]));
    await waitForServerPumpTimer();

    expect(transport.received).toEqual([]);
    expect(
      sockets[0]!.sent.some(
        (batch) =>
          batch instanceof Uint8Array &&
          decodeWebSocketFrameBatch(batch).some((frame) => frame[0] === 88),
      ),
    ).toBe(true);
    runtime.disconnect();
  });

  it("marks server carrier ingress as requiring a distinct peer pass", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    const peerWork: Array<boolean | undefined> = [];
    const unsubscribe = runtime.onPeerTransportWork((requiresDistinctPass) =>
      peerWork.push(requiresDistinctPass),
    );

    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([Uint8Array.from([1, 42])]));
    await vi.waitFor(() => expect(transport.received).toHaveLength(1));

    expect(peerWork).toContain(true);
    unsubscribe();
    await runtime.close();
  });

  it("does not emit the fake server hello before the client prelude and hello", async () => {
    const socket = new FakeWebSocket("ws://127.0.0.1:4200/apps/app-a/ws");
    const received: Uint8Array[] = [];
    socket.addEventListener("message", (event) => received.push(event.data as Uint8Array));

    socket.send(encodeWebSocketFrameBatch([Uint8Array.from([1, 42])]));
    await Promise.resolve();
    expect(received).toEqual([]);

    socket.send(encodeWebSocketPrelude("{}", new Uint8Array(16)));
    await Promise.resolve();
    expect(received).toEqual([]);

    socket.send(encodeWebSocketFrameBatch([encodeWireClientHello()]));
    await Promise.resolve();

    expect(received).toHaveLength(1);
    expect(isWireHello(decodeWebSocketFrameBatch(received[0]!)[0]!)).toBe(true);
  });

  it("retries a pending edge wait when a websocket frame arrives without a native callback", async () => {
    let settled = false;
    let transportTicks = 0;
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    transport.tick = () => {
      transportTicks += 1;
      if (transportTicks >= 2) settled = true;
      return 0;
    };
    const write = {
      txId: "00000000000070008000000000000007",
      payload: new Uint8Array(),
      rowId: new Uint8Array(16),
      wait: () => (settled ? Promise.resolve() : new Promise<void>(() => {})),
      writeState: () => ({}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            insert: () => write,
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    transportTicks = 0;

    const inserted = runtime.insert(
      "todos",
      { title: { type: "Text", value: "pump delayed fate" } },
      null,
      "00000000-0000-0000-0000-000000000007",
    );

    const wait = runtime.waitForTransaction(await committedTxId(inserted), "edge");
    await Promise.resolve();
    await Promise.resolve();
    expect(transportTicks).toBe(1);

    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([Uint8Array.from([1, 42])]));
    await wait;

    expect(transportTicks).toBeGreaterThanOrEqual(2);
  });

  it("rejects active Edge and Global waits and subscriptions for a relayed terminal error without inventing a rejection", async () => {
    const remoteSettlement = new Promise<void>(() => {});
    const localSubscription = {
      closed: false,
      readAll: () => [],
      close() {
        this.closed = true;
        return true;
      },
    };
    const edgeSubscription = {
      closed: false,
      readAll: () => [],
      close() {
        this.closed = true;
        return true;
      },
    };
    const globalSubscription = {
      closed: false,
      readAll: () => [],
      close() {
        this.closed = true;
        return true;
      },
    };
    const subscriptions = [localSubscription, edgeSubscription, globalSubscription];
    let nativeMutationError: ((event: unknown) => void) | undefined;
    const write = {
      txId: "00000000000070008000000000000008",
      payload: new Uint8Array(),
      rowId: new Uint8Array(16),
      wait: (tier: string) => (tier === "local" ? Promise.resolve() : remoteSettlement),
      writeState: () => ({}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            insert: () => write,
            subscribe: () => subscriptions.shift()!,
            onMutationError: (callback: (event: unknown) => void) => {
              nativeMutationError = callback;
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const mutationErrors = vi.fn();
    runtime.onMutationError(mutationErrors);
    expect(nativeMutationError).toBeTypeOf("function");
    const authoritativeRejection = {
      code: "permission_denied",
      reason: "authority rejected the mutation",
      transaction: {
        transactionId: "00000000000070008000000000000008",
        kind: "mergeable",
        sealed: true,
        latestSettlement: {
          kind: "rejected",
          transactionId: "00000000000070008000000000000008",
          code: "permission_denied",
          reason: "authority rejected the mutation",
        },
      },
    };
    nativeMutationError?.(authoritativeRejection);
    expect(mutationErrors).toHaveBeenCalledWith(authoritativeRejection);
    mutationErrors.mockClear();

    const inserted = runtime.insert(
      "todos",
      { title: { type: "Text", value: "still locally durable" } },
      null,
      "00000000-0000-0000-0000-000000000008",
    );
    const txId = await committedTxId(inserted);
    await expect(runtime.waitForTransaction(txId, "local")).resolves.toBeUndefined();

    const localHandle = runtime.createSubscription(
      JSON.stringify({ table: "todos" }),
      null,
      "local",
    );
    const edgeHandle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "edge");
    const globalHandle = runtime.createSubscription(
      JSON.stringify({ table: "todos" }),
      null,
      "global",
    );
    const localUpdates = vi.fn();
    const edgeUpdates = vi.fn();
    const globalUpdates = vi.fn();
    runtime.executeSubscription(localHandle, localUpdates);
    runtime.executeSubscription(edgeHandle, edgeUpdates);
    runtime.executeSubscription(globalHandle, globalUpdates);
    const edgeWait = runtime.waitForTransaction(txId, "edge");
    const globalWait = runtime.waitForTransaction(txId, "global");
    await Promise.resolve();

    runtime.reportRemoteServerTransportError(new Error("Protocol: terminal upstream failure"));

    await expect(edgeWait).rejects.toThrow("Protocol: terminal upstream failure");
    await expect(globalWait).rejects.toThrow("Protocol: terminal upstream failure");
    expect(localSubscription.closed).toBe(false);
    expect(localUpdates).not.toHaveBeenCalled();
    expect(edgeSubscription.closed).toBe(true);
    expect(globalSubscription.closed).toBe(true);
    for (const updates of [edgeUpdates, globalUpdates]) {
      expect(updates).toHaveBeenCalledWith(expect.any(Error));
      const firstUpdate = updates.mock.calls[0];
      if (!firstUpdate) throw new Error("terminal transport error did not wake subscription");
      expect((firstUpdate[0] as Error).message).toBe("Protocol: terminal upstream failure");
    }
    // Unlike the authoritative rejection above, a transport failure has no
    // fate and must not use the mutation-rejection callback path.
    expect(mutationErrors).not.toHaveBeenCalled();
  });

  it("delivers a terminal error to armed Edge and Global waits before reconnect clears transport state", async () => {
    const remoteSettlement = deferred<void>();
    const remoteWaitsArmed = deferred<void>();
    let remoteWaits = 0;
    const write = {
      txId: "00000000000070008000000000000009",
      payload: new Uint8Array(),
      rowId: new Uint8Array(16),
      wait: (tier: string) => {
        if (tier === "local") return Promise.resolve();
        remoteWaits += 1;
        if (remoteWaits === 2) remoteWaitsArmed.resolve();
        return remoteSettlement.promise;
      },
      writeState: () => ({}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            insert: () => write,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const inserted = runtime.insert(
      "todos",
      { title: { type: "Text", value: "terminal beats reconnect clear" } },
      null,
      "00000000-0000-0000-0000-000000000009",
    );
    const txId = await committedTxId(inserted);
    const edgeWait = runtime.waitForTransaction(txId, "edge");
    const globalWait = runtime.waitForTransaction(txId, "global");

    // This event barrier proves both remote waits reached the terminal waiter
    // registration point before the failure and replacement race begins.
    await remoteWaitsArmed.promise;
    expect(
      (runtime as unknown as { serverTransportErrorWaiters: unknown[] })
        .serverTransportErrorWaiters,
    ).toHaveLength(2);

    runtime.reportRemoteServerTransportError(new Error("Protocol: terminal before reconnect"));
    const replacement = runtime.disconnect({ rejectWaiters: false });
    remoteSettlement.resolve();
    await replacement;

    await expect(edgeWait).rejects.toThrow("Protocol: terminal before reconnect");
    await expect(globalWait).rejects.toThrow("Protocol: terminal before reconnect");
  });

  it("retries a pending edge wait when a websocket frame arrives without a native callback", async () => {
    let settled = false;
    let transportTicks = 0;
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    transport.tick = () => {
      transportTicks += 1;
      if (transportTicks >= 2) settled = true;
      return 0;
    };
    const write = {
      txId: "00000000000070008000000000000007",
      payload: new Uint8Array(),
      rowId: new Uint8Array(16),
      wait: () => (settled ? Promise.resolve() : new Promise<void>(() => {})),
      writeState: () => ({}),
      nextWriteStateChange: () => new Promise<void>(() => {}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            insert: () => write,
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    transportTicks = 0;

    const inserted = runtime.insert(
      "todos",
      { title: { type: "Text", value: "pump delayed fate" } },
      null,
      "00000000-0000-0000-0000-000000000007",
    );

    const wait = runtime.waitForTransaction(await committedTxId(inserted), "edge");
    await Promise.resolve();
    await Promise.resolve();
    expect(transportTicks).toBe(1);

    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([Uint8Array.from([1, 42])]));
    await wait;

    expect(transportTicks).toBeGreaterThanOrEqual(2);
  });

  it("uses the binding scheduler to drive native db ticks outside server pumps", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([Uint8Array.from([7])]);
    let schedulerCallback: ((urgency: "immediate" | "deferred") => void) | undefined;
    let dbTicks = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => ({
          connectUpstream: () => transport,
          wireFeatures: () => CLIENT_WIRE_FEATURES,
          setTickScheduler: (callback: (urgency: "immediate" | "deferred") => void) => {
            schedulerCallback = callback;
          },
          tick: () => {
            dbTicks += 1;
            transport.tick();
          },
        }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    expect(schedulerCallback).toBeTypeOf("function");

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();

    expect(transport.tickCount).toBeGreaterThan(0);
    const initialDbTicks = dbTicks;
    expect(initialDbTicks).toBeGreaterThan(0);

    schedulerCallback?.("immediate");
    await Promise.resolve();

    expect(transport.tickCount).toBeGreaterThan(1);
    expect(dbTicks).toBe(initialDbTicks + 1);
  });

  it("serializes asynchronous db ticks and preserves a wake received while suspended", async () => {
    let schedulerCallback: ((urgency: "immediate" | "deferred") => void) | undefined;
    let releaseFirstTick!: () => void;
    let dbTicks = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            setTickScheduler: (callback: (urgency: "immediate" | "deferred") => void) => {
              schedulerCallback = callback;
            },
            tick: () => {
              dbTicks += 1;
              if (dbTicks === 1) {
                return new Promise<void>((resolve) => {
                  releaseFirstTick = resolve;
                });
              }
            },
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    schedulerCallback?.("immediate");
    await Promise.resolve();
    expect(dbTicks).toBe(1);

    schedulerCallback?.("immediate");
    await Promise.resolve();
    expect(dbTicks).toBe(1);

    releaseFirstTick();
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    await new Promise<void>((resolve) => setTimeout(resolve, 0));
    expect(dbTicks).toBe(2);
    runtime.close();
  });

  it("admits a peer only after a suspended owner-wide evaluator pass exits", async () => {
    let releaseTick!: () => void;
    let accepted = 0;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            tick: () =>
              new Promise<void>((resolve) => {
                releaseTick = resolve;
              }),
            acceptSubscriber: () => {
              accepted += 1;
              return transport;
            },
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    const progress = runtime.progressPeerTransport();
    await Promise.resolve();
    const admission = runtime.acceptPeer();
    await Promise.resolve();
    expect(accepted).toBe(0);

    releaseTick();
    await progress;
    expect(await admission).toBe(transport);
    expect(accepted).toBe(1);
    await runtime.close();
  });

  // This is a binding scheduler test: a real storage future cannot reliably
  // force the one-microtask gap between an idle check and synchronous admission.
  it("does not yield between an idle check and peer admission when a tick is queued", async () => {
    let schedule!: (urgency: "immediate" | "deferred") => void;
    let releaseTick!: () => void;
    let tickHoldingNode = false;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            setTickScheduler: (callback: typeof schedule) => {
              schedule = callback;
            },
            tick: () => {
              tickHoldingNode = true;
              return new Promise<void>((resolve) => {
                releaseTick = () => {
                  tickHoldingNode = false;
                  resolve();
                };
              });
            },
            acceptSubscriber: () => {
              if (tickHoldingNode) throw new Error("admission reentered a suspended tick");
              return transport;
            },
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    schedule("immediate");
    const admission = runtime.acceptPeer();
    try {
      await expect(admission).resolves.toBe(transport);
    } finally {
      releaseTick?.();
      await runtime.close();
    }
  });

  // Deferred binding operations pin ownership races that real IndexedDB timing
  // cannot reliably arrange. The browser restart receipt covers the real I/O.
  it("waits for a real tick and defers retirement while subscriber admission is suspended", async () => {
    const storage = deferred<Transport>();
    const finishTick = deferred<void>();
    const transport = new FakeTransport([]);
    let admitting = true;
    let ticks = 0;
    let retired = false;
    const oldTransport = new FakeTransport([]);
    oldTransport.close = () => {
      if (admitting) throw new Error("retirement reentered subscriber recovery");
      retired = true;
      return true;
    };
    const runtime = NativeRuntimeAdapter.fromDb(
      fakeDb({
        acceptSubscriber: async () => {
          try {
            return await storage.promise;
          } finally {
            admitting = false;
          }
        },
        tick: async () => {
          if (admitting) throw new Error("tick reentered subscriber recovery");
          ticks++;
          await finishTick.promise;
        },
      }) as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const admission = runtime.acceptPeer();
    let progressed = false;
    const progress = runtime.progressPeerTransport().then(() => {
      progressed = true;
    });
    const retirement = runtime.retirePeerTransport(oldTransport);
    try {
      await Promise.resolve();
      expect(progressed).toBe(false);
      expect(retired).toBe(false);
      storage.resolve(transport);
      await admission;
      await retirement;
      expect(retired).toBe(true);
      await vi.waitFor(() => expect(ticks).toBeGreaterThan(0));
      expect(progressed).toBe(false);
      finishTick.resolve();
      await progress;
    } finally {
      storage.resolve(transport);
      finishTick.resolve();
      await Promise.allSettled([admission, progress, retirement]);
      await runtime.close();
    }
  });

  it("releases queued admission and preserves a tick wake after recovery and retirement fail", async () => {
    const storage = deferred<Transport>();
    const readError = new Error("page read failed");
    const retirementError = new Error("old transport retirement failed");
    const transport = new FakeTransport([]);
    const oldTransport = new FakeTransport([]);
    oldTransport.close = () => {
      throw retirementError;
    };
    let schedule!: (urgency: "immediate" | "deferred") => void;
    let firstAdmission = true;
    let recovering = true;
    let ticks = 0;
    const runtime = NativeRuntimeAdapter.fromDb(
      fakeDb({
        setTickScheduler: (callback: typeof schedule) => {
          schedule = callback;
        },
        acceptSubscriber: async () => {
          if (firstAdmission) {
            firstAdmission = false;
            try {
              return await storage.promise;
            } finally {
              recovering = false;
            }
          }
          if (recovering) throw new Error("second admission reentered recovery");
          return transport;
        },
        tick: () => {
          if (recovering) throw new Error("scheduled tick reentered recovery");
          ticks++;
        },
      }) as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const failedAdmission = expect(runtime.acceptPeer()).rejects.toBe(readError);
    const nextAdmission = runtime.acceptPeer();
    const retirement = expect(runtime.retirePeerTransport(oldTransport)).rejects.toBe(
      retirementError,
    );
    schedule("immediate");
    try {
      storage.reject(readError);
      await failedAdmission;
      await retirement;
      await expect(nextAdmission).resolves.toBe(transport);
      await vi.waitFor(() => expect(ticks).toBeGreaterThan(0));
    } finally {
      storage.reject(readError);
      await Promise.allSettled([failedAdmission, nextAdmission, retirement]);
      await runtime.close();
    }
  });

  it("closes a late subscriber before disposing its owner and rejects queued admission", async () => {
    const storage = deferred<Transport>();
    const transport = new FakeTransport([]);
    let recovering = true;
    let subscriberClosed = false;
    let ownerClosed = false;
    transport.close = () => {
      if (recovering || ownerClosed) throw new Error("subscriber retired outside owner lifetime");
      subscriberClosed = true;
      return true;
    };
    const admit = vi.fn(async () => {
      try {
        return await storage.promise;
      } finally {
        recovering = false;
      }
    });
    const runtime = NativeRuntimeAdapter.fromDb(
      fakeDb({
        acceptSubscriber: admit,
        tick: () => {
          throw new Error("closing owner must not start a queued tick");
        },
        close: () => {
          if (recovering || !subscriberClosed) throw new Error("owner disposed before subscriber");
          ownerClosed = true;
        },
      }) as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const admission = expect(runtime.acceptPeer()).rejects.toBeInstanceOf(Error);
    const queued = expect(runtime.acceptPeer()).rejects.toBeInstanceOf(Error);
    const progress = runtime.progressPeerTransport();
    const closing = runtime.close();
    try {
      await Promise.resolve();
      expect(ownerClosed).toBe(false);
      storage.resolve(transport);
      await Promise.all([admission, queued, progress, closing]);
      expect(subscriberClosed).toBe(true);
      expect(ownerClosed).toBe(true);
      expect(admit).toHaveBeenCalledOnce();
    } finally {
      storage.resolve(transport);
      await Promise.allSettled([admission, queued, progress, closing]);
    }
  });

  it("rejects admission queued behind a tick when owner closure wins", async () => {
    const finishTick = deferred<void>();
    const admit = vi.fn(() => new FakeTransport([]));
    const runtime = NativeRuntimeAdapter.fromDb(
      fakeDb({
        acceptSubscriber: admit,
        tick: () => finishTick.promise,
      }) as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const progress = runtime.progressPeerTransport();
    const admission = expect(runtime.acceptPeer()).rejects.toBeInstanceOf(Error);
    const closing = runtime.close();
    try {
      finishTick.resolve();
      await Promise.all([progress, admission, closing]);
      expect(admit).not.toHaveBeenCalled();
    } finally {
      finishTick.resolve();
      await Promise.allSettled([progress, admission, closing]);
    }
  });

  it("yields to the host event loop when every core tick schedules more work", async () => {
    let schedulerCallback: ((urgency: "immediate" | "deferred") => void) | undefined;
    let dbTicks = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            setTickScheduler: (callback: (urgency: "immediate" | "deferred") => void) => {
              schedulerCallback = callback;
            },
            tick: () => {
              dbTicks += 1;
              schedulerCallback?.("immediate");
            },
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    const hostTask = new Promise<void>((resolve) => setTimeout(resolve, 0));
    schedulerCallback?.("immediate");
    await hostTask;

    expect(dbTicks).toBeGreaterThan(0);
    expect(dbTicks).toBeLessThanOrEqual(4);
    runtime.close();
  });

  it("yields recurring deferred core work with an attached peer pump to the host task", async () => {
    let schedulerCallback: ((urgency: "immediate" | "deferred") => void) | undefined;
    let dbTicks = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            setTickScheduler: (callback: typeof schedulerCallback) => {
              schedulerCallback = callback;
            },
            tick: () => {
              dbTicks += 1;
              // Bound a broken implementation without relying on a timeout that
              // cannot fire while microtasks starve the host task queue.
              if (dbTicks < 64) schedulerCallback?.("deferred");
            },
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const errors: unknown[] = [];
    const hostTask = new Promise<void>((resolve) => setTimeout(resolve, 0));
    const pump = new BrowserWorkerTransportPump(
      runtime,
      new FakeTransport([]),
      () => undefined,
      (error) => errors.push(error),
    );
    try {
      schedulerCallback?.("deferred");
      await hostTask;
      expect(errors).toEqual([]);
      expect(dbTicks).toBeGreaterThan(0);
      expect(dbTicks).toBeLessThanOrEqual(4);
    } finally {
      pump.close();
      await runtime.close();
    }
  });

  it("stages an already-arrived websocket frame group before one native transport tick", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    transport.tickCount = 0;

    const frames = [Uint8Array.from([1]), Uint8Array.from([1, 42]), Uint8Array.from([1, 43])];
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch(frames));
    await Promise.resolve();
    await waitForServerPumpTimer();
    expect(transport.receivedBatches).toEqual([frames]);
    expect(transport.received).toEqual(frames);
    expect(transport.tickCount).toBe(1);
  });

  it("discards a suspended transport tick after reconnect", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    let releaseFirstTick!: () => void;
    const first = new FakeTransport([Uint8Array.from([1, 99])]);
    first.tick = (() =>
      new Promise<number>((resolve) => {
        releaseFirstTick = () => resolve(0);
      })) as never;
    const second = new FakeTransport([]);
    const transports = [first, second];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transports.shift()!,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await vi.waitFor(() => expect(releaseFirstTick).toBeTypeOf("function"));
    runtime.updateAuth(JSON.stringify({ jwt_token: "replacement.jwt" }));
    await waitForFakeWebSocketNegotiation();
    releaseFirstTick();
    await runtime.waitForUpstreamServerConnection();
    await Promise.resolve();
    await Promise.resolve();

    expect(sockets).toHaveLength(2);
    expect(sockets[0]!.closed).toBe(true);
    expect(
      sockets[1]!.sent
        .filter((frame): frame is Uint8Array => frame instanceof Uint8Array)
        .flatMap((batch) => decodeWebSocketFrameBatch(batch)),
    ).not.toContainEqual(Uint8Array.from([1, 99]));
    runtime.close();
  });

  it("coalesces separate websocket messages that arrive before the server pump timer", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await runtime.waitForUpstreamServerConnection();
    transport.tickCount = 0;

    const first = Uint8Array.from([1, 10]);
    const second = Uint8Array.from([1, 11]);
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([first]));
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([second]));
    await Promise.resolve();
    await waitForServerPumpTimer();

    expect(transport.receivedBatches).toEqual([[first, second]]);
    expect(transport.received).toEqual([first, second]);
    expect(transport.tickCount).toBe(1);
  });

  it("runs another server pass when work is scheduled during an active pass", async () => {
    globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    let tickCount = 0;
    let releaseFirstTick!: () => void;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            tick: () => {
              tickCount += 1;
              if (tickCount !== 1) return undefined;
              return new Promise<void>((resolve) => {
                releaseFirstTick = resolve;
              });
            },
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await vi.waitFor(() => expect(releaseFirstTick).toBeTypeOf("function"));
    // A mutation or peer pass can discover server work while the previous
    // server pass is suspended in storage. That wakeup must not be dropped.
    (runtime as unknown as { scheduleServerPump(): void }).scheduleServerPump();
    releaseFirstTick();

    await vi.waitFor(() => expect(tickCount).toBeGreaterThanOrEqual(2));
    await runtime.close();
  });
});

const testSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;
class FakeTransport implements Transport {
  closed = false;
  readonly received: Uint8Array[] = [];
  readonly receivedBatches: Uint8Array[][] = [];
  tickCount = 0;
  auxiliaryConsumedFirstByte: number | undefined;
  readonly auxiliaryOutgoing: Uint8Array[] = [];

  constructor(private readonly outgoing: Uint8Array[]) {}

  close(): boolean {
    this.closed = true;
    return true;
  }

  recvWireFrames(): unknown[] {
    return this.outgoing.splice(0);
  }

  sendWireFrame(frame: Uint8Array): void {
    this.received.push(frame);
  }

  sendWireFrames(frames: readonly Uint8Array[]): void {
    const batch = [...frames];
    this.receivedBatches.push(batch);
    this.received.push(...batch);
  }

  routeAuxiliaryWireFrame(frame: Uint8Array): Uint8Array | undefined {
    return frame[0] === this.auxiliaryConsumedFirstByte ? undefined : frame;
  }

  recvAuxiliaryWireFrames(): unknown[] {
    return this.auxiliaryOutgoing.splice(0);
  }

  tick(): number {
    this.tickCount += 1;
    return 0;
  }
}

class FakeWebSocket {
  binaryType: "arraybuffer" | "blob" = "arraybuffer";
  readonly readyState = 1;
  readonly sent: Array<Uint8Array | string> = [];
  private readonly closeListeners: Array<(event: { data: unknown }) => void> = [];
  private readonly messageListeners: Array<(event: { data: unknown }) => void> = [];
  closed = false;

  private sawClientPrelude = false;
  private serverHelloScheduled = false;

  constructor(readonly url: string) {}

  send(data: Uint8Array | string): void {
    this.sent.push(data);
    if (typeof data === "string") {
      this.sawClientPrelude = isClientPrelude(data);
      return;
    }
    if (!this.sawClientPrelude || this.serverHelloScheduled) return;
    if (!isClientHelloBatch(data)) return;
    this.serverHelloScheduled = true;
    queueMicrotask(() => {
      if (!this.closed) this.emitMessage(encodeWebSocketFrameBatch([encodeWireServerHello()]));
    });
  }

  close(): void {
    this.closed = true;
  }

  addEventListener(type: string, listener: (event: { data: unknown }) => void): void {
    if (type === "message") this.messageListeners.push(listener);
    if (type === "close") this.closeListeners.push(listener);
  }

  emitServerClose(): void {
    this.closed = true;
    for (const listener of this.closeListeners)
      listener({ code: 1012, reason: "server shutting down" } as never);
  }

  emitMessage(data: Uint8Array): void {
    for (const listener of this.messageListeners) listener({ data });
  }
}

function encodeWireServerHello(epoch: bigint = 1n): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(0); // WireFrame::Hello
  writer.u64(WIRE_PROTOCOL_VERSION);
  writer.u64(WIRE_PROTOCOL_VERSION);
  writer.u64(CLIENT_WIRE_FEATURES);
  writer.u64(1); // WirePeerRole::Core
  writer.some((authority) => {
    authority.bytes(Uint8Array.from({ length: 16 }, () => 0x5e));
    authority.u64(epoch);
  });
  return writer.finish();
}

function isClientPrelude(data: string): boolean {
  try {
    const prelude = JSON.parse(data) as { peer_identity?: unknown; auth?: unknown };
    return (
      typeof prelude.peer_identity === "string" &&
      prelude.auth !== null &&
      typeof prelude.auth === "object"
    );
  } catch {
    return false;
  }
}

function isClientHelloBatch(data: Uint8Array): boolean {
  try {
    const frames = decodeWebSocketFrameBatch(data);
    if (frames.length !== 1 || !isWireHello(frames[0]!)) return false;
    const hello = new PostcardReader(frames[0]!);
    hello.u64(); // WireFrame::Hello
    hello.u64(); // min_protocol_version
    hello.u64(); // max_protocol_version
    hello.u64(); // features
    return hello.u64() === 0; // WirePeerRole::Client
  } catch {
    return false;
  }
}
function fakeDb<T extends object>(
  db: T,
): T & { setTickScheduler(callback: (urgency: "immediate" | "deferred") => void): void } {
  type FakeOpenBatch = {
    kind: "mergeable" | "exclusive";
    author?: Uint8Array;
  };
  const implementation = db as T & {
    connectUpstream?(): Transport;
    tick?(): void | Promise<void>;
  };
  const openBatches = new Map<string, FakeOpenBatch>();
  const requireOpenBatch = (openTransactionId: string): void => {
    const batch = openBatches.get(openTransactionId);
    if (!batch) throw new Error(`unknown batch ${openTransactionId}`);
  };
  let upstream: Transport | undefined;
  const result: Record<string, unknown> = {
    setTickScheduler: () => undefined,
    onMutationError: () => undefined,
    // The production binding advertises its compiled capability mask. Keep
    // this transport fixture honest about the same handshake boundary.
    wireFeatures: () => CLIENT_WIRE_FEATURES,
    beginTransaction: (
      openTransactionId: string,
      kind: FakeOpenBatch["kind"],
      author?: Uint8Array,
    ) => {
      openBatches.set(openTransactionId, { kind, author });
    },
    insert: (_table: string, _cells: Uint8Array, options?: { rowId?: Uint8Array }) => ({
      ...fakeWrite(),
      rowId: options?.rowId ?? new Uint8Array(16),
    }),
    insertInTransaction: (
      openTransactionId: string,
      _table: string,
      _cells: Uint8Array,
      options?: { rowId?: Uint8Array },
    ) => (requireOpenBatch(openTransactionId), options?.rowId ?? new Uint8Array(16)),
    restore: () => fakeWrite(),
    restoreInTransaction: (openTransactionId: string) => requireOpenBatch(openTransactionId),
    update: () => fakeWrite(),
    updateInTransaction: (openTransactionId: string) => requireOpenBatch(openTransactionId),
    upsert: () => fakeWrite(),
    upsertInTransaction: (openTransactionId: string) => requireOpenBatch(openTransactionId),
    delete: () => fakeWrite(),
    deleteInTransaction: (openTransactionId: string) => requireOpenBatch(openTransactionId),
    commitTransaction: (openTransactionId: string) => {
      const batch = openBatches.get(openTransactionId);
      if (!batch) throw new Error(`unknown batch ${openTransactionId}`);
      openBatches.delete(openTransactionId);
      return fakeWrite();
    },
    rollbackTransaction: (openTransactionId: string) => {
      const batch = openBatches.get(openTransactionId);
      if (!batch) throw new Error(`unknown batch ${openTransactionId}`);
      openBatches.delete(openTransactionId);
    },
    ...db,
  };
  if (implementation.connectUpstream) {
    result.connectUpstream = () => {
      upstream = implementation.connectUpstream!();
      return upstream;
    };
  }
  if (implementation.tick) {
    result.tick = async () => {
      await implementation.tick!();
      await upstream?.tick();
    };
  }
  return result as T & {
    setTickScheduler(callback: (urgency: "immediate" | "deferred") => void): void;
  };
}

function fakeWrite() {
  return {
    txId: "00000000000070008000000000000001",
    payload: new Uint8Array(0),
    rowId: new Uint8Array(16),
    wait: async () => undefined,
    writeState: () => ({}),
  };
}
