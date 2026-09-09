import { schema as s } from "../../schema-namespace.js";
import { authorColumnType } from "../../magic-columns.js";
import { afterEach, describe, expect, it, vi } from "vitest";
import { performance } from "node:perf_hooks";
import type {
  ColumnDescriptor,
  RuntimeSubscriptionDelta,
  WasmSchema,
} from "../../drivers/types.js";
import {
  createRecord,
  PostcardReader,
  PostcardWriter,
  queryWithPredicates,
  readNativeSubscriptionDelta,
  writeNativeRowDescriptor,
  type DescriptorField,
  type NativeRowDescriptorField,
} from "./native-codec.js";
import {
  CLIENT_WIRE_FEATURES,
  decodeWebSocketFrameBatch,
  encodeWebSocketFrameBatch,
  isWireHello,
  WIRE_PROTOCOL_VERSION,
} from "./websocket.js";
import {
  formatUuid,
  NativeRuntimeAdapter,
  decodeSubscriptionDelta,
  type Transport,
} from "./native-runtime-adapter.js";
import { encodeSchema } from "./schema-codec.js";
import { applySubscriptionDelta, SubscriptionManager } from "../subscription-manager.js";
import { setNamedRowValuesEnumerable } from "./row-values-transport.js";
import { encodeNativeNullValue, storageColumnValueType } from "./native-row-codec.js";
import { type TxId, type WriteReceipt } from "../client.js";
import {
  ANONYMOUS_JWT_ISSUER,
  LOCAL_FIRST_JWT_ISSUER,
  STATIC_BEARER_SESSION_ISSUER,
  SYSTEM_SESSION_ISSUER,
  TRUSTED_RESERVED_SESSION_TOKEN_FIELD,
  internalSessionFromVerifiedReservedJwtPayload,
  trustedReservedSessionToken,
  markTrustedReservedSession,
} from "../client-session.js";
import { SYSTEM_AUTHOR_ID } from "../system-identity.js";

type NativeDbForTest = ReturnType<
  NonNullable<ConstructorParameters<typeof NativeRuntimeAdapter>[0]>["openMemory"]
>;

async function committedTxId(receipt: WriteReceipt): Promise<TxId> {
  if (receipt.kind !== "committed") throw new Error("expected committed write receipt");
  return await receipt.txId;
}

const previousWebSocket = globalThis.WebSocket;
const TEST_RUNTIME_AUTHOR = new TextEncoder().encode('["urn:jazz:test","runtime"]');
const RESERVED_TEST_ISSUERS = [
  SYSTEM_SESSION_ISSUER,
  LOCAL_FIRST_JWT_ISSUER,
  STATIC_BEARER_SESSION_ISSUER,
  ANONYMOUS_JWT_ISSUER,
];

/** Test fixtures must state the same producer provenance as Rust bindings. */
function physicalNativeDescriptor(
  descriptor: readonly DescriptorField[],
): NativeRowDescriptorField[] {
  return descriptor.map((field, index) => {
    if (field.name === undefined) throw new Error("test native descriptor field requires a name");
    return {
      id: index + 1,
      outputName: field.name.startsWith("_app_") ? field.name.slice("_app_".length) : field.name,
      valueType: field.valueType,
      kind: "stored-column",
    };
  });
}

/** Current-row fixtures declare public names independently of carrier spelling. */
function currentNativeDescriptor(
  descriptor: readonly DescriptorField[],
  sourceNames: Readonly<Record<string, string>>,
): NativeRowDescriptorField[] {
  return descriptor.map((field, index) => {
    const name = field.name;
    if (name === undefined) throw new Error("current fixture field requires a name");
    const outputName = sourceNames[name];
    if (outputName !== undefined) {
      return { id: index + 1, outputName, valueType: field.valueType, kind: "stored-column" };
    }
    if (name === "row_uuid") return { name, valueType: field.valueType, kind: "hidden-metadata" };
    if (["$createdBy", "$createdAt", "$updatedBy", "$updatedAt"].includes(name)) {
      return { name, valueType: field.valueType, kind: "result-field" };
    }
    throw new Error(`current fixture field has no declared publication role: ${name}`);
  });
}

function decodeSchemaSource(bytes: Uint8Array) {
  return JSON.parse(new TextDecoder().decode(bytes)) as {
    tables: WasmSchema;
  };
}

function decodeTestDeltas(
  deltas: unknown[],
  _columns: readonly ColumnDescriptor[] = testSchema.todos.columns,
) {
  return deltas.map((delta) => runtimeDeltaChanges(delta as RuntimeSubscriptionDelta));
}

function runtimeDeltaChanges(delta: RuntimeSubscriptionDelta) {
  return [
    ...delta.updated.map((change) => ({
      kind: 2 as const,
      id: runtimeResultId(change.sourceId, change.occurrenceKey),
      index: change.index,
      row: change.row,
    })),
    ...delta.added.map((change) => ({
      kind: 0 as const,
      id: runtimeResultId(change.sourceId, change.occurrenceKey),
      index: change.index,
      row: change.row,
    })),
    ...delta.removed.map((change) => ({
      kind: 1 as const,
      id: runtimeResultId(change.sourceId, change.occurrenceKey),
      index: change.index,
    })),
  ];
}

function runtimeResultId(sourceId: string, occurrenceKey: Uint8Array): string {
  if (
    occurrenceKey.length === 25 &&
    occurrenceKey[0] === 1 &&
    occurrenceKey.subarray(17).every((byte) => byte === 0)
  )
    return sourceId;
  return `result:${Array.from(occurrenceKey, (byte) => byte.toString(16).padStart(2, "0")).join("")}`;
}

async function waitForFakeWebSocketNegotiation(): Promise<void> {
  for (let turn = 0; turn < 6; turn += 1) await Promise.resolve();
}

describe("NativeRuntimeAdapter server transport", () => {
  afterEach(() => {
    globalThis.WebSocket = previousWebSocket;
  });

  it("encodes indexes and merge strategies in schema source", () => {
    const schemaBytes = encodeSchema({
      counters: {
        columns: [
          {
            name: "count",
            column_type: { type: "Integer" },
            nullable: false,
            merge_strategy: "Counter",
          },
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
        indexed_columns: ["title", "done"],
      },
    });

    expect(decodeSchemaSource(schemaBytes)).toMatchObject({
      tables: {
        counters: {
          indexed_columns: ["title", "done"],
          columns: [{ name: "count", merge_strategy: "Counter" }, {}, {}],
        },
      },
    });
  });

  it("preserves GSet merge strategies for Rust validation", () => {
    const encoded = encodeSchema({
      docs: {
        columns: [
          {
            name: "tags",
            column_type: { type: "Array", element: { type: "Text" } },
            nullable: false,
            merge_strategy: "GSet",
          },
        ],
      },
    });
    expect(decodeSchemaSource(encoded).tables.docs?.columns[0]?.merge_strategy).toBe("GSet");
  });

  it("accepts and ignores a synchronous native database close result", async () => {
    const close = vi.fn(() => true);
    const runtime = new NativeRuntimeAdapter(
      { openMemory: () => fakeDb({ close }) },
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    await expect(runtime.close()).resolves.toBeUndefined();
    await expect(runtime.close()).resolves.toBeUndefined();
    expect(close).toHaveBeenCalledTimes(1);
  });

  it("resolves connect only after the owned native transport has pumped", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([Uint8Array.from([9])]);
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

    expect(transport.tickCount).toBeGreaterThan(0);
    expect(decodeWebSocketFrameBatch(sockets[0]!.sent[2]! as Uint8Array)).toEqual([
      Uint8Array.from([9]),
    ]);
  });

  it("pumps the newly owned transport before auth-refresh reconnect readiness", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transports = [new FakeTransport([]), new FakeTransport([Uint8Array.from([4, 5, 6])])];
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
    await runtime.waitForUpstreamServerConnection();
    await runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));
    await runtime.waitForUpstreamServerConnection();

    expect(sockets).toHaveLength(2);
    expect(decodeWebSocketFrameBatch(sockets[1]!.sent[2]! as Uint8Array)).toEqual([
      Uint8Array.from([4, 5, 6]),
    ]);
  });

  it("moves a strict remote read from a stalled handshake to its auth-refresh replacement", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }

      override send(data: Uint8Array | string): void {
        // Leave the first carrier in its handshake. The second one uses the
        // normal fake server response after the auth refresh replaces it.
        if (sockets[0] === this) {
          this.sent.push(data);
          return;
        }
        super.send(data);
      }
    } as unknown as typeof WebSocket;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => encodeRows([]),
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
    let settled = false;
    const read = runtime.query(JSON.stringify({ table: "todos" }), null, "edge").then((rows) => {
      settled = true;
      return rows;
    });
    await waitForFakeWebSocketNegotiation();
    expect(settled).toBe(false);

    await runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));
    await waitForFakeWebSocketNegotiation();

    expect(sockets).toHaveLength(2);
    await vi.waitFor(() => expect(settled).toBe(true));
    await expect(read).resolves.toEqual([]);
  });

  it("normalizes direct backend transport auth on connect and refresh", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemoryAsBackend: () =>
          fakeDb({ connectUpstream: () => new FakeTransport([]), tick: () => undefined }),
        openMemory: () => {
          throw new Error("ordinary open must not be selected for a backend runtime");
        },
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
      { backendMode: true, readAuthorizationHost: "trusted-serving" },
    );
    const auth = JSON.stringify({ backend_secret: "backend", jwt_token: "incidental.jwt" });

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", auth);
    await runtime.waitForUpstreamServerConnection();
    await runtime.updateAuth(auth);
    await runtime.waitForUpstreamServerConnection();

    expect(sockets).toHaveLength(2);
    for (const socket of sockets) {
      const prelude = JSON.parse(socket.sent[0] as string) as {
        peer_identity: string;
        auth: Record<string, unknown>;
      };
      expect(prelude.peer_identity).toBe('["urn:jazz:system","system"]');
      expect(prelude.auth).toEqual({ backend_secret: "backend", sub: "system" });
    }
  });

  it("waits for server admission before running a strict relation query", async () => {
    globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
    const calls: unknown[][] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (...args: unknown[]) => {
              calls.push(args);
              return encodeRelationSnapshot([]);
            },
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
    const read = runtime.query(
      JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
      null,
      "edge",
    );
    await Promise.resolve();

    expect(calls).toEqual([]);
    await waitForFakeWebSocketNegotiation();
    await expect(read).resolves.toEqual([]);
    expect(calls).toHaveLength(1);
  });

  it("retries a typed not-ready error before hello without failing strict reads", async () => {
    vi.useFakeTimers();
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }

      override send(data: Uint8Array | string): void {
        if (typeof data === "string") return super.send(data);
        if (sockets.length === 1 && isClientHelloBatch(data)) {
          this.sent.push(data);
          queueMicrotask(() => {
            this.emitMessage(
              encodeWebSocketFrameBatch([encodeWireError(6, 3, "catalogue bootstrapping")]),
            );
          });
          return;
        }
        super.send(data);
      }
    } as unknown as typeof WebSocket;
    const calls: unknown[][] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (...args: unknown[]) => {
              calls.push(args);
              return encodeRelationSnapshot([]);
            },
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
    const read = runtime.query(
      JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
      null,
      "edge",
    );
    await vi.advanceTimersByTimeAsync(25);
    await waitForFakeWebSocketNegotiation();

    await expect(read).resolves.toEqual([]);
    expect(sockets).toHaveLength(2);
    expect(calls).toHaveLength(1);
    await runtime.close();
    vi.useRealTimers();
  });

  it("cancels a pending pre-hello retry on disconnect", async () => {
    vi.useFakeTimers();
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }

      override send(data: Uint8Array | string): void {
        if (typeof data === "string") return super.send(data);
        if (isClientHelloBatch(data)) {
          this.sent.push(data);
          queueMicrotask(() => {
            this.emitMessage(
              encodeWebSocketFrameBatch([encodeWireError(6, 3, "catalogue bootstrapping")]),
            );
          });
          return;
        }
        super.send(data);
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
    await Promise.resolve();
    await runtime.disconnect();
    await vi.advanceTimersByTimeAsync(1_000);

    expect(sockets).toHaveLength(1);
    await runtime.close();
    vi.useRealTimers();
  });

  it("runs a strict relation query normally after server admission", async () => {
    globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
    const calls: unknown[][] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (...args: unknown[]) => {
              calls.push(args);
              return encodeRelationSnapshot([]);
            },
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

    await expect(
      runtime.query(
        JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
        null,
        "edge",
      ),
    ).resolves.toEqual([]);
    expect(calls).toHaveLength(1);
    expect(calls[0]?.[1]).toEqual({ tier: "edge" });
  });

  it("moves a strict relation query from a stalled handshake to its auth-refresh replacement", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }

      override send(data: Uint8Array | string): void {
        if (sockets[0] === this) {
          this.sent.push(data);
          return;
        }
        super.send(data);
      }
    } as unknown as typeof WebSocket;
    let relationQueries = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              relationQueries += 1;
              return encodeRelationSnapshot([]);
            },
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
    const read = runtime.query(
      JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
      null,
      "edge",
    );
    await waitForFakeWebSocketNegotiation();
    expect(relationQueries).toBe(0);

    await runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));
    await waitForFakeWebSocketNegotiation();

    expect(sockets).toHaveLength(2);
    await expect(read).resolves.toEqual([]);
    expect(relationQueries).toBe(1);
  });

  it("rejects a strict relation query when its pre-admission carrier terminates", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }

      override send(data: Uint8Array | string): void {
        this.sent.push(data);
      }
    } as unknown as typeof WebSocket;
    let relationQueries = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              relationQueries += 1;
              return encodeRows([]);
            },
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
    const read = runtime.query(
      JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
      null,
      "edge",
    );
    await waitForFakeWebSocketNegotiation();
    sockets[0]!.emitMessage(
      encodeWebSocketFrameBatch([encodeWireError(3, 1, "pre-admission denied")]),
    );

    await expect(read).rejects.toThrow("pre-admission denied");
    expect(relationQueries).toBe(0);
  });

  it("returns an empty result when closing during a strict relation query handshake", async () => {
    globalThis.WebSocket = class extends FakeWebSocket {
      override send(data: Uint8Array | string): void {
        this.sent.push(data);
      }
    } as unknown as typeof WebSocket;
    let relationQueries = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              relationQueries += 1;
              return encodeRows([]);
            },
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
    const read = runtime.query(
      JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
      null,
      "edge",
    );
    await waitForFakeWebSocketNegotiation();
    expect(relationQueries).toBe(0);

    await runtime.close();

    await expect(read).resolves.toEqual([]);
    expect(relationQueries).toBe(0);
  });

  it("requires native db bindings to expose a tick scheduler", () => {
    expect(
      () =>
        new NativeRuntimeAdapter(
          {
            openMemory: () => ({
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
        ),
    ).toThrow("Native runtime requires db.setTickScheduler");
  });

  it("reports websocket auth failures through the auth failure callback", async () => {
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
    const authFailures: string[] = [];
    runtime.onAuthFailure((reason) => authFailures.push(reason));

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await waitForFakeWebSocketNegotiation();

    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([encodeWireError(3, 1, "token expired")]));
    await Promise.resolve();

    expect(authFailures).toEqual(["expired"]);
    expect(transport.received).toEqual([]);
  });

  it("reports pre-hello auth failures and reconnects with refreshed auth", async () => {
    const sockets: FakeWebSocket[] = [];
    let allowServerHello = false;
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }

      override send(data: Uint8Array | string): void {
        if (allowServerHello) super.send(data);
        else this.sent.push(data);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    let upstreamConnections = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => {
              upstreamConnections += 1;
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
    const authFailures: string[] = [];
    runtime.onAuthFailure((reason) => authFailures.push(reason));

    runtime.connect(
      "ws://127.0.0.1:4200/apps/app-a/ws",
      JSON.stringify({ jwt_token: "invalid.jwt" }),
    );
    await waitForFakeWebSocketNegotiation();
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([encodeWireError(3, 1, "invalid token")]));
    await waitForFakeWebSocketNegotiation();

    expect(authFailures).toEqual(["invalid"]);
    expect(upstreamConnections).toBe(0);
    expect(sockets[0]!.closed).toBe(true);
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([encodeWireServerHello()]));
    await waitForFakeWebSocketNegotiation();
    expect(authFailures).toEqual(["invalid"]);
    expect(upstreamConnections).toBe(0);

    allowServerHello = true;
    await runtime.updateAuth(JSON.stringify({ jwt_token: "fresh.jwt" }));
    await waitForFakeWebSocketNegotiation();

    expect(sockets).toHaveLength(2);
    expect(upstreamConnections).toBe(1);
    expect(JSON.parse(sockets[1]!.sent[0] as string).jwt_token).toBe("fresh.jwt");
  });

  it("rejects a strict remote read when its pre-admission carrier terminates without replacement", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }

      override send(data: Uint8Array | string): void {
        // Hold the handshake so the remote read is waiting on this carrier
        // when the authority sends its terminal pre-admission error.
        this.sent.push(data);
      }
    } as unknown as typeof WebSocket;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => encodeRows([]),
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
    const read = runtime.query(JSON.stringify({ table: "todos" }), null, "edge");
    await waitForFakeWebSocketNegotiation();

    sockets[0]!.emitMessage(
      encodeWebSocketFrameBatch([encodeWireError(3, 1, "pre-admission denied")]),
    );

    await expect(read).rejects.toThrow("pre-admission denied");
  });

  it("does not report non-auth websocket errors as auth failures", async () => {
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
    const authFailures: string[] = [];
    runtime.onAuthFailure((reason) => authFailures.push(reason));

    runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await waitForFakeWebSocketNegotiation();

    sockets[0]!.emitMessage(
      encodeWebSocketFrameBatch([encodeWireError(5, 3, "conflicting commit unit")]),
    );
    await Promise.resolve();

    expect(authFailures).toEqual([]);
    expect(transport.received).toEqual([]);
  });

  it("fails active subscriptions when the websocket reports a fatal wire error", async () => {
    const sockets: FakeWebSocket[] = [];
    globalThis.WebSocket = class extends FakeWebSocket {
      constructor(url: string) {
        super(url);
        sockets.push(this);
      }
    } as unknown as typeof WebSocket;
    const transport = new FakeTransport([]);
    const subscription = {
      closed: false,
      readAll: () => [],
      close() {
        this.closed = true;
        return true;
      },
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            connectUpstream: () => transport,
            subscribe: () => subscription,
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
    await waitForFakeWebSocketNegotiation();
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "edge");
    const updates = vi.fn();
    runtime.executeSubscription(handle, updates);
    await Promise.resolve();

    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([encodeWireError(5, 3, "server died")]));
    await Promise.resolve();

    expect(subscription.closed).toBe(true);
    expect(updates).toHaveBeenCalledTimes(1);
    expect(updates.mock.calls[0]![0]).toBeInstanceOf(Error);
    expect((updates.mock.calls[0]![0] as Error).message).toBe("server died");
    expect(updates.mock.calls[0]).toHaveLength(1);
  });

  it.each(["local", "edge", "global"] as const)(
    "forwards the core's ready %s subscription reset directly",
    (tier) => {
      const rowId = uuidBytes("00000000-0000-0000-0000-000000000123");
      const events = [
        {
          type: "delta",
          reset: true,
          settled: true,
          delta: encodeSubscriptionDelta({
            added: [{ table: "todos", rowId, title: "settled row" }],
            updated: [],
            removed: [],
          }),
        },
      ];
      const runtime = new NativeRuntimeAdapter(
        {
          openMemory: () =>
            fakeDb({
              subscribe: () => ({
                readAll: () => events.splice(0),
                close: () => true,
              }),
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

      const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, tier);
      const updates = vi.fn();
      runtime.executeSubscription(handle, updates);

      expect(updates).toHaveBeenCalledTimes(1);
      const decoded = decodeTestDeltas([updates.mock.calls[0]![0]]);
      expect(decoded).toHaveLength(1);
      expect(decoded[0]).toHaveLength(1);
      const firstDelta = decoded[0]![0]!;
      expect(firstDelta).toMatchObject({
        kind: 0,
        id: "00000000-0000-0000-0000-000000000123",
        index: 0,
      });
      if (firstDelta.kind !== 0) {
        throw new Error(`expected added delta, got kind ${firstDelta.kind}`);
      }
      expect(firstDelta.row.values[0]).toEqual({ type: "Text", value: "settled row" });
    },
  );

  it("uses the caller-supplied table for update and delete", () => {
    const calls: unknown[] = [];
    const write = {
      payload: new Uint8Array(),
      wait: async () => undefined,
      writeState: () => ({}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => Uint8Array.from([0]),
            update: (table: string, rowId: Uint8Array, patch: Uint8Array) => {
              calls.push(["update", table, rowId, patch]);
              return write;
            },
            delete: (table: string, rowId: Uint8Array) => {
              calls.push(["delete", table, rowId]);
              return write;
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        todos: {
          columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
        },
        projects: {
          columns: [{ name: "name", column_type: { type: "Text" }, nullable: false }],
        },
      },
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.update("projects", "00000000-0000-0000-0000-000000000001", {
      name: { type: "Text", value: "Project" },
    });
    runtime.delete("projects", "00000000-0000-0000-0000-000000000001");

    expect(calls.map((call) => (call as unknown[]).slice(0, 2))).toEqual([
      ["update", "projects"],
      ["delete", "projects"],
    ]);
  });

  it("routes typed large-value update descriptors only for ordinary update contexts", () => {
    const calls: unknown[][] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => Uint8Array.from([0]),
            updateLargeValues: (...args: unknown[]) => {
              calls.push(args);
              return fakeWrite();
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
    const descriptors = [
      {
        kind: "splice",
        column: "title",
        within: { kind: "text_utf16", from: 0, to: 4 },
        splices: [{ at: 0, delete: 0, insert: [120] }],
      },
    ];

    const receipt = runtime.updateLargeValues(
      "todos",
      "00000000-0000-0000-0000-000000000001",
      {},
      descriptors,
      JSON.stringify({ updated_at: 43_000 }),
    );

    expect(receipt.kind).toBe("committed");
    expect(calls).toHaveLength(1);
    expect(calls[0]?.[0]).toBe("todos");
    expect(formatUuid(calls[0]?.[1] as Uint8Array)).toBe("00000000-0000-0000-0000-000000000001");
    expect(calls[0]?.[3]).toBe(descriptors);
    expect(calls[0]?.[4]).toBe(43_000);

    expect(() =>
      runtime.updateLargeValues(
        "todos",
        "00000000-0000-0000-0000-000000000001",
        {},
        descriptors,
        JSON.stringify({ transaction_id: "00000000000000000000000000000001" }),
      ),
    ).toThrow("Update failed: WriteError");
    expect(() =>
      runtime.updateLargeValues(
        "todos",
        "00000000-0000-0000-0000-000000000001",
        {},
        descriptors,
        JSON.stringify({ branch_view: { head: { values: {} } } }),
      ),
    ).toThrow("Typed large-value updates are not supported in branch views.");
    expect(() =>
      runtime.updateLargeValues(
        "todos",
        "00000000-0000-0000-0000-000000000001",
        {},
        descriptors,
        JSON.stringify({ user_id: "00000000-0000-0000-0000-000000000009" }),
      ),
    ).toThrow("Typed large-value updates do not yet support an attributed identity.");
    expect(calls).toHaveLength(1);
  });

  it("accepts typed partial projections as ordinary carrier columns", async () => {
    const rowId = uuidBytes("00000000-0000-0000-0000-000000000001");
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => encodeRows([{ table: "todos", rowId, title: "A😀BC" }]),
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

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          select_columns: [{ kind: "text_utf16", column: "title", from: 1, to: 3 }],
        }),
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "A😀BC" }],
      },
    ]);
  });

  it("serves default and local queries from fresh local state", async () => {
    const insertedRowIds: Uint8Array[] = [];
    const write = {
      payload: new Uint8Array(),
      wait: async () => undefined,
      writeState: () => ({}),
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () =>
              encodeRows([
                {
                  table: "todos",
                  rowId: insertedRowIds[0]!,
                  title: "fresh local write",
                },
              ]),
            insert: (_table: string, _cells: Uint8Array, options?: { rowId?: Uint8Array }) => {
              insertedRowIds.push(options?.rowId ?? new Uint8Array(16));
              return write;
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
    runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "fresh local write" },
      },
      null,
      "00000000-0000-0000-0000-000000000000",
    );

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000000",
        values: [{ type: "Text", value: "fresh local write" }],
      },
    ]);
    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "local")).resolves.toEqual(
      [
        {
          table: "todos",
          id: "00000000-0000-0000-0000-000000000000",
          values: [{ type: "Text", value: "fresh local write" }],
        },
      ],
    );
  });

  it("routes exact and head-over-base mutation targets to branch-aware bindings", () => {
    const calls: unknown[][] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            insert: (...args: unknown[]) => {
              calls.push(["insert", ...args]);
              return fakeWrite();
            },
            update: (...args: unknown[]) => {
              calls.push(["update", ...args]);
              return fakeWrite();
            },
            upsert: (...args: unknown[]) => {
              calls.push(["upsert", ...args]);
              return fakeWrite();
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
    const head = { values: { workspace: [14, 14] } };
    const base = { Current: { values: { workspace: [14, 2] } } };

    runtime.insert(
      "todos",
      { title: { type: "Text", value: "branch" } },
      JSON.stringify({ branch_view: { head } }),
      "00000000-0000-0000-0000-000000000001",
    );
    runtime.update(
      "todos",
      "00000000-0000-0000-0000-000000000001",
      { title: { type: "Text", value: "updated" } },
      JSON.stringify({ branch_view: { head, base } }),
    );
    runtime.upsert(
      "todos",
      "00000000-0000-0000-0000-000000000001",
      {},
      JSON.stringify({ branch_view: { head, base } }),
    );

    expect(calls[0]?.[0]).toBe("insert");
    expect(calls[0]?.at(-1)).toMatchObject({ branch: head });
    expect(calls[1]?.[0]).toBe("update");
    expect(calls[1]?.at(-1)).toMatchObject({ head, base });
    expect(calls[2]?.[0]).toBe("upsert");
    expect(calls[2]?.at(-1)).toMatchObject({ head, base });
  });

  it("runs scheduled core ticks before post-wait edge reads", async () => {
    let schedulerCallback: ((urgency: "immediate" | "deferred") => void) | undefined;
    let ticked = false;
    let subscriptionDrained = false;
    const rowId = uuidBytes("00000000-0000-0000-0000-000000000123");
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => ({
          all: () =>
            ticked
              ? encodeRows([
                  {
                    table: "todos",
                    rowId,
                    title: "visible after scheduled tick",
                  },
                ])
              : encodeRows([]),
          subscribe: () => ({
            readAll: () => {
              if (!ticked || subscriptionDrained) return [];
              subscriptionDrained = true;
              return [
                subscriptionReset([
                  {
                    table: "todos",
                    rowId,
                    title: "visible after scheduled tick",
                  },
                ]),
              ];
            },
          }),
          insert: () => {
            schedulerCallback?.("deferred");
            return fakeWrite();
          },
          setTickScheduler: (callback: (urgency: "immediate" | "deferred") => void) => {
            schedulerCallback = callback;
          },
          connectUpstream: () => new FakeTransport([]),
          tick: () => {
            ticked = true;
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
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "edge");
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    const inserted = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "visible after scheduled tick" },
      },
      null,
      "00000000-0000-0000-0000-000000000123",
    );

    await runtime.waitForTransaction(await committedTxId(inserted), "edge");

    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "edge")).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000123",
        values: [{ type: "Text", value: "visible after scheduled tick" }],
      },
    ]);
    expect(decodeTestDeltas(deltas.slice(0, 2))).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000123",
          row: {
            id: "00000000-0000-0000-0000-000000000123",
            values: [{ type: "Text", value: "visible after scheduled tick" }],
          },
          index: 0,
        },
      ],
    ]);
  });

  it("keeps a session-scoped client query on the client-local read path", async () => {
    let clientReads = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, _opts: unknown, _tx: unknown, author: Uint8Array) => {
              if (author) {
                throw new Error("ordinary client query must not use trusted serving");
              }
              clientReads += 1;
              return encodeRows([
                {
                  table: "todos",
                  rowId: new Uint8Array(16),
                  title: "client local",
                },
              ]);
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

    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({
          user_id: "00000000-0000-0000-0000-0000000000a1",
          claims: {},
          issuer: "https://issuer.example",
          authMode: "external",
        }),
        "local",
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000000",
        values: [{ type: "Text", value: "client local" }],
      },
    ]);
    expect(clientReads).toBe(1);
  });

  it.each(["query", "subscription"] as const)(
    "installs client correlation claims before opening a %s without preparing a native plan",
    async (kind) => {
      const app = s.defineApp({ todos: s.table({ title: s.string() }) });
      let releaseClaims!: () => void;
      const installed = new Promise<void>((resolve) => {
        releaseClaims = resolve;
      });
      const setSessionClaims = vi.fn(() => installed);
      const all = vi.fn(() => encodeRows([]));
      const subscribe = vi.fn(() => ({ readAll: () => [], close: () => undefined }));
      const runtime = new NativeRuntimeAdapter(
        { openMemory: () => fakeDb({ setSessionClaims, all, subscribe }) } as never,
        app.todos._schema,
        new Uint8Array(16),
        TEST_RUNTIME_AUTHOR,
        1,
        true,
      );
      const session = JSON.stringify({
        user_id: "00000000-0000-0000-0000-0000000000a1",
        issuer: "https://issuer.example",
        authMode: "external",
        claims: { role: "reader" },
      });
      const open = () =>
        kind === "query"
          ? runtime.query(app.todos._build(), session, "local")
          : runtime.createSubscription(app.todos._build(), session, "local");
      try {
        const pending = open();
        expect(setSessionClaims).toHaveBeenCalledOnce();
        expect(setSessionClaims).toHaveBeenCalledWith(expect.objectContaining({ role: "reader" }));
        expect(all).not.toHaveBeenCalled();
        expect(subscribe).not.toHaveBeenCalled();
        releaseClaims();
        await pending;
        const read = kind === "query" ? all : subscribe;
        await vi.waitFor(() => expect(read).toHaveBeenCalledOnce());
        await open();
        expect(read).toHaveBeenCalledTimes(2);
        expect(setSessionClaims).toHaveBeenCalledOnce();
      } finally {
        releaseClaims();
        await runtime.close();
      }
    },
  );

  it("returns unknown locally without consulting hidden policy evidence", () => {
    let authoritativeChecks = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            authorizeReadForIdentity: () => {
              authoritativeChecks += 1;
              return "allowed" as const;
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
    const session = {
      issuer: "https://issuer.example",
      user_id: "00000000-0000-0000-0000-0000000000a1",
      claims: {},
      authMode: "external" as const,
    };

    expect(runtime.canReadLocally("todos", "00000000-0000-0000-0000-000000000001", session)).toBe(
      "unknown",
    );
    expect(
      runtime.canInsertLocally("todos", { title: { type: "Text", value: "candidate" } }, session),
    ).toBe("unknown");
    expect(authoritativeChecks).toBe(0);
  });

  it("returns unknown while offline", async () => {
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
    await runtime.disconnect();

    expect(
      runtime.canDeleteLocally("todos", "00000000-0000-0000-0000-000000000001", {
        user_id: "00000000-0000-0000-0000-0000000000a1",
        claims: {},
        issuer: "https://issuer.example",
        authMode: "external",
      }),
    ).toBe("unknown");
    await expect(
      runtime.requestDeletePermissionAdvice("todos", "00000000-0000-0000-0000-000000000001"),
    ).resolves.toBe("unknown");
  });

  it("cancels native permission waiters when authority advice times out", async () => {
    vi.useFakeTimers();
    let cancellations = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            requestDeletePermissionAdvice: () => ({
              promise: new Promise(() => {}),
              cancel: () => {
                cancellations += 1;
              },
            }),
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
    Object.assign(runtime as object, { serverTransport: {}, serverCarrier: {} });

    const advice = runtime.requestDeletePermissionAdvice(
      "todos",
      "00000000-0000-0000-0000-000000000001",
    );
    await vi.advanceTimersByTimeAsync(2_000);

    await expect(advice).resolves.toBe("unknown");
    expect(cancellations).toBe(1);
    vi.useRealTimers();
  });

  it("delegates each scoped backend advice request with its immutable session binding", async () => {
    const authoritative = vi.fn(() => "allowed" as const);
    const backend = fakeDb({
      requestInsertPermissionAdvice: authoritative,
      requestReadPermissionAdvice: authoritative,
      requestUpdatePermissionAdvice: authoritative,
      requestDeletePermissionAdvice: authoritative,
      tick: () => undefined,
    });
    const runtime = new NativeRuntimeAdapter(
      null,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
      { readAuthorizationHost: "trusted-serving", backendMode: true, db: backend },
    );
    Object.assign(runtime as object, { serverTransport: {}, serverCarrier: {} });
    const session = {
      issuer: "https://issuer.example",
      user_id: "requester",
      claims: {},
      authMode: "external" as const,
    };
    const id = "00000000-0000-0000-0000-000000000001";
    await expect(
      Promise.all([
        runtime.requestInsertPermissionAdvice(
          "todos",
          { title: { type: "Text", value: "candidate" } },
          session,
        ),
        runtime.requestReadPermissionAdvice("todos", id, session),
        runtime.requestUpdatePermissionAdvice("todos", id, {}, session),
        runtime.requestDeletePermissionAdvice("todos", id, session),
      ]),
    ).resolves.toEqual(["allowed", "allowed", "allowed", "allowed"]);
    expect(authoritative).toHaveBeenCalledTimes(4);
    const delegatedIdentity = expect.any(Uint8Array);
    for (const call of authoritative.mock.calls) {
      expect(call.at(-2)).toEqual(delegatedIdentity);
      // Rust derives reserved authMode from the admitted identity. Do not
      // synthesize a provider claim that differs from the authenticated JWT.
      expect(call.at(-1)).toEqual({ iss: session.issuer, sub: session.user_id });
    }
    const forgedSystem = { ...session, issuer: SYSTEM_SESSION_ISSUER, user_id: SYSTEM_AUTHOR_ID };
    await expect(runtime.requestReadPermissionAdvice("todos", id, forgedSystem)).resolves.toBe(
      "unknown",
    );
    expect(authoritative).toHaveBeenCalledTimes(4);
    const trustedSystem = markTrustedReservedSession({ ...forgedSystem });
    await expect(runtime.requestReadPermissionAdvice("todos", id, trustedSystem)).resolves.toBe(
      "allowed",
    );
    expect(authoritative).toHaveBeenCalledTimes(5);
    // The unscoped connection operation still has its ordinary advice path.
    await expect(runtime.requestReadPermissionAdvice("todos", id)).resolves.toBe("allowed");
    expect(authoritative).toHaveBeenCalledTimes(6);
  });

  it("fails closed for malformed direct and pollable native permission advice", async () => {
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            requestInsertPermissionAdvice: () => "permit" as never,
            requestReadPermissionAdvice: () => ({
              poll: () => "permit",
              cancel: () => {},
            }),
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
    Object.assign(runtime as object, { serverTransport: {}, serverCarrier: {} });

    await expect(
      runtime.requestInsertPermissionAdvice("todos", {
        title: { type: "Text", value: "candidate" },
      }),
    ).resolves.toBe("unknown");
    await expect(
      runtime.requestReadPermissionAdvice("todos", "00000000-0000-0000-0000-000000000001"),
    ).resolves.toBe("unknown");
  });

  it("cancels a pending pollable native permission request on runtime close", async () => {
    let cancellations = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            requestDeletePermissionAdvice: () => ({
              poll: () => null,
              cancel: () => {
                cancellations += 1;
              },
            }),
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
    Object.assign(runtime as object, {
      serverTransport: { tick: () => 0, recvWireFrames: () => [], close: () => true },
      serverCarrier: { close: () => {} },
    });

    const advice = runtime.requestDeletePermissionAdvice(
      "todos",
      "00000000-0000-0000-0000-000000000001",
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    await runtime.close();

    await expect(advice).resolves.toBe("unknown");
    expect(cancellations).toBe(1);
  });

  it("does not locally evaluate permission advice even on a serving-configured runtime", () => {
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            authorizeReadForIdentity: () => "allowed" as const,
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
      { readAuthorizationHost: "trusted-serving" },
    );
    const session = {
      issuer: "https://issuer.example",
      user_id: "00000000-0000-0000-0000-0000000000a1",
      claims: {},
      authMode: "external" as const,
    };

    expect(runtime.canReadLocally("todos", "00000000-0000-0000-0000-000000000001", session)).toBe(
      "unknown",
    );
    expect(
      runtime.canInsertLocally("todos", { title: { type: "Text", value: "candidate" } }, session),
    ).toBe("unknown");
  });

  it("uses trusted serving only when the host explicitly selects it", async () => {
    const authors: string[] = [];
    const claimUpdates: Array<{ author: string; claims: Record<string, unknown> }> = [];
    const externalIssuer = "https://issuer.example";
    const externalUserId = "00000000-0000-0000-0000-0000000000a1";
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (
              _query: unknown,
              _opts: unknown,
              _tx: unknown,
              author: Uint8Array,
              claims: Record<string, unknown>,
            ) => {
              if (!author) throw new Error("trusted serving query must provide an author");
              authors.push(new TextDecoder().decode(author));
              claimUpdates.push({ author: new TextDecoder().decode(author), claims });
              return encodeRows([
                {
                  table: "todos",
                  rowId: new Uint8Array(16),
                  title: "trusted serving",
                },
              ]);
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
      { readAuthorizationHost: "trusted-serving" },
    );

    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({
          user_id: externalUserId,
          claims: { role: "reader", subject: "application-owned-subject" },
          issuer: externalIssuer,
          authMode: "external",
        }),
        "local",
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000000",
        values: [{ type: "Text", value: "trusted serving" }],
      },
    ]);
    expect(authors).toEqual([`["${externalIssuer}","${externalUserId}"]`]);
    expect(claimUpdates).toEqual([
      {
        author: `["${externalIssuer}","${externalUserId}"]`,
        claims: {
          role: "reader",
          subject: "application-owned-subject",
          iss: externalIssuer,
          sub: externalUserId,
        },
      },
    ]);
  });

  it("rejects reserved issuers in public read sessions but preserves private trusted identity", async () => {
    const authors: string[] = [];
    const privateSystemAuthor = JSON.stringify([SYSTEM_SESSION_ISSUER, SYSTEM_AUTHOR_ID]);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, _opts: unknown, _tx: unknown, author: Uint8Array) => {
              if (!author) throw new Error("trusted serving query must provide an author");
              authors.push(new TextDecoder().decode(author));
              return encodeRows([
                {
                  table: "todos",
                  rowId: new Uint8Array(16),
                  title: "private trusted identity",
                },
              ]);
            },
            subscribe: () => {
              throw new Error("reserved public session must be rejected before subscribing");
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new TextEncoder().encode(privateSystemAuthor),
      1,
      true,
      { readAuthorizationHost: "trusted-serving" },
    );

    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "local")).resolves.toEqual(
      [
        {
          table: "todos",
          id: "00000000-0000-0000-0000-000000000000",
          values: [{ type: "Text", value: "private trusted identity" }],
        },
      ],
    );
    expect(authors).toEqual([privateSystemAuthor]);

    for (const issuer of RESERVED_TEST_ISSUERS) {
      const sessionJson = JSON.stringify({
        issuer,
        user_id: "public-caller",
        claims: {},
        authMode:
          issuer === LOCAL_FIRST_JWT_ISSUER
            ? "local-first"
            : issuer === ANONYMOUS_JWT_ISSUER
              ? "anonymous"
              : "external",
      });
      await expect(
        runtime.query(JSON.stringify({ table: "todos" }), sessionJson, "local"),
      ).rejects.toThrow("reserved issuer");
      expect(() =>
        runtime.createSubscription(JSON.stringify({ table: "todos" }), sessionJson, "local"),
      ).toThrow("reserved issuer");
    }
  });

  it("admits verified reserved sessions carrying the in-process runtime capability", async () => {
    const authors: string[] = [];
    const trustedSession = internalSessionFromVerifiedReservedJwtPayload(
      { iss: LOCAL_FIRST_JWT_ISSUER, sub: "verified-user" },
      "local-first",
    )!;
    const runtimeSessionJson = JSON.stringify({
      ...trustedSession,
      [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: trustedReservedSessionToken(trustedSession),
    });
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, _opts: unknown, _tx: unknown, author: Uint8Array) => {
              if (!author) throw new Error("trusted serving query must provide an author");
              authors.push(new TextDecoder().decode(author));
              return encodeRows([
                {
                  table: "todos",
                  rowId: new Uint8Array(16),
                  title: "verified reserved identity",
                },
              ]);
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
      { readAuthorizationHost: "trusted-serving" },
    );

    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), runtimeSessionJson, "local"),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000000",
        values: [{ type: "Text", value: "verified reserved identity" }],
      },
    ]);
    expect(authors).toEqual(['["urn:jazz:local-first","verified-user"]']);
  });

  it("keeps first-party reserved browser sessions out of public native identity ingress", async () => {
    const setIdentityClaims = vi.fn(() => {
      // This represents the raw NAPI/WASM identity ABI. It deliberately
      // rejects reserved canonical subjects, just as the real binding does.
      throw new Error("author issuer is reserved");
    });
    const all = vi.fn(() =>
      encodeRows([
        {
          table: "todos",
          rowId: new Uint8Array(16),
          title: "local replica",
        },
      ]),
    );
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all,
            setIdentityClaims,
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

    for (const [issuer, authMode] of [
      [LOCAL_FIRST_JWT_ISSUER, "local-first"],
      [ANONYMOUS_JWT_ISSUER, "anonymous"],
    ] as const) {
      const trustedSession = internalSessionFromVerifiedReservedJwtPayload(
        { iss: issuer, sub: `${authMode}-browser-user` },
        authMode,
      )!;
      await expect(
        runtime.query(
          JSON.stringify({ table: "todos" }),
          JSON.stringify({
            ...trustedSession,
            [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: trustedReservedSessionToken(trustedSession),
          }),
          "local",
        ),
      ).resolves.toHaveLength(1);
    }

    // A serialized lookalike has no in-process capability and must still be
    // rejected before it can reach either the local replica or the native ABI.
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({
          issuer: LOCAL_FIRST_JWT_ISSUER,
          user_id: "forged-browser-user",
          claims: {},
          authMode: "local-first",
        }),
        "local",
      ),
    ).rejects.toThrow("reserved issuer");

    expect(all).toHaveBeenCalledTimes(2);
    expect(setIdentityClaims).not.toHaveBeenCalled();
  });

  it("decodes fixed-width array columns from native row batches", async () => {
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => encodeArrayRows(),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      arraySchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    await expect(runtime.query(JSON.stringify({ table: "arrays" }))).resolves.toEqual([
      {
        table: "arrays",
        id: "00000000-0000-0000-0000-000000000010",
        values: [
          {
            type: "Array",
            value: [
              { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
              { type: "Uuid", value: "00000000-0000-0000-0000-000000000002" },
            ],
          },
          {
            type: "Array",
            value: [
              { type: "Double", value: 65536 },
              { type: "Double", value: 1234 },
            ],
          },
        ],
      },
    ]);
  });

  it("carries scalar comparison relation IR in the prepared Query envelope", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return new Uint8Array([0]);
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

    const relation = {
      Limit: {
        input: {
          Filter: {
            input: { TableScan: { table: "todos" } },
            predicate: {
              Cmp: {
                left: { column: "title" },
                op: "Gt",
                right: { Literal: { type: "Text", value: "m" } },
              },
            },
          },
        },
        limit: 5,
      },
    };
    await runtime.query(JSON.stringify({ table: "todos", relation_ir: relation }));

    expect(preparedBytes).toEqual(queryWithPredicates("todos", [], { relation }));
  });

  it("trusts native prepared queries for simple equality relation filters", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "keep",
                },
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
                  title: "drop",
                },
              ]);
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

    const relation = {
      Filter: {
        input: { TableScan: { table: "todos" } },
        predicate: {
          Cmp: {
            left: { column: "title" },
            op: "Eq",
            right: { Literal: { type: "Text", value: "keep" } },
          },
        },
      },
    };
    await expect(
      runtime.query(JSON.stringify({ table: "todos", relation_ir: relation })),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "keep" }],
      },
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000002",
        values: [{ type: "Text", value: "drop" }],
      },
    ]);
    expect(preparedBytes).toEqual(queryWithPredicates("todos", [], { relation }));
  });

  it("carries a payload enum match relation filter in the Query envelope", () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: (query: Uint8Array) => {
              preparedBytes = query;
              return new ReadableStream();
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        events: {
          columns: [
            {
              name: "event",
              column_type: {
                type: "EnumPayload",
                cases: [
                  {
                    name: "message",
                    fields: [{ name: "level", column_type: { type: "Integer" }, nullable: false }],
                  },
                ],
              },
              nullable: false,
            },
          ],
        },
      },
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "events",
        relation_ir: {
          Project: {
            input: {
              Filter: {
                input: { TableScan: { table: "events" } },
                predicate: {
                  EnumMatch: {
                    column: { column: "event", scope: "events" },
                    case: "message",
                    payload: {
                      Cmp: {
                        left: { column: "level" },
                        op: "Eq",
                        right: { Literal: { type: "Integer", value: 2 } },
                      },
                    },
                  },
                },
              },
            },
            columns: [{ alias: "event", expr: { Column: { column: "event", scope: "events" } } }],
          },
        },
      }),
    );

    expect(handle).toBe(1);
    expect(preparedBytes).toEqual(
      queryWithPredicates("events", [], {
        relation: {
          Project: {
            input: {
              Filter: {
                input: { TableScan: { table: "events" } },
                predicate: {
                  EnumMatch: {
                    column: { column: "event", scope: "events" },
                    case: "message",
                    payload: {
                      Cmp: {
                        left: { column: "level" },
                        op: "Eq",
                        right: { Literal: { type: "Integer", value: 2 } },
                      },
                    },
                  },
                },
              },
            },
            columns: [{ alias: "event", expr: { Column: { column: "event", scope: "events" } } }],
          },
        },
      }),
    );
  });

  it.each(["stream", "native", "native-close-throws"] as const)(
    "forwards %s subscription source failures exactly once without an unhandled rejection",
    async (kind) => {
      const error = new Error("subscription source failed");
      let controller!: ReadableStreamDefaultController<unknown>;
      const cleanupError = new Error("source close failed");
      const cleanupLog = vi.spyOn(console, "error").mockImplementation(() => {});
      const close = vi.fn(() => true);
      if (kind === "native-close-throws")
        close.mockImplementationOnce(() => {
          throw cleanupError;
        });
      const source =
        kind === "stream"
          ? new ReadableStream({
              start(value) {
                controller = value;
              },
            })
          : {
              readAll: () => {
                throw error;
              },
              close,
            };
      const runtime = runtimeWithSubscriptionSource(source);
      const app = s.defineApp({ todos: s.table({ title: s.string() }) });
      const handle = runtime.createSubscription(app.todos._build());
      const callback = vi.fn();
      runtime.executeSubscription(handle, callback);
      if (kind === "stream") controller.error(error);
      // Cross a host turn: an escaped void reader promise is an unhandled
      // rejection here, which Vitest treats as a failed test run.
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(callback.mock.calls).toEqual([[error]]);
      expect(() => runtime.executeSubscription(handle, callback)).toThrow("already been activated");
      expect(callback).toHaveBeenCalledTimes(1);
      if (kind !== "stream") expect(close).toHaveBeenCalledTimes(1);
      if (kind === "native-close-throws") {
        expect(cleanupLog).toHaveBeenCalledWith(
          "Jazz subscription source cleanup failed",
          cleanupError,
        );
      } else expect(cleanupLog).not.toHaveBeenCalled();
      await runtime.close();
      cleanupLog.mockRestore();
    },
  );

  it.each(["native", "stream"] as const)(
    "activates a %s subscription once without replacing its callback or replaying rows",
    async (kind) => {
      const rowId = uuidBytes("00000000-0000-0000-0000-000000000123");
      const opening = subscriptionReset([{ table: "todos", rowId, title: "initial" }]);
      const change = {
        type: "delta",
        delta: encodeSubscriptionDelta({
          added: [],
          updated: [{ table: "todos", rowId, title: "updated" }],
          removed: [],
          updatedIndices: [4],
        }),
      };
      const readAll = vi.fn().mockReturnValueOnce([opening, change]).mockReturnValue([]);
      const source =
        kind === "native"
          ? { readAll, close: vi.fn() }
          : new ReadableStream({
              start(controller) {
                controller.enqueue(opening);
                controller.enqueue(change);
              },
            });
      const runtime = runtimeWithSubscriptionSource(source);
      const app = s.defineApp({ todos: s.table({ title: s.string() }) });
      const handle = runtime.createSubscription(app.todos._build());
      const replacement = vi.fn();
      const callback = vi.fn(() => {
        expect(() => runtime.executeSubscription(handle, replacement)).toThrow(
          "already been activated",
        );
      });
      try {
        // Core wakes before activation must not consume the opening event.
        await new Promise((resolve) => setTimeout(resolve, 0));
        expect(readAll).not.toHaveBeenCalled();
        runtime.executeSubscription(handle, callback);
        if (kind === "native") expect(callback).toHaveBeenCalledTimes(2);
        await vi.waitFor(() => expect(callback).toHaveBeenCalledTimes(2));
        expect(callback.mock.calls[0]).toEqual([expect.objectContaining({ reset: true })]);
        expect(callback.mock.calls[1]).toEqual([
          expect.objectContaining({
            added: [],
            updated: [expect.objectContaining({ index: 4 })],
            removed: [],
          }),
        ]);
        expect(() => runtime.executeSubscription(handle, replacement)).toThrow(
          "already been activated",
        );
        expect(callback).toHaveBeenCalledTimes(2);
        expect(replacement).not.toHaveBeenCalled();
      } finally {
        await runtime.close();
      }
    },
  );

  it.each(["unsubscribe", "close"] as const)(
    "suppresses a pending stream read rejection after subscription %s",
    async (termination) => {
      let rejectRead!: (error: Error) => void;
      const pending = new Promise<never>((_, reject) => {
        rejectRead = reject;
      });
      // A binding read can already be in flight when its owner is retired.
      // Cancelling its source must not turn that late rejection into an app error.
      const cancel = vi.fn(async () => undefined);
      const runtime = runtimeWithSubscriptionSource({
        getReader: () => ({ read: () => pending, cancel }),
      });
      const app = s.defineApp({ todos: s.table({ title: s.string() }) });
      const handle = runtime.createSubscription(app.todos._build());
      const callback = vi.fn();
      runtime.executeSubscription(handle, callback);
      if (termination === "unsubscribe") runtime.unsubscribe(handle);
      else await runtime.close();
      rejectRead(new Error("late retired source failure"));
      await new Promise((resolve) => setTimeout(resolve, 0));
      expect(callback).not.toHaveBeenCalled();
      expect(cancel).toHaveBeenCalled();
      await runtime.close();
    },
  );

  it("trusts native reset deltas for simple equality relation filters", async () => {
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: (query: Uint8Array) => {
              preparedBytes = query;
              return new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              });
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
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "todos" } },
            predicate: {
              Cmp: {
                left: { column: "title" },
                op: "Eq",
                right: { Literal: { type: "Text", value: "keep" } },
              },
            },
          },
        },
      }),
    );
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue(
      subscriptionReset([
        {
          table: "todos",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
          title: "keep",
        },
        {
          table: "todos",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
          title: "drop",
        },
      ]),
    );
    await Promise.resolve();

    expect(decodeTestDeltas(deltas.slice(0, 2))).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000001",
            values: [{ type: "Text", value: "keep" }],
          },
        },
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000002",
          index: 1,
          row: {
            id: "00000000-0000-0000-0000-000000000002",
            values: [{ type: "Text", value: "drop" }],
          },
        },
      ],
    ]);
    expectPreparedRelationEnvelope(preparedBytes!, "todos");
  });

  it("routes Join relation IR through the canonical Query API", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "should not be read",
                },
              ]);
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

    await expect(
      runtime.query(JSON.stringify({ table: "todos", relation_ir: unsupportedJoinRelationIr() })),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "should not be read" }],
      },
    ]);
    expect(calls).toEqual(["all"]);
  });

  it("preserves raw subscription literal number spellings in the Query relation envelope", () => {
    let queryBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: (bytes: Uint8Array) => {
              queryBytes = bytes;
              return new ReadableStream();
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

    runtime.createSubscription(
      '{"table":"todos","relation_ir":{"Union":{"inputs":[{"label":"source","input":{"Filter":{"input":{"TableScan":{"table":"todos"}},"predicate":{"Cmp":{"left":{"column":"priority"},"op":"Eq","right":{"Literal":1.0}}}}}}]}}}',
    );

    expect(queryBytes).toBeDefined();
    expect(Array.from(queryBytes!.slice(-10))).toEqual([
      4, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0xf8, 0x3f,
    ]);
  });

  it("carries Project relation IR in the prepared Query envelope", () => {
    const calls: string[] = [];
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: (query: Uint8Array) => {
              preparedBytes = query;
              calls.push("subscribe");
              return new ReadableStream();
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

    const handle = runtime.createSubscription(
      JSON.stringify({ table: "todos", relation_ir: unsupportedProjectRelationIr() }),
    );
    expect(handle).toBe(1);
    expect(calls).toEqual(["subscribe"]);
    expectPreparedRelationEnvelope(preparedBytes!, "todos");
  });

  it("subscribes to supported root relation IR through one Query envelope", () => {
    const calls: string[] = [];
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: (query: Uint8Array) => {
              preparedBytes = query;
              calls.push("subscribe");
              return new ReadableStream();
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            { name: "priority", column_type: { type: "Integer" }, nullable: false },
          ],
        },
      },
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        select: ["title"],
        relation_ir: {
          Limit: {
            input: {
              Offset: {
                input: {
                  OrderBy: {
                    input: {
                      Filter: {
                        input: { TableScan: { table: "todos" } },
                        predicate: {
                          Cmp: {
                            left: { column: "title" },
                            op: "Eq",
                            right: { Literal: { type: "Text", value: "native" } },
                          },
                        },
                      },
                    },
                    terms: [{ column: { column: "priority" }, direction: "Desc" }],
                  },
                },
                offset: 2,
              },
            },
            limit: 3,
          },
        },
      }),
    );

    expect(handle).toBe(1);
    expect(calls).toEqual(["subscribe"]);
    expectPreparedRelationEnvelope(preparedBytes!, "todos", ["title"]);
  });

  it("encodes public typed-builder root orderBy into native query bytes", () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: (query: Uint8Array) => {
              preparedBytes = query;
              return new ReadableStream();
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

    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [["createdAt", "desc"]],
        limit: 10,
      }),
    );

    expect(handle).toBe(1);
    expect(readPreparedQueryShape(preparedBytes!)).toEqual({
      table: "todos",
      predicates: [],
      orderBy: [{ column: "createdAt", directionTag: 1 }],
      limit: 10,
      offset: 0,
    });
  });

  it("encodes integer query literals with the Groove I32 tag", () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: (query: Uint8Array) => {
              preparedBytes = query;
              return new ReadableStream();
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            { name: "priority", column_type: { type: "Integer" }, nullable: false },
          ],
        },
      },
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "todos" } },
            predicate: {
              Cmp: {
                left: { column: "priority" },
                op: "Lt",
                right: { Literal: { type: "Integer", value: -1 } },
              },
            },
          },
        },
      }),
    );

    expectPreparedRelationEnvelope(preparedBytes!, "todos");
  });

  it("preserves signed i32 query literal boundaries and rejects overflow", () => {
    const query = queryWithPredicates("metrics", [
      { column: "count", op: "Gte", value: { type: "Integer", value: -0x80000000 } },
      { column: "count", op: "Eq", value: { type: "Integer", value: 0 } },
      { column: "count", op: "Lte", value: { type: "Integer", value: 0x7fffffff } },
    ]);

    expect(readPreparedComparisonLiterals(query)).toEqual([
      { predicateTag: 7, column: "count", literal: { tag: 15, value: -0x80000000 } },
      { predicateTag: 3, column: "count", literal: { tag: 15, value: 0 } },
      { predicateTag: 9, column: "count", literal: { tag: 15, value: 0x7fffffff } },
    ]);
    expect(() =>
      queryWithPredicates("metrics", [
        { column: "count", op: "Eq", value: { type: "Integer", value: -0x80000001 } },
      ]),
    ).toThrow("Integer value must be a signed 32-bit integer");
    expect(() =>
      queryWithPredicates("metrics", [
        { column: "count", op: "Eq", value: { type: "Integer", value: 0x80000000 } },
      ]),
    ).toThrow("Integer value must be a signed 32-bit integer");
  });

  it("encodes BIGINT query literals as signed i64 values", () => {
    const query = queryWithPredicates("metrics", [
      { column: "largeCount", op: "Gt", value: { type: "BigInt", value: 9007199254740993n } },
      { column: "largeCount", op: "Lt", value: { type: "BigInt", value: -5n } },
    ]);

    expect(readPreparedComparisonLiterals(query)).toEqual([
      { predicateTag: 6, column: "largeCount", literal: { tag: 14, value: 9007199254740993n } },
      { predicateTag: 8, column: "largeCount", literal: { tag: 14, value: -5n } },
    ]);
    for (const value of [-(1n << 63n) - 1n, 1n << 63n]) {
      expect(() =>
        queryWithPredicates("metrics", [
          { column: "largeCount", op: "Eq", value: { type: "BigInt", value } },
        ]),
      ).toThrow("BigInt value must be a signed 64-bit integer");
    }
  });

  it("preserves signed policy literals for Rust lowering", () => {
    const encoded = encodeSchema({
      metrics: {
        columns: [{ name: "score", column_type: { type: "Integer" }, nullable: false }],
        policies: {
          select: {
            using: {
              type: "And",
              exprs: [
                {
                  type: "Cmp",
                  column: "score",
                  op: "Ge",
                  value: { type: "Literal", value: { type: "Integer", value: -7 } },
                },
                {
                  type: "Cmp",
                  column: "score",
                  op: "Le",
                  value: { type: "Literal", value: { type: "Integer", value: 8 } },
                },
              ],
            },
          },
        },
      },
    });
    expect(decodeSchemaSource(encoded).tables.metrics?.policies?.select?.using).toEqual({
      type: "And",
      exprs: [
        {
          type: "Cmp",
          column: "score",
          op: "Ge",
          value: { type: "Literal", value: { type: "Integer", value: -7 } },
        },
        {
          type: "Cmp",
          column: "score",
          op: "Le",
          value: { type: "Literal", value: { type: "Integer", value: 8 } },
        },
      ],
    });
  });

  it("materializes array subquery reset deltas for subscriptions", async () => {
    const calls: string[] = [];
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    const relationSchema = {
      users: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    } satisfies WasmSchema;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              });
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      relationSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "users",
        array_subqueries: [
          {
            column_name: "todosViaOwner",
            table: "todos",
            inner_column: "owner_id",
            outer_column: "id",
          },
        ],
      }),
    );
    expect(handle).toBe(1);

    const deltas: unknown[] = [];
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });
    controller!.enqueue({
      type: "delta",
      reset: true,
      delta: encodeTerminalSubscriptionDelta(relationSchema),
    });
    await Promise.resolve();

    expect(calls).toEqual(["subscribe"]);
    const relationOutputColumns: ColumnDescriptor[] = [
      relationSchema.users.columns[0]!,
      {
        name: "todosViaOwner",
        column_type: {
          type: "Array",
          element: { type: "Row", columns: relationSchema.todos.columns },
        },
        nullable: false,
      },
    ];
    expect(decodeTestDeltas(deltas, relationOutputColumns)).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000001",
            values: [
              { type: "Text", value: "Ada" },
              {
                type: "Array",
                value: [
                  {
                    type: "Row",
                    value: {
                      id: "00000000-0000-0000-0000-000000000002",
                      values: [{ type: "Text", value: "Ship relation reads" }],
                    },
                  },
                ],
              },
            ],
          },
        },
      ],
    ]);
  });

  it("materializes array subquery relation snapshots for reads", async () => {
    const calls: string[] = [];
    const relationSchema = {
      users: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    } satisfies WasmSchema;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return encodeTerminalRelationSnapshot(relationSchema);
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      relationSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    const rows = (await runtime.query(
      JSON.stringify({
        table: "users",
        array_subqueries: [
          {
            column_name: "todosViaOwner",
            table: "todos",
            inner_column: "owner_id",
            outer_column: "users.id",
          },
        ],
      }),
    )) as Array<{
      table: string;
      id: string;
      values: unknown[];
      valuesByColumn?: Map<string, unknown>;
    }>;

    expect(calls).toEqual(["all"]);
    expect(rows).toHaveLength(1);
    expect(rows[0]?.table).toBe("users");
    expect(rows[0]?.valuesByColumn?.get("todosViaOwner")).toEqual({
      type: "Array",
      value: [
        {
          type: "Row",
          value: {
            id: "00000000-0000-0000-0000-000000000002",
            values: [{ type: "Text", value: "Ship relation reads" }],
          },
        },
      ],
    });

    setNamedRowValuesEnumerable(rows, true);
    const clonedRows = structuredClone(rows);
    setNamedRowValuesEnumerable(rows, false);
    setNamedRowValuesEnumerable(clonedRows, false);
    const clonedRelation = clonedRows[0]?.valuesByColumn?.get("todosViaOwner") as
      | {
          type: "Array";
          value: Array<{
            type: "Row";
            value: { valuesByColumn?: Map<string, unknown> };
          }>;
        }
      | undefined;
    expect(clonedRelation?.value[0]?.value.valuesByColumn?.get("title")).toEqual({
      type: "Text",
      value: "Ship relation reads",
    });

    const unrelated = { valuesByColumn: "application data" };
    setNamedRowValuesEnumerable(unrelated, false);
    expect(Object.getOwnPropertyDescriptor(unrelated, "valuesByColumn")?.enumerable).toBe(true);
  });

  it("decodes native subscription chunks", async () => {
    const calls: string[] = [];
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              });
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
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue(
      subscriptionReset([
        {
          table: "todos",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
          title: "native",
        },
      ]),
    );
    await Promise.resolve();

    expect(calls).toEqual(["subscribe"]);
    expect(decodeTestDeltas(deltas.slice(0, 2))).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000001",
            values: [{ type: "Text", value: "native" }],
          },
        },
      ],
    ]);
  });

  it("routes Gather subscriptions through the prepared-query subscription", () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream();
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

    expect(
      runtime.createSubscription(
        JSON.stringify({
          table: "todos",
          relation_ir: {
            Gather: {
              seed: { TableScan: { table: "todos" } },
              step: {
                Project: {
                  input: {
                    Join: {
                      left: { TableScan: { table: "todos" } },
                      right: { TableScan: { table: "todos" } },
                      on: [{ left: { column: "parent_id" }, right: { column: "id" } }],
                      join_kind: "Inner",
                    },
                  },
                  columns: [],
                },
              },
              frontier_key: { RowId: "Current" },
              bound: { MaxDepth: 3 },
              dedupe_key: [],
            },
          },
        }),
      ),
    ).toBe(1);
    expect(calls).toEqual(["subscribe"]);
  });

  it("passes supported read tiers and propagation through native read options", async () => {
    const readOptions: unknown[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return new Uint8Array([0]);
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

    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        null,
        "edge",
        JSON.stringify({ propagation: "local-only" }),
      ),
    ).resolves.toEqual([]);

    expect(readOptions).toEqual([{ tier: "edge", propagation: "local_only" }]);
  });

  it("selects one backend authority context for plain, relation, subscription, and transaction reads", async () => {
    const calls: string[] = [];
    let reads = 0;
    let subscriptions = 0;
    const nativeDb = fakeDb({
      all: (_query: Uint8Array, _opts: unknown, openTransactionId: string, author: Uint8Array) => {
        if (author) throw new Error("backend authority must be implicit in its native open");
        const sequence = reads++;
        if (sequence === 2 || sequence === 4) {
          calls.push(openTransactionId ? "transaction-snapshot" : "snapshot");
          return encodeRelationSnapshot([]);
        }
        calls.push(sequence === 1 ? "relation" : openTransactionId ? "transaction" : "plain");
        return encodeRows([]);
      },
      subscribe: (_query: Uint8Array, _opts: unknown, author: Uint8Array) => {
        if (author) throw new Error("backend authority must be implicit in its native open");
        calls.push(subscriptions++ === 0 ? "subscription" : "relation-subscription");
        return new ReadableStream();
      },
      tick: () => undefined,
    });
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => {
          throw new Error("ordinary open must not be selected for a backend runtime");
        },
        openMemoryAsBackend: () => nativeDb,
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
      { backendMode: true, readAuthorizationHost: "trusted-serving" },
    );

    await runtime.query(JSON.stringify({ table: "todos" }));
    await runtime.query(
      JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
    );
    await runtime.query(
      JSON.stringify({
        table: "todos",
        array_subqueries: [
          {
            column_name: "children",
            table: "todos",
            inner_column: "id",
            outer_column: "todos.id",
          },
        ],
      }),
    );
    runtime.createSubscription(JSON.stringify({ table: "todos" }));
    runtime.createSubscription(
      JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
    );
    runtime.beginTransaction("mergeable", "backend-read-tx" as never);
    await runtime.query(
      JSON.stringify({ table: "todos" }),
      null,
      null,
      JSON.stringify({ transaction_id: "backend-read-tx" }),
    );

    await runtime.query(
      JSON.stringify({
        table: "todos",
        array_subqueries: [
          {
            column_name: "children",
            table: "todos",
            inner_column: "id",
            outer_column: "todos.id",
          },
        ],
      }),
      null,
      null,
      JSON.stringify({ transaction_id: "backend-read-tx" }),
    );

    expect(calls).toEqual([
      "plain",
      "relation",
      "snapshot",
      "subscription",
      "relation-subscription",
      "transaction",
      "transaction-snapshot",
    ]);
  });

  it("keeps backend authority when registering a schema view", async () => {
    const all = vi.fn(() => encodeRows([]));
    let nativeDb: ReturnType<typeof fakeDb>;
    nativeDb = fakeDb({
      all,
      registerSchema: () => nativeDb,
      tick: () => undefined,
    });
    const runtime = new NativeRuntimeAdapter(
      {
        openMemoryAsBackend: () => nativeDb,
        openMemory: () => {
          throw new Error("ordinary open must not be selected for a backend runtime");
        },
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
      { backendMode: true, readAuthorizationHost: "trusted-serving" },
    );

    await runtime.registerSchemaView(testSchema).query(JSON.stringify({ table: "todos" }));

    expect(all).toHaveBeenCalledOnce();
  });

  it("hydrates broad Edge members with one native read", async () => {
    const readOptions: unknown[] = [];
    const row = {
      table: "todos",
      rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
      title: "authority-selected todo",
    };
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return encodeRows([row]);
            },
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
    runtime.connectUpstreamPeer();

    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "edge")).resolves.toEqual([
      {
        id: "00000000-0000-0000-0000-000000000001",
        table: "todos",
        values: [{ type: "Text", value: "authority-selected todo" }],
      },
    ]);

    expect(readOptions).toEqual([{ tier: "edge" }]);
  });

  it("forwards a standalone exact Edge read through all", async () => {
    const readOptions: unknown[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return encodeRows([]);
            },
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
    runtime.connectUpstreamPeer();

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          conditions: [{ column: "id", op: "eq", value: "00000000-0000-0000-0000-000000000001" }],
        }),
        null,
        "edge",
      ),
    ).resolves.toEqual([]);

    expect(readOptions).toEqual([{ tier: "edge" }]);
  });

  it("ignores the removed propagate read option", async () => {
    const readOptions: unknown[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return new Uint8Array([0]);
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

    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        null,
        "edge",
        JSON.stringify({ propagate: false }),
      ),
    ).resolves.toEqual([]);

    expect(readOptions).toEqual([{ tier: "edge" }]);
  });

  it("keeps concurrent client reads on the raw client path", async () => {
    globalThis.WebSocket = FakeWebSocket as unknown as typeof WebSocket;
    const attachedSubjects: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, _opts: unknown, _tx: unknown, author: Uint8Array) => {
              if (author) throw new Error("client coverage must not use an authority identity");
              attachedSubjects.push("client");
              return encodeRows([]);
            },
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
    await runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await waitForFakeWebSocketNegotiation();

    await Promise.all([
      runtime.query(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({
          issuer: "https://issuer.example",
          user_id: "00000000-0000-0000-0000-0000000000a1",
        }),
        "edge",
      ),
      runtime.query(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({
          issuer: "https://issuer.example",
          user_id: "00000000-0000-0000-0000-0000000000b2",
        }),
        "edge",
      ),
    ]);

    expect(attachedSubjects).toEqual(["client", "client"]);
  });

  it("passes supported read tiers and branch views through", async () => {
    const runtime = emptyNativeRuntime();

    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "edge")).resolves.toEqual(
      [],
    );
    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), null, "planetary"),
    ).rejects.toThrow("unsupported read tier");
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        null,
        "local",
        JSON.stringify({ propagation: "local" }),
      ),
    ).rejects.toThrow("does not support read propagation");
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        null,
        "local",
        JSON.stringify({ read_view: { source: "branch" } }),
      ),
    ).resolves.toEqual([]);
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos" }),
        null,
        "local",
        JSON.stringify({ readView: { source: "branch" } }),
      ),
    ).resolves.toEqual([]);
  });

  it("passes include_deleted query intent through native read options", async () => {
    const readOptions: unknown[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return new Uint8Array([0]);
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

    await expect(
      runtime.query(JSON.stringify({ table: "todos", include_deleted: true }), null, "edge"),
    ).resolves.toEqual([]);

    expect(readOptions).toEqual([{ tier: "edge", include_deleted: true }]);
  });

  it("polls a pending binding-owned read until it completes", async () => {
    let polls = 0;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => ({
              poll: () => (++polls < 2 ? null : encodeRows([])),
            }),
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
    runtime.connectUpstreamPeer();

    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "edge")).resolves.toEqual(
      [],
    );
    expect(polls).toBe(2);
  });

  it("processes already-admitted worker activity before starting a fresh native read", async () => {
    let processed = false;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              expect(processed).toBe(true);
              return encodeRows([]);
            },
            connectUpstream: () => new FakeTransport([]),
            setNonDurableClient: () => undefined,
            tick: () => {
              processed = true;
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
    runtime.setNonDurableClient();
    runtime.connectUpstreamPeer();
    runtime.notifyPeerTransportActivity();

    await expect(runtime.query(JSON.stringify({ table: "todos" }), null, "local")).resolves.toEqual(
      [],
    );
  });

  it("rejects a pending binding-owned read when its server transport errors", async () => {
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
            all: () => ({ poll: () => null }),
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
    await runtime.connect("ws://127.0.0.1:4200/apps/app-a/ws", "{}");
    await waitForFakeWebSocketNegotiation();

    const query = runtime.query(JSON.stringify({ table: "todos" }), null, "edge");
    await Promise.resolve();
    sockets[0]!.emitMessage(encodeWebSocketFrameBatch([encodeWireError(4, 3, "server busy")]));

    await expect(query).rejects.toThrow("server busy");
  });

  it("passes supported subscription read tiers through", () => {
    const runtime = emptyNativeRuntime();

    expect(() =>
      runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "edge"),
    ).not.toThrow();
    expect(() =>
      runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "planetary"),
    ).toThrow("unsupported read tier");
  });

  it("rejects include_deleted subscription query intent", () => {
    const runtime = emptyNativeRuntime();

    expect(() =>
      runtime.createSubscription(JSON.stringify({ table: "todos", include_deleted: true })),
    ).toThrow("include_deleted subscriptions");
  });

  it("rejects permission introspection selected columns before preparing flat queries", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return new Uint8Array([0]);
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

    await expect(
      runtime.query(JSON.stringify({ table: "todos", select_columns: ["title", "$canRead"] })),
    ).rejects.toThrow("permission-introspection query");
    await expect(
      runtime.query(
        JSON.stringify({ table: "todos", select_columns: ["title", "todos.$canRead"] }),
      ),
    ).rejects.toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("rejects permission introspection predicates before preparing flat queries", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return new Uint8Array([0]);
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

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          conditions: [{ column: "$canRead", op: "eq", value: true }],
        }),
      ),
    ).rejects.toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("rejects canonical permission predicates hidden inside Not(In(...))", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return new Uint8Array([0]);
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

    const plantedSensitivePredicate = {
      Not: {
        In: {
          left: { column: "$canRead" },
          values: [{ Literal: { type: "Boolean", value: true } }],
        },
      },
    };
    for (const query of [
      { table: "todos", conditions: [plantedSensitivePredicate] },
      {
        table: "todos",
        array_subqueries: [
          {
            column_name: "children",
            table: "todos",
            inner_column: "id",
            outer_column: "todos.id",
            filters: [plantedSensitivePredicate],
          },
        ],
      },
    ]) {
      await expect(runtime.query(JSON.stringify(query))).rejects.toThrow(
        "permission-introspection query",
      );
    }
    expect(calls).toEqual([]);
  });

  it("lets the core reject relation reads inside a transaction", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              throw new Error("relation reads inside a transaction are not supported");
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
    const transactionId = "relation-read-batch" as never;
    runtime.beginTransaction("mergeable", transactionId);
    const opts = JSON.stringify({ transaction_id: transactionId });

    await expect(
      runtime.query(
        JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
        undefined,
        undefined,
        opts,
      ),
    ).rejects.toThrow("relation reads inside a transaction are not supported");
    expect(calls).toEqual(["all"]);
  });

  it("rejects permission introspection in array subqueries before native snapshot prep", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return new Uint8Array([0]);
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

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          array_subqueries: [
            {
              column_name: "children",
              table: "todos",
              inner_column: "id",
              outer_column: "todos.id",
              select_columns: ["title", "$canRead"],
            },
          ],
        }),
      ),
    ).rejects.toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("rejects permission introspection before subscribing to flat queries", () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: () => {
              calls.push("subscribe");
              return new ReadableStream();
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

    expect(() =>
      runtime.createSubscription(
        JSON.stringify({
          table: "todos",
          conditions: [{ column: "$canRead", op: "eq", value: true }],
          select_columns: ["title", "$canRead"],
        }),
      ),
    ).toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("rejects permission introspection relation projections before the native query API", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return new Uint8Array([0]);
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

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          relation_ir: {
            Project: {
              input: { TableScan: { table: "todos" } },
              columns: [
                {
                  alias: "$canRead",
                  expr: { Column: { scope: "todos", column: "$canRead" } },
                },
              ],
            },
          },
        }),
      ),
    ).rejects.toThrow("permission-introspection query");
    expect(calls).toEqual([]);
  });

  it("keeps provenance selected columns on the native flat query path", async () => {
    const calls: string[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => {
              calls.push("all");
              return encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "native provenance",
                  createdAt: 42,
                },
              ]);
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

    await expect(
      runtime.query(JSON.stringify({ table: "todos", select_columns: ["title", "$createdAt"] })),
    ).resolves.toHaveLength(1);
    expect(calls).toEqual(["all"]);
  });

  it("passes local-only subscription propagation through native read options", () => {
    const readOptions: unknown[] = [];
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: (_query: unknown, opts: unknown) => {
              readOptions.push(opts);
              return new ReadableStream();
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

    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        null,
        "edge",
        JSON.stringify({ propagation: "local-only" }),
      ),
    ).not.toThrow();

    expect(readOptions).toEqual([{ tier: "edge", propagation: "local_only" }]);
  });

  it("passes non-default read_view subscription options through", () => {
    const runtime = emptyNativeRuntime();

    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        null,
        "edge",
        JSON.stringify({ read_view: { source: "branch" } }),
      ),
    ).not.toThrow();
    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        null,
        "edge",
        JSON.stringify({ readView: { source: "branch" } }),
      ),
    ).not.toThrow();
  });

  it("accepts well-formed subscription sessions and rejects malformed sessions", () => {
    const runtime = emptyNativeRuntime();

    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({
          issuer: "https://issuer.example",
          user_id: "00000000-0000-0000-0000-000000000000",
        }),
      ),
    ).not.toThrow();
    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({ issuer: "https://issuer.example", user_id: null }),
      ),
    ).toThrow("session is missing user_id");
    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({ issuer: "https://issuer.example", user_id: "\ud800" }),
      ),
    ).toThrow("session is missing user_id");
    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({ issuer: "\udc00", user_id: "00000000-0000-0000-0000-000000000000" }),
      ),
    ).toThrow("session is missing issuer");
    expect(() =>
      runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        JSON.stringify({ user_id: "00000000-0000-0000-0000-000000000000" }),
      ),
    ).toThrow("session is missing issuer");
    expect(() =>
      runtime.insert(
        "todos",
        { title: { type: "Text", value: "missing issuer" } },
        JSON.stringify({ user_id: "00000000-0000-0000-0000-000000000000" }),
      ),
    ).toThrow("session is missing issuer");
  });

  it("applies subscription deltas to the full keyed snapshot", async () => {
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: () =>
              new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              }),
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
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue(
      subscriptionReset([
        {
          table: "todos",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
          title: "first",
        },
        {
          table: "todos",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
          title: "second",
        },
      ]),
    );
    await Promise.resolve();

    controller!.enqueue({
      type: "delta",
      delta: encodeSubscriptionDelta({
        added: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000003"),
            title: "third",
          },
        ],
        updated: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
            title: "second updated",
          },
        ],
        removed: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
          },
        ],
        addedIndices: [1],
        updatedPreviousIndices: [1],
        updatedIndices: [0],
        removedIndices: [0],
      }),
    });
    await Promise.resolve();

    controller!.enqueue(subscriptionReset([]));
    await Promise.resolve();

    expect(decodeTestDeltas(deltas.slice(0, 2))).toEqual([
      [
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000001",
            values: [{ type: "Text", value: "first" }],
          },
        },
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000002",
          index: 1,
          row: {
            id: "00000000-0000-0000-0000-000000000002",
            values: [{ type: "Text", value: "second" }],
          },
        },
      ],
      [
        {
          kind: 2,
          id: "00000000-0000-0000-0000-000000000002",
          index: 0,
          row: {
            id: "00000000-0000-0000-0000-000000000002",
            values: [{ type: "Text", value: "second updated" }],
          },
        },
        {
          kind: 0,
          id: "00000000-0000-0000-0000-000000000003",
          index: 1,
          row: {
            id: "00000000-0000-0000-0000-000000000003",
            values: [{ type: "Text", value: "third" }],
          },
        },
        {
          kind: 1,
          id: "00000000-0000-0000-0000-000000000001",
          index: 0,
        },
      ],
    ]);
    expect(deltas.at(-1)).toMatchObject({ reset: true, added: [], updated: [], removed: [] });
  });

  it("encodes public id equality relation filters into prepared native queries", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "native returned requested",
                },
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
                  title: "native returned extra",
                },
              ]);
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

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          relation_ir: {
            Filter: {
              input: { TableScan: { table: "todos" } },
              predicate: {
                Cmp: {
                  left: { column: "id" },
                  op: "Eq",
                  right: {
                    Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
                  },
                },
              },
            },
          },
        }),
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "native returned requested" }],
      },
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000002",
        values: [{ type: "Text", value: "native returned extra" }],
      },
    ]);
    expectPreparedRelationEnvelope(preparedBytes!, "todos");
  });

  it("preserves raw provenance timestamps from native rows without Date.now fallbacks", async () => {
    const createdAtMs = 42;
    const updatedAtMs = 43;
    const rowId = "00000000-0000-0000-0000-000000000001";
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () =>
              encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes(rowId),
                  title: "native provenance",
                  createdAt: createdAtMs,
                  updatedAt: updatedAtMs,
                },
              ]),
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

    const [row] = (await runtime.query(
      JSON.stringify({
        table: "todos",
        select_columns: ["title", "$createdAt", "$updatedAt"],
        relation_ir: { TableScan: { table: "todos" } },
      }),
    )) as Array<{ valuesByColumn?: Map<string, { type: string; value: number }> }>;

    expect(row?.valuesByColumn?.get("$createdAt")).toEqual({
      type: "Timestamp",
      value: createdAtMs,
    });
    expect(row?.valuesByColumn?.get("$updatedAt")).toEqual({
      type: "Timestamp",
      value: updatedAtMs,
    });
  });

  it("decodes a present nullable empty fixed-width array using its storage element type", async () => {
    const schema = {
      todos: {
        columns: [
          {
            name: "assigneesIds",
            column_type: { type: "Array", element: { type: "Uuid" } },
            nullable: true,
          },
        ],
      },
    } satisfies WasmSchema;
    const descriptor = [
      {
        name: "assigneesIds",
        valueType: { tag: 15, inner: { tag: 14, inner: { tag: 11 } } },
      },
    ];
    const writer = new PostcardWriter();
    writer.vec((batch) => {
      batch.string("todos");
      writeNativeRowDescriptor(batch, physicalNativeDescriptor(descriptor));
      batch.vec((row) => {
        row.bytes(uuidBytes("00000000-0000-0000-0000-000000000001"));
        row.bool(false);
        row.bytes(createRecord(descriptor, [presentBytes(new Uint8Array())]));
      }, 1);
    }, 1);
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: () => writer.finish(),
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      schema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Array", value: [] }],
      },
    ]);
  });

  it("encodes public id in conditions into prepared native queries", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return new Uint8Array([0]);
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

    await runtime.query(
      JSON.stringify({
        table: "todos",
        conditions: [
          {
            column: "id",
            op: "in",
            value: ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"],
          },
        ],
      }),
    );

    expect(readPreparedUuidIn(preparedBytes!)).toEqual({
      table: "todos",
      column: "id",
      values: ["00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"],
    });
  });

  it("encodes uuid-looking condition values as text for text columns", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return new Uint8Array([0]);
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

    await runtime.query(
      JSON.stringify({
        table: "todos",
        conditions: [
          {
            column: "title",
            op: "eq",
            value: "00000000-0000-0000-0000-000000000001",
          },
        ],
      }),
    );

    expect(readPreparedComparison(preparedBytes!)).toEqual({
      table: "todos",
      predicateTag: 3,
      column: "title",
      literalTag: 6,
      value: "00000000-0000-0000-0000-000000000001",
      limit: undefined,
    });
  });

  it("preserves relation IR in literals for numeric and timestamp columns", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return new Uint8Array([0]);
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        metrics: {
          columns: [
            { name: "count", column_type: { type: "Integer" }, nullable: false },
            { name: "ratio", column_type: { type: "Double" }, nullable: false },
            { name: "createdAt", column_type: { type: "Timestamp" }, nullable: false },
          ],
        },
      } satisfies WasmSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "metrics",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "metrics" } },
            predicate: {
              And: [
                {
                  In: {
                    left: { column: "count" },
                    values: [
                      { Literal: { type: "Integer", value: 5 } },
                      { Literal: { type: "Integer", value: 10 } },
                    ],
                  },
                },
                {
                  In: {
                    left: { column: "ratio" },
                    values: [
                      { Literal: { type: "Double", value: 1.5 } },
                      { Literal: { type: "Double", value: 2.5 } },
                    ],
                  },
                },
                {
                  In: {
                    left: { column: "createdAt" },
                    values: [
                      { Literal: { type: "Timestamp", value: 1767225600000 } },
                      { Literal: { type: "Timestamp", value: 1767312000000 } },
                    ],
                  },
                },
              ],
            },
          },
        },
      }),
    );

    expectPreparedRelationEnvelope(preparedBytes!, "metrics");
  });

  it("preserves relation IR range literal types for double and timestamp columns", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return new Uint8Array([0]);
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        metrics: {
          columns: [
            { name: "ratio", column_type: { type: "Double" }, nullable: false },
            { name: "createdAt", column_type: { type: "Timestamp" }, nullable: false },
          ],
        },
      } satisfies WasmSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "metrics",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "metrics" } },
            predicate: {
              And: [
                {
                  Cmp: {
                    left: { column: "ratio" },
                    op: "Gt",
                    right: { Literal: { type: "Double", value: 1.5 } },
                  },
                },
                {
                  Cmp: {
                    left: { column: "ratio" },
                    op: "Lt",
                    right: { Literal: { type: "Double", value: 4.5 } },
                  },
                },
                {
                  Cmp: {
                    left: { column: "createdAt" },
                    op: "Gt",
                    right: { Literal: { type: "Timestamp", value: 1770076800000 } },
                  },
                },
                {
                  Cmp: {
                    left: { column: "createdAt" },
                    op: "Lt",
                    right: { Literal: { type: "Timestamp", value: 1770336000000 } },
                  },
                },
              ],
            },
          },
        },
      }),
    );

    expectPreparedRelationEnvelope(preparedBytes!, "metrics");
  });

  it("does not filter native subscription reset deltas by public id in JS", async () => {
    let controller: ReadableStreamDefaultController<unknown> | undefined;
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: (query: Uint8Array) => {
              preparedBytes = query;
              return new ReadableStream({
                start(streamController) {
                  controller = streamController;
                },
              });
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
    const deltas: unknown[] = [];
    const handle = runtime.createSubscription(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Filter: {
            input: { TableScan: { table: "todos" } },
            predicate: {
              Cmp: {
                left: { column: "id" },
                op: "Eq",
                right: {
                  Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
                },
              },
            },
          },
        },
      }),
    );
    runtime.executeSubscription(handle, (delta: unknown) => {
      deltas.push(delta);
    });

    controller!.enqueue(
      subscriptionReset([
        {
          table: "todos",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
          title: "requested",
        },
        {
          table: "todos",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
          title: "extra from native",
        },
      ]),
    );
    await Promise.resolve();

    expect(decodeTestDeltas(deltas)[0]).toHaveLength(2);
    expectPreparedRelationEnvelope(preparedBytes!, "todos");
  });

  it("delivers packed reset rows with the same public shape as legacy decode when native batches include internal fields", () => {
    const chunk = {
      type: "delta",
      reset: true,
      settled: true,
      delta: encodeSubscriptionDelta({
        added: [
          {
            table: "todos",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000123"),
            title: "packed reset public row",
            txTime: 123,
          },
        ],
        updated: [],
        removed: [],
      }),
    };
    const runtime = runtimeWithNativeSubscriptionChunk(chunk);
    const deltas: RuntimeSubscriptionDelta[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, null, null);

    runtime.executeSubscription(handle, (delta: RuntimeSubscriptionDelta) => {
      deltas.push(delta);
    });

    expect(deltas).toHaveLength(1);
    const decoded = runtimeDeltaChanges(deltas[0]!);
    expect(decoded).toEqual([
      {
        kind: 0,
        id: "00000000-0000-0000-0000-000000000123",
        index: 0,
        row: {
          id: "00000000-0000-0000-0000-000000000123",
          values: [{ type: "Text", value: "packed reset public row" }],
        },
      },
    ]);
    expect(decoded[0]?.kind).toBe(0);
    if (decoded[0]?.kind !== 0) throw new Error("expected added row");
    expect(Object.keys(decoded[0].row)).toEqual(["id", "values"]);
    runtime.close();
  });

  it("materializes typed-occurrence resets instead of collapsing them in the packed path", () => {
    const rowId = uuidBytes("00000000-0000-0000-0000-000000000124");
    const key = (suffix: number) => {
      const bytes = new Uint8Array(50);
      bytes[0] = 1;
      bytes.set(rowId, 1);
      new DataView(bytes.buffer).setUint32(17, 1);
      bytes.fill(2, 21, 37);
      new DataView(bytes.buffer).setUint32(37, 1);
      new DataView(bytes.buffer).setUint32(41, 0);
      new DataView(bytes.buffer).setUint32(45, 1);
      bytes[49] = suffix;
      return bytes;
    };
    const runtime = runtimeWithNativeSubscriptionChunk({
      type: "delta",
      reset: true,
      settled: true,
      delta: encodeSubscriptionDelta({
        added: [
          { table: "todos", rowId, title: "direct" },
          { table: "todos", rowId, title: "inherited" },
        ],
        updated: [],
        removed: [],
        addedOccurrenceKeys: [key(1), key(2)],
      }),
    });
    const deltas: RuntimeSubscriptionDelta[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, null, null);
    runtime.executeSubscription(handle, (delta: RuntimeSubscriptionDelta) => deltas.push(delta));
    const decoded = runtimeDeltaChanges(deltas[0]!);
    expect(decoded).toHaveLength(2);
    expect(decoded[0]!.id).not.toBe(decoded[1]!.id);
    expect(decoded.map((change) => change.id)).toEqual([
      expect.stringContaining("result:01"),
      expect.stringContaining("result:01"),
    ]);
    runtime.close();
  });

  it("reconciles native relation subscription lifecycles without leaking projection records", () => {
    const first = uuidBytes("00000000-0000-0000-0000-000000000401");
    const second = uuidBytes("00000000-0000-0000-0000-000000000402");
    const chunks = [
      relationSubscriptionChunk({
        reset: true,
        rootAdded: [{ table: "todos", rowId: first, title: "first" }],
      }),
      relationSubscriptionChunk({
        rootUpdated: [{ table: "todos", rowId: first, title: "first updated" }],
      }),
      relationSubscriptionChunk({
        rootRemoved: [{ table: "todos", rowId: first }],
      }),
      relationSubscriptionChunk({
        reset: true,
        rootAdded: [{ table: "todos", rowId: second, title: "second" }],
      }),
    ];
    const runtime = runtimeWithNativeRelationSubscriptionChunks(chunks);
    const deltas: RuntimeSubscriptionDelta[] = [];
    const handle = runtime.createSubscription(
      JSON.stringify({ table: "todos", relation_ir: supportedGatherRelationIr("todos") }),
      null,
      null,
      null,
    );

    runtime.executeSubscription(handle, (delta: RuntimeSubscriptionDelta) => deltas.push(delta));

    expect(deltas).toHaveLength(4);
    expect(decodeTestDeltas([deltas[0]!])[0]).toMatchObject([
      { kind: 0, id: formatUuid(first), index: 0, row: { values: [{ value: "first" }] } },
    ]);
    expect(decodeTestDeltas([deltas[1]!])[0]).toMatchObject([
      {
        kind: 2,
        id: formatUuid(first),
        index: 0,
        row: { values: [{ value: "first updated" }] },
      },
    ]);
    expect(decodeTestDeltas([deltas[2]!])[0]).toEqual([
      { kind: 1, id: formatUuid(first), index: 0 },
    ]);
    expect(decodeTestDeltas([deltas[3]!])[0]).toMatchObject([
      { kind: 0, id: formatUuid(second), index: 0, row: { values: [{ value: "second" }] } },
    ]);
    expect(deltas[0]!.reset).toBe(true);
    expect(deltas[3]!.reset).toBe(true);
    runtime.close();

    const ordinary = runtimeWithNativeSubscriptionChunk(
      relationSubscriptionChunk({
        reset: true,
        rootAdded: [{ table: "todos", rowId: first, title: "ordinary" }],
      }),
    );
    const ordinaryDeltas: RuntimeSubscriptionDelta[] = [];
    const ordinaryHandle = ordinary.createSubscription(JSON.stringify({ table: "todos" }));
    ordinary.executeSubscription(ordinaryHandle, (delta: RuntimeSubscriptionDelta) =>
      ordinaryDeltas.push(delta),
    );
    expect(decodeTestDeltas(ordinaryDeltas)[0]).toMatchObject([
      { kind: 0, id: formatUuid(first), row: { values: [{ value: "ordinary" }] } },
    ]);
    ordinary.close();
  });

  it("rewraps user field option bytes when packed reset frames filter engine records", () => {
    const schema = {
      notes: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "note", column_type: { type: "Text" }, nullable: true },
        ],
      },
    } satisfies WasmSchema;
    const chunk = {
      type: "delta",
      reset: true,
      settled: true,
      delta: encodeUserWrappedSubscriptionDelta({
        table: "notes",
        rowId: uuidBytes("00000000-0000-0000-0000-000000000321"),
        title: "plain public title",
        note: "nullable public note",
      }),
    };
    const runtime = runtimeWithNativeSubscriptionChunk(chunk, schema);
    const deltas: RuntimeSubscriptionDelta[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "notes" }), null, null, null);

    runtime.executeSubscription(handle, (delta: RuntimeSubscriptionDelta) => {
      deltas.push(delta);
    });

    expect(deltas).toHaveLength(1);
    const decoded = runtimeDeltaChanges(deltas[0]!);
    expect(decoded).toEqual([
      {
        kind: 0,
        id: "00000000-0000-0000-0000-000000000321",
        index: 0,
        row: {
          id: "00000000-0000-0000-0000-000000000321",
          values: [
            { type: "Text", value: "plain public title" },
            { type: "Text", value: "nullable public note" },
          ],
        },
      },
    ]);
    runtime.close();
  });

  it("normalizes CurrentRow carriers before publishing relation reset frames", () => {
    const schema = {
      notes: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "note", column_type: { type: "Text" }, nullable: true },
        ],
      },
    } satisfies WasmSchema;
    const runtime = runtimeWithNativeRelationSubscriptionChunks(
      [
        {
          type: "delta",
          reset: true,
          settled: true,
          delta: encodeUserWrappedSubscriptionDelta({
            table: "notes",
            rowId: uuidBytes("00000000-0000-0000-0000-000000000322"),
            title: "hop target title",
            note: "hop target note",
          }),
        },
      ],
      schema,
    );
    const deltas: RuntimeSubscriptionDelta[] = [];
    const handle = runtime.createSubscription(
      JSON.stringify({ table: "notes", relation_ir: supportedGatherRelationIr("notes") }),
      null,
      null,
      null,
    );

    runtime.executeSubscription(handle, (delta: RuntimeSubscriptionDelta) => deltas.push(delta));

    expect(deltas).toHaveLength(1);
    expect(runtimeDeltaChanges(deltas[0]!)).toEqual([
      {
        kind: 0,
        id: "00000000-0000-0000-0000-000000000322",
        index: 0,
        row: {
          id: "00000000-0000-0000-0000-000000000322",
          values: [
            { type: "Text", value: "hop target title" },
            { type: "Text", value: "hop target note" },
          ],
        },
      },
    ]);
    runtime.close();
  });

  it("passes structured provenance authors through public subscription frames", () => {
    const schema = {
      notes: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "note", column_type: { type: "Text" }, nullable: true },
        ],
      },
    } satisfies WasmSchema;
    const publicColumns = [
      ...schema.notes.columns,
      { name: "$createdBy", column_type: authorColumnType(), nullable: false },
      { name: "$createdAt", column_type: { type: "Timestamp" }, nullable: false },
    ] as const;
    const nativeDelta = readNativeSubscriptionDelta(
      new PostcardReader(
        encodeUserWrappedSubscriptionDelta({
          table: "notes",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000321"),
          title: "public title",
          note: "public note",
        }),
      ),
    );

    const applied = decodeSubscriptionDelta(nativeDelta, schema, true, {
      rootTable: "notes",
      rootColumns: publicColumns,
    });

    const [change] = runtimeDeltaChanges(applied);
    expect(change?.kind).toBe(0);
    if (!change || change.kind !== 0) throw new Error("expected inserted row");
    expect(change.row.values).toEqual([
      { type: "Text", value: "public title" },
      { type: "Text", value: "public note" },
      {
        type: "Row",
        value: {
          values: [
            { type: "Uuid", value: "00000000-0000-4000-8000-000000000001" },
            {
              type: "Row",
              value: {
                values: [
                  { type: "Text", value: "https://issuer.example" },
                  { type: "Text", value: "user-1" },
                ],
              },
            },
          ],
        },
      },
      { type: "Timestamp", value: 123 },
    ]);
  });

  it.each([
    { name: "missing record", provenanceBytes: new Uint8Array() },
    { name: "truncated record", provenanceBytes: encodedAuthorFixture().subarray(0, 4) },
    {
      name: "malformed UTF-8 subject",
      provenanceBytes: encodedAuthorFixture(Uint8Array.of(2, 255)),
    },
    { name: "ASCII-blank component", provenanceBytes: encodedAuthorFixture(inlineScalar(" ")) },
  ])("rejects malformed public provenance author bytes: $name", ({ provenanceBytes }) => {
    const schema = {
      notes: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "note", column_type: { type: "Text" }, nullable: true },
        ],
      },
    } satisfies WasmSchema;
    const publicColumns = [
      ...schema.notes.columns,
      { name: "$createdBy", column_type: authorColumnType(), nullable: false },
      { name: "$createdAt", column_type: { type: "Timestamp" }, nullable: false },
    ] as const;
    const nativeDelta = readNativeSubscriptionDelta(
      new PostcardReader(
        encodeUserWrappedSubscriptionDelta({
          table: "notes",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000321"),
          title: "public title",
          note: "public note",
          provenanceBytes,
        }),
      ),
    );

    expect(() =>
      decodeSubscriptionDelta(nativeDelta, schema, true, {
        rootTable: "notes",
        rootColumns: publicColumns,
      }),
    ).toThrow(/record|offset|UTF-8|portable|nonempty/i);
  });

  it("keeps ordinary text decoding strict while validating provenance specially", () => {
    const schema = {
      notes: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "note", column_type: { type: "Text" }, nullable: true },
        ],
      },
    } satisfies WasmSchema;
    const publicColumns = [
      ...schema.notes.columns,
      { name: "$createdBy", column_type: authorColumnType(), nullable: false },
      { name: "$createdAt", column_type: { type: "Timestamp" }, nullable: false },
    ] as const;
    const nativeDelta = readNativeSubscriptionDelta(
      new PostcardReader(
        encodeUserWrappedSubscriptionDelta({
          table: "notes",
          rowId: uuidBytes("00000000-0000-0000-0000-000000000321"),
          title: "public title",
          titleBytes: new TextEncoder().encode("unwrapped ordinary text"),
          note: "public note",
        }),
      ),
    );

    expect(() =>
      decodeSubscriptionDelta(nativeDelta, schema, true, {
        rootTable: "notes",
        rootColumns: publicColumns,
      }),
    ).toThrow("indirect scalar crossed a logical binding boundary");
  });

  it("encodes range id comparisons into prepared native queries", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return encodeRows([
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                  title: "drop",
                },
                {
                  table: "todos",
                  rowId: uuidBytes("00000000-0000-0000-0000-000000000002"),
                  title: "keep",
                },
              ]);
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

    await expect(
      runtime.query(
        JSON.stringify({
          table: "todos",
          relation_ir: {
            Filter: {
              input: { TableScan: { table: "todos" } },
              predicate: {
                Cmp: {
                  left: { column: "id" },
                  op: "Gt",
                  right: {
                    Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
                  },
                },
              },
            },
          },
        }),
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000001",
        values: [{ type: "Text", value: "drop" }],
      },
      {
        table: "todos",
        id: "00000000-0000-0000-0000-000000000002",
        values: [{ type: "Text", value: "keep" }],
      },
    ]);
    expectPreparedRelationEnvelope(preparedBytes!, "todos");
  });

  it("pushes limits with native id predicates", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return new Uint8Array([0]);
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

    await runtime.query(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Limit: {
            input: {
              Filter: {
                input: { TableScan: { table: "todos" } },
                predicate: {
                  Cmp: {
                    left: { column: "id" },
                    op: "Eq",
                    right: {
                      Literal: { type: "Uuid", value: "00000000-0000-0000-0000-000000000001" },
                    },
                  },
                },
              },
            },
            limit: 1,
          },
        },
      }),
    );

    expectPreparedRelationEnvelope(preparedBytes!, "todos");
  });

  it("carries relation order and pagination in the Query envelope", async () => {
    let preparedBytes: Uint8Array | undefined;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            all: (query: Uint8Array) => {
              preparedBytes = query;
              return new Uint8Array([0]);
            },
            tick: () => undefined,
          }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      {
        todos: {
          columns: [
            { name: "title", column_type: { type: "Text" }, nullable: false },
            { name: "priority", column_type: { type: "Integer" }, nullable: false },
          ],
        },
      },
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    await runtime.query(
      JSON.stringify({
        table: "todos",
        relation_ir: {
          Limit: {
            input: {
              Offset: {
                input: {
                  OrderBy: {
                    input: {
                      Filter: {
                        input: { TableScan: { table: "todos" } },
                        predicate: {
                          Cmp: {
                            left: { column: "title" },
                            op: "Eq",
                            right: { Literal: { type: "Text", value: "ship it" } },
                          },
                        },
                      },
                    },
                    terms: [
                      { column: { column: "priority" }, direction: "Desc" },
                      { column: { column: "title" }, direction: "Asc" },
                    ],
                  },
                },
                offset: 5,
              },
            },
            limit: 10,
          },
        },
      }),
    );

    expectPreparedRelationEnvelope(preparedBytes!, "todos");
  });
});

describe("NativeRuntimeAdapter streaming inserts", () => {
  it("infers the physical kind and applies backpressure to async chunks", async () => {
    const pushed: Uint8Array[] = [];
    let finished = false;
    const beginStreamingMutation = vi.fn(
      (
        _table: string,
        _rowId: Uint8Array,
        _cells: Uint8Array,
        _column: string,
        _mutation?: string,
        _author?: Uint8Array,
        _updatedAtMs?: number,
        _head?: unknown,
        _base?: unknown,
      ) => ({
        push(chunk: Uint8Array) {
          pushed.push(Uint8Array.from(chunk));
        },
        finish() {
          finished = true;
          return fakeWrite();
        },
        abort: vi.fn(),
      }),
    );
    const schema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    } satisfies WasmSchema;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => fakeDb({ beginStreamingMutation }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      schema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );

    const result = await runtime.streamingMutation(
      "insert",
      "todos",
      { done: { type: "Boolean", value: false } },
      "title",
      (async function* () {
        yield "hello ";
        yield new TextEncoder().encode("world");
      })(),
      null,
      "00000000-0000-0000-0000-000000000123",
    );

    expect(beginStreamingMutation).toHaveBeenCalledOnce();
    expect(beginStreamingMutation.mock.calls[0]?.[3]).toBe("title");
    expect(beginStreamingMutation.mock.calls[0]?.[4]).toBe("insert");
    expect(pushed.map((chunk) => new TextDecoder().decode(chunk))).toEqual(["hello ", "world"]);
    expect(finished).toBe(true);
    expect(result.id).toBe("00000000-0000-0000-0000-000000000123");
  });

  it("forwards update identity, branch view, and custom timestamp to native finish", async () => {
    const beginStreamingMutation = vi.fn(() => ({
      push: () => undefined,
      finish: () => fakeWrite(),
      abort: vi.fn(),
    }));
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => fakeDb({ beginStreamingMutation }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
      { readAuthorizationHost: "trusted-serving" },
    );
    const head = { values: { workspace: [1, 2] } };
    const base = { Current: { values: { workspace: [1, 1] } } };

    await runtime.streamingMutation(
      "update",
      "todos",
      {},
      "title",
      (async function* () {
        yield "updated";
      })(),
      JSON.stringify({
        session: {
          issuer: "https://issuer.example",
          user_id: "user-1",
          claims: { role: "editor" },
        },
        updated_at: 1_234,
        branch_view: { head, base },
      }),
      "00000000-0000-0000-0000-000000000123",
    );

    const call = beginStreamingMutation.mock.calls[0] as unknown[];
    expect(call[4]).toBe("update");
    expect(call[5]).toBeInstanceOf(Uint8Array);
    expect(call[6]).toBeUndefined();
    expect(call[7]).toBe(1234);
    expect(call[8]).toEqual(head);
    expect(call[9]).toEqual(base);
  });

  it.each([
    {
      name: "valid canonical attribution",
      attribution: JSON.stringify(["https://issuer.example", "user-1"]),
      expectedAuthor: JSON.stringify(["https://issuer.example", "user-1"]),
    },
    {
      name: "noncanonical JSON whitespace",
      attribution: `[ "https://issuer.example", "user-1" ]`,
      expectedAuthor: undefined,
    },
    {
      name: "blank subject",
      attribution: JSON.stringify(["https://issuer.example", " "]),
      expectedAuthor: undefined,
    },
  ])("strictly parses write-context canonical author attribution: $name", async (testCase) => {
    const ownerAuthor = JSON.stringify(["urn:jazz:test", "owner"]);
    const beginStreamingMutation = vi.fn(
      (
        _table: string,
        _rowId: Uint8Array,
        _cells: Uint8Array,
        _column: string,
        _mutation?: "insert" | "update" | "upsert",
        _author?: Uint8Array,
        _updatedAtMs?: number,
        _head?: unknown,
        _base?: unknown,
      ) => ({
        push: () => undefined,
        finish: () => fakeWrite(),
        abort: vi.fn(),
      }),
    );
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => fakeDb({ beginStreamingMutation }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new TextEncoder().encode(ownerAuthor),
      1,
      true,
      { readAuthorizationHost: "trusted-serving" },
    );

    await runtime.streamingMutation(
      "update",
      "todos",
      {},
      "title",
      (async function* () {
        yield "updated";
      })(),
      JSON.stringify({ attribution: testCase.attribution }),
      "00000000-0000-0000-0000-000000000123",
    );

    const author = beginStreamingMutation.mock.calls[0]?.[5];
    expect(author instanceof Uint8Array ? new TextDecoder().decode(author) : undefined).toBe(
      testCase.expectedAuthor ?? ownerAuthor,
    );
  });

  it("passes provenance separately from admission through the unified binding ABI", async () => {
    const insert = vi.fn(
      (
        _table: string,
        _cells: Uint8Array,
        _options: { author?: Uint8Array; attribution?: Uint8Array },
      ) => fakeWrite(),
    );
    const beginTransaction = vi.fn();
    const beginStreamingMutation = vi.fn(
      (
        _table: string,
        _rowId: Uint8Array,
        _cells: Uint8Array,
        _column: string,
        _mutation: "insert" | "update" | "upsert" | undefined,
        _author: Uint8Array | undefined,
        _attribution: Uint8Array,
      ) => ({
        push: () => undefined,
        finish: () => fakeWrite(),
        abort: () => undefined,
      }),
    );
    const nativeDb = fakeDb({
      insert,
      beginTransaction,
      beginStreamingMutation,
    });
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => {
          throw new Error("ordinary open must not be selected for a backend runtime");
        },
        openMemoryAsBackend: () => nativeDb,
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
      { backendMode: true, readAuthorizationHost: "trusted-serving" },
    );
    const attribution = JSON.stringify(["https://issuer.example", "alice"]);
    const context = JSON.stringify({ attribution });

    runtime.insert(
      "todos",
      { title: { type: "Text", value: "credited to alice" } },
      context,
      "00000000-0000-0000-0000-000000000123",
    );
    const insertCall = insert.mock.calls[0];
    expect(insertCall?.[0]).toBe("todos");
    expect(insertCall?.[2].author).toBeUndefined();
    expect(new TextDecoder().decode(insertCall?.[2].attribution!)).toBe(attribution);

    runtime.beginTransaction("mergeable", "attributed-batch" as never, context);
    const transactionCall = beginTransaction.mock.calls[0];
    expect(transactionCall?.[0]).toBe("attributed-batch");
    expect(transactionCall?.[1]).toBe("mergeable");
    expect(transactionCall?.[2]).toBeUndefined();
    expect(new TextDecoder().decode(transactionCall?.[3])).toBe(attribution);

    runtime.beginTransaction("exclusive", "attributed-exclusive" as never, context);
    const exclusiveCall = beginTransaction.mock.calls[1];
    expect(exclusiveCall?.[1]).toBe("exclusive");
    expect(new TextDecoder().decode(exclusiveCall?.[3])).toBe(attribution);

    await runtime.streamingMutation(
      "insert",
      "todos",
      {},
      "title",
      (async function* () {
        yield "credited to alice";
      })(),
      context,
      "00000000-0000-0000-0000-000000000125",
    );
    const streamingCall = beginStreamingMutation.mock.calls[0];
    expect(streamingCall?.[0]).toBe("todos");
    expect(new TextDecoder().decode(streamingCall?.[6])).toBe(attribution);

    const branched = JSON.stringify({ attribution, branch_view: { head: { values: {} } } });
    expect(() =>
      runtime.insert(
        "todos",
        { title: { type: "Text", value: "must not fall back to root" } },
        branched,
        "00000000-0000-0000-0000-000000000124",
      ),
    ).toThrow("do not support branch views");
    expect(insert).toHaveBeenCalledTimes(1);
  });

  it("rejects malformed backend attribution instead of falling back to SYSTEM", () => {
    const insert = vi.fn(() => fakeWrite());
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => {
          throw new Error("not used");
        },
        openMemoryAsBackend: () => fakeDb({ insert }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
      { backendMode: true, readAuthorizationHost: "trusted-serving" },
    );
    expect(() =>
      runtime.insert(
        "todos",
        { title: { type: "Text", value: "must not become SYSTEM" } },
        JSON.stringify({ attribution: `[ "https://issuer.example", "alice" ]` }),
        "00000000-0000-0000-0000-000000000123",
      ),
    ).toThrow("backend attribution must be a canonical author subject string");
    expect(insert).not.toHaveBeenCalled();
  });

  it("does not mix per-write provenance into a transaction opened without it", () => {
    const beginTransaction = vi.fn();
    const insert = vi.fn();
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => {
          throw new Error("not used");
        },
        openMemoryAsBackend: () => fakeDb({ beginTransaction, insert }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
      { backendMode: true, readAuthorizationHost: "trusted-serving" },
    );
    const id = "ordinary-batch" as never;
    runtime.beginTransaction("mergeable", id);

    expect(() =>
      runtime.insert(
        "todos",
        { title: { type: "Text", value: "must not lose provenance" } },
        JSON.stringify({
          transaction_id: id,
          attribution: JSON.stringify(["https://issuer.example", "alice"]),
        }),
        "00000000-0000-0000-0000-000000000123",
      ),
    ).toThrow("opened without backend attribution");
    expect(insert).not.toHaveBeenCalled();
  });

  it("binds reserved backend attribution to the exact verified session capability", () => {
    const insert = vi.fn(
      (
        _table: string,
        _cells: Uint8Array,
        options?: { rowId?: Uint8Array; attribution?: Uint8Array },
      ) => ({
        ...fakeWrite(),
        rowId: options?.rowId ?? new Uint8Array(16),
      }),
    );
    const runtime = new NativeRuntimeAdapter(
      {
        openMemoryAsBackend: () => fakeDb({ insert }),
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
      { backendMode: true, readAuthorizationHost: "trusted-serving" },
    );
    const session = internalSessionFromVerifiedReservedJwtPayload(
      {
        iss: LOCAL_FIRST_JWT_ISSUER,
        sub: "verified-attribution",
      },
      "local-first",
    )!;
    session.account_id = "00000000-0000-4000-8000-000000000001";
    const token = trustedReservedSessionToken(session);
    const attribution = JSON.stringify([session.account_id, session.issuer, session.user_id]);
    const write = (context: object) =>
      runtime.insert(
        "todos",
        {
          title: { type: "Text", value: "verified provenance" },
        },
        JSON.stringify(context),
        "00000000-0000-0000-0000-000000000123",
      );
    write({ attribution, session: { ...session, [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: token } });
    expect(insert).toHaveBeenCalledTimes(1);
    expect(new TextDecoder().decode(insert.mock.calls[0]![2]?.attribution!)).toBe(attribution);
    for (const context of [
      { attribution },
      { attribution, session },
      {
        attribution: JSON.stringify([session.account_id, session.issuer, "different-subject"]),
        session: { ...session, [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: token },
      },
      {
        attribution: JSON.stringify([
          "00000000-0000-4000-8000-000000000002",
          session.issuer,
          session.user_id,
        ]),
        session: { ...session, [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: token },
      },
      {
        attribution: JSON.stringify([
          "00000000-0000-0000-0000-000000000000",
          SYSTEM_SESSION_ISSUER,
          session.user_id,
        ]),
        session: { ...session, [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: token },
      },
    ])
      expect(() => write(context)).toThrow("reserved issuer");
    expect(insert).toHaveBeenCalledTimes(1);
  });

  it("rejects reserved public write-context sessions and attributions", async () => {
    const beginStreamingMutation = vi.fn(() => ({
      push: () => undefined,
      finish: () => fakeWrite(),
      abort: vi.fn(),
    }));
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => fakeDb({ beginStreamingMutation }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new TextEncoder().encode(JSON.stringify(["urn:jazz:test", "owner"])),
      1,
      true,
      { readAuthorizationHost: "trusted-serving" },
    );

    async function* source() {
      yield "updated";
    }

    for (const issuer of RESERVED_TEST_ISSUERS) {
      await expect(
        runtime.streamingMutation(
          "update",
          "todos",
          {},
          "title",
          source(),
          JSON.stringify({
            session: {
              issuer,
              user_id: "public-caller",
              claims: {},
            },
          }),
          "00000000-0000-0000-0000-000000000123",
        ),
      ).rejects.toThrow("reserved issuer");

      await expect(
        runtime.streamingMutation(
          "update",
          "todos",
          {},
          "title",
          source(),
          JSON.stringify({ attribution: JSON.stringify([issuer, "public-caller"]) }),
          "00000000-0000-0000-0000-000000000123",
        ),
      ).rejects.toThrow("reserved issuer");
    }

    await expect(
      runtime.streamingMutation(
        "update",
        "todos",
        {},
        "title",
        source(),
        JSON.stringify({ attribution: SYSTEM_AUTHOR_ID }),
        "00000000-0000-0000-0000-000000000123",
      ),
    ).rejects.toThrow("reserved issuer");

    expect(beginStreamingMutation).not.toHaveBeenCalled();
  });

  it("admits verified reserved write-context sessions carrying the runtime capability", async () => {
    const beginStreamingMutation = vi.fn(
      (
        _table: string,
        _rowId: Uint8Array,
        _cells: Uint8Array,
        _column: string,
        _mutation?: "insert" | "update" | "upsert",
        _author?: Uint8Array,
        _updatedAtMs?: number,
        _head?: unknown,
        _base?: unknown,
      ) => ({
        push: () => undefined,
        finish: () => fakeWrite(),
        abort: vi.fn(),
      }),
    );
    const trustedSession = internalSessionFromVerifiedReservedJwtPayload(
      { iss: LOCAL_FIRST_JWT_ISSUER, sub: "verified-writer" },
      "local-first",
    )!;
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () => fakeDb({ beginStreamingMutation }),
        openBrowser: async () => {
          throw new Error("not used");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      new TextEncoder().encode(JSON.stringify(["urn:jazz:test", "owner"])),
      1,
      true,
      { readAuthorizationHost: "trusted-serving" },
    );

    await runtime.streamingMutation(
      "update",
      "todos",
      {},
      "title",
      (async function* () {
        yield "updated";
      })(),
      JSON.stringify({
        session: {
          ...trustedSession,
          [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: trustedReservedSessionToken(trustedSession),
        },
      }),
      "00000000-0000-0000-0000-000000000123",
    );

    const author = beginStreamingMutation.mock.calls[0]?.[5];
    expect(author instanceof Uint8Array ? new TextDecoder().decode(author) : undefined).toBe(
      '["urn:jazz:local-first","verified-writer"]',
    );
  });

  it("aborts the native upload when the producer fails", async () => {
    const abort = vi.fn();
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            beginStreamingMutation: () => ({
              push: () => undefined,
              finish: () => fakeWrite(),
              abort,
            }),
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

    await expect(
      runtime.streamingMutation(
        "insert",
        "todos",
        {},
        "title",
        (async function* () {
          yield "partial";
          throw new Error("producer failed");
        })(),
      ),
    ).rejects.toThrow("producer failed");
    expect(abort).toHaveBeenCalledOnce();
  });
});

describe("NativeRuntimeAdapter TS adapter perf canary", () => {
  it.skipIf(process.env.JAZZ_TS_ADAPTER_PERF !== "1")(
    "measures reset delivery for one large subscription and many small subscriptions",
    () => {
      const largeRows = Array.from({ length: 24_000 }, (_, index) => ({
        table: "todos",
        rowId: indexedUuidBytes(index + 1),
        title: `large-${index}`,
      }));
      const smallChunks = Array.from({ length: 95 }, (_, subscriptionIndex) =>
        Array.from({ length: 7 }, (_, rowIndex) => ({
          table: "todos",
          rowId: indexedUuidBytes(100_000 + subscriptionIndex * 100 + rowIndex),
          title: `small-${subscriptionIndex}-${rowIndex}`,
        })),
      );

      const measurements: Array<{ label: string; rows: number; ms: number }> = [];
      const runSubscription = (label: string, rows: EncodedTestRow[]) => {
        const chunk = {
          type: "delta",
          reset: true,
          settled: true,
          delta: encodeSubscriptionDelta({ added: rows, updated: [], removed: [] }),
        };
        const runtime = runtimeWithNativeSubscriptionChunk(chunk);
        let callbackCount = 0;
        let addedCount = 0;
        const handle = runtime.createSubscription(
          JSON.stringify({ table: "todos" }),
          null,
          null,
          null,
        );
        const started = performance.now();
        runtime.executeSubscription(handle, (delta: RuntimeSubscriptionDelta) => {
          addedCount += delta.added.length;
          callbackCount += 1;
        });
        const ms = performance.now() - started;
        expect(callbackCount).toBe(1);
        expect(addedCount).toBe(rows.length);
        measurements.push({ label, rows: rows.length, ms });
        runtime.close();
      };

      runSubscription("large-reset", largeRows);
      for (let index = 0; index < smallChunks.length; index += 1) {
        runSubscription(`small-reset-${index}`, smallChunks[index]!);
      }

      const smallMs = measurements.slice(1).reduce((sum, measurement) => sum + measurement.ms, 0);
      const smallMedian =
        measurements
          .slice(1)
          .map((measurement) => measurement.ms)
          .sort((left, right) => left - right)[Math.floor(smallChunks.length / 2)] ?? 0;
      console.info(
        JSON.stringify({
          largeMs: measurements[0]!.ms,
          smallTotalMs: smallMs,
          smallMedianMs: smallMedian,
          smallCount: smallChunks.length,
        }),
      );
    },
  );
});

const testSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;

function emptyNativeRuntime(): NativeRuntimeAdapter {
  return new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => new Uint8Array([0]),
          subscribe: () => new ReadableStream(),
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
}

function runtimeWithSubscriptionSource(source: unknown): NativeRuntimeAdapter {
  return new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          subscribe: () => source,
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
}

function runtimeWithNativeSubscriptionChunk(
  chunk: unknown,
  schema: WasmSchema = testSchema,
): NativeRuntimeAdapter {
  // Native readAll drains its queue; returning the same non-empty batch forever
  // would make the adapter's bounded drain loop spin rather than model NAPI.
  const chunks = [chunk];
  return new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => new Uint8Array([0]),
          subscribe: () => ({
            readAll: () => chunks.splice(0),
            close: () => true,
          }),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    schema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
}

function runtimeWithNativeRelationSubscriptionChunks(
  chunks: unknown[],
  schema: WasmSchema = testSchema,
): NativeRuntimeAdapter {
  return new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          subscribe: () => ({
            readAll: () => chunks.splice(0),
            close: () => true,
          }),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    schema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
}

function relationSubscriptionChunk({
  reset = false,
  settled = true,
  rootAdded = [],
  rootUpdated = [],
  rootRemoved = [],
}: {
  reset?: boolean;
  settled?: boolean;
  rootAdded?: EncodedTestRow[];
  rootUpdated?: EncodedTestRow[];
  rootRemoved?: Array<{ table: string; rowId: Uint8Array }>;
}) {
  return {
    type: "delta",
    reset,
    settled,
    delta: encodeSubscriptionDelta({
      added: rootAdded,
      updated: rootUpdated,
      removed: rootRemoved,
    }),
  };
}

function indexedUuidBytes(index: number): Uint8Array {
  const bytes = new Uint8Array(16);
  new DataView(bytes.buffer).setUint32(12, index, false);
  return bytes;
}

function readPreparedComparison(query: Uint8Array): {
  table: string;
  predicateTag: number;
  column: string;
  literalTag: number;
  value: string;
  limit: number | undefined;
} {
  const reader = new PostcardReader(query);
  const table = reader.string();
  const predicateCount = reader.u64();
  expect(predicateCount).toBe(1);
  const predicateTag = reader.u64();
  const leftOperandTag = reader.u64();
  expect(leftOperandTag).toBe(0);
  const column = reader.string();
  const rightOperandTag = reader.u64();
  expect(rightOperandTag).toBe(3);
  const literalTag = reader.u64();
  const value = reader.string();
  const tail = readPreparedQueryTail(reader);
  const limit = tail.limit;
  return { table, predicateTag, column, literalTag, value, limit };
}

function readPreparedUuidComparison(query: Uint8Array): {
  table: string;
  predicateTag: number;
  column: string;
  literalTag: number;
  value: string;
  limit: number | undefined;
} {
  const reader = new PostcardReader(query);
  const table = reader.string();
  const predicateCount = reader.u64();
  expect(predicateCount).toBe(1);
  const predicateTag = reader.u64();
  const leftOperandTag = reader.u64();
  expect(leftOperandTag).toBe(0);
  const column = reader.string();
  const rightOperandTag = reader.u64();
  expect(rightOperandTag).toBe(3);
  const literalTag = reader.u64();
  const value = formatUuidForTest(reader.bytes());
  const tail = readPreparedQueryTail(reader);
  const limit = tail.limit;
  return { table, predicateTag, column, literalTag, value, limit };
}

function readPreparedUuidIn(query: Uint8Array): {
  table: string;
  column: string;
  values: string[];
} {
  const reader = new PostcardReader(query);
  const table = reader.string();
  const predicateCount = reader.u64();
  expect(predicateCount).toBe(1);
  expect(reader.u64()).toBe(5);
  expect(reader.u64()).toBe(0);
  const column = reader.string();
  const values = reader.readVec((valueReader) => {
    expect(valueReader.u64()).toBe(3);
    expect(valueReader.u64()).toBe(9);
    return formatUuidForTest(valueReader.bytes());
  });
  return { table, column, values };
}

function readPreparedInLiterals(
  query: Uint8Array,
): Array<{ column: string; literals: Array<{ tag: number; value: number | bigint }> }> {
  const reader = new PostcardReader(query);
  reader.string();
  return reader.readVec((predicateReader) => {
    expect(predicateReader.u64()).toBe(5);
    expect(predicateReader.u64()).toBe(0);
    const column = predicateReader.string();
    const literals = predicateReader.readVec((valueReader) => {
      expect(valueReader.u64()).toBe(3);
      return readPreparedNumericLiteral(valueReader);
    });
    return { column, literals };
  });
}

function readPreparedComparisonLiterals(query: Uint8Array): Array<{
  predicateTag: number;
  column: string;
  literal: { tag: number; value: number | bigint };
}> {
  const reader = new PostcardReader(query);
  reader.string();
  return reader.readVec((predicateReader) => {
    const predicateTag = predicateReader.u64();
    expect(predicateReader.u64()).toBe(0);
    const column = predicateReader.string();
    expect(predicateReader.u64()).toBe(3);
    return { predicateTag, column, literal: readPreparedNumericLiteral(predicateReader) };
  });
}

function readPreparedNumericLiteral(reader: PostcardReader): {
  tag: number;
  value: number | bigint;
} {
  const tag = reader.u64();
  switch (tag) {
    case 2:
    case 3:
      return { tag, value: reader.u64() };
    case 4:
      return { tag, value: reader.f64Le() };
    case 14:
      return { tag, value: reader.i64() };
    case 15:
      return { tag, value: Number(reader.i64()) };
    default:
      throw new Error(`expected numeric prepared literal tag, got ${tag}`);
  }
}

function readPreparedLimit(query: Uint8Array): number | undefined {
  const reader = new PostcardReader(query);
  reader.string();
  reader.readVec(() => {
    skipPreparedPredicate(reader);
  });
  return readPreparedQueryTail(reader).limit;
}

function skipPreparedPredicate(reader: PostcardReader): void {
  const predicateTag = reader.u64();
  if (predicateTag === 5) {
    skipPreparedOperand(reader);
    reader.readVec(() => {
      skipPreparedOperand(reader);
    });
    return;
  }
  skipPreparedOperand(reader);
  skipPreparedOperand(reader);
}

function skipPreparedOperand(reader: PostcardReader): void {
  const operandTag = reader.u64();
  if (operandTag === 0) {
    reader.string();
    return;
  }
  expect(operandTag).toBe(3);
  skipPreparedLiteral(reader);
}

function skipPreparedLiteral(reader: PostcardReader): void {
  const literalTag = reader.u64();
  switch (literalTag) {
    case 2:
    case 3:
      reader.u64();
      return;
    case 4:
      reader.f64Le();
      return;
    case 14:
    case 15:
      reader.i64();
      return;
    case 5:
      reader.bool();
      return;
    case 6:
      reader.string();
      return;
    case 7:
    case 9:
      reader.bytes();
      return;
    case 11:
    case 12:
      reader.readVec(() => {
        skipPreparedLiteral(reader);
      });
      return;
    case 13:
      reader.option(() => {
        skipPreparedLiteral(reader);
      });
      return;
    default:
      throw new Error(`unsupported prepared literal tag ${literalTag}`);
  }
}

function readPreparedQueryTail(
  reader: PostcardReader,
  opts: { prefixAlreadySkipped?: boolean } = {},
): {
  select: string[] | undefined;
  orderBy: Array<{ column: string; directionTag: number }>;
  limit: number | undefined;
  offset: number;
} {
  if (!opts.prefixAlreadySkipped) {
    reader.readVec(() => undefined); // joins
    reader.option(() => undefined); // flat_join
    reader.readVec(() => undefined); // policy_branches
    reader.readVec(() => undefined); // reachable
    reader.readVec(() => undefined); // inherits
    reader.readVec(() => undefined); // includes
    reader.readVec(() => undefined); // array_subqueries
  }
  const select = reader.option((selectReader) => selectReader.readVec(() => selectReader.string()));
  const orderByCount = reader.u64();
  const orderBy = Array.from({ length: orderByCount }, () => ({
    column: reader.string(),
    directionTag: reader.u64(),
  }));
  reader.option(() => undefined); // aggregate
  const limit = reader.option((optionReader) => optionReader.u64());
  const offset = reader.u64();
  return { select, orderBy, limit, offset };
}

function expectPreparedRelationEnvelope(query: Uint8Array, table: string, select?: string[]): void {
  const reader = new PostcardReader(query);
  expect(reader.string()).toBe(table);
  expect(reader.readVec(() => undefined)).toEqual([]); // filters
  const tail = readPreparedQueryTail(reader);
  expect(tail).toEqual({ select, orderBy: [], limit: undefined, offset: 0 });
  expect(reader.option(() => true)).toBe(true);
}

function readPreparedSelect(query: Uint8Array): string[] | undefined {
  const reader = new PostcardReader(query);
  reader.string();
  reader.readVec(() => {
    reader.u64();
    reader.u64();
    reader.string();
    reader.u64();
    reader.u64();
    reader.string();
  });
  reader.readVec(() => undefined);
  reader.option(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  reader.readVec(() => undefined);
  return readPreparedQueryTail(reader, { prefixAlreadySkipped: true }).select;
}

function readPreparedQueryShape(query: Uint8Array): {
  table: string;
  predicates: Array<{ column: string; opTag: number; literalTag: number; value: string }>;
  orderBy: Array<{ column: string; directionTag: number }>;
  limit: number | undefined;
  offset: number;
} {
  const reader = new PostcardReader(query);
  const table = reader.string();
  const predicateCount = reader.u64();
  const predicates = Array.from({ length: predicateCount }, () => {
    const opTag = reader.u64();
    expect(reader.u64()).toBe(0);
    const column = reader.string();
    expect(reader.u64()).toBe(3);
    const literalTag = reader.u64();
    const value = reader.string();
    return { column, opTag, literalTag, value };
  });
  const { orderBy, limit, offset } = readPreparedQueryTail(reader);
  return { table, predicates, orderBy, limit, offset };
}

function readPreparedFirstLiteral(query: Uint8Array): {
  column: string;
  opTag: number;
  literalTag: number;
  value: number;
} {
  const reader = new PostcardReader(query);
  reader.string();
  expect(reader.u64()).toBeGreaterThan(0);
  const opTag = reader.u64();
  expect(reader.u64()).toBe(0);
  const column = reader.string();
  expect(reader.u64()).toBe(3);
  const literalTag = reader.u64();
  const value =
    literalTag === 13 || literalTag === 14 || literalTag === 15
      ? Number(reader.i64())
      : reader.u64();
  return { column, opTag, literalTag, value };
}

function unsupportedJoinRelationIr(): unknown {
  return {
    Join: {
      left: { TableScan: { table: "todos" } },
      right: { TableScan: { table: "projects" } },
      on: [
        {
          left: { column: "todos.project_id" },
          right: { column: "projects.id" },
        },
      ],
      join_kind: "Inner",
    },
  };
}

function supportedGatherRelationIr(table: string): unknown {
  return {
    Gather: {
      seed: { TableScan: { table } },
      step: { TableScan: { table } },
      frontier_key: { RowId: "Current" },
      bound: "Fixpoint",
      dedupe_key: [{ RowId: "Current" }],
    },
  };
}

function unsupportedProjectRelationIr(): unknown {
  return {
    Project: {
      input: { TableScan: { table: "todos" } },
      columns: [{ expr: { Column: { column: "title" } }, alias: "title" }],
    },
  };
}

const arraySchema = {
  arrays: {
    columns: [
      {
        name: "chunk_refs",
        column_type: { type: "Array", element: { type: "Uuid" } },
        nullable: false,
      },
      {
        name: "chunk_sizes",
        column_type: { type: "Array", element: { type: "Double" } },
        nullable: false,
      },
    ],
  },
} satisfies WasmSchema;

class FakeTransport implements Transport {
  closed = false;
  readonly received: Uint8Array[] = [];
  readonly receivedBatches: Uint8Array[][] = [];
  tickCount = 0;

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

  tick(): number {
    this.tickCount += 1;
    return 0;
  }
}

class FakeWebSocket {
  binaryType: "arraybuffer" | "blob" = "arraybuffer";
  readonly readyState = 1;
  readonly sent: Array<Uint8Array | string> = [];
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
  }

  emitMessage(data: Uint8Array): void {
    for (const listener of this.messageListeners) listener({ data });
  }
}

function encodeWireError(code: number, retry: number, message: string): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(2);
  writer.u64(code);
  writer.u64(retry);
  writer.string(message);
  return writer.finish();
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

type EncodedTestRow = {
  table: string;
  rowId: Uint8Array;
  title: string;
  txTime?: number;
  createdAt?: number;
  updatedAt?: number;
};

function encodeRows(rows: EncodedTestRow[]): Uint8Array {
  const writer = new PostcardWriter();
  writeRowBatches(writer, rows);
  return writer.finish();
}

function encodeRelationSnapshot(rows: EncodedTestRow[], rootCount = rows.length): Uint8Array {
  const writer = new PostcardWriter();
  writer.u64(rootCount);
  writeRowBatches(writer, rows);
  return writer.finish();
}

function subscriptionReset(rows: EncodedTestRow[]) {
  return {
    type: "delta",
    reset: true,
    delta: encodeSubscriptionDelta({ added: rows, updated: [], removed: [] }),
  };
}

function encodeTerminalRelationSnapshot(schema: WasmSchema): Uint8Array {
  const childColumns = schema.todos!.columns;
  const rootColumns: ColumnDescriptor[] = [
    schema.users!.columns[0]!,
    {
      name: "todosViaOwner",
      column_type: { type: "Array", element: { type: "Row", columns: childColumns } },
      nullable: false,
    },
  ];
  const childDescriptor = [
    { name: "row_uuid", valueType: { tag: 11 } },
    { name: "title", valueType: storageColumnValueType(childColumns[0]!) },
  ];
  const descriptor = [
    { name: "title", valueType: storageColumnValueType(rootColumns[0]!) },
    {
      name: "todosViaOwner",
      valueType: { tag: 14, inner: { tag: 16, record: childDescriptor } },
    },
  ];
  const childRecord = concatBytes([
    uuidBytes("00000000-0000-0000-0000-000000000002"),
    createRecord(childDescriptor.slice(1), [inlineScalar("Ship relation reads")]),
  ]);
  const nestedRowsHeader = new Uint8Array(4);
  new DataView(nestedRowsHeader.buffer).setUint32(0, 1, true);
  const nestedRows = concatBytes([nestedRowsHeader, childRecord]);
  const writer = new PostcardWriter();
  writer.u64(1);
  writer.vec((batch) => {
    batch.string("users");
    writeNativeRowDescriptor(batch, physicalNativeDescriptor(descriptor));
    batch.vec((row) => {
      row.bytes(uuidBytes("00000000-0000-0000-0000-000000000001"));
      row.bool(false);
      row.bytes(createRecord(descriptor, [inlineScalar("Ada"), nestedRows]));
    }, 1);
  }, 1);
  return writer.finish();
}

function encodeTerminalSubscriptionDelta(schema: WasmSchema): Uint8Array {
  const rowBatches = encodeTerminalRelationSnapshot(schema).subarray(1);
  const suffix = new PostcardWriter();
  suffix.vec(() => {}, 0); // updated rows
  suffix.vec(() => {}, 0); // removed rows
  const rowId = uuidBytes("00000000-0000-0000-0000-000000000001");
  const resultKey = Uint8Array.from([1, ...rowId, 0, 0, 0, 0, 0, 0, 0, 0]);
  suffix.vec((key) => key.bytes(resultKey), 1); // added occurrence keys
  suffix.vec(() => {}, 0); // updated occurrence keys
  suffix.vec(() => {}, 0); // removed occurrence keys
  suffix.vec((index) => index.u64(0), 1); // added indices
  suffix.vec(() => {}, 0); // updated previous indices
  suffix.vec(() => {}, 0); // updated indices
  suffix.vec(() => {}, 0); // removed indices
  return concatBytes([rowBatches, suffix.finish()]);
}

function writeRowBatches(
  writer: PostcardWriter,
  rows: EncodedTestRow[],
  fieldKind: "stored-column" | "result-field" = "stored-column",
): void {
  const rowsByTable = new Map<string, EncodedTestRow[]>();
  for (const row of rows) {
    const tableRows = rowsByTable.get(row.table) ?? [];
    tableRows.push(row);
    rowsByTable.set(row.table, tableRows);
  }
  writer.vec((batch, batchIndex) => {
    const [table, tableRows] = Array.from(rowsByTable.entries())[batchIndex]!;
    const hasTxTime = tableRows.some((row) => row.txTime !== undefined);
    const hasProvenance = tableRows.some(
      (row) => row.createdAt !== undefined || row.updatedAt !== undefined,
    );
    const descriptor = [
      { name: "title", valueType: { tag: 8 } },
      ...(hasProvenance
        ? [
            { name: "$createdAt", valueType: { tag: 3 } },
            { name: "$updatedAt", valueType: { tag: 3 } },
          ]
        : []),
      ...(hasTxTime ? [{ name: "tx_time", valueType: { tag: 3 } }] : []),
    ];
    batch.string(table);
    writeNativeRowDescriptor(
      batch,
      descriptor.map((field, index) =>
        fieldKind === "stored-column"
          ? {
              id: index + 1,
              outputName: field.name,
              valueType: field.valueType,
              kind: fieldKind,
            }
          : {
              name: field.name,
              valueType: field.valueType,
              kind: fieldKind,
            },
      ),
    );
    batch.vec((row, index) => {
      const source = tableRows[index]!;
      row.bytes(source.rowId);
      row.bool(false);
      const values: Uint8Array[] = [inlineScalar(source.title)];
      if (hasProvenance) {
        values.push(u64Bytes(source.createdAt ?? 0));
        values.push(u64Bytes(source.updatedAt ?? 0));
      }
      if (hasTxTime) {
        values.push(txTimeBytes(source.txTime ?? 0));
      }
      row.bytes(createRecord(descriptor, values));
    }, tableRows.length);
  }, rowsByTable.size);
}

function encodeSubscriptionDelta(delta: {
  added: EncodedTestRow[];
  updated: EncodedTestRow[];
  removed: Array<{ table: string; rowId: Uint8Array }>;
  addedOccurrenceKeys?: Uint8Array[];
  updatedOccurrenceKeys?: Uint8Array[];
  removedOccurrenceKeys?: Uint8Array[];
  addedIndices?: number[];
  updatedPreviousIndices?: number[];
  updatedIndices?: number[];
  removedIndices?: number[];
}): Uint8Array {
  const writer = new PostcardWriter();
  writeRowBatches(writer, delta.added);
  writeRowBatches(writer, delta.updated);
  writer.vec((removed, index) => {
    const source = delta.removed[index]!;
    removed.string(source.table);
    removed.bytes(source.rowId);
  }, delta.removed.length);
  const rowKey = (rowId: Uint8Array) => Uint8Array.from([1, ...rowId, 0, 0, 0, 0, 0, 0, 0, 0]);
  for (const keys of [
    delta.addedOccurrenceKeys ?? delta.added.map((row) => rowKey(row.rowId)),
    delta.updatedOccurrenceKeys ?? delta.updated.map((row) => rowKey(row.rowId)),
    delta.removedOccurrenceKeys ?? delta.removed.map((row) => rowKey(row.rowId)),
  ]) {
    writer.vec((key, index) => key.bytes(keys[index]!), keys.length);
  }
  for (const indices of [
    delta.addedIndices ?? delta.added.map((_, index) => index),
    delta.updatedPreviousIndices ?? delta.updated.map((_, index) => index),
    delta.updatedIndices ?? delta.updated.map((_, index) => index),
    delta.removedIndices ?? delta.removed.map((_, index) => index),
  ]) {
    writer.vec((indexWriter, index) => indexWriter.u64(indices[index]!), indices.length);
  }
  return writer.finish();
}

it("keeps same-row union occurrences distinct through apply, removal, and reopen", () => {
  const rowId = new Uint8Array(16).fill(7);
  const typedKey = (label: string) => {
    const labelBytes = inlineScalar(label);
    const key = new Uint8Array(1 + 16 + 4 + 16 + 4 + 4 + 4 + labelBytes.length);
    key[0] = 1;
    key.fill(7, 1, 17);
    new DataView(key.buffer).setUint32(17, 1);
    key.fill(8, 21, 37);
    new DataView(key.buffer).setUint32(37, 1);
    new DataView(key.buffer).setUint32(41, 0);
    new DataView(key.buffer).setUint32(45, labelBytes.length);
    key.set(labelBytes, 49);
    return key;
  };
  const direct = typedKey("direct");
  const inherited = typedKey("inherited");
  const decode = (bytes: Uint8Array) => readNativeSubscriptionDelta(new PostcardReader(bytes));
  const initial = decode(
    encodeSubscriptionDelta({
      added: [
        { table: "todos", rowId, title: "direct" },
        { table: "todos", rowId, title: "inherited" },
      ],
      updated: [],
      removed: [],
      addedOccurrenceKeys: [direct, inherited],
    }),
  );
  const first = decodeSubscriptionDelta(initial, testSchema);
  const firstDelta = runtimeDeltaChanges(first);
  expect(firstDelta.map((change) => change.id)).toEqual([
    expect.stringContaining("result:01"),
    expect.stringContaining("result:01"),
  ]);
  expect(firstDelta[0]!.id).not.toBe(firstDelta[1]!.id);
  const manager = new SubscriptionManager<{ id: string; title: string }>();
  const transformed = manager.handleDelta(first, (row) => ({
    id: row.id,
    title: row.values[0]?.type === "Text" ? row.values[0].value : "",
  }));
  expect(transformed.all).toHaveLength(2);
  expect(transformed.all?.map((item) => item.id)).toEqual([formatUuid(rowId), formatUuid(rowId)]);
  const publicRows: Array<{ id: string; title: string }> = [];
  applySubscriptionDelta(publicRows, transformed);
  expect(publicRows).toHaveLength(2);

  const update = decode(
    encodeSubscriptionDelta({
      added: [],
      updated: [{ table: "todos", rowId, title: "inherited updated" }],
      removed: [],
      updatedOccurrenceKeys: [inherited],
    }),
  );
  const afterUpdate = decodeSubscriptionDelta(update, testSchema);
  const updatedDelta = runtimeDeltaChanges(afterUpdate);
  expect(updatedDelta).toHaveLength(1);
  expect(updatedDelta[0]!.id).toBe(firstDelta[1]!.id);
  const publicUpdate = manager.handleDelta(afterUpdate, (row) => ({
    id: row.id,
    title: row.values[0]?.type === "Text" ? row.values[0].value : "",
  }));
  expect(publicUpdate.all).toHaveLength(2);
  applySubscriptionDelta(publicRows, publicUpdate);
  expect(publicRows).toHaveLength(2);

  const removal = decode(
    encodeSubscriptionDelta({
      added: [],
      updated: [],
      removed: [{ table: "todos", rowId }],
      removedOccurrenceKeys: [direct],
    }),
  );
  const second = decodeSubscriptionDelta(removal, testSchema);
  expect(runtimeDeltaChanges(second)[0]!.id).toBe(firstDelta[0]!.id);
  const publicRemoval = manager.handleDelta(second, (row) => ({ id: row.id, title: "" }));
  expect(publicRemoval.all).toHaveLength(1);
  applySubscriptionDelta(publicRows, publicRemoval);
  expect(publicRows).toHaveLength(1);

  const reopened = decodeSubscriptionDelta(
    decode(
      encodeSubscriptionDelta({
        added: [{ table: "todos", rowId, title: "inherited" }],
        updated: [],
        removed: [],
        addedOccurrenceKeys: [inherited],
      }),
    ),
    testSchema,
    true,
  );
  expect(runtimeDeltaChanges(reopened)[0]!.id).toBe(firstDelta[1]!.id);
  const publicReset = manager.handleDelta(reopened, (row) => ({
    id: row.id,
    title: row.values[0]?.type === "Text" ? row.values[0].value : "",
  }));
  applySubscriptionDelta(publicRows, publicReset);
  expect(publicRows).toEqual([{ id: formatUuid(rowId), title: "inherited" }]);
});

it("uses Rust's explicit indices for root replacement and movement", () => {
  const ids = [1, 2, 3].map((value) => {
    const bytes = new Uint8Array(16);
    bytes[15] = value;
    return bytes;
  });
  const decode = (bytes: Uint8Array) => readNativeSubscriptionDelta(new PostcardReader(bytes));
  const initial = decodeSubscriptionDelta(
    decode(
      encodeSubscriptionDelta({
        added: ids.map((rowId, index) => ({ table: "todos", rowId, title: `todo-${index}` })),
        updated: [],
        removed: [],
      }),
    ),
    testSchema,
  );
  const manager = new SubscriptionManager<{ id: string }>();
  const apply = (delta: RuntimeSubscriptionDelta) =>
    manager.handleDelta(delta, (row) => ({ id: row.id })).all?.map((row) => row.id);
  expect(apply(initial)).toEqual(ids.map((id) => formatUuid(id)));
  const replaced = ids[0]!;
  const afterTitleOnlyReplacement = decodeSubscriptionDelta(
    decode(
      encodeSubscriptionDelta({
        added: [],
        updated: [{ table: "todos", rowId: replaced, title: "renamed" }],
        removed: [],
        updatedPreviousIndices: [0],
        updatedIndices: [0],
      }),
    ),
    testSchema,
  );

  expect(apply(afterTitleOnlyReplacement)).toEqual(ids.map((id) => formatUuid(id)));
  expect(runtimeDeltaChanges(afterTitleOnlyReplacement)).toEqual([
    expect.objectContaining({ id: formatUuid(replaced), index: 0 }),
  ]);

  const afterSortReplacement = decodeSubscriptionDelta(
    decode(
      encodeSubscriptionDelta({
        added: [],
        updated: [{ table: "todos", rowId: replaced, title: "renamed and moved" }],
        removed: [],
        updatedPreviousIndices: [0],
        updatedIndices: [2],
      }),
    ),
    testSchema,
  );
  expect(apply(afterSortReplacement)).toEqual([
    formatUuid(ids[1]!),
    formatUuid(ids[2]!),
    formatUuid(replaced),
  ]);

  const moved = ids[2]!;
  const afterExplicitMove = decodeSubscriptionDelta(
    decode(
      encodeSubscriptionDelta({
        added: [],
        updated: [{ table: "todos", rowId: moved, title: "todo-2" }],
        removed: [],
        updatedPreviousIndices: [1],
        updatedIndices: [0],
      }),
    ),
    testSchema,
  );
  expect(apply(afterExplicitMove)).toEqual([
    formatUuid(moved),
    formatUuid(ids[1]!),
    formatUuid(replaced),
  ]);
});

it("preserves the producer's explicit position over lazy relation state", () => {
  const rowId = new Uint8Array(16);
  rowId[15] = 3;
  const decode = (bytes: Uint8Array) => readNativeSubscriptionDelta(new PostcardReader(bytes));
  const applied = decodeSubscriptionDelta(
    decode(
      encodeSubscriptionDelta({
        added: [{ table: "todos", rowId, title: "third" }],
        updated: [],
        removed: [],
        addedIndices: [2],
      }),
    ),
    testSchema,
    false,
    null,
  );

  expect(runtimeDeltaChanges(applied)).toEqual([
    expect.objectContaining({ id: formatUuid(rowId), index: 2 }),
  ]);
});

function authorFixtureDescriptor(): DescriptorField[] {
  return [
    { name: "account", valueType: { tag: 11 } },
    {
      name: "identity",
      valueType: {
        tag: 16,
        record: [
          { name: "issuer", valueType: { tag: 8 } },
          { name: "subject", valueType: { tag: 8 } },
        ],
      },
    },
  ];
}

function encodedAuthorFixture(subject = inlineScalar("user-1")): Uint8Array {
  const descriptor = authorFixtureDescriptor();
  return createRecord(descriptor, [
    uuidBytes("00000000-0000-4000-8000-000000000001"),
    createRecord(descriptor[1]!.valueType.record!, [
      inlineScalar("https://issuer.example"),
      subject,
    ]),
  ]);
}

function encodeUserWrappedSubscriptionDelta(row: {
  table: string;
  rowId: Uint8Array;
  title: string;
  titleBytes?: Uint8Array;
  note: string;
  provenanceBytes?: Uint8Array;
}): Uint8Array {
  const descriptor = [
    { name: "row_uuid", valueType: { tag: 11 } },
    { name: "user_title", valueType: { tag: 15, inner: { tag: 8 } } },
    { name: "user_note", valueType: { tag: 15, inner: { tag: 15, inner: { tag: 8 } } } },
    { name: "$createdBy", valueType: { tag: 16, record: authorFixtureDescriptor() } },
    { name: "$createdAt", valueType: { tag: 3 } },
  ];
  const delta = new PostcardWriter();
  delta.vec((batch) => {
    batch.string(row.table);
    writeNativeRowDescriptor(
      batch,
      currentNativeDescriptor(descriptor, { user_title: "title", user_note: "note" }),
    );
    batch.vec((encodedRow) => {
      encodedRow.bytes(row.rowId);
      encodedRow.bool(false);
      encodedRow.bytes(
        createRecord(descriptor, [
          row.rowId,
          presentBytes(row.titleBytes ?? inlineScalar(row.title)),
          presentBytes(presentBytes(inlineScalar(row.note))),
          row.provenanceBytes ?? encodedAuthorFixture(),
          u64Bytes(123),
        ]),
      );
    }, 1);
  }, 1);
  delta.vec(() => undefined, 0);
  delta.vec(() => undefined, 0);
  delta.vec((key) => key.bytes(Uint8Array.from([1, ...row.rowId, 0, 0, 0, 0, 0, 0, 0, 0])), 1);
  delta.vec(() => undefined, 0);
  delta.vec(() => undefined, 0);
  delta.vec((index) => index.u64(0), 1);
  delta.vec(() => undefined, 0);
  delta.vec(() => undefined, 0);
  delta.vec(() => undefined, 0);
  return delta.finish();
}

function encodeTeamGatherSubscriptionDelta(delta: {
  added?: Array<{ rowId: Uint8Array; name: string | null }>;
  updated?: Array<{ rowId: Uint8Array; name: string | null }>;
  addedOccurrenceKeys?: Uint8Array[];
  updatedOccurrenceKeys?: Uint8Array[];
}): Uint8Array {
  const descriptor = [
    { name: "row_uuid", valueType: { tag: 11 } },
    { name: "user_name", valueType: { tag: 15, inner: { tag: 8 } } },
    { name: "user_org_id", valueType: { tag: 15, inner: { tag: 11 } } },
    { name: "user_parent_id", valueType: { tag: 15, inner: { tag: 11 } } },
    { name: "$createdBy", valueType: { tag: 16, record: authorFixtureDescriptor() } },
    { name: "$createdAt", valueType: { tag: 3 } },
    { name: "$updatedBy", valueType: { tag: 16, record: authorFixtureDescriptor() } },
    { name: "$updatedAt", valueType: { tag: 3 } },
  ];
  const added = delta.added ?? [];
  const updated = delta.updated ?? [];
  const writer = new PostcardWriter();
  writeTeamGatherBatches(writer, added, descriptor);
  writeTeamGatherBatches(writer, updated, descriptor);
  writer.vec(() => undefined, 0);
  for (const keys of [
    delta.addedOccurrenceKeys ??
      added.map((row) => Uint8Array.from([1, ...row.rowId, 0, 0, 0, 0, 0, 0, 0, 0])),
    delta.updatedOccurrenceKeys ??
      updated.map((row) => Uint8Array.from([1, ...row.rowId, 0, 0, 0, 0, 0, 0, 0, 0])),
    [],
  ]) {
    writer.vec((key, index) => key.bytes(keys[index]!), keys.length);
  }
  writer.vec((indexWriter, index) => indexWriter.u64(index), added.length);
  writer.vec((indexWriter, index) => indexWriter.u64(index), updated.length);
  writer.vec((indexWriter, index) => indexWriter.u64(index), updated.length);
  writer.vec(() => undefined, 0);
  return writer.finish();
}

function writeTeamGatherBatches(
  writer: PostcardWriter,
  rows: Array<{ rowId: Uint8Array; name: string | null }>,
  descriptor: DescriptorField[],
): void {
  writer.vec(
    (batch) => {
      batch.string("teams");
      writeNativeRowDescriptor(
        batch,
        currentNativeDescriptor(descriptor, {
          user_name: "name",
          user_org_id: "org_id",
          user_parent_id: "parent_id",
        }),
      );
      batch.vec((row, index) => {
        const source = rows[index]!;
        row.bytes(source.rowId);
        row.bool(false);
        row.bytes(
          createRecord(descriptor, [
            source.rowId,
            source.name === null
              ? encodeNativeNullValue(descriptor[1]!.valueType)
              : presentBytes(inlineScalar(source.name)),
            encodeNativeNullValue(descriptor[2]!.valueType),
            encodeNativeNullValue(descriptor[3]!.valueType),
            encodedAuthorFixture(),
            u64Bytes(123),
            encodedAuthorFixture(),
            u64Bytes(123),
          ]),
        );
      }, rows.length);
    },
    rows.length === 0 ? 0 : 1,
  );
}

function typedOccurrenceKey(label: string): Uint8Array {
  const labelBytes = inlineScalar(label);
  const key = new Uint8Array(1 + 16 + 4 + 16 + 4 + 4 + 4 + labelBytes.length);
  key[0] = 1;
  key.fill(1, 1, 17);
  new DataView(key.buffer).setUint32(17, 1);
  key.fill(2, 21, 37);
  new DataView(key.buffer).setUint32(37, 1);
  new DataView(key.buffer).setUint32(41, 0);
  new DataView(key.buffer).setUint32(45, labelBytes.length);
  key.set(labelBytes, 49);
  return key;
}

function presentBytes(bytes: Uint8Array): Uint8Array {
  const output = new Uint8Array(bytes.length + 1);
  output[0] = 1;
  output.set(bytes, 1);
  return output;
}

function inlineScalar(value: string): Uint8Array {
  return Uint8Array.from([2, ...new TextEncoder().encode(value)]);
}

function encodeArrayRows(): Uint8Array {
  const descriptor = [
    { name: "chunk_refs", valueType: { tag: 14, inner: { tag: 11 } } },
    { name: "chunk_sizes", valueType: { tag: 14, inner: { tag: 6 } } },
  ];
  const writer = new PostcardWriter();
  writer.vec((batch) => {
    batch.string("arrays");
    writeNativeRowDescriptor(batch, physicalNativeDescriptor(descriptor));
    batch.vec((row) => {
      row.bytes(uuidBytes("00000000-0000-0000-0000-000000000010"));
      row.bool(false);
      row.bytes(
        createRecord(descriptor, [
          concatBytes([
            uuidBytes("00000000-0000-0000-0000-000000000001"),
            uuidBytes("00000000-0000-0000-0000-000000000002"),
          ]),
          concatBytes([doubleBytes(65536), doubleBytes(1234)]),
        ]),
      );
    }, 1);
  }, 1);
  return writer.finish();
}

function fakeDb<T extends object>(db: T): T & NativeDbForTest {
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
    // A real native binding always advertises the wire features compiled into
    // it. Individual tests can still explicitly set this to `undefined` when
    // exercising the missing-binding diagnostic.
    wireFeatures: () => CLIENT_WIRE_FEATURES,
    setTickScheduler: () => undefined,
    onMutationError: () => undefined,
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
  return result as T & NativeDbForTest;
}

function fakeWrite() {
  return {
    txId: "00000000000070008000000000000001",
    payload: new Uint8Array(0),
    wait: async () => undefined,
    writeState: () => ({}),
  };
}

function uuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  const bytes = new Uint8Array(16);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

function formatUuidForTest(bytes: Uint8Array): string {
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function doubleBytes(value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setFloat64(0, value, true);
  return bytes;
}

function txTimeBytes(value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value) << 18n, true);
  return bytes;
}

function u64Bytes(value: number): Uint8Array {
  const bytes = new Uint8Array(8);
  new DataView(bytes.buffer).setBigUint64(0, BigInt(value), true);
  return bytes;
}

function concatBytes(chunks: Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((sum, chunk) => sum + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

it("retains deferred subscription failure until execute installs its callback", async () => {
  const failure = new Error("planted subscription failure");
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          subscribe: () => ({
            poll: () => {
              throw failure;
            },
            cancel: () => {},
            setWake: () => {},
          }),
          tick: () => undefined,
        } as never),
      openBrowser: async () => {
        throw new Error("unused");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
  await new Promise((resolve) => setTimeout(resolve, 10));
  const callback = vi.fn();
  runtime.executeSubscription(handle, callback);
  await new Promise((resolve) => setTimeout(resolve, 10));
  expect(callback).toHaveBeenCalledWith(failure);
});

it.each([null, undefined])(
  "wakes a pending subscription while an async transport tick waits on its owner (pending %s)",
  async (pendingResult) => {
    let polls = 0;
    let wake = () => {};
    let releaseOwner!: () => void;
    const ownerReleased = new Promise<void>((resolve) => {
      releaseOwner = resolve;
    });
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: () =>
          fakeDb({
            subscribe: () => ({
              poll: () => {
                polls++;
                if (polls === 1) {
                  queueMicrotask(() => wake());
                  return pendingResult;
                }
                releaseOwner();
                return new ReadableStream();
              },
              cancel: () => {},
              setWake: (callback: () => void) => {
                wake = callback;
              },
            }),
            tick: () => ownerReleased,
          } as never),
        openBrowser: async () => {
          throw new Error("unused");
        },
      } as never,
      testSchema,
      new Uint8Array(16),
      TEST_RUNTIME_AUTHOR,
      1,
      true,
    );
    const inner = runtime as unknown as Record<string, any>;
    inner.serverTransport = { recvWireFrames: () => [], close: () => {} };
    inner.serverCarrier = { send: () => {}, close: () => {} };
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
    try {
      await new Promise((resolve) => setTimeout(resolve, 30));
      expect(polls).toBeGreaterThan(1);
    } finally {
      releaseOwner();
      runtime.unsubscribe(handle);
      inner.closed = true;
    }
  },
);

it("cancels a wake-driven subscription before shutdown waits for its blocked tick", async () => {
  let cancellations = 0;
  let releaseOwner!: () => void;
  const ownerReleased = new Promise<void>((resolve) => {
    releaseOwner = resolve;
  });
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          subscribe: () => ({
            poll: () => null,
            setWake: () => {},
            cancel: () => {
              cancellations += 1;
              releaseOwner();
            },
          }),
          tick: () => ownerReleased,
        } as never),
      openBrowser: async () => {
        throw new Error("unused");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  const inner = runtime as unknown as Record<string, any>;
  inner.serverTransport = { recvWireFrames: () => [], close: () => {} };
  inner.serverCarrier = { send: () => {}, close: () => {} };
  runtime.createSubscription(JSON.stringify({ table: "todos" }));
  await runtime.close();
  expect(cancellations).toBeGreaterThan(0);
});

it("isolates throwing callbacks when replaying a deferred subscription failure", async () => {
  const failure = new Error("subscription rejected");
  const callbackFailure = new Error("user callback rejected");
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          subscribe: () => ({
            poll: () => {
              throw failure;
            },
            setWake: () => {},
            cancel: () => {},
          }),
          tick: () => undefined,
        } as never),
      openBrowser: async () => {
        throw new Error("unused");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }));
  await new Promise<void>((resolve) => queueMicrotask(resolve));
  await new Promise<void>((resolve) => queueMicrotask(resolve));
  const scheduled: Array<() => void> = [];
  const timer = vi.spyOn(globalThis, "setTimeout").mockImplementation((callback) => {
    scheduled.push(callback as () => void);
    return 0 as never;
  });
  try {
    expect(() =>
      runtime.executeSubscription(handle, () => {
        throw callbackFailure;
      }),
    ).not.toThrow();
    expect(scheduled).toHaveLength(1);
    expect(scheduled[0]).toThrow(callbackFailure);
  } finally {
    timer.mockRestore();
    runtime.unsubscribe(handle);
    await runtime.close();
  }
});

it("passes different claims independently on same-query reads", async () => {
  const admitted: unknown[] = [];
  const setClaims = vi.fn();
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: (
            _query: Uint8Array,
            _opts: unknown,
            _transaction: unknown,
            identity: Uint8Array,
            claims: unknown,
          ) => {
            const admission = {
              identity: new Uint8Array(identity),
              claims: structuredClone(claims),
            };
            admitted.push(admission);
            return encodeRows([]);
          },
          setIdentityClaims: setClaims,
          tick: () => undefined,
        } as never),
      openBrowser: async () => {
        throw new Error("unused");
      },
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
    { readAuthorizationHost: "trusted-serving" },
  );
  const session = {
    issuer: "https://issuer.example",
    user_id: "same-query-reader",
    claims: { team: "team-a" },
    authMode: "external",
  };
  await runtime.query(JSON.stringify({ table: "todos" }), JSON.stringify(session));
  await runtime.query(
    JSON.stringify({ table: "todos" }),
    JSON.stringify({ ...session, claims: { team: "team-b" } }),
  );
  expect(admitted).toHaveLength(2);
  expect((admitted[0] as { claims: unknown }).claims).toMatchObject({ team: "team-a" });
  expect((admitted[1] as { claims: unknown }).claims).toMatchObject({ team: "team-b" });
  expect(setClaims).not.toHaveBeenCalled();
  await runtime.close();
});
