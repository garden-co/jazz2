import { schema as s } from "../index.js";
import { translateQuery } from "./query-adapter.js";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { execFile } from "node:child_process";
import { WebSocket } from "undici";
import { afterEach, describe, expect, it } from "vitest";
import type { SubscriptionEvent as NapiSubscriptionEvent } from "jazz-napi";
import type { ColumnType, Value, WasmSchema } from "../drivers/types.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "../testing/index.js";
import { FEATURE_PAYLOAD_ZSTD, webSocketUrl } from "./native-runtime/websocket.js";
import { openConfig, queryFromTable } from "./native-runtime/native-codec.js";
import { NativeRuntimeAdapter } from "./native-runtime/native-runtime-adapter.js";
import { encodeSchema } from "./native-runtime/native-runtime-adapter.js";
import { hasJazzNapiBuild, loadNapiModule } from "./testing/napi-runtime-test-utils.js";
import { testAccountId, testAuthorBytes } from "./testing/account-fixtures.js";
import { SubscriptionManager } from "./subscription-manager.js";
import type { WasmRow } from "../drivers/types.js";
import {
  createOpenTransactionId,
  type TxId,
  type OpenTransactionId,
  type WriteReceipt,
} from "./client.js";

const require = createRequire(import.meta.url);
const execFileAsync = promisify(execFile);
const here = dirname(fileURLToPath(import.meta.url));
const TEST_EXTERNAL_ISSUER = "https://issuer.example";

function testExternalAccountId(userId: string): string {
  return testAccountId(JSON.stringify([TEST_EXTERNAL_ISSUER, userId]));
}

function testExternalSession(userId: string, claims: Record<string, unknown> = {}) {
  return {
    issuer: TEST_EXTERNAL_ISSUER,
    user_id: userId,
    account_id: testExternalAccountId(userId),
    claims,
  };
}

function testExternalSessionJson(userId: string, claims: Record<string, unknown> = {}): string {
  return JSON.stringify(testExternalSession(userId, claims));
}

function testExternalAuthorBytes(userId: string): Uint8Array {
  const session = testExternalSession(userId);
  return new TextEncoder().encode(
    JSON.stringify([session.account_id, session.issuer, session.user_id]),
  );
}

const debugSubscriptionEventFixture = hasJazzNapiBuild()
  ? (
      require("jazz-napi") as typeof import("jazz-napi") & {
        __testSubscriptionEvents?: () => NapiSubscriptionEvent[];
      }
    ).__testSubscriptionEvents
  : undefined;

async function runNapiFixture(name: string, args: string[] = []) {
  return await execFileAsync(process.execPath, [join(here, "__fixtures__", name), ...args], {
    encoding: "utf8",
    timeout: 9_000,
  });
}

function beginTestBatch(runtime: NativeRuntimeAdapter): OpenTransactionId {
  const id = createOpenTransactionId();
  runtime.beginTransaction("mergeable", id);
  return id;
}

async function committedTxId(receipt: WriteReceipt): Promise<TxId> {
  if (receipt.kind !== "committed") throw new Error("expected committed write receipt");
  return await receipt.txId;
}

function expectStaged(receipt: WriteReceipt, openTransactionId: OpenTransactionId): void {
  expect(receipt).toMatchObject({ kind: "staged", openTransactionId });
}

const TEST_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const DEFAULTS_SCHEMA: WasmSchema = {
  counters: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      {
        name: "largeCount",
        column_type: { type: "BigInt" },
        nullable: false,
        default: { type: "BigInt", value: 9007199254740993n },
      },
    ],
  },
};

const BYTEA_SCHEMA: WasmSchema = {
  blobs: {
    columns: [{ name: "data", column_type: { type: "Bytea" }, nullable: false }],
  },
};

it("accepts only canonical authors through the NAPI open-config codec", async () => {
  const { NapiDb } = await loadNapiModule();
  const canonical = new TextEncoder().encode('["https://issuer.example","canonical-author"]');
  const db = NapiDb.openMemory(
    encodeSchema(TEST_SCHEMA),
    openConfig(deterministicBytes("napi-open-config:node"), canonical, 1, true),
  );
  db.close?.();
  expect(() =>
    NapiDb.openMemory(
      encodeSchema(TEST_SCHEMA),
      openConfig(
        deterministicBytes("napi-open-config:legacy-node"),
        deterministicBytes("napi-open-config:legacy-author"),
        1,
        true,
      ),
    ),
  ).toThrow(/canonical UTF-8 author identity/i);
});

it("ships a zstd-capable NAPI receiver and rejects an uncompiled negotiated feature before admission", async () => {
  const { NapiDb } = await loadNapiModule();
  const db = NapiDb.openMemory(
    encodeSchema(TEST_SCHEMA),
    openConfig(
      deterministicBytes("napi-wire-capability:node"),
      testAuthorBytes("napi-wire-capability:author"),
      1,
      true,
    ),
  );
  try {
    const features = db.wireFeatures();
    expect(features & FEATURE_PAYLOAD_ZSTD).toBe(FEATURE_PAYLOAD_ZSTD);
    expect(() =>
      db.connectUpstreamWithSession(
        1,
        features | (1 << 30),
        Buffer.from(deterministicBytes("napi-wire-capability:remote")),
        1n,
        Buffer.from(deterministicBytes("napi-wire-capability:local")),
        1n,
      ),
    ).toThrow(/native binding was not compiled with 0x40000000/);
  } finally {
    await db.close();
  }
});

const SIGNED_DEFAULT_CASES: Array<{
  name: string;
  columnType: ColumnType;
  value: Value;
}> = [
  {
    name: "i32 minimum",
    columnType: { type: "Integer" },
    value: { type: "Integer", value: -2_147_483_648 },
  },
  {
    name: "i32 negative one",
    columnType: { type: "Integer" },
    value: { type: "Integer", value: -1 },
  },
  { name: "i32 zero", columnType: { type: "Integer" }, value: { type: "Integer", value: 0 } },
  { name: "i32 one", columnType: { type: "Integer" }, value: { type: "Integer", value: 1 } },
  {
    name: "i32 maximum",
    columnType: { type: "Integer" },
    value: { type: "Integer", value: 2_147_483_647 },
  },
  {
    name: "i64 minimum",
    columnType: { type: "BigInt" },
    value: { type: "BigInt", value: -(1n << 63n) },
  },
  {
    name: "i64 negative one",
    columnType: { type: "BigInt" },
    value: { type: "BigInt", value: -1n },
  },
  { name: "i64 zero", columnType: { type: "BigInt" }, value: { type: "BigInt", value: 0n } },
  { name: "i64 one", columnType: { type: "BigInt" }, value: { type: "BigInt", value: 1n } },
  {
    name: "i64 maximum",
    columnType: { type: "BigInt" },
    value: { type: "BigInt", value: (1n << 63n) - 1n },
  },
];

const ALICE_ID = "00000000-0000-4000-8000-0000000000a1";
const BOB_ID = "00000000-0000-4000-8000-0000000000b2";

const OWNED_TODOS_SCHEMA: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "done", column_type: { type: "Boolean" }, nullable: false },
      { name: "owner_id", column_type: { type: "Text" }, nullable: false },
    ],
    policies: {
      select: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["claims", "sub"] },
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["claims", "sub"] },
        },
      },
      update: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["claims", "sub"] },
        },
      },
      delete: {
        using: {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["claims", "sub"] },
        },
      },
    },
  },
};

const CHAT_POLICY_SCHEMA: WasmSchema = {
  chats: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "visibility", column_type: { type: "Text" }, nullable: false },
      { name: "owner_id", column_type: { type: "Text" }, nullable: false },
    ],
    policies: {
      select: {
        using: {
          type: "Or",
          exprs: [
            {
              type: "Cmp",
              column: "visibility",
              op: "Eq",
              value: { type: "Literal", value: { type: "Text", value: "public" } },
            },
            {
              type: "Exists",
              table: "chat_members",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "chat_id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["__jazz_outer_row", "id"] },
                  },
                  {
                    type: "Cmp",
                    column: "user_id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["claims", "sub"] },
                  },
                ],
              },
            },
          ],
        },
      },
      insert: { with_check: { type: "True" } },
      update: { using: { type: "True" } },
      delete: { using: { type: "True" } },
    },
  },
  chat_members: {
    columns: [
      {
        name: "chat_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "chats",
      },
      { name: "user_id", column_type: { type: "Text" }, nullable: false },
    ],
    policies: {
      select: {
        using: {
          type: "Cmp",
          column: "user_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["claims", "sub"] },
        },
      },
      insert: {
        with_check: {
          type: "Cmp",
          column: "user_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["claims", "sub"] },
        },
      },
      update: { using: { type: "True" } },
      delete: {
        using: {
          type: "Cmp",
          column: "user_id",
          op: "Eq",
          value: { type: "SessionRef", path: ["claims", "sub"] },
        },
      },
    },
  },
  messages: {
    columns: [
      {
        name: "chat_id",
        column_type: { type: "Uuid" },
        nullable: false,
        references: "chats",
      },
      { name: "text", column_type: { type: "Text" }, nullable: false },
    ],
    policies: {
      select: {
        using: {
          type: "Or",
          exprs: [
            {
              type: "Exists",
              table: "chats",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["__jazz_outer_row", "chat_id"] },
                  },
                  {
                    type: "Cmp",
                    column: "visibility",
                    op: "Eq",
                    value: { type: "Literal", value: { type: "Text", value: "public" } },
                  },
                ],
              },
            },
            {
              type: "Exists",
              table: "chat_members",
              condition: {
                type: "And",
                exprs: [
                  {
                    type: "Cmp",
                    column: "chat_id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["__jazz_outer_row", "chat_id"] },
                  },
                  {
                    type: "Cmp",
                    column: "user_id",
                    op: "Eq",
                    value: { type: "SessionRef", path: ["claims", "sub"] },
                  },
                ],
              },
            },
          ],
        },
      },
      insert: { with_check: { type: "True" } },
      update: { using: { type: "True" } },
      delete: { using: { type: "True" } },
    },
  },
};

describe.skipIf(!hasJazzNapiBuild())("jazz-napi native runtime memory DB", () => {
  let server: LocalJazzServerHandle | null = null;
  const runtimes: NativeRuntimeAdapter[] = [];
  const previousWebSocket = globalThis.WebSocket;

  afterEach(async () => {
    for (const runtime of runtimes.splice(0)) {
      await runtime.close();
    }
    await server?.stop();
    server = null;
    globalThis.WebSocket = previousWebSocket;
  });

  it("rejects a removed upsert branch option by JavaScript property presence", async () => {
    const { NapiDb } = await loadNapiModule();
    const db = NapiDb.openMemory(
      encodeSchema(TEST_SCHEMA),
      openConfig(
        deterministicBytes("napi-upsert-removed-branch:node"),
        testAuthorBytes("napi-upsert-removed-branch:author"),
        1,
        true,
      ),
    );
    const upsert = (options: object) =>
      // The rejected options must win over deliberately invalid row bytes: this
      // exercises the raw native JavaScript boundary, not only the Rust parser.
      (
        db as unknown as {
          upsert(table: string, rowId: Uint8Array, cells: Uint8Array, options: object): unknown;
        }
      ).upsert("todos", new Uint8Array(16), new Uint8Array(), options);

    const removed = /option `branch` is not supported; use `head`/;
    expect(() => upsert({ branch: undefined })).toThrow(removed);
    expect(() => upsert({ branch: null })).toThrow(removed);
    expect(() => upsert({ branch: undefined, head: { branch: "draft" } })).toThrow(removed);

    const inherited = Object.create({ branch: undefined });
    expect(() => upsert(inherited)).toThrow(removed);

    const getter = Object.defineProperty({}, "branch", {
      get() {
        throw new Error("removed branch getter must never be evaluated");
      },
    });
    expect(() => upsert(getter)).toThrow(removed);

    const proxied = new Proxy({}, { has: (_target, key) => key === "branch" });
    expect(() => upsert(proxied)).toThrow(removed);
    db.close?.();
  });

  it("selects and filters public provenance authors as structured records", async () => {
    const { NapiDb } = await loadNapiModule();
    const authorSeed = "jazz-napi-public-provenance:author";
    const author = {
      account: testAccountId(authorSeed),
      identity: { issuer: "urn:jazz:test", subject: authorSeed },
    };
    const provenanceApp = s.defineApp({ todos: s.table({ title: s.string(), done: s.boolean() }) });
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-public-provenance:node"),
      testAuthorBytes(authorSeed),
      1,
      true,
    );
    runtimes.push(runtime);
    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "created by canonical author" },
      done: { type: "Boolean", value: false },
    });
    await runtime.waitForTransaction(await committedTxId(inserted), "local");

    await expect(
      runtime.query(
        translateQuery(
          provenanceApp.todos
            .select("title", "$createdBy", "$updatedBy")
            .where({ $createdBy: author })
            ._build(),
          TEST_SCHEMA,
        ),
        null,
        "local",
      ),
    ).resolves.toEqual([
      {
        table: "todos",
        id: inserted.id,
        values: [
          { type: "Text", value: "created by canonical author" },
          expect.objectContaining({
            type: "Row",
            value: expect.objectContaining({
              values: [
                { type: "Uuid", value: author.account },
                expect.objectContaining({
                  type: "Row",
                  value: expect.objectContaining({
                    values: [
                      { type: "Text", value: author.identity.issuer },
                      { type: "Text", value: author.identity.subject },
                    ],
                  }),
                }),
              ],
            }),
          }),
          expect.objectContaining({
            type: "Row",
            value: expect.objectContaining({
              values: [
                { type: "Uuid", value: author.account },
                expect.objectContaining({
                  type: "Row",
                  value: expect.objectContaining({
                    values: [
                      { type: "Text", value: author.identity.issuer },
                      { type: "Text", value: author.identity.subject },
                    ],
                  }),
                }),
              ],
            }),
          }),
        ],
      },
    ]);
  });

  it("compiles the policy graph perf source fixture through NAPI", async () => {
    const { NapiDb } = await loadNapiModule();
    const source = JSON.parse(
      readFileSync(
        new URL("../testing/fixtures/policy-graph-perf/schema-source.json", import.meta.url),
        "utf8",
      ),
    ) as { mergedSchema: WasmSchema };
    const schema = encodeSchema(source.mergedSchema);
    const db = NapiDb.openMemory(
      schema,
      openConfig(
        deterministicBytes("jazz-napi-native-runtime:policy-graph-perf-node"),
        testAuthorBytes("jazz-napi-native-runtime:policy-graph-perf-author"),
        1,
        true,
      ),
    );

    db.close?.();
  }, 20_000);

  it("releases N-API callbacks when the owner closes with a schema view retained", async () => {
    const schema = Buffer.from(encodeSchema(TEST_SCHEMA)).toString("base64");
    const config = Buffer.from(
      openConfig(
        deterministicBytes("jazz-napi-native-runtime:retained-view-node"),
        testAuthorBytes("jazz-napi-native-runtime:retained-view-author"),
        1,
        true,
      ),
    ).toString("base64");

    for (const storage of ["memory", "persistent"]) {
      const result = await runNapiFixture("napi-close-retained-schema-view.mjs", [
        storage,
        schema,
        config,
      ]);

      expect(result.stderr).toBe("");
      expect(result.stdout).toBe(`owner closed with ${storage} schema view retained\n`);
    }
  }, 10_000);

  it("emits core tick scheduler wakes through the NAPI bridge", async () => {
    const { NapiDb } = await loadNapiModule();
    const wakes: string[] = [];
    const db = NapiDb.openMemory(
      encodeSchema(TEST_SCHEMA),
      openConfig(
        deterministicBytes("jazz-napi-native-runtime:scheduler-node"),
        testAuthorBytes("jazz-napi-native-runtime:scheduler-author"),
        1,
        true,
      ),
    );

    db.setTickScheduler((error: Error | null, urgency: string) => {
      if (error) throw error;
      wakes.push(urgency);
    });
    const transport = db.connectUpstream();

    await waitFor(
      async () => (wakes.length > 0 ? wakes : undefined),
      "NAPI tick scheduler did not emit a wake",
    );

    expect(wakes).toContain("immediate");
    expect(transport.close()).toBe(true);
    db.close?.();
  });

  it("delivers mutation errors as exactly one event argument through NAPI", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d005";
    const node = deterministicBytes("jazz-napi-mutation-error-shape:node");
    const author = testAuthorBytes("jazz-napi-mutation-error-shape:author");
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      backendSecret: "core-napi-mutation-error-shape-backend",
      adminSecret: "core-napi-mutation-error-shape-admin",
      schema: encodeSchema(OWNED_TODOS_SCHEMA),
    });

    const nativeDb = NapiDb.openMemory(
      encodeSchema(OWNED_TODOS_SCHEMA),
      openConfig(node, author, 1, true),
    );
    let callbackArgs: unknown[] | undefined;
    nativeDb.onMutationError((...args: unknown[]) => {
      callbackArgs = args;
    });
    const runtime = NativeRuntimeAdapter.fromDb(
      nativeDb as never,
      OWNED_TODOS_SCHEMA,
      node,
      author,
      1,
      true,
    );
    runtimes.push(runtime);
    runtime.connect(
      webSocketUrl(server.url, appId),
      JSON.stringify({ backend_secret: server.backendSecret }),
    );

    const aliceSession = JSON.stringify({
      issuer: "https://issuer.example",
      user_id: ALICE_ID,
    });
    const inserted = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "rejected mutation callback row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    const transactionId = await committedTxId(inserted);

    const args = await waitFor(
      async () => callbackArgs,
      "NAPI mutation error callback did not receive the rejected write",
      10_000,
    );
    expect(args).toHaveLength(1);
    expect(args[0]).toMatchObject({
      code: "permission_denied",
      reason: "Write rejected by server authorization",
      transaction: { transactionId },
    });
  }, 15_000);

  it("admits concurrent reads and cancels a subscription before callback admission", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-concurrent-query-admission:node"),
      testAuthorBytes("jazz-napi-concurrent-query-admission:alice"),
      1,
      true,
    );
    runtimes.push(runtime);
    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "concurrent admission" },
      done: { type: "Boolean", value: false },
    });
    await runtime.waitForTransaction(await committedTxId(inserted), "local");
    const query = JSON.stringify({ table: "todos" });
    let cancelledCallbacks = 0;
    const cancelled = runtime.createSubscription(query);
    runtime.unsubscribe(cancelled);
    runtime.executeSubscription(cancelled, () => {
      cancelledCallbacks += 1;
    });
    const reads = await Promise.all(Array.from({ length: 12 }, () => runtime.query(query)));
    for (const rows of reads) expect(rows).toEqual([expect.objectContaining({ id: inserted.id })]);
    expect(cancelledCallbacks).toBe(0);
    const active = runtime.createSubscription(query);
    const opening = new Promise<unknown>((resolve) => runtime.executeSubscription(active, resolve));
    await opening;
    runtime.unsubscribe(active);
    await expect(runtime.query(query)).resolves.toEqual([
      expect.objectContaining({ id: inserted.id }),
    ]);
  });

  it("opens, mutates one row, and queries it through the native runtime payload shape", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime:node"),
      testAuthorBytes("jazz-napi-native-runtime:author"),
      1,
      true,
    );

    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "direct napi memory row" },
      done: { type: "Boolean", value: false },
    });

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "direct napi memory row" },
          { type: "Boolean", value: false },
        ],
      },
    ]);

    runtime.update("todos", inserted.id, {
      title: { type: "Text", value: "direct napi updated row" },
    });

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "direct napi updated row" },
          { type: "Boolean", value: false },
        ],
      },
    ]);

    runtime.delete("todos", inserted.id);

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
  });

  it("streams a typed text column through NAPI before publishing the row", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-streaming-insert:node"),
      testAuthorBytes("jazz-napi-streaming-insert:author"),
      1,
      true,
      { readAuthorizationHost: "trusted-serving" },
    );
    runtimes.push(runtime);

    const inserted = await runtime.streamingMutation(
      "insert",
      "todos",
      { done: { type: "Boolean", value: false } },
      "title",
      (async function* () {
        yield "streamed ";
        yield new TextEncoder().encode("through NAPI ");
        yield "\ud83d";
        yield "\ude80";
      })(),
      null,
      "00000000-0000-4000-8000-000000000123",
    );

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "streamed through NAPI 🚀" },
          { type: "Boolean", value: false },
        ],
      },
    ]);

    await runtime.streamingMutation(
      "update",
      "todos",
      { done: { type: "Boolean", value: true } },
      "title",
      (async function* () {
        yield "streamed update";
      })(),
      JSON.stringify({
        session: testExternalSession(ALICE_ID, { role: "editor" }),
        updated_at: 42_000,
      }),
      inserted.id,
    );
    await runtime.streamingMutation(
      "upsert",
      "todos",
      {},
      "title",
      (async function* () {
        yield "streamed upsert";
      })(),
      null,
      inserted.id,
    );

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "streamed upsert" },
          { type: "Boolean", value: true },
        ],
      },
    ]);
  });

  it("allocates clock-backed ordered UUIDv7 ids in Rust for ordinary and staged inserts", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime:uuidv7-node"),
      testAuthorBytes("jazz-napi-native-runtime:uuidv7-author"),
      1,
      true,
    );
    runtimes.push(runtime);

    const insert = (title: string, writeContext?: string) =>
      runtime.insert(
        "todos",
        {
          title: { type: "Text", value: title },
          done: { type: "Boolean", value: false },
        },
        writeContext,
      );

    const first = insert("first");
    const second = insert("second");
    const openTransactionId = beginTestBatch(runtime);
    const writeContext = JSON.stringify({ transaction_id: openTransactionId });
    const third = insert("third", writeContext);
    const fourth = insert("fourth", writeContext);

    for (const row of [first, second, third, fourth]) {
      expect(row.id).toMatch(
        /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
      );
    }

    const generatedAtMs = Number.parseInt(first.id.replaceAll("-", "").slice(0, 12), 16);
    expect(Math.abs(Date.now() - generatedAtMs)).toBeLessThan(60_000);
    const ids = [first.id, second.id, third.id, fourth.id];
    expect(ids).toEqual([...ids].sort());

    await runtime.commitTransaction(openTransactionId);
    const rows = (await runtime.query(JSON.stringify({ table: "todos" }))) as Array<{
      id: string;
    }>;
    expect(rows.map((row) => row.id)).toEqual([first.id, second.id, third.id, fourth.id]);
  });

  it("applies column defaults for direct napi inserts with omitted cells", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      DEFAULTS_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-defaults:node"),
      testAuthorBytes("jazz-napi-native-runtime-defaults:author"),
      1,
      true,
    );

    const inserted = runtime.insert("counters", {
      title: { type: "Text", value: "direct napi default row" },
    });

    await expect(runtime.query(JSON.stringify({ table: "counters" }))).resolves.toEqual([
      {
        id: inserted.id,
        table: "counters",
        values: [
          { type: "Text", value: "direct napi default row" },
          { type: "BigInt", value: 9007199254740993n },
        ],
      },
    ]);

    runtime.close();
  });

  it.each(SIGNED_DEFAULT_CASES)(
    "round-trips the $name schema default through a direct napi insert",
    async ({ columnType, value }) => {
      const { NapiDb } = await loadNapiModule();
      const runtime = new NativeRuntimeAdapter(
        { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
        {
          signed_defaults: {
            columns: [
              { name: "title", column_type: { type: "Text" }, nullable: false },
              { name: "value", column_type: columnType, nullable: false, default: value },
            ],
          },
        },
        deterministicBytes("jazz-napi-native-runtime-signed-defaults:node"),
        testAuthorBytes("jazz-napi-native-runtime-signed-defaults:author"),
        1,
        true,
      );

      const inserted = runtime.insert("signed_defaults", {
        title: { type: "Text", value: "direct napi signed default row" },
      });

      await expect(runtime.query(JSON.stringify({ table: "signed_defaults" }))).resolves.toEqual([
        {
          id: inserted.id,
          table: "signed_defaults",
          values: [{ type: "Text", value: "direct napi signed default row" }, value],
        },
      ]);

      runtime.close();
    },
  );

  it("delivers native NAPI subscription updates through the native handle", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-subscription:node"),
      testAuthorBytes("jazz-napi-native-runtime-subscription:author"),
      21,
      true,
    );
    runtimes.push(runtime);

    const manager = new SubscriptionManager<WasmRow>();
    const updates: ReturnType<SubscriptionManager<WasmRow>["handleDelta"]>[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "local");
    runtime.executeSubscription(handle, (delta: unknown) => {
      if (delta instanceof Error) throw delta;
      updates.push(
        manager.handleDelta(
          delta as Parameters<SubscriptionManager<WasmRow>["handleDelta"]>[0],
          (row) => row,
        ),
      );
    });

    expect(updates).toEqual([{ all: [], delta: [], reset: true }]);

    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "direct napi subscribed row" },
      done: { type: "Boolean", value: false },
    });

    expect(updates).toHaveLength(2);
    expect(updates[1]?.delta).toEqual([
      {
        kind: 0,
        id: inserted.id,
        index: 0,
        item: {
          id: inserted.id,
          values: [
            { type: "Text", value: "direct napi subscribed row" },
            { type: "Boolean", value: false },
          ],
        },
      },
    ]);

    runtime.update("todos", inserted.id, {
      title: { type: "Text", value: "direct napi subscribed updated row" },
    });

    expect(updates).toHaveLength(3);
    expect(updates[2]?.delta).toEqual([
      {
        kind: 2,
        id: inserted.id,
        index: 0,
        item: {
          id: inserted.id,
          values: [
            { type: "Text", value: "direct napi subscribed updated row" },
            { type: "Boolean", value: false },
          ],
        },
      },
    ]);

    runtime.unsubscribe(handle);
  });

  it("publishes a truthful local empty opening while full propagation continues upstream", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-provisional-empty:node"),
      testAuthorBytes("jazz-napi-native-runtime-provisional-empty:author"),
      25,
      true,
    );
    runtimes.push(runtime);

    const defaultFullUpdates: unknown[] = [];
    const defaultFull = runtime.createSubscription(
      JSON.stringify({ table: "todos" }),
      null,
      "local",
    );
    runtime.executeSubscription(defaultFull, (delta: unknown) => defaultFullUpdates.push(delta));
    expect(defaultFullUpdates).toHaveLength(1);

    const localOnlyUpdates: unknown[] = [];
    const localOnly = runtime.createSubscription(
      JSON.stringify({ table: "todos" }),
      null,
      "local",
      JSON.stringify({ propagation: "local-only" }),
    );
    runtime.executeSubscription(localOnly, (delta: unknown) => localOnlyUpdates.push(delta));
    expect(localOnlyUpdates).toHaveLength(1);

    runtime.unsubscribe(defaultFull);
    runtime.unsubscribe(localOnly);
  });

  it("returns raw NAPI subscription payloads without duplicate root terminal edits", async () => {
    const { NapiDb } = await loadNapiModule();
    const node = deterministicBytes("jazz-napi-native-runtime-raw-subscription:node");
    const author = testAuthorBytes("jazz-napi-native-runtime-raw-subscription:author");
    const rawEvents: NapiSubscriptionEvent[] = [];
    const expectRawBinaryPayload = (event: (typeof rawEvents)[number] | undefined) => {
      expect(event).toBeDefined();
      if (!event) throw new Error("expected a raw subscription event");
      expect(event.type).toBe("delta");
      if (event.type !== "delta") throw new Error(`expected a delta event, received ${event.type}`);
      expect(event.delta).toBeInstanceOf(Uint8Array);
      expect(Array.isArray(event.delta)).toBe(false);
      expect(Buffer.isBuffer(event.delta)).toBe(false);
      expect((event.delta as Uint8Array).byteLength).toBeGreaterThan(0);
      expect(Array.isArray(event.terminalOperations)).toBe(true);
      return event;
    };

    const observeSource = (source: object) =>
      new Proxy(source, {
        get(sourceTarget, sourceProperty) {
          const sourceValue = Reflect.get(sourceTarget, sourceProperty, sourceTarget) as unknown;
          if (sourceProperty === "readAll" && typeof sourceValue === "function") {
            return () => {
              const events = Reflect.apply(
                sourceValue,
                sourceTarget,
                [],
              ) as NapiSubscriptionEvent[];
              rawEvents.push(...events);
              return events;
            };
          }
          return typeof sourceValue === "function" ? sourceValue.bind(sourceTarget) : sourceValue;
        },
      });
    const observePending = (pending: object) =>
      new Proxy(pending, {
        get(target, property) {
          const value = Reflect.get(target, property, target) as unknown;
          if (property === "poll" && typeof value === "function") {
            return () => {
              const source = Reflect.apply(value, target, []) as object | null | undefined;
              return source == null ? source : observeSource(source);
            };
          }
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    const observeDb = (nativeDb: ReturnType<typeof NapiDb.openMemory>) =>
      new Proxy(nativeDb, {
        get(target, property) {
          const value = Reflect.get(target, property, target) as unknown;
          if (property === "subscribe" && typeof value === "function") {
            return (...args: unknown[]) => {
              const source = Reflect.apply(value, target, args) as object;
              return "setWake" in source ? observePending(source) : observeSource(source);
            };
          }
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    const runtime = new NativeRuntimeAdapter(
      {
        openMemory: (schema, config) => observeDb(NapiDb.openMemory(schema, config)) as never,
      },
      BYTEA_SCHEMA,
      node,
      author,
      23,
      true,
    );
    runtimes.push(runtime);

    const manager = new SubscriptionManager<WasmRow>();
    const updates: ReturnType<SubscriptionManager<WasmRow>["handleDelta"]>[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "blobs" }), null, "local");
    runtime.executeSubscription(handle, (delta: unknown) => {
      if (delta instanceof Error) throw delta;
      updates.push(
        manager.handleDelta(
          delta as Parameters<SubscriptionManager<WasmRow>["handleDelta"]>[0],
          (row) => row,
        ),
      );
    });

    const initialReset = rawEvents.find((event) => event.type === "delta" && event.reset === true);
    const rawReset = expectRawBinaryPayload(initialReset);
    expect(rawReset.terminalOperations).toEqual([]);

    const eventsBeforeInsert = rawEvents.length;
    const fullByteRange = Uint8Array.from(Array.from({ length: 256 }, (_, index) => index));
    const inserted = runtime.insert("blobs", {
      data: { type: "Bytea", value: fullByteRange },
    });
    const rawDelta = rawEvents
      .slice(eventsBeforeInsert)
      .find((event) => event.type === "delta" && event.reset === false);

    const rawIncremental = expectRawBinaryPayload(rawDelta);
    expect(rawIncremental.terminalOperations).toEqual([]);

    const delivered = updates[1]?.delta[0];
    expect(delivered?.id).toBe(inserted.id);
    if (!delivered || !("item" in delivered)) {
      throw new Error("expected a delivered row item");
    }
    const deliveredValue = delivered.item?.values[0];
    expect(deliveredValue?.type).toBe("Bytea");
    if (deliveredValue?.type !== "Bytea") throw new Error("expected a delivered Bytea value");
    expect(deliveredValue.value).toBeInstanceOf(Uint8Array);
    expect(deliveredValue.value).toEqual(fullByteRange);

    runtime.unsubscribe(handle);
  });

  // The fixture is deliberately absent from release addons. These complementary
  // tests report that profile boundary explicitly instead of silently returning.
  it.skipIf(debugSubscriptionEventFixture !== undefined)(
    "release addons omit the debug-only subscription event fixture",
    () => {
      expect(debugSubscriptionEventFixture).toBeUndefined();
    },
  );

  it.skipIf(debugSubscriptionEventFixture === undefined)(
    "debug addons normalize real Rust rejection and closed subscription events",
    async () => {
      const { NapiDb } = await loadNapiModule();
      const fixture = debugSubscriptionEventFixture!;
      const [
        unsupportedEvent,
        pendingEvent,
        serverFailureEvent,
        invalidAuthorityEvent,
        closedEvent,
      ] = fixture();
      expect([
        unsupportedEvent,
        pendingEvent,
        serverFailureEvent,
        invalidAuthorityEvent,
        closedEvent,
      ]).toStrictEqual([
        {
          type: "rejected",
          reason: {
            type: "UnsupportedShapeCapability",
            detail: "fixture unsupported shape",
          },
        },
        {
          type: "rejected",
          reason: { type: "ShapeRegistrationPendingCatalogueAdmission" },
        },
        {
          type: "rejected",
          reason: { type: "ServerFailure", code: "QueryValidation" },
        },
        {
          type: "rejected",
          reason: {
            type: "InvalidAuthoritySourceClosure",
            transition: "fixture invalid transition",
          },
        },
        { type: "closed" },
      ]);
      if (
        !unsupportedEvent ||
        !pendingEvent ||
        !serverFailureEvent ||
        !invalidAuthorityEvent ||
        !closedEvent
      ) {
        throw new Error("jazz-napi test fixture returned incomplete events");
      }

      const openHarness = (label: string) => {
        const injectedEvents: NapiSubscriptionEvent[] = [];
        const observedEvents: NapiSubscriptionEvent[] = [];
        const observeDb = (nativeDb: ReturnType<typeof NapiDb.openMemory>) =>
          new Proxy(nativeDb, {
            get(target, property) {
              const value = Reflect.get(target, property, target) as unknown;
              if (property === "subscribe" && typeof value === "function") {
                return (...args: unknown[]) => {
                  const source = Reflect.apply(value, target, args) as object;
                  return new Proxy(source, {
                    get(sourceTarget, sourceProperty) {
                      const sourceValue = Reflect.get(
                        sourceTarget,
                        sourceProperty,
                        sourceTarget,
                      ) as unknown;
                      if (sourceProperty === "readAll" && typeof sourceValue === "function") {
                        return () => {
                          const events = Reflect.apply(
                            sourceValue,
                            sourceTarget,
                            [],
                          ) as NapiSubscriptionEvent[];
                          const injected = injectedEvents.splice(0);
                          observedEvents.push(...injected, ...events);
                          return [...injected, ...events];
                        };
                      }
                      return typeof sourceValue === "function"
                        ? sourceValue.bind(sourceTarget)
                        : sourceValue;
                    },
                  });
                };
              }
              return typeof value === "function" ? value.bind(target) : value;
            },
          });
        const runtime = new NativeRuntimeAdapter(
          {
            openMemory: (schema, config) => observeDb(NapiDb.openMemory(schema, config)) as never,
          },
          TEST_SCHEMA,
          deterministicBytes(`jazz-napi-native-runtime-event-variants:${label}:node`),
          testAuthorBytes(`jazz-napi-native-runtime-event-variants:${label}:author`),
          24,
          true,
        );
        runtimes.push(runtime);
        const notifications: unknown[][] = [];
        const handle = runtime.createSubscription(
          JSON.stringify({ table: "todos" }),
          null,
          "local",
        );
        runtime.executeSubscription(handle, (...args: unknown[]) => notifications.push(args));
        expect(notifications).toHaveLength(1);
        return { runtime, injectedEvents, observedEvents, notifications };
      };

      const pending = openHarness("pending");
      pending.injectedEvents.push(pendingEvent);
      expect(pending.notifications).toHaveLength(1);
      pending.runtime.insert("todos", {
        title: { type: "Text", value: "still subscribed after pending admission" },
        done: { type: "Boolean", value: false },
      });
      expect(pending.observedEvents).toContainEqual({
        type: "rejected",
        reason: { type: "ShapeRegistrationPendingCatalogueAdmission" },
      });
      expect(pending.notifications).toHaveLength(2);

      const unsupported = openHarness("unsupported");
      unsupported.injectedEvents.push(unsupportedEvent);
      unsupported.runtime.insert("todos", {
        title: { type: "Text", value: "unsupported before delivery" },
        done: { type: "Boolean", value: false },
      });
      expect(unsupported.notifications).toHaveLength(2);
      expect(unsupported.notifications[1]?.[0]).toBeInstanceOf(Error);
      expect(String(unsupported.notifications[1]?.[0])).toContain(
        "UnsupportedShapeCapability: fixture unsupported shape",
      );
      expect(unsupported.notifications[1]?.[1]).toBeUndefined();

      const rejected = openHarness("rejected");
      rejected.injectedEvents.push(serverFailureEvent);
      rejected.runtime.insert("todos", {
        title: { type: "Text", value: "rejected before delivery" },
        done: { type: "Boolean", value: false },
      });
      expect(rejected.observedEvents).toContainEqual({
        type: "rejected",
        reason: { type: "ServerFailure", code: "QueryValidation" },
      });
      expect(rejected.notifications).toHaveLength(2);
      expect(rejected.notifications[1]?.[0]).toBeInstanceOf(Error);
      expect(String(rejected.notifications[1]?.[0])).toContain("ServerFailure: QueryValidation");
      expect(rejected.notifications[1]?.[1]).toBeUndefined();

      const closed = openHarness("closed");
      closed.injectedEvents.push(closedEvent);
      closed.runtime.insert("todos", {
        title: { type: "Text", value: "not delivered after close" },
        done: { type: "Boolean", value: false },
      });
      expect(closed.observedEvents).toContainEqual({ type: "closed" });
      expect(closed.notifications).toHaveLength(1);
    },
  );

  it("delivers a multi-write mergeable transaction as one subscription delta", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-transaction-delta:node"),
      testAuthorBytes("jazz-napi-native-runtime-transaction-delta:author"),
      22,
      true,
    );
    runtimes.push(runtime);

    const manager = new SubscriptionManager<WasmRow>();
    const updates: ReturnType<SubscriptionManager<WasmRow>["handleDelta"]>[] = [];
    const handle = runtime.createSubscription(JSON.stringify({ table: "todos" }), null, "local");
    runtime.executeSubscription(handle, (delta: unknown) => {
      updates.push(
        manager.handleDelta(
          delta as Parameters<SubscriptionManager<WasmRow>["handleDelta"]>[0],
          (row) => row,
        ),
      );
    });

    expect(updates).toEqual([{ all: [], delta: [], reset: true }]);

    const tx = beginTestBatch(runtime);
    const writeContext = JSON.stringify({ transaction_id: tx });
    const first = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "transaction first" },
        done: { type: "Boolean", value: false },
      },
      writeContext,
      "33333333-3333-4333-8333-333333333333",
    );
    const second = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "transaction second" },
        done: { type: "Boolean", value: true },
      },
      writeContext,
      "44444444-4444-4444-8444-444444444444",
    );

    expectStaged(first, tx);
    expectStaged(second, tx);
    expect(updates).toHaveLength(1);

    const txId = await runtime.commitTransaction(tx);
    await runtime.waitForTransaction(txId, "local");

    expect(updates).toHaveLength(2);
    expect(updates[1]?.reset).not.toBe(true);
    expect(updates[1]?.delta).toHaveLength(2);
    expect(updates[1]?.delta.map((change) => change.id).sort()).toEqual([first.id, second.id]);

    runtime.unsubscribe(handle);
  });

  it("stages client-local session writes without treating a session as serving authority", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-policy:node"),
      testAuthorBytes("jazz-napi-native-runtime-policy:author"),
      11,
      true,
    );
    const aliceSession = testExternalSessionJson(ALICE_ID);
    const bobSession = testExternalSessionJson(BOB_ID);

    const aliceTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "alice local row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    await runtime.waitForTransaction(await committedTxId(aliceTodo), "local");

    const aliceRows = await runtime.query(
      JSON.stringify({ table: "todos" }),
      aliceSession,
      "local",
    );
    expect(aliceRows).toHaveLength(1);
    expect(aliceRows).toEqual([
      expect.objectContaining({
        id: aliceTodo.id,
        table: "todos",
      }),
    ]);
    expect((aliceRows as Array<{ values: unknown[] }>)[0]?.values.slice(0, 3)).toEqual([
      { type: "Text", value: "alice local row" },
      { type: "Boolean", value: false },
      { type: "Text", value: ALICE_ID },
    ]);

    const foreignOwnerTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "alice locally stages bob-owned row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: BOB_ID },
      },
      aliceSession,
    );
    await runtime.waitForTransaction(await committedTxId(foreignOwnerTodo), "local");

    const aliceRowsAfterForeignOwnerInsert = await runtime.query(
      JSON.stringify({ table: "todos" }),
      aliceSession,
      "local",
    );
    expect(aliceRowsAfterForeignOwnerInsert).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: aliceTodo.id, table: "todos" }),
        expect.objectContaining({ id: foreignOwnerTodo.id, table: "todos" }),
      ]),
    );

    const bobTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "bob local row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: BOB_ID },
      },
      bobSession,
    );
    await runtime.waitForTransaction(await committedTxId(bobTodo), "local");

    const aliceRowsAfterBobInsert = await runtime.query(
      JSON.stringify({ table: "todos" }),
      aliceSession,
      "local",
    );
    expect(aliceRowsAfterBobInsert).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: aliceTodo.id, table: "todos" }),
        expect.objectContaining({ id: foreignOwnerTodo.id, table: "todos" }),
        expect.objectContaining({ id: bobTodo.id, table: "todos" }),
      ]),
    );
    expect(aliceRowsAfterBobInsert).toHaveLength(3);
  });

  it("delivers all client-local subscription rows even when callers supply sessions", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-policy-subscription:node"),
      testAuthorBytes("jazz-napi-native-runtime-policy-subscription:author"),
      14,
      true,
    );
    runtimes.push(runtime);

    const aliceSession = testExternalSessionJson(ALICE_ID);
    const bobSession = testExternalSessionJson(BOB_ID);
    const query = JSON.stringify({ table: "todos" });
    const aliceUpdates: unknown[] = [];
    // Terminal layouts are registered once at the subscription boundary and
    // later deltas reference that stable id. Keep one decoder for the whole
    // subscription rather than treating independent callbacks as snapshots.
    const aliceSubscription = new SubscriptionManager<WasmRow>();
    const decodeAliceDelta = (delta: unknown) =>
      aliceSubscription.handleDelta(
        delta as Parameters<SubscriptionManager<WasmRow>["handleDelta"]>[0],
        (row) => row,
      );
    const aliceDecodedUpdates: ReturnType<SubscriptionManager<WasmRow>["handleDelta"]>[] = [];

    const aliceHandle = runtime.createSubscription(query, aliceSession, "local");
    runtime.executeSubscription(aliceHandle, (delta: unknown) => {
      aliceUpdates.push(delta);
      aliceDecodedUpdates.push(decodeAliceDelta(delta));
    });

    expect(aliceDecodedUpdates[0]).toEqual({ all: [], delta: [], reset: true });

    const aliceTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "alice subscribed row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    const bobTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "bob subscribed row" },
        done: { type: "Boolean", value: true },
        owner_id: { type: "Text", value: BOB_ID },
      },
      bobSession,
    );

    await Promise.all([
      runtime.waitForTransaction(await committedTxId(aliceTodo), "local"),
      runtime.waitForTransaction(await committedTxId(bobTodo), "local"),
    ]);

    await expect(runtime.query(query, aliceSession, "local")).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: aliceTodo.id }),
        expect.objectContaining({ id: bobTodo.id }),
      ]),
    );
    await expect(runtime.query(query, bobSession, "local")).resolves.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: aliceTodo.id }),
        expect.objectContaining({ id: bobTodo.id }),
      ]),
    );

    expect(aliceUpdates).toHaveLength(3);
    expect(aliceDecodedUpdates[1]).toEqual(
      expect.objectContaining({
        all: [expect.objectContaining({ id: aliceTodo.id })],
        delta: [
          expect.objectContaining({
            kind: 0,
            id: aliceTodo.id,
            item: expect.objectContaining({
              id: aliceTodo.id,
              values: [
                { type: "Text", value: "alice subscribed row" },
                { type: "Boolean", value: false },
                { type: "Text", value: ALICE_ID },
              ],
            }),
          }),
        ],
      }),
    );
    const finalRows = aliceDecodedUpdates[2]?.all;
    expect(finalRows).toHaveLength(2);
    expect(finalRows?.map((row) => row.id).sort()).toEqual([aliceTodo.id, bobTodo.id].sort());
    expect(aliceDecodedUpdates[2]?.delta).toEqual([
      expect.objectContaining({ kind: 0, id: bobTodo.id }),
    ]);

    runtime.unsubscribe(aliceHandle);
  });

  it("uses session identity for trusted-serving NAPI reads", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-delete-policy:node"),
      testAuthorBytes("jazz-napi-native-runtime-delete-policy:author"),
      12,
      true,
      { readAuthorizationHost: "trusted-serving" },
    );
    const aliceSession = testExternalSessionJson(ALICE_ID);
    const bobSession = testExternalSessionJson(BOB_ID);

    const aliceTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "alice delete row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    const bobTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "bob delete row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: BOB_ID },
      },
      bobSession,
    );

    await Promise.all([
      runtime.waitForTransaction(await committedTxId(aliceTodo), "local"),
      runtime.waitForTransaction(await committedTxId(bobTodo), "local"),
    ]);

    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), aliceSession, "local"),
    ).resolves.toEqual([expect.objectContaining({ id: aliceTodo.id })]);
    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), bobSession, "local"),
    ).resolves.toEqual([expect.objectContaining({ id: bobTodo.id })]);

    // This adapter's direct writes are advisory: capture both cross-identity
    // deletes and prove their local effects through identity-scoped reads.
    const aliceDeletesBob = runtime.delete("todos", bobTodo.id, aliceSession);
    const bobDeletesAlice = runtime.delete("todos", aliceTodo.id, bobSession);
    await Promise.all([
      runtime.waitForTransaction(await committedTxId(aliceDeletesBob), "local"),
      runtime.waitForTransaction(await committedTxId(bobDeletesAlice), "local"),
    ]);

    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), aliceSession, "local"),
    ).resolves.toEqual([]);
    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), bobSession, "local"),
    ).resolves.toEqual([]);
  });

  it("enforces the opening trusted identity for transaction-local NAPI reads", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-transaction-read-identity:node"),
      testAuthorBytes("jazz-napi-transaction-read-identity:author"),
      23,
      true,
      { readAuthorizationHost: "trusted-serving" },
    );
    runtimes.push(runtime);
    const aliceSession = testExternalSessionJson(ALICE_ID);
    const bobSession = testExternalSessionJson(BOB_ID);
    const aliceTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "allowed for opening Alice" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    const bobTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "denied to opening Alice" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: BOB_ID },
      },
      bobSession,
    );
    await Promise.all([
      runtime.waitForTransaction(await committedTxId(aliceTodo), "local"),
      runtime.waitForTransaction(await committedTxId(bobTodo), "local"),
    ]);

    const transactionId = createOpenTransactionId();
    runtime.beginTransaction("mergeable", transactionId, aliceSession);
    const staged = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "staged for opening Alice" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      JSON.stringify({ transaction_id: transactionId, session: JSON.parse(aliceSession) }),
    );

    const rows = (await runtime.query(
      JSON.stringify({ table: "todos" }),
      bobSession,
      "local",
      JSON.stringify({ transaction_id: transactionId }),
    )) as Array<{ id: string }>;
    expect(rows.map((row) => row.id).sort()).toEqual([aliceTodo.id, staged.id].sort());
    expect(rows.map((row) => row.id)).not.toContain(bobTodo.id);

    // Exercise the real raw NAPI ABI as well as the adapter. The adapter must
    // retain Alice, but the core is the authority boundary: supplying Bob's
    // bytes directly cannot re-authorize this Alice-opened mergeable batch.
    const raw = runtime as unknown as {
      db: {
        all(
          query: Uint8Array,
          opts: unknown,
          openTransactionId: string,
          author: Uint8Array,
        ): Uint8Array | Promise<Uint8Array>;
      };
    };
    const query = queryFromTable("todos");
    const aliceAuthor = testExternalAuthorBytes(ALICE_ID);
    const bobAuthor = testExternalAuthorBytes(BOB_ID);
    await expect(
      Promise.resolve().then(() => raw.db.all(query, undefined, transactionId, aliceAuthor)),
    ).resolves.toBeInstanceOf(Uint8Array);
    await expect(
      Promise.resolve().then(() => raw.db.all(query, undefined, transactionId, bobAuthor)),
    ).rejects.toThrow(/open transaction identity.*bound identity/i);
    await runtime.rollbackTransaction(transactionId);
  });

  it("authorizes NAPI websocket preflights through the admitted client session", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    expect(Object.getOwnPropertyNames(NapiDb.prototype)).toEqual(
      expect.arrayContaining([
        "requestInsertPermissionAdvice",
        "requestReadPermissionAdvice",
        "requestUpdatePermissionAdvice",
        "requestDeletePermissionAdvice",
      ]),
    );
    const appId = "00000000-0000-0000-0000-00000000d006";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      backendSecret: "core-napi-permission-advice-backend",
      adminSecret: "core-napi-permission-advice-admin",
      schema: encodeSchema(OWNED_TODOS_SCHEMA),
    });

    const openRuntime = (userId: string, sourceId: number) => {
      const runtime = new NativeRuntimeAdapter(
        { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
        OWNED_TODOS_SCHEMA,
        deterministicBytes(`jazz-napi-permission-advice:${userId}:node`),
        testExternalAuthorBytes(userId),
        sourceId,
        true,
      );
      runtimes.push(runtime);
      runtime.connect(
        webSocketUrl(server!.url, appId),
        JSON.stringify({
          backend_secret: server!.backendSecret,
          backend_session: testExternalSession(userId, { sub: userId }),
        }),
      );
      return runtime;
    };

    const alice = openRuntime(ALICE_ID, 61);
    const bob = openRuntime(BOB_ID, 62);
    const aliceTodo = alice.insert("todos", {
      title: { type: "Text", value: "alice authority advice row" },
      done: { type: "Boolean", value: false },
      owner_id: { type: "Text", value: ALICE_ID },
    });
    const bobTodo = bob.insert("todos", {
      title: { type: "Text", value: "bob authority advice row" },
      done: { type: "Boolean", value: false },
      owner_id: { type: "Text", value: BOB_ID },
    });
    await Promise.all([
      waitForPromise(
        alice.waitForTransaction(await committedTxId(aliceTodo), "edge"),
        "alice authority row did not settle",
      ),
      waitForPromise(
        bob.waitForTransaction(await committedTxId(bobTodo), "edge"),
        "bob authority row did not settle",
      ),
    ]);

    await expect(
      alice.requestInsertPermissionAdvice("todos", {
        title: { type: "Text", value: "allowed candidate" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      }),
    ).resolves.toBe("allowed");
    await expect(
      alice.requestInsertPermissionAdvice("todos", {
        title: { type: "Text", value: "denied candidate" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: BOB_ID },
      }),
    ).resolves.toBe("denied");

    await expect(alice.requestReadPermissionAdvice("todos", aliceTodo.id)).resolves.toBe("allowed");
    await expect(alice.requestReadPermissionAdvice("todos", bobTodo.id)).resolves.toBe("denied");
    await expect(
      alice.requestUpdatePermissionAdvice("todos", aliceTodo.id, {
        done: { type: "Boolean", value: true },
      }),
    ).resolves.toBe("allowed");
    await expect(
      alice.requestUpdatePermissionAdvice("todos", bobTodo.id, {
        done: { type: "Boolean", value: true },
      }),
    ).resolves.toBe("denied");
    await expect(alice.requestDeletePermissionAdvice("todos", aliceTodo.id)).resolves.toBe(
      "allowed",
    );
    await expect(alice.requestDeletePermissionAdvice("todos", bobTodo.id)).resolves.toBe("denied");

    await alice.disconnect();
    await expect(
      alice.requestInsertPermissionAdvice("todos", {
        title: { type: "Text", value: "offline candidate" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      }),
    ).resolves.toBe("unknown");
  }, 20_000);

  it("does not authenticate client-local session identities to an upstream authority", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d003";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      backendSecret: "core-napi-owned-delete-backend",
      adminSecret: "core-napi-owned-delete-admin",
      schema: encodeSchema(OWNED_TODOS_SCHEMA),
    });

    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      OWNED_TODOS_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-edge-delete-policy:node"),
      testAuthorBytes("jazz-napi-native-runtime-edge-delete-policy:author"),
      13,
      true,
    );
    runtimes.push(runtime);
    runtime.connect(
      webSocketUrl(server.url, appId),
      JSON.stringify({ backend_secret: server.backendSecret }),
    );

    const aliceSession = testExternalSessionJson(ALICE_ID);
    const bobSession = testExternalSessionJson(BOB_ID);

    const aliceTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "alice edge delete row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: ALICE_ID },
      },
      aliceSession,
    );
    const bobTodo = runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "bob edge delete row" },
        done: { type: "Boolean", value: false },
        owner_id: { type: "Text", value: BOB_ID },
      },
      bobSession,
    );

    await Promise.all([
      runtime.waitForTransaction(await committedTxId(aliceTodo), "local"),
      runtime.waitForTransaction(await committedTxId(bobTodo), "local"),
    ]);
    await expect(
      runtime.query(JSON.stringify({ table: "todos" }), aliceSession, "local"),
    ).resolves.toHaveLength(2);

    const aliceDenied = runtime.waitForTransaction(await committedTxId(aliceTodo), "edge");
    await expect(aliceDenied).rejects.toMatchObject({
      kind: "rejected",
      code: "permission_denied",
      reason: "Write rejected by server authorization",
    });
    const aliceRejection = await aliceDenied.catch((error) => error);
    expect(Object.getOwnPropertyDescriptor(aliceRejection, "message")).toMatchObject({
      enumerable: false,
      value: expect.stringContaining("AuthorizationDenied"),
    });
    await expect(aliceDenied).rejects.toThrow("AuthorizationDenied");
    const bobDenied = runtime.waitForTransaction(await committedTxId(bobTodo), "edge");
    await expect(bobDenied).rejects.toMatchObject({
      kind: "rejected",
      code: "permission_denied",
      reason: "Write rejected by server authorization",
    });
    await expect(bobDenied).rejects.toThrow("AuthorizationDenied");
  }, 15_000);

  it("keeps persistent client-local session writes optimistic until the authority rejects them", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d004";
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-core-owned-delete-"));
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      backendSecret: "core-napi-persistent-owned-delete-backend",
      adminSecret: "core-napi-persistent-owned-delete-admin",
      schema: encodeSchema(OWNED_TODOS_SCHEMA),
    });

    try {
      const runtime = new NativeRuntimeAdapter(
        {
          openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDb.openPersistent(path, schema, config) as never,
        },
        OWNED_TODOS_SCHEMA,
        deterministicBytes("jazz-napi-native-runtime-persistent-edge-delete-policy:node"),
        testAuthorBytes("jazz-napi-native-runtime-persistent-edge-delete-policy:author"),
        14,
        true,
        { persistentPath: join(tempDir, "db") },
      );
      runtimes.push(runtime);
      runtime.connect(
        webSocketUrl(server.url, appId),
        JSON.stringify({ backend_secret: server.backendSecret }),
      );

      const aliceSession = JSON.stringify({ issuer: "https://issuer.example", user_id: ALICE_ID });
      const bobSession = JSON.stringify({ issuer: "https://issuer.example", user_id: BOB_ID });

      const aliceTodo = runtime.insert(
        "todos",
        {
          title: { type: "Text", value: "alice persistent edge delete row" },
          done: { type: "Boolean", value: false },
          owner_id: { type: "Text", value: ALICE_ID },
        },
        aliceSession,
      );
      const bobTodo = runtime.insert(
        "todos",
        {
          title: { type: "Text", value: "bob persistent edge delete row" },
          done: { type: "Boolean", value: false },
          owner_id: { type: "Text", value: BOB_ID },
        },
        bobSession,
      );

      await Promise.all([
        runtime.waitForTransaction(await committedTxId(aliceTodo), "local"),
        runtime.waitForTransaction(await committedTxId(bobTodo), "local"),
      ]);
      await expect(
        runtime.query(JSON.stringify({ table: "todos" }), aliceSession, "local"),
      ).resolves.toHaveLength(2);

      await expect(
        runtime.waitForTransaction(await committedTxId(aliceTodo), "edge"),
      ).rejects.toThrow("AuthorizationDenied");
      await expect(
        runtime.waitForTransaction(await committedTxId(bobTodo), "edge"),
      ).rejects.toThrow("AuthorizationDenied");
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  }, 15_000);

  it("supports native runtime parity writes, mergeable transactions, and upstream transport", async () => {
    const { NapiDb } = await loadNapiModule();
    const runtime = new NativeRuntimeAdapter(
      { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
      TEST_SCHEMA,
      deterministicBytes("jazz-napi-native-runtime-parity:node"),
      testAuthorBytes("jazz-napi-native-runtime-parity:author"),
      2,
      true,
    );

    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "direct napi parity row" },
      done: { type: "Boolean", value: false },
    });
    runtime.delete("todos", inserted.id);
    runtime.restore("todos", inserted.id, {
      title: { type: "Text", value: "direct napi restored row" },
      done: { type: "Boolean", value: false },
    });
    runtime.upsert("todos", "11111111-1111-4111-8111-111111111111", {
      title: { type: "Text", value: "direct napi upserted row" },
      done: { type: "Boolean", value: false },
    });

    const tx = beginTestBatch(runtime);
    runtime.update(
      "todos",
      inserted.id,
      { done: { type: "Boolean", value: true } },
      JSON.stringify({ transaction_id: tx }),
    );
    runtime.upsert(
      "todos",
      inserted.id,
      {
        title: { type: "Text", value: "direct napi tx upserted row" },
        done: { type: "Boolean", value: true },
      },
      JSON.stringify({ transaction_id: tx }),
    );
    runtime.insert(
      "todos",
      {
        title: { type: "Text", value: "direct napi tx row" },
        done: { type: "Boolean", value: false },
      },
      JSON.stringify({ transaction_id: tx }),
      "22222222-2222-4222-8222-222222222222",
    );
    const committed = await runtime.commitTransaction(tx);
    await runtime.waitForTransaction(committed, "local");

    const rows = await runtime.query(JSON.stringify({ table: "todos" }));
    expect(rows).toHaveLength(3);
    expect(rows).toEqual(
      expect.arrayContaining([
        {
          id: inserted.id,
          table: "todos",
          values: [
            { type: "Text", value: "direct napi tx upserted row" },
            { type: "Boolean", value: true },
          ],
        },
        {
          id: "11111111-1111-4111-8111-111111111111",
          table: "todos",
          values: [
            { type: "Text", value: "direct napi upserted row" },
            { type: "Boolean", value: false },
          ],
        },
        {
          id: "22222222-2222-4222-8222-222222222222",
          table: "todos",
          values: [
            { type: "Text", value: "direct napi tx row" },
            { type: "Boolean", value: false },
          ],
        },
      ]),
    );

    const transport = await runtime.connectUpstreamPeer();
    expect(transport.tick()).toBeGreaterThanOrEqual(0);
    expect(transport.recvWireFrames()).toEqual(expect.any(Array));
    expect(transport.close()).toBe(true);
    expect(transport.close()).toBe(false);
  });

  it("propagates an edge-tier query over the native runtime/server boundary and returns remote row adds", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d001";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "core-napi-edge-query-admin",
      schema: encodeSchema(TEST_SCHEMA),
    });

    const openRuntime = (peer: string, sourceId: number) => {
      const runtime = new NativeRuntimeAdapter(
        { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
        TEST_SCHEMA,
        deterministicBytes(`jazz-napi-core-edge:${peer}:node`),
        testAuthorBytes(`jazz-napi-core-edge:${peer}:author`),
        sourceId,
        true,
      );
      runtimes.push(runtime);
      runtime.connect(
        webSocketUrl(server!.url, appId),
        JSON.stringify({ admin_secret: server!.adminSecret }),
      );
      return runtime;
    };

    const writer = openRuntime("writer", 31);
    const reader = openRuntime("reader", 32);
    const queryJson = JSON.stringify({ table: "todos" });

    expect(await reader.query(queryJson, null, "local")).toEqual([]);

    const inserted = writer.insert("todos", {
      title: { type: "Text", value: "direct napi propagated edge row" },
      done: { type: "Boolean", value: false },
    });
    await waitForPromise(
      writer.waitForTransaction(await committedTxId(inserted), "edge"),
      "writer insert did not settle at edge",
    );

    const propagatedRow = await waitFor(async () => {
      const rows = (await reader.query(queryJson, null, "edge")) as Array<{
        id: string;
        table: string;
        values: unknown[];
      }>;
      return rows.find((row) => row.id === inserted.id);
    }, "reader edge query did not receive the propagated row add");

    expect(propagatedRow).toEqual({
      id: inserted.id,
      table: "todos",
      values: [
        { type: "Text", value: "direct napi propagated edge row" },
        { type: "Boolean", value: false },
      ],
    });
  }, 15_000);

  it("propagates an edge-tier query through a persistent core server", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d002";
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-core-server-"));
    server = await startLocalJazzServer({
      appId,
      dataDir: tempDir,
      adminSecret: "core-napi-persistent-edge-query-admin",
      schema: encodeSchema(TEST_SCHEMA),
    });

    const openRuntime = (peer: string, sourceId: number, targetServer: LocalJazzServerHandle) => {
      const runtime = new NativeRuntimeAdapter(
        { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
        TEST_SCHEMA,
        deterministicBytes(`jazz-napi-core-persistent-edge:${peer}:node`),
        testAuthorBytes(`jazz-napi-core-persistent-edge:${peer}:author`),
        sourceId,
        true,
      );
      runtimes.push(runtime);
      runtime.connect(
        webSocketUrl(targetServer.url, appId),
        JSON.stringify({ admin_secret: targetServer.adminSecret }),
      );
      return runtime;
    };

    try {
      const writer = openRuntime("writer", 41, server);

      const inserted = writer.insert("todos", {
        title: { type: "Text", value: "direct napi persistent propagated edge row" },
        done: { type: "Boolean", value: false },
      });
      await waitForPromise(
        writer.waitForTransaction(await committedTxId(inserted), "edge"),
        "writer insert did not settle at persistent edge",
      );
      await writer.close();
      runtimes.splice(runtimes.indexOf(writer), 1);

      await server.stop();
      server = await startLocalJazzServer({
        appId,
        dataDir: tempDir,
        adminSecret: "core-napi-persistent-edge-query-admin",
        schema: encodeSchema(TEST_SCHEMA),
      });

      const reader = openRuntime("reader", 42, server);
      const queryJson = JSON.stringify({ table: "todos" });
      // A fresh strict-remote read owns its first authority receipt.  It must
      // not resolve with the reader's empty local state while the restarted
      // server is still delivering that receipt; callers should receive the
      // persisted row from this one read.
      const rows = (await reader.query(queryJson, null, "edge")) as Array<{
        id: string;
        table: string;
        values: unknown[];
      }>;
      const propagatedRow = rows.find((row) => row.id === inserted.id);

      expect(propagatedRow).toEqual({
        id: inserted.id,
        table: "todos",
        values: [
          { type: "Text", value: "direct napi persistent propagated edge row" },
          { type: "Boolean", value: false },
        ],
      });
    } finally {
      rmSync(tempDir, { recursive: true, force: true });
    }
  }, 15_000);

  it("propagates session-authenticated branch-policy reads over websocket", async () => {
    globalThis.WebSocket ??= WebSocket as unknown as typeof globalThis.WebSocket;

    const { NapiDb } = await loadNapiModule();
    const appId = "00000000-0000-0000-0000-00000000d003";
    server = await startLocalJazzServer({
      appId,
      inMemory: true,
      adminSecret: "core-napi-branch-policy-admin",
      backendSecret: "core-napi-branch-policy-backend",
      schema: encodeSchema(CHAT_POLICY_SCHEMA),
    });

    const openRuntime = (userId: string, sourceId: number) => {
      const runtime = new NativeRuntimeAdapter(
        { openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never },
        CHAT_POLICY_SCHEMA,
        deterministicBytes(`jazz-napi-core-branch-policy:${userId}:node`),
        testExternalAuthorBytes(userId),
        sourceId,
        true,
      );
      runtimes.push(runtime);
      runtime.connect(
        webSocketUrl(server!.url, appId),
        JSON.stringify({
          backend_secret: "core-napi-branch-policy-backend",
          backend_session: testExternalSession(userId),
        }),
      );
      return runtime;
    };

    const writer = openRuntime(ALICE_ID, 51);
    const reader = openRuntime(BOB_ID, 52);
    const inserted = writer.insert("chats", {
      title: { type: "Text", value: "public websocket branch chat" },
      visibility: { type: "Text", value: "public" },
      owner_id: { type: "Text", value: ALICE_ID },
    });

    await waitForPromise(
      writer.waitForTransaction(await committedTxId(inserted), "edge"),
      "writer public chat insert did not settle at edge",
    );

    const bobSession = testExternalSessionJson(BOB_ID);
    const propagatedRow = await waitFor(async () => {
      const rows = (await reader.query(
        JSON.stringify({ table: "chats" }),
        bobSession,
        "edge",
      )) as Array<{
        id: string;
        table: string;
        values: unknown[];
      }>;
      return rows.find((row) => row.id === inserted.id);
    }, "reader edge query did not receive public branch-policy chat");

    expect(propagatedRow).toEqual({
      id: inserted.id,
      table: "chats",
      values: [
        { type: "Text", value: "public websocket branch chat" },
        { type: "Text", value: "public" },
        { type: "Text", value: ALICE_ID },
      ],
    });

    const message = writer.insert("messages", {
      chat_id: { type: "Uuid", value: inserted.id },
      text: { type: "Text", value: "hello through public chat policy" },
    });
    await waitForPromise(
      writer.waitForTransaction(await committedTxId(message), "edge"),
      "writer public-chat message insert did not settle at edge",
    );

    const propagatedMessage = await waitFor(async () => {
      const rows = (await reader.query(
        JSON.stringify({ table: "messages" }),
        bobSession,
        "edge",
      )) as Array<{
        id: string;
        table: string;
        values: unknown[];
      }>;
      return rows.find((row) => row.id === message.id);
    }, "reader edge query did not receive message through public-chat branch policy");

    expect(propagatedMessage).toEqual({
      id: message.id,
      table: "messages",
      values: [
        { type: "Uuid", value: inserted.id },
        { type: "Text", value: "hello through public chat policy" },
      ],
    });
  }, 15_000);

  it("reopens a persistent database and reads previously written rows", async () => {
    const { NapiDb } = await loadNapiModule();
    const tempDir = mkdtempSync(join(tmpdir(), "jazz-napi-core-"));
    const dataPath = join(tempDir, "db");
    const node = deterministicBytes("jazz-napi-core-persistent:node");
    const author = testAuthorBytes("jazz-napi-core-persistent:author");
    let firstRuntime: NativeRuntimeAdapter | null = null;
    let secondRuntime: NativeRuntimeAdapter | null = null;

    try {
      firstRuntime = new NativeRuntimeAdapter(
        {
          openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDb.openPersistent(path, schema, config) as never,
        },
        TEST_SCHEMA,
        node,
        author,
        7,
        true,
        { persistentPath: dataPath },
      );

      const inserted = firstRuntime.insert("todos", {
        title: { type: "Text", value: "direct napi persistent row" },
        done: { type: "Boolean", value: false },
      });
      await firstRuntime.waitForTransaction(await committedTxId(inserted), "local");
      await firstRuntime.close();
      firstRuntime = null;

      secondRuntime = new NativeRuntimeAdapter(
        {
          openMemory: (schema, config) => NapiDb.openMemory(schema, config) as never,
          openPersistent: (path, schema, config) =>
            NapiDb.openPersistent(path, schema, config) as never,
        },
        TEST_SCHEMA,
        node,
        author,
        7,
        true,
        { persistentPath: dataPath },
      );

      await expect(secondRuntime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
        {
          id: inserted.id,
          table: "todos",
          values: [
            { type: "Text", value: "direct napi persistent row" },
            { type: "Boolean", value: false },
          ],
        },
      ]);
      await secondRuntime.close();
      secondRuntime = null;
    } finally {
      await firstRuntime?.close();
      await secondRuntime?.close();
      rmSync(tempDir, { recursive: true, force: true });
    }
  });
});

function deterministicBytes(seed: string): Uint8Array {
  let hash = 0x811c9dc5;
  const bytes = new Uint8Array(16);
  const view = new DataView(bytes.buffer);
  for (let round = 0; round < 4; round += 1) {
    for (let i = 0; i < seed.length; i += 1) {
      hash ^= seed.charCodeAt(i) + round;
      hash = Math.imul(hash, 0x01000193);
    }
    view.setUint32(round * 4, hash >>> 0, true);
  }
  return bytes;
}

async function waitFor<T>(
  read: () => Promise<T | undefined>,
  message: string,
  timeoutMs = 5_000,
): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  do {
    const value = await read();
    if (value !== undefined) return value;
    await sleep(25);
  } while (Date.now() < deadline);
  throw new Error(message);
}

async function waitForPromise<T>(
  promise: Promise<T>,
  message: string,
  timeoutMs = 5_000,
): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const timeoutPromise = new Promise<never>((_, reject) => {
    timeout = setTimeout(() => reject(new Error(message)), timeoutMs);
  });
  try {
    return await Promise.race([promise, timeoutPromise]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

async function sleep(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
