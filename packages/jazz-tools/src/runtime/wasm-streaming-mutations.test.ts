import { translateQuery } from "./query-adapter.js";
import { describe, expect, it, onTestFinished } from "vitest";
import { schema as s } from "../index.js";
import type { TxId, WriteReceipt } from "./client.js";
import {
  encodeCellsForRow,
  encodeSchema,
  NativeRuntimeAdapter,
} from "./native-runtime/native-runtime-adapter.js";
import { openConfig, queryFromTable } from "./native-runtime/native-codec.js";
import {
  createWasmRuntime,
  hasJazzWasmBuild,
  loadWasmModuleForTest,
} from "./testing/wasm-runtime-test-utils.js";
import { testAccountId, testAuthorBytes } from "./testing/account-fixtures.js";

const app = s.defineApp({
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    metadata: s.json().optional(),
  }),
});
const streamingApp = s.defineApp({
  todos: s.table({
    title: s.string().optional(),
    done: s.boolean(),
  }),
});

type PageStoreMetadata = {
  pageSize: number;
  generation: number;
  rootPageId: number | null;
  nextPageId: number;
};

function deferredVoid() {
  let resolve!: () => void;
  const promise = new Promise<void>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function createTestPageStore() {
  let persistedMetadata: PageStoreMetadata | null = null;
  const pages = new Map<number, Uint8Array>();
  let pendingGate:
    | {
        started: () => void;
        wait: Promise<void>;
        release: () => void;
        failAfterRelease: boolean;
      }
    | undefined;

  const pageStore = {
    metadata: async () => persistedMetadata,
    readPage: async (pageId: number) => pages.get(pageId) ?? null,
    commitPages: async (
      expectedGeneration: number,
      pageSize: number,
      rootPageId: number,
      nextPageId: number,
      pageIds: number[],
      pageBytes: Uint8Array[],
      deletedPageIds: number[],
    ) => {
      const gate = pendingGate;
      if (gate) {
        pendingGate = undefined;
        gate.started();
        await gate.wait;
        if (gate.failAfterRelease) throw new Error("injected page-store commit failure");
      }
      for (let index = 0; index < pageIds.length; index += 1) {
        pages.set(pageIds[index]!, new Uint8Array(pageBytes[index]!));
      }
      for (const pageId of deletedPageIds) pages.delete(pageId);
      persistedMetadata = {
        pageSize,
        generation: expectedGeneration + 1,
        rootPageId: rootPageId < 0 ? null : rootPageId,
        nextPageId,
      };
      return persistedMetadata;
    },
  };

  return {
    pageStore,
    armCommitGate(failAfterRelease = false) {
      const wait = deferredVoid();
      const started = deferredVoid();
      pendingGate = {
        started: started.resolve,
        wait: wait.promise,
        release: wait.resolve,
        failAfterRelease,
      };
      return { started: started.promise, release: wait.resolve };
    },
  };
}

async function createBrowserWasmFixture() {
  const wasmModule = await loadWasmModuleForTest();
  const node = new Uint8Array(16);
  node[0] = 1;
  const author = testAuthorBytes("wasm-streaming-abort:author");
  const pageStore = createTestPageStore();
  const db = await wasmModule.WasmDb.openBrowser(
    pageStore.pageStore,
    encodeSchema(streamingApp.wasmSchema),
    openConfig(node, author, 1, true),
    "wasm-streaming-abort-owner",
  );
  const runtime = NativeRuntimeAdapter.fromDb(db, streamingApp.wasmSchema, node, author, 1, true);
  onTestFinished(async () => {
    await runtime.close?.();
  });
  return { db, runtime, pageStore, author };
}

function uuidBytes(value: string): Uint8Array {
  return Uint8Array.from(value.replaceAll("-", "").match(/../g)!, (byte) =>
    Number.parseInt(byte, 16),
  );
}

async function committedTxId(receipt: WriteReceipt): Promise<TxId> {
  if (receipt.kind !== "committed") throw new Error("expected committed write receipt");
  return await receipt.txId;
}

async function withWatchdog<T>(promise: Promise<T>, label: string, timeoutMs = 3_000): Promise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs}ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    if (timeout) clearTimeout(timeout);
  }
}

describe.skipIf(!hasJazzWasmBuild())("WASM streaming mutations", () => {
  it("keeps real WASM reads pending while storage owns the node", async () => {
    const { db, runtime, pageStore, author } = await createBrowserWasmFixture();
    const query = queryFromTable("todos");
    const opts = { tier: "local", propagation: "local_only" };
    const commitGate = pageStore.armCommitGate();
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000131"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
      author,
    );
    const push = upload.push(new TextEncoder().encode("pending owner"));
    let cancelSubscription: number | undefined;
    try {
      await withWatchdog(commitGate.started, "push holds the real WASM owner");
      const subscription = db.subscribe(query, opts) as {
        poll(): unknown | undefined;
        cancel(): void;
      };
      // wasm-bindgen encodes both Option::None return types as undefined.
      expect(subscription.poll()).toBeUndefined();
      subscription.cancel();

      let cancelledCallbacks = 0;
      const cancelled = runtime.createSubscription(JSON.stringify({ table: "todos" }));
      runtime.executeSubscription(cancelled, () => {
        cancelledCallbacks += 1;
      });
      runtime.unsubscribe(cancelled);

      let finished = false;
      const pendingQuery = runtime.query(JSON.stringify({ table: "todos" })).then(
        (rows) => {
          finished = true;
          return { rows };
        },
        (error: unknown) => {
          finished = true;
          return { error };
        },
      );
      cancelSubscription = runtime.createSubscription(
        JSON.stringify({ table: "todos" }),
        undefined,
        "local",
        JSON.stringify({ propagation: "local-only" }),
      );
      const opening = deferredVoid();
      let openingError: unknown;
      runtime.executeSubscription(cancelSubscription, (result: unknown) => {
        if (result instanceof Error) openingError = result;
        opening.resolve();
      });
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      await new Promise<void>((resolve) => queueMicrotask(resolve));
      expect(finished).toBe(false);
      commitGate.release();
      await push;
      await expect(withWatchdog(pendingQuery, "woken concurrent query")).resolves.toEqual({
        rows: [],
      });
      await withWatchdog(opening.promise, "woken concurrent subscription");
      expect(openingError).toBeUndefined();
      expect(cancelledCallbacks).toBe(0);
      await upload.abort();
    } finally {
      commitGate.release();
      if (cancelSubscription !== undefined) runtime.unsubscribe(cancelSubscription);
      await upload.abort();
    }
  });

  it("waits for an in-flight WASM push before aborting its staged upload", async () => {
    const { db, runtime, pageStore, author } = await createBrowserWasmFixture();
    const commitGate = pageStore.armCommitGate();
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000128"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
      author,
    );

    const push = upload.push(new TextEncoder().encode("gated push"));
    try {
      await withWatchdog(commitGate.started, "push entered storage");
      let abortSettled = false;
      const abort = upload.abort().then((result: boolean) => {
        abortSettled = true;
        return result;
      });
      const finishRejected = expect(upload.finish()).rejects.toThrow(/closed/i);
      const secondPushRejected = expect(upload.push(Uint8Array.of(65))).rejects.toThrow(/closed/i);
      const duplicate = upload.abort();
      await Promise.all([finishRejected, secondPushRejected]);
      await expect(duplicate).resolves.toBe(false);
      expect(abortSettled).toBe(false);

      commitGate.release();
      await expect(push).resolves.toBeUndefined();
      await expect(abort).resolves.toBe(true);
      expect(abortSettled).toBe(true);
      await expect(upload.finish()).rejects.toThrow(/closed/i);
      await expect(upload.push(Uint8Array.of(65))).rejects.toThrow(/closed/i);
    } finally {
      commitGate.release();
    }
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);

    db.setLargeValueStagingPolicy(Number.MAX_SAFE_INTEGER, 60_000, 0);
    // Expiry uses the platform clock; advance one event-loop turn past maxAgeMs=0.
    await new Promise((resolve) => setTimeout(resolve, 2));
    await expect(db.evictExpiredStagedLargeValues()).resolves.toBe(0);
  });

  it("hands failed in-flight push ownership to abort without retaining staged chunks", async () => {
    const { db, runtime, pageStore, author } = await createBrowserWasmFixture();
    const commitGate = pageStore.armCommitGate(true);
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000130"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
      author,
    );
    const pushRejected = expect(
      upload.push(new TextEncoder().encode("failed push")),
    ).rejects.toThrow(/injected page-store commit failure/);
    try {
      await withWatchdog(commitGate.started, "failed push entered storage");
      const abort = upload.abort();
      commitGate.release();
      await pushRejected;
      await expect(abort).resolves.toBe(true);
      await expect(upload.abort()).resolves.toBe(false);
      await expect(upload.finish()).rejects.toThrow(/closed/i);
    } finally {
      commitGate.release();
    }
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
    db.setLargeValueStagingPolicy(Number.MAX_SAFE_INTEGER, 60_000, 0);
    await new Promise((resolve) => setTimeout(resolve, 2));
    await expect(db.evictExpiredStagedLargeValues()).resolves.toBe(0);
  });

  it("keeps abort terminal across synchronous finish and repeated abort calls", async () => {
    const { db, runtime, author } = await createBrowserWasmFixture();
    const upload = db.beginStreamingMutation(
      "todos",
      uuidBytes("00000000-0000-4000-8000-000000000129"),
      encodeCellsForRow(streamingApp.wasmSchema.todos!, {
        done: { type: "Boolean", value: false },
      }),
      "title",
      "insert",
      author,
    );

    await expect(
      upload.push(new TextEncoder().encode("preserved push result")),
    ).resolves.toBeUndefined();
    const abort = upload.abort();
    const finish = upload.finish();
    const repeatedAbort = upload.abort();

    await expect(abort).resolves.toBe(true);
    await expect(repeatedAbort).resolves.toBe(false);
    await expect(finish).rejects.toThrow(/closed/i);
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
  });

  it("selects and filters public provenance authors as structured records", async () => {
    const appId = "wasm-public-provenance";
    const authorSeed = `${appId}:test:default:author`;
    const author = {
      account: testAccountId(authorSeed),
      identity: { issuer: "urn:jazz:test", subject: authorSeed },
    };
    const runtime = await createWasmRuntime(app.wasmSchema, { appId });
    const inserted = runtime.insert("todos", {
      title: { type: "Text", value: "created by canonical author" },
      done: { type: "Boolean", value: false },
    });
    await runtime.waitForTransaction(await committedTxId(inserted), "local");

    await expect(
      runtime.query(
        translateQuery(
          app.todos
            .select("title", "$createdBy", "$updatedBy")
            .where({ $createdBy: author })
            ._build(),
          app.wasmSchema,
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

  it("streams insert, update, and partial upsert through the browser runtime boundary", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-mutations",
    });
    const id = "00000000-0000-4000-8000-000000000123";

    await runtime.streamingMutation!(
      "insert",
      "todos",
      { done: { type: "Boolean", value: false } },
      "title",
      (async function* () {
        yield "streamed ";
        yield new TextEncoder().encode("through WASM ");
        yield "\ud83d";
        yield "\ude80";
      })(),
      null,
      id,
    );
    await runtime.streamingMutation!(
      "update",
      "todos",
      { done: { type: "Boolean", value: true } },
      "title",
      (async function* () {
        yield "updated";
      })(),
      null,
      id,
    );
    await runtime.streamingMutation!(
      "upsert",
      "todos",
      {},
      "title",
      (async function* () {
        yield "upserted";
      })(),
      null,
      id,
    );

    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
      {
        id,
        table: "todos",
        values: [
          { type: "Text", value: "upserted" },
          { type: "Boolean", value: true },
          { type: "Null" },
        ],
      },
    ]);
  });

  it("finishes concurrent streamed writes without wedging the shared WASM runtime", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-concurrent-streaming-publication",
    });
    const source = async function* (prefix: string) {
      yield `${prefix} first `;
      await Promise.resolve();
      yield `${prefix} second`;
    };
    let bodyError: unknown;

    try {
      const writes = await withWatchdog(
        Promise.all([
          runtime.streamingMutation!(
            "insert",
            "todos",
            { done: { type: "Boolean", value: false } },
            "title",
            source("one"),
            null,
            "00000000-0000-4000-8000-000000000201",
          ),
          runtime.streamingMutation!(
            "insert",
            "todos",
            { done: { type: "Boolean", value: true } },
            "title",
            source("two"),
            null,
            "00000000-0000-4000-8000-000000000202",
          ),
        ]),
        "concurrent streamed WASM publication",
      );
      const txIds = await Promise.all(writes.map(committedTxId));
      await withWatchdog(
        Promise.all(txIds.map((txId) => runtime.waitForTransaction!(txId, "local"))),
        "concurrent streamed WASM local settlement",
      );

      await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([
        {
          id: "00000000-0000-4000-8000-000000000201",
          table: "todos",
          values: [
            { type: "Text", value: "one first one second" },
            { type: "Boolean", value: false },
            { type: "Null" },
          ],
        },
        {
          id: "00000000-0000-4000-8000-000000000202",
          table: "todos",
          values: [
            { type: "Text", value: "two first two second" },
            { type: "Boolean", value: true },
            { type: "Null" },
          ],
        },
      ]);
    } catch (error) {
      bodyError = error;
    }
    let cleanupError: unknown;
    try {
      const closeRuntime = runtime.close;
      if (!closeRuntime) throw new Error("WASM runtime does not expose close()");
      await withWatchdog(
        Promise.resolve().then(() => closeRuntime.call(runtime)),
        "concurrent streamed WASM runtime cleanup",
        1_000,
      );
    } catch (error) {
      cleanupError = error;
    }
    // The publication failure is the actionable signal when both phases fail.
    if (bodyError) throw bodyError;
    if (cleanupError) throw cleanupError;
  });

  it("rejects fragmented invalid JSON without publishing a row", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-invalid-json",
    });

    await expect(
      runtime.streamingMutation!(
        "insert",
        "todos",
        {
          title: { type: "Text", value: "invalid" },
          done: { type: "Boolean", value: false },
        },
        "metadata",
        (async function* () {
          yield '{"nested":[';
          yield "1,";
          yield "]}";
        })(),
        null,
        "00000000-0000-4000-8000-000000000124",
      ),
    ).rejects.toThrow();
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
  });

  it("stops push uploads at the ingress limit before accepting the row", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-rate-limit",
    });
    runtime.setLargeValueStagingPolicy!(1, 60_000, null);

    await expect(
      runtime.streamingMutation!(
        "insert",
        "todos",
        { done: { type: "Boolean", value: false } },
        "title",
        (async function* () {
          yield "x".repeat(512 * 1024);
        })(),
        null,
        "00000000-0000-4000-8000-000000000125",
      ),
    ).rejects.toThrow(/rate limit/i);
    await expect(runtime.query(JSON.stringify({ table: "todos" }))).resolves.toEqual([]);
  });

  it("rejects unsafe custom timestamps before consuming the stream", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-unsafe-timestamp",
    });
    let consumed = false;

    await expect(
      runtime.streamingMutation!(
        "insert",
        "todos",
        { done: { type: "Boolean", value: false } },
        "title",
        (async function* () {
          consumed = true;
          yield "never consumed";
        })(),
        JSON.stringify({ updated_at: Number.MAX_SAFE_INTEGER + 1 }),
        "00000000-0000-4000-8000-000000000126",
      ),
    ).rejects.toThrow(/safe integer/i);
    expect(consumed).toBe(false);
  });

  it("releases persisted pending chunks when the producer aborts", async () => {
    const runtime = await createWasmRuntime(app.wasmSchema, {
      appId: "wasm-streaming-explicit-abort",
    });

    await expect(
      runtime.streamingMutation!(
        "insert",
        "todos",
        { done: { type: "Boolean", value: false } },
        "title",
        (async function* () {
          yield "x".repeat(512 * 1024);
          throw new Error("producer aborted");
        })(),
        null,
        "00000000-0000-4000-8000-000000000127",
      ),
    ).rejects.toThrow("producer aborted");

    runtime.setLargeValueStagingPolicy!(Number.MAX_SAFE_INTEGER, 60_000, 0);
    await new Promise((resolve) => setTimeout(resolve, 2));
    await expect(runtime.evictExpiredStagedLargeValues!()).resolves.toBe(0);
  });
});
