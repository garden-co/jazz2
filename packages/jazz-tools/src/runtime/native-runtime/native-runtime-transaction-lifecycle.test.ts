import { expect, it, vi } from "vitest";
import {
  PostcardWriter,
  createRecord,
  writeNativeRowDescriptor,
  type DescriptorField,
  type NativeRowDescriptorField,
} from "./native-codec.js";
import type { WasmSchema } from "../../drivers/types.js";
import { NativeRuntimeAdapter } from "./native-runtime-adapter.js";
import {
  createOpenTransactionId,
  type TxId,
  type MutationErrorEvent,
  type OpenTransactionId,
} from "../client.js";

function beginTestBatch(runtime: NativeRuntimeAdapter, userId?: string): OpenTransactionId {
  const id = createOpenTransactionId();
  runtime.beginTransaction(
    "mergeable",
    id,
    userId === undefined
      ? undefined
      : JSON.stringify({ issuer: "https://issuer.example", user_id: userId }),
  );
  return id;
}

const testSchema = {
  todos: {
    columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
  },
} satisfies WasmSchema;
const TEST_RUNTIME_AUTHOR = new TextEncoder().encode('["urn:jazz:test","runtime"]');

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

type EncodedTestRow = {
  table: string;
  rowId: Uint8Array;
  title: string;
};

function encodeRows(rows: EncodedTestRow[]): Uint8Array {
  const writer = new PostcardWriter();
  const byTable = new Map<string, EncodedTestRow[]>();
  for (const row of rows) {
    const tableRows = byTable.get(row.table) ?? [];
    tableRows.push(row);
    byTable.set(row.table, tableRows);
  }
  writer.vec((batch, batchIndex) => {
    const [table, tableRows] = Array.from(byTable.entries())[batchIndex]!;
    const descriptor = [{ name: "title", valueType: { tag: 8 } }];
    batch.string(table);
    writeNativeRowDescriptor(batch, physicalNativeDescriptor(descriptor));
    batch.vec((row, index) => {
      const source = tableRows[index]!;
      row.bytes(source.rowId);
      row.bool(false);
      row.bytes(
        createRecord(descriptor, [Uint8Array.from([2, ...new TextEncoder().encode(source.title)])]),
      );
    }, tableRows.length);
  }, byTable.size);
  return writer.finish();
}

function fakeDb<T extends object>(
  db: T,
): T & { setTickScheduler(callback: (urgency: "immediate" | "deferred") => void): void } {
  type FakeOpenBatch = {
    kind: "mergeable" | "exclusive";
    author?: Uint8Array;
  };
  const openBatches = new Map<string, FakeOpenBatch>();
  const requireOpenBatch = (openTransactionId: string): void => {
    const batch = openBatches.get(openTransactionId);
    if (!batch) throw new Error(`unknown batch ${openTransactionId}`);
  };
  return {
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
}

function fakeWrite() {
  return {
    txId: "00000000000070008000000000000001",
    payload: new Uint8Array(0),
    wait: async () => undefined,
    writeState: () => ({}),
  };
}

it("quiesces foreground mutation admission before capturing its final HLC", async () => {
  const insert = vi.fn(() => ({ ...fakeWrite(), rowId: new Uint8Array(16) }));
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          foregroundTxTimeHighWater: () => 41n,
          insert,
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
  const preexisting = beginTestBatch(runtime);
  expect(await runtime.quiesceForegroundTxTimeHighWater()).toBe(41n);
  // This is the P0 handoff ordering: an already-open batch cannot mint H+1
  // after the high-water was captured, nor can a new synchronous batch start.
  expect(() => runtime.commitTransaction(preexisting)).toThrow("native runtime is closed");
  expect(() => beginTestBatch(runtime)).toThrow("native runtime is closed");
  expect(() => runtime.insert("todos", { title: { type: "Text", value: "late" } })).toThrow(
    "native runtime is closed",
  );
  expect(insert).not.toHaveBeenCalled();
  await runtime.close();
});

it("drains an already admitted streaming mutation before returning its foreground HLC", async () => {
  let highWater = 7n;
  let releaseSource!: () => void;
  const sourceGate = new Promise<void>((resolve) => {
    releaseSource = resolve;
  });
  const beginStreamingMutation = vi.fn(() => ({
    push: () => undefined,
    finish: () => {
      highWater = 42n;
      return fakeWrite();
    },
    abort: () => true,
  }));
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          foregroundTxTimeHighWater: () => highWater,
          beginStreamingMutation,
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

  const stream = runtime.streamingMutation(
    "insert",
    "todos",
    {},
    "title",
    (async function* () {
      await sourceGate;
      yield "late write";
    })(),
  );
  await Promise.resolve();
  expect(beginStreamingMutation).toHaveBeenCalledOnce();

  let handoffResolved = false;
  const handoff = runtime.quiesceForegroundTxTimeHighWater().then((value) => {
    handoffResolved = true;
    return value;
  });
  await Promise.resolve();
  expect(handoffResolved).toBe(false);

  releaseSource();
  await stream;
  expect(await handoff).toBe(42n);
  await runtime.close();
});

it("waits for a failed stream's native abort before foreground handoff", async () => {
  let releaseSource!: () => void;
  const sourceGate = new Promise<void>((resolve) => {
    releaseSource = resolve;
  });
  let releaseAbort!: () => void;
  const abortGate = new Promise<boolean>((resolve) => {
    releaseAbort = () => resolve(true);
  });
  const abort = vi.fn(() => abortGate);
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          foregroundTxTimeHighWater: () => 7n,
          beginStreamingMutation: () => ({
            push: () => undefined,
            finish: () => fakeWrite(),
            abort,
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

  const stream = runtime.streamingMutation(
    "insert",
    "todos",
    {},
    "title",
    (async function* () {
      await sourceGate;
      yield "partial upload";
      throw new Error("source cancelled");
    })(),
  );
  await Promise.resolve();
  const handoff = runtime.quiesceForegroundTxTimeHighWater();
  releaseSource();
  await vi.waitFor(() => expect(abort).toHaveBeenCalledOnce());

  let handoffResolved = false;
  void handoff.then(() => {
    handoffResolved = true;
  });
  await Promise.resolve();
  expect(handoffResolved).toBe(false);

  releaseAbort();
  await expect(stream).rejects.toThrow("source cancelled");
  await expect(handoff).resolves.toBe(7n);
  await runtime.close();
});

it("does not let a concurrent close preempt foreground HLC capture", async () => {
  const order: string[] = [];
  let releaseSource!: () => void;
  const sourceGate = new Promise<void>((resolve) => {
    releaseSource = resolve;
  });
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          foregroundTxTimeHighWater: () => {
            order.push("high-water");
            return 42n;
          },
          beginStreamingMutation: () => ({
            push: () => undefined,
            finish: () => fakeWrite(),
            abort: () => true,
          }),
          tick: () => undefined,
          close: () => {
            order.push("close");
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

  const stream = runtime.streamingMutation(
    "insert",
    "todos",
    {},
    "title",
    (async function* () {
      await sourceGate;
      yield "finish before close";
    })(),
  );
  await Promise.resolve();
  const handoff = runtime.quiesceForegroundTxTimeHighWater();
  const close = runtime.close();
  releaseSource();
  await stream;
  await expect(handoff).resolves.toBe(42n);
  await close;
  expect(order).toEqual(["high-water", "close"]);
});

it("awaits the binding-owned native close promise", async () => {
  let releaseClose!: () => void;
  const closeGate = new Promise<void>((resolve) => {
    releaseClose = resolve;
  });
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          foregroundTxTimeHighWater: () => 0n,
          tick: () => undefined,
          close: () => closeGate,
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

  let settled = false;
  const close = runtime.close().then(() => {
    settled = true;
  });
  await Promise.resolve();
  expect(settled).toBe(false);
  releaseClose();
  await close;
  expect(settled).toBe(true);
});

it("waits for every concurrently admitted stream before foreground handoff", async () => {
  let highWater = 7n;
  let releaseFirst!: () => void;
  let releaseSecond!: () => void;
  const firstGate = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const secondGate = new Promise<void>((resolve) => {
    releaseSecond = resolve;
  });
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          foregroundTxTimeHighWater: () => highWater,
          beginStreamingMutation: () => ({
            push: () => undefined,
            finish: () => {
              highWater += 1n;
              return fakeWrite();
            },
            abort: () => true,
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
  const makeSource = (gate: Promise<void>, value: string) =>
    (async function* () {
      await gate;
      yield value;
    })();
  const first = runtime.streamingMutation(
    "insert",
    "todos",
    {},
    "title",
    makeSource(firstGate, "one"),
  );
  const second = runtime.streamingMutation(
    "insert",
    "todos",
    {},
    "title",
    makeSource(secondGate, "two"),
  );
  await Promise.resolve();
  const handoff = runtime.quiesceForegroundTxTimeHighWater();
  let handoffResolved = false;
  void handoff.then(() => {
    handoffResolved = true;
  });

  releaseFirst();
  await first;
  await Promise.resolve();
  expect(handoffResolved).toBe(false);

  releaseSecond();
  await second;
  await expect(handoff).resolves.toBe(9n);
  await runtime.close();
});

function uuidBytes(value: string): Uint8Array {
  const hex = value.replaceAll("-", "");
  const bytes = new Uint8Array(16);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}

it("stages authenticated client mutations through the optimistic local core path", () => {
  const staged: string[] = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          insert: (table: string, _cells: Uint8Array, options?: { rowId?: Uint8Array }) => {
            staged.push(table);
            return { ...fakeWrite(), rowId: options?.rowId ?? new Uint8Array(16) };
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
    { title: { type: "Text", value: "optimistic" } },
    JSON.stringify({
      session: {
        issuer: "https://issuer.example",
        user_id: "00000000-0000-0000-0000-0000000000a1",
      },
    }),
    "00000000-0000-0000-0000-000000000001",
  );

  expect(staged).toEqual(["todos"]);
});

it("preserves logical user columns that share names with native storage metadata", () => {
  const collisionSchema = {
    records: {
      columns: ["schema_version", "parents", "authored_columns"].map((name) => ({
        name,
        column_type: { type: "Text" } as const,
        nullable: false,
      })),
    },
  } satisfies WasmSchema;
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          insert: (_table: string, _cells: Uint8Array, options?: { rowId?: Uint8Array }) => ({
            ...fakeWrite(),
            rowId: options?.rowId ?? new Uint8Array(16),
          }),
          tick: () => undefined,
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    collisionSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );

  const inserted = runtime.insert("records", {
    schema_version: { type: "Text", value: "v1" },
    parents: { type: "Text", value: "root" },
    authored_columns: { type: "Text", value: "all" },
  });

  expect(inserted.values).toEqual([
    { type: "Text", value: "v1" },
    { type: "Text", value: "root" },
    { type: "Text", value: "all" },
  ]);
});

it("uses identity-aware core txs only on an explicit trusted-serving host", () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const authors: string[] = [];
  const staged: string[] = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          beginTransaction: (
            _openTransactionId: string,
            _kind: "mergeable" | "exclusive",
            author?: Uint8Array,
          ) => {
            if (author) authors.push(new TextDecoder().decode(author));
          },
          insertInTransaction: (
            _openTransactionId: string,
            table: string,
            _cells: Uint8Array,
            options?: { rowId?: Uint8Array },
          ) => {
            staged.push(table);
            return options?.rowId ?? new Uint8Array(16);
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

  const tx = beginTestBatch(runtime, alice);
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "session tx" } },
    JSON.stringify({
      transaction_id: tx,
      session: { issuer: "https://issuer.example", user_id: alice },
    }),
    "00000000-0000-0000-0000-000000000001",
  );

  expect(authors).toEqual(['["https://issuer.example","00000000-0000-0000-0000-0000000000a1"]']);
  expect(staged).toEqual(["todos"]);
});

it("binds a trusted-serving exclusive transaction to its opening identity", () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const issuer = "https://issuer.example";
  const beganAs: string[] = [];
  const policySchema = {
    todos: {
      ...testSchema.todos,
      policies: {},
    },
  } satisfies WasmSchema;
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () => {
        const db = fakeDb({
          all: () => encodeRows([]),
          tick: () => undefined,
        }) as unknown as {
          beginTransaction(
            openTransactionId: string,
            kind: "mergeable" | "exclusive",
            author?: Uint8Array,
          ): void;
        };
        const begin = db.beginTransaction.bind(db);
        db.beginTransaction = (openTransactionId, kind, author) => {
          beganAs.push(author === undefined ? "none" : new TextDecoder().decode(author));
          begin(openTransactionId, kind, author);
        };
        return db;
      },
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    policySchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
    { readAuthorizationHost: "trusted-serving" },
  );

  const tx = createOpenTransactionId();
  runtime.beginTransaction("exclusive", tx, JSON.stringify({ issuer, user_id: alice }));
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "session-scoped exclusive write" } },
    JSON.stringify({ transaction_id: tx, session: { issuer, user_id: alice } }),
    "00000000-0000-0000-0000-000000000001",
  );

  expect(beganAs).toEqual([`["${issuer}","${alice}"]`]);
  expect(() =>
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "wrong subject" } },
      JSON.stringify({
        transaction_id: tx,
        session: { issuer, user_id: "00000000-0000-0000-0000-0000000000b2" },
      }),
      "00000000-0000-0000-0000-000000000002",
    ),
  ).toThrow("Native runtime exclusive transaction cannot mix write identities");
});

it("uses the opening identity for trusted-serving transaction reads", async () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const issuer = "https://issuer.example";
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: (
            _query: object,
            _opts: unknown,
            receivedTransactionId: string,
            receivedIdentity: Uint8Array,
            receivedClaims: unknown,
          ) => {
            expect(receivedTransactionId).toBe(transactionId);
            expect(new TextDecoder().decode(receivedIdentity)).toBe(`["${issuer}","${alice}"]`);
            expect(receivedClaims).toMatchObject({ team: "opening-team" });
            return encodeRows([
              {
                table: "todos",
                rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                title: "Alice's pending row",
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

  const transactionId = createOpenTransactionId();
  runtime.beginTransaction(
    "exclusive",
    transactionId,
    JSON.stringify({ issuer, user_id: alice, claims: { team: "opening-team" } }),
  );

  await expect(
    runtime.query(
      JSON.stringify({ table: "todos" }),
      JSON.stringify({
        issuer,
        user_id: "00000000-0000-0000-0000-0000000000b2",
        claims: { team: "later-team" },
      }),
      "local",
      JSON.stringify({ transaction_id: transactionId }),
    ),
  ).resolves.toEqual([
    {
      table: "todos",
      id: "00000000-0000-0000-0000-000000000001",
      values: [{ type: "Text", value: "Alice's pending row" }],
    },
  ]);
});

it("rejects a duplicate live OpenTransactionId without replacing its staged transaction", () => {
  const staged: string[] = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          insertInTransaction: (
            _openTransactionId: string,
            table: string,
            _cells: Uint8Array,
            options?: { rowId?: Uint8Array },
          ) => {
            staged.push(table);
            return options?.rowId ?? new Uint8Array(16);
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
  const id = createOpenTransactionId();
  runtime.beginTransaction("mergeable", id);
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "first" } },
    JSON.stringify({ transaction_id: id }),
  );

  expect(() => runtime.beginTransaction("mergeable", id)).toThrow(
    `Begin transaction failed: transaction ${id} has already been opened`,
  );
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "second" } },
    JSON.stringify({ transaction_id: id }),
  );

  expect(staged).toEqual(["todos", "todos"]);
});

it("commits empty exclusive transactions, rejects empty mergeable transactions, and rejects unknown waits", async () => {
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () => fakeDb({}),
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
  const emptyMergeable = createOpenTransactionId();
  runtime.beginTransaction("mergeable", emptyMergeable);
  expect(() => runtime.commitTransaction(emptyMergeable)).toThrow(
    "empty mergeable transaction has no committed unit; roll it back instead",
  );
  await runtime.rollbackTransaction(emptyMergeable);

  const openTransactionId = createOpenTransactionId();
  runtime.beginTransaction("exclusive", openTransactionId);
  const committed = await runtime.commitTransaction(openTransactionId);
  expect(committed).toBe("00000000000070008000000000000001");
  await expect(
    runtime.waitForTransaction("00000000000070008000000000000002" as TxId, "local"),
  ).rejects.toThrow(
    "Wait for transaction failed: unknown transaction 00000000000070008000000000000002",
  );

  const reopened = new NativeRuntimeAdapter(
    {
      openMemory: () => fakeDb({}),
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
  await expect(reopened.waitForTransaction(committed, "local")).rejects.toThrow(
    `Wait for transaction failed: unknown transaction ${committed}`,
  );
});

it("keeps a transaction open through a failed commit, then permits rollback", async () => {
  const nativeRollback = vi.fn();
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          commitTransaction: () => {
            throw new Error("injected commit failure");
          },
          rollbackTransaction: nativeRollback,
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
  const openBatchId = beginTestBatch(runtime);
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "rollback after failed commit" } },
    JSON.stringify({ transaction_id: openBatchId }),
  );

  expect(() => runtime.commitTransaction(openBatchId)).toThrow("injected commit failure");

  await expect(runtime.rollbackTransaction(openBatchId)).resolves.toBe(true);
  expect(nativeRollback).toHaveBeenCalledOnce();
});

it("closing a schema view does not close its owner's transaction", () => {
  const nativeDb = fakeDb({});
  Object.assign(nativeDb, { registerSchema: () => nativeDb });
  const owner = new NativeRuntimeAdapter(
    {
      openMemory: () => nativeDb,
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
  const view = owner.registerSchemaView(testSchema);
  const openBatchId = beginTestBatch(view);
  view.insert(
    "todos",
    { title: { type: "Text", value: "view closes before parent batch" } },
    JSON.stringify({ transaction_id: openBatchId }),
  );

  void view.close();
  expect(() => owner.commitTransaction(openBatchId)).not.toThrow();
});

it("binds the trusted-serving identity when an exclusive transaction begins", () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const observed: Array<{ phase: "begin" | "commit"; author?: string }> = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          beginTransaction: (
            _openTransactionId: string,
            _kind: "mergeable" | "exclusive",
            author?: Uint8Array,
          ) =>
            observed.push({
              phase: "begin",
              author: author && new TextDecoder().decode(author),
            }),
          insertInTransaction: (
            _openTransactionId: string,
            _table: string,
            _cells: Uint8Array,
            options?: { rowId?: Uint8Array },
          ) => options?.rowId ?? new Uint8Array(16),
          commitTransaction: (
            _openTransactionId: string,
            _kind?: "mergeable" | "exclusive",
            author?: Uint8Array,
          ) => {
            observed.push({ phase: "commit", author: author && new TextDecoder().decode(author) });
            return fakeWrite();
          },
        }),
      openBrowser: async () => {
        throw new Error("not used");
      },
    } as never,
    testSchema,
    TEST_RUNTIME_AUTHOR,
    TEST_RUNTIME_AUTHOR,
    1,
    true,
    { readAuthorizationHost: "trusted-serving" },
  );

  const openTransactionId = createOpenTransactionId();
  runtime.beginTransaction(
    "exclusive",
    openTransactionId,
    JSON.stringify({ issuer: "https://issuer.example", user_id: alice }),
  );
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "exclusive" } },
    JSON.stringify({
      transaction_id: openTransactionId,
      session: { issuer: "https://issuer.example", user_id: alice },
    }),
  );
  runtime.commitTransaction(openTransactionId);

  expect(observed).toEqual([
    { phase: "begin", author: `["https://issuer.example","${alice}"]` },
    // The native core persists the bound subject; commit must not accept a replacement.
    { phase: "commit", author: undefined },
  ]);
});

it("emits an onMutationError event for an unawaited rejected write", async () => {
  const txId = "00000000000070008000000000000042" as TxId;
  let mutationErrorCallback: ((event: MutationErrorEvent) => void) | undefined;
  const write = {
    txId,
    payload: new Uint8Array(),
    wait: async () => undefined,
    writeState: () => ({}),
  };
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          insert: () => write,
          onMutationError: (callback: (event: MutationErrorEvent) => void) => {
            mutationErrorCallback = callback;
          },
        }),
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  const listener = vi.fn();
  runtime.onMutationError(listener);

  runtime.insert(
    "todos",
    { title: { type: "Text", value: "rejected" } },
    null,
    "00000000-0000-0000-0000-000000000042",
  );
  mutationErrorCallback?.({
    code: "permission_denied",
    reason: "Write rejected by server authorization",
    transaction: {
      transactionId: txId,
      kind: "mergeable",
      sealed: true,
      latestSettlement: {
        kind: "rejected",
        transactionId: txId,
        code: "permission_denied",
        reason: "Write rejected by server authorization",
      },
    },
  });

  await vi.waitFor(() => expect(listener).toHaveBeenCalledTimes(1));
  expect(listener).toHaveBeenCalledWith({
    code: "permission_denied",
    reason: "Write rejected by server authorization",
    transaction: {
      transactionId: txId,
      kind: "mergeable",
      sealed: true,
      latestSettlement: {
        kind: "rejected",
        transactionId: txId,
        code: "permission_denied",
        reason: "Write rejected by server authorization",
      },
    },
  });
});

it("does not emit onMutationError when an active wait handles the rejection", async () => {
  const txId = "00000000000070008000000000000043" as TxId;
  let rejected = false;
  const stateChangeWaiters: Array<() => void> = [];
  const nextWriteStateChange = () =>
    new Promise<void>((resolve) => {
      stateChangeWaiters.push(resolve);
    });
  const write = {
    txId,
    payload: new Uint8Array(),
    wait: async () => {
      await nextWriteStateChange();
      if (rejected) throw new Error("WriteRejected: AuthorizationDenied");
    },
    writeState: () => ({}),
  };
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          insert: () => write,
          onMutationError: () => undefined,
        }),
    } as never,
    testSchema,
    new Uint8Array(16),
    TEST_RUNTIME_AUTHOR,
    1,
    true,
  );
  const listener = vi.fn();
  runtime.onMutationError(listener);

  runtime.insert(
    "todos",
    { title: { type: "Text", value: "rejected" } },
    null,
    "00000000-0000-0000-0000-000000000043",
  );
  const wait = runtime.waitForTransaction(txId, "edge");
  await Promise.resolve();
  rejected = true;
  stateChangeWaiters.splice(0).forEach((resolve) => resolve());

  await expect(wait).rejects.toMatchObject({
    kind: "rejected",
    transactionId: txId,
    code: "permission_denied",
  });
  await new Promise((resolve) => setTimeout(resolve, 0));
  expect(listener).not.toHaveBeenCalled();
});

it("passes caller-supplied updatedAt into staged mergeable transaction writes", () => {
  const updatedAt = 1_704_067_200_123;
  const expectedUpdatedAtMs = updatedAt;
  const staged: Array<{ op: string; updatedAtMs: number | null | undefined }> = [];
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          insertInTransaction: (
            _openTransactionId: string,
            _table: string,
            _cells: Uint8Array,
            options?: { rowId?: Uint8Array; updatedAtMs?: number },
          ) => {
            staged.push({ op: "insert", updatedAtMs: options?.updatedAtMs });
            return options?.rowId ?? new Uint8Array(16);
          },
          updateInTransaction: (
            _openTransactionId: string,
            _table: string,
            _rowId: Uint8Array,
            _patch: Uint8Array,
            options?: { updatedAtMs?: number },
          ) => staged.push({ op: "update", updatedAtMs: options?.updatedAtMs }),
          upsertInTransaction: (
            _openTransactionId: string,
            _table: string,
            _rowId: Uint8Array,
            _cells: Uint8Array,
            options?: { updatedAtMs?: number },
          ) => staged.push({ op: "upsert", updatedAtMs: options?.updatedAtMs }),
          restoreInTransaction: (
            _openTransactionId: string,
            _table: string,
            _rowId: Uint8Array,
            _cells: Uint8Array,
            options?: { updatedAtMs?: number },
          ) => staged.push({ op: "restore", updatedAtMs: options?.updatedAtMs }),
          deleteInTransaction: (
            _openTransactionId: string,
            _table: string,
            _rowId: Uint8Array,
            options?: { updatedAtMs?: number },
          ) => staged.push({ op: "delete", updatedAtMs: options?.updatedAtMs }),
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

  const tx = beginTestBatch(runtime);
  const context = JSON.stringify({ transaction_id: tx, updated_at: updatedAt });
  const rowId = "00000000-0000-0000-0000-000000000001";
  runtime.insert("todos", { title: { type: "Text", value: "inserted" } }, context, rowId);
  runtime.update("todos", rowId, { title: { type: "Text", value: "updated" } }, context);
  runtime.upsert("todos", rowId, { title: { type: "Text", value: "upserted" } }, context);
  runtime.restore("todos", rowId, { title: { type: "Text", value: "restored" } }, context);
  runtime.delete("todos", rowId, context);

  expect(staged).toEqual([
    { op: "insert", updatedAtMs: expectedUpdatedAtMs },
    { op: "update", updatedAtMs: expectedUpdatedAtMs },
    { op: "upsert", updatedAtMs: expectedUpdatedAtMs },
    { op: "restore", updatedAtMs: expectedUpdatedAtMs },
    { op: "delete", updatedAtMs: expectedUpdatedAtMs },
  ]);
});

it("preserves the full branch view for staged mergeable upserts", () => {
  let received: { head?: unknown; base?: unknown } | undefined;
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
          upsertInTransaction: (
            _openTransactionId: string,
            _table: string,
            _rowId: Uint8Array,
            _cells: Uint8Array,
            options?: { head?: unknown; base?: unknown },
          ) => {
            received = options;
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
  const head = { values: { workspace: [15, 14] } };
  const base = { Current: { values: { workspace: [15, 2] } } };
  const tx = beginTestBatch(runtime);

  runtime.upsert(
    "todos",
    "00000000-0000-0000-0000-000000000001",
    { title: { type: "Text", value: "upserted" } },
    JSON.stringify({ transaction_id: tx, branch_view: { head, base } }),
  );

  expect(received).toMatchObject({ head, base });
});

it("rejects mixed identities within one trusted-serving mergeable transaction", () => {
  const alice = "00000000-0000-0000-0000-0000000000a1";
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: () => encodeRows([]),
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

  const tx = beginTestBatch(runtime, alice);
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "one" } },
    JSON.stringify({
      transaction_id: tx,
      session: { issuer: "https://issuer.example", user_id: alice },
    }),
    "00000000-0000-0000-0000-000000000001",
  );

  expect(() =>
    runtime.insert(
      "todos",
      { title: { type: "Text", value: "two" } },
      JSON.stringify({
        transaction_id: tx,
        session: {
          issuer: "https://issuer.example",
          user_id: "00000000-0000-0000-0000-0000000000b2",
        },
      }),
      "00000000-0000-0000-0000-000000000002",
    ),
  ).toThrow("Native runtime mergeable transaction cannot mix write identities");
});

it("keeps session-scoped transaction reads on the client-local native method", async () => {
  let transactionReads = 0;
  const runtime = new NativeRuntimeAdapter(
    {
      openMemory: () =>
        fakeDb({
          all: (
            _query: object,
            _opts: unknown,
            receivedTransactionId: string,
            receivedIdentity: Uint8Array,
          ) => {
            expect(receivedTransactionId).toBe(transactionId);
            expect(receivedIdentity).toBeUndefined();
            transactionReads += 1;
            return encodeRows([
              {
                table: "todos",
                rowId: uuidBytes("00000000-0000-0000-0000-000000000001"),
                title: "alice pending",
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

  const transactionId = beginTestBatch(runtime, "00000000-0000-0000-0000-0000000000a1");
  runtime.insert(
    "todos",
    { title: { type: "Text", value: "alice pending" } },
    JSON.stringify({
      transaction_id: transactionId,
      session: {
        issuer: "https://issuer.example",
        user_id: "00000000-0000-0000-0000-0000000000a1",
      },
    }),
    "00000000-0000-0000-0000-000000000001",
  );

  await expect(
    runtime.query(
      JSON.stringify({ table: "todos" }),
      JSON.stringify({
        issuer: "https://issuer.example",
        user_id: "00000000-0000-0000-0000-0000000000b2",
      }),
      "local",
      JSON.stringify({ transaction_id: transactionId }),
    ),
  ).resolves.toEqual([
    {
      table: "todos",
      id: "00000000-0000-0000-0000-000000000001",
      values: [{ type: "Text", value: "alice pending" }],
    },
  ]);
  await expect(
    runtime.query(
      JSON.stringify({ table: "todos" }),
      JSON.stringify({
        issuer: "https://issuer.example",
        user_id: "00000000-0000-0000-0000-0000000000a1",
      }),
      "local",
      JSON.stringify({ transaction_id: transactionId }),
    ),
  ).resolves.toEqual([
    {
      table: "todos",
      id: "00000000-0000-0000-0000-000000000001",
      values: [{ type: "Text", value: "alice pending" }],
    },
  ]);
  expect(transactionReads).toBe(2);
});
