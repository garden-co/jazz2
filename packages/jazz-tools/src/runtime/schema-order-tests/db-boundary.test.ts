import { describe, expect, it, vi } from "vitest";
import { Db, createDbFromClient, type QueryBuilder, type TableProxy } from "../db.js";
import type { InsertValues, WasmRow, WasmSchema } from "../../drivers/types.js";
import { JazzClient, WriteHandle, WriteResult, type DirectInsertResult } from "../client.js";

class TestDb extends Db {
  constructor(private readonly testClient: JazzClient) {
    super({ appId: "schema-order-test" }, null);
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return this.testClient;
  }
}

function makeHandleClient(): JazzClient {
  return {
    waitForPersistedBatch: vi.fn(async () => undefined),
  } as unknown as JazzClient;
}

function makeWriteResult(row: DirectInsertResult): WriteResult<DirectInsertResult> {
  return new WriteResult(row, row.batchId, makeHandleClient());
}

function makeWriteHandle(batchId: string): WriteHandle {
  return new WriteHandle(batchId, makeHandleClient());
}

describe("Db runtime schema order", () => {
  it("uses the generated schema order for inserts when the runtime schema is sorted", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const runtimeSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "title", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const create = vi.fn<(...args: [string, InsertValues]) => WriteResult<DirectInsertResult>>(() =>
      makeWriteResult({
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        batchId: "batch-schema-order-runtime",
      }),
    );
    const client = {
      getSchema: () => new Map(Object.entries(runtimeSchema)),
      create,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    const { value: row } = db.insert(table, { title: "Buy milk", done: false });

    expect(create).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      undefined,
    );
    expect(row).toEqual({
      id: "todo-1",
      title: "Buy milk",
      done: false,
    });
  });

  it("uses the generated schema order when transforming query results", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const runtimeSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
          { name: "title", column_type: { type: "Text" }, nullable: false },
        ],
      },
    };
    const query = vi.fn<(...args: [string, object?]) => Promise<WasmRow[]>>(async () => [
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Sorted title" },
          { type: "Boolean", value: true },
        ],
      },
    ]);
    const client = {
      getSchema: () => new Map(Object.entries(runtimeSchema)),
      query,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const builder = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _build: () =>
        JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        }),
    } satisfies QueryBuilder<{ id: string; title: string; done: boolean }>;

    const rows = await db.all(builder);

    expect(rows).toEqual([
      {
        id: "todo-1",
        title: "Sorted title",
        done: true,
      },
    ]);
  });

  it("does not fetch runtime schema for client-backed queries that stay within the declared schema", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const runtime = {
      returnsDeclaredSchemaRows: true,
      query: vi.fn(async () => [
        {
          id: "todo-1",
          values: [
            { type: "Text", value: "No schema fetch" },
            { type: "Boolean", value: true },
          ],
        },
      ]),
      getSchema: vi.fn(() => new Map()),
      getSchemaHash: vi.fn(() => "runtime-schema-hash"),
      onSyncMessageReceived: vi.fn(),
      createSubscription: vi.fn(),
      executeSubscription: vi.fn(),
      unsubscribe: vi.fn(),
      addServer: vi.fn(),
      removeServer: vi.fn(),
      addClient: vi.fn(() => "client-id"),
    };
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "client-backed-schema-order",
      schema: generatedSchema,
    });
    const db = createDbFromClient({ appId: "client-backed-schema-order" }, client);
    const builder = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _build: () =>
        JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        }),
    } satisfies QueryBuilder<{ id: string; title: string; done: boolean }>;

    await expect(db.all(builder)).resolves.toEqual([
      {
        id: "todo-1",
        title: "No schema fetch",
        done: true,
      },
    ]);
    expect(runtime.getSchema).not.toHaveBeenCalled();
    expect(runtime.getSchemaHash).not.toHaveBeenCalled();
  });

  it("falls back to the generated schema when the runtime schema is missing a table", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const create = vi.fn<(...args: [string, InsertValues]) => WriteResult<DirectInsertResult>>(() =>
      makeWriteResult({
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        batchId: "batch-schema-order-generated",
      }),
    );
    const client = {
      getSchema: () => new Map(),
      create,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    const { value: row } = db.insert(table, { title: "Buy milk", done: false });

    expect(create).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      undefined,
    );
    expect(row).toEqual({
      id: "todo-1",
      title: "Buy milk",
      done: false,
    });
  });

  it("forwards a caller-supplied create id to the runtime client", () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const create = vi.fn<
      (...args: [string, InsertValues, { id: string }]) => WriteResult<DirectInsertResult>
    >(() =>
      makeWriteResult({
        id: externalId,
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        batchId: "batch-1",
      }),
    );
    const client = {
      getSchema: () => new Map(),
      create,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    const row = db.insert(table, { title: "Buy milk", done: false }, { id: externalId });

    expect(create).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      { id: externalId },
    );
    expect(row.value).toEqual({
      id: externalId,
      title: "Buy milk",
      done: false,
    });
  });

  it("forwards caller-supplied upsert ids to the runtime client", () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const upsert = vi.fn<(...args: [string, InsertValues, { id: string }]) => WriteHandle>(() =>
      makeWriteHandle("batch-upsert"),
    );
    const client = {
      getSchema: () => new Map(),
      upsert,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    expect(db.upsert(table, { title: "Buy milk", done: false }, { id: externalId })).toMatchObject({
      batchId: "batch-upsert",
    });

    expect(upsert).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      { id: externalId },
    );
  });

  it("forwards custom updatedAt overrides on insert, update, and upsert", () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const updatedAt = 1_764_000_000_000_000;
    const create = vi.fn<
      (...args: [string, InsertValues, { updatedAt: number }]) => WriteResult<DirectInsertResult>
    >(() =>
      makeWriteResult({
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        batchId: "batch-1",
      }),
    );
    const update = vi.fn<(...args: [string, InsertValues, { updatedAt: number }]) => WriteHandle>(
      () => makeWriteHandle("batch-update"),
    );
    const upsert = vi.fn<
      (...args: [string, InsertValues, { id: string; updatedAt: number }]) => WriteHandle
    >(() => makeWriteHandle("batch-upsert"));
    const client = {
      getSchema: () => new Map(),
      create,
      update,
      upsert,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    db.insert(table, { title: "Buy milk", done: false }, { updatedAt });
    db.update(table, "todo-1", { done: true }, { updatedAt });
    db.upsert(table, { done: true }, { id: "todo-1", updatedAt });

    expect(create).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      { updatedAt },
    );
    expect(update).toHaveBeenCalledWith(
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      { updatedAt },
    );
    expect(upsert).toHaveBeenCalledWith(
      "todos",
      {
        done: { type: "Boolean", value: true },
      },
      { id: "todo-1", updatedAt },
    );
  });

  it("forwards custom updatedAt overrides through client-backed db mutations", () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const updatedAt = 1_764_000_000_000_000;
    const createHandleInternal = vi.fn(() =>
      makeWriteResult({
        id: "todo-1",
        values: [
          { type: "Text", value: "Buy milk" },
          { type: "Boolean", value: false },
        ],
        batchId: "batch-insert",
      }),
    );
    const updateHandleInternal = vi.fn<() => WriteHandle>(() => makeWriteHandle("batch-update"));
    const upsertHandleInternal = vi.fn<() => WriteHandle>(() => makeWriteHandle("batch-upsert"));
    const client = {
      getSchema: () => new Map(Object.entries(generatedSchema)),
      createHandleInternal,
      updateHandleInternal,
      upsertHandleInternal,
    } as unknown as JazzClient;
    const db = createDbFromClient({ appId: "client-backed-db-test" }, client);
    const table = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _initType: {} as { title: string; done: boolean },
    } satisfies TableProxy<
      { id: string; title: string; done: boolean },
      { title: string; done: boolean }
    >;

    db.insert(table, { title: "Buy milk", done: false }, { updatedAt });
    db.update(table, "todo-1", { done: true }, { updatedAt });
    db.upsert(table, { done: true }, { id: "todo-1", updatedAt });

    expect(createHandleInternal).toHaveBeenCalledWith(
      "todos",
      {
        title: { type: "Text", value: "Buy milk" },
        done: { type: "Boolean", value: false },
      },
      undefined,
      undefined,
      { updatedAt },
    );
    expect(updateHandleInternal).toHaveBeenCalledWith(
      "todo-1",
      {
        done: { type: "Boolean", value: true },
      },
      undefined,
      undefined,
      undefined,
      updatedAt,
    );
    expect(upsertHandleInternal).toHaveBeenCalledWith(
      "todos",
      {
        done: { type: "Boolean", value: true },
      },
      "todo-1",
      undefined,
      undefined,
      updatedAt,
    );
  });

  it("falls back to the generated schema for query results when the runtime schema is missing a table", async () => {
    const generatedSchema: WasmSchema = {
      todos: {
        columns: [
          { name: "title", column_type: { type: "Text" }, nullable: false },
          { name: "done", column_type: { type: "Boolean" }, nullable: false },
        ],
      },
    };
    const query = vi.fn<(...args: [string, object?]) => Promise<WasmRow[]>>(async () => [
      {
        id: "todo-1",
        values: [
          { type: "Text", value: "Generated title" },
          { type: "Boolean", value: true },
        ],
      },
    ]);
    const client = {
      getSchema: () => new Map(),
      query,
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const builder = {
      _table: "todos",
      _schema: generatedSchema,
      _rowType: {} as { id: string; title: string; done: boolean },
      _build: () =>
        JSON.stringify({
          table: "todos",
          conditions: [],
          includes: {},
          orderBy: [],
        }),
    } satisfies QueryBuilder<{ id: string; title: string; done: boolean }>;

    const rows = await db.all(builder);

    expect(rows).toEqual([
      {
        id: "todo-1",
        title: "Generated title",
        done: true,
      },
    ]);
  });
});
