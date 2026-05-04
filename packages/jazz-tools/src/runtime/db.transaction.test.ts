import { describe, expect, it, vi } from "vitest";
import { Db, createDbFromClient, type TableProxy } from "./db.js";
import type { WasmSchema } from "../drivers/types.js";
import {
  WriteResult,
  WriteHandle,
  type JazzClient,
  type LocalBatchRecord,
  type Row,
} from "./client.js";
import type { Session } from "./context.js";

class TestDb extends Db {
  constructor(private readonly testClient: JazzClient) {
    super({ appId: "transaction-db-test" }, null);
  }

  protected override getClient(_schema: WasmSchema): JazzClient {
    return this.testClient;
  }
}

class MultiClientDb extends Db {
  constructor(private readonly clientsBySchema: Map<WasmSchema, JazzClient>) {
    super({ appId: "transaction-db-test" }, null);
  }

  protected override getClient(schema: WasmSchema): JazzClient {
    const client = this.clientsBySchema.get(schema);
    if (!client) {
      throw new Error("missing test client for schema");
    }
    return client;
  }
}

function todoSchema(): WasmSchema {
  return {
    todos: {
      columns: [
        { name: "title", column_type: { type: "Text" }, nullable: false },
        { name: "done", column_type: { type: "Boolean" }, nullable: false },
      ],
    },
  };
}

function todoTable() {
  const schema = todoSchema();
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as { id: string; title: string; done: boolean },
    _initType: {} as { title: string; done: boolean },
  } satisfies TableProxy<
    { id: string; title: string; done: boolean },
    { title: string; done: boolean }
  >;
}

function todoQuery() {
  const schema = todoSchema();
  return {
    _table: "todos",
    _schema: schema,
    _rowType: {} as { id: string; title: string; done: boolean },
    _build() {
      return JSON.stringify({
        table: "todos",
        conditions: [],
        includes: {},
        orderBy: [],
      });
    },
  };
}

function makeLocalBatchRecord(
  batchId: string,
  mode: LocalBatchRecord["mode"] = "transactional",
): LocalBatchRecord {
  return {
    batchId,
    mode,
    sealed: false,
    latestSettlement: null,
  };
}

function makeHandleClient(mode: LocalBatchRecord["mode"] = "transactional", acknowledged = false) {
  return {
    waitForPersistedBatch: vi.fn(async () => undefined),
    localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, mode)),
    acknowledgeRejectedBatch: vi.fn(() => acknowledged),
  };
}

function makeWriteHandle(batchId: string, mode: LocalBatchRecord["mode"] = "transactional") {
  const client = makeHandleClient(mode);
  return {
    handle: new WriteHandle(batchId, client as unknown as JazzClient),
    client,
  };
}

type TestTransactionStatus = "active" | "committed" | "rolledBack";

function assertTestTransactionActive(status: TestTransactionStatus, batchId: string): void {
  if (status === "committed") {
    throw new Error(`Transaction ${batchId} is already committed`);
  }
  if (status === "rolledBack") {
    throw new Error(`Transaction ${batchId} has already been rolled back`);
  }
}

describe("Db transactions", () => {
  it("creates a typed db transaction bound by its first table operation", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-1",
      values: [
        { type: "Text", value: "Transactional" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-tx",
    } as Row;
    const committedRuntime = makeWriteHandle("batch-tx");
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-tx"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => committedRuntime.handle),
      localBatchRecord: vi.fn((batchId = "batch-tx") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-tx")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const beginTransactionInternal = vi.fn(() => runtimeTransaction);
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal,
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const tx = db.beginTransaction();
    const inserted = tx.insert(table, { title: "Transactional", done: false });
    const updated = tx.update(table, "todo-1", { done: true });
    const deleted = tx.delete(table, "todo-1");

    expect(beginTransactionInternal).toHaveBeenCalledWith();
    expect(tx.batchId()).toBe("batch-tx");
    expect(inserted).toEqual({
      id: "todo-1",
      title: "Transactional",
      done: false,
    });
    expect(runtimeTransaction.create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Transactional" },
      done: { type: "Boolean", value: false },
    });
    expect(runtimeTransaction.update).toHaveBeenCalledWith("todo-1", {
      done: { type: "Boolean", value: true },
    });
    expect(runtimeTransaction.delete).toHaveBeenCalledWith("todo-1");
    expect(updated).toBeUndefined();
    expect(deleted).toBeUndefined();
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-tx");
    await expect(committed.wait({ tier: "global" })).resolves.toBeUndefined();
    expect(committedRuntime.client.waitForPersistedBatch).toHaveBeenCalledWith(
      "batch-tx",
      "global",
    );
    expect(runtimeTransaction.commit).toHaveBeenCalledWith();
    expect(tx.localBatchRecord()).toMatchObject({ batchId: "batch-tx" });
    expect(tx.localBatchRecords()).toEqual([makeLocalBatchRecord("batch-tx")]);
    expect(tx.acknowledgeRejectedBatch()).toBe(false);
  });

  it("uses declared schemas for transaction writes without fetching runtime schema", () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-tx-fast-path",
      values: [
        { type: "Text", value: "Fast transaction" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-tx-fast-path",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-tx-fast-path"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      upsert: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => makeWriteHandle("batch-tx-fast-path").handle),
      localBatchRecord: vi.fn((batchId = "batch-tx-fast-path") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-tx-fast-path")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const getSchema = vi.fn(() => new Map(Object.entries(todoSchema())));
    const getSchemaHash = vi.fn(() => "schema-hash");
    const client = {
      getSchema,
      getSchemaHash,
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const tx = db.beginTransaction();

    tx.insert(table, { title: "Fast transaction", done: false });
    tx.upsert(table, { title: "Fast transaction upsert" }, { id: "todo-tx-fast-path" });
    tx.update(table, "todo-tx-fast-path", { done: true });

    expect(getSchema).not.toHaveBeenCalled();
    expect(getSchemaHash).not.toHaveBeenCalled();
    expect(runtimeTransaction.create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Fast transaction" },
      done: { type: "Boolean", value: false },
    });
    expect(runtimeTransaction.upsert).toHaveBeenCalledWith(
      "todos",
      { title: { type: "Text", value: "Fast transaction upsert" } },
      { id: "todo-tx-fast-path" },
    );
    expect(runtimeTransaction.update).toHaveBeenCalledWith("todo-tx-fast-path", {
      done: { type: "Boolean", value: true },
    });
  });

  it("threads session-backed db transactions through beginTransactionInternal", async () => {
    const table = todoTable();
    const session: Session = {
      user_id: "alice",
      claims: { role: "writer" },
      authMode: "external",
    };
    const runtimeRow = {
      id: "todo-2",
      values: [
        { type: "Text", value: "Session transaction" },
        { type: "Boolean", value: true },
      ],
      batchId: "batch-session-tx",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-session-tx"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => makeWriteHandle("batch-session-tx").handle),
      localBatchRecord: vi.fn((batchId = "batch-session-tx") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-session-tx")]),
      acknowledgeRejectedBatch: vi.fn(() => true),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const db = createDbFromClient(
      { appId: "client-backed-transaction" },
      runtimeClient as unknown as JazzClient,
      session,
      "alice@writer",
    );

    const tx = db.beginTransaction();
    const inserted = tx.insert(table, { title: "Session transaction", done: true });

    expect(runtimeClient.beginTransactionInternal).toHaveBeenCalledWith(session, "alice@writer");
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-session-tx");
    expect(runtimeTransaction.commit).toHaveBeenCalledWith();
    expect(inserted).toEqual({
      id: "todo-2",
      title: "Session transaction",
      done: true,
    });
  });

  it("commits a typed callback transaction and returns the callback result handle", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-callback",
      values: [
        { type: "Text", value: "Callback transaction" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-callback",
    } as Row;
    const committedRuntime = makeWriteHandle("batch-callback");
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-callback"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => committedRuntime.handle),
      localBatchRecord: vi.fn((batchId = "batch-callback") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-callback")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      waitForPersistedBatch: vi.fn(async () => undefined),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const handle = db.transaction((tx) => {
      const todo = tx.insert(table, { title: "Callback transaction", done: false });
      return todo;
    });

    expect(handle).not.toBeInstanceOf(Promise);
    expect(handle).toBeInstanceOf(WriteResult);
    expect(handle.batchId).toBe("batch-callback");
    expect(handle.value).toEqual({
      id: "todo-callback",
      title: "Callback transaction",
      done: false,
    });
    expect(runtimeTransaction.commit).toHaveBeenCalledTimes(1);
    await expect(handle.wait({ tier: "global" })).resolves.toEqual({
      id: "todo-callback",
      title: "Callback transaction",
      done: false,
    });
    expect(client.waitForPersistedBatch).toHaveBeenCalledWith("batch-callback", "global");
  });

  it("rolls back a callback transaction when the callback throws", () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-callback-thrown",
      values: [
        { type: "Text", value: "Thrown callback transaction" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-callback-thrown",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-callback-thrown"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-callback-thrown").handle),
      rollback: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-callback-thrown") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-callback-thrown")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const error = new Error("callback failed");

    expect(() =>
      db.transaction((tx) => {
        tx.insert(table, { title: "Thrown callback transaction", done: false });
        throw error;
      }),
    ).toThrow(error);

    expect(runtimeTransaction.commit).not.toHaveBeenCalled();
    expect(runtimeTransaction.rollback).toHaveBeenCalledTimes(1);
  });

  it("rolls back a callback transaction when the callback rejects", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-callback-rejected",
      values: [
        { type: "Text", value: "Rejected callback transaction" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-callback-rejected",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-callback-rejected"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-callback-rejected").handle),
      rollback: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-callback-rejected") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-callback-rejected")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const error = new Error("callback failed");

    await expect(
      db.transaction(async (tx) => {
        tx.insert(table, { title: "Rejected callback transaction", done: false });
        return Promise.reject(error);
      }),
    ).rejects.toBe(error);

    expect(runtimeTransaction.commit).not.toHaveBeenCalled();
    expect(runtimeTransaction.rollback).toHaveBeenCalledTimes(1);
  });

  it("routes typed transaction upserts through the runtime transaction", () => {
    const table = todoTable();
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-upsert-tx"),
      create: vi.fn(),
      update: vi.fn(),
      upsert: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-upsert-tx").handle),
      localBatchRecord: vi.fn((batchId = "batch-upsert-tx") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-upsert-tx")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    db.transaction((tx) => {
      tx.upsert(table, { title: "Updated in transaction" }, { id: "todo-upsert-tx" });
    });

    expect(runtimeTransaction.upsert).toHaveBeenCalledWith(
      "todos",
      { title: { type: "Text", value: "Updated in transaction" } },
      { id: "todo-upsert-tx" },
    );
  });

  it("commits a typed async callback transaction after the callback resolves", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-async-callback",
      values: [
        { type: "Text", value: "Async callback transaction" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-async-callback",
    } as Row;
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-async-callback"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => makeWriteHandle("batch-async-callback").handle),
      localBatchRecord: vi.fn((batchId = "batch-async-callback") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-async-callback")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      waitForPersistedBatch: vi.fn(async () => undefined),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const handlePromise = db.transaction(async (tx) => {
      const todo = tx.insert(table, { title: "Async callback transaction", done: false });
      expect(runtimeTransaction.commit).not.toHaveBeenCalled();
      return todo;
    });

    expect(handlePromise).toBeInstanceOf(Promise);
    const handle = await handlePromise;
    expect(handle).toBeInstanceOf(WriteResult);
    expect(handle.batchId).toBe("batch-async-callback");
    expect(handle.value).toEqual({
      id: "todo-async-callback",
      title: "Async callback transaction",
      done: false,
    });
    expect(runtimeTransaction.commit).toHaveBeenCalledTimes(1);
    await expect(handle.wait({ tier: "global" })).resolves.toEqual({
      id: "todo-async-callback",
      title: "Async callback transaction",
      done: false,
    });
    expect(client.waitForPersistedBatch).toHaveBeenCalledWith("batch-async-callback", "global");
  });

  it("rejects db transaction writes after commit", () => {
    const table = todoTable();
    const runtimeRow = {
      id: "todo-closed",
      values: [
        { type: "Text", value: "Closed" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-closed",
    } as Row;
    let status: TestTransactionStatus = "active";
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-closed"),
      create: vi.fn(() => {
        assertTestTransactionActive(status, "batch-closed");
        return runtimeRow;
      }),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => {
        assertTestTransactionActive(status, "batch-closed");
        status = "committed";
        return makeWriteHandle("batch-closed").handle;
      }),
      localBatchRecord: vi.fn((batchId = "batch-closed") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-closed")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const db = createDbFromClient(
      { appId: "client-backed-transaction" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();
    tx.insert(table, { title: "Closed", done: false });
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-closed");

    expect(() => tx.insert(table, { title: "Nope", done: false })).toThrow(/committed/i);
    expect(runtimeTransaction.create).toHaveBeenCalledTimes(2);
  });

  it("throws when committing a db transaction before any actions", () => {
    const beginTransactionInternal = vi.fn();
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal,
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const tx = db.beginTransaction();

    expect(() => tx.commit()).toThrow(
      "DbTransaction.commit() requires at least one table operation first",
    );
    expect(beginTransactionInternal).not.toHaveBeenCalled();
  });

  it("supports typed reads scoped to the open transaction", async () => {
    const table = todoTable();
    const query = todoQuery();
    const runtimeRow: Row = {
      id: "todo-read-1",
      values: [
        { type: "Text", value: "Transactional read" },
        { type: "Boolean", value: false },
      ],
    };
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-read"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(),
      delete: vi.fn(),
      query: vi.fn(async () => [runtimeRow]),
      commit: vi.fn(() => makeWriteHandle("batch-read").handle),
      localBatchRecord: vi.fn((batchId = "batch-read") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-read")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const tx = db.beginTransaction();
    tx.insert(table, { title: "Transactional read", done: false });

    await expect(tx.all(query)).resolves.toEqual([
      {
        id: "todo-read-1",
        title: "Transactional read",
        done: false,
      },
    ]);
    await expect(tx.one(query)).resolves.toEqual({
      id: "todo-read-1",
      title: "Transactional read",
      done: false,
    });

    expect(runtimeTransaction.query).toHaveBeenCalledTimes(2);
  });

  it("rejects db transaction reads after commit", async () => {
    const table = todoTable();
    const query = todoQuery();
    let status: TestTransactionStatus = "active";
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-read-closed"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      query: vi.fn(async () => {
        assertTestTransactionActive(status, "batch-read-closed");
        return [];
      }),
      commit: vi.fn(() => {
        assertTestTransactionActive(status, "batch-read-closed");
        status = "committed";
        return makeWriteHandle("batch-read-closed").handle;
      }),
      localBatchRecord: vi.fn((batchId = "batch-read-closed") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-read-closed")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const db = createDbFromClient(
      { appId: "client-backed-transaction" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();
    tx.update(table, "todo-read-closed", { done: false });
    const committed = tx.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-read-closed");

    await expect(tx.all(query)).rejects.toThrow(/committed/i);
    expect(runtimeTransaction.query).toHaveBeenCalledTimes(1);
  });

  it("rolls back db transactions without committing the underlying batch", () => {
    const table = todoTable();
    let status: TestTransactionStatus = "active";
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-rollback"),
      create: vi.fn(() => {
        assertTestTransactionActive(status, "batch-rollback");
        return {} as Row;
      }),
      update: vi.fn(() => {
        assertTestTransactionActive(status, "batch-rollback");
      }),
      delete: vi.fn(),
      commit: vi.fn(() => {
        assertTestTransactionActive(status, "batch-rollback");
        status = "committed";
        return makeWriteHandle("batch-rollback").handle;
      }),
      rollback: vi.fn(() => {
        assertTestTransactionActive(status, "batch-rollback");
        status = "rolledBack";
      }),
      localBatchRecord: vi.fn((batchId = "batch-rollback") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-rollback")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const db = createDbFromClient(
      { appId: "client-backed-rollback" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();
    tx.update(table, "todo-rollback", { done: false });
    tx.rollback();

    expect(runtimeTransaction.rollback).toHaveBeenCalledTimes(1);
    expect(runtimeTransaction.commit).not.toHaveBeenCalled();
    expect(() => tx.commit()).toThrow(/rolled back/i);
    expect(() => tx.rollback()).toThrow(/rolled back/i);
    expect(() => tx.insert(table, { title: "Nope", done: false })).toThrow(/rolled back/i);
    expect(runtimeTransaction.commit).toHaveBeenCalledTimes(1);
    expect(runtimeTransaction.rollback).toHaveBeenCalledTimes(2);
    expect(runtimeTransaction.create).toHaveBeenCalledTimes(1);
    expect(runtimeTransaction.update).toHaveBeenCalledTimes(1);
  });

  it("rejects db transaction rollback after commit", () => {
    const table = todoTable();
    let status: TestTransactionStatus = "active";
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-commit-before-rollback"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => {
        assertTestTransactionActive(status, "batch-commit-before-rollback");
        status = "committed";
        return makeWriteHandle("batch-commit-before-rollback").handle;
      }),
      rollback: vi.fn(() => {
        assertTestTransactionActive(status, "batch-commit-before-rollback");
        status = "rolledBack";
      }),
      localBatchRecord: vi.fn((batchId = "batch-commit-before-rollback") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-commit-before-rollback")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const db = createDbFromClient(
      { appId: "client-backed-commit-before-rollback" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();
    tx.update(table, "todo-commit-before-rollback", { done: false });
    tx.commit();

    expect(() => tx.rollback()).toThrow(/committed/i);
    expect(runtimeTransaction.rollback).toHaveBeenCalledTimes(1);
  });

  it("delegates terminal transaction errors to runtime operations", () => {
    const table = todoTable();
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-runtime-rolled-back"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => {
        throw new Error("runtime transaction has already been rolled back");
      }),
      rollback: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-runtime-rolled-back") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-runtime-rolled-back")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const db = createDbFromClient(
      { appId: "client-backed-runtime-status" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();
    tx.update(table, "todo-runtime-status", { done: false });

    expect(() => tx.commit()).toThrow(/runtime transaction has already been rolled back/);
    expect(runtimeTransaction.commit).toHaveBeenCalledTimes(1);
  });

  it("delegates terminal write errors to runtime transaction operations", () => {
    const table = todoTable();
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-runtime-write-rolled-back"),
      create: vi.fn(() => {
        throw new Error("runtime write rejected after rollback");
      }),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-runtime-write-rolled-back").handle),
      rollback: vi.fn(),
      localBatchRecord: vi.fn((batchId = "batch-runtime-write-rolled-back") =>
        makeLocalBatchRecord(batchId),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-runtime-write-rolled-back")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const runtimeClient = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const db = createDbFromClient(
      { appId: "client-backed-runtime-write-status" },
      runtimeClient as unknown as JazzClient,
    );

    const tx = db.beginTransaction();

    expect(() => tx.insert(table, { title: "Nope", done: false })).toThrow(
      /runtime write rejected after rollback/,
    );
    expect(runtimeTransaction.create).toHaveBeenCalledTimes(1);
  });

  it("rejects db transaction writes against a different client/schema", () => {
    const primaryTable = todoTable();
    const secondaryTable = {
      ...todoTable(),
      _schema: todoSchema(),
    };
    const runtimeTransaction = {
      batchId: vi.fn(() => "batch-cross-client"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-cross-client").handle),
      localBatchRecord: vi.fn((batchId = "batch-cross-client") => makeLocalBatchRecord(batchId)),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-cross-client")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const primaryClient = {
      getSchema: () => new Map(Object.entries(primaryTable._schema)),
      beginTransactionInternal: vi.fn(() => runtimeTransaction),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const secondaryClient = {
      getSchema: () => new Map(Object.entries(secondaryTable._schema)),
      beginTransactionInternal: vi.fn(),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId)),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new MultiClientDb(
      new Map([
        [primaryTable._schema, primaryClient],
        [secondaryTable._schema, secondaryClient],
      ]),
    );

    const tx = db.beginTransaction();
    tx.update(primaryTable, "todo-cross-client", { done: true });

    expect(() => tx.insert(secondaryTable, { title: "Wrong client", done: false })).toThrow(
      /cannot be used with table "todos" from a different schema\/client/,
    );
    expect(runtimeTransaction.update).toHaveBeenCalledTimes(1);
    expect(runtimeTransaction.create).not.toHaveBeenCalled();
  });

  it("creates a typed db batch bound by its first table operation", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-direct-1",
      values: [
        { type: "Text", value: "Direct batch" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-direct",
    } as Row;
    const committedRuntime = makeWriteHandle("batch-direct", "direct");
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-direct"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => committedRuntime.handle),
      localBatchRecord: vi.fn((batchId = "batch-direct") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-direct", "direct")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const beginBatchInternal = vi.fn(() => runtimeBatch);
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginBatchInternal,
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const batch = db.beginBatch();
    const inserted = batch.insert(table, { title: "Direct batch", done: false });
    const updated = batch.update(table, "todo-direct-1", { done: true });
    const deleted = batch.delete(table, "todo-direct-1");

    expect(beginBatchInternal).toHaveBeenCalledWith();
    expect(batch.batchId()).toBe("batch-direct");
    expect(inserted).toEqual({
      id: "todo-direct-1",
      title: "Direct batch",
      done: false,
    });
    expect(runtimeBatch.create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Direct batch" },
      done: { type: "Boolean", value: false },
    });
    expect(runtimeBatch.update).toHaveBeenCalledWith("todo-direct-1", {
      done: { type: "Boolean", value: true },
    });
    expect(runtimeBatch.delete).toHaveBeenCalledWith("todo-direct-1");
    expect(updated).toBeUndefined();
    expect(deleted).toBeUndefined();
    const committed = batch.commit();
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBe("batch-direct");
    await expect(committed.wait({ tier: "global" })).resolves.toBeUndefined();
    expect(committedRuntime.client.waitForPersistedBatch).toHaveBeenCalledWith(
      "batch-direct",
      "global",
    );
    expect(runtimeBatch.commit).toHaveBeenCalledWith();
    expect(batch.localBatchRecord()).toMatchObject({
      batchId: "batch-direct",
      mode: "direct",
    });
    expect(batch.localBatchRecords()).toEqual([makeLocalBatchRecord("batch-direct", "direct")]);
    expect(batch.acknowledgeRejectedBatch()).toBe(false);
  });

  it("uses declared schemas for direct batch writes without fetching runtime schema", () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-batch-fast-path",
      values: [
        { type: "Text", value: "Fast batch" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-direct-fast-path",
    } as Row;
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-direct-fast-path"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      upsert: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => makeWriteHandle("batch-direct-fast-path", "direct").handle),
      localBatchRecord: vi.fn((batchId = "batch-direct-fast-path") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-direct-fast-path", "direct")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const getSchema = vi.fn(() => new Map(Object.entries(todoSchema())));
    const getSchemaHash = vi.fn(() => "schema-hash");
    const client = {
      getSchema,
      getSchemaHash,
      beginBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const batch = db.beginBatch();

    batch.insert(table, { title: "Fast batch", done: false });
    batch.upsert(table, { title: "Fast batch upsert" }, { id: "todo-batch-fast-path" });
    batch.update(table, "todo-batch-fast-path", { done: true });

    expect(getSchema).not.toHaveBeenCalled();
    expect(getSchemaHash).not.toHaveBeenCalled();
    expect(runtimeBatch.create).toHaveBeenCalledWith("todos", {
      title: { type: "Text", value: "Fast batch" },
      done: { type: "Boolean", value: false },
    });
    expect(runtimeBatch.upsert).toHaveBeenCalledWith(
      "todos",
      { title: { type: "Text", value: "Fast batch upsert" } },
      { id: "todo-batch-fast-path" },
    );
    expect(runtimeBatch.update).toHaveBeenCalledWith("todo-batch-fast-path", {
      done: { type: "Boolean", value: true },
    });
  });

  it("commits a callback batch and returns the callback result handle", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-direct-callback",
      values: [
        { type: "Text", value: "Callback batch" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-direct-callback",
    } as Row;
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-direct-callback"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => makeWriteHandle("batch-direct-callback", "direct").handle),
      localBatchRecord: vi.fn((batchId = "batch-direct-callback") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-direct-callback", "direct")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      waitForPersistedBatch: vi.fn(async () => undefined),
      beginBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const handle = db.batch((batch) => {
      const todo = batch.insert(table, { title: "Callback batch", done: false });
      return todo;
    });

    expect(handle).not.toBeInstanceOf(Promise);
    expect(handle).toBeInstanceOf(WriteResult);
    expect(handle.batchId).toBe("batch-direct-callback");
    expect(handle.value).toEqual({
      id: "todo-direct-callback",
      title: "Callback batch",
      done: false,
    });
    expect(runtimeBatch.commit).toHaveBeenCalledTimes(1);
    await expect(handle.wait({ tier: "edge" })).resolves.toEqual({
      id: "todo-direct-callback",
      title: "Callback batch",
      done: false,
    });
    expect(client.waitForPersistedBatch).toHaveBeenCalledWith("batch-direct-callback", "edge");
  });

  it("does not commit a callback batch when the callback rejects", async () => {
    const table = todoTable();
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-direct-callback-rejected"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-direct-callback-rejected", "direct").handle),
      localBatchRecord: vi.fn((batchId = "batch-direct-callback-rejected") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [
        makeLocalBatchRecord("batch-direct-callback-rejected", "direct"),
      ]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);
    const error = new Error("callback failed");

    await expect(db.batch(async () => Promise.reject(error))).rejects.toBe(error);

    expect(runtimeBatch.commit).not.toHaveBeenCalled();
  });

  it("routes typed direct batch upserts through the runtime batch", () => {
    const table = todoTable();
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-upsert-direct"),
      create: vi.fn(),
      update: vi.fn(),
      upsert: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-upsert-direct", "direct").handle),
      localBatchRecord: vi.fn((batchId = "batch-upsert-direct") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-upsert-direct", "direct")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    db.batch((batch) => {
      batch.upsert(table, { title: "Updated in direct batch" }, { id: "todo-upsert-direct" });
    });

    expect(runtimeBatch.upsert).toHaveBeenCalledWith(
      "todos",
      { title: { type: "Text", value: "Updated in direct batch" } },
      { id: "todo-upsert-direct" },
    );
  });

  it("commits a typed async callback batch after the callback resolves", async () => {
    const table = todoTable();
    const runtimeRow: Row = {
      id: "todo-direct-async-callback",
      values: [
        { type: "Text", value: "Async callback batch" },
        { type: "Boolean", value: false },
      ],
      batchId: "batch-direct-async-callback",
    } as Row;
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-direct-async-callback"),
      create: vi.fn(() => runtimeRow),
      update: vi.fn(() => undefined),
      delete: vi.fn(() => undefined),
      commit: vi.fn(() => makeWriteHandle("batch-direct-async-callback", "direct").handle),
      localBatchRecord: vi.fn((batchId = "batch-direct-async-callback") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [
        makeLocalBatchRecord("batch-direct-async-callback", "direct"),
      ]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      waitForPersistedBatch: vi.fn(async () => undefined),
      beginBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const handlePromise = db.batch(async (batch) => {
      const todo = batch.insert(table, { title: "Async callback batch", done: false });
      expect(runtimeBatch.commit).not.toHaveBeenCalled();
      return todo;
    });

    expect(handlePromise).toBeInstanceOf(Promise);
    const handle = await handlePromise;
    expect(handle).toBeInstanceOf(WriteResult);
    expect(handle.batchId).toBe("batch-direct-async-callback");
    expect(handle.value).toEqual({
      id: "todo-direct-async-callback",
      title: "Async callback batch",
      done: false,
    });
    expect(runtimeBatch.commit).toHaveBeenCalledTimes(1);
    await expect(handle.wait({ tier: "edge" })).resolves.toEqual({
      id: "todo-direct-async-callback",
      title: "Async callback batch",
      done: false,
    });
    expect(client.waitForPersistedBatch).toHaveBeenCalledWith(
      "batch-direct-async-callback",
      "edge",
    );
  });

  it("throws when committing a db batch before any actions", () => {
    const beginBatchInternal = vi.fn();
    const client = {
      getSchema: () => new Map(Object.entries(todoSchema())),
      beginBatchInternal,
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new TestDb(client);

    const batch = db.beginBatch();

    expect(() => batch.commit()).toThrow(
      "DbDirectBatch.commit() requires at least one table operation first",
    );
    expect(beginBatchInternal).not.toHaveBeenCalled();
  });

  it("rejects db batch writes against a different client/schema", () => {
    const primaryTable = todoTable();
    const secondaryTable = {
      ...todoTable(),
      _schema: todoSchema(),
    };
    const runtimeBatch = {
      batchId: vi.fn(() => "batch-cross-client-direct"),
      create: vi.fn(),
      update: vi.fn(),
      delete: vi.fn(),
      commit: vi.fn(() => makeWriteHandle("batch-cross-client-direct", "direct").handle),
      localBatchRecord: vi.fn((batchId = "batch-cross-client-direct") =>
        makeLocalBatchRecord(batchId, "direct"),
      ),
      localBatchRecords: vi.fn(() => [makeLocalBatchRecord("batch-cross-client-direct", "direct")]),
      acknowledgeRejectedBatch: vi.fn(() => false),
    };
    const primaryClient = {
      getSchema: () => new Map(Object.entries(primaryTable._schema)),
      beginBatchInternal: vi.fn(() => runtimeBatch),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const secondaryClient = {
      getSchema: () => new Map(Object.entries(secondaryTable._schema)),
      beginBatchInternal: vi.fn(),
      localBatchRecord: vi.fn((batchId: string) => makeLocalBatchRecord(batchId, "direct")),
      acknowledgeRejectedBatch: vi.fn(() => false),
    } as unknown as JazzClient;
    const db = new MultiClientDb(
      new Map([
        [primaryTable._schema, primaryClient],
        [secondaryTable._schema, secondaryClient],
      ]),
    );

    const batch = db.beginBatch();
    batch.update(primaryTable, "todo-cross-client-direct", { done: true });

    expect(() => batch.insert(secondaryTable, { title: "Wrong client", done: false })).toThrow(
      /cannot be used with table "todos" from a different schema\/client/,
    );
    expect(runtimeBatch.update).toHaveBeenCalledTimes(1);
    expect(runtimeBatch.create).not.toHaveBeenCalled();
  });
});
