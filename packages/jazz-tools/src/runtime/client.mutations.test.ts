import { describe, expect, it, vi } from "vitest";
import { JazzClient, type Runtime } from "./client.js";
import type { AppContext, Session } from "./context.js";

function makeClient(runtimeOverrides: Partial<Runtime> = {}) {
  const insertCalls: Array<[string, Record<string, unknown>]> = [];
  const insertWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const updateWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const updateCalls: Array<[string, Record<string, unknown>]> = [];
  const deleteWithSessionCalls: Array<[string, string | undefined]> = [];
  const deleteCalls: string[] = [];
  const canInsertCalls: Array<[string, Record<string, unknown>]> = [];
  const canUpdateWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> =
    [];
  const canInsertWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> =
    [];
  const canUpdateCalls: Array<[string, Record<string, unknown>]> = [];

  const runtimeBase: Runtime = {
    loadLocalBatchRecord: () => null,
    loadLocalBatchRecords: () => [],
    insert: (table: string, values: Record<string, unknown>) => {
      insertCalls.push([table, values]);
      return {
        id: "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "insert-batch-id",
      };
    },
    insertDurable: async () => ({
      id: "00000000-0000-0000-0000-000000000001",
      values: [],
      batchId: "insert-batch-id",
    }),
    insertWithSession: (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      insertWithSessionCalls.push([table, values, writeContextJson ?? undefined]);
      return {
        id: "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "insert-with-session-batch-id",
      };
    },
    update: (objectId: string, updates: Record<string, unknown>) => {
      updateCalls.push([objectId, updates]);
      return { batchId: "update-batch-id" };
    },
    updateDurable: async () => {},
    updateWithSession: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateWithSessionCalls.push([objectId, updates, writeContextJson ?? undefined]);
      return { batchId: "update-with-session-batch-id" };
    },
    delete: (objectId: string) => {
      deleteCalls.push(objectId);
      return { batchId: "delete-batch-id" };
    },
    deleteDurable: async () => {},
    deleteWithSession: (objectId: string, writeContextJson?: string | null) => {
      deleteWithSessionCalls.push([objectId, writeContextJson ?? undefined]);
      return { batchId: "delete-with-session-batch-id" };
    },
    canInsert: async (table: string, values: Record<string, unknown>) => {
      canInsertCalls.push([table, values]);
      return true;
    },
    canInsertWithSession: async (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      canInsertWithSessionCalls.push([table, values, writeContextJson ?? undefined]);
      return "unknown" as const;
    },
    canUpdate: async (objectId: string, updates: Record<string, unknown>) => {
      canUpdateCalls.push([objectId, updates]);
      return false;
    },
    canUpdateWithSession: async (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      canUpdateWithSessionCalls.push([objectId, updates, writeContextJson ?? undefined]);
      return true;
    },
    query: async () => [],
    subscribe: () => 0,
    createSubscription: () => 0,
    executeSubscription: () => {},
    unsubscribe: () => {},
    onSyncMessageReceived: () => {},
    onSyncMessageToSend: () => {},
    sealBatch: vi.fn(),
    addServer: () => {},
    removeServer: () => {},
    addClient: () => "00000000-0000-0000-0000-000000000001",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };
  const runtime: Runtime = { ...runtimeBase, ...runtimeOverrides };

  const context: AppContext = {
    appId: "test-app",
    schema: {},
    serverUrl: "http://localhost:1625",
    backendSecret: "test-backend-secret",
  };

  const JazzClientCtor = JazzClient as unknown as {
    new (
      runtime: Runtime,
      context: AppContext,
      defaultDurabilityTier: "local" | "edge" | "global",
    ): JazzClient;
  };

  return {
    client: new JazzClientCtor(runtime, context, "edge"),
    runtime,
    insertCalls,
    insertWithSessionCalls,
    updateCalls,
    updateWithSessionCalls,
    deleteCalls,
    deleteWithSessionCalls,
    canInsertCalls,
    canInsertWithSessionCalls,
    canUpdateCalls,
    canUpdateWithSessionCalls,
  };
}

describe("JazzClient mutation durability split", () => {
  it("keeps Bytea mutations as Uint8Array at the runtime boundary", () => {
    const { client, insertCalls, updateCalls } = makeClient();
    const payload = new Uint8Array([1, 2, 3]);
    const insertValues = {
      payload: { type: "Bytea" as const, value: payload },
    };
    const updateValues = {
      payload: { type: "Bytea" as const, value: payload },
    };

    client.create("todos", insertValues);
    client.update("row-1", updateValues);

    expect(insertCalls).toHaveLength(1);
    expect(updateCalls).toHaveLength(1);
    expect(insertCalls[0]?.[1]).toBe(insertValues);
    expect(updateCalls[0]?.[1]).toBe(updateValues);

    const insertPayload = insertCalls[0]?.[1].payload as
      | { type: "Bytea"; value: Uint8Array }
      | undefined;
    const updatePayload = updateCalls[0]?.[1].payload as
      | { type: "Bytea"; value: Uint8Array }
      | undefined;

    expect(insertPayload?.type).toBe("Bytea");
    expect(updatePayload?.type).toBe("Bytea");
    expect(insertPayload?.value).toBeInstanceOf(Uint8Array);
    expect(updatePayload?.value).toBeInstanceOf(Uint8Array);
    expect(insertPayload?.value).toBe(payload);
    expect(updatePayload?.value).toBe(payload);
    expect(Array.from(insertPayload?.value ?? [])).toEqual([1, 2, 3]);
    expect(Array.from(updatePayload?.value ?? [])).toEqual([1, 2, 3]);
  });

  it("rethrows synchronous runtime mutation errors", () => {
    const insertError = new Error("Insert failed: indexed value too large");
    const updateError = new Error("Update failed: indexed value too large");
    const { client } = makeClient({
      insert: () => {
        throw insertError;
      },
      update: () => {
        throw updateError;
      },
    });

    expect(() => client.create("todos", {})).toThrow(insertError);
    expect(() =>
      client.update("row-1", { done: { type: "Boolean" as const, value: true } }),
    ).toThrow(updateError);
  });

  it("routes update/delete through the synchronous runtime methods", () => {
    const { client, runtime, updateCalls, deleteCalls } = makeClient();
    const updates = { done: { type: "Boolean" as const, value: true } };

    expect(client.update("row-1", updates)).toEqual({
      batchId: "update-batch-id",
    });
    expect(client.delete("row-1")).toEqual({
      batchId: "delete-batch-id",
    });

    expect(updateCalls).toEqual([["row-1", updates]]);
    expect(deleteCalls).toEqual(["row-1"]);
    expect(runtime.sealBatch).toHaveBeenCalledWith("update-batch-id");
    expect(runtime.sealBatch).toHaveBeenCalledWith("delete-batch-id");
  });

  it("routes permission preflight through local runtime methods", async () => {
    const { client, canInsertCalls, canUpdateCalls } = makeClient();
    const values = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };

    await expect(client.canInsert("todos", values)).resolves.toBe(true);
    await expect(client.canUpdate("row-1", updates)).resolves.toBe(false);

    expect(canInsertCalls).toEqual([["todos", values]]);
    expect(canUpdateCalls).toEqual([["row-1", updates]]);
  });

  it("routes attributed writes through session-aware runtime methods", async () => {
    const { client, insertWithSessionCalls, updateWithSessionCalls, deleteWithSessionCalls } =
      makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const attributedContext = JSON.stringify({ attribution: "alice" });

    client.createInternal("todos", insertValues, undefined, "alice");
    client.updateInternal("row-1", updates, undefined, "alice");
    client.deleteInternal("row-1", undefined, "alice");

    expect(insertWithSessionCalls).toEqual([["todos", insertValues, attributedContext]]);
    expect(updateWithSessionCalls).toEqual([["row-1", updates, attributedContext]]);
    expect(deleteWithSessionCalls).toEqual([["row-1", attributedContext]]);
  });

  it("routes attributed permission preflight through session-aware runtime methods", async () => {
    const { client, canInsertWithSessionCalls, canUpdateWithSessionCalls } = makeClient();
    const values = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const attributedContext = JSON.stringify({ attribution: "alice" });

    await expect(client.canInsertInternal("todos", values, undefined, "alice")).resolves.toBe(
      "unknown",
    );
    await expect(client.canUpdateInternal("row-1", updates, undefined, "alice")).resolves.toBe(
      true,
    );

    expect(canInsertWithSessionCalls).toEqual([["todos", values, attributedContext]]);
    expect(canUpdateWithSessionCalls).toEqual([["row-1", updates, attributedContext]]);
  });

  it("forwards caller-supplied create ids to runtime insert methods", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insert = vi.fn(
      (table: string, values: Record<string, unknown>, objectId?: string | null) => {
        return { id: objectId ?? "generated-id", values: [], batchId: "batch-1" };
      },
    );
    const { client, runtime } = makeClient({ insert });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };

    const created = client.create("todos", insertValues, { id: externalId });

    expect(insert).toHaveBeenCalledWith("todos", insertValues, externalId);
    expect(created.value.id).toBe(externalId);
    expect(runtime.sealBatch).toHaveBeenCalledWith("batch-1");
  });

  it("falls back to update when upsert sees an existing object id", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insert = vi.fn(() => {
      throw insertError;
    });
    const update = vi.fn(() => ({ batchId: "fallback-update-batch" }));
    const { client } = makeClient({
      insert,
      update,
    });
    const values = { title: { type: "Text" as const, value: "Updated title" } };

    expect(client.upsert("todos", values, { id: externalId })).toEqual({
      batchId: "fallback-update-batch",
    });

    expect(insert).toHaveBeenCalledWith("todos", values, externalId);
    expect(update).toHaveBeenCalledWith(externalId, values);
  });

  it("returns the inserted batch id when upsert creates a new row", () => {
    const { client } = makeClient({
      insert: () => ({
        id: "00000000-0000-0000-0000-000000000001",
        values: [],
        batchId: "batch-created-via-upsert",
      }),
    });
    const values = { title: { type: "Text" as const, value: "New todo" } };

    expect(client.upsert("todos", values, { id: "row-1" })).toEqual({
      batchId: "batch-created-via-upsert",
    });
  });

  it("encodes session and attribution together when both are provided", () => {
    const { client, insertWithSessionCalls } = makeClient();
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
      authMode: "external",
    };
    const insertValues = { title: { type: "Text" as const, value: "Attributed" } };

    client.createInternal("todos", insertValues, session, "alice");

    expect(insertWithSessionCalls).toEqual([
      [
        "todos",
        insertValues,
        JSON.stringify({
          session,
          attribution: "alice",
        }),
      ],
    ]);
  });

  it("encodes custom updated_at overrides for create and update mutation options", async () => {
    const insertWithSession = vi.fn(
      (
        table: string,
        values: Record<string, unknown>,
        _writeContextJson?: string | null,
        objectId?: string | null,
      ) => ({
        id: objectId ?? "generated-id",
        values: [],
        batchId: "generated-batch-id",
      }),
    );
    const updateWithSession = vi.fn(() => ({ batchId: "generated-update-batch-id" }));
    const { client } = makeClient({
      insertWithSession,
      updateWithSession,
    });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    client.create("todos", insertValues, { updatedAt });
    client.update("row-1", updates, { updatedAt });

    expect(insertWithSession).toHaveBeenCalledWith("todos", insertValues, updatedAtContext);
    expect(updateWithSession).toHaveBeenCalledWith("row-1", updates, updatedAtContext);
  });

  it("preserves custom updated_at overrides when upsert falls back to update", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insertWithSession = vi.fn(() => {
      throw insertError;
    });
    const updateWithSession = vi.fn(() => ({ batchId: "fallback-update-session-batch" }));
    const { client } = makeClient({
      insertWithSession,
      updateWithSession,
    });
    const values = { title: { type: "Text" as const, value: "Updated title" } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    expect(client.upsert("todos", values, { id: externalId, updatedAt })).toEqual({
      batchId: "fallback-update-session-batch",
    });

    expect(insertWithSession).toHaveBeenCalledWith("todos", values, updatedAtContext, externalId);
    expect(updateWithSession).toHaveBeenCalledWith(externalId, values, updatedAtContext);
  });
});
