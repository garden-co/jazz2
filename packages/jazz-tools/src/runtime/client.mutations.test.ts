import { describe, expect, it, vi } from "vitest";
import { JazzClient, type Runtime } from "./client.js";
import type { AppContext, Session } from "./context.js";

function makeClient(runtimeOverrides: Partial<Runtime> = {}) {
  const insertCalls: Array<[string, Record<string, unknown>]> = [];
  const insertWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const insertDurableWithSessionCalls: Array<
    [string, Record<string, unknown>, string | undefined, string]
  > = [];
  const updateWithSessionCalls: Array<[string, Record<string, unknown>, string | undefined]> = [];
  const updateCalls: Array<[string, Record<string, unknown>]> = [];
  const deleteWithSessionCalls: Array<[string, string | undefined]> = [];
  const updateDurableCalls: Array<[string, Record<string, unknown>, string]> = [];
  const updateDurableWithSessionCalls: Array<
    [string, Record<string, unknown>, string | undefined, string]
  > = [];
  const deleteCalls: string[] = [];
  const deleteDurableCalls: Array<[string, string]> = [];
  const deleteDurableWithSessionCalls: Array<[string, string | undefined, string]> = [];

  const runtimeBase: Runtime = {
    insert: (table: string, values: Record<string, unknown>) => {
      insertCalls.push([table, values]);
      return { id: "00000000-0000-0000-0000-000000000001", values: [] };
    },
    insertWithSession: (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      insertWithSessionCalls.push([table, values, writeContextJson ?? undefined]);
      return { id: "00000000-0000-0000-0000-000000000001", values: [] };
    },
    insertDurable: async () => ({ id: "00000000-0000-0000-0000-000000000001", values: [] }),
    insertDurableWithSession: async (
      table: string,
      values: Record<string, unknown>,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      insertDurableWithSessionCalls.push([table, values, writeContextJson ?? undefined, tier]);
      return { id: "00000000-0000-0000-0000-000000000001", values: [] };
    },
    update: (objectId: string, updates: Record<string, unknown>) => {
      updateCalls.push([objectId, updates]);
    },
    updateWithSession: (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
    ) => {
      updateWithSessionCalls.push([objectId, updates, writeContextJson ?? undefined]);
    },
    updateDurable: async (objectId: string, updates: Record<string, unknown>, tier: string) => {
      updateDurableCalls.push([objectId, updates, tier]);
    },
    updateDurableWithSession: async (
      objectId: string,
      updates: Record<string, unknown>,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      updateDurableWithSessionCalls.push([objectId, updates, writeContextJson ?? undefined, tier]);
    },
    delete: (objectId: string) => {
      deleteCalls.push(objectId);
    },
    deleteWithSession: (objectId: string, writeContextJson?: string | null) => {
      deleteWithSessionCalls.push([objectId, writeContextJson ?? undefined]);
    },
    deleteDurable: async (objectId: string, tier: string) => {
      deleteDurableCalls.push([objectId, tier]);
    },
    deleteDurableWithSession: async (
      objectId: string,
      writeContextJson?: string | null,
      tier = "edge",
    ) => {
      deleteDurableWithSessionCalls.push([objectId, writeContextJson ?? undefined, tier]);
    },
    query: async () => [],
    subscribe: () => 0,
    createSubscription: () => 0,
    executeSubscription: () => {},
    unsubscribe: () => {},
    onSyncMessageReceived: () => {},
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
    insertCalls,
    insertWithSessionCalls,
    insertDurableWithSessionCalls,
    updateCalls,
    updateWithSessionCalls,
    updateDurableCalls,
    updateDurableWithSessionCalls,
    deleteCalls,
    deleteDurableCalls,
    deleteWithSessionCalls,
    deleteDurableWithSessionCalls,
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
    const { client, updateCalls, deleteCalls } = makeClient();
    const updates = { done: { type: "Boolean" as const, value: true } };

    expect(client.update("row-1", updates)).toBeUndefined();
    expect(client.delete("row-1")).toBeUndefined();

    expect(updateCalls).toEqual([["row-1", updates]]);
    expect(deleteCalls).toEqual(["row-1"]);
  });

  it("routes updateDurable/deleteDurable through durability-aware runtime methods", async () => {
    const { client, updateDurableCalls, deleteDurableCalls } = makeClient();
    const updates = { done: { type: "Boolean" as const, value: true } };

    const updatePending = client.updateDurable("row-1", updates);
    const deletePending = client.deleteDurable("row-1", { tier: "global" });

    expect(updatePending).toBeInstanceOf(Promise);
    expect(deletePending).toBeInstanceOf(Promise);

    await updatePending;
    await deletePending;

    expect(updateDurableCalls).toEqual([["row-1", updates, "edge"]]);
    expect(deleteDurableCalls).toEqual([["row-1", "global"]]);
  });

  it("routes attributed writes through session-aware runtime methods", async () => {
    const {
      client,
      insertWithSessionCalls,
      insertDurableWithSessionCalls,
      updateWithSessionCalls,
      updateDurableWithSessionCalls,
      deleteWithSessionCalls,
      deleteDurableWithSessionCalls,
    } = makeClient();
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const attributedContext = JSON.stringify({ attribution: "alice" });

    client.createInternal("todos", insertValues, undefined, "alice");
    await client.createDurableInternal("todos", insertValues, undefined, "alice");
    client.updateInternal("row-1", updates, undefined, "alice");
    await client.updateDurableInternal("row-1", updates, undefined, "alice");
    client.deleteInternal("row-1", undefined, "alice");
    await client.deleteDurableInternal("row-1", undefined, "alice", { tier: "global" });

    expect(insertWithSessionCalls).toEqual([["todos", insertValues, attributedContext]]);
    expect(insertDurableWithSessionCalls).toEqual([
      ["todos", insertValues, attributedContext, "edge"],
    ]);
    expect(updateWithSessionCalls).toEqual([["row-1", updates, attributedContext]]);
    expect(updateDurableWithSessionCalls).toEqual([["row-1", updates, attributedContext, "edge"]]);
    expect(deleteWithSessionCalls).toEqual([["row-1", attributedContext]]);
    expect(deleteDurableWithSessionCalls).toEqual([["row-1", attributedContext, "global"]]);
  });

  it("forwards caller-supplied create ids to runtime insert methods", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insert = vi.fn(
      (table: string, values: Record<string, unknown>, objectId?: string | null) => {
        return { id: objectId ?? "generated-id", values: [] };
      },
    );
    const insertDurable = vi.fn(
      async (
        table: string,
        values: Record<string, unknown>,
        tier: string,
        objectId?: string | null,
      ) => {
        return { id: objectId ?? "generated-id", values: [] };
      },
    );
    const { client } = makeClient({ insert, insertDurable });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };

    const created = client.create("todos", insertValues, { id: externalId });
    const createdDurable = await client.createDurable("todos", insertValues, { id: externalId });

    expect(insert).toHaveBeenCalledWith("todos", insertValues, externalId);
    expect(insertDurable).toHaveBeenCalledWith("todos", insertValues, "edge", externalId);
    expect(created.id).toBe(externalId);
    expect(createdDurable.id).toBe(externalId);
  });

  it("falls back to update when upsert sees an existing object id", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insert = vi.fn(() => {
      throw insertError;
    });
    const insertDurable = vi.fn(async () => {
      throw insertError;
    });
    const update = vi.fn();
    const updateDurable = vi.fn(async () => {});
    const { client } = makeClient({ insert, insertDurable, update, updateDurable });
    const values = { title: { type: "Text" as const, value: "Updated title" } };

    expect(client.upsert("todos", values, { id: externalId })).toBeUndefined();
    await expect(
      client.upsertDurable("todos", values, { id: externalId }),
    ).resolves.toBeUndefined();

    expect(insert).toHaveBeenCalledWith("todos", values, externalId);
    expect(insertDurable).toHaveBeenCalledWith("todos", values, "edge", externalId);
    expect(update).toHaveBeenCalledWith(externalId, values);
    expect(updateDurable).toHaveBeenCalledWith(externalId, values, "edge");
  });

  it("encodes session and attribution together when both are provided", () => {
    const { client, insertWithSessionCalls } = makeClient();
    const session: Session = {
      user_id: "backend-user",
      claims: { role: "admin" },
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
      }),
    );
    const insertDurableWithSession = vi.fn(
      async (
        table: string,
        values: Record<string, unknown>,
        _writeContextJson?: string | null,
        _tier = "edge",
        objectId?: string | null,
      ) => ({
        id: objectId ?? "generated-id",
        values: [],
      }),
    );
    const updateWithSession = vi.fn();
    const updateDurableWithSession = vi.fn(async () => {});
    const { client } = makeClient({
      insertWithSession,
      insertDurableWithSession,
      updateWithSession,
      updateDurableWithSession,
    });
    const insertValues = { title: { type: "Text" as const, value: "Draft" } };
    const updates = { done: { type: "Boolean" as const, value: true } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    client.create("todos", insertValues, { updatedAt });
    await client.createDurable("todos", insertValues, {
      id: "todo-1",
      tier: "global",
      updatedAt,
    });
    client.update("row-1", updates, { updatedAt });
    await client.updateDurable("row-1", updates, { tier: "global", updatedAt });

    expect(insertWithSession).toHaveBeenCalledWith("todos", insertValues, updatedAtContext);
    expect(insertDurableWithSession).toHaveBeenCalledWith(
      "todos",
      insertValues,
      updatedAtContext,
      "global",
      "todo-1",
    );
    expect(updateWithSession).toHaveBeenCalledWith("row-1", updates, updatedAtContext);
    expect(updateDurableWithSession).toHaveBeenCalledWith(
      "row-1",
      updates,
      updatedAtContext,
      "global",
    );
  });

  it("preserves custom updated_at overrides when upsert falls back to update", async () => {
    const externalId = "01963f3e-5cbe-7a62-8d7c-123456789abc";
    const insertError = new Error(`encoding error: object already exists: ${externalId}`);
    const insertWithSession = vi.fn(() => {
      throw insertError;
    });
    const insertDurableWithSession = vi.fn(async () => {
      throw insertError;
    });
    const updateWithSession = vi.fn();
    const updateDurableWithSession = vi.fn(async () => {});
    const { client } = makeClient({
      insertWithSession,
      insertDurableWithSession,
      updateWithSession,
      updateDurableWithSession,
    });
    const values = { title: { type: "Text" as const, value: "Updated title" } };
    const updatedAt = 1_764_000_000_000_000;
    const updatedAtContext = JSON.stringify({ updated_at: updatedAt });

    expect(client.upsert("todos", values, { id: externalId, updatedAt })).toBeUndefined();
    await expect(
      client.upsertDurable("todos", values, { id: externalId, updatedAt }),
    ).resolves.toBeUndefined();

    expect(insertWithSession).toHaveBeenCalledWith("todos", values, updatedAtContext, externalId);
    expect(insertDurableWithSession).toHaveBeenCalledWith(
      "todos",
      values,
      updatedAtContext,
      "edge",
      externalId,
    );
    expect(updateWithSession).toHaveBeenCalledWith(externalId, values, updatedAtContext);
    expect(updateDurableWithSession).toHaveBeenCalledWith(
      externalId,
      values,
      updatedAtContext,
      "edge",
    );
  });
});
