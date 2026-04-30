import { describe, expect, it, vi } from "vitest";
import { JazzRnRuntimeAdapter, type JazzRnRuntimeBinding } from "./jazz-rn-runtime-adapter.js";
import { decodeFFIRowFromJson, encodeFFIRecordToJson } from "../runtime/ffi-value.js";

function createBinding(overrides: Partial<JazzRnRuntimeBinding> = {}): JazzRnRuntimeBinding {
  return {
    addClient: vi.fn(() => "client-1"),
    addServer: vi.fn(),
    batchedTick: vi.fn(),
    close: vi.fn(),
    connect: vi.fn(),
    disconnect: vi.fn(),
    updateAuth: vi.fn(),
    onAuthFailure: vi.fn(),
    createSubscription: vi.fn(() => 9n),
    delete_: vi.fn(() => JSON.stringify({ batchId: "batch-delete-1" })),
    deleteWithSession: vi.fn(() => JSON.stringify({ batchId: "batch-delete-2" })),
    canInsert: vi.fn(() => JSON.stringify(true)),
    canInsertWithSession: vi.fn(() => JSON.stringify("unknown")),
    canUpdate: vi.fn(() => JSON.stringify(false)),
    canUpdateWithSession: vi.fn(() => JSON.stringify(true)),
    executeSubscription: vi.fn(),
    flush: vi.fn(),
    getSchemaHash: vi.fn(() => "schema-hash"),
    insert: vi.fn((_table, _valuesJson) =>
      JSON.stringify({ id: "row-1", values: [], batchId: "batch-1" }),
    ),
    insertWithSession: vi.fn((_table, _valuesJson, _writeContextJson) =>
      JSON.stringify({ id: "row-1", values: [], batchId: "batch-2" }),
    ),
    onBatchedTickNeeded: vi.fn(),
    onSyncMessageReceived: vi.fn(),
    onSyncMessageReceivedFromClient: vi.fn(),
    query: vi.fn(() => JSON.stringify([{ id: "row-1", values: [] }])),
    removeServer: vi.fn(),
    setClientRole: vi.fn(),
    subscribe: vi.fn(() => 7n),
    unsubscribe: vi.fn(),
    update: vi.fn(() => JSON.stringify({ batchId: "batch-update-1" })),
    updateWithSession: vi.fn(() => JSON.stringify({ batchId: "batch-update-2" })),
    ...overrides,
  };
}

describe("JazzRnRuntimeAdapter", () => {
  it("defers batched tick execution to avoid re-entrancy", async () => {
    const binding = createBinding();
    new JazzRnRuntimeAdapter(binding, {});

    const onBatchedTickNeeded = binding.onBatchedTickNeeded as ReturnType<typeof vi.fn>;
    const callbackObject = onBatchedTickNeeded.mock.calls[0]![0];

    callbackObject.requestBatchedTick();
    expect(binding.batchedTick).not.toHaveBeenCalled();

    await Promise.resolve();
    expect(binding.batchedTick).toHaveBeenCalledTimes(1);
  });

  it("serializes mutation payloads and parses query responses", async () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const row = adapter.insert("todos", { title: { type: "Text", value: "milk" } });
    expect(row).toEqual({ id: "row-1", values: [], batchId: "batch-1" });
    expect(binding.insert).toHaveBeenCalledWith(
      "todos",
      JSON.stringify({ title: { type: "Text", value: "milk" } }),
      undefined,
    );

    adapter.update("row-1", { done: { type: "Boolean", value: true } });
    expect(binding.update).toHaveBeenCalledWith(
      "row-1",
      JSON.stringify({ done: { type: "Boolean", value: true } }),
    );

    adapter.delete("row-1");
    expect(binding.delete_).toHaveBeenCalledWith("row-1");

    await expect(adapter.query("{}", null, null)).resolves.toEqual([{ id: "row-1", values: [] }]);
  });

  it("encodes Bytea mutations with an explicit FFI transport shape", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.insert("files", {
      data: { type: "Bytea", value: new Uint8Array([0x01, 0x02, 0xff]) },
    });

    expect(binding.insert).toHaveBeenCalledWith(
      "files",
      JSON.stringify({
        data: { type: "Bytea", value: "0102ff" },
      }),
      undefined,
    );
  });

  it("round-trips Bytea values through the RN FFI JSON codec", () => {
    const encoded = JSON.parse(
      encodeFFIRecordToJson({
        data: { type: "Bytea", value: new Uint8Array([0x01, 0x02, 0xff]) },
        chunks: {
          type: "Array",
          value: [{ type: "Bytea", value: new Uint8Array([0x0a, 0x0b]) }],
        },
        nested: {
          type: "Row",
          value: {
            id: "nested-row",
            values: [{ type: "Bytea", value: new Uint8Array([0x7f]) }],
          },
        },
      }),
    ) as Record<string, unknown>;

    expect(encoded).toEqual({
      data: { type: "Bytea", value: "0102ff" },
      chunks: {
        type: "Array",
        value: [{ type: "Bytea", value: "0a0b" }],
      },
      nested: {
        type: "Row",
        value: {
          id: "nested-row",
          values: [{ type: "Bytea", value: "7f" }],
        },
      },
    });

    const decoded = decodeFFIRowFromJson(
      JSON.stringify({
        id: "row-1",
        values: [encoded.data, encoded.chunks, encoded.nested],
      }),
    );

    const [data, chunks, nested] = decoded.values;
    expect(decoded.id).toBe("row-1");
    expect(data).toEqual({ type: "Bytea", value: new Uint8Array([0x01, 0x02, 0xff]) });
    expect(chunks).toEqual({
      type: "Array",
      value: [{ type: "Bytea", value: new Uint8Array([0x0a, 0x0b]) }],
    });
    expect(nested).toEqual({
      type: "Row",
      value: {
        id: "nested-row",
        values: [{ type: "Bytea", value: new Uint8Array([0x7f]) }],
      },
    });
  });

  it("serializes write context payloads for session-aware mutations", async () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});
    const writeContextJson = JSON.stringify({
      session: { user_id: "alice", claims: {} },
      attribution: "alice",
    });

    const row = adapter.insertWithSession(
      "todos",
      { title: { type: "Text", value: "milk" } },
      writeContextJson,
    );
    expect(row).toEqual({ id: "row-1", values: [], batchId: "batch-2" });
    expect(binding.insertWithSession).toHaveBeenCalledWith(
      "todos",
      JSON.stringify({ title: { type: "Text", value: "milk" } }),
      writeContextJson,
      undefined,
    );

    adapter.updateWithSession(
      "row-1",
      { done: { type: "Boolean", value: true } },
      writeContextJson,
    );
    expect(binding.updateWithSession).toHaveBeenCalledWith(
      "row-1",
      JSON.stringify({ done: { type: "Boolean", value: true } }),
      writeContextJson,
    );

    adapter.deleteWithSession("row-1", writeContextJson);
    expect(binding.deleteWithSession).toHaveBeenCalledWith("row-1", writeContextJson);
  });

  it("serializes permission preflight payloads and parses decisions", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    expect(adapter.canInsert("todos", { title: { type: "Text", value: "milk" } })).toBe(true);
    expect(binding.canInsert).toHaveBeenCalledWith(
      "todos",
      JSON.stringify({ title: { type: "Text", value: "milk" } }),
    );

    expect(adapter.canUpdate("row-1", { done: { type: "Boolean", value: true } })).toBe(false);
    expect(binding.canUpdate).toHaveBeenCalledWith(
      "row-1",
      JSON.stringify({ done: { type: "Boolean", value: true } }),
    );
  });

  it("serializes write context payloads for session-aware permission preflights", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});
    const writeContextJson = JSON.stringify({
      session: { user_id: "alice", claims: {} },
      attribution: "alice",
    });

    expect(
      adapter.canInsertWithSession(
        "todos",
        { title: { type: "Text", value: "milk" } },
        writeContextJson,
      ),
    ).toBe("unknown");
    expect(binding.canInsertWithSession).toHaveBeenCalledWith(
      "todos",
      JSON.stringify({ title: { type: "Text", value: "milk" } }),
      writeContextJson,
    );

    expect(
      adapter.canUpdateWithSession(
        "row-1",
        { done: { type: "Boolean", value: true } },
        writeContextJson,
      ),
    ).toBe(true);
    expect(binding.canUpdateWithSession).toHaveBeenCalledWith(
      "row-1",
      JSON.stringify({ done: { type: "Boolean", value: true } }),
      writeContextJson,
    );
  });

  it("bridges subscription callbacks with handle conversion", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const onUpdate = vi.fn();
    const handle = adapter.subscribe("{}", onUpdate, null, null);
    expect(handle).toBe(7);

    const subscribeMock = binding.subscribe as ReturnType<typeof vi.fn>;
    const subscriptionCallback = subscribeMock.mock.calls[0]![1];
    subscriptionCallback.onUpdate('{"added":[],"removed":[],"updated":[],"pending":false}');
    expect(onUpdate).toHaveBeenCalledWith({
      added: [],
      removed: [],
      updated: [],
      pending: false,
    });

    adapter.unsubscribe(handle);
    expect(binding.unsubscribe).toHaveBeenCalledWith(7n);
  });

  it("bridges 2-phase createSubscription + executeSubscription with handle conversion", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const handle = adapter.createSubscription("{}", null, null);
    expect(handle).toBe(9);
    expect(binding.createSubscription).toHaveBeenCalledWith("{}", undefined, undefined);

    const onUpdate = vi.fn();
    adapter.executeSubscription(handle, onUpdate);

    const executeMock = binding.executeSubscription as ReturnType<typeof vi.fn>;
    expect(executeMock).toHaveBeenCalledTimes(1);
    expect(executeMock.mock.calls[0]![0]).toBe(9n);

    const callbackObject = executeMock.mock.calls[0]![1];
    callbackObject.onUpdate('{"added":[],"removed":[],"updated":[],"pending":false}');
    expect(onUpdate).toHaveBeenCalledWith({
      added: [],
      removed: [],
      updated: [],
      pending: false,
    });

    adapter.unsubscribe(handle);
    expect(binding.unsubscribe).toHaveBeenCalledWith(9n);
  });

  it("swallows exceptions thrown by subscription callbacks crossing the native boundary", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const onUpdate = vi.fn(() => {
      throw new Error("sub boom");
    });
    adapter.subscribe("{}", onUpdate, null, null);
    const subscribeMock = binding.subscribe as ReturnType<typeof vi.fn>;
    const subscriptionCallback = subscribeMock.mock.calls[0]![1];
    expect(() => subscriptionCallback.onUpdate("[]")).not.toThrow();
  });

  it("passes canonical subscription tuple updates through unchanged", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const onUpdate = vi.fn();
    adapter.subscribe("{}", onUpdate, null, null);
    const subscribeMock = binding.subscribe as ReturnType<typeof vi.fn>;
    const subscriptionCallback = subscribeMock.mock.calls[0]![1];

    subscriptionCallback.onUpdate(
      JSON.stringify({
        added: [],
        removed: [],
        updated: [
          [
            { id: "row-u", values: [{ type: "Text", value: "before" }] },
            { id: "row-u", values: [{ type: "Text", value: "after" }] },
          ],
        ],
        pending: false,
      }),
    );

    expect(onUpdate).toHaveBeenCalledWith({
      added: [],
      removed: [],
      updated: [
        [
          { id: "row-u", values: [{ type: "Text", value: "before" }] },
          { id: "row-u", values: [{ type: "Text", value: "after" }] },
        ],
      ],
      pending: false,
    });
  });

  it("wraps Jazz RN errors with error name and cause", async () => {
    const runtimeError = {
      tag: "Runtime",
      inner: {
        message: "indexed value too large",
      },
    };
    const binding = createBinding({
      insert: vi.fn(() => {
        throw runtimeError;
      }),
      query: vi.fn(() => {
        throw runtimeError;
      }),
      update: vi.fn(() => {
        throw runtimeError;
      }),
      delete_: vi.fn(() => {
        throw runtimeError;
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const insertError = (() => {
      try {
        adapter.insert("todos", {});
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(insertError).toBeInstanceOf(Error);
    expect((insertError as Error).name).toBe("JazzRnRuntimeError");
    expect((insertError as Error).message).toBe("indexed value too large");
    expect((insertError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((insertError as Error & { tag?: unknown }).tag).toBe("Runtime");

    const queryError = await adapter.query("{}", null, null).catch((error: unknown) => error);
    expect(queryError).toBeInstanceOf(Error);
    expect((queryError as Error).name).toBe("JazzRnRuntimeError");
    expect((queryError as Error).message).toBe("indexed value too large");
    expect((queryError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((queryError as Error & { tag?: unknown }).tag).toBe("Runtime");

    const updateError = (() => {
      try {
        adapter.update("row-1", { done: { type: "Boolean", value: true } });
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(updateError).toBeInstanceOf(Error);
    expect((updateError as Error).name).toBe("JazzRnRuntimeError");
    expect((updateError as Error).message).toBe("indexed value too large");
    expect((updateError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((updateError as Error & { tag?: unknown }).tag).toBe("Runtime");

    const deleteError = (() => {
      try {
        adapter.delete("row-1");
        return null;
      } catch (error) {
        return error;
      }
    })();
    expect(deleteError).toBeInstanceOf(Error);
    expect((deleteError as Error).name).toBe("JazzRnRuntimeError");
    expect((deleteError as Error).message).toBe("indexed value too large");
    expect((deleteError as Error & { cause?: unknown }).cause).toBe(runtimeError);
    expect((deleteError as Error & { tag?: unknown }).tag).toBe("Runtime");
  });

  it("does not wrap non-Jazz errors", () => {
    const binding = createBinding({
      insert: vi.fn(() => {
        throw new Error("plain failure");
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const error = (() => {
      try {
        adapter.insert("todos", {});
        return null;
      } catch (caught) {
        return caught;
      }
    })();

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).name).toBe("Error");
    expect((error as Error).message).toBe("plain failure");
    expect((error as Error & { tag?: unknown }).tag).toBeUndefined();
  });

  it("derives error name for non-runtime Jazz tags", () => {
    const schemaError = {
      tag: "Schema",
      inner: {
        message: "schema mismatch",
      },
    };
    const binding = createBinding({
      insert: vi.fn(() => {
        throw schemaError;
      }),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const error = (() => {
      try {
        adapter.insert("todos", {});
        return null;
      } catch (caught) {
        return caught;
      }
    })();

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).name).toBe("JazzRnSchemaError");
    expect((error as Error).message).toBe("schema mismatch");
    expect((error as Error & { tag?: unknown }).tag).toBe("Schema");
    expect((error as Error & { cause?: unknown }).cause).toBe(schemaError);
  });

  it("no-ops sync hooks after close", () => {
    const binding = createBinding();
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.close();
    adapter.addServer();
    adapter.removeServer();
    adapter.onSyncMessageReceived('{"Ping":{}}');
    adapter.onSyncMessageReceivedFromClient("client-1", '{"Ping":{}}');

    expect(binding.addServer).not.toHaveBeenCalled();
    expect(binding.removeServer).not.toHaveBeenCalled();
    expect(binding.onSyncMessageReceived).not.toHaveBeenCalled();
    expect(binding.onSyncMessageReceivedFromClient).not.toHaveBeenCalled();
  });

  it("forwards updateAuth JSON payload to the native binding", () => {
    const updateAuth = vi.fn();
    const binding = createBinding({ updateAuth });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    adapter.updateAuth?.(JSON.stringify({ jwt_token: "refreshed" }));

    expect(updateAuth).toHaveBeenCalledWith(JSON.stringify({ jwt_token: "refreshed" }));
  });

  it("registers onAuthFailure callback with the native binding and invokes it on failure", () => {
    let captured: { onFailure: (reason: string) => void } | null = null;
    const onAuthFailure = vi.fn((cb: { onFailure: (reason: string) => void }) => {
      captured = cb;
    });
    const binding = createBinding({ onAuthFailure });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    const listener = vi.fn();
    adapter.onAuthFailure?.(listener);

    expect(onAuthFailure).toHaveBeenCalledTimes(1);
    expect(captured).not.toBeNull();
    expect(captured!.onFailure).toBeInstanceOf(Function);

    captured!.onFailure("token expired");
    expect(listener).toHaveBeenCalledWith("token expired");
  });

  it("bridges rejected batch helpers", () => {
    const binding = createBinding({
      loadLocalBatchRecord: vi.fn(() =>
        JSON.stringify({
          batchId: "batch-1",
          latestSettlement: {
            kind: "rejected",
            code: "WriteRejected",
            reason: "nope",
          },
        }),
      ),
      loadLocalBatchRecords: vi.fn(() =>
        JSON.stringify([
          {
            batchId: "batch-1",
            latestSettlement: {
              kind: "rejected",
              code: "WriteRejected",
              reason: "nope",
            },
          },
        ]),
      ),
      drainRejectedBatchIds: vi.fn(() => ["batch-1"]),
      acknowledgeRejectedBatch: vi.fn(() => true),
      sealBatch: vi.fn(),
    });
    const adapter = new JazzRnRuntimeAdapter(binding, {});

    expect(adapter.loadLocalBatchRecord("batch-1")).toEqual({
      batchId: "batch-1",
      latestSettlement: {
        kind: "rejected",
        code: "WriteRejected",
        reason: "nope",
      },
    });
    expect(adapter.loadLocalBatchRecords()).toEqual([
      {
        batchId: "batch-1",
        latestSettlement: {
          kind: "rejected",
          code: "WriteRejected",
          reason: "nope",
        },
      },
    ]);
    expect(adapter.drainRejectedBatchIds()).toEqual(["batch-1"]);
    expect(adapter.acknowledgeRejectedBatch("batch-1")).toBe(true);
    adapter.sealBatch("batch-1");

    expect(binding.loadLocalBatchRecord).toHaveBeenCalledWith("batch-1");
    expect(binding.loadLocalBatchRecords).toHaveBeenCalledTimes(1);
    expect(binding.drainRejectedBatchIds).toHaveBeenCalledTimes(1);
    expect(binding.acknowledgeRejectedBatch).toHaveBeenCalledWith("batch-1");
    expect(binding.sealBatch).toHaveBeenCalledWith("batch-1");
  });
});
