import { describe, it, expect, vi } from "vitest";
import {
  JazzClient,
  resolveDefaultDurabilityTier,
  type MutationErrorEvent,
  type Runtime,
  WriteHandle,
} from "./client.js";
import type { AppContext } from "./context.js";
import type { WasmSchema } from "../drivers/types.js";

function makeFakeRuntime() {
  const runtime = {
    updateAuth: vi.fn<(auth_json: string) => void>(),
    onAuthFailure: vi.fn<(callback: (reason: string) => void) => void>(),
    // Runtime interface stubs
    insert: vi.fn(),
    insertDurable: vi.fn(),
    update: vi.fn(),
    updateDurable: vi.fn(),
    delete: vi.fn(),
    deleteDurable: vi.fn(),
    query:
      vi.fn<
        (
          query_json: string,
          session_json?: string | null,
          tier?: string | null,
          options_json?: string | null,
        ) => Promise<any>
      >(),
    subscribe:
      vi.fn<
        (
          query_json: string,
          on_update: Function,
          session_json?: string | null,
          tier?: string | null,
          options_json?: string | null,
        ) => number
      >(),
    createSubscription:
      vi.fn<
        (
          query_json: string,
          session_json?: string | null,
          tier?: string | null,
          options_json?: string | null,
        ) => number
      >(),
    executeSubscription: vi.fn<(handle: number, on_update: Function) => void>(),
    unsubscribe: vi.fn<(handle: number) => void>(),
    loadLocalBatchRecord: vi.fn<
      (batch_id: string) => ReturnType<NonNullable<Runtime["loadLocalBatchRecord"]>>
    >(() => null),
    loadLocalBatchRecords: vi.fn<() => ReturnType<NonNullable<Runtime["loadLocalBatchRecords"]>>>(
      () => [],
    ),
    requestBatchSettlements: vi.fn<(batch_ids: string[]) => void>(),
    drainRejectedBatchIds: vi.fn<() => string[]>(() => []),
    acknowledgeRejectedBatch: vi.fn<(batch_id: string) => boolean>(() => false),
    sealBatch: vi.fn<(batch_id: string) => void>(),
    onSyncMessageReceived: vi.fn(),
    addServer: vi.fn(),
    removeServer: vi.fn(),
    addClient: vi.fn().mockReturnValue("client-id"),
    returnsDeclaredSchemaRows: false as boolean,
    getSchema: vi.fn().mockReturnValue({}),
    getSchemaHash: vi.fn().mockReturnValue("hash"),
    close: vi.fn(),
  } satisfies Runtime;

  return runtime;
}

function makeContext(): AppContext {
  return {
    appId: "test-app",
    schema: {},
    serverUrl: "https://example.test",
    jwtToken: "initial.jwt.token",
  };
}

describe("JazzClient onAuthFailure wiring", () => {
  it("registers runtimeOptions.onAuthFailure with runtime.onAuthFailure on construction", () => {
    const runtime = makeFakeRuntime();
    const onAuthFailure = vi.fn();

    JazzClient.connectWithRuntime(runtime as any, makeContext(), { onAuthFailure });

    expect(runtime.onAuthFailure).toHaveBeenCalledTimes(1);

    // Invoke whatever callback was registered:
    const registered = runtime.onAuthFailure.mock.calls[0][0];
    registered("token expired");
    expect(onAuthFailure).toHaveBeenCalledWith("expired");
  });

  it("does nothing when runtimeOptions.onAuthFailure is omitted", () => {
    const runtime = makeFakeRuntime();
    JazzClient.connectWithRuntime(runtime as any, makeContext(), {});
    expect(runtime.onAuthFailure).not.toHaveBeenCalled();
  });
});

describe("JazzClient.updateAuthToken", () => {
  it("forwards refreshed JWT to the Rust runtime via runtime.updateAuth", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    client.updateAuthToken("new.jwt.token");

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: "new.jwt.token" });
  });

  it("forwards undefined JWT (clear) as null jwt_token", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());

    client.updateAuthToken(undefined);

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: null });
  });

  it("preserves admin_secret from context across token refresh", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      ...makeContext(),
      adminSecret: "admin-xyz",
    });

    client.updateAuthToken("new.jwt.token");

    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({
      jwt_token: "new.jwt.token",
      admin_secret: "admin-xyz",
    });
  });

  it("preserves backend_secret from context across token refresh", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      ...makeContext(),
      backendSecret: "backend-abc",
    });

    client.updateAuthToken("new.jwt.token");

    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({
      jwt_token: "new.jwt.token",
      backend_secret: "backend-abc",
    });
  });
});

describe("JazzClient.updateCookieSession", () => {
  it("refreshes transport auth without requiring a JS-readable JWT", () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "cookie-app",
      schema: {},
      serverUrl: "https://example.test",
      cookieSession: {
        user_id: "alice",
        claims: {
          role: "reader",
          auth_mode: "external",
          subject: "alice-subject",
          issuer: "https://issuer.example",
        },
        authMode: "external",
      },
    });

    client.updateCookieSession({
      user_id: "alice",
      claims: {
        role: "writer",
        auth_mode: "external",
        subject: "alice-subject",
        issuer: "https://issuer.example",
      },
      authMode: "external",
    });

    expect(runtime.updateAuth).toHaveBeenCalledTimes(1);
    const arg = runtime.updateAuth.mock.calls[0][0] as string;
    expect(JSON.parse(arg)).toMatchObject({ jwt_token: null });
  });
});

describe("resolveDefaultDurabilityTier", () => {
  it("uses local as the default offline durability tier", () => {
    expect(resolveDefaultDurabilityTier({})).toBe("local");
  });

  it("still prefers edge when a server is configured outside the browser runtime", () => {
    expect(resolveDefaultDurabilityTier({ serverUrl: "https://example.test" })).toBe("edge");
  });
});

describe("JazzClient runtime schema caching", () => {
  it("reuses the normalized runtime schema while the schema hash is unchanged", () => {
    const schema: WasmSchema = {
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    };
    const runtime = makeFakeRuntime();
    runtime.getSchema.mockReturnValue(schema);
    runtime.getSchemaHash.mockReturnValue("schema-hash-1");
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "schema-cache-app",
      schema,
    });

    expect(client.getSchema()).toBe(schema);
    expect(client.getSchema()).toBe(schema);

    expect(runtime.getSchema).toHaveBeenCalledTimes(1);
    expect(runtime.getSchemaHash).toHaveBeenCalledTimes(2);
  });

  it("refreshes the cached schema when the runtime schema hash changes", () => {
    const firstSchema: WasmSchema = {
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    };
    const secondSchema: WasmSchema = {
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
        policies: {},
      },
    };
    const runtime = makeFakeRuntime();
    runtime.getSchema.mockReturnValueOnce(firstSchema).mockReturnValueOnce(secondSchema);
    runtime.getSchemaHash.mockReturnValueOnce("schema-hash-1").mockReturnValueOnce("schema-hash-2");
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "schema-cache-refresh-app",
      schema: firstSchema,
    });

    expect(client.getSchema()).toBe(firstSchema);
    expect(client.getSchema()).toBe(secondSchema);

    expect(runtime.getSchema).toHaveBeenCalledTimes(2);
  });

  it("skips schema fetches for runtimes that already return declared-schema rows", async () => {
    const schema: WasmSchema = {
      todos: {
        columns: [{ name: "title", column_type: { type: "Text" }, nullable: false }],
      },
    };
    const runtime = makeFakeRuntime();
    runtime.returnsDeclaredSchemaRows = true;
    runtime.query.mockResolvedValue([
      {
        id: "todo-1",
        values: [{ type: "Text", value: "already aligned" }],
      },
    ]);
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "declared-row-runtime",
      schema,
    });

    await expect(
      client.query({
        _schema: schema,
        _build: () =>
          JSON.stringify({
            table: "todos",
            conditions: [],
            includes: {},
            orderBy: [],
          }),
      }),
    ).resolves.toEqual([
      {
        id: "todo-1",
        values: [{ type: "Text", value: "already aligned" }],
      },
    ]);

    expect(runtime.getSchemaHash).not.toHaveBeenCalled();
    expect(runtime.getSchema).not.toHaveBeenCalled();
  });
});

describe("JazzClient transactions", () => {
  it("returns a write handle from commit so callers can wait on the batch", async () => {
    const runtime = makeFakeRuntime();
    const client = JazzClient.connectWithRuntime(runtime as any, makeContext());
    const waitForPersistedBatch = vi
      .spyOn(client, "waitForPersistedBatch")
      .mockResolvedValue(undefined);

    const committed = client.beginTransaction().commit();

    expect(runtime.sealBatch).toHaveBeenCalledTimes(1);
    expect(committed).toBeInstanceOf(WriteHandle);
    expect(committed.batchId).toBeDefined();
    await expect(committed.wait({ tier: "edge" })).resolves.toBeUndefined();
    expect(waitForPersistedBatch).toHaveBeenCalledWith(committed.batchId, "edge");
  });
});

describe("JazzClient mutation error handling", () => {
  function makeRejectedBatchRecord(batchId: string) {
    return {
      batchId,
      mode: "direct" as const,
      sealed: true,
      latestSettlement: {
        kind: "rejected" as const,
        batchId,
        code: "permission_denied",
        reason: "write rejected by policy",
      },
    };
  }

  it("replays queued rejected batches to new listeners without scanning all batch records", () => {
    const runtime = makeFakeRuntime();
    runtime.drainRejectedBatchIds = vi.fn(() => ["batch-rejected"]);
    runtime.loadLocalBatchRecord = vi.fn((batchId: string) => makeRejectedBatchRecord(batchId));
    runtime.loadLocalBatchRecords = vi.fn(() => {
      throw new Error("should not scan all local batch records");
    });
    runtime.acknowledgeRejectedBatch = vi.fn(() => true);
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "queued-rejection-app",
      schema: {},
    });

    const seen: MutationErrorEvent[] = [];

    client.onMutationError((event) => {
      seen.push(event);
    });

    expect(runtime.drainRejectedBatchIds).toHaveBeenCalledTimes(1);
    expect(runtime.loadLocalBatchRecord).toHaveBeenCalledWith("batch-rejected");
    expect(runtime.loadLocalBatchRecords).not.toHaveBeenCalled();
    expect(runtime.acknowledgeRejectedBatch).toHaveBeenCalledWith("batch-rejected");
    expect(seen).toEqual([
      {
        code: "permission_denied",
        reason: "write rejected by policy",
        batch: makeRejectedBatchRecord("batch-rejected"),
      },
    ]);
  });

  it("checks only runtime-reported rejected batch ids after sync", () => {
    const runtime = makeFakeRuntime();
    runtime.drainRejectedBatchIds = vi
      .fn<() => string[]>(() => [])
      .mockReturnValueOnce([])
      .mockReturnValueOnce(["batch-rejected"]);
    runtime.loadLocalBatchRecord = vi.fn((batchId: string) => makeRejectedBatchRecord(batchId));
    runtime.loadLocalBatchRecords = vi.fn(() => {
      throw new Error("should not scan all local batch records");
    });
    runtime.acknowledgeRejectedBatch = vi.fn(() => true);
    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "sync-rejection-app",
      schema: {},
    });

    const seen: MutationErrorEvent[] = [];
    client.onMutationError((event) => {
      seen.push(event);
    });

    client.getRuntime().onSyncMessageReceived("sync-payload");

    expect(runtime.drainRejectedBatchIds).toHaveBeenCalledTimes(2);
    expect(runtime.loadLocalBatchRecord).toHaveBeenCalledWith("batch-rejected");
    expect(runtime.loadLocalBatchRecords).not.toHaveBeenCalled();
    expect(seen).toEqual([
      {
        code: "permission_denied",
        reason: "write rejected by policy",
        batch: makeRejectedBatchRecord("batch-rejected"),
      },
    ]);
  });
});

describe("JazzClient batch settlement requests", () => {
  it("requests settlement tracking once for unresolved batch waits", async () => {
    const runtime = makeFakeRuntime();
    runtime.loadLocalBatchRecord.mockImplementation(() => ({
      batchId: "batch-edge",
      mode: "direct",
      sealed: true,
      latestSettlement: {
        kind: "durableDirect",
        batchId: "batch-edge",
        confirmedTier: "local",
        visibleMembers: [],
      },
    }));

    const client = JazzClient.connectWithRuntime(runtime as any, {
      appId: "batch-wait-request-app",
      schema: {},
    });

    void client.waitForPersistedBatch("batch-edge", "edge").catch(() => undefined);
    expect(runtime.requestBatchSettlements).toHaveBeenCalledWith(["batch-edge"]);

    await new Promise((resolve) => setTimeout(resolve, 60));
    expect(runtime.requestBatchSettlements).toHaveBeenCalledTimes(1);
    await client.shutdown();
  });
});
