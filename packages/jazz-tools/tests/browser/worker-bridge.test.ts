/**
 * Browser integration tests for Worker Bridge + OPFS persistence.
 *
 * Runs in a real Chromium browser via @vitest/browser + playwright.
 * Uses real jazz-wasm, real dedicated Workers, real OPFS storage.
 *
 * Server sync tests use a real jazz-tools server spawned by global-setup.
 */

import { describe, it, expect, afterEach } from "vitest";
import { createDb, Db, type QueryBuilder } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import {
  TestCleanup,
  createSyncedDb,
  sleep,
  uniqueDbName,
  waitForCondition,
  waitForQuery,
  waitForWorkerMessageType,
  withTimeout,
} from "./support.js";
import {
  blockTestingServerNetwork,
  getIsolatedTestingServerInfo,
  getTestingServerInfo,
  getTestingServerJwtForUser,
  getTestingServerNetworkDebug,
  type TestingServerInfo,
  unblockTestingServerNetwork,
} from "./testing-server.js";
import {
  closeRemoteBrowserDb,
  createRemoteBrowserDb,
  waitForRemoteBrowserDbTitle,
} from "./remote-browser-db.js";
import { CompiledPermissions, schema as s } from "../../src/";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../../src/runtime/schema-fetch.js";

interface DebugLensEdgeState {
  sourceHash: string;
  targetHash: string;
}

interface DebugSchemaState {
  currentSchemaHash: string;
  liveSchemaHashes: string[];
  knownSchemaHashes: string[];
  pendingSchemaHashes: string[];
  lensEdges: DebugLensEdgeState[];
}

// ---------------------------------------------------------------------------
// Test schema — a simple "todos" table
// ---------------------------------------------------------------------------

const schema = {
  projects: s.table({
    name: s.string(),
  }),
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    projectId: s.ref("projects").optional(),
    tags: s.array(s.string()).optional(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
const app: s.App<AppSchema> = s.defineApp(schema);
const { projects, todos } = app;
type Todo = s.RowOf<typeof todos>;

const rejectAllPermissions = s.definePermissions(app, ({ policy }) => [
  policy.projects.allowRead.never(),
  policy.projects.allowInsert.never(),
  policy.projects.allowUpdate.never(),
  policy.projects.allowDelete.never(),
  policy.todos.allowRead.never(),
  policy.todos.allowInsert.never(),
  policy.todos.allowUpdate.never(),
  policy.todos.allowDelete.never(),
]);

interface WorkerMessageDebugEvent {
  atMs: number;
  type: string;
  details?: Record<string, unknown>;
}

interface WorkerMessageProbe {
  dispose(): void;
  snapshot(): WorkerMessageDebugEvent[];
}

function summarizeWorkerMessage(
  data: { type?: string; [key: string]: unknown } | undefined,
): Record<string, unknown> | undefined {
  if (!data?.type) {
    return undefined;
  }

  switch (data.type) {
    case "init-ok":
      return { clientId: data.clientId };
    case "error":
      return { message: data.message };
    case "sync":
      return { payloadCount: Array.isArray(data.payload) ? data.payload.length : undefined };
    case "peer-sync":
      return {
        peerId: data.peerId,
        term: data.term,
        payloadCount: Array.isArray(data.payload) ? data.payload.length : undefined,
      };
    default:
      return undefined;
  }
}

function attachWorkerMessageProbe(db: Db): WorkerMessageProbe {
  const worker = (db as unknown as { worker?: Worker | null }).worker;
  const startedAt = Date.now();
  const events: WorkerMessageDebugEvent[] = [];

  if (!worker) {
    return {
      dispose() {},
      snapshot() {
        return [];
      },
    };
  }

  const handler = (event: MessageEvent<{ type?: string; [key: string]: unknown }>) => {
    events.push({
      atMs: Date.now() - startedAt,
      type: event.data?.type ?? "unknown",
      details: summarizeWorkerMessage(event.data),
    });
    if (events.length > 40) {
      events.shift();
    }
  };

  worker.addEventListener("message", handler);

  return {
    dispose() {
      worker.removeEventListener("message", handler);
    },
    snapshot() {
      return [...events];
    },
  };
}

function getDbWorkerDebugState(db: Db): Record<string, unknown> {
  const anyDb = db as unknown as {
    tabRole?: unknown;
    tabId?: unknown;
    currentLeaderTabId?: unknown;
    currentLeaderTerm?: unknown;
    activeRemoteLeaderTabId?: unknown;
    primaryDbName?: unknown;
    workerDbName?: unknown;
    workerBridge?: {
      state?: {
        phase?: unknown;
        workerClientId?: unknown;
        expectsUpstreamServer?: unknown;
        upstreamServerConnected?: unknown;
        resolveUpstreamServerReady?: unknown;
        serverPayloadForwarder?: unknown;
        pendingSyncPayloadsForWorker?: unknown[];
      };
    } | null;
  };
  const bridgeState = anyDb.workerBridge?.state;

  return {
    tabRole: anyDb.tabRole,
    tabId: anyDb.tabId,
    currentLeaderTabId: anyDb.currentLeaderTabId,
    currentLeaderTerm: anyDb.currentLeaderTerm,
    activeRemoteLeaderTabId: anyDb.activeRemoteLeaderTabId,
    primaryDbName: anyDb.primaryDbName,
    workerDbName: anyDb.workerDbName,
    bridge: bridgeState
      ? {
          phase: bridgeState.phase,
          workerClientId: bridgeState.workerClientId,
          expectsUpstreamServer: bridgeState.expectsUpstreamServer,
          upstreamServerConnected: bridgeState.upstreamServerConnected,
          waitingForUpstreamServer: Boolean(bridgeState.resolveUpstreamServerReady),
          hasServerPayloadForwarder: Boolean(bridgeState.serverPayloadForwarder),
          pendingSyncPayloadsForWorker: bridgeState.pendingSyncPayloadsForWorker?.length ?? 0,
        }
      : null,
  };
}

async function rethrowWithWorkerDiagnostics(
  label: string,
  error: unknown,
  serverUrl: string,
  dbs: Array<{ name: string; db: Db; probe?: WorkerMessageProbe }>,
): Promise<never> {
  const network = await getTestingServerNetworkDebug(serverUrl);
  const diagnostics = {
    network,
    dbs: dbs.map(({ name, db, probe }) => ({
      name,
      state: getDbWorkerDebugState(db),
      recentWorkerMessages: probe?.snapshot() ?? [],
    })),
  };
  const message = error instanceof Error ? error.message : String(error);
  throw new Error(`${label}: ${message}\nDiagnostics: ${JSON.stringify(diagnostics, null, 2)}`);
}

/** QueryBuilder that selects all todos. */
const allTodos: QueryBuilder<Todo> = app.todos;

/** QueryBuilder that selects all todos by project. */
function todosByProject(projectId: string): QueryBuilder<Todo> {
  return app.todos.where({ projectId });
}

// Fixture schema family pushed by global-setup (`examples/todo-server-rs/schema`), v2.
const catalogueSchemaV1: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "completed", column_type: { type: "Boolean" }, nullable: false },
    ],
  },
};

const catalogueSchemaV2: WasmSchema = {
  todos: {
    columns: [
      { name: "title", column_type: { type: "Text" }, nullable: false },
      { name: "completed", column_type: { type: "Boolean" }, nullable: false },
      { name: "description", column_type: { type: "Text" }, nullable: true },
    ],
  },
};

interface CatalogueTodo {
  id: string;
  title: string;
  completed: boolean;
  description?: string;
}

const allCatalogueTodos: QueryBuilder<CatalogueTodo> = {
  _table: "todos",
  _schema: catalogueSchemaV2,
  _rowType: {} as CatalogueTodo,
  _build() {
    return JSON.stringify({
      table: "todos",
      conditions: [],
      includes: {},
      orderBy: [],
    });
  },
};

/**
 * Sets up a server with the given app schema and permissions.
 */
async function getServerWithPermissions(
  app: { wasmSchema: WasmSchema },
  permissions: CompiledPermissions,
): Promise<{ appId: string; serverUrl: string; adminSecret: string }> {
  const { appId, serverUrl, adminSecret } = await getIsolatedTestingServerInfo();
  const { hash: schemaHash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema: app.wasmSchema,
  });
  const { head } = await fetchPermissionsHead(serverUrl, { appId, adminSecret });
  await publishStoredPermissions(serverUrl, {
    appId,
    adminSecret,
    schemaHash,
    permissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });
  return { appId, serverUrl, adminSecret };
}

/**
 * Creates a non-admin Db with the given appId and serverUrl.
 */
async function getNonAdminClientDb(
  appId: string,
  serverUrl: string,
  ctx: TestCleanup,
): Promise<Db> {
  const jwtToken = await getTestingServerJwtForUser(
    "browser-offline-rejected-wait",
    {
      role: "user",
    },
    appId,
  );
  return ctx.track(
    await createDb({
      appId,
      driver: { type: "persistent", dbName: uniqueDbName("sync-wait-edge-rejected-offline") },
      serverUrl,
      jwtToken,
    }),
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Worker Bridge with OPFS", () => {
  const ctx = new TestCleanup();
  const remoteBrowserDbIds = new Set<string>();

  function trackRemoteBrowserDb(id: string): string {
    remoteBrowserDbIds.add(id);
    return id;
  }

  async function waitForRemoteTodoTitle(
    id: string,
    title: string,
    label: string,
    timeoutMs: number,
    tier?: "local" | "edge",
  ): Promise<Record<string, unknown>[]> {
    try {
      return await waitForRemoteBrowserDbTitle({ id, title, timeoutMs, tier });
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw new Error(`${label}: ${message}`);
    }
  }

  /** Shorthand: track a Db for cleanup. */
  function track(db: Db): Db {
    return ctx.track(db);
  }

  /** Shorthand: track a subscription for cleanup. */
  function trackSubscription(unsubscribe: () => void): () => void {
    return ctx.trackSubscription(unsubscribe);
  }

  function untrack(db: Db): void {
    ctx.untrack(db);
  }

  function getTabRole(db: Db): "leader" | "follower" | null {
    const role = (db as any).tabRole;
    if (role === "leader" || role === "follower") {
      return role;
    }
    return null;
  }

  async function waitForLeaderAndFollower(a: Db, b: Db): Promise<{ leader: Db; follower: Db }> {
    await waitForCondition(
      async () => {
        const roleA = getTabRole(a);
        const roleB = getTabRole(b);
        return roleA === "leader" && roleB === "follower";
      },
      12000,
      "Expected one elected leader and one follower",
    ).catch(async () => {
      await waitForCondition(
        async () => {
          const roleA = getTabRole(a);
          const roleB = getTabRole(b);
          return roleA === "follower" && roleB === "leader";
        },
        12000,
        "Expected one elected leader and one follower",
      );
    });

    const roleA = getTabRole(a);
    const roleB = getTabRole(b);
    if (roleA === "leader" && roleB === "follower") {
      return { leader: a, follower: b };
    }
    if (roleA === "follower" && roleB === "leader") {
      return { leader: b, follower: a };
    }
    throw new Error("Unable to determine leader/follower roles");
  }

  async function waitForSingleLeader(tabs: Db[]): Promise<Db> {
    await waitForCondition(
      async () => {
        let leaders = 0;
        let knownRoles = 0;
        for (const tab of tabs) {
          const role = getTabRole(tab);
          if (!role) continue;
          knownRoles += 1;
          if (role === "leader") leaders += 1;
        }
        return knownRoles === tabs.length && leaders === 1;
      },
      12000,
      "Expected exactly one elected leader across tabs",
    );

    const leader = tabs.find((tab) => getTabRole(tab) === "leader");
    if (!leader) {
      throw new Error("Expected one leader after convergence");
    }
    return leader;
  }

  afterEach(async () => {
    for (const id of remoteBrowserDbIds) {
      try {
        await closeRemoteBrowserDb(id);
      } catch {
        // Best effort
      }
    }
    remoteBrowserDbIds.clear();
    await ctx.cleanup();
  });

  // -------------------------------------------------------------------------
  // 1. Worker initialization
  // -------------------------------------------------------------------------

  it("creates Db with worker in browser environment", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("init") },
      }),
    );
    expect(db).toBeDefined();
    expect(db).toBeInstanceOf(Db);
  });

  // -------------------------------------------------------------------------
  // 2. Insert + local query through worker bridge
  // -------------------------------------------------------------------------

  it("inserts a row and queries it back", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("insert-query") },
      }),
    );

    // Insert (sync — runs on main-thread in-memory runtime)
    const {
      value: { id },
    } = db.insert(todos, { title: "Buy milk", done: false });
    expect(id).toBeTruthy();
    expect(typeof id).toBe("string");

    // Query (async — runs on main-thread runtime)
    const results = await db.all(allTodos);
    expect(results.length).toBe(1);
    expect(results[0].id).toBe(id);
    expect(results[0].title).toBe("Buy milk");
    expect(results[0].done).toBe(false);
  });

  it("inserts multiple rows and queries all", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("multi-insert") },
      }),
    );

    db.insert(todos, { title: "Task A", done: false });
    db.insert(todos, { title: "Task B", done: true });
    db.insert(todos, { title: "Task C", done: false });

    const results = await db.all(allTodos);
    expect(results.length).toBe(3);

    const titles = results.map((r) => r.title).sort();
    expect(titles).toEqual(["Task A", "Task B", "Task C"]);
  });

  it("sync insert before bridge init is persisted after init completes", async () => {
    const dbName = uniqueDbName("sync-insert-before-bridge-ready");
    const db1 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    // First I/O operation, bridge hasn't been initialized yet.
    const {
      value: { id },
    } = db1.insert(todos, { title: "Test", done: false });

    await waitForCondition(
      async () => {
        const row = await db1.one(allTodos, { tier: "local" });
        return row?.id === id;
      },
      8_000,
      "sync insert should be forwarded to worker after bridge init",
    );

    await db1.shutdown();
    untrack(db1);

    const db2 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    const persistedRow = await db2.one(allTodos, { tier: "local" });
    expect(persistedRow?.id).toBe(id);
  });

  it("sync insert is not persisted if bridge fails to init", async () => {
    const dbName = uniqueDbName("sync-insert-bridge-init-failure");
    const db1 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    // @ts-expect-error - worker is private
    const worker = db1.worker as Worker;
    const originalPostMessage = worker.postMessage.bind(worker);
    worker.postMessage = ((message: unknown, transfer?: Transferable[]) => {
      const typed = message as { type?: string } | undefined;
      if (typed?.type === "init") {
        queueMicrotask(() => {
          worker.dispatchEvent(
            new MessageEvent("message", {
              data: { type: "error", message: "forced bridge init failure for test" },
            }),
          );
        });
        return;
      }
      return originalPostMessage(message, { transfer });
    }) as Worker["postMessage"];

    const {
      value: { id },
    } = db1.insert(todos, { title: "Test", done: false });
    expect(id).toBeDefined();

    worker.postMessage = originalPostMessage;
    // Shutdown fails to ensure bridge is ready, but steps down as leader before that
    await expect(db1.shutdown()).rejects.toThrow(
      "Worker init failed: forced bridge init failure for test",
    );

    untrack(db1);

    const db2 = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    const persistedRows = await db2.all(allTodos, { tier: "local" });
    expect(persistedRows.length).toEqual(0);
  });

  it("query rejects if bridge fails to init", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("query-bridge-init-failure") },
      }),
    );

    // @ts-expect-error - worker is private
    const worker = db.worker as Worker;
    const originalPostMessage = worker.postMessage.bind(worker);
    worker.postMessage = ((message: unknown, transfer?: Transferable[]) => {
      const typed = message as { type?: string } | undefined;
      if (typed?.type === "init") {
        queueMicrotask(() => {
          worker.dispatchEvent(
            new MessageEvent("message", {
              data: { type: "error", message: "forced bridge init failure for query test" },
            }),
          );
        });
        return;
      }
      return originalPostMessage(message, { transfer });
    }) as Worker["postMessage"];

    await expect(db.all(allTodos, { tier: "local" })).rejects.toThrow(
      "Worker init failed: forced bridge init failure for query test",
    );

    worker.postMessage = originalPostMessage;
  });

  // -------------------------------------------------------------------------
  // 3. Update + delete through worker bridge
  // -------------------------------------------------------------------------

  it("updates a row", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("update") },
      }),
    );

    const { value: inserted } = db.insert(todos, { title: "Original", done: false });
    const { id } = inserted;
    const result = db.update(todos, id, { done: true });
    expect(result).toMatchObject({
      wait: expect.any(Function),
    });

    const results = await db.all(allTodos);
    expect(results.length).toBe(1);
    expect(results[0].title).toBe("Original");
    expect(results[0].done).toBe(true);
  });

  it("updates a row durably", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("update-durable") },
      }),
    );

    const { id } = await db
      .insert(todos, { title: "Original", done: false })
      .wait({ tier: "local" });

    const updateHandle = db.update(todos, id, { done: true });
    await updateHandle.wait({ tier: "local" });

    const results = await db.all(allTodos, { tier: "local" });
    expect(results.length).toBe(1);
    expect(results[0].done).toBe(true);
  });

  it("deletes a row", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete") },
      }),
    );

    const { value: inserted } = db.insert(todos, { title: "Ephemeral", done: false });
    const { id } = inserted;
    expect((await db.all(allTodos)).length).toBe(1);

    const result = db.delete(todos, id);
    expect(result).toMatchObject({
      wait: expect.any(Function),
    });
    const results = await db.all(allTodos);
    expect(results.length).toBe(0);
  });

  it("deletes a row durably", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete-durable") },
      }),
    );

    const { id } = await db
      .insert(todos, { title: "Ephemeral", done: false })
      .wait({ tier: "local" });
    expect((await db.all(allTodos, { tier: "local" })).length).toBe(1);

    const deleteHandle = db.delete(todos, id);
    await deleteHandle.wait({ tier: "local" });

    const results = await db.all(allTodos, { tier: "local" });
    expect(results.length).toBe(0);
  });

  // -------------------------------------------------------------------------
  // 4. OPFS persistence across shutdown + re-open
  // -------------------------------------------------------------------------

  it("persists data across shutdown and re-create (OPFS)", async () => {
    const dbName = uniqueDbName("persistence");

    const db1 = await createDb({ appId: "test-app", driver: { type: "persistent", dbName } });
    db1.insert(todos, { title: "Survive reload", done: true });
    const before = await db1.all(allTodos);
    expect(before.length).toBe(1);
    await db1.shutdown();

    // New Db with same dbName — worker reopens OPFS, main thread starts empty.
    // Using "local" settled tier makes the query wait for the worker's
    // QuerySettled response, ensuring OPFS data arrives before resolving.
    const db2 = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const after = await db2.all(allTodos, { tier: "local" });
    expect(after.length).toBe(1);
    expect(after[0].title).toBe("Survive reload");
    expect(after[0].done).toBe(true);
  });

  it("recovers data from WAL after crash (no snapshot flush)", async () => {
    const dbName = uniqueDbName("crash-recovery");

    const db1 = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    // wait({ tier: "local" }) ensures data is in OPFS WAL before we crash
    await db1.insert(todos, { title: "Crash-proof", done: false }).wait({ tier: "local" });
    await db1.insert(todos, { title: "Also survives", done: true }).wait({ tier: "local" });

    // Simulate crash: release OPFS handles WITHOUT flushing snapshot.
    // WAL has the data, but snapshot is stale. Recovery must replay WAL.
    // (Real worker.terminate() doesn't reliably release OPFS exclusive
    // locks within the same page session — only a full page reload does.)
    await (db1 as any).ensureBridgeReady();
    const worker = (db1 as any).worker as Worker;
    worker.postMessage({ type: "simulate-crash" });
    await waitForWorkerMessageType(worker, "shutdown-ok", 5000, "simulate-crash");
    worker.terminate();
    // Null out dead worker bridge so Db shutdown only frees client-side resources.
    (db1 as any).worker = null;
    (db1 as any).workerBridge = null;
    await db1.shutdown();

    // New Db with same dbName — worker must recover from OPFS WAL
    const db2 = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const after = await db2.all(allTodos, { tier: "local" });
    expect(after.length).toBe(2);

    const titles = after.map((r) => r.title).sort();
    expect(titles).toEqual(["Also survives", "Crash-proof"]);
  });

  it("recovers from OPFS handle conflict after abrupt worker termination", async () => {
    // Hold a WritableStream on the OPFS file from the main thread — this
    // blocks createSyncAccessHandle in the worker, exactly like a stale
    // handle from a previous page load.
    //
    //  main thread: createWritable() ──── holds ──── close()
    //  worker:            createSyncAccessHandle() → conflict → retry → success
    //
    const dbName = uniqueDbName("handle-conflict");
    // Coupled to OpfsFile::file_name() in crates/opfs-btree/src/file.rs
    const fileName = `${dbName}.opfsbtree`;

    // Pre-create the OPFS file and hold a writable stream to block the worker.
    const root = await navigator.storage.getDirectory();
    const fileHandle = await root.getFileHandle(fileName, { create: true });
    const writable = await fileHandle.createWritable();

    // Start the Db — its worker will hit NoModificationAllowedError and retry.
    const dbPromise = createDb({ appId: "test-app", driver: { type: "persistent", dbName } });

    // Release the lock after 100ms so the retry can succeed.
    setTimeout(() => writable.close(), 100);

    const db = track(await dbPromise);
    const rows = await db.all(allTodos, { tier: "local" });
    expect(rows).toEqual([]);
  });

  it("deletes OPFS storage for the current namespace and keeps the same Db usable", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("delete-storage") },
      }),
    );

    await db.insert(todos, { title: "Should be deleted", done: false }).wait({ tier: "local" });
    const before = await db.all(allTodos, { tier: "local" });
    expect(before.length).toBe(1);
    expect(before[0].title).toBe("Should be deleted");

    await db.deleteClientStorage();

    const afterDelete = await db.all(allTodos, { tier: "local" });
    expect(afterDelete).toEqual([]);

    const {
      value: { id },
    } = db.insert(todos, { title: "Fresh after delete", done: true });
    const afterReinsert = await db.all(allTodos, { tier: "local" });
    expect(afterReinsert).toHaveLength(1);
    expect(afterReinsert[0].id).toBe(id);
    expect(afterReinsert[0].title).toBe("Fresh after delete");
    expect(afterReinsert[0].done).toBe(true);
  });

  it("deletes OPFS storage across leader and follower tabs when requested from a follower", async () => {
    const dbName = uniqueDbName("delete-storage-follower");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const { leader, follower } = await waitForLeaderAndFollower(dbA, dbB);

    await leader
      .insert(todos, { title: "Leader data before follower wipe", done: false })
      .wait({ tier: "local" });
    await follower
      .insert(todos, { title: "Follower data before follower wipe", done: true })
      .wait({ tier: "local" });

    await waitForCondition(
      async () => {
        const leaderRows = await leader.all(allTodos, { tier: "local" });
        const followerRows = await follower.all(allTodos, { tier: "local" });
        return leaderRows.length === 2 && followerRows.length === 2;
      },
      8000,
      "Leader and follower should both observe pre-wipe rows",
    );

    await follower.deleteClientStorage();

    await waitForCondition(
      async () => {
        const leaderRows = await leader.all(allTodos, { tier: "local" });
        const followerRows = await follower.all(allTodos, { tier: "local" });
        return leaderRows.length === 0 && followerRows.length === 0;
      },
      12000,
      "Follower-initiated storage wipe should clear both leader and follower namespaces",
    );

    const marker = `fresh-after-follower-wipe-${Date.now()}`;
    await leader.insert(todos, { title: marker, done: false }).wait({ tier: "local" });

    await waitForCondition(
      async () => {
        const leaderRows = await leader.all(allTodos, { tier: "local" });
        const followerRows = await follower.all(allTodos, { tier: "local" });
        const leaderHas = leaderRows.some((row) => row.title === marker);
        const followerHas = followerRows.some((row) => row.title === marker);
        return leaderHas && followerHas;
      },
      12000,
      "Both tabs should recover cleanly after follower-initiated storage wipe",
    );
  });

  it("logout with wipeData clears browser storage before the next session opens", async () => {
    const dbName = uniqueDbName("logout-wipe");
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );

    await db
      .insert(todos, { title: "Should be wiped on logout", done: false })
      .wait({ tier: "local" });
    expect((await db.all(allTodos, { tier: "local" })).length).toBe(1);

    await db.logout({ wipeData: true });
    untrack(db);

    const reopened = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName },
      }),
    );
    const rows = await reopened.all(allTodos, { tier: "local" });
    expect(rows).toEqual([]);
  });

  it("rehydrates worker catalogue schemas/lenses and restores them on main thread", async () => {
    const dbName = uniqueDbName("catalogue-schema-lens-rehydrate");
    const seeded = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    // Initialize worker/main runtimes with schema v2 from client context.
    await seeded.all(allCatalogueTodos, { tier: "local" });

    // Seed historical v1 schema + auto lens v1->v2 directly into worker OPFS.
    await seedWorkerLiveSchema(seeded, catalogueSchemaV1);

    await waitForCondition(
      async () => {
        const state = await getWorkerDebugSchemaState(seeded);
        return hasRestoredCatalogueState(state);
      },
      12_000,
      "Seeded worker should hold schema/lens state beyond client context",
    );

    await seeded.shutdown();
    untrack(seeded);

    const offline = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    await offline.all(allCatalogueTodos, { tier: "local" });

    await waitForCondition(
      async () => {
        const state = await getWorkerDebugSchemaState(offline);
        return hasRestoredCatalogueState(state);
      },
      12_000,
      "Offline worker should rehydrate schema/lens state from OPFS manifest",
    );

    await waitForCondition(
      async () => {
        await offline.all(allCatalogueTodos, { tier: "local" });
        const mainState = getMainDebugSchemaState(offline, catalogueSchemaV2);
        return hasRestoredCatalogueState(mainState);
      },
      12_000,
      "Main thread should restore schema/lens state via worker catalogue sync",
    );
  }, 90_000);

  // -------------------------------------------------------------------------
  // 5. Durable insert resolves at local tier
  // -------------------------------------------------------------------------

  it("insert resolves when local acks", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("with-ack") },
      }),
    );

    // insert("local") should resolve once the worker's OPFS has it
    const result = db.insert(todos, { title: "Durable", done: false });
    await result.wait({ tier: "local" });
  });

  // -------------------------------------------------------------------------
  // 6. Subscription through worker bridge
  // -------------------------------------------------------------------------

  it("subscriptions fire on insert", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("subscribe") },
      }),
    );

    const received: Todo[][] = [];

    const unsub = trackSubscription(
      db.subscribeAll(allTodos, (delta) => {
        received.push([...delta.all]);
      }),
    );

    db.insert(todos, { title: "Observed", done: false });

    // Wait for subscription to fire
    await waitForCondition(
      async () => received.some((r) => r.length > 0),
      3000,
      "Subscription should fire after insert",
    );

    const last = received[received.length - 1];
    expect(last.length).toBe(1);
    expect(last[0].title).toBe("Observed");

    unsub();
  });

  it("subscriptions fire when using queries with filters", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("subscribe") },
      }),
    );

    const received: Todo[][] = [];

    const {
      value: { id: projectId },
    } = db.insert(projects, { name: "Observed Project" });
    const unsub = trackSubscription(
      db.subscribeAll(todosByProject(projectId), (delta) => {
        received.push([...delta.all]);
      }),
    );

    db.insert(todos, { title: "Observed", done: false, projectId });
    const {
      value: { id: anotherProjectId },
    } = db.insert(projects, { name: "Ignored Project" });
    db.insert(todos, { title: "Not observed", done: false, projectId: anotherProjectId });

    // Wait for subscription to fire
    await waitForCondition(
      async () => received.some((r) => r.length > 0),
      3000,
      "Subscription should fire after insert",
    );

    const last = received[received.length - 1];
    expect(last.length).toBe(1);
    expect(last[0].title).toBe("Observed");

    unsub();
  });

  it("delivers an initial scoped subscription snapshot after seeding many synced rows", async () => {
    const sharedLocalAuthToken = generateAuthSecret();
    const syncServer = await publishSyncServerSchemaAndPermissions("subscribe-initial-snapshot");
    const db = await createSyncedDb(
      ctx,
      "subscribe-initial-snapshot",
      sharedLocalAuthToken,
      syncServer,
    );

    const insertedIds: string[] = [];
    for (let i = 0; i < 120; i += 1) {
      const { id } = await db
        .insert(todos, { title: `seeded-${i}`, done: i % 2 === 0 })
        .wait({ tier: "local" });
      insertedIds.push(id);
    }

    const targetId = insertedIds[0];
    const received: Todo[][] = [];
    const unsub = trackSubscription(
      db.subscribeAll(todos.where({ id: targetId }), (delta) => {
        received.push([...delta.all]);
      }),
    );

    await waitForCondition(
      async () =>
        received.some((rows) => rows.length === 1 && rows[0]?.id === targetId && rows[0]?.title),
      8000,
      "Seeded synced row should appear in initial scoped subscription snapshot",
    );

    const last = received[received.length - 1];
    expect(last).toHaveLength(1);
    expect(last[0].id).toBe(targetId);
    expect(last[0].title).toBe("seeded-0");

    unsub();
  }, 60000);

  it("delivers an initial scoped subscription snapshot for jwt-backed synced rows", async () => {
    const { appId, serverUrl, adminSecret } =
      await publishSyncServerSchemaAndPermissions("subscribe-initial-jwt");
    const db = track(
      await createDb({
        appId,
        driver: { type: "persistent", dbName: uniqueDbName("subscribe-initial-jwt") },
        serverUrl,
        adminSecret,
        jwtToken: await getTestingServerJwtForUser("subscribe-initial-jwt", undefined, appId),
      }),
    );

    const insertedIds: string[] = [];
    for (let i = 0; i < 120; i += 1) {
      const { id } = await db
        .insert(todos, { title: `seeded-jwt-${i}`, done: i % 2 === 0 })
        .wait({ tier: "local" });
      insertedIds.push(id);
    }

    const targetId = insertedIds[0];
    const received: Todo[][] = [];
    const unsub = trackSubscription(
      db.subscribeAll(todos.where({ id: targetId }), (delta) => {
        received.push([...delta.all]);
      }),
    );

    await waitForCondition(
      async () =>
        received.some((rows) => rows.length === 1 && rows[0]?.id === targetId && rows[0]?.title),
      8000,
      "JWT-backed seeded row should appear in initial scoped subscription snapshot",
    );

    const last = received[received.length - 1];
    expect(last).toHaveLength(1);
    expect(last[0].id).toBe(targetId);
    expect(last[0].title).toBe("seeded-jwt-0");

    unsub();
  }, 60000);

  it("forwards page lifecycle hints from main thread to worker bridge", async () => {
    const db = track(
      await createDb({
        appId: "test-app",
        driver: { type: "persistent", dbName: uniqueDbName("lifecycle") },
      }),
    );

    db.insert(todos, { title: "Prime bridge", done: false });
    await (db as any).ensureBridgeReady();

    const bridge = (db as any).workerBridge;
    expect(bridge).toBeTruthy();

    const seenEvents: string[] = [];
    const originalSendLifecycleHint = bridge.sendLifecycleHint.bind(bridge);
    bridge.sendLifecycleHint = (event: string) => {
      seenEvents.push(event);
      originalSendLifecycleHint(event);
    };

    (db as any).onPageHide();
    (db as any).onPageFreeze();
    (db as any).onPageResume();

    expect(seenEvents).toEqual(["pagehide", "freeze", "resume"]);
  });

  // -------------------------------------------------------------------------
  // 7. Server sync through worker
  // -------------------------------------------------------------------------

  it("propagates synced row from client A to client B", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-a-to-b");
    const sharedLocalAuthToken = generateAuthSecret();
    const dbA = await createSyncedDb(ctx, "sync-a", sharedLocalAuthToken, syncServer);
    const dbB = await createSyncedDb(ctx, "sync-b", sharedLocalAuthToken, syncServer);

    const title = `sync-a-to-b-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await withTimeout(
      dbA.insert(todos, { title, done: false }).wait({ tier: "local" }),
      10000,
      "A insert(worker) did not resolve",
    );

    const rowsOnB = await waitForTodos(
      dbB,
      (rows) => rows.some((row) => row.title === title),
      "A -> B propagation",
      20000,
    );
    expect(rowsOnB.some((row) => row.title === title)).toBe(true);
  }, 60000);

  it("propagates synced row from client B to client A", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-b-to-a");
    const sharedLocalAuthToken = generateAuthSecret();
    const dbA = await createSyncedDb(ctx, "sync-a-reverse", sharedLocalAuthToken, syncServer);
    const dbB = await createSyncedDb(ctx, "sync-b-reverse", sharedLocalAuthToken, syncServer);

    const title = `sync-b-to-a-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await withTimeout(
      dbB.insert(todos, { title, done: true }).wait({ tier: "local" }),
      10000,
      "B insert(worker) did not resolve",
    );

    const rowsOnA = await waitForTodos(
      dbA,
      (rows) => rows.some((row) => row.title === title),
      "B -> A propagation",
      20000,
    );
    expect(rowsOnA.some((row) => row.title === title)).toBe(true);
  }, 60000);

  it("resolves insert wait at edge tier through the worker bridge", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-wait-edge");
    const sharedLocalAuthToken = generateAuthSecret();
    const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

    const title = `wait-edge-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    const inserted = db.insert(todos, { title, done: false });
    const { value: insertedTodo } = inserted;

    await withTimeout(inserted.wait({ tier: "edge" }), 10000, "insert wait(edge) did not resolve");

    expect(insertedTodo.id).toBeTruthy();
    expect(insertedTodo.title).toBe(title);

    const rowsAtEdge = await waitForTodos(
      db,
      (rows) => rows.some((row) => row.id === insertedTodo.id && row.title === title),
      "insert wait(edge) row becomes queryable at edge",
      20000,
      "edge",
    );
    expect(rowsAtEdge.some((row) => row.id === insertedTodo.id)).toBe(true);
  }, 60000);

  it("server permissions check rejects client optimistic insert", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions(
      "sync-wait-edge",
      rejectAllPermissions,
    );

    const sharedLocalAuthToken = generateAuthSecret();
    const db = await createSyncedDb(ctx, "sync-wait-edge", sharedLocalAuthToken, syncServer);

    const insertResult = db.insert(todos, { title: "Rejected", done: false });
    await expect(insertResult.wait({ tier: "edge" })).rejects.toMatchObject({
      name: "PersistedWriteRejectedError",
      batchId: insertResult.batchId,
      code: "permission_denied",
    });
  });

  it("recovers sync after browser-side network loss with B in a separate context", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-recover");
    const sharedLocalAuthToken = generateAuthSecret();
    const { appId, serverUrl, adminSecret } = syncServer;
    const dbA = await createSyncedDb(ctx, "sync-recover-a", sharedLocalAuthToken, syncServer);
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("sync-recover-remote"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId,
      dbName: uniqueDbName("sync-recover-b"),
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      serverUrl,
      adminSecret,
      localFirstSecret: sharedLocalAuthToken,
    });

    const baselineTitle = `baseline-network-recover-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: baselineTitle, done: false }).wait({ tier: "local" }),
      10000,
      "Baseline insert(worker) did not resolve",
    );

    await waitForRemoteTodoTitle(
      remoteDbId,
      baselineTitle,
      "B sees baseline row before browser-side network block",
      20000,
    );

    await blockTestingServerNetwork(serverUrl);
    await sleep(500);
    await unblockTestingServerNetwork(serverUrl);
    await sleep(250);

    (dbA as any).sendLifecycleHint?.("freeze");
    await sleep(50);
    (dbA as any).sendLifecycleHint?.("resume");
    (dbA as any).workerBridge?.replayServerConnection?.();

    const recoveredTitle = `network-recovered-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: recoveredTitle, done: false }).wait({ tier: "local" }),
      10000,
      "Recovered insert(worker) did not resolve",
    );

    const rowsOnB = await waitForRemoteTodoTitle(
      remoteDbId,
      recoveredTitle,
      "B sees row written after browser-side network recovery",
      20000,
    );
    expect(rowsOnB.some((row) => row.title === recoveredTitle)).toBe(true);
  }, 60000);

  /**
   *   writer ──baseline write──► server
   *   fresh probe starts while server traffic is blocked
   *   probe ──edge query pending──X server
   *   network unblocks
   *   expected: the first fresh edge query completes without needing a second client recreate
   */
  it("replays a fresh edge query once upstream attaches after init", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("edge-late-attach");
    const sharedLocalAuthToken = generateAuthSecret();
    const { serverUrl } = syncServer;
    const dbWriter = await createSyncedDb(
      ctx,
      "edge-late-attach-writer",
      sharedLocalAuthToken,
      syncServer,
    );
    const writerProbe = attachWorkerMessageProbe(dbWriter);
    let probeProbe: WorkerMessageProbe | null = null;

    try {
      const baselineTitle = `edge-late-baseline-${Date.now()}`;
      await withTimeout(
        dbWriter.insert(todos, { title: baselineTitle, done: false }).wait({ tier: "local" }),
        10000,
        "Baseline insert(worker) did not resolve",
      );

      await waitForTodos(
        dbWriter,
        (rows) => rows.some((row) => row.title === baselineTitle),
        "Writer sees baseline row at edge before blocking",
        20000,
        "edge",
      ).catch((error) =>
        rethrowWithWorkerDiagnostics(
          "Writer baseline edge read failed before network block",
          error,
          serverUrl,
          [{ name: "writer", db: dbWriter, probe: writerProbe }],
        ),
      );

      await blockTestingServerNetwork(serverUrl);
      await sleep(250);

      const dbProbe = await createSyncedDb(
        ctx,
        "edge-late-attach-probe",
        sharedLocalAuthToken,
        syncServer,
      );
      probeProbe = attachWorkerMessageProbe(dbProbe);
      const probeRowsPromise = waitForTodos(
        dbProbe,
        (rows) => rows.some((row) => row.title === baselineTitle),
        "Fresh edge query resolves after upstream attach",
        20000,
        "edge",
      ).catch((error) =>
        rethrowWithWorkerDiagnostics(
          "Fresh probe edge read stayed pending after unblock",
          error,
          serverUrl,
          [
            { name: "writer", db: dbWriter, probe: writerProbe },
            { name: "probe", db: dbProbe, probe: probeProbe ?? undefined },
          ],
        ),
      );

      await sleep(500);
      await unblockTestingServerNetwork(serverUrl);
      await sleep(250);

      const rowsOnProbe = await probeRowsPromise;
      expect(rowsOnProbe.some((row) => row.title === baselineTitle)).toBe(true);
    } finally {
      probeProbe?.dispose();
      writerProbe.dispose();
      await unblockTestingServerNetwork(serverUrl);
    }
  }, 60000);

  /**
   *   A ──baseline write──► server ◄── B sees baseline
   *   browser blocks Jazz server traffic without reloading the page
   *   A ──offline write(worker)──X server
   *   A ──new online write──► server ◄── B sees control write
   *   expected: the earlier offline worker write also promotes to B + fresh edge client
   */
  it("promotes offline worker rows after reconnect while the worker stays alive", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-offline");
    const sharedLocalAuthToken = generateAuthSecret();
    const { appId, serverUrl, adminSecret } = syncServer;
    const dbA = await createSyncedDb(ctx, "sync-offline-a", sharedLocalAuthToken, syncServer);
    const dbAProbe = attachWorkerMessageProbe(dbA);
    const remoteDbId = trackRemoteBrowserDb(uniqueDbName("sync-offline-remote"));
    await createRemoteBrowserDb({
      id: remoteDbId,
      appId,
      dbName: uniqueDbName("sync-offline-b"),
      table: "todos",
      schemaJson: JSON.stringify(app.wasmSchema),
      serverUrl,
      adminSecret,
      localFirstSecret: sharedLocalAuthToken,
    });

    const baselineTitle = `baseline-before-offline-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: baselineTitle, done: false }).wait({ tier: "local" }),
      10000,
      "Baseline insert(worker) did not resolve",
    );

    await waitForRemoteTodoTitle(
      remoteDbId,
      baselineTitle,
      "B sees baseline row before disconnect",
      20000,
    );

    await blockTestingServerNetwork(serverUrl);
    // Disconnect the WS transport so the block takes effect immediately.
    // Playwright route blocking only intercepts new connections; the existing
    // WebSocket must be closed explicitly for the offline simulation to hold.
    (dbA as any).workerBridge?.disconnectUpstream?.();
    await sleep(250);

    const offlineTitle = `offline-worker-row-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: offlineTitle, done: true }).wait({ tier: "local" }),
      10000,
      "Offline insert(worker) did not resolve",
    );

    await waitForTodos(
      dbA,
      (rows) => rows.some((row) => row.title === offlineTitle),
      "A sees offline worker row locally",
      10000,
      "local",
    );

    await expect(
      waitForRemoteTodoTitle(
        remoteDbId,
        offlineTitle,
        "B should not see offline row while A is disconnected",
        2500,
      ),
    ).rejects.toThrow();

    await unblockTestingServerNetwork(serverUrl);
    // Re-establish the worker's upstream WebSocket now that the network is live again.
    (dbA as any).workerBridge?.reconnectUpstream?.();
    await sleep(250);

    (dbA as any).sendLifecycleHint?.("freeze");
    await sleep(50);
    (dbA as any).sendLifecycleHint?.("resume");
    (dbA as any).workerBridge?.replayServerConnection?.();

    const postReconnectTitle = `post-reconnect-control-${Date.now()}`;
    await withTimeout(
      dbA.insert(todos, { title: postReconnectTitle, done: false }).wait({ tier: "local" }),
      10000,
      "Post-reconnect control insert(worker) did not resolve",
    );

    await waitForTodos(
      dbA,
      (rows) => rows.some((row) => row.title === postReconnectTitle),
      "A sees control row locally after reconnect",
      10000,
      "local",
    );
    await waitForRemoteTodoTitle(
      remoteDbId,
      postReconnectTitle,
      "B sees control row written after reconnect",
      20000,
    );

    const rowsOnB = await waitForRemoteTodoTitle(
      remoteDbId,
      offlineTitle,
      "B sees offline worker row after reconnect",
      20000,
    );
    expect(rowsOnB.some((row) => row.title === offlineTitle)).toBe(true);

    let dbProbeTrace: WorkerMessageProbe | null = null;
    try {
      const dbProbe = await createSyncedDb(
        ctx,
        "sync-offline-probe",
        sharedLocalAuthToken,
        syncServer,
      );
      dbProbeTrace = attachWorkerMessageProbe(dbProbe);
      const rowsOnProbe = await waitForTodos(
        dbProbe,
        (rows) => rows.some((row) => row.title === offlineTitle),
        "Fresh client sees offline worker row at edge after reconnect",
        20000,
        "edge",
      ).catch((error) =>
        rethrowWithWorkerDiagnostics(
          "Fresh probe edge read failed after reconnect",
          error,
          serverUrl,
          [
            { name: "writer-a", db: dbA, probe: dbAProbe },
            { name: "probe", db: dbProbe, probe: dbProbeTrace ?? undefined },
          ],
        ),
      );
      expect(rowsOnProbe.some((row) => row.title === offlineTitle)).toBe(true);
    } finally {
      dbProbeTrace?.dispose();
      dbAProbe.dispose();
    }
  }, 120000);

  it("local-only subscriptions receive rows from opfs", async () => {
    const dbName = uniqueDbName("sync-local-only");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    const snapshots: Todo[][] = [];
    const unsub = trackSubscription(
      dbA.subscribeAll(
        allTodos,
        (delta) => {
          snapshots.push([...delta.all]);
        },
        { propagation: "local-only" },
      ),
    );

    await dbA.insert(todos, { title: "local-only-local-1", done: true }).wait({ tier: "local" });

    // Wait for initial local-only snapshot.
    await waitForCondition(
      async () => snapshots.length > 0,
      5000,
      "local-only subscription should receive in-memory insert",
    );

    unsub();

    // Simulate a page refresh: close first instance, then reopen same namespace.
    await dbA.shutdown();
    untrack(dbA);

    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );

    await waitForCondition(
      async () => {
        const rows = await dbB.all(allTodos, { propagation: "local-only" });
        return rows.some((row) => row.title === "local-only-local-1");
      },
      8000,
      "local-only query should retrieve persisted OPFS rows after reopen",
    );

    const snapshotsB = await dbB.all(allTodos, { propagation: "local-only" });
    expect(snapshotsB.length).toBe(1);
    expect(snapshotsB[0].title).toBe("local-only-local-1");
  }, 60000);

  it("local-only subscriptions do not receive rows from sync server", async () => {
    const syncServer = await publishSyncServerSchemaAndPermissions("sync-local-only");
    const sharedLocalAuthToken = generateAuthSecret();
    const dbA = await createSyncedDb(ctx, "sync-local-only-a", sharedLocalAuthToken, syncServer);
    const dbB = await createSyncedDb(ctx, "sync-local-only-b", sharedLocalAuthToken, syncServer);

    const snapshots: Todo[][] = [];
    const unsub = trackSubscription(
      dbB.subscribeAll(
        allTodos,
        (delta) => {
          snapshots.push([...delta.all]);
        },
        { propagation: "local-only" },
      ),
    );

    // Wait for initial local-only snapshot.
    await waitForCondition(
      async () => snapshots.length > 0,
      5000,
      "local-only subscription should produce an initial snapshot",
    );

    const remoteTitle = `remote-for-local-only-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    await withTimeout(
      dbA.insert(todos, { title: remoteTitle, done: false }).wait({ tier: "local" }),
      10000,
      "A insert(worker) did not resolve",
    );

    // Give sync enough time; local-only must still not see remote data.
    await sleep(3000);
    const latestAfterRemote = snapshots[snapshots.length - 1] ?? [];
    expect(latestAfterRemote.some((row) => row.title === remoteTitle)).toBe(false);

    const localTitle = `local-only-local-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
    dbB.insert(todos, { title: localTitle, done: true });

    await waitForCondition(
      async () => {
        const latest = snapshots[snapshots.length - 1] ?? [];
        return latest.some((row) => row.title === localTitle);
      },
      8000,
      "local-only subscription should still include local inserts",
    );

    const latest = snapshots[snapshots.length - 1] ?? [];
    expect(latest.some((row) => row.title === localTitle)).toBe(true);
    expect(latest.some((row) => row.title === remoteTitle)).toBe(false);

    unsub();
  }, 60000);

  // -------------------------------------------------------------------------
  // 8. Leader election + cross-tab peer routing
  // -------------------------------------------------------------------------

  it("routes follower writes through the elected leader", async () => {
    const dbName = uniqueDbName("leader-route");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const { leader, follower } = await waitForLeaderAndFollower(dbA, dbB);

    const receivedByLeader: string[] = [];
    const unsubscribe = leader.subscribeAll(
      allTodos as QueryBuilder<Todo & { id: string }>,
      (delta) => {
        for (const todo of delta.all) {
          receivedByLeader.push(todo.title);
        }
      },
    );

    follower.insert(todos, { title: "Routed via leader", done: false });

    await waitForCondition(
      async () => receivedByLeader.includes("Routed via leader"),
      8000,
      "Leader should receive follower write through peer routing",
    );

    await waitForCondition(
      async () => {
        const leaderRows = await leader.all(allTodos, { tier: "local" });
        const followerRows = await follower.all(allTodos, { tier: "local" });
        const leaderHas = leaderRows.some((row) => row.title === "Routed via leader");
        const followerHas = followerRows.some((row) => row.title === "Routed via leader");
        return leaderHas && followerHas;
      },
      8000,
      "Both leader and follower should observe routed write",
    );

    unsubscribe();
  });

  it("fails over to follower after leader shutdown", async () => {
    const dbName = uniqueDbName("leader-failover");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const { leader, follower } = await waitForLeaderAndFollower(dbA, dbB);

    await leader.shutdown();
    untrack(leader);

    await waitForCondition(
      async () => getTabRole(follower) === "leader",
      12000,
      "Follower should be promoted to leader after shutdown",
    );

    const {
      value: { id },
    } = follower.insert(todos, { title: "Post-failover", done: true });
    await waitForCondition(
      async () => {
        const rows = await follower.all(allTodos, { tier: "local" });
        return rows.some((row) => row.id === id && row.title === "Post-failover");
      },
      8000,
      "New leader should continue processing writes after failover",
    );
  });

  it("re-elects cleanly when a closed leader tab is reopened", async () => {
    const dbName = uniqueDbName("leader-reopen");
    const dbA = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const dbB = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const { leader: initialLeader, follower: survivor } = await waitForLeaderAndFollower(dbA, dbB);

    await initialLeader.shutdown();
    untrack(initialLeader);

    await waitForCondition(
      async () => getTabRole(survivor) === "leader",
      12000,
      "Surviving tab should become leader after leader closes",
    );

    const reopened = track(
      await createDb({ appId: "test-app", driver: { type: "persistent", dbName } }),
    );
    const currentLeader = await waitForSingleLeader([survivor, reopened]);
    const currentFollower = currentLeader === survivor ? reopened : survivor;
    await currentLeader.all(allTodos, { tier: "local" });

    const marker = `reopen-${Date.now()}`;
    await withTimeout(
      currentFollower.insert(todos, { title: marker, done: false }).wait({ tier: "local" }),
      10000,
      "Follower insert during reopen re-election did not resolve",
    );

    await waitForCondition(
      async () => {
        const leaderRows = await currentLeader.all(allTodos, { tier: "local" });
        const followerRows = await currentFollower.all(allTodos, { tier: "local" });
        const leaderHas = leaderRows.some((row) => row.title === marker);
        const followerHas = followerRows.some((row) => row.title === marker);
        return leaderHas && followerHas;
      },
      8000,
      "Reopened tab and current leader should converge after re-election",
    );
  });
});

// ---------------------------------------------------------------------------
// Local helpers (thin wrappers over support.ts using local schema types)
// ---------------------------------------------------------------------------

async function waitForTodos(
  db: Db,
  predicate: (rows: Todo[]) => boolean,
  label: string,
  timeoutMs = 15000,
  tier?: "local" | "edge",
): Promise<Todo[]> {
  return waitForQuery(db, allTodos, predicate, label, timeoutMs, tier);
}

async function publishSyncServerSchemaAndPermissions(
  scope: string,
  permissions?: CompiledPermissions,
): Promise<TestingServerInfo> {
  const testingServer = await getTestingServerInfo(uniqueDbName(`worker-bridge-${scope}`));
  const { appId, serverUrl, adminSecret } = testingServer;
  const { hash: schemaHash } = await publishStoredSchema(serverUrl, {
    appId,
    adminSecret,
    schema: app.wasmSchema,
  });
  const { head } = await fetchPermissionsHead(serverUrl, { appId, adminSecret });
  const permissionsToPublish = permissions ?? {
    todos: {
      select: { using: { type: "True" } },
      insert: { with_check: { type: "True" } },
      update: {
        using: { type: "True" },
        with_check: { type: "True" },
      },
      delete: { using: { type: "True" } },
    },
    projects: {
      select: { using: { type: "True" } },
      insert: { with_check: { type: "True" } },
      update: {
        using: { type: "True" },
        with_check: { type: "True" },
      },
      delete: { using: { type: "True" } },
    },
  };
  await publishStoredPermissions(serverUrl, {
    appId,
    adminSecret,
    schemaHash,
    permissions: permissionsToPublish,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });

  return testingServer;
}

function hasRestoredCatalogueState(state: DebugSchemaState): boolean {
  return state.liveSchemaHashes.length > 1 && state.lensEdges.length > 0;
}

function getMainDebugSchemaState(db: Db, schemaForClient: WasmSchema): DebugSchemaState {
  const client = (db as any).getClient(schemaForClient);
  const runtime = client.getRuntime() as { __debugSchemaState?: () => DebugSchemaState };
  if (typeof runtime.__debugSchemaState !== "function") {
    throw new Error("Expected runtime.__debugSchemaState to be available");
  }
  return runtime.__debugSchemaState();
}

async function getWorkerDebugSchemaState(db: Db, timeoutMs = 5000): Promise<DebugSchemaState> {
  await (db as any).ensureBridgeReady();
  const worker = (db as any).worker as Worker | null;
  if (!worker) {
    throw new Error("Expected worker instance to exist");
  }

  return new Promise<DebugSchemaState>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`debug-schema-state: no response within ${timeoutMs}ms`));
    }, timeoutMs);

    const handler = (event: MessageEvent) => {
      const data = event.data as
        | { type?: string; state?: DebugSchemaState; message?: string }
        | undefined;
      if (!data?.type) return;

      if (data.type === "debug-schema-state-ok" && data.state) {
        cleanup();
        resolve(data.state);
        return;
      }

      if (
        data.type === "error" &&
        typeof data.message === "string" &&
        data.message.includes("debug-schema-state")
      ) {
        cleanup();
        reject(new Error(data.message));
      }
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.removeEventListener("message", handler);
    };

    worker.addEventListener("message", handler);
    worker.postMessage({ type: "debug-schema-state" });
  });
}

async function seedWorkerLiveSchema(db: Db, schema: WasmSchema, timeoutMs = 5000): Promise<void> {
  await (db as any).ensureBridgeReady();
  const worker = (db as any).worker as Worker | null;
  if (!worker) {
    throw new Error("Expected worker instance to exist");
  }

  const schemaJson = JSON.stringify(schema);

  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`debug-seed-live-schema: no response within ${timeoutMs}ms`));
    }, timeoutMs);

    const handler = (event: MessageEvent) => {
      const data = event.data as { type?: string; message?: string } | undefined;
      if (!data?.type) return;

      if (data.type === "debug-seed-live-schema-ok") {
        cleanup();
        resolve();
        return;
      }

      if (
        data.type === "error" &&
        typeof data.message === "string" &&
        data.message.includes("debug-seed-live-schema")
      ) {
        cleanup();
        reject(new Error(data.message));
      }
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.removeEventListener("message", handler);
    };

    worker.addEventListener("message", handler);
    worker.postMessage({ type: "debug-seed-live-schema", schemaJson });
  });
}
