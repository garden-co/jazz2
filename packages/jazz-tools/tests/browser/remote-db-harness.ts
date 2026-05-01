import { createDb, type Db, type QueryBuilder } from "../../src/runtime/db.js";
import type { DbConfig } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";

export interface RemoteBrowserDbCreateInput {
  id: string;
  appId: string;
  dbName: string;
  table: string;
  schemaJson: string;
  serverUrl?: string;
  adminSecret?: string;
  localFirstSecret?: string;
  logLevel?: DbConfig["logLevel"];
}

export interface RemoteBrowserDbWaitForTitleInput {
  id: string;
  title: string;
  timeoutMs: number;
  tier?: "local" | "edge";
}

interface RemoteBrowserDbState {
  db: Db;
  query: QueryBuilder<Record<string, unknown>>;
}

declare global {
  interface Window {
    __jazzRemoteBrowserDbs__?: Map<string, RemoteBrowserDbState>;
  }
}

function getRemoteStateStore(): Map<string, RemoteBrowserDbState> {
  if (!window.__jazzRemoteBrowserDbs__) {
    window.__jazzRemoteBrowserDbs__ = new Map();
  }
  return window.__jazzRemoteBrowserDbs__;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timeoutId = setTimeout(() => {
          reject(new Error(`${label} after ${timeoutMs}ms`));
        }, timeoutMs);
      }),
    ]);
  } finally {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
  }
}

function makeAllRowsQuery(
  table: string,
  schema: WasmSchema,
): QueryBuilder<Record<string, unknown>> {
  return {
    _table: table,
    _schema: schema,
    _rowType: {} as Record<string, unknown>,
    _build() {
      return JSON.stringify({
        table,
        conditions: [],
        includes: {},
        orderBy: [],
      });
    },
  };
}

export async function createRemoteBrowserDb(input: RemoteBrowserDbCreateInput): Promise<void> {
  const store = getRemoteStateStore();
  const existing = store.get(input.id);
  if (existing) {
    await existing.db.shutdown();
    store.delete(input.id);
  }

  const schema = JSON.parse(input.schemaJson) as WasmSchema;
  const db = await createDb({
    appId: input.appId,
    driver: { type: "persistent", dbName: input.dbName },
    serverUrl: input.serverUrl,
    ...(input.localFirstSecret ? { secret: input.localFirstSecret } : {}),
    adminSecret: input.adminSecret,
    logLevel: input.logLevel,
  });

  store.set(input.id, {
    db,
    query: makeAllRowsQuery(input.table, schema),
  });
}

export async function waitForRemoteBrowserDbTitle(
  input: RemoteBrowserDbWaitForTitleInput,
): Promise<Record<string, unknown>[]> {
  const store = getRemoteStateStore();
  const state = store.get(input.id);
  if (!state) {
    throw new Error(`Remote browser db "${input.id}" was not initialized`);
  }

  const deadline = Date.now() + input.timeoutMs;
  let lastRows: Record<string, unknown>[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const remainingMs = Math.max(1, deadline - Date.now());
      const queryTimeoutMs = Math.min(5000, remainingMs);
      const rows = await withTimeout(
        state.db.all(state.query, { tier: input.tier }),
        queryTimeoutMs,
        `Remote browser db "${input.id}" query did not resolve`,
      );
      if (rows.some((row) => row.title === input.title)) {
        return rows;
      }
      lastRows = rows;
    } catch (error) {
      lastError = error;
    }
    await sleep(100);
  }

  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(
    `Remote browser db "${input.id}" did not observe title "${input.title}" within ${input.timeoutMs}ms; ` +
      `lastRows=${JSON.stringify(lastRows.slice(0, 10))}; lastError=${lastErrorMessage}`,
  );
}

export async function closeRemoteBrowserDb(id: string): Promise<void> {
  const store = getRemoteStateStore();
  const state = store.get(id);
  if (!state) {
    return;
  }

  await state.db.shutdown();
  store.delete(id);
}
