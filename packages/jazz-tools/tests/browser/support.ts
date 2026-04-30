/**
 * Shared test utilities for browser integration tests.
 *
 * Mirrors the Rust `tests/support/mod.rs` pattern: generic polling
 * primitives, query helpers, cleanup tracking, and synced-Db factory.
 */

import { createDb, Db, type QueryBuilder } from "../../src/runtime/db.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { getTestingServerInfo } from "./testing-server.js";
import type { TestingServerInfo } from "./testing-server.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";

// ---------------------------------------------------------------------------
// Primitives
// ---------------------------------------------------------------------------

/** Await a fixed number of milliseconds. */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Generate a unique dbName to isolate OPFS state between tests. */
export function uniqueDbName(label: string): string {
  return `test-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

// ---------------------------------------------------------------------------
// Polling helpers  (cf. Rust wait_for / wait_for_query)
// ---------------------------------------------------------------------------

/**
 * Polls an async predicate until it returns true or the timeout expires.
 *
 * Equivalent of Rust `wait_for` — the lowest-level waiting primitive.
 */
export async function waitForCondition(
  check: () => Promise<boolean>,
  timeoutMs: number,
  message: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = undefined;
  while (Date.now() < deadline) {
    try {
      if (await check()) return;
    } catch (error) {
      lastError = error;
    }
    await sleep(50);
  }

  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(`Timeout after ${timeoutMs}ms: ${message}; lastError=${lastErrorMessage}`);
}

/**
 * Re-runs a query (`db.all`) until rows satisfy `predicate` or timeout.
 *
 * Equivalent of Rust `wait_for_query`. Returns the matching rows for
 * further assertions.
 */
export async function waitForQuery<T>(
  db: Db,
  queryBuilder: QueryBuilder<T>,
  predicate: (rows: T[]) => boolean,
  label: string,
  timeoutMs = 15000,
  tier?: "local" | "edge",
): Promise<T[]> {
  const deadline = Date.now() + timeoutMs;
  let lastRows: T[] = [];
  let lastError: unknown = undefined;

  while (Date.now() < deadline) {
    try {
      const remainingMs = Math.max(1, deadline - Date.now());
      const queryTimeoutMs = Math.min(5000, remainingMs);
      const rows = await withTimeout(
        db.all(queryBuilder, { tier }),
        queryTimeoutMs,
        `${label}: db.all did not resolve`,
      );
      if (predicate(rows)) {
        return rows;
      }
      lastRows = rows;
    } catch (error) {
      lastError = error;
    }
    await sleep(150);
  }

  const rowPreview = JSON.stringify(lastRows.slice(0, 10));
  const lastErrorMessage =
    lastError instanceof Error ? lastError.message : lastError ? String(lastError) : "none";
  throw new Error(
    `${label}: timed out after ${timeoutMs}ms (tier=${tier ?? "default"}); ` +
      `lastRowsCount=${lastRows.length}; lastRows=${rowPreview}; lastError=${lastErrorMessage}`,
  );
}

/**
 * Race a promise against a timeout.
 *
 * Useful for operations that should complete within a deadline (e.g.
 * `db.insert(...).wait({ tier: "edge" })`) but don't have built-in timeout support.
 */
export async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  label: string,
): Promise<T> {
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

/**
 * Wait for a worker to emit a message with a specific `type` field.
 */
export async function waitForWorkerMessageType(
  worker: Worker,
  expectedType: string,
  timeoutMs: number,
  label: string,
): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`${label}: no ${expectedType} worker message within ${timeoutMs}ms`));
    }, timeoutMs);

    const handler = (event: MessageEvent) => {
      const data = event.data as { type?: string } | undefined;
      if (data?.type === expectedType) {
        cleanup();
        resolve();
      }
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.removeEventListener("message", handler);
    };

    worker.addEventListener("message", handler);
  });
}

// ---------------------------------------------------------------------------
// Query builders
// ---------------------------------------------------------------------------

/** Build a QueryBuilder that selects all rows from a table (no conditions). */
export function makeQuery<T>(table: string, wasmSchema: WasmSchema): QueryBuilder<T> {
  return {
    _table: table,
    _schema: wasmSchema,
    _rowType: {} as T,
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

// ---------------------------------------------------------------------------
// Cleanup tracker  (cf. Rust TestingClient lifecycle)
// ---------------------------------------------------------------------------

/**
 * Tracks Dbs and subscriptions for deterministic teardown in afterEach.
 *
 * Usage:
 * ```ts
 * const ctx = new TestCleanup();
 * afterEach(() => ctx.cleanup());
 *
 * const db = ctx.track(await createDb(...));
 * const unsub = ctx.trackSubscription(db.subscribeAll(...));
 * ```
 */
export class TestCleanup {
  private dbs: Db[] = [];
  private subscriptions: Array<() => void> = [];

  /** Register a Db for shutdown during cleanup. Returns the same Db. */
  track(db: Db): Db {
    this.dbs.push(db);
    return db;
  }

  /** Remove a Db from the tracking list (e.g. after manual shutdown). */
  untrack(db: Db): void {
    const index = this.dbs.indexOf(db);
    if (index >= 0) {
      this.dbs.splice(index, 1);
    }
  }

  /** Register a subscription unsubscribe function. Returns a wrapped unsub. */
  trackSubscription(unsubscribe: () => void): () => void {
    this.subscriptions.push(unsubscribe);
    return () => {
      try {
        unsubscribe();
      } finally {
        const index = this.subscriptions.indexOf(unsubscribe);
        if (index >= 0) {
          this.subscriptions.splice(index, 1);
        }
      }
    };
  }

  /** Tear down all tracked subscriptions and dbs. Call from afterEach. */
  async cleanup(): Promise<void> {
    for (const unsubscribe of this.subscriptions.splice(0)) {
      try {
        unsubscribe();
      } catch {
        // Best effort
      }
    }
    for (const db of this.dbs.splice(0).reverse()) {
      try {
        await db.shutdown();
      } catch {
        // Best effort
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Synced Db factory  (cf. Rust TestingClient::builder())
// ---------------------------------------------------------------------------

/**
 * Create a Db connected to the testing server with OPFS persistence.
 *
 * Equivalent of Rust `TestingClient::builder().with_server(...).connect()`.
 * The returned Db is automatically tracked by `ctx` for cleanup.
 */
export async function createSyncedDb(
  ctx: TestCleanup,
  label: string,
  secret?: string,
  testingServer?: TestingServerInfo,
): Promise<Db> {
  const localFirstSecret = secret ?? generateAuthSecret();
  const { appId, serverUrl } = testingServer ?? (await getTestingServerInfo());
  return ctx.track(
    await createDb({
      appId,
      driver: { type: "persistent", dbName: uniqueDbName(label) },
      serverUrl,
      secret: localFirstSecret,
    }),
  );
}
