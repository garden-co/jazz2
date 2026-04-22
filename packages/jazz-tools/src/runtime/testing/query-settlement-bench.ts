import { randomUUID } from "node:crypto";
import { schema as s } from "../../index.js";
import type { Db } from "../db.js";

const DEFAULT_INSERT_COUNT = 1_200;
const DEFAULT_WARM_DELAY_MS = 5_000;

const settlementBenchApp = s.defineApp({
  bench_records: s.table({
    title: s.string(),
    bucket: s.string(),
    active: s.boolean(),
  }),
});

export interface QuerySettlementBenchOptions {
  label: string;
  insertCount?: number;
  warmDelayMs?: number;
}

export interface QuerySettlementScenarioResult {
  label: string;
  elapsedMs: number;
  rowCount: number;
}

export interface QuerySettlementBenchResult {
  label: string;
  insertCount: number;
  warmDelayMs: number;
  immediateFull: QuerySettlementScenarioResult;
  localOnly: QuerySettlementScenarioResult;
  warmedFull: QuerySettlementScenarioResult;
}

async function withBenchDb<T>(label: string, run: (db: Db) => Promise<T>): Promise<T> {
  const [{ createJazzContext }, { publishStoredSchema }, { startLocalJazzServer }] =
    await Promise.all([
      import("../../backend/create-jazz-context.js"),
      import("../schema-fetch.js"),
      import("../../testing/local-jazz-server.js"),
    ]);
  const appId = randomUUID();
  const backendSecret = `query-settlement-backend-${label}-${appId}`;
  const adminSecret = `query-settlement-admin-${label}-${appId}`;
  const server = await startLocalJazzServer({
    appId,
    backendSecret,
    adminSecret,
  });

  try {
    await publishStoredSchema(server.url, {
      appId,
      adminSecret,
      schema: settlementBenchApp.wasmSchema,
    });

    const context = createJazzContext({
      appId,
      driver: { type: "memory" },
      serverUrl: server.url,
      backendSecret,
      adminSecret,
      env: "test",
      userBranch: "main",
      tier: "edge",
    });

    try {
      return await run(context.asBackend(settlementBenchApp));
    } finally {
      await context.shutdown();
    }
  } finally {
    await server.stop();
  }
}

async function seedBurst(db: Db, insertCount: number): Promise<void> {
  const pendingWrites = [];

  for (let index = 0; index < insertCount; index += 1) {
    pendingWrites.push(
      db.insert(settlementBenchApp.bench_records, {
        title: `Bench Record ${index}`,
        bucket: `bucket-${index % 24}`,
        active: index % 5 !== 0,
      }),
    );
  }

  await Promise.all(pendingWrites.map((write) => write.wait({ tier: "local" })));
}

async function measureScenario(
  label: string,
  insertCount: number,
  options?: {
    propagation?: "full" | "local-only";
    warmDelayMs?: number;
  },
): Promise<QuerySettlementScenarioResult> {
  return await withBenchDb(label, async (db) => {
    await seedBurst(db, insertCount);

    const warmDelayMs = options?.warmDelayMs ?? 0;
    if (warmDelayMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, warmDelayMs));
    }

    const query = settlementBenchApp.bench_records.where({ active: true });
    const startedAt = performance.now();
    const rows = await db.all(
      query,
      options?.propagation ? { propagation: options.propagation } : undefined,
    );

    return {
      label,
      elapsedMs: performance.now() - startedAt,
      rowCount: rows.length,
    };
  });
}

export async function runQuerySettlementBench(
  options: QuerySettlementBenchOptions,
): Promise<QuerySettlementBenchResult> {
  const insertCount = options.insertCount ?? DEFAULT_INSERT_COUNT;
  const warmDelayMs = options.warmDelayMs ?? DEFAULT_WARM_DELAY_MS;

  const immediateFull = await measureScenario(`${options.label}-immediate-full`, insertCount);
  const localOnly = await measureScenario(`${options.label}-local-only`, insertCount, {
    propagation: "local-only",
  });
  const warmedFull = await measureScenario(`${options.label}-warmed-full`, insertCount, {
    warmDelayMs,
  });

  return {
    label: options.label,
    insertCount,
    warmDelayMs,
    immediateFull,
    localOnly,
    warmedFull,
  };
}
