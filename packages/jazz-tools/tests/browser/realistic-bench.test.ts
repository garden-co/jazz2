import { commands } from "vitest/browser";
import { describe, expect, it } from "vitest";
import { createDb, type Db, type QueryBuilder, type TableProxy } from "../../src/runtime/db.js";
import { generateAuthSecret } from "../../src/runtime/auth-secret-store.js";
import type { WasmSchema } from "../../src/drivers/types.js";
import { loadWasmModule } from "../../src/runtime/client.js";
import { getTestingServerInfo, getTestingServerJwtForUser } from "./testing-server.js";

import schemaJson from "../../../../benchmarks/realistic/schema/project_board.schema.json";
import profileJson from "../../../../benchmarks/realistic/profiles/s.json";
import w1Json from "../../../../benchmarks/realistic/scenarios/w1_interactive.json";
import w3Json from "../../../../benchmarks/realistic/scenarios/w3_offline_reconnect.json";
import w4Json from "../../../../benchmarks/realistic/scenarios/w4_cold_start.json";
import b1Json from "../../../../benchmarks/realistic/scenarios/b1_server_crud_sustained.json";
import b2Json from "../../../../benchmarks/realistic/scenarios/b2_server_reads_sustained.json";
import b3Json from "../../../../benchmarks/realistic/scenarios/b3_server_cold_load_large.json";
import b4Json from "../../../../benchmarks/realistic/scenarios/b4_server_fanout_updates.json";
import b5Json from "../../../../benchmarks/realistic/scenarios/b5_server_permission_recursive.json";
import b6Json from "../../../../benchmarks/realistic/scenarios/b6_server_hotspot_history.json";

declare const __JAZZ_REALISTIC_BROWSER_SCENARIOS__: string;
declare const __JAZZ_REALISTIC_BROWSER_RUN_ID__: string;
declare const __JAZZ_REALISTIC_BROWSER_LIMIT_OVERRIDES_JSON__: string;

type PersistenceTier = "local" | "edge" | "core";

function durabilityOptions(tier: PersistenceTier): { tier: "local" | "edge" | "global" } {
  return { tier: tier === "core" ? "global" : tier };
}

interface ProfileConfig {
  id: string;
  seed: number;
  users: number;
  organizations: number;
  projects: number;
  tasks: number;
  comments: number;
  watchers_per_task: number;
  activity_events: number;
  hot_project_fraction: number;
}

interface W1Scenario {
  id: string;
  name: string;
  seed: number;
  operation_count: number;
  mix: Array<{ operation: string; weight: number }>;
}

interface W3Scenario {
  id: string;
  name: string;
  seed: number;
  offline_write_count: number;
  timeout_seconds: number;
}

interface W4Scenario {
  id: string;
  name: string;
  seed: number;
  reopen_cycles: number;
}

interface B1Scenario {
  id: string;
  name: string;
  seed: number;
  insert_count: number;
  update_count: number;
  delete_count: number;
}

interface B2Scenario {
  id: string;
  name: string;
  seed: number;
  request_count: number;
  mix: Array<{ operation: string; weight: number }>;
}

interface B3Scenario {
  id: string;
  name: string;
  seed: number;
  reopen_cycles: number;
  large_multiplier: number;
}

interface B4Scenario {
  id: string;
  name: string;
  seed: number;
  subscriber_counts: number[];
  rounds: number;
  timeout_seconds: number;
}

interface B5Scenario {
  id: string;
  name: string;
  seed: number;
  folders: number;
  documents: number;
  read_request_count: number;
  update_attempt_count: number;
  allow_fraction: number;
  recursive_depth: number;
}

interface B6Scenario {
  id: string;
  name: string;
  seed: number;
  hot_task_count: number;
  update_count: number;
}

interface UserRow {
  id: string;
  display_name: string;
  email: string;
}

interface OrganizationRow {
  id: string;
  name: string;
  created_at: number;
}

interface MembershipRow {
  id: string;
  organization_id: string;
  user_id: string;
  role: string;
}

interface ProjectRow {
  id: string;
  organization_id: string;
  name: string;
  archived: boolean;
  updated_at: number;
}

interface TaskRow {
  id: string;
  project_id: string;
  title: string;
  status: string;
  priority: number;
  assignee_id: string;
  updated_at: number;
  due_at: number | null;
}

interface CommentRow {
  id: string;
  task_id: string;
  author_id: string;
  body: string;
  created_at: number;
}

interface TaskWatcherRow {
  id: string;
  task_id: string;
  user_id: string;
}

interface ActivityRow {
  id: string;
  project_id: string;
  task_id: string | null;
  actor_id: string;
  kind: string;
  created_at: number;
  payload: string;
}

interface PermissionFolderRow {
  id: string;
  parent_id: string | null;
  owner_id: string;
  title: string;
  updated_at: number;
}

interface PermissionDocumentRow {
  id: string;
  folder_id: string;
  editor_id: string;
  body: string;
  revision: number;
  updated_at: number;
}

interface SeedState {
  users: string[];
  projects: string[];
  taskIds: string[];
  taskProjectIdx: number[];
  commentsPerTask: number[];
  hotProjectCount: number;
}

interface OpSummary {
  count: number;
  avg_ms: number;
  p50_ms: number;
  p95_ms: number;
  p99_ms: number;
}

interface ScenarioResult {
  scenario_id: string;
  scenario_name: string;
  profile_id: string;
  topology: string;
  total_operations: number;
  wall_time_ms: number;
  throughput_ops_per_sec: number;
  operation_summaries: Record<string, OpSummary>;
  extra: Record<string, unknown>;
}

const TARGET_TIMING_WINDOW_MS = 40;
const MAX_BATCHED_TIMING_REPEATS = 64;
const DEFAULT_CI_BROWSER_LIMITS = {
  w1OperationCount: 1200,
  w4Cycles: 25,
  b1InsertCount: 4096,
  b1UpdateCount: 4096,
  b1DeleteCount: 4096,
  b2RequestCount: 768,
  b3Cycles: 12,
  b3LargeMultiplier: 8,
  b4MaxSubscribers: 128,
  b4Rounds: 12,
  b5ReadRequests: 160,
  b5UpdateAttempts: 120,
  b6UpdateCount: 6000,
} as const;
type BrowserLimitOverrides = Partial<Record<keyof typeof DEFAULT_CI_BROWSER_LIMITS, number>>;

function resolveBrowserLimitOverrides(): BrowserLimitOverrides {
  const raw = (__JAZZ_REALISTIC_BROWSER_LIMIT_OVERRIDES_JSON__ ?? "").trim();
  if (!raw) {
    return {};
  }

  try {
    const parsed = JSON.parse(raw) as Record<string, unknown>;
    const overrides: BrowserLimitOverrides = {};
    for (const [key, value] of Object.entries(parsed)) {
      if (!(key in DEFAULT_CI_BROWSER_LIMITS)) continue;
      if (typeof value !== "number" || !Number.isFinite(value)) continue;
      overrides[key as keyof typeof DEFAULT_CI_BROWSER_LIMITS] = Math.max(1, Math.floor(value));
    }
    return overrides;
  } catch {
    return {};
  }
}

const CI_BROWSER_LIMITS = {
  ...DEFAULT_CI_BROWSER_LIMITS,
  ...resolveBrowserLimitOverrides(),
} as const;

const schema = (schemaJson as { tables: WasmSchema }).tables;
const profile = profileJson as unknown as ProfileConfig;
const w1 = w1Json as unknown as W1Scenario;
const w3 = w3Json as unknown as W3Scenario;
const w4 = w4Json as unknown as W4Scenario;
const b1 = b1Json as unknown as B1Scenario;
const b2 = b2Json as unknown as B2Scenario;
const b3 = b3Json as unknown as B3Scenario;
const b4 = b4Json as unknown as B4Scenario;
const b5 = b5Json as unknown as B5Scenario;
const b6 = b6Json as unknown as B6Scenario;
const browserScenarioSelection = new Set(
  (__JAZZ_REALISTIC_BROWSER_SCENARIOS__ ?? "")
    .split(",")
    .map((value) => value.trim().toUpperCase())
    .filter(Boolean),
);
const browserRunId = (__JAZZ_REALISTIC_BROWSER_RUN_ID__ ?? "").trim() || "local";

const usersTable = tableProxy<UserRow, Omit<UserRow, "id">>("users");
const organizationsTable = tableProxy<OrganizationRow, Omit<OrganizationRow, "id">>(
  "organizations",
);
const membershipsTable = tableProxy<MembershipRow, Omit<MembershipRow, "id">>("memberships");
const projectsTable = tableProxy<ProjectRow, Omit<ProjectRow, "id">>("projects");
const tasksTable = tableProxy<TaskRow, Omit<TaskRow, "id">>("tasks");
const commentsTable = tableProxy<CommentRow, Omit<CommentRow, "id">>("task_comments");
const taskWatchersTable = tableProxy<TaskWatcherRow, Omit<TaskWatcherRow, "id">>("task_watchers");
const activityTable = tableProxy<ActivityRow, Omit<ActivityRow, "id">>("activity_events");

class Lcg {
  private state: bigint;

  constructor(seed: number) {
    this.state = BigInt(seed >>> 0) | 1n;
  }

  nextU64(): bigint {
    this.state = (this.state * 6364136223846793005n + 1442695040888963407n) & ((1n << 64n) - 1n);
    return this.state;
  }

  nextInt(upper: number): number {
    if (upper <= 1) return 0;
    return Number(this.nextU64() % BigInt(upper));
  }

  pickWeightedIndex(weights: number[]): number {
    const total = weights.reduce((sum, w) => sum + w, 0);
    if (total <= 0) return 0;
    let cursor = this.nextInt(total);
    for (let i = 0; i < weights.length; i += 1) {
      if (cursor < weights[i]) return i;
      cursor -= weights[i];
    }
    return Math.max(0, weights.length - 1);
  }
}

function tableProxy<T, Init>(table: string, tableSchema: WasmSchema = schema): TableProxy<T, Init> {
  return {
    _table: table,
    _schema: tableSchema,
    _rowType: {} as T,
    _initType: {} as Init,
  };
}

function query<T>(
  table: string,
  conditions: Array<{ column: string; op: string; value: unknown }> = [],
  orderBy: Array<[string, "asc" | "desc"]> = [],
  limit?: number,
  querySchema: WasmSchema = schema,
): QueryBuilder<T> {
  return {
    _table: table,
    _schema: querySchema,
    _rowType: {} as T,
    _build() {
      return JSON.stringify({
        table,
        conditions,
        includes: {},
        orderBy,
        limit,
        offset: 0,
      });
    },
  };
}

function nowMicros(): number {
  return Date.now() * 1000;
}

function uniqueDbName(label: string): string {
  return `realistic-${browserRunId}-${label}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

function hash32(input: string): number {
  let hash = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    hash ^= input.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

async function benchmarkAppId(label: string): Promise<string> {
  const { appId } = await getTestingServerInfo();
  const segments = ["a", "b", "c", "d"].map((suffix) =>
    hash32(`${appId}|${browserRunId}|${label}|${suffix}`).toString(16).padStart(8, "0"),
  );
  const s0 = segments[0];
  const s1 = segments[1];
  const s2 = segments[2];
  const s3 = segments[3];
  const variant = ((parseInt(s2[0], 16) & 0x3) | 0x8).toString(16);
  return `${s0}-${s1.slice(0, 4)}-4${s1.slice(1, 4)}-${variant}${s2.slice(1, 4)}-${s2.slice(4)}${s3}`;
}

function summarizeLatencies(values: number[], count = values.length): OpSummary {
  if (values.length === 0) {
    return { count: 0, avg_ms: 0, p50_ms: 0, p95_ms: 0, p99_ms: 0 };
  }
  const sorted = [...values].sort((a, b) => a - b);
  const at = (p: number): number => {
    const idx = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * p) - 1));
    return sorted[idx];
  };
  const avg = sorted.reduce((sum, x) => sum + x, 0) / sorted.length;
  return {
    count,
    avg_ms: avg,
    p50_ms: at(0.5),
    p95_ms: at(0.95),
    p99_ms: at(0.99),
  };
}

function createProgressReporter(label: string, total: number): (done: number) => void {
  const step = Math.max(1, Math.floor(total / 4));
  let next = step;
  return (done: number): void => {
    while (done >= next && next < total) {
      progressLog(`${label}: ${Math.min(done, total)}/${total}`);
      next += step;
    }
  };
}

async function measureBatchedLatency(
  remaining: number,
  runOnce: (batchIndex: number) => Promise<void>,
  options: {
    minWindowMs?: number;
    maxRepeats?: number;
  } = {},
): Promise<{ repeats: number; elapsedMs: number; perOpMs: number }> {
  const minWindowMs = options.minWindowMs ?? TARGET_TIMING_WINDOW_MS;
  const maxRepeats = Math.max(
    1,
    Math.min(remaining, options.maxRepeats ?? MAX_BATCHED_TIMING_REPEATS),
  );

  const startedAt = performance.now();
  let repeats = 0;
  while (repeats < maxRepeats) {
    await runOnce(repeats);
    repeats += 1;
    if (repeats >= 1 && performance.now() - startedAt >= minWindowMs) {
      break;
    }
  }

  const elapsedMs = performance.now() - startedAt;
  return {
    repeats,
    elapsedMs,
    perOpMs: elapsedMs / Math.max(1, repeats),
  };
}

function progressLog(message: string): void {
  // eslint-disable-next-line no-console
  console.log(`[realistic-progress] ${message}`);
}

function reportLoopProgress(label: string, index: number, total: number): void {
  if (total <= 0) return;
  const step = Math.max(1, Math.floor(total / 4));
  if (index > 0 && index % step === 0) {
    progressLog(`${label}: ${index}/${total}`);
  }
}

function scaledProfile(input: ProfileConfig): ProfileConfig {
  // Browser benchmark pass keeps relational shape but scales volume down
  // so W1/W3/W4 complete reliably in CI-like environments.
  const tasks = Math.max(100, Math.floor(input.tasks * 0.03));
  const comments = Math.max(tasks, Math.floor(input.comments * 0.03));
  const activity_events = Math.max(tasks, Math.floor(input.activity_events * 0.03));
  return {
    ...input,
    tasks,
    comments,
    activity_events,
  };
}

function scaledLargeProfile(input: ProfileConfig, multiplier: number): ProfileConfig {
  const base = scaledProfile(input);
  const factor = Math.max(1, Math.floor(multiplier));
  const tasks = Math.min(4000, base.tasks * factor);
  const comments = Math.min(16000, Math.max(tasks, base.comments * factor));
  const activity_events = Math.min(12000, Math.max(tasks, base.activity_events * factor));
  return {
    ...base,
    id: `${base.id}_L`,
    tasks,
    comments,
    activity_events,
  };
}

async function createServerDb(
  appId: string,
  dbName: string,
  sub: string,
  claims: Record<string, unknown> = {},
  options: {
    includeAdminSecret?: boolean;
    includeJwt?: boolean;
    localFirstSecret?: string;
  } = {},
): Promise<Db> {
  const { serverUrl, adminSecret } = await getTestingServerInfo();
  const config: Parameters<typeof createDb>[0] = {
    appId,
    dbName,
    serverUrl,
    adminSecret: options.includeAdminSecret === false ? undefined : adminSecret,
    logLevel: "warn",
  };
  if (options.includeJwt !== false) {
    config.jwtToken = await getTestingServerJwtForUser(sub, claims);
  }
  if (options.localFirstSecret) {
    config.secret = options.localFirstSecret;
  }
  return createDb(config);
}

async function waitForCondition(
  description: string,
  timeoutMs: number,
  condition: () => boolean,
): Promise<void> {
  const deadline = performance.now() + timeoutMs;
  while (performance.now() < deadline) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`Timed out waiting for ${description}`);
}

async function storageUsageBytes(): Promise<number | null> {
  if (typeof navigator === "undefined" || !navigator.storage?.estimate) return null;
  const estimate = await navigator.storage.estimate();
  return typeof estimate.usage === "number" ? estimate.usage : null;
}

async function seedDataset(db: Db, config: ProfileConfig): Promise<SeedState> {
  progressLog(
    `seed start profile=${config.id} users=${config.users} orgs=${config.organizations} projects=${config.projects} tasks=${config.tasks} comments=${config.comments}`,
  );
  const users: string[] = [];
  const organizations: string[] = [];
  const projects: string[] = [];
  const taskIds: string[] = [];
  const taskProjectIdx: number[] = [];
  const commentsPerTask = new Array<number>(config.tasks).fill(0);

  const ts = nowMicros();
  for (let i = 0; i < config.users; i += 1) {
    reportLoopProgress("seed users", i, config.users);
    const { id } = await db.insert(usersTable, {
      display_name: `User ${i}`,
      email: `user${i}@bench.test`,
    });
    users.push(id);
  }

  for (let i = 0; i < config.organizations; i += 1) {
    reportLoopProgress("seed organizations", i, config.organizations);
    const { id } = await db.insert(organizationsTable, {
      name: `Org ${i}`,
      created_at: ts + i,
    });
    organizations.push(id);
  }

  for (let i = 0; i < config.users; i += 1) {
    reportLoopProgress("seed memberships", i, config.users);
    await db.insert(membershipsTable, {
      organization_id: organizations[i % organizations.length],
      user_id: users[i],
      role: i % 9 === 0 ? "admin" : "member",
    });
  }

  for (let i = 0; i < config.projects; i += 1) {
    reportLoopProgress("seed projects", i, config.projects);
    const { id } = await db.insert(projectsTable, {
      organization_id: organizations[i % organizations.length],
      name: `Project ${i}`,
      archived: false,
      updated_at: ts + i,
    });
    projects.push(id);
  }

  const statuses = ["todo", "in_progress", "review", "done"] as const;
  for (let i = 0; i < config.tasks; i += 1) {
    reportLoopProgress("seed tasks", i, config.tasks);
    const projectIdx = i % projects.length;
    const assigneeIdx = i % users.length;
    const { id } = await db.insert(tasksTable, {
      project_id: projects[projectIdx],
      title: `Task ${i}`,
      status: statuses[i % statuses.length],
      priority: 1 + (i % 4),
      assignee_id: users[assigneeIdx],
      updated_at: ts + i,
      due_at: ts + i * 11,
    });
    taskIds.push(id);
    taskProjectIdx.push(projectIdx);
  }

  for (let i = 0; i < config.comments; i += 1) {
    reportLoopProgress("seed comments", i, config.comments);
    const taskIdx = i % taskIds.length;
    await db.insert(commentsTable, {
      task_id: taskIds[taskIdx],
      author_id: users[(i * 7) % users.length],
      body: `Comment ${i} body`,
      created_at: ts + i,
    });
    commentsPerTask[taskIdx] += 1;
  }

  for (let taskIdx = 0; taskIdx < taskIds.length; taskIdx += 1) {
    reportLoopProgress("seed task_watchers", taskIdx, taskIds.length);
    for (let w = 0; w < config.watchers_per_task; w += 1) {
      await db.insert(taskWatchersTable, {
        task_id: taskIds[taskIdx],
        user_id: users[(taskIdx + w) % users.length],
      });
    }
  }

  for (let i = 0; i < config.activity_events; i += 1) {
    reportLoopProgress("seed activity_events", i, config.activity_events);
    const taskIdx = i % taskIds.length;
    await db.insert(activityTable, {
      project_id: projects[taskProjectIdx[taskIdx]],
      task_id: taskIds[taskIdx],
      actor_id: users[(i * 11) % users.length],
      kind: i % 3 === 0 ? "task_updated" : "comment_added",
      created_at: ts + i,
      payload: `{"event":${i}}`,
    });
  }

  const hotProjectCount = Math.max(1, Math.round(config.projects * config.hot_project_fraction));
  progressLog("seed complete");
  return {
    users,
    projects,
    taskIds,
    taskProjectIdx,
    commentsPerTask,
    hotProjectCount,
  };
}

async function runW1(db: Db, config: ProfileConfig, state: SeedState): Promise<ScenarioResult> {
  const operationCount = Math.min(w1.operation_count, CI_BROWSER_LIMITS.w1OperationCount);
  progressLog(`W1 start operations=${operationCount}`);
  const rng = new Lcg(w1.seed ^ config.seed);
  const weights = w1.mix.map((x) => x.weight);
  const latencies: Record<string, number[]> = {};
  const opCounts: Record<string, number> = {};
  const reportProgress = createProgressReporter("W1 progress", operationCount);

  const wallStart = performance.now();
  for (let i = 0; i < operationCount; ) {
    const op = w1.mix[rng.pickWeightedIndex(weights)].operation;
    const batch = await measureBatchedLatency(operationCount - i, async (batchIndex) => {
      const currentIndex = i + batchIndex;
      switch (op) {
        case "query_board": {
          const project = state.projects[rng.nextInt(state.hotProjectCount)];
          await db.all(
            query<TaskRow>(
              "tasks",
              [{ column: "project_id", op: "eq", value: project }],
              [["updated_at", "desc"]],
              200,
            ),
            "local",
          );
          break;
        }
        case "query_my_work": {
          const assignee = state.users[rng.nextInt(state.users.length)];
          await db.all(
            query<TaskRow>(
              "tasks",
              [
                { column: "assignee_id", op: "eq", value: assignee },
                { column: "status", op: "eq", value: "in_progress" },
              ],
              [["updated_at", "desc"]],
              200,
            ),
            "local",
          );
          break;
        }
        case "query_task_detail": {
          const taskId = state.taskIds[rng.nextInt(state.taskIds.length)];
          await db.all(
            query<CommentRow>(
              "task_comments",
              [{ column: "task_id", op: "eq", value: taskId }],
              [["created_at", "desc"]],
              200,
            ),
            "local",
          );
          await db.all(
            query<ActivityRow>(
              "activity_events",
              [{ column: "task_id", op: "eq", value: taskId }],
              [["created_at", "desc"]],
              200,
            ),
            "local",
          );
          break;
        }
        case "update_task_status": {
          const taskIdx = rng.nextInt(state.taskIds.length);
          await db.update(tasksTable, state.taskIds[taskIdx], {
            status: ["todo", "in_progress", "review", "done"][rng.nextInt(4)],
            priority: 1 + rng.nextInt(4),
            assignee_id: state.users[rng.nextInt(state.users.length)],
            updated_at: nowMicros(),
          });
          break;
        }
        case "insert_comment": {
          const taskIdx = rng.nextInt(state.taskIds.length);
          await db.insert(commentsTable, {
            task_id: state.taskIds[taskIdx],
            author_id: state.users[rng.nextInt(state.users.length)],
            body: `interactive comment ${currentIndex}`,
            created_at: nowMicros(),
          });
          state.commentsPerTask[taskIdx] += 1;
          break;
        }
        case "update_project_meta": {
          const projectIdx = rng.nextInt(state.projects.length);
          await db.update(projectsTable, state.projects[projectIdx], {
            name: `Project ${projectIdx} v${currentIndex}`,
            updated_at: nowMicros(),
          });
          break;
        }
        default:
          throw new Error(`Unknown operation in W1 mix: ${op}`);
      }
    });
    (latencies[op] ||= []).push(batch.perOpMs);
    opCounts[op] = (opCounts[op] || 0) + batch.repeats;
    i += batch.repeats;
    reportProgress(i);
  }
  const wallMs = performance.now() - wallStart;

  const operation_summaries: Record<string, OpSummary> = {};
  for (const [op, samples] of Object.entries(latencies)) {
    operation_summaries[op] = summarizeLatencies(samples, opCounts[op] ?? samples.length);
  }

  return {
    scenario_id: w1.id,
    scenario_name: w1.name,
    profile_id: config.id,
    topology: "local_only",
    total_operations: operationCount,
    wall_time_ms: wallMs,
    throughput_ops_per_sec: operationCount / Math.max(0.001, wallMs / 1000),
    operation_summaries,
    extra: {
      hot_projects: state.hotProjectCount,
      dataset: {
        users: config.users,
        projects: config.projects,
        tasks: config.tasks,
        comments: config.comments,
      },
    },
  };
}

async function runW3(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("W3 start");
  const dbName = uniqueDbName("w3");
  const appId = await benchmarkAppId("w3");
  const { serverUrl, adminSecret } = await getTestingServerInfo();
  const token = await getTestingServerJwtForUser("realistic-user");
  const rng = new Lcg(w3.seed ^ config.seed);
  const offlineWrites = Math.min(w3.offline_write_count, 20);
  let offlineDb: Db | null = null;
  let onlineDb: Db | null = null;

  try {
    offlineDb = await createDb({ appId, dbName, logLevel: "warn" });
    const state = await seedDataset(offlineDb, config);
    const targetTaskIdx = rng.nextInt(state.taskIds.length);
    const targetTaskId = state.taskIds[targetTaskIdx];
    const baseline = state.commentsPerTask[targetTaskIdx];

    const offlineWriteStart = performance.now();
    for (let i = 0; i < offlineWrites; i += 1) {
      await offlineDb
        .insert(commentsTable, {
          task_id: targetTaskId,
          author_id: state.users[rng.nextInt(state.users.length)],
          body: `offline_reconnect_marker_${i}`,
          created_at: nowMicros(),
        })
        .wait({ tier: "local" });
    }
    const offlineWriteMs = performance.now() - offlineWriteStart;
    await offlineDb.shutdown();
    offlineDb = null;

    const reconnectStart = performance.now();
    onlineDb = await createDb({
      appId,
      dbName,
      serverUrl,
      jwtToken: token,
      adminSecret,
      logLevel: "warn",
    });

    const target = baseline + offlineWrites;
    const timeoutAt = performance.now() + Math.min(w3.timeout_seconds, 20) * 1000;
    let observed = 0;
    let polls = 0;
    while (performance.now() < timeoutAt) {
      const rows = await onlineDb.all(
        query<CommentRow>("task_comments", [{ column: "task_id", op: "eq", value: targetTaskId }]),
        "edge",
      );
      observed = rows.length;
      polls += 1;
      if (polls % 5 === 0) {
        progressLog(`W3 polling observed=${observed} target=${target} polls=${polls}`);
      }
      if (observed >= target) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    if (observed < target) {
      throw new Error(
        `W3 timed out waiting for edge settlement (observed=${observed}, target=${target})`,
      );
    }
    const reconnectMs = performance.now() - reconnectStart;

    return {
      scenario_id: w3.id,
      scenario_name: w3.name,
      profile_id: config.id,
      topology: "single_hop",
      total_operations: offlineWrites + 1,
      wall_time_ms: offlineWriteMs + reconnectMs,
      throughput_ops_per_sec:
        (offlineWrites + 1) / Math.max(0.001, (offlineWriteMs + reconnectMs) / 1000),
      operation_summaries: {
        offline_writes: {
          count: offlineWrites,
          avg_ms: offlineWriteMs / Math.max(1, offlineWrites),
          p50_ms: offlineWriteMs / Math.max(1, offlineWrites),
          p95_ms: offlineWriteMs / Math.max(1, offlineWrites),
          p99_ms: offlineWriteMs / Math.max(1, offlineWrites),
        },
        reconnect_settlement: {
          count: 1,
          avg_ms: reconnectMs,
          p50_ms: reconnectMs,
          p95_ms: reconnectMs,
          p99_ms: reconnectMs,
        },
      },
      extra: {
        target_task_id: targetTaskId,
        baseline_comments: baseline,
        target_comments_after_reconnect: target,
        observed_comments_after_reconnect: observed,
        poll_iterations: polls,
      },
    };
  } finally {
    if (offlineDb) await offlineDb.shutdown();
    if (onlineDb) await onlineDb.shutdown();
  }
}

async function runW4(config: ProfileConfig): Promise<ScenarioResult> {
  const dbName = uniqueDbName("w4");
  const appId = await benchmarkAppId("w4");
  const cycles = Math.min(w4.reopen_cycles, CI_BROWSER_LIMITS.w4Cycles);
  progressLog(`W4 start cycles=${cycles}`);
  let db: Db | null = null;
  const latencies: number[] = [];

  try {
    db = await createDb({ appId, dbName, logLevel: "warn" });
    const state = await seedDataset(db, config);
    const hotProjectId = state.projects[0];
    await db.all(
      query<TaskRow>(
        "tasks",
        [{ column: "project_id", op: "eq", value: hotProjectId }],
        [["updated_at", "desc"]],
        200,
      ),
      "local",
    );
    await db.shutdown();
    db = null;

    const wallStart = performance.now();
    for (let i = 0; i < cycles; i += 1) {
      progressLog(`W4 cycle ${i + 1}/${cycles}`);
      const t0 = performance.now();
      db = await createDb({ appId, dbName, logLevel: "warn" });
      await db.all(
        query<TaskRow>(
          "tasks",
          [{ column: "project_id", op: "eq", value: hotProjectId }],
          [["updated_at", "desc"]],
          200,
        ),
        "local",
      );
      await db.shutdown();
      db = null;
      latencies.push(performance.now() - t0);
    }
    const wallMs = performance.now() - wallStart;

    return {
      scenario_id: w4.id,
      scenario_name: w4.name,
      profile_id: config.id,
      topology: "local_only",
      total_operations: cycles,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: cycles / Math.max(0.001, wallMs / 1000),
      operation_summaries: {
        cold_reopen: summarizeLatencies(latencies),
      },
      extra: { cycles },
    };
  } finally {
    if (db) await db.shutdown();
  }
}

async function runB1(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B1 start");
  const appId = await benchmarkAppId("b1");
  const dbName = uniqueDbName("b1");
  const rng = new Lcg(b1.seed ^ config.seed);
  const insertCount = Math.min(b1.insert_count, CI_BROWSER_LIMITS.b1InsertCount);
  const updateCount = Math.min(b1.update_count, CI_BROWSER_LIMITS.b1UpdateCount);
  const deleteCount = Math.min(
    Math.min(b1.delete_count, CI_BROWSER_LIMITS.b1DeleteCount),
    insertCount,
  );
  const insertedCommentIds: string[] = [];
  const latencies: Record<string, number[]> = {};
  const opCounts: Record<string, number> = {};

  let db: Db | null = null;
  try {
    db = await createServerDb(appId, dbName, "realistic-b1");
    const state = await seedDataset(db, config);
    const wallStart = performance.now();
    const reportInsertProgress = createProgressReporter("B1 inserts", insertCount);
    const reportUpdateProgress = createProgressReporter("B1 updates", updateCount);
    const reportDeleteProgress = createProgressReporter("B1 deletes", deleteCount);

    for (let i = 0; i < insertCount; ) {
      const batch = await measureBatchedLatency(insertCount - i, async (batchIndex) => {
        const currentIndex = i + batchIndex;
        const taskIdx = rng.nextInt(state.taskIds.length);
        const { id } = await db.insert(commentsTable, {
          task_id: state.taskIds[taskIdx],
          author_id: state.users[rng.nextInt(state.users.length)],
          body: `b1_insert_comment_${currentIndex}`,
          created_at: nowMicros(),
        });
        insertedCommentIds.push(id);
      });
      (latencies.insert_sync ||= []).push(batch.perOpMs);
      opCounts.insert_sync = (opCounts.insert_sync || 0) + batch.repeats;
      i += batch.repeats;
      reportInsertProgress(i);
    }

    for (let i = 0; i < updateCount; ) {
      const batch = await measureBatchedLatency(updateCount - i, async () => {
        const taskId = state.taskIds[rng.nextInt(state.taskIds.length)];
        await db.update(tasksTable, taskId, {
          priority: 1 + rng.nextInt(4),
          status: ["todo", "in_progress", "review", "done"][rng.nextInt(4)],
          updated_at: nowMicros(),
        });
      });
      (latencies.update_sync ||= []).push(batch.perOpMs);
      opCounts.update_sync = (opCounts.update_sync || 0) + batch.repeats;
      i += batch.repeats;
      reportUpdateProgress(i);
    }

    for (let i = 0; i < deleteCount; ) {
      const batch = await measureBatchedLatency(deleteCount - i, async (batchIndex) => {
        await db.delete(commentsTable, insertedCommentIds[i + batchIndex]);
      });
      (latencies.delete_sync ||= []).push(batch.perOpMs);
      opCounts.delete_sync = (opCounts.delete_sync || 0) + batch.repeats;
      i += batch.repeats;
      reportDeleteProgress(i);
    }

    const wallMs = performance.now() - wallStart;
    const totalOperations = insertCount + updateCount + deleteCount;
    const operationSummaries: Record<string, OpSummary> = {};
    for (const [op, samples] of Object.entries(latencies)) {
      operationSummaries[op] = summarizeLatencies(samples, opCounts[op] ?? samples.length);
    }

    return {
      scenario_id: b1.id,
      scenario_name: b1.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: totalOperations,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: totalOperations / Math.max(0.001, wallMs / 1000),
      operation_summaries: operationSummaries,
      extra: {
        inserts: insertCount,
        updates: updateCount,
        deletes: deleteCount,
      },
    };
  } finally {
    if (db) await db.shutdown();
  }
}

async function runB2(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B2 start");
  const appId = await benchmarkAppId("b2");
  const dbName = uniqueDbName("b2");
  const rng = new Lcg(b2.seed ^ config.seed);
  const requestCount = Math.min(b2.request_count, CI_BROWSER_LIMITS.b2RequestCount);
  const weights = b2.mix.map((x) => x.weight);
  const latencies: Record<string, number[]> = {};
  const opCounts: Record<string, number> = {};

  let db: Db | null = null;
  try {
    db = await createServerDb(appId, dbName, "realistic-b2");
    const state = await seedDataset(db, config);

    const wallStart = performance.now();
    const reportProgress = createProgressReporter("B2 reads", requestCount);
    for (let i = 0; i < requestCount; ) {
      const op = b2.mix[rng.pickWeightedIndex(weights)].operation;
      const batch = await measureBatchedLatency(requestCount - i, async () => {
        switch (op) {
          case "query_board": {
            const project = state.projects[rng.nextInt(state.hotProjectCount)];
            await db.all(
              query<TaskRow>(
                "tasks",
                [{ column: "project_id", op: "eq", value: project }],
                [["updated_at", "desc"]],
                200,
              ),
            );
            break;
          }
          case "query_my_work": {
            const assignee = state.users[rng.nextInt(state.users.length)];
            await db.all(
              query<TaskRow>(
                "tasks",
                [
                  { column: "assignee_id", op: "eq", value: assignee },
                  { column: "status", op: "eq", value: "in_progress" },
                ],
                [["updated_at", "desc"]],
                200,
              ),
            );
            break;
          }
          case "query_task_detail": {
            const taskId = state.taskIds[rng.nextInt(state.taskIds.length)];
            await db.all(
              query<CommentRow>(
                "task_comments",
                [{ column: "task_id", op: "eq", value: taskId }],
                [["created_at", "desc"]],
                200,
              ),
            );
            await db.all(
              query<ActivityRow>(
                "activity_events",
                [{ column: "task_id", op: "eq", value: taskId }],
                [["created_at", "desc"]],
                200,
              ),
            );
            break;
          }
          default:
            throw new Error(`Unknown operation in B2 mix: ${op}`);
        }
      });
      (latencies[op] ||= []).push(batch.perOpMs);
      opCounts[op] = (opCounts[op] || 0) + batch.repeats;
      i += batch.repeats;
      reportProgress(i);
    }
    const wallMs = performance.now() - wallStart;

    const operationSummaries: Record<string, OpSummary> = {};
    for (const [op, samples] of Object.entries(latencies)) {
      operationSummaries[op] = summarizeLatencies(samples, opCounts[op] ?? samples.length);
    }

    return {
      scenario_id: b2.id,
      scenario_name: b2.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: requestCount,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: requestCount / Math.max(0.001, wallMs / 1000),
      operation_summaries: operationSummaries,
      extra: {
        read_mix: b2.mix,
      },
    };
  } finally {
    if (db) await db.shutdown();
  }
}

async function runB3(config: ProfileConfig): Promise<ScenarioResult> {
  const appId = await benchmarkAppId("b3");
  const dbName = uniqueDbName("b3");
  const cycles = Math.min(b3.reopen_cycles, CI_BROWSER_LIMITS.b3Cycles);
  const largeConfig = scaledLargeProfile(
    config,
    Math.min(b3.large_multiplier, CI_BROWSER_LIMITS.b3LargeMultiplier),
  );
  progressLog(
    `B3 start cycles=${cycles} tasks=${largeConfig.tasks} comments=${largeConfig.comments}`,
  );
  const latencies: number[] = [];
  let seedDb: Db | null = null;
  let cycleDb: Db | null = null;

  try {
    seedDb = await createServerDb(appId, dbName, "realistic-b3-seed");
    const state = await seedDataset(seedDb, largeConfig);
    const hotProjectId = state.projects[0];
    await seedDb.all(
      query<TaskRow>(
        "tasks",
        [{ column: "project_id", op: "eq", value: hotProjectId }],
        [["updated_at", "desc"]],
        200,
      ),
    );
    await seedDb.shutdown();
    seedDb = null;

    const wallStart = performance.now();
    for (let i = 0; i < cycles; i += 1) {
      progressLog(`B3 cycle ${i + 1}/${cycles}`);
      const t0 = performance.now();
      cycleDb = await createServerDb(appId, dbName, "realistic-b3-cycle");
      await cycleDb.all(
        query<TaskRow>(
          "tasks",
          [{ column: "project_id", op: "eq", value: hotProjectId }],
          [["updated_at", "desc"]],
          200,
        ),
      );
      await cycleDb.shutdown();
      cycleDb = null;
      latencies.push(performance.now() - t0);
    }
    const wallMs = performance.now() - wallStart;

    return {
      scenario_id: b3.id,
      scenario_name: b3.name,
      profile_id: largeConfig.id,
      topology: "single_hop_browser",
      total_operations: cycles,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: cycles / Math.max(0.001, wallMs / 1000),
      operation_summaries: {
        cold_reopen_query: summarizeLatencies(latencies),
      },
      extra: {
        cycles,
        dataset: {
          users: largeConfig.users,
          projects: largeConfig.projects,
          tasks: largeConfig.tasks,
          comments: largeConfig.comments,
          activity_events: largeConfig.activity_events,
        },
      },
    };
  } finally {
    if (seedDb) await seedDb.shutdown();
    if (cycleDb) await cycleDb.shutdown();
  }
}

async function runB4(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B4 start");
  const appId = await benchmarkAppId("b4");
  const dbName = uniqueDbName("b4");
  const subscriberCounts = b4.subscriber_counts.map((x) =>
    Math.max(1, Math.min(CI_BROWSER_LIMITS.b4MaxSubscribers, x)),
  );
  const rounds = Math.min(b4.rounds, CI_BROWSER_LIMITS.b4Rounds);
  const timeoutMs = Math.min(b4.timeout_seconds, 30) * 1000;
  const fanoutDeliveryLatencies: number[] = [];
  const operationSummaries: Record<string, OpSummary> = {};
  const fanoutElemsPerSec: Record<string, number> = {};
  let writer: Db | null = null;

  try {
    writer = await createServerDb(appId, dbName, "realistic-b4-writer");
    const state = await seedDataset(writer, config);
    const targetTaskId = state.taskIds[0];
    await writer.update(tasksTable, targetTaskId, {
      priority: 0,
      updated_at: nowMicros(),
    });

    const wallStart = performance.now();
    let totalRounds = 0;
    for (const subscriberCount of subscriberCounts) {
      progressLog(`B4 fanout subscribers=${subscriberCount}`);
      const unsubscribeFns: Array<() => void> = [];
      const seenAt = new Array<number>(subscriberCount).fill(0);
      const ready = new Array<boolean>(subscriberCount).fill(false);
      let targetPriority = -1;

      try {
        for (let i = 0; i < subscriberCount; i += 1) {
          const unsubscribe = writer.subscribeAll(
            query<TaskRow>("tasks", [{ column: "id", op: "eq", value: targetTaskId }], [], 1),
            (delta) => {
              ready[i] = true;
              const row = delta.all[0];
              if (!row) return;
              if (row.priority === targetPriority && seenAt[i] === 0) {
                seenAt[i] = performance.now();
              }
            },
          );
          unsubscribeFns.push(unsubscribe);
        }

        await waitForCondition(`fanout subscribe ready n=${subscriberCount}`, timeoutMs, () =>
          ready.every(Boolean),
        );

        // Warmup update confirms all subscribers are active before measured rounds.
        targetPriority = 9_000 + subscriberCount;
        seenAt.fill(0);
        await writer.update(tasksTable, targetTaskId, {
          priority: targetPriority,
          updated_at: nowMicros(),
        });
        await waitForCondition(`fanout warmup n=${subscriberCount}`, timeoutMs, () =>
          seenAt.every((x) => x > 0),
        );

        const perCountLatencies: number[] = [];
        for (let round = 0; round < rounds; round += 1) {
          targetPriority = 10_000 + subscriberCount * 100 + round;
          seenAt.fill(0);

          const t0 = performance.now();
          await writer.update(tasksTable, targetTaskId, {
            priority: targetPriority,
            updated_at: nowMicros(),
          });
          await waitForCondition(`fanout n=${subscriberCount} round=${round + 1}`, timeoutMs, () =>
            seenAt.every((x) => x > 0),
          );

          const deliveredAt = Math.max(...seenAt);
          const deliveryMs = Math.max(0, deliveredAt - t0);
          perCountLatencies.push(deliveryMs);
          fanoutDeliveryLatencies.push(deliveryMs);
          totalRounds += 1;
        }

        operationSummaries[`fanout_delivery_n${subscriberCount}`] =
          summarizeLatencies(perCountLatencies);
        const groupWallMs = perCountLatencies.reduce((sum, x) => sum + x, 0);
        fanoutElemsPerSec[`n${subscriberCount}`] =
          (subscriberCount * perCountLatencies.length) / Math.max(0.001, groupWallMs / 1000);
      } finally {
        while (unsubscribeFns.length > 0) {
          const unsubscribe = unsubscribeFns.pop();
          if (unsubscribe) unsubscribe();
        }
      }
    }

    const wallMs = performance.now() - wallStart;
    operationSummaries.fanout_delivery = summarizeLatencies(fanoutDeliveryLatencies);

    return {
      scenario_id: b4.id,
      scenario_name: b4.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: totalRounds,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: totalRounds / Math.max(0.001, wallMs / 1000),
      operation_summaries: operationSummaries,
      extra: {
        subscriber_counts: subscriberCounts,
        rounds_per_count: rounds,
        fanout_elems_per_sec: fanoutElemsPerSec,
      },
    };
  } finally {
    if (writer) await writer.shutdown();
  }
}

function permissionRecursiveSchema(recursiveDepth: number): WasmSchema {
  const folderSelectPolicy = {
    using: {
      type: "Or",
      exprs: [
        {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        {
          type: "And",
          exprs: [
            {
              type: "IsNotNull",
              column: "parent_id",
            },
            {
              type: "Inherits",
              operation: "Select",
              via_column: "parent_id",
              max_depth: recursiveDepth,
            },
          ],
        },
      ],
    },
  };

  const folderUpdatePolicy = {
    using: {
      type: "Or",
      exprs: [
        {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        {
          type: "And",
          exprs: [
            {
              type: "IsNotNull",
              column: "parent_id",
            },
            {
              type: "Inherits",
              operation: "Update",
              via_column: "parent_id",
              max_depth: recursiveDepth,
            },
          ],
        },
      ],
    },
    with_check: {
      type: "Or",
      exprs: [
        {
          type: "Cmp",
          column: "owner_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        {
          type: "And",
          exprs: [
            {
              type: "IsNotNull",
              column: "parent_id",
            },
            {
              type: "Inherits",
              operation: "Update",
              via_column: "parent_id",
              max_depth: recursiveDepth,
            },
          ],
        },
      ],
    },
  };

  const documentSelectPolicy = {
    using: {
      type: "Inherits",
      operation: "Select",
      via_column: "folder_id",
      max_depth: recursiveDepth,
    },
  };

  const documentUpdatePolicy = {
    using: {
      type: "And",
      exprs: [
        {
          type: "Cmp",
          column: "editor_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        {
          type: "Inherits",
          operation: "Update",
          via_column: "folder_id",
          max_depth: recursiveDepth,
        },
      ],
    },
    with_check: {
      type: "And",
      exprs: [
        {
          type: "Cmp",
          column: "editor_id",
          op: "Eq",
          value: {
            type: "SessionRef",
            path: ["user_id"],
          },
        },
        {
          type: "Inherits",
          operation: "Update",
          via_column: "folder_id",
          max_depth: recursiveDepth,
        },
      ],
    },
  };

  return {
    folders: {
      columns: [
        {
          name: "parent_id",
          column_type: { type: "Uuid" },
          nullable: true,
          references: "folders",
        },
        {
          name: "owner_id",
          column_type: { type: "Text" },
          nullable: false,
        },
        {
          name: "title",
          column_type: { type: "Text" },
          nullable: false,
        },
        {
          name: "updated_at",
          column_type: { type: "Timestamp" },
          nullable: false,
        },
      ],
      policies: {
        select: folderSelectPolicy,
        insert: {},
        update: folderUpdatePolicy,
        delete: {},
      },
    },
    documents: {
      columns: [
        {
          name: "folder_id",
          column_type: { type: "Uuid" },
          nullable: false,
          references: "folders",
        },
        {
          name: "editor_id",
          column_type: { type: "Text" },
          nullable: false,
        },
        {
          name: "body",
          column_type: { type: "Text" },
          nullable: false,
        },
        {
          name: "revision",
          column_type: { type: "Integer" },
          nullable: false,
        },
        {
          name: "updated_at",
          column_type: { type: "Timestamp" },
          nullable: false,
        },
      ],
      policies: {
        select: documentSelectPolicy,
        insert: {},
        update: documentUpdatePolicy,
        delete: {},
      },
    },
  };
}

interface PermissionSeedState {
  allowedDocumentIds: string[];
  allowedEditableDocumentIds: string[];
  allowedReadOnlyDocumentIds: string[];
  deniedDocumentIds: string[];
}

async function seedPermissionDataset(
  db: Db,
  scenario: B5Scenario,
  permissionSchema: WasmSchema,
  owners: {
    allowedOwnerId: string;
    deniedOwnerId: string;
    intermediateOwnerId: string;
  },
  tier: PersistenceTier = "local",
): Promise<PermissionSeedState> {
  const folderTable = tableProxy<PermissionFolderRow, Omit<PermissionFolderRow, "id">>(
    "folders",
    permissionSchema,
  );
  const documentTable = tableProxy<PermissionDocumentRow, Omit<PermissionDocumentRow, "id">>(
    "documents",
    permissionSchema,
  );
  const rng = new Lcg(scenario.seed);
  const totalFolders = Math.max(4, scenario.folders);
  const totalDocuments = Math.max(20, scenario.documents);
  progressLog(`B5 seed start tier=${tier} folders=${totalFolders} documents=${totalDocuments}`);

  const allowedFolders: string[] = [];
  const deniedFolders: string[] = [];
  const ts = nowMicros();
  const { id: allowedRootId } = await db
    .insert(folderTable, {
      parent_id: null,
      owner_id: owners.allowedOwnerId,
      title: "allowed-root",
      updated_at: ts,
    })
    .wait(durabilityOptions(tier));
  const { id: deniedRootId } = await db
    .insert(folderTable, {
      parent_id: null,
      owner_id: owners.deniedOwnerId,
      title: "denied-root",
      updated_at: ts + 1,
    })
    .wait(durabilityOptions(tier));
  allowedFolders.push(allowedRootId);
  deniedFolders.push(deniedRootId);

  for (let i = 2; i < totalFolders; i += 1) {
    reportLoopProgress("B5 seed folders", i, totalFolders);
    const allowedChain = i % 2 === 0;
    const parent = allowedChain
      ? allowedFolders[allowedFolders.length - 1]
      : deniedFolders[deniedFolders.length - 1];
    const { id } = await db
      .insert(folderTable, {
        parent_id: parent,
        // Keep allowed-chain folders owned by the allowed principal so that
        // permissioned document updates can validate folder FKs locally.
        owner_id: allowedChain ? owners.allowedOwnerId : owners.deniedOwnerId,
        title: `folder-${i}`,
        updated_at: ts + i,
      })
      .wait(durabilityOptions(tier));
    if (allowedChain) {
      allowedFolders.push(id);
    } else {
      deniedFolders.push(id);
    }
  }

  const allowedDocumentIds: string[] = [];
  const allowedEditableDocumentIds: string[] = [];
  const allowedReadOnlyDocumentIds: string[] = [];
  const deniedDocumentIds: string[] = [];
  const allowThreshold = Math.max(1, Math.min(99, Math.round(scenario.allow_fraction * 100)));
  for (let i = 0; i < totalDocuments; i += 1) {
    reportLoopProgress("B5 seed documents", i, totalDocuments);
    const useAllowed = rng.nextInt(100) < allowThreshold;
    const folderList = useAllowed ? allowedFolders : deniedFolders;
    const folderId = folderList[rng.nextInt(folderList.length)];
    const allowAllowedUserWrite = rng.nextInt(100) < allowThreshold;
    const editorId = useAllowed
      ? allowAllowedUserWrite
        ? owners.allowedOwnerId
        : owners.intermediateOwnerId
      : owners.deniedOwnerId;
    const { id } = await db
      .insert(documentTable, {
        folder_id: folderId,
        editor_id: editorId,
        body: `doc-${i}`,
        revision: 0,
        updated_at: ts + 10_000 + i,
      })
      .wait(durabilityOptions(tier));
    if (useAllowed) {
      allowedDocumentIds.push(id);
      if (editorId === owners.allowedOwnerId) {
        allowedEditableDocumentIds.push(id);
      } else {
        allowedReadOnlyDocumentIds.push(id);
      }
    } else {
      deniedDocumentIds.push(id);
    }
  }

  progressLog(
    `B5 seed complete allowed=${allowedDocumentIds.length} denied=${deniedDocumentIds.length} editable=${allowedEditableDocumentIds.length} read_only=${allowedReadOnlyDocumentIds.length}`,
  );

  return {
    allowedDocumentIds,
    allowedEditableDocumentIds,
    allowedReadOnlyDocumentIds,
    deniedDocumentIds,
  };
}

async function runB5(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B5 start");
  const appId = await benchmarkAppId("b5");
  const dbPrefix = uniqueDbName("b5");
  const rng = new Lcg(b5.seed ^ config.seed);
  const permissionSchema = permissionRecursiveSchema(Math.max(1, b5.recursive_depth));
  const reads = Math.min(b5.read_request_count, CI_BROWSER_LIMITS.b5ReadRequests);
  const updates = Math.min(b5.update_attempt_count, CI_BROWSER_LIMITS.b5UpdateAttempts);
  const latencies: Record<string, number[]> = {};
  const documentTable = tableProxy<PermissionDocumentRow, Omit<PermissionDocumentRow, "id">>(
    "documents",
    permissionSchema,
  );
  const documentsQuery = query<PermissionDocumentRow>(
    "documents",
    [],
    [["updated_at", "desc"]],
    200,
    permissionSchema,
  );
  const foldersQuery = query<PermissionFolderRow>(
    "folders",
    [],
    [["updated_at", "desc"]],
    200,
    permissionSchema,
  );
  const visibleDocumentsQuery = query<PermissionDocumentRow>(
    "documents",
    [],
    [["updated_at", "desc"]],
    1000,
    permissionSchema,
  );
  let seedDb: Db | null = null;
  let allowedDb: Db | null = null;
  let deniedDb: Db | null = null;
  let unsubscribeAllowed: (() => void) | null = null;
  let unsubscribeAllowedFolders: (() => void) | null = null;
  let unsubscribeDenied: (() => void) | null = null;
  let unsubscribeDeniedFolders: (() => void) | null = null;

  try {
    const seedLocalSecret = generateAuthSecret();
    const allowedLocalSecret = generateAuthSecret();
    const deniedLocalSecret = generateAuthSecret();
    const intermediateLocalSecret = generateAuthSecret();
    const wasmModule = await loadWasmModule();
    const allowedPrincipalId = wasmModule.WasmRuntime.deriveUserId(allowedLocalSecret);
    const deniedPrincipalId = wasmModule.WasmRuntime.deriveUserId(deniedLocalSecret);
    const intermediatePrincipalId = wasmModule.WasmRuntime.deriveUserId(intermediateLocalSecret);
    const allowedSession = {
      user_id: allowedPrincipalId,
      claims: {
        auth_mode: "local-first",
      },
    };
    const deniedSession = {
      user_id: deniedPrincipalId,
      claims: {
        auth_mode: "local-first",
      },
    };

    // Seed with a sync-enabled client, then validate with two non-privileged
    // browser clients that authenticate only via local-auth HTTP headers.
    seedDb = await createServerDb(
      appId,
      `${dbPrefix}-seed`,
      "realistic-b5-seed",
      {},
      {
        includeJwt: false,
        localFirstSecret: seedLocalSecret,
      },
    );
    const seeded = await seedPermissionDataset(seedDb, b5, permissionSchema, {
      allowedOwnerId: allowedPrincipalId,
      deniedOwnerId: deniedPrincipalId,
      intermediateOwnerId: intermediatePrincipalId,
    });
    const seededDeniedCount = seeded.deniedDocumentIds.length;
    const allowedCount = seeded.allowedDocumentIds.length;
    if (seededDeniedCount === 0 || allowedCount === 0) {
      throw new Error("B5 requires both allowed and denied document populations");
    }
    if (
      seeded.allowedEditableDocumentIds.length === 0 ||
      seeded.allowedReadOnlyDocumentIds.length === 0
    ) {
      throw new Error("B5 requires both editable and read-only allowed documents");
    }

    allowedDb = await createServerDb(
      appId,
      `${dbPrefix}-allowed`,
      "realistic-b5-allowed",
      {},
      {
        includeAdminSecret: false,
        includeJwt: false,
        localFirstSecret: allowedLocalSecret,
      },
    );
    deniedDb = await createServerDb(
      appId,
      `${dbPrefix}-denied`,
      "realistic-b5-denied",
      {},
      {
        includeAdminSecret: false,
        includeJwt: false,
        localFirstSecret: deniedLocalSecret,
      },
    );

    let warmAllowedVisible = 0;
    let warmDeniedVisible = 0;
    let initialAllowedVisible: PermissionDocumentRow[] = [];
    let initialDeniedVisible: PermissionDocumentRow[] = [];
    let visibleAllowedFolders: PermissionFolderRow[] = [];
    unsubscribeAllowed = allowedDb.subscribeAll(
      visibleDocumentsQuery,
      (delta) => {
        initialAllowedVisible = delta.all;
        warmAllowedVisible = delta.all.length;
      },
      undefined,
      allowedSession,
    );
    unsubscribeAllowedFolders = allowedDb.subscribeAll(
      foldersQuery,
      (delta) => {
        visibleAllowedFolders = delta.all;
      },
      undefined,
      allowedSession,
    );
    unsubscribeDenied = deniedDb.subscribeAll(
      visibleDocumentsQuery,
      (delta) => {
        initialDeniedVisible = delta.all;
        warmDeniedVisible = delta.all.length;
      },
      undefined,
      deniedSession,
    );
    unsubscribeDeniedFolders = deniedDb.subscribeAll(
      foldersQuery,
      () => {
        // Keep recursive folder visibility materialized for the denied client too.
      },
      undefined,
      deniedSession,
    );
    let warmed = false;
    const warmupDeadline = performance.now() + 20_000;
    let lastWarmupLog = 0;
    while (performance.now() < warmupDeadline) {
      const allowedVisibleIds = new Set(initialAllowedVisible.map((row) => row.id));
      const deniedVisibleIds = new Set(initialDeniedVisible.map((row) => row.id));
      const allowedHasVisibleOwned = seeded.allowedDocumentIds.some((id) =>
        allowedVisibleIds.has(id),
      );
      const deniedHasVisibleOwned = seeded.deniedDocumentIds.some((id) => deniedVisibleIds.has(id));
      if (allowedHasVisibleOwned && deniedHasVisibleOwned) {
        warmed = true;
        break;
      }

      const now = performance.now();
      if (now - lastWarmupLog >= 1000) {
        lastWarmupLog = now;
        progressLog(
          `B5 warmup waiting allowed_visible=${warmAllowedVisible} denied_visible=${warmDeniedVisible}`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, 50));
    }
    if (!warmed) {
      throw new Error(
        `B5 warmup timed out allowed_visible=${warmAllowedVisible} denied_visible=${warmDeniedVisible}`,
      );
    }
    progressLog(
      `B5 warmup complete allowed_visible=${warmAllowedVisible} denied_visible=${warmDeniedVisible}`,
    );
    const initialAllowedVisibleIds = new Set(initialAllowedVisible.map((row) => row.id));
    const initialAllowedVisibleById = new Map(initialAllowedVisible.map((row) => [row.id, row]));
    const initialDeniedVisibleIds = new Set(initialDeniedVisible.map((row) => row.id));
    const visibleAllowedFolderIds = new Set(visibleAllowedFolders.map((row) => row.id));
    const deniedDocumentIdsForAllowed = seeded.deniedDocumentIds.filter(
      (id) => !initialAllowedVisibleIds.has(id),
    );

    const allowedUpdateIds = seeded.allowedEditableDocumentIds.filter((id) => {
      const row = initialAllowedVisibleById.get(id);
      return row != null && visibleAllowedFolderIds.has(row.folder_id);
    });
    const deniedUpdateIds = deniedDocumentIdsForAllowed;
    const hasAllowedUpdateCandidates = allowedUpdateIds.length > 0;
    if (!hasAllowedUpdateCandidates) {
      progressLog(
        `B5 no editable update candidates (visible_docs=${initialAllowedVisibleIds.size}, visible_folders=${visibleAllowedFolderIds.size})`,
      );
    }
    if (deniedUpdateIds.length === 0) {
      throw new Error("B5 requires at least one non-visible denied document for denied updates");
    }
    progressLog(
      `B5 candidates editable=${allowedUpdateIds.length} denied=${deniedUpdateIds.length}`,
    );

    let allowedUpdateSuccess = 0;
    let deniedUpdateRejected = 0;
    let unexpectedDeniedForAllowed = 0;
    let unexpectedAllowedForDenied = 0;
    let firstAllowedUpdateError: string | null = null;
    let firstDeniedUpdateError: string | null = null;

    const wallStart = performance.now();
    const opCounts: Record<string, number> = {};
    const reportReadProgress = createProgressReporter("B5 reads", reads);
    const reportUpdateProgress = createProgressReporter("B5 updates", updates);

    for (let i = 0; i < reads; ) {
      const batch = await measureBatchedLatency(reads - i, async (batchIndex) => {
        if ((i + batchIndex) % 2 === 0) {
          await allowedDb.all(documentsQuery);
        } else {
          await allowedDb.all(foldersQuery);
        }
      });
      (latencies.permission_reads ||= []).push(batch.perOpMs);
      opCounts.permission_reads = (opCounts.permission_reads || 0) + batch.repeats;
      i += batch.repeats;
      reportReadProgress(i);
    }

    for (let i = 0; i < updates; ) {
      const shouldAllow =
        hasAllowedUpdateCandidates &&
        (deniedUpdateIds.length === 0 || rng.nextInt(100) < Math.round(b5.allow_fraction * 100));
      const targetIds = shouldAllow ? allowedUpdateIds : deniedUpdateIds;
      const batchKey = shouldAllow ? "permission_updates_allowed" : "permission_updates_denied";
      const batch = await measureBatchedLatency(
        updates - i,
        async (batchIndex) => {
          const currentIndex = i + batchIndex;
          const targetId = targetIds[rng.nextInt(targetIds.length)];
          try {
            await allowedDb.update(documentTable, targetId, {
              body: `b5-update-${currentIndex}`,
              revision: currentIndex + 1,
              updated_at: nowMicros(),
            });
            if (shouldAllow) {
              allowedUpdateSuccess += 1;
            } else {
              unexpectedAllowedForDenied += 1;
            }
          } catch (error) {
            const message = error instanceof Error ? error.message : String(error);
            if (shouldAllow) {
              unexpectedDeniedForAllowed += 1;
              if (firstAllowedUpdateError == null) {
                firstAllowedUpdateError = message;
              }
            } else {
              deniedUpdateRejected += 1;
              if (firstDeniedUpdateError == null) {
                firstDeniedUpdateError = message;
              }
            }
          }
        },
        { maxRepeats: 1 },
      );
      (latencies[batchKey] ||= []).push(batch.perOpMs);
      opCounts[batchKey] = (opCounts[batchKey] || 0) + batch.repeats;
      i += batch.repeats;
      reportUpdateProgress(i);
    }
    const wallMs = performance.now() - wallStart;

    const visibleAllowedDocuments = initialAllowedVisible;
    const visibleDeniedDocuments = initialDeniedVisible;
    const allowedVisibleIds = new Set(visibleAllowedDocuments.map((row) => row.id));
    const deniedVisibleIds = new Set(visibleDeniedDocuments.map((row) => row.id));
    const leakedDeniedReads = deniedDocumentIdsForAllowed.filter((id) =>
      allowedVisibleIds.has(id),
    ).length;
    const visibleAllowedReads = seeded.allowedDocumentIds.filter((id) =>
      allowedVisibleIds.has(id),
    ).length;
    const deniedOwnVisibleReads = seeded.deniedDocumentIds.filter((id) =>
      deniedVisibleIds.has(id),
    ).length;
    const leakedAllowedToDeniedReads = seeded.allowedDocumentIds.filter((id) =>
      deniedVisibleIds.has(id),
    ).length;

    const operationSummaries: Record<string, OpSummary> = {};
    for (const [op, samples] of Object.entries(latencies)) {
      operationSummaries[op] = summarizeLatencies(samples, opCounts[op] ?? samples.length);
    }

    return {
      scenario_id: b5.id,
      scenario_name: b5.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: reads + updates,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: (reads + updates) / Math.max(0.001, wallMs / 1000),
      operation_summaries: operationSummaries,
      extra: {
        recursive_depth: b5.recursive_depth,
        allowed_documents_seeded: allowedCount,
        denied_documents_seeded: deniedDocumentIdsForAllowed.length,
        denied_documents_seeded_total: seededDeniedCount,
        denied_documents_initially_leaked: seededDeniedCount - deniedDocumentIdsForAllowed.length,
        allowed_updates_succeeded: allowedUpdateSuccess,
        denied_updates_rejected: deniedUpdateRejected,
        unexpected_denied_for_allowed: unexpectedDeniedForAllowed,
        unexpected_allowed_for_denied: unexpectedAllowedForDenied,
        first_allowed_update_error: firstAllowedUpdateError,
        first_denied_update_error: firstDeniedUpdateError,
        warm_visible_documents_allowed: warmAllowedVisible,
        warm_visible_documents_denied: warmDeniedVisible,
        allowed_update_candidates: allowedUpdateIds.length,
        denied_update_candidates: deniedUpdateIds.length,
        visible_documents_total: visibleAllowedDocuments.length,
        allowed_documents_visible: visibleAllowedReads,
        denied_documents_visible: leakedDeniedReads,
        denied_documents_visible_to_denied: deniedOwnVisibleReads,
        allowed_documents_visible_to_denied: leakedAllowedToDeniedReads,
        allowed_editable_documents_seeded: seeded.allowedEditableDocumentIds.length,
        allowed_read_only_documents_seeded: seeded.allowedReadOnlyDocumentIds.length,
        denied_documents_visible_initial_to_allowed: seeded.deniedDocumentIds.filter((id) =>
          initialAllowedVisibleIds.has(id),
        ).length,
        allowed_documents_visible_initial_to_denied: seeded.allowedDocumentIds.filter((id) =>
          initialDeniedVisibleIds.has(id),
        ).length,
      },
    };
  } finally {
    if (unsubscribeAllowed) unsubscribeAllowed();
    if (unsubscribeAllowedFolders) unsubscribeAllowedFolders();
    if (unsubscribeDenied) unsubscribeDenied();
    if (unsubscribeDeniedFolders) unsubscribeDeniedFolders();
    if (seedDb) await seedDb.shutdown();
    if (allowedDb) await allowedDb.shutdown();
    if (deniedDb) await deniedDb.shutdown();
  }
}

async function runB6(config: ProfileConfig): Promise<ScenarioResult> {
  progressLog("B6 start");
  const appId = await benchmarkAppId("b6");
  const dbName = uniqueDbName("b6");
  const rng = new Lcg(b6.seed ^ config.seed);
  const updateCount = Math.min(b6.update_count, CI_BROWSER_LIMITS.b6UpdateCount);
  const latencies: number[] = [];
  let measuredUpdates = 0;
  let db: Db | null = null;

  try {
    db = await createServerDb(appId, dbName, "realistic-b6");
    const state = await seedDataset(db, config);
    const hotTaskCount = Math.max(1, Math.min(state.taskIds.length, b6.hot_task_count));
    const hotTasks = state.taskIds.slice(0, hotTaskCount);
    const beforeBytes = await storageUsageBytes();

    const wallStart = performance.now();
    const reportProgress = createProgressReporter("B6 hotspot updates", updateCount);
    for (let i = 0; i < updateCount; ) {
      const batch = await measureBatchedLatency(updateCount - i, async (batchIndex) => {
        const currentIndex = i + batchIndex;
        const taskId = hotTasks[rng.nextInt(hotTasks.length)];
        await db.update(tasksTable, taskId, {
          title: `Hotspot ${taskId.slice(0, 8)} rev ${currentIndex}`,
          priority: 1 + (currentIndex % 4),
          status: ["todo", "in_progress", "review", "done"][currentIndex % 4],
          updated_at: nowMicros(),
        });
      });
      latencies.push(batch.perOpMs);
      measuredUpdates += batch.repeats;
      i += batch.repeats;
      reportProgress(i);
    }
    const wallMs = performance.now() - wallStart;
    const afterBytes = await storageUsageBytes();
    const storageDelta =
      beforeBytes != null && afterBytes != null ? Math.max(0, afterBytes - beforeBytes) : null;

    await db.all(query<TaskRow>("tasks", [{ column: "id", op: "eq", value: hotTasks[0] }], [], 1));

    return {
      scenario_id: b6.id,
      scenario_name: b6.name,
      profile_id: config.id,
      topology: "single_hop_browser",
      total_operations: updateCount,
      wall_time_ms: wallMs,
      throughput_ops_per_sec: updateCount / Math.max(0.001, wallMs / 1000),
      operation_summaries: {
        hotspot_update_sync: summarizeLatencies(latencies, measuredUpdates),
      },
      extra: {
        hot_task_count: hotTaskCount,
        storage_usage_before_bytes: beforeBytes,
        storage_usage_after_bytes: afterBytes,
        storage_usage_delta_bytes: storageDelta,
      },
    };
  } finally {
    if (db) await db.shutdown();
  }
}

describe("realistic browser benchmark harness", () => {
  it("delivers an initial scoped snapshot for a seeded server-backed project board query", async () => {
    const cfg = scaledProfile(profile);
    const appId = await benchmarkAppId("b4-subscribe-repro");
    const dbName = uniqueDbName("b4-subscribe-repro");
    let db: Db | null = null;

    try {
      db = await createServerDb(appId, dbName, "realistic-b4-subscribe-repro");
      const state = await seedDataset(db, cfg);
      const targetTaskId = state.taskIds[0];
      const received: TaskRow[][] = [];

      const unsubscribe = db.subscribeAll(
        query<TaskRow>("tasks", [{ column: "id", op: "eq", value: targetTaskId }], [], 1),
        (delta) => {
          received.push([...delta.all]);
        },
      );

      try {
        await waitForCondition(
          "seeded project-board row appears in initial scoped subscription snapshot",
          8_000,
          () =>
            received.some(
              (rows) =>
                rows.length === 1 &&
                rows[0]?.id === targetTaskId &&
                typeof rows[0]?.title === "string",
            ),
        );
      } finally {
        unsubscribe();
      }

      const last = received[received.length - 1];
      expect(last).toHaveLength(1);
      expect(last[0].id).toBe(targetTaskId);
    } finally {
      if (db) await db.shutdown();
    }
  }, 60_000);

  it("delivers an initial scoped snapshot after a local update on a seeded server-backed row", async () => {
    const cfg = scaledProfile(profile);
    const appId = await benchmarkAppId("b4-subscribe-after-update");
    const dbName = uniqueDbName("b4-subscribe-after-update");
    let db: Db | null = null;

    try {
      db = await createServerDb(appId, dbName, "realistic-b4-subscribe-after-update");
      const state = await seedDataset(db, cfg);
      const targetTaskId = state.taskIds[0];
      db.update(tasksTable, targetTaskId, {
        priority: 0,
        updated_at: nowMicros(),
      });

      const received: TaskRow[][] = [];
      const unsubscribe = db.subscribeAll(
        query<TaskRow>("tasks", [{ column: "id", op: "eq", value: targetTaskId }], [], 1),
        (delta) => {
          received.push([...delta.all]);
        },
      );

      try {
        await waitForCondition(
          "locally updated seeded row appears in initial scoped subscription snapshot",
          8_000,
          () =>
            received.some(
              (rows) =>
                rows.length === 1 && rows[0]?.id === targetTaskId && rows[0]?.priority === 0,
            ),
        );
      } finally {
        unsubscribe();
      }

      const last = received[received.length - 1];
      expect(last).toHaveLength(1);
      expect(last[0].id).toBe(targetTaskId);
      expect(last[0].priority).toBe(0);
    } finally {
      if (db) await db.shutdown();
    }
  }, 60_000);

  it("profiles repeated query_my_work reads on a seeded server-backed dataset", async () => {
    const cfg = scaledProfile(profile);
    const appId = await benchmarkAppId("b2-query-my-work-profile");
    const dbName = uniqueDbName("b2-query-my-work-profile");
    let db: Db | null = null;

    try {
      db = await createServerDb(appId, dbName, "realistic-b2-query-my-work-profile");
      const state = await seedDataset(db, cfg);
      const assignee = state.users[0];
      const workQuery = query<TaskRow>(
        "tasks",
        [
          { column: "assignee_id", op: "eq", value: assignee },
          { column: "status", op: "eq", value: "in_progress" },
        ],
        [["updated_at", "desc"]],
        200,
      );

      const anyDb = db as any;
      const client = anyDb.getClient(schema);
      const runtime = client.getRuntime();
      const phases: Array<{ phase: string; ms: number }> = [];

      const originalEnsureQueryReady = anyDb.ensureQueryReady.bind(anyDb);
      anyDb.ensureQueryReady = async (...args: unknown[]) => {
        const startedAt = performance.now();
        try {
          return await originalEnsureQueryReady(...args);
        } finally {
          phases.push({ phase: "db.ensureQueryReady", ms: performance.now() - startedAt });
        }
      };

      const originalWaitForRemoteReadAvailability =
        client.waitForRemoteReadAvailability.bind(client);
      client.waitForRemoteReadAvailability = async (...args: unknown[]) => {
        const startedAt = performance.now();
        try {
          return await originalWaitForRemoteReadAvailability(...args);
        } finally {
          phases.push({
            phase: "client.waitForRemoteReadAvailability",
            ms: performance.now() - startedAt,
          });
        }
      };

      const originalQueryInternal = client.queryInternal.bind(client);
      client.queryInternal = async (...args: unknown[]) => {
        const startedAt = performance.now();
        try {
          return await originalQueryInternal(...args);
        } finally {
          phases.push({ phase: "client.queryInternal", ms: performance.now() - startedAt });
        }
      };

      const originalRuntimeQuery = runtime.query.bind(runtime);
      runtime.query = async (...args: unknown[]) => {
        const startedAt = performance.now();
        try {
          return await originalRuntimeQuery(...args);
        } finally {
          phases.push({ phase: "runtime.query", ms: performance.now() - startedAt });
        }
      };

      const startedAt = performance.now();
      const rows = await db.all(workQuery);
      const totalMs = performance.now() - startedAt;
      await commands.writeRealisticBrowserReport("profile-query-my-work", {
        runner: "jazz-ts-browser-opfs-profile",
        generated_at: new Date().toISOString(),
        profile: cfg.id,
        scenarios: [
          {
            scenario_id: "PROFILE_QUERY_MY_WORK",
            scenario_name: "query_my_work_single_run",
            profile_id: cfg.id,
            topology: "single_hop_browser",
            total_operations: 1,
            wall_time_ms: totalMs,
            throughput_ops_per_sec: 1000 / Math.max(1, totalMs),
            operation_summaries: {},
            extra: {
              row_count: rows.length,
              phases,
            },
          },
        ],
      });

      expect(Array.isArray(rows)).toBe(true);
    } finally {
      if (db) await db.shutdown();
    }
  }, 180_000);

  it("profiles one recursive permission read on a warmed server-backed dataset", async () => {
    const cfg = scaledProfile(profile);
    const appId = await benchmarkAppId("b5-permission-read-profile");
    const dbPrefix = uniqueDbName("b5-permission-read-profile");
    const permissionSchema = permissionRecursiveSchema(Math.max(1, b5.recursive_depth));
    let seedDb: Db | null = null;
    let allowedDb: Db | null = null;
    let deniedDb: Db | null = null;
    let unsubscribeAllowed: (() => void) | null = null;
    let unsubscribeDenied: (() => void) | null = null;

    try {
      const seedLocalSecret = generateAuthSecret();
      const allowedLocalSecret = generateAuthSecret();
      const deniedLocalSecret = generateAuthSecret();
      const intermediateLocalSecret = generateAuthSecret();
      const wasmModule = await loadWasmModule();
      const allowedPrincipalId = wasmModule.WasmRuntime.deriveUserId(allowedLocalSecret);
      const deniedPrincipalId = wasmModule.WasmRuntime.deriveUserId(deniedLocalSecret);
      const intermediatePrincipalId = wasmModule.WasmRuntime.deriveUserId(intermediateLocalSecret);

      seedDb = await createServerDb(
        appId,
        `${dbPrefix}-seed`,
        "realistic-b5-permission-read-seed",
        {},
        { includeJwt: false, localFirstSecret: seedLocalSecret },
      );
      await seedPermissionDataset(seedDb, b5, permissionSchema, {
        allowedOwnerId: allowedPrincipalId,
        deniedOwnerId: deniedPrincipalId,
        intermediateOwnerId: intermediatePrincipalId,
      });

      allowedDb = await createServerDb(
        appId,
        `${dbPrefix}-allowed`,
        "realistic-b5-permission-read-allowed",
        {},
        {
          includeAdminSecret: false,
          includeJwt: false,
          localFirstSecret: allowedLocalSecret,
        },
      );
      deniedDb = await createServerDb(
        appId,
        `${dbPrefix}-denied`,
        "realistic-b5-permission-read-denied",
        {},
        {
          includeAdminSecret: false,
          includeJwt: false,
          localFirstSecret: deniedLocalSecret,
        },
      );

      const allowedSession = {
        user_id: allowedPrincipalId,
        claims: { auth_mode: "local-first" },
      };
      const deniedSession = {
        user_id: deniedPrincipalId,
        claims: { auth_mode: "local-first" },
      };
      const visibleDocumentsQuery = query<PermissionDocumentRow>(
        "documents",
        [],
        [["updated_at", "desc"]],
        1000,
        permissionSchema,
      );

      let warmAllowedVisible = 0;
      let warmDeniedVisible = 0;
      unsubscribeAllowed = allowedDb.subscribeAll(
        visibleDocumentsQuery,
        (delta) => {
          warmAllowedVisible = delta.all.length;
        },
        undefined,
        allowedSession,
      );
      unsubscribeDenied = deniedDb.subscribeAll(
        visibleDocumentsQuery,
        (delta) => {
          warmDeniedVisible = delta.all.length;
        },
        undefined,
        deniedSession,
      );

      const warmupDeadline = performance.now() + 30_000;
      while (performance.now() < warmupDeadline) {
        if (warmAllowedVisible > 0 && warmDeniedVisible > 0) {
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
      expect(warmAllowedVisible).toBeGreaterThan(0);
      expect(warmDeniedVisible).toBeGreaterThan(0);

      const anyDb = allowedDb as any;
      const client = anyDb.getClient(permissionSchema);
      const runtime = client.getRuntime();
      const phases: Array<{ phase: string; ms: number }> = [];

      const originalEnsureQueryReady = anyDb.ensureQueryReady.bind(anyDb);
      anyDb.ensureQueryReady = async (...args: unknown[]) => {
        const startedAt = performance.now();
        try {
          return await originalEnsureQueryReady(...args);
        } finally {
          phases.push({ phase: "db.ensureQueryReady", ms: performance.now() - startedAt });
        }
      };

      const originalWaitForRemoteReadAvailability =
        client.waitForRemoteReadAvailability.bind(client);
      client.waitForRemoteReadAvailability = async (...args: unknown[]) => {
        const startedAt = performance.now();
        try {
          return await originalWaitForRemoteReadAvailability(...args);
        } finally {
          phases.push({
            phase: "client.waitForRemoteReadAvailability",
            ms: performance.now() - startedAt,
          });
        }
      };

      const originalQueryInternal = client.queryInternal.bind(client);
      client.queryInternal = async (...args: unknown[]) => {
        const startedAt = performance.now();
        try {
          return await originalQueryInternal(...args);
        } finally {
          phases.push({ phase: "client.queryInternal", ms: performance.now() - startedAt });
        }
      };

      const originalRuntimeQuery = runtime.query.bind(runtime);
      runtime.query = async (...args: unknown[]) => {
        const startedAt = performance.now();
        try {
          return await originalRuntimeQuery(...args);
        } finally {
          phases.push({ phase: "runtime.query", ms: performance.now() - startedAt });
        }
      };

      const startedAt = performance.now();
      const rows = await allowedDb.all(visibleDocumentsQuery);
      const totalMs = performance.now() - startedAt;

      await commands.writeRealisticBrowserReport("profile-permission-read", {
        runner: "jazz-ts-browser-opfs-profile",
        generated_at: new Date().toISOString(),
        profile: cfg.id,
        scenarios: [
          {
            scenario_id: "PROFILE_PERMISSION_READ",
            scenario_name: "permission_read_single_run",
            profile_id: cfg.id,
            topology: "single_hop_browser",
            total_operations: 1,
            wall_time_ms: totalMs,
            throughput_ops_per_sec: 1000 / Math.max(1, totalMs),
            operation_summaries: {},
            extra: {
              row_count: rows.length,
              warm_allowed_visible: warmAllowedVisible,
              warm_denied_visible: warmDeniedVisible,
              phases,
            },
          },
        ],
      });

      expect(rows.length).toBeGreaterThan(0);
    } finally {
      if (unsubscribeAllowed) unsubscribeAllowed();
      if (unsubscribeDenied) unsubscribeDenied();
      if (seedDb) await seedDb.shutdown();
      if (allowedDb) await allowedDb.shutdown();
      if (deniedDb) await deniedDb.shutdown();
    }
  }, 180_000);

  it("runs local and server-backed realistic scenarios against worker OPFS runtime", async () => {
    const restoreLogs = elevateBenchLogLevel();
    const cfg = scaledProfile(profile);
    progressLog(`bench start profile=${cfg.id}`);

    try {
      const runners = [
        {
          id: "W1",
          run: async (): Promise<ScenarioResult> => {
            const appId = await benchmarkAppId("w1");
            const dbName = uniqueDbName("w1");
            let db: Db | null = null;
            try {
              db = await createDb({ appId, dbName, logLevel: "warn" });
              const state = await seedDataset(db, cfg);
              return await runW1(db, cfg, state);
            } finally {
              if (db) await db.shutdown();
            }
          },
        },
        { id: "W4", run: async (): Promise<ScenarioResult> => runW4(cfg) },
        { id: "B1", run: async (): Promise<ScenarioResult> => runB1(cfg) },
        { id: "B2", run: async (): Promise<ScenarioResult> => runB2(cfg) },
        { id: "B3", run: async (): Promise<ScenarioResult> => runB3(cfg) },
        { id: "B4", run: async (): Promise<ScenarioResult> => runB4(cfg) },
        { id: "B5", run: async (): Promise<ScenarioResult> => runB5(cfg) },
        { id: "B6", run: async (): Promise<ScenarioResult> => runB6(cfg) },
      ];
      const knownIds = new Set(runners.map((runner) => runner.id));
      for (const requestedId of browserScenarioSelection) {
        if (!knownIds.has(requestedId)) {
          throw new Error(`Unknown browser benchmark scenario id '${requestedId}'`);
        }
      }

      const selectedRunners =
        browserScenarioSelection.size === 0
          ? runners
          : runners.filter((runner) => browserScenarioSelection.has(runner.id));
      const scenarioResults: ScenarioResult[] = [];
      for (const runner of selectedRunners) {
        scenarioResults.push(await runner.run());
      }
      const resultsById = new Map(scenarioResults.map((result) => [result.scenario_id, result]));

      const report = {
        runner: "jazz-ts-browser-opfs",
        generated_at: new Date().toISOString(),
        profile: cfg.id,
        scenarios: scenarioResults,
      };

      await commands.writeRealisticBrowserReport(browserRunId, report);

      // Keeping output machine-readable makes it easy to pipe into trend tooling.
      // eslint-disable-next-line no-console
      console.log("[realistic-bench]", JSON.stringify(report));

      const w1Result = resultsById.get("W1");
      if (w1Result) {
        expect(w1Result.total_operations).toBeGreaterThan(0);
        expect(w1Result.throughput_ops_per_sec).toBeGreaterThan(0);
      }

      const w4Result = resultsById.get("W4");
      if (w4Result) {
        expect(w4Result.operation_summaries.cold_reopen.count).toBeGreaterThan(0);
      }

      const b1Result = resultsById.get("B1");
      if (b1Result) {
        expect(b1Result.operation_summaries.insert_sync.count).toBeGreaterThan(0);
      }

      const b2Result = resultsById.get("B2");
      if (b2Result) {
        expect(b2Result.total_operations).toBeGreaterThan(0);
      }

      const b3Result = resultsById.get("B3");
      if (b3Result) {
        expect(b3Result.operation_summaries.cold_reopen_query.count).toBeGreaterThan(0);
      }

      const b4Result = resultsById.get("B4");
      if (b4Result) {
        expect(b4Result.operation_summaries.fanout_delivery.count).toBeGreaterThan(0);
      }

      const b5Result = resultsById.get("B5");
      if (b5Result) {
        expect(b5Result.operation_summaries.permission_reads.count).toBeGreaterThan(0);
        expect(Number(b5Result.extra.allowed_documents_visible)).toBeGreaterThan(0);
        expect(Number(b5Result.extra.denied_documents_seeded)).toBeGreaterThan(0);
        expect(Number(b5Result.extra.denied_documents_visible)).toBe(0);
        if (Number(b5Result.extra.allowed_update_candidates) > 0) {
          expect(Number(b5Result.extra.allowed_updates_succeeded)).toBeGreaterThan(0);
        }
        expect(Number(b5Result.extra.denied_updates_rejected)).toBeGreaterThan(0);
      }

      const b6Result = resultsById.get("B6");
      if (b6Result) {
        expect(b6Result.operation_summaries.hotspot_update_sync.count).toBeGreaterThan(0);
      }
    } finally {
      restoreLogs();
    }
  }, 420_000);
});

function elevateBenchLogLevel(): () => void {
  const original = {
    log: console.log,
    info: console.info,
    debug: console.debug,
    trace: console.trace,
  };

  const allow = (args: unknown[]) =>
    typeof args[0] === "string" &&
    (args[0].startsWith("[realistic-bench]") ||
      args[0].startsWith("[realistic-progress]") ||
      args[0].startsWith("[jazz-server]"));

  console.log = (...args: unknown[]) => {
    if (allow(args)) original.log(...args);
  };
  console.info = (...args: unknown[]) => {
    if (allow(args)) original.info(...args);
  };
  console.debug = () => {};
  console.trace = () => {};

  return () => {
    console.log = original.log;
    console.info = original.info;
    console.debug = original.debug;
    console.trace = original.trace;
  };
}
