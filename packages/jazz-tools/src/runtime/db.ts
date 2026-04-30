/**
 * High-level database class for typed queries and mutations.
 *
 * Connects QueryBuilder to JazzClient for actual query execution.
 * Handles query translation, execution, and result transformation.
 *
 * Key design:
 * - createDb() is async (pre-loads WASM module)
 * - insert/update/delete are sync (local-first immediate writes, no durability wait)
 * - all/one are async (need storage I/O for queries)
 */

import type { WasmSchema, WasmRow, StorageDriver } from "../drivers/types.js";
import { normalizeRuntimeSchema, serializeRuntimeSchema } from "../drivers/schema-wire.js";
import type { RuntimeSourcesConfig, Session } from "./context.js";
import {
  DirectBatch as RuntimeDirectBatch,
  WriteResult,
  JazzClient,
  type LocalBatchRecord,
  type MutationErrorEvent,
  loadWasmModule,
  Transaction as RuntimeTransaction,
  WriteHandle,
  type CreateOptions,
  type UpdateOptions,
  type UpsertOptions,
  type WasmModule,
  type DurabilityTier,
  type PermissionDecision,
  type QueryExecutionOptions,
  type QueryPropagation,
  type QueryVisibility,
  resolveEffectiveQueryExecutionOptions,
  runInBatch,
} from "./client.js";
import { WorkerBridge, type PeerSyncBatch, type WorkerBridgeOptions } from "./worker-bridge.js";
import type { AuthFailureReason } from "./sync-transport.js";
import { translateQuery } from "./query-adapter.js";
import { transformRow, transformRows } from "./row-transformer.js";
import { toInsertRecord, toUpdateRecord } from "./value-converter.js";
import { SubscriptionManager, type SubscriptionDelta } from "./subscription-manager.js";
import { createAuthStateStore, type AuthState, type AuthStateStoreOptions } from "./auth-state.js";
import { resolveClientSessionSync, ANONYMOUS_JWT_ISSUER } from "./client-session.js";
import {
  createConventionalFileStorage,
  type ConventionalFileApp,
  type FileReadOptions,
  type FileWriteOptions,
} from "./file-storage.js";
import { analyzeRelations } from "../codegen/relation-analyzer.js";
import { TabLeaderElection, type LeaderRole, type LeaderSnapshot } from "./tab-leader-election.js";
import type { WorkerLifecycleEvent } from "../worker/worker-protocol.js";
import { normalizeBuiltQuery, type BuiltRelation } from "./query-builder-shape.js";
import {
  appendWorkerRuntimeWasmUrl,
  resolveRuntimeConfigSyncInitInput,
  resolveWorkerBootstrapWasmUrl,
  resolveRuntimeConfigWorkerUrl,
} from "./runtime-config.js";

type WasmLogLevel = "error" | "warn" | "info" | "debug" | "trace";
const DEFAULT_WASM_LOG_LEVEL: WasmLogLevel = "warn";
const STORAGE_RESET_REQUEST_RETRY_MS = 200;
const STORAGE_RESET_REQUEST_TIMEOUT_MS = 5_000;
const STORAGE_RESET_DISCOVERY_WINDOW_MS = 600;
const STORAGE_RESET_ACK_QUIET_MS = 150;

function setGlobalWasmLogLevel(level?: WasmLogLevel): void {
  (globalThis as any).__JAZZ_WASM_LOG_LEVEL = level ?? DEFAULT_WASM_LOG_LEVEL;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function createOperationId(prefix: string): string {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  if (cryptoObj && typeof cryptoObj.randomUUID === "function") {
    return `${prefix}-${cryptoObj.randomUUID()}`;
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function toError(error: unknown, fallbackMessage: string): Error {
  return error instanceof Error ? error : new Error(error ? String(error) : fallbackMessage);
}

/**
 * Configuration for creating a Db instance.
 */
export interface DbConfig {
  /** Application identifier (used for isolation) */
  appId: string;
  /** Storage driver mode (defaults to persistent). */
  driver?: StorageDriver;
  /** Optional server URL for sync */
  serverUrl?: string;
  /** Optional runtime source overrides for WASM and worker loading. */
  runtimeSources?: RuntimeSourcesConfig;
  /** Environment (e.g., "dev", "prod") */
  env?: string;
  /** User branch name (default: "main") */
  userBranch?: string;
  /** JWT token for server authentication */
  jwtToken?: string;
  /** Mirrored session for local permission evaluation when sync auth uses cookies. */
  cookieSession?: Session;
  /** Admin secret for catalogue sync */
  adminSecret?: string;
  /** Database name for OPFS persistence (browser only, default: appId) */
  dbName?: string;
  /** Optional WASM tracing level for benchmark/debug scenarios (default: "warn"). */
  logLevel?: WasmLogLevel;
  /** Enable runtime tracing for DevTools-only diagnostics. */
  devMode?: boolean;
  /** Local-first auth via a local seed. Mutually exclusive with jwtToken. */
  secret?: string;
}

function resolveStorageDriver(driver?: StorageDriver): StorageDriver {
  return driver ?? { type: "persistent" };
}

function shouldBypassLocalPolicies(config: DbConfig): boolean {
  return !!config.adminSecret;
}

function stripSchemaPolicies(schema: WasmSchema): WasmSchema {
  return Object.fromEntries(
    Object.entries(schema).map(([tableName, tableSchema]) => [
      tableName,
      {
        ...tableSchema,
        policies: undefined,
      },
    ]),
  ) as WasmSchema;
}

function trimOptionalString(value?: string | null): string | null {
  if (typeof value !== "string") {
    return null;
  }

  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

/** @internal Derive the default browser persistence namespace for this Db config. */
export function resolveDefaultPersistentDbName(config: DbConfig): string {
  const driver = resolveStorageDriver(config.driver);
  const explicitDbName = trimOptionalString(
    (driver.type === "persistent" ? driver.dbName : undefined) ?? config.dbName,
  );
  if (explicitDbName) {
    return explicitDbName;
  }

  const session = resolveClientSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
  });

  if (!session?.user_id || session.authMode === "anonymous") {
    return config.appId;
  }

  return `${config.appId}::${encodeURIComponent(session.user_id)}`;
}

/**
 * Interface that QueryBuilder classes implement.
 * Generated builders expose these internal properties for Db to use.
 */
export interface QueryBuilder<T> {
  /** Table name for this query */
  readonly _table: string;
  /** Schema reference for translation and transformation */
  readonly _schema: WasmSchema;
  /** Optional TypeScript-only per-column transforms carried by typed query handles. */
  readonly _columnTransforms?: ColumnTransformMap;
  /** Build and return the query as JSON */
  _build(): string;
  /** @internal Phantom brand — enables TypeScript to infer T from usage */
  readonly _rowType: T;
}

export type QueryOptions = QueryExecutionOptions;

function ordinaryDbQueryOptions(options?: QueryOptions): QueryOptions {
  return { localUpdates: "deferred", ...options };
}

export interface ActiveQuerySubscriptionTrace {
  id: string;
  query: string;
  table: string;
  branches: string[];
  tier: DurabilityTier;
  propagation: QueryPropagation;
  createdAt: string;
  stack?: string;
}

export interface LogoutOptions {
  wipeData?: boolean;
}

type ActiveQuerySubscriptionTraceListener = (
  traces: readonly ActiveQuerySubscriptionTrace[],
) => void;

type StoredActiveQuerySubscriptionTrace = ActiveQuerySubscriptionTrace & {
  visibility: QueryVisibility;
};

type RuntimeQueryTracePayload = {
  table: string;
  branches: string[];
};

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T | PromiseLike<T>) => void;
  reject: (reason?: unknown) => void;
};

type StorageResetContext = {
  requestId: string;
  initiatedBySelf: boolean;
  coordinatorTabId: string | null;
  begun: boolean;
  completed: boolean;
  preparePromise: Promise<string> | null;
  completion: Deferred<void>;
};

type StorageResetCoordinatorState = {
  requestId: string;
  startedAtMs: number;
  lastAckAtMs: number;
  ackedNamespacesByTabId: Map<string, string>;
  runPromise: Promise<void> | null;
};

function createDeferred<T>(): Deferred<T> {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function trimSubscriptionTraceStack(stack: string | undefined): string | undefined {
  if (!stack) {
    return stack;
  }

  const lines = stack.split("\n");
  if (lines.length <= 1) {
    return stack;
  }

  const isInternalFrame = (line: string): boolean => {
    return (
      line.includes("Db.registerActiveQuerySubscriptionTrace") ||
      line.includes("Db.subscribeAll") ||
      line.includes("SubscriptionsOrchestrator.ensureEntryForKey") ||
      line.includes("SubscriptionsOrchestrator.getCacheEntry") ||
      line.includes("/node_modules/") ||
      line.includes("react-dom") ||
      line.includes("react_stack_bottom_frame")
    );
  };

  const firstOriginIndex = lines.findIndex((line, index) => index > 0 && !isInternalFrame(line));
  if (firstOriginIndex <= 0) {
    return stack;
  }

  return [lines[0], ...lines.slice(firstOriginIndex)].join("\n");
}

function cloneActiveQuerySubscriptionTrace(
  trace: ActiveQuerySubscriptionTrace,
): ActiveQuerySubscriptionTrace {
  return {
    ...trace,
    branches: [...trace.branches],
  };
}

function resolveHopOutputTable(
  schema: WasmSchema,
  startTable: string,
  hops: readonly string[],
): string {
  if (hops.length === 0) {
    return startTable;
  }
  const relations = analyzeRelations(schema);
  let currentTable = startTable;
  for (const hopName of hops) {
    const candidates = relations.get(currentTable) ?? [];
    const relation = candidates.find((candidate) => candidate.name === hopName);
    if (!relation) {
      throw new Error(`Unknown relation "${hopName}" on table "${currentTable}"`);
    }
    currentTable = relation.toTable;
  }
  return currentTable;
}

function resolveBuiltRelationOutputTable(schema: WasmSchema, relation: BuiltRelation): string {
  if (relation.union) {
    const first = relation.union.inputs[0];
    if (!first) {
      throw new Error("union(...) requires at least one relation.");
    }
    const firstTable = resolveBuiltRelationOutputTable(schema, first);
    for (const input of relation.union.inputs.slice(1)) {
      const inputTable = resolveBuiltRelationOutputTable(schema, input);
      if (inputTable !== firstTable) {
        throw new Error("union(...) requires all relations to output the same table.");
      }
    }
    return firstTable;
  }

  const seedTable = relation.gather?.seed
    ? resolveBuiltRelationOutputTable(schema, relation.gather.seed)
    : relation.table;
  if (!seedTable) {
    throw new Error("gather(...) seed relation is missing table metadata.");
  }
  const hops = relation.hops ?? [];
  return hops.length > 0 ? resolveHopOutputTable(schema, seedTable, hops) : seedTable;
}

function resolveBuiltQueryOutputTable(
  schema: WasmSchema,
  builtQuery: ReturnType<typeof normalizeBuiltQuery>,
): string {
  if (builtQuery.gather?.seed) {
    const gatherTable = resolveBuiltRelationOutputTable(schema, builtQuery.gather.seed);
    return builtQuery.hops.length > 0
      ? resolveHopOutputTable(schema, gatherTable, builtQuery.hops)
      : gatherTable;
  }

  return builtQuery.hops.length > 0
    ? resolveHopOutputTable(schema, builtQuery.table, builtQuery.hops)
    : builtQuery.table;
}

function resolveSchemaWithTable(
  preferredSchema: WasmSchema,
  fallbackSchema: WasmSchema | (() => WasmSchema),
  tableName: string,
): WasmSchema {
  if (preferredSchema[tableName]) {
    return preferredSchema;
  }

  return typeof fallbackSchema === "function" ? fallbackSchema() : fallbackSchema;
}

function createRuntimeSchemaResolver(getRuntimeSchema: () => WasmSchema): {
  get: () => WasmSchema;
  peek: () => WasmSchema | undefined;
} {
  let cachedRuntimeSchema: WasmSchema | undefined;

  return {
    get: () => {
      if (!cachedRuntimeSchema) {
        cachedRuntimeSchema = getRuntimeSchema();
      }
      return cachedRuntimeSchema;
    },
    peek: () => cachedRuntimeSchema,
  };
}

function assertTableBelongsToClient<T, Init>(
  table: TableProxy<T, Init>,
  expectedClient: JazzClient,
  resolveClient: (schema: WasmSchema) => JazzClient,
  operation: string,
): void {
  if (resolveClient(table._schema) === expectedClient) {
    return;
  }
  throw new Error(
    `${operation} is bound to the client chosen by the first table used and cannot be used with table "${table._table}" from a different schema/client.`,
  );
}

/**
 * Interface for table proxies used with mutations.
 * Generated table constants implement this interface.
 *
 * @typeParam T - The row type (e.g., `{ id: string; title: string; done: boolean }`)
 * @typeParam Init - The init type for inserts (e.g., `{ title: string; done: boolean }`)
 */
export interface TableProxy<T, Init> {
  /** Table name */
  readonly _table: string;
  /** Schema reference */
  readonly _schema: WasmSchema;
  /** Optional TypeScript-only per-column transforms carried by typed table handles. */
  readonly _columnTransforms?: ColumnTransformMap;
  /** @internal Phantom brand — enables TypeScript to infer T from usage */
  readonly _rowType: T;
  /** @internal Phantom brand — enables TypeScript to infer Init from usage */
  readonly _initType: Init;
}

export interface ColumnTransform {
  from(value: unknown): unknown;
  to(value: unknown): unknown;
}

export type ColumnTransformMap = Record<string, ColumnTransform>;

type DbTransactionBinding = {
  client: JazzClient;
  runtimeTransaction: RuntimeTransaction;
};

type DbDirectBatchBinding = {
  client: JazzClient;
  runtimeBatch: RuntimeDirectBatch;
};

const dbTransactionBindings = new WeakMap<DbTransaction, DbTransactionBinding>();
const dbDirectBatchBindings = new WeakMap<DbDirectBatch, DbDirectBatchBinding>();

function getDbTransactionBinding(
  transaction: DbTransaction,
  operation: string,
): DbTransactionBinding {
  const binding = dbTransactionBindings.get(transaction);
  if (!binding) {
    throw new Error(`DbTransaction.${operation}() requires at least one table operation first`);
  }
  return binding;
}

function getDbDirectBatchBinding(batch: DbDirectBatch, operation: string): DbDirectBatchBinding {
  const binding = dbDirectBatchBindings.get(batch);
  if (!binding) {
    throw new Error(`DbDirectBatch.${operation}() requires at least one table operation first`);
  }
  return binding;
}

function transformOutputRow<T>(
  source: { readonly _columnTransforms?: ColumnTransformMap },
  row: unknown,
): T {
  return transformOutputColumns(source, row) as T;
}

function transformOutputColumns(
  source: { readonly _columnTransforms?: ColumnTransformMap },
  row: unknown,
): unknown {
  if (!source._columnTransforms || typeof row !== "object" || row === null) {
    return row;
  }

  const transformed = { ...(row as Record<string, unknown>) };
  for (const [column, transform] of Object.entries(source._columnTransforms)) {
    if (column in transformed) {
      transformed[column] = transform.from(transformed[column]);
    }
  }
  return transformed;
}

function transformInsertInput(
  table: TableProxy<unknown, unknown>,
  data: unknown,
): Record<string, unknown> {
  return transformInputColumns(table, data as Record<string, unknown>);
}

function transformUpdateInput(
  table: TableProxy<unknown, unknown>,
  data: unknown,
): Record<string, unknown> {
  return transformInputColumns(table, data as Record<string, unknown>);
}

function transformInputColumns(
  table: TableProxy<unknown, unknown>,
  data: Record<string, unknown>,
): Record<string, unknown> {
  if (!table._columnTransforms) {
    return data;
  }

  const transformed = { ...data };
  for (const [column, transform] of Object.entries(table._columnTransforms)) {
    if (column in transformed) {
      transformed[column] = transform.to(transformed[column]);
    }
  }
  return transformed;
}

function backendScopedAuthState(session?: Session | null): AuthState {
  return {
    authMode: session?.authMode ?? "external",
    session: session ?? null,
  };
}

/**
 * Transactions group a set of writes that should settle together after an authority validates them.
 *
 * Data read and written through this transaction is scoped to it, and will only be
 * globally visible once it's committed using {@link DbTransaction.commit} and
 * accepted by the authority.
 */
export class DbTransaction {
  private committed = false;

  constructor(
    private readonly resolveClient: (schema: WasmSchema) => JazzClient,
    private readonly beginRuntimeTransaction: (client: JazzClient) => RuntimeTransaction,
  ) {}

  private ensureActive(): void {
    if (this.committed) {
      const batchId = dbTransactionBindings.get(this)?.runtimeTransaction.batchId() ?? "unbound";
      throw new Error(`Transaction ${batchId} is already committed`);
    }
  }

  private resolveInputSchema<T, Init>(table: TableProxy<T, Init>): WasmSchema {
    const { client } = this.bindTable(table, "DbTransaction");
    return resolveSchemaWithTable(
      table._schema,
      normalizeRuntimeSchema(client.getSchema()),
      table._table,
    );
  }

  private bindTable<T, Init>(table: TableProxy<T, Init>, operation: string): DbTransactionBinding {
    const existingBinding = dbTransactionBindings.get(this);
    if (existingBinding) {
      assertTableBelongsToClient(table, existingBinding.client, this.resolveClient, operation);
      return existingBinding;
    }

    const client = this.resolveClient(table._schema);
    const runtimeTransaction = this.beginRuntimeTransaction(client);
    const binding = { client, runtimeTransaction };
    dbTransactionBindings.set(this, binding);
    return binding;
  }

  private bindQuery<T>(query: QueryBuilder<T>): DbTransactionBinding {
    return this.bindTable(query as unknown as TableProxy<T, never>, "DbTransaction");
  }

  private requireRuntimeTransaction(operation: string): RuntimeTransaction {
    return getDbTransactionBinding(this, operation).runtimeTransaction;
  }

  batchId(): string {
    return this.requireRuntimeTransaction("batchId").batchId();
  }

  /**
   * Commit the transaction. Data will be globally visible once it's accepted by the authority.
   */
  commit(): WriteHandle {
    const runtimeTransaction = this.requireRuntimeTransaction("commit");
    this.committed = true;
    return runtimeTransaction.commit();
  }

  /**
   * Insert a new row into a table.
   *
   * The insert is scoped to this transaction, and will only be globally visible
   * once it's committed with {@link DbTransaction.commit}.
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init): T {
    this.ensureActive();
    const transformedData = transformInsertInput(table, data);
    const values = toInsertRecord(transformedData, this.resolveInputSchema(table), table._table);
    const row = this.requireRuntimeTransaction("insert").create(table._table, values);
    return transformOutputRow(table, transformRow(row, table._schema, table._table));
  }

  /**
   * Update an existing row in a table.
   *
   * The update is scoped to this transaction, and will only be globally visible
   * once it's committed with {@link DbTransaction.commit}.
   */
  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void {
    this.ensureActive();
    const transformedData = transformUpdateInput(table, data);
    const updates = toUpdateRecord(transformedData, this.resolveInputSchema(table), table._table);
    this.requireRuntimeTransaction("update").update(id, updates);
  }

  /**
   * Delete an existing row from a table.
   *
   * The delete is scoped to this transaction, and will only be globally visible
   * once it's committed with {@link DbTransaction.commit}.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string): void {
    this.ensureActive();
    const { runtimeTransaction } = this.bindTable(table, "DbTransaction");
    runtimeTransaction.delete(id);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * Read data is scoped to this transaction.
   */
  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    this.ensureActive();
    const { client, runtimeTransaction } = this.bindQuery(query);
    const runtimeSchema = normalizeRuntimeSchema(client.getSchema());
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(query._schema, runtimeSchema, builtQuery.table);
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema, outputTable);
    const rows = await runtimeTransaction.query(
      translateQuery(builderJson, planningSchema),
      options,
    );
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const transformedRows = transformRows(
      rows,
      outputSchema,
      outputTable,
      outputIncludes,
      builtQuery.select,
    );
    return transformedRows.map((row) =>
      transformOutputRow(outputTable === builtQuery.table ? query : {}, row),
    );
  }

  /**
   * Execute a query and return the first matching row, or null.
   *
   * Read data is scoped to this transaction.
   */
  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const results = await this.all(query, options);
    return results[0] ?? null;
  }

  localBatchRecord(batchId = this.batchId()): LocalBatchRecord | null {
    return this.requireRuntimeTransaction("localBatchRecord").localBatchRecord(batchId);
  }

  localBatchRecords(): LocalBatchRecord[] {
    return this.requireRuntimeTransaction("localBatchRecords").localBatchRecords();
  }

  acknowledgeRejectedBatch(batchId = this.batchId()): boolean {
    return this.requireRuntimeTransaction("acknowledgeRejectedBatch").acknowledgeRejectedBatch(
      batchId,
    );
  }
}

/**
 * Transaction object available inside {@link Db.transaction}'s callback.
 */
export type DbTransactionScope = Omit<DbTransaction, "commit">;

/**
 * Direct batches group a set of writes that should settle immediately, without an authority,
 * while still being part of the same batch.
 *
 * Data written through this direct batch is globally visible immediately.
 */
export class DbDirectBatch {
  private committedHandle: WriteHandle | null = null;

  constructor(
    private readonly resolveClient: (schema: WasmSchema) => JazzClient,
    private readonly beginRuntimeBatch: (client: JazzClient) => RuntimeDirectBatch,
  ) {}

  private resolveInputSchema<T, Init>(table: TableProxy<T, Init>): WasmSchema {
    const { client } = this.bindTable(table, "DbDirectBatch");
    return resolveSchemaWithTable(
      table._schema,
      normalizeRuntimeSchema(client.getSchema()),
      table._table,
    );
  }

  private bindTable<T, Init>(table: TableProxy<T, Init>, operation: string): DbDirectBatchBinding {
    const existingBinding = dbDirectBatchBindings.get(this);
    if (existingBinding) {
      assertTableBelongsToClient(table, existingBinding.client, this.resolveClient, operation);
      return existingBinding;
    }

    const client = this.resolveClient(table._schema);
    const runtimeBatch = this.beginRuntimeBatch(client);
    const binding = { client, runtimeBatch };
    dbDirectBatchBindings.set(this, binding);
    return binding;
  }

  private requireRuntimeBatch(operation: string): RuntimeDirectBatch {
    return getDbDirectBatchBinding(this, operation).runtimeBatch;
  }

  batchId(): string {
    return this.requireRuntimeBatch("batchId").batchId();
  }

  private ensureActive(): void {
    if (this.committedHandle) {
      const batchId = dbDirectBatchBindings.get(this)?.runtimeBatch.batchId() ?? "unbound";
      throw new Error(`Direct batch ${batchId} is already committed`);
    }
  }

  /**
   * Commit the direct batch. Data is visible optimistically immediately and can
   * be waited on through the returned handle.
   */
  commit(): WriteHandle {
    if (this.committedHandle) {
      return this.committedHandle;
    }
    const handle = this.requireRuntimeBatch("commit").commit();
    this.committedHandle = handle;
    return handle;
  }

  insert<T, Init>(table: TableProxy<T, Init>, data: Init): T {
    this.ensureActive();
    const transformedData = transformInsertInput(table, data);
    const values = toInsertRecord(transformedData, this.resolveInputSchema(table), table._table);
    const row = this.requireRuntimeBatch("insert").create(table._table, values);
    return transformOutputRow(table, transformRow(row, table._schema, table._table));
  }

  update<T, Init>(table: TableProxy<T, Init>, id: string, data: Partial<Init>): void {
    this.ensureActive();
    const transformedData = transformUpdateInput(table, data);
    const updates = toUpdateRecord(transformedData, this.resolveInputSchema(table), table._table);
    this.requireRuntimeBatch("update").update(id, updates);
  }

  delete<T, Init>(table: TableProxy<T, Init>, id: string): void {
    this.ensureActive();
    const { runtimeBatch } = this.bindTable(table, "DbDirectBatch");
    runtimeBatch.delete(id);
  }

  localBatchRecord(batchId = this.batchId()): LocalBatchRecord | null {
    return this.requireRuntimeBatch("localBatchRecord").localBatchRecord(batchId);
  }

  localBatchRecords(): LocalBatchRecord[] {
    return this.requireRuntimeBatch("localBatchRecords").localBatchRecords();
  }

  acknowledgeRejectedBatch(batchId = this.batchId()): boolean {
    return this.requireRuntimeBatch("acknowledgeRejectedBatch").acknowledgeRejectedBatch(batchId);
  }
}

/**
 * Batch object available inside {@link Db.batch}'s callback.
 */
export type DbBatchScope = Omit<DbDirectBatch, "commit">;

interface BroadcastChannelLike {
  postMessage(data: unknown): void;
  addEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  removeEventListener(type: "message", listener: (event: MessageEvent) => void): void;
  close(): void;
}

interface FollowerSyncMessage {
  type: "follower-sync";
  fromTabId: string;
  toLeaderTabId: string;
  term: number;
  payload: Uint8Array[];
}

interface LeaderSyncMessage {
  type: "leader-sync";
  fromLeaderTabId: string;
  toTabId: string;
  term: number;
  payload: Uint8Array[];
}

interface FollowerCloseMessage {
  type: "follower-close";
  fromTabId: string;
  toLeaderTabId: string;
  term: number;
}

interface StorageResetRequestMessage {
  type: "storage-reset-request";
  requestId: string;
  fromTabId: string;
  toLeaderTabId: string | null;
  term: number;
}

interface StorageResetBeginMessage {
  type: "storage-reset-begin";
  requestId: string;
  coordinatorTabId: string;
  term: number;
}

interface StorageResetAckMessage {
  type: "storage-reset-ack";
  requestId: string;
  fromTabId: string;
  namespace: string;
}

interface StorageResetFinishedMessage {
  type: "storage-reset-finished";
  requestId: string;
  success: boolean;
  errorMessage?: string;
}

type TabSyncMessage =
  | FollowerSyncMessage
  | LeaderSyncMessage
  | FollowerCloseMessage
  | StorageResetRequestMessage
  | StorageResetBeginMessage
  | StorageResetAckMessage
  | StorageResetFinishedMessage;

function resolveBroadcastChannelCtor(): (new (name: string) => BroadcastChannelLike) | null {
  const ctor = (globalThis as { BroadcastChannel?: unknown }).BroadcastChannel;
  if (typeof ctor !== "function") return null;
  return ctor as new (name: string) => BroadcastChannelLike;
}

function isBinaryPayloadArray(value: unknown): value is Uint8Array[] {
  return Array.isArray(value) && value.every((entry) => entry instanceof Uint8Array);
}

function isTabSyncMessage(value: unknown): value is TabSyncMessage {
  if (typeof value !== "object" || value === null) return false;
  const message = value as Record<string, unknown>;

  if (message.type === "follower-sync") {
    return (
      typeof message.fromTabId === "string" &&
      typeof message.toLeaderTabId === "string" &&
      typeof message.term === "number" &&
      isBinaryPayloadArray(message.payload)
    );
  }

  if (message.type === "leader-sync") {
    return (
      typeof message.fromLeaderTabId === "string" &&
      typeof message.toTabId === "string" &&
      typeof message.term === "number" &&
      isBinaryPayloadArray(message.payload)
    );
  }

  if (message.type === "follower-close") {
    return (
      typeof message.fromTabId === "string" &&
      typeof message.toLeaderTabId === "string" &&
      typeof message.term === "number"
    );
  }

  if (message.type === "storage-reset-request") {
    return (
      typeof message.requestId === "string" &&
      typeof message.fromTabId === "string" &&
      (typeof message.toLeaderTabId === "string" || message.toLeaderTabId === null) &&
      typeof message.term === "number"
    );
  }

  if (message.type === "storage-reset-begin") {
    return (
      typeof message.requestId === "string" &&
      typeof message.coordinatorTabId === "string" &&
      typeof message.term === "number"
    );
  }

  if (message.type === "storage-reset-ack") {
    return (
      typeof message.requestId === "string" &&
      typeof message.fromTabId === "string" &&
      typeof message.namespace === "string"
    );
  }

  if (message.type === "storage-reset-finished") {
    return (
      typeof message.requestId === "string" &&
      typeof message.success === "boolean" &&
      (typeof message.errorMessage === "string" || message.errorMessage === undefined)
    );
  }

  return false;
}

function isLeaderDebugEnabled(): boolean {
  const globalFlag = (globalThis as { __JAZZ_LEADER_DEBUG__?: unknown }).__JAZZ_LEADER_DEBUG__;
  if (globalFlag === true) return true;

  try {
    if (typeof localStorage !== "undefined") {
      return localStorage.getItem("jazz:leader-debug") === "1";
    }
  } catch {
    // Ignore storage access errors (e.g. privacy mode / unavailable storage).
  }

  return false;
}

/**
 * High-level database interface for typed queries and mutations.
 *
 * Usage:
 * ```typescript
 * const db = await createDb({ appId: "my-app", driver });
 *
 * // Mutations
 * const { value: inserted } = db.insert(app.todos, { title: "Buy milk", done: false });
 * db.update(app.todos, inserted.id, { done: true });
 * db.delete(app.todos, inserted.id);
 *
 * // Async queries (need storage I/O)
 * const todos = await db.all(app.todos.where({ done: false }));
 * const todo = await db.one(app.todos.where({ id: inserted.id }));
 *
 * // Subscriptions
 * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
 *   console.log("All todos:", delta.all);
 *   console.log("Changes:", delta.delta);
 * });
 * ```
 */
export class Db {
  private clients = new Map<string, JazzClient>();
  private config: DbConfig;
  private wasmModule: WasmModule | null;
  private readonly authStateStore;
  private workerBridge: WorkerBridge | null = null;
  private worker: Worker | null = null;
  private bridgeReady: Promise<void> | null = null;
  private primaryDbName: string | null = null;
  private workerDbName: string | null = null;
  private leaderElection: TabLeaderElection | null = null;
  private leaderElectionUnsubscribe: (() => void) | null = null;
  private tabRole: LeaderRole = "follower";
  private tabId: string | null = null;
  private currentLeaderTabId: string | null = null;
  private currentLeaderTerm = 0;
  private syncChannel: BroadcastChannelLike | null = null;
  private readonly leaderPeerIds = new Set<string>();
  private activeRemoteLeaderTabId: string | null = null;
  private workerReconfigure: Promise<void> = Promise.resolve();
  private activeStorageReset: StorageResetContext | null = null;
  private storageResetCoordinator: StorageResetCoordinatorState | null = null;
  private _localFirstSecret: string | null = null;
  private localFirstRefreshTimer: ReturnType<typeof setTimeout> | null = null;
  private isShuttingDown = false;
  private lifecycleHooksAttached = false;
  private readonly activeQuerySubscriptionTraces = new Map<
    string,
    StoredActiveQuerySubscriptionTrace
  >();
  private readonly activeQuerySubscriptionTraceListeners =
    new Set<ActiveQuerySubscriptionTraceListener>();
  /**
   * Listeners attached with {@link Db.onMutationError} that are notified when a write operation
   * (insert, update, delete) is rejected. Errors from all {@link Db.clients} (including those
   * added after the listeners are attached) are forwarded to all Db listeners.
   */
  private readonly mutationErrorListeners = new Set<(event: MutationErrorEvent) => void>();
  /**
   * Unsubscribers for {@link Db.clients}'s {@link JazzClient.onMutationError} listeners
   */
  private readonly clientMutationErrorUnsubscribers = new Map<JazzClient, () => void>();
  private nextActiveQuerySubscriptionTraceId = 1;
  private readonly onSyncChannelMessage = (event: MessageEvent): void => {
    this.handleSyncChannelMessage(event.data);
  };
  private readonly onVisibilityChange = (): void => {
    if (typeof document === "undefined") return;
    const hidden = document.visibilityState === "hidden";
    this.sendLifecycleHint(hidden ? "visibility-hidden" : "visibility-visible");
  };
  private readonly onPageHide = (): void => {
    this.sendLifecycleHint("pagehide");
  };
  private readonly onPageFreeze = (): void => {
    this.sendLifecycleHint("freeze");
  };
  private readonly onPageResume = (): void => {
    this.sendLifecycleHint("resume");
  };

  /**
   * Protected constructor - use createDb() in regular app code.
   */
  protected constructor(
    config: DbConfig,
    wasmModule: WasmModule | null,
    authStateOptions?: AuthStateStoreOptions,
  ) {
    this.config = config;
    this.wasmModule = wasmModule;
    this.authStateStore = createAuthStateStore(config, authStateOptions);
  }

  /** @internal Store the seed used for local-first auth and schedule token refresh. */
  initLocalFirstAuth(seed: string, ttlSeconds: number): void {
    this._localFirstSecret = seed;
    this.scheduleLocalFirstRefresh(ttlSeconds);
  }

  private scheduleLocalFirstRefresh(ttlSeconds: number): void {
    if (this.localFirstRefreshTimer) {
      clearTimeout(this.localFirstRefreshTimer);
    }
    // Refresh at 80% of TTL
    const refreshMs = ttlSeconds * 800; // 80% of TTL in ms
    this.localFirstRefreshTimer = setTimeout(() => {
      this.refreshLocalFirstToken();
    }, refreshMs);
  }

  private refreshLocalFirstToken(): void {
    if (!this._localFirstSecret || this.isShuttingDown) return;

    try {
      const wasmModule = this.wasmModule;
      if (!wasmModule) return;

      const ttlSeconds = 3600;
      const nowSeconds = BigInt(Math.floor(Date.now() / 1000));
      const newToken = wasmModule.WasmRuntime.mintJazzSelfSignedToken(
        this._localFirstSecret,
        "urn:jazz:local-first",
        this.config.appId,
        BigInt(ttlSeconds),
        nowSeconds,
      );
      this.updateAuthToken(newToken);
      this.scheduleLocalFirstRefresh(ttlSeconds);
    } catch (e) {
      console.error("Failed to refresh local-first token:", e);
    }
  }

  protected markUnauthenticated(reason: AuthFailureReason): void {
    this.authStateStore.markUnauthenticated(reason);
  }

  protected applyAuthUpdate(token: string | null): boolean {
    const jwtToken = token ?? undefined;
    const previousToken = this.config.jwtToken;
    const previousState = this.authStateStore.getState();
    const nextState = this.authStateStore.applyJwtToken(jwtToken);
    const tokenChanged = previousToken !== jwtToken;

    if (!tokenChanged && nextState === previousState) {
      return false;
    }

    this.config.jwtToken = jwtToken;

    for (const client of this.clients.values()) {
      client.updateAuthToken(jwtToken);
    }

    this.workerBridge?.updateAuth({
      jwtToken,
    });

    return true;
  }

  protected applyCookieSessionUpdate(session: Session | null): boolean {
    const cookieSession = session ?? undefined;
    const previousSession = this.config.cookieSession;
    const previousState = this.authStateStore.getState();
    const nextState = this.authStateStore.applyCookieSession(cookieSession);
    const sessionChanged = JSON.stringify(previousSession) !== JSON.stringify(cookieSession);

    if (!sessionChanged && nextState === previousState) {
      return false;
    }

    this.config.cookieSession = cookieSession;

    for (const client of this.clients.values()) {
      client.updateCookieSession(cookieSession);
    }

    this.workerBridge?.updateAuth({
      jwtToken: this.config.jwtToken,
    });

    return true;
  }

  /**
   * Create a Db instance with pre-loaded WASM module.
   * @internal Use createDb() instead.
   */
  static async create(config: DbConfig): Promise<Db> {
    const wasmModule = await loadWasmModule(config.runtimeSources);
    return new Db(config, wasmModule);
  }

  /**
   * Create a Db instance backed by a dedicated worker with OPFS persistence.
   *
   * The main thread runs an in-memory WASM runtime.
   * The worker runs a persistent WASM runtime (OPFS).
   * WorkerBridge wires them together via postMessage.
   *
   * @internal Use createDb() instead — it auto-detects browser.
   */
  static async createWithWorker(config: DbConfig): Promise<Db> {
    const wasmModule = await loadWasmModule(config.runtimeSources);
    const db = new Db(config, wasmModule);
    const persistentDriver = resolveStorageDriver(config.driver);
    if (persistentDriver.type !== "persistent") {
      throw new Error("Worker-backed Db requires driver.type='persistent'");
    }
    db.primaryDbName = resolveDefaultPersistentDbName(config);
    db.workerDbName = db.primaryDbName;

    try {
      const election = new TabLeaderElection({
        appId: config.appId,
        dbName: db.primaryDbName,
      });
      db.leaderElection = election;
      election.start();

      let initialLeader: LeaderSnapshot | null = null;
      try {
        // Allow at least one startup election window with default heartbeat settings.
        initialLeader = await election.waitForInitialLeader(1600);
      } catch {
        // Fall back to whatever state election has reached so far.
        initialLeader = election.snapshot();
      }
      db.adoptLeaderSnapshot(initialLeader);
      db.workerDbName = Db.resolveWorkerDbNameForSnapshot(db.primaryDbName, initialLeader);
      db.logLeaderDebug("initial-election");
      db.openSyncChannel();
      db.attachLifecycleHooks();
      db.leaderElectionUnsubscribe = election.onChange((snapshot) => {
        db.onLeaderElectionChange(snapshot);
      });

      db.worker = await Db.spawnWorker(config.runtimeSources);

      return db;
    } catch (error) {
      db.closeSyncChannel();
      db.detachLifecycleHooks();
      if (db.leaderElectionUnsubscribe) {
        db.leaderElectionUnsubscribe();
        db.leaderElectionUnsubscribe = null;
      }
      if (db.leaderElection) {
        db.leaderElection.stop();
        db.leaderElection = null;
      }
      throw error;
    }
  }

  /**
   * Get or create a JazzClient for the given schema.
   * Synchronous because WASM module is pre-loaded.
   *
   * In worker mode, the first call per schema also initializes the
   * WorkerBridge (async). Subsequent calls are sync.
   */
  protected getClient(schema: WasmSchema): JazzClient {
    if (!this.wasmModule) {
      throw new Error("Db runtime module is not initialized for this Db implementation");
    }

    const runtimeSchema = shouldBypassLocalPolicies(this.config)
      ? stripSchemaPolicies(schema)
      : schema;

    // Use stringified schema as cache key
    const key = serializeRuntimeSchema(runtimeSchema);

    if (!this.clients.has(key)) {
      setGlobalWasmLogLevel(this.config.logLevel);

      // Create in-memory runtime (works for both direct and worker mode)
      const client = JazzClient.connectSync(
        this.wasmModule,
        {
          appId: this.config.appId,
          schema: runtimeSchema,
          driver: this.config.driver,
          // In worker mode, don't connect to server directly — worker handles it
          serverUrl: this.worker ? undefined : this.config.serverUrl,
          env: this.config.env,
          userBranch: this.config.userBranch,
          jwtToken: this.config.jwtToken,
          cookieSession: this.config.cookieSession,
          adminSecret: this.config.adminSecret,
          tier: this.worker ? undefined : "local",
          // Keep worker-bridged browser clients on local durability by default.
          // For direct (non-worker) clients connected to a server, default to edge.
          defaultDurabilityTier: this.worker
            ? undefined
            : this.config.serverUrl
              ? "edge"
              : undefined,
        },
        {
          // Worker-bridged runtimes exchange postcard payloads with peers;
          // direct browser/server routing keeps JSON payloads.
          useBinaryEncoding: this.worker !== null,
          onAuthFailure: (reason) => {
            this.markUnauthenticated(reason);
          },
        },
      );

      // In worker mode, set up the bridge for this client
      if (this.worker && !this.workerBridge) {
        this.attachWorkerBridge(key, client);
      }

      this.attachMutationErrorHandler(client);
      // Direct (non-worker) clients with a serverUrl must open their own
      // Rust transport — the worker bridge is not doing it for them.
      if (!this.worker && this.config.serverUrl) {
        client.connectTransport(this.config.serverUrl, {
          jwt_token: this.config.jwtToken,
          admin_secret: this.config.adminSecret,
        });
      }

      this.attachMutationErrorHandler(client);
      this.clients.set(key, client);
    }

    return this.clients.get(key)!;
  }

  /**
   * Attaches a mutation error handler to the given client, ensuring all listeners in
   * {@link Db.mutationErrorListeners} are notified.
   */
  private attachMutationErrorHandler(client: JazzClient): void {
    if (this.mutationErrorListeners.size === 0) {
      return;
    }
    if (this.clientMutationErrorUnsubscribers.has(client)) {
      return;
    }
    this.clientMutationErrorUnsubscribers.set(
      client,
      client.onMutationError((event) => {
        for (const listener of this.mutationErrorListeners) {
          listener(event);
        }
      }),
    );
  }
  /**
   * Wait for the worker bridge to be initialized (if in worker mode).
   * No-op if not using a worker.
   */
  protected async ensureBridgeReady(): Promise<void> {
    await this.workerReconfigure;
    if (this.bridgeReady) {
      await this.bridgeReady;
    }
  }

  protected async ensureQueryReady(options?: QueryOptions): Promise<void> {
    await this.ensureBridgeReady();
    if (!this.workerBridge || !this.config.serverUrl) {
      return;
    }
    if (!options?.tier || options.tier === "local") {
      return;
    }
    await this.workerBridge.waitForUpstreamServerConnection();
  }

  private attachWorkerBridge(schemaJson: string, client: JazzClient): void {
    if (!this.worker) {
      throw new Error("Cannot attach worker bridge without an active worker");
    }

    const bridge = new WorkerBridge(this.worker, client.getRuntime());
    this.leaderPeerIds.clear();
    bridge.onPeerSync((batch) => {
      this.handleWorkerPeerSync(batch);
    });
    this.applyBridgeRoutingForCurrentLeader(bridge, false);
    bridge.onAuthFailure((reason) => {
      this.markUnauthenticated(reason);
    });
    this.workerBridge = bridge;
    const bridgeReady = bridge
      .init(this.buildWorkerBridgeOptions(schemaJson))
      .then(() => undefined);
    bridgeReady.catch(() => undefined);
    this.bridgeReady = bridgeReady;
  }

  private buildWorkerBridgeOptions(schemaJson: string): WorkerBridgeOptions {
    const driver = resolveStorageDriver(this.config.driver);
    if (driver.type !== "persistent") {
      throw new Error("Worker bridge is only available for driver.type='persistent'");
    }

    const locationHref = typeof location !== "undefined" ? location.href : undefined;

    // Opt-in default: when a bundler plugin (e.g. `withJazz` for Next) copies
    // the wasm into the host app and advertises the URL via
    // NEXT_PUBLIC_JAZZ_WASM_URL, pick it up so the worker receives an
    // absolute URL and skips the (Turbopack-unreliable) bundler default.
    //
    // Precedence follows RuntimeSourcesConfig: any of wasmModule / wasmSource /
    // wasmUrl / baseUrl already supplied by the caller wins — we only fill in
    // when none of those is set, preserving the documented resolution order
    // for Vite/webpack/Svelte/etc. callers.
    const configRuntimeSources = this.config.runtimeSources;
    // Use the literal `process.env.NEXT_PUBLIC_JAZZ_WASM_URL` form: Next's
    // build-time replacement only rewrites that exact property access. Optional
    // chaining on `process.env` can bypass the replacement in Turbopack and
    // leave this as `undefined` in client bundles, defeating the fallback.
    const envWasmUrl =
      typeof process !== "undefined" && process.env
        ? process.env.NEXT_PUBLIC_JAZZ_WASM_URL
        : undefined;
    // Any explicit override means the caller is taking control of wasm/worker
    // resolution — don't second-guess them by injecting a Next-plugin URL.
    // `workerUrl` counts too: the spawn path at `Db.spawnWorker` already
    // resolves a wasm URL colocated with the custom worker script via
    // `appendWorkerRuntimeWasmUrl` + `readWorkerRuntimeWasmUrl`.
    const hasConfiguredSource =
      !!configRuntimeSources?.wasmUrl ||
      !!configRuntimeSources?.baseUrl ||
      !!configRuntimeSources?.workerUrl ||
      !!resolveRuntimeConfigSyncInitInput(configRuntimeSources);
    const runtimeSources =
      hasConfiguredSource || !envWasmUrl || typeof location === "undefined"
        ? configRuntimeSources
        : {
            ...configRuntimeSources,
            wasmUrl: new URL(envWasmUrl, location.href).href,
          };

    // For the static-URL spawn path (no explicit workerUrl/baseUrl), compute a
    // fallback WASM URL for non-bundled contexts where wasmModule.default() may fail.
    let fallbackWasmUrl: string | undefined;
    if (!runtimeSources?.workerUrl && !runtimeSources?.baseUrl && !runtimeSources?.wasmUrl) {
      if (!resolveRuntimeConfigSyncInitInput(runtimeSources)) {
        fallbackWasmUrl =
          resolveWorkerBootstrapWasmUrl(import.meta.url, locationHref, runtimeSources) ?? undefined;
      }
    }

    return {
      schemaJson,
      appId: this.config.appId,
      env: this.config.env ?? "dev",
      userBranch: this.config.userBranch ?? "main",
      dbName: this.workerDbName ?? driver.dbName ?? this.config.appId,
      serverUrl: this.config.serverUrl,
      jwtToken: this.config.jwtToken,
      adminSecret: this.config.adminSecret,
      runtimeSources,
      fallbackWasmUrl,
      logLevel: this.config.logLevel,
    };
  }

  private adoptLeaderSnapshot(snapshot: LeaderSnapshot): void {
    this.tabRole = snapshot.role;
    this.tabId = snapshot.tabId;
    this.currentLeaderTabId = snapshot.leaderTabId;
    this.currentLeaderTerm = snapshot.term;
  }

  private openSyncChannel(): void {
    if (this.syncChannel || !this.primaryDbName) return;
    const ChannelCtor = resolveBroadcastChannelCtor();
    if (!ChannelCtor) {
      this.logLeaderDebug("sync-channel-unavailable");
      return;
    }

    const channelName = `jazz-tab-sync:${this.config.appId}:${this.primaryDbName}`;
    this.syncChannel = new ChannelCtor(channelName);
    this.syncChannel.addEventListener("message", this.onSyncChannelMessage);
    this.logLeaderDebug("sync-channel-open", {
      channelName,
    });
  }

  private closeSyncChannel(): void {
    if (!this.syncChannel) return;
    this.syncChannel.removeEventListener("message", this.onSyncChannelMessage);
    this.syncChannel.close();
    this.syncChannel = null;
    this.logLeaderDebug("sync-channel-close");
  }

  private postSyncChannelMessage(message: TabSyncMessage): void {
    this.syncChannel?.postMessage(message);
  }

  private getOrCreateStorageResetContext(
    requestId: string,
    initiatedBySelf: boolean,
  ): StorageResetContext {
    if (this.activeStorageReset?.requestId === requestId) {
      if (initiatedBySelf) {
        this.activeStorageReset.initiatedBySelf = true;
      }
      return this.activeStorageReset;
    }

    const completion = createDeferred<void>();
    // Suppress unhandled rejection warnings for remote-initiated resets that
    // have no local caller awaiting the completion promise.
    void completion.promise.catch(() => undefined);

    const context: StorageResetContext = {
      requestId,
      initiatedBySelf,
      coordinatorTabId: null,
      begun: false,
      completed: false,
      preparePromise: null,
      completion,
    };
    this.activeStorageReset = context;
    return context;
  }

  private clearStorageResetContext(requestId: string): void {
    if (this.activeStorageReset?.requestId === requestId) {
      this.activeStorageReset = null;
    }
    if (this.storageResetCoordinator?.requestId === requestId) {
      this.storageResetCoordinator = null;
    }
  }

  private resolveStorageResetContext(context: StorageResetContext): void {
    if (context.completed) {
      return;
    }
    context.completed = true;
    context.completion.resolve();
    this.clearStorageResetContext(context.requestId);
  }

  private rejectStorageResetContext(context: StorageResetContext, error: unknown): void {
    if (context.completed) {
      return;
    }
    context.completed = true;
    context.completion.reject(toError(error, "Browser storage reset failed"));
    this.clearStorageResetContext(context.requestId);
  }

  private async prepareForStorageReset(
    context: StorageResetContext,
    coordinatorTabId: string,
  ): Promise<string> {
    if (context.preparePromise) {
      return await context.preparePromise;
    }

    context.begun = true;
    context.coordinatorTabId = coordinatorTabId;
    context.preparePromise = (async () => {
      if (this.bridgeReady) {
        await this.bridgeReady;
      }

      const namespace = this.currentWorkerNamespace();
      await this.shutdownWorkerAndClientsForStorageReset();

      if (this.tabId && coordinatorTabId !== this.tabId) {
        this.postSyncChannelMessage({
          type: "storage-reset-ack",
          requestId: context.requestId,
          fromTabId: this.tabId,
          namespace,
        });
      }

      return namespace;
    })();

    return await context.preparePromise;
  }

  private async waitForStorageResetQuiescence(
    coordinator: StorageResetCoordinatorState,
  ): Promise<void> {
    while (true) {
      const now = Date.now();
      const elapsed = now - coordinator.startedAtMs;
      const idleMs = now - coordinator.lastAckAtMs;
      if (elapsed >= STORAGE_RESET_DISCOVERY_WINDOW_MS && idleMs >= STORAGE_RESET_ACK_QUIET_MS) {
        return;
      }
      await sleep(25);
    }
  }

  private async collectStorageResetNamespaces(
    extraNamespaces: Iterable<string>,
  ): Promise<string[]> {
    const namespaces = new Set<string>();
    const primaryDbName = this.primaryDbName;
    if (primaryDbName) {
      namespaces.add(primaryDbName);
    }
    for (const namespace of extraNamespaces) {
      namespaces.add(namespace);
    }

    if (!primaryDbName) {
      return [...namespaces];
    }

    const rootDirectory = await navigator.storage.getDirectory();
    const rootWithEntries = rootDirectory as FileSystemDirectoryHandle & {
      entries?: () => AsyncIterable<[string, FileSystemHandle]>;
    };
    if (typeof rootWithEntries.entries !== "function") {
      return [...namespaces];
    }

    const suffix = ".opfsbtree";
    const fallbackPrefix = `${primaryDbName}__fallback__`;

    for await (const [name] of rootWithEntries.entries()) {
      if (!name.endsWith(suffix)) continue;
      const namespace = name.slice(0, -suffix.length);
      if (namespace === primaryDbName || namespace.startsWith(fallbackPrefix)) {
        namespaces.add(namespace);
      }
    }

    return [...namespaces];
  }

  private async resumeAfterStorageReset(): Promise<void> {
    if (this.worker || this.isShuttingDown) {
      return;
    }
    this.worker = await Db.spawnWorker(this.config.runtimeSources);
  }

  private async runSingleTabStorageReset(context: StorageResetContext): Promise<void> {
    const coordinatorTabId = this.tabId ?? "single-tab-reset";
    let resultError: Error | null = null;

    try {
      const namespace = await this.prepareForStorageReset(context, coordinatorTabId);
      const namespaces = await this.collectStorageResetNamespaces([namespace]);
      for (const candidate of namespaces) {
        await this.removeOpfsNamespaceFile(candidate);
      }
    } catch (error) {
      resultError = toError(error, "Browser storage reset failed");
    }

    try {
      await this.resumeAfterStorageReset();
    } catch (error) {
      if (!resultError) {
        resultError = toError(error, "Failed to restart browser worker after storage reset");
      }
    }

    if (resultError) {
      throw resultError;
    }
  }

  private async startStorageResetAsCoordinator(context: StorageResetContext): Promise<void> {
    if (this.storageResetCoordinator?.requestId === context.requestId) {
      return await (this.storageResetCoordinator.runPromise ?? context.completion.promise);
    }

    if (!this.tabId || this.tabRole !== "leader") {
      throw new Error("Storage reset coordination requires the current tab to be the leader.");
    }

    const coordinator: StorageResetCoordinatorState = {
      requestId: context.requestId,
      startedAtMs: Date.now(),
      lastAckAtMs: Date.now(),
      ackedNamespacesByTabId: new Map(),
      runPromise: null,
    };
    this.storageResetCoordinator = coordinator;

    coordinator.runPromise = (async () => {
      let resultError: Error | null = null;

      try {
        this.postSyncChannelMessage({
          type: "storage-reset-begin",
          requestId: context.requestId,
          coordinatorTabId: this.tabId!,
          term: this.currentLeaderTerm,
        });

        const localNamespace = await this.prepareForStorageReset(context, this.tabId!);
        coordinator.ackedNamespacesByTabId.set(this.tabId!, localNamespace);
        coordinator.lastAckAtMs = Date.now();

        await this.waitForStorageResetQuiescence(coordinator);

        const namespaces = await this.collectStorageResetNamespaces(
          coordinator.ackedNamespacesByTabId.values(),
        );
        for (const namespace of namespaces) {
          await this.removeOpfsNamespaceFile(namespace);
        }
      } catch (error) {
        resultError = toError(error, "Browser storage reset failed");
      }

      try {
        await this.resumeAfterStorageReset();
      } catch (error) {
        if (!resultError) {
          resultError = toError(error, "Failed to restart browser worker after storage reset");
        }
      }

      this.postSyncChannelMessage({
        type: "storage-reset-finished",
        requestId: context.requestId,
        success: resultError === null,
        ...(resultError ? { errorMessage: resultError.message } : {}),
      });

      if (resultError) {
        throw resultError;
      }
    })()
      .then(() => {
        this.resolveStorageResetContext(context);
      })
      .catch((error) => {
        this.rejectStorageResetContext(context, error);
      })
      .finally(() => {
        if (this.storageResetCoordinator?.requestId === context.requestId) {
          this.storageResetCoordinator = null;
        }
      });

    await coordinator.runPromise;
  }

  private async requestCoordinatedStorageReset(): Promise<void> {
    if (!this.syncChannel || !this.tabId) {
      const requestId = createOperationId("storage-reset");
      const context = this.getOrCreateStorageResetContext(requestId, true);
      try {
        await this.runSingleTabStorageReset(context);
        this.resolveStorageResetContext(context);
      } catch (error) {
        this.rejectStorageResetContext(context, error);
      }
      await context.completion.promise;
      return;
    }

    if (this.activeStorageReset) {
      await this.activeStorageReset.completion.promise;
      return;
    }

    const requestId = createOperationId("storage-reset");
    const context = this.getOrCreateStorageResetContext(requestId, true);

    if (this.tabRole === "leader") {
      await this.startStorageResetAsCoordinator(context);
      return;
    }

    const deadline = Date.now() + STORAGE_RESET_REQUEST_TIMEOUT_MS;
    while (!context.begun) {
      if ((this.tabRole as LeaderRole) === "leader") {
        await this.startStorageResetAsCoordinator(context);
        return;
      }

      this.postSyncChannelMessage({
        type: "storage-reset-request",
        requestId,
        fromTabId: this.tabId,
        toLeaderTabId: this.currentLeaderTabId,
        term: this.currentLeaderTerm,
      });

      const settled = await Promise.race([
        context.completion.promise.then(
          () => true,
          () => true,
        ),
        sleep(STORAGE_RESET_REQUEST_RETRY_MS).then(() => false),
      ]);
      if (settled) {
        await context.completion.promise;
        return;
      }

      if (Date.now() >= deadline) {
        const error = new Error(
          "Timed out waiting for the leader tab to begin browser storage reset.",
        );
        this.rejectStorageResetContext(context, error);
        throw error;
      }
    }

    await context.completion.promise;
  }

  private attachLifecycleHooks(): void {
    if (this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;

    document.addEventListener("visibilitychange", this.onVisibilityChange);
    window.addEventListener("pagehide", this.onPageHide);
    // "freeze"/"resume" are non-standard but available in Chromium lifecycle APIs.
    document.addEventListener("freeze", this.onPageFreeze as EventListener);
    document.addEventListener("resume", this.onPageResume as EventListener);
    this.lifecycleHooksAttached = true;
  }

  private detachLifecycleHooks(): void {
    if (!this.lifecycleHooksAttached) return;
    if (typeof window === "undefined" || typeof document === "undefined") return;

    document.removeEventListener("visibilitychange", this.onVisibilityChange);
    window.removeEventListener("pagehide", this.onPageHide);
    document.removeEventListener("freeze", this.onPageFreeze as EventListener);
    document.removeEventListener("resume", this.onPageResume as EventListener);
    this.lifecycleHooksAttached = false;
  }

  private sendLifecycleHint(event: WorkerLifecycleEvent): void {
    if (this.isShuttingDown || !this.worker) return;
    this.logLeaderDebug("lifecycle-hint", { event });

    if (this.workerBridge) {
      this.workerBridge.sendLifecycleHint(event);
      return;
    }

    this.worker.postMessage({
      type: "lifecycle-hint",
      event,
      sentAtMs: Date.now(),
    });
  }

  private logLeaderDebug(event: string, extra?: Record<string, unknown>): void {
    if (!isLeaderDebugEnabled()) return;
    console.info("[db:leader]", event, {
      tabId: this.tabId,
      role: this.tabRole,
      term: this.currentLeaderTerm,
      leaderTabId: this.currentLeaderTabId,
      workerDbName: this.workerDbName,
      ...extra,
    });
  }

  private handleSyncChannelMessage(raw: unknown): void {
    if (this.isShuttingDown || !this.tabId) return;
    if (!isTabSyncMessage(raw)) return;

    switch (raw.type) {
      case "storage-reset-request":
        this.handleStorageResetRequest(raw);
        return;
      case "storage-reset-begin":
        this.handleStorageResetBegin(raw);
        return;
      case "storage-reset-ack":
        this.handleStorageResetAck(raw);
        return;
      case "storage-reset-finished":
        this.handleStorageResetFinished(raw);
        return;
      case "follower-sync":
        this.handleFollowerSync(raw);
        return;
      case "leader-sync":
        this.handleLeaderSync(raw);
        return;
      case "follower-close":
        this.handleFollowerClose(raw);
        return;
    }
  }

  private handleStorageResetRequest(message: StorageResetRequestMessage): void {
    if (this.tabRole !== "leader") return;
    if (!this.tabId) return;
    if (message.fromTabId === this.tabId) return;
    if (message.toLeaderTabId && message.toLeaderTabId !== this.tabId) return;
    if (message.term !== this.currentLeaderTerm) return;
    if (this.activeStorageReset && this.activeStorageReset.requestId !== message.requestId) return;

    const context = this.getOrCreateStorageResetContext(message.requestId, false);
    void this.startStorageResetAsCoordinator(context).catch(() => undefined);
  }

  private handleStorageResetBegin(message: StorageResetBeginMessage): void {
    if (!this.currentLeaderTabId) return;
    if (message.coordinatorTabId !== this.currentLeaderTabId) return;
    if (message.term !== this.currentLeaderTerm) return;
    if (message.coordinatorTabId === this.tabId) return;
    if (this.activeStorageReset && this.activeStorageReset.requestId !== message.requestId) return;

    const context = this.getOrCreateStorageResetContext(message.requestId, false);
    context.begun = true;
    context.coordinatorTabId = message.coordinatorTabId;

    void this.prepareForStorageReset(context, message.coordinatorTabId).catch((error) => {
      this.rejectStorageResetContext(context, error);
    });
  }

  private handleStorageResetAck(message: StorageResetAckMessage): void {
    const coordinator = this.storageResetCoordinator;
    if (!coordinator || coordinator.requestId !== message.requestId) return;

    coordinator.ackedNamespacesByTabId.set(message.fromTabId, message.namespace);
    coordinator.lastAckAtMs = Date.now();
  }

  private handleStorageResetFinished(message: StorageResetFinishedMessage): void {
    const context = this.activeStorageReset;
    if (!context || context.requestId !== message.requestId || context.completed) return;

    void (async () => {
      let resultError: Error | null = message.success
        ? null
        : new Error(message.errorMessage ?? "Browser storage reset failed");

      try {
        await this.resumeAfterStorageReset();
      } catch (error) {
        if (!resultError) {
          resultError = toError(error, "Failed to restart browser worker after storage reset");
        }
      }

      if (resultError) {
        this.rejectStorageResetContext(context, resultError);
      } else {
        this.resolveStorageResetContext(context);
      }
    })();
  }

  private handleFollowerSync(message: FollowerSyncMessage): void {
    if (this.tabRole !== "leader") return;
    if (!this.workerBridge) return;
    if (!this.tabId || message.toLeaderTabId !== this.tabId) return;
    if (message.term !== this.currentLeaderTerm) return;

    if (!this.leaderPeerIds.has(message.fromTabId)) {
      this.leaderPeerIds.add(message.fromTabId);
      this.workerBridge.openPeer(message.fromTabId);
      this.logLeaderDebug("peer-open", {
        peerId: message.fromTabId,
      });
    }
    this.workerBridge.sendPeerSync(message.fromTabId, message.term, message.payload);
  }

  private handleLeaderSync(message: LeaderSyncMessage): void {
    if (this.tabRole !== "follower") return;
    if (!this.workerBridge) return;
    if (!this.tabId || message.toTabId !== this.tabId) return;
    if (!this.currentLeaderTabId || message.fromLeaderTabId !== this.currentLeaderTabId) return;
    if (message.term !== this.currentLeaderTerm) return;

    for (const payload of message.payload) {
      this.workerBridge.applyIncomingServerPayload(payload);
    }
  }

  private handleFollowerClose(message: FollowerCloseMessage): void {
    if (this.tabRole !== "leader") return;
    if (!this.workerBridge) return;
    if (!this.tabId || message.toLeaderTabId !== this.tabId) return;
    if (message.term !== this.currentLeaderTerm) return;
    if (!this.leaderPeerIds.has(message.fromTabId)) return;

    this.leaderPeerIds.delete(message.fromTabId);
    this.workerBridge.closePeer(message.fromTabId);
    this.logLeaderDebug("peer-close", {
      peerId: message.fromTabId,
    });
  }

  private handleWorkerPeerSync(batch: PeerSyncBatch): void {
    if (this.isShuttingDown) return;
    if (this.tabRole !== "leader") return;
    if (!this.tabId) return;
    if (batch.term !== this.currentLeaderTerm) return;

    this.postSyncChannelMessage({
      type: "leader-sync",
      fromLeaderTabId: this.tabId,
      toTabId: batch.peerId,
      term: batch.term,
      payload: batch.payload,
    });
  }

  private sendFollowerClose(leaderTabId: string | null, term: number): void {
    if (!leaderTabId || !this.tabId) return;
    if (leaderTabId === this.tabId) return;

    this.logLeaderDebug("follower-close", {
      toLeaderTabId: leaderTabId,
      closeTerm: term,
    });

    this.postSyncChannelMessage({
      type: "follower-close",
      fromTabId: this.tabId,
      toLeaderTabId: leaderTabId,
      term,
    });
  }

  private applyBridgeRoutingForCurrentLeader(
    bridge: WorkerBridge,
    replayConnection: boolean,
  ): void {
    if (this.tabRole === "leader") {
      bridge.setServerPayloadForwarder(null);
      this.activeRemoteLeaderTabId = null;
      this.logLeaderDebug("upstream-mode", {
        mode: "leader-direct",
      });
    } else {
      bridge.setServerPayloadForwarder((payload) => {
        if (!this.tabId || !this.currentLeaderTabId) return;
        if (this.currentLeaderTabId === this.tabId) return;

        this.postSyncChannelMessage({
          type: "follower-sync",
          fromTabId: this.tabId,
          toLeaderTabId: this.currentLeaderTabId,
          term: this.currentLeaderTerm,
          payload: [payload],
        });
      });
      this.activeRemoteLeaderTabId = this.currentLeaderTabId;
      this.logLeaderDebug("upstream-mode", {
        mode: "follower-via-leader",
        upstreamLeaderTabId: this.currentLeaderTabId,
      });
    }

    if (replayConnection) {
      bridge.replayServerConnection();
      this.logLeaderDebug("upstream-replay");
    }
  }

  private onLeaderElectionChange(snapshot: LeaderSnapshot): void {
    if (this.isShuttingDown || !this.primaryDbName) return;

    const previousRole = this.tabRole;
    const previousLeaderTabId = this.currentLeaderTabId;
    const previousTerm = this.currentLeaderTerm;
    this.adoptLeaderSnapshot(snapshot);
    this.logLeaderDebug("leader-change", {
      previousRole,
      previousLeaderTabId,
      previousTerm,
    });

    if (previousRole === "follower" && previousLeaderTabId !== this.currentLeaderTabId) {
      this.sendFollowerClose(previousLeaderTabId, previousTerm);
    }

    const nextDbName = Db.resolveWorkerDbNameForSnapshot(this.primaryDbName, snapshot);
    const dbNameChanged = nextDbName !== this.workerDbName;
    this.workerDbName = nextDbName;

    // No bridge means no runtime server edge exists yet.
    if (!this.workerBridge) return;

    this.enqueueWorkerReconfigure(async () => {
      if (this.isShuttingDown) return;
      if (dbNameChanged) {
        this.logLeaderDebug("worker-restart", {
          reason: "db-name-change",
        });
        await this.restartWorkerWithCurrentDbName();
        return;
      }

      if (this.workerBridge) {
        this.applyBridgeRoutingForCurrentLeader(this.workerBridge, true);
      }
    });
  }

  private enqueueWorkerReconfigure(task: () => Promise<void>): void {
    this.workerReconfigure = this.workerReconfigure.then(task).catch((error) => {
      console.error("[db] Worker reconfigure failed:", error);
    });
  }

  private async restartWorkerWithCurrentDbName(): Promise<void> {
    const currentWorker = this.worker;
    if (!currentWorker) return;

    // If bridge init is in flight, wait before tearing down.
    if (this.bridgeReady) {
      await this.bridgeReady;
    }

    if (this.workerBridge) {
      try {
        await this.workerBridge.shutdown(currentWorker);
      } catch {
        // Best effort
      }
      this.workerBridge = null;
    }
    this.bridgeReady = null;

    currentWorker.terminate();
    this.worker = await Db.spawnWorker(this.config.runtimeSources);

    // Re-attach immediately for existing client runtime(s) so subscriptions replay.
    const first = this.clients.entries().next();
    if (!first.done) {
      const [schemaJson, client] = first.value;
      this.attachWorkerBridge(schemaJson, client);
      if (this.bridgeReady) {
        await this.bridgeReady;
      }
    }
  }

  private currentWorkerNamespace(): string {
    const driver = resolveStorageDriver(this.config.driver);
    if (driver.type !== "persistent") {
      throw new Error("Worker namespace is only available for driver.type='persistent'");
    }
    return this.workerDbName ?? driver.dbName ?? this.config.appId;
  }

  private async shutdownWorkerAndClientsForStorageReset(): Promise<void> {
    const currentWorker = this.worker;

    if (this.workerBridge && currentWorker) {
      try {
        await this.workerBridge.shutdown(currentWorker);
      } catch {
        // Best effort: if the bridge shutdown times out, we still terminate below.
      }
    }
    this.workerBridge = null;
    this.bridgeReady = null;

    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();
    this.leaderPeerIds.clear();
    this.activeRemoteLeaderTabId = null;

    if (currentWorker) {
      currentWorker.terminate();
    }
    this.worker = null;
  }

  private async removeOpfsNamespaceFile(namespace: string): Promise<void> {
    const rootDirectory = await navigator.storage.getDirectory();
    const fileName = `${namespace}.opfsbtree`;
    try {
      await rootDirectory.removeEntry(fileName, { recursive: false });
    } catch (error) {
      const name = (error as { name?: string } | undefined)?.name;
      if (name === "NotFoundError") {
        return;
      }
      if (name === "NoModificationAllowedError" || name === "InvalidStateError") {
        throw new Error(
          `Failed to delete browser storage for "${namespace}" because OPFS is locked by another tab. Close other tabs and retry.`,
        );
      }
      throw new Error(
        `Failed to delete browser storage for "${namespace}": ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }

  private static resolveWorkerDbNameForSnapshot(
    primaryDbName: string,
    snapshot: LeaderSnapshot,
  ): string {
    if (snapshot.role === "leader") return primaryDbName;
    return `${primaryDbName}__fallback__${snapshot.tabId}`;
  }

  private static async spawnWorker(runtimeSources?: RuntimeSourcesConfig): Promise<Worker> {
    let worker: Worker;

    if (runtimeSources?.workerUrl || runtimeSources?.baseUrl) {
      // Explicit worker location — use dynamic URL resolution.
      const locationHref = typeof location !== "undefined" ? location.href : undefined;
      const syncInitInput = resolveRuntimeConfigSyncInitInput(runtimeSources);
      const wasmUrl = syncInitInput
        ? null
        : resolveWorkerBootstrapWasmUrl(import.meta.url, locationHref, runtimeSources);
      const workerUrl = appendWorkerRuntimeWasmUrl(
        resolveRuntimeConfigWorkerUrl(import.meta.url, locationHref, runtimeSources),
        wasmUrl,
      );
      worker = new Worker(workerUrl, { type: "module" });
    } else {
      // Static URL pattern — bundlers (Turbopack, webpack, Vite) detect this
      // and automatically bundle the worker script + its WASM dependency.
      worker = new Worker(new URL("../worker/jazz-worker.js", import.meta.url), {
        type: "module",
      });
    }

    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => reject(new Error("Worker bootstrap timeout")), 15000);
      const handler = (event: MessageEvent) => {
        if (event.data.type === "ready") {
          clearTimeout(timeout);
          worker.removeEventListener("message", handler);
          resolve();
        } else if (event.data.type === "error") {
          clearTimeout(timeout);
          worker.removeEventListener("message", handler);
          reject(new Error(event.data.message));
        }
      };
      worker.addEventListener("message", handler);
      worker.addEventListener("error", (e) => {
        clearTimeout(timeout);
        reject(new Error(`Worker load error: ${e.message}`));
      });
    });

    return worker;
  }

  updateAuthToken(jwtToken: string | null): void {
    this.applyAuthUpdate(jwtToken);
  }

  updateCookieSession(cookieSession: Session | null): void {
    this.applyCookieSessionUpdate(cookieSession);
  }

  getAuthState(): AuthState {
    return this.authStateStore.getState();
  }

  /**
   * Mint a short-lived local-first JWT proving possession of the current identity.
   * Returns `null` if the current session is not local-first.
   */
  async getLocalFirstIdentityProof(options?: {
    ttlSeconds?: number;
    audience?: string;
  }): Promise<string | null> {
    if (!this._localFirstSecret) {
      return null;
    }

    const wasmModule = this.wasmModule;
    if (!wasmModule) {
      return null;
    }

    const ttl = options?.ttlSeconds ?? 60;
    const audience = options?.audience ?? this.config.appId;
    const nowSeconds = BigInt(Math.floor(Date.now() / 1000));

    return wasmModule.WasmRuntime.mintJazzSelfSignedToken(
      this._localFirstSecret,
      "urn:jazz:local-first",
      audience,
      BigInt(ttl),
      nowSeconds,
    );
  }

  onAuthChanged(listener: (state: AuthState) => void): () => void {
    return this.authStateStore.onChange((state) => {
      listener(state);
    });
  }

  /**
   * Attach a fallback listener to be notified when a write operation
   * (insert, update, delete) is rejected.
   * This callback is only called if the write error is not surfaced by
   * {@link WriteHandle.wait}.
   * This callback is called even after app restarts (which does not
   * happen with {@link WriteHandle.wait}).
   */
  onMutationError(listener: (event: MutationErrorEvent) => void): () => void {
    this.mutationErrorListeners.add(listener);
    for (const client of this.clients.values()) {
      this.attachMutationErrorHandler(client);
    }
    return () => {
      this.mutationErrorListeners.delete(listener);
      if (this.mutationErrorListeners.size > 0) {
        return;
      }
      for (const unsubscribe of this.clientMutationErrorUnsubscribers.values()) {
        unsubscribe();
      }
      this.clientMutationErrorUnsubscribers.clear();
    };
  }

  getConfig(): DbConfig {
    // Return a copy of the config to avoid editing the original config.
    return structuredClone(this.config);
  }

  setDevMode(enabled: boolean): void {
    this.config.devMode = enabled;
  }

  /**
   * @internal
   */
  getActiveQuerySubscriptions(): ActiveQuerySubscriptionTrace[] {
    return Array.from(this.activeQuerySubscriptionTraces.values())
      .filter((trace) => trace.visibility === "public")
      .map(({ visibility: _visibility, ...trace }) => cloneActiveQuerySubscriptionTrace(trace));
  }

  /**
   * @internal
   */
  onActiveQuerySubscriptionsChange(listener: ActiveQuerySubscriptionTraceListener): () => void {
    this.activeQuerySubscriptionTraceListeners.add(listener);
    listener(this.getActiveQuerySubscriptions());
    return () => {
      this.activeQuerySubscriptionTraceListeners.delete(listener);
    };
  }

  /**
   * Insert a new row into a table without waiting for durability.
   *
   * Use {@link WriteResult.wait} to wait for durable confirmation.
   *
   * @param table Table proxy from generated app module
   * @param data Init object with column values
   * @returns Write result containing the inserted row
   */
  insert<T, Init>(table: TableProxy<T, Init>, data: Init, options?: CreateOptions): WriteResult<T> {
    const client = this.getClient(table._schema);
    // Don't wait for bridge to be ready in worker mode. Inserts will be propagated once the bridge is ready.
    // If the bridge fails to initialize, the insert will be lost on restart.
    const transformedData = transformInsertInput(table, data);
    const values = toInsertRecord(transformedData, table._schema, table._table);
    const inserted = client.create(table._table, values, options);
    return inserted.mapValue((row) =>
      transformOutputRow(table, transformRow(row, table._schema, table._table)),
    );
  }

  /**
   * Create or update a row with a caller-supplied id without waiting for durability.
   *
   * Use {@link WriteHandle.wait} to wait for durable confirmation.
   */
  upsert<T, Init>(
    table: TableProxy<T, Init>,
    data: Partial<Init>,
    options: UpsertOptions,
  ): WriteHandle {
    const client = this.getClient(table._schema);
    const transformedData = transformUpdateInput(table, data);
    const values = toUpdateRecord(transformedData, table._schema, table._table);
    return client.upsert(table._table, values, options);
  }

  /**
   * Update an existing row without waiting for durability.
   *
   * Use {@link WriteHandle.wait} to wait for durable confirmation.
   */
  update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): WriteHandle {
    const client = this.getClient(table._schema);
    const transformedData = transformUpdateInput(table, data);
    const updates = toUpdateRecord(transformedData, table._schema, table._table);
    return client.update(id, updates, options);
  }

  /**
   * Delete a row without waiting for durability.
   *
   * Use {@link WriteHandle.wait} to wait for durable confirmation.
   */
  delete<T, Init>(table: TableProxy<T, Init>, id: string): WriteHandle {
    const client = this.getClient(table._schema);
    return client.delete(id);
  }

  async canInsert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<PermissionDecision> {
    const client = this.getClient(table._schema);
    const transformedData = transformInsertInput(table, data);
    const values = toInsertRecord(transformedData, table._schema, table._table);
    return client.canInsert(table._table, values);
  }

  async canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): Promise<PermissionDecision> {
    const client = this.getClient(table._schema);
    const transformedData = transformUpdateInput(table, data);
    const updates = toUpdateRecord(transformedData, table._schema, table._table);
    return client.canUpdate(id, updates);
  }

  /**
   * Begin a new transaction.
   *
   * Use transactions when several writes should settle together after an authority validates them.
   *
   * Use {@link DbTransaction.commit} to commit the transaction.
   *
   * Prefer using {@link Db.transaction} when an explicit commit is not required.
   */
  beginTransaction(): DbTransaction {
    return new DbTransaction(
      (schema) => this.getClient(schema),
      (client) => client.beginTransactionInternal(),
    );
  }

  /**
   * Run {@link callback} inside a transaction and commit it once the callback returns.
   *
   * Use transactions when several writes should settle together after an authority validates them.
   *
   * @returns a write result containing the result of the callback
   */
  transaction<TResult>(
    callback: (tx: DbTransactionScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  transaction<TResult>(callback: (tx: DbTransactionScope) => TResult): WriteResult<TResult>;
  transaction<TResult>(
    callback: (tx: DbTransactionScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const transaction = this.beginTransaction();
    return runInBatch(
      transaction,
      callback,
      () => getDbTransactionBinding(transaction, "result").client,
    );
  }

  /**
   * Begin a new batch.
   *
   * Use a batch when several visible writes should settle together.
   * Call {@link DbDirectBatch.commit} to freeze the batch, then wait on the
   * returned handle if you need durable confirmation.
   *
   * Prefer using {@link Db.batch} when an explicit commit is not required.
   */
  beginBatch(): DbDirectBatch {
    return new DbDirectBatch(
      (schema) => this.getClient(schema),
      (client) => client.beginBatchInternal(),
    );
  }

  /**
   * Run {@link callback} inside a batch and commit it once the callback returns.
   *
   * Use a batch when several visible writes should settle together.
   *
   * @returns a write result containing the result of the callback
   */
  batch<TResult>(
    callback: (batch: DbBatchScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  batch<TResult>(callback: (batch: DbBatchScope) => TResult): WriteResult<TResult>;
  batch<TResult>(
    callback: (batch: DbBatchScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const batch = this.beginBatch();
    return runInBatch(batch, callback, () => getDbDirectBatchBinding(batch, "result").client);
  }

  /**
   * Delete browser OPFS storage for this Db's active namespace and reopen a clean worker.
   *
   * This clears the primary namespace plus any active follower fallback namespaces for the same
   * browser app/database. It does not touch localStorage-based local-first auth state.
   *
   * Behavior:
   * - Browser worker-backed Db only (throws in non-browser/non-worker runtimes)
   * - Can be initiated from either leader or follower tabs
   * - Coordinates worker shutdown over the tab sync channel before deleting OPFS files
   * - Serializes with worker reconfigure operations
   * - Tears down worker + clients, deletes OPFS files, respawns workers
   * - If deletion fails, all participating tabs still respawn their workers before surfacing the error
   */
  async deleteClientStorage(): Promise<void> {
    if (resolveStorageDriver(this.config.driver).type !== "persistent") {
      throw new Error("deleteClientStorage() is only available when driver.type='persistent'.");
    }

    if (!isBrowser()) {
      console.error(
        "deleteClientStorage() is only available on browser worker-backed Db instances.",
      );
      return;
    }

    const operation = this.workerReconfigure.then(async () => {
      await this.requestCoordinatedStorageReset();
    });

    this.workerReconfigure = operation.then(
      () => undefined,
      () => undefined,
    );

    await operation;
  }

  /**
   * Release the current Db instance for logout flows.
   *
   * When `wipeData` is enabled in browser persistent mode, Jazz first coordinates a cross-tab OPFS
   * wipe and then shuts this Db down. Callers should still sign out of their external auth provider
   * separately and recreate `JazzProvider` / `Db` after logout.
   */
  async logout(options: LogoutOptions = {}): Promise<void> {
    if (options.wipeData) {
      await this.deleteClientStorage();
    }

    await this.shutdown();
  }

  /**
   * Execute a query and return all matching rows as typed objects.
   *
   * @param query QueryBuilder instance (e.g., app.todos.where({done: false}))
   * @returns Array of typed objects matching the query
   */
  async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    const client = this.getClient(query._schema);
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(client.getSchema()),
    );
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(
      query._schema,
      runtimeSchema.get,
      builtQuery.table,
    );
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema.get, outputTable);
    const queryOptions = ordinaryDbQueryOptions(options);
    await this.ensureQueryReady(queryOptions);
    const wasmQuery = translateQuery(builderJson, planningSchema);
    const rows = await client.query(wasmQuery, queryOptions);
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const transformedRows = transformRows(
      rows,
      outputSchema,
      outputTable,
      outputIncludes,
      builtQuery.select,
    );
    return transformedRows.map((row) =>
      transformOutputRow(outputTable === builtQuery.table ? query : {}, row),
    );
  }

  /**
   * Execute a query and return the first matching row, or null.
   *
   * @param query QueryBuilder instance
   * @param options Optional read durability options
   * @returns First matching typed object, or null if none found
   */
  async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const results = await this.all(query, options);
    return results[0] ?? null;
  }

  /**
   * Create a conventional `files` row by chunking a browser Blob into `file_parts`.
   *
   * Expects `app.files` and `app.file_parts` to follow the built-in file-storage conventions.
   */
  async createFileFromBlob<FileRow extends { id: string }, FileInit, FilePartRow, FilePartInit>(
    app: ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit>,
    blob: Blob,
    options?: FileWriteOptions,
  ): Promise<FileRow> {
    return createConventionalFileStorage(this, app).fromBlob(blob, options);
  }

  /**
   * Create a conventional `files` row by chunking a browser ReadableStream into `file_parts`.
   *
   * Expects `app.files` and `app.file_parts` to follow the built-in file-storage conventions.
   */
  async createFileFromStream<FileRow extends { id: string }, FileInit, FilePartRow, FilePartInit>(
    app: ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit>,
    stream: ReadableStream<unknown>,
    options?: FileWriteOptions,
  ): Promise<FileRow> {
    return createConventionalFileStorage(this, app).fromStream(stream, options);
  }

  /**
   * Load a conventional file as a browser ReadableStream by querying the file row first
   * and then reading each referenced `file_parts` row sequentially.
   */
  async loadFileAsStream<FileRow extends { id: string }, FileInit, FilePartRow, FilePartInit>(
    app: ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit>,
    fileOrId: string | FileRow,
    options?: FileReadOptions,
  ): Promise<ReadableStream<Uint8Array>> {
    return createConventionalFileStorage(this, app).toStream(fileOrId, options);
  }

  /**
   * Load a conventional file as a Blob using the same sequential part-query path as `loadFileAsStream`.
   */
  async loadFileAsBlob<FileRow extends { id: string }, FileInit, FilePartRow, FilePartInit>(
    app: ConventionalFileApp<FileRow, FileInit, FilePartRow, FilePartInit>,
    fileOrId: string | FileRow,
    options?: FileReadOptions,
  ): Promise<Blob> {
    return createConventionalFileStorage(this, app).toBlob(fileOrId, options);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * The callback receives a SubscriptionDelta with:
   * - `all`: Complete current result set
   * - `delta`: Ordered list of row-level changes
   *
   * @param query QueryBuilder instance
   * @param callback Called with delta whenever results change
   * @returns Unsubscribe function
   *
   * @example
   * ```typescript
   * const unsubscribe = db.subscribeAll(app.todos, (delta) => {
   *   setTodos(delta.all);
   *   for (const change of delta.delta) {
   *     if (change.kind === 0) {
   *       console.log("New row:", change.row);
   *     }
   *   }
   * });
   *
   * // Later: stop receiving updates
   * unsubscribe();
   * ```
   */
  subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
    session?: Session,
  ): () => void {
    const manager = new SubscriptionManager<T>();
    const client = this.getClient(query._schema);
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(client.getSchema()),
    );
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(
      query._schema,
      runtimeSchema.get,
      builtQuery.table,
    );
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema.get, outputTable);
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const wasmQuery = translateQuery(builderJson, planningSchema);

    const transform = (row: WasmRow): T =>
      transformOutputRow(
        outputTable === builtQuery.table ? query : {},
        transformRow(row, outputSchema, outputTable, outputIncludes, builtQuery.select),
      );

    const handleDelta = (delta: Parameters<SubscriptionManager<T>["handleDelta"]>[0]) => {
      const typedDelta = manager.handleDelta(delta, transform);
      callback(typedDelta);
    };

    const queryOptions = ordinaryDbQueryOptions(options);
    const subId =
      session !== undefined
        ? client.subscribeInternal(
            wasmQuery,
            handleDelta,
            session,
            queryOptions,
            runtimeSchema.peek(),
          )
        : client.subscribe(wasmQuery, handleDelta, queryOptions);
    const traceId = this.registerActiveQuerySubscriptionTrace(
      wasmQuery,
      builtQuery.table,
      queryOptions,
    );

    // Return unsubscribe function
    return () => {
      this.unregisterActiveQuerySubscriptionTrace(traceId);
      client.unsubscribe(subId);
      manager.clear();
    };
  }

  /**
   * Shutdown the Db and release all resources.
   * Closes all memoized JazzClient connections and the worker.
   */
  async shutdown(): Promise<void> {
    this.isShuttingDown = true;
    if (this.localFirstRefreshTimer) {
      clearTimeout(this.localFirstRefreshTimer);
      this.localFirstRefreshTimer = null;
    }
    this.clearActiveQuerySubscriptionTraces();
    this.logLeaderDebug("shutdown");
    this.sendFollowerClose(this.activeRemoteLeaderTabId, this.currentLeaderTerm);
    this.activeRemoteLeaderTabId = null;
    this.leaderPeerIds.clear();
    this.closeSyncChannel();
    this.detachLifecycleHooks();

    if (this.leaderElectionUnsubscribe) {
      this.leaderElectionUnsubscribe();
      this.leaderElectionUnsubscribe = null;
    }
    if (this.leaderElection) {
      this.leaderElection.stop();
      this.leaderElection = null;
    }

    await this.workerReconfigure;

    // Ensure bridge init has completed before sending shutdown —
    // otherwise the worker may still be opening OPFS handles
    await this.ensureBridgeReady();

    // Shutdown worker bridge — waits for OPFS handles to be released
    if (this.workerBridge && this.worker) {
      await this.workerBridge.shutdown(this.worker);
      this.workerBridge = null;
    }

    for (const unsubscribe of this.clientMutationErrorUnsubscribers.values()) {
      unsubscribe();
    }
    this.clientMutationErrorUnsubscribers.clear();
    this.mutationErrorListeners.clear();
    for (const client of this.clients.values()) {
      await client.shutdown();
    }
    this.clients.clear();

    if (this.worker) {
      this.worker.terminate();
      this.worker = null;
    }
  }

  private notifyActiveQuerySubscriptionTraceListeners(): void {
    if (this.activeQuerySubscriptionTraceListeners.size === 0) {
      return;
    }

    const snapshot = this.getActiveQuerySubscriptions();
    for (const listener of this.activeQuerySubscriptionTraceListeners) {
      listener(snapshot);
    }
  }

  private registerActiveQuerySubscriptionTrace(
    queryJson: string,
    fallbackTable: string,
    options?: QueryOptions,
  ): string | null {
    if (!this.config.devMode) {
      return null;
    }

    const resolvedOptions = resolveEffectiveQueryExecutionOptions(this.config, options);
    const payload = this.parseRuntimeQueryTracePayload(queryJson, fallbackTable);
    const traceId = `sub-${this.nextActiveQuerySubscriptionTraceId++}`;

    this.activeQuerySubscriptionTraces.set(traceId, {
      id: traceId,
      query: queryJson,
      table: payload.table,
      branches: payload.branches,
      tier: resolvedOptions.tier,
      propagation: resolvedOptions.propagation,
      createdAt: new Date().toISOString(),
      stack: trimSubscriptionTraceStack(new Error().stack),
      visibility: resolvedOptions.visibility ?? "public",
    });
    this.notifyActiveQuerySubscriptionTraceListeners();

    return traceId;
  }

  private unregisterActiveQuerySubscriptionTrace(traceId: string | null): void {
    if (!traceId) {
      return;
    }
    if (!this.activeQuerySubscriptionTraces.delete(traceId)) {
      return;
    }
    this.notifyActiveQuerySubscriptionTraceListeners();
  }

  private clearActiveQuerySubscriptionTraces(): void {
    if (this.activeQuerySubscriptionTraces.size === 0) {
      return;
    }
    this.activeQuerySubscriptionTraces.clear();
    this.notifyActiveQuerySubscriptionTraceListeners();
  }

  private parseRuntimeQueryTracePayload(
    queryJson: string,
    fallbackTable: string,
  ): RuntimeQueryTracePayload {
    try {
      const parsed = JSON.parse(queryJson) as { table?: unknown; branches?: unknown };
      const table = typeof parsed.table === "string" ? parsed.table : fallbackTable;
      const branches = Array.isArray(parsed.branches)
        ? parsed.branches.filter((branch): branch is string => typeof branch === "string")
        : [];

      return {
        table,
        branches: branches.length > 0 ? branches : [this.config.userBranch ?? "main"],
      };
    } catch {
      return {
        table: fallbackTable,
        branches: [this.config.userBranch ?? "main"],
      };
    }
  }
}

/**
 * A Db implementation that delegates all operations to an existing {@link JazzClient}.
 * Used only for tests.
 */
class ClientBackedDb extends Db {
  private readonly hasScopedAuthState: boolean;

  constructor(
    config: DbConfig,
    private readonly runtimeClient: JazzClient,
    private readonly session?: Session,
    private readonly attribution?: string,
    scopedAuthState?: AuthState,
  ) {
    super(
      config,
      null,
      scopedAuthState
        ? {
            initialState: scopedAuthState,
            lockAuthenticatedState: true,
          }
        : undefined,
    );
    this.hasScopedAuthState = scopedAuthState !== undefined;
  }

  override updateAuthToken(jwtToken: string | null): void {
    if (this.hasScopedAuthState) {
      return;
    }

    if (!this.applyAuthUpdate(jwtToken)) {
      return;
    }

    this.runtimeClient.updateAuthToken(jwtToken ?? undefined);
  }

  override onMutationError(listener: (event: MutationErrorEvent) => void): () => void {
    return this.runtimeClient.onMutationError(listener);
  }
  override updateCookieSession(cookieSession: Session | null): void {
    if (this.hasScopedAuthState) {
      return;
    }

    if (!this.applyCookieSessionUpdate(cookieSession)) {
      return;
    }

    this.runtimeClient.updateCookieSession(cookieSession ?? undefined);
  }

  override insert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
    options?: CreateOptions,
  ): WriteResult<T> {
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(this.runtimeClient.getSchema()),
    );
    const inputSchema = resolveSchemaWithTable(table._schema, runtimeSchema.get, table._table);
    const transformedData = transformInsertInput(table, data);
    const values = toInsertRecord(transformedData, inputSchema, table._table);
    return this.runtimeClient
      .createHandleInternal(table._table, values, this.session, this.attribution, options)
      .mapValue((row) => transformOutputRow(table, transformRow(row, table._schema, table._table)));
  }

  override upsert<T, Init>(
    table: TableProxy<T, Init>,
    data: Partial<Init>,
    options: UpsertOptions,
  ): WriteHandle {
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(this.runtimeClient.getSchema()),
    );
    const inputSchema = resolveSchemaWithTable(table._schema, runtimeSchema.get, table._table);
    const transformedData = transformUpdateInput(table, data);
    const values = toUpdateRecord(transformedData, inputSchema, table._table);
    return this.runtimeClient.upsertHandleInternal(
      table._table,
      values,
      options.id,
      this.session,
      this.attribution,
      options.updatedAt,
    );
  }

  override update<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
    options?: UpdateOptions,
  ): WriteHandle {
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(this.runtimeClient.getSchema()),
    );
    const inputSchema = resolveSchemaWithTable(table._schema, runtimeSchema.get, table._table);
    const transformedData = transformUpdateInput(table, data);
    const updates = toUpdateRecord(transformedData, inputSchema, table._table);
    return this.runtimeClient.updateHandleInternal(
      id,
      updates,
      this.session,
      this.attribution,
      undefined,
      options?.updatedAt,
    );
  }

  override delete<T, Init>(_table: TableProxy<T, Init>, id: string): WriteHandle {
    return this.runtimeClient.deleteHandleInternal(id, this.session, this.attribution);
  }

  override async canInsert<T, Init>(
    table: TableProxy<T, Init>,
    data: Init,
  ): Promise<PermissionDecision> {
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(this.runtimeClient.getSchema()),
    );
    const inputSchema = resolveSchemaWithTable(table._schema, runtimeSchema.get, table._table);
    const transformedData = transformInsertInput(table, data);
    const values = toInsertRecord(transformedData, inputSchema, table._table);
    return this.runtimeClient.canInsertInternal(
      table._table,
      values,
      this.session,
      this.attribution,
    );
  }

  override async canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): Promise<PermissionDecision> {
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(this.runtimeClient.getSchema()),
    );
    const inputSchema = resolveSchemaWithTable(table._schema, runtimeSchema.get, table._table);
    const transformedData = transformUpdateInput(table, data);
    const updates = toUpdateRecord(transformedData, inputSchema, table._table);
    return this.runtimeClient.canUpdateInternal(id, updates, this.session, this.attribution);
  }

  override beginTransaction(): DbTransaction {
    const client = this.runtimeClient;
    return new DbTransaction(
      () => client,
      () => client.beginTransactionInternal(this.session, this.attribution),
    );
  }

  override transaction<TResult>(
    callback: (tx: DbTransactionScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  override transaction<TResult>(
    callback: (tx: DbTransactionScope) => TResult,
  ): WriteResult<TResult>;
  override transaction<TResult>(
    callback: (tx: DbTransactionScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const transaction = this.beginTransaction();
    return runInBatch(
      transaction,
      callback,
      () => getDbTransactionBinding(transaction, "result").client,
    );
  }

  override beginBatch(): DbDirectBatch {
    const client = this.runtimeClient;
    return new DbDirectBatch(
      () => client,
      () => client.beginBatchInternal(this.session, this.attribution),
    );
  }

  override batch<TResult>(
    callback: (batch: DbBatchScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  override batch<TResult>(callback: (batch: DbBatchScope) => TResult): WriteResult<TResult>;
  override batch<TResult>(
    callback: (batch: DbBatchScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const batch = this.beginBatch();
    return runInBatch(batch, callback, () => getDbDirectBatchBinding(batch, "result").client);
  }

  override async all<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T[]> {
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(this.runtimeClient.getSchema()),
    );
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(
      query._schema,
      runtimeSchema.get,
      builtQuery.table,
    );
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema.get, outputTable);
    const queryOptions = ordinaryDbQueryOptions(options);
    await this.ensureQueryReady(queryOptions);
    const rows = await this.runtimeClient.queryInternal(
      translateQuery(builderJson, planningSchema),
      this.session,
      queryOptions,
      runtimeSchema.peek(),
    );
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const transformedRows = transformRows(
      rows,
      outputSchema,
      outputTable,
      outputIncludes,
      builtQuery.select,
    );
    return transformedRows.map((row) =>
      transformOutputRow(outputTable === builtQuery.table ? query : {}, row),
    );
  }

  override async one<T>(query: QueryBuilder<T>, options?: QueryOptions): Promise<T | null> {
    const results = await this.all(query, options);
    return results[0] ?? null;
  }

  override subscribeAll<T extends { id: string }>(
    query: QueryBuilder<T>,
    callback: (delta: SubscriptionDelta<T>) => void,
    options?: QueryOptions,
    _session?: Session,
  ): () => void {
    const manager = new SubscriptionManager<T>();
    const runtimeSchema = createRuntimeSchemaResolver(() =>
      normalizeRuntimeSchema(this.runtimeClient.getSchema()),
    );
    const builderJson = query._build();
    const builtQuery = normalizeBuiltQuery(JSON.parse(builderJson), query._table);
    const planningSchema = resolveSchemaWithTable(
      query._schema,
      runtimeSchema.get,
      builtQuery.table,
    );
    const outputTable = resolveBuiltQueryOutputTable(planningSchema, builtQuery);
    const outputSchema = resolveSchemaWithTable(query._schema, runtimeSchema.get, outputTable);
    const outputIncludes = outputTable !== builtQuery.table ? {} : builtQuery.includes;
    const wasmQuery = translateQuery(builderJson, planningSchema);

    const transform = (row: WasmRow): T =>
      transformOutputRow(
        outputTable === builtQuery.table ? query : {},
        transformRow(row, outputSchema, outputTable, outputIncludes, builtQuery.select),
      );

    const subId = this.runtimeClient.subscribeInternal(
      wasmQuery,
      (delta) => {
        const typedDelta = manager.handleDelta(delta, transform);
        callback(typedDelta);
      },
      this.session,
      ordinaryDbQueryOptions(options),
      runtimeSchema.peek(),
    );

    return () => {
      this.runtimeClient.unsubscribe(subId);
      manager.clear();
    };
  }

  override async shutdown(): Promise<void> {
    // The owning JazzContext owns the runtime lifecycle.
  }
}

/**
 * Check if running in a browser environment with Worker support.
 */
function isBrowser(): boolean {
  return typeof Worker !== "undefined" && typeof window !== "undefined";
}

/**
 * Generate a 32-byte ephemeral seed for anonymous auth.
 *
 * Uses `globalThis.crypto.getRandomValues`, which is available in all
 * supported environments (browser, Node ≥15, React Native, edge workers).
 */
function generateEphemeralSeedBase64Url(): string {
  const bytes = new Uint8Array(32);
  globalThis.crypto.getRandomValues(bytes);
  let binary = "";
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/**
 * Create a new Db instance with the given configuration.
 *
 * This is an **async** factory function that pre-loads the WASM module.
 * After creation, local-first mutations (`insert`/`update`/`delete`) are synchronous.
 * Use the `wait` method when you need a Promise that resolves at a durability tier.
 *
 * In browser environments, automatically uses a dedicated worker for
 * OPFS persistence. In Node.js, uses in-memory storage.
 *
 * @param config Database configuration
 * @returns Promise resolving to Db instance ready for queries and mutations
 *
 * @example
 * ```typescript
 * const db = await createDb({
 *   appId: "my-app",
 *   schema: mySchema,
 * });
 * ```
 */
export async function createDb(config: DbConfig): Promise<Db> {
  if (config.secret && (config.jwtToken || config.cookieSession)) {
    throw new Error("DbConfig error: secret, jwtToken, and cookieSession are mutually exclusive");
  }
  if (config.jwtToken && config.cookieSession) {
    throw new Error("DbConfig error: jwtToken and cookieSession are mutually exclusive");
  }

  let resolvedConfig = { ...config };

  // Local-first auth: resolve seed and mint a JWT
  let localFirstSecret: string | null = null;
  if (config.secret) {
    const secret = config.secret;
    localFirstSecret = secret;

    const wasmModule = await loadWasmModule(config.runtimeSources);
    const nowSeconds = BigInt(Math.floor(Date.now() / 1000));
    const jwtToken = wasmModule.WasmRuntime.mintJazzSelfSignedToken(
      secret,
      "urn:jazz:local-first",
      config.appId,
      BigInt(3600),
      nowSeconds,
    );
    resolvedConfig = { ...resolvedConfig, jwtToken };
  } else if (!config.jwtToken && !config.cookieSession && !config.adminSecret) {
    // Anonymous: mint an ephemeral keypair + anonymous JWT.
    // Admin-secret clients intentionally stay sessionless so local policy
    // evaluation does not preempt backend-authorized transport writes.
    const wasmModule = await loadWasmModule(config.runtimeSources);
    const ephemeralSeed = generateEphemeralSeedBase64Url();
    const nowSeconds = BigInt(Math.floor(Date.now() / 1000));
    const jwtToken = wasmModule.WasmRuntime.mintJazzSelfSignedToken(
      ephemeralSeed,
      ANONYMOUS_JWT_ISSUER,
      config.appId,
      BigInt(3600),
      nowSeconds,
    );
    resolvedConfig = { ...resolvedConfig, jwtToken };
  }

  const driver = resolveStorageDriver(resolvedConfig.driver);

  if (driver.type === "memory" && !resolvedConfig.serverUrl) {
    throw new Error("driver.type='memory' requires serverUrl.");
  }

  logAuthModeInDev(resolvedConfig);

  let db: Db;
  if (isBrowser() && driver.type === "persistent") {
    db = await Db.createWithWorker(resolvedConfig);
  } else {
    db = await Db.create(resolvedConfig);
  }

  if (localFirstSecret) {
    db.initLocalFirstAuth(localFirstSecret, 3600);
  }

  return db;
}

function logAuthModeInDev(config: DbConfig): void {
  if (config.env === "prod") return;
  const session = resolveClientSessionSync({
    appId: config.appId,
    jwtToken: config.jwtToken,
    cookieSession: config.cookieSession,
  });
  const authMode = session?.authMode ?? "anonymous";
  const description =
    authMode === "anonymous"
      ? "anonymous — ephemeral identity, no write permissions on synced data"
      : authMode === "local-first"
        ? "local-first — identity persisted locally via secret"
        : "external — identity issued by an auth provider";
  console.info(`[jazz] auth mode: ${authMode} (${description})`);
}

export function createDbFromClient(
  config: DbConfig,
  client: JazzClient,
  session?: Session,
  attribution?: string,
  scopedAuthState?: AuthState,
): Db {
  return new ClientBackedDb(
    config,
    client,
    session,
    attribution,
    scopedAuthState ?? (session || attribution ? backendScopedAuthState(session) : undefined),
  );
}
