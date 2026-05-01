/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */

import type { AppContext, RuntimeSourcesConfig, Session } from "./context.js";
import type { InsertValues, Value, RowDelta, WasmSchema } from "../drivers/types.js";
import { normalizeRuntimeSchema, serializeRuntimeSchema } from "../drivers/schema-wire.js";
import {
  applyUserAuthHeaders,
  type AuthFailureReason,
  type RuntimeSyncOutboxCallback,
} from "./sync-transport.js";
import {
  resolveClientSessionStateSync,
  LOCAL_FIRST_JWT_ISSUER,
  ANONYMOUS_JWT_ISSUER,
} from "./client-session.js";
import { mapAuthReason } from "./auth-state.js";
import { translateQuery } from "./query-adapter.js";
import { isHiddenIncludeColumnName, resolveSelectedColumns } from "./select-projection.js";
import {
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "./runtime-config.js";
import { normalizeRuntimeWriteError } from "./anonymous-write-denied-error.js";
import { appScopedUrl, httpUrlToWs } from "./url.js";

/**
 * Minimal request shape supported by `JazzClient.forRequest()`.
 *
 * Works with common server frameworks (Express, Fastify, Hono, Web Request wrappers)
 * as long as Authorization headers are exposed through `header(name)` or `headers`.
 */
export interface RequestLike {
  header?: (name: string) => string | undefined;
  headers?: Headers | Record<string, string | string[] | undefined>;
}

/**
 * Common interface for WASM and NAPI runtimes.
 *
 * Both `WasmRuntime` (from jazz-wasm) and `NapiRuntime` (from jazz-napi)
 * satisfy this interface, allowing `JazzClient` to work with either backend.
 */
export interface Runtime {
  insert(table: string, values: InsertValues, object_id?: string | null): DirectInsertResult;
  insertWithSession?(
    table: string,
    values: InsertValues,
    write_context_json?: string | null,
    object_id?: string | null,
  ): DirectInsertResult;
  update(object_id: string, values: Record<string, Value>): DirectMutationResult;
  updateWithSession?(
    object_id: string,
    values: Record<string, Value>,
    write_context_json?: string | null,
  ): DirectMutationResult;
  delete(object_id: string): DirectMutationResult;
  deleteWithSession?(object_id: string, write_context_json?: string | null): DirectMutationResult;
  insertPersisted?(table: string, values: InsertValues, tier: string): PersistedInsertResult;
  insertPersistedWithSession?(
    table: string,
    values: InsertValues,
    write_context_json: string | null | undefined,
    tier: string,
  ): PersistedInsertResult;
  updatePersisted?(object_id: string, values: any, tier: string): PersistedMutationResult;
  updatePersistedWithSession?(
    object_id: string,
    values: any,
    write_context_json: string | null | undefined,
    tier: string,
  ): PersistedMutationResult;
  deletePersisted?(object_id: string, tier: string): PersistedMutationResult;
  deletePersistedWithSession?(
    object_id: string,
    write_context_json: string | null | undefined,
    tier: string,
  ): PersistedMutationResult;
  loadLocalBatchRecord?(batch_id: string): LocalBatchRecord | null;
  loadLocalBatchRecords?(): LocalBatchRecord[];
  drainRejectedBatchIds?(): string[];
  acknowledgeRejectedBatch?(batch_id: string): boolean;
  sealBatch?(batch_id: string): void;
  query(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): Promise<any>;
  subscribe(
    query_json: string,
    on_update: Function,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): number;
  createSubscription(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): number;
  executeSubscription(handle: number, on_update: Function): void;
  unsubscribe(handle: number): void;
  onSyncMessageReceived(payload: Uint8Array | string, seq?: number | null): void;
  /** Route outbox messages to the transport layer. Required for WASM worker-bridge; no-op for NAPI/RN (Rust owns the transport). */
  onSyncMessageToSend?(callback: RuntimeSyncOutboxCallback): void;
  addServer(serverCatalogueStateHash?: string | null, nextSyncSeq?: number | null): void;
  removeServer(): void;
  addClient(): string;
  /**
   * When true, runtime row outputs are already aligned to the declared schema order.
   */
  returnsDeclaredSchemaRows?: boolean;
  getSchema(): any;
  getSchemaHash(): string;
  close?(): void | Promise<void>;
  setClientRole?(client_id: string, role: string): void;
  onSyncMessageReceivedFromClient?(client_id: string, payload: Uint8Array | string): void;
  /** Connect to a Jazz server over WebSocket (Rust transport). */
  connect?(url: string, auth_json: string): void;
  /** Disconnect from the Jazz server and drop the transport handle. */
  disconnect?(): void;
  /** Push updated auth credentials into the live Rust transport. */
  updateAuth?(auth_json: string): void;
  /** Register a callback invoked when the Rust transport rejects the JWT. */
  onAuthFailure?(callback: (reason: string) => void): void;
}

/**
 * Authentication configuration for connecting to a Jazz server.
 *
 * Maps directly to the Rust `AuthConfig` struct in `jazz-tools/src/transport_manager.rs`.
 * All fields are optional; supply only the ones relevant to your auth mode.
 */
export interface AuthConfig {
  /** JWT bearer token for user authentication. */
  jwt_token?: string;
  /** Backend service secret for server-to-server calls. */
  backend_secret?: string;
  /** Admin secret for privileged sync and `/admin/*` catalogue operations. */
  admin_secret?: string;
  /** Opaque session payload forwarded by a backend proxy. */
  backend_session?: unknown;
}

/**
 * Persistence tier for durability guarantees.
 *
 * - `local`: Persisted in local durable storage
 * - `edge`: Persisted at edge server
 * - `global`: Persisted at global server
 */
export type DurabilityTier = "local" | "edge" | "global";
/**
 * Controls when a write is visible to subscriptions.
 *
 * - With `"immediate"`, your own local writes appear in the subscription while it's still waiting for
 * the tier to confirm the initial snapshot (only once the subscription has settled at least once).
 * - With `"deferred"`, all delivery is held until the tier confirms.
 * Default is `"immediate"`.
 */
export type LocalUpdatesMode = "immediate" | "deferred";
/**
 * Controls where the subscription reads data from.
 *
 * - With `"full"`, the subscription is sent to upstream servers, which push matching data back.
 * - With `"local-only"`, only local storage is queried and no server communication happens.
 */
export type QueryPropagation = "full" | "local-only";
/**
 * Whether this query should be shown in the inspector.
 * Useful for helpers and framework internals that create subscriptions
 * but should stay out of the DB inspector.
 * Defaults to `"public"`.
 */
export type QueryVisibility = "public" | "hidden_from_live_query_list";
export interface QueryExecutionOptions {
  tier?: DurabilityTier;
  localUpdates?: LocalUpdatesMode;
  propagation?: QueryPropagation;
  visibility?: QueryVisibility;
}

type TransactionQueryOverlay = {
  batchId: string;
  branchName: string;
  rowIds: string[];
};

type InternalQueryExecutionOptions = QueryExecutionOptions & {
  transactionOverlay?: TransactionQueryOverlay;
};

export interface ResolvedQueryExecutionOptions {
  tier: DurabilityTier;
  localUpdates: LocalUpdatesMode;
  propagation: QueryPropagation;
  visibility: QueryVisibility;
}

type ResolvedInternalQueryExecutionOptions = ResolvedQueryExecutionOptions & {
  transactionOverlay?: TransactionQueryOverlay;
};

interface TimestampOverrideOptions {
  updatedAt?: number;
}

export type BatchMode = "direct" | "transactional";

export interface VisibleBatchMember {
  objectId: string;
  branchName: string;
  batchId: string;
}

export type BatchSettlement =
  | {
      kind: "missing";
      batchId: string;
    }
  | {
      kind: "rejected";
      batchId: string;
      code: string;
      reason: string;
    }
  | {
      kind: "durableDirect";
      batchId: string;
      confirmedTier: DurabilityTier;
      visibleMembers: VisibleBatchMember[];
    }
  | {
      kind: "acceptedTransaction";
      batchId: string;
      confirmedTier: DurabilityTier;
      visibleMembers: VisibleBatchMember[];
    };

export interface LocalBatchRecord {
  batchId: string;
  mode: BatchMode;
  sealed: boolean;
  latestSettlement: BatchSettlement | null;
}

export interface CreateOptions extends TimestampOverrideOptions {
  id?: string;
}

export interface UpsertOptions extends TimestampOverrideOptions {
  id: string;
}

export interface UpdateOptions extends TimestampOverrideOptions {}

/**
 * A mutation error event emitted by {@link JazzClient.onMutationError}.
 * Contains enough information to understand the cause of the error and
 * correlate it with a specific mutation.
 */
export interface MutationErrorEvent {
  code: string;
  reason: string;
  batch: LocalBatchRecord;
}

/**
 * Query row result.
 */
export interface Row {
  id: string;
  values: Value[];
}

export interface DirectInsertResult extends Row {
  batchId: string;
}

export interface DirectMutationResult {
  batchId: string;
}

interface WriteContextPayload {
  session?: Session;
  attribution?: string;
  updated_at?: number;
  batch_mode?: BatchMode;
  batch_id?: string;
  target_branch_name?: string;
}

interface PersistedInsertResult {
  batchId: string;
  row: Row;
}

interface PersistedMutationResult {
  batchId: string;
}

/**
 * Subscription callback type.
 */
export type SubscriptionCallback = (delta: RowDelta) => void;

export interface ConnectSyncRuntimeOptions {
  useBinaryEncoding?: boolean;
  onAuthFailure?: (reason: AuthFailureReason) => void;
}

/**
 * QueryBuilder-compatible input accepted by query and subscribe APIs.
 */
export interface QueryInput {
  _build(): string;
  /** Optional schema metadata available on generated QueryBuilder objects. */
  _schema?: WasmSchema;
}

type QueryExecutionDefaultsContext = {
  serverUrl?: string;
  defaultDurabilityTier?: DurabilityTier;
};

export function resolveDefaultDurabilityTier(
  context: QueryExecutionDefaultsContext,
): DurabilityTier {
  if (context.defaultDurabilityTier) {
    return context.defaultDurabilityTier;
  }

  if (isBrowserRuntime()) {
    return "local";
  }

  // In non-browser environments, default to edge when connected to a server.
  // For local/in-memory runtimes without a server, keep local semantics.
  return context.serverUrl ? "edge" : "local";
}

export function resolveEffectiveQueryExecutionOptions(
  context: QueryExecutionDefaultsContext,
  options?: QueryExecutionOptions,
): ResolvedQueryExecutionOptions {
  return {
    tier: options?.tier ?? resolveDefaultDurabilityTier(context),
    localUpdates: options?.localUpdates ?? "immediate",
    propagation: options?.propagation ?? "full",
    visibility: options?.visibility ?? "public",
  };
}

type RelationIrNode = Record<string, unknown>;
type ArraySubqueryPlan = {
  table: string;
  selectColumns: string[];
  nested: ArraySubqueryPlan[];
};

function resolveQueryJson(query: string | QueryInput): string {
  if (typeof query === "string") {
    return query;
  }

  const builtQuery = query._build();
  const schema = query._schema;
  if (!schema || typeof schema !== "object" || Array.isArray(schema)) {
    return builtQuery;
  }

  // Query payloads already in runtime form include relation_ir and should pass through unchanged.
  try {
    const parsed = JSON.parse(builtQuery) as Record<string, unknown>;
    if (parsed && typeof parsed === "object" && "relation_ir" in parsed) {
      return builtQuery;
    }
  } catch {
    return builtQuery;
  }

  return translateQuery(builtQuery, schema);
}

function resolveRelationIrOutputTable(node: unknown): string | null {
  if (!node || typeof node !== "object") {
    return null;
  }

  const relation = node as RelationIrNode;

  if ("TableScan" in relation) {
    const tableScan = relation.TableScan as { table?: unknown } | undefined;
    return typeof tableScan?.table === "string" ? tableScan.table : null;
  }

  if ("Filter" in relation) {
    return resolveRelationIrOutputTable(
      (relation.Filter as { input?: unknown } | undefined)?.input,
    );
  }

  if ("OrderBy" in relation) {
    return resolveRelationIrOutputTable(
      (relation.OrderBy as { input?: unknown } | undefined)?.input,
    );
  }

  if ("Limit" in relation) {
    return resolveRelationIrOutputTable((relation.Limit as { input?: unknown } | undefined)?.input);
  }

  if ("Offset" in relation) {
    return resolveRelationIrOutputTable(
      (relation.Offset as { input?: unknown } | undefined)?.input,
    );
  }

  if ("Project" in relation) {
    return resolveRelationIrOutputTable(
      (relation.Project as { input?: unknown } | undefined)?.input,
    );
  }

  if ("Gather" in relation) {
    const gather = relation.Gather as { seed?: unknown } | undefined;
    return resolveRelationIrOutputTable(gather?.seed);
  }

  return null;
}

function parseArraySubqueryPlans(value: unknown): ArraySubqueryPlan[] {
  if (!Array.isArray(value)) {
    return [];
  }

  const plans: ArraySubqueryPlan[] = [];
  for (const entry of value) {
    if (typeof entry !== "object" || entry === null) {
      continue;
    }
    const plan = entry as {
      table?: unknown;
      select_columns?: unknown;
      nested_arrays?: unknown;
    };
    if (typeof plan.table !== "string") {
      continue;
    }
    plans.push({
      table: plan.table,
      selectColumns: Array.isArray(plan.select_columns)
        ? plan.select_columns.filter((column): column is string => typeof column === "string")
        : [],
      nested: parseArraySubqueryPlans(plan.nested_arrays),
    });
  }

  return plans;
}

function resolveQueryAlignmentPlan(queryJson: string): {
  outputTable: string | null;
  arraySubqueries: ArraySubqueryPlan[];
  selectColumns: string[];
} {
  try {
    const parsed = JSON.parse(queryJson) as {
      table?: unknown;
      relation_ir?: unknown;
      array_subqueries?: unknown;
      select_columns?: unknown;
    };
    return {
      outputTable:
        typeof parsed.table === "string"
          ? parsed.table
          : resolveRelationIrOutputTable(parsed.relation_ir),
      arraySubqueries: parseArraySubqueryPlans(parsed.array_subqueries),
      selectColumns: Array.isArray(parsed.select_columns)
        ? parsed.select_columns.filter((column): column is string => typeof column === "string")
        : [],
    };
  } catch {
    return {
      outputTable: null,
      arraySubqueries: [],
      selectColumns: [],
    };
  }
}

function resolveNodeTier(tier: AppContext["tier"]): string | undefined {
  if (!tier) return undefined;
  if (Array.isArray(tier)) {
    return tier[0];
  }
  return tier;
}

function isBrowserRuntime(): boolean {
  return typeof window !== "undefined" && typeof document !== "undefined";
}

function getScheduler(): (task: () => void) => void {
  if ("scheduler" in globalThis) {
    return (task: () => void) => {
      // See: https://developer.mozilla.org/en-US/docs/Web/API/Scheduler/postTask
      // @ts-ignore Scheduler is not yet provided by the dom library
      void globalThis.scheduler.postTask(task, { priority: "user-visible" });
    };
  }

  // Wrap rather than returning queueMicrotask directly: the native function
  // throws "Illegal invocation" when called without globalThis as receiver.
  return (task: () => void) => queueMicrotask(task);
}

function encodeQueryExecutionOptions(options: InternalQueryExecutionOptions): string | undefined {
  const payload: {
    propagation?: QueryPropagation;
    local_updates?: LocalUpdatesMode;
    transaction_overlay?: {
      batch_id: string;
      branch_name: string;
      row_ids: string[];
    };
  } = {};
  if ((options.propagation ?? "full") !== "full") {
    payload.propagation = options.propagation;
  }
  if ((options.localUpdates ?? "immediate") !== "immediate") {
    payload.local_updates = options.localUpdates;
  }
  if (options.transactionOverlay && options.transactionOverlay.rowIds.length > 0) {
    payload.transaction_overlay = {
      batch_id: options.transactionOverlay.batchId,
      branch_name: options.transactionOverlay.branchName,
      row_ids: options.transactionOverlay.rowIds,
    };
  }

  if (!payload.propagation && !payload.local_updates && !payload.transaction_overlay) {
    return undefined;
  }

  return JSON.stringify(payload);
}

function readHeader(request: RequestLike, name: string): string | undefined {
  const lower = name.toLowerCase();

  const fromMethod = request.header?.(name) ?? request.header?.(lower);
  if (typeof fromMethod === "string") {
    return fromMethod;
  }

  const headers = request.headers;
  if (!headers) {
    return undefined;
  }

  if (typeof Headers !== "undefined" && headers instanceof Headers) {
    return headers.get(name) ?? headers.get(lower) ?? undefined;
  }

  const record = headers as Record<string, string | string[] | undefined>;
  const raw = record[name] ?? record[lower];
  if (Array.isArray(raw)) {
    return raw[0];
  }
  return raw;
}

function normalizeSubscriptionCallbackArgs(args: unknown[]): RowDelta | string | undefined {
  if (args.length === 1) {
    return args[0] as RowDelta | string;
  }

  if (args.length === 2 && args[0] == null) {
    return args[1] as RowDelta | string | undefined;
  }

  console.error("Invalid subscription callback arguments", args);
  return undefined;
}

function decodeBase64Url(value: string): string {
  const base64 = value.replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);

  if (typeof atob === "function") {
    return atob(padded);
  }
  if (typeof Buffer !== "undefined") {
    return Buffer.from(padded, "base64").toString("utf8");
  }

  throw new Error("No base64 decoder available in this runtime");
}

export function sessionFromRequest(request: RequestLike): Session {
  const authHeader = readHeader(request, "authorization");
  if (!authHeader?.startsWith("Bearer ")) {
    throw new Error("Missing or invalid Authorization header");
  }

  const token = authHeader.slice("Bearer ".length).trim();
  const parts = token.split(".");
  if (parts.length < 2) {
    throw new Error("Invalid JWT format");
  }
  const payloadPart = parts[1];
  if (payloadPart === undefined) {
    throw new Error("Invalid JWT format");
  }

  let payload: unknown;
  try {
    payload = JSON.parse(decodeBase64Url(payloadPart));
  } catch {
    throw new Error("Invalid JWT payload");
  }

  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new Error("Invalid JWT payload");
  }

  const typedPayload = payload as { sub?: unknown; iss?: unknown; claims?: unknown };
  if (typeof typedPayload.sub !== "string" || typedPayload.sub.length === 0) {
    throw new Error("JWT payload missing sub");
  }

  const claims =
    typedPayload.claims &&
    typeof typedPayload.claims === "object" &&
    !Array.isArray(typedPayload.claims)
      ? (typedPayload.claims as Record<string, unknown>)
      : {};

  // Derive authMode from the issuer claim
  const issuer = typeof typedPayload.iss === "string" ? typedPayload.iss.trim() : undefined;
  let authMode: Session["authMode"];
  if (issuer === LOCAL_FIRST_JWT_ISSUER) {
    authMode = "local-first";
  } else if (issuer === ANONYMOUS_JWT_ISSUER) {
    authMode = "anonymous";
  } else {
    authMode = "external";
  }

  return { user_id: typedPayload.sub, claims, authMode };
}

function shouldFallbackToUpsertUpdate(error: unknown): boolean {
  const message = error instanceof Error ? error.message : String(error);
  return message.includes("object already exists") || message.includes("Create failed: Conflict");
}

type BatchWriteContext = {
  batchMode: BatchMode;
  batchId: string;
  targetBranchName: string;
};

function composeTargetBranchName(schemaContext: {
  env: string;
  schema_hash: string;
  user_branch: string;
}): string {
  return `${schemaContext.env}-${schemaContext.schema_hash.slice(0, 12)}-${schemaContext.user_branch}`;
}

function generateBatchId(): string {
  const cryptoObj = (globalThis as { crypto?: Crypto }).crypto;
  const bytes = new Uint8Array(16);

  if (cryptoObj && typeof cryptoObj.getRandomValues === "function") {
    cryptoObj.getRandomValues(bytes);
  } else {
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Math.floor(Math.random() * 256);
    }
  }

  const timestamp = Date.now();
  bytes[0] = Math.floor(timestamp / 2 ** 40) & 0xff;
  bytes[1] = Math.floor(timestamp / 2 ** 32) & 0xff;
  bytes[2] = Math.floor(timestamp / 2 ** 24) & 0xff;
  bytes[3] = Math.floor(timestamp / 2 ** 16) & 0xff;
  bytes[4] = Math.floor(timestamp / 2 ** 8) & 0xff;
  bytes[5] = timestamp & 0xff;
  bytes[6] = (bytes[6] & 0x0f) | 0x70;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;

  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function normalizeUpdatedAt(updatedAt?: number): number | undefined {
  if (updatedAt === undefined) {
    return undefined;
  }
  if (!Number.isFinite(updatedAt) || !Number.isInteger(updatedAt) || updatedAt < 0) {
    throw new Error("Invalid updatedAt override. Expected a non-negative integer.");
  }
  return updatedAt;
}

function durabilityTierRank(tier: DurabilityTier): number {
  switch (tier) {
    case "local":
      return 0;
    case "edge":
      return 1;
    case "global":
      return 2;
  }
}

function settlementSatisfiesTier(
  settlement: BatchSettlement | null | undefined,
  tier: DurabilityTier,
): boolean {
  if (!settlement) {
    return false;
  }

  if (settlement.kind !== "durableDirect" && settlement.kind !== "acceptedTransaction") {
    return false;
  }

  return durabilityTierRank(settlement.confirmedTier) >= durabilityTierRank(tier);
}

function rejectionFromSettlement(
  settlement: BatchSettlement | null | undefined,
): PersistedWriteRejectedError | null {
  if (!settlement || settlement.kind !== "rejected") {
    return null;
  }
  return new PersistedWriteRejectedError(settlement.batchId, settlement.code, settlement.reason);
}

/**
 * Error returned when a write fails to be persisted at a given durability tier.
 */
export class PersistedWriteRejectedError extends Error {
  readonly name = "PersistedWriteRejectedError";

  constructor(
    readonly batchId: string,
    readonly code: string,
    readonly reason: string,
  ) {
    super(`Persisted batch ${batchId} was rejected (${code}): ${reason}`);
  }
}

/**
 * Returned by upsert, update, and delete operations, and explicitly-committed transactions.
 * Allows waiting for the write to be persisted at a given durability tier.
 */
export class WriteHandle<T = void> {
  readonly #client: JazzClient;

  constructor(
    readonly batchId: string,
    client: JazzClient,
  ) {
    this.#client = client;
  }

  /**
   * Wait for the write to be persisted at a given durability tier.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the write is rejected.
   */
  async wait(options: { tier: DurabilityTier }): Promise<T> {
    return this.#client.waitForPersistedBatch(this.batchId, options.tier) as Promise<T>;
  }

  protected client(): JazzClient {
    return this.#client;
  }
}

/**
 * Returned by insert operations and auto-committed transactions.
 * Allows getting the inserted value and waiting for the write
 * to be persisted at a given durability tier.
 */
export class WriteResult<T> extends WriteHandle<T> {
  constructor(
    readonly value: T,
    batchId: string,
    client: JazzClient,
  ) {
    super(batchId, client);
  }

  /**
   * Wait for the write to be persisted at a given durability tier.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the write is rejected.
   * @returns the inserted row.
   */
  override async wait(options: { tier: DurabilityTier }): Promise<T> {
    await super.wait(options);
    return this.value;
  }

  mapValue<U>(transformValue: (value: T) => U): WriteResult<U> {
    return new WriteResult(transformValue(this.value), this.batchId, this.client());
  }
}

function isPromiseLike<T>(value: T | PromiseLike<T>): value is PromiseLike<T> {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    typeof (value as PromiseLike<T>).then === "function"
  );
}

type RunAndCommitResult<TResult> =
  TResult extends PromiseLike<unknown>
    ? Promise<WriteResult<Awaited<TResult>>>
    : WriteResult<TResult>;

export function runInBatch<TBatchOrTx extends { commit(): WriteHandle }, TResult>(
  batchOrTx: TBatchOrTx,
  callback: (target: TBatchOrTx) => TResult,
  client: JazzClient | (() => JazzClient),
): RunAndCommitResult<TResult> {
  const value = callback(batchOrTx);
  const resultClient = typeof client === "function" ? client : () => client;
  if (isPromiseLike(value)) {
    return value.then((resolvedValue) => {
      const committed = batchOrTx.commit();
      return new WriteResult(resolvedValue as Awaited<TResult>, committed.batchId, resultClient());
    }) as RunAndCommitResult<TResult>;
  }
  const committed = batchOrTx.commit();
  return new WriteResult(value, committed.batchId, resultClient()) as RunAndCommitResult<TResult>;
}

export class Transaction {
  private committedHandle: WriteHandle | null = null;
  private readonly touchedRowIds = new Set<string>();

  constructor(
    private readonly client: JazzClient,
    private readonly batchContext: BatchWriteContext,
    private readonly session?: Session,
    private readonly attribution?: string,
  ) {}

  private get committed(): boolean {
    return this.committedHandle !== null;
  }

  private ensureActive(): void {
    if (this.committed) {
      throw new Error(`Transaction ${this.batchContext.batchId} is already committed`);
    }
  }

  private markTouchedRow(rowId: string): void {
    this.touchedRowIds.add(rowId);
  }

  private queryOptions(options?: QueryExecutionOptions): InternalQueryExecutionOptions {
    return {
      ...options,
      localUpdates: "deferred",
      transactionOverlay: {
        batchId: this.batchContext.batchId,
        branchName: this.batchContext.targetBranchName,
        rowIds: [...this.touchedRowIds],
      },
    };
  }

  batchId(): string {
    return this.batchContext.batchId;
  }

  commit(): WriteHandle {
    if (this.committedHandle) {
      return this.committedHandle;
    }
    const handle = this.client.sealBatch(this.batchId());
    this.committedHandle = handle;
    return handle;
  }

  create(table: string, values: InsertValues, options?: CreateOptions): Row {
    this.ensureActive();
    const row = this.client.createInternal(
      table,
      values,
      this.session,
      this.attribution,
      options,
      this.batchContext,
    );
    this.markTouchedRow(row.id);
    return row;
  }

  upsert(table: string, values: InsertValues, options: UpsertOptions): void {
    this.ensureActive();
    this.client.upsertInternal(
      table,
      values,
      options.id,
      this.session,
      this.attribution,
      options.updatedAt,
      this.batchContext,
    );
    this.markTouchedRow(options.id);
  }

  update(objectId: string, updates: Record<string, Value>): void {
    this.ensureActive();
    this.client.updateInternal(
      objectId,
      updates,
      this.session,
      this.attribution,
      this.batchContext,
    );
    this.markTouchedRow(objectId);
  }

  delete(objectId: string): void {
    this.ensureActive();
    this.client.deleteInternal(objectId, this.session, this.attribution, this.batchContext);
    this.markTouchedRow(objectId);
  }

  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    this.ensureActive();
    return this.client.queryInternal(query, this.session, this.queryOptions(options));
  }

  localBatchRecord(batchId = this.batchId()): LocalBatchRecord | null {
    return this.client.localBatchRecord(batchId);
  }

  localBatchRecords(): LocalBatchRecord[] {
    return this.client.localBatchRecords();
  }

  acknowledgeRejectedBatch(batchId = this.batchId()): boolean {
    return this.client.acknowledgeRejectedBatch(batchId);
  }
}

/**
 * Transaction object available inside {@link JazzClient.transaction}'s callback.
 */
export type TransactionScope = Omit<Transaction, "commit">;

export class DirectBatch {
  private committedHandle: WriteHandle | null = null;

  constructor(
    private readonly client: JazzClient,
    private readonly batchContext: BatchWriteContext,
    private readonly session?: Session,
    private readonly attribution?: string,
  ) {}

  batchId(): string {
    return this.batchContext.batchId;
  }

  private ensureActive(): void {
    if (this.committedHandle) {
      throw new Error(`Direct batch ${this.batchContext.batchId} is already committed`);
    }
  }

  commit(): WriteHandle {
    if (this.committedHandle) {
      return this.committedHandle;
    }
    const handle = this.client.sealBatch(this.batchId());
    this.committedHandle = handle;
    return handle;
  }

  create(table: string, values: InsertValues, options?: CreateOptions): Row {
    this.ensureActive();
    return this.client.createInternal(
      table,
      values,
      this.session,
      this.attribution,
      options,
      this.batchContext,
    );
  }

  upsert(table: string, values: InsertValues, options: UpsertOptions): void {
    this.ensureActive();
    this.client.upsertInternal(
      table,
      values,
      options.id,
      this.session,
      this.attribution,
      options.updatedAt,
      this.batchContext,
    );
  }

  update(objectId: string, updates: Record<string, Value>): void {
    this.ensureActive();
    this.client.updateInternal(
      objectId,
      updates,
      this.session,
      this.attribution,
      this.batchContext,
    );
  }

  delete(objectId: string): void {
    this.ensureActive();
    this.client.deleteInternal(objectId, this.session, this.attribution, this.batchContext);
  }

  localBatchRecord(batchId = this.batchId()): LocalBatchRecord | null {
    return this.client.localBatchRecord(batchId);
  }

  localBatchRecords(): LocalBatchRecord[] {
    return this.client.localBatchRecords();
  }

  acknowledgeRejectedBatch(batchId = this.batchId()): boolean {
    return this.client.acknowledgeRejectedBatch(batchId);
  }
}

/**
 * Batch object available inside {@link JazzClient.batch}'s callback.
 */
export type BatchScope = Omit<DirectBatch, "commit">;

/**
 * Session-scoped client for backend operations.
 *
 * Created by `JazzClient.forSession()`. Allows backend applications
 * to perform operations as a specific user via header-based authentication.
 */
export class SessionClient {
  private client: JazzClient;
  private session: Session;

  constructor(client: JazzClient, session: Session) {
    this.client = client;
    this.session = session;
  }

  /**
   * Create a new row as this session's user.
   */
  async create(table: string, values: InsertValues, options?: CreateOptions): Promise<string> {
    if (!this.client.getServerUrl()) {
      throw new Error("No server connection");
    }

    const response = await this.client.sendRequest(
      this.client.getRequestUrl("/sync/object"),
      "POST",
      {
        table,
        values,
        schema_context: this.client.getSchemaContext(),
        ...(options?.id ? { object_id: options.id } : {}),
        ...(options?.updatedAt !== undefined
          ? { updated_at: normalizeUpdatedAt(options.updatedAt) }
          : {}),
      },
      this.session,
    );

    if (!response.ok) {
      throw new Error(`Create failed: ${response.statusText}`);
    }

    const result = await response.json();
    return result.object_id;
  }

  /**
   * Create or update a row as this session's user using a caller-supplied id.
   */
  async upsert(table: string, values: InsertValues, options: UpsertOptions): Promise<void> {
    try {
      await this.create(table, values, options);
      return;
    } catch (error) {
      if (!shouldFallbackToUpsertUpdate(error)) {
        throw error;
      }
    }

    await this.update(options.id, values as Record<string, Value>, {
      updatedAt: options.updatedAt,
    });
  }

  /**
   * Update a row as this session's user.
   */
  async update(
    objectId: string,
    updates: Record<string, Value>,
    options?: UpdateOptions,
  ): Promise<void> {
    if (!this.client.getServerUrl()) {
      throw new Error("No server connection");
    }

    const updateArray = Object.entries(updates);

    const response = await this.client.sendRequest(
      this.client.getRequestUrl("/sync/object"),
      "PUT",
      {
        object_id: objectId,
        updates: updateArray,
        schema_context: this.client.getSchemaContext(),
        ...(options?.updatedAt !== undefined
          ? { updated_at: normalizeUpdatedAt(options.updatedAt) }
          : {}),
      },
      this.session,
    );

    if (!response.ok) {
      throw new Error(`Update failed: ${response.statusText}`);
    }
  }

  /**
   * Delete a row as this session's user.
   */
  async delete(objectId: string): Promise<void> {
    if (!this.client.getServerUrl()) {
      throw new Error("No server connection");
    }

    const response = await this.client.sendRequest(
      this.client.getRequestUrl("/sync/object/delete"),
      "POST",
      {
        object_id: objectId,
        schema_context: this.client.getSchemaContext(),
      },
      this.session,
    );

    if (!response.ok) {
      throw new Error(`Delete failed: ${response.statusText}`);
    }
  }

  /**
   * Query as this session's user.
   */
  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    return this.client.queryInternal(query, this.session, options);
  }

  /**
   * Subscribe to a query as this session's user.
   */
  subscribe(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    options?: QueryExecutionOptions,
  ): number {
    return this.client.subscribeInternal(query, callback, this.session, options);
  }

  beginTransaction(): Transaction {
    return this.client.beginTransactionInternal(this.session);
  }

  transaction<TResult>(
    callback: (tx: TransactionScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  transaction<TResult>(callback: (tx: TransactionScope) => TResult): WriteResult<TResult>;
  transaction<TResult>(
    callback: (tx: TransactionScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const transaction = this.beginTransaction();
    return runInBatch(transaction, callback, this.client);
  }

  beginBatch(): DirectBatch {
    return this.client.beginBatchInternal(this.session);
  }

  batch<TResult>(
    callback: (batch: BatchScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  batch<TResult>(callback: (batch: BatchScope) => TResult): WriteResult<TResult>;
  batch<TResult>(
    callback: (batch: BatchScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const batch = this.beginBatch();
    return runInBatch(batch, callback, this.client);
  }

  localBatchRecord(batchId: string): LocalBatchRecord | null {
    return this.client.localBatchRecord(batchId);
  }

  localBatchRecords(): LocalBatchRecord[] {
    return this.client.localBatchRecords();
  }

  acknowledgeRejectedBatch(batchId: string): boolean {
    return this.client.acknowledgeRejectedBatch(batchId);
  }
}

/**
 * High-level Jazz client.
 */
export class JazzClient {
  private runtime: Runtime;
  private scheduler: (task: () => void) => void;
  private context: AppContext;
  private resolvedSession: Session | null;
  private defaultDurabilityTier: DurabilityTier;
  /**
   * Promises created with {@link DirectBatch.wait} or {@TODO_link WriteHandle.wait}
   * that are waiting for a batch to be settled.
   */
  private readonly pendingBatchWaiters = new Map<
    string,
    Array<{
      tier: DurabilityTier;
      resolve: () => void;
      reject: (error: Error) => void;
    }>
  >();
  /**
   * Listeners attached with {@link JazzClient.onMutationError} that are notified when a batch is rejected.
   */
  private readonly mutationErrorListeners = new Set<(event: MutationErrorEvent) => void>();
  private readonly acknowledgedRejectedBatchErrors = new Map<string, PersistedWriteRejectedError>();
  private settlementPollTimer: ReturnType<typeof setTimeout> | null = null;
  private shutdownPromise: Promise<void> | null = null;
  private cachedRuntimeSchemaHash: string | null = null;
  private cachedRuntimeSchema: WasmSchema | null = null;

  private resolveSessionFromContext(): Session | null {
    return resolveClientSessionStateSync({
      appId: this.context.appId,
      jwtToken: this.context.jwtToken,
      cookieSession: this.context.cookieSession,
    }).session;
  }

  private buildTransportAuthPayload(): {
    jwt_token: string | null;
    admin_secret?: string;
    backend_secret?: string;
  } {
    const payload: {
      jwt_token: string | null;
      admin_secret?: string;
      backend_secret?: string;
    } = { jwt_token: this.context.jwtToken ?? null };
    if (this.context.adminSecret) {
      payload.admin_secret = this.context.adminSecret;
    }
    if (this.context.backendSecret) {
      payload.backend_secret = this.context.backendSecret;
    }
    return payload;
  }

  private returnsDeclaredSchemaRows(): boolean {
    return this.runtime.returnsDeclaredSchemaRows === true;
  }

  private constructor(
    runtime: Runtime,
    context: AppContext,
    defaultDurabilityTier: DurabilityTier,
    runtimeOptions?: ConnectSyncRuntimeOptions,
  ) {
    this.runtime = this.wrapRuntime(runtime);
    this.scheduler = getScheduler();
    this.context = context;
    this.defaultDurabilityTier = defaultDurabilityTier;
    this.resolvedSession = this.resolveSessionFromContext();

    if (runtimeOptions?.onAuthFailure) {
      const handler = runtimeOptions.onAuthFailure;
      this.runtime.onAuthFailure?.((reason: string) => {
        handler(mapAuthReason(reason));
      });
    }
  }

  private wrapRuntime(runtime: Runtime): Runtime {
    return new Proxy(runtime, {
      get: (target, property, receiver) => {
        const value = Reflect.get(target, property, receiver);
        if (property === "onSyncMessageReceived" && typeof value === "function") {
          return (payload: Uint8Array | string, seq?: number | null) => {
            const batchesWithPendingWaiters = new Set(this.pendingBatchWaiters.keys());
            value.call(target, payload, seq);
            this.flushPendingBatchWaiters();
            this.flushUnhandledMutationErrors(
              this.drainRejectedBatchIds(),
              batchesWithPendingWaiters,
            );
          };
        }
        if (typeof value === "function") {
          return value.bind(target);
        }
        return value;
      },
    });
  }

  /**
   * Connect to Jazz with the given context.
   *
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance
   */
  static async connect(
    context: AppContext,
    runtimeOptions?: ConnectSyncRuntimeOptions,
  ): Promise<JazzClient> {
    // Load WASM module dynamically
    const wasmModule = await loadWasmModule(context.runtimeSources);

    // Create WASM runtime (storage is now synchronous in-memory)
    const schemaJson = serializeRuntimeSchema(context.schema);
    const runtime = new wasmModule.WasmRuntime(
      schemaJson,
      context.appId,
      context.env ?? "dev",
      context.userBranch ?? "main",
      resolveNodeTier(context.tier),
    );

    const client = new JazzClient(
      runtime,
      context,
      resolveDefaultDurabilityTier(context),
      runtimeOptions,
    );

    return client;
  }

  /**
   * Create client synchronously with a pre-loaded WASM module.
   *
   * Use this after loading WASM via `loadWasmModule()` to avoid
   * async client creation. This enables sync mutations in the Db class.
   *
   * @param wasmModule Pre-loaded WASM module from loadWasmModule()
   * @param context Application context with driver and schema
   * @returns Connected JazzClient instance (created synchronously)
   */
  static connectSync(
    wasmModule: WasmModule,
    context: AppContext,
    runtimeOptions?: ConnectSyncRuntimeOptions,
  ): JazzClient {
    // Create WASM runtime (storage is now synchronous in-memory)
    const schemaJson = serializeRuntimeSchema(context.schema);
    const runtime = new wasmModule.WasmRuntime(
      schemaJson,
      context.appId,
      context.env ?? "dev",
      context.userBranch ?? "main",
      resolveNodeTier(context.tier),
      runtimeOptions?.useBinaryEncoding ?? false,
    );

    return new JazzClient(runtime, context, resolveDefaultDurabilityTier(context), runtimeOptions);
  }

  /**
   * Create client from a pre-constructed runtime (e.g., NapiRuntime).
   *
   * This allows server-side apps to use the native NAPI backend directly
   * without WASM loading.
   *
   * @param runtime A runtime implementing the Runtime interface
   * @param context Application context
   * @returns Connected JazzClient instance
   */
  static connectWithRuntime(
    runtime: Runtime,
    context: AppContext,
    runtimeOptions?: ConnectSyncRuntimeOptions,
  ): JazzClient {
    return new JazzClient(runtime, context, resolveDefaultDurabilityTier(context), runtimeOptions);
  }

  /**
   * Create a session-scoped client for backend operations.
   *
   * This allows backend applications to perform operations as a specific user.
   * Requires `backendSecret` to be configured in the `AppContext`.
   *
   * @param session Session to impersonate
   * @returns SessionClient for performing operations as the given user
   * @throws Error if backendSecret is not configured
   *
   * @example
   * ```typescript
   * const userSession = { user_id: "user-123", claims: {} };
   * const userClient = client.forSession(userSession);
   * const id = await userClient.create("todos", {
   *   title: { type: "Text", value: "Buy milk" },
   *   done: { type: "Boolean", value: false },
   * });
   * ```
   */
  forSession(session: Session): SessionClient {
    if (!this.context.backendSecret) {
      throw new Error("backendSecret required for session impersonation");
    }
    if (!this.context.serverUrl) {
      throw new Error("serverUrl required for session impersonation");
    }
    return new SessionClient(this, session);
  }

  /**
   * Create a session-scoped client from an authenticated HTTP request.
   *
   * Extracts `Authorization: Bearer <jwt>` and maps payload fields:
   * - `sub` -> `session.user_id`
   * - `claims` -> `session.claims` (defaults to `{}`)
   *
   * This helper only extracts payload fields and does not validate JWT signatures.
   * JWT verification should happen in your auth middleware before request handling.
   */
  forRequest(request: RequestLike): SessionClient {
    return this.forSession(sessionFromRequest(request));
  }

  beginTransaction(): Transaction {
    return this.beginTransactionInternal();
  }

  transaction<TResult>(
    callback: (tx: TransactionScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  transaction<TResult>(callback: (tx: TransactionScope) => TResult): WriteResult<TResult>;
  transaction<TResult>(
    callback: (tx: TransactionScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const transaction = this.beginTransaction();
    return runInBatch(transaction, callback, this);
  }

  beginBatch(): DirectBatch {
    return this.beginBatchInternal();
  }

  batch<TResult>(
    callback: (batch: BatchScope) => Promise<TResult>,
  ): Promise<WriteResult<Awaited<TResult>>>;
  batch<TResult>(callback: (batch: BatchScope) => TResult): WriteResult<TResult>;
  batch<TResult>(
    callback: (batch: BatchScope) => TResult | Promise<TResult>,
  ): WriteResult<TResult> | Promise<WriteResult<Awaited<TResult>>> {
    const batch = this.beginBatch();
    return runInBatch(batch, callback, this);
  }

  private createBatchContext(batchMode: BatchMode): BatchWriteContext {
    return {
      batchMode,
      batchId: generateBatchId(),
      targetBranchName: composeTargetBranchName(this.getSchemaContext()),
    };
  }

  beginTransactionInternal(session?: Session, attribution?: string): Transaction {
    return new Transaction(
      this,
      this.createBatchContext("transactional"),
      this.resolveWriteSession(session, attribution),
      attribution,
    );
  }

  beginBatchInternal(session?: Session, attribution?: string): DirectBatch {
    return new DirectBatch(
      this,
      this.createBatchContext("direct"),
      this.resolveWriteSession(session, attribution),
      attribution,
    );
  }

  localBatchRecord(batchId: string): LocalBatchRecord | null {
    return this.requireBatchRecordMethod("loadLocalBatchRecord")(batchId);
  }

  localBatchRecords(): LocalBatchRecord[] {
    const records = this.requireBatchRecordMethod("loadLocalBatchRecords")();
    return [...records].sort((left, right) => left.batchId.localeCompare(right.batchId));
  }

  onMutationError(listener: (event: MutationErrorEvent) => void): () => void {
    this.mutationErrorListeners.add(listener);
    this.flushUnhandledMutationErrors();
    this.ensureSettlementPolling();
    return () => {
      this.mutationErrorListeners.delete(listener);
      if (!this.shouldPollSettlements()) {
        this.cancelSettlementPolling();
      }
    };
  }

  private acknowledgeRejectedBatchInternal(batchId: string): boolean {
    const rejection = rejectionFromSettlement(this.localBatchRecord(batchId)?.latestSettlement);
    const acknowledged = this.requireBatchRecordMethod("acknowledgeRejectedBatch")(batchId);
    if (acknowledged && rejection) {
      this.acknowledgedRejectedBatchErrors.set(batchId, rejection);
    }
    return acknowledged;
  }

  acknowledgeRejectedBatch(batchId: string): boolean {
    const acknowledged = this.acknowledgeRejectedBatchInternal(batchId);
    this.flushPendingBatchWaiters();
    return acknowledged;
  }

  sealBatch(batchId: string): WriteHandle {
    this.requireBatchRecordMethod("sealBatch")(batchId);
    return new WriteHandle(batchId, this);
  }

  /**
   * Enable backend-scoped sync auth for this client.
   *
   * In backend mode, sync/event transport uses `X-Jazz-Backend-Secret` instead
   * of end-user auth headers and intentionally does not send admin headers.
   */
  asBackend(): JazzClient {
    if (!this.context.backendSecret) {
      throw new Error("backendSecret required for backend mode");
    }
    if (!this.context.serverUrl) {
      throw new Error("serverUrl required for backend mode");
    }
    return this;
  }

  updateAuthToken(jwtToken?: string): void {
    this.context.jwtToken = jwtToken;
    this.resolvedSession = this.resolveSessionFromContext();
    // Push the refreshed credentials into the Rust transport. `updateAuth`
    // is optional on the Runtime interface because not every binding exposes
    // it yet; bindings that do will route this to TransportControl::UpdateAuth.
    // Carry forward admin/backend secrets from context — omitting them here
    // would deserialise to None on the Rust side and silently erase any
    // privileged credentials the transport was connected with.
    this.runtime.updateAuth?.(JSON.stringify(this.buildTransportAuthPayload()));
  }

  updateCookieSession(cookieSession?: Session): void {
    this.context.cookieSession = cookieSession;
    this.resolvedSession = this.resolveSessionFromContext();
    this.runtime.updateAuth?.(JSON.stringify(this.buildTransportAuthPayload()));
  }

  private normalizeQueryExecutionOptions(
    options?: InternalQueryExecutionOptions,
  ): ResolvedInternalQueryExecutionOptions {
    const resolved = resolveEffectiveQueryExecutionOptions(
      { ...this.context, defaultDurabilityTier: this.defaultDurabilityTier },
      options,
    );
    if (!options?.transactionOverlay) {
      return resolved;
    }
    return {
      ...resolved,
      transactionOverlay: options.transactionOverlay,
    };
  }

  private encodeWriteContext(
    session?: Session,
    attribution?: string,
    batchContext?: BatchWriteContext,
    updatedAt?: number,
  ): string | undefined {
    if (!session && attribution === undefined && !batchContext && updatedAt === undefined) {
      return undefined;
    }
    if (attribution === undefined && session && !batchContext && updatedAt === undefined) {
      return JSON.stringify(session);
    }

    const payload: WriteContextPayload = {};
    if (session) {
      payload.session = session;
    }
    if (attribution !== undefined) {
      payload.attribution = attribution;
    }
    if (updatedAt !== undefined) {
      payload.updated_at = normalizeUpdatedAt(updatedAt);
    }
    if (batchContext) {
      payload.batch_mode = batchContext.batchMode;
      payload.batch_id = batchContext.batchId;
      payload.target_branch_name = batchContext.targetBranchName;
    }
    return JSON.stringify(payload);
  }

  private resolveWriteSession(session?: Session, attribution?: string): Session | undefined {
    if (session) {
      return session;
    }
    if (attribution !== undefined) {
      return undefined;
    }
    return this.resolvedSession ?? undefined;
  }

  private requireSessionWriteMethod<
    T extends keyof Pick<Runtime, "insertWithSession" | "updateWithSession" | "deleteWithSession">,
  >(method: T): NonNullable<Runtime[T]> {
    const runtimeMethod = this.runtime[method];
    if (!runtimeMethod) {
      throw new Error(`${String(method)} is not supported by this runtime`);
    }
    return runtimeMethod.bind(this.runtime) as NonNullable<Runtime[T]>;
  }

  private requireBatchRecordMethod<
    T extends keyof Pick<
      Runtime,
      "loadLocalBatchRecord" | "loadLocalBatchRecords" | "acknowledgeRejectedBatch" | "sealBatch"
    >,
  >(method: T): NonNullable<Runtime[T]> {
    const runtimeMethod = this.runtime[method];
    if (!runtimeMethod) {
      throw new Error(`${String(method)} is not supported by this runtime`);
    }
    return runtimeMethod.bind(this.runtime) as NonNullable<Runtime[T]>;
  }

  private alignRowValuesToDeclaredSchema(
    table: string,
    values: Value[],
    runtimeSchema?: WasmSchema,
    arraySubqueries: ArraySubqueryPlan[] = [],
    selectColumns: string[] = [],
  ): Value[] {
    if (this.returnsDeclaredSchemaRows()) {
      return values;
    }

    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();
    const declaredTable = this.context.schema[table];
    const runtimeTable = effectiveRuntimeSchema[table];

    if (!declaredTable || !runtimeTable) {
      return values;
    }

    const projectedVisibleColumnCount =
      selectColumns.length > 0
        ? resolveSelectedColumns(table, this.context.schema, selectColumns).filter(
            (columnName) => !isHiddenIncludeColumnName(columnName),
          ).length
        : 0;

    if (projectedVisibleColumnCount > 0) {
      if (values.length < projectedVisibleColumnCount) {
        return values;
      }

      const projectedValues = values.slice(0, projectedVisibleColumnCount);
      const trailingValues = values.slice(projectedVisibleColumnCount);
      if (arraySubqueries.length === 0) {
        return projectedValues.concat(trailingValues);
      }

      const alignedTrailingValues = trailingValues.map((value, index) => {
        const plan = arraySubqueries[index];
        if (!plan) {
          return value;
        }
        return this.alignIncludedValueToDeclaredSchema(value, plan, effectiveRuntimeSchema);
      });

      return projectedValues.concat(alignedTrailingValues);
    }

    if (values.length < runtimeTable.columns.length) {
      return values;
    }

    const valuesByColumn = new Map<string, Value>();
    for (let index = 0; index < runtimeTable.columns.length; index += 1) {
      const column = runtimeTable.columns[index];
      if (!column) {
        return values;
      }
      const value = values[index];
      if (value === undefined) {
        return values;
      }
      valuesByColumn.set(column.name, value);
    }

    const reorderedValues: Value[] = [];
    for (const column of declaredTable.columns) {
      const value = valuesByColumn.get(column.name);
      if (value === undefined) {
        return values;
      }
      reorderedValues.push(value);
    }

    const trailingValues = values.slice(runtimeTable.columns.length);
    if (arraySubqueries.length === 0) {
      return reorderedValues.concat(trailingValues);
    }

    const alignedTrailingValues = trailingValues.map((value, index) => {
      const plan = arraySubqueries[index];
      if (!plan) {
        return value;
      }
      return this.alignIncludedValueToDeclaredSchema(value, plan, effectiveRuntimeSchema);
    });

    return reorderedValues.concat(alignedTrailingValues);
  }

  private alignIncludedValueToDeclaredSchema(
    value: Value,
    plan: ArraySubqueryPlan,
    runtimeSchema?: WasmSchema,
  ): Value {
    if (this.returnsDeclaredSchemaRows()) {
      return value;
    }

    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();
    if (value.type !== "Array") {
      return value;
    }

    return {
      ...value,
      value: value.value.map((entry) => {
        if (entry.type !== "Row") {
          return entry;
        }

        return {
          ...entry,
          value: {
            ...entry.value,
            values: this.alignRowValuesToDeclaredSchema(
              plan.table,
              entry.value.values,
              effectiveRuntimeSchema,
              plan.nested,
              plan.selectColumns,
            ),
          },
        };
      }),
    };
  }

  private alignQueryRowsToDeclaredSchema(
    queryJson: string,
    rows: Row[],
    runtimeSchema?: WasmSchema,
  ): Row[] {
    if (this.returnsDeclaredSchemaRows()) {
      return rows;
    }

    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();
    const { outputTable, arraySubqueries, selectColumns } = resolveQueryAlignmentPlan(queryJson);
    if (!outputTable) {
      return rows;
    }

    return rows.map((row) => ({
      ...row,
      values: this.alignRowValuesToDeclaredSchema(
        outputTable,
        row.values,
        effectiveRuntimeSchema,
        arraySubqueries,
        selectColumns,
      ),
    }));
  }

  private alignSubscriptionDeltaToDeclaredSchema(
    queryJson: string,
    delta: RowDelta,
    runtimeSchema?: WasmSchema,
  ): RowDelta {
    if (this.returnsDeclaredSchemaRows()) {
      return delta;
    }

    const effectiveRuntimeSchema = runtimeSchema ?? this.getSchema();
    const { outputTable, arraySubqueries, selectColumns } = resolveQueryAlignmentPlan(queryJson);
    if (!outputTable || !Array.isArray(delta)) {
      return delta;
    }

    return delta.map((change) => {
      if ((change.kind === 0 || change.kind === 2) && change.row) {
        return {
          ...change,
          row: {
            ...change.row,
            values: this.alignRowValuesToDeclaredSchema(
              outputTable,
              change.row.values as Value[],
              effectiveRuntimeSchema,
              arraySubqueries,
              selectColumns,
            ),
          },
        };
      }

      return change;
    });
  }

  /**
   * Insert a new row into a table without waiting for durability.
   */
  create(table: string, values: InsertValues, options?: CreateOptions): WriteResult<Row> {
    return this.createHandleInternal(table, values, undefined, undefined, options);
  }

  createHandleInternal(
    table: string,
    values: InsertValues,
    session?: Session,
    attribution?: string,
    options?: CreateOptions,
    batchContext?: BatchWriteContext,
  ): WriteResult<Row> {
    const row = this.createInternal(table, values, session, attribution, options, batchContext);
    if (!batchContext) {
      this.sealBatch(row.batchId);
    }
    return new WriteResult(row, row.batchId, this);
  }

  /**
   * Create or update a row with a caller-supplied id without waiting for durability.
   */
  upsert(table: string, values: InsertValues, options: UpsertOptions): WriteHandle {
    return this.upsertHandleInternal(
      table,
      values,
      options.id,
      undefined,
      undefined,
      options.updatedAt,
    );
  }

  upsertHandleInternal(
    table: string,
    values: InsertValues,
    objectId: string,
    session?: Session,
    attribution?: string,
    updatedAt?: number,
    batchContext?: BatchWriteContext,
  ): WriteHandle {
    const result = this.upsertInternal(
      table,
      values,
      objectId,
      session,
      attribution,
      updatedAt,
      batchContext,
    );
    if (!batchContext) {
      this.sealBatch(result.batchId);
    }
    return new WriteHandle(result.batchId, this);
  }

  /**
   * Insert a new row into a table with an optional session for policy checks.
   * @internal
   */
  createInternal(
    table: string,
    values: InsertValues,
    session?: Session,
    attribution?: string,
    options?: CreateOptions,
    batchContext?: BatchWriteContext,
  ): DirectInsertResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const row =
      effectiveSession ||
      attribution !== undefined ||
      batchContext ||
      options?.updatedAt !== undefined
        ? options?.id
          ? this.requireSessionWriteMethod("insertWithSession")(
              table,
              values,
              this.encodeWriteContext(
                effectiveSession,
                attribution,
                batchContext,
                options.updatedAt,
              ),
              options.id,
            )
          : this.requireSessionWriteMethod("insertWithSession")(
              table,
              values,
              this.encodeWriteContext(
                effectiveSession,
                attribution,
                batchContext,
                options?.updatedAt,
              ),
            )
        : options?.id
          ? this.runtime.insert(table, values, options.id)
          : this.runtime.insert(table, values);
    return {
      ...row,
      values: this.alignRowValuesToDeclaredSchema(table, row.values as Value[]),
    };
  }

  /**
   * Create or update a row with a caller-supplied id, optionally scoped to a session.
   * @internal
   */
  upsertInternal(
    table: string,
    values: InsertValues,
    objectId: string,
    session?: Session,
    attribution?: string,
    updatedAt?: number,
    batchContext?: BatchWriteContext,
  ): DirectMutationResult {
    try {
      const created = this.createInternal(
        table,
        values,
        session,
        attribution,
        {
          id: objectId,
          updatedAt,
        },
        batchContext,
      );
      return { batchId: created.batchId };
    } catch (error) {
      if (!shouldFallbackToUpsertUpdate(error)) {
        throw error;
      }
    }

    return this.updateInternal(
      objectId,
      values as Record<string, Value>,
      session,
      attribution,
      batchContext,
      updatedAt,
    );
  }

  /**
   * Execute a query and return all matching rows.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param options Optional read durability options
   * @returns Array of matching rows
   */
  async query(query: string | QueryInput, options?: QueryExecutionOptions): Promise<Row[]> {
    return this.queryInternal(query, this.resolvedSession ?? undefined, options);
  }

  /**
   * Internal query with optional session and read durability options.
   * @internal
   */
  async queryInternal(
    query: string | QueryInput,
    session?: Session,
    options?: InternalQueryExecutionOptions,
    runtimeSchema?: WasmSchema,
  ): Promise<Row[]> {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const queryJson = resolveQueryJson(query);
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const effectiveRuntimeSchema =
      runtimeSchema ?? (this.returnsDeclaredSchemaRows() ? undefined : this.getSchema());
    const results = await this.runtime.query(
      queryJson,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );
    return this.alignQueryRowsToDeclaredSchema(queryJson, results as Row[], effectiveRuntimeSchema);
  }

  /**
   * Update a row by ID without waiting for durability.
   */
  update(objectId: string, updates: Record<string, Value>, options?: UpdateOptions): WriteHandle {
    return this.updateHandleInternal(
      objectId,
      updates,
      undefined,
      undefined,
      undefined,
      options?.updatedAt,
    );
  }

  updateHandleInternal(
    objectId: string,
    updates: Record<string, Value>,
    session?: Session,
    attribution?: string,
    batchContext?: BatchWriteContext,
    updatedAt?: number,
  ): WriteHandle {
    const result = this.updateInternal(
      objectId,
      updates,
      session,
      attribution,
      batchContext,
      updatedAt,
    );
    if (!batchContext) {
      this.sealBatch(result.batchId);
    }
    return new WriteHandle(result.batchId, this);
  }

  /**
   * Update a row by ID without waiting for durability, optionally scoped to a session.
   * @internal
   */
  updateInternal(
    objectId: string,
    updates: Record<string, Value>,
    session?: Session,
    attribution?: string,
    batchContext?: BatchWriteContext,
    updatedAt?: number,
  ): DirectMutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined || batchContext || updatedAt !== undefined) {
      return this.requireSessionWriteMethod("updateWithSession")(
        objectId,
        updates,
        this.encodeWriteContext(effectiveSession, attribution, batchContext, updatedAt),
      );
    }
    return this.runtime.update(objectId, updates);
  }

  /**
   * Delete a row by ID without waiting for durability.
   */
  delete(objectId: string): WriteHandle {
    return this.deleteHandleInternal(objectId);
  }

  deleteHandleInternal(
    objectId: string,
    session?: Session,
    attribution?: string,
    batchContext?: BatchWriteContext,
    updatedAt?: number,
  ): WriteHandle {
    const result = this.deleteInternal(objectId, session, attribution, batchContext, updatedAt);
    if (!batchContext) {
      this.sealBatch(result.batchId);
    }
    return new WriteHandle(result.batchId, this);
  }

  /**
   * Delete a row by ID without waiting for durability, optionally scoped to a session.
   * @internal
   */
  deleteInternal(
    objectId: string,
    session?: Session,
    attribution?: string,
    batchContext?: BatchWriteContext,
    updatedAt?: number,
  ): DirectMutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined || batchContext || updatedAt !== undefined) {
      return this.requireSessionWriteMethod("deleteWithSession")(
        objectId,
        this.encodeWriteContext(effectiveSession, attribution, batchContext, updatedAt),
      );
    }
    return this.runtime.delete(objectId);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * @param query Query builder or JSON-encoded query specification
   * @param callback Called with delta whenever results change
   * @param options Optional read durability options
   * @returns Subscription ID for unsubscribing
   */
  subscribe(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    options?: QueryExecutionOptions,
  ): number {
    return this.subscribeInternal(query, callback, this.resolvedSession ?? undefined, options);
  }

  /**
   * Internal subscribe with optional session and read durability options.
   *
   * Uses the runtime's 2-phase subscribe API: `createSubscription` allocates
   * a handle synchronously (zero work), then `executeSubscription` is deferred
   * via the scheduler so compilation + first tick run outside the caller's
   * synchronous stack (e.g. outside a React render).
   *
   * @internal
   */
  subscribeInternal(
    query: string | QueryInput,
    callback: SubscriptionCallback,
    session?: Session,
    options?: QueryExecutionOptions,
    runtimeSchema?: WasmSchema,
  ): number {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const queryJson = resolveQueryJson(query);
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const effectiveRuntimeSchema =
      runtimeSchema ?? (this.returnsDeclaredSchemaRows() ? undefined : this.getSchema());

    const handle = this.runtime.createSubscription(
      queryJson,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );

    this.scheduler(() => {
      this.runtime.executeSubscription(handle, (...args: unknown[]) => {
        const deltaJsonOrObject = normalizeSubscriptionCallbackArgs(args);
        if (deltaJsonOrObject === undefined) {
          return;
        }

        const delta: RowDelta =
          typeof deltaJsonOrObject === "string" ? JSON.parse(deltaJsonOrObject) : deltaJsonOrObject;
        callback(
          this.alignSubscriptionDeltaToDeclaredSchema(queryJson, delta, effectiveRuntimeSchema),
        );
      });
    });

    return handle;
  }

  /**
   * Unsubscribe from a query.
   *
   * @param subscriptionId ID returned from subscribe()
   */
  unsubscribe(subscriptionId: number): void {
    this.runtime.unsubscribe(subscriptionId);
  }

  /**
   * Connect to a Jazz server over WebSocket using the Rust transport layer.
   *
   * Accepts an HTTP/HTTPS server URL (e.g. "http://localhost:4000") and
   * converts it to the corresponding WebSocket `/ws` endpoint URL before
   * passing it to the underlying Rust runtime's `connect()`.  Already-WS URLs
   * are passed through unchanged.
   *
   * @param url  Server URL — http(s):// or ws(s)://. `/apps/<appId>/ws` is appended automatically.
   * @param auth Authentication credentials for the connection.
   */
  connectTransport(url: string, auth: AuthConfig): void {
    if (!this.runtime.connect) {
      throw new Error("Underlying runtime does not support connect()");
    }
    this.runtime.connect(httpUrlToWs(url, this.context.appId), JSON.stringify(auth));
  }

  /**
   * Disconnect from the Jazz server and drop the Rust transport handle.
   *
   * No-op if the underlying runtime does not support disconnect().
   */
  disconnectTransport(): void {
    this.runtime.disconnect?.();
  }

  /**
   * Get the current schema.
   */
  getSchema(): WasmSchema {
    const schemaHash = this.runtime.getSchemaHash();
    if (this.cachedRuntimeSchemaHash === schemaHash && this.cachedRuntimeSchema) {
      return this.cachedRuntimeSchema;
    }

    const schema = normalizeRuntimeSchema(this.runtime.getSchema());
    this.cachedRuntimeSchemaHash = schemaHash;
    this.cachedRuntimeSchema = schema;
    return schema;
  }

  /**
   * Get the underlying runtime (for WorkerBridge).
   * @internal
   */
  getRuntime(): Runtime {
    return this.runtime;
  }

  /**
   * Get the server URL (for SessionClient).
   * @internal
   */
  getServerUrl(): string | undefined {
    return this.context.serverUrl;
  }

  /**
   * Build a fully-qualified endpoint URL against the configured server.
   * @internal
   */
  getRequestUrl(path: string): string {
    if (!this.context.serverUrl) {
      throw new Error("No server connection");
    }
    return appScopedUrl(this.context.serverUrl, this.context.appId, path);
  }

  /**
   * Get schema context for server requests.
   * @internal
   */
  getSchemaContext(): {
    env: string;
    schema_hash: string;
    user_branch: string;
  } {
    return {
      env: this.context.env ?? "dev",
      schema_hash: this.runtime.getSchemaHash(),
      user_branch: this.context.userBranch ?? "main",
    };
  }

  /**
   * Send an HTTP request with appropriate auth headers.
   * @internal
   */
  async sendRequest(
    url: string,
    method: string,
    body: unknown,
    session?: Session,
  ): Promise<Response> {
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };

    // Priority 1: Backend impersonation (via SessionClient)
    if (session && this.context.backendSecret) {
      headers["X-Jazz-Backend-Secret"] = this.context.backendSecret;
      headers["X-Jazz-Session"] = btoa(JSON.stringify(session));
    }
    // Priority 2: frontend auth (JWT bearer token)
    else {
      applyUserAuthHeaders(headers, {
        jwtToken: this.context.jwtToken,
      });
    }

    return fetch(url, {
      method,
      headers,
      body: JSON.stringify(body),
    });
  }

  private batchWaitOutcome(
    batchId: string,
    tier: DurabilityTier,
  ): { settled: true; error: Error | null } | { settled: false } {
    const acknowledgedRejection = this.acknowledgedRejectedBatchErrors.get(batchId);
    if (acknowledgedRejection) {
      return { settled: true, error: acknowledgedRejection };
    }

    const settlement = this.localBatchRecord(batchId)?.latestSettlement;
    const rejection = rejectionFromSettlement(settlement);
    if (rejection) {
      return { settled: true, error: rejection };
    }
    if (settlementSatisfiesTier(settlement, tier)) {
      return { settled: true, error: null };
    }

    return { settled: false };
  }

  private flushPendingBatchWaiters(): void {
    if (this.pendingBatchWaiters.size === 0) {
      return;
    }

    const rejectedBatchIdsHandledByWaiters = new Set<string>();
    for (const [batchId, waiters] of this.pendingBatchWaiters) {
      const remaining: typeof waiters = [];
      for (const waiter of waiters) {
        const outcome = this.batchWaitOutcome(batchId, waiter.tier);
        if (!outcome.settled) {
          remaining.push(waiter);
          continue;
        }
        if (outcome.error) {
          waiter.reject(outcome.error);
          rejectedBatchIdsHandledByWaiters.add(batchId);
        } else {
          waiter.resolve();
        }
      }
      if (remaining.length > 0) {
        this.pendingBatchWaiters.set(batchId, remaining);
      } else {
        this.pendingBatchWaiters.delete(batchId);
      }
    }

    for (const batchId of rejectedBatchIdsHandledByWaiters) {
      this.acknowledgeRejectedBatchInternal(batchId);
    }
  }

  private shouldPollSettlements(): boolean {
    return this.pendingBatchWaiters.size > 0 || this.mutationErrorListeners.size > 0;
  }

  private ensureSettlementPolling(): void {
    if (this.settlementPollTimer !== null) {
      return;
    }
    if (!this.shouldPollSettlements()) {
      return;
    }

    // Rust-owned transports can settle batch records without routing through
    // JS `onSyncMessageReceived`, so poll retained batch metadata while waits
    // or mutation error listeners are outstanding.
    this.settlementPollTimer = setTimeout(() => {
      this.settlementPollTimer = null;
      const batchesWithPendingWaiters = new Set(this.pendingBatchWaiters.keys());
      this.flushPendingBatchWaiters();
      this.flushUnhandledMutationErrors(this.drainRejectedBatchIds(), batchesWithPendingWaiters);

      this.ensureSettlementPolling();
    }, 20);
  }

  private cancelSettlementPolling(): void {
    if (this.settlementPollTimer === null) {
      return;
    }

    clearTimeout(this.settlementPollTimer);
    this.settlementPollTimer = null;
  }

  private flushUnhandledMutationErrors(
    rejectedBatchIds: readonly string[] = this.drainRejectedBatchIds(),
    batchesHandledByLiveWaiters: ReadonlySet<string> = new Set<string>(),
  ): void {
    for (const batchId of rejectedBatchIds) {
      const record = this.localBatchRecord(batchId);
      if (!record) {
        continue;
      }
      const settlement = record.latestSettlement;
      if (!settlement || settlement.kind !== "rejected") {
        continue;
      }
      if (batchesHandledByLiveWaiters.has(record.batchId)) {
        continue;
      }
      if ((this.pendingBatchWaiters.get(record.batchId)?.length ?? 0) > 0) {
        continue;
      }

      const event: MutationErrorEvent = {
        code: settlement.code,
        reason: settlement.reason,
        batch: record,
      };

      if (this.mutationErrorListeners.size === 0) {
        // If there are no listeners configured, we log the error to the console by default.
        console.error("Unhandled Jazz mutation error", event);
      } else {
        for (const listener of this.mutationErrorListeners) {
          listener(event);
        }
      }

      this.acknowledgeRejectedBatchInternal(record.batchId);
    }
  }

  private drainRejectedBatchIds(): string[] {
    const drainRejectedBatchIds = this.runtime.drainRejectedBatchIds;
    if (!drainRejectedBatchIds) {
      return [];
    }
    return [...new Set(drainRejectedBatchIds.call(this.runtime))].sort();
  }

  waitForPersistedBatch(batchId: string, tier: DurabilityTier): Promise<void> {
    const outcome = this.batchWaitOutcome(batchId, tier);
    if (outcome.settled) {
      return outcome.error ? Promise.reject(outcome.error) : Promise.resolve();
    }

    return new Promise<void>((resolve, reject) => {
      const waiters = this.pendingBatchWaiters.get(batchId) ?? [];
      waiters.push({ tier, resolve, reject });
      this.pendingBatchWaiters.set(batchId, waiters);
      this.flushPendingBatchWaiters();
      this.ensureSettlementPolling();
    });
  }

  /**
   * Shutdown the client and release resources.
   */
  async shutdown(): Promise<void> {
    if (this.shutdownPromise) {
      return await this.shutdownPromise;
    }

    this.shutdownPromise = (async () => {
      this.cancelSettlementPolling();

      // Disconnect Rust-owned transport if present.
      this.runtime.disconnect?.();

      // Close runtime if it supports explicit shutdown (e.g., NapiRuntime).
      if (this.runtime.close) {
        await this.runtime.close();
      }
    })();

    return await this.shutdownPromise;
  }
}

/**
 * WASM module type for sync client creation.
 * This is the type of the jazz-wasm module after dynamic import.
 */
export type WasmModule = typeof import("jazz-wasm");

async function tryLoadNodePackagedWasmBinary(): Promise<Uint8Array | null> {
  const moduleBuiltin = process.getBuiltinModule?.("module");
  const fsBuiltin = process.getBuiltinModule?.("fs");
  const pathBuiltin = process.getBuiltinModule?.("path");

  if (!moduleBuiltin || !fsBuiltin || !pathBuiltin) {
    return null;
  }

  const { createRequire } = moduleBuiltin;
  const { existsSync, readFileSync } = fsBuiltin;
  const { dirname, resolve } = pathBuiltin;

  const require = createRequire(import.meta.url);
  const packageJsonPath = require.resolve("jazz-wasm/package.json");
  const packageDir = dirname(packageJsonPath);
  const wasmPath = resolve(packageDir, "pkg/jazz_wasm_bg.wasm");

  if (!existsSync(wasmPath)) {
    return null;
  }

  return readFileSync(wasmPath);
}

/**
 * Load and initialize the WASM module.
 *
 * Exported so that `createDb()` can pre-load the module for sync mutations.
 */
export async function loadWasmModule(runtime?: RuntimeSourcesConfig): Promise<WasmModule> {
  // Cast to any — wasm-bindgen glue exports (default, initSync) aren't in .d.ts
  const wasmModule: any = await import("jazz-wasm");
  const syncInitInput = resolveRuntimeConfigSyncInitInput(runtime);

  if (syncInitInput) {
    wasmModule.initSync(syncInitInput);
    return wasmModule;
  }

  // In Node.js, we need to read the .wasm file and use initSync.
  // In browsers/React Native, the default fetch-based init works (or default()).
  // Use try/catch so we skip the Node path when node:* modules are unavailable (e.g. RN).
  let nodeInitDone = false;
  if (typeof process !== "undefined" && process.versions?.node) {
    try {
      const wasmBinary = await tryLoadNodePackagedWasmBinary();
      if (wasmBinary) {
        wasmModule.initSync({ module: wasmBinary });
        nodeInitDone = true;
      }
    } catch {
      // Node modules unavailable (e.g. React Native with process polyfill)
    }
  }
  if (!nodeInitDone && typeof wasmModule.default === "function") {
    const wasmUrl =
      typeof location !== "undefined"
        ? resolveRuntimeConfigWasmUrl(import.meta.url, location.href, runtime)
        : null;

    if (wasmUrl) {
      await wasmModule.default({ module_or_path: wasmUrl });
    } else {
      await wasmModule.default();
    }
  }

  return wasmModule;
}
