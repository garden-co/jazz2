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
  buildEndpointUrl,
  applyUserAuthHeaders,
  type AuthFailureReason,
} from "./sync-transport.js";
import { resolveClientSessionStateSync } from "./client-session.js";
import { mapAuthReason } from "./auth-state.js";
import { translateQuery } from "./query-adapter.js";
import { isHiddenIncludeColumnName, resolveSelectedColumns } from "./select-projection.js";
import {
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "./runtime-config.js";
import { httpUrlToWs } from "./url.js";

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
  insert(table: string, values: InsertValues, object_id?: string | null): Row;
  insertWithSession?(
    table: string,
    values: InsertValues,
    write_context_json?: string | null,
    object_id?: string | null,
  ): Row;
  insertDurable(
    table: string,
    values: InsertValues,
    tier: string,
    object_id?: string | null,
  ): Promise<Row>;
  insertDurableWithSession?(
    table: string,
    values: InsertValues,
    write_context_json: string | null | undefined,
    tier: string,
    object_id?: string | null,
  ): Promise<Row>;
  update(object_id: string, values: Record<string, Value>): void;
  updateWithSession?(
    object_id: string,
    values: Record<string, Value>,
    write_context_json?: string | null,
  ): void;
  updateDurable(object_id: string, values: Record<string, Value>, tier: string): Promise<void>;
  updateDurableWithSession?(
    object_id: string,
    values: Record<string, Value>,
    write_context_json: string | null | undefined,
    tier: string,
  ): Promise<void>;
  delete(object_id: string): void;
  deleteWithSession?(object_id: string, write_context_json?: string | null): void;
  deleteDurable(object_id: string, tier: string): Promise<void>;
  deleteDurableWithSession?(
    object_id: string,
    write_context_json: string | null | undefined,
    tier: string,
  ): Promise<void>;
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
  addServer(serverCatalogueStateHash?: string | null, nextSyncSeq?: number | null): void;
  removeServer(): void;
  addClient(): string;
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
export type LocalUpdatesMode = "immediate" | "deferred";
export type QueryPropagation = "full" | "local-only";
export type QueryVisibility = "public" | "hidden_from_live_query_list";
export interface QueryExecutionOptions {
  tier?: DurabilityTier;
  localUpdates?: LocalUpdatesMode;
  propagation?: QueryPropagation;
  strictTransactions?: boolean;
  visibility?: QueryVisibility;
}

export interface ResolvedQueryExecutionOptions {
  tier: DurabilityTier;
  localUpdates: LocalUpdatesMode;
  propagation: QueryPropagation;
  strictTransactions: boolean;
  visibility: QueryVisibility;
}

export interface WriteDurabilityOptions {
  tier?: DurabilityTier;
}

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
      kind: "durable_direct";
      batchId: string;
      confirmedTier: DurabilityTier;
      visibleMembers: VisibleBatchMember[];
    }
  | {
      kind: "accepted_transaction";
      batchId: string;
      confirmedTier: DurabilityTier;
      visibleMembers: VisibleBatchMember[];
    };

export interface LocalBatchRecord {
  batchId: string;
  mode: BatchMode;
  requestedTier: DurabilityTier;
  sealed: boolean;
  latestSettlement: BatchSettlement | null;
}

export interface CreateOptions extends TimestampOverrideOptions {
  id?: string;
}

export interface CreateDurabilityOptions extends WriteDurabilityOptions, TimestampOverrideOptions {
  id?: string;
}

export interface UpsertOptions extends TimestampOverrideOptions {
  id: string;
}

export interface UpsertDurabilityOptions extends WriteDurabilityOptions, TimestampOverrideOptions {
  id: string;
}

export interface UpdateOptions extends TimestampOverrideOptions {}

export interface UpdateDurabilityOptions extends WriteDurabilityOptions, TimestampOverrideOptions {}

/**
 * Query row result.
 */
export interface Row {
  id: string;
  values: Value[];
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
    strictTransactions: options?.strictTransactions ?? false,
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

function encodeQueryExecutionOptions(options: QueryExecutionOptions): string | undefined {
  const payload: {
    propagation?: QueryPropagation;
    local_updates?: LocalUpdatesMode;
    strict_transactions?: boolean;
  } = {};
  if ((options.propagation ?? "full") !== "full") {
    payload.propagation = options.propagation;
  }
  if ((options.localUpdates ?? "immediate") !== "immediate") {
    payload.local_updates = options.localUpdates;
  }
  if (options.strictTransactions) {
    payload.strict_transactions = true;
  }

  if (!payload.propagation && !payload.local_updates && !payload.strict_transactions) {
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

  const typedPayload = payload as { sub?: unknown; claims?: unknown };
  if (typeof typedPayload.sub !== "string" || typedPayload.sub.length === 0) {
    throw new Error("JWT payload missing sub");
  }

  const claims =
    typedPayload.claims &&
    typeof typedPayload.claims === "object" &&
    !Array.isArray(typedPayload.claims)
      ? (typedPayload.claims as Record<string, unknown>)
      : {};

  return { user_id: typedPayload.sub, claims };
}

function isObjectAlreadyExistsError(error: unknown): boolean {
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

  if (settlement.kind !== "durable_direct" && settlement.kind !== "accepted_transaction") {
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

export class PersistedWrite<T> {
  constructor(
    private readonly client: JazzClient,
    private readonly requestedTier: DurabilityTier,
    private readonly persistedBatchId: string,
    private readonly persistedValue: T,
  ) {}

  batchId(): string {
    return this.persistedBatchId;
  }

  value(): T {
    return this.persistedValue;
  }

  async wait(): Promise<T> {
    await this.client.waitForPersistedBatch(this.persistedBatchId, this.requestedTier);
    return this.persistedValue;
  }
}

export class Transaction {
  private committed = false;

  constructor(
    private readonly client: JazzClient,
    private readonly batchContext: BatchWriteContext,
    private readonly session?: Session,
    private readonly attribution?: string,
  ) {}

  private ensureWritable(): void {
    if (this.committed) {
      throw new Error(`Transaction ${this.batchContext.batchId} is already committed`);
    }
  }

  batchId(): string {
    return this.batchContext.batchId;
  }

  commit(): string {
    if (this.committed) {
      return this.batchId();
    }
    const batchId = this.client.sealBatch(this.batchId());
    this.committed = true;
    return batchId;
  }

  create(table: string, values: InsertValues): Row {
    this.ensureWritable();
    return this.client.createInternal(
      table,
      values,
      this.session,
      this.attribution,
      undefined,
      this.batchContext,
    );
  }

  createPersisted(
    table: string,
    values: InsertValues,
    options?: WriteDurabilityOptions,
  ): PersistedWrite<Row> {
    this.ensureWritable();
    return this.client.createPersistedInternal(
      table,
      values,
      this.session,
      this.attribution,
      options,
      this.batchContext,
    );
  }

  update(objectId: string, updates: Record<string, Value>): void {
    this.ensureWritable();
    this.client.updateInternal(
      objectId,
      updates,
      this.session,
      this.attribution,
      this.batchContext,
    );
  }

  updatePersisted(
    objectId: string,
    updates: Record<string, Value>,
    options?: WriteDurabilityOptions,
  ): PersistedWrite<void> {
    this.ensureWritable();
    return this.client.updatePersistedInternal(
      objectId,
      updates,
      this.session,
      this.attribution,
      options,
      this.batchContext,
    );
  }

  delete(objectId: string): void {
    this.ensureWritable();
    this.client.deleteInternal(objectId, this.session, this.attribution, this.batchContext);
  }

  deletePersisted(objectId: string, options?: WriteDurabilityOptions): PersistedWrite<void> {
    this.ensureWritable();
    return this.client.deletePersistedInternal(
      objectId,
      this.session,
      this.attribution,
      options,
      this.batchContext,
    );
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

export class DirectBatch {
  constructor(
    private readonly client: JazzClient,
    private readonly batchContext: BatchWriteContext,
    private readonly session?: Session,
    private readonly attribution?: string,
  ) {}

  batchId(): string {
    return this.batchContext.batchId;
  }

  create(table: string, values: InsertValues): Row {
    return this.client.createInternal(
      table,
      values,
      this.session,
      this.attribution,
      undefined,
      this.batchContext,
    );
  }

  createPersisted(
    table: string,
    values: InsertValues,
    options?: WriteDurabilityOptions,
  ): PersistedWrite<Row> {
    return this.client.createPersistedInternal(
      table,
      values,
      this.session,
      this.attribution,
      options,
      this.batchContext,
    );
  }

  update(objectId: string, updates: Record<string, Value>): void {
    this.client.updateInternal(
      objectId,
      updates,
      this.session,
      this.attribution,
      this.batchContext,
    );
  }

  updatePersisted(
    objectId: string,
    updates: Record<string, Value>,
    options?: WriteDurabilityOptions,
  ): PersistedWrite<void> {
    return this.client.updatePersistedInternal(
      objectId,
      updates,
      this.session,
      this.attribution,
      options,
      this.batchContext,
    );
  }

  delete(objectId: string): void {
    this.client.deleteInternal(objectId, this.session, this.attribution, this.batchContext);
  }

  deletePersisted(objectId: string, options?: WriteDurabilityOptions): PersistedWrite<void> {
    return this.client.deletePersistedInternal(
      objectId,
      this.session,
      this.attribution,
      options,
      this.batchContext,
    );
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

  createPersisted(
    table: string,
    values: InsertValues,
    options?: WriteDurabilityOptions,
  ): PersistedWrite<Row> {
    return this.client.createPersistedInternal(table, values, this.session, undefined, options);
  }

  /**
   * Create or update a row as this session's user using a caller-supplied id.
   */
  async upsert(table: string, values: InsertValues, options: UpsertOptions): Promise<void> {
    try {
      await this.create(table, values, options);
      return;
    } catch (error) {
      if (!isObjectAlreadyExistsError(error)) {
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

  updatePersisted(
    objectId: string,
    updates: Record<string, Value>,
    options?: WriteDurabilityOptions,
  ): PersistedWrite<void> {
    return this.client.updatePersistedInternal(objectId, updates, this.session, undefined, options);
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

  deletePersisted(objectId: string, options?: WriteDurabilityOptions): PersistedWrite<void> {
    return this.client.deletePersistedInternal(objectId, this.session, undefined, options);
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

  beginDirectBatch(): DirectBatch {
    return this.client.beginDirectBatchInternal(this.session);
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
  private readonly pendingBatchWaiters = new Map<
    string,
    Array<{
      tier: DurabilityTier;
      resolve: () => void;
      reject: (error: Error) => void;
    }>
  >();
  private readonly acknowledgedRejectedBatchErrors = new Map<string, PersistedWriteRejectedError>();
  private shutdownPromise: Promise<void> | null = null;

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
            value.call(target, payload, seq);
            this.flushPendingBatchWaiters();
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

  beginDirectBatch(): DirectBatch {
    return this.beginDirectBatchInternal();
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

  beginDirectBatchInternal(session?: Session, attribution?: string): DirectBatch {
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

  acknowledgeRejectedBatch(batchId: string): boolean {
    const rejection = rejectionFromSettlement(this.localBatchRecord(batchId)?.latestSettlement);
    const acknowledged = this.requireBatchRecordMethod("acknowledgeRejectedBatch")(batchId);
    if (acknowledged && rejection) {
      this.acknowledgedRejectedBatchErrors.set(batchId, rejection);
    }
    this.flushPendingBatchWaiters();
    return acknowledged;
  }

  sealBatch(batchId: string): string {
    this.requireBatchRecordMethod("sealBatch")(batchId);
    return batchId;
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
    options?: QueryExecutionOptions,
  ): ResolvedQueryExecutionOptions {
    return resolveEffectiveQueryExecutionOptions(
      { ...this.context, defaultDurabilityTier: this.defaultDurabilityTier },
      options,
    );
  }

  private resolveWriteTier(options?: WriteDurabilityOptions): DurabilityTier {
    return options?.tier ?? this.defaultDurabilityTier;
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
    T extends keyof Pick<
      Runtime,
      | "insertWithSession"
      | "insertDurableWithSession"
      | "updateWithSession"
      | "updateDurableWithSession"
      | "deleteWithSession"
      | "deleteDurableWithSession"
    >,
  >(method: T): NonNullable<Runtime[T]> {
    const runtimeMethod = this.runtime[method];
    if (!runtimeMethod) {
      throw new Error(`${String(method)} is not supported by this runtime`);
    }
    return runtimeMethod.bind(this.runtime) as NonNullable<Runtime[T]>;
  }

  private requirePersistedWriteMethod<
    T extends keyof Pick<
      Runtime,
      | "insertPersisted"
      | "insertPersistedWithSession"
      | "updatePersisted"
      | "updatePersistedWithSession"
      | "deletePersisted"
      | "deletePersistedWithSession"
    >,
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
    runtimeSchema = this.getSchema(),
    arraySubqueries: ArraySubqueryPlan[] = [],
    selectColumns: string[] = [],
  ): Value[] {
    const declaredTable = this.context.schema[table];
    const runtimeTable = runtimeSchema[table];

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
        return this.alignIncludedValueToDeclaredSchema(value, plan, runtimeSchema);
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
      return this.alignIncludedValueToDeclaredSchema(value, plan, runtimeSchema);
    });

    return reorderedValues.concat(alignedTrailingValues);
  }

  private alignIncludedValueToDeclaredSchema(
    value: Value,
    plan: ArraySubqueryPlan,
    runtimeSchema = this.getSchema(),
  ): Value {
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
              runtimeSchema,
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
    runtimeSchema = this.getSchema(),
  ): Row[] {
    const { outputTable, arraySubqueries, selectColumns } = resolveQueryAlignmentPlan(queryJson);
    if (!outputTable) {
      return rows;
    }

    return rows.map((row) => ({
      ...row,
      values: this.alignRowValuesToDeclaredSchema(
        outputTable,
        row.values,
        runtimeSchema,
        arraySubqueries,
        selectColumns,
      ),
    }));
  }

  private alignSubscriptionDeltaToDeclaredSchema(
    queryJson: string,
    delta: RowDelta,
    runtimeSchema = this.getSchema(),
  ): RowDelta {
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
              runtimeSchema,
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
  create(table: string, values: InsertValues, options?: CreateOptions): Row {
    return this.createInternal(table, values, undefined, undefined, options);
  }

  /**
   * Create or update a row with a caller-supplied id without waiting for durability.
   */
  upsert(table: string, values: InsertValues, options: UpsertOptions): void {
    this.upsertInternal(table, values, options.id, undefined, undefined, options.updatedAt);
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
  ): Row {
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
      values: this.alignRowValuesToDeclaredSchema(table, row.values as Value[], this.getSchema()),
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
  ): void {
    try {
      this.createInternal(table, values, session, attribution, {
        id: objectId,
        updatedAt,
      });
      return;
    } catch (error) {
      if (!isObjectAlreadyExistsError(error)) {
        throw error;
      }
    }

    this.updateInternal(
      objectId,
      values as Record<string, Value>,
      session,
      attribution,
      undefined,
      updatedAt,
    );
  }

  /**
   * Insert a new row into a table and wait for durability at the requested tier.
   */
  async createDurable(
    table: string,
    values: InsertValues,
    options?: CreateDurabilityOptions,
  ): Promise<Row> {
    return this.createDurableInternal(table, values, undefined, undefined, options);
  }

  /**
   * Create or update a row with a caller-supplied id and wait for durability.
   */
  async upsertDurable(
    table: string,
    values: InsertValues,
    options: UpsertDurabilityOptions,
  ): Promise<void> {
    await this.upsertDurableInternal(table, values, options.id, undefined, undefined, options);
  }

  /**
   * Insert a new row into a table and wait for durability, optionally scoped to a session.
   * @internal
   */
  async createDurableInternal(
    table: string,
    values: InsertValues,
    session?: Session,
    attribution?: string,
    options?: CreateDurabilityOptions,
  ): Promise<Row> {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const row =
      effectiveSession || attribution !== undefined || options?.updatedAt !== undefined
        ? options?.id
          ? await this.requireSessionWriteMethod("insertDurableWithSession")(
              table,
              values,
              this.encodeWriteContext(effectiveSession, attribution, undefined, options.updatedAt),
              tier,
              options.id,
            )
          : await this.requireSessionWriteMethod("insertDurableWithSession")(
              table,
              values,
              this.encodeWriteContext(effectiveSession, attribution, undefined, options?.updatedAt),
              tier,
            )
        : options?.id
          ? await this.runtime.insertDurable(table, values, tier, options.id)
          : await this.runtime.insertDurable(table, values, tier);
    return {
      ...row,
      values: this.alignRowValuesToDeclaredSchema(table, row.values as Value[], this.getSchema()),
    };
  }

  createPersisted(
    table: string,
    values: InsertValues,
    options?: WriteDurabilityOptions,
  ): PersistedWrite<Row> {
    return this.createPersistedInternal(table, values, undefined, undefined, options);
  }

  createPersistedInternal(
    table: string,
    values: InsertValues,
    session?: Session,
    attribution?: string,
    options?: WriteDurabilityOptions,
    batchContext?: BatchWriteContext,
  ): PersistedWrite<Row> {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const result =
      effectiveSession || attribution !== undefined || batchContext
        ? this.requirePersistedWriteMethod("insertPersistedWithSession")(
            table,
            values,
            this.encodeWriteContext(effectiveSession, attribution, batchContext),
            tier,
          )
        : this.requirePersistedWriteMethod("insertPersisted")(table, values, tier);
    return new PersistedWrite(this, tier, result.batchId, {
      ...result.row,
      values: this.alignRowValuesToDeclaredSchema(
        table,
        result.row.values as Value[],
        this.getSchema(),
      ),
    });
  }

  /**
   * Create or update a row with a caller-supplied id and wait for durability,
   * optionally scoped to a session.
   * @internal
   */
  async upsertDurableInternal(
    table: string,
    values: InsertValues,
    objectId: string,
    session?: Session,
    attribution?: string,
    options?: UpsertDurabilityOptions,
  ): Promise<void> {
    try {
      await this.createDurableInternal(table, values, session, attribution, {
        ...options,
        id: objectId,
      });
      return;
    } catch (error) {
      if (!isObjectAlreadyExistsError(error)) {
        throw error;
      }
    }

    await this.updateDurableInternal(
      objectId,
      values as Record<string, Value>,
      session,
      attribution,
      options,
      options?.updatedAt,
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
    options?: QueryExecutionOptions,
  ): Promise<Row[]> {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const queryJson = resolveQueryJson(query);
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const runtimeSchema = this.getSchema();
    const results = await this.runtime.query(
      queryJson,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );
    return this.alignQueryRowsToDeclaredSchema(queryJson, results as Row[], runtimeSchema);
  }

  /**
   * Update a row by ID without waiting for durability.
   */
  update(objectId: string, updates: Record<string, Value>, options?: UpdateOptions): void {
    this.updateInternal(objectId, updates, undefined, undefined, undefined, options?.updatedAt);
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
  ): void {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined || batchContext || updatedAt !== undefined) {
      this.requireSessionWriteMethod("updateWithSession")(
        objectId,
        updates,
        this.encodeWriteContext(effectiveSession, attribution, batchContext, updatedAt),
      );
      return;
    }
    this.runtime.update(objectId, updates);
  }

  /**
   * Update a row by ID and wait for durability at the requested tier.
   */
  async updateDurable(
    objectId: string,
    updates: Record<string, Value>,
    options?: UpdateDurabilityOptions,
  ): Promise<void> {
    await this.updateDurableInternal(
      objectId,
      updates,
      undefined,
      undefined,
      options,
      options?.updatedAt,
    );
  }

  /**
   * Update a row by ID and wait for durability, optionally scoped to a session.
   * @internal
   */
  async updateDurableInternal(
    objectId: string,
    updates: Record<string, Value>,
    session?: Session,
    attribution?: string,
    options?: UpdateDurabilityOptions,
    updatedAt?: number,
  ): Promise<void> {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined || updatedAt !== undefined) {
      await this.requireSessionWriteMethod("updateDurableWithSession")(
        objectId,
        updates,
        this.encodeWriteContext(effectiveSession, attribution, undefined, updatedAt),
        tier,
      );
      return;
    }
    await this.runtime.updateDurable(objectId, updates, tier);
  }

  updatePersisted(
    objectId: string,
    updates: Record<string, Value>,
    options?: WriteDurabilityOptions,
  ): PersistedWrite<void> {
    return this.updatePersistedInternal(objectId, updates, undefined, undefined, options);
  }

  updatePersistedInternal(
    objectId: string,
    updates: Record<string, Value>,
    session?: Session,
    attribution?: string,
    options?: WriteDurabilityOptions,
    batchContext?: BatchWriteContext,
    updatedAt?: number,
  ): PersistedWrite<void> {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const result =
      effectiveSession || attribution !== undefined || batchContext || updatedAt !== undefined
        ? this.requirePersistedWriteMethod("updatePersistedWithSession")(
            objectId,
            updates,
            this.encodeWriteContext(effectiveSession, attribution, batchContext, updatedAt),
            tier,
          )
        : this.requirePersistedWriteMethod("updatePersisted")(objectId, updates, tier);
    return new PersistedWrite(this, tier, result.batchId, undefined);
  }

  /**
   * Delete a row by ID without waiting for durability.
   */
  delete(objectId: string): void {
    this.deleteInternal(objectId);
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
  ): void {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined || batchContext || updatedAt !== undefined) {
      this.requireSessionWriteMethod("deleteWithSession")(
        objectId,
        this.encodeWriteContext(effectiveSession, attribution, batchContext, updatedAt),
      );
      return;
    }
    this.runtime.delete(objectId);
  }

  /**
   * Delete a row by ID and wait for durability at the requested tier.
   */
  async deleteDurable(objectId: string, options?: WriteDurabilityOptions): Promise<void> {
    await this.deleteDurableInternal(objectId, undefined, undefined, options);
  }

  /**
   * Delete a row by ID and wait for durability, optionally scoped to a session.
   * @internal
   */
  async deleteDurableInternal(
    objectId: string,
    session?: Session,
    attribution?: string,
    options?: WriteDurabilityOptions,
    updatedAt?: number,
  ): Promise<void> {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    if (effectiveSession || attribution !== undefined || updatedAt !== undefined) {
      await this.requireSessionWriteMethod("deleteDurableWithSession")(
        objectId,
        this.encodeWriteContext(effectiveSession, attribution, undefined, updatedAt),
        tier,
      );
      return;
    }
    await this.runtime.deleteDurable(objectId, tier);
  }

  deletePersisted(objectId: string, options?: WriteDurabilityOptions): PersistedWrite<void> {
    return this.deletePersistedInternal(objectId, undefined, undefined, options);
  }

  deletePersistedInternal(
    objectId: string,
    session?: Session,
    attribution?: string,
    options?: WriteDurabilityOptions,
    batchContext?: BatchWriteContext,
    updatedAt?: number,
  ): PersistedWrite<void> {
    const tier = this.resolveWriteTier(options);
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const result =
      effectiveSession || attribution !== undefined || batchContext || updatedAt !== undefined
        ? this.requirePersistedWriteMethod("deletePersistedWithSession")(
            objectId,
            this.encodeWriteContext(effectiveSession, attribution, batchContext, updatedAt),
            tier,
          )
        : this.requirePersistedWriteMethod("deletePersisted")(objectId, tier);
    return new PersistedWrite(this, tier, result.batchId, undefined);
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
  ): number {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const sessionJson = session ? JSON.stringify(session) : undefined;
    const queryJson = resolveQueryJson(query);
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const runtimeSchema = this.getSchema();

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
        callback(this.alignSubscriptionDeltaToDeclaredSchema(queryJson, delta, runtimeSchema));
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
   * @param url        Server URL — http(s):// or ws(s)://. `/ws` is appended automatically.
   * @param auth       Authentication credentials for the connection.
   * @param pathPrefix Optional path prefix inserted before `/ws` (e.g. `/apps/<id>`).
   */
  connectTransport(url: string, auth: AuthConfig, pathPrefix?: string): void {
    if (!this.runtime.connect) {
      throw new Error("Underlying runtime does not support connect()");
    }
    this.runtime.connect(httpUrlToWs(url, pathPrefix), JSON.stringify(auth));
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
    return normalizeRuntimeSchema(this.runtime.getSchema());
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
    return buildEndpointUrl(this.context.serverUrl, path, this.context.serverPathPrefix);
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
