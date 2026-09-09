import { runtimeRandomBytes } from "./runtime-entropy.js";
/**
 * JazzClient - High-level TypeScript client for Jazz.
 *
 * Wraps the WASM runtime and provides a clean API for CRUD operations,
 * subscriptions, and sync.
 */

import type { AppContext, Session } from "./context.js";
import type {
  InsertValues,
  RuntimeSubscriptionDelta,
  Value,
  WasmSchema,
} from "../drivers/types.js";
import { normalizeRuntimeSchema } from "../drivers/schema-wire.js";
import type { AuthFailureReason } from "./auth-state.js";
import {
  TRUSTED_RESERVED_SESSION_TOKEN_FIELD,
  resolveClientSessionStateSync,
  trustedReservedSessionToken,
} from "./client-session.js";
import { getTrustedReservedSession, setTrustedReservedSession } from "./db-internal-session.js";
import { mapAuthReason } from "./auth-state.js";
import { httpUrlToWs } from "./url.js";

type RuntimeSerializedSession = Pick<
  Session,
  "account_id" | "issuer" | "user_id" | "claims" | "authMode"
> & {
  [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]?: string;
};

function serializeRuntimeSession(session: Session): RuntimeSerializedSession {
  const token = trustedReservedSessionToken(session);
  return {
    account_id: session.account_id,
    issuer: session.issuer,
    user_id: session.user_id,
    claims: session.claims,
    authMode: session.authMode,
    ...(token ? { [TRUSTED_RESERVED_SESSION_TOKEN_FIELD]: token } : {}),
  };
}

function encodeBranchColumnValue(value: Value): Uint8Array {
  const envelope = (tag: number, payload: Uint8Array): Uint8Array => {
    const encoded = new Uint8Array(2 + payload.length);
    encoded.set([1, tag]); // frozen branch-column codec version and scalar tag
    encoded.set(payload, 2);
    return encoded;
  };
  switch (value.type) {
    case "Integer":
      if (
        !Number.isSafeInteger(value.value) ||
        value.value < -0x80000000 ||
        value.value > 0x7fffffff
      ) {
        throw new Error("branch Integer values must be signed 32-bit integers");
      }
      {
        const payload = new Uint8Array(4);
        new DataView(payload.buffer).setInt32(0, value.value, true);
        return envelope(4, payload); // Groove I32
      }
    case "BigInt": {
      if (typeof value.value === "number" && !Number.isSafeInteger(value.value)) {
        throw new Error("branch BigInt values supplied as numbers must be safe integers");
      }
      const integer = BigInt(value.value);
      if (integer < -(1n << 63n) || integer > (1n << 63n) - 1n) {
        throw new Error("branch BigInt values must be signed 64-bit integers");
      }
      const payload = new Uint8Array(8);
      new DataView(payload.buffer).setBigInt64(0, integer, true);
      return envelope(5, payload); // Groove I64
    }
    case "Uuid": {
      const hex = value.value.replaceAll("-", "");
      if (!/^[0-9a-fA-F]{32}$/.test(hex)) throw new Error(`invalid branch UUID ${value.value}`);
      return envelope(
        7, // Groove UUID
        Uint8Array.from({ length: 16 }, (_, index) =>
          Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16),
        ),
      );
    }
    case "Text": {
      const text = new TextEncoder().encode(value.value);
      const payload = new Uint8Array(1 + text.length);
      payload[0] = 2; // Groove stored-scalar Primitive case
      payload.set(text, 1);
      return envelope(6, payload); // Groove String
    }
    default:
      throw new Error(
        `branch columns currently require Integer, BigInt, Text, or Uuid values; got ${value.type}`,
      );
  }
}

type WireBranchSelector = { values: Record<string, number[]> };
type WireBranchViewBase =
  | { Current: WireBranchSelector }
  | { Snapshot: { branch: WireBranchSelector; snapshot: unknown } };

function encodeBranchSelector(value: BranchSelector): WireBranchSelector {
  return {
    values: Object.fromEntries(
      Object.entries(value.values).map(([name, branchValue]) => [
        name,
        Array.from(encodeBranchColumnValue(branchValue)),
      ]),
    ),
  };
}

function encodeBranchViewBase(base: BranchViewBase | undefined): WireBranchViewBase | undefined {
  return base?.kind === "current"
    ? { Current: encodeBranchSelector(base.branch) }
    : base?.kind === "snapshot"
      ? { Snapshot: { branch: encodeBranchSelector(base.branch), snapshot: base.snapshot } }
      : undefined;
}

/**
 * Minimal request shape supported by backend request helpers.
 *
 * Works with common server frameworks (Express, Fastify, Hono, Web Request wrappers)
 * as long as Authorization headers are exposed through `header(name)` or `headers`.
 */
export interface RequestLike {
  header?: (name: string) => string | undefined;
  headers?: Headers | Record<string, string | string[] | undefined>;
}

/**
 * Common interface for the runtime backing `JazzClient`.
 */
export interface Runtime {
  insert(
    table: string,
    values: InsertValues,
    write_context_json?: string | null,
    object_id?: string | null,
  ): InsertResult;
  streamingMutation?(
    mutation: StreamingMutationKind,
    table: string,
    values: InsertValues,
    column: string,
    source: StreamingValueSource,
    write_context_json?: string | null,
    object_id?: string | null,
  ): Promise<StreamingInsertResult>;
  restore(
    table: string,
    object_id: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): InsertResult;
  update(
    table: string,
    object_id: string,
    values: Record<string, Value>,
    write_context_json?: string | null,
  ): MutationResult;
  /** Internal binding entrypoint for the typed page-edit update DSL. */
  updateLargeValues?(
    table: string,
    objectId: string,
    updates: Record<string, Value>,
    descriptors: readonly unknown[],
    writeContextJson?: string | null,
  ): MutationResult;
  upsert(
    table: string,
    object_id: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): MutationResult;
  delete(table: string, object_id: string, write_context_json?: string | null): MutationResult;
  canInsertLocally?(table: string, values: InsertValues, session?: Session): PermissionAdvice;
  canReadLocally?(table: string, objectId: string, session?: Session): PermissionAdvice;
  canUpdateLocally?(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): PermissionAdvice;
  canDeleteLocally?(table: string, objectId: string, session?: Session): PermissionAdvice;
  requestInsertPermissionAdvice?(
    table: string,
    values: InsertValues,
    session?: Session,
  ): Promise<PermissionAdvice>;
  requestReadPermissionAdvice?(
    table: string,
    objectId: string,
    session?: Session,
  ): Promise<PermissionAdvice>;
  requestUpdatePermissionAdvice?(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): Promise<PermissionAdvice>;
  requestDeletePermissionAdvice?(
    table: string,
    objectId: string,
    session?: Session,
  ): Promise<PermissionAdvice>;
  onMutationError(callback: (event: MutationErrorEvent) => void): void;
  waitForTransaction(txId: TxId | Promise<TxId>, tier: string): Promise<void>;
  query(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): Promise<any>;
  createSubscription(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
    options_json?: string | null,
  ): number;
  /** Install the subscription's callback and start delivery. May only be called once per handle. */
  executeSubscription(
    handle: number,
    onUpdate: (result: RuntimeSubscriptionDelta | Error) => void,
  ): void;
  unsubscribe(handle: number): void;
  close?(): void | Promise<void>;
  /** Abandon a runtime whose backing persistence epoch was invalidated. */
  discard?(): void;
  clearClientStorage?(): Promise<void>;
  /** Connect to a Jazz server over WebSocket (Rust transport). */
  connect(url: string, auth_json: string): void;
  /**
   * Disconnect from the Jazz server and drop the transport handle.
   *
   * Resolves once the runtime has completed the disconnect. For worker-backed
   * runtimes, this includes a round-trip in which the worker performs the
   * disconnect before replying.
   */
  disconnect(options?: { rejectWaiters?: boolean }): Promise<void>;
  /** Native admission owns its upstream independently of a JavaScript URL. */
  nativeUpstreamConfigured?(): boolean;
  /** Push updated auth credentials into the live Rust transport. */
  updateAuth(auth_json: string): void;
  /** Register a callback invoked when the Rust transport rejects the JWT. */
  onAuthFailure(callback: (reason: string) => void): void;
}

/**
 * Advisory result for a permission preflight. `allowed` and `denied` are
 * final only when a trusted-serving authority evaluated the request;
 * `unknown` means that a local replica or unavailable authority cannot decide.
 */
export type PermissionAdvice = "allowed" | "denied" | "unknown";

export interface TransactionalRuntime extends Runtime {
  beginTransaction(
    transactionKind: TransactionKind,
    id: OpenTransactionId,
    sessionJson?: string | null,
  ): OpenTransactionId;
  commitTransaction(id: OpenTransactionId): TxId | Promise<TxId>;
  rollbackTransaction(id: OpenTransactionId): Promise<boolean>;
}

declare const openTransactionIdBrand: unique symbol;
declare const txIdBrand: unique symbol;

/** Identity of a mutable transaction. Invalid after commit or rollback. */
export type OpenTransactionId = string & { readonly [openTransactionIdBrand]: true };
/** Immutable identity assigned to a successfully committed transaction. */
export type TxId = string & { readonly [txIdBrand]: true };

/** Generate a coordination-free UUIDv7 identity for a new mutable transaction. */
export function createOpenTransactionId(): OpenTransactionId {
  const bytes = runtimeRandomBytes(16);
  const timestamp = Date.now();
  bytes[0] = Math.floor(timestamp / 2 ** 40) & 0xff;
  bytes[1] = Math.floor(timestamp / 2 ** 32) & 0xff;
  bytes[2] = Math.floor(timestamp / 2 ** 24) & 0xff;
  bytes[3] = Math.floor(timestamp / 2 ** 16) & 0xff;
  bytes[4] = Math.floor(timestamp / 2 ** 8) & 0xff;
  bytes[5] = timestamp & 0xff;
  bytes[6] = (bytes[6]! & 0x0f) | 0x70;
  bytes[8] = (bytes[8]! & 0x3f) | 0x80;
  const hex = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
  return hex as OpenTransactionId;
}

/**
 * Authentication configuration for connecting to a Jazz server.
 *
 * Maps directly to the Rust `AuthConfig` struct in `jazz-tools/src/websocket_prelude_auth.rs`.
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
/** Product-facing policy for reads. It deliberately does not change write durability. */
export const ReadTier = {
  /** Cached local knowledge and pending writes; still syncs while connected. */
  LocalFirst: "local-first",
  /** Current remote query scope, without pending local writes; waits offline. */
  Remote: "remote",
  /** Remote scope plus pending scoped edits/new inserts online; local knowledge after explicit disconnect. */
  RemoteIfPossible: "remote-if-possible",
} as const;
export type ReadTier = (typeof ReadTier)[keyof typeof ReadTier];
/** @deprecated Read APIs also accept these legacy durability names unchanged. */
export type LegacyReadDurabilityTier = DurabilityTier;
export type QueryReadTier = ReadTier | LegacyReadDurabilityTier;
/** @internal Inspector-only tier that never subscribes upstream. */
type InternalQueryReadTier = QueryReadTier | "local-only";
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

/** Named values selecting one exact branch-local row coordinate. */
export interface BranchSelector {
  values: Record<string, Value>;
}

/** A live or application-resolved frozen base underneath a branch head. */
export type BranchViewBase =
  | { kind: "current"; branch: BranchSelector }
  | { kind: "snapshot"; branch: BranchSelector; snapshot: unknown };

/** User-facing head-over-base branch view. */
export interface BranchView {
  head: BranchSelector;
  base?: BranchViewBase;
}

export interface QueryExecutionOptions {
  /** `ReadTier.RemoteIfPossible` falls back only after an explicit disconnect. @deprecated DurabilityTier values remain accepted with their old meaning. */
  tier?: QueryReadTier;
  /** Admit exact-head history, falling back to an optional live or frozen base. */
  branch?: BranchView;
}

/**
 * Copy the product-facing subset of query options before crossing a public
 * JavaScript boundary.  TypeScript declarations are useful guidance, but a
 * caller can always supply an object through `as any` (or plain JavaScript).
 * In particular, transport propagation and own-write delivery are runtime
 * controls, not product API switches.
 *
 * @internal Db uses the analogous conversion when it lowers typed builders.
 */
export function publicQueryExecutionOptions(
  options?: QueryExecutionOptions,
): QueryExecutionOptions | undefined {
  if (!options) return undefined;
  const candidate = options as { tier?: unknown; branch?: unknown };
  const result: QueryExecutionOptions = {};
  if (isPublicQueryReadTier(candidate.tier)) result.tier = candidate.tier;
  if (candidate.branch !== undefined) result.branch = candidate.branch as BranchView;
  return result;
}

/** @internal `local-only` is deliberately excluded from the product surface. */
export function isPublicQueryReadTier(value: unknown): value is QueryReadTier {
  return (
    value === ReadTier.LocalFirst ||
    value === ReadTier.Remote ||
    value === ReadTier.RemoteIfPossible ||
    value === "local" ||
    value === "edge" ||
    value === "global"
  );
}

/** @internal Low-level read controls that are not part of the product-facing query API. */
export type InternalQueryExecutionOptions = Omit<QueryExecutionOptions, "tier"> & {
  tier?: InternalQueryReadTier;
  localUpdates?: LocalUpdatesMode;
  propagation?: QueryPropagation;
  visibility?: QueryVisibility;
  openTransactionId?: OpenTransactionId;
  runtimeSettledTier?: DurabilityTier | null;
};

export interface ResolvedQueryExecutionOptions {
  tier: DurabilityTier;
  localUpdates: LocalUpdatesMode;
  propagation: QueryPropagation;
  visibility: QueryVisibility;
  branch?: BranchView;
}

type ResolvedInternalQueryExecutionOptions = ResolvedQueryExecutionOptions & {
  openTransactionId?: OpenTransactionId;
};

interface TimestampOverrideOptions {
  updatedAt?: number;
}

/**
 * Selects the transaction semantics used for grouped writes.
 *
 * - `mergeable`: eventually-consistent writes that merge with concurrent writes.
 * - `exclusive`: serializable writes that are validated as one unit by the authority.
 */
export type TransactionKind = "mergeable" | "exclusive";

export type TransactionFate =
  | {
      kind: "missing";
      transactionId: TxId;
    }
  | {
      kind: "rejected";
      transactionId: TxId;
      code: string;
      reason: string;
    }
  | {
      kind: "accepted";
      transactionId: TxId;
      confirmedTier: DurabilityTier;
    };

export interface LocalTransactionRecord {
  transactionId: TxId;
  kind: TransactionKind;
  sealed: boolean;
  latestSettlement: TransactionFate;
  encodedRecord?: Uint8Array;
}

/**
 * A rejected write emitted by {@link JazzClient.onMutationError}.
 *
 * The event is a fallback for writes whose rejection was not handled by an
 * active {@link WriteHandle.wait} call.
 */
export interface MutationErrorEvent {
  code: string;
  reason: string;
  transaction: LocalTransactionRecord;
}

export interface InsertOptions extends TimestampOverrideOptions {
  id?: string;
  branch?: BranchSelector;
}

export interface UpdateOptions extends TimestampOverrideOptions {
  branch?: BranchView;
}

export interface DeleteOptions extends TimestampOverrideOptions {
  branch?: BranchView;
}

export interface RestoreOptions extends TimestampOverrideOptions {
  branch?: BranchSelector;
}

/**
 * Query row result.
 */
export interface Row {
  id: string;
  values: Value[];
}

export type WriteReceipt =
  | { readonly kind: "committed"; readonly txId: TxId | Promise<TxId> }
  | { readonly kind: "staged"; readonly openTransactionId: OpenTransactionId };

export type InsertResult = Row & WriteReceipt;
export type StreamingValueChunk = Uint8Array | string;
export type StreamingValueSource =
  | ReadableStream<StreamingValueChunk>
  | AsyncIterable<StreamingValueChunk>;
export type StreamingInsertResult = { id: string } & WriteReceipt;
export type StreamingMutationKind = "insert" | "update" | "upsert";
export type MutationResult = WriteReceipt;

interface WriteContextPayload {
  session?: Session;
  attribution?: string;
  updated_at?: number;
  transaction_id?: string;
  target_branch_name?: string;
  branch_view?: { head: WireBranchSelector; base?: WireBranchViewBase };
}

/**
 * Subscription callback type.
 *
 * The function form is retained for compatibility. Prefer
 * {@link SubscriptionCallbacks} so terminal failures are handled explicitly;
 * otherwise they are reported to `console.error`.
 */
export type SubscriptionCallback = (delta: RuntimeSubscriptionDelta) => void;
export interface SubscriptionCallbacks {
  /** Called for each native subscription delta. */
  onUpdate: SubscriptionCallback;
  /** Called once when the native subscription or update callback fails. */
  onError?: (error: Error) => void;
}

export interface ConnectRuntimeOptions {
  onAuthFailure?: (reason: AuthFailureReason) => void;
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
  options?: InternalQueryExecutionOptions,
): ResolvedQueryExecutionOptions {
  const selectedTier = options?.tier ?? resolveDefaultDurabilityTier(context);
  return {
    tier: resolveReadTier(selectedTier),
    localUpdates:
      options?.localUpdates ?? (selectedTier === ReadTier.Remote ? "deferred" : "immediate"),
    propagation: selectedTier === "local-only" ? "local-only" : (options?.propagation ?? "full"),
    visibility: options?.visibility ?? "public",
    branch: options?.branch,
  };
}

/** @internal Low-level runtimes retain the legacy three-tier wire contract. */
export function resolveReadTier(tier: InternalQueryReadTier): DurabilityTier {
  if (tier === "local-only") return "local";
  return tier === ReadTier.LocalFirst
    ? "local"
    : tier === ReadTier.Remote || tier === ReadTier.RemoteIfPossible
      ? "edge"
      : tier;
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
    transaction_id?: string;
    read_view?: {
      source: {
        BranchView: {
          head: WireBranchSelector;
          base?:
            | { Current: WireBranchSelector }
            | { Snapshot: { branch: WireBranchSelector; snapshot: unknown } };
        };
      };
    };
  } = {};
  if ((options.propagation ?? "full") !== "full") {
    payload.propagation = options.propagation;
  }
  if ((options.localUpdates ?? "immediate") !== "immediate") {
    payload.local_updates = options.localUpdates;
  }
  if (options.openTransactionId) {
    payload.transaction_id = options.openTransactionId;
  }
  if (options.branch) {
    const base = options.branch.base;
    payload.read_view = {
      source: {
        BranchView: {
          head: encodeBranchSelector(options.branch.head),
          base: encodeBranchViewBase(base),
        },
      },
    };
  }

  if (
    !payload.propagation &&
    !payload.local_updates &&
    !payload.transaction_id &&
    !payload.read_view
  ) {
    return undefined;
  }

  return JSON.stringify(payload);
}

function requireTransactionalRuntime(runtime: Runtime): TransactionalRuntime {
  if (
    typeof (runtime as Partial<TransactionalRuntime>).beginTransaction === "function" &&
    typeof (runtime as Partial<TransactionalRuntime>).commitTransaction === "function" &&
    typeof (runtime as Partial<TransactionalRuntime>).rollbackTransaction === "function"
  ) {
    return runtime as TransactionalRuntime;
  }

  throw new Error("This Jazz runtime does not support transactions");
}

function committedTxId(result: WriteReceipt): TxId | Promise<TxId> {
  if (result.kind !== "committed") {
    throw new Error(
      `Runtime returned staged transaction ${result.openTransactionId} for an ordinary write`,
    );
  }
  return result.txId;
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

function rejectionFromRuntimeWaitError(error: unknown): PersistedWriteRejectedError | null {
  if (!error || typeof error !== "object") {
    return null;
  }
  const candidate = error as {
    kind?: unknown;
    transactionId?: unknown;
    code?: unknown;
    reason?: unknown;
  };
  if (candidate.kind !== "rejected") {
    return null;
  }
  if (
    typeof candidate.code !== "string" ||
    typeof candidate.reason !== "string" ||
    typeof candidate.transactionId !== "string"
  ) {
    return null;
  }
  return new PersistedWriteRejectedError(
    candidate.transactionId as TxId,
    candidate.code,
    candidate.reason,
  );
}

/**
 * Error returned when a write fails to be persisted at a given durability tier.
 */
export class PersistedWriteRejectedError extends Error {
  readonly name = "PersistedWriteRejectedError";

  constructor(
    readonly transactionId: TxId,
    readonly code: string,
    readonly reason: string,
  ) {
    super(`Persisted transaction ${transactionId} was rejected (${code}): ${reason}`);
  }
}

/**
 * Returned by upsert, update, delete, and transaction operations.
 * Allows waiting for the write to be persisted at a given durability tier.
 */
export class WriteHandle<T = void, WaitResult = void> {
  readonly #client: JazzClient;
  readonly value: T;
  readonly txId: Promise<TxId>;

  constructor(txId: TxId | Promise<TxId>, client: JazzClient, value = undefined as T) {
    this.value = value;
    this.txId = Promise.resolve(txId);
    this.#client = client;
  }

  /**
   * Wait for the write to be persisted at a given durability tier.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the write is rejected.
   */
  async wait(options: { tier: DurabilityTier }): Promise<WaitResult> {
    return this.#client.waitForTransaction(this.txId, options.tier) as Promise<WaitResult>;
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
export class WriteResult<T> extends WriteHandle<T, T> {
  constructor(value: T, txId: TxId | Promise<TxId>, client: JazzClient) {
    super(txId, client, value);
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
    return new WriteResult(transformValue(this.value), this.txId, this.client());
  }
}

/**
 * Returned by explicitly-committed exclusive transactions.
 *
 * Exclusive transactions are accepted or rejected by the global authority, so
 * callers do not choose a durability tier when waiting for confirmation.
 */
export class ExclusiveWriteHandle extends WriteHandle<void> {
  /**
   * Wait for the exclusive transaction to be accepted or rejected by the authority.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the transaction is rejected.
   */
  override async wait(): Promise<void> {
    await this.client().waitForExclusiveTransaction(await this.txId);
  }
}

/**
 * Returned by auto-committed exclusive transactions.
 */
export class ExclusiveWriteResult<T> extends WriteResult<T> {
  /**
   * Wait for the exclusive transaction to be accepted or rejected by the authority.
   *
   * Rejects with a {@link PersistedWriteRejectedError} if the transaction is rejected.
   * @returns the callback result.
   */
  override async wait(): Promise<T> {
    await this.client().waitForExclusiveTransaction(await this.txId);
    return this.value;
  }

  override mapValue<U>(transformValue: (value: T) => U): ExclusiveWriteResult<U> {
    return new ExclusiveWriteResult(transformValue(this.value), this.txId, this.client());
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
  private shutdownPromise: Promise<void> | null = null;
  /** Facade-owned subscription releases, fenced against terminal reentrancy. */
  private activeSubscriptionReleases = new Map<number, () => void>();

  private registerSubscriptionRelease(handle: number, release: () => void): void {
    this.activeSubscriptionReleases.set(handle, release);
  }

  private releaseSubscription(handle: number): void {
    const release = this.activeSubscriptionReleases.get(handle);
    // Subscription ids are opaque facade results. Once their owner is gone,
    // duplicate public calls are intentionally no-ops rather than raw native
    // unsubscriptions (which could double-release a terminal handle).
    release?.();
  }

  private resolveSessionFromContext(): Session | null {
    return resolveClientSessionStateSync({
      appId: this.context.appId,
      accountId: this.context.accountId,
      jwtToken: this.context.jwtToken,
      cookieSession: this.context.cookieSession,
      trustedReservedSession: getTrustedReservedSession(this.context),
    }).internalSession;
  }

  private buildTransportAuthPayload(): {
    jwt_token: string | null;
    admin_secret?: string;
    backend_secret?: string;
    backend_session?: Session;
  } {
    const payload: {
      jwt_token: string | null;
      admin_secret?: string;
      backend_secret?: string;
      backend_session?: Session;
    } = { jwt_token: this.context.backendSecret ? null : (this.context.jwtToken ?? null) };
    // A backend open is a separate trusted-serving credential. Do not send an
    // incidental admin secret alongside it: server admission gives admin
    // precedence, which would turn this runtime's SYSTEM backend authority
    // into its adapter fallback wire identity.
    if (this.context.adminSecret && !this.context.backendSecret) {
      payload.admin_secret = this.context.adminSecret;
    }
    if (this.context.backendSecret) {
      payload.backend_secret = this.context.backendSecret;
      if (this.context.cookieSession) {
        payload.backend_session = this.context.cookieSession;
      }
    }
    return payload;
  }

  private constructor(
    runtime: Runtime,
    context: AppContext,
    defaultDurabilityTier: DurabilityTier,
    runtimeOptions?: ConnectRuntimeOptions,
  ) {
    this.runtime = runtime;
    this.scheduler = getScheduler();
    this.context = context;
    this.defaultDurabilityTier = defaultDurabilityTier;
    this.resolvedSession = this.resolveSessionFromContext();

    if (runtimeOptions?.onAuthFailure) {
      const handler = runtimeOptions.onAuthFailure;
      this.runtime.onAuthFailure((reason: string) => {
        handler(mapAuthReason(reason));
      });
    }

    this.runtime.onMutationError((event) => {
      console.error("Unhandled Jazz mutation error", event);
    });
  }

  /**
   * Create client from a pre-constructed runtime.
   *
   * RuntimeSource implementations use this after selecting the platform runtime.
   *
   * @param runtime A runtime implementing the Runtime interface
   * @param context Application context
   * @returns Connected JazzClient instance
   */
  static connectWithRuntime(
    runtime: Runtime,
    context: AppContext,
    runtimeOptions?: ConnectRuntimeOptions,
  ): JazzClient {
    return new JazzClient(runtime, context, resolveDefaultDurabilityTier(context), runtimeOptions);
  }

  beginTransaction(
    kind: TransactionKind,
    session?: Session,
    attribution?: string,
  ): OpenTransactionId {
    const id = createOpenTransactionId();
    const effectiveSession = this.resolveWriteSession(session, attribution);
    return requireTransactionalRuntime(this.runtime).beginTransaction(
      kind,
      id,
      this.encodeWriteContext(effectiveSession, attribution),
    );
  }

  onMutationError(listener: (event: MutationErrorEvent) => void): void {
    this.runtime.onMutationError(listener);
  }

  commitTransaction(id: OpenTransactionId): WriteHandle {
    const txId = requireTransactionalRuntime(this.runtime).commitTransaction(id);
    return new WriteHandle(txId, this);
  }

  rollbackTransaction(id: OpenTransactionId): Promise<boolean> {
    return requireTransactionalRuntime(this.runtime).rollbackTransaction(id);
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
    setTrustedReservedSession(this.context, undefined);
    this.resolvedSession = this.resolveSessionFromContext();
    // Push the refreshed credentials into the Rust transport.
    // Carry forward admin/backend secrets from context — omitting them here
    // would deserialise to None on the Rust side and silently erase any
    // privileged credentials the transport was connected with.
    this.runtime.updateAuth(JSON.stringify(this.buildTransportAuthPayload()));
  }

  /** @internal Update a token minted by a dedicated first-party reserved auth flow. */
  updateTrustedAuthToken(jwtToken: string, session: Session): void {
    this.context.jwtToken = jwtToken;
    setTrustedReservedSession(this.context, session);
    this.resolvedSession = this.resolveSessionFromContext();
    this.runtime.updateAuth(JSON.stringify(this.buildTransportAuthPayload()));
  }

  updateCookieSession(cookieSession?: Session): void {
    this.context.cookieSession = cookieSession;
    this.resolvedSession = this.resolveSessionFromContext();
    this.runtime.updateAuth(JSON.stringify(this.buildTransportAuthPayload()));
  }

  private normalizeQueryExecutionOptions(
    options?: InternalQueryExecutionOptions,
  ): ResolvedInternalQueryExecutionOptions {
    const resolved = resolveEffectiveQueryExecutionOptions(
      { ...this.context, defaultDurabilityTier: this.defaultDurabilityTier },
      options,
    );
    if (!options?.openTransactionId) {
      return resolved;
    }
    return {
      ...resolved,
      openTransactionId: options.openTransactionId,
    };
  }

  private encodeWriteContext(
    session?: Session,
    attribution?: string,
    openTransactionId?: OpenTransactionId,
    updatedAt?: number,
    branch?: BranchView,
  ): string | undefined {
    if (
      !session &&
      attribution === undefined &&
      !openTransactionId &&
      updatedAt === undefined &&
      !branch
    ) {
      return undefined;
    }
    if (
      attribution === undefined &&
      session &&
      !openTransactionId &&
      updatedAt === undefined &&
      !branch
    ) {
      return JSON.stringify(serializeRuntimeSession(session));
    }

    const payload: WriteContextPayload = {};
    if (session) {
      payload.session = serializeRuntimeSession(session);
    }
    if (attribution !== undefined) {
      payload.attribution = attribution;
    }
    if (updatedAt !== undefined) {
      payload.updated_at = normalizeUpdatedAt(updatedAt);
    }
    if (openTransactionId) {
      payload.transaction_id = openTransactionId;
    }
    if (branch) {
      payload.branch_view = {
        head: encodeBranchSelector(branch.head),
        base: encodeBranchViewBase(branch.base),
      };
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

  /**
   * Insert a new row into a table without waiting for durability.
   */
  insert(
    table: string,
    values: InsertValues,
    options?: InsertOptions,
    session?: Session,
    attribution?: string,
  ): WriteResult<Row> {
    const row = this.insertInternal(table, values, options, session, attribution);
    return new WriteResult(row, committedTxId(row), this);
  }

  /**
   * Consume one host byte/text stream and atomically insert the resulting
   * scalar with the other row values. The streamed value is intentionally not
   * copied back into the returned handle.
   */
  async insertStreaming(
    table: string,
    values: InsertValues,
    column: string,
    source: StreamingValueSource,
    options?: InsertOptions,
    session?: Session,
    attribution?: string,
  ): Promise<WriteHandle<{ id: string }>> {
    return this.streamingMutation(
      "insert",
      table,
      values,
      column,
      source,
      options,
      session,
      attribution,
    );
  }

  async updateStreaming(
    table: string,
    objectId: string,
    values: InsertValues,
    column: string,
    source: StreamingValueSource,
    options?: UpdateOptions,
    session?: Session,
    attribution?: string,
  ): Promise<WriteHandle<{ id: string }>> {
    return this.streamingMutation(
      "update",
      table,
      values,
      column,
      source,
      options,
      session,
      attribution,
      objectId,
    );
  }

  async upsertStreaming(
    table: string,
    objectId: string,
    values: InsertValues,
    column: string,
    source: StreamingValueSource,
    options?: UpdateOptions,
    session?: Session,
    attribution?: string,
  ): Promise<WriteHandle<{ id: string }>> {
    return this.streamingMutation(
      "upsert",
      table,
      values,
      column,
      source,
      options,
      session,
      attribution,
      objectId,
    );
  }

  private async streamingMutation(
    mutation: StreamingMutationKind,
    table: string,
    values: InsertValues,
    column: string,
    source: StreamingValueSource,
    options?: InsertOptions | UpdateOptions,
    session?: Session,
    attribution?: string,
    objectId?: string,
  ): Promise<WriteHandle<{ id: string }>> {
    if (!this.runtime.streamingMutation) {
      throw new Error("This runtime does not support streaming mutations");
    }
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      undefined,
      options?.updatedAt,
      options?.branch
        ? mutation === "insert"
          ? { head: options.branch as BranchSelector }
          : (options.branch as BranchView)
        : undefined,
    );
    const result = await this.runtime.streamingMutation(
      mutation,
      table,
      values,
      column,
      source,
      writeContext,
      objectId ?? ("id" in (options ?? {}) ? (options as InsertOptions).id : undefined),
    );
    if (result.kind !== "committed") {
      throw new Error("Streaming mutations cannot be staged inside an open transaction");
    }
    return new WriteHandle(result.txId, this, { id: result.id });
  }

  /**
   * @internal
   */
  insertInternal(
    table: string,
    values: InsertValues,
    options?: InsertOptions,
    session?: Session,
    attribution?: string,
    openTransactionId?: OpenTransactionId,
  ): InsertResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openTransactionId,
      options?.updatedAt,
      options?.branch ? { head: options.branch } : undefined,
    );
    const row = this.runtime.insert(table, values, writeContext, options?.id);
    return {
      ...row,
      values: row.values as Value[],
    };
  }

  /**
   * Restore a soft-deleted row with a caller-supplied id without waiting for durability.
   */
  restore(
    table: string,
    objectId: string,
    values: InsertValues,
    options?: RestoreOptions,
    session?: Session,
    attribution?: string,
  ): WriteResult<Row> {
    const row = this.restoreInternal(table, objectId, values, options, session, attribution);
    return new WriteResult(row, committedTxId(row), this);
  }

  /**
   * @internal
   */
  restoreInternal(
    table: string,
    objectId: string,
    values: InsertValues,
    options?: RestoreOptions,
    session?: Session,
    attribution?: string,
    openTransactionId?: OpenTransactionId,
  ): InsertResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openTransactionId,
      options?.updatedAt,
      options?.branch ? { head: options.branch } : undefined,
    );
    const row = this.runtime.restore(table, objectId, values, writeContext);
    return {
      ...row,
      values: row.values as Value[],
    };
  }

  /**
   * Create or update a row with a caller-supplied id without waiting for durability.
   */
  upsert(
    table: string,
    objectId: string,
    values: InsertValues,
    options?: UpdateOptions,
    session?: Session,
    attribution?: string,
  ): WriteHandle {
    const result = this.upsertInternal(table, objectId, values, options, session, attribution);
    return new WriteHandle(committedTxId(result), this);
  }

  /**
   * @internal
   */
  upsertInternal(
    table: string,
    objectId: string,
    values: InsertValues,
    options?: UpdateOptions,
    session?: Session,
    attribution?: string,
    openTransactionId?: OpenTransactionId,
  ): MutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openTransactionId,
      options?.updatedAt,
      options?.branch,
    );
    return this.runtime.upsert(table, objectId, values, writeContext);
  }

  /**
   * Execute a query and return all matching rows.
   *
   * @param query JSON-encoded runtime query specification
   * @param options Optional read durability options
   * @returns Array of matching rows
   */
  async query(query: string, options?: QueryExecutionOptions, session?: Session): Promise<Row[]> {
    return this.queryInternal(query, publicQueryExecutionOptions(options), session);
  }

  /** @internal */
  async queryInternal(
    query: string,
    options?: InternalQueryExecutionOptions,
    session?: Session,
  ): Promise<Row[]> {
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const effectiveSession = session ?? this.resolvedSession;
    const sessionJson = effectiveSession
      ? JSON.stringify(serializeRuntimeSession(effectiveSession))
      : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);
    const results = await this.runtime.query(
      query,
      sessionJson,
      options?.runtimeSettledTier === null
        ? undefined
        : (options?.runtimeSettledTier ?? normalizedOptions.tier),
      optionsJson,
    );
    return results as Row[];
  }

  /**
   * Update a row by ID without waiting for durability.
   */
  update(
    table: string,
    objectId: string,
    updates: Record<string, Value>,
    options?: UpdateOptions,
    session?: Session,
    attribution?: string,
  ): WriteHandle {
    const result = this.updateInternal(
      table,
      objectId,
      updates,
      options?.updatedAt,
      session,
      attribution,
      undefined,
      options?.branch,
    );
    return new WriteHandle(committedTxId(result), this);
  }

  /** @internal Typed `Db.update` diff lowering; not exposed as an imperative API. */
  updateLargeValues(
    table: string,
    objectId: string,
    updates: Record<string, Value>,
    descriptors: readonly unknown[],
    options?: UpdateOptions,
    session?: Session,
    attribution?: string,
  ): WriteHandle {
    const result = this.updateLargeValuesInternal(
      table,
      objectId,
      updates,
      descriptors,
      options?.updatedAt,
      session,
      attribution,
      undefined,
      options?.branch,
    );
    return new WriteHandle(committedTxId(result), this);
  }

  /** @internal */
  updateLargeValuesInternal(
    table: string,
    objectId: string,
    updates: Record<string, Value>,
    descriptors: readonly unknown[],
    updatedAt?: number,
    session?: Session,
    attribution?: string,
    openTransactionId?: OpenTransactionId,
    branch?: BranchView,
  ): MutationResult {
    if (openTransactionId || branch) {
      throw new Error(
        "Partial-value updates are not yet supported inside transactions or branch views.",
      );
    }
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      undefined,
      updatedAt,
    );
    if (!this.runtime.updateLargeValues) {
      throw new Error("Native runtime does not support typed partial-value updates.");
    }
    return this.runtime.updateLargeValues(table, objectId, updates, descriptors, writeContext);
  }

  /**
   * @internal
   */
  updateInternal(
    table: string,
    objectId: string,
    updates: Record<string, Value>,
    updatedAt?: number,
    session?: Session,
    attribution?: string,
    openTransactionId?: OpenTransactionId,
    branch?: BranchView,
  ): MutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openTransactionId,
      updatedAt,
      branch,
    );
    return this.runtime.update(table, objectId, updates, writeContext);
  }

  /**
   * Delete a row by ID without waiting for durability.
   */
  delete(
    table: string,
    objectId: string,
    options?: DeleteOptions,
    session?: Session,
    attribution?: string,
  ): WriteHandle {
    const result = this.deleteInternal(
      table,
      objectId,
      options?.updatedAt,
      session,
      attribution,
      undefined,
      options?.branch,
    );
    return new WriteHandle(committedTxId(result), this);
  }

  canInsertLocally(table: string, values: InsertValues, session?: Session): PermissionAdvice {
    if (!this.runtime.canInsertLocally) {
      throw new Error("Runtime does not support write-policy dry-run insert checks.");
    }
    return this.runtime.canInsertLocally(
      table,
      values,
      session ?? this.resolvedSession ?? undefined,
    );
  }

  canReadLocally(table: string, objectId: string, session?: Session): PermissionAdvice {
    if (!this.runtime.canReadLocally) {
      throw new Error("Runtime does not support read-policy dry-run checks.");
    }
    return this.runtime.canReadLocally(
      table,
      objectId,
      session ?? this.resolvedSession ?? undefined,
    );
  }

  canUpdateLocally(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): PermissionAdvice {
    if (!this.runtime.canUpdateLocally) {
      throw new Error("Runtime does not support write-policy dry-run update checks.");
    }
    return this.runtime.canUpdateLocally(
      table,
      objectId,
      values,
      session ?? this.resolvedSession ?? undefined,
    );
  }

  canDeleteLocally(table: string, objectId: string, session?: Session): PermissionAdvice {
    if (!this.runtime.canDeleteLocally) {
      throw new Error("Runtime does not support write-policy dry-run delete checks.");
    }
    return this.runtime.canDeleteLocally(
      table,
      objectId,
      session ?? this.resolvedSession ?? undefined,
    );
  }

  requestInsertPermissionAdvice(
    table: string,
    values: InsertValues,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return (
      this.runtime.requestInsertPermissionAdvice?.(
        table,
        values,
        session ?? this.resolvedSession ?? undefined,
      ) ?? Promise.resolve("unknown")
    );
  }

  requestReadPermissionAdvice(
    table: string,
    objectId: string,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return (
      this.runtime.requestReadPermissionAdvice?.(
        table,
        objectId,
        session ?? this.resolvedSession ?? undefined,
      ) ?? Promise.resolve("unknown")
    );
  }

  requestUpdatePermissionAdvice(
    table: string,
    objectId: string,
    values: Record<string, Value>,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return (
      this.runtime.requestUpdatePermissionAdvice?.(
        table,
        objectId,
        values,
        session ?? this.resolvedSession ?? undefined,
      ) ?? Promise.resolve("unknown")
    );
  }

  requestDeletePermissionAdvice(
    table: string,
    objectId: string,
    session?: Session,
  ): Promise<PermissionAdvice> {
    return (
      this.runtime.requestDeletePermissionAdvice?.(
        table,
        objectId,
        session ?? this.resolvedSession ?? undefined,
      ) ?? Promise.resolve("unknown")
    );
  }

  /**
   * @internal
   */
  deleteInternal(
    table: string,
    objectId: string,
    updatedAt?: number,
    session?: Session,
    attribution?: string,
    openTransactionId?: OpenTransactionId,
    branch?: BranchView,
  ): MutationResult {
    const effectiveSession = this.resolveWriteSession(session, attribution);
    const writeContext = this.encodeWriteContext(
      effectiveSession,
      attribution,
      openTransactionId,
      updatedAt,
      branch,
    );
    return this.runtime.delete(table, objectId, writeContext);
  }

  /**
   * Subscribe to a query and receive updates when results change.
   *
   * @param callbacks Delta callback, or callbacks object with terminal error handling.
   * Legacy function-form subscriptions report terminal failures to `console.error`.
   * @param options Optional read durability options
   * @returns Subscription ID for unsubscribing
   */
  subscribe(
    query: string,
    callbacks: SubscriptionCallback | SubscriptionCallbacks,
    options?: QueryExecutionOptions,
    session?: Session,
  ): number {
    return this.subscribeInternal(query, callbacks, publicQueryExecutionOptions(options), session);
  }

  /** @internal */
  subscribeInternal(
    query: string,
    callbacks: SubscriptionCallback | SubscriptionCallbacks,
    options?: InternalQueryExecutionOptions,
    session?: Session,
  ): number {
    const { onUpdate, onError } =
      typeof callbacks === "function" ? { onUpdate: callbacks, onError: undefined } : callbacks;
    const normalizedOptions = this.normalizeQueryExecutionOptions(options);
    const effectiveSession = session ?? this.resolvedSession;
    const sessionJson = effectiveSession
      ? JSON.stringify(serializeRuntimeSession(effectiveSession))
      : undefined;
    const optionsJson = encodeQueryExecutionOptions(normalizedOptions);

    const handle = this.runtime.createSubscription(
      query,
      sessionJson,
      normalizedOptions.tier,
      optionsJson,
    );

    let terminal = false;
    let nativeHandleReleased = false;
    const releaseNativeHandle = () => {
      if (nativeHandleReleased) return;
      nativeHandleReleased = true;
      if (this.activeSubscriptionReleases.get(handle) === releaseSubscription) {
        this.activeSubscriptionReleases.delete(handle);
      }
      this.runtime.unsubscribe(handle);
    };
    const releaseSubscription = () => {
      // A public unsubscribe is terminal too: a queued native frame must not
      // re-enter application callbacks after its owner has been released.
      terminal = true;
      releaseNativeHandle();
    };
    this.registerSubscriptionRelease(handle, releaseSubscription);
    const terminate = (error: Error, unsubscribe: boolean) => {
      if (terminal) return;
      terminal = true;
      if (unsubscribe) this.releaseSubscription(handle);
      if (!onError) {
        console.error("Unhandled Jazz subscription error", error);
        return;
      }
      try {
        onError(error);
      } catch (callbackError) {
        console.error("Jazz subscription error callback failed", callbackError);
      }
    };

    try {
      this.runtime.executeSubscription(handle, (result) => {
        if (terminal) return;
        if (result instanceof Error) {
          // A terminal callback means this facade owns the now-dead native
          // handle. Release it even if the core stream has stopped producing.
          terminate(result, true);
          return;
        }
        try {
          onUpdate(result);
        } catch (error) {
          // User callback failures must not escape through the native tick or
          // leave a live source publishing behind the terminal notification.
          terminate(error instanceof Error ? error : new Error(String(error)), true);
        }
      });
    } catch (error) {
      // createSubscription already transferred ownership to this facade. If
      // callback installation fails synchronously, no caller can own the
      // handle because subscribe() has not returned it yet.
      this.releaseSubscription(handle);
      throw error;
    }

    return handle;
  }

  /**
   * Unsubscribe from a query.
   *
   * @param subscriptionId ID returned from subscribe()
   */
  unsubscribe(subscriptionId: number): void {
    this.releaseSubscription(subscriptionId);
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
    this.runtime.connect(httpUrlToWs(url, this.context.appId), JSON.stringify(auth));
  }

  /**
   * Temporarily disconnect from the Jazz server without closing local runtime state.
   */
  async disconnectTransport(): Promise<void> {
    await this.runtime.disconnect({ rejectWaiters: false });
  }

  /**
   * Get the current schema.
   */
  getSchema(): WasmSchema {
    return normalizeRuntimeSchema(this.context.schema);
  }

  /** @internal Connection managers use this to attach native peer transports. */
  getRuntime(): Runtime {
    return this.runtime;
  }

  async waitForTransaction(txId: TxId | Promise<TxId>, tier: DurabilityTier): Promise<void> {
    try {
      await this.runtime.waitForTransaction(txId, tier);
    } catch (error) {
      throw this.normalizeTransactionWaitError(error);
    }
  }

  /** @internal */
  async waitForExclusiveTransaction(txId: TxId): Promise<void> {
    const hasAuthority =
      Boolean(this.context.serverUrl) || this.runtime.nativeUpstreamConfigured?.() === true;
    await this.waitForTransaction(txId, hasAuthority ? "global" : "local");
  }

  private normalizeTransactionWaitError(error: unknown): Error {
    return (
      rejectionFromRuntimeWaitError(error) ??
      (error instanceof Error ? error : new Error(String(error)))
    );
  }

  /**
   * Shutdown the client and release resources.
   */
  async shutdown(): Promise<void> {
    if (this.shutdownPromise) {
      return await this.shutdownPromise;
    }

    this.shutdownPromise = (async () => {
      // Close runtime if it supports explicit shutdown.
      if (this.runtime.close) {
        await this.runtime.close();
      } else {
        this.runtime.disconnect({ rejectWaiters: false });
      }
    })();

    return await this.shutdownPromise;
  }

  /** @internal Abandon runtime work after external storage invalidation/reset. */
  discard(): void {
    this.runtime.discard?.();
  }

  async clearClientStorage(): Promise<void> {
    if (!this.runtime.clearClientStorage) {
      throw new Error("Runtime does not support client storage reset.");
    }

    await this.runtime.clearClientStorage();
  }
}
