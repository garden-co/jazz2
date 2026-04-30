import type { InsertValues, Value, WasmSchema } from "../drivers/types.js";
import type {
  DirectInsertResult,
  DirectMutationResult,
  LocalBatchRecord,
  PermissionDecision,
  Row,
  Runtime,
} from "../runtime/client.js";
import { encodeFFIRecordToJson } from "../runtime/ffi-value.js";

export type JazzRnErrorTag =
  | "InvalidJson"
  | "InvalidUuid"
  | "InvalidTier"
  | "Schema"
  | "Runtime"
  | "Internal";

export type JazzRnNormalizedError = Error & {
  tag: JazzRnErrorTag;
  cause?: unknown;
};

export interface JazzRnRuntimeBinding {
  addClient(): string;
  addServer(serverCatalogueStateHash?: string | null, nextSyncSeq?: number | null): void;
  batchedTick(): void;
  close(): void;
  connect(url: string, authJson: string): void;
  disconnect(): void;
  updateAuth(authJson: string): void;
  onAuthFailure(callback: { onFailure(reason: string): void }): void;
  delete_(objectId: string): string;
  deleteWithSession?(objectId: string, writeContextJson: string | undefined): string;
  canInsert(table: string, valuesJson: string): string;
  canInsertWithSession?(
    table: string,
    valuesJson: string,
    writeContextJson: string | undefined,
  ): string;
  canUpdate(objectId: string, valuesJson: string): string;
  canUpdateWithSession?(
    objectId: string,
    valuesJson: string,
    writeContextJson: string | undefined,
  ): string;
  flush(): void;
  getSchemaHash(): string;
  insert(table: string, valuesJson: string, objectId: string | undefined): string;
  insertWithSession?(
    table: string,
    valuesJson: string,
    writeContextJson: string | undefined,
    objectId: string | undefined,
  ): string;
  loadLocalBatchRecord?(batchId: string): string | null;
  loadLocalBatchRecords?(): string;
  drainRejectedBatchIds?(): string[];
  onBatchedTickNeeded(
    callback:
      | {
          requestBatchedTick(): void;
        }
      | undefined,
  ): void;
  onSyncMessageReceived(messageJson: string, seq?: number | null): void;
  onSyncMessageReceivedFromClient(clientId: string, messageJson: string): void;
  query(queryJson: string, sessionJson: string | undefined, tier: string | undefined): string;
  removeServer(): void;
  setClientRole(clientId: string, role: string): void;
  createSubscription(
    queryJson: string,
    sessionJson: string | undefined,
    tier: string | undefined,
  ): bigint;
  executeSubscription(handle: bigint, callback: { onUpdate(deltaJson: string): void }): void;
  subscribe(
    queryJson: string,
    callback: { onUpdate(deltaJson: string): void },
    sessionJson: string | undefined,
    tier: string | undefined,
  ): bigint;
  unsubscribe(handle: bigint): void;
  update(objectId: string, valuesJson: string): string;
  updateWithSession?(
    objectId: string,
    valuesJson: string,
    writeContextJson: string | undefined,
  ): string;
  acknowledgeRejectedBatch?(batchId: string): boolean;
  sealBatch?(batchId: string): void;
  uniffiDestroy?(): void;
}

function assertWorkerTier(tier: string): void {
  if (tier !== "local") {
    throw new Error(
      `jazz-rn runtime adapter currently supports only 'local' tier for persisted mutations (received '${tier}')`,
    );
  }
}

function swallowCallbackError(context: string, error: unknown): void {
  // Callback exceptions crossing the UniFFI boundary can panic Rust and fail writes.
  // Keep runtime alive and surface the real JS error in logs.
  try {
    // eslint-disable-next-line no-console
    console.error(`[jazz-rn] ${context} callback failed`, error);
  } catch {
    // Ignore logging failures.
  }
}

function isJazzRnErrorLike(
  error: unknown,
): error is { tag: string; inner?: { message?: unknown } } {
  if (!error || typeof error !== "object") {
    return false;
  }
  const candidate = error as { tag?: unknown; inner?: unknown };
  return typeof candidate.tag === "string";
}

function normalizeJazzRnError(error: unknown): Error {
  if (!isJazzRnErrorLike(error)) {
    return error instanceof Error ? error : new Error(String(error));
  }

  const message =
    typeof error.inner?.message === "string" && error.inner.message.length > 0
      ? error.inner.message
      : String(error);
  const tag = error.tag as JazzRnErrorTag;
  const normalized = createErrorWithCause(message, error);
  normalized.name = `JazzRn${tag}Error`;
  Object.defineProperty(normalized, "tag", {
    value: tag,
    enumerable: false,
    configurable: true,
    writable: true,
  });
  return normalized as JazzRnNormalizedError;
}

function createErrorWithCause(message: string, cause: unknown): Error {
  try {
    return new Error(message, { cause });
  } catch {
    const fallback = new Error(message) as Error & { cause?: unknown };
    Object.defineProperty(fallback, "cause", {
      value: cause,
      enumerable: false,
      configurable: true,
      writable: true,
    });
    return fallback;
  }
}

function parsePermissionDecision(decisionJson: string): PermissionDecision {
  const decision = JSON.parse(decisionJson) as unknown;
  if (decision === true || decision === false || decision === "unknown") {
    return decision;
  }
  throw new Error(`Invalid permission decision: ${String(decision)}`);
}

export class JazzRnRuntimeAdapter implements Runtime {
  private readonly handleMap = new Map<number, bigint>();
  private closed = false;

  constructor(
    private readonly binding: JazzRnRuntimeBinding,
    private readonly schema: WasmSchema,
  ) {
    this.binding.onBatchedTickNeeded({
      requestBatchedTick: () => {
        // Avoid re-entering Rust while the originating call still holds its mutex.
        Promise.resolve()
          .then(() => {
            if (!this.closed) {
              this.binding.batchedTick();
            }
          })
          .catch(() => {
            // Ignore callback failures from deferred ticks.
          });
      },
    });
  }

  private requireWriteContextMethod<
    T extends
      | "insertWithSession"
      | "updateWithSession"
      | "deleteWithSession"
      | "canInsertWithSession"
      | "canUpdateWithSession",
  >(method: T): NonNullable<JazzRnRuntimeBinding[T]> {
    const runtimeMethod = this.binding[method];
    if (!runtimeMethod) {
      throw new Error(`${method} is not supported by this RN runtime binding`);
    }
    return runtimeMethod.bind(this.binding) as NonNullable<JazzRnRuntimeBinding[T]>;
  }

  private requireBatchRecordMethod<
    T extends
      | "loadLocalBatchRecord"
      | "loadLocalBatchRecords"
      | "drainRejectedBatchIds"
      | "acknowledgeRejectedBatch"
      | "sealBatch",
  >(method: T): NonNullable<JazzRnRuntimeBinding[T]> {
    const runtimeMethod = this.binding[method];
    if (!runtimeMethod) {
      throw new Error(`${method} is not supported by this RN runtime binding`);
    }
    return runtimeMethod.bind(this.binding) as NonNullable<JazzRnRuntimeBinding[T]>;
  }

  insert(table: string, values: InsertValues, object_id?: string | null): DirectInsertResult {
    try {
      const rowJson = this.binding.insert(
        table,
        encodeFFIRecordToJson(values),
        object_id ?? undefined,
      );
      return JSON.parse(rowJson) as DirectInsertResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  insertWithSession(
    table: string,
    values: InsertValues,
    write_context_json?: string | null,
    object_id?: string | null,
  ): DirectInsertResult {
    try {
      const rowJson = this.requireWriteContextMethod("insertWithSession")(
        table,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
        object_id ?? undefined,
      );
      return JSON.parse(rowJson) as DirectInsertResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  canInsert(table: string, values: InsertValues): PermissionDecision {
    try {
      const decisionJson = this.binding.canInsert(table, encodeFFIRecordToJson(values));
      return parsePermissionDecision(decisionJson);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  canInsertWithSession(
    table: string,
    values: InsertValues,
    write_context_json?: string | null,
  ): PermissionDecision {
    try {
      const decisionJson = this.requireWriteContextMethod("canInsertWithSession")(
        table,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
      );
      return parsePermissionDecision(decisionJson);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  update(object_id: string, values: Record<string, Value>): DirectMutationResult {
    try {
      const resultJson = this.binding.update(object_id, encodeFFIRecordToJson(values));
      return JSON.parse(resultJson) as DirectMutationResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  updateWithSession(
    object_id: string,
    values: Record<string, Value>,
    write_context_json?: string | null,
  ): DirectMutationResult {
    try {
      const resultJson = this.requireWriteContextMethod("updateWithSession")(
        object_id,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
      );
      return JSON.parse(resultJson) as DirectMutationResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  canUpdate(object_id: string, values: Record<string, Value>): PermissionDecision {
    try {
      const decisionJson = this.binding.canUpdate(object_id, encodeFFIRecordToJson(values));
      return parsePermissionDecision(decisionJson);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  canUpdateWithSession(
    object_id: string,
    values: Record<string, Value>,
    write_context_json?: string | null,
  ): PermissionDecision {
    try {
      const decisionJson = this.requireWriteContextMethod("canUpdateWithSession")(
        object_id,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
      );
      return parsePermissionDecision(decisionJson);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  delete(object_id: string): DirectMutationResult {
    try {
      const resultJson = this.binding.delete_(object_id);
      return JSON.parse(resultJson) as DirectMutationResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  deleteWithSession(object_id: string, write_context_json?: string | null): DirectMutationResult {
    try {
      const resultJson = this.requireWriteContextMethod("deleteWithSession")(
        object_id,
        write_context_json ?? undefined,
      );
      return JSON.parse(resultJson) as DirectMutationResult;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  loadLocalBatchRecord(batch_id: string): LocalBatchRecord | null {
    try {
      const recordJson = this.requireBatchRecordMethod("loadLocalBatchRecord")(batch_id);
      return recordJson ? (JSON.parse(recordJson) as LocalBatchRecord) : null;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  loadLocalBatchRecords(): LocalBatchRecord[] {
    try {
      const recordsJson = this.requireBatchRecordMethod("loadLocalBatchRecords")();
      return JSON.parse(recordsJson) as LocalBatchRecord[];
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  drainRejectedBatchIds(): string[] {
    try {
      return this.requireBatchRecordMethod("drainRejectedBatchIds")();
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  async query(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
  ): Promise<any> {
    try {
      const rowsJson = this.binding.query(query_json, session_json ?? undefined, tier ?? undefined);
      return JSON.parse(rowsJson);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  insertDurable(table: string, values: InsertValues, tier: string): Promise<Row> {
    assertWorkerTier(tier);
    const row = this.insert(table, values);
    this.binding.flush();
    return Promise.resolve(row);
  }

  insertDurableWithSession(
    table: string,
    values: InsertValues,
    write_context_json: string | null | undefined,
    tier: string,
  ): Promise<Row> {
    assertWorkerTier(tier);
    const row = this.insertWithSession(table, values, write_context_json);
    this.binding.flush();
    return Promise.resolve(row);
  }

  updateDurable(object_id: string, values: Record<string, Value>, tier: string): Promise<void> {
    assertWorkerTier(tier);
    this.update(object_id, values);
    this.binding.flush();
    return Promise.resolve();
  }

  updateDurableWithSession(
    object_id: string,
    values: Record<string, Value>,
    write_context_json: string | null | undefined,
    tier: string,
  ): Promise<void> {
    assertWorkerTier(tier);
    this.updateWithSession(object_id, values, write_context_json);
    this.binding.flush();
    return Promise.resolve();
  }

  deleteDurable(object_id: string, tier: string): Promise<void> {
    assertWorkerTier(tier);
    this.delete(object_id);
    this.binding.flush();
    return Promise.resolve();
  }

  deleteDurableWithSession(
    object_id: string,
    write_context_json: string | null | undefined,
    tier: string,
  ): Promise<void> {
    assertWorkerTier(tier);
    this.deleteWithSession(object_id, write_context_json);
    this.binding.flush();
    return Promise.resolve();
  }

  createSubscription(
    query_json: string,
    session_json?: string | null,
    tier?: string | null,
  ): number {
    const handle = this.binding.createSubscription(
      query_json,
      session_json ?? undefined,
      tier ?? undefined,
    );

    const numericHandle = Number(handle);
    if (!Number.isSafeInteger(numericHandle)) {
      throw new Error(`Subscription handle ${handle.toString()} is outside safe integer range`);
    }
    this.handleMap.set(numericHandle, handle);
    return numericHandle;
  }

  executeSubscription(handle: number, on_update: Function): void {
    const nativeHandle = this.handleMap.get(handle) ?? BigInt(handle);
    this.binding.executeSubscription(nativeHandle, {
      onUpdate: (deltaJson: string) => {
        try {
          const parsed = JSON.parse(deltaJson) as unknown;
          on_update(parsed);
        } catch (error) {
          swallowCallbackError("subscription", error);
        }
      },
    });
  }

  subscribe(
    query_json: string,
    on_update: Function,
    session_json?: string | null,
    tier?: string | null,
  ): number {
    const handle = this.binding.subscribe(
      query_json,
      {
        onUpdate: (deltaJson: string) => {
          try {
            const parsed = JSON.parse(deltaJson) as unknown;
            on_update(parsed);
          } catch (error) {
            swallowCallbackError("subscription", error);
          }
        },
      },
      session_json ?? undefined,
      tier ?? undefined,
    );

    const numericHandle = Number(handle);
    if (!Number.isSafeInteger(numericHandle)) {
      throw new Error(`Subscription handle ${handle.toString()} is outside safe integer range`);
    }
    this.handleMap.set(numericHandle, handle);
    return numericHandle;
  }

  unsubscribe(handle: number): void {
    const nativeHandle = this.handleMap.get(handle) ?? BigInt(handle);
    this.binding.unsubscribe(nativeHandle);
    this.handleMap.delete(handle);
  }

  onSyncMessageReceived(message_json: string, seq?: number | null): void {
    if (this.closed) return;
    this.binding.onSyncMessageReceived(message_json, seq);
  }

  onSyncMessageToSend(_callback: Function): void {
    // Server sync is handled by the Rust-owned WebSocket transport (runtime.connect()).
    // The outbox callback is no longer wired through UniFFI for RN.
  }

  connect(url: string, authJson: string): void {
    if (this.closed) return;
    this.binding.connect(url, authJson);
  }

  disconnect(): void {
    if (this.closed) return;
    this.binding.disconnect();
  }

  updateAuth(authJson: string): void {
    if (this.closed) return;
    this.binding.updateAuth(authJson);
  }

  onAuthFailure(callback: (reason: string) => void): void {
    if (this.closed) return;
    this.binding.onAuthFailure({
      onFailure: (reason: string) => {
        try {
          callback(reason);
        } catch (error) {
          swallowCallbackError("onAuthFailure", error);
        }
      },
    });
  }

  acknowledgeRejectedBatch(batch_id: string): boolean {
    try {
      return this.requireBatchRecordMethod("acknowledgeRejectedBatch")(batch_id);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  sealBatch(batch_id: string): void {
    try {
      this.requireBatchRecordMethod("sealBatch")(batch_id);
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  addServer(_serverCatalogueStateHash?: string | null, _nextSyncSeq?: number | null): void {
    if (this.closed) return;
    this.binding.addServer();
  }

  removeServer(): void {
    if (this.closed) return;
    this.binding.removeServer();
  }

  addClient(): string {
    return this.binding.addClient();
  }

  getSchema(): any {
    return this.schema;
  }

  getSchemaHash(): string {
    return this.binding.getSchemaHash();
  }

  setClientRole(client_id: string, role: string): void {
    this.binding.setClientRole(client_id, role);
  }

  onSyncMessageReceivedFromClient(client_id: string, message_json: string): void {
    if (this.closed) return;
    this.binding.onSyncMessageReceivedFromClient(client_id, message_json);
  }

  close(): void {
    if (this.closed) return;
    this.closed = true;
    this.binding.onBatchedTickNeeded(undefined);
    this.handleMap.clear();
    try {
      this.binding.close();
    } catch {
      // Ignore close failures on teardown.
    }
    this.binding.uniffiDestroy?.();
  }
}
