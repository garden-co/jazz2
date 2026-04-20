import type { InsertValues, Value, WasmSchema } from "../drivers/types.js";
import type { Row, Runtime } from "../runtime/client.js";
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
  delete_(objectId: string): void;
  deleteWithSession?(objectId: string, writeContextJson: string | undefined): void;
  flush(): void;
  getSchemaHash(): string;
  insert(table: string, valuesJson: string, objectId: string | undefined): string;
  insertWithSession?(
    table: string,
    valuesJson: string,
    writeContextJson: string | undefined,
    objectId: string | undefined,
  ): string;
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
  update(objectId: string, valuesJson: string): void;
  updateWithSession?(
    objectId: string,
    valuesJson: string,
    writeContextJson: string | undefined,
  ): void;
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

function isObjectNotFoundError(error: unknown): boolean {
  if (!error || typeof error !== "object") return false;
  const maybeInner = (error as { inner?: { message?: unknown } }).inner;
  const innerMessage =
    maybeInner && typeof maybeInner === "object" ? maybeInner.message : undefined;
  if (typeof innerMessage === "string" && innerMessage.includes("ObjectNotFound(")) {
    return true;
  }
  const message = String(error);
  return message.includes("ObjectNotFound(");
}

function swallowMissingObjectMutation(context: string, error: unknown): boolean {
  if (!isObjectNotFoundError(error)) return false;
  try {
    // eslint-disable-next-line no-console
    console.warn(`[jazz-rn] ${context}: object already missing, ignoring`, error);
  } catch {
    // Ignore logging failures.
  }
  return true;
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
    T extends "insertWithSession" | "updateWithSession" | "deleteWithSession",
  >(method: T): NonNullable<JazzRnRuntimeBinding[T]> {
    const runtimeMethod = this.binding[method];
    if (!runtimeMethod) {
      throw new Error(`${method} is not supported by this RN runtime binding`);
    }
    return runtimeMethod.bind(this.binding) as NonNullable<JazzRnRuntimeBinding[T]>;
  }

  insert(table: string, values: InsertValues, object_id?: string | null): Row {
    try {
      const rowJson = this.binding.insert(
        table,
        encodeFFIRecordToJson(values),
        object_id ?? undefined,
      );
      return JSON.parse(rowJson) as Row;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  insertWithSession(
    table: string,
    values: InsertValues,
    write_context_json?: string | null,
    object_id?: string | null,
  ): Row {
    try {
      const rowJson = this.requireWriteContextMethod("insertWithSession")(
        table,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
        object_id ?? undefined,
      );
      return JSON.parse(rowJson) as Row;
    } catch (error) {
      throw normalizeJazzRnError(error);
    }
  }

  update(object_id: string, values: Record<string, Value>): void {
    try {
      this.binding.update(object_id, encodeFFIRecordToJson(values));
    } catch (error) {
      if (swallowMissingObjectMutation("update", error)) return;
      throw normalizeJazzRnError(error);
    }
  }

  updateWithSession(
    object_id: string,
    values: Record<string, Value>,
    write_context_json?: string | null,
  ): void {
    try {
      this.requireWriteContextMethod("updateWithSession")(
        object_id,
        encodeFFIRecordToJson(values),
        write_context_json ?? undefined,
      );
    } catch (error) {
      if (swallowMissingObjectMutation("update", error)) return;
      throw normalizeJazzRnError(error);
    }
  }

  delete(object_id: string): void {
    try {
      this.binding.delete_(object_id);
    } catch (error) {
      if (swallowMissingObjectMutation("delete", error)) return;
      throw normalizeJazzRnError(error);
    }
  }

  deleteWithSession(object_id: string, write_context_json?: string | null): void {
    try {
      this.requireWriteContextMethod("deleteWithSession")(
        object_id,
        write_context_json ?? undefined,
      );
    } catch (error) {
      if (swallowMissingObjectMutation("delete", error)) return;
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

  onSyncMessageReceived(message_json: string, seq?: number | null): void {
    if (this.closed) return;
    this.binding.onSyncMessageReceived(message_json, seq);
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
