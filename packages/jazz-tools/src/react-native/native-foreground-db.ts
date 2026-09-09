import type { MutationErrorEvent } from "../runtime/client.js";

type ForegroundMutationKind = "insert" | "update" | "upsert" | "delete" | "restore";
type ForegroundMutationOptions = {
  rowId?: Uint8Array;
  branch?: unknown;
  head?: unknown;
  base?: unknown;
  updatedAtMs?: number;
  author?: Uint8Array;
  attribution?: Uint8Array;
};

type ForegroundPermissionAdviceAction =
  | { type: "insert"; table: string; cells: Uint8Array }
  | { type: "read" | "delete"; table: string; rowId: Uint8Array }
  | { type: "update"; table: string; rowId: Uint8Array; patch: Uint8Array };

type ForegroundCommand =
  | { type: "permissionAdvice"; action: ForegroundPermissionAdviceAction }
  | "tick"
  | "close"
  | { type: "disconnectNativeUpstream" }
  | { type: "reconnectNativeUpstream" }
  | { type: "nativeConnectionStatus" }
  | { type: "nativeSessionMetadata" }
  | {
      type: "all";
      query: Uint8Array;
      optionsJson: string;
      transaction?: number;
    }
  | { type: "localCurrentRow"; table: string; rowId: Uint8Array }
  | {
      type: "subscribe";
      query: Uint8Array;
      optionsJson: string;
    }
  | { type: "drainSubscription"; subscription: number }
  | { type: "unsubscribe"; subscription: number }
  | { type: "poll"; operation: number }
  | { type: "cancel"; operation: number }
  | { type: "beginTransaction"; kind: "mergeable" | "exclusive" }
  | {
      type: "insert";
      transaction: number;
      table: string;
      cells: Uint8Array;
      rowId?: Uint8Array;
    }
  | {
      type: "update";
      transaction: number;
      table: string;
      rowId: Uint8Array;
      patch: Uint8Array;
    }
  | {
      type: "upsert";
      transaction: number;
      table: string;
      rowId: Uint8Array;
      cells: Uint8Array;
    }
  | { type: "delete"; transaction: number; table: string; rowId: Uint8Array }
  | { type: "commitTransaction"; transaction: number }
  | { type: "rollbackTransaction"; transaction: number }
  | { type: "waitForTransaction"; txId: Uint8Array; tier: string }
  | { type: "waitForPendingWrites"; tier: string }
  | {
      type: "stageMutation";
      transaction: number;
      mutation: ForegroundMutationKind;
      table: string;
      rowId?: Uint8Array;
      cells: Uint8Array;
      optionsJson: string;
    }
  | {
      type: "directMutation";
      mutation: ForegroundMutationKind;
      table: string;
      rowId?: Uint8Array;
      cells: Uint8Array;
      optionsJson: string;
    }
  | { type: "writeState"; txId: Uint8Array }
  | { type: "drainMutationErrors" }
  | {
      type: "beginStreamingMutation";
      mutation: "insert" | "update" | "upsert";
      table: string;
      rowId: Uint8Array;
      cells: Uint8Array;
      column: string;
      optionsJson: string;
    }
  | { type: "pushStreamingMutation"; upload: number; chunk: Uint8Array }
  | { type: "finishStreamingMutation" | "abortStreamingMutation"; upload: number }
  | {
      type: "updateLargeValues";
      table: string;
      rowId: Uint8Array;
      patch: Uint8Array;
      descriptorsJson: string;
      updatedAtMs?: number;
    };

type ForegroundEvent =
  | {
      type: "delta";
      reset: boolean;
      settled: boolean;
      tier: string;
      delta: Uint8Array;
      terminalOperations?: unknown[];
    }
  | { type: "rejected"; reason: string }
  | { type: "closed" };

type NativeConnectionStatus = {
  type: "nativeConnectionStatus";
  configured: boolean;
  explicitlyOffline: boolean;
  connected: boolean;
};

type ForegroundResponse =
  | { type: "permissionAdvice"; advice: "allowed" | "denied" | "unknown" }
  | {
      type: "nativeSessionMetadata";
      node: Uint8Array;
      registryAuthority: string;
      accountId: string | null;
      issuer: string;
      userId: string;
    }
  | NativeConnectionStatus
  | { type: "ticked" }
  | { type: "rows"; rows: Uint8Array }
  | { type: "subscribed"; subscription: number }
  | { type: "subscriptionEvents"; events: ForegroundEvent[] }
  | { type: "unsubscribed"; closed: boolean }
  | { type: "closed"; closed: boolean }
  | { type: "pending"; operation: number }
  | { type: "operationError"; reason: string }
  | { type: "cancelled"; cancelled: boolean }
  | { type: "transactionOpened"; transaction: number }
  | { type: "inserted"; rowId: Uint8Array }
  | { type: "mutationStaged" }
  | { type: "transactionCommitted"; txId: Uint8Array }
  | { type: "transactionRolledBack"; rolledBack: boolean }
  | { type: "transactionSettled"; txId: Uint8Array }
  | { type: "writeState"; stateJson: string }
  | { type: "mutationErrors"; eventsJson: string }
  | { type: "streamingMutationOpened"; upload: number }
  | { type: "streamingMutationPushed" }
  | { type: "streamingMutationAborted"; aborted: boolean }
  | { type: "mutationCommitted"; txId: Uint8Array; rowId: Uint8Array };

export type NativeForegroundRuntime = {
  execute(command: Uint8Array): Uint8Array;
  tick(): void;
  isClosed?(): boolean;
  setTickScheduler?(callback: (urgency: string) => void): void;
  close(): boolean;
};

export type NativeForegroundModule = {
  installNativeForegroundRuntime(): NativeForegroundFactory;
  encodeNativeForegroundCommand(command: ForegroundCommand): Uint8Array;
  decodeNativeForegroundResponse(bytes: Uint8Array): ForegroundResponse;
};

export type NativeForegroundFactory = {
  accountSecret?(): Uint8Array;
  beginAccountSession?(requestJson: string): Uint8Array;
  attachAccountSchema?(capability: Uint8Array, schemaJson: string): Uint8Array;
  releaseAccountSession?(capability: Uint8Array): void;
  refreshAccountSession?(capability: Uint8Array, requestJson: string): void;
  readonly abiVersion: number;
  openAttached(capability: Uint8Array): NativeForegroundRuntime;
};

export const REACT_NATIVE_UNSUPPORTED_ERROR =
  "React Native native foreground does not support this operation yet";

/**
 * Narrow NativeDb consumer for the first JSI foreground slice.
 *
 * Rows and subscription deltas stay in the ordinary binding byte format, so
 * NativeRuntimeAdapter remains the sole schema-aware decoder. This is not a
 * second object-shaped database facade and intentionally grows only as the
 * shared Rust command ABI grows.
 */
/**
 * Thin `NativeDb` adapter over the native foreground command ABI.
 *
 * This deliberately contains no row, query, or mutation interpretation: the
 * normal `NativeRuntimeAdapter` owns the public API and existing codecs. A
 * direct mutation is represented by one ordinary native mergeable transaction;
 * explicit public transactions retain their native transaction handle until
 * commit/rollback.
 */
export class NativeForegroundDb {
  private closed = false;
  private mutationErrorCallback: ((event: MutationErrorEvent) => void) | undefined;
  private readonly transactions = new Map<string, NativeForegroundTransaction>();

  constructor(
    private readonly runtime: NativeForegroundRuntime,
    private readonly commands: NativeForegroundModule,
  ) {}

  setTickScheduler(callback: (first: Error | string | null, second?: string) => void): void {
    if (!this.runtime.setTickScheduler) {
      throw new Error(
        "React Native native foreground runtime does not expose owner wake scheduling",
      );
    }
    this.runtime.setTickScheduler((urgency) => {
      // Native invalidation clears the JSI registration first, but a
      // CallInvoker task may already have crossed into JS when close races
      // teardown. Do not let that stale task re-enter the adapter after its
      // foreground facade has become unusable.
      if (!this.closed) callback(urgency);
    });
  }

  onMutationError(callback: (event: MutationErrorEvent) => void): void {
    this.assertOpen();
    this.mutationErrorCallback = callback;
  }

  all(
    query: Uint8Array,
    opts: unknown,
    openTransactionId?: string,
  ): Uint8Array | { poll(): Uint8Array | null } {
    const transaction =
      openTransactionId === undefined
        ? undefined
        : this.openTransaction(openTransactionId, "read").handle;
    // An attached foreground is an ordinary peer of the native relay. One
    // bounded relay turn admits already-persisted rows before materializing a
    // local read; without it a newly opened foreground can only observe rows
    // after some unrelated caller happens to tick the host.
    this.tick();
    const response = this.execute({
      type: "all",
      query,
      optionsJson: JSON.stringify(opts ?? {}),
      transaction,
    });
    if (response.type === "rows") return response.rows;
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type === "pending") return this.pendingRows(response.operation);
    return unexpected("all", response.type);
  }

  // The shared mutation adapter needs one exact local row, without coverage
  // registration or hydration, to merge staged patches synchronously.
  localCurrentRow(table: string, rowId: Uint8Array): Uint8Array {
    const response = this.execute({ type: "localCurrentRow", table, rowId });
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "rows") return unexpected("localCurrentRow", response.type);
    return response.rows;
  }

  subscribe(query: Uint8Array, opts: unknown): NativeForegroundSubscription {
    this.tick();
    const response = this.execute({
      type: "subscribe",
      query,
      optionsJson: JSON.stringify(opts ?? {}),
    });
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "subscribed") return unexpected("subscribe", response.type);
    return new NativeForegroundSubscription(response.subscription, this);
  }

  tick(): void {
    this.assertOpen();
    this.runtime.tick();
    if (this.mutationErrorCallback) {
      const response = this.execute({ type: "drainMutationErrors" });
      if (response.type === "operationError") throw new Error(response.reason);
      if (response.type !== "mutationErrors")
        return unexpected("drainMutationErrors", response.type);
      const events = JSON.parse(response.eventsJson) as MutationErrorEvent[];
      for (const event of events)
        queueMicrotask(() => {
          if (!this.closed) this.mutationErrorCallback?.(event);
        });
    }
  }

  close(): boolean {
    if (this.closed) return false;
    this.closed = true;
    this.transactions.clear();
    try {
      const response = this.executeAllowClosed("close");
      if (response.type !== "closed") return unexpected("close", response.type);
      return response.closed;
    } finally {
      // The command owns logical Db closure. The HostObject close releases the
      // JSI handle and is deliberately safe to call after that transition.
      // It must also run when command execution fails (for example, because a
      // platform logout revoked the capability just before this call).
      this.runtime.close();
    }
  }

  isNativeForegroundClosed(): boolean {
    // Only a typed native closed/revoked receipt suppresses a stale wake.
    // Missing support or any unexpected native/probe failure is not closure.
    return this.closed || this.runtime.isClosed?.() === true;
  }

  nativeSessionMetadata(): {
    node: Uint8Array;
    registryAuthority: string;
    accountId: string | null;
    issuer: string;
    userId: string;
  } {
    const response = this.execute({ type: "nativeSessionMetadata" });
    if (response.type !== "nativeSessionMetadata")
      return unexpected("nativeSessionMetadata", response.type);
    return response;
  }

  disconnectNativeUpstream(): void {
    const response = this.execute({ type: "disconnectNativeUpstream" });
    if (response.type !== "nativeConnectionStatus")
      return unexpected("disconnectNativeUpstream", response.type);
  }

  reconnectNativeUpstream(): void {
    const response = this.execute({ type: "reconnectNativeUpstream" });
    if (response.type !== "nativeConnectionStatus")
      return unexpected("reconnectNativeUpstream", response.type);
  }

  nativeConnectionStatus(): NativeConnectionStatus {
    const response = this.execute({ type: "nativeConnectionStatus" });
    if (response.type !== "nativeConnectionStatus")
      return unexpected("nativeConnectionStatus", response.type);
    return response;
  }

  rejectAuthUpdate(): never {
    throw new Error(
      "React Native native foreground cannot rotate authentication in place; keep the existing admitted session or revoke the old native capability and create a new Db for the newly admitted session",
    );
  }

  drain(subscription: number, operation?: number): unknown[] | { pendingOperation: number } {
    const response =
      operation === undefined
        ? this.execute({ type: "drainSubscription", subscription })
        : this.execute({ type: "poll", operation });
    if (response.type === "pending") return { pendingOperation: response.operation };
    if (response.type === "operationError") {
      throw new Error(`React Native native foreground subscription failed: ${response.reason}`);
    }
    if (response.type !== "subscriptionEvents")
      return unexpected("drainSubscription", response.type);
    return response.events.map((event) => {
      if (event.type === "delta") return event;
      if (event.type === "closed") return event;
      return {
        type: "rejected",
        reason: { type: "ServerFailure", code: event.reason },
      };
    });
  }

  unsubscribe(subscription: number): boolean {
    if (this.closed) return false;
    const response = this.execute({ type: "unsubscribe", subscription });
    if (response.type !== "unsubscribed") return unexpected("unsubscribe", response.type);
    // Native unsubscribe retires the stream handle synchronously but queues
    // its core cleanup for the following ordinary turn. Drive that turn here:
    // after the JS reader stops polling there may be no unrelated operation
    // to do it for us.
    if (response.closed) this.tick();
    return response.closed;
  }

  requestInsertPermissionAdvice(table: string, cells: Uint8Array) {
    return this.requestPermissionAdvice({ type: "insert", table, cells });
  }

  requestReadPermissionAdvice(table: string, rowId: Uint8Array) {
    return this.requestPermissionAdvice({ type: "read", table, rowId });
  }

  requestUpdatePermissionAdvice(table: string, rowId: Uint8Array, patch: Uint8Array) {
    return this.requestPermissionAdvice({ type: "update", table, rowId, patch });
  }

  requestDeletePermissionAdvice(table: string, rowId: Uint8Array) {
    return this.requestPermissionAdvice({ type: "delete", table, rowId });
  }

  private requestPermissionAdvice(action: ForegroundPermissionAdviceAction) {
    let response = this.execute({ type: "permissionAdvice", action });
    if (response.type === "permissionAdvice") return response.advice;
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "pending") return unexpected("permissionAdvice", response.type);
    const operation = response.operation;
    let completed = false;
    return {
      poll: (): string | null => {
        if (completed || this.closed) return "unknown";
        this.tick();
        response = this.execute({ type: "poll", operation });
        if (response.type === "pending") return null;
        completed = true;
        if (response.type === "operationError") throw new Error(response.reason);
        if (response.type !== "permissionAdvice")
          return unexpected("permissionAdvice", response.type);
        return response.advice;
      },
      cancel: (): boolean => {
        if (completed || this.closed) return false;
        completed = true;
        const cancelled = this.execute({ type: "cancel", operation });
        if (cancelled.type !== "cancelled")
          return unexpected("permissionAdvice cancellation", cancelled.type);
        return cancelled.cancelled;
      },
    };
  }

  async waitForPendingWrites(tier: string): Promise<Uint8Array> {
    let response = this.execute({ type: "waitForPendingWrites", tier });
    while (response.type === "pending") {
      const operation = response.operation;
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      this.tick();
      response = this.execute({ type: "poll", operation });
    }
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "rows" || response.rows.length !== 0)
      return unexpected("waitForPendingWrites", response.type);
    return response.rows;
  }

  async waitForTransaction(txId: Uint8Array, tier: string): Promise<void> {
    if (!["local", "edge", "global"].includes(tier)) {
      throw new Error(`Unsupported write durability tier: ${tier}`);
    }
    let response = this.execute({ type: "waitForTransaction", txId, tier });
    while (response.type === "pending") {
      const operation = response.operation;
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      this.tick();
      response = this.execute({ type: "poll", operation });
    }
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "transactionSettled")
      return unexpected("waitForTransaction", response.type);
  }

  private execute(command: ForegroundCommand): ForegroundResponse {
    this.assertOpen();
    return this.executeAllowClosed(command);
  }

  private pendingRows(operation: number): { poll(): Uint8Array | null } {
    return {
      poll: () => {
        this.tick();
        const response = this.execute({ type: "poll", operation });
        if (response.type === "pending") return null;
        if (response.type === "operationError") {
          throw new Error(`React Native native foreground read failed: ${response.reason}`);
        }
        if (response.type !== "rows") return unexpected("poll", response.type);
        return response.rows;
      },
    };
  }

  private executeAllowClosed(command: ForegroundCommand): ForegroundResponse {
    return this.commands.decodeNativeForegroundResponse(
      this.runtime.execute(this.commands.encodeNativeForegroundCommand(command)),
    );
  }

  private assertOpen(): void {
    if (this.closed) throw new Error("React Native native foreground runtime is closed");
  }

  // The admitted native host already owns and validates the schema. A schema
  // view therefore shares this foreground rather than opening a second store.
  registerSchema(): NativeForegroundDb {
    return this;
  }

  beginTransaction(openTransactionId: string, kind: "mergeable" | "exclusive"): void {
    this.assertOpen();
    if (this.transactions.has(openTransactionId)) {
      throw new Error(
        `React Native native foreground transaction ${openTransactionId} is already open`,
      );
    }
    const response = this.execute({ type: "beginTransaction", kind });
    if (response.type !== "transactionOpened") return unexpected("beginTransaction", response.type);
    this.transactions.set(openTransactionId, {
      handle: response.transaction,
      kind,
      closed: false,
    });
  }

  commitTransaction(openTransactionId: string): NativeForegroundWrite {
    const transaction = this.openTransaction(openTransactionId, "commit");
    const response = this.execute({
      type: "commitTransaction",
      transaction: transaction.handle,
    });
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "transactionCommitted")
      return unexpected("commitTransaction", response.type);
    this.transactions.delete(openTransactionId);
    transaction.closed = true;
    return nativeWrite(this, response.txId);
  }

  rollbackTransaction(openTransactionId: string): void {
    const transaction = this.openTransaction(openTransactionId, "rollback");
    const response = this.execute({
      type: "rollbackTransaction",
      transaction: transaction.handle,
    });
    if (response.type !== "transactionRolledBack")
      return unexpected("rollbackTransaction", response.type);
    this.transactions.delete(openTransactionId);
    transaction.closed = true;
  }

  insert(
    table: string,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.directMutation("insert", table, options?.rowId, cells, options);
  }

  insertInTransaction(
    openTransactionId: string,
    table: string,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): Uint8Array {
    return this.stageMutation(
      this.openTransaction(openTransactionId, "insert into"),
      "insert",
      table,
      options?.rowId,
      cells,
      options,
    );
  }

  update(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.directMutation("update", table, rowId, patch, options);
  }

  updateInTransaction(
    openTransactionId: string,
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    options?: ForegroundMutationOptions,
  ): void {
    this.stageMutation(
      this.openTransaction(openTransactionId, "update in"),
      "update",
      table,
      rowId,
      patch,
      options,
    );
  }

  upsert(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.directMutation("upsert", table, rowId, cells, options);
  }

  upsertInTransaction(
    openTransactionId: string,
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): void {
    this.stageMutation(
      this.openTransaction(openTransactionId, "upsert into"),
      "upsert",
      table,
      rowId,
      cells,
      options,
    );
  }

  delete(
    table: string,
    rowId: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.directMutation("delete", table, rowId, new Uint8Array(), options);
  }

  deleteInTransaction(
    openTransactionId: string,
    table: string,
    rowId: Uint8Array,
    options?: ForegroundMutationOptions,
  ): void {
    this.stageMutation(
      this.openTransaction(openTransactionId, "delete from"),
      "delete",
      table,
      rowId,
      new Uint8Array(),
      options,
    );
  }

  restore(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    return this.directMutation("restore", table, rowId, cells, options);
  }

  restoreInTransaction(
    openTransactionId: string,
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): void {
    this.stageMutation(
      this.openTransaction(openTransactionId, "restore in"),
      "restore",
      table,
      rowId,
      cells,
      options,
    );
  }

  connectUpstream(): never {
    return unsupported("JavaScript upstream transport");
  }

  writeState(txId: Uint8Array): unknown {
    const response = this.execute({ type: "writeState", txId });
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "writeState") return unexpected("writeState", response.type);
    return JSON.parse(response.stateJson);
  }

  updateLargeValues(
    table: string,
    rowId: Uint8Array,
    patch: Uint8Array,
    descriptors: readonly unknown[],
    updatedAtMs?: number,
  ): NativeForegroundWrite {
    const response = this.execute({
      type: "updateLargeValues",
      table,
      rowId,
      patch,
      descriptorsJson: JSON.stringify(descriptors),
      updatedAtMs,
    });
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "transactionCommitted")
      return unexpected("updateLargeValues", response.type);
    return nativeWrite(this, response.txId, rowId);
  }

  beginStreamingMutation(
    table: string,
    rowId: Uint8Array,
    cells: Uint8Array,
    column: string,
    mutation: "insert" | "update" | "upsert" = "insert",
    _author?: Uint8Array,
    _attribution?: Uint8Array,
    updatedAtMs?: number,
    head?: unknown,
    base?: unknown,
  ) {
    if (updatedAtMs !== undefined && (!Number.isSafeInteger(updatedAtMs) || updatedAtMs < 0)) {
      throw new Error("updatedAtMs must be a non-negative safe integer");
    }
    const response = this.execute({
      type: "beginStreamingMutation",
      mutation,
      table,
      rowId,
      cells,
      column,
      optionsJson: JSON.stringify({ updatedAtMs, head, base }),
    });
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "streamingMutationOpened")
      return unexpected("beginStreamingMutation", response.type);
    const upload = response.upload;
    let closed = false;
    const assertOpen = () => {
      if (closed) throw new Error("streaming mutation is closed");
      this.assertOpen();
    };
    return {
      push: async (chunk: Uint8Array): Promise<void> => {
        assertOpen();
        const pushed = await this.completeMutationOperation(
          this.execute({ type: "pushStreamingMutation", upload, chunk }),
        );
        if (pushed.type === "operationError") throw new Error(pushed.reason);
        if (pushed.type !== "streamingMutationPushed")
          return unexpected("pushStreamingMutation", pushed.type);
      },
      finish: async (): Promise<NativeForegroundWrite> => {
        assertOpen();
        let finished = this.execute({ type: "finishStreamingMutation", upload });
        if (finished.type === "operationError") throw new Error(finished.reason);
        closed = true;
        while (finished.type === "pending") {
          const operation = finished.operation;
          await new Promise<void>((resolve) => setTimeout(resolve, 0));
          this.tick();
          finished = this.execute({ type: "poll", operation });
        }
        if (finished.type === "operationError") throw new Error(finished.reason);
        if (finished.type !== "transactionCommitted")
          return unexpected("finishStreamingMutation", finished.type);
        return nativeWrite(this, finished.txId, rowId);
      },
      abort: async (): Promise<boolean> => {
        if (closed || this.closed) return false;
        const admitted = this.execute({ type: "abortStreamingMutation", upload });
        if (admitted.type === "operationError") throw new Error(admitted.reason);
        closed = true;
        const aborted = await this.completeMutationOperation(admitted);
        if (aborted.type === "operationError") throw new Error(aborted.reason);
        if (aborted.type !== "streamingMutationAborted")
          return unexpected("abortStreamingMutation", aborted.type);
        return aborted.aborted;
      },
    };
  }

  private async completeMutationOperation(
    response: ForegroundResponse,
  ): Promise<ForegroundResponse> {
    while (response.type === "pending") {
      const operation = response.operation;
      await new Promise<void>((resolve) => setTimeout(resolve, 0));
      this.tick();
      response = this.execute({ type: "poll", operation });
    }
    return response;
  }

  private directMutation(
    mutation: ForegroundMutationKind,
    table: string,
    rowId: Uint8Array | undefined,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): NativeForegroundWrite {
    if (mutation === "upsert" && options && "branch" in options)
      throw new Error("upsert option `branch` is not supported; use `head`");
    if (
      options?.updatedAtMs !== undefined &&
      (!Number.isSafeInteger(options.updatedAtMs) || options.updatedAtMs < 0)
    )
      throw new Error("updatedAtMs must be a non-negative safe integer");
    const {
      rowId: _rowId,
      author: _author,
      attribution: _attribution,
      ...wireOptions
    } = options ?? {};
    const response = this.execute({
      type: "directMutation",
      mutation,
      table,
      rowId,
      cells,
      optionsJson: JSON.stringify(wireOptions),
    });
    if (response.type === "operationError") throw new Error(response.reason);
    if (response.type !== "mutationCommitted") return unexpected(mutation, response.type);
    return nativeWrite(this, response.txId, response.rowId);
  }

  private stageMutation(
    transaction: NativeForegroundTransaction,
    mutation: ForegroundMutationKind,
    table: string,
    rowId: Uint8Array | undefined,
    cells: Uint8Array,
    options?: ForegroundMutationOptions,
  ): Uint8Array {
    if (transaction.closed) throw new Error("React Native native foreground transaction is closed");
    if (mutation === "upsert" && options && "branch" in options) {
      throw new Error(
        "upsert option `branch` is not supported; use `head` (and optional `base`) for a branch view",
      );
    }
    if (
      options?.updatedAtMs !== undefined &&
      (!Number.isSafeInteger(options.updatedAtMs) || options.updatedAtMs < 0)
    ) {
      throw new Error("updatedAtMs must be a non-negative safe integer");
    }
    // Identity belongs to the admitted native capability, never the command.
    const {
      rowId: _rowId,
      author: _author,
      attribution: _attribution,
      ...wireOptions
    } = options ?? {};
    const response = this.execute({
      type: "stageMutation",
      transaction: transaction.handle,
      mutation,
      table,
      rowId,
      cells,
      optionsJson: JSON.stringify(wireOptions),
    });
    if (response.type === "operationError") throw new Error(response.reason);
    if (mutation === "insert" && response.type === "inserted") return response.rowId;
    if (mutation !== "insert" && response.type === "mutationStaged") return rowId!;
    return unexpected(mutation, response.type);
  }

  private openTransaction(id: string, operation: string): NativeForegroundTransaction {
    const transaction = this.transactions.get(id);
    if (!transaction || transaction.closed) {
      throw new Error(
        `React Native native foreground cannot ${operation} unknown transaction ${id}`,
      );
    }
    return transaction;
  }
}

type NativeForegroundTransaction = {
  handle: number;
  kind: "mergeable" | "exclusive";
  closed: boolean;
};

type NativeForegroundWrite = {
  /** Public write identity. Native binding spellings are normalized here. */
  readonly txId: string;
  readonly payload: Uint8Array;
  readonly rowId: Uint8Array;
  wait(tier: string): Promise<void>;
  writeState(): unknown;
  close(): boolean;
};

function nativeWrite(
  db: NativeForegroundDb,
  txId: Uint8Array<ArrayBufferLike>,
  rowId: Uint8Array<ArrayBufferLike> = new Uint8Array(16),
): NativeForegroundWrite {
  const id = formatTransactionId(txId);
  let closed = false;
  return {
    txId: id,
    payload: new Uint8Array(),
    rowId: rowId.slice(),
    async wait(tier: string): Promise<void> {
      if (closed) throw new Error("write state is unavailable");
      await db.waitForTransaction(txId, tier);
    },
    writeState: () => {
      if (closed) throw new Error("write state is unavailable");
      return db.writeState(txId);
    },
    close: () => {
      if (closed) return false;
      closed = true;
      return true;
    },
  };
}

class NativeForegroundSubscription {
  private closed = false;
  private pendingOperation: number | undefined;

  constructor(
    private readonly handle: number,
    private readonly db: NativeForegroundDb,
  ) {}

  readAll(): unknown[] | { retryAfterMs(): number } {
    if (this.closed) return [];
    // There is no native-to-JS wake callback in this first slice. Polling must
    // therefore also drive one fair relay turn before observing the stream;
    // Drain alone only consumes events which some other caller has advanced.
    this.db.tick();
    const events = this.db.drain(this.handle, this.pendingOperation);
    if (!Array.isArray(events)) {
      this.pendingOperation = events.pendingOperation;
      return { retryAfterMs: () => 0 };
    }
    this.pendingOperation = undefined;
    // The first command slice has no native-to-JS wake callback yet. Keep the
    // normal NativeRuntimeAdapter subscription reader suspended on its
    // existing retry contract so later native deltas are observable without
    // a second subscription implementation. This polling seam is deliberately
    // capability-gated and can become a wake-driven pending batch later.
    return events.length === 0 ? { retryAfterMs: () => 50 } : events;
  }

  close(): boolean {
    if (this.closed) return false;
    this.closed = true;
    return this.db.unsubscribe(this.handle);
  }
}

function unsupported(operation: string): never {
  throw new Error(`${REACT_NATIVE_UNSUPPORTED_ERROR}; ${operation} is unavailable`);
}

function unexpected(operation: string, response: string): never {
  throw new Error(
    `React Native native foreground ${operation} returned unexpected ${response} response`,
  );
}

function formatTransactionId(bytes: Uint8Array<ArrayBufferLike>): string {
  if (bytes.byteLength !== 16)
    throw new Error("React Native native foreground returned malformed transaction id");
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return hex;
}
