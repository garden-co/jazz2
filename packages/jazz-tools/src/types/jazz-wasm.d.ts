declare module "jazz-wasm" {
  export default function init(input?: unknown): Promise<void>;
  export function initSync(input?: unknown): void;
  export function nativeArtifactFingerprint(): string;
  export function generateId(): string;
  export function currentTimestamp(): bigint;
  export function deriveUserId(seedB64: string): string;
  export function mintLocalFirstToken(
    seedB64: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;
  export function mintAnonymousToken(
    seedB64: string,
    audience: string,
    ttlSeconds: number,
    nowSeconds: bigint,
  ): string;

  type PendingNativeOperation<T> = {
    poll(): T | undefined;
    cancel(): void;
    setWake(callback: () => void): void;
  };
  export class PendingNativeRead {
    poll(): Uint8Array | null;
  }
  export class WasmPermissionAdviceRequest {
    readonly promise: Promise<"allowed" | "denied" | "unknown">;
    cancel(): void;
  }

  export class WasmWrite {
    readonly txId: string;
    readonly payload: Uint8Array;
    readonly rowId: Uint8Array;
    writeState(): unknown;
    wait(tier: string): Promise<void>;
    close(): boolean;
  }

  export class StreamingMutation {
    push(chunk: Uint8Array): Promise<void>;
    finish(): Promise<WasmWrite>;
    abort(): Promise<boolean>;
  }

  export class WasmTransport {
    sendWireFrame(frame: Uint8Array): void;
    recvWireFrames(): Uint8Array[];
    routeAuxiliaryWireFrame(frame: Uint8Array): Promise<Uint8Array | undefined>;
    recvAuxiliaryWireFrames(maxFrames?: number, maxBytes?: number): Uint8Array[];
    auxiliaryOutboundReady(): Promise<void>;
    tick(): Promise<number>;
    updateAuthenticatedClaims(claims: Record<string, unknown>): Promise<void>;
    close(): boolean;
  }

  export type WriteOptions = {
    author?: Uint8Array;
    attribution?: Uint8Array;
    updatedAtMs?: number;
  };

  export type InsertOptions = WriteOptions & {
    rowId?: Uint8Array;
    branch?: unknown;
  };

  export type UpdateOptions = WriteOptions & {
    head?: unknown;
    base?: unknown;
  };

  export type UpsertOptions = UpdateOptions;

  export type DeleteOptions = UpdateOptions;
  export type RestoreOptions = WriteOptions & {
    branch?: unknown;
  };

  export class WasmDb {
    static openMemory(schema: Uint8Array, config: Uint8Array): WasmDb;
    /** Explicit trusted-backend ABI; raw open config cannot select SYSTEM. */
    static openMemoryAsBackend(schema: Uint8Array, config: Uint8Array): WasmDb;
    static openMemoryWithSelfSignedProof(
      schema: Uint8Array,
      config: Uint8Array,
      token: string,
      appId: string,
      claimedAuthor: string,
    ): WasmDb;
    /** Host-only relay open; `storageOwner` is supplied by broker ownership admission. */
    static openBrowser(
      pageStore: unknown,
      schema: Uint8Array,
      config: Uint8Array,
      storageOwner: string,
    ): Promise<WasmDb>;
    static openBrowserWithSelfSignedProof(
      pageStore: unknown,
      schema: Uint8Array,
      config: Uint8Array,
      token: string,
      appId: string,
      claimedAuthor: string,
      storageOwner: string,
    ): Promise<WasmDb>;
    setLargeValueStagingPolicy(
      incomingBytesPerWindow: number,
      windowMs: number,
      maxAgeMs?: number | null,
    ): void;
    evictExpiredStagedLargeValues(): Promise<number>;
    beginStreamingMutation(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      column: string,
      mutation?: "insert" | "update" | "upsert",
      author?: Uint8Array,
      attribution?: Uint8Array,
      updatedAtMs?: number,
      head?: unknown,
      base?: unknown,
    ): StreamingMutation;
    static destroyBrowserStorage(namespace: string): Promise<void>;

    registerSchema(schema: Uint8Array): WasmDb;
    beginTransaction(
      openTransactionId: string,
      kind: string,
      author?: Uint8Array | null,
      attribution?: Uint8Array | null,
    ): void;
    commitTransaction(openTransactionId: string, kind?: string | null): WasmWrite;
    rollbackTransaction(openTransactionId: string): void;

    all(
      query: Uint8Array,
      opts: unknown,
      openTransactionId?: string,
      author?: Uint8Array,
      claims?: Record<string, unknown>,
    ): Uint8Array | PendingNativeRead;
    subscribe(
      query: Uint8Array,
      opts: unknown,
      author?: Uint8Array,
      claims?: Record<string, unknown>,
    ): ReadableStream<unknown> | PendingNativeOperation<ReadableStream<unknown>>;

    insert(table: string, cells: Uint8Array, options?: InsertOptions): WasmWrite;
    insertInTransaction(
      openTransactionId: string,
      table: string,
      cells: Uint8Array,
      options?: InsertOptions,
    ): Uint8Array;
    canInsert(table: string, cells: Uint8Array): "allowed" | "denied" | "unknown";
    requestInsertPermissionAdvice(table: string, cells: Uint8Array): WasmPermissionAdviceRequest;
    requestReadPermissionAdvice(table: string, rowId: Uint8Array): WasmPermissionAdviceRequest;
    update(table: string, rowId: Uint8Array, patch: Uint8Array, options?: UpdateOptions): WasmWrite;
    updateInTransaction(
      openTransactionId: string,
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      options?: UpdateOptions,
    ): void;
    updateLargeValues(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
      descriptors: unknown,
      updatedAtMs?: number | null,
    ): WasmWrite;
    requestUpdatePermissionAdvice(
      table: string,
      rowId: Uint8Array,
      patch: Uint8Array,
    ): WasmPermissionAdviceRequest;
    requestDeletePermissionAdvice(table: string, rowId: Uint8Array): WasmPermissionAdviceRequest;
    upsert(table: string, rowId: Uint8Array, cells: Uint8Array, options?: UpsertOptions): WasmWrite;
    upsertInTransaction(
      openTransactionId: string,
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      options?: UpsertOptions,
    ): void;
    delete(table: string, rowId: Uint8Array, options?: DeleteOptions): WasmWrite;
    deleteInTransaction(
      openTransactionId: string,
      table: string,
      rowId: Uint8Array,
      options?: DeleteOptions,
    ): void;
    restore(
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      options?: RestoreOptions,
    ): WasmWrite;
    restoreInTransaction(
      openTransactionId: string,
      table: string,
      rowId: Uint8Array,
      cells: Uint8Array,
      options?: RestoreOptions,
    ): void;
    setTickScheduler(
      callback: (urgency: "immediate" | "deferred" | `after:${number}`) => void,
    ): void;
    onMutationError(callback: (event: any) => void): void;
    tick(): Promise<void>;
    setNonDurableClient(): void;
    /** @internal Foreground node-lease handoff only. */
    foregroundTxTimeHighWater(): bigint;
    /** @internal Foreground node-lease bootstrap only. */
    seedForegroundTxTimeHighWater(highWater: bigint): void;
    /** Exact wire features compiled into this WASM artifact. */
    wireFeatures(): number;
    close(): Promise<boolean>;
    setSessionClaims(claims: Record<string, unknown> | null): void | Promise<void>;
    connectUpstream(): Promise<WasmTransport>;
    connectUpstreamWithSession(
      protocolVersion: number,
      features: number,
      remoteNode: Uint8Array,
      remoteEpoch: bigint,
      localNode: Uint8Array,
      localEpoch: bigint,
    ): Promise<WasmTransport>;
    acceptSubscriber(
      identity: Uint8Array,
      claims: Record<string, unknown>,
      localEpoch?: bigint,
    ): Promise<WasmTransport>;
    acceptSubscriberWithSelfSignedProof(
      claims: Record<string, unknown>,
      token: string,
      appId: string,
      claimedAuthor: string,
      localEpoch?: bigint,
    ): Promise<WasmTransport>;
  }
}
