import type { LocalBatchRecord } from "../runtime/client.js";

declare module "jazz-wasm" {
  type SyncOutboxCallbackArgs =
    | [
        destinationKind: "server" | "client",
        destinationId: string,
        payload: string | Uint8Array,
        isCatalogue: boolean,
      ]
    | [
        err: unknown,
        destinationKind: "server" | "client",
        destinationId: string,
        payload: string | Uint8Array,
        isCatalogue: boolean,
      ];
  type SyncOutboxCallback = (...args: SyncOutboxCallbackArgs) => void;
  type InsertValues = Record<string, unknown>;

  export type WasmTraceEntry =
    | {
        kind: "span";
        sequence: number;
        name: string;
        target: string;
        level: string;
        startUnixNano: string;
        endUnixNano: string;
        fields: Record<string, string>;
      }
    | {
        kind: "log";
        sequence: number;
        target: string;
        level: string;
        timestampUnixNano: string;
        message: string;
        fields: Record<string, string>;
      }
    | { kind: "dropped"; count: number };

  export default function init(input?: unknown): Promise<void>;
  export function initSync(input?: unknown): void;
  export function setTraceEntryCollectionEnabled(enabled: boolean): void;
  export function drainTraceEntries(): WasmTraceEntry[];
  export function subscribeTraceEntries(callback: () => void): () => void;

  export class WasmRuntime {
    constructor(
      schemaJson: string,
      appId: string,
      env: string,
      userBranch: string,
      tier?: string,
      useBinaryEncoding?: boolean,
    );
    schedule?: (task: () => void) => void;

    insert(
      table: string,
      values: InsertValues,
      objectId?: string | null,
    ): { id: string; values: any[]; batchId: string };
    insertWithSession(
      table: string,
      values: InsertValues,
      sessionJson?: string | null,
      objectId?: string | null,
    ): { id: string; values: any[]; batchId: string };
    update(objectId: string, values: unknown): { batchId: string };
    updateWithSession(
      objectId: string,
      values: unknown,
      sessionJson?: string | null,
    ): { batchId: string };
    delete(objectId: string): { batchId: string };
    deleteWithSession(objectId: string, sessionJson?: string | null): { batchId: string };
    loadLocalBatchRecord(batchId: string): LocalBatchRecord | null;
    loadLocalBatchRecords(): LocalBatchRecord[];
    drainRejectedBatchIds(): string[];
    acknowledgeRejectedBatch(batchId: string): boolean;
    sealBatch(batchId: string): void;
    query(
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ): Promise<unknown>;
    createSubscription(
      queryJson: string,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ): number;
    executeSubscription(handle: number, onUpdate: Function): void;
    subscribe(
      queryJson: string,
      onUpdate: Function,
      sessionJson?: string | null,
      tier?: string | null,
      optionsJson?: string | null,
    ): number;
    unsubscribe(handle: number): void;
    onSyncMessageReceived(messageJson: string, seq?: number | null): void;
    onSyncMessageToSend(callback: SyncOutboxCallback): void;
    addServer(serverCatalogueStateHash?: string | null, nextSyncSeq?: number | null): void;
    removeServer(): void;
    addClient(): string;
    getSchema(): unknown;
    getSchemaHash(): string;
    close?(): void;
    setClientRole?(clientId: string, role: string): void;
    onSyncMessageReceivedFromClient?(clientId: string, messageJson: string): void;

    /** Derive a deterministic user ID (UUIDv5) from a base64url-encoded seed. */
    static deriveUserId(seedB64: string): string;
    /** Mint a Jazz self-signed JWT from a base64url-encoded seed. */
    static mintJazzSelfSignedToken(
      seedB64: string,
      issuer: string,
      audience: string,
      ttlSeconds: bigint,
      nowSeconds: bigint,
    ): string;
    /** Get the Ed25519 public key as base64url from a base64url-encoded seed. */
    static getPublicKeyBase64url(seedB64: string): string;
  }
}
