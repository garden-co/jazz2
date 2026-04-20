declare module "jazz-wasm" {
  export class WorkerClient {
    constructor(worker: Worker);

    /** Send InitPayload to the worker; returns the assigned client_id. */
    init(payload: {
      schema_json: string;
      app_id: string;
      env: string;
      user_branch: string;
      db_name: string;
      server_url?: string;
      server_path_prefix?: string;
      jwt_token?: string;
      admin_secret?: string;
      log_level?: string;
      fallback_wasm_url?: string;
    }): Promise<string>;

    shutdown(): Promise<void>;

    /** Send a sync frame (main-thread → worker). */
    send_sync(bytes: Uint8Array): void;
    send_peer_sync(peer_id: string, term: number, bytes: Uint8Array): void;
    peer_open(peer_id: string): void;
    peer_close(peer_id: string): void;

    update_auth(jwt?: string): void;
    disconnect_upstream(): void;
    reconnect_upstream(): void;

    lifecycle_hint(event: string, sent_at_ms: number): void;
    simulate_crash(): void;

    debug_schema_state(): Promise<unknown>;
    debug_seed_live_schema(schema_json: string): Promise<void>;

    /** Wire the main-thread WasmRuntime outbox to the worker. */
    installOnRuntime(runtime: import("jazz-wasm").WasmRuntime): void;

    /**
     * Set (or clear) the JS callback for `Destination::Server` outbox entries.
     * Pass `undefined` to restore leader mode (forward to worker).
     */
    setServerPayloadForwarder(cb: ((payload: Uint8Array) => void) | undefined): void;

    // Callback setters (worker → main).
    set_on_ready(cb: () => void): void;
    /** Called with a Uint8Array when a sync frame arrives from the worker. */
    set_on_sync(cb: (bytes: Uint8Array) => void): void;
    /** Called with (peerId: string, term: number, bytes: Uint8Array) for peer-sync frames. */
    set_on_peer_sync(cb: (peerId: string, term: number, bytes: Uint8Array) => void): void;
    /** Called with `true` when upstream connects, `false` when it disconnects. */
    set_on_upstream_status(cb: (connected: boolean) => void): void;
    /** Called with an AuthFailureReason string when auth fails. */
    set_on_auth_failed(cb: (reason: string) => void): void;
    set_on_error(cb: (msg: string) => void): void;
  }

  type InsertValues = Record<string, unknown>;

  export default function init(input?: unknown): Promise<void>;
  export function initSync(input?: unknown): void;

  /** Dedicated-worker entry point. Owns the full dispatch loop. */
  export function runWorker(): Promise<void>;

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
    ): { id: string; values: any[] };
    insertWithSession(
      table: string,
      values: InsertValues,
      sessionJson?: string | null,
      objectId?: string | null,
    ): { id: string; values: any[] };
    insertDurable(
      table: string,
      values: InsertValues,
      tier: string,
      objectId?: string | null,
    ): Promise<{ id: string; values: any[] }>;
    insertDurableWithSession(
      table: string,
      values: InsertValues,
      sessionJson: string | null | undefined,
      tier: string,
      objectId?: string | null,
    ): Promise<{ id: string; values: any[] }>;
    update(objectId: string, values: unknown): void;
    updateWithSession(objectId: string, values: unknown, sessionJson?: string | null): void;
    updateDurable(objectId: string, values: unknown, tier: string): Promise<void>;
    updateDurableWithSession(
      objectId: string,
      values: unknown,
      sessionJson: string | null | undefined,
      tier: string,
    ): Promise<void>;
    delete(objectId: string): void;
    deleteWithSession(objectId: string, sessionJson?: string | null): void;
    deleteDurable(objectId: string, tier: string): Promise<void>;
    deleteDurableWithSession(
      objectId: string,
      sessionJson: string | null | undefined,
      tier: string,
    ): Promise<void>;
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
    /** Mint a local-first JWT from a base64url-encoded seed. */
    static mintLocalFirstToken(
      seedB64: string,
      audience: string,
      ttlSeconds: bigint,
      nowSeconds: bigint,
    ): string;
    /** Get the Ed25519 public key as base64url from a base64url-encoded seed. */
    static getPublicKeyBase64url(seedB64: string): string;
  }
}
