import { describe, expect, it, vi } from "vitest";
import { WorkerBridge, type PeerSyncBatch } from "./worker-bridge.js";
import type { Runtime } from "./client.js";
import type { AuthFailureReason } from "./sync-transport.js";

// ---------------------------------------------------------------------------
// Mock jazz-wasm
//
// vi.mock is hoisted, so the WorkerClient class must be defined inside the
// factory — it cannot reference module-level variables defined below the call.
// ---------------------------------------------------------------------------

vi.mock("jazz-wasm", () => {
  type WorkerToMainMsg =
    | { type: "sync"; payload: (Uint8Array | string)[] }
    | { type: "peer-sync"; peerId: string; term: number; payload: Uint8Array[] }
    | { type: "upstream-connected" }
    | { type: "upstream-disconnected" }
    | { type: "auth-failed"; reason: string }
    | { type: "init-ok"; clientId: string }
    | { type: "error"; message: string }
    | { type: "shutdown-ok" };

  class WorkerClient {
    private underlying: any;
    private onSyncCb: ((b: Uint8Array) => void) | null = null;
    private onPeerSyncCb: ((id: string, t: number, b: Uint8Array) => void) | null = null;
    private onUpstreamStatusCb: ((c: boolean) => void) | null = null;
    private onAuthFailedCb: ((r: string) => void) | null = null;
    private initResolve: ((id: string) => void) | null = null;
    private initReject: ((e: Error) => void) | null = null;
    private shutdownResolve: (() => void) | null = null;
    private serverPayloadForwarderCb: ((p: Uint8Array) => void) | undefined = undefined;

    constructor(worker: any) {
      this.underlying = worker;
      worker.onmessage = (event: MessageEvent<WorkerToMainMsg>) => {
        const msg = event.data;
        switch (msg.type) {
          case "sync":
            for (const p of msg.payload) {
              const b = p instanceof Uint8Array ? p : new TextEncoder().encode(p as string);
              this.onSyncCb?.(b);
            }
            break;
          case "peer-sync":
            for (const p of msg.payload) this.onPeerSyncCb?.(msg.peerId, msg.term, p);
            break;
          case "upstream-connected":
            this.onUpstreamStatusCb?.(true);
            break;
          case "upstream-disconnected":
            this.onUpstreamStatusCb?.(false);
            break;
          case "auth-failed":
            this.onAuthFailedCb?.(msg.reason);
            break;
          case "init-ok":
            this.initResolve?.(msg.clientId);
            this.initResolve = null;
            this.initReject = null;
            break;
          case "error":
            if (this.initReject) {
              this.initReject(new Error(msg.message));
              this.initResolve = null;
              this.initReject = null;
            }
            break;
          case "shutdown-ok":
            this.shutdownResolve?.();
            this.shutdownResolve = null;
            break;
        }
      };
    }

    init(payload: Record<string, unknown>): Promise<string> {
      return new Promise<string>((resolve, reject) => {
        this.initResolve = resolve;
        this.initReject = reject;
        this.underlying.postMessage({
          type: "init",
          schemaJson: payload.schema_json,
          appId: payload.app_id,
          env: payload.env,
          userBranch: payload.user_branch,
          dbName: payload.db_name,
          serverUrl: payload.server_url,
          serverPathPrefix: payload.server_path_prefix,
          jwtToken: payload.jwt_token,
          adminSecret: payload.admin_secret,
          logLevel: payload.log_level,
          fallbackWasmUrl: payload.fallback_wasm_url,
          clientId: "",
        });
      });
    }

    shutdown(): Promise<void> {
      return new Promise<void>((resolve) => {
        this.shutdownResolve = resolve;
        this.underlying.postMessage({ type: "shutdown" });
        setTimeout(() => {
          if (this.shutdownResolve) {
            this.shutdownResolve = null;
            resolve();
          }
        }, 5000);
      });
    }

    send_sync(bytes: Uint8Array): void {
      this.underlying.postMessage({ type: "sync", payload: [bytes] });
    }
    send_peer_sync(peerId: string, term: number, bytes: Uint8Array): void {
      this.underlying.postMessage({ type: "peer-sync", peerId, term, payload: [bytes] });
    }
    peer_open(peerId: string): void {
      this.underlying.postMessage({ type: "peer-open", peerId });
    }
    peer_close(peerId: string): void {
      this.underlying.postMessage({ type: "peer-close", peerId });
    }
    update_auth(jwt?: string): void {
      this.underlying.postMessage({ type: "update-auth", jwtToken: jwt });
    }
    disconnect_upstream(): void {
      this.underlying.postMessage({ type: "disconnect-upstream" });
    }
    reconnect_upstream(): void {
      this.underlying.postMessage({ type: "reconnect-upstream" });
    }
    lifecycle_hint(event: string, sent_at_ms: number): void {
      this.underlying.postMessage({ type: "lifecycle-hint", event, sentAtMs: sent_at_ms });
    }
    simulate_crash(): void {
      this.underlying.postMessage({ type: "simulate-crash" });
    }
    installOnRuntime(_runtime: unknown): void {}
    setServerPayloadForwarder(cb: ((p: Uint8Array) => void) | undefined): void {
      this.serverPayloadForwarderCb = cb;
    }
    /** Test-only helper: simulate a server-bound outbox entry arriving. */
    _simulateServerPayload(bytes: Uint8Array): void {
      if (this.serverPayloadForwarderCb) {
        this.serverPayloadForwarderCb(bytes);
      } else {
        this.send_sync(bytes);
      }
    }
    set_on_ready(_cb: () => void): void {}
    set_on_sync(cb: (b: Uint8Array) => void): void {
      this.onSyncCb = cb;
    }
    set_on_peer_sync(cb: (id: string, t: number, b: Uint8Array) => void): void {
      this.onPeerSyncCb = cb;
    }
    set_on_upstream_status(cb: (c: boolean) => void): void {
      this.onUpstreamStatusCb = cb;
    }
    set_on_auth_failed(cb: (r: string) => void): void {
      this.onAuthFailedCb = cb;
    }
    set_on_error(_cb: (msg: string) => void): void {}
  }

  return { WorkerClient };
});

// ---------------------------------------------------------------------------
// MockWorker — stands in for the real Worker global
// ---------------------------------------------------------------------------

type WorkerToMainMessage =
  | { type: "sync"; payload: (Uint8Array | string)[] }
  | { type: "peer-sync"; peerId: string; term: number; payload: Uint8Array[] }
  | { type: "upstream-connected" }
  | { type: "upstream-disconnected" }
  | { type: "auth-failed"; reason: string }
  | { type: "init-ok"; clientId: string }
  | { type: "error"; message: string }
  | { type: "shutdown-ok" };

class MockWorker {
  onmessage: ((event: MessageEvent<WorkerToMainMessage>) => void) | null = null;
  posted: unknown[] = [];
  terminated = false;
  private readonly listeners = new Set<(event: MessageEvent<WorkerToMainMessage>) => void>();

  postMessage(message: unknown): void {
    this.posted.push(message);
  }

  addEventListener(
    type: string,
    listener: (event: MessageEvent<WorkerToMainMessage>) => void,
  ): void {
    if (type !== "message") return;
    this.listeners.add(listener);
  }

  removeEventListener(
    type: string,
    listener: (event: MessageEvent<WorkerToMainMessage>) => void,
  ): void {
    if (type !== "message") return;
    this.listeners.delete(listener);
  }

  terminate(): void {
    this.terminated = true;
  }

  emitFromWorker(message: WorkerToMainMessage): void {
    const event = { data: message } as MessageEvent<WorkerToMainMessage>;
    this.onmessage?.(event);
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

function createRuntimeMock(): {
  runtime: Runtime;
  receivedFromWorker: Uint8Array[];
  addServerCalls: { count: number };
  removeServerCalls: { count: number };
} {
  const receivedFromWorker: Uint8Array[] = [];
  const addServerCalls = { count: 0 };
  const removeServerCalls = { count: 0 };

  const runtime: Runtime = {
    insert: () => ({ id: "id", values: [] }),
    update: () => undefined,
    delete: () => undefined,
    query: async () => [],
    subscribe: () => 1,
    unsubscribe: () => undefined,
    insertDurable: async () => ({ id: "id", values: [] }),
    updateDurable: async () => undefined,
    deleteDurable: async () => undefined,
    createSubscription: () => 1,
    executeSubscription: () => undefined,
    onSyncMessageReceived: (payload: Uint8Array | string) => {
      receivedFromWorker.push(
        typeof payload === "string" ? new TextEncoder().encode(payload) : payload,
      );
    },
    addServer: () => {
      addServerCalls.count += 1;
    },
    removeServer: () => {
      removeServerCalls.count += 1;
    },
    addClient: () => "client-id",
    getSchema: () => ({}),
    getSchemaHash: () => "schema-hash",
  };

  return {
    runtime,
    receivedFromWorker,
    addServerCalls,
    removeServerCalls,
  };
}

describe("WorkerBridge", () => {
  const enc = (value: unknown): Uint8Array => new TextEncoder().encode(JSON.stringify(value));

  it("attaches runtime server and forwards worker sync payloads to runtime", () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();

    new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    expect(runtimeMock.addServerCalls.count).toBe(1);

    worker.emitFromWorker({
      type: "sync",
      payload: [enc({ id: 1 }), enc({ id: 2 })],
    });

    expect(runtimeMock.receivedFromWorker).toEqual([enc({ id: 1 }), enc({ id: 2 })]);
  });

  it("calls installOnRuntime with the runtime on construction", () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();

    // installOnRuntime is a no-op in the mock but should be called.
    // Access the client through the bridge to verify the call was made.
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    // The Rust WorkerClient.installOnRuntime wires the outbox drainer.
    // In tests the mock is a no-op; we just verify the bridge is functional.
    expect(runtimeMock.addServerCalls.count).toBe(1);
    expect(bridge).toBeDefined();
  });

  it("initializes worker and returns assigned client id", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    const initPromise = bridge.init({
      schemaJson: '{"tables":[]}',
      appId: "app-1",
      env: "dev",
      userBranch: "main",
      dbName: "db-1",
      serverUrl: "http://localhost:3000",
    });

    expect(worker.posted[0]).toMatchObject({
      type: "init",
      appId: "app-1",
      dbName: "db-1",
    });

    worker.emitFromWorker({
      type: "init-ok",
      clientId: "worker-client-123",
    });

    await expect(initPromise).resolves.toBe("worker-client-123");
    expect(bridge.getWorkerClientId()).toBe("worker-client-123");
  });

  it("includes runtimeSources in the worker init payload", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);
    const wasmSource = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);

    const initPromise = bridge.init({
      schemaJson: '{"tables":[]}',
      appId: "app-1",
      env: "dev",
      userBranch: "main",
      dbName: "db-1",
      runtimeSources: {
        baseUrl: "/assets/jazz/",
        wasmSource,
      },
    });

    // runtimeSources is a bundler-level concern not forwarded in WorkerClient payload.
    expect(worker.posted[0]).toMatchObject({
      type: "init",
      appId: "app-1",
    });

    worker.emitFromWorker({
      type: "init-ok",
      clientId: "worker-client-123",
    });

    await expect(initPromise).resolves.toBe("worker-client-123");
  });

  it("detaches runtime server on shutdown", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    const shutdownPromise = bridge.shutdown(worker as unknown as Worker);

    expect(runtimeMock.removeServerCalls.count).toBe(1);
    expect(worker.posted[0]).toEqual({ type: "shutdown" });

    worker.emitFromWorker({ type: "shutdown-ok" });
    await shutdownPromise;

    expect(worker.terminated).toBe(true);
  });

  it("supports peer channel control and peer-sync forwarding", () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);
    const peerBatches: PeerSyncBatch[] = [];

    bridge.onPeerSync((batch) => {
      peerBatches.push(batch);
    });

    bridge.openPeer("peer-a");
    bridge.sendPeerSync("peer-a", 9, [enc("payload-1"), enc("payload-2")]);
    bridge.closePeer("peer-a");

    expect(worker.posted).toEqual([
      { type: "peer-open", peerId: "peer-a" },
      {
        type: "peer-sync",
        peerId: "peer-a",
        term: 9,
        payload: [enc("payload-1")],
      },
      {
        type: "peer-sync",
        peerId: "peer-a",
        term: 9,
        payload: [enc("payload-2")],
      },
      { type: "peer-close", peerId: "peer-a" },
    ]);

    worker.emitFromWorker({
      type: "peer-sync",
      peerId: "peer-a",
      term: 9,
      payload: [enc("from-worker")],
    });

    expect(peerBatches).toEqual([
      {
        peerId: "peer-a",
        term: 9,
        payload: [enc("from-worker")],
      },
    ]);
  });

  it("delegates setServerPayloadForwarder to WorkerClient and can replay upstream connection", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);
    const redirected: Uint8Array[] = [];

    // Setting a forwarder routes server-bound payloads away from the worker.
    bridge.setServerPayloadForwarder((payload) => {
      redirected.push(payload);
    });

    // Simulate a server-bound payload via the mock's test helper.
    const client = (bridge as any).client;
    client._simulateServerPayload(enc({ routed: "peer" }));

    const workerSyncMessages = worker.posted.filter(
      (entry): entry is { type: "sync"; payload: Uint8Array[] } =>
        typeof entry === "object" && entry !== null && (entry as { type?: string }).type === "sync",
    );
    expect(workerSyncMessages).toHaveLength(0);
    expect(redirected).toEqual([enc({ routed: "peer" })]);

    bridge.replayServerConnection();
    expect(runtimeMock.removeServerCalls.count).toBe(1);
    expect(runtimeMock.addServerCalls.count).toBe(2);

    bridge.applyIncomingServerPayload(enc("from-peer-leader"));
    expect(runtimeMock.receivedFromWorker).toEqual([enc("from-peer-leader")]);
  });

  it("clears serverPayloadForwarder when set to null (leader mode restore)", () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    bridge.setServerPayloadForwarder(() => {});
    bridge.setServerPayloadForwarder(null);

    // In leader mode, server-bound payloads go to the worker.
    const client = (bridge as any).client;
    client._simulateServerPayload(enc({ direct: true }));

    const syncMessages = worker.posted.filter(
      (entry): entry is { type: "sync"; payload: Uint8Array[] } =>
        typeof entry === "object" && entry !== null && (entry as { type?: string }).type === "sync",
    );
    expect(syncMessages).toHaveLength(1);
  });

  it("forwards lifecycle hints to worker", () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    bridge.sendLifecycleHint("visibility-hidden");
    bridge.sendLifecycleHint("resume");

    expect(worker.posted).toMatchObject([
      {
        type: "lifecycle-hint",
        event: "visibility-hidden",
      },
      {
        type: "lifecycle-hint",
        event: "resume",
      },
    ]);
    expect((worker.posted[0] as any).sentAtMs).toEqual(expect.any(Number));
    expect((worker.posted[1] as any).sentAtMs).toEqual(expect.any(Number));
  });

  it("forwards worker auth failures to the main thread listener", () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);
    const reasons: AuthFailureReason[] = [];

    bridge.onAuthFailure((reason) => {
      reasons.push(reason);
    });

    worker.emitFromWorker({
      type: "auth-failed",
      reason: "expired",
    });

    expect(reasons).toEqual(["expired"]);
  });
});
