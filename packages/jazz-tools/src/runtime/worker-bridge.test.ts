import { afterEach, describe, expect, it } from "vitest";

import { WorkerBridge, type PeerSyncBatch } from "./worker-bridge.js";
import type { Runtime } from "./client.js";
import type { WorkerToMainMessage } from "../worker/worker-protocol.js";
import { OutboxDestinationKind, type AuthFailureReason } from "./sync-transport.js";

afterEach(() => {
  delete (globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__;
});

class MockWorker {
  onmessage: ((event: MessageEvent<WorkerToMainMessage>) => void) | null = null;
  posted: unknown[] = [];
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

  emitFromWorker(message: WorkerToMainMessage): void {
    const event = { data: message } as MessageEvent<WorkerToMainMessage>;
    this.onmessage?.(event);
    for (const listener of this.listeners) {
      listener(event);
    }
  }
}

type SendSyncPayloadCallback = (
  destinationKind: OutboxDestinationKind,
  destinationId: string,
  payload: Uint8Array,
  isCatalogue: boolean,
) => void;

function createRuntimeMock(): {
  runtime: Runtime;
  emitSyncPayload: SendSyncPayloadCallback;
  receivedFromWorker: Uint8Array[];
  addServerCalls: { count: number };
  removeServerCalls: { count: number };
} {
  let onSyncToSend: SendSyncPayloadCallback | null = null;
  const receivedFromWorker: Uint8Array[] = [];
  const addServerCalls = { count: 0 };
  const removeServerCalls = { count: 0 };

  const runtime: Runtime = {
    loadLocalBatchRecord: () => null,
    loadLocalBatchRecords: () => [],
    insert: () => ({ id: "id", values: [], batchId: "batch-id" }),
    update: () => ({
      batchId: "batch-id",
    }),
    delete: () => ({
      batchId: "batch-id",
    }),
    query: async () => [],
    subscribe: () => 1,
    unsubscribe: () => undefined,
    createSubscription: () => 1,
    executeSubscription: () => undefined,
    onSyncMessageReceived: (payload: Uint8Array | string) => {
      receivedFromWorker.push(
        typeof payload === "string" ? new TextEncoder().encode(payload) : payload,
      );
    },
    onSyncMessageToSend: (callback: SendSyncPayloadCallback) => {
      onSyncToSend = callback;
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
    emitSyncPayload: (
      destinationKind: OutboxDestinationKind,
      destinationId: string,
      payload: Uint8Array,
      isCatalogue = false,
    ) => {
      if (!onSyncToSend) {
        throw new Error("onSyncMessageToSend callback not registered");
      }
      onSyncToSend(destinationKind, destinationId, payload, isCatalogue);
    },
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

  it("batches server-bound runtime payloads into one worker sync message", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    runtimeMock.emitSyncPayload("server", "server-1", enc({ id: 1 }), false);
    runtimeMock.emitSyncPayload("server", "server-2", enc({ id: 2 }), false);
    runtimeMock.emitSyncPayload("client", "client-1", enc({ ignored: true }), false);

    // Outgoing payloads are buffered until init completes.
    let syncMessages = worker.posted.filter(
      (entry): entry is { type: "sync"; payload: Uint8Array[] } =>
        typeof entry === "object" && entry !== null && (entry as { type?: string }).type === "sync",
    );
    expect(syncMessages).toHaveLength(0);

    const initPromise = bridge.init({
      schemaJson: '{"tables":[]}',
      appId: "app-1",
      env: "dev",
      userBranch: "main",
      dbName: "db-1",
    });
    worker.emitFromWorker({ type: "init-ok", clientId: "worker-client-123" });
    await initPromise;
    await Promise.resolve();

    syncMessages = worker.posted.filter(
      (entry): entry is { type: "sync"; payload: Uint8Array[] } =>
        typeof entry === "object" && entry !== null && (entry as { type?: string }).type === "sync",
    );

    expect(syncMessages).toHaveLength(1);
    expect(syncMessages[0]).toEqual({
      type: "sync",
      payload: [enc({ id: 1 }), enc({ id: 2 })],
    });
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

    expect(worker.posted[0]).toMatchObject({
      type: "init",
      runtimeSources: {
        baseUrl: "/assets/jazz/",
        wasmSource,
      },
    });

    worker.emitFromWorker({
      type: "init-ok",
      clientId: "worker-client-123",
    });

    await expect(initPromise).resolves.toBe("worker-client-123");
  });

  it("passes telemetry collector url through worker init", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    const initPromise = bridge.init({
      schemaJson: '{"tables":[]}',
      appId: "app-1",
      env: "dev",
      userBranch: "main",
      dbName: "db-1",
      telemetryCollectorUrl: "http://127.0.0.1:54418",
    });

    expect(worker.posted[0]).toMatchObject({
      type: "init",
      telemetryCollectorUrl: "http://127.0.0.1:54418",
    });

    worker.emitFromWorker({
      type: "init-ok",
      clientId: "worker-client-123",
    });
    await initPromise;
  });

  it("does not install a global WASM trace callback during init", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    const initPromise = bridge.init({
      schemaJson: '{"tables":[]}',
      appId: "app-1",
      env: "dev",
      userBranch: "main",
      dbName: "db-1",
      telemetryCollectorUrl: "http://127.0.0.1:54418",
    });
    worker.emitFromWorker({ type: "init-ok", clientId: "worker-client-123" });
    await initPromise;

    expect((globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__).toBeUndefined();

    const shutdownPromise = bridge.shutdown(worker as unknown as Worker);
    worker.emitFromWorker({ type: "shutdown-ok" });
    await shutdownPromise;
  });

  it("detaches runtime server on shutdown and stops forwarding after disposal", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);

    const shutdownPromise = bridge.shutdown(worker as unknown as Worker);

    expect(runtimeMock.removeServerCalls.count).toBe(1);
    expect(worker.posted[0]).toEqual({ type: "shutdown" });

    worker.emitFromWorker({ type: "shutdown-ok" });
    await shutdownPromise;

    runtimeMock.emitSyncPayload("server", "server-1", enc({ dropped: true }), false);
    await Promise.resolve();

    const syncMessagesAfterShutdown = worker.posted.filter(
      (entry): entry is { type: "sync"; payload: Uint8Array[] } =>
        typeof entry === "object" && entry !== null && (entry as { type?: string }).type === "sync",
    );
    expect(syncMessagesAfterShutdown).toHaveLength(0);
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
        payload: [enc("payload-1"), enc("payload-2")],
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

  it("can redirect outgoing server payloads and replay upstream connection", async () => {
    const worker = new MockWorker();
    const runtimeMock = createRuntimeMock();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtimeMock.runtime);
    const redirected: Uint8Array[] = [];

    bridge.setServerPayloadForwarder((payload) => {
      redirected.push(payload);
    });
    runtimeMock.emitSyncPayload("server", "server-1", enc({ routed: "peer" }), false);
    await Promise.resolve();

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
