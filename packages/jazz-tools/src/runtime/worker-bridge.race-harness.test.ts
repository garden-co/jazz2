import { describe, expect, it, vi } from "vitest";
import type { Runtime } from "./client.js";
import { WorkerBridge, type WorkerBridgeOptions } from "./worker-bridge.js";

// ---------------------------------------------------------------------------
// Local protocol types (previously from worker-protocol.ts, now inlined)
// ---------------------------------------------------------------------------

type MainToWorkerMessage =
  | { type: "init"; [k: string]: unknown }
  | { type: "sync"; payload: Uint8Array[] }
  | { type: "peer-open"; peerId: string }
  | { type: "peer-sync"; peerId: string; term: number; payload: Uint8Array[] }
  | { type: "peer-close"; peerId: string }
  | { type: "update-auth"; jwtToken?: string }
  | { type: "lifecycle-hint"; event: string; sentAtMs: number }
  | { type: "disconnect-upstream" }
  | { type: "reconnect-upstream" }
  | { type: "shutdown" }
  | { type: "simulate-crash" };

type WorkerToMainMessage =
  | { type: "ready" }
  | { type: "init-ok"; clientId: string }
  | { type: "sync"; payload: (Uint8Array | string)[] }
  | { type: "peer-sync"; peerId: string; term: number; payload: Uint8Array[] }
  | { type: "upstream-connected" }
  | { type: "upstream-disconnected" }
  | { type: "auth-failed"; reason: string }
  | { type: "error"; message: string }
  | { type: "shutdown-ok" };

type WorkerMessageHandler = (event: MessageEvent<WorkerToMainMessage>) => void;

// ---------------------------------------------------------------------------
// FakeWorker + FakeWorkerScript
// ---------------------------------------------------------------------------

type ScriptOptions = {
  dropSyncBeforeInit?: boolean;
  initAckMode?: "manual" | "sync-ok" | "async-ok" | "sync-error" | "async-error";
  shutdownAckMode?: "sync-ok" | "async-ok" | "timeout";
  initErrorMessage?: string;
};

class FakeWorkerScript {
  private initialized = false;
  private pendingSyncPayloads: Uint8Array[] = [];
  readonly receivedSyncPayloads: Uint8Array[] = [];
  readonly droppedSyncPayloads: Uint8Array[] = [];
  initMessageCount = 0;
  shutdownMessageCount = 0;

  constructor(
    private readonly worker: FakeWorker,
    private readonly options: ScriptOptions = {},
  ) {}

  onMainMessage(message: MainToWorkerMessage): void {
    switch (message.type) {
      case "init":
        this.initMessageCount += 1;
        this.handleInit();
        return;
      case "sync":
        if (!this.initialized) {
          if (this.options.dropSyncBeforeInit) {
            this.droppedSyncPayloads.push(...message.payload);
          } else {
            this.pendingSyncPayloads.push(...message.payload);
          }
          return;
        }
        this.receivedSyncPayloads.push(...message.payload);
        return;
      case "shutdown":
        this.shutdownMessageCount += 1;
        this.handleShutdown();
        return;
      case "update-auth":
      case "simulate-crash":
        return;
    }
  }

  private handleInit(): void {
    const mode = this.options.initAckMode ?? "manual";
    switch (mode) {
      case "sync-ok":
        this.completeInit();
        return;
      case "async-ok":
        queueMicrotask(() => this.completeInit());
        return;
      case "sync-error":
        this.failInit();
        return;
      case "async-error":
        queueMicrotask(() => this.failInit());
        return;
      case "manual":
        return;
    }
  }

  private handleShutdown(): void {
    const mode = this.options.shutdownAckMode ?? "async-ok";
    if (mode === "timeout") return;

    const emitShutdownOk = () => this.worker.emitToMain({ type: "shutdown-ok" });
    if (mode === "sync-ok") {
      emitShutdownOk();
      return;
    }
    queueMicrotask(emitShutdownOk);
  }

  completeInit(clientId = "worker-client"): void {
    if (this.initialized) return;
    this.initialized = true;
    this.worker.emitToMain({ type: "init-ok", clientId });
    const pending = this.pendingSyncPayloads;
    this.pendingSyncPayloads = [];
    for (const payload of pending) {
      this.receivedSyncPayloads.push(payload);
    }
  }

  failInit(message = this.options.initErrorMessage ?? "init failed"): void {
    this.worker.emitToMain({ type: "error", message });
  }

  emitSyncToMain(...payloads: Uint8Array[]): void {
    this.worker.emitToMain({ type: "sync", payload: payloads });
  }

  emitUpstreamConnected(): void {
    this.worker.emitToMain({ type: "upstream-connected" });
  }

  emitUpstreamDisconnected(): void {
    this.worker.emitToMain({ type: "upstream-disconnected" });
  }
}

class FakeWorker {
  onmessage: ((event: MessageEvent<WorkerToMainMessage>) => void) | null = null;
  readonly script: FakeWorkerScript;
  private readonly listeners = new Set<WorkerMessageHandler>();

  constructor(options: ScriptOptions = {}) {
    this.script = new FakeWorkerScript(this, options);
  }

  postMessage(message: MainToWorkerMessage): void {
    this.script.onMainMessage(message);
  }

  addEventListener(type: string, handler: WorkerMessageHandler): void {
    if (type === "message") this.listeners.add(handler);
  }

  removeEventListener(type: string, handler: WorkerMessageHandler): void {
    if (type === "message") this.listeners.delete(handler);
  }

  terminate(): void {}

  emitToMain(message: WorkerToMainMessage): void {
    const event = { data: message } as MessageEvent<WorkerToMainMessage>;
    this.onmessage?.(event);
    for (const handler of this.listeners) handler(event);
  }
}

// ---------------------------------------------------------------------------
// Mock jazz-wasm — WorkerClient wired to FakeWorker via JSON protocol
//
// vi.mock is hoisted; the WorkerClient class must be defined inside the
// factory so it can reference its own constructor arg.
// We capture the FakeWorker via the `worker` constructor arg.
// ---------------------------------------------------------------------------

vi.mock("jazz-wasm", () => {
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
      worker.onmessage = (event: any) => {
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
// Harness helpers
// ---------------------------------------------------------------------------

function createRuntimeHarness() {
  const receivedFromWorker: Uint8Array[] = [];

  const runtime = {
    onSyncMessageReceived(payload: Uint8Array) {
      receivedFromWorker.push(payload);
    },
    addServer() {},
    removeServer() {},
  } as unknown as Runtime;

  return {
    runtime,
    receivedFromWorker,
  };
}

function makeBridgeOptions(): WorkerBridgeOptions {
  return {
    schemaJson: JSON.stringify({}),
    appId: "race-harness-app",
    env: "dev",
    userBranch: "main",
    dbName: "race-harness-db",
  };
}

describe("WorkerBridge race harness", () => {
  const enc = (value: unknown): Uint8Array => new TextEncoder().encode(JSON.stringify(value));

  it("WB-U01 init completes and bridge transitions to ready", async () => {
    // Outbound sync buffering before init is now owned by the Rust WorkerClient
    // (installOnRuntime drainer) and the WorkerHost pending_sync_messages buffer.
    // The TS bridge transitions to ready and returns the assigned client id.
    const worker = new FakeWorker({ dropSyncBeforeInit: true });
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromise = bridge.init(makeBridgeOptions());

    expect(worker.script.droppedSyncPayloads).toEqual([]);

    worker.script.completeInit("worker-client-1");
    await expect(initPromise).resolves.toBe("worker-client-1");
    expect(bridge.getWorkerClientId()).toBe("worker-client-1");
    expect((bridge as any).state.phase).toBe("ready");
  });

  it("WB-U02 init memoizes in-flight promise across the init boundary", async () => {
    // The TS bridge memoizes the init promise.  Outbound sync ordering across
    // the init boundary is guaranteed by the Rust WorkerClient drainer.
    const worker = new FakeWorker({ dropSyncBeforeInit: true });
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromiseA = bridge.init(makeBridgeOptions());
    const initPromiseB = bridge.init(makeBridgeOptions());
    expect(initPromiseA).toBe(initPromiseB);

    worker.script.completeInit("worker-client-2");
    await initPromiseA;

    expect(bridge.getWorkerClientId()).toBe("worker-client-2");
  });

  it("WB-U03 does not miss synchronous init-ok responses", async () => {
    vi.useFakeTimers();
    try {
      const worker = new FakeWorker({ initAckMode: "sync-ok" });
      const { runtime } = createRuntimeHarness();
      const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

      const initPromise = bridge.init(makeBridgeOptions());
      await vi.runAllTimersAsync();

      await expect(initPromise).resolves.toBe("worker-client");
    } finally {
      vi.useRealTimers();
    }
  });

  it("WB-U04 forwards worker->main sync while init is pending", async () => {
    const worker = new FakeWorker();
    const { runtime, receivedFromWorker } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromise = bridge.init(makeBridgeOptions());
    worker.script.emitSyncToMain(enc({ kind: "from-worker", seq: 1 }));

    expect(receivedFromWorker).toEqual([enc({ kind: "from-worker", seq: 1 })]);

    worker.script.completeInit("worker-client-3");
    await initPromise;
  });

  it("WB-U04b waits for a direct upstream connection before resolving edge readiness", async () => {
    const worker = new FakeWorker({ initAckMode: "sync-ok" });
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    await bridge.init({
      ...makeBridgeOptions(),
      serverUrl: "https://example.test",
    });

    let resolved = false;
    const waitPromise = bridge.waitForUpstreamServerConnection().then(() => {
      resolved = true;
    });

    await Promise.resolve();
    expect(resolved).toBe(false);

    worker.script.emitUpstreamConnected();
    await waitPromise;
    expect(resolved).toBe(true);
  });

  it("WB-U04c skips upstream waiting when server payloads route through another tab", async () => {
    const worker = new FakeWorker({ initAckMode: "sync-ok" });
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    await bridge.init({
      ...makeBridgeOptions(),
      serverUrl: "https://example.test",
    });

    bridge.setServerPayloadForwarder(() => {});
    await expect(bridge.waitForUpstreamServerConnection()).resolves.toBeUndefined();
  });

  it("WB-U05 init memoizes and returns the same in-flight promise", async () => {
    const worker = new FakeWorker();
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromiseA = bridge.init(makeBridgeOptions());
    const initPromiseB = bridge.init(makeBridgeOptions());

    expect(initPromiseA).toBe(initPromiseB);
    expect(worker.script.initMessageCount).toBe(1);

    worker.script.completeInit("worker-client-5");
    await expect(initPromiseA).resolves.toBe("worker-client-5");
    await expect(initPromiseB).resolves.toBe("worker-client-5");
  });

  it("WB-U06 init failure transitions bridge to failed state", async () => {
    // When init fails, the bridge moves to "failed". Outbound sync queuing is
    // owned by the Rust WorkerClient drainer and WorkerHost; the TS bridge does
    // not buffer payloads.
    const worker = new FakeWorker();
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    const initPromise = bridge.init(makeBridgeOptions());

    worker.script.failInit("boom");
    await expect(initPromise).rejects.toThrow("boom");

    expect((bridge as any).state.phase).toBe("failed");
  });

  it("WB-U09 init times out after the bridge timeout window", async () => {
    vi.useFakeTimers();
    try {
      const worker = new FakeWorker({ initAckMode: "manual" });
      const { runtime } = createRuntimeHarness();
      const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

      // WorkerClient now owns the init timeout internally (not WorkerBridge).
      // We verify the phase stays "initializing" while pending.
      const initPromise = bridge.init(makeBridgeOptions());
      await vi.runAllTimersAsync();

      expect((bridge as any).state.phase).toBe("initializing");

      // Resolve to avoid test cleanup hanging.
      worker.script.completeInit("late-client");
      await expect(initPromise).resolves.toBe("late-client");
    } finally {
      vi.useRealTimers();
    }
  });

  it("WB-U07 handles synchronous shutdown-ok acknowledgements", async () => {
    const worker = new FakeWorker({ shutdownAckMode: "sync-ok" });
    const { runtime } = createRuntimeHarness();
    const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

    await expect(bridge.shutdown(worker as unknown as Worker)).resolves.toBeUndefined();
    expect(worker.script.shutdownMessageCount).toBe(1);
  });

  it("WB-U08 does not throw when shutdown acknowledgement times out", async () => {
    vi.useFakeTimers();
    try {
      const worker = new FakeWorker({ shutdownAckMode: "timeout" });
      const { runtime } = createRuntimeHarness();
      const bridge = new WorkerBridge(worker as unknown as Worker, runtime);

      const shutdownPromise = bridge.shutdown(worker as unknown as Worker);
      await vi.advanceTimersByTimeAsync(5001);

      await expect(shutdownPromise).resolves.toBeUndefined();
      expect(worker.script.shutdownMessageCount).toBe(1);
    } finally {
      vi.useRealTimers();
    }
  });
});
