/**
 * WorkerBridge — Main-thread side of the worker communication bridge.
 *
 * Wires a main-thread WasmRuntime (in-memory) to a dedicated worker
 * (OPFS-persistent) via postMessage. The worker acts as the "server"
 * for the main thread's runtime.
 */

import type { Runtime } from "./client.js";
import type { RuntimeSourcesConfig } from "./context.js";
import type { AuthFailureReason } from "./sync-transport.js";
import type {
  InitMessage,
  WorkerLifecycleEvent,
  WorkerToMainMessage,
} from "../worker/worker-protocol.js";
import type { LocalBatchRecord } from "./client.js";
import { createSyncOutboxRouter } from "./sync-transport.js";

/**
 * Options for initializing the worker bridge.
 */
export interface WorkerBridgeOptions {
  schemaJson: string;
  appId: string;
  env: string;
  userBranch: string;
  dbName: string;
  serverUrl?: string;
  jwtToken?: string;
  adminSecret?: string;
  runtimeSources?: RuntimeSourcesConfig;
  fallbackWasmUrl?: string;
  logLevel?: "error" | "warn" | "info" | "debug" | "trace";
}

export interface PeerSyncBatch {
  peerId: string;
  term: number;
  payload: Uint8Array[];
}

type BridgePhase = "idle" | "initializing" | "ready" | "failed" | "shutting-down" | "disposed";
type BridgeEvent =
  | { type: "INIT_CALLED" }
  | { type: "INIT_OK"; clientId: string }
  | { type: "INIT_FAILED" }
  | { type: "SHUTDOWN_CALLED" }
  | { type: "SHUTDOWN_FINISHED" };

interface WorkerBridgeState {
  phase: BridgePhase;
  workerClientId: string | null;
  initPromise: Promise<string> | null;
  expectsUpstreamServer: boolean;
  upstreamServerConnected: boolean;
  upstreamServerReady: Promise<void>;
  resolveUpstreamServerReady: (() => void) | null;
  pendingSyncPayloadsForWorker: Uint8Array[];
  syncBatchFlushQueued: boolean;
  peerSyncListener: ((batch: PeerSyncBatch) => void) | null;
  authFailureListener: ((reason: AuthFailureReason) => void) | null;
  localBatchRecordsSyncListener: ((batches: LocalBatchRecord[]) => void) | null;
  mutationErrorReplayListener: ((batch: LocalBatchRecord) => void) | null;
  serverPayloadForwarder: ((payload: Uint8Array) => void) | null;
}

const INIT_RESPONSE_TIMEOUT_MS = 12_000;
const SHUTDOWN_ACK_TIMEOUT_MS = 5_000;

function createDeferredPromise(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((resolver) => {
    resolve = resolver;
  });
  return { promise, resolve };
}

/**
 * Bridge between main-thread runtime and dedicated worker.
 *
 * The bridge:
 * - Forwards outgoing sync messages from the main runtime to the worker
 * - Forwards incoming sync messages from the worker to the main runtime
 * - The worker is treated as the main thread's "server" for sync purposes
 */
export class WorkerBridge {
  private worker: Worker;
  private runtime: Runtime;
  private state: WorkerBridgeState;

  constructor(worker: Worker, runtime: Runtime) {
    const upstreamReady = createDeferredPromise();
    this.worker = worker;
    this.runtime = runtime;
    this.state = {
      phase: "idle",
      workerClientId: null,
      initPromise: null,
      expectsUpstreamServer: false,
      upstreamServerConnected: false,
      upstreamServerReady: upstreamReady.promise,
      resolveUpstreamServerReady: upstreamReady.resolve,
      pendingSyncPayloadsForWorker: [],
      syncBatchFlushQueued: false,
      peerSyncListener: null,
      authFailureListener: null,
      localBatchRecordsSyncListener: null,
      mutationErrorReplayListener: null,
      serverPayloadForwarder: null,
    };

    // Wire worker → main: incoming sync messages from worker
    this.worker.onmessage = (event: MessageEvent<WorkerToMainMessage>) => {
      const msg = event.data;
      if (msg.type === "sync") {
        for (const payload of msg.payload) {
          this.runtime.onSyncMessageReceived(payload);
        }
      } else if (msg.type === "upstream-connected") {
        this.markUpstreamServerConnected();
      } else if (msg.type === "upstream-disconnected") {
        this.markUpstreamServerDisconnected();
      } else if (msg.type === "auth-failed") {
        this.state.authFailureListener?.(msg.reason);
      } else if (msg.type === "local-batch-records-sync") {
        this.state.localBatchRecordsSyncListener?.(msg.batches);
      } else if (msg.type === "mutation-error-replay") {
        this.state.mutationErrorReplayListener?.(msg.batch);
      } else if (msg.type === "peer-sync") {
        this.state.peerSyncListener?.({
          peerId: msg.peerId,
          term: msg.term,
          payload: msg.payload,
        });
      }
    };

    // Wire main → worker: outgoing sync messages from runtime
    this.runtime.onSyncMessageToSend?.(
      createSyncOutboxRouter({
        onServerPayload: (payload) => {
          if (this.isDisposedLike()) return;

          if (this.state.serverPayloadForwarder) {
            this.state.serverPayloadForwarder(payload as Uint8Array);
          } else {
            this.enqueueSyncMessageForWorker(payload as Uint8Array);
          }
        },
      }),
    );

    // Register a server so the runtime sends sync messages to it
    this.runtime.addServer();
  }

  /**
   * Initialize the worker with schema and config.
   *
   * Waits for the worker to respond with init-ok.
   */
  init(options: WorkerBridgeOptions): Promise<string> {
    if (this.state.initPromise) {
      return this.state.initPromise;
    }

    if (this.isDisposedLike()) {
      const disposedError = Promise.reject(new Error("WorkerBridge has been disposed"));
      this.state.initPromise = disposedError;
      return disposedError;
    }

    this.transition({ type: "INIT_CALLED" });

    const initMsg: InitMessage = {
      type: "init",
      schemaJson: options.schemaJson,
      appId: options.appId,
      env: options.env,
      userBranch: options.userBranch,
      dbName: options.dbName,
      serverUrl: options.serverUrl,
      jwtToken: options.jwtToken,
      adminSecret: options.adminSecret,
      runtimeSources: options.runtimeSources,
      fallbackWasmUrl: options.fallbackWasmUrl,
      logLevel: options.logLevel,
      clientId: "", // Worker generates its own client ID for main thread
    };

    this.state.expectsUpstreamServer = Boolean(options.serverUrl);
    if (!this.state.expectsUpstreamServer) {
      this.markUpstreamServerConnected();
    } else {
      this.markUpstreamServerDisconnected();
    }

    const responsePromise = waitForMessage<WorkerToMainMessage>(
      this.worker,
      (msg) => msg.type === "init-ok" || msg.type === "error",
      INIT_RESPONSE_TIMEOUT_MS,
      "Worker init timeout",
    );

    this.worker.postMessage(initMsg);

    const initPromise = responsePromise
      .then((response) => {
        if (this.isDisposedLike()) {
          throw new Error("WorkerBridge has been disposed");
        }

        if (response.type === "error") {
          this.transition({ type: "INIT_FAILED" });
          throw new Error(`Worker init failed: ${response.message}`);
        }

        if (response.type === "init-ok") {
          if (this.state.phase !== "initializing") {
            // Ignore late init-ok after a terminal transition.
            throw new Error("Worker init response arrived after bridge left initializing state");
          }
          this.transition({ type: "INIT_OK", clientId: response.clientId });
          this.flushPendingSyncToWorker();
          return response.clientId;
        }

        throw new Error("Unexpected worker response");
      })
      .catch((error) => {
        if (this.state.phase !== "disposed") {
          this.transition({ type: "INIT_FAILED" });
        }
        throw error;
      });

    this.state.initPromise = initPromise;
    return initPromise;
  }

  /**
   * Update auth credentials in the worker.
   */
  updateAuth(auth: { jwtToken?: string }): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "update-auth", ...auth });
  }

  sendLifecycleHint(event: WorkerLifecycleEvent): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({
      type: "lifecycle-hint",
      event,
      sentAtMs: Date.now(),
    });
  }

  /**
   * Shut down the worker and wait for OPFS handles to be released.
   *
   * @param worker The Worker instance (needed for listening to shutdown-ok)
   */
  async shutdown(worker: Worker): Promise<void> {
    if (this.isDisposedLike()) return;

    this.transition({ type: "SHUTDOWN_CALLED" });

    const shutdownAckPromise = waitForMessage<WorkerToMainMessage>(
      worker,
      (msg) => msg.type === "shutdown-ok",
      SHUTDOWN_ACK_TIMEOUT_MS,
      "Worker shutdown timeout",
    );
    this.worker.postMessage({ type: "shutdown" });
    try {
      await shutdownAckPromise;
      this.transition({ type: "SHUTDOWN_FINISHED" });
    } catch {
      this.transition({ type: "SHUTDOWN_FINISHED" });
      // Timeout — worker may have already closed
    }
  }

  /**
   * Get the client ID the worker assigned to the main thread.
   */
  getWorkerClientId(): string | null {
    return this.state.workerClientId;
  }

  setServerPayloadForwarder(forwarder: ((payload: Uint8Array) => void) | null): void {
    if (this.isDisposedLike()) return;
    this.state.serverPayloadForwarder = forwarder;
  }

  async waitForUpstreamServerConnection(): Promise<void> {
    if (!this.state.expectsUpstreamServer) return;
    if (this.state.serverPayloadForwarder) return;
    if (this.state.upstreamServerConnected) return;
    await this.state.upstreamServerReady;
  }

  applyIncomingServerPayload(payload: Uint8Array): void {
    if (this.isDisposedLike()) return;
    this.runtime.onSyncMessageReceived(payload);
  }

  replayServerConnection(): void {
    if (this.isDisposedLike()) return;
    this.runtime.removeServer();
    this.runtime.addServer();
  }

  disconnectUpstream(): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "disconnect-upstream" });
  }

  reconnectUpstream(): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "reconnect-upstream" });
  }

  acknowledgeRejectedBatch(batchId: string): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "acknowledge-rejected-batch", batchId });
  }

  onPeerSync(listener: (batch: PeerSyncBatch) => void): void {
    this.state.peerSyncListener = listener;
  }

  onAuthFailure(listener: (reason: AuthFailureReason) => void): void {
    this.state.authFailureListener = listener;
  }

  onLocalBatchRecordsSync(listener: (batches: LocalBatchRecord[]) => void): void {
    this.state.localBatchRecordsSyncListener = listener;
  }

  onMutationErrorReplay(listener: (batch: LocalBatchRecord) => void): void {
    this.state.mutationErrorReplayListener = listener;
  }

  openPeer(peerId: string): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "peer-open", peerId });
  }

  sendPeerSync(peerId: string, term: number, payload: Uint8Array[]): void {
    if (this.isDisposedLike()) return;
    if (payload.length === 0) return;
    const message = {
      type: "peer-sync" as const,
      peerId,
      term,
      payload,
    };
    const transfer = collectPayloadTransferables(payload);
    this.worker.postMessage(message, transfer);
  }

  closePeer(peerId: string): void {
    if (this.isDisposedLike()) return;
    this.worker.postMessage({ type: "peer-close", peerId });
  }

  private enqueueSyncMessageForWorker(payload: Uint8Array): void {
    if (this.isDisposedLike()) return;

    this.state.pendingSyncPayloadsForWorker.push(payload);
    if (this.state.syncBatchFlushQueued) return;

    this.state.syncBatchFlushQueued = true;
    queueMicrotask(() => {
      if (this.isDisposedLike()) {
        this.state.syncBatchFlushQueued = false;
        this.state.pendingSyncPayloadsForWorker = [];
        return;
      }
      this.state.syncBatchFlushQueued = false;
      this.flushPendingSyncToWorker();
    });
  }

  private flushPendingSyncToWorker(): void {
    if (this.state.phase !== "ready" || this.state.pendingSyncPayloadsForWorker.length === 0) {
      return;
    }

    const payloads = this.state.pendingSyncPayloadsForWorker;
    this.state.pendingSyncPayloadsForWorker = [];

    const message = {
      type: "sync" as const,
      payload: payloads,
    };
    const transfer = collectPayloadTransferables(payloads);
    this.worker.postMessage(message, transfer);
  }

  private markUpstreamServerConnected(): void {
    this.state.upstreamServerConnected = true;
    const resolver = this.state.resolveUpstreamServerReady;
    this.state.resolveUpstreamServerReady = null;
    resolver?.();
  }

  private markUpstreamServerDisconnected(): void {
    if (!this.state.expectsUpstreamServer) {
      this.state.upstreamServerConnected = true;
      return;
    }
    if (!this.state.upstreamServerConnected && this.state.resolveUpstreamServerReady) {
      return;
    }
    const deferred = createDeferredPromise();
    this.state.upstreamServerConnected = false;
    this.state.upstreamServerReady = deferred.promise;
    this.state.resolveUpstreamServerReady = deferred.resolve;
  }

  private isDisposedLike(): boolean {
    return this.state.phase === "disposed" || this.state.phase === "shutting-down";
  }

  private transition(event: BridgeEvent): void {
    switch (event.type) {
      case "INIT_CALLED":
        if (this.state.phase === "idle" || this.state.phase === "failed") {
          this.state.phase = "initializing";
        }
        return;
      case "INIT_OK":
        if (this.state.phase !== "initializing") return;
        this.state.workerClientId = event.clientId;
        this.state.phase = "ready";
        return;
      case "INIT_FAILED":
        if (this.state.phase !== "initializing") return;
        this.state.phase = "failed";
        this.state.syncBatchFlushQueued = false;
        return;
      case "SHUTDOWN_CALLED":
        if (this.state.phase === "disposed" || this.state.phase === "shutting-down") return;
        this.state.phase = "shutting-down";
        // Detach upstream edge so the next bridge attach performs a clean replay.
        this.runtime.removeServer();
        return;
      case "SHUTDOWN_FINISHED":
        if (this.state.phase === "disposed") return;
        this.state.phase = "disposed";
        this.disposeInternals();
        return;
    }
  }

  private disposeInternals(): void {
    this.state.pendingSyncPayloadsForWorker = [];
    this.state.serverPayloadForwarder = null;
    this.state.peerSyncListener = null;
    this.state.syncBatchFlushQueued = false;
    this.runtime.onSyncMessageToSend?.(() => undefined);
  }
}

function collectPayloadTransferables(payloads: Uint8Array[]): Transferable[] {
  return payloads.map((payload) => payload.buffer);
}

/**
 * Wait for a specific message type from a worker.
 */
function waitForMessage<T>(
  worker: Worker,
  predicate: (msg: T) => boolean,
  timeoutMs: number,
  timeoutMessage: string,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(timeoutMessage));
    }, timeoutMs);

    const handler = (event: MessageEvent<T>) => {
      if (predicate(event.data)) {
        cleanup();
        resolve(event.data);
      }
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker.removeEventListener("message", handler);
    };

    worker.addEventListener("message", handler);
  });
}
