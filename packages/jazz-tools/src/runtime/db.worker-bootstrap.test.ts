import { afterEach, describe, expect, it, vi } from "vitest";

const { loadWasmModuleMock } = vi.hoisted(() => ({
  loadWasmModuleMock: vi.fn().mockResolvedValue({}) as any,
}));

const { FakeTabLeaderElection } = vi.hoisted(() => ({
  FakeTabLeaderElection: class {
    start(): void {}

    stop(): void {}

    snapshot() {
      return {
        role: "leader" as const,
        tabId: "tab-alice",
        leaderTabId: "tab-alice",
        term: 1,
      };
    }

    waitForInitialLeader(): Promise<{
      role: "leader";
      tabId: string;
      leaderTabId: string;
      term: number;
    }> {
      return Promise.resolve(this.snapshot());
    }

    onChange(): () => void {
      return () => {};
    }
  },
}));

vi.mock("./client.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./client.js")>();
  return {
    ...actual,
    loadWasmModule: loadWasmModuleMock,
  };
});

vi.mock("./tab-leader-election.js", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./tab-leader-election.js")>();
  return {
    ...actual,
    TabLeaderElection: FakeTabLeaderElection,
  };
});

import { Db } from "./db.js";

const originalWindow = (globalThis as Record<string, unknown>).window;
const originalLocation = globalThis.location;
const originalWorker = (globalThis as Record<string, unknown>).Worker;

afterEach(() => {
  loadWasmModuleMock.mockClear();

  if (originalWindow === undefined) {
    delete (globalThis as Record<string, unknown>).window;
  } else {
    (globalThis as Record<string, unknown>).window = originalWindow;
  }

  if (originalLocation === undefined) {
    delete (globalThis as Record<string, unknown>).location;
  } else {
    (globalThis as Record<string, unknown>).location = originalLocation;
  }

  if (originalWorker === undefined) {
    delete (globalThis as Record<string, unknown>).Worker;
  } else {
    (globalThis as Record<string, unknown>).Worker = originalWorker;
  }
});

describe("Db worker runtime bootstrap", () => {
  it("prefers explicit workerUrl and wasmUrl over baseUrl and fallback resolution", async () => {
    const spawnedWorkerUrls: string[] = [];

    class FakeWorker extends EventTarget {
      constructor(url: string | URL, _options?: WorkerOptions) {
        super();
        spawnedWorkerUrls.push(String(url));
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await Db.createWithWorker({
      appId: "worker-bootstrap-explicit-urls",
      driver: { type: "persistent", dbName: "worker-bootstrap-explicit-urls" },
      runtimeSources: {
        baseUrl: "/ignored/",
        workerUrl: "/custom/jazz-worker.js",
        wasmUrl: "/custom/jazz_wasm_bg.wasm",
      },
    });

    await db.shutdown();

    expect(spawnedWorkerUrls).toEqual([
      "http://localhost:3000/custom/jazz-worker.js?jazz-wasm-url=http%3A%2F%2Flocalhost%3A3000%2Fcustom%2Fjazz_wasm_bg.wasm",
    ]);
  });

  it("derives worker and wasm URLs from runtimeSources.baseUrl when explicit URLs are omitted", async () => {
    const spawnedWorkerUrls: string[] = [];

    class FakeWorker extends EventTarget {
      constructor(url: string | URL, _options?: WorkerOptions) {
        super();
        spawnedWorkerUrls.push(String(url));
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await Db.createWithWorker({
      appId: "worker-bootstrap-base-url",
      driver: { type: "persistent", dbName: "worker-bootstrap-base-url" },
      runtimeSources: {
        baseUrl: "/assets/jazz/",
      },
    });

    await db.shutdown();

    expect(spawnedWorkerUrls).toEqual([
      "http://localhost:3000/assets/jazz/worker/jazz-worker.js?jazz-wasm-url=http%3A%2F%2Flocalhost%3A3000%2Fassets%2Fjazz%2Fjazz_wasm_bg.wasm",
    ]);
  });

  it("uses the static bundler-detected URL pattern when no runtimeSources are provided", async () => {
    const spawnedWorkerUrls: string[] = [];

    class FakeWorker extends EventTarget {
      constructor(url: string | URL, _options?: WorkerOptions) {
        super();
        spawnedWorkerUrls.push(String(url));
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await Db.createWithWorker({
      appId: "worker-bootstrap-browser-assets",
      driver: { type: "persistent", dbName: "worker-bootstrap-browser-assets" },
    });

    await db.shutdown();

    expect(loadWasmModuleMock).toHaveBeenCalledTimes(1);
    expect(spawnedWorkerUrls).toHaveLength(1);
    expect(spawnedWorkerUrls[0]).toMatch(/worker\/jazz-worker\.js$/);
  });

  it("includes a fallbackWasmUrl in bridge options when no runtimeSources are provided", async () => {
    class FakeWorker extends EventTarget {
      constructor(_url: string | URL, _options?: WorkerOptions) {
        super();
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await Db.createWithWorker({
      appId: "worker-bootstrap-fallback-wasm",
      driver: { type: "persistent", dbName: "worker-bootstrap-fallback-wasm" },
    });

    const options = (db as any).buildWorkerBridgeOptions("{}");
    await db.shutdown();

    expect(options.fallbackWasmUrl).toMatch(/jazz_wasm_bg\.wasm$/);
  });

  it("passes telemetryCollectorUrl into worker bridge options", async () => {
    class FakeWorker extends EventTarget {
      constructor(_url: string | URL, _options?: WorkerOptions) {
        super();
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await Db.createWithWorker({
      appId: "worker-bootstrap-telemetry",
      driver: { type: "persistent", dbName: "worker-bootstrap-telemetry" },
      telemetryCollectorUrl: "http://127.0.0.1:54418",
    });

    const options = (db as any).buildWorkerBridgeOptions("{}");
    await db.shutdown();

    expect(options.telemetryCollectorUrl).toBe("http://127.0.0.1:54418");
  });

  it("does not append a bootstrap wasm URL when runtimeSources provides in-memory wasmSource", async () => {
    const spawnedWorkerUrls: string[] = [];

    class FakeWorker extends EventTarget {
      constructor(url: string | URL, _options?: WorkerOptions) {
        super();
        spawnedWorkerUrls.push(String(url));
        queueMicrotask(() => {
          const event = new Event("message");
          Object.defineProperty(event, "data", {
            value: { type: "ready" },
            configurable: true,
          });
          this.dispatchEvent(event);
        });
      }

      postMessage(): void {}

      terminate(): void {}
    }

    (globalThis as Record<string, unknown>).window = {};
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    (globalThis as Record<string, unknown>).Worker = FakeWorker;

    const db = await Db.createWithWorker({
      appId: "worker-bootstrap-wasm-source",
      driver: { type: "persistent", dbName: "worker-bootstrap-wasm-source" },
      runtimeSources: {
        workerUrl: "/custom/jazz-worker.js",
        wasmSource: new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]),
      },
    });

    await db.shutdown();

    expect(spawnedWorkerUrls).toEqual(["http://localhost:3000/custom/jazz-worker.js"]);
  });
});
