import { afterEach, describe, expect, it, vi } from "vitest";

const TELEMETRY_ENV_KEYS = [
  "VITE_JAZZ_TELEMETRY_COLLECTOR_URL",
  "NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
  "EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
] as const;

async function loadDbWithTelemetryMocks(wasmModule: unknown = {}) {
  vi.resetModules();

  const loadWasmModuleMock = vi.fn().mockResolvedValue(wasmModule);
  const disposeWasmTelemetryMock = vi.fn();
  const installWasmTelemetryMock = vi.fn(() => disposeWasmTelemetryMock);

  vi.doMock("./client.js", async (importOriginal) => {
    const actual = await importOriginal<typeof import("./client.js")>();
    return {
      ...actual,
      loadWasmModule: loadWasmModuleMock,
    };
  });

  vi.doMock("./sync-telemetry.js", () => ({
    installWasmTelemetry: installWasmTelemetryMock,
    resolveTelemetryCollectorUrlFromEnv: vi.fn(() => undefined),
  }));

  const { Db } = await import("./db.js");
  return {
    Db,
    disposeWasmTelemetryMock,
    installWasmTelemetryMock,
    loadWasmModuleMock,
  };
}

afterEach(() => {
  vi.doUnmock("./client.js");
  vi.doUnmock("./sync-telemetry.js");
  vi.restoreAllMocks();
  for (const key of TELEMETRY_ENV_KEYS) {
    delete process.env[key];
  }
});

describe("Db WASM telemetry", () => {
  it("does not start main-thread telemetry when telemetry is disabled", async () => {
    const { Db, installWasmTelemetryMock } = await loadDbWithTelemetryMocks();
    const db = await Db.create({ appId: "main-no-telemetry" });

    (db as any).installMainThreadWasmTelemetry();

    expect(installWasmTelemetryMock).not.toHaveBeenCalled();
    await db.shutdown();
  });

  it("starts main-thread telemetry only when a collector URL exists", async () => {
    const wasmModule = {
      setTraceEntryCollectionEnabled: vi.fn(),
      drainTraceEntries: vi.fn(),
      subscribeTraceEntries: vi.fn(() => vi.fn()),
    };
    const { Db, disposeWasmTelemetryMock, installWasmTelemetryMock } =
      await loadDbWithTelemetryMocks(wasmModule);
    const db = await Db.create({
      appId: "main-with-telemetry",
      telemetryCollectorUrl: "http://127.0.0.1:54418",
    });

    (db as any).installMainThreadWasmTelemetry();

    expect(installWasmTelemetryMock).toHaveBeenCalledTimes(1);
    expect(installWasmTelemetryMock).toHaveBeenCalledWith({
      wasmModule,
      collectorUrl: "http://127.0.0.1:54418",
      appId: "main-with-telemetry",
      runtimeThread: "main",
    });

    await db.shutdown();
    expect(disposeWasmTelemetryMock).toHaveBeenCalledTimes(1);
  });
});
