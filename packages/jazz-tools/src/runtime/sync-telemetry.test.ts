import { afterEach, describe, expect, it, vi } from "vitest";

const otelMocks = vi.hoisted(() => {
  const traceExporterConstructors = vi.fn();
  const traceProviderConstructors = vi.fn();
  const traceProcessorConstructors = vi.fn();
  const tracerNames: string[] = [];
  const startSpan = vi.fn((name: string, options: unknown) => {
    const span = { name, options, end: vi.fn() };
    traceSpans.push(span);
    return span;
  });
  const traceSpans: Array<{
    name: string;
    options: unknown;
    end: ReturnType<typeof vi.fn>;
  }> = [];
  const traceForceFlush = vi.fn(() => Promise.resolve());

  const logExporterConstructors = vi.fn();
  const logProviderConstructors = vi.fn();
  const logProcessorConstructors = vi.fn();
  const loggerNames: string[] = [];
  const emitLog = vi.fn();

  return {
    traceExporterConstructors,
    traceProviderConstructors,
    traceProcessorConstructors,
    tracerNames,
    startSpan,
    traceSpans,
    traceForceFlush,
    logExporterConstructors,
    logProviderConstructors,
    logProcessorConstructors,
    loggerNames,
    emitLog,
  };
});

vi.mock("@opentelemetry/exporter-trace-otlp-http", () => ({
  OTLPTraceExporter: class {
    constructor(config: unknown) {
      otelMocks.traceExporterConstructors(config);
    }
  },
}));

vi.mock("@opentelemetry/sdk-trace-base", () => ({
  BasicTracerProvider: class {
    constructor(config: unknown) {
      otelMocks.traceProviderConstructors(config);
    }

    getTracer(name: string) {
      otelMocks.tracerNames.push(name);
      return { startSpan: otelMocks.startSpan };
    }

    forceFlush() {
      return otelMocks.traceForceFlush();
    }
  },
  BatchSpanProcessor: class {
    constructor(exporter: unknown) {
      otelMocks.traceProcessorConstructors(exporter);
    }
  },
}));

vi.mock("@opentelemetry/exporter-logs-otlp-http", () => ({
  OTLPLogExporter: class {
    constructor(config: unknown) {
      otelMocks.logExporterConstructors(config);
    }
  },
}));

vi.mock("@opentelemetry/sdk-logs", () => ({
  LoggerProvider: class {
    constructor(config: unknown) {
      otelMocks.logProviderConstructors(config);
    }

    getLogger(name: string) {
      otelMocks.loggerNames.push(name);
      return { emit: otelMocks.emitLog };
    }
  },
  BatchLogRecordProcessor: class {
    constructor(exporter: unknown) {
      otelMocks.logProcessorConstructors(exporter);
    }
  },
}));

vi.mock("@opentelemetry/resources", () => ({
  resourceFromAttributes: vi.fn((attributes: unknown) => ({ attributes })),
}));

import {
  installWasmTelemetry,
  normalizeOtlpEndpoint,
  resolveTelemetryCollectorUrlFromEnv,
} from "./sync-telemetry.js";

type WasmTraceEntry = import("jazz-wasm").WasmTraceEntry;
type TraceEntrySubscriber = () => void;

type MockWasmModule = {
  setTraceEntryCollectionEnabled: ReturnType<typeof vi.fn<(enabled: boolean) => void>>;
  drainTraceEntries: ReturnType<typeof vi.fn<() => WasmTraceEntry[]>>;
  subscribeTraceEntries: ReturnType<typeof vi.fn<(callback: TraceEntrySubscriber) => () => void>>;
  subscribers: TraceEntrySubscriber[];
  unsubscribeTraceEntries: ReturnType<typeof vi.fn<() => void>>;
};

function createWasmModule(drains: WasmTraceEntry[][] = []): MockWasmModule {
  const subscribers: TraceEntrySubscriber[] = [];
  const unsubscribeTraceEntries = vi.fn(() => {
    subscribers.length = 0;
  });

  return {
    setTraceEntryCollectionEnabled: vi.fn<(enabled: boolean) => void>(),
    drainTraceEntries: vi.fn<() => WasmTraceEntry[]>(() => drains.shift() ?? []),
    subscribeTraceEntries: vi.fn((callback: TraceEntrySubscriber) => {
      subscribers.push(callback);
      return unsubscribeTraceEntries;
    }),
    subscribers,
    unsubscribeTraceEntries,
  };
}

async function flushQueuedMicrotasks(): Promise<void> {
  await Promise.resolve();
  await Promise.resolve();
}

const originalFetch = globalThis.fetch;
const originalTelemetryEnv = {
  VITE_JAZZ_TELEMETRY_COLLECTOR_URL: process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL,
  NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL: process.env.NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL,
  PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL: process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL,
  EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL: process.env.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL,
};

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
  for (const mock of [
    otelMocks.traceExporterConstructors,
    otelMocks.traceProviderConstructors,
    otelMocks.traceProcessorConstructors,
    otelMocks.startSpan,
    otelMocks.traceForceFlush,
    otelMocks.logExporterConstructors,
    otelMocks.logProviderConstructors,
    otelMocks.logProcessorConstructors,
    otelMocks.emitLog,
  ]) {
    mock.mockClear();
  }
  otelMocks.tracerNames.length = 0;
  otelMocks.traceSpans.length = 0;
  otelMocks.loggerNames.length = 0;
  if (originalFetch === undefined) {
    delete (globalThis as { fetch?: typeof fetch }).fetch;
  } else {
    globalThis.fetch = originalFetch;
  }
  delete (globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__;

  for (const [key, value] of Object.entries(originalTelemetryEnv)) {
    if (value === undefined) {
      delete process.env[key];
    } else {
      process.env[key] = value;
    }
  }
});

describe("telemetry OTLP helpers", () => {
  it("normalizes collector base urls and full OTLP endpoints", () => {
    expect(normalizeOtlpEndpoint("http://localhost:4318", "traces")).toBe(
      "http://localhost:4318/v1/traces",
    );
    expect(normalizeOtlpEndpoint("http://localhost:4318/v1/traces", "traces")).toBe(
      "http://localhost:4318/v1/traces",
    );
    expect(normalizeOtlpEndpoint("http://localhost:4318/v1/logs", "traces")).toBe(
      "http://localhost:4318/v1/traces",
    );
    expect(normalizeOtlpEndpoint("http://localhost:4318", "logs")).toBe(
      "http://localhost:4318/v1/logs",
    );
    expect(normalizeOtlpEndpoint("http://localhost:4318/v1/traces", "logs")).toBe(
      "http://localhost:4318/v1/logs",
    );
  });

  it("resolves collector url from literal public env keys", () => {
    delete process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL;
    process.env.NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL = " http://127.0.0.1:54418 ";
    delete process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;
    delete process.env.EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;

    expect(resolveTelemetryCollectorUrlFromEnv()).toBe("http://127.0.0.1:54418");
  });

  it("does not enable Rust collection or subscribe without a collector URL", async () => {
    const wasmModule = createWasmModule();

    installWasmTelemetry({
      wasmModule,
      collectorUrl: undefined,
      appId: "telemetry-app",
      runtimeThread: "worker",
    });

    await flushQueuedMicrotasks();

    expect(wasmModule.setTraceEntryCollectionEnabled).not.toHaveBeenCalled();
    expect(wasmModule.subscribeTraceEntries).not.toHaveBeenCalled();
    expect(wasmModule.drainTraceEntries).not.toHaveBeenCalled();
    expect(otelMocks.traceExporterConstructors).not.toHaveBeenCalled();
    expect(otelMocks.logExporterConstructors).not.toHaveBeenCalled();
  });

  it("does not throw when the WASM module has no trace entry hooks", async () => {
    const warnSpy = vi.spyOn(console, "warn").mockImplementation(() => {});

    const dispose = installWasmTelemetry({
      wasmModule: {} as MockWasmModule,
      collectorUrl: "http://127.0.0.1:54418",
      appId: "telemetry-app",
      runtimeThread: "worker",
    });

    dispose();
    await flushQueuedMicrotasks();

    expect(warnSpy).toHaveBeenCalledWith(
      "[jazz] WASM telemetry unavailable: trace entry hooks are missing.",
    );
    expect(otelMocks.traceExporterConstructors).not.toHaveBeenCalled();
    expect(otelMocks.logExporterConstructors).not.toHaveBeenCalled();
  });

  it("enables Rust collection and drains WASM telemetry when Rust notifies JS", async () => {
    const setIntervalSpy = vi.spyOn(globalThis, "setInterval");
    const wasmModule = createWasmModule([[], []]);

    const dispose = installWasmTelemetry({
      wasmModule,
      collectorUrl: "http://127.0.0.1:54418",
      appId: "telemetry-app",
      runtimeThread: "worker",
    });

    expect(wasmModule.setTraceEntryCollectionEnabled).toHaveBeenCalledWith(true);
    expect(wasmModule.subscribeTraceEntries).toHaveBeenCalledTimes(1);
    expect(setIntervalSpy).not.toHaveBeenCalled();
    expect((globalThis as Record<string, unknown>).__JAZZ_WASM_TRACE_SPAN__).toBeUndefined();

    wasmModule.subscribers[0]!();
    await flushQueuedMicrotasks();

    wasmModule.subscribers[0]!();
    await flushQueuedMicrotasks();

    expect(wasmModule.drainTraceEntries).toHaveBeenCalledTimes(2);
    dispose();
  });

  it("coalesces notifications while the drain microtask is pending", async () => {
    const wasmModule = createWasmModule([[], []]);

    const dispose = installWasmTelemetry({
      wasmModule,
      collectorUrl: "http://127.0.0.1:54418",
      appId: "telemetry-app",
      runtimeThread: "worker",
    });

    wasmModule.subscribers[0]!();
    wasmModule.subscribers[0]!();
    wasmModule.subscribers[0]!();

    expect(wasmModule.drainTraceEntries).not.toHaveBeenCalled();

    await flushQueuedMicrotasks();

    expect(wasmModule.drainTraceEntries).toHaveBeenCalledTimes(1);

    wasmModule.subscribers[0]!();
    await flushQueuedMicrotasks();

    expect(wasmModule.drainTraceEntries).toHaveBeenCalledTimes(2);
    dispose();
  });

  it("exports drained span, log, and dropped entries through OpenTelemetry", async () => {
    const wasmModule = createWasmModule([
      [
        {
          kind: "span",
          sequence: 0,
          name: "OpfsBTree::put",
          target: "opfs_btree::db",
          level: "TRACE",
          fields: { key_len: "8" },
          startUnixNano: "1775000000000000000",
          endUnixNano: "1775000000000001000",
        },
        {
          kind: "log",
          sequence: 1,
          target: "opfs_btree::db",
          level: "WARN",
          fields: { attempt: "2" },
          message: "retrying write",
          timestampUnixNano: "1775000000000002000",
        },
        { kind: "dropped", count: 3 },
      ],
    ]);

    const dispose = installWasmTelemetry({
      wasmModule,
      collectorUrl: "http://127.0.0.1:54418",
      appId: "telemetry-app",
      runtimeThread: "worker",
    });

    wasmModule.subscribers[0]!();
    await vi.waitFor(() => expect(otelMocks.startSpan).toHaveBeenCalledTimes(1));
    await vi.waitFor(() => expect(otelMocks.emitLog).toHaveBeenCalledTimes(2));

    expect(otelMocks.traceExporterConstructors).toHaveBeenCalledWith({
      url: "http://127.0.0.1:54418/v1/traces",
    });
    expect(otelMocks.logExporterConstructors).toHaveBeenCalledWith({
      url: "http://127.0.0.1:54418/v1/logs",
    });
    expect(otelMocks.startSpan).toHaveBeenCalledWith(
      "OpfsBTree::put",
      expect.objectContaining({
        attributes: expect.objectContaining({
          "jazz.runtime_thread": "worker",
          "jazz.span.target": "opfs_btree::db",
        }),
      }),
    );
    expect(otelMocks.traceSpans[0]!.end).toHaveBeenCalledWith([1775000000, 1000]);
    expect(otelMocks.emitLog).toHaveBeenCalledWith(
      expect.objectContaining({
        body: "retrying write",
        severityText: "WARN",
        attributes: expect.objectContaining({
          "jazz.runtime_thread": "worker",
          "jazz.log.target": "opfs_btree::db",
        }),
      }),
    );
    expect(otelMocks.emitLog).toHaveBeenCalledWith(
      expect.objectContaining({
        body: "Dropped 3 WASM telemetry records",
        severityText: "WARN",
      }),
    );

    dispose();
  });

  it("drains once and disables Rust collection on dispose", () => {
    const wasmModule = createWasmModule([[]]);

    const dispose = installWasmTelemetry({
      wasmModule,
      collectorUrl: "http://127.0.0.1:54418",
      appId: "telemetry-app",
      runtimeThread: "main",
    });

    dispose();

    expect(wasmModule.drainTraceEntries).toHaveBeenCalledTimes(1);
    expect(wasmModule.unsubscribeTraceEntries).toHaveBeenCalledTimes(1);
    expect(wasmModule.setTraceEntryCollectionEnabled).toHaveBeenLastCalledWith(false);
  });
});
