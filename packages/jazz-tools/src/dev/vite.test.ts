import { readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { jazzPlugin } from "./vite.js";
import * as devServer from "./dev-server.js";
import * as schemaWatcher from "./schema-watcher.js";
import { createTempRootTracker, getAvailablePort, todoSchema } from "./test-helpers.js";

const tempRoots = createTempRootTracker();
const originalJazzServerUrl = process.env.VITE_JAZZ_SERVER_URL;
const originalJazzAppId = process.env.VITE_JAZZ_APP_ID;
const originalJazzTelemetryCollectorUrl = process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL;
const originalAdminSecret = process.env.JAZZ_ADMIN_SECRET;

// Managed-runtime writes VITE_JAZZ_APP_ID / VITE_JAZZ_SERVER_URL to process.env
// on successful init; that state leaks across vitest workers in the same thread
// pool and flips later tests onto the env-driven cloud branch. Scrub before each
// test so every case starts from the same baseline.
beforeEach(() => {
  delete process.env.VITE_JAZZ_APP_ID;
  delete process.env.VITE_JAZZ_SERVER_URL;
  delete process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL;
  delete process.env.JAZZ_ADMIN_SECRET;
});

afterEach(async () => {
  vi.restoreAllMocks();
  await tempRoots.cleanup();

  if (originalJazzServerUrl === undefined) {
    delete process.env.VITE_JAZZ_SERVER_URL;
  } else {
    process.env.VITE_JAZZ_SERVER_URL = originalJazzServerUrl;
  }

  if (originalJazzAppId === undefined) {
    delete process.env.VITE_JAZZ_APP_ID;
  } else {
    process.env.VITE_JAZZ_APP_ID = originalJazzAppId;
  }

  if (originalJazzTelemetryCollectorUrl === undefined) {
    delete process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL;
  } else {
    process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL = originalJazzTelemetryCollectorUrl;
  }

  if (originalAdminSecret === undefined) {
    delete process.env.JAZZ_ADMIN_SECRET;
  } else {
    process.env.JAZZ_ADMIN_SECRET = originalAdminSecret;
  }
});

describe("jazzPlugin", () => {
  it("returns a Vite plugin with the correct name", () => {
    const plugin = jazzPlugin();
    expect(plugin.name).toBe("jazz");
  });

  it("config hook injects worker format and optimizeDeps exclude", () => {
    const plugin = jazzPlugin();
    const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
    const result = config!({}) as {
      worker?: { format?: string };
      optimizeDeps?: { exclude?: string[] };
    };
    expect(result.worker?.format).toBe("es");
    expect(result.optimizeDeps?.exclude).toContain("jazz-wasm");
  });

  it("config hook preserves existing optimizeDeps excludes", () => {
    const plugin = jazzPlugin();
    const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
    const result = config!({ optimizeDeps: { exclude: ["some-dep"] } }) as {
      optimizeDeps?: { exclude?: string[] };
    };
    expect(result.optimizeDeps?.exclude).toContain("jazz-wasm");
    expect(result.optimizeDeps?.exclude).toContain("some-dep");
  });

  // Without this alias, a Vite consumer installed via pnpm hits
  // "Failed to resolve import 'jazz-wasm'" at runtime — the bare specifier
  // in jazz-tools' chunk can't be found unless jazz-wasm is hoisted or a
  // direct dep. The alias resolves it from the plugin's own location.
  it("config hook aliases jazz-wasm to an absolute path", () => {
    const plugin = jazzPlugin();
    const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
    const result = config!({}) as {
      resolve?: { alias?: { find: RegExp | string; replacement: string }[] };
    };
    const alias = result.resolve?.alias?.find((a) => String(a.find) === "/^jazz-wasm$/");
    expect(alias).toBeDefined();
    expect(alias!.replacement).toMatch(/jazz_wasm\.js$/);
  });

  it("starts a server and pushes schema via configureServer hook", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-vite-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});

    const plugin = jazzPlugin({
      server: { port, adminSecret: "vite-test-admin" },
      schemaDir,
    });

    const closeHandlers: (() => Promise<void> | void)[] = [];
    const fakeViteServer = {
      config: {
        root: schemaDir,
        command: "serve" as const,
        env: {} as Record<string, string>,
      },
      httpServer: {
        once(_event: string, cb: () => void) {
          closeHandlers.push(cb);
        },
      },
      ws: { send() {} },
    };

    const configureServer = plugin.configureServer as (
      server: typeof fakeViteServer,
    ) => Promise<void>;
    await configureServer(fakeViteServer);

    const healthResponse = await fetch(`http://127.0.0.1:${port}/health`);
    expect(
      healthResponse.ok,
      `health ${healthResponse.status}: ${await healthResponse.text()}`,
    ).toBe(true);

    const schemasResponse = await fetch(
      `http://127.0.0.1:${port}/apps/${fakeViteServer.config.env.VITE_JAZZ_APP_ID}/schemas`,
      {
        headers: { "X-Jazz-Admin-Secret": "vite-test-admin" },
      },
    );
    const schemasRaw = await schemasResponse.text();
    expect(schemasResponse.ok, `schemas ${schemasResponse.status}: ${schemasRaw}`).toBe(true);
    const body = JSON.parse(schemasRaw) as { hashes?: string[] };
    expect(body.hashes?.length).toBeGreaterThan(0);
    expect(fakeViteServer.config.env.VITE_JAZZ_APP_ID).toBeTruthy();
    expect(fakeViteServer.config.env.VITE_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(process.env.VITE_JAZZ_APP_ID).toBe(fakeViteServer.config.env.VITE_JAZZ_APP_ID);
    expect(process.env.VITE_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(logSpy).toHaveBeenCalledWith(
      expect.stringContaining(
        `Open the inspector: https://jazz2-inspector.vercel.app/#serverUrl=${encodeURIComponent(
          `http://127.0.0.1:${port}`,
        )}&appId=${encodeURIComponent(fakeViteServer.config.env.VITE_JAZZ_APP_ID!)}&adminSecret=vite-test-admin`,
      ),
    );

    for (const handler of closeHandlers) {
      await handler();
    }

    await expect(fetch(`http://127.0.0.1:${port}/health`).then((r) => r.ok)).rejects.toThrow();
  }, 30_000);

  it("does not inject a dev server url during build", async () => {
    const plugin = jazzPlugin();
    const fakeViteServer = {
      config: {
        root: "/tmp/jazz-build",
        command: "build" as const,
        env: {} as Record<string, string>,
      },
      httpServer: null,
      ws: { send() {} },
    };

    const configureServer = plugin.configureServer as (
      server: typeof fakeViteServer,
    ) => Promise<void>;
    await configureServer(fakeViteServer);

    expect(fakeViteServer.config.env.VITE_JAZZ_APP_ID).toBeUndefined();
    expect(fakeViteServer.config.env.VITE_JAZZ_SERVER_URL).toBeUndefined();
    expect(process.env.VITE_JAZZ_APP_ID).toBe(originalJazzAppId);
    expect(process.env.VITE_JAZZ_SERVER_URL).toBe(originalJazzServerUrl);
  });

  it("persists the generated appId to .env in the project root", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-vite-env-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const plugin = jazzPlugin({
      server: { port, adminSecret: "vite-env-test-admin" },
      schemaDir,
    });

    const closeHandlers: (() => Promise<void> | void)[] = [];
    const fakeViteServer = {
      config: {
        root: schemaDir,
        command: "serve" as const,
        env: {} as Record<string, string>,
      },
      httpServer: {
        once(_event: string, cb: () => void) {
          closeHandlers.push(cb);
        },
      },
      ws: { send() {} },
    };

    const configureServer = plugin.configureServer as (
      server: typeof fakeViteServer,
    ) => Promise<void>;
    await configureServer(fakeViteServer);

    const generatedAppId = fakeViteServer.config.env.VITE_JAZZ_APP_ID;
    expect(generatedAppId).toBeTruthy();

    const envContent = await readFile(join(schemaDir, ".env"), "utf8");
    expect(envContent).toContain(`VITE_JAZZ_APP_ID=${generatedAppId}`);

    for (const handler of closeHandlers) {
      await handler();
    }
  }, 30_000);

  it("sends a full-reload to the browser on a successful schema watch push", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000060",
      port: 19880,
      url: "http://127.0.0.1:19880",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc123def4567890" });
    let capturedOnPush: ((hash: string) => void) | undefined;
    vi.spyOn(schemaWatcher, "watchSchema").mockImplementation((opts) => {
      capturedOnPush = opts.onPush;
      return { close: vi.fn() };
    });

    const schemaDir = await tempRoots.create("jazz-vite-reload-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const wsSend = vi.fn();
    const fakeViteServer = {
      config: {
        root: schemaDir,
        command: "serve" as const,
        env: {} as Record<string, string>,
      },
      httpServer: {
        once(_event: string, _cb: () => void) {},
      },
      ws: { send: wsSend },
    };

    const plugin = jazzPlugin({
      server: { port: 19880, adminSecret: "vite-reload-admin" },
      schemaDir,
    });
    const configureServer = plugin.configureServer as (
      server: typeof fakeViteServer,
    ) => Promise<void>;
    await configureServer(fakeViteServer);

    expect(capturedOnPush).toBeDefined();
    capturedOnPush!("abc123def4567890");

    expect(wsSend).toHaveBeenCalledWith({ type: "full-reload" });
  });

  it("exposes top-level telemetry options and starts server-side telemetry", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000061",
      port: 19881,
      url: "http://127.0.0.1:19881",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc123def4567890" });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const schemaDir = await tempRoots.create("jazz-vite-top-level-telemetry-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const plugin = jazzPlugin({
      server: { port: 19881, adminSecret: "vite-telemetry-admin" },
      telemetry: "http://127.0.0.1:54418",
      schemaDir,
    });

    const fakeViteServer = {
      config: {
        root: schemaDir,
        command: "serve" as const,
        env: {} as Record<string, string>,
      },
      httpServer: {
        once(_event: string, _cb: () => void) {},
      },
      ws: { send() {} },
    };
    const configureServer = plugin.configureServer as (
      server: typeof fakeViteServer,
    ) => Promise<void>;
    await configureServer(fakeViteServer);

    const startOptions = startSpy.mock.calls[0]![0] as Record<string, unknown>;
    expect(startOptions.telemetryCollectorUrl).toBe("http://127.0.0.1:54418");
    expect(fakeViteServer.config.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL).toBe(
      "http://127.0.0.1:54418",
    );
    expect(logSpy).toHaveBeenCalledWith("[jazz] telemetry collector: http://127.0.0.1:54418");
  });

  it("does not overwrite an existing JAZZ_APP_ID in .env when one is already set", async () => {
    const port = await getAvailablePort();
    const schemaDir = await tempRoots.create("jazz-vite-existing-env-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const existingAppId = "00000000-0000-0000-0000-000000000001";
    await writeFile(join(schemaDir, ".env"), `VITE_JAZZ_APP_ID=${existingAppId}\n`);

    const plugin = jazzPlugin({
      server: { port, adminSecret: "vite-existing-env-test-admin" },
      schemaDir,
    });

    const closeHandlers: (() => Promise<void> | void)[] = [];
    const fakeViteServer = {
      config: {
        root: schemaDir,
        command: "serve" as const,
        env: { VITE_JAZZ_APP_ID: existingAppId } as Record<string, string>,
      },
      httpServer: {
        once(_event: string, cb: () => void) {
          closeHandlers.push(cb);
        },
      },
      ws: { send() {} },
    };

    const configureServer = plugin.configureServer as (
      server: typeof fakeViteServer,
    ) => Promise<void>;
    await configureServer(fakeViteServer);

    expect(fakeViteServer.config.env.VITE_JAZZ_APP_ID).toBe(existingAppId);

    const envContent = await readFile(join(schemaDir, ".env"), "utf8");
    expect(envContent).toContain(`VITE_JAZZ_APP_ID=${existingAppId}`);
    expect(envContent.match(/JAZZ_APP_ID=/g)?.length).toBe(1);

    for (const handler of closeHandlers) {
      await handler();
    }
  }, 30_000);

  it("exposes telemetry collector url when reusing an env-provided server", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    process.env.VITE_JAZZ_SERVER_URL = "http://localhost:4242";
    process.env.VITE_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000090";
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc123def4567890" });
    vi.spyOn(devServer, "startLocalJazzServer");
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const schemaDir = await tempRoots.create("jazz-vite-telemetry-string-server-test-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    const plugin = jazzPlugin({
      appId: "00000000-0000-0000-0000-000000000090",
      adminSecret: "vite-telemetry-admin",
      telemetry: "http://127.0.0.1:54418",
      schemaDir,
    });
    const configureServer = plugin.configureServer as (server: {
      config: {
        root: string;
        command: "serve";
        env: Record<string, string>;
      };
      httpServer: { once(_event: string, _cb: () => void): void };
      ws: { send(): void };
    }) => Promise<void>;

    await configureServer({
      config: {
        root: schemaDir,
        command: "serve",
        env: {},
      },
      httpServer: { once() {} },
      ws: { send() {} },
    });

    expect(devServer.startLocalJazzServer).not.toHaveBeenCalled();
    expect(process.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL).toBe("http://127.0.0.1:54418");
    expect(logSpy).toHaveBeenCalledWith("[jazz] telemetry collector: http://127.0.0.1:54418");
  });
});
