import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { createTempRootTracker, getAvailablePort, todoSchema } from "./test-helpers.js";
import * as devServer from "./dev-server.js";
import * as schemaWatcher from "./schema-watcher.js";
import { jazzSvelteKit, __resetJazzSvelteKitPluginForTests } from "./sveltekit.js";
import type { ViteDevServer } from "./vite.js";

const dev = await import("./index.js");

const tempRoots = createTempRootTracker();
const originalJazzAppId = process.env.PUBLIC_JAZZ_APP_ID;
const originalJazzServerUrl = process.env.PUBLIC_JAZZ_SERVER_URL;
const originalJazzTelemetryCollectorUrl = process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;
const originalBackendSecret = process.env.BACKEND_SECRET;

function makeViteServer(
  command: "serve" | "build",
  root = "/tmp/jazz-sveltekit-test",
): ViteDevServer & { restart: ReturnType<typeof vi.fn> } {
  return {
    config: { root, command, env: {} },
    httpServer: { once() {} },
    ws: { send() {} },
    restart: vi.fn(() => Promise.resolve()),
  };
}

// Managed-runtime writes PUBLIC_JAZZ_APP_ID / PUBLIC_JAZZ_SERVER_URL to
// process.env on successful init; that state leaks across vitest workers in the
// same thread pool and flips later tests onto the env-driven cloud branch.
// Scrub before each test so every case starts from the same baseline.
beforeEach(() => {
  delete process.env.PUBLIC_JAZZ_APP_ID;
  delete process.env.PUBLIC_JAZZ_SERVER_URL;
  delete process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;
  delete process.env.JAZZ_ADMIN_SECRET;
  delete process.env.BACKEND_SECRET;
});

afterEach(async () => {
  await __resetJazzSvelteKitPluginForTests();
  await tempRoots.cleanup();
  // Shared /tmp roots accumulate .env files from managed-runtime's app-id
  // persistence; wipe them so the plugin's env-file backfill starts clean.
  for (const shared of ["/tmp/jazz-sveltekit-test", "/tmp/jazz-sk-noserver"]) {
    await rm(join(shared, ".env"), { force: true }).catch(() => undefined);
  }
  vi.restoreAllMocks();

  if (originalJazzAppId === undefined) {
    delete process.env.PUBLIC_JAZZ_APP_ID;
  } else {
    process.env.PUBLIC_JAZZ_APP_ID = originalJazzAppId;
  }

  if (originalJazzServerUrl === undefined) {
    delete process.env.PUBLIC_JAZZ_SERVER_URL;
  } else {
    process.env.PUBLIC_JAZZ_SERVER_URL = originalJazzServerUrl;
  }

  if (originalJazzTelemetryCollectorUrl === undefined) {
    delete process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL;
  } else {
    process.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL = originalJazzTelemetryCollectorUrl;
  }

  if (originalBackendSecret === undefined) {
    delete process.env.BACKEND_SECRET;
  } else {
    process.env.BACKEND_SECRET = originalBackendSecret;
  }
});

describe("jazzSvelteKit", () => {
  it("starts a local server in dev and injects PUBLIC_JAZZ_* env vars", async () => {
    const port = await getAvailablePort();
    const root = await tempRoots.create("jazz-sveltekit-test-");
    await mkdir(join(root, "src", "lib"), { recursive: true });
    await writeFile(join(root, "src", "lib", "schema.ts"), todoSchema());

    const plugin = jazzSvelteKit({
      server: { port, adminSecret: "sveltekit-test-admin" },
    });
    const viteServer = makeViteServer("serve", root);
    const configureServer = plugin.configureServer as (server: typeof viteServer) => Promise<void>;
    await configureServer(viteServer);

    const healthResponse = await fetch(`http://127.0.0.1:${port}/health`);
    expect(healthResponse.ok).toBe(true);

    const schemasResponse = await fetch(
      `http://127.0.0.1:${port}/apps/${viteServer.config.env!.PUBLIC_JAZZ_APP_ID}/schemas`,
      {
        headers: { "X-Jazz-Admin-Secret": "sveltekit-test-admin" },
      },
    );
    expect(schemasResponse.ok).toBe(true);
    const body = (await schemasResponse.json()) as { hashes?: string[] };
    expect(body.hashes?.length).toBeGreaterThan(0);

    expect(viteServer.config.env!.PUBLIC_JAZZ_APP_ID).toBeTruthy();
    expect(viteServer.config.env!.PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBe(viteServer.config.env!.PUBLIC_JAZZ_APP_ID);
    expect(process.env.PUBLIC_JAZZ_SERVER_URL).toBe(`http://127.0.0.1:${port}`);
  }, 30_000);

  it("exposes top-level telemetry options and starts server-side telemetry", async () => {
    const logSpy = vi.spyOn(console, "log").mockImplementation(() => {});
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000062",
      port: 19882,
      url: "http://127.0.0.1:19882",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc" });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-top-level-telemetry-test-");
    const plugin = jazzSvelteKit({
      server: { port: 19882, adminSecret: "sveltekit-telemetry-admin" },
      telemetry: "http://127.0.0.1:54418",
    });
    const viteServer = makeViteServer("serve", root);
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    const startOptions = startSpy.mock.calls[0]![0] as Record<string, unknown>;
    expect(startOptions.telemetryCollectorUrl).toBe("http://127.0.0.1:54418");
    expect(viteServer.config.env!.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL).toBe(
      "http://127.0.0.1:54418",
    );
    expect(logSpy).toHaveBeenCalledWith("[jazz] telemetry collector: http://127.0.0.1:54418");
  });

  it("restarts the dev server on first-ever cold start so SvelteKit's $env capture sees the freshly allocated appId", async () => {
    const port = await getAvailablePort();
    const root = await tempRoots.create("jazz-sveltekit-cold-start-");
    await mkdir(join(root, "src", "lib"), { recursive: true });
    await writeFile(join(root, "src", "lib", "schema.ts"), todoSchema());

    const plugin = jazzSvelteKit({
      server: { port, adminSecret: "cold-start-admin" },
    });
    const viteServer = makeViteServer("serve", root);
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(viteServer.restart).toHaveBeenCalledTimes(1);
  }, 30_000);

  it("does not restart when appId is already persisted in .env (warm start)", async () => {
    const persistedAppId = "00000000-0000-0000-0000-000000000099";
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: persistedAppId,
      port: 19990,
      url: "http://127.0.0.1:19990",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc" });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-warm-start-");
    await writeFile(join(root, ".env"), `PUBLIC_JAZZ_APP_ID=${persistedAppId}\n`);

    const plugin = jazzSvelteKit({
      server: { port: 19990, adminSecret: "warm-start-admin" },
    });
    const viteServer = makeViteServer("serve", root);
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(viteServer.restart).not.toHaveBeenCalled();
  });

  it("does not start a server during build", async () => {
    const spy = vi.spyOn(devServer, "startLocalJazzServer");

    const plugin = jazzSvelteKit({
      server: { port: 19999, adminSecret: "build-admin" },
    });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("build"));

    expect(spy).not.toHaveBeenCalled();
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBeUndefined();
  });

  it("does not start a server when server:false", async () => {
    const spy = vi.spyOn(devServer, "startLocalJazzServer");

    const plugin = jazzSvelteKit({ server: false });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve"));

    expect(spy).not.toHaveBeenCalled();
    expect(process.env.PUBLIC_JAZZ_APP_ID).toBeUndefined();
  });

  it("injects BACKEND_SECRET from the server handle", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000001",
      port: 19998,
      url: "http://127.0.0.1:19998",
      dataDir: undefined as unknown as string,
      backendSecret: "test-backend-secret",
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({
      hash: "abc",
    });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({
      server: { port: 19998, adminSecret: "backend-secret-admin" },
    });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve"));

    expect(process.env.BACKEND_SECRET).toBe("test-backend-secret");
  });

  it("builds jwksUrl from Vite's configured host and port", async () => {
    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000004",
      port: 19995,
      url: "http://127.0.0.1:19995",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({
      hash: "abc",
    });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-jwks-test-");
    const plugin = jazzSvelteKit({
      server: { port: 19995, adminSecret: "jwks-admin" },
    });
    const viteServer: ViteDevServer = {
      config: { root, command: "serve", env: {}, server: { port: 3000 } },
      httpServer: { once() {} },
      ws: { send() {} },
      restart: vi.fn(),
    };
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(startSpy).toHaveBeenCalledWith(
      expect.objectContaining({
        jwksUrl: "http://localhost:3000/api/auth/jwks",
      }),
    );
  });

  it("respects APP_ORIGIN when set, over Vite's configured port", async () => {
    const originalAppOrigin = process.env.APP_ORIGIN;
    process.env.APP_ORIGIN = "https://app.example.com";

    const startSpy = vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000005",
      port: 19994,
      url: "http://127.0.0.1:19994",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({
      hash: "abc",
    });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    try {
      const root = await tempRoots.create("jazz-sveltekit-apporigin-test-");
      const plugin = jazzSvelteKit({
        server: { port: 19994, adminSecret: "app-origin-admin" },
      });
      const viteServer: ViteDevServer = {
        config: { root, command: "serve", env: {}, server: { port: 3000 } },
        httpServer: { once() {} },
        ws: { send() {} },
        restart: vi.fn(),
      };
      await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

      expect(startSpy).toHaveBeenCalledWith(
        expect.objectContaining({
          jwksUrl: "https://app.example.com/api/auth/jwks",
        }),
      );
    } finally {
      if (originalAppOrigin === undefined) {
        delete process.env.APP_ORIGIN;
      } else {
        process.env.APP_ORIGIN = originalAppOrigin;
      }
    }
  });

  it("connects to an existing server via PUBLIC_JAZZ_SERVER_URL env var", async () => {
    process.env.PUBLIC_JAZZ_SERVER_URL = "http://jazz-test-server:4000";
    process.env.PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000010";

    vi.spyOn(devServer, "startLocalJazzServer");
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({
      hash: "abc",
    });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({ adminSecret: "env-test-admin" });
    const viteServer = makeViteServer("serve");
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(devServer.startLocalJazzServer).not.toHaveBeenCalled();
    expect(devServer.pushSchemaCatalogue).toHaveBeenCalledWith(
      expect.objectContaining({
        serverUrl: "http://jazz-test-server:4000",
        appId: "00000000-0000-0000-0000-000000000010",
      }),
    );
    expect(viteServer.config.env!.PUBLIC_JAZZ_SERVER_URL).toBe("http://jazz-test-server:4000");
  });

  it("connects to an existing server via options.server string URL", async () => {
    vi.spyOn(devServer, "startLocalJazzServer");
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({
      hash: "abc",
    });
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const plugin = jazzSvelteKit({
      server: "http://explicit-server:5000",
      adminSecret: "str-admin",
      appId: "00000000-0000-0000-0000-000000000020",
    });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve"));

    expect(devServer.startLocalJazzServer).not.toHaveBeenCalled();
    expect(devServer.pushSchemaCatalogue).toHaveBeenCalledWith(
      expect.objectContaining({
        serverUrl: "http://explicit-server:5000",
        appId: "00000000-0000-0000-0000-000000000020",
      }),
    );
  });

  it("throws when connecting to an existing server without adminSecret", async () => {
    process.env.PUBLIC_JAZZ_SERVER_URL = "http://jazz-test-server:4000";
    process.env.PUBLIC_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000010";

    const plugin = jazzSvelteKit({});
    await expect(
      (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve")),
    ).rejects.toThrow("adminSecret is required when connecting to an existing server");
  });

  it("throws when connecting to an existing server without appId", async () => {
    process.env.PUBLIC_JAZZ_SERVER_URL = "http://jazz-test-server:4000";
    delete process.env.PUBLIC_JAZZ_APP_ID;

    const plugin = jazzSvelteKit({ adminSecret: "admin" });
    await expect(
      (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(makeViteServer("serve")),
    ).rejects.toThrow("appId is required when connecting to an existing server");
  });

  it("sends a full-reload to the browser on a successful schema watch push", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000050",
      port: 19890,
      url: "http://127.0.0.1:19890",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockResolvedValue({ hash: "abc123def4567890" });
    let capturedOnPush: ((hash: string) => void) | undefined;
    vi.spyOn(schemaWatcher, "watchSchema").mockImplementation((opts) => {
      capturedOnPush = opts.onPush;
      return { close: vi.fn() };
    });

    const root = await tempRoots.create("jazz-sveltekit-reload-test-");
    const wsSend = vi.fn();
    const viteServer: ViteDevServer & { restart: ReturnType<typeof vi.fn> } = {
      config: { root, command: "serve", env: {} },
      httpServer: { once() {} },
      ws: { send: wsSend },
      restart: vi.fn(() => Promise.resolve()),
    };

    const plugin = jazzSvelteKit({
      server: { port: 19890, adminSecret: "reload-admin" },
    });
    await (plugin.configureServer as (s: ViteDevServer) => Promise<void>)(viteServer);

    expect(capturedOnPush).toBeDefined();
    capturedOnPush!("abc123def4567890");

    expect(wsSend).toHaveBeenCalledWith({ type: "full-reload" });
  });

  it("surfaces schema push failures as HMR errors", async () => {
    vi.spyOn(devServer, "startLocalJazzServer").mockResolvedValue({
      appId: "00000000-0000-0000-0000-000000000003",
      port: 19996,
      url: "http://127.0.0.1:19996",
      dataDir: undefined as unknown as string,
      stop: vi.fn().mockResolvedValue(undefined),
    });
    vi.spyOn(devServer, "pushSchemaCatalogue").mockRejectedValue(new Error("schema push failed"));
    vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({ close: vi.fn() });

    const root = await tempRoots.create("jazz-sveltekit-hmr-test-");
    const wsSend = vi.fn();
    const viteServer: ViteDevServer = {
      config: { root, command: "serve", env: {} },
      httpServer: { once() {} },
      ws: { send: wsSend },
    };

    const plugin = jazzSvelteKit({
      server: { port: 19996, adminSecret: "hmr-error-admin" },
    });
    const configureServer = plugin.configureServer as (s: ViteDevServer) => Promise<void>;

    await expect(configureServer(viteServer)).rejects.toThrow("schema push failed");
    expect(wsSend).toHaveBeenCalledWith(
      expect.objectContaining({
        type: "error",
        err: expect.objectContaining({
          message: expect.stringContaining("schema push failed"),
        }),
      }),
    );
  });
});

it("config hook adds jazz-napi to ssr.external", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  expect(config).toBeDefined();
  const result = config!({}) as { ssr?: { external?: string[] } };
  expect(result.ssr?.external).toContain("jazz-napi");
});

it("config hook preserves existing ssr.external entries", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  const result = config!({ ssr: { external: ["some-other-pkg"] } }) as {
    ssr?: { external?: string[] };
  };
  expect(result.ssr?.external).toContain("jazz-napi");
  expect(result.ssr?.external).toContain("some-other-pkg");
});

it("config hook injects worker format and optimizeDeps exclude", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  const result = config!({}) as {
    worker?: { format?: string };
    optimizeDeps?: { exclude?: string[] };
  };
  expect(result.worker?.format).toBe("es");
  expect(result.optimizeDeps?.exclude).toContain("jazz-wasm");
});

it("config hook preserves existing optimizeDeps excludes", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  const result = config!({ optimizeDeps: { exclude: ["some-dep"] } }) as {
    optimizeDeps?: { exclude?: string[] };
  };
  expect(result.optimizeDeps?.exclude).toContain("jazz-wasm");
  expect(result.optimizeDeps?.exclude).toContain("some-dep");
});

// Without this alias, a pnpm-installed SvelteKit app hits
// "Failed to resolve import 'jazz-wasm'" at runtime — the bare specifier
// in jazz-tools' chunk can't be found unless jazz-wasm is hoisted or a
// direct dep. The alias resolves it from the plugin's own location.
it("config hook aliases jazz-wasm to an absolute path", () => {
  const plugin = jazzSvelteKit();
  const config = (plugin as { config?: (c: Record<string, unknown>) => unknown }).config;
  const result = config!({}) as {
    resolve?: { alias?: { find: RegExp | string; replacement: string }[] };
  };
  const alias = result.resolve?.alias?.find((a) => String(a.find) === "/^jazz-wasm$/");
  expect(alias).toBeDefined();
  expect(alias!.replacement).toMatch(/jazz_wasm\.js$/);
});

describe("dev barrel", () => {
  it("exposes jazzSvelteKit", () => {
    expect((dev as Record<string, unknown>).jazzSvelteKit).toBeDefined();
  });
});
