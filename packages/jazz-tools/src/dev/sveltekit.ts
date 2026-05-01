import { join } from "node:path";
import { loadEnvFileIntoProcessEnv } from "./env-file.js";
import { ManagedDevRuntime } from "./managed-runtime.js";
import { resolveJazzWasmEntry } from "./vite.js";
import type {
  JazzServerOptions as BaseJazzServerOptions,
  JazzPluginOptions as BaseJazzPluginOptions,
  ViteDevServer,
} from "./vite.js";

const LOG_PREFIX = "[jazz]";

// SvelteKit extends the shared Vite-plugin options with server-side concerns.
// backendSecret is injected into process.env so server-side SvelteKit code
// (+page.server.ts, +server.ts, hooks) can read it. A Vite+React SPA has no
// server side and would have nothing to consume the env var, which is why
// the field lives here and not on the base Vite plugin options.
export interface JazzServerOptions extends BaseJazzServerOptions {
  backendSecret?: string;
}

export interface JazzPluginOptions extends Omit<BaseJazzPluginOptions, "server"> {
  server?: boolean | string | JazzServerOptions;
}

function resolveViteOrigin(viteServer: ViteDevServer): string {
  const server = viteServer.config.server;
  const port = server?.port ?? 5173;
  const hostOpt = server?.host;
  // Vite's `server.host` accepts boolean (true = listen on all, undefined/false
  // = localhost) or a string hostname. Only use the string form for building
  // the origin — the boolean form still means the dev URL is localhost.
  const host = typeof hostOpt === "string" ? hostOpt : "localhost";
  const scheme = server?.https ? "https" : "http";
  return `${scheme}://${host}:${port}`;
}

const runtime = new ManagedDevRuntime({
  appId: "PUBLIC_JAZZ_APP_ID",
  serverUrl: "PUBLIC_JAZZ_SERVER_URL",
  telemetryCollectorUrl: "PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
});

export function jazzSvelteKit(options: JazzPluginOptions = {}) {
  return {
    name: "jazz-sveltekit",

    config(config: { ssr?: { external?: string[] }; optimizeDeps?: { exclude?: string[] } }) {
      const existingSsr = config.ssr?.external ?? [];
      const existingExclude = config.optimizeDeps?.exclude ?? [];
      const jazzWasmEntry = resolveJazzWasmEntry();
      return {
        worker: { format: "es" as const },
        optimizeDeps: {
          exclude: Array.from(new Set([...existingExclude, "jazz-wasm"])),
        },
        ssr: { external: Array.from(new Set([...existingSsr, "jazz-napi"])) },
        ...(jazzWasmEntry
          ? { resolve: { alias: [{ find: /^jazz-wasm$/, replacement: jazzWasmEntry }] } }
          : {}),
      };
    },

    async configureServer(viteServer: ViteDevServer): Promise<void> {
      if (viteServer.config.command !== "serve" || options.server === false) return;

      // Vite (and SvelteKit-via-Vite) doesn't populate process.env from .env
      // for unprefixed keys, so the managed runtime's env-driven cloud-mode check
      // would otherwise never fire. Backfill before reading.
      loadEnvFileIntoProcessEnv(viteServer.config.root);

      // SvelteKit's vite-plugin captures env in its `config: { order: 'pre' }`
      // hook, before configureServer ever runs. If this is the first-ever start
      // (no persisted appId yet), SvelteKit's captured env is empty when we
      // allocate one below. Trigger a restart so its config hook re-runs and
      // re-reads the now-populated .env. No-ops on warm starts.
      const wasColdStart = !process.env.PUBLIC_JAZZ_APP_ID;

      const schemaDir = options.schemaDir ?? join(viteServer.config.root, "src", "lib");
      const serverOpt = options.server ?? true;

      // Extract backendSecret from the server config object (SvelteKit-only option).
      const serverConfig: JazzServerOptions =
        typeof serverOpt === "object" && serverOpt !== null ? serverOpt : {};
      const backendSecret = serverConfig.backendSecret ?? process.env.BACKEND_SECRET;

      // Pre-resolve jwksUrl from Vite's configured host/port when starting a
      // local server. Precedence: explicit option → APP_ORIGIN env → Vite config
      // → localhost:5173. Known limitation: Vite auto-port-increment means the
      // configured port may differ from the actual one; set APP_ORIGIN explicitly
      // to work around that.
      const resolvedServer = resolveServerWithJwks(serverOpt, viteServer);

      let managed;
      try {
        managed = await runtime.initialize({
          server: resolvedServer,
          schemaDir,
          envDir: viteServer.config.root,
          adminSecret: options.adminSecret,
          appId: options.appId,
          telemetry: options.telemetry,
          backendSecret,
          onSchemaError: (error) => {
            viteServer.ws.send({
              type: "error",
              err: {
                message: `${LOG_PREFIX} schema push failed: ${error.message}`,
                stack: error.stack,
              },
            });
          },
          onSchemaPush: () => {
            viteServer.ws.send({ type: "full-reload" });
          },
        });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        viteServer.ws.send({
          type: "error",
          err: {
            message: `${LOG_PREFIX} initialization failed: ${message}`,
            stack: error instanceof Error ? error.stack : undefined,
          },
        });
        throw error;
      }

      viteServer.config.env ??= {};
      viteServer.config.env.PUBLIC_JAZZ_APP_ID = managed.appId;
      viteServer.config.env.PUBLIC_JAZZ_SERVER_URL = managed.serverUrl;
      if (managed.telemetryCollectorUrl) {
        viteServer.config.env.PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL = managed.telemetryCollectorUrl;
      }

      if (wasColdStart && managed.appId) {
        console.log(
          `${LOG_PREFIX} initial appId allocated, restarting dev server so SvelteKit's $env captures it`,
        );
        void viteServer.restart?.();
      }
    },
  };
}

export async function __resetJazzSvelteKitPluginForTests(): Promise<void> {
  await runtime.resetForTests();
}

function resolveServerWithJwks(
  serverOpt: JazzPluginOptions["server"] | true,
  viteServer: ViteDevServer,
): BaseJazzPluginOptions["server"] {
  if (serverOpt === false || typeof serverOpt === "string") return serverOpt;

  const serverConfig: JazzServerOptions =
    typeof serverOpt === "object" && serverOpt !== null ? serverOpt : {};

  if (serverConfig.jwksUrl !== undefined) return serverConfig as BaseJazzServerOptions;

  const appOrigin = process.env.APP_ORIGIN ?? resolveViteOrigin(viteServer);
  return {
    ...serverConfig,
    jwksUrl: `${appOrigin}/api/auth/jwks`,
  } as BaseJazzServerOptions;
}
