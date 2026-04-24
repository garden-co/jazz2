import { createRequire } from "node:module";
import { loadEnvFileIntoProcessEnv } from "./env-file.js";
import { buildInspectorLink } from "./inspector-link.js";
import { ManagedDevRuntime } from "./managed-runtime.js";

// jazz-tools contains a dynamic `import("jazz-wasm")` that we intentionally
// keep out of Vite's dep optimizer (wasm-bindgen output breaks esbuild's
// pre-bundling). With pnpm's strict install layout, a bare `jazz-wasm`
// specifier left in a consumer bundle won't resolve at runtime because the
// package isn't hoisted to the project root. We resolve jazz-wasm from this
// module's location — where it IS a direct dependency of jazz-tools — and
// return it as an absolute path, so the plugin can alias the bare specifier
// without forcing the consumer to add jazz-wasm to their own package.json.
export function resolveJazzWasmEntry(): string | null {
  try {
    return createRequire(import.meta.url).resolve("jazz-wasm");
  } catch {
    return null;
  }
}

export interface JazzServerOptions {
  port?: number;
  adminSecret?: string;
  appId?: string;
  allowLocalFirstAuth?: boolean;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  catalogueAuthority?: "local" | "forward";
  catalogueAuthorityUrl?: string;
  catalogueAuthorityAdminSecret?: string;
}

export interface JazzPluginOptions {
  server?: boolean | string | JazzServerOptions;
  adminSecret?: string;
  schemaDir?: string;
  appId?: string;
}

const LOG_PREFIX = "[jazz]";

// Minimal subset of Vite's ViteDevServer — redeclared here to keep this
// module zero-dep on Vite's public types. Exported for sibling plugins
// (./sveltekit.ts) to share, so the shape is maintained in one place.
export interface ViteDevServer {
  config: {
    root: string;
    command: string;
    env?: Record<string, string>;
    server?: {
      port?: number;
      host?: string | boolean;
      https?: unknown;
    };
  };
  httpServer: { once(event: string, cb: () => void): void } | null;
  ws: {
    send(payload: { type: string; err?: { message: string; stack?: string } }): void;
  };
}

export function jazzPlugin(options: JazzPluginOptions = {}) {
  // Vite only surfaces VITE_*-prefixed vars to the client bundle, so the
  // scaffolder writes the two client-facing keys under the VITE_ prefix.
  // Use the same names here so process.env lookups match what's in `.env`.
  const runtime = new ManagedDevRuntime({
    appId: "VITE_JAZZ_APP_ID",
    serverUrl: "VITE_JAZZ_SERVER_URL",
  });

  return {
    name: "jazz",

    config(config: { ssr?: { external?: string[] }; optimizeDeps?: { exclude?: string[] } }) {
      const existingSsr = config.ssr?.external ?? [];
      const existingExclude = config.optimizeDeps?.exclude ?? [];
      const jazzWasmEntry = resolveJazzWasmEntry();
      return {
        worker: { format: "es" as const },
        optimizeDeps: { exclude: Array.from(new Set([...existingExclude, "jazz-wasm"])) },
        ssr: { external: Array.from(new Set([...existingSsr, "jazz-napi"])) },
        ...(jazzWasmEntry
          ? { resolve: { alias: [{ find: /^jazz-wasm$/, replacement: jazzWasmEntry }] } }
          : {}),
      };
    },

    async configureServer(viteServer: ViteDevServer) {
      if (viteServer.config.command !== "serve") return;

      if (options.server === false) {
        return;
      }

      // Vite does not populate process.env from .env for unprefixed
      // keys, so the managed runtime's env-driven cloud-mode check would
      // otherwise never fire. Backfill before reading.
      loadEnvFileIntoProcessEnv(viteServer.config.root);

      const schemaDir = options.schemaDir ?? viteServer.config.root;

      let managed;
      try {
        managed = await runtime.initialize({ ...options, schemaDir });
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        viteServer.ws.send({
          type: "error",
          err: {
            message: `${LOG_PREFIX} initialization failed: ${message}`,
            stack: error instanceof Error ? error.stack : undefined,
          },
        });
        return;
      }

      // Vite only exposes VITE_*-prefixed keys to the client bundle via
      // import.meta.env. process.env gets the same values via the managed
      // runtime's own write below.
      viteServer.config.env ??= {};
      viteServer.config.env.VITE_JAZZ_APP_ID = managed.appId;
      viteServer.config.env.VITE_JAZZ_SERVER_URL = managed.serverUrl;
      console.log(
        `${LOG_PREFIX} Open the inspector: ${buildInspectorLink(
          managed.serverUrl,
          managed.appId,
          managed.adminSecret,
        )}`,
      );

      viteServer.httpServer?.once("close", async () => {
        await runtime.dispose();
      });
    },
  };
}
