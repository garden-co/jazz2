import { createRequire } from "node:module";
import { copyFile, mkdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { buildInspectorLink } from "./inspector-link.js";
import { ManagedDevRuntime } from "./managed-runtime.js";
import type { JazzPluginOptions, JazzServerOptions } from "./vite.js";

export interface NextJazzServerOptions extends JazzServerOptions {
  backendSecret?: string;
}

export interface NextConfigLike {
  env?: Record<string, string | undefined>;
  serverExternalPackages?: string[];
  [key: string]: unknown;
}

interface NextConfigContextLike {
  defaultConfig: NextConfigLike;
}

type NextConfigFactory = (
  phase: string,
  context: NextConfigContextLike,
) => NextConfigLike | Promise<NextConfigLike>;

type NextConfigInput = NextConfigLike | NextConfigFactory;

export interface NextJazzPluginOptions extends JazzPluginOptions {
  server?: boolean | string | NextJazzServerOptions;
  appRoot?: string;
}

const DEVELOPMENT_PHASE = "phase-development-server";
const PRODUCTION_BUILD_PHASE = "phase-production-build";
const PUBLIC_APP_ID_ENV = "NEXT_PUBLIC_JAZZ_APP_ID";
const PUBLIC_SERVER_URL_ENV = "NEXT_PUBLIC_JAZZ_SERVER_URL";
const PUBLIC_TELEMETRY_COLLECTOR_URL_ENV = "NEXT_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL";
const PUBLIC_WASM_URL_ENV = "NEXT_PUBLIC_JAZZ_WASM_URL";
const PUBLIC_WASM_SUBPATH = "_jazz/jazz_wasm_bg.wasm";

function buildPublicWasmUrl(basePath: unknown): string {
  if (typeof basePath !== "string" || basePath.length === 0) {
    return `/${PUBLIC_WASM_SUBPATH}`;
  }
  const trimmed = basePath.replace(/\/+$/, "");
  const prefix = trimmed.startsWith("/") ? trimmed : `/${trimmed}`;
  return `${prefix}/${PUBLIC_WASM_SUBPATH}`;
}

const runtime = new ManagedDevRuntime({
  appId: PUBLIC_APP_ID_ENV,
  serverUrl: PUBLIC_SERVER_URL_ENV,
  telemetryCollectorUrl: PUBLIC_TELEMETRY_COLLECTOR_URL_ENV,
});

async function copyWasmToPublic(appRoot: string): Promise<void> {
  const require = createRequire(import.meta.url);
  const pkgJsonPath = require.resolve("jazz-wasm/package.json");
  const wasmSource = join(dirname(pkgJsonPath), "pkg", "jazz_wasm_bg.wasm");
  const wasmDest = join(appRoot, "public", PUBLIC_WASM_SUBPATH);
  await mkdir(dirname(wasmDest), { recursive: true });
  await copyFile(wasmSource, wasmDest);
}

function mergeServerExternalPackages(existing: string[] | undefined): string[] {
  return Array.from(new Set([...(existing ?? []), "jazz-tools", "jazz-napi"]));
}

async function resolveConfig(
  input: NextConfigInput | undefined,
  phase: string,
  context: NextConfigContextLike,
): Promise<NextConfigLike> {
  if (!input) return {};
  if (typeof input === "function") {
    return (await input(phase, context)) ?? {};
  }
  return input;
}

export function withJazz(
  nextConfig?: NextConfigInput,
  options: NextJazzPluginOptions = {},
): NextConfigFactory {
  let hasLoggedInspectorLink = false;

  return async (phase, context) => {
    const resolved = await resolveConfig(nextConfig, phase, context);
    const merged: NextConfigLike = {
      ...resolved,
      serverExternalPackages: mergeServerExternalPackages(resolved.serverExternalPackages),
    };

    // Copy jazz-wasm bytes into the host app's public/ dir so they're served
    // at a stable origin-root URL. Works around bundlers (Turbopack) that
    // don't transform wasm-bindgen's `new URL('*.wasm', import.meta.url)`
    // pattern inside worker chunks. Runs in dev and production-build so the
    // asset is present in the built output.
    const copyAndAdvertiseWasm = phase === DEVELOPMENT_PHASE || phase === PRODUCTION_BUILD_PHASE;
    if (copyAndAdvertiseWasm) {
      await copyWasmToPublic(options.appRoot ?? process.cwd());
    }
    const mergedWithWasmEnv: NextConfigLike = copyAndAdvertiseWasm
      ? {
          ...merged,
          env: {
            ...merged.env,
            [PUBLIC_WASM_URL_ENV]: buildPublicWasmUrl(merged.basePath),
          },
        }
      : merged;

    // Everything below is dev-only: managed server, APP_ID/SERVER_URL
    // injection. In production the host app supplies those via its own env.
    if (phase !== DEVELOPMENT_PHASE || options.server === false) {
      return mergedWithWasmEnv;
    }

    const serverOpt = options.server;
    const explicitBackendSecret =
      typeof serverOpt === "object" && serverOpt !== null && "backendSecret" in serverOpt
        ? serverOpt.backendSecret
        : undefined;
    const backendSecret = explicitBackendSecret ?? process.env.BACKEND_SECRET;

    const managed = await runtime.initialize({ ...options, backendSecret });
    if (!hasLoggedInspectorLink) {
      console.log(
        `[jazz] Open the inspector: ${buildInspectorLink(
          managed.serverUrl,
          managed.appId,
          managed.adminSecret,
        )}`,
      );
      hasLoggedInspectorLink = true;
    }

    return {
      ...mergedWithWasmEnv,
      env: {
        ...mergedWithWasmEnv.env,
        [PUBLIC_APP_ID_ENV]: managed.appId,
        [PUBLIC_SERVER_URL_ENV]: managed.serverUrl,
        ...(managed.telemetryCollectorUrl
          ? { [PUBLIC_TELEMETRY_COLLECTOR_URL_ENV]: managed.telemetryCollectorUrl }
          : {}),
        ...(managed.backendSecret ? { BACKEND_SECRET: managed.backendSecret } : {}),
      },
    };
  };
}

export async function __resetJazzNextPluginForTests(): Promise<void> {
  await runtime.resetForTests();
}
