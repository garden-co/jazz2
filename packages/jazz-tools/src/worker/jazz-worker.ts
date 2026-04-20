/**
 * Dedicated Worker entry point for Jazz.
 *
 * Rust owns all dispatch; this module only loads the wasm module and
 * hands off to `runWorker()`. See `crates/jazz-wasm/src/worker_host.rs`.
 *
 * The wasm URL resolution dance is preserved for bundler compatibility:
 * some bundlers can't resolve the default wasm binary URL automatically
 * when loaded inside a dedicated worker, so we fall back to the URL
 * embedded in the worker's own query string.
 */

import { readWorkerRuntimeWasmUrl } from "../runtime/runtime-config.js";

// Vitest browser mode installs __vitest_browser_runner__ on the page global,
// but dedicated workers can miss that setup. Provide the same no-op wrapper
// so transformed worker imports still resolve through the bundler.
const globalRef = globalThis as typeof globalThis & {
  __vitest_browser_runner__?: { wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T> };
};
if (!globalRef.__vitest_browser_runner__) {
  globalRef.__vitest_browser_runner__ = {
    wrapDynamicImport<T>(loader: () => Promise<T>): Promise<T> {
      return loader();
    },
  };
}

declare const self: {
  location?: { origin?: string; href?: string };
};

async function startWorker(): Promise<void> {
  const wasmModule = await import("jazz-wasm");

  // Resolve the wasm binary URL for contexts where the bundler can't wire it up
  // automatically (e.g. OPFS worker served from a custom path).
  const locationHref = (self as any).location?.href as string | undefined;
  const wasmUrl = readWorkerRuntimeWasmUrl(locationHref);

  if (typeof wasmModule.default === "function") {
    if (wasmUrl) {
      await wasmModule.default({ module_or_path: wasmUrl });
    } else {
      await wasmModule.default();
    }
  }

  await wasmModule.runWorker();
}

startWorker();
