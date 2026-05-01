import { mkdirSync, writeFileSync } from "node:fs";
import { defineConfig } from "vitest/config";
import wasm from "vite-plugin-wasm";
import topLevelAwait from "vite-plugin-top-level-await";
import { resolve } from "node:path";
import { playwright } from "@vitest/browser-playwright";
import {
  blockTestingServerNetwork,
  debugTestingServerNetwork,
  testingServerInfo,
  testingServerJwtForUser,
  unblockTestingServerNetwork,
} from "./tests/browser/testing-server-node.js";
import {
  closeRemoteBrowserDb,
  createRemoteBrowserDb,
  waitForRemoteBrowserDbTitle,
} from "./tests/browser/remote-browser-db-node.js";
import {
  REALISTIC_BROWSER_BENCH_TEST,
  shouldExcludeRealisticBrowserBench,
} from "./src/browser-benchmark-mode.js";

const realisticBrowserScenarios = process.env.JAZZ_REALISTIC_BROWSER_SCENARIOS ?? "";
const realisticBrowserRunId = process.env.JAZZ_REALISTIC_BROWSER_RUN_ID ?? "";
const realisticBrowserLimitOverrides =
  process.env.JAZZ_REALISTIC_BROWSER_LIMIT_OVERRIDES_JSON ?? "";
const abstractBench = process.env.JAZZ_ABSTRACT_BENCH ?? "";
const excludeRealisticBrowserBench = shouldExcludeRealisticBrowserBench();
const realisticBrowserBenchReportDir = resolve(__dirname, ".vitest-browser-bench");

export default defineConfig({
  define: {
    __JAZZ_ABSTRACT_BENCH__: JSON.stringify(abstractBench),
    __JAZZ_REALISTIC_BROWSER_SCENARIOS__: JSON.stringify(realisticBrowserScenarios),
    __JAZZ_REALISTIC_BROWSER_RUN_ID__: JSON.stringify(realisticBrowserRunId),
    __JAZZ_REALISTIC_BROWSER_LIMIT_OVERRIDES_JSON__: JSON.stringify(realisticBrowserLimitOverrides),
  },
  plugins: [wasm(), topLevelAwait()],
  server: {
    headers: {
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
    },
    fs: {
      allow: [resolve(__dirname, "../..")],
    },
  },
  optimizeDeps: {
    include: ["react/jsx-dev-runtime", "react/jsx-runtime"],
  },
  resolve: {
    alias: {
      // Needed because jazz-tools browser tests import from source (../../src/),
      // bypassing node_modules resolution. Consumers don't need this.
      "jazz-wasm": resolve(__dirname, "../../crates/jazz-wasm/pkg"),
    },
  },
  worker: {
    plugins: () => [wasm(), topLevelAwait()],
  },
  test: {
    browser: {
      enabled: true,
      provider: playwright(),
      instances: [{ browser: "chromium", headless: true }],
      commands: {
        testingServerInfo: async (_context, appId) => testingServerInfo(appId),
        testingServerBlockNetwork: async ({ context }, serverUrl) =>
          blockTestingServerNetwork(context, serverUrl),
        testingServerUnblockNetwork: async ({ context }, serverUrl) =>
          unblockTestingServerNetwork(context, serverUrl),
        testingServerNetworkDebug: async ({ context }, serverUrl) =>
          debugTestingServerNetwork(context, serverUrl),
        createRemoteBrowserDb: async ({ context, page }, input) =>
          createRemoteBrowserDb(context, page, input),
        waitForRemoteBrowserDbTitle: async (_commandContext, input) =>
          waitForRemoteBrowserDbTitle(input),
        closeRemoteBrowserDb: async (_commandContext, id) => closeRemoteBrowserDb(id),
        testingServerJwtForUser: async (_context, userId, claims, appId) =>
          testingServerJwtForUser(userId, claims, appId),
        writeRealisticBrowserReport: async (_context, runId, report) => {
          mkdirSync(realisticBrowserBenchReportDir, { recursive: true });
          const reportFile = resolve(realisticBrowserBenchReportDir, `${runId}.json`);
          writeFileSync(reportFile, JSON.stringify(report), "utf8");
          return reportFile;
        },
      },
    },
    include: ["tests/browser/**/*.test.ts", "tests/browser/**/*.test.tsx"],
    exclude: excludeRealisticBrowserBench ? [REALISTIC_BROWSER_BENCH_TEST] : [],
    globalSetup: ["tests/browser/global-setup.ts"],
    testTimeout: 30000,
  },
});
