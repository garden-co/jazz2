import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { join } from "node:path";
import { createTempRootTracker, todoSchema } from "./test-helpers.js";
import * as devServer from "./dev-server.js";
import * as schemaWatcher from "./schema-watcher.js";
import { ManagedDevRuntime } from "./managed-runtime.js";
import { writeFile } from "node:fs/promises";

const tempRoots = createTempRootTracker();

const originalJazzAppId = process.env.VITE_JAZZ_APP_ID;
const originalJazzServerUrl = process.env.VITE_JAZZ_SERVER_URL;
const originalAdminSecret = process.env.JAZZ_ADMIN_SECRET;

function makeRuntime(): ManagedDevRuntime {
  return new ManagedDevRuntime({
    appId: "VITE_JAZZ_APP_ID",
    serverUrl: "VITE_JAZZ_SERVER_URL",
  });
}

function makeFetchFailedError(code: string): TypeError & { cause?: unknown } {
  const error = new TypeError("fetch failed") as TypeError & { cause?: unknown };
  error.cause = Object.assign(new Error(`getaddrinfo ${code} v2.sync.jazz.tools`), {
    code,
    hostname: "v2.sync.jazz.tools",
  });
  return error;
}

beforeEach(() => {
  delete process.env.VITE_JAZZ_APP_ID;
  delete process.env.VITE_JAZZ_SERVER_URL;
  delete process.env.JAZZ_ADMIN_SECRET;
});

afterEach(async () => {
  await tempRoots.cleanup();
  vi.restoreAllMocks();

  if (originalJazzAppId === undefined) {
    delete process.env.VITE_JAZZ_APP_ID;
  } else {
    process.env.VITE_JAZZ_APP_ID = originalJazzAppId;
  }

  if (originalJazzServerUrl === undefined) {
    delete process.env.VITE_JAZZ_SERVER_URL;
  } else {
    process.env.VITE_JAZZ_SERVER_URL = originalJazzServerUrl;
  }

  if (originalAdminSecret === undefined) {
    delete process.env.JAZZ_ADMIN_SECRET;
  } else {
    process.env.JAZZ_ADMIN_SECRET = originalAdminSecret;
  }
});

describe("ManagedDevRuntime", () => {
  it("keeps env-driven Cloud startup alive when the initial schema push cannot reach the server", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-offline-cloud-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    process.env.VITE_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000777";
    process.env.VITE_JAZZ_SERVER_URL = "https://v2.sync.jazz.tools/";
    process.env.JAZZ_ADMIN_SECRET = "cloud-admin-secret";

    const startLocalJazzServer = vi.spyOn(devServer, "startLocalJazzServer");
    vi.spyOn(devServer, "pushSchemaCatalogue").mockRejectedValue(makeFetchFailedError("ENOTFOUND"));
    const watchSchema = vi.spyOn(schemaWatcher, "watchSchema").mockReturnValue({
      close: vi.fn(),
    });
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});

    const runtime = makeRuntime();
    const managed = await runtime.initialize({ schemaDir });

    expect(managed).toMatchObject({
      appId: "00000000-0000-0000-0000-000000000777",
      serverUrl: "https://v2.sync.jazz.tools/",
      adminSecret: "cloud-admin-secret",
    });
    expect(startLocalJazzServer).not.toHaveBeenCalled();
    expect(watchSchema).toHaveBeenCalledWith(
      expect.objectContaining({
        appId: "00000000-0000-0000-0000-000000000777",
        serverUrl: "https://v2.sync.jazz.tools/",
        adminSecret: "cloud-admin-secret",
        schemaDir,
      }),
    );
    expect(warn).toHaveBeenCalledWith(
      expect.stringContaining(
        "schema auto-push skipped because https://v2.sync.jazz.tools/ is unreachable",
      ),
    );
    expect(warn).toHaveBeenCalledWith(expect.stringContaining("comment out VITE_JAZZ_SERVER_URL"));

    await runtime.dispose();
  });

  it("still fails env-driven startup when the initial schema push reaches the server and is rejected", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-cloud-rejected-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    process.env.VITE_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000888";
    process.env.VITE_JAZZ_SERVER_URL = "https://v2.sync.jazz.tools/";
    process.env.JAZZ_ADMIN_SECRET = "cloud-admin-secret";

    vi.spyOn(devServer, "pushSchemaCatalogue").mockRejectedValue(
      new Error("Schema publish failed: 401 Unauthorized"),
    );
    vi.spyOn(console, "error").mockImplementation(() => {});

    await expect(makeRuntime().initialize({ schemaDir })).rejects.toThrow(
      "Schema publish failed: 401 Unauthorized",
    );
  });

  it("does not skip non-fetch errors just because their message contains a network error code", async () => {
    const schemaDir = await tempRoots.create("jazz-managed-cloud-non-fetch-error-");
    await writeFile(join(schemaDir, "schema.ts"), todoSchema());

    process.env.VITE_JAZZ_APP_ID = "00000000-0000-0000-0000-000000000999";
    process.env.VITE_JAZZ_SERVER_URL = "https://v2.sync.jazz.tools/";
    process.env.JAZZ_ADMIN_SECRET = "cloud-admin-secret";

    vi.spyOn(devServer, "pushSchemaCatalogue").mockRejectedValue(
      new Error("getaddrinfo ENOTFOUND v2.sync.jazz.tools"),
    );
    vi.spyOn(console, "error").mockImplementation(() => {});

    await expect(makeRuntime().initialize({ schemaDir })).rejects.toThrow(
      "getaddrinfo ENOTFOUND v2.sync.jazz.tools",
    );
  });
});
