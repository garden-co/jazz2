import { createServer as createNetServer } from "node:net";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DevServer } from "jazz-napi";
import { loadCompiledSchema } from "../schema-loader.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../runtime/schema-fetch.js";

const DEFAULT_APP_ID = "00000000-0000-0000-0000-000000000001";
const AUTO_PORT_MIN = 20_000;
const AUTO_PORT_RANGE = 20_000;

const autoAllocatedPorts = new Set<number>();

let nextAutoPort = AUTO_PORT_MIN + Math.floor(Math.random() * AUTO_PORT_RANGE);

export interface StartLocalJazzServerOptions {
  appId?: string;
  port?: number;
  dataDir?: string;
  inMemory?: boolean;
  jwksUrl?: string;
  backendSecret?: string;
  adminSecret?: string;
  allowLocalFirstAuth?: boolean;
  catalogueAuthority?: "local" | "forward";
  catalogueAuthorityUrl?: string;
  catalogueAuthorityAdminSecret?: string;
  telemetryCollectorUrl?: string;
  enableLogs?: boolean;
}

export interface LocalJazzServerHandle {
  appId: string;
  port: number;
  url: string;
  dataDir: string;
  adminSecret?: string;
  backendSecret?: string;
  stop: () => Promise<void>;
}

export interface PushSchemaCatalogueOptions {
  serverUrl: string;
  appId: string;
  adminSecret: string;
  schemaDir: string;
  env?: string;
  userBranch?: string;
  enableLogs?: boolean;
}

async function canBindPort(port: number): Promise<boolean> {
  return await new Promise<boolean>((resolve) => {
    const server = createNetServer();
    server.once("error", () => {
      resolve(false);
    });
    server.listen(port, "127.0.0.1", () => {
      server.close((error) => {
        void error;
        resolve(true);
      });
    });
  });
}

async function allocateAutoPort(): Promise<number> {
  for (let attempts = 0; attempts < AUTO_PORT_RANGE; attempts += 1) {
    const candidate = nextAutoPort;
    nextAutoPort = AUTO_PORT_MIN + ((nextAutoPort - AUTO_PORT_MIN + 1) % AUTO_PORT_RANGE);
    if (autoAllocatedPorts.has(candidate)) {
      continue;
    }
    if (!(await canBindPort(candidate))) {
      continue;
    }
    autoAllocatedPorts.add(candidate);
    return candidate;
  }

  throw new Error("Failed to allocate a local Jazz server port.");
}

async function createOwnedDataDir(): Promise<string> {
  return await mkdtemp(join(tmpdir(), "jazz-dev-server-"));
}

export async function startLocalJazzServer(
  options: StartLocalJazzServerOptions = {},
): Promise<LocalJazzServerHandle> {
  const appId = options.appId ?? DEFAULT_APP_ID;
  const port = options.port ?? (await allocateAutoPort());
  const ownsPort = options.port === undefined;
  const ownsDataDir = options.inMemory !== true && options.dataDir === undefined;
  const dataDir = ownsDataDir ? await createOwnedDataDir() : options.dataDir;

  let server;
  try {
    server = await DevServer.start({
      appId,
      port,
      dataDir,
      inMemory: options.inMemory,
      jwksUrl: options.jwksUrl,
      backendSecret: options.backendSecret,
      adminSecret: options.adminSecret,
      allowLocalFirstAuth: options.allowLocalFirstAuth,
      catalogueAuthority: options.catalogueAuthority,
      catalogueAuthorityUrl: options.catalogueAuthorityUrl,
      catalogueAuthorityAdminSecret: options.catalogueAuthorityAdminSecret,
      telemetryCollectorUrl: options.telemetryCollectorUrl,
    });
  } catch (error) {
    if (ownsPort) {
      autoAllocatedPorts.delete(port);
    }
    if (ownsDataDir && dataDir) {
      await rm(dataDir, { recursive: true, force: true }).catch(() => undefined);
    }
    throw error;
  }

  if (options.enableLogs === true) {
    console.log(`[jazz-server] started on ${server.url}`);
  }

  let stopPromise: Promise<void> | null = null;
  const stop = async () => {
    if (stopPromise) {
      return await stopPromise;
    }

    stopPromise = (async () => {
      try {
        await server.stop();
      } finally {
        if (ownsPort) {
          autoAllocatedPorts.delete(port);
        }
        if (ownsDataDir && dataDir) {
          await rm(dataDir, { recursive: true, force: true }).catch(() => undefined);
        }
      }
    })();

    return await stopPromise;
  };

  return {
    appId: server.appId,
    port: server.port,
    url: server.url,
    dataDir: server.dataDir,
    adminSecret: server.adminSecret ?? undefined,
    backendSecret: server.backendSecret ?? undefined,
    stop,
  };
}

export async function pushSchemaCatalogue(
  options: PushSchemaCatalogueOptions,
): Promise<{ hash: string }> {
  const compiled = await loadCompiledSchema(options.schemaDir);
  const result = await publishStoredSchema(options.serverUrl, {
    appId: options.appId,
    adminSecret: options.adminSecret,
    schema: compiled.wasmSchema,
  });

  if (compiled.permissions) {
    const { head } = await fetchPermissionsHead(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
    });
    await publishStoredPermissions(options.serverUrl, {
      appId: options.appId,
      adminSecret: options.adminSecret,
      schemaHash: result.hash,
      permissions: compiled.permissions,
      expectedParentBundleObjectId: head?.bundleObjectId ?? null,
    });
  }

  if (options.enableLogs === true) {
    console.log(
      `[jazz-schema-push] published ${result.hash} from ${compiled.schemaFile} to ${options.serverUrl}`,
    );
  }

  return { hash: result.hash };
}
