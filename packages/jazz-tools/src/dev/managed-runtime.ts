import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join, relative } from "node:path";
import type { LocalJazzServerHandle } from "./dev-server.js";
import type { JazzPluginOptions, JazzServerOptions } from "./vite.js";

function defaultPersistentDataDir(projectRoot: string): string {
  return join(projectRoot, "node_modules", ".cache", "jazz-dev-server");
}

const LOG_PREFIX = "[jazz]";

function isSchemaPushNetworkError(error: unknown): boolean {
  return error instanceof TypeError && error.message === "fetch failed";
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function warnInitialSchemaPushSkipped(opts: {
  serverUrl: string;
  envServerUrlKey: string | null;
  error: unknown;
}): void {
  const fallback =
    opts.envServerUrlKey === null
      ? "remove the remote server URL option"
      : `comment out ${opts.envServerUrlKey}`;
  console.warn(
    `${LOG_PREFIX} schema auto-push skipped because ${opts.serverUrl} is unreachable (${errorMessage(
      opts.error,
    )}). The dev server will keep using this app and server URL. To use a local Jazz dev server while offline, ${fallback}. Save schema.ts/permissions.ts or restart after reconnecting to publish again.`,
  );
}

function toRelativePath(absPath: string): string {
  const rel = relative(process.cwd(), absPath);
  if (!rel) return ".";
  // fall back to absolute if path escapes cwd
  if (rel.startsWith("..")) return absPath;
  return rel;
}

function printServerStartedBanner(opts: {
  serverUrl: string;
  appId: string;
  dataDir?: string;
  adminSecret?: string;
}): void {
  const useColor = Boolean(process.stdout.isTTY) && process.env.NO_COLOR === undefined;
  const bold = useColor ? "\x1b[1m" : "";
  const brand = useColor ? "\x1b[38;2;20;106;255m" : ""; // #146aff
  const reset = useColor ? "\x1b[0m" : "";
  const art = [
    "     ██╗ █████╗ ███████╗███████╗",
    "     ██║██╔══██╗╚══███╔╝╚══███╔╝",
    "     ██║███████║  ███╔╝   ███╔╝ ",
    "██   ██║██╔══██║ ███╔╝   ███╔╝  ",
    "╚█████╔╝██║  ██║███████╗███████╗",
    " ╚════╝ ╚═╝  ╚═╝╚══════╝╚══════╝",
  ];
  console.log("");
  for (const line of art) {
    console.log(`${bold}${brand}${line}${reset}`);
  }
  console.log("");
  console.log(
    `${bold}Running a local jazz server on ${reset}${bold}${brand}${opts.serverUrl}${reset}`,
  );
  if (opts.dataDir) {
    console.log(`${bold}Data dir:${reset} ${bold}${brand}${toRelativePath(opts.dataDir)}${reset}`);
  }
  console.log(`${bold}App id:${reset}   ${bold}${brand}${opts.appId}${reset}`);
  if (opts.adminSecret) {
    console.log(`${bold}Admin secret:${reset} ${bold}${brand}${opts.adminSecret}${reset}`);
  }
}

export type ManagedRuntime = {
  appId: string;
  serverUrl: string;
  adminSecret: string;
  backendSecret?: string;
};

type ManagedRuntimeConfig = {
  schemaDir: string;
  server: boolean | string | Record<string, unknown>;
  adminSecret: string | null;
  appId: string | null;
  publicServerUrl: string | null;
  publicAppId: string | null;
};

export interface ManagedRuntimeEnvKeys {
  appId: string;
  serverUrl: string;
}

function normalizeServerOption(
  server: JazzPluginOptions["server"],
): ManagedRuntimeConfig["server"] {
  if (server === undefined || server === true) return true;
  if (server === false || typeof server === "string") return server;
  return Object.keys(server)
    .sort()
    .reduce<Record<string, unknown>>((acc, key) => {
      const value = server[key as keyof JazzServerOptions];
      if (value !== undefined) {
        acc[key] = value;
      }
      return acc;
    }, {});
}

async function readEnvAppId(envPath: string, envKey: string): Promise<string | null> {
  try {
    const content = await readFile(envPath, "utf8");
    const match = content.match(new RegExp(`^${envKey}=(.+)$`, "m"));
    return match?.[1]?.trim() ?? null;
  } catch {
    return null;
  }
}

async function persistAppIdToEnv(envPath: string, envKey: string, appId: string): Promise<void> {
  let content = "";
  try {
    content = await readFile(envPath, "utf8");
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
  }
  if (content.includes(`${envKey}=`)) return;
  const line = `${envKey}=${appId}\n`;
  await mkdir(join(envPath, ".."), { recursive: true });
  await writeFile(envPath, content ? content + line : line);
}

export interface InitializeOptions extends JazzPluginOptions {
  backendSecret?: string;
  /** Directory in which to persist the generated app ID to a .env file. Defaults to schemaDir. */
  envDir?: string;
  /** Called when a schema watch push fails after initialisation. Use this to forward errors to e.g. Vite's HMR overlay. */
  onSchemaError?: (error: Error) => void;
  /** Called when the schema watcher successfully pushes an updated schema. Use this to e.g. trigger a Vite full-reload. */
  onSchemaPush?: (hash: string) => void;
}

export class ManagedDevRuntime {
  private initPromise: Promise<ManagedRuntime> | null = null;
  private initConfigSignature: string | null = null;
  private runtime: ManagedRuntime | null = null;
  private runtimeConfigSignature: string | null = null;
  private serverHandle: LocalJazzServerHandle | null = null;
  private watcher: { close: () => void } | null = null;
  private shutdownHooksInstalled = false;
  private cleanupHandler: (() => void) | null = null;

  constructor(private envKeys: ManagedRuntimeEnvKeys) {}

  private getManagedRuntimeConfig(options: JazzPluginOptions): ManagedRuntimeConfig {
    return {
      schemaDir: options.schemaDir ?? process.cwd(),
      server: normalizeServerOption(options.server),
      adminSecret: options.adminSecret ?? null,
      appId: options.appId ?? null,
      publicServerUrl: process.env[this.envKeys.serverUrl] ?? null,
      publicAppId: process.env[this.envKeys.appId] ?? null,
    };
  }

  private serializeConfig(config: ManagedRuntimeConfig): string {
    return JSON.stringify(config);
  }

  private assertCompatible(options: JazzPluginOptions): void {
    const requestedSignature = this.serializeConfig(this.getManagedRuntimeConfig(options));
    const matchesInitial = this.initConfigSignature === requestedSignature;
    const matchesRuntime = this.runtimeConfigSignature === requestedSignature;
    if ((this.runtime || this.initPromise) && !matchesInitial && !matchesRuntime) {
      throw new Error(
        `${LOG_PREFIX} conflicting Jazz dev runtime configuration; call resetForTests() before switching dev options`,
      );
    }
  }

  async dispose(): Promise<void> {
    this.watcher?.close();
    this.watcher = null;
    if (this.serverHandle) {
      await this.serverHandle.stop();
      this.serverHandle = null;
    }
    this.runtime = null;
    this.initPromise = null;
    this.initConfigSignature = null;
    this.runtimeConfigSignature = null;
  }

  private installShutdownHooks(): void {
    if (this.shutdownHooksInstalled) return;

    this.cleanupHandler = () => {
      void this.dispose();
    };

    process.once("SIGINT", this.cleanupHandler);
    process.once("SIGTERM", this.cleanupHandler);
    process.once("exit", this.cleanupHandler);
    this.shutdownHooksInstalled = true;
  }

  async resetForTests(): Promise<void> {
    if (this.cleanupHandler) {
      process.off("SIGINT", this.cleanupHandler);
      process.off("SIGTERM", this.cleanupHandler);
      process.off("exit", this.cleanupHandler);
    }
    this.cleanupHandler = null;
    this.shutdownHooksInstalled = false;
    await this.dispose();
  }

  async initialize(options: InitializeOptions): Promise<ManagedRuntime> {
    this.assertCompatible(options);
    if (this.runtime) return this.runtime;
    if (this.initPromise) return this.initPromise;

    const requestedConfig = this.getManagedRuntimeConfig(options);
    const requestedSignature = this.serializeConfig(requestedConfig);
    this.initConfigSignature = requestedSignature;

    this.initPromise = (async () => {
      const serverOpt = options.server ?? true;
      const schemaDir = requestedConfig.schemaDir;
      const envPath = join(options.envDir ?? schemaDir, ".env");
      let serverUrl: string;
      let adminSecret: string;
      let appId: string;
      let usesExistingServer = false;
      let existingServerEnvKey: string | null = null;

      try {
        if (serverOpt === false) {
          throw new Error(`${LOG_PREFIX} server=false should bypass initialization`);
        }

        if (process.env[this.envKeys.serverUrl]) {
          usesExistingServer = true;
          existingServerEnvKey = this.envKeys.serverUrl;
          serverUrl = process.env[this.envKeys.serverUrl]!;
          adminSecret = options.adminSecret ?? process.env.JAZZ_ADMIN_SECRET ?? "";
          appId = process.env[this.envKeys.appId] ?? options.appId ?? "";
          if (!adminSecret) {
            throw new Error(
              `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
            );
          }
          if (!appId) {
            throw new Error(
              `${LOG_PREFIX} appId is required when connecting to an existing server`,
            );
          }
          console.log(`${LOG_PREFIX} using server from env: ${serverUrl}`);
          console.log(`${LOG_PREFIX} app id: ${appId}`);
        } else if (typeof serverOpt === "string") {
          usesExistingServer = true;
          serverUrl = serverOpt;
          adminSecret = options.adminSecret ?? "";
          appId = options.appId ?? "";
          if (!adminSecret) {
            throw new Error(
              `${LOG_PREFIX} adminSecret is required when connecting to an existing server`,
            );
          }
          if (!appId) {
            throw new Error(
              `${LOG_PREFIX} appId is required when connecting to an existing server`,
            );
          }
          console.log(`${LOG_PREFIX} app id: ${appId}`);
        } else {
          const serverConfig = typeof serverOpt === "object" ? serverOpt : {};
          adminSecret =
            serverConfig.adminSecret ??
            options.adminSecret ??
            `jazz-dev-${randomUUID().slice(0, 8)}`;
          const envAppId = await readEnvAppId(envPath, this.envKeys.appId);
          appId =
            process.env[this.envKeys.appId] ??
            envAppId ??
            serverConfig.appId ??
            options.appId ??
            randomUUID();

          let dataDir = serverConfig.dataDir;
          if (dataDir === undefined && serverConfig.inMemory !== true) {
            const projectRoot = options.envDir ?? schemaDir;
            dataDir = defaultPersistentDataDir(projectRoot);
            await mkdir(dataDir, { recursive: true });
          }

          const { startLocalJazzServer } = await import("./dev-server.js");
          this.serverHandle = await startLocalJazzServer({
            appId,
            port: serverConfig.port ?? 0,
            adminSecret,
            backendSecret: options.backendSecret,
            allowLocalFirstAuth: serverConfig.allowLocalFirstAuth,
            dataDir,
            inMemory: serverConfig.inMemory,
            jwksUrl: serverConfig.jwksUrl,
            catalogueAuthority: serverConfig.catalogueAuthority,
            catalogueAuthorityUrl: serverConfig.catalogueAuthorityUrl,
            catalogueAuthorityAdminSecret: serverConfig.catalogueAuthorityAdminSecret,
          });

          serverUrl = this.serverHandle.url;
          printServerStartedBanner({
            serverUrl,
            appId,
            dataDir: this.serverHandle.dataDir,
            adminSecret,
          });
        }

        await persistAppIdToEnv(envPath, this.envKeys.appId, appId);

        const { pushSchemaCatalogue } = await import("./dev-server.js");
        try {
          await pushSchemaCatalogue({ serverUrl, appId, adminSecret, schemaDir });
          console.log(`${LOG_PREFIX} schema published`);
        } catch (error) {
          if (usesExistingServer && isSchemaPushNetworkError(error)) {
            warnInitialSchemaPushSkipped({
              serverUrl,
              envServerUrlKey: existingServerEnvKey,
              error,
            });
          } else {
            throw error;
          }
        }

        const { watchSchema } = await import("./schema-watcher.js");
        this.watcher = watchSchema({
          schemaDir,
          serverUrl,
          appId,
          adminSecret,
          onPush: (hash) => {
            console.log(`${LOG_PREFIX} schema updated (${hash.slice(0, 12)})`);
            options.onSchemaPush?.(hash);
          },
          onError: (error) => {
            console.error(`${LOG_PREFIX} schema push failed:`, error.message);
            options.onSchemaError?.(error);
          },
        });

        this.installShutdownHooks();

        const backendSecret = this.serverHandle?.backendSecret;

        process.env[this.envKeys.appId] = appId;
        process.env[this.envKeys.serverUrl] = serverUrl;
        if (backendSecret) {
          process.env.BACKEND_SECRET = backendSecret;
        }

        this.runtime = { appId, serverUrl, adminSecret, backendSecret };
        this.runtimeConfigSignature = this.serializeConfig(this.getManagedRuntimeConfig(options));
        return this.runtime;
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`${LOG_PREFIX} initialization failed:`, message);
        await this.dispose();
        throw error;
      }
    })();

    try {
      return await this.initPromise;
    } catch (error) {
      this.initPromise = null;
      this.initConfigSignature = null;
      throw error;
    }
  }
}
