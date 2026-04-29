import { access } from "node:fs/promises";
import { basename, dirname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { createJazzContext, Db, Session, type JazzContext } from "../backend/index.js";
import type { CompiledPermissions } from "../permissions/index.js";
import {
  pushSchemaCatalogue,
  startLocalJazzServer,
  type LocalJazzServerHandle,
} from "./local-jazz-server.js";

/**
 * A test app for permissions tests. Simplifies setting up a test app and provides methods
 * for seeding the database and validating policy checks.
 */
export class PolicyTestApp {
  constructor(
    private readonly expect: Function,
    private readonly app: any,
    private readonly jazzContext: JazzContext,
    private readonly server: LocalJazzServerHandle,
  ) {}

  /**
   * Seed the database with the given callback.
   * The callback is executed in an admin database context.
   */
  seed<T>(callback: (db: Db) => T): T {
    const db = this.jazzContext.asBackend(this.app);
    return callback(db);
  }

  /**
   * Get a database client for the given session.
   */
  as(session: Session): Db {
    return this.jazzContext.forSession(session, this.app);
  }

  /**
   * Assert that the callback does not throw a policy error.
   * TODO: rollback mutations performed as part of the callback (once we support transactions).
   */
  expectAllowed(callback: () => unknown): void {
    this.expect(callback).not.toThrow();
  }

  /**
   * Assert that the callback throws a policy error.
   * TODO: rollback mutations performed as part of the callback (once we support transactions).
   */
  expectDenied(callback: () => unknown): void {
    this.expect(callback).toThrow('WriteError("policy denied');
  }

  /**
   * Shutdown the test app. This will stop the local Jazz client and server.
   */
  async shutdown(): Promise<void> {
    await this.jazzContext.shutdown();
    await this.server.stop();
  }
}

async function pathExists(path: string): Promise<boolean> {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function resolvePolicyTestSchemaPaths(schemaDir: string): Promise<{
  catalogueDir: string;
  appModulePath: string;
  permissionsModulePath?: string;
}> {
  const directAppModule = join(schemaDir, "app.js");
  if (await pathExists(directAppModule)) {
    return {
      catalogueDir: schemaDir,
      appModulePath: directAppModule,
      permissionsModulePath: (await pathExists(join(schemaDir, "permissions.js")))
        ? join(schemaDir, "permissions.js")
        : undefined,
    };
  }

  for (const extension of ["ts", "js"]) {
    const directRootSchema = join(schemaDir, `schema.${extension}`);
    if (await pathExists(directRootSchema)) {
      const permissionsModulePath = await findPermissionsModulePath(schemaDir);
      return {
        catalogueDir: schemaDir,
        appModulePath: directRootSchema,
        permissionsModulePath,
      };
    }
  }

  if (basename(schemaDir) === "schema") {
    const appRoot = dirname(schemaDir);
    for (const extension of ["ts", "js"]) {
      const parentRootSchema = join(appRoot, `schema.${extension}`);
      if (await pathExists(parentRootSchema)) {
        const permissionsModulePath = await findPermissionsModulePath(appRoot);
        return {
          catalogueDir: appRoot,
          appModulePath: parentRootSchema,
          permissionsModulePath,
        };
      }
    }
  }

  throw new Error(
    `Could not find a schema app near ${schemaDir}. Expected app.js, schema.ts, or schema.js.`,
  );
}

async function findPermissionsModulePath(rootDir: string): Promise<string | undefined> {
  for (const extension of ["ts", "js"]) {
    const candidate = join(rootDir, `permissions.${extension}`);
    if (await pathExists(candidate)) {
      return candidate;
    }
  }

  return undefined;
}

/**
 * Create a new policy test app.
 * This will start a local Jazz server and push the schema catalogue to it.
 * Returns a PolicyTestApp instance that can be used to seed the database and validate policy checks.
 * @param schemaDir - The directory containing the Jazz schema and permissions
 * @param expectFn - The expect function to use for assertions
 */
export async function createPolicyTestApp(
  schemaDir: string,
  expectFn: Function,
): Promise<PolicyTestApp> {
  const backendSecret = `backend-secret`;
  const adminSecret = `admin-secret`;
  const resolvedPaths = await resolvePolicyTestSchemaPaths(schemaDir);
  const server = await startLocalJazzServer({
    backendSecret,
    adminSecret,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: server.appId,
    adminSecret,
    schemaDir: resolvedPaths.catalogueDir,
    env: "test",
    userBranch: "main",
  });

  const appModule = await import(pathToFileURL(resolvedPaths.appModulePath).href);
  const app = appModule.default ?? appModule.app ?? appModule;
  if (!app) {
    throw new Error(`No schema app module found near ${schemaDir}`);
  }
  const permissionsModule = resolvedPaths.permissionsModulePath
    ? await import(pathToFileURL(resolvedPaths.permissionsModulePath).href)
    : null;
  const permissions = (permissionsModule?.default ?? permissionsModule?.permissions) as
    | CompiledPermissions
    | undefined;
  if (!permissions) {
    throw new Error(`No permissions module found near ${resolvedPaths.appModulePath}`);
  }
  const jazzContext = createJazzContext({
    appId: server.appId,
    app,
    permissions,
    driver: { type: "memory" },
    serverUrl: server.url,
    backendSecret,
    env: "test",
    userBranch: "main",
  });

  return new PolicyTestApp(expectFn, app, jazzContext, server);
}
