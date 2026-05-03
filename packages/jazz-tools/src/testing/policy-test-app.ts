import { createJazzContext, Db, Session, type JazzContext } from "../backend/index.js";
import type { WasmSchema } from "../drivers/types.js";
import type { CompiledPermissions } from "../permissions/index.js";
import {
  fetchPermissionsHead,
  publishStoredPermissions,
  publishStoredSchema,
} from "../runtime/schema-fetch.js";
import { startLocalJazzServer, type LocalJazzServerHandle } from "./local-jazz-server.js";

type PolicyTestAppSchema = { wasmSchema: WasmSchema };

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

/**
 * Create a new policy test app.
 * This will start a local Jazz server and push the schema catalogue to it.
 * @returns a {@link PolicyTestApp} instance that can be used to seed the database and validate policy checks.
 * @param app - The Jazz app created with `defineApp(...)`
 * @param permissions - The permissions created with `definePermissions(...)`
 * @param expectFn - The `expect` function to use for assertions (e.g. `expect` from `vitest`)
 */
export async function createPolicyTestApp(
  app: PolicyTestAppSchema,
  permissions: CompiledPermissions,
  expectFn: Function,
): Promise<PolicyTestApp> {
  const backendSecret = `backend-secret`;
  const adminSecret = `admin-secret`;
  const server = await startLocalJazzServer({
    backendSecret,
    adminSecret,
  });

  const { hash: schemaHash } = await publishStoredSchema(server.url, {
    appId: server.appId,
    adminSecret,
    schema: app.wasmSchema,
  });
  const { head } = await fetchPermissionsHead(server.url, {
    appId: server.appId,
    adminSecret,
  });
  await publishStoredPermissions(server.url, {
    appId: server.appId,
    adminSecret,
    schemaHash,
    permissions,
    expectedParentBundleObjectId: head?.bundleObjectId ?? null,
  });

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
