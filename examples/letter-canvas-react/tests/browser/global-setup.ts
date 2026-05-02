import { join } from "node:path";
import { TestingServer, pushSchemaCatalogue } from "jazz-tools/testing";
import { TEST_PORT, ADMIN_SECRET, APP_ID } from "./test-constants.js";

export { TEST_PORT, ADMIN_SECRET, APP_ID };

let server: Promise<TestingServer> | null = null;

export async function setup(): Promise<void> {
  if (server) {
    await server;
    return;
  }

  server = TestingServer.start({
    appId: APP_ID,
    port: TEST_PORT,
    adminSecret: ADMIN_SECRET,
  });

  const handle = await server;

  await pushSchemaCatalogue({
    serverUrl: handle.url,
    appId: handle.appId,
    adminSecret: handle.adminSecret,
    schemaDir: join(import.meta.dirname ?? __dirname, "../.."),
  });
}

export async function teardown(): Promise<void> {
  const handle = await server;
  await handle?.stop();
}
