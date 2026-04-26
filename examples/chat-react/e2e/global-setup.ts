import { join } from "node:path";
import type { FullConfig } from "@playwright/test";
import { startLocalJazzServer, pushSchemaCatalogue } from "jazz-tools/testing";

const APP_ID = "00000000-0000-0000-0000-000000000101";
const ADMIN_SECRET = "chat-react-e2e-admin-secret";
const SERVER_PORT = 5184;

async function globalSetup(_config: FullConfig): Promise<() => Promise<void>> {
  const server = await startLocalJazzServer({
    appId: APP_ID,
    port: SERVER_PORT,
    adminSecret: ADMIN_SECRET,
    allowLocalFirstAuth: true,
    inMemory: true,
  });

  await pushSchemaCatalogue({
    serverUrl: server.url,
    appId: APP_ID,
    adminSecret: ADMIN_SECRET,
    schemaDir: join(import.meta.dirname ?? __dirname, ".."),
  });

  return async () => {
    await server.stop();
  };
}

export default globalSetup;
