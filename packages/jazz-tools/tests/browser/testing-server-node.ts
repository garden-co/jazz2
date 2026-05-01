import type { BrowserContext, Route } from "playwright";
type JazzNapiTestingServer = import("jazz-napi").TestingServer;

interface StartedTestingServer {
  server: JazzNapiTestingServer;
  appId: string;
  serverUrl: string;
  adminSecret: string;
}

const DEFAULT_TESTING_SERVER_KEY = "__default__";
const testingServerPromises = new Map<string, Promise<StartedTestingServer>>();
const blockedServerRoutes = new WeakMap<BrowserContext, Map<string, (route: Route) => void>>();
const browserContextIds = new WeakMap<BrowserContext, number>();
let nextBrowserContextId = 1;

async function loadTestingServer(): Promise<typeof import("jazz-napi").TestingServer> {
  try {
    const module = await import("jazz-napi");
    return module.TestingServer;
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    throw new Error(
      "Browser tests require the jazz-napi TestingServer host binding. Run `pnpm --filter jazz-napi build` first.\n\n" +
        `Original error: ${message}`,
    );
  }
}

async function startTestingServer(appId?: string): Promise<StartedTestingServer> {
  const TestingServer = await loadTestingServer();
  const server = await TestingServer.start(appId ? { appId } : undefined);
  return {
    server,
    appId: server.appId,
    serverUrl: server.url,
    adminSecret: server.adminSecret,
  };
}

async function getOrStartTestingServer(appId?: string): Promise<StartedTestingServer> {
  const key = appId ?? DEFAULT_TESTING_SERVER_KEY;
  const existing = testingServerPromises.get(key);

  if (!existing) {
    const startedServer = startTestingServer(appId).catch((error) => {
      testingServerPromises.delete(key);
      throw error;
    });
    testingServerPromises.set(key, startedServer);
    return startedServer;
  }

  return existing;
}

export async function testingServerInfo(appId?: string): Promise<{
  appId: string;
  serverUrl: string;
  adminSecret: string;
}> {
  const serverInfo = await getOrStartTestingServer(appId);
  return {
    appId: serverInfo.appId,
    serverUrl: serverInfo.serverUrl,
    adminSecret: serverInfo.adminSecret,
  };
}

export async function testingServerJwtForUser(
  userId: string,
  claims?: Record<string, unknown>,
  appId?: string,
): Promise<string> {
  const { server } = await getOrStartTestingServer(appId);
  return server.jwtForUser(userId, claims);
}

export async function stopTestingServer(): Promise<void> {
  const runningServers = [...testingServerPromises.values()];
  testingServerPromises.clear();

  if (runningServers.length === 0) {
    return;
  }

  for (const runningServer of runningServers) {
    try {
      const { server } = await runningServer;
      await server.stop();
    } catch {
      // Swallow all errors: either startup never produced a server (nothing to stop),
      // or stop() itself failed (nothing recoverable during teardown).
    }
  }
}

function testingServerUrlPattern(serverUrl: string): string {
  return `${serverUrl.replace(/\/+$/, "")}/**`;
}

function getBrowserContextId(context: BrowserContext): number {
  let id = browserContextIds.get(context);
  if (!id) {
    id = nextBrowserContextId++;
    browserContextIds.set(context, id);
  }
  return id;
}

export interface TestingServerNetworkDebugState {
  contextId: number;
  pattern: string;
  blocked: boolean;
  activePatterns: string[];
}

export async function debugTestingServerNetwork(
  context: BrowserContext,
  serverUrl: string,
): Promise<TestingServerNetworkDebugState> {
  const pattern = testingServerUrlPattern(serverUrl);
  const contextRoutes = blockedServerRoutes.get(context);
  return {
    contextId: getBrowserContextId(context),
    pattern,
    blocked: contextRoutes?.has(pattern) ?? false,
    activePatterns: contextRoutes ? [...contextRoutes.keys()] : [],
  };
}

export async function blockTestingServerNetwork(
  context: BrowserContext,
  serverUrl: string,
): Promise<void> {
  const pattern = testingServerUrlPattern(serverUrl);
  const contextId = getBrowserContextId(context);
  let contextRoutes = blockedServerRoutes.get(context);
  if (!contextRoutes) {
    contextRoutes = new Map();
    blockedServerRoutes.set(context, contextRoutes);
  }
  if (contextRoutes.has(pattern)) {
    console.info("[testing-server-network]", {
      action: "block-skip",
      contextId,
      pattern,
      activePatterns: [...contextRoutes.keys()],
    });
    return;
  }

  const handler = (route: Route) => {
    void route.abort("internetdisconnected");
  };
  contextRoutes.set(pattern, handler);
  await context.route(pattern, handler);
  console.info("[testing-server-network]", {
    action: "block",
    contextId,
    pattern,
    activePatterns: [...contextRoutes.keys()],
  });
}

export async function unblockTestingServerNetwork(
  context: BrowserContext,
  serverUrl: string,
): Promise<void> {
  const pattern = testingServerUrlPattern(serverUrl);
  const contextId = getBrowserContextId(context);
  const contextRoutes = blockedServerRoutes.get(context);
  const handler = contextRoutes?.get(pattern);
  if (!handler) {
    console.info("[testing-server-network]", {
      action: "unblock-skip",
      contextId,
      pattern,
      activePatterns: contextRoutes ? [...contextRoutes.keys()] : [],
    });
    return;
  }

  await context.unroute(pattern, handler);
  contextRoutes?.delete(pattern);
  console.info("[testing-server-network]", {
    action: "unblock",
    contextId,
    pattern,
    activePatterns: contextRoutes ? [...contextRoutes.keys()] : [],
  });
}
