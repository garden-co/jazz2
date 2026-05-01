import { useState, useEffect, use, Suspense } from "react";
import { JazzProvider, attachDevTools, useJazzClient } from "jazz-tools/react";
import type { DbConfig } from "jazz-tools";
import { BrowserAuthSecretStore } from "jazz-tools";
import { TodoList } from "./TodoList.js";
import { GenerateData } from "./GenerateData.js";
import { app } from "../schema";

const devToolsAttachedClients = new WeakSet<object>();

function DevToolsRegistration() {
  const client = useJazzClient();

  useEffect(() => {
    if (devToolsAttachedClients.has(client as object)) {
      return;
    }

    void attachDevTools(client, app.wasmSchema);
    devToolsAttachedClients.add(client as object);

    if (location.origin.includes("localhost")) {
      Object.defineProperty(window, "jazzClient", {
        value: client,
        writable: true,
      });
    }
  }, [client]);

  return null;
}

function useHash() {
  const [hash, setHash] = useState(location.hash);
  useEffect(() => {
    const onHashChange = () => setHash(location.hash);
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);
  return hash;
}

function Router() {
  const hash = useHash();

  if (hash === "#list") {
    return (
      <>
        <h1>Todos</h1>
        <p>
          <a href="#">Back to Generate</a>
        </p>
        <TodoList />
      </>
    );
  }

  return <GenerateData />;
}

const appId = import.meta.env.VITE_JAZZ_APP_ID;
const serverUrl = import.meta.env.VITE_JAZZ_SERVER_URL;
const telemetryCollectorUrl = import.meta.env.VITE_JAZZ_TELEMETRY_COLLECTOR_URL;

if (!appId) {
  throw new Error("JAZZ_APP_ID is required");
}

if (!serverUrl) {
  throw new Error("JAZZ_SERVER_URL is required");
}

function AppInner() {
  const secret = use(BrowserAuthSecretStore.getOrCreateSecret());
  const config: DbConfig = {
    appId,
    env: import.meta.env.DEV ? "dev" : "prod",
    userBranch: "main",
    devMode: import.meta.env.DEV,
    secret,
    serverUrl,
    telemetryCollectorUrl,
    logLevel: telemetryCollectorUrl ? "debug" : undefined,
  };

  return (
    <JazzProvider config={config} fallback={<p>Loading...</p>}>
      <DevToolsRegistration />
      <Router />
    </JazzProvider>
  );
}

// #region context-setup-react
export function App() {
  return (
    <Suspense fallback={<p>Loading...</p>}>
      <AppInner />
    </Suspense>
  );
}
// #endregion context-setup-react
