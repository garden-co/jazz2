import type { ReactNode } from "react";
import type { Session } from "../runtime/context.js";
import type { Db, DbConfig } from "../runtime/db.js";
import {
  JazzProvider as CoreJazzProvider,
  useDb as useCoreDb,
  useJazzClient as useCoreJazzClient,
  useSession,
} from "../react-core/provider.js";
import { createJazzClient, type JazzClient as CreatedJazzClient } from "./create-jazz-client.js";

// In dev builds, pull in a generated module that withJazz (next.ts/vite.ts/...)
// rewrites on every schema push. The bundler tracks this as a dependency of the
// React provider, so any push to the file forces a full reload of the host app
// without each framework plugin needing its own dev-server WebSocket wiring.
if (process.env.NODE_ENV === "development" && typeof window !== "undefined") {
  import("jazz-tools/_dev/schema-hash").catch(() => {});
}

export { JazzClientProvider, type JazzClientProviderProps } from "../react-core/provider.js";

interface JazzClientContextValue {
  db: Db;
  manager: CreatedJazzClient["manager"];
  session: Session | null;
  shutdown: CreatedJazzClient["shutdown"];
}

export type JazzProviderProps = {
  config: DbConfig;
  fallback?: ReactNode;
  children: ReactNode;
  onJWTExpired?: () => Promise<string | null | undefined>;
};

export function JazzProvider({ config, fallback, children, onJWTExpired }: JazzProviderProps) {
  return (
    <CoreJazzProvider
      config={config}
      fallback={fallback}
      createJazzClient={createJazzClient}
      onJWTExpired={onJWTExpired}
    >
      {children}
    </CoreJazzProvider>
  );
}

export function useJazzClient(): JazzClientContextValue {
  return useCoreJazzClient() as JazzClientContextValue;
}

/**
 * Get a Jazz {@link Db} instance that can be used to read and write data.
 */
export function useDb(): Db {
  return useCoreDb<Db>();
}

export { useSession };

export type { JazzClientContextValue };
