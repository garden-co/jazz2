"server-only";

import { app as schemaApp } from "../schema";
import permissions from "../permissions";

// This is a workaround to resolve correctly NAPI modules in the monorepo
// Real-world apps should just `import { createJazzContext } from "jazz-tools/backend"`
import { createRequire as createRequireFromModule } from "node:module";
const createRequire =
  process.getBuiltinModule?.("module")?.createRequire ?? createRequireFromModule;
const nodeRequire = createRequire(import.meta.url);
const { createJazzContext } = nodeRequire(
  "jazz-tools/backend",
) as typeof import("jazz-tools/backend");

const context = createJazzContext({
  appId: process.env.NEXT_PUBLIC_JAZZ_APP_ID!,
  app: schemaApp,
  permissions,
  driver: { type: "memory" },
  serverUrl: process.env.NEXT_PUBLIC_JAZZ_SERVER_URL!,
  backendSecret: process.env.BACKEND_SECRET!,
});

export const db = context.asBackend();
