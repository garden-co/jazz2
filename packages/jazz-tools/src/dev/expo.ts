import { buildInspectorLink } from "./inspector-link.js";
import { ManagedDevRuntime } from "./managed-runtime.js";
import type { JazzPluginOptions, JazzServerOptions } from "./vite.js";

export type { JazzPluginOptions, JazzServerOptions };

export interface ExpoConfigLike {
  extra?: Record<string, unknown>;
  [key: string]: unknown;
}

const runtime = new ManagedDevRuntime({
  appId: "EXPO_PUBLIC_JAZZ_APP_ID",
  serverUrl: "EXPO_PUBLIC_JAZZ_SERVER_URL",
  telemetryCollectorUrl: "EXPO_PUBLIC_JAZZ_TELEMETRY_COLLECTOR_URL",
});

let hasLoggedInspectorLink = false;

export async function withJazz(
  expoConfig: ExpoConfigLike,
  options: JazzPluginOptions = {},
): Promise<ExpoConfigLike> {
  if (process.env.NODE_ENV === "production" || options.server === false) {
    return expoConfig;
  }

  const managed = await runtime.initialize(options);

  if (!hasLoggedInspectorLink) {
    console.log(
      `[jazz] Open the inspector: ${buildInspectorLink(
        managed.serverUrl,
        managed.appId,
        managed.adminSecret,
      )}`,
    );
    hasLoggedInspectorLink = true;
  }

  return {
    ...expoConfig,
    extra: {
      ...expoConfig.extra,
      jazzAppId: managed.appId,
      jazzServerUrl: managed.serverUrl,
      ...(managed.telemetryCollectorUrl
        ? { jazzTelemetryCollectorUrl: managed.telemetryCollectorUrl }
        : {}),
    },
  };
}

export async function __resetJazzPluginForTests(): Promise<void> {
  hasLoggedInspectorLink = false;
  await runtime.resetForTests();
}
