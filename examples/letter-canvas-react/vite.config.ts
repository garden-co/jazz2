import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";

const hostedJazzServerUrl = "https://v2.sync.jazz.tools/";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");

  return {
    plugins: [
      react(),
      jazzPlugin({
        adminSecret: env.JAZZ_ADMIN_SECRET,
        appId: env.VITE_JAZZ_APP_ID,
        server: hostedJazzServerUrl,
      }),
    ],
    build: { target: "es2020" },
    worker: { format: "es" },
    optimizeDeps: {
      exclude: ["jazz-wasm"],
    },
  };
});
