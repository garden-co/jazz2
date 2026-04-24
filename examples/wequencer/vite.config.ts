import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import basicSsl from "@vitejs/plugin-basic-ssl";
import { jazzPlugin } from "jazz-tools/dev/vite";

// Fixed port so the static proxy config and jazzPlugin agree on where the
// Jazz server lives. basicSsl() makes Vite HTTPS so multi-device collaboration
// works (non-localhost origins aren't secure contexts without it), and the
// proxy routes Jazz traffic through Vite to avoid mixed-content blocks.
const JAZZ_PORT = 4200;

export default defineConfig({
  plugins: [svelte(), basicSsl(), jazzPlugin({ server: { port: JAZZ_PORT } })],
  server: {
    proxy: {
      "/apps": { target: `http://127.0.0.1:${JAZZ_PORT}`, ws: true },
      "/sync": `http://127.0.0.1:${JAZZ_PORT}`,
      "/events": `http://127.0.0.1:${JAZZ_PORT}`,
      "/health": `http://127.0.0.1:${JAZZ_PORT}`,
      "/auth": `http://127.0.0.1:${JAZZ_PORT}`,
    },
  },
});
