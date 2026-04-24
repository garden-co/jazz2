import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { jazzPlugin } from "jazz-tools/dev/vite";
import { WORKOS_CLIENT_ID } from "./constants.js";

export default defineConfig({
  plugins: [
    react(),
    jazzPlugin({
      server: {
        jwksUrl: `https://api.workos.com/sso/jwks/${WORKOS_CLIENT_ID}`,
      },
    }),
  ],
  server: {
    proxy: {
      "/sync": {
        target: "http://127.0.0.1:1625",
        changeOrigin: true,
      },
      "/events": {
        target: "http://127.0.0.1:1625",
        changeOrigin: true,
      },
      "/auth": {
        target: "http://127.0.0.1:3001",
        changeOrigin: true,
      },
      "/health": {
        target: "http://127.0.0.1:1625",
        changeOrigin: true,
      },
      "/api/auth": {
        target: "http://127.0.0.1:3001",
        changeOrigin: true,
      },
    },
  },
});
