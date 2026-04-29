import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["tests/permissions/**/*.test.ts"],
    testTimeout: 30000,
  },
});
