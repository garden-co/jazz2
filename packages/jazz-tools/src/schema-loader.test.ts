import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { loadCompiledSchema } from "./schema-loader.js";

const WITH_DEFAULTS_DIR = fileURLToPath(
  new URL("./testing/fixtures/with-defaults", import.meta.url),
);

describe("loadCompiledSchema", () => {
  it("preserves column defaults from defineApp (no lossy AST round-trip)", async () => {
    const { app } = (await import("./testing/fixtures/with-defaults/schema.js")) as {
      app: { wasmSchema: unknown };
    };
    const loaded = await loadCompiledSchema(WITH_DEFAULTS_DIR);

    expect(JSON.stringify(loaded.wasmSchema)).toBe(JSON.stringify(app.wasmSchema));

    const doneColumn = loaded.wasmSchema.todos?.columns.find((c) => c.name === "done");
    expect(doneColumn?.default).toEqual({ type: "Boolean", value: false });
  });
});
