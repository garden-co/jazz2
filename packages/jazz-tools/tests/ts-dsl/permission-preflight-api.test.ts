import { describe, expect, it } from "vitest";
import type { Db } from "../../src/runtime/db.js";
import { app } from "./fixtures/basic/schema";

function assertLocalOnlyPermissionPreflightApi(db: Db, todoId: string) {
  const todoInput = {
    title: "Ship permission polish",
    done: false,
    ownerId: null,
    projectId: "project-1",
    tags: [],
  };

  db.canInsert(app.todos, todoInput);
  db.canUpdate(app.todos, todoId, { done: true });

  // @ts-expect-error Permission preflight is local-only and no longer accepts tier/options.
  db.canInsert(app.todos, todoInput, { tier: "global" });

  // @ts-expect-error Permission preflight is local-only and no longer accepts tier/options.
  db.canUpdate(app.todos, todoId, { done: true }, { tier: "edge" });

  // @ts-expect-error Delete permission preflight is intentionally not part of the public API.
  db.canDelete(app.todos, todoId);
}

describe("permission preflight API", () => {
  it("keeps type coverage for the local-only can* surface", () => {
    expect(typeof assertLocalOnlyPermissionPreflightApi).toBe("function");
  });
});
