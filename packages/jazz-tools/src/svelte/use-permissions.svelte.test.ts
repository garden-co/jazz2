import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushSync } from "svelte";
import type { PermissionDecision, TableProxy } from "../runtime/index.js";
import "./test-helpers.svelte.js";

const mocks = vi.hoisted(() => {
  const canInsert = vi.fn();
  const canUpdate = vi.fn();

  return {
    canInsert,
    canUpdate,
    reset() {
      canInsert.mockReset();
      canUpdate.mockReset();
    },
  };
});

vi.mock("./context.svelte.js", () => ({
  getDb: () => ({
    canInsert: mocks.canInsert,
    canUpdate: mocks.canUpdate,
  }),
}));

const { CanInsertPermission, CanUpdatePermission } = await import("./use-permissions.svelte.js");

type TodoRow = {
  id: string;
  title: string;
  done: boolean;
};

type TodoInput = {
  title: string;
  done: boolean;
};

const todos = {
  _table: "todos",
  _schema: {} as TableProxy<TodoRow, TodoInput>["_schema"],
  _rowType: {} as TodoRow,
  _initType: {} as TodoInput,
} satisfies TableProxy<TodoRow, TodoInput>;

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function settle() {
  await Promise.resolve();
  flushSync();
}

describe("svelte/permission helpers", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("returns unknown until canInsert resolves, then returns the decision", async () => {
    const decision = deferred<PermissionDecision>();
    mocks.canInsert.mockReturnValue(decision.promise);
    const message = { title: "Ship auth chat", done: false };
    let permission!: InstanceType<typeof CanInsertPermission<TodoRow, TodoInput>>;

    const cleanup = $effect.root(() => {
      permission = new CanInsertPermission(todos, message);
    });
    await settle();

    expect(permission.current).toBe("unknown");
    expect(mocks.canInsert).toHaveBeenCalledWith(todos, message);

    decision.resolve(true);
    await decision.promise;
    await settle();

    expect(permission.current).toBe(true);
    cleanup();
  });

  it("does not call canInsert until data is available", async () => {
    mocks.canInsert.mockResolvedValue(true);
    let permission!: InstanceType<typeof CanInsertPermission<TodoRow, TodoInput>>;

    const cleanup = $effect.root(() => {
      permission = new CanInsertPermission(todos, undefined);
    });
    await settle();

    expect(permission.current).toBe("unknown");
    expect(mocks.canInsert).not.toHaveBeenCalled();
    cleanup();
  });

  it("rechecks canInsert when a getter produces a new object", async () => {
    mocks.canInsert.mockResolvedValue(true);
    let tick = $state(0);
    let permission!: InstanceType<typeof CanInsertPermission<TodoRow, TodoInput>>;

    const cleanup = $effect.root(() => {
      permission = new CanInsertPermission(todos, () => {
        tick;
        return { title: "Ship auth chat", done: false };
      });
    });
    await settle();
    await settle();

    expect(permission.current).toBe(true);
    expect(mocks.canInsert).toHaveBeenCalledTimes(1);

    tick += 1;
    await settle();
    await settle();

    expect(permission.current).toBe(true);
    expect(mocks.canInsert).toHaveBeenCalledTimes(2);
    cleanup();
  });

  it("rechecks canInsert when a draft mutates in place", async () => {
    mocks.canInsert.mockResolvedValue(true);
    let draft = $state({ title: "Ship auth chat", done: false });
    let permission!: InstanceType<typeof CanInsertPermission<TodoRow, TodoInput>>;

    const cleanup = $effect.root(() => {
      permission = new CanInsertPermission(todos, () => draft);
    });
    await settle();
    await settle();

    expect(permission.current).toBe(true);
    expect(mocks.canInsert).toHaveBeenCalledTimes(1);

    draft.done = true;
    await settle();
    await settle();

    expect(permission.current).toBe(true);
    expect(mocks.canInsert).toHaveBeenCalledTimes(2);
    expect(mocks.canInsert).toHaveBeenLastCalledWith(todos, draft);
    cleanup();
  });

  it("returns unknown until canUpdate resolves, then returns the decision", async () => {
    const decision = deferred<PermissionDecision>();
    mocks.canUpdate.mockReturnValue(decision.promise);
    const patch = { done: true };
    let permission!: InstanceType<typeof CanUpdatePermission<TodoRow, TodoInput>>;

    const cleanup = $effect.root(() => {
      permission = new CanUpdatePermission(todos, "todo-1", patch);
    });
    await settle();

    expect(permission.current).toBe("unknown");
    expect(mocks.canUpdate).toHaveBeenCalledWith(todos, "todo-1", patch);

    decision.resolve(false);
    await decision.promise;
    await settle();

    expect(permission.current).toBe(false);
    cleanup();
  });

  it("rechecks canUpdate when a patch mutates in place", async () => {
    mocks.canUpdate.mockResolvedValue(true);
    let patch = $state({ done: false });
    let permission!: InstanceType<typeof CanUpdatePermission<TodoRow, TodoInput>>;

    const cleanup = $effect.root(() => {
      permission = new CanUpdatePermission(todos, "todo-1", () => patch);
    });
    await settle();
    await settle();

    expect(permission.current).toBe(true);
    expect(mocks.canUpdate).toHaveBeenCalledTimes(1);

    patch.done = true;
    await settle();
    await settle();

    expect(permission.current).toBe(true);
    expect(mocks.canUpdate).toHaveBeenCalledTimes(2);
    expect(mocks.canUpdate).toHaveBeenLastCalledWith(todos, "todo-1", patch);
    cleanup();
  });
});
