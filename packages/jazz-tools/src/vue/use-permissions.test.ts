import { beforeEach, describe, expect, it, vi } from "vitest";
import { effectScope, nextTick, reactive, ref } from "vue";
import type { PermissionDecision, TableProxy } from "../runtime/index.js";

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

vi.mock("./provider.js", () => ({
  useDb: () => ({
    canInsert: mocks.canInsert,
    canUpdate: mocks.canUpdate,
  }),
}));

import { useCanInsert, useCanUpdate } from "./use-permissions.js";

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

describe("vue/permission composables", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("returns unknown until canInsert resolves, then returns the decision", async () => {
    const decision = deferred<PermissionDecision>();
    mocks.canInsert.mockReturnValue(decision.promise);
    const message = { title: "Ship auth chat", done: false };
    const scope = effectScope();

    const result = scope.run(() => useCanInsert(todos, message))!;

    expect(result.value).toBe("unknown");
    expect(mocks.canInsert).toHaveBeenCalledWith(todos, message);

    decision.resolve(true);
    await decision.promise;
    await nextTick();

    expect(result.value).toBe(true);
    scope.stop();
  });

  it("does not call canInsert until data is available", () => {
    mocks.canInsert.mockResolvedValue(true);
    const scope = effectScope();

    const result = scope.run(() => useCanInsert(todos, undefined))!;

    expect(result.value).toBe("unknown");
    expect(mocks.canInsert).not.toHaveBeenCalled();
    scope.stop();
  });

  it("rechecks canInsert when the input ref receives a new object", async () => {
    mocks.canInsert.mockResolvedValue(true);
    const input = ref({ title: "Ship auth chat", done: false });
    const scope = effectScope();

    const result = scope.run(() => useCanInsert(todos, input))!;
    await nextTick();
    await Promise.resolve();

    expect(result.value).toBe(true);
    expect(mocks.canInsert).toHaveBeenCalledTimes(1);

    input.value = { title: "Ship auth chat", done: false };
    await nextTick();
    await Promise.resolve();

    expect(result.value).toBe(true);
    expect(mocks.canInsert).toHaveBeenCalledTimes(2);
    scope.stop();
  });

  it("rechecks canInsert when a reactive draft mutates in place", async () => {
    mocks.canInsert.mockResolvedValue(true);
    const draft = reactive({ title: "Ship auth chat", done: false });
    const scope = effectScope();

    const result = scope.run(() => useCanInsert(todos, draft))!;
    await nextTick();
    await Promise.resolve();

    expect(result.value).toBe(true);
    expect(mocks.canInsert).toHaveBeenCalledTimes(1);

    draft.done = true;
    await nextTick();
    await Promise.resolve();

    expect(result.value).toBe(true);
    expect(mocks.canInsert).toHaveBeenCalledTimes(2);
    expect(mocks.canInsert).toHaveBeenLastCalledWith(todos, draft);
    scope.stop();
  });

  it("returns unknown until canUpdate resolves, then returns the decision", async () => {
    const decision = deferred<PermissionDecision>();
    mocks.canUpdate.mockReturnValue(decision.promise);
    const patch = { done: true };
    const scope = effectScope();

    const result = scope.run(() => useCanUpdate(todos, "todo-1", patch))!;

    expect(result.value).toBe("unknown");
    expect(mocks.canUpdate).toHaveBeenCalledWith(todos, "todo-1", patch);

    decision.resolve(false);
    await decision.promise;
    await nextTick();

    expect(result.value).toBe(false);
    scope.stop();
  });

  it("rechecks canUpdate when a reactive patch mutates in place", async () => {
    mocks.canUpdate.mockResolvedValue(true);
    const patch = reactive({ done: false });
    const scope = effectScope();

    const result = scope.run(() => useCanUpdate(todos, "todo-1", patch))!;
    await nextTick();
    await Promise.resolve();

    expect(result.value).toBe(true);
    expect(mocks.canUpdate).toHaveBeenCalledTimes(1);

    patch.done = true;
    await nextTick();
    await Promise.resolve();

    expect(result.value).toBe(true);
    expect(mocks.canUpdate).toHaveBeenCalledTimes(2);
    expect(mocks.canUpdate).toHaveBeenLastCalledWith(todos, "todo-1", patch);
    scope.stop();
  });
});
