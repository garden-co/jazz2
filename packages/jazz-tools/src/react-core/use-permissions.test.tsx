import React from "react";
import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { PermissionDecision, TableProxy } from "../runtime/index.js";
import { JazzClientProvider } from "./provider.js";
import { makeFakeClient } from "./test-utils.js";
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

function makePermissionClient(methods: {
  canInsert?: ReturnType<typeof vi.fn>;
  canUpdate?: ReturnType<typeof vi.fn>;
}) {
  const client = makeFakeClient({ authMode: "local-first", userId: "alice", claims: {} });
  Object.assign(client.db, methods);
  return client;
}

function wrapperFor(client: ReturnType<typeof makeFakeClient>) {
  return function Wrapper({ children }: { children: React.ReactNode }) {
    return <JazzClientProvider client={client as any}>{children}</JazzClientProvider>;
  };
}

describe("permission hooks", () => {
  it("returns unknown until canInsert resolves, then returns the decision", async () => {
    const decision = deferred<PermissionDecision>();
    const canInsert = vi.fn(() => decision.promise);
    const client = makePermissionClient({ canInsert });
    const message = { title: "Ship auth chat", done: false };

    const { result } = renderHook(() => useCanInsert(todos, message), {
      wrapper: wrapperFor(client),
    });

    expect(result.current).toBe("unknown");
    expect(canInsert).toHaveBeenCalledWith(todos, message);

    await act(async () => {
      decision.resolve(true);
      await decision.promise;
    });

    expect(result.current).toBe(true);
  });

  it("does not call canInsert until data is available", () => {
    const canInsert = vi.fn(async () => true as const);
    const client = makePermissionClient({ canInsert });

    const { result } = renderHook(() => useCanInsert(todos, undefined), {
      wrapper: wrapperFor(client),
    });

    expect(result.current).toBe("unknown");
    expect(canInsert).not.toHaveBeenCalled();
  });

  it("does not repeat canInsert when an equal object is recreated", async () => {
    const canInsert = vi.fn(async () => true as const);
    const client = makePermissionClient({ canInsert });

    const { result, rerender } = renderHook(
      ({ title }) => useCanInsert(todos, { title, done: false }),
      {
        initialProps: { title: "Ship auth chat" },
        wrapper: wrapperFor(client),
      },
    );

    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current).toBe(true);
    expect(canInsert).toHaveBeenCalledTimes(1);

    rerender({ title: "Ship auth chat" });

    expect(result.current).toBe(true);
    expect(canInsert).toHaveBeenCalledTimes(1);
  });

  it("ignores stale canInsert results after the input changes", async () => {
    const firstDecision = deferred<PermissionDecision>();
    const secondDecision = deferred<PermissionDecision>();
    const canInsert = vi
      .fn()
      .mockReturnValueOnce(firstDecision.promise)
      .mockReturnValueOnce(secondDecision.promise);
    const client = makePermissionClient({ canInsert });
    const firstMessage = { title: "First draft", done: false };
    const secondMessage = { title: "Second draft", done: false };

    const { result, rerender } = renderHook(({ message }) => useCanInsert(todos, message), {
      initialProps: { message: firstMessage },
      wrapper: wrapperFor(client),
    });

    expect(result.current).toBe("unknown");

    rerender({ message: secondMessage });
    expect(canInsert).toHaveBeenCalledTimes(2);

    await act(async () => {
      secondDecision.resolve(false);
      await secondDecision.promise;
    });

    expect(result.current).toBe(false);

    await act(async () => {
      firstDecision.resolve(true);
      await firstDecision.promise;
    });

    expect(result.current).toBe(false);
  });

  it("returns unknown until canUpdate resolves, then returns the decision", async () => {
    const decision = deferred<PermissionDecision>();
    const canUpdate = vi.fn(() => decision.promise);
    const client = makePermissionClient({ canUpdate });
    const patch = { done: true };

    const { result } = renderHook(() => useCanUpdate(todos, "todo-1", patch), {
      wrapper: wrapperFor(client),
    });

    expect(result.current).toBe("unknown");
    expect(canUpdate).toHaveBeenCalledWith(todos, "todo-1", patch);

    await act(async () => {
      decision.resolve(false);
      await decision.promise;
    });

    expect(result.current).toBe(false);
  });

  it("does not repeat canUpdate when an equal patch object is recreated", async () => {
    const canUpdate = vi.fn(async () => true as const);
    const client = makePermissionClient({ canUpdate });

    const { result, rerender } = renderHook(({ done }) => useCanUpdate(todos, "todo-1", { done }), {
      initialProps: { done: true },
      wrapper: wrapperFor(client),
    });

    await act(async () => {
      await Promise.resolve();
    });

    expect(result.current).toBe(true);
    expect(canUpdate).toHaveBeenCalledTimes(1);

    rerender({ done: true });

    expect(result.current).toBe(true);
    expect(canUpdate).toHaveBeenCalledTimes(1);
  });
});
