import { beforeEach, describe, expect, it, vi } from "vitest";
import { flushSync } from "svelte";
import "./test-helpers.svelte.js";

const mocks = vi.hoisted(() => {
  const unsubscribe = vi.fn();
  const subscribe = vi.fn(() => unsubscribe);
  const makeQueryKey = vi.fn((q: any) => `key:${q?._marker ?? "?"}`);
  const getCacheEntry = vi.fn(() => ({
    state: { status: "fulfilled", data: [] },
    subscribe,
  }));

  return {
    makeQueryKey,
    getCacheEntry,
    subscribe,
    unsubscribe,
    reset() {
      unsubscribe.mockReset();
      subscribe.mockReset().mockReturnValue(unsubscribe);
      makeQueryKey.mockReset().mockImplementation((q: any) => `key:${q?._marker ?? "?"}`);
      getCacheEntry.mockReset().mockReturnValue({
        state: { status: "fulfilled", data: [] },
        subscribe,
      });
    },
  };
});

vi.mock("./context.svelte.js", () => ({
  getJazzContext: () => ({
    db: null,
    session: null,
    manager: {
      makeQueryKey: mocks.makeQueryKey,
      getCacheEntry: mocks.getCacheEntry,
    },
  }),
}));

const { QuerySubscription } = await import("./use-all.svelte.js");

function makeQuery(marker = "todos") {
  return { _marker: marker } as any;
}

async function settle() {
  await Promise.resolve();
  flushSync();
}

describe("svelte/QuerySubscription", () => {
  beforeEach(() => {
    mocks.reset();
  });

  it("subscribes when given a plain QueryBuilder", async () => {
    const query = makeQuery("inbox");
    const cleanup = $effect.root(() => {
      new QuerySubscription(query);
    });
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    cleanup();
  });

  it("does not subscribe when given undefined", async () => {
    const cleanup = $effect.root(() => {
      new QuerySubscription(undefined);
    });
    await settle();

    expect(mocks.makeQueryKey).not.toHaveBeenCalled();
    expect(mocks.subscribe).not.toHaveBeenCalled();

    cleanup();
  });

  it("accepts a getter and subscribes with the resolved query", async () => {
    const query = makeQuery("inbox");
    const cleanup = $effect.root(() => {
      new QuerySubscription(() => query);
    });
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    cleanup();
  });

  it("getter returning undefined does not subscribe", async () => {
    const cleanup = $effect.root(() => {
      new QuerySubscription(() => undefined);
    });
    await settle();

    expect(mocks.makeQueryKey).not.toHaveBeenCalled();
    expect(mocks.subscribe).not.toHaveBeenCalled();

    cleanup();
  });

  it("getter reading $state re-subscribes when state changes", async () => {
    let filter = $state<string | null>(null);
    const inboxQuery = makeQuery("inbox");
    const filteredQuery = makeQuery("filtered");

    const cleanup = $effect.root(() => {
      new QuerySubscription(() => (filter ? filteredQuery : inboxQuery));
    });
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenLastCalledWith(inboxQuery, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    filter = "alice";
    await settle();

    expect(mocks.unsubscribe).toHaveBeenCalledTimes(1);
    expect(mocks.makeQueryKey).toHaveBeenLastCalledWith(filteredQuery, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(2);

    cleanup();
  });

  it("getter flipping undefined → query subscribes only after the flip", async () => {
    let filter = $state<string | null>(null);
    const filteredQuery = makeQuery("filtered");

    const cleanup = $effect.root(() => {
      new QuerySubscription(() => (filter ? filteredQuery : undefined));
    });
    await settle();

    expect(mocks.makeQueryKey).not.toHaveBeenCalled();
    expect(mocks.subscribe).not.toHaveBeenCalled();

    filter = "alice";
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(filteredQuery, undefined);
    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    cleanup();
  });

  it("getter flipping query → undefined unsubscribes", async () => {
    let active = $state(true);
    const query = makeQuery("inbox");

    const cleanup = $effect.root(() => {
      new QuerySubscription(() => (active ? query : undefined));
    });
    await settle();

    expect(mocks.subscribe).toHaveBeenCalledTimes(1);

    active = false;
    await settle();

    expect(mocks.unsubscribe).toHaveBeenCalledTimes(1);

    cleanup();
  });

  it("options accepts a getter and forwards the resolved value", async () => {
    const query = makeQuery("inbox");

    const cleanup = $effect.root(() => {
      new QuerySubscription(query, () => ({ tier: "edge" as const }));
    });
    await settle();

    expect(mocks.makeQueryKey).toHaveBeenCalledWith(query, { tier: "edge" });

    cleanup();
  });
});
