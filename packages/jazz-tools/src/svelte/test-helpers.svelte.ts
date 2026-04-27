import { vi } from "vitest";

// Shared context mock for Svelte rune tests.
// Only setContext/getContext are mocked (no component tree in tests);
// the rest of the Svelte runtime — including the compiler-generated
// rune primitives — is left real.

export const contextStore = new Map<unknown, unknown>();

vi.mock("svelte", async (importOriginal) => {
  const actual = await importOriginal<typeof import("svelte")>();
  return {
    ...actual,
    setContext: (key: unknown, value: unknown) => contextStore.set(key, value),
    getContext: (key: unknown) => contextStore.get(key),
    onDestroy: vi.fn(),
  };
});
