import { describe, expect, it } from "vitest";
import { cursorShouldRender } from "../src/cursor-visibility.js";

describe("cursorShouldRender", () => {
  it("keeps the local user's cursor visible even after it has been idle", () => {
    expect(
      cursorShouldRender(
        {
          user_id: "alice",
          $updatedAt: new Date("2026-04-22T21:00:00.000Z"),
        },
        "alice",
        Date.parse("2026-04-22T21:00:10.000Z"),
      ),
    ).toBe(true);
  });

  it("hides remote cursors after they have been idle for too long", () => {
    expect(
      cursorShouldRender(
        {
          user_id: "bob",
          $updatedAt: new Date("2026-04-22T21:00:00.000Z"),
        },
        "alice",
        Date.parse("2026-04-22T21:00:10.000Z"),
      ),
    ).toBe(false);
  });

  it("keeps recently updated remote cursors visible", () => {
    expect(
      cursorShouldRender(
        {
          user_id: "bob",
          $updatedAt: new Date("2026-04-22T21:00:09.000Z"),
        },
        "alice",
        Date.parse("2026-04-22T21:00:10.000Z"),
      ),
    ).toBe(true);
  });

  it("keeps remote cursors visible when native timestamps are not available yet", () => {
    expect(
      cursorShouldRender(
        {
          user_id: "bob",
          $updatedAt: null,
        },
        "alice",
        Date.parse("2026-04-22T21:00:10.000Z"),
      ),
    ).toBe(true);
  });
});
