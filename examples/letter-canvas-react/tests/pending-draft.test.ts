import { describe, expect, it } from "vitest";
import { cursorHasActiveDraftPreview } from "../src/pending-draft.js";

describe("cursorHasActiveDraftPreview", () => {
  it("renders a draft only while the cursor is actively dragging that new letter", () => {
    expect(
      cursorHasActiveDraftPreview({
        gesture: "hand",
        x: 320,
        y: 180,
        drag_value: "a",
        drag_x: 320,
        drag_y: 180,
      }),
    ).toBe(true);
  });

  it("suppresses stale draft data when a collaborator is just hovering another letter", () => {
    expect(
      cursorHasActiveDraftPreview({
        gesture: "hand",
        x: 480,
        y: 240,
        drag_value: "a",
        drag_x: 320,
        drag_y: 180,
      }),
    ).toBe(false);
  });
});
