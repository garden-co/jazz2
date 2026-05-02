import { describe, expect, it } from "vitest";
import {
  clientPointToClampedLogical,
  clientPointToLogical,
  logicalPositionStyle,
  pointOverLetter,
} from "../src/geometry.js";

function mockBoard(rect: Partial<DOMRect>): HTMLDivElement {
  return {
    getBoundingClientRect: () =>
      ({
        left: 100,
        top: 50,
        right: 1300,
        bottom: 725,
        width: 1200,
        height: 675,
        ...rect,
      }) as DOMRect,
  } as HTMLDivElement;
}

describe("geometry helpers", () => {
  it("maps client coordinates into the fixed logical canvas", () => {
    expect(clientPointToLogical(mockBoard({}), 700, 387.5)).toEqual({ x: 600, y: 337.5 });
    expect(clientPointToLogical(mockBoard({}), 50, 387.5)).toBeNull();
  });

  it("clamps drag coordinates to the board instead of dropping them", () => {
    expect(clientPointToClampedLogical(mockBoard({}), 50, 800)).toEqual({ x: 0, y: 675 });
  });

  it("expresses logical coordinates as percentages for responsive rendering", () => {
    expect(logicalPositionStyle(600, 337.5)).toEqual({ left: "50%", top: "50%" });
  });

  it("detects when a cursor is close enough to a letter to show the hand icon", () => {
    expect(
      pointOverLetter({ x: 132, y: 134 }, [
        {
          id: "letter-1",
          canvasId: "canvas-1",
          value: "a",
          x: 100,
          y: 100,
          placed_by: "alice",
        },
      ]),
    ).toBe(true);
  });
});
