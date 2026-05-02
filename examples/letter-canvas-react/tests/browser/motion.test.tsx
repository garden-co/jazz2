import { describe, expect, it } from "vitest";
import {
  advanceMotionState,
  createMotionState,
  projectMotionTarget,
  retargetMotionState,
  type MotionSettings,
} from "../../src/motion.js";

const TEST_SETTINGS: MotionSettings = {
  smoothingMs: 90,
  predictionMs: 60,
  predictionFadeMs: 180,
  maxPredictionDistance: 18,
  snapDistance: 0.35,
};

describe("motion smoothing", () => {
  it("projects a slight amount forward from recent movement", () => {
    let state = createMotionState({ id: "cursor", x: 120, y: 90 }, 0);
    state = retargetMotionState(state, { id: "cursor", x: 180, y: 90 }, 50);

    const projected = projectMotionTarget(state, 70, TEST_SETTINGS);

    expect(projected.x).toBeGreaterThan(180);
    expect(projected.x).toBeLessThanOrEqual(198);
    expect(projected.y).toBe(90);
  });

  it("fades prediction away and settles back on the synced target", () => {
    let state = createMotionState({ id: "letter", x: 320, y: 220 }, 0);
    state = retargetMotionState(state, { id: "letter", x: 420, y: 260 }, 50);

    const projectedWhileFresh = projectMotionTarget(state, 70, TEST_SETTINGS);
    const projectedWhenStale = projectMotionTarget(state, 320, TEST_SETTINGS);

    expect(projectedWhileFresh.x).toBeGreaterThan(420);
    expect(projectedWhenStale).toEqual({ x: 420, y: 260 });

    state = advanceMotionState(state, 140, TEST_SETTINGS);
    state = advanceMotionState(state, 320, TEST_SETTINGS);
    state = advanceMotionState(state, 460, TEST_SETTINGS);

    expect(state.currentX).toBeCloseTo(420, 1);
    expect(state.currentY).toBeCloseTo(260, 1);
  });

  it("snaps immediate local motion straight to the latest pointer position", () => {
    let state = createMotionState({ id: "me", x: 40, y: 50 }, 0);
    state = retargetMotionState(state, { id: "me", x: 200, y: 180, immediate: true }, 50);
    state = advanceMotionState(state, 80, TEST_SETTINGS);

    expect(state.currentX).toBe(200);
    expect(state.currentY).toBe(180);
    expect(state.velocityX).toBe(0);
    expect(state.velocityY).toBe(0);
  });
});
