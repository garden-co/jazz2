import * as React from "react";

export type MotionTarget = {
  id: string;
  x: number;
  y: number;
  immediate?: boolean;
  predictive?: boolean;
};

export type MotionState = {
  currentX: number;
  currentY: number;
  targetX: number;
  targetY: number;
  velocityX: number;
  velocityY: number;
  sampledAtMs: number;
  frameAtMs: number;
  immediate: boolean;
  predictive: boolean;
};

export type MotionBounds = {
  minX: number;
  maxX: number;
  minY: number;
  maxY: number;
};

export type MotionSettings = {
  smoothingMs: number;
  predictionMs: number;
  predictionFadeMs: number;
  maxPredictionDistance: number;
  snapDistance: number;
  bounds?: MotionBounds;
};

export function createMotionState(target: MotionTarget, nowMs: number): MotionState {
  return {
    currentX: target.x,
    currentY: target.y,
    targetX: target.x,
    targetY: target.y,
    velocityX: 0,
    velocityY: 0,
    sampledAtMs: nowMs,
    frameAtMs: nowMs,
    immediate: target.immediate ?? false,
    predictive: target.predictive ?? true,
  };
}

export function retargetMotionState(
  state: MotionState,
  target: MotionTarget,
  nowMs: number,
): MotionState {
  const immediate = target.immediate ?? false;
  const predictive = target.predictive ?? true;

  if (immediate) {
    return {
      currentX: target.x,
      currentY: target.y,
      targetX: target.x,
      targetY: target.y,
      velocityX: 0,
      velocityY: 0,
      sampledAtMs: nowMs,
      frameAtMs: nowMs,
      immediate,
      predictive,
    };
  }

  const dtMs = Math.max(16, nowMs - state.sampledAtMs);
  const velocityX = ((target.x - state.targetX) / dtMs) * 1000;
  const velocityY = ((target.y - state.targetY) / dtMs) * 1000;

  return {
    ...state,
    targetX: target.x,
    targetY: target.y,
    velocityX,
    velocityY,
    sampledAtMs: nowMs,
    immediate,
    predictive,
  };
}

export function projectMotionTarget(
  state: MotionState,
  nowMs: number,
  settings: MotionSettings,
): { x: number; y: number } {
  const ageMs = Math.max(0, nowMs - state.sampledAtMs);
  const predictiveScale = state.predictive ? clamp(1 - ageMs / settings.predictionFadeMs, 0, 1) : 0;
  const predictionFactor = (settings.predictionMs / 1000) * predictiveScale;

  const offsetX = clamp(
    state.velocityX * predictionFactor,
    -settings.maxPredictionDistance,
    settings.maxPredictionDistance,
  );
  const offsetY = clamp(
    state.velocityY * predictionFactor,
    -settings.maxPredictionDistance,
    settings.maxPredictionDistance,
  );

  return clampPoint({ x: state.targetX + offsetX, y: state.targetY + offsetY }, settings.bounds);
}

export function advanceMotionState(
  state: MotionState,
  nowMs: number,
  settings: MotionSettings,
): MotionState {
  if (state.immediate) {
    return {
      ...state,
      currentX: state.targetX,
      currentY: state.targetY,
      velocityX: 0,
      velocityY: 0,
      frameAtMs: nowMs,
    };
  }

  const dtMs = Math.max(0, nowMs - state.frameAtMs);
  if (dtMs === 0) {
    return state;
  }

  const projected = projectMotionTarget(state, nowMs, settings);
  const alpha = 1 - Math.exp(-dtMs / settings.smoothingMs);
  const nextX = state.currentX + (projected.x - state.currentX) * alpha;
  const nextY = state.currentY + (projected.y - state.currentY) * alpha;
  const settled =
    Math.abs(projected.x - nextX) <= settings.snapDistance &&
    Math.abs(projected.y - nextY) <= settings.snapDistance;
  const clamped = clampPoint(settled ? projected : { x: nextX, y: nextY }, settings.bounds);

  return {
    ...state,
    currentX: clamped.x,
    currentY: clamped.y,
    frameAtMs: nowMs,
  };
}

export function useSmoothedPositions(
  targets: readonly MotionTarget[],
  settings: MotionSettings,
): ReadonlyMap<string, { x: number; y: number }> {
  const recordsRef = React.useRef(new Map<string, MotionState>());
  const [positions, setPositions] = React.useState<ReadonlyMap<string, { x: number; y: number }>>(
    () => new Map(),
  );

  React.useEffect(() => {
    const nowMs = performance.now();
    const nextIds = new Set<string>();
    let changed = false;

    for (const target of targets) {
      nextIds.add(target.id);
      const existing = recordsRef.current.get(target.id);
      const nextState = !existing
        ? createMotionState(target, nowMs)
        : existing.targetX === target.x &&
            existing.targetY === target.y &&
            existing.immediate === (target.immediate ?? false) &&
            existing.predictive === (target.predictive ?? true)
          ? existing
          : retargetMotionState(existing, target, nowMs);
      if (
        !existing ||
        existing.targetX !== nextState.targetX ||
        existing.targetY !== nextState.targetY ||
        existing.immediate !== nextState.immediate ||
        existing.predictive !== nextState.predictive
      ) {
        changed = true;
      }
      recordsRef.current.set(target.id, nextState);
    }

    for (const id of recordsRef.current.keys()) {
      if (!nextIds.has(id)) {
        recordsRef.current.delete(id);
        changed = true;
      }
    }

    if (changed) {
      setPositions(snapshotMotionPositions(recordsRef.current));
    }
  }, [targets]);

  React.useEffect(() => {
    let frameId = 0;
    let cancelled = false;

    const tick = (nowMs: number) => {
      if (cancelled) {
        return;
      }

      let changed = false;
      for (const [id, state] of recordsRef.current) {
        const nextState = advanceMotionState(state, nowMs, settings);
        if (
          Math.abs(nextState.currentX - state.currentX) > 0.02 ||
          Math.abs(nextState.currentY - state.currentY) > 0.02
        ) {
          changed = true;
        }
        recordsRef.current.set(id, nextState);
      }

      if (changed) {
        setPositions(snapshotMotionPositions(recordsRef.current));
      }
      frameId = window.requestAnimationFrame(tick);
    };

    frameId = window.requestAnimationFrame(tick);

    return () => {
      cancelled = true;
      window.cancelAnimationFrame(frameId);
    };
  }, [settings]);

  return positions;
}

function snapshotMotionPositions(
  records: ReadonlyMap<string, MotionState>,
): ReadonlyMap<string, { x: number; y: number }> {
  return new Map(
    [...records.entries()].map(([id, state]) => [id, { x: state.currentX, y: state.currentY }]),
  );
}

function clampPoint(point: { x: number; y: number }, bounds?: MotionBounds) {
  if (!bounds) {
    return point;
  }

  return {
    x: clamp(point.x, bounds.minX, bounds.maxX),
    y: clamp(point.y, bounds.minY, bounds.maxY),
  };
}

function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}
