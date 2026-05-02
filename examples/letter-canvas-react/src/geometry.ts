import type * as React from "react";
import type { Letter } from "../schema.js";
import {
  BOARD_HEIGHT,
  BOARD_WIDTH,
  CURSOR_HAND_RADIUS_X,
  CURSOR_HAND_RADIUS_Y,
} from "./constants.js";

export type Point = { x: number; y: number };

export function logicalPositionStyle(x: number, y: number): React.CSSProperties {
  return {
    left: `${(x / BOARD_WIDTH) * 100}%`,
    top: `${(y / BOARD_HEIGHT) * 100}%`,
  };
}

export function clientPointToLogical(
  board: HTMLDivElement | null,
  clientX: number,
  clientY: number,
) {
  if (!board) {
    return null;
  }

  const rect = board.getBoundingClientRect();
  if (
    clientX < rect.left ||
    clientX > rect.right ||
    clientY < rect.top ||
    clientY > rect.bottom ||
    rect.width === 0 ||
    rect.height === 0
  ) {
    return null;
  }

  return {
    x: ((clientX - rect.left) / rect.width) * BOARD_WIDTH,
    y: ((clientY - rect.top) / rect.height) * BOARD_HEIGHT,
  };
}

export function clientPointToClampedLogical(
  board: HTMLDivElement | null,
  clientX: number,
  clientY: number,
) {
  if (!board) {
    return null;
  }

  const rect = board.getBoundingClientRect();
  if (rect.width === 0 || rect.height === 0) {
    return null;
  }

  return {
    x: clamp(((clientX - rect.left) / rect.width) * BOARD_WIDTH, 0, BOARD_WIDTH),
    y: clamp(((clientY - rect.top) / rect.height) * BOARD_HEIGHT, 0, BOARD_HEIGHT),
  };
}

export function pointOverLetter(point: Point, letters: Letter[]) {
  return letters.some(
    (letter) =>
      Math.abs(point.x - letter.x) <= CURSOR_HAND_RADIUS_X &&
      Math.abs(point.y - letter.y) <= CURSOR_HAND_RADIUS_Y,
  );
}

export function resolveDisplayPoint(
  positions: ReadonlyMap<string, Point>,
  id: string,
  fallbackX: number,
  fallbackY: number,
) {
  return positions.get(id) ?? { x: fallbackX, y: fallbackY };
}

export function clamp(value: number, min: number, max: number) {
  return Math.min(max, Math.max(min, value));
}

export function formatPercent(value: number) {
  return value.toFixed(2);
}
