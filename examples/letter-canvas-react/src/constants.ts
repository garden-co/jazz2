import type { MotionSettings } from "./motion.js";

export const appId = import.meta.env.VITE_JAZZ_APP_ID ?? "019dba27-d0eb-7571-9631-6c6cfa54f364";
export const serverUrl = "https://v2.sync.jazz.tools/";

export const BOARD_WIDTH = 1200;
export const BOARD_HEIGHT = 675;
export const BOARD_ASPECT_RATIO = "16:9";
export const BOARD_CENTER = { x: BOARD_WIDTH / 2, y: BOARD_HEIGHT / 2 };

export const LETTER_WIDTH = 64;
export const LETTER_HEIGHT = 68;
export const LETTER_HALF_WIDTH = LETTER_WIDTH / 2;
export const LETTER_HALF_HEIGHT = LETTER_HEIGHT / 2;
export const CURSOR_HAND_RADIUS_X = LETTER_HALF_WIDTH + 10;
export const CURSOR_HAND_RADIUS_Y = LETTER_HALF_HEIGHT + 10;

export const LETTER_SOURCE_ROWS = ["qwertyuiop", "asdfghjkl", "zxcvbnm"].map((row) =>
  row.split(""),
);

export const INPUT_WRITE_INTERVAL_MS = 50;
export const DRAWER_TAP_HINT_MS = 1400;
export const DRAWER_TAP_DISTANCE_PX = 72;
export const LOCATION_QR_SIZE = 288;
export const CURSOR_VISIBILITY_TICK_MS = 250;

export const PARENT_ARROW_KEY_MESSAGE_TYPE = "jazz-letter-canvas:arrow-key";
export const FORWARDED_ARROW_KEYS = new Set(["ArrowUp", "ArrowRight", "ArrowDown", "ArrowLeft"]);

export const CURSOR_MOTION_SETTINGS: MotionSettings = {
  smoothingMs: 78,
  predictionMs: 44,
  predictionFadeMs: 150,
  maxPredictionDistance: 18,
  snapDistance: 0.4,
  bounds: { minX: 0, maxX: BOARD_WIDTH, minY: 0, maxY: BOARD_HEIGHT },
};

export const LETTER_MOTION_SETTINGS: MotionSettings = {
  smoothingMs: 92,
  predictionMs: 32,
  predictionFadeMs: 160,
  maxPredictionDistance: 12,
  snapDistance: 0.35,
  bounds: {
    minX: LETTER_HALF_WIDTH,
    maxX: BOARD_WIDTH - LETTER_HALF_WIDTH,
    minY: 36,
    maxY: BOARD_HEIGHT - 36,
  },
};

export const CURSOR_COLORS = [
  "#f05d5e",
  "#f28f3b",
  "#70c1b3",
  "#247ba0",
  "#6c5ce7",
  "#ef476f",
  "#06d6a0",
  "#118ab2",
];
