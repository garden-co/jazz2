import type { Cursor } from "../schema.js";
import { CURSOR_COLORS } from "./constants.js";
import { formatUserLabel } from "./format.js";
import { logicalPositionStyle, type Point } from "./geometry.js";

export function CursorMarker({
  cursor,
  isMe,
  iconKind,
  position,
}: {
  cursor: Cursor;
  isMe: boolean;
  iconKind: "pointer" | "hand";
  position: Point;
}) {
  const color = colorFromUserId(cursor.user_id);

  return (
    <div
      data-testid="cursor-marker"
      data-cursor-color={color}
      data-cursor-icon={iconKind}
      data-cursor-user-id={cursor.user_id}
      className="cursor-marker"
      style={logicalPositionStyle(position.x, position.y)}
    >
      <div className="cursor-glyph" style={{ color }}>
        <CursorIcon kind={iconKind} />
      </div>
      <div className="cursor-label" style={{ borderColor: `${color}33`, color }}>
        {isMe ? "You" : formatUserLabel(cursor.user_id)}
      </div>
    </div>
  );
}

function colorFromUserId(userId: string) {
  let hash = 0;
  for (let index = 0; index < userId.length; index += 1) {
    hash = (hash * 31 + userId.charCodeAt(index)) >>> 0;
  }
  return CURSOR_COLORS[hash % CURSOR_COLORS.length]!;
}

function CursorIcon({ kind }: { kind: "pointer" | "hand" }) {
  if (kind === "hand") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M18 11V6a2 2 0 0 0-2-2a2 2 0 0 0-2 2" />
        <path d="M14 10V4a2 2 0 0 0-2-2a2 2 0 0 0-2 2v2" />
        <path d="M10 10.5V6a2 2 0 0 0-2-2a2 2 0 0 0-2 2v8" />
        <path d="M18 8a2 2 0 1 1 4 0v6a8 8 0 0 1-8 8h-2c-2.8 0-4.5-.86-5.99-2.34l-3.6-3.6a2 2 0 0 1 2.83-2.82L7 15" />
      </svg>
    );
  }

  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M4.037 4.688a.495.495 0 0 1 .651-.651l16 6.5a.5.5 0 0 1-.063.947l-6.124 1.58a2 2 0 0 0-1.438 1.435l-1.579 6.126a.5.5 0 0 1-.947.063z" />
    </svg>
  );
}
