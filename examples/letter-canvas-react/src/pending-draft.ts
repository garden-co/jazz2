type DraftCursor = {
  gesture: "pointer" | "hand";
  x: number;
  y: number;
  drag_value: string | null;
  drag_x: number | null;
  drag_y: number | null;
};

const ACTIVE_DRAFT_POSITION_EPSILON = 1;

export function cursorHasActiveDraftPreview(cursor: DraftCursor) {
  if (
    cursor.gesture !== "hand" ||
    !cursor.drag_value ||
    cursor.drag_x === null ||
    cursor.drag_y === null
  ) {
    return false;
  }

  return (
    Math.abs(cursor.x - cursor.drag_x) <= ACTIVE_DRAFT_POSITION_EPSILON &&
    Math.abs(cursor.y - cursor.drag_y) <= ACTIVE_DRAFT_POSITION_EPSILON
  );
}
