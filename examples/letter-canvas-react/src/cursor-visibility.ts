const REMOTE_CURSOR_IDLE_MS = 3500;

export function cursorShouldRender(
  cursor: {
    user_id: string;
    $updatedAt?: Date | null;
  },
  sessionUserId: string | null,
  nowMs: number,
) {
  if (cursor.user_id === sessionUserId) {
    return true;
  }

  const updatedAtMs = cursor.$updatedAt?.getTime();
  if (updatedAtMs === undefined || Number.isNaN(updatedAtMs)) {
    return true;
  }

  return nowMs - updatedAtMs <= REMOTE_CURSOR_IDLE_MS;
}
