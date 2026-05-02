export function formatUserLabel(userId: string | null) {
  if (!userId) {
    return "guest";
  }

  return `guest-${userId.slice(-4)}`;
}

export function formatLetterGlyph(value: string) {
  return value.toLowerCase();
}

export function drawerOffset(rowIndex: number) {
  return [0, 5, 10][rowIndex] ?? 0;
}
