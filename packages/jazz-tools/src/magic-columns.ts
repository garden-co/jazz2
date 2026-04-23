import type { ColumnType } from "./drivers/types.js";

export const RESERVED_MAGIC_COLUMN_PREFIX = "$";

export const PERMISSION_INTROSPECTION_COLUMNS = ["$canRead", "$canEdit", "$canDelete"] as const;
export const PROVENANCE_MAGIC_COLUMNS = [
  "$createdBy",
  "$createdAt",
  "$updatedBy",
  "$updatedAt",
] as const;
export const PROVENANCE_MAGIC_TIMESTAMP_COLUMNS = ["$createdAt", "$updatedAt"] as const;

export type PermissionIntrospectionColumn = (typeof PERMISSION_INTROSPECTION_COLUMNS)[number];
export type ProvenanceMagicColumn = (typeof PROVENANCE_MAGIC_COLUMNS)[number];
export type ProvenanceMagicTimestampColumn = (typeof PROVENANCE_MAGIC_TIMESTAMP_COLUMNS)[number];

export function isPermissionIntrospectionColumn(
  column: string,
): column is PermissionIntrospectionColumn {
  return PERMISSION_INTROSPECTION_COLUMNS.includes(column as PermissionIntrospectionColumn);
}

export function isProvenanceMagicColumn(column: string): column is ProvenanceMagicColumn {
  return PROVENANCE_MAGIC_COLUMNS.includes(column as ProvenanceMagicColumn);
}

export function isProvenanceMagicTimestampColumn(
  column: string,
): column is ProvenanceMagicTimestampColumn {
  return PROVENANCE_MAGIC_TIMESTAMP_COLUMNS.includes(column as ProvenanceMagicTimestampColumn);
}

export function isReservedMagicColumnName(column: string): boolean {
  return column.startsWith(RESERVED_MAGIC_COLUMN_PREFIX);
}

export function assertUserColumnNameAllowed(column: string): void {
  if (isReservedMagicColumnName(column)) {
    throw new Error(
      `Column name "${column}" is reserved for magic columns. Names starting with "${RESERVED_MAGIC_COLUMN_PREFIX}" are reserved for system fields.`,
    );
  }
}

export function magicColumnType(column: string): ColumnType | undefined {
  if (isPermissionIntrospectionColumn(column)) {
    return { type: "Boolean" };
  }
  if (column === "$createdBy" || column === "$updatedBy") {
    return { type: "Text" };
  }
  if (column === "$createdAt" || column === "$updatedAt") {
    return { type: "Timestamp" };
  }
  return undefined;
}
