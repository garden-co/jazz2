import { useEffect, useState } from "react";
import type { PermissionDecision, TableProxy } from "../runtime/index.js";
import { useDb } from "./provider.js";

type Optional<T> = T | null | undefined;

type PermissionDb = {
  canInsert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<PermissionDecision>;
  canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): Promise<PermissionDecision>;
};

function permissionInputKey(value: unknown): string {
  return JSON.stringify(value) ?? String(value);
}

type InsertResolution<T, Init> = {
  table: TableProxy<T, Init>;
  dataKey: string;
  decision: PermissionDecision;
};

type UpdateResolution<T, Init> = {
  table: TableProxy<T, Init>;
  id: string;
  dataKey: string;
  decision: PermissionDecision;
};

/**
 * Ask whether an insert would be accepted by the current local permission state.
 *
 * Returns `"unknown"` until the permission check resolves, or when `table`/`data`
 * is missing. Rejections fail closed as `false`.
 */
export function useCanInsert<T, Init>(
  table: Optional<TableProxy<T, Init>>,
  data: Optional<Init>,
): PermissionDecision {
  const db = useDb<PermissionDb>();
  const [resolution, setResolution] = useState<InsertResolution<T, Init> | null>(null);
  const dataKey = data == null ? null : permissionInputKey(data);

  useEffect(() => {
    if (!table || data == null || dataKey == null) return;

    let cancelled = false;

    void db.canInsert(table, data).then(
      (decision) => {
        if (!cancelled) {
          setResolution({ table, dataKey, decision });
        }
      },
      () => {
        if (!cancelled) {
          setResolution({ table, dataKey, decision: false });
        }
      },
    );

    return () => {
      cancelled = true;
    };
  }, [dataKey, db, table]);

  if (!table || dataKey == null) return "unknown";

  return resolution?.table === table && resolution.dataKey === dataKey
    ? resolution.decision
    : "unknown";
}

/**
 * Ask whether an update would be accepted by the current local permission state.
 *
 * Returns `"unknown"` until the permission check resolves, or when `table`, `id`,
 * or `data` is missing. Rejections fail closed as `false`.
 */
export function useCanUpdate<T, Init>(
  table: Optional<TableProxy<T, Init>>,
  id: Optional<string>,
  data: Optional<Partial<Init>>,
): PermissionDecision {
  const db = useDb<PermissionDb>();
  const [resolution, setResolution] = useState<UpdateResolution<T, Init> | null>(null);
  const dataKey = data == null ? null : permissionInputKey(data);

  useEffect(() => {
    if (!table || !id || data == null || dataKey == null) return;

    let cancelled = false;

    void db.canUpdate(table, id, data).then(
      (decision) => {
        if (!cancelled) {
          setResolution({ table, id, dataKey, decision });
        }
      },
      () => {
        if (!cancelled) {
          setResolution({ table, id, dataKey, decision: false });
        }
      },
    );

    return () => {
      cancelled = true;
    };
  }, [dataKey, db, id, table]);

  if (!table || !id || dataKey == null) return "unknown";

  return resolution?.table === table && resolution.id === id && resolution.dataKey === dataKey
    ? resolution.decision
    : "unknown";
}
