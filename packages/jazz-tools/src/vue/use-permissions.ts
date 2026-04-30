import { ref, toValue, watch, type MaybeRefOrGetter, type Ref } from "vue";
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

/**
 * Ask whether an insert would be accepted by the current local permission state.
 *
 * The returned ref is `"unknown"` until the permission check resolves, or when
 * `table`/`data` is missing. Rejections fail closed as `false`.
 */
export function useCanInsert<T, Init>(
  table: MaybeRefOrGetter<Optional<TableProxy<T, Init>>>,
  data: MaybeRefOrGetter<Optional<Init>>,
): Ref<PermissionDecision> {
  const db = useDb() as PermissionDb;
  const decision = ref<PermissionDecision>("unknown") as Ref<PermissionDecision>;
  let runId = 0;

  watch(
    [() => toValue(table), () => toValue(data)],
    ([resolvedTable, resolvedData]) => {
      if (!resolvedTable || resolvedData == null) {
        runId += 1;
        decision.value = "unknown";
        return;
      }

      const activeRunId = ++runId;
      decision.value = "unknown";

      void db.canInsert(resolvedTable, resolvedData).then(
        (nextDecision) => {
          if (activeRunId === runId) {
            decision.value = nextDecision;
          }
        },
        () => {
          if (activeRunId === runId) {
            decision.value = false;
          }
        },
      );
    },
    { immediate: true, deep: true },
  );

  return decision;
}

/**
 * Ask whether an update would be accepted by the current local permission state.
 *
 * The returned ref is `"unknown"` until the permission check resolves, or when
 * `table`, `id`, or `data` is missing. Rejections fail closed as `false`.
 */
export function useCanUpdate<T, Init>(
  table: MaybeRefOrGetter<Optional<TableProxy<T, Init>>>,
  id: MaybeRefOrGetter<Optional<string>>,
  data: MaybeRefOrGetter<Optional<Partial<Init>>>,
): Ref<PermissionDecision> {
  const db = useDb() as PermissionDb;
  const decision = ref<PermissionDecision>("unknown") as Ref<PermissionDecision>;
  let runId = 0;

  watch(
    [() => toValue(table), () => toValue(id), () => toValue(data)],
    ([resolvedTable, resolvedId, resolvedData]) => {
      if (!resolvedTable || !resolvedId || resolvedData == null) {
        runId += 1;
        decision.value = "unknown";
        return;
      }

      const activeRunId = ++runId;
      decision.value = "unknown";

      void db.canUpdate(resolvedTable, resolvedId, resolvedData).then(
        (nextDecision) => {
          if (activeRunId === runId) {
            decision.value = nextDecision;
          }
        },
        () => {
          if (activeRunId === runId) {
            decision.value = false;
          }
        },
      );
    },
    { immediate: true, deep: true },
  );

  return decision;
}
