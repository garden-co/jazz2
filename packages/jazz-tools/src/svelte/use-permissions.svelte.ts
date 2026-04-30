import { onDestroy } from "svelte";
import type { PermissionDecision, TableProxy } from "../runtime/index.js";
import { getDb } from "./context.svelte.js";

type Optional<T> = T | null | undefined;

type PermissionDb = {
  canInsert<T, Init>(table: TableProxy<T, Init>, data: Init): Promise<PermissionDecision>;
  canUpdate<T, Init>(
    table: TableProxy<T, Init>,
    id: string,
    data: Partial<Init>,
  ): Promise<PermissionDecision>;
};

type MaybeGetter<T> = T | (() => T);

function resolve<T>(value: MaybeGetter<T>): T {
  return typeof value === "function" ? (value as () => T)() : value;
}

let unkeyableDataVersion = 0;

function dataKey(value: unknown): string {
  try {
    return JSON.stringify($state.snapshot(value)) ?? "undefined";
  } catch {
    return `unkeyable:${++unkeyableDataVersion}`;
  }
}

/**
 * Reactive Svelte permission preflight for inserts.
 *
 * Instantiate in a component script block and read `.current`.
 */
export class CanInsertPermission<T, Init> {
  current: PermissionDecision = $state("unknown");

  #db = getDb() as PermissionDb;
  #runId = 0;
  #lastTable: TableProxy<T, Init> | null = null;
  #lastData: Init | null = null;
  #lastDataKey: string | null = null;

  constructor(
    table: MaybeGetter<Optional<TableProxy<T, Init>>>,
    data: MaybeGetter<Optional<Init>>,
  ) {
    $effect(() => {
      const resolvedTable = resolve(table);
      const resolvedData = resolve(data);

      if (!resolvedTable || resolvedData == null) {
        this.#lastTable = null;
        this.#lastData = null;
        this.#lastDataKey = null;
        this.#runId += 1;
        this.current = "unknown";
        return;
      }

      const resolvedDataKey = dataKey(resolvedData);

      if (
        this.#lastTable === resolvedTable &&
        this.#lastData === resolvedData &&
        this.#lastDataKey === resolvedDataKey
      ) {
        return;
      }

      this.#lastTable = resolvedTable;
      this.#lastData = resolvedData;
      this.#lastDataKey = resolvedDataKey;

      const activeRunId = ++this.#runId;
      this.current = "unknown";

      void this.#db.canInsert(resolvedTable, resolvedData).then(
        (decision) => {
          if (activeRunId === this.#runId) {
            this.current = decision;
          }
        },
        () => {
          if (activeRunId === this.#runId) {
            this.current = false;
          }
        },
      );
    });

    onDestroy(() => {
      this.#runId += 1;
    });
  }
}

/**
 * Reactive Svelte permission preflight for updates.
 *
 * Instantiate in a component script block and read `.current`.
 */
export class CanUpdatePermission<T, Init> {
  current: PermissionDecision = $state("unknown");

  #db = getDb() as PermissionDb;
  #runId = 0;
  #lastTable: TableProxy<T, Init> | null = null;
  #lastId: string | null = null;
  #lastData: Partial<Init> | null = null;
  #lastDataKey: string | null = null;

  constructor(
    table: MaybeGetter<Optional<TableProxy<T, Init>>>,
    id: MaybeGetter<Optional<string>>,
    data: MaybeGetter<Optional<Partial<Init>>>,
  ) {
    $effect(() => {
      const resolvedTable = resolve(table);
      const resolvedId = resolve(id);
      const resolvedData = resolve(data);

      if (!resolvedTable || !resolvedId || resolvedData == null) {
        this.#lastTable = null;
        this.#lastId = null;
        this.#lastData = null;
        this.#lastDataKey = null;
        this.#runId += 1;
        this.current = "unknown";
        return;
      }

      const resolvedDataKey = dataKey(resolvedData);

      if (
        this.#lastTable === resolvedTable &&
        this.#lastId === resolvedId &&
        this.#lastData === resolvedData &&
        this.#lastDataKey === resolvedDataKey
      ) {
        return;
      }

      this.#lastTable = resolvedTable;
      this.#lastId = resolvedId;
      this.#lastData = resolvedData;
      this.#lastDataKey = resolvedDataKey;

      const activeRunId = ++this.#runId;
      this.current = "unknown";

      void this.#db.canUpdate(resolvedTable, resolvedId, resolvedData).then(
        (decision) => {
          if (activeRunId === this.#runId) {
            this.current = decision;
          }
        },
        () => {
          if (activeRunId === this.#runId) {
            this.current = false;
          }
        },
      );
    });

    onDestroy(() => {
      this.#runId += 1;
    });
  }
}
