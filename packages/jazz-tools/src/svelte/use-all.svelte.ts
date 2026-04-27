import { onDestroy } from "svelte";
import type { QueryBuilder, QueryOptions } from "../runtime/db.js";
import type { SubscriptionDelta } from "../runtime/subscription-manager.js";
import { applyDelta } from "../reconcile-array.js";
import { getJazzContext } from "./context.svelte.js";

type MaybeGetter<T> = T | (() => T);

function resolve<T>(value: MaybeGetter<T>): T {
  return typeof value === "function" ? (value as () => T)() : value;
}

/**
 * Reactive query subscription. Instantiate in a component script block,
 * access results via `.current`.
 *
 * @param query - the database query, or a getter for a dynamic query
 *   (e.g. `() => filter ? app.todos.where({ title: { contains: filter } }) : undefined`).
 *   When a getter is passed, any reactive reads inside it are tracked, so the
 *   subscription re-runs when its dependencies change.
 * @param options - optional query execution options, or a getter for them
 *
 * ```svelte
 * <script lang="ts">
 *   const todos = new QuerySubscription(app.todos.where({ done: false }), { tier: "edge" });
 * </script>
 *
 * {#if todos.loading}
 *   <p>Loading...</p>
 * {:else if todos.error}
 *   <p>Error: {todos.error.message}</p>
 * {:else}
 *   {#each todos.current ?? [] as todo}
 *     <p>{todo.title}</p>
 *   {/each}
 * {/if}
 * ```
 */
export class QuerySubscription<T extends { id: string }> {
  current: T[] | undefined = $state();
  loading: boolean = $state(true);
  error: Error | null = $state(null);

  #unsubscribe: (() => void) | null = null;

  constructor(
    query: MaybeGetter<QueryBuilder<T> | undefined>,
    options?: MaybeGetter<QueryOptions | undefined>,
  ) {
    const ctx = getJazzContext();
    this.current = resolve(options)?.tier ? undefined : [];

    $effect(() => {
      const resolvedQuery = resolve(query);
      if (!resolvedQuery) {
        this.current = undefined;
        this.loading = false;
        return;
      }

      const manager = ctx.manager;
      if (!manager) return;

      const resolvedOptions = resolve(options);

      this.loading = true;
      this.error = null;

      try {
        const key = manager.makeQueryKey(resolvedQuery, resolvedOptions);
        const entry = manager.getCacheEntry<T>(key);

        // Apply initial state from cache
        if (entry.state.status === "fulfilled") {
          this.current = entry.state.data;
          this.loading = false;
        }

        this.#unsubscribe = entry.subscribe({
          onfulfilled: (data: T[]) => {
            this.current = data;
            this.loading = false;
          },
          onDelta: (delta: SubscriptionDelta<T>) => {
            if (this.current) {
              applyDelta(this.current, delta);
            } else {
              this.current = delta.all;
            }
          },
          onError: (error: unknown) => {
            this.error = error instanceof Error ? error : new Error(String(error));
            this.current = undefined;
            this.loading = false;
          },
        });
      } catch (e) {
        this.error = e instanceof Error ? e : new Error(String(e));
        this.loading = false;
      }

      return () => {
        this.#cleanup();
      };
    });

    onDestroy(() => {
      this.#cleanup();
    });
  }

  #cleanup() {
    if (this.#unsubscribe) {
      this.#unsubscribe();
      this.#unsubscribe = null;
    }
  }
}
