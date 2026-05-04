# App Surface — Status Quo

Most Jazz users meet the system through two files:

- `schema.ts`
- application code that calls `createDb(...)`

That is intentional. The runtime is doing a lot underneath, but the app-facing surface is meant to feel like ordinary table-first application code.

## The Basic Shape

You define tables:

```ts
import { schema as s } from "jazz-tools";

const schema = {
  todos: s.table({
    title: s.string(),
    done: s.boolean(),
    projectId: s.ref("projects").optional(),
  }),
  projects: s.table({
    name: s.string(),
  }),
};

type AppSchema = s.Schema<typeof schema>;
export const app: s.App<AppSchema> = s.defineApp(schema);
```

From that one definition you get:

- typed table handles such as `app.todos`
- typed row/input helpers such as `RowOf`, `InsertOf`, and `WhereOf`
- runtime schema metadata used by the engine

## What `app.todos` Really Is

Each table handle is a typed query builder plus a table identity.

That is why application code can write:

```ts
app.todos.where({ done: false }).orderBy("title").limit(10);
```

and also:

```ts
db.insert(app.todos, { title: "Ship docs", done: false });
```

The table handle is the shared entry point for reads, writes, and subscriptions.

## The Normal App Workflow

```ts
const db = await createDb(config);

const todos = await db.all(app.todos.where({ done: false }));

const unsubscribe = db.subscribeAll(app.todos, ({ all }) => {
  console.log(all);
});

await db.insert(app.todos, { title: "Ship docs", done: false });
```

That is the friendly promise of the stack:

- you think in tables and rows
- the runtime handles row histories, visibility, sync, and persistence underneath

## Query Builder Surface

The typed query builders expose the table-first operations most application code cares about:

- `where(...)`
- `select(...)`
- `include(...)`
- `orderBy(...)`
- `limit(...)`
- `offset(...)`
- relation traversal helpers such as `hopTo(...)` and `gather(...)`

These builders are immutable. Each call returns a new query shape that `Db` can translate into the runtime query representation.

## Runtime Surface

The current `Db` API centers around a small set of predictable operations:

- `all(...)`
- `one(...)`
- `insert(...)`
- `update(...)`
- `delete(...)`
- `subscribeAll(...)`
- `beginBatch()`
- `beginTransaction()`

Simple write calls are one-member direct batches under the hood. They seal immediately and return
handles that callers can wait on for a specific durability tier.

## Explicit Batch APIs

For callers that want to group writes or opt into authority-decided transactions, the app surface
now exposes explicit batch handles.

At the runtime-client layer:

- `client.beginBatch()`
- `client.beginTransaction()`
- `client.localBatchRecord(batchId)`
- `client.localBatchRecords()`
- `client.acknowledgeRejectedBatch(batchId)`

At the typed `Db` layer:

- `db.beginBatch()`
- `db.beginTransaction()`

The returned handles (`DirectBatch`, `Transaction`, `DbDirectBatch`, `DbTransaction`) reuse the
same CRUD surface as normal writes, but with one shared logical `BatchId`.
For the typed `Db` wrappers, the first table operation fixes which underlying runtime client/schema
owns that handle.

Open batch writes are intentionally not individually waitable:

- `tx.insert(...)` and `batch.insert(...)` return the inserted row
- `tx.update(...)`, `tx.delete(...)`, `batch.update(...)`, and `batch.delete(...)` return `void`
- `tx.commit()` and `batch.commit()` return the batch-shaped write handle
- `tx.rollback()` closes an open transaction handle without sealing or committing the batch

That makes the common durable path explicit in the type shape:

```ts
const batch = db.beginBatch();
batch.insert(app.todos, { title: "Ship it", done: false });
await batch.commit().wait({ tier: "edge" });

const tx = db.beginTransaction();
tx.update(app.todos, "todo-1", { done: true });
await tx.commit().wait({ tier: "global" });
```

Transactional handles also expose transaction-scoped reads over their own staged rows:

- `Transaction.query(...)`
- `DbTransaction.all(...)`
- `DbTransaction.one(...)`

Both explicit batch handles add the explicit completion step:

- `tx.commit()` in TypeScript
- `batch.commit()` in TypeScript

Transactional handles also expose `tx.rollback()` / `DbTransaction.rollback()`. Rollback only marks
that transaction handle as rolled back: it does not seal the batch, does not remove pending staged
rows, and makes any later write, read, `commit()`, or `rollback()` call on the same transaction fail.

Persisted writes are batch-shaped too:

- the handle exposes `batchId()`
- `wait()` resolves when the requested replayable durability outcome is satisfied, or rejects if the batch is rejected
- `localBatchRecord()` reloads retained local state
- `acknowledgeRejectedBatch()` prunes retained rejected records once the app has handled them

## What App Code Does _Not_ Need to Care About

The runtime still tracks engine-owned row information such as:

- row ids
- batch ids
- branches
- visibility state
- durability tiers

But those fields are not the normal surface application authors work with. The app-facing API stays table-first, while the runtime uses those engine fields to make local-first behavior reliable.

For the lower-level runtime/storage story underneath these APIs, see [Batches](batches.md).

## Framework Bindings

React, Vue, and Svelte adapters sit on top of the same `Db` and `app` surface.

They mainly add:

- context/provider setup
- hook/store integration
- lifecycle-aware subscription management

The data model does not change between frameworks.

## Key Files

| File                                    | Purpose                               |
| --------------------------------------- | ------------------------------------- |
| `packages/jazz-tools/src/typed-app.ts`  | Typed app/table/query builder surface |
| `packages/jazz-tools/src/runtime/db.ts` | App-facing runtime API                |
| `packages/jazz-tools/src/index.ts`      | Main TypeScript export surface        |
| `packages/jazz-tools/src/react/`        | React bindings                        |
| `packages/jazz-tools/src/vue/`          | Vue bindings                          |
| `packages/jazz-tools/src/svelte/`       | Svelte bindings                       |
