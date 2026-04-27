---
"jazz-tools": patch
---

`QuerySubscription` in the Svelte bindings now accepts a getter for both the query and the options, e.g. `new QuerySubscription(() => filter ? app.todos.where({ title: { contains: filter } }) : undefined)`. Reactive reads inside the getter are tracked, so the subscription re-runs when its dependencies change. The bare-value forms continue to work unchanged.
