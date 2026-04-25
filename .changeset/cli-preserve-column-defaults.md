---
"jazz-tools": patch
---

Fix CLI `loadCompiledSchema` dropping column `.default(...)` values, which caused `deploy` to publish a different schema hash than the runtime computed and broke permission checks with `Your declared schema <A> is disconnected from the schema used to enforce permissions: <B>`. The original `wasmSchema` from `defineApp` is now preserved instead of being round-tripped through a lossy AST.
