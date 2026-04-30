# Deep Tri-State Policy Unknown

## What

Make `"unknown"` precise inside relation-backed policy evaluation for consultative `db.canInsert` and `db.canUpdate` checks.

## Notes

The MVP `db.canInsert` and `db.canUpdate` APIs return `true | false | "unknown"`, but they are intentionally pragmatic: they return `"unknown"` only for boundary readiness gaps such as missing target row state, schema, or permissions context.

The existing policy evaluator is boolean and fail-closed. That is correct for enforcement, but it collapses several distinct preflight cases into `false`:

- no matching related row exists
- matching related rows may exist but are not synchronized locally
- a parent row for `Inherits` is missing
- a reverse-reference scan is incomplete
- an `ExistsRel` graph cannot prove completeness from local data

This follow-up makes those distinctions explicit for consultative preflight without weakening enforcement.

Goals:

- Add a deep tri-state evaluator for consultative policy checks.
- Keep write enforcement fail-closed.
- Preserve `false` for proven denial.
- Return `"unknown"` when relation-backed policy truth depends on data the runtime cannot prove complete from local data.
- Share as much code as possible with the current boolean evaluator.

Non-goals:

- No change to public `db.can*` return type.
- No reservation or lease semantics for a `true` decision.
- No requirement to fetch missing dependency data on demand.
- No production-facing denial causes in this slice.

Required design work:

- Define a deep `PolicyDecision` with `Allow`, `Deny`, and `Unknown`.
- Add boolean conversion only at enforcement boundaries:
  - `Allow -> true`
  - `Deny -> false`
  - `Unknown -> false`
- Add expression combinators:
  - `And`: any `Deny` wins; otherwise any `Unknown` wins; otherwise `Allow`
  - `Or`: any `Allow` wins; otherwise any `Unknown` wins; otherwise `Deny`
  - `Not`: `Allow -> Deny`, `Deny -> Allow`, `Unknown -> Unknown`
- Audit existing `return false` paths in:
  - `crates/jazz-tools/src/query_manager/graph_nodes/policy_eval.rs`
  - `crates/jazz-tools/src/query_manager/policy_graph.rs`
  - `crates/jazz-tools/src/query_manager/writes.rs`
  - `crates/jazz-tools/src/query_manager/server_queries.rs`
- Classify each false as:
  - proven denial
  - structural invalidity
  - missing data
  - incomplete dependency scope

Dependency completeness:

- target row materialized locally
- direct parent row materialized locally
- relation scan complete enough for `Exists`
- reverse-reference scan complete enough for `InheritsReferencing`
- relation-IR graph complete enough for `ExistsRel`
- recursive closure bounded and complete for the requested max depth

If completeness cannot be proven, consultative preflight should return `Unknown`.

Implementation steps:

- Add deep `PolicyDecision` and combinators with unit tests.
- Add a tri-state sibling to `PolicyContextEvaluator::evaluate_row_access`.
- Keep existing boolean enforcement paths unchanged until the tri-state path is covered.
- Make preflight call the tri-state evaluator.
- Make enforcement call the boolean evaluator or explicitly collapse `Unknown` to deny.
- Teach `PolicyGraph` and `ExistsOutput` to distinguish "settled false" from "not enough data to settle".
- Make local row loaders report missing data separately from hard delete or structural errors.
- Add ReBAC tests for:
  - `policy.exists(...)` with a missing dependency returning `Unknown`
  - forward `Inherits` missing parent returning `Unknown`
  - reverse `InheritsReferencing` incomplete scan returning `Unknown`
  - `ExistsRel` incomplete relation path returning `Unknown`
  - structurally invalid relation still returning `false`
  - enforcement still rejecting all `Unknown` cases

Verification:

```bash
cargo test -p jazz-tools rebac --lib
cargo test -p jazz-tools query_manager::manager_tests::policies --lib
pnpm --filter jazz-tools test -- packages/jazz-tools/src/runtime
```
