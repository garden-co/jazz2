# Test Catalogue And Strategy

## Current Catalogue

This catalogue was built from test/spec filenames and test declarations across the repo. It is a map of the current surface, not a judgement that every test should stay.

| Area                                            | Files | Approx. tests | Current role                                                                                                               |
| ----------------------------------------------- | ----: | ------------: | -------------------------------------------------------------------------------------------------------------------------- |
| Rust core                                       |    12 |           514 | Core relational semantics, query manager, schema manager, sync manager, auth/server integration, row history, runtime core |
| TypeScript runtime                              |    60 |           561 | Client/runtime behavior, worker bridge, storage modes, transactions, auth state, subscriptions, browser runtime behavior   |
| TypeScript schema/DSL/permissions               |    13 |           141 | DSL typing and query APIs, generated/typed apps, permissions expression behavior, schema wire format                       |
| Framework adapters                              |    18 |           107 | React, React core, React Native, Expo, Svelte, Vue client/provider hooks and runtime adapters                              |
| `create-jazz`                                   |     7 |            55 | Scaffolding, package manager detection, hosted cloud provisioning/env init, CLI integration                                |
| Inspector                                       |    14 |            68 | Inspector routes, data explorer components, live query UI, standalone browser inspector                                    |
| Examples and starters                           |    72 |           199 | Todo/chat/game/example flows, starter app e2e smoke tests, docs examples, walkthrough screenshots                          |
| Dev tools, MCP, Better Auth, CLI, orchestration |    33 |           393 | Package-side utilities, CLI, dev server plugins, MCP backends/server, Better Auth adapter, subscription orchestration      |
| Benchmarks and stress                           |     4 |            12 | Realistic performance scenarios and stress-test config                                                                     |
| Docs and root utilities                         |     4 |            16 | Presentation deck helper and root SVG plotter tests                                                                        |

Total: 237 test/spec files, about 2,066 declared tests.

## Current Test Shapes

### Rust core

Primary files:

- `crates/jazz-tools/src/query_manager/manager_tests.rs`
- `crates/jazz-tools/src/query_manager/rebac_tests.rs`
- `crates/jazz-tools/src/query_manager/types/tests.rs`
- `crates/jazz-tools/src/runtime_core/tests.rs`
- `crates/jazz-tools/src/schema_manager/integration_tests.rs`
- `crates/jazz-tools/src/sync_manager/tests.rs`
- `crates/jazz-tools/src/test_row_history.rs`
- `crates/jazz-tools/tests/auth_test.rs`
- `crates/jazz-tools/tests/test_server.rs`

This is the real semantic backbone. It has good volume, but the boundaries are blurry: some files mix narrow unit-style cases with multi-client behavior, schema evolution, sync, persistence, and policy scenarios. `runtime_core/tests.rs`, `schema_manager/integration_tests.rs`, and `query_manager/manager_tests.rs` are especially dense and should be split by contract rather than by implementation module.

### TypeScript runtime

Primary clusters:

- Node/runtime tests in `packages/jazz-tools/src/runtime/*.test.ts`
- Browser runtime tests in `packages/jazz-tools/tests/browser/*.test.ts(x)`
- NAPI/WASM/bootstrap tests in `packages/jazz-tools/src/runtime/napi.*.test.ts`, `client.node-wasm-init.test.ts`, and browser worker/bootstrap tests

This suite checks whether the Rust-backed runtime is usable from TypeScript. It is valuable, but it overlaps heavily with Rust semantics in places. The highest-value tests here are boundary tests: conversion, runtime initialization, storage scope, worker bridge, transactions as observed through the public TS API, auth/session state, subscriptions, and browser-only coordination.

### TypeScript schema, DSL, and permissions

Primary clusters:

- `packages/jazz-tools/tests/ts-dsl/*.test.ts`
- `packages/jazz-tools/src/permissions/*.test.ts`
- `packages/jazz-tools/src/dsl.test.ts`
- `packages/jazz-tools/src/schema-permissions.test.ts`
- `packages/jazz-tools/src/drivers/schema-wire.test.ts`

This suite is the contract for the app author API. It should stay close to realistic app code and type-level expectations. It should not duplicate every Rust query case; it should prove that the TS DSL generates the intended contracts and preserves typing.

### Framework adapters

Primary clusters:

- React and React core tests in `packages/jazz-tools/src/react*`
- React Native and Expo tests in `packages/jazz-tools/src/react-native` and `packages/jazz-tools/src/expo`
- Svelte tests in `packages/jazz-tools/src/svelte`
- Vue tests in `packages/jazz-tools/src/vue`

These should mostly be adapter contract tests: provider lifecycle, auth state propagation, subscriptions/hooks, runtime adapter compatibility, and framework-specific integration edge cases. They should be small in number and high signal.

### Product and tooling surfaces

Primary clusters:

- `packages/create-jazz/src/*.test.ts`
- `packages/jazz-tools/src/dev/*.test.ts`
- `packages/jazz-tools/src/mcp/*.test.ts`
- `packages/jazz-tools/src/better-auth-adapter/*.test.ts`
- `packages/jazz-tools/src/cli.test.ts`
- `packages/inspector/**/*.test.ts(x)` and `packages/inspector/tests/browser/*.spec.ts`

These are separate products riding on the core. They should have their own user-facing workflow smoke tests plus focused tests for parsing, generated output, and adapter-specific policy.

### Examples, starters, docs, benchmarks, stress

Primary clusters:

- Starter Playwright e2e suites under `starters/*/e2e`
- Example app browser and e2e tests under `examples/*`
- Docs example tests under `examples/docs/*`
- Benchmarks under `benchmarks/realistic`
- Root/document helper tests under `tests/` and `docs/lib`

These tests are currently doing three jobs: validating examples, validating starter templates, and providing end-to-end product confidence. That is too much for an undifferentiated bucket. They should become explicit smoke/canary suites with a small shared matrix, while deeper behavior moves into core/runtime tests.

## Problems To Solve

1. Test intent is not encoded in location or name.
   A reader cannot reliably tell whether a test is a core semantic contract, a regression repro, a framework adapter contract, an example smoke test, or a benchmark guard.

2. Several suites duplicate behavior at different layers.
   Some Rust, TS runtime, browser, and example tests appear to cover the same relational behaviors. Duplication is useful only when each layer catches a different class of failure.

3. The highest-value behavior is not represented as an explicit coverage matrix.
   Core invariants like relational visibility, permissions, schema evolution, history, sync/reconnect, storage durability, transactions, and auth/session behavior should have named coverage owners.

4. Example and starter tests are carrying too much semantic responsibility.
   They are good canaries, but they are expensive and noisy places to assert core database semantics.

5. Benchmarks and stress tests are mixed into the mental model of correctness tests.
   They should remain visible, but be treated as performance/regression gates with different failure triage.

6. A few root/doc utility tests sit outside package ownership.
   They may be valid, but they need an owner and a reason to exist in repo-wide CI.

## Proposed Structure

Use test tiers that describe intent first, then runner.

### Tier 0: Pure Units

Purpose: tiny deterministic functions where a direct unit test is clearer than an integration scenario.

Allowed examples: value converters, route-line geometry, package-manager detection, serialization helpers.

Rule: no mocks of major system collaborators. If the fixture needs a fake database, it is not Tier 0.

### Tier 1: Semantic Core Contracts

Purpose: relational database correctness independent of TypeScript/framework packaging.

Owned by Rust tests.

Coverage owners:

- Query semantics: filters, ordering, limits, joins/relations, magic columns, visibility
- Permissions/ReBAC: allow/deny, inherited policies, anonymous behavior, provenance
- Schema lifecycle: declaration, migration/evolution, renames, compatibility, schema hashes
- Row history: encode/decode, update/delete history, conflict representation
- Transactions and batches: local staging, settlement, rejection, idempotency
- Sync protocol: reconnect, dedupe, outbox behavior, multi-client propagation
- Storage: persistence, recovery, retained local batch records

### Tier 2: Runtime Boundary Contracts

Purpose: prove the public TypeScript runtime faithfully exposes the core across Node, browser, WASM, NAPI, workers, and storage modes.

Owned by `packages/jazz-tools/src/runtime` and `packages/jazz-tools/tests/browser`.

Coverage owners:

- Runtime initialization and build output
- NAPI/WASM loading and failure modes
- Public `db` behavior observed from TypeScript
- Browser storage scope and tab leadership
- Worker bridge and message races
- Auth state, secret stores, recovery phrase/passkey backup
- Subscription delivery and sorting through public APIs

### Tier 3: App Author API Contracts

Purpose: prove that Jazz feels correct to application authors.

Owned by TS DSL, permissions DSL, schema wire, and framework adapter tests.

Coverage owners:

- Type inference and compile-time guarantees
- Generated app shape
- Query/insert/update/delete API ergonomics
- Permission DSL expressiveness and failure modes
- Framework provider/hook lifecycle
- Adapter-specific runtime integration

### Tier 4: Product Workflow Smoke Tests

Purpose: prove shipped surfaces work end to end without exhaustively retesting core semantics.

Owned by `create-jazz`, inspector, starters, docs examples, and selected example apps.

Coverage owners:

- `create-jazz` can scaffold each supported starter family
- Each starter family has one representative todo/auth backup e2e path
- Inspector can connect, inspect schema/data, and mutate rows
- Docs examples compile and one or two canonical snippets run
- Example apps have one smoke path per app-specific feature, not per database invariant

### Tier 5: Performance, Stress, And Visual Canaries

Purpose: detect performance cliffs, long-running reliability issues, and visual/document regressions.

Owned by benchmarks, stress tests, walkthrough screenshots, root/doc utility tests.

Coverage owners:

- Realistic benchmark scenarios
- Browser stress scenarios
- Screenshot/walkthrough canaries
- Documentation rendering utilities

## Coverage Matrix To Build

Create a table where every important behavior has exactly one primary owner and zero or more boundary/canary owners.

Initial rows:

| Behavior                          | Primary owner                 | Boundary/canary owners                                     |
| --------------------------------- | ----------------------------- | ---------------------------------------------------------- |
| Basic CRUD visibility             | Rust core                     | TS runtime, one starter todo e2e                           |
| Query filters/order/limit         | Rust query manager            | TS DSL query API, browser subscribeAll                     |
| Relations/subgraphs               | Rust query/schema manager     | TS DSL generated app                                       |
| Permission allow/deny             | Rust ReBAC                    | TS permissions DSL, browser anonymous/write-denied         |
| Auth/session transitions          | TS runtime                    | framework adapters, auth starters                          |
| Schema declaration/evolution      | Rust schema manager           | TS schema wire, migrations DSL                             |
| Row history encode/decode         | Rust row history/runtime core | TS value/row transformer                                   |
| Local-first persistence           | Rust runtime/storage          | browser persisted/storage-isolation                        |
| Worker bridge/tab leadership      | TS browser runtime            | selected app smoke                                         |
| Sync/reconnect/outbox             | Rust sync/runtime core        | browser worker bridge, future websocket/multi-server tests |
| Transactions/batches              | Rust runtime core             | TS `db.transaction`                                        |
| NAPI/WASM packaging               | TS runtime                    | package build/CI                                           |
| Framework hook/provider lifecycle | framework adapter tests       | starter e2e smoke                                          |
| Scaffolding                       | `create-jazz`                 | one generated starter e2e                                  |
| Inspector workflows               | inspector tests               | standalone inspector browser test                          |

## Migration Plan

### Phase 1: Label and inventory

Add lightweight metadata to test files using naming and directory conventions before moving code:

- `*.unit.test.ts` / small Rust module tests for Tier 0
- `*.contract.test.ts` or Rust contract modules for Tiers 1-3
- `*.integration.test.ts` for multi-component boundaries
- `*.e2e.spec.ts` for Playwright product flows
- `*.bench.test.*` and `*.stress.test.*` for Tier 5

Then generate a committed catalogue from file paths, runner, tier, package, and rough behavior tags.

### Phase 2: Define primary owners

For each row in the coverage matrix, choose one primary suite. When a behavior already has duplicate coverage, keep the lowest reliable layer as the semantic owner and demote higher-layer copies to smoke tests.

Expected moves:

- Core relational semantics move toward Rust contract tests.
- TS runtime keeps public API boundary tests, not duplicate engine cases.
- Framework tests keep lifecycle and adapter behavior, not database semantics.
- Starters/examples keep one happy-path smoke per user workflow.

### Phase 3: Prune or rewrite low-signal tests

For each test file, assign one of:

- Keep: covers a named contract in its owning tier.
- Move: valuable behavior, wrong location/tier.
- Collapse: duplicates another test but adds useful setup or fixture data.
- Delete: only asserts implementation trivia, snapshots churn, or duplicates a stronger test.
- Promote: currently a narrow repro that should become a named contract.

Do this file-by-file and require a short reason in the pruning PR.

### Phase 4: Fill explicit gaps

Known gaps from current repo notes:

- Multi-server sync coverage is tracked in `todo/issues/test_multi-server-sync.md`.
- WebSocket transport integration coverage is tracked in `todo/issues/test_websocket-transport-integration.md`.
- Expo Android websocket e2e is tracked in `todo/issues/expo-android-maestro-e2e-ws-unverified.md`.
- Sync protocol reliability appears as an MVP idea in `todo/ideas/1_mvp/sync-protocol-reliability.md`.

Turn those into contract rows before adding more ad hoc tests.

### Phase 5: CI lanes

Split CI by intent:

- Fast contract lane: Rust core + Node TS runtime/DSL.
- Browser boundary lane: Vitest browser runtime + framework adapter browser tests.
- Product smoke lane: selected starters, inspector, create-jazz.
- Examples/docs lane: example app smoke and docs examples.
- Performance/stress lane: scheduled or opt-in unless explicitly promoted to release gate.

The goal is not fewer tests by default. The goal is that every test has a job, an owner, and a clear reason to fail CI.

## First Pass Decisions

Start with these refactors because they should clarify the most with the least risk:

1. Split `crates/jazz-tools/src/runtime_core/tests.rs` by contract area: transactions/batches, storage/recovery, sync, schema evolution, permissions, history.
2. Split `crates/jazz-tools/src/query_manager/manager_tests.rs` into query semantics groups with one realistic domain fixture per group.
3. Mark TS runtime tests that duplicate Rust semantics and decide whether they are boundary tests or deletions.
4. Reduce starter e2e suites to a matrix: framework family x auth mode x runtime mode, with one canonical todo flow and one backup/auth flow where applicable.
5. Move example-specific behavior out of generic confidence accounting; examples should prove their own app behavior and act as smoke tests for docs.
6. Add the coverage matrix as the review checklist for every new feature touching relational semantics.

## First Pass Progress

Completed:

- Split `crates/jazz-tools/src/runtime_core/tests.rs` into shared fixtures plus contract modules:
  - `runtime_core/tests/install_transport_tests.rs`
  - `runtime_core/tests/basic.rs`
  - `runtime_core/tests/sync_replay.rs`
  - `runtime_core/tests/write_batch.rs`
  - `runtime_core/tests/write_batch/direct.rs`
  - `runtime_core/tests/write_batch/storage_flush.rs`
  - `runtime_core/tests/write_batch/persisted.rs`
  - `runtime_core/tests/write_batch/transactional.rs`
  - `runtime_core/tests/write_batch/rejection_recovery.rs`
  - `runtime_core/tests/query_subscription.rs`
  - `runtime_core/tests/schema_catalogue.rs`
  - `runtime_core/tests/fk_remove_error.rs`
- Verified with `cargo test -p jazz-tools runtime_core::tests --features test --lib`.
- Split `crates/jazz-tools/src/query_manager/manager_tests.rs` into shared fixtures plus behavior modules:
  - `manager_tests/bootstrap.rs`
  - `manager_tests/json_storage.rs`
  - `manager_tests/crud_queries.rs`
  - `manager_tests/updates.rs`
  - `manager_tests/deletes.rs`
  - `manager_tests/recursive_queries.rs`
  - `manager_tests/joins.rs`
  - `manager_tests/array_subqueries.rs`
  - `manager_tests/policies.rs`
  - `manager_tests/branches.rs`
  - `manager_tests/contributing_ids.rs`
  - `manager_tests/server_subscriptions.rs`
  - `manager_tests/subscriptions.rs`
  - `manager_tests/e2e_sync.rs`
  - `manager_tests/client_lifecycle.rs`
  - `manager_tests/misc.rs`
- Verified with `cargo test -p jazz-tools query_manager::manager_tests --features test --lib`.
- Split `crates/jazz-tools/src/query_manager/rebac_tests.rs` into shared fixtures plus policy modules:
  - `rebac_tests/insert_policies.rs`
  - `rebac_tests/inherited_policies.rs`
  - `rebac_tests/exists_policies.rs`
  - `rebac_tests/exists_rel_policies.rs`
  - `rebac_tests/mutations.rs`
  - `rebac_tests/select_policies.rs`
  - `rebac_tests/recursive_inheritance.rs`
  - `rebac_tests/magic_provenance.rs`
  - `rebac_tests/inheritance_validation.rs`
  - `rebac_tests/declared_fk_inheritance.rs`
- Verified with `cargo test -p jazz-tools query_manager::rebac_tests --features test --lib`.
- Split `crates/jazz-tools/src/sync_manager/tests.rs` into shared fixtures plus sync behavior modules:
  - `sync_manager/tests/basic.rs`
  - `sync_manager/tests/query_scope.rs`
  - `sync_manager/tests/subscriptions.rs`
  - `sync_manager/tests/row_batch_state.rs`
  - `sync_manager/tests/settlements.rs`
  - `sync_manager/tests/transaction_sealing.rs`
  - `sync_manager/tests/stale_replay.rs`
  - `sync_manager/tests/server_sync.rs`
  - `sync_manager/tests/client_lifecycle.rs`
  - `sync_manager/tests/permissions.rs`
- Verified with `cargo test -p jazz-tools sync_manager::tests --features test --lib`.
- Split `crates/jazz-tools/src/schema_manager/integration_tests.rs` into shared fixtures plus integration modules:
  - `schema_manager/integration_tests/tests/catalogue.rs`
  - `schema_manager/integration_tests/tests/migration.rs`
  - `schema_manager/integration_tests/tests/renames.rs`
  - `schema_manager/integration_tests/tests/lenses.rs`
  - `schema_manager/integration_tests/tests/writes.rs`
  - `schema_manager/integration_tests/tests/query_subscription.rs`
  - `schema_manager/integration_tests/tests/sync.rs`
  - `schema_manager/integration_tests/tests/misc.rs`
- Verified with `cargo test -p jazz-tools schema_manager::integration_tests --features test --lib`.
- Verified the full Rust lib test suite for `jazz-tools` with `cargo test -p jazz-tools --features test --lib`.

## Role Assignments After First Reorg

These assignments are the first concrete interpretation of the tier model above. They are intentionally descriptive rather than restrictive: new tests can still move as behavior ownership becomes clearer.

### Rust core contracts

The split Rust modules are the primary owners for relational semantics and sync/runtime invariants:

- `runtime_core/tests/basic.rs`: broad runtime smoke and user-subscription behavior that does not fit a narrower runtime contract yet.
- `runtime_core/tests/query_subscription.rs`: query visibility, durability-tier reads, local overlays, settled/remote tier behavior, and subscription updates observed through `RuntimeCore`.
- `runtime_core/tests/schema_catalogue.rs`: schema catalogue publishing, schema evolution, old/new schema compatibility, and schema sync behavior.
- `runtime_core/tests/sync_replay.rs`: reconnect and query replay behavior.
- `runtime_core/tests/fk_remove_error.rs`: focused regression contracts around FK-related removal/update edge cases.
- `runtime_core/tests/write_batch/*`: direct, persisted, transactional, rejection/recovery, and storage-flush batch behavior. This is the primary contract owner for transaction and settlement semantics at the runtime boundary.
- `query_manager/manager_tests/*`: query semantics, graph behavior, subscriptions, server sync, CRUD, branches, and query-manager lifecycle.
- `query_manager/rebac_tests/*`: permission/ReBAC policy semantics. TS permissions tests should prove DSL shape and translation, not re-own these semantics.
- `sync_manager/tests/*`: sync-manager state propagation, query scopes, subscriptions, row-batch state, settlements, transaction sealing, stale replay, server sync, client lifecycle, and permission handoff.
- `schema_manager/integration_tests/tests/*`: schema lifecycle, catalogue integration, migrations, lenses, renames, query integration, sync, and write behavior.

### TypeScript runtime boundary contracts

The newly split TS runtime directories should stay focused on public TypeScript boundary behavior:

- `packages/jazz-tools/src/runtime/query-adapter-tests/*`: primary owner for query-builder JSON to runtime query-shape translation. These tests should use app-shaped fixtures and should not grow into Rust query-engine semantic cases.
- `packages/jazz-tools/src/runtime/client-tests/for-request.test.ts`: request-scoped client behavior, backend mode validation, JWT/session extraction, query/subscribe forwarding, subscription callback payload normalization, query options, and two-phase subscribe lifecycle.
- `packages/jazz-tools/src/runtime/client-tests/support.ts`: local fixtures only for the two client-test suites. If another runtime suite needs these helpers, decide whether the helper belongs in `runtime/testing` before widening it.
- `packages/jazz-tools/src/runtime/schema-order-tests/client-boundary.test.ts`: public `JazzClient` row/value alignment across create, query, included relation rows, magic projections, and subscription deltas.
- `packages/jazz-tools/src/runtime/schema-order-tests/db-boundary.test.ts`: typed `Db` alignment, generated-schema fallback, caller-supplied mutation metadata, and client-backed DB forwarding.
- `packages/jazz-tools/src/runtime/row-transformer.test.ts`: isolated value unwrapping and row transformation helper behavior. This should not own public runtime or `Db` boundary contracts.

### First cleanup target

Start the first non-move-only cleanup around runtime client duplication:

1. Compare `runtime/schema-order-tests/client-boundary.test.ts`, `runtime/schema-order-tests/db-boundary.test.ts`, `runtime/row-transformer.test.ts`, and Rust client/query alignment tests.
2. Keep one primary owner for schema-order semantics at each layer:
   - Rust for core row alignment semantics.
   - TS runtime for public client API boundary behavior.
   - `row-transformer` only for isolated conversion helpers.
3. Convert any duplicate TS runtime assertions into either adapter-specific boundary checks or deletion candidates.
4. Do not delete anything until the PR names the stronger owner and explains what failure class would still be caught.

Use the same pattern for `forRequest`/NAPI later: `client-tests/for-request.test.ts` should own public request-scoped client behavior, while `napi.for-request.test.ts` should only own NAPI-specific runtime wiring.

## Appendix: File Inventory

This inventory includes test, spec, benchmark-test, config, and test-support files discovered by filename. The pruning pass should mark support/config files separately from executable test files.

### Rust core and native packages

- `crates/jazz-rn/src/__tests__/index.test.tsx`
- `crates/jazz-tools/src/query_manager/manager_tests.rs`
- `crates/jazz-tools/src/query_manager/rebac_tests.rs`
- `crates/jazz-tools/src/query_manager/types/tests.rs`
- `crates/jazz-tools/src/runtime_core/tests.rs`
- `crates/jazz-tools/src/schema_manager/integration_tests.rs`
- `crates/jazz-tools/src/server/testing.rs`
- `crates/jazz-tools/src/sync_manager/tests.rs`
- `crates/jazz-tools/src/test_row_history.rs`
- `crates/jazz-tools/tests/auth_test.rs`
- `crates/jazz-tools/tests/test_server.rs`
- `vendor/librocksdb-sys/src/test.rs`

### TypeScript runtime

- `packages/jazz-tools/src/runtime/anonymous-write-denied-error.test.ts`
- `packages/jazz-tools/src/runtime/auth-secret-store.test.ts`
- `packages/jazz-tools/src/runtime/auth-state.test.ts`
- `packages/jazz-tools/src/runtime/client-session.test.ts`
- `packages/jazz-tools/src/runtime/client.browser-assets.test.ts`
- `packages/jazz-tools/src/runtime/client.build-output.test.ts`
- `packages/jazz-tools/src/runtime/client-tests/for-request.test.ts`
- `packages/jazz-tools/src/runtime/client-tests/support.ts`
- `packages/jazz-tools/src/runtime/client.mutations.test.ts`
- `packages/jazz-tools/src/runtime/client.node-wasm-init.test.ts`
- `packages/jazz-tools/src/runtime/client.test.ts`
- `packages/jazz-tools/src/runtime/db.anonymous.test.ts`
- `packages/jazz-tools/src/runtime/db.auth-state.test.ts`
- `packages/jazz-tools/src/runtime/db.browser-storage-scope.test.ts`
- `packages/jazz-tools/src/runtime/db.dev-mode.test.ts`
- `packages/jazz-tools/src/runtime/db.driver-mode.test.ts`
- `packages/jazz-tools/src/runtime/db.local-first-auth.test.ts`
- `packages/jazz-tools/src/runtime/db.persisted.test.ts`
- `packages/jazz-tools/src/runtime/db.transaction.test.ts`
- `packages/jazz-tools/src/runtime/db.transport.test.ts`
- `packages/jazz-tools/src/runtime/db.worker-bootstrap.test.ts`
- `packages/jazz-tools/src/runtime/file-storage.test.ts`
- `packages/jazz-tools/src/runtime/introspection-fetch.ts`
- `packages/jazz-tools/src/runtime/napi.auth-failure.test.ts`
- `packages/jazz-tools/src/runtime/napi.for-request.test.ts`
- `packages/jazz-tools/src/runtime/napi.integration.test.ts`
- `packages/jazz-tools/src/runtime/passkey-backup.test.ts`
- `packages/jazz-tools/src/runtime/permissions.repro.test.ts`
- `packages/jazz-tools/src/runtime/query-adapter-tests/basic-query-structure.test.ts`
- `packages/jazz-tools/src/runtime/query-adapter-tests/condition-translation.test.ts`
- `packages/jazz-tools/src/runtime/query-adapter-tests/error-handling.test.ts`
- `packages/jazz-tools/src/runtime/query-adapter-tests/full-query-translation.test.ts`
- `packages/jazz-tools/src/runtime/query-adapter-tests/include-translation.test.ts`
- `packages/jazz-tools/src/runtime/query-adapter-tests/orderby-translation.test.ts`
- `packages/jazz-tools/src/runtime/query-adapter-tests/self-referential-relations.test.ts`
- `packages/jazz-tools/src/runtime/query-adapter-tests/support.ts`
- `packages/jazz-tools/src/runtime/recovery-phrase.integration.test.ts`
- `packages/jazz-tools/src/runtime/recovery-phrase.test.ts`
- `packages/jazz-tools/src/runtime/row-transformer.test.ts`
- `packages/jazz-tools/src/runtime/schema-fetch.test.ts`
- `packages/jazz-tools/src/runtime/schema-marshalling.abstract-bench.test.ts`
- `packages/jazz-tools/src/runtime/schema-order-tests/client-boundary.test.ts`
- `packages/jazz-tools/src/runtime/schema-order-tests/db-boundary.test.ts`
- `packages/jazz-tools/src/runtime/subscription-manager.test.ts`
- `packages/jazz-tools/src/runtime/tab-leader-election.test.ts`
- `packages/jazz-tools/src/runtime/testing/napi-runtime-test-utils.ts`
- `packages/jazz-tools/src/runtime/testing/wasm-runtime-test-utils.ts`
- `packages/jazz-tools/src/runtime/url.test.ts`
- `packages/jazz-tools/src/runtime/value-converter.test.ts`
- `packages/jazz-tools/src/runtime/worker-bridge.race-harness.test.ts`
- `packages/jazz-tools/src/runtime/worker-bridge.test.ts`
- `packages/jazz-tools/tests/browser/db.all.test.ts`
- `packages/jazz-tools/tests/browser/db.auth-refresh.test.ts`
- `packages/jazz-tools/tests/browser/db.auth-refresh.worker.test.ts`
- `packages/jazz-tools/tests/browser/db.storage-isolation.test.ts`
- `packages/jazz-tools/tests/browser/db.subscribeAll.sort.test.ts`
- `packages/jazz-tools/tests/browser/db.subscribeAll.test.ts`
- `packages/jazz-tools/tests/browser/db.transaction-reads.test.ts`
- `packages/jazz-tools/tests/browser/history-conflict.test.ts`
- `packages/jazz-tools/tests/browser/leader-lock.test.ts`
- `packages/jazz-tools/tests/browser/react-core-provider.test.tsx`
- `packages/jazz-tools/tests/browser/realistic-bench.test.ts`
- `packages/jazz-tools/tests/browser/schema-marshalling.abstract-bench.test.ts`
- `packages/jazz-tools/tests/browser/tab-leader-election.test.ts`
- `packages/jazz-tools/tests/browser/testing-server-node.ts`
- `packages/jazz-tools/tests/browser/testing-server.ts`
- `packages/jazz-tools/tests/browser/useAll.test.tsx`
- `packages/jazz-tools/tests/browser/useAllSuspense.test.tsx`
- `packages/jazz-tools/tests/browser/worker-bridge.test.ts`

### TypeScript schema, DSL, and permissions

- `packages/jazz-tools/src/drivers/schema-wire.test.ts`
- `packages/jazz-tools/src/dsl.test.ts`
- `packages/jazz-tools/src/permissions/index.test.ts`
- `packages/jazz-tools/src/permissions/type-inference.test.ts`
- `packages/jazz-tools/src/schema-permissions.test.ts`
- `packages/jazz-tools/tests/ts-dsl/delete-api.test.ts`
- `packages/jazz-tools/tests/ts-dsl/generated-app.test.ts`
- `packages/jazz-tools/tests/ts-dsl/insert-api.test.ts`
- `packages/jazz-tools/tests/ts-dsl/migrations.test.ts`
- `packages/jazz-tools/tests/ts-dsl/query-api.test.ts`
- `packages/jazz-tools/tests/ts-dsl/typed-app.circular.test.ts`
- `packages/jazz-tools/tests/ts-dsl/typed-app.test.ts`
- `packages/jazz-tools/tests/ts-dsl/update-api.test.ts`

### Framework adapters

- `packages/jazz-tools/src/expo/auth-secret-store.test.ts`
- `packages/jazz-tools/src/react-core/provider.onjwtexpired.test.tsx`
- `packages/jazz-tools/src/react-core/test-utils.ts`
- `packages/jazz-tools/src/react-core/use-auth-state.test.tsx`
- `packages/jazz-tools/src/react-core/use-local-first-auth.test.tsx`
- `packages/jazz-tools/src/react-native/create-jazz-client.test.ts`
- `packages/jazz-tools/src/react-native/db.test.ts`
- `packages/jazz-tools/src/react-native/jazz-rn-runtime-adapter.test.ts`
- `packages/jazz-tools/src/react/create-jazz-client.integration.test.ts`
- `packages/jazz-tools/src/react/create-jazz-client.test.ts`
- `packages/jazz-tools/src/svelte/compiler-warnings.svelte.test.ts`
- `packages/jazz-tools/src/svelte/context.svelte.test.ts`
- `packages/jazz-tools/src/svelte/create-jazz-client.test.ts`
- `packages/jazz-tools/src/svelte/rune-patterns.svelte.test.ts`
- `packages/jazz-tools/src/svelte/test-helpers.svelte.ts`
- `packages/jazz-tools/src/vue/create-jazz-client.integration.test.ts`
- `packages/jazz-tools/src/vue/create-jazz-client.test.ts`
- `packages/jazz-tools/src/vue/use-all.test.ts`

### Product and tooling surfaces

- `packages/create-jazz/src/cli.integration.test.ts`
- `packages/create-jazz/src/cloud-env.test.ts`
- `packages/create-jazz/src/cloud-init.test.ts`
- `packages/create-jazz/src/cloud-provision.test.ts`
- `packages/create-jazz/src/detect-pm.test.ts`
- `packages/create-jazz/src/scaffold.test.ts`
- `packages/create-jazz/vitest.config.ts`
- `packages/inspector/src/App.test.tsx`
- `packages/inspector/src/components/data-explorer/RowMutationSidebar.test.tsx`
- `packages/inspector/src/components/data-explorer/TableDataGrid.test.tsx`
- `packages/inspector/src/components/data-explorer/TableFilterBuilder.test.tsx`
- `packages/inspector/src/components/data-explorer/TableSchemaDefinition.test.tsx`
- `packages/inspector/src/components/data-explorer/row-mutation-form.test.ts`
- `packages/inspector/src/components/inspector-layout/index.test.tsx`
- `packages/inspector/src/pages/data-explorer/index.test.tsx`
- `packages/inspector/src/pages/live-query/LiveQueryFilters.test.tsx`
- `packages/inspector/src/pages/live-query/index.test.tsx`
- `packages/inspector/src/routes.test.tsx`
- `packages/inspector/tests/browser/standalone-inspector.spec.ts`
- `packages/inspector/tests/browser/test-constants.ts`
- `packages/inspector/vitest.config.ts`
- `packages/jazz-tools/src/backend/create-jazz-context.test.ts`
- `packages/jazz-tools/src/backend/request-auth.test.ts`
- `packages/jazz-tools/src/better-auth-adapter/index.test.ts`
- `packages/jazz-tools/src/better-auth-adapter/schema.test.ts`
- `packages/jazz-tools/src/better-auth-adapter/utils.test.ts`
- `packages/jazz-tools/src/browser-benchmark-mode.test.ts`
- `packages/jazz-tools/src/cli.test.ts`
- `packages/jazz-tools/src/dev-tools/dev-tools.test.ts`
- `packages/jazz-tools/src/dev/dev-server.test.ts`
- `packages/jazz-tools/src/dev/expo.test.ts`
- `packages/jazz-tools/src/dev/inspector-link.ts`
- `packages/jazz-tools/src/dev/next.test.ts`
- `packages/jazz-tools/src/dev/schema-watcher.test.ts`
- `packages/jazz-tools/src/dev/sveltekit.test.ts`
- `packages/jazz-tools/src/dev/test-helpers.ts`
- `packages/jazz-tools/src/dev/vite.test.ts`
- `packages/jazz-tools/src/mcp/backend-naive.test.ts`
- `packages/jazz-tools/src/mcp/backend-sqlite.test.ts`
- `packages/jazz-tools/src/mcp/build-index.test.ts`
- `packages/jazz-tools/src/mcp/integration.test.ts`
- `packages/jazz-tools/src/mcp/server.test.ts`
- `packages/jazz-tools/src/reconcile-array.test.ts`
- `packages/jazz-tools/src/subscriptions-orchestrator.integration.test.ts`
- `packages/jazz-tools/src/subscriptions-orchestrator.test.ts`
- `packages/jazz-tools/src/testing/index.test.ts`
- `packages/jazz-tools/src/testing/policy-test-app.ts`
- `packages/jazz-tools/src/testing/relation-ir-test-helpers.ts`
- `packages/jazz-tools/src/worker/jazz-worker.test.ts`
- `packages/jazz-tools/test-support/jazz-rn-vitest-stub.ts`
- `packages/jazz-tools/vitest.config.browser.ts`
- `packages/jazz-tools/vitest.config.react.ts`
- `packages/jazz-tools/vitest.config.svelte.ts`
- `packages/jazz-tools/vitest.config.ts`

### Examples, starters, docs, benchmarks, and root utilities

- `benchmarks/realistic/ci_benchmarks.test.mjs`
- `benchmarks/realistic/ci_scenarios.test.mjs`
- `benchmarks/realistic/update_history.test.mjs`
- `docs/lib/presentation-deck.test.ts`
- `examples/auth-betterauth-chat/e2e/chat-auth.spec.ts`
- `examples/auth-betterauth-chat/vitest.config.ts`
- `examples/auth-simple-chat/e2e/chat-auth.spec.ts`
- `examples/chat-react/tests/browser/canvas.test.tsx`
- `examples/chat-react/tests/browser/chat-app.test.tsx`
- `examples/chat-react/tests/browser/chat-settings.test.tsx`
- `examples/chat-react/tests/browser/invite.test.tsx`
- `examples/chat-react/tests/browser/profile.test.tsx`
- `examples/chat-react/tests/browser/send-permission.test.tsx`
- `examples/chat-react/tests/browser/test-constants.ts`
- `examples/chat-react/tests/browser/uploads.test.tsx`
- `examples/chat-react/vitest.config.browser.ts`
- `examples/chat-react/walkthrough/screenshots.test.ts`
- `examples/docs/todo-client-localfirst-react/tests/browser/auth-snippets.test.tsx`
- `examples/docs/todo-client-localfirst-react/tests/browser/test-constants.ts`
- `examples/docs/todo-client-localfirst-react/tests/browser/todo-app.test.tsx`
- `examples/docs/todo-client-localfirst-react/vitest.config.browser.ts`
- `examples/docs/todo-client-localfirst-ts/tests/browser/files-and-blobs-snippets.test.ts`
- `examples/docs/todo-client-localfirst-ts/tests/browser/test-constants.ts`
- `examples/docs/todo-client-localfirst-ts/tests/browser/todo-app.test.ts`
- `examples/docs/todo-client-localfirst-ts/vitest.config.browser.ts`
- `examples/docs/todo-server-ts/tests/backend-context.test.ts`
- `examples/docs/todo-server-ts/tests/integration.test.ts`
- `examples/docs/todo-server-ts/vitest.config.ts`
- `examples/moon-lander-react/tests/browser/multiplayer.test.tsx`
- `examples/moon-lander-react/tests/browser/soak.test.tsx`
- `examples/moon-lander-react/tests/browser/test-constants.ts`
- `examples/moon-lander-react/tests/browser/test-helpers.ts`
- `examples/moon-lander-react/tests/browser/writes.test.ts`
- `examples/moon-lander-react/vitest.config.browser.ts`
- `examples/moon-lander-react/walkthrough/screenshots.test.ts`
- `examples/nextjs-csr-ssr/e2e/csr-ssr.spec.ts`
- `examples/todo-client-localfirst-react/tests/browser/test-constants.ts`
- `examples/todo-client-localfirst-react/tests/browser/todo-app.test.tsx`
- `examples/todo-client-localfirst-react/vitest.config.browser.ts`
- `examples/todo-client-localfirst-svelte/tests/browser/test-constants.ts`
- `examples/todo-client-localfirst-svelte/tests/browser/todo-app.test.ts`
- `examples/todo-client-localfirst-svelte/vitest.config.browser.ts`
- `examples/todo-client-localfirst-ts/tests/browser/test-constants.ts`
- `examples/todo-client-localfirst-ts/tests/browser/todo-app.test.ts`
- `examples/todo-client-localfirst-ts/vitest.config.browser.ts`
- `examples/todo-server-ts/tests/integration.test.ts`
- `examples/todo-server-ts/vitest.config.ts`
- `examples/wequencer/e2e/app.test.ts`
- `examples/wequencer/e2e/beats.test.ts`
- `examples/wequencer/e2e/instruments.test.ts`
- `examples/wequencer/e2e/participants.test.ts`
- `examples/wequencer/e2e/sync.test.ts`
- `examples/wequencer/e2e/transport.test.ts`
- `examples/wequencer/src/jam.test.ts`
- `examples/wequencer/vitest.config.browser.ts`
- `examples/wequencer/walkthrough/screenshots.test.ts`
- `examples/world-tour/src/lib/__tests__/calendar-grid.test.ts`
- `examples/world-tour/src/lib/__tests__/nearest-stop.test.ts`
- `examples/world-tour/src/lib/__tests__/route-line.test.ts`
- `examples/world-tour/vitest.config.ts`
- `examples/world-tour/walkthrough/screenshots.test.ts`
- `starters/next-betterauth/e2e/todo-flow.spec.ts`
- `starters/next-hybrid/e2e/auth-backup.spec.ts`
- `starters/next-hybrid/e2e/todo-flow.spec.ts`
- `starters/next-localfirst/e2e/auth-backup.spec.ts`
- `starters/next-localfirst/e2e/todo-flow.spec.ts`
- `starters/react-betterauth/e2e/todo-flow.spec.ts`
- `starters/react-hybrid/e2e/auth-backup.spec.ts`
- `starters/react-hybrid/e2e/todo-flow.spec.ts`
- `starters/react-localfirst/e2e/auth-backup.spec.ts`
- `starters/react-localfirst/e2e/todo-flow.spec.ts`
- `starters/sveltekit-betterauth/e2e/todo-flow.spec.ts`
- `starters/sveltekit-hybrid/e2e/auth-backup.spec.ts`
- `starters/sveltekit-hybrid/e2e/todo-flow.spec.ts`
- `starters/sveltekit-localfirst/e2e/auth-backup.spec.ts`
- `starters/sveltekit-localfirst/e2e/todo-flow.spec.ts`
- `stress-tests/todo-react/vitest.config.browser.ts`
- `tests/svg-plotter-geometry.test.ts`
- `tests/svg-plotter-render.test.ts`
- `tests/svg-plotter-style.test.ts`
