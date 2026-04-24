# jazz-tools

## 2.0.0-alpha.40

### Patch Changes

- 72e8727: Docs/example: document the correct Expo setup for `jazz-tools`.

  `jazz-tools` emits `import.meta.url` from its runtime, so Expo apps must enable `unstable_transformImportMeta` in `babel-preset-expo` or Hermes fails to parse the bundle. `@expo/metro-config` only auto-detects `.babelrc`, `.babelrc.js`, and `babel.config.js` — `.cjs`/`.mjs` variants are silently ignored, and `config.transformer.extendsBabelConfigPath` is a no-op on Expo's pipeline. The Expo install docs and the `todo-client-localfirst-expo` example now use a plain CJS `babel.config.js` with `unstable_transformImportMeta: true`, drop the stray `.babelrc` shim, and stop declaring `"type": "module"`. `metro.config.mjs` stays ESM so it can top-level `await withJazz(...)`.

- 19ab5c4: Fix JazzProvider re-initialising when an inline config object is passed on every render. The useEffect dep array previously included `config` (the object reference); it now uses `configKey` (the JSON-stringified value) so that structurally identical config objects no longer trigger a cleanup→reacquire cycle.
- c5f0807: Wrap React `use` inside `JazzClientProvider` so it remains compatible with React 18 while preserving behavior on newer React versions.
- 402d104: `jazzPlugin` and `jazzSvelteKit` now alias `jazz-wasm` to an absolute path resolved from `jazz-tools`'s own install location. This removes the need for Vite/SvelteKit consumers on pnpm to add `jazz-wasm` as a direct dependency just to work around pnpm's strict-isolation layout.
  - jazz-wasm@2.0.0-alpha.40
  - jazz-rn@2.0.0-alpha.40

## 2.0.0-alpha.39

### Patch Changes

- 94bd2b8: Update Jazz server URL in generated starter apps to https://v2.sync.jazz.tools/.
- 2a3545c: Stop replaying upstream-confirmed rows to the server on reconnect. The client's full-storage sync now skips rows whose `confirmed_tier` is already above the node's own tier, so a user-role client no longer re-pushes subscription-delivered rows it never authored. Previously these replays were rejected by row-level update policies (e.g. "Update denied by USING policy — cannot see old row") on every reconnect.
  - jazz-wasm@2.0.0-alpha.39
  - jazz-rn@2.0.0-alpha.39

## 2.0.0-alpha.38

### Patch Changes

- a4453e0: `jazzPlugin` and `jazzSvelteKit` now inject `worker.format: "es"` and `optimizeDeps.exclude: ["jazz-wasm"]` via a Vite `config` hook. Consumers no longer need to set these manually in their `vite.config.ts`.
  - jazz-wasm@2.0.0-alpha.38
  - jazz-rn@2.0.0-alpha.38

## 2.0.0-alpha.37

### Patch Changes

- c825970: Re-apply stored Rejected batch settlements on runtime startup so that a crash between persisting a rejection and deleting its visible row no longer causes the lingering row to flash into queries on reload before being retracted.
- a4b83ea: `jazz-tools deploy` now warns about tables that have no explicit permission policy, matching the behaviour of `jazz-tools validate`.
- f8981c6: Fix `jazz-tools deploy` so apps without a `permissions.ts` file still publish their structural schema. The CLI now skips the permissions publish step instead of failing when no current permissions are defined.
- 751eff9: Fall back to ephemeral in-memory storage when OPFS is blocked by a SecurityError (Firefox private browsing, Safari private mode). Jazz now initialises successfully without persistence instead of failing to load entirely.
- 961361c: Treat reconnect row-history replay as idempotent only when the incoming row exactly matches the stored history member. This avoids spuriously reclassifying replayed inserts as updates on insert-only tables while still allowing same-batch corrections to propagate their final payload.
- 0fc5388: Add `jazz-tools schema hash` command to print the short hash of the current `schema.ts` without hitting the server or writing a local snapshot.
- efa67bf: Admin-secret clients now bypass local row-policy enforcement so writes still reach the sync server, where permissions are actually checked.
- fd3bd95: `Transaction.commit()` now returns a write handle, which allows waiting for ack from a given durability tier
  - jazz-wasm@2.0.0-alpha.37
  - jazz-rn@2.0.0-alpha.37

## 2.0.0-alpha.36

### Patch Changes

- Cache runtime schema lookups across query paths and surface unhandled rejected mutations through targeted batch-id queues instead of rescanning every retained local batch record. This also brings the React Native runtime onto the same rejected-batch helper surface as WASM and N-API.
- 8bb9fbc: Allow caller-supplied row ids to use any valid UUID and rely on explicit row metadata for created-at semantics.
- 3578b10: Stop persisting and rehydrating a bogus empty schema on dynamic-schema servers.

  `SchemaManager::new_server` leaves the context uninitialized with a sentinel hash. Runtime construction then called `ensure_current_schema_persisted`, writing a placeholder `catalogue_schema` row whose content hashed to the empty-schema digest. On rehydrate that hash surfaced as an "unreachable schema hash" in every connection diagnostics call. The persist path now no-ops while uninitialized, and `process_catalogue_update` ignores empty schemas for forward-compatibility with sqlite files written by the pre-fix server.

- 34e9ca4: Add a browser `window.__jazz.clearStorage()` helper for framework clients, and move the FAQ to a top-level docs page with updated storage-reset guidance.
- Updated dependencies
- Updated dependencies [8bb9fbc]
  - jazz-wasm@2.0.0-alpha.36
  - jazz-rn@2.0.0-alpha.36

## 2.0.0-alpha.35

### Minor Changes

- 9067b0c: Add static external JWT verification alongside JWKS-based verification in `jazz-tools`.

  The Rust server CLI now accepts `--jwt-public-key` / `JAZZ_JWT_PUBLIC_KEY`, and the TypeScript backend `createJazzContext(...)` path now accepts `jwtPublicKey`. Both server entrypoints reject configs that set both `jwksUrl` and the new static-key option at the same time.

### Patch Changes

- 4d67804: Promote `authMode` to a first-class typed field, add anonymous auth, and overhaul the React provider.
  - **`Session.authMode`** is now `"external" | "local-first" | "anonymous"`, derived from the JWT `iss` claim instead of an opaque string in `claims`. The permissions DSL exposes `session.authMode` and supports `session.where({ authMode })`.
  - **Anonymous auth**: when `DbConfig` has neither `secret` nor `jwtToken`, the client mints an ephemeral token. Anonymous sessions can read but are structurally denied writes (checked before policy evaluation); failures surface as `AnonymousWriteDeniedError` on the client.
  - **`DbConfig` flattened**: `auth: { localFirstSecret }` → `secret`.
  - **`AuthState` flattened**: `{ authMode, session, error? }` — no more `status` / `transport` union.
  - **React provider**: `JazzProvider` uses Suspense + `React.use()`; new `onJWTExpired` prop serializes refresh calls and replaces custom sync-wrapper components. New `useAuthState()` and `useLocalFirstAuth()` hooks. Context carries only `{ client }`.
  - **Identity module**: `mint_local_first_token` / `verify_local_first_identity_proof` → `mint_jazz_self_signed_token` / `verify_jazz_self_signed_proof`, taking an explicit issuer.

- 75756a6: Standardize BatchId JSON values on hex strings across Jazz write-context bindings.

  Batch write contexts now accept the TypeScript wire shape used by current clients, including lowercase `batch_mode` values and string `batch_id` values, and reject the old array-style BatchId JSON representation.

- e5a3189: Add schema-level per-column merge strategies to `jazz-tools`.

  Columns now default to MRCA-relative per-column LWW, and non-nullable integer columns can opt into `merge("counter")` to merge concurrent snapshots by summing their MRCA-relative deltas. Merge strategy is schema metadata, so different schema versions can resolve the same conflicting history differently without rewriting stored rows.

- 772ce14: Require a published permissions head before session-scoped writes can rely on backend authority. Persisted writes against enforcing backends without a current permissions head now reject explicitly with `permissions_head_missing`, and synced-query tests now publish permissions before expecting backend-visible rows or cross-schema authorization results. Session-scoped queries still withhold authoritative remote scope before a permissions head exists, but explicit query/subscription rejection is deferred for now.
- 947f362: Simplify external JWT identity: `session.user_id` is now the JWT `sub` claim verbatim. The `jazz_principal_id` claim, the `external_identities` server mapping, and the hashed `external:…` fallback are removed. External providers must emit the desired Jazz user id as `sub` directly (e.g. via `getSubject: ({ user }) => user.id`). Also fixes `authMode` resolution in the policy evaluator and preserves `AnonymousWriteDeniedError` through the runtime write path.
- caad318: Modify write APIs to return a `WriteHandle`, which allows callers to wait for a given durability tier to acknowledge the write or reject it. Also introduces a global `onMutationError` handler to receive errors that aren't explicitly handled with `WriteHandle.wait`.
- 45be93a: `jazz-tools` now routes its sync, schema, permissions, migration, and introspection requests under app-scoped server paths like `/apps/<appId>/...` instead of relying on a configurable `serverPathPrefix`. Server-backed CLI commands now take `<appId>` when resolving those endpoints.
- Updated dependencies [4d67804]
- Updated dependencies [75756a6]
- Updated dependencies [947f362]
- Updated dependencies [caad318]
  - jazz-wasm@2.0.0-alpha.35
  - jazz-rn@2.0.0-alpha.35

## 2.0.0-alpha.34

### Patch Changes

- 0585935: Fix OPFS B-tree page splitting for large index keys by choosing split points based on encoded page size instead of entry count. This prevents synced inserts with many near-threshold JSON index values from failing with leaf or internal split fit errors.
- 213288a: Bind qualified `where(...)` filters on hopped permission relations to the relation's actual joined scope so correlated `exists(...)` closures over gathered team grants evaluate correctly at runtime.
- 66dc47a: Direct write conflicts now resolve with MRCA-based per-column LWW for visible merge previews and merge-on-write rebases, including accepted transactional rows.

  Visible rows also persist compact winner provenance ordinals so tier-aware reads can reuse merged previews without re-walking row history when tiers have already converged.
  - jazz-wasm@2.0.0-alpha.34
  - jazz-rn@2.0.0-alpha.34

## 2.0.0-alpha.33

### Patch Changes

- jazz-wasm@2.0.0-alpha.33
- jazz-rn@2.0.0-alpha.33

## 2.0.0-alpha.32

### Patch Changes

- 2d10b2e: Include the failing index column in synced insert index-update error logs so OPFS-backed index failures are easier to diagnose.
  - jazz-wasm@2.0.0-alpha.32
  - jazz-rn@2.0.0-alpha.32

## 2.0.0-alpha.31

### Patch Changes

- ea68566: Isolate browser-local Jazz persistence by default across users and app scopes.

  `createDb()` now derives the default browser persistent namespace from both `appId` and the resolved authenticated principal when no explicit `dbName` is provided, preventing one user from reopening another user's OPFS-backed local cache. `BrowserAuthSecretStore` also now accepts scope hints like `appId`, `userId`, and `sessionId` so browser apps can avoid sharing one global local-first identity secret across unrelated sessions.

- 44b90c0: Fix misleading schema-mismatch recovery guidance during client/server handshakes.

  The transport handshake now sends the client's declared structural schema hash separately from the catalogue-state digest, so server-side connection diagnostics only suggest migrations for real schema hashes that the CLI can resolve.

- 50e46c0: Add HttpOnly cookie auth support to `jazz-tools` with a mirrored browser
  `cookieSession` for local permission evaluation. Servers can now accept JWT auth
  from a configured auth cookie, and cookie-backed websocket handshakes are
  restricted to same-origin requests.
- fd98d6e: Add coordinated browser logout and storage wipe support so follower tabs can trigger an OPFS reset through the elected leader, stale fallback namespaces are removed, and `db.logout({ wipeData: true })` clears browser state before the next session starts.
- 09e16b4: Support recursive gather seeds built from composed same-table relations, including hop-based and unioned permission closures.
- 2e8e918: Cap oversized secondary index keys to a 5 KiB budget so large text values still use the truncate-and-hash encoding without producing OPFS index entries that can overflow B-tree page splits.
- 05649ae: Add a new `deploy` CLI command to upload the current schema and permissions to the server. Replaces the existing `permissions push` command.
- 80a0360: Add `updatedAt` overrides to `insert`, `update`, and `upsert` mutation options in `jazz-tools`.

  The same override is available on the durable variants, so callers can stamp `$updatedAt` explicitly on a per-write basis without changing attribution or session scoping.

- 11921e6: Use static `new URL(...)` import for worker when no explicit runtime sources are configured, allowing bundlers (Turbopack, webpack, Vite) to detect and co-bundle the worker script and its WASM dependency automatically.

  Also passes a computed `fallbackWasmUrl` in the worker init message so non-bundled (static HTML) deployments still receive an explicit WASM path as a last resort if `wasmModule.default()` fails.

- Updated dependencies [09e16b4]
  - jazz-wasm@2.0.0-alpha.31
  - jazz-rn@2.0.0-alpha.31

## 2.0.0-alpha.30

### Patch Changes

- 848e94d: Replace HTTP `/sync` + SSE `/events` with a single Rust-owned WebSocket `/ws` transport, and wire JWT rotation and server-side auth rejection end-to-end across all bindings.

  **Transport rewrite**
  - New `TransportManager` in `jazz-tools` owns the WebSocket connection, framing (4-byte big-endian length + JSON), reconnect with exponential backoff, periodic heartbeat, and a `TransportControl` channel that observes `Shutdown` / `UpdateAuth` in every phase (connect, backoff, handshake, connected). Dropped `TransportHandle` triggers an implicit shutdown.
  - New `install_transport` helper on `RuntimeCore` centralises the boilerplate (create manager → seed catalogue state hash → register handle → spawn) so all four bindings converge on one code path.
  - `NativeWsStream` (tokio-tungstenite + rustls) and `WasmWsStream` (ws_stream_wasm) implement the shared `StreamAdapter` trait.

  **Auth refresh**
  - `JazzClient.updateAuthToken(jwt)` now pushes the refreshed credentials into the live transport via `runtime.updateAuth`, which routes through `TransportControl::UpdateAuth` and triggers a reconnect with the new auth. Previously the call only mutated local context.
  - `ConnectSyncRuntimeOptions.onAuthFailure` is now wired to `runtime.onAuthFailure` and fires whenever the server rejects the WS handshake with `Unauthorized`. NAPI exposes a `ThreadsafeFunction`-based callback; React Native exposes a UniFFI `callback_interface`; WASM keeps its existing `Function` callback.
  - The worker posts `auth-failed` back to the main thread when `runtime.updateAuth` throws, and supports `update-auth` / `disconnect-upstream` / `reconnect-upstream` messages from the bridge.

  **Bindings**
  - WASM, NAPI, and React Native all now expose `connect`, `disconnect`, `update_auth`, `on_auth_failure`.
  - React Native: `JazzRnRuntimeAdapter` forwards `updateAuth` and `onAuthFailure` to the UniFFI binding (previously missing — auth refresh was a silent no-op on RN).
  - `JazzClient.updateAuthToken` carries `admin_secret` and `backend_secret` forward from context (previously the serialised payload only included `jwt_token`, silently erasing privileged credentials on every refresh).

  **Breaking changes**
  - `POST /sync` and `GET /events` HTTP routes are deleted; external callers receive 404. Use the WebSocket `/ws` route via `runtime.connect(url, authJson)`.
  - `RuntimeCore<S, Sch, Sy>` is now `RuntimeCore<S, Sch>` — the `SyncSender` generic parameter has been removed.
  - `NapiSyncSender` and `RnSyncSender` are removed; bindings use `runtime.connect` instead.
  - `TokioRuntime::new` no longer takes the trailing `SyncSender` argument.
  - Cargo features `transport` and `transport-http` are removed; transport types are default-on, and `transport-websocket` enables the WS implementation.

  **Tests**
  - Inline `TransportManager` tests cover shutdown and `update_auth` in every phase (connect / backoff / handshake / connected).
  - Re-enabled two previously-`#[ignore]`d sync-reliability tests after fixing a debounce-flag race in `TokioScheduler::schedule_batched_tick`.
  - Integration tests migrated from `/events` to `/ws`.
  - New TS coverage: `client.test.ts` (onAuthFailure wiring + secret preservation), `napi.auth-failure.test.ts` (E2E against real NAPI + server), `db.transport.test.ts`, `url.test.ts`, `db.auth-refresh.worker.test.ts` (browser worker round-trip), RN adapter tests for `updateAuth` / `onAuthFailure`.

- e346057: Fix React Native cold-start on offline and unblock initial subscriptions when the transport can't reach the server.
  - `jazz-rn` now regenerates its UniFFI bindings for the `insert` / `insert_with_session` signatures introduced with the caller-supplied UUIDv7 APIs, so the native library and JS adapter agree at startup and Jazz initializes in the app.
  - `jazz-rn` now calls `rehydrate_schema_manager_from_catalogue` after opening SQLite, matching the WASM runtime, so offline cold-starts can decode previously-persisted rows against their original schema/permissions history.
  - `jazz-tools` bounds the "hold remote query frontier while transport connects" wait so a never-completing transport no longer stalls first subscription delivery forever. Pending servers now clear on a new `TransportInbound::ConnectFailed` event (fired from the connect/handshake error paths in both tokio and wasm run loops), with a 2s safety-net timeout for hung connects. The frontier hold also re-evaluates live at settle time so offline or hung first-connect cases release immediately once pending clears.

  No public-API break. RowPolicyMode selection, persisted-row wire format, and transport handshake semantics are unchanged.

- b8581ad: The standalone inspector now shows each table's currently published sync-server permissions alongside its stored schema, making it easier to verify the rules your server is actively enforcing.
- Updated dependencies [5c80e4d]
- Updated dependencies [848e94d]
- Updated dependencies [e346057]
  - jazz-wasm@2.0.0-alpha.30
  - jazz-rn@2.0.0-alpha.30

## 2.0.0-alpha.29

### Patch Changes

- 58ace62: Add external UUIDv7 create APIs and id-based upsert APIs across the Rust and TypeScript client surfaces.
- Updated dependencies [58ace62]
  - jazz-wasm@2.0.0-alpha.29
  - jazz-rn@2.0.0-alpha.29

## 2.0.0-alpha.28

### Minor Changes

- f6d18f8: Add BIP39 recovery phrase for local-first identity, exposed at the new `jazz-tools/passphrase` subpath. `RecoveryPhrase.fromSecret` / `RecoveryPhrase.toSecret` encode and decode the 32-byte local-first auth secret as a 24-word English mnemonic, with structured `RecoveryPhraseError` codes and forgiving whitespace/case normalization. Also fixes a latent cache bug in `BrowserAuthSecretStore` and `ExpoAuthSecretStore` where `saveSecret` did not invalidate `cachedPromise`, so a restore after `getOrCreateSecret` would silently keep the pre-restore secret.

### Patch Changes

- 6b2ceff: Reduce migration workflow churn for schema changes that do not require row transforms.

  `jazz-tools migrations create` and `jazz-tools migrations push` now treat default-only and column-order-only schema hash changes as compatible transitions that do not need a reviewed migration file, while still requiring reviewed migrations for incompatible changes like nullability or reference updates. The CLI also now accepts reviewed migration modules that load through CommonJS-style nested `default` exports.

- 6afc27e: Fix row-level policies that reference a row's own `id`, including claim checks like `id IN @session.claims.editable_doc_ids`, so write permissions evaluate against the row `ObjectId` even when the table has no explicit `id` column.
- 9b45ec5: Adopt the new row-permission strategy across client and server runtimes. Local clients that only have a structural schema stay permissive for offline reads and writes, while runtimes with current permissions and sync servers enforce deny-by-default row access for session-scoped reads, inserts, updates, and deletes.
- 234b138: Added `jazzSvelteKit()` Vite plugin (`jazz-tools/dev/sveltekit`) for SvelteKit and Vite+Svelte projects. Starts an embedded Jazz dev server, publishes and watches the schema, and injects `PUBLIC_JAZZ_APP_ID`/`PUBLIC_JAZZ_SERVER_URL` into the Vite env. Supports three modes: embedded local server (default), connect to an explicit URL via `server: "https://…"`, or connect to a server already described in `PUBLIC_JAZZ_SERVER_URL`. Defaults `schemaDir` to `src/lib/` to match SvelteKit conventions.
- e752ae2: Remove demo auth, anonymous auth, and synthetic users. The only valid auth modes are now local-first (Ed25519 JWT) and external (JWKS JWT). Add Expo support for local-first auth secret generation via expo-crypto.
- 4792880: Verify bearer JWTs inside backend `createJazzContext(...).forRequest()` / `withAttributionForRequest()`, add backend `jwksUrl` and `allowSelfSigned` config, and share JWT-to-session mapping with the runtime session helpers. These request-scoped backend helpers are now async so callers can await self-signed or JWKS-backed verification.
- Updated dependencies [9b45ec5]
  - jazz-wasm@2.0.0-alpha.28
  - jazz-rn@2.0.0-alpha.28

## 2.0.0-alpha.27

### Patch Changes

- d872a4d: `allowedTo` now accepts bare relation names (e.g. `"project"`) in addition to full FK column names (`"projectId"`).
- cfaed19: Fix enum literals in nested policies

  Nested relation-backed permission filters now serialize enum literals as tagged runtime values instead of raw strings, so publishing permissions and loading them into `createJazzContext(...)` works for cases like `grant_role: "viewer"`.

- 1fb1395: Add `From<T>` impls on `Value` for common types and a `row_input!` macro for ergonomic `HashMap<String, Value>` construction.
- 463098a: Ship the new unified row-history storage engine across Jazz runtimes.

  Relational rows, query visibility, and sync replay now go through the same storage-backed path instead of mixing durable state with older in-memory cache layers. In practice this makes local persistence and sync behavior more consistent across browser, Node, and native runtimes, especially around cold start, reconnect, and large local datasets.

- Updated dependencies [463098a]
  - jazz-wasm@2.0.0-alpha.27
  - jazz-rn@2.0.0-alpha.27

## 2.0.0-alpha.26

### Patch Changes

- 15ce77e: Fix large global query and subscription snapshots dropping rows by sequencing sync delivery and delaying `QuerySettled` tier unlocks until earlier sync updates have been applied.
- 5a2adfd: Fix `state_referenced_locally` compiler warnings in Svelte components by moving prop reads into reactive contexts.
- 8be5761: Add `AddTable`, `RemoveTable` and `RenameTable` migrations
- 75b30a9: feat: enhance inspector with inline editings, resizeable panels and a shiny new grid
- 75b30a9: Fix the inspector data grid freezing the browser tab when paging or sorting, and improve diagnostics around pending query transitions.
- 9968d2f: Fix `createPolicyTestApp(...)` so policy test helpers no longer hard-code Vitest's `expect`.

  Callers now pass the `expect` function explicitly, which keeps `jazz-tools/testing` policy assertions working when the test harness provides its own assertion context.

- 6bb5d9f: Add a `runtimeSources` client config API for explicit Wasm and worker bootstrap across browser and edge-style runtimes, including `baseUrl`, `wasmUrl`, `workerUrl`, `wasmSource`, and `wasmModule` overrides exported from the runtime and framework entrypoints.
- d302911: Allow `QuerySubscription` (Svelte) and `useAll` (Vue) to accept `undefined` queries, matching the React `useAll` behaviour. When `undefined` is passed, the subscription returns `undefined` without subscribing.
- 4d57125: Fix schema comparison in `permissions push` CLI command
- Updated dependencies [a1cb9d5]
  - jazz-rn@2.0.0-alpha.26
  - jazz-wasm@2.0.0-alpha.26

## 2.0.0-alpha.25

### Patch Changes

- 30df2b4: Fix browser worker reconnect after network loss when offline `local`-tier writes were queued locally.

  The worker now aborts its stale upstream events stream before scheduling reconnect after sync POST failures, which lets later writes promote normally once network access returns. This also adds browser regression coverage for the split-context reconnect case where one client stays online while another writes offline and reconnects.
  - jazz-wasm@2.0.0-alpha.25
  - jazz-rn@2.0.0-alpha.25

## 2.0.0-alpha.24

### Patch Changes

- 08f10b9: CLI schema resolution now accepts apps that keep `schema.ts` and `permissions.ts` in `src/` as well as the app root.

  The legacy `--schema-dir ./schema` shim is no longer supported. Point CLI commands at the app root instead, where Jazz will resolve `schema.ts` from either the root or `src/`.
  - jazz-wasm@2.0.0-alpha.24
  - jazz-rn@2.0.0-alpha.24

## 2.0.0-alpha.23

### Patch Changes

- d1d19a5: Allow development-mode clients to auto-publish the current structural schema from `schema.ts` without an admin secret, while keeping non-schema catalogue writes admin-only. Improve `jazz-tools --help` and the docs so the CLI and publishing workflow more clearly explain when schema auto-push is enough versus when to run `permissions push` or `migrations push`.
- a41135e: Self-hosted servers now clean up disconnected client state after a configurable TTL, while deferring cleanup for clients that still have unprocessed inbox entries.
- 8b16d59: Replace Fjall with RocksDB as the default persistent storage engine for server, Node.js client, and CLI.

  **BREAKING:** Server data stored with Fjall is not compatible — existing servers must start from a clean data directory.

- b5193ad: Add World Tour Vue 3 example app demonstrating schema, permissions, live queries, file handling, and co-located component data access.
  - jazz-wasm@2.0.0-alpha.23
  - jazz-rn@2.0.0-alpha.23

## 2.0.0-alpha.22

### Patch Changes

- dedab8f: Add authorship-based edit metadata for row writes across the runtime and bindings.

  Rows now expose `$createdBy`, `$createdAt`, `$updatedBy`, and `$updatedAt` magic columns in queries and permissions, and backend contexts can override stamped authorship with `withAttribution(...)`, `withAttributionForSession(...)`, and `withAttributionForRequest(...)`.

- 568aa27: Add reconcileArray for granular Svelte and Vue reactivity in onDelta callbacks
- 6aca383: Fix a sync-server permission bypass where replicated soft deletes could skip `DELETE` policy evaluation.

  User writes received as `ObjectUpdated` payloads now inspect delete metadata before the sync permission check is queued. Soft-delete commits are classified as `DELETE` operations instead of `UPDATE`, so replicated row deletions correctly use delete policies and are rejected when the client lacks delete access.

- 4d53497: Self-hosted server now supports JWKS key rotation without a restart. Keys are cached with a configurable TTL (5 minutes by default, override with `JAZZ_JWKS_CACHE_TTL_SECS`) and automatically refetched when a JWT arrives with an unknown key ID or a signature mismatch. A 10-second cooldown prevents forced refreshes from being abused as a DoS vector. If the JWKS endpoint goes down, the server continues validating against the stale cached keyset.
- fd7ecd0: Schema authoring no longer has a build/codegen step. Apps now define their schema directly in TypeScript with the namespaced API (`import { schema as s } from "jazz-tools"`), and `jazz-tools validate` is just an optional local preflight check.

  Current `permissions.ts` is now separate from the structural schema and migration lifecycle, instead of being versioned as part of schema identity.

  Runtime permission enforcement now follows the latest published permissions head independently of client schema hashes, with learned schemas, migration lenses, and permissions rehydrated from the local catalogue on restart.

- 3bd07c5: Improve React Native runtime error reporting by normalizing UniFFI bridge failures into standard `Error` objects with stable `name`, `message`, `cause`, and `tag` metadata.

  Thanks [Schniz](https://github.com/Schniz)!

- 195db76: Add rune-based Svelte test infrastructure with real $state/$effect verification
- 113a73d: `jazz-tools server` now logs a ready-to-open inspector URL on startup using `https://jazz2-inspector.vercel.app/` with `url`, `appId`, and `adminSecret` encoded in the hash fragment.
- Updated dependencies [dedab8f]
- Updated dependencies [fd7ecd0]
  - jazz-wasm@2.0.0-alpha.22
  - jazz-rn@2.0.0-alpha.22

## 2.0.0-alpha.21

### Patch Changes

- 52b737b: Fix server-side row insert permission evaluation
- 65adab0: Add utils to simplify testing permissions
- eb31a76: Fix mixed `select("*", "$canDelete")` projections so permission introspection columns can be combined with wildcard row selection, including nested include projections, and document the supported query shape.
- 51094d9: Fix catalogue sync so clients receive shared catalogue updates correctly, and skip resending the catalogue on reconnect when the client and server are already aligned.
- 695862b: Allow TypeScript `update(...)` and `updateDurable(...)` calls to clear nullable fields with `null`.

  Passing `undefined` still leaves a field unchanged, and required fields still reject `null`.

- 47a9aae: Align the Vue and Svelte bindings more closely with React: Vue `useAll` now accepts `QueryOptions` and re-exports `DurabilityTier`/`QueryOptions`, while Svelte query subscriptions now use the shared subscription orchestrator, surface async subscription errors, and export `createExtensionJazzClient` and `attachDevTools` for extension tooling.
- 62406d3: Use separate fields for foreign key columns and resolved references
- Updated dependencies [477c43c]
  - jazz-rn@2.0.0-alpha.21
  - jazz-wasm@2.0.0-alpha.21

## 2.0.0-alpha.20

### Patch Changes

- 9f4d4d9: Bound oversized index keys by keeping as much real value prefix as fits in the durable key and appending a length plus hash overflow trailer.

  This keeps large indexed string and JSON equality lookups working without exceeding storage key limits, while preserving prefix-based ordering instead of collapsing oversized values to a pure hash ordering. Large `array(ref(...))` values also continue to support exact array equality and per-member reference indexing.

- Updated dependencies [9f4d4d9]
  - jazz-wasm@2.0.0-alpha.20
  - jazz-rn@2.0.0-alpha.20

## 2.0.0-alpha.19

### Patch Changes

- f2c10a1: Simplify `jazz-tools/backend` to expose only the high-level `Db` and `createJazzContext` APIs. `JazzContext` no longer exposes a low-level `client()` escape hatch, and the backend entrypoint no longer re-exports low-level runtime client and transport internals.
- 4015614: Add `.requireIncludes()` option to query builders to avoid loading rows if any of their included references are missing
- ecb2db8: Add high-level `Db` helpers for chunked browser file storage:
  `createFileFromBlob(...)`, `createFileFromStream(...)`, `loadFileAsBlob(...)`,
  and `loadFileAsStream(...)`.

  Document the conventional `files` / `file_parts` schema, permission setup, and
  blob/stream usage on the new Files & Blobs docs page.
  - jazz-wasm@2.0.0-alpha.19
  - jazz-rn@2.0.0-alpha.19

## 2.0.0-alpha.18

### Patch Changes

- ca2b0b0: Fix generated `schema/app.ts` query builder class fields for Expo compatibility by replacing `declare readonly` phantom fields with `readonly ...!:`.

  Expo's Babel pipeline rejects `declare` class fields without Flow-specific options, causing generated schemas to fail compilation in React Native apps. This change keeps the same type inference intent and does not change runtime behavior.

- 33bc53f: Fail indexed writes cleanly when an indexed value would exceed the storage key limit instead of panicking in native storage.

  Oversized indexed inserts and updates now return a normal mutation error to JS callers, and local updates can recover rows that were previously left in a partial index state by older panic-driven failures.

- d9261b7: Move Jazz client creation into the default React and React Native `JazzProvider` so Strict Mode remounts do not trigger extra startup delays, while still exposing `JazzClientProvider` for apps that need to supply their own client instance.
- 83f4f5d: Use xxHash-based checksums for `opfs-btree` pages and superblocks to reduce checksum overhead in persistent browser storage.

  Existing OPFS stores created by older builds are not checksum-compatible with this change and will need to be recreated after upgrading.

- Updated dependencies [33bc53f]
- Updated dependencies [83f4f5d]
  - jazz-wasm@2.0.0-alpha.18
  - jazz-rn@2.0.0-alpha.18

## 2.0.0-alpha.17

### Patch Changes

- 9002dde: Fix `jazz build` not regenerating `app.ts` on subsequent builds.

  The bin entry point was treating the TypeScript schema build as a one-time bootstrap step, skipping it whenever `current.sql` already existed. Removed the guard so `app.ts` and `current.sql` are always regenerated when `current.ts` is present.

- 6672f98: Remove local write-time foreign-key existence checks so inserts and updates no longer fail just because a referenced row has not been synced into the active query set yet.
- bb10f1c: Add a shared `jazz-tools/expo/polyfills` entrypoint for Expo apps and ensure published `jazz-rn` packages include the generated C++ bindings required for native builds.
  - jazz-wasm@2.0.0-alpha.17

## 2.0.0-alpha.16

### Patch Changes

- f81ecab: Fix backend N-API timestamp writes when `Timestamp` values arrive from TypeScript as JS numbers.

  `createJazzContext(...)` and other backend N-API mutation paths now accept integral epoch-millisecond timestamp payloads produced by the TS value converter, instead of rejecting modern dates as floating-point values during Rust deserialization.

- 30d2f08: Batch server-bound `/sync` payloads created in the same microtask into a single ordered request.
  - jazz-wasm@2.0.0-alpha.16

## 2.0.0-alpha.15

### Patch Changes

- 5684a18: Normalize schema manager table columns before hashing sorting by name.

  This makes logically equivalent schemas produce the same schema hash even when their column declarations are ordered differently.

- 6664ee5: Use the derived local anonymous/demo session for `JazzClient` query and subscription permission checks when no JWT is configured.
- 8877b8b: Fix runtime schema-order compatibility after sorted table columns.

  `Db` mutations and query transforms now tolerate runtime schemas returned as `Map`s, and low-level `JazzClient` create/query/subscribe APIs preserve the declared schema column order expected by generated bindings and app code.

- ac3a73e: Fix Rust schema-order compatibility when runtime table columns are sorted differently from the declared app schema, including `JazzClient` create/query flows and `SchemaManager` inserts.
- f9812d7: Fix lens SQL parsing for `TIMESTAMP` defaults so numeric defaults like `DEFAULT 0` are coerced to timestamp values instead of integers.

  This resolves type mismatches when applying migrations that add timestamp columns with numeric defaults, and adds regression coverage for `TIMESTAMP DEFAULT 0`.

- 4871b02: Switch the native persistent storage engine from SurrealKV to Fjall for the CLI, NAPI bindings, and React Native bindings.

  Native local data now lives in Fjall-backed stores and uses `.fjall` database paths by default.

- 4fff7e9: Improve type inference for `include` and `select` in TS queries
- e32e6a9: Fix backend N-API sync regression where outbound messages were dropped before they reached the server.

  `createJazzContext(...).asBackend()` now accepts the real nested N-API sync callback shape used by published alpha builds, so backend query subscriptions and other upstream sync traffic can leave the local runtime again.

- 971f8cf: Add `$canRead`, `$canEdit`, and `$canDelete` permission introspection magic columns to queries, and reserve the `$` column prefix for system magic fields.
- bb39e15: Modify inserts to return the inserted row instead of just the id
- 8571fdb: Make query optional in `useAll` to support conditionally running queries when inputs are missing
- 9accce0: `QuerySubscription` in the Svelte bindings now accepts an options object as its second argument (e.g. `{ tier: 'edge' }`), matching the React `useAll` API. The previous bare-string form is removed.
- 78e074f: Split the local-first insert APIs in `jazz-tools`.
  - `db.insert(...)` now applies the write immediately and returns the inserted row synchronously.
  - `db.insertDurable(...)` waits for the requested durability tier before resolving.

- 4fd041c: Split the local-first update/delete APIs in `jazz-tools`.
  - `db.update(...)` and `db.delete(...)` now apply immediately and return `void`.
  - `db.updateDurable(...)` and `db.deleteDurable(...)` wait for the requested durability tier before resolving.
  - `db.deleteFrom(...)` has been renamed to `db.delete(...)`.

- Add Vue bindings and a `jazz-tools/vue` entrypoint, with matching docs and example coverage.
- Updated dependencies [bb39e15]
  - jazz-wasm@2.0.0-alpha.15

## 2.0.0-alpha.14

### Patch Changes

- ad29f43: fix query sync provenance for paginated, nested subquery, and recursive subscriptions
- a4da52d: Wait for the initial server event stream handshake before returning from `JazzClient::connect`, preventing `EdgeServer` settled queries from racing the connection after server restart.
- 78092d3: Add support for `EXISTS (SELECT FROM <table> WHERE <expr>)` in SQL policy expressions.
- 78092d3: Fix `@session.__jazz_outer_row.id` not resolving inside EXISTS subquery policies. Previously the outer row's UUID was silently treated as an unresolvable column, causing all EXISTS policy checks to evaluate to false on the server.
- dc25263: Fix: sync server now falls back to the server-established session when a `QuerySubscription` payload omits one.

  Demo and anonymous auth clients sent `session: None` in subscription payloads, causing all their queries to return empty results after the payload-session change in #147. The server now prefers the session it validated from auth headers during the SSE handshake, falling back to the payload only for fully unauthenticated clients. Payload sessions that differ from the server-established session are ignored and a warning is logged.

- 2943587: Fix a race condition in `subscribe_internal` where the callback could be called before it was registered.
- a952d98: Fix missing `id` fields on rows returned from included array subqueries, including nested relation results.
- ec0ff2d: Add a built-in MCP server (`npx jazz-tools mcp`) that exposes Jazz documentation as tools for AI assistants. Supports full-text search via SQLite FTS5 (Node 22.13+) with a plain-text fallback for older runtimes.
- 2f5ccba: Add an in-memory storage driver across the Jazz JS, WASM, NAPI, and React Native runtimes.

  Backend contexts can now opt into memory-backed runtimes without local persistence, and runtime driver-mode coverage was expanded to exercise the new in-memory path.

- 49307fa: Quote keyword and non-bare identifiers when emitting frozen schema and lens SQL from Rust so round-tripping generated SQL continues to parse.
- Updated dependencies [2f5ccba]
  - jazz-wasm@2.0.0-alpha.14

## 2.0.0-alpha.13

### Patch Changes

- ff4ccb3: Support quoted SQL identifiers in `jazz-tools` schema parsing/generation, including reserved keyword column names like `"table"`.
  - jazz-wasm@2.0.0-alpha.13

## 2.0.0-alpha.12

### Patch Changes

- 8bcde79: Harden runtime sync outbox handling across WASM/RN and NAPI callback contracts by typing both callback shapes, routing both through a shared normalizer, and adding conformance tests that assert identical `/sync` behavior.
  - jazz-wasm@2.0.0-alpha.12

## 2.0.0-alpha.11

### Patch Changes

- 969a139: Overhauled durability APIs to use a single `DurabilityTier` model across reads and writes.
  - Reads now take `{ tier, localUpdates }`, where `localUpdates` defaults to `"immediate"` so local writes are reflected right away even when waiting for a more remote durability tier.
  - Writes now use the base methods with optional `{ tier }` and environment-aware defaults (`"worker"` for clients, `"edge"` for backend contexts).
  - Renamed the top tier from `"core"` to `"global"` for clearer semantics.
  - Added multi-tier node identity support so single-node deployments (like CLI and cloud-server today) can acknowledge both `"edge"` and `"global"`.

- 98ba0f9: Fixed array subquery incremental updates so parent row fields stay correct. Previously, when related rows changed after subscribing, update payloads could return corrupted parent values (for example, garbled `id` or `name`).
- 48053ac: fix(codegen): generate DROP COLUMN statements for all affected tables in multi-table migrations
- debd2c3: Add `asBackend()` for server-side Jazz clients using backend-secret auth, and enforce backend-role limits so backend sync can write row data but cannot write schema/permissions catalogue entries.
- a955504: Allow backend `JazzClient` and `SessionClient` query/subscribe calls to consume generated query builders directly. Query-builder payloads with `_schema` are now translated automatically to runtime query JSON (`relation_ir`), so backend code can call `context.forRequest(...).query(app.todos.where(...))` without manual `translateQuery(...)`.
  - jazz-wasm@2.0.0-alpha.11

## 2.0.0-alpha.10

### Patch Changes

- b058893: fix `jazz-tools build` bootstrap behavior by routing through the TypeScript schema CLI when `schema/current.ts` exists and `schema/current.sql` is missing
- ddf7756: Tighten generated query helper and include types for stronger inference and stricter contracts.

  This preserves include-aware returned row types by keeping `QueryBuilder<...WithIncludes<I>>` / `_rowType` aligned with selected includes, narrows generated `*Include` relation flags to `true` (instead of `boolean`), tightens `gather(...)` step callback typing, avoids optional-include selector collapse to `never` in nested array includes, and removes unnecessary `unknown` casts in generated include helpers.
  - jazz-wasm@2.0.0-alpha.10

## 2.0.0-alpha.9

### Patch Changes

- eef9942: Fix WebAssembly fetch behavior in Next.js runtimes.
  - jazz-wasm@2.0.0-alpha.9

## 2.0.0-alpha.8

### Patch Changes

- 401db01: fix cold load of object history
- d1f17a9: fix: ensure query subgraphs share branch and schema context of parent graph
- 4775a79: Add a high-level server-side `createJazzContext` API in `jazz-tools/backend` with lazy runtime setup from generated app DSL objects, plus request/session-scoped helpers (`forRequest`, `forSession`) and lifecycle helpers (`flush`, `shutdown`).
  - jazz-wasm@2.0.0-alpha.8

## 2.0.0-alpha.7

### Patch Changes

- Add Expo support.
- 6b19ea3: Add support for JSON columns.
- 47dbdba: Added Svelte support.
  - jazz-wasm@2.0.0-alpha.7
