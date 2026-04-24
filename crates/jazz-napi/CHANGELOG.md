# jazz-napi

## 2.0.0-alpha.40

## 2.0.0-alpha.39

## 2.0.0-alpha.38

## 2.0.0-alpha.37

## 2.0.0-alpha.36

### Patch Changes

- Cache runtime schema lookups across query paths and surface unhandled rejected mutations through targeted batch-id queues instead of rescanning every retained local batch record. This also brings the React Native runtime onto the same rejected-batch helper surface as WASM and N-API.
- 8bb9fbc: Allow caller-supplied row ids to use any valid UUID and rely on explicit row metadata for created-at semantics.

## 2.0.0-alpha.35

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

- 947f362: Simplify external JWT identity: `session.user_id` is now the JWT `sub` claim verbatim. The `jazz_principal_id` claim, the `external_identities` server mapping, and the hashed `external:…` fallback are removed. External providers must emit the desired Jazz user id as `sub` directly (e.g. via `getSubject: ({ user }) => user.id`). Also fixes `authMode` resolution in the policy evaluator and preserves `AnonymousWriteDeniedError` through the runtime write path.
- caad318: Modify write APIs to return a `WriteHandle`, which allows callers to wait for a given durability tier to acknowledge the write or reject it. Also introduces a global `onMutationError` handler to receive errors that aren't explicitly handled with `WriteHandle.wait`.

## 2.0.0-alpha.34

## 2.0.0-alpha.33

### Patch Changes

- ae47629: Publish a fresh alpha after the previous alpha release partially succeeded and npm refused to republish an existing platform package version.

## 2.0.0-alpha.32

## 2.0.0-alpha.31

### Patch Changes

- 09e16b4: Support recursive gather seeds built from composed same-table relations, including hop-based and unioned permission closures.

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

## 2.0.0-alpha.29

### Patch Changes

- 58ace62: Add external UUIDv7 create APIs and id-based upsert APIs across the Rust and TypeScript client surfaces.

## 2.0.0-alpha.28

### Patch Changes

- 9b45ec5: Adopt the new row-permission strategy across client and server runtimes. Local clients that only have a structural schema stay permissive for offline reads and writes, while runtimes with current permissions and sync servers enforce deny-by-default row access for session-scoped reads, inserts, updates, and deletes.

## 2.0.0-alpha.27

### Patch Changes

- 463098a: Ship the new unified row-history storage engine across Jazz runtimes.

  Relational rows, query visibility, and sync replay now go through the same storage-backed path instead of mixing durable state with older in-memory cache layers. In practice this makes local persistence and sync behavior more consistent across browser, Node, and native runtimes, especially around cold start, reconnect, and large local datasets.

## 2.0.0-alpha.26

### Patch Changes

- 15ce77e: Fix large global query and subscription snapshots dropping rows by sequencing sync delivery and delaying `QuerySettled` tier unlocks until earlier sync updates have been applied.

## 2.0.0-alpha.25

## 2.0.0-alpha.24

## 2.0.0-alpha.23

### Patch Changes

- 8b16d59: Replace Fjall with RocksDB as the default persistent storage engine for server, Node.js client, and CLI.

  **BREAKING:** Server data stored with Fjall is not compatible — existing servers must start from a clean data directory.

## 2.0.0-alpha.22

### Patch Changes

- dedab8f: Add authorship-based edit metadata for row writes across the runtime and bindings.

  Rows now expose `$createdBy`, `$createdAt`, `$updatedBy`, and `$updatedAt` magic columns in queries and permissions, and backend contexts can override stamped authorship with `withAttribution(...)`, `withAttributionForSession(...)`, and `withAttributionForRequest(...)`.

- fd7ecd0: Schema authoring no longer has a build/codegen step. Apps now define their schema directly in TypeScript with the namespaced API (`import { schema as s } from "jazz-tools"`), and `jazz-tools validate` is just an optional local preflight check.

  Current `permissions.ts` is now separate from the structural schema and migration lifecycle, instead of being versioned as part of schema identity.

  Runtime permission enforcement now follows the latest published permissions head independently of client schema hashes, with learned schemas, migration lenses, and permissions rehydrated from the local catalogue on restart.

## 2.0.0-alpha.21

## 2.0.0-alpha.20

### Patch Changes

- 9f4d4d9: Bound oversized index keys by keeping as much real value prefix as fits in the durable key and appending a length plus hash overflow trailer.

  This keeps large indexed string and JSON equality lookups working without exceeding storage key limits, while preserving prefix-based ordering instead of collapsing oversized values to a pure hash ordering. Large `array(ref(...))` values also continue to support exact array equality and per-member reference indexing.

## 2.0.0-alpha.19

### Patch Changes

- 1cf799c: Configure `jazz-napi` to generate scoped `@garden-co/*` native package names at the source and publish the generated loader from release builds.

## 2.0.0-alpha.18

### Patch Changes

- 33bc53f: Fail indexed writes cleanly when an indexed value would exceed the storage key limit instead of panicking in native storage.

  Oversized indexed inserts and updates now return a normal mutation error to JS callers, and local updates can recover rows that were previously left in a partial index state by older panic-driven failures.

## 2.0.0-alpha.17

### Patch Changes

- 94ef47c: Restore scoped `@garden-co/*` native binding package resolution in the published N-API loader after the recent generated loader regression.

## 2.0.0-alpha.16

### Patch Changes

- b2899b1: Fix the published N-API loader so packaged Jazz backend builds resolve scoped `@garden-co/*` native binding packages on every platform fallback path.

## 2.0.0-alpha.15

### Patch Changes

- 4871b02: Switch the native persistent storage engine from SurrealKV to Fjall for the CLI, NAPI bindings, and React Native bindings.

  Native local data now lives in Fjall-backed stores and uses `.fjall` database paths by default.

- bb39e15: Modify inserts to return the inserted row instead of just the id

## 2.0.0-alpha.14

### Patch Changes

- 2f5ccba: Add an in-memory storage driver across the Jazz JS, WASM, NAPI, and React Native runtimes.

  Backend contexts can now opt into memory-backed runtimes without local persistence, and runtime driver-mode coverage was expanded to exercise the new in-memory path.

## 2.0.0-alpha.13

## 2.0.0-alpha.12

## 2.0.0-alpha.11

## 2.0.0-alpha.10

## 2.0.0-alpha.9

## 2.0.0-alpha.8

## 2.0.0-alpha.7

### Patch Changes

- 8090ccd: Use scoped `@garden-co/*` platform package names for published N-API binaries.
- 6b19ea3: Add support for JSON columns.
