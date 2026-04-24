# cojson-core-rn

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

- caad318: Modify write APIs to return a `WriteHandle`, which allows callers to wait for a given durability tier to acknowledge the write or reject it. Also introduces a global `onMutationError` handler to receive errors that aren't explicitly handled with `WriteHandle.wait`.

## 2.0.0-alpha.34

## 2.0.0-alpha.33

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

- e346057: Fix React Native cold-start on offline and unblock initial subscriptions when the transport can't reach the server.
  - `jazz-rn` now regenerates its UniFFI bindings for the `insert` / `insert_with_session` signatures introduced with the caller-supplied UUIDv7 APIs, so the native library and JS adapter agree at startup and Jazz initializes in the app.
  - `jazz-rn` now calls `rehydrate_schema_manager_from_catalogue` after opening SQLite, matching the WASM runtime, so offline cold-starts can decode previously-persisted rows against their original schema/permissions history.
  - `jazz-tools` bounds the "hold remote query frontier while transport connects" wait so a never-completing transport no longer stalls first subscription delivery forever. Pending servers now clear on a new `TransportInbound::ConnectFailed` event (fired from the connect/handshake error paths in both tokio and wasm run loops), with a 2s safety-net timeout for hung connects. The frontier hold also re-evaluates live at settle time so offline or hung first-connect cases release immediately once pending clears.

  No public-API break. RowPolicyMode selection, persisted-row wire format, and transport handshake semantics are unchanged.

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

- a1cb9d5: Switch the mobile persistent storage engine to SQLite.

  **WARNING:** Existing local data stored with the previous Fjall-based engine is not compatible with SQLite. On-device data will be lost on upgrade — users will need to re-sync from the server.

## 2.0.0-alpha.25

## 2.0.0-alpha.24

## 2.0.0-alpha.23

## 2.0.0-alpha.22

### Patch Changes

- dedab8f: Add authorship-based edit metadata for row writes across the runtime and bindings.

  Rows now expose `$createdBy`, `$createdAt`, `$updatedBy`, and `$updatedAt` magic columns in queries and permissions, and backend contexts can override stamped authorship with `withAttribution(...)`, `withAttributionForSession(...)`, and `withAttributionForRequest(...)`.

- fd7ecd0: Schema authoring no longer has a build/codegen step. Apps now define their schema directly in TypeScript with the namespaced API (`import { schema as s } from "jazz-tools"`), and `jazz-tools validate` is just an optional local preflight check.

  Current `permissions.ts` is now separate from the structural schema and migration lifecycle, instead of being versioned as part of schema identity.

  Runtime permission enforcement now follows the latest published permissions head independently of client schema hashes, with learned schemas, migration lenses, and permissions rehydrated from the local catalogue on restart.

## 2.0.0-alpha.21

### Patch Changes

- 477c43c: Remove Nitro Modules code (`crates/jazz-nitro`, `examples/rn-jazz-nitro`) in favor of uniffi for the React Native bridge.

## 2.0.0-alpha.20

### Patch Changes

- 9f4d4d9: Bound oversized index keys by keeping as much real value prefix as fits in the durable key and appending a length plus hash overflow trailer.

  This keeps large indexed string and JSON equality lookups working without exceeding storage key limits, while preserving prefix-based ordering instead of collapsing oversized values to a pure hash ordering. Large `array(ref(...))` values also continue to support exact array equality and per-member reference indexing.

## 2.0.0-alpha.19

## 2.0.0-alpha.18

### Patch Changes

- 33bc53f: Fail indexed writes cleanly when an indexed value would exceed the storage key limit instead of panicking in native storage.

  Oversized indexed inserts and updates now return a normal mutation error to JS callers, and local updates can recover rows that were previously left in a partial index state by older panic-driven failures.

## 2.0.0-alpha.17

### Patch Changes

- bb10f1c: Add a shared `jazz-tools/expo/polyfills` entrypoint for Expo apps and ensure published `jazz-rn` packages include the generated C++ bindings required for native builds.

## 2.0.0-alpha.16

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

## 0.20.9

## 0.20.8

## 0.20.7

## 0.20.6

## 0.20.5

## 0.20.4

## 0.20.3

## 0.20.2

## 0.20.1

## 0.20.0

### Minor Changes

- 8934d8a: ## Full native crypto (0.20.0)

  With this release we complete the migration to a pure Rust toolchain and remove the JavaScript crypto compatibility layer. The native Rust core now runs everywhere: React Native, Edge runtimes, all server-side environments, and the web.

  ## 💥 Breaking changes

  ### Crypto providers / fallback behavior
  - **Removed `PureJSCrypto`** from `cojson` (including the `cojson/crypto/PureJSCrypto` export).
  - **Removed `RNQuickCrypto`** from `jazz-tools`.
  - **No more fallback to JavaScript crypto**: if crypto fails to initialize, Jazz now throws an error instead of falling back silently.
  - **React Native + Expo**: **`RNCrypto` (via `cojson-core-rn`) is now the default**.

  Full migration guide: `https://jazz.tools/docs/upgrade/0-20-0`

### Patch Changes

- 89332d5: Moved stable JSON serialization from JavaScript to Rust in SessionLog operations

  ### Changes
  - **`tryAdd`**: Stable serialization now happens in Rust. The Rust layer parses each transaction and re-serializes it to ensure a stable JSON representation for signature verification. JavaScript side now uses `JSON.stringify` instead of `stableStringify`.

  - **`addNewPrivateTransaction`** and **`addNewTrustingTransaction`**: Removed `stableStringify` usage since the data is either encrypted (private) or already in string format (trusting), making stable serialization unnecessary on the JS side.

## 0.19.22

## 0.19.19

## 0.19.18

## 0.19.17

## 0.19.16

## 0.19.15

## 0.19.14

### Patch Changes

- 41d4c52: Enabled flexible page-size support for Android builds, enabling support for 16KB page sizes to ensure compatibility with upcoming Android hardware and Google Play requirements for cojson-core-rn.

## 0.19.13

## 0.19.12

## 0.19.11

## 0.19.10

### Patch Changes

- 4f5a5e7: Version bump to align the fixed version

## 0.1.1

### Patch Changes

- d901caa: Added cojson-core-rn that improves ReactNative crypto performance
