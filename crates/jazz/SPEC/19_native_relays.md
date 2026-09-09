# jazz — Specification · 19. Native relay hosts

## Overview

React Native, Swift, and Kotlin are hosts for the normal Jazz client and peer
protocol. They do not receive a separate local database, query evaluator,
mutation interpreter, or synchronization implementation.

A native host owns one **process-local relay** per explicit `{ app namespace,
storage namespace, auth scope }`. The relay contains a durable ordinary client
`Db` over a portable ordered-KV backend (SQLite in the first native host). Each
UI runtime gets a separate, in-memory ordinary client `Db`; it attaches to the
relay through the normal peer protocol. The relay may itself attach to an
upstream server through that same protocol.

This chapter defines the host boundary only. Chapters 3–8, 13, and 16 continue
to own transactions, permissions, sync, client API, and subscriptions.

## Details

### 19.1 Topology and ownership

```text
RN / Swift / Kotlin UI instance A ─ in-memory client Db ┐
RN / Swift / Kotlin UI instance B ─ in-memory client Db ├─ persistent native relay Db ─ upstream
                                                       └─ SQLite ordered-KV store
```

The relay is a normal non-history-complete `Db`:

- UI writes are ordinary local client commits sent to the relay.
- Each in-memory UI `Db` is explicitly non-durable: its optimistic state has
  `None` durability until the persistent relay acknowledges it. This also
  selects the ordinary relay-authority handoff for Local subscriptions, so a
  foreground observes a sibling foreground's relay-persisted write without
  waiting for the relay's own upstream to settle it.
- The relay persists them, forwards them to its own upstream when present, and
  carries fate/view updates back over the ordinary peer protocol.
- UI query and subscription semantics are the existing `Db` semantics.
- No binding may read directly from SQLite or bypass a `Db` to answer an app
  query.

Each successful socket handshake installs a fresh core upstream connection
with the connector's unchanged authenticated session context before any frames
are delivered. Its queue belongs to that socket generation, not to the relay's
lifetime. Termination immediately closes that queue to stale bridge traffic and
retains exact-generation core detach until the owner becomes available; a late
terminal notification cannot detach a replacement. Core detach owns receipt
demotion and pending advice/outbox handoff. Foreground operations and accepted
local transactions remain owned throughout reconnect. Socket cancellation during
installation retains the same cleanup even before Connected is reported. Native
installation uses the fallible core upstream entry point: peer owners are
acquired in stable registry order, and parked replay units are loaded before
admission changes receipts or outbox. A read error does not admit a successor.
The legacy core attach entry retains its historical empty-upload fallback on
replay read errors; native socket installation must not use that fallback.

One admitted persistent scope owns exactly one authenticated upstream socket
worker. Foregrounds are leases on that relay, not socket owners: opening a
second foreground reuses the scope worker and cannot create a competing bearer
connection to the same SQLite store. The worker stays alive across a clean
foreground handoff. Explicit foreground disconnect synchronously cancels and
joins that worker before publishing offline state; reconnect restarts it using
the retained native admission. Trusted scope revocation and host teardown
stop the worker and retire the admission. A bearer session requires HTTPS/WSS for a remote Edge; plaintext is
accepted only for `localhost`, IP loopback, or the documented Android emulator
host aliases (`10.0.2.2` and `10.0.3.2`). Typed network-unavailability I/O failures and
handshake timeouts leave local relay work available while the worker retries.
An I/O wrapper alone is insufficient: rustls certificate/protocol failures
arrive as `InvalidData` I/O errors and remain terminal, as do unknown I/O kinds.
A structured pre-Hello `NotReady`/`Later` response is likewise retryable,
matching browser admission. Authentication denial, invalid protocol, TLS, and
unclassified connection failures remain terminal; diagnostic text never selects
this category. A retryable failure cannot clear a previous terminal denial.
A failed bridge, owner pump, or established transport is also recorded as a
scope terminal error and surfaced to
foreground ticks until a later authenticated reconnect clears it; it may never
silently degrade into an indefinitely pending foreground operation.

Opening a foreground returns its in-memory client handle synchronously. If
the persistent owner is busy, the host retains normal subscriber admission as
a future and pumps it without blocking the UI thread. Its identity and claims
are captured from the admitted scope before it is queued; the persistent
subscriber is registered only after admission succeeds. Closing the foreground
or revoking its scope cancels admission and discards its queued traffic, so
no late subscriber can be installed. An admission failure is retained and
surfaced by that foreground's operations and ticks while sibling foregrounds
continue to progress.

The scope key has no token material. Trusted platform code derives an opaque
non-empty `auth_scope` only after authentication and admits the complete scope config to
the native host: auth scope, SQLite path, schema, persistent `DbIdentity`, and
validated session claims. JavaScript receives only an opaque random 256-bit
admission capability
and cannot choose or amend those values through the command codec. UI peer
identities are derived inside the host from the admitted author and a fresh
cryptographically random node UUID for each ephemeral foreground `Db`; they
are never derived from a restarting handle counter. This keeps fresh
in-memory HLCs from reusing a transaction identity after process restart. The
persistent relay identity remains stable and platform-supplied. Reusing a
scope with a different path, schema, or
identity fails. Trusted logout revokes the capability and atomically closes all
relay/client aliases opened through it; guessed and revoked capabilities cannot
open a scope. Each attached UI client owns one exclusive foreground-node lease
before it can mint a transaction. A clean detach reads the client runtime's
complete minted-HLC high-water through the native core, atomically persists it
while the lease remains active, and only then returns that node to the relay's
reusable pool. A crash, failed readout/persistence, forced close, or other
uncertain detach retires the node permanently; no expiry or handle-derived node
may make it reusable. Tokens belong to upstream session negotiation. Logout also
chooses either
retention or deletion through a separate, user-visible storage-lifecycle API.
No current host may reuse a relay after an auth-scope change.

#### Trusted platform admission codec

The host admission ABI is deliberately separate from the generic relay command
ABI. Kotlin and Swift/Objective-C pass one complete strict JSON object to
`jazz_native_relay_host_admit_scope_json`; JavaScript never receives that
object or a method that accepts it. Its exact top-level fields are:

```text
{
  scope: { app_namespace, storage_namespace, auth_scope },
  sqlite_path,
  schema_json,
  identity: { node, author },
  claims
}
```

Unknown fields at the top level, scope, or identity level fail closed. Rust
parses the schema JSON, serializes it to its normalized JSON spelling, builds
the `JazzSchema`, validates scope and the non-`SYSTEM` author, and validates
the typed claims before it admits the scope. `auth_scope` is required: omitted,
`null`, empty, and whitespace-only values fail before a capability is minted or
a relay registry entry can exist. Admission input is bounded to
1 MiB independently of peer-frame limits. Credential-bearing claims named
`authorization`, `access_token`, `refresh_token`, `id_token`,
`bearer_token`, or `token` (case-insensitive) fail closed. This is a boundary
rule, not a replacement for JWT verification: platform authentication verifies
a credential first, derives `auth_scope` and validated claims, then discards
the bearer material from this admission call.

On success the ABI returns exactly 32 random bytes: the opaque admission
capability. The trusted platform layer owns its lifetime and may hand that
opaque value to foreground JavaScript only for relay lifecycle commands. It
never logs, derives storage names from, or decodes the capability. A second
admission for the same scope with different SQLite path, schema, durable
identity, or claims fails before minting a capability, including when no relay
alias has yet been opened.

Auth switching is ordered: revoke every capability for the old authenticated
scope (which atomically closes its relay and UI-client aliases), then admit the
new complete scope. A revoked or guessed capability cannot open or attach a
client. Platform invalidation first makes every foreground-runtime lease
uncallable, then releases its host wrapper. Each installed JSI factory retains
an opaque host-state lease so a late finalizer cannot dereference freed state;
that retained state is released only when the final factory/foreground object
is gone. The lease is keyed by the actual UI runtime (Android's native runtime
token; iOS's module/bridge instance), never by the process-global relay host:
invalidating UI runtime A cannot invalidate a still-live UI runtime B that
shares the same relay. No platform binding retains a Rust `Db` pointer across
that lifecycle.

The lifecycle mutex protects native FFI operations against invalidation, but
MUST NOT be held while accessing JavaScript objects, registering/removing JS
callbacks, or constructing JS responses or exceptions. Even an ordinary JSI
allocation can run garbage collection: collecting a foreground HostFunction
can finalize its retained handle and acquire that same mutex. Bindings copy JS
arguments before locking and release the lock before constructing results or
reporting errors. Native response bytes remain independently owned until their
JS copy is complete. The device acceptance receipt exercises this boundary by
closing a sibling foreground during response construction, without depending
on garbage-collection timing.

`Db` and its peer connections are executor-local. A native relay therefore owns
all core values on one dedicated native owner thread. Host calls are encoded
commands with responses; JSI/JNI/Swift must never retain or dereference a Rust
`Db` handle. This is a host scheduling constraint, not new Jazz concurrency
semantics.

### 19.2 Native ABI

The shared native core publishes a monotonically versioned capability number.
The JavaScript wrapper declares the range it understands during `open`. If the
installed native component is outside that range, startup fails before opening
storage with a clear **“new native development/release build required”** error.
This makes OTA JavaScript updates safe without pretending they can update an
embedded Rust library.

The first public ABI is V1. It includes host-generated opaque admission
capabilities and trusted revocation; no earlier implementation number or
compatibility path is part of the released contract.

The ABI stays coarse and binary:

- open/close relay scope and attach/detach UI client;
- send and drain complete canonical peer frames for each UI client and the
  relay's upstream transport; host diagnostics expose only handle/queue state;
- encode/decode the same schema, row, query, error, and peer-frame contracts
  used by WASM/NAPI where they apply;
- drain/push peer protocol frames and lifecycle notifications;
- execute a compact command/event set for `Db` operations.

Host wrappers must not create an object-per-row native API. Subscription events
remain the maintained event stream from chapter 16. The RN TurboModule is one
such host; it is not part of the core crate.

The C host serializes all commands internally. Every directional peer queue has
both encoded-byte and message-count budgets. The transport seam returns its
typed `Backpressure` outcome for capacity exhaustion, allowing the ordinary
peer state machine to retain and retry a stateful send; diagnostics remain the
separate source of queue depth. Receive calls drain a
bounded batch, and each pump services a bounded round-robin subset of UI peers.
Callers retry after draining or scheduling another pump rather than spinning an
unbounded native turn.

`execute` and the `JazzRelay` TurboModule expose only the opaque postcard
`RelayCommandRequest` command/frame vocabulary. Neither admits nor revokes a
scope, and neither accepts claims, tokens, storage paths, schema, or durable
identity. The dedicated trusted admission/revocation C entries are callable
only by platform lifecycle code; this prohibition is source-contract tested in
the RN package in addition to the Rust ABI tests.

### 19.3 SQLite backend contract

`jazz-storage-sqlite` is a native implementation of Groove's existing async
`OrderedKvStorage` contract. Its format is one SQLite WAL database with:

- versioned `meta` format markers;
- a stable interned-column-family catalog;
- bytewise ordered `(column_family, key)` primary keys;
- atomic `write_many` over ordinary ordered-key/value sets and deletes;
- explicit close and flush boundaries;
- reopen that adds requested column families without losing existing contents.

It validates an existing format before adoption and returns a structured storage
error for unknown families, malformed/foreign layouts, close, and SQLite
corruption. A future Durable Objects adapter implements this logical
ordered-KV contract using the DO SQLite API; it does not reuse `rusqlite` or
claim native file/WAL behavior.

### 19.4 Package and platform contract

`jazz-rn` requires the React Native New Architecture. That is an intentional
current boundary: the generated relay spec is a TurboModule, and old
architecture builds fail during Android Gradle evaluation or iOS pod install
with an actionable instruction to enable it (Expo: add the `jazz-rn` config
plugin, then run `expo prebuild`).

**Current checkpoint (not device support).** The package autolinks an Android
and iOS `JazzRelay` TurboModule. A source/package build without staged native
artifacts reports ABI `0` and rejects commands. Trusted artifact jobs build the
Android static libraries and iOS XCFramework, and the npm file contract includes
them when the release assembly stages them; merely producing a CI artifact does
not make an npm package usable. Stock Expo Go cannot load arbitrary native code
and is unsupported.

**Target shipping contract.** A published `jazz-rn` package is a standard
current React Native New Architecture TurboModule package:

- its iOS podspec vendors prebuilt XCFramework device and simulator slices;
- its Android Gradle module vendors AAR/shared-library ABI slices;
- React Native autolinking supports bare RN without Expo as a dependency;
- Expo prebuild/CNG/EAS discover that same native module through autolinking,
  without manual Podfile, Gradle, AppDelegate, or MainApplication changes. The
  current config plugin remains only to require New Architecture for Expo apps,
  not to locate the native code.

Expo development builds retain Metro, QR loading, and Fast Refresh once the
matching native build is installed. JS-only changes do not rebuild Rust; a
native relay ABI or native package change does.

The Rust core is independent of React Native and Expo. Swift Package Manager
and Maven/Kotlin packages consume the same relay core and artifact slices.

### 19.5 Required verification ladder

1. Shared Rust contracts: ordered storage behavior, format rejection, reopen,
   deltas, flush/close, and planted negative checks.
2. Native relay contracts: two UI clients sharing one relay, distinct scopes,
   auth switch/logout, upstream reconnect, reload/reopen, and corrupted store.
   The exact host receipt
   `jazz_native_relay::tests::admitted_scope_capabilities_are_unguessable_and_revocation_closes_all_aliases`
   proves that revocation closes every alias and that the old capability cannot
   be reused; do not duplicate this lifecycle assertion in binding-only tests.
3. First-party RN test app: scenarios emit structured machine-readable
   results; the app itself is not a Maestro test script.
4. Linux Blacksmith: Rust/TS contracts, Android native artifact build, Android
   emulator install/launch/result collection via `adb`.
5. macOS Blacksmith: iOS simulator build/link/install/launch/result collection
   via `simctl`.

CI caches Cargo+sccache, pnpm, Gradle, CocoaPods, and native artifacts by
toolchain + lockfile + native-source fingerprint. Native artifacts are built
independently from JS-only scenario changes.

### 19.6 Foreground native-runtime execution

React Native does **not** use the browser/WASM runtime as its foreground
engine. Hermes does not expose the WebAssembly API required by the generated
browser binding, and bundling that binding would duplicate a large core image
beside the native relay. Switching a test app from Hermes to a different JS
engine is not a supported workaround: it changes the application engine rather
than supplying Jazz's native runtime.

Instead, each foreground JavaScript client is backed by one distinct in-memory
ordinary `Db` owned by the same native relay owner thread. This preserves the
topology from 19.1--there are still two memory-only UI replicas and one durable
SQLite relay replica--while eliminating the browser-WASM dependency:

```text
JS runtime A ─ JSI encoded binding calls ─ foreground Db A ┐
JS runtime B ─ JSI encoded binding calls ─ foreground Db B ├─ relay Db ─ upstream
                                                   (one native owner thread)
```

The foreground `Db`s are not views into SQLite and do not share query,
transaction, subscription, or lifecycle state. The relay owner merely
serializes their ordinary core operations and their normal peer links. A
foreground write is committed in its own in-memory `Db`, crosses the ordinary
peer connection to the relay on a bounded pump, and then follows the existing
fate/subscription path. In particular, no RN binding may answer a query by
reading relay SQLite directly.

#### 19.6.1 JSI boundary

`JazzRelay` remains the small TurboModule used for package discovery, ABI
diagnostics, trusted platform admission, and lifecycle control. It installs a
private versioned JSI foreground-runtime factory; the factory is not an
object-per-row API and it is not a second JavaScript implementation of Jazz.

The factory opens a foreground runtime only from a 32-byte admitted capability:

```ts
// Binding-internal shape, not an application configuration object.
const foreground = installNativeForegroundRuntime().openAttached(capability);
```

It never accepts a SQLite path, schema, session claims, token, identity, or
server URL. `openAttached` causes the host to create one fresh foreground `Db`
using the schema, admitted author, claims, and relay scope already validated at
the trusted native boundary. A guessed, revoked, or mismatched capability
fails before a runtime object is returned. Closing the runtime releases exactly
that foreground alias; revoking its capability closes every alias for that
scope as specified in 19.1.

The installer first removes the private global and then requires the native
module to synchronously replace it with a configurable own property of the
current JS runtime. ABI equality alone is not enough: a no-op installer must
not reuse a same-ABI HostObject from a preceding bridge/runtime. The JS wrapper
also rejects a non-`Uint8Array` or non-32-byte argument before it crosses JSI;
the native host remains responsible for admission validation and copying those
bytes into its bounded owner-thread command.

The JSI object implements the existing internal byte-oriented `NativeDb`
contract consumed by `NativeRuntimeAdapter`: encoded schemas, prepared queries,
row batches, mutations, write receipts, transaction handles, subscriptions,
and peer/tick scheduling use the same binding codecs as other native bindings.
The TypeScript public `Db` API stays unchanged. Rust-owned result memory is
copied into JS-owned `Uint8Array`/ArrayBuffer values before returning; no JSI
object exposes a Rust pointer or borrows Rust-owned bytes after a call returns.

#### 19.6.2 Serialized foreground `NativeDb` command contract

The foreground JSI HostObject has one binary operation:

```ts
foreground.execute(request: Uint8Array): Uint8Array
```

It copies a complete postcard `ForegroundDbCommandRequest` into the relay
owner queue and returns one complete postcard `ForegroundDbCommandResponse`.
The foreground handle travels as an out-of-band opaque C/JSI handle; it is not
encoded in the request and no command can open a different scope. `execute` is
private binding machinery: applications interact only with the maintained
TypeScript `Db` API.

The vocabulary is shared by every native host (RN JSI, Swift, Kotlin, and any
future NAPI-compatible host). It is not an RN object API. Its operation mapping
is intentionally the maintained `NativeDb` contract, grouped to keep the wire
surface orderly:

| Command family           | Existing `NativeDb` operations mapped by the binding                                          | Response / handle rule                                                                                                                 |
| ------------------------ | --------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| lifecycle and scheduling | `Probe`, `tick`, `close`, `setTickScheduler`, `onMutationError`                               | probe and bounded tick are synchronous; close is idempotent; native-to-JS wakes are coalesced callbacks, never a borrowed Rust closure |
| schema and identity      | `registerSchema`, `setIdentityClaims`, `setNonDurableClient`, `setRelayAuthoritySessionOwner` | schema/view handles are opaque native IDs scoped to their foreground                                                                   |
| queries                  | `all`                                                                                         | canonical `Query` bytes are prepared inside core; rows use the existing encoded row-batch bytes                                        |
| subscriptions            | `subscribe`, drain, cancel                                                                    | subscription handles are opaque; batches are existing encoded subscription payloads                                                    |
| writes and transactions  | `begin/commit/rollback`, insert/update/upsert/delete/restore, `writeState`, `wait`            | transaction and write handles are opaque; write receipts retain the existing encoded receipt contract                                  |
| large values and advice  | streaming upload, large-value update, permission advice                                       | existing value and advice payload codecs; streaming handles are opaque and explicitly finished/cancelled                               |
| peer transport           | `connectUpstream*`, accept subscriber, receive/send frames, close                             | normal canonical peer-frame bytes; this remains the only route between foreground and relay/upstream                                   |

Commands are append-only within a native ABI version: adding a new discriminant
is allowed only when older bindings cannot select or decode it; changing a
discriminant's fields, response layout, semantic meaning, or an existing codec
is a breaking native ABI change. The top-level relay ABI is bumped for such a
change and the JS wrapper's supported range rejects an OTA/native mismatch
before opening a foreground. Unknown or malformed command/response bytes fail
closed, with no partially returned buffer. Command outputs are copied into
JS-owned memory before Rust frees its response allocation.

**V1 foreground request registry.** Request ordinals and ordered fields are:

| Ordinal | Request                  | Ordered fields                                                                                                   |
| ------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| 0       | Probe                    | none                                                                                                             |
| 1       | Tick                     | none                                                                                                             |
| 2       | All                      | canonical Query byte vector, options_json string, transaction option u64                                         |
| 3       | Subscribe                | canonical Query byte vector, options_json string                                                                 |
| 4       | DrainSubscription        | subscription u64                                                                                                 |
| 5       | Unsubscribe              | subscription u64                                                                                                 |
| 6       | Close                    | none                                                                                                             |
| 7       | Poll                     | operation u64                                                                                                    |
| 8       | Cancel                   | operation u64                                                                                                    |
| 9       | BeginTransaction         | kind enum                                                                                                        |
| 10      | Insert                   | transaction u64, table string, cells byte vector, row_id option 16 raw bytes                                     |
| 11      | Update                   | transaction u64, table string, row_id 16 raw bytes, patch byte vector                                            |
| 12      | Upsert                   | transaction u64, table string, row_id 16 raw bytes, cells byte vector                                            |
| 13      | Delete                   | transaction u64, table string, row_id 16 raw bytes                                                               |
| 14      | CommitTransaction        | transaction u64                                                                                                  |
| 15      | RollbackTransaction      | transaction u64                                                                                                  |
| 16      | WaitForCoreTransaction   | tx_id 16 raw bytes                                                                                               |
| 17      | WaitForTransaction       | tx_id 16 raw bytes, tier string                                                                                  |
| 18      | StageMutation            | transaction u64, mutation enum, table string, row_id option 16 raw bytes, cells byte vector, options_json string |
| 19      | DisconnectNativeUpstream | none                                                                                                             |
| 20      | ReconnectNativeUpstream  | none                                                                                                             |
| 21      | NativeConnectionStatus   | none                                                                                                             |
| 22      | NativeSessionMetadata    | none                                                                                                             |
| 23      | WriteState               | tx_id 16 raw bytes                                                                                               |
| 24      | DrainMutationErrors      | none                                                                                                             |
| 25      | BeginStreamingMutation   | mutation enum, table string, row_id 16 raw bytes, cells byte vector, column string, options_json string          |
| 26      | PushStreamingMutation    | upload u64, chunk byte vector                                                                                    |
| 27      | FinishStreamingMutation  | upload u64                                                                                                       |
| 28      | AbortStreamingMutation   | upload u64                                                                                                       |
| 29      | LocalCurrentRow          | table string, row_id 16 raw bytes                                                                                |
| 30      | UpdateLargeValues        | table string, row_id 16 raw bytes, patch byte vector, descriptors_json string, updated_at_ms option u64          |
| 31      | DirectMutation           | mutation enum, table string, row_id option 16 raw bytes, cells byte vector, options_json string                  |
| 32      | PermissionAdvice         | action enum                                                                                                      |
| 33      | WaitForPendingWrites     | tier string                                                                                                      |

The command bytes are pinned by
`foreground_transaction_postcard_layout_matches_the_handwritten_ts_codec`,
`foreground_extension_v1_byte_contract`,
`foreground_continuation_v1_byte_contract`, and
`relation_read_uses_the_canonical_query_byte_contract`. Both ordinary and
relational reads carry one canonical `Query`; relational syntax is nested in
`Query.relation` and is normalized while core prepares the read.

Response 24 is MutationCommitted:
tx_id 16 raw bytes followed by row_id 16 raw bytes. Direct writes use the core's
queued admission and return its reserved write identity before suspended owner
work completes. Synchronous LocalCurrentRow and WriteState report an explicit
transient busy error when the semantic owner is unavailable; they never invent
an empty row or an unobserved write state. Invalid target option keys are rejected
at the byte boundary, including null-valued keys. Canceling a finish or abort
operation retires its result handle; the already admitted operation continues
through bounded foreground cleanup. Finish and abort use first-closing-wins;
canceling a result does not roll back a finish. Canceling a push leaves its
partial upload available for an explicit abort. Foreground close retires all
pending operations and uploads. Subscription event ordinal 3 is reserved for
StructuredDelta: reset bool, settled bool, tier string, delta byte vector,
terminal_operations_json string. Existing event ordinals 0–2 are unchanged;
terminal operations use `binding_codec::terminal_operations_to_json`.

The mutation enum has fixed ordinals Insert=0, Update=1, Upsert=2, Delete=3,
Restore=4. Response 17 is NativeConnectionStatus with three ordered booleans:
configured, explicitly_offline, connected. All fields use postcard 1's existing
canonical V1 envelope: unsigned varints, UTF-8 strings/byte vectors prefixed by
varint byte length, options with a 0/1 presence byte, and raw fixed arrays.
Options strings reuse the established native binding JSON option vocabulary;
validation belongs to the shared native option parser, never the host bridge.
A supported discriminant does not grant capabilities or change admission identity.
The byte-level Rust contract is pinned by
`foreground_extension_v1_byte_contract`; additive handler availability must be
verified by the corresponding real C ABI acceptance tests.

`All` attaches ordinary core coverage with the supplied read options before
evaluating. An optional
foreground-owned transaction handle selects the opening snapshot and staged
write overlay through the existing transaction read APIs; it cannot select a
sibling foreground's transaction or replace its opening identity/claims.
The pending future awaits owner admission and coverage without blocking the
owner thread. Completion or cancellation queues a bounded coverage cleanup;
ordinary owner turns acquire the node asynchronously before releasing its pins.
The read admission budget includes retained reads and queued cleanups, and
foreground retirement cancels those local obligations after cancelling retained
ticks and reads. Relation snapshots use
`binding_codec::encode_relation_snapshot`. Subscription event 3,
`StructuredDelta`, appends the existing terminal-operation JSON codec to the
ordinary reset/settled/tier/row-delta fields. Event 0 remains unchanged. The new
event byte contract is pinned by `foreground_structured_delta_v1_byte_contract`.

`All` and `Subscribe` each accept one bounded canonical Postcard `Query`.
Preparation, including relation lowering, happens behind those core operations;
the native boundary exposes neither a prepared-query handle nor a query-kind
discriminator.

### Relation-query Postcard carrier

Relation-query payloads use the same typed Postcard grammar as their enclosing
`Query`, `ShapeAst`, and native command envelopes. `Query.relation` and
`ShapeBody::Relation` recursively serialize the relation expression, predicate,
column/key/project references, recursion bound, and a typed JSON literal tree.
The literal tree explicitly distinguishes null, boolean, i64, u64, f64 bits,
string, array, and object entries; it does not serialize `serde_json::Value`
directly and does not embed JSON or a second custom byte codec. Native reads
carry that relation value only as the `Query.relation` field.

Relation payloads are capped at 1 MiB, nesting at 128, structural collections
at 4,096, strings at 65,536 UTF-8 bytes, and semantic dimensions at `u32::MAX`.
Union labels remain unique, UTF-8, NUL-free, and 1 through 4,096 bytes. Rows,
cells, snapshots, and subscription deltas retain their existing native binding
encodings; only the relation-query AST carrier changes.

Within a `Query.relation` option and a `ShapeBody::Relation` variant,
`WireRelationQuery { rel }` is nested directly in the enclosing Postcard value;
it is never length-wrapped as a byte vector at the native read boundary.
Postcard enum ordinals follow the declared Rust order: relation
expressions are `TableScan`, `Filter`, `Union`, `Join`, `Project`, `Gather`,
`Distinct`, `OrderBy`, `Offset`, `Limit`; predicates are `Cmp`, `IsNull`,
`IsNotNull`, `In`, `Contains`, `EnumMatch`, `And`, `Or`, `Not`, `True`, `False`;
and value references are `Literal`, `Param`, `SessionRef`, `OuterColumn`,
`FrontierColumn`, `RowId`. Struct fields use declaration order. Literal ordinals
are `Null`, `Bool`, `I64`, `U64`, `F64(bits)`, `String`, `Array`, and `Object`.
Objects contain sorted, unique UTF-8 keys; `F64` carries raw IEEE-754 bits, so
negative zero is preserved. A receiver requires an exact canonical Postcard
payload with no trailing bytes or overlong alternative spelling.

**V1 vertical slice.** Native relay ABI V1 defines the concrete foreground
foreground vocabulary: `Probe`, bounded `Tick`, idempotent `Close`, and the
local-first query lifecycle `All`, `Subscribe`, `DrainSubscription`,
`Unsubscribe`. Query inputs are exactly canonical postcard `Query` bytes; read
output is the existing `binding_codec::encode_rows` payload and subscription
deltas are the existing `binding_codec::encode_subscription_delta` payload.
Subscription identifiers are owner-thread-local opaque u64 handles allocated
once across every foreground attached to that relay, so a value copied from a
sibling foreground cannot alias a same-number local resource. JavaScript handle
responses require one complete, minimally encoded postcard u64 and reject
trailing bytes or values above `Number.MAX_SAFE_INTEGER`; they never round or
truncate an opaque native handle. Core retains asynchronous canonical
preparation inside the read operation when a retained tick already owns the
node. Subscription opening is likewise retained behind its ordinary
subscription handle, and drain polls it with the live relay wake. Cancelling an
unopened subscription retires its opener before it can publish events. Neither
preparation nor subscription admission may block or reenter the node owner.
`All` and `DrainSubscription` first poll their foreground-owned operation and
return its ordinary rows/events only when ready. If physical large-value
hydration needs chunk or peer I/O, they instead return an opaque pending
operation handle. `Poll` reports the exact ready, pending, or terminal-error
state; `Cancel` drops a pending operation and reports whether it owned that
cancellation. A draining subscription keeps its already-dequeued raw event
batch subscription-owned until a ready response has published it. Thus a
cancelled or failed hydration attempt returns that exact batch, in order, on
the next drain rather than dropping or duplicating events; unsubscribe,
foreground close, and capability revocation intentionally discard it together
with the whole subscription. A pending operation is scoped to the foreground,
bounded, and cancelled automatically by its owning unsubscribe, foreground
close, or capability revocation. Crucially, the owner thread never waits for that work:
the platform runs its normal bounded `Tick`/peer pump, then polls again. An
in-process foreground relay routes its auxiliary chunk request/response lane
through the same `PeerIoPump` boundary before attempting a semantic `Db` tick;
that lane never takes the semantic node lock, so a hydration future holding
that lock cannot deadlock its own fulfillment. Semantic foreground ticks wait
until that pending operation completes or is cancelled.
`Unsubscribe` retires the handle synchronously and queues the ordinary core
cleanup; the next bounded `Tick` performs its finalization, because awaiting
that acknowledgement while already executing on the core owner thread would
deadlock. Repeated close or unsubscribe reports `false`.

ABI V1 also includes a deliberately narrow write family: `BeginTransaction` with
the ordinary `mergeable` or `exclusive` core semantics, full-cell
`Insert`/`Update`/`Upsert`/`Delete`, `CommitTransaction`, and
`RollbackTransaction`. `WaitForCoreTransaction` accepts only that foreground's
previously committed public `txId` and becomes a pending operation until the
ordinary foreground/relay/Edge/Core fate path reaches Core durability. It lets
a host prove authoritative admission without reading relay SQLite; callers
continue bounded ticks and `Poll` while it waits. Cell payloads are the established postcard
`(RecordDescriptor, encoded-record)` envelope already used by NAPI/WASM; the
foreground codec does not expose a React-Native row object representation.
The host creates an opaque transaction handle, binds it to exactly one
foreground, caps the number of open handles, and abandons all still-open
handles when that foreground closes or its capability is revoked. A successful
commit returns the normal public 16-byte `txId`, never the mutable handle.
Schema, permission, and transaction errors remain ordinary
`OperationError` responses so the eventual shared adapter keeps their existing
error attribution; malformed bytes and lifecycle failures still fail closed at
the C boundary.

This slice intentionally delegates every mutation to the existing core
transaction APIs with their default options. It therefore does not invent
branch targets, custom timestamps or write attribution, row-version CAS,
large-value diffs, restore, or write-state APIs. Full-cell local writes
do not need a new pending protocol: requests which would require asynchronous
large-value hydration are not representable by this codec. Existing `Pending`
and `Poll` remain the async path for query/subscription materialization.

Explicit foreground transactions admit begin, staging, reads, commit and rollback
onto the existing core FIFO without blocking the native owner thread. Returned
transaction/row identifiers acknowledge admission; asynchronous staging failures
surface through the committed write's ordinary settlement/error channel. Byte
request and response tags are unchanged. A read is queued at command arrival,
including its deferred preparation and transaction-scoped coverage, so a later
stage or commit cannot overtake it. If coverage is awaiting delivery without
holding the node owner, tick maintenance continues while the FIFO head stays
retained; a cold operation retaining the owner still yields the native turn.
Cancelling a read drops its coverage and releases that FIFO fence, but does not
roll back a later admitted commit. Rollback and foreground close enqueue cleanup
after already-admitted transaction work and immediately retire the public handle.
The relay worker retains removed foreground owners until their ordinary local
`Db::close` drain finishes, including accepted commits; bounded cleanup turns run
without callbacks into the closed JavaScript runtime. It does not wait for all
peer frames or strengthen foreground close into a relay/network flush primitive.
Retirement drops retained query-preparation futures before attempting HLC
handoff. The HLC read is a single bounded poll: a contended owner makes the
handoff uncertain and permanently retires that node identity; a later foreground
receives a different node rather than guessing a safe clock floor. Accepted local
work remains with the closing owner. Peer-I/O errors do not skip local close
progress, and an abandonment error is reported only after close ownership has
been retained. Final relay shutdown still awaits the accepted local close drain;
this does not promise bounded completion of a never-ready storage operation.

**Permission advice.** Command 38 appends `PermissionAdvice { action }`, with
action ordinals Insert=0 (table, encoded cells), Read=1 (table, 16-byte row id),
Update=2 (table, row id, encoded patch), and Delete=3 (table, row id). Response 25
appends the advice enum Allowed=0, Denied=1, Unknown=2. Strings and byte vectors
use the existing V1 postcard framing. Pending responses use the existing
foreground-owned operation handle and poll/cancel/close lifecycle.

An admitted foreground requests advice through its scope's persistent ordinary
`Db`, whose upstream carries the authenticated scope subject. Its immediate
foreground-to-relay link cannot supply authoritative advice: the relay remains
a partial replica. The native admission must match the foreground author to the
persistent scope author before enabling this route. Generic peer attachments
cannot use it. Advice still comes only from the upstream authority, contains no
supporting rows or policy reasons, and never stages a mutation. The requesting
foreground owns delivery and cancellation even though the request uses the
scope's shared upstream. Offline/timeouts resolve Unknown; synchronous local
advice remains Unknown.

The V1 subset otherwise deliberately supports only `ReadOpts::default()`
local-first reads. It fails closed for remote tiers/read views, relation
terminal operations and any not-yet-shared mutation
contract rather than silently receiving a distinct RN meaning. The admitted
native scope continues to own schema, canonical session/author identity,
claims, and ordinary peer synchronization; JavaScript only provides canonical
query or encoded-cell bytes. `tick`/`close` convenience JSI methods may remain
internal compatibility shorthands only while they invoke the same foreground
lifecycle; `jazz-tools` must move to `execute` as each family is implemented.

**Wake registration.** ABI V1 includes the private JSI `setTickScheduler(callback)`
companion on each foreground handle. The callback receives `"immediate"`,
`"deferred"`, or `"after:<milliseconds>"`; it schedules the adapter's normal
JS-side tick and does not synchronously call the native handle. Rust records
one opaque platform callback per foreground Db. The C++ registration coalesces
owner-thread signals per foreground/runtime through that runtime's `CallInvoker`,
chooses the most urgent pending wake, and never invokes JavaScript while a host
or relay mutex is held. Teardown synchronously clears Rust's callback before
freeing its platform context; scope revocation sends a terminal cancel signal,
so an already queued callback becomes a no-op. This is a JSI lifecycle extension,
not a new byte command discriminant.

Synchronous local operations (opening an attached foreground runtime, local
writes, transaction bookkeeping, and ready local reads) may synchronously send
a bounded command to the native owner thread and wait for its response. A read
or subscription operation that needs asynchronous chunk/peer progress returns
the existing pending binding object and is resumed by the adapter's scheduler;
it must not block the JS thread while waiting for storage or the network.

#### 19.6.3 Ownership, callbacks, and teardown

- The application/authentication layer alone admits and revokes scope
  capabilities. JavaScript can carry a capability to the factory but cannot
  construct scope configuration.
- The native host owns all `Db`, SQLite, peer connection, and queue state on
  its dedicated owner thread. JSI calls copy an encoded request into that
  thread's bounded command queue and copy the response back. A JSI object never
  retains a Rust `Db` pointer.
- Core wakeups cross from the owner thread through React Native's `CallInvoker`
  to a registered JavaScript scheduler callback. They are coalesced by runtime
  and only schedule an ordinary adapter tick; they never invoke JavaScript while
  holding a host/relay mutex.
- Subscription delivery crosses as encoded batches. The JS adapter owns user
  callbacks and cancellation; the native runtime owns the underlying core
  subscription until that cancellation/close command has been processed.
- The native module invalidation path first marks each factory lease inactive
  under its lifecycle lock, then releases the platform's host-wrapper pointer.
  Every installed factory and foreground HostObject holds an opaque retained
  Rust-host lease; that lease keeps host state alive until finalization, but
  rejects all further foreground FFI calls after invalidation. Thus an Android
  activity recreation or iOS bridge reload cannot race a late JSI callback or
  finalizer into freed Rust state. The final retained lease releases host state
  only after the last foreground object is gone; trusted capabilities remain
  owned by the platform lifecycle as in 19.1.

#### 19.6.4 Platform and packaging contract

Android and iOS implement the same C ABI and JSI factory protocol. Android's
JNI/Kotlin and iOS's Objective-C++/Swift layers are thin converters for
byte/typed-array ownership, trusted admission, and lifecycle events; neither
contains query evaluation or peer semantics. The JSI factory is packaged with
the same Android ABI slices and iOS XCFramework slices as the relay, so its
version is covered by the native relay ABI check before startup.

Bare React Native applications obtain the module through autolinking. Expo
prebuild/CNG and Expo development builds obtain that same module through the
config plugin and autolinking. Expo Go remains unsupported because it cannot
contain the native JSI/relay artifact. An over-the-air JavaScript update whose
factory ABI is incompatible fails before opening a runtime with the existing
"new native development/release build required" diagnostic.

#### 19.6.5 Implementation and acceptance sequence

1. Define and source-test the versioned private JSI factory installer and its
   capability-only `openAttached` entry. This is a binding contract, not yet a
   claim of device support.
2. Implement the capability-only Rust host substrate against the already
   attached `MemoryStorage` client: it opens an opaque foreground alias and
   executes postcard `Probe`/`Tick`/`Close` commands through a bounded owner
   queue. This V1 command slice is implemented and tested at the C ABI and
   JSI wrapper boundaries; it is not yet a complete JS database binding.
3. The shared C++ JSI factory is now installed through Android JNI and iOS
   Objective-C++ and exposes capability-only opening plus the binary command
   seam. Complete the remaining `NativeDb` families in that seam, then make
   `jazz-tools/react-native` select this runtime source rather than loading
   `jazz-wasm`; remove the JavaScript relay-frame adapter from the foreground
   path once the host owns the peer link.
4. Black-box Rust, TypeScript, Android-emulator, and iOS-simulator receipts
   now cover capability admission/revocation, process reopen, scope-selected
   durability, and two aliases in **one** installed JSI runtime. The required
   proof that two **physical JSI runtimes** open from one admitted capability,
   write in A, and publish the subscription change in B remains pending. It
   must plant a wrong/missing B observation and require the installed-device
   receipt to fail; no same-runtime alias receipt may be used as evidence for
   that multi-runtime property.
5. Add native upstream transport/auth-refresh ownership after the local
   two-runtime receipt is stable. Tokens remain in platform-owned session
   negotiation and never enter the JSI foreground factory.

## Implementation ledger

| Layer                     | Status                 | Verification                                                                                        | Remaining work                                                             |
| ------------------------- | ---------------------- | --------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- |
| SQLite ordered-KV         | implemented            | crate conformance: order/prefix/range, atomic unknown-CF rejection, reopen, format rejection, close | add injected crash/durability and full differential receipt                |
| Native owner-thread relay | implemented foundation | lifecycle/frame host codec; normal `Db` peer links for persistent relay ↔ in-memory clients         | platform artifact wrappers and black-box two-client/upstream restart tests |
| RN TurboModule/package    | checkpoint implemented | generated Android+iOS `JazzRelay` contract, unavailable ABI/error receipts                          | embed and package prebuilt artifacts                                       |
| Expo/bare RN app          | prebuild scaffold      | New-Architecture config plugin plus Android/iOS prebuild commands                                   | first-party device app, Android/iOS runners, cache actions                 |

## Open questions

- The owner-thread relay currently proves the required core boundary, but the
  final command taxonomy should be extracted from NAPI/WASM codecs rather than
  copied by the first RN wrapper.
- Define the product-facing retention/deletion UX for logout before publishing a
  destructive `deleteScope` API.
- Measure SQLite versus RocksDB only after the common relay exists. SQLite is
  the first mobile/default adapter; storage choice remains hidden from the JS
  API.
