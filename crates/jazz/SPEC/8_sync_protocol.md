# jazz — Specification · 8. Sync protocol

## Overview

One protocol carries everything between nodes. This chapter defines that peer
protocol: how writes travel up as commit units, how fates and query-driven view
updates travel down, how payloads are deduplicated and rehydrated, and how
mergeable vs exclusive transactions are delivered. It ties together transactions
(ch. 3), history (ch. 4), queries (ch. 6), and authorization (ch. 7); the
deployment roles are chapter 9.

Invariant digest:

- `INV-SYNC-5`: A receiver applying a fate update MUST NOT move `global_time` backward and MUST raise observed durability only by a supplied `Some(DurabilityTier)` claim using monotone max semantics; `None` MUST leave durability unchanged.
- `INV-SYNC-7`: A query update MUST identify supporting physical row versions individually, never imply query membership from whole-transaction possession. Result members, query-source roles and program facts are receiver-local bookkeeping, not fields in the base peer read protocol.
- `INV-SYNC-8`: A view server MUST use `peer_payload_inventory.complete_tx_payloads` only for tx-level complete payloads covered by the peer payload inventory; payload dedup MUST be peer-scoped, not subscription-scoped, and partial bundles MUST remain eligible for later payload emission until complete-tx payload coverage is established.
- `INV-SYNC-9`: A receiver MUST NOT install a complete supporting set until its referenced transaction metadata and exact native bodies are available and valid for the selected authority usage.
- `INV-SYNC-10`: Every non-pending `ViewUpdate` MUST replace the subscription’s complete supporting-row set atomically. There is no wire reset flag; application resets and deltas are derived locally.
- `INV-SYNC-11`: Complete supporting-set replacement and subscription detach MUST preserve per-peer payload dedup while peer state survives.
- `INV-SYNC-12`: Downstream subscription view updates MUST contain accepted/settled state only and MUST NOT emit pending versions to non-origin peers.
- `INV-SYNC-13`: Downstream view construction MUST apply the peer identity's read policy before emitting result-set entries, version bundles, or complete tx payload refs.
- `INV-SYNC-14`: A read-policy revocation MUST remove the affected row from future settled subscription result sets but MUST NOT require redaction of previously delivered local copies.
- `INV-SYNC-15`: Exclusive transaction payloads MAY be delivered, stored, and participate partially at the transaction level; receiver-visible subscription state MUST expose them only when complete for the maintained subscription view being served, and partial fragments MUST NOT update whole-database current indexes.
- `INV-SYNC-16`: A mergeable transaction MAY be delivered and applied partially; each visible mergeable version can contribute without waiting for `tx.n_total_writes`.
- `INV-SYNC-17`: A supporting set and its native payloads MUST include enough authorized deletion-register evidence to reconstruct the row’s visible presence or absence.
- `INV-SYNC-27`: Shared deletion-history storage is local representation only: sync payloads continue to identify deletion versions by logical table, branch key, row, transaction, and schema, and receivers MUST resolve the sender's record through their own stable physical mapping.
- `INV-SYNC-18`: An edge acting as mergeable fate authority MUST defer fate assignment until the relevant permission-scope subscription has settled for the writer and affected tables.
- `INV-SYNC-20`: Applying only the local input differences between complete supporting sets MUST be observationally equivalent to evaluating the newer complete set, including enter/leave churn and exact-version replacement.
- `INV-SYNC-21`: Wire `TxId` and row-version payloads MUST use node UUIDs and schema version IDs, not node-local integer aliases.
- `INV-SYNC-22`: An edge MUST share upstream permission-scope subscriptions whenever one settled subscription can satisfy every dependent acceptance gate.
- `INV-SYNC-23`: A serving peer MUST reject a capability-gapped live subscription with `SyncMessage::SubscribeRejected` addressed to the requested `SubscriptionKey`; the rejected subscription MUST NOT become active, `Unsubscribe` for it is a no-op, and the connection MUST keep serving other subscriptions.
- `INV-SYNC-24`: Known-state payload dedup MUST omit only native bodies, preserving the complete supporting-row set and inventory references. Fast declarations may omit only versions settled at or before their declared position; not-yet-fated versions MUST be shipped.
- `INV-SYNC-25`: A stream served under known-state dedup followed by its repair responses MUST be observationally equivalent to the same stream served without dedup.
- `INV-SYNC-26`: A receiver detecting a referenced version without its body MUST be able to request exactly those `(table, row_uuid, tx_time, tx_node_id)` payloads, and the server MUST serve them subject to ordinary read policy. The repair vocabulary and server/client repair helpers are implemented and activated for declared known-state subscriptions.
- `INV-SYNC-27`: A fast known-state declaration MUST only be made for contiguously applied, unevicted served streams; any local eviction touching stored row-version bodies invalidates persisted fast declarations before another declaration can be made.
- `INV-SYNC-29`: A fast known-state declaration carrying authorization progress may affect native-body dedup only when its server-stamped progress matches the serving peer’s current token for that reader and binding view. It MUST NOT replace the complete supporting set or the fresh selected-authority confirmation.
- `INV-SYNC-30`: `settled_through` is a durable canonical-view history cursor for known-state payload dedup and repair, not a subscription or one-shot coverage receipt. Edge/Global settlement and coverage additionally require a fresh confirming `ViewUpdate` from the selected continuously active upstream connection. A new settled one-shot requires confirmation for its exact current usage-site `SubscriptionKey`; an update for a detached predecessor cannot satisfy it even when shape, binding, and options are equal. Disconnect, restart, edge switch, or any update from a nonselected upstream invalidates all selected-authority receipts immediately unless an exact recomputation closure is proven.
- `INV-SYNC-28`: The pre-reconstruction terminal carrier is historical scaffolding and is retired by `INV-SYNC-36`; it is not an authority-output compatibility contract.
- `INV-SYNC-31`: A downstream subscription MUST synchronize exact canonical authored supporting versions, never application-projected rows as replicated truth.
- `INV-SYNC-32`: A receiver MUST select branch-key-qualified authored-history winners before projection, decode each synchronized fact in its authored schema, project it through the ordered catalogue lineage into the subscription read schema, and derive terminal output with its local IVM without supplementing unrelated local history.
- `INV-SYNC-33`: The serving authority MUST decide disclosure under the exact reader and query context and send sufficient authorized supporting versions to reproduce the view. Opaque evidence or residual-program carriers require a separately specified extension and are not part of the base protocol.
- `INV-SYNC-34`: A subscription is settled only after its complete supporting set and exact native witnesses are validated for the selected authority usage. Reconnect, repair and recovery MUST re-establish that evidence before reporting settlement.
- `INV-SYNC-35`: A receiver MUST finish installation of the complete supporting set and local IVM update before publication. Persisted fast-known-state evidence MUST NOT claim partial or non-durable native input installation.
- `INV-SYNC-36`: Peer sync carries an exact authorized input closure, never authority-produced application terminal rows or ordered terminal operations. The receiver reconciles admitted authority inputs with tier-eligible local inputs and derives the only application terminal by running its local copy of the identified maintained Groove program.
- `INV-TX-2`: Committing an exclusive transaction MUST store the commit locally as `Fate::Pending` with `DurabilityTier::Local` and emit exactly one `SyncMessage::CommitUnit`.
- `INV-TX-3`: A commit unit whose Transaction.ntotalwrites does not equal the delivered version count MUST be rejected by the fate authority as RejectionReason::MalformedCommit(...)...
- `INV-TX-4`: Duplicate commit units with identical payloads MUST be idempotent and return the already-known fate; duplicate units with conflicting payloads MUST fail as Error::Conf...
- `INV-TX-5`: The authority MUST park a commit unit with missing parent/schema/content prerequisites and MUST decide it only after all prerequisites are present.
- `INV-TX-11`: Accepted core commits MUST receive a strictly increasing authority-minted `GlobalTime`; accepted state and the core committed frontier MUST become durable atomically before publication.
- `INV-TX-23`: Fate authority MUST be structurally wired by the host. Applying a bare unfated commit unit on a non-authority sync path MUST stage or park it pending remote fate; it MUST NOT accept, assign global timestamp, or create merge versions from that payload.

- `INV-SYNC-37`: LocalOnly propagation MUST remain on the calling node. Every remote subscription with propagate_upstream=false MUST be rejected regardless of identity, trust, role or worker transport.
- `INV-SYNC-38`: An extra local query input absent from a completed selected-authority scope MUST be revalidated; scope absence or Unknown MUST NOT assert deletion or access loss. Bounded batches MUST preserve eventual retry/progression for supported active queries.
- `INV-SYNC-39`: Confirmed current unavailability MUST be scoped to the exact effective identity/claims and filter current application inputs before joins, counts and limits. It MUST NOT erase shared content, expose the cause, or affect SYSTEM and other contexts.
- `INV-SYNC-40`: Readmission MUST follow complete authorized native content ingestion and fresh correlated evidence. Durable per-row denial and clear watermarks MUST survive reopen and prevent stale replies from reversing a newer decision; authoritative inclusion MUST be able to revalidate an excluded row.
- `INV-SYNC-41`: A partial Edge MUST NOT authorize query or exact-version repair bytes using stale cached policy inputs. Delegated client scopes remain client-scoped across trusted links; Edge-owned SYSTEM query reconciliation MUST NOT create access-loss markers.
- `INV-SYNC-42`: An authorized deletion MUST retain native content and deletion witnesses and includeDeleted semantics; deletion-only evidence MUST NOT certify a complete Readable coordinate or override confirmed access loss.
- `INV-SYNC-43`: Validated receipt application MUST be owned through durable and runtime source updates to completion or fail closed; caller cancellation MUST NOT leave normal queries using a source state inconsistent with persisted availability evidence.

- `INV-SYNC-44`: Every non-pending query update MUST describe one complete supporting physical row/version set, including the empty set. The wire MUST NOT assign query-input roles or carry separate source-completeness facts. Receivers MUST validate and install the set atomically before deriving results locally. Encoding, validating and comparing a complete set may scale with its size, including ordered-index lookup costs. Local query maintenance after comparison MUST still apply only the changed inputs; receiving a complete set does not authorize rebuilding every local result.
- `INV-SYNC-45`: Native supporting rows MUST follow the authority catalogue that identifies them, including permission-advice hydration. Repair MUST check exact content/deletion layers and branches and use the live usage’s admitted policy binding. Authorized deletion witnesses remain repairable under includeDeleted semantics; retired usages MUST NOT initiate repair.

- `INV-SYNC-46`: A delayed native-version repair MUST NOT reinstall a supporting snapshot superseded by a later complete snapshot for the same subscription. This ordering state is receiver-local and MUST NOT require query-input labels or a new wire field. A receiver yielding to fetch missing bodies MUST first finish applying complete updates already consumed in that receive batch; a later repair must never discard another subscription’s received update or an admitted publication.

## Details

### 8.1 One protocol, roles not code

Sync uses one peer protocol everywhere in the deployment. UI, worker, edge, and
core links all exchange the same `SyncMessage` vocabulary; a tier's behavior is
determined by its role, not by a separate wire language (ch. 1, principle 2).
Roles include relay links (`PeerRole::Relay`), edge-client links
(`PeerRole::ClientLink { identity }`), fate authority, durability, and eviction.

A relay link is an authenticated transport capability with no permission
subject. It neither implies `AuthorSubject::SYSTEM` nor independently narrows
reads. An edge-client link carries the terminated peer identity and narrows
reads under that identity. A scope-isolated client relay may carry only the
foreground binding admitted for that exact relay scope and attachment; the
upstream authority, not the relay, evaluates policy under that binding (ch. 7,
ch. 9).

A browser's scope-isolated client relay authenticates with its foreground
session, not an administrative credential incidentally present in application
configuration. Administrative admission must not replace that session or
silently change the relay's transport capability.

**Implementation status (2026-07-27).** Relay aggregation onto a shared upstream
shape is intended, but the current implementation does not guarantee it. Its
aggregation and covering-shape semantics remain an open design question below.

The peer wire form is binary-first. `WireFrame` wraps `Hello`,
`Message(WireEnvelope)`, and `Error`; `WireEnvelope.payload` contains a
postcard-encoded `SyncMessage` plus protocol version and feature bits. Postcard
is the canonical runtime frame/envelope format; JSON fixtures are only
human-readable golden checks. Row/version payloads remain groove custom
`Record` bytes inside protocol messages; postcard wraps those bytes, it does not
replace row encoding. The same split applies at the binding ABI (ch. 13):
commands, acks, and event metadata are postcard envelopes, while row-shaped
payloads are descriptor/raw `Record` bytes at the hot boundary.

#### Relation-query Postcard grammar in peer and binding envelopes

Where a peer `ShapeAst` or binding `Query.relation`/`ShapeBody::Relation`
contains a relation subtree, it uses the one typed Postcard relation-query
grammar specified in [§19, Relation-query Postcard carrier](19_native_relays.md#relation-query-postcard-carrier).
The direct native relation-read `WireRelationQuery` uses that same grammar; no
WireFrame- or binding-specific relation subcodec exists. The committed
`fixtures/relation_query_postcard.json` corpus pins its semantic-to-byte cases,
the Rust receipt rejects noncanonical payloads, and TypeScript independently
encodes the corpus and rejects malformed relation input. It is compatibility
evidence, not a migration input.

**Decision, 2026-08-28 — the sole wire protocol is v1.** `ViewUpdate` carries
settled version payloads only through `version_carriers`; the transitional
duplicate `version_bundles` field is absent. Every endpoint advertises exactly
wire-protocol v1 and requires every peer Hello to advertise exactly
`min_protocol_version=1, max_protocol_version=1`; ranges such as `0..=1`,
`1..=2`, and `1..=15` reject before payload decoding. There are no compatibility
aliases, migration paths, or old wire decoders. `VersionBundle` remains the semantic unit produced when a
carrier is expanded and remains the direct payload of `RowVersionPayloads`
repair responses.

Transaction and row-version authors use the native record
`{account: UUID, identity: {issuer: String, subject: String}}`. System authors
carry the reserved nil account and issuer plus originating node UUID subject;
forwarding preserves that origin. This data is never a permission capability.
Accountless reader sessions remain distinct from non-null row authors. Large scalar descriptors use Groove's canonical
internal enum/record encoding rather than the former private tagged/postcard
payload. Wire row-version `$createdAt` and `$updatedAt` values are Unix
milliseconds; the packed HLC is internal ordering state and is not protocol
data. The wire-v1 golden fixture set is the only supported message layout.
Wire-protocol v1 is independent of other formats that are also labelled v1,
including storage, catalogue, migration-lens, and NAPI/WASM binding formats.
`MigrationLens` payloads in that fixture set are
their bounded canonical `jazz-migration-lens-v1` byte blob (with the lens id
derived on decode), replacing postcard's former field-by-field representation.

**Decision, 2026-08-31 — relay-delegated policy snapshots are v1 fields.**
`Subscribe` now ends with `delegated_session` and `FetchRowVersions` ends with
`delegated_session`; each is an optional `(identity, claims)` snapshot. `None`
is the ordinary direct-session form. `Some` has two admitted sources:

- an admitted `TrustedBackend` link may deliberately attribute work to an
  explicit backend session; or
- a scope-isolated client-relay link may carry the immutable foreground binding
  issued for its exact durable authentication scope and current admission
  epoch.

The receiver validates the snapshot against the link's server-issued
capability before shape admission or repair serving. An ordinary session/client
link, an unscoped relay, a mismatched scope, a stale admission epoch, or a raw
client-supplied binding MUST be rejected. A scope-isolated relay sends the
validated immutable snapshot for each upstream coverage or repair request it
owns, so two sessions with equal query bindings but distinct claims cannot
share an evaluator or repair authorization. The upstream authority scopes both
initial evaluation and row-version repair to that snapshot; the relay does not
evaluate policy. `SYSTEM` is never a relay transport identity or delegated
subject. This is a deliberate redefinition of the sole, unreleased v1 layout:
there is no old-shape decoder or compatibility path.

### 8.1.1 Frozen wire-protocol v1 byte contract

`WireFrame` and its `WireEnvelope.payload` are each **one complete postcard
value**. A conformant decoder MUST reject a valid prefix followed by any
trailing byte; concatenation belongs only to the documented WebSocket
`Vec<Vec<u8>>` batch carrier. In particular, a binding MUST NOT hand a byte
suffix from one frame to the semantic decoder, and a semantic decoder MUST NOT
silently leave a suffix for its caller (`INV-WIRE-1`).
Every postcard `u64`, including an enum discriminant or length prefix, MUST use
its shortest base-128 spelling. A redundant continuation byte, more than ten
bytes, or a tenth-byte payload above `1` is malformed rather than a compatible
alternate encoding. TypeScript decoders that expose a `u64` as a JavaScript
`number` MUST reject values above `Number.MAX_SAFE_INTEGER`; only fields kept
as `bigint` may retain the full `u64` domain.
Writers encode a ZigZag `i64` only from a safe JavaScript `number` or a
`bigint` in `[-2^63, 2^63-1]`; they MUST reject an unsafe number or either
out-of-range signed endpoint before emitting bytes.
The TypeScript WebSocket carrier MUST consume the entire outer batch and the
entire known `Hello`, `Message`, or `Error` frame whenever it parses or
classifies that frame; reading only the enum tag is not frame acceptance.
Within `WireHello.authority`, `WireAuthorityEndpoint.node` is the postcard
sequence representation of `NodeUuid`: a canonical length prefix of `16`
followed by exactly sixteen UUID bytes, then the postcard `u64` authority
epoch. It is not a bare fixed-width UUID. A carrier MUST consume this complete
optional endpoint before applying the exact-frame EOF check. Omitting the
sequence length, accepting a valid Hello plus a suffix, or treating a genuine
endpoint byte as a suffix is malformed framing, not version compatibility. A
length other than exactly `16` MUST be rejected even when the declared byte
sequence and the remaining Hello fields are otherwise well formed.

Postcard enum ordinals are wire data. The wire-protocol v1 baseline freezes these permanent
discriminants (decimal):

| enum            | frozen discriminants                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WireFrame`     | `Hello=0`, `Message=1`, `Error=2`, `MessageFragment=3`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `WirePeerRole`  | `Client=0`, `Core=1`, `Edge=2`, `Relay=3`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `WireErrorCode` | `UnsupportedProtocolVersion=0`, `UnsupportedFeature=1`, `MalformedFrame=2`, `AuthFailed=3`, `Backpressure=4`, `Internal=5`, `NotReady=6`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| `WireRetry`     | `Never=0`, `AfterAuth=1`, `AfterResume=2`, `Later=3`                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `SyncMessage`   | `ChunkRequestBatch=0`, `ChunkResponseBatch=1`, `SessionClaims=2`, `CommitUnit=3`, `FateUpdate=4`, `RegisterShape=5`, `Subscribe=6`, `SubscribeRejected=7`, `Unsubscribe=8`, `PublishSchema=9`, `PublishSchemaWithLens=10`, `PublishLens=11`, `SetCurrentWriteSchema=12`, `CatalogueAck=13`, `ViewUpdate=14`, `FetchRowVersions=15`, `RowVersionPayloads=16`, `CatalogueSnapshot=17`, `PermissionAdviceRequest=18`, `PermissionAdviceResponse=19`, `AuthorizationScopeSubscribe=20`, `AuthorizationScopeReceipt=21`, `AuthorizationScopeIntent=22`, `AuthorizationScopeView=23`, `AuthorizationScopeAggregateReceipt=24`, `AuthorizationScopeUnavailable=25`, `AuthorizationScopeDecision=26`, `ChunkUploadStart=27`, `ChunkUploadNodes=28`, `ChunkUploadResult=29`, `AuthorityPublication=30` |

Future variants MUST append after these values; existing variants, fields, and
their field order MUST NOT be reordered, inserted before, reused, or decoded
through a migration path. A new optional semantic variant additionally needs a
new negotiated feature bit. Wire-protocol v1 intentionally provides neither
old-version decoding nor migration.

`AuthorityPublication` has the postcard field order `tx_id`, `commits`. `tx_id`
is the upload/acknowledgement anchor and must occur among the members. `commits`
is a length-prefixed sequence in strictly increasing `TxId` order; each member
has field order `tx`, `versions`, using exactly the ordinary `CommitUnit`
transaction and complete-version encodings. It is not a new transaction or a
persistent storage format. Compression and fragmentation preserve the complete
logical publication; a receiver never admits a physical fragment or parks one
member for independent admission.

Only a host-authenticated authority control-plane connection may submit this
message to core. An admin-authenticated catalogue bootstrap connection also
possesses that authority capability. A wire role, `SYSTEM` author, ordinary
backend credential, or delegated session does not establish it. Normal
`CommitUnit` messages on the authority link still undergo their normal policy
checks; the prior-edge-admission privilege belongs to this publication only.

Before changing transaction state, core validates the whole publication's
structure, schema availability, replay identities, and parent closure. Members
include all not-yet-globally-acknowledged parents; omitted parents must already
be globally accepted at core. Missing catalogue/dependency context fails the
whole attempt without acknowledging or separately parking members. The sender
retains/reconstructs the complete publication for retry after context repair.
The members' canonical transaction, history, and current-state records are
persisted in one ordinary Groove batch. A crash or cancellation cannot expose
only an accepted prefix after reopen; process-local alias allocations may
precede this batch but are not admitted transactions. After all members are
durable, core reconciles residual cross-edge heads using
the shared merge machinery (ch. 4), then emits ordinary per-transaction fates.
The edge retains a publication until every member has a selected-authority
Global receipt, not merely its anchor. Partial acknowledgements never remove
the remaining members' authority binding or their reconnect replay obligation.
Edge recovery reconstructs unacknowledged publications from accepted history,
including other clients' writes and edge merges, without requiring those
clients to reconnect. It stops ancestry traversal at Global acknowledgements.

Feature bits are also permanent: `SyncMessagePayload=1<<0`,
`SessionFrame=1<<1`, `StructuredErrors=1<<2`, `PayloadLz4=1<<3`,
`PayloadZstd=1<<4`, `MessageFragmentation=1<<5`,
`AuthorizationScopeReceipts=1<<6`, `AuthorizationScopeViews=1<<7`, and
`AuxiliaryChunks=1<<8`, `ScopeIsolatedClientRelay=1<<9`, and
`AuthorityPublications=1<<10`. `Hello` negotiates only the intersection. A message
envelope or fragment MUST NOT declare a bit outside that intersection. Feature
masks are postcard `u64` values and MUST be decoded and compared across all 64
bits; a binding language MUST NOT apply a narrowing 32-bit bitwise operation.
Any unsupported low or high bit, including `1<<32`, rejects the Hello before
its accepted mask is converted to a narrower runtime type. The feature mask
and authority epoch remain `bigint` through wire decoding, so canonical values
through `2^64-1` are representable without a JavaScript number conversion. Exactly
one compression bit may be active on an envelope; when both codecs are
negotiated, an outbound wire-protocol v1 sender selects LZ4 and emits only its bit. A
receiver rejects an envelope declaring both codecs, a codec change within one
connection, corrupt compressed bytes, or a decompressed payload exceeding the
logical-message budget. Compression is applied before fragmentation and removed
only after complete fragment reassembly; it never changes semantic bytes.

`WireMessageFragment` is the complete physical-fragment layout in field order:
`protocol_version`, `features`, `session`, `message_id`, `message_digest`,
`total_len`, `offset`, `payload`. Its digest covers the entire compressed
payload; reassembly admits only negotiated, session-authenticated, in-range,
non-overlapping extents with exact contiguous coverage and matching metadata,
then verifies that digest before decompression or semantic decode. The resource
limits and expiry/deduplication rules are normative in
`SPEC/13_transport_message_fragmentation.md`.

JSON version cells use the schema-derived `StoredScalar(Json)` descriptor,
including inline cells. This is the same existing scalar codec used by local
physical JSON columns. The pre-freeze correction tracked with #2461 changes
the serialized `VersionRecord` descriptor for inline JSON, but leaves its raw
inline scalar bytes unchanged; the old `String` descriptor could not encode
indirect JSON at all. Receivers reject old inline JSON records whose descriptor
no longer matches the authored schema. This correction must be shared by the
contained and typed identity candidates before freezing v1; it introduces no
new durable storage encoding or compatibility fallback.
`fixtures/large_json_wire_v1.json` pins the old inline descriptor and the corrected
inline/indirect records. Rust checks exact bytes, decoded values, roundtrips,
and rejection of the old descriptor before storage.

Optional JSON columns additionally retain their column-level nullable wrapper
inside the version record's independent authored/omitted wrapper (#2733).
`fixtures/nullable_json_wire_v1.json` pins distinct bytes for an omitted cell,
SQL null, JSON literal null, and a populated object using the existing v1 codecs.
This pre-freeze descriptor correction requires matching peers and fresh data
for affected optional JSON schemas; it does not add a legacy decoding fallback.
The non-nullable JSON corpus remains unchanged.

The wire-protocol v1 frozen corpora are `crates/jazz/fixtures/wire_message_frames.json` and
`crates/jazz/fixtures/wire_hello_frames.json`:
Rust independently decodes every hard-coded frame, re-encodes the semantic
value to the exact same payload and frame bytes, and TypeScript independently
reads every transport envelope through its production postcard reader, rejects
a suffix on every corpus frame, and round-trips each through the exact batch
carrier. The Hello corpus crosses every frozen peer role with both absent and
present authority endpoints and one- and multi-byte feature masks; supported
Core cases additionally pass through the production TypeScript WebSocket
negotiation path. Its
binding companion, `binding_codec_golden.json`, covers NAPI/WASM's shared
Rust-produced relation-snapshot and subscription-delta byte ABI, consumed by
the production TypeScript decoder. These corpora are compatibility evidence,
not a permissive migration input. Adding a new case requires a SPEC decision,
an invariant citation, and a review of every language consumer.

Inside Rust, `Db` and `PeerConnection` keep the semantic `Transport` surface over
`SyncMessage`. Binding/server byte transports use `WireFrame` and are bridged at
the edge of the core, so handshake, socket state, malformed-byte errors, and
backpressure do not become DB semantics. Transports such as websockets or
channels are binding-supplied drivers layered underneath these semantics after
they are proven in simulation (appendix A). The only ordering assumption is
**per-link FIFO**. Cross-link races and rehydration make stronger end-to-end
delivery guarantees unaffordable, so "parked orphan" is a first-class protocol
state with counters and tests (§8.2).

WebSocket carriers batch by default: one binary WebSocket message carries a
postcard `Vec<Vec<u8>>`, where each inner byte vector is one encoded
`WireFrame`. The batch envelope is transport-local and must not be confused
with row encoding or semantic sync messages; batching reduces socket/message
overhead while preserving the core's per-link FIFO `WireFrame` stream.

Fast reconnect currently uses Rust `ResumeCursor` as subscriber-connection
shipped-state: it records what that connection has already received so a
runtime-local reconnect can catch up from the cursor. This is separate from
`WireSession` metadata, which the byte transport adapter enforces when an
expected session is configured: missing, wrong-identity, stale-epoch, and
wrong-session frames fail admission with structured wire errors before semantic
sync messages are emitted. These are still runtime-local shipped-state and
admission scaffolds, not durable network resume credentials. The session
protocol still needs to specify portable session credentials, resume
acceptance/rejection, auth expiry, and unsupported-feature diagnostics through
`Hello`, message, and error frames.

The message variants and their payloads are:

| message                                                                            | direction      | payload                                                                                                                |
| ---------------------------------------------------------------------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `CommitUnit`                                                                       | up             | `{ tx: Transaction, versions: Vec<VersionRecord> }`                                                                    |
| `FateUpdate`                                                                       | down           | `{ tx_id, fate, global_time: Option<GlobalTime>, durability: Option<DurabilityTier> }`                                 |
| `RegisterShape`                                                                    | up             | `{ shape_id, ast: ShapeAst, opts: RegisterShapeOptions }`                                                              |
| `Subscribe`                                                                        | up             | `{ shape_id, subscription: SubscriptionKey, values: Vec<Value> }`                                                      |
| `SubscribeRejected`                                                                | down           | `{ subscription: SubscriptionKey, reason: SubscribeRejectReason }`                                                     |
| `Unsubscribe`                                                                      | up             | `{ subscription: SubscriptionKey }`                                                                                    |
| `ViewUpdate`                                                                       | down           | `{ subscription, settled_through, authorization_progress, version_carriers, peer_payload_inventory, supporting_rows }` |
| `PublishSchemaWithLens` / `PublishLens` / `SetCurrentWriteSchema` / `CatalogueAck` | catalogue lane | ch. 10                                                                                                                 |

A `VersionCarrier` in `ViewUpdate.version_carriers` is either one owned
`VersionBundle` or a packed run that expands to the same bundle sequence. A
`VersionBundle` is `{ tx, versions, scope, fate, global_time, durability }`: a
settled **view payload bundle** with the fate state observed when it shipped.
`scope` explicitly distinguishes
`CompleteTransaction` from `ViewScoped`; cardinality equality is not a scope
witness. A complete bundle carries the authored `tx.n_total_writes` and may
enter the peer's complete-transaction-payload inventory for later dedup. A
view-scoped bundle carries only the row/version witnesses admitted by that
selected view and MUST redact `tx.n_total_writes` to `versions.len()`. It never
establishes complete-payload coverage, even when those numbers happen to equal
the authored transaction's true cardinality.

### 8.1.2 WebSocket admission prelude (SPEC08)

Before the binary `WireFrame` carrier begins, a client sends exactly one
UTF-8 JSON object as its first WebSocket message. The server accepts that
prelude as either a WebSocket text message or a binary message containing the
same UTF-8 bytes. It then requires one binary WebSocket message that decodes to
exactly one `WireFrame::Hello`. The ordinary writer form is a complete
postcard `Vec<Vec<u8>>` singleton batch; the route also accepts its documented
complete raw-postcard `WireFrame` handshake form. The prelude and that first
wire message each have the ordinary two-second handshake read deadline. Only
the first message is parsed as a prelude; later text messages have no admission
effect.

The prelude object has these server-owned fields:

- `peer_identity`: required canonical `AuthorSubject` JSON string.
- `auth`: required `AuthConfig` object. Its current fields are `jwt_token`,
  `backend_secret`, `admin_secret`, and `backend_session`.
- `bootstrap_catalogue`: optional boolean; omission means `false`.
- `requested_link`: optional string enum; omission means `ordinary_session`.
  The only admitted values are `ordinary_session` and
  `scope_isolated_client_relay`. The latter produces its scoped-link admission
  only for an authenticated session that negotiated
  `FEATURE_SCOPE_ISOLATED_CLIENT_RELAY`; a session missing that feature
  receives `UnsupportedFeature/Never`. Admin and backend credentials retain
  their ordinary-link admission when they send this client-only request.

The JSON object is an evolution envelope, not an authority grant. Unknown
top-level fields and unknown fields nested in `auth` are ignored. An unknown
`requested_link` is rejected, because it requests an authority-bearing link
mode. Missing required fields, non-UTF-8 bytes, malformed JSON, a trailing JSON
suffix, or an invalid known field are rejected before admission.

The native Rust writer serializes only `peer_identity`, `auth`, and a true
`bootstrap_catalogue`; its optional `AuthConfig` members currently serialize as
explicit JSON nulls. The TypeScript native-runtime writer preserves its
existing top-level auth and `sub` compatibility fields while also writing the
nested `auth` object; those extra top-level fields are ignored by the server.
Neither representation is a second protocol nor an authorization alias. The
shared `jazz-websocket-prelude-v1` fixture pins the exact writer strings and
server parse result.

Axum applies a 2 MiB ceiling to both inbound WebSocket frame and message size,
including the initial text or binary prelude. An exactly 2 MiB valid prelude
may proceed to the wire Hello; a valid prelude of 2 MiB plus one byte must not
reach admission. This is the physical carrier ceiling, not a reduced logical
sync or catalogue limit.

### 8.2 Upstream: commit units

Upstream sync moves committed history, not in-progress edits. A committed
transaction travels as one atomic commit unit
(`SyncMessage::CommitUnit { tx, versions }`); open state never ships (ch. 3,
`INV-TX-2`).

Commit-unit delivery is idempotent by `tx_id`. If a known `tx_id` arrives with a
conflicting payload, the receiver rejects it as `ConflictingCommitUnit`
(`INV-TX-4`). The transaction's `n_total_writes` must equal the number of version
records in the unit (`INV-TX-3`). If the unit references parents, schema
versions, or content that the receiver does not yet know, the receiver parks the
unit until those dependencies arrive (`INV-TX-5`).

Receiving a bare unfated commit unit is not authority. On a non-authority node,
`apply_sync_message` stages or parks that commit unit as pending remote fate and
waits for a `FateUpdate`; it must not accept the unit, assign global timestamp, or
create merge versions from it (`INV-TX-23`). Only a structurally wired fate
authority path may decide fate (ch. 3 §3.6, ch. 9).

### 8.3 Fates downstream

Downstream fate messages tell peers how an already-submitted transaction has
settled. A verdict travels as
`SyncMessage::FateUpdate { tx_id, fate, global_time, durability }`.

The `durability` field is an optional _claim_. A receiver raises observed
durability monotonically only when the message carries `Some(_)`; `None` leaves
the observed durability unchanged. A receiver also never moves `global_time`
backward (`INV-SYNC-5`). When an authority accepts a commit, it assigns a
monotone `GlobalTime` that advances the allocator and watermark (ch. 3,
`INV-TX-11`) and maintains the global-current tables and change stream (ch. 4).

### 8.4 Downstream: query-driven supporting rows

A subscription sends a query and receives the current physical row versions
needed to evaluate it. Every non-pending `ViewUpdate` contains one complete
`supporting_rows` set for that subscription, including the empty set. A later
set replaces the previous set. An opening-pending response is a lifecycle
notification and carries no supporting rows; it is not a partial dataset.

Each supporting row identifies its permanent physical table UUID, authored
record table name, row UUID, transaction, concrete content or deletion layer,
and branch coordinate. The native version carriers provide the corresponding
authored bytes. They retain their authored schema and are interpreted through
the admitted catalogue and lens lineage; bytes authored under one schema MUST
NOT be relabeled as another schema's row (`INV-SYNC-31..32`).

The wire carries neither result members nor query-source role labels, separate
source-completeness facts, relation facts, residual programs, or application
terminal operations. For example, a person/manager query receives ordinary
physical rows. The receiver's compiler determines which rows participate in
each scan. If the same row plays two roles, it need not be transmitted twice
for that reason. Internal compiler source identities and local output deltas
remain implementation details (`INV-SYNC-7`, `INV-SYNC-36`, `INV-SYNC-44`).

The authority filters disclosure under the exact admitted reader and query
context before shipping any supporting version. Pending versions remain local
to their author until accepted. A partial Edge consumes Core-authorized inputs
for delegated client queries; possession of other cached rows is not authority
to serve them to that reader (`INV-SYNC-12..14`, `INV-SYNC-41`).

### 8.4.1 Reconstructing results and repairing native bodies

The receiver evaluates the query with its own IVM. A complete supporting set
may replace tasks A and B with B and C, but the receiver compares the sets and
applies only the changed local inputs. The application sees A removed and C
added, without a reset of B. Initial attachment may publish a local reset;
subsequent complete wire sets do not require repeated application resets.
Complete sets require references proportional to their size; constructing and
looking up ordered indexes may add O(n log n) processing. That does not justify
rebuilding all local query results.

Native body dedup is separate from supporting-set membership. Complete
transaction inventory may suppress already-retained native payloads, but MUST
NOT remove their references from the complete supporting set. A partial
transaction bundle establishes only its explicit payload coverage, never
complete-transaction inventory merely because cardinalities happen to match
(`INV-SYNC-8..9`, `INV-SYNC-24..26`).

A receiver lacking an exact referenced body requests repair before installing
the set. Content and deletion are independent layers: retaining content for a
row and transaction does not establish possession of its deletion-register
witness. Branch coordinates are likewise part of the exact lookup. A repair
coordinate naming a physical row and transaction may require both native
layers, but cannot authorize unrelated transaction siblings.

For example, a still-readable task may have been deleted while its receiver
was disconnected. The normal task query no longer displays it, but repair can
still supply its authorized deletion witness. Repair authorization uses the
ordinary authorized `includeDeleted` semantics rather than treating absence
from a non-deleted query as revoked access. A reader whose current policy no
longer permits the row receives no protected bytes. The repair uses the live
usage's admitted identity and claims; it cannot borrow another usage's binding
or begin work for a retired attachment (`INV-SYNC-42`, `INV-SYNC-45`).

### 8.4.2 Atomic installation and ordering

A complete supporting set is installed only after its catalogue, transaction
metadata, and exact native witnesses have been validated and admitted. Chunking
or a separate native-body repair round trip MUST NOT expose a partial set as
the subscription's new answer. Receiver-local storage and query maintenance
finish the installation before publication. A fast-known-state receipt MUST
NOT claim a partial or non-durable installation (`INV-SYNC-34..35`).

Native ingestion may batch several received updates into one local storage and
IVM boundary while preserving per-link FIFO order. Application terminal rows
and `Insert`/`Update`/`Remove`/`Move` operations are outputs of that local IVM;
they are not peer-supplied replication truth. The binding ABI may carry them
locally without making them part of the sync protocol.

If an old set needs repair and a newer complete set arrives, the old set is
superseded. A delayed repair may populate the immutable cache but MUST NOT
reinstall the old set or move the application's answer backward. Unsent repair
work for superseded sets can be discarded; an already-sent request retains its
reply correlation until completion. Repairs are split into bounded wire
requests, and a set requiring several batches remains uninstalled until all
of its needed bodies are available (`INV-SYNC-46`). These are local scheduling
rules and require no per-source roles, incremental-set proofs, or extra
completeness fields on the wire.

The receiver must also reconcile stale extra local inputs as specified below.
Readable negative evidence for more complex queries requires the authority to
collect sufficient supporting rows; simplifying the carrier alone does not
implement that collection. The scalar pilot and its explicit limitations are
specified under “Readable negative evidence and the pilot boundary.” Opaque
policy evidence and shallow aggregates require a separately specified,
feature-negotiated extension; they MUST NOT be smuggled into ordinary row
payloads as projected results or unnamed program facts.

### 8.5 Subscription Attach, Reset, And Detach

`Subscribe` attaches one usage-site subscription id to a registered shape and a
binding value vector. A peer may register the same `shape_id` under multiple
serving option sets; the serving side selects the option set by
`Subscribe.subscription.read_view`, the `ReadViewKey` derived from the resolved
read identity. The serving side groups subscriptions by canonical program
instance `(shape, resolved_read, policy, binding)` and maintains one shared view
for that key, then fans `ViewUpdate`s out to each usage-site `SubscriptionKey`. Remote serving
options are settled-only: `Local`/`None` are link-local facade tiers and must be
normalized before propagation or rejected by a serving peer. A new usage-site
subscription receives a complete supporting-row replacement once ready;
every later non-pending update is also a complete replacement. The receiver
compares it with the previous set and derives local input changes
(`INV-SYNC-10`); the sender does not need to retain a matching incremental
predecessor at the receiver.

While a usage-site `SubscriptionKey` is active on a live link, its canonical
attachment is immutable. The `Subscribe.shape_id` must equal
`Subscribe.subscription.shape_id`; a mismatch is a malformed peer request and
is dropped before registration lookup. After resolving the registration,
binding values, and serving options to a canonical program instance, replaying
the active key for that exact same instance is resource-idempotent: the serving
peer replaces the usage site's known-state declaration, marks it pending for an
initial refresh, and emits the resulting current-connection `ViewUpdate` and
applicable authorization receipt. The replay retains one canonical coverage
group, maintained view, and relayed subscription owner, and a later
`Unsubscribe` still detaches that sole attachment. Reusing the key for a
different canonical instance, or reusing a key already owned by the current-row
producer, is a malformed peer request: the serving peer drops it without a wire
response and preserves the original attachment and stream without any state or
delivery side effect.
Installing a current-row producer applies the same ownership rule after first
admitting any queued peer requests. An identical current-row owner is an
idempotent no-op; an ordinary subscription that already owns the derived key
causes a local protocol error before current-row update construction, sending,
or ownership registration.

If a `Subscribe` request cannot be served because the registered shape/read-view
has a permanent maintained-subscription capability gap, the serving peer replies
with `SyncMessage::SubscribeRejected { subscription, reason }` addressed to the
same `SubscriptionKey`. The initial reason vocabulary is
`SubscribeRejectReason::UnsupportedShapeCapability { detail }`; `detail` is
human-readable diagnostic text mapped at the serving boundary, not the internal
lowering `CapabilityReport`. After `SubscribeRejected`, that subscription is not
active, the requester must not expect `ViewUpdate`s for it, and `Unsubscribe`
for the same key is a no-op. The connection and any other subscriptions on it
remain live (`INV-SYNC-23`).

`Unsubscribe` detaches one usage-site subscription. When the last usage-site
subscription for a canonical program instance detaches, the serving side may drop
the shared maintained view and its runtime subscription state. Per-peer payload dedup
survives view reset and detach while peer state survives (`INV-SYNC-11`).

### 8.6 Policy narrowing in sync

Sync never emits view material before applying the receiving peer's read policy.
During view construction, the peer identity's policy is checked before any result
entry, bundle, or ref is emitted (`INV-SYNC-13`, ch. 7).
Revocation affects future delivery: it removes a row from future settled result
sets but never redacts an already-delivered local copy (`INV-SYNC-14`).

### 8.7 Partial vs atomic delivery

Downstream delivery preserves view visibility, not transport completeness. A
mergeable transaction may be delivered and applied **partially**: each visible
mergeable version contributes independently (`INV-SYNC-16`). Exclusive payloads
may also be partial at the transaction level and may be stored immediately, but
each maintained subscription view exposes exclusive result members only when the
payload required by that view is complete. This is a **view-complete exclusive
payload**, not necessarily a complete transaction payload. Otherwise the payload
remains stored but invisible for that view (`INV-SYNC-15`, ch. 3, ch. 7).

**Implementation status (2026-07-27).** The peer payload inventory is deliberately narrow:
`peer_payload_inventory.complete_tx_payloads: Vec<TxId>` names only complete
transaction payload coverage, not broad "known versions" and not partial row
payload coverage. Partial and version-level dedup is the committed known-state
design (§8.11), which retires this inventory rather than extending it.

The postcard `WireFrame`/`WireEnvelope` format and groove row `Record` encoding
do not change when future inventory fields are added.

### 8.8 Protocol size limits

Protocol size limits are enforced at the layer that can recover correctly:

- An encoded `WireFrame` is capped at 2 MiB before postcard frame decode.
  `WireEnvelope.payload` is one physical fragment, not a semantic-message
  ceiling. Generic fragmentation/reassembly carries an encoded `SyncMessage`
  of any ordinary database size atomically across bounded frames. Receivers
  enforce fixed advertised-length, decompressed-output, concurrent-assembly,
  aggregate staged-byte, 30-second no-progress, and five-minute maximum-age
  limits as adversarial resource defences. Exact duplicates and rejected
  extents do not count as progress. Those budgets are transport policy, not
  query, catalogue, or transaction semantics.
- A `RegisterShape` AST is capped at 64 KiB encoded. This is a semantic
  admission limit for the shape-registration request; the connection may
  continue after the rejected request. Server shells may expose this as
  configuration later for unusually large generated query shapes.
- A `CommitUnit` is capped at 4096 row-version records, independently of its
  encoded byte size. This CPU/fan-out limit is transaction semantics: an
  over-limit commit unit is rejected as
  `Fate::Rejected(MalformedCommit(_))`, the connection remains live, and later
  well-formed commit units may still settle.
- Structured-output v4 adds named `MAX_STRUCTURED_RESULT_DEPTH` and
  `MAX_STRUCTURED_RESULT_WIDTH` limits in `protocol_limits.rs`. A receiver MUST
  enforce both before recursively decoding/allocating an untrusted structured
  snapshot, replacement, or chunk accumulation. Byte caps alone do not bound
  recursive decoder stack depth or the count of children/nodes allocated from a
  compact payload. The limits apply to the rendered payload at every nesting
  level and are protocol-admission limits: over-limit input is rejected before
  semantic application (`INV-SYNC-28`).

Outbound websocket batching is byte-budgeted at the physical layer: senders
split batches across binary messages rather than relying on a count-only batch
limit. A logical `SyncMessage` is fragmented first, so each encoded `WireFrame`
fits the wire-frame budget without truncation or semantic-layer chunking.

**Wire encoding posture (target optimization guidance).** High-rate serial
transactions (keystroke-grade chains: same author, same row, near-monotone
times) make consecutive sync messages highly redundant. The wire harvests that
redundancy generically, in two layers, rather than by introducing run-shaped
message semantics: (1) **per-connection stream compression** — a compression
context that persists across frames on one transport, so cross-message
repetition (subscription keys, row ids, authors, adjacent timestamps)
compresses without any wire-format change; and (2) **columnar `ViewUpdate`
internals** — a reserved append-only message variant whose member/bundle
payloads use this protocol's independent columnar wire encoding. A lone single-edit transaction with nothing before or after it pays full framing and transaction overhead by design — it is lone precisely when there is nothing to amortize against. Storage remains an independent row-only layer.

Native transports advertise zstd-3 stream compression by default when the
feature is compiled in. WASM/browser artifacts keep transport compression
opt-in so bundle-size trade-offs stay explicit; reconnect resets the compression
context and relies on known-state redelivery for correctness.

### 8.9 Edge mergeable fate deferral and permission-scope subscriptions

An edge that acts as mergeable fate authority needs the relevant policy data
before it can decide a write's fate. It therefore must defer fate assignment
until the relevant **permission-scope subscription** has settled; until then it
retains the unit only in its in-memory deferred-admission state, outside edge
history (`INV-SYNC-18`). Once the scope settles, the edge ingests the authorized
unit exactly once and routes its edge fate; a denied unit is rejected without
being ingested.

A permission-scope subscription is an _upstream_ subscription opened by the edge
against core for the policy data required by its acceptance gate. It is keyed by
`(policy_shape, writer_claim)` (ch. 9 §9.5): the write policy's query shape bound
to the writer's `claim("user")`. This hydrates only the policy rows that writer's
writes can depend on, never a whole table.

Permission scopes are shared at the sync level whenever one settled subscription
can satisfy every dependent acceptance gate (`INV-SYNC-22`).

**Implementation status (verified 2026-07-27).** Exact-key scopes are shared and
reference-counted by dependent gates; this is covered by
`edge_deduplicates_scope_subscription_for_repeated_deferred_units` and
`edge_releases_scope_subscription_after_last_deferred_unit_resolves`
(`crates/jazz/tests/four_tier.rs`). Whether and how a broader scope can satisfy a
narrower one remains an open design question below.

### 8.10 Catalogue lane

Catalogue messages (`PublishSchemaWithLens`, `PublishLens`,
`SetCurrentWriteSchema`, `CatalogueAck`) share this protocol lane; their
semantics are chapter 10.

_Further invariants._ `INV-SYNC-21` — wire `TxId` and row-version payloads use
node UUIDs and schema-version IDs, never node-local integer aliases (ch. 2).

### 8.11 Known state: reconnect declarations and payload dedup

Steady-state and reconnect payload dedup is built on three properties the
protocol already has: the **client is the sole authority on what it durably
holds**; every `ViewUpdate` is **self-auditing** because it references the row
versions it treats as in scope, so a receiver structurally detects
"referenced without body" at apply time; and the serving side may therefore
model receiver knowledge **optimistically**, updating its model at emission
time with no acknowledgement traffic. There is no durable-apply ack and the
`Hello` handshake does not carry knowledge state; declarations ride per query.

A subscriber declares its known state per usage-site query in one of two forms:

- **Fast declaration** — `(shape, binding, completeness class, position p)`:
  "I have contiguously applied the stream you served me for this query through
  global position `p`, and none of it has been locally evicted." In the current
  implementation `p` is the exact `settled_through` stamp previously emitted by
  the serving node for the same canonical binding view. The client records and
  persists this cursor when applying `ViewUpdate`s and echoes it on resubscribe.
  Any local eviction touching stored row-version bodies invalidates persisted
  fast facts before another declaration can be made (`INV-SYNC-27`).
- **Slow declaration** — an explicit set of row-version identities
  `(row_uuid, tx_time, tx_node_id)`: used when no valid fast fact exists
  (fresh store, eviction, corruption). The client evaluates the query locally
  and declares exactly the versions it holds. Oversized exact declarations
  degrade to no declaration and a full ship; they are never truncated because a
  partial exact declaration would silently overclaim. Version identities use the
  wire `TxId` form (`INV-SYNC-21`); unfated versions are declarable because
  `TxId`s exist before fate.

#### Authorization progress

A fast declaration may additionally carry an **authorization-progress token**.
It is a server-stamped monotonic generation of the authorization state governing
this reader's visibility for this canonical binding view (shape, binding, and
read view). It is deliberately part of the declaration, rather than an
out-of-band connection hint: it qualifies exactly the state the subscriber is
claiming to have applied and persists with that state across reconnects.
`ViewUpdate` carries the server stamp beside its
peer-payload inventory, so the receiver persists it atomically with the
corresponding settled fast fact before later echoing it in the declaration.

The serving peer owns the token. Its granularity is **one reader plus one
canonical binding view**, not a global policy-head counter. It advances when
that reader/view is rebuilt because its effective authorization changed (for
example, session claims changed or a permissions head was installed). This
avoids forcing every reader to reset for unrelated policy churn. The cost of a
token that is too coarse is excess resets; the cost of one that is too fine is
unsafe suppression of a reset, so an absent token, an unknown server generation,
or a mismatch is always treated conservatively. The peer retains the generation
in its resumable peer state; if that state is not available after server loss,
the old token cannot match.

A matching token lets the server conclude that a pre-cursor membership
difference is not evidence of an authorization change. It does **not** assert
payload possession (the ordinary known-state body/repair rules still apply),
nor does a mismatched token itself prove that membership is unreconstructible.

The reset rule has two bounds. A reset is **required** when authorization
progress differs and the resulting membership cannot be reconstructed from the
data cursor (a removal or a newly visible member settled at or before `p`). A
reset is **forbidden** when authorization progress matches and the data cursor
is sufficient; when it is not sufficient, the server sends the smallest
expressible incremental repair and resets only if that repair cannot be encoded
as normal additions/removals. Conversely, an authorization-token mismatch with
only post-cursor additions is reconstructible and therefore must not reset.

Every `ViewUpdate` carries `settled_through`, the core-assigned global time
through which the canonical binding view was evaluated. Its meaning is per
binding view: this update reflects every global change at or before that
time that can affect the served view, including authorization and revocation
effects. It does not claim that the receiver possesses unrelated transactions,
and neither density nor numerical adjacency is required: the authority may
advance one binding directly across arbitrarily many irrelevant commits. It may
be persisted and reused across reconnects
or edges serving the same authoritative database lineage for known-state payload
dedup and repair. It is not an active-connection receipt: a subscription is
settled, and a usage-site one-shot attachment is remotely covered, only after
the selected continuously live upstream connection has sent a fresh confirming
`ViewUpdate`. A fresh `Edge`/`Global` one-shot requires that confirmation for
its exact current usage-site `SubscriptionKey`; a late update for a detached
predecessor cannot satisfy the new attachment even when shape, binding, and
options are equal. Disconnect, client restart, edge switch, or applying any
view update from a nonselected upstream immediately retires all
selected-authority receipts and
makes cached rows unsettled/local until the selected authority reconfirms. A stale cursor can
under-claim knowledge and cause extra bodies to ship;
it cannot over-claim because rows entering the view after `p` have membership
settle positions after `p`, and therefore do not satisfy the skip rule below.
After a nonselected update at cut `p`, a selected link's queued confirmation at
an earlier cut cannot restore settlement; its confirming `settled_through` must
reach at least `p`. The same floor applies to fallback-staged or deferred
updates marked ineligible for an authority receipt, even if their link becomes
selected before the update is finally applied.

Only cores are history-complete. An edge or client therefore tracks
`settled_through` per binding/subscription as proof that each exact result is
materialized. A fresh subscription requires its own authoritative evaluation; a
receipt for one binding says nothing about another binding's local result. A
validated receipt from the selected authority nevertheless advances the node's
`committed_global_time`: that field is the newest authority-committed coordinate
known to the node, not a claim of local data completeness. When a result is
assembled from multiple binding views, coverage is bounded by the required
views' confirmed cuts. The separate `history_complete` capability determines
whether `committed_global_time` is also a locally readable complete-history
frontier (ch. 3 and ch. 5).

For reconstruction, `settled_through` is necessary but not sufficient. The
receiver must have admitted the complete supporting set, the catalogue needed
to interpret it, and every exact native body for the selected authority usage.
Its local IVM must finish before settlement is published. A fast cursor permits
native-body dedup; it does not stand in for a fresh authority confirmation or
allow partially available inputs. Recovery rebuilds from durable native state
and repairs missing bodies, rather than trusting a persisted projected terminal
cache (`INV-SYNC-34`).

The serving side's skip rule is one comparison (`INV-SYNC-24`): a version body
may be omitted iff the receiver's membership in it is believed — "row in the
query's scope now" under a fast declaration, exact set membership under a slow
declaration — and, for fast declarations, the version settled at or before
`p`. Not-yet-fated versions are always shipped under a fast declaration.
The complete supporting-row set and inventory refs are never omitted — only
payload bodies.

The optimism is bounded by two nets. First, the structural integrity check: a
receiver that encounters a referenced version without holding its body treats
this as a **known-state miss**, not an error. Second, the precise repair
request: the receiver requests exactly the missing `(row_uuid, tx_time,
tx_node_id)` payloads, and the server MUST serve them subject to ordinary read
policy (`INV-SYNC-26`). Convergence is preserved: a stream served under
known-state dedup followed by its repairs MUST be observationally equivalent
to the same stream served without dedup (`INV-SYNC-25`, cf. `INV-SYNC-20`).
A receiver must not fill a gap from another binding's authority receipt or
claim settlement while an exact supporting body is unavailable. A superseded
set is discarded when a newer complete set arrives. The canonical repair-carrying case is
visibility gained without a new version being minted — a policy/membership
change admitting rows whose versions settled at or before `p` (ch. 7);
version-minting scope entry is self-consistent because the entering version
settles above `p`.

Holdings from point-in-time reads dedup conservatively: a version is assumed
held only for rows **unchanged since the declared cut** (current version
settled at or before the cut). The serving side never reconstructs historical
winners for dedup — that is a per-row history walk (O(history) reads), and for
current-view serving it buys nothing: a row changed since the cut must ship
its current version regardless.

This section is the committed replacement for extending
`peer_payload_inventory.complete_tx_payloads` toward partial or version-level
coverage (§8.4, §8.7): the complete-tx inventory remains the implemented
mechanism for non-declared streams, and it is retired rather than extended as
known-state coverage grows.

_Further invariants._ `INV-SYNC-24` — fast and slow declarations omit only
eligible version bodies; `INV-SYNC-25` — dedup + repairs converge to the
undeduped stream; `INV-SYNC-26` — repair requests are exact and policy-checked;
`INV-SYNC-27` — persisted fast declarations require contiguous application and
no eviction; eviction invalidates the persisted fact. Persisting slow exact
declarations is intentionally not part of v1; they are derived from the
receiver's current local store when needed.

### 8.13 Subsumed sync and wire notes

The former SyncManager and query/sync integration notes are folded here as the
same protocol-level rule: subscriptions are desired-state declarations over
validated shapes and bindings, not a separate query transport. A peer registers
the shape, subscribes the binding, receives an initial coverage result, and then
receives live updates driven by maintained-view state (ch. 16). Reconnect should
replay desired subscriptions and locally-authored pending commit units before
falling back to broader snapshots.

There is one wire vocabulary across network links and worker bridges. Browser
main-thread to worker communication may use `postMessage` as a carrier, but the
semantic payload should remain the same wire-frame/SyncMessage envelope used by
network sync. Transport-local batching, compression, and resume metadata must
not leak into row/version encoding.

**Implementation status (2026-07-27).** The receiver still uses the core
staged-batch seam rather than an `OrderedKvStorage` transaction. The wire
envelope has no portable resume credentials or trace/replay ids, and the
canonical cross-language fixture set is incomplete. The ordinary committed-unit
path also remains primarily client-to-core; the client-to-edge-to-core topology
is being exercised incrementally. Worker bridges have not yet converged on the
network wire-frame batches.

### Query-driven reconciliation of current inputs

Query-driven sync remains the freshness mechanism. A cached row that does not
participate in another query need not be refreshed. Applications may maintain
low-priority propagated queries independently of visible UI when they want
continued freshness. This contract is not an all-local-rows background scan and
does not promise bounded wall-clock staleness while disconnected.

A completed selected-authority input scope is compared with the eligible local
inputs of the same query. Missing local inputs follow ordinary delivery. Extra
local inputs require revalidation; absence from the query scope alone MUST NOT
be interpreted as deletion or access loss. The initial implementation covers
current/default scalar roots. It does not establish complete related or negative
dependency reconciliation merely by making final result sets equal.

Candidate discovery uses ordinary client-local visibility, including stale cached
rows that need revalidation. It MUST NOT use a serving query that reevaluates
cached permission rules: those rules can hide the very candidates needing
revalidation, and their evaluation can wait for work owned by the same runtime
pass. Candidate discovery does not authorize delivery; the selected authority
checks current access before returning row versions or an unavailable outcome.

For example, a cached task changes from `done=false` to `done=true` while its
reader is offline. An empty unfinished-task scope does not update the cached
value. Revalidating that extra task obtains its readable current native version;
local IVM then removes it from the unfinished list while an all-tasks query can
still show the updated task. A live-exit push is an eager optimization; a missed
push must not be the only opportunity to repair this query after reconnect.

This exchange also crosses local foreground-to-worker links. A default local
query may read through a durable worker before reaching an Edge or Core; the
immediate hop being Local does not disable reconciliation. Each hop retains its
fresh endpoint epoch and authenticated session independently of whether its
peer is an authority. An ordinary client need not advertise an authority in
order to receive current-row responses. A scope-isolated worker forwards only
its admitted session binding, and the foreground correlates the reply against
the same canonical identity and provider claims. Provider claims must not acquire
synthetic fields merely by passing through a different JavaScript adapter.

The known-row exchange in chapter 7 uses explicit global physical table and row
identities, not a public column named `id`. Batches contain at most 64 distinct
coordinates. The batch cap bounds work in flight, not eventual coverage. Transient
Unknown answers preserve candidates for bounded retries without tight polling.
The exchange is mandatory; missing optional support is not an Unknown outcome.
Query closure, claims
replacement and selected-authority replacement invalidate owned outstanding work.
Pending local writes are not replaced simply because they are absent upstream.

Fresh authoritative inclusion of a locally unavailable row must also trigger
ordered revalidation: scanning only currently visible local rows would otherwise
make exclusion permanent. Readable native payloads are ingested before clearing
an exclusion. Core evaluation sequence is comparable only in its connection
epoch; durable per-row cut/catalogue floors survive epoch and Core changes.
Production wire authority endpoints allocate a nonzero random 64-bit connection
incarnation, independent of process-local counters, so restarting a Core with the
same node identity does not reuse an old receipt epoch. Incarnations are compared
for equality, never numeric order; allocation has the collision probability of
a random 64-bit nonce. The existing wire `u64` encoding is unchanged.

Native row repair MUST preserve a bundle's ViewScoped transaction cardinality.
A withheld parent coordinate in an incomplete transaction remains inconclusive;
a proven wrong parent in a complete transaction still fails validation. This
allows later authorized readmission without fetching forbidden intermediate
versions or weakening parent-coordinate checks.

### Readable negative evidence and the pilot boundary

Consider a query for projects without tasks. A client knows project P and no
related task, so P matches locally. Another node creates task T under P. Core
excludes P, but P's own row version is unchanged. Revalidating only P cannot
correct the client: T is the missing evidence preventing P from matching.

The intended readable-evidence design includes T among the query's synchronized
inputs when the reader may access it, even though T is not a final result row.
Local IVM can then derive the exclusion. A result-derived source closure is not
a complete inventory of such evidence: it may omit both a rejected root and the
rows which caused its rejection. Supporting this requires query-operator input
tracking beyond surviving result contributors; a matching final result count
is not proof of dependency completeness.

If T is unreadable, neither its bytes nor a fact revealing its existence may be
sent without a separate disclosure contract. P MUST NOT be marked inaccessible
merely because unreadable evidence changes membership while P itself remains
readable. This differs from a hidden grant change which actually removes read
access to P: Core can then issue the generic current-unavailable decision for P
without exposing the grant.

This example specifies the design boundary, not implemented negative-query
support. The public query facade currently rejects the needed negative relation
form, and scalar extra-row revalidation does not solve it. Supported positive
related queries also require their relevant child inputs to be reconciled: a
changed tag can remove a task without changing that task. Related-input coverage
and general readable negative evidence remain tracked in #2660; opaque evidence
is deferred.

### Local propagation is not a remote capability

`Propagation::LocalOnly` is a setting on the calling node. It MUST NOT send a
remote query and MUST NOT be implemented by telling another node to stop there.
Every peer subscription with `propagate_upstream=false` MUST be rejected through
the ordinary subscription rejection path, regardless of trust, SYSTEM identity,
Core/Edge role or worker transport. This rule covers both RegisterShape and
Subscribe admission. Local-only API execution remains available on every node.

A browser foreground's strictly local query therefore reads its own cached and
pending state. It does not fetch worker-only rows. A normal propagated query can
still receive worker data. Calling the worker a durable owner does not exempt
its peer protocol from this invariant.

### Both trust boundaries and exact-version repair

An Edge's own SYSTEM query can have a stale extra input after an offline query
exit just as a client can. The trusted Edge-to-Core path must refresh that input
without recording an access-loss marker in SYSTEM shared storage. A delegated
client scope crossing the same trusted connection remains bound to its admitted
client identity and immutable claims.

On an untrusted client-to-Edge path, shared cache possession is never evidence
of permission to disclose. A partial Edge may hold a fresh task fetched for
SYSTEM and an obsolete grant permitting Alice. An explicit repair must not use that cached grant to authorize fresh bytes for
Alice. Ordinary Edge evaluation uses its maintained local policy inputs and also
honors verified Core access-loss decisions for the admitted reader.
Exact-version repair is subject to the same current read authorization contract
as ordinary repair at Core. Knowing a row/transaction coordinate is not a grant.

The pilot's bounded Core-backed repair gate must preserve legitimate missing-body
recovery, rather than silently disabling repair. It may send only the requested
versions authorized for the exact pending client request after a current
Core-backed readable decision. Unknown is not authorization or access loss.
Connection/claims replacement cancels pending repair; trusted SYSTEM and existing
scope-isolated retained-repair semantics remain distinct. Current authorization
does not promise recovery of historical bytes after actual access withdrawal.

### Host-admitted authority query delegation

A verified Admin credential on a SYSTEM, non-bootstrap authority connection may
receive the host-only `AuthorityQueryDelegate` capability. This permits immutable
per-request query policy bindings on trusted Edge-to-Core links. Bare
`TrustedAuthority`, bootstrap `TrustedAdmin`, raw peer roles, and wire claims do
not grant this capability. Existing admission APIs default to no capability;
write authorization and publication trust are unchanged.

A server Edge evaluates ordinary admitted queries and their read policies over
its local data. It forwards the subscription to keep inputs synchronized, but
opening the local evaluator does not require the exact Core-selected result for
that new query. A scope-isolated browser/native client relay has a different
role: it still consumes the selected authority's exact inputs and does not
re-evaluate permissions from an incomplete client cache.

Extra-row and missing-body repairs remain Core-authorized under the admitted
reader in this implementation. A verified current-unavailable decision excludes
that physical row from the Edge's ordinary serving graph for that exact reader
and claims, as well as from client-local reads in that context. It does not
remove shared storage, constrain SYSTEM, or become an input to permission-proof
evaluation. A later verified readable decision clears the exclusion. Unknown,
connection replacement and stale replies retain the existing retry/cancellation
rules. These are delegated reader decisions on a trusted transport, not a claim
that the Edge's own SYSTEM identity lost access.

For example, Alice can read task T through grant G. Core revokes G while updating
T, but T still matches Alice's task filter. Repair must not fetch just T as
SYSTEM and then apply an old cached G: that could disclose T's new content.
The admitted Alice repair returns current-unavailable without that content;
the Edge's serving graph excludes T for Alice. Other readers and SYSTEM retain
their independent access. Fully local repair requires maintained coverage of
all authorization inputs and is outside this bounded restoration.

Strict receivers retain the selected usage's deletion-layer CoveredInput facts
and exact version bodies beside the content graph, since a tombstone contributes
no app tuple. They forward those witnesses as ordinary physical row/version references;
replacement or teardown releases them. They never select a deletion from an
unrelated shared-cache version. This retained state is proportional to the
selected scope's deletion witnesses and changes only with its source receipt.

### Mandatory current-row availability messages

`CurrentRowsRequest`, `CurrentRowsReceipt`, and `CurrentRowsCancel` are mandatory
wire-protocol v1 semantic messages. They require no optional feature bit and use
the existing named postcard control codec and native `VersionCarrier` encoding;
the byte corpus pins all three variants. Ordinary version validation and
authenticated link admission still apply. No compatibility with peers lacking
these messages is promised. Unknown describes indeterminate current availability
or unavailable authority, never missing protocol support. See SPEC 7's bounded
current-row availability contract for authorization and receipt validation.

### Atomic supporting-row view payload

`ViewUpdatePayload.supporting_rows` is the complete supporting physical
row/version set for one subscription. There are no per-query source IDs, role
labels, completeness facts, result members, or input-delta fields on the wire.
A row reference names its permanent physical table UUID, row UUID and exact
native version; the authored table name remains lookup metadata required by the
existing native version-repair API, never a query occurrence identity.

Every non-opening-pending payload is a replacement snapshot, including an empty
set. The receiver installs it atomically only after all referenced versions are
available and validated, then evaluates its ordinary local query over that
physical dataset. Repeated scans of a table consume the same local dataset.
Compiled source slots and graph bookkeeping are receiver-local implementation
details; they are not authority claims transported by the peer. An established
listener still receives ordinary local result deltas: replacing the supporting
snapshot does not reopen the listener or force an application-level reset.
Permission-advice hydration obeys the same catalogue-before-row ordering as an
ordinary subscription. Opening-pending markers contain no supporting rows and
must not initiate missing-version repair. Repair may retain immutable bytes
from an older update, but must not reinstall its supporting set after a later
complete set for the same subscription has arrived. The existing
subscription, authenticated authority, cut, epoch and ordering boundaries remain.
CurrentRows is the separate current/unavailable reconciliation exchange; query
exclusion alone is not global unavailability. This transport simplification does
not claim complete negative-query evidence or add shallow aggregate transport.

Native row-version carriers are unchanged. The named postcard semantic codec
and byte corpus pin this mandatory pre-release layout; old layouts are unsupported.

## Open Questions

- 🔶 [#2660](https://github.com/garden-co/jazz/issues/2660) — Query-driven reconciliation pilot and deferred related/negative-input completeness.

- 🔶 [#2503](https://github.com/garden-co/jazz/issues/2503) — Bound restart-recovered authority publications without exposing an original write separately from its edge-generated merges.
- 🔶 [#1784](https://github.com/garden-co/jazz/issues/1784) — Protocol parking, transport state, materialization options, coverage/subsumption, retention, and version tags.
- 🔶 [#1779](https://github.com/garden-co/jazz/issues/1779) — Catalogue admission and synchronization.
