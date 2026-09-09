//! Native, process-local Jazz relay for mobile and future platform bindings.
//!
//! The relay is deliberately a host component, not another Jazz runtime. It
//! owns a durable [`jazz::db::Db`] over SQLite and serves one in-memory `Db`
//! for each UI runtime over the ordinary Jazz peer protocol. React Native,
//! Swift, and Kotlin bindings put their ABI-specific command codecs above this
//! crate; they do not implement query, write, policy, or sync behavior here.

mod account_crypto;
mod account_session;
pub use account_crypto::{
    jazz_native_relay_account_secret, jazz_native_relay_mint_local_first_token,
};
pub use account_session::{
    jazz_native_relay_host_lease_attach_account_schema_json,
    jazz_native_relay_host_lease_begin_account_session_json,
    jazz_native_relay_host_lease_refresh_account_session,
    jazz_native_relay_host_lease_release_account_session,
};
mod foreground_mutations;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::c_void;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::rc::Rc;
#[cfg(test)]
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak, mpsc};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;

use futures::lock::Mutex as LocalMutex;
use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, PeerConnection, PeerIoPump, ReadOpts,
    SerializedReadResult, SerializedSubscriptionAuthorization, SubscriptionEvent,
    SubscriptionStream, TickScheduler, TickUrgency, Transport, UpdateOptions, UpsertOptions,
    block_on,
};
use jazz::foreground_node_lease::{ForegroundNodeLease, ForegroundNodeLeasePool};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::protocol::SyncMessage;
use jazz::protocol_limits::{MAX_LOGICAL_MESSAGE_BYTES, validate_logical_message_len};
#[cfg(test)]
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::storage_codec_profile::epoch_1_storage_codec_profile;
use jazz::time::TxTime;
use jazz::tools::AppId;
use jazz::tools::native_transport_connector::{
    NativeTransportConnector, NativeTransportError, NativeTransportRequest, NativeTransportTerminal,
};
use jazz::tools::websocket_prelude_auth::AuthConfig;
use jazz::tools::{OpenTransactionId, TransactionId};
use jazz::tx::{DurabilityTier as CoreDurabilityTier, TxId};
use jazz::wire::{TransportError, WireTransport, decode_sync_message, encode_sync_message};
use jazz_native_transport::NativeWebSocketConnector;
use jazz_storage_sqlite::{Durability as SqliteDurability, SqliteStorage};
use thiserror::Error;

/// The first public native-relay ABI. Future breaking command/wire changes
/// receive a distinct version; no historical implementation number is public.
pub const NATIVE_RELAY_ABI_V1: u16 = 1;

const FOREGROUND_WAKE_IMMEDIATE: u8 = 0;
const FOREGROUND_WAKE_DEFERRED: u8 = 1;
const FOREGROUND_WAKE_AFTER: u8 = 2;
const FOREGROUND_WAKE_CANCELLED: u8 = 3;
pub type ForegroundWakeCallback = unsafe extern "C" fn(*mut c_void, u64, u8, u64);

const NATIVE_RELAY_QUEUE_MAX_MESSAGES: usize = 1024;
const NATIVE_RELAY_QUEUE_MAX_BYTES: usize = MAX_LOGICAL_MESSAGE_BYTES;
/// Commands which cross from a platform/JSI call into a relay owner must not
/// be allowed to accumulate without bound. Peer frames have their own byte
/// budget above; this is the independent bound for owner-thread work such as
/// foreground opens, closes, and ticks.
const NATIVE_RELAY_OWNER_COMMAND_MAX: usize = 1_024;
/// One physical queue slot is reserved for serialized host teardown. Ordinary
/// work has its own atomic admission budget and therefore cannot consume it.
const NATIVE_RELAY_OWNER_TEARDOWN_RESERVE: usize = 1;
const NATIVE_RELAY_DRAIN_MAX_MESSAGES: usize = 64;
const NATIVE_RELAY_DRAIN_TARGET_BYTES: usize = 8 * 1024 * 1024;
const NATIVE_RELAY_PUMP_MAX_CLIENTS: usize = 64;
/// Foreground commands must remain bounded even while large-value hydration is
/// waiting for peer I/O. Each operation retains only one local future and is
/// owned by exactly one foreground alias.
const NATIVE_RELAY_FOREGROUND_PENDING_MAX: usize = 64;
/// Foreground commands are copied across the JSI/C boundary and decoded before
/// they reach a foreground owner. Keep this independent from both peer-frame
/// and trusted-admission budgets.
const NATIVE_RELAY_FOREGROUND_COMMAND_MAX_BYTES: usize = 1024 * 1024;
/// Direct C callers are a test/embedding seam, never a platform JSI runtime.
/// Keep their aliases distinct from every platform-issued runtime token.
const DIRECT_FOREGROUND_RUNTIME_TOKEN: u64 = u64::MAX;
/// Open core transactions retain mutable runtime state. Their handles are
/// foreground-local and bounded exactly like suspended foreground operations.
const NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX: usize = 64;
/// Trusted platform admission carries schema and validated claims, but must
/// remain bounded independently of the generic peer-frame budget.
const NATIVE_RELAY_ADMISSION_MAX_BYTES: usize = 1024 * 1024;

/// Codec-owned commands accepted by the native relay C ABI.
///
/// This surface owns relay lifecycle and ordinary peer-frame transport only.
/// JNI/Swift wrappers carry these postcard bytes unchanged; query, mutation,
/// and row semantics remain absent.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum RelayCommandRequest {
    Probe,
    Open {
        supported_abi_minimum: u16,
        supported_abi_maximum: u16,
        admitted_scope: AdmissionCapability,
    },
    Attach {
        relay: u64,
    },
    CloseClient {
        client: u64,
    },
    CloseRelay {
        relay: u64,
    },
    Pump {
        relay: u64,
    },
    /// Give one complete canonical Jazz peer frame to an attached in-memory
    /// UI client. The host never decodes rows or queries here.
    SendClientFrame {
        client: u64,
        frame: Vec<u8>,
    },
    /// Drain frames destined for one attached in-memory UI client.
    ReceiveClientFrames {
        client: u64,
    },
    /// Give one complete canonical Jazz peer frame to the relay's upstream
    /// transport.
    SendRelayFrame {
        relay: u64,
        frame: Vec<u8>,
    },
    /// Drain frames destined for the relay's upstream transport.
    ReceiveRelayFrames {
        relay: u64,
    },
    /// Host-only lifecycle diagnostics. This deliberately exposes handles and
    /// queue depths, never database rows, query state, or auth material.
    Diagnostics {
        relay: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RelayScopeRequest {
    pub app_namespace: String,
    pub storage_namespace: String,
    pub auth_scope: Option<String>,
}

/// Normalized configuration admitted internally after the strict trusted JSON
/// boundary. It is deliberately not a [`RelayCommandRequest`] variant.
#[derive(Clone, Debug, PartialEq)]
struct RelayScopeAdmissionRequest {
    pub scope: RelayScopeRequest,
    pub sqlite_path: String,
    pub schema_json: String,
    pub identity: DbIdentity,
    pub claims: BTreeMap<String, Value>,
}

/// Credential-bearing setup is a private platform-to-relay handoff.  The
/// bearer is decoded only through Jazz's shared *unverified* scope projection
/// so a refresh can select a distinct local cache.  It is never verified,
/// turned into claims, or exposed to postcard/JSI; Edge authentication remains
/// authoritative.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PrivateSessionSetupJson {
    server_url: String,
    app_id: String,
    jwt: String,
    storage_root: String,
}

#[derive(Clone)]
struct PendingPrivateSession {
    claims: BTreeMap<String, Value>,
    scope: RelayScopeRequest,
    sqlite_path: String,
    identity: DbIdentity,
    socket: Option<PrivateRelaySocketSession>,
}

/// Ephemeral native-only input retained only until trusted revocation. It is
/// never part of an admitted scope, relay diagnostics, postcard, or SQLite.
#[derive(Clone, PartialEq, Eq)]
struct PrivateRelaySocketSession {
    server_url: String,
    app_id: String,
    bearer: String,
}

/// JSON-shaped form accepted only by the platform-owned admission C entry
/// point. Keeping this separate from the postcard command codec makes the
/// platform integration practical without letting JavaScript construct scope
/// configuration. Rust still owns validation and normalization.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustedRelayScopeAdmissionJson {
    scope: TrustedRelayScopeJson,
    sqlite_path: String,
    schema_json: String,
    identity: TrustedRelayIdentityJson,
    claims: BTreeMap<String, Value>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustedRelayScopeJson {
    app_namespace: String,
    storage_namespace: String,
    auth_scope: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustedRelayIdentityJson {
    node: jazz::ids::NodeUuid,
    author: jazz::ids::AuthorSubject,
}

impl TrustedRelayScopeAdmissionJson {
    fn normalize(self) -> Result<RelayScopeAdmissionRequest, JazzNativeRelayStatus> {
        if self.sqlite_path.trim().is_empty() {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        let scope = RelayScopeRequest {
            app_namespace: self.scope.app_namespace,
            storage_namespace: self.scope.storage_namespace,
            auth_scope: self.scope.auth_scope,
        };
        RelayScope::from(scope.clone())
            .validate()
            .map_err(relay_status)?;
        if matches!(self.identity.author, jazz::ids::AuthorSubject::System) {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        reject_bearer_claims(&self.claims)?;
        // Parse and reserialize before storing so the trusted boundary has one
        // normalized schema spelling and malformed JSON cannot reach admission.
        let schema_value = serde_json::from_str::<serde_json::Value>(&self.schema_json)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let schema_json = serde_json::to_string(&schema_value)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        Ok(RelayScopeAdmissionRequest {
            scope,
            sqlite_path: self.sqlite_path,
            schema_json,
            identity: DbIdentity {
                node: self.identity.node,
                author: self.identity.author,
            },
            claims: self.claims,
        })
    }
}

fn reject_bearer_claims(claims: &BTreeMap<String, Value>) -> Result<(), JazzNativeRelayStatus> {
    // These values belong exclusively to upstream-session negotiation. The
    // relay receives validated identity claims, never a bearer credential that
    // could be persisted or exposed through diagnostics.
    const CREDENTIAL_CLAIMS: &[&str] = &[
        "authorization",
        "access_token",
        "refresh_token",
        "id_token",
        "bearer_token",
        "token",
    ];
    if claims.keys().any(|key| {
        let normalized = key.to_ascii_lowercase();
        CREDENTIAL_CLAIMS.contains(&normalized.as_str())
    }) {
        return Err(JazzNativeRelayStatus::LifecycleFailure);
    }
    Ok(())
}

/// A bearer may traverse plaintext only to a platform-local Edge used by the
/// test harness or an emulator. Real remote Edge sessions require HTTPS/WSS;
/// accepting arbitrary `http://` here would let a private-session bearer leak
/// before Edge can authenticate it.
fn validate_private_session_endpoint(server_url: &str) -> Result<url::Url, JazzNativeRelayStatus> {
    let url =
        url::Url::parse(server_url.trim()).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
    match url.scheme() {
        "https" => Ok(url),
        "http" if private_plaintext_host_is_allowed(&url) => Ok(url),
        "http" => Err(JazzNativeRelayStatus::LifecycleFailure),
        _ => Err(JazzNativeRelayStatus::LifecycleFailure),
    }
}

fn private_plaintext_host_is_allowed(url: &url::Url) -> bool {
    let Some(host) = url.host_str() else {
        return false;
    };
    let host = host.trim_matches(['[', ']']);
    host.eq_ignore_ascii_case("localhost")
        || matches!(host, "10.0.2.2" | "10.0.3.2")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|address| address.is_loopback())
}

/// Unguessable authority to open one host-admitted native scope.
///
/// Its representation is opaque to JavaScript and platform bindings. They
/// carry its raw 32 bytes only as a handle for ordinary relay commands.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize, serde::Serialize)]
pub struct AdmissionCapability([u8; 32]);

impl std::fmt::Debug for AdmissionCapability {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AdmissionCapability([redacted])")
    }
}

impl From<RelayScopeRequest> for RelayScope {
    fn from(value: RelayScopeRequest) -> Self {
        Self {
            app_namespace: value.app_namespace,
            storage_namespace: value.storage_namespace,
            auth_scope: value.auth_scope,
        }
    }
}

/// Codec-owned response for [`RelayCommandRequest`].
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum RelayCommandResponse {
    Probe {
        abi_version: u16,
    },
    Opened {
        relay: u64,
    },
    Attached {
        client: u64,
    },
    Closed {
        closed: bool,
    },
    Pumped,
    Frames {
        frames: Vec<Vec<u8>>,
    },
    Diagnostics {
        attached_clients: u64,
        inbound_frames: u64,
        outbound_frames: u64,
    },
}

/// Commands for one opaque in-memory foreground `Db`.
///
/// This is intentionally a separate vocabulary from [`RelayCommandRequest`]:
/// relay commands own persistent-relay lifecycle and peer frames, while these
/// commands own the existing byte-oriented `NativeDb` surface for one UI
/// runtime. Both are postcard and are versioned by [`NATIVE_RELAY_ABI_V1`].
/// A caller can carry an opaque foreground handle only after capability-only
/// admission; it can never smuggle an open configuration through this codec.
///
/// Query bytes use the canonical `Query` carrier already produced by the
/// shared JS codec. This is intentionally *not* a second RN query AST.
///
/// This is the settled V1 command vocabulary. Future incompatible changes
/// require a new relay ABI, while a command's established payload is immutable.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundDbCommandRequest {
    /// Verify that this attached foreground is still live and return the ABI.
    Probe,
    /// Run one bounded ordinary core turn for this foreground and its relay.
    Tick,
    /// Materialize the current local-first result for a canonical query.
    All {
        query: Vec<u8>,
        options_json: String,
        transaction: Option<u64>,
    },
    /// Open a local-first subscription for a canonical query.
    Subscribe {
        query: Vec<u8>,
        options_json: String,
    },
    /// Drain currently publishable events without waiting. Each delta is
    /// encoded through `jazz::binding_codec`, exactly like NAPI and WASM.
    DrainSubscription {
        subscription: u64,
    },
    /// Cancel one subscription and wait for the core finalization ack.
    Unsubscribe {
        subscription: u64,
    },
    /// Close this foreground alias. Repeated closes report `closed: false`.
    Close,
    /// Poll one foreground-owned operation which previously suspended on
    /// chunk or peer I/O. Polling never drives the owner thread to completion.
    Poll {
        operation: u64,
    },
    /// Drop one suspended operation. Repeated or unknown cancels report
    /// `cancelled: false`.
    Cancel {
        operation: u64,
    },
    /// Open a foreground-owned core transaction. The host chooses the opaque
    /// handle and binds it permanently to this foreground identity.
    BeginTransaction {
        kind: ForegroundTransactionKind,
    },
    /// Stage one full-cell insert under an open foreground transaction. This
    /// reuses the existing native encoded-cell record vocabulary.
    Insert {
        transaction: u64,
        table: String,
        cells: Vec<u8>,
        row_id: Option<[u8; 16]>,
    },
    /// Stage one full-cell patch under an open foreground transaction.
    Update {
        transaction: u64,
        table: String,
        row_id: [u8; 16],
        patch: Vec<u8>,
    },
    /// Stage one full-cell upsert under an open foreground transaction.
    Upsert {
        transaction: u64,
        table: String,
        row_id: [u8; 16],
        cells: Vec<u8>,
    },
    /// Stage one soft delete under an open foreground transaction.
    Delete {
        transaction: u64,
        table: String,
        row_id: [u8; 16],
    },
    /// Commit one open foreground transaction. The response returns the
    /// public committed `txId`, not the mutable transaction handle.
    CommitTransaction {
        transaction: u64,
    },
    /// Roll back one open foreground transaction. Closing or revoking a
    /// foreground also abandons all its still-open transactions.
    RollbackTransaction {
        transaction: u64,
    },
    /// Wait for a committed foreground transaction to reach authoritative
    /// Core admission. This remains a pending operation so the platform keeps
    /// driving its ordinary native relay ticks while the Edge/Core path runs.
    WaitForCoreTransaction {
        tx_id: [u8; 16],
    },
    WaitForTransaction {
        tx_id: [u8; 16],
        tier: String,
    },
    /// Options use the established native JSON option vocabulary; author identity
    /// remains bound to the admitted foreground capability.
    StageMutation {
        transaction: u64,
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    },
    DisconnectNativeUpstream,
    ReconnectNativeUpstream,
    NativeConnectionStatus,
    NativeSessionMetadata,
    WriteState {
        tx_id: [u8; 16],
    },
    DrainMutationErrors,
    BeginStreamingMutation {
        mutation: ForegroundMutationKind,
        table: String,
        row_id: [u8; 16],
        cells: Vec<u8>,
        column: String,
        options_json: String,
    },
    PushStreamingMutation {
        upload: u64,
        chunk: Vec<u8>,
    },
    FinishStreamingMutation {
        upload: u64,
    },
    AbortStreamingMutation {
        upload: u64,
    },
    LocalCurrentRow {
        table: String,
        row_id: [u8; 16],
    },
    UpdateLargeValues {
        table: String,
        row_id: [u8; 16],
        patch: Vec<u8>,
        descriptors_json: String,
        updated_at_ms: Option<u64>,
    },
    DirectMutation {
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    },
    PermissionAdvice {
        action: ForegroundPermissionAdviceAction,
    },
    /// General shutdown barrier; identity remains bound to the native capability.
    WaitForPendingWrites {
        tier: String,
    },
}

/// Append-only V1 advice grammar: Insert=0, Read=1, Update=2, Delete=3.
/// Identity comes exclusively from the admitted foreground, never this payload.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundPermissionAdviceAction {
    Insert {
        table: String,
        cells: Vec<u8>,
    },
    Read {
        table: String,
        row: [u8; 16],
    },
    Update {
        table: String,
        row: [u8; 16],
        patch: Vec<u8>,
    },
    Delete {
        table: String,
        row: [u8; 16],
    },
}

/// Append-only V1 advice result: Allowed=0, Denied=1, Unknown=2.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundPermissionAdvice {
    Allowed,
    Denied,
    Unknown,
}

/// Frozen postcard mutation ordinals within the V1 StageMutation envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundMutationKind {
    Insert,
    Update,
    Upsert,
    Delete,
    Restore,
}

/// The two existing Jazz transaction semantics. This native byte vocabulary
/// merely selects them; it never interprets permissions, snapshots, or
/// transaction read/write sets itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundTransactionKind {
    Mergeable,
    Exclusive,
}

/// Response for [`ForegroundDbCommandRequest`].
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundDbCommandResponse {
    Probe {
        abi_version: u16,
    },
    Ticked,
    Rows {
        rows: Vec<u8>,
    },
    Subscribed {
        subscription: u64,
    },
    SubscriptionEvents {
        events: Vec<ForegroundSubscriptionEvent>,
    },
    Unsubscribed {
        closed: bool,
    },
    Closed {
        closed: bool,
    },
    /// The operation has suspended; callers must return to the owner loop so
    /// an ordinary Tick can advance peer/chunk I/O before polling again.
    Pending {
        operation: u64,
    },
    /// A previously pending foreground operation failed without producing a
    /// partial binding payload. This is terminal for that operation only.
    OperationError {
        reason: String,
    },
    /// A pending foreground operation was explicitly cancelled.
    Cancelled {
        cancelled: bool,
    },
    TransactionOpened {
        transaction: u64,
    },
    Inserted {
        row_id: [u8; 16],
    },
    MutationStaged,
    TransactionCommitted {
        tx_id: [u8; 16],
    },
    TransactionRolledBack {
        rolled_back: bool,
    },
    TransactionSettled {
        tx_id: [u8; 16],
    },
    NativeConnectionStatus {
        configured: bool,
        explicitly_offline: bool,
        connected: bool,
    },
    NativeSessionMetadata {
        node: [u8; 16],
        registry_authority: String,
        account_id: Option<[u8; 16]>,
        issuer: String,
        user_id: String,
    },
    WriteState {
        state_json: String,
    },
    MutationErrors {
        events_json: String,
    },
    StreamingMutationOpened {
        upload: u64,
    },
    StreamingMutationPushed,
    StreamingMutationAborted {
        aborted: bool,
    },
    MutationCommitted {
        tx_id: [u8; 16],
        row_id: [u8; 16],
    },
    PermissionAdvice {
        advice: ForegroundPermissionAdvice,
    },
}

/// One already-materialized subscription event.  The byte payload deliberately
/// reuses the normal binding codec; the JSI bridge only copies bytes and never
/// interprets row/query state.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ForegroundSubscriptionEvent {
    Delta {
        reset: bool,
        settled: bool,
        tier: String,
        delta: Vec<u8>,
    },
    Rejected {
        reason: String,
    },
    Closed,
    /// Existing terminal-operation JSON codec, alongside ordinary row deltas.
    StructuredDelta {
        reset: bool,
        settled: bool,
        tier: String,
        delta: Vec<u8>,
        terminal_operations_json: String,
    },
}

/// ABI-owned response buffer. On successful execution, `data` is allocated by
/// Rust and must be released exactly once through
/// [`jazz_native_relay_bytes_free`]. Do not copy this struct before freeing it.
#[repr(C)]
pub struct JazzNativeRelayBytes {
    pub data: *mut u8,
    pub len: usize,
}

impl JazzNativeRelayBytes {
    const EMPTY: Self = Self {
        data: std::ptr::null_mut(),
        len: 0,
    };
}

/// Status returned by the native C ABI. Diagnostic strings and Rust error
/// types intentionally remain inside the host binding; callers branch only on
/// these stable classes.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JazzNativeRelayStatus {
    Ok = 0,
    InvalidArgument = 1,
    InvalidCommand = 2,
    EncodeFailure = 3,
    InvalidHandle = 4,
    LifecycleFailure = 5,
    InvalidAbiRange = 6,
    IncompatibleAbi = 7,
    Backpressure = 8,
}

/// Explicit host-owned lifecycle registry for JNI/Swift. No global relay map.
pub struct NativeRelayHost {
    registry: NativeRelayRegistry,
    admitted_scopes: BTreeMap<AdmissionCapability, AdmittedRelayScope>,
    pending_private_sessions: BTreeMap<AdmissionCapability, PendingPrivateSession>,
    account_session_owners: BTreeMap<AdmissionCapability, account_session::AccountSessionOwner>,
    invalidated_runtime_tokens: BTreeSet<u64>,
    private_socket_sessions: BTreeMap<AdmissionCapability, PrivateRelaySocketSession>,
    /// One authenticated upstream worker owns each durable relay scope.  UI
    /// foregrounds are only peer leases on that relay; opening a second root
    /// must never open a competing bearer socket for the same SQLite store.
    private_scope_workers: BTreeMap<RelayScope, PrivateScopeSocketWorker>,
    explicitly_offline_scopes: BTreeSet<RelayScope>,
    relays: BTreeMap<u64, OpenedRelay>,
    clients: BTreeMap<u64, (u64, NativeRelayClient)>,
    /// Foreground aliases opened through the capability-only C ABI. Keeping
    /// this separate from the general command handles makes it impossible for
    /// JSI to turn a guessed number into a relay/client pairing.
    foregrounds: BTreeMap<u64, OpenedForeground>,
    /// Native-relay-owned foreground node leases, partitioned by durable
    /// relay scope. A foreground never chooses or retains this state itself.
    foreground_node_leases: BTreeMap<RelayScope, ForegroundNodeLeasePool>,
    next_handle: u64,
    #[cfg(test)]
    thread_start_counter: Option<Arc<AtomicUsize>>,
}

#[derive(Clone)]
struct AdmittedRelayScope {
    config: RelayOpenConfig,
    claims: BTreeMap<String, Value>,
}

struct OpenedRelay {
    scope: RelayScope,
    admitted_scope: AdmissionCapability,
    relay: NativeRelay,
}

struct PrivateScopeSocketWorker {
    admitted_scope: AdmissionCapability,
    _worker: NativeRelaySocketWorker,
    connected: Arc<AtomicBool>,
    /// A transient bridge/socket failure is observable to foreground calls
    /// until a new authenticated connection succeeds.  This prevents a
    /// background worker from silently turning an upstream failure into an
    /// indefinite query timeout.
    terminal_error: Arc<Mutex<Option<String>>>,
}

struct OpenedForeground {
    scope: RelayScope,
    relay: u64,
    client: u64,
    runtime_token: u64,
    wake: Option<Arc<ForegroundWakeState>>,
    lease: ForegroundNodeLease,
}

#[derive(Clone, Copy)]
struct ForegroundWakeRegistration {
    callback: ForegroundWakeCallback,
    context: usize,
}

/// The only two legitimate endings for a foreground lease. A clean explicit
/// close may return a node after reading its owner-local HLC; every forced
/// teardown is uncertain and therefore permanently retires it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ForegroundTeardown {
    CleanHandoff,
    Retire,
}
impl ForegroundWakeRegistration {
    fn cancelled(self, foreground: u64) {
        unsafe {
            (self.callback)(
                self.context as *mut c_void,
                foreground,
                FOREGROUND_WAKE_CANCELLED,
                0,
            )
        }
    }
}

/// Thread-safe ownership indirection between an owner-local `Db` scheduler and
/// a raw platform callback context.
///
/// Owner queue saturation can prevent the host from immediately replacing the
/// scheduler inside the thread-affine `Db`. Inerting this state is independent
/// of that queue: it synchronizes with an in-flight callback and guarantees
/// that a scheduler retained by the owner can never dereference the context
/// again. Only then may the host emit `CANCELLED`, allowing the platform to
/// release its registration safely.
struct ForegroundWakeState {
    registration: Mutex<Option<ForegroundWakeRegistration>>,
}

impl ForegroundWakeState {
    fn new(registration: ForegroundWakeRegistration) -> Self {
        Self {
            registration: Mutex::new(Some(registration)),
        }
    }

    fn wake(&self, foreground: u64, kind: u8, delay_ms: u64) {
        let registration = self
            .registration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let Some(registration) = *registration else {
            return;
        };
        unsafe {
            (registration.callback)(
                registration.context as *mut c_void,
                foreground,
                kind,
                delay_ms,
            )
        }
    }

    fn is_active(&self) -> bool {
        self.registration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_some()
    }

    /// Prevent every future raw-context call and wait for an in-flight one to
    /// finish before handing the registration back for its final cancellation.
    fn inert(&self) -> Option<ForegroundWakeRegistration> {
        self.registration
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
    }
}

impl Default for NativeRelayHost {
    fn default() -> Self {
        Self {
            registry: NativeRelayRegistry::default(),
            admitted_scopes: BTreeMap::new(),
            pending_private_sessions: BTreeMap::new(),
            account_session_owners: BTreeMap::new(),
            invalidated_runtime_tokens: BTreeSet::new(),
            private_socket_sessions: BTreeMap::new(),
            private_scope_workers: BTreeMap::new(),
            explicitly_offline_scopes: BTreeSet::new(),
            relays: BTreeMap::new(),
            clients: BTreeMap::new(),
            foregrounds: BTreeMap::new(),
            foreground_node_leases: BTreeMap::new(),
            next_handle: 1,
            #[cfg(test)]
            thread_start_counter: None,
        }
    }
}

impl NativeRelayHost {
    fn allocate(&mut self) -> Result<u64, RelayError> {
        let handle = self.next_handle;
        self.next_handle = self
            .next_handle
            .checked_add(1)
            .ok_or(RelayError::ClientIdExhausted)?;
        Ok(handle)
    }

    fn allocate_admission_capability(&self) -> Result<AdmissionCapability, RelayError> {
        loop {
            let mut bytes = [0_u8; 32];
            getrandom::fill(&mut bytes).map_err(|error| RelayError::Entropy(error.to_string()))?;
            let capability = AdmissionCapability(bytes);
            if !self.admitted_scopes.contains_key(&capability) {
                return Ok(capability);
            }
        }
    }

    fn acquire_foreground_lease(
        &mut self,
        scope: &RelayScope,
    ) -> Result<ForegroundNodeLease, JazzNativeRelayStatus> {
        let pool = self
            .foreground_node_leases
            .entry(scope.clone())
            .or_default();
        if let Some(lease) = pool.acquire_reusable() {
            return Ok(lease);
        }
        loop {
            let mut bytes = [0_u8; 16];
            getrandom::fill(&mut bytes)
                .map_err(|error| relay_status(RelayError::Entropy(error.to_string())))?;
            match pool.acquire_fresh(NodeUuid::from_bytes(bytes)) {
                Ok(lease) => return Ok(lease),
                // CSPRNG collision against a live, reusable, or retired node
                // is vanishingly unlikely, but never turn one into a lease
                // alias or a spurious lifecycle failure.
                Err(jazz::foreground_node_lease::ForegroundNodeLeaseError::DuplicateNode) => {}
                Err(jazz::foreground_node_lease::ForegroundNodeLeaseError::InactiveLease) => {
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
            }
        }
    }

    fn clean_foreground_lease(
        &mut self,
        scope: &RelayScope,
        lease: ForegroundNodeLease,
        high_water: TxTime,
    ) -> Result<(), JazzNativeRelayStatus> {
        // This host is the relay's single serialized lifecycle owner. Updating
        // the pool immediately after native-core readout is its atomic durable
        // handoff boundary; a future durable pool backend must perform the
        // exact same transition transactionally before exposing the reuse.
        self.foreground_node_leases
            .get_mut(scope)
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)?
            .clean_handoff(ForegroundNodeLease {
                confirmed_tx_time: high_water,
                ..lease
            })
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)
    }

    fn retire_foreground_lease(&mut self, scope: &RelayScope, lease: ForegroundNodeLease) {
        let Some(pool) = self.foreground_node_leases.get_mut(scope) else {
            return;
        };
        let _ = pool.retire(lease);
    }

    /// Ensure the single native-owned upstream worker for this admitted
    /// persistent scope exists.  Foregrounds attach to the same relay through
    /// ordinary peers; the worker is deliberately owned by the scope rather
    /// than by any foreground or opaque relay alias.
    fn ensure_private_scope_worker(
        &mut self,
        admitted_scope: AdmissionCapability,
        scope: &RelayScope,
        relay: NativeRelay,
        peer_identity: jazz::ids::AuthorSubject,
    ) -> Result<(), JazzNativeRelayStatus> {
        if self.explicitly_offline_scopes.contains(scope) {
            return Ok(());
        }
        let Some(session) = self.private_socket_sessions.get(&admitted_scope).cloned() else {
            return Ok(());
        };
        if let Some(existing) = self.private_scope_workers.get(scope) {
            // Account attachment/refresh promotes one shared snapshot before
            // opening a foreground. Legacy admissions still require exact
            // capability ownership; mismatched snapshots cannot borrow it.
            return (existing.admitted_scope == admitted_scope
                || (self.account_session_owners.contains_key(&admitted_scope)
                    && self
                        .account_session_owners
                        .contains_key(&existing.admitted_scope)
                    && self.private_socket_sessions.get(&existing.admitted_scope)
                        == Some(&session)))
            .then_some(())
            .ok_or(JazzNativeRelayStatus::LifecycleFailure);
        }
        let worker =
            Self::prepare_private_scope_worker(admitted_scope, relay, peer_identity, session)?;
        worker._worker.activate();
        self.private_scope_workers.insert(scope.clone(), worker);
        Ok(())
    }

    fn prepare_private_scope_worker(
        admitted_scope: AdmissionCapability,
        relay: NativeRelay,
        peer_identity: jazz::ids::AuthorSubject,
        session: PrivateRelaySocketSession,
    ) -> Result<PrivateScopeSocketWorker, JazzNativeRelayStatus> {
        let terminal_error = Arc::new(Mutex::new(None));
        let terminal_for_event = Arc::clone(&terminal_error);
        let connected = Arc::new(AtomicBool::new(false));
        let connected_for_event = Arc::clone(&connected);
        let worker = NativeRelaySocketWorker::prepare_with_connector(
            relay,
            NativeRelaySocketConfig {
                server_url: session.server_url,
                app_id: AppId::from_string(&session.app_id)
                    .unwrap_or_else(|_| AppId::from_name(&session.app_id)),
                peer_identity,
                auth: AuthConfig {
                    jwt_token: Some(session.bearer),
                    ..AuthConfig::default()
                },
                reconnect_delay: std::time::Duration::from_secs(1),
                on_event: Arc::new(move |event| match event {
                    NativeRelaySocketEvent::Connected => {
                        connected_for_event.store(true, Ordering::Release);
                        *terminal_for_event
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
                    }
                    NativeRelaySocketEvent::TerminalError(error) => {
                        connected_for_event.store(false, Ordering::Release);
                        *terminal_for_event
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(error);
                    }
                    NativeRelaySocketEvent::Reconnecting | NativeRelaySocketEvent::Stopped => {
                        connected_for_event.store(false, Ordering::Release);
                    }
                }),
            },
            Arc::new(NativeWebSocketConnector),
        )
        .map_err(relay_status)?;
        Ok(PrivateScopeSocketWorker {
            admitted_scope,
            _worker: worker,
            connected,
            terminal_error,
        })
    }

    fn foreground_connectivity(
        &mut self,
        foreground: u64,
        disconnect: Option<bool>,
    ) -> Result<ForegroundDbCommandResponse, JazzNativeRelayStatus> {
        let opened = self
            .foregrounds
            .get(&foreground)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        let scope = opened.scope.clone();
        let relay = self
            .relays
            .get(&opened.relay)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        let capability = relay.admitted_scope;
        let native_relay = relay.relay.clone();
        let configured = self.private_socket_sessions.contains_key(&capability);
        if disconnect.is_some() && !configured {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        match disconnect {
            Some(true) => {
                // Drop cancels and joins the native bearer socket. Only publish
                // explicit offline after the real transport has stopped.
                self.private_scope_workers.remove(&scope);
                self.explicitly_offline_scopes.insert(scope.clone());
            }
            Some(false) => {
                let author = self
                    .admitted_scopes
                    .get(&capability)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .config
                    .identity
                    .author;
                let was_offline = self.explicitly_offline_scopes.remove(&scope);
                if let Err(error) =
                    self.ensure_private_scope_worker(capability, &scope, native_relay, author)
                {
                    if was_offline {
                        self.explicitly_offline_scopes.insert(scope.clone());
                    }
                    return Err(error);
                }
            }
            None => {}
        }
        Ok(ForegroundDbCommandResponse::NativeConnectionStatus {
            configured,
            explicitly_offline: self.explicitly_offline_scopes.contains(&scope),
            connected: self
                .private_scope_workers
                .get(&scope)
                .is_some_and(|worker| worker.connected.load(Ordering::Acquire)),
        })
    }

    fn private_scope_terminal_error(&self, scope: &RelayScope) -> Option<String> {
        self.private_scope_workers.get(scope).and_then(|worker| {
            worker
                .terminal_error
                .lock()
                .ok()
                .and_then(|error| error.clone())
        })
    }

    fn execute(
        &mut self,
        command: RelayCommandRequest,
    ) -> Result<RelayCommandResponse, JazzNativeRelayStatus> {
        match command {
            RelayCommandRequest::Probe => Ok(RelayCommandResponse::Probe {
                abi_version: NATIVE_RELAY_ABI_V1,
            }),
            RelayCommandRequest::Open {
                supported_abi_minimum,
                supported_abi_maximum,
                admitted_scope,
            } => {
                let supported_abi = NativeRelayAbiRange {
                    minimum: supported_abi_minimum,
                    maximum: supported_abi_maximum,
                };
                ensure_native_relay_abi_compatible(supported_abi).map_err(|error| match error {
                    RelayError::InvalidAbiRange { .. } => JazzNativeRelayStatus::InvalidAbiRange,
                    RelayError::IncompatibleAbi { .. } => JazzNativeRelayStatus::IncompatibleAbi,
                    _ => JazzNativeRelayStatus::LifecycleFailure,
                })?;
                let mut config = self
                    .admitted_scopes
                    .get(&admitted_scope)
                    .map(|admitted| admitted.config.clone())
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                config.supported_abi = supported_abi;
                let scope = config.scope.clone();
                let peer_identity = config.identity.author;
                let relay = self.registry.open(config).map_err(relay_status)?;
                self.ensure_private_scope_worker(
                    admitted_scope,
                    &scope,
                    relay.clone(),
                    peer_identity,
                )?;
                let handle = self
                    .allocate()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                self.relays.insert(
                    handle,
                    OpenedRelay {
                        scope,
                        admitted_scope,
                        relay,
                    },
                );
                Ok(RelayCommandResponse::Opened { relay: handle })
            }
            RelayCommandRequest::Attach {
                relay: relay_handle,
            } => {
                let (relay, author, claims) = {
                    let opened = self
                        .relays
                        .get(&relay_handle)
                        .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                    let admitted = self
                        .admitted_scopes
                        .get(&opened.admitted_scope)
                        .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                    (
                        opened.relay.clone(),
                        admitted.config.identity.author,
                        admitted.claims.clone(),
                    )
                };
                let handle = self
                    .allocate()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                let client = relay
                    .attach_client(fresh_client_identity(author).map_err(relay_status)?, claims)
                    .map_err(relay_status)?;
                self.clients.insert(handle, (relay_handle, client));
                Ok(RelayCommandResponse::Attached { client: handle })
            }
            RelayCommandRequest::CloseClient { client } => Ok(RelayCommandResponse::Closed {
                closed: self
                    .clients
                    .remove(&client)
                    .map(|(_, client)| client.close().is_ok())
                    .unwrap_or(false),
            }),
            RelayCommandRequest::CloseRelay { relay } => {
                let Some(opened) = self.relays.remove(&relay) else {
                    return Ok(RelayCommandResponse::Closed { closed: false });
                };
                let scope = opened.scope;
                let clients = self
                    .clients
                    .iter()
                    .filter_map(|(handle, (owner, _))| (*owner == relay).then_some(*handle))
                    .collect::<Vec<_>>();
                for handle in clients {
                    if let Some((_, client)) = self.clients.remove(&handle) {
                        let _ = client.close();
                    }
                }
                let final_alias = !self
                    .relays
                    .values()
                    .any(|remaining| remaining.scope == scope);
                if final_alias && !self.private_scope_workers.contains_key(&scope) {
                    self.registry
                        .close(&scope)
                        .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                }
                Ok(RelayCommandResponse::Closed { closed: true })
            }
            RelayCommandRequest::Pump { relay } => {
                self.relays
                    .get(&relay)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .relay
                    .pump()
                    .map_err(relay_status)?;
                Ok(RelayCommandResponse::Pumped)
            }
            RelayCommandRequest::SendClientFrame { client, frame } => {
                self.clients
                    .get(&client)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .1
                    .wire()
                    .push_inbound_encoded(&frame)
                    .map_err(relay_status)?;
                Ok(RelayCommandResponse::Pumped)
            }
            RelayCommandRequest::ReceiveClientFrames { client } => {
                Ok(RelayCommandResponse::Frames {
                    frames: self
                        .clients
                        .get(&client)
                        .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                        .1
                        .wire()
                        .take_outbound_encoded()
                        .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?,
                })
            }
            RelayCommandRequest::SendRelayFrame { relay, frame } => {
                self.relays
                    .get(&relay)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .relay
                    .wire()
                    .push_inbound_encoded(&frame)
                    .map_err(relay_status)?;
                Ok(RelayCommandResponse::Pumped)
            }
            RelayCommandRequest::ReceiveRelayFrames { relay } => Ok(RelayCommandResponse::Frames {
                frames: self
                    .relays
                    .get(&relay)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                    .relay
                    .wire()
                    .take_outbound_encoded()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?,
            }),
            RelayCommandRequest::Diagnostics { relay } => {
                let relay_handle = relay;
                let relay = self
                    .relays
                    .get(&relay_handle)
                    .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
                let (inbound_frames, outbound_frames) = relay
                    .relay
                    .wire()
                    .queue_depths()
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
                Ok(RelayCommandResponse::Diagnostics {
                    attached_clients: self
                        .clients
                        .iter()
                        .filter(|(_, (owner, _))| *owner == relay_handle)
                        .count() as u64,
                    inbound_frames: inbound_frames as u64,
                    outbound_frames: outbound_frames as u64,
                })
            }
        }
    }

    /// Open one in-memory foreground client for an already admitted scope.
    ///
    /// This deliberately bypasses the postcard lifecycle envelope: the
    /// capability is not a JavaScript command payload, and the C caller gets
    /// only an opaque foreground handle. Internally it still uses the same
    /// `NativeRelay` owner thread and ordinary peer link as `Open` + `Attach`.
    fn open_foreground(
        &mut self,
        admitted_scope: AdmissionCapability,
        runtime_token: u64,
    ) -> Result<u64, JazzNativeRelayStatus> {
        if runtime_token == 0 {
            return Err(JazzNativeRelayStatus::InvalidArgument);
        }
        if self.invalidated_runtime_tokens.contains(&runtime_token) {
            return Err(JazzNativeRelayStatus::InvalidHandle);
        }
        if self
            .account_session_owners
            .get(&admitted_scope)
            .is_some_and(|owner| owner.runtime_token != runtime_token)
        {
            return Err(JazzNativeRelayStatus::InvalidHandle);
        }
        let (config, author, claims) = {
            let admitted = self
                .admitted_scopes
                .get(&admitted_scope)
                .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
            (
                admitted.config.clone(),
                admitted.config.identity.author,
                admitted.claims.clone(),
            )
        };
        let scope = config.scope.clone();
        let relay = self.registry.open(config).map_err(relay_status)?;
        self.ensure_private_scope_worker(admitted_scope, &scope, relay.clone(), author)?;
        let relay_handle = self
            .allocate()
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        self.relays.insert(
            relay_handle,
            OpenedRelay {
                scope: scope.clone(),
                admitted_scope,
                relay: relay.clone(),
            },
        );
        let client_handle = self
            .allocate()
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let lease = self.acquire_foreground_lease(&scope)?;
        let client = match relay.attach_foreground_client(
            DbIdentity {
                node: lease.node,
                author,
            },
            claims,
            lease,
        ) {
            Ok(client) => client,
            Err(error) => {
                // The relay alias was not yet observable to the caller. Undo
                // it before returning the attachment failure.
                self.relays.remove(&relay_handle);
                let final_alias = !self.relays.values().any(|opened| opened.scope == scope);
                if final_alias && !self.private_scope_workers.contains_key(&scope) {
                    let _ = self.registry.close(&scope);
                }
                // No foreground runtime observed this lease, so it is a known
                // zero-high-water failure rather than an uncertain handoff.
                if self
                    .clean_foreground_lease(&scope, lease, TxTime::default())
                    .is_err()
                {
                    self.retire_foreground_lease(&scope, lease);
                }
                return Err(relay_status(error));
            }
        };
        self.clients.insert(client_handle, (relay_handle, client));
        let foreground = match self.allocate() {
            Ok(foreground) => foreground,
            Err(_) => {
                if let Some((_, client)) = self.clients.remove(&client_handle) {
                    let _ = client.close();
                }
                self.relays.remove(&relay_handle);
                let final_alias = !self.relays.values().any(|opened| opened.scope == scope);
                if final_alias && !self.private_scope_workers.contains_key(&scope) {
                    let _ = self.registry.close(&scope);
                }
                // A client existed briefly. Its close failure means the
                // runtime may have minted after attachment, so conservatively
                // retire rather than returning the zero-water lease.
                self.retire_foreground_lease(&scope, lease);
                return Err(JazzNativeRelayStatus::LifecycleFailure);
            }
        };
        self.foregrounds.insert(
            foreground,
            OpenedForeground {
                scope,
                relay: relay_handle,
                client: client_handle,
                runtime_token,
                wake: None,
                lease,
            },
        );
        Ok(foreground)
    }

    fn tick_foreground(&mut self, foreground: u64) -> Result<(), JazzNativeRelayStatus> {
        let opened = self
            .foregrounds
            .get(&foreground)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        let (relay, scope) = (opened.relay, opened.scope.clone());
        if self.private_scope_terminal_error(&scope).is_some() {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        self.relays
            .get(&relay)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        self.foreground_client(foreground)?
            .pump_foreground()
            .map_err(relay_status)
    }

    /// Lease-scoped C ABI calls must not turn an opaque foreground handle into
    /// a cross-runtime capability. Handles are host-global, while a retained
    /// platform lease is scoped to exactly one installed JSI runtime.
    ///
    /// A missing handle remains valid for idempotent close; operations that
    /// require a live foreground perform their ordinary liveness check after
    /// this guard. A live handle owned by another runtime always fails.
    fn require_lease_foreground_runtime(
        &self,
        runtime_token: u64,
        foreground: u64,
    ) -> Result<(), JazzNativeRelayStatus> {
        match self.foregrounds.get(&foreground) {
            Some(opened) if opened.runtime_token == runtime_token => Ok(()),
            Some(_) => Err(JazzNativeRelayStatus::InvalidHandle),
            None => Ok(()),
        }
    }

    fn foreground_client(
        &self,
        foreground: u64,
    ) -> Result<&NativeRelayClient, JazzNativeRelayStatus> {
        let client = self
            .foregrounds
            .get(&foreground)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?
            .client;
        self.clients
            .get(&client)
            .map(|(_, client)| client)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)
    }

    /// Retire or cleanly hand off exactly one foreground alias.
    ///
    /// Removing the public handle is deliberately the first step. It prevents
    /// a concurrent/later native call from beginning more work while this
    /// function drains the owner-thread state. The Rust scheduler is then
    /// cleared synchronously before the platform callback receives its
    /// cancellation notification; a queued platform turn may still run, but
    /// it has no native scheduler or foreground handle to re-enter.
    fn teardown_foreground(
        &mut self,
        foreground_handle: u64,
        teardown: ForegroundTeardown,
    ) -> Result<bool, JazzNativeRelayStatus> {
        let Some(foreground) = self.foregrounds.remove(&foreground_handle) else {
            return Ok(false);
        };

        // Do not use `set_foreground_wake_callback` here: the foreground was
        // intentionally removed above to make all new public work fail
        // synchronously. The retained client still reaches the owner thread
        // directly and clears its `Db` scheduler before its raw platform
        // callback context can be released.
        let client = self
            .clients
            .get(&foreground.client)
            .map(|(_, client)| client.clone());
        // First inert the shared state without touching the saturated owner
        // queue. This is the actual raw-context ownership boundary: even if
        // the owner retains its scheduler, it can no longer call the platform.
        let wake = foreground.wake.and_then(|wake| wake.inert());
        let wake_cleared = client
            .as_ref()
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)
            .and_then(|client| {
                client
                    .set_foreground_wake_callback(foreground_handle, None)
                    .map_err(relay_status)
            });
        if let Some(wake) = wake {
            wake.cancelled(foreground_handle);
        }

        // Read out the runtime-owned HLC only for an explicit clean close.
        // Revocation and platform invalidation have no supported clean
        // handoff, even if this synchronous teardown happens to succeed.
        let high_water = match teardown {
            ForegroundTeardown::CleanHandoff => client
                .as_ref()
                .ok_or(JazzNativeRelayStatus::LifecycleFailure)
                .and_then(|client| client.minted_tx_time_high_water().map_err(relay_status)),
            ForegroundTeardown::Retire => Ok(TxTime::default()),
        };

        // `NativeRelayClient::close` removes the owner-local client. Dropping
        // it aborts pending foreground futures/subscriptions and its explicit
        // transaction drain abandons every mutable transaction handle.
        let client_closed = self
            .clients
            .remove(&foreground.client)
            .map(|(_, client)| client.close())
            .transpose()
            .map_err(relay_status);
        let opened = self.relays.remove(&foreground.relay);
        let final_alias = !self
            .relays
            .values()
            .any(|remaining| remaining.scope == foreground.scope);
        let relay_closed = match opened {
            Some(opened)
                if final_alias && !self.private_scope_workers.contains_key(&foreground.scope) =>
            {
                self.registry
                    .close(&opened.scope)
                    .map(|_| ())
                    .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)
            }
            Some(_) => Ok(()),
            None => Err(JazzNativeRelayStatus::LifecycleFailure),
        };
        let owner_closed = matches!(client_closed, Ok(Some(())));
        let relay_closed = relay_closed.is_ok();
        let wake_cleared = wake_cleared.is_ok();
        match teardown {
            ForegroundTeardown::CleanHandoff
                if wake_cleared && owner_closed && relay_closed && high_water.is_ok() =>
            {
                if self
                    .clean_foreground_lease(
                        &foreground.scope,
                        foreground.lease,
                        high_water.expect("clean handoff already checked high water"),
                    )
                    .is_err()
                {
                    self.retire_foreground_lease(&foreground.scope, foreground.lease);
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
            }
            ForegroundTeardown::Retire => {
                self.retire_foreground_lease(&foreground.scope, foreground.lease);
                if !wake_cleared || !owner_closed || !relay_closed {
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
            }
            ForegroundTeardown::CleanHandoff => {
                self.retire_foreground_lease(&foreground.scope, foreground.lease);
                return Err(JazzNativeRelayStatus::LifecycleFailure);
            }
        }
        Ok(true)
    }

    fn close_foreground(&mut self, foreground_handle: u64) -> Result<bool, JazzNativeRelayStatus> {
        self.teardown_foreground(foreground_handle, ForegroundTeardown::CleanHandoff)
    }

    /// Platform runtime invalidation is an uncertain shutdown. It must retire
    /// only aliases owned by that token, preserve sibling runtimes, and never
    /// return a node identity to the reusable pool without a confirmed HLC
    /// handoff.
    fn retire_foregrounds_for_runtime(
        &mut self,
        runtime_token: u64,
    ) -> Result<(), JazzNativeRelayStatus> {
        self.invalidated_runtime_tokens.insert(runtime_token);
        let foregrounds = self
            .foregrounds
            .iter()
            .filter_map(|(handle, opened)| {
                (opened.runtime_token == runtime_token).then_some(*handle)
            })
            .collect::<Vec<_>>();
        let mut failed = false;
        for handle in foregrounds {
            if self
                .teardown_foreground(handle, ForegroundTeardown::Retire)
                .is_err()
            {
                failed = true;
            }
        }
        let sessions = self
            .account_session_owners
            .iter()
            .filter_map(|(capability, owner)| {
                (owner.runtime_token == runtime_token).then_some(*capability)
            })
            .collect::<Vec<_>>();
        for session in sessions {
            if self.revoke_scope(session).is_err() {
                failed = true;
            }
        }
        (!failed)
            .then_some(())
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)
    }

    fn set_foreground_wake_callback(
        &mut self,
        foreground: u64,
        callback: Option<ForegroundWakeCallback>,
        context: usize,
    ) -> Result<(), JazzNativeRelayStatus> {
        let client = self.foreground_client(foreground)?.clone();
        let wake = callback.map(|callback| {
            Arc::new(ForegroundWakeState::new(ForegroundWakeRegistration {
                callback,
                context,
            }))
        });
        client
            .set_foreground_wake_callback(foreground, wake.clone())
            .map_err(relay_status)?;
        let previous = std::mem::replace(
            &mut self
                .foregrounds
                .get_mut(&foreground)
                .ok_or(JazzNativeRelayStatus::InvalidHandle)?
                .wake,
            wake,
        );
        // The owner command completed before its response, so the prior
        // scheduler is already dropped. Inerting its host-held state now is
        // only defensive against future changes which retain scheduler clones.
        if let Some(previous) = previous {
            let _ = previous.inert();
        }
        Ok(())
    }

    fn admit_scope(
        &mut self,
        request: RelayScopeAdmissionRequest,
    ) -> Result<AdmissionCapability, JazzNativeRelayStatus> {
        RelayScope::from(request.scope.clone())
            .validate()
            .map_err(relay_status)?;
        if matches!(request.identity.author, jazz::ids::AuthorSubject::System) {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        reject_bearer_claims(&request.claims)?;
        let public_schema = serde_json::from_str(&request.schema_json)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let schema =
            JazzSchema::new(&public_schema).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let config = RelayOpenConfig {
            supported_abi: NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_V1,
                maximum: NATIVE_RELAY_ABI_V1,
            },
            scope: request.scope.into(),
            sqlite_path: PathBuf::from(request.sqlite_path),
            schema,
            identity: request.identity,
            #[cfg(test)]
            thread_start_counter: self.thread_start_counter.clone(),
        };
        // A scope is immutable once trusted code has admitted it. Reject a
        // conflicting second configuration before JavaScript can receive a
        // capability, even if no relay alias has been opened yet.
        if self.admitted_scopes.values().any(|admitted| {
            admitted.config.scope == config.scope
                && (admitted.config.sqlite_path != config.sqlite_path
                    || admitted.config.schema.version_id() != config.schema.version_id()
                    || admitted.config.identity != config.identity
                    || admitted.claims != request.claims)
        }) {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        let handle = self.allocate_admission_capability().map_err(relay_status)?;
        self.admitted_scopes.insert(
            handle,
            AdmittedRelayScope {
                config,
                claims: request.claims,
            },
        );
        Ok(handle)
    }

    fn begin_private_session(
        &mut self,
        request: PrivateSessionSetupJson,
    ) -> Result<AdmissionCapability, JazzNativeRelayStatus> {
        let endpoint = validate_private_session_endpoint(&request.server_url)?;
        let origin = Some(endpoint.origin().ascii_serialization())
            .filter(|origin| origin != "null")
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)?;
        let app_id = request.app_id.trim();
        if app_id.is_empty() || request.storage_root.trim().is_empty() {
            return Err(JazzNativeRelayStatus::LifecycleFailure);
        }
        let (issuer, subject) = jazz::tools::unverified_jwt_scope_subject(&request.jwt)
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)?;
        let author = jazz::ids::AuthorSubject::authenticated(&issuer, &subject)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let canonical_subject = author.canonical().to_owned();
        let scope = RelayScopeRequest {
            app_namespace: origin,
            storage_namespace: app_id.to_owned(),
            auth_scope: Some(canonical_subject.clone()),
        };
        RelayScope::from(scope.clone())
            .validate()
            .map_err(relay_status)?;
        let digest = blake3::hash(
            format!(
                "{}\0{}\0{}",
                scope.app_namespace, scope.storage_namespace, canonical_subject
            )
            .as_bytes(),
        );
        let sqlite_path = PathBuf::from(request.storage_root)
            .join(format!("{}.sqlite", digest.to_hex()))
            .display()
            .to_string();
        let mut node = [0_u8; 16];
        getrandom::fill(&mut node).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let capability = self.allocate_admission_capability().map_err(relay_status)?;
        self.pending_private_sessions.insert(
            capability,
            PendingPrivateSession {
                claims: BTreeMap::new(),
                scope,
                sqlite_path,
                identity: DbIdentity {
                    node: NodeUuid::from_bytes(node),
                    author,
                },
                socket: Some(PrivateRelaySocketSession {
                    server_url: request.server_url,
                    app_id: request.app_id,
                    bearer: request.jwt,
                }),
            },
        );
        Ok(capability)
    }

    fn attach_canonical_schema(
        &mut self,
        session: AdmissionCapability,
        schema_json: &str,
    ) -> Result<AdmissionCapability, JazzNativeRelayStatus> {
        let mut pending = self
            .pending_private_sessions
            .remove(&session)
            .ok_or(JazzNativeRelayStatus::InvalidHandle)?;
        let runtime_owner = self.account_session_owners.remove(&session);
        let account_snapshot = runtime_owner.as_ref().map(|_| {
            (
                pending.claims.clone(),
                pending
                    .socket
                    .as_ref()
                    .map(|socket| socket.bearer.clone())
                    .unwrap_or_default(),
            )
        });
        if runtime_owner.is_some() {
            // Reuse the durable relay identity, but do not publish a new
            // credential/claims snapshot until schema/path validation succeeds.
            let scope = RelayScope::from(pending.scope.clone());
            if let Some((capability, existing)) = self
                .admitted_scopes
                .iter()
                .find(|(_, entry)| entry.config.scope == scope)
            {
                if !self.account_session_owners.contains_key(capability) {
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
                let old_transport = self
                    .private_socket_sessions
                    .get(capability)
                    .map(|socket| (&socket.server_url, &socket.app_id));
                let new_transport = pending
                    .socket
                    .as_ref()
                    .map(|socket| (&socket.server_url, &socket.app_id));
                if old_transport != new_transport {
                    return Err(JazzNativeRelayStatus::LifecycleFailure);
                }
                pending.identity = existing.config.identity;
                pending.claims = existing.claims.clone();
            }
        }
        // Canonicalize at this credential-free boundary before constructing a
        // JazzSchema. A malformed schema consumes the one-shot session setup;
        // callers must restart setup rather than attach a different schema.
        let value = serde_json::from_str::<serde_json::Value>(schema_json)
            .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let schema_json =
            serde_json::to_string(&value).map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        let socket = pending.socket;
        let admitted = self.admit_scope(RelayScopeAdmissionRequest {
            scope: pending.scope,
            sqlite_path: pending.sqlite_path,
            schema_json,
            identity: pending.identity,
            claims: pending.claims,
        })?;
        if let Some(socket) = socket {
            self.private_socket_sessions.insert(admitted, socket);
        }
        if let Some(owner) = runtime_owner {
            self.account_session_owners.insert(admitted, owner);
        }
        if let Some((claims, token)) = account_snapshot
            && let Err(status) = self.promote_account_scope_snapshot(admitted, &token, claims)
        {
            let _ = self.revoke_scope(admitted);
            return Err(status);
        }
        Ok(admitted)
    }

    fn revoke_scope(
        &mut self,
        admitted_scope: AdmissionCapability,
    ) -> Result<bool, JazzNativeRelayStatus> {
        self.account_session_owners.remove(&admitted_scope);
        // Cancellation also consumes a setup that has not attached its schema
        // yet. Otherwise a late attachment could resurrect a canceled session.
        if self
            .pending_private_sessions
            .remove(&admitted_scope)
            .is_some()
        {
            return Ok(true);
        }
        let Some(admitted) = self.admitted_scopes.remove(&admitted_scope) else {
            return Ok(false);
        };
        // A sibling account context retains the shared upstream owner. Its
        // foreground and bearer outlive this context's normal shutdown.
        let successor = self.admitted_scopes.iter().find_map(|(capability, entry)| {
            (entry.config.scope == admitted.config.scope
                && self.account_session_owners.contains_key(capability)
                && self.private_socket_sessions.get(capability)
                    == self.private_socket_sessions.get(&admitted_scope))
            .then_some(*capability)
        });
        if let Some(successor) = successor
            && let Some(worker) = self.private_scope_workers.get_mut(&admitted.config.scope)
            && worker.admitted_scope == admitted_scope
        {
            worker.admitted_scope = successor;
        }
        // Removing the native-only session ensures a later re-admission must
        // provide a fresh bearer. Any opened worker is dropped below with its
        // relay alias, which synchronously cancels its socket thread.
        self.private_socket_sessions.remove(&admitted_scope);
        if !self
            .admitted_scopes
            .values()
            .any(|entry| entry.config.scope == admitted.config.scope)
        {
            self.explicitly_offline_scopes
                .remove(&admitted.config.scope);
        }
        // This is the scope worker's trusted lifetime boundary. Dropping it
        // synchronously cancels and joins its bearer socket before the
        // durable relay can be closed below.
        if self
            .private_scope_workers
            .get(&admitted.config.scope)
            .is_some_and(|worker| worker.admitted_scope == admitted_scope)
        {
            self.private_scope_workers.remove(&admitted.config.scope);
        }
        let relay_handles = self
            .relays
            .iter()
            .filter_map(|(handle, opened)| {
                (opened.admitted_scope == admitted_scope).then_some(*handle)
            })
            .collect::<Vec<_>>();
        // Revocation is an unclean lifecycle boundary. Each foreground must
        // synchronously reject new work, clear its owner wake, abort pending
        // work, and retire its lease after admission has stopped new opens.
        // The old `retain` merely hid aliases and left raw wake contexts plus
        // active node identities behind.
        let foregrounds = self
            .foregrounds
            .iter()
            .filter_map(|(handle, foreground)| {
                relay_handles.contains(&foreground.relay).then_some(*handle)
            })
            .collect::<Vec<_>>();
        let mut failed = false;
        for foreground in foregrounds {
            if self
                .teardown_foreground(foreground, ForegroundTeardown::Retire)
                .is_err()
            {
                failed = true;
            }
        }
        let mut removed_scopes = Vec::new();
        for relay_handle in relay_handles {
            if let Some(opened) = self.relays.remove(&relay_handle) {
                removed_scopes.push(opened.scope);
            }
            let client_handles = self
                .clients
                .iter()
                .filter_map(|(handle, (owner, _))| (*owner == relay_handle).then_some(*handle))
                .collect::<Vec<_>>();
            for client_handle in client_handles {
                if let Some((_, client)) = self.clients.remove(&client_handle) {
                    let _ = client.close();
                }
            }
        }
        removed_scopes.sort();
        removed_scopes.dedup();
        for scope in removed_scopes {
            if !self.relays.values().any(|opened| opened.scope == scope)
                && !self
                    .admitted_scopes
                    .values()
                    .any(|entry| entry.config.scope == scope)
            {
                let _ = self.registry.close(&scope);
            }
        }
        // A scope-owned worker intentionally keeps the durable relay alive
        // after its last foreground lease closes. Revocation must therefore
        // also close a worker-only registry entry; otherwise a fresh private
        // session mints a new relay node but collides with the old SQLite
        // owner on restart.
        if !self
            .admitted_scopes
            .values()
            .any(|remaining| remaining.config.scope == admitted.config.scope)
        {
            self.registry
                .close(&admitted.config.scope)
                .map_err(|_| JazzNativeRelayStatus::LifecycleFailure)?;
        }
        (!failed)
            .then_some(true)
            .ok_or(JazzNativeRelayStatus::LifecycleFailure)
    }
}

/// Mint an ephemeral client identity for one foreground runtime.
///
/// A foreground `Db` owns a fresh in-memory HLC. Its node must therefore be
/// fresh too: deriving it from the host-local handle counter would repeat the
/// first node after every process restart, allowing same-clock restarts to
/// reuse a transaction identity. The persistent relay identity remains the
/// trusted platform configuration; this random identity is only for its
/// short-lived in-memory peers.
fn fresh_client_identity(author: jazz::ids::AuthorSubject) -> Result<DbIdentity, RelayError> {
    let mut node = [0_u8; 16];
    getrandom::fill(&mut node).map_err(|error| RelayError::Entropy(error.to_string()))?;
    Ok(DbIdentity {
        node: jazz::ids::NodeUuid::from_bytes(node),
        author,
    })
}

fn relay_status(error: RelayError) -> JazzNativeRelayStatus {
    match error {
        RelayError::InvalidAbiRange { .. } => JazzNativeRelayStatus::InvalidAbiRange,
        RelayError::IncompatibleAbi { .. } => JazzNativeRelayStatus::IncompatibleAbi,
        RelayError::QueueCapacityExceeded { .. } | RelayError::OwnerQueueFull => {
            JazzNativeRelayStatus::Backpressure
        }
        RelayError::Db(error) if error.code == jazz::db::ErrorCode::Backpressure => {
            JazzNativeRelayStatus::Backpressure
        }
        _ => JazzNativeRelayStatus::LifecycleFailure,
    }
}

/// C ABI seam for Android/JNI, Swift, and other platform artifact wrappers.
///
/// The platform layer may use this probe before it decodes any relay command.
/// It deliberately exposes no `Db`, storage, row, or query handles; commands
/// stay behind the future shared binary relay codec.
#[unsafe(no_mangle)]
pub extern "C" fn jazz_native_relay_abi_version() -> u16 {
    NATIVE_RELAY_ABI_V1
}

/// Execute one codec-owned native relay command.
///
/// `request` is a complete postcard [`RelayCommandRequest`]. On `Ok`, `out`
/// receives Rust-owned postcard [`RelayCommandResponse`] bytes. On any error,
/// `out` is reset to an empty buffer. This function has no storage side effects
/// until a future command explicitly defines them. `out` must already be empty
/// or freed before reuse; resetting an owned buffer would lose its allocation.
///
/// # Safety
///
/// When `request_len` is nonzero, `request` must point to that many readable
/// bytes. `out` must be a valid, writable `JazzNativeRelayBytes` for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_execute(
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    // SAFETY: `out` is non-null and exclusively owned by the caller for this
    // call. Reset it before decoding so every error has one unambiguous state.
    unsafe { *out = JazzNativeRelayBytes::EMPTY };
    if request.is_null() && request_len != 0 {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let request = if request_len == 0 {
        &[]
    } else {
        // SAFETY: non-null was checked above; the caller supplies exactly this
        // many immutable request bytes for the duration of this call.
        unsafe { std::slice::from_raw_parts(request, request_len) }
    };
    let command = match postcard::from_bytes::<RelayCommandRequest>(request) {
        Ok(command) => command,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let response = match command {
        RelayCommandRequest::Probe => RelayCommandResponse::Probe {
            abi_version: NATIVE_RELAY_ABI_V1,
        },
        _ => return JazzNativeRelayStatus::InvalidCommand,
    };
    let bytes = match postcard::to_allocvec(&response) {
        Ok(bytes) => bytes,
        Err(_) => return JazzNativeRelayStatus::EncodeFailure,
    };
    let boxed = bytes.into_boxed_slice();
    // SAFETY: `out` was validated above; the returned allocation remains owned
    // by Rust until the matching free call below.
    unsafe {
        *out = JazzNativeRelayBytes {
            len: boxed.len(),
            data: Box::into_raw(boxed).cast(),
        };
    }
    JazzNativeRelayStatus::Ok
}

/// Opaque C-owned native relay host. It owns one scope registry and all handles.
#[repr(C)]
pub struct JazzNativeRelayHost {
    inner: Arc<Mutex<NativeRelayHost>>,
}

/// Retained native-host ownership for a JSI factory and its foreground
/// HostObjects. A platform may release its `JazzNativeRelayHost` wrapper while
/// JavaScript finalizers still exist; this Arc keeps the Rust host state alive
/// until the final foreground object has become unreachable.
#[repr(C)]
pub struct JazzNativeRelayHostLease {
    inner: Arc<Mutex<NativeRelayHost>>,
    runtime_token: u64,
}

#[unsafe(no_mangle)]
pub extern "C" fn jazz_native_relay_host_new() -> *mut JazzNativeRelayHost {
    Box::into_raw(Box::new(JazzNativeRelayHost {
        inner: Arc::new(Mutex::new(NativeRelayHost::default())),
    }))
}

#[unsafe(no_mangle)]
/// # Safety
///
/// `host` must be null or an unfreed pointer returned by host_new, and no
/// concurrent call may retain or execute it while this function runs.
pub unsafe extern "C" fn jazz_native_relay_host_free(host: *mut JazzNativeRelayHost) {
    if !host.is_null() {
        unsafe {
            drop(Box::from_raw(host));
        }
    }
}

/// Retain a host for private JSI foreground objects.
///
/// The returned lease keeps the host's Rust state alive after the platform
/// releases its original host wrapper. It is intentionally opaque: callers
/// can pass it only to the attached-foreground APIs and must release it once
/// the last factory/HostObject for that JS runtime has been invalidated.
///
/// # Safety
/// `host` must be a live pointer returned by [`jazz_native_relay_host_new`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_retain(
    host: *mut JazzNativeRelayHost,
    runtime_token: u64,
) -> *mut JazzNativeRelayHostLease {
    if host.is_null() || runtime_token == 0 || runtime_token == DIRECT_FOREGROUND_RUNTIME_TOKEN {
        return std::ptr::null_mut();
    }
    let inner = unsafe { Arc::clone(&(&*host).inner) };
    Box::into_raw(Box::new(JazzNativeRelayHostLease {
        inner,
        runtime_token,
    }))
}

/// Release one host lease returned by [`jazz_native_relay_host_retain`].
///
/// # Safety
/// `lease` must be null or an unfreed pointer returned by host_retain.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_free(lease: *mut JazzNativeRelayHostLease) {
    if !lease.is_null() {
        unsafe { drop(Box::from_raw(lease)) };
    }
}

/// Retire every alias belonging to this platform runtime. Repeating it is
/// safe, and it intentionally does not cleanly recycle foreground node IDs.
///
/// # Safety
/// `lease` must be a live lease returned by host_retain.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_invalidate_foreground_runtime(
    lease: *mut JazzNativeRelayHostLease,
) -> JazzNativeRelayStatus {
    if lease.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    match host.retire_foregrounds_for_runtime(lease.runtime_token) {
        Ok(()) => JazzNativeRelayStatus::Ok,
        Err(status) => status,
    }
}

/// Execute lifecycle commands against one explicit host context.
/// `out` must already be empty or freed before reuse.
///
/// # Safety
/// `host`, request bytes, and `out` follow the same validity rules as
/// [`jazz_native_relay_execute`]; `host` must be returned by host_new and not freed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_execute(
    host: *mut JazzNativeRelayHost,
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe {
        *out = JazzNativeRelayBytes::EMPTY;
    }
    if host.is_null() || (request.is_null() && request_len != 0) {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let request = if request_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(request, request_len) }
    };
    let command = match postcard::from_bytes::<RelayCommandRequest>(request) {
        Ok(command) => command,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    let response = match host.execute(command) {
        Ok(response) => response,
        Err(status) => return status,
    };
    let bytes = match postcard::to_allocvec(&response) {
        Ok(bytes) => bytes,
        Err(_) => return JazzNativeRelayStatus::EncodeFailure,
    };
    let boxed = bytes.into_boxed_slice();
    unsafe {
        *out = JazzNativeRelayBytes {
            len: boxed.len(),
            data: Box::into_raw(boxed).cast(),
        };
    }
    JazzNativeRelayStatus::Ok
}

/// Admit one complete trusted scope described as strict JSON by Kotlin or
/// Swift/Objective-C. This is intentionally a separate platform-only entry
/// point: generic JavaScript `execute` accepts only [`RelayCommandRequest`]
/// and can never carry paths, schema, claims, or bearer credentials.
///
/// The returned bytes are exactly one random 256-bit capability. They are
/// opaque to JavaScript; the platform host may hand them to foreground code,
/// but only the native relay can interpret them.
///
/// # Safety
/// `host`, request bytes, and `out` follow the same rules as
/// [`jazz_native_relay_host_execute`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_admit_scope_json(
    host: *mut JazzNativeRelayHost,
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe { *out = JazzNativeRelayBytes::EMPTY };
    if host.is_null()
        || request.is_null()
        || request_len == 0
        || request_len > NATIVE_RELAY_ADMISSION_MAX_BYTES
    {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let request = unsafe { std::slice::from_raw_parts(request, request_len) };
    let request = match serde_json::from_slice::<TrustedRelayScopeAdmissionJson>(request) {
        Ok(request) => request,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let request = match request.normalize() {
        Ok(request) => request,
        Err(status) => return status,
    };
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    let capability = match host.admit_scope(request) {
        Ok(capability) => capability,
        Err(status) => return status,
    };
    let mut capability = capability.0.to_vec();
    unsafe {
        *out = JazzNativeRelayBytes {
            len: capability.len(),
            data: capability.as_mut_ptr(),
        };
    }
    std::mem::forget(capability);
    JazzNativeRelayStatus::Ok
}

/// Begin private native session setup. This accepts a bearer only on the
/// trusted platform boundary and uses its unverified payload solely to choose
/// the local SQLite partition. It returns an opaque one-shot setup capability;
/// a schema must be attached separately without credentials.
///
/// # Safety
/// `host`, request bytes, and `out` obey the same validity and ownership rules
/// as [`jazz_native_relay_host_admit_scope_json`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_begin_private_session_json(
    host: *mut JazzNativeRelayHost,
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe { *out = JazzNativeRelayBytes::EMPTY };
    if host.is_null()
        || request.is_null()
        || request_len == 0
        || request_len > NATIVE_RELAY_ADMISSION_MAX_BYTES
    {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let request = unsafe { std::slice::from_raw_parts(request, request_len) };
    let request = match serde_json::from_slice::<PrivateSessionSetupJson>(request) {
        Ok(request) => request,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    let capability = match host.begin_private_session(request) {
        Ok(capability) => capability,
        Err(status) => return status,
    };
    let mut bytes = capability.0.to_vec();
    unsafe {
        *out = JazzNativeRelayBytes {
            data: bytes.as_mut_ptr(),
            len: bytes.len(),
        }
    };
    std::mem::forget(bytes);
    JazzNativeRelayStatus::Ok
}

/// Attach one credential-free canonical schema to a one-shot private setup.
///
/// # Safety
/// `host` and `out` are live ABI pointers; `session_capability` points to
/// exactly 32 readable bytes and `schema_json` points to `schema_len` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_attach_canonical_schema_json(
    host: *mut JazzNativeRelayHost,
    session_capability: *const u8,
    capability_len: usize,
    schema_json: *const u8,
    schema_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe { *out = JazzNativeRelayBytes::EMPTY };
    if host.is_null()
        || session_capability.is_null()
        || capability_len != 32
        || schema_json.is_null()
        || schema_len == 0
        || schema_len > NATIVE_RELAY_ADMISSION_MAX_BYTES
    {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let mut capability = [0; 32];
    unsafe {
        capability.copy_from_slice(std::slice::from_raw_parts(session_capability, 32));
    }
    let schema = unsafe { std::slice::from_raw_parts(schema_json, schema_len) };
    let schema = match std::str::from_utf8(schema) {
        Ok(schema) => schema,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    let capability = match host.attach_canonical_schema(AdmissionCapability(capability), schema) {
        Ok(capability) => capability,
        Err(status) => return status,
    };
    let mut bytes = capability.0.to_vec();
    unsafe {
        *out = JazzNativeRelayBytes {
            data: bytes.as_mut_ptr(),
            len: bytes.len(),
        }
    };
    std::mem::forget(bytes);
    JazzNativeRelayStatus::Ok
}

/// Revoke exactly one opaque 256-bit admission capability held by trusted
/// platform lifecycle code. This avoids making Kotlin/Swift encode a postcard
/// revocation request and remains deliberately unavailable to JavaScript.
///
/// # Safety
/// `host` must be a live host pointer. When non-null, `capability` must point
/// to exactly 32 readable bytes for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_revoke_scope_capability(
    host: *mut JazzNativeRelayHost,
    capability: *const u8,
    capability_len: usize,
) -> JazzNativeRelayStatus {
    if host.is_null() || capability.is_null() || capability_len != 32 {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let capability = unsafe { std::slice::from_raw_parts(capability, capability_len) };
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(capability);
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    match host.revoke_scope(AdmissionCapability(bytes)) {
        Ok(_) => JazzNativeRelayStatus::Ok,
        Err(status) => status,
    }
}

/// Open one actual memory-only foreground client from a trusted 32-byte
/// admission capability.
///
/// This is intentionally not expressible through [`RelayCommandRequest`]: a
/// foreground factory may carry the opaque capability, but it must never be
/// able to smuggle one into the generic JavaScript command channel. The output
/// is an opaque host-local handle, not a Rust `Db` pointer. The foreground
/// client runs on the relay owner thread and is already connected to the
/// admitted scope's persistent relay through the ordinary peer protocol.
///
/// # Safety
/// `host` must be live, `capability` must point to exactly 32 readable bytes,
/// and `out_foreground` must be writable for the duration of this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_open_attached_foreground(
    host: *mut JazzNativeRelayHost,
    capability: *const u8,
    capability_len: usize,
    out_foreground: *mut u64,
) -> JazzNativeRelayStatus {
    if host.is_null() || capability.is_null() || capability_len != 32 || out_foreground.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe { *out_foreground = 0 };
    let capability = unsafe { std::slice::from_raw_parts(capability, capability_len) };
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(capability);
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    match host.open_foreground(AdmissionCapability(bytes), DIRECT_FOREGROUND_RUNTIME_TOKEN) {
        Ok(foreground) => {
            unsafe { *out_foreground = foreground };
            JazzNativeRelayStatus::Ok
        }
        Err(status) => status,
    }
}

/// Perform one bounded ordinary core tick for an attached foreground client.
///
/// This is the first native implementation of an existing `NativeDb` method:
/// it makes no row/object API available to JSI, but proves the handle invokes
/// the real memory `Db` and its peer link on the dedicated owner thread.
/// A full byte codec for reads/writes/subscriptions will extend this same
/// handle rather than open another runtime or access relay SQLite directly.
///
/// # Safety
/// `host` must be live and `foreground` must be an unclosed handle returned by
/// [`jazz_native_relay_host_open_attached_foreground`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_tick_attached_foreground(
    host: *mut JazzNativeRelayHost,
    foreground: u64,
) -> JazzNativeRelayStatus {
    if host.is_null() || foreground == 0 {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    match host.tick_foreground(foreground) {
        Ok(()) => JazzNativeRelayStatus::Ok,
        Err(status) => status,
    }
}

/// Close exactly one attached foreground client. It is intentionally
/// idempotent: a JSI HostObject finalizer and an explicit JavaScript `close`
/// may race during bridge teardown without turning a stale alias into an
/// error. The out flag reports whether this call owned the close transition.
///
/// # Safety
/// `host` must be live and `out_closed` writable for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_close_attached_foreground(
    host: *mut JazzNativeRelayHost,
    foreground: u64,
    out_closed: *mut bool,
) -> JazzNativeRelayStatus {
    if host.is_null() || foreground == 0 || out_closed.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe { *out_closed = false };
    let mut host = match unsafe { (&*host).inner.lock() } {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    match host.close_foreground(foreground) {
        Ok(closed) => {
            unsafe { *out_closed = closed };
            JazzNativeRelayStatus::Ok
        }
        Err(status) => status,
    }
}

/// Lease-safe variant of [`jazz_native_relay_host_open_attached_foreground`].
/// Private JSI factories must use this form rather than retain a raw host
/// pointer across bridge teardown.
///
/// # Safety
/// `lease` must be live, `capability` exactly 32 readable bytes, and
/// `out_foreground` writable for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_open_attached_foreground(
    lease: *mut JazzNativeRelayHostLease,
    capability: *const u8,
    capability_len: usize,
    out_foreground: *mut u64,
) -> JazzNativeRelayStatus {
    if lease.is_null() || capability.is_null() || capability_len != 32 || out_foreground.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe { *out_foreground = 0 };
    let capability = unsafe { std::slice::from_raw_parts(capability, capability_len) };
    let mut bytes = [0_u8; 32];
    bytes.copy_from_slice(capability);
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    match host.open_foreground(AdmissionCapability(bytes), lease.runtime_token) {
        Ok(foreground) => {
            unsafe { *out_foreground = foreground };
            JazzNativeRelayStatus::Ok
        }
        Err(status) => status,
    }
}

/// Lease-safe variant of [`jazz_native_relay_host_tick_attached_foreground`].
///
/// # Safety
/// `lease` must be live for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_tick_attached_foreground(
    lease: *mut JazzNativeRelayHostLease,
    foreground: u64,
) -> JazzNativeRelayStatus {
    if lease.is_null() || foreground == 0 {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    if let Err(status) = host.require_lease_foreground_runtime(lease.runtime_token, foreground) {
        return status;
    }
    match host.tick_foreground(foreground) {
        Ok(()) => JazzNativeRelayStatus::Ok,
        Err(status) => status,
    }
}

/// Lease-safe variant of [`jazz_native_relay_host_close_attached_foreground`].
///
/// # Safety
/// `lease` must be live and `out_closed` writable for this call.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_close_attached_foreground(
    lease: *mut JazzNativeRelayHostLease,
    foreground: u64,
    out_closed: *mut bool,
) -> JazzNativeRelayStatus {
    if lease.is_null() || foreground == 0 || out_closed.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe { *out_closed = false };
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    if let Err(status) = host.require_lease_foreground_runtime(lease.runtime_token, foreground) {
        return status;
    }
    match host.close_foreground(foreground) {
        Ok(closed) => {
            unsafe { *out_closed = closed };
            JazzNativeRelayStatus::Ok
        }
        Err(status) => status,
    }
}

/// Register or clear the native-to-JavaScript wake sink for one attached
/// foreground. The callback may only schedule a later platform turn.
///
/// # Safety
/// `lease` must be live. A non-null callback context must remain valid until
/// this callback is cleared, the foreground is closed/revoked, or the lease is
/// released.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_set_foreground_wake_callback(
    lease: *mut JazzNativeRelayHostLease,
    foreground: u64,
    callback: Option<ForegroundWakeCallback>,
    context: *mut c_void,
) -> JazzNativeRelayStatus {
    if lease.is_null() || foreground == 0 {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    if let Err(status) = host.require_lease_foreground_runtime(lease.runtime_token, foreground) {
        return status;
    }
    match host.set_foreground_wake_callback(foreground, callback, context as usize) {
        Ok(()) => JazzNativeRelayStatus::Ok,
        Err(status) => status,
    }
}

/// Execute one complete postcard [`ForegroundDbCommandRequest`] against an
/// attached foreground `Db` retained by a private JSI factory.
///
/// This is the shared native database-command seam for RN, Swift, and Kotlin:
/// the platform binding only copies bytes in/out, while Rust owns the handle,
/// scheduling, and core operation. It is deliberately *not* reachable through
/// [`RelayCommandRequest`] or the public relay TurboModule command API.
///
/// # Safety
/// `lease` must be live, `foreground` must be an opaque handle returned by an
/// attached-foreground open, `request` must be readable for `request_len`
/// bytes (unless that length is zero), and `out` must be writable. On every
/// error `out` is reset to an empty buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_host_lease_execute_foreground(
    lease: *mut JazzNativeRelayHostLease,
    foreground: u64,
    request: *const u8,
    request_len: usize,
    out: *mut JazzNativeRelayBytes,
) -> JazzNativeRelayStatus {
    if out.is_null() {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    unsafe { *out = JazzNativeRelayBytes::EMPTY };
    if lease.is_null()
        || foreground == 0
        || request_len > NATIVE_RELAY_FOREGROUND_COMMAND_MAX_BYTES
        || (request.is_null() && request_len != 0)
    {
        return JazzNativeRelayStatus::InvalidArgument;
    }
    let request = if request_len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(request, request_len) }
    };
    let command = match decode_foreground_command(request) {
        Ok(command) => command,
        Err(_) => return JazzNativeRelayStatus::InvalidCommand,
    };
    let lease = unsafe { &*lease };
    let mut host = match lease.inner.lock() {
        Ok(host) => host,
        Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
    };
    if let Err(status) = host.require_lease_foreground_runtime(lease.runtime_token, foreground) {
        return status;
    }
    // Every command validates foreground liveness before returning a result.
    // This keeps a stale JSI object from treating `Probe` as a capability
    // oracle after explicit close or platform revocation.
    if !host.foregrounds.contains_key(&foreground)
        && !matches!(command, ForegroundDbCommandRequest::Close)
    {
        return JazzNativeRelayStatus::InvalidHandle;
    }
    let response = match command {
        ForegroundDbCommandRequest::NativeSessionMetadata => {
            let opened = match host.foregrounds.get(&foreground) {
                Some(opened) => opened,
                None => return JazzNativeRelayStatus::InvalidHandle,
            };
            let admitted = match host
                .relays
                .get(&opened.relay)
                .and_then(|relay| host.admitted_scopes.get(&relay.admitted_scope))
            {
                Some(admitted) => admitted,
                None => return JazzNativeRelayStatus::InvalidHandle,
            };
            let author = admitted.config.identity.author;
            let (issuer, user_id) = author.principal_parts();
            ForegroundDbCommandResponse::NativeSessionMetadata {
                node: *opened.lease.node.0.as_bytes(),
                registry_authority: opened.scope.app_namespace.clone(),
                account_id: author.account_id().map(|account| *account.0.as_bytes()),
                issuer,
                user_id,
            }
        }
        ForegroundDbCommandRequest::DisconnectNativeUpstream => {
            match host.foreground_connectivity(foreground, Some(true)) {
                Ok(response) => response,
                Err(status) => return status,
            }
        }
        ForegroundDbCommandRequest::ReconnectNativeUpstream => {
            match host.foreground_connectivity(foreground, Some(false)) {
                Ok(response) => response,
                Err(status) => return status,
            }
        }
        ForegroundDbCommandRequest::NativeConnectionStatus => {
            match host.foreground_connectivity(foreground, None) {
                Ok(response) => response,
                Err(status) => return status,
            }
        }
        ForegroundDbCommandRequest::Probe => ForegroundDbCommandResponse::Probe {
            abi_version: NATIVE_RELAY_ABI_V1,
        },
        ForegroundDbCommandRequest::PermissionAdvice { action } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.request_foreground_permission_advice(action) {
                Ok(poll) => foreground_operation_response(poll),
                Err(error) => ForegroundDbCommandResponse::OperationError {
                    reason: error.to_string(),
                },
            }
        }
        ForegroundDbCommandRequest::Tick => match host.tick_foreground(foreground) {
            Ok(()) => ForegroundDbCommandResponse::Ticked,
            Err(status) => return status,
        },
        ForegroundDbCommandRequest::All {
            query,
            options_json,
            transaction,
        } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.start_foreground_read(query, options_json, transaction) {
                Ok(poll) => foreground_operation_response(poll),
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::LocalCurrentRow { table, row_id } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.local_current_foreground_row(table, row_id) {
                Ok(rows) => ForegroundDbCommandResponse::Rows { rows },
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::Subscribe {
            query,
            options_json,
        } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match foreground_read_opts_from_json(&options_json)
                .and_then(|opts| client.subscribe_foreground_query_with_options(query, opts))
            {
                Ok(subscription) => ForegroundDbCommandResponse::Subscribed { subscription },
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::WaitForPendingWrites { tier } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            let tier = match tier.as_str() {
                "local" => CoreDurabilityTier::Local,
                "edge" => CoreDurabilityTier::Edge,
                "global" => CoreDurabilityTier::Global,
                _ => return JazzNativeRelayStatus::InvalidArgument,
            };
            match client.wait_for_pending_writes(tier) {
                Ok(poll) => foreground_operation_response(poll),
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::WaitForTransaction { tx_id, tier } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            let tier = match tier.as_str() {
                "local" => CoreDurabilityTier::Local,
                "edge" => CoreDurabilityTier::Edge,
                "global" => CoreDurabilityTier::Global,
                _ => return JazzNativeRelayStatus::InvalidArgument,
            };
            match client.wait_for_foreground_transaction(tx_id, tier) {
                Ok(poll) => foreground_operation_response(poll),
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::DrainSubscription { subscription } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.drain_foreground_subscription(subscription) {
                Ok(poll) => foreground_operation_response(poll),
                Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
            }
        }
        ForegroundDbCommandRequest::Unsubscribe { subscription } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.close_foreground_subscription(subscription) {
                Ok(closed) => ForegroundDbCommandResponse::Unsubscribed { closed },
                Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
            }
        }
        ForegroundDbCommandRequest::Close => match host.close_foreground(foreground) {
            Ok(closed) => ForegroundDbCommandResponse::Closed { closed },
            Err(status) => return status,
        },
        ForegroundDbCommandRequest::Poll { operation } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.poll_foreground_operation(operation) {
                Ok(poll) => foreground_operation_response(poll),
                Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
            }
        }
        ForegroundDbCommandRequest::Cancel { operation } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.cancel_foreground_operation(operation) {
                Ok(cancelled) => ForegroundDbCommandResponse::Cancelled { cancelled },
                Err(_) => return JazzNativeRelayStatus::LifecycleFailure,
            }
        }
        ForegroundDbCommandRequest::BeginTransaction { kind } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.begin_foreground_transaction(kind) {
                Ok(transaction) => ForegroundDbCommandResponse::TransactionOpened { transaction },
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::Insert {
            transaction,
            table,
            cells,
            row_id,
        } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.insert_foreground_transaction(transaction, table, cells, row_id) {
                Ok(row_id) => ForegroundDbCommandResponse::Inserted {
                    row_id: *row_id.as_bytes(),
                },
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::Update {
            transaction,
            table,
            row_id,
            patch,
        } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.update_foreground_transaction(transaction, table, row_id, patch) {
                Ok(()) => ForegroundDbCommandResponse::MutationStaged,
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::Upsert {
            transaction,
            table,
            row_id,
            cells,
        } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.upsert_foreground_transaction(transaction, table, row_id, cells) {
                Ok(()) => ForegroundDbCommandResponse::MutationStaged,
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::Delete {
            transaction,
            table,
            row_id,
        } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.delete_foreground_transaction(transaction, table, row_id) {
                Ok(()) => ForegroundDbCommandResponse::MutationStaged,
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::StageMutation {
            transaction,
            mutation,
            table,
            row_id,
            cells,
            options_json,
        } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.stage_foreground_mutation(
                transaction,
                mutation,
                table,
                row_id,
                cells,
                options_json,
            ) {
                Ok(Some(row_id)) => ForegroundDbCommandResponse::Inserted {
                    row_id: *row_id.as_bytes(),
                },
                Ok(None) => ForegroundDbCommandResponse::MutationStaged,
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::CommitTransaction { transaction } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.commit_foreground_transaction(transaction) {
                Ok(tx_id) => ForegroundDbCommandResponse::TransactionCommitted {
                    tx_id: *tx_id.as_bytes(),
                },
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::WaitForCoreTransaction { tx_id } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.wait_for_core_transaction(tx_id) {
                Ok(poll) => foreground_operation_response(poll),
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        ForegroundDbCommandRequest::RollbackTransaction { transaction } => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.rollback_foreground_transaction(transaction) {
                Ok(rolled_back) => {
                    ForegroundDbCommandResponse::TransactionRolledBack { rolled_back }
                }
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
        command @ (ForegroundDbCommandRequest::WriteState { .. }
        | ForegroundDbCommandRequest::DrainMutationErrors
        | ForegroundDbCommandRequest::BeginStreamingMutation { .. }
        | ForegroundDbCommandRequest::PushStreamingMutation { .. }
        | ForegroundDbCommandRequest::FinishStreamingMutation { .. }
        | ForegroundDbCommandRequest::AbortStreamingMutation { .. }
        | ForegroundDbCommandRequest::UpdateLargeValues { .. }
        | ForegroundDbCommandRequest::DirectMutation { .. }) => {
            let client = match host.foreground_client(foreground) {
                Ok(client) => client,
                Err(status) => return status,
            };
            match client.execute_mutation_command(command) {
                Ok(response) => response,
                Err(error) => match foreground_command_error(error) {
                    Ok(response) => response,
                    Err(status) => return status,
                },
            }
        }
    };
    let bytes = match postcard::to_allocvec(&response) {
        Ok(bytes) => bytes,
        Err(_) => return JazzNativeRelayStatus::EncodeFailure,
    };
    let boxed = bytes.into_boxed_slice();
    unsafe {
        *out = JazzNativeRelayBytes {
            len: boxed.len(),
            data: Box::into_raw(boxed).cast(),
        };
    }
    JazzNativeRelayStatus::Ok
}

/// Release any owned response buffer returned by this native C ABI.
///
/// The struct is reset before returning, making repeated frees of the *same
/// struct* a no-op. Copying the struct and freeing both copies is invalid.
///
/// # Safety
///
/// `bytes` must be null or point to a writable `JazzNativeRelayBytes` returned
/// by this ABI (or its reset empty value); callers must not free copied values.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn jazz_native_relay_bytes_free(bytes: *mut JazzNativeRelayBytes) {
    if bytes.is_null() {
        return;
    }
    // SAFETY: a non-null caller-owned output struct was supplied.
    let bytes = unsafe { &mut *bytes };
    if bytes.data.is_null() {
        bytes.len = 0;
        return;
    }
    // SAFETY: the native C ABI creates this allocation with
    // exactly the recorded length and capacity. Reset before dropping so a
    // second call on this struct cannot free it again.
    let allocation = unsafe { Vec::from_raw_parts(bytes.data, bytes.len, bytes.len) };
    *bytes = JazzNativeRelayBytes::EMPTY;
    drop(allocation);
}

/// Inclusive ABI-version range understood by a native host wrapper.
///
/// This is deliberately independent of any particular binding generator.
/// Future TurboModule, Swift, and Kotlin wrappers carry this range in their
/// open request, so an OTA JavaScript update cannot accidentally issue commands
/// to an incompatible embedded native library.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NativeRelayAbiRange {
    pub minimum: u16,
    pub maximum: u16,
}

impl NativeRelayAbiRange {
    pub const fn includes(self, version: u16) -> bool {
        self.minimum <= version && version <= self.maximum
    }

    pub fn validate(self) -> Result<(), RelayError> {
        if self.minimum > self.maximum {
            return Err(RelayError::InvalidAbiRange {
                minimum: self.minimum,
                maximum: self.maximum,
            });
        }
        Ok(())
    }
}

/// Check the native relay ABI before opening storage or allocating an owner
/// thread. Bindings should surface the resulting error unchanged: users need a
/// new native development or release build, rather than a cache reset.
pub fn ensure_native_relay_abi_compatible(
    wrapper_range: NativeRelayAbiRange,
) -> Result<u16, RelayError> {
    wrapper_range.validate()?;
    if wrapper_range.includes(NATIVE_RELAY_ABI_V1) {
        Ok(NATIVE_RELAY_ABI_V1)
    } else {
        Err(RelayError::IncompatibleAbi {
            native: NATIVE_RELAY_ABI_V1,
            minimum: wrapper_range.minimum,
            maximum: wrapper_range.maximum,
        })
    }
}

/// Explicit process-local persistence/synchronization scope.
///
/// Authentication material is intentionally absent. `auth_scope` is an opaque,
/// required stable subject/tenant discriminator supplied by the host after
/// validation; tokens are sent to an upstream connection, never used as storage
/// names. The `Option` preserves the strict JSON boundary's ability to reject
/// an omitted or `null` field rather than silently manufacturing an anonymous
/// persistent scope.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct RelayScope {
    pub app_namespace: String,
    pub storage_namespace: String,
    pub auth_scope: Option<String>,
}

impl RelayScope {
    pub fn validate(&self) -> Result<(), RelayError> {
        for (field, value) in [
            ("app namespace", self.app_namespace.as_str()),
            ("storage namespace", self.storage_namespace.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(RelayError::InvalidScope(format!(
                    "{field} must not be empty"
                )));
            }
        }
        match self.auth_scope.as_deref() {
            Some(auth_scope) if !auth_scope.trim().is_empty() => {}
            Some(_) => {
                return Err(RelayError::InvalidScope(
                    "auth scope must not be empty".to_owned(),
                ));
            }
            None => {
                return Err(RelayError::InvalidScope(
                    "auth scope is required for a persistent relay".to_owned(),
                ));
            }
        }
        Ok(())
    }
}

/// Immutable configuration used to start a relay owner thread.
#[derive(Clone, Debug)]
pub struct RelayOpenConfig {
    /// Inclusive ABI range implemented by the calling platform wrapper. This
    /// is mandatory: opening a scope must reject an OTA/native mismatch before
    /// allocating an owner thread or touching SQLite.
    pub supported_abi: NativeRelayAbiRange,
    pub scope: RelayScope,
    /// Exact owned path chosen by the platform wrapper. The native relay never
    /// interpolates auth tokens or untrusted strings into a filesystem path.
    pub sqlite_path: PathBuf,
    pub schema: JazzSchema,
    pub identity: DbIdentity,
    #[cfg(test)]
    thread_start_counter: Option<Arc<AtomicUsize>>,
}

impl RelayOpenConfig {
    fn validate(&self) -> Result<(), RelayError> {
        ensure_native_relay_abi_compatible(self.supported_abi)?;
        self.scope.validate()
    }
}

/// Stable handle for one process-local UI peer.
#[derive(Clone)]
pub struct NativeRelayClient {
    relay: NativeRelay,
    id: u64,
    wire: NativeRelayWire,
}

impl NativeRelayClient {
    fn pump_foreground(&self) -> Result<(), RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            worker.pump()?;
            worker.foreground_client(id)?;
            Ok(())
        })
    }

    fn set_foreground_wake_callback(
        &self,
        foreground: u64,
        wake: Option<Arc<ForegroundWakeState>>,
    ) -> Result<(), RelayError> {
        let id = self.id;
        self.relay.run_teardown(move |worker| {
            let client = worker
                .clients
                .get(&id)
                .ok_or(RelayError::UnknownClient(id))?;
            // Registration replacement is an owner operation. Discard any
            // signal captured by the prior raw callback before publishing the
            // replacement, so a stale inert Arc cannot suppress its wake.
            worker
                .pending_foreground_wakes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&foreground);
            let generation = worker
                .foreground_wake_generations
                .entry(id)
                .or_default()
                .clone();
            let expected_generation = generation.fetch_add(1, Ordering::AcqRel) + 1;
            let scheduler = wake.map(|wake| ForegroundWakeScheduler {
                wake,
                foreground,
                pending: worker.pending_foreground_wakes.clone(),
                generation,
                expected_generation,
                owner_wake: OwnerWakeNotifier {
                    commands: worker.owner_commands.clone(),
                    queued: worker.owner_wake_queued.clone(),
                },
            });
            let has_foreground_scheduler = {
                let mut foregrounds = worker
                    .wake
                    .foregrounds
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                foregrounds.remove(&id);
                if let Some(scheduler) = &scheduler {
                    foregrounds.insert(id, scheduler.clone());
                }
                !foregrounds.is_empty()
            };
            // No scheduler is the core's explicit manual-driving mode. An
            // installed scheduler with no recipient suppresses that mode's
            // second serve pass without arranging a replacement owner turn.
            worker
                .persistent
                .set_tick_scheduler(has_foreground_scheduler.then(|| {
                    Rc::new(RelayWakeScheduler(Arc::clone(&worker.wake))) as Rc<dyn TickScheduler>
                }));
            client.db.set_tick_scheduler(
                scheduler.map(|scheduler| Rc::new(scheduler) as Rc<dyn TickScheduler>),
            );
            Ok(())
        })
    }
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Run an ABI-adapter operation against this UI runtime on the relay's
    /// owning thread. The closure is intentionally Rust-only; public bindings
    /// expose a small encoded command surface instead of leaking `Db` handles
    /// across JSI/JNI/Swift boundaries.
    pub fn with_db<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&Db<MemoryStorage>) -> Result<T, RelayError> + Send + 'static,
    ) -> Result<T, RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            let client = worker
                .clients
                .get(&id)
                .ok_or(RelayError::UnknownClient(id))?;
            client.check_admission()?;
            operation(&client.db)
        })
    }

    fn minted_tx_time_high_water(&self) -> Result<TxTime, RelayError> {
        let id = self.id;
        self.relay.run_teardown(move |worker| {
            let client = worker
                .clients
                .get_mut(&id)
                .ok_or(RelayError::UnknownClient(id))?;
            // Clean handoff retires this foreground. Release any suspended
            // owner before reading its monotonic minted HLC.
            client.cancel_pending_work();
            let mut read = Box::pin(client.db.minted_tx_time_high_water());
            match read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(high_water) => Ok(high_water),
                Poll::Pending => Err(RelayError::ForegroundCommand(
                    "foreground HLC readout is busy; retire its node identity".into(),
                )),
            }
        })
    }

    pub fn close(self) -> Result<(), RelayError> {
        let id = self.id;
        self.relay.run_teardown(move |worker| {
            let no_foreground_scheduler = {
                let mut foregrounds = worker
                    .wake
                    .foregrounds
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                foregrounds.remove(&id);
                foregrounds.is_empty()
            };
            if no_foreground_scheduler {
                worker.persistent.set_tick_scheduler(None);
            }
            worker.retire_foreground(id)
        })
    }

    pub fn wire(&self) -> NativeRelayWire {
        self.wire.clone()
    }

    fn request_foreground_permission_advice(
        &self,
        action: ForegroundPermissionAdviceAction,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.request_foreground_permission_advice(id, action))
    }

    fn start_foreground_read(
        &self,
        query: Vec<u8>,
        options_json: String,
        transaction: Option<u64>,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.start_foreground_read(id, query, options_json, transaction))
    }

    fn local_current_foreground_row(
        &self,
        table: String,
        row_id: [u8; 16],
    ) -> Result<Vec<u8>, RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            let client = worker.foreground_client(id)?;
            let mut read = Box::pin(
                client
                    .db
                    .local_current_row(&table, RowUuid::from_bytes(row_id)),
            );
            let row = match read
                .as_mut()
                .poll(&mut Context::from_waker(futures::task::noop_waker_ref()))
            {
                Poll::Ready(row) => row.map_err(RelayError::Db)?,
                Poll::Pending => {
                    return Err(RelayError::ForegroundCommand(
                        "local write row is temporarily busy; retry after the next native turn"
                            .into(),
                    ));
                }
            };
            jazz::binding_codec::encode_rows(&row.into_iter().collect::<Vec<_>>()).map_err(
                |error| RelayError::ForegroundCommand(format!("encode local current row: {error}")),
            )
        })
    }

    fn subscribe_foreground_query_with_options(
        &self,
        query: Vec<u8>,
        opts: ReadOpts,
    ) -> Result<u64, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.subscribe_foreground_query_with_options(id, query, opts))
    }

    fn wait_for_pending_writes(
        &self,
        tier: CoreDurabilityTier,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.wait_for_pending_writes(id, tier))
    }
    fn wait_for_foreground_transaction(
        &self,
        tx_id: [u8; 16],
        tier: CoreDurabilityTier,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.wait_for_foreground_transaction(id, tx_id, tier))
    }

    fn drain_foreground_subscription(
        &self,
        subscription: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.drain_foreground_subscription(id, subscription))
    }

    fn poll_foreground_operation(
        &self,
        operation: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.poll_foreground_operation(id, operation))
    }

    fn cancel_foreground_operation(&self, operation: u64) -> Result<bool, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.cancel_foreground_operation(id, operation))
    }

    fn close_foreground_subscription(&self, subscription: u64) -> Result<bool, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.close_foreground_subscription(id, subscription))
    }

    fn begin_foreground_transaction(
        &self,
        kind: ForegroundTransactionKind,
    ) -> Result<u64, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.begin_foreground_transaction(id, kind))
    }

    fn stage_foreground_mutation(
        &self,
        transaction: u64,
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    ) -> Result<Option<RowUuid>, RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            worker.stage_foreground_mutation(
                id,
                transaction,
                mutation,
                table,
                row_id,
                cells,
                options_json,
            )
        })
    }

    fn insert_foreground_transaction(
        &self,
        transaction: u64,
        table: String,
        cells: Vec<u8>,
        row_id: Option<[u8; 16]>,
    ) -> Result<RowUuid, RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            worker.insert_foreground_transaction(id, transaction, table, cells, row_id)
        })
    }

    fn update_foreground_transaction(
        &self,
        transaction: u64,
        table: String,
        row_id: [u8; 16],
        patch: Vec<u8>,
    ) -> Result<(), RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            worker.update_foreground_transaction(id, transaction, table, row_id, patch)
        })
    }

    fn upsert_foreground_transaction(
        &self,
        transaction: u64,
        table: String,
        row_id: [u8; 16],
        cells: Vec<u8>,
    ) -> Result<(), RelayError> {
        let id = self.id;
        self.relay.run(move |worker| {
            worker.upsert_foreground_transaction(id, transaction, table, row_id, cells)
        })
    }

    fn delete_foreground_transaction(
        &self,
        transaction: u64,
        table: String,
        row_id: [u8; 16],
    ) -> Result<(), RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.delete_foreground_transaction(id, transaction, table, row_id))
    }

    fn commit_foreground_transaction(&self, transaction: u64) -> Result<TransactionId, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.commit_foreground_transaction(id, transaction))
    }

    fn wait_for_core_transaction(
        &self,
        tx_id: [u8; 16],
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.wait_for_core_transaction(id, tx_id))
    }

    fn rollback_foreground_transaction(&self, transaction: u64) -> Result<bool, RelayError> {
        let id = self.id;
        self.relay
            .run(move |worker| worker.rollback_foreground_transaction(id, transaction))
    }
}

// A Db poll may temporarily switch to a stacker segment. Platform callbacks
// can attach to the JVM, whose stack checks require the original pthread stack.
// Schedulers only record owner-local signals; each owner operation flushes them
// after all polls return and before publishing its result or parking again.
// Foreground handles are host-global, not client-local; that host owns this
// relay's registry. Replacement is itself an owner operation, so a pending
// entry never crosses into a later registration's operation.
type PendingForegroundWakes = Arc<Mutex<BTreeMap<u64, PendingForegroundWake>>>;

#[derive(Clone)]
struct OwnerWakeNotifier {
    commands: Weak<mpsc::SyncSender<RelayCommand>>,
    queued: Arc<AtomicBool>,
}

impl OwnerWakeNotifier {
    fn signal(&self) {
        if self.queued.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(commands) = self.commands.upgrade() else {
            self.queued.store(false, Ordering::Release);
            return;
        };
        if commands
            .try_send(RelayCommand::FlushForegroundWakes)
            .is_err()
        {
            self.queued.store(false, Ordering::Release);
        }
    }
}

struct PendingForegroundWake {
    wake: Arc<ForegroundWakeState>,
    generation: Arc<AtomicU64>,
    expected_generation: u64,
    kind: u8,
    delay_ms: u64,
}

#[derive(Clone)]
struct ForegroundWakeScheduler {
    wake: Arc<ForegroundWakeState>,
    foreground: u64,
    pending: PendingForegroundWakes,
    generation: Arc<AtomicU64>,
    expected_generation: u64,
    owner_wake: OwnerWakeNotifier,
}
impl ForegroundWakeScheduler {
    fn wake(&self, kind: u8, delay_ms: u64) {
        // A retained Context waker can outlive callback replacement. Do not
        // let its inert registration occupy this foreground's coalescing slot.
        if self.generation.load(Ordering::Acquire) != self.expected_generation
            || !self.wake.is_active()
        {
            return;
        }
        let mut pending = self
            .pending
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.generation.load(Ordering::Acquire) != self.expected_generation
            || !self.wake.is_active()
        {
            return;
        }
        let entry = pending
            .entry(self.foreground)
            .or_insert_with(|| PendingForegroundWake {
                wake: self.wake.clone(),
                generation: self.generation.clone(),
                expected_generation: self.expected_generation,
                kind,
                delay_ms,
            });
        if entry.generation.load(Ordering::Acquire) != entry.expected_generation
            || !entry.wake.is_active()
        {
            *entry = PendingForegroundWake {
                wake: self.wake.clone(),
                generation: self.generation.clone(),
                expected_generation: self.expected_generation,
                kind,
                delay_ms,
            };
        }
        if kind == FOREGROUND_WAKE_IMMEDIATE
            || (kind == FOREGROUND_WAKE_DEFERRED && entry.kind == FOREGROUND_WAKE_AFTER)
            || (kind == FOREGROUND_WAKE_AFTER
                && entry.kind == FOREGROUND_WAKE_AFTER
                && delay_ms < entry.delay_ms)
        {
            entry.kind = kind;
            entry.delay_ms = delay_ms;
        }
        drop(pending);
        self.owner_wake.signal();
    }
}
impl TickScheduler for ForegroundWakeScheduler {
    fn schedule_tick(&self, urgency: TickUrgency) {
        self.wake(
            match urgency {
                TickUrgency::Immediate => FOREGROUND_WAKE_IMMEDIATE,
                TickUrgency::Deferred => FOREGROUND_WAKE_DEFERRED,
                // The fixed foreground wake ABI already represents a later
                // owner turn as an `after:0` callback. Keep cold hydration
                // out of the current JSI turn without expanding that ABI.
                TickUrgency::AfterCurrentTurn => FOREGROUND_WAKE_AFTER,
            },
            0,
        )
    }
    fn schedule_tick_after(&self, delay_ms: u64) {
        self.wake(FOREGROUND_WAKE_AFTER, delay_ms)
    }

    fn query_runtime_waker(&self) -> Option<Waker> {
        Some(Waker::from(Arc::new(self.clone())))
    }
}

impl Wake for ForegroundWakeScheduler {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        ForegroundWakeScheduler::wake(self, FOREGROUND_WAKE_AFTER, 0);
    }
}

/// A retained relay future can become ready after the foreground which
/// started its tick has closed. Wake all remaining leases of this scope;
/// each platform callback already coalesces turns and has an inert teardown
/// guard. Never keep the registry mutex held while calling the platform.
#[derive(Default)]
struct RelayWake {
    foregrounds: Mutex<BTreeMap<u64, ForegroundWakeScheduler>>,
}

impl RelayWake {
    fn signal(&self, delay_ms: u64) {
        let foregrounds = self
            .foregrounds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for foreground in foregrounds {
            foreground.wake(FOREGROUND_WAKE_AFTER, delay_ms);
        }
    }
}

impl Wake for RelayWake {
    fn wake(self: Arc<Self>) {
        self.signal(0);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.signal(0);
    }
}

struct RelayWakeScheduler(Arc<RelayWake>);

impl TickScheduler for RelayWakeScheduler {
    fn schedule_tick(&self, _urgency: TickUrgency) {
        // All persistent work is driven on a later bounded host turn.
        self.0.signal(0);
    }

    fn schedule_tick_after(&self, delay_ms: u64) {
        self.0.signal(delay_ms);
    }

    fn query_runtime_waker(&self) -> Option<Waker> {
        Some(Waker::from(Arc::clone(&self.0)))
    }
}

/// Thread-safe handle to one executor-local relay owner.
#[derive(Clone)]
pub struct NativeRelay {
    inner: Arc<RelayInner>,
}

struct RelayInner {
    jobs: Mutex<Option<Arc<mpsc::SyncSender<RelayCommand>>>>,
    normal_queue_depth: Arc<AtomicUsize>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
    liveness: Arc<RelayLiveness>,
    wire: NativeRelayWire,
    sqlite_path: PathBuf,
    schema_version: jazz::ids::SchemaVersionId,
    identity: DbIdentity,
}

struct RelayLiveness {
    alive: AtomicBool,
    gate: Mutex<()>,
}
impl RelayLiveness {
    fn new() -> Self {
        Self {
            alive: AtomicBool::new(true),
            gate: Mutex::new(()),
        }
    }
    fn is_alive(&self) -> bool {
        self.alive.load(Ordering::Acquire)
    }
    fn enter(&self) -> Result<std::sync::MutexGuard<'_, ()>, RelayError> {
        let guard = self
            .gate
            .lock()
            .map_err(|_| RelayError::Poisoned("relay terminal gate"))?;
        if self.is_alive() {
            Ok(guard)
        } else {
            Err(RelayError::Closed)
        }
    }
    fn mark_terminal(&self) {
        self.alive.store(false, Ordering::Release);
    }
}
struct OwnerLiveness(Arc<RelayLiveness>);
impl Drop for OwnerLiveness {
    fn drop(&mut self) {
        self.0.mark_terminal();
    }
}

impl RelayInner {
    fn shutdown(&self) -> Result<(), RelayError> {
        self.liveness.mark_terminal();
        let sender = self
            .jobs
            .lock()
            .map_err(|_| RelayError::Poisoned("relay command queue"))?
            .take();
        if let Some(sender) = sender {
            let (done_tx, done_rx) = mpsc::channel();
            if sender.send(RelayCommand::Shutdown(done_tx)).is_ok() {
                let _ = done_rx.recv();
            }
        }
        if let Some(join) = self
            .join
            .lock()
            .map_err(|_| RelayError::Poisoned("relay owner join"))?
            .take()
        {
            let _ = join.join();
        }
        Ok(())
    }
}

impl Drop for RelayInner {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

/// Thread-safe upstream protocol queues owned by the host integration.
///
/// A native network/ABI wrapper writes authenticated upstream messages to
/// `inbound` and drains `outbound`. The relay only sees normal `SyncMessage`
/// traffic through a regular `Db::connect_upstream` transport.
#[derive(Clone)]
pub struct NativeRelayWire {
    inbound: Arc<Mutex<BoundedMessageQueue>>,
    outbound: Arc<Mutex<BoundedMessageQueue>>,
    liveness: Option<Arc<RelayLiveness>>,
    connection_liveness: Option<Arc<RelayLiveness>>,
}

impl Default for NativeRelayWire {
    fn default() -> Self {
        Self {
            inbound: Arc::new(Mutex::new(BoundedMessageQueue::default())),
            outbound: Arc::new(Mutex::new(BoundedMessageQueue::default())),
            liveness: None,
            connection_liveness: None,
        }
    }
}

struct QueuedMessage {
    message: SyncMessage,
    encoded_len: usize,
}

#[derive(Default)]
struct BoundedMessageQueue {
    messages: VecDeque<QueuedMessage>,
    encoded_bytes: usize,
}

impl BoundedMessageQueue {
    fn pop_auxiliary(&mut self) -> Option<SyncMessage> {
        let position = self.messages.iter().position(|queued| {
            matches!(
                queued.message,
                SyncMessage::ChunkRequestBatch(_) | SyncMessage::ChunkResponseBatch(_)
            )
        })?;
        let queued = self
            .messages
            .remove(position)
            .expect("position was present");
        self.encoded_bytes -= queued.encoded_len;
        Some(queued.message)
    }

    fn push(&mut self, message: SyncMessage, direction: &'static str) -> Result<(), RelayError> {
        if self.messages.len() >= NATIVE_RELAY_QUEUE_MAX_MESSAGES {
            return Err(RelayError::QueueCapacityExceeded {
                direction,
                queued_messages: self.messages.len(),
                queued_bytes: self.encoded_bytes,
            });
        }
        let encoded_len = encode_sync_message(&message)
            .map_err(RelayError::EncodePeerMessage)?
            .len();
        validate_encoded_peer_message_len(encoded_len)?;
        let next_bytes = self.encoded_bytes.saturating_add(encoded_len);
        if next_bytes > NATIVE_RELAY_QUEUE_MAX_BYTES {
            return Err(RelayError::QueueCapacityExceeded {
                direction,
                queued_messages: self.messages.len(),
                queued_bytes: self.encoded_bytes,
            });
        }
        self.messages.push_back(QueuedMessage {
            message,
            encoded_len,
        });
        self.encoded_bytes = next_bytes;
        Ok(())
    }

    fn pop(&mut self) -> Option<SyncMessage> {
        let queued = self.messages.pop_front()?;
        self.encoded_bytes -= queued.encoded_len;
        Some(queued.message)
    }

    fn drain_messages(&mut self) -> Vec<SyncMessage> {
        let mut drained = Vec::new();
        let mut drained_bytes = 0_usize;
        while drained.len() < NATIVE_RELAY_DRAIN_MAX_MESSAGES {
            let Some(front) = self.messages.front() else {
                break;
            };
            if !drained.is_empty()
                && drained_bytes.saturating_add(front.encoded_len) > NATIVE_RELAY_DRAIN_TARGET_BYTES
            {
                break;
            }
            let queued = self.messages.pop_front().expect("front was present");
            drained_bytes += queued.encoded_len;
            self.encoded_bytes -= queued.encoded_len;
            drained.push(queued.message);
        }
        drained
    }

    fn len(&self) -> usize {
        self.messages.len()
    }
}

type WireLivenessGuards<'a> = (
    Option<std::sync::MutexGuard<'a, ()>>,
    Option<std::sync::MutexGuard<'a, ()>>,
);

impl NativeRelayWire {
    fn for_owner(liveness: Arc<RelayLiveness>) -> Self {
        Self {
            inbound: Arc::new(Mutex::new(BoundedMessageQueue::default())),
            outbound: Arc::new(Mutex::new(BoundedMessageQueue::default())),
            liveness: Some(liveness),
            connection_liveness: Some(Arc::new(RelayLiveness::new())),
        }
    }
    fn enter(&self) -> Result<WireLivenessGuards<'_>, RelayError> {
        let owner = self.liveness.as_ref().map(|l| l.enter()).transpose()?;
        let connection = self
            .connection_liveness
            .as_ref()
            .map(|l| l.enter())
            .transpose()?;
        Ok((owner, connection))
    }

    /// A retired connection is a transient transport condition: its owner may
    /// still be detaching the old peer or installing its replacement. Keep the
    /// owner and connection liveness domains separate so core retains outbound
    /// work until the replacement has been installed.
    fn enter_queue_transport(&self) -> Result<WireLivenessGuards<'_>, TransportError> {
        let owner = self
            .liveness
            .as_ref()
            .map(|l| l.enter())
            .transpose()
            .map_err(transport_queue_error)?;
        let connection = match self
            .connection_liveness
            .as_ref()
            .map(|l| l.enter())
            .transpose()
        {
            Ok(connection) => connection,
            Err(RelayError::Closed) => return Err(TransportError::Backpressure),
            Err(error) => return Err(transport_queue_error(error)),
        };
        Ok((owner, connection))
    }
    fn retire_connection(&self) {
        if let Some(liveness) = &self.connection_liveness {
            let _guard = liveness
                .gate
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            liveness.mark_terminal();
        }
    }
    pub fn push_inbound(&self, message: SyncMessage) -> Result<(), RelayError> {
        let _terminal = self.enter()?;
        self.inbound
            .lock()
            .map_err(|_| RelayError::Poisoned("upstream inbound queue"))?
            .push(message, "inbound")
    }

    pub fn take_outbound(&self) -> Result<Vec<SyncMessage>, RelayError> {
        let _terminal = self.enter()?;
        Ok(self
            .outbound
            .lock()
            .map_err(|_| RelayError::Poisoned("upstream outbound queue"))?
            .drain_messages())
    }

    /// Admit one postcard-encoded ordinary peer message from a binding.
    ///
    /// This is the binary boundary for native hosts: it preserves Jazz's
    /// shared `SyncMessage` vocabulary instead of inventing a React Native
    /// object API. The caller supplies one complete logical message; network
    /// framing and fragmentation remain the responsibility of its transport.
    pub fn push_inbound_encoded(&self, bytes: &[u8]) -> Result<(), RelayError> {
        validate_encoded_peer_message_len(bytes.len())?;
        let message = decode_sync_message(bytes).map_err(RelayError::DecodePeerMessage)?;
        self.push_inbound(message)
    }

    /// Drain ordinary peer messages as postcard payloads for a binding.
    ///
    /// The encoded payload uses the canonical Jazz sync-message codec. A
    /// future TurboModule transports these bytes as `ArrayBuffer`/`Uint8Array`,
    /// keeping it thin and shared.
    pub fn take_outbound_encoded(&self) -> Result<Vec<Vec<u8>>, RelayError> {
        let _terminal = self.enter()?;
        let mut outbound = self
            .outbound
            .lock()
            .map_err(|_| RelayError::Poisoned("upstream outbound queue"))?;
        // Encode while the batch remains queued. A failed codec/size check
        // leaves every message intact for retry and diagnostics.
        let encoded = outbound
            .messages
            .iter()
            .take(NATIVE_RELAY_DRAIN_MAX_MESSAGES)
            .scan(0_usize, |bytes, queued| {
                if *bytes != 0
                    && bytes.saturating_add(queued.encoded_len) > NATIVE_RELAY_DRAIN_TARGET_BYTES
                {
                    return None;
                }
                *bytes += queued.encoded_len;
                Some(&queued.message)
            })
            .map(|message| {
                let bytes = encode_sync_message(message).map_err(RelayError::EncodePeerMessage)?;
                validate_logical_message_len(bytes.len())
                    .map_err(RelayError::PeerMessageTooLarge)?;
                Ok(bytes)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for _ in 0..encoded.len() {
            let _ = outbound.pop();
        }
        Ok(encoded)
    }

    pub fn queue_depths(&self) -> Result<(usize, usize), RelayError> {
        let inbound = self
            .inbound
            .lock()
            .map_err(|_| RelayError::Poisoned("native relay inbound queue"))?
            .len();
        let outbound = self
            .outbound
            .lock()
            .map_err(|_| RelayError::Poisoned("native relay outbound queue"))?
            .len();
        Ok((inbound, outbound))
    }
}

/// Move ordinary semantic messages between the relay's owner-local queue and
/// a negotiated native socket. The socket is always wrapped in
/// `WireTransportAdapter`: this bridge never serializes `SyncMessage` bytes,
/// frames, fragments, or reconnect protocol itself.
///
/// A native worker calls this in each bounded network turn, then requests the
/// relay's ordinary pump. Keeping the bridge generic makes the production
/// `WebSocketTransport` path and deterministic edge fixtures exercise exactly
/// the same framing boundary.
enum NativeRelayWireBridgeError {
    Relay(RelayError),
    /// `WebSocketTransport` closed its outbound receiver after choosing an
    /// established transport terminal. This is deliberately distinct from a
    /// relay/wire liveness closure, which means the owner is gone.
    SocketPumpClosed,
}

fn bridge_native_relay_wire_once_classified<T: WireTransport>(
    relay_wire: &NativeRelayWire,
    upstream: &mut jazz::db::WireTransportAdapter<T>,
) -> Result<bool, NativeRelayWireBridgeError> {
    let mut progressed = false;
    for message in relay_wire
        .take_outbound()
        .map_err(NativeRelayWireBridgeError::Relay)?
    {
        let _live_connection = relay_wire
            .enter()
            .map_err(NativeRelayWireBridgeError::Relay)?;
        upstream.send(message).map_err(|error| match error {
            // `WebSocketTransport` exposes an already-retired peer through
            // this concrete transport state. Its terminal future decides
            // whether the socket worker reconnects; every other send error
            // remains a terminal foreground error.
            TransportError::Failed(message) if message == "websocket pump is closed" => {
                NativeRelayWireBridgeError::SocketPumpClosed
            }
            error => NativeRelayWireBridgeError::Relay(RelayError::ForegroundCommand(format!(
                "native upstream send: {error:?}"
            ))),
        })?;
        progressed = true;
    }
    loop {
        match upstream.try_recv() {
            Some(message) => {
                relay_wire
                    .push_inbound(message)
                    .map_err(NativeRelayWireBridgeError::Relay)?;
                progressed = true;
            }
            None => return Ok(progressed),
        }
    }
}

pub fn bridge_native_relay_wire_once<T: WireTransport>(
    relay_wire: &NativeRelayWire,
    upstream: &mut jazz::db::WireTransportAdapter<T>,
) -> Result<bool, RelayError> {
    bridge_native_relay_wire_once_classified(relay_wire, upstream).map_err(|error| match error {
        NativeRelayWireBridgeError::Relay(error) => error,
        NativeRelayWireBridgeError::SocketPumpClosed => RelayError::Closed,
    })
}

/// Native-owned socket lifecycle for one untrusted foreground relay scope.
///
/// Android and iOS call this shared worker from their private session setup;
/// neither platform gets a raw protocol codec or reconnect loop.  The worker
/// supplies the bearer only to the normal Edge WebSocket prelude and always
/// uses the authenticated, non-SYSTEM connection mode.
pub struct NativeRelaySocketWorker {
    activation: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    cancelled: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
    join: Mutex<Option<thread::JoinHandle<()>>>,
}

/// Private native socket inputs. This is intentionally absent from postcard
/// commands and diagnostics: JavaScript can neither configure the endpoint
/// nor read its bearer.
pub struct NativeRelaySocketConfig {
    pub server_url: String,
    pub app_id: AppId,
    pub peer_identity: jazz::ids::AuthorSubject,
    pub auth: AuthConfig,
    pub reconnect_delay: std::time::Duration,
    pub on_event: Arc<dyn Fn(NativeRelaySocketEvent) + Send + Sync>,
}

impl std::fmt::Debug for NativeRelaySocketConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeRelaySocketConfig")
            .field("server_url", &self.server_url)
            .field("app_id", &self.app_id)
            .field("peer_identity", &self.peer_identity)
            .field("auth", &"<redacted>")
            .field("reconnect_delay", &self.reconnect_delay)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NativeRelaySocketEvent {
    Connected,
    Reconnecting,
    /// A bridge or established transport failed. The worker will retry, but
    /// its scope-owned relay surfaces this failure to foreground ticks until a
    /// subsequent authenticated connection reaches `Connected`.
    TerminalError(String),
    Stopped,
}

impl NativeRelaySocketWorker {
    /// Start the production native WebSocket worker.
    pub fn start(relay: NativeRelay, config: NativeRelaySocketConfig) -> Result<Self, RelayError> {
        Self::start_with_connector(relay, config, Arc::new(NativeWebSocketConnector))
    }

    /// Composition seam for deterministic native-host tests. Production uses
    /// [`NativeWebSocketConnector`] above; the connector still owns TLS,
    /// WebSocket framing, and Edge's authenticated handshake.
    pub fn start_with_connector(
        relay: NativeRelay,
        config: NativeRelaySocketConfig,
        connector: Arc<dyn NativeTransportConnector>,
    ) -> Result<Self, RelayError> {
        let worker = Self::prepare_with_connector(relay, config, connector)?;
        worker.activate();
        Ok(worker)
    }

    fn activate(&self) {
        if let Some(start) = self
            .activation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take()
        {
            let _ = start.send(());
        }
    }

    fn prepare_with_connector(
        relay: NativeRelay,
        config: NativeRelaySocketConfig,
        connector: Arc<dyn NativeTransportConnector>,
    ) -> Result<Self, RelayError> {
        if config.peer_identity == jazz::ids::AuthorSubject::SYSTEM
            || config.auth.jwt_token.as_deref().is_none_or(str::is_empty)
            || config.auth.backend_secret.is_some()
            || config.auth.admin_secret.is_some()
            || config.auth.backend_session.is_some()
        {
            return Err(RelayError::ForegroundCommand(
                "native relay sockets require an ordinary non-SYSTEM bearer session".to_owned(),
            ));
        }
        let cancelled = Arc::new(AtomicBool::new(false));
        let wake = Arc::new(tokio::sync::Notify::new());
        let thread_cancelled = Arc::clone(&cancelled);
        let thread_wake = Arc::clone(&wake);
        // Reserve all fallible resources before a caller replaces live state.
        // The new worker cannot attach upstream until explicitly activated.
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_io()
            .enable_time()
            .build()
            .map_err(|error| RelayError::OwnerThread(error.to_string()))?;
        let (activate, activation) = std::sync::mpsc::channel();
        let join = thread::Builder::new()
            .name("jazz-native-relay-socket".to_owned())
            .spawn(move || {
                if activation.recv().is_err() {
                    return;
                }
                runtime.block_on(run_native_relay_socket_worker(
                    relay,
                    config,
                    connector,
                    thread_cancelled,
                    thread_wake,
                ));
            })
            .map_err(|error| RelayError::OwnerThread(error.to_string()))?;
        Ok(Self {
            activation: Mutex::new(Some(activate)),
            cancelled,
            wake,
            join: Mutex::new(Some(join)),
        })
    }

    pub fn cancel(&self) {
        self.activation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
        self.cancelled.store(true, Ordering::Release);
        self.wake.notify_waiters();
    }
}

impl Drop for NativeRelaySocketWorker {
    fn drop(&mut self) {
        self.cancel();
        if let Ok(mut join) = self.join.lock()
            && let Some(join) = join.take()
        {
            let _ = join.join();
        }
    }
}

struct SocketUpstreamLease {
    relay: NativeRelay,
    generation: u64,
    wire: NativeRelayWire,
}

impl Drop for SocketUpstreamLease {
    fn drop(&mut self) {
        // Stop stale bridge traffic immediately, even if owner cleanup must
        // wait behind an already-running local operation.
        self.wire.retire_connection();
        loop {
            let generation = self.generation;
            match self
                .relay
                .run_teardown(move |worker| worker.retire_socket_upstream(generation))
            {
                Err(RelayError::OwnerQueueFull) => {
                    thread::sleep(std::time::Duration::from_millis(1))
                }
                _ => break,
            }
        }
    }
}

async fn run_native_relay_socket_worker(
    relay: NativeRelay,
    config: NativeRelaySocketConfig,
    connector: Arc<dyn NativeTransportConnector>,
    cancelled: Arc<AtomicBool>,
    wake: Arc<tokio::sync::Notify>,
) {
    'reconnect: while !cancelled.load(Ordering::Acquire) {
        let request = NativeTransportRequest {
            server_url: config.server_url.clone(),
            app_id: config.app_id,
            peer_identity: config.peer_identity,
            auth: config.auth.clone(),
            wake: Arc::new(|| {}),
        };
        let connected = tokio::select! {
            result = connector.connect(request) => result,
            _ = wake.notified() => break,
        };
        let connected = match connected {
            Ok(connected) => connected,
            Err(error) => {
                if !error.is_retryable() {
                    (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                        "native relay socket connect failed: {error}"
                    )));
                }
                (config.on_event)(NativeRelaySocketEvent::Reconnecting);
                tokio::select! {
                    _ = tokio::time::sleep(config.reconnect_delay) => {},
                    _ = wake.notified() => break,
                }
                continue;
            }
        };
        if connected.permits_delegated_sessions {
            (config.on_event)(NativeRelaySocketEvent::TerminalError(
                "native relay socket connector granted forbidden delegated-session authority"
                    .to_owned(),
            ));
            (config.on_event)(NativeRelaySocketEvent::Reconnecting);
            tokio::select! {
                _ = tokio::time::sleep(config.reconnect_delay) => {},
                _ = wake.notified() => break,
            }
            continue;
        }
        if connected
            .session_context
            .is_some_and(|context| context.link_identity != config.peer_identity)
        {
            (config.on_event)(NativeRelaySocketEvent::TerminalError(
                "native relay connector session identity differs from admission".into(),
            ));
            break;
        }
        let session_context = connected.session_context;
        let (generation, wire) = loop {
            if cancelled.load(Ordering::Acquire) {
                break 'reconnect;
            }
            match relay.run(move |worker| worker.begin_socket_upstream(session_context)) {
                Ok(installed) => break installed,
                Err(RelayError::OwnerQueueFull) => {
                    tokio::select! {
                        _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
                        _ = wake.notified() => {},
                    }
                    continue;
                }
                Err(error) => {
                    (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                        "native relay upstream install failed: {error}"
                    )));
                    break 'reconnect;
                }
            }
        };
        let lease = SocketUpstreamLease {
            relay: relay.clone(),
            generation,
            wire,
        };
        loop {
            if cancelled.load(Ordering::Acquire) {
                break 'reconnect;
            }
            match relay.run(move |worker| {
                worker.poll_upstream_transition()?;
                if worker.socket_generation != generation || worker.socket_wire.is_none() {
                    return Err(RelayError::Closed);
                }
                Ok(worker.upstream_attached && worker.upstream_transition.is_none())
            }) {
                Ok(true) => break,
                Ok(false) | Err(RelayError::OwnerQueueFull) => {}
                Err(error) => {
                    // The socket may retry a peer transport loss, but it must
                    // never replace a dead or failed relay owner. In
                    // particular, preserving a poisoned storage/Db error as a
                    // reconnect would strand strict foreground operations
                    // without a lifecycle failure.
                    if !cancelled.load(Ordering::Acquire) {
                        (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                            "native relay upstream transition failed: {error}"
                        )));
                    }
                    break 'reconnect;
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(1)) => {},
                _ = wake.notified() => {},
            }
        }
        (config.on_event)(NativeRelaySocketEvent::Connected);
        let mut upstream = jazz::db::WireTransportAdapter::new_with_session_context(
            connected.transport,
            connected.protocol_version,
            connected.features,
            None,
            connected.session_context,
        );
        let mut terminal = connected.terminal;
        loop {
            if cancelled.load(Ordering::Acquire) {
                drop(lease);
                (config.on_event)(NativeRelaySocketEvent::Stopped);
                return;
            }
            match bridge_native_relay_wire_once_classified(&lease.wire, &mut upstream) {
                Ok(_) => {}
                Err(NativeRelayWireBridgeError::SocketPumpClosed) => {
                    // The outbound pump is closed only after its owner has
                    // published the established transport terminal. That
                    // terminal, rather than the private queue-close marker,
                    // decides whether this outage is retryable.
                    let terminal = loop {
                        if cancelled.load(Ordering::Acquire) {
                            break 'reconnect;
                        }
                        tokio::select! {
                            terminal = &mut terminal => break terminal,
                            _ = wake.notified() => {},
                            _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
                        }
                    };
                    if let NativeTransportTerminal::Failed(NativeTransportError::Terminal(error)) =
                        terminal
                    {
                        (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                            "native relay socket terminated: {error}"
                        )));
                    }
                    break;
                }
                Err(NativeRelayWireBridgeError::Relay(error)) => {
                    if !cancelled.load(Ordering::Acquire) {
                        (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                            "native relay wire bridge failed: {error}"
                        )));
                    }
                    break 'reconnect;
                }
            }
            if let Err(error) = relay.pump() {
                // Relay closure is an owner/scope lifetime boundary. It is
                // distinct from the connection-local closure above: report it
                // to foreground work, then stop rather than resurrecting a
                // relay whose owner has gone away. Cancellation may race this
                // path during intentional host teardown, for which Stopped is
                // the only lifecycle event.
                if !cancelled.load(Ordering::Acquire) {
                    (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                        "native relay owner pump failed: {error}"
                    )));
                }
                // A closed relay means its owner/scope is gone, not merely
                // that this socket needs replacement. Never resurrect it.
                break 'reconnect;
            }
            tokio::select! {
                terminal = &mut terminal => {
                    // An established peer loss is retryable transport state:
                    // the ClientDb retains local work and its existing recovery
                    // driver parks strict remote operations until replacement.
                    // Do not poison the foreground host, which would turn a
                    // network outage into a local SQLite failure.
                    if let NativeTransportTerminal::Failed(NativeTransportError::Terminal(error)) = terminal {
                        (config.on_event)(NativeRelaySocketEvent::TerminalError(format!(
                            "native relay socket terminated: {error}"
                        )));
                    }
                    break;
                },
                _ = tokio::time::sleep(std::time::Duration::from_millis(10)) => {},
                _ = wake.notified() => {},
            }
        }
        drop(lease);
        if !cancelled.load(Ordering::Acquire) {
            (config.on_event)(NativeRelaySocketEvent::Reconnecting);
        }
    }
    (config.on_event)(NativeRelaySocketEvent::Stopped);
}

fn validate_encoded_peer_message_len(len: usize) -> Result<(), RelayError> {
    validate_logical_message_len(len).map_err(RelayError::PeerMessageTooLarge)
}

struct QueueTransport {
    wire: NativeRelayWire,
    session_context: Option<jazz::db::ConnectionSessionContext>,
}

impl Transport for QueueTransport {
    fn connection_session_context(&self) -> Option<jazz::db::ConnectionSessionContext> {
        self.session_context
    }
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        let _terminal = self.wire.enter_queue_transport()?;
        self.wire
            .outbound
            .lock()
            .map_err(|_| TransportError::Failed("native relay outbound queue poisoned".to_owned()))?
            .push(message, "outbound")
            .map_err(transport_queue_error)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let _terminal = self.wire.enter().ok()?;
        self.wire.inbound.lock().ok()?.pop()
    }
}

struct DuplexTransport {
    wire: NativeRelayWire,
    session_context: jazz::db::ConnectionSessionContext,
}

impl Transport for DuplexTransport {
    fn connection_session_context(&self) -> Option<jazz::db::ConnectionSessionContext> {
        Some(self.session_context)
    }

    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        let _terminal = self.wire.enter().map_err(transport_queue_error)?;
        self.wire
            .outbound
            .lock()
            .map_err(|_| TransportError::Failed("native relay client queue poisoned".to_owned()))?
            .push(message, "client outbound")
            .map_err(transport_queue_error)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let _terminal = self.wire.enter().ok()?;
        self.wire.inbound.lock().ok()?.pop()
    }
}

fn duplex(
    liveness: Arc<RelayLiveness>,
    client: DbIdentity,
    relay: DbIdentity,
) -> (Box<dyn Transport>, Box<dyn Transport>, NativeRelayWire) {
    // The host has admitted this foreground to one account-scoped relay.
    // Retain per-hop receipt epochs without treating the foreground as an
    // authority or allowing it to select a different session.
    let relay_endpoint = jazz::wire::WireAuthorityEndpoint::fresh(relay.node);
    let client_context = jazz::db::ConnectionSessionContext {
        local: jazz::wire::WireAuthorityEndpoint::fresh(client.node),
        remote: Some(relay_endpoint),
        link_identity: client.author,
        negotiated_features: jazz::wire::current_wire_features(),
    };
    let relay_context = jazz::db::ConnectionSessionContext {
        local: relay_endpoint,
        remote: None,
        link_identity: client.author,
        negotiated_features: jazz::wire::current_wire_features(),
    };
    let wire = NativeRelayWire::for_owner(liveness);
    let reverse = NativeRelayWire {
        inbound: Arc::clone(&wire.outbound),
        outbound: Arc::clone(&wire.inbound),
        liveness: wire.liveness.clone(),
        connection_liveness: wire.connection_liveness.clone(),
    };
    (
        Box::new(DuplexTransport {
            wire: wire.clone(),
            session_context: client_context,
        }),
        Box::new(DuplexTransport {
            wire: reverse,
            session_context: relay_context,
        }),
        wire,
    )
}

type ForegroundSubscriptionOpen =
    Pin<Box<dyn Future<Output = Result<SubscriptionStream, RelayError>>>>;

struct ConnectedClient {
    author: jazz::ids::AuthorSubject,
    refreshed_claims: Option<BTreeMap<String, Value>>,
    retiring: bool,
    admitted_scope_advice: bool,
    db: Rc<Db<MemoryStorage>>,
    tick: Option<RelayTickFuture>,
    upstream_io: RelayPeerIo,
    served_io: Option<RelayPeerIo>,
    admission: Option<RelayAdmissionFuture>,
    admission_error: Option<jazz::db::Error>,
    wire: NativeRelayWire,
    pending_subscriptions: BTreeMap<u64, ForegroundSubscriptionOpen>,
    subscriptions: BTreeMap<u64, SubscriptionStream>,
    pending_operations: BTreeMap<u64, ForegroundPendingOperation>,
    mutation_cleanups: Vec<ForegroundOperationFuture>,
    read_cleanups: Rc<RefCell<VecDeque<jazz::db::QueryAttachment>>>,
    read_cleanup: Option<Pin<Box<dyn Future<Output = ()>>>>,
    transactions: BTreeMap<u64, ForegroundTransaction>,
    /// Public transaction ids are opaque digests, while only the foreground
    /// owner may retain the core causal id needed for a settlement wait.
    committed_transactions: BTreeMap<TransactionId, TxId>,
    mutations: foreground_mutations::MutationHandles,
    next_foreground_handle: u64,
    // The core stores weak references for lifecycle ownership; retaining both
    // endpoints is what keeps the normal peer protocol connection alive.
    _upstream: Rc<LocalMutex<PeerConnection<MemoryStorage>>>,
    _served: Option<Rc<LocalMutex<PeerConnection<SqliteStorage>>>>,
}

#[derive(Clone, Copy)]
struct ForegroundTransaction {
    open_tx_id: OpenTransactionId,
    kind: ForegroundTransactionKind,
}

impl ConnectedClient {
    fn poll_mutation_cleanup(&mut self, waker: &Waker) {
        let mut context = Context::from_waker(waker);
        self.mutation_cleanups
            .retain_mut(|future| future.as_mut().poll(&mut context).is_pending());
    }

    fn check_admission(&self) -> Result<(), RelayError> {
        self.admission_error
            .as_ref()
            .map_or(Ok(()), |error| Err(RelayError::Db(error.clone())))
    }

    fn poll_claim_refresh(&mut self) {
        if let (Some(claims), Some(served)) = (&self.refreshed_claims, &self._served)
            && let Some(mut peer) = served.try_lock()
        {
            peer.update_authenticated_session_claims(claims.clone());
            self.refreshed_claims = None;
        }
    }

    fn poll_admission(&mut self, waker: &Waker) {
        self.poll_claim_refresh();
        let Some(mut admission) = self.admission.take() else {
            return;
        };
        let mut context = Context::from_waker(waker);
        match admission.as_mut().poll(&mut context) {
            Poll::Pending => self.admission = Some(admission),
            Poll::Ready(Ok(served)) => {
                self.served_io = Some(RelayPeerIo::new(
                    served
                        .try_lock()
                        .expect("newly admitted peer is not borrowed")
                        .io_pump(),
                    NativeRelayWire {
                        inbound: Arc::clone(&self.wire.outbound),
                        outbound: Arc::clone(&self.wire.inbound),
                        liveness: self.wire.liveness.clone(),
                        connection_liveness: self.wire.connection_liveness.clone(),
                    },
                ));
                self._served = Some(served);
                self.poll_claim_refresh();
            }
            Poll::Ready(Err(error)) => {
                self.cancel_pending_work();
                self.admission_error = Some(error);
                self.db.schedule_tick(TickUrgency::Immediate);
            }
        }
    }

    fn poll_served_io(&mut self, waker: &Waker) -> Result<(), RelayError> {
        if let Some(io) = &mut self.served_io {
            io.poll(waker)?;
        }
        Ok(())
    }

    fn poll_read_cleanup(&mut self, waker: &Waker) {
        if self.read_cleanup.is_none()
            && let Some(attachment) = self.read_cleanups.borrow_mut().pop_front()
        {
            let db = Rc::clone(&self.db);
            self.read_cleanup = Some(Box::pin(async move {
                db.detach_query_async(attachment).await;
            }));
        }
        if let Some(cleanup) = self.read_cleanup.as_mut() {
            let mut context = Context::from_waker(waker);
            if cleanup.as_mut().poll(&mut context).is_ready() {
                self.read_cleanup = None;
            }
        }
    }

    fn cancel_pending_work(&mut self) {
        self.admission = None;
        if self._served.is_none() {
            // No peer owns these frames yet. Closing or rejecting the
            // foreground cancels its queued traffic as well as admission.
            for queue in [&self.wire.inbound, &self.wire.outbound] {
                *queue
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                    BoundedMessageQueue::default();
            }
        }
        self.tick = None;
        self.pending_operations.clear();
        self.pending_subscriptions.clear();
        self.mutation_cleanups.clear();
        self.read_cleanup = None;
        self.read_cleanups.borrow_mut().clear();
        self.upstream_io.incoming = None;
        if let Some(io) = &mut self.served_io {
            io.incoming = None;
        }
    }

    /// Abandon every foreground-owned transaction before dropping the client.
    /// An attached foreground can be closed explicitly, revoked, or retired
    /// during host shutdown; none of those paths may leave a mutable core
    /// transaction reusable by a later foreground handle.
    fn abandon_foreground_transactions(&mut self) -> Result<(), RelayError> {
        if self.retiring {
            return Ok(());
        }
        self.retiring = true;
        self.cancel_pending_work();
        let transactions = std::mem::take(&mut self.transactions);
        let first_error = self.mutations.close(&self.db).err();
        for transaction in transactions.into_values() {
            self.db
                .enqueue_abandon_transaction_handle(transaction.open_tx_id);
        }
        first_error.map_or(Ok(()), Err)
    }
}

impl Drop for ConnectedClient {
    fn drop(&mut self) {
        let _ = self.abandon_foreground_transactions();
    }
}

type ForegroundOperationFuture =
    Pin<Box<dyn Future<Output = Result<ForegroundOperationResult, RelayError>> + 'static>>;

type RelayTickFuture = Pin<Box<dyn Future<Output = Result<(), jazz::db::Error>>>>;
type RelayAdmissionFuture = Pin<
    Box<
        dyn Future<Output = Result<Rc<LocalMutex<PeerConnection<SqliteStorage>>>, jazz::db::Error>>,
    >,
>;

/// A peer's chunk lane must progress even while its semantic tick or a
/// foreground read owns the node. Retain both the endpoint and any suspended
/// local chunk read without borrowing the semantic connection.
struct RelayPeerIo {
    pump: PeerIoPump,
    wire: NativeRelayWire,
    incoming: Option<Pin<Box<dyn Future<Output = ()>>>>,
}

impl RelayPeerIo {
    fn new(pump: PeerIoPump, wire: NativeRelayWire) -> Self {
        Self {
            pump,
            wire,
            incoming: None,
        }
    }

    fn poll(&mut self, waker: &Waker) -> Result<(), RelayError> {
        let mut context = Context::from_waker(waker);
        for _ in 0..NATIVE_RELAY_DRAIN_MAX_MESSAGES {
            if self.incoming.is_none() {
                let message = self
                    .wire
                    .inbound
                    .lock()
                    .map_err(|_| RelayError::Poisoned("auxiliary inbound queue"))?
                    .pop_auxiliary();
                let Some(message) = message else { break };
                let pump = self.pump.clone();
                self.incoming = Some(Box::pin(async move {
                    // Only chunk messages are extracted; canonical traffic
                    // remains in its original FIFO for the semantic tick.
                    let _ = pump.route_incoming(message).await;
                }));
            }
            if self
                .incoming
                .as_mut()
                .expect("incoming was installed")
                .as_mut()
                .poll(&mut context)
                .is_pending()
            {
                break;
            }
            self.incoming = None;
        }
        let mut transport = QueueTransport {
            wire: self.wire.clone(),
            session_context: None,
        };
        for _ in 0..NATIVE_RELAY_DRAIN_MAX_MESSAGES {
            match self.pump.send_outbound(&mut transport, 1) {
                Ok(true) => {}
                Ok(false) | Err(TransportError::Backpressure) => break,
                Err(error) => {
                    return Err(RelayError::ForegroundCommand(format!(
                        "auxiliary transport failed: {error:?}"
                    )));
                }
            }
        }
        Ok(())
    }
}

fn poll_relay_tick<S>(
    db: &Rc<Db<S>>,
    pending: &mut Option<RelayTickFuture>,
    waker: &Waker,
) -> Result<(), RelayError>
where
    S: jazz::groove::storage::OrderedKvStorage + jazz::groove::storage::ReopenableStorage + 'static,
{
    let tick = pending.get_or_insert_with(|| {
        let db = Rc::clone(db);
        Box::pin(async move { db.tick().await })
    });
    let mut context = Context::from_waker(waker);
    match tick.as_mut().poll(&mut context) {
        Poll::Ready(result) => {
            *pending = None;
            map_tick_result(result)
        }
        Poll::Pending => Ok(()),
    }
}

/// A pending binding operation is foreground-owned, bounded, and deliberately
/// independent from the JSI call that started it. Dropping it cancels any
/// chunk-demand waiter held by the future.
struct ForegroundPendingOperation {
    subscription: Option<u64>,
    future: ForegroundOperationFuture,
    finish_on_cancel: bool,
}

enum ForegroundOperationResult {
    PermissionAdvice(ForegroundPermissionAdvice),
    Rows(Vec<u8>),
    SubscriptionEvents(Vec<ForegroundSubscriptionEvent>),
    TransactionSettled(TransactionId),
    TransactionCommitted(TransactionId),
    StreamingMutationPushed,
    StreamingMutationAborted(bool),
}

enum ForegroundOperationPoll {
    Pending { operation: u64 },
    Ready(ForegroundOperationResult),
    Error { reason: String },
}

fn foreground_operation_response(poll: ForegroundOperationPoll) -> ForegroundDbCommandResponse {
    match poll {
        ForegroundOperationPoll::Ready(ForegroundOperationResult::PermissionAdvice(advice)) => {
            ForegroundDbCommandResponse::PermissionAdvice { advice }
        }
        ForegroundOperationPoll::Pending { operation } => {
            ForegroundDbCommandResponse::Pending { operation }
        }
        ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(rows)) => {
            ForegroundDbCommandResponse::Rows { rows }
        }
        ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(events)) => {
            ForegroundDbCommandResponse::SubscriptionEvents { events }
        }
        ForegroundOperationPoll::Ready(ForegroundOperationResult::StreamingMutationPushed) => {
            ForegroundDbCommandResponse::StreamingMutationPushed
        }
        ForegroundOperationPoll::Ready(ForegroundOperationResult::StreamingMutationAborted(
            aborted,
        )) => ForegroundDbCommandResponse::StreamingMutationAborted { aborted },
        ForegroundOperationPoll::Ready(ForegroundOperationResult::TransactionCommitted(tx_id)) => {
            ForegroundDbCommandResponse::TransactionCommitted {
                tx_id: *tx_id.as_bytes(),
            }
        }
        ForegroundOperationPoll::Ready(ForegroundOperationResult::TransactionSettled(tx_id)) => {
            ForegroundDbCommandResponse::TransactionSettled {
                tx_id: *tx_id.as_bytes(),
            }
        }
        ForegroundOperationPoll::Error { reason } => {
            ForegroundDbCommandResponse::OperationError { reason }
        }
    }
}

/// Keep logical core failures in the foreground command vocabulary so the
/// shared adapter can preserve its normal permission/schema/error behavior.
/// Lifecycle failures remain C-ABI statuses: a stale foreground must never
/// look like a recoverable user mutation error.
fn foreground_command_error(
    error: RelayError,
) -> Result<ForegroundDbCommandResponse, JazzNativeRelayStatus> {
    match error {
        RelayError::Closed | RelayError::Poisoned(_) | RelayError::OwnerThread(_) => {
            Err(JazzNativeRelayStatus::LifecycleFailure)
        }
        RelayError::QueueCapacityExceeded { .. } => Err(JazzNativeRelayStatus::Backpressure),
        // Preserve the core Error prefix consumed by the shared TS adapter's
        // rejection normalizer, exactly as NAPI and WASM do.
        RelayError::Db(error) => Ok(ForegroundDbCommandResponse::OperationError {
            reason: error.to_string(),
        }),
        error => Ok(ForegroundDbCommandResponse::OperationError {
            reason: error.to_string(),
        }),
    }
}

/// Decode exactly one foreground command. Unlike `postcard::from_bytes`, this
/// rejects a syntactically valid command followed by ignored suffix bytes: the
/// byte ABI is one-command-per-call and must have one canonical spelling.
fn decode_foreground_command(bytes: &[u8]) -> Result<ForegroundDbCommandRequest, RelayError> {
    let (command, trailing) = postcard::take_from_bytes(bytes)
        .map_err(|error| RelayError::ForegroundCommand(format!("decode command: {error}")))?;
    if !trailing.is_empty() {
        return Err(RelayError::ForegroundCommand(
            "foreground command has trailing bytes".to_owned(),
        ));
    }
    let canonical = postcard::to_allocvec(&command).map_err(|error| {
        RelayError::ForegroundCommand(format!("encode canonical foreground command: {error}"))
    })?;
    if canonical != bytes {
        return Err(RelayError::ForegroundCommand(
            "foreground command is not canonically encoded".to_owned(),
        ));
    }
    Ok(command)
}

struct ClosingForeground {
    client: ConnectedClient,
    close: Option<RelayTickFuture>,
}

impl ClosingForeground {
    fn poll(&mut self, waker: &Waker) -> Result<bool, RelayError> {
        // Retirement is local. Failed peer I/O cannot prevent the local drain
        // from being polled or make a completed close wait for network recovery.
        let _ = self.client.upstream_io.poll(waker);
        let _ = self.client.poll_served_io(waker);
        if let Some(close) = &mut self.close
            && let Poll::Ready(result) = close.as_mut().poll(&mut Context::from_waker(waker))
        {
            self.close = None;
            result.map_err(RelayError::Db)?;
        }
        let _ = self.client.upstream_io.poll(waker);
        let _ = self.client.poll_served_io(waker);
        Ok(self.close.is_none())
    }
}

type UpstreamTransition = Pin<
    Box<
        dyn Future<
            Output = Result<
                Option<(
                    Rc<LocalMutex<PeerConnection<SqliteStorage>>>,
                    NativeRelayWire,
                )>,
                RelayError,
            >,
        >,
    >,
>;

struct RelayWorker {
    wake: Arc<RelayWake>,
    persistent: Rc<Db<SqliteStorage>>,
    persistent_tick: Option<RelayTickFuture>,
    upstream_io: RelayPeerIo,
    pending_foreground_wakes: PendingForegroundWakes,
    foreground_wake_generations: BTreeMap<u64, Arc<AtomicU64>>,
    owner_wake_queued: Arc<AtomicBool>,
    owner_commands: Weak<mpsc::SyncSender<RelayCommand>>,
    _upstream: Rc<LocalMutex<PeerConnection<SqliteStorage>>>,
    upstream_attached: bool,
    socket_generation: u64,
    socket_wire: Option<NativeRelayWire>,
    upstream_transition: Option<UpstreamTransition>,
    clients: BTreeMap<u64, ConnectedClient>,
    closing: VecDeque<ClosingForeground>,
    next_client_id: u64,
    pump_cursor: Option<u64>,
    schema: JazzSchema,
    liveness: Arc<RelayLiveness>,
}

impl RelayWorker {
    fn flush_foreground_wakes(&mut self) {
        let pending = std::mem::take(
            &mut *self
                .pending_foreground_wakes
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for (foreground, pending) in pending {
            if pending.generation.load(Ordering::Acquire) == pending.expected_generation {
                pending
                    .wake
                    .wake(foreground, pending.kind, pending.delay_ms);
            }
        }
    }

    fn open(
        config: RelayOpenConfig,
        wire: NativeRelayWire,
        liveness: Arc<RelayLiveness>,
        owner_wake_queued: Arc<AtomicBool>,
        owner_commands: Weak<mpsc::SyncSender<RelayCommand>>,
    ) -> Result<Self, RelayError> {
        let column_families = config.schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let codec_profile = epoch_1_storage_codec_profile().map_err(RelayError::Storage)?;
        let persistent = Rc::new(
            block_on(Db::open(DbConfig {
                schema: config.schema.clone(),
                storage: SqliteStorage::open_with_durability_and_codec_profile(
                    config.sqlite_path,
                    &refs,
                    SqliteDurability::WalNoSync,
                    &codec_profile,
                )
                .map_err(RelayError::Storage)?,
                identity: config.identity,
                id_source: None,
            }))
            .map_err(RelayError::Db)?,
        );
        let upstream = block_on(persistent.connect_upstream(Box::new(QueueTransport {
            wire: wire.clone(),
            session_context: None,
        })));
        let upstream_io = RelayPeerIo::new(block_on(upstream.lock()).io_pump(), wire);
        let wake = Arc::new(RelayWake::default());
        Ok(Self {
            wake,
            pending_foreground_wakes: Arc::default(),
            foreground_wake_generations: BTreeMap::new(),
            owner_wake_queued,
            owner_commands,
            persistent,
            persistent_tick: None,
            upstream_io,
            _upstream: upstream,
            upstream_attached: true,
            socket_generation: 0,
            socket_wire: None,
            upstream_transition: None,
            clients: BTreeMap::new(),
            closing: VecDeque::new(),
            next_client_id: 1,
            pump_cursor: None,
            schema: config.schema,
            liveness,
        })
    }

    fn begin_socket_upstream(
        &mut self,
        session_context: Option<jazz::db::ConnectionSessionContext>,
    ) -> Result<(u64, NativeRelayWire), RelayError> {
        self.socket_generation = self
            .socket_generation
            .checked_add(1)
            .ok_or(RelayError::ClientIdExhausted)?;
        self.upstream_io.wire.retire_connection();
        if let Some(wire) = self.socket_wire.take() {
            wire.retire_connection();
        }
        self.persistent_tick = None;
        self.upstream_io.incoming = None;
        self.upstream_transition = None;
        let wire = NativeRelayWire::for_owner(Arc::clone(&self.liveness));
        self.socket_wire = Some(wire.clone());
        let next_wire = wire.clone();
        let persistent = Rc::clone(&self.persistent);
        let previous = Rc::clone(&self._upstream);
        self.upstream_attached = false;
        self.upstream_transition = Some(Box::pin(async move {
            persistent
                .detach_connection_async(&previous)
                .await
                .map_err(RelayError::Db)?;
            let connection = persistent
                .try_connect_upstream(Box::new(QueueTransport {
                    wire: next_wire.clone(),
                    session_context,
                }))
                .await
                .map_err(RelayError::Db)?;
            Ok(Some((connection, next_wire)))
        }));
        self.poll_upstream_transition()?;
        Ok((self.socket_generation, wire))
    }

    fn retire_socket_upstream(&mut self, generation: u64) -> Result<bool, RelayError> {
        if generation != self.socket_generation || self.socket_wire.is_none() {
            return Ok(false);
        }
        self.socket_wire.take().unwrap().retire_connection();
        self.persistent_tick = None;
        self.upstream_io.incoming = None;
        self.upstream_transition = None;
        let persistent = Rc::clone(&self.persistent);
        let previous = Rc::clone(&self._upstream);
        self.upstream_attached = false;
        self.upstream_transition = Some(Box::pin(async move {
            persistent
                .detach_connection_async(&previous)
                .await
                .map_err(RelayError::Db)?;
            Ok(None)
        }));
        self.poll_upstream_transition()?;
        Ok(true)
    }

    fn poll_upstream_transition(&mut self) -> Result<(), RelayError> {
        let Some(mut transition) = self.upstream_transition.take() else {
            return Ok(());
        };
        let waker = Waker::from(Arc::clone(&self.wake));
        match transition.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Pending => self.upstream_transition = Some(transition),
            Poll::Ready(Ok(Some((connection, wire)))) => {
                self.upstream_io = RelayPeerIo::new(
                    connection
                        .try_lock()
                        .expect("installed upstream owner is available")
                        .io_pump(),
                    wire,
                );
                self._upstream = connection;
                self.upstream_attached = true;
            }
            Poll::Ready(Ok(None)) => {}
            Poll::Ready(Err(error)) => {
                if let Some(wire) = self.socket_wire.take() {
                    wire.retire_connection();
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn retire_foreground(&mut self, id: u64) -> Result<(), RelayError> {
        let mut client = self
            .clients
            .remove(&id)
            .ok_or(RelayError::UnknownClient(id))?;
        if let Some(generation) = self.foreground_wake_generations.remove(&id) {
            generation.fetch_add(1, Ordering::AcqRel);
        }
        let abandoned = client.abandon_foreground_transactions();
        let db = Rc::clone(&client.db);
        self.closing.push_back(ClosingForeground {
            client,
            close: Some(Box::pin(async move { db.close().await })),
        });
        // Cleanup is owned by this worker after public handle retirement. Its
        // timeout-driven owner turns continue even with no JS wake callbacks.
        abandoned
    }

    fn poll_closing(&mut self, waker: &Waker) -> Result<(), RelayError> {
        for _ in 0..self.closing.len().min(NATIVE_RELAY_PUMP_MAX_CLIENTS) {
            let mut closing = self.closing.pop_front().expect("selected closing owner");
            match closing.poll(waker) {
                Ok(true) => {}
                Ok(false) => self.closing.push_back(closing),
                Err(error) => {
                    // A core/storage close error is terminal for this owner.
                    // Release it now; other local drains still need their turns.
                    debug_assert!(closing.close.is_none());
                    return Err(error);
                }
            }
        }
        Ok(())
    }

    fn retire_all_foregrounds(&mut self) {
        let ids: Vec<_> = self.clients.keys().copied().collect();
        for id in ids {
            let _ = self.retire_foreground(id);
        }
    }

    fn finish_foreground_retirement(&mut self) {
        self.retire_all_foregrounds();
        while !self.closing.is_empty() {
            // pump polls local closes before any persistent peer work. An
            // unrelated I/O failure must not discard still-pending local work;
            // a terminal core close error removes only that failed owner.
            let _ = self.pump();
            if !self.closing.is_empty() {
                // No JavaScript callback remains to schedule these turns. Keep
                // pending local storage moving without spinning on a peer error.
                thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }

    fn refresh_account_claims(
        &mut self,
        author: jazz::ids::AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) {
        for client in self
            .clients
            .values_mut()
            .filter(|client| client.author == author)
        {
            client.db.set_identity_claims(author, claims.clone());
            client.refreshed_claims = Some(claims.clone());
            client.poll_claim_refresh();
        }
        self.wake.wake_by_ref();
    }

    fn attach_client(
        &mut self,
        identity: DbIdentity,
        claims: BTreeMap<String, Value>,
        tx_time_floor: Option<TxTime>,
        admitted_scope_advice: bool,
    ) -> Result<u64, RelayError> {
        let column_families = self.schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let db = Rc::new(
            block_on(Db::open(DbConfig {
                schema: self.schema.clone(),
                storage: MemoryStorage::new(&refs).expect("valid memory storage families"),
                identity,
                id_source: None,
            }))
            .map_err(RelayError::Db)?,
        );
        // A foreground owns only an in-memory preview. Its paired persistent
        // relay provides Local durability and the relay-authority subscription
        // handoff; treating this Db as durable makes sibling subscriptions wait
        // for the relay's upstream authority instead of consuming local state.
        db.set_non_durable_client();
        db.set_identity_claims(identity.author, claims.clone());
        if let Some(high_water) = tx_time_floor {
            block_on(db.reserve_minted_tx_time_after(high_water)).map_err(RelayError::Db)?;
        }
        let (client_transport, relay_transport, wire) = duplex(
            Arc::clone(&self.liveness),
            identity,
            self.persistent.identity(),
        );
        let upstream = block_on(db.connect_upstream(client_transport));
        // The scope and claims are captured now, while the capability is
        // admitted. Only the ordinary peer's owner/storage preparation may
        // wait; opening the new memory foreground must return synchronously.
        let persistent = Rc::clone(&self.persistent);
        let admission: RelayAdmissionFuture = Box::pin(async move {
            persistent
                .accept_subscriber_with_claims_async(relay_transport, identity.author, claims)
                .await
        });
        let upstream_io = RelayPeerIo::new(block_on(upstream.lock()).io_pump(), wire.clone());
        let id = self.next_client_id;
        self.next_client_id = self
            .next_client_id
            .checked_add(1)
            .ok_or(RelayError::ClientIdExhausted)?;
        self.clients.insert(
            id,
            ConnectedClient {
                author: identity.author,
                refreshed_claims: None,
                retiring: false,
                admitted_scope_advice,
                mutations: foreground_mutations::MutationHandles::new(&db),
                db,
                tick: None,
                upstream_io,
                served_io: None,
                admission: Some(admission),
                admission_error: None,
                wire,
                pending_subscriptions: BTreeMap::new(),
                subscriptions: BTreeMap::new(),
                pending_operations: BTreeMap::new(),
                mutation_cleanups: Vec::new(),
                read_cleanups: Rc::new(RefCell::new(VecDeque::new())),
                read_cleanup: None,
                transactions: BTreeMap::new(),
                committed_transactions: BTreeMap::new(),
                next_foreground_handle: 1,
                _upstream: upstream,
                _served: None,
            },
        );
        let client = self.clients.get_mut(&id).expect("new client was inserted");
        client.poll_admission(&Waker::from(Arc::clone(&self.wake)));
        if let Err(error) = client.check_admission() {
            self.clients.remove(&id);
            return Err(error);
        }
        Ok(id)
    }

    fn pump(&mut self) -> Result<(), RelayError> {
        let waker = Waker::from(Arc::clone(&self.wake));
        self.poll_upstream_transition()?;
        self.poll_closing(&waker)?;
        // One fair relay turn has exactly three protocol phases. A UI upload
        // becomes relay input, the relay applies/forwards it, then UI clients
        // observe resulting view/fate messages. More cascades schedule another
        // host turn; spinning until quiescence here would make a busy
        // subscription graph monopolize the native owner thread.
        let client_ids = bounded_round_robin_ids(&self.clients, self.pump_cursor);
        if let Some(last) = client_ids.last() {
            self.pump_cursor = Some(*last);
        }
        for id in &client_ids {
            let client = self.clients.get_mut(id).expect("selected client exists");
            client.poll_admission(&waker);
            if client.admission_error.is_some() {
                continue;
            }
            client.upstream_io.poll(&waker)?;
            poll_relay_tick(&client.db, &mut client.tick, &waker)?;
            client.poll_served_io(&waker)?;
            client.poll_read_cleanup(&waker);
            client.poll_mutation_cleanup(&waker);
        }
        if self.upstream_attached {
            self.upstream_io.poll(&waker)?;
        }
        poll_relay_tick(&self.persistent, &mut self.persistent_tick, &waker)?;
        for id in &client_ids {
            let client = self.clients.get_mut(id).expect("selected client exists");
            if client.admission_error.is_some() {
                continue;
            }
            client.poll_served_io(&waker)?;
            client.upstream_io.poll(&waker)?;
            poll_relay_tick(&client.db, &mut client.tick, &waker)?;
            client.poll_read_cleanup(&waker);
            client.poll_mutation_cleanup(&waker);
        }
        Ok(())
    }

    fn foreground_client(&self, client: u64) -> Result<&ConnectedClient, RelayError> {
        let client = self
            .clients
            .get(&client)
            .ok_or(RelayError::UnknownClient(client))?;
        client.check_admission()?;
        Ok(client)
    }

    fn foreground_client_mut(&mut self, client: u64) -> Result<&mut ConnectedClient, RelayError> {
        let client = self
            .clients
            .get_mut(&client)
            .ok_or(RelayError::UnknownClient(client))?;
        client.check_admission()?;
        Ok(client)
    }

    fn next_foreground_handle(client: &mut ConnectedClient) -> Result<u64, RelayError> {
        let handle = client.next_foreground_handle;
        client.next_foreground_handle = client
            .next_foreground_handle
            .checked_add(1)
            .ok_or(RelayError::ClientIdExhausted)?;
        Ok(handle)
    }

    fn start_foreground_read(
        &mut self,
        client: u64,
        query: Vec<u8>,
        options_json: String,
        transaction: Option<u64>,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let cleanups = {
            let client = self.foreground_client(client)?;
            if client.read_cleanups.borrow().len()
                + usize::from(client.read_cleanup.is_some())
                + client.pending_operations.len()
                >= NATIVE_RELAY_FOREGROUND_PENDING_MAX
            {
                return Err(RelayError::ForegroundCommand(
                    "foreground read cleanup capacity exceeded".to_owned(),
                ));
            }
            Rc::clone(&client.read_cleanups)
        };
        let opts = foreground_read_opts_from_json(&options_json)?;
        let open_tx = transaction
            .map(|handle| {
                self.foreground_transaction(client, handle)
                    .map(|(_, tx)| tx.open_tx_id)
            })
            .transpose()?;
        let db = {
            let client = self.foreground_client(client)?;
            Rc::clone(&client.db)
        };
        let read_db = Rc::clone(&db);
        let future: ForegroundOperationFuture = Box::pin(async move {
            let release_db = Rc::clone(&read_db);
            let result = read_db
                .all_serialized_query(
                    &query,
                    opts,
                    open_tx,
                    None,
                    None,
                    true,
                    || false,
                    move |attachment| {
                        cleanups.borrow_mut().push_back(attachment);
                        release_db.schedule_tick(TickUrgency::Immediate);
                    },
                )
                .await
                .map_err(RelayError::Db)?;
            let rows = match result {
                SerializedReadResult::Rows(rows) => jazz::binding_codec::encode_rows(&rows)
                    .map_err(|error| {
                        RelayError::ForegroundCommand(format!("encode row payload: {error}"))
                    })?,
                SerializedReadResult::Relation(snapshot) => {
                    jazz::binding_codec::encode_relation_snapshot(&snapshot).map_err(|error| {
                        RelayError::ForegroundCommand(format!("encode relation snapshot: {error}"))
                    })?
                }
            };
            Ok(ForegroundOperationResult::Rows(rows))
        });
        // Admit at command arrival, before a later commit can retire the open
        // transaction. Deferred query preparation must not reorder this read.
        let future = if let Some(tx) = open_tx {
            let (cancel, cancelled) = futures::channel::oneshot::channel::<()>();
            let receive = db.enqueue_transaction_read(tx, async move {
                // Dropping the observation retires its coverage and releases
                // the FIFO fence, without cancelling a later admitted commit.
                let result = match futures::future::select(cancelled, future).await {
                    futures::future::Either::Left(_) => Err(RelayError::Closed),
                    futures::future::Either::Right((result, _)) => result,
                };
                Ok(result)
            });
            Box::pin(async move {
                let _cancel_on_drop = cancel;
                receive
                    .await
                    .map_err(|_| RelayError::Closed)?
                    .map_err(RelayError::Db)?
            }) as ForegroundOperationFuture
        } else {
            future
        };
        self.start_foreground_operation(client, None, future)
    }

    fn subscribe_foreground_query_with_options(
        &mut self,
        client: u64,
        query: Vec<u8>,
        opts: ReadOpts,
    ) -> Result<u64, RelayError> {
        let client_id = client;
        let client = self.foreground_client_mut(client)?;
        let db = Rc::clone(&client.db);
        let opener: ForegroundSubscriptionOpen = Box::pin(async move {
            db.subscribe_serialized_query(
                &query,
                opts,
                None,
                SerializedSubscriptionAuthorization::ClientLocal,
            )
            .await
            .map_err(RelayError::Db)
        });
        self.start_foreground_subscription(client_id, opener)
    }

    fn start_foreground_subscription(
        &mut self,
        client: u64,
        mut opener: ForegroundSubscriptionOpen,
    ) -> Result<u64, RelayError> {
        let waker = Waker::from(Arc::clone(&self.wake));
        let client = self.foreground_client_mut(client)?;
        if client.pending_subscriptions.len() >= NATIVE_RELAY_FOREGROUND_PENDING_MAX {
            return Err(RelayError::ForegroundCommand(
                "foreground subscription opening capacity exceeded".to_owned(),
            ));
        }
        let handle = Self::next_foreground_handle(client)?;
        match opener.as_mut().poll(&mut Context::from_waker(&waker)) {
            Poll::Ready(result) => {
                client.subscriptions.insert(handle, result?);
            }
            Poll::Pending => {
                client.pending_subscriptions.insert(handle, opener);
            }
        }
        Ok(handle)
    }

    fn drain_foreground_subscription(
        &mut self,
        client: u64,
        subscription: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let waker = Waker::from(Arc::clone(&self.wake));
        let state = self.foreground_client_mut(client)?;
        if let Some(mut opener) = state.pending_subscriptions.remove(&subscription) {
            match opener.as_mut().poll(&mut Context::from_waker(&waker)) {
                Poll::Ready(result) => {
                    state.subscriptions.insert(subscription, result?);
                }
                Poll::Pending => {
                    state.pending_subscriptions.insert(subscription, opener);
                    return Ok(ForegroundOperationPoll::Ready(
                        ForegroundOperationResult::SubscriptionEvents(Vec::new()),
                    ));
                }
            }
        }
        let existing = self
            .foreground_client(client)?
            .pending_operations
            .iter()
            .find_map(|(operation, pending)| {
                (pending.subscription == Some(subscription)).then_some(*operation)
            });
        if let Some(operation) = existing {
            return Ok(ForegroundOperationPoll::Pending { operation });
        }
        let (db, pending) = {
            let client = self.foreground_client_mut(client)?;
            let mut pending = Vec::new();
            {
                let stream = client.subscriptions.get_mut(&subscription).ok_or_else(|| {
                    RelayError::ForegroundCommand(format!(
                        "unknown foreground subscription {subscription}"
                    ))
                })?;
                while pending.len() < NATIVE_RELAY_DRAIN_MAX_MESSAGES {
                    let Some(event) = stream.try_next_event() else {
                        break;
                    };
                    pending.push(event);
                }
            }
            (Rc::clone(&client.db), pending)
        };
        if pending.is_empty() {
            return Ok(ForegroundOperationPoll::Ready(
                ForegroundOperationResult::SubscriptionEvents(Vec::new()),
            ));
        }
        let future: ForegroundOperationFuture = Box::pin(async move {
            let mut events = Vec::with_capacity(pending.len());
            for mut event in pending {
                // A missing chunk is intentionally allowed to suspend this
                // retained future. The foreground owner can then tick peer
                // I/O and poll again; never block the owner thread here.
                db.hydrate_subscription_event_for_binding(&mut event)
                    .await
                    .map_err(RelayError::Db)?;
                events.push(encode_foreground_subscription_event(event)?);
            }
            Ok(ForegroundOperationResult::SubscriptionEvents(events))
        });
        self.start_foreground_operation(client, Some(subscription), future)
    }

    fn request_foreground_permission_advice(
        &mut self,
        client: u64,
        action: ForegroundPermissionAdviceAction,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        use jazz::protocol::{PermissionAdvice, PermissionAdviceAction};
        let action = match action {
            ForegroundPermissionAdviceAction::Insert { table, cells } => {
                PermissionAdviceAction::Insert {
                    table,
                    cells: decode_foreground_cells(&cells)?,
                }
            }
            ForegroundPermissionAdviceAction::Read { table, row } => PermissionAdviceAction::Read {
                table,
                row: RowUuid::from_bytes(row),
            },
            ForegroundPermissionAdviceAction::Update { table, row, patch } => {
                PermissionAdviceAction::Update {
                    table,
                    row: RowUuid::from_bytes(row),
                    patch: decode_foreground_cells(&patch)?,
                }
            }
            ForegroundPermissionAdviceAction::Delete { table, row } => {
                PermissionAdviceAction::Delete {
                    table,
                    row: RowUuid::from_bytes(row),
                }
            }
        };
        if !self.foreground_client(client)?.admitted_scope_advice {
            return Ok(ForegroundOperationPoll::Ready(
                ForegroundOperationResult::PermissionAdvice(ForegroundPermissionAdvice::Unknown),
            ));
        }
        // The foreground's immediate upstream is a partial replica and cannot
        // issue advice. The capability binds this lease to the persistent Db's
        // authenticated upstream subject; request on that ordinary Db while
        // keeping cancellation and delivery exclusively foreground-owned.
        let advice = self.persistent.request_permission_advice(action);
        self.start_foreground_operation(
            client,
            None,
            Box::pin(async move {
                Ok(ForegroundOperationResult::PermissionAdvice(
                    match advice.await {
                        PermissionAdvice::Allowed => ForegroundPermissionAdvice::Allowed,
                        PermissionAdvice::Denied => ForegroundPermissionAdvice::Denied,
                        PermissionAdvice::Unknown => ForegroundPermissionAdvice::Unknown,
                    },
                ))
            }),
        )
    }

    fn start_foreground_operation(
        &mut self,
        client: u64,
        subscription: Option<u64>,
        future: ForegroundOperationFuture,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let operation = {
            let client = self.foreground_client_mut(client)?;
            if client.pending_operations.len() + client.mutation_cleanups.len()
                >= NATIVE_RELAY_FOREGROUND_PENDING_MAX
            {
                return Err(RelayError::ForegroundCommand(format!(
                    "foreground pending operation capacity {} exceeded",
                    NATIVE_RELAY_FOREGROUND_PENDING_MAX
                )));
            }
            let operation = Self::next_foreground_handle(client)?;
            client.pending_operations.insert(
                operation,
                ForegroundPendingOperation {
                    subscription,
                    future,
                    finish_on_cancel: false,
                },
            );
            operation
        };
        self.poll_foreground_operation(client, operation)
    }

    fn poll_foreground_operation(
        &mut self,
        client: u64,
        operation: u64,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let Some(mut pending_operation) = self
            .foreground_client_mut(client)?
            .pending_operations
            .remove(&operation)
        else {
            return Err(RelayError::ForegroundCommand(format!(
                "unknown foreground operation {operation}"
            )));
        };
        let waker = Waker::from(Arc::clone(&self.wake));
        let mut context = Context::from_waker(&waker);
        match pending_operation.future.as_mut().poll(&mut context) {
            Poll::Ready(Ok(result)) => Ok(ForegroundOperationPoll::Ready(result)),
            Poll::Ready(Err(error)) => Ok(ForegroundOperationPoll::Error {
                reason: match error {
                    RelayError::Db(error) => error.to_string(),
                    error => error.to_string(),
                },
            }),
            Poll::Pending => {
                self.foreground_client_mut(client)?
                    .pending_operations
                    .insert(operation, pending_operation);
                Ok(ForegroundOperationPoll::Pending { operation })
            }
        }
    }

    fn cancel_foreground_operation(
        &mut self,
        client: u64,
        operation: u64,
    ) -> Result<bool, RelayError> {
        let client = self.foreground_client_mut(client)?;
        let Some(pending) = client.pending_operations.remove(&operation) else {
            return Ok(false);
        };
        if pending.finish_on_cancel {
            // Cancellation retires the caller's result, not admitted finish/abort.
            client.mutation_cleanups.push(pending.future);
            self.wake.wake_by_ref();
        }
        Ok(true)
    }

    fn close_foreground_subscription(
        &mut self,
        client: u64,
        subscription: u64,
    ) -> Result<bool, RelayError> {
        let client = self.foreground_client_mut(client)?;
        client
            .pending_operations
            .retain(|_, pending| pending.subscription != Some(subscription));
        if client.pending_subscriptions.remove(&subscription).is_some() {
            return Ok(true);
        }
        let Some(subscription) = client.subscriptions.remove(&subscription) else {
            return Ok(false);
        };
        // `SubscriptionStream::close` awaits a node turn. This command is
        // itself executing on that node's owner thread, so awaiting it here
        // would deadlock the JSI call. Dropping queues the identical cleanup
        // without an acknowledgement; the following ordinary Tick performs
        // it. The handle is already retired, so no subsequent drain can
        // publish an old buffered event.
        drop(subscription);
        Ok(true)
    }

    fn begin_foreground_transaction(
        &mut self,
        client: u64,
        kind: ForegroundTransactionKind,
    ) -> Result<u64, RelayError> {
        let (db, handle) = {
            let client = self.foreground_client_mut(client)?;
            if client.transactions.len() >= NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX {
                return Err(RelayError::ForegroundCommand(format!(
                    "foreground transaction capacity {} exceeded",
                    NATIVE_RELAY_FOREGROUND_TRANSACTION_MAX
                )));
            }
            (Rc::clone(&client.db), Self::next_foreground_handle(client)?)
        };
        let open_tx_id = OpenTransactionId::new();
        match kind {
            ForegroundTransactionKind::Mergeable => {
                db.enqueue_begin_mergeable(open_tx_id, None, None)
            }
            ForegroundTransactionKind::Exclusive => {
                db.enqueue_begin_exclusive(open_tx_id, None, None)
            }
        }
        .map_err(RelayError::Db)?;
        db.drive_queued_mutation_once();
        if let Some(error) = db.queued_transaction_error(open_tx_id) {
            db.enqueue_abandon_transaction_handle(open_tx_id);
            db.drive_queued_mutation_once();
            return Err(RelayError::Db(error));
        }
        self.foreground_client_mut(client)?
            .transactions
            .insert(handle, ForegroundTransaction { open_tx_id, kind });
        Ok(handle)
    }

    fn foreground_transaction(
        &self,
        client: u64,
        transaction: u64,
    ) -> Result<(Rc<Db<MemoryStorage>>, ForegroundTransaction), RelayError> {
        let client = self.foreground_client(client)?;
        let transaction = client
            .transactions
            .get(&transaction)
            .copied()
            .ok_or_else(|| {
                RelayError::ForegroundCommand(format!(
                    "unknown foreground transaction {transaction}"
                ))
            })?;
        Ok((Rc::clone(&client.db), transaction))
    }

    #[allow(clippy::too_many_arguments)] // Mirrors the flat, versioned command envelope.
    fn stage_foreground_mutation(
        &mut self,
        client: u64,
        transaction: u64,
        mutation: ForegroundMutationKind,
        table: String,
        row_id: Option<[u8; 16]>,
        cells: Vec<u8>,
        options_json: String,
    ) -> Result<Option<RowUuid>, RelayError> {
        let options = foreground_mutations::parse_mutation_options(mutation, &options_json)?;
        let (db, transaction) = self.foreground_transaction(client, transaction)?;
        let cells = if matches!(mutation, ForegroundMutationKind::Delete) {
            Default::default()
        } else {
            decode_foreground_cells(&cells)?
        };
        let row_id = row_id.map(RowUuid::from_bytes);
        let exact_target = options
            .branch
            .clone()
            .map(jazz::db::ExactWriteTarget::Branch)
            .unwrap_or_default();
        let target = match options.head {
            Some(head) => jazz::db::WriteTarget::BranchView {
                head,
                base: options.base,
            },
            None if options.base.is_none() => Default::default(),
            None => {
                return Err(RelayError::ForegroundCommand(
                    "branch view base requires a head selector".into(),
                ));
            }
        };
        let updated_at_ms = options.updated_at_ms;
        let id = transaction.open_tx_id;
        let result = match mutation {
            ForegroundMutationKind::Insert => db
                .enqueue_transaction_insert(
                    id,
                    table,
                    cells,
                    jazz::db::InsertOptions {
                        row_id,
                        target: exact_target,
                        updated_at_ms,
                        ..Default::default()
                    },
                )
                .map(Some),
            mutation => {
                let row = row_id.ok_or_else(|| {
                    RelayError::ForegroundCommand("mutation requires row id".into())
                })?;
                match mutation {
                    ForegroundMutationKind::Update => db.enqueue_transaction_update(
                        id,
                        table,
                        row,
                        cells,
                        UpdateOptions {
                            target,
                            updated_at_ms,
                            ..Default::default()
                        },
                    ),
                    ForegroundMutationKind::Upsert => db.enqueue_transaction_upsert(
                        id,
                        table,
                        row,
                        cells,
                        UpsertOptions {
                            target,
                            updated_at_ms,
                            ..Default::default()
                        },
                    ),
                    ForegroundMutationKind::Delete => db.enqueue_transaction_delete(
                        id,
                        table,
                        row,
                        DeleteOptions {
                            target,
                            updated_at_ms,
                            ..Default::default()
                        },
                    ),
                    ForegroundMutationKind::Restore => db.enqueue_transaction_restore(
                        id,
                        table,
                        row,
                        Some(cells),
                        jazz::db::RestoreOptions {
                            target: exact_target,
                            updated_at_ms,
                            ..Default::default()
                        },
                    ),
                    ForegroundMutationKind::Insert => unreachable!(),
                }
                .map(|()| None)
            }
        }
        .map_err(RelayError::Db)?;
        db.drive_queued_mutation_once();
        if let Some(error) = db.queued_transaction_error(id) {
            return Err(RelayError::Db(error));
        }
        Ok(result)
    }

    fn insert_foreground_transaction(
        &mut self,
        client: u64,
        transaction: u64,
        table: String,
        cells: Vec<u8>,
        row_id: Option<[u8; 16]>,
    ) -> Result<RowUuid, RelayError> {
        self.stage_foreground_mutation(
            client,
            transaction,
            ForegroundMutationKind::Insert,
            table,
            row_id,
            cells,
            "{}".into(),
        )?
        .ok_or_else(|| RelayError::ForegroundCommand("insert omitted row id".into()))
    }

    fn update_foreground_transaction(
        &mut self,
        client: u64,
        transaction: u64,
        table: String,
        row_id: [u8; 16],
        patch: Vec<u8>,
    ) -> Result<(), RelayError> {
        self.stage_foreground_mutation(
            client,
            transaction,
            ForegroundMutationKind::Update,
            table,
            Some(row_id),
            patch,
            "{}".into(),
        )
        .map(|_| ())
    }

    fn upsert_foreground_transaction(
        &mut self,
        client: u64,
        transaction: u64,
        table: String,
        row_id: [u8; 16],
        cells: Vec<u8>,
    ) -> Result<(), RelayError> {
        self.stage_foreground_mutation(
            client,
            transaction,
            ForegroundMutationKind::Upsert,
            table,
            Some(row_id),
            cells,
            "{}".into(),
        )
        .map(|_| ())
    }

    fn delete_foreground_transaction(
        &mut self,
        client: u64,
        transaction: u64,
        table: String,
        row_id: [u8; 16],
    ) -> Result<(), RelayError> {
        self.stage_foreground_mutation(
            client,
            transaction,
            ForegroundMutationKind::Delete,
            table,
            Some(row_id),
            Vec::new(),
            "{}".into(),
        )
        .map(|_| ())
    }

    fn commit_foreground_transaction(
        &mut self,
        client: u64,
        transaction: u64,
    ) -> Result<TransactionId, RelayError> {
        let (db, transaction_state) = self.foreground_transaction(client, transaction)?;
        let write = match transaction_state.kind {
            ForegroundTransactionKind::Mergeable => {
                db.enqueue_commit_mergeable_handle(transaction_state.open_tx_id)
            }
            ForegroundTransactionKind::Exclusive => {
                db.enqueue_commit_exclusive_handle(transaction_state.open_tx_id)
            }
        }
        .map_err(RelayError::Db)?;
        self.foreground_client_mut(client)?
            .transactions
            .remove(&transaction);
        db.drive_queued_mutation_once();
        if let Some(error) = db.take_queued_mutation_failure(write.mergeable_tx_id()) {
            return Err(RelayError::Db(error));
        }
        let public_id = TransactionId::from_committed_tx(write.mergeable_tx_id());
        self.foreground_client_mut(client)?
            .mutations
            .writes
            .borrow_mut()
            .insert(public_id, Rc::new(write));
        Ok(public_id)
    }

    fn wait_for_pending_writes(
        &mut self,
        client: u64,
        tier: CoreDurabilityTier,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let foreground = Rc::clone(&self.foreground_client(client)?.db);
        let persistent = Rc::clone(&self.persistent);
        let future: ForegroundOperationFuture = Box::pin(async move {
            foreground
                .wait_for_pending_writes(tier)
                .await
                .map_err(RelayError::Db)?;
            persistent
                .wait_for_pending_writes(tier)
                .await
                .map_err(RelayError::Db)?;
            // Empty byte completion uses the existing pollable command result.
            Ok(ForegroundOperationResult::Rows(Vec::new()))
        });
        self.start_foreground_operation(client, None, future)
    }

    fn wait_for_core_transaction(
        &mut self,
        client: u64,
        public_tx_id: [u8; 16],
    ) -> Result<ForegroundOperationPoll, RelayError> {
        self.wait_for_foreground_transaction(client, public_tx_id, CoreDurabilityTier::Global)
    }

    fn wait_for_foreground_transaction(
        &mut self,
        client: u64,
        public_tx_id: [u8; 16],
        tier: CoreDurabilityTier,
    ) -> Result<ForegroundOperationPoll, RelayError> {
        let retained = {
            let client = self.foreground_client(client)?;
            client
                .mutations
                .writes
                .borrow()
                .iter()
                .find(|(id, _)| *id.as_bytes() == public_tx_id)
                .map(|(id, write)| (*id, Rc::clone(write)))
        };
        if let Some((id, write)) = retained {
            // WriteHandle::wait is a snapshot check. The core callback API
            // owns the asynchronous waiter and preserves queued no-op aliases.
            let db = Rc::clone(&self.foreground_client(client)?.db);
            let future: ForegroundOperationFuture = Box::pin(async move {
                // Register only after the bounded operation slot is admitted.
                let (send, receive) = futures::channel::oneshot::channel();
                db.wait_for_write_with(&write, tier, move |result| {
                    let _ = send.send(result);
                });
                let _retained_write = write;
                receive
                    .await
                    .map_err(|_| RelayError::Closed)?
                    .map_err(RelayError::Db)?;
                Ok(ForegroundOperationResult::TransactionSettled(id))
            });
            return self.start_foreground_operation(client, None, future);
        }
        let (db, public_tx_id, tx_id) = {
            let client = self.foreground_client(client)?;
            let (&public_tx_id, &tx_id) = client
                .committed_transactions
                .iter()
                .find(|(public, _)| *public.as_bytes() == public_tx_id)
                .ok_or_else(|| {
                    RelayError::ForegroundCommand(
                        "unknown foreground transaction id for Core settlement".to_owned(),
                    )
                })?;
            (Rc::clone(&client.db), public_tx_id, tx_id)
        };
        let future: ForegroundOperationFuture = Box::pin(async move {
            db.wait_for_transaction(tx_id, tier)
                .await
                .map_err(RelayError::Db)?;
            Ok(ForegroundOperationResult::TransactionSettled(public_tx_id))
        });
        self.start_foreground_operation(client, None, future)
    }

    fn rollback_foreground_transaction(
        &mut self,
        client: u64,
        transaction: u64,
    ) -> Result<bool, RelayError> {
        let (db, transaction_state) = self.foreground_transaction(client, transaction)?;
        db.enqueue_abandon_transaction_handle(transaction_state.open_tx_id);
        db.drive_queued_mutation_once();
        self.foreground_client_mut(client)?
            .transactions
            .remove(&transaction);
        Ok(true)
    }
}

fn foreground_read_opts_from_json(json: &str) -> Result<ReadOpts, RelayError> {
    let failure =
        |error: String| RelayError::ForegroundCommand(format!("invalid read options: {error}"));
    let supplied: serde_json::Value =
        serde_json::from_str(json).map_err(|e| failure(e.to_string()))?;
    let mut value =
        serde_json::to_value(ReadOpts::default()).map_err(|e| failure(e.to_string()))?;
    if supplied.is_null() {
        return Ok(ReadOpts::default());
    }
    let object = supplied
        .as_object()
        .ok_or_else(|| failure("expected object".to_owned()))?;
    for (key, item) in object {
        if item.is_null() {
            continue;
        }
        let key = if key == "readView" {
            "read_view"
        } else {
            key.as_str()
        };
        let normalized = match (key, item.as_str()) {
            ("tier", Some("local" | "Local" | "local-first" | "LocalFirst")) => Some("Local"),
            (
                "tier",
                Some(
                    "edge" | "Edge" | "remote" | "Remote" | "remote-if-possible"
                    | "RemoteIfPossible",
                ),
            ) => Some("Edge"),
            ("tier", Some("global" | "Global" | "core" | "Core")) => Some("Global"),
            ("tier", Some("none" | "None")) => Some("None"),
            ("local_updates", Some("immediate" | "Immediate")) => Some("Immediate"),
            ("local_updates", Some("deferred" | "Deferred")) => Some("Deferred"),
            ("propagation", Some("full" | "Full")) => Some("Full"),
            ("propagation", Some("LocalOnly" | "local_only" | "localOnly" | "local-only")) => {
                Some("LocalOnly")
            }
            _ => None,
        };
        value[key] = normalized
            .map(|s| serde_json::Value::String(s.to_owned()))
            .unwrap_or_else(|| item.clone());
    }
    serde_json::from_value(value).map_err(|e| failure(e.to_string()))
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ForegroundMutationOptions {
    branch: Option<jazz::protocol::BranchSelector>,
    head: Option<jazz::protocol::BranchSelector>,
    base: Option<jazz::protocol::BranchViewBase>,
    updated_at_ms: Option<u64>,
}

/// Decode the established NAPI/WASM encoded-cell record envelope. The
/// foreground ABI deliberately shares this compact descriptor-plus-bytes
/// representation; it does not invent a React-Native row/value object shape.
fn decode_foreground_cells(bytes: &[u8]) -> Result<jazz::db::RowCells, RelayError> {
    jazz::binding_codec::decode_named_cells(bytes)
        .map_err(|error| RelayError::ForegroundCommand(format!("decode cells: {error}")))
}

fn encode_foreground_subscription_event(
    mut event: SubscriptionEvent,
) -> Result<ForegroundSubscriptionEvent, RelayError> {
    match &mut event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            terminal_operations,
            settled,
            tier,
            ..
        } => {
            let delta = jazz::binding_codec::encode_subscription_delta(added, updated, removed)
                .map_err(|error| {
                    RelayError::ForegroundCommand(format!("encode subscription delta: {error}"))
                })?;
            if !terminal_operations.is_empty() {
                let terminal_operations_json =
                    jazz::binding_codec::terminal_operations_to_json(terminal_operations)
                        .map_err(|error| {
                            RelayError::ForegroundCommand(format!(
                                "encode terminal operations: {error}"
                            ))
                        })?
                        .to_string();
                return Ok(ForegroundSubscriptionEvent::StructuredDelta {
                    reset: *reset,
                    settled: *settled,
                    tier: format!("{tier:?}").to_ascii_lowercase(),
                    delta,
                    terminal_operations_json,
                });
            }
            Ok(ForegroundSubscriptionEvent::Delta {
                reset: *reset,
                settled: *settled,
                tier: format!("{tier:?}").to_ascii_lowercase(),
                delta,
            })
        }
        SubscriptionEvent::Rejected { reason } => Ok(ForegroundSubscriptionEvent::Rejected {
            reason: format!("{reason:?}"),
        }),
        SubscriptionEvent::Closed => Ok(ForegroundSubscriptionEvent::Closed),
    }
}

fn map_tick_result<T>(result: Result<T, jazz::db::Error>) -> Result<(), RelayError> {
    result.map(|_| ()).map_err(RelayError::Db)
}

fn transport_queue_error(error: RelayError) -> TransportError {
    match error {
        RelayError::QueueCapacityExceeded { .. } => TransportError::Backpressure,
        error => TransportError::Failed(error.to_string()),
    }
}

fn bounded_round_robin_ids<T>(clients: &BTreeMap<u64, T>, cursor: Option<u64>) -> Vec<u64> {
    let mut client_ids = clients.keys().copied().collect::<Vec<_>>();
    if let Some(cursor) = cursor {
        let split = client_ids.partition_point(|id| *id <= cursor);
        client_ids.rotate_left(split);
    }
    client_ids.truncate(NATIVE_RELAY_PUMP_MAX_CLIENTS);
    client_ids
}

type RelayJob = Box<dyn FnOnce(&mut RelayWorker) + Send + 'static>;

struct NormalOwnerQueuePermit(Arc<AtomicUsize>);

impl NormalOwnerQueuePermit {
    fn acquire(depth: &Arc<AtomicUsize>) -> Result<Self, RelayError> {
        depth
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |depth| {
                (depth < NATIVE_RELAY_OWNER_COMMAND_MAX).then_some(depth + 1)
            })
            .map_err(|_| RelayError::OwnerQueueFull)?;
        Ok(Self(Arc::clone(depth)))
    }
}

impl Drop for NormalOwnerQueuePermit {
    fn drop(&mut self) {
        let previous = self.0.fetch_sub(1, Ordering::AcqRel);
        debug_assert!(previous > 0);
    }
}

enum RelayCommand {
    FlushForegroundWakes,
    Run {
        job: RelayJob,
        _normal_permit: Option<NormalOwnerQueuePermit>,
    },
    Shutdown(mpsc::Sender<()>),
}

impl NativeRelay {
    pub fn spawn(config: RelayOpenConfig) -> Result<Self, RelayError> {
        // This is before channel/thread creation and the worker's SQLite open.
        config.validate()?;
        let sqlite_path = config.sqlite_path.clone();
        let schema_version = config.schema.version_id();
        let identity = config.identity;
        let liveness = Arc::new(RelayLiveness::new());
        let wire = NativeRelayWire::for_owner(Arc::clone(&liveness));
        // One coalesced internal owner-wake may coexist with the normal command
        // budget and its lifecycle reserve; it never consumes teardown capacity.
        let (commands, receiver) = mpsc::sync_channel::<RelayCommand>(
            NATIVE_RELAY_OWNER_COMMAND_MAX + NATIVE_RELAY_OWNER_TEARDOWN_RESERVE + 1,
        );
        let commands = Arc::new(commands);
        let normal_queue_depth = Arc::new(AtomicUsize::new(0));
        let owner_wake_queued = Arc::new(AtomicBool::new(false));
        let (started_tx, started_rx) = mpsc::channel();
        let owner_wire = wire.clone();
        let owner_liveness = Arc::clone(&liveness);
        let owner_wake_queued_for_worker = Arc::clone(&owner_wake_queued);
        let owner_commands = Arc::downgrade(&commands);
        #[cfg(test)]
        if let Some(counter) = &config.thread_start_counter {
            counter.fetch_add(1, Ordering::Relaxed);
        }
        let join = thread::Builder::new()
            .name("jazz-native-relay".to_owned())
            .spawn(move || {
                let _liveness = OwnerLiveness(owner_liveness.clone());
                let mut worker = match RelayWorker::open(
                    config,
                    owner_wire,
                    owner_liveness,
                    owner_wake_queued_for_worker,
                    owner_commands,
                ) {
                    Ok(worker) => {
                        let _ = started_tx.send(Ok(()));
                        worker
                    }
                    Err(error) => {
                        let _ = started_tx.send(Err(error));
                        return;
                    }
                };
                loop {
                    let command = if worker.closing.is_empty() {
                        receiver
                            .recv()
                            .map_err(|_| mpsc::RecvTimeoutError::Disconnected)
                    } else {
                        receiver.recv_timeout(std::time::Duration::from_millis(1))
                    };
                    let command = match command {
                        Ok(command) => command,
                        Err(mpsc::RecvTimeoutError::Timeout) => {
                            let _ = worker.pump();
                            continue;
                        }
                        Err(mpsc::RecvTimeoutError::Disconnected) => {
                            worker.finish_foreground_retirement();
                            return;
                        }
                    };
                    match command {
                        RelayCommand::FlushForegroundWakes => {
                            worker.owner_wake_queued.store(false, Ordering::Release);
                            worker.flush_foreground_wakes();
                        }
                        RelayCommand::Run {
                            job,
                            _normal_permit,
                        } => {
                            job(&mut worker);
                            if !worker.closing.is_empty() {
                                let _ = worker.pump();
                            }
                        }
                        RelayCommand::Shutdown(done) => {
                            worker.finish_foreground_retirement();
                            drop(worker);
                            let _ = done.send(());
                            return;
                        }
                    }
                }
            })
            .map_err(|error| RelayError::OwnerThread(error.to_string()))?;
        started_rx.recv().map_err(|_| {
            RelayError::OwnerThread("owner exited before opening relay".to_owned())
        })??;
        Ok(Self {
            inner: Arc::new(RelayInner {
                jobs: Mutex::new(Some(commands)),
                normal_queue_depth,
                join: Mutex::new(Some(join)),
                liveness,
                wire,
                sqlite_path,
                schema_version,
                identity,
            }),
        })
    }

    pub fn abi_version(&self) -> u16 {
        NATIVE_RELAY_ABI_V1
    }

    /// Verify that a host wrapper understands this embedded native relay before
    /// opening a scope. This is also available as
    /// [`ensure_native_relay_abi_compatible`] for wrappers that must check
    /// before constructing a relay.
    pub fn ensure_abi_compatible(wrapper_range: NativeRelayAbiRange) -> Result<u16, RelayError> {
        ensure_native_relay_abi_compatible(wrapper_range)
    }

    pub fn wire(&self) -> NativeRelayWire {
        self.inner.wire.clone()
    }

    fn is_alive(&self) -> bool {
        self.inner.liveness.is_alive()
    }

    pub fn attach_client(
        &self,
        identity: DbIdentity,
        claims: BTreeMap<String, Value>,
    ) -> Result<NativeRelayClient, RelayError> {
        let id = self.run(move |worker| worker.attach_client(identity, claims, None, false))?;
        Ok(NativeRelayClient {
            relay: self.clone(),
            id,
            wire: self.run(move |worker| {
                worker
                    .clients
                    .get(&id)
                    .map(|client| client.wire.clone())
                    .ok_or(RelayError::UnknownClient(id))
            })?,
        })
    }

    fn attach_foreground_client(
        &self,
        identity: DbIdentity,
        claims: BTreeMap<String, Value>,
        lease: ForegroundNodeLease,
    ) -> Result<NativeRelayClient, RelayError> {
        if identity.author != self.inner.identity.author {
            return Err(RelayError::ScopeConfigurationMismatch);
        }
        let id = self.run(move |worker| {
            worker.attach_client(identity, claims, Some(lease.confirmed_tx_time), true)
        })?;
        Ok(NativeRelayClient {
            relay: self.clone(),
            id,
            wire: self.run(move |worker| {
                worker
                    .clients
                    .get(&id)
                    .map(|client| client.wire.clone())
                    .ok_or(RelayError::UnknownClient(id))
            })?,
        })
    }

    pub fn pump(&self) -> Result<(), RelayError> {
        self.run(|worker| worker.pump())
    }

    fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut RelayWorker) -> Result<T, RelayError> + Send + 'static,
    ) -> Result<T, RelayError> {
        self.run_with_queue_class(operation, false)
    }

    /// Host lifecycle work uses the one reserved physical queue slot. The
    /// `NativeRelayHost` serializes these calls under its mutex, so at most one
    /// teardown command can occupy the reserve while ordinary callers remain
    /// bounded by `NATIVE_RELAY_OWNER_COMMAND_MAX` independent permits.
    fn run_teardown<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut RelayWorker) -> Result<T, RelayError> + Send + 'static,
    ) -> Result<T, RelayError> {
        self.run_with_queue_class(operation, true)
    }

    fn run_with_queue_class<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut RelayWorker) -> Result<T, RelayError> + Send + 'static,
        teardown: bool,
    ) -> Result<T, RelayError> {
        let (response_tx, response_rx) = mpsc::channel();
        let job: RelayJob = Box::new(move |worker| {
            let result = operation(worker);
            worker.flush_foreground_wakes();
            let _ = response_tx.send(result);
        });
        let normal_permit = (!teardown)
            .then(|| NormalOwnerQueuePermit::acquire(&self.inner.normal_queue_depth))
            .transpose()?;
        let admitted = {
            let _terminal = self.inner.liveness.enter()?;
            self.inner
                .jobs
                .lock()
                .map_err(|_| RelayError::Poisoned("relay command queue"))?
                .as_ref()
                .ok_or(RelayError::Closed)?
                .try_send(RelayCommand::Run {
                    job,
                    _normal_permit: normal_permit,
                })
                .map_err(|error| match error {
                    mpsc::TrySendError::Full(_) => RelayError::OwnerQueueFull,
                    mpsc::TrySendError::Disconnected(_) => RelayError::Closed,
                })
        };
        if let Err(error) = admitted {
            if matches!(error, RelayError::Closed) {
                self.inner.liveness.mark_terminal();
            }
            return Err(error);
        }
        response_rx.recv().map_err(|_| {
            self.inner.liveness.mark_terminal();
            RelayError::Closed
        })?
    }
}

/// A registry is owned by the platform host (application process), not global
/// Rust state. That makes teardown explicit and lets Android services, iOS app
/// delegates, and test processes choose their own lifecycle semantics.
#[derive(Default)]
pub struct NativeRelayRegistry {
    relays: Mutex<BTreeMap<RelayScope, NativeRelay>>,
}

impl NativeRelayRegistry {
    pub fn open(&self, config: RelayOpenConfig) -> Result<NativeRelay, RelayError> {
        config.validate()?;
        let mut relays = self
            .relays
            .lock()
            .map_err(|_| RelayError::Poisoned("relay registry"))?;
        if let Some(existing) = relays.get(&config.scope) {
            if existing.inner.sqlite_path != config.sqlite_path
                || existing.inner.schema_version != config.schema.version_id()
                || existing.inner.identity != config.identity
            {
                return Err(RelayError::ScopeConfigurationMismatch);
            }
            if existing.is_alive() {
                return Ok(existing.clone());
            }
            let stale = existing.clone();
            stale.inner.shutdown()?;
            relays.remove(&config.scope);
        }
        // Linked identities share an account's durable root, but never its
        // live authorization scope. Keep the old owner exclusive until close
        // has joined it, including when that owner has become terminal.
        if relays
            .values()
            .any(|relay| relay.inner.sqlite_path == config.sqlite_path)
        {
            return Err(RelayError::StorageAlreadyOwned);
        }
        let relay = NativeRelay::spawn(config.clone())?;
        relays.insert(config.scope, relay.clone());
        Ok(relay)
    }

    pub fn close(&self, scope: &RelayScope) -> Result<bool, RelayError> {
        let mut relays = self
            .relays
            .lock()
            .map_err(|_| RelayError::Poisoned("relay registry"))?;
        let Some(relay) = relays.get(scope).cloned() else {
            return Ok(false);
        };
        relay.inner.shutdown()?;
        relays.remove(scope);
        Ok(true)
    }
}

#[derive(Debug, Error)]
pub enum RelayError {
    #[error("invalid native relay ABI range {minimum}..={maximum}")]
    InvalidAbiRange { minimum: u16, maximum: u16 },
    #[error(
        "native relay ABI {native} is incompatible with wrapper range {minimum}..={maximum}; a new native development/release build is required"
    )]
    IncompatibleAbi {
        native: u16,
        minimum: u16,
        maximum: u16,
    },
    #[error("native relay peer message exceeds the logical-message limit: {0}")]
    PeerMessageTooLarge(String),
    #[error("failed to decode native relay peer message: {0}")]
    DecodePeerMessage(postcard::Error),
    #[error("failed to encode native relay peer message: {0}")]
    EncodePeerMessage(postcard::Error),
    #[error(
        "native relay {direction} queue capacity exceeded ({queued_messages} messages, {queued_bytes} encoded bytes); drain or pump before retrying"
    )]
    QueueCapacityExceeded {
        direction: &'static str,
        queued_messages: usize,
        queued_bytes: usize,
    },
    #[error("invalid native relay scope: {0}")]
    InvalidScope(String),
    #[error("failed to open native relay owner thread: {0}")]
    OwnerThread(String),
    #[error("native relay host entropy failed: {0}")]
    Entropy(String),
    #[error("native relay is closed")]
    Closed,
    #[error("native relay owner command queue is full; retry after the next scheduled tick")]
    OwnerQueueFull,
    #[error("native relay internal mutex poisoned: {0}")]
    Poisoned(&'static str),
    #[error("native relay does not know UI client {0}")]
    UnknownClient(u64),
    #[error("a native relay scope is already open with a different storage path or schema")]
    ScopeConfigurationMismatch,
    #[error(
        "native account storage is still owned by another live scope; close it before switching identities"
    )]
    StorageAlreadyOwned,
    #[error("native relay UI client id space exhausted")]
    ClientIdExhausted,
    #[error("SQLite storage failed: {0}")]
    Storage(jazz::groove::storage::Error),
    #[error("Jazz database failed: {0}")]
    Db(jazz::db::Error),
    #[error("foreground NativeDb command failed: {0}")]
    ForegroundCommand(String),
}

#[cfg(test)]
mod tests {
    mod publication_codec;

    // This is intentionally an internal transport-ownership test: the public
    // user-visible behavior of rows/subscriptions belongs to the Db suites.
    // Here we prove the native host does not accidentally create one durable
    // store per UI runtime or share it across explicit auth scopes.
    use super::*;
    use jazz::db::{InsertOptions, Transport, WireTransportAdapter};
    use jazz::groove::records::{BorrowedRecord, RecordDescriptor, ValueType};
    use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
    use jazz::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES;
    use jazz::time::TxTime;
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
    use jazz::tx::TxId;
    use jazz_server::{EdgeUpstreamHealth, JazzServer, TestJwtIssuer};
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    #[derive(Default)]
    struct TestWireTransport {
        inbound: VecDeque<Vec<u8>>,
        outbound: Vec<Vec<u8>>,
    }

    impl WireTransport for TestWireTransport {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            self.outbound.push(frame);
            Ok(())
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.inbound.pop_front()
        }
    }

    struct IdleWire;

    impl WireTransport for IdleWire {
        fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
            Ok(())
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            None
        }
    }

    struct ReconnectingTestConnector {
        calls: AtomicUsize,
        bearer_seen: Arc<Mutex<Vec<String>>>,
    }

    impl NativeTransportConnector for ReconnectingTestConnector {
        fn connect(
            &self,
            request: NativeTransportRequest,
        ) -> jazz::tools::native_transport_connector::NativeTransportFuture {
            self.bearer_seen.lock().unwrap().push(
                request
                    .auth
                    .jwt_token
                    .expect("worker supplies bearer only to connector"),
            );
            let call = self.calls.fetch_add(1, Ordering::AcqRel);
            Box::pin(async move {
                Ok(
                    jazz::tools::native_transport_connector::ConnectedNativeTransport {
                        transport: Box::new(IdleWire),
                        protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                        features: jazz::wire::current_wire_features(),
                        session_context: None,
                        permits_delegated_sessions: false,
                        terminal: if call == 0 {
                            Box::pin(async {
                                jazz::tools::native_transport_connector::NativeTransportTerminal::PeerClosed(
                                "test close".to_owned(),
                            )
                            })
                        } else {
                            Box::pin(std::future::pending())
                        },
                    },
                )
            })
        }

        fn bootstrap_catalogue(
            &self,
            _request: NativeTransportRequest,
        ) -> jazz::tools::native_transport_connector::NativeCatalogueBootstrapFuture {
            Box::pin(async { panic!("ordinary relay socket must not bootstrap as Edge") })
        }
    }

    struct ClosedPumpWire;

    impl WireTransport for ClosedPumpWire {
        fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
            // This is the private WebSocketTransport signal that its outbound
            // pump has already selected a terminal result.
            Err(TransportError::Failed(
                "websocket pump is closed".to_owned(),
            ))
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            None
        }
    }

    struct ClosedPumpTestConnector {
        calls: AtomicUsize,
        first_terminal: Arc<Mutex<Option<tokio::sync::oneshot::Sender<NativeTransportTerminal>>>>,
    }

    impl NativeTransportConnector for ClosedPumpTestConnector {
        fn connect(
            &self,
            _request: NativeTransportRequest,
        ) -> jazz::tools::native_transport_connector::NativeTransportFuture {
            let call = self.calls.fetch_add(1, Ordering::AcqRel);
            let first_terminal = Arc::clone(&self.first_terminal);
            Box::pin(async move {
                let terminal: jazz::tools::native_transport_connector::NativeTransportTerminalFuture =
                    if call == 0 {
                        let (sender, receiver) = tokio::sync::oneshot::channel();
                        *first_terminal.lock().unwrap() = Some(sender);
                        Box::pin(async move {
                        receiver.await.unwrap_or(NativeTransportTerminal::OwnerDropped)
                    })
                    } else {
                        Box::pin(std::future::pending())
                    };
                Ok(
                    jazz::tools::native_transport_connector::ConnectedNativeTransport {
                        transport: if call == 0 {
                            Box::new(ClosedPumpWire)
                        } else {
                            Box::new(IdleWire)
                        },
                        protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                        features: jazz::wire::current_wire_features(),
                        session_context: None,
                        permits_delegated_sessions: false,
                        terminal,
                    },
                )
            })
        }

        fn bootstrap_catalogue(
            &self,
            _request: NativeTransportRequest,
        ) -> jazz::tools::native_transport_connector::NativeCatalogueBootstrapFuture {
            Box::pin(async { panic!("ordinary relay socket must not bootstrap as Edge") })
        }
    }

    struct PendingTestConnector {
        calls: AtomicUsize,
    }

    impl NativeTransportConnector for PendingTestConnector {
        fn connect(
            &self,
            _request: NativeTransportRequest,
        ) -> jazz::tools::native_transport_connector::NativeTransportFuture {
            self.calls.fetch_add(1, Ordering::AcqRel);
            Box::pin(async {
                Ok(
                    jazz::tools::native_transport_connector::ConnectedNativeTransport {
                        transport: Box::new(IdleWire),
                        protocol_version: jazz::wire::WIRE_PROTOCOL_VERSION,
                        features: jazz::wire::current_wire_features(),
                        session_context: None,
                        permits_delegated_sessions: false,
                        terminal: Box::pin(std::future::pending()),
                    },
                )
            })
        }

        fn bootstrap_catalogue(
            &self,
            _request: NativeTransportRequest,
        ) -> jazz::tools::native_transport_connector::NativeCatalogueBootstrapFuture {
            Box::pin(async { panic!("ordinary relay socket must not bootstrap as Edge") })
        }
    }

    fn schema() -> JazzSchema {
        JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("todos").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap()
    }

    fn permissive_schema() -> JazzSchema {
        let allow = PolicyExpr::True;
        let policies = TablePolicies::new()
            .with_select(allow.clone())
            .with_insert(allow.clone())
            .with_update(Some(allow.clone()), allow.clone())
            .with_delete(allow);
        JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("todos")
                        .column("title", ColumnType::Text)
                        .policies(policies),
                )
                .build(),
        )
        .unwrap()
    }

    #[test]
    fn canceled_private_session_cannot_attach_after_account_handoff() {
        // This tests the private pre-context capability boundary: there is no
        // public database yet through which cancellation could be observed.
        use base64::Engine;
        let root = tempfile::tempdir().unwrap();
        let mut host = NativeRelayHost::default();
        let setup = host
            .begin_private_session(PrivateSessionSetupJson {
                server_url: "https://edge.example".into(),
                app_id: "cancel-account-setup".into(),
                jwt: format!(
                    "x.{}.x",
                    base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .encode(br#"{"iss":"https://issuer.example","sub":"alice"}"#)
                ),
                storage_root: root.path().display().to_string(),
            })
            .unwrap();
        assert!(host.revoke_scope(setup).unwrap());
        assert!(!host.revoke_scope(setup).unwrap(), "release is idempotent");
        assert_eq!(
            host.attach_canonical_schema(
                setup,
                &serde_json::to_string(schema().public_schema()).unwrap()
            ),
            Err(JazzNativeRelayStatus::InvalidHandle)
        );
        assert!(host.pending_private_sessions.is_empty());
        assert!(host.admitted_scopes.is_empty());
    }

    #[test]
    fn private_session_setup_partitions_before_credential_free_schema_attachment() {
        use base64::Engine;
        let root = tempfile::tempdir().unwrap();
        let jwt = format!(
            "x.{}.x",
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(br#"{"iss":"https://issuer.example","sub":"alice"}"#)
        );
        let mut host = NativeRelayHost::default();
        let setup = host
            .begin_private_session(PrivateSessionSetupJson {
                server_url: "HTTPS://EDGE.example:443/sync?ignored=yes".to_owned(),
                app_id: "app-a".to_owned(),
                jwt,
                storage_root: root.path().display().to_string(),
            })
            .unwrap();
        let pending = host.pending_private_sessions.get(&setup).unwrap();
        assert_eq!(pending.scope.app_namespace, "https://edge.example");
        assert_eq!(pending.scope.storage_namespace, "app-a");
        assert_eq!(
            pending.scope.auth_scope.as_deref(),
            Some(r#"["https://issuer.example","alice"]"#)
        );
        assert_eq!(
            host.attach_canonical_schema(setup, "not-json"),
            Err(JazzNativeRelayStatus::LifecycleFailure)
        );
        assert_eq!(
            host.attach_canonical_schema(setup, "{}"),
            Err(JazzNativeRelayStatus::InvalidHandle),
            "a malformed schema consumes the setup capability"
        );
        assert!(host.admitted_scopes.is_empty());
        let socket_setup = host
            .begin_private_session(PrivateSessionSetupJson {
                server_url: "https://edge.example".to_owned(),
                app_id: "app-a".to_owned(),
                jwt: format!(
                    "x.{}.x",
                    base64::engine::general_purpose::URL_SAFE_NO_PAD
                        .encode(br#"{"iss":"https://issuer.example","sub":"alice"}"#)
                ),
                storage_root: root.path().display().to_string(),
            })
            .unwrap();
        let admitted = host
            .attach_canonical_schema(
                socket_setup,
                &serde_json::to_string(schema().public_schema()).unwrap(),
            )
            .unwrap();
        assert!(host.admitted_scopes[&admitted].claims.is_empty());
        assert_eq!(
            host.private_socket_sessions[&admitted].bearer,
            "x.eyJpc3MiOiJodHRwczovL2lzc3Vlci5leGFtcGxlIiwic3ViIjoiYWxpY2UifQ.x"
        );
        assert!(host.revoke_scope(admitted).unwrap());
        assert!(!host.private_socket_sessions.contains_key(&admitted));
        assert!(validate_private_session_endpoint("http://edge.example").is_err());
        assert!(validate_private_session_endpoint("http://127.0.0.1:9876").is_ok());
        assert!(validate_private_session_endpoint("http://[::1]:9876").is_ok());
        assert!(validate_private_session_endpoint("http://10.0.2.2:9876").is_ok());
        assert!(validate_private_session_endpoint("https://edge.example").is_ok());
        assert!(
            host.begin_private_session(PrivateSessionSetupJson {
                server_url: "ftp://edge.example".to_owned(),
                app_id: "app-a".to_owned(),
                jwt: "x.e30.x".to_owned(),
                storage_root: root.path().display().to_string(),
            })
            .is_err()
        );
    }

    /// Two foreground roots sharing Alice's admitted scope must attach to one
    /// durable relay and one bearer socket worker, rather than racing two
    /// independent upstream bridges against the same SQLite store.
    #[test]
    fn private_scope_owns_one_socket_worker_across_foreground_leases() {
        use base64::Engine;
        let root = tempfile::tempdir().unwrap();
        let jwt = format!(
            "x.{}.x",
            base64::engine::general_purpose::URL_SAFE_NO_PAD
                .encode(br#"{"iss":"https://issuer.example","sub":"alice"}"#)
        );
        let mut host = NativeRelayHost::default();
        let pending = host
            .begin_private_session(PrivateSessionSetupJson {
                server_url: "http://127.0.0.1:9".to_owned(),
                app_id: "one-worker-per-scope".to_owned(),
                jwt,
                storage_root: root.path().display().to_string(),
            })
            .unwrap();
        let admitted = host
            .attach_canonical_schema(
                pending,
                &serde_json::to_string(schema().public_schema()).unwrap(),
            )
            .unwrap();
        let first = host
            .open_foreground(admitted, DIRECT_FOREGROUND_RUNTIME_TOKEN)
            .unwrap();
        let second = host
            .open_foreground(admitted, DIRECT_FOREGROUND_RUNTIME_TOKEN)
            .unwrap();
        assert_ne!(first, second);
        assert_eq!(host.private_scope_workers.len(), 1);
        assert_eq!(host.foregrounds.len(), 2);
        assert!(host.revoke_scope(admitted).unwrap());
        assert!(host.private_scope_workers.is_empty());
        assert!(host.foregrounds.is_empty());
    }

    fn encoded_cells(descriptor: RecordDescriptor, raw: Vec<u8>) -> Vec<u8> {
        jazz::binding_codec::encode_named_cells(&jazz::groove::records::OwnedRecord::new(
            raw, descriptor,
        ))
        .expect("fixture cells encode")
    }

    fn encoded_title_cells(title: &str) -> Vec<u8> {
        let descriptor = RecordDescriptor::new([("title", ValueType::String)]);
        let raw = descriptor
            .create(&[Value::String(title.to_owned())])
            .expect("fixture title record is valid");
        encoded_cells(descriptor, raw)
    }

    #[derive(serde::Deserialize)]
    struct DecodedForegroundRowBatch {
        table: String,
        #[serde(deserialize_with = "publication_codec::descriptor")]
        descriptor: RecordDescriptor,
        rows: Vec<DecodedForegroundRow>,
    }

    #[derive(serde::Deserialize)]
    struct DecodedForegroundRow {
        row_id: RowUuid,
        deleted: bool,
        raw: Vec<u8>,
    }

    fn assert_exact_todo_rows(rows: &[u8], row_id: RowUuid, title: &str) {
        let batches = postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(rows)
            .expect("foreground row bytes use the shared binding row-batch codec");
        assert_eq!(
            batches.len(),
            1,
            "the query returns one contiguous todo batch"
        );
        let batch = &batches[0];
        assert_eq!(batch.table, "todos");
        assert_eq!(
            batch.rows.len(),
            1,
            "the query returns exactly the seeded row"
        );
        let row = &batch.rows[0];
        assert_eq!(
            row.row_id, row_id,
            "the exact row identity survives relay delivery"
        );
        assert!(!row.deleted, "the persisted row remains live");
        let record = BorrowedRecord::new(&row.raw, &batch.descriptor);
        let values = record
            .to_values()
            .expect("the row decodes through its binding descriptor");
        assert_eq!(
            values.first(),
            Some(&Value::Uuid(row_id.0)),
            "the physical row identity is exact"
        );
        assert_eq!(
            values.get(1),
            Some(&Value::Nullable(Some(Box::new(Value::String(
                title.to_owned()
            ))))),
            "the declared logical title is exact"
        );
    }

    /// Test-only owner for the same C ABI that Kotlin and Swift use.  It is
    /// deliberately confined to this `#[cfg(test)]` module: a device fixture
    /// needs to prove process close/reopen, but production bindings must not
    /// expose a generic host-reset or SQLite-teardown operation to JavaScript.
    struct NativeHostAbiFixture {
        accounts: RefCell<BTreeMap<String, jazz::account_registry::AccountId>>,
        host: *mut JazzNativeRelayHost,
        lease: *mut JazzNativeRelayHostLease,
    }

    impl NativeHostAbiFixture {
        fn new() -> Self {
            let host = jazz_native_relay_host_new();
            assert!(!host.is_null(), "native host allocation succeeds");
            let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
            assert!(!lease.is_null(), "native host lease succeeds");
            Self {
                host,
                lease,
                accounts: RefCell::new(BTreeMap::new()),
            }
        }

        fn admit(
            &self,
            sqlite_path: &std::path::Path,
            auth_scope: &str,
            schema: &JazzSchema,
            byte: u8,
        ) -> [u8; 32] {
            let identity = DbIdentity {
                node: NodeUuid::from_bytes([byte; 16]),
                author: AuthorSubject::for_test_bytes([byte.wrapping_add(1); 16]),
            };
            let request = serde_json::json!({
                "scope": {
                    "app_namespace": "native-host-reopen-abi",
                    "storage_namespace": "default",
                    "auth_scope": auth_scope,
                },
                "sqlite_path": sqlite_path.display().to_string(),
                "schema_json": serde_json::to_string(schema.public_schema()).unwrap(),
                "identity": serde_json::to_value(identity).unwrap(),
                "claims": {},
            });
            let request = serde_json::to_vec(&request).unwrap();
            let mut output = JazzNativeRelayBytes::EMPTY;
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_admit_scope_json(
                        self.host,
                        request.as_ptr(),
                        request.len(),
                        &mut output,
                    )
                },
                JazzNativeRelayStatus::Ok,
                "trusted native admission succeeds"
            );
            let capability = unsafe { std::slice::from_raw_parts(output.data, output.len) };
            assert_eq!(
                capability.len(),
                32,
                "admission returns only one opaque capability"
            );
            let mut bytes = [0; 32];
            bytes.copy_from_slice(capability);
            unsafe { jazz_native_relay_bytes_free(&mut output) };
            bytes
        }

        /// Exercise the private platform session handoff used by Android and
        /// iOS. Unlike `admit`, neither the endpoint nor the bearer crosses
        /// the generic relay command ABI.
        async fn begin_account_session(
            &self,
            server_url: &str,
            app_id: &str,
            bearer: &str,
            storage_root: &std::path::Path,
            schema: &JazzSchema,
        ) -> [u8; 32] {
            let registry = format!(
                "{}/apps/{app_id}/accounts",
                server_url.trim_end_matches('/')
            );
            let (issuer, subject) = jazz::tools::unverified_jwt_scope_subject(bearer).unwrap();
            let key = serde_json::to_string(&(&registry, &issuer, &subject)).unwrap();
            // Retain the enrolled handle across offline reopening; authentication
            // and registration happen before context construction, not inside it.
            let retained = self.accounts.borrow().get(&key).copied();
            let account = if let Some(account) = retained {
                account
            } else {
                let response = reqwest::Client::new()
                    .post(format!("{registry}/register"))
                    .bearer_auth(bearer)
                    .send()
                    .await
                    .unwrap();
                assert!(
                    response.status().is_success(),
                    "account registration: {}",
                    response.status()
                );
                let response: serde_json::Value = response.json().await.unwrap();
                let account = serde_json::from_value(response["account"].clone()).unwrap();
                self.accounts.borrow_mut().insert(key, account);
                account
            };
            let request = serde_json::json!({
                "server_url": server_url, "registry": registry,
                "app_id": app_id, "env": "test", "account_id": account,
                "issuer": issuer, "subject": subject, "jwt": bearer,
            });
            let root = storage_root.to_str().unwrap().as_bytes();
            let request = serde_json::to_vec(&request).expect("private session JSON encodes");
            let mut setup = JazzNativeRelayBytes::EMPTY;
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_begin_account_session_json(
                        self.lease,
                        request.as_ptr(),
                        request.len(),
                        root.as_ptr(),
                        root.len(),
                        &mut setup,
                    )
                },
                JazzNativeRelayStatus::Ok,
                "private endpoint and ephemeral bearer are accepted only at the native boundary"
            );
            let setup = take_capability(&mut setup);
            let schema =
                serde_json::to_string(schema.public_schema()).expect("schema JSON encodes");
            let mut admitted = JazzNativeRelayBytes::EMPTY;
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_attach_account_schema_json(
                        self.lease,
                        setup.as_ptr(),
                        setup.len(),
                        schema.as_ptr(),
                        schema.len(),
                        &mut admitted,
                    )
                },
                JazzNativeRelayStatus::Ok,
                "credential-free canonical schema attachment admits the private session"
            );
            take_capability(&mut admitted)
        }

        fn revoke_private_session(&self, capability: &[u8; 32]) {
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_revoke_scope_capability(
                        self.host,
                        capability.as_ptr(),
                        capability.len(),
                    )
                },
                JazzNativeRelayStatus::Ok,
                "trusted revocation stops the native relay and its socket worker"
            );
        }

        fn open_foreground(&self, capability: &[u8; 32]) -> u64 {
            let (status, foreground) = self.try_open_foreground(capability);
            assert_eq!(
                status,
                JazzNativeRelayStatus::Ok,
                "C ABI opens one admitted foreground"
            );
            assert_ne!(foreground, 0);
            foreground
        }

        fn try_open_foreground(&self, capability: &[u8; 32]) -> (JazzNativeRelayStatus, u64) {
            let mut foreground = 0;
            let status = unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    self.lease,
                    capability.as_ptr(),
                    capability.len(),
                    &mut foreground,
                )
            };
            (status, foreground)
        }

        fn execute(
            &self,
            foreground: u64,
            command: ForegroundDbCommandRequest,
        ) -> ForegroundDbCommandResponse {
            let request = postcard::to_allocvec(&command).unwrap();
            let mut output = JazzNativeRelayBytes::EMPTY;
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_execute_foreground(
                        self.lease,
                        foreground,
                        request.as_ptr(),
                        request.len(),
                        &mut output,
                    )
                },
                JazzNativeRelayStatus::Ok,
                "foreground command succeeds through the native C ABI"
            );
            let response = postcard::from_bytes(unsafe {
                std::slice::from_raw_parts(output.data, output.len)
            })
            .expect("native C ABI returns one canonical foreground response");
            unsafe { jazz_native_relay_bytes_free(&mut output) };
            response
        }

        fn tick(&self, foreground: u64) {
            assert_eq!(
                self.tick_status(foreground),
                JazzNativeRelayStatus::Ok,
                "native relay tick succeeds"
            );
        }

        fn tick_status(&self, foreground: u64) -> JazzNativeRelayStatus {
            unsafe { jazz_native_relay_host_lease_tick_attached_foreground(self.lease, foreground) }
        }

        fn insert_todo(&self, foreground: u64, row_id: [u8; 16], title: &str) -> [u8; 16] {
            let ForegroundDbCommandResponse::TransactionOpened { transaction } = self.execute(
                foreground,
                ForegroundDbCommandRequest::BeginTransaction {
                    kind: ForegroundTransactionKind::Mergeable,
                },
            ) else {
                panic!("foreground transaction must open");
            };
            assert_eq!(
                self.execute(
                    foreground,
                    ForegroundDbCommandRequest::Insert {
                        transaction,
                        table: "todos".to_owned(),
                        cells: encoded_title_cells(title),
                        row_id: Some(row_id),
                    },
                ),
                ForegroundDbCommandResponse::Inserted { row_id }
            );
            let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = self.execute(
                foreground,
                ForegroundDbCommandRequest::CommitTransaction { transaction },
            ) else {
                panic!("foreground transaction must commit");
            };
            assert_ne!(tx_id, [0; 16]);
            tx_id
        }

        async fn wait_for_core_transaction(&self, foreground: u64, tx_id: [u8; 16]) {
            let mut operation = match self.execute(
                foreground,
                ForegroundDbCommandRequest::WaitForCoreTransaction { tx_id },
            ) {
                ForegroundDbCommandResponse::Pending { operation } => operation,
                ForegroundDbCommandResponse::TransactionSettled { tx_id: settled } => {
                    assert_eq!(settled, tx_id);
                    return;
                }
                response => panic!("Core settlement did not start: {response:?}"),
            };
            for _ in 0..600 {
                self.tick(foreground);
                match self.execute(foreground, ForegroundDbCommandRequest::Poll { operation }) {
                    ForegroundDbCommandResponse::Pending { operation: pending } => {
                        operation = pending;
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    ForegroundDbCommandResponse::TransactionSettled { tx_id: settled } => {
                        assert_eq!(
                            settled, tx_id,
                            "Core settles the same foreground transaction"
                        );
                        return;
                    }
                    ForegroundDbCommandResponse::OperationError { reason } => {
                        panic!("Core rejected native foreground transaction: {reason}")
                    }
                    response => {
                        panic!("Core settlement returned unexpected response: {response:?}")
                    }
                }
            }
            panic!("timed out waiting for native foreground transaction to reach Core");
        }

        fn rows_after_sync(&self, foreground: u64) -> Vec<u8> {
            // Local-first reads are allowed to finish with their current
            // local knowledge. Drive the ordinary relay loop before starting
            // the read, rather than treating an initial empty local snapshot
            // as evidence that replication completed.
            for _ in 0..64 {
                self.tick(foreground);
            }
            let query = postcard::to_allocvec(&Query::from("todos")).unwrap();

            let ForegroundDbCommandResponse::Subscribed { .. } = self.execute(
                foreground,
                ForegroundDbCommandRequest::Subscribe {
                    query: query.clone(),
                    options_json: "{}".into(),
                },
            ) else {
                panic!("foreground subscription preparation must return a handle");
            };

            // Keep the same asynchronous read until it completes. Starting a
            // second All after a Pending reply leaks the first observation and
            // can exhaust the intentionally bounded native operation capacity.
            let mut request = ForegroundDbCommandRequest::All {
                query,
                options_json: "{}".into(),
                transaction: None,
            };
            for _ in 0..120 {
                self.tick(foreground);
                std::thread::sleep(Duration::from_millis(25));
                match self.execute(foreground, request) {
                    ForegroundDbCommandResponse::Rows { rows } => return rows,
                    ForegroundDbCommandResponse::Pending { operation } => {
                        request = ForegroundDbCommandRequest::Poll { operation };
                    }
                    response => {
                        panic!("foreground All returned an unexpected response: {response:?}")
                    }
                }
            }
            panic!("foreground read did not settle after bounded native relay ticks");
        }
    }

    fn take_capability(output: &mut JazzNativeRelayBytes) -> [u8; 32] {
        let bytes = unsafe { std::slice::from_raw_parts(output.data, output.len) };
        assert_eq!(
            bytes.len(),
            32,
            "native private admission returns one opaque capability"
        );
        let mut capability = [0; 32];
        capability.copy_from_slice(bytes);
        unsafe { jazz_native_relay_bytes_free(output) };
        capability
    }

    async fn wait_for_persisted_todo(
        fixture: &NativeHostAbiFixture,
        foreground: u64,
        row_id: [u8; 16],
        title: &str,
        stage: &str,
    ) {
        for _ in 0..120 {
            let rows = fixture.rows_after_sync(foreground);
            if !postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&rows)
                .expect("foreground row bytes decode")
                .is_empty()
            {
                assert_exact_todo_rows(&rows, RowUuid::from_bytes(row_id), title);
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("timed out waiting for {stage} to materialize from the persistent native relay");
    }

    /// A real Core and Edge authenticate a native private-session bearer while
    /// Alice writes through a foreground relay. Revoking that admission stops
    /// its socket/relay; a fresh worker then reopens the same SQLite partition
    /// and reads Alice's row back through the ordinary foreground protocol.
    ///
    /// ```text
    /// alice foreground ──peer──► native SQLite relay ──JWT WebSocket──► Edge ──upstream──► Core
    ///       │                         │
    ///       └──write, close/revoke────┴──new worker/relay──readback──► persisted row
    /// ```
    ///
    /// This host-only receipt is deliberately continuously driven from a
    /// multithread Tokio harness. It proves topology and lifecycle readiness;
    /// an Android emulator/device run remains a separate installed-artifact
    /// acceptance receipt.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn private_session_edge_core_write_survives_worker_and_relay_restart() {
        private_session_restart_receipt(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn private_session_edge_core_write_survives_offline_relay_restart() {
        private_session_restart_receipt(true).await;
    }

    /// Advice uses the real negotiated socket authority through the C ABI.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn private_session_permission_advice_uses_real_socket_authority() {
        let issuer = TestJwtIssuer::start().await;
        let schema = permissive_schema();
        let server = JazzServer::builder()
            .with_schema(schema.public_schema().clone())
            .with_jwks_url(issuer.endpoint())
            .with_native_transport_connector(jazz_testkit::native_connector())
            .start()
            .await;
        let storage = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let bearer = TestJwtIssuer::jwt_for_user("native-advice-alice");
        let admitted = fixture
            .begin_account_session(
                &server.base_url(),
                &server.app_id().to_string(),
                &bearer,
                storage.path(),
                &schema,
            )
            .await;
        let foreground = fixture.open_foreground(&admitted);
        let client = unsafe { &*fixture.host }
            .inner
            .lock()
            .unwrap()
            .foreground_client(foreground)
            .unwrap()
            .clone();
        jazz_testkit::wait_for(Duration::from_secs(5), "real upstream installed", || {
            let installed = client
                .relay
                .run(|worker| {
                    Ok(worker.upstream_attached
                        && worker.socket_wire.is_some()
                        && worker.upstream_transition.is_none())
                })
                .unwrap();
            async move { installed.then_some(()) }
        })
        .await;
        let response = fixture.execute(
            foreground,
            ForegroundDbCommandRequest::PermissionAdvice {
                action: ForegroundPermissionAdviceAction::Insert {
                    table: "todos".into(),
                    cells: encoded_title_cells("dry run"),
                },
            },
        );
        let ForegroundDbCommandResponse::Pending { operation } = response else {
            panic!("advice pending: {response:?}");
        };
        let advice = jazz_testkit::wait_for(Duration::from_secs(5), "authoritative advice", || {
            fixture.tick(foreground);
            let response =
                fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
            async move {
                match response {
                    ForegroundDbCommandResponse::Pending { .. } => None,
                    response => Some(response),
                }
            }
        })
        .await;
        assert!(
            matches!(
                advice,
                ForegroundDbCommandResponse::PermissionAdvice {
                    advice: ForegroundPermissionAdvice::Allowed
                }
            ),
            "{advice:?}"
        );
        fixture.revoke_private_session(&admitted);
        assert_eq!(
            server.shutdown().await,
            jazz_server::ShutdownPhase::StorageClosed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn recovered_sibling_cannot_replace_live_exclusive_upload() {
        let issuer = TestJwtIssuer::start().await;
        let schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("todos")
                        .column("title", ColumnType::Text)
                        .column("done", ColumnType::Boolean)
                        .policies(
                            TablePolicies::new()
                                .with_select(PolicyExpr::True)
                                .with_insert(PolicyExpr::True)
                                .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                        ),
                )
                .build(),
        )
        .unwrap();
        let server = JazzServer::builder()
            .with_schema(schema.public_schema().clone())
            .with_jwks_url(issuer.endpoint())
            .with_native_transport_connector(jazz_testkit::native_connector())
            .start()
            .await;
        let storage = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let bearer = TestJwtIssuer::jwt_for_user("native-advice-alice");
        let admitted = fixture
            .begin_account_session(
                &server.base_url(),
                &server.app_id().to_string(),
                &bearer,
                storage.path(),
                &schema,
            )
            .await;
        let foreground = fixture.open_foreground(&admitted);
        let client = unsafe { &*fixture.host }
            .inner
            .lock()
            .unwrap()
            .foreground_client(foreground)
            .unwrap()
            .clone();
        jazz_testkit::wait_for(Duration::from_secs(5), "real upstream installed", || {
            let installed = client
                .relay
                .run(|worker| {
                    Ok(worker.upstream_attached
                        && worker.socket_wire.is_some()
                        && worker.upstream_transition.is_none())
                })
                .unwrap();
            async move { installed.then_some(()) }
        })
        .await;
        let row_id = [0x61; 16];
        let descriptor =
            RecordDescriptor::new([("title", ValueType::String), ("done", ValueType::Bool)]);
        let now_ms = || {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64
        };
        let raw = descriptor
            .create(&[Value::String("before".into()), Value::Bool(false)])
            .unwrap();
        let ForegroundDbCommandResponse::MutationCommitted {
            tx_id: inserted, ..
        } = fixture.execute(
            foreground,
            ForegroundDbCommandRequest::DirectMutation {
                mutation: ForegroundMutationKind::Insert,
                table: "todos".into(),
                row_id: Some(row_id),
                cells: encoded_cells(descriptor, raw),
                options_json: format!(r#"{{"updatedAtMs":{}}}"#, now_ms()),
            },
        )
        else {
            panic!("direct insertion queues");
        };
        let mut initial_wait = fixture.execute(
            foreground,
            ForegroundDbCommandRequest::WaitForTransaction {
                tx_id: inserted,
                tier: "global".into(),
            },
        );
        for _ in 0..10000 {
            let ForegroundDbCommandResponse::Pending { operation } = initial_wait else {
                break;
            };
            fixture.tick(foreground);
            tokio::task::yield_now().await;
            initial_wait =
                fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
        }
        assert!(
            matches!(
                initial_wait,
                ForegroundDbCommandResponse::TransactionSettled { .. }
            ),
            "{initial_wait:?}"
        );
        assert_eq!(
            postcard::to_allocvec(&Query::from("todos").limit(1)).unwrap(),
            vec![
                5, 116, 111, 100, 111, 115, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0
            ]
        );
        let mut remote = fixture.execute(
            foreground,
            ForegroundDbCommandRequest::All {
                query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
                options_json: r#"{"tier":"edge","local_updates":"deferred"}"#.into(),
                transaction: None,
            },
        );
        for _ in 0..1000 {
            let ForegroundDbCommandResponse::Pending { operation } = remote else {
                break;
            };
            fixture.tick(foreground);
            tokio::time::sleep(Duration::from_millis(1)).await;
            remote = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
        }
        assert!(
            matches!(remote, ForegroundDbCommandResponse::Rows { .. }),
            "{remote:?}"
        );
        fixture.execute(
            foreground,
            ForegroundDbCommandRequest::DisconnectNativeUpstream,
        );
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = fixture.execute(
            foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Exclusive,
            },
        ) else {
            panic!("exclusive transaction opens");
        };
        assert_eq!(
            fixture.execute(
                foreground,
                ForegroundDbCommandRequest::StageMutation {
                    transaction,
                    mutation: ForegroundMutationKind::Update,
                    table: "todos".into(),
                    row_id: Some(row_id),
                    cells: encoded_title_cells("accepted"),
                    options_json: format!(r#"{{"updatedAtMs":{}}}"#, now_ms()),
                }
            ),
            ForegroundDbCommandResponse::MutationStaged
        );
        let mut read = fixture.execute(
            foreground,
            ForegroundDbCommandRequest::All {
                query: postcard::to_allocvec(&Query::from("todos").limit(1)).unwrap(),
                options_json: r#"{"tier":"local","local_updates":"deferred"}"#.into(),
                transaction: Some(transaction),
            },
        );
        for _ in 0..200 {
            let ForegroundDbCommandResponse::Pending { operation } = read else {
                break;
            };
            fixture.tick(foreground);
            tokio::time::sleep(Duration::from_millis(1)).await;
            read = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
        }
        assert!(
            matches!(read, ForegroundDbCommandResponse::Rows { .. }),
            "{read:?}"
        );
        let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = fixture.execute(
            foreground,
            ForegroundDbCommandRequest::CommitTransaction { transaction },
        ) else {
            panic!("exclusive commit queues");
        };
        let ForegroundDbCommandResponse::Pending { operation } = fixture.execute(
            foreground,
            ForegroundDbCommandRequest::WaitForTransaction {
                tx_id,
                tier: "global".into(),
            },
        ) else {
            panic!("offline acceptance waits");
        };
        for _ in 0..100 {
            fixture.tick(foreground);
            assert!(matches!(
                fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation }),
                ForegroundDbCommandResponse::Pending { .. }
            ));
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Reopening a sibling recovers pending local history before reconnect.
        // That recovery must not replace the live exclusive commit's evidence.
        let _sibling = fixture.open_foreground(&admitted);
        fixture.execute(
            foreground,
            ForegroundDbCommandRequest::ReconnectNativeUpstream,
        );
        let mut settled = ForegroundDbCommandResponse::Pending { operation };
        for _ in 0..1000 {
            fixture.tick(foreground);
            settled = fixture.execute(foreground, ForegroundDbCommandRequest::Poll { operation });
            if !matches!(settled, ForegroundDbCommandResponse::Pending { .. }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(
            settled,
            ForegroundDbCommandResponse::TransactionSettled { tx_id }
        );
        fixture.revoke_private_session(&admitted);
        assert_eq!(
            server.shutdown().await,
            jazz_server::ShutdownPhase::StorageClosed
        );
    }

    /// The C ABI must surface an actual Edge authentication denial, even though
    /// a disconnected peer no longer disables local SQLite work.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn private_session_invalid_bearer_still_fails_closed() {
        let issuer = TestJwtIssuer::start().await;
        let schema = schema();
        let edge = JazzServer::builder()
            .with_schema(schema.public_schema().clone())
            .with_jwks_url(issuer.endpoint())
            .with_native_transport_connector(jazz_testkit::native_connector())
            .start()
            .await;
        let storage = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let mut bearer = TestJwtIssuer::jwt_for_user("native-private-alice");
        let initial = fixture
            .begin_account_session(
                &edge.base_url(),
                &edge.app_id().to_string(),
                &bearer,
                storage.path(),
                &schema,
            )
            .await;
        fixture.revoke_private_session(&initial);
        // Retain the enrolled account, then corrupt only its transport proof.
        // The native socket must reject it even though registration succeeded.
        let signature = bearer.rfind('.').unwrap() + 1;
        let replacement = if &bearer[signature..signature + 1] == "A" {
            "B"
        } else {
            "A"
        };
        bearer.replace_range(signature..signature + 1, replacement);
        let admitted = fixture
            .begin_account_session(
                &edge.base_url(),
                &edge.app_id().to_string(),
                &bearer,
                storage.path(),
                &schema,
            )
            .await;
        let foreground = fixture.open_foreground(&admitted);
        jazz_testkit::wait_for(
            Duration::from_secs(5),
            "invalid signature fails the native foreground closed",
            || {
                let denied =
                    fixture.tick_status(foreground) == JazzNativeRelayStatus::LifecycleFailure;
                async move { denied.then_some(()) }
            },
        )
        .await;
        fixture.revoke_private_session(&admitted);
        assert_eq!(
            edge.shutdown().await,
            jazz_server::ShutdownPhase::StorageClosed
        );
    }

    async fn private_session_restart_receipt(offline: bool) {
        let issuer = TestJwtIssuer::start().await;
        let schema = schema();
        let public_schema = schema.public_schema().clone();
        let core = JazzServer::builder()
            .with_schema(public_schema.clone())
            .with_jwks_url(issuer.endpoint())
            .with_native_transport_connector(jazz_testkit::native_connector())
            .start()
            .await;
        let edge = JazzServer::builder()
            .with_app_id(core.app_id())
            .with_schema(public_schema)
            .with_jwks_url(issuer.endpoint())
            .with_admin_secret(core.admin_secret().to_owned())
            .with_upstream_url(core.base_url())
            .with_native_transport_connector(jazz_testkit::native_connector())
            .start()
            .await;

        jazz_testkit::wait_for(
            Duration::from_secs(15),
            "local Edge attaches its ordinary upstream Core wire",
            || {
                let connected =
                    edge.server_state().edge_upstream_health() == EdgeUpstreamHealth::Connected;
                async move { connected.then_some(()) }
            },
        )
        .await;

        // Mint this bearer at runtime from the local issuer. No bearer or
        // signing material is checked into the relay/device fixture.
        let bearer = TestJwtIssuer::jwt_for_user("native-private-alice");
        let storage = tempfile::tempdir().expect("private relay storage root");
        let fixture = NativeHostAbiFixture::new();
        let admitted = fixture
            .begin_account_session(
                &edge.base_url(),
                &core.app_id().to_string(),
                &bearer,
                storage.path(),
                &schema,
            )
            .await;
        let foreground = fixture.open_foreground(&admitted);

        jazz_testkit::wait_for(
            Duration::from_secs(15),
            "native relay's normal bearer-authenticated Edge websocket",
            || {
                let connected = edge.server_state().shutdown.active_websockets() > 0;
                async move { connected.then_some(()) }
            },
        )
        .await;

        let row_id = [0x3a; 16];
        let committed = fixture.insert_todo(foreground, row_id, "survives native worker restart");
        wait_for_persisted_todo(
            &fixture,
            foreground,
            row_id,
            "survives native worker restart",
            "the initial private foreground write",
        )
        .await;

        fixture
            .wait_for_core_transaction(foreground, committed)
            .await;

        assert_eq!(
            fixture.execute(foreground, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true },
            "the first foreground cleanly hands off before trusted relay revocation"
        );
        fixture.revoke_private_session(&admitted);
        assert_eq!(
            fixture.try_open_foreground(&admitted).0,
            JazzNativeRelayStatus::InvalidHandle,
            "revoked private-session capability cannot restart the old worker"
        );

        let endpoint = edge.base_url();
        let app_id = core.app_id().to_string();
        let mut edge = Some(edge);
        let mut core = Some(core);
        if offline {
            assert_eq!(
                edge.take().unwrap().shutdown().await,
                jazz_server::ShutdownPhase::StorageClosed
            );
            assert_eq!(
                core.take().unwrap().shutdown().await,
                jazz_server::ShutdownPhase::StorageClosed
            );
        }

        let reopened = fixture
            .begin_account_session(&endpoint, &app_id, &bearer, storage.path(), &schema)
            .await;
        let reopened_foreground = fixture.open_foreground(&reopened);
        if !offline {
            jazz_testkit::wait_for(
                Duration::from_secs(15),
                "replacement native worker reconnects through normal Edge auth",
                || {
                    let connected = edge
                        .as_ref()
                        .unwrap()
                        .server_state()
                        .shutdown
                        .active_websockets()
                        > 0;
                    async move { connected.then_some(()) }
                },
            )
            .await;
        }
        wait_for_persisted_todo(
            &fixture,
            reopened_foreground,
            row_id,
            "survives native worker restart",
            "the replacement worker/relay readback",
        )
        .await;

        fixture.revoke_private_session(&reopened);
        if !offline {
            assert_eq!(
                edge.take().unwrap().shutdown().await,
                jazz_server::ShutdownPhase::StorageClosed
            );
            assert_eq!(
                core.take().unwrap().shutdown().await,
                jazz_server::ShutdownPhase::StorageClosed
            );
        }
    }

    impl Drop for NativeHostAbiFixture {
        fn drop(&mut self) {
            // Match platform teardown order: invalidate/release every retained
            // foreground lease before dropping the host itself.
            unsafe { jazz_native_relay_host_lease_free(self.lease) };
            unsafe { jazz_native_relay_host_free(self.host) };
        }
    }

    /// Models the two stages of the platform wake bridge without a JSI
    /// runtime: Rust's owner thread queues a platform task, and the platform
    /// later decides whether that queued task may still deliver. This remains
    /// an internal lifecycle receipt because raw callback-context lifetime is
    /// a native-host concern, not a user-visible Db API.
    #[derive(Default)]
    struct QueuedNativeWake {
        active: AtomicBool,
        cancelled: AtomicUsize,
        callbacks_after_cancel: AtomicUsize,
        queued: Mutex<Vec<(u64, u8, u64)>>,
        callback_ready: Condvar,
        delivered: AtomicUsize,
    }

    impl QueuedNativeWake {
        fn active() -> Self {
            Self {
                active: AtomicBool::new(true),
                ..Self::default()
            }
        }

        fn queued(&self) -> usize {
            self.queued.lock().unwrap().len()
        }

        fn wait_for_queued(&self, expected: usize) -> bool {
            let deadline = std::time::Instant::now() + Duration::from_secs(1);
            let mut queued = self.queued.lock().unwrap();
            while queued.len() < expected {
                let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                if remaining.is_zero() {
                    return false;
                }
                let (next, timeout) = self.callback_ready.wait_timeout(queued, remaining).unwrap();
                queued = next;
                if timeout.timed_out() {
                    return queued.len() >= expected;
                }
            }
            true
        }

        fn assert_no_callback_for(&self, duration: Duration) {
            let queued = self.queued.lock().unwrap();
            let _ = self.callback_ready.wait_timeout(queued, duration).unwrap();
            assert_eq!(self.callbacks_after_cancel.load(Ordering::Acquire), 0);
        }

        /// Run platform turns which were queued before a revoke. Cancellation
        /// must make these harmless instead of delivering a stale result into
        /// a foreground whose owner/context has already been torn down.
        fn deliver_queued(&self) {
            let queued = std::mem::take(&mut *self.queued.lock().unwrap());
            if self.active.load(Ordering::Acquire) {
                self.delivered.fetch_add(queued.len(), Ordering::AcqRel);
            }
        }
    }

    unsafe extern "C" fn queue_native_wake(
        context: *mut c_void,
        foreground: u64,
        kind: u8,
        delay_ms: u64,
    ) {
        let wake = unsafe { &*(context as *const QueuedNativeWake) };
        if kind == FOREGROUND_WAKE_CANCELLED {
            wake.active.store(false, Ordering::Release);
            wake.cancelled.fetch_add(1, Ordering::AcqRel);
            return;
        }
        if wake.active.load(Ordering::Acquire) {
            wake.queued
                .lock()
                .unwrap()
                .push((foreground, kind, delay_ms));
        } else {
            wake.callbacks_after_cancel.fetch_add(1, Ordering::AcqRel);
        }
        wake.callback_ready.notify_all();
    }

    struct SaturatedOwner {
        release: Option<mpsc::Sender<()>>,
        drained: mpsc::Receiver<()>,
    }

    impl SaturatedOwner {
        fn release_and_wait(mut self) {
            self.release
                .take()
                .expect("owner blocker remains held")
                .send(())
                .expect("owner blocker remains alive");
            self.drained
                .recv()
                .expect("owner drains every admitted saturation job");
        }
    }

    impl Drop for SaturatedOwner {
        fn drop(&mut self) {
            // A failed assertion must not leave the relay owner blocked and
            // make fixture shutdown hang indefinitely.
            if let Some(release) = self.release.take() {
                let _ = release.send(());
            }
        }
    }

    /// Deterministically occupy the owner and fill every bounded command slot.
    /// This is an internal queue-failure seam: the public C ABI can observe
    /// backpressure, but cannot safely manufacture a blocked owner thread.
    fn saturate_owner(relay: &NativeRelay) -> SaturatedOwner {
        let sender = relay
            .inner
            .jobs
            .lock()
            .unwrap()
            .as_ref()
            .expect("relay owner queue is live")
            .clone();
        let (started_tx, started_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        sender
            .try_send(RelayCommand::Run {
                job: Box::new(move |_| {
                    started_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                }),
                _normal_permit: None,
            })
            .expect("owner blocker enters an empty queue");
        started_rx.recv().expect("owner blocker started");

        for _ in 0..NATIVE_RELAY_OWNER_COMMAND_MAX {
            sender
                .try_send(RelayCommand::Run {
                    job: Box::new(|_| {}),
                    _normal_permit: None,
                })
                .expect("every bounded owner slot is available exactly once");
        }
        let (drained_tx, drained_rx) = mpsc::channel();
        sender
            .try_send(RelayCommand::Run {
                job: Box::new(move |_| {
                    let _ = drained_tx.send(());
                }),
                _normal_permit: None,
            })
            .expect("the final owner slot carries a deterministic drain receipt");
        sender
            .try_send(RelayCommand::FlushForegroundWakes)
            .expect("the coalesced internal wake occupies its dedicated slot");
        assert!(matches!(
            sender.try_send(RelayCommand::Run {
                job: Box::new(|_| {}),
                _normal_permit: None,
            }),
            Err(mpsc::TrySendError::Full(_))
        ));
        SaturatedOwner {
            release: Some(release_tx),
            drained: drained_rx,
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum SaturatedTeardownKind {
        Close,
        Revoke,
        RuntimeInvalidation,
    }

    #[test]
    fn native_c_abi_process_reopen_rehydrates_the_exact_persisted_foreground_row() {
        // This is the close/reopen receipt that an RN device host needs, but
        // it deliberately uses no device-only production API. The test owns a
        // first host process, writes through its foreground C ABI, destroys
        // that host, then starts a wholly fresh host and re-admits the same
        // trusted scope.
        let directory = tempfile::tempdir().unwrap();
        let alice_path = directory.path().join("fixture-user-a.sqlite");
        let schema = permissive_schema();
        let row_id = [0x41; 16];
        let row_uuid = RowUuid::from_bytes(row_id);
        let title = "survives host restart";

        {
            let first_process = NativeHostAbiFixture::new();
            let alice = first_process.admit(&alice_path, "fixture-user-a", &schema, 0x41);
            let foreground = first_process.open_foreground(&alice);
            let ForegroundDbCommandResponse::TransactionOpened { transaction } = first_process
                .execute(
                    foreground,
                    ForegroundDbCommandRequest::BeginTransaction {
                        kind: ForegroundTransactionKind::Mergeable,
                    },
                )
            else {
                panic!("foreground transaction must open");
            };
            assert_eq!(
                first_process.execute(
                    foreground,
                    ForegroundDbCommandRequest::Insert {
                        transaction,
                        table: "todos".to_owned(),
                        cells: encoded_title_cells(title),
                        row_id: Some(row_id),
                    },
                ),
                ForegroundDbCommandResponse::Inserted { row_id }
            );
            assert!(matches!(
                first_process.execute(
                    foreground,
                    ForegroundDbCommandRequest::CommitTransaction { transaction },
                ),
                ForegroundDbCommandResponse::TransactionCommitted { tx_id } if tx_id != [0; 16]
            ));

            // First prove the foreground that committed through the C ABI can
            // materialize its exact local row before we ask the relay to
            // persist it for the next process.
            let rows = first_process.rows_after_sync(foreground);
            assert_exact_todo_rows(&rows, row_uuid, title);
            assert_eq!(
                first_process.execute(foreground, ForegroundDbCommandRequest::Close),
                ForegroundDbCommandResponse::Closed { closed: true }
            );
        }

        // Dropping `first_process` releases its last lease and host, which in
        // turn closes the owner thread and SQLite. This fresh host has no
        // in-memory registry, capabilities, foregrounds, or queries from the
        // first process.
        let second_process = NativeHostAbiFixture::new();
        let alice = second_process.admit(&alice_path, "fixture-user-a", &schema, 0x41);
        let reopened = second_process.open_foreground(&alice);
        let reopened_rows = second_process.rows_after_sync(reopened);
        assert_exact_todo_rows(&reopened_rows, row_uuid, title);

        // Auth-scope-to-SQLite-path selection remains a trusted *platform*
        // contract, not one the generic C admission ABI can infer or enforce:
        // it intentionally accepts the complete path selected by Kotlin or
        // Swift. Device acceptance must therefore prove scope isolation with
        // the compiled Android/iOS fixture path selectors; this core C-ABI
        // receipt deliberately proves only that an already-selected path
        // survives a complete native-host recreation.
    }

    #[test]
    fn native_c_abi_foregrounds_use_the_exact_admitted_capability_and_scope() {
        // This is intentionally an exact C-ABI receipt rather than a host-map
        // unit test. Admit A before B, then prove B's actual foreground sees
        // B's exact trusted scope. Replacing the capability lookup in
        // `open_foreground` with `.values().next()` makes B attach to A and
        // fails this receipt before revocation can mask that mix-up.
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let schema = permissive_schema();
        let a_capability = fixture.admit(
            &directory.path().join("scope-a.sqlite"),
            "scope-a",
            &schema,
            0x51,
        );
        let b_capability = fixture.admit(
            &directory.path().join("scope-b.sqlite"),
            "scope-b",
            &schema,
            0x61,
        );
        let forged_capability = [0xa5; 32];
        assert_ne!(forged_capability, a_capability);
        assert_ne!(forged_capability, b_capability);

        let (forged_status, forged_foreground) = fixture.try_open_foreground(&forged_capability);
        assert_eq!(forged_status, JazzNativeRelayStatus::InvalidHandle);
        assert_eq!(
            forged_foreground, 0,
            "a valid-length, unadmitted capability cannot produce an opaque handle"
        );

        let a = fixture.open_foreground(&a_capability);
        let b = fixture.open_foreground(&b_capability);
        assert!(matches!(
            fixture.execute(a, ForegroundDbCommandRequest::Probe),
            ForegroundDbCommandResponse::Probe { abi_version } if abi_version == NATIVE_RELAY_ABI_V1
        ));
        assert!(matches!(
            fixture.execute(b, ForegroundDbCommandRequest::Probe),
            ForegroundDbCommandResponse::Probe { abi_version } if abi_version == NATIVE_RELAY_ABI_V1
        ));

        fixture.insert_todo(a, [0xa1; 16], "scope-a-only");
        fixture.insert_todo(b, [0xb1; 16], "scope-b-only");
        assert_exact_todo_rows(
            &fixture.rows_after_sync(a),
            RowUuid::from_bytes([0xa1; 16]),
            "scope-a-only",
        );
        assert_exact_todo_rows(
            &fixture.rows_after_sync(b),
            RowUuid::from_bytes([0xb1; 16]),
            "scope-b-only",
        );

        assert_eq!(
            unsafe {
                jazz_native_relay_host_revoke_scope_capability(
                    fixture.host,
                    a_capability.as_ptr(),
                    a_capability.len(),
                )
            },
            JazzNativeRelayStatus::Ok,
            "trusted native code may revoke exactly A's capability"
        );
        assert_eq!(
            fixture.tick_status(a),
            JazzNativeRelayStatus::InvalidHandle,
            "revocation retires A's already-open foreground"
        );
        // Tick and byte-command execution use separate C entry points. The
        // installed device receipt probes the stale JSI alias, so prove that
        // exact liveness boundary instead of inferring it from tick failure.
        let request = postcard::to_allocvec(&ForegroundDbCommandRequest::Probe).unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_execute_foreground(
                    fixture.lease,
                    a,
                    request.as_ptr(),
                    request.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::InvalidHandle,
            "a revoked foreground cannot answer the installed JSI byte Probe"
        );
        assert!(output.data.is_null());
        assert_eq!(output.len, 0);
        assert_eq!(
            fixture.tick_status(b),
            JazzNativeRelayStatus::Ok,
            "revoking A cannot disturb B's separately admitted foreground"
        );
        let (reopen_a_status, reopen_a) = fixture.try_open_foreground(&a_capability);
        assert_eq!(reopen_a_status, JazzNativeRelayStatus::InvalidHandle);
        assert_eq!(reopen_a, 0);
        let b_after_a_revoke = fixture.open_foreground(&b_capability);
        assert_eq!(
            fixture.tick_status(b_after_a_revoke),
            JazzNativeRelayStatus::Ok,
            "B remains admissible after A revocation"
        );
    }

    #[test]
    fn c_abi_sibling_commit_wakes_retained_subscription_before_its_delta_drains() {
        // This is deliberately an internal C-ABI receipt. The public Db
        // integration suites establish subscription semantics; only the
        // native host owns opaque foreground handles and raw callback
        // registrations. It mirrors the installed A/B byte-command sequence
        // and proves that B's post-commit callback arrives before B drains
        // the committed delta.
        let directory = tempfile::tempdir().unwrap();
        // Raw callback contexts must outlive the host, including on assertion failure.
        let a_wake = Arc::new(QueuedNativeWake::active());
        let b_wake = Arc::new(QueuedNativeWake::active());
        let fixture = NativeHostAbiFixture::new();
        let capability = fixture.admit(
            &directory.path().join("same-runtime-wake.sqlite"),
            "same-runtime-wake",
            &permissive_schema(),
            0xc1,
        );
        let a = fixture.open_foreground(&capability);
        let b = fixture.open_foreground(&capability);
        assert_ne!(a, b, "each alias has an independent opaque C-ABI handle");

        for (foreground, wake) in [(a, &a_wake), (b, &b_wake)] {
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_set_foreground_wake_callback(
                        fixture.lease,
                        foreground,
                        Some(queue_native_wake),
                        Arc::as_ptr(wake) as *mut c_void,
                    )
                },
                JazzNativeRelayStatus::Ok,
                "each installed alias registers its own native wake callback"
            );
        }

        let ForegroundDbCommandResponse::Subscribed { subscription } = fixture.execute(
            b,
            ForegroundDbCommandRequest::Subscribe {
                query: postcard::to_allocvec(&Query::from("todos")).unwrap(),
                options_json: "{}".into(),
            },
        ) else {
            panic!("B opens the retained subscription");
        };

        let mut initial_reset_seen = false;
        for _ in 0..96 {
            match fixture.execute(
                b,
                ForegroundDbCommandRequest::DrainSubscription { subscription },
            ) {
                ForegroundDbCommandResponse::SubscriptionEvents { events } => {
                    initial_reset_seen |= events.iter().any(|event| {
                        matches!(
                            event,
                            ForegroundSubscriptionEvent::Delta { reset: true, .. }
                                | ForegroundSubscriptionEvent::StructuredDelta { reset: true, .. }
                        )
                    });
                }
                ForegroundDbCommandResponse::Pending { operation } => {
                    fixture.tick(b);
                    let response =
                        fixture.execute(b, ForegroundDbCommandRequest::Poll { operation });
                    if let ForegroundDbCommandResponse::SubscriptionEvents { events } = response {
                        initial_reset_seen |= events.iter().any(|event| {
                            matches!(
                                event,
                                ForegroundSubscriptionEvent::Delta { reset: true, .. }
                                    | ForegroundSubscriptionEvent::StructuredDelta {
                                        reset: true,
                                        ..
                                    }
                            )
                        });
                    }
                }
                response => panic!("initial B subscription drain failed: {response:?}"),
            }
            if initial_reset_seen {
                break;
            }
            fixture.tick(b);
        }
        assert!(
            initial_reset_seen,
            "B observes and drains its initial reset before A writes"
        );
        a_wake.queued.lock().unwrap().clear();
        b_wake.queued.lock().unwrap().clear();

        let ForegroundDbCommandResponse::TransactionOpened { transaction } = fixture.execute(
            a,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            },
        ) else {
            panic!("A opens the byte-command transaction");
        };
        let row_id = [0xd1; 16];
        assert_eq!(
            fixture.execute(
                a,
                ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "todos".into(),
                    cells: encoded_title_cells("wake across opaque aliases"),
                    row_id: Some(row_id),
                },
            ),
            ForegroundDbCommandResponse::Inserted { row_id }
        );
        assert!(matches!(
            fixture.execute(
                a,
                ForegroundDbCommandRequest::CommitTransaction { transaction },
            ),
            ForegroundDbCommandResponse::TransactionCommitted { .. }
        ));

        let mut b_woke = false;
        for _ in 0..96 {
            fixture.tick(a);
            fixture.tick(b);
            if b_wake.queued() != 0 {
                b_woke = true;
                break;
            }
        }
        assert!(
            b_woke,
            "A's committed byte transaction schedules B's native callback before B drains"
        );
        assert!(
            b_wake
                .queued
                .lock()
                .unwrap()
                .iter()
                .any(|(foreground, _, _)| *foreground == b),
            "the callback preserves B's exact opaque foreground handle"
        );

        let mut observed_row_delta = false;
        for _ in 0..96 {
            match fixture.execute(
                b,
                ForegroundDbCommandRequest::DrainSubscription { subscription },
            ) {
                ForegroundDbCommandResponse::SubscriptionEvents { events } => {
                    observed_row_delta |= events.iter().any(|event| match event {
                        ForegroundSubscriptionEvent::Delta { reset, delta, .. }
                        | ForegroundSubscriptionEvent::StructuredDelta { reset, delta, .. } => {
                            !reset
                                && delta
                                    .windows(row_id.len())
                                    .any(|candidate| candidate == row_id)
                        }
                        ForegroundSubscriptionEvent::Rejected { .. }
                        | ForegroundSubscriptionEvent::Closed => false,
                    });
                }
                ForegroundDbCommandResponse::Pending { operation } => {
                    fixture.tick(b);
                    let response =
                        fixture.execute(b, ForegroundDbCommandRequest::Poll { operation });
                    if let ForegroundDbCommandResponse::SubscriptionEvents { events } = response {
                        observed_row_delta |= events.iter().any(|event| match event {
                            ForegroundSubscriptionEvent::Delta { reset, delta, .. }
                            | ForegroundSubscriptionEvent::StructuredDelta {
                                reset, delta, ..
                            } => {
                                !reset
                                    && delta
                                        .windows(row_id.len())
                                        .any(|candidate| candidate == row_id)
                            }
                            ForegroundSubscriptionEvent::Rejected { .. }
                            | ForegroundSubscriptionEvent::Closed => false,
                        });
                    }
                }
                response => panic!("post-commit B subscription drain failed: {response:?}"),
            }
            if observed_row_delta {
                break;
            }
            fixture.tick(a);
            fixture.tick(b);
        }
        assert!(
            observed_row_delta,
            "B's retained subscription materializes A's committed row through the ordinary relay"
        );
    }

    #[test]
    fn revocation_clears_queued_owner_wakes_and_retires_only_its_foreground_lease() {
        // Internal native-host lifecycle receipt. A JSI owner turn can already
        // be queued when trusted code revokes its admitted scope. The old
        // `foregrounds.retain(...)` implementation hid A's handle but left
        // its owner client, raw callback context, and active lease alive.
        // This exact C-ABI path proves revocation clears all three while B
        // remains usable.
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let schema = permissive_schema();
        let a_capability = fixture.admit(
            &directory.path().join("revoke-a.sqlite"),
            "revoke-a",
            &schema,
            0x71,
        );
        let b_capability = fixture.admit(
            &directory.path().join("revoke-b.sqlite"),
            "revoke-b",
            &schema,
            0x81,
        );
        let a = fixture.open_foreground(&a_capability);
        let b = fixture.open_foreground(&b_capability);
        let (a_lease, b_lease, a_scope, b_scope) = unsafe {
            let host = (*fixture.host).inner.lock().unwrap();
            (
                host.foregrounds[&a].lease,
                host.foregrounds[&b].lease,
                host.foregrounds[&a].scope.clone(),
                host.foregrounds[&b].scope.clone(),
            )
        };

        let a_wake = Arc::new(QueuedNativeWake::active());
        let b_wake = Arc::new(QueuedNativeWake::active());
        for (foreground, wake) in [(a, &a_wake), (b, &b_wake)] {
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_set_foreground_wake_callback(
                        fixture.lease,
                        foreground,
                        Some(queue_native_wake),
                        Arc::as_ptr(wake) as *mut c_void,
                    )
                },
                JazzNativeRelayStatus::Ok,
                "the owner installs one native callback before it schedules work"
            );
        }

        // This is a real owner-thread wake, not a direct callback invocation:
        // the public Db scheduler crosses the native relay owner and queues a
        // later platform turn for each foreground.
        unsafe {
            let host = (*fixture.host).inner.lock().unwrap();
            for foreground in [a, b] {
                host.foreground_client(foreground)
                    .unwrap()
                    .with_db(|db| {
                        db.schedule_tick(TickUrgency::Deferred);
                        Ok(())
                    })
                    .unwrap();
            }
        }
        assert_eq!(a_wake.queued(), 1, "A has one queued platform wake");
        assert_eq!(b_wake.queued(), 1, "B has one queued platform wake");

        assert_eq!(
            unsafe {
                jazz_native_relay_host_revoke_scope_capability(
                    fixture.host,
                    a_capability.as_ptr(),
                    a_capability.len(),
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert_eq!(
            a_wake.cancelled.load(Ordering::Acquire),
            1,
            "revocation clears A's raw native callback before its context can be dropped"
        );
        assert_eq!(
            b_wake.cancelled.load(Ordering::Acquire),
            0,
            "revoking A never touches B's native callback"
        );
        assert_eq!(fixture.tick_status(a), JazzNativeRelayStatus::InvalidHandle);
        assert_eq!(fixture.tick_status(b), JazzNativeRelayStatus::Ok);
        let mut late_finalizer_closed = true;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_close_attached_foreground(
                    fixture.lease,
                    a,
                    &mut late_finalizer_closed,
                )
            },
            JazzNativeRelayStatus::Ok,
            "a finalizer that races after revocation remains an idempotent no-op"
        );
        assert!(
            !late_finalizer_closed,
            "revocation already owned A's only teardown transition"
        );

        // Deliver the tasks only after revocation. A's stale task is a no-op;
        // B's independent task remains live, which catches both a UAF-prone
        // omitted cancellation and over-broad scope teardown.
        a_wake.deliver_queued();
        b_wake.deliver_queued();
        assert_eq!(a_wake.delivered.load(Ordering::Acquire), 0);
        assert_eq!(b_wake.delivered.load(Ordering::Acquire), 1);

        unsafe {
            let mut host = (*fixture.host).inner.lock().unwrap();
            let pool = host
                .foreground_node_leases
                .get_mut(&a_scope)
                .expect("the revoked scope retains its lease retirement ledger");
            assert_eq!(
                pool.acquire_reusable(),
                None,
                "A's revoked identity was retired, never clean-returned"
            );
            assert_eq!(
                pool.acquire_fresh(a_lease.node),
                Err(jazz::foreground_node_lease::ForegroundNodeLeaseError::DuplicateNode),
                "the retired A identity cannot be minted again"
            );
            let b_pool = host
                .foreground_node_leases
                .get_mut(&b_scope)
                .expect("the independent live scope retains B's lease pool");
            assert_eq!(
                b_pool.acquire_fresh(b_lease.node),
                Err(jazz::foreground_node_lease::ForegroundNodeLeaseError::DuplicateNode),
                "B's still-live foreground retains its own exclusive lease"
            );
        }
    }

    #[test]
    fn saturated_owner_cannot_outlive_a_cancelled_raw_wake_context() {
        // Internal native-host lifecycle receipt. Saturating the owner makes
        // both scheduler clearing and owner-local client removal fail with
        // `OwnerQueueFull`. Close, revoke, and runtime invalidation must still
        // make the retained scheduler inert before telling the platform its
        // raw callback context can be freed.
        for teardown in [
            SaturatedTeardownKind::Close,
            SaturatedTeardownKind::Revoke,
            SaturatedTeardownKind::RuntimeInvalidation,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let fixture = NativeHostAbiFixture::new();
            let schema = permissive_schema();
            let path = directory.path().join("shared-saturated-owner.sqlite");

            // Two distinct capabilities intentionally name the exact same
            // trusted scope. They share one owner, while revoking A must not
            // revoke B's independent admission.
            let a_capability = fixture.admit(&path, "shared-owner", &schema, 0x91);
            let b_capability = fixture.admit(&path, "shared-owner", &schema, 0x91);
            assert_ne!(a_capability, b_capability);
            let b_runtime = unsafe { jazz_native_relay_host_retain(fixture.host, 2) };
            assert!(!b_runtime.is_null());
            let a = fixture.open_foreground(&a_capability);
            let mut b = 0;
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_open_attached_foreground(
                        b_runtime,
                        b_capability.as_ptr(),
                        b_capability.len(),
                        &mut b,
                    )
                },
                JazzNativeRelayStatus::Ok
            );
            assert_ne!(b, 0);

            let (a_lease, b_lease, scope, a_client) = unsafe {
                let host = (*fixture.host).inner.lock().unwrap();
                (
                    host.foregrounds[&a].lease,
                    host.foregrounds[&b].lease,
                    host.foregrounds[&a].scope.clone(),
                    host.foreground_client(a).unwrap().clone(),
                )
            };
            let a_wake = Arc::new(QueuedNativeWake::active());
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_set_foreground_wake_callback(
                        fixture.lease,
                        a,
                        Some(queue_native_wake),
                        Arc::as_ptr(&a_wake) as *mut c_void,
                    )
                },
                JazzNativeRelayStatus::Ok
            );

            let saturated = saturate_owner(&a_client.relay);
            match teardown {
                SaturatedTeardownKind::Close => {
                    let mut closed = true;
                    assert_eq!(
                        unsafe {
                            jazz_native_relay_host_lease_close_attached_foreground(
                                fixture.lease,
                                a,
                                &mut closed,
                            )
                        },
                        JazzNativeRelayStatus::LifecycleFailure
                    );
                    assert!(!closed);
                }
                SaturatedTeardownKind::Revoke => assert_eq!(
                    unsafe {
                        jazz_native_relay_host_revoke_scope_capability(
                            fixture.host,
                            a_capability.as_ptr(),
                            a_capability.len(),
                        )
                    },
                    JazzNativeRelayStatus::LifecycleFailure
                ),
                SaturatedTeardownKind::RuntimeInvalidation => assert_eq!(
                    unsafe {
                        jazz_native_relay_host_lease_invalidate_foreground_runtime(fixture.lease)
                    },
                    JazzNativeRelayStatus::LifecycleFailure
                ),
            }
            assert_eq!(
                a_wake.cancelled.load(Ordering::Acquire),
                1,
                "{teardown:?} inertizes the callback before reporting its forced retirement"
            );

            // A late HostObject finalizer is an idempotent no-op even while
            // the owner queue remains saturated; it must not attempt another
            // scheduler operation or reinterpret the failed teardown as live.
            let mut late_closed = true;
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_close_attached_foreground(
                        fixture.lease,
                        a,
                        &mut late_closed,
                    )
                },
                JazzNativeRelayStatus::Ok
            );
            assert!(!late_closed);
            unsafe {
                let mut host = (*fixture.host).inner.lock().unwrap();
                assert!(
                    host.foregrounds.contains_key(&b),
                    "{teardown:?} preserves B's foreground while its shared owner is saturated"
                );
                assert!(
                    host.admitted_scopes
                        .contains_key(&AdmissionCapability(b_capability)),
                    "{teardown:?} preserves B's independent admission"
                );
                let pool = host.foreground_node_leases.get_mut(&scope).unwrap();
                assert_eq!(
                    pool.acquire_reusable(),
                    None,
                    "{teardown:?} never clean-returns A while B is live"
                );
                assert_eq!(
                    pool.acquire_fresh(a_lease.node),
                    Err(jazz::foreground_node_lease::ForegroundNodeLeaseError::DuplicateNode),
                    "{teardown:?} permanently retires A"
                );
                assert_eq!(
                    pool.acquire_fresh(b_lease.node),
                    Err(jazz::foreground_node_lease::ForegroundNodeLeaseError::DuplicateNode),
                    "{teardown:?} keeps B's lease active"
                );
            }

            saturated.release_and_wait();
            assert_eq!(
                unsafe { jazz_native_relay_host_lease_tick_attached_foreground(b_runtime, b) },
                JazzNativeRelayStatus::Ok,
                "{teardown:?} leaves B usable after shared-owner backpressure clears"
            );
            // The owner still has A's Db because its close command could not
            // enter the full queue. A real scheduler request after the
            // platform has processed cancellation must nevertheless never
            // touch that raw context again.
            a_client
                .with_db(|db| {
                    db.schedule_tick(TickUrgency::Immediate);
                    Ok(())
                })
                .expect("orphaned owner client remains available until terminal cleanup");
            assert_eq!(
                a_wake.callbacks_after_cancel.load(Ordering::Acquire),
                0,
                "{teardown:?} leaves only an inert owner scheduler"
            );

            let mut b_closed = false;
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_close_attached_foreground(
                        b_runtime,
                        b,
                        &mut b_closed,
                    )
                },
                JazzNativeRelayStatus::Ok
            );
            assert!(b_closed);
            unsafe { jazz_native_relay_host_lease_free(b_runtime) };
        }
    }

    #[test]
    fn foreground_cell_and_command_decoders_require_one_canonical_envelope() {
        // Internal ABI-boundary receipt: the JSI adapter only transports
        // bytes, so the Rust decoder owns exact-envelope and duplicate-field
        // rejection. A duplicate must not silently become last-write-wins in
        // the `RowCells` map.
        let host_cells = b"\x01\x01\x05score\x03\x08\x2a\x00\x00\x00\x00\x00\x00\x00";
        assert_eq!(
            decode_foreground_cells(host_cells).unwrap(),
            BTreeMap::from([("score".to_owned(), Value::U64(42))]),
            "RN consumes the same explicit named-cell grammar as NAPI and WASM"
        );
        let duplicate =
            RecordDescriptor::new([("title", ValueType::String), ("title", ValueType::String)]);
        let raw = duplicate
            .create(&[
                Value::String("first".to_owned()),
                Value::String("second".to_owned()),
            ])
            .unwrap();
        let mut cells = encoded_cells(duplicate, raw);
        assert!(decode_foreground_cells(&cells).is_err());

        cells.push(0);
        assert!(decode_foreground_cells(&cells).is_err());

        let canonical =
            jazz::binding_codec::encode_named_cells(&jazz::groove::records::OwnedRecord::new(
                7_u64.to_le_bytes().to_vec(),
                RecordDescriptor::new([("count", ValueType::U64)]),
            ))
            .unwrap();
        assert_eq!(
            canonical,
            vec![
                1, 1, 5, b'c', b'o', b'u', b'n', b't', 3, 8, 7, 0, 0, 0, 0, 0, 0, 0
            ]
        );
        assert_eq!(
            decode_foreground_cells(&canonical).unwrap().get("count"),
            Some(&Value::U64(7))
        );

        let mut command = postcard::to_allocvec(&ForegroundDbCommandRequest::Probe).unwrap();
        command.push(0);
        assert!(decode_foreground_command(&command).is_err());
        assert!(decode_foreground_command(&[0x80, 0]).is_err());
    }

    fn config(path: PathBuf, auth_scope: Option<&str>) -> RelayOpenConfig {
        RelayOpenConfig {
            supported_abi: NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_V1,
                maximum: NATIVE_RELAY_ABI_V1,
            },
            scope: RelayScope {
                app_namespace: "native-relay-test".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: auth_scope.map(str::to_owned),
            },
            sqlite_path: path,
            schema: schema(),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0xa1; 16]),
                author: AuthorSubject::for_test_bytes([0xa2; 16]),
            },
            thread_start_counter: None,
        }
    }

    #[test]
    fn public_relay_open_rejects_a_planted_groove_only_manifest() {
        // This is an internal physical-admission receipt. The public relay
        // opener must not silently adopt a root that was initialized through
        // SQLite's generic Groove-only convenience API.
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("relay.sqlite");
        let config = config(path.clone(), Some("alice"));
        let column_families = config.schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        drop(SqliteStorage::open(&path, &refs).expect("plant generic Groove manifest"));

        let registry = NativeRelayRegistry::default();
        assert!(
            registry.open(config).is_err(),
            "the public relay open must reject a manifest that omits Jazz codecs"
        );
    }

    #[test]
    fn attached_client_identity_preserves_the_admitted_canonical_author() {
        let admitted = AuthorSubject::authenticated("https://issuer.example", "opaque-subject")
            .expect("fixture issuer and subject are valid");

        let client = fresh_client_identity(admitted).expect("OS entropy mints foreground node");

        assert_eq!(client.author, admitted);
        assert_eq!(
            client.author.canonical(),
            r#"["https://issuer.example","opaque-subject"]"#
        );
        assert_ne!(client.author, AuthorSubject::SYSTEM);
    }

    #[test]
    fn foreground_identity_is_fresh_across_fixed_clock_process_restarts() {
        let author = AuthorSubject::for_test_bytes([0x42; 16]);
        // Model the first foreground of two independently restarted native
        // hosts. Both HLCs start at the same fixed physical position; their
        // transaction identities must nevertheless differ by fresh node.
        let before_restart = fresh_client_identity(author).expect("OS entropy is available");
        let after_restart = fresh_client_identity(author).expect("OS entropy is available");
        assert_ne!(before_restart.node, after_restart.node);
        let fixed_time = TxTime::from(1234);
        assert_ne!(
            TxId::new(fixed_time, before_restart.node),
            TxId::new(fixed_time, after_restart.node),
            "same-millisecond first writes after a process restart need distinct transaction ids"
        );

        // Planted old construction: a handle-derived JAZZRN prefix would
        // repeat after every host reset and makes the assertion above unsafe.
        let mut deterministic = [0_u8; 16];
        deterministic[..8].copy_from_slice(b"JAZZRN\0\0");
        deterministic[8..].copy_from_slice(&2_u64.to_be_bytes());
        let old_first_foreground = NodeUuid::from_bytes(deterministic);
        assert_ne!(before_restart.node, old_first_foreground);
        assert_ne!(after_restart.node, old_first_foreground);
    }

    // Cancellation has no public TypeScript one-shot API. This host-boundary
    // test uses the existing test-only owner suspension to make contention
    // deterministic, then observes successful cancellation and released pins.
    // Internal scheduling receipt: JavaScript cannot deterministically suspend the
    // semantic owner between native commands. All data and uploads still use core.
    #[test]
    fn permission_advice_route_requires_the_admitted_scope_author() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("advice-scope.sqlite"),
            Some("advice"),
        ))
        .unwrap();
        let identity = fresh_client_identity(AuthorSubject::for_test_bytes([0x47; 16])).unwrap();
        let generic = relay.attach_client(identity, BTreeMap::new()).unwrap();
        assert!(matches!(
            generic
                .request_foreground_permission_advice(ForegroundPermissionAdviceAction::Read {
                    table: "todos".into(),
                    row: [1; 16]
                })
                .unwrap(),
            ForegroundOperationPoll::Ready(ForegroundOperationResult::PermissionAdvice(
                ForegroundPermissionAdvice::Unknown
            ))
        ));
        assert!(matches!(
            relay.attach_foreground_client(
                identity,
                BTreeMap::new(),
                ForegroundNodeLease {
                    node: identity.node,
                    confirmed_tx_time: TxTime::default()
                }
            ),
            Err(RelayError::ScopeConfigurationMismatch)
        ));
        generic.close().unwrap();
    }

    // The native socket queue and owner suspension are private scheduling
    // boundaries; public row receipts below prove accepted work survives them.
    #[test]
    fn socket_install_cancellation_waits_for_owner_and_preserves_local_writes() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("socket-owner.sqlite"),
            Some("socket-owner"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x47; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        let (generation, wire, committed) = relay
            .run(move |worker| {
                let persistent = Rc::clone(&worker.persistent);
                let mut held: RelayTickFuture = Box::pin(async move {
                    persistent.hold_node_owner_for_test().await;
                    Ok(())
                });
                assert!(
                    held.as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                worker.clients.get_mut(&id).unwrap().tick = Some(held);
                let start = std::time::Instant::now();
                let (generation, wire) = worker.begin_socket_upstream(None)?;
                assert!(start.elapsed() < std::time::Duration::from_millis(100));
                assert!(worker.upstream_transition.is_some());
                let (committed, _) = worker.direct_foreground_mutation(
                    id,
                    ForegroundMutationKind::Insert,
                    "todos".into(),
                    None,
                    encoded_title_cells("accepted while install waits"),
                    "{}".into(),
                )?;
                assert!(worker.retire_socket_upstream(generation)?);
                assert!(worker.upstream_transition.is_some());
                worker.clients.get_mut(&id).unwrap().tick = None;
                worker.poll_upstream_transition()?;
                assert!(worker.upstream_transition.is_none());
                assert!(!worker.upstream_attached);
                Ok((generation, wire, committed))
            })
            .unwrap();
        assert!(matches!(wire.take_outbound(), Err(RelayError::Closed)));
        let (replacement, fresh) = relay
            .run(|worker| worker.begin_socket_upstream(None))
            .unwrap();
        assert!(replacement > generation);
        assert!(
            !relay
                .run(move |worker| worker.retire_socket_upstream(generation))
                .unwrap()
        );
        assert!(fresh.take_outbound().is_ok());
        let mut settled = client
            .wait_for_foreground_transaction(*committed.as_bytes(), CoreDurabilityTier::Local)
            .unwrap();
        for _ in 0..100 {
            let ForegroundOperationPoll::Pending { operation } = settled else {
                break;
            };
            relay.pump().unwrap();
            settled = client.poll_foreground_operation(operation).unwrap();
        }
        assert!(matches!(
            settled,
            ForegroundOperationPoll::Ready(ForegroundOperationResult::TransactionSettled(_))
        ));
        client.close().unwrap();
    }

    #[test]
    fn retired_connection_backpressures_but_owner_close_stays_terminal() {
        let owner = Arc::new(RelayLiveness::new());
        let wire = NativeRelayWire::for_owner(Arc::clone(&owner));
        let mut transport = QueueTransport {
            wire: wire.clone(),
            session_context: None,
        };
        let message = SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        };

        wire.retire_connection();
        assert!(matches!(
            transport.send(message.clone()),
            Err(TransportError::Backpressure)
        ));
        assert!(matches!(wire.take_outbound(), Err(RelayError::Closed)));

        owner.mark_terminal();
        assert!(matches!(
            transport.send(message),
            Err(TransportError::Failed(error)) if error.contains("native relay is closed")
        ));
    }

    #[test]
    fn retired_socket_wire_rejects_stale_advice_and_cannot_detach_successor() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("socket-epoch.sqlite"),
            Some("socket-epoch"),
        ))
        .unwrap();
        let (first, stale) = relay
            .run(|worker| worker.begin_socket_upstream(None))
            .unwrap();
        let (second, fresh) = relay
            .run(|worker| worker.begin_socket_upstream(None))
            .unwrap();
        assert!(second > first);
        assert!(matches!(
            stale.push_inbound(SyncMessage::AuthorizationScopeDecision {
                request_id: jazz::protocol::PermissionAdviceRequestId([1; 16]),
                advice: jazz::protocol::PermissionAdvice::Allowed,
            }),
            Err(RelayError::Closed)
        ));
        assert!(matches!(stale.take_outbound(), Err(RelayError::Closed)));
        assert!(
            !relay
                .run(move |worker| worker.retire_socket_upstream(first))
                .unwrap()
        );
        assert!(fresh.take_outbound().is_ok());
        assert!(
            relay
                .run(move |worker| worker.retire_socket_upstream(second))
                .unwrap()
        );
        assert!(matches!(fresh.take_outbound(), Err(RelayError::Closed)));
    }

    #[test]
    fn streaming_push_yields_to_a_held_owner_and_close_cancels_it() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("upload-owner.sqlite"),
            Some("upload"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x47; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        let upload = relay
            .run(move |worker| {
                worker.begin_foreground_streaming_mutation(
                    id,
                    ForegroundMutationKind::Insert,
                    "todos".into(),
                    [0x49; 16],
                    {
                        let descriptor =
                            RecordDescriptor::new(std::iter::empty::<(&str, ValueType)>());
                        let raw = descriptor.create(&[]).unwrap();
                        encoded_cells(descriptor, raw)
                    },
                    "title".into(),
                    "{}".into(),
                )
            })
            .unwrap();
        let held = relay
            .run(move |worker| {
                let db = Rc::clone(&worker.foreground_client(id)?.db);
                worker.start_foreground_operation(
                    id,
                    None,
                    Box::pin(async move {
                        db.hold_node_owner_for_test().await;
                        unreachable!()
                    }),
                )
            })
            .unwrap();
        assert!(matches!(held, ForegroundOperationPoll::Pending { .. }));
        let push = relay
            .run(move |worker| {
                worker.push_foreground_streaming_mutation(id, upload, vec![b'x'; 65536])
            })
            .unwrap();
        assert!(matches!(push, ForegroundOperationPoll::Pending { .. }));
        client
            .close()
            .expect("close must release the owner and unfinished upload");
        relay
            .pump()
            .expect("relay remains usable after pending upload teardown");
    }

    // Internal scheduling receipt: JavaScript cannot deterministically suspend the
    // semantic owner between native commands. All data and uploads still use core.
    #[test]
    fn streaming_abort_during_pending_push_cannot_resurrect_upload() {
        pending_push_abort_receipt(false);
    }

    #[test]
    fn cancelled_streaming_push_remains_abortable() {
        pending_push_abort_receipt(true);
    }

    fn pending_push_abort_receipt(cancel_push: bool) {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("upload-owner.sqlite"),
            Some("upload"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x47; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        let upload = relay
            .run(move |worker| {
                worker.begin_foreground_streaming_mutation(
                    id,
                    ForegroundMutationKind::Insert,
                    "todos".into(),
                    [0x49; 16],
                    {
                        let descriptor =
                            RecordDescriptor::new(std::iter::empty::<(&str, ValueType)>());
                        let raw = descriptor.create(&[]).unwrap();
                        encoded_cells(descriptor, raw)
                    },
                    "title".into(),
                    "{}".into(),
                )
            })
            .unwrap();
        let held = relay
            .run(move |worker| {
                let db = Rc::clone(&worker.foreground_client(id)?.db);
                worker.start_foreground_operation(
                    id,
                    None,
                    Box::pin(async move {
                        db.hold_node_owner_for_test().await;
                        unreachable!()
                    }),
                )
            })
            .unwrap();
        assert!(matches!(held, ForegroundOperationPoll::Pending { .. }));
        let push = relay
            .run(move |worker| {
                worker.push_foreground_streaming_mutation(id, upload, vec![b'x'; 65536])
            })
            .unwrap();
        assert!(matches!(push, ForegroundOperationPoll::Pending { .. }));
        let abort = relay
            .run(move |worker| worker.abort_foreground_streaming_mutation(id, upload))
            .unwrap();
        let ForegroundOperationPoll::Pending { operation: abort } = abort else {
            panic!("abort must await in-flight push");
        };
        let ForegroundOperationPoll::Pending { operation: holder } = held else {
            unreachable!()
        };
        assert!(client.cancel_foreground_operation(holder).unwrap());
        let ForegroundOperationPoll::Pending { operation: push } = push else {
            unreachable!()
        };
        if cancel_push {
            assert!(client.cancel_foreground_operation(push).unwrap());
        }
        for _ in 0..if cancel_push { 0 } else { 10 } {
            relay.pump().unwrap();
            if matches!(
                client.poll_foreground_operation(push).unwrap(),
                ForegroundOperationPoll::Ready(_)
            ) {
                break;
            }
        }
        assert!(matches!(
            client.poll_foreground_operation(abort).unwrap(),
            ForegroundOperationPoll::Ready(ForegroundOperationResult::StreamingMutationAborted(
                true
            ))
        ));
        assert!(
            relay
                .run(move |worker| worker.push_foreground_streaming_mutation(id, upload, vec![1]))
                .is_err()
        );
        client.close().unwrap();
    }

    // Internal cancellation receipt: public JavaScript cannot hold the node owner
    // across an exact native operation boundary or inspect core upload journals.
    #[test]
    fn cancelled_upload_results_finish_cleanup_without_pinning_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("cancel-upload.sqlite"),
            Some("uploads"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x51; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        for finish in [false, true] {
            for round in 0..4 {
                let upload = relay
                    .run(move |worker| {
                        let descriptor =
                            RecordDescriptor::new(std::iter::empty::<(&str, ValueType)>());
                        let raw = descriptor.create(&[]).unwrap();
                        worker.begin_foreground_streaming_mutation(
                            id,
                            ForegroundMutationKind::Insert,
                            "todos".into(),
                            [if finish { 60 + round } else { 70 + round }; 16],
                            encoded_cells(descriptor, raw),
                            "title".into(),
                            "{}".into(),
                        )
                    })
                    .unwrap();
                let mut push = relay
                    .run(move |worker| {
                        worker.push_foreground_streaming_mutation(id, upload, vec![b'x'; 65536])
                    })
                    .unwrap();
                for _ in 0..100 {
                    let ForegroundOperationPoll::Pending { operation } = push else {
                        break;
                    };
                    relay.pump().unwrap();
                    push = client.poll_foreground_operation(operation).unwrap();
                }
                assert!(matches!(
                    push,
                    ForegroundOperationPoll::Ready(
                        ForegroundOperationResult::StreamingMutationPushed
                    )
                ));
                let held = relay
                    .run(move |worker| {
                        let db = Rc::clone(&worker.foreground_client(id)?.db);
                        worker.start_foreground_operation(
                            id,
                            None,
                            Box::pin(async move {
                                db.hold_node_owner_for_test().await;
                                unreachable!()
                            }),
                        )
                    })
                    .unwrap();
                let ForegroundOperationPoll::Pending { operation: holder } = held else {
                    unreachable!()
                };
                let result = relay
                    .run(move |worker| {
                        if finish {
                            worker.finish_foreground_streaming_mutation(id, upload)
                        } else {
                            worker.abort_foreground_streaming_mutation(id, upload)
                        }
                    })
                    .unwrap();
                let ForegroundOperationPoll::Pending { operation } = result else {
                    panic!("held owner must defer terminal upload operation");
                };
                assert!(client.cancel_foreground_operation(operation).unwrap());
                assert!(client.poll_foreground_operation(operation).is_err());
                assert!(client.cancel_foreground_operation(holder).unwrap());
                for _ in 0..100 {
                    relay.pump().unwrap();
                    if relay
                        .run(move |worker| {
                            Ok(worker.foreground_client(id)?.mutation_cleanups.is_empty())
                        })
                        .unwrap()
                    {
                        break;
                    }
                }
                relay
                    .run(move |worker| {
                        let client = worker.foreground_client(id)?;
                        assert!(client.mutation_cleanups.is_empty());
                        assert_eq!(client.mutations.upload_count_for_test(), 0);
                        assert_eq!(
                            block_on(client.db.pending_upload_count_for_test()).unwrap(),
                            0
                        );
                        Ok(())
                    })
                    .unwrap();
            }
        }
        client.close().unwrap();
    }

    #[test]
    fn mutation_command_rejects_unsupported_target_options_before_admission() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("mutation-options.sqlite"),
            Some("options"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x52; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        let tx = client
            .begin_foreground_transaction(ForegroundTransactionKind::Mergeable)
            .unwrap();
        for (mutation, key) in [
            (ForegroundMutationKind::Insert, "head"),
            (ForegroundMutationKind::Restore, "base"),
            (ForegroundMutationKind::Update, "branch"),
            (ForegroundMutationKind::Delete, "branch"),
            (ForegroundMutationKind::Upsert, "branch"),
        ] {
            for value in ["null", "\"draft\""] {
                let options = format!("{{\"{key}\":{value}}}");
                let direct_options = options.clone();
                let error = relay
                    .run(move |worker| {
                        worker.direct_foreground_mutation(
                            id,
                            mutation,
                            "todos".into(),
                            Some([0x53; 16]),
                            encoded_title_cells("must not write"),
                            direct_options,
                        )
                    })
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains(&format!("option `{key}` is not supported"))
                );
                let error = client
                    .stage_foreground_mutation(
                        tx,
                        mutation,
                        "todos".into(),
                        Some([0x53; 16]),
                        encoded_title_cells("must not write"),
                        options,
                    )
                    .unwrap_err();
                assert!(
                    error
                        .to_string()
                        .contains(&format!("option `{key}` is not supported"))
                );
            }
        }
        client.close().unwrap();
    }

    #[test]
    fn ordinary_multi_large_scalar_insert_settles_through_native_relay() {
        let directory = tempfile::tempdir().unwrap();
        let mut config = config(directory.path().join("multi-large.sqlite"), Some("large"));
        config.schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("documents")
                        .column("body", ColumnType::Text)
                        .column("payload", ColumnType::Bytea)
                        .column("metadata", ColumnType::Json { schema: None })
                        .column("done", ColumnType::Boolean),
                )
                .build(),
        )
        .unwrap();
        let relay = NativeRelay::spawn(config).unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x54; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        let (tx_id, _) = relay
            .run(move |worker| {
                let descriptor = RecordDescriptor::new([
                    ("body", ValueType::String),
                    ("payload", ValueType::Bytes),
                    ("metadata", ValueType::String),
                    ("done", ValueType::Bool),
                ]);
                let prefix = "a".repeat(70000);
                let json_prefix = prefix.clone();
                let mut bytes = vec![7; 70006];
                bytes[70000..].copy_from_slice(&[0, 1, 2, 3, 4, 5]);
                let raw = descriptor
                    .create(&[
                        Value::String(format!("{prefix}A😀BC")),
                        Value::Bytes(bytes),
                        Value::String(format!(
                            "{{\"padding\":\"{json_prefix}\",\"nested\":{{\"answer\":42}}}}"
                        )),
                        Value::Bool(false),
                    ])
                    .unwrap();
                worker.direct_foreground_mutation(
                    id,
                    ForegroundMutationKind::Insert,
                    "documents".into(),
                    None,
                    encoded_cells(descriptor, raw),
                    "{}".into(),
                )
            })
            .unwrap();
        let mut wait = client
            .wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local)
            .unwrap();
        for _ in 0..100 {
            let ForegroundOperationPoll::Pending { operation } = wait else {
                break;
            };
            relay
                .pump()
                .expect("multi-large local settlement must preserve the underlying relay error");
            wait = client.poll_foreground_operation(operation).unwrap();
        }
        assert!(matches!(
            wait,
            ForegroundOperationPoll::Ready(ForegroundOperationResult::TransactionSettled(_))
        ));
        client.close().unwrap();
    }

    // Internal receipt: deterministic owner contention is not exposed by the public JS API.
    #[test]
    fn standalone_mutation_queues_behind_a_held_owner() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("write-owner.sqlite"),
            Some("write"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x48; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        let holder = relay
            .run(move |worker| {
                let db = Rc::clone(&worker.foreground_client(id)?.db);
                worker.start_foreground_operation(
                    id,
                    None,
                    Box::pin(async move {
                        db.hold_node_owner_for_test().await;
                        unreachable!()
                    }),
                )
            })
            .unwrap();
        let ForegroundOperationPoll::Pending { operation: holder } = holder else {
            unreachable!()
        };
        let (tx_id, row_id) = relay
            .run(move |worker| {
                worker.direct_foreground_mutation(
                    id,
                    ForegroundMutationKind::Insert,
                    "todos".into(),
                    None,
                    encoded_title_cells("queued"),
                    "{}".into(),
                )
            })
            .unwrap();
        assert_eq!(
            relay
                .run(move |worker| worker.foreground_write_state(id, *tx_id.as_bytes()))
                .unwrap(),
            "{\"fate\":\"Pending\",\"global_time\":null,\"durability\":\"None\"}"
        );
        assert!(
            client
                .local_current_foreground_row("todos".into(), *row_id.as_bytes())
                .unwrap_err()
                .to_string()
                .contains("temporarily busy")
        );
        assert!(client.cancel_foreground_operation(holder).unwrap());
        let mut wait = client
            .wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local)
            .unwrap();
        for _ in 0..100 {
            let ForegroundOperationPoll::Pending { operation } = wait else {
                break;
            };
            relay.pump().unwrap();
            wait = client.poll_foreground_operation(operation).unwrap();
        }
        assert!(matches!(
            wait,
            ForegroundOperationPoll::Ready(ForegroundOperationResult::TransactionSettled(_))
        ));
        let rows = client
            .local_current_foreground_row("todos".into(), *row_id.as_bytes())
            .unwrap();
        let batches: Vec<DecodedForegroundRowBatch> = postcard::from_bytes(&rows).unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].rows.len(), 1);
        assert_eq!(batches[0].rows[0].row_id, row_id);
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        let mut read = client
            .start_foreground_read(query, "{}".into(), None)
            .unwrap();
        for _ in 0..100 {
            let ForegroundOperationPoll::Pending { operation } = read else {
                break;
            };
            relay.pump().unwrap();
            read = client.poll_foreground_operation(operation).unwrap();
        }
        let ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(rows)) = read else {
            panic!("queued row must become readable");
        };
        assert_exact_todo_rows(&rows, row_id, "queued");
        client.close().unwrap();
    }

    // Internal receipt: JS cannot deliberately hold the native owner. All results
    // are asserted through the foreground transaction/read/settlement boundary.
    #[test]
    fn explicit_transactions_queue_reads_before_commit_under_owner_contention() {
        for kind in [
            ForegroundTransactionKind::Mergeable,
            ForegroundTransactionKind::Exclusive,
        ] {
            let directory = tempfile::tempdir().unwrap();
            let relay =
                NativeRelay::spawn(config(directory.path().join("tx-owner.sqlite"), Some("tx")))
                    .unwrap();
            let client = relay
                .attach_client(
                    fresh_client_identity(AuthorSubject::for_test_bytes([0x49; 16])).unwrap(),
                    BTreeMap::new(),
                )
                .unwrap();
            let id = client.id;
            let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
            let holder = relay
                .run(move |worker| {
                    let db = Rc::clone(&worker.foreground_client(id)?.db);
                    worker.start_foreground_operation(
                        id,
                        None,
                        Box::pin(async move {
                            db.hold_node_owner_for_test().await;
                            unreachable!()
                        }),
                    )
                })
                .unwrap();
            let ForegroundOperationPoll::Pending { operation: holder } = holder else {
                unreachable!()
            };
            let transaction = client.begin_foreground_transaction(kind).unwrap();
            let row = client
                .insert_foreground_transaction(
                    transaction,
                    "todos".into(),
                    encoded_title_cells("first"),
                    None,
                )
                .unwrap();
            client
                .stage_foreground_mutation(
                    transaction,
                    ForegroundMutationKind::Update,
                    "todos".into(),
                    Some(*row.as_bytes()),
                    encoded_title_cells("queued"),
                    "{}".into(),
                )
                .unwrap();
            let mut read = client
                .start_foreground_read(query.clone(), "{}".into(), Some(transaction))
                .unwrap();
            assert!(matches!(read, ForegroundOperationPoll::Pending { .. }));
            client
                .update_foreground_transaction(
                    transaction,
                    "todos".into(),
                    *row.as_bytes(),
                    encoded_title_cells("after-read"),
                )
                .unwrap();
            let tx_id = client.commit_foreground_transaction(transaction).unwrap();
            assert!(client.cancel_foreground_operation(holder).unwrap());
            for _ in 0..100 {
                let ForegroundOperationPoll::Pending { operation } = read else {
                    break;
                };
                relay.pump().unwrap();
                read = client.poll_foreground_operation(operation).unwrap();
            }
            let ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(rows)) = read else {
                panic!("transaction read did not complete")
            };
            assert_exact_todo_rows(&rows, row, "queued");
            let mut wait = client
                .wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local)
                .unwrap();
            for _ in 0..100 {
                let ForegroundOperationPoll::Pending { operation } = wait else {
                    break;
                };
                relay.pump().unwrap();
                wait = client.poll_foreground_operation(operation).unwrap();
            }
            assert!(matches!(
                wait,
                ForegroundOperationPoll::Ready(ForegroundOperationResult::TransactionSettled(_))
            ));
            let mut committed = client
                .start_foreground_read(query, "{}".into(), None)
                .unwrap();
            for _ in 0..100 {
                let ForegroundOperationPoll::Pending { operation } = committed else {
                    break;
                };
                relay.pump().unwrap();
                committed = client.poll_foreground_operation(operation).unwrap();
            }
            let ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(rows)) = committed
            else {
                panic!("committed row absent")
            };
            assert_exact_todo_rows(&rows, row, "after-read");
            client.close().unwrap();
        }
    }

    // Internal owner hold makes cancellation and rollback contention deterministic.
    #[test]
    fn cancelled_transaction_read_releases_commit_fence_and_rollback_is_bounded() {
        for rollback in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let relay = NativeRelay::spawn(config(
                directory.path().join("tx-cancel.sqlite"),
                Some("tx-cancel"),
            ))
            .unwrap();
            let client = relay
                .attach_client(
                    fresh_client_identity(AuthorSubject::for_test_bytes([0x4a; 16])).unwrap(),
                    BTreeMap::new(),
                )
                .unwrap();
            let id = client.id;
            let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
            let holder = relay
                .run(move |worker| {
                    let db = Rc::clone(&worker.foreground_client(id)?.db);
                    worker.start_foreground_operation(
                        id,
                        None,
                        Box::pin(async move {
                            db.hold_node_owner_for_test().await;
                            unreachable!()
                        }),
                    )
                })
                .unwrap();
            let ForegroundOperationPoll::Pending { operation: holder } = holder else {
                unreachable!()
            };
            let tx = client
                .begin_foreground_transaction(ForegroundTransactionKind::Mergeable)
                .unwrap();
            let row = client
                .insert_foreground_transaction(
                    tx,
                    "todos".into(),
                    encoded_title_cells("cancelled observer"),
                    None,
                )
                .unwrap();
            let read = client
                .start_foreground_read(query.clone(), "{}".into(), Some(tx))
                .unwrap();
            let ForegroundOperationPoll::Pending { operation: read } = read else {
                unreachable!()
            };
            assert!(client.cancel_foreground_operation(read).unwrap());
            let committed = if rollback {
                assert!(client.rollback_foreground_transaction(tx).unwrap());
                None
            } else {
                Some(client.commit_foreground_transaction(tx).unwrap())
            };
            assert!(client.cancel_foreground_operation(holder).unwrap());
            for _ in 0..20 {
                relay.pump().unwrap();
            }
            if let Some(tx_id) = committed {
                let mut wait = client
                    .wait_for_foreground_transaction(*tx_id.as_bytes(), CoreDurabilityTier::Local)
                    .unwrap();
                for _ in 0..100 {
                    let ForegroundOperationPoll::Pending { operation } = wait else {
                        break;
                    };
                    relay.pump().unwrap();
                    wait = client.poll_foreground_operation(operation).unwrap();
                }
                assert!(matches!(
                    wait,
                    ForegroundOperationPoll::Ready(ForegroundOperationResult::TransactionSettled(
                        _
                    ))
                ));
            }
            let mut read = client
                .start_foreground_read(query, "{}".into(), None)
                .unwrap();
            for _ in 0..100 {
                let ForegroundOperationPoll::Pending { operation } = read else {
                    break;
                };
                relay.pump().unwrap();
                read = client.poll_foreground_operation(operation).unwrap();
            }
            let ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(rows)) = read else {
                panic!("read did not settle")
            };
            if rollback {
                assert!(
                    postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&rows)
                        .unwrap()
                        .iter()
                        .all(|b| b.rows.is_empty())
                );
            } else {
                assert_exact_todo_rows(&rows, row, "cancelled observer");
            }
            client.close().unwrap();
        }
    }

    // Internal owner contention has no public JS test control.
    #[test]
    fn foreground_close_with_queued_transaction_is_bounded() {
        for committed in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let relay = NativeRelay::spawn(config(
                directory.path().join("tx-close.sqlite"),
                Some("tx-close"),
            ))
            .unwrap();
            let client = relay
                .attach_client(
                    fresh_client_identity(AuthorSubject::for_test_bytes([0x4b; 16])).unwrap(),
                    BTreeMap::new(),
                )
                .unwrap();
            let id = client.id;
            relay
                .run(move |worker| {
                    let db = Rc::clone(&worker.foreground_client(id)?.db);
                    worker.start_foreground_operation(
                        id,
                        None,
                        Box::pin(async move {
                            db.hold_node_owner_for_test().await;
                            unreachable!()
                        }),
                    )
                })
                .unwrap();
            thread_local! { static CLOSED_DB: RefCell<std::rc::Weak<Db<MemoryStorage>>> = const { RefCell::new(std::rc::Weak::new()) }; }
            relay
                .run(move |worker| {
                    CLOSED_DB.with(|weak| {
                        *weak.borrow_mut() =
                            Rc::downgrade(&worker.foreground_client(id).unwrap().db)
                    });
                    Ok(())
                })
                .unwrap();
            let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
            let tx = client
                .begin_foreground_transaction(ForegroundTransactionKind::Exclusive)
                .unwrap();
            client
                .insert_foreground_transaction(
                    tx,
                    "todos".into(),
                    encoded_title_cells("abandoned"),
                    None,
                )
                .unwrap();
            let pending = client
                .start_foreground_read(query, "{}".into(), Some(tx))
                .unwrap();
            assert!(matches!(pending, ForegroundOperationPoll::Pending { .. }));
            thread_local! { static CLOSED_WRITE: RefCell<Option<Rc<jazz::db::WriteHandle<MemoryStorage>>>> = const { RefCell::new(None) }; }
            if committed {
                let tx_id = client.commit_foreground_transaction(tx).unwrap();
                relay
                    .run(move |worker| {
                        CLOSED_WRITE.with(|write| {
                            *write.borrow_mut() = worker
                                .foreground_client(id)
                                .unwrap()
                                .mutations
                                .writes
                                .borrow()
                                .get(&tx_id)
                                .cloned()
                        });
                        Ok(())
                    })
                    .unwrap();
            }
            let stale = client.clone();
            client.close().unwrap();
            let mut released = false;
            for _ in 0..100 {
                released = relay
                    .run(|_| Ok(CLOSED_DB.with(|weak| weak.borrow().upgrade().is_none())))
                    .unwrap();
                if released {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert!(
                released,
                "closed foreground must release its Db allocation without JS ticks"
            );
            assert!(stale.commit_foreground_transaction(tx).is_err());
            let sibling = relay
                .attach_client(
                    fresh_client_identity(AuthorSubject::for_test_bytes([0x4c; 16])).unwrap(),
                    BTreeMap::new(),
                )
                .unwrap();
            let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
            let mut read = sibling
                .start_foreground_read(query, "{}".into(), None)
                .unwrap();
            for _ in 0..100 {
                let ForegroundOperationPoll::Pending { operation } = read else {
                    break;
                };
                relay.pump().unwrap();
                read = sibling.poll_foreground_operation(operation).unwrap();
            }
            let ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(rows)) = read else {
                panic!("sibling read stalled")
            };
            if committed {
                relay
                    .run(|_| {
                        CLOSED_WRITE.with(|write| {
                            let write = write.borrow_mut().take().unwrap();
                            let error = block_on(write.write_state())
                                .expect_err("published write's node has been retired");
                            assert_eq!(error.code, jazz::db::ErrorCode::NotObserved);
                            assert_eq!(error.message, "database handle was dropped");
                        });
                        Ok(())
                    })
                    .unwrap();
            } else {
                assert!(
                    postcard::from_bytes::<Vec<DecodedForegroundRowBatch>>(&rows)
                        .unwrap()
                        .iter()
                        .all(|b| b.rows.is_empty())
                );
            }
            sibling.close().unwrap();
        }
    }

    // Internal receipt observes the local commit after Db::close completes,
    // but before its retained owner is released. Close does not promise relay flush.
    #[test]
    fn closing_owner_finishes_local_commit_despite_peer_io_failure() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("close-local.sqlite"),
            Some("close-local"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x4e; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        let observed = Arc::new(AtomicBool::new(false));
        let receipt = Arc::clone(&observed);
        thread_local! { static RETIRED_DB: RefCell<std::rc::Weak<Db<MemoryStorage>>> = const { RefCell::new(std::rc::Weak::new()) }; }
        relay
            .run(move |worker| {
                let db = Rc::clone(&worker.foreground_client(id)?.db);
                RETIRED_DB.with(|weak| *weak.borrow_mut() = Rc::downgrade(&db));
                let held = Rc::clone(&db);
                worker.start_foreground_operation(
                    id,
                    None,
                    Box::pin(async move {
                        held.hold_node_owner_for_test().await;
                        unreachable!()
                    }),
                )?;
                let tx = worker
                    .begin_foreground_transaction(id, ForegroundTransactionKind::Mergeable)?;
                let row = worker.insert_foreground_transaction(
                    id,
                    tx,
                    "todos".into(),
                    encoded_title_cells("accepted locally"),
                    None,
                )?;
                worker.commit_foreground_transaction(id, tx)?;
                worker.retire_foreground(id)?;
                let closing = worker.closing.back_mut().unwrap();
                let failed_inbound = Arc::new(Mutex::new(BoundedMessageQueue::default()));
                let poison = Arc::clone(&failed_inbound);
                assert!(
                    std::panic::catch_unwind(move || {
                        let _held = poison.lock().unwrap();
                        panic!("controlled auxiliary queue failure");
                    })
                    .is_err()
                );
                closing.client.upstream_io.wire.inbound = failed_inbound;
                assert!(closing.client.upstream_io.poll(Waker::noop()).is_err());
                let close = closing.close.take().unwrap();
                closing.close = Some(Box::pin(async move {
                    close.await?;
                    let current = db
                        .local_current_row("todos", row)
                        .await?
                        .expect("accepted local commit must complete before owner release");
                    assert_eq!(current.row_uuid(), row);
                    assert!(!current.is_deleted());
                    assert_eq!(
                        current.cell(
                            schema()
                                .tables()
                                .iter()
                                .find(|table| table.name == "todos")
                                .unwrap(),
                            "title"
                        ),
                        Some(Value::String("accepted locally".into()))
                    );
                    receipt.store(true, Ordering::Release);
                    Ok(())
                }));
                Ok(())
            })
            .unwrap();
        let mut released = false;
        for _ in 0..100 {
            released = relay
                .run(|_| Ok(RETIRED_DB.with(|weak| weak.borrow().upgrade().is_none())))
                .unwrap();
            if released {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(
            observed.load(Ordering::Acquire),
            "local commit completion receipt"
        );
        assert!(released, "peer I/O failure cannot pin the closing owner");
    }

    // Internal receipt: public hosts cannot deterministically stall the local
    // transaction FIFO or poison the independent persistent peer queue.
    #[test]
    fn shutdown_keeps_local_commit_despite_persistent_io_failure() {
        shutdown_failure_receipt(true, false);
    }

    #[test]
    fn disconnected_owner_keeps_local_commit_despite_persistent_io_failure() {
        shutdown_failure_receipt(true, true);
    }

    #[test]
    fn shutdown_core_close_error_releases_failed_owner_and_finishes_sibling() {
        shutdown_failure_receipt(false, false);
    }

    fn shutdown_failure_receipt(persistent_io_failure: bool, disconnected: bool) {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("shutdown-local.sqlite"),
            Some("shutdown-local"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x4e; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        let observed = Arc::new(AtomicBool::new(false));
        let receipt = Arc::clone(&observed);
        relay
            .run(move |worker| {
                let tx = worker
                    .begin_foreground_transaction(id, ForegroundTransactionKind::Mergeable)?;
                let (db, state) = worker.foreground_transaction(id, tx)?;
                let liveness = Arc::clone(&worker.liveness);
                let _read = db.enqueue_transaction_read(state.open_tx_id, async move {
                    // Deterministically keep accepted work pending through the
                    // first shutdown turn, without timing or a JS-owned future.
                    let mut pending_turns = 2;
                    futures::future::poll_fn(move |context| {
                        if liveness.is_alive() {
                            return Poll::Pending;
                        }
                        if pending_turns > 0 {
                            pending_turns -= 1;
                            context.waker().wake_by_ref();
                            Poll::Pending
                        } else {
                            Poll::Ready(())
                        }
                    })
                    .await;
                    Ok(())
                });
                db.drive_queued_mutation_once();
                let row = worker.insert_foreground_transaction(
                    id,
                    tx,
                    "todos".into(),
                    encoded_title_cells("accepted before shutdown"),
                    None,
                )?;
                worker.commit_foreground_transaction(id, tx)?;
                worker.retire_foreground(id)?;
                let closing = worker.closing.back_mut().unwrap();
                let close = closing.close.take().unwrap();
                closing.close = Some(Box::pin(async move {
                    close.await?;
                    let current = db.local_current_row("todos", row).await?.expect(
                        "accepted local commit must finish before shutdown releases its owner",
                    );
                    assert_eq!(current.row_uuid(), row);
                    receipt.store(true, Ordering::Release);
                    Ok(())
                }));
                if persistent_io_failure {
                    let failed_inbound = Arc::new(Mutex::new(BoundedMessageQueue::default()));
                    let poison = Arc::clone(&failed_inbound);
                    assert!(
                        std::panic::catch_unwind(move || {
                            let _held = poison.lock().unwrap();
                            panic!("controlled persistent queue failure");
                        })
                        .is_err()
                    );
                    worker.upstream_io.wire.inbound = failed_inbound;
                } else {
                    let failed_id = worker.attach_client(
                        fresh_client_identity(AuthorSubject::for_test_bytes([0x4f; 16]))?,
                        BTreeMap::new(),
                        None,
                        false,
                    )?;
                    worker.retire_foreground(failed_id)?;
                    let liveness = Arc::clone(&worker.liveness);
                    worker.closing.back_mut().unwrap().close =
                        Some(Box::pin(futures::future::poll_fn(move |_| {
                            if liveness.is_alive() {
                                Poll::Pending
                            } else {
                                Poll::Ready(Err(jazz::db::Error {
                                    code: jazz::db::ErrorCode::NotObserved,
                                    message: "controlled terminal storage close failure".into(),
                                }))
                            }
                        })));
                    // Enter the same final drain on this owner turn, so an
                    // earlier background turn cannot consume the injected error.
                    worker.liveness.mark_terminal();
                    worker.finish_foreground_retirement();
                    assert!(
                        worker.closing.is_empty(),
                        "terminal core failure must not end the drain before its pending sibling"
                    );
                }
                Ok(())
            })
            .unwrap();
        if disconnected {
            // Drop the final command sender while retaining the join handle.
            relay.inner.jobs.lock().unwrap().take();
        }
        relay.inner.shutdown().unwrap();
        assert!(
            observed.load(Ordering::Acquire),
            "shutdown must finish the accepted local commit despite an independent peer or sibling close failure"
        );
    }

    #[test]
    fn rolled_back_staging_failures_retire_queued_error_bookkeeping() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("tx-errors.sqlite"),
            Some("tx-errors"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x4d; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        relay
            .run(move |worker| {
                for _ in 0..32 {
                    let tx = worker
                        .begin_foreground_transaction(id, ForegroundTransactionKind::Mergeable)?;
                    let (db, state) = worker.foreground_transaction(id, tx)?;
                    assert!(
                        worker
                            .insert_foreground_transaction(
                                id,
                                tx,
                                "missing_table".into(),
                                encoded_title_cells("rejected"),
                                None
                            )
                            .is_err()
                    );
                    assert!(db.queued_transaction_error(state.open_tx_id).is_some());
                    assert!(worker.rollback_foreground_transaction(id, tx)?);
                    assert!(db.queued_transaction_error(state.open_tx_id).is_none());
                }
                assert!(worker.foreground_client(id)?.transactions.is_empty());
                Ok(())
            })
            .unwrap();
        client.close().unwrap();
    }

    #[test]
    fn cancelled_read_releases_coverage_after_contended_owner_resumes() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("coverage-cleanup.sqlite"),
            Some("cleanup"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x45; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        let read = match client
            .start_foreground_read(query, "{\"tier\":\"edge\"}".into(), None)
            .unwrap()
        {
            ForegroundOperationPoll::Pending { operation } => operation,
            _ => panic!("remote coverage without an authority must remain pending"),
        };
        let id = client.id;
        let holder = relay
            .run(move |worker| {
                let db = Rc::clone(&worker.foreground_client(id)?.db);
                let future: ForegroundOperationFuture = Box::pin(async move {
                    db.hold_node_owner_for_test().await;
                    unreachable!("owner holder ends only on cancellation")
                });
                worker.start_foreground_operation(id, None, future)
            })
            .unwrap();
        let ForegroundOperationPoll::Pending { operation: holder } = holder else {
            panic!("owner holder must remain pending");
        };
        assert!(client.cancel_foreground_operation(read).unwrap());
        relay
            .pump()
            .expect("cleanup cannot wait synchronously for the held owner");
        assert_eq!(
            relay
                .run(move |worker| Ok(worker
                    .foreground_client(id)?
                    .db
                    .query_coverage_attachment_counts_for_test()))
                .unwrap(),
            (1, 1)
        );
        assert!(client.cancel_foreground_operation(holder).unwrap());
        relay.pump().unwrap();
        assert_eq!(
            relay
                .run(move |worker| Ok(worker
                    .foreground_client(id)?
                    .db
                    .query_coverage_attachment_counts_for_test()))
                .unwrap(),
            (0, 0)
        );
    }

    // A retained semantic owner and native capability teardown are host
    // boundaries. Keep the contention deterministic with the existing owner
    // suspension hook, then assert real C-ABI opens, writes, close and revoke.
    fn hold_persistent_owner(relay: &NativeRelay) {
        relay
            .run(|worker| {
                let db = Rc::clone(&worker.persistent);
                worker.persistent_tick = Some(Box::pin(async move {
                    db.hold_node_owner_for_test().await;
                    unreachable!("test owner is released by dropping its future")
                }));
                Ok(())
            })
            .unwrap();
        relay.pump().unwrap();
    }

    #[test]
    fn foreground_admission_waits_for_owner_and_close_discards_unadmitted_traffic() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let capability = fixture.admit(
            &directory.path().join("admission.sqlite"),
            "admission",
            &permissive_schema(),
            0x63,
        );
        let keeper = fixture.open_foreground(&capability);
        let relay = unsafe {
            (*fixture.host)
                .inner
                .lock()
                .unwrap()
                .foreground_client(keeper)
                .unwrap()
                .relay
                .clone()
        };
        hold_persistent_owner(&relay);
        let closed = fixture.open_foreground(&capability);
        let closed_client = unsafe {
            (*fixture.host)
                .inner
                .lock()
                .unwrap()
                .foreground_client(closed)
                .unwrap()
                .clone()
        };
        fixture.insert_todo(closed, [0x71; 16], "cancelled before peer admission");
        fixture.tick(closed);
        let id = closed_client.id;
        relay
            .run(move |worker| {
                let client = &worker.clients[&id];
                assert!(client.admission.is_some());
                assert!(
                    client._served.is_none(),
                    "a waiting admission cannot install a peer"
                );
                Ok(())
            })
            .unwrap();
        assert!(
            closed_client.wire.outbound.lock().unwrap().len() > 0,
            "ordinary foreground traffic queues before admission"
        );
        assert_eq!(
            fixture.execute(closed, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
        assert_eq!(closed_client.wire.outbound.lock().unwrap().len(), 0);
        assert_eq!(closed_client.wire.inbound.lock().unwrap().len(), 0);

        let admitted = fixture.open_foreground(&capability);
        let row = [0x72; 16];
        fixture.insert_todo(admitted, row, "admitted after owner resumed");
        fixture.tick(admitted);
        relay
            .run(|worker| {
                worker.persistent_tick = None;
                Ok(())
            })
            .unwrap();
        for _ in 0..16 {
            fixture.tick(admitted);
        }
        assert_exact_todo_rows(
            &fixture.rows_after_sync(keeper),
            RowUuid::from_bytes(row),
            "admitted after owner resumed",
        );
        assert_eq!(
            fixture.execute(admitted, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
        assert_eq!(
            fixture.execute(keeper, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
    }

    #[test]
    fn revocation_drops_waiting_subscriber_admission_and_preserves_another_scope() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let schema = permissive_schema();
        let a_capability = fixture.admit(
            &directory.path().join("admission-a.sqlite"),
            "admission-a",
            &schema,
            0x64,
        );
        let b_capability = fixture.admit(
            &directory.path().join("admission-b.sqlite"),
            "admission-b",
            &schema,
            0x65,
        );
        let a = fixture.open_foreground(&a_capability);
        let b = fixture.open_foreground(&b_capability);
        let relay = unsafe {
            (*fixture.host)
                .inner
                .lock()
                .unwrap()
                .foreground_client(a)
                .unwrap()
                .relay
                .clone()
        };
        hold_persistent_owner(&relay);
        let waiting = fixture.open_foreground(&a_capability);
        fixture.insert_todo(waiting, [0x73; 16], "revoked before admission");
        fixture.tick(waiting);
        assert_eq!(
            unsafe {
                jazz_native_relay_host_revoke_scope_capability(
                    fixture.host,
                    a_capability.as_ptr(),
                    a_capability.len(),
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert_eq!(
            fixture.tick_status(waiting),
            JazzNativeRelayStatus::InvalidHandle
        );
        assert!(matches!(relay.pump(), Err(RelayError::Closed)));
        let row = [0x74; 16];
        fixture.insert_todo(b, row, "independent scope survives");
        assert_exact_todo_rows(
            &fixture.rows_after_sync(b),
            RowUuid::from_bytes(row),
            "independent scope survives",
        );
    }

    #[test]
    fn delayed_admission_failure_is_owned_by_the_opening_foreground() {
        // A failing admission future is injected here because this receipt
        // owns host error delivery, not the storage/auth implementation that
        // produces the ordinary core Error.
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let capability = fixture.admit(
            &directory.path().join("admission-error.sqlite"),
            "admission-error",
            &permissive_schema(),
            0x66,
        );
        let keeper = fixture.open_foreground(&capability);
        let relay = unsafe {
            (*fixture.host)
                .inner
                .lock()
                .unwrap()
                .foreground_client(keeper)
                .unwrap()
                .relay
                .clone()
        };
        hold_persistent_owner(&relay);
        let failed = fixture.open_foreground(&capability);
        let client = unsafe {
            (*fixture.host)
                .inner
                .lock()
                .unwrap()
                .foreground_client(failed)
                .unwrap()
                .clone()
        };
        let id = client.id;
        relay
            .run(move |worker| {
                worker.clients.get_mut(&id).unwrap().admission = Some(Box::pin(async {
                    Err(jazz::db::Error {
                        code: jazz::db::ErrorCode::Protocol,
                        message: "admission was rejected".into(),
                    })
                }));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            fixture.tick_status(failed),
            JazzNativeRelayStatus::LifecycleFailure
        );
        assert!(
            matches!(client.start_foreground_read(postcard::to_allocvec(&Query::from("todos")).unwrap(), "{}".into(), None), Err(RelayError::Db(error)) if error.message == "admission was rejected")
        );
        assert_eq!(
            fixture.tick_status(failed),
            JazzNativeRelayStatus::LifecycleFailure
        );
        fixture.tick(keeper);
        assert_eq!(
            fixture.execute(failed, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
        relay
            .run(|worker| {
                worker.persistent_tick = None;
                Ok(())
            })
            .unwrap();
        fixture.tick(keeper);
    }

    #[test]
    fn cancelled_read_cleanup_keeps_scheduling_until_its_bounded_queue_is_empty() {
        // Cancellation and coalesced native callbacks have no one-shot JS
        // public API. Exercise real coverage attachments and the registered
        // host callback while isolating cleanup from unrelated peer wakes.
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let capability = fixture.admit(
            &directory.path().join("cleanup-batch.sqlite"),
            "cleanup-batch",
            &permissive_schema(),
            0x62,
        );
        let foreground = fixture.open_foreground(&capability);
        let wake = Arc::new(QueuedNativeWake::active());
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_set_foreground_wake_callback(
                    fixture.lease,
                    foreground,
                    Some(queue_native_wake),
                    Arc::as_ptr(&wake) as *mut c_void,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let client = unsafe {
            (*fixture.host)
                .inner
                .lock()
                .unwrap()
                .foreground_client(foreground)
                .unwrap()
                .clone()
        };
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        for _ in 0..7 {
            let ForegroundOperationPoll::Pending { operation } = client
                .start_foreground_read(query.clone(), "{\"tier\":\"edge\"}".into(), None)
                .unwrap()
            else {
                panic!("remote read without an authority remains pending");
            };
            assert!(client.cancel_foreground_operation(operation).unwrap());
        }
        let id = client.id;
        for remaining in (0..7).rev() {
            assert!(
                wake.queued() > 0,
                "the remaining cleanup batch must have a scheduled owner turn"
            );
            // Simulate the platform consuming every coalesced notification.
            wake.queued.lock().unwrap().clear();
            let queued = client
                .relay
                .run(move |worker| {
                    let waker = Waker::from(Arc::clone(&worker.wake));
                    let client = worker.foreground_client_mut(id)?;
                    client.poll_read_cleanup(&waker);
                    assert!(
                        client.read_cleanup.is_none(),
                        "resident detach finishes in its turn"
                    );
                    let queued = client.read_cleanups.borrow().len();
                    Ok(queued)
                })
                .unwrap();
            assert_eq!(
                queued, remaining,
                "each cleanup turn drains exactly one attachment"
            );
        }
        assert_eq!(
            client
                .with_db(|db| Ok(db.query_coverage_attachment_counts_for_test()))
                .unwrap(),
            (0, 0)
        );
        assert_eq!(
            fixture.execute(foreground, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
    }

    #[test]
    fn retained_pump_waker_reaches_live_siblings_and_retires_closed_callbacks() {
        // A suspended future's actual Context waker and the raw platform
        // callback lifetime are host mechanics, so exercise them at this
        // internal owner boundary rather than substituting a Db test executor.
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let capability = fixture.admit(
            &directory.path().join("wake.sqlite"),
            "wake",
            &permissive_schema(),
            0x61,
        );
        let a = fixture.open_foreground(&capability);
        let b = fixture.open_foreground(&capability);
        let a_wake = Arc::new(QueuedNativeWake::active());
        let b_wake = Arc::new(QueuedNativeWake::active());
        for (foreground, wake) in [(a, &a_wake), (b, &b_wake)] {
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_set_foreground_wake_callback(
                        fixture.lease,
                        foreground,
                        Some(queue_native_wake),
                        Arc::as_ptr(wake) as *mut c_void,
                    )
                },
                JazzNativeRelayStatus::Ok
            );
        }
        let client = unsafe {
            (*fixture.host)
                .inner
                .lock()
                .unwrap()
                .foreground_client(a)
                .unwrap()
                .clone()
        };
        let captured = Arc::new(Mutex::new(None::<Waker>));
        let captured_by_future = Arc::clone(&captured);
        let id = client.id;
        client
            .relay
            .run(move |worker| {
                worker.clients.get_mut(&id).unwrap().tick =
                    Some(Box::pin(std::future::poll_fn(move |context| {
                        *captured_by_future.lock().unwrap() = Some(context.waker().clone());
                        Poll::Pending
                    })));
                Ok(())
            })
            .unwrap();
        fixture.tick(a);
        let waker = captured
            .lock()
            .unwrap()
            .take()
            .expect("pump polls the retained future");
        a_wake.queued.lock().unwrap().clear();
        b_wake.queued.lock().unwrap().clear();
        waker.wake_by_ref();
        assert!(
            a_wake.wait_for_queued(1),
            "owner notification must eventually schedule its foreground"
        );
        assert!(
            b_wake.wait_for_queued(1),
            "owner notification must eventually reach a live sibling"
        );
        assert_eq!(
            a_wake.queued(),
            1,
            "future readiness schedules its foreground"
        );
        assert_eq!(
            b_wake.queued(),
            1,
            "future readiness also reaches a live sibling"
        );
        assert_eq!(
            fixture.execute(a, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
        assert_eq!(
            client
                .relay
                .run(|worker| Ok(worker.foreground_wake_generations.len()))
                .unwrap(),
            1,
            "retiring one foreground releases its generation while its sibling remains live"
        );
        b_wake.queued.lock().unwrap().clear();
        waker.wake_by_ref();
        assert!(
            b_wake.wait_for_queued(1),
            "the shared future must still reach a live sibling"
        );
        assert_eq!(
            b_wake.queued(),
            1,
            "the shared future can outlive its original foreground"
        );
        assert_eq!(a_wake.callbacks_after_cancel.load(Ordering::Acquire), 0);
        assert_eq!(
            fixture.execute(b, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
        waker.wake_by_ref();
        a_wake.assert_no_callback_for(Duration::from_millis(50));
        b_wake.assert_no_callback_for(Duration::from_millis(50));
    }

    #[test]
    fn replacement_wake_before_old_host_state_becomes_inert_uses_current_generation() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("replacement.sqlite"),
            Some("replacement"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x62; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let old_receipt = Arc::new(QueuedNativeWake::active());
        let new_receipt = Arc::new(QueuedNativeWake::active());
        let state = |receipt: &Arc<QueuedNativeWake>| {
            Arc::new(ForegroundWakeState::new(ForegroundWakeRegistration {
                callback: queue_native_wake,
                context: Arc::as_ptr(receipt) as usize,
            }))
        };
        let old_state = state(&old_receipt);
        let new_state = state(&new_receipt);
        client
            .set_foreground_wake_callback(1, Some(old_state.clone()))
            .unwrap();
        let id = client.id;
        let old_scheduler = relay
            .run(move |worker| Ok(worker.wake.foregrounds.lock().unwrap()[&id].clone()))
            .unwrap();
        client
            .set_foreground_wake_callback(1, Some(new_state.clone()))
            .unwrap();
        old_receipt.queued.lock().unwrap().clear();
        new_receipt.queued.lock().unwrap().clear();
        relay
            .run(move |worker| {
                old_scheduler.schedule_tick(TickUrgency::Immediate);
                worker.clients[&id].db.schedule_tick(TickUrgency::Deferred);
                Ok(())
            })
            .unwrap();
        old_state.inert();
        new_state.inert();
        assert_eq!(
            new_receipt.queued(),
            1,
            "the current registration receives its wake before old host retirement"
        );
    }

    #[test]
    fn structured_foreground_read_progresses_without_blocking_the_owner() {
        // This native-boundary receipt needs the actual owner queue: a Rust
        // client executor would keep polling the read itself and hide a host
        // Tick which blocks the only thread able to resume or cancel it.
        let (done_tx, done_rx) = mpsc::channel();
        let receipt = thread::spawn(move || {
            use jazz::query::{ArraySubquery, ArraySubqueryRequirement};
            let directory = tempfile::tempdir().unwrap();
            let allow = PolicyExpr::True;
            let policies = TablePolicies::new()
                .with_select(allow.clone())
                .with_insert(allow.clone())
                .with_update(Some(allow.clone()), allow.clone())
                .with_delete(allow);
            let mut relay_config = config(
                directory.path().join("structured.sqlite"),
                Some("structured"),
            );
            relay_config.schema = JazzSchema::new(
                &SchemaBuilder::new()
                    .table(
                        TableSchemaBuilder::new("groups")
                            .column("title", ColumnType::Text)
                            .policies(policies.clone()),
                    )
                    .table(
                        TableSchemaBuilder::new("tasks")
                            .column("title", ColumnType::Text)
                            .fk_column("group_id", "groups")
                            .policies(policies.clone()),
                    )
                    .table(
                        TableSchemaBuilder::new("notes")
                            .column("title", ColumnType::Text)
                            .fk_column("task_id", "tasks")
                            .policies(policies),
                    )
                    .build(),
            )
            .unwrap();
            let relay = NativeRelay::spawn(relay_config).unwrap();
            let client = relay
                .attach_client(
                    DbIdentity {
                        node: NodeUuid::from_bytes([0x91; 16]),
                        author: AuthorSubject::for_test_bytes([0x92; 16]),
                    },
                    BTreeMap::new(),
                )
                .unwrap();
            let group = client
                .with_db(|db| {
                    let group = block_on(db.insert(
                        "groups",
                        BTreeMap::from([("title".into(), Value::String("group".into()))]),
                        Default::default(),
                    ))
                    .map_err(RelayError::Db)?
                    .row_uuid();
                    let task = block_on(db.insert(
                        "tasks",
                        BTreeMap::from([
                            ("title".into(), Value::String("task".into())),
                            ("group_id".into(), Value::Uuid(group.0)),
                        ]),
                        Default::default(),
                    ))
                    .map_err(RelayError::Db)?
                    .row_uuid();
                    block_on(db.insert(
                        "notes",
                        BTreeMap::from([
                            ("title".into(), Value::String("note".into())),
                            ("task_id".into(), Value::Uuid(task.0)),
                        ]),
                        Default::default(),
                    ))
                    .map_err(RelayError::Db)?;
                    Ok(group)
                })
                .unwrap();
            let query = Query::from("groups").array_subquery(
                ArraySubquery::new("tasksViaGroup", "tasks", "group_id", "id")
                    .select(["title"])
                    .requirement(ArraySubqueryRequirement::AtLeastOne)
                    .nested(ArraySubquery::new("notesViaTask", "notes", "task_id", "id")),
            );
            let prepared = postcard::to_allocvec(&query).unwrap();
            let mut response = client
                .start_foreground_read(prepared, "{}".into(), None)
                .unwrap();
            let mut turns = 0;
            let bytes = loop {
                match response {
                    ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(bytes)) => {
                        break bytes;
                    }
                    ForegroundOperationPoll::Pending { operation } => {
                        assert!(
                            turns < 512,
                            "structured read must finish through bounded host turns"
                        );
                        relay.pump().unwrap();
                        response = client.poll_foreground_operation(operation).unwrap();
                        turns += 1;
                    }
                    _ => panic!("structured read returned an unexpected response"),
                }
            };
            #[derive(serde::Deserialize)]
            struct Snapshot {
                root_count: u64,
                rows: Vec<DecodedForegroundRowBatch>,
            }
            let snapshot: Snapshot = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(snapshot.root_count, 1);
            assert_eq!(snapshot.rows[0].rows[0].row_id, group);
            client.close().unwrap();
            drop(relay);
            done_tx.send(()).unwrap();
        });
        done_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("native owner must return from Tick and finish the structured read");
        receipt.join().unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn foreground_wakes_leave_alternate_stacks_before_native_callback() {
        // Internal embedding-boundary receipt: row results cannot reveal which
        // native stack invokes a callback. ART derives attachment bounds from
        // pthread metadata, so a scheduler called on stacker's separate segment
        // must defer JNI until the owner operation returns to that stack.
        #[derive(Default)]
        struct Receipt {
            operation_returned: AtomicBool,
            calls: Mutex<Vec<(u8, u64, bool, bool)>>,
        }
        fn on_pthread_stack() -> bool {
            let marker = 0u8;
            unsafe {
                let mut attr = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
                assert_eq!(
                    libc::pthread_getattr_np(libc::pthread_self(), attr.as_mut_ptr()),
                    0
                );
                let mut attr = attr.assume_init();
                let mut base = std::ptr::null_mut();
                let mut size = 0;
                assert_eq!(libc::pthread_attr_getstack(&attr, &mut base, &mut size), 0);
                assert_eq!(libc::pthread_attr_destroy(&mut attr), 0);
                let address = &marker as *const u8 as usize;
                (base as usize..base as usize + size).contains(&address)
            }
        }
        unsafe extern "C" fn capture(context: *mut c_void, _: u64, kind: u8, delay: u64) {
            let receipt = unsafe { &*(context as *const Receipt) };
            receipt.calls.lock().unwrap().push((
                kind,
                delay,
                on_pthread_stack(),
                receipt.operation_returned.load(Ordering::SeqCst),
            ));
        }
        let directory = tempfile::tempdir().unwrap();
        let relay =
            NativeRelay::spawn(config(directory.path().join("stack.sqlite"), Some("stack")))
                .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x44; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let receipt = Arc::new(Receipt::default());
        let state = Arc::new(ForegroundWakeState::new(ForegroundWakeRegistration {
            callback: capture,
            context: Arc::as_ptr(&receipt) as usize,
        }));
        client
            .set_foreground_wake_callback(1, Some(state.clone()))
            .unwrap();
        receipt.calls.lock().unwrap().clear();
        for (urgencies, expected, fail) in [
            (
                vec![TickUrgency::AfterCurrentTurn],
                (FOREGROUND_WAKE_AFTER, 0),
                false,
            ),
            (
                vec![TickUrgency::Deferred],
                (FOREGROUND_WAKE_DEFERRED, 0),
                false,
            ),
            (
                vec![TickUrgency::Immediate, TickUrgency::Deferred],
                (FOREGROUND_WAKE_IMMEDIATE, 0),
                true,
            ),
            (vec![], (FOREGROUND_WAKE_AFTER, 3), false),
        ] {
            receipt.operation_returned.store(false, Ordering::SeqCst);
            let done = receipt.clone();
            let timer_state = state.clone();
            let client_id = client.id;
            let result = relay.run(move |worker| {
                let db = &worker.clients[&client_id].db;
                let generation = worker
                    .foreground_wake_generations
                    .entry(client_id)
                    .or_default()
                    .clone();
                let timer = ForegroundWakeScheduler {
                    wake: timer_state,
                    foreground: 1,
                    pending: worker.pending_foreground_wakes.clone(),
                    expected_generation: generation.load(Ordering::Acquire),
                    generation,
                    owner_wake: OwnerWakeNotifier {
                        commands: worker.owner_commands.clone(),
                        queued: worker.owner_wake_queued.clone(),
                    },
                };
                assert!(on_pthread_stack());
                stacker::grow(8 * 1024 * 1024, || {
                    assert!(
                        !on_pthread_stack(),
                        "control enters a separate stack segment"
                    );
                    timer.schedule_tick_after(20);
                    timer.schedule_tick_after(3);
                    for urgency in urgencies {
                        db.schedule_tick(urgency);
                    }
                    assert!(
                        done.calls.lock().unwrap().is_empty(),
                        "native callback must wait for original stack"
                    );
                });
                done.operation_returned.store(true, Ordering::SeqCst);
                if fail {
                    Err(RelayError::Closed)
                } else {
                    Ok(())
                }
            });
            assert_eq!(result.is_err(), fail);
            assert_eq!(
                *receipt.calls.lock().unwrap(),
                vec![(expected.0, expected.1, true, true)],
                "wake flushes even on operation error, before response and owner park"
            );
            receipt.calls.lock().unwrap().clear();
        }
        // Cancellation after a signal but before flush must make its queued
        // registration inert, including when no later owner command arrives.
        let cancel = state.clone();
        client
            .with_db(move |db| {
                stacker::grow(8 * 1024 * 1024, || {
                    db.schedule_tick(TickUrgency::Immediate);
                    cancel.inert();
                });
                Ok(())
            })
            .unwrap();
        assert!(receipt.calls.lock().unwrap().is_empty());

        // Replacing the same foreground's inert callback cannot leave an old
        // pending Arc suppressing the new registration's signal.
        let replacement = Arc::new(Receipt::default());
        replacement.operation_returned.store(true, Ordering::SeqCst);
        let replacement_state = Arc::new(ForegroundWakeState::new(ForegroundWakeRegistration {
            callback: capture,
            context: Arc::as_ptr(&replacement) as usize,
        }));
        client
            .set_foreground_wake_callback(1, Some(replacement_state.clone()))
            .unwrap();
        replacement.calls.lock().unwrap().clear();
        client
            .with_db(|db| {
                stacker::grow(8 * 1024 * 1024, || db.schedule_tick(TickUrgency::Deferred));
                Ok(())
            })
            .unwrap();
        assert_eq!(
            *replacement.calls.lock().unwrap(),
            vec![(FOREGROUND_WAKE_DEFERRED, 0, true, true)]
        );
        assert!(receipt.calls.lock().unwrap().is_empty());
        replacement.calls.lock().unwrap().clear();
        let stale_state = state.clone();
        let client_id = client.id;
        relay
            .run(move |worker| {
                // A retained Context waker may still own the cancelled registration.
                let generation = worker
                    .foreground_wake_generations
                    .get(&client_id)
                    .expect("foreground registration exists")
                    .clone();
                let stale = ForegroundWakeScheduler {
                    wake: stale_state,
                    foreground: 1,
                    pending: worker.pending_foreground_wakes.clone(),
                    expected_generation: generation.load(Ordering::Acquire) - 1,
                    generation,
                    owner_wake: OwnerWakeNotifier {
                        commands: worker.owner_commands.clone(),
                        queued: worker.owner_wake_queued.clone(),
                    },
                };
                stale.schedule_tick(TickUrgency::Immediate);
                worker.clients[&client_id]
                    .db
                    .schedule_tick(TickUrgency::Deferred);
                Ok(())
            })
            .unwrap();
        assert_eq!(
            *replacement.calls.lock().unwrap(),
            vec![(FOREGROUND_WAKE_DEFERRED, 0, true, true)],
            "stale cancelled scheduler must not suppress a replacement registration's wake"
        );
        replacement_state.inert();
    }

    #[test]
    fn pending_foreground_operation_never_blocks_the_owner_tick() {
        // This is intentionally an internal host-boundary receipt: creating a
        // missing chunk through the public API would require a full routed
        // topology, while the invariant under test is narrower. A pending
        // operation must be retained and polled around ordinary owner ticks,
        // never driven by `block_on` on the owner thread.
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("pending.sqlite"),
            Some("pending"),
        ))
        .expect("relay opens");
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x44; 16]))
                    .expect("OS entropy mints foreground node"),
                BTreeMap::new(),
            )
            .expect("client attaches");
        let ready = Arc::new(AtomicBool::new(false));
        let pending_ready = Arc::clone(&ready);
        let client_id = client.id;
        relay
            .run(move |worker| {
                let future: ForegroundOperationFuture = Box::pin(std::future::poll_fn(move |_| {
                    if pending_ready.load(Ordering::SeqCst) {
                        Poll::Ready(Ok(ForegroundOperationResult::Rows(vec![0x2a])))
                    } else {
                        Poll::Pending
                    }
                }));
                worker
                    .clients
                    .get_mut(&client_id)
                    .expect("attached client remains owner-local")
                    .pending_operations
                    .insert(
                        99,
                        ForegroundPendingOperation {
                            subscription: None,
                            future,
                            finish_on_cancel: false,
                        },
                    );
                Ok(())
            })
            .expect("plant operation on owner");

        assert!(matches!(
            client.poll_foreground_operation(99),
            Ok(ForegroundOperationPoll::Pending { operation: 99 })
        ));
        relay
            .pump()
            .expect("a pending foreground operation cannot starve owner tick");
        ready.store(true, Ordering::SeqCst);
        assert!(matches!(
            client.poll_foreground_operation(99),
            Ok(ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(rows))) if rows == vec![0x2a]
        ));
        assert!(matches!(
            client.poll_foreground_operation(99),
            Err(RelayError::ForegroundCommand(_))
        ));

        // Keep the receipt sensitive to the original regression: these two
        // command paths may create/poll a retained future, but must not call
        // the generic spinning helper while they own the relay thread.
        let source = include_str!("lib.rs");
        let relay_worker = source
            .split_once("impl RelayWorker")
            .expect("relay worker implementation remains present")
            .1;
        for function in [
            "fn start_foreground_read(",
            "fn drain_foreground_subscription(",
        ] {
            let body = relay_worker
                .split_once(function)
                .expect("foreground function remains present")
                .1
                .split("\n    fn ")
                .next()
                .expect("foreground function has a body");
            assert!(
                !body.contains("block_on("),
                "{function} must return pending work rather than spin the owner thread"
            );
        }
    }

    #[test]
    fn relation_read_uses_the_canonical_query_byte_contract() {
        let command = ForegroundDbCommandRequest::All {
            query: vec![0, 1, b't', 0],
            options_json: "{}".into(),
            transaction: None,
        };
        let expected = [2, 4, 0, 1, b't', 0, 2, b'{', b'}', 0];
        assert_eq!(postcard::to_allocvec(&command).unwrap(), expected);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandRequest>(&expected).unwrap(),
            command
        );
    }

    // Deterministic owner contention is a native scheduling boundary that the
    // public TS API cannot hold on demand. The read and subscription still use
    // the real canonical query and native foreground handlers.
    #[test]
    fn preparation_and_subscription_wait_for_contended_foreground_owner() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("prepare-held.sqlite"),
            Some("prepare-held"),
        ))
        .unwrap();
        let client = relay
            .attach_client(
                fresh_client_identity(AuthorSubject::for_test_bytes([0x44; 16])).unwrap(),
                BTreeMap::new(),
            )
            .unwrap();
        let id = client.id;
        relay
            .run(move |worker| {
                let client = worker.foreground_client_mut(id)?;
                let db = Rc::clone(&client.db);
                client.tick = Some(Box::pin(async move {
                    db.hold_node_owner_for_test().await;
                    unreachable!()
                }));
                Ok(())
            })
            .unwrap();
        relay.pump().unwrap();
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        let operation = relay.run(move |worker| {
            let read = worker.start_foreground_read(
                id,
                query.clone(),
                "{}".into(),
                None,
            )?;
            let ForegroundOperationPoll::Pending { operation } = read else {
                panic!("read must await held owner");
            };
            let cancelled =
                worker.subscribe_foreground_query_with_options(
                    id,
                    query.clone(),
                    ReadOpts::default(),
                )?;
            assert!(worker.close_foreground_subscription(id, cancelled)?);
            assert!(!worker.foreground_client(id)?.pending_subscriptions.contains_key(&cancelled));
            let live =
                worker.subscribe_foreground_query_with_options(
                    id,
                    query,
                    ReadOpts::default(),
                )?;
            assert!(matches!(worker.drain_foreground_subscription(id, live)?,
                ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(events)) if events.is_empty()));
            worker.foreground_client_mut(id)?.tick = None;
            Ok((operation, live))
        }).unwrap();
        let (operation, subscription) = operation;
        let mut read_ready = false;
        let mut subscription_ready = false;
        for _ in 0..32 {
            relay.pump().unwrap();
            let (read, subscribed) = relay
                .run(move |worker| {
                    let read = if read_ready {
                        true
                    } else {
                        matches!(
                            worker.poll_foreground_operation(id, operation)?,
                            ForegroundOperationPoll::Ready(ForegroundOperationResult::Rows(_))
                        )
                    };
                    let _ = worker.drain_foreground_subscription(id, subscription)?;
                    let subscribed = worker
                        .foreground_client(id)?
                        .subscriptions
                        .contains_key(&subscription);
                    Ok((read, subscribed))
                })
                .unwrap();
            read_ready = read;
            subscription_ready = subscribed;
            if read_ready && subscription_ready {
                break;
            }
        }
        assert!(read_ready, "read resumes after owner release");
        assert!(subscription_ready, "subscription opens after owner release");
        client.close().unwrap();
    }

    #[test]
    fn linked_identity_storage_handoff_requires_previous_owner_close() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("account.sqlite");
        let registry = NativeRelayRegistry::default();
        let mut alice = config(path.clone(), Some("alice"));
        let mut bob = config(path, Some("bob"));
        let account = jazz::account_registry::AccountId(
            "07070707-0707-0707-0707-070707070707".parse().unwrap(),
        );
        alice.identity.author = AuthorSubject::authenticated("https://issuer.example", "alice")
            .unwrap()
            .with_account(account);
        bob.identity.author = AuthorSubject::authenticated("https://issuer.example", "bob")
            .unwrap()
            .with_account(account);
        let old = registry.open(alice.clone()).unwrap();
        assert!(matches!(
            registry.open(bob.clone()),
            Err(RelayError::StorageAlreadyOwned)
        ));
        registry.close(&alice.scope).unwrap();
        assert!(
            old.pump().is_err(),
            "retained old aliases cannot survive handoff"
        );
        let next = registry.open(bob.clone()).unwrap();
        assert!(next.pump().is_ok());
        registry.close(&bob.scope).unwrap();
    }

    #[test]
    fn relay_shares_one_scope_and_forwards_two_ui_client_writes_upstream() {
        let directory = tempfile::tempdir().unwrap();
        let registry = NativeRelayRegistry::default();
        let first = registry
            .open(config(directory.path().join("alice.sqlite"), Some("alice")))
            .unwrap();
        let same = registry
            .open(config(directory.path().join("alice.sqlite"), Some("alice")))
            .unwrap();
        let other = registry
            .open(config(directory.path().join("bob.sqlite"), Some("bob")))
            .unwrap();
        assert!(Arc::ptr_eq(&first.inner, &same.inner));
        assert!(!Arc::ptr_eq(&first.inner, &other.inner));
        assert!(matches!(
            registry.open(config(directory.path().join("wrong.sqlite"), Some("alice"))),
            Err(RelayError::ScopeConfigurationMismatch)
        ));
        let mut wrong_identity = config(directory.path().join("alice.sqlite"), Some("alice"));
        wrong_identity.identity = DbIdentity {
            node: NodeUuid::from_bytes([0xe1; 16]),
            author: AuthorSubject::for_test_bytes([0xe2; 16]),
        };
        assert!(matches!(
            registry.open(wrong_identity),
            Err(RelayError::ScopeConfigurationMismatch)
        ));
        let mut wrong_schema = config(directory.path().join("alice.sqlite"), Some("alice"));
        wrong_schema.schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("notes").column("body", ColumnType::Text))
                .build(),
        )
        .unwrap();
        assert!(matches!(
            registry.open(wrong_schema),
            Err(RelayError::ScopeConfigurationMismatch)
        ));

        let first_client = first
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xb1; 16]),
                    author: AuthorSubject::for_test_bytes([0xb2; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        let second_client = same
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xc1; 16]),
                    author: AuthorSubject::for_test_bytes([0xc2; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        assert_ne!(first_client.id(), second_client.id());

        first_client
            .with_db(|db| {
                block_on(db.insert(
                    "todos",
                    BTreeMap::from([("title".to_owned(), Value::String("native".to_owned()))]),
                    InsertOptions {
                        row_id: Some(RowUuid::from_bytes([0xd1; 16])),
                        ..Default::default()
                    },
                ))
                .map_err(RelayError::Db)?;
                Ok(())
            })
            .unwrap();
        second_client
            .with_db(|db| {
                block_on(db.insert(
                    "todos",
                    BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
                    InsertOptions {
                        row_id: Some(RowUuid::from_bytes([0xd2; 16])),
                        ..Default::default()
                    },
                ))
                .map_err(RelayError::Db)?;
                Ok(())
            })
            .unwrap();

        // Saturate the stateful upstream transport before the relay tries to
        // forward either commit. Core must retain the unsent protocol state on
        // `TransportError::Backpressure`, then retry it after the host drains.
        let filler = SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        };
        while first.wire().queue_depths().unwrap().1 < NATIVE_RELAY_QUEUE_MAX_MESSAGES {
            first
                .wire()
                .outbound
                .lock()
                .unwrap()
                .push(filler.clone(), "test saturated upstream")
                .unwrap();
        }
        first.pump().unwrap();
        assert_eq!(
            first.wire().queue_depths().unwrap().1,
            NATIVE_RELAY_QUEUE_MAX_MESSAGES,
            "backpressured stateful messages must remain pending instead of displacing the queue",
        );
        while first.wire().queue_depths().unwrap().1 != 0 {
            let drained = first.wire().take_outbound().unwrap();
            assert!(!drained.is_empty());
        }
        first.pump().unwrap();
        let outbound = first.wire().take_outbound().unwrap();
        let forwarded_rows = outbound
            .iter()
            .filter_map(|message| match message {
                SyncMessage::CommitUnit { versions, .. } => Some(versions),
                _ => None,
            })
            .flatten()
            .map(jazz::protocol::VersionRecord::row_uuid)
            .collect::<Vec<_>>();
        assert_eq!(forwarded_rows.len(), 2);
        assert!(forwarded_rows.contains(&RowUuid::from_bytes([0xd1; 16])));
        assert!(forwarded_rows.contains(&RowUuid::from_bytes([0xd2; 16])));
        assert_eq!(
            outbound
                .iter()
                .filter(|message| matches!(message, SyncMessage::CommitUnit { .. }))
                .count(),
            2,
            "each in-memory UI client must reach the shared persistent relay and its upstream"
        );
    }

    #[test]
    fn sibling_ui_subscription_observes_another_ui_client_write() {
        // Internal relay-topology receipt: each UI runtime owns an isolated
        // in-memory Db, so a sibling write is observable only if the ordinary
        // client -> persistent relay -> sibling protocol path is complete.
        let directory = tempfile::tempdir().unwrap();
        let registry = NativeRelayRegistry::default();
        let mut relay_config = config(directory.path().join("shared.sqlite"), Some("alice"));
        relay_config.schema = permissive_schema();
        let relay = registry.open(relay_config).unwrap();
        let writer = relay
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xb1; 16]),
                    author: AuthorSubject::for_test_bytes([0xb2; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        let reader = relay
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xc1; 16]),
                    author: AuthorSubject::for_test_bytes([0xb2; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();

        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        let subscription = reader
            .subscribe_foreground_query_with_options(query, ReadOpts::default())
            .unwrap();
        let reader_id = reader.id();
        for _ in 0..16 {
            relay.pump().unwrap();
        }
        relay
            .run(move |worker| {
                let reader = worker.foreground_client_mut(reader_id)?;
                let stream = reader.subscriptions.get_mut(&subscription).unwrap();
                while stream.try_next_event().is_some() {}
                Ok(())
            })
            .unwrap();

        let expected = RowUuid::from_bytes([0xd1; 16]);
        writer
            .with_db(move |db| {
                block_on(db.insert(
                    "todos",
                    BTreeMap::from([("title".to_owned(), Value::String("native".to_owned()))]),
                    InsertOptions {
                        row_id: Some(expected),
                        ..Default::default()
                    },
                ))
                .map_err(RelayError::Db)?;
                Ok(())
            })
            .unwrap();

        let mut observed = false;
        for _ in 0..32 {
            relay.pump().unwrap();
            observed |= relay
                .run(move |worker| {
                    let reader = worker.foreground_client_mut(reader_id)?;
                    let stream = reader.subscriptions.get_mut(&subscription).unwrap();
                    let mut observed = false;
                    while let Some(event) = stream.try_next_event() {
                        if let SubscriptionEvent::Delta { added, updated, .. } = event {
                            observed |= added
                                .iter()
                                .chain(updated.iter())
                                .any(|row| row.row_uuid() == expected);
                        }
                    }
                    Ok(observed)
                })
                .unwrap();
            if observed {
                break;
            }
        }
        assert!(
            observed,
            "the persistent relay must route a UI write to a sibling UI subscription"
        );
    }

    #[test]
    fn retained_foreground_subscription_wakes_after_its_initial_reset_and_sibling_commit() {
        // This is an internal native-host receipt: public rows cannot establish
        // that the owner scheduled B's platform callback rather than relying on
        // the test's manual relay pumps. It covers the installed JSI sequence
        // where B drains its initial reset, then A commits through a foreground
        // transaction while B's stream remains retained.
        let directory = tempfile::tempdir().unwrap();
        let registry = NativeRelayRegistry::default();
        let mut relay_config = config(
            directory.path().join("wake-after-reset.sqlite"),
            Some("alice"),
        );
        relay_config.schema = permissive_schema();
        let relay = registry.open(relay_config).unwrap();
        let writer = relay
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xb3; 16]),
                    author: AuthorSubject::for_test_bytes([0xb4; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        let reader = relay
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0xc3; 16]),
                    author: AuthorSubject::for_test_bytes([0xb4; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        let reader_wake = Arc::new(QueuedNativeWake::active());
        reader
            .set_foreground_wake_callback(
                2,
                Some(Arc::new(ForegroundWakeState::new(
                    ForegroundWakeRegistration {
                        callback: queue_native_wake,
                        context: Arc::as_ptr(&reader_wake) as usize,
                    },
                ))),
            )
            .unwrap();

        let subscription = reader
            .subscribe_foreground_query_with_options(
                postcard::to_allocvec(&Query::from("todos")).unwrap(),
                ReadOpts::default(),
            )
            .unwrap();
        for _ in 0..16 {
            relay.pump().unwrap();
        }
        let initial = reader.drain_foreground_subscription(subscription).unwrap();
        assert!(
            matches!(
                initial,
                ForegroundOperationPoll::Ready(ForegroundOperationResult::SubscriptionEvents(_))
            ),
            "the initial reset drains without retiring the retained subscription"
        );
        reader_wake.queued.lock().unwrap().clear();

        let row = RowUuid::from_bytes([0xd3; 16]);
        let transaction = writer
            .begin_foreground_transaction(ForegroundTransactionKind::Mergeable)
            .unwrap();
        assert_eq!(
            writer
                .insert_foreground_transaction(
                    transaction,
                    "todos".into(),
                    encoded_title_cells("wake after reset"),
                    Some(*row.as_bytes()),
                )
                .unwrap(),
            row
        );
        writer.commit_foreground_transaction(transaction).unwrap();

        for _ in 0..32 {
            relay.pump().unwrap();
            if reader_wake.wait_for_queued(1) {
                break;
            }
        }
        assert!(
            reader_wake.wait_for_queued(1),
            "the retained reader subscription must schedule a post-commit native wake"
        );
        let wakes = reader_wake.queued.lock().unwrap();
        assert!(
            wakes.iter().any(|(foreground, _, _)| *foreground == 2),
            "the post-commit wake targets the retained reader foreground"
        );
    }

    #[test]
    fn abi_handshake_accepts_supported_versions_before_storage_opens() {
        assert_eq!(
            ensure_native_relay_abi_compatible(NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_V1,
                maximum: NATIVE_RELAY_ABI_V1,
            })
            .unwrap(),
            NATIVE_RELAY_ABI_V1
        );
        assert_eq!(
            NativeRelay::ensure_abi_compatible(NativeRelayAbiRange {
                minimum: 0,
                maximum: NATIVE_RELAY_ABI_V1,
            })
            .unwrap(),
            NATIVE_RELAY_ABI_V1
        );
    }

    #[test]
    fn abi_handshake_rejects_invalid_and_incompatible_ranges() {
        assert!(matches!(
            ensure_native_relay_abi_compatible(NativeRelayAbiRange {
                minimum: 2,
                maximum: 1,
            }),
            Err(RelayError::InvalidAbiRange {
                minimum: 2,
                maximum: 1,
            })
        ));
        assert!(matches!(
            ensure_native_relay_abi_compatible(NativeRelayAbiRange {
                minimum: NATIVE_RELAY_ABI_V1.saturating_add(1),
                maximum: u16::MAX,
            }),
            Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_V1
        ));
    }

    #[test]
    fn incompatible_open_creates_no_relay_or_sqlite_store() {
        let directory = tempfile::tempdir().unwrap();
        let sqlite_path = directory.path().join("must-not-exist.sqlite");
        let mut open = config(sqlite_path.clone(), Some("alice"));
        open.supported_abi = NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1.saturating_add(1),
            maximum: u16::MAX,
        };
        let registry = NativeRelayRegistry::default();

        assert!(matches!(
            registry.open(open),
            Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_V1
        ));
        assert!(
            !sqlite_path.exists(),
            "ABI rejection must happen before SQLite creates a database"
        );
        assert!(
            registry.relays.lock().unwrap().is_empty(),
            "ABI rejection must not register a partially-open relay"
        );
    }

    #[test]
    fn direct_incompatible_spawn_creates_no_owner_thread_or_sqlite_store() {
        let directory = tempfile::tempdir().unwrap();
        let sqlite_path = directory.path().join("must-not-exist-direct.sqlite");
        let mut open = config(sqlite_path.clone(), Some("alice"));
        open.supported_abi = NativeRelayAbiRange {
            minimum: NATIVE_RELAY_ABI_V1.saturating_add(1),
            maximum: u16::MAX,
        };
        let threads_started = Arc::new(AtomicUsize::new(0));
        open.thread_start_counter = Some(Arc::clone(&threads_started));

        assert!(matches!(
            NativeRelay::spawn(open),
            Err(RelayError::IncompatibleAbi { native, .. }) if native == NATIVE_RELAY_ABI_V1
        ));
        assert_eq!(threads_started.load(Ordering::Relaxed), 0);
        assert!(
            !sqlite_path.exists(),
            "ABI rejection must happen before SQLite creates a database"
        );
    }

    #[test]
    fn encoded_peer_messages_use_the_shared_postcard_contract() {
        let wire = NativeRelayWire::default();
        let message = SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
        };
        let bytes = encode_sync_message(&message).unwrap();

        wire.push_inbound_encoded(&bytes).unwrap();
        assert_eq!(wire.inbound.lock().unwrap().pop(), Some(message.clone()));

        wire.outbound
            .lock()
            .unwrap()
            .push(message, "test outbound")
            .unwrap();
        let encoded = wire.take_outbound_encoded().unwrap();
        assert_eq!(encoded.len(), 1);
        assert_eq!(wire.outbound.lock().unwrap().len(), 0);
        assert_eq!(
            decode_sync_message(&encoded[0]).unwrap(),
            SyncMessage::SessionClaims {
                identity: AuthorSubject::SYSTEM,
                claims: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
            }
        );
    }

    #[test]
    fn native_upstream_bridge_uses_the_negotiated_wire_adapter_in_both_directions() {
        let outbound = SyncMessage::SessionClaims {
            identity: AuthorSubject::for_test_bytes([0x61; 16]),
            claims: BTreeMap::new(),
        };
        let inbound = SyncMessage::SessionClaims {
            identity: AuthorSubject::for_test_bytes([0x62; 16]),
            claims: BTreeMap::new(),
        };
        let relay_wire = NativeRelayWire::default();
        relay_wire
            .outbound
            .lock()
            .unwrap()
            .push(outbound.clone(), "test relay outbound")
            .unwrap();

        // A peer adapter produces a real framed network payload. The relay
        // bridge receives that frame only through another adapter; it cannot
        // accidentally accept an unframed postcard message as a second wire.
        let mut peer = WireTransportAdapter::current(TestWireTransport::default());
        peer.send(inbound.clone()).unwrap();
        let peer_wire = peer.into_inner();
        let mut upstream = WireTransportAdapter::current(TestWireTransport {
            inbound: peer_wire.outbound.into(),
            outbound: Vec::new(),
        });

        assert!(bridge_native_relay_wire_once(&relay_wire, &mut upstream).unwrap());
        assert_eq!(relay_wire.inbound.lock().unwrap().pop(), Some(inbound));

        let sent_to_edge = upstream.into_inner().outbound;
        assert_eq!(sent_to_edge.len(), 1);
        let mut edge = WireTransportAdapter::current(TestWireTransport {
            inbound: sent_to_edge.into(),
            outbound: Vec::new(),
        });
        assert_eq!(edge.try_recv(), Some(outbound));
    }

    // Internal queue seam: a host cannot deliberately block the relay owner.
    #[test]
    fn review_socket_install_recovers_after_owner_queue_saturation() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("socket-capacity.sqlite"),
            Some("socket-capacity"),
        ))
        .unwrap();
        let saturated = saturate_owner(&relay);
        let (events_tx, events_rx) = mpsc::channel();
        let connector = Arc::new(ReconnectingTestConnector {
            calls: AtomicUsize::new(0),
            bearer_seen: Arc::new(Mutex::new(Vec::new())),
        });
        let worker = NativeRelaySocketWorker::start_with_connector(
            relay,
            NativeRelaySocketConfig {
                server_url: "https://edge.example".into(),
                app_id: AppId::from_name("socket-capacity"),
                peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
                auth: AuthConfig {
                    jwt_token: Some("edge-validated-bearer".into()),
                    ..AuthConfig::default()
                },
                reconnect_delay: std::time::Duration::from_millis(1),
                on_event: Arc::new(move |event| {
                    let _ = events_tx.send(event);
                }),
            },
            connector.clone(),
        )
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while connector.calls.load(Ordering::Acquire) < 1 {
            if std::time::Instant::now() >= deadline {
                saturated.release_and_wait();
                panic!("connector did not reach installation");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let premature_event = events_rx.recv_timeout(Duration::from_millis(20));
        saturated.release_and_wait();
        assert!(matches!(
            premature_event,
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        let mut connected = false;
        for _ in 0..8 {
            match events_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap()
            {
                NativeRelaySocketEvent::Connected => {
                    connected = true;
                    break;
                }
                NativeRelaySocketEvent::Stopped => break,
                _ => {}
            }
        }
        worker.cancel();
        assert!(
            connected,
            "transient owner backpressure must not permanently stop socket installation"
        );
    }

    #[test]
    fn socket_install_queue_retry_cancels_before_admission() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("socket-capacity.sqlite"),
            Some("socket-capacity"),
        ))
        .unwrap();
        let saturated = saturate_owner(&relay);
        let (events_tx, events_rx) = mpsc::channel();
        let connector = Arc::new(ReconnectingTestConnector {
            calls: AtomicUsize::new(0),
            bearer_seen: Arc::new(Mutex::new(Vec::new())),
        });
        let worker = NativeRelaySocketWorker::start_with_connector(
            relay.clone(),
            NativeRelaySocketConfig {
                server_url: "https://edge.example".into(),
                app_id: AppId::from_name("socket-capacity"),
                peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
                auth: AuthConfig {
                    jwt_token: Some("edge-validated-bearer".into()),
                    ..AuthConfig::default()
                },
                reconnect_delay: std::time::Duration::from_millis(1),
                on_event: Arc::new(move |event| {
                    let _ = events_tx.send(event);
                }),
            },
            connector.clone(),
        )
        .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while connector.calls.load(Ordering::Acquire) < 1 {
            if std::time::Instant::now() >= deadline {
                saturated.release_and_wait();
                panic!("connector did not reach installation");
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        worker.cancel();
        let stopped = events_rx.recv_timeout(Duration::from_secs(1));
        saturated.release_and_wait();
        assert_eq!(stopped.unwrap(), NativeRelaySocketEvent::Stopped);
        assert_eq!(relay.run(|worker| Ok(worker.socket_generation)).unwrap(), 0);
    }

    #[test]
    fn closed_bridge_uses_its_owned_terminal_before_reconnecting() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("closed-bridge.sqlite"),
            Some("closed-bridge"),
        ))
        .unwrap();
        let (events_tx, events_rx) = mpsc::channel();
        let connector = Arc::new(ClosedPumpTestConnector {
            calls: AtomicUsize::new(0),
            first_terminal: Arc::new(Mutex::new(None)),
        });
        let worker = NativeRelaySocketWorker::start_with_connector(
            relay.clone(),
            NativeRelaySocketConfig {
                server_url: "https://edge.example".into(),
                app_id: AppId::from_name("closed-bridge"),
                peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
                auth: AuthConfig {
                    jwt_token: Some("edge-validated-bearer".into()),
                    ..AuthConfig::default()
                },
                reconnect_delay: std::time::Duration::ZERO,
                on_event: Arc::new(move |event| {
                    let _ = events_tx.send(event);
                }),
            },
            connector.clone(),
        )
        .unwrap();
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::Connected
        );
        relay
            .run(|worker| {
                worker
                    .socket_wire
                    .as_ref()
                    .expect("first socket is installed")
                    .outbound
                    .lock()
                    .unwrap()
                    .push(
                        SyncMessage::SessionClaims {
                            identity: AuthorSubject::SYSTEM,
                            claims: BTreeMap::new(),
                        },
                        "test closed bridge",
                    )
            })
            .unwrap();
        let terminal = connector
            .first_terminal
            .lock()
            .unwrap()
            .take()
            .expect("first connector published its terminal sender");
        terminal
            .send(NativeTransportTerminal::Failed(
                NativeTransportError::Terminal("malformed peer wire batch".into()),
            ))
            .unwrap();
        assert!(matches!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::TerminalError(error)
                if error.contains("malformed peer wire batch")
        ));
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::Reconnecting
        );
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::Connected
        );
        worker.cancel();
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::Stopped
        );
    }

    #[test]
    fn cancellation_does_not_wait_for_a_closed_bridge_terminal() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("closed-bridge-cancel.sqlite"),
            Some("closed-bridge-cancel"),
        ))
        .unwrap();
        let (events_tx, events_rx) = mpsc::channel();
        let connector = Arc::new(ClosedPumpTestConnector {
            calls: AtomicUsize::new(0),
            first_terminal: Arc::new(Mutex::new(None)),
        });
        let worker = NativeRelaySocketWorker::start_with_connector(
            relay.clone(),
            NativeRelaySocketConfig {
                server_url: "https://edge.example".into(),
                app_id: AppId::from_name("closed-bridge-cancel"),
                peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
                auth: AuthConfig {
                    jwt_token: Some("edge-validated-bearer".into()),
                    ..AuthConfig::default()
                },
                reconnect_delay: std::time::Duration::ZERO,
                on_event: Arc::new(move |event| {
                    let _ = events_tx.send(event);
                }),
            },
            connector,
        )
        .unwrap();
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::Connected
        );
        relay
            .run(|worker| {
                worker
                    .socket_wire
                    .as_ref()
                    .expect("first socket is installed")
                    .outbound
                    .lock()
                    .unwrap()
                    .push(
                        SyncMessage::SessionClaims {
                            identity: AuthorSubject::SYSTEM,
                            claims: BTreeMap::new(),
                        },
                        "test closed bridge cancellation",
                    )
            })
            .unwrap();
        worker.cancel();
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::Stopped
        );
    }

    #[test]
    fn owner_close_is_terminal_and_never_resurrects_the_socket() {
        let directory = tempfile::tempdir().unwrap();
        let relay = NativeRelay::spawn(config(
            directory.path().join("owner-close.sqlite"),
            Some("owner-close"),
        ))
        .unwrap();
        let (events_tx, events_rx) = mpsc::channel();
        let connector = Arc::new(PendingTestConnector {
            calls: AtomicUsize::new(0),
        });
        let worker = NativeRelaySocketWorker::start_with_connector(
            relay.clone(),
            NativeRelaySocketConfig {
                server_url: "https://edge.example".into(),
                app_id: AppId::from_name("owner-close"),
                peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
                auth: AuthConfig {
                    jwt_token: Some("edge-validated-bearer".into()),
                    ..AuthConfig::default()
                },
                reconnect_delay: std::time::Duration::ZERO,
                on_event: Arc::new(move |event| {
                    let _ = events_tx.send(event);
                }),
            },
            connector.clone(),
        )
        .unwrap();
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::Connected
        );
        relay.inner.shutdown().unwrap();
        assert!(matches!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::TerminalError(error) if error.contains("native relay is closed")
        ));
        assert_eq!(
            events_rx.recv_timeout(Duration::from_secs(1)).unwrap(),
            NativeRelaySocketEvent::Stopped
        );
        assert_eq!(connector.calls.load(Ordering::Acquire), 1);
        drop(worker);
    }

    #[test]
    fn native_socket_worker_reauthenticates_on_reconnect_and_cancels_cleanly() {
        let directory = tempfile::tempdir().unwrap();
        let relay =
            NativeRelay::spawn(config(directory.path().join("relay.sqlite"), Some("alice")))
                .unwrap();
        let (events_tx, events_rx) = mpsc::channel();
        let bearer_seen = Arc::new(Mutex::new(Vec::new()));
        let connector = Arc::new(ReconnectingTestConnector {
            calls: AtomicUsize::new(0),
            bearer_seen: Arc::clone(&bearer_seen),
        });
        let worker = NativeRelaySocketWorker::start_with_connector(
            relay,
            NativeRelaySocketConfig {
                server_url: "https://edge.example".to_owned(),
                app_id: AppId::from_name("native-relay-socket-test"),
                peer_identity: AuthorSubject::for_test_bytes([0x63; 16]),
                auth: AuthConfig {
                    jwt_token: Some("edge-validated-bearer".to_owned()),
                    ..AuthConfig::default()
                },
                reconnect_delay: std::time::Duration::ZERO,
                on_event: Arc::new(move |event| {
                    let _ = events_tx.send(event);
                }),
            },
            connector,
        )
        .unwrap();

        assert_eq!(
            events_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap(),
            NativeRelaySocketEvent::Connected
        );
        assert_eq!(
            events_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap(),
            NativeRelaySocketEvent::Reconnecting
        );
        assert_eq!(
            events_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap(),
            NativeRelaySocketEvent::Connected
        );
        worker.cancel();
        assert_eq!(
            events_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .unwrap(),
            NativeRelaySocketEvent::Stopped
        );
        drop(worker);
        assert_eq!(
            bearer_seen.lock().unwrap().as_slice(),
            ["edge-validated-bearer", "edge-validated-bearer"],
            "each reconnect sends the bearer to Edge again, without exposing it to relay state"
        );
    }

    #[test]
    fn client_transport_keeps_the_wire_boundary_opaque_and_directional() {
        let directory = tempfile::tempdir().unwrap();
        let relay =
            NativeRelay::spawn(config(directory.path().join("relay.sqlite"), Some("alice")))
                .unwrap();
        let client = relay
            .attach_client(
                DbIdentity {
                    node: NodeUuid::from_bytes([0x91; 16]),
                    author: AuthorSubject::for_test_bytes([0x92; 16]),
                },
                BTreeMap::new(),
            )
            .unwrap();
        let frame = encode_sync_message(&SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        })
        .unwrap();

        client.wire().push_inbound_encoded(&frame).unwrap();
        assert_eq!(client.wire().queue_depths().unwrap(), (1, 0));
        relay.pump().unwrap();
        assert_eq!(client.wire().queue_depths().unwrap().0, 0);
    }

    #[test]
    fn encoded_peer_messages_reject_invalid_bytes() {
        assert!(matches!(
            NativeRelayWire::default().push_inbound_encoded(&[0xff]),
            Err(RelayError::DecodePeerMessage(_))
        ));
    }

    #[test]
    fn encoded_peer_messages_reject_the_exact_logical_message_limit_boundary() {
        let bytes = vec![0; MAX_LOGICAL_MESSAGE_BYTES + 1];
        assert!(matches!(
            NativeRelayWire::default().push_inbound_encoded(&bytes),
            Err(RelayError::PeerMessageTooLarge(message))
                if message.contains(&(MAX_LOGICAL_MESSAGE_BYTES + 1).to_string())
        ));
    }

    #[test]
    fn native_queue_applies_count_backpressure_without_losing_admitted_messages() {
        let message = SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        };
        let wire = NativeRelayWire::default();
        for _ in 0..NATIVE_RELAY_QUEUE_MAX_MESSAGES {
            wire.push_inbound(message.clone()).unwrap();
        }
        assert!(matches!(
            wire.push_inbound(message),
            Err(RelayError::QueueCapacityExceeded {
                queued_messages: NATIVE_RELAY_QUEUE_MAX_MESSAGES,
                ..
            })
        ));
        assert_eq!(
            wire.queue_depths().unwrap().0,
            NATIVE_RELAY_QUEUE_MAX_MESSAGES
        );
    }

    #[test]
    fn native_queue_drains_in_bounded_batches() {
        let wire = NativeRelayWire::default();
        for _ in 0..(NATIVE_RELAY_DRAIN_MAX_MESSAGES + 1) {
            wire.outbound
                .lock()
                .unwrap()
                .push(
                    SyncMessage::SessionClaims {
                        identity: AuthorSubject::SYSTEM,
                        claims: BTreeMap::new(),
                    },
                    "test outbound",
                )
                .unwrap();
        }
        assert_eq!(
            wire.take_outbound_encoded().unwrap().len(),
            NATIVE_RELAY_DRAIN_MAX_MESSAGES
        );
        assert_eq!(wire.queue_depths().unwrap().1, 1);
    }

    #[test]
    fn pump_client_selection_is_bounded_and_round_robins() {
        let clients = (1..=(NATIVE_RELAY_PUMP_MAX_CLIENTS as u64 + 3))
            .map(|id| (id, ()))
            .collect::<BTreeMap<_, _>>();
        let first = bounded_round_robin_ids(&clients, None);
        assert_eq!(first.len(), NATIVE_RELAY_PUMP_MAX_CLIENTS);
        assert_eq!(first[0], 1);
        let second = bounded_round_robin_ids(&clients, first.last().copied());
        assert_eq!(&second[..3], &[65, 66, 67]);
        assert_eq!(second[3], 1);
    }

    #[test]
    fn c_host_lifecycle_open_attach_close_and_bounded_pump_are_handle_safe() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        assert!(!host.is_null());
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0x71; 16]),
            author: AuthorSubject::for_test_bytes([0x72; 16]),
        };
        let admission = RelayScopeAdmissionRequest {
            scope: RelayScopeRequest {
                app_namespace: "host-receipt".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some("opaque-subject".to_owned()),
            },
            sqlite_path: directory.path().join("host.sqlite").display().to_string(),
            schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
            identity,
            claims: BTreeMap::new(),
        };
        let admitted_scope = unsafe { (*host).inner.lock().unwrap().admit_scope(admission) }
            .expect("test admission is valid");
        let open = RelayCommandRequest::Open {
            supported_abi_minimum: NATIVE_RELAY_ABI_V1,
            supported_abi_maximum: NATIVE_RELAY_ABI_V1,
            admitted_scope,
        };
        unsafe fn command(
            host: *mut JazzNativeRelayHost,
            request: RelayCommandRequest,
        ) -> Result<RelayCommandResponse, JazzNativeRelayStatus> {
            let bytes = postcard::to_allocvec(&request).unwrap();
            let mut output = JazzNativeRelayBytes::EMPTY;
            let status = unsafe {
                jazz_native_relay_host_execute(host, bytes.as_ptr(), bytes.len(), &mut output)
            };
            if !matches!(status, JazzNativeRelayStatus::Ok) {
                return Err(status);
            }
            let response = postcard::from_bytes(unsafe {
                std::slice::from_raw_parts(output.data, output.len)
            })
            .unwrap();
            unsafe { jazz_native_relay_bytes_free(&mut output) };
            Ok(response)
        }
        let relay = match unsafe { command(host, open.clone()) }.unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected open response: {response:?}"),
        };
        let alias = match unsafe { command(host, open.clone()) }.unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected alias response: {response:?}"),
        };
        assert_ne!(relay, alias);
        unsafe {
            let host = (*host).inner.lock().unwrap();
            assert!(Arc::ptr_eq(
                &host.relays.get(&relay).unwrap().relay.inner,
                &host.relays.get(&alias).unwrap().relay.inner,
            ));
        }
        let client = match unsafe { command(host, RelayCommandRequest::Attach { relay }) }.unwrap()
        {
            RelayCommandResponse::Attached { client } => client,
            response => panic!("unexpected attach response: {response:?}"),
        };
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay }) },
            Ok(RelayCommandResponse::Pumped)
        ));
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay }) }.unwrap(),
            RelayCommandResponse::Closed { closed: true }
        );
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay: alias }) },
            Ok(RelayCommandResponse::Pumped)
        ));
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseClient { client }) }.unwrap(),
            RelayCommandResponse::Closed { closed: false }
        );
        let reopened = match unsafe { command(host, open) }.unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected reopen response: {response:?}"),
        };
        unsafe {
            let host = (*host).inner.lock().unwrap();
            assert!(Arc::ptr_eq(
                &host.relays.get(&alias).unwrap().relay.inner,
                &host.relays.get(&reopened).unwrap().relay.inner,
            ));
        }
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay }) }.unwrap(),
            RelayCommandResponse::Closed { closed: false }
        );
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay }) },
            Err(JazzNativeRelayStatus::InvalidHandle)
        ));
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay: alias }) }.unwrap(),
            RelayCommandResponse::Closed { closed: true }
        );
        assert!(matches!(
            unsafe { command(host, RelayCommandRequest::Pump { relay: reopened }) },
            Ok(RelayCommandResponse::Pumped)
        ));
        assert_eq!(
            unsafe { command(host, RelayCommandRequest::CloseRelay { relay: reopened }) }.unwrap(),
            RelayCommandResponse::Closed { closed: true }
        );
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn c_host_open_rejects_wrapper_abi_before_storage_and_resets_output() {
        let directory = tempfile::tempdir().unwrap();
        let sqlite_path = directory.path().join("must-not-open.sqlite");
        let host = jazz_native_relay_host_new();
        let threads_started = Arc::new(AtomicUsize::new(0));
        unsafe {
            (*host).inner.lock().unwrap().thread_start_counter = Some(Arc::clone(&threads_started))
        };
        let admitted_scope = unsafe {
            (*host)
                .inner
                .lock()
                .unwrap()
                .admit_scope(RelayScopeAdmissionRequest {
                    scope: RelayScopeRequest {
                        app_namespace: "abi-rejection".to_owned(),
                        storage_namespace: "default".to_owned(),
                        auth_scope: Some("opaque-subject".to_owned()),
                    },
                    sqlite_path: sqlite_path.display().to_string(),
                    schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                    identity: DbIdentity {
                        node: NodeUuid::from_bytes([0x81; 16]),
                        author: AuthorSubject::for_test_bytes([0x82; 16]),
                    },
                    claims: BTreeMap::new(),
                })
                .unwrap()
        };
        let request = |minimum, maximum| RelayCommandRequest::Open {
            supported_abi_minimum: minimum,
            supported_abi_maximum: maximum,
            admitted_scope,
        };
        for (request, expected) in [
            (request(3, 2), JazzNativeRelayStatus::InvalidAbiRange),
            (request(2, 2), JazzNativeRelayStatus::IncompatibleAbi),
        ] {
            let encoded = postcard::to_allocvec(&request).unwrap();
            let mut output = JazzNativeRelayBytes {
                data: std::ptr::dangling_mut(),
                len: 99,
            };
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_execute(
                        host,
                        encoded.as_ptr(),
                        encoded.len(),
                        &mut output,
                    )
                },
                expected
            );
            assert!(output.data.is_null());
            assert_eq!(output.len, 0);
            assert!(!sqlite_path.exists());
            assert_eq!(threads_started.load(Ordering::Relaxed), 0);
        }
        let mut output = JazzNativeRelayBytes {
            data: std::ptr::dangling_mut(),
            len: 77,
        };
        assert_eq!(
            unsafe {
                jazz_native_relay_host_execute(
                    std::ptr::null_mut(),
                    std::ptr::null(),
                    1,
                    &mut output,
                )
            },
            JazzNativeRelayStatus::InvalidArgument
        );
        assert!(output.data.is_null());
        assert_eq!(output.len, 0);
        unsafe { jazz_native_relay_host_free(host) };
    }

    // This is necessarily an internal ABI-boundary test: Kotlin/Swift do not
    // run in this Rust target, but the production C entry proves their only
    // permitted admission path rejects malformed/credential-bearing config
    // before a JavaScript-visible capability exists.
    #[test]
    fn trusted_json_admission_is_strict_bounded_and_never_echoes_config() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0x91; 16]),
            author: AuthorSubject::for_test_bytes([0x92; 16]),
        };
        let request = |claims: BTreeMap<String, Value>| {
            serde_json::json!({
                "scope": {
                    "app_namespace": "trusted-host",
                    "storage_namespace": "primary",
                    "auth_scope": "opaque-validated-subject",
                },
                "sqlite_path": directory.path().join("trusted.sqlite").display().to_string(),
                "schema_json": serde_json::to_string(schema().public_schema()).unwrap(),
                "identity": serde_json::to_value(identity).unwrap(),
                "claims": serde_json::to_value(claims).unwrap(),
            })
        };

        // The platform-owned C ABI is the only production path that can mint
        // a capability. Exercise the JSON forms serde would otherwise map to
        // `Option::None` here, and prove they fail before either registry can
        // change. This is intentionally an ABI-boundary test rather than a
        // Rust-only `RelayScope` unit test.
        for (case, auth_scope) in [
            ("omitted", None),
            ("null", Some(serde_json::Value::Null)),
            ("empty", Some(serde_json::Value::String(String::new()))),
            (
                "whitespace",
                Some(serde_json::Value::String(" \t\n ".to_owned())),
            ),
        ] {
            let mut rejected = request(BTreeMap::new());
            let scope = rejected["scope"]
                .as_object_mut()
                .expect("trusted request has an object scope");
            match auth_scope {
                Some(auth_scope) => {
                    scope.insert("auth_scope".to_owned(), auth_scope);
                }
                None => {
                    scope.remove("auth_scope");
                }
            }
            let rejected = serde_json::to_vec(&rejected).unwrap();
            let mut output = JazzNativeRelayBytes {
                data: std::ptr::dangling_mut(),
                len: 99,
            };
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_admit_scope_json(
                        host,
                        rejected.as_ptr(),
                        rejected.len(),
                        &mut output,
                    )
                },
                JazzNativeRelayStatus::LifecycleFailure,
                "{case} auth scope must fail closed",
            );
            assert!(output.data.is_null(), "{case} must not return a capability");
            assert_eq!(output.len, 0, "{case} must not return a capability");
            let host_state = unsafe { (*host).inner.lock().unwrap() };
            assert!(
                host_state.admitted_scopes.is_empty(),
                "{case} must not mutate the admission registry"
            );
            assert!(host_state.relays.is_empty(), "{case} must not open a relay");
        }

        let encoded = serde_json::to_vec(&request(BTreeMap::from([(
            "role".to_owned(),
            Value::String("member".to_owned()),
        )])))
        .unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    host,
                    encoded.as_ptr(),
                    encoded.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert_eq!(output.len, 32, "only the opaque capability crosses out");
        assert_ne!(
            unsafe { std::slice::from_raw_parts(output.data, output.len) },
            encoded.as_slice(),
            "the trusted config must never be reflected to JavaScript"
        );
        unsafe { jazz_native_relay_bytes_free(&mut output) };

        let mut unknown = request(BTreeMap::new());
        unknown["unexpected"] = serde_json::Value::Bool(true);
        let unknown = serde_json::to_vec(&unknown).unwrap();
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    host,
                    unknown.as_ptr(),
                    unknown.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::InvalidCommand
        );

        let mut bearer = request(BTreeMap::from([(
            "access_token".to_owned(),
            Value::String("never-persist-a-bearer".to_owned()),
        )]));
        // Keep this otherwise-valid request outside the already-admitted
        // scope, so rejection proves the credential filter rather than the
        // immutable-scope configuration check below.
        bearer["scope"]["auth_scope"] =
            serde_json::Value::String("different-valid-subject".to_owned());
        let bearer = serde_json::to_vec(&bearer).unwrap();
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    host,
                    bearer.as_ptr(),
                    bearer.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::LifecycleFailure
        );

        let oversized = vec![b'x'; NATIVE_RELAY_ADMISSION_MAX_BYTES + 1];
        assert_eq!(
            unsafe {
                jazz_native_relay_host_admit_scope_json(
                    host,
                    oversized.as_ptr(),
                    oversized.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::InvalidArgument
        );
        assert!(output.data.is_null());
        assert_eq!(output.len, 0);
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn trusted_admission_rejects_conflicting_scope_before_open() {
        let directory = tempfile::tempdir().unwrap();
        let mut host = NativeRelayHost::default();
        let admission = |sqlite_path: &str,
                         schema_json: String,
                         identity: DbIdentity,
                         claims: BTreeMap<String, Value>| {
            RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "trusted-host".to_owned(),
                    storage_namespace: "primary".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: sqlite_path.to_owned(),
                schema_json,
                identity,
                claims,
            }
        };
        let primary = directory
            .path()
            .join("primary.sqlite")
            .display()
            .to_string();
        let schema_json = serde_json::to_string(schema().public_schema()).unwrap();
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: AuthorSubject::for_test_bytes([0xa2; 16]),
        };
        let claims = BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]);
        host.admit_scope(admission(
            &primary,
            schema_json.clone(),
            identity,
            claims.clone(),
        ))
        .unwrap();

        let other_path = directory.path().join("other.sqlite").display().to_string();
        let other_schema = JazzSchema::new(
            &SchemaBuilder::new()
                .table(TableSchemaBuilder::new("other").column("title", ColumnType::Text))
                .build(),
        )
        .unwrap();
        let other_identity = DbIdentity {
            node: NodeUuid::from_bytes([0xb1; 16]),
            author: AuthorSubject::for_test_bytes([0xb2; 16]),
        };
        for (label, request) in [
            (
                "SQLite path",
                admission(&other_path, schema_json.clone(), identity, claims.clone()),
            ),
            (
                "schema",
                admission(
                    &primary,
                    serde_json::to_string(other_schema.public_schema()).unwrap(),
                    identity,
                    claims.clone(),
                ),
            ),
            (
                "durable identity",
                admission(
                    &primary,
                    schema_json.clone(),
                    other_identity,
                    claims.clone(),
                ),
            ),
            (
                "validated claims",
                admission(
                    &primary,
                    schema_json.clone(),
                    identity,
                    BTreeMap::from([("role".to_owned(), Value::String("admin".to_owned()))]),
                ),
            ),
        ] {
            assert_eq!(
                host.admit_scope(request),
                Err(JazzNativeRelayStatus::LifecycleFailure),
                "a scope cannot mint a second capability with changed trusted {label}"
            );
            assert_eq!(
                host.admitted_scopes.len(),
                1,
                "failed {label} admission must not leave a usable capability"
            );
        }
    }

    #[test]
    fn relay_scope_requires_a_nonempty_authenticated_scope() {
        let directory = tempfile::tempdir().unwrap();
        let mut relay = config(directory.path().join("empty-auth.sqlite"), Some("   "));

        assert!(matches!(relay.validate(), Err(RelayError::InvalidScope(_))));

        relay.scope.auth_scope = None;
        assert!(matches!(relay.validate(), Err(RelayError::InvalidScope(_))));
    }

    #[test]
    fn clean_foreground_handoff_does_not_repeat_synchronous_transaction_ids() {
        // The binding C ABI reserves transaction identities synchronously,
        // before taking the node lock. Exercise that actual path across the
        // host's clean node-lease handoff, rather than only reading its HLC.
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let capability = fixture.admit(
            &directory.path().join("handoff-reservation.sqlite"),
            "handoff-reservation",
            &permissive_schema(),
            0xc1,
        );
        let first = fixture.open_foreground(&capability);
        let first_tx = fixture.insert_todo(first, [0xd1; 16], "first lease holder");
        assert!(matches!(
            fixture.execute(first, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        ));
        let second = fixture.open_foreground(&capability);
        let second_tx = fixture.insert_todo(second, [0xd2; 16], "second lease holder");
        assert_ne!(
            first_tx, second_tx,
            "a cleanly reused foreground node must reserve a fresh public transaction identity"
        );
        assert!(matches!(
            fixture.execute(second, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        ));
    }

    #[test]
    fn clean_foreground_handoff_reuses_one_node_only_after_advancing_its_hlc() {
        // This is intentionally an internal host-lifecycle receipt. The
        // user-visible write protocol is exercised by the Db integration
        // suites; here we need to observe the adapter-owned lease and the
        // runtime-owned HLC exactly at their handoff boundary.
        let directory = tempfile::tempdir().unwrap();
        let mut host = NativeRelayHost::default();
        let capability = host
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "foreground-clean-handoff".to_owned(),
                    storage_namespace: "default".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: directory
                    .path()
                    .join("foreground.sqlite")
                    .display()
                    .to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0xc1; 16]),
                    author: AuthorSubject::for_test_bytes([0xc2; 16]),
                },
                claims: BTreeMap::new(),
            })
            .expect("trusted fixture admission succeeds");

        let first = host.open_foreground(capability, 1).unwrap();
        let first_lease = host.foregrounds[&first].lease;
        let first_client = host.foreground_client(first).unwrap().clone();
        let transaction = first_client
            .begin_foreground_transaction(ForegroundTransactionKind::Mergeable)
            .unwrap();
        first_client
            .insert_foreground_transaction(
                transaction,
                "todos".to_owned(),
                encoded_title_cells("first lease holder"),
                Some([0xc3; 16]),
            )
            .unwrap();
        first_client
            .commit_foreground_transaction(transaction)
            .unwrap();
        let first_high_water = first_client.minted_tx_time_high_water().unwrap();
        assert!(first_high_water > TxTime::default());

        assert!(host.close_foreground(first).unwrap());
        let second = host.open_foreground(capability, 2).unwrap();
        let second_lease = host.foregrounds[&second].lease;
        assert_eq!(second_lease.node, first_lease.node);
        assert_eq!(second_lease.confirmed_tx_time, first_high_water);
        assert!(
            host.foreground_client(second)
                .unwrap()
                .minted_tx_time_high_water()
                .unwrap()
                > first_high_water,
            "the reused runtime reserves an HLC strictly past its predecessor before exposure"
        );
        assert!(host.close_foreground(second).unwrap());
    }

    #[test]
    fn uncertain_foreground_readout_retires_instead_of_reissuing_its_node() {
        // Internal failure-path receipt: removing the owner-local client
        // models a runtime that disappeared before native close could read its
        // HLC. There is deliberately no best-effort reuse in that case.
        let directory = tempfile::tempdir().unwrap();
        let mut host = NativeRelayHost::default();
        let capability = host
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "foreground-uncertain-handoff".to_owned(),
                    storage_namespace: "default".to_owned(),
                    auth_scope: Some("opaque-validated-subject".to_owned()),
                },
                sqlite_path: directory
                    .path()
                    .join("foreground.sqlite")
                    .display()
                    .to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0xd1; 16]),
                    author: AuthorSubject::for_test_bytes([0xd2; 16]),
                },
                claims: BTreeMap::new(),
            })
            .expect("trusted fixture admission succeeds");

        let first = host.open_foreground(capability, 1).unwrap();
        let first_lease = host.foregrounds[&first].lease;
        let first_client = host.foregrounds[&first].client;
        drop(host.clients.remove(&first_client));
        assert_eq!(
            host.close_foreground(first),
            Err(JazzNativeRelayStatus::LifecycleFailure)
        );

        let second = host.open_foreground(capability, 2).unwrap();
        assert_ne!(host.foregrounds[&second].lease.node, first_lease.node);
        assert!(host.close_foreground(second).unwrap());
    }

    // Internal queue hold models a cold transaction owner during synchronous
    // native handoff. The public lease must retire rather than reuse an uncertain HLC.
    #[test]
    fn contended_hlc_readout_retires_identity_and_keeps_local_close_live() {
        let directory = tempfile::tempdir().unwrap();
        let mut host = NativeRelayHost::default();
        let capability = host
            .admit_scope(RelayScopeAdmissionRequest {
                scope: RelayScopeRequest {
                    app_namespace: "contended-handoff".into(),
                    storage_namespace: "default".into(),
                    auth_scope: Some("validated".into()),
                },
                sqlite_path: directory
                    .path()
                    .join("handoff.sqlite")
                    .display()
                    .to_string(),
                schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                identity: DbIdentity {
                    node: NodeUuid::from_bytes([0xc1; 16]),
                    author: AuthorSubject::for_test_bytes([0xc2; 16]),
                },
                claims: BTreeMap::new(),
            })
            .unwrap();
        let first = host.open_foreground(capability, 1).unwrap();
        let keeper = host.open_foreground(capability, 2).unwrap();
        let old_node = host.foregrounds[&first].lease.node;
        let client = host.foreground_client(first).unwrap().clone();
        let id = client.id;
        let relay = client.relay.clone();
        let tx = client
            .begin_foreground_transaction(ForegroundTransactionKind::Mergeable)
            .unwrap();
        let release = relay
            .run(move |worker| {
                let (db, transaction) = worker.foreground_transaction(id, tx)?;
                let (release, released) = futures::channel::oneshot::channel::<()>();
                let held = Rc::clone(&db);
                let _read = db.enqueue_transaction_read(transaction.open_tx_id, async move {
                    let hold = Box::pin(held.hold_node_owner_for_test());
                    let _ = futures::future::select(released, hold).await;
                    Ok(())
                });
                db.drive_queued_mutation_once();
                Ok(release)
            })
            .unwrap();
        client
            .insert_foreground_transaction(
                tx,
                "todos".into(),
                encoded_title_cells("admitted before retirement"),
                None,
            )
            .unwrap();
        client.commit_foreground_transaction(tx).unwrap();
        assert_eq!(
            host.close_foreground(first),
            Err(JazzNativeRelayStatus::LifecycleFailure)
        );
        assert!(!host.foregrounds.contains_key(&first));
        let next = host.open_foreground(capability, 3).unwrap();
        assert_ne!(host.foregrounds[&next].lease.node, old_node);
        release.send(()).unwrap();
        for _ in 0..100 {
            if relay.run(|worker| Ok(worker.closing.is_empty())).unwrap() {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        assert!(relay.run(|worker| Ok(worker.closing.is_empty())).unwrap());
        assert!(host.close_foreground(next).unwrap());
        assert!(host.close_foreground(keeper).unwrap());
    }

    #[test]
    fn admitted_scope_capabilities_are_unguessable_and_revocation_closes_all_aliases() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        let admission = |name: &str, byte: u8| RelayScopeAdmissionRequest {
            scope: RelayScopeRequest {
                app_namespace: "capability-test".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some(name.to_owned()),
            },
            sqlite_path: directory
                .path()
                .join(format!("{name}.sqlite"))
                .display()
                .to_string(),
            schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([byte; 16]),
                author: AuthorSubject::for_test_bytes([byte.wrapping_add(1); 16]),
            },
            claims: BTreeMap::from([("sub".to_owned(), Value::String(name.to_owned()))]),
        };
        unsafe fn admit(
            host: *mut JazzNativeRelayHost,
            request: RelayScopeAdmissionRequest,
        ) -> AdmissionCapability {
            unsafe { (*host).inner.lock().unwrap().admit_scope(request) }
                .expect("test admission is valid")
        }
        let alice = unsafe { admit(host, admission("alice", 0xa1)) };
        let bob = unsafe { admit(host, admission("bob", 0xb1)) };
        assert_ne!(alice, bob);
        assert_ne!(alice.0, [0; 32]);
        assert_ne!(bob.0, [0; 32]);

        let open = |admitted_scope| RelayCommandRequest::Open {
            supported_abi_minimum: NATIVE_RELAY_ABI_V1,
            supported_abi_maximum: NATIVE_RELAY_ABI_V1,
            admitted_scope,
        };
        let execute = |request| unsafe { (*host).inner.lock().unwrap().execute(request) };
        let alice_relay = match execute(open(alice)).unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected open response: {response:?}"),
        };
        let alice_alias = match execute(open(alice)).unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected open response: {response:?}"),
        };
        let alice_client =
            match execute(RelayCommandRequest::Attach { relay: alice_relay }).unwrap() {
                RelayCommandResponse::Attached { client } => client,
                response => panic!("unexpected attach response: {response:?}"),
            };
        let bob_relay = match execute(open(bob)).unwrap() {
            RelayCommandResponse::Opened { relay } => relay,
            response => panic!("unexpected open response: {response:?}"),
        };

        let mut guessed = alice;
        guessed.0[0] ^= 0x80;
        assert_eq!(
            execute(open(guessed)),
            Err(JazzNativeRelayStatus::InvalidHandle)
        );

        assert!(unsafe { (*host).inner.lock().unwrap().revoke_scope(alice) }.unwrap());

        assert_eq!(
            execute(open(alice)),
            Err(JazzNativeRelayStatus::InvalidHandle)
        );
        assert_eq!(
            execute(RelayCommandRequest::Pump { relay: alice_relay }),
            Err(JazzNativeRelayStatus::InvalidHandle)
        );
        assert_eq!(
            execute(RelayCommandRequest::Pump { relay: alice_alias }),
            Err(JazzNativeRelayStatus::InvalidHandle)
        );
        assert_eq!(
            execute(RelayCommandRequest::CloseClient {
                client: alice_client
            })
            .unwrap(),
            RelayCommandResponse::Closed { closed: false }
        );
        assert!(matches!(
            execute(RelayCommandRequest::Pump { relay: bob_relay }),
            Ok(RelayCommandResponse::Pumped)
        ));
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn attached_foreground_c_abi_opens_ticks_and_closes_only_admitted_aliases() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        let admission = RelayScopeAdmissionRequest {
            scope: RelayScopeRequest {
                app_namespace: "foreground-c-abi".to_owned(),
                storage_namespace: "default".to_owned(),
                auth_scope: Some("opaque-validated-subject".to_owned()),
            },
            sqlite_path: directory
                .path()
                .join("foreground.sqlite")
                .display()
                .to_string(),
            schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0x71; 16]),
                author: AuthorSubject::for_test_bytes([0x72; 16]),
            },
            claims: BTreeMap::from([("role".to_owned(), Value::String("member".to_owned()))]),
        };
        let capability = unsafe { (*host).inner.lock().unwrap().admit_scope(admission) }
            .expect("trusted fixture admission succeeds");

        let mut foreground = u64::MAX;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_open_attached_foreground(
                    host,
                    capability.0.as_ptr(),
                    capability.0.len() - 1,
                    &mut foreground,
                )
            },
            JazzNativeRelayStatus::InvalidArgument
        );
        assert_eq!(foreground, u64::MAX, "invalid calls do not write outputs");

        foreground = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_open_attached_foreground(
                    host,
                    capability.0.as_ptr(),
                    capability.0.len(),
                    &mut foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert_ne!(foreground, 0, "a real foreground alias was opened");
        assert_eq!(
            unsafe { jazz_native_relay_host_tick_attached_foreground(host, foreground) },
            JazzNativeRelayStatus::Ok,
            "tick reaches the actual memory Db through its owner thread"
        );

        let mut closed = false;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_close_attached_foreground(host, foreground, &mut closed)
            },
            JazzNativeRelayStatus::Ok
        );
        assert!(closed, "the first close owns the foreground transition");
        assert_eq!(
            unsafe { jazz_native_relay_host_tick_attached_foreground(host, foreground) },
            JazzNativeRelayStatus::InvalidHandle,
            "a closed JSI alias cannot continue to invoke the owner"
        );
        closed = true;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_close_attached_foreground(host, foreground, &mut closed)
            },
            JazzNativeRelayStatus::Ok
        );
        assert!(
            !closed,
            "explicit close and finalization are safely idempotent"
        );

        assert_eq!(
            unsafe {
                jazz_native_relay_host_revoke_scope_capability(
                    host,
                    capability.0.as_ptr(),
                    capability.0.len(),
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let mut reopened = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_open_attached_foreground(
                    host,
                    capability.0.as_ptr(),
                    capability.0.len(),
                    &mut reopened,
                )
            },
            JazzNativeRelayStatus::InvalidHandle,
            "revocation closes aliases and prevents all future opens"
        );
        assert_eq!(reopened, 0);
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn retained_runtime_leases_cannot_operate_each_others_foregrounds() {
        let directory = tempfile::tempdir().unwrap();
        let fixture = NativeHostAbiFixture::new();
        let schema = permissive_schema();
        let a_capability = fixture.admit(
            &directory.path().join("lease-a.sqlite"),
            "lease-a",
            &schema,
            0xa1,
        );
        let b_capability = fixture.admit(
            &directory.path().join("lease-b.sqlite"),
            "lease-b",
            &schema,
            0xb1,
        );
        let a = fixture.open_foreground(&a_capability);
        let b_runtime = unsafe { jazz_native_relay_host_retain(fixture.host, 2) };
        assert!(!b_runtime.is_null());
        let mut b = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    b_runtime,
                    b_capability.as_ptr(),
                    b_capability.len(),
                    &mut b,
                )
            },
            JazzNativeRelayStatus::Ok,
        );

        // All lease-scoped entrypoints reject B when addressed through A's
        // retained runtime token. In particular, a foreign close must not
        // consume B's handle or its node lease.
        assert_eq!(
            unsafe { jazz_native_relay_host_lease_tick_attached_foreground(fixture.lease, b) },
            JazzNativeRelayStatus::InvalidHandle,
        );
        let request = postcard::to_allocvec(&ForegroundDbCommandRequest::Probe).unwrap();
        let mut output = JazzNativeRelayBytes::EMPTY;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_execute_foreground(
                    fixture.lease,
                    b,
                    request.as_ptr(),
                    request.len(),
                    &mut output,
                )
            },
            JazzNativeRelayStatus::InvalidHandle,
        );
        assert!(output.data.is_null() && output.len == 0);
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_set_foreground_wake_callback(
                    fixture.lease,
                    b,
                    None,
                    std::ptr::null_mut(),
                )
            },
            JazzNativeRelayStatus::InvalidHandle,
        );
        let mut foreign_closed = true;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_close_attached_foreground(
                    fixture.lease,
                    b,
                    &mut foreign_closed,
                )
            },
            JazzNativeRelayStatus::InvalidHandle,
        );
        assert!(
            !foreign_closed,
            "rejected close cannot report a foreign foreground as closed"
        );

        // Each runtime remains fully capable of operating its own foreground.
        assert_eq!(
            unsafe { jazz_native_relay_host_lease_tick_attached_foreground(fixture.lease, a) },
            JazzNativeRelayStatus::Ok,
        );
        assert_eq!(
            unsafe { jazz_native_relay_host_lease_tick_attached_foreground(b_runtime, b) },
            JazzNativeRelayStatus::Ok,
        );
        let mut b_closed = false;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_close_attached_foreground(b_runtime, b, &mut b_closed)
            },
            JazzNativeRelayStatus::Ok,
        );
        assert!(b_closed);
        unsafe { jazz_native_relay_host_lease_free(b_runtime) };
    }

    #[test]
    fn retained_foreground_lease_outlives_the_platform_host_wrapper() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        let capability = unsafe {
            (*host)
                .inner
                .lock()
                .unwrap()
                .admit_scope(RelayScopeAdmissionRequest {
                    scope: RelayScopeRequest {
                        app_namespace: "foreground-lease-receipt".to_owned(),
                        storage_namespace: "default".to_owned(),
                        auth_scope: Some("opaque-validated-subject".to_owned()),
                    },
                    sqlite_path: directory
                        .path()
                        .join("foreground.sqlite")
                        .display()
                        .to_string(),
                    schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                    identity: DbIdentity {
                        node: NodeUuid::from_bytes([0x81; 16]),
                        author: AuthorSubject::for_test_bytes([0x82; 16]),
                    },
                    claims: BTreeMap::new(),
                })
                .expect("trusted fixture admission succeeds")
        };
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        assert!(!lease.is_null());
        let mut foreground = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    lease,
                    capability.0.as_ptr(),
                    capability.0.len(),
                    &mut foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );

        // This is the precise teardown race that the JSI lease closes: platform
        // ownership may disappear while a finalizer still owns its foreground
        // handle. The retained opaque lease keeps Rust state alive and usable.
        unsafe { jazz_native_relay_host_free(host) };
        assert_eq!(
            unsafe { jazz_native_relay_host_lease_tick_attached_foreground(lease, foreground) },
            JazzNativeRelayStatus::Ok
        );
        let mut closed = false;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_close_attached_foreground(
                    lease,
                    foreground,
                    &mut closed,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert!(closed);
        unsafe { jazz_native_relay_host_lease_free(lease) };
    }

    #[test]
    fn invalidating_one_retained_runtime_retires_only_its_foregrounds() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        let capability = unsafe {
            (*host)
                .inner
                .lock()
                .unwrap()
                .admit_scope(RelayScopeAdmissionRequest {
                    scope: RelayScopeRequest {
                        app_namespace: "independent-foreground-leases".to_owned(),
                        storage_namespace: "default".to_owned(),
                        auth_scope: Some("opaque-validated-subject".to_owned()),
                    },
                    sqlite_path: directory
                        .path()
                        .join("foreground.sqlite")
                        .display()
                        .to_string(),
                    schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                    identity: DbIdentity {
                        node: NodeUuid::from_bytes([0xc1; 16]),
                        author: AuthorSubject::for_test_bytes([0xc2; 16]),
                    },
                    claims: BTreeMap::new(),
                })
                .unwrap()
        };
        let first_lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        let second_lease = unsafe { jazz_native_relay_host_retain(host, 2) };
        let open = |lease| {
            let mut foreground = 0;
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_lease_open_attached_foreground(
                        lease,
                        capability.0.as_ptr(),
                        capability.0.len(),
                        &mut foreground,
                    )
                },
                JazzNativeRelayStatus::Ok
            );
            foreground
        };
        let first = open(first_lease);
        let second = open(second_lease);
        assert_eq!(
            unsafe { jazz_native_relay_host_lease_invalidate_foreground_runtime(first_lease) },
            JazzNativeRelayStatus::Ok
        );
        assert_eq!(
            unsafe { jazz_native_relay_host_lease_tick_attached_foreground(first_lease, first) },
            JazzNativeRelayStatus::InvalidHandle,
            "runtime invalidation retires its foreground before a JS finalizer runs"
        );
        assert_eq!(
            unsafe { jazz_native_relay_host_lease_tick_attached_foreground(second_lease, second) },
            JazzNativeRelayStatus::Ok,
            "runtime invalidation cannot revoke a sibling runtime sharing the relay"
        );
        assert_eq!(
            unsafe { jazz_native_relay_host_lease_invalidate_foreground_runtime(second_lease) },
            JazzNativeRelayStatus::Ok
        );
        assert!(unsafe { (*host).inner.lock().unwrap().foregrounds.is_empty() });
        unsafe { jazz_native_relay_host_lease_free(first_lease) };
        unsafe { jazz_native_relay_host_lease_free(second_lease) };
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn attached_foregrounds_are_independent_and_closing_one_keeps_the_other_live() {
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        let capability = unsafe {
            (*host)
                .inner
                .lock()
                .unwrap()
                .admit_scope(RelayScopeAdmissionRequest {
                    scope: RelayScopeRequest {
                        app_namespace: "two-foregrounds".to_owned(),
                        storage_namespace: "default".to_owned(),
                        auth_scope: Some("opaque-validated-subject".to_owned()),
                    },
                    sqlite_path: directory
                        .path()
                        .join("foreground.sqlite")
                        .display()
                        .to_string(),
                    schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                    identity: DbIdentity {
                        node: NodeUuid::from_bytes([0x91; 16]),
                        author: AuthorSubject::for_test_bytes([0x92; 16]),
                    },
                    claims: BTreeMap::new(),
                })
                .expect("trusted fixture admission succeeds")
        };

        let open = |host: *mut JazzNativeRelayHost| {
            let mut foreground = 0;
            assert_eq!(
                unsafe {
                    jazz_native_relay_host_open_attached_foreground(
                        host,
                        capability.0.as_ptr(),
                        capability.0.len(),
                        &mut foreground,
                    )
                },
                JazzNativeRelayStatus::Ok
            );
            foreground
        };
        let first = open(host);
        let second = open(host);
        assert_ne!(
            first, second,
            "each JS runtime receives its own opaque foreground handle"
        );
        assert_eq!(
            unsafe { jazz_native_relay_host_tick_attached_foreground(host, first) },
            JazzNativeRelayStatus::Ok
        );
        assert_eq!(
            unsafe { jazz_native_relay_host_tick_attached_foreground(host, second) },
            JazzNativeRelayStatus::Ok
        );

        let mut first_closed = false;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_close_attached_foreground(host, first, &mut first_closed)
            },
            JazzNativeRelayStatus::Ok
        );
        assert!(first_closed);
        assert_eq!(
            unsafe { jazz_native_relay_host_tick_attached_foreground(host, first) },
            JazzNativeRelayStatus::InvalidHandle,
            "closing one JS runtime cannot leave its foreground alias usable"
        );
        assert_eq!(
            unsafe { jazz_native_relay_host_tick_attached_foreground(host, second) },
            JazzNativeRelayStatus::Ok,
            "closing one JS runtime cannot tear down its sibling foreground"
        );

        assert_eq!(
            unsafe {
                jazz_native_relay_host_revoke_scope_capability(
                    host,
                    capability.0.as_ptr(),
                    capability.0.len(),
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert_eq!(
            unsafe { jazz_native_relay_host_tick_attached_foreground(host, second) },
            JazzNativeRelayStatus::InvalidHandle,
            "revocation invalidates every foreground belonging to the capability"
        );
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn foreground_command_c_abi_uses_one_binary_runtime_vocabulary() {
        let directory = tempfile::tempdir().unwrap();
        // Internal ABI test: public row APIs cannot expose native metadata
        // framing or prove the admitted account survives the capability boundary.
        let author = AuthorSubject::for_test_bytes([0xa2; 16]).with_account(
            jazz::account_registry::AccountId(NodeUuid::from_bytes([7; 16]).0),
        );
        let host = jazz_native_relay_host_new();
        let capability = unsafe {
            (*host)
                .inner
                .lock()
                .unwrap()
                .admit_scope(RelayScopeAdmissionRequest {
                    scope: RelayScopeRequest {
                        app_namespace: "foreground-command-abi".to_owned(),
                        storage_namespace: "default".to_owned(),
                        auth_scope: Some("opaque-validated-subject".to_owned()),
                    },
                    sqlite_path: directory
                        .path()
                        .join("foreground.sqlite")
                        .display()
                        .to_string(),
                    schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                    identity: DbIdentity {
                        node: NodeUuid::from_bytes([0xa1; 16]),
                        author,
                    },
                    claims: BTreeMap::new(),
                })
                .expect("trusted fixture admission succeeds")
        };
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        let mut foreground = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    lease,
                    capability.0.as_ptr(),
                    capability.0.len(),
                    &mut foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );

        let execute = |command| {
            let request = postcard::to_allocvec(&command).unwrap();
            let mut response = JazzNativeRelayBytes::EMPTY;
            let status = unsafe {
                jazz_native_relay_host_lease_execute_foreground(
                    lease,
                    foreground,
                    request.as_ptr(),
                    request.len(),
                    &mut response,
                )
            };
            let bytes = unsafe { std::slice::from_raw_parts(response.data, response.len) }.to_vec();
            unsafe { jazz_native_relay_bytes_free(&mut response) };
            (status, bytes)
        };
        let (status, response) = execute(ForegroundDbCommandRequest::NativeSessionMetadata);
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        let (issuer, user_id) = author.principal_parts();
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
            ForegroundDbCommandResponse::NativeSessionMetadata {
                node: unsafe {
                    (*host).inner.lock().unwrap().foregrounds[&foreground]
                        .lease
                        .node
                        .0
                        .into_bytes()
                },
                registry_authority: "foreground-command-abi".into(),
                account_id: Some([7; 16]),
                issuer,
                user_id,
            }
        );
        let (status, response) = execute(ForegroundDbCommandRequest::Probe);
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
            ForegroundDbCommandResponse::Probe {
                abi_version: NATIVE_RELAY_ABI_V1
            }
        );
        let (status, response) = execute(ForegroundDbCommandRequest::Tick);
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
            ForegroundDbCommandResponse::Ticked
        );
        let query = postcard::to_allocvec(&Query::from("todos")).unwrap();
        let (status, response) = execute(ForegroundDbCommandRequest::All {
            query: query.clone(),
            options_json: "{}".into(),
            transaction: None,
        });
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        let mut response = postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap();
        let mut turns = 0;
        let rows = loop {
            match response {
                ForegroundDbCommandResponse::Rows { rows } => break rows,
                ForegroundDbCommandResponse::Pending { operation } if turns < 100 => {
                    let (status, tick) = execute(ForegroundDbCommandRequest::Tick);
                    assert_eq!(status, JazzNativeRelayStatus::Ok);
                    assert_eq!(
                        postcard::from_bytes::<ForegroundDbCommandResponse>(&tick).unwrap(),
                        ForegroundDbCommandResponse::Ticked
                    );
                    let (status, poll) = execute(ForegroundDbCommandRequest::Poll { operation });
                    assert_eq!(status, JazzNativeRelayStatus::Ok);
                    response = postcard::from_bytes::<ForegroundDbCommandResponse>(&poll).unwrap();
                    turns += 1;
                }
                ForegroundDbCommandResponse::Pending { .. } => {
                    panic!("all did not finish after {turns} owner turns")
                }
                other => panic!("all returned an unexpected response: {other:?}"),
            }
        };
        assert!(rows.len() <= 2, "empty foreground read must stay bounded");
        let (status, response) = execute(ForegroundDbCommandRequest::Subscribe {
            query,
            options_json: "{}".into(),
        });
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        let ForegroundDbCommandResponse::Subscribed { subscription } =
            postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap()
        else {
            panic!("subscribe must return an opaque subscription handle");
        };
        let (status, response) =
            execute(ForegroundDbCommandRequest::DrainSubscription { subscription });
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        assert!(matches!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
            ForegroundDbCommandResponse::SubscriptionEvents { .. }
        ));
        let (status, response) = execute(ForegroundDbCommandRequest::Unsubscribe { subscription });
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
            ForegroundDbCommandResponse::Unsubscribed { closed: true }
        );
        let (status, response) = execute(ForegroundDbCommandRequest::Cancel { operation: 999 });
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
            ForegroundDbCommandResponse::Cancelled { cancelled: false },
            "an unknown operation cannot cancel a different foreground's work"
        );
        let (status, response) = execute(ForegroundDbCommandRequest::Close);
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
        let (status, response) = execute(ForegroundDbCommandRequest::Close);
        assert_eq!(status, JazzNativeRelayStatus::Ok);
        assert_eq!(
            postcard::from_bytes::<ForegroundDbCommandResponse>(&response).unwrap(),
            ForegroundDbCommandResponse::Closed { closed: false },
            "the byte ABI retains the same idempotent close rule as the JSI convenience method"
        );

        let request = postcard::to_allocvec(&ForegroundDbCommandRequest::Probe).unwrap();
        let mut stale_response = JazzNativeRelayBytes {
            data: usize::MAX as *mut u8,
            len: usize::MAX,
        };
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_execute_foreground(
                    lease,
                    foreground,
                    request.as_ptr(),
                    request.len(),
                    &mut stale_response,
                )
            },
            JazzNativeRelayStatus::InvalidHandle
        );
        assert!(stale_response.data.is_null() && stale_response.len == 0);
        unsafe { jazz_native_relay_host_lease_free(lease) };
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn foreground_transaction_commands_delegate_to_core_and_retire_with_their_owner() {
        // This is intentionally an internal C-ABI receipt: it proves the
        // byte command family reaches the ordinary foreground Db and its core
        // transaction handles, while public row/query semantics stay covered
        // by the Db integration suites.
        let directory = tempfile::tempdir().unwrap();
        let host = jazz_native_relay_host_new();
        let capability = unsafe {
            (*host)
                .inner
                .lock()
                .unwrap()
                .admit_scope(RelayScopeAdmissionRequest {
                    scope: RelayScopeRequest {
                        app_namespace: "foreground-transaction-abi".to_owned(),
                        storage_namespace: "default".to_owned(),
                        auth_scope: Some("opaque-validated-subject".to_owned()),
                    },
                    sqlite_path: directory
                        .path()
                        .join("foreground.sqlite")
                        .display()
                        .to_string(),
                    schema_json: serde_json::to_string(schema().public_schema()).unwrap(),
                    identity: DbIdentity {
                        node: NodeUuid::from_bytes([0xb1; 16]),
                        author: AuthorSubject::for_test_bytes([0xb2; 16]),
                    },
                    claims: BTreeMap::new(),
                })
                .expect("trusted fixture admission succeeds")
        };
        let lease = unsafe { jazz_native_relay_host_retain(host, 1) };
        let mut foreground = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    lease,
                    capability.0.as_ptr(),
                    capability.0.len(),
                    &mut foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        let execute = |foreground, command| {
            let request = postcard::to_allocvec(&command).unwrap();
            let mut response = JazzNativeRelayBytes::EMPTY;
            let status = unsafe {
                jazz_native_relay_host_lease_execute_foreground(
                    lease,
                    foreground,
                    request.as_ptr(),
                    request.len(),
                    &mut response,
                )
            };
            let bytes = if response.data.is_null() {
                Vec::new()
            } else {
                unsafe { std::slice::from_raw_parts(response.data, response.len) }.to_vec()
            };
            unsafe { jazz_native_relay_bytes_free(&mut response) };
            (status, bytes)
        };
        let response = |foreground, command| {
            let (status, bytes) = execute(foreground, command);
            assert_eq!(status, JazzNativeRelayStatus::Ok);
            postcard::from_bytes::<ForegroundDbCommandResponse>(&bytes).unwrap()
        };

        let ForegroundDbCommandResponse::TransactionOpened { transaction } = response(
            foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            },
        ) else {
            panic!("begin must return an opaque transaction handle");
        };
        let row_id = [0x71; 16];
        assert_eq!(
            response(
                foreground,
                ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "todos".to_owned(),
                    cells: encoded_title_cells("mergeable"),
                    row_id: Some(row_id),
                },
            ),
            ForegroundDbCommandResponse::Inserted { row_id }
        );
        assert_eq!(
            response(
                foreground,
                ForegroundDbCommandRequest::Update {
                    transaction,
                    table: "todos".to_owned(),
                    row_id,
                    patch: encoded_title_cells("updated"),
                },
            ),
            ForegroundDbCommandResponse::MutationStaged
        );
        assert_eq!(
            response(
                foreground,
                ForegroundDbCommandRequest::Upsert {
                    transaction,
                    table: "todos".to_owned(),
                    row_id,
                    cells: encoded_title_cells("upserted"),
                },
            ),
            ForegroundDbCommandResponse::MutationStaged
        );
        assert_eq!(
            response(
                foreground,
                ForegroundDbCommandRequest::Delete {
                    transaction,
                    table: "todos".to_owned(),
                    row_id,
                },
            ),
            ForegroundDbCommandResponse::MutationStaged
        );
        let ForegroundDbCommandResponse::TransactionCommitted { tx_id } = response(
            foreground,
            ForegroundDbCommandRequest::CommitTransaction { transaction },
        ) else {
            panic!("commit must return the public committed txId");
        };
        assert_ne!(tx_id, [0; 16]);
        assert!(matches!(
            response(
                foreground,
                ForegroundDbCommandRequest::RollbackTransaction { transaction }
            ),
            ForegroundDbCommandResponse::OperationError { .. }
        ));

        let ForegroundDbCommandResponse::TransactionOpened { transaction } = response(
            foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Exclusive,
            },
        ) else {
            panic!("exclusive begin must return a handle");
        };
        assert_eq!(
            response(
                foreground,
                ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "todos".to_owned(),
                    cells: encoded_title_cells("rolled back"),
                    row_id: Some([0x72; 16]),
                },
            ),
            ForegroundDbCommandResponse::Inserted { row_id: [0x72; 16] }
        );
        assert_eq!(
            response(
                foreground,
                ForegroundDbCommandRequest::RollbackTransaction { transaction },
            ),
            ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: true }
        );

        let mut second_foreground = 0;
        assert_eq!(
            unsafe {
                jazz_native_relay_host_lease_open_attached_foreground(
                    lease,
                    capability.0.as_ptr(),
                    capability.0.len(),
                    &mut second_foreground,
                )
            },
            JazzNativeRelayStatus::Ok
        );
        assert!(matches!(
            response(
                second_foreground,
                ForegroundDbCommandRequest::CommitTransaction { transaction }
            ),
            ForegroundDbCommandResponse::OperationError { .. }
        ));

        // Invalid schema/cell input is a logical operation error rather than
        // a lifecycle failure, preserving the core error boundary for the
        // shared adapter.
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = response(
            foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            },
        ) else {
            panic!("begin must return a handle");
        };
        assert!(matches!(
            response(
                foreground,
                ForegroundDbCommandRequest::Insert {
                    transaction,
                    table: "missing_table".to_owned(),
                    cells: encoded_title_cells("nope"),
                    row_id: Some([0x73; 16]),
                }
            ),
            ForegroundDbCommandResponse::OperationError { .. }
        ));
        assert_eq!(
            response(
                foreground,
                ForegroundDbCommandRequest::RollbackTransaction { transaction },
            ),
            ForegroundDbCommandResponse::TransactionRolledBack { rolled_back: true }
        );

        // Closing a foreground retires all of its mutable core transactions.
        // A fresh alias is allowed later, but cannot retain or complete the
        // closed alias's opaque handle.
        let ForegroundDbCommandResponse::TransactionOpened { transaction } = response(
            second_foreground,
            ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            },
        ) else {
            panic!("second foreground begin must return a handle");
        };
        assert_eq!(
            response(second_foreground, ForegroundDbCommandRequest::Close),
            ForegroundDbCommandResponse::Closed { closed: true }
        );
        let (status, stale_response) = execute(
            second_foreground,
            ForegroundDbCommandRequest::CommitTransaction { transaction },
        );
        assert_eq!(status, JazzNativeRelayStatus::InvalidHandle);
        assert!(stale_response.is_empty());

        unsafe { jazz_native_relay_host_lease_free(lease) };
        unsafe { jazz_native_relay_host_free(host) };
    }

    #[test]
    fn foreground_transaction_postcard_layout_matches_the_handwritten_ts_codec() {
        assert_eq!(
            postcard::to_allocvec(&ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Mergeable,
            })
            .unwrap(),
            vec![9, 0]
        );
        assert_eq!(
            postcard::to_allocvec(&ForegroundDbCommandRequest::BeginTransaction {
                kind: ForegroundTransactionKind::Exclusive,
            })
            .unwrap(),
            vec![9, 1]
        );
        assert_eq!(
            postcard::to_allocvec(&ForegroundDbCommandRequest::Insert {
                transaction: 3,
                table: "todos".to_owned(),
                cells: vec![1, 2],
                row_id: None,
            })
            .unwrap(),
            vec![10, 3, 5, b't', b'o', b'd', b'o', b's', 2, 1, 2, 0]
        );
        assert_eq!(
            postcard::to_allocvec(&ForegroundDbCommandRequest::Update {
                transaction: 3,
                table: "todos".to_owned(),
                row_id: [7; 16],
                patch: vec![9],
            })
            .unwrap(),
            [
                vec![11, 3, 5, b't', b'o', b'd', b'o', b's'],
                vec![7; 16],
                vec![1, 9]
            ]
            .concat()
        );
        assert_eq!(
            postcard::to_allocvec(&ForegroundDbCommandResponse::TransactionCommitted {
                tx_id: [4; 16],
            })
            .unwrap(),
            [vec![13], vec![4; 16]].concat()
        );
        assert_eq!(
            postcard::to_allocvec(&ForegroundDbCommandResponse::TransactionRolledBack {
                rolled_back: true,
            })
            .unwrap(),
            vec![14, 1]
        );
        // Authoritative settlement has one pinned byte spelling for
        // JNI/Swift/JSI wrappers.
        assert_eq!(
            postcard::to_allocvec(&ForegroundDbCommandRequest::WaitForCoreTransaction {
                tx_id: [5; 16],
            })
            .unwrap(),
            [vec![16], vec![5; 16]].concat()
        );
        assert_eq!(
            postcard::to_allocvec(&ForegroundDbCommandResponse::TransactionSettled {
                tx_id: [6; 16],
            })
            .unwrap(),
            [vec![15], vec![6; 16]].concat()
        );
    }

    // Public row results cannot detect a changed native event discriminant.
    #[test]
    fn foreground_structured_delta_v1_byte_contract() {
        let event = ForegroundSubscriptionEvent::StructuredDelta {
            reset: true,
            settled: false,
            tier: "local".into(),
            delta: vec![9],
            terminal_operations_json: "[]".into(),
        };
        let bytes = [
            vec![3, 1, 0, 5],
            b"local".to_vec(),
            vec![1, 9, 2],
            b"[]".to_vec(),
        ]
        .concat();
        assert_eq!(postcard::to_allocvec(&event).unwrap(), bytes);
        assert_eq!(
            postcard::from_bytes::<ForegroundSubscriptionEvent>(&bytes).unwrap(),
            event
        );
    }

    // Internal byte assertions are necessary: public row results cannot reveal
    // a changed enum ordinal or option encoding that breaks installed hosts.
    #[test]
    fn foreground_extension_v1_byte_contract() {
        let cases = [
            (
                ForegroundDbCommandRequest::All {
                    query: vec![1, 128, 2],
                    options_json: "{}".into(),
                    transaction: None,
                },
                vec![2, 3, 1, 128, 2, 2, 123, 125, 0],
            ),
            (
                ForegroundDbCommandRequest::All {
                    query: vec![1],
                    options_json: "{}".into(),
                    transaction: Some(256),
                },
                vec![2, 1, 1, 2, 123, 125, 1, 128, 2],
            ),
            (
                ForegroundDbCommandRequest::Subscribe {
                    query: vec![1],
                    options_json: "{}".into(),
                },
                vec![3, 1, 1, 2, 123, 125],
            ),
            (
                ForegroundDbCommandRequest::WaitForTransaction {
                    tx_id: [7; 16],
                    tier: "core".into(),
                },
                [vec![17], vec![7; 16], vec![4, 99, 111, 114, 101]].concat(),
            ),
            (
                ForegroundDbCommandRequest::StageMutation {
                    transaction: 1,
                    mutation: ForegroundMutationKind::Restore,
                    table: "t".into(),
                    row_id: None,
                    cells: vec![],
                    options_json: "{}".into(),
                },
                vec![18, 1, 4, 1, 116, 0, 0, 2, 123, 125],
            ),
            (
                ForegroundDbCommandRequest::DisconnectNativeUpstream,
                vec![19],
            ),
            (
                ForegroundDbCommandRequest::ReconnectNativeUpstream,
                vec![20],
            ),
            (ForegroundDbCommandRequest::NativeConnectionStatus, vec![21]),
        ];
        for (command, bytes) in cases {
            assert_eq!(postcard::to_allocvec(&command).unwrap(), bytes);
            assert_eq!(
                postcard::from_bytes::<ForegroundDbCommandRequest>(&bytes).unwrap(),
                command
            );
        }
        assert_eq!(
            postcard::to_allocvec(&ForegroundDbCommandResponse::NativeConnectionStatus {
                configured: true,
                explicitly_offline: false,
                connected: true
            })
            .unwrap(),
            vec![16, 1, 0, 1]
        );
        for (ordinal, kind) in [
            ForegroundMutationKind::Insert,
            ForegroundMutationKind::Update,
            ForegroundMutationKind::Upsert,
            ForegroundMutationKind::Delete,
            ForegroundMutationKind::Restore,
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(postcard::to_allocvec(&kind).unwrap(), vec![ordinal as u8]);
        }
    }

    #[test]
    fn foreground_continuation_v1_byte_contract() {
        // Internal byte fixtures pin host/OTA compatibility that row-level
        // database assertions cannot observe.
        let cases = [
            (ForegroundDbCommandRequest::NativeSessionMetadata, vec![22]),
            (
                ForegroundDbCommandRequest::WaitForPendingWrites {
                    tier: "core".into(),
                },
                vec![33, 4, 99, 111, 114, 101],
            ),
            (
                ForegroundDbCommandRequest::WriteState { tx_id: [7; 16] },
                [vec![23], vec![7; 16]].concat(),
            ),
            (ForegroundDbCommandRequest::DrainMutationErrors, vec![24]),
            (
                ForegroundDbCommandRequest::BeginStreamingMutation {
                    mutation: ForegroundMutationKind::Update,
                    table: "t".into(),
                    row_id: [7; 16],
                    cells: vec![9],
                    column: "c".into(),
                    options_json: "{}".into(),
                },
                [
                    vec![25, 1, 1, 116],
                    vec![7; 16],
                    vec![1, 9, 1, 99, 2, 123, 125],
                ]
                .concat(),
            ),
            (
                ForegroundDbCommandRequest::PushStreamingMutation {
                    upload: 128,
                    chunk: vec![9],
                },
                vec![26, 128, 1, 1, 9],
            ),
            (
                ForegroundDbCommandRequest::FinishStreamingMutation { upload: 128 },
                vec![27, 128, 1],
            ),
            (
                ForegroundDbCommandRequest::AbortStreamingMutation { upload: 128 },
                vec![28, 128, 1],
            ),
            (
                ForegroundDbCommandRequest::LocalCurrentRow {
                    table: "t".into(),
                    row_id: [7; 16],
                },
                [vec![29, 1, 116], vec![7; 16]].concat(),
            ),
            (
                ForegroundDbCommandRequest::UpdateLargeValues {
                    table: "t".into(),
                    row_id: [7; 16],
                    patch: vec![9],
                    descriptors_json: "[]".into(),
                    updated_at_ms: Some(128),
                },
                [
                    vec![30, 1, 116],
                    vec![7; 16],
                    vec![1, 9, 2, 91, 93, 1, 128, 1],
                ]
                .concat(),
            ),
            (
                ForegroundDbCommandRequest::DirectMutation {
                    mutation: ForegroundMutationKind::Insert,
                    table: "t".into(),
                    row_id: Some([7; 16]),
                    cells: vec![9],
                    options_json: "{}".into(),
                },
                [vec![31, 0, 1, 116, 1], vec![7; 16], vec![1, 9, 2, 123, 125]].concat(),
            ),
        ];
        for (command, bytes) in cases {
            assert_eq!(postcard::to_allocvec(&command).unwrap(), bytes);
            assert_eq!(
                postcard::from_bytes::<ForegroundDbCommandRequest>(&bytes).unwrap(),
                command
            );
        }
        let responses = [
            (
                ForegroundDbCommandResponse::NativeSessionMetadata {
                    node: [9; 16],
                    registry_authority: "r".into(),
                    account_id: None,
                    issuer: "i".into(),
                    user_id: "u".into(),
                },
                [vec![17], vec![9; 16], vec![1, 114, 0, 1, 105, 1, 117]].concat(),
            ),
            (
                ForegroundDbCommandResponse::NativeSessionMetadata {
                    node: [9; 16],
                    registry_authority: "r".into(),
                    account_id: Some([7; 16]),
                    issuer: "i".into(),
                    user_id: "u".into(),
                },
                [
                    vec![17],
                    vec![9; 16],
                    vec![1, 114, 1],
                    vec![7; 16],
                    vec![1, 105, 1, 117],
                ]
                .concat(),
            ),
            (
                ForegroundDbCommandResponse::WriteState {
                    state_json: "{}".into(),
                },
                vec![18, 2, 123, 125],
            ),
            (
                ForegroundDbCommandResponse::MutationErrors {
                    events_json: "[]".into(),
                },
                vec![19, 2, 91, 93],
            ),
            (
                ForegroundDbCommandResponse::StreamingMutationOpened { upload: 128 },
                vec![20, 128, 1],
            ),
            (
                ForegroundDbCommandResponse::StreamingMutationPushed,
                vec![21],
            ),
            (
                ForegroundDbCommandResponse::StreamingMutationAborted { aborted: true },
                vec![22, 1],
            ),
            (
                ForegroundDbCommandResponse::MutationCommitted {
                    tx_id: [7; 16],
                    row_id: [8; 16],
                },
                [vec![23], vec![7; 16], vec![8; 16]].concat(),
            ),
        ];
        for (response, bytes) in responses {
            assert_eq!(postcard::to_allocvec(&response).unwrap(), bytes);
            assert_eq!(
                postcard::from_bytes::<ForegroundDbCommandResponse>(&bytes).unwrap(),
                response
            );
        }
    }

    #[test]
    fn c_host_serializes_concurrent_commands() {
        let host = jazz_native_relay_host_new() as usize;
        let probe = postcard::to_allocvec(&RelayCommandRequest::Probe).unwrap();
        let workers = (0..8)
            .map(|_| {
                let probe = probe.clone();
                std::thread::spawn(move || {
                    for _ in 0..100 {
                        let mut output = JazzNativeRelayBytes::EMPTY;
                        assert_eq!(
                            unsafe {
                                jazz_native_relay_host_execute(
                                    host as *mut JazzNativeRelayHost,
                                    probe.as_ptr(),
                                    probe.len(),
                                    &mut output,
                                )
                            },
                            JazzNativeRelayStatus::Ok
                        );
                        unsafe { jazz_native_relay_bytes_free(&mut output) };
                    }
                })
            })
            .collect::<Vec<_>>();
        for worker in workers {
            worker.join().unwrap();
        }
        unsafe { jazz_native_relay_host_free(host as *mut JazzNativeRelayHost) };
    }

    #[test]
    fn terminal_owner_aliases_cannot_reopen_or_run_after_panic() {
        let directory = tempfile::tempdir().unwrap();
        let registry = NativeRelayRegistry::default();
        let relay_config = config(directory.path().join("terminal.sqlite"), Some("alice"));
        let stale = registry.open(relay_config.clone()).unwrap();
        assert!(matches!(
            stale.run::<()>(|_| panic!("planted owner failure")),
            Err(RelayError::Closed)
        ));
        assert!(matches!(stale.pump(), Err(RelayError::Closed)));
        let replacement = registry.open(relay_config).unwrap();
        assert!(!Arc::ptr_eq(&stale.inner, &replacement.inner));
        assert!(replacement.pump().is_ok());
    }

    #[test]
    fn closing_a_scope_terminally_retires_held_aliases() {
        let directory = tempfile::tempdir().unwrap();
        let registry = NativeRelayRegistry::default();
        let relay_config = config(directory.path().join("close.sqlite"), Some("alice"));
        let stale = registry.open(relay_config.clone()).unwrap();
        assert!(registry.close(&relay_config.scope).unwrap());
        assert!(matches!(stale.pump(), Err(RelayError::Closed)));
        assert!(registry.open(relay_config).unwrap().pump().is_ok());
    }
}
