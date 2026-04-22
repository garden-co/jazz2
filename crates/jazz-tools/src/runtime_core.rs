//! RuntimeCore - Unified synchronous runtime logic for both native and WASM.
//!
//! This module provides the shared core logic that both jazz-tokio
//! and jazz-wasm wrap. RuntimeCore is generic over `Storage` and `Scheduler`
//! which provide platform-specific behavior.
//!
//! ## Design
//!
//! - `immediate_tick()` - processes managers synchronously, schedules batched_tick if needed
//! - `batched_tick()` - sends sync messages, applies parked responses/messages, calls immediate_tick
//! - Queries return `QueryFuture` for cross-platform awaiting
//! - Sync messages are "parked" and processed in batched_tick
//!
//! ## Usage
//!
//! ```ignore
//! let runtime = RuntimeCore::new(schema_manager, storage, scheduler);
//! runtime.insert(
//!     "users",
//!     std::collections::HashMap::from([
//!         ("id".to_string(), id),
//!         ("name".to_string(), name),
//!     ]),
//! )?;
//! runtime.immediate_tick();
//! let future = runtime.query(query);
//! let results = future.await?;
//! ```

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::oneshot;
use tracing::{debug, debug_span, info, trace, trace_span};

use crate::object::{BranchName, ObjectId};
use crate::query_manager::QuerySubscriptionId;
use crate::query_manager::manager::{QueryError, QueryUpdate};
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{
    OrderedRowDelta, Schema, SchemaHash, TableName, TablePolicies, Value,
};
use crate::row_format::decode_row;
use crate::row_histories::BatchId;
use crate::schema_manager::{Lens, SchemaManager};
use crate::storage::Storage;
use crate::sync_manager::{
    ClientId, DurabilityTier, InboxEntry, OutboxEntry, RowBatchKey, ServerId,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryLocalOverlay {
    pub batch_id: BatchId,
    pub branch_name: BranchName,
    pub row_ids: Vec<ObjectId>,
}

// ============================================================================
// Scheduler and SyncSender traits
// ============================================================================

/// Schedules batched ticks on the platform's event loop.
///
/// No `Send` bound — WASM types (`Rc`, `Function`) are `!Send`.
/// Tokio enforces `Send` at the point of use (`Arc<Mutex<...>>`).
pub trait Scheduler {
    fn schedule_batched_tick(&self);
}

/// Sends sync messages to the network.
///
/// No `Send` bound — WASM types are `!Send`. Send is enforced
/// by the concrete wrapping type where needed.
pub trait SyncSender {
    fn send_sync_message(&self, message: OutboxEntry);
    fn as_any(&self) -> &dyn Any;
}

// ============================================================================
// Test helpers
// ============================================================================

/// No-op scheduler for tests — tests call tick explicitly.
pub struct NoopScheduler;

impl Scheduler for NoopScheduler {
    fn schedule_batched_tick(&self) {}
}

/// Collects sync messages for test inspection.
pub struct VecSyncSender {
    messages: std::sync::Mutex<Vec<OutboxEntry>>,
}

impl Default for VecSyncSender {
    fn default() -> Self {
        Self {
            messages: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl VecSyncSender {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take all collected messages.
    pub fn take(&self) -> Vec<OutboxEntry> {
        std::mem::take(&mut self.messages.lock().unwrap())
    }
}

impl SyncSender for VecSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        self.messages.lock().unwrap().push(message);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Handle to a subscription managed by RuntimeCore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub u64);

// Re-export QueryHandle from query_manager for convenience
pub use crate::query_manager::manager::QueryHandle as QMQueryHandle;
pub use subscriptions::ReadDurabilityOptions;

/// Errors from runtime operations.
#[derive(Debug, Clone)]
pub enum RuntimeError {
    QueryError(String),
    WriteError(String),
    NotFound,
    AnonymousWriteDenied {
        table: crate::query_manager::types::TableName,
        operation: crate::query_manager::policy::Operation,
    },
}

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RuntimeError::QueryError(s) => write!(f, "Query error: {}", s),
            RuntimeError::WriteError(s) => write!(f, "Write error: {}", s),
            RuntimeError::NotFound => write!(f, "Not found"),
            RuntimeError::AnonymousWriteDenied { table, operation } => {
                write!(
                    f,
                    "anonymous session cannot {} on table {}",
                    operation, table
                )
            }
        }
    }
}

impl std::error::Error for RuntimeError {}

impl From<QueryError> for RuntimeError {
    fn from(e: QueryError) -> Self {
        match e {
            QueryError::AnonymousWriteDenied { table, operation } => {
                RuntimeError::AnonymousWriteDenied { table, operation }
            }
            other => RuntimeError::QueryError(other.to_string()),
        }
    }
}

/// Convert a `QueryError` from a write path, preserving the
/// `AnonymousWriteDenied` variant and mapping anything else to `WriteError`.
pub(crate) fn write_error_from_query(e: QueryError) -> RuntimeError {
    match e {
        QueryError::AnonymousWriteDenied { table, operation } => {
            RuntimeError::AnonymousWriteDenied { table, operation }
        }
        other => RuntimeError::WriteError(other.to_string()),
    }
}

/// Type alias for query results.
pub type QueryResult = Result<Vec<(ObjectId, Vec<Value>)>, RuntimeError>;
/// Type alias for inserted row payloads.
pub type InsertedRow = (ObjectId, Vec<Value>);
/// Type alias for plain insert results that carry the inserted row plus its logical batch id.
pub type DirectInsertResult = (InsertedRow, BatchId);

/// Structured rejection returned by persisted writes when their batch is rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PersistedWriteRejection {
    pub batch_id: BatchId,
    pub code: String,
    pub reason: String,
}

/// Terminal outcome for a persisted write wait.
pub type PersistedWriteAck = std::result::Result<(), PersistedWriteRejection>;

/// Future that resolves to query results.
///
/// Cross-platform future implementation using `futures::channel::oneshot`.
/// Works with both tokio and wasm_bindgen_futures executors.
pub struct QueryFuture {
    receiver: oneshot::Receiver<QueryResult>,
}

impl QueryFuture {
    /// Create a new QueryFuture from a oneshot receiver.
    pub fn new(receiver: oneshot::Receiver<QueryResult>) -> Self {
        Self { receiver }
    }
}

impl Future for QueryFuture {
    type Output = QueryResult;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.receiver)
            .poll(cx)
            .map(|r| r.unwrap_or_else(|_| Err(RuntimeError::QueryError("Query cancelled".into()))))
    }
}

/// Sender for fulfilling a QueryFuture.
pub type QuerySender = oneshot::Sender<QueryResult>;

/// Result of an immediate_tick cycle.
#[derive(Debug, Default)]
pub struct TickOutput {
    /// Subscription updates for this tick.
    pub subscription_updates: Vec<QueryUpdate>,
}

/// Delta for a subscription callback.
#[derive(Debug, Clone)]
pub struct SubscriptionDelta {
    /// The subscription handle.
    pub handle: SubscriptionHandle,
    /// The row changes with position-annotated ordering.
    pub ordered_delta: OrderedRowDelta,
    /// Output descriptor for decoding the binary row data.
    /// Use with `decode_row(&descriptor, &row.data)` to get `Vec<Value>`.
    pub descriptor: crate::query_manager::types::RowDescriptor,
}

/// Callback type for subscriptions.
///
/// On native platforms, callbacks must be `Send` for thread safety.
/// On WASM (single-threaded), `Send` is not required.
#[cfg(target_arch = "wasm32")]
pub type SubscriptionCallback = Box<dyn Fn(SubscriptionDelta) + 'static>;

#[cfg(not(target_arch = "wasm32"))]
pub type SubscriptionCallback = Box<dyn Fn(SubscriptionDelta) + Send + 'static>;

/// State for a subscription.
struct SubscriptionState {
    /// QueryManager's internal subscription ID.
    query_sub_id: QuerySubscriptionId,
    /// Callback invoked on updates.
    callback: SubscriptionCallback,
}

/// Pending one-shot query waiting for first subscription callback.
struct PendingOneShotQuery {
    subscription_id: QuerySubscriptionId,
    sender: Option<QuerySender>,
}

/// Unified runtime core for both native and WASM platforms.
///
/// Generic over `Storage` for data persistence and `Scheduler` for tick scheduling.
/// All business logic is synchronous.
pub struct RuntimeCore<S: Storage, Sch: Scheduler> {
    schema_manager: SchemaManager,
    pub(crate) storage: S,
    scheduler: Sch,
    /// True when storage was mutated since the last WAL flush barrier.
    storage_write_pending_flush: bool,
    /// Transport handle for WebSocket sync.
    pub(crate) transport: Option<crate::transport_manager::TransportHandle>,
    /// Fallback outbox sender used when no `TransportHandle` is set (e.g. on
    /// the server side, where the runtime fans out via `ConnectionEventHub`
    /// instead of a WebSocket connection).
    pub(crate) sync_sender: Option<Box<dyn SyncSender + Send>>,

    /// Parked sync messages (from network).
    parked_sync_messages: Vec<InboxEntry>,
    /// Sequenced server messages buffered for in-order application.
    parked_sync_messages_by_server_seq: HashMap<ServerId, BTreeMap<u64, InboxEntry>>,
    /// Next expected per-server stream sequence.
    next_expected_server_seq: HashMap<ServerId, u64>,
    /// Highest per-server stream sequence already applied to the inbox.
    last_applied_server_seq: HashMap<ServerId, u64>,

    /// Subscription tracking with callbacks.
    subscriptions: HashMap<SubscriptionHandle, SubscriptionState>,
    /// Reverse map for routing updates.
    subscription_reverse: HashMap<QuerySubscriptionId, SubscriptionHandle>,
    next_subscription_handle: u64,
    /// Created-but-not-yet-executed subscriptions (2-phase subscribe).
    pending_subscriptions: HashMap<SubscriptionHandle, subscriptions::PendingSubscription>,

    /// Pending one-shot queries (query() calls waiting for first callback).
    pending_one_shot_queries: HashMap<SubscriptionHandle, PendingOneShotQuery>,

    /// Watchers for persistence acks: (row, branch, logical write batch, requested_tier) → senders.
    /// A tier >= requested tier satisfies the watcher (e.g., EdgeServer ack satisfies Local).
    ack_watchers: HashMap<RowBatchKey, Vec<(DurabilityTier, oneshot::Sender<PersistedWriteAck>)>>,

    /// Rejected replayable batch ids that should be surfaced once to bindings.
    rejected_batch_ids: BTreeSet<crate::row_histories::BatchId>,

    /// Label for tracing (e.g. "local", "edge", "client").
    tier_label: &'static str,

    /// Optional sync-message tracer used by tests to record outgoing/incoming
    /// payloads under a human-readable participant name. `None` in production.
    pub(crate) sync_tracer: Option<(crate::sync_tracer::SyncTracer, String)>,

    /// Called when the transport rejects auth during the WS handshake.
    /// The String argument is a human-readable reason (e.g. "Unauthorized").
    pub(crate) auth_failure_callback: Option<Box<dyn Fn(String) + Send + 'static>>,
}

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    /// Create a new RuntimeCore.
    pub fn new(mut schema_manager: SchemaManager, mut storage: S, scheduler: Sch) -> Self {
        let _ = schema_manager.ensure_current_schema_persisted(&mut storage);
        schema_manager
            .query_manager_mut()
            .set_auto_apply_query_settled(false);
        let rejected_batch_ids = storage
            .scan_local_batch_records()
            .map(|records| {
                records
                    .into_iter()
                    .filter_map(|record| {
                        matches!(
                            record.latest_settlement,
                            Some(crate::batch_fate::BatchSettlement::Rejected { .. })
                        )
                        .then_some(record.batch_id)
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self {
            schema_manager,
            storage,
            scheduler,
            storage_write_pending_flush: false,
            transport: None,
            sync_sender: None,
            parked_sync_messages: Vec::new(),
            parked_sync_messages_by_server_seq: HashMap::new(),
            next_expected_server_seq: HashMap::new(),
            last_applied_server_seq: HashMap::new(),
            subscriptions: HashMap::new(),
            subscription_reverse: HashMap::new(),
            next_subscription_handle: 0,
            pending_subscriptions: HashMap::new(),
            pending_one_shot_queries: HashMap::new(),
            ack_watchers: HashMap::new(),
            rejected_batch_ids,
            tier_label: "unknown",
            sync_tracer: None,
            auth_failure_callback: None,
        }
    }

    /// Set the tier label used in tracing spans.
    pub fn set_tier_label(&mut self, label: &'static str) {
        self.tier_label = label;
    }

    /// Register a callback that fires when the transport receives an auth failure
    /// from the server during the WS handshake.  The callback receives a
    /// human-readable reason string (e.g. "Unauthorized").
    pub fn set_auth_failure_callback(&mut self, cb: impl Fn(String) + Send + 'static) {
        self.auth_failure_callback = Some(Box::new(cb));
    }

    /// Attach a sync-message tracer. All outbox entries this runtime sends
    /// and all inbox entries it receives will be recorded under `name`.
    pub fn set_sync_tracer(&mut self, tracer: crate::sync_tracer::SyncTracer, name: String) {
        self.sync_tracer = Some((tracer, name));
    }

    /// Get mutable reference to the Storage.
    pub fn storage_mut(&mut self) -> &mut S {
        &mut self.storage
    }

    /// Get reference to the Storage.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Flush the storage to persistent medium.
    pub fn flush_storage(&self) {
        self.storage.flush();
    }

    /// Flush only the WAL buffer (not the full snapshot).
    pub fn flush_wal(&self) {
        self.storage.flush_wal();
    }

    pub(crate) fn mark_storage_write_pending_flush(&mut self) {
        self.storage_write_pending_flush = true;
    }

    pub(crate) fn clear_storage_write_pending_flush(&mut self) {
        self.storage_write_pending_flush = false;
    }

    /// Consume RuntimeCore and return the Storage.
    /// Used for cold-start testing to transfer driver state.
    pub fn into_storage(self) -> S {
        self.storage
    }

    /// Get reference to the Scheduler.
    pub fn scheduler(&self) -> &Sch {
        &self.scheduler
    }

    /// Get mutable reference to the Scheduler.
    pub fn scheduler_mut(&mut self) -> &mut Sch {
        &mut self.scheduler
    }

    /// Persist the current schema to the catalogue for server sync.
    pub fn persist_schema(&mut self) -> ObjectId {
        let id = self.schema_manager.persist_schema(&mut self.storage);
        self.mark_storage_write_pending_flush();
        info!(object_id = %id, "persisted schema to catalogue");
        id
    }

    /// Publish any known schema object to the catalogue and in-memory schema manager.
    pub fn publish_schema(&mut self, schema: Schema) -> ObjectId {
        let schema_hash = crate::query_manager::types::SchemaHash::compute(&schema);

        if self.schema_manager.get_known_schema(&schema_hash).is_none() {
            self.schema_manager.add_known_schema(schema.clone());
        }

        let id = self
            .schema_manager
            .persist_schema_object(&mut self.storage, &schema);
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        id
    }

    pub fn publish_permissions_bundle(
        &mut self,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, crate::schema_manager::SchemaError> {
        let id = self.schema_manager.publish_permissions_bundle(
            &mut self.storage,
            schema_hash,
            permissions,
            expected_parent_bundle_object_id,
        )?;
        if id.is_some() {
            self.mark_storage_write_pending_flush();
        }
        self.immediate_tick();
        Ok(id)
    }

    /// Publish a reviewed lens edge to the active schema manager and catalogue.
    pub fn publish_lens(&mut self, lens: &Lens) -> Result<ObjectId, RuntimeError> {
        let id = self
            .schema_manager
            .publish_lens(&mut self.storage, lens)
            .map_err(|error| RuntimeError::WriteError(error.to_string()))?;
        self.mark_storage_write_pending_flush();
        self.immediate_tick();
        Ok(id)
    }
    // =========================================================================
    // Schema/State Access
    // =========================================================================

    /// Get the current schema.
    pub fn current_schema(&self) -> &Schema {
        self.schema_manager.current_schema()
    }

    /// Get mutable access to the underlying SchemaManager.
    pub fn schema_manager_mut(&mut self) -> &mut SchemaManager {
        &mut self.schema_manager
    }

    /// Add a historical live schema and persist both schema and lens catalogue objects.
    pub fn add_live_schema_and_persist_catalogue(
        &mut self,
        schema: Schema,
    ) -> Result<(), crate::schema_manager::context::SchemaError> {
        let lens = self.schema_manager.add_live_schema(schema.clone())?.clone();
        self.schema_manager
            .persist_schema_object(&mut self.storage, &schema);
        self.schema_manager.persist_lens(&mut self.storage, &lens);
        Ok(())
    }

    /// Get access to the underlying SchemaManager.
    pub fn schema_manager(&self) -> &SchemaManager {
        &self.schema_manager
    }
}

/// Create a `TransportManager`, seed it with the current catalogue state hash,
/// install its handle on the given core, and return the manager for the caller
/// to spawn on an appropriate executor.
///
/// Centralises the boilerplate that would otherwise be duplicated in every
/// binding (Tokio, NAPI, RN, WASM).
#[cfg(feature = "transport")]
pub fn install_transport<S, Sch, W, T>(
    core: &mut RuntimeCore<S, Sch>,
    url: String,
    auth: crate::transport_manager::AuthConfig,
    tick: T,
) -> crate::transport_manager::TransportManager<W, T>
where
    S: crate::storage::Storage,
    Sch: Scheduler,
    W: crate::transport_manager::StreamAdapter + 'static,
    T: crate::transport_manager::TickNotifier + 'static,
{
    debug_assert!(
        core.transport().is_none(),
        "install_transport called while a transport is already installed; call clear_transport / disconnect first"
    );
    let (handle, manager) = crate::transport_manager::create::<W, T>(url, auth, tick);
    handle.set_catalogue_state_hash(Some(core.schema_manager().catalogue_state_hash()));
    handle.set_declared_schema_hash(
        core.schema_manager()
            .has_current_schema()
            .then(|| core.schema_manager().current_hash().to_string()),
    );
    core.schema_manager
        .query_manager_mut()
        .sync_manager_mut()
        .add_pending_server(handle.server_id);
    core.set_transport(handle);
    manager
}

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    /// Attach a transport handle. Replaces any existing transport.
    pub fn set_transport(&mut self, handle: crate::transport_manager::TransportHandle) {
        self.transport = Some(handle);
    }

    /// Detach the transport handle and remove its server from sync state.
    pub fn clear_transport(&mut self) {
        if let Some(h) = self.transport.take() {
            self.remove_server(h.server_id);
        }
    }

    /// Returns a reference to the active transport handle, if any.
    pub fn transport(&self) -> Option<&crate::transport_manager::TransportHandle> {
        self.transport.as_ref()
    }
}

impl<S: Storage, Sch: Scheduler> RuntimeCore<S, Sch> {
    /// Install a fallback sync sender used when no `TransportHandle` is set.
    /// On the server side, this is the bridge from the runtime's outbox into
    /// the per-connection `ConnectionEventHub` channels.
    pub fn set_sync_sender(&mut self, sender: Box<dyn SyncSender + Send>) {
        self.sync_sender = Some(sender);
    }

    #[cfg(test)]
    pub fn sync_sender(&self) -> &VecSyncSender {
        self.sync_sender
            .as_ref()
            .expect("test runtime must install a VecSyncSender")
            .as_any()
            .downcast_ref::<VecSyncSender>()
            .expect("test runtime sync sender must be VecSyncSender")
    }
}

mod subscriptions;
mod sync;
mod ticks;
mod writes;

#[cfg(test)]
mod tests;
