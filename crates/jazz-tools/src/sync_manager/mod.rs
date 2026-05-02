use std::collections::{HashMap, HashSet};
use std::time::Duration;

use web_time::Instant;

use crate::batch_fate::{BatchSettlement, SealedBatchSubmission};
use crate::catalogue::CatalogueEntry;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::row_histories::{BatchId, RowVisibilityChange};
use crate::storage::Storage;

// Module declarations
pub mod clock;
pub mod forwarding;
pub mod inbox;
pub mod permissions;
pub mod sync_logic;
pub mod sync_tracer;
pub mod types;

use clock::MonotonicClock;

#[cfg(test)]
mod tests;

// Re-export all public types
pub use types::*;

/// How long an installed transport may sit in `pending_servers` before callers
/// treat it as offline.
pub const PENDING_SERVER_TIMEOUT: Duration = Duration::from_secs(2);

// ============================================================================
// SyncManager
// ============================================================================

/// Manages synchronization state atop storage-backed row and catalogue state.
///
/// Coordinates:
/// - Upstream servers (trusted, receive all our objects)
/// - Downstream clients (untrusted, receive query-filtered subsets)
#[derive(Clone)]
pub struct SyncManager {
    pub(super) clock: MonotonicClock,
    pub(super) catalogue_entries: HashMap<ObjectId, CatalogueEntry>,
    pub(super) allow_unprivileged_schema_catalogue_writes: bool,

    pub(super) servers: HashMap<ServerId, ServerState>,
    pub(super) pending_servers: HashMap<ServerId, Instant>,
    pub(super) clients: HashMap<ClientId, ClientState>,

    pub(super) inbox: Vec<InboxEntry>,
    pub(super) outbox: Vec<OutboxEntry>,
    /// Pending permission checks awaiting policy evaluation.
    pub(super) pending_permission_checks: Vec<PendingPermissionCheck>,
    /// Pending query subscriptions awaiting QueryGraph building by QueryManager.
    pub(super) pending_query_subscriptions: Vec<PendingQuerySubscription>,
    /// Pending query unsubscriptions awaiting cleanup by QueryManager.
    pub(super) pending_query_unsubscriptions: Vec<PendingQueryUnsubscription>,
    /// Row visibility changes applied through row-history sync.
    pub(super) pending_row_visibility_changes: Vec<RowVisibilityChange>,
    /// Catalogue/system entry updates awaiting SchemaManager processing.
    pub(super) pending_catalogue_updates: Vec<CatalogueEntry>,

    pub(super) next_pending_id: u64,

    /// This node's durability identities (empty = don't emit durability notifications).
    pub(super) my_tiers: HashSet<DurabilityTier>,
    /// Tracks which clients are interested in row batch-member state updates.
    pub(super) row_batch_interest: HashMap<RowBatchKey, HashSet<ClientId>>,

    /// Tracks which clients originated each query (for relaying QuerySettled).
    pub(super) query_origin: HashMap<QueryId, HashSet<ClientId>>,
    /// Latest remote scope snapshots keyed by upstream server and query id.
    pub(super) remote_query_scopes: HashMap<(ServerId, QueryId), HashSet<(ObjectId, BranchName)>>,
    /// Query ids whose remote scope changed since the last QueryManager process.
    pub(super) remote_query_scope_dirty: HashSet<QueryId>,
    /// Pending QuerySettled notifications for QueryManager to process.
    pub(super) pending_query_settled: Vec<PendingQuerySettled>,
    /// Pending query rejections waiting for QueryManager to fail local subscriptions.
    pub(super) pending_query_rejections: Vec<PendingQueryRejection>,
    /// Pending replayable batch settlements for RuntimeCore to process.
    pub(super) pending_batch_settlements: Vec<BatchSettlement>,

    /// Row batch-member state acks received during inbox processing.
    pub(super) received_row_batch_acks: Vec<(RowBatchKey, DurabilityTier)>,
}

impl std::fmt::Debug for SyncManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncManager")
            .field("clock", &self.clock)
            .field("catalogue_entries", &self.catalogue_entries)
            .field(
                "allow_unprivileged_schema_catalogue_writes",
                &self.allow_unprivileged_schema_catalogue_writes,
            )
            .field("servers", &self.servers)
            .field("pending_servers", &self.pending_servers)
            .field("clients", &self.clients)
            .field("inbox", &self.inbox)
            .field("outbox", &self.outbox)
            .field("pending_permission_checks", &self.pending_permission_checks)
            .field(
                "pending_query_subscriptions",
                &self.pending_query_subscriptions,
            )
            .field(
                "pending_query_unsubscriptions",
                &self.pending_query_unsubscriptions,
            )
            .field(
                "pending_row_visibility_changes",
                &self.pending_row_visibility_changes,
            )
            .field("pending_catalogue_updates", &self.pending_catalogue_updates)
            .field("next_pending_id", &self.next_pending_id)
            .field("my_tiers", &self.my_tiers)
            .field("row_batch_interest", &self.row_batch_interest)
            .field("query_origin", &self.query_origin)
            .field("remote_query_scopes", &self.remote_query_scopes)
            .field("pending_query_settled", &self.pending_query_settled)
            .field("pending_query_rejections", &self.pending_query_rejections)
            .field("pending_batch_settlements", &self.pending_batch_settlements)
            .field("received_row_batch_acks", &self.received_row_batch_acks)
            .finish()
    }
}

impl Default for SyncManager {
    fn default() -> Self {
        Self::new()
    }
}

fn short_hash(hash: &impl ToString) -> String {
    hash.to_string().chars().take(12).collect()
}

pub(crate) fn log_schema_warning(
    warning: &SchemaWarning,
    origin: Option<&str>,
    subscription_id: Option<u64>,
) {
    tracing::warn!(
        origin = origin,
        sub_id = subscription_id,
        query_id = warning.query_id.0,
        table = warning.table_name,
        row_count = warning.row_count,
        from_hash = %warning.from_hash,
        to_hash = %warning.to_hash,
        "Detected {} rows of {} with differing schema versions. To ensure data visibility and forward/backward compatibility, run `npx jazz-tools@alpha schema export --schema-hash {}`. Then generate a migration with `npx jazz-tools@alpha migrations create --fromHash {} --toHash <targetHash>`.",
        warning.row_count,
        warning.table_name,
        short_hash(&warning.from_hash),
        short_hash(&warning.from_hash),
    );
}

pub(crate) fn log_connection_schema_diagnostics(
    diagnostics: &ConnectionSchemaDiagnostics,
    origin: Option<&str>,
) {
    let client_hash = short_hash(&diagnostics.client_schema_hash);

    if let Some(permissions_hash) = diagnostics.disconnected_permissions_schema_hash {
        let permissions_hash = short_hash(&permissions_hash);
        tracing::error!(
            origin = origin,
            client_schema_hash = %client_hash,
            permissions_schema_hash = %permissions_hash,
            "Your declared schema {} is disconnected from the schema used to enforce permissions: {}. Reads and writes may fail until you add a migration. To recover, run `npx jazz-tools@alpha migrations create --fromHash {} --toHash {}`.",
            client_hash,
            permissions_hash,
            permissions_hash,
            client_hash,
        );
    }

    if !diagnostics.unreachable_schema_hashes.is_empty() {
        let unreachable_hashes: Vec<String> = diagnostics
            .unreachable_schema_hashes
            .iter()
            .map(short_hash)
            .collect();
        tracing::warn!(
            origin = origin,
            client_schema_hash = %client_hash,
            unreachable_schema_hashes = ?unreachable_hashes,
            "Server knows schema branches that are unreachable from your declared schema {}: {}. Some data may be missing from reads until you add migrations. To recover, run `npx jazz-tools@alpha migrations create --fromHash <unreachableHash> --toHash {}` for each listed schema.",
            client_hash,
            unreachable_hashes.join(", "),
            client_hash,
        );
    }
}

impl SyncManager {
    pub fn new() -> Self {
        Self {
            clock: MonotonicClock::new(),
            catalogue_entries: HashMap::new(),
            allow_unprivileged_schema_catalogue_writes: false,
            servers: HashMap::new(),
            pending_servers: HashMap::new(),
            clients: HashMap::new(),
            inbox: Vec::new(),
            outbox: Vec::new(),
            pending_permission_checks: Vec::new(),
            pending_query_subscriptions: Vec::new(),
            pending_query_unsubscriptions: Vec::new(),
            pending_row_visibility_changes: Vec::new(),
            pending_catalogue_updates: Vec::new(),
            next_pending_id: 0,
            my_tiers: HashSet::new(),
            row_batch_interest: HashMap::new(),
            query_origin: HashMap::new(),
            remote_query_scopes: HashMap::new(),
            remote_query_scope_dirty: HashSet::new(),
            pending_query_settled: Vec::new(),
            pending_query_rejections: Vec::new(),
            pending_batch_settlements: Vec::new(),
            received_row_batch_acks: Vec::new(),
        }
    }

    pub fn reserve_timestamp(&mut self) -> u64 {
        self.clock.reserve_timestamp()
    }

    /// Add a durability identity for this node (enables durability notifications).
    pub fn with_durability_tier(mut self, tier: DurabilityTier) -> Self {
        self.my_tiers.insert(tier);
        self
    }

    /// Allow authenticated user clients to publish structural schema catalogue
    /// objects directly. Intended for development servers only.
    pub fn with_unprivileged_schema_catalogue_writes(mut self) -> Self {
        self.allow_unprivileged_schema_catalogue_writes = true;
        self
    }

    /// Add multiple durability identities for this node.
    pub fn with_durability_tiers<I>(mut self, tiers: I) -> Self
    where
        I: IntoIterator<Item = DurabilityTier>,
    {
        self.my_tiers.extend(tiers);
        self
    }

    /// True when this runtime instance represents a durability tier identity
    /// (worker/edge/global) rather than a top-level client.
    pub fn has_durability_identity(&self) -> bool {
        !self.my_tiers.is_empty()
    }

    /// True when this node can satisfy acknowledgements for the requested tier
    /// using one of its local durability identities.
    pub fn has_local_durability_at_least(&self, requested_tier: DurabilityTier) -> bool {
        self.my_tiers
            .iter()
            .any(|local_tier| *local_tier >= requested_tier)
    }

    /// Return this node's local durability identities.
    pub fn local_durability_tiers(&self) -> HashSet<DurabilityTier> {
        self.my_tiers.clone()
    }

    /// True when this runtime currently has at least one upstream server.
    pub fn has_connected_servers(&self) -> bool {
        !self.servers.is_empty()
    }

    /// Return the strongest durability tier this node can attest to locally.
    pub fn max_local_durability_tier(&self) -> Option<DurabilityTier> {
        self.my_tiers.iter().copied().max()
    }

    /// Approximate heap-backed memory owned by sync state, grouped for benches.
    ///
    /// Returns `(catalogue, connections, subscriptions, queues, total)`.
    pub fn memory_size(&self) -> (usize, usize, usize, usize, usize) {
        let mut catalogue = 0usize;
        for (object_id, entry) in &self.catalogue_entries {
            catalogue += std::mem::size_of_val(object_id);
            catalogue += std::mem::size_of_val(entry);
            catalogue += 48;
        }

        let mut connections = 0usize;
        for (server_id, state) in &self.servers {
            connections += std::mem::size_of_val(server_id);
            connections += std::mem::size_of_val(state);
            connections += 48;
            connections += state.sent_metadata.len() * std::mem::size_of::<ObjectId>();
            for ((object_id, branch_name), batch_ids) in &state.sent_batch_ids {
                connections += std::mem::size_of_val(object_id);
                connections += std::mem::size_of_val(branch_name);
                connections += batch_ids.len() * std::mem::size_of::<BatchId>();
                connections += 48;
            }
        }
        for (client_id, state) in &self.clients {
            connections += std::mem::size_of_val(client_id);
            connections += std::mem::size_of_val(state);
            connections += 48;
            connections += state.sent_metadata.len() * std::mem::size_of::<ObjectId>();
            if let Some(session) = &state.session {
                connections += session.user_id.len();
            }
            for ((object_id, branch_name), batch_ids) in &state.sent_batch_ids {
                connections += std::mem::size_of_val(object_id);
                connections += std::mem::size_of_val(branch_name);
                connections += batch_ids.len() * std::mem::size_of::<BatchId>();
                connections += 48;
            }
        }
        connections += self.my_tiers.len() * std::mem::size_of::<DurabilityTier>();

        let mut subscriptions = 0usize;
        for state in self.clients.values() {
            for (query_id, scope) in &state.queries {
                subscriptions += std::mem::size_of_val(query_id);
                subscriptions += std::mem::size_of_val(scope);
                subscriptions += scope.scope.len() * std::mem::size_of::<(ObjectId, BranchName)>();
                if let Some(session) = &scope.session {
                    subscriptions += session.user_id.len();
                }
                subscriptions += 48;
            }
        }
        for (row_batch_key, clients) in &self.row_batch_interest {
            subscriptions += std::mem::size_of_val(row_batch_key);
            subscriptions += clients.len() * std::mem::size_of::<ClientId>();
            subscriptions += 48;
        }
        for (query_id, clients) in &self.query_origin {
            subscriptions += std::mem::size_of_val(query_id);
            subscriptions += clients.len() * std::mem::size_of::<ClientId>();
            subscriptions += 48;
        }
        for (key, scope) in &self.remote_query_scopes {
            subscriptions += std::mem::size_of_val(key);
            subscriptions += scope.len() * std::mem::size_of::<(ObjectId, BranchName)>();
            subscriptions += 48;
        }

        let queues = self.inbox.len() * std::mem::size_of::<InboxEntry>()
            + self.outbox.len() * std::mem::size_of::<OutboxEntry>()
            + self.pending_permission_checks.len() * std::mem::size_of::<PendingPermissionCheck>()
            + self.pending_query_subscriptions.len()
                * std::mem::size_of::<PendingQuerySubscription>()
            + self.pending_query_unsubscriptions.len()
                * std::mem::size_of::<PendingQueryUnsubscription>()
            + self.pending_row_visibility_changes.len()
                * std::mem::size_of::<RowVisibilityChange>()
            + self.pending_catalogue_updates.len() * std::mem::size_of::<CatalogueEntry>()
            + self.pending_query_settled.len() * std::mem::size_of::<PendingQuerySettled>()
            + self.pending_batch_settlements.len() * std::mem::size_of::<BatchSettlement>()
            + self.received_row_batch_acks.len()
                * std::mem::size_of::<(RowBatchKey, DurabilityTier)>();

        let total = catalogue + connections + subscriptions + queues;
        (catalogue, connections, subscriptions, queues, total)
    }

    // ========================================================================
    // Connection Management
    // ========================================================================

    /// Add a server connection using storage-backed current-state replay.
    pub fn add_server_with_storage<H: Storage>(
        &mut self,
        server_id: ServerId,
        skip_catalogue_sync: bool,
        storage: &H,
    ) {
        self.pending_servers.remove(&server_id);
        self.servers.insert(server_id, ServerState::default());
        self.queue_full_sync_to_server_from_storage(server_id, storage);
        if !skip_catalogue_sync {
            self.queue_catalogue_sync_to_server_from_storage(server_id, storage);
        }
    }

    pub fn add_pending_server(&mut self, server_id: ServerId) {
        if self.servers.contains_key(&server_id) {
            return;
        }
        self.pending_servers.insert(server_id, Instant::now());
    }

    pub fn remove_pending_server(&mut self, server_id: ServerId) {
        self.pending_servers.remove(&server_id);
    }

    pub fn has_servers_or_pending_servers(&self) -> bool {
        if !self.servers.is_empty() {
            return true;
        }
        let now = Instant::now();
        self.pending_servers
            .values()
            .any(|since| now.duration_since(*since) < PENDING_SERVER_TIMEOUT)
    }

    pub fn request_batch_settlements_from_server(
        &mut self,
        server_id: ServerId,
        batch_ids: Vec<crate::row_histories::BatchId>,
    ) {
        if batch_ids.is_empty() {
            return;
        }

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::BatchSettlementNeeded { batch_ids },
        });
    }

    pub fn seal_batch_to_servers(&mut self, submission: SealedBatchSubmission) {
        let now = Instant::now();
        let mut server_ids: Vec<_> = self.servers.keys().copied().collect();
        server_ids.extend(
            self.pending_servers
                .iter()
                .filter(|(_, since)| now.duration_since(**since) < PENDING_SERVER_TIMEOUT)
                .map(|(server_id, _)| *server_id),
        );
        for server_id in server_ids {
            self.outbox.push(OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::SealBatch {
                    submission: submission.clone(),
                },
            });
        }
    }

    /// Remove a server connection.
    pub fn remove_server(&mut self, server_id: ServerId) {
        self.servers.remove(&server_id);
        self.pending_servers.remove(&server_id);
        let mut removed_query_ids = HashSet::new();
        self.remote_query_scopes
            .retain(|(remote_server_id, query_id), _| {
                let keep = *remote_server_id != server_id;
                if !keep {
                    removed_query_ids.insert(*query_id);
                }
                keep
            });
        self.remote_query_scope_dirty.extend(removed_query_ids);
    }

    /// Add a client connection using storage-backed catalogue replay.
    pub fn add_client_with_storage<H: Storage>(&mut self, storage: &H, client_id: ClientId) {
        self.clients.insert(client_id, ClientState::default());
        self.queue_catalogue_sync_to_client_from_storage(client_id, storage);
    }

    /// Remove a client connection and all associated state.
    ///
    /// Returns `false` if the client has unprocessed inbox entries — the
    /// caller should retry later to avoid dropping data that hasn't been
    /// persisted to storage yet.
    pub fn remove_client(&mut self, client_id: ClientId) -> bool {
        let has_inbox = self
            .inbox
            .iter()
            .any(|e| e.source == Source::Client(client_id));

        if has_inbox {
            tracing::warn!(
                %client_id,
                "skipping reap: client has unprocessed inbox entries"
            );
            return false;
        }

        self.clients.remove(&client_id);
        // Clean up interest map
        self.row_batch_interest.retain(|_, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
        // Clean up query origin map
        self.query_origin.retain(|_, clients| {
            clients.remove(&client_id);
            !clients.is_empty()
        });
        // Clean up pending queues
        self.pending_permission_checks
            .retain(|c| c.client_id != client_id);
        self.pending_query_subscriptions
            .retain(|s| s.client_id != client_id);
        self.pending_query_unsubscriptions
            .retain(|u| u.client_id != client_id);
        // Drop queued outbox messages for this client
        self.outbox
            .retain(|e| e.destination != Destination::Client(client_id));
        true
    }

    /// Get server state.
    pub fn get_server(&self, server_id: ServerId) -> Option<&ServerState> {
        self.servers.get(&server_id)
    }

    pub fn has_servers(&self) -> bool {
        !self.servers.is_empty()
    }

    /// Get client state.
    pub fn get_client(&self, client_id: ClientId) -> Option<&ClientState> {
        self.clients.get(&client_id)
    }

    /// Set the session for a client.
    pub fn set_client_session(&mut self, client_id: ClientId, session: Session) {
        if let Some(client) = self.clients.get_mut(&client_id) {
            client.session = Some(session);
        }
    }

    /// Set the role for a client.
    pub fn set_client_role(&mut self, client_id: ClientId, role: ClientRole) {
        if let Some(client) = self.clients.get_mut(&client_id) {
            client.role = role;
        }
    }

    // ========================================================================
    // Outbox / Inbox
    // ========================================================================

    /// Take all outbox entries, clearing the outbox.
    pub fn take_outbox(&mut self) -> Vec<OutboxEntry> {
        std::mem::take(&mut self.outbox)
    }

    /// Restore previously dequeued outbox entries ahead of any newly queued ones.
    pub(crate) fn prepend_outbox(&mut self, mut entries: Vec<OutboxEntry>) {
        if entries.is_empty() {
            return;
        }
        entries.append(&mut self.outbox);
        self.outbox = entries;
    }

    /// Get a reference to the outbox (for checking if empty).
    pub fn outbox(&self) -> &[OutboxEntry] {
        &self.outbox
    }

    /// Push an entry to the inbox for processing.
    pub fn push_inbox(&mut self, entry: InboxEntry) {
        self.inbox.push(entry);
    }

    /// Process all inbox entries.
    pub fn process_inbox<H: Storage>(&mut self, storage: &mut H) {
        let entries = std::mem::take(&mut self.inbox);
        for entry in entries {
            self.process_inbox_entry(storage, entry);
        }
    }

    // ========================================================================
    // Pending Query Subscriptions
    // ========================================================================

    /// Take pending query subscriptions for QueryManager to process.
    ///
    /// QueryManager will build QueryGraphs for these and call back with computed scopes.
    pub fn take_pending_query_subscriptions(&mut self) -> Vec<PendingQuerySubscription> {
        std::mem::take(&mut self.pending_query_subscriptions)
    }

    /// Re-queue pending query subscriptions that couldn't be processed yet.
    ///
    /// Called by QueryManager when schema isn't available for some subscriptions.
    pub fn requeue_pending_query_subscriptions(&mut self, subs: Vec<PendingQuerySubscription>) {
        self.pending_query_subscriptions.extend(subs);
    }

    /// Take pending query unsubscriptions for QueryManager to process.
    ///
    /// QueryManager will remove server-side QueryGraphs and forward upstream.
    pub fn take_pending_query_unsubscriptions(&mut self) -> Vec<PendingQueryUnsubscription> {
        std::mem::take(&mut self.pending_query_unsubscriptions)
    }

    /// Storage-backed version of `set_client_query_scope` that can replay row
    /// objects directly from storage-backed visible rows.
    pub fn set_client_query_scope_with_storage<H: Storage + ?Sized>(
        &mut self,
        storage: &H,
        client_id: ClientId,
        query_id: QueryId,
        scope: HashSet<(ObjectId, BranchName)>,
        session: Option<Session>,
    ) {
        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };

        let old_query_scope = client
            .queries
            .get(&query_id)
            .map(|query| query.scope.clone())
            .unwrap_or_default();
        let old_scope: HashSet<(ObjectId, BranchName)> = client
            .queries
            .values()
            .flat_map(|q| q.scope.iter().cloned())
            .collect();

        client.queries.insert(
            query_id,
            QueryScope {
                scope: scope.clone(),
                session,
            },
        );

        let new_scope: HashSet<(ObjectId, BranchName)> = client
            .queries
            .values()
            .flat_map(|q| q.scope.iter().cloned())
            .collect();

        let no_longer_visible: HashSet<(ObjectId, BranchName)> =
            old_scope.difference(&new_scope).cloned().collect();
        let newly_visible_for_query: Vec<(ObjectId, BranchName)> =
            scope.difference(&old_query_scope).cloned().collect();

        self.prune_client_scope_tracking(client_id, &no_longer_visible);

        for (object_id, branch_name) in newly_visible_for_query {
            self.queue_initial_sync_to_client_with_storage(
                storage,
                client_id,
                object_id,
                branch_name,
                true,
            );
        }
    }

    /// Drop a client's query subscription state.
    ///
    /// Removes per-query scope and origin tracking.
    pub fn drop_client_query_subscription(&mut self, client_id: ClientId, query_id: QueryId) {
        if let Some(client) = self.clients.get_mut(&client_id) {
            let old_scope: HashSet<(ObjectId, BranchName)> = client
                .queries
                .values()
                .flat_map(|q| q.scope.iter().cloned())
                .collect();
            client.queries.remove(&query_id);
            let new_scope: HashSet<(ObjectId, BranchName)> = client
                .queries
                .values()
                .flat_map(|q| q.scope.iter().cloned())
                .collect();
            let no_longer_visible: HashSet<(ObjectId, BranchName)> =
                old_scope.difference(&new_scope).cloned().collect();
            self.prune_client_scope_tracking(client_id, &no_longer_visible);
        }

        if let Some(clients) = self.query_origin.get_mut(&query_id) {
            clients.remove(&client_id);
            if clients.is_empty() {
                self.query_origin.remove(&query_id);
            }
        }
    }

    fn prune_client_scope_tracking(
        &mut self,
        client_id: ClientId,
        removed_scope: &HashSet<(ObjectId, BranchName)>,
    ) {
        if removed_scope.is_empty() {
            return;
        }

        let mut removed_row_batches = Vec::new();
        let Some(client) = self.clients.get_mut(&client_id) else {
            return;
        };

        for &(object_id, branch_name) in removed_scope {
            if let Some(batch_ids) = client.sent_batch_ids.remove(&(object_id, branch_name)) {
                removed_row_batches.extend(
                    batch_ids
                        .into_iter()
                        .map(|batch_id| RowBatchKey::new(object_id, branch_name, batch_id)),
                );
            }
        }

        for key in removed_row_batches {
            if let Some(clients) = self.row_batch_interest.get_mut(&key) {
                clients.remove(&client_id);
                if clients.is_empty() {
                    self.row_batch_interest.remove(&key);
                }
            }
        }
    }

    /// Send a QuerySubscription to all connected servers.
    ///
    /// Called by QueryManager when a client creates a subscription that should
    /// be forwarded upstream for server-side evaluation.
    pub fn send_query_subscription_to_servers(
        &mut self,
        query_id: QueryId,
        query: Query,
        session: Option<Session>,
        propagation: QueryPropagation,
        policy_context_tables: Vec<String>,
    ) {
        let server_ids: Vec<ServerId> = self.servers.keys().copied().collect();
        for server_id in server_ids {
            self.send_query_subscription_to_server(
                server_id,
                query_id,
                query.clone(),
                session.clone(),
                propagation,
                policy_context_tables.clone(),
            );
        }
    }

    /// Send a QuerySubscription to one specific server.
    ///
    /// Used when replaying existing subscriptions after a late server connect.
    pub fn send_query_subscription_to_server(
        &mut self,
        server_id: ServerId,
        query_id: QueryId,
        query: Query,
        session: Option<Session>,
        propagation: QueryPropagation,
        policy_context_tables: Vec<String>,
    ) {
        if !self.servers.contains_key(&server_id) {
            return;
        }

        self.outbox.push(OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::QuerySubscription {
                query_id,
                query: Box::new(query),
                session,
                propagation,
                policy_context_tables,
            },
        });
    }

    /// Send a QueryUnsubscription to all connected servers.
    ///
    /// Called by QueryManager when a client unsubscribes from a synced query.
    pub fn send_query_unsubscription_to_servers(&mut self, query_id: QueryId) {
        for &server_id in self.servers.keys() {
            self.outbox.push(OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::QueryUnsubscription { query_id },
            });
        }
    }

    /// Take pending QuerySettled notifications for QueryManager to process.
    pub fn take_pending_query_settled(&mut self) -> Vec<PendingQuerySettled> {
        std::mem::take(&mut self.pending_query_settled)
    }

    /// Re-queue QuerySettled notifications that are still blocked on stream sequencing.
    pub fn requeue_pending_query_settled(&mut self, pending: Vec<PendingQuerySettled>) {
        self.pending_query_settled.extend(pending);
    }

    /// Take pending query rejections for QueryManager to process.
    pub fn take_pending_query_rejections(&mut self) -> Vec<PendingQueryRejection> {
        std::mem::take(&mut self.pending_query_rejections)
    }

    /// Return the union of latest upstream scope snapshots for this query.
    pub fn remote_query_scope(&self, query_id: QueryId) -> HashSet<(ObjectId, BranchName)> {
        self.remote_query_scopes
            .iter()
            .filter(|((_, remote_query_id), _)| *remote_query_id == query_id)
            .flat_map(|(_, scope)| scope.iter().copied())
            .collect()
    }

    /// Whether we have received at least one upstream scope snapshot for this query.
    pub fn has_remote_query_scope_snapshot(&self, query_id: QueryId) -> bool {
        self.remote_query_scopes
            .keys()
            .any(|(_, remote_query_id)| *remote_query_id == query_id)
    }

    /// Take query ids whose upstream scope changed since the last process pass.
    pub fn take_remote_query_scope_dirty(&mut self) -> HashSet<QueryId> {
        std::mem::take(&mut self.remote_query_scope_dirty)
    }

    /// Take pending replayable batch settlements for RuntimeCore to process.
    pub fn take_pending_batch_settlements(&mut self) -> Vec<BatchSettlement> {
        std::mem::take(&mut self.pending_batch_settlements)
    }

    pub fn pending_batch_settlements(&self) -> &[BatchSettlement] {
        &self.pending_batch_settlements
    }

    pub fn push_pending_batch_settlement(&mut self, settlement: BatchSettlement) {
        self.pending_batch_settlements.push(settlement);
    }

    /// Take received row batch-member persistence state since last call.
    /// Used by RuntimeCore to resolve row `_persisted` mutation receivers.
    pub fn take_received_row_batch_acks(&mut self) -> Vec<(RowBatchKey, DurabilityTier)> {
        std::mem::take(&mut self.received_row_batch_acks)
    }

    /// Take pending row visibility changes for QueryManager to materialize
    /// into indices and subscriptions.
    pub fn take_pending_row_visibility_changes(&mut self) -> Vec<RowVisibilityChange> {
        std::mem::take(&mut self.pending_row_visibility_changes)
    }

    /// Take pending catalogue/system entry updates for QueryManager/SchemaManager.
    pub fn take_pending_catalogue_updates(&mut self) -> Vec<CatalogueEntry> {
        std::mem::take(&mut self.pending_catalogue_updates)
    }

    /// Requeue row visibility changes that could not be processed yet,
    /// typically because the corresponding schema has not been activated yet.
    pub fn requeue_pending_row_visibility_changes(&mut self, updates: Vec<RowVisibilityChange>) {
        self.pending_row_visibility_changes.extend(updates);
    }

    /// Emit a QuerySettled notification to a client.
    ///
    /// Called by QueryManager when a server subscription settles for the first time.
    pub fn emit_query_settled(
        &mut self,
        client_id: ClientId,
        query_id: QueryId,
        tier: DurabilityTier,
        scope: &HashSet<(ObjectId, BranchName)>,
    ) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::QuerySettled {
                query_id,
                tier,
                scope: sorted_query_scope_snapshot(scope),
                through_seq: 0,
            },
        });
    }

    /// Emit a schema warning to a client.
    pub fn emit_schema_warning(&mut self, client_id: ClientId, warning: SchemaWarning) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::SchemaWarning(warning),
        });
    }

    /// Emit a query subscription rejection error to a client.
    pub fn emit_query_subscription_rejected(
        &mut self,
        client_id: ClientId,
        query_id: QueryId,
        code: impl Into<String>,
        reason: impl Into<String>,
    ) {
        self.outbox.push(OutboxEntry {
            destination: Destination::Client(client_id),
            payload: SyncPayload::Error(SyncError::QuerySubscriptionRejected {
                query_id,
                code: code.into(),
                reason: reason.into(),
            }),
        });
    }
}

fn sorted_query_scope_snapshot(
    scope: &HashSet<(ObjectId, BranchName)>,
) -> Vec<(ObjectId, BranchName)> {
    let mut entries: Vec<_> = scope.iter().copied().collect();
    entries.sort_by(
        |(left_object_id, left_branch), (right_object_id, right_branch)| {
            left_object_id
                .cmp(right_object_id)
                .then_with(|| left_branch.as_str().cmp(right_branch.as_str()))
        },
    );
    entries
}
