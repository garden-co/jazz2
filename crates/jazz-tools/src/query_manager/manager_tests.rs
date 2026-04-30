//! QueryManager integration tests.
//!
//! Tests for CRUD operations, subscriptions, syncing, and deletions.

use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;

use serde_json::json;
use smallvec::smallvec;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::{Layer, Registry};

use crate::metadata::{DeleteKind, MetadataKey, RowProvenance, row_provenance_metadata};
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::manager::{QueryError, QueryManager};
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::session::Session as PolicySession;
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, ComposedBranchName, PolicyExpr, RowDescriptor, Schema, TableName,
    TablePolicies, TableSchema, Value,
};
use crate::row_histories::{BatchId, HistoryScan, RowState, StoredRowBatch, VisibleRowEntry};
use crate::schema_manager::encoding::encode_schema;
use crate::storage::{
    HistoryRowBytes, IndexMutation, MemoryStorage, OpfsBTreeStorage, OwnedHistoryRowBytes,
    OwnedVisibleRowBytes, RawTableMutation, RawTableRows, Storage, StorageError, VisibleRowBytes,
};
use crate::sync_manager::{InboxEntry, ServerId, Source, SyncManager, SyncPayload};
use crate::test_support::{
    apply_test_row_batch, create_test_row, load_test_row_metadata, load_test_row_tip_ids,
    persist_test_schema, put_test_row_metadata, seeded_memory_storage,
};

#[derive(Debug, Clone)]
struct IncomingRowBatch {
    parents: smallvec::SmallVec<[BatchId; 2]>,
    content: Vec<u8>,
    timestamp: u64,
    author: String,
}

impl IncomingRowBatch {
    fn row_provenance(&self) -> RowProvenance {
        RowProvenance::for_insert(self.author.clone(), self.timestamp)
    }

    fn to_row(&self, object_id: ObjectId, branch: &str, state: RowState) -> StoredRowBatch {
        let metadata = row_provenance_metadata(&self.row_provenance(), None)
            .into_iter()
            .collect::<HashMap<_, _>>();
        StoredRowBatch::new(
            object_id,
            branch,
            self.parents.iter().copied().collect::<Vec<_>>(),
            self.content.clone(),
            self.row_provenance(),
            metadata,
            state,
            None,
        )
    }
}

fn test_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
        .into(),
    );
    schema
}

fn recursive_team_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        RowDescriptor::new(vec![ColumnDescriptor::new("team_id", ColumnType::Integer)]).into(),
    );
    schema.insert(
        TableName::new("team_edges"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("child_team", ColumnType::Integer),
            ColumnDescriptor::new("parent_team", ColumnType::Integer),
        ])
        .into(),
    );
    schema
}

fn recursive_hop_team_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]).into(),
    );
    schema.insert(
        TableName::new("team_edges"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("child_team", ColumnType::Uuid),
            ColumnDescriptor::new("parent_team", ColumnType::Uuid),
        ])
        .into(),
    );
    schema
}

/// Helper to create QueryManager with schema on default branch.
fn create_query_manager(
    sync_manager: SyncManager,
    schema: Schema,
) -> (QueryManager, MemoryStorage) {
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema, "dev", "main");
    let storage = seeded_memory_storage(&qm.schema_context().current_schema);
    (qm, storage)
}

/// Get the current branch name from a QueryManager.
fn get_branch(qm: &QueryManager) -> String {
    qm.schema_context().branch_name().as_str().to_string()
}

struct CountingCatalogueUpsertsStorage {
    inner: MemoryStorage,
    catalogue_upserts: Cell<usize>,
}

impl CountingCatalogueUpsertsStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            catalogue_upserts: Cell::new(0),
        }
    }

    fn catalogue_upserts(&self) -> usize {
        self.catalogue_upserts.get()
    }
}

impl Storage for CountingCatalogueUpsertsStorage {
    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn apply_raw_table_mutations(
        &mut self,
        mutations: &[RawTableMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner.apply_raw_table_mutations(mutations)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_row_bytes(table, rows)
    }

    fn upsert_visible_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[VisibleRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.upsert_visible_region_row_bytes(table, rows)
    }

    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[OwnedHistoryRowBytes],
        visible_rows: &[OwnedVisibleRowBytes],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner
            .apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowBatch],
        visible_entries: &[VisibleRowEntry],
        encoded_history_rows: &[OwnedHistoryRowBytes],
        encoded_visible_rows: &[OwnedVisibleRowBytes],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
    }

    fn upsert_catalogue_entry(
        &mut self,
        entry: &crate::catalogue::CatalogueEntry,
    ) -> Result<(), StorageError> {
        self.catalogue_upserts.set(self.catalogue_upserts.get() + 1);
        self.inner.upsert_catalogue_entry(entry)
    }

    fn load_catalogue_entry(
        &self,
        object_id: crate::object::ObjectId,
    ) -> Result<Option<crate::catalogue::CatalogueEntry>, StorageError> {
        self.inner.load_catalogue_entry(object_id)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedEvent {
    level: tracing::Level,
    message: Option<String>,
    fields: BTreeMap<String, String>,
}

#[derive(Clone, Default)]
struct EventCollector {
    events: std::sync::Arc<std::sync::Mutex<Vec<CapturedEvent>>>,
}

impl EventCollector {
    fn snapshot(&self) -> Vec<CapturedEvent> {
        self.events.lock().unwrap().clone()
    }
}

impl<S> Layer<S> for EventCollector
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = CapturedEventVisitor::default();
        event.record(&mut visitor);
        self.events.lock().unwrap().push(CapturedEvent {
            level: *event.metadata().level(),
            message: visitor.message,
            fields: visitor.fields,
        });
    }
}

#[derive(Default)]
struct CapturedEventVisitor {
    message: Option<String>,
    fields: BTreeMap<String, String>,
}

impl CapturedEventVisitor {
    fn record_value(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = Some(value.clone());
        }
        self.fields.insert(field.name().to_string(), value);
    }
}

impl Visit for CapturedEventVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.record_value(field, format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.record_value(field, value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.record_value(field, value.to_string());
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.record_value(field, value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.record_value(field, value.to_string());
    }
}

struct FailOnIndexColumnStorage {
    inner: MemoryStorage,
    failing_column: &'static str,
}

impl FailOnIndexColumnStorage {
    fn new(failing_column: &'static str) -> Self {
        Self {
            inner: MemoryStorage::new(),
            failing_column,
        }
    }
}

impl Storage for FailOnIndexColumnStorage {
    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn apply_raw_table_mutations(
        &mut self,
        mutations: &[RawTableMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner.apply_raw_table_mutations(mutations)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.append_history_region_row_bytes(table, rows)
    }

    fn upsert_visible_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[VisibleRowBytes<'_>],
    ) -> Result<(), StorageError> {
        self.inner.upsert_visible_region_row_bytes(table, rows)
    }

    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[OwnedHistoryRowBytes],
        visible_rows: &[OwnedVisibleRowBytes],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner
            .apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowBatch],
        visible_entries: &[VisibleRowEntry],
        encoded_history_rows: &[OwnedHistoryRowBytes],
        encoded_visible_rows: &[OwnedVisibleRowBytes],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        self.inner.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
    }

    fn apply_index_mutations(
        &mut self,
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        if index_mutations.iter().any(|mutation| {
            matches!(
                mutation,
                IndexMutation::Insert { column, .. } | IndexMutation::Remove { column, .. }
                    if *column == self.failing_column
            )
        }) {
            return Err(StorageError::IoError(format!(
                "simulated index failure for column {}",
                self.failing_column
            )));
        }
        self.inner.apply_index_mutations(index_mutations)
    }

    fn upsert_catalogue_entry(
        &mut self,
        entry: &crate::catalogue::CatalogueEntry,
    ) -> Result<(), StorageError> {
        self.inner.upsert_catalogue_entry(entry)
    }

    fn load_catalogue_entry(
        &self,
        object_id: crate::object::ObjectId,
    ) -> Result<Option<crate::catalogue::CatalogueEntry>, StorageError> {
        self.inner.load_catalogue_entry(object_id)
    }
}

fn get_branch_for_user_branch(qm: &QueryManager, user_branch: &str) -> String {
    ComposedBranchName::new(
        &qm.schema_context().env,
        qm.schema_context().current_hash,
        user_branch,
    )
    .to_branch_name()
    .as_str()
    .to_string()
}

fn connect_server(
    qm: &mut QueryManager,
    storage: &MemoryStorage,
    server_id: crate::sync_manager::ServerId,
) {
    qm.sync_manager_mut()
        .add_server_with_storage(server_id, false, storage);
}

fn connect_client(
    qm: &mut QueryManager,
    storage: &MemoryStorage,
    client_id: crate::sync_manager::ClientId,
) {
    qm.sync_manager_mut()
        .add_client_with_storage(storage, client_id);
}

fn connect_query_manager_upstream(
    qm: &mut QueryManager,
    storage: &MemoryStorage,
    server_id: crate::sync_manager::ServerId,
) {
    qm.add_server_with_storage(storage, server_id, false);
}

fn load_visible_row(storage: &MemoryStorage, row_id: ObjectId, branch: &str) -> StoredRowBatch {
    let row_locator = storage
        .load_row_locator(row_id)
        .unwrap()
        .expect("row locator should exist");
    storage
        .load_visible_region_row(row_locator.table.as_str(), branch, row_id)
        .unwrap()
        .expect("visible row should exist")
}

fn stored_row_commit(
    parents: smallvec::SmallVec<[BatchId; 2]>,
    content: Vec<u8>,
    timestamp: u64,
    author: impl Into<String>,
) -> IncomingRowBatch {
    IncomingRowBatch {
        parents,
        content,
        timestamp,
        author: author.into(),
    }
}

fn add_row_commit(
    storage: &mut MemoryStorage,
    object_id: ObjectId,
    branch: &str,
    parents: Vec<BatchId>,
    content: Vec<u8>,
    timestamp: u64,
    author: impl Into<String>,
) -> BatchId {
    let author = author.into();
    let provenance = if parents.is_empty() {
        RowProvenance::for_insert(author.clone(), timestamp)
    } else {
        RowProvenance {
            created_by: author.clone(),
            created_at: 1_000,
            updated_by: author.clone(),
            updated_at: timestamp,
        }
    };
    let row = StoredRowBatch::new(
        object_id,
        branch,
        parents,
        content,
        provenance,
        Default::default(),
        RowState::VisibleDirect,
        None,
    );
    let batch_id = row.batch_id();
    apply_test_row_batch(storage, object_id, branch, row).unwrap();
    batch_id
}

fn test_row_metadata(storage: &MemoryStorage, row_id: ObjectId) -> HashMap<String, String> {
    load_test_row_metadata(storage, row_id).expect("row metadata should be available")
}

fn test_row_tip_ids(
    storage: &MemoryStorage,
    row_id: ObjectId,
    branch: impl AsRef<str>,
) -> Vec<BatchId> {
    load_test_row_tip_ids(storage, row_id, branch.as_ref())
        .expect("row branch tips should be available")
}

fn receive_row_commit(
    qm: &mut QueryManager,
    _storage: &mut MemoryStorage,
    object_id: ObjectId,
    branch: &str,
    commit: IncomingRowBatch,
) -> BatchId {
    let row = commit.to_row(object_id, branch, RowState::VisibleDirect);
    let batch_id = row.batch_id();
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row,
        },
    });
    batch_id
}

use crate::object::ObjectId;
use crate::query_manager::query::Query;

fn json_documents_schema(schema: Option<serde_json::Value>) -> Schema {
    let mut out = Schema::new();
    out.insert(
        TableName::new("documents"),
        RowDescriptor::new(vec![ColumnDescriptor::new(
            "payload",
            ColumnType::Json { schema },
        )])
        .into(),
    );
    out
}

fn visual_description_schema() -> Schema {
    let mut out = Schema::new();
    out.insert(
        TableName::new("visual_description"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("config", ColumnType::Json { schema: None }).nullable(),
        ])
        .into(),
    );
    out
}

/// Helper to execute a query synchronously via subscribe/process/unsubscribe.
/// Returns Vec<(ObjectId, Vec<Value>)> matching old execute() return type.
fn execute_query<H: Storage>(
    qm: &mut QueryManager,
    storage: &mut H,
    query: Query,
) -> Result<Vec<(ObjectId, Vec<Value>)>, QueryError> {
    let sub_id = qm.subscribe(query)?;
    qm.process(storage);
    let results = qm.get_subscription_results(sub_id);
    qm.unsubscribe_with_sync(sub_id);
    Ok(results)
}

// ========================================================================
// Lazy loading and subscription tests
// ========================================================================

// NOTE: cold_start_loads_persisted_indices_and_rows, cold_start_only_loads_queried_rows,
// and cold_start_with_sorted_query tests were removed because they used
// process_storage_with_driver() and load_indices_from_driver() which no longer exist.
// Cold start behavior is now handled by the Storage-based storage layer.

// ========================================================================
// Row content update propagation tests
// ========================================================================

// ========================================================================
// End-to-End Sync Integration Tests (Followup 9)
// ========================================================================

// ========================================================================
// Soft Delete Tests
// ========================================================================

// ========================================================================
// Undelete Tests
// ========================================================================

// ========================================================================
// Hard Delete Tests
// ========================================================================

// ========================================================================
// Truncate Tests
// ========================================================================

// ========================================================================
// Include Deleted Query Tests
// ========================================================================

// ========================================================================
// Delete Subscription Delta Tests
// ========================================================================

// ========================================================================
// Join integration tests
// ========================================================================

fn join_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema
}

fn join_schema_with_implicit_base_id() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]).into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Uuid),
        ])
        .into(),
    );
    schema
}

fn join_schema_with_magic_permissions() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ]),
            TablePolicies::new().with_select(PolicyExpr::True),
        ),
    );

    let posts_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
        ColumnDescriptor::new("owner_id", ColumnType::Text),
    ]);
    let owner_policy = PolicyExpr::eq_session("owner_id", vec!["user_id".into()]);
    let posts_policies = TablePolicies::new()
        .with_select(owner_policy.clone())
        .with_update(Some(owner_policy.clone()), PolicyExpr::True)
        .with_delete(owner_policy);
    schema.insert(
        TableName::new("posts"),
        TableSchema::with_policies(posts_descriptor, posts_policies),
    );

    schema
}

// ========================================================================
// Array subquery (correlated subquery) tests
// ========================================================================

fn file_storage_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("file_parts"),
        RowDescriptor::new(vec![ColumnDescriptor::new("label", ColumnType::Text)]).into(),
    );
    schema.insert(
        TableName::new("files"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new(
                "parts",
                ColumnType::Array {
                    element: Box::new(ColumnType::Uuid),
                },
            )
            .references("file_parts"),
        ])
        .into(),
    );
    schema
}

fn files_with_parts_descriptor() -> RowDescriptor {
    // Sub-row has schema columns only; id is in Value::Row { id, .. }
    let part_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("label", ColumnType::Text)]);
    RowDescriptor::new(vec![
        ColumnDescriptor::new(
            "parts",
            ColumnType::Array {
                element: Box::new(ColumnType::Uuid),
            },
        ),
        ColumnDescriptor::new(
            "part_rows",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(part_descriptor),
                }),
            },
        ),
    ])
}

fn users_posts_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema
}

/// Output descriptor for users with posts array subquery.
fn users_with_posts_descriptor() -> RowDescriptor {
    // Posts row descriptor: [id, title, author_id]
    // The row's ObjectId is in Value::Row { id, .. }, not prepended as a column.
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
    ]);
    RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "posts",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(posts_row_desc),
                }),
            },
        ),
    ])
}

fn groups_users_array_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("groups"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new(
                "member_ids",
                ColumnType::Array {
                    element: Box::new(ColumnType::Integer),
                },
            ),
        ])
        .into(),
    );
    schema
}

// ========================================================================
// Policy (ReBAC) integration tests
// ========================================================================

fn policy_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("team_id", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]),
            TablePolicies::new().with_select(
                // owner_id = @session.user_id OR team_id IN @session.claims.teams
                PolicyExpr::or(vec![
                    PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                    PolicyExpr::in_session("team_id", vec!["claims".into(), "teams".into()]),
                ]),
            ),
        ),
    );
    schema
}

fn join_policy_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            TablePolicies::new().with_select(PolicyExpr::True),
        ),
    );
    schema.insert(
        TableName::new("posts"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_name", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]),
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_name", vec!["user_id".into()])),
        ),
    );
    schema
}

fn legacy_documents_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        RowDescriptor::new(vec![ColumnDescriptor::new("title", ColumnType::Text)]).into(),
    );
    schema
}

fn current_documents_permission_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Text),
            ]),
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                .with_update(
                    Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
                    PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                )
                .with_delete(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ),
    );
    schema
}

fn legacy_documents_to_current_permissions_lens() -> crate::schema_manager::lens::Lens {
    let legacy_schema = legacy_documents_schema();
    let current_schema = current_documents_permission_schema();
    let legacy_hash = crate::query_manager::types::SchemaHash::compute(&legacy_schema);
    let current_hash = crate::query_manager::types::SchemaHash::compute(&current_schema);
    let mut transform = crate::schema_manager::lens::LensTransform::new();
    transform.push(
        crate::schema_manager::lens::LensOp::AddColumn {
            table: "documents".to_string(),
            column: "owner_id".to_string(),
            column_type: ColumnType::Text,
            default: Value::Text("alice".into()),
        },
        false,
    );
    crate::schema_manager::lens::Lens::new(legacy_hash, current_hash, transform)
}

fn configure_legacy_client_with_current_permissions(qm: &mut QueryManager) {
    let legacy_schema = legacy_documents_schema();
    let legacy_hash = crate::query_manager::types::SchemaHash::compute(&legacy_schema);
    let mut known_schemas = std::collections::HashMap::new();
    known_schemas.insert(legacy_hash, legacy_schema);
    qm.set_known_schemas(std::sync::Arc::new(known_schemas));
    qm.register_lens(legacy_documents_to_current_permissions_lens());
    qm.set_authorization_schema(current_documents_permission_schema());
}

fn legacy_join_provenance_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]).into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("owner_name", ColumnType::Text),
            ColumnDescriptor::new("title", ColumnType::Text),
        ])
        .into(),
    );
    schema
}

fn current_join_provenance_permission_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            TablePolicies::new().with_select(PolicyExpr::True),
        ),
    );
    schema.insert(
        TableName::new("posts"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_name", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("viewer_name", ColumnType::Text),
            ]),
            TablePolicies::new().with_select(PolicyExpr::eq_session(
                "viewer_name",
                vec!["user_id".into()],
            )),
        ),
    );
    schema
}

fn legacy_join_provenance_to_current_permissions_lens() -> crate::schema_manager::lens::Lens {
    let legacy_schema = legacy_join_provenance_schema();
    let current_schema = current_join_provenance_permission_schema();
    let legacy_hash = crate::query_manager::types::SchemaHash::compute(&legacy_schema);
    let current_hash = crate::query_manager::types::SchemaHash::compute(&current_schema);
    let mut transform = crate::schema_manager::lens::LensTransform::new();
    transform.push(
        crate::schema_manager::lens::LensOp::AddColumn {
            table: "posts".to_string(),
            column: "viewer_name".to_string(),
            column_type: ColumnType::Text,
            default: Value::Text("bob".into()),
        },
        false,
    );
    crate::schema_manager::lens::Lens::new(legacy_hash, current_hash, transform)
}

fn configure_legacy_join_client_with_current_permissions(qm: &mut QueryManager) {
    let legacy_schema = legacy_join_provenance_schema();
    let legacy_hash = crate::query_manager::types::SchemaHash::compute(&legacy_schema);
    let mut known_schemas = std::collections::HashMap::new();
    known_schemas.insert(legacy_hash, legacy_schema);
    qm.set_known_schemas(std::sync::Arc::new(known_schemas));
    qm.register_lens(legacy_join_provenance_to_current_permissions_lens());
    qm.set_authorization_schema(current_join_provenance_permission_schema());
}

// ========================================================================
// Branch-aware query tests
// ========================================================================

// ============================================================================
// Contributing ObjectIds Tests
// ============================================================================

// ============================================================================
// Server-Side Query Subscription Tests
// ============================================================================

// ============================================================================
// Client subscribe_with_sync Tests
// ============================================================================

// ============================================================================
// Part 5: Multi-Tier Forwarding Tests
// ============================================================================

// Test that a mid-tier server forwards QuerySubscription to upstream servers.

// Test that a mid-tier server forwards QueryUnsubscription to upstream servers.

// Test that objects from upstream are relayed to downstream clients with matching scope.

// ============================================================================
// Part 6: End-to-End Integration Tests
// ============================================================================

/// Helper to exchange messages between client and server QueryManagers.
/// Runs multiple rounds until no more messages are exchanged.
fn pump_messages(
    client: &mut QueryManager,
    server: &mut QueryManager,
    client_io: &mut MemoryStorage,
    server_io: &mut MemoryStorage,
    client_id: crate::sync_manager::ClientId,
    server_id: crate::sync_manager::ServerId,
) {
    use crate::sync_manager::{Destination, InboxEntry, Source};

    for _ in 0..10 {
        // Client → Server
        let client_outbox = client.sync_manager_mut().take_outbox();
        let client_to_server: Vec<_> = client_outbox
            .into_iter()
            .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
            .collect();

        for entry in client_to_server {
            server.sync_manager_mut().push_inbox(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            });
        }
        server.process(server_io);

        // Server → Client
        let server_outbox = server.sync_manager_mut().take_outbox();
        let server_to_client: Vec<_> = server_outbox
            .into_iter()
            .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
            .collect();

        if server_to_client.is_empty() {
            break;
        }

        for entry in server_to_client {
            client.sync_manager_mut().push_inbox(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
        client.process(client_io);
    }
}

/// Helper to exchange messages in a 3-tier topology: client <-> edge <-> core.
/// Runs multiple rounds until no more routable messages are produced.
#[allow(clippy::too_many_arguments)]
fn pump_messages_three_tier(
    client: &mut QueryManager,
    edge: &mut QueryManager,
    core: &mut QueryManager,
    client_io: &mut MemoryStorage,
    edge_io: &mut MemoryStorage,
    core_io: &mut MemoryStorage,
    client_id_on_edge: crate::sync_manager::ClientId,
    edge_server_id_for_client: crate::sync_manager::ServerId,
    edge_id_on_core: crate::sync_manager::ClientId,
    core_server_id_for_edge: crate::sync_manager::ServerId,
) {
    use crate::sync_manager::{Destination, InboxEntry, Source};

    for _ in 0..20 {
        let mut moved = false;

        // Client -> Edge
        let client_outbox = client.sync_manager_mut().take_outbox();
        for entry in client_outbox {
            if matches!(entry.destination, Destination::Server(id) if id == edge_server_id_for_client)
            {
                moved = true;
                edge.sync_manager_mut().push_inbox(InboxEntry {
                    source: Source::Client(client_id_on_edge),
                    payload: entry.payload,
                });
            }
        }

        // Edge -> (Client or Core)
        let edge_outbox = edge.sync_manager_mut().take_outbox();
        for entry in edge_outbox {
            match entry.destination {
                Destination::Client(id) if id == client_id_on_edge => {
                    moved = true;
                    client.sync_manager_mut().push_inbox(InboxEntry {
                        source: Source::Server(edge_server_id_for_client),
                        payload: entry.payload,
                    });
                }
                Destination::Server(id) if id == core_server_id_for_edge => {
                    moved = true;
                    core.sync_manager_mut().push_inbox(InboxEntry {
                        source: Source::Client(edge_id_on_core),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }

        // Core -> Edge
        let core_outbox = core.sync_manager_mut().take_outbox();
        for entry in core_outbox {
            if matches!(entry.destination, Destination::Client(id) if id == edge_id_on_core) {
                moved = true;
                edge.sync_manager_mut().push_inbox(InboxEntry {
                    source: Source::Server(core_server_id_for_edge),
                    payload: entry.payload,
                });
            }
        }

        if !moved {
            break;
        }

        client.process(client_io);
        edge.process(edge_io);
        core.process(core_io);
    }
}

// E2E: Client subscribes to query, receives matching data from server.

// E2E: Client can cold-load a paginated remote query with a non-zero offset.

// E2E: Client receives new matching rows as server inserts them.

// E2E: Client does NOT receive rows that don't match the query filter.

// E2E: Permissions filter what gets synced - client only receives permitted rows.

// E2E: New rows that don't match permissions are NOT synced.

// E2E: In a 3-tier topology, upstream must sync policy-evaluation dependencies.
//
// Scenario:
// - documents SELECT policy: owner_id = @session.user_id OR INHERITS SELECT VIA folder_id
// - core has folder(owner=alice) and document(owner=bob, folder=alice_folder)
// - alice queries documents from downstream client via edge
//
// Expected:
// - core deems the document visible (via INHERITS)
// - edge must receive enough rows to re-evaluate the same policy and relay to client

// E2E: Untrusted downstream clients keep result-set-only scope (no policy context rows).

/// Push a query subscription inbox entry for a client.
fn push_query_subscription(
    qm: &mut QueryManager,
    client_id: crate::sync_manager::ClientId,
    query_id: u64,
    query: crate::query_manager::query::Query,
) {
    use crate::sync_manager::{InboxEntry, QueryId, Source, SyncPayload};
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(query_id),
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
}

mod array_subqueries;
mod bootstrap;
mod branches;
mod client_lifecycle;
mod contributing_ids;
mod crud_queries;
mod deletes;
mod e2e_sync;
mod joins;
mod json_storage;
mod misc;
mod policies;
mod recursive_queries;
mod server_subscriptions;
mod subscriptions;
mod updates;
