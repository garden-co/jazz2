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
use crate::object::BranchName;
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::manager::{QueryError, QueryManager};
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::session::Session as PolicySession;
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, ComposedBranchName, PolicyExpr, RowDescriptor, Schema, TableName,
    TablePolicies, TableSchema, Value,
};
use crate::row_histories::{
    BatchId, HistoryScan, QueryRowBatch, RowState, StoredRowBatch, VisibleRowEntry,
};
use crate::schema_manager::encoding::encode_schema;
use crate::storage::{
    HistoryRowBytes, IndexMutation, MemoryStorage, OpfsBTreeStorage, OwnedHistoryRowBytes,
    OwnedVisibleRowBytes, RawTableMutation, RawTableRows, Storage, StorageError, VisibleRowBytes,
};
use crate::sync_manager::{InboxEntry, ServerId, Source, SyncManager, SyncPayload};
use crate::test_row_history::{
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

struct CountingQueryRowLoadsStorage {
    inner: MemoryStorage,
    query_row_loads: Cell<usize>,
}

impl CountingQueryRowLoadsStorage {
    fn new(schema: &Schema) -> Self {
        Self {
            inner: seeded_memory_storage(schema),
            query_row_loads: Cell::new(0),
        }
    }

    fn query_row_loads(&self) -> usize {
        self.query_row_loads.get()
    }
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

impl Storage for CountingQueryRowLoadsStorage {
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
        self.inner.upsert_catalogue_entry(entry)
    }

    fn load_catalogue_entry(
        &self,
        object_id: crate::object::ObjectId,
    ) -> Result<Option<crate::catalogue::CatalogueEntry>, StorageError> {
        self.inner.load_catalogue_entry(object_id)
    }

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        self.query_row_loads.set(self.query_row_loads.get() + 1);
        self.inner.load_visible_query_row(table, branch, row_id)
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: crate::sync_manager::DurabilityTier,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        self.query_row_loads.set(self.query_row_loads.get() + 1);
        self.inner
            .load_visible_query_row_for_tier(table, branch, row_id, required_tier)
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

#[test]
fn direct_query_manager_bootstrap_persists_canonical_schema_bytes_for_flat_row_storage() {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("id", ColumnType::Uuid),
        ])),
    );
    let schema_hash = crate::query_manager::types::SchemaHash::compute(&schema);

    let mut qm = QueryManager::new(SyncManager::new());
    qm.set_current_schema(schema.clone(), "dev", "main");

    let mut storage = MemoryStorage::new();
    qm.ensure_known_schemas_catalogued(&mut storage)
        .expect("schema bootstrap should succeed");

    let schema_entry = storage
        .load_catalogue_entry(schema_hash.to_object_id())
        .expect("catalogue lookup should succeed")
        .expect("schema should be catalogued");
    assert_eq!(
        schema_entry.content,
        encode_schema(&schema),
        "direct QueryManager bootstrapping should persist canonical schema bytes"
    );

    let descriptor = qm.schema_context().current_schema[&TableName::new("users")]
        .columns
        .clone();
    let values = descriptor
        .columns
        .iter()
        .map(|column| match column.name.as_str() {
            "id" => Value::Uuid(ObjectId::new()),
            "name" => Value::Text("Alice".into()),
            other => panic!("unexpected column {other}"),
        })
        .collect::<Vec<_>>();
    let inserted = qm
        .insert(&mut storage, "users", &values)
        .expect("insert should succeed");

    let history_bytes = storage
        .scan_history_region_bytes("users", HistoryScan::Branch)
        .expect("history bytes should be readable");
    assert_eq!(history_bytes.len(), 1);
    assert_eq!(
        storage
            .scan_history_row_batches("users", inserted.row_id)
            .expect("flat history rows should decode with keyed storage context")
            .len(),
        history_bytes.len(),
        "direct QueryManager writes should persist keyed-decodable flat history rows after schema bootstrap"
    );
}

#[test]
fn direct_query_manager_catalogues_known_schemas_only_once_per_storage() {
    let mut qm = QueryManager::new(SyncManager::new());
    qm.set_current_schema(test_schema(), "dev", "main");
    let mut storage = CountingCatalogueUpsertsStorage::new();

    let first = vec![Value::Text("Alice".into()), Value::Integer(1)];
    qm.insert(&mut storage, "users", &first)
        .expect("first insert should succeed");
    let first_upserts = storage.catalogue_upserts();
    assert!(
        first_upserts >= 1,
        "first insert should catalogue the current schema"
    );

    let second = vec![Value::Text("Bob".into()), Value::Integer(2)];
    qm.insert(&mut storage, "users", &second)
        .expect("second insert should succeed");

    assert_eq!(
        storage.catalogue_upserts(),
        first_upserts,
        "ordinary writes should not recatalogue unchanged schemas on the same storage",
    );
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

#[test]
fn insert_json_preserves_original_text() {
    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(None);
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let raw = "{\n  \"name\": \"Ada\",\n  \"active\": true\n}";
    qm.insert(&mut storage, "documents", &[Value::Text(raw.to_string())])
        .expect("insert valid json");

    let query = qm.query("documents").build();
    let rows = execute_query(&mut qm, &mut storage, query).expect("query inserted row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, vec![Value::Text(raw.to_string())]);
}

#[test]
fn insert_rejects_invalid_json_text() {
    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(None);
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let err = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("{\"name\":true".to_string())],
        )
        .expect_err("invalid JSON must be rejected");

    assert!(
        matches!(&err, QueryError::EncodingError(msg) if msg.contains("invalid JSON for column `payload`")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn insert_rejects_json_schema_violation() {
    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(Some(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    })));
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let err = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("{\"name\":123}".to_string())],
        )
        .expect_err("schema-invalid JSON must be rejected");

    assert!(
        matches!(&err, QueryError::EncodingError(msg) if msg.contains("JSON schema validation failed for column `payload`")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn update_rejects_json_schema_violation() {
    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(Some(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    })));
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let inserted = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("{\"name\":\"ok\"}".to_string())],
        )
        .expect("insert valid row first");

    let err = qm
        .update(
            &mut storage,
            inserted.row_id,
            &[Value::Text("{\"name\":42}".to_string())],
        )
        .expect_err("invalid update payload must be rejected");
    assert!(
        matches!(&err, QueryError::EncodingError(msg) if msg.contains("JSON schema validation failed for column `payload`")),
        "unexpected error: {err:?}"
    );

    let query = qm.query("documents").build();
    let rows = execute_query(&mut qm, &mut storage, query).expect("query existing row");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1,
        vec![Value::Text("{\"name\":\"ok\"}".to_string())]
    );
}

#[test]
fn synced_insert_log_includes_failing_index_column() {
    let collector = EventCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(None);
    let descriptor = schema[&TableName::new("documents")].columns.clone();

    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema.clone(), "dev", "main");

    let mut storage = FailOnIndexColumnStorage::new("payload");
    persist_test_schema(&mut storage, &schema);

    let branch = get_branch(&qm);
    let row_id = ObjectId::new();
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "documents".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    let raw_json = json!({
        "content": "x".repeat(4_096),
        "kind": "payload"
    })
    .to_string();
    let row_data = encode_row(&descriptor, &[Value::Text(raw_json)]).unwrap();

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: stored_row_commit(smallvec![], row_data, 1_000, row_id.to_string()).to_row(
                row_id,
                &branch,
                RowState::VisibleDirect,
            ),
        },
    });
    qm.process(&mut storage);

    let event = collector
        .snapshot()
        .into_iter()
        .find(|event| {
            event.level == tracing::Level::ERROR
                && event.message.as_deref() == Some("failed to update indices for synced insert")
        })
        .expect("synced insert failure should be logged");

    assert_eq!(
        event.fields.get("index_column").map(String::as_str),
        Some("payload")
    );
    assert_eq!(
        event.fields.get("table").map(String::as_str),
        Some("documents")
    );
}

#[test]
fn synced_insert_many_large_json_configs_survive_opfs_splits() {
    let collector = EventCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let sync_manager = SyncManager::new();
    let schema = visual_description_schema();
    let descriptor = schema[&TableName::new("visual_description")]
        .columns
        .clone();

    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema.clone(), "dev", "main");

    let mut storage = OpfsBTreeStorage::memory(4 * 1024 * 1024).expect("open opfs storage");
    persist_test_schema(&mut storage, &schema);

    let branch = get_branch(&qm);
    let table = "visual_description";
    let max_index_value_segment_len =
        5 * 1024 - (4 + table.len() + 1 + "config".len() + 1 + branch.len() + 1 + 32);
    let max_inline_text_bytes = (max_index_value_segment_len / 2).saturating_sub(1);
    let json_overhead = "{\"config\":\"\"}".len();
    let shared_prefix = "a".repeat(max_inline_text_bytes - json_overhead - 8);

    for i in 0..128 {
        let row_id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(MetadataKey::Table.to_string(), table.to_string());
        put_test_row_metadata(&mut storage, row_id, metadata);

        let payload = if i % 8 == 7 {
            format!("{{\"config\":\"b{i:08x}\"}}")
        } else {
            format!("{{\"config\":\"{shared_prefix}{i:08x}\"}}")
        };
        let row_data = encode_row(&descriptor, &[Value::Text(payload)]).unwrap();

        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(ServerId::new()),
            payload: SyncPayload::RowBatchCreated {
                metadata: None,
                row: stored_row_commit(smallvec![], row_data, 1_000 + i as u64, row_id.to_string())
                    .to_row(row_id, &branch, RowState::VisibleDirect),
            },
        });
    }

    qm.process(&mut storage);

    let failures = collector
        .snapshot()
        .into_iter()
        .filter(|event| {
            event.level == tracing::Level::ERROR
                && event.message.as_deref() == Some("failed to update indices for synced insert")
        })
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "synced inserts should not hit opfs split failures: {failures:?}"
    );

    let query = qm.query(table).build();
    let rows = execute_query(&mut qm, &mut storage, query).expect("query synced rows");
    assert_eq!(rows.len(), 128, "all synced rows should remain queryable");
}

#[test]
fn insert_and_get() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, handle.row_id);
    assert_eq!(results[0].1[0], Value::Text("Alice".into()));
    assert_eq!(results[0].1[1], Value::Integer(100));
}

#[test]
fn insert_and_query() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let bob = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();
    let charlie = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    // Query all
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);
    assert!(results.iter().any(|(id, values)| {
        *id == alice.row_id && values == &vec![Value::Text("Alice".into()), Value::Integer(100)]
    }));
    assert!(results.iter().any(|(id, values)| {
        *id == bob.row_id && values == &vec![Value::Text("Bob".into()), Value::Integer(50)]
    }));
    assert!(results.iter().any(|(id, values)| {
        *id == charlie.row_id && values == &vec![Value::Text("Charlie".into()), Value::Integer(75)]
    }));

    // Query with filter
    let query = qm
        .query("users")
        .filter_ge("score", Value::Integer(75))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);
    assert!(results.iter().any(|(id, values)| {
        *id == alice.row_id && values == &vec![Value::Text("Alice".into()), Value::Integer(100)]
    }));
    assert!(results.iter().any(|(id, values)| {
        *id == charlie.row_id && values == &vec![Value::Text("Charlie".into()), Value::Integer(75)]
    }));
    assert!(
        results.iter().all(|(id, _)| *id != bob.row_id),
        "Bob should not match score >= 75 filter"
    );
}

#[test]
fn recursive_query_expands_transitive_team_edges() {
    let sync_manager = SyncManager::new();
    let schema = recursive_team_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Seed team and edge data:
    // 1 -> 2 -> 3 -> 1 (cycle)
    qm.insert(&mut storage, "teams", &[Value::Integer(1)])
        .unwrap();
    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Integer(1), Value::Integer(2)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Integer(2), Value::Integer(3)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Integer(3), Value::Integer(1)],
    )
    .unwrap();

    let query = qm
        .query("teams")
        .select(&["team_id"])
        .filter_eq("team_id", Value::Integer(1))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "team_id")
                .select(&["parent_team"])
                .max_depth(10)
        })
        .build();

    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    let mut ids: Vec<i32> = results
        .into_iter()
        .filter_map(|(_, values)| match values.first() {
            Some(Value::Integer(i)) => Some(*i),
            _ => None,
        })
        .collect();
    ids.sort_unstable();

    assert_eq!(ids, vec![1, 2, 3], "Should compute recursive closure");
}

#[test]
fn recursive_query_with_hop_expands_transitive_closure() {
    let sync_manager = SyncManager::new();
    let schema = recursive_hop_team_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let team1 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-1".into())])
        .unwrap();
    let team2 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-2".into())])
        .unwrap();
    let team3 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-3".into())])
        .unwrap();

    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Uuid(team1.row_id), Value::Uuid(team2.row_id)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Uuid(team2.row_id), Value::Uuid(team3.row_id)],
    )
    .unwrap();

    let query = qm
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();

    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    let mut names: Vec<String> = results
        .into_iter()
        .filter_map(|(_, values)| match values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect();
    names.sort();
    assert_eq!(names, vec!["team-1", "team-2", "team-3"]);
}

#[test]
fn recursive_query_with_join_project_step_is_rejected() {
    let sync_manager = SyncManager::new();
    let schema = recursive_hop_team_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query_result = qm
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .join("teams")
                .alias("__recursive_hop_0")
                .on("team_edges.parent_team", "__recursive_hop_0.id")
                .result_element_index(1)
                .max_depth(10)
        })
        .try_build();

    assert!(
        query_result.is_err(),
        "recursive join-projection shape should be rejected"
    );
}

#[test]
fn recursive_hop_query_subscriptions_receive_expansion_updates() {
    let sync_manager = SyncManager::new();
    let schema = recursive_hop_team_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let team1 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-1".into())])
        .unwrap();
    let team2 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-2".into())])
        .unwrap();
    let team3 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-3".into())])
        .unwrap();

    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Uuid(team1.row_id), Value::Uuid(team2.row_id)],
    )
    .unwrap();

    let query = qm
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let _initial_updates = qm.take_updates();

    qm.insert(
        &mut storage,
        "team_edges",
        &[Value::Uuid(team2.row_id), Value::Uuid(team3.row_id)],
    )
    .unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Recursive hop subscription should receive updates");
    let team_descriptor = qm
        .schema_context()
        .current_schema
        .get(&TableName::new("teams"))
        .unwrap();
    let added_names: Vec<String> = delta
        .added
        .iter()
        .filter_map(|row| {
            match decode_row(&team_descriptor.columns, &row.data)
                .ok()?
                .first()
            {
                Some(Value::Text(name)) => Some(name.clone()),
                _ => None,
            }
        })
        .collect();
    assert!(
        added_names.contains(&"team-3".to_string()),
        "team-3 should be added when edge team-2 -> team-3 is inserted"
    );
}

#[test]
fn query_with_sort_and_limit() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(50)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Charlie".into()), Value::Integer(75)],
    )
    .unwrap();

    let query = qm.query("users").order_by_desc("score").limit(2).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1[0], Value::Text("Alice".into())); // 100
    assert_eq!(results[1].1[0], Value::Text("Charlie".into())); // 75
}

#[test]
fn query_order_by_id_is_deterministic() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let h1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let h2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();
    let h3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    let query = qm.query("users").order_by("id").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);

    let actual: Vec<_> = results.iter().map(|(id, _)| *id).collect();
    let mut expected = vec![h1.row_id, h2.row_id, h3.row_id];
    expected.sort();
    assert_eq!(actual, expected);
}

#[test]
fn query_without_order_by_defaults_to_id_ascending() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let h1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let h2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();
    let h3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);

    let actual: Vec<_> = results.iter().map(|(id, _)| *id).collect();
    let mut expected = vec![h1.row_id, h2.row_id, h3.row_id];
    expected.sort();
    assert_eq!(actual, expected);
}

#[test]
fn update_row() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Updated".into()), Value::Integer(150)],
    )
    .unwrap();

    let updated_query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice Updated".into()))
        .build();
    let updated_rows = execute_query(&mut qm, &mut storage, updated_query).unwrap();
    assert_eq!(updated_rows.len(), 1);
    assert_eq!(updated_rows[0].0, handle.row_id);
    assert_eq!(updated_rows[0].1[1], Value::Integer(150));

    let old_query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let old_rows = execute_query(&mut qm, &mut storage, old_query).unwrap();
    assert_eq!(
        old_rows.len(),
        0,
        "Old indexed value should no longer match"
    );
}

#[test]
fn table_not_found_error() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let result = qm.insert(&mut storage, "nonexistent", &[Value::Text("test".into())]);
    match result {
        Err(QueryError::TableNotFound(table)) => {
            assert_eq!(table, TableName::new("nonexistent"));
        }
        other => panic!("Expected TableNotFound(nonexistent), got {other:?}"),
    }
}

#[test]
fn column_count_mismatch_error() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let result = qm.insert(&mut storage, "users", &[Value::Text("Alice".into())]);
    match result {
        Err(QueryError::ColumnCountMismatch { expected, actual }) => {
            assert_eq!(expected, 2, "users table has two columns in test_schema()");
            assert_eq!(actual, 1, "insert call provided one value");
        }
        other => panic!("Expected ColumnCountMismatch, got {other:?}"),
    }
}

#[test]
fn insert_returns_handle_with_batch_id() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Handle should reference a query-visible row with inserted content.
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    let inserted = results
        .iter()
        .find(|(id, _)| *id == handle.row_id)
        .expect("Inserted row should be query-visible");
    assert_eq!(inserted.1[0], Value::Text("Alice".into()));
    assert_eq!(inserted.1[1], Value::Integer(100));

    // Handle should have a valid logical batch identity
    assert!(handle.batch_id.0 != [0; 16]);
}

#[test]
fn insert_materializes_visible_and_history_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let descriptor = schema
        .get(&TableName::new("users"))
        .unwrap()
        .columns
        .clone();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let branch = get_branch(&qm);
    let visible = storage.scan_visible_region("users", &branch).unwrap();
    let history = storage
        .scan_history_region(
            "users",
            &branch,
            HistoryScan::Row {
                row_id: handle.row_id,
            },
        )
        .unwrap();

    assert_eq!(visible.len(), 1);
    assert_eq!(history, visible);

    let stored = &history[0];
    assert_eq!(stored.row_id, handle.row_id);
    assert_eq!(stored.branch, branch);
    assert_eq!(stored.state, RowState::VisibleDirect);
    assert!(!stored.is_deleted);
    assert_eq!(
        decode_row(&descriptor, &stored.data).unwrap(),
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn row_is_indexed_after_insert() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Row should be immediately query-visible via indexed column lookup.
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, handle.row_id);
}

#[test]
fn index_persistence_via_insert() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Test".into()), Value::Integer(42)],
        )
        .unwrap();

    // Verify row is query-visible via both indexed columns.
    let by_name = qm
        .query("users")
        .filter_eq("name", Value::Text("Test".into()))
        .build();
    let by_name_results = execute_query(&mut qm, &mut storage, by_name).unwrap();
    assert_eq!(by_name_results.len(), 1);
    assert_eq!(by_name_results[0].0, handle.row_id);

    let by_score = qm
        .query("users")
        .filter_eq("score", Value::Integer(42))
        .build();
    let by_score_results = execute_query(&mut qm, &mut storage, by_score).unwrap();
    assert_eq!(by_score_results.len(), 1);
    assert_eq!(by_score_results[0].0, handle.row_id);
}

// ========================================================================
// Lazy loading and subscription tests
// ========================================================================

#[test]
fn can_register_query_immediately() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Can register a query subscription immediately
    let query = qm.query("users").build();
    let sub_id = qm
        .subscribe(query)
        .expect("Registering a simple query should succeed");

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Registered subscription should receive updates");
    assert_eq!(delta.added.len(), 1);
}

#[test]
fn subscription_updates_after_insert_and_process() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Register subscription
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Process - should settle subscriptions
    qm.process(&mut storage);

    // Now we should have subscription updates
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.added.len(), 1);
    assert_eq!(
        updates[0].delta.added[0].id, handle.row_id,
        "Delta should identify the inserted row"
    );
}

#[test]
fn query_reads_visible_region_after_legacy_commit_history_is_removed() {
    let schema = test_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handle = writer_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let mut reader_qm = QueryManager::new(SyncManager::new());
    reader_qm.set_current_schema(schema, "dev", "main");

    let query = reader_qm.query("users").build();
    let rows = execute_query(&mut reader_qm, &mut storage, query)
        .expect("visible-row query should succeed without legacy object-backed storage");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, handle.row_id);
    assert_eq!(
        rows[0].1,
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn sorted_limited_subscription_reorders_when_new_top_row_arrives() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let query = qm.query("users").order_by_desc("score").limit(2).build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(50)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Charlie".into()), Value::Integer(75)],
    )
    .unwrap();
    qm.process(&mut storage);
    let _initial_updates = qm.take_updates();

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Diana".into()), Value::Integer(125)],
    )
    .unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("sorted/limited subscription should emit update for new top row");

    assert_eq!(delta.added.len(), 1);
    assert_eq!(delta.removed.len(), 1);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1[0], Value::Text("Diana".into())); // 125
    assert_eq!(results[1].1[0], Value::Text("Alice".into())); // 100
}

#[test]
fn offset_limited_subscription_shifts_window_when_deleting_row_before_window() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .order_by("score")
        .offset(1)
        .limit(2)
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    let handle_a = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("A".into()), Value::Integer(1)],
        )
        .unwrap();
    let handle_b = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("B".into()), Value::Integer(2)],
        )
        .unwrap();
    let handle_c = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("C".into()), Value::Integer(3)],
        )
        .unwrap();
    let handle_d = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("D".into()), Value::Integer(4)],
        )
        .unwrap();

    qm.process(&mut storage);
    let _initial_updates = qm.take_updates();

    let initial_results = qm.get_subscription_results(sub_id);
    assert_eq!(
        initial_results
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        vec![handle_b.row_id, handle_c.row_id]
    );

    qm.delete(&mut storage, handle_a.row_id).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("offset/limit subscription should emit update when leading row is deleted");

    assert_eq!(delta.removed.len(), 1);
    assert_eq!(delta.removed[0].id, handle_b.row_id);
    assert_eq!(delta.added.len(), 1);
    assert_eq!(delta.added[0].id, handle_d.row_id);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![handle_c.row_id, handle_d.row_id]
    );
}

#[test]
fn multiple_inserts_all_visible_in_query() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Multiple inserts
    let h1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let h2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();
    let h3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    // Query returns all rows with expected identities and payload values.
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);
    let row_1 = results
        .iter()
        .find(|(id, _)| *id == h1.row_id)
        .expect("Alice row should be present");
    let row_2 = results
        .iter()
        .find(|(id, _)| *id == h2.row_id)
        .expect("Bob row should be present");
    let row_3 = results
        .iter()
        .find(|(id, _)| *id == h3.row_id)
        .expect("Charlie row should be present");
    assert_eq!(row_1.1[0], Value::Text("Alice".into()));
    assert_eq!(row_2.1[0], Value::Text("Bob".into()));
    assert_eq!(row_3.1[0], Value::Text("Charlie".into()));
    assert_eq!(row_1.1[1], Value::Integer(100));
    assert_eq!(row_2.1[1], Value::Integer(50));
    assert_eq!(row_3.1[1], Value::Integer(75));

    // Sorted query works
    let query = qm.query("users").order_by_desc("score").limit(2).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1[0], Value::Text("Alice".into())); // 100
    assert_eq!(results[1].1[0], Value::Text("Charlie".into())); // 75
}

// NOTE: cold_start_loads_persisted_indices_and_rows, cold_start_only_loads_queried_rows,
// and cold_start_with_sorted_query tests were removed because they used
// process_storage_with_driver() and load_indices_from_driver() which no longer exist.
// Cold start behavior is now handled by the Storage-based storage layer.

#[test]
fn local_update_updates_all_column_indices() {
    // Verifies that local update() correctly:
    // 1. Removes old values from column indices
    // 2. Adds new values to column indices
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with name="Alice", score=100
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Query by name="Alice" → finds row
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);

    // Query by score=100 → finds row
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);

    // Update to name="Bob", score=200
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Bob".into()), Value::Integer(200)],
    )
    .unwrap();

    // Query by name="Alice" → empty (old value removed from index)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Old name value should be removed from index"
    );

    // Query by name="Bob" → finds row (new value in index)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Bob".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "New name value should be in index");

    // Query by score=100 → empty (old value removed from index)
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Old score value should be removed from index"
    );

    // Query by score=200 → finds row (new value in index)
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(200))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "New score value should be in index");
}

#[test]
fn synced_update_updates_column_indices() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    // This test verifies that direct synced commits (receive_commit)
    // update indices through the row-native update lane.

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Simulate receiving a new object from sync
    let row_id = crate::object::ObjectId::new();
    let author = row_id;

    // Receive object with table metadata
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    // Encode the initial row data (name="Alice", score=100)
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let initial_data = encode_row(
        &descriptor,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Receive the first commit (insert)
    let commit1 = stored_row_commit(smallvec![], initial_data.clone(), 1000, author.to_string());
    let commit1_id = receive_row_commit(&mut qm, &mut storage, row_id, &branch, commit1);

    // Process to handle the row-native update
    qm.process(&mut storage);

    // Query by name="Alice" → finds row
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Should find row by name=Alice after sync insert"
    );

    // Query by score=100 → finds row
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Should find row by score=100 after sync insert"
    );

    // Encode updated row data (name="Bob", score=200)
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Bob".into()), Value::Integer(200)],
    )
    .unwrap();

    // Receive the second commit (update)
    let commit2 = stored_row_commit(
        smallvec![commit1_id],
        updated_data.clone(),
        2000,
        author.to_string(),
    );
    receive_row_commit(&mut qm, &mut storage, row_id, &branch, commit2);

    // Process to handle the row-native update
    qm.process(&mut storage);

    // Query by name="Alice" → empty (old value removed from index)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Old name value should be removed from index after sync update"
    );

    // Query by name="Bob" → finds row (new value in index)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Bob".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "New name value should be in index after sync update"
    );

    // Query by score=100 → empty (old value removed from index)
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Old score value should be removed from index after sync update"
    );

    // Query by score=200 → finds row (new value in index)
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(200))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "New score value should be in index after sync update"
    );
}

#[test]
fn synced_insert_materializes_visible_and_history_rows() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let descriptor = schema
        .get(&TableName::new("users"))
        .unwrap()
        .columns
        .clone();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);
    let row_id = crate::object::ObjectId::new();

    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    let row_data = encode_row(
        &descriptor,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    let commit = stored_row_commit(smallvec![], row_data, 1000, row_id.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, &branch, commit);

    qm.process(&mut storage);

    let visible = storage.scan_visible_region("users", &branch).unwrap();
    let history = storage
        .scan_history_region("users", &branch, HistoryScan::Row { row_id })
        .unwrap();

    assert_eq!(visible.len(), 1);
    assert_eq!(history, visible);

    let stored = &history[0];
    assert_eq!(stored.row_id, row_id);
    assert_eq!(stored.branch, branch);
    assert_eq!(stored.state, RowState::VisibleDirect);
    assert!(!stored.is_deleted);
    assert_eq!(
        decode_row(&descriptor, &stored.data).unwrap(),
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn lens_transform_failure_drops_row_instead_of_fallback() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    // Build a live schema without registering a lens path to current.
    // Rows from that branch should be dropped at materialization time.
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let mut live_schema = Schema::new();
    live_schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
            ColumnDescriptor::new("email", ColumnType::Text),
        ])
        .into(),
    );
    let live_descriptor = live_schema
        .get(&TableName::new("users"))
        .expect("live schema table should exist")
        .columns
        .clone();
    qm.add_live_schema(live_schema);

    let current_branch = get_branch(&qm);
    let live_branch = qm
        .all_query_branches()
        .into_iter()
        .find(|b| b != &current_branch)
        .expect("live schema branch should exist");

    let row_id = ObjectId::new();
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    let live_data = encode_row(
        &live_descriptor,
        &[
            Value::Text("Alice".into()),
            Value::Integer(100),
            Value::Text("alice@example.com".into()),
        ],
    )
    .unwrap();
    let commit = stored_row_commit(smallvec![], live_data, 1000, row_id.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, &live_branch, commit);
    qm.process(&mut storage);

    assert!(
        qm.row_is_indexed_on_branch(&storage, "users", &live_branch, row_id),
        "row should be indexed on live branch before subscription settle"
    );

    let sub_id = qm.subscribe(qm.query("users").build()).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        0,
        "row from branch with failed lens transform should be dropped"
    );
}

#[test]
fn synced_insert_appears_in_subscription_delta() {
    use crate::query_manager::encoding::{decode_row, encode_row};
    use std::collections::HashMap;

    // Verify that a synced insert appears in subscription deltas

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Simulate receiving a new row from sync BEFORE subscribing
    // (similar to existing synced_update_updates_column_indices pattern)
    let row_id = crate::object::ObjectId::new();
    let author = row_id;

    // Receive object with table metadata
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    // Subscribe before the synced row arrives.
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Encode the row data (name="SyncedUser", score=42)
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let row_data = encode_row(
        &descriptor,
        &[Value::Text("SyncedUser".into()), Value::Integer(42)],
    )
    .unwrap();

    // Receive the commit (insert)
    let commit = stored_row_commit(smallvec![], row_data, 1000, author.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, &branch, commit);

    // Process to handle the row-native update
    qm.process(&mut storage);

    // Verify subscription delta contains the added row
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Delta should contain one added row"
    );

    // Decode the row to verify contents
    let row = &updates[0].delta.added[0];
    let values = decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("SyncedUser".into()));
    assert_eq!(values[1], Value::Integer(42));
}

#[test]
fn synced_update_is_visible_in_query() {
    use crate::query_manager::encoding::encode_row;
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    // Verify that synced updates (same row, new content) update indices correctly
    // and are visible in subsequent queries.
    // (Subscription delta behavior is covered by synced_update_emits_subscription_delta.)

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Insert a row locally first
    let insert_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let row_id = insert_handle.row_id;
    let first_commit_id = insert_handle.batch_id;

    // Process to settle the initial insert
    qm.process(&mut storage);
    let base_timestamp = load_visible_row(&storage, row_id, &branch).updated_at;

    // Verify initial data is queryable
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find initial row");
    assert_eq!(results[0].1[0], Value::Text("Alice".into()));
    assert_eq!(results[0].1[1], Value::Integer(100));

    // Now simulate a synced update to this row (e.g., from another peer)
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Alice Updated".into()), Value::Integer(200)],
    )
    .unwrap();

    let author = row_id; // Self-authored for simplicity
    let update_commit = stored_row_commit(
        smallvec![first_commit_id],
        updated_data,
        base_timestamp + 1,
        author.to_string(),
    );
    let row = update_commit.to_row(row_id, &branch, RowState::VisibleDirect);
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row,
        },
    });

    // Process to handle the synced update
    qm.process(&mut storage);

    // Old data should no longer be in index
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 0, "Old name should not be found");

    // New data should be queryable
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice Updated".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find updated row by new name");
    assert_eq!(results[0].1[0], Value::Text("Alice Updated".into()));
    assert_eq!(results[0].1[1], Value::Integer(200));

    // Score index should also be updated
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(200))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find updated row by new score");
}

#[test]
fn synced_row_visible_in_filtered_subscription() {
    use crate::query_manager::encoding::{decode_row, encode_row};
    use std::collections::HashMap;

    // Verify that synced rows are correctly filtered by subscription predicates.
    // Rows matching the filter appear in deltas; rows not matching are excluded.

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Subscribe to filtered query: users with score > 25
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(25))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Row descriptor for encoding
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);

    // --- Test 1: Synced row that matches filter (score=30 > 25) ---

    let row_id_1 = crate::object::ObjectId::new();
    let author_1 = row_id_1;

    let mut metadata_1 = HashMap::new();
    metadata_1.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id_1, metadata_1);

    let data_1 = encode_row(
        &descriptor,
        &[Value::Text("HighScorer".into()), Value::Integer(30)],
    )
    .unwrap();

    let commit_1 = stored_row_commit(smallvec![], data_1, 1000, author_1.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id_1, &branch, commit_1);

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(
        updates.len(),
        1,
        "Should have subscription update for matching row"
    );
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Delta should contain the matching row"
    );

    // Verify the row data
    let row = &updates[0].delta.added[0];
    let values = decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("HighScorer".into()));
    assert_eq!(values[1], Value::Integer(30));

    // --- Test 2: Synced row that does NOT match filter (score=20 < 25) ---

    let row_id_2 = crate::object::ObjectId::new();
    let author_2 = row_id_2;

    let mut metadata_2 = HashMap::new();
    metadata_2.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id_2, metadata_2);

    let data_2 = encode_row(
        &descriptor,
        &[Value::Text("LowScorer".into()), Value::Integer(20)],
    )
    .unwrap();

    let commit_2 = stored_row_commit(smallvec![], data_2, 2000, author_2.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id_2, &branch, commit_2);

    qm.process(&mut storage);

    let updates = qm.take_updates();
    // Should have NO updates because the row doesn't match the filter
    assert_eq!(
        updates.len(),
        0,
        "Should have no subscription update for non-matching row"
    );

    // But verify it's in the index (just not in the filtered subscription)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("LowScorer".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Non-matching row should still be in index"
    );
}

// ========================================================================
// Row content update propagation tests
// ========================================================================

#[test]
fn local_update_emits_subscription_delta() {
    // Verify that local qm.update() causes subscription to emit an update delta
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Subscribe to all users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get the initial add
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].delta.added.len(), 1);

    // Update the row
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Updated".into()), Value::Integer(200)],
    )
    .unwrap();

    // Process
    qm.process(&mut storage);

    // Should have an update delta
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Delta should contain one updated row"
    );

    // Verify old and new values
    let (old_row, new_row) = &updates[0].delta.updated[0];
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let old_values =
        crate::query_manager::encoding::decode_row(&descriptor, &old_row.data).unwrap();
    let new_values =
        crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();

    assert_eq!(old_values[0], Value::Text("Alice".into()));
    assert_eq!(old_values[1], Value::Integer(100));
    assert_eq!(new_values[0], Value::Text("Alice Updated".into()));
    assert_eq!(new_values[1], Value::Integer(200));
}

#[test]
fn update_reads_visible_region_after_legacy_commit_history_is_removed() {
    let schema = test_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handle = writer_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let mut reader_qm = QueryManager::new(SyncManager::new());
    reader_qm.set_current_schema(schema, "dev", "main");

    reader_qm
        .update(
            &mut storage,
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .expect(
            "update should succeed from visible-row state without legacy object-backed storage",
        );

    let query = reader_qm.query("users").build();
    let rows = execute_query(&mut reader_qm, &mut storage, query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, handle.row_id);
    assert_eq!(
        rows[0].1,
        vec![Value::Text("Alice Updated".into()), Value::Integer(200)]
    );
}

#[test]
fn synced_update_emits_subscription_delta() {
    use crate::query_manager::encoding::encode_row;
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    // Verify that synced updates (receive_commit) cause subscription to emit update delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row locally first
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let row_id = handle.row_id;
    let first_commit_id = handle.batch_id;

    // Subscribe to all users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get the initial add
    qm.process(&mut storage);
    let _updates = qm.take_updates(); // Clear initial add
    let branch = get_branch(&qm);
    let base_timestamp = load_visible_row(&storage, row_id, &branch).updated_at;

    // Now simulate a synced update
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Alice Synced".into()), Value::Integer(300)],
    )
    .unwrap();

    let author = row_id;
    let update_commit = stored_row_commit(
        smallvec![first_commit_id],
        updated_data,
        base_timestamp + 1,
        author.to_string(),
    );
    let row = update_commit.to_row(row_id, &branch, RowState::VisibleDirect);
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row,
        },
    });

    // Process
    qm.process(&mut storage);

    // Should have an update delta
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Delta should contain one updated row"
    );

    // Verify new values
    let (_old_row, new_row) = &updates[0].delta.updated[0];
    let new_values =
        crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();
    assert_eq!(new_values[0], Value::Text("Alice Synced".into()));
    assert_eq!(new_values[1], Value::Integer(300));
}

#[test]
fn multiple_updates_same_row_single_delta() {
    // Verify that marking a row updated multiple times before process()
    // results in a single update delta reflecting final state

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert and subscribe
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let query = qm.query("users").build();
    let _sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let _updates = qm.take_updates(); // Clear initial add

    // Update twice before process()
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice V2".into()), Value::Integer(200)],
    )
    .unwrap();
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice V3".into()), Value::Integer(300)],
    )
    .unwrap();

    // Single process()
    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Should have single update delta, not two"
    );

    // Verify it reflects final state
    let (_old_row, new_row) = &updates[0].delta.updated[0];
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let new_values =
        crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();
    assert_eq!(new_values[0], Value::Text("Alice V3".into()));
    assert_eq!(new_values[1], Value::Integer(300));
}

#[test]
fn update_fails_filter_emits_removal() {
    // Verify: row passes filter, then update fails filter -> removal delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with score=100
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Subscribe to score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Row should be added initially"
    );
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);

    // Update score to 30 (fails filter)
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(30)],
    )
    .unwrap();

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.removed.len(),
        1,
        "Row should be removed when it fails filter"
    );
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
}

#[test]
fn update_passes_filter_emits_addition() {
    // Verify: row fails filter initially, then update passes filter -> addition delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with score=30 (fails filter)
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(30)],
        )
        .unwrap();

    // Subscribe to score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    // Row doesn't match filter, so no addition/removals, but we should still get an update for the subscription
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
    assert!(updates[0].delta.removed.is_empty());

    // Update score to 100 (passes filter)
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Row should be added when it now passes filter"
    );
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);
    assert!(updates[0].delta.updated.is_empty());
    assert!(updates[0].delta.removed.is_empty());
}

#[test]
fn synced_update_that_fails_filter_emits_removal_delta() {
    use crate::query_manager::encoding::encode_row;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    let query = qm
        .query("users")
        .filter_ge("score", Value::Integer(75))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let base_batch_id = handle.batch_id;

    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.added.len(), 1);
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);

    let base_timestamp = load_visible_row(&storage, handle.row_id, &branch).updated_at;
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Alice".into()), Value::Integer(30)],
    )
    .unwrap();
    let synced_commit = stored_row_commit(
        smallvec![base_batch_id],
        updated_data,
        base_timestamp + 1,
        handle.row_id.to_string(),
    );
    receive_row_commit(&mut qm, &mut storage, handle.row_id, &branch, synced_commit);

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.removed.len(),
        1,
        "synced update should remove the row when it no longer matches the filter"
    );
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
}

#[test]
fn synced_boolean_eq_update_that_fails_filter_emits_removal_delta() {
    use crate::query_manager::encoding::encode_row;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("todos"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
            ColumnDescriptor::new("priority", ColumnType::Integer).nullable(),
            ColumnDescriptor::new("owner_id", ColumnType::Uuid).nullable(),
            ColumnDescriptor::new(
                "tags",
                ColumnType::Array {
                    element: Box::new(ColumnType::Text),
                },
            ),
            ColumnDescriptor::new("payload", ColumnType::Bytea).nullable(),
        ])
        .into(),
    );
    let (mut qm, mut storage) = create_query_manager(SyncManager::new(), schema);
    let branch = get_branch(&qm);

    let query = qm
        .query("todos")
        .filter_eq("done", Value::Boolean(false))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    let handle = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("watch-me".into()),
                Value::Boolean(false),
                Value::Null,
                Value::Null,
                Value::Array(vec![Value::Text("x".into())]),
                Value::Null,
            ],
        )
        .unwrap();
    let base_batch_id = handle.batch_id;

    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.added.len(), 1);
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);

    let base_timestamp = load_visible_row(&storage, handle.row_id, &branch).updated_at;
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("done", ColumnType::Boolean),
        ColumnDescriptor::new("priority", ColumnType::Integer).nullable(),
        ColumnDescriptor::new("owner_id", ColumnType::Uuid).nullable(),
        ColumnDescriptor::new(
            "tags",
            ColumnType::Array {
                element: Box::new(ColumnType::Text),
            },
        ),
        ColumnDescriptor::new("payload", ColumnType::Bytea).nullable(),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[
            Value::Text("watch-me".into()),
            Value::Boolean(true),
            Value::Null,
            Value::Null,
            Value::Array(vec![Value::Text("x".into())]),
            Value::Null,
        ],
    )
    .unwrap();
    let synced_commit = stored_row_commit(
        smallvec![base_batch_id],
        updated_data,
        base_timestamp + 1,
        handle.row_id.to_string(),
    );
    receive_row_commit(&mut qm, &mut storage, handle.row_id, &branch, synced_commit);

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.removed.len(),
        1,
        "synced boolean update should remove the row when it no longer matches the filter"
    );
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
}

#[test]
fn update_still_passes_filter_emits_update() {
    // Verify: row passes filter, update still passes filter -> update delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with score=100
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Subscribe to score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let _updates = qm.take_updates(); // Clear initial add

    // Update score to 200 (still passes filter)
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Updated".into()), Value::Integer(200)],
    )
    .unwrap();

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Row should be updated when it still passes filter"
    );
    assert_eq!(updates[0].delta.updated[0].0.id, handle.row_id);
    assert_eq!(updates[0].delta.updated[0].1.id, handle.row_id);
}

#[test]
fn update_to_untracked_row_is_silent() {
    // Verify: row doesn't match filter, update still doesn't match -> no delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with score=30 (fails filter)
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(30)],
        )
        .unwrap();

    // Subscribe to score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let _sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
    assert!(updates[0].delta.removed.is_empty());

    // Update score to 40 (still fails filter)
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(40)],
    )
    .unwrap();

    qm.process(&mut storage);

    let updates = qm.take_updates();
    // No updates should be emitted since the row doesn't match the filter before or after the update
    assert!(
        updates.is_empty(),
        "No updates for row that doesn't match filter before or after update"
    );
}

#[test]
fn insert_then_update_same_cycle() {
    // Verify: insert + update before process() -> single added delta with final values

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Subscribe first
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Insert
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Update before process()
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Updated".into()), Value::Integer(200)],
    )
    .unwrap();

    // Single process()
    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);

    // Should be added (not updated, since this is new to subscription)
    assert_eq!(updates[0].delta.added.len(), 1, "Row should be added");
    assert!(
        updates[0].delta.updated.is_empty(),
        "No spurious update delta"
    );

    // Verify final values
    let row = &updates[0].delta.added[0];
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let values = crate::query_manager::encoding::decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("Alice Updated".into()));
    assert_eq!(values[1], Value::Integer(200));
}

// ========================================================================
// End-to-End Sync Integration Tests (Followup 9)
// ========================================================================

#[test]
fn sync_inbox_insert_flows_to_subscription_delta() {
    // End-to-end test: sync message → SyncManager inbox → QueryManager subscription
    // This tests the full path through push_inbox() → process_inbox() → process()
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Add a "server" that we'll receive updates from
    let server_id = ServerId::new();
    connect_server(&mut qm, &storage, server_id);

    // Subscribe to users table
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to initialize - expect an initial empty update (subscription settled)
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have initial settlement update");
    assert!(
        updates[0].delta.added.is_empty(),
        "Initial delta should be empty"
    );

    // Construct the sync message payload
    let row_id = crate::object::ObjectId::new();
    let author = row_id;

    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let row_data = encode_row(
        &descriptor,
        &[Value::Text("SyncedUser".into()), Value::Integer(42)],
    )
    .unwrap();

    let commit = stored_row_commit(smallvec![], row_data, 1000, author.to_string());
    let row = commit.to_row(row_id, &branch, RowState::VisibleDirect);

    // Object metadata marking it as a "users" table row
    let mut obj_metadata = std::collections::HashMap::new();
    obj_metadata.insert(MetadataKey::Table.to_string(), "users".to_string());

    // Push the sync message through SyncManager's inbox
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(crate::sync_manager::RowMetadata {
                id: row_id,
                metadata: obj_metadata,
            }),
            row,
        },
    });

    // Process the inbox (SyncManager level)
    qm.sync_manager_mut().process_inbox(&mut storage);

    // Process (QueryManager level) - this should pick up the object update
    qm.process(&mut storage);

    // Verify subscription received the delta
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Delta should contain one added row"
    );

    // Verify the row contents
    let row = &updates[0].delta.added[0];
    let values = decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("SyncedUser".into()));
    assert_eq!(values[1], Value::Integer(42));
}

#[test]
fn sync_inbox_update_flows_to_subscription_delta() {
    // End-to-end test: sync update message → subscription emits update delta
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Add a "server"
    let server_id = ServerId::new();
    connect_server(&mut qm, &storage, server_id);
    // Insert a row locally first
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let row_id = handle.row_id;
    let first_commit_id = handle.batch_id;

    // Subscribe to users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get initial state
    qm.process(&mut storage);
    let _ = qm.take_updates(); // Clear initial delta
    let base_timestamp = load_visible_row(&storage, row_id, &branch).updated_at;

    // Now simulate receiving an update from sync (as if another peer modified the row)
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Alice Updated".into()), Value::Integer(999)],
    )
    .unwrap();

    let update_commit = stored_row_commit(
        smallvec![first_commit_id],
        updated_data,
        base_timestamp + 1,
        row_id.to_string(),
    );
    let row = update_commit.to_row(row_id, &branch, RowState::VisibleDirect);

    // Push the update through SyncManager inbox
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: None, // No metadata needed for existing object
            row,
        },
    });

    // Process both layers
    qm.sync_manager_mut().process_inbox(&mut storage);
    qm.process(&mut storage);

    // Verify subscription received update delta
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Delta should contain one updated row"
    );

    // Verify the new values
    let (_old_row, new_row) = &updates[0].delta.updated[0];
    let values = decode_row(&descriptor, &new_row.data).unwrap();
    assert_eq!(values[0], Value::Text("Alice Updated".into()));
    assert_eq!(values[1], Value::Integer(999));
}

#[test]
fn two_peer_sync_insert_reaches_subscription() {
    // Full two-peer test: Peer A inserts → (simulated sync) → Peer B subscription delta
    // This demonstrates the conceptual flow even though we construct the payload manually
    use crate::query_manager::encoding::decode_row;
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    // Create two peers
    let sync_manager_a = SyncManager::new();
    let sync_manager_b = SyncManager::new();
    let schema = test_schema();
    let (mut peer_a, mut storage_a) = create_query_manager(sync_manager_a, schema.clone());
    let (mut peer_b, mut storage_b) = create_query_manager(sync_manager_b, schema);

    // Peer B sets up query subscription
    let query = peer_b.query("users").build();
    let sub_id = peer_b.subscribe(query).unwrap();

    // Peer B adds a "server" (representing Peer A)
    let peer_a_as_server = ServerId::new();
    connect_server(&mut peer_b, &storage_b, peer_a_as_server);

    // Process both to initialize
    peer_a.process(&mut storage_a);
    peer_b.process(&mut storage_b);
    let _ = peer_b.take_updates();

    // Peer A inserts a row
    let handle = peer_a
        .insert(
            &mut storage_a,
            "users",
            &[Value::Text("FromPeerA".into()), Value::Integer(123)],
        )
        .unwrap();
    let row_id = handle.row_id;

    // Read the actual row metadata and visible row from storage.
    // This simulates "what would be sent over the wire"
    let branch_name = get_branch(&peer_a);
    let metadata = test_row_metadata(&storage_a, row_id);
    let row = load_visible_row(&storage_a, row_id, &branch_name);

    // Send to Peer B via SyncManager inbox
    peer_b.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(peer_a_as_server),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(crate::sync_manager::RowMetadata {
                id: row_id,
                metadata,
            }),
            row,
        },
    });

    // Peer B processes the sync message
    peer_b.sync_manager_mut().process_inbox(&mut storage_b);
    peer_b.process(&mut storage_b);

    // Verify Peer B's subscription received the row
    let updates = peer_b.take_updates();
    assert_eq!(
        updates.len(),
        1,
        "Peer B should have one subscription update"
    );
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Delta should contain one added row"
    );

    // Verify the row came from Peer A
    let row = &updates[0].delta.added[0];
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let values = decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("FromPeerA".into()));
    assert_eq!(values[1], Value::Integer(123));
}

// ========================================================================
// Soft Delete Tests
// ========================================================================

#[test]
fn soft_delete_removes_from_id_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Verify row is in _id index
    assert!(qm.row_is_indexed(&storage, "users", handle.row_id));

    // Delete the row
    let delete_handle = qm.delete(&mut storage, handle.row_id).unwrap();
    assert_eq!(delete_handle.row_id, handle.row_id);

    // Verify row is no longer in _id index
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));
}

#[test]
fn soft_delete_adds_to_id_deleted_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Verify row is NOT in _id_deleted index
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Verify row IS in _id_deleted index
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn soft_deleted_row_not_in_query_results() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert rows
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(50)],
    )
    .unwrap();

    // Verify both rows are visible
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);

    // Delete Alice
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Verify only Bob is visible
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Bob".into()));
}

#[test]
fn delete_already_deleted_row_fails() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Try to delete again - should fail
    let result = qm.delete(&mut storage, handle.row_id);
    match result {
        Err(QueryError::RowAlreadyDeleted(row_id)) => assert_eq!(row_id, handle.row_id),
        other => panic!("Expected RowAlreadyDeleted for deleted row, got {other:?}"),
    }
}

#[test]
fn soft_delete_with_concurrent_tips_merges_preserved_content() {
    // Test that soft deleting an object with two concurrent tips results
    // in a soft delete commit with merged content from both field updates.
    use crate::object::BranchName;
    use crate::query_manager::encoding::encode_row;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Original".into()), Value::Integer(0)],
        )
        .unwrap();
    qm.process(&mut storage);

    // Get the initial commit as the common parent
    let branch = get_branch(&qm);
    let branch_name = BranchName::new(&branch);
    let initial_tips: Vec<_> = test_row_tip_ids(&storage, handle.row_id, &branch)
        .iter()
        .copied()
        .collect();
    assert_eq!(initial_tips.len(), 1);
    let parent = initial_tips[0];

    // Create two concurrent updates with different timestamps and content.
    // Both have the same parent, creating diverging tips.
    let descriptor = qm
        .schema()
        .get(&TableName::new("users"))
        .unwrap()
        .columns
        .clone();
    let base_timestamp = load_visible_row(&storage, handle.row_id, &branch).updated_at;

    // Commit A: lower timestamp, update the first user column only.
    let content_a = encode_row(
        &descriptor,
        &[Value::Text("TipA".into()), Value::Integer(0)],
    )
    .unwrap();
    let commit_a = stored_row_commit(
        smallvec![parent],
        content_a,
        base_timestamp + 1,
        handle.row_id.to_string(),
    );

    // Commit B: higher timestamp, update the second user column only.
    let content_b = encode_row(
        &descriptor,
        &[Value::Text("Original".into()), Value::Integer(200)],
    )
    .unwrap();
    let commit_b = stored_row_commit(
        smallvec![parent],
        content_b.clone(),
        base_timestamp + 2,
        handle.row_id.to_string(),
    );

    // Add both commits to create concurrent tips
    // We need to receive these as synced commits
    let commit_a_id = receive_row_commit(&mut qm, &mut storage, handle.row_id, &branch, commit_a);
    let commit_b_id = receive_row_commit(&mut qm, &mut storage, handle.row_id, &branch, commit_b);

    qm.process(&mut storage);

    // Verify we now have concurrent tips
    let tips: Vec<_> = test_row_tip_ids(&storage, handle.row_id, &branch)
        .iter()
        .copied()
        .collect();
    assert_eq!(tips.len(), 2, "Should have 2 concurrent tips");
    assert!(tips.contains(&commit_a_id));
    assert!(tips.contains(&commit_b_id));

    // Now soft delete - should preserve merged content from both concurrent tips.
    let delete_handle = qm.delete(&mut storage, handle.row_id).unwrap();

    // Get the delete commit and verify its content
    let delete_row = load_visible_row(&storage, handle.row_id, branch_name.as_str());

    assert_eq!(
        delete_row.batch_id(),
        delete_handle.batch_id,
        "visible row should be the delete version"
    );
    assert_eq!(
        decode_row(&descriptor, &delete_row.data).unwrap(),
        vec![Value::Text("TipA".into()), Value::Integer(200)],
        "Soft delete should preserve merged content from the conflicted frontier"
    );
    assert_eq!(delete_row.delete_kind, Some(DeleteKind::Soft));

    // Additionally verify that querying with include_deleted shows the correct content
    let query = qm.query("users").include_deleted().build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("TipA".into()));
    assert_eq!(results[0].1[1], Value::Integer(200));
}

// ========================================================================
// Undelete Tests
// ========================================================================

#[test]
fn undelete_adds_to_id_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Verify row is not in _id index
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));

    // Undelete with new values
    qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Restored".into()), Value::Integer(150)],
    )
    .unwrap();

    // Verify row is back in _id index
    assert!(qm.row_is_indexed(&storage, "users", handle.row_id));
}

#[test]
fn undelete_removes_from_id_deleted_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));

    // Undelete
    qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Verify row is NOT in _id_deleted index
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn undelete_row_appears_in_query_results() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Verify not visible
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 0);

    // Undelete with new values
    qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Restored".into()), Value::Integer(200)],
    )
    .unwrap();

    // Verify visible again with new values
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Alice Restored".into()));
    assert_eq!(results[0].1[1], Value::Integer(200));
}

#[test]
fn undelete_nondeleted_row_fails() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Try to undelete a non-deleted row - should fail
    let result = qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    );
    match result {
        Err(QueryError::RowNotDeleted(row_id)) => assert_eq!(row_id, handle.row_id),
        other => panic!("Expected RowNotDeleted for non-deleted row, got {other:?}"),
    }
}

// ========================================================================
// Hard Delete Tests
// ========================================================================

#[test]
fn hard_delete_removes_from_id_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Hard delete the row
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Verify row is not in _id index
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));
}

#[test]
fn hard_delete_removes_from_id_deleted_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Soft delete first (puts it in _id_deleted)
    qm.delete(&mut storage, handle.row_id).unwrap();
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));

    // Then hard delete (removes from _id_deleted)
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Verify row is NOT in _id_deleted index
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn hard_deleted_row_not_in_any_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Hard delete
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Verify row is not in _id index
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));
    // Verify row is not in _id_deleted index
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn soft_then_hard_delete_removes_from_id_deleted() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Soft delete - row should be in _id_deleted
    qm.delete(&mut storage, handle.row_id).unwrap();
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));

    // Hard delete - row should be removed from _id_deleted
    qm.hard_delete(&mut storage, handle.row_id).unwrap();
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn undelete_hard_deleted_row_fails() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Hard delete
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Try to undelete - should fail
    let result = qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    );
    match result {
        Err(QueryError::RowHardDeleted(row_id)) => assert_eq!(row_id, handle.row_id),
        other => panic!("Expected RowHardDeleted for hard-deleted row, got {other:?}"),
    }
}

#[test]
fn undelete_hard_deleted_row_fails_after_legacy_commit_history_is_removed() {
    let schema = test_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handle = writer_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let _hard_delete = writer_qm.hard_delete(&mut storage, handle.row_id).unwrap();

    let mut reader_qm = QueryManager::new(SyncManager::new());
    reader_qm.set_current_schema(schema, "dev", "main");

    let result = reader_qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    );
    match result {
        Err(QueryError::RowHardDeleted(row_id)) => assert_eq!(row_id, handle.row_id),
        other => panic!("Expected RowHardDeleted for visible-only hard delete, got {other:?}"),
    }
}

#[test]
fn undelete_syncs_row_batch_created_to_server() {
    use crate::sync_manager::{Destination, ServerId, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let server_id = ServerId::new();
    connect_server(&mut qm, &storage, server_id);
    let _ = qm.sync_manager_mut().take_outbox();

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let _ = qm.sync_manager_mut().take_outbox();

    qm.delete(&mut storage, handle.row_id).unwrap();
    let _ = qm.sync_manager_mut().take_outbox();

    let undeleted = qm
        .undelete(
            &mut storage,
            handle.row_id,
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let outbox = qm.sync_manager_mut().take_outbox();
    let forwarded = outbox
        .iter()
        .find(|entry| matches!(entry.destination, Destination::Server(id) if id == server_id))
        .expect("undelete should forward the restored row upstream");

    match &forwarded.payload {
        SyncPayload::RowBatchCreated { row, .. } => {
            assert_eq!(row.row_id, handle.row_id);
            assert_eq!(row.batch_id(), undeleted.batch_id);
            assert!(!row.is_deleted);
            assert!(!row.is_soft_deleted());
            assert!(!row.is_hard_deleted());
        }
        other => panic!("undelete should sync as RowBatchCreated, got {other:?}"),
    }
}

// ========================================================================
// Truncate Tests
// ========================================================================

#[test]
fn truncate_soft_deleted_row() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Soft delete
    qm.delete(&mut storage, handle.row_id).unwrap();
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));

    // Truncate (upgrade to hard delete)
    qm.truncate(&mut storage, handle.row_id).unwrap();

    // Verify row is completely gone
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn hard_delete_syncs_row_batch_created_to_server() {
    use crate::sync_manager::{Destination, ServerId, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let server_id = ServerId::new();
    connect_server(&mut qm, &storage, server_id);
    let _ = qm.sync_manager_mut().take_outbox();

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let _ = qm.sync_manager_mut().take_outbox();

    let hard_delete = qm.hard_delete(&mut storage, handle.row_id).unwrap();

    let outbox = qm.sync_manager_mut().take_outbox();
    let forwarded = outbox
        .iter()
        .find(|entry| matches!(entry.destination, Destination::Server(id) if id == server_id))
        .expect("hard delete should forward the tombstone upstream");

    match &forwarded.payload {
        SyncPayload::RowBatchCreated { row, .. } => {
            assert_eq!(row.row_id, handle.row_id);
            assert_eq!(row.batch_id(), hard_delete.batch_id);
            assert!(row.is_deleted);
            assert!(row.is_hard_deleted());
            assert_eq!(row.data, Vec::<u8>::new());
        }
        other => panic!("hard delete should sync as RowBatchCreated, got {other:?}"),
    }
}

#[test]
fn truncate_nondeleted_row_fails() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Try to truncate a non-deleted row - should fail
    let result = qm.truncate(&mut storage, handle.row_id);
    match result {
        Err(QueryError::RowNotDeleted(row_id)) => assert_eq!(row_id, handle.row_id),
        other => panic!("Expected RowNotDeleted for non-deleted row, got {other:?}"),
    }
}

// ========================================================================
// Include Deleted Query Tests
// ========================================================================

#[test]
fn include_deleted_query_returns_soft_deleted_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert rows
    let handle1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(50)],
    )
    .unwrap();

    // Delete Alice
    qm.delete(&mut storage, handle1.row_id).unwrap();

    // Normal query - only Bob (Alice is in _id_deleted, not _id)
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Bob".into()));

    // Include deleted query - scans both _id and _id_deleted indices
    // Soft-deleted rows have preserved content, so both Alice and Bob are returned
    let query = qm.query("users").include_deleted().build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);

    // Verify Alice's data is preserved
    let alice_result = results
        .iter()
        .find(|r| r.1[0] == Value::Text("Alice".into()))
        .expect("include_deleted query should include the soft-deleted Alice row");
    assert_eq!(alice_result.1[1], Value::Integer(100));

    // Verify that Alice is in the _id_deleted index
    assert!(qm.row_is_deleted(&storage, "users", handle1.row_id));
}

#[test]
fn include_deleted_query_does_not_return_hard_deleted_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert rows
    let handle1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let bob_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();

    // Hard delete Alice
    qm.hard_delete(&mut storage, handle1.row_id).unwrap();

    // Include deleted query - only Bob (Alice is hard deleted)
    let query = qm.query("users").include_deleted().build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, bob_handle.row_id);
    assert_eq!(results[0].1[0], Value::Text("Bob".into()));
}

// ========================================================================
// Delete Subscription Delta Tests
// ========================================================================

#[test]
fn soft_delete_emits_removal_delta() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Subscribe to all users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get initial delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.added.len(), 1); // Alice added
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);

    // Delete Alice
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Process and check for removal delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.removed.len(), 1);
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
}

#[test]
fn hard_delete_emits_removal_delta() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Subscribe to all users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get initial delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.added.len(), 1); // Alice added
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);

    // Hard delete Alice
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Process and check for removal delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.removed.len(), 1);
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
}

#[test]
fn delete_reads_visible_region_after_legacy_commit_history_is_removed() {
    let schema = test_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handle = writer_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let mut reader_qm = QueryManager::new(SyncManager::new());
    reader_qm.set_current_schema(schema, "dev", "main");

    reader_qm.delete(&mut storage, handle.row_id).expect(
        "delete should succeed from visible-row state without legacy object-backed storage",
    );

    let query = reader_qm.query("users").build();
    let rows = execute_query(&mut reader_qm, &mut storage, query).unwrap();
    assert!(
        rows.is_empty(),
        "soft-deleted row should disappear from normal current-state queries"
    );
    assert!(reader_qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn delete_row_not_in_subscription_no_delta() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert rows
    let alice_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(50)],
    )
    .unwrap();

    // Subscribe to users with score >= 75 (only Alice)
    let query = qm
        .query("users")
        .filter_ge("score", Value::Integer(75))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get initial delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.added.len(), 1); // Only Alice
    assert_eq!(updates[0].delta.added[0].id, alice_handle.row_id);

    // Delete Alice (who IS in subscription) - should emit removal delta
    qm.delete(&mut storage, alice_handle.row_id).unwrap();

    // Process and verify we got removal delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.removed.len(), 1);
    assert_eq!(updates[0].delta.removed[0].id, alice_handle.row_id);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
}

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

#[test]
fn join_compiles_but_not_executed_yet() {
    // This test validates that join queries compile and don't panic,
    // even though full join execution is not yet implemented.
    // Once execute() supports joins, this test can be extended.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    // Build a join query
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .build();

    // The query should compile successfully
    assert!(query.is_join());
    assert_eq!(query.joins.len(), 1);
}

#[test]
fn join_query_with_projection_compiles() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .select(&["name", "title"])
        .build();

    assert!(query.is_join());
    assert_eq!(
        query.select_columns,
        Some(vec!["name".to_string(), "title".to_string()])
    );
}

#[test]
fn join_query_with_alias_compiles() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .alias("u")
        .join("posts")
        .alias("p")
        .on("u.id", "p.author_id")
        .build();

    assert!(query.is_join());
    assert_eq!(query.alias, Some("u".to_string()));
    assert_eq!(query.joins[0].alias, Some("p".to_string()));
}

#[test]
fn self_join_query_compiles() {
    // Self-join: employees with their managers
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("employees"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("manager_id", ColumnType::Integer).nullable(),
        ])
        .into(),
    );

    let sync_manager = SyncManager::new();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("employees")
        .alias("e")
        .join("employees")
        .alias("m")
        .on("e.manager_id", "m.id")
        .build();

    assert!(query.is_join());
    assert_eq!(query.alias, Some("e".to_string()));
    assert_eq!(query.joins[0].table.as_str(), "employees");
    assert_eq!(query.joins[0].alias, Some("m".to_string()));
}

#[test]
fn multi_join_query_compiles() {
    // Three-way join: orders -> customers, orders -> products
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("orders"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("customer_id", ColumnType::Integer),
            ColumnDescriptor::new("product_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("customers"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("products"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );

    let sync_manager = SyncManager::new();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("orders")
        .join("customers")
        .on("customer_id", "id")
        .join("products")
        .on("product_id", "id")
        .build();

    assert!(query.is_join());
    assert_eq!(query.joins.len(), 2);
    assert_eq!(query.joins[0].table.as_str(), "customers");
    assert_eq!(query.joins[1].table.as_str(), "products");
}

#[test]
fn join_without_on_clause_fails_query_build() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let result = qm.query("users").join("posts").try_build();
    assert!(
        result.is_err(),
        "Join queries without ON should fail at build time"
    );
}

#[test]
fn join_subscription_fails_for_invalid_join_column() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .join("posts")
        .on("missing_column", "author_id")
        .build();
    let result = qm.subscribe(query);

    match result {
        Err(QueryError::QueryCompilationError(msg)) => {
            assert_eq!(
                msg,
                "invalid relation plan: unsupported relation_ir shape for schema-context query compilation"
            );
        }
        other => panic!(
            "Join queries with invalid join columns should fail with QueryCompilationError, got {other:?}"
        ),
    }
}

#[test]
fn join_subscription_fails_for_circular_join_chain() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .join("users")
        .on("author_id", "id")
        .build();
    let result = qm.subscribe(query);

    match result {
        Err(QueryError::QueryCompilationError(msg)) => {
            assert_eq!(
                msg,
                "invalid relation plan: unsupported relation_ir shape for schema-context query compilation"
            );
        }
        other => panic!(
            "Circular/self join chains should fail with QueryCompilationError, got {other:?}"
        ),
    }
}

#[test]
fn join_subscription_marks_dirty_for_joined_table() {
    // Inserts into a JOINED table should trigger observable updates for join subscriptions.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Seed base table row so a later post insert can produce a join match.
    let user_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();

    // Subscribe to a join query: users JOIN posts ON users.id = posts.author_id.
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process once to settle initial state and clear bootstrap updates.
    qm.process(&mut storage);
    let _ = qm.take_updates();

    // Insert into the JOINED table (posts), not the base table (users).
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Test Post".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Join subscription should emit an update after joined-table insert");
    assert_eq!(
        delta.added.len(),
        1,
        "Joined insert should add one joined row"
    );

    let row = &delta.added[0];
    assert_eq!(
        row.id, user_handle.row_id,
        "Join output should still be keyed by base-table row id"
    );
    assert!(
        row.data
            .windows("Alice".len())
            .any(|window| window == "Alice".as_bytes()),
        "Joined row payload should contain base-table value"
    );
    assert!(
        row.data
            .windows("Test Post".len())
            .any(|window| window == "Test Post".as_bytes()),
        "Joined row payload should contain joined-table value"
    );
}

#[test]
fn join_produces_combined_tuples() {
    // Test that a join produces tuples with elements from both tables.
    // This verifies basic join functionality and tuple structure.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a user
    let user_id = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();

    // Insert a post by that user
    let post_id = qm
        .insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Hello World".into()),
                Value::Integer(1), // author_id matches user id
            ],
        )
        .unwrap();

    // Subscribe to a join query
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get join results
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates for subscription");

    // Should have one joined row
    assert_eq!(delta.added.len(), 1, "Should have one joined result");

    // Validate row identity and ensure payload carries values from both sides of the join.
    let row = &delta.added[0];
    assert_eq!(
        row.id, user_id.row_id,
        "Join output should be keyed by base table row id"
    );
    assert_ne!(
        row.id, post_id.row_id,
        "Join output should not be keyed by joined table row id"
    );
    assert!(
        row.data
            .windows("Alice".len())
            .any(|w| w == "Alice".as_bytes()),
        "Joined row payload should contain base-table text value"
    );
    assert!(
        row.data
            .windows("Hello World".len())
            .any(|w| w == "Hello World".as_bytes()),
        "Joined row payload should contain joined-table text value"
    );
}

#[test]
fn join_filter_on_joined_table_column() {
    // Test filtering on a column from the JOINED table (not the base table).
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert users
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    let bob_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(2), Value::Text("Bob".into())],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello World".into()), // Should NOT match "Rust"
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Learning Rust".into()), // SHOULD match filter
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Join with filter on posts.title
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        // This filter should match only the joined post title.
        .filter_eq("title", Value::Text("Learning Rust".into()))
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates");

    // Should only have one result (the "Learning Rust" post)
    assert_eq!(
        delta.added.len(),
        1,
        "Filter on joined table column should work"
    );
    let row = &delta.added[0];
    assert_eq!(
        row.id, bob_handle.row_id,
        "Only Bob's joined row should match Learning Rust"
    );
    assert!(
        row.data
            .windows("Bob".len())
            .any(|window| window == "Bob".as_bytes()),
        "Joined row should include Bob's name"
    );
    assert!(
        row.data
            .windows("Learning Rust".len())
            .any(|window| window == "Learning Rust".as_bytes()),
        "Joined row should include the matching post title"
    );
}

#[test]
fn join_filter_on_scoped_alias_columns() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    let bob_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(2), Value::Text("Bob".into())],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello World".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Learning Rust".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .alias("u")
        .join("posts")
        .alias("p")
        .on("u.id", "p.author_id")
        .filter_eq("u.name", Value::Text("Bob".into()))
        .filter_eq("p.title", Value::Text("Learning Rust".into()))
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates");

    assert_eq!(
        delta.added.len(),
        1,
        "Scoped alias filters should resolve against the correct joined columns"
    );
    let row = &delta.added[0];
    assert_eq!(row.id, bob_handle.row_id);
    assert!(
        row.data
            .windows("Bob".len())
            .any(|window| window == "Bob".as_bytes()),
        "Joined row should include the filtered base alias value"
    );
    assert!(
        row.data
            .windows("Learning Rust".len())
            .any(|window| window == "Learning Rust".as_bytes()),
        "Joined row should include the filtered joined alias value"
    );
}

#[test]
fn join_subscription_can_project_joined_element_output() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    let post = qm
        .insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Hello World".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .result_element_index(1)
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Expected projected join update");

    assert_eq!(delta.added.len(), 1);
    let row = &delta.added[0];
    assert_eq!(
        row.id, post.row_id,
        "Projected join output should be keyed by joined row id"
    );
    assert!(
        row.data
            .windows("Hello World".len())
            .any(|window| window == "Hello World".as_bytes()),
        "Projected join output should contain joined-table payload"
    );
}

#[test]
fn join_subscription_can_execute_precise_relation_ir_projection() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, ProjectColumn, ProjectExpr, RelExpr,
    };

    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let user = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello World".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    let mut query = qm
        .query("users")
        .join("posts")
        .on("users.id", "posts.author_id")
        .build();
    query.relation_ir = RelExpr::Project {
        input: Box::new(RelExpr::Join {
            left: Box::new(RelExpr::TableScan {
                table: TableName::new("users"),
            }),
            right: Box::new(RelExpr::TableScan {
                table: TableName::new("posts"),
            }),
            on: vec![JoinCondition {
                left: ColumnRef::scoped("users", "id"),
                right: ColumnRef::scoped("posts", "author_id"),
            }],
            join_kind: JoinKind::Inner,
        }),
        columns: vec![
            ProjectColumn {
                alias: "author_name".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("users", "name")),
            },
            ProjectColumn {
                alias: "post_title".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("posts", "title")),
            },
        ],
    };
    query.select_columns = None;

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("Expected precise projection update");

    assert_eq!(update.delta.added.len(), 1);
    assert_eq!(update.descriptor.columns.len(), 2);
    assert_eq!(update.descriptor.columns[0].name, "author_name");
    assert_eq!(update.descriptor.columns[1].name, "post_title");

    let row = &update.delta.added[0];
    assert_eq!(row.id, user.row_id);
    let values =
        decode_row(&update.descriptor, &row.data).expect("should decode precise projection");
    assert_eq!(
        values,
        vec![
            Value::Text("Alice".into()),
            Value::Text("Hello World".into())
        ]
    );
}

#[test]
fn join_subscription_precise_relation_ir_full_joined_element_preserves_implicit_id_row_shape() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, ProjectColumn, ProjectExpr, RelExpr,
    };

    let sync_manager = SyncManager::new();
    let schema = join_schema_with_implicit_base_id();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(&mut storage, "users", &[Value::Text("Alice".into())])
        .unwrap();
    let post = qm
        .insert(
            &mut storage,
            "posts",
            &[Value::Text("Hello World".into()), Value::Uuid(alice.row_id)],
        )
        .unwrap();

    let mut query = qm
        .query("users")
        .join("posts")
        .on("users.id", "posts.author_id")
        .build();
    query.relation_ir = RelExpr::Project {
        input: Box::new(RelExpr::Join {
            left: Box::new(RelExpr::TableScan {
                table: TableName::new("users"),
            }),
            right: Box::new(RelExpr::TableScan {
                table: TableName::new("posts"),
            }),
            on: vec![JoinCondition {
                left: ColumnRef::scoped("users", "id"),
                right: ColumnRef::scoped("__hop_0", "author_id"),
            }],
            join_kind: JoinKind::Inner,
        }),
        columns: vec![
            ProjectColumn {
                alias: "id".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "id")),
            },
            ProjectColumn {
                alias: "title".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "title")),
            },
            ProjectColumn {
                alias: "author_id".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "author_id")),
            },
        ],
    };
    query.select_columns = None;

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("Expected precise implicit-id projection update");

    assert_eq!(update.delta.added.len(), 1);
    assert_eq!(
        update.descriptor.columns.len(),
        2,
        "descriptor should only contain declared data columns",
    );
    assert_eq!(update.descriptor.columns[0].name, "title");
    assert_eq!(update.descriptor.columns[1].name, "author_id");

    let row = &update.delta.added[0];
    assert_eq!(row.id, post.row_id);
    let values = decode_row(&update.descriptor, &row.data)
        .expect("should decode full joined element projection");
    assert_eq!(
        values,
        vec![Value::Text("Hello World".into()), Value::Uuid(alice.row_id)]
    );
}

#[test]
fn join_subscription_can_filter_and_project_magic_columns() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, ProjectColumn,
        ProjectExpr, RelExpr, ValueRef,
    };

    let sync_manager = SyncManager::new();
    let schema = join_schema_with_magic_permissions();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("Alice Post".into()),
            Value::Integer(1),
            Value::Text("alice".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("Bob Post".into()),
            Value::Integer(2),
            Value::Text("bob".into()),
        ],
    )
    .unwrap();

    let mut query = qm
        .query("users")
        .join("posts")
        .on("users.id", "posts.author_id")
        .build();
    query.relation_ir = RelExpr::Project {
        input: Box::new(RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("posts"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("users", "id"),
                    right: ColumnRef::scoped("posts", "author_id"),
                }],
                join_kind: JoinKind::Inner,
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::scoped("posts", "$canDelete"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::Literal(Value::Boolean(true)),
            },
        }),
        columns: vec![
            ProjectColumn {
                alias: "user_name".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("users", "name")),
            },
            ProjectColumn {
                alias: "post_title".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("posts", "title")),
            },
            ProjectColumn {
                alias: "can_edit".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("posts", "$canEdit")),
            },
            ProjectColumn {
                alias: "can_delete".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("posts", "$canDelete")),
            },
        ],
    };
    query.select_columns = None;

    let sub_id = qm
        .subscribe_with_session(query, Some(PolicySession::new("alice")), None)
        .unwrap();
    qm.process(&mut storage);
    let update = qm
        .take_updates()
        .into_iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("joined magic projection update");

    assert_eq!(update.delta.added.len(), 1);
    assert_eq!(update.descriptor.columns.len(), 4);
    let values = decode_row(&update.descriptor, &update.delta.added[0].data).unwrap();
    assert_eq!(
        values,
        vec![
            Value::Text("Alice".into()),
            Value::Text("Alice Post".into()),
            Value::Boolean(true),
            Value::Boolean(true),
        ]
    );
}

#[test]
fn join_subscription_supports_implicit_base_id_keys() {
    let sync_manager = SyncManager::new();
    let schema = join_schema_with_implicit_base_id();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(&mut storage, "users", &[Value::Text("Alice".into())])
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("Hello".into()),
            Value::Uuid(alice.row_id), // FK to implicit users.id
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .join("posts")
        .on("users.id", "posts.author_id")
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have join subscription update");

    assert_eq!(delta.added.len(), 1, "Expected implicit-id join match");
    let row = &delta.added[0];
    assert_eq!(
        row.id, alice.row_id,
        "Join result should remain keyed by base row identity"
    );
    assert!(
        row.data
            .windows("Alice".len())
            .any(|window| window == "Alice".as_bytes()),
        "Joined row should include base-table value"
    );
    assert!(
        row.data
            .windows("Hello".len())
            .any(|window| window == "Hello".as_bytes()),
        "Joined row should include joined-table value"
    );
}

#[test]
fn deleting_parent_row_does_not_cascade_to_joined_rows() {
    // Deleting a parent row should not implicitly delete rows in other tables.
    // Child-table data should remain.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let user = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello World".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    let users_before_query = qm.query("users").build();
    let users_before = execute_query(&mut qm, &mut storage, users_before_query).unwrap();
    assert_eq!(
        users_before.len(),
        1,
        "Precondition: parent row should exist"
    );
    let posts_before_query = qm.query("posts").build();
    let posts_before = execute_query(&mut qm, &mut storage, posts_before_query).unwrap();
    assert_eq!(
        posts_before.len(),
        1,
        "Precondition: child row should exist"
    );

    qm.delete(&mut storage, user.row_id).unwrap();
    qm.process(&mut storage);

    let users_after_query = qm.query("users").build();
    let users_after = execute_query(&mut qm, &mut storage, users_after_query).unwrap();
    assert_eq!(
        users_after.len(),
        0,
        "Parent row should be deleted from its table"
    );

    let posts_query = qm.query("posts").build();
    let posts = execute_query(&mut qm, &mut storage, posts_query).unwrap();
    assert_eq!(
        posts.len(),
        1,
        "Child table row should remain after parent delete"
    );
    assert_eq!(posts[0].1[1], Value::Text("Hello World".into()));
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

#[test]
fn uuid_array_fk_forward_materialization_preserves_order_and_duplicates() {
    let sync_manager = SyncManager::new();
    let schema = file_storage_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let part_a = qm
        .insert(&mut storage, "file_parts", &[Value::Text("A".into())])
        .unwrap();
    let part_b = qm
        .insert(&mut storage, "file_parts", &[Value::Text("B".into())])
        .unwrap();

    qm.insert(
        &mut storage,
        "files",
        &[Value::Array(vec![
            Value::Uuid(part_b.row_id),
            Value::Uuid(part_a.row_id),
            Value::Uuid(part_b.row_id),
        ])],
    )
    .unwrap();

    let query = qm
        .query("files")
        .with_array("part_rows", |sub| {
            sub.from("file_parts").correlate("id", "files.parts")
        })
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let update = qm
        .take_updates()
        .into_iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("files subscription should produce one update");
    let row_values =
        decode_row(&files_with_parts_descriptor(), &update.delta.added[0].data).unwrap();
    let part_rows = row_values[1]
        .as_array()
        .expect("part_rows should be an array");
    let labels: Vec<String> = part_rows
        .iter()
        .map(|row| {
            let values = row.as_row().expect("part row");
            assert!(row.row_id().is_some(), "row should have an id");
            match &values[0] {
                Value::Text(label) => label.clone(),
                other => panic!("expected text label, got {other:?}"),
            }
        })
        .collect();
    assert_eq!(labels, vec!["B", "A", "B"]);
}

#[test]
fn uuid_array_fk_reverse_membership_and_index_updates_on_edit() {
    let sync_manager = SyncManager::new();
    let schema = file_storage_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    let part_a = qm
        .insert(&mut storage, "file_parts", &[Value::Text("A".into())])
        .unwrap();
    let part_b = qm
        .insert(&mut storage, "file_parts", &[Value::Text("B".into())])
        .unwrap();
    let file = qm
        .insert(
            &mut storage,
            "files",
            &[Value::Array(vec![
                Value::Uuid(part_a.row_id),
                Value::Uuid(part_b.row_id),
                Value::Uuid(part_b.row_id),
            ])],
        )
        .unwrap();

    let query = qm
        .query("file_parts")
        .with_array("files", |sub| {
            sub.from("files").correlate("parts", "file_parts.id")
        })
        .build();
    let before = execute_query(&mut qm, &mut storage, query.clone()).unwrap();
    let before_counts: std::collections::HashMap<String, usize> = before
        .iter()
        .map(|(_, values)| {
            let label = match &values[0] {
                Value::Text(label) => label.clone(),
                other => panic!("expected label text, got {other:?}"),
            };
            let count = values[1]
                .as_array()
                .expect("files include should be array")
                .len();
            (label, count)
        })
        .collect();
    assert_eq!(before_counts.get("A"), Some(&1));
    assert_eq!(before_counts.get("B"), Some(&1));

    let ids_for_a_before =
        storage.index_lookup("files", "parts", &branch, &Value::Uuid(part_a.row_id));
    let ids_for_b_before =
        storage.index_lookup("files", "parts", &branch, &Value::Uuid(part_b.row_id));
    assert!(ids_for_a_before.contains(&file.row_id));
    assert!(ids_for_b_before.contains(&file.row_id));

    qm.update(
        &mut storage,
        file.row_id,
        &[Value::Array(vec![Value::Uuid(part_b.row_id)])],
    )
    .unwrap();

    let after = execute_query(&mut qm, &mut storage, query).unwrap();
    let after_counts: std::collections::HashMap<String, usize> = after
        .iter()
        .map(|(_, values)| {
            let label = match &values[0] {
                Value::Text(label) => label.clone(),
                other => panic!("expected label text, got {other:?}"),
            };
            let count = values[1]
                .as_array()
                .expect("files include should be array")
                .len();
            (label, count)
        })
        .collect();
    assert_eq!(after_counts.get("A"), Some(&0));
    assert_eq!(after_counts.get("B"), Some(&1));

    let ids_for_a_after =
        storage.index_lookup("files", "parts", &branch, &Value::Uuid(part_a.row_id));
    let ids_for_b_after =
        storage.index_lookup("files", "parts", &branch, &Value::Uuid(part_b.row_id));
    assert!(
        !ids_for_a_after.contains(&file.row_id),
        "removed array members should be removed from membership index"
    );
    assert!(ids_for_b_after.contains(&file.row_id));
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

#[test]
fn array_subquery_single_user_with_posts() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert one user: Alice with id=1
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert two posts for Alice
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post 1".into()),
            Value::Integer(1), // author_id = 1 (Alice)
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Alice Post 2".into()),
            Value::Integer(1), // author_id = 1 (Alice)
        ],
    )
    .unwrap();

    // Query users with their posts as array
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update");

    // Should have exactly 1 user row
    assert_eq!(delta.added.len(), 1, "Expected 1 user row");

    // Decode the output row
    let output_descriptor = users_with_posts_descriptor();
    let row_data = &delta.added[0].data;
    let values = decode_row(&output_descriptor, row_data).expect("Should decode output row");

    // Verify user fields
    assert_eq!(values[0], Value::Integer(1), "User id should be 1");
    assert_eq!(
        values[1],
        Value::Text("Alice".into()),
        "User name should be Alice"
    );

    // Verify posts array
    let posts = values[2].as_array().expect("Third column should be array");
    assert_eq!(posts.len(), 2, "Alice should have 2 posts");

    // Each post is a Row of [id, title, author_id] with id in the Row struct
    for post in posts {
        let post_values = post.as_row().expect("Each post should be a Row");
        assert_eq!(
            post_values.len(),
            3,
            "Post should have 3 fields (schema cols)"
        );
        assert!(post.row_id().is_some(), "Post Row should have an id");
        // Verify author_id matches Alice (index 2)
        assert_eq!(
            post_values[2],
            Value::Integer(1),
            "Post author_id should be 1 (Alice)"
        );
    }
}

#[test]
fn array_subquery_update_descriptor_includes_array_column() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello world".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("should have subscription update");

    assert_eq!(
        update.descriptor.columns.len(),
        3,
        "Update descriptor should have 3 columns (base + array), got {}: {:?}",
        update.descriptor.columns.len(),
        update
            .descriptor
            .columns
            .iter()
            .map(|c| &c.name)
            .collect::<Vec<_>>()
    );

    let row_data = &update.delta.added[0].data;
    let values =
        decode_row(&update.descriptor, row_data).expect("should decode with update descriptor");

    assert_eq!(values[0], Value::Integer(1), "user id");
    assert_eq!(
        values[1],
        Value::Text("Alice".into()),
        "user name should be 'Alice', not corrupted"
    );

    // The included posts array should contain the post we inserted
    let posts = values[2]
        .as_array()
        .expect("third column should be the posts array");
    assert_eq!(posts.len(), 1, "Alice should have 1 post");
    let post_row = posts[0].as_row().expect("post element should be a Row");
    assert!(posts[0].row_id().is_some(), "post Row should have an id");
    assert_eq!(post_row[0], Value::Integer(100), "post id");
    assert_eq!(post_row[1], Value::Text("Hello world".into()), "post title");
    assert_eq!(post_row[2], Value::Integer(1), "post author_id");
}

#[test]
fn array_subquery_user_with_no_posts() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user with no posts
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Lonely".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update");

    assert_eq!(delta.added.len(), 1, "Should have 1 user");

    let output_descriptor = users_with_posts_descriptor();
    let values =
        decode_row(&output_descriptor, &delta.added[0].data).expect("Should decode output row");

    assert_eq!(values[0], Value::Integer(1));
    assert_eq!(values[1], Value::Text("Lonely".into()));

    // Posts array should be empty
    let posts = values[2].as_array().expect("Should have posts array");
    assert_eq!(posts.len(), 0, "User with no posts should have empty array");
}

#[test]
fn array_subquery_require_result_filters_and_readds_rows() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .require_result()
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update");
    assert!(
        initial_delta.added.is_empty(),
        "user without posts should be filtered when include is required"
    );

    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello world".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.process(&mut storage);

    let follow_up_updates = qm.take_updates();
    let follow_up_delta = follow_up_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update after post insert");

    assert_eq!(follow_up_delta.added.len(), 1);
    let row = decode_row(
        &users_with_posts_descriptor(),
        &follow_up_delta.added[0].data,
    )
    .unwrap();
    assert_eq!(row[0], Value::Integer(1));
    assert_eq!(row[1], Value::Text("Alice".into()));
    assert_eq!(row[2].as_array().expect("posts array").len(), 1);
}

#[test]
fn array_subquery_require_full_cardinality_filters_incomplete_array_refs() {
    let sync_manager = SyncManager::new();
    let schema = groups_users_array_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "groups",
        &[
            Value::Integer(10),
            Value::Text("Maintainers".into()),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    let query = qm
        .query("groups")
        .with_array("members", |sub| {
            sub.from("users")
                .correlate("user_id", "groups.member_ids")
                .require_match_correlation_cardinality()
        })
        .build();

    let initial_results = execute_query(&mut qm, &mut storage, query.clone()).unwrap();
    assert!(
        initial_results.is_empty(),
        "group should be filtered when one referenced member is missing"
    );

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();
    let follow_up_results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(follow_up_results.len(), 1);
    let row = &follow_up_results[0].1;
    assert_eq!(row[0], Value::Integer(10));
    assert_eq!(row[1], Value::Text("Maintainers".into()));
    assert_eq!(
        row[2],
        Value::Array(vec![Value::Integer(1), Value::Integer(2)])
    );
    assert_eq!(row[3].as_array().expect("members array").len(), 2);
}

#[test]
fn array_subquery_multiple_users_correct_correlation() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert users
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();

    // Alice's posts (author_id = 1)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Bob's posts (author_id = 2)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(200),
            Value::Text("Bob Post".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates");

    assert_eq!(delta.added.len(), 2, "Should have 2 users");

    let output_descriptor = users_with_posts_descriptor();

    // Build a map of user_id -> posts for verification
    let mut user_posts: std::collections::HashMap<i32, Vec<i32>> = std::collections::HashMap::new();
    for row in &delta.added {
        let values = decode_row(&output_descriptor, &row.data).expect("decode");
        let user_id = match &values[0] {
            Value::Integer(id) => id,
            _ => panic!("User id should be integer"),
        };
        let posts = values[2].as_array().expect("posts array");
        let post_ids: Vec<i32> = posts
            .iter()
            .filter_map(|p| {
                let row_vals = p.as_row()?;
                match &row_vals[0] {
                    Value::Integer(id) => Some(*id),
                    _ => None,
                }
            })
            .collect();
        user_posts.insert(*user_id, post_ids);
    }

    // Alice (id=1) should have post 100
    assert_eq!(
        user_posts.get(&1),
        Some(&vec![100]),
        "Alice should have post 100"
    );

    // Bob (id=2) should have post 200
    assert_eq!(
        user_posts.get(&2),
        Some(&vec![200]),
        "Bob should have post 200"
    );
}

#[test]
fn array_subquery_delta_on_inner_insert() {
    // Test: after subscription, inserting a new post should emit a delta
    // with the updated user row containing the new post in the array.
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user Alice
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert initial post
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post 1".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Subscribe to users with posts
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    // Consume initial update
    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have initial update");
    assert_eq!(initial_delta.added.len(), 1, "Initial: 1 user");

    // Verify initial state: Alice has 1 post
    let output_descriptor = users_with_posts_descriptor();
    let initial_values =
        decode_row(&output_descriptor, &initial_delta.added[0].data).expect("decode initial");
    let initial_posts = initial_values[2].as_array().expect("posts array");
    assert_eq!(initial_posts.len(), 1, "Initially Alice has 1 post");

    // NOW: Insert a new post for Alice
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Post 2".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.process(&mut storage);

    // Check delta after inner insert
    let updates_after_insert = qm.take_updates();
    let delta_after = updates_after_insert
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have delta after post insert");

    // Should have an update (old row removed, new row with updated array added)
    // or just updated entries
    let total_changes = delta_after.added.len() + delta_after.updated.len();
    assert!(
        total_changes > 0,
        "Should have changes after inserting post"
    );

    // Find the new state - either in added or as new part of updated
    let new_row_data = if !delta_after.added.is_empty() {
        &delta_after.added[0].data
    } else if !delta_after.updated.is_empty() {
        &delta_after.updated[0].1.data
    } else {
        panic!("Expected added or updated row");
    };

    let new_values = decode_row(&output_descriptor, new_row_data).expect("decode new");
    let new_posts = new_values[2].as_array().expect("posts array");
    assert_eq!(
        new_posts.len(),
        2,
        "After insert, Alice should have 2 posts"
    );

    // Verify both post IDs are present
    let post_ids: Vec<i32> = new_posts
        .iter()
        .filter_map(|p| match &p.as_row()?[0] {
            Value::Integer(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert!(post_ids.contains(&100), "Should contain post 100");
    assert!(post_ids.contains(&101), "Should contain post 101");
}

#[test]
fn array_subquery_reevaluate_all_does_not_corrupt_base_columns() {
    // When the inner table changes after subscription, reevaluate_all re-builds
    // the output tuple from the stored current_tuple.  Those stored tuples are
    // encoded with the *combined* descriptor (base cols + array col), but
    // build_output_tuple was decoding them with the *base* descriptor only,
    // causing the variable-length offset table to be misread and garbling
    // base column values on every subsequent fire.
    //
    // ASCII flow:
    //
    //   insert Alice (no posts)
    //   subscribe → [Alice, []] Added   ← outer tuple encoded with base desc (correct)
    //   insert Post → reevaluate_all    ← old_tuple now encoded with combined desc
    //                → [???, [Post]] Updated  ← name garbled before fix

    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert Alice with NO posts — ensures the stored current_tuple after the
    // first fire carries an empty-array combined encoding.
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    // Consume and verify the initial Added delta.
    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have initial update");
    assert_eq!(initial_delta.added.len(), 1, "Initial: 1 user row Added");

    let output_descriptor = users_with_posts_descriptor();
    let initial_values =
        decode_row(&output_descriptor, &initial_delta.added[0].data).expect("decode initial row");
    assert_eq!(
        initial_values[0],
        Value::Integer(1),
        "Initial: id must be 1"
    );
    assert_eq!(
        initial_values[1],
        Value::Text("Alice".into()),
        "Initial: name must be Alice"
    );
    assert_eq!(
        initial_values[2].as_array().expect("posts array").len(),
        0,
        "Initial: posts must be empty"
    );

    // Now insert a post — this triggers reevaluate_all on the ArraySubqueryNode.
    // The old_tuple in current_tuples is the combined-descriptor-encoded row
    // from the initial fire.  Before the fix, build_output_tuple decodes it
    // with the base descriptor, corrupting the variable-length name field.
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("First Post".into()),
            Value::Integer(1), // author_id = Alice
        ],
    )
    .unwrap();
    qm.process(&mut storage);

    let second_updates = qm.take_updates();
    let second_delta = second_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have delta after post insert");

    assert!(
        !second_delta.updated.is_empty() || !second_delta.added.is_empty(),
        "Expected an Updated or Added row after inner insert"
    );

    let new_row_data = if !second_delta.updated.is_empty() {
        &second_delta.updated[0].1.data
    } else {
        &second_delta.added[0].data
    };

    let new_values = decode_row(&output_descriptor, new_row_data).expect("decode updated row");

    // These are the assertions that expose the bug: before the fix, the
    // variable-length offset table mismatch causes id and name to be garbled.
    assert_eq!(
        new_values[0],
        Value::Integer(1),
        "After inner insert: id must not be corrupted"
    );
    assert_eq!(
        new_values[1],
        Value::Text("Alice".into()),
        "After inner insert: name must not be corrupted"
    );
    assert_eq!(
        new_values[2].as_array().expect("posts array").len(),
        1,
        "After inner insert: posts array must contain 1 post"
    );
}

#[test]
fn array_subquery_delta_on_outer_insert() {
    // Test: after subscription, inserting a new user should emit a delta
    // with the new user row (with their posts array).
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user Alice with a post
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Also insert a post for Bob (who doesn't exist yet)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(200),
            Value::Text("Bob Post".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Subscribe
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    // Consume initial update (just Alice)
    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have initial update");
    assert_eq!(initial_delta.added.len(), 1, "Initial: only Alice");

    // NOW: Insert Bob
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();
    qm.process(&mut storage);

    // Check delta after outer insert
    let updates_after = qm.take_updates();
    let delta_after = updates_after
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have delta after user insert");

    // Should have Bob added
    assert_eq!(delta_after.added.len(), 1, "Bob should be added");

    let output_descriptor = users_with_posts_descriptor();
    let bob_values =
        decode_row(&output_descriptor, &delta_after.added[0].data).expect("decode Bob");

    assert_eq!(bob_values[0], Value::Integer(2), "Should be Bob (id=2)");
    assert_eq!(
        bob_values[1],
        Value::Text("Bob".into()),
        "Name should be Bob"
    );

    // Bob should have his post (id=200)
    let bob_posts = bob_values[2].as_array().expect("posts array");
    assert_eq!(bob_posts.len(), 1, "Bob should have 1 post");

    let post_row = bob_posts[0].as_row().expect("post should be Row");
    assert!(
        bob_posts[0].row_id().is_some(),
        "post Row should have an id"
    );
    assert_eq!(
        post_row[0],
        Value::Integer(200),
        "Bob's post should be id=200"
    );
}

#[test]
fn array_subquery_with_order_by() {
    // Test: posts should be ordered by id descending
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert posts in random order
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(102),
            Value::Text("Middle".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("First".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Last".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Query with order_by_desc on id
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .order_by_desc("id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    let output_descriptor = users_with_posts_descriptor();
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 3, "Should have 3 posts");

    // Verify order: should be 102, 101, 100 (descending by id)
    let post_ids: Vec<i32> = posts
        .iter()
        .filter_map(|p| match &p.as_row()?[0] {
            Value::Integer(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(
        post_ids,
        vec![102, 101, 100],
        "Posts should be ordered by id desc"
    );
}

#[test]
fn array_subquery_with_limit() {
    // Test: limit should restrict number of posts returned
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert 5 posts
    for i in 100..105 {
        qm.insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(i),
                Value::Text(format!("Post {}", i)),
                Value::Integer(1),
            ],
        )
        .unwrap();
    }

    // Query with limit 2
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .order_by("id")
                .limit(2)
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    let output_descriptor = users_with_posts_descriptor();
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 2, "Limit should restrict to 2 posts");

    // Verify first 2 posts by id ascending
    let post_ids: Vec<i32> = posts
        .iter()
        .filter_map(|p| match &p.as_row()?[0] {
            Value::Integer(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(post_ids, vec![100, 101], "Should get first 2 posts by id");
}

#[test]
fn array_subquery_with_select_columns() {
    // Test: select specific columns from inner query
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user and post
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post Title".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Query selecting only id and title (not author_id)
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .select(&["id", "title"])
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build descriptor for selected columns only
    // Row id is in Value::Row { id, .. }, not as a column
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
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
    ]);

    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 1, "Should have 1 post");

    let post_row = posts[0].as_row().expect("post Row");
    assert_eq!(post_row.len(), 2, "Post should have 2 columns (id, title)");
    assert!(posts[0].row_id().is_some(), "post Row should have an id");
    assert_eq!(post_row[0], Value::Integer(100));
    assert_eq!(post_row[1], Value::Text("Post Title".into()));
}

#[test]
fn array_subquery_with_join() {
    // Test: join inside array subquery
    // users with_array of (posts joined with comments)
    let sync_manager = SyncManager::new();
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
    schema.insert(
        TableName::new("comments"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("post_id", ColumnType::Integer),
        ])
        .into(),
    );

    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert posts
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post A".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Post B".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Insert comments
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1000),
            Value::Text("Comment on A".into()),
            Value::Integer(100), // post_id = 100
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1001),
            Value::Text("Another on A".into()),
            Value::Integer(100), // post_id = 100
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1002),
            Value::Text("Comment on B".into()),
            Value::Integer(101), // post_id = 101
        ],
    )
    .unwrap();

    // Query users with (posts joined with comments)
    // This should give us: for each user, an array of (post, comment) pairs
    let query = qm
        .query("users")
        .with_array("post_comments", |sub| {
            sub.from("posts")
                .join("comments")
                .on("posts.id", "comments.post_id")
                .correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build descriptor for joined output:
    // posts columns + comments columns (row id in Value::Row { id, .. })
    let joined_row_desc = RowDescriptor::new(vec![
        // posts columns
        ColumnDescriptor::new("post_id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
        // comments columns
        ColumnDescriptor::new("comment_id", ColumnType::Integer),
        ColumnDescriptor::new("text", ColumnType::Text),
        ColumnDescriptor::new("post_id", ColumnType::Integer),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "post_comments",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(joined_row_desc),
                }),
            },
        ),
    ]);

    assert_eq!(delta.added.len(), 1, "Should have 1 user");
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    assert_eq!(values[0], Value::Integer(1)); // user id
    assert_eq!(values[1], Value::Text("Alice".into())); // user name

    let post_comments = values[2].as_array().expect("post_comments array");
    // Each (post, comment) pair - Post A has 2 comments, Post B has 1
    assert_eq!(post_comments.len(), 3, "Should have 3 post-comment pairs");

    // Verify the joined rows contain both post and comment data
    for pc in post_comments {
        let row = pc.as_row().expect("joined row");
        assert_eq!(row.len(), 6, "Joined row should have 6 columns");
        assert!(pc.row_id().is_some(), "joined Row should have an id");
        // Post id should be either 100 or 101
        let post_id = match &row[0] {
            Value::Integer(id) => id,
            _ => panic!("Expected integer for post id"),
        };
        assert!(*post_id == 100 || *post_id == 101);
        // Comment post_id should match the post id
        assert_eq!(row[5], Value::Integer(*post_id));
    }
}

#[test]
fn array_subquery_nested() {
    // Test: nested array subqueries
    // users with_array(posts with_array(comments))
    let sync_manager = SyncManager::new();
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
    schema.insert(
        TableName::new("comments"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("post_id", ColumnType::Integer),
        ])
        .into(),
    );

    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert posts
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post A".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Post B".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Insert comments - 2 on Post A, 1 on Post B
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1000),
            Value::Text("Comment 1 on A".into()),
            Value::Integer(100),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1001),
            Value::Text("Comment 2 on A".into()),
            Value::Integer(100),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1002),
            Value::Text("Comment on B".into()),
            Value::Integer(101),
        ],
    )
    .unwrap();

    // Query: users with posts, where each post has its comments
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .with_array("comments", |sub2| {
                    sub2.from("comments").correlate("post_id", "posts.id")
                })
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build nested descriptor matching runtime output:
    // comments row: [id, text, post_id] (row id in Value::Row { id, .. })
    let comments_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("comment_id", ColumnType::Integer),
        ColumnDescriptor::new("text", ColumnType::Text),
        ColumnDescriptor::new("post_id", ColumnType::Integer),
    ]);
    // posts row with comments array: [id, title, author_id, comments[]]
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("post_id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
        ColumnDescriptor::new(
            "comments",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(comments_row_desc),
                }),
            },
        ),
    ]);
    // users row with posts array: [id, name, posts[]]
    let output_descriptor = RowDescriptor::new(vec![
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
    ]);

    assert_eq!(delta.added.len(), 1, "Should have 1 user");
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    assert_eq!(values[0], Value::Integer(1)); // user id
    assert_eq!(values[1], Value::Text("Alice".into())); // user name

    let posts = values[2].as_array().expect("posts array");
    assert_eq!(posts.len(), 2, "Alice should have 2 posts");

    // Check each post has its comments
    for post in posts {
        let post_row = post.as_row().expect("post row");
        assert_eq!(
            post_row.len(),
            4,
            "Post should have 4 columns (id, title, author_id, comments)"
        );
        assert!(post.row_id().is_some(), "post Row should have an id");

        let post_id = match &post_row[0] {
            Value::Integer(id) => id,
            _ => panic!("Expected integer for post id"),
        };

        let comments = post_row[3].as_array().expect("comments array");

        if *post_id == 100 {
            // Post A has 2 comments
            assert_eq!(comments.len(), 2, "Post A should have 2 comments");
            for comment in comments {
                let comment_row = comment.as_row().expect("comment row");
                // comment_row: [id:Int, text:Text, post_id:Int]
                assert!(comment.row_id().is_some(), "comment should have an id");
                assert_eq!(comment_row[2], Value::Integer(100)); // post_id
            }
        } else if *post_id == 101 {
            // Post B has 1 comment
            assert_eq!(comments.len(), 1, "Post B should have 1 comment");
            let comment_row = comments[0].as_row().expect("comment row");
            assert!(comments[0].row_id().is_some(), "comment should have an id");
            assert_eq!(comment_row[2], Value::Integer(101)); // post_id
        } else {
            panic!("Unexpected post id: {}", post_id);
        }
    }
}

#[test]
fn array_subquery_multiple_columns() {
    // Test: two separate (non-nested) array subquery columns
    // users with posts[] and with comments[] (comments directly on user)
    let sync_manager = SyncManager::new();
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
    schema.insert(
        TableName::new("comments"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("user_id", ColumnType::Integer),
        ])
        .into(),
    );

    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert users
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();

    // Insert posts - Alice has 2, Bob has 1
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post 1".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Alice Post 2".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(102),
            Value::Text("Bob Post".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Insert comments (directly on users) - Alice has 1, Bob has 2
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1000),
            Value::Text("Alice comment".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1001),
            Value::Text("Bob comment 1".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1002),
            Value::Text("Bob comment 2".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Query: users with both posts[] and comments[]
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .with_array("comments", |sub| {
            sub.from("comments").correlate("user_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build descriptor: users + posts[] + comments[]
    // Row ids are in Value::Row { id, .. }, not as columns
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("post_id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
    ]);
    let comments_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("comment_id", ColumnType::Integer),
        ColumnDescriptor::new("text", ColumnType::Text),
        ColumnDescriptor::new("user_id", ColumnType::Integer),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
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
        ColumnDescriptor::new(
            "comments",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(comments_row_desc),
                }),
            },
        ),
    ]);

    assert_eq!(delta.added.len(), 2, "Should have 2 users");

    // Decode and verify each user
    for row in &delta.added {
        let values = decode_row(&output_descriptor, &row.data).expect("decode");
        let user_id = match &values[0] {
            Value::Integer(id) => id,
            _ => panic!("Expected integer for user id"),
        };

        let posts = values[2].as_array().expect("posts array");
        let comments = values[3].as_array().expect("comments array");

        if *user_id == 1 {
            // Alice: 2 posts, 1 comment
            assert_eq!(values[1], Value::Text("Alice".into()));
            assert_eq!(posts.len(), 2, "Alice should have 2 posts");
            assert_eq!(comments.len(), 1, "Alice should have 1 comment");
        } else if *user_id == 2 {
            // Bob: 1 post, 2 comments
            assert_eq!(values[1], Value::Text("Bob".into()));
            assert_eq!(posts.len(), 1, "Bob should have 1 post");
            assert_eq!(comments.len(), 2, "Bob should have 2 comments");
        } else {
            panic!("Unexpected user id: {}", user_id);
        }
    }
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

#[test]
fn policy_filters_select_results() {
    let sync_manager = SyncManager::new();
    let schema = policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert documents
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("alice".into()),
            Value::Text("eng".into()),
            Value::Text("Alice's eng doc".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("eng".into()),
            Value::Text("Bob's eng doc".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("sales".into()),
            Value::Text("Bob's sales doc".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("charlie".into()),
            Value::Text("design".into()),
            Value::Text("Charlie's design doc".into()),
        ],
    )
    .unwrap();

    // Alice can see: her own doc + all eng docs = 2 docs
    let alice_session = PolicySession::new("alice").with_claims(json!({"teams": ["eng"]}));

    let query = qm.query("documents").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(alice_session), None)
        .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let alice_update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .unwrap();

    assert_eq!(
        alice_update.delta.added.len(),
        2,
        "Alice should see 2 docs (her own + Bob's eng doc)"
    );

    // Bob on sales team can see: his 2 docs + no team docs (sales only) = 2 docs
    let bob_session = PolicySession::new("bob").with_claims(json!({"teams": ["sales"]}));

    let query2 = qm.query("documents").build();
    let sub_id2 = qm
        .subscribe_with_session(query2, Some(bob_session), None)
        .unwrap();

    qm.process(&mut storage);
    let updates2 = qm.take_updates();
    let bob_update = updates2
        .iter()
        .find(|u| u.subscription_id == sub_id2)
        .unwrap();

    assert_eq!(
        bob_update.delta.added.len(),
        2,
        "Bob should see 2 docs (his own 2 docs)"
    );
}

#[test]
fn policy_filtered_query_reads_visible_region_after_legacy_commit_history_is_removed() {
    let schema = policy_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handles = vec![
        writer_qm
            .insert(
                &mut storage,
                "documents",
                &[
                    Value::Text("alice".into()),
                    Value::Text("eng".into()),
                    Value::Text("Alice's eng doc".into()),
                ],
            )
            .unwrap(),
        writer_qm
            .insert(
                &mut storage,
                "documents",
                &[
                    Value::Text("bob".into()),
                    Value::Text("eng".into()),
                    Value::Text("Bob's eng doc".into()),
                ],
            )
            .unwrap(),
        writer_qm
            .insert(
                &mut storage,
                "documents",
                &[
                    Value::Text("bob".into()),
                    Value::Text("sales".into()),
                    Value::Text("Bob's sales doc".into()),
                ],
            )
            .unwrap(),
        writer_qm
            .insert(
                &mut storage,
                "documents",
                &[
                    Value::Text("charlie".into()),
                    Value::Text("design".into()),
                    Value::Text("Charlie's design doc".into()),
                ],
            )
            .unwrap(),
    ];
    writer_qm.process(&mut storage);

    for _handle in handles {}

    let mut reader_qm = QueryManager::new(SyncManager::new());
    reader_qm.set_current_schema(schema, "dev", "main");

    let alice_session = PolicySession::new("alice").with_claims(json!({"teams": ["eng"]}));
    let query = reader_qm.query("documents").build();
    let sub_id = reader_qm
        .subscribe_with_session(query, Some(alice_session), None)
        .unwrap();

    reader_qm.process(&mut storage);

    let results = reader_qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 2);

    let titles: Vec<_> = results
        .iter()
        .filter_map(|(_, row)| match &row[2] {
            Value::Text(title) => Some(title.as_str()),
            _ => None,
        })
        .collect();
    assert!(titles.contains(&"Alice's eng doc"));
    assert!(titles.contains(&"Bob's eng doc"));
}

#[test]
fn no_session_returns_all_rows() {
    let sync_manager = SyncManager::new();
    let schema = policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert documents
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("alice".into()),
            Value::Text("eng".into()),
            Value::Text("Doc 1".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("sales".into()),
            Value::Text("Doc 2".into()),
        ],
    )
    .unwrap();

    // Without session, all rows should be returned (policy not applied)
    let query = qm.query("documents").build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .unwrap();

    assert_eq!(
        update.delta.added.len(),
        2,
        "Without session, should see all 2 docs"
    );
}

#[test]
fn permissive_local_runtime_without_loaded_policies_returns_all_rows() {
    let sync_manager = SyncManager::new();
    // Use the regular test_schema which has no policies
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(200)],
    )
    .unwrap();

    // Without a loaded policy bundle, local session-scoped reads stay permissive.
    let session = PolicySession::new("some_user");
    let query = qm.query("users").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(session), None)
        .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .unwrap();

    assert_eq!(
        update.delta.added.len(),
        2,
        "policy-less local runtimes should keep returning rows until a compiled bundle is loaded"
    );
}

#[test]
fn loaded_empty_permissions_bundle_hides_rows_without_explicit_read_policy() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, test_schema());

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(200)],
    )
    .unwrap();

    qm.set_authorization_schema(test_schema());

    let query = qm.query("users").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(PolicySession::new("alice")), None)
        .unwrap();

    qm.process(&mut storage);

    assert!(
        qm.get_subscription_results(sub_id).is_empty(),
        "loaded empty permissions bundle should deny session-scoped reads without an explicit read grant"
    );
}

#[test]
fn join_query_applies_policy_filter_on_joined_table() {
    let sync_manager = SyncManager::new();
    let schema = join_policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(&mut storage, "users", &[Value::Text("alice".into())])
        .unwrap();
    qm.insert(&mut storage, "users", &[Value::Text("bob".into())])
        .unwrap();

    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("alice".into()),
            Value::Text("Alice post".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[Value::Text("bob".into()), Value::Text("Bob post".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .join("posts")
        .on("users.name", "posts.owner_name")
        .build();
    let sub_id = qm
        .subscribe_with_session(query, Some(PolicySession::new("alice")), None)
        .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("join subscription should emit initial delta");
    assert_eq!(
        update.delta.added.len(),
        1,
        "join query should apply policy filter on joined table rows"
    );
}

#[test]
fn local_subscription_uses_current_permissions_after_lens_transform() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, legacy_documents_schema());
    configure_legacy_client_with_current_permissions(&mut qm);

    qm.insert(
        &mut storage,
        "documents",
        &[Value::Text("Legacy doc".into())],
    )
    .unwrap();

    let query = qm.query("documents").build();
    let alice_sub = qm
        .subscribe_with_session(query.clone(), Some(PolicySession::new("alice")), None)
        .unwrap();
    let bob_sub = qm
        .subscribe_with_session(query, Some(PolicySession::new("bob")), None)
        .unwrap();

    qm.process(&mut storage);

    let alice_results = qm.get_subscription_results(alice_sub);
    assert_eq!(
        alice_results.len(),
        1,
        "Alice should see the legacy row after lens-based auth transform"
    );
    assert_eq!(alice_results[0].1, vec![Value::Text("Legacy doc".into())]);
    assert!(
        qm.get_subscription_results(bob_sub).is_empty(),
        "Bob should be filtered out by the current permission schema"
    );
}

#[test]
fn local_insert_uses_current_permissions_after_lens_transform() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, legacy_documents_schema());
    configure_legacy_client_with_current_permissions(&mut qm);

    qm.insert_with_session(
        &mut storage,
        "documents",
        &[Value::Text("Alice insert".into())],
        Some(&PolicySession::new("alice")),
    )
    .expect("alice insert should be allowed by transformed current permissions");

    let err = qm
        .insert_with_session(
            &mut storage,
            "documents",
            &[Value::Text("Bob insert".into())],
            Some(&PolicySession::new("bob")),
        )
        .expect_err("bob insert should be denied by transformed current permissions");
    assert_eq!(
        err,
        QueryError::PolicyDenied {
            table: TableName::new("documents"),
            operation: crate::query_manager::policy::Operation::Insert,
        }
    );
}

#[test]
fn loaded_empty_permissions_bundle_denies_local_insert_without_explicit_insert_policy() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, test_schema());

    qm.set_authorization_schema(test_schema());

    let err = qm
        .insert_with_session(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
            Some(&PolicySession::new("alice")),
        )
        .expect_err("loaded empty permissions bundle should deny insert without explicit policy");

    assert_eq!(
        err,
        QueryError::PolicyDenied {
            table: TableName::new("users"),
            operation: crate::query_manager::policy::Operation::Insert,
        }
    );
}

#[test]
fn local_update_and_delete_use_current_permissions_after_lens_transform() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, legacy_documents_schema());
    configure_legacy_client_with_current_permissions(&mut qm);

    let inserted = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("Legacy doc".into())],
        )
        .unwrap();

    let update_err = qm
        .update_with_session(
            &mut storage,
            inserted.row_id,
            &[Value::Text("Bob edit".into())],
            Some(&PolicySession::new("bob")),
        )
        .expect_err("bob update should be denied by transformed current permissions");
    assert_eq!(
        update_err,
        QueryError::PolicyDenied {
            table: TableName::new("documents"),
            operation: crate::query_manager::policy::Operation::Update,
        }
    );

    qm.update_with_session(
        &mut storage,
        inserted.row_id,
        &[Value::Text("Alice edit".into())],
        Some(&PolicySession::new("alice")),
    )
    .expect("alice update should be allowed by transformed current permissions");

    let delete_err = qm
        .delete_with_session(
            &mut storage,
            inserted.row_id,
            Some(&PolicySession::new("bob")),
        )
        .expect_err("bob delete should be denied by transformed current permissions");
    assert_eq!(
        delete_err,
        QueryError::PolicyDenied {
            table: TableName::new("documents"),
            operation: crate::query_manager::policy::Operation::Delete,
        }
    );

    qm.delete_with_session(
        &mut storage,
        inserted.row_id,
        Some(&PolicySession::new("alice")),
    )
    .expect("alice delete should be allowed by transformed current permissions");
}

#[test]
fn local_join_query_uses_current_permissions_for_joined_provenance_after_lens_transform() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, legacy_join_provenance_schema());
    configure_legacy_join_client_with_current_permissions(&mut qm);

    qm.insert(&mut storage, "users", &[Value::Text("bob".into())])
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("bob".into()),
            Value::Text("Bob private post".into()),
        ],
    )
    .unwrap();

    let query = QueryBuilder::new("users")
        .join("posts")
        .on("users.name", "posts.owner_name")
        .build();
    let alice_sub = qm
        .subscribe_with_session(query.clone(), Some(PolicySession::new("alice")), None)
        .unwrap();
    let bob_sub = qm
        .subscribe_with_session(query, Some(PolicySession::new("bob")), None)
        .unwrap();

    qm.process(&mut storage);

    assert!(
        qm.get_subscription_results(alice_sub).is_empty(),
        "Joined provenance rows should be filtered when the transformed current permissions deny them"
    );

    let bob_results = qm.get_subscription_results(bob_sub);
    assert_eq!(
        bob_results.len(),
        1,
        "Bob should see the joined row after provenance rows are transformed into the current auth schema"
    );
    assert!(
        bob_results[0]
            .1
            .iter()
            .any(|value| matches!(value, Value::Text(text) if text == "Bob private post")),
        "Joined result should retain the post payload after current-permissions filtering"
    );
}

#[test]
fn server_join_query_uses_current_permissions_for_joined_provenance() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::query_manager::types::{ComposedBranchName, SchemaHash};
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};

    let authorization_schema = join_policy_schema();
    let structural_schema: Schema = authorization_schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut structural = table_schema.clone();
            structural.policies = TablePolicies::default();
            (*table_name, structural)
        })
        .collect();
    let schema_hash = SchemaHash::compute(&structural_schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    let sync_manager = SyncManager::new();
    let mut server_qm = QueryManager::new(sync_manager);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, structural_schema);
    server_qm.set_known_schemas(Arc::new(known_schemas));
    let storage_schema = authorization_schema.clone();
    server_qm.set_authorization_schema(authorization_schema);

    let mut storage = seeded_memory_storage(&storage_schema);
    let author = ObjectId::new();

    let mut user_metadata = HashMap::new();
    user_metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    let user_id = create_test_row(&mut storage, Some(user_metadata));
    add_row_commit(
        &mut storage,
        user_id,
        &branch,
        vec![],
        encode_row(
            &RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            &[Value::Text("bob".into())],
        )
        .unwrap(),
        1000,
        author.to_string(),
    );

    let mut post_metadata = HashMap::new();
    post_metadata.insert(MetadataKey::Table.to_string(), "posts".to_string());
    let post_id = create_test_row(&mut storage, Some(post_metadata));
    add_row_commit(
        &mut storage,
        post_id,
        &branch,
        vec![],
        encode_row(
            &RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_name", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]),
            &[
                Value::Text("bob".into()),
                Value::Text("Bob private post".into()),
            ],
        )
        .unwrap(),
        1000,
        author.to_string(),
    );

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let session = PolicySession::new("alice");
    server_qm
        .sync_manager_mut()
        .set_client_session(client_id, session.clone());

    let query = QueryBuilder::new("users")
        .branch(&branch)
        .join("posts")
        .on("users.name", "posts.owner_name")
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: Some(session),
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|entry| matches!(entry.destination, Destination::Client(id) if id == client_id))
        .filter(|entry| matches!(entry.payload, SyncPayload::RowBatchNeeded { .. }))
        .collect();

    assert!(
        row_updates.is_empty(),
        "Joined rows should be filtered when current permissions deny any contributing provenance row"
    );
}

// ========================================================================
// Branch-aware query tests
// ========================================================================

#[test]
fn index_key_includes_branch() {
    // Verify branch isolation through observable query results.
    // A row inserted on the current branch should not appear on another branch.

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert on the schema's branch.
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    let branch = get_branch(&qm);
    let current_branch_query = qm.query("users").branch(&branch).build();
    let current_branch_results =
        execute_query(&mut qm, &mut storage, current_branch_query).unwrap();
    assert_eq!(
        current_branch_results.len(),
        1,
        "Should find row on current branch"
    );
    assert_eq!(current_branch_results[0].1[0], Value::Text("Alice".into()));

    // Verify the row is NOT visible on a different branch.
    let other_branch = get_branch_for_user_branch(&qm, "some-other-branch");
    let other_branch_query = qm.query("users").branch(&other_branch).build();
    let other_branch_results = execute_query(&mut qm, &mut storage, other_branch_query).unwrap();
    assert!(
        other_branch_results.is_empty(),
        "Should NOT find row on a different branch"
    );
}

#[test]
fn query_builder_single_branch_uses_correct_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert on default "main" branch
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Query explicitly specifying "main" branch
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find row on main branch");

    // Query specifying a different branch should return no results
    // (since we haven't inserted on that branch)
    let draft_branch = get_branch_for_user_branch(&qm, "draft");
    let query = qm.query("users").branch(&draft_branch).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 0, "Should not find row on draft branch");
}

#[test]
fn query_builder_explicit_main_branch() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(50)],
    )
    .unwrap();

    // Explicit .branch("main") should work same as default
    let query_explicit = qm.query("users").build();
    let query_default = qm.query("users").build();

    let results_explicit = execute_query(&mut qm, &mut storage, query_explicit).unwrap();
    let results_default = execute_query(&mut qm, &mut storage, query_default).unwrap();

    assert_eq!(results_explicit.len(), results_default.len());
    assert_eq!(results_explicit.len(), 2);
}

#[test]
fn query_on_composed_noncurrent_branch_reads_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    let inserted = qm
        .insert_on_branch(
            &mut storage,
            "users",
            &draft_branch,
            &[Value::Text("Dora".into()), Value::Integer(42)],
        )
        .unwrap();

    assert_eq!(
        storage.index_lookup("users", "_id", &draft_branch, &Value::Uuid(inserted.row_id)),
        vec![inserted.row_id]
    );

    let query = qm.query("users").branch(&draft_branch).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Dora".into()));
}

#[test]
fn query_multi_branch_requires_explicit_branch() {
    // Verify Query.branches field exists and works
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);
    let main_branch = get_branch(&qm);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    // Multi-branch query with explicit branches
    let query = qm
        .query("users")
        .branches(&[main_branch.as_str(), draft_branch.as_str()])
        .build();
    assert_eq!(query.branches.len(), 2);
    assert!(query.is_multi_branch());

    // Query without explicit branch has empty branches field.
    // The actual branches are resolved at execution time from schema context.
    let query = qm.query("users").build();
    assert!(query.branches.is_empty());
    assert!(!query.is_multi_branch());
}

#[test]
fn join_query_with_multiple_branches_reads_all_branches() {
    let sync_manager = SyncManager::new();
    let schema = join_policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let main_branch = get_branch(&qm);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    qm.insert(&mut storage, "users", &[Value::Text("alice".into())])
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[Value::Text("alice".into()), Value::Text("main post".into())],
    )
    .unwrap();

    qm.insert_on_branch(
        &mut storage,
        "users",
        &draft_branch,
        &[Value::Text("dora".into())],
    )
    .unwrap();
    qm.insert_on_branch(
        &mut storage,
        "posts",
        &draft_branch,
        &[Value::Text("dora".into()), Value::Text("draft post".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .branches(&[main_branch.as_str(), draft_branch.as_str()])
        .join("posts")
        .on("users.name", "posts.owner_name")
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("join subscription should emit initial delta");
    assert_eq!(
        update.delta.added.len(),
        2,
        "join query across branches should include rows from each branch"
    );
}

#[test]
fn handle_object_update_respects_branch() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    // Verify that handle_object_update updates the correct branch's indices.
    // Rows on a non-schema branch should NOT appear in queries on the schema branch.
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Get the actual schema branch
    let schema_branch = get_branch(&qm);

    let row_id = crate::object::ObjectId::new();
    let author = row_id;

    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let row_data = encode_row(
        &descriptor,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Receive commit on "other-branch" (not the schema's branch)
    let commit = stored_row_commit(smallvec![], row_data.clone(), 1000, author.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, "other-branch", commit);

    qm.process(&mut storage);

    // Query schema branch - should NOT find the row (it's on other-branch)
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Row on other-branch should not appear in schema branch query"
    );

    // Now insert on schema branch and verify it appears in default query
    let row_id2 = crate::object::ObjectId::new();
    let mut metadata2 = HashMap::new();
    metadata2.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id2, metadata2);

    let commit2 = stored_row_commit(smallvec![], row_data, 2000, row_id2.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id2, &schema_branch, commit2);

    qm.process(&mut storage);

    // Schema branch should now have 1 row
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Row on schema branch should appear in default query"
    );
}

// ============================================================================
// Contributing ObjectIds Tests
// ============================================================================

#[test]
fn contributing_ids_reflect_filter() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert 3 rows with different scores
    let handle1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let handle2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    let handle3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    // Subscribe to query: score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query.clone()).unwrap();

    qm.process(&mut storage);

    // Get contributing ObjectIds
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    // Should have 2 rows (Alice: 100, Charlie: 75), not Bob (30)
    assert_eq!(contributing.len(), 2, "Should have 2 contributing IDs");

    let branch_str = get_branch(&qm);
    let branch = crate::object::BranchName::new(&branch_str);
    assert!(
        contributing.contains(&(handle1.row_id, branch)),
        "Alice should be in contributing set"
    );
    assert!(
        !contributing.contains(&(handle2.row_id, branch)),
        "Bob should NOT be in contributing set (score < 50)"
    );
    assert!(
        contributing.contains(&(handle3.row_id, branch)),
        "Charlie should be in contributing set"
    );
}

#[test]
fn contributing_ids_update_reactively() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert 2 rows initially
    let _handle1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let handle2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();

    // Subscribe to query: score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query.clone()).unwrap();

    qm.process(&mut storage);

    // Initially 1 match (Alice: 100)
    let contributing = qm.get_subscription_contributing_ids(sub_id);
    assert_eq!(
        contributing.len(),
        1,
        "Should have 1 contributing ID initially"
    );

    // Insert new row with score > 50
    let handle3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    qm.process(&mut storage);

    // Now 2 matches
    let contributing = qm.get_subscription_contributing_ids(sub_id);
    assert_eq!(
        contributing.len(),
        2,
        "Should have 2 contributing IDs after insert"
    );

    let branch_str = get_branch(&qm);
    let branch = crate::object::BranchName::new(&branch_str);
    assert!(
        contributing.contains(&(handle3.row_id, branch)),
        "Charlie should be in contributing set"
    );

    // Update Bob's score to > 50
    qm.update(
        &mut storage,
        handle2.row_id,
        &[Value::Text("Bob".into()), Value::Integer(60)],
    )
    .unwrap();
    qm.process(&mut storage);

    // Now 3 matches
    let contributing = qm.get_subscription_contributing_ids(sub_id);
    assert_eq!(
        contributing.len(),
        3,
        "Should have 3 contributing IDs after update"
    );
    assert!(
        contributing.contains(&(handle2.row_id, branch)),
        "Bob should now be in contributing set"
    );
}

#[test]
fn contributing_ids_for_limit_offset_include_ordered_prefix() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle_a = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("A".into()), Value::Integer(1)],
        )
        .unwrap();
    let handle_b = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("B".into()), Value::Integer(2)],
        )
        .unwrap();
    let handle_c = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("C".into()), Value::Integer(3)],
        )
        .unwrap();
    let _handle_d = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("D".into()), Value::Integer(4)],
        )
        .unwrap();

    let query = qm
        .query("users")
        .order_by("score")
        .offset(2)
        .limit(1)
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);

    let branch = crate::object::BranchName::new(get_branch(&qm));
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    assert_eq!(
        contributing.len(),
        3,
        "Paginated queries need the ordered prefix through offset + limit"
    );
    assert!(contributing.contains(&(handle_a.row_id, branch)));
    assert!(contributing.contains(&(handle_b.row_id, branch)));
    assert!(contributing.contains(&(handle_c.row_id, branch)));
}

#[test]
fn contributing_ids_for_offset_only_include_full_input() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handles = [
        qm.insert(
            &mut storage,
            "users",
            &[Value::Text("A".into()), Value::Integer(1)],
        )
        .unwrap(),
        qm.insert(
            &mut storage,
            "users",
            &[Value::Text("B".into()), Value::Integer(2)],
        )
        .unwrap(),
        qm.insert(
            &mut storage,
            "users",
            &[Value::Text("C".into()), Value::Integer(3)],
        )
        .unwrap(),
        qm.insert(
            &mut storage,
            "users",
            &[Value::Text("D".into()), Value::Integer(4)],
        )
        .unwrap(),
    ];

    let query = qm.query("users").order_by("score").offset(2).build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);

    let branch = crate::object::BranchName::new(get_branch(&qm));
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    assert_eq!(
        contributing.len(),
        handles.len(),
        "Offset without limit still needs the full ordered input to replay locally"
    );
    for handle in handles {
        assert!(contributing.contains(&(handle.row_id, branch)));
    }
}

#[test]
fn contributing_ids_for_array_subquery_include_inner_rows() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let user = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();
    let post1 = qm
        .insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Post 1".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
    let post2 = qm
        .insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Post 2".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);

    let branch = crate::object::BranchName::new(get_branch(&qm));
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    assert_eq!(
        contributing.len(),
        3,
        "Array subquery outputs depend on both outer and inner rows"
    );
    assert!(contributing.contains(&(user.row_id, branch)));
    assert!(contributing.contains(&(post1.row_id, branch)));
    assert!(contributing.contains(&(post2.row_id, branch)));
}

#[test]
fn contributing_ids_for_recursive_hop_include_recursive_dependencies() {
    let sync_manager = SyncManager::new();
    let schema = recursive_hop_team_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let team1 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-1".into())])
        .unwrap();
    let team2 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-2".into())])
        .unwrap();
    let team3 = qm
        .insert(&mut storage, "teams", &[Value::Text("team-3".into())])
        .unwrap();

    let edge1 = qm
        .insert(
            &mut storage,
            "team_edges",
            &[Value::Uuid(team1.row_id), Value::Uuid(team2.row_id)],
        )
        .unwrap();
    let edge2 = qm
        .insert(
            &mut storage,
            "team_edges",
            &[Value::Uuid(team2.row_id), Value::Uuid(team3.row_id)],
        )
        .unwrap();

    let query = qm
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);

    let branch = crate::object::BranchName::new(get_branch(&qm));
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    assert_eq!(
        contributing.len(),
        5,
        "Recursive hop outputs depend on both discovered rows and traversal edges"
    );
    assert!(contributing.contains(&(team1.row_id, branch)));
    assert!(contributing.contains(&(team2.row_id, branch)));
    assert!(contributing.contains(&(team3.row_id, branch)));
    assert!(contributing.contains(&(edge1.row_id, branch)));
    assert!(contributing.contains(&(edge2.row_id, branch)));
}

// ============================================================================
// Server-Side Query Subscription Tests
// ============================================================================

#[test]
fn server_builds_query_graph_on_subscription() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Server has existing data: 3 users, 2 with score > 50
    let handle1 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let _handle2 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    let handle3 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Add a client
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    // Client sends QuerySubscription for score > 50
    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    // Server should send RowBatchNeeded for matching users (Alice, Charlie)
    let outbox = server_qm.sync_manager_mut().take_outbox();

    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter_map(|e| match &e.payload {
            SyncPayload::RowBatchNeeded { row, .. } => Some(row.row_id),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_updates.len(),
        2,
        "Should send 2 RowBatchNeeded messages for matching users"
    );

    let sent_ids: std::collections::HashSet<_> = row_updates.into_iter().collect();

    assert!(sent_ids.contains(&handle1.row_id), "Alice should be sent");
    assert!(sent_ids.contains(&handle3.row_id), "Charlie should be sent");
}

#[test]
fn server_subscription_reads_visible_region_after_legacy_commit_history_is_removed() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let schema = test_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handle = writer_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    writer_qm.process(&mut storage);

    let (mut server_qm, _) = create_query_manager(SyncManager::new(), schema);
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|entry| matches!(entry.destination, Destination::Client(id) if id == client_id))
        .filter_map(|entry| match &entry.payload {
            SyncPayload::RowBatchNeeded { row, .. } => Some(row.row_id),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_updates.len(),
        1,
        "server subscription should settle from visible rows without legacy object-backed storage"
    );
    assert_eq!(row_updates[0], handle.row_id);
}

#[test]
fn local_stale_recompile_failure_drops_subscription_and_reports_failure() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let sub_id = qm.subscribe(qm.query("users").build()).unwrap();
    qm.process(&mut storage);
    let _ = qm.take_updates();

    {
        let sub = qm
            .subscriptions
            .get_mut(&sub_id)
            .expect("subscription should exist");
        sub.query = QueryBuilder::new("no_such_table").build();
        sub.needs_recompile = true;
    }

    qm.process(&mut storage);

    assert!(
        !qm.subscriptions.contains_key(&sub_id),
        "failed stale recompile should drop the local subscription"
    );

    let failures = qm.take_failed_subscriptions();
    assert_eq!(
        failures.len(),
        1,
        "expected exactly one reported local subscription failure"
    );
    assert_eq!(failures[0].subscription_id, sub_id);
    assert!(
        failures[0].reason.contains("no_such_table"),
        "failure reason should include compile context: {}",
        failures[0].reason
    );
}

#[test]
fn server_sends_error_for_uncompilable_query_subscription() {
    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, QueryId, Source, SyncError, SyncPayload,
    };
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    // Query references a table that does not exist in schema.
    let invalid_query = QueryBuilder::new("no_such_table").build();
    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(42),
            query: Box::new(invalid_query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let (code, reason) = outbox
        .iter()
        .find_map(|entry| match (&entry.destination, &entry.payload) {
            (
                Destination::Client(id),
                SyncPayload::Error(SyncError::QuerySubscriptionRejected {
                    query_id,
                    code,
                    reason,
                }),
            ) if *id == client_id && *query_id == QueryId(42) => {
                Some((code.clone(), reason.clone()))
            }
            _ => None,
        })
        .expect("Server should send an error payload when query subscription compilation fails");
    assert_eq!(code, "query_compilation_failed");
    assert!(
        reason.contains("query_id 42"),
        "error reason should include query id context: {reason}"
    );
    assert!(
        reason.contains("no_such_table"),
        "error reason should include compile error context: {reason}"
    );
}

#[test]
fn server_stale_recompile_failure_drops_subscription_and_notifies_client() {
    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, QueryId, ServerId, Source, SyncError, SyncPayload,
    };
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut server_qm, &storage, upstream_id);
    let _ = server_qm.sync_manager_mut().take_outbox();

    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    let valid_query = server_qm.query("users").build();
    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(7),
            query: Box::new(valid_query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    {
        let sub = server_qm
            .server_subscriptions
            .get_mut(&(client_id, QueryId(7)))
            .expect("server subscription should exist");
        sub.query = QueryBuilder::new("no_such_table").build();
        sub.needs_recompile = true;
    }

    server_qm.process(&mut storage);

    assert!(
        !server_qm
            .server_subscriptions
            .contains_key(&(client_id, QueryId(7))),
        "failed stale recompile should drop the server subscription"
    );
    assert!(
        !server_qm
            .sync_manager()
            .get_client(client_id)
            .expect("client should still exist")
            .queries
            .contains_key(&QueryId(7)),
        "client query scope should be cleared after fail-fast drop"
    );

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let (rejection_code, rejection_reason) = outbox
        .iter()
        .find_map(|entry| match (&entry.destination, &entry.payload) {
            (
                Destination::Client(id),
                SyncPayload::Error(SyncError::QuerySubscriptionRejected {
                    query_id,
                    code,
                    reason,
                }),
            ) if *id == client_id && *query_id == QueryId(7) => {
                Some((code.clone(), reason.clone()))
            }
            _ => None,
        })
        .expect("client should receive QuerySubscriptionRejected on stale recompile failure");
    assert_eq!(rejection_code, "query_recompile_failed");
    assert!(
        rejection_reason.contains("query recompilation failed for query_id 7"),
        "rejection should include query id context: {rejection_reason}"
    );
    assert!(
        rejection_reason.contains("no_such_table"),
        "rejection should include compile error context: {rejection_reason}"
    );

    assert!(
        outbox.iter().any(|entry| matches!(
            (&entry.destination, &entry.payload),
            (
                Destination::Server(id),
                SyncPayload::QueryUnsubscription { query_id }
            ) if *id == upstream_id && *query_id == QueryId(7)
        )),
        "stale recompile failure should forward QueryUnsubscription upstream"
    );
}

#[test]
fn server_pushes_new_matches() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Server has 1 user initially
    let _handle1 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Add client and subscribe
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    // Clear initial outbox
    let _ = server_qm.sync_manager_mut().take_outbox();

    // Insert new matching user
    let handle2 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Should send RowBatchNeeded for new matching user
    let outbox = server_qm.sync_manager_mut().take_outbox();

    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter_map(|e| match &e.payload {
            SyncPayload::RowBatchNeeded { row, .. } => Some(row.row_id),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_updates.len(),
        1,
        "Should send 1 RowBatchNeeded for new matching user"
    );

    assert_eq!(
        row_updates[0], handle2.row_id,
        "Should send Charlie's ObjectId"
    );
}

#[test]
fn server_subscription_telemetry_tracks_grouping_and_unsubscribe_lifecycle() {
    use crate::sync_manager::{
        ClientId, InboxEntry, QueryId, QueryPropagation, Source, SyncPayload,
    };

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let repeated_query = server_qm.query("users").build();
    let repeated_query_json = serde_json::to_string(&repeated_query).unwrap();
    let filtered_query = server_qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();

    let client_a = ClientId::new();
    let client_b = ClientId::new();
    let client_c = ClientId::new();
    for client_id in [client_a, client_b, client_c] {
        connect_client(&mut server_qm, &storage, client_id);
    }

    for (client_id, query_id, query, propagation) in [
        (
            client_a,
            QueryId(1),
            repeated_query.clone(),
            QueryPropagation::Full,
        ),
        (
            client_b,
            QueryId(2),
            repeated_query.clone(),
            QueryPropagation::Full,
        ),
        (
            client_c,
            QueryId(3),
            repeated_query.clone(),
            QueryPropagation::LocalOnly,
        ),
        (
            client_c,
            QueryId(4),
            filtered_query.clone(),
            QueryPropagation::Full,
        ),
    ] {
        server_qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id,
                query: Box::new(query),
                session: None,
                propagation,
                policy_context_tables: vec![],
            },
        });
    }

    server_qm.process(&mut storage);

    let telemetry = server_qm.server_subscription_telemetry();
    assert_eq!(telemetry.len(), 3);
    assert!(telemetry.iter().any(|group| {
        group.count == 2 && group.propagation == QueryPropagation::Full && group.table == "users"
    }));
    assert!(
        telemetry
            .iter()
            .any(|group| { group.count == 1 && group.propagation == QueryPropagation::LocalOnly })
    );
    assert!(
        telemetry
            .iter()
            .any(|group| { group.count == 1 && group.query.contains("\"name\"") })
    );

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_b),
        payload: SyncPayload::QueryUnsubscription {
            query_id: QueryId(2),
        },
    });
    server_qm.process(&mut storage);

    let telemetry_after_unsubscribe = server_qm.server_subscription_telemetry();
    assert!(telemetry_after_unsubscribe.iter().any(|group| {
        group.count == 1
            && group.propagation == QueryPropagation::Full
            && group.query == repeated_query_json
    }));
}

#[test]
fn server_does_not_push_non_matching() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Add client and subscribe to score > 50
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, client_id);

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    // Insert non-matching user (score = 30)
    let _handle = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Should NOT send RowBatchNeeded for non-matching user
    let outbox = server_qm.sync_manager_mut().take_outbox();

    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter(|e| matches!(e.payload, SyncPayload::RowBatchNeeded { .. }))
        .collect();

    assert_eq!(
        row_updates.len(),
        0,
        "Should NOT send RowBatchNeeded for non-matching user"
    );
}

// ============================================================================
// Client subscribe_with_sync Tests
// ============================================================================

#[test]
fn subscribe_with_sync_sends_to_servers() {
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    // Add a server
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client_qm, &_storage, server_id);

    // Clear initial outbox (full sync to server)
    let _ = client_qm.sync_manager_mut().take_outbox();

    // Subscribe with sync
    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let _sub_id = client_qm.subscribe_with_sync(query, None, None).unwrap();

    // Check outbox for QuerySubscription
    let outbox = client_qm.sync_manager_mut().take_outbox();

    let query_subs: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        query_subs.len(),
        1,
        "Should send QuerySubscription to server"
    );
}

#[test]
fn subscribe_with_sync_local_only_sends_to_connected_tier() {
    use crate::sync_manager::QueryPropagation;
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client_qm, &_storage, server_id);
    let _ = client_qm.sync_manager_mut().take_outbox();

    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let _sub_id = client_qm
        .subscribe_with_sync_and_propagation(query, None, None, QueryPropagation::LocalOnly)
        .unwrap();

    let outbox = client_qm.sync_manager_mut().take_outbox();
    let query_subs: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        query_subs.len(),
        1,
        "local-only subscription should be sent to connected tier"
    );
}

#[test]
fn subscribe_with_sync_local_only_on_persistence_tier_does_not_send_upstream() {
    use crate::sync_manager::QueryPropagation;
    use crate::sync_manager::{Destination, DurabilityTier, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let schema = test_schema();
    let (mut worker_qm, _storage) = create_query_manager(sync_manager, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut worker_qm, &_storage, server_id);
    let _ = worker_qm.sync_manager_mut().take_outbox();

    let query = worker_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let _sub_id = worker_qm
        .subscribe_with_sync_and_propagation(query, None, None, QueryPropagation::LocalOnly)
        .unwrap();

    let outbox = worker_qm.sync_manager_mut().take_outbox();
    let query_subs: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        query_subs.len(),
        0,
        "local-tier local-only subscription should not be sent to upstream sync server"
    );
}

#[test]
fn add_server_replays_existing_local_query_subscriptions() {
    use crate::sync_manager::{Destination, QueryId, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = client_qm.subscribe_with_sync(query, None, None).unwrap();

    // No server connected yet, so no outbound subscription forwarding.
    let outbox_before_add_server = client_qm.sync_manager_mut().take_outbox();
    assert!(
        outbox_before_add_server
            .iter()
            .all(|e| !matches!(e.payload, SyncPayload::QuerySubscription { .. })),
        "Should not forward QuerySubscription before any server is connected"
    );

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_query_manager_upstream(&mut client_qm, &_storage, server_id);

    let outbox = client_qm.sync_manager_mut().take_outbox();
    let replayed: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter_map(|e| {
            if let SyncPayload::QuerySubscription { query_id, .. } = &e.payload {
                Some(*query_id)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        replayed,
        vec![QueryId(sub_id.0)],
        "add_server should replay active local subscriptions to the new server"
    );
}

#[test]
fn add_server_replays_local_only_query_subscriptions() {
    use crate::sync_manager::QueryPropagation;
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let _sub_id = client_qm
        .subscribe_with_sync_and_propagation(query, None, None, QueryPropagation::LocalOnly)
        .unwrap();

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_query_manager_upstream(&mut client_qm, &_storage, server_id);
    let outbox = client_qm.sync_manager_mut().take_outbox();

    let replayed: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        replayed.len(),
        1,
        "add_server should replay local-only subscriptions"
    );
}

#[test]
fn unsubscribe_with_sync_sends_to_servers() {
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    // Add a server
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client_qm, &_storage, server_id);

    // Subscribe with sync
    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client_qm.subscribe_with_sync(query, None, None).unwrap();

    // Clear outbox
    let _ = client_qm.sync_manager_mut().take_outbox();

    // Unsubscribe with sync
    client_qm.unsubscribe_with_sync(sub_id);

    // Check outbox for QueryUnsubscription
    let outbox = client_qm.sync_manager_mut().take_outbox();

    let query_unsubs: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QueryUnsubscription { .. }))
        .collect();

    assert_eq!(
        query_unsubs.len(),
        1,
        "Should send QueryUnsubscription to server"
    );
}

// ============================================================================
// Part 5: Multi-Tier Forwarding Tests
// ============================================================================

/// Test that a mid-tier server forwards QuerySubscription to upstream servers.
#[test]
fn mid_tier_forwards_query_subscription_upstream() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncPayload};
    use uuid::Uuid;

    // Setup mid-tier server with schema
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    // Add upstream server
    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);

    // Add downstream client
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut mid_tier, &storage, client_id);

    // Clear the outbox (add_server queues full sync)
    let _ = mid_tier.sync_manager_mut().take_outbox();

    // Simulate receiving QuerySubscription from client
    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(42),
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    // Process the subscription
    mid_tier.process(&mut storage);

    // Check that QuerySubscription was forwarded to upstream server
    let outbox = mid_tier.sync_manager_mut().take_outbox();

    let forwarded: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == upstream_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        forwarded.len(),
        1,
        "Mid-tier should forward QuerySubscription to upstream server"
    );
}

#[test]
fn mid_tier_does_not_forward_local_only_query_subscription_upstream() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);

    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut mid_tier, &storage, client_id);
    let _ = mid_tier.sync_manager_mut().take_outbox();

    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(42),
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::LocalOnly,
            policy_context_tables: vec![],
        },
    });
    mid_tier.process(&mut storage);

    let outbox = mid_tier.sync_manager_mut().take_outbox();
    let forwarded: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == upstream_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        forwarded.len(),
        0,
        "Mid-tier should not forward local-only QuerySubscription upstream"
    );
}

#[test]
fn add_server_does_not_replay_downstream_local_only_query_subscription() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    let downstream_client = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut mid_tier, &storage, downstream_client);

    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(downstream_client),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(77),
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::LocalOnly,
            policy_context_tables: vec![],
        },
    });
    mid_tier.process(&mut storage);
    let _ = mid_tier.sync_manager_mut().take_outbox();

    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_query_manager_upstream(&mut mid_tier, &storage, upstream_id);

    let outbox = mid_tier.sync_manager_mut().take_outbox();
    let replayed: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == upstream_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        replayed.len(),
        0,
        "add_server should not replay downstream local-only subscriptions upstream"
    );
}

/// Test that a mid-tier server forwards QueryUnsubscription to upstream servers.
#[test]
fn mid_tier_forwards_query_unsubscription_upstream() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncPayload};
    use uuid::Uuid;

    // Setup mid-tier server
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    // Add upstream server and downstream client
    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);
    connect_client(&mut mid_tier, &storage, client_id);

    // First, establish subscription
    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let query_id = crate::sync_manager::QueryId(42);

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id,
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
    mid_tier.process(&mut storage);

    // Clear outbox
    let _ = mid_tier.sync_manager_mut().take_outbox();

    // Now send unsubscription
    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QueryUnsubscription { query_id },
    });
    mid_tier.process(&mut storage);

    // Check that QueryUnsubscription was forwarded upstream
    let outbox = mid_tier.sync_manager_mut().take_outbox();

    let forwarded: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == upstream_id))
        .filter(|e| matches!(e.payload, SyncPayload::QueryUnsubscription { .. }))
        .collect();

    assert_eq!(
        forwarded.len(),
        1,
        "Mid-tier should forward QueryUnsubscription to upstream server"
    );
}

/// Test that objects from upstream are relayed to downstream clients with matching scope.
#[test]
fn mid_tier_relays_objects_to_clients_with_matching_scope() {
    use crate::object::ObjectId;
    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, RowMetadata, ServerId, Source, SyncPayload,
    };
    use uuid::Uuid;

    // Setup mid-tier server
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema.clone());

    // Add upstream server and downstream client
    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);
    connect_client(&mut mid_tier, &storage, client_id);

    // Insert a matching row locally first (so it's in scope)
    let handle = mid_tier
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    mid_tier.process(&mut storage);

    // Get the schema branch
    let branch_str = get_branch(&mid_tier);

    // Establish client subscription
    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(42),
            query: Box::new(query),
            session: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
    mid_tier.process(&mut storage);

    // Clear outbox (initial sync messages)
    let _ = mid_tier.sync_manager_mut().take_outbox();

    // Now receive an update for the existing object from upstream
    // (simulating upstream sending fresh data)
    let table_schema = schema.get(&TableName::new("users")).unwrap();
    let row_data = encode_row(
        &table_schema.columns,
        &[Value::Text("Alice".into()), Value::Integer(80)],
    )
    .unwrap();

    let author = ObjectId::new();
    let base_timestamp = load_visible_row(&storage, handle.row_id, &branch_str).updated_at;
    let commit = stored_row_commit(
        smallvec![handle.batch_id],
        row_data,
        base_timestamp + 1,
        author.to_string(),
    );
    let row = commit.to_row(handle.row_id, &branch_str, RowState::VisibleDirect);

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(upstream_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: handle.row_id,
                metadata: [("table".to_string(), "users".to_string())]
                    .into_iter()
                    .collect(),
            }),
            row,
        },
    });
    mid_tier.process(&mut storage);

    // Check that the update was forwarded to the client
    let outbox = mid_tier.sync_manager_mut().take_outbox();

    let relayed: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter(|e| match &e.payload {
            SyncPayload::RowBatchNeeded { row, .. } => row.row_id == handle.row_id,
            _ => false,
        })
        .collect();

    assert_eq!(
        relayed.len(),
        1,
        "Mid-tier should relay matching row batch entries downstream"
    );
}

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

/// E2E: Client subscribes to query, receives matching data from server.
#[test]
fn e2e_client_receives_server_data_via_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    // Setup server with data
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(90)],
        )
        .unwrap();
    server.process(&mut server_io);

    // Setup client (no data yet)
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Subscribe to all object updates (needed to receive sync'd data)

    // Connect client to server
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);

    // Clear initial sync messages (we want query-driven sync)
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes to users with score > 50 (should match Alice and Charlie)
    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    // Exchange messages between client and server
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should now have the matching rows
    let results = client.get_subscription_results(sub_id);

    assert_eq!(results.len(), 2, "Client should receive 2 matching users");

    let names: Vec<_> = results
        .iter()
        .filter_map(|(_, row)| {
            if let Value::Text(name) = &row[0] {
                Some(name.as_str())
            } else {
                None
            }
        })
        .collect();

    assert!(names.contains(&"Alice"), "Should contain Alice");
    assert!(names.contains(&"Charlie"), "Should contain Charlie");
    assert!(!names.contains(&"Bob"), "Should NOT contain Bob");
}

/// E2E: Client can cold-load a paginated remote query with a non-zero offset.
#[test]
fn e2e_client_receives_paginated_server_data_on_cold_offset_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    for (name, score) in [("A", 1), ("B", 2), ("C", 3), ("D", 4)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let query = client
        .query("users")
        .order_by("score")
        .offset(2)
        .limit(1)
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "Cold paginated subscription should materialize the requested page"
    );
    assert_eq!(results[0].1[0], Value::Text("C".into()));
    assert_eq!(results[0].1[1], Value::Integer(3));
}

#[test]
fn e2e_client_receives_paginated_server_data_on_cold_offset_only_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    for (name, score) in [("A", 1), ("B", 2), ("C", 3), ("D", 4)] {
        server
            .insert(
                &mut server_io,
                "users",
                &[Value::Text(name.into()), Value::Integer(score)],
            )
            .unwrap();
    }
    server.process(&mut server_io);

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let query = client.query("users").order_by("score").offset(2).build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        2,
        "Offset-only query should keep trailing rows"
    );
    assert_eq!(results[0].1[0], Value::Text("C".into()));
    assert_eq!(results[1].1[0], Value::Text("D".into()));
}

#[test]
fn e2e_client_receives_array_subquery_server_data_via_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = users_posts_schema();

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Post 1".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Post 2".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
    server.process(&mut server_io);

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let query = client
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1, "Client should receive the user row");
    assert_eq!(results[0].1[0], Value::Integer(1));
    assert_eq!(results[0].1[1], Value::Text("Alice".into()));

    let posts = results[0].1[2].as_array().expect("posts array");
    assert_eq!(
        posts.len(),
        2,
        "Client should receive inner rows needed for the array"
    );
}

#[test]
fn e2e_client_receives_recursive_hop_server_data_via_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = recursive_hop_team_schema();

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    let team1 = server
        .insert(&mut server_io, "teams", &[Value::Text("team-1".into())])
        .unwrap();
    let team2 = server
        .insert(&mut server_io, "teams", &[Value::Text("team-2".into())])
        .unwrap();
    let team3 = server
        .insert(&mut server_io, "teams", &[Value::Text("team-3".into())])
        .unwrap();
    server
        .insert(
            &mut server_io,
            "team_edges",
            &[Value::Uuid(team1.row_id), Value::Uuid(team2.row_id)],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "team_edges",
            &[Value::Uuid(team2.row_id), Value::Uuid(team3.row_id)],
        )
        .unwrap();
    server.process(&mut server_io);

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let query = client
        .query("teams")
        .filter_eq("name", Value::Text("team-1".into()))
        .with_recursive(|r| {
            r.from("team_edges")
                .correlate("child_team", "_id")
                .select(&["parent_team"])
                .hop("teams", "parent_team")
                .max_depth(10)
        })
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let mut names: Vec<String> = client
        .get_subscription_results(sub_id)
        .into_iter()
        .filter_map(|(_, values)| match values.first() {
            Some(Value::Text(name)) => Some(name.clone()),
            _ => None,
        })
        .collect();
    names.sort();

    assert_eq!(
        names,
        vec!["team-1", "team-2", "team-3"],
        "Client should receive recursive dependencies needed to replay the closure"
    );
}

/// E2E: Client receives new matching rows as server inserts them.
#[test]
fn e2e_client_receives_new_matching_row() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    // Setup server (initially empty)
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    // Setup client
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes to users with score > 50
    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    // Initial sync (empty)
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );
    assert_eq!(
        client.get_subscription_results(sub_id).len(),
        0,
        "Initially empty"
    );

    // Server inserts a matching row
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    server.process(&mut server_io);

    // Exchange messages
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should now have Alice
    let results = client.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1, "Client should receive new matching user");
    assert_eq!(results[0].1[0], Value::Text("Alice".into()));
}

/// E2E: Client does NOT receive rows that don't match the query filter.
#[test]
fn e2e_client_does_not_receive_non_matching_row() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    // Setup server and client
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes to users with score > 50
    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Server inserts a NON-matching row (score = 30)
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    server.process(&mut server_io);

    // Exchange messages
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should NOT have Bob
    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        0,
        "Client should NOT receive non-matching user"
    );
}

/// E2E: Permissions filter what gets synced - client only receives permitted rows.
#[test]
fn e2e_permissions_prevent_sync() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    // Create schema with documents table that has owner-based policy
    // Policy: owner_id must match session.user_id
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Text), // Text to match user_id string
            ]),
            policies: TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        },
    );

    // Setup server with docs owned by different users
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    server
        .insert(
            &mut server_io,
            "documents",
            &[
                Value::Text("Alice's doc".into()),
                Value::Text("alice".into()),
            ],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "documents",
            &[Value::Text("Bob's doc".into()), Value::Text("bob".into())],
        )
        .unwrap();
    server.process(&mut server_io);

    // Setup client
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes as Alice (user_id = "alice")
    let alice_session = PolicySession::new("alice");
    let query = client.query("documents").build();

    let sub_id = client
        .subscribe_with_sync(query, Some(alice_session), None)
        .unwrap();

    // Exchange messages
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should ONLY have Alice's doc
    let results = client.get_subscription_results(sub_id);

    assert_eq!(
        results.len(),
        1,
        "Client should only receive docs they have permission to see"
    );
    assert_eq!(
        results[0].1[0],
        Value::Text("Alice's doc".into()),
        "Should be Alice's doc"
    );
}

/// E2E: New rows that don't match permissions are NOT synced.
#[test]
fn e2e_permissions_prevent_new_row_sync() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    // Create schema with owner-based policy
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Text), // Text to match user_id string
            ]),
            policies: TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        },
    );

    // Setup server and client
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes as Alice
    let alice_session = PolicySession::new("alice");
    let query = client.query("documents").build();

    let sub_id = client
        .subscribe_with_sync(query, Some(alice_session), None)
        .unwrap();
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Server inserts Alice's doc
    server
        .insert(
            &mut server_io,
            "documents",
            &[
                Value::Text("Alice's doc".into()),
                Value::Text("alice".into()),
            ],
        )
        .unwrap();
    server.process(&mut server_io);
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    assert_eq!(
        client.get_subscription_results(sub_id).len(),
        1,
        "Client should have Alice's doc"
    );

    // Server inserts Bob's doc (owner_id = "bob")
    server
        .insert(
            &mut server_io,
            "documents",
            &[Value::Text("Bob's doc".into()), Value::Text("bob".into())],
        )
        .unwrap();
    server.process(&mut server_io);
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should still only have Alice's doc
    let results = client.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1, "Client should NOT receive Bob's doc");
    assert_eq!(results[0].1[0], Value::Text("Alice's doc".into()));
}

#[test]
fn local_subscription_does_not_filter_rows_without_remote_scope() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.process(&mut storage);

    let sub_id = qm.subscribe(qm.query("users").build()).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "plain local subscriptions should ignore remote scope"
    );
    assert_eq!(
        results[0].1,
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn sync_backed_subscription_without_remote_scope_snapshot_keeps_local_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.process(&mut storage);

    let sub_id = qm
        .subscribe_with_sync(
            qm.query("users").build(),
            None,
            Some(crate::sync_manager::DurabilityTier::Local),
        )
        .unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed subscriptions should keep local rows until a remote scope snapshot arrives"
    );
    assert_eq!(
        results[0].1,
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn sync_backed_subscription_holds_initial_delivery_without_reloading_on_tier_confirmations() {
    use crate::sync_manager::{InboxEntry, QueryId, ServerId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema, "dev", "main");
    let mut storage = CountingQueryRowLoadsStorage::new(&qm.schema_context().current_schema);

    let inserted = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    qm.process(&mut storage);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    qm.add_server_with_storage(&storage, server_id, false);

    let sub_id = qm
        .subscribe_with_sync(
            qm.query("users").build(),
            None,
            Some(crate::sync_manager::DurabilityTier::Local),
        )
        .unwrap();
    qm.process(&mut storage);
    let initial_query_row_loads = storage.query_row_loads();

    let subscription = qm
        .subscriptions
        .get(&sub_id)
        .expect("subscription should exist");
    assert!(
        !subscription.query_frontier_complete,
        "subscription should still be waiting on upstream QuerySettled"
    );
    assert!(
        !subscription.settled_once,
        "subscription should keep the first delivery held until QuerySettled arrives"
    );
    assert!(
        !subscription.graph.has_dirty_nodes(),
        "initial hold should still keep the graph settled"
    );

    for confirmed_tier in [
        crate::sync_manager::DurabilityTier::EdgeServer,
        crate::sync_manager::DurabilityTier::GlobalServer,
    ] {
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(server_id),
            payload: SyncPayload::RowBatchStateChanged {
                row_id: inserted.row_id,
                branch_name: BranchName::new("main"),
                batch_id: inserted.batch_id,
                state: None,
                confirmed_tier: Some(confirmed_tier),
            },
        });
        qm.process(&mut storage);
    }

    assert_eq!(
        storage.query_row_loads(),
        initial_query_row_loads,
        "upstream tier confirmations should not keep reloading rows whose visibility is unchanged"
    );

    let subscription = qm
        .subscriptions
        .get(&sub_id)
        .expect("subscription should exist");
    assert!(
        !subscription.graph.has_dirty_nodes(),
        "confirmations whose visibility is unchanged should not dirty the graph"
    );
    assert!(
        !subscription.settled_once,
        "confirmations should not release the held initial snapshot"
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::QuerySettled {
            query_id: QueryId(sub_id.0),
            tier: crate::sync_manager::DurabilityTier::Local,
            through_seq: 0,
        },
    });
    qm.process(&mut storage);

    let subscription = qm
        .subscriptions
        .get(&sub_id)
        .expect("subscription should exist");
    assert!(
        subscription.query_frontier_complete,
        "QuerySettled should release the waiting frontier"
    );
    assert!(
        subscription.settled_once,
        "frontier completion should allow the held initial delivery"
    );
    assert!(
        !subscription.graph.has_dirty_nodes(),
        "frontier completion should leave the graph settled"
    );

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].1,
        vec![Value::Text("Alice".into()), Value::Integer(100)]
    );
}

#[test]
fn sync_backed_session_subscription_keeps_local_rows_when_server_scope_is_empty() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Text),
            ]),
            policies: TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        },
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    client
        .insert(
            &mut client_io,
            "documents",
            &[
                Value::Text("Alice's doc".into()),
                Value::Text("alice".into()),
            ],
        )
        .unwrap();
    client.process(&mut client_io);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("documents").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::Local),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed session subscriptions should keep local rows even when the server scope is empty"
    );
    assert_eq!(
        results[0].1,
        vec![
            Value::Text("Alice's doc".into()),
            Value::Text("alice".into())
        ]
    );
}

#[test]
fn sync_backed_exists_rel_session_subscription_keeps_local_rows_when_server_scope_is_empty() {
    use crate::query_manager::relation_ir::{
        ColumnRef, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef, ValueRef,
    };
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            policies: TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                rel: RelExpr::Filter {
                    input: Box::new(RelExpr::TableScan {
                        table: TableName::new("user_team_edges"),
                    }),
                    predicate: PredicateExpr::And(vec![
                        PredicateExpr::Cmp {
                            left: ColumnRef::unscoped("team_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::RowId(RowIdRef::Outer),
                        },
                        PredicateExpr::Cmp {
                            left: ColumnRef::unscoped("user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                    ]),
                },
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let team_row = client
        .insert(&mut client_io, "teams", &[Value::Text("Alice".into())])
        .unwrap();
    client
        .insert(
            &mut client_io,
            "user_team_edges",
            &[Value::Text("alice".into()), Value::Uuid(team_row.row_id)],
        )
        .unwrap();
    client.process(&mut client_io);

    let local_sub_id = client
        .subscribe_with_session(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);
    assert_eq!(
        client.get_subscription_results(local_sub_id).len(),
        1,
        "local session subscriptions should already see the related-row grant"
    );

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed EXISTS policies should keep local rows even when the server scope is empty"
    );
    assert_eq!(results[0].1, vec![Value::Text("Alice".into())]);
}

#[test]
fn sync_backed_exists_session_subscription_keeps_local_rows_when_server_scope_is_empty() {
    use crate::query_manager::policy::{CmpOp, OUTER_ROW_SESSION_PREFIX, PolicyValue};
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            policies: TablePolicies::new().with_select(PolicyExpr::Exists {
                table: "user_team_edges".into(),
                condition: Box::new(PolicyExpr::And(vec![
                    PolicyExpr::Cmp {
                        column: "team_id".into(),
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(vec![
                            OUTER_ROW_SESSION_PREFIX.into(),
                            "id".into(),
                        ]),
                    },
                    PolicyExpr::eq_session("user_id", vec!["user_id".into()]),
                ])),
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let team_row = client
        .insert(&mut client_io, "teams", &[Value::Text("Alice".into())])
        .unwrap();
    client
        .insert(
            &mut client_io,
            "user_team_edges",
            &[Value::Text("alice".into()), Value::Uuid(team_row.row_id)],
        )
        .unwrap();
    client.process(&mut client_io);

    let local_sub_id = client
        .subscribe_with_session(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);
    assert_eq!(
        client.get_subscription_results(local_sub_id).len(),
        1,
        "local session subscriptions should already see the correlated EXISTS grant"
    );

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed EXISTS policies should keep local rows even when the server scope is empty"
    );
    assert_eq!(results[0].1, vec![Value::Text("Alice".into())]);
}

#[test]
fn sync_backed_joined_exists_rel_session_subscription_keeps_local_rows_when_server_scope_is_empty()
{
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef,
        ValueRef,
    };
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            policies: TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                rel: RelExpr::Filter {
                    input: Box::new(RelExpr::Join {
                        left: Box::new(RelExpr::TableScan {
                            table: TableName::new("user_team_edges"),
                        }),
                        right: Box::new(RelExpr::TableScan {
                            table: TableName::new("teams"),
                        }),
                        on: vec![JoinCondition {
                            left: ColumnRef::scoped("user_team_edges", "team_id"),
                            right: ColumnRef::scoped("__join_0", "id"),
                        }],
                        join_kind: JoinKind::Inner,
                    }),
                    predicate: PredicateExpr::And(vec![
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("user_team_edges", "user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("__join_0", "id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::RowId(RowIdRef::Outer),
                        },
                    ]),
                },
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let team_row = client
        .insert(&mut client_io, "teams", &[Value::Text("Alice".into())])
        .unwrap();
    client
        .insert(
            &mut client_io,
            "user_team_edges",
            &[Value::Text("alice".into()), Value::Uuid(team_row.row_id)],
        )
        .unwrap();
    client.process(&mut client_io);

    let local_sub_id = client
        .subscribe_with_session(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);
    assert_eq!(
        client.get_subscription_results(local_sub_id).len(),
        1,
        "local session subscriptions should already see the joined EXISTS_REL grant"
    );

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed joined EXISTS_REL policies should keep local rows even when the server scope is empty"
    );
    assert_eq!(results[0].1, vec![Value::Text("Alice".into())]);
}

#[test]
fn fail_closed_server_withholds_session_scope_before_permissions_head() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef,
        ValueRef,
    };
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            policies: TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                rel: RelExpr::Filter {
                    input: Box::new(RelExpr::Join {
                        left: Box::new(RelExpr::TableScan {
                            table: TableName::new("user_team_edges"),
                        }),
                        right: Box::new(RelExpr::TableScan {
                            table: TableName::new("teams"),
                        }),
                        on: vec![JoinCondition {
                            left: ColumnRef::scoped("user_team_edges", "team_id"),
                            right: ColumnRef::scoped("__join_0", "id"),
                        }],
                        join_kind: JoinKind::Inner,
                    }),
                    predicate: PredicateExpr::And(vec![
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("user_team_edges", "user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("__join_0", "id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::RowId(RowIdRef::Outer),
                        },
                    ]),
                },
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let mut structural_server_schema = Schema::new();
    structural_server_schema.insert(
        TableName::new("teams"),
        TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
            "name",
            ColumnType::Text,
        )])),
    );
    structural_server_schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, structural_server_schema);
    server.require_authorization_schema();
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let team_row = client
        .insert(&mut client_io, "teams", &[Value::Text("Alice".into())])
        .unwrap();
    let edge_row = client
        .insert(
            &mut client_io,
            "user_team_edges",
            &[Value::Text("alice".into()), Value::Uuid(team_row.row_id)],
        )
        .unwrap();
    client.process(&mut client_io);

    client.clear_local_pending_row_overlay("teams", team_row.row_id);
    client.clear_local_pending_row_overlay("user_team_edges", edge_row.row_id);
    client.process(&mut client_io);

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    assert!(
        !client
            .sync_manager()
            .has_remote_query_scope_snapshot(crate::sync_manager::QueryId(sub_id.0)),
        "server without a published permissions head should not advertise an authoritative remote scope"
    );

    let failures = client.take_failed_subscriptions();
    assert!(
        failures.is_empty(),
        "missing permissions head should not surface an explicit subscription failure yet: {failures:?}"
    );
}

#[test]
fn synced_session_query_for_exists_rel_sends_policy_context_tables_upstream() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef,
        ValueRef,
    };
    use crate::sync_manager::DurabilityTier;
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            policies: TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                rel: RelExpr::Filter {
                    input: Box::new(RelExpr::Join {
                        left: Box::new(RelExpr::TableScan {
                            table: TableName::new("user_team_edges"),
                        }),
                        right: Box::new(RelExpr::TableScan {
                            table: TableName::new("teams"),
                        }),
                        on: vec![JoinCondition {
                            left: ColumnRef::scoped("user_team_edges", "team_id"),
                            right: ColumnRef::scoped("__join_0", "id"),
                        }],
                        join_kind: JoinKind::Inner,
                    }),
                    predicate: PredicateExpr::And(vec![
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("user_team_edges", "user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("__join_0", "id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::RowId(RowIdRef::Outer),
                        },
                    ]),
                },
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let sync_manager = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let (mut qm, storage) = create_query_manager(sync_manager, schema);
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut qm, &storage, server_id);
    let _ = qm.sync_manager_mut().take_outbox();

    qm.subscribe_with_sync(
        qm.query("teams").build(),
        Some(PolicySession::new("alice")),
        Some(DurabilityTier::EdgeServer),
    )
    .unwrap();

    let outbox = qm.sync_manager_mut().take_outbox();
    let forwarded = outbox
        .into_iter()
        .find_map(|entry| match entry.payload {
            SyncPayload::QuerySubscription {
                policy_context_tables,
                ..
            } => Some(policy_context_tables),
            _ => None,
        })
        .expect("subscription should be forwarded upstream");

    assert!(
        forwarded.iter().any(|table| table == "user_team_edges"),
        "EXISTS_REL subscriptions should declare their policy-context tables upstream"
    );
}

#[test]
fn backend_sync_subscription_without_handshake_session_stays_unscoped_without_permissions_head() {
    use crate::sync_manager::ClientRole;
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("identity_key", ColumnType::Text).nullable(),
            ]),
            policies: TablePolicies::new().with_select(PolicyExpr::eq_session(
                "identity_key",
                vec!["user_id".into()],
            )),
        },
    );

    let mut structural_server_schema = Schema::new();
    structural_server_schema.insert(
        TableName::new("teams"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("identity_key", ColumnType::Text).nullable(),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, structural_server_schema);
    server.require_authorization_schema();
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    server
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Backend);
    let _ = client.sync_manager_mut().take_outbox();

    let team_row = client
        .insert(
            &mut client_io,
            "teams",
            &[Value::Text("Bob".into()), Value::Text("bob".into())],
        )
        .unwrap();
    client.process(&mut client_io);
    client.clear_local_pending_row_overlay("teams", team_row.row_id);
    client.process(&mut client_io);

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("bob")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    assert!(
        !client
            .sync_manager()
            .has_remote_query_scope_snapshot(crate::sync_manager::QueryId(sub_id.0)),
        "backend-authenticated clients without a handshake session should not receive an authoritative remote scope"
    );

    let failures = client.take_failed_subscriptions();
    assert!(
        failures.is_empty(),
        "missing permissions head should not surface an explicit subscription failure yet: {failures:?}"
    );
}

#[test]
fn synced_subscription_filters_rows_removed_from_remote_scope() {
    use crate::query_manager::policy::Operation;
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("recursive_folders"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("parent_id", ColumnType::Uuid)
                    .nullable()
                    .references("recursive_folders"),
            ]),
            TablePolicies::new().with_select(PolicyExpr::or(vec![
                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                PolicyExpr::and(vec![
                    PolicyExpr::IsNotNull {
                        column: "parent_id".into(),
                    },
                    PolicyExpr::inherits(Operation::Select, "parent_id"),
                ]),
            ])),
        ),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let root_id = server
        .insert(
            &mut server_io,
            "recursive_folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let child_id = server
        .insert(
            &mut server_io,
            "recursive_folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let _grand_id = server
        .insert(
            &mut server_io,
            "recursive_folders",
            &[
                Value::Text("carol".into()),
                Value::Text("Grand".into()),
                Value::Uuid(child_id),
            ],
        )
        .unwrap()
        .row_id;
    server.process(&mut server_io);

    let sub_id = client
        .subscribe_with_sync(
            client.query("recursive_folders").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );
    assert_eq!(client.get_subscription_results(sub_id).len(), 1);

    server
        .update(
            &mut server_io,
            child_id,
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root_id),
            ],
        )
        .unwrap();
    server.process(&mut server_io);
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );
    assert_eq!(client.get_subscription_results(sub_id).len(), 3);

    server
        .update(
            &mut server_io,
            child_id,
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Null,
            ],
        )
        .unwrap();
    server.process(&mut server_io);
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let remote_scope = client
        .sync_manager()
        .remote_query_scope(crate::sync_manager::QueryId(sub_id.0));
    assert_eq!(
        remote_scope,
        [(root_id, crate::object::BranchName::new(get_branch(&client)))]
            .into_iter()
            .collect()
    );

    let subscription = client
        .subscriptions
        .get(&sub_id)
        .expect("client subscription");
    assert_eq!(subscription.current_ordered_ids, vec![root_id]);
    assert_eq!(subscription.current_visible_rows.len(), 1);
    assert_eq!(
        decode_row(
            &subscription.graph.combined_descriptor,
            &subscription.current_visible_rows[&root_id].data
        )
        .unwrap(),
        vec![
            Value::Text("alice".into()),
            Value::Text("Root".into()),
            Value::Null
        ]
    );
}

/// E2E: In a 3-tier topology, upstream must sync policy-evaluation dependencies.
///
/// Scenario:
/// - documents SELECT policy: owner_id = @session.user_id OR INHERITS SELECT VIA folder_id
/// - core has folder(owner=alice) and document(owner=bob, folder=alice_folder)
/// - alice queries documents from downstream client via edge
///
/// Expected:
/// - core deems the document visible (via INHERITS)
/// - edge must receive enough rows to re-evaluate the same policy and relay to client
#[test]
fn e2e_three_tier_policy_dependencies_must_sync_downstream() {
    use crate::query_manager::policy::Operation;
    use crate::sync_manager::{ClientId, ClientRole, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("name", ColumnType::Text),
            ]),
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ),
    );
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("folder_id", ColumnType::Uuid)
                    .nullable()
                    .references("folders"),
            ]),
            TablePolicies::new().with_select(PolicyExpr::or(vec![
                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                PolicyExpr::inherits(Operation::Select, "folder_id"),
            ])),
        ),
    );

    // Core (upstream) has data.
    let (mut core, mut core_io) = create_query_manager(SyncManager::new(), schema.clone());
    let folder_id = core
        .insert(
            &mut core_io,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Alice folder".into()),
            ],
        )
        .unwrap()
        .row_id;
    core.insert(
        &mut core_io,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("Bob doc in Alice folder".into()),
            Value::Uuid(folder_id),
        ],
    )
    .unwrap();
    core.process(&mut core_io);

    // Edge (mid-tier) starts empty.
    let (mut edge, mut edge_io) = create_query_manager(SyncManager::new(), schema.clone());

    // Downstream client starts empty.
    let (mut client, mut client_io) = create_query_manager(SyncManager::new(), schema.clone());

    // Topology: client <-> edge <-> core
    let edge_server_id_for_client = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id_on_edge = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let core_server_id_for_edge = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let edge_id_on_core = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    client
        .sync_manager_mut()
        .add_server_with_storage(edge_server_id_for_client, false, &client_io);
    connect_client(&mut edge, &edge_io, client_id_on_edge);
    edge.sync_manager_mut()
        .set_client_role(client_id_on_edge, ClientRole::Peer);
    connect_server(&mut edge, &edge_io, core_server_id_for_edge);
    connect_client(&mut core, &core_io, edge_id_on_core);
    core.sync_manager_mut()
        .set_client_role(edge_id_on_core, ClientRole::Peer);

    // Clear non-query bootstrap traffic.
    let _ = client.sync_manager_mut().take_outbox();
    let _ = edge.sync_manager_mut().take_outbox();
    let _ = core.sync_manager_mut().take_outbox();

    // Client subscribes as alice.
    let sub_id = client
        .subscribe_with_sync(
            client.query("documents").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);

    pump_messages_three_tier(
        &mut client,
        &mut edge,
        &mut core,
        &mut client_io,
        &mut edge_io,
        &mut core_io,
        client_id_on_edge,
        edge_server_id_for_client,
        edge_id_on_core,
        core_server_id_for_edge,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "Client should receive docs visible via INHERITS through edge in 3-tier sync"
    );
}

/// E2E: Untrusted downstream clients keep result-set-only scope (no policy context rows).
#[test]
fn e2e_three_tier_untrusted_downstream_keeps_result_only_scope() {
    use crate::query_manager::policy::Operation;
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("name", ColumnType::Text),
            ]),
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ),
    );
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("folder_id", ColumnType::Uuid)
                    .nullable()
                    .references("folders"),
            ]),
            TablePolicies::new().with_select(PolicyExpr::or(vec![
                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                PolicyExpr::inherits(Operation::Select, "folder_id"),
            ])),
        ),
    );

    let (mut core, mut core_io) = create_query_manager(SyncManager::new(), schema.clone());
    let folder_id = core
        .insert(
            &mut core_io,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Alice folder".into()),
            ],
        )
        .unwrap()
        .row_id;
    core.insert(
        &mut core_io,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("Bob doc in Alice folder".into()),
            Value::Uuid(folder_id),
        ],
    )
    .unwrap();
    core.process(&mut core_io);

    let (mut edge, mut edge_io) = create_query_manager(SyncManager::new(), schema.clone());
    let (mut client, mut client_io) = create_query_manager(SyncManager::new(), schema.clone());

    let edge_server_id_for_client = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id_on_edge = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let core_server_id_for_edge = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let edge_id_on_core = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    client
        .sync_manager_mut()
        .add_server_with_storage(edge_server_id_for_client, false, &client_io);
    connect_client(&mut edge, &edge_io, client_id_on_edge);
    connect_server(&mut edge, &edge_io, core_server_id_for_edge);
    connect_client(&mut core, &core_io, edge_id_on_core);

    let _ = client.sync_manager_mut().take_outbox();
    let _ = edge.sync_manager_mut().take_outbox();
    let _ = core.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("documents").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);

    pump_messages_three_tier(
        &mut client,
        &mut edge,
        &mut core,
        &mut client_io,
        &mut edge_io,
        &mut core_io,
        client_id_on_edge,
        edge_server_id_for_client,
        edge_id_on_core,
        core_server_id_for_edge,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        0,
        "Untrusted downstream should keep current result-only sync behavior"
    );
}

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

#[test]
fn remove_client_cleans_up_server_subscriptions() {
    //
    // alice (client) ──subscribes──▶ server
    //
    // alice subscribes to two queries, then disconnects.
    // server_subscriptions should be empty after remove_client.
    //
    use crate::sync_manager::{ClientId, QueryId};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, alice);

    let query = server_qm.query("users").build();
    push_query_subscription(&mut server_qm, alice, 1, query.clone());
    push_query_subscription(&mut server_qm, alice, 2, query);
    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    assert!(
        server_qm
            .server_subscriptions
            .contains_key(&(alice, QueryId(1)))
    );
    assert!(
        server_qm
            .server_subscriptions
            .contains_key(&(alice, QueryId(2)))
    );

    server_qm.remove_client(alice);

    assert!(
        server_qm.server_subscriptions.is_empty(),
        "server_subscriptions should be empty after client disconnect"
    );
    assert!(
        server_qm.sync_manager().get_client(alice).is_none(),
        "client should be removed from SyncManager"
    );
}

#[test]
fn remove_client_preserves_other_clients_subscriptions() {
    //
    // alice (client) ──subscribes──▶ server ◀──subscribes── bob (client)
    //
    // Both subscribe, then alice disconnects.
    // bob's subscription should survive.
    //
    use crate::sync_manager::{ClientId, QueryId};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let bob = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, alice);
    connect_client(&mut server_qm, &storage, bob);

    let query = server_qm.query("users").build();
    push_query_subscription(&mut server_qm, alice, 1, query.clone());
    push_query_subscription(&mut server_qm, bob, 1, query);
    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    assert_eq!(server_qm.server_subscriptions.len(), 2);

    server_qm.remove_client(alice);

    assert_eq!(
        server_qm.server_subscriptions.len(),
        1,
        "only bob's subscription should remain"
    );
    assert!(
        server_qm
            .server_subscriptions
            .contains_key(&(bob, QueryId(1)))
    );
    assert!(server_qm.sync_manager().get_client(bob).is_some());
}

#[test]
fn remove_client_cleans_active_policy_checks() {
    //
    // alice ──write──▶ server (policy check in-flight)
    // bob   ──write──▶ server (policy check in-flight)
    //
    // alice disconnects → only bob's policy check remains.
    //
    use crate::query_manager::policy::Operation;
    use crate::sync_manager::{ClientId, PendingPermissionCheck, PendingUpdateId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, _storage) = create_query_manager(sync_manager, schema);

    let alice = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let bob = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &_storage, alice);
    connect_client(&mut server_qm, &_storage, bob);

    let obj_id = crate::object::ObjectId::new();
    let make_check = |id: u64, client_id: ClientId| PendingPermissionCheck {
        id: PendingUpdateId(id),
        client_id,
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(crate::sync_manager::RowMetadata {
                id: obj_id,
                metadata: HashMap::from([(MetadataKey::Table.to_string(), "users".to_string())]),
            }),
            row: StoredRowBatch::new(
                obj_id,
                "main",
                Vec::new(),
                b"alice".to_vec(),
                RowProvenance::for_insert(obj_id.to_string(), 1_000),
                HashMap::new(),
                RowState::VisibleDirect,
                None,
            ),
        },
        session: crate::query_manager::session::Session {
            user_id: format!("{client_id}"),
            claims: serde_json::Value::Null,
            auth_mode: Default::default(),
        },
        schema_wait_started_at: None,
        metadata: Default::default(),
        old_content: None,
        new_content: None,
        operation: Operation::Insert,
    };

    // Insert policy checks directly into the active_policy_checks map
    use crate::query_manager::manager::PolicyCheckState;
    server_qm.active_policy_checks.insert(
        PendingUpdateId(1),
        PolicyCheckState {
            graphs: vec![],
            table: "users".into(),
            branch: crate::object::BranchName::new("main"),
            pending_check: make_check(1, alice),
        },
    );
    server_qm.active_policy_checks.insert(
        PendingUpdateId(2),
        PolicyCheckState {
            graphs: vec![],
            table: "users".into(),
            branch: crate::object::BranchName::new("main"),
            pending_check: make_check(2, bob),
        },
    );

    assert_eq!(server_qm.active_policy_checks.len(), 2);

    server_qm.remove_client(alice);

    assert_eq!(
        server_qm.active_policy_checks.len(),
        1,
        "only bob's policy check should remain"
    );
    assert!(
        server_qm
            .active_policy_checks
            .contains_key(&PendingUpdateId(2))
    );
}

#[test]
fn remove_client_is_idempotent() {
    //
    // Calling remove_client twice should not panic or corrupt state.
    //
    use crate::sync_manager::ClientId;
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, alice);

    let query = server_qm.query("users").build();
    push_query_subscription(&mut server_qm, alice, 1, query);
    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    server_qm.remove_client(alice);
    server_qm.remove_client(alice); // second call — should be a no-op

    assert!(server_qm.server_subscriptions.is_empty());
    assert!(server_qm.sync_manager().get_client(alice).is_none());
}

#[test]
fn anonymous_insert_is_denied_before_policy_eval() {
    // Even when the schema has an allow-all insert policy, an anonymous session
    // must be rejected with AnonymousWriteDenied before policy evaluation runs.
    use crate::query_manager::policy::Operation;
    use crate::query_manager::session::AuthMode;

    let mut schema = Schema::new();
    let mut table_schema = TableSchema::new(RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]));
    // Allow-all insert policy — anonymous session must still be denied structurally.
    table_schema.policies = TablePolicies::new().with_insert(PolicyExpr::True);
    schema.insert(TableName::new("users"), table_schema);

    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let anon = PolicySession::new("anon-user").with_auth_mode(AuthMode::Anonymous);

    let err = qm
        .insert_with_session(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(42)],
            Some(&anon),
        )
        .expect_err("anonymous insert must be denied");

    assert_eq!(
        err,
        QueryError::AnonymousWriteDenied {
            table: TableName::new("users"),
            operation: Operation::Insert,
        }
    );
}

#[test]
fn anonymous_update_is_denied_before_policy_eval() {
    use crate::query_manager::policy::Operation;
    use crate::query_manager::session::AuthMode;

    let mut schema = Schema::new();
    let mut table_schema = TableSchema::new(RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]));
    // Allow-all update policies — anonymous session must still be denied structurally.
    table_schema.policies =
        TablePolicies::new().with_update(Some(PolicyExpr::True), PolicyExpr::True);
    schema.insert(TableName::new("users"), table_schema);

    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row without a session so it succeeds.
    let inserted = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(10)],
        )
        .expect("insert without session should succeed");

    let anon = PolicySession::new("anon-user").with_auth_mode(AuthMode::Anonymous);

    let err = qm
        .update_with_session(
            &mut storage,
            inserted.row_id,
            &[Value::Text("Bob updated".into()), Value::Integer(20)],
            Some(&anon),
        )
        .expect_err("anonymous update must be denied");

    assert_eq!(
        err,
        QueryError::AnonymousWriteDenied {
            table: TableName::new("users"),
            operation: Operation::Update,
        }
    );
}

#[test]
fn anonymous_delete_is_denied_before_policy_eval() {
    use crate::query_manager::policy::Operation;
    use crate::query_manager::session::AuthMode;

    let mut schema = Schema::new();
    let mut table_schema = TableSchema::new(RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]));
    // Allow-all delete policy — anonymous session must still be denied structurally.
    table_schema.policies = TablePolicies::new().with_delete(PolicyExpr::True);
    schema.insert(TableName::new("users"), table_schema);

    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row without a session so it succeeds.
    let inserted = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Carol".into()), Value::Integer(5)],
        )
        .expect("insert without session should succeed");

    let anon = PolicySession::new("anon-user").with_auth_mode(AuthMode::Anonymous);

    let err = qm
        .delete_with_session(&mut storage, inserted.row_id, Some(&anon))
        .expect_err("anonymous delete must be denied");

    assert_eq!(
        err,
        QueryError::AnonymousWriteDenied {
            table: TableName::new("users"),
            operation: Operation::Delete,
        }
    );
}
