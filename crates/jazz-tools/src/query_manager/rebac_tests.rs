//! ReBAC Policy Evaluation Integration Tests
//!
//! Tests for the async permission evaluation system using policy graphs.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use smallvec::smallvec;

use crate::batch_fate::BatchSettlement;
use crate::metadata::{
    DeleteKind, MetadataKey, RowProvenance, SYSTEM_PRINCIPAL_ID, row_provenance_metadata,
};
use crate::object::{BranchName, ObjectId};
use crate::row_histories::BatchId;
use crate::storage::{MemoryStorage, Storage};
use crate::sync_manager::{
    ClientId, Destination, InboxEntry, QueryId, RowMetadata, Source, SyncError, SyncManager,
    SyncPayload,
};
use crate::test_support::{
    apply_test_row_batch, create_test_row, load_test_row_metadata, load_test_row_tip_ids,
    seeded_memory_storage,
};

use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::manager::QueryError;
use crate::query_manager::manager::QueryManager;
use crate::query_manager::policy::Operation;
use crate::query_manager::policy::PolicyExpr;
use crate::query_manager::query::{Query, QueryBuilder};
use crate::query_manager::relation_ir::{
    ColumnRef, PredicateCmpOp, PredicateExpr, RelExpr, ValueRef,
};
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, ComposedBranchName, RowDescriptor, RowPolicyMode, Schema,
    SchemaHash, TableName, TablePolicies, TableSchema, Value,
};
use crate::row_histories::{RowState, StoredRowBatch};

/// Helper to create QueryManager with schema on default branch.
fn create_query_manager(sync_manager: SyncManager, schema: Schema) -> QueryManager {
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema, "dev", "main");
    qm
}

fn create_query_manager_with_policy_mode(
    sync_manager: SyncManager,
    schema: Schema,
    row_policy_mode: RowPolicyMode,
) -> QueryManager {
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema_with_policy_mode(schema, "dev", "main", row_policy_mode);
    qm
}

/// Get the schema context's branch name.
fn get_branch(qm: &QueryManager) -> String {
    qm.schema_context().branch_name().as_str().to_string()
}

fn connect_client(qm: &mut QueryManager, storage: &MemoryStorage, client_id: ClientId) {
    qm.sync_manager_mut()
        .add_client_with_storage(storage, client_id);
}

fn set_client_query_scope(
    qm: &mut QueryManager,
    storage: &MemoryStorage,
    client_id: ClientId,
    query_id: QueryId,
    scope: HashSet<(ObjectId, BranchName)>,
    session: Option<Session>,
) {
    qm.sync_manager_mut()
        .set_client_query_scope_with_storage(storage, client_id, query_id, scope, session);
}

#[derive(Debug, Clone)]
struct IncomingRowBatch {
    batch_id: BatchId,
    parents: smallvec::SmallVec<[BatchId; 2]>,
    content: Vec<u8>,
    timestamp: u64,
    author: String,
    delete_kind: Option<DeleteKind>,
}

impl IncomingRowBatch {
    fn row_provenance(&self) -> RowProvenance {
        RowProvenance::for_insert(self.author.clone(), self.timestamp)
    }

    fn row_metadata(&self) -> HashMap<String, String> {
        row_provenance_metadata(&self.row_provenance(), self.delete_kind)
            .into_iter()
            .collect()
    }

    fn to_row(&self, object_id: ObjectId, branch: &str, state: RowState) -> StoredRowBatch {
        StoredRowBatch::new_with_batch_id(
            self.batch_id,
            object_id,
            branch,
            self.parents.iter().copied().collect::<Vec<_>>(),
            self.content.clone(),
            self.row_provenance(),
            self.row_metadata(),
            state,
            None,
        )
    }
}

fn stored_row_commit(
    parents: smallvec::SmallVec<[BatchId; 2]>,
    content: Vec<u8>,
    timestamp: u64,
    author: impl Into<String>,
    delete_kind: Option<DeleteKind>,
) -> IncomingRowBatch {
    IncomingRowBatch {
        batch_id: BatchId::new(),
        parents,
        content,
        timestamp,
        author: author.into(),
        delete_kind,
    }
}

fn row_batch_created_payload(
    object_id: ObjectId,
    branch: &str,
    metadata: Option<RowMetadata>,
    commit: &IncomingRowBatch,
) -> SyncPayload {
    SyncPayload::RowBatchCreated {
        metadata,
        row: commit.to_row(object_id, branch, RowState::VisibleDirect),
    }
}

fn row_batch_id_for_commit(
    object_id: ObjectId,
    branch: &str,
    commit: &IncomingRowBatch,
) -> BatchId {
    commit
        .to_row(object_id, branch, RowState::VisibleDirect)
        .batch_id()
}

fn client_write_rejection_reason(
    outbox: &[crate::sync_manager::OutboxEntry],
    client_id: ClientId,
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
) -> Option<String> {
    let mut saw_rejected_state = false;
    let mut settlement_reason = None;

    for entry in outbox {
        if entry.destination != Destination::Client(client_id) {
            continue;
        }

        match &entry.payload {
            SyncPayload::Error(SyncError::PermissionDenied { reason, .. }) => {
                return Some(reason.clone());
            }
            SyncPayload::RowBatchStateChanged {
                row_id: rejected_row_id,
                branch_name,
                batch_id: rejected_batch_id,
                state: Some(RowState::Rejected),
                ..
            } if *rejected_row_id == row_id
                && branch_name.as_str() == branch
                && *rejected_batch_id == batch_id =>
            {
                saw_rejected_state = true;
            }
            SyncPayload::BatchSettlement {
                settlement:
                    BatchSettlement::Rejected {
                        batch_id: rejected_batch_id,
                        reason,
                        ..
                    },
            } if *rejected_batch_id == batch_id => {
                settlement_reason = Some(reason.clone());
            }
            _ => {}
        }
    }

    settlement_reason.or_else(|| saw_rejected_state.then(|| "rejected".to_string()))
}

fn client_write_was_rejected(
    outbox: &[crate::sync_manager::OutboxEntry],
    client_id: ClientId,
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
) -> bool {
    client_write_rejection_reason(outbox, client_id, row_id, branch, batch_id).is_some()
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

fn test_row_metadata(storage: &MemoryStorage, row_id: ObjectId) -> Option<HashMap<String, String>> {
    load_test_row_metadata(storage, row_id)
}

fn test_row_tip_ids(
    storage: &MemoryStorage,
    row_id: ObjectId,
    branch: impl AsRef<str>,
) -> Result<Vec<BatchId>, crate::storage::StorageError> {
    load_test_row_tip_ids(storage, row_id, branch.as_ref())
}

/// Schema for ReBAC tests: documents with owner_id policy + folders for INHERITS
fn rebac_test_schema() -> Schema {
    let mut schema = Schema::new();

    // Folders table (parent for documents)
    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let folders_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor, folders_policies),
    );

    // Documents table with owner_id policy
    let docs_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("folder_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let docs_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(docs_descriptor, docs_policies),
    );

    schema
}

fn magic_introspection_schema() -> Schema {
    let mut schema = Schema::new();

    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::with_policies(
            admins_descriptor,
            TablePolicies::new().with_select(PolicyExpr::True),
        ),
    );

    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_update(
            Some(PolicyExpr::Exists {
                table: "admins".into(),
                condition: Box::new(PolicyExpr::eq_session("user_id", vec!["user_id".into()])),
            }),
            PolicyExpr::True,
        )
        .with_delete(PolicyExpr::ExistsRel {
            rel: RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("admins"),
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("user_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["user_id".into()]),
                },
            },
        });
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor, protected_policies),
    );

    schema
}

fn provenance_notes_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![ColumnDescriptor::new("title", ColumnType::Text)])
}

fn provenance_notes_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("notes"),
        TableSchema::new(provenance_notes_descriptor()),
    );
    schema
}

fn authorship_permissions_schema() -> Schema {
    let mut schema = Schema::new();
    let created_by_is_session = PolicyExpr::eq_session("$createdBy", vec!["user_id".into()]);
    let notes_policies = TablePolicies::new()
        .with_select(created_by_is_session.clone())
        .with_insert(created_by_is_session.clone())
        .with_update(
            Some(created_by_is_session.clone()),
            created_by_is_session.clone(),
        )
        .with_delete(created_by_is_session);
    schema.insert(
        TableName::new("notes"),
        TableSchema::with_policies(provenance_notes_descriptor(), notes_policies),
    );
    schema
}

fn query_rows(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    query: Query,
    session: Option<Session>,
) -> Vec<(ObjectId, Vec<Value>)> {
    let sub_id = qm
        .subscribe_with_session(query, session, None)
        .expect("query subscription should be created");

    for _ in 0..10 {
        qm.process(storage);
    }

    let results = qm.get_subscription_results(sub_id);
    qm.unsubscribe_with_sync(sub_id);
    results
}

fn recursive_folders_schema(max_depth: Option<usize>) -> Schema {
    let mut schema = Schema::new();

    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);

    let select_policy = PolicyExpr::Or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::Inherits {
            operation: Operation::Select,
            via_column: "parent_id".into(),
            max_depth,
        },
    ]);

    let update_using = PolicyExpr::Or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::Inherits {
            operation: Operation::Update,
            via_column: "parent_id".into(),
            max_depth,
        },
    ]);

    let folders_policies = TablePolicies::new()
        .with_select(select_policy)
        .with_update(Some(update_using), PolicyExpr::True);

    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor, folders_policies),
    );

    schema
}

/// Helper to encode a document row
fn encode_document(owner_id: &str, title: &str, folder_id: Option<ObjectId>) -> Vec<u8> {
    let docs_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("folder_id", ColumnType::Uuid).nullable(),
    ]);
    encode_row(
        &docs_desc,
        &[
            Value::Text(owner_id.into()),
            Value::Text(title.into()),
            match folder_id {
                Some(id) => Value::Uuid(id),
                None => Value::Null,
            },
        ],
    )
    .unwrap()
}

fn encode_folder(owner_id: &str, name: &str) -> Vec<u8> {
    let folders_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    encode_row(
        &folders_desc,
        &[Value::Text(owner_id.into()), Value::Text(name.into())],
    )
    .unwrap()
}

/// Helper to create a document metadata map
fn document_metadata() -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    m.insert(MetadataKey::Table.to_string(), "documents".to_string());
    m
}

fn folder_metadata() -> std::collections::HashMap<String, String> {
    let mut m = std::collections::HashMap::new();
    m.insert(MetadataKey::Table.to_string(), "folders".to_string());
    m
}

fn inherited_insert_schema() -> (Schema, RowDescriptor, SchemaHash) {
    let mut schema = Schema::new();

    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let folders_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor.clone(), folders_policies),
    );

    let documents_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("folder_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let documents_policies = TablePolicies::new().with_insert(PolicyExpr::And(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::Or(vec![
            PolicyExpr::IsNull {
                column: "folder_id".into(),
            },
            PolicyExpr::inherits(Operation::Select, "folder_id"),
        ]),
    ]));
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(documents_descriptor, documents_policies),
    );

    let schema_hash = SchemaHash::compute(&schema);
    (schema, folders_descriptor, schema_hash)
}

fn inherited_insert_branch(schema_hash: SchemaHash) -> String {
    ComposedBranchName::new("dev", schema_hash, "client-alice-main")
        .to_branch_name()
        .as_str()
        .to_string()
}

fn create_server_mode_query_manager(schema: Schema, schema_hash: SchemaHash) -> QueryManager {
    let sync_manager = SyncManager::new();
    let mut qm = QueryManager::new(sync_manager);
    qm.schema = Arc::new(schema.clone());
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, schema);
    qm.set_known_schemas(Arc::new(known_schemas));
    qm
}

fn seed_folder_on_branch(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    branch: &str,
    owner_id: &str,
    name: &str,
    folders_descriptor: &RowDescriptor,
) -> ObjectId {
    let folder_id = create_test_row(storage, Some(folder_metadata()));
    let folder_content = encode_folder(owner_id, name);
    add_row_commit(
        storage,
        folder_id,
        branch,
        vec![],
        folder_content.clone(),
        1000,
        ObjectId::new().to_string(),
    );
    QueryManager::update_indices_for_insert_on_branch(
        storage,
        "folders",
        branch,
        folder_id,
        &folder_content,
        folders_descriptor,
    )
    .unwrap();
    qm.persist_row_region_tip(storage, "folders", folder_id, branch);
    folder_id
}

fn enqueue_inherited_insert(
    qm: &mut QueryManager,
    client_id: ClientId,
    doc_id: ObjectId,
    branch: &str,
    folder_id: ObjectId,
    title: &str,
) -> IncomingRowBatch {
    let commit = stored_row_commit(
        smallvec![],
        encode_document("alice", title, Some(folder_id)),
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            doc_id,
            branch,
            Some(RowMetadata {
                id: doc_id,
                metadata: document_metadata(),
            }),
            &commit,
        ),
    });

    commit
}

fn run_recursive_folder_update(max_depth: Option<usize>) -> (bool, bool) {
    let schema = recursive_folders_schema(max_depth);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let root_handle = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .unwrap();
    let child_handle = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root_handle.row_id),
            ],
        )
        .unwrap();
    let grand_handle = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Grandchild".into()),
                Value::Uuid(child_handle.row_id),
            ],
        )
        .unwrap();

    let grand_id = grand_handle.row_id;
    let branch = get_branch(&qm);

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let mut scope = HashSet::new();
    scope.insert((grand_id, branch.clone().into()));
    set_client_query_scope(&mut qm, &storage, client_id, QueryId(100), scope, None);
    qm.sync_manager_mut().take_outbox();

    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);

    let update_content = encode_row(
        &folders_descriptor,
        &[
            Value::Text("bob".into()),
            Value::Text("Renamed by Alice".into()),
            Value::Uuid(child_handle.row_id),
        ],
    )
    .unwrap();

    let update_commit = stored_row_commit(
        smallvec![grand_handle.batch_id],
        update_content,
        4200,
        ObjectId::new().to_string(),
        None,
    );

    let object_metadata = test_row_metadata(&storage, grand_id).unwrap_or_default();

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            grand_id,
            &branch,
            Some(RowMetadata {
                id: grand_id,
                metadata: object_metadata,
            }),
            &update_commit,
        ),
    });

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        grand_id,
        &branch,
        row_batch_id_for_commit(grand_id, &branch, &update_commit),
    );

    let tips = test_row_tip_ids(&storage, grand_id, &branch).unwrap();
    let applied = tips.contains(&row_batch_id_for_commit(grand_id, &branch, &update_commit));

    (denied, applied)
}

// Test that EXISTS clause in INSERT policy correctly denies writes.
//
// Scenario: Insert policy requires EXISTS (SELECT FROM admins WHERE user_id = @session.user_id)
// A non-admin user tries to insert - should be denied.

// Test that UPDATE checks USING policy (can session see the old row?).
//
// Scenario: Alice owns a document. Bob tries to update it.
// The USING policy (owner_id = @session.user_id) should deny Bob because
// he cannot "see" Alice's document.
//
// CURRENT BUG: Only WITH CHECK is evaluated for UPDATE, not USING.
// See: manager.rs:1246-1247 - "TODO: Full USING check for UPDATE"

// Test that INHERITS in SELECT policy correctly filters rows in query results.
//
// Scenario: Documents inherit SELECT policy from their parent folder.
// Alice owns folder F. Bob owns document D in folder F.
// When Alice queries documents, she should NOT see Bob's document D
// because even though D is in her folder, INHERITS should check
// if Alice can see D directly (which requires owner_id = alice).
//
// Actually, let's reverse this: Alice should be able to see documents
// in her folder via INHERITS, even if she doesn't own them directly.
//
// Scenario revised:
// - Folder F owned by Alice
// - Document D in folder F, owned by Bob
// - SELECT policy: owner_id = @user_id OR INHERITS SELECT VIA folder_id
// - Alice should see D because she owns the folder (INHERITS passes)
// - Charlie (owns neither) should NOT see D
//
// FIXED: PolicyFilterNode now properly evaluates INHERITS using PolicyGraph.

// Test that EXISTS clause in UPDATE USING policy correctly denies updates.
//
// Scenario: UPDATE policy has USING = EXISTS (only admins can update protected rows)
// - Alice is an admin, Bob is not
// - Both try to update a protected row
// - Bob should be denied (USING EXISTS fails), Alice should be allowed

// ============================================================================
// INHERITS Cycle Detection Tests
// ============================================================================

// Test that INHERITS cycles are detected during schema validation.
// Cycle: A → B → A (direct cycle between two tables)

// Test that self-referential INHERITS is detected as a cycle.
// Cycle: Folder → Folder (self-reference via parent_id)

// Test that valid INHERITS chains (no cycles) pass validation.

// Test that bounded self-referential INHERITS is accepted by cycle validation.

fn declared_file_inheritance_schema(array_edge: bool) -> Schema {
    let mut schema = Schema::new();

    let source_fk_column = if array_edge { "images" } else { "image" };
    let inherited_read = PolicyExpr::InheritsReferencing {
        operation: Operation::Select,
        source_table: "todos".into(),
        via_column: source_fk_column.into(),
        max_depth: None,
    };
    let inherited_update = PolicyExpr::InheritsReferencing {
        operation: Operation::Update,
        source_table: "todos".into(),
        via_column: source_fk_column.into(),
        max_depth: None,
    };

    let files_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let files_policies = TablePolicies::new()
        .with_select(PolicyExpr::or(vec![
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
            inherited_read,
        ]))
        .with_update(
            Some(PolicyExpr::or(vec![
                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                inherited_update,
            ])),
            PolicyExpr::True,
        );
    schema.insert(
        TableName::new("files"),
        TableSchema::with_policies(files_descriptor, files_policies),
    );

    let image_column = if array_edge {
        ColumnDescriptor::new(
            "images",
            ColumnType::Array {
                element: Box::new(ColumnType::Uuid),
            },
        )
        .references("files")
    } else {
        ColumnDescriptor::new("image", ColumnType::Uuid)
            .nullable()
            .references("files")
    };
    let todos_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        image_column,
    ]);
    let todos_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_update(
            Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
            PolicyExpr::True,
        );
    schema.insert(
        TableName::new("todos"),
        TableSchema::with_policies(todos_descriptor, todos_policies),
    );

    schema
}

mod declared_fk_inheritance;
mod exists_policies;
mod exists_rel_policies;
mod inheritance_validation;
mod inherited_policies;
mod insert_policies;
mod magic_provenance;
mod mutations;
mod recursive_inheritance;
mod select_policies;
