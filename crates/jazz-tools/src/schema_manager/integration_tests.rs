//! Integration tests for SchemaManager - full flow from schema to transformation.

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::collections::HashMap;

    use crate::metadata::{MetadataKey, RowProvenance, row_provenance_metadata};
    use crate::object::{BranchName, ObjectId};
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::query_manager::manager::{LocalUpdates, QueryError};
    use crate::query_manager::session::WriteContext;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, Schema, SchemaBuilder, SchemaHash, TableName,
        TableSchema, Value,
    };
    use crate::row_histories::{RowState, StoredRowBatch, VisibleRowEntry};
    use crate::schema_manager::{
        AppId, Lens, LensOp, LensTransform, SchemaContext, SchemaManager, generate_lens,
    };
    use crate::storage::{
        HistoryRowBytes, IndexMutation, MemoryStorage, OwnedHistoryRowBytes, OwnedVisibleRowBytes,
        RawTableMutation, RawTableRows, Storage, StorageError, VisibleRowBytes,
    };
    use crate::sync_manager::{
        InboxEntry, QueryPropagation, RowBatchKey, ServerId, Source, SyncManager, SyncPayload,
    };

    fn make_commit_id(n: u8) -> crate::row_histories::BatchId {
        crate::row_histories::BatchId([n; 16])
    }

    fn test_app_id() -> AppId {
        AppId::from_name("integration-test-app")
    }

    #[derive(Debug, Clone)]
    struct IncomingRowBatch {
        content: Vec<u8>,
        timestamp: u64,
        author: String,
    }

    impl IncomingRowBatch {
        fn to_row(&self, object_id: ObjectId, branch: &str) -> StoredRowBatch {
            let metadata = row_provenance_metadata(
                &RowProvenance::for_insert(self.author.clone(), self.timestamp),
                None,
            )
            .into_iter()
            .collect::<HashMap<_, _>>();
            StoredRowBatch::new(
                object_id,
                branch,
                Vec::<crate::row_histories::BatchId>::new(),
                self.content.clone(),
                RowProvenance::for_insert(self.author.clone(), self.timestamp),
                metadata,
                RowState::VisibleDirect,
                None,
            )
        }
    }

    fn stored_row_commit(
        content: Vec<u8>,
        timestamp: u64,
        author: impl Into<String>,
    ) -> IncomingRowBatch {
        IncomingRowBatch {
            content,
            timestamp,
            author: author.into(),
        }
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
        fn raw_table_put(
            &mut self,
            table: &str,
            key: &str,
            value: &[u8],
        ) -> Result<(), StorageError> {
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
            self.inner.apply_encoded_row_mutation(
                table,
                history_rows,
                visible_rows,
                index_mutations,
            )
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
            object_id: ObjectId,
        ) -> Result<Option<crate::catalogue::CatalogueEntry>, StorageError> {
            self.inner.load_catalogue_entry(object_id)
        }
    }

    // Test full migration workflow: v1 -> v2 with added column.

    // Test column rename through lens.

    // Test multi-table schema evolution.

    // Test draft lens detection and rejection.

    // Test validation of schema context.
    // ========================================================================
    // QueryManager Integration Tests
    // ========================================================================
    use crate::query_manager::graph::QueryGraph;
    use crate::query_manager::manager::QueryManager;
    use crate::query_manager::query::{Query, QueryBuilder};
    use crate::test_support::put_test_row_metadata;
    /// Helper to execute a query synchronously via subscribe/process/unsubscribe on SchemaManager.
    fn execute_query(
        manager: &mut SchemaManager,
        storage: &mut MemoryStorage,
        query: Query,
    ) -> Vec<(ObjectId, Vec<Value>)> {
        let qm = manager.query_manager_mut();
        let sub_id = qm.subscribe(query).unwrap();
        qm.process(storage);
        let results = qm.get_subscription_results(sub_id);
        qm.unsubscribe_with_sync(sub_id);
        results
    }

    fn execute_query_with_local_overlay(
        manager: &mut SchemaManager,
        storage: &mut MemoryStorage,
        query: Query,
        row_id: ObjectId,
        branch: &str,
        batch_id: crate::row_histories::BatchId,
    ) -> Vec<(ObjectId, Vec<Value>)> {
        let qm = manager.query_manager_mut();
        let sub_id = qm
            .subscribe_with_sync_and_propagation_with_local_overlay(
                query,
                None,
                None,
                crate::query_manager::subscriptions::SubscriptionExecutionOptions {
                    local_updates: LocalUpdates::Immediate,
                    propagation: QueryPropagation::Full,
                    local_overlay_rows: HashMap::from([(
                        row_id,
                        RowBatchKey::new(row_id, BranchName::new(branch), batch_id),
                    )]),
                },
            )
            .unwrap();
        qm.process(storage);
        let results = qm.get_subscription_results(sub_id);
        qm.unsubscribe_with_sync(sub_id);
        results
    }

    /// Ingest a remote row batch entry on a specific branch through the storage-backed sync path.
    /// QueryManager picks this up during `process()` via the sync inbox.
    #[allow(clippy::too_many_arguments)]
    fn ingest_remote_row(
        qm: &mut QueryManager,
        storage: &mut MemoryStorage,
        table: &str,
        schema_hash: SchemaHash,
        object_id: ObjectId,
        branch: &str,
        content: Vec<u8>,
        timestamp: u64,
    ) {
        let mut metadata = HashMap::new();
        metadata.insert(MetadataKey::Table.to_string(), table.to_string());
        metadata.insert(
            MetadataKey::OriginSchemaHash.to_string(),
            schema_hash.to_string(),
        );
        put_test_row_metadata(storage, object_id, metadata);

        let commit = stored_row_commit(content, timestamp, object_id.to_string());
        let row = commit.to_row(object_id, branch);
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(ServerId::new()),
            payload: SyncPayload::RowBatchCreated {
                metadata: None,
                row,
            },
        });
    }

    /// Ingest a remote catalogue object on the `main` branch through sync path.
    fn ingest_remote_catalogue_object(
        qm: &mut QueryManager,
        _storage: &mut MemoryStorage,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        content: Vec<u8>,
        _timestamp: u64,
    ) {
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(ServerId::new()),
            payload: SyncPayload::CatalogueEntryUpdated {
                entry: crate::catalogue::CatalogueEntry {
                    object_id,
                    metadata,
                    content,
                },
            },
        });
    }

    // Test QueryManager with schema context initialization.

    // Test QueryGraph compilation with schema context and column translation.

    // Test that SchemaManager's context can be used with QueryManager.

    // ========================================================================
    // End-to-end test-cache integration tests
    // ========================================================================

    // End-to-end test: Insert rows in old schema format, query with new schema,
    // verify lens transforms are applied.

    // ========================================================================
    // Multi-Hop Lens Path Integration Tests
    // ========================================================================

    // End-to-end test: v1 -> v2 -> v3 multi-hop transform.
    // Insert rows in v1 and v2 format, query with v3 schema,
    // verify lens transforms are applied across multiple hops.

    // Test multi-hop with chained column renames across versions.

    // End-to-end test with column rename: query uses new column name,
    // lens translates for old schema index lookup.

    // End-to-end test with table rename: query uses new table name,
    // lens translates old-branch scans and row decoding through the rename.

    // Table renames must also keep existing subscriptions reactive when old-schema
    // rows arrive after the query graph has already been compiled.

    // Existing old-schema subscriptions must recompile when a renamed future branch
    // becomes live, so new-table writes remain visible through the old table name.

    // Rows from renamed tables are migrated on write (in both updates and deletes).
    // ========================================================================
    // Catalogue Sync Tests
    // ========================================================================
    use crate::metadata::ObjectType;
    use crate::schema_manager::{
        decode_lens_transform, decode_schema, encode_lens_transform, encode_schema,
    };

    // Test schema persistence and encoding roundtrip.

    // Test lens persistence and encoding roundtrip.

    // Test catalogue update processing: schema received via sync.

    // Test catalogue update processing: lens makes pending schema live.

    // Test catalogue update processing: draft lenses are stored but must not
    // activate pending schemas.

    // Test pending catalogue updates are queued in QueryManager.

    // Non-matching app_id catalogue objects must be ignored for all schema-shape variants.

    // Unknown catalogue type must be ignored even for materially different schema payloads.

    // Pushing the exact same schema (same hash/content) should be a no-op.

    // Malformed schema payload should fail decode path deterministically.

    // Malformed lens payload should fail decode path deterministically.

    // E2E test: Full catalogue sync flow with data query.
    //
    // This test simulates the complete flow where:
    // 1. Client A (v2) persists schema and lens to catalogue
    // 2. Client B (v1) receives via catalogue sync
    // 3. Client A writes a row on v2 branch
    // 4. Client B queries and receives transformed data

    // Test multi-hop lens cascade activation via catalogue.
    //
    // Scenario: v1 client receives v2, then v3, then lens(v1->v2), then lens(v2->v3).
    // After each step, verify correct pending/live states.
    // ========================================================================
    // Multi-Client Server Schema Sync Tests
    // ========================================================================
    use crate::sync_manager::{ClientId, ClientRole, Destination, DurabilityTier};

    // E2E test: Two clients with same schema, server with empty schema.
    //
    // NOTE: This test is incomplete. The current architecture requires servers
    // to be initialized with the schema. Catalogue sync is designed for schema
    // EVOLUTION (adding new schema versions via lenses), not for schema
    // BOOTSTRAPPING (starting with no schema).
    //
    // The main test `e2e_two_clients_query_subscriptions_through_server`
    // validates the intended use case where all nodes share the same schema.
    //
    // Now implemented via lazy schema activation in QueryManager.

    // E2E test: Two clients, server all with same schema - query subscriptions sync.
    //
    // This is the more direct test of the user's question: both clients issue
    // query subscriptions that correctly sync through the server.

    // E2E test: Server with empty schema receives schema via sync, then handles queries.
    //
    // This tests the full scenario: server starts with no schema knowledge,
    // receives schema through catalogue sync, and can then process queries.

    // ========================================================================
    // Pending Row Updates Tests (rows arriving before schema)
    // ========================================================================

    // Test that rows arriving before their schema is known are buffered
    // and processed when the schema activates.
    //
    // Scenario:
    // 1. Client B (v1 schema) receives a row on the v2 branch (unknown schema)
    // 2. The row is buffered in pending_row_visibility_changes
    // 3. Client B receives schema v2 and lens v1->v2 via catalogue
    // 4. process() activates v2 and retries pending rows
    // 5. The row is now queryable with lens transform applied

    // ========================================================================
    // Query Settlement Tier Tests
    // ========================================================================

    // Test 1: Subscribe with settled_tier=None — immediate delivery (current behavior).

    // Test 2: Client A subscribes on server B with settled_tier=Local.
    // B settles → emits QuerySettled(Local). After A receives it, A delivers.

    // Test 3: A subscribes with settled_tier=EdgeServer through B (Worker) to C (EdgeServer).
    // Worker settling is insufficient. EdgeServer settling satisfies the requirement.

    // Test 4: A subscribes with settled_tier=EdgeServer through B (Worker) to C (EdgeServer).
    // C's QuerySettled(EdgeServer) should relay through B back to A.

    // Test 5: Data accumulates while waiting for tier. First delivery contains all rows.

    // Test 5: One-shot query() with settled_tier via subscribe_with_sync.
    // With `local_updates = Immediate`, the subscription should deliver the
    // locally pending row once the initial frontier is complete, before the
    // requested tier confirms.

    // Test 6: One-shot query() with settled_tier resolves to empty snapshot after tier settle.
    mod catalogue;
    mod lenses;
    mod locator_only_storage;
    mod migration;
    mod misc;
    mod query_subscription;
    mod renames;
    mod sync;
    mod writes;
}
