use super::*;

#[test]
fn test_sync_edit_fires_callback_synchronously() {
    let mut core = create_test_runtime();

    let callback_count = Arc::new(Mutex::new(0usize));
    let count_clone = callback_count.clone();

    let query = Query::new("users");
    let _handle = core
        .subscribe(
            query,
            move |delta| {
                if !delta.ordered_delta.added.is_empty() {
                    *count_clone.lock().unwrap() += 1;
                }
            },
            None,
        )
        .unwrap();

    core.immediate_tick();
    let initial_count = *callback_count.lock().unwrap();

    let _ = core.insert(
        "users",
        user_insert_values(ObjectId::new(), "test@test.com"),
        None,
    );
    core.immediate_tick();

    let final_count = *callback_count.lock().unwrap();
    assert!(
        final_count > initial_count,
        "Callback must fire synchronously after insert when index ready"
    );
}

#[test]
fn rc_query_reads_old_schema_row_after_evolving_to_new_schema() {
    let v1 = schema_evolution_v1();
    let v2 = schema_evolution_v2();

    let mut old_runtime = create_runtime_with_schema(v1.clone(), "schema-evolution-test");
    let user_id = ObjectId::new();
    let inserted_values = HashMap::from([
        ("id".to_string(), Value::Uuid(user_id)),
        ("name".to_string(), Value::Text("Alice".to_string())),
    ]);
    let ((inserted_id, _), _) = old_runtime.insert("users", inserted_values, None).unwrap();

    let storage = old_runtime.into_storage();

    let mut evolved_runtime = create_runtime_with_storage(v2, "schema-evolution-test", storage);
    evolved_runtime
        .add_live_schema_and_persist_catalogue(v1)
        .expect("v1 should be attachable as a live schema for v2");
    evolved_runtime.immediate_tick();

    let results = execute_runtime_query(&mut evolved_runtime, Query::new("users"), None);

    assert_eq!(
        results.len(),
        1,
        "Expected one row visible after schema evolution"
    );

    let (queried_id, queried_values) = &results[0];
    let current_schema = evolved_runtime.current_schema();
    let id_idx = column_index(current_schema, "users", "id");
    let name_idx = column_index(current_schema, "users", "name");
    let email_idx = column_index(current_schema, "users", "email");
    assert_eq!(*queried_id, inserted_id);
    assert_eq!(queried_values.len(), 3, "Row should decode in v2 shape");
    assert_eq!(queried_values[id_idx], Value::Uuid(user_id));
    assert_eq!(queried_values[name_idx], Value::Text("Alice".to_string()));
    assert_eq!(
        queried_values[email_idx],
        Value::Text(String::new()),
        "New required column should be backfilled with the lens default",
    );
}

#[test]
fn rc_update_old_schema_row_after_evolution_copies_row_to_current_schema() {
    let v1 = schema_evolution_v1();
    let v2 = schema_evolution_v2();

    let mut old_runtime = create_runtime_with_schema(v1.clone(), "schema-evolution-update-test");
    let user_id = ObjectId::new();
    let inserted_values = HashMap::from([
        ("id".to_string(), Value::Uuid(user_id)),
        ("name".to_string(), Value::Text("Alice".to_string())),
    ]);
    let ((inserted_id, _), _) = old_runtime.insert("users", inserted_values, None).unwrap();

    let storage = old_runtime.into_storage();

    let mut evolved_runtime =
        create_runtime_with_storage(v2.clone(), "schema-evolution-update-test", storage);
    evolved_runtime
        .add_live_schema_and_persist_catalogue(v1.clone())
        .expect("v1 should be attachable as a live schema for v2");
    evolved_runtime.immediate_tick();

    evolved_runtime
        .update(
            inserted_id,
            vec![
                ("name".to_string(), Value::Text("Alice Updated".to_string())),
                (
                    "email".to_string(),
                    Value::Text("alice.updated@example.com".to_string()),
                ),
            ],
            None,
        )
        .expect("Updating an old-schema row should succeed via copy-on-write");

    let results = execute_runtime_query(&mut evolved_runtime, Query::new("users"), None);
    assert_eq!(
        results.len(),
        1,
        "Copy-on-write should preserve a single logical row"
    );

    let (queried_id, queried_values) = &results[0];
    let current_schema = evolved_runtime.current_schema();
    let id_idx = column_index(current_schema, "users", "id");
    let name_idx = column_index(current_schema, "users", "name");
    let email_idx = column_index(current_schema, "users", "email");
    assert_eq!(*queried_id, inserted_id);
    assert_eq!(
        queried_values.len(),
        3,
        "Updated row should decode in v2 shape"
    );
    assert_eq!(queried_values[id_idx], Value::Uuid(user_id));
    assert_eq!(
        queried_values[name_idx],
        Value::Text("Alice Updated".to_string())
    );
    assert_eq!(
        queried_values[email_idx],
        Value::Text("alice.updated@example.com".to_string()),
    );
}

#[test]
fn rc_delete_old_schema_row_after_evolution_hides_row_from_queries() {
    let v1 = schema_evolution_v1();
    let v2 = schema_evolution_v2();

    let mut old_runtime = create_runtime_with_schema(v1.clone(), "schema-evolution-delete-test");
    let user_id = ObjectId::new();
    let inserted_values = HashMap::from([
        ("id".to_string(), Value::Uuid(user_id)),
        ("name".to_string(), Value::Text("Alice".to_string())),
    ]);
    let ((inserted_id, _), _) = old_runtime.insert("users", inserted_values, None).unwrap();

    let storage = old_runtime.into_storage();

    let mut evolved_runtime =
        create_runtime_with_storage(v2.clone(), "schema-evolution-delete-test", storage);
    evolved_runtime
        .add_live_schema_and_persist_catalogue(v1.clone())
        .expect("v1 should be attachable as a live schema for v2");
    evolved_runtime.immediate_tick();

    evolved_runtime
        .delete(inserted_id, None)
        .expect("Deleting an old-schema row should succeed after schema evolution");

    let results = execute_runtime_query(&mut evolved_runtime, Query::new("users"), None);
    assert_eq!(
        results.len(),
        0,
        "Deleted old-schema row should no longer be visible after schema evolution",
    );
}

/// FIXME: this is an undesired behavior. See `/todo/ideas/1_mvp/lens-hardening.md`
#[test]
fn rc_old_client_update_removes_unseen_newer_fields() {
    let v1 = schema_evolution_v1();
    let v2 = schema_evolution_v2();

    // Flow:
    // v2 client writes row with email on the v2 branch
    // v1 client reads that row through v2 -> v1 lens and updates only name
    // v2 client does not see the original email after the v1-originated update
    let mut new_runtime =
        create_runtime_with_schema(v2.clone(), "schema-evolution-backward-update-test");
    let user_id = ObjectId::new();
    let inserted_values = HashMap::from([
        ("id".to_string(), Value::Uuid(user_id)),
        ("name".to_string(), Value::Text("Alice".to_string())),
        (
            "email".to_string(),
            Value::Text("alice@example.com".to_string()),
        ),
    ]);
    let ((inserted_id, _), _) = new_runtime.insert("users", inserted_values, None).unwrap();

    let storage = new_runtime.into_storage();

    let mut old_runtime =
        create_runtime_with_storage(v1.clone(), "schema-evolution-backward-update-test", storage);
    old_runtime
        .add_live_schema_and_persist_catalogue(v2.clone())
        .expect("v2 should be attachable as a live schema for v1");
    old_runtime.immediate_tick();

    old_runtime
        .update(
            inserted_id,
            vec![(
                "name".to_string(),
                Value::Text("Alice Updated From v1".to_string()),
            )],
            None,
        )
        .expect("Updating a newer-schema row from an old client should succeed");

    let history_bytes = old_runtime
        .storage()
        .scan_history_region_bytes(
            "users",
            crate::row_histories::HistoryScan::Row {
                row_id: inserted_id,
            },
        )
        .expect("history bytes should be readable after old-client update");
    assert!(
        old_runtime
            .storage()
            .scan_history_row_batches("users", inserted_id)
            .expect("row-history bytes should remain decodable with keyed schema context")
            .len()
            == history_bytes.len(),
        "all row-history versions should remain decodable as flat rows once their schemas are in catalogue"
    );

    let storage = old_runtime.into_storage();

    let mut reloaded_v2 =
        create_runtime_with_storage(v2.clone(), "schema-evolution-backward-update-test", storage);
    reloaded_v2
        .add_live_schema_and_persist_catalogue(v1.clone())
        .expect("v1 should be attachable as a live schema for v2");
    reloaded_v2.immediate_tick();

    let results = execute_runtime_query(&mut reloaded_v2, Query::new("users"), None);
    assert_eq!(
        results.len(),
        1,
        "Old-client update should still leave one logical row visible"
    );

    let (queried_id, queried_values) = &results[0];
    let current_schema = reloaded_v2.current_schema();
    let id_idx = column_index(current_schema, "users", "id");
    let name_idx = column_index(current_schema, "users", "name");
    let email_idx = column_index(current_schema, "users", "email");

    assert_eq!(*queried_id, inserted_id);
    assert_eq!(queried_values[id_idx], Value::Uuid(user_id));
    assert_eq!(
        queried_values[name_idx],
        Value::Text("Alice Updated From v1".to_string()),
    );
    assert_eq!(
        queried_values[email_idx],
        Value::Text("".to_string()),
        "Old-client updates remove unseen new-schema fields",
    );
}

#[test]
fn runtime_bootstraps_current_schema_into_catalogue_for_flat_row_history() {
    let schema = schema_evolution_v1();
    let schema_hash = SchemaHash::compute(&schema);
    let mut core = create_runtime_with_schema(schema.clone(), "flat-row-history-bootstrap");

    let schema_entry = core
        .storage()
        .load_catalogue_entry(schema_hash.to_object_id())
        .expect("catalogue lookup should succeed");
    assert!(
        schema_entry.is_some(),
        "runtime startup should persist the current schema into catalogue storage"
    );

    let row_id = ObjectId::new();
    let ((inserted_id, _), _) = core
        .insert("users", user_insert_values(row_id, "Alice"), None)
        .expect("insert should succeed");

    let history_bytes = core
        .storage()
        .scan_history_region_bytes(
            "users",
            crate::row_histories::HistoryScan::Row {
                row_id: inserted_id,
            },
        )
        .expect("history bytes should be readable after insert");
    assert_eq!(history_bytes.len(), 1);

    let user_descriptor = schema
        .get(&TableName::new("users"))
        .expect("users table should exist")
        .columns
        .clone();
    let decoded = crate::row_histories::decode_flat_history_row(
        &user_descriptor,
        inserted_id,
        "main",
        core.storage()
            .scan_history_row_batches("users", inserted_id)
            .expect("typed history rows should be readable")
            .first()
            .expect("one history row should exist")
            .batch_id(),
        &history_bytes[0],
    )
    .expect("flat history row should decode with the catalogue-backed descriptor");
    assert_eq!(decoded.row_id, inserted_id);
    assert!(!decoded.data.is_empty());
}

#[test]
fn test_persist_schema_then_add_server_sends_catalogue() {
    // Mirror the WASM flow EXACTLY: NO immediate_tick before persist_schema
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), NoopScheduler);
    // NO immediate_tick() here — matches WASM openPersistent flow

    // persist_schema — stages a catalogue object before the first tick
    let schema_obj_id = core.persist_schema();

    // add_server — should call queue_full_sync_to_server which includes the catalogue
    let server_id = ServerId::new();
    core.add_server(server_id);

    // batched_tick — should flush catalogue to outbox → sync sender
    core.batched_tick();

    // Check that the catalogue was sent
    let messages = core.sync_sender().take();
    let catalogue_msg = messages.iter().find(|m| {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &m.payload {
            entry.object_id == schema_obj_id
                && entry
                    .metadata
                    .get(crate::metadata::MetadataKey::Type.as_str())
                    .map(|t| t == crate::metadata::ObjectType::CatalogueSchema.as_str())
                    .unwrap_or(false)
        } else {
            false
        }
    });
    let permissions_msg = messages.iter().find(|m| {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &m.payload {
            entry
                .metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .map(|t| {
                    t == crate::metadata::ObjectType::CataloguePermissions.as_str()
                        || t == crate::metadata::ObjectType::CataloguePermissionsBundle.as_str()
                        || t == crate::metadata::ObjectType::CataloguePermissionsHead.as_str()
                })
                .unwrap_or(false)
        } else {
            false
        }
    });

    assert!(
        catalogue_msg.is_some(),
        "Catalogue schema object should be in outbox after add_server + batched_tick. \
             Messages found: {}",
        messages
            .iter()
            .map(|m| format!("{:?}", m.payload))
            .collect::<Vec<_>>()
            .join(", ")
    );
    assert!(
        permissions_msg.is_none(),
        "persist_schema should not implicitly publish permissions catalogue objects"
    );
}

#[test]
fn test_batched_tick_keeps_outbox_when_no_transport_or_sync_sender_is_installed() {
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);

    core.persist_schema();
    let server_id = ServerId::new();
    core.add_server(server_id);

    core.batched_tick();

    let outbox = core
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .take_outbox();
    assert!(
        outbox
            .iter()
            .any(|entry| matches!(entry.destination, Destination::Server(id) if id == server_id)),
        "batched_tick without a transport should leave server-bound outbox entries intact"
    );
}

#[test]
fn test_publish_permissions_bundle_then_add_server_sends_head_and_bundle() {
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let schema_hash = SchemaHash::compute(&schema);
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), NoopScheduler);

    core.persist_schema();
    core.publish_permissions_bundle(
        schema_hash,
        std::collections::HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]),
        None,
    )
    .expect("publish permissions bundle");

    let server_id = ServerId::new();
    core.add_server(server_id);
    core.batched_tick();

    let messages = core.sync_sender().take();
    let bundle_msg = messages.iter().find(|m| {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &m.payload {
            entry
                .metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .map(|t| t == crate::metadata::ObjectType::CataloguePermissionsBundle.as_str())
                .unwrap_or(false)
        } else {
            false
        }
    });
    let head_msg = messages.iter().find(|m| {
        if let SyncPayload::CatalogueEntryUpdated { entry } = &m.payload {
            entry
                .metadata
                .get(crate::metadata::MetadataKey::Type.as_str())
                .map(|t| t == crate::metadata::ObjectType::CataloguePermissionsHead.as_str())
                .unwrap_or(false)
        } else {
            false
        }
    });

    assert!(
        bundle_msg.is_some(),
        "Explicit permission publication should sync the immutable permissions bundle object"
    );
    assert!(
        head_msg.is_some(),
        "Explicit permission publication should sync the mutable permissions head object"
    );
}

#[test]
fn test_matching_catalogue_hash_skips_catalogue_replay_on_add_server() {
    let schema = test_schema();
    let app_id = AppId::from_name("test-app");
    let sync_manager = SyncManager::new();
    let schema_manager = SchemaManager::new(sync_manager, schema, app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), NoopScheduler);

    let schema_obj_id = core.persist_schema();
    let ((row_object_id, _), _) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    let catalogue_state_hash = core.schema_manager().catalogue_state_hash();

    let server_id = ServerId::new();
    core.add_server_with_catalogue_state_hash(server_id, Some(&catalogue_state_hash));
    core.batched_tick();

    let messages = core.sync_sender().take();
    let catalogue_msg = messages.iter().find(|m| {
        matches!(
            &m.payload,
            SyncPayload::CatalogueEntryUpdated { entry } if entry.object_id == schema_obj_id
        )
    });
    let row_msg = messages.iter().find(|m| match &m.payload {
        SyncPayload::RowBatchCreated { row, .. } => row.row_id == row_object_id,
        _ => false,
    });

    assert!(
        catalogue_msg.is_none(),
        "Catalogue replay should be skipped when hashes already match"
    );
    assert!(
        row_msg.is_some(),
        "Regular row objects should still be sent during the full sync walk"
    );
}
// =========================================================================
// Foreign Key — No Write-Time Validation
// =========================================================================
//
// FK write-time existence checks are intentionally removed: in a local-first
// system with query-scoped sync, the referenced row may not be loaded yet,
// causing false violations. True referential integrity will be enforced by
// global transactions (specs/todo/b_launch/globally_consistent_transactions.md).
//
// These tests document that FK-referencing writes succeed even when the
// target row is absent from the local index. They double as scaffolding for
// when global transactions re-introduce server-side FK checks.
