use super::*;

#[test]
fn schema_manager_update_uses_visible_row_after_legacy_commit_history_is_removed() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v1_branch = format!("dev-{}-main", v1_hash.short());

    let mut writer =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    writer.add_live_schema(v1.clone()).unwrap();

    let mut storage = MemoryStorage::new();
    let row_id = ObjectId::new();
    let v1_table = v1.get(&TableName::new("users")).unwrap();
    let original_content = encode_row(
        &v1_table.columns,
        &[Value::Uuid(row_id), Value::Text("Alice".to_string())],
    )
    .unwrap();
    ingest_remote_row(
        writer.query_manager_mut(),
        &mut storage,
        "users",
        v1_hash,
        row_id,
        &v1_branch,
        original_content,
        1_000,
    );
    writer.process(&mut storage);

    let mut reader =
        SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
    reader.add_live_schema(v1).unwrap();

    reader
            .update_with_write_context(
                &mut storage,
                row_id,
                &[
                    ("name".to_string(), Value::Text("Alice Updated".to_string())),
                    (
                        "email".to_string(),
                        Value::Text("alice@example.com".to_string()),
                    ),
                ],
                None,
            )
            .expect(
                "schema update should succeed from visible row state without legacy object-backed storage",
            );

    let query = reader
        .query("users")
        .select(&["id", "name", "email"])
        .build();
    let results = execute_query(&mut reader, &mut storage, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, row_id);
    assert_eq!(
        results[0].1,
        vec![
            Value::Uuid(row_id),
            Value::Text("Alice Updated".to_string()),
            Value::Text("alice@example.com".to_string()),
        ]
    );
}

#[test]
fn schema_manager_can_update_denies_known_hard_deleted_row() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("name", ColumnType::Text)
                .column("score", ColumnType::Integer),
        )
        .build();

    let mut manager =
        SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
    let mut storage = MemoryStorage::new();

    let inserted = manager
        .insert(
            &mut storage,
            "users",
            HashMap::from([
                ("name".to_string(), Value::Text("Alice".to_string())),
                ("score".to_string(), Value::Integer(100)),
            ]),
        )
        .unwrap();
    manager
        .query_manager_mut()
        .hard_delete(&mut storage, inserted.row_id)
        .unwrap();

    let decision = manager
        .can_update_with_write_context(
            &mut storage,
            inserted.row_id,
            &[("score".to_string(), Value::Integer(101))],
            None,
        )
        .unwrap();

    assert_eq!(
        decision,
        crate::query_manager::types::PermissionPreflightDecision::Deny
    );
}

#[test]
fn removed_table_then_readded_does_not_resurface_old_rows() {
    // Branch story:
    //
    // v1: users(id, name)   <- alice lives here
    // v2: <users dropped>
    // v3: users(id, name)   <- bob lives here
    //
    // A v3 query for `users` must only see the v3 table. The v1 `users`
    // rows belong to a removed table lineage and must not be treated as
    // the same logical table just because the name appears again in v3.

    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();
    let v2 = SchemaBuilder::new().build();
    let v3 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);
    let v3_hash = SchemaHash::compute(&v3);

    let lens_v1_v2 = generate_lens(&v1, &v2);
    let lens_v2_v3 = generate_lens(&v2, &v3);

    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v3.clone(), "dev", "main");
    qm.add_live_schema(v2.clone());
    qm.register_lens(lens_v2_v3);
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens_v1_v2);
    let mut storage = MemoryStorage::new();

    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());
    let v3_branch = format!("dev-{}-main", v3_hash.short());

    let lens_path = qm.schema_context().lens_path(&v1_hash).unwrap();
    assert_eq!(lens_path.len(), 2, "expected v1 -> v2 -> v3 lens path");

    assert_eq!(
        crate::schema_manager::translate_table_name_to_schema(
            qm.schema_context(),
            "users",
            &v1_hash,
        ),
        None,
        "the re-added v3 users table must not translate back into the removed v1 table",
    );

    let v1_table = v1.get(&TableName::new("users")).unwrap();
    let alice_id = ObjectId::new();
    let alice_row = vec![Value::Uuid(alice_id), Value::Text("Alice".to_string())];
    let alice_data = encode_row(&v1_table.columns, &alice_row).unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        alice_id,
        &v1_branch,
        alice_data,
        1_000,
    );

    let bob_id = ObjectId::new();
    qm.insert(
        &mut storage,
        "users",
        &[
            Value::Uuid(bob_id),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ],
    )
    .unwrap();

    let query = QueryBuilder::new("users")
        .branches(&[&v1_branch, &v2_branch, &v3_branch])
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let results = qm.get_subscription_results(sub_id);
    qm.unsubscribe_with_sync(sub_id);

    assert_eq!(
        results.len(),
        1,
        "v1 rows from the dropped table lineage must not appear under the re-added v3 table",
    );
    assert_eq!(
        results[0].1,
        vec![
            Value::Uuid(bob_id),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ]
    );
}

#[test]
fn transactional_insert_uses_frozen_target_branch_schema() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email_address", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameColumn {
            table: "users".to_string(),
            old_name: "email".to_string(),
            new_name: "email_address".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    manager.add_live_schema_with_lens(v1.clone(), lens).unwrap();

    let mut storage = MemoryStorage::new();
    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let current_branch = manager.branch_name().as_str().to_string();

    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional)
        .with_target_branch_name(v1_branch.clone());

    let inserted = manager
        .insert_with_write_context(
            &mut storage,
            "users",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                (
                    "email".to_string(),
                    Value::Text("alice@example.com".to_string()),
                ),
            ]),
            Some(&write_context),
        )
        .expect("frozen-target insert should use the target branch schema");

    let target_rows = execute_query_with_local_overlay(
        &mut manager,
        &mut storage,
        QueryBuilder::new("users").branch(v1_branch.clone()).build(),
        inserted.row_id,
        &v1_branch,
        inserted.batch_id,
    );
    assert_eq!(target_rows.len(), 1);
    assert_eq!(target_rows[0].0, inserted.row_id);
    assert!(
        target_rows[0]
            .1
            .iter()
            .any(|value| value == &Value::Text("alice@example.com".to_string()))
    );

    let current_rows = execute_query(
        &mut manager,
        &mut storage,
        QueryBuilder::new("users").branch(current_branch).build(),
    );
    assert!(
        current_rows.is_empty(),
        "insert should stay on the frozen target branch rather than drifting to the manager current branch"
    );
}

#[test]
fn transactional_insert_rejects_target_branch_outside_current_family() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let mut manager =
        SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
    let mut storage = MemoryStorage::new();

    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional)
        .with_target_branch_name("prod-111111111111-feature");

    let err = manager
        .insert_with_write_context(
            &mut storage,
            "users",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                (
                    "email".to_string(),
                    Value::Text("alice@example.com".to_string()),
                ),
            ]),
            Some(&write_context),
        )
        .expect_err("cross-family target branch should be rejected");

    assert!(
        matches!(err, QueryError::EncodingError(ref msg) if msg.contains("outside the current schema family")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn transactional_update_uses_frozen_target_branch_schema() {
    // current branch: v2 users(id, email_address)
    //                  |
    // tx target:       v1 users(id, email)
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email_address", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameColumn {
            table: "users".to_string(),
            old_name: "email".to_string(),
            new_name: "email_address".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    manager.add_live_schema_with_lens(v1.clone(), lens).unwrap();

    let mut storage = MemoryStorage::new();
    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let current_branch = manager.branch_name().as_str().to_string();
    let row_id = ObjectId::new();

    let v2_table = v2.get(&TableName::new("users")).unwrap();
    let row_data = encode_row(
        &v2_table.columns,
        &[
            Value::Uuid(row_id),
            Value::Text("alice@example.com".to_string()),
        ],
    )
    .unwrap();

    ingest_remote_row(
        manager.query_manager_mut(),
        &mut storage,
        "users",
        v2_hash,
        row_id,
        &current_branch,
        row_data,
        1_000,
    );
    manager.process(&mut storage);

    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional)
        .with_target_branch_name(v1_branch.clone());

    let batch_id = manager
        .update_with_write_context(
            &mut storage,
            row_id,
            &[(
                "email".to_string(),
                Value::Text("alice+tx@example.com".to_string()),
            )],
            Some(&write_context),
        )
        .expect("frozen-target update should use the target branch schema");

    let target_rows = execute_query_with_local_overlay(
        &mut manager,
        &mut storage,
        QueryBuilder::new("users").branch(v1_branch.clone()).build(),
        row_id,
        &v1_branch,
        batch_id,
    );
    assert_eq!(target_rows.len(), 1);
    assert_eq!(target_rows[0].0, row_id);
    assert!(
        target_rows[0]
            .1
            .iter()
            .any(|value| value == &Value::Text("alice+tx@example.com".to_string()))
    );

    let current_rows = execute_query(
        &mut manager,
        &mut storage,
        QueryBuilder::new("users").branch(current_branch).build(),
    );
    assert_eq!(current_rows.len(), 1);
    assert!(
        current_rows[0]
            .1
            .iter()
            .any(|value| value == &Value::Text("alice@example.com".to_string())),
        "frozen-target update should not rewrite the manager current branch"
    );
}

#[test]
fn transactional_delete_uses_frozen_target_branch_schema() {
    // current branch: v2 users(id, email_address)
    //                  |
    // tx target:       v1 users(id, email)
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email_address", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameColumn {
            table: "users".to_string(),
            old_name: "email".to_string(),
            new_name: "email_address".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    manager.add_live_schema_with_lens(v1.clone(), lens).unwrap();

    let mut storage = MemoryStorage::new();
    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let current_branch = manager.branch_name().as_str().to_string();
    let row_id = ObjectId::new();

    let v2_table = v2.get(&TableName::new("users")).unwrap();
    let row_data = encode_row(
        &v2_table.columns,
        &[
            Value::Uuid(row_id),
            Value::Text("alice@example.com".to_string()),
        ],
    )
    .unwrap();

    ingest_remote_row(
        manager.query_manager_mut(),
        &mut storage,
        "users",
        v2_hash,
        row_id,
        &current_branch,
        row_data,
        1_000,
    );
    manager.process(&mut storage);

    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional)
        .with_target_branch_name(v1_branch.clone());

    manager
        .delete(&mut storage, row_id, Some(&write_context))
        .expect("frozen-target delete should use the target branch schema");

    let target_rows = execute_query(
        &mut manager,
        &mut storage,
        QueryBuilder::new("users").branch(v1_branch.clone()).build(),
    );
    assert!(
        target_rows.is_empty(),
        "frozen-target delete should create the soft delete on the target branch"
    );

    let current_rows = execute_query(
        &mut manager,
        &mut storage,
        QueryBuilder::new("users")
            .branch(current_branch.clone())
            .build(),
    );
    assert_eq!(current_rows.len(), 1);
    assert!(
        manager
            .query_manager()
            .row_is_deleted_on_branch(&storage, "users", &v1_branch, row_id),
        "target branch should carry the delete marker"
    );
    assert!(
        !manager.query_manager().row_is_deleted_on_branch(
            &storage,
            "users",
            &current_branch,
            row_id
        ),
        "current branch should remain untouched"
    );
}

#[test]
fn e2e_rows_buffered_until_schema_activates() {
    // Schema v1: users(id, name)
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    // Schema v2: users(id, name, email)
    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    // Client starts with v1 schema only
    let mut client =
        SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main").unwrap();
    let mut storage = MemoryStorage::new();

    // Get branch names
    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());

    // Create a row on the v2 branch (simulate receiving via sync)
    // IMPORTANT: When schema goes through encode/decode, columns are sorted alphabetically.
    // So we need to encode the row in the same order it would arrive via sync.
    // v2 columns alphabetically: email, id, name
    let row_id = ObjectId::new();

    // Simulate what the schema looks like after encode/decode (alphabetical columns)
    let v2_encoded_decoded = decode_schema(&encode_schema(&v2)).unwrap();
    let v2_table = v2_encoded_decoded.get(&TableName::new("users")).unwrap();

    // Build values in the correct column order (alphabetical)
    let row_values: Vec<Value> = v2_table
        .columns
        .columns
        .iter()
        .map(|col| match col.name.as_str() {
            "id" => Value::Uuid(row_id),
            "name" => Value::Text("Alice".to_string()),
            "email" => Value::Text("alice@example.com".to_string()),
            _ => panic!("unexpected column"),
        })
        .collect();
    let row_data = encode_row(&v2_table.columns, &row_values).unwrap();

    ingest_remote_row(
        client.query_manager_mut(),
        &mut storage,
        "users",
        v2_hash,
        row_id,
        &v2_branch,
        row_data,
        1_000,
    );

    // Process - this should trigger handle_object_update for the v2 row
    // Since v2 schema is unknown, it should be buffered
    client.process(&mut storage);

    // Query on v1 branch should not find the row (it's on v2 branch)
    let query = QueryBuilder::new("users").branch(&v1_branch).build();
    let results = execute_query(&mut client, &mut storage, query);
    assert_eq!(
        results.len(),
        0,
        "Row on v2 branch should not appear in v1 query yet"
    );

    // === Now simulate receiving v2 schema and lens via catalogue ===

    // Receive v2 schema
    let v2_encoded = encode_schema(&v2);
    let mut schema_metadata = HashMap::new();
    schema_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    schema_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    schema_metadata.insert(MetadataKey::SchemaHash.to_string(), v2_hash.to_string());

    client
        .process_catalogue_update(v2_hash.to_object_id(), &schema_metadata, &v2_encoded)
        .unwrap();

    // v2 should be pending (no lens yet)
    assert!(client.context().is_pending(&v2_hash));

    // Receive lens v1->v2
    let lens = generate_lens(&v1, &v2);
    let lens_encoded = encode_lens_transform(&lens.forward);
    let mut lens_metadata = HashMap::new();
    lens_metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueLens.to_string(),
    );
    lens_metadata.insert(
        MetadataKey::AppId.to_string(),
        test_app_id().uuid().to_string(),
    );
    lens_metadata.insert(MetadataKey::SourceHash.to_string(), v1_hash.to_string());
    lens_metadata.insert(MetadataKey::TargetHash.to_string(), v2_hash.to_string());

    client
        .process_catalogue_update(lens.object_id(), &lens_metadata, &lens_encoded)
        .unwrap();

    // Process again - this should:
    // 1. Activate v2 schema (now has lens path)
    // 2. Call sync_context()
    // 3. Retry pending row updates
    client.process(&mut storage);

    // v2 should now be live
    assert!(
        client.context().is_live(&v2_hash),
        "v2 schema should be live after lens received"
    );
    assert_eq!(client.all_branches().len(), 2, "Should have 2 branches now");

    // Now query across both branches - should find the row
    let multi_query = QueryBuilder::new("users")
        .branches(&[&v1_branch, &v2_branch])
        .build();
    let sub_id = client.query_manager_mut().subscribe(multi_query).unwrap();
    client.process(&mut storage);

    let results = client.query_manager().get_subscription_results(sub_id);

    // Should have 1 row (from v2 branch, transformed to v1 format)
    assert_eq!(
        results.len(),
        1,
        "Should find the buffered row after schema activation"
    );

    // The row should be transformed via lens (v2 -> v1 removes email column)
    // Note: v1 has 2 columns (id, name), so after transform we get 2 columns
    // Columns are alphabetically sorted: id, name
    assert_eq!(
        results[0].1.len(),
        2,
        "Row should be transformed to v1 format (2 columns)"
    );

    // Find columns by name since order depends on encoding
    let v1_encoded_decoded = decode_schema(&encode_schema(&v1)).unwrap();
    let v1_table = v1_encoded_decoded.get(&TableName::new("users")).unwrap();
    let id_idx = v1_table
        .columns
        .columns
        .iter()
        .position(|c| c.name.as_str() == "id")
        .unwrap();
    let name_idx = v1_table
        .columns
        .columns
        .iter()
        .position(|c| c.name.as_str() == "name")
        .unwrap();

    assert_eq!(results[0].1[id_idx], Value::Uuid(row_id));
    assert_eq!(results[0].1[name_idx], Value::Text("Alice".to_string()));
}
