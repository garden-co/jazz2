use super::*;

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
    let initial_tips: Vec<_> = test_row_tip_ids(&storage, handle.row_id, &branch).to_vec();
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
    let tips: Vec<_> = test_row_tip_ids(&storage, handle.row_id, &branch).to_vec();
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
