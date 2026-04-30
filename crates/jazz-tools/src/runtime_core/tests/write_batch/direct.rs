use super::*;

#[test]
fn rc_insert_returns_immediately() {
    let mut s = create_3tier_rc();
    let user_id = ObjectId::new();
    let expected_values = user_row_values(user_id, "Alice");
    let ((id, row_values), _) =
        s.a.insert("users", user_insert_values(user_id, "Alice"), None)
            .unwrap();
    assert!(!id.uuid().is_nil());
    assert_eq!(row_values, expected_values);

    let query = Query::new("users");
    let results = execute_query(&mut s.a, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, id);
    assert_eq!(results[0].1, row_values);
}

#[test]
fn rc_insert_data_syncs_to_server() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 1, "Server B should have the synced row");
    assert_eq!(results[0].0, id);
}

#[test]
fn rc_insert_syncs_exact_row_batch_without_row_region_reads() {
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batch-direct-sync-test",
        Box::new(RowRegionReadFailingStorage::new()),
    );
    let server_id = ServerId::new();
    core.add_server(server_id);
    core.batched_tick();
    core.sync_sender().take();

    let ((row_id, _row_values), _) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.batched_tick();

    let messages = core.sync_sender().take();
    let row_sync = messages
        .iter()
        .find(|entry| matches!(&entry.payload, SyncPayload::RowBatchCreated { row, .. } if row.row_id == row_id))
        .expect("insert should still sync the row upstream");

    match &row_sync.payload {
        SyncPayload::RowBatchCreated { row, .. } => {
            assert_eq!(row.row_id, row_id);
        }
        other => {
            panic!("local row writes should sync using the authored row batch entry, got {other:?}")
        }
    }
}

#[test]
fn rc_sealed_direct_batch_requests_settlement_from_connected_server() {
    let mut s = create_3tier_rc();
    let ((row_id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    let branch_name = s.a.schema_manager().branch_name();
    let batch_id =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap()
            .expect("insert should create one visible row")
            .batch_id;

    s.a.sync_sender().take();
    s.a.seal_batch(batch_id).unwrap();
    s.a.batched_tick();

    let outbox = s.a.sync_sender().take();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::SealBatch { submission },
        } if *server_id == s.b_server_for_a && submission.batch_id == batch_id
    )));
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::BatchSettlementNeeded { batch_ids },
        } if *server_id == s.b_server_for_a && batch_ids == &vec![batch_id]
    )));
}

#[test]
fn rc_update_sync() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    s.a.update(id, vec![("name".into(), Value::Text("Bob".into()))], None)
        .unwrap();
    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[1], Value::Text("Bob".into()));
}

#[test]
fn rc_delete_sync() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    s.a.delete(id, None).unwrap();
    pump_a_to_b(&mut s);

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 0, "Row should be deleted on B");
}

#[test]
fn rc_same_row_direct_batch_overwrites_in_place() {
    let mut core = create_test_runtime();
    let batch_id = BatchId::new();
    let write_context = WriteContext::default().with_batch_id(batch_id);

    let ((row_id, _), _) = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();

    core.update(
        row_id,
        vec![("name".to_string(), Value::Text("Alicia".to_string()))],
        Some(&write_context),
    )
    .unwrap();

    let branch_name = core.schema_manager().branch_name();
    let history_rows = core
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(
        history_rows.len(),
        1,
        "rewriting the same row inside one direct batch should overwrite the batch member instead of appending a second history row"
    );
    assert_eq!(history_rows[0].batch_id, batch_id);
    assert_eq!(history_rows[0].batch_id(), batch_id);

    let visible_row = core
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("direct batch row should stay visible");
    assert_eq!(visible_row.batch_id, batch_id);
    assert_eq!(visible_row.batch_id(), batch_id);
}

#[test]
fn rc_worker_direct_batch_retains_all_visible_members() {
    let mut s = create_3tier_rc();
    let batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Direct)
        .with_batch_id(batch_id);

    let ((first_row_id, _), first_batch_id) =
        s.b.insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();
    let ((second_row_id, _), second_batch_id) =
        s.b.insert(
            "users",
            user_insert_values(ObjectId::new(), "Bob"),
            Some(&write_context),
        )
        .unwrap();
    assert_eq!(first_batch_id, batch_id);
    assert_eq!(second_batch_id, batch_id);

    let branch_name = s.b.schema_manager().branch_name();
    let local_record =
        s.b.storage()
            .load_local_batch_record(batch_id)
            .unwrap()
            .expect("worker should retain one direct batch record for shared writes");

    match local_record.latest_settlement {
        Some(crate::batch_fate::BatchSettlement::DurableDirect {
            batch_id: settled_batch_id,
            confirmed_tier,
            visible_members,
        }) => {
            assert_eq!(settled_batch_id, batch_id);
            assert_eq!(confirmed_tier, DurabilityTier::Local);
            assert_eq!(
                visible_members.len(),
                2,
                "shared direct batches should retain all current members under one settlement"
            );
            assert!(visible_members.iter().any(|member| {
                member.object_id == first_row_id
                    && member.branch_name == branch_name
                    && member.batch_id == batch_id
            }));
            assert!(visible_members.iter().any(|member| {
                member.object_id == second_row_id
                    && member.branch_name == branch_name
                    && member.batch_id == batch_id
            }));
        }
        other => panic!("expected durable direct settlement, got {other:?}"),
    }
}

#[test]
fn rc_sealed_direct_batch_rejects_further_writes() {
    let mut core = create_test_runtime();
    let batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Direct)
        .with_batch_id(batch_id);

    let ((row_id, _), _) = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
        )
        .unwrap();

    core.seal_batch(batch_id).unwrap();

    let record = core
        .storage()
        .load_local_batch_record(batch_id)
        .unwrap()
        .expect("sealed direct batch should keep its local record");
    assert_eq!(record.mode, crate::batch_fate::BatchMode::Direct);
    assert!(record.sealed);
    assert_eq!(
        record
            .sealed_submission
            .as_ref()
            .expect("sealed direct batch should persist a submission")
            .captured_frontier,
        Vec::<CapturedFrontierMember>::new(),
        "direct batch seals should not capture transactional frontier state"
    );

    let err = core
        .update(
            row_id,
            vec![("name".to_string(), Value::Text("Alicia".to_string()))],
            Some(&write_context),
        )
        .expect_err("sealed direct batches should be frozen");
    let err = format!("{err:?}");
    assert!(
        err.contains("already sealed"),
        "expected sealed-batch error, got {err:?}"
    );
}

#[test]
fn rc_local_batch_record_does_not_seal_current_open_direct_batch() {
    let mut core = create_test_runtime();
    let batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Direct)
        .with_batch_id(batch_id);

    core.insert(
        "users",
        user_insert_values(ObjectId::new(), "Alice"),
        Some(&write_context),
    )
    .unwrap();

    let record = core
        .local_batch_record(batch_id)
        .unwrap()
        .expect("open direct batch should have a local record");
    assert!(!record.sealed);
    assert!(record.sealed_submission.is_none());
}

#[test]
fn rc_restart_recovers_completed_sealed_batch_from_storage() {
    let schema = test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let batch_id = BatchId::new();
    let row_id = ObjectId::new();
    let staged_row = staged_user_row(row_id, batch_id, 1_000, "Alice");

    let mut old_runtime = create_runtime_with_schema_and_sync_manager(
        schema.clone(),
        "transactional-restart-seal-recovery-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );
    old_runtime
        .storage_mut()
        .put_row_locator(
            row_id,
            Some(&RowLocator {
                table: "users".into(),
                origin_schema_hash: Some(schema_hash),
            }),
        )
        .unwrap();
    old_runtime
        .storage_mut()
        .append_history_region_rows("users", std::slice::from_ref(&staged_row))
        .unwrap();
    old_runtime
        .storage_mut()
        .upsert_sealed_batch_submission(&SealedBatchSubmission::new(
            batch_id,
            crate::object::BranchName::new("main"),
            vec![SealedBatchMember {
                object_id: row_id,
                row_digest: staged_row.content_digest(),
            }],
            Vec::new(),
        ))
        .unwrap();

    let storage = old_runtime.into_storage();
    let restarted = create_runtime_with_storage_and_sync_manager(
        schema,
        "transactional-restart-seal-recovery-test",
        storage,
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );

    let settlement = restarted
        .storage()
        .load_authoritative_batch_settlement(batch_id)
        .unwrap()
        .expect("restart should recover and settle completed sealed batch");
    assert!(matches!(
        settlement,
        crate::batch_fate::BatchSettlement::AcceptedTransaction {
            batch_id: settled_batch_id,
            confirmed_tier: DurabilityTier::Local,
            ref visible_members,
        } if settled_batch_id == batch_id
            && *visible_members == vec![crate::batch_fate::VisibleBatchMember {
                object_id: row_id,
                branch_name: crate::object::BranchName::new("main"),
                batch_id,
            }]
    ));

    let visible = restarted
        .storage()
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("restart recovery should publish the accepted row");
    assert_eq!(
        visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(visible.batch_id, batch_id);
    assert_eq!(
        restarted
            .storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap(),
        None,
        "recovered settlement should prune the sealed submission marker"
    );
}
