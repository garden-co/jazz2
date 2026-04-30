use super::*;

#[test]
fn rc_update_direct_batch_remains_pending_until_terminal_settlement() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    let branch_name = s.a.schema_manager().branch_name();
    let insert_batch_id =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), id)
            .unwrap()
            .expect("insert should create one visible row")
            .batch_id;

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchSettlement {
            settlement: crate::batch_fate::BatchSettlement::DurableDirect {
                batch_id: insert_batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
                visible_members: vec![crate::batch_fate::VisibleBatchMember {
                    object_id: id,
                    branch_name,
                    batch_id: insert_batch_id,
                }],
            },
        },
    });
    s.a.immediate_tick();

    let update_batch_id =
        s.a.update(id, vec![("name".into(), Value::Text("Bob".into()))], None)
            .unwrap();

    let update_record =
        s.a.storage()
            .load_local_batch_record(update_batch_id)
            .unwrap()
            .expect("direct update should create a local batch record");
    assert_eq!(
        update_record.latest_settlement,
        Some(crate::batch_fate::BatchSettlement::DurableDirect {
            batch_id: update_batch_id,
            confirmed_tier: DurabilityTier::Local,
            visible_members: vec![crate::batch_fate::VisibleBatchMember {
                object_id: id,
                branch_name,
                batch_id: update_batch_id,
            }],
        })
    );

    s.a.sync_sender().take();
    s.a.remove_server(s.b_server_for_a);
    s.a.add_server(s.b_server_for_a);
    s.a.batched_tick();

    let outbox = s.a.sync_sender().take();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::BatchSettlementNeeded { batch_ids },
        } if *server_id == s.b_server_for_a && batch_ids == &vec![update_batch_id]
    )));
}

#[test]
fn rc_delete_direct_batch_remains_pending_until_terminal_settlement() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    let branch_name = s.a.schema_manager().branch_name();
    let insert_batch_id =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), id)
            .unwrap()
            .expect("insert should create one visible row")
            .batch_id;

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchSettlement {
            settlement: crate::batch_fate::BatchSettlement::DurableDirect {
                batch_id: insert_batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
                visible_members: vec![crate::batch_fate::VisibleBatchMember {
                    object_id: id,
                    branch_name,
                    batch_id: insert_batch_id,
                }],
            },
        },
    });
    s.a.immediate_tick();

    let delete_batch_id = s.a.delete(id, None).unwrap();

    let delete_record =
        s.a.storage()
            .load_local_batch_record(delete_batch_id)
            .unwrap()
            .expect("direct delete should create a local batch record");
    assert_eq!(
        delete_record.latest_settlement,
        Some(crate::batch_fate::BatchSettlement::DurableDirect {
            batch_id: delete_batch_id,
            confirmed_tier: DurabilityTier::Local,
            visible_members: vec![crate::batch_fate::VisibleBatchMember {
                object_id: id,
                branch_name,
                batch_id: delete_batch_id,
            }],
        })
    );

    s.a.sync_sender().take();
    s.a.remove_server(s.b_server_for_a);
    s.a.add_server(s.b_server_for_a);
    s.a.batched_tick();

    let outbox = s.a.sync_sender().take();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::BatchSettlementNeeded { batch_ids },
        } if *server_id == s.b_server_for_a && batch_ids == &vec![delete_batch_id]
    )));
}

#[test]
fn rc_insert_persisted_resolves_on_worker_ack() {
    let mut s = create_3tier_rc();
    let user_id = ObjectId::new();
    let expected_values = user_row_values(user_id, "Alice");
    let ((id, row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(user_id, "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();
    assert!(!id.0.is_nil());
    assert_eq!(row_values, expected_values);

    assert!(
        receiver.try_recv().is_err() || receiver.try_recv() == Ok(None),
        "Receiver should not be resolved before ack"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(Ok(()))) => {}
        Ok(Some(Err(rejection))) => panic!("Receiver should not reject: {rejection:?}"),
        Ok(None) => panic!("Receiver should be resolved after Local ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }
}

#[test]
fn rc_insert_persisted_does_not_touch_legacy_ack_storage() {
    let calls = Arc::new(Mutex::new(LegacyStorageCallCounts));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-no-legacy-ack-storage",
        Box::new(LegacyPersistenceObservingStorage::new(Arc::clone(&calls))),
    );

    let ((row_id, _row_values), mut receiver) = core
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    let branch_name = core.schema_manager().branch_name();
    let batch_id = core
        .storage
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("persisted insert should materialize a visible row")
        .batch_id;

    core.push_sync_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name,
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Local),
        },
    });
    core.immediate_tick();

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Ok(()))),
        "row persisted receiver should resolve from row-batch state changes alone"
    );
    assert_eq!(
        *calls.lock().unwrap(),
        LegacyStorageCallCounts,
        "row durability updates should not touch legacy durability-ack storage"
    );
}

#[test]
fn rc_insert_persisted_ignores_row_state_changed_for_different_row_same_batch_id() {
    let mut s = create_3tier_rc();
    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    let branch_name = s.a.schema_manager().branch_name();
    let row_batch_id =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap()
            .expect("insert should create one visible row")
            .batch_id;

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::RowBatchStateChanged {
            row_id: ObjectId::new(),
            branch_name,
            batch_id: row_batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Local),
        },
    });
    s.a.immediate_tick();

    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "row persisted receivers should ignore row-state acks for a different row, even if the batch id matches"
    );
}

#[test]
fn rc_insert_persisted_holds_until_correct_tier() {
    let mut s = create_3tier_rc();
    let (_id, mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::EdgeServer,
        )
        .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "Local ack should not satisfy EdgeServer request"
    );

    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(Ok(()))) => {}
        Ok(Some(Err(rejection))) => panic!("Receiver should not reject: {rejection:?}"),
        Ok(None) => panic!("Receiver should be resolved after EdgeServer ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }
}

#[test]
fn rc_insert_persisted_higher_tier_satisfies_lower() {
    let mut s = create_3tier_rc();
    let (_id, mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    pump_3tier(&mut s);

    match receiver.try_recv() {
        Ok(Some(Ok(()))) => {}
        Ok(Some(Err(rejection))) => panic!("Local request should not reject: {rejection:?}"),
        Ok(None) => panic!("EdgeServer ack should satisfy Local request"),
        Err(_) => panic!("Receiver was cancelled"),
    }
}

#[test]
fn rc_insert_persisted_tracks_local_batch_record_and_settlement() {
    let mut s = create_3tier_rc();
    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    let branch_name = s.a.schema_manager().branch_name();
    let visible_row =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap()
            .expect("insert should create one visible row");
    let batch_id = visible_row.batch_id;

    let initial_record =
        s.a.storage()
            .load_local_batch_record(batch_id)
            .unwrap()
            .expect("persisted write should create a local batch record");
    assert_eq!(initial_record.batch_id, batch_id);
    assert_eq!(initial_record.mode, crate::batch_fate::BatchMode::Direct);
    assert_eq!(
        initial_record.latest_settlement, None,
        "client-side persisted direct writes should start pending until an upstream durability settlement arrives"
    );

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name,
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Local),
        },
    });
    s.a.immediate_tick();

    assert_eq!(receiver.try_recv(), Ok(Some(Ok(()))));

    let settled_record =
        s.a.storage()
            .load_local_batch_record(batch_id)
            .unwrap()
            .expect("settled local batch record should still be present");
    assert_eq!(
        settled_record.latest_settlement,
        Some(crate::batch_fate::BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::Local,
            visible_members: vec![crate::batch_fate::VisibleBatchMember {
                object_id: row_id,
                branch_name,
                batch_id,
            }],
        })
    );
}

#[test]
fn rc_insert_persisted_retains_batch_after_waiter_tier_is_met() {
    let mut s = create_3tier_rc();
    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    let branch_name = s.a.schema_manager().branch_name();
    let visible_row =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap()
            .expect("insert should create one visible row");
    let batch_id = visible_row.batch_id;

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Ok(()))),
        "the caller-facing worker wait should resolve once worker confirms"
    );

    s.a.remove_server(s.b_server_for_a);
    s.a.add_server(s.b_server_for_a);
    s.a.batched_tick();

    let outbox = s.a.sync_sender().take();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::BatchSettlementNeeded { batch_ids },
        } if *server_id == s.b_server_for_a && batch_ids == &vec![batch_id]
    )));
}

#[test]
fn rc_insert_persisted_retains_batch_after_edge_waiter_tier_is_met() {
    let mut s = create_3tier_rc();
    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::EdgeServer,
        )
        .unwrap();

    let branch_name = s.a.schema_manager().branch_name();
    let visible_row =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap()
            .expect("insert should create one visible row");
    let batch_id = visible_row.batch_id;

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);
    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "worker confirmation should not satisfy an edge wait"
    );

    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);
    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Ok(()))),
        "edge confirmation should resolve the caller-facing wait"
    );

    let settled_record =
        s.a.storage()
            .load_local_batch_record(batch_id)
            .unwrap()
            .expect("edge-accepted direct batch should stay retained");
    assert_eq!(
        settled_record.latest_settlement,
        Some(crate::batch_fate::BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::EdgeServer,
            visible_members: vec![crate::batch_fate::VisibleBatchMember {
                object_id: row_id,
                branch_name,
                batch_id,
            }],
        })
    );

    s.a.sync_sender().take();
    s.a.remove_server(s.b_server_for_a);
    s.a.add_server(s.b_server_for_a);
    s.a.batched_tick();

    let outbox = s.a.sync_sender().take();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::BatchSettlementNeeded { batch_ids },
        } if *server_id == s.b_server_for_a && batch_ids == &vec![batch_id]
    )));
}

#[test]
fn rc_insert_persisted_terminal_direct_settlement_stops_reconciliation() {
    let mut s = create_3tier_rc();
    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::EdgeServer,
        )
        .unwrap();

    let branch_name = s.a.schema_manager().branch_name();
    let visible_row =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap()
            .expect("insert should create one visible row");
    let batch_id = visible_row.batch_id;

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);
    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);
    assert_eq!(receiver.try_recv(), Ok(Some(Ok(()))));

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchSettlement {
            settlement: crate::batch_fate::BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
                visible_members: vec![crate::batch_fate::VisibleBatchMember {
                    object_id: row_id,
                    branch_name,
                    batch_id,
                }],
            },
        },
    });
    s.a.immediate_tick();

    let settled_record =
        s.a.storage()
            .load_local_batch_record(batch_id)
            .unwrap()
            .expect("terminally accepted direct batch should still be inspectable");
    assert_eq!(
        settled_record.latest_settlement,
        Some(crate::batch_fate::BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::GlobalServer,
            visible_members: vec![crate::batch_fate::VisibleBatchMember {
                object_id: row_id,
                branch_name,
                batch_id,
            }],
        })
    );

    s.a.sync_sender().take();
    s.a.remove_server(s.b_server_for_a);
    s.a.add_server(s.b_server_for_a);
    s.a.batched_tick();

    let outbox = s.a.sync_sender().take();
    assert!(!outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::BatchSettlementNeeded { batch_ids },
        } if *server_id == s.b_server_for_a && batch_ids.contains(&batch_id)
    )));
}

#[test]
fn rc_insert_persisted_resolves_from_batch_settlement_without_row_state_changed() {
    let mut s = create_3tier_rc();
    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    let branch_name = s.a.schema_manager().branch_name();
    let visible_row =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap()
            .expect("insert should create one visible row");
    let batch_id = visible_row.batch_id;

    s.a.push_sync_inbox(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchSettlement {
            settlement: crate::batch_fate::BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::Local,
                visible_members: vec![crate::batch_fate::VisibleBatchMember {
                    object_id: row_id,
                    branch_name,
                    batch_id,
                }],
            },
        },
    });
    s.a.immediate_tick();

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Ok(()))),
        "persisted receivers should resolve from replayable batch settlement even when a live row-batch ack was missed"
    );
}

#[test]
fn rc_insert_persisted_reconnect_reconciles_pending_batch_from_server() {
    // alice -> worker
    //   write reaches worker, but the live settlement never comes back
    //   then alice reconnects and asks for the batch fate explicitly
    let mut s = create_3tier_rc();
    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    let branch_name = s.a.schema_manager().branch_name();
    let visible_row =
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap()
            .expect("insert should create one visible row");
    let batch_id = visible_row.batch_id;

    pump_a_to_b(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(None),
        "without the return settlement, the persisted receiver should still be pending"
    );

    s.a.remove_server(s.b_server_for_a);
    s.a.add_server(s.b_server_for_a);

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Ok(()))),
        "reconnect should reconcile the still-pending batch from the server's current durable truth"
    );

    let settled_record =
        s.a.storage()
            .load_local_batch_record(batch_id)
            .unwrap()
            .expect("reconciled local batch record should still be present");
    assert_eq!(
        settled_record.latest_settlement,
        Some(crate::batch_fate::BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::Local,
            visible_members: vec![crate::batch_fate::VisibleBatchMember {
                object_id: row_id,
                branch_name,
                batch_id,
            }],
        })
    );
}

#[test]
fn rc_add_server_requests_pending_batch_settlement_reconciliation() {
    let mut s = create_3tier_rc();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    s.a.remove_server(s.b_server_for_a);

    let ((row_id, _row_values), _receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
            DurabilityTier::Local,
        )
        .unwrap();

    let history_rows =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;

    s.a.seal_batch(batch_id).unwrap();
    s.a.add_server(s.b_server_for_a);
    s.a.batched_tick();

    let outbox = s.a.sync_sender().take();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::BatchSettlementNeeded { batch_ids },
        } if *server_id == s.b_server_for_a && batch_ids == &vec![batch_id]
    )));
}

#[test]
fn rc_missing_batch_settlement_retransmits_original_captured_frontier() {
    let mut s = create_3tier_rc();
    let existing_row_id = ObjectId::new();
    let later_row_id = ObjectId::new();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    let ((existing_row_id, _), _) =
        s.a.insert("users", user_insert_values(existing_row_id, "Seen"), None)
            .unwrap();
    let existing_history_rows =
        s.a.storage()
            .scan_history_row_batches("users", existing_row_id)
            .unwrap();
    let existing_branch_name =
        crate::object::BranchName::new(existing_history_rows[0].branch.as_str());
    let existing_batch_id = existing_history_rows[0].batch_id();

    let ((row_id, _row_values), _receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
            DurabilityTier::Local,
        )
        .unwrap();

    let history_rows =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    let batch_id = history_rows[0].batch_id;
    let branch_name = crate::object::BranchName::new(history_rows[0].branch.as_str());
    let row_digest = history_rows[0].content_digest();
    s.a.seal_batch(batch_id).unwrap();

    let sealed_submission =
        s.a.storage()
            .load_local_batch_record(batch_id)
            .unwrap()
            .expect("sealed batch should retain a local batch record")
            .sealed_submission
            .expect("sealed batch should persist its original submission");
    assert_eq!(
        sealed_submission.captured_frontier,
        vec![CapturedFrontierMember {
            object_id: existing_row_id,
            branch_name: existing_branch_name,
            batch_id: existing_batch_id,
        }]
    );

    s.a.batched_tick();
    let dropped_outbox = s.a.sync_sender().take();
    assert!(dropped_outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::SealBatch { submission },
        } if *server_id == s.b_server_for_a && *submission == sealed_submission
    )));

    s.a.insert("users", user_insert_values(later_row_id, "Later"), None)
        .unwrap();

    s.a.park_sync_message(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchSettlement {
            settlement: crate::batch_fate::BatchSettlement::Missing { batch_id },
        },
    });
    s.a.batched_tick();

    let replay_outbox = s.a.sync_sender().take();
    assert!(
        replay_outbox.iter().any(|entry| matches!(
            &entry,
            OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::SealBatch { submission },
            } if *server_id == s.b_server_for_a && *submission == sealed_submission
        )),
        "expected Missing replay to resend the original sealed submission, got {replay_outbox:?}"
    );
    assert!(replay_outbox.iter().all(|entry| !matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(server_id),
            payload: SyncPayload::SealBatch { submission },
        } if *server_id == s.b_server_for_a
            && submission.batch_id == batch_id
            && submission.target_branch_name == branch_name
            && submission.members == vec![SealedBatchMember {
                object_id: row_id,
                row_digest,
            }]
            && submission.captured_frontier.iter().any(|member| member.object_id == later_row_id)
    )), "replayed seal should not recapture rows written after the batch was sealed");
}

#[test]
fn rc_update_persisted_resolves_on_ack() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    let mut receiver =
        s.a.update_persisted(
            id,
            vec![("name".into(), Value::Text("Bob".into()))],
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(Ok(()))) => {}
        Ok(Some(Err(rejection))) => panic!("Update receiver should not reject: {rejection:?}"),
        Ok(None) => panic!("Update receiver should be resolved after Local ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results[0].1[1], Value::Text("Bob".into()));
}

#[test]
fn rc_delete_persisted_resolves_on_ack() {
    let mut s = create_3tier_rc();
    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    pump_a_to_b(&mut s);

    let mut receiver =
        s.a.delete_persisted(id, None, DurabilityTier::Local)
            .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match receiver.try_recv() {
        Ok(Some(Ok(()))) => {}
        Ok(Some(Err(rejection))) => panic!("Delete receiver should not reject: {rejection:?}"),
        Ok(None) => panic!("Delete receiver should be resolved after Local ack"),
        Err(_) => panic!("Receiver was cancelled"),
    }

    let query = Query::new("users");
    let results = execute_query(&mut s.b, query);
    assert_eq!(results.len(), 0);
}

#[test]
fn rc_multiple_persisted_inserts_independent() {
    let mut s = create_3tier_rc();

    let (_id1, mut receiver1) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    let (_id2, mut receiver2) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Bob"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    pump_3tier(&mut s);

    match receiver1.try_recv() {
        Ok(Some(Ok(()))) => {}
        Ok(Some(Err(rejection))) => panic!("receiver1 should not reject: {rejection:?}"),
        Ok(None) => panic!("receiver1 should be resolved"),
        Err(_) => panic!("receiver1 cancelled"),
    }
    match receiver2.try_recv() {
        Ok(Some(Ok(()))) => {}
        Ok(Some(Err(rejection))) => panic!("receiver2 should not reject: {rejection:?}"),
        Ok(None) => panic!("receiver2 should be resolved"),
        Err(_) => panic!("receiver2 cancelled"),
    }
}
