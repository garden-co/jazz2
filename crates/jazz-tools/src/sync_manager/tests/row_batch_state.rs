use super::*;

#[test]
fn row_batch_created_emits_batch_settlement_to_source() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    seed_users_schema(&mut io);

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(
                *settlement,
                BatchSettlement::DurableDirect {
                    batch_id: row.batch_id,
                    confirmed_tier: DurabilityTier::Local,
                    visible_members: vec![VisibleBatchMember {
                        object_id: row_id,
                        branch_name: BranchName::new("main"),
                        batch_id: row.batch_id,
                    }],
                }
            );
        }
        other => panic!("expected BatchSettlement to server, got {other:?}"),
    }
}

#[test]
fn row_batch_created_stamps_local_durability_into_storage() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    seed_users_schema(&mut io);

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row,
        },
    );

    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("visible row");
    let history = io
        .scan_history_region(
            "users",
            "main",
            crate::row_histories::HistoryScan::Row { row_id },
        )
        .unwrap();

    assert_eq!(visible.confirmed_tier, None);
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].confirmed_tier, None);
    assert_eq!(
        io.load_authoritative_batch_settlement(history[0].batch_id)
            .unwrap()
            .and_then(|settlement| settlement.confirmed_tier()),
        Some(DurabilityTier::EdgeServer)
    );
    assert_eq!(
        io.load_visible_region_row_for_tier("users", "main", row_id, DurabilityTier::EdgeServer)
            .unwrap()
            .expect("tiered visible row")
            .confirmed_tier,
        Some(DurabilityTier::EdgeServer)
    );
}

#[test]
fn row_batch_state_changed_updates_row_region_confirmed_tier_monotonically() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );
    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Local),
        },
    );

    let visible = io.scan_visible_region("users", "main").unwrap();
    let history = io
        .scan_history_region(
            "users",
            "main",
            crate::row_histories::HistoryScan::Row { row_id },
        )
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(history.len(), 1);
    // Tier-only acks are stored as authoritative batch settlements. They must
    // not force visible/history row rewrites.
    assert_eq!(visible[0].confirmed_tier, None);
    assert_eq!(history[0].confirmed_tier, None);
    assert_eq!(
        io.load_authoritative_batch_settlement(batch_id)
            .unwrap()
            .and_then(|settlement| settlement.confirmed_tier()),
        Some(DurabilityTier::EdgeServer)
    );
}

#[test]
fn row_batch_state_changed_tier_only_does_not_enqueue_pending_row_update() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    assert!(sm.take_pending_row_visibility_changes().is_empty());
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id)
            .unwrap()
            .expect("visible row")
            .confirmed_tier,
        None
    );
    assert_eq!(
        io.load_history_row_batch("users", "main", row_id, batch_id)
            .unwrap()
            .expect("history row")
            .confirmed_tier,
        None
    );
    assert_eq!(
        io.load_authoritative_batch_settlement(batch_id)
            .unwrap()
            .and_then(|settlement| settlement.confirmed_tier()),
        Some(DurabilityTier::EdgeServer)
    );
}

#[test]
fn row_batch_state_changed_tier_only_persists_authoritative_settlement() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;
    seed_visible_row(&mut sm, &mut io, "users", row);

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_settlement(batch_id).unwrap(),
        Some(BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::EdgeServer,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id,
            }],
        })
    );
}

#[test]
fn row_batch_state_changed_does_not_ack_when_storage_patch_fails() {
    let mut sm = SyncManager::new();
    let mut io = FailingHistoryPatchStorage::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;
    seed_visible_row(&mut sm, io.inner_mut(), "users", row.clone());
    io.fail_history_load = true;

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    assert!(sm.take_received_row_batch_acks().is_empty());
    assert!(sm.take_pending_row_visibility_changes().is_empty());
    let loaded = io
        .inner
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("visible row should remain present");
    assert_eq!(loaded.confirmed_tier, None);
}

#[test]
fn row_batch_state_changed_relays_to_clients_that_received_row_batch_needed() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let initial = sm.take_outbox();
    assert!(initial.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchNeeded { row: needed, .. },
        } if *id == client_id && needed.row_id == row_id
    )));

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Local),
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::RowBatchStateChanged {
                    row_id: changed_row_id,
                    batch_id: changed_batch_id,
                    confirmed_tier: Some(DurabilityTier::Local),
                    ..
                },
        } if id == client_id && changed_row_id == row_id && changed_batch_id == batch_id
    )));
}

#[test]
fn row_batch_state_changed_relays_direct_batch_settlement_to_interested_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let mut row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    row.confirmed_tier = Some(DurabilityTier::Local);
    let batch_id = row.batch_id;

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    persist_visible_row_settlement(&mut io, row_id, &row);

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );
    let _ = sm.take_outbox();

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Local),
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if id == client_id && settlement == BatchSettlement::DurableDirect {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Local,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id: row.batch_id,
            }],
        }
    )));
}

#[test]
fn batch_settlement_from_server_relays_to_row_batch_interested_writer() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;
    let branch_name = BranchName::new("main");
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    add_server(&mut sm, &io, server_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );
    sm.take_outbox();

    let settlement = BatchSettlement::DurableDirect {
        batch_id,
        confirmed_tier: DurabilityTier::EdgeServer,
        visible_members: vec![VisibleBatchMember {
            object_id: row_id,
            branch_name,
            batch_id,
        }],
    };
    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::BatchSettlement {
            settlement: settlement.clone(),
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement: relayed },
        } if id == client_id && relayed == settlement
    )));
}

#[test]
fn row_batch_state_changed_relays_accepted_transaction_settlement_to_interested_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        Some(DurabilityTier::Local),
    );
    let batch_id = row.batch_id;

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    persist_visible_row_settlement(&mut io, row_id, &row);

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );
    let _ = sm.take_outbox();

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Local),
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if id == client_id && settlement == BatchSettlement::AcceptedTransaction {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Local,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id: row.batch_id,
            }],
        }
    )));
}

#[test]
fn row_batch_state_changed_persists_accepted_transaction_tier_upgrade_authoritatively() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        Some(DurabilityTier::Local),
    );
    let batch_id = row.batch_id;

    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    io.upsert_authoritative_batch_settlement(&BatchSettlement::AcceptedTransaction {
        batch_id: row.batch_id,
        confirmed_tier: DurabilityTier::Local,
        visible_members: vec![VisibleBatchMember {
            object_id: row_id,
            branch_name: BranchName::new("main"),
            batch_id: row.batch_id,
        }],
    })
    .unwrap();

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_settlement(row.batch_id)
            .unwrap(),
        Some(BatchSettlement::AcceptedTransaction {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::EdgeServer,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id: row.batch_id,
            }],
        })
    );
}

#[test]
fn row_batch_state_changed_stops_relaying_after_scope_removal() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );
    let _ = sm.take_outbox();

    set_client_query_scope(&mut sm, &io, client_id, QueryId(1), HashSet::new(), None);
    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id: row.batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Local),
        },
    );

    assert!(sm.take_outbox().into_iter().all(|entry| !matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchStateChanged { row_id: changed_row_id, .. },
        } if id == client_id && changed_row_id == row_id
    )));
}

#[test]
fn client_row_batch_state_ack_does_not_leak_to_other_subscribers() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let alice = ClientId::new();
    let bob = ClientId::new();
    let row_id = ObjectId::new();
    let mut row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice-todo");
    row.confirmed_tier = Some(DurabilityTier::GlobalServer);
    let batch_id = row.batch_id;

    add_client(&mut sm, &io, alice);
    add_client(&mut sm, &io, bob);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    persist_visible_row_settlement(&mut io, row_id, &row);

    for (client_id, query_id) in [(alice, QueryId(1)), (bob, QueryId(2))] {
        set_client_query_scope(
            &mut sm,
            &io,
            client_id,
            query_id,
            HashSet::from([(row_id, BranchName::new("main"))]),
            None,
        );
    }

    let initial = sm.take_outbox();
    assert!(initial.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchNeeded { row: needed, .. },
        } if *id == alice && needed.row_id == row_id
    )));
    assert!(initial.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchNeeded { row: needed, .. },
        } if *id == bob && needed.row_id == row_id
    )));

    sm.process_from_client(
        &mut io,
        bob,
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Local),
        },
    );

    let outbox = sm.take_outbox();
    assert!(
        outbox.iter().all(|entry| !matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload:
                    SyncPayload::RowBatchStateChanged { .. } | SyncPayload::BatchSettlement { .. },
            } if *id == alice
        )),
        "alice should not receive bob's per-client row-batch ack: {outbox:#?}"
    );
}
