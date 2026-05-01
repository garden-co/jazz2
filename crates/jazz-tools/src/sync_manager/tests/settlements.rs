use super::*;

#[test]
fn initial_query_sync_replays_current_direct_batch_settlement() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let mut row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    row.confirmed_tier = Some(DurabilityTier::Local);

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

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if *id == client_id && *settlement == BatchSettlement::DurableDirect {
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
fn initial_query_sync_sends_only_current_row_for_deep_history() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let older = visible_row(row_id, "main", Vec::new(), 1_000, b"older");
    let newer = visible_row(row_id, "main", vec![older.batch_id()], 2_000, b"newer");

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_users_schema(&mut io);
    create_test_row_with_id(&mut io, row_id, Some(row_metadata("users")));
    io.append_history_region_rows("users", &[older.clone(), newer.clone()])
        .unwrap();
    io.upsert_visible_region_rows(
        "users",
        std::slice::from_ref(&VisibleRowEntry::rebuild(
            newer.clone(),
            &[older.clone(), newer.clone()],
        )),
    )
    .unwrap();

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let row_payloads: Vec<_> = sm
        .take_outbox()
        .into_iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::RowBatchNeeded { row, .. },
            } if id == client_id => Some(row),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_payloads.len(),
        1,
        "initial sync should send only the current row"
    );
    assert_eq!(row_payloads[0].batch_id(), newer.batch_id());
    assert_eq!(row_payloads[0].data, newer.data);
    assert!(
        row_payloads[0].parents.is_empty(),
        "initial sync payload should be self-contained for subscribers"
    );
}

#[test]
fn initial_query_sync_replays_current_accepted_transaction_settlement() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        Some(DurabilityTier::Local),
    );

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

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if *id == client_id && *settlement == BatchSettlement::AcceptedTransaction {
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
fn batch_settlement_needed_returns_current_accepted_transaction() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        Some(DurabilityTier::Local),
    );

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    persist_visible_row_settlement(&mut io, row_id, &row);

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchSettlementNeeded {
            batch_ids: vec![row.batch_id],
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
fn batch_settlement_needed_returns_missing_without_persisted_visible_settlement() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let mut row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    row.confirmed_tier = Some(DurabilityTier::Local);

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchSettlementNeeded {
            batch_ids: vec![row.batch_id],
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if id == client_id && settlement == BatchSettlement::Missing { batch_id: row.batch_id }
    )));
}

#[test]
fn batch_settlement_needed_returns_missing_for_unknown_batch() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchSettlementNeeded {
            batch_ids: vec![batch_id],
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if id == client_id && settlement == BatchSettlement::Missing { batch_id }
    )));
}

#[test]
fn batch_settlement_needed_returns_persisted_rejected_without_visible_rows() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let settlement = BatchSettlement::Rejected {
        batch_id,
        code: "permission_denied".to_string(),
        reason: "writer lacks publish rights".to_string(),
    };

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    io.upsert_authoritative_batch_settlement(&settlement)
        .unwrap();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchSettlementNeeded {
            batch_ids: vec![batch_id],
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement: returned },
        } if id == client_id && returned == settlement
    )));
}

#[test]
fn server_rejected_batch_settlement_is_forwarded_to_interested_client() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let settlement = BatchSettlement::Rejected {
        batch_id: row.batch_id,
        code: "permission_denied".to_string(),
        reason: "writer lacks publish rights".to_string(),
    };

    seed_users_schema(&mut io);
    add_client(&mut sm, &io, client_id);
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
            row,
        },
    );
    sm.take_outbox();

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
            payload: SyncPayload::BatchSettlement { settlement: returned },
        } if id == client_id && returned == settlement
    )));
}

#[test]
fn seal_batch_accepts_all_staged_transactional_rows_as_one_settlement() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let first_row_id = ObjectId::new();
    let second_row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(first_row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );
    let second_row = row_with_batch_state(
        visible_row(second_row_id, "main", Vec::new(), 1_100, b"bob"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    for row in [first_row.clone(), second_row.clone()] {
        sm.process_from_client(
            &mut io,
            client_id,
            SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: row.row_id,
                    metadata: row_metadata("users"),
                }),
                row,
            },
        );
    }
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![
                    SealedBatchMember {
                        object_id: first_row_id,
                        row_digest: first_row.content_digest(),
                    },
                    SealedBatchMember {
                        object_id: second_row_id,
                        row_digest: second_row.content_digest(),
                    },
                ],
                Vec::new(),
            ),
        },
    );

    let settlement = io
        .load_authoritative_batch_settlement(batch_id)
        .unwrap()
        .expect("sealed transactional batch should persist an authoritative settlement");
    let BatchSettlement::AcceptedTransaction {
        batch_id: settled_batch_id,
        confirmed_tier,
        visible_members,
    } = settlement
    else {
        panic!("expected accepted transactional settlement, got {settlement:?}");
    };
    assert_eq!(settled_batch_id, batch_id);
    assert_eq!(confirmed_tier, DurabilityTier::Local);
    assert_eq!(visible_members.len(), 2);
    assert!(visible_members.contains(&VisibleBatchMember {
        object_id: first_row_id,
        branch_name: BranchName::new("main"),
        batch_id,
    }));
    assert!(visible_members.contains(&VisibleBatchMember {
        object_id: second_row_id,
        branch_name: BranchName::new("main"),
        batch_id,
    }));

    let first_visible = io
        .load_visible_region_row("users", "main", first_row_id)
        .unwrap()
        .expect("first row should become visible after seal");
    let second_visible = io
        .load_visible_region_row("users", "main", second_row_id)
        .unwrap()
        .expect("second row should become visible after seal");
    assert_eq!(
        first_visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(
        second_visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement: returned },
        } if *id == client_id && *returned == BatchSettlement::AcceptedTransaction {
            batch_id,
            confirmed_tier: DurabilityTier::Local,
            visible_members: visible_members.clone(),
        }
    )));
}

#[test]
fn seal_batch_rejection_stops_when_settlement_persistence_fails() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = FailingHistoryPatchStorage::new();
    io.fail_authoritative_settlement_upsert = true;
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(io.inner_mut());

    sm.add_client_with_storage(&io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let staged_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: staged_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: staged_row.clone(),
        },
    );
    sm.take_outbox();

    let mut submission = sealed_submission(
        batch_id,
        "main",
        vec![SealedBatchMember {
            object_id: row_id,
            row_digest: staged_row.content_digest(),
        }],
        Vec::new(),
    );
    submission.batch_digest = crate::digest::Digest32([255; 32]);

    sm.process_from_client(&mut io, client_id, SyncPayload::SealBatch { submission });

    assert_eq!(
        io.load_authoritative_batch_settlement(batch_id).unwrap(),
        None
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "failed settlement persistence should not publish or reject the batch"
    );
    assert!(sm.take_outbox().is_empty());
}
