use super::*;

fn persist_direct_settlement_for_row(
    core: &mut TestCore,
    row: &crate::row_histories::StoredRowBatch,
    tier: DurabilityTier,
) {
    core.storage_mut()
        .upsert_authoritative_batch_settlement(&crate::batch_fate::BatchSettlement::DurableDirect {
            batch_id: row.batch_id,
            confirmed_tier: tier,
            visible_members: vec![crate::batch_fate::VisibleBatchMember {
                object_id: row.row_id,
                branch_name: BranchName::new(&row.branch),
                batch_id: row.batch_id,
            }],
        })
        .unwrap();
}

#[test]
fn rc_query_no_settled_tier_immediate() {
    let mut s = create_3tier_rc();

    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    let mut future = s.a.query(Query::new("users"), None);

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 1, "Should have one row");
            assert_eq!(results[0].0, id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query with settled_tier=None should resolve immediately"),
    }
}

#[test]
fn rc_query_settled_tier_holds() {
    let mut s = create_3tier_rc();

    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Local),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should be pending before Local settlement"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 1, "Should have one row after settlement");
            assert_eq!(results[0].0, id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after Worker QuerySettled"),
    }
}

#[test]
fn rc_query_remote_tier_immediate_local_updates_falls_back_to_local_pending_row() {
    let mut s = create_3tier_rc();

    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::EdgeServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should wait for the initial remote frontier"
    );

    // Local frontier completion is not enough for an EdgeServer read.
    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should ignore a lower-tier frontier"
    );

    // Once the requested tier settles, immediate local updates can overlay the
    // locally-authored row even though the row itself is still pending.
    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(results.len(), 1, "Should have one locally pending row");
            assert_eq!(results[0].0, id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve once the EdgeServer frontier is complete"),
    }
}

#[test]
fn rc_query_remote_tier_immediate_local_updates_survives_empty_remote_scope_snapshot() {
    let mut s = create_3tier_rc();

    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    // Keep the row local-only so B replies with an empty remote scope snapshot.
    s.a.batched_tick();
    s.a.sync_sender().take();

    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::EdgeServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should wait for the initial remote frontier"
    );

    s.a.batched_tick();
    let a_out = s.a.sync_sender().take();
    for entry in a_out {
        if entry.destination == Destination::Server(s.b_server_for_a)
            && matches!(entry.payload, SyncPayload::QuerySubscription { .. })
        {
            s.b.park_sync_message(InboxEntry {
                source: Source::Client(s.a_client_of_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();

    let b_out = s.b.sync_sender().take();
    assert!(
        b_out.iter().any(|entry| matches!(
            entry.payload,
            SyncPayload::QuerySettled { ref scope, .. } if scope.is_empty()
        )),
        "Expected an empty settled remote scope from B"
    );
    for entry in b_out {
        match entry.destination {
            Destination::Client(id) if id == s.a_client_of_b => {
                s.a.park_sync_message(InboxEntry {
                    source: Source::Server(s.b_server_for_a),
                    payload: entry.payload,
                });
            }
            Destination::Server(id) if id == s.c_server_for_b => {
                s.c.park_sync_message(InboxEntry {
                    source: Source::Client(s.b_client_of_c),
                    payload: entry.payload,
                });
            }
            _ => {}
        }
    }
    s.a.batched_tick();
    s.a.immediate_tick();
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should ignore B's lower-tier empty frontier"
    );

    s.c.batched_tick();
    s.c.immediate_tick();
    pump_c_to_b_to_a(&mut s);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(
                results.len(),
                1,
                "Immediate local updates should keep the local row visible"
            );
            assert_eq!(results[0].0, id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after EdgeServer frontier completion"),
    }
}

#[test]
fn rc_query_local_transaction_overlay_shows_only_the_requested_staged_insert() {
    let mut core = create_runtime_with_schema(test_schema(), "query-local-transaction-overlay");
    let branch_name = core.schema_manager().branch_name();

    let alice_batch = BatchId::new();
    let bob_batch = BatchId::new();

    let alice_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: Some(alice_batch),
        target_branch_name: None,
    };
    let bob_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: Some(bob_batch),
        target_branch_name: None,
    };

    let ((alice_id, _), _) = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "alice-draft"),
            Some(&alice_context),
        )
        .unwrap();
    let ((_bob_id, _), _) = core
        .insert(
            "users",
            user_insert_values(ObjectId::new(), "bob-draft"),
            Some(&bob_context),
        )
        .unwrap();

    let alice_rows = execute_runtime_query_with_local_overlay(
        &mut core,
        Query::new("users"),
        None,
        ReadDurabilityOptions::default(),
        crate::sync_manager::QueryPropagation::Full,
        QueryLocalOverlay {
            batch_id: alice_batch,
            branch_name,
            row_ids: vec![alice_id],
        },
    );

    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].0, alice_id);
    assert_eq!(alice_rows[0].1[1], Value::Text("alice-draft".into()));

    let visible_rows = execute_runtime_query(&mut core, Query::new("users"), None);
    assert_eq!(
        visible_rows,
        Vec::<(ObjectId, Vec<Value>)>::new(),
        "ordinary reads should not see staged transactional rows"
    );
}

#[test]
fn rc_query_local_transaction_overlay_keeps_same_row_updates_isolated_by_batch() {
    let mut core = create_runtime_with_schema(test_schema(), "query-local-transaction-same-row");
    let branch_name = core.schema_manager().branch_name();

    let ((row_id, _), _) = core
        .insert("users", user_insert_values(ObjectId::new(), "shared"), None)
        .unwrap();

    let alice_batch = BatchId::new();
    let bob_batch = BatchId::new();

    let alice_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: Some(alice_batch),
        target_branch_name: None,
    };
    let bob_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: Some(bob_batch),
        target_branch_name: None,
    };

    core.update(
        row_id,
        vec![("name".into(), Value::Text("alice-draft".into()))],
        Some(&alice_context),
    )
    .unwrap();
    core.update(
        row_id,
        vec![("name".into(), Value::Text("bob-draft".into()))],
        Some(&bob_context),
    )
    .unwrap();

    let visible_rows = execute_runtime_query(&mut core, Query::new("users"), None);
    assert_eq!(visible_rows.len(), 1);
    assert_eq!(visible_rows[0].1[1], Value::Text("shared".into()));

    let alice_rows = execute_runtime_query_with_local_overlay(
        &mut core,
        Query::new("users"),
        None,
        ReadDurabilityOptions::default(),
        crate::sync_manager::QueryPropagation::Full,
        QueryLocalOverlay {
            batch_id: alice_batch,
            branch_name: branch_name.clone(),
            row_ids: vec![row_id],
        },
    );
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].1[1], Value::Text("alice-draft".into()));

    let bob_rows = execute_runtime_query_with_local_overlay(
        &mut core,
        Query::new("users"),
        None,
        ReadDurabilityOptions::default(),
        crate::sync_manager::QueryPropagation::Full,
        QueryLocalOverlay {
            batch_id: bob_batch,
            branch_name,
            row_ids: vec![row_id],
        },
    );
    assert_eq!(bob_rows.len(), 1);
    assert_eq!(bob_rows[0].1[1], Value::Text("bob-draft".into()));
}

#[test]
fn rc_query_remote_tier_session_exists_rel_waits_without_permissions_head() {
    let schema = session_exists_rel_teams_schema();
    let mut client = create_runtime_with_schema(schema, "session-exists-rel-query");
    let mut server = create_runtime_with_schema_and_sync_manager(
        structural_session_exists_rel_teams_schema(),
        "session-exists-rel-query",
        SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer),
    );
    server
        .schema_manager_mut()
        .query_manager_mut()
        .require_authorization_schema();

    let alice_session = Session::new("alice");
    let client_id = ClientId::new();
    let server_id = ServerId::new();

    server.add_client(client_id, Some(alice_session.clone()));
    client.add_server(server_id);

    client.batched_tick();
    server.batched_tick();
    client.sync_sender().take();
    server.sync_sender().take();

    let ((team_id, _row_values), _) = client
        .insert(
            "teams",
            HashMap::from([("name".to_string(), Value::Text("Alice".into()))]),
            None,
        )
        .unwrap();
    client
        .insert(
            "user_team_edges",
            HashMap::from([
                ("user_id".to_string(), Value::Text("alice".into())),
                ("team_id".to_string(), Value::Uuid(team_id)),
            ]),
            None,
        )
        .unwrap();

    // Keep the policy context row local-only so the query must honor immediate
    // local updates instead of relying on an upstream scope snapshot.
    client.batched_tick();
    client.sync_sender().take();

    let mut future = client.query_with_propagation(
        Query::new("teams"),
        Some(alice_session),
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::EdgeServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should wait for the initial remote frontier"
    );

    client.batched_tick();
    let client_outbox = client.sync_sender().take();
    for entry in client_outbox {
        if entry.destination == Destination::Server(server_id)
            && matches!(entry.payload, SyncPayload::QuerySubscription { .. })
        {
            server.park_sync_message(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            });
        }
    }

    server.batched_tick();
    server.immediate_tick();

    let server_outbox = server.sync_sender().take();
    assert!(
        !server_outbox
            .iter()
            .any(|entry| matches!(entry.payload, SyncPayload::QuerySettled { .. })),
        "server without a published permissions head should not settle an authoritative session query"
    );
    for entry in server_outbox {
        if entry.destination == Destination::Client(client_id) {
            client.park_sync_message(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
    }

    client.batched_tick();
    client.immediate_tick();

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => panic!(
            "Query should remain pending until the server can authorize scope, got {results:?}"
        ),
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => {}
    }
}

#[test]
fn rc_query_remote_tier_backend_client_session_exists_rel_waits_without_permissions_head() {
    let schema = session_exists_rel_teams_schema();
    let mut client = create_runtime_with_schema(schema, "backend-session-exists-rel-query");
    let mut server = create_runtime_with_schema_and_sync_manager(
        structural_session_exists_rel_teams_schema(),
        "backend-session-exists-rel-query",
        SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer),
    );
    server
        .schema_manager_mut()
        .query_manager_mut()
        .require_authorization_schema();

    let alice_session = Session::new("alice");
    let client_id = ClientId::new();
    let server_id = ServerId::new();

    server.add_client(client_id, Some(alice_session.clone()));
    server
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Backend);
    client.add_server(server_id);

    client.batched_tick();
    server.batched_tick();
    client.sync_sender().take();
    server.sync_sender().take();

    let ((team_id, _row_values), _) = client
        .insert(
            "teams",
            HashMap::from([("name".to_string(), Value::Text("Alice".into()))]),
            None,
        )
        .unwrap();
    client
        .insert(
            "user_team_edges",
            HashMap::from([
                ("user_id".to_string(), Value::Text("alice".into())),
                ("team_id".to_string(), Value::Uuid(team_id)),
            ]),
            None,
        )
        .unwrap();

    client.batched_tick();
    client.sync_sender().take();

    let mut future = client.query_with_propagation(
        Query::new("teams"),
        Some(alice_session),
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::EdgeServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should wait for the initial remote frontier"
    );

    client.batched_tick();
    let client_outbox = client.sync_sender().take();
    for entry in client_outbox {
        if entry.destination == Destination::Server(server_id)
            && matches!(entry.payload, SyncPayload::QuerySubscription { .. })
        {
            server.park_sync_message(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            });
        }
    }

    server.batched_tick();
    server.immediate_tick();

    let server_outbox = server.sync_sender().take();
    assert!(
        !server_outbox
            .iter()
            .any(|entry| matches!(entry.payload, SyncPayload::QuerySettled { .. })),
        "server without a published permissions head should not settle an authoritative backend session query"
    );
    for entry in server_outbox {
        if entry.destination == Destination::Client(client_id) {
            client.park_sync_message(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
    }

    client.batched_tick();
    client.immediate_tick();

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => panic!(
            "Query should remain pending until the server can authorize backend scope, got {results:?}"
        ),
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => {}
    }
}

#[test]
fn rc_query_remote_tier_backend_client_session_exists_rel_waits_for_synced_policy_rows_without_permissions_head()
 {
    let schema = session_exists_rel_teams_schema();
    let mut client = create_runtime_with_schema(schema, "backend-session-exists-rel-synced");
    let mut server = create_runtime_with_schema_and_sync_manager(
        structural_session_exists_rel_teams_schema(),
        "backend-session-exists-rel-synced",
        SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer),
    );
    server
        .schema_manager_mut()
        .query_manager_mut()
        .require_authorization_schema();

    let alice_session = Session::new("alice");
    let client_id = ClientId::new();
    let server_id = ServerId::new();

    server.add_client(client_id, Some(alice_session.clone()));
    server
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Backend);
    client.add_server(server_id);

    client.batched_tick();
    server.batched_tick();
    client.sync_sender().take();
    server.sync_sender().take();

    let ((team_id, _row_values), _) = client
        .insert(
            "teams",
            HashMap::from([("name".to_string(), Value::Text("Alice".into()))]),
            None,
        )
        .unwrap();
    client
        .insert(
            "user_team_edges",
            HashMap::from([
                ("user_id".to_string(), Value::Text("alice".into())),
                ("team_id".to_string(), Value::Uuid(team_id)),
            ]),
            None,
        )
        .unwrap();

    pump_client_messages_to_server(&mut client, &mut server, server_id, client_id);
    let server_outbox = server.sync_sender().take();
    assert!(
        !server_outbox
            .iter()
            .any(|entry| matches!(entry.payload, SyncPayload::QuerySettled { .. })),
        "server without a published permissions head should not settle an authoritative backend session query even when policy rows synced"
    );
    for entry in server_outbox {
        if entry.destination == Destination::Client(client_id) {
            client.park_sync_message(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
    }
    client.batched_tick();
    client.immediate_tick();

    let mut future = client.query_with_propagation(
        Query::new("teams"),
        Some(alice_session),
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::EdgeServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should wait for the initial remote frontier"
    );

    client.batched_tick();
    let client_outbox = client.sync_sender().take();
    for entry in client_outbox {
        if entry.destination == Destination::Server(server_id)
            && matches!(entry.payload, SyncPayload::QuerySubscription { .. })
        {
            server.park_sync_message(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            });
        }
    }

    server.batched_tick();
    server.immediate_tick();

    for entry in server.sync_sender().take() {
        if entry.destination == Destination::Client(client_id) {
            client.park_sync_message(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
    }

    client.batched_tick();
    client.immediate_tick();

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => panic!(
            "Query should remain pending until the server can authorize synced policy scope, got {results:?}"
        ),
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => {}
    }
}

#[test]
fn rc_query_settled_tier_empty_resolves() {
    let mut s = create_3tier_rc();

    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Local),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should be pending before Local settlement"
    );

    // No rows inserted anywhere; query should still resolve once settled tier is reached.
    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(
                results.len(),
                0,
                "Settled query with no rows should resolve to empty result"
            );
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after Worker QuerySettled"),
    }
}

#[test]
fn query_reads_pick_row_batches_by_required_durability_tier() {
    let mut core = create_runtime_with_schema_and_sync_manager(
        test_schema(),
        "tier-aware-visible-row",
        SyncManager::new(),
    );
    let branch_name = core.schema_manager().branch_name().to_string();

    // Row history:
    //   v1 --(global)--> visible for global queries
    //    \
    //     `-- v2 --(worker)--> current head for worker queries
    let row_id = ObjectId::new();
    let ((object_id, _), _) = core
        .insert("users", user_insert_values(row_id, "Alice-global"), None)
        .unwrap();
    core.immediate_tick();

    let first_visible = core
        .storage()
        .load_visible_region_row("users", &branch_name, object_id)
        .unwrap()
        .expect("first visible row");
    persist_direct_settlement_for_row(&mut core, &first_visible, DurabilityTier::GlobalServer);

    core.update(
        object_id,
        vec![("name".into(), Value::Text("Alice-worker".into()))],
        None,
    )
    .unwrap();
    core.immediate_tick();

    let second_visible = core
        .storage()
        .load_visible_region_row("users", &branch_name, object_id)
        .unwrap()
        .expect("second visible row");
    persist_direct_settlement_for_row(&mut core, &second_visible, DurabilityTier::Local);

    let worker_rows = execute_runtime_query_with_durability_and_propagation(
        &mut core,
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Local),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::LocalOnly,
    );
    let global_rows = execute_runtime_query_with_durability_and_propagation(
        &mut core,
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::GlobalServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::LocalOnly,
    );

    assert_eq!(
        worker_rows,
        vec![(object_id, user_row_values(row_id, "Alice-worker"))]
    );
    assert_eq!(
        global_rows,
        vec![(object_id, user_row_values(row_id, "Alice-global"))]
    );
}

#[test]
fn query_reads_merge_conflicting_row_batches_by_required_durability_tier() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build();
    let mut core = create_runtime_with_schema_and_sync_manager(
        schema.clone(),
        "tier-aware-merged-row",
        SyncManager::new(),
    );
    let branch_name = core.schema_manager().branch_name().to_string();
    let descriptor = &schema[&TableName::new("todos")].columns;

    let ((row_id, _row_values), _) = core
        .insert(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("base".into())),
                ("done".to_string(), Value::Boolean(false)),
            ]),
            None,
        )
        .unwrap();
    core.immediate_tick();

    let base = core
        .storage()
        .load_visible_region_row("todos", &branch_name, row_id)
        .unwrap()
        .expect("base visible row");
    persist_direct_settlement_for_row(&mut core, &base, DurabilityTier::GlobalServer);
    let base = core
        .storage()
        .load_visible_region_row("todos", &branch_name, row_id)
        .unwrap()
        .expect("patched base visible row");

    let edge_title = crate::row_histories::StoredRowBatch::new(
        row_id,
        branch_name.clone(),
        vec![base.batch_id()],
        encode_row(
            descriptor,
            &[Value::Text("edge-title".into()), Value::Boolean(false)],
        )
        .unwrap(),
        crate::metadata::RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
        HashMap::new(),
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
    let worker_done = crate::row_histories::StoredRowBatch::new(
        row_id,
        branch_name.clone(),
        vec![base.batch_id()],
        encode_row(
            descriptor,
            &[Value::Text("base".into()), Value::Boolean(true)],
        )
        .unwrap(),
        crate::metadata::RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
        HashMap::new(),
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
    persist_direct_settlement_for_row(&mut core, &edge_title, DurabilityTier::EdgeServer);
    persist_direct_settlement_for_row(&mut core, &worker_done, DurabilityTier::Local);

    core.storage_mut()
        .append_history_region_rows("todos", &[edge_title.clone(), worker_done.clone()])
        .unwrap();
    core.storage_mut()
        .upsert_visible_region_rows(
            "todos",
            std::slice::from_ref(
                &crate::row_histories::VisibleRowEntry::rebuild_with_descriptor(
                    descriptor,
                    &[base.clone(), edge_title.clone(), worker_done.clone()],
                )
                .unwrap()
                .expect("merged visible entry"),
            ),
        )
        .unwrap();

    let worker_preview = core
        .storage()
        .load_visible_region_row_for_tier("todos", &branch_name, row_id, DurabilityTier::Local)
        .unwrap()
        .expect("worker preview");
    let edge_preview = core
        .storage()
        .load_visible_region_row_for_tier("todos", &branch_name, row_id, DurabilityTier::EdgeServer)
        .unwrap()
        .expect("edge preview");
    let global_preview = core
        .storage()
        .load_visible_region_row_for_tier(
            "todos",
            &branch_name,
            row_id,
            DurabilityTier::GlobalServer,
        )
        .unwrap()
        .expect("global preview");
    assert_eq!(
        decode_row(descriptor, &worker_preview.data).unwrap(),
        vec![Value::Text("edge-title".into()), Value::Boolean(true)]
    );
    assert_eq!(
        decode_row(descriptor, &edge_preview.data).unwrap(),
        vec![Value::Text("edge-title".into()), Value::Boolean(false)]
    );
    assert_eq!(
        decode_row(descriptor, &global_preview.data).unwrap(),
        vec![Value::Text("base".into()), Value::Boolean(false)]
    );

    let worker_rows = execute_runtime_query_with_durability_and_propagation(
        &mut core,
        Query::new("todos"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Local),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::LocalOnly,
    );
    let edge_rows = execute_runtime_query_with_durability_and_propagation(
        &mut core,
        Query::new("todos"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::EdgeServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::LocalOnly,
    );
    let global_rows = execute_runtime_query_with_durability_and_propagation(
        &mut core,
        Query::new("todos"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::GlobalServer),
            local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
        },
        crate::sync_manager::QueryPropagation::LocalOnly,
    );

    assert_eq!(
        worker_rows,
        vec![(
            row_id,
            vec![Value::Text("edge-title".into()), Value::Boolean(true)]
        )]
    );
    assert_eq!(
        edge_rows,
        vec![(
            row_id,
            vec![Value::Text("edge-title".into()), Value::Boolean(false)]
        )]
    );
    assert_eq!(
        global_rows,
        vec![(
            row_id,
            vec![Value::Text("base".into()), Value::Boolean(false)]
        )]
    );
}

#[test]
fn rc_query_settled_before_data_should_not_drop_upstream_rows() {
    let mut s = create_3tier_rc();

    // Seed data on server B that client A has not synced yet.
    let ((row_id, _row_values), _) =
        s.b.insert(
            "users",
            user_insert_values(ObjectId::new(), "upstream-row"),
            None,
        )
        .unwrap();
    s.b.immediate_tick();
    s.b.batched_tick();
    s.b.sync_sender().take();

    // One-shot settled query on A should wait for Local settlement.
    let mut future = s.a.query_with_propagation(
        Query::new("users"),
        None,
        ReadDurabilityOptions {
            tier: Some(DurabilityTier::Local),
            local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
        },
        crate::sync_manager::QueryPropagation::Full,
    );

    let waker = noop_waker();
    let mut cx = std::task::Context::from_waker(&waker);
    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should be pending before Local settlement"
    );

    // Deliver A -> B query subscription and let B compute response traffic.
    pump_a_to_b(&mut s);
    s.b.batched_tick();
    let b_out = s.b.sync_sender().take();

    // Force QuerySettled before row delivery to expose ordering assumptions.
    let mut settled_to_a = Vec::new();
    let mut rows_to_a = Vec::new();
    let mut durability_to_a = Vec::new();
    for entry in b_out {
        if entry.destination != Destination::Client(s.a_client_of_b) {
            continue;
        }
        match entry.payload {
            payload @ SyncPayload::QuerySettled { .. } => settled_to_a.push(payload),
            payload @ SyncPayload::RowBatchNeeded { .. } => rows_to_a.push(payload),
            payload @ SyncPayload::RowBatchStateChanged { .. }
            | payload @ SyncPayload::BatchSettlement { .. } => durability_to_a.push(payload),
            _ => {}
        }
    }

    assert!(
        !settled_to_a.is_empty(),
        "Expected QuerySettled notification for A"
    );
    assert!(!rows_to_a.is_empty(), "Expected row payload for A");
    // Mirror connected stream initialization: first expected seq is 1.
    s.a.set_next_expected_server_sequence(s.b_server_for_a, 1);

    let mut next_update_seq = 1u64;
    let settled_seq_base = (rows_to_a.len() + durability_to_a.len()) as u64 + 1;

    for (idx, payload) in settled_to_a.into_iter().enumerate() {
        s.a.park_sync_message_with_sequence(
            InboxEntry {
                source: Source::Server(s.b_server_for_a),
                payload,
            },
            settled_seq_base + idx as u64,
        );
    }
    s.a.batched_tick();
    s.a.immediate_tick();

    assert!(
        Pin::new(&mut future).poll(&mut cx).is_pending(),
        "Query should stay pending until lower sequence row payload arrives"
    );

    for payload in rows_to_a {
        s.a.park_sync_message_with_sequence(
            InboxEntry {
                source: Source::Server(s.b_server_for_a),
                payload,
            },
            next_update_seq,
        );
        next_update_seq += 1;
    }
    for payload in durability_to_a {
        s.a.park_sync_message_with_sequence(
            InboxEntry {
                source: Source::Server(s.b_server_for_a),
                payload,
            },
            next_update_seq,
        );
        next_update_seq += 1;
    }
    s.a.batched_tick();
    s.a.immediate_tick();

    match Pin::new(&mut future).poll(&mut cx) {
        Poll::Ready(Ok(results)) => {
            assert_eq!(
                results.len(),
                1,
                "Sequenced delivery should prevent settled-before-data resolution"
            );
            assert_eq!(results[0].0, row_id);
        }
        Poll::Ready(Err(e)) => panic!("Query failed: {:?}", e),
        Poll::Pending => panic!("Query should resolve after row payload and QuerySettled"),
    }
}

#[test]
fn rc_subscribe_settled_tier() {
    let mut s = create_3tier_rc();

    let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                received_clone.lock().unwrap().push(rows);
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::Local),
                local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();
    s.a.immediate_tick();

    assert!(
        received.lock().unwrap().is_empty(),
        "Callback should not fire before Local settlement"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    let calls = received.lock().unwrap();
    assert!(
        !calls.is_empty(),
        "Callback should fire after Worker QuerySettled"
    );
    assert!(
        calls
            .iter()
            .any(|delivery| delivery.iter().any(|(row_id, _)| *row_id == id)),
        "Should eventually deliver the locally inserted row after Worker settlement"
    );
}

#[test]
fn rc_subscribe_global_tier_ignores_lower_tier_query_settled() {
    let mut s = create_3tier_rc();

    let received = Arc::new(Mutex::new(Vec::<SubscriptionDelta>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                received_clone.lock().unwrap().push(delta);
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::GlobalServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    s.a.park_sync_message(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::QuerySettled {
            query_id: crate::sync_manager::QueryId(0),
            tier: DurabilityTier::Local,
            scope: Vec::<(ObjectId, crate::object::BranchName)>::new(),
            through_seq: 0,
        },
    });
    s.a.batched_tick();
    s.a.immediate_tick();

    assert!(
        received.lock().unwrap().is_empty(),
        "Local QuerySettled must not release a Global-tier subscription"
    );

    s.a.park_sync_message(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::QuerySettled {
            query_id: crate::sync_manager::QueryId(0),
            tier: DurabilityTier::GlobalServer,
            scope: Vec::<(ObjectId, crate::object::BranchName)>::new(),
            through_seq: 0,
        },
    });
    s.a.batched_tick();
    s.a.immediate_tick();

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "Global QuerySettled should release the initial empty snapshot"
    );
    assert!(calls[0].ordered_delta.added.is_empty());
}

#[test]
fn rc_subscribe_remote_tier_immediate_local_updates() {
    let mut s = create_3tier_rc();

    let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                received_clone.lock().unwrap().push(rows);
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    // Initial delivery should still wait for the initial remote frontier.
    let ((first_id, _row_values), _) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "local-first"),
            None,
        )
        .unwrap();
    s.a.immediate_tick();

    let calls = received.lock().unwrap();
    assert!(
        calls.is_empty(),
        "Initial delivery should wait for query frontier completion"
    );
    drop(calls);

    // Local frontier completion is not enough to unblock an EdgeServer read.
    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);
    let calls = received.lock().unwrap();
    assert!(
        calls.is_empty(),
        "Local frontier completion should not release the EdgeServer subscription"
    );
    drop(calls);

    // Once EdgeServer settles, the first snapshot can include the local row via
    // the immediate local-update overlay.
    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);
    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "First callback should happen after EdgeServer frontier completion"
    );
    assert_eq!(
        calls[0].len(),
        1,
        "First callback should contain the local row"
    );
    assert_eq!(calls[0][0].0, first_id);
    drop(calls);

    // After initial delivery, local updates should callback immediately.
    let ((second_id, _row_values), _) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "local-second"),
            None,
        )
        .unwrap();
    s.a.immediate_tick();

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        2,
        "Second local write should trigger immediate callback"
    );
    let second_delivery = &calls[1];
    assert_eq!(
        second_delivery.len(),
        1,
        "Second callback should contain one added row"
    );
    assert_eq!(second_delivery[0].0, second_id);
}

#[test]
fn rc_subscribe_remote_tier_immediate_local_updates_survives_empty_remote_scope_snapshot() {
    let mut s = create_3tier_rc();

    let ((id, _row_values), _) =
        s.a.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
            .unwrap();

    // Keep the row local-only so B replies with an empty remote scope snapshot.
    s.a.batched_tick();
    s.a.sync_sender().take();

    let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                received_clone
                    .lock()
                    .unwrap()
                    .push(decode_added_rows(&delta));
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    s.a.batched_tick();
    let a_out = s.a.sync_sender().take();
    for entry in a_out {
        if entry.destination == Destination::Server(s.b_server_for_a)
            && matches!(entry.payload, SyncPayload::QuerySubscription { .. })
        {
            s.b.park_sync_message(InboxEntry {
                source: Source::Client(s.a_client_of_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();

    let b_out = s.b.sync_sender().take();
    assert!(
        b_out.iter().any(|entry| matches!(
            entry.payload,
            SyncPayload::QuerySettled { ref scope, .. } if scope.is_empty()
        )),
        "Expected an empty settled remote scope from B"
    );
    for entry in b_out {
        match entry.destination {
            Destination::Client(id) if id == s.a_client_of_b => {
                s.a.park_sync_message(InboxEntry {
                    source: Source::Server(s.b_server_for_a),
                    payload: entry.payload,
                });
            }
            Destination::Server(id) if id == s.c_server_for_b => {
                s.c.park_sync_message(InboxEntry {
                    source: Source::Client(s.b_client_of_c),
                    payload: entry.payload,
                });
            }
            _ => {}
        }
    }
    s.a.batched_tick();
    s.a.immediate_tick();
    assert!(
        received.lock().unwrap().is_empty(),
        "B's lower-tier empty frontier should not release the EdgeServer subscription"
    );

    s.c.batched_tick();
    s.c.immediate_tick();
    pump_c_to_b_to_a(&mut s);

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "EdgeServer frontier completion should emit one initial snapshot"
    );
    assert_eq!(
        calls[0].len(),
        1,
        "Initial snapshot should keep the local row"
    );
    assert_eq!(calls[0][0].0, id);
}

#[test]
fn rc_transaction_visible_subscription_can_overlay_local_pending_batch() {
    // alice strict-subscribes at EdgeServer durability
    //   alice stages one transactional row locally
    //   the row should appear via alice's local pending overlay once EdgeServer settles
    let mut s = create_3tier_rc();

    let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                let rows = decode_added_rows(&delta);
                received_clone.lock().unwrap().push(rows);
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    let ((row_id, _row_values), _) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "alice-pending"),
            Some(&write_context),
        )
        .unwrap();
    s.a.immediate_tick();

    assert!(
        received.lock().unwrap().is_empty(),
        "initial delivery should still wait for the first upstream frontier"
    );

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    assert!(
        received.lock().unwrap().is_empty(),
        "local frontier completion should not unblock an EdgeServer subscription"
    );

    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "EdgeServer frontier completion should unblock the first snapshot"
    );
    assert_eq!(
        calls[0].len(),
        1,
        "alice should see her own staged transactional row through the local overlay"
    );
    assert_eq!(calls[0][0].0, row_id);
    drop(calls);
}

#[test]
fn rc_transaction_visible_subscription_removes_local_pending_overlay_when_rejected() {
    // alice strict-subscribes at EdgeServer durability
    //   EdgeServer frontier first opens the subscription with an empty snapshot
    //   alice then stages one transactional row locally
    //   the row appears only through the local pending overlay
    //   a replayable rejected batch settlement should remove that overlay immediately
    let mut s = create_3tier_rc();

    let received = Arc::new(Mutex::new(Vec::<SubscriptionDelta>::new()));
    let received_clone = received.clone();

    let _handle =
        s.a.subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                received_clone.lock().unwrap().push(delta);
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::EdgeServer),
                local_updates: crate::query_manager::manager::LocalUpdates::Immediate,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    {
        let calls = received.lock().unwrap();
        assert_eq!(calls.len(), 0);
    }

    pump_b_to_c(&mut s);
    pump_c_to_b_to_a(&mut s);

    {
        let calls = received.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].ordered_delta.added.is_empty());
        assert!(calls[0].ordered_delta.removed.is_empty());
    }

    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    let ((row_id, _row_values), _) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "alice-pending"),
            Some(&write_context),
        )
        .unwrap();
    s.a.immediate_tick();

    let history_rows =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;

    {
        let calls = received.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(decode_added_rows(&calls[1]).len(), 1);
        assert_eq!(decode_added_rows(&calls[1])[0].0, row_id);
    }

    s.a.seal_batch(batch_id).unwrap();

    s.a.park_sync_message(InboxEntry {
        source: Source::Server(s.b_server_for_a),
        payload: SyncPayload::BatchSettlement {
            settlement: crate::batch_fate::BatchSettlement::Rejected {
                batch_id,
                code: "permission_denied".to_string(),
                reason: "writer lacks publish rights".to_string(),
            },
        },
    });
    s.a.batched_tick();

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        3,
        "rejected settlement should trigger a removal callback after the local overlay add"
    );
    assert_eq!(calls[2].ordered_delta.added.len(), 0);
    assert_eq!(calls[2].ordered_delta.updated.len(), 0);
    assert_eq!(calls[2].ordered_delta.removed.len(), 1);
    assert_eq!(calls[2].ordered_delta.removed[0].id, row_id);
}

#[test]
fn rc_transaction_visible_subscription_hides_partial_accepted_batch_until_scope_complete() {
    // alice authors one transactional batch with two rows
    //   worker accepts it and reports both rows in the QuerySettled scope
    //   downstream strict visibility must hide the first delivered row until the second arrives
    let mut s = create_3tier_rc();

    let schema = test_schema();
    let app_id = AppId::from_name("durability-test");
    let mgr_d = SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
    let mut d = new_test_core(mgr_d, MemoryStorage::new(), NoopScheduler);

    let d_client_of_b = ClientId::new();
    let b_server_for_d = ServerId::new();
    {
        s.b.add_client(d_client_of_b, None);
        s.b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(d_client_of_b, ClientRole::Peer);
    }
    d.add_server(b_server_for_d);

    d.immediate_tick();
    d.batched_tick();
    d.sync_sender().take();
    s.b.immediate_tick();
    s.b.batched_tick();
    s.b.sync_sender().take();

    let batch_id = crate::row_histories::BatchId::new();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: Some(batch_id),
        target_branch_name: None,
    };

    let ((first_id, _row_values), _) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice-one"),
            Some(&write_context),
        )
        .unwrap();
    let ((second_id, _row_values), _) =
        s.a.insert(
            "users",
            user_insert_values(ObjectId::new(), "Alice-two"),
            Some(&write_context),
        )
        .unwrap();

    s.a.seal_batch(batch_id).unwrap();
    pump_a_to_b(&mut s);
    s.b.batched_tick();
    s.b.sync_sender().take();

    let received = Arc::new(Mutex::new(Vec::<Vec<(ObjectId, Vec<Value>)>>::new()));
    let received_clone = received.clone();

    let _handle = d
        .subscribe_with_durability_and_propagation(
            Query::new("users"),
            move |delta| {
                received_clone
                    .lock()
                    .unwrap()
                    .push(decode_added_rows(&delta));
            },
            None,
            ReadDurabilityOptions {
                tier: Some(DurabilityTier::Local),
                local_updates: crate::query_manager::manager::LocalUpdates::Deferred,
            },
            crate::sync_manager::QueryPropagation::Full,
        )
        .unwrap();

    d.batched_tick();
    for entry in d.sync_sender().take() {
        if entry.destination == Destination::Server(b_server_for_d) {
            s.b.park_sync_message(InboxEntry {
                source: Source::Client(d_client_of_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();
    s.b.batched_tick();

    let mut first_row_payload = None;
    let mut remaining_row_payloads = Vec::new();
    let mut control_payloads = Vec::new();
    for entry in s.b.sync_sender().take() {
        if entry.destination != Destination::Client(d_client_of_b) {
            continue;
        }

        match entry.payload {
            SyncPayload::RowBatchNeeded { metadata, row } => {
                let row_id = row.row_id;
                let payload = SyncPayload::RowBatchNeeded { metadata, row };
                if row_id == first_id && first_row_payload.is_none() {
                    first_row_payload = Some(payload);
                } else if row_id == second_id || row_id == first_id {
                    remaining_row_payloads.push(payload);
                }
            }
            payload @ SyncPayload::BatchSettlement { .. }
            | payload @ SyncPayload::QuerySettled { .. }
            | payload @ SyncPayload::RowBatchStateChanged { .. } => {
                control_payloads.push(payload);
            }
            _ => {}
        }
    }

    let first_row_payload = first_row_payload.expect("expected first row payload");
    assert!(
        control_payloads
            .iter()
            .any(|payload| matches!(payload, SyncPayload::QuerySettled { query_id, scope, .. }
                if *query_id == crate::sync_manager::QueryId(0)
                    && scope.iter().map(|(object_id, _)| *object_id).collect::<std::collections::HashSet<_>>()
                        == std::collections::HashSet::from([first_id, second_id]))),
        "expected settled scope covering both accepted transaction members, got {control_payloads:#?}"
    );
    assert!(
        control_payloads.iter().any(|payload| matches!(
            payload,
            SyncPayload::BatchSettlement {
                settlement: crate::batch_fate::BatchSettlement::AcceptedTransaction {
                    batch_id: settled_batch_id,
                    visible_members,
                    ..
                }
            } if *settled_batch_id == batch_id
                && visible_members.iter().any(|member| member.object_id == first_id)
                && visible_members.iter().any(|member| member.object_id == second_id)
        )),
        "expected accepted transaction settlement for the shared batch"
    );

    for payload in control_payloads
        .into_iter()
        .chain(std::iter::once(first_row_payload))
    {
        d.park_sync_message(InboxEntry {
            source: Source::Server(b_server_for_d),
            payload,
        });
    }
    d.batched_tick();
    d.immediate_tick();

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        1,
        "frontier completion should emit one initial snapshot"
    );
    assert!(
        calls[0].is_empty(),
        "strict visibility should hide the partial accepted batch"
    );
    drop(calls);

    for payload in remaining_row_payloads {
        d.park_sync_message(InboxEntry {
            source: Source::Server(b_server_for_d),
            payload,
        });
    }
    d.batched_tick();
    d.immediate_tick();

    let calls = received.lock().unwrap();
    assert_eq!(
        calls.len(),
        2,
        "completing the batch should emit a second delta"
    );
    assert_eq!(
        calls[1]
            .iter()
            .map(|(id, _)| *id)
            .collect::<std::collections::HashSet<_>>(),
        std::collections::HashSet::from([first_id, second_id]),
        "both accepted rows should appear together once the scoped batch is complete"
    );
}
