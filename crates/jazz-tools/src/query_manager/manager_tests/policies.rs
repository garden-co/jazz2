use super::*;

#[test]
fn policy_filters_select_results() {
    let sync_manager = SyncManager::new();
    let schema = policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert documents
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("alice".into()),
            Value::Text("eng".into()),
            Value::Text("Alice's eng doc".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("eng".into()),
            Value::Text("Bob's eng doc".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("sales".into()),
            Value::Text("Bob's sales doc".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("charlie".into()),
            Value::Text("design".into()),
            Value::Text("Charlie's design doc".into()),
        ],
    )
    .unwrap();

    // Alice can see: her own doc + all eng docs = 2 docs
    let alice_session = PolicySession::new("alice").with_claims(json!({"teams": ["eng"]}));

    let query = qm.query("documents").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(alice_session), None)
        .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let alice_update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .unwrap();

    assert_eq!(
        alice_update.delta.added.len(),
        2,
        "Alice should see 2 docs (her own + Bob's eng doc)"
    );

    // Bob on sales team can see: his 2 docs + no team docs (sales only) = 2 docs
    let bob_session = PolicySession::new("bob").with_claims(json!({"teams": ["sales"]}));

    let query2 = qm.query("documents").build();
    let sub_id2 = qm
        .subscribe_with_session(query2, Some(bob_session), None)
        .unwrap();

    qm.process(&mut storage);
    let updates2 = qm.take_updates();
    let bob_update = updates2
        .iter()
        .find(|u| u.subscription_id == sub_id2)
        .unwrap();

    assert_eq!(
        bob_update.delta.added.len(),
        2,
        "Bob should see 2 docs (his own 2 docs)"
    );
}

#[test]
fn policy_filtered_query_reads_visible_region_after_legacy_commit_history_is_removed() {
    let schema = policy_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handles = vec![
        writer_qm
            .insert(
                &mut storage,
                "documents",
                &[
                    Value::Text("alice".into()),
                    Value::Text("eng".into()),
                    Value::Text("Alice's eng doc".into()),
                ],
            )
            .unwrap(),
        writer_qm
            .insert(
                &mut storage,
                "documents",
                &[
                    Value::Text("bob".into()),
                    Value::Text("eng".into()),
                    Value::Text("Bob's eng doc".into()),
                ],
            )
            .unwrap(),
        writer_qm
            .insert(
                &mut storage,
                "documents",
                &[
                    Value::Text("bob".into()),
                    Value::Text("sales".into()),
                    Value::Text("Bob's sales doc".into()),
                ],
            )
            .unwrap(),
        writer_qm
            .insert(
                &mut storage,
                "documents",
                &[
                    Value::Text("charlie".into()),
                    Value::Text("design".into()),
                    Value::Text("Charlie's design doc".into()),
                ],
            )
            .unwrap(),
    ];
    writer_qm.process(&mut storage);

    for _handle in handles {}

    let mut reader_qm = QueryManager::new(SyncManager::new());
    reader_qm.set_current_schema(schema, "dev", "main");

    let alice_session = PolicySession::new("alice").with_claims(json!({"teams": ["eng"]}));
    let query = reader_qm.query("documents").build();
    let sub_id = reader_qm
        .subscribe_with_session(query, Some(alice_session), None)
        .unwrap();

    reader_qm.process(&mut storage);

    let results = reader_qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 2);

    let titles: Vec<_> = results
        .iter()
        .filter_map(|(_, row)| match &row[2] {
            Value::Text(title) => Some(title.as_str()),
            _ => None,
        })
        .collect();
    assert!(titles.contains(&"Alice's eng doc"));
    assert!(titles.contains(&"Bob's eng doc"));
}

#[test]
fn no_session_returns_all_rows() {
    let sync_manager = SyncManager::new();
    let schema = policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert documents
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("alice".into()),
            Value::Text("eng".into()),
            Value::Text("Doc 1".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("sales".into()),
            Value::Text("Doc 2".into()),
        ],
    )
    .unwrap();

    // Without session, all rows should be returned (policy not applied)
    let query = qm.query("documents").build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .unwrap();

    assert_eq!(
        update.delta.added.len(),
        2,
        "Without session, should see all 2 docs"
    );
}

#[test]
fn permission_preflight_reports_allow_and_deny_without_writing() {
    let sync_manager = SyncManager::new();
    let schema = current_documents_permission_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let alice = PolicySession::new("alice");
    let bob = PolicySession::new("bob");

    assert_eq!(
        qm.can_insert_with_session(
            &mut storage,
            "documents",
            &[
                Value::Text("Alice's draft".into()),
                Value::Text("alice".into()),
            ],
            Some(&alice),
        )
        .unwrap(),
        crate::query_manager::types::PermissionPreflightDecision::Allow
    );
    assert_eq!(
        qm.can_insert_with_session(
            &mut storage,
            "documents",
            &[Value::Text("Bob's draft".into()), Value::Text("bob".into()),],
            Some(&alice),
        )
        .unwrap(),
        crate::query_manager::types::PermissionPreflightDecision::Deny
    );

    let inserted = qm
        .insert(
            &mut storage,
            "documents",
            &[
                Value::Text("Existing doc".into()),
                Value::Text("alice".into()),
            ],
        )
        .unwrap();

    assert_eq!(
        qm.can_update_with_session(
            &mut storage,
            inserted.row_id,
            &[
                Value::Text("Updated doc".into()),
                Value::Text("alice".into()),
            ],
            Some(&alice),
        )
        .unwrap(),
        crate::query_manager::types::PermissionPreflightDecision::Allow
    );
    assert_eq!(
        qm.can_update_with_session(
            &mut storage,
            inserted.row_id,
            &[
                Value::Text("Bob tries to update".into()),
                Value::Text("alice".into()),
            ],
            Some(&bob),
        )
        .unwrap(),
        crate::query_manager::types::PermissionPreflightDecision::Deny
    );

    let sub_id = qm.subscribe(qm.query("documents").build()).unwrap();
    qm.process(&mut storage);
    let rows = qm.get_subscription_results(sub_id);
    assert_eq!(
        rows.len(),
        1,
        "preflight should not create, update, or delete rows"
    );
    assert_eq!(rows[0].1[0], Value::Text("Existing doc".into()));
}

#[test]
fn loaded_empty_permissions_bundle_hides_rows_without_explicit_read_policy() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, test_schema());

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(200)],
    )
    .unwrap();

    qm.set_authorization_schema(test_schema());

    let query = qm.query("users").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(PolicySession::new("alice")), None)
        .unwrap();

    qm.process(&mut storage);

    assert!(
        qm.get_subscription_results(sub_id).is_empty(),
        "loaded empty permissions bundle should deny session-scoped reads without an explicit read grant"
    );
}

#[test]
fn local_subscription_uses_current_permissions_after_lens_transform() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, legacy_documents_schema());
    configure_legacy_client_with_current_permissions(&mut qm);

    qm.insert(
        &mut storage,
        "documents",
        &[Value::Text("Legacy doc".into())],
    )
    .unwrap();

    let query = qm.query("documents").build();
    let alice_sub = qm
        .subscribe_with_session(query.clone(), Some(PolicySession::new("alice")), None)
        .unwrap();
    let bob_sub = qm
        .subscribe_with_session(query, Some(PolicySession::new("bob")), None)
        .unwrap();

    qm.process(&mut storage);

    let alice_results = qm.get_subscription_results(alice_sub);
    assert_eq!(
        alice_results.len(),
        1,
        "Alice should see the legacy row after lens-based auth transform"
    );
    assert_eq!(alice_results[0].1, vec![Value::Text("Legacy doc".into())]);
    assert!(
        qm.get_subscription_results(bob_sub).is_empty(),
        "Bob should be filtered out by the current permission schema"
    );
}

#[test]
fn local_insert_uses_current_permissions_after_lens_transform() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, legacy_documents_schema());
    configure_legacy_client_with_current_permissions(&mut qm);

    qm.insert_with_session(
        &mut storage,
        "documents",
        &[Value::Text("Alice insert".into())],
        Some(&PolicySession::new("alice")),
    )
    .expect("alice insert should be allowed by transformed current permissions");

    let err = qm
        .insert_with_session(
            &mut storage,
            "documents",
            &[Value::Text("Bob insert".into())],
            Some(&PolicySession::new("bob")),
        )
        .expect_err("bob insert should be denied by transformed current permissions");
    assert_eq!(
        err,
        QueryError::PolicyDenied {
            table: TableName::new("documents"),
            operation: crate::query_manager::policy::Operation::Insert,
        }
    );
}

#[test]
fn loaded_empty_permissions_bundle_denies_local_insert_without_explicit_insert_policy() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, test_schema());

    qm.set_authorization_schema(test_schema());

    let err = qm
        .insert_with_session(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
            Some(&PolicySession::new("alice")),
        )
        .expect_err("loaded empty permissions bundle should deny insert without explicit policy");

    assert_eq!(
        err,
        QueryError::PolicyDenied {
            table: TableName::new("users"),
            operation: crate::query_manager::policy::Operation::Insert,
        }
    );
}

#[test]
fn local_update_and_delete_use_current_permissions_after_lens_transform() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, legacy_documents_schema());
    configure_legacy_client_with_current_permissions(&mut qm);

    let inserted = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("Legacy doc".into())],
        )
        .unwrap();

    let update_err = qm
        .update_with_session(
            &mut storage,
            inserted.row_id,
            &[Value::Text("Bob edit".into())],
            Some(&PolicySession::new("bob")),
        )
        .expect_err("bob update should be denied by transformed current permissions");
    assert_eq!(
        update_err,
        QueryError::PolicyDenied {
            table: TableName::new("documents"),
            operation: crate::query_manager::policy::Operation::Update,
        }
    );

    qm.update_with_session(
        &mut storage,
        inserted.row_id,
        &[Value::Text("Alice edit".into())],
        Some(&PolicySession::new("alice")),
    )
    .expect("alice update should be allowed by transformed current permissions");

    let delete_err = qm
        .delete_with_session(
            &mut storage,
            inserted.row_id,
            Some(&PolicySession::new("bob")),
        )
        .expect_err("bob delete should be denied by transformed current permissions");
    assert_eq!(
        delete_err,
        QueryError::PolicyDenied {
            table: TableName::new("documents"),
            operation: crate::query_manager::policy::Operation::Delete,
        }
    );

    qm.delete_with_session(
        &mut storage,
        inserted.row_id,
        Some(&PolicySession::new("alice")),
    )
    .expect("alice delete should be allowed by transformed current permissions");
}

#[test]
fn e2e_permissions_prevent_sync() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    // Create schema with documents table that has owner-based policy
    // Policy: owner_id must match session.user_id
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Text), // Text to match user_id string
            ]),
            policies: TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        },
    );

    // Setup server with docs owned by different users
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    server
        .insert(
            &mut server_io,
            "documents",
            &[
                Value::Text("Alice's doc".into()),
                Value::Text("alice".into()),
            ],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "documents",
            &[Value::Text("Bob's doc".into()), Value::Text("bob".into())],
        )
        .unwrap();
    server.process(&mut server_io);

    // Setup client
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes as Alice (user_id = "alice")
    let alice_session = PolicySession::new("alice");
    let query = client.query("documents").build();

    let sub_id = client
        .subscribe_with_sync(query, Some(alice_session), None)
        .unwrap();

    // Exchange messages
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should ONLY have Alice's doc
    let results = client.get_subscription_results(sub_id);

    assert_eq!(
        results.len(),
        1,
        "Client should only receive docs they have permission to see"
    );
    assert_eq!(
        results[0].1[0],
        Value::Text("Alice's doc".into()),
        "Should be Alice's doc"
    );
}

#[test]
fn e2e_permissions_prevent_new_row_sync() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    // Create schema with owner-based policy
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Text), // Text to match user_id string
            ]),
            policies: TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        },
    );

    // Setup server and client
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes as Alice
    let alice_session = PolicySession::new("alice");
    let query = client.query("documents").build();

    let sub_id = client
        .subscribe_with_sync(query, Some(alice_session), None)
        .unwrap();
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Server inserts Alice's doc
    server
        .insert(
            &mut server_io,
            "documents",
            &[
                Value::Text("Alice's doc".into()),
                Value::Text("alice".into()),
            ],
        )
        .unwrap();
    server.process(&mut server_io);
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    assert_eq!(
        client.get_subscription_results(sub_id).len(),
        1,
        "Client should have Alice's doc"
    );

    // Server inserts Bob's doc (owner_id = "bob")
    server
        .insert(
            &mut server_io,
            "documents",
            &[Value::Text("Bob's doc".into()), Value::Text("bob".into())],
        )
        .unwrap();
    server.process(&mut server_io);
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should still only have Alice's doc
    let results = client.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1, "Client should NOT receive Bob's doc");
    assert_eq!(results[0].1[0], Value::Text("Alice's doc".into()));
}

#[test]
fn sync_backed_session_subscription_keeps_local_rows_when_server_scope_is_empty() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Text),
            ]),
            policies: TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        },
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    client
        .insert(
            &mut client_io,
            "documents",
            &[
                Value::Text("Alice's doc".into()),
                Value::Text("alice".into()),
            ],
        )
        .unwrap();
    client.process(&mut client_io);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("documents").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::Local),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed session subscriptions should keep local rows even when the server scope is empty"
    );
    assert_eq!(
        results[0].1,
        vec![
            Value::Text("Alice's doc".into()),
            Value::Text("alice".into())
        ]
    );
}

#[test]
fn sync_backed_exists_rel_session_subscription_keeps_local_rows_when_server_scope_is_empty() {
    use crate::query_manager::relation_ir::{
        ColumnRef, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef, ValueRef,
    };
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            policies: TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                rel: RelExpr::Filter {
                    input: Box::new(RelExpr::TableScan {
                        table: TableName::new("user_team_edges"),
                    }),
                    predicate: PredicateExpr::And(vec![
                        PredicateExpr::Cmp {
                            left: ColumnRef::unscoped("team_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::RowId(RowIdRef::Outer),
                        },
                        PredicateExpr::Cmp {
                            left: ColumnRef::unscoped("user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                    ]),
                },
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let team_row = client
        .insert(&mut client_io, "teams", &[Value::Text("Alice".into())])
        .unwrap();
    client
        .insert(
            &mut client_io,
            "user_team_edges",
            &[Value::Text("alice".into()), Value::Uuid(team_row.row_id)],
        )
        .unwrap();
    client.process(&mut client_io);

    let local_sub_id = client
        .subscribe_with_session(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);
    assert_eq!(
        client.get_subscription_results(local_sub_id).len(),
        1,
        "local session subscriptions should already see the related-row grant"
    );

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed EXISTS policies should keep local rows even when the server scope is empty"
    );
    assert_eq!(results[0].1, vec![Value::Text("Alice".into())]);
}

#[test]
fn sync_backed_exists_session_subscription_keeps_local_rows_when_server_scope_is_empty() {
    use crate::query_manager::policy::{CmpOp, OUTER_ROW_SESSION_PREFIX, PolicyValue};
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            policies: TablePolicies::new().with_select(PolicyExpr::Exists {
                table: "user_team_edges".into(),
                condition: Box::new(PolicyExpr::And(vec![
                    PolicyExpr::Cmp {
                        column: "team_id".into(),
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(vec![
                            OUTER_ROW_SESSION_PREFIX.into(),
                            "id".into(),
                        ]),
                    },
                    PolicyExpr::eq_session("user_id", vec!["user_id".into()]),
                ])),
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let team_row = client
        .insert(&mut client_io, "teams", &[Value::Text("Alice".into())])
        .unwrap();
    client
        .insert(
            &mut client_io,
            "user_team_edges",
            &[Value::Text("alice".into()), Value::Uuid(team_row.row_id)],
        )
        .unwrap();
    client.process(&mut client_io);

    let local_sub_id = client
        .subscribe_with_session(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);
    assert_eq!(
        client.get_subscription_results(local_sub_id).len(),
        1,
        "local session subscriptions should already see the correlated EXISTS grant"
    );

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed EXISTS policies should keep local rows even when the server scope is empty"
    );
    assert_eq!(results[0].1, vec![Value::Text("Alice".into())]);
}

#[test]
fn fail_closed_server_withholds_session_scope_before_permissions_head() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef,
        ValueRef,
    };
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            policies: TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                rel: RelExpr::Filter {
                    input: Box::new(RelExpr::Join {
                        left: Box::new(RelExpr::TableScan {
                            table: TableName::new("user_team_edges"),
                        }),
                        right: Box::new(RelExpr::TableScan {
                            table: TableName::new("teams"),
                        }),
                        on: vec![JoinCondition {
                            left: ColumnRef::scoped("user_team_edges", "team_id"),
                            right: ColumnRef::scoped("__join_0", "id"),
                        }],
                        join_kind: JoinKind::Inner,
                    }),
                    predicate: PredicateExpr::And(vec![
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("user_team_edges", "user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("__join_0", "id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::RowId(RowIdRef::Outer),
                        },
                    ]),
                },
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let mut structural_server_schema = Schema::new();
    structural_server_schema.insert(
        TableName::new("teams"),
        TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
            "name",
            ColumnType::Text,
        )])),
    );
    structural_server_schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, structural_server_schema);
    server.require_authorization_schema();
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let team_row = client
        .insert(&mut client_io, "teams", &[Value::Text("Alice".into())])
        .unwrap();
    let edge_row = client
        .insert(
            &mut client_io,
            "user_team_edges",
            &[Value::Text("alice".into()), Value::Uuid(team_row.row_id)],
        )
        .unwrap();
    client.process(&mut client_io);

    client.clear_local_pending_row_overlay("teams", team_row.row_id);
    client.clear_local_pending_row_overlay("user_team_edges", edge_row.row_id);
    client.process(&mut client_io);

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    assert!(
        !client
            .sync_manager()
            .has_remote_query_scope_snapshot(crate::sync_manager::QueryId(sub_id.0)),
        "server without a published permissions head should not advertise an authoritative remote scope"
    );

    let failures = client.take_failed_subscriptions();
    assert!(
        failures.is_empty(),
        "missing permissions head should not surface an explicit subscription failure yet: {failures:?}"
    );
}

#[test]
fn synced_session_query_for_exists_rel_sends_policy_context_tables_upstream() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef,
        ValueRef,
    };
    use crate::sync_manager::DurabilityTier;
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            policies: TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                rel: RelExpr::Filter {
                    input: Box::new(RelExpr::Join {
                        left: Box::new(RelExpr::TableScan {
                            table: TableName::new("user_team_edges"),
                        }),
                        right: Box::new(RelExpr::TableScan {
                            table: TableName::new("teams"),
                        }),
                        on: vec![JoinCondition {
                            left: ColumnRef::scoped("user_team_edges", "team_id"),
                            right: ColumnRef::scoped("__join_0", "id"),
                        }],
                        join_kind: JoinKind::Inner,
                    }),
                    predicate: PredicateExpr::And(vec![
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("user_team_edges", "user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("__join_0", "id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::RowId(RowIdRef::Outer),
                        },
                    ]),
                },
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let sync_manager = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let (mut qm, storage) = create_query_manager(sync_manager, schema);
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut qm, &storage, server_id);
    let _ = qm.sync_manager_mut().take_outbox();

    qm.subscribe_with_sync(
        qm.query("teams").build(),
        Some(PolicySession::new("alice")),
        Some(DurabilityTier::EdgeServer),
    )
    .unwrap();

    let outbox = qm.sync_manager_mut().take_outbox();
    let forwarded = outbox
        .into_iter()
        .find_map(|entry| match entry.payload {
            SyncPayload::QuerySubscription {
                policy_context_tables,
                ..
            } => Some(policy_context_tables),
            _ => None,
        })
        .expect("subscription should be forwarded upstream");

    assert!(
        forwarded.iter().any(|table| table == "user_team_edges"),
        "EXISTS_REL subscriptions should declare their policy-context tables upstream"
    );
}

#[test]
fn backend_sync_subscription_without_handshake_session_stays_unscoped_without_permissions_head() {
    use crate::sync_manager::ClientRole;
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("identity_key", ColumnType::Text).nullable(),
            ]),
            policies: TablePolicies::new().with_select(PolicyExpr::eq_session(
                "identity_key",
                vec!["user_id".into()],
            )),
        },
    );

    let mut structural_server_schema = Schema::new();
    structural_server_schema.insert(
        TableName::new("teams"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("identity_key", ColumnType::Text).nullable(),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, structural_server_schema);
    server.require_authorization_schema();
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    server
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Backend);
    let _ = client.sync_manager_mut().take_outbox();

    let team_row = client
        .insert(
            &mut client_io,
            "teams",
            &[Value::Text("Bob".into()), Value::Text("bob".into())],
        )
        .unwrap();
    client.process(&mut client_io);
    client.clear_local_pending_row_overlay("teams", team_row.row_id);
    client.process(&mut client_io);

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("bob")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    assert!(
        !client
            .sync_manager()
            .has_remote_query_scope_snapshot(crate::sync_manager::QueryId(sub_id.0)),
        "backend-authenticated clients without a handshake session should not receive an authoritative remote scope"
    );

    let failures = client.take_failed_subscriptions();
    assert!(
        failures.is_empty(),
        "missing permissions head should not surface an explicit subscription failure yet: {failures:?}"
    );
}

#[test]
fn e2e_three_tier_policy_dependencies_must_sync_downstream() {
    use crate::query_manager::policy::Operation;
    use crate::sync_manager::{ClientId, ClientRole, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("name", ColumnType::Text),
            ]),
            TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ),
    );
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("folder_id", ColumnType::Uuid)
                    .nullable()
                    .references("folders"),
            ]),
            TablePolicies::new().with_select(PolicyExpr::or(vec![
                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                PolicyExpr::inherits(Operation::Select, "folder_id"),
            ])),
        ),
    );

    // Core (upstream) has data.
    let (mut core, mut core_io) = create_query_manager(SyncManager::new(), schema.clone());
    let folder_id = core
        .insert(
            &mut core_io,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Alice folder".into()),
            ],
        )
        .unwrap()
        .row_id;
    core.insert(
        &mut core_io,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("Bob doc in Alice folder".into()),
            Value::Uuid(folder_id),
        ],
    )
    .unwrap();
    core.process(&mut core_io);

    // Edge (mid-tier) starts empty.
    let (mut edge, mut edge_io) = create_query_manager(SyncManager::new(), schema.clone());

    // Downstream client starts empty.
    let (mut client, mut client_io) = create_query_manager(SyncManager::new(), schema.clone());

    // Topology: client <-> edge <-> core
    let edge_server_id_for_client = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id_on_edge = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let core_server_id_for_edge = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let edge_id_on_core = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    client
        .sync_manager_mut()
        .add_server_with_storage(edge_server_id_for_client, false, &client_io);
    connect_client(&mut edge, &edge_io, client_id_on_edge);
    edge.sync_manager_mut()
        .set_client_role(client_id_on_edge, ClientRole::Peer);
    connect_server(&mut edge, &edge_io, core_server_id_for_edge);
    connect_client(&mut core, &core_io, edge_id_on_core);
    core.sync_manager_mut()
        .set_client_role(edge_id_on_core, ClientRole::Peer);

    // Clear non-query bootstrap traffic.
    let _ = client.sync_manager_mut().take_outbox();
    let _ = edge.sync_manager_mut().take_outbox();
    let _ = core.sync_manager_mut().take_outbox();

    // Client subscribes as alice.
    let sub_id = client
        .subscribe_with_sync(
            client.query("documents").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);

    pump_messages_three_tier(
        &mut client,
        &mut edge,
        &mut core,
        &mut client_io,
        &mut edge_io,
        &mut core_io,
        client_id_on_edge,
        edge_server_id_for_client,
        edge_id_on_core,
        core_server_id_for_edge,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "Client should receive docs visible via INHERITS through edge in 3-tier sync"
    );
}

#[test]
fn remove_client_cleans_active_policy_checks() {
    //
    // alice ──write──▶ server (policy check in-flight)
    // bob   ──write──▶ server (policy check in-flight)
    //
    // alice disconnects → only bob's policy check remains.
    //
    use crate::query_manager::policy::Operation;
    use crate::sync_manager::{ClientId, PendingPermissionCheck, PendingUpdateId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, _storage) = create_query_manager(sync_manager, schema);

    let alice = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let bob = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &_storage, alice);
    connect_client(&mut server_qm, &_storage, bob);

    let obj_id = crate::object::ObjectId::new();
    let make_check = |id: u64, client_id: ClientId| PendingPermissionCheck {
        id: PendingUpdateId(id),
        client_id,
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(crate::sync_manager::RowMetadata {
                id: obj_id,
                metadata: HashMap::from([(MetadataKey::Table.to_string(), "users".to_string())]),
            }),
            row: StoredRowBatch::new(
                obj_id,
                "main",
                Vec::new(),
                b"alice".to_vec(),
                RowProvenance::for_insert(obj_id.to_string(), 1_000),
                HashMap::new(),
                RowState::VisibleDirect,
                None,
            ),
        },
        session: crate::query_manager::session::Session {
            user_id: format!("{client_id}"),
            claims: serde_json::Value::Null,
            auth_mode: Default::default(),
        },
        schema_wait_started_at: None,
        metadata: Default::default(),
        old_content: None,
        new_content: None,
        operation: Operation::Insert,
    };

    // Insert policy checks directly into the active_policy_checks map
    use crate::query_manager::manager::PolicyCheckState;
    server_qm.active_policy_checks.insert(
        PendingUpdateId(1),
        PolicyCheckState {
            graphs: vec![],
            table: "users".into(),
            branch: crate::object::BranchName::new("main"),
            pending_check: make_check(1, alice),
        },
    );
    server_qm.active_policy_checks.insert(
        PendingUpdateId(2),
        PolicyCheckState {
            graphs: vec![],
            table: "users".into(),
            branch: crate::object::BranchName::new("main"),
            pending_check: make_check(2, bob),
        },
    );

    assert_eq!(server_qm.active_policy_checks.len(), 2);

    server_qm.remove_client(alice);

    assert_eq!(
        server_qm.active_policy_checks.len(),
        1,
        "only bob's policy check should remain"
    );
    assert!(
        server_qm
            .active_policy_checks
            .contains_key(&PendingUpdateId(2))
    );
}

#[test]
fn anonymous_insert_is_denied_before_policy_eval() {
    // Even when the schema has an allow-all insert policy, an anonymous session
    // must be rejected with AnonymousWriteDenied before policy evaluation runs.
    use crate::query_manager::policy::Operation;
    use crate::query_manager::session::AuthMode;

    let mut schema = Schema::new();
    let mut table_schema = TableSchema::new(RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]));
    // Allow-all insert policy — anonymous session must still be denied structurally.
    table_schema.policies = TablePolicies::new().with_insert(PolicyExpr::True);
    schema.insert(TableName::new("users"), table_schema);

    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let anon = PolicySession::new("anon-user").with_auth_mode(AuthMode::Anonymous);

    let err = qm
        .insert_with_session(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(42)],
            Some(&anon),
        )
        .expect_err("anonymous insert must be denied");

    assert_eq!(
        err,
        QueryError::AnonymousWriteDenied {
            table: TableName::new("users"),
            operation: Operation::Insert,
        }
    );
}

#[test]
fn anonymous_update_is_denied_before_policy_eval() {
    use crate::query_manager::policy::Operation;
    use crate::query_manager::session::AuthMode;

    let mut schema = Schema::new();
    let mut table_schema = TableSchema::new(RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]));
    // Allow-all update policies — anonymous session must still be denied structurally.
    table_schema.policies =
        TablePolicies::new().with_update(Some(PolicyExpr::True), PolicyExpr::True);
    schema.insert(TableName::new("users"), table_schema);

    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row without a session so it succeeds.
    let inserted = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(10)],
        )
        .expect("insert without session should succeed");

    let anon = PolicySession::new("anon-user").with_auth_mode(AuthMode::Anonymous);

    let err = qm
        .update_with_session(
            &mut storage,
            inserted.row_id,
            &[Value::Text("Bob updated".into()), Value::Integer(20)],
            Some(&anon),
        )
        .expect_err("anonymous update must be denied");

    assert_eq!(
        err,
        QueryError::AnonymousWriteDenied {
            table: TableName::new("users"),
            operation: Operation::Update,
        }
    );
}

#[test]
fn anonymous_delete_is_denied_before_policy_eval() {
    use crate::query_manager::policy::Operation;
    use crate::query_manager::session::AuthMode;

    let mut schema = Schema::new();
    let mut table_schema = TableSchema::new(RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]));
    // Allow-all delete policy — anonymous session must still be denied structurally.
    table_schema.policies = TablePolicies::new().with_delete(PolicyExpr::True);
    schema.insert(TableName::new("users"), table_schema);

    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row without a session so it succeeds.
    let inserted = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Carol".into()), Value::Integer(5)],
        )
        .expect("insert without session should succeed");

    let anon = PolicySession::new("anon-user").with_auth_mode(AuthMode::Anonymous);

    let err = qm
        .delete_with_session(&mut storage, inserted.row_id, Some(&anon))
        .expect_err("anonymous delete must be denied");

    assert_eq!(
        err,
        QueryError::AnonymousWriteDenied {
            table: TableName::new("users"),
            operation: Operation::Delete,
        }
    );
}
