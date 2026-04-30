use super::*;

#[test]
fn rebac_inherited_insert_uses_payload_branch_for_parent_lookup() {
    let (schema, folders_descriptor, schema_hash) = inherited_insert_schema();
    let branch = inherited_insert_branch(schema_hash);

    // Server mode keeps current_branch() at "main", while the write arrives on
    // a composed client branch. The inherited parent lookup must use payload
    // branch context, not current_branch().
    let mut storage = seeded_memory_storage(&schema);
    let mut qm = create_server_mode_query_manager(schema, schema_hash);

    assert_ne!(qm.current_branch(), branch);
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let folder_id = seed_folder_on_branch(
        &mut qm,
        &mut storage,
        &branch,
        "alice",
        "Shared Folder",
        &folders_descriptor,
    );
    let doc_id = create_test_row(&mut storage, Some(document_metadata()));

    let mut scope = HashSet::new();
    scope.insert((doc_id, branch.clone().into()));
    set_client_query_scope(&mut qm, &storage, client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    let commit = enqueue_inherited_insert(
        &mut qm,
        client_id,
        doc_id,
        &branch,
        folder_id,
        "Allowed via folder ownership",
    );

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        doc_id,
        &branch,
        row_batch_id_for_commit(doc_id, &branch, &commit),
    );
    assert!(
        !denied,
        "Inherited insert should use the payload branch to find the parent folder"
    );

    let tips = test_row_tip_ids(&storage, doc_id, &branch).unwrap();
    assert!(
        tips.contains(&row_batch_id_for_commit(doc_id, &branch, &commit)),
        "Document insert should be applied when the parent folder is visible on the payload branch"
    );
}

#[test]
fn rebac_inherited_insert_uses_payload_branch_after_cold_start() {
    let (schema, folders_descriptor, schema_hash) = inherited_insert_schema();
    let branch = inherited_insert_branch(schema_hash);
    let mut storage = seeded_memory_storage(&schema);

    let mut seed_qm = create_server_mode_query_manager(schema.clone(), schema_hash);
    let folder_id = seed_folder_on_branch(
        &mut seed_qm,
        &mut storage,
        &branch,
        "alice",
        "Cold Folder",
        &folders_descriptor,
    );

    let mut qm = create_server_mode_query_manager(schema, schema_hash);
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    qm.sync_manager_mut().take_outbox();

    let doc_id = ObjectId::new();
    let commit = enqueue_inherited_insert(
        &mut qm,
        client_id,
        doc_id,
        &branch,
        folder_id,
        "Cold-start branch lookup",
    );

    // Cold start:
    //   storage: folder exists only on the composed payload branch
    //   cache:   parent folder is not loaded at all
    //   write:   document insert must still authorize on that payload branch
    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        doc_id,
        &branch,
        row_batch_id_for_commit(doc_id, &branch, &commit),
    );
    assert!(
        !denied,
        "Inherited insert should authorize on the payload branch even after a cold start"
    );

    let tips = test_row_tip_ids(&storage, doc_id, &branch).unwrap();
    assert!(
        tips.contains(&row_batch_id_for_commit(doc_id, &branch, &commit)),
        "Document insert should be applied after settlement reads the parent from the payload branch"
    );
}

#[test]
fn rebac_inherited_insert_uses_visible_row_region_after_legacy_branch_history_is_removed() {
    let (schema, folders_descriptor, schema_hash) = inherited_insert_schema();
    let branch = inherited_insert_branch(schema_hash);
    let mut storage = seeded_memory_storage(&schema);

    let mut seed_qm = create_server_mode_query_manager(schema.clone(), schema_hash);
    let folder_id = seed_folder_on_branch(
        &mut seed_qm,
        &mut storage,
        &branch,
        "alice",
        "Cold Folder",
        &folders_descriptor,
    );

    let _folder_commit_id = *test_row_tip_ids(&storage, folder_id, &branch)
        .unwrap()
        .first()
        .expect("seed folder should have one tip");
    let mut qm = create_server_mode_query_manager(schema, schema_hash);
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    qm.sync_manager_mut().take_outbox();

    let doc_id = ObjectId::new();
    let commit = enqueue_inherited_insert(
        &mut qm,
        client_id,
        doc_id,
        &branch,
        folder_id,
        "Cold-start branch lookup",
    );

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        doc_id,
        &branch,
        row_batch_id_for_commit(doc_id, &branch, &commit),
    );
    assert!(
        !denied,
        "Inherited insert should authorize from the visible row region without legacy branch commits"
    );

    let tips = test_row_tip_ids(&storage, doc_id, &branch).unwrap();
    assert!(
        tips.contains(&row_batch_id_for_commit(doc_id, &branch, &commit)),
        "Document insert should still be applied after permission settlement"
    );
}

#[test]
fn rebac_inherited_insert_uses_requested_branch_instead_of_reusing_cached_branch() {
    let (schema, folders_descriptor, schema_hash) = inherited_insert_schema();
    let branch = inherited_insert_branch(schema_hash);
    let mut storage = seeded_memory_storage(&schema);

    let mut seed_qm = create_server_mode_query_manager(schema.clone(), schema_hash);
    let folder_id = seed_folder_on_branch(
        &mut seed_qm,
        &mut storage,
        "main",
        "bob",
        "Main Folder",
        &folders_descriptor,
    );
    add_row_commit(
        &mut storage,
        folder_id,
        &branch,
        vec![],
        encode_folder("alice", "Dev Folder"),
        1000,
        ObjectId::new().to_string(),
    );
    QueryManager::update_indices_for_insert_on_branch(
        &mut storage,
        "folders",
        &branch,
        folder_id,
        &encode_folder("alice", "Dev Folder"),
        &folders_descriptor,
    )
    .unwrap();
    seed_qm.persist_row_region_tip(&mut storage, "folders", folder_id, &branch);

    let mut qm = create_server_mode_query_manager(schema, schema_hash);
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    qm.sync_manager_mut().take_outbox();

    assert!(
        storage
            .load_visible_region_row("folders", "main", folder_id)
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .load_visible_region_row("folders", &branch, folder_id)
            .unwrap()
            .is_some(),
        "requested-branch visible state should already live in storage"
    );

    let doc_id = ObjectId::new();
    let commit = enqueue_inherited_insert(
        &mut qm,
        client_id,
        doc_id,
        &branch,
        folder_id,
        "Hydrate branch instead of reusing main",
    );

    // Wrong-branch reuse:
    //   storage: folder[main] = bob, folder[dev] = alice
    //   write:   document[dev] must consult folder[dev], not reuse folder[main]
    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        doc_id,
        &branch,
        row_batch_id_for_commit(doc_id, &branch, &commit),
    );
    assert!(
        !denied,
        "Inherited insert should use the requested payload branch instead of reusing cached main"
    );

    let tips = test_row_tip_ids(&storage, doc_id, &branch).unwrap();
    assert!(
        tips.contains(&row_batch_id_for_commit(doc_id, &branch, &commit)),
        "Document insert should apply once the requested parent branch is consulted"
    );
}

#[test]
fn rebac_inherits_filters_select_query_results() {
    use crate::query_manager::query::QueryBuilder;

    // Schema with INHERITS policy
    let mut schema = Schema::new();

    // Folders table
    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let folders_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor.clone(), folders_policies),
    );

    // Documents table with INHERITS
    let docs_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("folder_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);

    // SELECT policy: owner_id = @user_id OR INHERITS SELECT VIA folder_id
    let docs_policies = TablePolicies::new().with_select(PolicyExpr::Or(vec![
        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
        PolicyExpr::Inherits {
            operation: Operation::Select,
            via_column: "folder_id".into(),
            max_depth: None,
        },
    ]));
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(docs_descriptor.clone(), docs_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    // Create Alice's folder
    let mut folder_meta = std::collections::HashMap::new();
    folder_meta.insert(MetadataKey::Table.to_string(), "folders".to_string());
    let folder_id = create_test_row(&mut storage, Some(folder_meta));

    let folder_content = encode_row(
        &folders_descriptor,
        &[
            Value::Text("alice".into()),
            Value::Text("Alice's Folder".into()),
        ],
    )
    .unwrap();
    let author = ObjectId::new();
    add_row_commit(
        &mut storage,
        folder_id,
        "main",
        vec![],
        folder_content,
        1000,
        author.to_string(),
    );

    // Create Bob's document in Alice's folder
    let mut doc_meta = std::collections::HashMap::new();
    doc_meta.insert(MetadataKey::Table.to_string(), "documents".to_string());
    let doc_id = create_test_row(&mut storage, Some(doc_meta));

    let doc_content = encode_row(
        &docs_descriptor,
        &[
            Value::Text("bob".into()),
            Value::Text("Bob's Doc in Alice's Folder".into()),
            Value::Uuid(folder_id),
        ],
    )
    .unwrap();
    add_row_commit(
        &mut storage,
        doc_id,
        "main",
        vec![],
        doc_content,
        1000,
        author.to_string(),
    );

    // Charlie subscribes to documents query with his session
    let charlie_session = Session::new("charlie");
    let query = QueryBuilder::new("documents").branch("main").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(charlie_session), None)
        .unwrap();

    // Process to settle the query
    for _ in 0..10 {
        qm.process(&mut storage);
    }

    // Get Charlie's query results via take_updates
    let updates = qm.take_updates();
    let charlie_update = updates.iter().find(|u| u.subscription_id == sub_id);

    // Charlie should NOT see Bob's document (doesn't own it, doesn't own folder)
    // The update should either be missing or have an empty added set
    let has_rows = charlie_update
        .map(|u| !u.delta.added.is_empty())
        .unwrap_or(false);

    assert!(
        !has_rows,
        "Charlie should not see Bob's document - he owns neither the doc nor the folder. \
         INHERITS should have denied access, but currently it always returns true."
    );
}

#[test]
fn inherits_select_denies_when_parent_operation_policy_is_missing() {
    use crate::query_manager::query::QueryBuilder;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("folders"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("owner_id", ColumnType::Text),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );

    let documents_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("folder_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let documents_policies = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "folder_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(documents_descriptor, documents_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let folder = qm
        .insert(
            &mut storage,
            "folders",
            &[Value::Text("alice".into()), Value::Text("Shared".into())],
        )
        .expect("folder insert should succeed");
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("Inherited doc".into()),
            Value::Uuid(folder.row_id),
        ],
    )
    .expect("document insert should succeed");

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("documents").select(&["title"]).build(),
        Some(Session::new("alice")),
    );

    assert!(
        rows.is_empty(),
        "child rows should be denied when INHERITS reaches a parent table with no explicit SELECT policy"
    );
}

#[test]
fn local_insert_with_inherits_policy_allows_missing_parent_policy_in_permissive_local() {
    let mut schema = Schema::new();
    let folders_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("title", ColumnType::Text)]);
    schema.insert(
        TableName::new("folders"),
        TableSchema::new(folders_descriptor.clone()),
    );

    let documents_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("folder_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let documents_policies = TablePolicies::new().with_insert(PolicyExpr::Inherits {
        operation: Operation::Insert,
        via_column: "folder_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(documents_descriptor, documents_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm =
        create_query_manager_with_policy_mode(sync_manager, schema, RowPolicyMode::PermissiveLocal);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let folder = qm
        .insert(
            &mut storage,
            "folders",
            &[Value::Text("alice folder".into())],
        )
        .expect("seed folder row");

    qm.insert_with_session(
        &mut storage,
        "documents",
        &[Value::Text("draft doc".into()), Value::Uuid(folder.row_id)],
        Some(&Session::new("alice")),
    )
    .expect(
        "permissive local runtimes should treat missing parent INSERT policy as allow for INHERITS",
    );
}

#[test]
fn local_update_with_inherits_referencing_allows_missing_source_policy_in_permissive_local() {
    let mut schema = Schema::new();
    let files_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
    ]);
    let files_policies = TablePolicies::new().with_update(
        Some(PolicyExpr::InheritsReferencing {
            operation: Operation::Update,
            source_table: "todos".into(),
            via_column: "file_id".into(),
            max_depth: None,
        }),
        PolicyExpr::True,
    );
    schema.insert(
        TableName::new("files"),
        TableSchema::with_policies(files_descriptor, files_policies),
    );

    let todos_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("file_id", ColumnType::Uuid)
            .nullable()
            .references("files"),
    ]);
    schema.insert(TableName::new("todos"), TableSchema::new(todos_descriptor));

    let sync_manager = SyncManager::new();
    let mut qm =
        create_query_manager_with_policy_mode(sync_manager, schema, RowPolicyMode::PermissiveLocal);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let file = qm
        .insert(
            &mut storage,
            "files",
            &[Value::Text("bob".into()), Value::Text("shared-file".into())],
        )
        .expect("seed file row");
    qm.insert(
        &mut storage,
        "todos",
        &[
            Value::Text("alice".into()),
            Value::Text("todo referencing file".into()),
            Value::Uuid(file.row_id),
        ],
    )
    .expect("seed referencing todo row");

    qm.update_with_session(
        &mut storage,
        file.row_id,
        &[
            Value::Text("bob".into()),
            Value::Text("updated by alice".into()),
        ],
        Some(&Session::new("alice")),
    )
    .expect(
        "permissive local runtimes should treat missing source UPDATE policy as allow for INHERITS_REFERENCING",
    );
}

#[test]
fn local_update_with_check_inherits_denies_when_parent_is_not_updateable() {
    let mut schema = Schema::new();
    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let folders_policies = TablePolicies::new().with_update(
        Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        PolicyExpr::Inherits {
            operation: Operation::Update,
            via_column: "parent_id".into(),
            max_depth: Some(10),
        },
    );
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor.clone(), folders_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let root = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .expect("create root");
    let child = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root.row_id),
            ],
        )
        .expect("create child");

    let update_err = qm
        .update_with_session(
            &mut storage,
            child.row_id,
            &[
                Value::Text("bob".into()),
                Value::Text("Child renamed".into()),
                Value::Uuid(root.row_id),
            ],
            Some(&Session::new("bob")),
        )
        .expect_err("update should fail inherited WITH CHECK");
    assert!(matches!(
        update_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Update
        } if table == TableName::new("folders")
    ));
}

#[test]
fn local_update_with_check_inherits_uses_visible_row_region_after_legacy_branch_history_is_removed()
{
    let mut schema = Schema::new();
    let folders_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let folders_policies = TablePolicies::new().with_update(
        Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        PolicyExpr::Inherits {
            operation: Operation::Update,
            via_column: "parent_id".into(),
            max_depth: Some(10),
        },
    );
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folders_descriptor.clone(), folders_policies),
    );

    let mut writer_qm = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);
    let mut storage = seeded_memory_storage(&writer_qm.schema_context().current_schema);

    let root = writer_qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .expect("create root");
    let child = writer_qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root.row_id),
            ],
        )
        .expect("create child");

    let mut qm = create_query_manager(SyncManager::new(), schema);
    let update_err = qm
        .update_with_session(
            &mut storage,
            child.row_id,
            &[
                Value::Text("bob".into()),
                Value::Text("Child renamed".into()),
                Value::Uuid(root.row_id),
            ],
            Some(&Session::new("bob")),
        )
        .expect_err("update should still evaluate inherited WITH CHECK from visible rows");
    assert!(matches!(
        update_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Update
        } if table == TableName::new("folders")
    ));
}

#[test]
fn rebac_inherits_no_cycle_passes() {
    use crate::query_manager::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    // Organizations table (no INHERITS)
    let org_desc = RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
    let org_policy =
        TablePolicies::new().with_select(PolicyExpr::eq_session("name", vec!["org".into()]));
    schema.insert(
        TableName::new("orgs"),
        TableSchema::with_policies(org_desc, org_policy),
    );

    // Teams table - INHERITS from orgs
    let team_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("org_id", ColumnType::Uuid)
            .nullable()
            .references("orgs"),
    ]);
    let team_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "org_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("teams"),
        TableSchema::with_policies(team_desc, team_policy),
    );

    // Projects table - INHERITS from teams
    let project_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("team_id", ColumnType::Uuid)
            .nullable()
            .references("teams"),
    ]);
    let project_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "team_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("projects"),
        TableSchema::with_policies(project_desc, project_policy),
    );

    // Should pass - this is a valid chain: projects → teams → orgs
    let result = validate_no_inherits_cycles(&schema);
    assert!(
        result.is_ok(),
        "Valid INHERITS chain should pass validation: {:?}",
        result
    );
}
