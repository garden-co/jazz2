//! Facade mutation lifecycle, local visibility, and operation-level authorization tests.

use super::*;

/// Test-only capability wrapper for a transport whose remote endpoint was
/// authenticated and admitted as a trusted backend.  The transport capability
/// is deliberately separate from `accept_subscriber_with_trust`: accepting a
/// peer does not let an arbitrary client-side transport self-authorize
/// delegated session bindings.
struct TrustedBackendTransport {
    inner: Box<dyn Transport>,
}

impl Transport for TrustedBackendTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), crate::wire::TransportError> {
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }

    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        self.inner.connection_session_context()
    }

    fn permits_delegated_sessions(&self) -> bool {
        true
    }
}

/// JSON pointer writes use the RFC 6901 array-index grammar, so a caller
/// cannot mutate `alice`'s first array member through a leading-zero path that
/// an equivalent read treats as absent.
#[test]
fn json_set_rejects_noncanonical_array_indices() {
    let mut value = serde_json::json!({ "items": ["first", "second"] });
    let error = super::super::mutations::apply_json_set(
        &mut value,
        "/items/01",
        serde_json::json!("replaced"),
    )
    .expect_err("leading-zero JSON array pointer must be rejected");
    assert_eq!(error.code, ErrorCode::Query);
    assert_eq!(error.message, "JSON array pointer token is not an index");
    assert_eq!(value, serde_json::json!({ "items": ["first", "second"] }));

    super::super::mutations::apply_json_set(&mut value, "/items/1", serde_json::json!("ok"))
        .expect("canonical JSON array index succeeds");
    assert_eq!(value, serde_json::json!({ "items": ["first", "ok"] }));
}

#[derive(Clone)]
struct RetryableChunkResolver {
    retry_after_ms: u32,
}

impl groove::chunks::MissingChunkResolver for RetryableChunkResolver {
    fn resolve(
        &self,
        _request: groove::chunks::ChunkRequest,
    ) -> groove::chunks::ChunkFuture<'_, Result<bytes::Bytes, groove::chunks::ChunkError>> {
        Box::pin(async move {
            Err(groove::chunks::ChunkError::Retryable {
                retry_after_ms: self.retry_after_ms,
            })
        })
    }
}

fn branch_column_reference_policy_schema() -> JazzSchema {
    let policy = PublicPolicyExpr::Exists {
        table: "branches".to_owned(),
        condition: Box::new(PublicPolicyExpr::And(vec![
            public_outer_eq("branch_key", "branch_id"),
            public_session_eq("owner", &["claims", "sub"]),
        ])),
    };
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("branches")
                    .fk_column("branch_key", "branches")
                    .column("name", PublicColumnType::Text)
                    .column("owner", PublicColumnType::Uuid)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(PublicPolicyExpr::True)
                            .with_insert(PublicPolicyExpr::True)
                            .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                            .with_delete(PublicPolicyExpr::True),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .fk_column("branch_id", "branches")
                    .column("title", PublicColumnType::Text)
                    .branch_by("branch_id")
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(policy.clone())
                            .with_insert(policy.clone())
                            .with_update(Some(policy.clone()), policy.clone())
                            .with_delete(policy),
                    ),
            ),
    )
}

fn branch_update_read_policy_schema() -> JazzSchema {
    let owner_write = public_session_eq("owner", &["claims", "sub"]);
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("branch", PublicColumnType::Text)
                .column("owner", PublicColumnType::Uuid)
                .column("published", PublicColumnType::Boolean)
                .column("secret", PublicColumnType::Text)
                .branch_by("branch")
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_literal_eq("published", PublicValue::Boolean(true)))
                        .with_insert(owner_write.clone())
                        .with_update(Some(owner_write.clone()), owner_write),
                ),
        ),
    )
}

fn branch_upsert_rule_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("branch", PublicColumnType::Text)
                    .column("title", PublicColumnType::Text)
                    .column("done", PublicColumnType::Boolean)
                    .branch_by("branch"),
            )
            .table(
                PublicTableSchemaBuilder::new("root_todos")
                    .column("title", PublicColumnType::Text)
                    .column("done", PublicColumnType::Boolean),
            ),
    )
}

#[test]
fn admitted_server_authorizes_branch_write_through_referenced_application_row() {
    let schema = branch_column_reference_policy_schema();
    let owner = AuthorSubject::for_test_bytes([0x76; 16]);
    let outsider = AuthorSubject::for_test_bytes([0x77; 16]);
    let branch = row(0x78);
    let selector = BranchSelector::new([("branch_id", Value::Uuid(branch.0))]);
    let server = open_core(0x75, AuthorSubject::SYSTEM, &schema);
    server
        .insert_with_id(
            "branches",
            branch,
            BTreeMap::from([
                ("branch_key".to_owned(), Value::Uuid(branch.0)),
                ("name".to_owned(), Value::String("draft".to_owned())),
                ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
            ]),
        )
        .unwrap();
    let owner_client = open_db(0x76, owner, &schema);
    let outsider_client = open_db(0x77, outsider, &schema);
    let (owner_transport, owner_server_transport) = duplex();
    let _owner_upstream = crate::db::block_on(owner_client.connect_upstream(owner_transport));
    let _owner_subscriber = server.accept_subscriber(owner_server_transport, owner);
    let (outsider_transport, outsider_server_transport) = duplex();
    let _outsider_upstream =
        crate::db::block_on(outsider_client.connect_upstream(outsider_transport));
    let _outsider_subscriber = server.accept_subscriber(outsider_server_transport, outsider);

    let accepted = owner_client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("allowed".to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(row(0x79)),
                target: crate::db::ExactWriteTarget::Branch(selector.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    owner_client.tick().unwrap();
    server.tick().unwrap();
    owner_client.tick().unwrap();
    assert_eq!(
        block_on(accepted.wait(DurabilityTier::Global)).unwrap(),
        accepted.mergeable_tx_id()
    );

    let denied = outsider_client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("denied".to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(row(0x7a)),
                target: crate::db::ExactWriteTarget::Branch(selector),
                ..Default::default()
            },
        )
        .unwrap();
    assert_authority_rejects_staged_write(&outsider_client, &server, &denied);
}

/// A session that can satisfy `UPDATE` policy but cannot select a branch row
/// cannot update it through the immediate facade. A mergeable transaction may
/// stage the same structural mutation, but the fate authority must reject it
/// from its complete policy inputs.
///
/// mallory ──update branch row──► read-hidden source ──► denied
#[test]
fn session_branch_updates_require_read_visibility_before_staging() {
    let schema = branch_update_read_policy_schema();
    let writer = AuthorSubject::for_test_bytes([0x7b; 16]);
    let branch = BranchSelector::new([("branch", Value::String("draft".to_owned()))]);
    let row_id = row(0x7c);
    let db = block_on(Db::open_history_complete(DbConfig {
        schema: schema.clone(),
        storage: rocks_storage(&schema),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x7a; 16]),
            author: AuthorSubject::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x7a))),
    }))
    .expect("open history-complete authority");
    db.set_test_provider_claims(writer, test_provider_claims(writer));

    let seed = block_on(db.insert(
        "todos",
        BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(writer.test_uuid())),
            ("published".to_owned(), Value::Bool(false)),
            (
                "secret".to_owned(),
                Value::String("read-hidden source".to_owned()),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row_id),
            target: crate::db::ExactWriteTarget::Branch(branch.clone()),
            ..Default::default()
        },
    ))
    .expect("seed hidden branch row");
    db.finalize_local_mergeable_commit_for_test(seed.mergeable_tx_id())
        .expect("settle seed row");

    let prepared = db.prepare_query(&db.table("todos")).expect("prepare query");
    let read_opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    }
    .branch_view(branch.clone(), None);
    assert!(
        block_on(db.all_for_identity(&prepared, read_opts.clone(), writer))
            .expect("writer's branch read")
            .is_empty()
    );

    let facade_error = match block_on(db.update(
        "todos",
        row_id,
        BTreeMap::from([
            ("branch".to_owned(), Value::String("draft".to_owned())),
            ("owner".to_owned(), Value::Uuid(writer.test_uuid())),
            ("published".to_owned(), Value::Bool(true)),
            ("secret".to_owned(), Value::String("replacement".to_owned())),
        ]),
        crate::db::UpdateOptions {
            identity: crate::db::WriteIdentity::Session(writer),
            target: crate::db::WriteTarget::BranchView {
                head: branch.clone(),
                base: None,
            },
            ..Default::default()
        },
    )) {
        Ok(_) => panic!("facade branch update must require read visibility"),
        Err(error) => error,
    };
    assert_eq!(facade_error.code, crate::db::ErrorCode::WriteRejected);
    assert!(facade_error.message.contains("UPDATE"));

    let upsert_error = match block_on(db.upsert(
        "todos",
        row_id,
        BTreeMap::from([("published".to_owned(), Value::Bool(true))]),
        crate::db::UpsertOptions {
            identity: crate::db::WriteIdentity::Session(writer),
            target: crate::db::WriteTarget::BranchView {
                head: branch.clone(),
                base: None,
            },
            ..Default::default()
        },
    )) {
        Ok(_) => panic!("branch upsert must not infer a hidden target is absent"),
        Err(error) => error,
    };
    assert_eq!(upsert_error.code, crate::db::ErrorCode::WriteRejected);
    assert!(upsert_error.message.contains("UPSERT"));

    let (_, tx_id) = block_on(db.transaction_for_identity(writer, async |tx| {
        tx.update(
            "todos",
            row_id,
            BTreeMap::from([("published".to_owned(), Value::Bool(true))]),
            crate::db::UpdateOptions {
                target: crate::db::WriteTarget::BranchView {
                    head: branch.clone(),
                    base: None,
                },
                ..Default::default()
            },
        )
        .await
    }))
    .expect("mergeable client staging must not issue a policy verdict");
    db.finalize_local_mergeable_commit_for_test(tx_id)
        .expect("history-complete fate authority settles the staged write");
    let transaction_state = db.write_state(tx_id).expect("staged transaction state");
    assert!(matches!(
        transaction_state,
        WriteState {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }
    ));

    assert!(
        block_on(db.all_for_identity(&prepared, read_opts.clone(), writer))
            .expect("denial does not disclose a branch row")
            .is_empty()
    );
    let authority_rows = block_on(db.all_for_identity(&prepared, read_opts, AuthorSubject::SYSTEM))
        .expect("authority can inspect the unchanged source");
    let table = &schema.tables[0];
    assert_eq!(authority_rows.len(), 1);
    assert_eq!(
        authority_rows[0].cell(table, "secret"),
        Some(Value::String("read-hidden source".to_owned()))
    );
}

/// A policy-free point update uses its known row id, while absent/deleted
/// targets retain the facade's existing rejection behavior. Tables with a
/// read policy deliberately retain the client-local query dispatch; client
/// replicas rely on upstream sync, rather than local policy re-evaluation, for
/// confidentiality.
///
/// policy-free root update ──► direct current-row lookup
/// policy-bearing root update ──► existing ClientLocal point-query dispatch
#[test]
fn point_update_preimage_fast_path_preserves_target_and_policy_dispatch() {
    let unscoped_schema = schema();
    let db = open_db(0x7d, AuthorSubject::SYSTEM, &unscoped_schema);
    let unscoped_owner = AuthorSubject::for_test_bytes([0x7d; 16]);
    let live = row(0x7e);
    let deleted = row(0x7f);
    let missing = row(0x80);

    for (row_id, title) in [(live, "live"), (deleted, "deleted")] {
        let write = db
            .insert(
                "todos",
                cells(title, false, unscoped_owner),
                InsertOptions {
                    row_id: Some(row_id),
                    ..Default::default()
                },
            )
            .expect("seed policy-free row");
        block_on(write.wait(DurabilityTier::Local)).expect("settle policy-free seed");
    }

    assert_eq!(
        crate::node::take_client_physical_row_query_calls_for_test(),
        0,
        "discard point-query calls from earlier tests on this thread"
    );
    let update = block_on(db.update(
        "todos",
        live,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
    ))
    .expect("known policy-free point target updates");
    block_on(update.wait(DurabilityTier::Local)).expect("settle point update");
    assert_eq!(
        crate::node::take_client_physical_row_query_calls_for_test(),
        0,
        "policy-free preimage must use the direct current-row lookup"
    );
    let rows = prepared_read(&db, &db.table("todos"));
    assert_eq!(
        rows.iter()
            .find(|candidate| candidate.row_uuid() == live)
            .and_then(|candidate| candidate.cell(&unscoped_schema.tables[0], "done")),
        Some(Value::Bool(true))
    );

    let missing_error = match block_on(db.update(
        "todos",
        missing,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
    )) {
        Ok(_) => panic!("absent point target stays rejected"),
        Err(error) => error,
    };
    assert_eq!(missing_error.code, crate::db::ErrorCode::WriteRejected);
    assert!(missing_error.message.contains("UPDATE"));

    let deletion = db
        .delete("todos", deleted, Default::default())
        .expect("delete policy-free row");
    block_on(deletion.wait(DurabilityTier::Local)).expect("settle deletion");
    let deleted_error = match block_on(db.update(
        "todos",
        deleted,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
    )) {
        Ok(_) => panic!("deleted point target stays rejected"),
        Err(error) => error,
    };
    assert_eq!(deleted_error.code, crate::db::ErrorCode::WriteRejected);
    assert!(deleted_error.message.contains("deleted"));

    let owner = AuthorSubject::for_test_bytes([0x81; 16]);
    let intruder = AuthorSubject::for_test_bytes([0x82; 16]);
    let scoped_schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .column("owner", PublicColumnType::Uuid)
                .column("body", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_session_eq("owner", &["claims", "sub"]))
                        .with_insert(PublicPolicyExpr::True)
                        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True),
                ),
        ),
    );
    let scoped = open_db(0x81, intruder, &scoped_schema);
    scoped.set_identity_claims(intruder, test_provider_claims(intruder));
    let hidden = row(0x83);
    let seed = scoped
        .insert(
            "documents",
            BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
                ("body".to_owned(), Value::String("private".to_owned())),
            ]),
            InsertOptions {
                row_id: Some(hidden),
                ..Default::default()
            },
        )
        .expect("authority seeds hidden row");
    block_on(seed.wait(DurabilityTier::Local)).expect("settle hidden seed");
    assert_eq!(
        crate::node::take_client_physical_row_query_calls_for_test(),
        0,
        "discard unrelated point-query calls before the policy-bearing update"
    );
    let policy_bearing_update = block_on(scoped.update(
        "documents",
        hidden,
        BTreeMap::from([("body".to_owned(), Value::String("leak".to_owned()))]),
        Default::default(),
    ))
    .expect("the manually resident row follows ordinary ClientLocal staging semantics");
    block_on(policy_bearing_update.wait(DurabilityTier::Local))
        .expect("settle policy-bearing update");
    assert_eq!(
        crate::node::take_client_physical_row_query_calls_for_test(),
        1,
        "a table with a read policy must retain ClientLocal point-query dispatch"
    );
}

#[test]
fn branch_view_upserts_reject_tombstones_and_preserve_other_insertability_cases() {
    let schema = branch_upsert_rule_schema();
    let db = open_db(0x7d, AuthorSubject::SYSTEM, &schema);
    let query = db.table("todos");
    let root_query = db.table("root_todos");
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "todos")
        .expect("todos table");
    let base = BranchSelector::new([("branch", Value::String("base".to_owned()))]);
    let head = BranchSelector::new([("branch", Value::String("head".to_owned()))]);
    let standalone_tombstone = row(0x7e);
    let standalone_inherited = row(0x7f);
    let standalone_absent = row(0x80);
    let transaction_tombstone = row(0x81);
    let transaction_inherited = row(0x82);
    let transaction_absent = row(0x83);
    let standalone_root_tombstone = row(0x84);
    let transaction_root_tombstone = row(0x85);

    for (row_id, title) in [
        (standalone_tombstone, "standalone tombstone"),
        (standalone_inherited, "standalone inherited"),
        (transaction_tombstone, "transaction tombstone"),
        (transaction_inherited, "transaction inherited"),
    ] {
        block_on(db.insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.to_owned())),
                ("done".to_owned(), Value::Bool(false)),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row_id),
                target: crate::db::ExactWriteTarget::Branch(base.clone()),
                ..Default::default()
            },
        ))
        .expect("seed base row");
    }
    for (row_id, title) in [
        (standalone_root_tombstone, "standalone root tombstone"),
        (transaction_root_tombstone, "transaction root tombstone"),
    ] {
        block_on(db.insert(
            "root_todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.to_owned())),
                ("done".to_owned(), Value::Bool(false)),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row_id),
                ..Default::default()
            },
        ))
        .expect("seed root row");
    }

    for row_id in [standalone_tombstone, transaction_tombstone] {
        block_on(db.delete(
            "todos",
            row_id,
            crate::db::DeleteOptions {
                target: crate::db::WriteTarget::BranchView {
                    head: head.clone(),
                    base: Some(BranchViewBase::current(base.clone())),
                },
                ..Default::default()
            },
        ))
        .expect("create head-local tombstone");
    }
    for row_id in [standalone_root_tombstone, transaction_root_tombstone] {
        block_on(db.delete("root_todos", row_id, Default::default()))
            .expect("create root tombstone");
    }

    let standalone_tombstone_error = expect_error(block_on(db.upsert(
        "todos",
        standalone_tombstone,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        crate::db::UpsertOptions {
            target: crate::db::WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::current(base.clone())),
            },
            ..Default::default()
        },
    )));
    assert_eq!(
        standalone_tombstone_error.code,
        crate::db::ErrorCode::WriteRejected
    );
    assert!(
        standalone_tombstone_error
            .message
            .contains("already deleted")
    );

    block_on(db.upsert(
        "todos",
        standalone_inherited,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        crate::db::UpsertOptions {
            target: crate::db::WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::current(base.clone())),
            },
            ..Default::default()
        },
    ))
    .expect("copy and patch inherited row");
    block_on(db.upsert(
        "todos",
        standalone_absent,
        BTreeMap::from([
            (
                "title".to_owned(),
                Value::String("standalone absent".to_owned()),
            ),
            ("done".to_owned(), Value::Bool(true)),
        ]),
        crate::db::UpsertOptions {
            target: crate::db::WriteTarget::BranchView {
                head: head.clone(),
                base: Some(BranchViewBase::current(base.clone())),
            },
            ..Default::default()
        },
    ))
    .expect("insert absent row in head");
    let standalone_root_error = expect_error(block_on(db.upsert(
        "root_todos",
        standalone_root_tombstone,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
    )));
    assert_eq!(
        standalone_root_error.code,
        crate::db::ErrorCode::WriteRejected
    );
    assert!(standalone_root_error.message.contains("already deleted"));

    block_on(db.transaction(async |tx| {
        let branch_target = || crate::db::WriteTarget::BranchView {
            head: head.clone(),
            base: Some(BranchViewBase::current(base.clone())),
        };
        let tombstone_error = tx
            .upsert(
                "todos",
                transaction_tombstone,
                BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
                crate::db::UpsertOptions {
                    target: branch_target(),
                    ..Default::default()
                },
            )
            .await
            .expect_err("transaction branch upsert must reject a head tombstone");
        assert_eq!(tombstone_error.code, crate::db::ErrorCode::WriteRejected);
        assert!(tombstone_error.message.contains("already deleted"));

        tx.upsert(
            "todos",
            transaction_inherited,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            crate::db::UpsertOptions {
                target: branch_target(),
                ..Default::default()
            },
        )
        .await?;
        tx.upsert(
            "todos",
            transaction_absent,
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("transaction absent".to_owned()),
                ),
                ("done".to_owned(), Value::Bool(true)),
            ]),
            crate::db::UpsertOptions {
                target: branch_target(),
                ..Default::default()
            },
        )
        .await?;

        let root_error = tx
            .upsert(
                "root_todos",
                transaction_root_tombstone,
                BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
                Default::default(),
            )
            .await
            .expect_err("transaction root upsert must match standalone tombstone rejection");
        assert_eq!(root_error.code, crate::db::ErrorCode::WriteRejected);
        assert!(root_error.message.contains("already deleted"));
        Ok(())
    }))
    .expect("commit accepted branch upserts");

    block_on(async {
        let mut node = db.node.node.lock().await;
        for row_id in [standalone_tombstone, transaction_tombstone] {
            assert!(
                node.local_content_winner_tx_id_in_branch("todos", &head, row_id)
                    .await
                    .expect("inspect rejected branch upsert")
                    .is_none(),
                "rejected branch upsert must not emit tombstone-hidden content"
            );
        }
    });

    let branch_rows = prepared_all(
        &db,
        &query,
        ReadOpts {
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        }
        .branch_view(head.clone(), Some(BranchViewBase::current(base.clone()))),
    );
    assert!(!row_ids(&branch_rows).contains(&standalone_tombstone));
    assert!(!row_ids(&branch_rows).contains(&transaction_tombstone));
    for (row_id, title) in [
        (standalone_inherited, "standalone inherited"),
        (standalone_absent, "standalone absent"),
        (transaction_inherited, "transaction inherited"),
        (transaction_absent, "transaction absent"),
    ] {
        let visible = branch_rows
            .iter()
            .find(|current| current.row_uuid() == row_id)
            .expect("successful branch upsert must be visible");
        assert_eq!(
            visible.cell(table, "title"),
            Some(Value::String(title.to_owned()))
        );
        assert_eq!(visible.cell(table, "done"), Some(Value::Bool(true)));
    }

    let root_rows = prepared_read(&db, &root_query);
    assert!(!row_ids(&root_rows).contains(&standalone_root_tombstone));
    assert!(!row_ids(&root_rows).contains(&transaction_root_tombstone));
}

#[test]
fn db_facade_mutation_lifecycle_writes_reads_deletes_and_restores() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];

    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells("draft todo", false),
            Default::default(),
        )
        .unwrap();
    let todo = write.row_uuid();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("draft todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(false)));

    let write = db
        .update(
            "todos",
            todo,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("draft todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));

    let write = db.delete("todos", todo, Default::default()).unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert!(prepared_read(&db, &query).is_empty());

    let write = db
        .restore(
            "todos",
            todo,
            Some(doctest_support::todo_cells("restored todo", true)),
            Default::default(),
        )
        .unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("restored todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));
}

/// A facade delete starts or extends only the deletion-register history.
///
/// alice writes content, then deletes it locally. The delete must not try to
/// use that content version as a deletion-register parent: the two histories
/// are independent and the first deletion therefore has no parent.
#[test]
fn delete_starts_a_deletion_history_without_parenting_content() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let row = row(0x9d);
    let inserted = doctest_support::block_on(db.insert(
        "todos",
        doctest_support::todo_cells("draft", false),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
    doctest_support::block_on(inserted.wait(DurabilityTier::Local)).unwrap();

    let deleted = db.delete("todos", row, Default::default()).unwrap();
    doctest_support::block_on(deleted.wait(DurabilityTier::Local)).unwrap();
    assert!(prepared_read(&db, &db.table("todos")).is_empty());
}

/// A policy-free partial update inherits its omitted cells directly from the
/// physical winner; it must not prepare a serving query merely to prove
/// unconditional read visibility.
#[test]
fn policy_free_partial_update_skips_the_serving_read_query() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells("original", false),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    db.node.node.borrow_mut().reset_query_engine_read_metrics();
    let write = db
        .update(
            "todos",
            row,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let metrics = db.node.node.borrow().query_engine_read_metrics().clone();
    assert_eq!(
        metrics.source_primary_key_scans, 0,
        "policy-free partial updates must not prepare a physical serving query"
    );
    assert_eq!(metrics.source_full_scans, 0);
    let rows = prepared_read(&db, &db.table("todos"));
    assert_eq!(rows.len(), 1);
    let table = &doctest_support::schema().tables[0];
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("original".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn full_row_replacement_cannot_bless_an_inherited_large_value_descriptor() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let chunks = std::rc::Rc::new(groove::chunks::MemoryChunkStorage::new());
    block_on(async {
        db.node.node.lock().await.set_chunk_storage(chunks);
    });
    let title = "x".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 1);
    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells(&title, false),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();
    block_on(write.wait(DurabilityTier::Local)).unwrap();
    let descriptor = block_on(async {
        let mut node = db.node.node.lock().await;
        match node
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
        {
            Value::Large(value_ref) => value_ref,
            other => panic!("oversized title stayed inline: {other:?}"),
        }
    });

    let error = match block_on(db.update(
        "todos",
        row,
        BTreeMap::from([
            ("title".to_owned(), Value::Large(descriptor)),
            ("done".to_owned(), Value::Bool(true)),
        ]),
        Default::default(),
    )) {
        Ok(_) => panic!("raw full-row input must not bless an inherited descriptor"),
        Err(error) => error,
    };
    assert!(
        error.message.contains("unverified large-value descriptor"),
        "unexpected descriptor rejection: {error:?}"
    );
}

#[test]
fn high_level_large_value_apis_keep_descriptors_private_and_publish_edits() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let chunks = std::rc::Rc::new(groove::chunks::MemoryChunkStorage::new());
    block_on(async {
        db.node.node.lock().await.set_chunk_storage(chunks.clone());
    });
    let mut title = "a".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 257);
    title.push_str("🙂tail");
    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells(&title, false),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();
    block_on(write.wait(DurabilityTier::Local)).unwrap();
    let original_ref = block_on(async {
        let mut node = db.node.node.lock().await;
        match node
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
        {
            Value::Large(value_ref) => value_ref,
            other => panic!("oversized title stayed inline: {other:?}"),
        }
    });

    let unrelated = db
        .update(
            "todos",
            row,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap();
    block_on(unrelated.wait(DurabilityTier::Local)).unwrap();
    let after_unrelated = block_on(async {
        let mut node = db.node.node.lock().await;
        match node
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
        {
            Value::Large(value_ref) => value_ref,
            other => panic!("updated title became inline: {other:?}"),
        }
    });
    assert_eq!(original_ref, after_unrelated);

    assert_eq!(
        block_on(db.read_value_range("todos", row, "title", 10..18)).unwrap(),
        b"aaaaaaaa"
    );
    assert_eq!(
        block_on(db.read_text_utf16_range(
            "todos",
            row,
            "title",
            title.encode_utf16().count() as u64 - 6..title.encode_utf16().count() as u64 - 4,
        ))
        .unwrap(),
        "🙂"
    );

    let append = block_on(db.append_value("todos", row, "title", b"/appended".to_vec())).unwrap();
    block_on(append.wait(DurabilityTier::Local)).unwrap();
    title.push_str("/appended");

    let splice = block_on(db.splice_value("todos", row, "title", 4, 3, b"XYZ".to_vec())).unwrap();
    block_on(splice.wait(DurabilityTier::Local)).unwrap();
    title.replace_range(4..7, "XYZ");
    let edited_ref = block_on(async {
        let mut node = db.node.node.lock().await;
        match node
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
        {
            Value::Large(value_ref) => value_ref,
            other => panic!("edited title became inline: {other:?}"),
        }
    });
    assert_eq!(original_ref.root, edited_ref.root);

    let rows = db
        .read(&db.prepare_query(&db.table("todos")).unwrap())
        .unwrap();
    assert_eq!(
        rows[0].cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::String(title.clone()))
    );

    // This deliberately plants the internal descriptor immediately before
    // the binding-only hydrator. The corresponding public WASM receipt covers the
    // full encode/decode path; this focused lower-level assertion proves the
    // boundary still rejects a regression even if a maintained terminal
    // happens to materialize the same row earlier in the pipeline.
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let mut event = block_on(subscription.next_raw()).unwrap();
    // Storage-backed maintained roots retain only an exact `(table, row, tx)`
    // identity internally and may therefore carry an indirect descriptor in
    // this raw event. Bindings always cross the explicit hydration boundary
    // before exposing it to application code; keep the public assertion below
    // on that boundary rather than requiring opening to synchronously fetch a
    // cold chunk (which would deadlock an edge before its I/O pump starts).
    block_on(db.hydrate_subscription_event_for_binding(&mut event)).unwrap();
    let (descriptor, title_index, terminal_value) = {
        let SubscriptionEvent::Delta { added, .. } = &mut event else {
            panic!("expected opening subscription delta");
        };
        let (descriptor, record) = added[0].row.encoded_record();
        let descriptor = descriptor.clone();
        let mut values = descriptor.bind(record).to_values().unwrap();
        let title_index = values
            .iter()
            .position(|value| {
                matches!(value, Value::String(value) if value == &title)
                    || matches!(value, Value::Nullable(Some(value)) if matches!(value.as_ref(), Value::String(value) if value == &title))
            })
            .expect("opening row contains the logical title");
        values[title_index] = match &values[title_index] {
            Value::Nullable(_) => Value::Nullable(Some(Box::new(Value::Large(edited_ref.clone())))),
            _ => Value::Large(edited_ref.clone()),
        };
        added[0].row = CurrentRow::new(
            "todos",
            OwnedRecord::new(descriptor.create(&values).unwrap(), descriptor.clone()),
        );
        assert!(
            matches!(
                added[0]
                    .row
                    .cell(&doctest_support::schema().tables[0], "title"),
                Some(Value::Large(_))
            ),
            "planted positive: the maintained event reaches the binding with a physical descriptor"
        );
        (
            descriptor,
            title_index,
            added[0].row.encoded_record().1.to_vec(),
        )
    };
    let mut ordinary_event = event.clone();
    let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = &mut event
    else {
        unreachable!("opening subscription event is a delta");
    };
    terminal_operations.push(groove::ivm::TerminalOperation {
        root_descriptor: descriptor.clone(),
        root_key: Vec::new(),
        path: Vec::new(),
        edit: groove::ivm::TerminalEdit::Update {
            key: Vec::new(),
            value: terminal_value.clone(),
        },
    });
    // A locally absent chunk with no peer retry instruction is terminal. The
    // event remains physically intact so a caller can safely abandon it; it is
    // never silently retried forever.
    block_on(async {
        db.node
            .node
            .lock()
            .await
            .set_chunk_storage(std::rc::Rc::new(groove::chunks::MemoryChunkStorage::new()));
        db.node
            .node
            .lock()
            .await
            .set_missing_chunk_resolver(std::rc::Rc::new(groove::chunks::UnavailableChunkResolver));
    });
    assert!(matches!(
        block_on(db.hydrate_subscription_event_for_binding_outcome(&mut ordinary_event)),
        Err(BindingHydrationError::Error(_))
    ));
    block_on(async {
        db.node.node.lock().await.set_chunk_storage(chunks.clone());
    });
    block_on(db.hydrate_subscription_event_for_binding(&mut ordinary_event)).unwrap();
    let SubscriptionEvent::Delta { added, .. } = ordinary_event else {
        panic!("expected opening subscription delta");
    };
    assert_eq!(
        added[0]
            .row
            .cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::String(title.clone())),
        "the subscription binding boundary must materialize the indirect text scalar"
    );
    block_on(db.hydrate_subscription_event_for_binding(&mut event)).unwrap();
    let mut nested_event = event.clone();
    let SubscriptionEvent::Delta {
        added: terminal_added,
        terminal_operations,
        ..
    } = event
    else {
        panic!("expected terminal subscription delta");
    };
    assert!(matches!(
        terminal_added[0]
            .row
            .cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::Large(_))
    ));
    let groove::ivm::TerminalEdit::Update { value, .. } = &terminal_operations[0].edit else {
        unreachable!("planted terminal operation is an update");
    };
    let terminal_values = descriptor.bind(value).to_values().unwrap();
    assert!(
        matches!(&terminal_values[title_index], Value::String(value) if value == &title)
            || matches!(
                &terminal_values[title_index],
                Value::Nullable(Some(value))
                    if matches!(value.as_ref(), Value::String(value) if value == &title)
            ),
        "structured terminal operations must not encode physical indirect scalars for bindings"
    );

    // A root terminal insertion can contain a whole collected tree. Hydration
    // must descend through arrays and nested records before that record reaches
    // the binding, rather than only looking for a top-level descriptor.
    let nested_root_descriptor = RecordDescriptor::new([(
        "children",
        groove::records::ValueType::Array(Box::new(groove::records::ValueType::Record(Box::new(
            descriptor,
        )))),
    )]);
    let root_insert_value = nested_root_descriptor
        .create(&[Value::Array(vec![Value::Record(OwnedRecord::new(
            terminal_value.clone(),
            descriptor,
        ))])])
        .unwrap();
    let mut root_insert_event = nested_event.clone();
    let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = &mut root_insert_event
    else {
        unreachable!("expected terminal subscription delta");
    };
    terminal_operations.clear();
    terminal_operations.push(groove::ivm::TerminalOperation {
        root_descriptor: nested_root_descriptor,
        root_key: Vec::new(),
        path: Vec::new(),
        edit: groove::ivm::TerminalEdit::Insert {
            index: 0,
            key: Vec::new(),
            value: root_insert_value,
        },
    });
    block_on(db.hydrate_subscription_event_for_binding(&mut root_insert_event)).unwrap();
    let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = root_insert_event
    else {
        unreachable!("expected terminal subscription delta");
    };
    let groove::ivm::TerminalEdit::Insert { value, .. } = &terminal_operations[0].edit else {
        unreachable!("expected root terminal insertion");
    };
    let root_values = nested_root_descriptor.bind(value).to_values().unwrap();
    let [Value::Array(children)] = root_values.as_slice() else {
        unreachable!("expected root terminal collection");
    };
    let [Value::Record(child)] = children.as_slice() else {
        unreachable!("expected one root terminal child");
    };
    let root_child_values = descriptor.bind(child.raw()).to_values().unwrap();
    assert!(
        matches!(&root_child_values[title_index], Value::String(value) if value == &title)
            || matches!(
                &root_child_values[title_index],
                Value::Nullable(Some(value))
                    if matches!(value.as_ref(), Value::String(value) if value == &title)
            ),
        "root terminal insertions must recursively materialize nested large values"
    );

    // A nested terminal operation keeps the root descriptor for layout
    // discovery, but its payload is a child record. Hydrating it as the root
    // record corrupts the raw bytes and only becomes visible when the binding
    // later decodes the operation.
    let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = &mut nested_event
    else {
        unreachable!("expected terminal subscription delta");
    };
    terminal_operations.clear();
    terminal_operations.push(groove::ivm::TerminalOperation {
        root_descriptor: nested_root_descriptor,
        root_key: Vec::new(),
        path: vec![groove::ivm::TerminalPathSegment::Collection(
            "children".to_owned(),
        )],
        edit: groove::ivm::TerminalEdit::Insert {
            index: 0,
            key: Vec::new(),
            value: terminal_value.clone(),
        },
    });
    block_on(db.hydrate_subscription_event_for_binding(&mut nested_event)).unwrap();
    let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = &nested_event
    else {
        unreachable!("expected terminal subscription delta");
    };
    let groove::ivm::TerminalEdit::Insert { value, .. } = &terminal_operations[0].edit else {
        unreachable!("expected nested terminal insertion");
    };
    let nested_values = descriptor.bind(value).to_values().unwrap();
    assert!(
        matches!(&nested_values[title_index], Value::String(value) if value == &title)
            || matches!(
                &nested_values[title_index],
                Value::Nullable(Some(value))
                    if matches!(value.as_ref(), Value::String(value) if value == &title)
            ),
        "nested terminal payloads must use their child descriptor when hydrating large values"
    );

    // Follow an actual Collection/Key/Collection terminal path and preserve
    // the retained payload exactly when its chunk is only retryably absent.
    let nested_child_descriptor = RecordDescriptor::new([(
        "notes",
        groove::records::ValueType::Array(Box::new(groove::records::ValueType::Record(Box::new(
            descriptor,
        )))),
    )]);
    let deep_root_descriptor = RecordDescriptor::new([(
        "children",
        groove::records::ValueType::Array(Box::new(groove::records::ValueType::Record(Box::new(
            nested_child_descriptor,
        )))),
    )]);
    let mut deep_event = nested_event.clone();
    let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = &mut deep_event
    else {
        unreachable!("expected terminal subscription delta");
    };
    terminal_operations.clear();
    terminal_operations.push(groove::ivm::TerminalOperation {
        root_descriptor: deep_root_descriptor,
        root_key: Vec::new(),
        path: vec![
            groove::ivm::TerminalPathSegment::Collection("children".to_owned()),
            groove::ivm::TerminalPathSegment::Key(vec![1]),
            groove::ivm::TerminalPathSegment::Collection("notes".to_owned()),
        ],
        edit: groove::ivm::TerminalEdit::Insert {
            index: 0,
            key: Vec::new(),
            value: terminal_value,
        },
    });
    let retained_deep_value = match &deep_event {
        SubscriptionEvent::Delta {
            terminal_operations,
            ..
        } => match &terminal_operations[0].edit {
            groove::ivm::TerminalEdit::Insert { value, .. } => value.clone(),
            _ => unreachable!("expected deep terminal insertion"),
        },
        _ => unreachable!("expected terminal subscription delta"),
    };
    block_on(async {
        db.node
            .node
            .lock()
            .await
            .set_chunk_storage(std::rc::Rc::new(groove::chunks::MemoryChunkStorage::new()));
        db.node
            .node
            .lock()
            .await
            .set_missing_chunk_resolver(std::rc::Rc::new(RetryableChunkResolver {
                retry_after_ms: 37,
            }));
    });
    assert!(matches!(
        block_on(db.hydrate_subscription_event_for_binding_outcome(&mut deep_event)),
        Err(BindingHydrationError::RetryableChunkUnavailable { retry_after_ms: 37 })
    ));
    let retained_after_retry = match &deep_event {
        SubscriptionEvent::Delta {
            terminal_operations,
            ..
        } => match &terminal_operations[0].edit {
            groove::ivm::TerminalEdit::Insert { value, .. } => value,
            _ => unreachable!("expected deep terminal insertion"),
        },
        _ => unreachable!("expected terminal subscription delta"),
    };
    assert_eq!(
        retained_after_retry, &retained_deep_value,
        "retryable hydration must leave the retained terminal payload unchanged"
    );
    block_on(async {
        let mut node = db.node.node.lock().await;
        node.set_chunk_storage(chunks.clone());
        node.set_missing_chunk_resolver(std::rc::Rc::new(groove::chunks::UnavailableChunkResolver));
    });
    block_on(db.hydrate_subscription_event_for_binding(&mut deep_event)).unwrap();
    let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = deep_event
    else {
        unreachable!("expected terminal subscription delta");
    };
    let groove::ivm::TerminalEdit::Insert { value, .. } = &terminal_operations[0].edit else {
        unreachable!("expected deep terminal insertion");
    };
    let deep_values = descriptor.bind(value).to_values().unwrap();
    assert!(
        matches!(&deep_values[title_index], Value::String(value) if value == &title)
            || matches!(
                &deep_values[title_index],
                Value::Nullable(Some(value))
                    if matches!(value.as_ref(), Value::String(value) if value == &title)
            ),
        "deep terminal paths must hydrate their child descriptor after retry"
    );

    let mut snapshot = RelationSnapshot {
        root_count: 1,
        rows: vec![added[0].row.clone()],
        edges: Vec::new(),
    };
    let (descriptor, record) = snapshot.rows[0].encoded_record();
    let descriptor = descriptor.clone();
    let mut values = descriptor.bind(record).to_values().unwrap();
    values[title_index] = match &values[title_index] {
        Value::Nullable(_) => Value::Nullable(Some(Box::new(Value::Large(edited_ref.clone())))),
        _ => Value::Large(edited_ref.clone()),
    };
    snapshot.rows[0] = CurrentRow::new(
        "todos",
        OwnedRecord::new(descriptor.create(&values).unwrap(), descriptor),
    );
    assert!(matches!(
        snapshot.rows[0].cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::Large(_))
    ));
    block_on(db.hydrate_relation_snapshot_for_binding(&mut snapshot)).unwrap();
    assert_eq!(
        snapshot.rows[0].cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::String(title.clone())),
        "relation snapshots must not encode physical indirect scalars for bindings"
    );

    let json = format!(
        "{{\"padding\":\"{}\",\"selected\":{{\"answer\":42}}}}",
        "p".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES)
    );
    let json_row = db
        .insert(
            "todos",
            doctest_support::todo_cells(&json, false),
            Default::default(),
        )
        .unwrap()
        .row_uuid();
    assert_eq!(
        block_on(db.read_json_pointer("todos", json_row, "title", "/selected/answer")).unwrap(),
        Some(serde_json::json!(42))
    );
}

#[test]
fn partial_value_update_publishes_text_splice_and_ordinary_patch_atomically() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut title = "a".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 32);
    title.push_str("tail");
    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells(&title, false),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    let start = title.len() as u64 - 4;
    let write = block_on(db.update_with_large_value_mutations(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        vec![LargeValueUpdate::Splice {
            column: "title".to_owned(),
            within: LargeValueUpdatePage::TextUtf8 {
                from: start,
                to: start + 4,
            },
            splices: vec![LargeValueUpdateSplice {
                at: 0,
                delete: 4,
                insert: b"done".to_vec(),
            }],
        }],
    ))
    .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    title.replace_range(title.len() - 4.., "done");
    let rows = prepared_read(&db, &db.table("todos"));
    let table = &doctest_support::schema().tables[0];
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String(title.clone()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));

    let error = match block_on(db.update_with_large_value_mutations(
        "todos",
        row,
        BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
        vec![LargeValueUpdate::JsonSet {
            column: "title".to_owned(),
            edits: vec![JsonSetEdit {
                at: "/not-a-text-edit".to_owned(),
                value: serde_json::json!("malformed raw descriptor"),
            }],
        }],
    )) {
        Ok(_) => panic!("a JSON descriptor for a text column must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::Schema);
    assert_eq!(error.message, "JSON set requires a JSON column");

    let rows = prepared_read(&db, &db.table("todos"));
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String(title)),
        "a schema-rejected raw descriptor must not publish a partial row version"
    );
    assert_eq!(
        rows[0].cell(table, "done"),
        Some(Value::Bool(true)),
        "ordinary cells staged with a rejected descriptor must roll back too"
    );
}

#[test]
fn high_level_large_value_reads_authorize_before_descriptor_lookup() {
    let allowed = "readable-large-value/".repeat(5_000);
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .column("body", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::eq_literal(
                            "body",
                            PublicValue::Text(allowed.clone()),
                        ))
                        .with_insert(PublicPolicyExpr::True)
                        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                        .with_delete(PublicPolicyExpr::True),
                ),
        ),
    );
    let reader = AuthorSubject::for_test_bytes([0x4e; 16]);
    let db = open_db(0x4e, reader, &schema);
    let visible = row(0x4e);
    let hidden = row(0x4f);
    db.insert(
        "documents",
        BTreeMap::from([("body".to_owned(), Value::String(allowed.clone()))]),
        InsertOptions {
            row_id: Some(visible),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "documents",
        BTreeMap::from([(
            "body".to_owned(),
            Value::String(format!("{}x", &allowed[..allowed.len() - 1])),
        )]),
        InsertOptions {
            row_id: Some(hidden),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(
        block_on(db.read_value_range("documents", visible, "body", 0..8)).unwrap(),
        b"readable"
    );
    let denied = block_on(db.read_value_range("documents", hidden, "body", 0..8)).unwrap_err();
    assert_eq!(denied.code, ErrorCode::NotObserved);
}

#[test]
fn nullable_large_text_uses_the_same_high_level_read_and_edit_surface() {
    let schema = build_public_db_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("notes").nullable_column("body", PublicColumnType::Text),
    ));
    let db = open_db(0x4d, AuthorSubject::SYSTEM, &schema);
    let row = row(0x4d);
    let body = "n".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 73);
    db.insert(
        "notes",
        BTreeMap::from([(
            "body".to_owned(),
            Value::Nullable(Some(Box::new(Value::String(body.clone())))),
        )]),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(
        block_on(db.read_value_range("notes", row, "body", 7..13)).unwrap(),
        b"nnnnnn"
    );
    block_on(db.append_value("notes", row, "body", b"end".to_vec())).unwrap();
    let result = db
        .read(&db.prepare_query(&db.table("notes")).unwrap())
        .unwrap();
    assert_eq!(
        result[0].cell(&schema.tables[0], "body"),
        Some(Value::Nullable(Some(Box::new(Value::String(format!(
            "{body}end"
        ))))))
    );
}

#[test]
fn db_facade_runs_saas_shaped_local_lane_end_to_end() {
    let schema = schema();
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: owner,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x11))),
    }))
    .unwrap();

    let query = Query::from("todos");
    let write = db
        .insert(
            "todos",
            cells("ship facade", false, owner),
            Default::default(),
        )
        .unwrap();
    let todo = write.row_uuid();
    let table = &schema.tables[0];
    let rows = prepared_read(&db, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("ship facade".to_owned()))
    );
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    db.update(
        "todos",
        todo,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
    )
    .unwrap();
    let updated = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn core_db_self_finalizes_own_writes_to_global() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &schema);

    let write = core
        .insert("todos", cells("authority write", false, owner))
        .unwrap();
    // No upstream, no connection: a Core Db is the authority, so its own
    // write is immediately Accepted/Global.
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    assert_eq!(core.read(&Query::from("todos")).unwrap().len(), 1);
}

#[test]
fn db_sync_surface_uploads_client_writes_for_authority_fate() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    // A local client write is Local and queued for upload.
    let write = client
        .insert(
            "todos",
            cells("from client", false, author),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();

    // Drive: client uploads the commit unit -> server (authority) accepts to
    // Global and sends the fate back -> client applies the fate.
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    // The client's own write reached Global once the authority fate landed.
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    // The authority received and applied the uploaded row.
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn byte_wire_uploads_client_writes_for_authority_fate() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = byte_duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert(
            "todos",
            cells("from client", false, author),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn db_sync_surface_uploads_client_exclusive_commit_for_global_fate() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe1);
    let exclusive = client.exclusive_tx().unwrap();
    exclusive
        .insert(
            "todos",
            cells("exclusive", false, author),
            crate::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .unwrap();
    let tx_id = exclusive.commit().unwrap();

    assert_eq!(
        client.write_state(tx_id).unwrap(),
        WriteState {
            fate: Fate::Pending,
            global_time: None,
            durability: DurabilityTier::Local,
        }
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let state = client.write_state(tx_id).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert!(
        state.global_time.is_some(),
        "the authority acceptance carries its assigned Global timestamp"
    );
    assert_eq!(state.durability, DurabilityTier::Global);
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn anonymous_authority_exclusive_write_is_rejected_before_policy_evaluation() {
    let schema = schema();
    let anonymous =
        AuthorSubject::from_canonical(r#"["urn:jazz:anonymous","exclusive-anonymous"]"#).unwrap();
    let core = open_core_with_claims(0x5e, anonymous, &schema, BTreeMap::new());
    // This authority-local path cannot use a remote session transport. Its
    // accountless subject must fail at provisional durable authorship, before
    // policy evaluation can stage a row or transaction.
    let Err(error) = core.exclusive_tx() else {
        panic!("accountless author unexpectedly opened a durable transaction")
    };

    assert_eq!(error.code, ErrorCode::Protocol, "{error:?}");
    assert!(error.message.contains("admitted account author"));
    assert!(core.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn db_sync_surface_returns_exclusive_conflict_fate_to_client() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe2);
    let first = client.exclusive_tx().unwrap();
    let second = client.exclusive_tx().unwrap();
    first
        .insert(
            "todos",
            cells("first", false, author),
            crate::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .unwrap();
    second
        .insert(
            "todos",
            cells("second", false, author),
            crate::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .unwrap();
    let first_tx = first.commit().unwrap();
    let second_error = second.commit().unwrap_err();
    assert_eq!(second_error.code, ErrorCode::TransactionConflict);
    assert!(second_error.message.contains("visible parent changed"));

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let state = client.write_state(first_tx).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert!(
        state.global_time.is_some(),
        "the authority acceptance carries its assigned Global timestamp"
    );
    assert_eq!(state.durability, DurabilityTier::Global);
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    let table = &schema.tables[0];
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("first".to_owned()))
    );
}

/// An authority rejection with no application waiter is delivered once through
/// the mutation-error callback on the following scheduled database tick. This
/// is an ordinary client connection, so the fate has no edge-forwarding route
/// and must still run the local write-state handler.
#[test]
fn unhandled_rejection_is_delivered_as_mutation_error() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    client.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));

    let write = client
        .insert(
            "todos",
            cells("rejected", false, author),
            Default::default(),
        )
        .unwrap();
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .unwrap();

    client.tick().unwrap();
    assert!(events.borrow().is_empty());
    client.tick().unwrap();

    let events = events.borrow();
    assert_eq!(events.len(), 1);
    assert_eq!(
        client.write_state(write.mergeable_tx_id()).unwrap(),
        WriteState {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: DurabilityTier::Edge,
        }
    );
    assert_eq!(events[0].code, "permission_denied");
    assert_eq!(
        events[0].transaction.transaction_id,
        TransactionId::from_committed_tx(write.mergeable_tx_id())
    );
    assert_eq!(events[0].transaction.kind, TransactionKind::Mergeable);
}

/// A queued empty root update validates its target asynchronously, but still
/// exposes one stable reservation to the synchronous caller. Once validation
/// completes, that reservation aliases the already-current row transaction;
/// it must never manufacture a second row version or become unobservable.
#[test]
fn queued_empty_update_aliases_current_transaction_without_publishing() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc0; 16]);
    let db = open_db(0xc0, author, &schema);
    let row = row(0xc1);
    crate::db::block_on(db.insert(
        "todos",
        cells("existing", false, author),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .expect("seed current row");

    let preceding = db
        .enqueue_update(
            "todos".to_owned(),
            row,
            cells("first queued update", false, author),
            Default::default(),
        )
        .expect("queue a predecessor before the no-op");

    let queued = db
        .enqueue_update("todos".to_owned(), row, BTreeMap::new(), Default::default())
        .expect("synchronous facade reserves an empty update");
    let alias_weak = Rc::downgrade(
        queued
            .queued_alias
            .as_ref()
            .expect("empty queued update owns a handle-local completion alias"),
    );
    let reserved_tx_id = queued.mergeable_tx_id();
    assert_ne!(
        reserved_tx_id,
        preceding.mergeable_tx_id(),
        "reservation is a fresh request identity"
    );
    assert_eq!(
        crate::db::block_on(queued.write_state())
            .expect("pending queued write state")
            .fate,
        Fate::Pending
    );
    let waited = Rc::new(RefCell::new(None));
    let waited_for_callback = Rc::clone(&waited);
    db.wait_for_write_with(&queued, DurabilityTier::Local, move |outcome| {
        *waited_for_callback.borrow_mut() = Some(outcome);
    });
    assert!(
        waited.borrow().is_none(),
        "a binding-owned wait remains pending until its queued update has been admitted"
    );

    db.drive_queued_mutation_once();
    assert_eq!(
        crate::db::block_on(queued.write_state())
            .expect("no-op remains pending behind its FIFO predecessor")
            .fate,
        Fate::Pending,
        "the no-op cannot observe or alias a row version before earlier queued work completes"
    );

    db.tick()
        .expect("queued no-op validates its current target");
    db.tick()
        .expect("binding-owned wait observes the completed no-op target");

    assert_eq!(
        waited
            .borrow_mut()
            .take()
            .expect("binding-owned wait resolves after the no-op target is known")
            .expect("current transaction satisfies local durability"),
        reserved_tx_id,
        "binding wait preserves the queued request identity while observing its current target"
    );

    assert_eq!(
        crate::db::block_on(queued.wait(DurabilityTier::Local))
            .expect("the reservation resolves through the current transaction"),
        reserved_tx_id,
        "queued callers retain the request identity they were synchronously given"
    );
    assert_eq!(
        crate::db::block_on(queued.write_state())
            .expect("write handle follows the completed no-op alias"),
        db.write_state(preceding.mergeable_tx_id())
            .expect("current row transaction remains observable")
    );
    assert!(
        db.write_state(reserved_tx_id).is_err(),
        "a no-op reservation is not a durable node transaction; only its returned handle owns the completion alias"
    );
    let current_row = prepared_read(&db, &Query::from("todos"))
        .into_iter()
        .next()
        .expect("one seeded row");
    assert_eq!(
        crate::db::block_on(db.node.node().borrow_mut().current_row_tx_id(&current_row))
            .expect("current-row transaction lookup"),
        preceding.mergeable_tx_id(),
        "the empty update must not publish a new row version"
    );
    drop(queued);
    assert!(
        alias_weak.upgrade().is_none(),
        "successful no-op aliases are retained only by their write handles"
    );
}

/// A queued no-op can observe the still-pending current transaction at a
/// stricter tier. Its synthetic request must report that rejection as its own,
/// without consuming the current transaction's independently registered
/// mutation-error ownership.
#[test]
fn queued_empty_update_rejection_does_not_consume_its_target_error() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let client = open_db(0xc2, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let current = client
        .insert(
            "todos",
            cells("pending current row", false, author),
            Default::default(),
        )
        .expect("create the pending current transaction");
    let target_tx_id = current.mergeable_tx_id();
    let alias = client
        .enqueue_update(
            "todos".to_owned(),
            current.row_uuid(),
            BTreeMap::new(),
            Default::default(),
        )
        .expect("queue empty update against pending current row");
    let public_tx_id = alias.mergeable_tx_id();
    client
        .tick()
        .expect("empty update resolves its completion target before the fate arrives");

    let target_outcome = Rc::new(RefCell::new(None));
    let target_callback = Rc::clone(&target_outcome);
    client.wait_for_transaction_with(target_tx_id, DurabilityTier::Edge, move |outcome| {
        *target_callback.borrow_mut() = Some(outcome);
    });
    let alias_outcome = Rc::new(RefCell::new(None));
    let alias_callback = Rc::clone(&alias_outcome);
    client.wait_for_write_with(&alias, DurabilityTier::Edge, move |outcome| {
        *alias_callback.borrow_mut() = Some(outcome);
    });
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id: target_tx_id,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .expect("authority fate reaches client");
    client.tick().expect("fate settles both active observers");

    let alias_error = alias_outcome
        .borrow_mut()
        .take()
        .expect("aliased write observer resolves")
        .expect_err("target rejection is surfaced through the alias");
    assert_eq!(alias_error.code, ErrorCode::WriteRejected);
    assert!(
        alias_error.message.contains(&format!("{public_tx_id:?}")),
        "the alias reports its public request identity"
    );
    assert!(
        !alias_error.message.contains(&format!("{target_tx_id:?}")),
        "the private completion target does not leak through the alias error"
    );
    let target_error = target_outcome
        .borrow_mut()
        .take()
        .expect("target observer resolves independently")
        .expect_err("target remains responsible for its own rejection");
    assert_eq!(target_error.code, ErrorCode::WriteRejected);
    assert!(
        target_error.message.contains(&format!("{target_tx_id:?}")),
        "the target observer retains its own transaction identity"
    );
    assert!(
        client
            .node
            .node()
            .borrow()
            .rejected_transaction(target_tx_id)
            .is_none(),
        "the target observer, not the alias, consumed its mutation-error ownership"
    );
}

/// A live application waiter consumes an authority rejection and prevents the
/// fallback mutation-error callback from firing, including when the fate has
/// no edge-forwarding route and only the ordinary local handler can notify it.
#[test]
fn waited_rejection_is_not_delivered_as_mutation_error() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let client = open_db(0xc2, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    client.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));

    let write = client
        .insert(
            "todos",
            cells("waited rejection", false, author),
            Default::default(),
        )
        .unwrap();
    let wait_result = Rc::new(RefCell::new(None));
    let callback_result = Rc::clone(&wait_result);
    client.wait_for_transaction_with(
        write.mergeable_tx_id(),
        DurabilityTier::Edge,
        move |result| *callback_result.borrow_mut() = Some(result),
    );
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .unwrap();

    client.tick().unwrap();
    assert_eq!(
        wait_result.borrow_mut().take().unwrap().unwrap_err().code,
        ErrorCode::WriteRejected
    );
    client.tick().unwrap();

    assert!(events.borrow().is_empty());
    assert!(
        client
            .node
            .node()
            .borrow()
            .rejected_transaction(write.mergeable_tx_id())
            .is_none()
    );
}

/// An explicit wait that begins after the rejection was queued still consumes
/// it before the next-tick fallback callback can deliver it.
#[test]
fn wait_after_rejection_suppresses_queued_mutation_error() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc4; 16]);
    let client = open_db(0xc4, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    client.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));

    let write = client
        .insert(
            "todos",
            cells("late wait rejection", false, author),
            Default::default(),
        )
        .unwrap();
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .unwrap();
    client.tick().unwrap();

    let error =
        block_on(client.wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Edge))
            .unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(error.message.contains("AuthorizationDenied"));
    assert!(
        error
            .message
            .contains(&format!("{:?}", write.mergeable_tx_id()))
    );
    client.tick().unwrap();

    assert!(events.borrow().is_empty());
    assert!(
        client
            .node
            .node()
            .borrow()
            .rejected_transaction(write.mergeable_tx_id())
            .is_none()
    );
}

/// A rejected transaction that was not delivered before shutdown is recovered
/// from durable storage and delivered after the reopened client registers its
/// callback.
#[test]
fn undelivered_mutation_error_is_recovered_after_reopen() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc3; 16]);
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xc3; 16]),
        author,
    };
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xc3))),
    }))
    .unwrap();
    let (client_transport, mut authority_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let write = client
        .insert(
            "todos",
            cells("rejected before reopen", false, author),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .unwrap();
    client.tick().unwrap();

    drop(write);
    drop(upstream);
    drop(authority_transport);
    drop(client);

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xc3))),
    }))
    .unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    reopened.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));
    reopened.tick().unwrap();

    let events = events.borrow();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].transaction.transaction_id,
        TransactionId::from_committed_tx(tx_id)
    );
    drop(events);
    drop(reopened);

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let acknowledged_reopen = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xc3))),
    }))
    .unwrap();
    let replayed_events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&replayed_events);
    acknowledged_reopen.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));
    acknowledged_reopen.tick().unwrap();
    assert!(replayed_events.borrow().is_empty());
}

/// Close drains observers while durable storage is still live. A waiter which
/// claims a rejection during that drain must acknowledge it immediately rather
/// than leaving it to be replayed as an unhandled error after reopen.
#[test]
fn close_acknowledges_rejection_claimed_by_drained_waiter() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc4; 16]);
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xc4; 16]),
        author,
    };
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage: RocksDbStorage::open(dir.path(), &refs).expect("open durable client storage"),
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xc4))),
    }))
    .expect("open durable client");
    let (client_transport, mut authority_transport) = duplex();
    let upstream = block_on(client.connect_upstream(client_transport));
    let write = client
        .insert(
            "todos",
            cells("close-drained rejection", false, author),
            Default::default(),
        )
        .expect("create pending write");
    let tx_id = write.mergeable_tx_id();
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .expect("authority fate reaches durable client");
    client
        .tick()
        .expect("client persists the authority rejection");

    let drained_outcome = Rc::new(RefCell::new(None));
    let callback_outcome = Rc::clone(&drained_outcome);
    client.wait_for_transaction_with(tx_id, DurabilityTier::Edge, move |outcome| {
        *callback_outcome.borrow_mut() = Some(outcome);
    });
    drop(write);
    drop(upstream);
    drop(authority_transport);
    block_on(client.close()).expect("close drains and acknowledges the waiter");
    assert_eq!(
        drained_outcome
            .borrow_mut()
            .take()
            .expect("close drained the waiting observer")
            .expect_err("the drained waiter sees the stored rejection")
            .code,
        ErrorCode::WriteRejected
    );
    drop(client);

    let reopened = block_on(Db::open(DbConfig {
        schema,
        storage: RocksDbStorage::open(dir.path(), &refs).expect("reopen durable client storage"),
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xc4))),
    }))
    .expect("reopen durable client");
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    reopened.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));
    reopened
        .tick()
        .expect("reopened client services pending errors");
    assert!(
        events.borrow().is_empty(),
        "the close-drained rejection was acknowledged exactly once before storage closed"
    );
}

#[test]
fn write_fate_and_durability_are_queryable_through_facade() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert(
            "todos",
            cells("facade state", false, author),
            Default::default(),
        )
        .unwrap();
    assert_eq!(
        write.write_state().unwrap(),
        WriteState {
            fate: Fate::Pending,
            global_time: None,
            durability: DurabilityTier::Local,
        }
    );
    assert_eq!(
        client.write_state(write.mergeable_tx_id()).unwrap(),
        write.write_state().unwrap()
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let state = write.write_state().unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert!(
        state.global_time.is_some(),
        "the authority acceptance carries its assigned Global timestamp"
    );
    assert_eq!(state.durability, DurabilityTier::Global);
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
}

#[test]
fn session_upload_rejects_forged_made_by_without_ingesting_rows() {
    let schema = owner_write_schema();
    let session_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let forged_author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xf1), client.next_now_ms())
                .made_by(forged_author)
                .cells(cells("forged", false, session_author)),
        )
        .unwrap();
    client
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let handle = WriteHandle {
        node: Rc::downgrade(&client.node.node),
        row_uuid: row(0xf1),
        tx_id,
        local_tier: DurabilityTier::Local,
        queued_status: None,
        queued_alias: None,
    };
    let err = block_on(handle.wait(DurabilityTier::Global)).unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn session_upload_strips_forged_system_permission_before_storage_and_publication() {
    let schema = schema();
    let session_author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let edge_node = NodeUuid::from_bytes([0xe2; 16]);
    let edge = open_core(0xe2, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc2, session_author, &schema);

    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        session_author,
        NodeUuid::from_bytes([0xc2; 16]),
        1,
        edge_node,
        2,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            session_author,
            BTreeMap::new(),
            CommitUnitTrust::Session,
        );

    let write = client
        .insert(
            "todos",
            cells("forged permission subject", false, session_author),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    let mut forged = client
        .node
        .node
        .borrow_mut()
        .commit_unit_for(tx_id)
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = &mut forged else {
        unreachable!("commit_unit_for returns a CommitUnit");
    };
    tx.permission_subject = Some(AuthorSubject::SYSTEM);
    assert!(queue_pending_upload_in(
        &client.node.outbox,
        tx_id,
        Some(forged),
    ));

    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();

    let SyncMessage::CommitUnit { tx, .. } =
        edge.node().borrow_mut().commit_unit_for(tx_id).unwrap()
    else {
        unreachable!("edge retained the accepted transaction");
    };
    assert_eq!(
        tx.permission_subject, None,
        "storage drops untrusted SYSTEM"
    );

    crate::db::block_on(edge.node().borrow_mut().apply_fate_update(
        tx_id,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    ))
    .unwrap();
    assert!(matches!(
        crate::db::block_on(edge.node().borrow_mut().transaction_state(tx_id)),
        Some((Fate::Accepted, None, DurabilityTier::Edge))
    ));
    let publication = edge
        .node()
        .borrow_mut()
        .edge_authority_publication_for(tx_id)
        .unwrap();
    let published = publication
        .commits
        .iter()
        .find(|unit| unit.tx.tx_id == tx_id)
        .expect("publication contains its anchor transaction");
    assert_eq!(
        published.tx.permission_subject, None,
        "publication cannot re-emit a session-forged capability"
    );
}

#[test]
fn session_upload_uses_connection_identity_for_write_policy() {
    let schema = owner_write_schema();
    let session_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert(
            "todos",
            cells("honest", false, session_author),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);
}

// This sync-boundary test is intentionally lower-level: the public policy
// test app reaches this same prepared server write-policy path, but cannot
// distinguish a malformed prepared claim binding from an ordinary denial.
#[test]
fn admitted_server_prepared_write_policy_binds_text_user_id_claim() {
    let schema = owner_id_session_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let alice_client = open_db(0xa1, alice, &schema);
    let bob_client = open_db(0xb2, bob, &schema);
    let alice_claims = BTreeMap::from([(
        crate::query::provider_claim_key("sub"),
        Value::String("alice-subject".to_owned()),
    )]);
    alice_client.set_test_provider_claims(alice, alice_claims.clone());
    let bob_claims = BTreeMap::from([(
        crate::query::provider_claim_key("sub"),
        Value::String("bob-subject".to_owned()),
    )]);
    bob_client.set_test_provider_claims(bob, bob_claims.clone());

    let (alice_transport, alice_server_transport) = duplex();
    let _alice_upstream = crate::db::block_on(alice_client.connect_upstream(alice_transport));
    let _alice_subscriber =
        server.accept_subscriber_with_claims(alice_server_transport, alice, alice_claims);
    let (bob_transport, bob_server_transport) = duplex();
    let _bob_upstream = crate::db::block_on(bob_client.connect_upstream(bob_transport));
    let _bob_subscriber =
        server.accept_subscriber_with_claims(bob_server_transport, bob, bob_claims);

    let accepted = alice_client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("owned by alice".to_owned()),
                ),
                (
                    "owner_id".to_owned(),
                    Value::String("alice-subject".to_owned()),
                ),
            ]),
            Default::default(),
        )
        .unwrap();
    alice_client.tick().unwrap();
    server.tick().unwrap();
    alice_client.tick().unwrap();
    assert_eq!(
        block_on(accepted.wait(DurabilityTier::Global)).unwrap(),
        accepted.mergeable_tx_id(),
        "the admitted server must bind public session.user as Text in its prepared write-policy plan"
    );

    let denied = bob_client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("spoofed by bob".to_owned()),
                ),
                (
                    "owner_id".to_owned(),
                    Value::String("alice-subject".to_owned()),
                ),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(0xb2)),
                identity: crate::db::WriteIdentity::Session(bob),
                ..Default::default()
            },
        )
        .unwrap();
    assert_authority_rejects_staged_write(&bob_client, &server, &denied);
    let rows = server.read(&Query::from("messages")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), accepted.row_uuid());
}

#[test]
fn admitted_server_prepared_write_policy_coerces_string_user_id_to_uuid_column() {
    let schema = owner_uuid_session_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xa3; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb3; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let alice_client = open_db(0xa3, alice, &schema);
    let bob_client = open_db(0xb3, bob, &schema);
    let alice_claims = BTreeMap::from([(
        crate::query::provider_claim_key("sub"),
        Value::String(alice.test_uuid().to_string()),
    )]);
    let bob_claims = BTreeMap::from([(
        crate::query::provider_claim_key("sub"),
        Value::String(bob.test_uuid().to_string()),
    )]);
    alice_client.set_test_provider_claims(alice, alice_claims.clone());
    bob_client.set_test_provider_claims(bob, bob_claims.clone());

    let (alice_transport, alice_server_transport) = duplex();
    let _alice_upstream = crate::db::block_on(alice_client.connect_upstream(alice_transport));
    let _alice_subscriber =
        server.accept_subscriber_with_claims(alice_server_transport, alice, alice_claims);
    let (bob_transport, bob_server_transport) = duplex();
    let _bob_upstream = crate::db::block_on(bob_client.connect_upstream(bob_transport));
    let _bob_subscriber =
        server.accept_subscriber_with_claims(bob_server_transport, bob, bob_claims);

    let accepted = alice_client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("owned by alice".to_owned()),
                ),
                ("owner_id".to_owned(), Value::Uuid(alice.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();
    alice_client.tick().unwrap();
    server.tick().unwrap();
    alice_client.tick().unwrap();
    assert_eq!(
        block_on(accepted.wait(DurabilityTier::Global)).unwrap(),
        accepted.mergeable_tx_id(),
        "the prepared descriptor must preserve UUID policy columns while coercing public user_id text"
    );

    let denied = bob_client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("spoofed by bob".to_owned()),
                ),
                ("owner_id".to_owned(), Value::Uuid(alice.test_uuid())),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(0xb3)),
                identity: crate::db::WriteIdentity::Session(bob),
                ..Default::default()
            },
        )
        .unwrap();
    assert_authority_rejects_staged_write(&bob_client, &server, &denied);
    let rows = server.read(&Query::from("messages")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), accepted.row_uuid());
}

#[test]
fn admitted_server_prepared_write_policy_fails_closed_for_wrong_user_id_type() {
    let schema = owner_id_session_write_schema();
    let author = AuthorSubject::for_test_bytes([0xa4; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa4, author, &schema);
    let claims = BTreeMap::from([(crate::query::provider_claim_key("sub"), Value::Bool(true))]);
    client.set_test_provider_claims(author, claims.clone());

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(server_transport, author, claims);
    let write = client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("must not ingest".to_owned()),
                ),
                ("owner_id".to_owned(), Value::String("true".to_owned())),
            ]),
            Default::default(),
        )
        .unwrap();

    client.tick().unwrap();
    let error = server.tick().unwrap_err();
    assert!(
        error.to_string().contains("claims3:sub has wrong type"),
        "a non-coercible claim must fail before authorization support can admit the write: {error}"
    );
    assert!(
        server.read(&Query::from("messages")).unwrap().is_empty(),
        "a malformed session claim must never ingest a protected row"
    );
    drop(write);
}

#[test]
fn session_delete_uses_current_row_for_owner_write_policy() {
    let schema = owner_write_schema();
    let session_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let other_author = AuthorSubject::for_test_bytes([0xd1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert(
            "todos",
            cells("owned", false, session_author),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();

    let bad_delete = client
        .delete(
            "todos",
            row,
            crate::db::DeleteOptions {
                identity: crate::db::WriteIdentity::Session(other_author),
                ..Default::default()
            },
        )
        .unwrap();
    assert_authority_rejects_staged_write(&client, &server, &bad_delete);
    let client_rows = prepared_read(&client, &Query::from("todos"));
    assert_eq!(client_rows.len(), 1);
    assert_eq!(client_rows[0].row_uuid(), row);
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);

    let delete = client
        .delete(
            "todos",
            row,
            crate::db::DeleteOptions {
                identity: crate::db::WriteIdentity::Session(session_author),
                ..Default::default()
            },
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(delete.wait(DurabilityTier::Global)).unwrap(),
        delete.mergeable_tx_id()
    );
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn trusted_backend_upload_uses_backend_policy_and_stores_user_made_by() {
    let schema = owner_write_schema();
    let backend_author = AuthorSubject::for_test_bytes([0xb0; 16]);
    // Provenance records a different admitted account author while policy
    // evaluation uses the trusted backend. Attribution must not replace the
    // effective permission subject or bypass the required account author.
    let attributed_user = AuthorSubject::for_test_bytes([0xb1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(backend.connect_upstream(backend_transport));
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );
    backend.tick().unwrap();
    server.tick().unwrap();

    let tx_id = backend
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xf2), backend.next_now_ms())
                .made_by(attributed_user)
                .permission_subject(backend_author)
                .cells(cells("attributed", false, backend_author)),
        )
        .unwrap();
    backend
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    let SyncMessage::CommitUnit { tx, .. } =
        server.node().borrow_mut().commit_unit_for(tx_id).unwrap()
    else {
        panic!("expected stored commit unit");
    };
    assert_eq!(tx.made_by, attributed_user);
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0xf2));
}

#[test]
fn trusted_backend_upload_applies_session_claim_assertions_for_write_policy() {
    let schema = editor_claim_write_schema();
    let backend_author = AuthorSubject::for_test_bytes([0xb0; 16]);
    let editor_author = AuthorSubject::for_test_bytes([0xe1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream =
        crate::db::block_on(backend.connect_upstream(Box::new(TrustedBackendTransport {
            inner: backend_transport,
        })));
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    backend.set_test_provider_claims(
        editor_author,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    let write = backend
        .insert(
            "todos",
            cells("claim-backed", false, editor_author),
            crate::db::InsertOptions {
                row_id: Some(row(0xe1)),
                identity: crate::db::WriteIdentity::Session(editor_author),
                ..Default::default()
            },
        )
        .unwrap();

    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0xe1));
}

#[test]
fn session_claim_assertions_require_trusted_backend_upload() {
    let schema = editor_claim_write_schema();
    let session_author = AuthorSubject::for_test_bytes([0xe1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xe1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    client.set_test_provider_claims(
        session_author,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    let write = client
        .insert(
            "todos",
            cells("claim-backed", false, session_author),
            crate::db::InsertOptions {
                row_id: Some(row(0xe2)),
                identity: crate::db::WriteIdentity::Session(session_author),
                ..Default::default()
            },
        )
        .unwrap();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let err = block_on(write.wait(DurabilityTier::Global)).unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn trusted_backend_delete_uses_permission_subject_parent_for_write_policy() {
    let schema = owner_write_schema();
    let backend_author = AuthorSubject::for_test_bytes([0xb0; 16]);
    let attributed_user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream =
        crate::db::block_on(backend.connect_upstream(Box::new(TrustedBackendTransport {
            inner: backend_transport,
        })));
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );
    // The trusted backend may attribute a mutation to this admitted provider
    // session, but its UUID owner policy still reads the raw provider claim.
    backend.set_test_provider_claims(attributed_user, test_provider_claims(attributed_user));
    backend.tick().unwrap();
    server.tick().unwrap();

    let insert = backend
        .insert(
            "todos",
            cells("attributed", false, attributed_user),
            crate::db::InsertOptions {
                row_id: Some(row(0xf3)),
                identity: crate::db::WriteIdentity::Session(attributed_user),
                ..Default::default()
            },
        )
        .unwrap();
    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();
    block_on(insert.wait(DurabilityTier::Global)).unwrap();

    let delete = backend
        .delete(
            "todos",
            row(0xf3),
            crate::db::DeleteOptions {
                identity: crate::db::WriteIdentity::Session(attributed_user),
                ..Default::default()
            },
        )
        .unwrap();
    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    assert_eq!(
        block_on(delete.wait(DurabilityTier::Global)).unwrap(),
        delete.mergeable_tx_id()
    );
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn client_insert_advice_is_unknown_without_writing() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let other = AuthorSubject::for_test_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    owner_db.set_test_provider_claims(owner, test_provider_claims(owner));
    other_db.set_test_provider_claims(other, test_provider_claims(other));

    assert_eq!(
        owner_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap(),
        PermissionAdvice::Unknown,
    );
    assert_eq!(
        other_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap(),
        PermissionAdvice::Unknown,
    );
    assert_eq!(
        owner_db
            .authorize_insert_for_identity("todos", cells("owned", false, owner), owner)
            .unwrap(),
        PermissionAdvice::Allowed,
    );
    assert_eq!(
        owner_db
            .authorize_insert_for_identity("todos", cells("owned", false, owner), other)
            .unwrap(),
        PermissionAdvice::Denied,
    );
    assert_eq!(prepared_read(&owner_db, &owner_db.table("todos")).len(), 0);
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 0);
}

#[test]
fn client_delete_advice_is_unknown_without_mutating() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let other = AuthorSubject::for_test_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    owner_db.set_test_provider_claims(owner, test_provider_claims(owner));
    other_db.set_test_provider_claims(other, test_provider_claims(other));
    let row = row(1);
    let write = owner_db
        .insert(
            "todos",
            cells("owned", false, owner),
            crate::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();
    other_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(
            owner_db
                .node
                .node
                .borrow_mut()
                .commit_unit_for(write.mergeable_tx_id())
                .unwrap(),
        )
        .unwrap();

    assert_eq!(
        owner_db.can_delete("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        other_db.can_delete("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        owner_db
            .authorize_delete_for_identity("todos", row, owner)
            .unwrap(),
        PermissionAdvice::Allowed,
    );
    assert_eq!(
        owner_db
            .authorize_delete_for_identity("todos", row, other)
            .unwrap(),
        PermissionAdvice::Denied,
    );
    assert_eq!(prepared_read(&owner_db, &owner_db.table("todos")).len(), 1);
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 0);
}

#[test]
fn core_attributed_insert_uses_core_identity_for_policy_and_user_for_made_by() {
    let schema = owner_write_schema();
    let backend = AuthorSubject::for_test_bytes([0xbe; 16]);
    let attributed_user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let core = open_core(0x5e, backend, &schema);
    let write = core
        .insert_attributed(
            attributed_user,
            "todos",
            cells("attributed", false, backend),
        )
        .unwrap();

    let unit = core
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("commit unit expected");
    };

    assert_eq!(tx.made_by, attributed_user);
    assert_eq!(core.read(&core.table("todos")).unwrap().len(), 1);
}

#[test]
fn client_attributed_insert_to_different_user_is_rejected() {
    let schema = owner_write_schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let attributed_user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let err = match client
        .insert(
            "todos",
            cells("forged", false, client_author),
            crate::db::InsertOptions {
                identity: crate::db::WriteIdentity::Attribution(attributed_user),
                ..Default::default()
            },
        )
        .resolve()
    {
        Ok(_) => panic!("client attribution should be rejected"),
        Err(err) => err,
    };

    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert_eq!(prepared_read(&client, &client.table("todos")).len(), 0);
}

/// A trusted backend admits a write as its own policy subject while recording
/// the external author as durable provenance; an ordinary Db cannot mint that
/// split. Alice's row is deliberately owned by the backend, not Alice, so the
/// positive path proves that authorization did not accidentally follow
/// provenance.
///
/// ```text
/// trusted backend ──admit as backend──► owner policy ──allow──► row made_by=alice
/// ordinary client ──claim made_by=alice──────────────────────► rejected locally
/// ```
#[test]
fn backend_attribution_separates_owner_policy_from_external_provenance() {
    let schema = owner_write_schema();
    let backend_author = AuthorSubject::for_test_bytes([0xb0; 16]);
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let trusted_backend = block_on(unsafe {
        // SAFETY: this integration fixture represents the private capability
        // minted only by an explicitly opened, authenticated backend runtime.
        Db::open_with_backend_attribution(DbConfig {
            schema: schema.clone(),
            storage: rocks_storage(&schema),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0xb0; 16]),
                author: backend_author,
            },
            id_source: Some(Box::new(SeededRowIdSource::new(0xb0))),
        })
    })
    .unwrap();

    let write = trusted_backend
        .insert(
            "todos",
            cells(
                "admitted by backend, credited to alice",
                false,
                backend_author,
            ),
            crate::db::InsertOptions {
                row_id: Some(row(0xb1)),
                identity: crate::db::WriteIdentity::Attribution(alice),
                ..Default::default()
            },
        )
        .unwrap();
    let unit = trusted_backend
        .node
        .node
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("backend-attributed write must publish a commit unit");
    };
    assert_eq!(
        tx.made_by, alice,
        "external canonical subject is provenance"
    );
    assert_eq!(
        prepared_read(&trusted_backend, &trusted_backend.table("todos")).len(),
        1,
        "the backend-owned policy row proves admission used backend_author rather than alice"
    );

    let ordinary_runtime = open_db(0xb2, backend_author, &schema);
    let err = match ordinary_runtime
        .insert(
            "todos",
            cells("forged external provenance", false, backend_author),
            crate::db::InsertOptions {
                row_id: Some(row(0xb2)),
                identity: crate::db::WriteIdentity::Attribution(alice),
                ..Default::default()
            },
        )
        .resolve()
    {
        Ok(_) => panic!("an ordinary Db must not gain backend attribution from its author value"),
        Err(err) => err,
    };
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(prepared_read(&ordinary_runtime, &ordinary_runtime.table("todos")).is_empty());
}

/// Backend-attributed transactions and resumable large-value uploads retain
/// one external provenance subject for their final commit while each admission
/// check remains the trusted backend's. Alice is never granted ownership of the
/// backend-owned rows in this fixture.
///
/// ```text
/// backend ──open attributed batch / finish streamed upload──► policy as backend
///                                                       └──► committed made_by=alice
/// ```
#[test]
fn backend_attribution_survives_transactions_and_streaming_publication() {
    let schema = owner_write_schema();
    let backend_author = AuthorSubject::for_test_bytes([0xb6; 16]);
    let alice = AuthorSubject::for_test_bytes([0xa6; 16]);
    let backend = block_on(unsafe {
        // SAFETY: this fixture stands in for the binding's explicit backend
        // constructor, the only production issuer of this capability.
        Db::open_with_backend_attribution(DbConfig {
            schema: schema.clone(),
            storage: rocks_storage(&schema),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0xb6; 16]),
                author: backend_author,
            },
            id_source: Some(Box::new(SeededRowIdSource::new(0xb6))),
        })
    })
    .unwrap();

    let batch = OpenTransactionId::new();
    block_on(
        backend.begin_mergeable_with_identity(batch, crate::db::WriteIdentity::Attribution(alice)),
    )
    .unwrap();
    block_on(backend.mergeable_tx_ref(batch).insert(
        "todos",
        cells("mergeable provenance", false, backend_author),
        crate::db::InsertOptions {
            row_id: Some(row(0xb7)),
            ..Default::default()
        },
    ))
    .unwrap();
    let batch_tx = block_on(backend.commit_mergeable_handle(batch)).unwrap();
    let SyncMessage::CommitUnit { tx, .. } = backend
        .node
        .node
        .borrow_mut()
        .commit_unit_for(batch_tx)
        .unwrap()
    else {
        panic!("mergeable batch must publish a commit unit");
    };
    assert_eq!(tx.made_by, alice);
    assert_eq!(tx.permission_subject, Some(backend_author));

    let exclusive = OpenTransactionId::new();
    block_on(
        backend
            .begin_exclusive_with_identity(exclusive, crate::db::WriteIdentity::Attribution(alice)),
    )
    .unwrap();
    block_on(backend.exclusive_tx_ref(exclusive).insert(
        "todos",
        cells("exclusive provenance", false, backend_author),
        crate::db::InsertOptions {
            row_id: Some(row(0xba)),
            ..Default::default()
        },
    ))
    .unwrap();
    let exclusive_tx = block_on(backend.commit_exclusive_handle(exclusive)).unwrap();
    let SyncMessage::CommitUnit { tx, .. } = backend
        .node
        .node
        .borrow_mut()
        .commit_unit_for(exclusive_tx)
        .unwrap()
    else {
        panic!("exclusive transaction must publish a commit unit");
    };
    assert_eq!(tx.made_by, alice);
    assert_eq!(tx.permission_subject, Some(backend_author));

    // An empty attributed patch only checks whether the trusted backend may
    // observe the row. It must not reinterpret external provenance as the
    // admission identity and hide this backend-owned row from itself.
    let no_op = block_on(backend.update(
        "todos",
        row(0xb7),
        BTreeMap::new(),
        crate::db::UpdateOptions {
            identity: crate::db::WriteIdentity::Attribution(alice),
            ..Default::default()
        },
    ))
    .expect("an attributed no-op update uses backend admission for visibility");
    assert_eq!(no_op.mergeable_tx_id(), batch_tx);

    let streaming_cells = BTreeMap::from([
        ("done".to_owned(), Value::Bool(false)),
        ("owner".to_owned(), Value::Uuid(backend_author.test_uuid())),
    ]);
    let mut upload = backend
        .begin_streaming_value_upload("todos", &streaming_cells, "title")
        .unwrap();
    block_on(backend.push_streaming_value_upload(&mut upload, b"streamed provenance")).unwrap();
    let streamed = block_on(backend.finish_streaming_value_upload(
        upload,
        crate::db::StreamingMutationKind::Insert,
        "todos",
        row(0xb8),
        streaming_cells,
        "title",
        crate::db::WriteIdentity::Attribution(alice),
        None,
        None,
        None,
    ))
    .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = backend
        .node
        .node
        .borrow_mut()
        .commit_unit_for(streamed.mergeable_tx_id())
        .unwrap()
    else {
        panic!("streamed attributed write must publish a commit unit");
    };
    assert_eq!(tx.made_by, alice);
    assert_eq!(prepared_read(&backend, &backend.table("todos")).len(), 3);

    let branch = BranchSelector::new([("draft", Value::String("alice".to_owned()))]);
    let attributed = crate::db::WriteIdentity::Attribution(alice);
    for result in [
        block_on(backend.insert(
            "todos",
            BTreeMap::new(),
            crate::db::InsertOptions {
                identity: attributed,
                target: crate::db::ExactWriteTarget::Branch(branch.clone()),
                ..Default::default()
            },
        )),
        block_on(backend.upsert(
            "todos",
            row(0xb9),
            BTreeMap::new(),
            crate::db::UpsertOptions {
                identity: attributed,
                target: crate::db::WriteTarget::BranchView {
                    head: branch.clone(),
                    base: None,
                },
                ..Default::default()
            },
        )),
        block_on(backend.restore(
            "todos",
            row(0xb9),
            None,
            crate::db::RestoreOptions {
                identity: attributed,
                target: crate::db::ExactWriteTarget::Branch(branch.clone()),
                ..Default::default()
            },
        )),
    ] {
        let err = match result {
            Ok(_) => panic!("generic attributed branch writes must fail before lookup"),
            Err(err) => err,
        };
        assert_eq!(err.code, ErrorCode::WriteRejected);
    }
    for result in [
        block_on(backend.update(
            "todos",
            row(0xb9),
            BTreeMap::new(),
            crate::db::UpdateOptions {
                identity: attributed,
                target: crate::db::WriteTarget::BranchView {
                    head: branch.clone(),
                    base: None,
                },
                ..Default::default()
            },
        )),
        block_on(backend.delete(
            "todos",
            row(0xb9),
            crate::db::DeleteOptions {
                identity: attributed,
                target: crate::db::WriteTarget::BranchView {
                    head: branch.clone(),
                    base: None,
                },
                ..Default::default()
            },
        )),
    ] {
        let err = match result {
            Ok(_) => panic!("generic attributed branch views must fail before lookup"),
            Err(err) => err,
        };
        assert_eq!(err.code, ErrorCode::WriteRejected);
    }

    let transaction = OpenTransactionId::new();
    block_on(
        backend.begin_mergeable_with_identity(
            transaction,
            crate::db::WriteIdentity::Attribution(alice),
        ),
    )
    .unwrap();
    let err = match block_on(backend.mergeable_tx_ref(transaction).insert(
        "todos",
        BTreeMap::new(),
        crate::db::InsertOptions {
            target: crate::db::ExactWriteTarget::Branch(branch.clone()),
            ..Default::default()
        },
    )) {
        Ok(_) => panic!("attributed batches must reject branch staging before defaults or lookup"),
        Err(err) => err,
    };
    assert_eq!(err.code, ErrorCode::WriteRejected);
    backend.abandon_transaction_handle(transaction).unwrap();

    let upload_cells = BTreeMap::from([
        ("done".to_owned(), Value::Bool(false)),
        ("owner".to_owned(), Value::Uuid(backend_author.test_uuid())),
    ]);
    let upload = backend
        .begin_streaming_value_upload("todos", &upload_cells, "title")
        .unwrap();
    let err = match block_on(backend.finish_streaming_value_upload(
        upload,
        crate::db::StreamingMutationKind::Insert,
        "todos",
        row(0xba),
        upload_cells,
        "title",
        crate::db::WriteIdentity::Attribution(alice),
        None,
        Some(branch),
        None,
    )) {
        Ok(_) => panic!("attributed streaming branch targets must fail before final staging"),
        Err(err) => err,
    };
    assert_eq!(err.code, ErrorCode::WriteRejected);

    assert_eq!(prepared_read(&backend, &backend.table("todos")).len(), 3);
}

/// A trusted backend admits a streamed value as `SYSTEM` while retaining the
/// externally supplied canonical subject in the resulting commit provenance.
///
/// ```text
/// SYSTEM (editor claim) ──authorizes upload──► commit.made_by = alice
/// alice (no editor claim) ──cannot authorize the equivalent direct write
/// ```
#[test]
fn attributed_streaming_uses_system_admission_without_losing_provenance() {
    let schema = editor_claim_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xab; 16]);
    let server = open_core(0xac, AuthorSubject::SYSTEM, &schema);
    server.node().borrow_mut().set_test_provider_claims(
        AuthorSubject::SYSTEM,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    server
        .node()
        .borrow_mut()
        .set_test_provider_claims(alice, BTreeMap::new());
    let backend = block_on(unsafe {
        // SAFETY: this is the explicit trusted-backend constructor exercised by
        // the native binding, with SYSTEM as its admission identity.
        Db::open_with_backend_attribution(DbConfig {
            schema: schema.clone(),
            storage: rocks_storage(&schema),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0xab; 16]),
                author: AuthorSubject::SYSTEM,
            },
            id_source: Some(Box::new(SeededRowIdSource::new(0xab))),
        })
    })
    .unwrap();
    let (backend_transport, server_transport) = duplex();
    let _upstream = block_on(backend.connect_upstream(backend_transport));
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );

    let direct = block_on(backend.insert(
        "todos",
        cells("alice cannot admit", false, alice),
        crate::db::InsertOptions {
            row_id: Some(row(0xab)),
            identity: crate::db::WriteIdentity::Session(alice),
            ..Default::default()
        },
    ))
    .expect("the client may stage a write before the serving backend evaluates it");
    for _ in 0..4 {
        backend.tick().unwrap();
        server.tick().unwrap();
    }
    let direct_err = block_on(direct.wait(DurabilityTier::Global))
        .expect_err("planted negative: Alice must not inherit SYSTEM's editor claim");
    assert_eq!(direct_err.code, ErrorCode::WriteRejected);

    let cells = BTreeMap::from([
        ("done".to_owned(), Value::Bool(false)),
        ("owner".to_owned(), Value::Uuid(alice.test_uuid())),
    ]);
    let mut upload = backend
        .begin_streaming_value_upload("todos", &cells, "title")
        .unwrap();
    block_on(backend.push_streaming_value_upload(&mut upload, b"SYSTEM-admitted")).unwrap();
    let write = block_on(backend.finish_streaming_value_upload(
        upload,
        crate::db::StreamingMutationKind::Insert,
        "todos",
        row(0xac),
        cells,
        "title",
        crate::db::WriteIdentity::Attribution(alice),
        None,
        None,
        None,
    ))
    .expect("SYSTEM must authorize the attributed streaming write");
    for _ in 0..8 {
        backend.tick().unwrap();
        server.tick().unwrap();
    }
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let SyncMessage::CommitUnit { tx, .. } = backend
        .node
        .node
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap()
    else {
        panic!("streamed attributed write must publish a commit unit");
    };
    assert_eq!(tx.made_by, alice);
}

#[test]
fn default_insert_keeps_subject_and_made_by_equal() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0xa1, owner, &schema);
    let write = db
        .insert("todos", cells("default", false, owner), Default::default())
        .unwrap();
    let unit = db
        .node
        .node
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("commit unit expected");
    };

    assert_eq!(tx.made_by, owner);
    assert_eq!(prepared_read(&db, &db.table("todos")).len(), 1);
}

fn queued_local_publication(db: &Db<RocksDbStorage>, expects_upload_unit: bool) -> TxId {
    let publications = db.node.pending_local_publications.borrow();
    assert_eq!(
        publications.len(),
        1,
        "publication must cross into node ownership before refresh returns"
    );
    let publication = publications.front().unwrap();
    assert_eq!(publication.upload_unit.is_some(), expects_upload_unit);
    publication.published.tx_id()
}

fn cancel_during_subscription_refresh<F, T>(
    db: &Db<RocksDbStorage>,
    future: F,
    expects_upload_unit: bool,
) -> TxId
where
    F: Future<Output = Result<T, Error>>,
{
    db.stall_next_subscription_refresh.set(true);
    let mut future = Box::pin(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);

    for _ in 0..128 {
        match future.as_mut().poll(&mut context) {
            Poll::Pending => {}
            Poll::Ready(Ok(_)) => {
                panic!("commit completed instead of stalling in subscription refresh")
            }
            Poll::Ready(Err(error)) => {
                panic!("commit failed before stalled subscription refresh: {error}")
            }
        }
        if !db.node.pending_local_publications.borrow().is_empty() {
            let tx_id = queued_local_publication(db, expects_upload_unit);
            drop(future);
            return tx_id;
        }
    }
    panic!("publication never transferred to the node-owned persistence queue")
}

fn settle_cancelled_local_publication(db: &Db<RocksDbStorage>, tx_id: TxId) {
    block_on(db.tick()).unwrap();
    assert!(db.node.pending_local_publications.borrow().is_empty());
    assert_eq!(
        db.write_state(tx_id).unwrap(),
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
            global_time: None,
        },
        "a cancelled caller must not abandon node-owned persistence"
    );
}
fn assert_internal_subscription_refresh_failure(subscription: &mut SubscriptionStream) {
    let event = block_on(subscription.next_raw()).expect("refresh failure event");
    assert_eq!(
        event,
        SubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::ServerFailure {
                code: SubscribeServerFailureCode::Internal,
            },
        }
    );
}

/// A deferred local writer transfers its publication to the node queue before
/// its affected subscription refresh fails; a sibling stream still receives
/// its delta, and the later runtime tick persists the queued write.
#[test]
fn deferred_write_refresh_error_returns_committed_handle_and_keeps_persistence_owned() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa2; 16]);
    let db = open_db(0xa2, owner, &schema);
    db.set_deferred_local_persistence(true);
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let _opening = block_on(subscription.next_raw()).unwrap();

    db.fail_next_subscription_refresh.set(true);
    let write = block_on(db.insert(
        "todos",
        cells("persist me", false, owner),
        Default::default(),
    ))
    .expect("publication-owned write must return its handle");

    let published = queued_local_publication(&db, false);
    assert_eq!(published, write.mergeable_tx_id());
    assert_internal_subscription_refresh_failure(&mut subscription);
    settle_cancelled_local_publication(&db, published);
    assert_eq!(
        block_on(write.wait(DurabilityTier::Local)).unwrap(),
        published
    );
}

/// A synchronous local writer persists before refreshing subscriptions: the
/// affected stream becomes terminal with its original error, while its sibling
/// keeps receiving later deltas without turning either write into a failure.
#[test]
fn persisted_write_refresh_error_returns_committed_handle() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa6; 16]);
    let db = open_db(0xa6, owner, &schema);
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let _opening = block_on(subscription.next_raw()).unwrap();

    db.fail_next_subscription_refresh.set(true);
    let write = block_on(db.insert(
        "todos",
        cells("already persisted", false, owner),
        Default::default(),
    ))
    .expect("persisted write must not report observer refresh as commit failure");

    assert!(db.node.pending_local_publications.borrow().is_empty());
    assert_eq!(
        db.write_state(write.mergeable_tx_id()).unwrap(),
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
            global_time: None,
        }
    );
    assert_internal_subscription_refresh_failure(&mut subscription);
}

/// One deferred local writer publishes two transactions; the node runtime must
/// persist and enqueue both exactly once, in authoring order, before a later
/// idle tick can revisit the outbox.
#[test]
fn deferred_local_publications_persist_and_enqueue_in_publication_order_once() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa7; 16]);
    let db = open_db(0xa7, owner, &schema);
    db.set_deferred_local_persistence(true);

    let first = block_on(db.insert(
        "todos",
        cells("first ordered publication", false, owner),
        Default::default(),
    ))
    .unwrap();
    let second = block_on(db.insert(
        "todos",
        cells("second ordered publication", false, owner),
        Default::default(),
    ))
    .unwrap();
    let tx_ids = [first.mergeable_tx_id(), second.mergeable_tx_id()];
    assert_eq!(
        db.node
            .pending_local_publications
            .borrow()
            .iter()
            .map(|publication| publication.published.tx_id())
            .collect::<Vec<_>>(),
        tx_ids
    );

    block_on(db.tick()).unwrap();
    assert!(db.node.pending_local_publications.borrow().is_empty());
    assert_eq!(
        db.node
            .outbox
            .borrow()
            .iter()
            .map(|upload| upload.tx_id)
            .collect::<Vec<_>>(),
        tx_ids
    );
    for tx_id in tx_ids {
        assert_eq!(
            db.write_state(tx_id).unwrap(),
            WriteState {
                fate: Fate::Pending,
                durability: DurabilityTier::Local,
                global_time: None,
            }
        );
    }

    block_on(db.tick()).unwrap();
    assert_eq!(db.node.outbox.borrow().len(), 2);
}

/// Internal because the regression needs the exact overlap between a retained
/// owner operation holding node state and an earlier publication's settlement.
/// The browser counterpart performs insert/subscribe/insert without delays.
#[test]
fn deferred_publication_does_not_starve_the_queued_operation_holding_node_state() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa8; 16]);
    let db = open_db(0xa8, owner, &schema);
    db.set_deferred_local_persistence(true);
    let write = block_on(db.insert(
        "todos",
        cells("settle while next operation is cold", false, owner),
        Default::default(),
    ))
    .unwrap();
    let node = db.node.node();
    let (release, blocked) = futures::channel::oneshot::channel();
    db.node
        .enqueue_transaction_operation(
            OpenTransactionId::new(),
            Box::pin(async move {
                let _guard = node.lock().await;
                blocked.await.expect("owner operation must remain retained");
                Ok(())
            }),
        )
        .unwrap();
    db.drive_queued_mutation_once();

    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut tick = Box::pin(db.tick());
    assert!(matches!(
        tick.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(tick);
    assert_eq!(db.node.pending_local_publications.borrow().len(), 1);

    release.send(()).unwrap();
    block_on(db.tick()).unwrap();
    assert!(db.node.pending_local_publications.borrow().is_empty());
    assert_eq!(
        db.write_state(write.mergeable_tx_id()).unwrap().durability,
        DurabilityTier::Local
    );
    assert_eq!(prepared_read(&db, &db.table("todos")).len(), 1);
}

/// Causal flow: a mergeable transaction enters the node-owned deferred queue,
/// its subscriber refresh stalls, and the caller future is cancelled. The next
/// node tick must still persist the exact queued transaction.
#[test]
fn deferred_mergeable_handle_refresh_cancellation_keeps_persistence_owned() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa3; 16]);
    let db = open_db(0xa3, owner, &schema);
    db.set_deferred_local_persistence(true);
    let open = OpenTransactionId::new();
    block_on(db.begin_mergeable(open)).unwrap();
    block_on(db.mergeable_tx_ref(open).insert(
        "todos",
        cells("mergeable handle", false, owner),
        Default::default(),
    ))
    .unwrap();

    let published =
        cancel_during_subscription_refresh(&db, db.commit_mergeable_handle(open), false);
    settle_cancelled_local_publication(&db, published);
}

/// Causal flow: an exclusive transaction transfers its upload-bearing
/// publication to the deferred queue, refresh stalls, then its caller is
/// cancelled. The node tick must retain and persist that exact publication.
#[test]
fn deferred_exclusive_refresh_cancellation_keeps_persistence_owned() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa4; 16]);
    let db = open_db(0xa4, owner, &schema);
    db.set_deferred_local_persistence(true);
    let open = OpenTransactionId::new();
    block_on(db.begin_exclusive(open)).unwrap();
    block_on(db.exclusive_tx_ref(open).insert(
        "todos",
        cells("exclusive handle", false, owner),
        Default::default(),
    ))
    .unwrap();

    let published = cancel_during_subscription_refresh(&db, db.commit_exclusive_handle(open), true);
    settle_cancelled_local_publication(&db, published);
}

/// A deferred local writer and its resident subscriber share one runtime: the
/// write must queue durability work and still deliver the visible subscription
/// delta before the write call returns.
#[test]
fn deferred_write_still_refreshes_resident_subscriptions_before_returning() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa5; 16]);
    let db = open_db(0xa5, owner, &schema);
    db.set_deferred_local_persistence(true);
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let _opening = block_on(subscription.next_raw()).unwrap();

    let write = block_on(db.insert(
        "todos",
        cells("resident refresh", false, owner),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(
        queued_local_publication(&db, false),
        write.mergeable_tx_id()
    );
    let event = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta { added, .. } = event else {
        panic!("successful deferred write must publish a resident delta");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.row_uuid(), write.row_uuid());
}

/// Internal binding-contract regression: an async client helper would drive
/// extra owner turns and hide whether one resident admission poll publishes.
#[test]
fn queued_resident_insert_publishes_subscription_in_one_admission_turn() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean),
        ),
    );
    let owner = AuthorSubject::for_test_bytes([0xb4; 16]);
    let db = block_on(Db::open_history_complete(DbConfig::new(
        schema.clone(),
        crate::groove::storage::MemoryStorage::new(
            &schema
                .column_families()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
        )
        .unwrap(),
        DbIdentity {
            node: NodeUuid::from_bytes([0xb4; 16]),
            author: owner,
        },
    )))
    .unwrap();
    struct HostScheduler;
    impl TickScheduler for HostScheduler {
        fn schedule_tick(&self, _: TickUrgency) {}
        fn schedule_tick_after(&self, _: u64) {}
        fn query_runtime_waker(&self) -> Option<Waker> {
            Some(Waker::noop().clone())
        }
    }
    db.set_tick_scheduler(Some(Rc::new(HostScheduler)));
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let _opening = block_on(subscription.next_raw()).unwrap();
    let write = db
        .enqueue_insert(
            "todos".to_owned(),
            [
                ("title".into(), Value::String("resident queued row".into())),
                ("done".into(), Value::Bool(false)),
            ]
            .into(),
            Default::default(),
        )
        .unwrap();
    db.drive_queued_mutation_once();
    let event = subscription
        .try_next_event()
        .expect("resident insertion must publish in its admission turn");
    let SubscriptionEvent::Delta { added, .. } = event else {
        panic!("expected a row delta")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.row_uuid(), write.row_uuid());
}

/// Alice inserts, populates, and clears an optional JSON cell. SQL null must
/// remain distinct from a JSON document whose contents are the literal null.
#[test]
fn nullable_json_round_trips_null_and_populated_cells() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .nullable_column("metadata", PublicColumnType::Json { schema: None }),
        ),
    );
    let db = open_db(0x6d, AuthorSubject::SYSTEM, &schema);
    let inserted = db
        .insert(
            "documents",
            BTreeMap::from([("metadata".to_owned(), Value::Nullable(None))]),
            Default::default(),
        )
        .unwrap()
        .row_uuid();
    let query = db.prepare_query(&db.table("documents")).unwrap();
    let values = [
        Value::Nullable(None),
        Value::Nullable(Some(Box::new(Value::String("{\"answer\":42}".into())))),
        Value::Nullable(None),
        Value::Nullable(Some(Box::new(Value::String("null".into())))),
    ];
    for value in values {
        db.update(
            "documents",
            inserted,
            BTreeMap::from([("metadata".to_owned(), value.clone())]),
            Default::default(),
        )
        .unwrap();
        let rows = db.read(&query).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].cell(&schema.tables[0], "metadata"), Some(value));
    }
}
