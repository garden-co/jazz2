//! Prepared reads, plan installation, runtime invalidation, and ordered snapshots.

use super::*;

fn joined_issue_query() -> Query {
    Query::from("issues").join_via("issue_tags", "issue", [eq(col("tag"), lit("prepared"))])
}

fn indexed_documents_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .column("team", PublicColumnType::Uuid)
                .column("active", PublicColumnType::Boolean)
                .column("title", PublicColumnType::Text)
                .index_only(["team"]),
        ),
    )
}

fn multi_index_documents_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .column("team", PublicColumnType::Uuid)
                .column("active", PublicColumnType::Boolean)
                .column("title", PublicColumnType::Text)
                .index_only(["team", "active"]),
        ),
    )
}

/// A maintained equality source is both an indexed hydration source and a
/// live IVM source. In particular, a row changing either indexed equality must
/// enter/leave exactly once rather than remaining filtered by the prefix that
/// selected the initial snapshot.
#[test]
fn maintained_multi_index_query_tracks_either_index_transition() {
    let schema = multi_index_documents_schema();
    let author = AuthorSubject::for_test_bytes([0xd1; 16]);
    let db = open_db(0xd1, author, &schema);
    let team = row(0xa0);
    let matching = row(1);
    let inactive = row(2);
    let other_team = row(3);
    let cells = |team: RowUuid, active: bool, title: &str| {
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("active".to_owned(), Value::Bool(active)),
            ("title".to_owned(), Value::String(title.to_owned())),
        ])
    };
    for (id, values) in [
        (matching, cells(team, true, "matching")),
        (inactive, cells(team, false, "inactive")),
        (other_team, cells(row(0xb0), true, "other team")),
    ] {
        db.insert(
            "documents",
            values,
            crate::db::InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let query = Query::from("documents")
        .filter(eq(col("team"), lit(Value::Uuid(team.0))))
        .filter(eq(col("active"), lit(true)));
    db.node.node.borrow_mut().reset_query_engine_read_metrics();
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&initial.rows), vec![matching]);
    assert!(
        db.node
            .node
            .borrow()
            .query_engine_read_metrics()
            .source_index_probes
            >= 2,
        "the maintained Local source must probe both equality indices"
    );

    db.update(
        "documents",
        inactive,
        BTreeMap::from([("active".to_owned(), Value::Bool(true))]),
        Default::default(),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![inactive]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.update(
        "documents",
        inactive,
        BTreeMap::from([("team".to_owned(), Value::Uuid(row(0xb0).0))]),
        Default::default(),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed.iter().map(|row| row.row_uuid).collect::<Vec<_>>(),
        vec![inactive]
    );
}

/// Empty durable index prefixes remain live sources. The first matching row
/// must not be lost merely because indexed hydration had no row to materialize.
#[test]
fn maintained_empty_index_prefix_delivers_first_matching_insert_once() {
    let schema = indexed_documents_schema();
    let author = AuthorSubject::for_test_bytes([0xd2; 16]);
    let db = open_db(0xd2, author, &schema);
    let team = row(0xa2);
    let query = Query::from("documents").filter(eq(col("team"), lit(Value::Uuid(team.0))));
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert!(
        initial.rows.is_empty(),
        "empty prefix must still open a subscription"
    );

    let first = row(4);
    db.insert(
        "documents",
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("active".to_owned(), Value::Bool(true)),
            ("title".to_owned(), Value::String("first".to_owned())),
        ]),
        crate::db::InsertOptions {
            row_id: Some(first),
            ..Default::default()
        },
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![first]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert!(
        subscription.try_next_event().is_none(),
        "first insert must deliver exactly once"
    );
}

/// A Local subscription remains complete when its settled source is indexed;
/// the empty ahead overlay does not replace that indexed snapshot.
#[test]
fn maintained_local_index_snapshot_is_complete() {
    let schema = indexed_documents_schema();
    let author = AuthorSubject::for_test_bytes([0xd3; 16]);
    let db = open_db(0xd3, author, &schema);
    let team = row(0xa3);
    let matching = row(5);
    for (id, row_team) in [(matching, team), (row(6), row(0xb3))] {
        db.seed_settled_mergeable_for_bootstrap(
            "documents",
            id,
            author,
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(row_team.0)),
                ("active".to_owned(), Value::Bool(true)),
                ("title".to_owned(), Value::String("settled".to_owned())),
            ]),
        )
        .unwrap();
    }

    let query = Query::from("documents").filter(eq(col("team"), lit(Value::Uuid(team.0))));
    db.node.node.borrow_mut().reset_query_engine_read_metrics();
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    let snapshot = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&snapshot.rows), vec![matching]);
    let node = db.node.node.borrow();
    let metrics = node.query_engine_read_metrics();
    assert!(metrics.source_index_probes >= 1);
}

/// A Global subscription starts empty until its authority has settled the
/// indexed source, then installs the same complete indexed snapshot that its
/// Local counterpart would observe. This exercises the asynchronous delivery
/// boundary that previously made the direct maintained index experiment lose
/// fresh snapshots on worker-backed storage.
#[test]
fn maintained_global_index_snapshot_waits_for_settled_source() {
    let schema = indexed_documents_schema();
    let client_author = AuthorSubject::for_test_bytes([0xd5; 16]);
    let server = open_core(0xd4, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xd5, client_author, &schema);
    let team = row(0xa4);
    let matching = seed(
        &server,
        "documents",
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(team.0)),
            ("active".to_owned(), Value::Bool(true)),
            ("title".to_owned(), Value::String("matching".to_owned())),
        ]),
    );
    seed(
        &server,
        "documents",
        BTreeMap::from([
            ("team".to_owned(), Value::Uuid(row(0xb4).0)),
            ("active".to_owned(), Value::Bool(true)),
            ("title".to_owned(), Value::String("unrelated".to_owned())),
        ]),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("documents").filter(eq(col("team"), lit(Value::Uuid(team.0))));
    server.node().borrow_mut().reset_query_engine_read_metrics();
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(
        opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty(),
        "Global must not claim a snapshot before the authority settles it"
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let snapshot = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&snapshot.rows), vec![matching]);
    assert!(
        server
            .node()
            .borrow()
            .query_engine_read_metrics()
            .source_index_probes
            >= 1,
        "the authority must hydrate the settled Global source through its equality index"
    );
}

#[test]
fn negated_membership_uses_two_valued_null_semantics() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("items")
                .nullable_column("label", PublicColumnType::Text)
                .nullable_column("null_option", PublicColumnType::Text)
                .nullable_column("blocked_option", PublicColumnType::Text),
        ),
    );
    let db = open_db(0xb8, AuthorSubject::SYSTEM, &schema);
    for (id, label) in [
        (row(1), Value::Nullable(None)),
        (
            row(2),
            Value::Nullable(Some(Box::new(Value::String("blocked".to_owned())))),
        ),
        (
            row(3),
            Value::Nullable(Some(Box::new(Value::String("allowed".to_owned())))),
        ),
    ] {
        db.seed_settled_mergeable_for_bootstrap(
            "items",
            id,
            AuthorSubject::SYSTEM,
            BTreeMap::from([
                ("label".to_owned(), label),
                ("null_option".to_owned(), Value::Nullable(None)),
                (
                    "blocked_option".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String("blocked".to_owned())))),
                ),
            ]),
        )
        .unwrap();
    }

    let matching_ids = |options: &[&str]| {
        let query = Query::from("items")
            .filter(not(in_list(col("label"), options.iter().copied().map(col))));
        let prepared = db.prepare_query(&query).unwrap();
        row_ids(&db.read(&prepared).unwrap())
            .into_iter()
            .collect::<BTreeSet<_>>()
    };

    assert_eq!(
        matching_ids(&["null_option"]),
        BTreeSet::from([row(2), row(3)]),
        "NOT(null IN [null]) is false, while non-null values differ from null"
    );
    assert_eq!(
        matching_ids(&["blocked_option"]),
        BTreeSet::from([row(1), row(3)]),
        "a null value differs from every non-null membership option"
    );
    assert_eq!(
        matching_ids(&["null_option", "blocked_option"]),
        BTreeSet::from([row(3)]),
        "mixed options exclude both null and matching non-null values"
    );
}

#[test]
fn prepared_query_discards_graph_handle_when_runtime_changes() {
    let schema = issue_schema();
    let db = open_db(0xb7, AuthorSubject::SYSTEM, &schema);
    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    let runtime_token = db.node.node.borrow().groove_runtime_token();
    assert!(
        prepared
            .plan_for_tier(DurabilityTier::Local, runtime_token)
            .is_some()
    );
    assert!(
        prepared
            .plan_for_tier(DurabilityTier::Local, runtime_token.wrapping_add(1))
            .is_none()
    );
}

fn seed_issue_project(db: &Db<RocksDbStorage>, author: AuthorSubject) {
    db.seed_settled_mergeable_for_bootstrap(
        "projects",
        row(10),
        author,
        BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
    )
    .unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "issues",
        row(1),
        author,
        issue_cells("Platform", "open", author, row(10), 5, &["api"], None),
    )
    .unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "issue_tags",
        row(20),
        author,
        BTreeMap::from([
            ("issue".to_owned(), Value::Uuid(row(1).0)),
            ("tag".to_owned(), Value::String("prepared".to_owned())),
        ]),
    )
    .unwrap();
}

#[test]
fn prepared_current_write_query_installs_and_reads_non_simple_plan() {
    let schema = issue_schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0xa1, author, &schema);
    seed_issue_project(&db, author);

    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    assert!(prepared.has_plan_for_tier(DurabilityTier::Local));
    assert!(prepared.has_plan_for_tier(DurabilityTier::Global));
    db.node
        .node
        .borrow_mut()
        .clear_prepared_query_plan_cache_for_test();

    let rows = db.read(&prepared).unwrap();

    assert_eq!(row_ids(&rows), vec![row(1)]);
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "stored prepared plans should be used without replanning"
    );
}

#[test]
fn local_subscribe_uses_prepared_non_simple_plan() {
    let schema = issue_schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0xa2, author, &schema);
    seed_issue_project(&db, author);

    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    db.node
        .node
        .borrow_mut()
        .clear_prepared_query_plan_cache_for_test();

    let mut subscription = block_on(db.subscribe(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();

    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_raw()).unwrap())),
        vec![row(1)]
    );
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "initial subscribe read should consume the stored prepared plan"
    );
}

#[test]
fn subscription_reset_preserves_ordered_window_rank() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0xa3, author, &schema);
    for (id, title) in [(4, "alpha"), (1, "bravo"), (3, "charlie"), (2, "delta")] {
        db.seed_settled_mergeable_for_bootstrap(
            "todos",
            row(id),
            author,
            cells(title, false, author),
        )
        .unwrap();
    }

    let query = Query::from("todos")
        .order_by("title", OrderDirection::Asc)
        .offset(1)
        .limit(2);
    let mut subscription = prepared_subscribe(
        &db,
        &query,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    )
    .unwrap();

    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_raw()).unwrap())),
        vec![row(1), row(3)],
        "reset rows must retain the selected ordered window rather than member-key order"
    );
}

#[test]
fn subscription_reset_preserves_ordered_flat_join_window_with_duplicate_roots() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .nullable_fk_column("ownerId", "users"),
            ),
    );
    let db = open_db(0xc4, AuthorSubject::for_test_bytes([0xc4; 16]), &schema);
    for (id, name) in [(0xa1, "alice"), (0xb1, "maria"), (0xc1, "zoe")] {
        db.insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    for (id, owner) in [(0x11, 0xa1), (0x22, 0xa1), (0x33, 0xb1), (0x44, 0xc1)] {
        db.insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(format!("todo-{id:x}"))),
                (
                    "ownerId".to_owned(),
                    Value::Nullable(Some(Box::new(Value::Uuid(row(owner).0)))),
                ),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }

    // The selected window is maria, then Alice's two distinct joined
    // occurrences. Opaque terminal key map order would instead put Alice
    // before maria, so this specifically proves materialization follows the
    // lowered CollectBy sequence after custom sort, offset, and limit.
    let query = Query::from("users")
        .join_via_column("todos", "ownerId", "id", [])
        .order_by("name", OrderDirection::Desc)
        .offset(1)
        .limit(3);
    assert_eq!(
        row_ids(&prepared_read(&db, &query)),
        vec![row(0xb1), row(0xa1), row(0xa1)],
        "the direct query establishes the lowered collector order",
    );
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("flat join subscription must open with a delta");
    };
    assert_eq!(
        added
            .iter()
            .map(|output| output.row_uuid())
            .collect::<Vec<_>>(),
        vec![row(0xb1), row(0xa1), row(0xa1)],
    );
    assert_eq!(
        added
            .iter()
            .map(|output| output.occurrence_id.clone())
            .collect::<Vec<_>>(),
        vec![
            OutputOccurrenceId::new(
                ObjectId::from_uuid(row(0xb1).0),
                [ObjectId::from_uuid(row(0x33).0)],
            ),
            OutputOccurrenceId::new(
                ObjectId::from_uuid(row(0xa1).0),
                [ObjectId::from_uuid(row(0x11).0)],
            ),
            OutputOccurrenceId::new(
                ObjectId::from_uuid(row(0xa1).0),
                [ObjectId::from_uuid(row(0x22).0)],
            ),
        ]
    );
    block_on(subscription.close()).unwrap();

    // A non-key order update stays within the unwindowed result but changes
    // its rank. Groove emits an exact opaque-key Move; the public subscription
    // reducer consumes root terminal edits into indexed `updated` rows (only
    // descendant terminal edits cross this API boundary).
    let move_query = Query::from("users")
        .join_via_column("todos", "ownerId", "id", [])
        .order_by("name", OrderDirection::Desc);
    let mut moved = prepared_subscribe(&db, &move_query, ReadOpts::default()).unwrap();
    let _initial = block_on(moved.next_raw()).unwrap();
    db.update(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("zzzz".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    db.tick().unwrap();
    let SubscriptionEvent::Delta { updated, .. } = block_on(moved.next_raw()).unwrap() else {
        panic!("rank change must publish a structured delta");
    };
    assert!(updated.iter().any(|output| {
        output.row.row_uuid() == row(0xb1)
            && output.previous_index == Some(1)
            && output.index == 0
            && output.row.cell_at(0) == Some(Value::String("zzzz".to_owned()))
    }));
}

#[test]
fn simple_prepared_current_write_query_uses_lowered_plan() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0xa3, author, &schema);
    db.insert(
        "todos",
        cells("simple", false, author),
        crate::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    )
    .unwrap();

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Local));
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Global));

    let rows = db.read(&prepared).unwrap();

    assert_eq!(row_ids(&rows), vec![row(1)]);
    assert!(
        db.node
            .node
            .borrow()
            .prepared_query_plan_cache_is_empty_for_test(),
        "simple prepared current reads should stay on the direct lowered path without installing a shared plan"
    );
}

#[test]
fn filtered_root_prepared_query_still_reads_without_preinstalled_plan() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0xa4, author, &schema);
    db.insert(
        "todos",
        cells("wanted", false, author),
        crate::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    )
    .unwrap();

    let prepared = db
        .prepare_query(&Query::from("todos").filter(eq(col("title"), lit("wanted"))))
        .unwrap();
    assert!(!prepared.has_plan_for_tier(DurabilityTier::Local));
    assert_eq!(
        db.read(&prepared)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![row(1)]
    );
}
#[test]
fn profiled_read_matches_ordinary_read_for_unselected_query() {
    let schema = issue_schema();
    let author = AuthorSubject::for_test_bytes([0xa6; 16]);
    let db = open_db(0xa6, author, &schema);
    seed_issue_project(&db, author);

    let prepared = db.prepare_query(&joined_issue_query()).unwrap();
    let ordinary = db.read(&prepared).unwrap();
    let (profiled, _profile) = db.read_profiled(&prepared).unwrap();

    assert_eq!(
        profiled, ordinary,
        "profiled reads must preserve ordinary public rows and descriptors",
    );
}

#[test]
fn authoritative_global_bound_read_uses_the_declared_index() {
    // `Db::all` at Global consumes an upstream, identity-scoped result set.
    // A standalone authority must instead use the explicit serving API, which
    // evaluates the bound query against its complete settled state.
    let schema = indexed_documents_schema();
    let db = block_on(Db::open_history_complete(DbConfig {
        schema: schema.clone(),
        storage: rocks_storage(&schema),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0xa5; 16]),
            author: AuthorSubject::SYSTEM,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xa5))),
    }))
    .expect("open history-complete standalone authority");
    let wanted_team = row(0x51);
    let wanted = row(0x52);
    let other = row(0x53);
    for (id, team, title) in [(wanted, wanted_team, "wanted"), (other, row(0x54), "other")] {
        db.seed_settled_mergeable_for_bootstrap(
            "documents",
            id,
            AuthorSubject::SYSTEM,
            BTreeMap::from([
                ("team".to_owned(), Value::Uuid(team.0)),
                ("active".to_owned(), Value::Bool(true)),
                ("title".to_owned(), Value::String(title.to_owned())),
            ]),
        )
        .unwrap();
    }
    let prepared = db
        .prepare_query_bound(
            &Query::from("documents")
                .filter(eq(col("team"), param("team")))
                .filter(eq(col("active"), lit(true))),
            BTreeMap::from([("team".to_owned(), Value::Uuid(wanted_team.0))]),
        )
        .expect("prepare bound indexed query");

    db.node.node.borrow().reset_storage_read_metrics();
    let rows = block_on(db.all_for_identity(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
        AuthorSubject::SYSTEM,
    ))
    .expect("authoritative Global bound read");
    let metrics = db.node.node.borrow().take_storage_read_metrics();

    assert_eq!(row_ids(&rows), vec![wanted]);
    assert_eq!(metrics.global_current_indexes.reads, 1);
    assert_eq!(metrics.global_current_rows.reads, 1);
}

#[test]
fn relation_query_one_shot_hop_uses_unified_query_path() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("bob".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xb1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x11)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("bob todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xb1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x22)),
            ..Default::default()
        },
    )
    .unwrap();

    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "name".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::Literal(serde_json::Value::String(
                            "alice".to_owned(),
                        )),
                    },
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x11)]);
}

/// Internal relation-IR construction is necessary here because the Rust DB
/// integration surface is the public relation-query API exercised by WASM and
/// NAPI; the assertion is the user-visible one-shot membership result.
#[test]
fn relation_union_all_preserves_labeled_same_row_derivations() {
    let schema = relation_schema();
    let db = open_db(0xca, AuthorSubject::for_test_bytes([0xca; 16]), &schema);
    let alice = row(0xa1);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(alice),
            ..Default::default()
        },
    )
    .unwrap();
    let arm = |label: &str| crate::query::RelationUnionArm {
        label: label.to_owned(),
        input: RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: "users".to_owned(),
                alias: None,
            }),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("users".to_owned()),
                    column: "name".to_owned(),
                },
                op: RelationCmpOp::Eq,
                right: RelationValueRef::Literal(serde_json::Value::String("alice".to_owned())),
            },
        },
    };
    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Union {
                inputs: vec![arm("first"), arm("second")],
            }),
            columns: vec![crate::query::RelationProjectColumn {
                alias: "name".to_owned(),
                expr: RelationProjectExpr::Column(RelationColumnRef {
                    scope: Some("users".to_owned()),
                    column: "name".to_owned(),
                }),
            }],
        },
    };
    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![alice, alice]);

    let mut subscription =
        block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    let SubscriptionEvent::Delta { added: opened, .. } =
        subscription.try_next_event().expect("opened event")
    else {
        panic!("subscription opening must be a delta")
    };
    assert_eq!(
        opened
            .iter()
            .map(|output| output.row.row_uuid())
            .collect::<Vec<_>>(),
        vec![alice, alice]
    );
    assert_ne!(opened[0].occurrence_id, opened[1].occurrence_id);
    db.update(
        "users",
        alice,
        BTreeMap::from([("name".to_owned(), Value::String("bob".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    let (_, _, removed) = delta_rows(subscription.try_next_event().expect("removal event"));
    assert_eq!(
        removed.iter().map(|row| row.row_uuid).collect::<Vec<_>>(),
        vec![alice, alice]
    );
    assert_ne!(removed[0].occurrence_id, removed[1].occurrence_id);

    // Global order/window stays outside the UNION ALL arms. Rows from the
    // same physical source tie on the user order and UUID, so the semantic
    // arm carrier is the final deterministic page key.
    db.update(
        "users",
        alice,
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    let windowed_query = RelationQuery {
        rel: RelationExpr::Limit {
            input: Box::new(RelationExpr::Offset {
                input: Box::new(RelationExpr::OrderBy {
                    input: Box::new(query.rel),
                    terms: vec![RelationOrderBy {
                        column: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "name".to_owned(),
                        },
                        direction: OrderDirection::Asc,
                    }],
                }),
                offset: 1,
            }),
            limit: 1,
        },
    };
    let mut windowed =
        block_on(db.subscribe_relation_query(&windowed_query, ReadOpts::default())).unwrap();
    let SubscriptionEvent::Delta {
        added: windowed_opened,
        ..
    } = windowed.try_next_event().expect("windowed opening event")
    else {
        panic!("windowed subscription opening must be a delta");
    };
    assert_eq!(windowed_opened.len(), 1);
    assert_eq!(
        windowed_opened[0].occurrence_id.union_arms(),
        &[(0, "second".to_owned())],
        "the offset crosses the first physical-row occurrence into the second union arm",
    );
}

#[test]
fn relation_query_one_shot_hop_accepts_runtime_uuid_literal_filter() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("bob".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xb1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x11)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("bob todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xb1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x22)),
            ..Default::default()
        },
    )
    .unwrap();

    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "id".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::Literal(serde_json::json!({
                            "type": "Uuid",
                            "value": row(0xa1).0.to_string(),
                        })),
                    },
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x11)]);
}

#[test]
fn relation_query_one_shot_multi_hop_scalar_fk_uses_nested_join_path() {
    let schema = relation_hop_schema();
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    db.insert(
        "orgs",
        BTreeMap::from([("name".to_owned(), Value::String("Org A".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0x01)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "orgs",
        BTreeMap::from([("name".to_owned(), Value::String("Org B".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0x02)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "teams",
        BTreeMap::from([
            ("name".to_owned(), Value::String("Team A".to_owned())),
            (
                "org_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x01).0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x11)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "users",
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            (
                "team_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x11).0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x21)),
            ..Default::default()
        },
    )
    .unwrap();

    let query = users_to_orgs_relation_query();

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0x01)]);
}

#[test]
fn relation_query_subscription_hop_uses_unified_query_path() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("alice todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x11)),
            ..Default::default()
        },
    )
    .unwrap();

    let query = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::TableScan {
                    table: "users".to_owned(),
                    alias: None,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };

    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    let opened = opened_rows(stream.try_next_event().expect("opened event"));
    assert_eq!(row_ids(&opened), vec![row(0x11)]);
}

#[test]
fn relation_query_subscription_hop_preserves_projected_self_reference_cells() {
    let schema = relation_hop_schema();
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    let parent = row(0x10);
    let team = row(0x11);
    let user = row(0x21);
    db.insert(
        "teams",
        BTreeMap::from([("name".to_owned(), Value::String("Parent".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(parent),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "teams",
        BTreeMap::from([
            ("name".to_owned(), Value::String("Team A".to_owned())),
            (
                "parent_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(parent.0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(team),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "users",
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            (
                "team_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(team.0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(user),
            ..Default::default()
        },
    )
    .unwrap();

    let query = users_to_teams_relation_query();
    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![team]);
    assert_eq!(
        snapshot.rows[0].cell(&schema.tables[1], "name"),
        Some(Value::String("Team A".to_owned()))
    );
    assert_eq!(
        snapshot.rows[0].cell(&schema.tables[1], "parent_id"),
        Some(Value::Nullable(Some(Box::new(Value::Uuid(parent.0)))))
    );

    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    let opened = opened_rows(stream.try_next_event().expect("opened event"));
    let opened_team = opened
        .iter()
        .find(|row| row.row_uuid() == team)
        .expect("joined team row");
    assert_eq!(
        opened_team.cell(&schema.tables[1], "name"),
        Some(Value::String("Team A".to_owned()))
    );
    assert_eq!(
        opened_team.cell(&schema.tables[1], "parent_id"),
        Some(Value::Nullable(Some(Box::new(Value::Uuid(parent.0)))))
    );

    db.update(
        "teams",
        team,
        BTreeMap::from([("name".to_owned(), Value::String("Team B".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    let (_, changed, removed) = delta_rows(stream.try_next_event().expect("updated event"));
    assert!(removed.is_empty());
    assert_eq!(row_ids(&changed), vec![team]);
    assert_eq!(
        changed[0].cell(&schema.tables[1], "name"),
        Some(Value::String("Team B".to_owned()))
    );
    assert_eq!(
        changed[0].cell(&schema.tables[1], "parent_id"),
        Some(Value::Nullable(Some(Box::new(Value::Uuid(parent.0)))))
    );
}

fn users_to_teams_relation_query() -> RelationQuery {
    RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Filter {
                    input: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    predicate: RelationPredicate::Cmp {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "name".to_owned(),
                        },
                        op: RelationCmpOp::Eq,
                        right: RelationValueRef::Literal(serde_json::Value::String(
                            "User A".to_owned(),
                        )),
                    },
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "teams".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "team_id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "name".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "name".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "parent_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "parent_id".to_owned(),
                    }),
                },
            ],
        },
    }
}

#[test]
fn relation_query_subscription_multi_hop_scalar_fk_uses_nested_join_path() {
    let schema = relation_hop_schema();
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    let query = users_to_orgs_relation_query();
    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    assert!(opened_rows(stream.try_next_event().expect("opened event")).is_empty());

    db.insert(
        "orgs",
        BTreeMap::from([("name".to_owned(), Value::String("Org A".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0x01)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "teams",
        BTreeMap::from([
            ("name".to_owned(), Value::String("Team A".to_owned())),
            (
                "org_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x01).0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x11)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "users",
        BTreeMap::from([
            ("name".to_owned(), Value::String("User A".to_owned())),
            (
                "team_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0x11).0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x21)),
            ..Default::default()
        },
    )
    .unwrap();

    let opened = opened_rows(stream.try_next_event().expect("opened event"));
    assert_eq!(row_ids(&opened), vec![row(0x01)]);
}

fn users_to_orgs_relation_query() -> RelationQuery {
    RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::Join {
                    left: Box::new(RelationExpr::TableScan {
                        table: "users".to_owned(),
                        alias: None,
                    }),
                    right: Box::new(RelationExpr::TableScan {
                        table: "teams".to_owned(),
                        alias: Some("__hop_0".to_owned()),
                    }),
                    on: vec![crate::query::RelationJoinCondition {
                        left: RelationColumnRef {
                            scope: Some("users".to_owned()),
                            column: "team_id".to_owned(),
                        },
                        right: RelationColumnRef {
                            scope: Some("__hop_0".to_owned()),
                            column: "id".to_owned(),
                        },
                    }],
                    join_kind: RelationJoinKind::Inner,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "orgs".to_owned(),
                    alias: Some("__hop_1".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "org_id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_1".to_owned()),
                        column: "id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_1".to_owned()),
                        column: "id".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "name".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_1".to_owned()),
                        column: "name".to_owned(),
                    }),
                },
            ],
        },
    }
}

#[test]
fn relation_query_gather_uses_unified_reachable_lowering_for_reads_and_subscriptions() {
    // This is an integration-level facade test: the public relation-query read
    // and subscription APIs must both use the same maintained reachability
    // program for the canonical gather IR emitted by the TypeScript builder.
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("teams")
                .column("name", PublicColumnType::Text)
                .nullable_fk_column("parent_id", "teams"),
        ),
    );
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    let query = teams_gather_relation_query();
    let mut stream = block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    assert!(opened_rows(stream.try_next_event().expect("opened event")).is_empty());

    let root = row(0x01);
    let middle = row(0x02);
    let leaf = row(0x03);
    db.insert(
        "teams",
        BTreeMap::from([("name".to_owned(), Value::String("root".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(root),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "teams",
        BTreeMap::from([
            ("name".to_owned(), Value::String("middle".to_owned())),
            (
                "parent_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(root.0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(middle),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "teams",
        BTreeMap::from([
            ("name".to_owned(), Value::String("leaf".to_owned())),
            (
                "parent_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(middle.0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(leaf),
            ..Default::default()
        },
    )
    .unwrap();

    let changed = opened_rows(stream.try_next_event().expect("gathered rows event"));
    assert_eq!(
        row_ids(&changed).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, middle, leaf])
    );

    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    assert_eq!(
        row_ids(&snapshot.rows).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, middle, leaf])
    );

    let filtered_query = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(query.rel.clone()),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("teams".to_owned()),
                    column: "name".to_owned(),
                },
                op: RelationCmpOp::Ne,
                right: RelationValueRef::Literal(serde_json::Value::String("middle".to_owned())),
            },
        },
    };
    let filtered = block_on(db.all_relation_query(&filtered_query, ReadOpts::default())).unwrap();
    assert_eq!(
        row_ids(&filtered.rows).into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([root, leaf])
    );

    let or_true = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(query.rel.clone()),
            predicate: RelationPredicate::Or(vec![
                RelationPredicate::True,
                RelationPredicate::False,
            ]),
        },
    };
    let unfiltered = block_on(db.all_relation_query(&or_true, ReadOpts::default())).unwrap();
    assert_eq!(
        row_ids(&unfiltered.rows)
            .into_iter()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([root, middle, leaf])
    );

    let not_true = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(query.rel.clone()),
            predicate: RelationPredicate::Not(Box::new(RelationPredicate::True)),
        },
    };
    let empty = block_on(db.all_relation_query(&not_true, ReadOpts::default())).unwrap();
    assert!(empty.rows.is_empty());

    let filter_after_limit = RelationQuery {
        rel: RelationExpr::Filter {
            input: Box::new(RelationExpr::Limit {
                input: Box::new(RelationExpr::OrderBy {
                    input: Box::new(query.rel.clone()),
                    terms: vec![RelationOrderBy {
                        column: RelationColumnRef {
                            scope: Some("teams".to_owned()),
                            column: "name".to_owned(),
                        },
                        direction: OrderDirection::Asc,
                    }],
                }),
                limit: 1,
            }),
            predicate: RelationPredicate::Cmp {
                left: RelationColumnRef {
                    scope: Some("teams".to_owned()),
                    column: "name".to_owned(),
                },
                op: RelationCmpOp::Eq,
                right: RelationValueRef::Literal(serde_json::Value::String("root".to_owned())),
            },
        },
    };
    let error =
        block_on(db.all_relation_query(&filter_after_limit, ReadOpts::default())).unwrap_err();
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error
            .message
            .contains("gather output filters cannot wrap limit or offset")
    );
}

fn teams_gather_relation_query() -> RelationQuery {
    RelationQuery {
        rel: RelationExpr::Gather {
            seed: Box::new(RelationExpr::Filter {
                input: Box::new(RelationExpr::TableScan {
                    table: "teams".to_owned(),
                    alias: None,
                }),
                predicate: RelationPredicate::Cmp {
                    left: RelationColumnRef {
                        scope: Some("teams".to_owned()),
                        column: "name".to_owned(),
                    },
                    op: RelationCmpOp::Eq,
                    right: RelationValueRef::Literal(serde_json::Value::String("leaf".to_owned())),
                },
            }),
            step: Box::new(RelationExpr::Project {
                input: Box::new(RelationExpr::Join {
                    left: Box::new(RelationExpr::Filter {
                        input: Box::new(RelationExpr::TableScan {
                            table: "teams".to_owned(),
                            alias: None,
                        }),
                        predicate: RelationPredicate::And(vec![RelationPredicate::Cmp {
                            left: RelationColumnRef {
                                scope: Some("teams".to_owned()),
                                column: "id".to_owned(),
                            },
                            op: RelationCmpOp::Eq,
                            right: RelationValueRef::RowId(RelationRowIdRef::Frontier),
                        }]),
                    }),
                    right: Box::new(RelationExpr::TableScan {
                        table: "teams".to_owned(),
                        alias: Some("__recursive_hop_0".to_owned()),
                    }),
                    on: vec![crate::query::RelationJoinCondition {
                        left: RelationColumnRef {
                            scope: Some("teams".to_owned()),
                            column: "parent_id".to_owned(),
                        },
                        right: RelationColumnRef {
                            scope: Some("__recursive_hop_0".to_owned()),
                            column: "id".to_owned(),
                        },
                    }],
                    join_kind: RelationJoinKind::Inner,
                }),
                columns: vec![crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__recursive_hop_0".to_owned()),
                        column: "id".to_owned(),
                    }),
                }],
            }),
            frontier_key: crate::query::RelationKeyRef::RowId(RelationRowIdRef::Current),
            bound: crate::query::RecursionBound::MaxDepth(10),
            dedupe_key: vec![crate::query::RelationKeyRef::RowId(
                RelationRowIdRef::Current,
            )],
        },
    }
}
#[derive(Clone, Copy, Debug)]
enum RelationWindowCase {
    OffsetOutsideLimit,
    LimitOutsideOffset,
    NestedLimits,
    NestedOffsets,
}

fn projected_users_relation_expr() -> RelationExpr {
    RelationExpr::Project {
        input: Box::new(RelationExpr::TableScan {
            table: "users".to_owned(),
            alias: None,
        }),
        columns: vec![
            crate::query::RelationProjectColumn {
                alias: "id".to_owned(),
                expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
            },
            crate::query::RelationProjectColumn {
                alias: "name".to_owned(),
                expr: RelationProjectExpr::Column(RelationColumnRef {
                    scope: Some("users".to_owned()),
                    column: "name".to_owned(),
                }),
            },
        ],
    }
}

fn ordered_relation_expr(input: RelationExpr, scope: &str) -> RelationExpr {
    RelationExpr::OrderBy {
        input: Box::new(input),
        terms: vec![RelationOrderBy {
            column: RelationColumnRef {
                scope: Some(scope.to_owned()),
                column: "name".to_owned(),
            },
            direction: OrderDirection::Asc,
        }],
    }
}

fn windowed_relation_expr(input: RelationExpr, case: RelationWindowCase) -> RelationExpr {
    match case {
        RelationWindowCase::OffsetOutsideLimit => RelationExpr::Offset {
            input: Box::new(RelationExpr::Limit {
                input: Box::new(input),
                limit: 5,
            }),
            offset: 3,
        },
        RelationWindowCase::LimitOutsideOffset => RelationExpr::Limit {
            input: Box::new(RelationExpr::Offset {
                input: Box::new(input),
                offset: 3,
            }),
            limit: 5,
        },
        RelationWindowCase::NestedLimits => RelationExpr::Limit {
            input: Box::new(RelationExpr::Limit {
                input: Box::new(input),
                limit: 5,
            }),
            limit: 3,
        },
        RelationWindowCase::NestedOffsets => RelationExpr::Offset {
            input: Box::new(RelationExpr::Offset {
                input: Box::new(input),
                offset: 3,
            }),
            offset: 2,
        },
    }
}

fn relation_window_cases() -> [(RelationWindowCase, Vec<RowUuid>); 4] {
    [
        (RelationWindowCase::OffsetOutsideLimit, vec![row(4), row(5)]),
        (
            RelationWindowCase::LimitOutsideOffset,
            vec![row(4), row(5), row(6), row(7), row(8)],
        ),
        (
            RelationWindowCase::NestedLimits,
            vec![row(1), row(2), row(3)],
        ),
        (
            RelationWindowCase::NestedOffsets,
            vec![row(6), row(7), row(8)],
        ),
    ]
}

#[test]
fn relation_query_pagination_composes_windows_for_reads_and_subscriptions() {
    let schema = relation_schema();
    let db = open_db(0xe1, AuthorSubject::for_test_bytes([0xe1; 16]), &schema);
    for (id, name) in [
        (1, "a"),
        (2, "b"),
        (3, "c"),
        (4, "d"),
        (5, "e"),
        (6, "f"),
        (7, "g"),
        (8, "h"),
    ] {
        db.insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }

    for (case, expected) in relation_window_cases() {
        let query = RelationQuery {
            rel: windowed_relation_expr(
                ordered_relation_expr(projected_users_relation_expr(), "users"),
                case,
            ),
        };
        let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
        assert_eq!(row_ids(&snapshot.rows), expected, "{case:?}");
    }

    let query = RelationQuery {
        rel: windowed_relation_expr(
            ordered_relation_expr(projected_users_relation_expr(), "users"),
            RelationWindowCase::OffsetOutsideLimit,
        ),
    };
    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    let mut subscription =
        block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    let opened = opened_rows(subscription.try_next_event().expect("opened event"));
    assert_eq!(row_ids(&opened), row_ids(&snapshot.rows));
}

#[test]
fn relation_query_gather_pagination_composes_windows_for_reads_and_subscriptions() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("teams")
                .column("name", PublicColumnType::Text)
                .nullable_fk_column("parent_id", "teams"),
        ),
    );
    let db = open_db(0xe2, AuthorSubject::for_test_bytes([0xe2; 16]), &schema);
    for (id, name, parent) in [
        (1, "a", None),
        (2, "b", Some(1)),
        (3, "c", Some(2)),
        (4, "d", Some(3)),
        (5, "e", Some(4)),
        (6, "f", Some(5)),
        (7, "g", Some(6)),
        (8, "leaf", Some(7)),
    ] {
        let mut values = BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]);
        if let Some(parent) = parent {
            values.insert(
                "parent_id".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(parent).0)))),
            );
        }
        db.insert(
            "teams",
            values,
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = RelationQuery {
        rel: windowed_relation_expr(
            ordered_relation_expr(teams_gather_relation_query().rel, "teams"),
            RelationWindowCase::OffsetOutsideLimit,
        ),
    };
    let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
    let mut subscription =
        block_on(db.subscribe_relation_query(&query, ReadOpts::default())).unwrap();
    let opened = opened_rows(subscription.try_next_event().expect("opened event"));
    assert_eq!(row_ids(&opened), row_ids(&snapshot.rows));

    for (case, expected) in relation_window_cases() {
        let query = RelationQuery {
            rel: windowed_relation_expr(
                ordered_relation_expr(teams_gather_relation_query().rel, "teams"),
                case,
            ),
        };
        let snapshot = block_on(db.all_relation_query(&query, ReadOpts::default())).unwrap();
        assert_eq!(row_ids(&snapshot.rows), expected, "{case:?}");
    }
}

#[test]
fn relation_snapshot_reverse_array_skips_deleted_children() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("deleted todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x11)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("visible todo".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x22)),
            ..Default::default()
        },
    )
    .unwrap();
    db.delete("todos", row(0x11), Default::default()).unwrap();

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(ArraySubquery::new(
            "todosViaOwner",
            "todos",
            "owner_id",
            "id",
        ))
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        vec!["visible todo".to_owned()]
    );
}

#[test]
fn maintained_subscription_with_two_reference_includes_opens_with_source_coverage() {
    let schema = access_edge_include_schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0xee, AuthorSubject::SYSTEM, &schema);
    server
        .insert_with_id(
            "teams",
            row(0xa1),
            BTreeMap::from([("name".to_owned(), Value::String("resource team".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "teams",
            row(0xb1),
            BTreeMap::from([("name".to_owned(), Value::String("member team".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "team_access_edges",
            row(0xc1),
            BTreeMap::from([
                ("resource_id".to_owned(), Value::Uuid(row(0xa1).0)),
                ("team_id".to_owned(), Value::Uuid(row(0xb1).0)),
            ]),
        )
        .unwrap();

    let query = Query::from("team_access_edges")
        .include("resource_id")
        .include("team_id");
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    let message = drive_subscriber_until_payload(&subscriber, client_transport.as_mut());
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription: served,
        supporting_rows: program_fact_adds,
        ..
    }) = message
    else {
        panic!("expected include subscription view update, got {message:?}");
    };
    assert_eq!(served, subscription);
    let mut tables = program_fact_adds
        .iter()
        .map(|input| input.version_table.as_str())
        .collect::<Vec<_>>();
    tables.sort_unstable();
    assert_eq!(tables, vec!["team_access_edges", "teams", "teams"]);

    client_transport
        .send(SyncMessage::Unsubscribe { subscription })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    let message = drive_subscriber_until_payload(&subscriber, client_transport.as_mut());
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription: served,
        supporting_rows: program_fact_adds,
        ..
    }) = message
    else {
        panic!("expected reopened include subscription view update, got {message:?}");
    };
    assert_eq!(served, subscription);
    let mut tables = program_fact_adds
        .iter()
        .map(|input| input.version_table.as_str())
        .collect::<Vec<_>>();
    tables.sort_unstable();
    assert_eq!(tables, vec!["team_access_edges", "teams", "teams"]);
}

#[test]
fn relation_snapshot_reverse_array_skips_deleted_children_with_camel_case_ref() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("done", PublicColumnType::Boolean)
                    .nullable_fk_column("ownerId", "users"),
            ),
    );
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("deleted todo".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            (
                "ownerId".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0xa1).0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x11)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("visible todo".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            (
                "ownerId".to_owned(),
                Value::Nullable(Some(Box::new(Value::Uuid(row(0xa1).0)))),
            ),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x22)),
            ..Default::default()
        },
    )
    .unwrap();
    let joined_before_delete = prepared_read(
        &db,
        &Query::from("users").join_via_column("todos", "ownerId", "id", []),
    );
    assert_eq!(row_ids(&joined_before_delete), vec![row(0xa1), row(0xa1)]);
    let occurrence = |joined| {
        OutputOccurrenceId::new(
            ObjectId::from_uuid(row(0xa1).0),
            [ObjectId::from_uuid(row(joined).0)],
        )
    };
    let joined_snapshot = RelationSnapshot {
        root_count: joined_before_delete.len(),
        rows: joined_before_delete.clone(),
        edges: Vec::new(),
    };
    assert!(subscription_outputs_with_occurrence_sidecar(&joined_snapshot, &[]).is_err());
    assert!(
        subscription_outputs_with_occurrence_sidecar(
            &joined_snapshot,
            &[occurrence(0x11), occurrence(0x11)],
        )
        .is_err()
    );
    assert!(
        subscription_outputs_with_occurrence_sidecar(
            &joined_snapshot,
            &[
                OutputOccurrenceId::single_source(ObjectId::from_uuid(row(0xbb).0)),
                occurrence(0x22),
            ],
        )
        .is_err()
    );
    let joined_query = Query::from("users").join_via_column("todos", "ownerId", "id", []);
    let prepared_join = prepared(&db, &joined_query);
    let mut subscription = block_on(db.subscribe(&prepared_join, ReadOpts::default())).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("joined subscription must start with a delta");
    };
    assert_eq!(added.len(), 2);
    let occurrence_ids = added
        .iter()
        .map(|output| output.occurrence_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(occurrence_ids.len(), 2);
    assert_eq!(
        added
            .iter()
            .map(|output| output.occurrence_id.clone())
            .collect::<Vec<_>>(),
        vec![occurrence(0x11), occurrence(0x22)]
    );
    assert!(
        added
            .iter()
            .all(|output| output.occurrence_id.canonical_bytes().len() == 32)
    );
    db.delete("todos", row(0x11), Default::default()).unwrap();
    db.tick().unwrap();
    let SubscriptionEvent::Delta { removed, .. } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("joined occurrence removal must emit a delta");
    };
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].occurrence_id, occurrence(0x11));

    let joined = prepared_read(
        &db,
        &Query::from("users").join_via_column("todos", "ownerId", "id", []),
    );
    assert_eq!(row_ids(&joined), vec![row(0xa1)]);

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "ownerId", "id").select(["id"]),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaOwner", "row_uuid"),
        vec![Value::Uuid(row(0x22).0)]
    );
}

/// Alice reads a projected reverse include whose parent has an unrelated JSON cell.
/// The binding hydration boundary must expose the parent and child without physical types.
#[test]
fn relation_snapshot_json_parent_binding_hydration() {
    assert_relation_snapshot_json_binding_hydration(false);
}

/// Alice includes a child with JSON, so collector anchor and child arms must
/// agree on the physical JSON descriptor before binding hydration.
#[test]
fn relation_snapshot_json_child_binding_hydration() {
    assert_relation_snapshot_json_binding_hydration(true);
}

fn assert_relation_snapshot_json_binding_hydration(child_json: bool) {
    let mut children = PublicTableSchemaBuilder::new("children").fk_column("parentId", "parents");
    if child_json {
        children = children.column("metadata", PublicColumnType::Json { schema: None });
    }
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("parents")
                    .column("name", PublicColumnType::Text)
                    .column("metadata", PublicColumnType::Json { schema: None }),
            )
            .table(children),
    );
    let db = open_db(0xc2, AuthorSubject::for_test_bytes([0xc2; 16]), &schema);
    // Db's binding-facing API accepts core cells; row_input! is for JazzClient's
    // public Value algebra and cannot represent this lower-level input type.
    let parent = db
        .insert(
            "parents",
            BTreeMap::from([
                ("name".into(), Value::String("alice".into())),
                ("metadata".into(), Value::String("{}".into())),
            ]),
            Default::default(),
        )
        .unwrap()
        .row_uuid();
    let mut child_cells = BTreeMap::from([("parentId".into(), Value::Uuid(parent.0))]);
    if child_json {
        child_cells.insert("metadata".into(), Value::String("{}".into()));
    }
    let child = db
        .insert("children", child_cells, Default::default())
        .unwrap()
        .row_uuid();
    let children = ArraySubquery::new("childrenViaParent", "children", "parentId", "id");
    let children = if child_json {
        children
    } else {
        children.select(["id"])
    };
    let query = Query::from("parents")
        .select(["id"])
        .array_subquery(children);
    let prepared = db.prepare_query(&query).unwrap();
    let mut snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    block_on(db.hydrate_rows_for_binding(&mut snapshot.rows)).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![parent]);
    assert_eq!(
        terminal_nested_values(&snapshot, parent, "childrenViaParent", "row_uuid"),
        vec![Value::Uuid(child.0)]
    );
}

#[test]
fn relation_snapshot_reverse_array_reads_local_nullable_ref_child() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .nullable_fk_column("ownerId", "users"),
            ),
    );
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    let user = db
        .insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
            Default::default(),
        )
        .unwrap()
        .row_uuid();
    let todo = db
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("visible todo".to_owned())),
                (
                    "ownerId".to_owned(),
                    Value::Nullable(Some(Box::new(Value::Uuid(user.0)))),
                ),
            ]),
            Default::default(),
        )
        .unwrap()
        .row_uuid();

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(user.0))))
        .array_subquery(
            ArraySubquery::new("todosViaOwner", "todos", "ownerId", "id").select(["id"]),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();

    assert_eq!(row_ids(&snapshot.rows), vec![user]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, user, "todosViaOwner", "row_uuid"),
        vec![Value::Uuid(todo.0)]
    );
}

#[test]
fn relation_snapshot_reverse_array_limit_reads_local_child() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("projects").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .fk_column("projectId", "projects"),
            ),
    );
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    let project = db
        .insert(
            "projects",
            BTreeMap::from([("name".to_owned(), Value::String("Announcements".to_owned()))]),
            Default::default(),
        )
        .unwrap()
        .row_uuid();
    let _todo = db
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("visible todo".to_owned())),
                ("projectId".to_owned(), Value::Uuid(project.0)),
            ]),
            Default::default(),
        )
        .unwrap()
        .row_uuid();

    let query = Query::from("projects")
        .filter(eq(col("id"), lit(Value::Uuid(project.0))))
        .array_subquery(
            ArraySubquery::new("todosViaProject", "todos", "projectId", "id")
                .select(["title"])
                .limit(1),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();

    assert_eq!(row_ids(&snapshot.rows), vec![project]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_text_values(&snapshot, project, "todosViaProject", "title"),
        vec!["visible todo".to_owned()]
    );
}

#[test]
fn relation_snapshot_unordered_array_offset_uses_child_row_id_order() {
    let schema = relation_schema();
    let db = open_db(0xd4, AuthorSubject::for_test_bytes([0xd4; 16]), &schema);
    let parent = row(0x41);
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(parent),
            ..Default::default()
        },
    )
    .unwrap();
    for id in [0xb1, 0xb2, 0xb3] {
        db.insert(
            "comments",
            BTreeMap::from([
                ("body".to_owned(), Value::String("tie".to_owned())),
                ("todo_id".to_owned(), Value::Uuid(parent.0)),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let query = Query::from("todos").array_subquery(
        ArraySubquery::new("comments", "comments", "todo_id", "id")
            .offset(1)
            .limit(1),
    );
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();

    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, parent, "comments", "row_uuid"),
        vec![Value::Uuid(row(0xb2).0)]
    );
}

#[test]
fn relation_snapshot_reverse_array_projects_provenance_magic_columns() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("projects").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("done", PublicColumnType::Boolean)
                    .column(
                        "tags",
                        PublicColumnType::Array {
                            element: Box::new(PublicColumnType::Text),
                        },
                    )
                    .fk_column("projectId", "projects")
                    .nullable_fk_column("ownerId", "users")
                    .array_fk_column("assigneesIds", "users"),
            )
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text)),
    );
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    db.insert(
        "projects",
        BTreeMap::from([("name".to_owned(), Value::String("Announcements".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("Write tests".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            (
                "tags".to_owned(),
                Value::Array(vec![Value::String("dev".to_owned())]),
            ),
            ("projectId".to_owned(), Value::Uuid(row(0xa1).0)),
            ("ownerId".to_owned(), Value::Nullable(None)),
            ("assigneesIds".to_owned(), Value::Array(Vec::new())),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x22)),
            ..Default::default()
        },
    )
    .unwrap();

    let query = Query::from("projects")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(
            ArraySubquery::new("todosViaProject", "todos", "projectId", "id")
                .select([
                    "title",
                    "done",
                    "tags",
                    "projectId",
                    "ownerId",
                    "assigneesIds",
                    "$createdAt",
                    "$updatedAt",
                ])
                .limit(1),
        )
        .limit(1);
    let prepared = db.prepare_query(&query).unwrap();
    let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);
    assert!(snapshot.edges.is_empty());
    assert_eq!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaProject", "row_uuid"),
        vec![Value::Uuid(row(0x22).0)]
    );
    assert!(matches!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaProject", "$createdAt").as_slice(),
        [Value::U64(_)]
    ));
    assert!(matches!(
        terminal_nested_values(&snapshot, row(0xa1), "todosViaProject", "$updatedAt").as_slice(),
        [Value::U64(_)]
    ));
}

#[test]
fn version_bearing_current_source_preserves_provenance_timestamps() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let id = row(0x7a);
    db.insert(
        "todos",
        doctest_support::todo_cells("provenance", false),
        crate::db::InsertOptions {
            row_id: Some(id),
            updated_at_ms: Some(1_234),
            ..Default::default()
        },
    )
    .unwrap();
    {
        let mut node = db.node.node.borrow_mut();
        let table = node.table("todos").unwrap().clone();
        let rows = node
            .test_content_current_with_version(&table, DurabilityTier::Local)
            .unwrap();
        let created_at = rows.descriptor.field_index("created_at").unwrap();
        let record = rows
            .iter()
            .find(|(record, weight)| *weight > 0 && record.get_uuid(0).unwrap() == id.0)
            .unwrap()
            .0;
        assert_eq!(record.get_u64(created_at).unwrap(), 1_234);
    }

    let query = db
        .table("todos")
        .select(["title", "$createdAt", "$updatedAt"])
        .filter(eq(col("id"), lit(Value::Uuid(id.0))));
    let prepared = db.prepare_query(&query).unwrap();
    let rows = block_on(db.all(&prepared, ReadOpts::default())).unwrap();
    let row = rows.iter().find(|row| row.row_uuid() == id).unwrap();
    assert_eq!(row.raw_field("$createdAt"), Some(Value::U64(1_234)));
    assert_eq!(row.raw_field("$updatedAt"), Some(Value::U64(1_234)));
    assert_eq!(row.raw_field("user_done"), None);
}

/// The native descriptor is only observable at the binding boundary, so this
/// exercises a public subscription and then checks its encoded carrier.
#[test]
fn subscription_opening_retains_selected_created_at_in_native_carrier() {
    use crate::binding_codec::{RowDescriptorFieldName, row_batches};

    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let id = row(0x7b);
    db.insert(
        "todos",
        doctest_support::todo_cells("subscription provenance", false),
        crate::db::InsertOptions {
            row_id: Some(id),
            updated_at_ms: Some(4_321),
            ..Default::default()
        },
    )
    .unwrap();
    let query = db
        .table("todos")
        .select(["title", "$createdAt"])
        .filter(eq(col("id"), lit(Value::Uuid(id.0))));
    let prepared = db.prepare_query(&query).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_event()).unwrap()
    else {
        panic!("expected opening subscription delta");
    };
    let rows = added.into_iter().map(|row| row.row).collect::<Vec<_>>();
    let batches = row_batches(&rows).expect("opening rows encode for the native binding");
    assert!(batches[0].descriptor.iter().any(|field| matches!(
        field.name,
        RowDescriptorFieldName::ResultField { name } if name == "$createdAt"
    )));
    block_on(subscription.close()).unwrap();
}

#[test]
fn db_at_reads_historical_cut_and_partial_requires_server() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let partial = open_db(0xc1, author, &schema);
    let todo = row(0x42);

    core.insert_with_id("todos", todo, cells("draft", false, author))
        .unwrap();
    let first = core.node().borrow().committed_global_time();
    core.update(
        "todos",
        todo,
        BTreeMap::from([("title".to_owned(), Value::String("final".to_owned()))]),
    )
    .unwrap();
    let second = core.node().borrow().committed_global_time();

    let table = &schema.tables[0];
    let at_first = core.at(first, &Query::from("todos")).unwrap();
    assert_eq!(at_first.len(), 1);
    assert_eq!(
        at_first[0].cell(table, "title"),
        Some(Value::String("draft".to_owned()))
    );
    let at_second = core.at(second, &Query::from("todos")).unwrap();
    assert_eq!(
        at_second[0].cell(table, "title"),
        Some(Value::String("final".to_owned()))
    );

    let partial_todos = partial.prepare_query(&Query::from("todos")).unwrap();
    let err = partial.at(GlobalTime(1), &partial_todos).unwrap_err();
    assert_eq!(err.code, ErrorCode::HistoricalReadRequiresServer);
    assert_eq!(err.message, "historical read requires server evaluation");
}

#[test]
fn db_query_builder_expresses_s1_shaped_filters_and_include_modes() {
    let schema = issue_schema();
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x22; 16]),
            author: alice,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x22))),
    }))
    .unwrap();

    db.insert(
        "projects",
        BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(10)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "issues",
        issue_cells(
            "ship api query builder",
            "open",
            alice,
            row(10),
            5,
            &["api", "platform"],
            None,
        ),
        crate::db::InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "issues",
        issue_cells("closed work", "done", alice, row(10), 3, &["api"], Some(99)),
        crate::db::InsertOptions {
            row_id: Some(row(2)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "issues",
        issue_cells("someone else", "open", bob, row(10), 8, &["platform"], None),
        crate::db::InsertOptions {
            row_id: Some(row(3)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "issues",
        issue_cells("missing project", "open", alice, row(99), 6, &["api"], None),
        crate::db::InsertOptions {
            row_id: Some(row(4)),
            ..Default::default()
        },
    )
    .unwrap();

    let s1_query = db
        .table("issues")
        .filter(all_of([
            eq(col("assignee"), lit(alice.test_uuid())),
            in_list(col("state"), [lit("open"), lit("blocked")]),
            not(ne(col("state"), lit("open"))),
            any_of([
                contains(col("title"), lit("api")),
                contains(col("labels"), lit("api")),
            ]),
            gt(col("priority"), lit(4_u64)),
            lte(col("priority"), lit(6_u64)),
            is_null(col("snoozed_until")),
        ]))
        .include("project")
        .select([
            "title", "state", "assignee", "project", "priority", "labels",
        ])
        .limit(10)
        .offset(0);

    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "issues")
        .unwrap();
    let read_rows = prepared_read(&db, &s1_query);
    assert_eq!(row_ids(&read_rows), vec![row(1)]);
    assert_eq!(
        read_rows[0].cell(table, "title"),
        Some(Value::String("ship api query builder".to_owned()))
    );
    assert_eq!(read_rows[0].cell(table, "snoozed_until"), None);
    let all_rows = prepared_all(&db, &s1_query, ReadOpts::default());
    assert_eq!(row_ids(&all_rows), vec![row(1)]);

    let holes_query = db
        .table("issues")
        .filter(eq(col("assignee"), lit(alice.test_uuid())))
        .filter(eq(col("state"), lit("open")))
        .include_with(Include::new("project").join_mode(JoinMode::Holes));
    assert_eq!(
        row_ids(&prepared_read(&db, &holes_query)),
        vec![row(1), row(4)]
    );

    let require_query = holes_query.clone().include_with(
        Include::new("project")
            .join_mode(JoinMode::Holes)
            .require_includes(),
    );
    assert_eq!(row_ids(&prepared_read(&db, &require_query)), vec![row(1)]);
    assert_eq!(
        row_ids(&prepared_all(&db, &require_query, ReadOpts::default())),
        vec![row(1)],
        "required scalar includes must retain public Root membership gating"
    );

    let paged = db
        .table("issues")
        .filter(eq(col("state"), lit("open")))
        .include_with(Include::new("project").join_mode(JoinMode::Holes))
        .offset(1)
        .limit(1);
    assert_eq!(row_ids(&prepared_read(&db, &paged)), vec![row(3)]);
}

#[test]
fn payload_enum_match_filters_one_shot_and_maintained_case_transitions() {
    let schema = payload_enum_query_schema();
    let db = open_db(0xe7, AuthorSubject::for_test_bytes([0xe7; 16]), &schema);
    let matching = row(0xe1);
    let other_case = row(0xe2);
    db.insert(
        "events",
        BTreeMap::from([("event".to_owned(), payload_message(2))]),
        crate::db::InsertOptions {
            row_id: Some(matching),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "events",
        BTreeMap::from([("event".to_owned(), payload_closed(2))]),
        crate::db::InsertOptions {
            row_id: Some(other_case),
            ..Default::default()
        },
    )
    .unwrap();

    let query = Query::from("events").filter(Predicate::EnumMatch {
        column: "event".to_owned(),
        case: "message".to_owned(),
        payload: Box::new(Predicate::Eq(
            Operand::Column("level".to_owned()),
            Operand::Literal(Value::I32(2)),
        )),
    });
    assert_eq!(row_ids(&prepared_read(&db, &query)), vec![matching]);

    let prepared_query = prepared(&db, &query);
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&initial.rows), vec![matching]);

    db.update(
        "events",
        matching,
        BTreeMap::from([("event".to_owned(), payload_closed(2))]),
        Default::default(),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![matching]
    );
    assert!(db.read(&prepared_query).unwrap().is_empty());

    db.update(
        "events",
        other_case,
        BTreeMap::from([("event".to_owned(), payload_message(2))]),
        Default::default(),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![other_case]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(
        row_ids(&db.read(&prepared_query).unwrap()),
        vec![other_case]
    );
}

#[test]
fn client_read_advice_is_unknown_even_when_a_local_winner_exists() {
    let schema = owner_read_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let other = AuthorSubject::for_test_bytes([0xb2; 16]);
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let row = row(1);
    let write = core
        .insert_with_id("todos", row, cells("private", false, owner))
        .unwrap();

    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    owner_db
        .node
        .node
        .borrow_mut()
        .set_test_provider_claims(owner, test_provider_claims(owner));
    other_db
        .node
        .node
        .borrow_mut()
        .set_test_provider_claims(other, test_provider_claims(other));
    let unit = core
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .resolve();
    let SyncMessage::CommitUnit { tx, versions } = unit.unwrap() else {
        panic!("commit unit expected");
    };
    owner_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: versions.clone(),
        })
        .unwrap();
    other_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::CommitUnit { tx, versions })
        .unwrap();

    assert_eq!(
        owner_db.can_read("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        other_db.can_read("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        owner_db
            .authorize_read_for_identity("todos", row, owner)
            .unwrap(),
        PermissionAdvice::Allowed,
    );
    assert_eq!(
        owner_db
            .authorize_read_for_identity("todos", row, other)
            .unwrap(),
        PermissionAdvice::Denied,
    );
}

#[test]
fn permission_introspection_magic_columns_fail_closed_on_prepare_query() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();

    let query = db.table("todos").select(["$canRead"]);
    let error = expect_error(db.prepare_query(&query));
    assert_eq!(error.code, ErrorCode::Query);
    assert!(
        error.message.contains("unsupported")
            && error.message.contains("permission introspection")
            && error.message.contains("$canRead"),
        "unexpected error message: {}",
        error.message
    );

    let provenance_query = db.table("todos").select(["$createdAt", "$createdBy"]);
    db.prepare_query(&provenance_query).unwrap();
}

#[test]
fn read_opts_default_and_effective_tier_preserve_local_update_contract() {
    let opts = ReadOpts::default();
    assert_eq!(opts.tier, DurabilityTier::Local);
    assert_eq!(opts.local_updates, LocalUpdates::Immediate);
    assert_eq!(opts.propagation, Propagation::Full);

    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::None,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::Local
    );
    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::Global
    );
    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::None,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::None
    );
}

#[test]
fn edge_read_opts_and_wait_honor_edge_durability() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells("edge observed", false),
            Default::default(),
        )
        .unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    assert_eq!(
        effective_read_tier(&ReadOpts {
            tier: DurabilityTier::Edge,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        }),
        DurabilityTier::Edge
    );
    assert!(
        doctest_support::block_on(db.all_for_identity(
            &prepared_query,
            ReadOpts {
                tier: DurabilityTier::Edge,
                local_updates: LocalUpdates::Immediate,
                propagation: Propagation::LocalOnly,
                include_deleted: false,
                ..ReadOpts::default()
            },
            AuthorSubject::SYSTEM,
        ))
        .unwrap()
        .is_empty()
    );
    let not_observed = doctest_support::block_on(write.wait(DurabilityTier::Edge)).unwrap_err();
    assert_eq!(not_observed.code, ErrorCode::NotObserved);

    // E1: edge-accept produced directly; E2 wires the acceptance path.
    db.node
        .node
        .borrow_mut()
        .apply_fate_update(
            write.mergeable_tx_id(),
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();

    assert_eq!(
        doctest_support::block_on(write.wait(DurabilityTier::Edge)).unwrap(),
        write.mergeable_tx_id()
    );
    assert_eq!(
        row_ids(
            &doctest_support::block_on(db.all_for_identity(
                &prepared_query,
                ReadOpts {
                    tier: DurabilityTier::Edge,
                    local_updates: LocalUpdates::Immediate,
                    propagation: Propagation::LocalOnly,
                    include_deleted: false,
                    ..ReadOpts::default()
                },
                AuthorSubject::SYSTEM,
            ))
            .unwrap()
        ),
        vec![write.row_uuid()]
    );
}

#[test]
fn native_publication_finalizes_catalogue_ids_for_current_nested_grouped_and_joined_rows() {
    use crate::binding_codec::{
        RowDescriptorFieldName, encode_relation_snapshot, encode_rows, row_batches,
    };
    let schema = relation_schema();
    let db = open_db(0x91, AuthorSubject::for_test_bytes([0x91; 16]), &schema);
    for (table, id, fields) in [
        (
            "users",
            0xa1,
            BTreeMap::from([("name".to_owned(), Value::String("reader".to_owned()))]),
        ),
        (
            "todos",
            0x11,
            BTreeMap::from([
                ("title".to_owned(), Value::String("task".to_owned())),
                ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
            ]),
        ),
    ] {
        db.insert(
            table,
            fields,
            InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let current = prepared_all(&db, &Query::from("todos"), ReadOpts::default());
    let current_batches = row_batches(&current).expect("ordinary producer resolves physical IDs");
    let title_id = current_batches[0]
        .descriptor
        .iter()
        .find_map(|field| match field.name {
            RowDescriptorFieldName::StoredColumn {
                id,
                output_name: "title",
            } => Some(id),
            _ => None,
        })
        .expect("title has a stored binding");
    let grouped = prepared_all(
        &db,
        &Query::from("todos").count().group_by("title"),
        ReadOpts::default(),
    );
    let grouped_batches =
        row_batches(&grouped).expect("grouped source binding preserves catalogue ID");
    assert!(
        grouped_batches[0]
            .descriptor
            .iter()
            .any(|field| matches!(field.name,
        RowDescriptorFieldName::StoredColumn { id, output_name: "title" } if id == title_id))
    );
    assert!(current_batches[0].descriptor.iter().any(|field| matches!(
        field.name,
        RowDescriptorFieldName::HiddenMetadata { name: "tx_time" }
    )));
    assert!(current_batches[0].descriptor.iter().any(|field| matches!(
        field.name,
        RowDescriptorFieldName::ResultField { name: "$createdAt" }
    )));
    for query in [
        Query::from("todos").count().group_by("title"),
        Query::from("todos").select(["title", "$createdAt", "$updatedBy"]),
        Query::from("todos").aggregate([crate::query::Aggregate::count().alias("schema_version")]),
        Query::from("users").array_subquery(ArraySubquery::new("todos", "todos", "owner_id", "id")),
        Query::from("users").join_via_column("todos", "owner_id", "id", []),
    ] {
        let rows = prepared_all(&db, &query, ReadOpts::default());
        assert!(!rows.is_empty());
        assert!(
            !encode_rows(&rows)
                .expect("all returned rows have finalized native bindings")
                .is_empty()
        );
        if let Some(aggregate) = &query.aggregate {
            let batches = row_batches(&rows).unwrap();
            if aggregate.group_by.is_some() {
                assert!(batches[0].descriptor.iter().any(|field| matches!(field.name,
                    RowDescriptorFieldName::StoredColumn { id, output_name: "title" } if id == title_id)));
            }
            assert!(batches[0].descriptor.iter().any(|field| matches!(field.name,
                RowDescriptorFieldName::ResultField { name } if name == aggregate.aggregates[0].alias)));
        }
        if query.table == "todos" && query.aggregate.is_none() {
            let batches = row_batches(&rows).unwrap();
            for name in ["$createdAt", "$updatedBy"] {
                assert!(batches[0].descriptor.iter().any(|field| matches!(field.name, RowDescriptorFieldName::ResultField { name: published } if published == name)), "projected public provenance {name} remains visible");
            }
        }
        let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
        let snapshot = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
        assert!(!snapshot.rows.is_empty());
        if let Some(aggregate) = &query.aggregate {
            assert_eq!(snapshot.rows.len(), 1);
            assert_eq!(
                snapshot.rows[0].application_field(&aggregate.aggregates[0].alias),
                Some(Value::U64(1)),
                "aggregate reset preserves the computed count",
            );
            if aggregate.group_by.is_some() {
                assert!(snapshot.rows[0].application_field("title").is_some());
                assert_eq!(
                    snapshot.rows[0].application_field("title"),
                    Some(Value::String("task".to_owned()))
                );
            }
        }

        assert!(
            !encode_relation_snapshot(&snapshot)
                .expect("reset producer retains finalized bindings")
                .is_empty()
        );
    }
}

/// Alice's admitted team-A subscription keeps its claims when a live catalogue
/// token invalidates the maintained plan, while the same subject's ambient
/// claims say team B. This internal test uses the existing token-invalidation
/// hook because that lifecycle boundary has no small public trigger.
///
/// alice/A subscribe ──token invalidation──► rebuild ──new A row──► alice/A
#[test]
fn request_claims_survive_subscription_runtime_rebuild() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_session_eq("title", &["claims", "team"])),
                ),
        ),
    );
    let db = open_db(0x6a, AuthorSubject::SYSTEM, &schema);
    let a = block_on(db.insert(
        "todos",
        [("title".into(), Value::String("team-a".into()))].into(),
        Default::default(),
    ))
    .unwrap()
    .row_uuid();
    let b = block_on(db.insert(
        "todos",
        [("title".into(), Value::String("team-b".into()))].into(),
        Default::default(),
    ))
    .unwrap()
    .row_uuid();
    let alice = AuthorSubject::for_test_bytes([0x6b; 16]);
    let claims = |team: &str| {
        [(
            crate::query::provider_claim_key("team"),
            Value::String(team.into()),
        )]
        .into()
    };
    let prepared = block_on(db.prepare_query_async(&db.table("todos"))).unwrap();
    let scoped = prepared
        .clone()
        .with_identity_claims(alice, claims("team-a"));
    let opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    };
    db.set_identity_claims(alice, claims("team-b"));
    let mut subscription =
        block_on(db.subscribe_for_identity(&scoped, opts.clone(), alice)).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_event()).unwrap()
    else {
        panic!("expected initial rows")
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![a]
    );
    let scoped_b = prepared
        .clone()
        .with_identity_claims(alice, claims("team-b"));
    let mut subscription_b =
        block_on(db.subscribe_for_identity(&scoped_b, opts.clone(), alice)).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription_b.next_event()).unwrap()
    else {
        panic!("expected team-B initial rows")
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![b]
    );
    db.node
        .node
        .borrow_mut()
        .invalidate_groove_runtime_for_test();
    block_on(db.refresh_subscriptions()).unwrap();
    let new_a = block_on(db.insert(
        "todos",
        [("title".into(), Value::String("team-a".into()))].into(),
        Default::default(),
    ))
    .unwrap()
    .row_uuid();
    block_on(db.refresh_subscriptions()).unwrap();
    let mut saw_new_a = false;
    while let Some(event) = subscription.try_next_event() {
        if let SubscriptionEvent::Delta { added, .. } = event {
            assert!(
                !added
                    .iter()
                    .map(|row| row.row_uuid())
                    .collect::<Vec<_>>()
                    .contains(&b),
                "rebuild must never substitute ambient team-B claims"
            );
            saw_new_a |= added
                .iter()
                .map(|row| row.row_uuid())
                .collect::<Vec<_>>()
                .contains(&new_a);
        }
    }
    assert!(
        saw_new_a,
        "rebuilt subscription still receives its admitted team's updates"
    );
    assert_eq!(
        row_ids(&block_on(db.all_for_identity(&prepared, opts, alice)).unwrap()),
        vec![b],
        "request scope restores ambient claims"
    );
    while let Some(event) = subscription_b.try_next_event() {
        if let SubscriptionEvent::Delta { added, .. } = event {
            assert!(
                added
                    .iter()
                    .all(|row| row.row_uuid() != a && row.row_uuid() != new_a),
                "team-B request must not reuse team-A's prepared plan"
            );
        }
    }
    block_on(subscription_b.close()).unwrap();
    block_on(subscription.close()).unwrap();
}

/// Exercise the public Rust prepared-handle API: a missing provider claim
/// removes one OR branch, but must not poison a later request by the same author.
#[test]
fn prepared_request_claim_presence_keeps_policy_branches_isolated() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("rooms")
                .column("owner", PublicColumnType::Uuid)
                .column("joinCode", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new().with_select(PublicPolicyExpr::Or(vec![
                        PublicPolicyExpr::eq_session(
                            "owner",
                            vec!["user".into(), "account".into()],
                        ),
                        PublicPolicyExpr::eq_session(
                            "joinCode",
                            vec!["claims".into(), "invite".into()],
                        ),
                    ])),
                ),
        ),
    );
    let db = open_db(0x6c, AuthorSubject::SYSTEM, &schema);
    let room = block_on(
        db.insert(
            "rooms",
            [
                ("owner".into(), Value::Uuid(row(0x6e).0)),
                ("joinCode".into(), Value::String("invite-a".into())),
            ]
            .into(),
            Default::default(),
        ),
    )
    .unwrap()
    .row_uuid();
    let author = AuthorSubject::for_test_bytes([0x6d; 16])
        .with_account(crate::account_registry::AccountId(row(0x6d).0));
    let prepared = block_on(db.prepare_query_async(&db.table("rooms"))).unwrap();
    let absent = prepared
        .clone()
        .with_identity_claims(author, BTreeMap::new());
    let present = prepared.with_identity_claims(
        author,
        [(
            crate::query::provider_claim_key("invite"),
            Value::String("invite-a".into()),
        )]
        .into(),
    );
    let opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..Default::default()
    };
    assert!(
        block_on(db.all_for_identity(&absent, opts.clone(), author))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        row_ids(&block_on(db.all_for_identity(&present, opts.clone(), author)).unwrap()),
        vec![room]
    );
    let mut denied = block_on(db.subscribe_for_identity(&absent, opts.clone(), author)).unwrap();
    let mut admitted = block_on(db.subscribe_for_identity(&present, opts, author)).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(denied.next_event()).unwrap() else {
        panic!("initial denied event")
    };
    assert!(added.is_empty());
    let SubscriptionEvent::Delta { added, .. } = block_on(admitted.next_event()).unwrap() else {
        panic!("initial admitted event")
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![room]
    );
    block_on(denied.close()).unwrap();
    block_on(admitted.close()).unwrap();
}

/// Alice requires an optional detail and its optional leaf. Required filtering
/// must preserve nullable parent cells in both empty and populated collectors.
#[test]
fn required_nested_nullable_includes_preserve_parent_descriptors() {
    use crate::query::ArraySubqueryRequirement;
    for requirement in [
        ArraySubqueryRequirement::AtLeastOne,
        ArraySubqueryRequirement::MatchCorrelationCardinality,
    ] {
        let schema = build_public_db_test_schema(
            PublicSchemaBuilder::new()
                .table(
                    PublicTableSchemaBuilder::new("roots")
                        .nullable_fk_column("detailId", "details"),
                )
                .table(
                    PublicTableSchemaBuilder::new("details").nullable_fk_column("leafId", "leaves"),
                )
                .table(
                    PublicTableSchemaBuilder::new("leaves").column("name", PublicColumnType::Text),
                ),
        );
        let db = open_db(0xd7, AuthorSubject::SYSTEM, &schema);
        let query = Query::from("roots").array_subquery(
            ArraySubquery::new("detail", "details", "id", "detailId")
                .requirement(requirement)
                .nested(
                    ArraySubquery::new("leaf", "leaves", "id", "leafId").requirement(requirement),
                ),
        );
        let prepared = db.prepare_query(&query).unwrap();
        let empty = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
        assert!(empty.rows.is_empty());
        let leaf = db
            .insert(
                "leaves",
                BTreeMap::from([("name".into(), Value::String("Alice".into()))]),
                Default::default(),
            )
            .unwrap()
            .row_uuid();
        let optional_ref =
            |id: Option<RowUuid>| Value::Nullable(id.map(|id| Box::new(Value::Uuid(id.0))));
        let detail = db
            .insert(
                "details",
                BTreeMap::from([("leafId".into(), optional_ref(Some(leaf)))]),
                Default::default(),
            )
            .unwrap()
            .row_uuid();
        let root = db
            .insert(
                "roots",
                BTreeMap::from([("detailId".into(), optional_ref(Some(detail)))]),
                Default::default(),
            )
            .unwrap()
            .row_uuid();
        db.insert(
            "roots",
            BTreeMap::from([("detailId".into(), optional_ref(None))]),
            Default::default(),
        )
        .unwrap();
        let snapshot = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
        assert_eq!(row_ids(&snapshot.rows), vec![root]);
        let root_table = schema
            .tables
            .iter()
            .find(|table| table.name == "roots")
            .unwrap();
        assert_eq!(
            snapshot.rows[0].cell(root_table, "detailId"),
            Some(optional_ref(Some(detail)))
        );
        assert_eq!(
            terminal_nested_values(&snapshot, root, "detail", "row_uuid"),
            vec![Value::Uuid(detail.0)]
        );
        db.update(
            "details",
            detail,
            BTreeMap::from([("leafId".into(), optional_ref(None))]),
            Default::default(),
        )
        .unwrap();
        let removed = block_on(db.all_relation_snapshot(&prepared, ReadOpts::default())).unwrap();
        assert!(removed.rows.is_empty());
    }
}
