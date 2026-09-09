//! Local opening, synchronous delivery, scheduling, and upstream registration.

use super::*;

#[test]
fn db_facade_local_subscription_reports_initial_and_changed_results() {
    let schema = doctest_support::schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let db = doctest_support::block_on(Db::open_history_complete(DbConfig {
        schema,
        storage: doctest_support::MemoryStorage::new(&refs).expect("valid memory storage families"),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x1111))),
    }))
    .unwrap();
    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];
    let prepared_query = prepared(&db, &query);
    let mut subscription = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();

    assert!(opened_rows(doctest_support::block_on(subscription.next_raw()).unwrap()).is_empty());

    let todo = RowUuid::from_bytes([0x44; 16]);
    db.seed_settled_mergeable_for_bootstrap(
        "todos",
        todo,
        db.identity.author,
        doctest_support::todo_cells("subscription makes a todo appear", true),
    )
    .unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_raw()).unwrap());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(row_ids(&added), vec![todo]);
    assert_eq!(
        added[0].cell(table, "title"),
        Some(Value::String("subscription makes a todo appear".to_owned()))
    );
    assert_eq!(added[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn db_facade_subscription_refresh_preserves_read_tier() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let mut subscription = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();

    assert!(
        subscription.try_next_event().is_none(),
        "Global waits for authority coverage even when the local result is empty"
    );

    db.insert(
        "todos",
        doctest_support::todo_cells("pending local-only write", true),
        Default::default(),
    )
    .unwrap();

    assert_eq!(prepared_read(&db, &query).len(), 1);
    assert!(
        subscription.try_next_event().is_none(),
        "a local write does not settle a Global subscription"
    );
}

#[test]
fn db_facade_subscription_accepts_local_tier_for_alpha_style_live_reads() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
    let opened = doctest_support::block_on(subscription.next_raw()).unwrap();
    assert_eq!(opened_rows(opened), Vec::<CurrentRow>::new());

    db.insert(
        "todos",
        doctest_support::todo_cells("local callback", false),
        Default::default(),
    )
    .unwrap();
    let changed = doctest_support::block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta { added, tier, .. } = changed else {
        panic!("expected local subscription delta");
    };
    assert_eq!(tier, DurabilityTier::Local);
    assert_eq!(added.len(), 1);
    assert_eq!(scheduler.take(), vec![TickUrgency::Deferred]);
}

#[test]
fn local_write_is_readable_synchronously_without_running_tick() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    db.insert(
        "todos",
        doctest_support::todo_cells("read before tick", false),
        Default::default(),
    )
    .unwrap();

    let rows = db.read(&prepared_query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(scheduler.take(), vec![TickUrgency::Deferred]);
}

#[test]
fn local_write_notifies_subscription_synchronously_without_running_tick() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let mut subscription =
        doctest_support::block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
    assert!(opened_rows(doctest_support::block_on(subscription.next_raw()).unwrap()).is_empty());

    db.insert(
        "todos",
        doctest_support::todo_cells("notify before tick", false),
        Default::default(),
    )
    .unwrap();

    let (added, updated, removed) =
        delta_rows(doctest_support::block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(scheduler.take(), vec![TickUrgency::Deferred]);
}

#[test]
fn db_facade_schedules_immediate_tick_for_attached_query_coverage() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    db.attach_query_with_opts(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::Full,
            include_deleted: false,
            ..ReadOpts::default()
        },
    )
    .unwrap();

    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
}

#[test]
fn db_facade_local_only_subscription_does_not_register_upstream_coverage() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    let mut subscription = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();

    assert!(opened_rows(doctest_support::block_on(subscription.next_raw()).unwrap()).is_empty());
    assert_eq!(scheduler.take(), Vec::<TickUrgency>::new());
    assert!(db.node.upstream_subscriptions.borrow().is_empty());
}

#[test]
fn propagated_subscriptions_refcount_upstream_coverage_by_shape() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let baseline = db.runtime_stats_for_test().active_subscriptions;
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);
    let opts = ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    };

    let mut first = doctest_support::block_on(db.subscribe(&prepared_query, opts.clone())).unwrap();
    let _ = doctest_support::block_on(first.next_raw()).unwrap();
    assert_eq!(pending_upstream_subscribe_count(&db), 1);

    let mut second = doctest_support::block_on(db.subscribe(&prepared_query, opts)).unwrap();
    let _ = doctest_support::block_on(second.next_raw()).unwrap();
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 2
    );
    assert_eq!(
        pending_upstream_subscribe_count(&db),
        1,
        "second propagating registrant should share upstream coverage"
    );

    drop(first);
    doctest_support::block_on(db.tick()).unwrap();
    assert_eq!(
        db.runtime_stats_for_test().active_subscriptions,
        baseline + 1,
        "dropping one propagated stream must release only its local Groove output"
    );
    assert_eq!(
        pending_upstream_unsubscribe_count(&db),
        0,
        "upstream coverage stays live while another propagating registrant remains"
    );

    drop(second);
    doctest_support::block_on(db.tick()).unwrap();
    assert_eq!(db.runtime_stats_for_test().active_subscriptions, baseline);
    assert_eq!(pending_upstream_unsubscribe_count(&db), 1);
}

#[test]
fn local_only_subscription_is_not_forwarded_on_late_upstream_connect() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&db, &query);

    let mut inspector = doctest_support::block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    let _ = doctest_support::block_on(inspector.next_raw()).unwrap();

    let (client_transport, _server_transport) = duplex();
    let upstream = crate::db::block_on(db.connect_upstream(client_transport));
    let pending_subscribes = match &upstream.borrow().link {
        ConnectionLink::Upstream(UpstreamConnectionState { pending, .. }) => pending
            .iter()
            .filter(|command| matches!(command, PendingUpstreamCommand::Subscribe(_)))
            .count(),
        _ => unreachable!("connect_upstream creates upstream links"),
    };
    assert_eq!(pending_subscribes, 0);
}

#[test]
fn db_facade_schedules_immediate_tick_for_upstream_connection() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    let (client_transport, _server_transport) = duplex();

    let _upstream = crate::db::block_on(db.connect_upstream(client_transport));

    assert_eq!(scheduler.take(), vec![TickUrgency::Immediate]);
}

#[test]
fn upstream_inbound_application_completes_synchronously_or_schedules_continuation() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x51, author, &schema);
    let client = open_db(0x52, author, &schema);
    let scheduler = Rc::new(RecordingScheduler::default());
    client.set_tick_scheduler(Some(scheduler.clone()));
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);
    scheduler.take();

    let query = client.table("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    scheduler.take();

    client.tick().unwrap();
    assert!(scheduler.take().is_empty());
    let mut scheduled = Vec::new();
    let mut delivered = Vec::new();
    for _ in 0..32 {
        server.tick().unwrap();
        client.tick().unwrap();
        scheduled.extend(scheduler.take());
        delivered.extend(std::iter::from_fn(|| subscription.try_next_event()));
        if !scheduled.is_empty() || !delivered.is_empty() {
            break;
        }
    }
    assert!(
        !delivered.is_empty() || scheduled == vec![TickUrgency::Immediate],
        "inbound application must either publish resident output in this turn or schedule its continuation"
    );
}
