//! Shared node scheduling, dirty-generation cascades, and connection servicing tests.

use super::*;
use groove::storage::{TestStorage, TestStorageOperation};

#[test]
fn reopened_wait_observer_yields_instead_of_sync_polling_cold_storage() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc9; 16]);
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let (storage, _) = TestStorage::controlled(&column_family_refs);
    let reopen_handle = storage.clone();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0xc9; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xc9))),
    }))
    .unwrap();
    let tx_id = db
        .insert(
            "todos",
            cells("cold reopened wait", false, author),
            Default::default(),
        )
        .unwrap()
        .mergeable_tx_id();
    block_on(db.close()).unwrap();
    drop(db);

    let reopened_storage = block_on(reopen_handle.reopen(column_families)).unwrap();
    let control = reopened_storage.control();
    let eviction_handle = reopened_storage.clone();
    let reopened = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage: reopened_storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0xc9; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xca))),
    }))
    .unwrap();
    eviction_handle.evict_all();
    control.pause_on(TestStorageOperation::ScanOpen);
    control.pause_on(TestStorageOperation::Get);

    let observed = Rc::new(RefCell::new(None));
    let callback_observed = Rc::clone(&observed);
    reopened.wait_for_transaction_with(
        tx_id,
        DurabilityTier::Local,
        move |result: Result<TxId, Error>| {
            *callback_observed.borrow_mut() = Some(result);
        },
    );
    reopened.node.poll_transaction_wait_observers();
    assert!(
        observed.borrow().is_none(),
        "a cold wait observation must yield to the owner instead of completing from reservation or synchronously polling storage"
    );

    control.resume();
    for _ in 0..4 {
        reopened.node.poll_transaction_wait_observers();
        if observed.borrow().is_some() {
            break;
        }
    }
    assert_eq!(observed.borrow_mut().take().unwrap().unwrap(), tx_id);
}

/// A failed durable acknowledgement remains retained, but a poisoned database
/// must surface the terminal tick instead of hot-looping a scheduler forever.
#[test]
fn deferred_rejection_acknowledgement_failure_requires_explicit_reopen_without_hot_loop() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xca; 16]);
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&refs);
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0xca; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xca))),
    }))
    .expect("open controlled rejection fixture");
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = block_on(db.connect_upstream(client_transport));
    let write = db
        .insert(
            "todos",
            cells("retry deferred acknowledgement", false, author),
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
        .expect("authority fate reaches fixture");
    db.tick().expect("persist authority rejection");

    let outcome = Rc::new(RefCell::new(None));
    let callback = Rc::clone(&outcome);
    db.wait_for_transaction_with(tx_id, DurabilityTier::Edge, move |result| {
        *callback.borrow_mut() = Some(result);
    });
    let scheduler = Rc::new(RecordingScheduler::default());
    db.set_tick_scheduler(Some(scheduler.clone()));
    control.take_observed();
    scheduler.take();
    control.fail_next(TestStorageOperation::WriteMany);
    let error = db
        .tick()
        .expect_err("failed acknowledgement surfaces through the owner tick");
    assert!(
        error.to_string().contains("injected WriteMany failure"),
        "the original acknowledgement failure is not hidden"
    );
    assert_eq!(
        outcome
            .borrow_mut()
            .take()
            .expect("waiter observes rejection")
            .expect_err("rejected write")
            .code,
        ErrorCode::WriteRejected
    );
    assert!(
        db.node
            .node()
            .borrow()
            .rejected_transaction(tx_id)
            .is_some(),
        "failed acknowledgement remains retained until a reopened database can acknowledge it"
    );

    let first_attempts = control
        .take_observed()
        .iter()
        .filter(|operation| **operation == TestStorageOperation::WriteMany)
        .count();
    assert!(
        first_attempts > 0,
        "the first owner turn attempted the durable acknowledgement"
    );
    assert_eq!(
        scheduler.take(),
        vec![TickUrgency::Immediate],
        "the only wake is the waiter completion scheduled before its acknowledgement fails"
    );
    let error = db
        .tick()
        .expect_err("a poisoned database remains terminal until reopened");
    assert!(
        error.to_string().contains("poisoned"),
        "the later explicit tick makes the reopen requirement visible"
    );
    assert!(
        scheduler.take().is_empty(),
        "a permanently poisoned acknowledgement cannot create an owner-turn hot loop"
    );
    assert!(
        db.node
            .node()
            .borrow()
            .rejected_transaction(tx_id)
            .is_some(),
        "an indeterminate atomic-write failure never loses the rejected transaction"
    );
}

/// Close must fail before writing its clean-close marker when a drained
/// waiter cannot durably acknowledge a rejection.
#[test]
fn close_fails_before_clean_marker_when_rejection_acknowledgement_fails() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xcb; 16]);
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&refs);
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0xcb; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xcb))),
    }))
    .expect("open controlled close fixture");
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = block_on(db.connect_upstream(client_transport));
    let write = db
        .insert(
            "todos",
            cells("close failed acknowledgement", false, author),
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
        .expect("authority fate reaches fixture");
    db.tick().expect("persist authority rejection");

    let outcome = Rc::new(RefCell::new(None));
    let callback = Rc::clone(&outcome);
    db.wait_for_transaction_with(tx_id, DurabilityTier::Edge, move |result| {
        *callback.borrow_mut() = Some(result);
    });
    control.take_observed();
    control.fail_next(TestStorageOperation::WriteMany);
    let error = block_on(db.close()).expect_err("close propagates acknowledgement failure");
    assert!(
        error.to_string().contains("injected WriteMany failure"),
        "close exposes the durable acknowledgement failure"
    );
    assert_eq!(
        outcome
            .borrow_mut()
            .take()
            .expect("close drains the waiter")
            .expect_err("drained waiter observes rejection")
            .code,
        ErrorCode::WriteRejected
    );
    assert!(
        db.node
            .node()
            .borrow()
            .rejected_transaction(tx_id)
            .is_some(),
        "failed close retains the rejection for an explicit retry"
    );
    assert!(
        !control
            .take_observed()
            .contains(&TestStorageOperation::Close),
        "failed acknowledgement prevents storage close and its clean-close marker"
    );
}

#[test]
fn large_write_pushes_staging_before_syncing_its_referencing_row() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let core = open_core(0xc0, AuthorSubject::SYSTEM, &schema);
    let writer = open_db(0xc1, author, &schema);
    let (writer_transport, core_transport) = duplex();
    let _upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let title = "push-before-row/".repeat(8_000);
    writer
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.clone())),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();

    for _ in 0..16 {
        writer.tick().unwrap();
        core.tick().unwrap();
        if !core.read(&core.table("todos")).unwrap().is_empty() {
            break;
        }
    }
    let rows = core.read(&core.table("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell_at(0), Some(Value::String(title)));
}

/// Internal topology canary: exact push-before-row ordering on both relay legs
/// and pull forwarding after edge chunk eviction are protocol/runtime
/// properties that are not observable through the public client API alone.
/// The accepted write and reconstructed value are still asserted through that
/// API. Every node is opened with its own storage directory.
#[test]
fn large_value_pushes_through_edge_then_pulls_from_core_after_edge_chunk_eviction() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc4; 16]);
    let core = open_core(0xc5, AuthorSubject::SYSTEM, &schema);
    let upload_edge = open_db(0xc6, AuthorSubject::SYSTEM, &schema);
    let writer = open_db(0xc7, author, &schema);

    let (upload_edge_transport, core_upload_transport, upload_edge_to_core) =
        duplex_with_client_outbound_tap();
    let _upload_edge_upstream =
        crate::db::block_on(upload_edge.connect_upstream(upload_edge_transport));
    let _core_upload_edge = core.accept_subscriber_with_trust(
        core_upload_transport,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let (writer_transport, upload_edge_client_transport, writer_to_upload_edge) =
        duplex_with_client_outbound_tap();
    let _writer_upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _upload_edge_writer = upload_edge.accept_subscriber(upload_edge_client_transport, author);

    let title = "multi-hop-large-value/".repeat(8_000);
    let write = writer
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.clone())),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();

    let mut writer_messages = Vec::new();
    let mut upload_edge_messages = Vec::new();
    for _ in 0..64 {
        writer.tick().unwrap();
        writer_messages.extend(writer_to_upload_edge.borrow().iter().cloned());
        upload_edge.tick().unwrap();
        upload_edge_messages.extend(upload_edge_to_core.borrow().iter().cloned());
        core.tick().unwrap();
        upload_edge.tick().unwrap();
        writer.tick().unwrap();
        if writer.write_state(write.tx_id).unwrap().durability == DurabilityTier::Global {
            break;
        }
    }
    assert_eq!(
        writer.write_state(write.tx_id).unwrap().durability,
        DurabilityTier::Global
    );
    assert_eq!(
        core.read(&core.table("todos")).unwrap()[0].cell_at(0),
        Some(Value::String(title.clone()))
    );

    for (leg, messages) in [
        ("writer-to-upload-edge", writer_messages),
        ("upload-edge-to-core", upload_edge_messages),
    ] {
        let staged = messages
            .iter()
            .rposition(|message| matches!(message, SyncMessage::ChunkUploadNodes(_)))
            .unwrap_or_else(|| panic!("{leg} sends receiver-requested chunk nodes"));
        let row = messages
            .iter()
            .position(|message| {
                matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.tx_id)
            })
            .unwrap_or_else(|| panic!("{leg} sends the referencing row"));
        assert!(staged < row, "{leg} stages the chunks before the row");
    }
    assert_eq!(
        prepared_read(&upload_edge, &upload_edge.table("todos")).len(),
        1,
        "the upload edge retained the accepted row"
    );

    // Retain the accepted row and its disclosed locator, but replace only the
    // edge's Groove chunk backend with an empty independent store. Its only
    // route to the value bytes is now to forward this edge-local access to Core.
    upload_edge
        .node
        .node
        .borrow_mut()
        .set_chunk_storage(Rc::new(groove::chunks::MemoryChunkStorage::new()));
    let query = upload_edge.table("todos");
    let mut subscription = prepared_subscribe(
        &upload_edge,
        &query,
        ReadOpts {
            tier: DurabilityTier::Local,
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
    )
    .unwrap();
    let mut received = None;
    let mut snapshot = RelationSnapshot::default();
    let mut pull_messages = Vec::new();
    let mut pending_event = None;
    for _ in 0..128 {
        upload_edge.tick().unwrap();
        pull_messages.extend(upload_edge_to_core.borrow().iter().cloned());
        core.tick().unwrap();
        upload_edge.tick().unwrap();
        if pending_event.is_none() {
            pending_event = subscription.try_next_event();
        }
        if pull_messages
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkRequestBatch(_)))
            && let Some(event) = pending_event.as_mut()
        {
            crate::db::block_on(upload_edge.hydrate_subscription_event_for_binding(event)).unwrap();
            apply_subscription_event(&mut snapshot, pending_event.take().unwrap());
        }
        received = snapshot.rows.first().and_then(|row| row.cell_at(0));
        if received == Some(Value::String(title.clone())) {
            break;
        }
    }
    assert_eq!(
        snapshot.rows.len(),
        1,
        "the empty edge delivers the referencing row",
    );
    assert!(
        pull_messages
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkRequestBatch(_))),
        "the empty edge requests missing chunks from Core"
    );
    assert_eq!(
        received,
        Some(Value::String(title)),
        "the empty edge forwards the missing chunk pull to Core"
    );
}

#[derive(Clone)]
struct PausedUploadRetryClock(Rc<Cell<u64>>);

impl UploadRetryClock for PausedUploadRetryClock {
    fn now_ms(&self) -> u64 {
        self.0.get()
    }
}

/// Internal transport test: the public write outcome is asserted below, but
/// the exact-batch retry and no-early-resend properties sit below the public
/// API at the peer protocol boundary.
#[test]
fn rate_limited_push_waits_then_retries_the_exact_batch_without_rejecting_the_write() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let core = open_core(0xc3, AuthorSubject::SYSTEM, &schema);
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy {
            incoming_bytes_per_window: crate::node::LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES
                + 1,
            window_ms: 60_000,
            max_age_ms: 10 * 60 * 1_000,
        });
    let writer = open_db(0xc2, author, &schema);
    let clock = Rc::new(Cell::new(10_000));
    writer
        .node
        .set_upload_retry_clock_for_test(Rc::new(PausedUploadRetryClock(Rc::clone(&clock))));
    let scheduler = Rc::new(RecordingScheduler::default());
    writer.set_tick_scheduler(Some(scheduler.clone()));
    let writer_node = NodeUuid::from_bytes([0xc2; 16]);
    let core_node = NodeUuid::from_bytes([0xc3; 16]);
    let (writer_transport, core_transport, writer_outbound) =
        duplex_with_admitted_session_context_and_client_outbound_tap(
            author,
            writer_node,
            1,
            core_node,
            1,
        );
    let upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let write = writer
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("rate-limited/".repeat(8_000)),
                ),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();

    // Start, receive the requested frontier, then send the first batch that
    // Core rate-limits. Capture it before the Core transport drains it.
    writer.tick().unwrap();
    core.tick().unwrap();
    scheduler.take();
    writer.tick().unwrap();
    assert_eq!(
        scheduler.take(),
        vec![TickUrgency::Immediate, TickUrgency::AfterCurrentTurn],
        "the requested upload frontier preserves its exact coalesced scheduler-owned writer wake"
    );
    writer.tick().unwrap();
    let first_batch = writer_outbound
        .borrow()
        .iter()
        .find_map(|message| match message {
            SyncMessage::ChunkUploadNodes(batch) => Some(batch.clone()),
            _ => None,
        })
        .expect("writer sends the requested chunk batch");
    core.tick().unwrap();
    writer.tick().unwrap();

    assert_eq!(
        scheduler.take_delays(),
        vec![1_000],
        "RateLimited schedules the bounded admission deadline rather than a deferred hot loop"
    );
    assert!(
        !matches!(
            writer.write_state(write.tx_id).unwrap().fate,
            Fate::Rejected(_)
        ),
        "a rate-limited batch remains resumable"
    );

    // Reconnect before the deadline. The old transport's queue-local upload
    // state must transfer only to this same logical destination, while the
    // node-scoped deadline also gates a fresh Start on any replacement link.
    assert!(writer.detach_connection(&upstream));
    let (reconnected_transport, reconnected_core_transport, reconnected_outbound) =
        duplex_with_admitted_session_context_and_client_outbound_tap(
            author,
            writer_node,
            2,
            core_node,
            2,
        );
    let _reconnected_upstream = crate::db::block_on(writer.connect_upstream(reconnected_transport));
    let _reconnected_subscriber = core.accept_subscriber(reconnected_core_transport, author);

    // An unrelated immediate/manual host tick before the deadline must not
    // resend the batch. The paused clock makes this deterministic.
    for _ in 0..3 {
        writer.tick().unwrap();
        assert!(
            reconnected_outbound.borrow().is_empty(),
            "reconnect sends neither Start nor chunk nodes before the admission deadline"
        );
    }

    // The receiver becomes admissible before the scheduled retry, then the
    // fake clock advances exactly to that deadline. The retry is byte-for-byte
    // the same requested batch, not a restarted upload or a new row write.
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy::default());
    clock.set(11_000);
    writer.tick().unwrap();
    assert!(
        !reconnected_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadStart(_))),
        "same-destination reconnect resumes the retained frontier instead of restarting upload"
    );
    let retry_batch = reconnected_outbound
        .borrow()
        .iter()
        .find_map(|message| match message {
            SyncMessage::ChunkUploadNodes(batch) => Some(batch.clone()),
            _ => None,
        })
        .expect("the deadline permits the retained batch to retry");
    assert_eq!(
        retry_batch, first_batch,
        "retry retains the exact failed batch"
    );

    for _ in 0..128 {
        core.tick().unwrap();
        writer.tick().unwrap();
        if writer.write_state(write.tx_id).unwrap().durability == DurabilityTier::Global {
            break;
        }
    }
    assert_eq!(
        writer.write_state(write.tx_id).unwrap().durability,
        DurabilityTier::Global,
        "the delayed retry eventually publishes the original write"
    );
    assert_eq!(core.read(&core.table("todos")).unwrap().len(), 1);
}

/// Unauthenticated links retain the bounded admission deadline but never a
/// receiver-specific frontier; expiry still independently reclaims staging.
#[test]
fn unauthenticated_reconnect_restarts_after_deadline_and_does_not_prevent_ttl_cleanup() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc8; 16]);
    let core = open_core(0xc9, AuthorSubject::SYSTEM, &schema);
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy {
            incoming_bytes_per_window: crate::node::LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES
                + 1,
            window_ms: 60_000,
            max_age_ms: 10 * 60 * 1_000,
        });
    let writer = open_db(0xc8, author, &schema);
    let clock = Rc::new(Cell::new(20_000));
    writer
        .node
        .set_upload_retry_clock_for_test(Rc::new(PausedUploadRetryClock(Rc::clone(&clock))));
    let scheduler = Rc::new(RecordingScheduler::default());
    writer.set_tick_scheduler(Some(scheduler.clone()));
    let (writer_transport, core_transport, writer_outbound) = duplex_with_client_outbound_tap();
    let upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let write = writer
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("expired-rate-limited/".repeat(8_000)),
                ),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();

    writer.tick().unwrap();
    core.tick().unwrap();
    scheduler.take();
    writer.tick().unwrap();
    assert_eq!(
        scheduler.take(),
        vec![TickUrgency::Immediate, TickUrgency::AfterCurrentTurn],
        "the requested upload frontier preserves its exact coalesced scheduler-owned writer wake"
    );
    writer.tick().unwrap();
    assert!(
        writer_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadNodes(_)))
    );
    assert!(
        !writer_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::CommitUnit { .. })),
        "the initial row commit remains behind the rate-limited upload"
    );
    core.tick().unwrap();
    writer.tick().unwrap();
    assert_eq!(scheduler.take_delays(), vec![1_000]);

    assert!(writer.detach_connection(&upstream));
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy {
            incoming_bytes_per_window: u64::MAX,
            window_ms: 60_000,
            max_age_ms: 0,
        });
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        crate::db::block_on(core.server.evict_expired_staged_large_values()).unwrap(),
        1,
        "the abandoned receiver-side staging claim expires"
    );

    assert!(
        writer.node.detached_large_value_uploads.borrow().is_empty(),
        "a context-free link never retains another receiver's missing-node frontier"
    );
    assert!(
        writer
            .node
            .large_value_upload_retry_deadlines
            .borrow()
            .contains_key(&write.tx_id),
        "the sender retains only the bounded admission deadline"
    );

    let (reconnected_transport, reconnected_core_transport, reconnected_outbound) =
        duplex_with_client_outbound_tap();
    let _reconnected_upstream = crate::db::block_on(writer.connect_upstream(reconnected_transport));
    let _reconnected_subscriber = core.accept_subscriber(reconnected_core_transport, author);
    writer.tick().unwrap();
    assert!(
        reconnected_outbound.borrow().is_empty(),
        "an unauthenticated reconnect remains gated before the deadline"
    );
    clock.set(21_000);
    writer.tick().unwrap();
    assert!(
        reconnected_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadStart(_))),
        "after the deadline an unauthenticated reconnect starts a fresh handshake"
    );
    assert!(
        !reconnected_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadNodes(_))),
        "an unauthenticated reconnect never replays the previous receiver frontier"
    );
}

fn assert_different_authenticated_destination_restarts_upload(
    reconnect_remote_node: NodeUuid,
    reconnect_link_identity: AuthorSubject,
) {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xd2; 16]);
    let writer_node = NodeUuid::from_bytes([0xd2; 16]);
    let core_node = NodeUuid::from_bytes([0xd3; 16]);
    let core = open_core(0xd3, AuthorSubject::SYSTEM, &schema);
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy {
            incoming_bytes_per_window: crate::node::LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES
                + 1,
            window_ms: 60_000,
            max_age_ms: 10 * 60 * 1_000,
        });
    let writer = open_db(0xd2, author, &schema);
    let clock = Rc::new(Cell::new(30_000));
    writer
        .node
        .set_upload_retry_clock_for_test(Rc::new(PausedUploadRetryClock(Rc::clone(&clock))));
    let (writer_transport, core_transport, _writer_outbound) =
        duplex_with_admitted_session_context_and_client_outbound_tap(
            author,
            writer_node,
            1,
            core_node,
            1,
        );
    let upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let write = writer
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("isolated/".repeat(8_000))),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();
    writer.tick().unwrap();
    core.tick().unwrap();
    writer.tick().unwrap();
    core.tick().unwrap();
    writer.tick().unwrap();
    assert!(
        writer
            .node
            .large_value_upload_retry_deadlines
            .borrow()
            .contains_key(&write.tx_id)
    );
    assert!(writer.detach_connection(&upstream));
    assert_eq!(writer.node.detached_large_value_uploads.borrow().len(), 1);

    clock.set(31_000);
    let (reconnect_transport, reconnect_core_transport, reconnect_outbound) =
        duplex_with_admitted_session_context_and_client_outbound_tap(
            reconnect_link_identity,
            writer_node,
            2,
            reconnect_remote_node,
            2,
        );
    let _reconnect = crate::db::block_on(writer.connect_upstream(reconnect_transport));
    let _reconnect_subscriber = core.accept_subscriber(reconnect_core_transport, author);
    writer.tick().unwrap();
    assert!(
        reconnect_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadStart(_))),
        "a mismatched authenticated destination starts a fresh handshake"
    );
    assert!(
        !reconnect_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadNodes(_))),
        "a mismatched authenticated destination never receives the retained frontier"
    );
    assert_eq!(
        writer.node.detached_large_value_uploads.borrow().len(),
        1,
        "a mismatched reconnect cannot consume the original destination's frontier"
    );
}

#[test]
fn reconnect_to_different_authenticated_node_never_replays_upload_frontier() {
    assert_different_authenticated_destination_restarts_upload(
        NodeUuid::from_bytes([0xd4; 16]),
        AuthorSubject::for_test_bytes([0xd2; 16]),
    );
}

#[test]
fn reconnect_with_different_authenticated_link_never_replays_upload_frontier() {
    assert_different_authenticated_destination_restarts_upload(
        NodeUuid::from_bytes([0xd3; 16]),
        AuthorSubject::for_test_bytes([0xd5; 16]),
    );
}

/// A Core schedules a fresh owner turn for a peer-edge subscriber that was
/// visited before a later client upload, so Bob receives Alice's later
/// canonical row without an unrelated next websocket frame.
///
/// ```text
/// bob --empty Global subscribe--> peer edge --> Core
/// alice --later CommitUnit----------------------> Core
///                                                |
///                 Core ViewUpdate <--------------+
/// bob <--- peer-edge local IVM refresh <---------+
/// ```
///
/// The peer connection is deliberately accepted before Alice's connection.
/// That makes Core service the already-covered peer first, then accept Alice's
/// write. The Core tick following Alice's upload must request a fresh owner
/// turn for the earlier peer. It deliberately must not recurse into that peer
/// synchronously: opening the view can suspend on cold storage and would
/// otherwise withhold inbound write receipts.
#[test]
fn core_later_client_upload_refreshes_earlier_peer_subscription_on_next_owner_turn() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob_author = AuthorSubject::for_test_bytes([0xb1; 16]);
    let core = open_core(0xd1, AuthorSubject::SYSTEM, &schema);
    let core_scheduler = Rc::new(RecordingScheduler::default());
    core.server.set_scheduler(Some(core_scheduler.clone()));
    let peer_edge = open_db(0xd2, AuthorSubject::SYSTEM, &schema);
    let bob = open_db(0xd3, bob_author, &schema);

    // Keep the Core-to-peer queue observable, and accept this peer before
    // Alice so the ordering under test is fixed.
    let (peer_transport, core_transport, core_to_peer) = duplex_with_server_outbound_tap();
    let _peer_upstream = crate::db::block_on(peer_edge.connect_upstream(peer_transport));
    let _core_peer = core.accept_subscriber_with_trust(
        core_transport,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let (bob_transport, peer_client_transport) = duplex();
    let _bob_upstream = crate::db::block_on(bob.connect_upstream(bob_transport));
    let _peer_client = peer_edge.accept_subscriber(peer_client_transport, bob_author);

    let query = bob.table("todos");
    let mut subscription = prepared_subscribe(&bob, &query, global_subscribe_opts()).unwrap();
    let opening = (0..32)
        .find_map(|_| {
            bob.tick().unwrap();
            peer_edge.tick().unwrap();
            core.tick().unwrap();
            peer_edge.tick().unwrap();
            bob.tick().unwrap();
            subscription.try_next_event()
        })
        .expect("Bob receives the established empty Global view");
    assert!(event_settled(&opening));
    assert!(opened_rows(opening).is_empty());
    assert!(
        core_to_peer.borrow().is_empty(),
        "the empty opening has been fully consumed before Alice writes"
    );
    core_scheduler.take();

    let alice_edge = open_db(0xd4, alice, &schema);
    let (alice_transport, core_alice_transport) = duplex();
    let _alice_upstream = crate::db::block_on(alice_edge.connect_upstream(alice_transport));
    let _core_alice = core.accept_subscriber(core_alice_transport, alice);
    let write = alice_edge
        .insert(
            "todos",
            cells("later row", false, alice),
            crate::db::InsertOptions {
                row_id: Some(row(0xd5)),
                ..Default::default()
            },
        )
        .unwrap();

    // One edge tick uploads Alice's local commit; one Core tick finalizes it
    // and asks the host for a fresh turn to serve the earlier peer connection.
    alice_edge.tick().unwrap();
    core.tick().unwrap();
    let wakes = core_scheduler.take();
    assert!(
        wakes.contains(&TickUrgency::AfterCurrentTurn),
        "the post-receive subscriber refresh yields to a fresh owner turn instead of recursively ticking a potentially cold view"
    );
    assert!(
        core_to_peer.borrow().is_empty(),
        "the first pass does not synchronously re-enter the earlier subscriber"
    );
    core.tick().unwrap();
    let later_view_updates = core_to_peer
        .borrow()
        .iter()
        .filter(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    supporting_rows: program_fact_adds,
                    settled_through,
                    ..
                }) if *settled_through > GlobalTime(0)
                    && program_fact_adds.iter().any(|fact| {
                        matches!(fact,
                            input
                                if input.version_table.as_str() == "todos"
                                    && input.row == row(0xd5)
                                    && input.version.tx == write.tx_id
                        )
                    })
            )
        })
        .count();
    assert_eq!(
        later_view_updates, 1,
        "the scheduled Core owner turn sends the later canonical membership to the already-covered peer"
    );

    // Applying that upstream ViewUpdate must dirty and refresh the existing
    // Bob connection in the same peer-edge service pass.
    peer_edge.tick().unwrap();
    bob.tick().unwrap();
    let delivered = subscription
        .try_next_event()
        .expect("Bob receives the later row without a retry or a new query");
    let (added, updated, removed) = delta_rows(delivered);
    assert_eq!(row_ids(&added), vec![row(0xd5)]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    // The scheduled follow-up clears its dirty work. A quiet later tick must
    // neither replay the unchanged view nor self-arm another serving loop.
    core.tick().unwrap();
    assert!(
        core_to_peer.borrow().is_empty(),
        "a post-cascade idle tick emits no unchanged peer update"
    );
}

/// An Edge immediately flushes an upload queued by a later client connection
/// through the upstream connection that was already visited in the same pass.
///
/// The upstream connection is deliberately installed first. One client tick
/// places the commit on the Edge subscriber transport; one Edge tick must both
/// ingest it and emit the corresponding Core-bound `CommitUnit`.
#[test]
fn edge_later_client_upload_flushes_earlier_upstream_in_same_tick() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge = open_db(0xd1, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xd2, alice, &schema);

    let (edge_transport, _core_transport, edge_to_core) = duplex_with_client_outbound_tap();
    let _edge_upstream = crate::db::block_on(edge.connect_upstream(edge_transport));

    let (client_transport, edge_client_transport) = duplex();
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _edge_client = edge.accept_subscriber(edge_client_transport, alice);

    let write = client
        .insert(
            "todos",
            cells("later upload", false, alice),
            crate::db::InsertOptions {
                row_id: Some(row(0xd3)),
                ..Default::default()
            },
        )
        .unwrap();
    client.tick().unwrap();
    edge.tick().unwrap();

    let uploads = edge_to_core
        .borrow()
        .iter()
        .filter(|message| {
            matches!(
                message,
                SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.tx_id
            )
        })
        .count();
    assert_eq!(
        uploads, 1,
        "one Edge service pass flushes the later client upload through the earlier upstream link"
    );

    edge.tick().unwrap();
    assert_eq!(
        edge_to_core
            .borrow()
            .iter()
            .filter(|message| {
                matches!(
                    message,
                    SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.tx_id
                )
            })
            .count(),
        1,
        "a quiet follow-up tick does not replay the same upload"
    );
}

/// Edge admission compares a client HLC's Unix-millisecond physical component
/// with the authority wall clock, not with the process-relative retry timer.
/// This needs the real connection topology: direct NodeState admission never
/// exercises the served-client edge path where retry timing is also available.
#[test]
fn edge_admits_client_write_with_current_unix_timestamp() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa4; 16]);
    let edge = open_core(0xd6, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xd7, alice, &schema);

    let (client_transport, edge_client_transport) = duplex();
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _edge_client = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_client_transport,
            alice,
            test_provider_claims(alice),
            CommitUnitTrust::Session,
        );

    let unix_now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("test clock is after Unix epoch")
        .as_millis()
        .try_into()
        .expect("Unix milliseconds fit u64");
    let write = client
        .insert(
            "todos",
            cells("current timestamp", false, alice),
            crate::db::InsertOptions {
                row_id: Some(row(0xd8)),
                updated_at_ms: Some(unix_now_ms),
                ..Default::default()
            },
        )
        .expect("client creates a current-time local write");

    client.tick().expect("client uploads the write");
    for _ in 0..8 {
        edge.tick().expect("edge services the client write");
        if matches!(
            crate::db::block_on(edge.node().borrow_mut().transaction_state(write.tx_id)),
            Some((Fate::Accepted, None, DurabilityTier::Edge))
        ) {
            break;
        }
    }

    let edge_state = crate::db::block_on(edge.node().borrow_mut().transaction_state(write.tx_id));
    assert!(
        matches!(
            edge_state,
            Some((Fate::Accepted, None, DurabilityTier::Edge))
        ),
        "a current Unix-time client write becomes Edge durable instead of being compared to the retry timer; observed {edge_state:?}"
    );
    assert!(
        edge.server
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == write.tx_id),
        "the admitted Edge write is retained for its upstream authority"
    );

    let future = client
        .insert(
            "todos",
            cells("future timestamp", false, alice),
            crate::db::InsertOptions {
                row_id: Some(row(0xd9)),
                updated_at_ms: Some(
                    unix_now_ms
                        .saturating_add(crate::node::SKEW_TOLERANCE_MS)
                        .saturating_add(10_000),
                ),
                ..Default::default()
            },
        )
        .expect("client creates a far-future local write");
    client.tick().expect("client uploads the far-future write");
    for _ in 0..8 {
        edge.tick().expect("edge services the far-future write");
        if matches!(
            crate::db::block_on(edge.node().borrow_mut().transaction_state(future.tx_id)),
            Some((
                Fate::Rejected(RejectionReason::ClientClockTooFarAhead),
                None,
                DurabilityTier::Local
            ))
        ) {
            break;
        }
    }
    let future_state =
        crate::db::block_on(edge.node().borrow_mut().transaction_state(future.tx_id));
    assert!(
        matches!(
            future_state,
            Some((
                Fate::Rejected(RejectionReason::ClientClockTooFarAhead),
                None,
                DurabilityTier::Local
            ))
        ),
        "the same Edge path retains forward-skew rejection; observed {future_state:?}"
    );
}

#[test]
fn pending_global_state_does_not_complete_remote_wait_or_prune_upload() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, author, &schema);
    let write = client
        .insert(
            "todos",
            cells("pending global", false, author),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();

    client
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Pending,
            global_time: None,
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    let state = client.write_state(tx_id).unwrap();
    assert_eq!(state.fate, Fate::Pending);
    assert_eq!(state.durability, DurabilityTier::Global);
    assert!(
        block_on(
            client
                .node
                .transaction_wait_outcome(tx_id, DurabilityTier::Global)
        )
        .is_none(),
        "a hydration-only durability claim must not complete a remote transaction wait"
    );
    assert_eq!(
        block_on(
            client
                .node
                .transaction_wait_outcome(tx_id, DurabilityTier::Local)
        )
        .expect("local persistence completes independently of authority fate")
        .unwrap(),
        tx_id
    );

    client.tick().unwrap();
    assert!(
        client
            .node
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "Pending+Global must retain the canonical upload until an Accepted fate"
    );
}

#[test]
fn global_wait_requires_authority_timestamp_after_accepted_global_durability() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, author, &schema);
    let write = client
        .insert(
            "todos",
            cells("authority timestamp", false, author),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();

    client
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    let state = client.write_state(tx_id).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.global_time, None);
    assert_eq!(state.durability, DurabilityTier::Global);
    assert!(
        block_on(
            client
                .node
                .transaction_wait_outcome(tx_id, DurabilityTier::Global)
        )
        .is_none(),
        "Accepted+Global without an authority timestamp cannot complete Global wait"
    );
    assert_eq!(
        block_on(
            client
                .node
                .transaction_wait_outcome(tx_id, DurabilityTier::Edge)
        )
        .expect("Accepted Edge durability does not require a Global timestamp")
        .unwrap(),
        tx_id
    );

    client
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(7)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    assert_eq!(
        client.write_state(tx_id).unwrap().global_time,
        Some(GlobalTime(7))
    );
    assert_eq!(
        block_on(
            client
                .node
                .transaction_wait_outcome(tx_id, DurabilityTier::Global)
        )
        .expect("authority timestamp completes Global wait")
        .unwrap(),
        tx_id
    );
}

/// Internal receipt injection is needed to separate durability from authority
/// time; public transport normally delivers both fields in the same fate.
#[test]
fn pending_writes_barrier_waits_for_global_authority_timestamp() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let client = open_db(0xc2, author, &schema);
    let write = client
        .insert(
            "todos",
            cells("graceful handoff", false, author),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    client
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    let mut barrier = std::pin::pin!(client.wait_for_pending_writes(DurabilityTier::Global));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    // Let asynchronous storage and the normal scheduler run before deciding
    // this is a receipt wait, rather than merely an unfinished index read.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        client.tick().unwrap();
        assert!(
            std::future::Future::poll(barrier.as_mut(), &mut context).is_pending(),
            "Global durability without authority time must not permit context handoff"
        );
        if client
            .node
            .write_state_waiters
            .borrow()
            .contains_key(&tx_id)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "barrier must reach the transaction receipt wait"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    client
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(8)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    // This direct node injection bypasses PeerConnection's ordinary receipt
    // notification. Deliver that wake explicitly; the barrier still owns and
    // evaluates the actual transaction completion predicate.
    if let Some(waiters) = client.node.write_state_waiters.borrow_mut().remove(&tx_id) {
        for waiter in waiters {
            let crate::db::WriteStateWaiterNotify::Future(sender) = waiter.notify;
            let _ = sender.send(());
        }
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        client.tick().unwrap();
        if let std::task::Poll::Ready(result) =
            std::future::Future::poll(barrier.as_mut(), &mut context)
        {
            result.expect("complete authority receipt releases the handoff");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "complete authority receipt must release the handoff"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

/// Internal fate injection separates durability from authority time while the
/// public backend open/attributed write/reopen exercise the host boundary.
#[test]
fn backend_pending_writes_barrier_includes_recovered_attributed_writes() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let column_families = schema.column_families();
    let names = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let (storage, _) = TestStorage::controlled(&names);
    let retained = storage.clone();
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xc2; 16]),
        author,
    };
    // SAFETY: this fixture represents host-admitted backend authority.
    let client = block_on(unsafe {
        Db::open_with_backend_attribution(DbConfig {
            schema: schema.clone(),
            storage,
            identity,
            id_source: Some(Box::new(SeededRowIdSource::new(0xc2))),
        })
    })
    .unwrap();
    let tx_id = client
        .insert(
            "todos",
            cells("graceful attributed handoff", false, author),
            crate::db::InsertOptions {
                row_id: Some(row(0xd3)),
                identity: crate::db::WriteIdentity::Attribution(AuthorSubject::for_test_bytes(
                    [0xd3; 16],
                )),
                ..Default::default()
            },
        )
        .unwrap()
        .mergeable_tx_id();
    block_on(client.close()).unwrap();
    drop(client);
    let storage = block_on(retained.reopen(column_families)).unwrap();
    let client = block_on(unsafe {
        Db::open_with_backend_attribution(DbConfig {
            schema: schema.clone(),
            storage,
            identity,
            id_source: Some(Box::new(SeededRowIdSource::new(0xc3))),
        })
    })
    .unwrap();
    assert!(
        client
            .node
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "reopening must restore user-attributed backend uploads"
    );
    client
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    let mut barrier = std::pin::pin!(client.wait_for_pending_writes(DurabilityTier::Global));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    // Let asynchronous storage and the normal scheduler run before deciding
    // this is a receipt wait, rather than merely an unfinished index read.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        client.tick().unwrap();
        assert!(
            std::future::Future::poll(barrier.as_mut(), &mut context).is_pending(),
            "Global durability without authority time must not permit context handoff"
        );
        if client
            .node
            .write_state_waiters
            .borrow()
            .contains_key(&tx_id)
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "barrier must reach the transaction receipt wait"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    client
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(8)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    // This direct node injection bypasses PeerConnection's ordinary receipt
    // notification. Deliver that wake explicitly; the barrier still owns and
    // evaluates the actual transaction completion predicate.
    if let Some(waiters) = client.node.write_state_waiters.borrow_mut().remove(&tx_id) {
        for waiter in waiters {
            let crate::db::WriteStateWaiterNotify::Future(sender) = waiter.notify;
            let _ = sender.send(());
        }
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        client.tick().unwrap();
        if let std::task::Poll::Ready(result) =
            std::future::Future::poll(barrier.as_mut(), &mut context)
        {
            result.expect("complete authority receipt releases the handoff");
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "complete authority receipt must release the handoff"
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
}

#[test]
fn write_state_waiter_resolves_on_remote_fate_update() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let write = client
        .insert(
            "todos",
            cells("wait for fate", false, owner),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    assert_eq!(
        client.write_state(tx_id).unwrap().durability,
        DurabilityTier::Local
    );

    let changed = client.next_write_state_change(tx_id);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(changed);

    let state = client.write_state(tx_id).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.durability, DurabilityTier::Global);
}

#[test]
fn db_sync_surface_preserves_creator_provenance_across_peer_update() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let receiver = open_db(0xc1, alice, &schema);

    let write = server
        .insert_attributed(alice, "todos", cells("created by alice", false, alice))
        .unwrap();
    let row = write.row_uuid();
    let query = Query::from("todos");
    let create_unit = server
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    receiver
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(create_unit)
        .unwrap();

    server.next_now_ms.set(2);
    let bob_update = server
        .update_attributed(
            bob,
            "todos",
            row,
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("updated by bob".to_owned()),
            )]),
        )
        .unwrap();
    block_on(bob_update.wait(DurabilityTier::Global)).unwrap();
    let server_rows = server.read(&query).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(
        server_rows[0].provenance().unwrap().unwrap().updated_by,
        bob
    );
    let update_unit = server
        .node()
        .borrow_mut()
        .commit_unit_for(bob_update.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = update_unit else {
        panic!("expected update commit unit");
    };
    assert_eq!(versions[0].created_by(), alice);
    assert_eq!(versions[0].updated_by(), bob);
    let receiver_updates = receiver
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::CommitUnit { tx, versions })
        .unwrap();
    assert!(
        receiver_updates.iter().any(|message| {
            matches!(
                message,
                SyncMessage::FateUpdate {
                    fate: Fate::Accepted,
                    ..
                }
            )
        }),
        "receiver should accept the update, got {receiver_updates:?}"
    );
    let receiver_unit = receiver
        .node
        .node
        .borrow_mut()
        .commit_unit_for(bob_update.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit {
        versions: receiver_versions,
        ..
    } = receiver_unit
    else {
        panic!("expected receiver commit unit");
    };
    assert_eq!(receiver_versions[0].created_by(), alice);
    assert_eq!(receiver_versions[0].updated_by(), bob);

    let alice_rows = prepared_read(&receiver, &query);
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].row_uuid(), row);
    let provenance = alice_rows[0]
        .provenance()
        .unwrap()
        .expect("current rows should carry provenance");
    assert_eq!(provenance.created_by, alice);
    assert_eq!(provenance.updated_by, bob);
    assert!(
        provenance.created_at < provenance.updated_at,
        "updating a row must preserve creator provenance while advancing updater provenance"
    );
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_private_table_query() {
    let schema = owner_id_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let writer = open_db(0xa1, alice, &schema);
    let reader = open_db(0xb2, bob, &schema);

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(alice.test_uuid().to_string()),
        )]),
    );
    let write = writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                (
                    "owner_id".to_owned(),
                    Value::String(alice.test_uuid().to_string()),
                ),
            ]),
            Default::default(),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();
    writer.tick().unwrap();
    assert!(
        matches!(write.write_state().unwrap().fate, Fate::Accepted),
        "the private row must be accepted before testing Bob's read denial"
    );

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = crate::db::block_on(reader.connect_upstream(reader_transport));
    let _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(bob.test_uuid().to_string()),
        )]),
    );
    let query = Query::from("messages");
    let mut subscription = prepared_subscribe(&reader, &query, edge_subscribe_opts()).unwrap();
    assert!(subscription.try_next_event().is_none());
    reader.tick().unwrap();
    server.tick().unwrap();
    reader.tick().unwrap();
    assert!(opened_rows(next_settled_opening(&mut subscription)).is_empty());
    assert!(prepared_all(&reader, &query, edge_subscribe_opts()).is_empty());
}

/// A real client commonly reads its self-membership grant before querying the
/// resource that grant authorizes. The second subscription must publish a
/// result membership even when the first subscription already delivered the
/// resource as policy support.
fn membership_grant_then_parent_query_keeps_disjunctive_read_proof(indexed: bool) {
    let member_exists = public_exists(
        "members",
        [
            public_outer_eq("workspace_id", "id"),
            public_session_eq("subject", &["claims", "user_id"]),
        ],
    );
    let workspaces = PublicTableSchemaBuilder::new("workspaces")
        .column("owner_subject", PublicColumnType::Text)
        .policies(
            PublicTablePolicies::new()
                .with_select(PublicPolicyExpr::Or(vec![
                    public_session_eq("owner_subject", &["claims", "user_id"]),
                    member_exists,
                ]))
                .with_insert(PublicPolicyExpr::True),
        );
    let members = PublicTableSchemaBuilder::new("members")
        .fk_column("workspace_id", "workspaces")
        .column("subject", PublicColumnType::Text)
        .column("role", PublicColumnType::Text)
        .policies(
            PublicTablePolicies::new()
                .with_select(PublicPolicyExpr::Or(vec![
                    public_session_eq("subject", &["claims", "user_id"]),
                    PublicPolicyExpr::Inherits {
                        operation: PublicOperation::Select,
                        via_column: "workspace_id".to_owned(),
                        max_depth: None,
                    },
                ]))
                .with_insert(PublicPolicyExpr::True),
        );
    let workspaces = if indexed {
        workspaces.index_only(["owner_subject"])
    } else {
        workspaces
    };
    let members = if indexed {
        members.index_only(["workspace_id", "subject", "role"])
    } else {
        members
    };
    let schema =
        build_public_db_test_schema(PublicSchemaBuilder::new().table(workspaces).table(members));
    let manager = AuthorSubject::for_test_bytes([0xa1; 16]);
    let owner = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa1, manager, &schema);
    let owner_client = open_db(0xb2, owner, &schema);
    let (owner_transport, server_owner_transport) = duplex();
    let _owner_upstream = crate::db::block_on(owner_client.connect_upstream(owner_transport));
    let _owner_subscriber = server.accept_subscriber_with_claims(
        server_owner_transport,
        owner,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(owner.test_uuid().to_string()),
        )]),
    );
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(
        server_transport,
        manager,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(manager.test_uuid().to_string()),
        )]),
    );
    let workspace = owner_client
        .insert(
            "workspaces",
            BTreeMap::from([(
                "owner_subject".to_owned(),
                Value::String(owner.test_uuid().to_string()),
            )]),
            Default::default(),
        )
        .unwrap();
    let grant = owner_client
        .insert(
            "members",
            BTreeMap::from([
                (
                    "workspace_id".to_owned(),
                    Value::Uuid(workspace.row_uuid().0),
                ),
                (
                    "subject".to_owned(),
                    Value::String(manager.test_uuid().to_string()),
                ),
                ("role".to_owned(), Value::String("member".to_owned())),
            ]),
            Default::default(),
        )
        .unwrap();
    for _ in 0..16 {
        owner_client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        if server.read(&server.table("members")).unwrap().len() == 1 {
            break;
        }
    }
    assert_eq!(server.read(&server.table("workspaces")).unwrap().len(), 1);
    assert_eq!(server.read(&server.table("members")).unwrap().len(), 1);
    let grant_query =
        Query::from("members").filter(eq(col("id"), lit(Value::Uuid(grant.row_uuid().0))));
    let mut grant_subscription =
        prepared_subscribe(&client, &grant_query, edge_subscribe_opts()).unwrap();
    for _ in 0..16 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        if !prepared_all(&client, &grant_query, edge_subscribe_opts()).is_empty() {
            break;
        }
    }
    if indexed {
        assert_eq!(
            server
                .node()
                .borrow()
                .query_engine_read_metrics()
                .source_index_probes,
            0,
            "the disjunctive policy must retain a complete source path",
        );
    }
    assert_eq!(
        prepared_all(&client, &grant_query, edge_subscribe_opts()).len(),
        1
    );
    while grant_subscription.try_next_event().is_some() {}

    let workspace_query =
        Query::from("workspaces").filter(eq(col("id"), lit(Value::Uuid(workspace.row_uuid().0))));
    let mut workspace_subscription =
        prepared_subscribe(&client, &workspace_query, edge_subscribe_opts()).unwrap();
    for _ in 0..16 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        if !prepared_all(&client, &workspace_query, edge_subscribe_opts()).is_empty() {
            break;
        }
    }
    assert_eq!(
        prepared_all(&client, &workspace_query, edge_subscribe_opts())
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![workspace.row_uuid()],
    );
    assert!(workspace_subscription.try_next_event().is_some());
}

/// Covers the normal source layout after the self-membership subscription has
/// already delivered workspace policy support to the client.
#[test]
fn db_sync_surface_membership_grant_then_parent_query_keeps_disjunctive_read_proof() {
    membership_grant_then_parent_query_keeps_disjunctive_read_proof(false);
}

/// Covers the indexed layout, where a disjunctive proof must still retain the
/// complete source path instead of selecting one arm's index for the union.
#[test]
fn db_sync_surface_indexed_membership_grant_then_parent_query_keeps_disjunctive_read_proof() {
    membership_grant_then_parent_query_keeps_disjunctive_read_proof(true);
}

/// A prepared trusted-serving read binds each request session's text `user_id`
/// independently: Alice receives her seeded message while Bob receives none.
///
/// ```text
/// system ──seed owner_id=alice──► server prepared read
///                                      │
///                         Alice session ─┼──► [alice message]
///                           Bob session ─└──► []
/// ```
#[test]
fn prepared_server_read_binds_text_session_user_id_per_session() {
    // Mirror the public test app: a nullable camel-case `ownerId` grants to
    // its matching session or to every session when unowned. In particular,
    // this exercises the disjunctive policy plan rather than only the
    // scalar-equality fast path.
    let read_policy = PublicPolicyExpr::or(vec![
        public_session_eq("ownerId", &["claims", "sub"]),
        PublicPolicyExpr::IsNull {
            column: "ownerId".to_owned(),
        },
    ]);
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .nullable_column("ownerId", PublicColumnType::Text)
                .policies(PublicTablePolicies::new().with_select(read_policy)),
        ),
    );
    let server = open_db(0x5e, AuthorSubject::SYSTEM, &schema);
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let alice_subject = "alice-session-subject";
    let bob_subject = "bob-session-subject";
    server.set_test_provider_claims(
        alice,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(alice_subject.into()),
        )]),
    );
    server.set_test_provider_claims(
        bob,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(bob_subject.into()),
        )]),
    );

    let seeded = server
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("for alice".to_owned())),
                ("done".to_owned(), Value::Bool(false)),
                (
                    "ownerId".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String(alice_subject.into())))),
                ),
            ]),
            Default::default(),
        )
        .expect("system seed must write the protected message");
    block_on(seeded.wait(DurabilityTier::Local)).expect("seed must settle locally");

    // The public `where({ id })` facade contributes an ordinary prepared
    // parameter alongside the hidden policy claim. Keep that mixed binding in
    // this regression so the descriptor cannot accidentally bind Alice's
    // claim into the query-id slot (or vice versa).
    let query = Query::from("todos").filter(eq(col("id"), lit(Value::Uuid(seeded.row_uuid().0))));
    let prepared = prepared(&server, &query);
    let alice_rows = block_on(server.all_for_identity(&prepared, ReadOpts::default(), alice))
        .expect("Alice's prepared read must evaluate against her session claims");
    let bob_rows = block_on(server.all_for_identity(&prepared, ReadOpts::default(), bob))
        .expect("Bob's prepared read must evaluate against his session claims");

    assert_eq!(row_ids(&alice_rows), vec![seeded.row_uuid()]);
    assert!(bob_rows.is_empty());
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_after_runtime_schema_publish() {
    let public_schema = owner_id_public_schema();
    let permission_schema = owner_id_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &public_schema);
    let writer = open_db(0xa1, alice, &permission_schema);
    let alice_reader = open_db(0xa2, alice, &permission_schema);
    let reader = open_db(0xb2, bob, &permission_schema);

    let schema_version = SchemaVersion::new(permission_schema.clone());
    let schema_id = schema_version.id;
    let acks = server.publish_schema(schema_version).unwrap();
    assert!(acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));
    let current_acks = server
        .server
        .node()
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: schema_id,
            },
        })
        .unwrap();
    assert!(current_acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(alice.test_uuid().to_string()),
        )]),
    );
    writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                (
                    "owner_id".to_owned(),
                    Value::String(alice.test_uuid().to_string()),
                ),
            ]),
            Default::default(),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (alice_transport, server_alice_transport) = duplex();
    let _alice_upstream = crate::db::block_on(alice_reader.connect_upstream(alice_transport));
    let _alice_subscriber = server.accept_subscriber_with_claims(
        server_alice_transport,
        alice,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(alice.test_uuid().to_string()),
        )]),
    );
    let query = Query::from("messages");
    let mut alice_subscription =
        prepared_subscribe(&alice_reader, &query, edge_subscribe_opts()).unwrap();
    assert!(alice_subscription.try_next_event().is_none());
    alice_reader.tick().unwrap();
    server.tick().unwrap();
    alice_reader.tick().unwrap();
    let (added, updated, removed) = delta_rows(next_settled_opening(&mut alice_subscription));
    assert_eq!(
        added.len(),
        1,
        "Alice's matching text session claim must read the seeded row"
    );
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(
        row_ids(&prepared_all(&alice_reader, &query, edge_subscribe_opts())),
        vec![added[0].row_uuid()],
    );

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = crate::db::block_on(reader.connect_upstream(reader_transport));
    let _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(bob.test_uuid().to_string()),
        )]),
    );
    let mut subscription = prepared_subscribe(&reader, &query, edge_subscribe_opts()).unwrap();
    assert!(subscription.try_next_event().is_none());

    reader.tick().unwrap();
    server.tick().unwrap();
    reader.tick().unwrap();
    assert!(opened_rows(next_settled_opening(&mut subscription)).is_empty());
    assert!(prepared_all(&reader, &query, edge_subscribe_opts()).is_empty());
}

#[test]
fn detached_subscriber_is_not_served_on_server_tick() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(subscription.try_next_event().is_none());
    client.tick().unwrap();

    assert!(server.server.detach_connection(&subscriber));
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(subscription.try_next_event().is_none());
    assert!(prepared_read(&client, &query).is_empty());
}

#[test]
fn byte_wire_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_bytes, server_bytes) = byte_duplex_raw();
    let server_inbound = Rc::clone(&server_bytes.inbound);
    let _upstream = crate::db::block_on(
        client.connect_upstream(Box::new(WireTransportAdapter::current(client_bytes))),
    );
    let _subscriber = server.accept_subscriber(
        Box::new(WireTransportAdapter::current(server_bytes)),
        client_author,
    );

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(subscription.try_next_event().is_none());

    client.tick().unwrap();
    {
        let queued = server_inbound.borrow();
        let first = queued.front().expect("register shape frame");
        let second = queued.get(1).expect("subscribe frame");
        let mut decoder = WireStreamDecoder::new(current_wire_features()).unwrap();
        let first = match decode_frame(first).unwrap() {
            WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
            other => panic!("expected message frame, got {other:?}"),
        };
        let second = match decode_frame(second).unwrap() {
            WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
            other => panic!("expected message frame, got {other:?}"),
        };
        let SyncMessage::RegisterShape { shape_id, .. } = first else {
            panic!("expected RegisterShape, got {first:?}");
        };
        let SyncMessage::Subscribe(subscribe) = second else {
            panic!("expected Subscribe, got {second:?}");
        };
        assert_eq!(subscribe.shape_id, shape_id);
        assert_eq!(subscribe.subscription.shape_id, shape_id);
    }
    server.tick().unwrap();
    client.tick().unwrap();

    let table = &schema.tables[0];
    let rows = prepared_read(&client, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("from server".to_owned()))
    );
    let (added, updated, removed) = delta_rows(next_settled_opening(&mut subscription));
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    seed(&server, "todos", cells("second", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn single_upstream_tick_applies_multiple_subscription_updates() {
    let schema = issue_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let project = row(1);
    server
        .insert_with_id(
            "projects",
            project,
            BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
        )
        .unwrap();
    seed(
        &server,
        "issues",
        issue_cells("API", "open", owner, project, 5, &["api"], None),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let projects = Query::from("projects");
    let issues = Query::from("issues");
    let mut project_subscription =
        prepared_subscribe(&client, &projects, global_subscribe_opts()).unwrap();
    let mut issue_subscription =
        prepared_subscribe(&client, &issues, global_subscribe_opts()).unwrap();
    assert!(project_subscription.try_next_event().is_none());
    assert!(issue_subscription.try_next_event().is_none());

    client.tick().unwrap();
    server.tick().unwrap();
    let stats = client.tick_stats().unwrap();

    assert_eq!(prepared_read(&client, &projects).len(), 1);
    assert_eq!(prepared_read(&client, &issues).len(), 1);
    assert_eq!(stats.subscription_events, 2);
    assert_eq!(
        delta_rows(next_settled_opening(&mut project_subscription))
            .0
            .len(),
        1
    );
    assert_eq!(
        delta_rows(next_settled_opening(&mut issue_subscription))
            .0
            .len(),
        1
    );
}

#[test]
fn subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(subscription.try_next_event().is_none());

    // The ordinary whole-table subscription owns its initial snapshot.
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(next_settled_opening(&mut subscription));
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 3);

    // Resume and a fresh ordinary subscription publish the same exact
    // CoveredInput closure at the three-row frontier.
    let full_server = open_core(0x6e, AuthorSubject::SYSTEM, &schema);
    seed(&full_server, "todos", cells("first", false, owner));
    seed(&full_server, "todos", cells("second", false, owner));
    seed(&full_server, "todos", cells("third", true, owner));
    let full_client = open_db(0xc2, client_author, &schema);
    let (full_client_transport, full_server_transport) = duplex();
    let _full_upstream = crate::db::block_on(full_client.connect_upstream(full_client_transport));
    let full_subscriber = full_server.accept_subscriber(full_server_transport, client_author);
    let mut full_subscription =
        prepared_subscribe(&full_client, &query, global_subscribe_opts()).unwrap();
    assert!(full_subscription.try_next_event().is_none());
    full_client.tick().unwrap();
    full_server.tick().unwrap();
    full_client.tick().unwrap();
    assert_eq!(
        delta_rows(next_settled_opening(&mut full_subscription))
            .0
            .len(),
        3
    );
    let covered_full_bytes = full_subscriber.borrow().last_resume_bytes().unwrap();
    let full_policy = match &full_subscriber.borrow().link {
        ConnectionLink::Subscriber(state) => state
            .coverage_groups
            .values()
            .next()
            .expect("fresh control owns one coverage group")
            .policy_binding
            .clone(),
        ConnectionLink::Upstream(_) => unreachable!("fresh authority link is a subscriber"),
    };

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = duplex();
    let _resumed_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    let resume_policy = match &resumed.borrow().link {
        ConnectionLink::Subscriber(state) => state
            .coverage_groups
            .values()
            .next()
            .expect("resumed connection owns one coverage group")
            .policy_binding
            .clone(),
        ConnectionLink::Upstream(_) => unreachable!("resumed authority link is a subscriber"),
    };
    assert_eq!(
        resume_policy, full_policy,
        "resume and the full-response control must use the same authenticated policy scope"
    );
    assert!(
        resume_bytes > 0,
        "resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert!(
        resume_bytes <= covered_full_bytes,
        "resume must not exceed the equivalent fresh CoveredInput response: legacy_current_rows={full_bytes}, covered_full={covered_full_bytes}, resume={resume_bytes}"
    );
    assert_eq!(prepared_read(&client, &query).len(), 3);
    assert!(
        prepared_read(&client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn current_rows_uses_its_connection_claim_snapshot_not_the_author_cache() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let admitted = BTreeMap::from([(
        crate::query::provider_claim_key("session"),
        Value::String("admitted".to_owned()),
    )]);
    let stale = BTreeMap::from([(
        crate::query::provider_claim_key("session"),
        Value::String("stale sibling".to_owned()),
    )]);
    let client = open_db(0xc1, author, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber =
        server.accept_subscriber_with_claims(server_transport, author, admitted.clone());
    let query = Query::from("todos");
    let attachment = client
        .attach_query_with_opts(&prepared(&client, &query), global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();

    // Simulate another connection for the same subject updating the legacy
    // identity-keyed compatibility cache after this link was authenticated.
    server.node().borrow_mut().set_session_claims(author, stale);
    subscriber.borrow_mut().tick().unwrap();
    let connection = subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        panic!("ordinary server connection must remain a subscriber link");
    };
    let coverage = state
        .served
        .get(&attachment.subscription())
        .expect("ordinary usage must be admitted");
    let group = state
        .coverage_groups
        .get(coverage)
        .expect("ordinary usage must retain a coverage group");
    assert_eq!(
        group.policy_binding,
        (author, admitted),
        "ordinary whole-table serving must use the exact session claims admitted on this connection"
    );
}

#[test]
fn byte_wire_subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 1);
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(subscription.try_next_event().is_none());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(next_settled_opening(&mut subscription));
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 3);

    // The byte transport follows the same semantic distinction as the in-memory
    // transport above: bound resume against the corresponding CoveredInput
    // full response, not against the legacy current-row snapshot.
    let full_server = open_core(0x6e, AuthorSubject::SYSTEM, &schema);
    seed(&full_server, "todos", cells("first", false, owner));
    seed(&full_server, "todos", cells("second", false, owner));
    seed(&full_server, "todos", cells("third", true, owner));
    let full_client = open_db(0xc2, client_author, &schema);
    let (full_client_transport, full_server_transport) = byte_duplex_with_session(client_author, 3);
    let _full_upstream = crate::db::block_on(full_client.connect_upstream(full_client_transport));
    let full_subscriber = full_server.accept_subscriber(full_server_transport, client_author);
    let mut full_subscription =
        prepared_subscribe(&full_client, &query, global_subscribe_opts()).unwrap();
    assert!(full_subscription.try_next_event().is_none());
    full_client.tick().unwrap();
    full_server.tick().unwrap();
    full_client.tick().unwrap();
    assert_eq!(
        delta_rows(next_settled_opening(&mut full_subscription))
            .0
            .len(),
        3
    );
    let covered_full_bytes = full_subscriber.borrow().last_resume_bytes().unwrap();
    let full_policy = match &full_subscriber.borrow().link {
        ConnectionLink::Subscriber(state) => state
            .coverage_groups
            .values()
            .next()
            .expect("fresh control owns one coverage group")
            .policy_binding
            .clone(),
        ConnectionLink::Upstream(_) => unreachable!("fresh authority link is a subscriber"),
    };

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 2);
    let _resumed_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    let resume_policy = match &resumed.borrow().link {
        ConnectionLink::Subscriber(state) => state
            .coverage_groups
            .values()
            .next()
            .expect("resumed connection owns one coverage group")
            .policy_binding
            .clone(),
        ConnectionLink::Upstream(_) => unreachable!("resumed authority link is a subscriber"),
    };
    assert_eq!(
        resume_policy, full_policy,
        "byte-wire resume and its full-response control must use the same authenticated policy scope"
    );
    assert!(
        resume_bytes > 0,
        "byte-wire resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert!(
        resume_bytes <= covered_full_bytes,
        "byte-wire resume must not exceed the equivalent fresh CoveredInput response: legacy_current_rows={full_bytes}, covered_full={covered_full_bytes}, resume={resume_bytes}"
    );
    assert_eq!(prepared_read(&client, &query).len(), 3);
    assert!(
        prepared_read(&client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn connect_upstream_announces_existing_subscriptions_on_first_tick() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));

    client.tick().unwrap();
    let first = upstream_transport.try_recv().unwrap();
    let second = upstream_transport.try_recv().unwrap();
    assert!(upstream_transport.try_recv().is_none());

    let SyncMessage::RegisterShape { shape_id, .. } = first else {
        panic!("expected existing subscription shape to be registered upstream first");
    };
    let SyncMessage::Subscribe(subscribe) = second else {
        panic!("expected existing subscription to be announced upstream second");
    };
    assert_eq!(subscribe.shape_id, shape_id);
    assert_eq!(subscribe.subscription.shape_id, shape_id);
}

/// This is intentionally an internal lifecycle test: the public symptom is a
/// binding panic, but reproducing its ordering requires holding the exact node
/// state that an interruptible evaluation or hydration operation owns.
#[test]
fn connect_upstream_waits_for_active_node_state_borrow() {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    let schema = schema();
    let client = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    let node = client.node.node();
    let held_node = crate::db::block_on(node.lock());
    let (client_transport, _server_transport) = duplex();
    let mut connection = pin!(client.connect_upstream(client_transport));
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    assert!(matches!(connection.as_mut().poll(&mut cx), Poll::Pending));
    drop(held_node);
    let _connection = crate::db::block_on(connection);
}

/// Test-only marker for an authenticated SYSTEM backend transport. Ordinary
/// session links must not send `SessionClaims`: their authenticated handshake
/// is the authority for those claims.
struct DelegationCapableTransport {
    inner: Box<dyn Transport>,
}

impl Transport for DelegationCapableTransport {
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

// SessionClaims has no distinct public state once the receiving NodeState has
// ignored an identical map, so wire-count coverage must inspect a delegation-
// capable backend transport. The policy-visible integration coverage lives
// above this facade; this test protects its otherwise unobservable wire-
// chatter contract without granting the same ability to ordinary sessions.
#[test]
fn repeated_identical_session_claims_emit_once_on_a_delegation_capable_connection() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(Box::new(
        DelegationCapableTransport {
            inner: client_transport,
        },
    )));
    let claims = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);
    client.set_test_provider_claims(client_author, claims.clone());
    client.set_test_provider_claims(client_author, claims);
    client.tick().unwrap();

    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { .. })
    ));
    assert!(
        upstream_transport.try_recv().is_none(),
        "an unchanged claim map must not produce another wire message"
    );
}

#[test]
fn ordinary_session_links_do_not_forward_claims() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    client.set_test_provider_claims(
        client_author,
        BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]),
    );
    client.tick().unwrap();

    assert!(
        upstream_transport.try_recv().is_none(),
        "ordinary session links must rely on their authenticated handshake, not smuggle SessionClaims"
    );
}

// This is lower-level for the same reason as the wire-count test above. In
// particular, it is the regression that a global deduplication would miss:
// each newly attached transport must receive the current map independently.
#[test]
fn current_session_claims_reach_late_and_reconnected_delegation_capable_upstreams() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let claims = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);
    let admitted_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("reader".to_owned()),
    )]);

    client.set_test_provider_claims(client_author, claims.clone());
    let (first_transport, mut first_upstream_transport) = duplex();
    let first_upstream = crate::db::block_on(client.connect_upstream(Box::new(
        DelegationCapableTransport {
            inner: first_transport,
        },
    )));
    client.tick().unwrap();
    assert!(matches!(
        first_upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims: received })
            if identity == client_author && received == admitted_claims
    ));
    assert!(first_upstream_transport.try_recv().is_none());

    client.set_test_provider_claims(client_author, claims.clone());
    assert!(client.detach_connection(&first_upstream));

    let (reconnected_transport, mut reconnected_upstream_transport) = duplex();
    let _reconnected_upstream = crate::db::block_on(client.connect_upstream(Box::new(
        DelegationCapableTransport {
            inner: reconnected_transport,
        },
    )));
    client.tick().unwrap();
    assert!(matches!(
        reconnected_upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims: received })
            if identity == client_author && received == admitted_claims
    ));
    assert!(reconnected_upstream_transport.try_recv().is_none());
}

#[test]
fn changed_session_claims_advance_delivery_on_a_delegation_capable_connection() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(Box::new(
        DelegationCapableTransport {
            inner: client_transport,
        },
    )));
    let reader = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);
    let writer = BTreeMap::from([("role".to_owned(), Value::String("writer".to_owned()))]);
    let reader_admitted = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("reader".to_owned()),
    )]);
    let writer_admitted = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("writer".to_owned()),
    )]);

    client.set_test_provider_claims(client_author, reader.clone());
    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { claims, .. }) if claims == reader_admitted
    ));

    client.set_test_provider_claims(client_author, reader);
    client.tick().unwrap();
    assert!(upstream_transport.try_recv().is_none());

    client.set_test_provider_claims(client_author, writer.clone());
    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims })
            if identity == client_author && claims == writer_admitted
    ));
    assert!(upstream_transport.try_recv().is_none());
}

#[test]
fn global_subscription_registers_array_subquery_upstream_coverage() {
    let schema = relation_schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));

    let query = Query::from("users").array_subquery(
        ArraySubquery::new("todos", "todos", "owner_id", "id")
            .nested(ArraySubquery::new("comments", "comments", "todo_id", "id")),
    );
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();

    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::RegisterShape { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::Subscribe(_))
    ));
}

#[test]
fn array_subquery_attachment_registers_upstream_coverage() {
    let schema = relation_schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));

    let query = Query::from("users").array_subquery(
        ArraySubquery::new("todos", "todos", "owner_id", "id")
            .nested(ArraySubquery::new("comments", "comments", "todo_id", "id")),
    );
    let prepared = prepared(&client, &query);
    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();

    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::RegisterShape { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::Subscribe(_))
    ));
    client.detach_query(attachment);
}

#[test]
fn upload_is_not_marked_sent_after_one_shot_backpressure_and_retries() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let outbound = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let transport = BackpressureOnceTransport {
        outbound: Rc::clone(&outbound),
        failed: false,
    };
    let _upstream = crate::db::block_on(client.connect_upstream(Box::new(transport)));

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xf1), client.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("retry", false, client_author)),
        )
        .unwrap();
    assert!(
        client
            .node
            .outbox
            .borrow_mut()
            .push(PendingUpload { tx_id, unit: None }),
        "test setup queues the retry upload once"
    );

    client.tick().unwrap();
    assert!(outbound.borrow().is_empty());
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .sync_metrics()
            .transport_backpressure_retries,
        1
    );

    client.tick().unwrap();
    let sent = outbound.borrow_mut().pop_front().unwrap();
    let SyncMessage::CommitUnit { tx, .. } = sent else {
        panic!("expected retried commit upload");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert!(outbound.borrow_mut().pop_front().is_none());
}

/// A terminal authority rejection releases its upload from the shared outbox,
/// so reconnecting the client cannot replay a transaction whose user-visible
/// outcome is already final.
///
/// writer ──CommitUnit──► authority
/// writer ◄─rejected fate── authority
/// writer ──reconnect──► replacement authority (no replay)
#[test]
fn rejected_upload_is_not_replayed_after_reconnect() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xe1; 16]);
    let client = open_db(0xe1, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));

    let write = client
        .insert(
            "todos",
            cells("rejected", false, author),
            Default::default(),
        )
        .expect("stage local upload");
    client.tick().expect("send initial upload");
    let uploaded = std::iter::from_fn(|| authority_transport.try_recv()).find_map(|message| {
        matches!(message, SyncMessage::CommitUnit { ref tx, .. } if tx.tx_id == write.mergeable_tx_id())
            .then_some(message)
    });
    assert!(
        uploaded.is_some(),
        "authority receives the staged upload once"
    );

    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: None,
        })
        .expect("return terminal rejection");
    client.tick().expect("apply terminal rejection");
    let rejected = crate::db::block_on(write.wait(DurabilityTier::Global))
        .expect_err("rejected upload stays terminal");
    assert_eq!(rejected.code, ErrorCode::WriteRejected);

    assert!(client.detach_connection(&upstream));
    let (reconnected_transport, mut replacement_authority) = duplex();
    let _reconnected = crate::db::block_on(client.connect_upstream(reconnected_transport));
    client.tick().expect("tick replacement connection");
    assert!(
        std::iter::from_fn(|| replacement_authority.try_recv()).all(
            |message| !matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.mergeable_tx_id())
        ),
        "replacement authority must not replay a terminally rejected upload"
    );
}

/// Each upstream owns an independent upload cursor.  A fate cleanup can make
/// one cursor non-contiguous relative to the shared oldest-first outbox; that
/// link must fall back to the complete set difference rather than treating its
/// newest uploaded entry as proof that the missing middle entry was sent.
///
/// upstream A: [first, middle, last]
/// upstream B: [first,      -, last] ──tick──► [middle]
#[test]
fn upload_cursor_hole_replays_only_the_missing_entry_on_that_upstream() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xe2; 16]);
    let client = open_db(0xe2, author, &schema);
    let (first_transport, mut first_authority) = duplex();
    let _first = crate::db::block_on(client.connect_upstream(first_transport));
    let (second_transport, mut second_authority) = duplex();
    let _second = crate::db::block_on(client.connect_upstream(second_transport));

    let writes = ["first", "middle", "last"]
        .into_iter()
        .map(|title| {
            client
                .insert("todos", cells(title, false, author), Default::default())
                .expect("stage upload")
        })
        .collect::<Vec<_>>();
    let tx_ids = writes
        .iter()
        .map(|write| write.mergeable_tx_id())
        .collect::<Vec<_>>();

    client
        .tick()
        .expect("send every new entry to both upstreams");
    for authority in [&mut first_authority, &mut second_authority] {
        let sent = std::iter::from_fn(|| authority.try_recv())
            .filter_map(|message| match message {
                SyncMessage::CommitUnit { tx, .. } => Some(tx.tx_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(sent, tx_ids, "initial cursor is contiguous per upstream");
    }

    let connections = client.node.connections.borrow().clone();
    assert_eq!(
        connections.len(),
        2,
        "fixture attached two independent links"
    );
    let mut second = crate::db::block_on(connections[1].lock());
    let crate::db::peer_connection::ConnectionLink::Upstream(state) = &mut second.link else {
        panic!("second fixture link is upstream");
    };
    assert!(
        state.uploaded.remove(&tx_ids[1]),
        "plant one middle cursor hole while retaining both surrounding receipts"
    );
    drop(second);

    client
        .tick()
        .expect("conservatively repair the cursor hole");
    assert!(
        std::iter::from_fn(|| first_authority.try_recv())
            .all(|message| !matches!(message, SyncMessage::CommitUnit { .. })),
        "the independent complete cursor must not duplicate uploads"
    );
    let repaired = std::iter::from_fn(|| second_authority.try_recv())
        .filter_map(|message| match message {
            SyncMessage::CommitUnit { tx, .. } => Some(tx.tx_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        repaired,
        vec![tx_ids[1]],
        "the hole fallback sends precisely the missing middle upload once"
    );
}

/// Upload entries remain replayable until an applied terminal fate either
/// rejects them or carries accepted Global durability plus authority time. An
/// Accepted fate at Local, Edge, or Global-without-time is only progress:
/// reconnect must resend it until a later time-bearing Global fate releases the
/// shared outbox entry.
#[test]
fn accepted_upload_releases_outbox_only_after_global_durability_and_authority_time() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xe3; 16]);
    let client = open_db(0xe3, author, &schema);
    let (client_transport, mut authority) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let write = client
        .insert(
            "todos",
            cells("accepted in stages", false, author),
            Default::default(),
        )
        .expect("stage upload");
    let tx_id = write.mergeable_tx_id();

    assert!(
        client
            .node
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "the initial Pending/retryable upload remains in the outbox"
    );

    client.tick().expect("send initial upload");
    assert!(
        std::iter::from_fn(|| authority.try_recv()).any(
            |message| matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == tx_id)
        )
    );
    for durability in [DurabilityTier::Local, DurabilityTier::Edge] {
        authority
            .send(SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                global_time: None,
                durability: Some(durability),
            })
            .expect("return non-global acceptance");
        client.tick().expect("apply non-global acceptance");
        assert!(
            client
                .node
                .outbox
                .borrow()
                .iter()
                .any(|pending| pending.tx_id == tx_id),
            "{durability:?} acceptance is not terminal for upload replay"
        );
    }

    authority
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Global),
        })
        .expect("return timeless global acceptance");
    client.tick().expect("apply timeless global acceptance");
    assert!(
        client
            .node
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "Accepted+Global without authority time must retain the shared outbox upload"
    );

    authority
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(7)),
            durability: Some(DurabilityTier::Global),
        })
        .expect("return time-bearing global acceptance");
    client
        .tick()
        .expect("apply terminal time-bearing global acceptance");
    assert!(
        !client
            .node
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "time-bearing Global acceptance releases the upload from the shared outbox"
    );
}

#[test]
fn local_missing_upload_body_still_kills_sync_driver() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, _server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let missing_tx = TxId::new(
        crate::time::TxTime::from(client.next_now_ms()),
        NodeUuid::from_bytes([0xee; 16]),
    );
    assert!(
        client.node.outbox.borrow_mut().push(PendingUpload {
            tx_id: missing_tx,
            unit: None,
        }),
        "test setup queues the missing upload once"
    );

    let error = client.tick().unwrap_err();
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(
        error.message.contains("missing transaction"),
        "unexpected local-fatal error: {}",
        error.message
    );
}

#[test]
fn detach_connection_removes_connection_from_db_ticks() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));

    assert!(client.detach_connection(&upstream));
    assert!(!client.detach_connection(&upstream));

    client.tick().unwrap();
    assert!(upstream_transport.try_recv().is_none());
}

#[test]
fn accepted_subscriber_is_served_under_subscriber_author_identity() {
    let schema = owner_read_schema();
    let subscriber_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server_author = AuthorSubject::for_test_bytes([0x5e; 16]);
    let other_author = AuthorSubject::for_test_bytes([0xd1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, subscriber_author, &schema);

    let visible = seed(
        &server,
        "todos",
        cells("for subscriber", false, subscriber_author),
    );
    seed(&server, "todos", cells("for server", false, server_author));
    seed(
        &server,
        "todos",
        cells("for someone else", false, other_author),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, subscriber_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(subscription.try_next_event().is_none());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let (rows, updated, removed) = delta_rows(next_settled_opening(&mut subscription));
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(row_ids(&rows), vec![visible]);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("for subscriber".to_owned()))
    );
}

#[test]
fn client_initial_sync_flush_cadence_preserves_public_snapshot_delivery() {
    let schema = schema();
    let server = open_core(0xd4, AuthorSubject::SYSTEM, &schema);
    for ordinal in 0..3_u8 {
        server
            .insert_with_id(
                "todos",
                row(0xd0 + ordinal),
                BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String(format!("server {ordinal}")),
                    ),
                    ("done".to_owned(), Value::Bool(false)),
                    ("owner".to_owned(), Value::Uuid(row(0xd4).0)),
                ]),
            )
            .unwrap();
    }

    let client_author = AuthorSubject::for_test_bytes([0xd5; 16]);
    let client = open_db(0xd5, client_author, &schema);
    client
        .set_initial_sync_flush_cadence(InitialSyncFlushCadence::every(
            NonZeroUsize::new(2).unwrap(),
        ))
        .unwrap();
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = client.table("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(subscription.try_next_event().is_none());

    for _ in 0..20 {
        client.tick().unwrap();
        server.server.tick().unwrap();
        client.tick().unwrap();
        if let Some(event) = subscription.try_next_event() {
            assert!(matches!(
                &event,
                SubscriptionEvent::Delta {
                    reset: true,
                    publishable: true,
                    settled: true,
                    ..
                }
            ));
            assert_eq!(opened_rows(event).len(), 3);
            return;
        }
    }
    panic!("client configured with a cadence must receive the initial snapshot");
}

/// Internal because only controlled storage can hold a new maintained graph
/// between replacement installation and its first local terminal batch. The
/// public contract is the stream: a runtime rebuild must not publish that
/// incomplete graph as an empty reset.
#[test]
fn cold_runtime_replacement_defers_empty_facade_until_local_snapshot_arrives() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xd1; 16]);
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&column_family_refs);
    let eviction_handle = storage.clone();
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0xd1; 16]),
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xd1))),
    }))
    .expect("open controlled cold-replacement fixture");
    let old = row(0xd1);
    db.insert(
        "todos",
        cells("old", false, author),
        crate::db::InsertOptions {
            row_id: Some(old),
            ..Default::default()
        },
    )
    .expect("persist old row");
    db.tick().expect("settle old row");

    let query = Query::from("todos").filter(eq(col("done"), lit(Value::Bool(false))));
    let prepared = db.prepare_query(&query).expect("prepare todos query");
    let mut subscription =
        block_on(db.subscribe(&prepared, ReadOpts::default())).expect("open local subscription");
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("expected opening subscription delta");
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [old]
    );

    // Stage a successor directly in the node so ordinary write publication
    // cannot refresh the old terminal before the replacement boundary.
    let new = row(0xd2);
    db.node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", old, db.next_now_ms())
                .made_by(author)
                .permission_subject(author)
                .cells(cells("old", true, author)),
        )
        .expect("stage old-row removal");
    db.node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", new, db.next_now_ms())
                .made_by(author)
                .permission_subject(author)
                .cells(cells("new", false, author)),
        )
        .expect("stage new-row addition");

    db.invalidate_groove_runtime_for_test();
    eviction_handle.evict_all();
    control.pause_on(TestStorageOperation::ScanOpen);
    control.pause_on(TestStorageOperation::Get);
    assert_eq!(
        db.refresh_subscriptions()
            .expect("open cold replacement without publishing it"),
        0
    );
    assert!(
        subscription.try_next_event().is_none(),
        "the incomplete cold terminal must retain the last delivered facade"
    );

    control.resume();
    let mut reset = None;
    for _ in 0..8 {
        db.refresh_subscriptions()
            .expect("drain resumed replacement terminal");
        if let Some(event) = subscription.try_next_event() {
            reset = Some(event);
            break;
        }
    }
    let Some(SubscriptionEvent::Delta {
        reset: true,
        added,
        updated,
        removed,
        ..
    }) = reset
    else {
        panic!("expected one complete reset after the local replacement snapshot");
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [new]
    );
    assert!(updated.is_empty());
    assert_eq!(
        removed.iter().map(|row| row.row_uuid).collect::<Vec<_>>(),
        [old]
    );
    assert!(
        subscription.try_next_event().is_none(),
        "the first local batch replaces the retained facade exactly once"
    );

    // The successor can also be genuinely empty. It must publish that deletion
    // only after its local terminal batch, rather than leaking a transient
    // empty facade before the replacement has initialized.
    db.node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", new, db.next_now_ms())
                .made_by(author)
                .permission_subject(author)
                .cells(cells("new", true, author)),
        )
        .expect("stage empty successor");
    db.invalidate_groove_runtime_for_test();
    eviction_handle.evict_all();
    control.pause_on(TestStorageOperation::ScanOpen);
    control.pause_on(TestStorageOperation::Get);
    assert_eq!(
        db.refresh_subscriptions()
            .expect("open cold empty replacement without publishing it"),
        0
    );
    assert!(
        subscription.try_next_event().is_none(),
        "a cold empty successor must keep the last complete facade"
    );

    control.resume();
    let mut empty_reset = None;
    for _ in 0..8 {
        db.refresh_subscriptions()
            .expect("drain resumed empty replacement terminal");
        if let Some(event) = subscription.try_next_event() {
            empty_reset = Some(event);
            break;
        }
    }
    let Some(SubscriptionEvent::Delta {
        reset: true,
        added,
        updated,
        removed,
        ..
    }) = empty_reset
    else {
        panic!("expected one empty reset after the local replacement snapshot");
    };
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed.iter().map(|row| row.row_uuid).collect::<Vec<_>>(),
        [new]
    );

    let mut fresh = block_on(db.subscribe(&prepared, ReadOpts::default()))
        .expect("open a subscription after the empty replacement settled");
    let SubscriptionEvent::Delta { added, .. } = block_on(fresh.next_raw()).unwrap() else {
        panic!("expected opening delta after empty replacement");
    };
    assert!(
        added.is_empty(),
        "a subscription opened after the replacement must not inherit the retained facade"
    );
}
