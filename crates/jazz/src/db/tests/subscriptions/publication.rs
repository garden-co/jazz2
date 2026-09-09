//! Deterministic publication-window tests. Rows come from a public Db/query;
//! the internal sender lets these tests place settlement between exact result
//! changes without relying on network scheduling or storage timing.

use super::*;

fn fixture_rows() -> Vec<CurrentRow> {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    for title in ["first", "second", "temporary"] {
        block_on(db.insert(
            "todos",
            doctest_support::todo_cells(title, false),
            Default::default(),
        ))
        .unwrap();
    }
    let query = db
        .prepare_query(&db.table("todos").order_by("title", OrderDirection::Asc))
        .unwrap();
    block_on(db.all(&query, ReadOpts::default())).unwrap()
}

fn snapshot(rows: Vec<CurrentRow>) -> RelationSnapshot {
    RelationSnapshot {
        root_count: rows.len(),
        rows,
        edges: Vec::new(),
    }
}

fn sender(tier: DurabilityTier) -> (SubscriptionSender, UnboundedReceiver<SubscriptionEvent>) {
    let (sender, receiver) = unbounded();
    (
        SubscriptionSender {
            sender,
            publication: Rc::new(RefCell::new(SubscriptionPublication::default())),
            requested_tier: tier,
        },
        receiver,
    )
}

fn publish(
    sender: &SubscriptionSender,
    previous: &RelationSnapshot,
    current: &RelationSnapshot,
    tier: DurabilityTier,
    settled: bool,
    reset: bool,
    materialized: bool,
) -> Result<bool, Error> {
    let before = sender.checkpoint(
        tier,
        settled,
        previous,
        &RelationSnapshotIndex::from_snapshot(previous),
    )?;
    sender.publish(
        subscription_delta_event_with_reset(tier, settled, previous, current, reset, true),
        before,
        current,
        &RelationSnapshotIndex::from_snapshot(current),
        materialized,
    )
}

#[test]
fn subscription_publication_waits_only_for_remote_tiers() {
    let rows = fixture_rows();
    let empty = snapshot(Vec::new());
    let current = snapshot(vec![rows[0].clone()]);
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        let (sender, mut receiver) = sender(tier);
        let immediate = publish(&sender, &empty, &current, tier, false, true, true).unwrap();
        assert_eq!(immediate, tier == DurabilityTier::Local);
        if immediate {
            assert!(matches!(
                receiver.try_recv().unwrap(),
                SubscriptionEvent::Delta {
                    reset: true,
                    settled: false,
                    ..
                }
            ));
        } else {
            assert!(receiver.try_recv().is_err());
            publish(&sender, &current, &current, tier, true, false, true).unwrap();
            let SubscriptionEvent::Delta {
                reset,
                added,
                settled,
                ..
            } = receiver.try_recv().unwrap()
            else {
                panic!("expected opening");
            };
            assert!(reset && settled);
            assert_eq!(added.len(), 1);
            assert_eq!(added[0].row, rows[0]);
        }
    }
}

#[test]
fn subscription_publication_coalesces_from_last_emitted_occurrences() {
    let rows = fixture_rows();
    let initial = snapshot(vec![rows[0].clone(), rows[1].clone()]);
    let intermediate = snapshot(vec![rows[2].clone(), rows[1].clone()]);
    let final_rows = snapshot(vec![rows[1].clone(), rows[0].clone()]);
    for tier in [DurabilityTier::Edge, DurabilityTier::Global] {
        let (sender, mut receiver) = sender(tier);
        publish(
            &sender,
            &snapshot(Vec::new()),
            &initial,
            tier,
            true,
            true,
            true,
        )
        .unwrap();
        receiver.try_recv().unwrap();
        publish(&sender, &initial, &intermediate, tier, false, false, true).unwrap();
        publish(
            &sender,
            &intermediate,
            &final_rows,
            tier,
            false,
            false,
            true,
        )
        .unwrap();
        assert!(receiver.try_recv().is_err());
        publish(&sender, &final_rows, &final_rows, tier, true, false, true).unwrap();
        let SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            terminal_operations,
            ..
        } = receiver.try_recv().unwrap()
        else {
            panic!("expected delta");
        };
        assert!(!reset);
        assert!(added.is_empty() && removed.is_empty() && terminal_operations.is_empty());
        assert_eq!(updated.len(), 2);
        assert_eq!((updated[0].previous_index, updated[0].index), (Some(1), 0));
        assert_eq!((updated[1].previous_index, updated[1].index), (Some(0), 1));
        assert_eq!(updated[0].row, rows[1]);
        assert_eq!(updated[1].row, rows[0]);
        assert!(sender.publication.borrow().deferred.is_none());
    }
}

#[test]
fn subscription_publication_resets_discard_withheld_history() {
    let rows = fixture_rows();
    let initial = snapshot(vec![rows[0].clone()]);
    let replacement = snapshot(vec![rows[1].clone()]);
    let (sender, mut receiver) = sender(DurabilityTier::Edge);
    publish(
        &sender,
        &snapshot(Vec::new()),
        &initial,
        DurabilityTier::Edge,
        true,
        true,
        true,
    )
    .unwrap();
    receiver.try_recv().unwrap();
    publish(
        &sender,
        &initial,
        &replacement,
        DurabilityTier::Edge,
        false,
        true,
        true,
    )
    .unwrap();
    let key = [vec![10], rows[1].row_uuid().0.as_bytes().to_vec()].concat();
    let mut event = subscription_delta_event(
        DurabilityTier::Edge,
        false,
        &replacement,
        &replacement,
        true,
    );
    if let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = &mut event
    {
        terminal_operations.push(groove::ivm::TerminalOperation {
            root_key: key.clone(),
            root_descriptor: *rows[1].encoded_record().0,
            path: Vec::new(),
            edit: groove::ivm::TerminalEdit::Move { key, index: 0 },
        });
    }
    sender
        .publish(
            event,
            None,
            &replacement,
            &RelationSnapshotIndex::from_snapshot(&replacement),
            true,
        )
        .unwrap();
    publish(
        &sender,
        &replacement,
        &replacement,
        DurabilityTier::Edge,
        true,
        false,
        true,
    )
    .unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        terminal_operations,
        ..
    } = receiver.try_recv().unwrap()
    else {
        panic!("expected reset");
    };
    assert!(reset && terminal_operations.is_empty());
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row, rows[1]);
}

#[test]
fn subscription_publication_withholds_incomplete_local_rows_and_rejects_false_settlement() {
    let rows = fixture_rows();
    let empty = snapshot(Vec::new());
    let current = snapshot(vec![rows[0].clone()]);
    let (sender, mut receiver) = sender(DurabilityTier::Local);
    // Repeated incomplete frames retain one baseline, not a growing event log.
    for _ in 0..32 {
        assert!(
            !publish(
                &sender,
                &empty,
                &current,
                DurabilityTier::Local,
                false,
                true,
                false
            )
            .unwrap()
        );
    }
    assert!(receiver.try_recv().is_err());
    assert!(
        publish(
            &sender,
            &current,
            &current,
            DurabilityTier::Local,
            true,
            false,
            false
        )
        .is_err()
    );
    publish(
        &sender,
        &current,
        &current,
        DurabilityTier::Local,
        true,
        false,
        true,
    )
    .unwrap();
    assert!(matches!(
        receiver.try_recv().unwrap(),
        SubscriptionEvent::Delta { reset: true, .. }
    ));
}

/// Use an actual maintained include to ensure a withheld child edit becomes
/// part of the emitted root replacement, without replaying that edit twice.
#[test]
fn subscription_publication_coalesces_maintained_child_edits() {
    let schema = relation_schema();
    let db = open_db(0xc7, AuthorSubject::for_test_bytes([0xc7; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("parent".to_owned()))]),
        InsertOptions {
            row_id: Some(row(1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("before".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(1).0)),
        ]),
        InsertOptions {
            row_id: Some(row(2)),
            ..Default::default()
        },
    )
    .unwrap();
    let query =
        Query::from("users").array_subquery(ArraySubquery::new("todos", "todos", "owner_id", "id"));
    let prepared = prepared(&db, &query);
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    let mut opening = block_on(stream.next_event()).unwrap();
    // The local maintained stream supplies rows; this controlled publisher
    // models a remote stream whose opening receipt has already settled.
    if let SubscriptionEvent::Delta { settled, tier, .. } = &mut opening {
        *settled = true;
        *tier = DurabilityTier::Edge;
    }
    let initial = {
        let state = stream._state.borrow();
        SubscriptionPublicationSnapshot::capture(&state.snapshot, &state.snapshot_index).unwrap()
    };
    let (sender, mut receiver) = sender(DurabilityTier::Edge);
    sender
        .publish(
            opening,
            None,
            &initial.snapshot,
            &RelationSnapshotIndex::from_snapshot(&initial.snapshot),
            true,
        )
        .unwrap();
    receiver.try_recv().unwrap();

    db.update(
        "todos",
        row(2),
        BTreeMap::from([("title".to_owned(), Value::String("after".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    let mut change = block_on(stream.next_event()).unwrap();
    if let SubscriptionEvent::Delta {
        settled,
        tier,
        terminal_operations,
        ..
    } = &mut change
    {
        assert!(
            !terminal_operations.is_empty(),
            "exercise maintained descendant edits"
        );
        *settled = false;
        *tier = DurabilityTier::Edge;
    }
    let before = sender
        .checkpoint(
            DurabilityTier::Edge,
            false,
            &initial.snapshot,
            &RelationSnapshotIndex::from_snapshot(&initial.snapshot),
        )
        .unwrap();
    {
        let state = stream._state.borrow();
        sender
            .publish(change, before, &state.snapshot, &state.snapshot_index, true)
            .unwrap();
        assert!(receiver.try_recv().is_err());
        let empty = subscription_delta_event(
            DurabilityTier::Edge,
            true,
            &state.snapshot,
            &state.snapshot,
            true,
        );
        sender
            .publish(empty, None, &state.snapshot, &state.snapshot_index, true)
            .unwrap();
    }
    let SubscriptionEvent::Delta {
        reset,
        updated,
        terminal_operations,
        ..
    } = receiver.try_recv().unwrap()
    else {
        panic!("expected coalesced root update");
    };
    assert!(!reset && terminal_operations.is_empty());
    assert_eq!(updated.len(), 1);
    let (descriptor, raw) = updated[0].encoded_record();
    let Value::Array(children) = descriptor.bind(raw).get("todos").unwrap() else {
        panic!("expected included todos");
    };
    let Value::Record(child) = &children[0] else {
        panic!("expected child record");
    };
    assert_eq!(
        child.get("title").unwrap(),
        Value::String("after".to_owned())
    );
}
