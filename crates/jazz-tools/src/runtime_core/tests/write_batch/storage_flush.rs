use super::*;

#[test]
fn rc_row_writes_do_not_touch_legacy_commit_storage() {
    let calls = Arc::new(Mutex::new(LegacyStorageCallCounts));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-no-legacy-commit-storage",
        Box::new(LegacyPersistenceObservingStorage::new(Arc::clone(&calls))),
    );

    let ((row_id, _row_values), _) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    core.update(
        row_id,
        vec![("name".into(), Value::Text("Bob".into()))],
        None,
    )
    .unwrap();
    core.delete(row_id, None).unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        LegacyStorageCallCounts,
        "row writes should persist only via row histories, not legacy branch commit storage"
    );
}

#[test]
fn rc_local_row_writes_batch_row_and_index_mutations() {
    let calls = Arc::new(Mutex::new(RowMutationCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batched-storage-mutation",
        Box::new(RowMutationObservingStorage::new(Arc::clone(&calls))),
    );

    let ((row_id, _row_values), _) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.update(
        row_id,
        vec![("name".into(), Value::Text("Bob".into()))],
        None,
    )
    .unwrap();
    core.delete(row_id, None).unwrap();

    assert_eq!(
        *calls.lock().unwrap(),
        RowMutationCallCounts {
            row_mutation_calls: 3,
            separate_index_mutation_calls: 0,
            flush_wal_calls: 0,
        },
        "local row writes should persist row history, visible heads, and index changes in one storage mutation"
    );
}

#[test]
fn rc_batched_tick_skips_flush_wal_without_storage_writes() {
    let calls = Arc::new(Mutex::new(RowMutationCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batched-no-flush",
        Box::new(RowMutationObservingStorage::new(Arc::clone(&calls))),
    );

    core.batched_tick();

    assert_eq!(
        calls.lock().unwrap().flush_wal_calls,
        0,
        "read-only batched ticks should not flush the WAL"
    );
}

#[test]
fn rc_local_write_without_outbox_still_schedules_batched_tick_for_flush() {
    let scheduler = CountingScheduler::default();
    let app_id = AppId::from_name("row-schedule-flush-without-outbox");
    let schema_manager =
        SchemaManager::new(SyncManager::new(), test_schema(), app_id, "dev", "main").unwrap();
    let mut core = new_test_core(schema_manager, MemoryStorage::new(), scheduler.clone());
    core.immediate_tick();
    let scheduled_before = scheduler.schedule_count();

    core.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    assert!(
        scheduler.schedule_count() > scheduled_before,
        "local writes without peers should still schedule batched_tick so the WAL flush barrier runs"
    );
}

#[test]
fn rc_batched_tick_flushes_wal_after_local_write() {
    let calls = Arc::new(Mutex::new(RowMutationCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batched-flush-after-write",
        Box::new(RowMutationObservingStorage::new(Arc::clone(&calls))),
    );

    core.insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.batched_tick();

    assert_eq!(
        calls.lock().unwrap().flush_wal_calls,
        1,
        "a batched tick after a local write should flush the WAL once"
    );
}

#[test]
fn rc_batched_tick_skips_flush_wal_for_query_settled_only_message() {
    let calls = Arc::new(Mutex::new(RowMutationCallCounts::default()));
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "row-batched-query-settled",
        Box::new(RowMutationObservingStorage::new(Arc::clone(&calls))),
    );

    core.push_sync_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::QuerySettled {
            query_id: crate::sync_manager::QueryId(1),
            tier: DurabilityTier::Local,
            through_seq: 1,
        },
    });
    core.batched_tick();

    assert_eq!(
        calls.lock().unwrap().flush_wal_calls,
        0,
        "query-settled notifications alone should not flush the WAL"
    );
}
