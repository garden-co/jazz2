//! Focused write-path benchmark for comparing plain mutations to observed mutations.
//!
//! The key reproduction case here is a content-only update on a fixed-size table.
//! That keeps result cardinality stable so the benchmark isolates the overhead of
//! maintaining a live query, rather than measuring table growth across iterations.

mod common;

use common::{create_runtime, create_session, current_timestamp, setup_data};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::WriteContext;
use jazz_tools::query_manager::types::Value;

const USER_ID: &str = "benchmark_user";

fn update_write_path_with_and_without_observer(c: &mut Criterion) {
    let mut group = c.benchmark_group("observer_write_path/update_content");

    {
        let scale = 1_000usize;
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("no_observer", scale),
            &scale,
            |b, &scale| {
                let mut core = create_runtime();
                let data = setup_data(&mut core, scale, USER_ID);
                let session = create_session(USER_ID);
                let write_ctx = WriteContext::from_session(session);
                let doc_ids = data.owned_documents;
                let mut doc_idx = 0usize;
                let mut update_counter = 0u64;

                b.iter(|| {
                    update_counter += 1;
                    let doc_id = doc_ids[doc_idx % doc_ids.len()];
                    doc_idx += 1;

                    core.update(
                        doc_id,
                        vec![
                            (
                                "content".to_string(),
                                Value::Text(format!("Updated content {}", update_counter)),
                            ),
                            (
                                "created_at".to_string(),
                                Value::Timestamp(current_timestamp() + update_counter),
                            ),
                        ],
                        Some(&write_ctx),
                    )
                    .expect("update without observer should succeed");
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("observe_all", scale),
            &scale,
            |b, &scale| {
                let mut core = create_runtime();
                let data = setup_data(&mut core, scale, USER_ID);
                let session = create_session(USER_ID);
                let write_ctx = WriteContext::from_session(session.clone());
                let doc_ids = data.owned_documents;
                let mut doc_idx = 0usize;
                let mut update_counter = 0u64;

                let _handle = core
                    .subscribe(Query::new("documents"), |_delta| {}, Some(session))
                    .expect("subscribe");
                core.immediate_tick();
                core.batched_tick();

                b.iter(|| {
                    update_counter += 1;
                    let doc_id = doc_ids[doc_idx % doc_ids.len()];
                    doc_idx += 1;

                    core.update(
                        doc_id,
                        vec![
                            (
                                "content".to_string(),
                                Value::Text(format!("Observed updated content {}", update_counter)),
                            ),
                            (
                                "created_at".to_string(),
                                Value::Timestamp(current_timestamp() + update_counter),
                            ),
                        ],
                        Some(&write_ctx),
                    )
                    .expect("update with observer should succeed");
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = update_write_path_with_and_without_observer
}
criterion_main!(benches);
