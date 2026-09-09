//! WASM integration tests for jazz-wasm.
//!
//! Run with: wasm-pack test --node

#![cfg(target_arch = "wasm32")]

use std::collections::BTreeMap;

use jazz::groove::records::{BorrowedRecord, RecordDescriptor, Value};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchema};
use jazz_wasm::{
    current_timestamp, derive_user_id, generate_id, mint_anonymous_token, mint_local_first_token,
    WasmDb,
};
use serde::{Deserialize, Serialize};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn test_generate_id() {
    let id1 = generate_id();
    let id2 = generate_id();

    // IDs should be valid UUID format
    assert_eq!(id1.len(), 36);
    assert_eq!(id2.len(), 36);

    // IDs should be unique
    assert_ne!(id1, id2);
}

#[wasm_bindgen_test]
fn test_current_timestamp() {
    let ts1 = current_timestamp();
    let ts2 = current_timestamp();

    // Timestamps should be reasonable (after 2024)
    assert!(ts1 > 1_704_067_200_000); // 2024-01-01 in milliseconds

    // Second timestamp should be >= first
    assert!(ts2 >= ts1);
}

#[wasm_bindgen_test]
fn test_identity_helpers_accept_valid_seed() {
    let seed = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    let user_id = derive_user_id(seed.to_string()).expect("derive user id");
    assert!(!user_id.is_empty());

    let token = mint_anonymous_token(
        seed.to_string(),
        "test-audience".to_string(),
        60,
        1_704_067_200,
    )
    .expect("mint anonymous token");
    assert_eq!(token.split('.').count(), 3);
}

#[wasm_bindgen_test]
fn test_identity_helpers_reject_invalid_seed() {
    let err = derive_user_id("not-base64url".to_string()).expect_err("invalid seed should fail");
    let message = err.as_string().unwrap_or_default();
    assert!(message.contains("seed"));
}

#[derive(Deserialize)]
struct DecodedRow {
    raw: Vec<u8>,
}

#[derive(Deserialize)]
struct DecodedRowBatch {
    table: String,
    descriptor: RecordDescriptor,
    rows: Vec<DecodedRow>,
}

#[derive(Deserialize)]
struct DecodedSubscriptionDelta {
    added: Vec<DecodedRowBatch>,
    updated: Vec<DecodedRowBatch>,
}

// This mirrors the private postcard input envelope exactly. It is intentionally
// local to the binding receipt: making the config a Rust API would freeze an
// implementation detail that JavaScript only ever sees as bytes.
#[derive(Serialize)]
struct OpenDbConfigFixture {
    identity: OpenDbIdentityFixture,
    row_id_seed: Option<u64>,
    history_complete: bool,
    initial_sync_flush_every: Option<u32>,
}

#[derive(Serialize)]
struct OpenDbIdentityFixture {
    node: NodeUuid,
    author: AuthorSubject,
}

fn fixture_db() -> WasmDb {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("values")
                .column("text", ColumnType::Text)
                .column("bytes", ColumnType::Bytea)
                .column("json", ColumnType::Json { schema: None })
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::True),
                ),
        )
        .build();
    let config = OpenDbConfigFixture {
        identity: OpenDbIdentityFixture {
            node: NodeUuid::from_bytes([0x51; 16]),
            author: AuthorSubject::for_test_bytes([0xa1; 16]),
        },
        row_id_seed: Some(51),
        history_complete: false,
        initial_sync_flush_every: None,
    };
    WasmDb::open_memory(
        serde_json::to_vec(&schema).expect("encode public schema JSON"),
        postcard::to_allocvec(&config).expect("encode open config postcard"),
    )
    .expect("open public WASM memory binding")
}

/// This is intentionally a binding-level receipt: wasm-bindgen owns the
/// JavaScript receiver borrow, so Rust callers cannot observe the re-entrant
/// aliasing failure that occurs when a lifecycle callback closes an otherwise
/// shared `WasmDb` receiver. `close` must therefore be callable through a
/// shared reference and remain idempotent.
#[wasm_bindgen_test]
async fn close_is_idempotent_without_an_exclusive_wasm_receiver() {
    let db = fixture_db();

    let closing = db.close();
    let error = db
        .set_non_durable_client()
        .expect_err("an operation admitted after close starts must fail");
    assert_eq!(error.as_string().as_deref(), Some("WasmDb is closed"));
    assert_eq!(await_promise(closing).await.as_bool(), Some(true));
    assert_eq!(await_promise(db.close()).await.as_bool(), Some(false));
}

#[wasm_bindgen_test]
async fn self_signed_subscriber_admission_requires_the_exact_proof() {
    let seed = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
    let app_id = "wasm-subscriber-proof";
    let now_seconds = (js_sys::Date::now() / 1_000.0) as u64;
    let token = mint_local_first_token(seed.to_owned(), app_id.to_owned(), 60, now_seconds)
        .expect("mint a current local-first proof");
    let claimed_author = serde_json::to_string(&(
        AuthorSubject::LOCAL_FIRST_ISSUER,
        derive_user_id(seed.to_owned()).expect("derive proof subject"),
    ))
    .expect("canonical proof author");
    let schema = SchemaBuilder::new().build();
    let config = OpenDbConfigFixture {
        // The proof-bearing entrypoint must replace this ordinary placeholder,
        // rather than treating an untrusted config author as privileged.
        identity: OpenDbIdentityFixture {
            node: NodeUuid::from_bytes([0x53; 16]),
            author: AuthorSubject::for_test_bytes([0xa3; 16]),
        },
        row_id_seed: Some(53),
        history_complete: false,
        initial_sync_flush_every: None,
    };
    let db = WasmDb::open_memory_with_self_signed_proof(
        serde_json::to_vec(&schema).expect("encode public schema JSON"),
        postcard::to_allocvec(&config).expect("encode open config postcard"),
        token.clone(),
        app_id.to_owned(),
        claimed_author.clone(),
    )
    .expect("open with a verified local-first proof");

    // A raw worker-port identity is always untrusted; a valid canonical
    // reserved identity must not bypass its issuer guard.
    assert!(db
        .accept_subscriber(claimed_author.as_bytes().to_vec(), JsValue::NULL)
        .is_err());

    await_promise(
        db.accept_subscriber_with_self_signed_proof(
            JsValue::NULL,
            token.clone(),
            app_id.to_owned(),
            claimed_author.clone(),
        )
        .expect("the exact verified proof admits the local worker follower"),
    )
    .await;
    assert!(db
        .accept_subscriber_with_self_signed_proof(
            JsValue::NULL,
            token,
            "wrong-app".to_owned(),
            claimed_author,
        )
        .is_err());
}

fn empty_cells() -> Vec<u8> {
    postcard::to_allocvec(&(
        RecordDescriptor::new(Vec::<(String, _)>::new()),
        Vec::<u8>::new(),
    ))
    .expect("encode empty cell envelope")
}

async fn await_promise(promise: js_sys::Promise) -> JsValue {
    JsFuture::from(promise)
        .await
        .expect("WASM promise resolves")
}

async fn next_stream_chunk(reader: &JsValue) -> JsValue {
    let read = js_sys::Reflect::get(reader, &JsValue::from_str("read"))
        .expect("ReadableStream reader has read")
        .dyn_into::<js_sys::Function>()
        .expect("reader.read is callable");
    let promise = read
        .call0(reader)
        .expect("call reader.read")
        .dyn_into::<js_sys::Promise>()
        .expect("reader.read returns promise");
    await_promise(promise).await
}

fn stream_reader(stream: JsValue) -> JsValue {
    let get_reader = js_sys::Reflect::get(&stream, &JsValue::from_str("getReader"))
        .expect("ReadableStream has getReader")
        .dyn_into::<js_sys::Function>()
        .expect("ReadableStream.getReader is callable");
    get_reader.call0(&stream).expect("obtain stream reader")
}

fn stream_delta(chunk: &JsValue) -> DecodedSubscriptionDelta {
    assert_eq!(
        js_sys::Reflect::get(chunk, &JsValue::from_str("type"))
            .expect("subscription chunk type")
            .as_string()
            .as_deref(),
        Some("delta"),
        "large-value hydration must not close or reject the JS stream"
    );
    let bytes = js_sys::Reflect::get(chunk, &JsValue::from_str("delta"))
        .expect("subscription delta")
        .dyn_into::<js_sys::Uint8Array>()
        .expect("subscription delta is Uint8Array")
        .to_vec();
    postcard::from_bytes(&bytes).expect("decode public subscription delta")
}

fn values_from_batches(batches: &[DecodedRowBatch]) -> BTreeMap<String, Value> {
    let batch = batches
        .iter()
        .find(|batch| batch.table == "values" && !batch.rows.is_empty())
        .expect("values output batch");
    let values = BorrowedRecord::new(&batch.rows[0].raw, &batch.descriptor)
        .to_values()
        .expect("decode logical public row");
    batch
        .descriptor
        .fields()
        .iter()
        .zip(values)
        .filter_map(|(field, value)| field.name.as_ref().map(|name| (name.clone(), value)))
        .collect()
}

#[wasm_bindgen_test(async)]
async fn public_wasm_large_values_hydrate_before_read_and_subscription_encoding() {
    // This is deliberately through the wasm-bindgen exported surface: public
    // schema JSON + postcard open config, streaming mutation promises,
    // serialized queries, unified read promise, and ReadableStream pulls.
    let db = fixture_db();
    let query = postcard::to_allocvec(&Query::from("values")).expect("encode query");
    let reader = stream_reader(
        db.subscribe(query.clone(), JsValue::NULL, None, JsValue::UNDEFINED)
            .expect("create public subscription"),
    );

    // The initial empty delta proves that the stream is live before the write.
    let initial = next_stream_chunk(&reader).await;
    let initial_delta = stream_delta(&initial);
    assert!(initial_delta.added.is_empty() && initial_delta.updated.is_empty());

    let row = RowUuid::from_bytes([0x71; 16]).0.as_bytes().to_vec();
    let text = format!("{}🙂", "text-".repeat(20_000));
    let bytes = (0..120_000)
        .map(|index| (index % 251) as u8)
        .collect::<Vec<_>>();
    let json = format!(r#"{{"kind":"large","body":"{}"}}"#, "json-".repeat(20_000));
    let cells = empty_cells();

    let text_upload = db
        .begin_streaming_mutation(
            "values".to_owned(),
            row.clone(),
            cells.clone(),
            "text".to_owned(),
            Some("insert".to_owned()),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("begin text upload");
    await_promise(text_upload.push(text.as_bytes().to_vec())).await;
    await_promise(text_upload.finish()).await;

    let bytes_upload = db
        .begin_streaming_mutation(
            "values".to_owned(),
            row.clone(),
            cells.clone(),
            "bytes".to_owned(),
            Some("update".to_owned()),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("begin bytea upload");
    await_promise(bytes_upload.push(bytes.clone())).await;
    await_promise(bytes_upload.finish()).await;

    let json_upload = db
        .begin_streaming_mutation(
            "values".to_owned(),
            row,
            cells,
            "json".to_owned(),
            Some("update".to_owned()),
            None,
            None,
            None,
            None,
            None,
        )
        .expect("begin JSON upload");
    await_promise(json_upload.push(json.as_bytes().to_vec())).await;
    await_promise(json_upload.finish()).await;

    // One delta per local commit is retained in source order. In particular,
    // a hydration delay is not allowed to drop the raw event, close the stream,
    // or let a later update overtake it.
    let mut subscription_values = None;
    for _ in 0..3 {
        let delta = stream_delta(&next_stream_chunk(&reader).await);
        let batches = if delta.added.is_empty() {
            &delta.updated
        } else {
            &delta.added
        };
        subscription_values = Some(values_from_batches(batches));
    }
    let subscription_values =
        subscription_values.expect("three post-write subscription deltas without stream closure");
    assert_eq!(
        subscription_values.get("text"),
        Some(&Value::String(text.clone()))
    );
    assert_eq!(
        subscription_values.get("bytes"),
        Some(&Value::Bytes(bytes.clone()))
    );
    assert_eq!(
        subscription_values.get("json"),
        Some(&Value::String(json.clone()))
    );

    let sync_opts = serde_wasm_bindgen::to_value(&serde_json::json!({ "sync": true }))
        .expect("encode synchronous read options");
    let synchronous_error = db
        .all(query.clone(), sync_opts, None, None, JsValue::UNDEFINED)
        .expect_err("synchronous public read must reject an indirect scalar");
    assert!(
        synchronous_error
            .as_string()
            .is_some_and(|message| message.contains("cannot materialize a large value")),
        "the legacy synchronous path must fail helpfully instead of leaking tag 14 to the logical decoder"
    );

    let read = db
        .all(query, JsValue::NULL, None, None, JsValue::UNDEFINED)
        .expect("start public read")
        .dyn_into::<js_sys::Promise>()
        .expect("asynchronous all returns a promise");
    let read = await_promise(read).await;
    let read_bytes = read
        .dyn_into::<js_sys::Uint8Array>()
        .expect("read resolves Uint8Array")
        .to_vec();
    let batches: Vec<DecodedRowBatch> =
        postcard::from_bytes(&read_bytes).expect("decode public read rows");
    let read_values = values_from_batches(&batches);
    assert_eq!(read_values.get("text"), Some(&Value::String(text)));
    assert_eq!(read_values.get("bytes"), Some(&Value::Bytes(bytes)));
    assert_eq!(read_values.get("json"), Some(&Value::String(json)));
}
