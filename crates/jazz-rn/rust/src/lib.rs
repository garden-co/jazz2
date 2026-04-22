// jazz-rn (Rust) — UniFFI surface for React Native.
//
// Note: This crate intentionally uses UniFFI proc-macros (no UDL). The RN bindings
// generator runs UniFFI in "library mode", reading this crate's metadata.
uniffi::setup_scaffolding!();

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::executor::block_on;
use serde::Deserialize;

use jazz_tools::binding_support::{
    align_query_rows_to_declared_schema, align_row_values_to_declared_schema,
    current_timestamp_ms as binding_current_timestamp_ms,
    default_read_durability_options as default_binding_read_durability_options,
    generate_id as generate_binding_id, parse_batch_id_input,
    parse_durability_tier as parse_binding_tier, parse_external_object_id, parse_query_input,
    parse_session_input, parse_write_context_input, query_rows_can_be_schema_aligned,
    serialize_local_batch_record, serialize_local_batch_records, subscription_delta_to_json,
};
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::{Session, WriteContext};
use jazz_tools::query_manager::types::{Schema, SchemaHash, TableName, Value};
use jazz_tools::runtime_core::{
    ReadDurabilityOptions, RuntimeCore, Scheduler, SubscriptionDelta, SubscriptionHandle,
};
use jazz_tools::schema_manager::{rehydrate_schema_manager_from_catalogue, AppId, SchemaManager};
use jazz_tools::storage::{SqliteStorage, Storage};
use jazz_tools::sync_manager::{
    ClientId, DurabilityTier, InboxEntry, QueryPropagation, ServerId, Source, SyncManager,
    SyncPayload,
};

// ============================================================================
// Errors
// ============================================================================

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum JazzRnError {
    #[error("invalid json: {message}")]
    InvalidJson { message: String },

    #[error("invalid uuid: {message}")]
    InvalidUuid { message: String },

    #[error("invalid persistence tier: {message}")]
    InvalidTier { message: String },

    #[error("schema error: {message}")]
    Schema { message: String },

    #[error("runtime error: {message}")]
    Runtime { message: String },

    #[error("internal error: {message}")]
    Internal { message: String },
}

fn json_err(e: serde_json::Error) -> JazzRnError {
    JazzRnError::InvalidJson {
        message: e.to_string(),
    }
}

fn runtime_err<E: std::fmt::Display>(e: E) -> JazzRnError {
    JazzRnError::Runtime {
        message: e.to_string(),
    }
}

fn panic_payload_to_string(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    if let Some(message) = payload.downcast_ref::<&str>() {
        return (*message).to_string();
    }
    "non-string panic payload".to_string()
}

fn with_panic_boundary<T, F>(context: &'static str, f: F) -> Result<T, JazzRnError>
where
    F: FnOnce() -> Result<T, JazzRnError>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(payload) => {
            let panic_message = panic_payload_to_string(payload);
            let backtrace = std::backtrace::Backtrace::force_capture();
            Err(JazzRnError::Internal {
                message: format!("panic in {context}: {panic_message}\n{backtrace}"),
            })
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "value")]
enum FfiJsonValue {
    Integer(i32),
    BigInt(i64),
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
    Bytea(String),
    Array(Vec<FfiJsonValue>),
    Row(FfiJsonRow),
    Null,
}

#[derive(Debug, Clone, Deserialize)]
struct FfiJsonRow {
    #[serde(default)]
    id: Option<ObjectId>,
    values: Vec<FfiJsonValue>,
}

fn ffi_json_err(message: impl Into<String>) -> JazzRnError {
    JazzRnError::InvalidJson {
        message: message.into(),
    }
}

fn decode_ffi_json_value(value: FfiJsonValue) -> Result<Value, JazzRnError> {
    match value {
        FfiJsonValue::Integer(value) => Ok(Value::Integer(value)),
        FfiJsonValue::BigInt(value) => Ok(Value::BigInt(value)),
        FfiJsonValue::Double(value) => Ok(Value::Double(value)),
        FfiJsonValue::Boolean(value) => Ok(Value::Boolean(value)),
        FfiJsonValue::Text(value) => Ok(Value::Text(value)),
        FfiJsonValue::Timestamp(value) => Ok(Value::Timestamp(value)),
        FfiJsonValue::Uuid(value) => Ok(Value::Uuid(value)),
        FfiJsonValue::Bytea(value) => hex::decode(value)
            .map(Value::Bytea)
            .map_err(|error| ffi_json_err(format!("invalid Bytea hex payload: {error}"))),
        FfiJsonValue::Array(values) => values
            .into_iter()
            .map(decode_ffi_json_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        FfiJsonValue::Row(row) => row
            .values
            .into_iter()
            .map(decode_ffi_json_value)
            .collect::<Result<Vec<_>, _>>()
            .map(|values| Value::Row { id: row.id, values }),
        FfiJsonValue::Null => Ok(Value::Null),
    }
}

fn decode_ffi_json_record(values_json: &str) -> Result<HashMap<String, Value>, JazzRnError> {
    let values: HashMap<String, FfiJsonValue> =
        serde_json::from_str(values_json).map_err(json_err)?;
    values
        .into_iter()
        .map(|(key, value)| decode_ffi_json_value(value).map(|value| (key, value)))
        .collect()
}

fn convert_insert_values(values_json: &str) -> Result<HashMap<String, Value>, JazzRnError> {
    decode_ffi_json_record(values_json)
}

fn convert_updates(values_json: &str) -> Result<Vec<(String, Value)>, JazzRnError> {
    let partial = decode_ffi_json_record(values_json)?;
    Ok(partial.into_iter().collect())
}

fn parse_query(query_json: &str) -> Result<Query, JazzRnError> {
    parse_query_input(query_json).map_err(|message| JazzRnError::InvalidJson { message })
}

fn parse_session(session_json: Option<String>) -> Result<Option<Session>, JazzRnError> {
    parse_session_input(session_json.as_deref())
        .map_err(|message| JazzRnError::InvalidJson { message })
}

fn parse_write_context(
    write_context_json: Option<String>,
) -> Result<Option<WriteContext>, JazzRnError> {
    parse_write_context_input(write_context_json.as_deref())
        .map_err(|message| JazzRnError::InvalidJson { message })
}

fn parse_tier(tier: &str) -> Result<DurabilityTier, JazzRnError> {
    parse_binding_tier(tier).map_err(|message| JazzRnError::InvalidTier { message })
}

fn default_read_durability_options(tier: Option<DurabilityTier>) -> ReadDurabilityOptions {
    default_binding_read_durability_options(tier)
}

fn parse_subscription_inputs(
    query_json: &str,
    session_json: Option<String>,
    tier: Option<String>,
) -> Result<(Query, Option<Session>, ReadDurabilityOptions), JazzRnError> {
    let query = parse_query(query_json)?;
    let session = parse_session(session_json)?;
    let tier = tier.as_deref().map(parse_tier).transpose()?;
    Ok((query, session, default_read_durability_options(tier)))
}

fn make_subscription_callback(
    callback: Box<dyn SubscriptionCallback>,
    declared_schema: Option<Schema>,
    table: Option<TableName>,
) -> impl Fn(SubscriptionDelta) + Send + 'static {
    move |delta: SubscriptionDelta| {
        let payload = subscription_delta_to_json(&delta, declared_schema.as_ref(), table.as_ref());
        if let Ok(json) = serde_json::to_string(&payload) {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                callback.on_update(json);
            }));
        }
    }
}

// ============================================================================
// Callbacks (JS-implemented) for scheduling + sync output
// ============================================================================

#[uniffi::export(callback_interface)]
pub trait BatchedTickCallback: Send + Sync {
    /// Called by Rust when it wants JS to call `runtime.batched_tick()`.
    fn request_batched_tick(&self);
}

#[uniffi::export(callback_interface)]
pub trait SubscriptionCallback: Send + Sync {
    /// Called when a subscription produces an update.
    fn on_update(&self, delta_json: String);
}

#[uniffi::export(callback_interface)]
pub trait AuthFailureCallback: Send + Sync {
    /// Invoked when the Rust transport receives an auth rejection from the server.
    /// `reason` is a human-readable string (e.g. "Unauthorized").
    fn on_failure(&self, reason: String);
}

// ============================================================================
// RnScheduler
// ============================================================================

#[derive(Clone, Default)]
struct RnScheduler {
    scheduled: Arc<AtomicBool>,
    callback: Arc<Mutex<Option<Box<dyn BatchedTickCallback>>>>,
}

impl RnScheduler {
    fn set_callback(&self, cb: Option<Box<dyn BatchedTickCallback>>) {
        if let Ok(mut slot) = self.callback.lock() {
            *slot = cb;
        }
    }

    fn clear_scheduled(&self) {
        self.scheduled.store(false, Ordering::SeqCst);
    }
}

impl Scheduler for RnScheduler {
    fn schedule_batched_tick(&self) {
        // Debounce: only one pending tick request at a time.
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            let called = if let Ok(guard) = self.callback.lock() {
                if let Some(cb) = guard.as_ref() {
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        cb.request_batched_tick();
                    }))
                    .is_ok()
                } else {
                    false
                }
            } else {
                false
            };

            // No callback registered; allow future scheduling attempts.
            if !called {
                self.scheduled.store(false, Ordering::SeqCst);
            }
        }
    }
}

// ============================================================================
// RnRuntime
// ============================================================================

type RnCoreType = RuntimeCore<SqliteStorage, RnScheduler>;

#[derive(uniffi::Object)]
pub struct RnRuntime {
    core: Mutex<RnCoreType>,
    upstream_server_id: Mutex<Option<ServerId>>,
    declared_schema: Schema,
    subscription_queries: Mutex<HashMap<u64, Query>>,
}

#[uniffi::export]
impl RnRuntime {
    #[uniffi::constructor]
    pub fn new(
        schema_json: String,
        app_id: String,
        jazz_env: String,
        user_branch: String,
        tier: Option<String>,
        data_path: Option<String>,
    ) -> Result<Arc<Self>, JazzRnError> {
        with_panic_boundary("new", || {
            let schema: Schema = serde_json::from_str(&schema_json).map_err(json_err)?;
            let declared_schema = schema.clone();

            let persistence_tier = tier.as_deref().map(parse_tier).transpose()?;

            let mut sync_manager = SyncManager::new();
            if let Some(t) = persistence_tier {
                sync_manager = sync_manager.with_durability_tier(t);
            }

            let app_id_obj =
                AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id));
            let mut schema_manager =
                SchemaManager::new(sync_manager, schema, app_id_obj, &jazz_env, &user_branch)
                    .map_err(|e| JazzRnError::Schema {
                        message: format!("{:?}", e),
                    })?;

            let resolved_data_path = data_path.unwrap_or_else(|| {
                let sanitized_app_id: String = app_id
                    .chars()
                    .map(|c| {
                        if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect();
                let mut default_path = std::env::temp_dir();
                default_path.push(format!("{sanitized_app_id}.sqlite"));
                default_path.to_string_lossy().into_owned()
            });
            let storage =
                SqliteStorage::open(&resolved_data_path).map_err(|e| JazzRnError::Runtime {
                    message: format!(
                        "Failed to open SQLite storage at '{}': {:?}",
                        resolved_data_path, e
                    ),
                })?;

            // Load previously-persisted schema history, permissions bundle, and lens
            // catalogue entries from storage into the in-memory schema manager so
            // offline cold-starts can decode and serve locally stored rows.
            if let Err(error) =
                rehydrate_schema_manager_from_catalogue(&mut schema_manager, &storage, app_id_obj)
            {
                eprintln!(
                    "jazz-rn: failed to rehydrate schema manager from catalogue storage for app {app_id_obj}: {error}"
                );
            }

            let scheduler = RnScheduler::default();

            let mut core = RuntimeCore::new(schema_manager, storage, scheduler);
            core.persist_schema();

            Ok(Arc::new(Self {
                core: Mutex::new(core),
                upstream_server_id: Mutex::new(None),
                declared_schema,
                subscription_queries: Mutex::new(HashMap::new()),
            }))
        })
    }

    /// Register a JS callback that schedules `batched_tick()` calls.
    pub fn on_batched_tick_needed(
        &self,
        callback: Option<Box<dyn BatchedTickCallback>>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("on_batched_tick_needed", || {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.scheduler_mut().set_callback(callback);
            Ok(())
        })
    }

    /// Run a batched tick. JS should call this when asked via `on_batched_tick_needed`.
    pub fn batched_tick(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("batched_tick", || {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.scheduler_mut().clear_scheduled();
            core.batched_tick();
            Ok(())
        })
    }

    // =========================================================================
    // CRUD
    // =========================================================================

    pub fn insert(
        &self,
        table: String,
        values_json: String,
        object_id: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("insert", || {
            let named_values = convert_insert_values(&values_json)?;
            let object_id = parse_external_object_id(object_id.as_deref())
                .map_err(|message| JazzRnError::InvalidUuid { message })?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let ((id, row_values), batch_id) = core
                .insert_with_id(&table, named_values, object_id, None)
                .map_err(runtime_err)?;
            let row_values = align_row_values_to_declared_schema(
                &self.declared_schema,
                core.current_schema(),
                &TableName::new(table.clone()),
                row_values,
            );
            serde_json::to_string(&serde_json::json!({
                "id": id.uuid().to_string(),
                "values": row_values,
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("insert serialization failed: {e}"),
            })
        })
    }

    pub fn insert_with_session(
        &self,
        table: String,
        values_json: String,
        write_context_json: Option<String>,
        object_id: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("insert_with_session", || {
            let named_values = convert_insert_values(&values_json)?;
            let write_context = parse_write_context(write_context_json)?;
            let object_id = parse_external_object_id(object_id.as_deref())
                .map_err(|message| JazzRnError::InvalidUuid { message })?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let ((id, row_values), batch_id) = core
                .insert_with_id(&table, named_values, object_id, write_context.as_ref())
                .map_err(runtime_err)?;
            let row_values = align_row_values_to_declared_schema(
                &self.declared_schema,
                core.current_schema(),
                &TableName::new(table.clone()),
                row_values,
            );
            serde_json::to_string(&serde_json::json!({
                "id": id.uuid().to_string(),
                "values": row_values,
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("insert serialization failed: {e}"),
            })
        })
    }

    pub fn update(&self, object_id: String, values_json: String) -> Result<String, JazzRnError> {
        with_panic_boundary("update", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let updates = convert_updates(&values_json)?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let batch_id = core.update(oid, updates, None).map_err(runtime_err)?;
            serde_json::to_string(&serde_json::json!({
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("update serialization failed: {e}"),
            })
        })
    }

    pub fn update_with_session(
        &self,
        object_id: String,
        values_json: String,
        write_context_json: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("update_with_session", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let updates = convert_updates(&values_json)?;
            let write_context = parse_write_context(write_context_json)?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let batch_id = core
                .update(oid, updates, write_context.as_ref())
                .map_err(runtime_err)?;
            serde_json::to_string(&serde_json::json!({
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("update serialization failed: {e}"),
            })
        })
    }

    #[uniffi::method(name = "delete")]
    pub fn delete_row(&self, object_id: String) -> Result<String, JazzRnError> {
        with_panic_boundary("delete", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let batch_id = core.delete(oid, None).map_err(runtime_err)?;
            serde_json::to_string(&serde_json::json!({
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("delete serialization failed: {e}"),
            })
        })
    }

    #[uniffi::method(name = "deleteWithSession")]
    pub fn delete_with_session(
        &self,
        object_id: String,
        write_context_json: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("delete_with_session", || {
            let uuid = uuid::Uuid::parse_str(&object_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let oid = ObjectId::from_uuid(uuid);
            let write_context = parse_write_context(write_context_json)?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let batch_id = core
                .delete(oid, write_context.as_ref())
                .map_err(runtime_err)?;
            serde_json::to_string(&serde_json::json!({
                "batchId": batch_id.to_string(),
            }))
            .map_err(|e| JazzRnError::Internal {
                message: format!("delete serialization failed: {e}"),
            })
        })
    }

    // =========================================================================
    // Queries
    // =========================================================================

    /// One-shot query returning a JSON string:
    /// `[{ "id": "<uuid>", "values": [ {type, value}, ... ] }, ...]`.
    pub fn query(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
    ) -> Result<String, JazzRnError> {
        with_panic_boundary("query", || {
            let query = parse_query(&query_json)?;
            let query_for_alignment = query.clone();
            let session = parse_session(session_json)?;
            let tier = tier.as_deref().map(parse_tier).transpose()?;

            // NOTE: query() triggers immediate_tick() internally.
            // We then block for the first callback result to be delivered.
            let (fut, runtime_schema) = {
                let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?;
                (
                    core.query_with_propagation(
                        query,
                        session,
                        default_read_durability_options(tier),
                        QueryPropagation::Full,
                    ),
                    core.current_schema().clone(),
                )
            };
            let results = block_on(fut).map_err(runtime_err)?;
            let results = align_query_rows_to_declared_schema(
                &self.declared_schema,
                &runtime_schema,
                &query_for_alignment,
                results,
            );

            let rows_json: Vec<serde_json::Value> = results
                .into_iter()
                .map(|(id, values)| {
                    serde_json::json!({
                        "id": id.uuid().to_string(),
                        "values": values,
                    })
                })
                .collect();

            serde_json::to_string(&rows_json).map_err(json_err)
        })
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    pub fn subscribe(
        &self,
        query_json: String,
        callback: Box<dyn SubscriptionCallback>,
        session_json: Option<String>,
        tier: Option<String>,
    ) -> Result<u64, JazzRnError> {
        with_panic_boundary("subscribe", || {
            let (query, session, durability) =
                parse_subscription_inputs(&query_json, session_json, tier)?;
            let alignment_table = if query_rows_can_be_schema_aligned(&query) {
                Some(query.table)
            } else {
                None
            };
            let callback = make_subscription_callback(
                callback,
                alignment_table
                    .as_ref()
                    .map(|_| self.declared_schema.clone()),
                alignment_table,
            );

            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;

            let handle = core
                .subscribe_with_durability_and_propagation(
                    query,
                    callback,
                    session,
                    durability,
                    QueryPropagation::Full,
                )
                .map_err(runtime_err)?;

            Ok(handle.0)
        })
    }

    pub fn unsubscribe(&self, handle: u64) -> Result<(), JazzRnError> {
        with_panic_boundary("unsubscribe", || {
            self.subscription_queries
                .lock()
                .map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?
                .remove(&handle);
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.unsubscribe(SubscriptionHandle(handle));
            Ok(())
        })
    }

    /// Phase 1 of 2-phase subscribe: allocate a handle and store query params.
    pub fn create_subscription(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
    ) -> Result<u64, JazzRnError> {
        with_panic_boundary("create_subscription", || {
            let (query, session, durability) =
                parse_subscription_inputs(&query_json, session_json, tier)?;
            let query_for_alignment = query.clone();

            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;

            let handle =
                core.create_subscription(query, session, durability, QueryPropagation::Full);
            drop(core);

            if query_rows_can_be_schema_aligned(&query_for_alignment) {
                self.subscription_queries
                    .lock()
                    .map_err(|_| JazzRnError::Internal {
                        message: "lock poisoned".into(),
                    })?
                    .insert(handle.0, query_for_alignment);
            }

            Ok(handle.0)
        })
    }

    /// Phase 2 of 2-phase subscribe: compile, register, sync, attach callback, tick.
    pub fn execute_subscription(
        &self,
        handle: u64,
        callback: Box<dyn SubscriptionCallback>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("execute_subscription", || {
            let alignment_table = self
                .subscription_queries
                .lock()
                .map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?
                .get(&handle)
                .map(|query| query.table);
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let callback = make_subscription_callback(
                callback,
                alignment_table
                    .as_ref()
                    .map(|_| self.declared_schema.clone()),
                alignment_table,
            );

            core.execute_subscription(SubscriptionHandle(handle), callback)
                .map_err(runtime_err)?;

            Ok(())
        })
    }

    // =========================================================================
    // Sync
    // =========================================================================

    pub fn on_sync_message_received(&self, message_json: String) -> Result<(), JazzRnError> {
        with_panic_boundary("on_sync_message_received", || {
            let payload: SyncPayload = serde_json::from_str(&message_json).map_err(json_err)?;
            let entry = InboxEntry {
                source: Source::Server(ServerId::new()),
                payload,
            };
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.park_sync_message(entry);
            Ok(())
        })
    }

    pub fn on_sync_message_received_from_client(
        &self,
        client_id: String,
        message_json: String,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("on_sync_message_received_from_client", || {
            let uuid = uuid::Uuid::parse_str(&client_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let cid = ClientId(uuid);
            let payload: SyncPayload = serde_json::from_str(&message_json).map_err(json_err)?;

            let entry = InboxEntry {
                source: Source::Client(cid),
                payload,
            };
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.park_sync_message(entry);
            Ok(())
        })
    }

    pub fn add_server(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("add_server", || {
            let server_id = {
                let mut slot =
                    self.upstream_server_id
                        .lock()
                        .map_err(|_| JazzRnError::Internal {
                            message: "lock poisoned".into(),
                        })?;
                if let Some(server_id) = *slot {
                    server_id
                } else {
                    let server_id = ServerId::new();
                    *slot = Some(server_id);
                    server_id
                }
            };

            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.remove_server(server_id);
            core.add_server(server_id);
            Ok(())
        })
    }

    pub fn remove_server(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("remove_server", || {
            let server_id = {
                let slot = self
                    .upstream_server_id
                    .lock()
                    .map_err(|_| JazzRnError::Internal {
                        message: "lock poisoned".into(),
                    })?;
                *slot
            };
            let Some(server_id) = server_id else {
                return Ok(());
            };
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.remove_server(server_id);
            Ok(())
        })
    }

    pub fn add_client(&self) -> Result<String, JazzRnError> {
        with_panic_boundary("add_client", || {
            let client_id = ClientId::new();
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.add_client(client_id, None);
            Ok(client_id.0.to_string())
        })
    }

    pub fn set_client_role(&self, client_id: String, role: String) -> Result<(), JazzRnError> {
        with_panic_boundary("set_client_role", || {
            use jazz_tools::sync_manager::ClientRole;

            let uuid = uuid::Uuid::parse_str(&client_id).map_err(|e| JazzRnError::InvalidUuid {
                message: e.to_string(),
            })?;
            let cid = ClientId(uuid);

            let client_role = match role.as_str() {
                "user" => ClientRole::User,
                "admin" => ClientRole::Admin,
                "peer" => ClientRole::Peer,
                _ => {
                    return Err(JazzRnError::Runtime {
                        message: format!(
                            "Invalid role '{}'. Must be 'user', 'admin', or 'peer'.",
                            role
                        ),
                    });
                }
            };

            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.set_client_role_by_name(cid, client_role);
            Ok(())
        })
    }

    // =========================================================================
    // Schema/state access
    // =========================================================================

    pub fn get_schema_hash(&self) -> Result<String, JazzRnError> {
        with_panic_boundary("get_schema_hash", || {
            let core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let schema = core.current_schema();
            Ok(SchemaHash::compute(schema).to_string())
        })
    }

    pub fn flush(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("flush", || {
            let core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.flush_storage();
            Ok(())
        })
    }

    pub fn load_local_batch_record(&self, batch_id: String) -> Result<Option<String>, JazzRnError> {
        with_panic_boundary("load_local_batch_record", || {
            let batch_id = parse_batch_id_input(&batch_id)
                .map_err(|message| JazzRnError::InvalidUuid { message })?;
            let core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let record = core.local_batch_record(batch_id).map_err(runtime_err)?;
            record
                .map(|record| {
                    serde_json::to_string(&serialize_local_batch_record(&record)).map_err(|error| {
                        JazzRnError::Internal {
                            message: format!(
                                "load_local_batch_record serialization failed: {error}"
                            ),
                        }
                    })
                })
                .transpose()
        })
    }

    pub fn load_local_batch_records(&self) -> Result<String, JazzRnError> {
        with_panic_boundary("load_local_batch_records", || {
            let core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            let records = core.local_batch_records().map_err(runtime_err)?;
            serde_json::to_string(&serialize_local_batch_records(&records)).map_err(|error| {
                JazzRnError::Internal {
                    message: format!("load_local_batch_records serialization failed: {error}"),
                }
            })
        })
    }

    pub fn request_batch_settlements(&self, batch_ids: Vec<String>) -> Result<(), JazzRnError> {
        with_panic_boundary("request_batch_settlements", || {
            let batch_ids = batch_ids
                .into_iter()
                .map(|batch_id| {
                    parse_batch_id_input(&batch_id)
                        .map_err(|message| JazzRnError::InvalidUuid { message })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.request_batch_settlements(batch_ids);
            Ok(())
        })
    }

    pub fn drain_rejected_batch_ids(&self) -> Result<Vec<String>, JazzRnError> {
        with_panic_boundary("drain_rejected_batch_ids", || {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            Ok(core
                .drain_rejected_batch_ids()
                .into_iter()
                .map(|batch_id| batch_id.to_string())
                .collect())
        })
    }

    pub fn acknowledge_rejected_batch(&self, batch_id: String) -> Result<bool, JazzRnError> {
        with_panic_boundary("acknowledge_rejected_batch", || {
            let batch_id = parse_batch_id_input(&batch_id)
                .map_err(|message| JazzRnError::InvalidUuid { message })?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.acknowledge_rejected_batch(batch_id)
                .map_err(runtime_err)
        })
    }

    pub fn seal_batch(&self, batch_id: String) -> Result<(), JazzRnError> {
        with_panic_boundary("seal_batch", || {
            let batch_id = parse_batch_id_input(&batch_id)
                .map_err(|message| JazzRnError::InvalidUuid { message })?;
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.seal_batch(batch_id).map_err(runtime_err)
        })
    }

    /// Flush and close the underlying storage, releasing filesystem locks.
    pub fn close(&self) -> Result<(), JazzRnError> {
        with_panic_boundary("close", || {
            let core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.flush_storage();
            core.storage().close().map_err(runtime_err)?;
            Ok(())
        })
    }

    /// Connect to a Jazz server over WebSocket.
    ///
    /// Parses `auth_json` into `AuthConfig`, wires a `TransportManager` into
    /// `RuntimeCore`, and spawns the manager loop on a dedicated Tokio thread.
    pub fn connect(&self, url: String, auth_json: String) -> Result<(), JazzRnError> {
        with_panic_boundary("connect", || {
            let auth: jazz_tools::transport_manager::AuthConfig =
                serde_json::from_str(&auth_json).map_err(json_err)?;
            let scheduler = self
                .core
                .lock()
                .map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?
                .scheduler()
                .clone();
            let tick = RnTickNotifier { scheduler };
            let manager = {
                let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                    message: "lock poisoned".into(),
                })?;
                jazz_tools::runtime_core::install_transport::<
                    _,
                    _,
                    jazz_tools::ws_stream::NativeWsStream,
                    _,
                >(&mut core, url, auth, tick)
            };
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("tokio rt");
                rt.block_on(manager.run());
            });
            Ok(())
        })
    }

    /// Disconnect from the Jazz server and drop the transport handle.
    pub fn disconnect(&self) {
        if let Ok(mut core) = self.core.lock() {
            if let Some(handle) = core.transport() {
                handle.disconnect();
            }
            core.clear_transport();
        }
    }

    /// Push updated auth credentials into the live transport.
    pub fn update_auth(&self, auth_json: String) -> Result<(), JazzRnError> {
        with_panic_boundary("update_auth", || {
            let auth: jazz_tools::transport_manager::AuthConfig =
                serde_json::from_str(&auth_json).map_err(json_err)?;
            if let Ok(core) = self.core.lock() {
                if let Some(handle) = core.transport() {
                    handle.update_auth(auth);
                }
            }
            Ok(())
        })
    }

    /// Register a callback that fires when the transport receives an auth
    /// rejection from the server during the WS handshake.
    pub fn on_auth_failure(
        &self,
        callback: Box<dyn AuthFailureCallback>,
    ) -> Result<(), JazzRnError> {
        with_panic_boundary("on_auth_failure", || {
            let mut core = self.core.lock().map_err(|_| JazzRnError::Internal {
                message: "lock poisoned".into(),
            })?;
            core.set_auth_failure_callback(move |reason| {
                callback.on_failure(reason);
            });
            Ok(())
        })
    }
}

// ============================================================================
// RnTickNotifier
// ============================================================================

/// `TickNotifier` implementation for the React Native (UniFFI) runtime.
///
/// Holds a clone of `RnScheduler` and calls `schedule_batched_tick()` whenever
/// the transport layer needs to wake up `batched_tick`.
struct RnTickNotifier {
    scheduler: RnScheduler,
}

impl jazz_tools::transport_manager::TickNotifier for RnTickNotifier {
    fn notify(&self) {
        self.scheduler.schedule_batched_tick();
    }
}

#[cfg(test)]
mod tests {
    use jazz_tools::binding_support::{
        align_query_rows_to_declared_schema, align_values_to_declared_schema,
        query_rows_can_be_schema_aligned,
    };
    use jazz_tools::object::ObjectId;
    use jazz_tools::query_manager::query::Query;
    use jazz_tools::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, Schema, SchemaBuilder, TableName, TableSchema,
        Value,
    };

    fn declared_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("description", ColumnType::Text),
            )
            .build()
    }

    fn runtime_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("description", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("title", ColumnType::Text),
            )
            .build()
    }

    #[test]
    fn query_rows_are_reordered_back_to_declared_schema() {
        let rows = vec![(
            ObjectId::new(),
            vec![
                Value::Text("note".to_string()),
                Value::Boolean(false),
                Value::Text("buy milk".to_string()),
            ],
        )];
        let query = Query::new("todos");

        let aligned = align_query_rows_to_declared_schema(
            &declared_todo_schema(),
            &runtime_todo_schema(),
            &query,
            rows,
        );

        assert_eq!(
            aligned[0].1,
            vec![
                Value::Text("buy milk".to_string()),
                Value::Boolean(false),
                Value::Text("note".to_string()),
            ]
        );
    }

    #[test]
    fn descriptor_values_are_reordered_back_to_declared_schema() {
        let runtime_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("description", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
            ColumnDescriptor::new("title", ColumnType::Text),
        ]);

        let aligned = align_values_to_declared_schema(
            &declared_todo_schema(),
            &TableName::new("todos"),
            &runtime_descriptor,
            vec![
                Value::Text("note".to_string()),
                Value::Boolean(true),
                Value::Text("ship fix".to_string()),
            ],
        );

        assert_eq!(
            aligned,
            vec![
                Value::Text("ship fix".to_string()),
                Value::Boolean(true),
                Value::Text("note".to_string()),
            ]
        );
    }

    #[test]
    fn simple_queries_are_schema_alignable() {
        assert!(query_rows_can_be_schema_aligned(&Query::new("todos")));
    }
}

// ============================================================================
// Module-level utilities
// ============================================================================

#[uniffi::export]
pub fn generate_id() -> String {
    generate_binding_id()
}

#[uniffi::export]
pub fn current_timestamp_ms() -> i64 {
    binding_current_timestamp_ms()
}

/// Mint a local-first JWT from a base64url-encoded 32-byte seed.
///
/// Returns a signed JWT that can be used as a bearer token for local-first auth.
/// `audience` should be the app ID (UUID) or a human-readable app name.
/// `ttl_seconds` controls token lifetime (e.g. 3600 for one hour).
#[uniffi::export]
pub fn mint_local_first_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: i64,
) -> Result<String, JazzRnError> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(&seed_b64)
        .map_err(|e| JazzRnError::Internal {
            message: format!("invalid base64 seed: {e}"),
        })?;
    let seed: [u8; 32] = bytes.try_into().map_err(|_| JazzRnError::Internal {
        message: "seed must be exactly 32 bytes".to_string(),
    })?;
    jazz_tools::identity::mint_jazz_self_signed_token(
        &seed,
        jazz_tools::identity::LOCAL_FIRST_ISSUER,
        &audience,
        ttl_seconds as u64,
    )
    .map_err(|e| JazzRnError::Internal { message: e })
}
