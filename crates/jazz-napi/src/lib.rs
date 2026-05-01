//! jazz-napi — Native Node.js bindings for Jazz.
//!
//! Provides `NapiRuntime` wrapping `RuntimeCore<SqliteStorage>` via napi-rs.
//! Exposed as the `jazz-napi` npm package for server-side TypeScript apps.
//!
//! # Architecture
//!
//! - `SqliteStorage` provides persistent on-disk storage
//! - `NapiScheduler` implements `Scheduler` using `ThreadsafeFunction` to schedule
//!   `batched_tick()` on the Node.js event loop (debounced)
//! - `NapiRuntime` wraps `Arc<Mutex<RuntimeCore<...>>>`
//! - Server sync uses the Rust-owned WebSocket transport via `connect()`

use napi::bindgen_prelude::*;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jazz_tools::binding_support::{
    align_query_rows_to_declared_schema, align_row_values_to_declared_schema, current_timestamp_ms,
    generate_id as generate_binding_id, parse_batch_id_input,
    parse_durability_tier as parse_binding_tier, parse_external_object_id, parse_query_input,
    parse_runtime_schema_input, parse_session_input, parse_write_context_input,
    query_rows_can_be_schema_aligned, serialize_local_batch_record, serialize_local_batch_records,
    subscription_delta_to_json,
};
use jazz_tools::identity;
use jazz_tools::middleware::AuthConfig;
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::{Session, WriteContext};
use jazz_tools::query_manager::types::{Schema, SchemaHash, TableName, Value};
use jazz_tools::runtime_core::{
    QueryLocalOverlay, ReadDurabilityOptions, RuntimeCore, Scheduler, SubscriptionDelta,
    SubscriptionHandle,
};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::server::{
    CatalogueAuthorityMode, HostedServer as JazzHostedServer, ServerBuilder, StorageBackend,
    TestingServer as JazzTestingServer,
};
use jazz_tools::storage::{MemoryStorage, SqliteStorage, Storage};
use jazz_tools::sync_manager::QueryPropagation;
use jazz_tools::sync_manager::{
    ClientId, DurabilityTier, InboxEntry, ServerId, Source, SyncManager, SyncPayload,
};

fn convert_updates(values: HashMap<String, Value>) -> Vec<(String, Value)> {
    values.into_iter().collect()
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", content = "value")]
enum FfiValue {
    Integer(i32),
    BigInt(i64),
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
    Bytea(#[serde(with = "serde_bytes")] Vec<u8>),
    Array(Vec<FfiValue>),
    Row(FfiRow),
    Null,
}

#[derive(Debug, Clone, Deserialize)]
struct FfiRow {
    #[serde(default)]
    id: Option<ObjectId>,
    values: Vec<FfiValue>,
}

impl From<FfiValue> for Value {
    fn from(value: FfiValue) -> Self {
        match value {
            FfiValue::Integer(value) => Value::Integer(value),
            FfiValue::BigInt(value) => Value::BigInt(value),
            FfiValue::Double(value) => Value::Double(value),
            FfiValue::Boolean(value) => Value::Boolean(value),
            FfiValue::Text(value) => Value::Text(value),
            FfiValue::Timestamp(value) => Value::Timestamp(value),
            FfiValue::Uuid(value) => Value::Uuid(value),
            FfiValue::Bytea(value) => Value::Bytea(value),
            FfiValue::Array(values) => Value::Array(values.into_iter().map(Value::from).collect()),
            FfiValue::Row(row) => Value::Row {
                id: row.id,
                values: row.values.into_iter().map(Value::from).collect(),
            },
            FfiValue::Null => Value::Null,
        }
    }
}

pub struct FfiRecordArg(HashMap<String, Value>);

impl TypeName for FfiRecordArg {
    fn type_name() -> &'static str {
        "Record<string, unknown>"
    }

    fn value_type() -> ValueType {
        ValueType::Object
    }
}

impl FromNapiValue for FfiRecordArg {
    unsafe fn from_napi_value(
        env: napi::sys::napi_env,
        napi_val: napi::sys::napi_value,
    ) -> Result<Self> {
        let env = Env::from_raw(env);
        let unknown = unsafe { Unknown::from_napi_value(env.raw(), napi_val)? };
        let values = env
            .from_js_value::<HashMap<String, FfiValue>, _>(unknown)
            .map_err(|error| napi::Error::from_reason(format!("Invalid values: {}", error)))?;
        Ok(Self(
            values
                .into_iter()
                .map(|(key, value)| (key, Value::from(value)))
                .collect(),
        ))
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct QueryExecutionOptionsWire {
    propagation: Option<String>,
    local_updates: Option<String>,
    transaction_overlay: Option<QueryTransactionOverlayWire>,
}

#[derive(Debug, Clone, Deserialize)]
struct QueryTransactionOverlayWire {
    batch_id: String,
    branch_name: String,
    row_ids: Vec<String>,
}

fn parse_read_durability_options(
    tier: Option<String>,
    options_json: Option<String>,
) -> napi::Result<(
    ReadDurabilityOptions,
    QueryPropagation,
    Option<QueryLocalOverlay>,
)> {
    let parsed_tier = tier
        .as_deref()
        .map(parse_binding_tier)
        .transpose()
        .map_err(napi::Error::from_reason)?;
    let Some(raw) = options_json else {
        return Ok((
            ReadDurabilityOptions {
                tier: parsed_tier,
                local_updates: jazz_tools::query_manager::manager::LocalUpdates::Immediate,
            },
            QueryPropagation::Full,
            None,
        ));
    };

    let options: QueryExecutionOptionsWire = serde_json::from_str(&raw)
        .map_err(|err| napi::Error::from_reason(format!("Invalid query options JSON: {err}")))?;

    let propagation = match options.propagation.as_deref() {
        None | Some("full") => Ok(QueryPropagation::Full),
        Some("local-only") => Ok(QueryPropagation::LocalOnly),
        Some(other) => Err(napi::Error::from_reason(format!(
            "Invalid propagation '{other}'. Must be 'full' or 'local-only'."
        ))),
    }?;

    let local_updates = match options.local_updates.as_deref() {
        None | Some("immediate") => Ok(jazz_tools::query_manager::manager::LocalUpdates::Immediate),
        Some("deferred") => Ok(jazz_tools::query_manager::manager::LocalUpdates::Deferred),
        Some(other) => Err(napi::Error::from_reason(format!(
            "Invalid localUpdates '{other}'. Must be 'immediate' or 'deferred'."
        ))),
    }?;

    let overlay = match options.transaction_overlay {
        None => None,
        Some(overlay) => Some(QueryLocalOverlay {
            batch_id: parse_batch_id_input(&overlay.batch_id).map_err(napi::Error::from_reason)?,
            branch_name: jazz_tools::object::BranchName::new(&overlay.branch_name),
            row_ids: overlay
                .row_ids
                .into_iter()
                .map(|row_id| {
                    parse_external_object_id(Some(&row_id))
                        .and_then(|maybe| maybe.ok_or_else(|| "missing query row id".to_string()))
                        .map_err(napi::Error::from_reason)
                })
                .collect::<napi::Result<Vec<_>>>()?,
        }),
    };

    Ok((
        ReadDurabilityOptions {
            tier: parsed_tier,
            local_updates,
        },
        propagation,
        overlay,
    ))
}

fn parse_node_durability_tiers(tier: Option<&str>) -> napi::Result<Vec<DurabilityTier>> {
    let Some(raw) = tier else {
        return Ok(Vec::new());
    };
    Ok(vec![parse_tier(raw)?])
}

fn parse_node_durability_tier(tier: Option<String>) -> napi::Result<Vec<DurabilityTier>> {
    parse_node_durability_tiers(tier.as_deref())
}

fn open_sqlite_storage(data_path: &str) -> napi::Result<SqliteStorage> {
    SqliteStorage::open(data_path)
        .map_err(|e| napi::Error::from_reason(format!("Failed to open storage: {:?}", e)))
}

// ============================================================================
fn parse_tier(tier: &str) -> napi::Result<DurabilityTier> {
    parse_binding_tier(tier).map_err(napi::Error::from_reason)
}

fn parse_optional_sequence(sequence: Option<f64>) -> napi::Result<Option<u64>> {
    let Some(sequence) = sequence else {
        return Ok(None);
    };
    if !sequence.is_finite() || sequence < 0.0 || sequence.fract() != 0.0 {
        return Err(napi::Error::from_reason(
            "Invalid stream sequence: expected a non-negative integer",
        ));
    }
    if sequence > u64::MAX as f64 {
        return Err(napi::Error::from_reason(
            "Invalid stream sequence: value exceeds u64 range",
        ));
    }
    Ok(Some(sequence as u64))
}

fn parse_query(json: &str) -> napi::Result<Query> {
    parse_query_input(json).map_err(napi::Error::from_reason)
}

fn parse_session_json(session_json: Option<String>) -> napi::Result<Option<Session>> {
    parse_session_input(session_json.as_deref())
        .map_err(|err| napi::Error::from_reason(format!("Invalid session JSON: {}", err)))
}

fn parse_write_context_json(
    write_context_json: Option<String>,
) -> napi::Result<Option<WriteContext>> {
    parse_write_context_input(write_context_json.as_deref())
        .map_err(|err| napi::Error::from_reason(format!("Invalid write context JSON: {}", err)))
}

fn parse_subscription_inputs(
    query_json: &str,
    session_json: Option<String>,
    tier: Option<String>,
    options_json: Option<String>,
) -> napi::Result<(
    Query,
    Option<Session>,
    ReadDurabilityOptions,
    QueryPropagation,
)> {
    let query = parse_query(query_json)?;
    let session = parse_session_json(session_json)?;
    let (durability, propagation, _overlay) = parse_read_durability_options(tier, options_json)?;
    Ok((query, session, durability, propagation))
}

fn parse_testing_server_start_options(
    options: Option<JsonValue>,
) -> napi::Result<TestingServerStartOptions> {
    match options {
        None | Some(JsonValue::Null) => Ok(TestingServerStartOptions::default()),
        Some(value) => serde_json::from_value(value).map_err(|error| {
            napi::Error::from_reason(format!("Invalid TestingServer options: {error}"))
        }),
    }
}

fn make_subscription_callback(
    tsfn: ThreadsafeFunction<serde_json::Value>,
    declared_schema: Option<Schema>,
    table: Option<TableName>,
) -> impl Fn(SubscriptionDelta) + Send + 'static {
    move |delta: SubscriptionDelta| {
        tsfn.call(
            Ok(subscription_delta_to_json(
                &delta,
                declared_schema.as_ref(),
                table.as_ref(),
            )),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}

fn napi_decode_seed(seed_b64: &str) -> napi::Result<[u8; 32]> {
    let bytes = URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|e| napi::Error::from_reason(format!("Invalid base64url seed: {}", e)))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("Seed must be exactly 32 bytes"))?;
    Ok(arr)
}

// ============================================================================
// NapiScheduler
// ============================================================================

type NapiCoreType = RuntimeCore<Box<dyn Storage + Send>, NapiScheduler>;

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestingServerStartOptions {
    app_id: Option<String>,
    port: Option<u16>,
    data_dir: Option<String>,
    persistent_storage: Option<bool>,
    admin_secret: Option<String>,
    backend_secret: Option<String>,
    jwks_url: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DevServerStartOptions {
    app_id: String,
    port: Option<u16>,
    data_dir: Option<String>,
    in_memory: Option<bool>,
    jwks_url: Option<String>,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
    allow_local_first_auth: Option<bool>,
    catalogue_authority: Option<String>,
    catalogue_authority_url: Option<String>,
    catalogue_authority_admin_secret: Option<String>,
    telemetry_collector_url: Option<String>,
}

fn parse_dev_server_start_options(options: JsonValue) -> napi::Result<DevServerStartOptions> {
    serde_json::from_value(options)
        .map_err(|error| napi::Error::from_reason(format!("Invalid DevServer options: {error}")))
}

static DEV_SERVER_OTEL_PROVIDER: OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    OnceLock::new();
static DEV_SERVER_TELEMETRY_INIT: OnceLock<()> = OnceLock::new();

fn init_dev_server_telemetry(collector_url: Option<&str>) {
    let Some(collector_url) = collector_url else {
        return;
    };

    DEV_SERVER_TELEMETRY_INIT.get_or_init(|| {
        use tracing_subscriber::layer::SubscriberExt as _;

        let endpoint = jazz_tools::otel::normalize_otlp_traces_endpoint(collector_url);
        let provider = jazz_tools::otel::init_tracer_provider_with_endpoint(
            "jazz-dev-server",
            Some(&endpoint),
        );
        let otel_layer = jazz_tools::otel::layer(&provider);
        let filter = tracing_subscriber::EnvFilter::from_default_env()
            .add_directive("jazz_tools=trace".parse().expect("valid tracing directive"))
            .add_directive("tower_http=debug".parse().expect("valid tracing directive"));

        if tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(filter).with(otel_layer),
        )
        .is_ok()
        {
            let _ = DEV_SERVER_OTEL_PROVIDER.set(provider);
        }
    });
}

/// Scheduler that schedules `batched_tick()` on the Node.js event loop via a
/// ThreadsafeFunction wrapping a noop JS function. The TSFN callback closure
/// does the actual work. Debounced: only one tick is pending at a time.
/// The TSFN type produced by `build_threadsafe_function().weak().build()`:
/// CalleeHandled = false, Weak = true (won't keep event loop alive).
type SchedulerTsfn = ThreadsafeFunction<(), (), (), napi::Status, false, true, 0>;

pub struct NapiScheduler {
    scheduled: Arc<AtomicBool>,
    core_ref: Weak<Mutex<NapiCoreType>>,
    tsfn: Option<SchedulerTsfn>,
}

impl NapiScheduler {
    fn new() -> Self {
        Self {
            scheduled: Arc::new(AtomicBool::new(false)),
            core_ref: Weak::new(),
            tsfn: None,
        }
    }

    fn set_core_ref(&mut self, core_ref: Weak<Mutex<NapiCoreType>>) {
        self.core_ref = core_ref;
    }

    fn set_tsfn(&mut self, tsfn: SchedulerTsfn) {
        self.tsfn = Some(tsfn);
    }
}

impl Scheduler for NapiScheduler {
    fn schedule_batched_tick(&self) {
        if !self.scheduled.swap(true, Ordering::SeqCst) {
            if let Some(ref tsfn) = self.tsfn {
                // CalleeHandled = false: pass value directly, not wrapped in Result
                tsfn.call((), ThreadsafeFunctionCallMode::NonBlocking);
            } else {
                self.scheduled.store(false, Ordering::SeqCst);
            }
        }
    }
}

fn build_napi_runtime(
    env: Env,
    schema_json: String,
    app_id: String,
    jazz_env: String,
    user_branch: String,
    storage: Box<dyn Storage + Send>,
    tier: Option<String>,
) -> napi::Result<NapiRuntime> {
    // Parse schema
    let runtime_schema = parse_runtime_schema_input(&schema_json)
        .map_err(|e| napi::Error::from_reason(format!("Invalid schema JSON: {}", e)))?;
    let schema = runtime_schema.schema;
    let declared_schema = schema.clone();

    // Parse optional tier
    let node_tiers = parse_node_durability_tier(tier)?;

    // Create sync manager
    let mut sync_manager = SyncManager::new();
    if !node_tiers.is_empty() {
        sync_manager = sync_manager.with_durability_tiers(node_tiers);
    }

    // Create schema manager
    let schema_manager = SchemaManager::new_with_policy_mode(
        sync_manager,
        schema,
        AppId::from_string(&app_id).unwrap_or_else(|_| AppId::from_name(&app_id)),
        &jazz_env,
        &user_branch,
        if runtime_schema.loaded_policy_bundle {
            jazz_tools::query_manager::types::RowPolicyMode::Enforcing
        } else {
            jazz_tools::query_manager::types::RowPolicyMode::PermissiveLocal
        },
    )
    .map_err(|e| napi::Error::from_reason(format!("Failed to create SchemaManager: {:?}", e)))?;

    // Create components
    let scheduler = NapiScheduler::new();

    // Create RuntimeCore and wrap
    let core = RuntimeCore::new(schema_manager, storage, scheduler);
    let core_arc = Arc::new(Mutex::new(core));

    // Set up the scheduler's TSFN
    {
        let core_weak = Arc::downgrade(&core_arc);
        let scheduled_flag = {
            let core_guard = core_arc
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            core_guard.scheduler().scheduled.clone()
        };

        let core_ref_for_tsfn = core_weak.clone();
        let flag_for_tsfn = scheduled_flag;

        let tick_fn = env.create_function_from_closure("__groove_tick", move |_ctx| {
            // Reset flag first so new ticks can be scheduled
            flag_for_tsfn.store(false, Ordering::SeqCst);
            if let Some(core_arc) = core_ref_for_tsfn.upgrade()
                && let Ok(mut core) = core_arc.lock()
            {
                core.batched_tick();
            }
            Ok(())
        })?;

        let tsfn = tick_fn.build_threadsafe_function().weak::<true>().build()?;

        // Set on scheduler
        let mut core_guard = core_arc
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core_guard.scheduler_mut().set_core_ref(core_weak);
        core_guard.scheduler_mut().set_tsfn(tsfn);

        // Persist schema to catalogue for server sync
        core_guard.persist_schema();
    }

    Ok(NapiRuntime {
        core: core_arc,
        upstream_server_id: Mutex::new(None),
        declared_schema,
        subscription_queries: Mutex::new(HashMap::new()),
    })
}

// ============================================================================
// NapiRuntime
// ============================================================================

#[napi]
pub struct NapiRuntime {
    core: Arc<Mutex<NapiCoreType>>,
    upstream_server_id: Mutex<Option<ServerId>>,
    declared_schema: Schema,
    subscription_queries: Mutex<HashMap<u64, Query>>,
}

#[napi]
impl NapiRuntime {
    /// Create a new NapiRuntime with SQLite-backed persistent storage.
    #[napi(constructor)]
    pub fn new(
        env: Env,
        schema_json: String,
        app_id: String,
        jazz_env: String,
        user_branch: String,
        data_path: String,
        tier: Option<String>,
    ) -> napi::Result<Self> {
        let storage = open_sqlite_storage(&data_path)?;

        build_napi_runtime(
            env,
            schema_json,
            app_id,
            jazz_env,
            user_branch,
            Box::new(storage),
            tier,
        )
    }

    /// Create a new NapiRuntime with in-memory storage (no local persistence).
    #[napi(js_name = "inMemory")]
    pub fn in_memory(
        env: Env,
        schema_json: String,
        app_id: String,
        jazz_env: String,
        user_branch: String,
        tier: Option<String>,
    ) -> napi::Result<Self> {
        build_napi_runtime(
            env,
            schema_json,
            app_id,
            jazz_env,
            user_branch,
            Box::new(MemoryStorage::new()),
            tier,
        )
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    #[napi]
    pub fn insert(
        &self,
        table: String,
        #[napi(ts_arg_type = "Record<string, unknown>")] values: FfiRecordArg,
        object_id: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let object_id =
            parse_external_object_id(object_id.as_deref()).map_err(napi::Error::from_reason)?;
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let ((object_id, row_values), batch_id) = core
            .insert_with_id(&table, values.0, object_id, None)
            .map_err(|e| napi::Error::from_reason(format!("Insert failed: {e}")))?;
        let row_values = align_row_values_to_declared_schema(
            &self.declared_schema,
            core.current_schema(),
            &TableName::new(table.clone()),
            row_values,
        );

        Ok(serde_json::json!({
            "id": object_id.uuid().to_string(),
            "values": row_values,
            "batchId": batch_id.to_string(),
        }))
    }

    #[napi(js_name = "insertWithSession")]
    pub fn insert_with_session(
        &self,
        table: String,
        #[napi(ts_arg_type = "Record<string, unknown>")] values: FfiRecordArg,
        write_context_json: Option<String>,
        object_id: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let write_context = parse_write_context_json(write_context_json)?;
        let object_id =
            parse_external_object_id(object_id.as_deref()).map_err(napi::Error::from_reason)?;

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let ((object_id, row_values), batch_id) = core
            .insert_with_id(&table, values.0, object_id, write_context.as_ref())
            .map_err(|e| napi::Error::from_reason(format!("Insert failed: {:?}", e)))?;
        let row_values = align_row_values_to_declared_schema(
            &self.declared_schema,
            core.current_schema(),
            &TableName::new(table.clone()),
            row_values,
        );

        Ok(serde_json::json!({
            "id": object_id.uuid().to_string(),
            "values": row_values,
            "batchId": batch_id.to_string(),
        }))
    }

    #[napi]
    pub fn update(
        &self,
        object_id: String,
        #[napi(ts_arg_type = "any")] values: FfiRecordArg,
    ) -> napi::Result<serde_json::Value> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let updates = convert_updates(values.0);

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let batch_id = core
            .update(oid, updates, None)
            .map_err(|e| napi::Error::from_reason(format!("Update failed: {e}")))?;

        Ok(serde_json::json!({
            "batchId": batch_id.to_string(),
        }))
    }

    #[napi(js_name = "updateWithSession")]
    pub fn update_with_session(
        &self,
        object_id: String,
        #[napi(ts_arg_type = "any")] values: FfiRecordArg,
        write_context_json: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let write_context = parse_write_context_json(write_context_json)?;

        let updates = convert_updates(values.0);

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let batch_id = core
            .update(oid, updates, write_context.as_ref())
            .map_err(|e| napi::Error::from_reason(format!("Update failed: {:?}", e)))?;

        Ok(serde_json::json!({
            "batchId": batch_id.to_string(),
        }))
    }

    #[napi(js_name = "delete")]
    pub fn delete_row(&self, object_id: String) -> napi::Result<serde_json::Value> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let batch_id = core
            .delete(oid, None)
            .map_err(|e| napi::Error::from_reason(format!("Delete failed: {:?}", e)))?;

        Ok(serde_json::json!({
            "batchId": batch_id.to_string(),
        }))
    }

    #[napi(js_name = "deleteWithSession")]
    pub fn delete_with_session(
        &self,
        object_id: String,
        write_context_json: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let uuid = uuid::Uuid::parse_str(&object_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let write_context = parse_write_context_json(write_context_json)?;

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let batch_id = core
            .delete(oid, write_context.as_ref())
            .map_err(|e| napi::Error::from_reason(format!("Delete failed: {:?}", e)))?;

        Ok(serde_json::json!({
            "batchId": batch_id.to_string(),
        }))
    }

    #[napi(js_name = "loadLocalBatchRecord", ts_return_type = "any | null")]
    pub fn load_local_batch_record(&self, batch_id: String) -> napi::Result<serde_json::Value> {
        let batch_id = parse_batch_id_input(&batch_id).map_err(napi::Error::from_reason)?;
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let record = core.local_batch_record(batch_id).map_err(|e| {
            napi::Error::from_reason(format!("Load local batch record failed: {e}"))
        })?;

        Ok(match record {
            Some(record) => serialize_local_batch_record(&record),
            None => serde_json::Value::Null,
        })
    }

    #[napi(js_name = "loadLocalBatchRecords", ts_return_type = "any[]")]
    pub fn load_local_batch_records(&self) -> napi::Result<serde_json::Value> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let records = core.local_batch_records().map_err(|e| {
            napi::Error::from_reason(format!("Load local batch records failed: {e}"))
        })?;

        Ok(serialize_local_batch_records(&records))
    }

    #[napi(js_name = "drainRejectedBatchIds", ts_return_type = "string[]")]
    pub fn drain_rejected_batch_ids(&self) -> napi::Result<Vec<String>> {
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        Ok(core
            .drain_rejected_batch_ids()
            .into_iter()
            .map(|batch_id| batch_id.to_string())
            .collect())
    }

    #[napi(js_name = "acknowledgeRejectedBatch")]
    pub fn acknowledge_rejected_batch(&self, batch_id: String) -> napi::Result<bool> {
        let batch_id = parse_batch_id_input(&batch_id).map_err(napi::Error::from_reason)?;
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.acknowledge_rejected_batch(batch_id).map_err(|e| {
            napi::Error::from_reason(format!("Acknowledge rejected batch failed: {e}"))
        })
    }

    #[napi(js_name = "sealBatch")]
    pub fn seal_batch(&self, batch_id: String) -> napi::Result<()> {
        let batch_id = parse_batch_id_input(&batch_id).map_err(napi::Error::from_reason)?;
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.seal_batch(batch_id)
            .map_err(|e| napi::Error::from_reason(format!("Seal batch failed: {e}")))
    }

    // =========================================================================
    // Queries
    // =========================================================================

    #[napi(ts_return_type = "Promise<any>")]
    pub async fn query(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
        options_json: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let query = parse_query(&query_json)?;
        let query_for_alignment = query.clone();
        let session = parse_session_json(session_json)?;

        let (durability, propagation, overlay) = parse_read_durability_options(tier, options_json)?;

        let (future, runtime_schema) = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            (
                match overlay {
                    Some(overlay) => core.query_with_local_overlay(
                        query,
                        session,
                        durability,
                        propagation,
                        overlay,
                    ),
                    None => core.query_with_propagation(query, session, durability, propagation),
                },
                core.current_schema().clone(),
            )
        };

        let rows = future
            .await
            .map_err(|e| napi::Error::from_reason(format!("Query failed: {:?}", e)))?;
        let rows = align_query_rows_to_declared_schema(
            &self.declared_schema,
            &runtime_schema,
            &query_for_alignment,
            rows,
        );

        let json_rows: Vec<serde_json::Value> = rows
            .into_iter()
            .map(|(id, values)| {
                serde_json::json!({
                    "id": id.uuid().to_string(),
                    "values": values
                })
            })
            .collect();

        Ok(serde_json::Value::Array(json_rows))
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    #[napi]
    pub fn subscribe(
        &self,
        query_json: String,
        #[napi(ts_arg_type = "(...args: any[]) => any")] on_update: ThreadsafeFunction<
            serde_json::Value,
        >,
        session_json: Option<String>,
        tier: Option<String>,
        options_json: Option<String>,
    ) -> napi::Result<f64> {
        let (query, session, durability, propagation) =
            parse_subscription_inputs(&query_json, session_json, tier, options_json)?;
        let alignment_table = query_rows_can_be_schema_aligned(&query).then_some(query.table);

        let callback = make_subscription_callback(
            on_update,
            alignment_table
                .as_ref()
                .map(|_| self.declared_schema.clone()),
            alignment_table,
        );

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let handle = core
            .subscribe_with_durability_and_propagation(
                query,
                callback,
                session,
                durability,
                propagation,
            )
            .map_err(|e| napi::Error::from_reason(format!("Subscribe failed: {:?}", e)))?;

        Ok(handle.0 as f64)
    }

    #[napi]
    pub fn unsubscribe(&self, handle: f64) -> napi::Result<()> {
        self.subscription_queries
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
            .remove(&(handle as u64));
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.unsubscribe(SubscriptionHandle(handle as u64));
        Ok(())
    }

    /// Phase 1 of 2-phase subscribe: allocate a handle and store query params.
    #[napi(js_name = "createSubscription")]
    pub fn create_subscription(
        &self,
        query_json: String,
        session_json: Option<String>,
        tier: Option<String>,
        options_json: Option<String>,
    ) -> napi::Result<f64> {
        let (query, session, durability, propagation) =
            parse_subscription_inputs(&query_json, session_json, tier, options_json)?;
        let query_for_alignment = query.clone();

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let handle = core.create_subscription(query, session, durability, propagation);
        drop(core);

        if query_rows_can_be_schema_aligned(&query_for_alignment) {
            self.subscription_queries
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?
                .insert(handle.0, query_for_alignment);
        }

        Ok(handle.0 as f64)
    }

    /// Phase 2 of 2-phase subscribe: compile, register, sync, attach callback, tick.
    #[napi(js_name = "executeSubscription")]
    pub fn execute_subscription(
        &self,
        handle: f64,
        #[napi(ts_arg_type = "(...args: any[]) => any")] on_update: ThreadsafeFunction<
            serde_json::Value,
        >,
    ) -> napi::Result<()> {
        let sub_handle = SubscriptionHandle(handle as u64);
        let alignment_table = self
            .subscription_queries
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
            .get(&(handle as u64))
            .map(|query| query.table);

        let callback = make_subscription_callback(
            on_update,
            alignment_table
                .as_ref()
                .map(|_| self.declared_schema.clone()),
            alignment_table,
        );

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.execute_subscription(sub_handle, callback)
            .map_err(|e| {
                napi::Error::from_reason(format!("Execute subscription failed: {:?}", e))
            })?;

        Ok(())
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    #[napi(js_name = "onSyncMessageReceived")]
    pub fn on_sync_message_received(
        &self,
        message_json: String,
        sequence: Option<f64>,
    ) -> napi::Result<()> {
        let mut payload: SyncPayload = serde_json::from_str(&message_json)
            .map_err(|e| napi::Error::from_reason(format!("Invalid sync message: {}", e)))?;
        let sequence = parse_optional_sequence(sequence)?;
        if let (None, SyncPayload::QuerySettled { through_seq, .. }) =
            (sequence.as_ref(), &mut payload)
        {
            // Local worker->main delivery is ordered and lossless, so the
            // upstream stream watermark cannot be interpreted against this
            // unsequenced in-process hop.
            *through_seq = 0;
        }
        let server_id = (*self
            .upstream_server_id
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?)
        .ok_or_else(|| {
            napi::Error::from_reason(
                "No upstream server registered; call addServer() before sync delivery",
            )
        })?;

        let entry = InboxEntry {
            source: Source::Server(server_id),
            payload,
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        if let Some(sequence) = sequence {
            core.park_sync_message_with_sequence(entry, sequence);
        } else {
            core.park_sync_message(entry);
        }
        Ok(())
    }

    /// Called by JS when a sync message arrives from a client (not a server).
    #[napi(js_name = "onSyncMessageReceivedFromClient")]
    pub fn on_sync_message_received_from_client(
        &self,
        client_id: String,
        message_json: String,
    ) -> napi::Result<()> {
        let uuid = uuid::Uuid::parse_str(&client_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let payload: SyncPayload = serde_json::from_str(&message_json)
            .map_err(|e| napi::Error::from_reason(format!("Invalid sync message: {}", e)))?;

        let entry = InboxEntry {
            source: Source::Client(cid),
            payload,
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.park_sync_message(entry);
        Ok(())
    }

    #[napi(js_name = "addServer")]
    pub fn add_server(
        &self,
        server_catalogue_state_hash: Option<String>,
        next_sync_seq: Option<f64>,
    ) -> napi::Result<()> {
        let next_sync_seq = parse_optional_sequence(next_sync_seq)?;
        let server_id = {
            let mut slot = self
                .upstream_server_id
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            if let Some(server_id) = *slot {
                server_id
            } else {
                let server_id = ServerId::new();
                *slot = Some(server_id);
                server_id
            }
        };
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        // Re-attach semantics: remove existing upstream edge then add again so
        // replay/full-sync runs on every successful reconnect.
        core.remove_server(server_id);
        core.add_server_with_catalogue_state_hash(
            server_id,
            server_catalogue_state_hash.as_deref(),
        );
        if let Some(next_sync_seq) = next_sync_seq {
            core.set_next_expected_server_sequence(server_id, next_sync_seq);
        }
        Ok(())
    }

    #[napi(js_name = "removeServer")]
    pub fn remove_server(&self) -> napi::Result<()> {
        let Some(server_id) = *self
            .upstream_server_id
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
        else {
            return Ok(());
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.remove_server(server_id);
        Ok(())
    }

    #[napi(js_name = "addClient")]
    pub fn add_client(&self) -> napi::Result<String> {
        let client_id = ClientId::new();
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.add_client(client_id, None);
        Ok(client_id.0.to_string())
    }

    /// Set a client's role ("user", "admin", or "peer").
    #[napi(js_name = "setClientRole")]
    pub fn set_client_role(&self, client_id: String, role: String) -> napi::Result<()> {
        use jazz_tools::sync_manager::ClientRole;

        let uuid = uuid::Uuid::parse_str(&client_id)
            .map_err(|e| napi::Error::from_reason(format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let client_role = match role.as_str() {
            "user" => ClientRole::User,
            "admin" => ClientRole::Admin,
            "peer" => ClientRole::Peer,
            _ => {
                return Err(napi::Error::from_reason(format!(
                    "Invalid role '{}'. Must be 'user', 'admin', or 'peer'.",
                    role
                )));
            }
        };

        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.set_client_role_by_name(cid, client_role);
        Ok(())
    }

    // =========================================================================
    // Schema Access
    // =========================================================================

    #[napi(js_name = "getSchema", ts_return_type = "any")]
    pub fn get_schema(&self) -> napi::Result<serde_json::Value> {
        serde_json::to_value(&self.declared_schema)
            .map_err(|e| napi::Error::from_reason(format!("Schema serialization failed: {}", e)))
    }

    #[napi(js_name = "getSchemaHash")]
    pub fn get_schema_hash(&self) -> napi::Result<String> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        let schema = core.current_schema();
        Ok(SchemaHash::compute(schema).to_string())
    }

    #[napi]
    pub fn flush(&self) -> napi::Result<()> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.storage().flush();
        Ok(())
    }

    /// Flush and close the underlying storage, releasing filesystem locks.
    #[napi]
    pub fn close(&self) -> napi::Result<()> {
        let core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.storage().flush();
        core.storage()
            .close()
            .map_err(|e| napi::Error::from_reason(format!("Failed to close storage: {:?}", e)))?;
        Ok(())
    }

    #[napi(js_name = "deriveUserId")]
    pub fn derive_user_id(seed_b64: String) -> napi::Result<String> {
        let seed = napi_decode_seed(&seed_b64)?;
        Ok(identity::derive_user_id(&seed).to_string())
    }

    #[napi(js_name = "mintLocalFirstToken")]
    pub fn mint_local_first_token(
        seed_b64: String,
        audience: String,
        ttl_seconds: u32,
    ) -> napi::Result<String> {
        let seed = napi_decode_seed(&seed_b64)?;
        identity::mint_jazz_self_signed_token(
            &seed,
            identity::LOCAL_FIRST_ISSUER,
            &audience,
            ttl_seconds as u64,
        )
        .map_err(napi::Error::from_reason)
    }

    #[napi(js_name = "getPublicKeyBase64url")]
    pub fn get_public_key_base64url(seed_b64: String) -> napi::Result<String> {
        let seed = napi_decode_seed(&seed_b64)?;
        let verifying_key = identity::derive_verifying_key(&seed);
        Ok(URL_SAFE_NO_PAD.encode(verifying_key.as_bytes()))
    }

    /// Connect to a Jazz server over WebSocket.
    ///
    /// Parses `auth_json` into `AuthConfig`, wires a `TransportManager` into
    /// `RuntimeCore` via `install_transport` (which seeds the catalogue state
    /// hash on the handle), and spawns the manager loop as a Tokio task.
    #[napi]
    pub fn connect(&self, url: String, auth_json: String) -> napi::Result<()> {
        let auth: jazz_tools::transport_manager::AuthConfig = serde_json::from_str(&auth_json)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let tick = NapiTickNotifier {
            core: Arc::clone(&self.core),
        };
        let manager = {
            let mut core = self
                .core
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            jazz_tools::runtime_core::install_transport::<
                _,
                _,
                jazz_tools::ws_stream::NativeWsStream,
                _,
            >(&mut core, url, auth, tick)
        };
        // Spawn the TransportManager loop. If we're inside an active Tokio
        // runtime (typical: Node.js with napi-rs bootstrapping one), use it.
        // Otherwise (e.g. Next.js SSG build workers that load the addon
        // without a runtime) fall back to a dedicated runtime on a background
        // thread so `tokio::spawn` never panics.
        match tokio::runtime::Handle::try_current() {
            Ok(rt_handle) => {
                rt_handle.spawn(manager.run());
            }
            Err(_) => {
                std::thread::spawn(move || {
                    let rt = match tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                    {
                        Ok(rt) => rt,
                        Err(e) => {
                            eprintln!("jazz-napi: failed to build fallback tokio runtime: {e}");
                            return;
                        }
                    };
                    rt.block_on(manager.run());
                });
            }
        }
        Ok(())
    }

    /// Disconnect from the Jazz server and drop the transport handle.
    #[napi]
    pub fn disconnect(&self) {
        if let Ok(mut core) = self.core.lock() {
            if let Some(handle) = core.transport() {
                handle.disconnect();
            }
            core.clear_transport();
        }
    }

    /// Push updated auth credentials into the live transport.
    #[napi]
    pub fn update_auth(&self, auth_json: String) -> napi::Result<()> {
        let auth: jazz_tools::transport_manager::AuthConfig = serde_json::from_str(&auth_json)
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        if let Ok(core) = self.core.lock()
            && let Some(handle) = core.transport()
        {
            handle.update_auth(auth);
        }
        Ok(())
    }

    /// Register a JS callback that fires when the Rust transport receives an
    /// auth rejection from the server during the WS handshake.
    ///
    /// The callback receives a single string argument: the rejection reason.
    #[napi(ts_args_type = "callback: (reason: string) => void")]
    pub fn on_auth_failure(
        &self,
        // CalleeHandled=false: JS callback receives (reason) not (error, reason).
        callback: ThreadsafeFunction<String, (), String, napi::Status, false, false, 0>,
    ) -> napi::Result<()> {
        let mut core = self
            .core
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        core.set_auth_failure_callback(move |reason| {
            callback.call(reason, ThreadsafeFunctionCallMode::NonBlocking);
        });
        Ok(())
    }
}

// ============================================================================
// NapiTickNotifier
// ============================================================================

/// `TickNotifier` implementation for the NAPI (Node.js) runtime.
///
/// Holds a weak-upgradeable reference to `RuntimeCore` and schedules a
/// `batched_tick` on the Node.js event loop whenever the transport layer
/// needs to wake up.
struct NapiTickNotifier {
    core: Arc<Mutex<NapiCoreType>>,
}

impl jazz_tools::transport_manager::TickNotifier for NapiTickNotifier {
    fn notify(&self) {
        if let Ok(core) = self.core.lock() {
            core.scheduler().schedule_batched_tick();
        }
    }
}

// ============================================================================
// TestingServer
// ============================================================================

#[napi]
pub struct TestingServer {
    inner: Mutex<Option<JazzTestingServer>>,
    app_id: String,
    url: String,
    port: u16,
    data_dir: String,
    backend_secret: String,
    admin_secret: String,
    built_in_jwt_helpers_available: bool,
}

#[napi]
impl TestingServer {
    #[napi(factory, ts_return_type = "Promise<TestingServer>")]
    pub async fn start(
        #[napi(
            ts_arg_type = "{ appId?: string; port?: number; dataDir?: string; persistentStorage?: boolean; adminSecret?: string; backendSecret?: string; jwksUrl?: string }"
        )]
        options: Option<JsonValue>,
    ) -> napi::Result<Self> {
        let options = parse_testing_server_start_options(options)?;

        let mut builder = JazzTestingServer::builder();

        if let Some(app_id) = options.app_id.as_deref() {
            let app_id = AppId::from_string(app_id).unwrap_or_else(|_| AppId::from_name(app_id));
            builder = builder.with_app_id(app_id);
        }

        if let Some(port) = options.port {
            builder = builder.with_port(port);
        }

        if let Some(data_dir) = options.data_dir {
            builder = builder.with_data_dir(data_dir);
        }

        if options.persistent_storage.unwrap_or(false) {
            builder = builder.with_persistent_storage();
        }

        if let Some(admin_secret) = options.admin_secret {
            builder = builder.with_admin_secret(admin_secret);
        }

        if let Some(backend_secret) = options.backend_secret {
            builder = builder.with_backend_secret(backend_secret);
        }

        if let Some(jwks_url) = options.jwks_url {
            builder = builder.with_jwks_url(jwks_url);
        }

        let server = builder.start().await;
        let app_id = server.app_id().to_string();
        let url = server.base_url();
        let port = server.port();
        let data_dir = server.data_dir().to_string_lossy().into_owned();
        let admin_secret = server.admin_secret().to_string();
        let backend_secret = server.backend_secret().to_string();
        let built_in_jwt_helpers_available = server.built_in_jwt_helpers_available();

        Ok(Self {
            inner: Mutex::new(Some(server)),
            app_id,
            url,
            port,
            data_dir,
            backend_secret,
            admin_secret,
            built_in_jwt_helpers_available,
        })
    }

    #[napi(getter, js_name = "appId")]
    pub fn app_id(&self) -> String {
        self.app_id.clone()
    }

    #[napi(getter)]
    pub fn url(&self) -> String {
        self.url.clone()
    }

    #[napi(getter)]
    pub fn port(&self) -> u16 {
        self.port
    }

    #[napi(getter, js_name = "dataDir")]
    pub fn data_dir(&self) -> String {
        self.data_dir.clone()
    }

    #[napi(getter, js_name = "backendSecret")]
    pub fn backend_secret(&self) -> String {
        self.backend_secret.clone()
    }

    #[napi(getter, js_name = "adminSecret")]
    pub fn admin_secret(&self) -> String {
        self.admin_secret.clone()
    }

    #[napi(js_name = "jwtForUser")]
    pub fn jwt_for_user(
        &self,
        user_id: String,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined")] claims: Option<JsonValue>,
    ) -> napi::Result<String> {
        if !self.built_in_jwt_helpers_available {
            return Err(napi::Error::from_reason(
                "TestingServer uses an external JWKS URL; built-in JWT helpers are unavailable. Mint JWTs from your external JWKS test fixture instead.",
            ));
        }

        let claims = claims.unwrap_or_else(|| json!({ "role": "user" }));
        Ok(JazzTestingServer::jwt_for_user_with_claims(
            &user_id, claims,
        ))
    }

    #[napi]
    pub async fn stop(&self) -> napi::Result<()> {
        let server = self
            .inner
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
            .take();

        if let Some(server) = server {
            server.shutdown().await;
        }

        Ok(())
    }
}

// ============================================================================
// DevServer
// ============================================================================

#[napi]
pub struct DevServer {
    inner: Mutex<Option<JazzHostedServer>>,
    app_id: String,
    url: String,
    port: u16,
    data_dir: String,
    backend_secret: Option<String>,
    admin_secret: Option<String>,
}

#[napi]
impl DevServer {
    #[napi(factory, ts_return_type = "Promise<DevServer>")]
    pub async fn start(
        #[napi(
            ts_arg_type = "{ appId: string; port?: number; dataDir?: string; inMemory?: boolean; jwksUrl?: string; allowLocalFirstAuth?: boolean; backendSecret?: string; adminSecret?: string; catalogueAuthority?: 'local' | 'forward'; catalogueAuthorityUrl?: string; catalogueAuthorityAdminSecret?: string; telemetryCollectorUrl?: string }"
        )]
        options: JsonValue,
    ) -> napi::Result<Self> {
        let opts = parse_dev_server_start_options(options)?;
        init_dev_server_telemetry(opts.telemetry_collector_url.as_deref());

        let app_id =
            AppId::from_string(&opts.app_id).unwrap_or_else(|_| AppId::from_name(&opts.app_id));

        let catalogue_authority = match opts.catalogue_authority.as_deref() {
            Some("forward") => {
                let base_url = opts.catalogue_authority_url.ok_or_else(|| {
                    napi::Error::from_reason(
                        "catalogueAuthorityUrl is required when catalogueAuthority is 'forward'",
                    )
                })?;
                let admin_secret = opts.catalogue_authority_admin_secret.ok_or_else(|| {
                    napi::Error::from_reason(
                        "catalogueAuthorityAdminSecret is required when catalogueAuthority is 'forward'",
                    )
                })?;
                CatalogueAuthorityMode::Forward {
                    base_url,
                    admin_secret,
                }
            }
            _ => CatalogueAuthorityMode::Local,
        };

        let auth_config = AuthConfig {
            jwks_url: opts.jwks_url,
            allow_local_first_auth: opts.allow_local_first_auth.unwrap_or(true),
            backend_secret: opts.backend_secret.clone(),
            admin_secret: opts.admin_secret.clone(),
            ..Default::default()
        };

        let in_memory = opts.in_memory.unwrap_or(false);
        let data_dir = if in_memory {
            String::new()
        } else {
            opts.data_dir.unwrap_or_else(|| "./data".to_string())
        };

        let mut server_builder = ServerBuilder::new(app_id)
            .with_auth_config(auth_config)
            .with_catalogue_authority(catalogue_authority);

        if in_memory {
            server_builder = server_builder.with_storage(StorageBackend::InMemory);
        } else {
            server_builder = server_builder.with_storage(StorageBackend::Sqlite {
                path: data_dir.clone().into(),
            });
        }

        let built = server_builder
            .build()
            .await
            .map_err(napi::Error::from_reason)?;

        let data_dir_path = std::path::PathBuf::from(&data_dir);

        let hosted = JazzHostedServer::start(
            built,
            opts.port,
            app_id,
            data_dir_path,
            opts.admin_secret.clone(),
            opts.backend_secret.clone(),
        )
        .await;

        let url = hosted.base_url();
        let port = hosted.port;
        let resolved_data_dir = hosted.data_dir.to_string_lossy().into_owned();

        Ok(Self {
            inner: Mutex::new(Some(hosted)),
            app_id: opts.app_id,
            url,
            port,
            data_dir: resolved_data_dir,
            backend_secret: opts.backend_secret,
            admin_secret: opts.admin_secret,
        })
    }

    #[napi(getter, js_name = "appId")]
    pub fn app_id(&self) -> String {
        self.app_id.clone()
    }

    #[napi(getter)]
    pub fn url(&self) -> String {
        self.url.clone()
    }

    #[napi(getter)]
    pub fn port(&self) -> u16 {
        self.port
    }

    #[napi(getter, js_name = "dataDir")]
    pub fn data_dir(&self) -> String {
        self.data_dir.clone()
    }

    #[napi(getter, js_name = "backendSecret")]
    pub fn backend_secret(&self) -> Option<String> {
        self.backend_secret.clone()
    }

    #[napi(getter, js_name = "adminSecret")]
    pub fn admin_secret(&self) -> Option<String> {
        self.admin_secret.clone()
    }

    #[napi]
    pub async fn stop(&self) -> napi::Result<()> {
        let mut server = self
            .inner
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
            .take();

        if let Some(ref mut server) = server {
            server.shutdown().await;
        }

        Ok(())
    }
}

// ============================================================================
// Module-level utility functions
// ============================================================================

#[napi(js_name = "generateId")]
pub fn generate_id() -> String {
    generate_binding_id()
}

#[napi(js_name = "currentTimestamp")]
pub fn current_timestamp() -> i64 {
    current_timestamp_ms()
}

#[napi(js_name = "parseSchema", ts_return_type = "any")]
pub fn parse_schema_fn(json: String) -> napi::Result<serde_json::Value> {
    let schema: Schema = serde_json::from_str(&json)
        .map_err(|e| napi::Error::from_reason(format!("Invalid schema JSON: {}", e)))?;
    serde_json::to_value(&schema)
        .map_err(|e| napi::Error::from_reason(format!("Schema serialization failed: {}", e)))
}

// ============================================================================
// Identity crypto utilities
// ============================================================================

fn decode_seed_napi(seed_b64: &str) -> napi::Result<[u8; 32]> {
    let bytes = URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|e| napi::Error::from_reason(format!("seed base64 decode error: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("seed must be exactly 32 bytes"))
}

#[napi(js_name = "deriveUserId")]
pub fn derive_user_id(seed_b64: String) -> napi::Result<String> {
    let seed = decode_seed_napi(&seed_b64)?;
    Ok(identity::derive_user_id(&seed).to_string())
}

#[napi(js_name = "mintLocalFirstToken")]
pub fn mint_local_first_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: u32,
) -> napi::Result<String> {
    let seed = decode_seed_napi(&seed_b64)?;
    identity::mint_jazz_self_signed_token(
        &seed,
        identity::LOCAL_FIRST_ISSUER,
        &audience,
        ttl_seconds as u64,
    )
    .map_err(napi::Error::from_reason)
}

#[napi(object)]
pub struct VerifyTokenResult {
    pub ok: bool,
    pub id: String,
    pub error: Option<String>,
}

#[napi(js_name = "verifyLocalFirstIdentityProof")]
pub fn verify_local_first_identity_proof_napi(
    token: Option<String>,
    expected_audience: String,
) -> VerifyTokenResult {
    let token = match token {
        Some(t) if !t.is_empty() => t,
        _ => {
            return VerifyTokenResult {
                ok: false,
                id: String::new(),
                error: Some("proofToken is required".to_string()),
            };
        }
    };
    match identity::verify_jazz_self_signed_proof(&token, &expected_audience) {
        Ok(verified) => VerifyTokenResult {
            ok: true,
            id: verified.user_id,
            error: None,
        },
        Err(e) => VerifyTokenResult {
            ok: false,
            id: String::new(),
            error: Some(e),
        },
    }
}

#[napi(js_name = "getPublicKeyBase64url")]
pub fn get_public_key_b64(seed_b64: String) -> napi::Result<String> {
    let seed = decode_seed_napi(&seed_b64)?;
    let verifying_key = identity::derive_verifying_key(&seed);
    Ok(URL_SAFE_NO_PAD.encode(verifying_key.as_bytes()))
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

    #[test]
    fn schema_json_roundtrip_preserves_enum_fk_and_defaults() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("files").column("name", ColumnType::Text))
            .table(
                TableSchema::builder("todos")
                    .column_with_default("done", ColumnType::Boolean, Value::Boolean(false))
                    .column(
                        "status",
                        ColumnType::Enum {
                            variants: vec!["done".to_string(), "todo".to_string()],
                        },
                    )
                    .fk_column("image", "files"),
            )
            .build();

        let encoded = serde_json::to_string(&schema).expect("serialize schema");
        let decoded: Schema = serde_json::from_str(&encoded).expect("deserialize schema");

        let status = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("status")
            .unwrap();
        assert_eq!(
            status.column_type,
            ColumnType::Enum {
                variants: vec!["done".to_string(), "todo".to_string()]
            }
        );

        let image = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("image")
            .unwrap();
        assert_eq!(image.references, Some(TableName::new("files")));

        let done = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("done")
            .unwrap();
        assert_eq!(done.default, Some(Value::Boolean(false)));
    }

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
