//! WasmRuntime - Main entry point for JavaScript applications.
//!
//! Provides the core Jazz database functionality exposed to JavaScript:
//! - CRUD operations (insert, query, update, delete)
//! - Reactive subscriptions with callback-based updates
//! - Sync message handling for server communication
//!
//! # Architecture
//!
//! - `MemoryStorage`/`OpfsBTreeStorage` provide synchronous storage (from jazz_tools::storage)
//! - `WasmScheduler` implements `Scheduler` using `spawn_local` (debounced)
//! - `JsSyncSender` implements `SyncSender` bridging to a JS callback (worker-bridge only; server sync via `connect()`)
//! - `WasmRuntime` wraps `Rc<RefCell<RuntimeCore<...>>>`

use std::any::Any;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::Once;

use jazz_tools::binding_support::parse_external_object_id;
use js_sys::Function;
use js_sys::Uint8Array;
use serde::Serialize;
#[cfg(target_arch = "wasm32")]
use tracing::warn;
use tracing::{debug_span, info, info_span};
use wasm_bindgen::prelude::*;

/// Initialize wasm-tracing exactly once (idempotent across multiple WasmRuntime instances).
fn init_tracing() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let max_level = wasm_log_level_from_global();
        let config = wasm_tracing::WasmLayerConfig::new()
            .with_max_level(max_level)
            .with_console_group_spans();
        let _ = wasm_tracing::set_as_global_default_with_config(config);
    });
}

fn wasm_log_level_from_global() -> tracing::Level {
    let global = js_sys::global();
    let key = JsValue::from_str("__JAZZ_WASM_LOG_LEVEL");
    let maybe_level = js_sys::Reflect::get(&global, &key)
        .ok()
        .and_then(|v| v.as_string())
        .map(|s| s.to_ascii_lowercase());

    match maybe_level.as_deref() {
        Some("error") => tracing::Level::ERROR,
        Some("warn") | Some("warning") => tracing::Level::WARN,
        Some("info") => tracing::Level::INFO,
        Some("debug") => tracing::Level::DEBUG,
        Some("trace") => tracing::Level::TRACE,
        _ => tracing::Level::WARN,
    }
}

/// Enable or disable collection of buffered tracing entries for JavaScript drains.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = setTraceEntryCollectionEnabled)]
pub fn set_trace_entry_collection_enabled(enabled: bool) {
    wasm_tracing::set_trace_entry_collection_enabled(enabled);
}

/// Drain buffered tracing entries collected by the wasm tracing layer.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = drainTraceEntries)]
pub fn drain_trace_entries() -> JsValue {
    wasm_tracing::drain_trace_entries()
}

/// Subscribe to notifications that buffered tracing entries are ready to drain.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen(js_name = subscribeTraceEntries)]
pub fn subscribe_trace_entries(callback: Function) -> Function {
    wasm_tracing::subscribe_trace_entries(callback)
}

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use jazz_tools::binding_support::{
    parse_batch_id_input, serialize_local_batch_record, serialize_local_batch_records,
};
use jazz_tools::identity;
use jazz_tools::object::ObjectId;
#[cfg(target_arch = "wasm32")]
use jazz_tools::query_manager::encoding::decode_row;
use jazz_tools::query_manager::manager::LocalUpdates;
#[cfg(target_arch = "wasm32")]
use jazz_tools::query_manager::query::Query;
use jazz_tools::query_manager::session::{Session, WriteContext};
#[cfg(target_arch = "wasm32")]
use jazz_tools::query_manager::types::{Row, RowDescriptor};
use jazz_tools::query_manager::types::{SchemaHash, Value};
use jazz_tools::runtime_core::{
    QueryLocalOverlay, ReadDurabilityOptions, RuntimeCore, Scheduler, SyncSender,
};
#[cfg(target_arch = "wasm32")]
use jazz_tools::runtime_core::{SubscriptionDelta, SubscriptionHandle};
#[cfg(target_arch = "wasm32")]
use jazz_tools::schema_manager::rehydrate_schema_manager_from_catalogue;
use jazz_tools::schema_manager::{AppId, SchemaManager};
#[cfg(target_arch = "wasm32")]
use jazz_tools::storage::OpfsBTreeStorage;
use jazz_tools::storage::{MemoryStorage, Storage};
use jazz_tools::sync_manager::QueryPropagation;
use jazz_tools::sync_manager::{
    ClientId, Destination, DurabilityTier, InboxEntry, OutboxEntry, ServerId, Source, SyncManager,
    SyncPayload,
};

use crate::query::parse_query;
use crate::types::SubscriptionRow;
#[cfg(target_arch = "wasm32")]
use crate::types::{
    SubscriptionRowAdded, SubscriptionRowChange, SubscriptionRowDelta, SubscriptionRowRemoved,
    SubscriptionRowUpdated,
};

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WasmSchemaStateDebug {
    current_schema_hash: String,
    live_schema_hashes: Vec<String>,
    known_schema_hashes: Vec<String>,
    pending_schema_hashes: Vec<String>,
    lens_edges: Vec<WasmLensEdgeDebug>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WasmLensEdgeDebug {
    source_hash: String,
    target_hash: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WasmInsertResult {
    id: String,
    values: Vec<Value>,
    batch_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WasmMutationResult {
    batch_id: String,
}

/// Parse a persistence tier string from JS.
fn parse_tier(tier: &str) -> Result<DurabilityTier, JsError> {
    match tier {
        "local" => Ok(DurabilityTier::Local),
        "edge" => Ok(DurabilityTier::EdgeServer),
        "global" => Ok(DurabilityTier::GlobalServer),
        _ => Err(JsError::new(&format!(
            "Invalid tier '{}'. Must be 'local', 'edge', or 'global'.",
            tier
        ))),
    }
}

fn parse_session_json(session_json: Option<String>) -> Result<Option<Session>, JsError> {
    if let Some(json) = session_json {
        let session = serde_json::from_str::<Session>(&json)
            .map_err(|e| JsError::new(&format!("Invalid session JSON: {}", e)))?;
        Ok(Some(session))
    } else {
        Ok(None)
    }
}

fn parse_write_context_json(
    write_context_json: Option<String>,
) -> Result<Option<WriteContext>, JsError> {
    if let Some(json) = write_context_json {
        match jazz_tools::binding_support::parse_write_context_input(Some(&json)) {
            Ok(context) => Ok(context),
            Err(err) => Err(JsError::new(&format!(
                "Invalid write context JSON: {}",
                err
            ))),
        }
    } else {
        Ok(None)
    }
}

#[derive(Debug, serde::Deserialize, Default)]
struct QueryExecutionOptionsWire {
    propagation: Option<String>,
    local_updates: Option<String>,
    transaction_overlay: Option<QueryTransactionOverlayWire>,
}

#[derive(Debug, serde::Deserialize)]
struct QueryTransactionOverlayWire {
    batch_id: String,
    branch_name: String,
    row_ids: Vec<String>,
}

fn parse_read_durability_options(
    tier: Option<String>,
    options_json: Option<String>,
) -> Result<
    (
        ReadDurabilityOptions,
        QueryPropagation,
        Option<QueryLocalOverlay>,
    ),
    JsError,
> {
    let parsed_tier = tier.as_deref().map(parse_tier).transpose()?;
    let Some(raw) = options_json else {
        return Ok((
            ReadDurabilityOptions {
                tier: parsed_tier,
                local_updates: LocalUpdates::Immediate,
            },
            QueryPropagation::Full,
            None,
        ));
    };

    let options: QueryExecutionOptionsWire = serde_json::from_str(&raw)
        .map_err(|e| JsError::new(&format!("Invalid query options JSON: {}", e)))?;

    let propagation = match options.propagation.as_deref() {
        None | Some("full") => Ok(QueryPropagation::Full),
        Some("local-only") => Ok(QueryPropagation::LocalOnly),
        Some(other) => Err(JsError::new(&format!(
            "Invalid propagation '{}'. Must be 'full' or 'local-only'.",
            other
        ))),
    }?;

    let local_updates = match options.local_updates.as_deref() {
        None | Some("immediate") => Ok(LocalUpdates::Immediate),
        Some("deferred") => Ok(LocalUpdates::Deferred),
        Some(other) => Err(JsError::new(&format!(
            "Invalid localUpdates '{}'. Must be 'immediate' or 'deferred'.",
            other
        ))),
    }?;

    let transaction_overlay = match options.transaction_overlay {
        None => None,
        Some(overlay) => Some(QueryLocalOverlay {
            batch_id: parse_batch_id_input(&overlay.batch_id)
                .map_err(|err| JsError::new(&format!("Invalid query batch id: {err}")))?,
            branch_name: jazz_tools::object::BranchName::new(&overlay.branch_name),
            row_ids: overlay
                .row_ids
                .into_iter()
                .map(|row_id| {
                    parse_external_object_id(Some(&row_id))
                        .and_then(|maybe| maybe.ok_or_else(|| "missing query row id".to_string()))
                        .map_err(|err| JsError::new(&format!("Invalid query row id: {err}")))
                })
                .collect::<Result<Vec<_>, _>>()?,
        }),
    };

    Ok((
        ReadDurabilityOptions {
            tier: parsed_tier,
            local_updates,
        },
        propagation,
        transaction_overlay,
    ))
}

#[cfg(target_arch = "wasm32")]
fn parse_subscription_inputs(
    query_json: &str,
    session_json: Option<String>,
    settled_tier: Option<String>,
    options_json: Option<String>,
) -> Result<
    (
        Query,
        Option<Session>,
        ReadDurabilityOptions,
        QueryPropagation,
    ),
    JsError,
> {
    let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;
    let session = parse_session_json(session_json)?;
    let (durability, propagation, _overlay) =
        parse_read_durability_options(settled_tier, options_json)?;
    Ok((query, session, durability, propagation))
}

#[cfg(target_arch = "wasm32")]
fn make_subscription_callback(on_update: Function) -> impl Fn(SubscriptionDelta) + 'static {
    move |delta: SubscriptionDelta| {
        let row_to_wasm = |row: &Row, descriptor: &RowDescriptor| -> SubscriptionRow {
            let values = decode_row(descriptor, &row.data)
                .map(|vals| vals.into_iter().map(Value::from).collect::<Vec<_>>())
                .unwrap_or_default();
            SubscriptionRow {
                id: row.id.uuid().to_string(),
                values,
            }
        };

        let descriptor = &delta.descriptor;
        let wasm_delta = SubscriptionRowDelta(
            delta
                .ordered_delta
                .removed
                .iter()
                .map(|change| {
                    SubscriptionRowChange::Removed(SubscriptionRowRemoved {
                        kind: 1,
                        id: change.id.uuid().to_string(),
                        index: change.index,
                    })
                })
                .chain(delta.ordered_delta.updated.iter().map(|change| {
                    SubscriptionRowChange::Updated(SubscriptionRowUpdated {
                        kind: 2,
                        id: change.id.uuid().to_string(),
                        index: change.new_index,
                        row: change.row.as_ref().map(|row| row_to_wasm(row, descriptor)),
                    })
                }))
                .chain(delta.ordered_delta.added.iter().map(|change| {
                    SubscriptionRowChange::Added(SubscriptionRowAdded {
                        kind: 0,
                        id: change.id.uuid().to_string(),
                        index: change.index,
                        row: row_to_wasm(&change.row, descriptor),
                    })
                }))
                .collect::<Vec<_>>(),
        );

        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        if let Ok(delta_value) = wasm_delta.serialize(&serializer) {
            let _ = on_update.call1(&JsValue::NULL, &delta_value);
        }
    }
}

fn parse_node_durability_tiers(tier: Option<&str>) -> Result<Vec<DurabilityTier>, JsError> {
    let Some(raw) = tier else {
        return Ok(Vec::new());
    };
    Ok(vec![parse_tier(raw)?])
}

fn tier_label_for_node_tier(tier: Option<&str>) -> &'static str {
    match tier {
        Some("local") => "local",
        Some("edge") => "edge",
        Some("global") => "global",
        _ => "client",
    }
}

#[cfg(target_arch = "wasm32")]
const DEFAULT_OPFS_CACHE_SIZE: usize = 32 * 1024 * 1024;

/// Build a `SchemaManager` from raw inputs. Shared by `open_persistent` and `open_ephemeral`.
#[cfg(target_arch = "wasm32")]
fn build_schema_manager(
    schema_json: &str,
    app_id: AppId,
    env: &str,
    user_branch: &str,
    tier: Option<&str>,
) -> Result<SchemaManager, JsError> {
    let runtime_schema = jazz_tools::binding_support::parse_runtime_schema_input(schema_json)
        .map_err(|e| JsError::new(&format!("Invalid schema JSON: {}", e)))?;
    let node_tiers = parse_node_durability_tiers(tier)?;
    let mut sync_manager = SyncManager::new();
    if !node_tiers.is_empty() {
        sync_manager = sync_manager.with_durability_tiers(node_tiers);
    }
    SchemaManager::new_with_policy_mode(
        sync_manager,
        runtime_schema.schema,
        app_id,
        env,
        user_branch,
        if runtime_schema.loaded_policy_bundle {
            jazz_tools::query_manager::types::RowPolicyMode::Enforcing
        } else {
            jazz_tools::query_manager::types::RowPolicyMode::PermissiveLocal
        },
    )
    .map_err(|e| JsError::new(&format!("Failed to create SchemaManager: {:?}", e)))
}

/// Wire up scheduler, sync sender, and `RuntimeCore` into a `WasmRuntime`.
/// Shared by `open_persistent` and `open_ephemeral`.
#[cfg(target_arch = "wasm32")]
fn assemble_wasm_runtime(
    schema_manager: SchemaManager,
    storage: Box<dyn Storage>,
    tier_label: &'static str,
    use_binary_encoding: bool,
) -> WasmRuntime {
    let scheduler = WasmScheduler::new();
    let sync_sender = JsSyncSender::new(use_binary_encoding);
    let mut core = RuntimeCore::new(schema_manager, storage, scheduler);
    core.set_tier_label(tier_label);
    core.set_sync_sender(Box::new(sync_sender.clone()));
    let core_rc = Rc::new(RefCell::new(core));
    {
        let mut core_guard = core_rc.borrow_mut();
        core_guard
            .scheduler_mut()
            .set_core_ref(Rc::downgrade(&core_rc));
    }
    core_rc.borrow_mut().persist_schema();
    WasmRuntime {
        core: core_rc,
        sync_sender,
        upstream_server_id: RefCell::new(None),
        tier_label,
    }
}

// ============================================================================
// Type alias
// ============================================================================

/// Concrete RuntimeCore type for WASM.
type WasmCoreType = RuntimeCore<Box<dyn Storage>, WasmScheduler>;

// ============================================================================
// WasmScheduler
// ============================================================================

/// Scheduler implementation for WASM.
///
/// Uses `wasm_bindgen_futures::spawn_local` to schedule a batched tick.
/// Debounced: only one task is scheduled at a time.
#[derive(Clone)]
pub struct WasmScheduler {
    /// Debounce flag for scheduled ticks.
    scheduled: Rc<RefCell<bool>>,
    /// Weak reference back to RuntimeCore for spawned tasks.
    core_ref: Weak<RefCell<WasmCoreType>>,
}

impl WasmScheduler {
    fn new() -> Self {
        Self {
            scheduled: Rc::new(RefCell::new(false)),
            core_ref: Weak::new(),
        }
    }

    fn set_core_ref(&mut self, core_ref: Weak<RefCell<WasmCoreType>>) {
        self.core_ref = core_ref;
    }
}

fn schedule_batched_tick_task(core_ref: Weak<RefCell<WasmCoreType>>, flag: Rc<RefCell<bool>>) {
    wasm_bindgen_futures::spawn_local(async move {
        *flag.borrow_mut() = false;

        let Some(core_rc) = core_ref.upgrade() else {
            return;
        };

        let needs_retry = if let Ok(mut core) = core_rc.try_borrow_mut() {
            core.batched_tick();
            false
        } else {
            true
        };

        if needs_retry {
            // Runtime is currently borrowed (e.g. during query/subscription setup).
            // Keep one retry queued rather than panicking on RefCell reborrow.
            let mut scheduled = flag.borrow_mut();
            if *scheduled {
                return;
            }
            *scheduled = true;
            drop(scheduled);
            schedule_batched_tick_task(core_ref.clone(), flag.clone());
        }
    });
}

impl Scheduler for WasmScheduler {
    fn schedule_batched_tick(&self) {
        let mut scheduled = self.scheduled.borrow_mut();
        if !*scheduled {
            *scheduled = true;
            drop(scheduled);
            schedule_batched_tick_task(self.core_ref.clone(), self.scheduled.clone());
        }
    }
}

// ============================================================================
// JsSyncSender
// ============================================================================

/// Bridges outbound sync messages from the Rust runtime to a JS callback.
///
/// This sender is intentionally kept for the **worker-bridge path only**:
/// the worker WASM runtime routes `"client"`-destination outbox messages
/// back to the main thread via postMessage. Server-bound messages go through
/// the Rust-owned WebSocket transport (`WasmRuntime::connect`) instead; any
/// `"server"` destination that arrives here is silently dropped by the JS
/// callback registered in `jazz-worker.ts`.
struct JsSyncSenderInner {
    callback: RefCell<Option<Function>>,
    use_binary_encoding: bool,
}

#[derive(Clone)]
pub struct JsSyncSender {
    inner: Rc<JsSyncSenderInner>,
}

// SAFETY: WASM is single-threaded; the JS callback never crosses threads.
// `Send` is required only because `RuntimeCore::sync_sender` is typed
// `Box<dyn SyncSender + Send>` for the multi-threaded Tokio backend.
unsafe impl Send for JsSyncSender {}

impl JsSyncSender {
    fn new(use_binary_encoding: bool) -> Self {
        Self {
            inner: Rc::new(JsSyncSenderInner {
                callback: RefCell::new(None),
                use_binary_encoding,
            }),
        }
    }

    fn set_callback(&self, callback: Function) {
        *self.inner.callback.borrow_mut() = Some(callback);
    }
}

impl SyncSender for JsSyncSender {
    fn send_sync_message(&self, message: OutboxEntry) {
        if let Some(ref callback) = *self.inner.callback.borrow() {
            let is_catalogue = message.payload.is_catalogue();
            let (destination_kind, destination_id) = match message.destination {
                Destination::Server(server_id) => ("server", server_id.0.to_string()),
                Destination::Client(client_id) => ("client", client_id.0.to_string()),
            };
            if self.inner.use_binary_encoding || destination_kind == "client" {
                if let Ok(payload_bytes) = message.payload.to_bytes() {
                    let payload_js = Uint8Array::from(payload_bytes.as_slice());
                    let _ = callback.call4(
                        &JsValue::NULL,
                        &JsValue::from_str(destination_kind),
                        &JsValue::from_str(&destination_id),
                        &payload_js.into(),
                        &JsValue::from_bool(is_catalogue),
                    );
                }
            } else {
                let payload_json = message.payload.to_json().unwrap();
                let _ = callback.call4(
                    &JsValue::NULL,
                    &JsValue::from_str(destination_kind),
                    &JsValue::from_str(&destination_id),
                    &JsValue::from_str(&payload_json),
                    &JsValue::from_bool(is_catalogue),
                );
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

// ============================================================================
// WasmRuntime
// ============================================================================

/// Main runtime for JavaScript applications.
///
/// Wraps `Rc<RefCell<WasmCoreType>>`.
/// All methods borrow the core, call RuntimeCore, and return.
/// Async scheduling happens via WasmScheduler.schedule_batched_tick().
#[wasm_bindgen]
pub struct WasmRuntime {
    core: Rc<RefCell<WasmCoreType>>,
    /// JS callback holder for outbound sync messages (worker-bridge path only).
    ///
    /// `on_sync_message_to_send` installs the JS-side callback here. The worker
    /// WASM runtime uses it to forward `"client"`-destination outbox messages to
    /// the main thread via postMessage. Server sync goes through `connect()`.
    sync_sender: JsSyncSender,
    upstream_server_id: RefCell<Option<ServerId>>,
    /// Label for tracing (e.g. "local", "edge", or "client").
    tier_label: &'static str,
}

#[wasm_bindgen]
impl WasmRuntime {
    /// Create a new WasmRuntime.
    ///
    /// Storage is synchronous (in-memory via MemoryStorage).
    ///
    /// # Arguments
    /// * `schema_json` - JSON-encoded schema definition
    /// * `app_id` - Application identifier
    /// * `env` - Environment (e.g., "dev", "prod")
    /// * `user_branch` - User's branch name (e.g., "main")
    /// * `tier` - Optional node durability tier ("local", "edge", "global").
    ///            Set for server nodes to enable ack emission.
    /// * `use_binary_encoding` - Optional outgoing sync payload encoding mode.
    ///   `Some(true)` emits postcard bytes (`Uint8Array`), otherwise JSON strings.
    #[wasm_bindgen(constructor)]
    pub fn new(
        schema_json: &str,
        app_id: &str,
        env: &str,
        user_branch: &str,
        tier: Option<String>,
        use_binary_encoding: Option<bool>,
    ) -> Result<WasmRuntime, JsError> {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();
        init_tracing();

        let tier_label = tier_label_for_node_tier(tier.as_deref());
        let _span = info_span!(
            "WasmRuntime::new",
            tier = tier_label,
            app_id,
            env,
            user_branch
        )
        .entered();
        info!("creating in-memory runtime");

        // Parse schema
        let runtime_schema = jazz_tools::binding_support::parse_runtime_schema_input(schema_json)
            .map_err(|e| JsError::new(&format!("Invalid schema JSON: {}", e)))?;
        let schema = runtime_schema.schema;
        // Parse optional tier
        let node_tiers = parse_node_durability_tiers(tier.as_deref())?;

        // Create sync manager
        let mut sync_manager = SyncManager::new();
        if !node_tiers.is_empty() {
            sync_manager = sync_manager.with_durability_tiers(node_tiers);
        }

        let app_id = AppId::from_string(app_id).unwrap_or_else(|_| AppId::from_name(app_id));

        // Create schema manager
        let schema_manager = SchemaManager::new_with_policy_mode(
            sync_manager,
            schema,
            app_id,
            env,
            user_branch,
            if runtime_schema.loaded_policy_bundle {
                jazz_tools::query_manager::types::RowPolicyMode::Enforcing
            } else {
                jazz_tools::query_manager::types::RowPolicyMode::PermissiveLocal
            },
        )
        .map_err(|e| JsError::new(&format!("Failed to create SchemaManager: {:?}", e)))?;

        // Create components
        let storage: Box<dyn Storage> = Box::new(MemoryStorage::new());
        let scheduler = WasmScheduler::new();
        let sync_sender = JsSyncSender::new(use_binary_encoding.unwrap_or(false));

        // Create RuntimeCore
        let mut core = RuntimeCore::new(schema_manager, storage, scheduler);
        core.set_tier_label(tier_label);
        // Install the JS-callback sender so `batched_tick` drains outbox
        // entries to the worker bridge (no transport handle here).
        core.set_sync_sender(Box::new(sync_sender.clone()));

        // Wrap in Rc<RefCell>
        let core_rc = Rc::new(RefCell::new(core));

        // Set the core_ref on the Scheduler
        {
            let mut core_guard = core_rc.borrow_mut();
            core_guard
                .scheduler_mut()
                .set_core_ref(Rc::downgrade(&core_rc));
        }

        // Persist schema to catalogue for server sync
        core_rc.borrow_mut().persist_schema();

        Ok(WasmRuntime {
            core: core_rc,
            sync_sender,
            upstream_server_id: RefCell::new(None),
            tier_label,
        })
    }

    /// Called by JS when a sync message arrives from the server.
    ///
    /// # Arguments
    /// * `payload` - Either postcard-encoded SyncPayload bytes (`Uint8Array`)
    ///   or JSON-encoded SyncPayload (`string`)
    #[wasm_bindgen(js_name = onSyncMessageReceived)]
    pub fn on_sync_message_received(
        &self,
        payload: JsValue,
        sequence: Option<f64>,
    ) -> Result<(), JsError> {
        let _span = debug_span!("wasm::onSyncMessageReceived", tier = self.tier_label).entered();
        let mut payload = self.parse_sync_payload(payload)?;
        let sequence = Self::parse_optional_sequence(sequence)?;
        if let (None, SyncPayload::QuerySettled { through_seq, .. }) =
            (sequence.as_ref(), &mut payload)
        {
            // Local worker->main delivery is ordered and lossless, so the
            // upstream stream watermark cannot be interpreted against this
            // unsequenced in-process hop.
            *through_seq = 0;
        }
        let server_id = (*self.upstream_server_id.borrow()).ok_or_else(|| {
            JsError::new("No upstream server registered; call addServer() before sync delivery")
        })?;

        let entry = InboxEntry {
            source: Source::Server(server_id),
            payload,
        };

        let mut core = self.core.borrow_mut();
        if let Some(sequence) = sequence {
            core.park_sync_message_with_sequence(entry, sequence);
        } else {
            core.park_sync_message(entry);
        }
        Ok(())
    }

    /// Called by JS when a sync message arrives from a client (not a server).
    ///
    /// # Arguments
    /// * `client_id` - UUID string of the sending client
    /// * `payload` - Postcard-encoded SyncPayload bytes
    #[wasm_bindgen(js_name = onSyncMessageReceivedFromClient)]
    pub fn on_sync_message_received_from_client(
        &self,
        client_id: &str,
        payload: JsValue,
    ) -> Result<(), JsError> {
        let _span = debug_span!(
            "wasm::onSyncMessageReceivedFromClient",
            tier = self.tier_label,
            client_id
        )
        .entered();
        let uuid = uuid::Uuid::parse_str(client_id)
            .map_err(|e| JsError::new(&format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let payload = self.parse_sync_payload(payload)?;

        let entry = InboxEntry {
            source: Source::Client(cid),
            payload,
        };

        self.core.borrow_mut().park_sync_message(entry);
        Ok(())
    }

    fn parse_sync_payload(&self, payload: JsValue) -> Result<SyncPayload, JsError> {
        if let Some(json) = payload.as_string() {
            SyncPayload::from_json(&json)
                .map_err(|e| JsError::new(&format!("Invalid sync payload JSON: {e}")))
        } else if payload.is_instance_of::<Uint8Array>() {
            let bytes = Uint8Array::new(&payload).to_vec();
            SyncPayload::from_bytes(&bytes)
                .map_err(|e| JsError::new(&format!("Invalid sync payload postcard: {e}")))
        } else {
            Err(JsError::new(
                "Invalid sync payload type: expected Uint8Array or JSON string",
            ))
        }
    }

    fn parse_optional_sequence(sequence: Option<f64>) -> Result<Option<u64>, JsError> {
        let Some(sequence) = sequence else {
            return Ok(None);
        };
        if !sequence.is_finite() || sequence < 0.0 || sequence.fract() != 0.0 {
            return Err(JsError::new(
                "Invalid stream sequence: expected a non-negative integer",
            ));
        }
        if sequence > u64::MAX as f64 {
            return Err(JsError::new(
                "Invalid stream sequence: value exceeds u64 range",
            ));
        }
        Ok(Some(sequence as u64))
    }

    /// Register a callback for outgoing sync messages.
    #[wasm_bindgen(js_name = onSyncMessageToSend)]
    pub fn on_sync_message_to_send(&self, callback: Function) {
        self.sync_sender.set_callback(callback);
    }

    // =========================================================================
    // CRUD Operations
    // =========================================================================

    /// Insert a row into a table.
    ///
    /// # Returns
    /// The inserted row as `{ id, values, batchId }`.
    #[wasm_bindgen]
    pub fn insert(
        &self,
        table: &str,
        values: JsValue,
        object_id: Option<String>,
    ) -> Result<JsValue, JsError> {
        let _span = debug_span!("wasm::insert", tier = self.tier_label, table).entered();
        let named_values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let object_id = parse_external_object_id(object_id.as_deref())
            .map_err(|message| JsError::new(&message))?;

        let mut core = self.core.borrow_mut();
        let ((object_id, row_values), batch_id) = core
            .insert_with_id(table, named_values, object_id, None)
            .map_err(|e| JsError::new(&format!("Insert failed: {e}")))?;

        let row = WasmInsertResult {
            id: object_id.uuid().to_string(),
            values: row_values,
            batch_id: batch_id.to_string(),
        };
        tracing::debug!(object_id = %row.id, "inserted");
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        row.serialize(&serializer)
            .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Insert a row into a table as an explicit session principal.
    #[wasm_bindgen(js_name = insertWithSession)]
    pub fn insert_with_session(
        &self,
        table: &str,
        values: JsValue,
        write_context_json: Option<String>,
        object_id: Option<String>,
    ) -> Result<JsValue, JsError> {
        let _span = debug_span!("wasm::insertWithSession", tier = self.tier_label, table).entered();
        let named_values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let write_context = parse_write_context_json(write_context_json)?;
        let object_id = parse_external_object_id(object_id.as_deref())
            .map_err(|message| JsError::new(&message))?;

        let mut core = self.core.borrow_mut();
        let ((object_id, row_values), batch_id) = core
            .insert_with_id(table, named_values, object_id, write_context.as_ref())
            .map_err(|e| JsError::new(&format!("Insert failed: {:?}", e)))?;

        let row = WasmInsertResult {
            id: object_id.uuid().to_string(),
            values: row_values,
            batch_id: batch_id.to_string(),
        };
        tracing::debug!(object_id = %row.id, "inserted_with_session");
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        row.serialize(&serializer)
            .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Execute a query and return results as a Promise.
    ///
    /// Optional durability tier controls remote settlement behavior.
    #[wasm_bindgen]
    pub fn query(
        &self,
        query_json: &str,
        session_json: Option<String>,
        settled_tier: Option<String>,
        options_json: Option<String>,
    ) -> Result<js_sys::Promise, JsError> {
        let _span = debug_span!("wasm::query", tier = self.tier_label).entered();
        let query = parse_query(query_json).map_err(|e| JsError::new(&e))?;
        let session = parse_session_json(session_json)?;

        let (durability, propagation, overlay) =
            parse_read_durability_options(settled_tier, options_json)?;

        let future = {
            let mut core = self.core.borrow_mut();
            match overlay {
                Some(overlay) => {
                    core.query_with_local_overlay(query, session, durability, propagation, overlay)
                }
                None => core.query_with_propagation(query, session, durability, propagation),
            }
        };

        let promise = wasm_bindgen_futures::future_to_promise(async move {
            let results = future
                .await
                .map_err(|e| JsValue::from_str(&format!("Query failed: {:?}", e)))?;

            let wasm_results: Vec<_> = results
                .into_iter()
                .map(|(id, values)| {
                    let wasm_values: Vec<Value> = values;
                    SubscriptionRow {
                        id: id.uuid().to_string(),
                        values: wasm_values,
                    }
                })
                .collect();

            let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
            wasm_results
                .serialize(&serializer)
                .map_err(|e| JsValue::from_str(&format!("Serialization failed: {:?}", e)))
        });

        Ok(promise)
    }

    /// Update a row by ObjectId.
    #[wasm_bindgen]
    pub fn update(&self, object_id: &str, values: JsValue) -> Result<JsValue, JsError> {
        let _span = debug_span!("wasm::update", tier = self.tier_label, object_id).entered();
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let updates: Vec<(String, Value)> = partial_values.into_iter().collect();

        let mut core = self.core.borrow_mut();
        let batch_id = core
            .update(oid, updates, None)
            .map_err(|e| JsError::new(&format!("Update failed: {e}")))?;

        tracing::debug!(object_id, "updated");
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        WasmMutationResult {
            batch_id: batch_id.to_string(),
        }
        .serialize(&serializer)
        .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Update a row by ObjectId as an explicit session principal.
    ///
    /// # Arguments
    /// * `object_id` - UUID string of target object
    /// * `values` - Partial update map (`{ columnName: Value }`)
    /// * `session_json` - Optional JSON-encoded Session used for policy checks
    #[wasm_bindgen(js_name = updateWithSession)]
    pub fn update_with_session(
        &self,
        object_id: &str,
        values: JsValue,
        write_context_json: Option<String>,
    ) -> Result<JsValue, JsError> {
        let _span =
            debug_span!("wasm::updateWithSession", tier = self.tier_label, object_id).entered();
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let write_context = parse_write_context_json(write_context_json)?;

        let partial_values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let updates: Vec<(String, Value)> = partial_values.into_iter().collect();

        let mut core = self.core.borrow_mut();
        let batch_id = core
            .update(oid, updates, write_context.as_ref())
            .map_err(|e| JsError::new(&format!("Update failed: {e}")))?;

        tracing::debug!(object_id, "updated_with_session");
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        WasmMutationResult {
            batch_id: batch_id.to_string(),
        }
        .serialize(&serializer)
        .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Delete a row by ObjectId.
    #[wasm_bindgen]
    pub fn delete(&self, object_id: &str) -> Result<JsValue, JsError> {
        let _span = debug_span!("wasm::delete", tier = self.tier_label, object_id).entered();
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let mut core = self.core.borrow_mut();
        let batch_id = core
            .delete(oid, None)
            .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?;

        tracing::debug!(object_id, "deleted");
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        WasmMutationResult {
            batch_id: batch_id.to_string(),
        }
        .serialize(&serializer)
        .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Delete a row by ObjectId as an explicit session principal.
    #[wasm_bindgen(js_name = deleteWithSession)]
    pub fn delete_with_session(
        &self,
        object_id: &str,
        write_context_json: Option<String>,
    ) -> Result<JsValue, JsError> {
        let _span =
            debug_span!("wasm::deleteWithSession", tier = self.tier_label, object_id).entered();
        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let write_context = parse_write_context_json(write_context_json)?;

        let mut core = self.core.borrow_mut();
        let batch_id = core
            .delete(oid, write_context.as_ref())
            .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?;

        tracing::debug!(object_id, "deleted_with_session");
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        WasmMutationResult {
            batch_id: batch_id.to_string(),
        }
        .serialize(&serializer)
        .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    // =========================================================================
    // Persisted CRUD Operations
    // =========================================================================

    /// Insert a row immediately, returning the logical batch id that tracks
    /// replayable persisted fate for this write.
    #[wasm_bindgen(js_name = insertPersisted)]
    pub fn insert_persisted(
        &self,
        table: &str,
        values: JsValue,
        tier: &str,
    ) -> Result<JsValue, JsError> {
        let persistence_tier = parse_tier(tier)?;
        let named_values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;

        let ((object_id, row_values), batch_id, _receiver) = {
            let mut core = self.core.borrow_mut();
            core.insert_persisted_with_batch_id(table, named_values, None, persistence_tier)
                .map_err(|e| JsError::new(&format!("Insert failed: {e}")))?
        };

        let payload = serde_json::json!({
            "batchId": batch_id.to_string(),
            "row": SubscriptionRow {
                id: object_id.uuid().to_string(),
                values: row_values,
            }
        });
        serde_wasm_bindgen::to_value(&payload)
            .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Insert a row immediately, returning the logical batch id that tracks
    /// replayable persisted fate for this write, scoped to an explicit session
    /// principal or transactional write context.
    #[wasm_bindgen(js_name = insertPersistedWithSession)]
    pub fn insert_persisted_with_session(
        &self,
        table: &str,
        values: JsValue,
        write_context_json: Option<String>,
        tier: &str,
    ) -> Result<JsValue, JsError> {
        let persistence_tier = parse_tier(tier)?;
        let named_values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let write_context = parse_write_context_json(write_context_json)?;

        let ((object_id, row_values), batch_id, _receiver) = {
            let mut core = self.core.borrow_mut();
            core.insert_persisted_with_batch_id(
                table,
                named_values,
                write_context.as_ref(),
                persistence_tier,
            )
            .map_err(|e| JsError::new(&format!("Insert failed: {:?}", e)))?
        };

        let payload = serde_json::json!({
            "batchId": batch_id.to_string(),
            "row": SubscriptionRow {
                id: object_id.uuid().to_string(),
                values: row_values,
            }
        });
        serde_wasm_bindgen::to_value(&payload)
            .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Update a row immediately, returning the logical batch id that tracks
    /// replayable persisted fate for this write.
    #[wasm_bindgen(js_name = updatePersisted)]
    pub fn update_persisted(
        &self,
        object_id: &str,
        values: JsValue,
        tier: &str,
    ) -> Result<JsValue, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let partial_values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let updates: Vec<(String, Value)> = partial_values.into_iter().collect();

        let (batch_id, _receiver) = {
            let mut core = self.core.borrow_mut();
            core.update_persisted_with_batch_id(oid, updates, None, persistence_tier)
                .map_err(|e| JsError::new(&format!("Update failed: {e}")))?
        };

        serde_wasm_bindgen::to_value(&serde_json::json!({
            "batchId": batch_id.to_string(),
        }))
        .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Update a row immediately, returning the logical batch id that tracks
    /// replayable persisted fate for this write, scoped to an explicit session
    /// principal or transactional write context.
    #[wasm_bindgen(js_name = updatePersistedWithSession)]
    pub fn update_persisted_with_session(
        &self,
        object_id: &str,
        values: JsValue,
        write_context_json: Option<String>,
        tier: &str,
    ) -> Result<JsValue, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let write_context = parse_write_context_json(write_context_json)?;

        let partial_values: HashMap<String, Value> = serde_wasm_bindgen::from_value(values)?;
        let updates: Vec<(String, Value)> = partial_values.into_iter().collect();

        let (batch_id, _receiver) = {
            let mut core = self.core.borrow_mut();
            core.update_persisted_with_batch_id(
                oid,
                updates,
                write_context.as_ref(),
                persistence_tier,
            )
            .map_err(|e| JsError::new(&format!("Update failed: {:?}", e)))?
        };

        serde_wasm_bindgen::to_value(&serde_json::json!({
            "batchId": batch_id.to_string(),
        }))
        .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Delete a row immediately, returning the logical batch id that tracks
    /// replayable persisted fate for this write.
    #[wasm_bindgen(js_name = deletePersisted)]
    pub fn delete_persisted(&self, object_id: &str, tier: &str) -> Result<JsValue, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);

        let (batch_id, _receiver) = {
            let mut core = self.core.borrow_mut();
            core.delete_persisted_with_batch_id(oid, None, persistence_tier)
                .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?
        };

        serde_wasm_bindgen::to_value(&serde_json::json!({
            "batchId": batch_id.to_string(),
        }))
        .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    /// Delete a row immediately, returning the logical batch id that tracks
    /// replayable persisted fate for this write, scoped to an explicit session
    /// principal or transactional write context.
    #[wasm_bindgen(js_name = deletePersistedWithSession)]
    pub fn delete_persisted_with_session(
        &self,
        object_id: &str,
        write_context_json: Option<String>,
        tier: &str,
    ) -> Result<JsValue, JsError> {
        let persistence_tier = parse_tier(tier)?;

        let uuid = uuid::Uuid::parse_str(object_id)
            .map_err(|e| JsError::new(&format!("Invalid ObjectId: {}", e)))?;
        let oid = ObjectId::from_uuid(uuid);
        let write_context = parse_write_context_json(write_context_json)?;

        let (batch_id, _receiver) = {
            let mut core = self.core.borrow_mut();
            core.delete_persisted_with_batch_id(oid, write_context.as_ref(), persistence_tier)
                .map_err(|e| JsError::new(&format!("Delete failed: {:?}", e)))?
        };

        serde_wasm_bindgen::to_value(&serde_json::json!({
            "batchId": batch_id.to_string(),
        }))
        .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    #[wasm_bindgen(js_name = loadLocalBatchRecord)]
    pub fn load_local_batch_record(&self, batch_id: &str) -> Result<JsValue, JsError> {
        let batch_id = parse_batch_id_input(batch_id).map_err(|err| JsError::new(&err))?;
        let core = self.core.borrow();
        let record = core
            .local_batch_record(batch_id)
            .map_err(|e| JsError::new(&format!("Load local batch record failed: {e}")))?;
        match record {
            Some(record) => {
                let serializer =
                    serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
                serialize_local_batch_record(&record)
                    .serialize(&serializer)
                    .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
            }
            None => Ok(JsValue::null()),
        }
    }

    #[wasm_bindgen(js_name = loadLocalBatchRecords)]
    pub fn load_local_batch_records(&self) -> Result<JsValue, JsError> {
        let core = self.core.borrow();
        let records = core
            .local_batch_records()
            .map_err(|e| JsError::new(&format!("Load local batch records failed: {e}")))?;
        let serializer = serde_wasm_bindgen::Serializer::new().serialize_maps_as_objects(true);
        serialize_local_batch_records(&records)
            .serialize(&serializer)
            .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    #[wasm_bindgen(js_name = drainRejectedBatchIds)]
    pub fn drain_rejected_batch_ids(&self) -> Result<JsValue, JsError> {
        let mut core = self.core.borrow_mut();
        let batch_ids = core
            .drain_rejected_batch_ids()
            .into_iter()
            .map(|batch_id| batch_id.to_string())
            .collect::<Vec<_>>();
        serde_wasm_bindgen::to_value(&batch_ids)
            .map_err(|e| JsError::new(&format!("Serialization failed: {:?}", e)))
    }

    #[wasm_bindgen(js_name = acknowledgeRejectedBatch)]
    pub fn acknowledge_rejected_batch(&self, batch_id: &str) -> Result<bool, JsError> {
        let batch_id = parse_batch_id_input(batch_id).map_err(|err| JsError::new(&err))?;
        let mut core = self.core.borrow_mut();
        core.acknowledge_rejected_batch(batch_id)
            .map_err(|e| JsError::new(&format!("Acknowledge rejected batch failed: {e}")))
    }

    #[wasm_bindgen(js_name = sealBatch)]
    pub fn seal_batch(&self, batch_id: &str) -> Result<(), JsError> {
        let batch_id = parse_batch_id_input(batch_id).map_err(|err| JsError::new(&err))?;
        let mut core = self.core.borrow_mut();
        core.seal_batch(batch_id)
            .map_err(|e| JsError::new(&format!("Seal batch failed: {e}")))
    }

    // =========================================================================
    // Subscriptions
    // =========================================================================

    /// Subscribe to a query with a callback.
    ///
    /// Default behavior matches RuntimeCore:
    /// - with upstream server: first callback waits for protocol QuerySettled convergence
    /// - without upstream server: first callback is local-immediate
    ///
    /// Pass durability options to override this default.
    ///
    /// # Returns
    /// Subscription handle (f64) for later unsubscription.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen]
    pub fn subscribe(
        &self,
        query_json: &str,
        on_update: Function,
        session_json: Option<String>,
        settled_tier: Option<String>,
        options_json: Option<String>,
    ) -> Result<f64, JsError> {
        let _span = debug_span!("wasm::subscribe", tier = self.tier_label).entered();
        let (query, session, durability, propagation) =
            parse_subscription_inputs(query_json, session_json, settled_tier, options_json)?;
        let callback = make_subscription_callback(on_update);

        let handle = self
            .core
            .borrow_mut()
            .subscribe_with_durability_and_propagation(
                query,
                callback,
                session,
                durability,
                propagation,
            )
            .map_err(|e| JsError::new(&format!("Subscribe failed: {:?}", e)))?;

        let subscription_id = handle.0;
        tracing::debug!(subscription_id, "subscribed");
        Ok(subscription_id as f64)
    }

    /// Unsubscribe from a query.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen]
    pub fn unsubscribe(&self, handle: f64) {
        let sub_id = handle as u64;
        let _span = tracing::debug_span!("wasm::unsubscribe", sub_id).entered();
        self.core
            .borrow_mut()
            .unsubscribe(SubscriptionHandle(sub_id));
    }

    /// Phase 1 of 2-phase subscribe: allocate a handle and store query params.
    /// No compilation, no sync, no tick — just bookkeeping.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = createSubscription)]
    pub fn create_subscription(
        &self,
        query_json: &str,
        session_json: Option<String>,
        settled_tier: Option<String>,
        options_json: Option<String>,
    ) -> Result<f64, JsError> {
        let _span = debug_span!("wasm::createSubscription", tier = self.tier_label).entered();
        let (query, session, durability, propagation) =
            parse_subscription_inputs(query_json, session_json, settled_tier, options_json)?;

        let handle =
            self.core
                .borrow_mut()
                .create_subscription(query, session, durability, propagation);

        tracing::debug!(handle = handle.0, "subscription created (pending)");
        Ok(handle.0 as f64)
    }

    /// Phase 2 of 2-phase subscribe: compile graph, register subscription,
    /// sync to servers, attach callback, and deliver the first delta.
    ///
    /// No-ops silently if the handle was already unsubscribed.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = executeSubscription)]
    pub fn execute_subscription(&self, handle: f64, on_update: Function) -> Result<(), JsError> {
        let sub_handle = SubscriptionHandle(handle as u64);
        let _span = debug_span!(
            "wasm::executeSubscription",
            handle = sub_handle.0,
            tier = self.tier_label
        )
        .entered();
        let callback = make_subscription_callback(on_update);

        self.core
            .borrow_mut()
            .execute_subscription(sub_handle, callback)
            .map_err(|e| JsError::new(&format!("Execute subscription failed: {:?}", e)))?;

        Ok(())
    }

    // =========================================================================
    // Sync Operations
    // =========================================================================

    /// Add a server connection.
    ///
    /// After adding the server, immediately flushes the outbox so that
    /// catalogue sync messages (from queue_full_sync_to_server) are sent
    /// before the call returns, rather than being deferred to a microtask.
    #[wasm_bindgen(js_name = addServer)]
    pub fn add_server(
        &self,
        server_catalogue_state_hash: Option<String>,
        next_sync_seq: Option<f64>,
    ) -> Result<(), JsError> {
        let _span = info_span!("wasm::addServer", tier = self.tier_label).entered();
        let server_id = {
            let mut slot = self.upstream_server_id.borrow_mut();
            if let Some(server_id) = *slot {
                server_id
            } else {
                let server_id = ServerId::new();
                *slot = Some(server_id);
                server_id
            }
        };
        let mut core = self.core.borrow_mut();
        // Re-attach semantics: remove existing upstream edge then add again so
        // replay/full-sync runs on every successful reconnect.
        core.remove_server(server_id);
        core.add_server_with_catalogue_state_hash(
            server_id,
            server_catalogue_state_hash.as_deref(),
        );
        if let Some(next_sync_seq) = Self::parse_optional_sequence(next_sync_seq)? {
            core.set_next_expected_server_sequence(server_id, next_sync_seq);
        }
        core.batched_tick();
        Ok(())
    }

    /// Remove the current upstream server connection.
    #[wasm_bindgen(js_name = removeServer)]
    pub fn remove_server(&self) {
        let mut core = self.core.borrow_mut();
        if let Some(server_id) = *self.upstream_server_id.borrow() {
            core.remove_server(server_id);
        }
    }

    /// Add a client connection (for server-side use in tests).
    #[wasm_bindgen(js_name = addClient)]
    pub fn add_client(&self) -> String {
        let _span = info_span!("wasm::addClient", tier = self.tier_label).entered();
        let client_id = ClientId::new();
        info!(%client_id, "generated client id");
        let mut core = self.core.borrow_mut();
        core.add_client(client_id, None);
        client_id.0.to_string()
    }

    /// Set a client's role.
    ///
    /// # Arguments
    /// * `client_id` - UUID string of the client
    /// * `role` - One of "user", "admin", "peer"
    #[wasm_bindgen(js_name = setClientRole)]
    pub fn set_client_role(&self, client_id: &str, role: &str) -> Result<(), JsError> {
        use jazz_tools::sync_manager::ClientRole;

        let uuid = uuid::Uuid::parse_str(client_id)
            .map_err(|e| JsError::new(&format!("Invalid client ID: {}", e)))?;
        let cid = ClientId(uuid);

        let client_role = match role {
            "user" => ClientRole::User,
            "admin" => ClientRole::Admin,
            "peer" => ClientRole::Peer,
            _ => {
                return Err(JsError::new(&format!(
                    "Invalid role '{}'. Must be 'user', 'admin', or 'peer'.",
                    role
                )));
            }
        };

        self.core
            .borrow_mut()
            .set_client_role_by_name(cid, client_role);
        Ok(())
    }

    // =========================================================================
    // Schema Access
    // =========================================================================

    /// Get the current schema as JSON.
    #[wasm_bindgen(js_name = getSchema)]
    pub fn get_schema(&self) -> Result<JsValue, JsError> {
        let core = self.core.borrow();
        let schema = core.current_schema();
        let wasm_schema = schema.clone();
        Ok(serde_wasm_bindgen::to_value(&wasm_schema)?)
    }

    /// Get the canonical schema hash (64-char hex).
    #[wasm_bindgen(js_name = getSchemaHash)]
    pub fn get_schema_hash(&self) -> String {
        let core = self.core.borrow();
        let schema = core.current_schema();
        SchemaHash::compute(schema).to_string()
    }

    /// Debug helper: expose schema/lens state currently loaded in SchemaManager.
    #[wasm_bindgen(js_name = __debugSchemaState)]
    pub fn debug_schema_state(&self) -> Result<JsValue, JsError> {
        let core = self.core.borrow();
        let schema_manager = core.schema_manager();

        let mut live_schema_hashes: Vec<String> = schema_manager
            .all_live_hashes()
            .into_iter()
            .map(|hash| hash.to_string())
            .collect();
        live_schema_hashes.sort();

        let mut known_schema_hashes: Vec<String> = schema_manager
            .known_schema_hashes()
            .into_iter()
            .map(|hash| hash.to_string())
            .collect();
        known_schema_hashes.sort();

        let mut pending_schema_hashes: Vec<String> = schema_manager
            .pending_schema_hashes()
            .into_iter()
            .map(|hash| hash.to_string())
            .collect();
        pending_schema_hashes.sort();

        let mut lens_edges: Vec<WasmLensEdgeDebug> = schema_manager
            .lens_edges()
            .into_iter()
            .map(|(source_hash, target_hash)| WasmLensEdgeDebug {
                source_hash: source_hash.to_string(),
                target_hash: target_hash.to_string(),
            })
            .collect();
        lens_edges.sort_by(|left, right| {
            left.source_hash
                .cmp(&right.source_hash)
                .then(left.target_hash.cmp(&right.target_hash))
        });

        let state = WasmSchemaStateDebug {
            current_schema_hash: schema_manager.current_hash().to_string(),
            live_schema_hashes,
            known_schema_hashes,
            pending_schema_hashes,
            lens_edges,
        };

        serde_wasm_bindgen::to_value(&state).map_err(|error| {
            JsError::new(&format!(
                "Failed to serialize debug schema state: {:?}",
                error
            ))
        })
    }

    /// Debug helper: seed a historical schema and persist schema/lens catalogue objects.
    #[wasm_bindgen(js_name = __debugSeedLiveSchema)]
    pub fn debug_seed_live_schema(&self, schema_json: &str) -> Result<(), JsError> {
        let schema = jazz_tools::binding_support::parse_runtime_schema_input(schema_json)
            .map_err(|e| JsError::new(&format!("Invalid schema JSON: {}", e)))?
            .schema;

        let mut core = self.core.borrow_mut();
        core.add_live_schema_and_persist_catalogue(schema)
            .map_err(|e| JsError::new(&format!("Failed to seed live schema: {:?}", e)))?;

        // Process pending updates and flush outbox so peer/main runtime can receive catalogue sync.
        core.immediate_tick();
        core.batched_tick();

        Ok(())
    }

    /// Flush all data to persistent storage (snapshot).
    #[wasm_bindgen]
    pub fn flush(&self) {
        let _span = debug_span!("wasm::flush", tier = self.tier_label).entered();
        self.core.borrow().flush_storage();
    }

    /// Flush only the WAL buffer to OPFS (not the snapshot).
    #[wasm_bindgen(js_name = flushWal)]
    pub fn flush_wal(&self) {
        let _span = debug_span!("wasm::flushWal", tier = self.tier_label).entered();
        self.core.borrow().flush_wal();
    }

    /// Create a persistent WasmRuntime backed by OPFS.
    ///
    /// Opens a single OPFS file namespace and restores state from the latest
    /// durable checkpoint.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openPersistent)]
    pub async fn open_persistent(
        schema_json: &str,
        app_id: &str,
        env: &str,
        user_branch: &str,
        db_name: &str,
        tier: Option<String>,
        use_binary_encoding: bool,
    ) -> Result<WasmRuntime, JsValue> {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();
        init_tracing();

        let tier_label = tier_label_for_node_tier(tier.as_deref());
        let _span = info_span!(
            "WasmRuntime::openPersistent",
            tier = tier_label,
            app_id,
            env,
            user_branch,
            db_name
        )
        .entered();
        info!("opening persistent OPFS runtime");

        let app_id = AppId::from_string(app_id).unwrap_or_else(|_| AppId::from_name(app_id));
        let mut schema_manager =
            build_schema_manager(schema_json, app_id, env, user_branch, tier.as_deref())
                .map_err(JsValue::from)?;

        let storage: Box<dyn Storage> = Box::new(
            OpfsBTreeStorage::open_opfs(db_name, DEFAULT_OPFS_CACHE_SIZE)
                .await
                .map_err(|e| {
                    if let jazz_tools::storage::StorageError::SecurityError(ref msg) = e {
                        let err = js_sys::Error::new(msg);
                        err.set_name("SecurityError");
                        JsValue::from(err)
                    } else {
                        JsValue::from(JsError::new(&format!("Storage: {:?}", e)))
                    }
                })?,
        );

        if let Err(error) =
            rehydrate_schema_manager_from_catalogue(&mut schema_manager, storage.as_ref(), app_id)
        {
            warn!(
                %app_id,
                ?error,
                "failed to rehydrate schema manager from catalogue storage"
            );
        }

        Ok(assemble_wasm_runtime(
            schema_manager,
            storage,
            tier_label,
            use_binary_encoding,
        ))
    }

    /// Create an ephemeral WasmRuntime backed by in-memory storage.
    ///
    /// Data is not persisted across page loads. Used as a fallback when OPFS
    /// is unavailable (e.g. Firefox private browsing mode).
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = openEphemeral)]
    pub fn open_ephemeral(
        schema_json: &str,
        app_id: &str,
        env: &str,
        user_branch: &str,
        db_name: &str,
        tier: Option<String>,
        use_binary_encoding: bool,
    ) -> Result<WasmRuntime, JsError> {
        #[cfg(feature = "console_error_panic_hook")]
        console_error_panic_hook::set_once();
        init_tracing();

        let tier_label = tier_label_for_node_tier(tier.as_deref());
        let _span = info_span!(
            "WasmRuntime::openEphemeral",
            tier = tier_label,
            app_id,
            env,
            user_branch,
            db_name
        )
        .entered();
        info!("opening ephemeral in-memory runtime (OPFS unavailable)");

        let app_id = AppId::from_string(app_id).unwrap_or_else(|_| AppId::from_name(app_id));
        let schema_manager =
            build_schema_manager(schema_json, app_id, env, user_branch, tier.as_deref())?;

        let storage: Box<dyn Storage> = Box::new(MemoryStorage::new());

        Ok(assemble_wasm_runtime(
            schema_manager,
            storage,
            tier_label,
            use_binary_encoding,
        ))
    }
}

fn decode_seed(seed_b64: &str) -> Result<[u8; 32], JsError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|e| JsError::new(&format!("seed base64 decode error: {e}")))?;
    let arr: [u8; 32] = bytes
        .try_into()
        .map_err(|_| JsError::new("seed must be exactly 32 bytes"))?;
    Ok(arr)
}

#[wasm_bindgen]
impl WasmRuntime {
    #[wasm_bindgen(js_name = "deriveUserId")]
    pub fn derive_user_id_static(seed_b64: &str) -> Result<String, JsError> {
        let seed = decode_seed(seed_b64)?;
        let user_id = identity::derive_user_id(&seed);
        Ok(user_id.to_string())
    }

    #[wasm_bindgen(js_name = "mintJazzSelfSignedToken")]
    pub fn mint_jazz_self_signed_token_static(
        seed_b64: &str,
        issuer: &str,
        audience: &str,
        ttl_seconds: u64,
        now_seconds: u64,
    ) -> Result<String, JsError> {
        let seed = decode_seed(seed_b64)?;
        // Resolve issuer string to a known &'static str.
        let static_issuer: &'static str = match issuer {
            identity::LOCAL_FIRST_ISSUER => identity::LOCAL_FIRST_ISSUER,
            identity::ANONYMOUS_ISSUER => identity::ANONYMOUS_ISSUER,
            other => return Err(JsError::new(&format!("unknown issuer: {other}"))),
        };
        identity::mint_jazz_self_signed_token_at(
            &seed,
            static_issuer,
            audience,
            ttl_seconds,
            now_seconds,
        )
        .map_err(|e| JsError::new(&e))
    }

    #[wasm_bindgen(js_name = "getPublicKeyBase64url")]
    pub fn get_public_key_b64_static(seed_b64: &str) -> Result<String, JsError> {
        let seed = decode_seed(seed_b64)?;
        let verifying_key = identity::derive_verifying_key(&seed);
        Ok(URL_SAFE_NO_PAD.encode(verifying_key.as_bytes()))
    }

    /// Connect to a Jazz server over WebSocket.
    ///
    /// Parses `auth_json` into `AuthConfig`, wires a `TransportManager` into
    /// `RuntimeCore`, and spawns the manager loop via `spawn_local`.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen]
    pub fn connect(&self, url: String, auth_json: String) -> Result<(), JsValue> {
        let auth: jazz_tools::transport_manager::AuthConfig =
            serde_json::from_str(&auth_json).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let scheduler = self.core.borrow().scheduler().clone();
        let tick = WasmTickNotifier { scheduler };
        let manager = {
            let mut core = self.core.borrow_mut();
            jazz_tools::runtime_core::install_transport::<_, _, crate::ws_stream::WasmWsStream, _>(
                &mut core, url, auth, tick,
            )
        };
        wasm_bindgen_futures::spawn_local(manager.run());
        Ok(())
    }

    /// Disconnect from the Jazz server and drop the transport handle.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen]
    pub fn disconnect(&self) {
        let mut core = self.core.borrow_mut();
        // Signal the manager to shut down before dropping the handle.
        if let Some(handle) = core.transport() {
            handle.disconnect();
        }
        // Drop the borrow before mutably clearing.
        core.clear_transport();
    }

    /// Push updated auth credentials into the live transport.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = "updateAuth")]
    pub fn update_auth(&self, auth_json: String) -> Result<(), JsValue> {
        let auth: jazz_tools::transport_manager::AuthConfig =
            serde_json::from_str(&auth_json).map_err(|e| JsValue::from_str(&e.to_string()))?;
        let core = self.core.borrow();
        if let Some(handle) = core.transport() {
            handle.update_auth(auth);
        }
        Ok(())
    }

    /// Register a JS callback that fires when the Rust transport receives an
    /// auth failure (Unauthorized) from the server during the WS handshake.
    ///
    /// The callback receives a single string argument: a human-readable reason.
    #[cfg(target_arch = "wasm32")]
    #[wasm_bindgen(js_name = "onAuthFailure")]
    pub fn on_auth_failure(&self, callback: Function) {
        // WASM is single-threaded; wrapping Function in a Send marker is safe here.
        struct SendFunction(Function);
        // SAFETY: WASM runs on a single thread; no concurrent access is possible.
        unsafe impl Send for SendFunction {}

        let send_fn = SendFunction(callback);
        self.core
            .borrow_mut()
            .set_auth_failure_callback(move |reason| {
                let reason_js = JsValue::from_str(&reason);
                let _ = send_fn.0.call1(&JsValue::NULL, &reason_js);
            });
    }
}

// ============================================================================
// WasmTickNotifier
// ============================================================================

/// `TickNotifier` implementation for the WASM runtime.
///
/// Holds a clone of `WasmScheduler` and calls `schedule_batched_tick()`
/// whenever the transport layer needs to wake up `batched_tick`.
#[cfg(target_arch = "wasm32")]
struct WasmTickNotifier {
    scheduler: WasmScheduler,
}

#[cfg(target_arch = "wasm32")]
impl jazz_tools::transport_manager::TickNotifier for WasmTickNotifier {
    fn notify(&self) {
        self.scheduler.schedule_batched_tick();
    }
}
