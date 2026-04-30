//! JazzClient implementation.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::jazz_tokio::{SubscriptionHandle as RuntimeSubHandle, TokioRuntime};
use crate::query_manager::manager::LocalUpdates;
use crate::query_manager::query::Query;
use crate::query_manager::session::Session;
use crate::query_manager::types::{OrderedRowDelta, RowDescriptor, Schema, TableName, Value};
use crate::runtime_core::ReadDurabilityOptions;
use crate::schema_manager::{SchemaManager, rehydrate_schema_manager_from_catalogue};
#[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
use crate::storage::SqliteStorage;
use crate::storage::{MemoryStorage, Storage};
#[cfg(feature = "rocksdb")]
use crate::storage::{RocksDBStorage, StorageError};
use crate::sync_manager::{ClientId, DurabilityTier, OutboxEntry, SyncManager};
use crate::transport_manager::AuthConfig as WsAuthConfig;
use base64::Engine;
use serde::Deserialize;
use tokio::sync::{RwLock, mpsc};
use uuid::Uuid;

use crate::{
    AppContext, AppId, ClientStorage, JazzError, ObjectId, Result, SubscriptionHandle,
    SubscriptionStream,
};

type DynStorage = Box<dyn Storage + Send>;
type ClientRuntime = TokioRuntime<DynStorage>;

#[derive(Debug, Deserialize)]
struct UnverifiedJwtClaims {
    sub: String,
    #[serde(default)]
    claims: serde_json::Value,
}

/// Jazz client for building applications.
///
/// Combines local storage with server sync.
pub struct JazzClient {
    /// Schema as declared by the client/app code.
    declared_schema: Schema,
    /// Session inferred from client auth context for user-scoped operations.
    default_session: Option<Session>,
    /// Handle to the local runtime.
    runtime: ClientRuntime,
    /// Whether a server URL was provided at construction time.
    has_server: bool,
    /// Active subscriptions (metadata).
    subscriptions: Arc<RwLock<HashMap<SubscriptionHandle, SubscriptionState>>>,
    /// Next subscription handle ID.
    next_handle: std::sync::atomic::AtomicU64,
}

/// State for an active subscription.
struct SubscriptionState {
    runtime_handle: RuntimeSubHandle,
}

fn build_client_schema_manager<S: Storage + ?Sized>(
    storage: &S,
    context: &AppContext,
) -> Result<SchemaManager> {
    let sync_manager = SyncManager::new();
    let mut schema_manager = SchemaManager::new(
        sync_manager,
        context.schema.clone(),
        context.app_id,
        "client",
        "main",
    )
    .map_err(|e| JazzError::Schema(format!("{:?}", e)))?;

    rehydrate_schema_manager_from_catalogue(&mut schema_manager, storage, context.app_id)
        .map_err(JazzError::Storage)?;

    Ok(schema_manager)
}

fn session_from_unverified_jwt(token: &str) -> Option<Session> {
    let payload = token.split('.').nth(1)?;
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(payload))
        .ok()?;
    let claims: UnverifiedJwtClaims = serde_json::from_slice(&payload).ok()?;
    let user_id = claims.sub.trim();
    if user_id.is_empty() {
        return None;
    }

    Some(Session {
        user_id: user_id.to_string(),
        claims: claims.claims,
        ..Session::new(user_id)
    })
}

fn default_session_from_context(context: &AppContext) -> Option<Session> {
    if context.backend_secret.is_some() || context.admin_secret.is_some() {
        return None;
    }

    context
        .jwt_token
        .as_deref()
        .and_then(session_from_unverified_jwt)
}

async fn wait_for_initial_transport_handshake(
    runtime: &ClientRuntime,
    timeout_after: Duration,
) -> Result<()> {
    let connected = tokio::time::timeout(timeout_after, runtime.transport_wait_until_connected())
        .await
        .map_err(|_| {
            JazzError::Connection(
                "timed out waiting for WebSocket handshake to complete".to_string(),
            )
        })?;
    if !connected {
        return Err(JazzError::Connection(
            "transport closed before WebSocket handshake completed".to_string(),
        ));
    }
    Ok(())
}

impl JazzClient {
    /// Connect to Jazz with the given configuration.
    ///
    /// This will:
    /// 1. Open local storage
    /// 2. Initialize the runtime
    /// 3. Connect to the server over WebSocket (if URL provided)
    /// 4. Wait for the initial WS handshake to complete
    pub async fn connect(context: AppContext) -> Result<Self> {
        let declared_schema = context.schema.clone();
        let default_session = default_session_from_context(&context);
        // Loaded for its side effect of persisting the client-id file on disk;
        // the wire ClientId is assigned by `TransportManager::create` at connect
        // time and is exposed via `runtime.transport_client_id()`.
        let _client_id = match context.storage {
            ClientStorage::Persistent => load_or_create_persistent_client_id(&context)?,
            ClientStorage::Memory => context.client_id.unwrap_or_default(),
        };

        let storage: DynStorage = match context.storage {
            ClientStorage::Persistent => open_persistent_storage(&context.data_dir).await?,
            ClientStorage::Memory => Box::new(MemoryStorage::new()),
        };

        let schema_manager = build_client_schema_manager(&storage, &context)?;

        // Create runtime. The sync callback is a no-op — the WS TransportManager
        // drives the outbox directly via its own channel.
        let runtime = TokioRuntime::new(schema_manager, storage, move |_entry: OutboxEntry| {});

        // Attach the tracer to the runtime so all outbox/inbox traffic is
        // recorded under the participant name.
        if let Some((ref tracer, ref name)) = context.sync_tracer {
            runtime.set_sync_tracer(tracer.clone(), name.clone());
        }

        // Persist schema to catalogue for server sync
        runtime
            .persist_schema()
            .map_err(|e| JazzError::Storage(e.to_string()))?;

        let has_server = !context.server_url.is_empty();

        if has_server {
            let ws_url = http_url_to_ws(&context.server_url, context.app_id)?;
            let auth = WsAuthConfig {
                jwt_token: context.jwt_token.clone(),
                backend_secret: context.backend_secret.clone(),
                admin_secret: context.admin_secret.clone(),
                backend_session: None,
            };
            runtime.connect(ws_url, auth);

            // Register the transport's wire ClientId with the tracer so the
            // server's outbox recorder can resolve `Destination::Client(cid)`
            // to the human-readable participant name.
            if let Some((ref tracer, ref name)) = context.sync_tracer
                && let Some(wire_cid) = runtime.transport_client_id()
            {
                tracer.register_client(wire_cid, name);
            }

            // Wait until the WS handshake has completed at least once.
            // `batched_tick` handles `TransportInbound::Connected` automatically —
            // it calls `add_server_with_catalogue_state_hash` — so we only need
            // to gate here until that first tick fires.
            wait_for_initial_transport_handshake(&runtime, Duration::from_secs(10)).await?;
        }

        Ok(Self {
            declared_schema,
            default_session,
            runtime,
            has_server,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
            next_handle: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Subscribe to a query.
    ///
    /// Returns a stream of row deltas as the data changes.
    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        self.subscribe_internal(query, self.default_session.clone())
            .await
    }

    /// Internal subscribe with optional session.
    async fn subscribe_internal(
        &self,
        query: Query,
        session: Option<Session>,
    ) -> Result<SubscriptionStream> {
        let handle = SubscriptionHandle(
            self.next_handle
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst),
        );

        // Create channel for this subscription's deltas.
        // tx is moved directly into the callback so the delta is never dropped due
        // to the race where immediate_tick fires the callback before we can insert
        // tx into a shared map.
        let (tx, rx) = mpsc::unbounded_channel::<OrderedRowDelta>();

        // Register with runtime using callback pattern
        // The callback bridges runtime updates to the channel
        let runtime_handle = self
            .runtime
            .subscribe(
                query.clone(),
                move |delta| {
                    // Route delta to the subscription stream without dropping
                    // updates when the consumer falls briefly behind.
                    let _ = tx.send(delta.ordered_delta);
                },
                session,
            )
            .map_err(|e| JazzError::Query(e.to_string()))?;

        // Track subscription metadata
        {
            let mut subs = self.subscriptions.write().await;
            subs.insert(handle, SubscriptionState { runtime_handle });
        }

        Ok(SubscriptionStream::new(rx))
    }

    /// One-shot query, optionally waiting for a durability tier.
    ///
    /// Returns the current results as `Vec<(ObjectId, Vec<Value>)>`.
    pub async fn query(
        &self,
        query: Query,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        let query_for_alignment = query.clone();
        let future = self
            .runtime
            .query(
                query,
                self.default_session.clone(),
                ReadDurabilityOptions {
                    tier: durability_tier,
                    local_updates: LocalUpdates::Immediate,
                },
            )
            .map_err(|e| JazzError::Query(e.to_string()))?;
        future
            .await
            .map(|rows| self.align_query_rows_to_declared_schema(&query_for_alignment, rows))
            .map_err(|e| JazzError::Query(format!("{:?}", e)))
    }

    /// Create a new row in a table.
    pub async fn create(
        &self,
        table: &str,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>)> {
        self.create_with_id(table, Option::<Uuid>::None, values)
            .await
    }

    /// Create a new row in a table using a caller-supplied UUID.
    pub async fn create_with_id(
        &self,
        table: &str,
        object_id: impl Into<Option<Uuid>>,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>)> {
        let (object_id, row_values, _batch_id) = self
            .runtime
            .insert_with_id(
                table,
                values,
                object_id.into().map(ObjectId::from_uuid),
                None,
            )
            .map_err(|e| JazzError::Write(e.to_string()))?;
        let row_values = match self.runtime.current_schema() {
            Ok(schema) => align_row_values_to_declared_schema(
                &self.declared_schema,
                &schema,
                &TableName::new(table),
                row_values,
            ),
            Err(_) => row_values,
        };
        Ok((object_id, row_values))
    }

    /// Create a new row and wait until it reaches the requested durability tier.
    pub async fn create_persisted(
        &self,
        table: &str,
        values: HashMap<String, Value>,
        tier: DurabilityTier,
    ) -> Result<(ObjectId, Vec<Value>)> {
        self.create_persisted_with_id(table, Option::<Uuid>::None, values, tier)
            .await
    }

    /// Create a new row with a caller-supplied UUID and wait for durability.
    pub async fn create_persisted_with_id(
        &self,
        table: &str,
        object_id: impl Into<Option<Uuid>>,
        values: HashMap<String, Value>,
        tier: DurabilityTier,
    ) -> Result<(ObjectId, Vec<Value>)> {
        let ((object_id, row_values), receiver) = self
            .runtime
            .insert_persisted_with_id(
                table,
                values,
                object_id.into().map(ObjectId::from_uuid),
                None,
                tier,
            )
            .map_err(|e| JazzError::Write(e.to_string()))?;
        let row_values = match self.runtime.current_schema() {
            Ok(schema) => align_row_values_to_declared_schema(
                &self.declared_schema,
                &schema,
                &TableName::new(table),
                row_values,
            ),
            Err(_) => row_values,
        };
        wait_for_persisted_write(receiver, "create row", tier).await?;
        Ok((object_id, row_values))
    }

    /// Create or update a row using a caller-supplied UUID.
    pub async fn upsert(
        &self,
        table: &str,
        object_id: Uuid,
        values: HashMap<String, Value>,
    ) -> Result<()> {
        self.runtime
            .upsert_with_id(table, ObjectId::from_uuid(object_id), values, None)
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Create or update a row and wait until it reaches the requested durability tier.
    pub async fn upsert_persisted(
        &self,
        table: &str,
        object_id: Uuid,
        values: HashMap<String, Value>,
        tier: DurabilityTier,
    ) -> Result<()> {
        let receiver = self
            .runtime
            .upsert_persisted_with_id(table, ObjectId::from_uuid(object_id), values, None, tier)
            .map_err(|e| JazzError::Write(e.to_string()))?;
        wait_for_persisted_write(receiver, "upsert row", tier).await
    }

    /// Update a row.
    pub async fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<()> {
        self.runtime
            .update(object_id, updates, None)
            .map(|_| ())
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Update a row and wait until it reaches the requested durability tier.
    pub async fn update_persisted(
        &self,
        object_id: ObjectId,
        updates: Vec<(String, Value)>,
        tier: DurabilityTier,
    ) -> Result<()> {
        let receiver = self
            .runtime
            .update_persisted(object_id, updates, None, tier)
            .map_err(|e| JazzError::Write(e.to_string()))?;
        wait_for_persisted_write(receiver, "update row", tier).await
    }

    /// Delete a row.
    pub async fn delete(&self, object_id: ObjectId) -> Result<()> {
        self.runtime
            .delete(object_id, None)
            .map(|_| ())
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    /// Delete a row and wait until it reaches the requested durability tier.
    pub async fn delete_persisted(&self, object_id: ObjectId, tier: DurabilityTier) -> Result<()> {
        let receiver = self
            .runtime
            .delete_persisted(object_id, None, tier)
            .map_err(|e| JazzError::Write(e.to_string()))?;
        wait_for_persisted_write(receiver, "delete row", tier).await
    }

    /// Unsubscribe from a subscription.
    pub async fn unsubscribe(&self, handle: SubscriptionHandle) -> Result<()> {
        let mut subs = self.subscriptions.write().await;
        if let Some(state) = subs.remove(&handle) {
            let _ = self.runtime.unsubscribe(state.runtime_handle);
        }
        Ok(())
    }

    /// Get the current schema.
    pub async fn schema(&self) -> Result<crate::query_manager::types::Schema> {
        self.runtime
            .current_schema()
            .map_err(|e| JazzError::Query(e.to_string()))
    }

    /// Check if connected to server.
    pub fn is_connected(&self) -> bool {
        self.has_server && self.runtime.transport_ever_connected()
    }

    /// Create a session-scoped client for backend operations.
    pub fn for_session(&self, session: Session) -> SessionClient<'_> {
        SessionClient {
            client: self,
            session,
        }
    }

    /// Shutdown the client and release resources.
    pub async fn shutdown(self) -> Result<()> {
        // Disconnect from server (drops the TransportHandle; manager task exits cleanly)
        if self.has_server {
            self.runtime.disconnect();
        }

        // Flush pending operations
        self.runtime
            .flush()
            .await
            .map_err(|e| JazzError::Connection(e.to_string()))?;

        // Flush storage state to disk for persistence
        self.runtime
            .with_storage(|storage| {
                storage.flush();
                storage.close()
            })
            .map_err(|e| JazzError::Storage(e.to_string()))?
            .map_err(|e| JazzError::Storage(e.to_string()))?;

        Ok(())
    }

    fn align_query_rows_to_declared_schema(
        &self,
        query: &Query,
        rows: Vec<(ObjectId, Vec<Value>)>,
    ) -> Vec<(ObjectId, Vec<Value>)> {
        if !query_rows_can_be_schema_aligned(query) {
            return rows;
        }

        let runtime_schema = match self.runtime.current_schema() {
            Ok(schema) => schema,
            Err(_) => return rows,
        };

        rows.into_iter()
            .map(|(id, values)| {
                (
                    id,
                    align_row_values_to_declared_schema(
                        &self.declared_schema,
                        &runtime_schema,
                        &query.table,
                        values,
                    ),
                )
            })
            .collect()
    }
}

/// Session-scoped client for backend operations.
pub struct SessionClient<'a> {
    client: &'a JazzClient,
    session: Session,
}

impl<'a> SessionClient<'a> {
    pub async fn create(
        &self,
        table: &str,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>)> {
        self.create_with_id(table, Option::<Uuid>::None, values)
            .await
    }

    pub async fn create_with_id(
        &self,
        table: &str,
        object_id: impl Into<Option<Uuid>>,
        values: HashMap<String, Value>,
    ) -> Result<(ObjectId, Vec<Value>)> {
        let (object_id, row_values, _batch_id) = self
            .client
            .runtime
            .insert_with_id(
                table,
                values,
                object_id.into().map(ObjectId::from_uuid),
                Some(&self.session),
            )
            .map_err(|e| JazzError::Write(e.to_string()))?;
        let row_values = match self.client.runtime.current_schema() {
            Ok(schema) => align_row_values_to_declared_schema(
                &self.client.declared_schema,
                &schema,
                &TableName::new(table),
                row_values,
            ),
            Err(_) => row_values,
        };
        Ok((object_id, row_values))
    }

    pub async fn create_persisted(
        &self,
        table: &str,
        values: HashMap<String, Value>,
        tier: DurabilityTier,
    ) -> Result<(ObjectId, Vec<Value>)> {
        self.create_persisted_with_id(table, Option::<Uuid>::None, values, tier)
            .await
    }

    pub async fn create_persisted_with_id(
        &self,
        table: &str,
        object_id: impl Into<Option<Uuid>>,
        values: HashMap<String, Value>,
        tier: DurabilityTier,
    ) -> Result<(ObjectId, Vec<Value>)> {
        let ((object_id, row_values), receiver) = self
            .client
            .runtime
            .insert_persisted_with_id(
                table,
                values,
                object_id.into().map(ObjectId::from_uuid),
                Some(&self.session),
                tier,
            )
            .map_err(|e| JazzError::Write(e.to_string()))?;
        let row_values = match self.client.runtime.current_schema() {
            Ok(schema) => align_row_values_to_declared_schema(
                &self.client.declared_schema,
                &schema,
                &TableName::new(table),
                row_values,
            ),
            Err(_) => row_values,
        };
        wait_for_persisted_write(receiver, "create row", tier).await?;
        Ok((object_id, row_values))
    }

    pub async fn upsert(
        &self,
        table: &str,
        object_id: Uuid,
        values: HashMap<String, Value>,
    ) -> Result<()> {
        self.client
            .runtime
            .upsert_with_id(
                table,
                ObjectId::from_uuid(object_id),
                values,
                Some(&self.session),
            )
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    pub async fn upsert_persisted(
        &self,
        table: &str,
        object_id: Uuid,
        values: HashMap<String, Value>,
        tier: DurabilityTier,
    ) -> Result<()> {
        let receiver = self
            .client
            .runtime
            .upsert_persisted_with_id(
                table,
                ObjectId::from_uuid(object_id),
                values,
                Some(&self.session),
                tier,
            )
            .map_err(|e| JazzError::Write(e.to_string()))?;
        wait_for_persisted_write(receiver, "upsert row", tier).await
    }

    pub async fn update(&self, object_id: ObjectId, updates: Vec<(String, Value)>) -> Result<()> {
        self.client
            .runtime
            .update(object_id, updates, Some(&self.session))
            .map(|_| ())
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    pub async fn update_persisted(
        &self,
        object_id: ObjectId,
        updates: Vec<(String, Value)>,
        tier: DurabilityTier,
    ) -> Result<()> {
        let receiver = self
            .client
            .runtime
            .update_persisted(object_id, updates, Some(&self.session), tier)
            .map_err(|e| JazzError::Write(e.to_string()))?;
        wait_for_persisted_write(receiver, "update row", tier).await
    }

    pub async fn delete(&self, object_id: ObjectId) -> Result<()> {
        self.client
            .runtime
            .delete(object_id, Some(&self.session))
            .map(|_| ())
            .map_err(|e| JazzError::Write(e.to_string()))
    }

    pub async fn delete_persisted(&self, object_id: ObjectId, tier: DurabilityTier) -> Result<()> {
        let receiver = self
            .client
            .runtime
            .delete_persisted(object_id, Some(&self.session), tier)
            .map_err(|e| JazzError::Write(e.to_string()))?;
        wait_for_persisted_write(receiver, "delete row", tier).await
    }

    pub async fn query(
        &self,
        query: Query,
        durability_tier: Option<DurabilityTier>,
    ) -> Result<Vec<(ObjectId, Vec<Value>)>> {
        let query_for_alignment = query.clone();
        let future = self
            .client
            .runtime
            .query(
                query,
                Some(self.session.clone()),
                ReadDurabilityOptions {
                    tier: durability_tier,
                    local_updates: LocalUpdates::Immediate,
                },
            )
            .map_err(|e| JazzError::Query(e.to_string()))?;
        future
            .await
            .map(|rows| {
                self.client
                    .align_query_rows_to_declared_schema(&query_for_alignment, rows)
            })
            .map_err(|e| JazzError::Query(format!("{:?}", e)))
    }

    pub async fn subscribe(&self, query: Query) -> Result<SubscriptionStream> {
        self.client
            .subscribe_internal(query, Some(self.session.clone()))
            .await
    }
}

fn query_rows_can_be_schema_aligned(query: &Query) -> bool {
    query.joins.is_empty()
        && query.array_subqueries.is_empty()
        && query.recursive.is_none()
        && query.select_columns.is_none()
        && query.result_element_index.is_none()
}

async fn wait_for_persisted_write(
    receiver: futures::channel::oneshot::Receiver<crate::runtime_core::PersistedWriteAck>,
    operation: &str,
    tier: DurabilityTier,
) -> Result<()> {
    receiver
        .await
        .map_err(|_| {
            JazzError::Sync(format!(
                "{operation} was cancelled before reaching {tier:?} durability"
            ))
        })?
        .map_err(|rejection| {
            JazzError::Sync(format!(
                "{operation} was rejected before reaching {tier:?} durability ({}): {}",
                rejection.code, rejection.reason
            ))
        })?;
    Ok(())
}

fn align_row_values_to_declared_schema(
    declared_schema: &Schema,
    runtime_schema: &Schema,
    table: &TableName,
    values: Vec<Value>,
) -> Vec<Value> {
    let Some(declared_table) = declared_schema.get(table) else {
        return values;
    };
    let Some(runtime_table) = runtime_schema.get(table) else {
        return values;
    };

    reorder_values_by_column_name(&runtime_table.columns, &declared_table.columns, &values)
        .unwrap_or(values)
}

fn reorder_values_by_column_name(
    source_descriptor: &RowDescriptor,
    target_descriptor: &RowDescriptor,
    values: &[Value],
) -> Option<Vec<Value>> {
    if values.len() != source_descriptor.columns.len()
        || source_descriptor.columns.len() != target_descriptor.columns.len()
    {
        return None;
    }

    let mut values_by_column = HashMap::with_capacity(values.len());
    for (column, value) in source_descriptor.columns.iter().zip(values.iter()) {
        values_by_column.insert(column.name, value.clone());
    }

    let mut reordered_values = Vec::with_capacity(values.len());
    for column in &target_descriptor.columns {
        reordered_values.push(values_by_column.remove(&column.name)?);
    }

    Some(reordered_values)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::*;
    #[cfg(feature = "rocksdb")]
    use crate::query_manager::policy::PolicyExpr;
    #[cfg(feature = "rocksdb")]
    use crate::query_manager::types::{SchemaHash, TablePolicies};
    #[cfg(feature = "rocksdb")]
    use crate::runtime_core::{NoopScheduler, RuntimeCore};
    use crate::schema_manager::AppId;
    #[cfg(feature = "rocksdb")]
    use crate::storage::RocksDBStorage;
    use crate::{ColumnType, ObjectId, SchemaBuilder, TableSchema};
    use serde_json::json;
    use tempfile::TempDir;

    fn declared_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean),
            )
            .build()
    }

    fn runtime_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("completed", ColumnType::Boolean)
                    .column("title", ColumnType::Text),
            )
            .build()
    }

    #[cfg(feature = "rocksdb")]
    fn learned_runtime_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("completed", ColumnType::Boolean)
                    .nullable_column("description", ColumnType::Text),
            )
            .build()
    }

    fn make_offline_context(
        app_id: AppId,
        data_dir: std::path::PathBuf,
        schema: Schema,
    ) -> AppContext {
        AppContext {
            app_id,
            client_id: None,
            schema,
            server_url: String::new(),
            data_dir,
            storage: ClientStorage::default(),
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
            sync_tracer: None,
        }
    }

    fn make_test_jwt(sub: &str, claims: serde_json::Value) -> String {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"alg":"none","typ":"JWT"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "sub": sub,
                "claims": claims,
            }))
            .expect("serialize jwt payload"),
        );
        format!("{header}.{payload}.sig")
    }

    #[cfg(feature = "rocksdb")]
    fn seed_rehydrated_client_storage(
        data_dir: &std::path::Path,
        app_id: AppId,
        publish_permissions: bool,
    ) -> (SchemaHash, SchemaHash) {
        std::fs::create_dir_all(data_dir).expect("create seeded client data dir");

        #[cfg(feature = "rocksdb")]
        let storage = {
            let db_path = data_dir.join("jazz.rocksdb");
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage")
        };
        let bundled_schema = declared_todo_schema();
        let learned_schema = learned_runtime_todo_schema();
        let bundled_hash = SchemaHash::compute(&bundled_schema);
        let learned_hash = SchemaHash::compute(&learned_schema);

        let schema_manager = SchemaManager::new(
            SyncManager::new(),
            learned_schema.clone(),
            app_id,
            "seed",
            "main",
        )
        .expect("seed schema manager");
        let mut runtime = RuntimeCore::new(schema_manager, storage, NoopScheduler);
        runtime.persist_schema();
        runtime.publish_schema(bundled_schema.clone());
        let lens = runtime
            .schema_manager()
            .generate_lens(&bundled_schema, &learned_schema);
        assert!(!lens.is_draft(), "seed lens should be publishable");
        runtime.publish_lens(&lens).expect("persist learned lens");

        if publish_permissions {
            runtime
                .publish_permissions_bundle(
                    learned_hash,
                    HashMap::from([(
                        TableName::new("todos"),
                        TablePolicies::new().with_select(PolicyExpr::True),
                    )]),
                    None,
                )
                .expect("seed permissions bundle");
        }

        let storage = runtime.into_storage();
        storage.flush();
        storage.close().expect("close seeded client storage");

        (bundled_hash, learned_hash)
    }

    #[cfg(feature = "rocksdb")]
    fn expected_client_catalogue_hash(context: &AppContext) -> String {
        #[cfg(feature = "rocksdb")]
        let storage = {
            let db_path = context.data_dir.join("jazz.rocksdb");
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage")
        };
        let schema_manager = build_client_schema_manager(&storage, context)
            .expect("rehydrate client schema manager");
        let catalogue_hash = schema_manager.catalogue_state_hash();
        storage.close().expect("close seeded client storage");
        catalogue_hash
    }

    #[cfg(feature = "rocksdb")]
    #[test]
    fn seeded_client_storage_persists_learned_schema_and_lens() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-seeded-storage");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, false);

        let db_path = data_dir.path().join("jazz.rocksdb");
        let storage =
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("open seeded client storage");

        let entries = storage
            .scan_catalogue_entries()
            .expect("scan seeded catalogue entries");
        let learned_object_id = learned_hash.to_object_id();
        assert!(
            entries
                .iter()
                .any(|entry| entry.object_id == learned_object_id),
            "seeded storage should persist the learned schema object"
        );
        assert!(
            entries.iter().any(|entry| entry.object_type()
                == Some(crate::metadata::ObjectType::CatalogueLens.as_str())),
            "seeded storage should persist at least one learned lens"
        );

        storage.close().expect("close seeded client storage");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn boxed_client_storage_rehydrates_learned_schema_from_catalogue() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-boxed-rehydrate");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, false);
        let context = make_offline_context(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
        );

        let concrete_storage = {
            let db_path = data_dir.path().join("jazz.rocksdb");
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024)
                .expect("open seeded client storage concretely")
        };
        let concrete_manager = build_client_schema_manager(&concrete_storage, &context)
            .expect("rehydrate schema manager from concrete storage");
        assert!(
            concrete_manager
                .known_schema_hashes()
                .contains(&learned_hash),
            "concrete storage rehydrate should learn the newer schema"
        );
        concrete_storage
            .close()
            .expect("close seeded client storage");

        let boxed_storage = open_persistent_storage(data_dir.path())
            .await
            .expect("open boxed client storage");
        let boxed_manager = build_client_schema_manager(boxed_storage.as_ref(), &context)
            .expect("rehydrate schema manager from boxed storage");
        assert!(
            boxed_manager.known_schema_hashes().contains(&learned_hash),
            "boxed client storage rehydrate should learn the newer schema"
        );
        boxed_storage.close().expect("close boxed client storage");
    }

    #[test]
    fn query_rows_are_reordered_back_to_declared_schema() {
        let aligned = align_row_values_to_declared_schema(
            &declared_todo_schema(),
            &runtime_todo_schema(),
            &TableName::new("todos"),
            vec![Value::Boolean(true), Value::Text("done".to_string())],
        );

        assert_eq!(
            aligned,
            vec![Value::Text("done".to_string()), Value::Boolean(true)]
        );
    }

    #[test]
    fn default_session_from_context_uses_jwt_claims_for_user_clients() {
        let app_id = AppId::from_name("client-jwt-session");
        let mut context = make_offline_context(
            app_id,
            TempDir::new().expect("tempdir").keep(),
            declared_todo_schema(),
        );
        context.jwt_token = Some(make_test_jwt("alice", json!({ "join_code": "secret-123" })));

        let session = default_session_from_context(&context).expect("derive session from jwt");
        assert_eq!(session.user_id, "alice");
        assert_eq!(session.claims["join_code"], "secret-123");
    }

    #[test]
    fn default_session_from_context_skips_backend_capable_clients() {
        let app_id = AppId::from_name("client-backend-session");
        let mut context = make_offline_context(
            app_id,
            TempDir::new().expect("tempdir").keep(),
            declared_todo_schema(),
        );
        context.jwt_token = Some(make_test_jwt("alice", json!({ "role": "user" })));
        context.backend_secret = Some("backend-secret".to_string());

        assert!(
            default_session_from_context(&context).is_none(),
            "backend/admin clients should keep using explicit SessionClient scopes"
        );
    }

    #[tokio::test]
    async fn initial_transport_handshake_wait_errors_when_transport_is_absent() {
        let app_id = AppId::from_name("client-missing-transport");
        let context = make_offline_context(
            app_id,
            TempDir::new().expect("tempdir").keep(),
            declared_todo_schema(),
        );
        let storage: DynStorage = Box::new(MemoryStorage::new());
        let schema_manager =
            build_client_schema_manager(storage.as_ref(), &context).expect("schema manager");
        let runtime = TokioRuntime::new(schema_manager, storage, |_entry: OutboxEntry| {});

        let result = wait_for_initial_transport_handshake(&runtime, Duration::from_secs(1)).await;

        match result {
            Err(JazzError::Connection(message)) => assert_eq!(
                message,
                "transport closed before WebSocket handshake completed"
            ),
            other => panic!("expected connection error for missing transport, got {other:?}"),
        }
    }

    #[test]
    fn simple_queries_are_schema_alignable() {
        let query = Query::new("todos");
        assert!(query_rows_can_be_schema_aligned(&query));
    }

    #[test]
    fn join_queries_are_not_schema_alignable() {
        let mut query = Query::new("todos");
        query.joins.push(crate::query_manager::query::JoinSpec {
            table: TableName::new("projects"),
            alias: None,
            on: Some(("project_id".to_string(), "id".to_string())),
        });

        assert!(!query_rows_can_be_schema_aligned(&query));
    }

    #[test]
    fn query_alignment_preserves_row_identity() {
        let object_id = ObjectId::new();
        let aligned = [(
            object_id,
            align_row_values_to_declared_schema(
                &declared_todo_schema(),
                &runtime_todo_schema(),
                &TableName::new("todos"),
                vec![Value::Boolean(false), Value::Text("keep-id".to_string())],
            ),
        )];

        assert_eq!(aligned[0].0, object_id);
        assert_eq!(
            aligned[0].1,
            vec![Value::Text("keep-id".to_string()), Value::Boolean(false)]
        );
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn client_rehydrates_learned_lens_from_local_catalogue_on_restart() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-rehydrate-lens");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, false);
        let context = make_offline_context(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
        );

        let client = JazzClient::connect(context).await.expect("connect client");

        let has_learned_schema = client
            .runtime
            .known_schema_hashes()
            .expect("read known schema hashes")
            .contains(&learned_hash);
        assert!(
            has_learned_schema,
            "client should restore newer learned schema"
        );

        let lens_path_len = client
            .runtime
            .with_schema_manager(|manager| manager.lens_path(&learned_hash).map(|path| path.len()))
            .expect("read client schema manager")
            .expect("lens path to bundled schema");
        assert_eq!(
            lens_path_len, 1,
            "client should restore learned migration lens"
        );

        client.shutdown().await.expect("shutdown client");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn client_rehydrates_permissions_head_and_bundle_from_local_catalogue_on_restart() {
        let data_dir = TempDir::new().expect("temp client dir");
        let app_id = AppId::from_name("client-rehydrate-permissions");
        let (_bundled_hash, learned_hash) =
            seed_rehydrated_client_storage(data_dir.path(), app_id, true);
        let context = make_offline_context(
            app_id,
            data_dir.path().to_path_buf(),
            declared_todo_schema(),
        );
        let expected_catalogue_hash = expected_client_catalogue_hash(&context);

        let client = JazzClient::connect(context).await.expect("connect client");

        let actual_catalogue_hash = client
            .runtime
            .catalogue_state_hash()
            .expect("read client catalogue hash");
        assert_eq!(
            actual_catalogue_hash, expected_catalogue_hash,
            "client should restore learned permissions head and bundle before any network sync"
        );

        let lens_path_exists = client
            .runtime
            .with_schema_manager(|manager| manager.lens_path(&learned_hash).is_ok())
            .expect("read client schema manager");
        assert!(
            lens_path_exists,
            "permissions rehydrate should preserve the target schema's learned lens context"
        );

        client.shutdown().await.expect("shutdown client");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn open_persistent_storage_retries_on_lock_contention() {
        let data_dir = TempDir::new().expect("temp dir");
        std::fs::create_dir_all(data_dir.path()).unwrap();

        let db_path = data_dir.path().join("jazz.rocksdb");
        // Hold the DB open so the next open hits a lock error.
        let _holder =
            RocksDBStorage::open(&db_path, 64 * 1024 * 1024).expect("first open should succeed");

        // Spawn a task that drops the holder after a short delay, unblocking the retry.
        let holder_handle = tokio::task::spawn_blocking({
            let holder = _holder;
            move || {
                std::thread::sleep(Duration::from_millis(150));
                drop(holder);
            }
        });

        // open_persistent_storage retries up to 100 times at 25ms intervals.
        // The holder is released after ~150ms, so this should succeed within a few retries.
        let storage = open_persistent_storage(data_dir.path()).await;
        assert!(
            storage.is_ok(),
            "should succeed after lock is released: {:?}",
            storage.err()
        );

        holder_handle.await.expect("holder task should complete");
    }

    #[cfg(feature = "rocksdb")]
    #[tokio::test]
    async fn open_persistent_storage_fails_on_non_lock_error() {
        // Point at a file (not a directory) so RocksDB gets a non-lock IO error.
        let data_dir = TempDir::new().expect("temp dir");
        let db_path = data_dir.path().join("jazz.rocksdb");
        // Create a regular file where rocksdb expects a directory.
        std::fs::write(&db_path, b"not a database").unwrap();

        let result = open_persistent_storage(data_dir.path()).await;
        assert!(
            result.is_err(),
            "non-lock errors should not be retried and should fail immediately"
        );
    }
}

fn load_or_create_persistent_client_id(context: &AppContext) -> Result<ClientId> {
    std::fs::create_dir_all(&context.data_dir)?;

    let client_id_path = context.data_dir.join("client_id");
    let client_id = if client_id_path.exists() {
        let id_str = std::fs::read_to_string(&client_id_path)?;
        ClientId::parse(id_str.trim()).unwrap_or_else(|| {
            let id = context.client_id.unwrap_or_default();
            let _ = std::fs::write(&client_id_path, id.to_string());
            id
        })
    } else if let Some(id) = context.client_id {
        std::fs::write(&client_id_path, id.to_string())?;
        id
    } else {
        let id = ClientId::new();
        std::fs::write(&client_id_path, id.to_string())?;
        id
    };

    Ok(client_id)
}

/// Convert an HTTP(S) server URL to the app-scoped WebSocket endpoint URL.
///
/// `http://host`, `my-app` → `ws://host/apps/my-app/ws`
/// `https://host` → `wss://host/apps/my-app/ws`
fn http_url_to_ws(server_url: &str, app_id: AppId) -> Result<String> {
    let trimmed = server_url.trim().trim_end_matches('/');
    let ws_suffix = format!("/apps/{}/ws", app_id);
    let (ws_scheme, rest) = if let Some(r) = trimmed.strip_prefix("https://") {
        ("wss", r)
    } else if let Some(r) = trimmed.strip_prefix("http://") {
        ("ws", r)
    } else if trimmed.starts_with("ws://") || trimmed.starts_with("wss://") {
        // Already a WS URL — replace any bare trailing /ws with the app-scoped path.
        let without_ws_suffix = trimmed.strip_suffix("/ws").unwrap_or(trimmed);
        return Ok(format!("{without_ws_suffix}{ws_suffix}"));
    } else {
        return Err(JazzError::Connection(format!(
            "invalid server URL '{server_url}': expected http:// or https://"
        )));
    };
    Ok(format!("{ws_scheme}://{rest}{ws_suffix}"))
}

async fn open_persistent_storage(data_dir: &std::path::Path) -> Result<DynStorage> {
    #[cfg(feature = "rocksdb")]
    {
        Ok(Box::new(open_rocksdb_storage(data_dir).await?))
    }
    #[cfg(all(feature = "sqlite", not(feature = "rocksdb")))]
    {
        std::fs::create_dir_all(data_dir)?;
        let db_path = data_dir.join("jazz.sqlite");
        SqliteStorage::open(&db_path)
            .map(|s| Box::new(s) as DynStorage)
            .map_err(|e| {
                JazzError::Connection(format!(
                    "failed to open sqlite storage '{}': {e:?}",
                    db_path.display()
                ))
            })
    }
    #[cfg(not(any(feature = "rocksdb", feature = "sqlite")))]
    {
        tracing::warn!("no persistent storage backend enabled, falling back to MemoryStorage");
        Ok(Box::new(MemoryStorage::new()))
    }
}

#[cfg(feature = "rocksdb")]
async fn open_rocksdb_storage(data_dir: &std::path::Path) -> Result<RocksDBStorage> {
    const MAX_ATTEMPTS: usize = 100;
    const RETRY_DELAY_MS: u64 = 25;

    std::fs::create_dir_all(data_dir)?;

    let db_path = data_dir.join("jazz.rocksdb");
    let mut opened = None;
    let mut last_err = None;

    for attempt in 0..MAX_ATTEMPTS {
        match RocksDBStorage::open(&db_path, 64 * 1024 * 1024) {
            Ok(storage) => {
                opened = Some(storage);
                break;
            }
            Err(err) => {
                let is_lock_error = matches!(
                    &err,
                    StorageError::IoError(msg)
                        if msg.contains("lock") || msg.contains("Lock") || msg.contains("busy")
                );
                if !is_lock_error || attempt + 1 == MAX_ATTEMPTS {
                    last_err = Some(err);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
            }
        }
    }

    opened.ok_or_else(|| {
        JazzError::Connection(format!(
            "failed to open rocksdb storage '{}': {:?}",
            db_path.display(),
            last_err
        ))
    })
}
