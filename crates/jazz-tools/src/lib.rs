pub mod batch_fate;
pub mod binding_support;
pub mod catalogue;
pub mod commit;
pub mod digest;
pub mod identity;
pub mod metadata;
#[cfg(any(feature = "cli", feature = "server"))]
pub mod middleware;
mod monotonic_clock;
pub mod object;
pub mod query_manager;
#[cfg(any(feature = "cli", feature = "server"))]
pub mod routes;
pub mod row_format;
pub mod row_histories;
pub mod runtime_core;
pub mod schema_manager;
#[cfg(any(feature = "cli", feature = "server"))]
pub mod server;
pub mod storage;
pub mod sync_manager;
pub mod sync_tracer;
#[cfg(test)]
mod test_row_history;
pub mod wire_types;
pub mod worker_frame;

#[cfg(feature = "runtime-tokio")]
pub mod runtime_tokio;
#[cfg(feature = "runtime-tokio")]
pub use runtime_tokio as jazz_tokio;

pub mod transport_protocol;
pub use transport_protocol as jazz_transport;
pub mod peer_stream_pump;
pub mod transport_manager;
#[cfg(feature = "transport-websocket")]
pub mod ws_stream;

#[cfg(feature = "client")]
mod client;

#[cfg(feature = "client")]
use std::path::PathBuf;

#[cfg(feature = "client")]
use thiserror::Error;

#[cfg(feature = "client")]
pub use client::{JazzClient, SessionClient};

#[cfg(feature = "client")]
pub use object::ObjectId;
#[cfg(feature = "client")]
pub use query_manager::query::{Query, QueryBuilder};
#[cfg(feature = "client")]
pub use query_manager::session::Session;
#[cfg(feature = "client")]
pub use query_manager::types::{
    ColumnType, OrderedRowDelta, Row, RowDelta, Schema, SchemaBuilder, TableName, TableSchema,
    Value,
};
#[cfg(feature = "client")]
pub use schema_manager::AppId;
#[cfg(feature = "client")]
pub use sync_manager::ClientId;
#[cfg(feature = "client")]
pub use sync_manager::DurabilityTier;
#[cfg(feature = "client")]
pub use sync_manager::ServerId;

/// Configuration for connecting to Jazz.
#[cfg(feature = "client")]
#[derive(Debug, Clone)]
pub struct AppContext {
    /// Application ID.
    pub app_id: AppId,
    /// Client ID (generated if not provided).
    pub client_id: Option<ClientId>,
    /// Schema for this client.
    pub schema: Schema,
    /// Server URL for sync (e.g., "http://localhost:1625").
    pub server_url: String,
    /// Local data directory for persistent storage.
    pub data_dir: PathBuf,
    /// Local storage backend.
    pub storage: ClientStorage,

    // Authentication fields
    /// JWT token for frontend authentication.
    /// Sent as `Authorization: Bearer <token>`.
    pub jwt_token: Option<String>,
    /// Backend secret for session impersonation.
    /// Enables `for_session()` to act as any user.
    pub backend_secret: Option<String>,
    /// Admin secret for privileged sync over WebSocket and `/admin/*` HTTP.
    /// On `/ws`, a valid admin secret authenticates this client as the backend.
    pub admin_secret: Option<String>,

    /// Optional sync message tracer for test observability.
    /// Set via `TestingClient::with_tracer()` — `None` in production.
    pub sync_tracer: Option<(crate::sync_tracer::SyncTracer, String)>,
}

/// Local storage backend for a client application.
#[cfg(feature = "client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientStorage {
    /// Persist client state to disk under `AppContext::data_dir`.
    #[default]
    Persistent,
    /// Keep all client state in memory for the lifetime of the process only.
    Memory,
}

/// Errors from Jazz client operations.
#[cfg(feature = "client")]
#[derive(Error, Debug)]
pub enum JazzError {
    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Query error: {0}")]
    Query(String),

    #[error("Write error: {0}")]
    Write(String),

    #[error("Sync error: {0}")]
    Sync(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("Schema error: {0}")]
    Schema(String),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel closed")]
    ChannelClosed,
}

/// Result type for Jazz operations.
#[cfg(feature = "client")]
pub type Result<T> = std::result::Result<T, JazzError>;

/// Handle to a subscription.
#[cfg(feature = "client")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionHandle(pub u64);

/// Stream of row deltas from a subscription.
#[cfg(feature = "client")]
pub struct SubscriptionStream {
    receiver: tokio::sync::mpsc::UnboundedReceiver<OrderedRowDelta>,
}

#[cfg(feature = "client")]
impl SubscriptionStream {
    /// Create a new subscription stream.
    pub(crate) fn new(receiver: tokio::sync::mpsc::UnboundedReceiver<OrderedRowDelta>) -> Self {
        Self { receiver }
    }

    /// Get the next delta, waiting if necessary.
    pub async fn next(&mut self) -> Option<OrderedRowDelta> {
        self.receiver.recv().await
    }
}
