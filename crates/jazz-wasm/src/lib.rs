//! jazz-wasm - WebAssembly bindings for the Jazz database engine.
//!
//! This crate provides JavaScript bindings for the Jazz local-first database engine,
//! enabling TypeScript/JavaScript applications to use Jazz with custom storage backends.
//!
//! # Architecture
//!
//! - **WasmRuntime**: Main entry point that wraps SchemaManager and provides CRUD operations
//! - **WasmQueryBuilder**: Fluent query builder exposed to JavaScript
//! - **JsStorageDriver**: Interface for JavaScript storage implementations (IndexedDB, node:sqlite)
//! - **Type bridges**: Serialization between Rust and JavaScript types
//!
//! # Usage
//!
//! ```javascript
//! import { WasmRuntime, WasmQueryBuilder } from 'jazz-wasm';
//!
//! // Create a storage driver (e.g., IndexedDB)
//! const driver = {
//!   async process(requests) {
//!     // Handle storage requests
//!     return responses;
//!   }
//! };
//!
//! // Create runtime
//! const schema = { todos: { columns: [...] } };
//! const runtime = new WasmRuntime(driver, JSON.stringify(schema), 'my-app', 'dev', 'main');
//!
//! // Insert a row
//! const id = await runtime.insert('todos', {
//!   title: { type: 'Text', value: 'Buy milk' },
//!   done: { type: 'Boolean', value: false },
//! });
//!
//! // Query with builder
//! const query = new WasmQueryBuilder('todos').branch('main').build();
//! const results = await runtime.query(query);
//!
//! // Subscribe to changes
//! const subId = await runtime.subscribe(query, (delta) => {
//!   console.log('Changes:', delta);
//! });
//!
//! // Tick must be called periodically
//! setInterval(() => runtime.tick(), 100);
//! ```

#![allow(clippy::new_without_default)]

pub mod driver_bridge;
pub mod post_message_stream;
pub mod query;
pub mod runtime;
pub mod types;
pub mod worker_client;
pub mod worker_host;
#[cfg(target_arch = "wasm32")]
pub mod ws_stream;

// Re-export main types for JavaScript
pub use driver_bridge::JsStorageDriver;
pub use query::WasmQueryBuilder;
pub use runtime::WasmRuntime;
#[cfg(target_arch = "wasm32")]
pub use worker_client::WorkerClient;

use wasm_bindgen::prelude::*;

/// Initialize the WASM module.
///
/// Sets up panic hook for better error messages in the browser console.
#[wasm_bindgen(start)]
pub fn init() {
    #[cfg(feature = "console_error_panic_hook")]
    console_error_panic_hook::set_once();
}

/// Parse a schema from JSON string.
///
/// Returns the schema as a JsValue for inspection.
#[wasm_bindgen(js_name = parseSchema)]
pub fn parse_schema(json: &str) -> Result<JsValue, JsError> {
    let schema: types::Schema =
        serde_json::from_str(json).map_err(|e| JsError::new(&format!("Parse error: {}", e)))?;
    Ok(serde_wasm_bindgen::to_value(&schema)?)
}

/// Generate a new UUID v7 (time-ordered).
///
/// Useful for generating row IDs on the client side.
#[wasm_bindgen(js_name = generateId)]
pub fn generate_id() -> String {
    uuid::Uuid::now_v7().to_string()
}

/// Get the current timestamp in microseconds since Unix epoch.
#[wasm_bindgen(js_name = currentTimestamp)]
pub fn current_timestamp() -> u64 {
    use web_time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}
