//! Integration tests for the todo server.
//!
//! Tests the full HTTP API end-to-end.

#[path = "../../../../crates/jazz-tools/tests/support/permissions.rs"]
mod permissions_support;

use std::convert::Infallible;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::response::sse::{Event, Sse};
use base64::Engine;
use futures_util::StreamExt as _;
use futures_util::stream::Stream;
use http_body_util::BodyExt;
use jazz_tools::{
    AppContext, AppId, ClientStorage, ColumnType, DurabilityTier, JazzClient, SchemaBuilder,
    TableSchema,
};
use serde::{Deserialize, Serialize};
use tempfile::TempDir;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tower::ServiceExt;
use tower_http::cors::CorsLayer;
use uuid::Uuid;

/// Todo item.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Todo {
    pub id: Uuid,
    pub title: String,
    pub done: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTodoRequest {
    pub title: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateTodoRequest {
    pub title: Option<String>,
    pub done: Option<bool>,
    pub description: Option<String>,
}

/// Application state.
pub struct AppState {
    pub client: JazzClient,
    pub sse_tx: broadcast::Sender<Vec<Todo>>,
}

fn test_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("projects").column("name", ColumnType::Text))
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean)
                .nullable_column("description", ColumnType::Text)
                .nullable_fk_column("parent", "todos")
                .nullable_fk_column("project", "projects"),
        )
        .build()
}

async fn setup_test_app(temp_dir: &TempDir) -> Router {
    setup_test_app_with_path(temp_dir.path().to_path_buf()).await
}

async fn setup_test_app_with_path(data_dir: PathBuf) -> Router {
    let context = AppContext {
        app_id: AppId::from_name("test-todos"),
        client_id: None,
        schema: test_schema(),
        server_url: String::new(),
        data_dir,
        storage: ClientStorage::Persistent,
        jwt_token: None,
        backend_secret: None,
        admin_secret: None,
        sync_tracer: None,
    };

    let client = JazzClient::connect(context).await.unwrap();
    let (sse_tx, _) = broadcast::channel::<Vec<Todo>>(16);
    let state = Arc::new(AppState { client, sse_tx });

    // Build router matching main.rs
    Router::new()
        .route("/todos", axum::routing::get(list_todos))
        .route("/todos", axum::routing::post(create_todo))
        .route("/todos/live", axum::routing::get(todos_live))
        .route("/todos/:id", axum::routing::put(update_todo))
        .route("/todos/:id", axum::routing::delete(delete_todo))
        .route("/health", axum::routing::get(health))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

// Route handlers
use axum::Json;
use axum::extract::{Path, State};
use axum::response::IntoResponse;
use jazz_tools::{ObjectId, QueryBuilder, Value};

fn row_to_todo(object_id: ObjectId, values: &[Value]) -> Option<Todo> {
    if values.len() < 2 {
        return None;
    }
    let title = match &values[0] {
        Value::Text(s) => s.clone(),
        _ => return None,
    };
    let done = match &values[1] {
        Value::Boolean(b) => *b,
        _ => return None,
    };
    let description = values.get(2).and_then(|v| match v {
        Value::Text(s) if !s.is_empty() => Some(s.clone()),
        _ => None,
    });
    Some(Todo {
        id: *object_id.uuid(),
        title,
        done,
        description,
    })
}

fn todo_values(
    title: impl Into<String>,
    description: impl Into<String>,
) -> std::collections::HashMap<String, Value> {
    jazz_tools::row_input!("title" => title.into(), "done" => false, "description" => description.into())
}

/// Broadcast current todos to all SSE connections.
async fn broadcast_todos(state: &AppState) {
    let query = QueryBuilder::new("todos").build();
    if let Ok(rows) = state.client.query(query, None).await {
        let todos: Vec<Todo> = rows
            .iter()
            .filter_map(|(id, values)| row_to_todo(*id, values))
            .collect();
        let _ = state.sse_tx.send(todos);
    }
}

/// SSE endpoint streaming all todos on changes.
async fn todos_live(
    State(state): State<Arc<AppState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.sse_tx.subscribe();

    let query = QueryBuilder::new("todos").build();
    let initial_todos: Vec<Todo> = state
        .client
        .query(query, None)
        .await
        .map(|rows| {
            rows.iter()
                .filter_map(|(id, values)| row_to_todo(*id, values))
                .collect()
        })
        .unwrap_or_default();

    let initial_event = futures_util::stream::once(async move {
        Ok::<_, Infallible>(
            Event::default().data(serde_json::to_string(&initial_todos).unwrap_or_default()),
        )
    });

    let update_stream = BroadcastStream::new(rx).filter_map(|result| async move {
        match result {
            Ok(todos) => Some(Ok::<_, Infallible>(
                Event::default().data(serde_json::to_string(&todos).unwrap_or_default()),
            )),
            Err(_) => None,
        }
    });

    Sse::new(initial_event.chain(update_stream))
}

async fn list_todos(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let query = QueryBuilder::new("todos").build();
    match state.client.query(query, None).await {
        Ok(rows) => {
            let todos: Vec<Todo> = rows
                .iter()
                .filter_map(|(id, values)| row_to_todo(*id, values))
                .collect();
            Json(todos).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn create_todo(
    State(state): State<Arc<AppState>>,
    Json(request): Json<CreateTodoRequest>,
) -> impl IntoResponse {
    let description = request.description.clone().unwrap_or_default();
    let values = todo_values(request.title.clone(), description.clone());
    match state.client.create("todos", values).await {
        Ok((row_id, row_values)) => {
            let todo = row_to_todo(row_id, &row_values);
            broadcast_todos(&state).await;
            (StatusCode::CREATED, Json(todo)).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn update_todo(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
    Json(request): Json<UpdateTodoRequest>,
) -> impl IntoResponse {
    let object_id = ObjectId::from_uuid(id);
    let mut updates = Vec::new();
    if let Some(title) = request.title {
        updates.push(("title".to_string(), Value::Text(title)));
    }
    if let Some(done) = request.done {
        updates.push(("done".to_string(), Value::Boolean(done)));
    }
    if let Some(description) = request.description {
        updates.push(("description".to_string(), Value::Text(description)));
    }

    match state.client.update(object_id, updates).await {
        Ok(()) => {
            broadcast_todos(&state).await;
            let query = QueryBuilder::new("todos").build();
            match state.client.query(query, None).await {
                Ok(rows) => {
                    for (oid, values) in &rows {
                        if *oid.uuid() == id
                            && let Some(todo) = row_to_todo(*oid, values)
                        {
                            return Json(todo).into_response();
                        }
                    }
                    (
                        StatusCode::NOT_FOUND,
                        Json(serde_json::json!({ "error": "Not found after refetch" })),
                    )
                        .into_response()
                }
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({ "error": e.to_string() })),
                )
                    .into_response(),
            }
        }
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": format!("Update failed: {}", e) })),
        )
            .into_response(),
    }
}

async fn delete_todo(
    State(state): State<Arc<AppState>>,
    Path(id): Path<Uuid>,
) -> impl IntoResponse {
    let object_id = ObjectId::from_uuid(id);
    match state.client.delete(object_id).await {
        Ok(()) => {
            broadcast_todos(&state).await;
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        )
            .into_response(),
    }
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "healthy" }))
}

// Tests

#[tokio::test]
async fn test_health_check() {
    let temp_dir = TempDir::new().unwrap();
    let app = setup_test_app(&temp_dir).await;

    let response = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_crud_operations() {
    let temp_dir = TempDir::new().unwrap();
    let app = setup_test_app(&temp_dir).await;

    // 1. List todos (should be empty)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/todos")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let todos: Vec<Todo> = serde_json::from_slice(&body).unwrap();
    assert!(todos.is_empty(), "Should start empty");

    // 2. Create a todo
    let create_req = CreateTodoRequest {
        title: "Buy milk".to_string(),
        description: None,
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/todos")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&create_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let created_todo: Todo = serde_json::from_slice(&body).unwrap();
    assert_eq!(created_todo.title, "Buy milk");
    assert!(!created_todo.done);

    // 3. List todos (should have one)
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/todos")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let todos: Vec<Todo> = serde_json::from_slice(&body).unwrap();
    assert_eq!(todos.len(), 1);
    assert_eq!(todos[0].title, "Buy milk");

    // 4. Update the todo
    let update_req = UpdateTodoRequest {
        title: None,
        done: Some(true),
        description: None,
    };
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PUT")
                .uri(format!("/todos/{}", created_todo.id))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&update_req).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let updated_todo: Todo = serde_json::from_slice(&body).unwrap();
    assert!(updated_todo.done, "Should be done");

    // 5. Delete the todo
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("DELETE")
                .uri(format!("/todos/{}", created_todo.id))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);

    // 6. Verify it's gone
    let response = app
        .oneshot(
            Request::builder()
                .uri("/todos")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let todos: Vec<Todo> = serde_json::from_slice(&body).unwrap();
    assert!(todos.is_empty(), "Should be empty after delete");
}

#[tokio::test]
async fn test_local_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let data_path = temp_dir.path().to_path_buf();

    // Phase 1: Create a todo with first client
    let created_id = {
        let context = AppContext {
            app_id: AppId::from_name("test-persist"),
            client_id: None,
            schema: test_schema(),
            server_url: String::new(),
            data_dir: data_path.clone(),
            storage: ClientStorage::Persistent,
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
            sync_tracer: None,
        };
        let client = JazzClient::connect(context).await.unwrap();

        // Create a todo
        let values = todo_values("Persist me", "");
        let (row_id, _row_values) = client.create("todos", values).await.unwrap();

        // Verify it exists
        let query = QueryBuilder::new("todos").build();
        let results = client.query(query, None).await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1[0], Value::Text("Persist me".to_string()));

        // Shutdown client to release Fjall file handles
        client.shutdown().await.unwrap();
        row_id
    };

    // Phase 2: Create new client with same data dir, verify todo persisted
    {
        let context = AppContext {
            app_id: AppId::from_name("test-persist"),
            client_id: None,
            schema: test_schema(),
            server_url: String::new(),
            data_dir: data_path,
            storage: ClientStorage::Persistent,
            jwt_token: None,
            backend_secret: None,
            admin_secret: None,
            sync_tracer: None,
        };
        let client = JazzClient::connect(context).await.unwrap();

        // Query todos - should have the one we created
        let query = QueryBuilder::new("todos").build();
        let results = client.query(query, None).await.unwrap();

        assert_eq!(
            results.len(),
            1,
            "Todo should persist across client restarts"
        );
        assert_eq!(results[0].0, created_id, "Should be the same row");
        assert_eq!(results[0].1[0], Value::Text("Persist me".to_string()));

        client.shutdown().await.unwrap();
    }
}

// === Server resync test infrastructure ===

/// Test server handle - kills process on drop.
struct TestServer {
    process: Child,
    port: u16,
    #[allow(dead_code)]
    data_dir: TempDir,
    #[allow(dead_code)]
    jwks_server: JwksServer,
}

/// Test admin secret for catalogue sync.
const TEST_ADMIN_SECRET: &str = "test-admin-secret-12345";

/// Test HMAC secret for JWT validation via JWKS.
const TEST_JWT_SECRET: &str = "test-jwt-secret-for-integration";
const TEST_JWT_KID: &str = "test-jwks-kid";

#[derive(Clone)]
struct JwksState {
    kid: String,
    secret_b64: String,
}

struct JwksServer {
    task: tokio::task::JoinHandle<()>,
    url: String,
}

impl JwksServer {
    async fn start(kid: &str, secret: &str) -> Self {
        let state = JwksState {
            kid: kid.to_string(),
            secret_b64: base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret.as_bytes()),
        };

        let app = Router::new()
            .route("/jwks", axum::routing::get(jwks_handler))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind JWKS server");
        let addr = listener.local_addr().expect("JWKS local addr");
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve JWKS");
        });

        Self {
            task,
            url: format!("http://{addr}/jwks"),
        }
    }
}

impl Drop for JwksServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Generate a test JWT for the given user ID.
fn make_test_jwt(user_id: &str) -> String {
    use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(serde::Serialize)]
    struct Claims {
        sub: String,
        claims: serde_json::Value,
        exp: u64,
    }
    let exp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let claims = Claims {
        sub: user_id.to_string(),
        claims: serde_json::json!({}),
        exp,
    };
    let key = EncodingKey::from_secret(TEST_JWT_SECRET.as_bytes());
    let mut header = Header::new(Algorithm::HS256);
    header.kid = Some(TEST_JWT_KID.to_string());
    encode(&header, &claims, &key).unwrap()
}

impl TestServer {
    /// Start a test server on the given port.
    async fn start(port: u16) -> Self {
        let data_dir = TempDir::new().expect("create temp dir");
        let jwks_server = JwksServer::start(TEST_JWT_KID, TEST_JWT_SECRET).await;

        // Use a deterministic UUID app ID for testing
        let app_id = "00000000-0000-0000-0000-000000000001";

        // Find the jazz-tools binary. When running tests, look in target/debug or target/release.
        let jazz_binary = Self::find_jazz_binary();

        let process = Command::new(&jazz_binary)
            .args([
                "server",
                app_id,
                "--port",
                &port.to_string(),
                "--data-dir",
                data_dir.path().to_str().unwrap(),
                "--admin-secret",
                TEST_ADMIN_SECRET,
            ])
            .env("JAZZ_JWKS_URL", &jwks_server.url)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| panic!("Failed to spawn jazz server at {:?}: {}", jazz_binary, e));

        let server = Self {
            process,
            port,
            data_dir,
            jwks_server,
        };

        // Wait for server to be ready
        server.wait_ready().await;

        server
    }

    /// Find the jazz-tools binary in cargo's target directory.
    fn find_jazz_binary() -> PathBuf {
        // Get the path to the test binary, which gives us the target directory
        let exe = std::env::current_exe().expect("get current exe");
        let target_dir = exe
            .parent() // deps
            .and_then(|p| p.parent()) // debug or release
            .expect("find target dir");

        let jazz_path = target_dir.join("jazz-tools");
        if jazz_path.exists() {
            return jazz_path;
        }

        // Try building if not found (useful for first run)
        panic!(
            "jazz binary not found at {:?}. Run `cargo build -p jazz-tools --bin jazz-tools --features cli` first.",
            jazz_path
        );
    }

    fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    async fn wait_ready(&self) {
        let client = reqwest::Client::new();
        let url = format!("{}/health", self.base_url());

        for i in 0..100 {
            match client.get(&url).send().await {
                Ok(_) => return,
                Err(e) => {
                    if i == 99 {
                        eprintln!("Last error: {:?}", e);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("Server failed to become ready within 10 seconds");
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        let _ = self.process.kill();
        let _ = self.process.wait();
    }
}

/// Find an available port by binding to port 0.
fn get_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind to port 0");
    listener.local_addr().unwrap().port()
}

async fn jwks_handler(
    axum::extract::State(state): axum::extract::State<JwksState>,
) -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "keys": [
            {
                "kty": "oct",
                "kid": state.kid,
                "alg": "HS256",
                "k": state.secret_b64,
            }
        ]
    }))
}

/// Test that data syncs through server between clients via subscribe.
///
/// This test verifies the subscribe-driven sync model:
/// 1. Client syncs schema to server via catalogue
/// 2. Client syncs row data to server
/// 3. Server stores data
/// 4. New client subscribes and receives data from server via sync
///
/// NOTE: This tests "subscribe triggers sync, data eventually arrives" behavior.
/// We do NOT currently have upstream confirmation - "complete" means local graph
/// settled, not that we've received all server data. See specs/sync_manager.md
/// Future Work section.
///
/// NOTE: The global-server lazy schema activation is tested in jazz's
/// `e2e_two_clients_server_schema_sync`. This integration test verifies
/// end-to-end client-server sync with persistent client IDs.
#[tokio::test]
async fn test_server_resync() {
    // 1. Start jazz-tools server
    let port = get_free_port();
    let server = TestServer::start(port).await;

    let data_dir = TempDir::new().unwrap();
    let data_path = data_dir.path().to_path_buf();

    // IMPORTANT: Client app_id must match server's app_id for schema sync to work
    let test_app_id = AppId::from_string("00000000-0000-0000-0000-000000000001").unwrap();

    // 2. Create a normal JWT client with todos schema, then add data.
    // User-scoped WS sync is what publishes the initial schema and rows here.
    // Adding admin_secret would now force backend mode and reject catalogue writes.
    {
        let context = AppContext {
            app_id: test_app_id,
            client_id: None,
            schema: test_schema(),
            server_url: server.base_url(),
            data_dir: data_path.clone(),
            storage: ClientStorage::Persistent,
            jwt_token: Some(make_test_jwt("client1-user")),
            backend_secret: None,
            admin_secret: None,
            sync_tracer: None,
        };
        let client = JazzClient::connect(context).await.unwrap();
        let permissions_schema = test_schema();
        permissions_support::publish_allow_all_permissions(
            &server.base_url(),
            test_app_id,
            TEST_ADMIN_SECRET,
            &permissions_schema,
        )
        .await;

        // Create a todo
        let values = todo_values("Synced todo", "");
        let (_row_id, _row_values) = client.create("todos", values).await.unwrap();

        // Verify it exists locally
        let query = QueryBuilder::new("todos").build();
        let results = client.query(query, None).await.unwrap();
        assert_eq!(results.len(), 1, "Todo should exist locally");

        // Wait for sync to server
        tokio::time::sleep(Duration::from_millis(1000)).await;

        client.shutdown().await.unwrap();
    }

    // 3. Wipe local data completely
    std::fs::remove_dir_all(&data_path).unwrap();
    std::fs::create_dir_all(&data_path).unwrap();

    // 4. New client should resync from server via one-shot query with EdgeServer tier.
    // No admin_secret — this client's local catalogue sync will be rejected by the
    // server (CatalogueWriteDenied / SessionRequired), which is fine: the server
    // already has the schema from Client 1. The JWT provides a session so the
    // client can read data via QuerySubscription.
    {
        let context = AppContext {
            app_id: test_app_id,
            client_id: None,
            schema: test_schema(),
            server_url: server.base_url(),
            data_dir: data_path,
            storage: ClientStorage::Persistent,
            jwt_token: Some(make_test_jwt("client2-user")),
            backend_secret: None,
            admin_secret: None, // Intentionally no admin - server already has schema
            sync_tracer: None,
        };
        let client = JazzClient::connect(context).await.unwrap();

        // One-shot query with EdgeServer durability tier — waits for the server's
        // QuerySettled response before resolving, ensuring synced data arrives.
        let query = QueryBuilder::new("todos").build();
        let results = tokio::time::timeout(
            Duration::from_secs(10),
            client.query(query, Some(DurabilityTier::EdgeServer)),
        )
        .await
        .expect("Query with EdgeServer tier should resolve within 10s")
        .expect("Query should succeed");

        assert_eq!(results.len(), 1, "Todo should resync from server");

        client.shutdown().await.unwrap();
    }

    drop(server);
}

/// Test that the /todos/live SSE endpoint streams all todos and updates on changes.
#[tokio::test]
async fn test_todos_live_sse() {
    use futures_util::StreamExt;
    use reqwest_eventsource::{Event, EventSource};

    let temp_dir = TempDir::new().unwrap();
    let app = setup_test_app(&temp_dir).await;

    // Start a real HTTP server for SSE (tower oneshot doesn't support streaming)
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{}", addr);

    let server_handle = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Connect to SSE endpoint
    let mut es = EventSource::get(format!("{}/todos/live", base_url));

    // Helper to get next event data
    async fn next_todos(es: &mut EventSource) -> Vec<Todo> {
        loop {
            match es.next().await {
                Some(Ok(Event::Message(msg))) => {
                    return serde_json::from_str(&msg.data).expect("parse todos");
                }
                Some(Ok(Event::Open)) => continue,
                Some(Err(e)) => panic!("SSE error: {:?}", e),
                None => panic!("SSE stream ended unexpectedly"),
            }
        }
    }

    // 1. Initial event should be empty list
    let initial = next_todos(&mut es).await;
    assert!(initial.is_empty(), "Should start empty");

    // 2. Create a todo - should see it in next event
    let client = reqwest::Client::new();
    let create_resp = client
        .post(format!("{}/todos", base_url))
        .json(&CreateTodoRequest {
            title: "SSE Test".to_string(),
            description: None,
        })
        .send()
        .await
        .unwrap();
    assert_eq!(create_resp.status(), 201);
    let created: Todo = create_resp.json().await.unwrap();

    let after_create = next_todos(&mut es).await;
    assert_eq!(after_create.len(), 1);
    assert_eq!(after_create[0].id, created.id);
    assert_eq!(after_create[0].title, "SSE Test");

    // 3. Update the todo - should see updated state
    client
        .put(format!("{}/todos/{}", base_url, created.id))
        .json(&UpdateTodoRequest {
            title: None,
            done: Some(true),
            description: None,
        })
        .send()
        .await
        .unwrap();

    let after_update = next_todos(&mut es).await;
    assert_eq!(after_update.len(), 1);
    assert!(after_update[0].done, "Should be done");

    // 4. Delete the todo - should see empty list again
    client
        .delete(format!("{}/todos/{}", base_url, created.id))
        .send()
        .await
        .unwrap();

    let after_delete = next_todos(&mut es).await;
    assert!(after_delete.is_empty(), "Should be empty after delete");

    // Clean up
    es.close();
    server_handle.abort();
}
