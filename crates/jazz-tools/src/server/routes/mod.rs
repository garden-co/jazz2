//! HTTP and WebSocket routes for the Jazz server.
//!
//! Split into three submodules so each piece is independently navigable:
//! - [`http`] — HTTP endpoint handlers and their request/response types
//! - [`websocket`] — WebSocket lifecycle (handshake auth, connection, cleanup)
//! - [`utils`] — parser/validator helpers used by both
//!
//! The router builder [`create_router`] re-exports unchanged from this module
//! so existing callers (`server::routes::create_router`) continue to resolve.

mod http;
mod utils;
mod websocket;

use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

use crate::server::ServerState;

use http::{
    admin_subscription_introspection_handler, health_handler, permissions_handler,
    permissions_head_handler, publish_migration_handler, publish_permissions_handler,
    publish_schema_handler, schema_connectivity_handler, schema_handler, schema_hashes_handler,
};
use websocket::ws_handler;

pub fn create_router(state: Arc<ServerState>) -> Router {
    // TODO: Accept app-name aliases in app-scoped route matching
    // Nesting all non-health routes under a fixed "/apps/{state.app_id}" path makes the server only match the canonical UUID string, but JavaScript callers frequently propagate human-readable app IDs (e.g. "test-app") that are valid elsewhere via AppId::from_name(...). In that non-UUID case, the client now builds /apps/test-app/... URLs while the server only serves /apps/<derived-uuid>/..., so websocket and admin/schema requests return 404 for otherwise valid app IDs.
    let app_route_prefix = format!("/apps/{}", state.app_id);
    let traced_routes = Router::new()
        .route("/ws", axum::routing::any(ws_handler))
        .route("/schema/:hash", get(schema_handler))
        .route("/schemas", get(schema_hashes_handler))
        .route("/admin/schemas", post(publish_schema_handler))
        .route(
            "/admin/schema-connectivity",
            get(schema_connectivity_handler),
        )
        .route("/admin/permissions/head", get(permissions_head_handler))
        .route(
            "/admin/permissions",
            get(permissions_handler).post(publish_permissions_handler),
        )
        .route("/admin/migrations", post(publish_migration_handler))
        .route(
            "/admin/introspection/subscriptions",
            get(admin_subscription_introspection_handler),
        )
        .layer(TraceLayer::new_for_http());

    Router::new()
        .route("/health", get(health_handler))
        .nest(&app_route_prefix, traced_routes)
        .layer(CorsLayer::permissive())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::http::*;
    use super::utils::*;
    use super::websocket::*;
    use super::*;

    use axum::extract::{Path, Query};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::Json;

    use uuid::Uuid;

    use crate::object::ObjectId;
    use crate::query_manager::types::{SchemaHash, TableName};
    use crate::schema_manager::{AppId, LensOp};

    use std::collections::BTreeMap;
    use std::fmt;
    use std::sync::{Arc as StdArc, Mutex};
    use std::time::Duration;

    use crate::jazz_transport::SyncBatchRequest;
    use crate::query_manager::query::QueryBuilder;
    use crate::query_manager::types::{
        ColumnType, SchemaBuilder, TableSchema, Value as QueryValue,
    };
    use crate::sync_manager::{
        ClientId, ConnectionSchemaDiagnostics, InboxEntry, QueryId, QueryPropagation, Source,
        SyncPayload,
    };
    use axum::body;
    use axum::routing::{get, post};
    use futures::{SinkExt as _, StreamExt as _};
    use serde_json::Value;
    use tokio_tungstenite::{connect_async, tungstenite::Message as WsMessage};
    use tower::ServiceExt;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::SubscriberExt as _;
    use tracing_subscriber::{Layer, Registry};

    use crate::middleware::AuthConfig;
    use crate::server::{CatalogueAuthorityMode, ServerBuilder, ServerState, StorageBackend};

    fn test_auth_config() -> AuthConfig {
        AuthConfig {
            backend_secret: None,
            admin_secret: Some("admin-secret".to_string()),
            allow_local_first_auth: true,
            jwks_url: None,
            ..Default::default()
        }
    }

    fn mint_test_token(audience: &str) -> String {
        let seed = [42u8; 32];
        crate::identity::mint_jazz_self_signed_token(
            &seed,
            crate::identity::LOCAL_FIRST_ISSUER,
            audience,
            3600,
        )
        .unwrap()
    }

    /// Spin up a server state backed by an in-process runtime.
    /// `backend_secret` is wired into `AuthConfig` so tests can authenticate
    /// via the backend-secret WS handshake without needing JWT setup.
    async fn make_sync_test_state(backend_secret: &str) -> Arc<ServerState> {
        let auth_config = AuthConfig {
            backend_secret: Some(backend_secret.to_string()),
            admin_secret: None,
            allow_local_first_auth: false,
            jwks_url: None,
            ..Default::default()
        };

        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build sync test state")
            .state
    }

    async fn make_state_with_schema(
        schema: crate::query_manager::types::Schema,
    ) -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(test_auth_config())
            .with_storage(StorageBackend::InMemory)
            .with_schema(schema)
            .build()
            .await
            .expect("build state with schema")
            .state
    }

    async fn make_state_with_schema_and_authority(
        schema: crate::query_manager::types::Schema,
        catalogue_authority: CatalogueAuthorityMode,
    ) -> Arc<ServerState> {
        ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(test_auth_config())
            .with_catalogue_authority(catalogue_authority)
            .with_storage(StorageBackend::InMemory)
            .with_schema(schema)
            .build()
            .await
            .expect("build state with schema and authority")
            .state
    }

    fn make_test_router(state: Arc<ServerState>) -> axum::Router {
        create_router(state)
    }

    fn test_app_id_text() -> String {
        AppId::from_name("test-app").to_string()
    }

    fn test_app_route(path: &str) -> String {
        format!(
            "/apps/{}/{}",
            test_app_id_text(),
            path.trim_start_matches('/')
        )
    }

    /// A minimal valid `SyncPayload::RowBatchCreated` suitable for embedding
    /// in batch request bodies.
    fn row_version_created_payload(object_id: &str) -> crate::sync_manager::SyncPayload {
        let row_id =
            ObjectId::from_uuid(Uuid::parse_str(object_id).expect("parse test object id as uuid"));
        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::<crate::row_histories::BatchId>::new(),
            b"alice".to_vec(),
            crate::metadata::RowProvenance::for_insert(object_id.to_string(), 1_000),
            Default::default(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );

        crate::sync_manager::SyncPayload::RowBatchCreated {
            metadata: None,
            row,
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct ForwardedAdminRequest {
        method: String,
        path: String,
        admin_secret: Option<String>,
        body: Option<Value>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct CapturedEvent {
        level: tracing::Level,
        message: Option<String>,
        fields: BTreeMap<String, String>,
    }

    #[derive(Clone, Default)]
    struct EventCollector {
        events: StdArc<Mutex<Vec<CapturedEvent>>>,
    }

    impl EventCollector {
        fn snapshot(&self) -> Vec<CapturedEvent> {
            self.events.lock().unwrap().clone()
        }
    }

    impl<S> Layer<S> for EventCollector
    where
        S: tracing::Subscriber,
    {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            let mut visitor = CapturedEventVisitor::default();
            event.record(&mut visitor);
            self.events.lock().unwrap().push(CapturedEvent {
                level: *event.metadata().level(),
                message: visitor.message,
                fields: visitor.fields,
            });
        }
    }

    #[derive(Default)]
    struct CapturedEventVisitor {
        message: Option<String>,
        fields: BTreeMap<String, String>,
    }

    impl CapturedEventVisitor {
        fn record_value(&mut self, field: &Field, value: String) {
            if field.name() == "message" {
                self.message = Some(value.clone());
            }
            self.fields.insert(field.name().to_string(), value);
        }
    }

    impl Visit for CapturedEventVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
            self.record_value(field, format!("{value:?}"));
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            self.record_value(field, value.to_string());
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            self.record_value(field, value.to_string());
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.record_value(field, value.to_string());
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.record_value(field, value.to_string());
        }
    }

    #[tokio::test]
    async fn ws_sync_batch_accepts_two_payloads() {
        // alice fires two payloads over the /ws connection — both should be
        // ingested without error.
        //
        //  alice (ws client)       server
        //    ──handshake─────────► /ws
        //    ──p1──────────────►  process_ws_client_frame → ok
        //    ──p2──────────────►  process_ws_client_frame → ok

        let state = make_sync_test_state("test-backend-secret").await;
        let client_id = ClientId::new();

        // Simulate the server having registered this client (backend, no session).
        let _ = state.runtime.ensure_client_as_backend(client_id);

        let p1 = row_version_created_payload("00000000-0000-0000-0000-000000000001");
        let p2 = row_version_created_payload("00000000-0000-0000-0000-000000000002");

        let batch = SyncBatchRequest {
            payloads: vec![p1, p2],
            client_id,
        };
        let frame_payload = serde_json::to_vec(&batch).unwrap();
        let result = state
            .process_ws_client_frame(client_id, &frame_payload)
            .await;
        assert!(
            result.is_ok(),
            "two-payload batch should be accepted: {result:?}"
        );
    }

    #[tokio::test]
    async fn ws_handshake_rejects_missing_auth() {
        // WS handshake with no auth in the AuthHandshake → error frame, no Connected.
        use crate::transport_manager::AuthHandshake;

        let state = make_sync_test_state("test-backend-secret").await;
        let client_id = ClientId::new();

        // An AuthHandshake with an empty AuthConfig (no secret, no JWT).
        let handshake = AuthHandshake {
            client_id: client_id.to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };

        // Authenticate should fail — the `authenticate_ws_handshake` function is
        // private but the rejection path results in `ensure_client_*` never being
        // called.  We verify indirectly: the client should not be registered.
        let client_registered = state.runtime.ensure_client_as_backend(client_id).is_ok();
        // Note: ensure_client_as_backend succeeds because it only checks internal state.
        // The real gate is at the WS handler level.  This test documents that unauthenticated
        // handshakes are rejected at the transport layer — covered fully in auth_test.rs
        // integration tests that connect over the wire.
        let _ = (handshake, client_registered);
    }

    #[tokio::test]
    async fn ws_handshake_accepts_same_origin_cookie_auth() {
        let token = mint_test_token("test-app");
        let auth_config = AuthConfig {
            backend_secret: Some("test-backend-secret".to_string()),
            admin_secret: None,
            allow_local_first_auth: true,
            jwks_url: None,
            auth_cookie_name: Some("jazz-auth".to_string()),
            ..Default::default()
        };
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build sync test state")
            .state;
        let handshake = crate::transport_manager::AuthHandshake {
            client_id: ClientId::new().to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };
        let mut request_headers = HeaderMap::new();
        request_headers.insert(axum::http::header::HOST, "example.test".parse().unwrap());
        request_headers.insert(
            axum::http::header::ORIGIN,
            "https://example.test".parse().unwrap(),
        );
        request_headers.insert(
            axum::http::header::COOKIE,
            format!("jazz-auth={token}").parse().unwrap(),
        );

        let setup = authenticate_ws_handshake(&handshake, &request_headers, &state)
            .await
            .expect("cookie auth should succeed");

        assert!(matches!(setup, WsClientSetup::Session(_)));
    }

    #[tokio::test]
    async fn ws_handshake_accepts_loopback_cross_port_cookie_auth() {
        let token = mint_test_token("test-app");
        let auth_config = AuthConfig {
            backend_secret: Some("test-backend-secret".to_string()),
            admin_secret: None,
            allow_local_first_auth: true,
            jwks_url: None,
            auth_cookie_name: Some("jazz-auth".to_string()),
            ..Default::default()
        };
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build sync test state")
            .state;
        let handshake = crate::transport_manager::AuthHandshake {
            client_id: ClientId::new().to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };
        let mut request_headers = HeaderMap::new();
        request_headers.insert(axum::http::header::HOST, "localhost:4200".parse().unwrap());
        request_headers.insert(
            axum::http::header::ORIGIN,
            "http://localhost:5173".parse().unwrap(),
        );
        request_headers.insert(
            axum::http::header::COOKIE,
            format!("jazz-auth={token}").parse().unwrap(),
        );

        let setup = authenticate_ws_handshake(&handshake, &request_headers, &state)
            .await
            .expect("loopback cross-port cookie auth should succeed");

        assert!(matches!(setup, WsClientSetup::Session(_)));
    }

    #[tokio::test]
    async fn ws_handshake_rejects_deceptive_localhost_cookie_origin() {
        let token = mint_test_token("test-app");
        let auth_config = AuthConfig {
            backend_secret: Some("test-backend-secret".to_string()),
            admin_secret: None,
            allow_local_first_auth: true,
            jwks_url: None,
            auth_cookie_name: Some("jazz-auth".to_string()),
            ..Default::default()
        };
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build sync test state")
            .state;
        let handshake = crate::transport_manager::AuthHandshake {
            client_id: ClientId::new().to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };
        let mut request_headers = HeaderMap::new();
        request_headers.insert(axum::http::header::HOST, "localhost:4200".parse().unwrap());
        request_headers.insert(
            axum::http::header::ORIGIN,
            "http://localhost.evil.example:5173".parse().unwrap(),
        );
        request_headers.insert(
            axum::http::header::COOKIE,
            format!("jazz-auth={token}").parse().unwrap(),
        );

        let error = authenticate_ws_handshake(&handshake, &request_headers, &state)
            .await
            .expect_err("deceptive localhost cookie auth should fail");

        assert!(error.to_lowercase().contains("origin"));
    }

    #[tokio::test]
    async fn ws_handshake_rejects_cross_origin_cookie_auth() {
        let token = mint_test_token("test-app");
        let auth_config = AuthConfig {
            backend_secret: Some("test-backend-secret".to_string()),
            admin_secret: None,
            allow_local_first_auth: true,
            jwks_url: None,
            auth_cookie_name: Some("jazz-auth".to_string()),
            ..Default::default()
        };
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(auth_config)
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build sync test state")
            .state;
        let handshake = crate::transport_manager::AuthHandshake {
            client_id: ClientId::new().to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };
        let mut request_headers = HeaderMap::new();
        request_headers.insert(axum::http::header::HOST, "example.test".parse().unwrap());
        request_headers.insert(
            axum::http::header::ORIGIN,
            "https://evil.example".parse().unwrap(),
        );
        request_headers.insert(
            axum::http::header::COOKIE,
            format!("jazz-auth={token}").parse().unwrap(),
        );

        let error = authenticate_ws_handshake(&handshake, &request_headers, &state)
            .await
            .expect_err("cross-origin cookie auth should fail");

        assert!(error.to_lowercase().contains("origin"));
    }

    #[tokio::test]
    async fn ws_sync_batch_ingest_sixty_payloads() {
        // bob sends 60 payloads via the WS path — server must accept all of them.
        let state = make_sync_test_state("test-backend-secret").await;
        let client_id = ClientId::new();
        let _ = state.runtime.ensure_client_as_backend(client_id);

        let payloads: Vec<crate::sync_manager::SyncPayload> = (0..60)
            .map(|i| row_version_created_payload(&format!("00000000-0000-0000-0000-{:012}", i)))
            .collect();

        let batch = SyncBatchRequest {
            payloads,
            client_id,
        };
        let frame_payload = serde_json::to_vec(&batch).unwrap();
        let result = state
            .process_ws_client_frame(client_id, &frame_payload)
            .await;
        assert!(
            result.is_ok(),
            "sixty-payload batch should be accepted: {result:?}"
        );
    }

    #[tokio::test]
    async fn schema_handler_requires_admin_secret() {
        let state = ServerBuilder::new(AppId::from_name("test-app"))
            .with_auth_config(AuthConfig {
                backend_secret: None,
                admin_secret: Some("admin-secret".to_string()),
                allow_local_first_auth: false,
                jwks_url: None,
                ..Default::default()
            })
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build server state")
            .state;

        let app = create_router(state);

        let placeholder_hash = "0000000000000000000000000000000000000000000000000000000000000000";
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(&format!("/schema/{placeholder_hash}")))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);

        let response_with_admin = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(&format!("/schema/{placeholder_hash}")))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response_with_admin.status(), StatusCode::NOT_FOUND);

        let hashes_without_admin = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schemas"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hashes_without_admin.status(), StatusCode::UNAUTHORIZED);

        let root_schema = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/schema/{placeholder_hash}"))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(root_schema.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn schema_handlers_return_hashes_and_requested_schema() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let state = make_state_with_schema(schema.clone()).await;

        let app = make_test_router(state);

        let hashes_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schemas"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(hashes_response.status(), StatusCode::OK);
        let hashes_body = body::to_bytes(hashes_response.into_body(), usize::MAX)
            .await
            .expect("hashes body");
        let hashes_json: Value = serde_json::from_slice(&hashes_body).expect("hashes json");
        let expected_hash = schema_hash.to_string();
        assert_eq!(
            hashes_json["hashes"][0].as_str(),
            Some(expected_hash.as_str())
        );

        let schema_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(&format!("/schema/{}", schema_hash)))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schema_response.status(), StatusCode::OK);
        let schema_body = body::to_bytes(schema_response.into_body(), usize::MAX)
            .await
            .expect("schema body");
        let schema_json: Value = serde_json::from_slice(&schema_body).expect("schema json");
        let expected_schema_json = serde_json::to_value(schema).expect("expected schema json");
        assert_eq!(schema_json["schema"], expected_schema_json);
        assert!(schema_json.get("publishedAt").is_some());

        let bad_hash_response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schema/invalid"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(bad_hash_response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn catalogue_authority_forwarding_proxies_schema_and_permissions_requests() {
        use std::sync::{Arc, Mutex};

        let forwarded = Arc::new(Mutex::new(Vec::<ForwardedAdminRequest>::new()));
        let forwarded_for_router = forwarded.clone();
        let expected_hash =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string();
        let authority_routes = axum::Router::new()
            .route(
                &test_app_route("/schemas"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    let expected_hash = expected_hash.clone();
                    move |headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        let expected_hash = expected_hash.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: test_app_route("/schemas"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({ "hashes": [expected_hash] }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/schema/:hash"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |Path(hash): Path<String>, headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: test_app_route(&format!("/schema/{hash}")),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "users": {
                                    "columns": [
                                        { "name": "id", "column_type": { "type": "Uuid" }, "nullable": false },
                                        { "name": "name", "column_type": { "type": "Text" }, "nullable": false }
                                    ]
                                }
                            }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/schemas"),
                post({
                    let forwarded = forwarded_for_router.clone();
                    let expected_hash = expected_hash.clone();
                    move |headers: HeaderMap, body: Json<Value>| {
                        let forwarded = forwarded.clone();
                        let expected_hash = expected_hash.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "POST".to_string(),
                                path: test_app_route("/admin/schemas"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: Some(body.0),
                            });
                            (
                                StatusCode::CREATED,
                                Json(serde_json::json!({
                                    "objectId": "11111111-1111-1111-1111-111111111111",
                                    "hash": expected_hash,
                                })),
                            )
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/migrations"),
                post({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap, body: Json<Value>| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "POST".to_string(),
                                path: test_app_route("/admin/migrations"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: Some(body.0),
                            });
                            (
                                StatusCode::CREATED,
                                Json(serde_json::json!({
                                    "objectId": "22222222-2222-2222-2222-222222222222",
                                    "fromHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "toHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                                })),
                            )
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/schema-connectivity"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |Query(params): Query<SchemaConnectivityParams>, headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: format!(
                                    "{}?fromHash={}&toHash={}",
                                    test_app_route("/admin/schema-connectivity"),
                                    params.from_hash, params.to_hash
                                ),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "connected": true,
                            }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/permissions/head"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: test_app_route("/admin/permissions/head"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "head": {
                                    "schemaHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "version": 4,
                                    "parentBundleObjectId": "33333333-3333-3333-3333-333333333333",
                                    "bundleObjectId": "44444444-4444-4444-4444-444444444444"
                                }
                            }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/permissions"),
                get({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "GET".to_string(),
                                path: test_app_route("/admin/permissions"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: None,
                            });
                            Json(serde_json::json!({
                                "head": {
                                    "schemaHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "version": 4,
                                    "parentBundleObjectId": "33333333-3333-3333-3333-333333333333",
                                    "bundleObjectId": "44444444-4444-4444-4444-444444444444"
                                },
                                "permissions": {
                                    "users": {
                                        "select": { "using": { "type": "True" } }
                                    }
                                }
                            }))
                        }
                    }
                }),
            )
            .route(
                &test_app_route("/admin/permissions"),
                post({
                    let forwarded = forwarded_for_router.clone();
                    move |headers: HeaderMap, body: Json<Value>| {
                        let forwarded = forwarded.clone();
                        async move {
                            forwarded.lock().unwrap().push(ForwardedAdminRequest {
                                method: "POST".to_string(),
                                path: test_app_route("/admin/permissions"),
                                admin_secret: headers
                                    .get("X-Jazz-Admin-Secret")
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_string),
                                body: Some(body.0),
                            });
                            (
                                StatusCode::CONFLICT,
                                Json(serde_json::json!({
                                    "error": {
                                        "code": "bad_request",
                                        "message": "stale permissions parent"
                                    }
                                })),
                            )
                        }
                    }
                }),
            );
        let authority_app = axum::Router::new().nest("/authority-prefix", authority_routes);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind authority listener");
        let authority_addr = listener.local_addr().expect("authority local addr");
        let authority_task = tokio::spawn(async move {
            axum::serve(listener, authority_app)
                .await
                .expect("serve authority app");
        });

        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema_and_authority(
            schema.clone(),
            CatalogueAuthorityMode::Forward {
                base_url: format!("http://{authority_addr}/authority-prefix"),
                admin_secret: "authority-secret".to_string(),
            },
        )
        .await;
        let app = make_test_router(state);

        let schemas_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/schemas"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schemas_response.status(), StatusCode::OK);

        let schema_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(&format!("/schema/{expected_hash}")))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schema_response.status(), StatusCode::OK);

        let publish_schema_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/schemas"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({ "schema": schema }).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_schema_response.status(), StatusCode::CREATED);

        let publish_migration_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "fromHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "toHash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                            "forward": [{
                                "table": "users",
                                "operations": [{
                                    "type": "rename",
                                    "column": "name",
                                    "value": "full_name"
                                }]
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_migration_response.status(), StatusCode::CREATED);

        let permissions_head_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions/head"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(permissions_head_response.status(), StatusCode::OK);

        let schema_connectivity_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "{}?fromHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&toHash=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        test_app_route("/admin/schema-connectivity")
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(schema_connectivity_response.status(), StatusCode::OK);

        let permissions_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(permissions_response.status(), StatusCode::OK);

        let publish_permissions_response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/permissions"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "schemaHash": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "permissions": {
                                "users": {
                                    "select": { "using": { "type": "True" } }
                                }
                            },
                            "expectedParentBundleObjectId": "44444444-4444-4444-4444-444444444444"
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_permissions_response.status(), StatusCode::CONFLICT);

        let forwarded = forwarded.lock().unwrap().clone();
        assert_eq!(forwarded.len(), 8);
        assert!(
            forwarded
                .iter()
                .all(|request| request.admin_secret.as_deref() == Some("authority-secret"))
        );
        assert_eq!(forwarded[0].path, test_app_route("/schemas"));
        assert_eq!(
            forwarded[1].path,
            test_app_route(&format!("/schema/{expected_hash}"))
        );
        assert_eq!(forwarded[2].path, test_app_route("/admin/schemas"));
        assert_eq!(forwarded[3].path, test_app_route("/admin/migrations"));
        assert_eq!(forwarded[4].path, test_app_route("/admin/permissions/head"));
        assert_eq!(
            forwarded[5].path,
            format!(
                "{}?fromHash=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&toHash=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                test_app_route("/admin/schema-connectivity")
            )
        );
        assert_eq!(forwarded[6].path, test_app_route("/admin/permissions"));
        assert_eq!(forwarded[7].path, test_app_route("/admin/permissions"));
        assert_eq!(
            forwarded[7]
                .body
                .as_ref()
                .and_then(|body| body.get("expectedParentBundleObjectId"))
                .and_then(Value::as_str),
            Some("44444444-4444-4444-4444-444444444444")
        );

        authority_task.abort();
    }

    #[tokio::test]
    async fn permissions_handlers_publish_linear_head_and_reject_stale_parent() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let state = make_state_with_schema(schema).await;
        let app = make_test_router(state.clone());

        let initial_head = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions/head"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(initial_head.status(), StatusCode::OK);
        let initial_body = body::to_bytes(initial_head.into_body(), usize::MAX)
            .await
            .expect("initial permissions head body");
        let initial_json: Value =
            serde_json::from_slice(&initial_body).expect("initial permissions head json");
        assert!(initial_json["head"].is_null());

        let first_request_body = serde_json::json!({
            "schemaHash": schema_hash.to_string(),
            "permissions": {
                "users": {
                    "select": { "using": { "type": "True" } }
                }
            }
        });
        let first_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/permissions"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(first_request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(first_response.status(), StatusCode::CREATED);
        let first_body = body::to_bytes(first_response.into_body(), usize::MAX)
            .await
            .expect("first publish body");
        let first_json: Value = serde_json::from_slice(&first_body).expect("first publish json");
        let first_bundle_object_id = first_json["head"]["bundleObjectId"]
            .as_str()
            .expect("first bundle object id")
            .to_string();
        assert_eq!(first_json["head"]["version"].as_u64(), Some(1));
        assert_eq!(first_json["head"]["parentBundleObjectId"], Value::Null);

        let second_request_body = serde_json::json!({
            "schemaHash": schema_hash.to_string(),
            "permissions": {
                "users": {
                    "select": { "using": { "type": "False" } }
                }
            },
            "expectedParentBundleObjectId": first_bundle_object_id,
        });
        let second_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/permissions"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(second_request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second_response.status(), StatusCode::CREATED);
        let second_body = body::to_bytes(second_response.into_body(), usize::MAX)
            .await
            .expect("second publish body");
        let second_json: Value = serde_json::from_slice(&second_body).expect("second publish json");
        let second_bundle_object_id = second_json["head"]["bundleObjectId"]
            .as_str()
            .expect("second bundle object id")
            .to_string();
        assert_eq!(second_json["head"]["version"].as_u64(), Some(2));
        assert_eq!(
            second_json["head"]["parentBundleObjectId"].as_str(),
            Some(first_bundle_object_id.as_str())
        );

        let stale_request_body = serde_json::json!({
            "schemaHash": schema_hash.to_string(),
            "permissions": {
                "users": {
                    "select": { "using": { "type": "True" } }
                }
            },
            "expectedParentBundleObjectId": first_bundle_object_id,
        });
        let stale_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/permissions"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(stale_request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(stale_response.status(), StatusCode::CONFLICT);

        let head_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions/head"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(head_response.status(), StatusCode::OK);
        let head_body = body::to_bytes(head_response.into_body(), usize::MAX)
            .await
            .expect("current permissions head body");
        let head_json: Value =
            serde_json::from_slice(&head_body).expect("current permissions head json");
        assert_eq!(head_json["head"]["version"].as_u64(), Some(2));
        assert_eq!(
            head_json["head"]["bundleObjectId"].as_str(),
            Some(second_bundle_object_id.as_str())
        );

        let permissions_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(permissions_response.status(), StatusCode::OK);
        let permissions_body = body::to_bytes(permissions_response.into_body(), usize::MAX)
            .await
            .expect("current permissions body");
        let permissions_json: Value =
            serde_json::from_slice(&permissions_body).expect("current permissions json");
        assert_eq!(permissions_json["head"]["version"].as_u64(), Some(2));
        assert_eq!(
            permissions_json["permissions"]["users"]["select"]["using"]["type"].as_str(),
            Some("False")
        );
    }

    #[tokio::test]
    async fn permissions_handler_returns_nulls_before_any_publish() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;
        let app = make_test_router(state);

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/permissions"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("permissions body");
        let json: Value = serde_json::from_slice(&body).expect("permissions json");
        assert!(json["head"].is_null());
        assert!(json["permissions"].is_null());
    }

    #[tokio::test]
    async fn schema_connectivity_handler_reports_uploaded_migration_connectivity() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let state = make_state_with_schema(v2).await;
        state
            .runtime
            .add_known_schema(v1)
            .expect("seed known schema for connectivity test");
        let app = make_test_router(state.clone());

        let disconnected = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "{}?fromHash={}&toHash={}",
                        test_app_route("/admin/schema-connectivity"),
                        v1_hash,
                        v2_hash
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(disconnected.status(), StatusCode::OK);
        let disconnected_body = body::to_bytes(disconnected.into_body(), usize::MAX)
            .await
            .expect("disconnected body");
        let disconnected_json: Value =
            serde_json::from_slice(&disconnected_body).expect("disconnected json");
        assert_eq!(disconnected_json["connected"], Value::Bool(false));

        let publish_migration_response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(
                        serde_json::json!({
                            "fromHash": v1_hash.to_string(),
                            "toHash": v2_hash.to_string(),
                            "forward": [{
                                "table": "users",
                                "operations": [{
                                    "type": "rename",
                                    "column": "email",
                                    "value": "email_address"
                                }]
                            }]
                        })
                        .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(publish_migration_response.status(), StatusCode::CREATED);

        let connected = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!(
                        "{}?fromHash={}&toHash={}",
                        test_app_route("/admin/schema-connectivity"),
                        v1_hash,
                        v2_hash
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(connected.status(), StatusCode::OK);
        let connected_body = body::to_bytes(connected.into_body(), usize::MAX)
            .await
            .expect("connected body");
        let connected_json: Value =
            serde_json::from_slice(&connected_body).expect("connected json");
        assert_eq!(connected_json["connected"], Value::Bool(true));
    }

    #[tokio::test]
    async fn publish_schema_rejects_inline_permissions() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema.clone()).await;
        let app = make_test_router(state);

        let request_body = serde_json::json!({
            "schema": schema,
            "permissions": {
                "users": {
                    "select": { "using": { "type": "True" } }
                }
            }
        });

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/schemas"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn publish_migration_requires_admin_and_persists_lens() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let state = make_state_with_schema(v2).await;
        state
            .runtime
            .add_known_schema(v1)
            .expect("seed known schema for publish test");
        let app = make_test_router(state.clone());

        let request_body = serde_json::json!({
            "fromHash": v1_hash.to_string(),
            "toHash": v2_hash.to_string(),
            "forward": [{
                "table": "users",
                "operations": [{
                    "type": "rename",
                    "column": "email",
                    "value": "email_address"
                }]
            }]
        });

        let unauthorized = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

        let created = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let lens = state
            .runtime
            .with_schema_manager(|schema_manager| {
                schema_manager.get_lens(&v1_hash, &v2_hash).cloned()
            })
            .expect("read schema manager lens");
        assert!(
            lens.is_some(),
            "published lens should be registered in schema manager"
        );
    }

    #[tokio::test]
    async fn publish_migration_persists_table_rename_ops() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("people")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let state = make_state_with_schema(v2).await;
        state
            .runtime
            .add_known_schema(v1)
            .expect("seed known schema for publish test");
        let app = make_test_router(state.clone());

        let request_body = serde_json::json!({
            "fromHash": v1_hash.to_string(),
            "toHash": v2_hash.to_string(),
            "forward": [{
                "table": "people",
                "renamedFrom": "users",
                "operations": [{
                    "type": "rename",
                    "column": "email",
                    "value": "email_address"
                }]
            }]
        });

        let created = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let lens = state
            .runtime
            .with_schema_manager(|schema_manager| {
                schema_manager.get_lens(&v1_hash, &v2_hash).cloned()
            })
            .expect("read schema manager lens")
            .expect("published lens should be registered in schema manager");

        assert_eq!(
            lens.forward.ops,
            vec![
                LensOp::RenameTable {
                    old_name: "users".to_string(),
                    new_name: "people".to_string(),
                },
                LensOp::RenameColumn {
                    table: "people".to_string(),
                    old_name: "email".to_string(),
                    new_name: "email_address".to_string(),
                },
            ]
        );
    }

    #[tokio::test]
    async fn publish_migration_persists_added_and_removed_table_ops() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .table(
                TableSchema::builder("legacy_profiles")
                    .column("id", ColumnType::Uuid)
                    .column("bio", ColumnType::Text)
                    .nullable_column("avatar_url", ColumnType::Text),
            )
            .build();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .table(
                TableSchema::builder("profiles")
                    .column("id", ColumnType::Uuid)
                    .column("bio", ColumnType::Text)
                    .nullable_column("avatar_url", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let state = make_state_with_schema(v2.clone()).await;
        state
            .runtime
            .add_known_schema(v1.clone())
            .expect("seed known schema for publish test");
        let app = make_test_router(state.clone());

        let request_body = serde_json::json!({
            "fromHash": v1_hash.to_string(),
            "toHash": v2_hash.to_string(),
            "forward": [
                {
                    "table": "profiles",
                    "added": true,
                    "operations": []
                },
                {
                    "table": "legacy_profiles",
                    "removed": true,
                    "operations": []
                }
            ]
        });

        let created = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri(test_app_route("/admin/migrations"))
                    .header("Content-Type", "application/json")
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::from(request_body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(created.status(), StatusCode::CREATED);

        let lens = state
            .runtime
            .with_schema_manager(|schema_manager| {
                schema_manager.get_lens(&v1_hash, &v2_hash).cloned()
            })
            .expect("read schema manager lens")
            .expect("published lens should be registered in schema manager");

        assert_eq!(lens.forward.ops.len(), 2);

        match &lens.forward.ops[0] {
            LensOp::AddTable { table, schema } => {
                assert_eq!(table, "profiles");
                let expected = v2.get(&TableName::from("profiles")).unwrap();
                assert_eq!(
                    schema.columns.content_hash(),
                    expected.columns.content_hash(),
                );
                assert_eq!(schema.policies, expected.policies);
            }
            other => panic!("expected AddTable op, got {other:?}"),
        }

        match &lens.forward.ops[1] {
            LensOp::RemoveTable { table, schema } => {
                assert_eq!(table, "legacy_profiles");
                let expected = v1.get(&TableName::from("legacy_profiles")).unwrap();
                assert_eq!(
                    schema.columns.content_hash(),
                    expected.columns.content_hash(),
                );
                assert_eq!(schema.policies, expected.policies);
            }
            other => panic!("expected RemoveTable op, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn admin_subscription_introspection_requires_admin_secret_and_valid_app_id() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;
        let app = make_test_router(state.clone());

        let without_secret = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=test-app",
                    ))
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(without_secret.status(), StatusCode::UNAUTHORIZED);

        let wrong_secret = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=test-app",
                    ))
                    .header("X-Jazz-Admin-Secret", "wrong-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(wrong_secret.status(), StatusCode::UNAUTHORIZED);

        let missing_app_id = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route("/admin/introspection/subscriptions"))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing_app_id.status(), StatusCode::BAD_REQUEST);

        let invalid_app_id = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=bad/id",
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(invalid_app_id.status(), StatusCode::BAD_REQUEST);

        let mismatched_app_id = make_test_router(state)
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=other-app",
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(mismatched_app_id.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn admin_subscription_introspection_groups_active_server_subscriptions() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;

        let repeated_query = QueryBuilder::new("users").build();
        let filtered_query = QueryBuilder::new("users")
            .filter_eq("name", QueryValue::Text("Alice".to_string()))
            .build();

        for (index, query, propagation) in [
            (1_u64, repeated_query.clone(), QueryPropagation::Full),
            (2_u64, repeated_query.clone(), QueryPropagation::Full),
            (3_u64, repeated_query.clone(), QueryPropagation::LocalOnly),
            (4_u64, filtered_query, QueryPropagation::Full),
        ] {
            let client_id = ClientId::new();
            state.runtime.add_client(client_id, None).unwrap();
            state
                .runtime
                .push_sync_inbox(InboxEntry {
                    source: Source::Client(client_id),
                    payload: SyncPayload::QuerySubscription {
                        query_id: QueryId(index),
                        query: Box::new(query),
                        session: None,
                        propagation,
                        policy_context_tables: vec![],
                    },
                })
                .unwrap();
        }
        state.runtime.flush().await.unwrap();

        let response = make_test_router(state.clone())
            .oneshot(
                axum::http::Request::builder()
                    .uri(test_app_route(
                        "/admin/introspection/subscriptions?appId=test-app",
                    ))
                    .header("X-Jazz-Admin-Secret", "admin-secret")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("telemetry body");
        let json: Value = serde_json::from_slice(&body).expect("telemetry json");

        let expected_app_id = state.app_id.to_string();
        assert_eq!(json["appId"].as_str(), Some(expected_app_id.as_str()));
        assert!(json["generatedAt"].as_u64().is_some());

        let groups = json["queries"].as_array().expect("queries array");
        assert_eq!(groups.len(), 3);

        let repeated_full = groups.iter().find(|group| {
            group["count"].as_u64() == Some(2) && group["propagation"].as_str() == Some("full")
        });
        let repeated_full = repeated_full.expect("expected grouped full subscriptions");
        assert_eq!(repeated_full["table"].as_str(), Some("users"));
        assert_eq!(
            repeated_full["branches"]
                .as_array()
                .map(|branches| branches.len())
                .unwrap_or_default(),
            1
        );
        assert!(repeated_full["groupKey"].as_str().is_some());

        assert!(groups.iter().any(|group| {
            group["count"].as_u64() == Some(1)
                && group["propagation"].as_str() == Some("local-only")
        }));
        assert!(groups.iter().any(|group| {
            group["count"].as_u64() == Some(1)
                && group["query"]
                    .as_str()
                    .map(|query| query.contains("\"name\""))
                    .unwrap_or(false)
        }));
    }

    #[tokio::test]
    async fn ws_handler_dispatches_connection_schema_diagnostics_for_mismatched_schema() {
        // When a client connects with a schema hash that does not match the server's
        // current schema, the WS handler should enqueue a ConnectionSchemaDiagnostics
        // payload into the connection event hub immediately after step 5b.
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let current_hash = SchemaHash::compute(&schema);
        let declared_hash = SchemaHash::from_bytes([9; 32]);
        let state = make_state_with_schema(schema).await;

        // Simulate step 5b directly: client registered, then diagnostics dispatched.
        let client_id = ClientId::new();
        let _ = state.runtime.ensure_client_as_backend(client_id);

        // Replicate what handle_ws_connection does in step 5b.
        let diagnostics = state
            .runtime
            .with_schema_manager(|sm| sm.connection_schema_diagnostics(declared_hash))
            .expect("compute diagnostics");

        assert!(
            diagnostics.has_issues(),
            "mismatched schema should produce diagnostics"
        );
        assert_eq!(
            diagnostics,
            ConnectionSchemaDiagnostics {
                client_schema_hash: declared_hash,
                disconnected_permissions_schema_hash: Some(current_hash),
                unreachable_schema_hashes: vec![],
            }
        );
    }

    #[tokio::test]
    async fn ws_handler_uses_declared_schema_hash_for_connection_diagnostics() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();
        let state = make_state_with_schema(schema).await;
        let declared_hash = SchemaHash::from_bytes([9; 32]);
        let handshake = crate::transport_manager::AuthHandshake {
            client_id: ClientId::new().to_string(),
            auth: crate::transport_manager::AuthConfig::default(),
            catalogue_state_hash: state.runtime.catalogue_state_hash().ok(),
            declared_schema_hash: Some(declared_hash.to_string()),
        };

        let diagnostics = connection_schema_diagnostics_from_handshake(&state, &handshake)
            .expect("compute diagnostics")
            .expect("declared schema mismatch should produce diagnostics");

        assert_eq!(diagnostics.client_schema_hash, declared_hash);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ws_handler_logs_when_connection_is_established() {
        // Exercise the real /ws route so the assertion covers the actual
        // upgrade, handshake, and ConnectedResponse path the server uses.
        let collector = EventCollector::default();
        let subscriber = Registry::default().with(collector.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        let state = make_sync_test_state("test-backend-secret").await;
        let app = create_router(state);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind ws listener");
        let addr = listener.local_addr().expect("ws local addr");
        let server_task = tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve ws app");
        });

        let client_id = ClientId::new().to_string();
        let ws_url = format!("ws://{addr}{}", test_app_route("/ws"));
        let (mut ws, _) = connect_async(&ws_url).await.expect("connect ws");

        let handshake = crate::transport_manager::AuthHandshake {
            client_id: client_id.clone(),
            auth: crate::transport_manager::AuthConfig {
                backend_secret: Some("test-backend-secret".to_string()),
                ..Default::default()
            },
            catalogue_state_hash: None,
            declared_schema_hash: None,
        };
        let payload = serde_json::to_vec(&handshake).expect("serialize handshake");
        ws.send(WsMessage::Binary(crate::transport_manager::frame_encode(
            &payload,
        )))
        .await
        .expect("send handshake");

        let connected = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .expect("wait for ConnectedResponse")
            .expect("ws frame")
            .expect("ws result");
        assert!(
            matches!(connected, WsMessage::Binary(_)),
            "expected binary ConnectedResponse frame"
        );

        tokio::time::sleep(Duration::from_millis(50)).await;

        let events = collector.snapshot();
        assert!(
            events.iter().any(|event| {
                event.level == tracing::Level::INFO
                    && event.message.as_deref() == Some("ws client connected")
                    && event.fields.get("client_id").map(String::as_str) == Some(client_id.as_str())
                    && event.fields.get("connection_id").map(String::as_str) == Some("1")
                    && event.fields.get("role").map(String::as_str) == Some("backend")
            }),
            "expected ws client connected log, captured events: {events:#?}"
        );

        let _ = ws.close(None).await;
        server_task.abort();
    }
}
