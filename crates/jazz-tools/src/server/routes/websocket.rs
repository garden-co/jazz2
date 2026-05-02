//! WebSocket handler — handshake authentication, connection lifecycle, and cleanup.

//! HTTP routes for the Jazz server.

use std::sync::Arc;

use axum::{
    extract::State,
    extract::ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade, close_code},
    http::HeaderMap,
    response::Response,
};

use crate::middleware::auth::{extract_session, validate_admin_secret, validate_backend_secret};
use crate::server::{ConnectionState, ServerState};
use crate::sync_manager::ClientId;

use super::utils::connection_schema_diagnostics_from_handshake;

pub(super) async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
) -> Response {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state, headers))
}

/// Outcome of authenticating a WS handshake.
#[derive(Debug)]
pub(super) enum WsClientSetup {
    Backend,
    Session(crate::query_manager::session::Session),
}

/// Authenticate a WebSocket `AuthHandshake`.
///
/// Priority is:
/// 1. `admin_secret` valid → `WsClientSetup::Backend`
/// 2. `backend_secret` present + no session header → `WsClientSetup::Backend`
/// 3. Otherwise → `extract_session` → `WsClientSetup::Session`
///
/// Returns `Err(message)` on auth failure; the caller should send a
/// `ServerEvent::Error` frame before closing.
pub(super) async fn authenticate_ws_handshake(
    handshake: &crate::transport_manager::AuthHandshake,
    request_headers: &HeaderMap,
    state: &Arc<ServerState>,
) -> Result<WsClientSetup, String> {
    use axum::http::HeaderValue;
    use base64::Engine as _;

    let auth = &handshake.auth;

    // `admin_secret` is an explicit request to run this WS transport as the
    // backend. Validate it first and short-circuit all user-scoped auth.
    if let Some(admin_secret) = auth.admin_secret.as_deref() {
        validate_admin_secret(Some(admin_secret), &state.auth_config)
            .map_err(|(_, msg)| msg.to_string())?;
        return Ok(WsClientSetup::Backend);
    }

    if request_uses_cookie_auth(handshake, request_headers, &state.auth_config) {
        validate_ws_cookie_origin(request_headers)?;
    }

    // Build a synthetic HeaderMap from the handshake auth fields, layered on
    // top of the original upgrade request so cookie-based auth remains visible.
    let mut headers = request_headers.clone();

    if let Some(jwt) = &auth.jwt_token {
        let value = HeaderValue::from_str(&format!("Bearer {jwt}"))
            .map_err(|e| format!("invalid jwt_token header value: {e}"))?;
        headers.insert(axum::http::header::AUTHORIZATION, value);
    }
    if let Some(secret) = &auth.backend_secret {
        let value = HeaderValue::from_str(secret)
            .map_err(|e| format!("invalid backend_secret header value: {e}"))?;
        headers.insert("X-Jazz-Backend-Secret", value);
    }
    if let Some(session_val) = &auth.backend_session {
        let json = serde_json::to_string(session_val)
            .map_err(|e| format!("failed to serialise backend_session: {e}"))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(json.as_bytes());
        let value = HeaderValue::from_str(&b64)
            .map_err(|e| format!("invalid backend_session header value: {e}"))?;
        headers.insert("X-Jazz-Session", value);
    }

    let has_jwt = headers.get(axum::http::header::AUTHORIZATION).is_some();
    let has_session_header = headers.get("X-Jazz-Session").is_some();
    let backend_secret = headers
        .get("X-Jazz-Backend-Secret")
        .and_then(|v| v.to_str().ok());

    // 2. Backend secret — only when no user-scoped JWT is present.  Clients
    //    that carry both a backend_secret and a jwt_token (e.g. test helpers
    //    that mirror the full credential set) must be treated as users so the
    //    connection carries a session for row-level policy evaluation.
    if backend_secret.is_some() && !has_jwt && !has_session_header {
        validate_backend_secret(backend_secret, &state.auth_config)
            .map_err(|(_, msg)| msg.to_string())?;
        return Ok(WsClientSetup::Backend);
    }

    // 3. JWT / session-impersonation path.
    let session = extract_session(
        &headers,
        state.app_id,
        &state.auth_config,
        state.jwt_verifier.as_deref(),
    )
    .await
    .map_err(|e| serde_json::to_string(&e).unwrap_or_else(|_| "authentication failed".into()))?;

    let session =
        session.ok_or_else(|| "Session required. Provide JWT or backend secret.".to_string())?;

    Ok(WsClientSetup::Session(session))
}

fn request_uses_cookie_auth(
    handshake: &crate::transport_manager::AuthHandshake,
    request_headers: &HeaderMap,
    auth_config: &crate::middleware::AuthConfig,
) -> bool {
    let Some(cookie_name) = auth_config.auth_cookie_name.as_deref() else {
        return false;
    };

    let has_explicit_auth = handshake.auth.jwt_token.is_some()
        || handshake.auth.backend_secret.is_some()
        || handshake.auth.backend_session.is_some()
        || handshake.auth.admin_secret.is_some()
        || request_headers
            .get(axum::http::header::AUTHORIZATION)
            .is_some()
        || request_headers.get("X-Jazz-Backend-Secret").is_some()
        || request_headers.get("X-Jazz-Session").is_some()
        || request_headers.get("X-Jazz-Admin-Secret").is_some();

    if has_explicit_auth {
        return false;
    }

    request_cookie_value(request_headers, cookie_name).is_some()
}

fn request_cookie_value<'a>(headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    let cookie_header = headers
        .get(axum::http::header::COOKIE)
        .and_then(|value| value.to_str().ok())?;

    cookie_header.split(';').find_map(|segment| {
        let trimmed = segment.trim();
        let (candidate_name, candidate_value) = trimmed.split_once('=')?;
        if candidate_name == name && !candidate_value.is_empty() {
            Some(candidate_value)
        } else {
            None
        }
    })
}

fn validate_ws_cookie_origin(headers: &HeaderMap) -> Result<(), String> {
    let host = headers
        .get("X-Forwarded-Host")
        .or_else(|| headers.get(axum::http::header::HOST))
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "Cookie auth requires Host header".to_string())?;

    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "Cookie auth requires Origin header".to_string())?;

    let origin_uri: axum::http::Uri = origin
        .parse()
        .map_err(|_| "Cookie auth requires a valid Origin header".to_string())?;
    let origin_authority = origin_uri
        .authority()
        .map(|authority| authority.as_str())
        .ok_or_else(|| "Cookie auth requires an Origin authority".to_string())?;

    let is_allowed_origin = origin_authority.eq_ignore_ascii_case(host)
        || is_loopback_cookie_origin(origin_uri.scheme_str(), origin_authority, host)?;

    if is_allowed_origin {
        Ok(())
    } else {
        Err("Cookie auth Origin must match Host".to_string())
    }
}

fn is_loopback_cookie_origin(
    origin_scheme: Option<&str>,
    origin_authority: &str,
    host: &str,
) -> Result<bool, String> {
    if !matches!(origin_scheme, Some("http") | Some("https")) {
        return Ok(false);
    }

    let origin_authority: axum::http::uri::Authority = origin_authority
        .parse()
        .map_err(|_| "Cookie auth requires a valid Origin authority".to_string())?;
    let host_authority: axum::http::uri::Authority = host
        .parse()
        .map_err(|_| "Cookie auth requires a valid Host header".to_string())?;

    Ok(
        is_loopback_dev_host(origin_authority.host())
            && is_loopback_dev_host(host_authority.host()),
    )
}

fn is_loopback_dev_host(host: &str) -> bool {
    let host = host.trim_matches(['[', ']']);
    host.eq_ignore_ascii_case("localhost")
        || host.to_ascii_lowercase().ends_with(".localhost")
        || host == "127.0.0.1"
        || host == "::1"
}

/// Send a `ServerEvent::Error` frame on the socket, best-effort.
async fn send_ws_error(socket: &mut WebSocket, message: &str) {
    send_ws_error_with_code(
        socket,
        crate::jazz_transport::ErrorCode::Unauthorized,
        message,
    )
    .await;
}

/// Send a `ServerEvent::Error` frame on the socket, best-effort.
async fn send_ws_error_with_code(
    socket: &mut WebSocket,
    code: crate::jazz_transport::ErrorCode,
    message: &str,
) {
    let event = crate::jazz_transport::ServerEvent::Error {
        message: message.to_string(),
        code,
    };
    if let Ok(bytes) = serde_json::to_vec(&event) {
        let _ = socket
            .send(Message::Binary(crate::transport_manager::frame_encode(
                &bytes,
            )))
            .await;
    }
}

async fn close_ws_with_protocol_reason(socket: &mut WebSocket, reason: &str) {
    let reason = reason.chars().take(123).collect::<String>();
    let _ = socket
        .send(Message::Close(Some(CloseFrame {
            code: close_code::PROTOCOL,
            reason: reason.into(),
        })))
        .await;
}

async fn handle_ws_connection(
    mut socket: WebSocket,
    state: Arc<ServerState>,
    request_headers: HeaderMap,
) {
    // 1. Read the first binary frame — expected to be AuthHandshake.
    let first = match socket.recv().await {
        Some(Ok(Message::Binary(b))) => b,
        _ => {
            let _ = socket.close().await;
            return;
        }
    };
    let payload = match crate::transport_manager::frame_decode(&first) {
        Some(p) => p.to_vec(),
        None => {
            let _ = socket.close().await;
            return;
        }
    };
    let handshake =
        match serde_json::from_slice::<crate::transport_manager::AuthHandshake>(&payload) {
            Ok(h) => h,
            Err(_) => {
                let _ = socket.close().await;
                return;
            }
        };

    // Older, pre-versioned clients deserialize as protocol version 0. Reject
    // them explicitly so developers see an actionable update prompt instead
    // of a dropped socket.
    if handshake.sync_protocol_version != crate::transport_manager::SYNC_PROTOCOL_VERSION {
        let message = format!(
            "Incompatible Jazz sync protocol: client sent {}, server requires {}. Please update Jazz.",
            handshake.sync_protocol_version,
            crate::transport_manager::SYNC_PROTOCOL_VERSION,
        );
        // Use BadRequest here so older clients that do not know newer error
        // codes can still deserialize and log the message.
        send_ws_error_with_code(
            &mut socket,
            crate::jazz_transport::ErrorCode::BadRequest,
            &message,
        )
        .await;
        close_ws_with_protocol_reason(&mut socket, &message).await;
        return;
    }

    // 2. Parse client_id.
    let client_id = match crate::sync_manager::ClientId::parse(&handshake.client_id) {
        Some(id) => id,
        None => {
            send_ws_error(&mut socket, "missing or invalid client_id").await;
            let _ = socket.close().await;
            return;
        }
    };

    // 3. Authenticate.
    let setup = match authenticate_ws_handshake(&handshake, &request_headers, &state).await {
        Ok(s) => s,
        Err(msg) => {
            send_ws_error(&mut socket, &msg).await;
            let _ = socket.close().await;
            return;
        }
    };
    let role = match &setup {
        WsClientSetup::Backend => "backend",
        WsClientSetup::Session(_) => "session",
    };

    // 4. Register with ConnectionEventHub (mirrors events_handler).
    let connection_id = state
        .next_connection_id
        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let (next_sync_seq, mut sync_rx) = state
        .connection_event_hub
        .register_connection(connection_id, client_id);
    {
        let mut connections = state.connections.write().await;
        connections.insert(connection_id, ConnectionState { client_id });
    }
    state.on_client_connected(client_id).await;

    // 5. Ensure the client state in the runtime.
    match setup {
        WsClientSetup::Backend => {
            let _ = state.runtime.ensure_client_as_backend(client_id);
        }
        WsClientSetup::Session(session) => {
            let _ = state.runtime.ensure_client_with_session(client_id, session);
        }
    }

    // 5b. Dispatch connection schema diagnostics if client sent a declared schema hash.
    match connection_schema_diagnostics_from_handshake(&state, &handshake) {
        Ok(Some(diagnostics)) => {
            state.connection_event_hub.dispatch_payload(
                client_id,
                crate::sync_manager::SyncPayload::ConnectionSchemaDiagnostics(diagnostics),
            );
        }
        Ok(None) => {}
        Err(err) => {
            tracing::error!(
                %client_id,
                declared_schema_hash = ?handshake.declared_schema_hash,
                "failed to compute connection schema diagnostics: {err}"
            );
        }
    }

    // 6. Send the Connected response.
    let resp = crate::transport_manager::ConnectedResponse {
        sync_protocol_version: crate::transport_manager::SYNC_PROTOCOL_VERSION,
        connection_id: connection_id.to_string(),
        client_id: client_id.to_string(),
        next_sync_seq: Some(next_sync_seq),
        catalogue_state_hash: state.runtime.catalogue_state_hash().ok(),
    };
    let resp_bytes = match serde_json::to_vec(&resp) {
        Ok(b) => b,
        Err(_) => {
            ws_cleanup(&state, connection_id, client_id).await;
            let _ = socket.close().await;
            return;
        }
    };
    if socket
        .send(Message::Binary(crate::transport_manager::frame_encode(
            &resp_bytes,
        )))
        .await
        .is_err()
    {
        ws_cleanup(&state, connection_id, client_id).await;
        return;
    }
    tracing::info!(connection_id, %client_id, role, "ws client connected");

    // 7. Bidirectional loop: inbound frames from client + outbound updates from hub.
    //    Also fires a periodic heartbeat so idle connections don't look half-open.
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(30));
    // Don't emit a heartbeat immediately after Connected — wait a full tick.
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await; // consume the immediate first tick
    loop {
        tokio::select! {
            msg = socket.recv() => match msg {
                Some(Ok(Message::Binary(data))) => {
                    let Some(inner) = crate::transport_manager::frame_decode(&data) else {
                        continue;
                    };
                    let inner = inner.to_vec();
                    if let Err(e) = state.process_ws_client_frame(client_id, &inner).await {
                        tracing::warn!(error = ?e, "ws client frame rejected");
                    }
                }
                Some(Ok(Message::Close(_))) | None => break,
                _ => continue,
            },
            update = sync_rx.recv() => {
                let Some(u) = update else { break };
                let mut updates = Vec::with_capacity(256);
                updates.push(crate::jazz_transport::SequencedSyncPayload {
                    seq: Some(u.seq),
                    payload: u.payload,
                });
                while updates.len() < 256 {
                    match sync_rx.try_recv() {
                        Ok(u) => updates.push(crate::jazz_transport::SequencedSyncPayload {
                            seq: Some(u.seq),
                            payload: u.payload,
                        }),
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }
                let event = if updates.len() == 1 {
                    let update = updates.pop().expect("single update is present");
                    crate::jazz_transport::ServerEvent::SyncUpdate {
                        seq: update.seq,
                        payload: Box::new(update.payload),
                    }
                } else {
                    crate::jazz_transport::ServerEvent::SyncUpdateBatch { updates }
                };
                let bytes = match serde_json::to_vec(&event) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let mut batch_settlements = 0usize;
                let mut settlement_batches =
                    std::collections::HashMap::<crate::row_histories::BatchId, usize>::new();
                let mut row_batches = 0usize;
                let mut query_settled = 0usize;
                let mut count_payload = |payload: &crate::sync_manager::SyncPayload| {
                    match payload {
                        crate::sync_manager::SyncPayload::BatchSettlement { settlement } => {
                            batch_settlements += 1;
                            *settlement_batches.entry(settlement.batch_id()).or_default() += 1;
                        }
                        crate::sync_manager::SyncPayload::RowBatchCreated { .. }
                        | crate::sync_manager::SyncPayload::RowBatchNeeded { .. } => {
                            row_batches += 1;
                        }
                        crate::sync_manager::SyncPayload::QuerySettled { .. } => {
                            query_settled += 1;
                        }
                        _ => {}
                    }
                };
                match &event {
                    crate::jazz_transport::ServerEvent::SyncUpdate { payload, .. } => {
                        count_payload(payload);
                    }
                    crate::jazz_transport::ServerEvent::SyncUpdateBatch { updates } => {
                        for update in updates {
                            count_payload(&update.payload);
                        }
                    }
                    _ => {}
                }
                let settlement_duplicate_messages: usize = settlement_batches
                    .values()
                    .map(|count| count.saturating_sub(1))
                    .sum();
                let mut top_settlement_duplicates: Vec<_> = settlement_batches
                    .iter()
                    .filter_map(|(batch_id, count)| {
                        let duplicates = count.saturating_sub(1);
                        (duplicates > 0).then_some((*batch_id, duplicates))
                    })
                    .collect();
                top_settlement_duplicates.sort_by(|(_, left), (_, right)| right.cmp(left));
                top_settlement_duplicates.truncate(8);
                tracing::warn!(
                    target: "jazz_timing",
                    update_count = match &event {
                        crate::jazz_transport::ServerEvent::SyncUpdate { .. } => 1,
                        crate::jazz_transport::ServerEvent::SyncUpdateBatch { updates } => updates.len(),
                        _ => 0,
                    },
                    bytes = bytes.len(),
                    row_batches,
                    query_settled,
                    batch_settlements,
                    settlement_unique_batches = settlement_batches.len(),
                    settlement_duplicate_messages,
                    top_settlement_duplicates = ?top_settlement_duplicates,
                    "[jazz timing] server websocket sending sync frame"
                );
                if socket
                    .send(Message::Binary(
                        crate::transport_manager::frame_encode(&bytes),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
            _ = heartbeat.tick() => {
                let event = crate::jazz_transport::ServerEvent::Heartbeat;
                let Ok(bytes) = serde_json::to_vec(&event) else { continue };
                if socket
                    .send(Message::Binary(
                        crate::transport_manager::frame_encode(&bytes),
                    ))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }

    ws_cleanup(&state, connection_id, client_id).await;
    let _ = socket.close().await;
}

/// Disconnect cleanup: mirrors the drop path in `events_handler`.
async fn ws_cleanup(state: &Arc<ServerState>, connection_id: u64, client_id: ClientId) {
    {
        let mut connections = state.connections.write().await;
        connections.remove(&connection_id);
    }
    state
        .connection_event_hub
        .unregister_connection(connection_id);
    state.on_connection_closed(client_id).await;
}
