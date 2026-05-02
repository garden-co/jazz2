//! Binary WebSocket transport protocol types for Jazz.
//!
//! This module defines the wire format for communication between Jazz clients
//! and servers over a single bidirectional WebSocket with length-prefixed
//! binary frames.
//!
//! # Protocol Overview
//!
//! - Clients connect to `/ws` and authenticate via an initial `AuthHandshake` frame
//! - Both directions use the same length-prefixed binary framing
//! - Server → client frames carry [`ServerEvent`] values
//! - Client → server frames carry [`SyncBatchRequest`] payloads
//!
//! # Wire Format
//!
//! Each frame: `[4 bytes: u32 big-endian length][N bytes: JSON]`

use serde::{Deserialize, Serialize};

use crate::sync_manager::{ClientId, QueryId, SyncPayload};

/// Unique identifier for a client's streaming connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId(pub u64);

// ============================================================================
// Client -> Server Requests
// ============================================================================

/// Request to push an ordered batch of sync payloads to the server's inbox.
///
/// All payloads share the same auth context (one auth check per POST).
/// The server applies them sequentially and returns one result per payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatchRequest {
    /// Ordered list of payloads from the client's outbox.
    pub payloads: Vec<SyncPayload>,
    /// Client ID for source tracking.
    pub client_id: ClientId,
}

/// Per-payload result within a `SyncBatchResponse`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncPayloadResult {
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response to a `SyncBatchRequest` — one result per input payload, in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatchResponse {
    pub results: Vec<SyncPayloadResult>,
}

// ============================================================================
// Server -> Client Events
// ============================================================================

/// Event sent over the binary streaming connection.
///
/// Note: Query results are NOT sent here directly. The server syncs the
/// underlying objects, and the client's local QueryManager handles query
/// notifications based on the synced data.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ServerEvent {
    /// Connection established, server sends connection ID and confirms client ID.
    Connected {
        connection_id: ConnectionId,
        /// The client ID the server is using for this connection.
        client_id: String,
        /// Next stream sequence expected from server for this connection.
        next_sync_seq: Option<u64>,
        /// Canonical digest of the server's catalogue state, when available.
        #[serde(skip_serializing_if = "Option::is_none")]
        catalogue_state_hash: Option<String>,
    },

    /// Subscription created successfully.
    Subscribed { query_id: QueryId },

    /// Sync update - object data changed.
    SyncUpdate {
        /// Per-connection stream sequence, if provided by the server.
        seq: Option<u64>,
        payload: Box<SyncPayload>,
    },

    /// Multiple ordered sync updates in one transport frame.
    ///
    /// Each item keeps its own stream sequence so clients can preserve the
    /// exact same ordering/watermark semantics as individual `SyncUpdate`
    /// frames while avoiding thousands of tiny websocket messages.
    SyncUpdateBatch { updates: Vec<SequencedSyncPayload> },

    /// Error response.
    Error { message: String, code: ErrorCode },

    /// Heartbeat to keep connection alive.
    Heartbeat,
}

impl ServerEvent {
    /// Get the variant name for debugging.
    pub fn variant_name(&self) -> &'static str {
        match self {
            ServerEvent::Connected { .. } => "Connected",
            ServerEvent::Subscribed { .. } => "Subscribed",
            ServerEvent::SyncUpdate { .. } => "SyncUpdate",
            ServerEvent::SyncUpdateBatch { .. } => "SyncUpdateBatch",
            ServerEvent::Error { .. } => "Error",
            ServerEvent::Heartbeat => "Heartbeat",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SequencedSyncPayload {
    pub seq: Option<u64>,
    pub payload: SyncPayload,
}

/// Error codes for server errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// Invalid request format.
    BadRequest,
    /// Client and server do not share a compatible sync protocol version.
    IncompatibleProtocol,
    /// Authentication required or failed.
    Unauthorized,
    /// Permission denied by policy.
    Forbidden,
    /// Resource not found.
    NotFound,
    /// Internal server error.
    Internal,
    /// Rate limit exceeded.
    RateLimited,
}

// ============================================================================
// HTTP Response Types
// ============================================================================

/// Generic success response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SuccessResponse {
    pub success: bool,
}

impl Default for SuccessResponse {
    fn default() -> Self {
        Self { success: true }
    }
}

/// Generic error response.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorResponse {
    pub error: String,
    pub code: ErrorCode,
}

impl ErrorResponse {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::BadRequest,
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::Unauthorized,
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::Forbidden,
        }
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::NotFound,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            error: message.into(),
            code: ErrorCode::Internal,
        }
    }
}

/// Auth failure reasons returned by runtime-facing HTTP endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnauthenticatedCode {
    Expired,
    Missing,
    Invalid,
    Disabled,
}

/// Structured unauthenticated response for the WebSocket transport.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnauthenticatedResponse {
    pub error: &'static str,
    pub code: UnauthenticatedCode,
    pub message: String,
}

impl UnauthenticatedResponse {
    pub fn expired(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Expired,
            message: message.into(),
        }
    }

    pub fn missing(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Missing,
            message: message.into(),
        }
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Invalid,
            message: message.into(),
        }
    }

    pub fn disabled(message: impl Into<String>) -> Self {
        Self {
            error: "unauthenticated",
            code: UnauthenticatedCode::Disabled,
            message: message.into(),
        }
    }
}

// ============================================================================
// Binary Frame Encoding/Decoding Helpers
// ============================================================================

impl ServerEvent {
    /// Encode as a length-prefixed binary frame.
    ///
    /// Format: `[4 bytes: u32 big-endian length][N bytes: JSON]`
    pub fn encode_frame(&self) -> Vec<u8> {
        let json = serde_json::to_vec(self).unwrap_or_default();
        let len = (json.len() as u32).to_be_bytes();
        let mut buf = Vec::with_capacity(4 + json.len());
        buf.extend_from_slice(&len);
        buf.extend_from_slice(&json);
        buf
    }

    /// Decode a single frame from a buffer.
    ///
    /// Returns `Some((event, bytes_consumed))` if a complete frame was available,
    /// or `None` if the buffer doesn't contain a complete frame yet.
    pub fn decode_frame(buf: &[u8]) -> Option<(Self, usize)> {
        if buf.len() < 4 {
            return None;
        }
        let len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;
        if buf.len() < 4 + len {
            return None;
        }
        let event: ServerEvent = serde_json::from_slice(&buf[4..4 + len]).ok()?;
        Some((event, 4 + len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_event_frame_roundtrip() {
        let event = ServerEvent::Connected {
            connection_id: ConnectionId(42),
            client_id: "test-client-id".to_string(),
            next_sync_seq: None,
            catalogue_state_hash: Some("digest-123".to_string()),
        };

        let frame = event.encode_frame();
        assert!(frame.len() > 4);

        let (decoded, consumed) = ServerEvent::decode_frame(&frame).unwrap();
        assert_eq!(consumed, frame.len());
        match decoded {
            ServerEvent::Connected {
                catalogue_state_hash,
                ..
            } => {
                assert_eq!(catalogue_state_hash.as_deref(), Some("digest-123"));
            }
            other => panic!("Expected Connected event, got {:?}", other.variant_name()),
        }
    }

    #[test]
    fn test_heartbeat_frame_roundtrip() {
        let event = ServerEvent::Heartbeat;
        let frame = event.encode_frame();

        let (decoded, consumed) = ServerEvent::decode_frame(&frame).unwrap();
        assert_eq!(consumed, frame.len());
        assert!(matches!(decoded, ServerEvent::Heartbeat));
    }

    #[test]
    fn test_decode_frame_incomplete() {
        // Too short for length prefix
        assert!(ServerEvent::decode_frame(&[0, 0]).is_none());

        // Length says 100 bytes but only 4 available
        let buf = [0, 0, 0, 100, 1, 2, 3, 4];
        assert!(ServerEvent::decode_frame(&buf).is_none());
    }

    #[test]
    fn test_error_response_constructors() {
        let err = ErrorResponse::bad_request("invalid query");
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert_eq!(err.error, "invalid query");

        let err = ErrorResponse::forbidden("not allowed");
        assert_eq!(err.code, ErrorCode::Forbidden);

        let err = UnauthenticatedResponse::expired("JWT expired");
        assert_eq!(err.error, "unauthenticated");
        assert_eq!(err.code, UnauthenticatedCode::Expired);
        assert_eq!(err.message, "JWT expired");
    }

    #[test]
    fn test_sync_batch_request_serialization() {
        use crate::metadata::RowProvenance;
        use crate::object::ObjectId;
        use crate::row_histories::{RowState, StoredRowBatch};
        use crate::sync_manager::ClientId;

        let row_id = ObjectId::new();
        let payload = SyncPayload::RowBatchCreated {
            metadata: None,
            row: StoredRowBatch::new(
                row_id,
                "main",
                Vec::new(),
                b"alice".to_vec(),
                RowProvenance::for_insert(row_id.to_string(), 1_000),
                Default::default(),
                RowState::VisibleDirect,
                None,
            ),
        };
        let request = SyncBatchRequest {
            payloads: vec![payload],
            client_id: ClientId::new(),
        };

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("payloads"));
        assert!(json.contains("RowBatchCreated"));
        assert!(json.contains("main"));

        let parsed: SyncBatchRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.payloads.len(), 1);
        assert!(matches!(
            parsed.payloads[0],
            SyncPayload::RowBatchCreated { .. }
        ));
    }

    #[test]
    fn test_sync_batch_response_serialization() {
        let response = SyncBatchResponse {
            results: vec![
                SyncPayloadResult {
                    ok: true,
                    error: None,
                },
                SyncPayloadResult {
                    ok: false,
                    error: Some("bad payload".into()),
                },
            ],
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("results"));
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("bad payload"));

        // ok:true entries must not include the error field
        assert!(!json.contains("\"error\":null"));
    }
}
