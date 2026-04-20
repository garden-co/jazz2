//! WorkerFrame — single protocol enum for browser main ↔ worker bridge.
//!
//! Replaces the TypeScript `worker-protocol.ts`. Encoded as bincode.
//!
//! Every variant maps 1:1 to a message type in the old TS protocol.

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum LifecycleEvent {
    VisibilityHidden,
    VisibilityVisible,
    Pagehide,
    Freeze,
    Resume,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum AuthFailureReason {
    Unauthorized,
    Expired,
    Invalid,
    Unknown,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct InitPayload {
    pub schema_json: String,
    pub app_id: String,
    pub env: String,
    pub user_branch: String,
    pub db_name: String,
    pub client_id: String,
    pub server_url: Option<String>,
    pub server_path_prefix: Option<String>,
    pub jwt_token: Option<String>,
    pub admin_secret: Option<String>,
    pub log_level: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct DebugLensEdgeState {
    pub source_hash: String,
    pub target_hash: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "camelCase")]
pub struct DebugSchemaState {
    pub current_schema_hash: String,
    pub live_schema_hashes: Vec<String>,
    pub known_schema_hashes: Vec<String>,
    pub pending_schema_hashes: Vec<String>,
    pub lens_edges: Vec<DebugLensEdgeState>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum WorkerFrame {
    // main → worker
    Init(InitPayload),
    Sync {
        bytes: Vec<u8>,
    },
    PeerSync {
        peer_id: String,
        term: u32,
        bytes: Vec<u8>,
    },
    PeerOpen {
        peer_id: String,
    },
    PeerClose {
        peer_id: String,
    },
    LifecycleHint {
        event: LifecycleEvent,
        sent_at_ms: u64,
    },
    UpdateAuth {
        jwt: Option<String>,
    },
    DisconnectUpstream,
    ReconnectUpstream,
    Shutdown,
    SimulateCrash,
    DebugSchemaState,
    DebugSeedLiveSchema {
        schema_json: String,
    },

    // worker → main
    Ready,
    InitOk {
        client_id: String,
    },
    UpstreamConnected,
    UpstreamDisconnected,
    AuthFailed {
        reason: AuthFailureReason,
    },
    ShutdownOk,
    DebugSchemaStateOk(DebugSchemaState),
    DebugSeedLiveSchemaOk,
    Error {
        msg: String,
    },
}

pub fn encode(frame: &WorkerFrame) -> Vec<u8> {
    bincode::serialize(frame).expect("WorkerFrame serialization infallible")
}

pub fn decode(bytes: &[u8]) -> Result<WorkerFrame, bincode::Error> {
    bincode::deserialize(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(frame: WorkerFrame) {
        let bytes = encode(&frame);
        let decoded = decode(&bytes).unwrap();
        assert_eq!(format!("{frame:?}"), format!("{decoded:?}"));
    }

    #[test]
    fn roundtrip_all_variants() {
        roundtrip(WorkerFrame::Init(InitPayload {
            schema_json: "{}".into(),
            app_id: "app".into(),
            env: "e".into(),
            user_branch: "u".into(),
            db_name: "db".into(),
            client_id: "c".into(),
            server_url: Some("https://s".into()),
            server_path_prefix: Some("/p".into()),
            jwt_token: Some("j".into()),
            admin_secret: None,
            log_level: Some("warn".into()),
        }));
        roundtrip(WorkerFrame::Sync {
            bytes: vec![1, 2, 3],
        });
        roundtrip(WorkerFrame::PeerSync {
            peer_id: "p".into(),
            term: 7,
            bytes: vec![9],
        });
        roundtrip(WorkerFrame::PeerOpen {
            peer_id: "p".into(),
        });
        roundtrip(WorkerFrame::PeerClose {
            peer_id: "p".into(),
        });
        roundtrip(WorkerFrame::LifecycleHint {
            event: LifecycleEvent::Resume,
            sent_at_ms: 123,
        });
        roundtrip(WorkerFrame::UpdateAuth {
            jwt: Some("x".into()),
        });
        roundtrip(WorkerFrame::UpdateAuth { jwt: None });
        roundtrip(WorkerFrame::DisconnectUpstream);
        roundtrip(WorkerFrame::ReconnectUpstream);
        roundtrip(WorkerFrame::Shutdown);
        roundtrip(WorkerFrame::SimulateCrash);
        roundtrip(WorkerFrame::DebugSchemaState);
        roundtrip(WorkerFrame::DebugSeedLiveSchema {
            schema_json: "{}".into(),
        });
        roundtrip(WorkerFrame::Ready);
        roundtrip(WorkerFrame::InitOk {
            client_id: "c".into(),
        });
        roundtrip(WorkerFrame::UpstreamConnected);
        roundtrip(WorkerFrame::UpstreamDisconnected);
        roundtrip(WorkerFrame::AuthFailed {
            reason: AuthFailureReason::Expired,
        });
        roundtrip(WorkerFrame::ShutdownOk);
        roundtrip(WorkerFrame::DebugSchemaStateOk(DebugSchemaState::default()));
        roundtrip(WorkerFrame::DebugSeedLiveSchemaOk);
        roundtrip(WorkerFrame::Error { msg: "bad".into() });
    }
}
