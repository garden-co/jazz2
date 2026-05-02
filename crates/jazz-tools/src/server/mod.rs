use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{RwLock, mpsc};
use tokio::time::Instant;

use crate::middleware::AuthConfig;
use crate::middleware::auth::JwtVerifier;
use crate::runtime_tokio::TokioRuntime;
use crate::schema_manager::AppId;
use crate::storage::Storage;
use crate::sync_manager::{ClientId, InboxEntry, Source, SyncPayload};

mod builder;
mod hosted;
pub mod routes;
#[cfg(feature = "test-utils")]
mod testing;

pub use builder::{BuiltServer, ServerBuilder, StorageBackend};
pub use hosted::HostedServer;
#[cfg(feature = "test-utils")]
pub use testing::{TestingJwksServer, TestingServer, TestingServerBuilder};

pub type DynStorage = Box<dyn Storage + Send>;

#[derive(Debug, Clone)]
pub struct SequencedSyncUpdate {
    pub seq: u64,
    pub payload: SyncPayload,
}

struct PreparedSyncDispatch {
    connection_id: u64,
    sender: mpsc::UnboundedSender<SequencedSyncUpdate>,
    update: SequencedSyncUpdate,
}

struct ConnectionStreamState {
    client_id: ClientId,
    next_sync_seq: u64,
    sender: mpsc::UnboundedSender<SequencedSyncUpdate>,
    sent_total: u64,
    sent_by_type: BTreeMap<&'static str, u64>,
    next_log_at: u64,
}

#[derive(Default)]
pub struct ConnectionEventHub {
    streams: Mutex<HashMap<u64, ConnectionStreamState>>,
}

impl ConnectionEventHub {
    pub fn register_connection(
        &self,
        connection_id: u64,
        client_id: ClientId,
    ) -> (u64, mpsc::UnboundedReceiver<SequencedSyncUpdate>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut streams = self.streams.lock().unwrap();
        streams.insert(
            connection_id,
            ConnectionStreamState {
                client_id,
                next_sync_seq: 1,
                sender,
                sent_total: 0,
                sent_by_type: BTreeMap::new(),
                next_log_at: 500,
            },
        );
        (1, receiver)
    }

    pub fn unregister_connection(&self, connection_id: u64) {
        self.streams.lock().unwrap().remove(&connection_id);
    }

    fn prepare_payload(
        &self,
        client_id: ClientId,
        payload: SyncPayload,
    ) -> Vec<PreparedSyncDispatch> {
        let mut prepared = Vec::new();
        let mut streams = self.streams.lock().unwrap();

        for (&connection_id, state) in streams.iter_mut() {
            if state.client_id != client_id {
                continue;
            }

            let seq = state.next_sync_seq;
            let through_seq = seq.saturating_sub(1);
            let payload = match &payload {
                SyncPayload::QuerySettled {
                    query_id,
                    tier,
                    scope,
                    ..
                } => SyncPayload::QuerySettled {
                    query_id: *query_id,
                    tier: *tier,
                    scope: scope.clone(),
                    through_seq,
                },
                _ => payload.clone(),
            };
            let kind = payload.variant_name();
            state.sent_total += 1;
            *state.sent_by_type.entry(kind).or_default() += 1;
            if matches!(payload, SyncPayload::QuerySettled { .. })
                || state.sent_total >= state.next_log_at
            {
                tracing::warn!(
                    target: "jazz_timing",
                    connection_id,
                    %client_id,
                    seq,
                    kind,
                    sent_total = state.sent_total,
                    sent_by_type = ?state.sent_by_type,
                    "[jazz timing] server stream fanout summary"
                );
                while state.sent_total >= state.next_log_at {
                    state.next_log_at += 500;
                }
            }

            prepared.push(PreparedSyncDispatch {
                connection_id,
                sender: state.sender.clone(),
                update: SequencedSyncUpdate { seq, payload },
            });
            state.next_sync_seq += 1;
        }

        prepared
    }

    fn dispatch_prepared(&self, prepared: Vec<PreparedSyncDispatch>) {
        let mut stale_connection_ids = Vec::new();

        for dispatch in prepared {
            if dispatch.sender.send(dispatch.update).is_err() {
                stale_connection_ids.push(dispatch.connection_id);
            }
        }

        if stale_connection_ids.is_empty() {
            return;
        }

        let mut streams = self.streams.lock().unwrap();
        for connection_id in stale_connection_ids {
            streams.remove(&connection_id);
        }
    }

    pub fn dispatch_payload(&self, client_id: ClientId, payload: SyncPayload) {
        let prepared = self.prepare_payload(client_id, payload);
        self.dispatch_prepared(prepared);
    }
}

/// Tracks when a client's last SSE connection closed, pending reap after TTL.
#[derive(Clone, Copy)]
pub struct DisconnectCandidate {
    /// When the last SSE connection closed.
    pub disconnected_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum CatalogueAuthorityMode {
    #[default]
    Local,
    Forward {
        base_url: String,
        admin_secret: String,
    },
}

impl CatalogueAuthorityMode {
    pub fn forward_target(&self) -> Option<(&str, &str)> {
        match self {
            Self::Local => None,
            Self::Forward {
                base_url,
                admin_secret,
            } => Some((base_url.as_str(), admin_secret.as_str())),
        }
    }
}

/// Server state shared across request handlers.
pub struct ServerState {
    pub runtime: TokioRuntime<DynStorage>,
    #[allow(dead_code)]
    pub app_id: AppId,
    pub connections: RwLock<HashMap<u64, ConnectionState>>,
    pub next_connection_id: std::sync::atomic::AtomicU64,
    /// Per-connection fanout for sequenced stream delivery.
    pub connection_event_hub: Arc<ConnectionEventHub>,
    /// Authentication configuration.
    pub auth_config: AuthConfig,
    /// Whether catalogue admin requests are handled locally or forwarded to an authority.
    pub catalogue_authority: CatalogueAuthorityMode,
    /// Shared HTTP client for forwarding admin requests to a remote authority.
    pub http_client: reqwest::Client,
    /// Configured verifier for external JWTs.
    pub jwt_verifier: Option<Arc<JwtVerifier>>,
    /// Clients that lost their SSE stream, waiting to be reaped after TTL.
    pub disconnect_candidates: RwLock<HashMap<ClientId, DisconnectCandidate>>,
    /// Client state TTL. Default: 5 minutes.
    /// Disconnected clients are reaped after this duration.
    pub client_ttl: RwLock<Duration>,
    /// Optional sync message tracer for test observability.
    pub sync_tracer: Option<crate::sync_tracer::SyncTracer>,
}

/// State for a single SSE connection.
pub struct ConnectionState {
    pub client_id: ClientId,
}

impl ServerState {
    /// Record that a connection closed. If this was the last SSE connection
    /// for the given client_id, add it to disconnect_candidates.
    ///
    /// The connections check and candidate insertion are done under the
    /// candidates write lock to prevent a TOCTOU race where a reconnect
    /// could slip in between the check and the insert.
    ///
    /// Lock ordering (no path nests these in conflicting order):
    ///   on_connection_closed:  candidates(write) → connections(read)
    ///   on_client_connected:   candidates(write)
    ///   events_handler:        connections(write) ; candidates(write)   (sequential)
    ///   run_sweep_once:        candidates(write) ; connections(read) ; core(Mutex)  (all sequential)
    pub async fn on_connection_closed(&self, client_id: ClientId) {
        let mut candidates = self.disconnect_candidates.write().await;
        let has_connections = self
            .connections
            .read()
            .await
            .values()
            .any(|c| c.client_id == client_id);
        if !has_connections {
            candidates.insert(
                client_id,
                DisconnectCandidate {
                    disconnected_at: Instant::now(),
                },
            );
        }
    }

    /// Record that a client reconnected. Remove from disconnect_candidates
    /// if present.
    pub async fn on_client_connected(&self, client_id: ClientId) {
        self.disconnect_candidates.write().await.remove(&client_id);
    }

    /// Run one sweep iteration: drain expired disconnect candidates,
    /// snapshot active connections, and reap those that are truly gone.
    /// Returns the list of reaped client IDs.
    pub async fn run_sweep_once(&self) -> Vec<ClientId> {
        let ttl = *self.client_ttl.read().await;
        let now = tokio::time::Instant::now();

        // Step 1: drain expired entries from candidates
        let expired: Vec<ClientId> = {
            let mut candidates = self.disconnect_candidates.write().await;
            let mut expired = Vec::new();
            candidates.retain(|&client_id, candidate| {
                if now.duration_since(candidate.disconnected_at) >= ttl {
                    expired.push(client_id);
                    false // remove from candidates
                } else {
                    true // keep
                }
            });
            expired
        };

        if expired.is_empty() {
            return Vec::new();
        }

        let mut reaped = Vec::new();
        let mut requeued = Vec::new();
        for client_id in expired {
            // Check for active connections right before reaping to close the
            // TOCTOU window: if a client reconnects between the candidate drain
            // above and this check, we see the new connection and skip.
            let has_connection = self
                .connections
                .read()
                .await
                .values()
                .any(|c| c.client_id == client_id);
            if has_connection {
                tracing::debug!(%client_id, "skipping reap: client reconnected");
                continue;
            }
            match self.runtime.remove_client(client_id) {
                Ok(true) => {
                    reaped.push(client_id);
                    tracing::debug!(%client_id, "reaped disconnected client");
                }
                Ok(false) => {
                    // Client has unpersisted data — re-queue for next sweep
                    requeued.push(client_id);
                }
                Err(e) => {
                    tracing::error!(%client_id, error = %e, "failed to reap client");
                }
            }
        }

        // Re-insert clients that couldn't be reaped yet
        if !requeued.is_empty() {
            let mut candidates = self.disconnect_candidates.write().await;
            for client_id in requeued {
                candidates.entry(client_id).or_insert(DisconnectCandidate {
                    disconnected_at: Instant::now(),
                });
            }
        }

        reaped
    }

    /// Update the client state TTL. Takes effect on the next sweep tick.
    pub async fn set_client_ttl(&self, ttl: Duration) {
        *self.client_ttl.write().await = ttl;
    }

    /// Process a raw binary payload received from a WebSocket client and push it
    /// into the runtime sync inbox.
    ///
    /// Frames are expected to be `OutboxEntry` JSON (as serialised by
    /// `TransportManager::run_connected`). If that parse fails we fall back to a
    /// raw `SyncBatchRequest` shape, which some callers send directly.
    pub async fn process_ws_client_frame(
        &self,
        client_id: ClientId,
        payload: &[u8],
    ) -> Result<(), String> {
        if let Ok(entry) =
            serde_json::from_slice::<crate::sync_manager::types::OutboxEntry>(payload)
        {
            let inbox = InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            };
            return self
                .runtime
                .push_sync_inbox(inbox)
                .map_err(|e| e.to_string());
        }

        match serde_json::from_slice::<crate::transport_protocol::SyncBatchRequest>(payload) {
            Ok(batch) => {
                for p in batch.payloads {
                    let inbox = InboxEntry {
                        source: Source::Client(client_id),
                        payload: p,
                    };
                    self.runtime
                        .push_sync_inbox(inbox)
                        .map_err(|e| e.to_string())?;
                }
                Ok(())
            }
            Err(e) => Err(format!("invalid ws payload: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::schema_manager::AppId;
    use crate::server::builder::{ServerBuilder, StorageBackend};

    async fn build_test_state() -> Arc<ServerState> {
        let app_id = AppId::from_name("lifecycle-test");
        let built = ServerBuilder::new(app_id)
            .with_storage(StorageBackend::InMemory)
            .build()
            .await
            .expect("build test server");
        built.state
    }

    /// Simulate adding a connection (like events_handler does).
    async fn add_connection(state: &ServerState, client_id: ClientId) -> u64 {
        let connection_id = state
            .next_connection_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        state
            .connections
            .write()
            .await
            .insert(connection_id, ConnectionState { client_id });
        connection_id
    }

    /// Simulate removing a connection (like stream cleanup does).
    async fn remove_connection(state: &ServerState, connection_id: u64) {
        let client_id = {
            let mut connections = state.connections.write().await;
            let conn = connections
                .remove(&connection_id)
                .expect("connection exists");
            conn.client_id
        };
        state.on_connection_closed(client_id).await;
    }

    #[tokio::test]
    async fn disconnect_adds_candidate_when_no_other_connections() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            candidates.contains_key(&alice),
            "alice should be a disconnect candidate"
        );
    }

    #[tokio::test]
    async fn disconnect_does_not_add_candidate_when_other_connections_exist() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn1 = add_connection(&state, alice).await;
        let _conn2 = add_connection(&state, alice).await;
        remove_connection(&state, conn1).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice still has an active connection"
        );
    }

    #[tokio::test]
    async fn reconnect_removes_candidate() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // alice reconnects
        let _conn2 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice reconnected — should not be a candidate"
        );
    }

    #[tokio::test]
    async fn disconnect_both_connections_adds_candidate() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn1 = add_connection(&state, alice).await;
        let conn2 = add_connection(&state, alice).await;
        remove_connection(&state, conn1).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(!candidates.contains_key(&alice), "alice still has conn2");
        drop(candidates);

        remove_connection(&state, conn2).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            candidates.contains_key(&alice),
            "both connections closed — alice should be a candidate"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_reaps_expired_candidates() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        // Register alice in the runtime so there's state to reap
        let _ = state.runtime.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Advance time past TTL (default 5 min)
        tokio::time::advance(Duration::from_secs(301)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice]);

        // Verify client state was actually removed
        let has_client = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(alice).is_some())
            .expect("lock");
        assert!(!has_client, "alice's ClientState should be gone");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_preserves_candidates_before_ttl() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.runtime.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Advance time but NOT past TTL
        tokio::time::advance(Duration::from_secs(60)).await;

        let reaped = state.run_sweep_once().await;
        assert!(reaped.is_empty());

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            candidates.contains_key(&alice),
            "alice should still be a candidate"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_skips_reconnected_client() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.runtime.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        tokio::time::advance(Duration::from_secs(301)).await;

        // Alice reconnects before sweep runs
        let _conn2 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        let reaped = state.run_sweep_once().await;
        assert!(
            reaped.is_empty(),
            "alice reconnected — should not be reaped"
        );

        let has_client = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(alice).is_some())
            .expect("lock");
        assert!(has_client, "alice's ClientState should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_skips_client_that_reconnected_after_expiry() {
        // Exercises the per-client connection check in run_sweep_once:
        // alice is in disconnect_candidates (expired), but also has an
        // active connection. Sweep should see the connection and skip her.
        //
        // We insert the candidate manually to bypass on_client_connected
        // (which would remove it), simulating the race where a reconnect
        // happens after candidates are drained but before the per-client
        // connection check.
        let state = build_test_state().await;
        let alice = ClientId::new();
        let _ = state.runtime.add_client(alice, None);

        // Insert an already-expired candidate directly
        {
            let mut candidates = state.disconnect_candidates.write().await;
            candidates.insert(
                alice,
                super::DisconnectCandidate {
                    disconnected_at: tokio::time::Instant::now() - Duration::from_secs(301),
                },
            );
        }

        // alice has an active connection (simulating reconnect)
        let _conn = add_connection(&state, alice).await;

        // Sweep drains alice from candidates (expired), but the
        // per-client connection check sees her connection → skip reap
        let reaped = state.run_sweep_once().await;
        assert!(
            reaped.is_empty(),
            "alice has active connection — should not be reaped"
        );

        let has_client = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(alice).is_some())
            .expect("lock");
        assert!(has_client, "alice's state should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_preserves_fresh_state_when_client_reconnects_after_drain() {
        //
        // Exercises the critical TOCTOU window in run_sweep_once:
        //
        //   sweep: drain candidates ──▶ (alice expired, removed from candidates)
        //                                     │
        //   alice: reconnect ──▶ ensure_client_with_session (fresh ClientState)
        //                        on_client_connected (no-op, already drained)
        //                        add_connection (visible in connections map)
        //                                     │
        //   sweep: per-client connection check ──▶ sees alice's connection → skip
        //
        // Without the per-client check, the sweep would use a stale snapshot
        // and destroy alice's freshly registered state.
        //
        let state = build_test_state().await;
        let alice = ClientId::new();
        let _ = state.runtime.add_client(alice, None);

        // alice disconnects and expires
        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;
        tokio::time::advance(Duration::from_secs(301)).await;

        // Manually drain candidates (simulating the first phase of run_sweep_once)
        let expired: Vec<ClientId> = {
            let mut candidates = state.disconnect_candidates.write().await;
            let drained: Vec<_> = candidates.keys().copied().collect();
            candidates.clear();
            drained
        };
        assert_eq!(expired, vec![alice]);

        // alice reconnects AFTER drain — fresh state created, new connection added
        let _ = state.runtime.ensure_client_with_session(
            alice,
            crate::query_manager::session::Session::new("alice"),
        );
        let _conn2 = add_connection(&state, alice).await;
        // on_client_connected is a no-op here (alice was already drained)
        state.on_client_connected(alice).await;

        // Now run the full sweep — it should see alice's connection and skip her
        // (we need to re-insert alice as expired to let sweep process her,
        // since we manually drained above)
        {
            let mut candidates = state.disconnect_candidates.write().await;
            candidates.insert(
                alice,
                super::DisconnectCandidate {
                    disconnected_at: tokio::time::Instant::now() - Duration::from_secs(301),
                },
            );
        }
        let reaped = state.run_sweep_once().await;

        assert!(
            reaped.is_empty(),
            "alice has fresh state and active connection — must not be reaped"
        );

        let has_client = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(alice).is_some())
            .expect("lock");
        assert!(has_client, "alice's fresh ClientState should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_reaps_multiple_expired_candidates() {
        let state = build_test_state().await;
        let alice = ClientId::new();
        let bob = ClientId::new();

        let _ = state.runtime.add_client(alice, None);
        let _ = state.runtime.add_client(bob, None);

        let conn_a = add_connection(&state, alice).await;
        let conn_b = add_connection(&state, bob).await;
        remove_connection(&state, conn_a).await;
        remove_connection(&state, conn_b).await;

        tokio::time::advance(Duration::from_secs(301)).await;

        let mut reaped = state.run_sweep_once().await;
        reaped.sort_by_key(|c| c.0);
        let mut expected = vec![alice, bob];
        expected.sort_by_key(|c| c.0);
        assert_eq!(reaped, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_does_not_affect_other_clients() {
        let state = build_test_state().await;
        let alice = ClientId::new();
        let bob = ClientId::new();

        let _ = state.runtime.add_client(alice, None);
        let _ = state.runtime.add_client(bob, None);

        // Only alice disconnects
        let conn_a = add_connection(&state, alice).await;
        let _conn_b = add_connection(&state, bob).await;
        remove_connection(&state, conn_a).await;

        tokio::time::advance(Duration::from_secs(301)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice]);

        let has_bob = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(bob).is_some())
            .expect("lock");
        assert!(has_bob, "bob should be unaffected");
    }

    #[tokio::test(start_paused = true)]
    async fn set_client_ttl_changes_sweep_behavior() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.runtime.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Set TTL to 1 second
        state.set_client_ttl(Duration::from_secs(1)).await;

        tokio::time::advance(Duration::from_secs(2)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice], "alice should be reaped with 1s TTL");
    }

    #[tokio::test(start_paused = true)]
    async fn runtime_ttl_change_takes_effect_on_next_sweep() {
        let state = build_test_state().await;
        let alice = ClientId::new();

        let _ = state.runtime.add_client(alice, None);

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Advance 2 seconds — not past default 5 min TTL
        tokio::time::advance(Duration::from_secs(2)).await;

        let reaped = state.run_sweep_once().await;
        assert!(reaped.is_empty(), "default TTL: alice should survive");

        // Now change TTL to 1 second — alice has been disconnected for 2s
        state.set_client_ttl(Duration::from_secs(1)).await;

        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice], "new TTL: alice should be reaped");
    }

    #[tokio::test(start_paused = true)]
    async fn reconnect_after_reaping_produces_fresh_state() {
        //
        // alice ──connects──▶ server ──disconnects──▶ TTL expires ──▶ reaped
        //                                                              │
        //                     alice ──reconnects──▶ fresh ClientState ◀┘
        //
        use crate::query_manager::session::Session;

        let state = build_test_state().await;
        let alice = ClientId::new();
        let session = Session::new("alice");

        // Connect, register with session
        let _ = state.runtime.add_client(alice, None);
        let _ = state
            .runtime
            .ensure_client_with_session(alice, session.clone());

        let conn = add_connection(&state, alice).await;
        remove_connection(&state, conn).await;

        // Reap
        tokio::time::advance(Duration::from_secs(301)).await;
        let reaped = state.run_sweep_once().await;
        assert_eq!(reaped, vec![alice]);

        // Verify gone
        let has_client = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(alice).is_some())
            .expect("lock");
        assert!(!has_client, "alice should be gone after reaping");

        // Reconnect — should get fresh state
        let _ = state.runtime.ensure_client_with_session(alice, session);
        let _conn2 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        let has_client = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(alice).is_some())
            .expect("lock");
        assert!(
            has_client,
            "alice should have fresh ClientState after reconnect"
        );

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice should not be a disconnect candidate"
        );
    }

    // ========================================================================
    // Lock ordering / sequential correctness tests
    //
    // These exercise sequential lock acquisition patterns to verify that
    // the candidates(write) → connections(read) nesting in on_connection_closed
    // produces correct state transitions when interleaved with other operations.
    // Note: single-threaded tokio cannot detect true two-task deadlocks.
    // ========================================================================

    #[tokio::test]
    async fn lock_ordering_disconnect_and_reconnect() {
        // Exercises sequential lock ordering: on_connection_closed takes
        // candidates(write) → connections(read), while on_client_connected
        // takes candidates(write). Running them sequentially verifies correct
        // state transitions under interleaved operations.
        let state = build_test_state().await;
        let alice = ClientId::new();

        let conn1 = add_connection(&state, alice).await;
        let conn2 = add_connection(&state, alice).await;

        // Simulate: conn1 closes, alice reconnects (via on_client_connected),
        // then conn2 closes — all interleaved.
        {
            let mut connections = state.connections.write().await;
            connections.remove(&conn1);
        }
        state.on_connection_closed(alice).await;

        // alice "reconnects" (conn2 is still active, plus a new one)
        let _conn3 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        // conn2 closes
        {
            let mut connections = state.connections.write().await;
            connections.remove(&conn2);
        }
        state.on_connection_closed(alice).await;

        // conn3 still active — should NOT be a candidate
        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice still has conn3 — must not be a candidate"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lock_ordering_sweep_during_disconnect() {
        // Exercises sequential lock ordering: sweep takes candidates(write)
        // then connections(read), while on_connection_closed takes
        // candidates(write) → connections(read). Verifies correct state
        // transitions when these operations are interleaved.
        let state = build_test_state().await;
        let alice = ClientId::new();
        let bob = ClientId::new();

        let _ = state.runtime.add_client(alice, None);
        let _ = state.runtime.add_client(bob, None);

        // alice disconnects and expires
        let conn_a = add_connection(&state, alice).await;
        remove_connection(&state, conn_a).await;
        tokio::time::advance(Duration::from_secs(301)).await;

        // bob disconnects while sweep is about to run
        let conn_b = add_connection(&state, bob).await;
        {
            let mut connections = state.connections.write().await;
            connections.remove(&conn_b);
        }

        // Interleave: sweep runs, then bob's on_connection_closed
        let reaped = state.run_sweep_once().await;
        state.on_connection_closed(bob).await;

        assert_eq!(reaped, vec![alice], "only alice should be reaped");

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            candidates.contains_key(&bob),
            "bob should be a candidate after disconnect"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn lock_ordering_sweep_and_reconnect() {
        // Exercises sequential lock ordering: sweep takes candidates(write)
        // then per-client connections(read), while connect path takes
        // connections(write) then on_client_connected takes candidates(write).
        // Verifies correct state transitions when these operations are
        // interleaved.
        let state = build_test_state().await;
        let alice = ClientId::new();
        let bob = ClientId::new();

        let _ = state.runtime.add_client(alice, None);
        let _ = state.runtime.add_client(bob, None);

        let conn_a = add_connection(&state, alice).await;
        let conn_b = add_connection(&state, bob).await;
        remove_connection(&state, conn_a).await;
        remove_connection(&state, conn_b).await;
        tokio::time::advance(Duration::from_secs(301)).await;

        // Interleave: alice reconnects before sweep runs — sweep's
        // per-client connection check catches her active connection
        let _conn_a2 = add_connection(&state, alice).await;
        state.on_client_connected(alice).await;

        let reaped = state.run_sweep_once().await;

        // alice reconnected — not reaped. bob — reaped.
        assert_eq!(reaped, vec![bob]);

        let has_alice = state
            .runtime
            .with_sync_manager(|sm| sm.get_client(alice).is_some())
            .expect("lock");
        assert!(has_alice, "alice reconnected — should be preserved");
    }

    #[tokio::test(start_paused = true)]
    async fn sweep_task_exits_when_state_is_dropped() {
        // The sweep task holds a Weak<ServerState>. When all strong refs are
        // dropped, the task should exit on its next tick.
        //
        // We verify by dropping all strong refs, advancing past the sweep
        // interval, and yielding. If the Weak upgrade succeeds (leak),
        // the sweep task would run forever and the test would hang.
        // With start_paused=true + advance, the interval tick fires
        // deterministically.
        let state = build_test_state().await;

        // Drop all strong refs — sweep task holds only a Weak
        drop(state);

        // Advance past sweep interval (30s) so the task would tick
        tokio::time::advance(Duration::from_secs(31)).await;
        tokio::task::yield_now().await;

        // If we get here without hanging, the Weak upgrade failed and the
        // task exited. With start_paused=true + advance, the interval tick
        // fires deterministically, and the Weak::upgrade returns None.
    }

    #[tokio::test]
    async fn on_connection_closed_is_atomic_wrt_candidates() {
        // Verifies that on_connection_closed checks connections and inserts
        // into candidates atomically (under the candidates write lock).
        // If a reconnect happens after the connection is removed but before
        // on_connection_closed runs, the candidate insertion must see the
        // new connection and NOT insert.
        let state = build_test_state().await;
        let alice = ClientId::new();

        // alice has one connection
        let conn1 = add_connection(&state, alice).await;

        // Remove conn1 from connections map
        {
            let mut connections = state.connections.write().await;
            connections.remove(&conn1);
        }

        // alice reconnects (new connection added) BEFORE on_connection_closed runs
        let _conn2 = add_connection(&state, alice).await;

        // Now on_connection_closed runs — should see conn2 and NOT insert
        state.on_connection_closed(alice).await;

        let candidates = state.disconnect_candidates.read().await;
        assert!(
            !candidates.contains_key(&alice),
            "alice has conn2 — on_connection_closed must see it and not insert"
        );
    }
}
