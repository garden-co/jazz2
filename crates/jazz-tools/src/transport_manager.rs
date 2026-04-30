//! WebSocket transport layer: TransportHandle, TransportManager, StreamAdapter, TickNotifier.
//!
//! TransportHandle is held by RuntimeCore (replaces SyncSender).
//! TransportManager owns the live WebSocket connection and reconnects on failure.

use crate::query_manager::types::SchemaHash;
use crate::sync_manager::types::{ClientId, InboxEntry, OutboxEntry, ServerId};
use futures::channel::mpsc;

pub trait TickNotifier: 'static {
    fn notify(&self);
}

#[allow(async_fn_in_trait)]
pub trait StreamAdapter: Sized {
    type Error: std::fmt::Display;
    async fn connect(url: &str) -> Result<Self, Self::Error>;
    async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error>;
    async fn close(&mut self);
}

#[derive(Debug)]
pub enum TransportInbound {
    Connected {
        catalogue_state_hash: Option<String>,
        next_sync_seq: Option<u64>,
    },
    Sync {
        entry: Box<InboxEntry>,
        sequence: Option<u64>,
    },
    Disconnected,
    /// First connect/handshake attempt after `install_transport` failed before
    /// the handshake completed (DNS/TCP/TLS error, or handshake network error).
    /// Consumers use this to release the initial frontier hold so subscriptions
    /// can deliver local state while the transport keeps retrying in the
    /// background.
    ConnectFailed {
        reason: String,
    },
    /// Server rejected the auth handshake with an Unauthorized error.
    /// The transport suspends retries and waits for `TransportControl::UpdateAuth`
    /// or `TransportControl::Shutdown` before attempting a new connection.
    AuthFailure {
        reason: String,
    },
}

#[derive(Debug)]
pub enum TransportControl {
    Shutdown,
    UpdateAuth(AuthConfig),
}

// M-6: derive Debug — all fields implement Debug.
#[derive(Debug)]
pub struct TransportHandle {
    pub server_id: ServerId,
    pub client_id: ClientId,
    pub outbox_tx: mpsc::UnboundedSender<OutboxEntry>,
    pub inbound_rx: mpsc::UnboundedReceiver<TransportInbound>,
    pub ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Async signal that flips to `true` on the first successful handshake.
    /// Callers can `clone()` the receiver and `await` `wait_for(|v| *v)` to
    /// be notified without polling. Kept alongside `ever_connected` so the
    /// wasm transport and other non-native wait paths can still read the
    /// latched state synchronously.
    #[cfg(feature = "transport-websocket")]
    pub(crate) connected_rx: tokio::sync::watch::Receiver<bool>,
    pub control_tx: mpsc::UnboundedSender<TransportControl>,
    /// Client's current catalogue-state digest. The TransportManager reads
    /// this at each handshake attempt so reconnects can tell the server
    /// whether catalogue replay is necessary. Shared with the manager via
    /// `Arc`.
    pub(crate) catalogue_state_hash: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    /// Client's declared structural schema hash. Sent separately from the
    /// catalogue-state digest so the server can emit schema diagnostics
    /// against a real schema hash.
    pub(crate) declared_schema_hash: std::sync::Arc<std::sync::Mutex<Option<String>>>,
}

impl TransportHandle {
    /// Returns None both when the channel is empty and when it's closed.
    pub fn try_recv_inbound(&mut self) -> Option<TransportInbound> {
        self.inbound_rx.try_recv().ok()
    }
    pub fn send_outbox(&self, entry: OutboxEntry) {
        let _ = self.outbox_tx.unbounded_send(entry);
    }
    pub fn has_ever_connected(&self) -> bool {
        self.ever_connected
            .load(std::sync::atomic::Ordering::Acquire)
    }
    pub fn disconnect(&self) {
        let _ = self.control_tx.unbounded_send(TransportControl::Shutdown);
    }
    pub fn update_auth(&self, auth: AuthConfig) {
        let _ = self
            .control_tx
            .unbounded_send(TransportControl::UpdateAuth(auth));
    }
    /// Update the catalogue state hash sent in subsequent auth handshakes.
    /// Callers use this when the client's catalogue changes so the next
    /// reconnect hands the server a fresh hash.
    pub fn set_catalogue_state_hash(&self, hash: Option<String>) {
        if let Ok(mut slot) = self.catalogue_state_hash.lock() {
            *slot = hash;
        }
    }

    /// Test-only accessor: returns the current catalogue state hash stored in
    /// this handle.
    #[cfg(test)]
    pub fn catalogue_state_hash_for_test(&self) -> Option<String> {
        self.catalogue_state_hash
            .lock()
            .ok()
            .and_then(|g| g.clone())
    }

    /// Update the declared schema hash sent in subsequent auth handshakes.
    pub fn set_declared_schema_hash(&self, hash: Option<String>) {
        if let Ok(mut slot) = self.declared_schema_hash.lock() {
            *slot = hash;
        }
    }

    /// Test-only accessor: returns the current declared schema hash stored in
    /// this handle.
    #[cfg(test)]
    pub fn declared_schema_hash_for_test(&self) -> Option<String> {
        self.declared_schema_hash
            .lock()
            .ok()
            .and_then(|g| g.clone())
    }
}

// I-4: hand-written Debug that redacts secret fields.
#[derive(Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct AuthConfig {
    pub jwt_token: Option<String>,
    pub backend_secret: Option<String>,
    pub admin_secret: Option<String>,
    pub backend_session: Option<serde_json::Value>,
}

impl std::fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuthConfig")
            .field("jwt_token", &self.jwt_token.as_ref().map(|_| "<redacted>"))
            .field(
                "backend_secret",
                &self.backend_secret.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "admin_secret",
                &self.admin_secret.as_ref().map(|_| "<redacted>"),
            )
            // backend_session may itself contain secrets; redact presence only.
            .field(
                "backend_session",
                &self.backend_session.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct AuthHandshake {
    pub client_id: String,
    pub auth: AuthConfig,
    pub catalogue_state_hash: Option<String>,
    pub declared_schema_hash: Option<String>,
}

impl AuthHandshake {
    pub fn declared_schema_hash(&self) -> Option<SchemaHash> {
        self.declared_schema_hash
            .as_deref()
            .and_then(SchemaHash::from_hex)
    }
}

#[cfg(test)]
mod handshake_tests {
    use super::*;
    use crate::query_manager::types::SchemaHash;

    #[test]
    fn auth_handshake_uses_declared_schema_hash_not_catalogue_state_hash() {
        let declared_hash = SchemaHash::from_bytes([7; 32]);
        let catalogue_hash = "ab".repeat(32);
        let handshake = AuthHandshake {
            client_id: "client-1".to_string(),
            auth: AuthConfig::default(),
            catalogue_state_hash: Some(catalogue_hash.clone()),
            declared_schema_hash: Some(declared_hash.to_string()),
        };

        assert_eq!(handshake.declared_schema_hash(), Some(declared_hash));

        let handshake_without_declared_hash = AuthHandshake {
            client_id: "client-1".to_string(),
            auth: AuthConfig::default(),
            catalogue_state_hash: Some(catalogue_hash),
            declared_schema_hash: None,
        };

        assert_eq!(handshake_without_declared_hash.declared_schema_hash(), None);
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ConnectedResponse {
    pub connection_id: String,
    pub client_id: String,
    pub next_sync_seq: Option<u64>,
    pub catalogue_state_hash: Option<String>,
}

#[derive(Default)]
pub struct ReconnectState {
    attempt: u32,
}

impl ReconnectState {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub async fn backoff(&mut self) {
        // I-2: cap applied AFTER adding jitter so the 10_000 ceiling is meaningful
        // at higher attempt counts if the min(5) exponent cap is ever raised.
        let base_ms = 300u64.saturating_mul(1u64 << self.attempt.min(5));
        let jitter = (rand::random::<u8>() as u64 * 200) / 255;
        let delay_ms = (base_ms + jitter).min(10_000);
        #[cfg(all(not(target_arch = "wasm32"), feature = "runtime-tokio"))]
        {
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        #[cfg(target_arch = "wasm32")]
        {
            gloo_timers::future::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }
        #[cfg(all(not(target_arch = "wasm32"), not(feature = "runtime-tokio")))]
        {
            let _ = delay_ms;
            futures::future::ready(()).await;
        }
        self.attempt += 1;
    }
}

pub struct TransportManager<W: StreamAdapter, T: TickNotifier> {
    pub server_id: ServerId,
    pub url: String,
    pub auth: AuthConfig,
    outbox_rx: mpsc::UnboundedReceiver<OutboxEntry>,
    inbound_tx: mpsc::UnboundedSender<TransportInbound>,
    pub tick: T,
    reconnect: ReconnectState,
    pub client_id: ClientId,
    ever_connected: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// Sender side of the handshake-completion watch. Held by the manager
    /// task so it can broadcast `true` once on the first handshake. Only
    /// present for the native Tokio WebSocket wait path; the wasm transport
    /// relies solely on the `AtomicBool` above.
    #[cfg(feature = "transport-websocket")]
    connected_tx: tokio::sync::watch::Sender<bool>,
    control_rx: mpsc::UnboundedReceiver<TransportControl>,
    /// Shared with `TransportHandle::catalogue_state_hash`. Read at each
    /// handshake attempt so reconnects can reflect catalogue changes.
    catalogue_state_hash: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    /// Shared with `TransportHandle::declared_schema_hash`. Read at each
    /// handshake attempt so the server sees the client's structural schema.
    declared_schema_hash: std::sync::Arc<std::sync::Mutex<Option<String>>>,
    _stream: std::marker::PhantomData<W>,
}

pub fn create<W: StreamAdapter, T: TickNotifier>(
    url: String,
    auth: AuthConfig,
    tick: T,
) -> (TransportHandle, TransportManager<W, T>) {
    let server_id = ServerId::new();
    let client_id = ClientId::new();
    let (outbox_tx, outbox_rx) = mpsc::unbounded();
    let (inbound_tx, inbound_rx) = mpsc::unbounded();
    let (control_tx, control_rx) = mpsc::unbounded();
    let ever_connected = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    #[cfg(feature = "transport-websocket")]
    let (connected_tx, connected_rx) = tokio::sync::watch::channel(false);
    let catalogue_state_hash = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
    let declared_schema_hash = std::sync::Arc::new(std::sync::Mutex::new(None::<String>));
    let handle = TransportHandle {
        server_id,
        client_id,
        outbox_tx,
        inbound_rx,
        ever_connected: ever_connected.clone(),
        #[cfg(feature = "transport-websocket")]
        connected_rx,
        control_tx,
        catalogue_state_hash: catalogue_state_hash.clone(),
        declared_schema_hash: declared_schema_hash.clone(),
    };
    let manager = TransportManager {
        server_id,
        url,
        auth,
        outbox_rx,
        inbound_tx,
        tick,
        reconnect: ReconnectState::new(),
        client_id,
        ever_connected,
        #[cfg(feature = "transport-websocket")]
        connected_tx,
        control_rx,
        catalogue_state_hash,
        declared_schema_hash,
        _stream: std::marker::PhantomData,
    };
    (handle, manager)
}

/// Encode a payload as a 4-byte big-endian length-prefixed frame.
pub(crate) fn frame_encode(payload: &[u8]) -> Vec<u8> {
    debug_assert!(
        payload.len() <= u32::MAX as usize,
        "frame payload exceeds u32 limit"
    );
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode a 4-byte big-endian length-prefixed frame, returning the payload slice.
pub(crate) fn frame_decode(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + len {
        return None;
    }
    Some(&data[4..4 + len])
}

/// Outcome of the auth handshake.
pub(crate) enum HandshakeResult {
    /// Server accepted; connection is open.
    Connected(ConnectedResponse),
    /// Server rejected the auth credentials.  Transport should suspend.
    AuthFailure(String),
    /// Network or protocol error; transport should back off and retry.
    NetworkError(String),
}

/// Handshake helpers shared between the Tokio and WASM run loops.
impl<W: StreamAdapter + 'static, T: TickNotifier + 'static> TransportManager<W, T> {
    /// Build the length-prefixed handshake frame from the current client identity, auth,
    /// and the latest catalogue + declared schema hashes known to the caller.
    fn build_handshake_frame(&self) -> Vec<u8> {
        let catalogue_state_hash = self
            .catalogue_state_hash
            .lock()
            .ok()
            .and_then(|g| g.clone());
        let declared_schema_hash = self
            .declared_schema_hash
            .lock()
            .ok()
            .and_then(|g| g.clone());
        let handshake = AuthHandshake {
            client_id: self.client_id.to_string(),
            auth: self.auth.clone(),
            catalogue_state_hash,
            declared_schema_hash,
        };
        let payload =
            serde_json::to_vec(&handshake).expect("AuthHandshake serialisation infallible");
        frame_encode(&payload)
    }

    /// Send the pre-built handshake frame and wait for the server's response.
    ///
    /// Distinguishes three outcomes:
    /// - `Connected` — server sent a valid `ConnectedResponse`
    /// - `AuthFailure` — server sent `ServerEvent::Error { code: Unauthorized }`
    /// - `NetworkError` — any other failure (network drop, parse error, etc.)
    async fn do_handshake(ws: &mut W, frame: Vec<u8>) -> HandshakeResult {
        if let Err(e) = ws.send(&frame).await {
            return HandshakeResult::NetworkError(e.to_string());
        }
        let resp_bytes = match ws.recv().await {
            Ok(Some(b)) => b,
            Ok(None) => {
                return HandshakeResult::NetworkError(
                    "server closed before handshake response".to_string(),
                );
            }
            Err(e) => return HandshakeResult::NetworkError(e.to_string()),
        };
        let Some(resp_payload) = frame_decode(&resp_bytes) else {
            return HandshakeResult::NetworkError("malformed handshake response".to_string());
        };

        // First try to parse as the success path.
        if let Ok(resp) = serde_json::from_slice::<ConnectedResponse>(resp_payload) {
            return HandshakeResult::Connected(resp);
        }

        // Fall back: check whether the server sent an explicit Error event.
        if let Ok(crate::transport_protocol::ServerEvent::Error { message, code }) =
            serde_json::from_slice::<crate::transport_protocol::ServerEvent>(resp_payload)
        {
            if code == crate::transport_protocol::ErrorCode::Unauthorized {
                return HandshakeResult::AuthFailure(message);
            }
            return HandshakeResult::NetworkError(format!("server error ({code:?}): {message}"));
        }

        HandshakeResult::NetworkError("unexpected handshake response".to_string())
    }
}

#[cfg(feature = "runtime-tokio")]
enum ConnectedExit {
    NetworkError,
    Shutdown,
    UpdateAuth(AuthConfig),
}

enum ControlOrPhase<T> {
    Control(Option<TransportControl>),
    Phase(T),
}

#[cfg(feature = "runtime-tokio")]
impl<W: StreamAdapter + 'static, T: TickNotifier + 'static> TransportManager<W, T> {
    /// Drive the transport: connect, authenticate, relay frames, reconnect on failure.
    /// Returns only when the `TransportHandle` is dropped or a Shutdown control is received.
    pub async fn run(mut self) {
        use futures::StreamExt as _;
        loop {
            // Phase: Connect.
            let connect_outcome = tokio::select! {
                biased;
                ctrl = self.control_rx.next() => ControlOrPhase::Control(ctrl),
                res = W::connect(&self.url) => ControlOrPhase::Phase(res),
            };
            let ws = match connect_outcome {
                ControlOrPhase::Control(None)
                | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => return,
                ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
                    self.auth = auth;
                    self.reconnect.reset();
                    continue;
                }
                ControlOrPhase::Phase(Ok(ws)) => ws,
                ControlOrPhase::Phase(Err(e)) => {
                    let reason = format!("{e}");
                    tracing::warn!("ws connect failed: {reason}");
                    let _ = self
                        .inbound_tx
                        .unbounded_send(TransportInbound::ConnectFailed { reason });
                    self.tick.notify();
                    let backoff_outcome = tokio::select! {
                        biased;
                        ctrl = self.control_rx.next() => ControlOrPhase::Control(ctrl),
                        _ = self.reconnect.backoff() => ControlOrPhase::Phase(()),
                    };
                    match backoff_outcome {
                        ControlOrPhase::Control(None)
                        | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => return,
                        ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
                            self.auth = auth;
                            self.reconnect.reset();
                            continue;
                        }
                        ControlOrPhase::Phase(()) => {}
                    }
                    continue;
                }
            };

            // Handshake phase: race against control channel so Shutdown/UpdateAuth is
            // observed even while waiting for the server's handshake response.
            // Build the outbound frame before entering the select so `self` is not
            // mutably borrowed through `perform_auth_handshake` at the same time as
            // `self.control_rx` (which would violate the single-mutable-borrow rule).
            let mut ws = ws;
            let handshake_outcome = {
                let handshake_frame = self.build_handshake_frame();
                tokio::select! {
                    biased;
                    ctrl = self.control_rx.next() => ControlOrPhase::Control(ctrl),
                    res = Self::do_handshake(&mut ws, handshake_frame) => ControlOrPhase::Phase(res),
                }
            };
            match handshake_outcome {
                ControlOrPhase::Control(None)
                | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => {
                    ws.close().await;
                    return;
                }
                ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
                    self.auth = auth;
                    ws.close().await;
                    self.reconnect.reset();
                    continue;
                }
                ControlOrPhase::Phase(HandshakeResult::Connected(resp)) => {
                    self.ever_connected
                        .store(true, std::sync::atomic::Ordering::Release);
                    #[cfg(feature = "transport-websocket")]
                    let _ = self.connected_tx.send(true);
                    let _ = self.inbound_tx.unbounded_send(TransportInbound::Connected {
                        catalogue_state_hash: resp.catalogue_state_hash,
                        next_sync_seq: resp.next_sync_seq,
                    });
                    self.tick.notify();
                    self.reconnect.reset();
                    match self.run_connected(&mut ws).await {
                        ConnectedExit::Shutdown => {
                            ws.close().await;
                            return;
                        }
                        ConnectedExit::NetworkError => {
                            let _ = self
                                .inbound_tx
                                .unbounded_send(TransportInbound::Disconnected);
                            self.tick.notify();
                            ws.close().await;
                        }
                        ConnectedExit::UpdateAuth(auth) => {
                            self.auth = auth;
                            let _ = self
                                .inbound_tx
                                .unbounded_send(TransportInbound::Disconnected);
                            self.tick.notify();
                            ws.close().await;
                            self.reconnect.reset();
                            continue;
                        }
                    }
                }
                ControlOrPhase::Phase(HandshakeResult::AuthFailure(reason)) => {
                    tracing::warn!(%reason, "ws auth handshake rejected: unauthorized");
                    ws.close().await;
                    let _ = self
                        .inbound_tx
                        .unbounded_send(TransportInbound::AuthFailure { reason });
                    self.tick.notify();
                    // Suspend reconnect loop; wait for UpdateAuth or Shutdown.
                    match self.control_rx.next().await {
                        None | Some(TransportControl::Shutdown) => return,
                        Some(TransportControl::UpdateAuth(auth)) => {
                            self.auth = auth;
                            self.reconnect.reset();
                        }
                    }
                    continue;
                }
                ControlOrPhase::Phase(HandshakeResult::NetworkError(e)) => {
                    tracing::warn!("ws auth handshake failed: {e}");
                    let _ = self
                        .inbound_tx
                        .unbounded_send(TransportInbound::ConnectFailed { reason: e });
                    self.tick.notify();
                    ws.close().await;
                }
            }
            let backoff_outcome = tokio::select! {
                biased;
                ctrl = self.control_rx.next() => ControlOrPhase::Control(ctrl),
                _ = self.reconnect.backoff() => ControlOrPhase::Phase(()),
            };
            match backoff_outcome {
                ControlOrPhase::Control(None)
                | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => return,
                ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
                    self.auth = auth;
                    self.reconnect.reset();
                    continue;
                }
                ControlOrPhase::Phase(()) => {}
            }
        }
    }

    async fn run_connected(&mut self, ws: &mut W) -> ConnectedExit {
        use futures::StreamExt as _;
        loop {
            tokio::select! {
                out = self.outbox_rx.next() => {
                    // outbox closed = handle dropped; control_rx will also return None shortly.
                    // Route to Shutdown so the same clean-exit path is taken.
                    let Some(entry) = out else { return ConnectedExit::Shutdown; };
                    let Ok(bytes) = serde_json::to_vec(&entry) else { continue; };
                    let frame = frame_encode(&bytes);
                    if ws.send(&frame).await.is_err() { return ConnectedExit::NetworkError; }
                }
                incoming = ws.recv() => {
                    match incoming {
                        Ok(Some(data)) => {
                            let Some(payload) = frame_decode(&data) else { continue; };
                            let Ok(event) = serde_json::from_slice::<crate::transport_protocol::ServerEvent>(payload) else { continue; };
                            match event {
                                crate::transport_protocol::ServerEvent::SyncUpdate { seq, payload } => {
                                    let entry = InboxEntry {
                                        source: crate::sync_manager::types::Source::Server(self.server_id),
                                        payload: *payload,
                                    };
                                    let _ = self.inbound_tx.unbounded_send(TransportInbound::Sync {
                                        entry: Box::new(entry),
                                        sequence: seq,
                                    });
                                    self.tick.notify();
                                }
                                crate::transport_protocol::ServerEvent::Heartbeat => {}
                                crate::transport_protocol::ServerEvent::Connected { .. } => {
                                    tracing::warn!("unexpected Connected frame mid-stream; ignoring");
                                }
                                crate::transport_protocol::ServerEvent::Error { message, code } => {
                                    tracing::warn!(message, ?code, "server reported error");
                                }
                                other => {
                                    tracing::debug!(variant = other.variant_name(), "received non-sync ServerEvent; skipping");
                                }
                            }
                        }
                        Ok(None) | Err(_) => return ConnectedExit::NetworkError,
                    }
                }
                ctrl = self.control_rx.next() => {
                    match ctrl {
                        None | Some(TransportControl::Shutdown) => return ConnectedExit::Shutdown,
                        Some(TransportControl::UpdateAuth(auth)) => return ConnectedExit::UpdateAuth(auth),
                    }
                }
            }
        }
    }
}

// WASM-compatible run() — uses `futures::select!` instead of `tokio::select!`.
// Activated when `runtime-tokio` is not (i.e. WASM).
#[cfg(not(feature = "runtime-tokio"))]
enum WasmConnectedExit {
    NetworkError,
    Shutdown,
    UpdateAuth(AuthConfig),
}

#[cfg(not(feature = "runtime-tokio"))]
impl<W: StreamAdapter + 'static, T: TickNotifier + 'static> TransportManager<W, T> {
    /// Drive the transport: connect, authenticate, relay frames, reconnect on failure.
    /// Returns only when the `TransportHandle` is dropped or a Shutdown control is received.
    pub async fn run(mut self) {
        use futures::{FutureExt as _, StreamExt as _};
        loop {
            // Phase: Connect.
            let connect_outcome = futures::select! {
                ctrl = self.control_rx.next().fuse() => ControlOrPhase::Control(ctrl),
                res = W::connect(&self.url).fuse() => ControlOrPhase::Phase(res),
            };
            let ws = match connect_outcome {
                ControlOrPhase::Control(None)
                | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => return,
                ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
                    self.auth = auth;
                    self.reconnect.reset();
                    continue;
                }
                ControlOrPhase::Phase(Ok(ws)) => ws,
                ControlOrPhase::Phase(Err(e)) => {
                    let reason = format!("{e}");
                    tracing::warn!("ws connect failed: {reason}");
                    let _ = self
                        .inbound_tx
                        .unbounded_send(TransportInbound::ConnectFailed { reason });
                    self.tick.notify();
                    let backoff_outcome = futures::select! {
                        ctrl = self.control_rx.next().fuse() => ControlOrPhase::Control(ctrl),
                        _ = self.reconnect.backoff().fuse() => ControlOrPhase::Phase(()),
                    };
                    match backoff_outcome {
                        ControlOrPhase::Control(None)
                        | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => return,
                        ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
                            self.auth = auth;
                            self.reconnect.reset();
                            continue;
                        }
                        ControlOrPhase::Phase(()) => {}
                    }
                    continue;
                }
            };

            // Handshake phase: race against control channel so Shutdown/UpdateAuth is
            // observed even while waiting for the server's handshake response.
            let mut ws = ws;
            let handshake_outcome = {
                let handshake_frame = self.build_handshake_frame();
                futures::select! {
                    ctrl = self.control_rx.next().fuse() => ControlOrPhase::Control(ctrl),
                    res = Self::do_handshake(&mut ws, handshake_frame).fuse() => ControlOrPhase::Phase(res),
                }
            };
            match handshake_outcome {
                ControlOrPhase::Control(None)
                | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => {
                    ws.close().await;
                    return;
                }
                ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
                    self.auth = auth;
                    ws.close().await;
                    self.reconnect.reset();
                    continue;
                }
                ControlOrPhase::Phase(HandshakeResult::Connected(resp)) => {
                    self.ever_connected
                        .store(true, std::sync::atomic::Ordering::Release);
                    let _ = self.inbound_tx.unbounded_send(TransportInbound::Connected {
                        catalogue_state_hash: resp.catalogue_state_hash,
                        next_sync_seq: resp.next_sync_seq,
                    });
                    self.tick.notify();
                    self.reconnect.reset();
                    match self.wasm_run_connected(&mut ws).await {
                        WasmConnectedExit::Shutdown => {
                            ws.close().await;
                            return;
                        }
                        WasmConnectedExit::NetworkError => {
                            let _ = self
                                .inbound_tx
                                .unbounded_send(TransportInbound::Disconnected);
                            self.tick.notify();
                            ws.close().await;
                        }
                        WasmConnectedExit::UpdateAuth(auth) => {
                            self.auth = auth;
                            let _ = self
                                .inbound_tx
                                .unbounded_send(TransportInbound::Disconnected);
                            self.tick.notify();
                            ws.close().await;
                            self.reconnect.reset();
                            continue;
                        }
                    }
                }
                ControlOrPhase::Phase(HandshakeResult::AuthFailure(reason)) => {
                    tracing::warn!(%reason, "ws auth handshake rejected: unauthorized");
                    ws.close().await;
                    let _ = self
                        .inbound_tx
                        .unbounded_send(TransportInbound::AuthFailure { reason });
                    self.tick.notify();
                    // Suspend reconnect loop; wait for UpdateAuth or Shutdown.
                    use futures::StreamExt as _;
                    match self.control_rx.next().await {
                        None | Some(TransportControl::Shutdown) => return,
                        Some(TransportControl::UpdateAuth(auth)) => {
                            self.auth = auth;
                            self.reconnect.reset();
                        }
                    }
                    continue;
                }
                ControlOrPhase::Phase(HandshakeResult::NetworkError(e)) => {
                    tracing::warn!("ws auth handshake failed: {e}");
                    let _ = self
                        .inbound_tx
                        .unbounded_send(TransportInbound::ConnectFailed { reason: e });
                    self.tick.notify();
                    ws.close().await;
                }
            }
            let backoff_outcome = futures::select! {
                ctrl = self.control_rx.next().fuse() => ControlOrPhase::Control(ctrl),
                _ = self.reconnect.backoff().fuse() => ControlOrPhase::Phase(()),
            };
            match backoff_outcome {
                ControlOrPhase::Control(None)
                | ControlOrPhase::Control(Some(TransportControl::Shutdown)) => return,
                ControlOrPhase::Control(Some(TransportControl::UpdateAuth(auth))) => {
                    self.auth = auth;
                    self.reconnect.reset();
                    continue;
                }
                ControlOrPhase::Phase(()) => {}
            }
        }
    }

    async fn wasm_run_connected(&mut self, ws: &mut W) -> WasmConnectedExit {
        use futures::{FutureExt as _, StreamExt as _};
        loop {
            futures::select! {
                out = self.outbox_rx.next().fuse() => {
                    // outbox closed = handle dropped; control_rx will also return None shortly.
                    // Route to Shutdown so the same clean-exit path is taken.
                    let Some(entry) = out else { return WasmConnectedExit::Shutdown; };
                    let Ok(bytes) = serde_json::to_vec(&entry) else { continue; };
                    let frame = frame_encode(&bytes);
                    if ws.send(&frame).await.is_err() { return WasmConnectedExit::NetworkError; }
                }
                incoming = ws.recv().fuse() => {
                    match incoming {
                        Ok(Some(data)) => {
                            let Some(payload) = frame_decode(&data) else { continue; };
                            let Ok(event) = serde_json::from_slice::<crate::transport_protocol::ServerEvent>(payload) else { continue; };
                            match event {
                                crate::transport_protocol::ServerEvent::SyncUpdate { seq, payload } => {
                                    let entry = InboxEntry {
                                        source: crate::sync_manager::types::Source::Server(self.server_id),
                                        payload: *payload,
                                    };
                                    let _ = self.inbound_tx.unbounded_send(TransportInbound::Sync {
                                        entry: Box::new(entry),
                                        sequence: seq,
                                    });
                                    self.tick.notify();
                                }
                                crate::transport_protocol::ServerEvent::Heartbeat => {}
                                crate::transport_protocol::ServerEvent::Connected { .. } => {
                                    tracing::warn!("unexpected Connected frame mid-stream; ignoring");
                                }
                                crate::transport_protocol::ServerEvent::Error { message, code } => {
                                    tracing::warn!(message, ?code, "server reported error");
                                }
                                other => {
                                    tracing::debug!(variant = other.variant_name(), "received non-sync ServerEvent; skipping");
                                }
                            }
                        }
                        Ok(None) | Err(_) => return WasmConnectedExit::NetworkError,
                    }
                }
                ctrl = self.control_rx.next().fuse() => {
                    match ctrl {
                        None | Some(TransportControl::Shutdown) => return WasmConnectedExit::Shutdown,
                        Some(TransportControl::UpdateAuth(auth)) => return WasmConnectedExit::UpdateAuth(auth),
                    }
                }
            }
        }
    }
}

#[cfg(feature = "runtime-tokio")]
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct MockStream {
        sent: Vec<Vec<u8>>,
        inbound: VecDeque<Vec<u8>>,
    }
    impl StreamAdapter for MockStream {
        type Error = &'static str;
        async fn connect(_url: &str) -> Result<Self, Self::Error> {
            // Pre-load a valid ConnectedResponse frame so the handshake succeeds.
            let resp = ConnectedResponse {
                connection_id: "conn-1".into(),
                client_id: "client-1".into(),
                next_sync_seq: Some(0),
                catalogue_state_hash: None,
            };
            let payload = serde_json::to_vec(&resp).unwrap();
            let frame = frame_encode(&payload);
            let mut inbound = VecDeque::new();
            inbound.push_back(frame);
            Ok(MockStream {
                sent: Vec::new(),
                inbound,
            })
        }
        async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
            self.sent.push(data.to_vec());
            Ok(())
        }
        async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
            Ok(self.inbound.pop_front())
        }
        async fn close(&mut self) {}
    }

    #[derive(Default)]
    struct TestStreamController {
        pub connect_pending: AtomicBool,
        pub recv_pending: AtomicBool,
        pub handshake_response: Mutex<Option<Vec<u8>>>,
        pub recv_queue: Mutex<VecDeque<Vec<u8>>>,
        pub close_calls: AtomicUsize,
        pub connect_calls: AtomicUsize,
        pub sent_frames: Mutex<Vec<Vec<u8>>>,
    }

    struct TestStreamAdapter {
        controller: Arc<TestStreamController>,
        handshake_delivered: bool,
    }

    thread_local! {
        static TEST_CONTROLLER: std::cell::RefCell<Option<Arc<TestStreamController>>> =
            const { std::cell::RefCell::new(None) };
    }

    fn install_controller(c: Arc<TestStreamController>) {
        TEST_CONTROLLER.with(|slot| *slot.borrow_mut() = Some(c));
    }

    fn take_controller() -> Arc<TestStreamController> {
        TEST_CONTROLLER
            .with(|slot| slot.borrow().clone())
            .expect("controller installed")
    }

    impl StreamAdapter for TestStreamAdapter {
        type Error = &'static str;

        async fn connect(_url: &str) -> Result<Self, Self::Error> {
            let controller = take_controller();
            controller.connect_calls.fetch_add(1, Ordering::SeqCst);
            if controller.connect_pending.load(Ordering::SeqCst) {
                futures::future::pending::<()>().await;
                unreachable!();
            }
            Ok(Self {
                controller,
                handshake_delivered: false,
            })
        }

        async fn send(&mut self, data: &[u8]) -> Result<(), Self::Error> {
            self.controller
                .sent_frames
                .lock()
                .unwrap()
                .push(data.to_vec());
            Ok(())
        }

        async fn recv(&mut self) -> Result<Option<Vec<u8>>, Self::Error> {
            if !self.handshake_delivered
                && let Some(frame) = self.controller.handshake_response.lock().unwrap().clone()
            {
                self.handshake_delivered = true;
                return Ok(Some(frame));
            }
            if let Some(frame) = self.controller.recv_queue.lock().unwrap().pop_front() {
                return Ok(Some(frame));
            }
            if self.controller.recv_pending.load(Ordering::SeqCst) {
                futures::future::pending::<Option<Vec<u8>>>().await;
                unreachable!();
            }
            Ok(None)
        }

        async fn close(&mut self) {
            self.controller.close_calls.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn make_handshake_response_frame() -> Vec<u8> {
        let resp = ConnectedResponse {
            connection_id: "conn-1".into(),
            client_id: "client-1".into(),
            next_sync_seq: Some(0),
            catalogue_state_hash: None,
        };
        let payload = serde_json::to_vec(&resp).unwrap();
        frame_encode(&payload)
    }

    #[derive(Clone)]
    struct CountingTick(Arc<AtomicUsize>);
    impl TickNotifier for CountingTick {
        fn notify(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn shutdown_during_connect() {
        let controller = Arc::new(TestStreamController::default());
        controller.connect_pending.store(true, Ordering::SeqCst);
        install_controller(controller.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
            "mock://".to_string(),
            AuthConfig::default(),
            CountingTick(counter.clone()),
        );
        let task = tokio::spawn(manager.run());

        // Let connect() start.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(controller.connect_calls.load(Ordering::SeqCst) >= 1);

        handle.disconnect();

        tokio::time::timeout(std::time::Duration::from_millis(200), task)
            .await
            .expect("manager should exit promptly after Shutdown during connect")
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_during_backoff() {
        let controller = Arc::new(TestStreamController::default());
        // handshake_response is None by default → handshake fails with
        // "server closed before handshake response", which routes into backoff.
        install_controller(controller.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
            "mock://".to_string(),
            AuthConfig::default(),
            CountingTick(counter.clone()),
        );
        let task = tokio::spawn(manager.run());

        // Wait for at least one failed connect/handshake cycle to enter backoff.
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        handle.disconnect();

        tokio::time::timeout(std::time::Duration::from_millis(200), task)
            .await
            .expect("manager should exit during backoff on Shutdown")
            .unwrap();
    }

    #[tokio::test]
    async fn shutdown_during_handshake() {
        let controller = Arc::new(TestStreamController::default());
        // connect succeeds; handshake pends (recv returns Pending forever until controller flips).
        controller.recv_pending.store(true, Ordering::SeqCst);
        // Do NOT pre-stage a handshake response — recv will see the queue empty and pend.
        install_controller(controller.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
            "mock://".to_string(),
            AuthConfig::default(),
            CountingTick(counter.clone()),
        );
        let task = tokio::spawn(manager.run());

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        handle.disconnect();

        tokio::time::timeout(std::time::Duration::from_millis(200), task)
            .await
            .expect("manager should exit during handshake on Shutdown")
            .unwrap();
        assert!(controller.close_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn shutdown_during_connected() {
        let controller = Arc::new(TestStreamController::default());
        *controller.handshake_response.lock().unwrap() = Some(make_handshake_response_frame());
        controller.recv_pending.store(true, Ordering::SeqCst);
        install_controller(controller.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let (mut handle, manager) = create::<TestStreamAdapter, CountingTick>(
            "mock://".to_string(),
            AuthConfig::default(),
            CountingTick(counter.clone()),
        );
        let task = tokio::spawn(manager.run());

        // Wait for Connected.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(handle.has_ever_connected());

        handle.disconnect();

        tokio::time::timeout(std::time::Duration::from_millis(200), task)
            .await
            .expect("manager should exit promptly after Shutdown while connected")
            .unwrap();
        // Shutdown does NOT emit Disconnected.
        let mut saw_disconnected = false;
        while let Some(msg) = handle.try_recv_inbound() {
            if matches!(msg, TransportInbound::Disconnected) {
                saw_disconnected = true;
            }
        }
        assert!(!saw_disconnected, "Shutdown must not emit Disconnected");
        assert!(controller.close_calls.load(Ordering::SeqCst) >= 1);
    }

    #[tokio::test]
    async fn handshake_marks_ever_connected_and_notifies_tick() {
        let counter = Arc::new(AtomicUsize::new(0));
        let tick = CountingTick(counter.clone());
        let (handle, manager) =
            create::<MockStream, CountingTick>("mock://".to_string(), AuthConfig::default(), tick);
        let task = tokio::spawn(manager.run());

        // Give the manager time to run the handshake.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        assert!(
            handle.has_ever_connected(),
            "handshake should have set ever_connected"
        );
        assert!(
            counter.load(Ordering::SeqCst) >= 1,
            "tick should have been notified"
        );

        drop(handle);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), task).await;
    }

    #[tokio::test]
    async fn update_auth_during_backoff() {
        let controller = Arc::new(TestStreamController::default());
        // No handshake response → handshake fails → enter backoff.
        install_controller(controller.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let initial_auth = AuthConfig::default();
        let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
            "mock://".to_string(),
            initial_auth,
            CountingTick(counter.clone()),
        );
        let task = tokio::spawn(manager.run());

        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        let calls_before = controller.connect_calls.load(Ordering::SeqCst);

        let new_auth = AuthConfig {
            jwt_token: Some("refreshed".into()),
            ..Default::default()
        };
        handle.update_auth(new_auth);

        // Manager should skip the remaining backoff and reconnect ~immediately.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let calls_after = controller.connect_calls.load(Ordering::SeqCst);
        assert!(
            calls_after > calls_before,
            "UpdateAuth during backoff should trigger immediate reconnect (before={calls_before}, after={calls_after})"
        );

        let frames = controller.sent_frames.lock().unwrap().clone();
        let latest_handshake = frames
            .iter()
            .rev()
            .find_map(|f| {
                let payload = frame_decode(f)?;
                serde_json::from_slice::<AuthHandshake>(payload).ok()
            })
            .expect("at least one AuthHandshake frame sent after update_auth");
        assert_eq!(
            latest_handshake.auth.jwt_token.as_deref(),
            Some("refreshed")
        );

        handle.disconnect();
        let _ = tokio::time::timeout(std::time::Duration::from_millis(200), task).await;
    }

    #[tokio::test]
    async fn update_auth_during_connected() {
        let controller = Arc::new(TestStreamController::default());
        *controller.handshake_response.lock().unwrap() = Some(make_handshake_response_frame());
        controller.recv_pending.store(true, Ordering::SeqCst);
        install_controller(controller.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let (mut handle, manager) = create::<TestStreamAdapter, CountingTick>(
            "mock://".to_string(),
            AuthConfig::default(),
            CountingTick(counter.clone()),
        );
        let task = tokio::spawn(manager.run());

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(handle.has_ever_connected());
        let initial_close_calls = controller.close_calls.load(Ordering::SeqCst);

        let new_auth = AuthConfig {
            jwt_token: Some("refreshed".into()),
            ..Default::default()
        };
        handle.update_auth(new_auth);

        // Expect: Disconnected emitted; stream closed; reconnect reaches Connected again.
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        let mut events = Vec::new();
        while let Some(msg) = handle.try_recv_inbound() {
            events.push(msg);
        }
        let has_disconnected = events
            .iter()
            .any(|e| matches!(e, TransportInbound::Disconnected));
        let connected_count = events
            .iter()
            .filter(|e| matches!(e, TransportInbound::Connected { .. }))
            .count();
        assert!(
            has_disconnected,
            "UpdateAuth while connected must emit Disconnected"
        );
        assert!(
            connected_count >= 1,
            "Expected at least one fresh Connected after auth refresh"
        );
        assert!(controller.close_calls.load(Ordering::SeqCst) > initial_close_calls);

        // Verify the latest handshake frame carried the refreshed JWT.
        let frames = controller.sent_frames.lock().unwrap().clone();
        let latest_handshake = frames
            .iter()
            .rev()
            .find_map(|f| {
                let payload = frame_decode(f)?;
                serde_json::from_slice::<AuthHandshake>(payload).ok()
            })
            .expect("at least one AuthHandshake frame");
        assert_eq!(
            latest_handshake.auth.jwt_token.as_deref(),
            Some("refreshed")
        );

        handle.disconnect();
        let _ = tokio::time::timeout(std::time::Duration::from_millis(200), task).await;
    }

    #[tokio::test]
    async fn handle_dropped_is_shutdown() {
        let controller = Arc::new(TestStreamController::default());
        controller.connect_pending.store(true, Ordering::SeqCst);
        install_controller(controller.clone());

        let counter = Arc::new(AtomicUsize::new(0));
        let (handle, manager) = create::<TestStreamAdapter, CountingTick>(
            "mock://".to_string(),
            AuthConfig::default(),
            CountingTick(counter.clone()),
        );
        let task = tokio::spawn(manager.run());

        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        drop(handle);

        tokio::time::timeout(std::time::Duration::from_millis(200), task)
            .await
            .expect("dropping the handle should shut down the manager")
            .unwrap();
    }
}
