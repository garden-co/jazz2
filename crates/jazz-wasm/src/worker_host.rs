//! WorkerHost — in-worker Rust entry point.
//!
//! Replaces the dispatch logic of
//! `packages/jazz-tools/src/worker/jazz-worker.ts`.
//!
//! Sub-task coverage:
//! - 6a: Pure helper functions + unit tests.
//! - 6b (this file): `run_worker` entry, `WorkerHost` state, `handle_init`,
//!   outbox drainer.
//! - 6c (TODO): Peer client registry + sync routing.
//! - 6d (TODO): Lifecycle / auth / upstream / debug / shutdown dispatch.

use jazz_tools::worker_frame::AuthFailureReason;
use std::collections::HashMap;

/// Build the WebSocket URL for the Rust transport from the init-payload fields.
///
/// Mirrors `httpUrlToWs` in `packages/jazz-tools/src/runtime/url.ts`:
/// - `http://host`   → `ws://host/ws`
/// - `https://host`  → `wss://host/ws`
/// - optional prefix is inserted before `/ws`
/// - `ws://`/`wss://` URLs are passed through; `/ws` appended if absent
/// - prefix with already-`/ws`-suffixed ws URL: strip existing `/ws`, append prefix + `/ws`
pub fn compose_connect_url(server_url: &str, path_prefix: Option<&str>) -> Result<String, String> {
    let base = server_url.trim_end_matches('/');
    let prefix = path_prefix
        .unwrap_or("")
        .trim_start_matches('/')
        .trim_end_matches('/');
    let tail = if prefix.is_empty() {
        "/ws".to_string()
    } else {
        format!("/{prefix}/ws")
    };

    if let Some(rest) = base.strip_prefix("http://") {
        return Ok(format!("ws://{rest}{tail}"));
    }
    if let Some(rest) = base.strip_prefix("https://") {
        return Ok(format!("wss://{rest}{tail}"));
    }
    if base.starts_with("ws://") || base.starts_with("wss://") {
        if !prefix.is_empty() {
            let trimmed = base.strip_suffix("/ws").unwrap_or(base);
            return Ok(format!("{trimmed}{tail}"));
        }
        return Ok(if base.ends_with("/ws") {
            base.to_string()
        } else {
            format!("{base}/ws")
        });
    }
    Err(format!(
        "Invalid server URL \"{server_url}\": expected http://, https://, ws://, or wss://"
    ))
}

/// Merge an incoming JWT into the worker-held auth map.
///
/// - `Some(non_empty)` replaces/sets the `jwt_token` entry.
/// - `Some("")` or `None` removes `jwt_token`.
/// - All other keys (e.g. `admin_secret`) are preserved.
///
/// Mirrors `mergeAuth` in `jazz-worker.ts`.
pub fn merge_auth(current: &mut HashMap<String, String>, incoming_jwt: Option<&str>) {
    match incoming_jwt {
        Some(j) if !j.is_empty() => {
            current.insert("jwt_token".into(), j.to_string());
        }
        _ => {
            current.remove("jwt_token");
        }
    }
}

/// Map a Rust auth-failure reason string to a typed `AuthFailureReason`.
///
/// Mirrors `mapAuthReason` in `packages/jazz-tools/src/runtime/auth-state.ts`.
/// The Rust transport sends the server's error message verbatim; we look for
/// well-known sub-strings and fall back to `Invalid` (== `invalid`).
pub fn map_auth_reason(reason: &str) -> AuthFailureReason {
    let lower = reason.to_lowercase();
    if lower.contains("expired") {
        return AuthFailureReason::Expired;
    }
    if lower.contains("missing") {
        // The TS side has `"missing"`; our enum uses `Unauthorized` for that case.
        // If a richer mapping is needed later, extend AuthFailureReason.
        return AuthFailureReason::Unauthorized;
    }
    if lower.contains("disabled") {
        // Same note as `missing` — treat as Unauthorized until the enum grows.
        return AuthFailureReason::Unauthorized;
    }
    AuthFailureReason::Invalid
}

// ============================================================================
// WorkerHost (wasm32 only)
// ============================================================================

#[cfg(target_arch = "wasm32")]
mod wasm_worker {
    use super::{compose_connect_url, map_auth_reason, merge_auth};
    use futures::StreamExt as _;
    use jazz_tools::runtime_core::ClientOutboxHandle;
    use jazz_tools::sync_manager::{ClientId, Destination, InboxEntry, Source, SyncPayload};
    use jazz_tools::worker_frame::{encode, InitPayload, WorkerFrame};
    use std::cell::{Cell, RefCell};
    use std::collections::HashMap;
    use std::rc::Rc;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::spawn_local;

    use crate::post_message_stream::PostMessageStream;
    use crate::runtime::WasmRuntime;

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// Post a `WorkerFrame` on `stream` (encodes to bincode bytes).
    async fn post_frame(
        stream: &Rc<RefCell<PostMessageStream<web_sys::DedicatedWorkerGlobalScope>>>,
        frame: &WorkerFrame,
    ) {
        use jazz_tools::transport_manager::StreamAdapter;
        let bytes = encode(frame);
        let _ = stream.borrow_mut().send(bytes).await;
    }

    // -------------------------------------------------------------------------
    // WorkerHost
    // -------------------------------------------------------------------------

    /// All host-side state that persists between inbound frames.
    ///
    /// Private to this module.  Not a `#[wasm_bindgen]` type.
    struct WorkerHost {
        /// The sending half of the worker ↔ main-thread PostMessage channel.
        /// Cloned into the drainer task so it stays alive.
        stream_send: Rc<RefCell<PostMessageStream<web_sys::DedicatedWorkerGlobalScope>>>,
        /// Runtime, present after a successful `Init`.
        runtime: Option<Rc<RefCell<WasmRuntime>>>,
        /// Runtime client-id for the main thread peer.
        main_client_id: Option<String>,
        /// Current auth map (`jwt_token`, `admin_secret`, …).
        current_auth: HashMap<String, String>,
        /// WebSocket URL stored after init so reconnect-upstream can reuse it.
        current_ws_url: Option<String>,
        /// Sync frames buffered before `Init` completes.
        pending_sync_messages: Vec<Vec<u8>>,
        /// PeerSync frames buffered before `Init` completes.
        /// Each entry is `(peer_id, term, payloads)`.
        pending_peer_sync_messages: Vec<(String, u32, Vec<Vec<u8>>)>,
        /// Whether init has fully completed.
        init_complete: bool,
        /// Maps peer_id → runtime ClientId string.
        peer_runtime_client_by_peer_id: HashMap<String, String>,
        /// Maps runtime ClientId string → peer_id.
        peer_id_by_runtime_client: HashMap<String, String>,
        /// Last known term for each peer_id.
        peer_term_by_peer_id: HashMap<String, u32>,
        /// Set to true when the worker should exit the dispatch loop.
        should_exit: bool,
    }

    impl WorkerHost {
        fn new(
            stream_send: Rc<RefCell<PostMessageStream<web_sys::DedicatedWorkerGlobalScope>>>,
        ) -> Self {
            Self {
                stream_send,
                runtime: None,
                main_client_id: None,
                current_auth: HashMap::new(),
                current_ws_url: None,
                pending_sync_messages: Vec::new(),
                pending_peer_sync_messages: Vec::new(),
                init_complete: false,
                peer_runtime_client_by_peer_id: HashMap::new(),
                peer_id_by_runtime_client: HashMap::new(),
                peer_term_by_peer_id: HashMap::new(),
                should_exit: false,
            }
        }

        // ------------------------------------------------------------------
        // ensure_peer_client — port of TS ensurePeerClient (lines 408-418)
        // ------------------------------------------------------------------

        /// Ensures a runtime client exists for the given peer_id, creating one
        /// if necessary. Returns the runtime ClientId string, or None if there
        /// is no runtime yet.
        fn ensure_peer_client(&mut self, peer_id: &str) -> Option<String> {
            if self.runtime.is_none() {
                return None;
            }
            if let Some(existing) = self.peer_runtime_client_by_peer_id.get(peer_id) {
                return Some(existing.clone());
            }
            let runtime = self.runtime.as_ref().unwrap();
            let client_id = runtime.borrow().add_client();
            if let Err(e) = runtime.borrow().set_client_role(&client_id, "peer") {
                web_sys::console::warn_1(
                    &format!("[worker] ensure_peer_client: set_client_role failed: {e:?}").into(),
                );
                return None;
            }
            self.peer_runtime_client_by_peer_id
                .insert(peer_id.to_string(), client_id.clone());
            self.peer_id_by_runtime_client
                .insert(client_id.clone(), peer_id.to_string());
            Some(client_id)
        }

        // ------------------------------------------------------------------
        // close_peer — port of TS closePeer (lines 420-426)
        // ------------------------------------------------------------------

        /// Removes all peer bookkeeping for the given peer_id. No runtime-side
        /// call needed (the WASM runtime has no removeClient binding).
        fn close_peer(&mut self, peer_id: &str) {
            if let Some(runtime_client_id) = self.peer_runtime_client_by_peer_id.remove(peer_id) {
                self.peer_id_by_runtime_client.remove(&runtime_client_id);
                self.peer_term_by_peer_id.remove(peer_id);
            }
        }

        // ------------------------------------------------------------------
        // post — send a WorkerFrame to the main thread
        // ------------------------------------------------------------------

        async fn post(&self, frame: WorkerFrame) {
            use jazz_tools::transport_manager::StreamAdapter;
            let bytes = encode(&frame);
            let _ = self.stream_send.borrow_mut().send(bytes).await;
        }

        // ------------------------------------------------------------------
        // flush_wal_best_effort — flush WAL on lifecycle hint
        // ------------------------------------------------------------------

        fn flush_wal_best_effort(&self) {
            if !self.init_complete {
                return;
            }
            if let Some(runtime_rc) = self.runtime.as_ref() {
                runtime_rc.borrow().flush_wal();
            }
        }

        // ------------------------------------------------------------------
        // handle_init
        // ------------------------------------------------------------------

        async fn handle_init(&mut self, payload: InitPayload) {
            // (a) Open the persistent OPFS-backed runtime.
            let runtime_result = WasmRuntime::open_persistent(
                &payload.schema_json,
                &payload.app_id,
                &payload.env,
                &payload.user_branch,
                &payload.db_name,
                Some("local".to_string()),
                false,
            )
            .await;

            let runtime = match runtime_result {
                Ok(r) => r,
                Err(e) => {
                    post_frame(
                        &self.stream_send,
                        &WorkerFrame::Error {
                            msg: format!("Init failed: {e:?}"),
                        },
                    )
                    .await;
                    return;
                }
            };

            // (b) Reset host state.
            self.init_complete = false;
            self.peer_runtime_client_by_peer_id.clear();
            self.peer_id_by_runtime_client.clear();
            self.peer_term_by_peer_id.clear();
            self.current_auth.clear();
            self.current_ws_url = None;

            let runtime_rc = Rc::new(RefCell::new(runtime));
            self.runtime = Some(runtime_rc.clone());

            // (c) Register main thread as a Peer client.
            let main_client_id = runtime_rc.borrow().add_client();
            if let Err(e) = runtime_rc.borrow().set_client_role(&main_client_id, "peer") {
                post_frame(
                    &self.stream_send,
                    &WorkerFrame::Error {
                        msg: format!("Init failed: set_client_role: {e:?}"),
                    },
                )
                .await;
                return;
            }
            self.main_client_id = Some(main_client_id.clone());

            // (d) Install auth-failure callback.
            //
            // The RuntimeCore callback must be `Send + 'static`, but PostMessageStream
            // holds `Rc` and is !Send. We route through a futures::channel to bridge
            // the gap: the callback sends a unit signal over an mpsc channel, and a
            // spawn_local task reads it and posts the frames.
            {
                let (auth_fail_tx, mut auth_fail_rx) =
                    futures::channel::mpsc::unbounded::<String>();

                // SAFETY: WASM is single-threaded; no concurrent access is possible.
                // This is the same pattern used by WasmRuntime::on_auth_failure.
                struct SendTx(futures::channel::mpsc::UnboundedSender<String>);
                unsafe impl Send for SendTx {}
                let send_tx = SendTx(auth_fail_tx);

                runtime_rc
                    .borrow()
                    .inner_core()
                    .borrow_mut()
                    .set_auth_failure_callback(move |reason| {
                        let _ = send_tx.0.unbounded_send(reason);
                    });

                let stream_for_auth = self.stream_send.clone();
                spawn_local(async move {
                    while let Some(reason) = auth_fail_rx.next().await {
                        post_frame(&stream_for_auth, &WorkerFrame::UpstreamDisconnected).await;
                        post_frame(
                            &stream_for_auth,
                            &WorkerFrame::AuthFailed {
                                reason: map_auth_reason(&reason),
                            },
                        )
                        .await;
                    }
                });
            }

            // (e) Install ClientOutboxHandle — replacement for JsSyncSender on this path.
            let (outbox_tx, outbox_rx) =
                futures::channel::mpsc::unbounded::<jazz_tools::sync_manager::OutboxEntry>();
            runtime_rc
                .borrow()
                .inner_core()
                .borrow_mut()
                .set_client_outbox(ClientOutboxHandle { tx: outbox_tx });

            // (f) Spawn the outbox drainer.
            {
                let stream_for_drain = self.stream_send.clone();
                let main_cid = main_client_id.clone();

                // Shared flag: true only during bootstrap-catalogue forwarding block.
                let bootstrap_flag = Rc::new(Cell::new(false));
                let bootstrap_flag_drainer = bootstrap_flag.clone();

                // Maps for peer routing — snapshotted as Rc<RefCell<...>> so the
                // drainer closure can be moved into spawn_local. 6c will feed real
                // data into these maps; for now they're always empty.
                let peer_id_by_runtime_client: Rc<RefCell<HashMap<String, String>>> =
                    Rc::new(RefCell::new(HashMap::new()));
                let peer_term_by_peer_id: Rc<RefCell<HashMap<String, u32>>> =
                    Rc::new(RefCell::new(HashMap::new()));

                // Share the peer maps with WorkerHost so 6c can populate them.
                // (For 6b they are always empty.)
                // Stash the Rcs so we can replace the plain HashMap fields in 6c.
                // For now we just use local variables.

                let mut outbox_rx = outbox_rx;

                spawn_local(async move {
                    use jazz_tools::transport_manager::StreamAdapter;

                    loop {
                        // Block until at least one entry is available.
                        let first = match outbox_rx.next().await {
                            Some(e) => e,
                            None => break, // channel closed
                        };

                        // Translate entry → WorkerFrame.
                        let mut frames: Vec<Vec<u8>> = Vec::new();
                        let mut translate_entry = |entry: jazz_tools::sync_manager::OutboxEntry| {
                            let bytes = match entry.payload.to_bytes() {
                                Ok(b) => b,
                                Err(_) => return,
                            };
                            match &entry.destination {
                                Destination::Client(cid) => {
                                    let cid_str = cid.0.to_string();
                                    if cid_str == main_cid {
                                        // Main-thread client.
                                        let frame = WorkerFrame::Sync { bytes };
                                        frames.push(encode(&frame));
                                    } else {
                                        // Peer client.
                                        let peer_map = peer_id_by_runtime_client.borrow();
                                        let Some(peer_id) = peer_map.get(&cid_str) else {
                                            return;
                                        };
                                        let term = peer_term_by_peer_id
                                            .borrow()
                                            .get(peer_id)
                                            .copied()
                                            .unwrap_or(0);
                                        let frame = WorkerFrame::PeerSync {
                                            peer_id: peer_id.clone(),
                                            term,
                                            bytes,
                                        };
                                        frames.push(encode(&frame));
                                    }
                                }
                                Destination::Server(_) => {
                                    // Server-bound payloads go through the Rust WebSocket
                                    // transport when connect() is active.  During the
                                    // bootstrap-catalogue forwarding window, catalogue-only
                                    // payloads are forwarded to the main thread so the main
                                    // runtime gets schema objects from the worker's OPFS store.
                                    if bootstrap_flag_drainer.get() && entry.payload.is_catalogue()
                                    {
                                        let frame = WorkerFrame::Sync { bytes };
                                        frames.push(encode(&frame));
                                    }
                                    // Otherwise drop silently.
                                }
                            }
                        };

                        translate_entry(first);

                        // Drain synchronously without blocking — mirrors TS microtask batching.
                        loop {
                            match outbox_rx.try_recv() {
                                Ok(entry) => translate_entry(entry),
                                Err(_) => break,
                            }
                        }

                        if !frames.is_empty() {
                            let _ = stream_for_drain.borrow_mut().send_batch(frames).await;
                        }
                    }
                });

                // Store the bootstrap flag on `self` so handle_init step (i) can
                // toggle it.  We smuggle it out via a local variable; drop the
                // `Rc` clones held here and re-derive them inside the block below.
                // (The drainer already holds its own clone of bootstrap_flag_drainer.)
                //
                // We need to hold `bootstrap_flag` alive for the block below, so
                // keep it in a local binding rather than moving into the drainer.
                //
                // The Rc here is the same one the drainer holds as
                // `bootstrap_flag_drainer`; they both point to the same Cell.

                // (g) Drain pre-init buffers.
                let buffered_sync = std::mem::take(&mut self.pending_sync_messages);
                self.init_complete = true; // mark before draining so re-entrant frames see correct state

                for raw in &buffered_sync {
                    match SyncPayload::from_bytes(raw) {
                        Ok(sync_payload) => {
                            let uuid = match uuid::Uuid::parse_str(&main_client_id) {
                                Ok(u) => u,
                                Err(_) => continue,
                            };
                            let entry = InboxEntry {
                                source: Source::Client(ClientId(uuid)),
                                payload: sync_payload,
                            };
                            runtime_rc
                                .borrow()
                                .inner_core()
                                .borrow_mut()
                                .park_sync_message(entry);
                        }
                        Err(_) => {
                            // Malformed pre-init sync frame — skip.
                        }
                    }
                }

                // Drain buffered PeerSync messages (accumulated before Init completed).
                let buffered_peer_sync = std::mem::take(&mut self.pending_peer_sync_messages);
                for (peer_id, term, payloads) in buffered_peer_sync {
                    let Some(peer_client_id) = self.ensure_peer_client(&peer_id) else {
                        continue;
                    };
                    self.peer_term_by_peer_id.insert(peer_id, term);
                    let uuid = match uuid::Uuid::parse_str(&peer_client_id) {
                        Ok(u) => u,
                        Err(_) => continue,
                    };
                    for raw in &payloads {
                        match SyncPayload::from_bytes(raw) {
                            Ok(sync_payload) => {
                                let entry = InboxEntry {
                                    source: Source::Client(ClientId(uuid)),
                                    payload: sync_payload,
                                };
                                runtime_rc
                                    .borrow()
                                    .inner_core()
                                    .borrow_mut()
                                    .park_sync_message(entry);
                            }
                            Err(_) => {
                                // Malformed pre-init peer sync frame — skip.
                            }
                        }
                    }
                }

                // (h) Mark init complete (already set above before draining).

                // (i) Bootstrap-catalogue forwarding.
                bootstrap_flag.set(true);
                struct FlagGuard<'a>(&'a Rc<Cell<bool>>);
                impl Drop for FlagGuard<'_> {
                    fn drop(&mut self) {
                        self.0.set(false);
                    }
                }
                let _guard = FlagGuard(&bootstrap_flag);
                {
                    // addServer(None, None) — triggers full catalogue sync to the "server"
                    // edge (which the drainer intercepts because the flag is set).
                    if let Err(e) = runtime_rc.borrow().add_server(None, None) {
                        web_sys::console::warn_1(
                            &format!("[worker] bootstrap addServer failed: {e:?}").into(),
                        );
                    }
                    runtime_rc.borrow().remove_server();
                }
                // FlagGuard drops here → bootstrap_flag.set(false).
                drop(_guard);
            }

            // (j) Post InitOk.
            post_frame(
                &self.stream_send,
                &WorkerFrame::InitOk {
                    client_id: main_client_id.clone(),
                },
            )
            .await;

            // (k) Connect to upstream if server_url provided.
            if let Some(server_url) = payload.server_url {
                if let Some(secret) = payload.admin_secret {
                    self.current_auth.insert("admin_secret".into(), secret);
                }
                merge_auth(&mut self.current_auth, payload.jwt_token.as_deref());

                match compose_connect_url(&server_url, payload.server_path_prefix.as_deref()) {
                    Err(e) => {
                        post_frame(
                            &self.stream_send,
                            &WorkerFrame::Error {
                                msg: format!("Init failed: bad server URL: {e}"),
                            },
                        )
                        .await;
                        return;
                    }
                    Ok(ws_url) => {
                        self.current_ws_url = Some(ws_url.clone());
                        let auth_json =
                            serde_json::to_string(&self.current_auth).unwrap_or_default();
                        match runtime_rc.borrow().connect(ws_url, auth_json) {
                            Ok(()) => {
                                post_frame(&self.stream_send, &WorkerFrame::UpstreamConnected)
                                    .await;
                            }
                            Err(_) => {
                                post_frame(&self.stream_send, &WorkerFrame::UpstreamDisconnected)
                                    .await;
                            }
                        }
                    }
                }
            }
        }
    }

    // -------------------------------------------------------------------------
    // run_worker — WASM entry point
    // -------------------------------------------------------------------------

    /// Dedicated-worker entry point.
    ///
    /// Called by the worker's top-level JS glue (e.g. `jazz-wasm` WASM init).
    /// Grabs the `DedicatedWorkerGlobalScope`, builds a `PostMessageStream`,
    /// posts `Ready`, then dispatches frames until the channel closes.
    #[wasm_bindgen(js_name = runWorker)]
    pub async fn run_worker() {
        use jazz_tools::transport_manager::StreamAdapter;
        use wasm_bindgen::JsCast;

        let scope = match js_sys::global().dyn_into::<web_sys::DedicatedWorkerGlobalScope>() {
            Ok(s) => s,
            Err(_) => {
                web_sys::console::error_1(
                    &"[jazz-wasm] runWorker: not running inside a DedicatedWorker".into(),
                );
                return;
            }
        };

        let scope_rc = Rc::new(scope);
        let stream = Rc::new(RefCell::new(PostMessageStream::new(scope_rc)));

        // Post Ready immediately so the main thread knows the worker is alive.
        post_frame(&stream, &WorkerFrame::Ready).await;

        let mut host = WorkerHost::new(stream.clone());

        // Main dispatch loop.
        loop {
            let raw = match stream.borrow_mut().recv().await {
                Ok(Some(bytes)) => bytes,
                Ok(None) | Err(_) => break, // stream closed or error
            };

            let frame = match jazz_tools::worker_frame::decode(&raw) {
                Ok(f) => f,
                Err(e) => {
                    web_sys::console::warn_1(
                        &format!("[jazz-wasm] runWorker: bad frame: {e}").into(),
                    );
                    continue;
                }
            };

            match frame {
                WorkerFrame::Init(payload) => {
                    host.handle_init(payload).await;
                }
                WorkerFrame::Sync { bytes } => {
                    if host.init_complete {
                        if let (Some(runtime), Some(main_client_id)) =
                            (host.runtime.as_ref(), host.main_client_id.as_deref())
                        {
                            let uuid = match uuid::Uuid::parse_str(main_client_id) {
                                Ok(u) => u,
                                Err(_) => continue,
                            };
                            match SyncPayload::from_bytes(&bytes) {
                                Ok(sync_payload) => {
                                    let entry = InboxEntry {
                                        source: Source::Client(ClientId(uuid)),
                                        payload: sync_payload,
                                    };
                                    runtime
                                        .borrow()
                                        .inner_core()
                                        .borrow_mut()
                                        .park_sync_message(entry);
                                }
                                Err(_) => {
                                    web_sys::console::warn_1(
                                        &"[worker] Sync: malformed payload — skipped".into(),
                                    );
                                }
                            }
                        }
                    } else {
                        host.pending_sync_messages.push(bytes);
                    }
                }
                WorkerFrame::PeerSync {
                    peer_id,
                    term,
                    bytes,
                } => {
                    if !host.init_complete
                        || host.runtime.is_none()
                        || host.main_client_id.is_none()
                    {
                        // Buffer: look up or create the entry for this peer_id.
                        if let Some(entry) = host
                            .pending_peer_sync_messages
                            .iter_mut()
                            .find(|(pid, _, _)| pid == &peer_id)
                        {
                            entry.2.push(bytes);
                        } else {
                            host.pending_peer_sync_messages
                                .push((peer_id, term, vec![bytes]));
                        }
                        continue;
                    }
                    // Extract runtime Rc before mutably borrowing self via ensure_peer_client.
                    let runtime_rc = host.runtime.as_ref().unwrap().clone();
                    let Some(peer_client_id) = host.ensure_peer_client(&peer_id) else {
                        continue;
                    };
                    host.peer_term_by_peer_id.insert(peer_id, term);
                    let uuid = match uuid::Uuid::parse_str(&peer_client_id) {
                        Ok(u) => u,
                        Err(_) => continue,
                    };
                    match SyncPayload::from_bytes(&bytes) {
                        Ok(sync_payload) => {
                            let entry = InboxEntry {
                                source: Source::Client(ClientId(uuid)),
                                payload: sync_payload,
                            };
                            runtime_rc
                                .borrow()
                                .inner_core()
                                .borrow_mut()
                                .park_sync_message(entry);
                        }
                        Err(_) => {
                            web_sys::console::warn_1(
                                &"[worker] PeerSync: malformed payload — skipped".into(),
                            );
                        }
                    }
                }
                WorkerFrame::PeerOpen { peer_id } => {
                    if host.init_complete && host.runtime.is_some() {
                        let _ = host.ensure_peer_client(&peer_id);
                    }
                }
                WorkerFrame::PeerClose { peer_id } => {
                    host.close_peer(&peer_id);
                }
                WorkerFrame::LifecycleHint { event, .. } => {
                    match event {
                        jazz_tools::worker_frame::LifecycleEvent::VisibilityHidden
                        | jazz_tools::worker_frame::LifecycleEvent::Pagehide
                        | jazz_tools::worker_frame::LifecycleEvent::Freeze => {
                            host.flush_wal_best_effort();
                        }
                        jazz_tools::worker_frame::LifecycleEvent::VisibilityVisible
                        | jazz_tools::worker_frame::LifecycleEvent::Resume => {
                            // Rust-owned transport handles reconnect automatically; no-op.
                        }
                    }
                }
                WorkerFrame::UpdateAuth { jwt } => {
                    merge_auth(&mut host.current_auth, jwt.as_deref());
                    if let Some(runtime_rc) = host.runtime.as_ref() {
                        let auth_json =
                            serde_json::to_string(&host.current_auth).unwrap_or_default();
                        if let Err(e) = runtime_rc.borrow().update_auth(auth_json) {
                            web_sys::console::warn_1(
                                &format!("[worker] update_auth failed: {e:?}").into(),
                            );
                            host.post(WorkerFrame::AuthFailed {
                                reason: jazz_tools::worker_frame::AuthFailureReason::Invalid,
                            })
                            .await;
                        }
                    }
                }
                WorkerFrame::DisconnectUpstream => {
                    if let Some(runtime_rc) = host.runtime.as_ref() {
                        runtime_rc.borrow().disconnect();
                        host.post(WorkerFrame::UpstreamDisconnected).await;
                    }
                }
                WorkerFrame::ReconnectUpstream => {
                    if let (Some(runtime_rc), Some(ws_url)) =
                        (host.runtime.as_ref(), host.current_ws_url.clone())
                    {
                        let auth_json =
                            serde_json::to_string(&host.current_auth).unwrap_or_default();
                        match runtime_rc.borrow().connect(ws_url, auth_json) {
                            Ok(()) => host.post(WorkerFrame::UpstreamConnected).await,
                            Err(_) => host.post(WorkerFrame::UpstreamDisconnected).await,
                        }
                    }
                }
                WorkerFrame::Shutdown => {
                    host.init_complete = false;
                    host.runtime = None;
                    host.peer_runtime_client_by_peer_id.clear();
                    host.peer_id_by_runtime_client.clear();
                    host.peer_term_by_peer_id.clear();
                    host.pending_peer_sync_messages.clear();
                    host.post(WorkerFrame::ShutdownOk).await;
                    host.should_exit = true;
                }
                WorkerFrame::SimulateCrash => {
                    host.init_complete = false;
                    if let Some(runtime_rc) = host.runtime.as_ref() {
                        runtime_rc.borrow().flush_wal();
                    }
                    host.runtime = None;
                    host.peer_runtime_client_by_peer_id.clear();
                    host.peer_id_by_runtime_client.clear();
                    host.peer_term_by_peer_id.clear();
                    host.pending_peer_sync_messages.clear();
                    host.post(WorkerFrame::ShutdownOk).await;
                    host.should_exit = true;
                }
                WorkerFrame::DebugSchemaState => {
                    if !host.init_complete || host.runtime.is_none() {
                        host.post(WorkerFrame::Error {
                            msg: "debug-schema-state requested before worker init complete".into(),
                        })
                        .await;
                        continue;
                    }
                    let js_state = {
                        let runtime = host.runtime.as_ref().unwrap().borrow();
                        runtime.debug_schema_state()
                    };
                    match js_state {
                        Ok(js_val) => {
                            match serde_wasm_bindgen::from_value::<
                                jazz_tools::worker_frame::DebugSchemaState,
                            >(js_val)
                            {
                                Ok(state) => {
                                    host.post(WorkerFrame::DebugSchemaStateOk(state)).await
                                }
                                Err(e) => {
                                    host.post(WorkerFrame::Error {
                                        msg: format!("debug-schema-state serialize failed: {e}"),
                                    })
                                    .await
                                }
                            }
                        }
                        Err(e) => {
                            host.post(WorkerFrame::Error {
                                msg: format!("debug-schema-state failed: {e:?}"),
                            })
                            .await
                        }
                    }
                }
                WorkerFrame::DebugSeedLiveSchema { schema_json } => {
                    if !host.init_complete || host.runtime.is_none() {
                        host.post(WorkerFrame::Error {
                            msg: "debug-seed-live-schema requested before worker init complete"
                                .into(),
                        })
                        .await;
                        continue;
                    }
                    let result = {
                        let runtime = host.runtime.as_ref().unwrap().borrow();
                        runtime.debug_seed_live_schema(&schema_json)
                    };
                    match result {
                        Ok(()) => {
                            if let Some(runtime_rc) = host.runtime.as_ref() {
                                runtime_rc.borrow().flush_wal();
                            }
                            host.post(WorkerFrame::DebugSeedLiveSchemaOk).await;
                        }
                        Err(e) => {
                            host.post(WorkerFrame::Error {
                                msg: format!("debug-seed-live-schema failed: {e:?}"),
                            })
                            .await
                        }
                    }
                }
                // worker→main frames should never arrive inbound; ignore.
                _ => {}
            }

            if host.should_exit {
                break;
            }
        }
    }
} // mod wasm_worker

#[cfg(target_arch = "wasm32")]
pub use wasm_worker::run_worker;

// ============================================================================
// Tests (native, no browser required)
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_auth_sets_and_clears_jwt() {
        let mut m = HashMap::new();
        m.insert("admin_secret".into(), "s".into());
        merge_auth(&mut m, Some("tok"));
        assert_eq!(m.get("jwt_token"), Some(&"tok".to_string()));
        merge_auth(&mut m, None);
        assert!(m.get("jwt_token").is_none());
        assert_eq!(m.get("admin_secret"), Some(&"s".to_string()));
    }

    #[test]
    fn merge_auth_empty_string_clears_jwt() {
        let mut m = HashMap::new();
        m.insert("jwt_token".into(), "old".into());
        merge_auth(&mut m, Some(""));
        assert!(m.get("jwt_token").is_none());
    }

    #[test]
    fn compose_connect_url_http_to_ws_plain() {
        assert_eq!(
            compose_connect_url("http://host", None).unwrap(),
            "ws://host/ws"
        );
    }

    #[test]
    fn compose_connect_url_https_to_wss_plain() {
        assert_eq!(
            compose_connect_url("https://host", None).unwrap(),
            "wss://host/ws"
        );
    }

    #[test]
    fn compose_connect_url_http_with_path_prefix() {
        assert_eq!(
            compose_connect_url("http://host", Some("/apps/xyz")).unwrap(),
            "ws://host/apps/xyz/ws"
        );
    }

    #[test]
    fn compose_connect_url_https_trims_trailing_slash() {
        assert_eq!(
            compose_connect_url("https://host/", None).unwrap(),
            "wss://host/ws"
        );
    }

    #[test]
    fn compose_connect_url_ws_passthrough_idempotent_suffix() {
        assert_eq!(
            compose_connect_url("ws://host", None).unwrap(),
            "ws://host/ws"
        );
        assert_eq!(
            compose_connect_url("ws://host/ws", None).unwrap(),
            "ws://host/ws"
        );
    }

    #[test]
    fn compose_connect_url_ws_with_prefix_strips_existing_suffix() {
        assert_eq!(
            compose_connect_url("ws://host/ws", Some("apps/a")).unwrap(),
            "ws://host/apps/a/ws"
        );
        assert_eq!(
            compose_connect_url("wss://host", Some("/apps/a/")).unwrap(),
            "wss://host/apps/a/ws"
        );
    }

    #[test]
    fn compose_connect_url_invalid_scheme_errors() {
        assert!(compose_connect_url("file://host", None).is_err());
        assert!(compose_connect_url("host", None).is_err());
    }

    #[test]
    fn map_auth_reason_matches_known_substrings() {
        assert!(matches!(
            map_auth_reason("token expired"),
            AuthFailureReason::Expired
        ));
        assert!(matches!(
            map_auth_reason("missing credentials"),
            AuthFailureReason::Unauthorized
        ));
        assert!(matches!(
            map_auth_reason("account disabled"),
            AuthFailureReason::Unauthorized
        ));
        assert!(matches!(
            map_auth_reason("whatever else"),
            AuthFailureReason::Invalid
        ));
    }
}
