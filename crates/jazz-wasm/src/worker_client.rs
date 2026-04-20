//! WorkerClient — main-thread Rust façade that replaces the TS `WorkerBridge`.
//!
//! Owns a `PostMessageStream<web_sys::Worker>` and dispatches inbound frames
//! from the dedicated worker to callbacks / oneshot resolvers. Outbound frames
//! are produced by:
//!   1. Explicit method calls on `WorkerClient` (e.g. `send_sync`, `update_auth`).
//!   2. The outbox drainer installed via `install_on_runtime`, which converts
//!      `OutboxEntry` values emitted by the main-thread `WasmRuntime` into
//!      `WorkerFrame::Sync` frames and posts them to the worker.

#[cfg(target_arch = "wasm32")]
mod wasm_client {
    use futures::StreamExt as _;
    use jazz_tools::runtime_core::ClientOutboxHandle;
    use jazz_tools::sync_manager::{Destination, OutboxEntry};
    use jazz_tools::worker_frame::{
        decode, encode, AuthFailureReason, DebugSchemaState, InitPayload, LifecycleEvent,
        WorkerFrame,
    };
    use std::cell::RefCell;
    use std::rc::Rc;
    use wasm_bindgen::prelude::*;
    use wasm_bindgen_futures::spawn_local;

    use crate::post_message_stream::PostMessageStream;
    use crate::runtime::WasmRuntime;

    // -------------------------------------------------------------------------
    // WorkerClient
    // -------------------------------------------------------------------------

    /// Main-thread façade for the dedicated Jazz worker.
    ///
    /// Mirrors `WorkerBridge` from `packages/jazz-tools/src/runtime/worker-bridge.ts`.
    #[wasm_bindgen]
    pub struct WorkerClient {
        stream: Rc<RefCell<PostMessageStream<web_sys::Worker>>>,

        // Oneshot slots for request/response pairs.
        init_tx: Rc<RefCell<Option<futures::channel::oneshot::Sender<Result<String, String>>>>>,
        shutdown_tx: Rc<RefCell<Option<futures::channel::oneshot::Sender<()>>>>,
        debug_schema_tx: Rc<
            RefCell<Option<futures::channel::oneshot::Sender<Result<DebugSchemaState, String>>>>,
        >,
        debug_seed_tx: Rc<RefCell<Option<futures::channel::oneshot::Sender<Result<(), String>>>>>,

        // Callback slots (worker → main).
        on_ready: Rc<RefCell<Option<js_sys::Function>>>,
        on_sync: Rc<RefCell<Option<js_sys::Function>>>,
        on_peer_sync: Rc<RefCell<Option<js_sys::Function>>>,
        on_upstream_status: Rc<RefCell<Option<js_sys::Function>>>,
        on_auth_failed: Rc<RefCell<Option<js_sys::Function>>>,
        on_error: Rc<RefCell<Option<js_sys::Function>>>,

        // Optional JS callback for `Destination::Server` outbox entries.
        // When set (follower mode), server-bound payloads are handed to this
        // function instead of being forwarded to the worker.
        server_payload_forwarder: Rc<RefCell<Option<js_sys::Function>>>,
    }

    #[wasm_bindgen]
    impl WorkerClient {
        // ------------------------------------------------------------------
        // constructor
        // ------------------------------------------------------------------

        #[wasm_bindgen(constructor)]
        pub fn new(worker: web_sys::Worker) -> WorkerClient {
            let worker_rc = Rc::new(worker);
            let stream = Rc::new(RefCell::new(PostMessageStream::new(worker_rc)));

            let init_tx: Rc<
                RefCell<Option<futures::channel::oneshot::Sender<Result<String, String>>>>,
            > = Rc::new(RefCell::new(None));
            let shutdown_tx: Rc<RefCell<Option<futures::channel::oneshot::Sender<()>>>> =
                Rc::new(RefCell::new(None));
            let debug_schema_tx: Rc<
                RefCell<
                    Option<futures::channel::oneshot::Sender<Result<DebugSchemaState, String>>>,
                >,
            > = Rc::new(RefCell::new(None));
            let debug_seed_tx: Rc<
                RefCell<Option<futures::channel::oneshot::Sender<Result<(), String>>>>,
            > = Rc::new(RefCell::new(None));

            let on_ready: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
            let on_sync: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
            let on_peer_sync: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
            let on_upstream_status: Rc<RefCell<Option<js_sys::Function>>> =
                Rc::new(RefCell::new(None));
            let on_auth_failed: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));
            let on_error: Rc<RefCell<Option<js_sys::Function>>> = Rc::new(RefCell::new(None));

            let server_payload_forwarder: Rc<RefCell<Option<js_sys::Function>>> =
                Rc::new(RefCell::new(None));

            // Spawn the inbox dispatcher.
            spawn_inbox_dispatcher(
                stream.clone(),
                init_tx.clone(),
                shutdown_tx.clone(),
                debug_schema_tx.clone(),
                debug_seed_tx.clone(),
                on_ready.clone(),
                on_sync.clone(),
                on_peer_sync.clone(),
                on_upstream_status.clone(),
                on_auth_failed.clone(),
                on_error.clone(),
            );

            WorkerClient {
                stream,
                init_tx,
                shutdown_tx,
                debug_schema_tx,
                debug_seed_tx,
                on_ready,
                on_sync,
                on_peer_sync,
                on_upstream_status,
                on_auth_failed,
                on_error,
                server_payload_forwarder,
            }
        }

        // ------------------------------------------------------------------
        // init
        // ------------------------------------------------------------------

        /// Deserialise `payload` as `InitPayload`, encode and send to the worker,
        /// then await the `InitOk` (or `Error`) response.
        ///
        /// Returns the `client_id` string assigned by the worker.
        pub async fn init(&self, payload: JsValue) -> Result<JsValue, JsValue> {
            let init_payload: InitPayload = serde_wasm_bindgen::from_value(payload)
                .map_err(|e| JsValue::from_str(&format!("init: bad payload: {e}")))?;

            let (tx, rx) = futures::channel::oneshot::channel::<Result<String, String>>();
            *self.init_tx.borrow_mut() = Some(tx);

            send_frame(&self.stream, &WorkerFrame::Init(init_payload)).await?;

            match rx.await {
                Ok(Ok(client_id)) => Ok(JsValue::from_str(&client_id)),
                Ok(Err(msg)) => Err(JsValue::from_str(&msg)),
                Err(_) => Err(JsValue::from_str("init: channel cancelled")),
            }
        }

        // ------------------------------------------------------------------
        // shutdown
        // ------------------------------------------------------------------

        pub async fn shutdown(&self) -> Result<(), JsValue> {
            let (tx, rx) = futures::channel::oneshot::channel::<()>();
            *self.shutdown_tx.borrow_mut() = Some(tx);

            send_frame(&self.stream, &WorkerFrame::Shutdown).await?;

            match rx.await {
                Ok(()) => Ok(()),
                Err(_) => {
                    // Timeout / channel dropped — worker may have already closed.
                    Ok(())
                }
            }
        }

        // ------------------------------------------------------------------
        // send_sync / send_peer_sync / peer_open / peer_close
        // ------------------------------------------------------------------

        pub fn send_sync(&self, bytes: Vec<u8>) -> Result<(), JsValue> {
            let stream = self.stream.clone();
            spawn_local(async move {
                let _ = send_frame(&stream, &WorkerFrame::Sync { bytes }).await;
            });
            Ok(())
        }

        pub fn send_peer_sync(&self, peer_id: String, term: u32, bytes: Vec<u8>) {
            let stream = self.stream.clone();
            spawn_local(async move {
                let _ = send_frame(
                    &stream,
                    &WorkerFrame::PeerSync {
                        peer_id,
                        term,
                        bytes,
                    },
                )
                .await;
            });
        }

        pub fn peer_open(&self, peer_id: String) {
            let stream = self.stream.clone();
            spawn_local(async move {
                let _ = send_frame(&stream, &WorkerFrame::PeerOpen { peer_id }).await;
            });
        }

        pub fn peer_close(&self, peer_id: String) {
            let stream = self.stream.clone();
            spawn_local(async move {
                let _ = send_frame(&stream, &WorkerFrame::PeerClose { peer_id }).await;
            });
        }

        // ------------------------------------------------------------------
        // update_auth / disconnect / reconnect
        // ------------------------------------------------------------------

        pub fn update_auth(&self, jwt: Option<String>) {
            let stream = self.stream.clone();
            spawn_local(async move {
                let _ = send_frame(&stream, &WorkerFrame::UpdateAuth { jwt }).await;
            });
        }

        pub fn disconnect_upstream(&self) {
            let stream = self.stream.clone();
            spawn_local(async move {
                let _ = send_frame(&stream, &WorkerFrame::DisconnectUpstream).await;
            });
        }

        pub fn reconnect_upstream(&self) {
            let stream = self.stream.clone();
            spawn_local(async move {
                let _ = send_frame(&stream, &WorkerFrame::ReconnectUpstream).await;
            });
        }

        // ------------------------------------------------------------------
        // lifecycle_hint
        // ------------------------------------------------------------------

        /// Parse `event` string to a `LifecycleEvent` and forward to the worker.
        ///
        /// Known event strings (case-insensitive match):
        /// - `"visibilityhidden"` / `"visibility-hidden"`
        /// - `"visibilityvisible"` / `"visibility-visible"`
        /// - `"pagehide"`
        /// - `"freeze"`
        /// - `"resume"`
        pub fn lifecycle_hint(&self, event: String, sent_at_ms: f64) {
            let lifecycle = match event.to_lowercase().replace('-', "").as_str() {
                "visibilityhidden" => LifecycleEvent::VisibilityHidden,
                "visibilityvisible" => LifecycleEvent::VisibilityVisible,
                "pagehide" => LifecycleEvent::Pagehide,
                "freeze" => LifecycleEvent::Freeze,
                "resume" => LifecycleEvent::Resume,
                _ => {
                    web_sys::console::warn_1(
                        &format!("[WorkerClient] unknown lifecycle event: {event}").into(),
                    );
                    return;
                }
            };
            let stream = self.stream.clone();
            spawn_local(async move {
                let _ = send_frame(
                    &stream,
                    &WorkerFrame::LifecycleHint {
                        event: lifecycle,
                        sent_at_ms: sent_at_ms as u64,
                    },
                )
                .await;
            });
        }

        // ------------------------------------------------------------------
        // simulate_crash
        // ------------------------------------------------------------------

        pub fn simulate_crash(&self) {
            let stream = self.stream.clone();
            spawn_local(async move {
                let _ = send_frame(&stream, &WorkerFrame::SimulateCrash).await;
            });
        }

        // ------------------------------------------------------------------
        // debug_schema_state
        // ------------------------------------------------------------------

        pub async fn debug_schema_state(&self) -> Result<JsValue, JsValue> {
            let (tx, rx) = futures::channel::oneshot::channel::<Result<DebugSchemaState, String>>();
            *self.debug_schema_tx.borrow_mut() = Some(tx);

            send_frame(&self.stream, &WorkerFrame::DebugSchemaState).await?;

            match rx.await {
                Ok(Ok(state)) => serde_wasm_bindgen::to_value(&state).map_err(|e| {
                    JsValue::from_str(&format!("debug_schema_state: serialize failed: {e}"))
                }),
                Ok(Err(msg)) => Err(JsValue::from_str(&msg)),
                Err(_) => Err(JsValue::from_str("debug_schema_state: channel cancelled")),
            }
        }

        // ------------------------------------------------------------------
        // debug_seed_live_schema
        // ------------------------------------------------------------------

        pub async fn debug_seed_live_schema(&self, schema_json: String) -> Result<(), JsValue> {
            let (tx, rx) = futures::channel::oneshot::channel::<Result<(), String>>();
            *self.debug_seed_tx.borrow_mut() = Some(tx);

            send_frame(
                &self.stream,
                &WorkerFrame::DebugSeedLiveSchema { schema_json },
            )
            .await?;

            match rx.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(msg)) => Err(JsValue::from_str(&msg)),
                Err(_) => Err(JsValue::from_str(
                    "debug_seed_live_schema: channel cancelled",
                )),
            }
        }

        // ------------------------------------------------------------------
        // install_on_runtime — wire main-thread runtime outbox to worker
        // ------------------------------------------------------------------

        /// Install a `ClientOutboxHandle` on `runtime` and spawn a drainer that
        /// encodes each `OutboxEntry` and routes it based on destination:
        ///
        /// - `Destination::Client` → wrap as `WorkerFrame::Sync` and post to the worker.
        /// - `Destination::Server` → if `server_payload_forwarder` is set (follower mode),
        ///   call the JS forwarder; otherwise forward to the worker as a `WorkerFrame::Sync`
        ///   frame so the worker's Rust transport can send it upstream (leader mode).
        ///
        /// Call this once after constructing both `WorkerClient` and the main-thread
        /// `WasmRuntime`.
        #[wasm_bindgen(js_name = installOnRuntime)]
        pub fn install_on_runtime(&self, runtime: &WasmRuntime) {
            let (tx, mut rx) = futures::channel::mpsc::unbounded::<OutboxEntry>();
            runtime
                .inner_core()
                .borrow_mut()
                .set_client_outbox(ClientOutboxHandle { tx });

            let stream = self.stream.clone();
            let fw_slot = self.server_payload_forwarder.clone();
            spawn_local(async move {
                use jazz_tools::transport_manager::StreamAdapter;

                loop {
                    // Block until at least one entry is available.
                    let first = match rx.next().await {
                        Some(e) => e,
                        None => break, // channel closed
                    };

                    let mut frames: Vec<Vec<u8>> = Vec::new();

                    let mut translate = |entry: OutboxEntry| {
                        match entry.destination {
                            Destination::Client(_) => {
                                // All client-bound entries from the main-thread core are
                                // destined for the worker's main-thread client.  Wrap as Sync.
                                match entry.payload.to_bytes() {
                                    Ok(bytes) => {
                                        frames.push(encode(&WorkerFrame::Sync { bytes }));
                                    }
                                    Err(e) => {
                                        web_sys::console::warn_1(
                                            &format!(
                                                "[WorkerClient] outbox: payload encode failed: {e}"
                                            )
                                            .into(),
                                        );
                                    }
                                }
                            }
                            Destination::Server(_) => {
                                match entry.payload.to_bytes() {
                                    Ok(bytes) => {
                                        if let Some(fw) = fw_slot.borrow().as_ref() {
                                            // Follower mode: hand to JS forwarder.
                                            let u8arr = js_sys::Uint8Array::from(&bytes[..]);
                                            let _ = fw.call1(&JsValue::NULL, &u8arr.into());
                                        } else {
                                            // Leader mode: forward to worker as Sync frame so
                                            // the worker's Rust transport sends it upstream.
                                            frames.push(encode(&WorkerFrame::Sync { bytes }));
                                        }
                                    }
                                    Err(e) => {
                                        web_sys::console::warn_1(
                                            &format!(
                                                "[WorkerClient] outbox: server payload encode failed: {e}"
                                            )
                                            .into(),
                                        );
                                    }
                                }
                            }
                        }
                    };

                    translate(first);

                    // Drain remaining entries without blocking — mirrors TS microtask batching
                    // and the WorkerHost drainer pattern (worker_host.rs lines 381-453).
                    loop {
                        match rx.try_recv() {
                            Ok(entry) => translate(entry),
                            Err(_) => break,
                        }
                    }

                    if !frames.is_empty() {
                        let _ = stream.borrow_mut().send_batch(frames).await;
                    }
                }
            });
        }

        // ------------------------------------------------------------------
        // setServerPayloadForwarder
        // ------------------------------------------------------------------

        /// Set (or clear) the JS callback that receives `Destination::Server` outbox
        /// entries from the main-thread runtime.
        ///
        /// When set (follower / leader-election hand-off mode), server-bound payloads
        /// are passed to `cb` as a `Uint8Array` instead of being forwarded to the
        /// worker.  Pass `None` (JS `null` / `undefined`) to restore leader mode.
        #[wasm_bindgen(js_name = setServerPayloadForwarder)]
        pub fn set_server_payload_forwarder(&self, cb: Option<js_sys::Function>) {
            *self.server_payload_forwarder.borrow_mut() = cb;
        }

        // ------------------------------------------------------------------
        // Callback setters
        // ------------------------------------------------------------------

        pub fn set_on_ready(&self, cb: js_sys::Function) {
            *self.on_ready.borrow_mut() = Some(cb);
        }

        pub fn set_on_sync(&self, cb: js_sys::Function) {
            *self.on_sync.borrow_mut() = Some(cb);
        }

        pub fn set_on_peer_sync(&self, cb: js_sys::Function) {
            *self.on_peer_sync.borrow_mut() = Some(cb);
        }

        pub fn set_on_upstream_status(&self, cb: js_sys::Function) {
            *self.on_upstream_status.borrow_mut() = Some(cb);
        }

        pub fn set_on_auth_failed(&self, cb: js_sys::Function) {
            *self.on_auth_failed.borrow_mut() = Some(cb);
        }

        pub fn set_on_error(&self, cb: js_sys::Function) {
            *self.on_error.borrow_mut() = Some(cb);
        }
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// Encode and send a `WorkerFrame` over `stream`.
    async fn send_frame(
        stream: &Rc<RefCell<PostMessageStream<web_sys::Worker>>>,
        frame: &WorkerFrame,
    ) -> Result<(), JsValue> {
        use jazz_tools::transport_manager::StreamAdapter;
        let bytes = encode(frame);
        stream
            .borrow_mut()
            .send(bytes)
            .await
            .map_err(|e| JsValue::from_str(&format!("send_frame error: {e}")))
    }

    /// Spawn the inbox dispatcher task.
    ///
    /// Reads frames from `stream` forever and routes each to the appropriate
    /// oneshot resolver or JS callback.
    #[allow(clippy::too_many_arguments)]
    fn spawn_inbox_dispatcher(
        stream: Rc<RefCell<PostMessageStream<web_sys::Worker>>>,
        init_tx: Rc<RefCell<Option<futures::channel::oneshot::Sender<Result<String, String>>>>>,
        shutdown_tx: Rc<RefCell<Option<futures::channel::oneshot::Sender<()>>>>,
        debug_schema_tx: Rc<
            RefCell<Option<futures::channel::oneshot::Sender<Result<DebugSchemaState, String>>>>,
        >,
        debug_seed_tx: Rc<RefCell<Option<futures::channel::oneshot::Sender<Result<(), String>>>>>,
        on_ready: Rc<RefCell<Option<js_sys::Function>>>,
        on_sync: Rc<RefCell<Option<js_sys::Function>>>,
        on_peer_sync: Rc<RefCell<Option<js_sys::Function>>>,
        on_upstream_status: Rc<RefCell<Option<js_sys::Function>>>,
        on_auth_failed: Rc<RefCell<Option<js_sys::Function>>>,
        on_error: Rc<RefCell<Option<js_sys::Function>>>,
    ) {
        spawn_local(async move {
            use jazz_tools::transport_manager::StreamAdapter;

            loop {
                let raw = match stream.borrow_mut().recv().await {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) | Err(_) => break,
                };

                let frame = match decode(&raw) {
                    Ok(f) => f,
                    Err(e) => {
                        web_sys::console::warn_1(&format!("[WorkerClient] bad frame: {e}").into());
                        continue;
                    }
                };

                match frame {
                    WorkerFrame::Ready => {
                        if let Some(cb) = on_ready.borrow().as_ref() {
                            let _ = cb.call0(&JsValue::NULL);
                        }
                    }

                    WorkerFrame::InitOk { client_id } => {
                        if let Some(tx) = init_tx.borrow_mut().take() {
                            let _ = tx.send(Ok(client_id));
                        }
                    }

                    WorkerFrame::Sync { bytes } => {
                        if let Some(cb) = on_sync.borrow().as_ref() {
                            let arr = js_sys::Uint8Array::from(bytes.as_slice());
                            let _ = cb.call1(&JsValue::NULL, &arr);
                        }
                    }

                    WorkerFrame::PeerSync {
                        peer_id,
                        term,
                        bytes,
                    } => {
                        if let Some(cb) = on_peer_sync.borrow().as_ref() {
                            let arr = js_sys::Uint8Array::from(bytes.as_slice());
                            let _ = cb.call3(
                                &JsValue::NULL,
                                &JsValue::from_str(&peer_id),
                                &JsValue::from_f64(term as f64),
                                &arr,
                            );
                        }
                    }

                    WorkerFrame::UpstreamConnected => {
                        if let Some(cb) = on_upstream_status.borrow().as_ref() {
                            let _ = cb.call1(&JsValue::NULL, &JsValue::TRUE);
                        }
                    }

                    WorkerFrame::UpstreamDisconnected => {
                        if let Some(cb) = on_upstream_status.borrow().as_ref() {
                            let _ = cb.call1(&JsValue::NULL, &JsValue::FALSE);
                        }
                    }

                    WorkerFrame::AuthFailed { reason } => {
                        if let Some(cb) = on_auth_failed.borrow().as_ref() {
                            let reason_str = match reason {
                                AuthFailureReason::Unauthorized => "unauthorized",
                                AuthFailureReason::Expired => "expired",
                                AuthFailureReason::Invalid => "invalid",
                                AuthFailureReason::Unknown => "unknown",
                            };
                            let _ = cb.call1(&JsValue::NULL, &JsValue::from_str(reason_str));
                        }
                    }

                    WorkerFrame::ShutdownOk => {
                        if let Some(tx) = shutdown_tx.borrow_mut().take() {
                            let _ = tx.send(());
                        }
                    }

                    WorkerFrame::DebugSchemaStateOk(state) => {
                        if let Some(tx) = debug_schema_tx.borrow_mut().take() {
                            let _ = tx.send(Ok(state));
                        }
                    }

                    WorkerFrame::DebugSeedLiveSchemaOk => {
                        if let Some(tx) = debug_seed_tx.borrow_mut().take() {
                            let _ = tx.send(Ok(()));
                        }
                    }

                    WorkerFrame::Error { msg } => {
                        // Check for pending init oneshot first.
                        if let Some(tx) = init_tx.borrow_mut().take() {
                            let _ = tx.send(Err(msg.clone()));
                            continue;
                        }
                        // Check for pending debug_schema oneshot.
                        if let Some(tx) = debug_schema_tx.borrow_mut().take() {
                            let _ = tx.send(Err(msg.clone()));
                            continue;
                        }
                        // Check for pending debug_seed oneshot.
                        if let Some(tx) = debug_seed_tx.borrow_mut().take() {
                            let _ = tx.send(Err(msg.clone()));
                            continue;
                        }
                        // Fall back to on_error callback.
                        if let Some(cb) = on_error.borrow().as_ref() {
                            let _ = cb.call1(&JsValue::NULL, &JsValue::from_str(&msg));
                        }
                    }

                    // main→worker frames arriving inbound: ignore.
                    _ => {}
                }
            }
        });
    }
}

#[cfg(target_arch = "wasm32")]
pub use wasm_client::WorkerClient;
