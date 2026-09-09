//! jazz-napi — Native Node.js bindings for Jazz.
//!
//! Provides Node.js bindings for the Jazz core database, server helpers, and
//! local-first identity utilities.
//!
//! # Architecture
//!
//! - `NapiDb` exposes the Jazz database directly over an
//!   encoded-row boundary for the TypeScript client packages.
//! - `JazzServer` exposes the Rust server process used by integration tests
//!   and Node deployments.
//! - Local-first JWT helpers stay here as package-level native utilities.
//!
//! # Allocator
//!
//! This crate uses `mimalloc-safe` (napi-rs–maintained mimalloc fork) as Rust's
//! `#[global_allocator]`. It does NOT override the host process's `malloc`/`free` —
//! Node.js / V8 keep their own allocator. The two coexist safely as long as
//! memory crosses the FFI boundary **by copy**, which is what napi-rs does today
//! for Vec/String/Buffer returns.
//!
//! Footgun: never `Vec::leak` / `Box::into_raw` an allocation across FFI and let
//! the host call `free()` on it — that mixes allocators and corrupts the heap.
//! If a future zero-copy shim is added, hand the host a Rust-defined finalizer
//! callback that frees through mimalloc instead.

#[cfg(feature = "rn-test-bridge")]
mod rn_test_bridge;

#[global_allocator]
static GLOBAL: mimalloc_safe::MiMalloc = mimalloc_safe::MiMalloc;

use napi::bindgen_prelude::*;
use napi::sys;
use napi::threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode};
use napi_derive::napi;
use serde::Deserialize;
/// Any JSON-compatible value crossing the native JavaScript boundary.
///
/// Keep this alias exported through napi-rs rather than relying on its Rust
/// import name: exported methods use `JsonValue` throughout their generated
/// declarations, so the package must define that name for TypeScript
/// consumers.
#[napi]
pub type JsonValue = serde_json::Value;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures::FutureExt;
use futures::future::LocalBoxFuture;
use futures::lock::Mutex as LocalMutex;
use futures::task::{ArcWake, waker};
use jazz::db::LargeValueUpdate as CoreLargeValueUpdate;
use jazz::db::StreamingMutationKind as CoreStreamingMutationKind;
use jazz::db::{
    ConnectionSessionContext as CoreConnectionSessionContext, Db as CoreDb,
    DbConfig as CoreDbConfig, DbIdentity as CoreDbIdentity,
    InitialSyncFlushCadence as CoreInitialSyncFlushCadence, LocalUpdates as CoreLocalUpdates,
    MutationErrorCallback as CoreMutationErrorCallback, PeerConnection as CorePeerConnection,
    Propagation as CorePropagation, ReadOpts as CoreReadOpts, RowCells as CoreRowCells,
    SeededRowIdSource as CoreSeededRowIdSource, SerializedReadResult as CoreSerializedReadResult,
    SerializedSubscriptionAuthorization as CoreSerializedSubscriptionAuthorization,
    StreamingValueUpload as CoreStreamingValueUpload, SubscriptionEvent as CoreSubscriptionEvent,
    SubscriptionStream, TickScheduler as CoreTickScheduler, TickUrgency as CoreTickUrgency,
    WireTransportAdapter as CoreWireTransportAdapter, WriteHandle, block_on as core_block_on,
};
use jazz::groove::records::Value as CoreValue;
use jazz::groove::storage::{
    MemoryStorage as CoreMemoryStorage, OrderedKvStorage as CoreOrderedKvStorage,
    ReopenableStorage as CoreReopenableStorage,
};
use jazz::ids::{
    AuthorSubject as CoreAuthorSubject, NodeUuid as CoreNodeUuid, RowUuid as CoreRowUuid,
};
use jazz::protocol::{
    BranchSelector as CoreBranchSelector, BranchViewBase as CoreBranchViewBase,
    DelegatedSessionBinding as CoreDelegatedSessionBinding,
    PermissionAdvice as CorePermissionAdvice, PermissionAdviceAction as CorePermissionAdviceAction,
    ReadViewSpec as CoreReadViewSpec,
};
use jazz::schema::JazzSchema;
use jazz::storage_codec_profile::epoch_1_storage_codec_profile;
use jazz::tools::OpenTransactionId as CoreOpenTransactionId;
use jazz::tools::identity;
use jazz::tools::{AppId, TransactionId};
use jazz::tx::{DurabilityTier as CoreDurabilityTier, TxId};
use jazz::wire::{
    TransportError, WireAuthorityEndpoint as CoreWireAuthorityEndpoint,
    WireTransport as CoreWireTransport,
};
use jazz_server::AuthConfig;
use jazz_server::{
    JazzServer as CoreJazzServer, ServerBuilder, ServerDataDir, StorageBackend, TEST_JWT_AUDIENCE,
    TEST_JWT_ISSUER, TestJwtIssuer as JazzTestJwtIssuer, TestJwtOptions,
};
use jazz_storage_rocksdb::{
    Durability as CoreRocksDbDurability, RocksDbStorage as CoreRocksDbStorage,
};

/// Exact build/ABI fingerprint for the generated native artifact.
#[napi]
pub fn native_artifact_fingerprint() -> String {
    option_env!("JAZZ_NATIVE_ARTIFACT_FINGERPRINT")
        .unwrap_or("missing-build-fingerprint")
        .to_owned()
}

/// Test-only bridge for executing the Rust-owned v1 binding corpus through the
/// generated NAPI artifact. It intentionally returns the frozen corpus rather
/// than a TypeScript reimplementation of its byte layouts.
#[napi(js_name = "__testBindingCodecGoldenFixture", skip_typescript)]
pub fn test_binding_codec_golden_fixture() -> String {
    jazz::binding_codec::BINDING_CODEC_GOLDEN_FIXTURE.to_owned()
}

/// Test-only direct execution of one frozen complete v1 frame through the
/// generated NAPI artifact. This reaches the production Rust frame, feature,
/// compression, and semantic-payload decoders; it is not a host wire API.
#[napi(js_name = "__testValidateWireFrameCorpus", skip_typescript)]
pub fn test_validate_wire_frame_corpus(frame: Buffer, negotiated_features: String) -> Result<()> {
    let negotiated_features = negotiated_features
        .parse()
        .map_err(|_| napi::Error::from_reason("test wire corpus features must be a u64 decimal"))?;
    jazz::wire::validate_frame_for_artifact_corpus(&frame, negotiated_features)
        .map_err(napi::Error::from_reason)
}

/// Test-only feature inventory paired with [`test_validate_wire_frame_corpus`].
#[napi(js_name = "__testWireFrameCorpusFeatures", skip_typescript)]
pub fn test_wire_frame_corpus_features() -> String {
    jazz::wire::current_wire_features().to_string()
}

#[derive(Clone, Debug, Deserialize)]
struct CoreOpenDbConfig {
    identity: CoreOpenDbIdentity,
    row_id_seed: Option<u64>,
    history_complete: bool,
    initial_sync_flush_every: Option<u32>,
    backend_credential: Option<String>,
}

/// This is a binding-internal wire capability, not a generic author parser.
/// The claimed author is accepted only after the signed proof derives the
/// exact same canonical local-first or anonymous subject.
#[derive(Clone, Debug, Deserialize, serde::Serialize)]
struct CoreSelfSignedClientProof {
    token: String,
    app_id: String,
    claimed_author: String,
}

/// Proof capabilities belong to one native runtime, including its schema and Tx views.
/// Claims remain operation-local; this map admits only the exact structured author.
#[derive(Clone, Default)]
struct NativeAuthorAdmissions(Rc<RefCell<HashMap<String, CoreSelfSignedClientProof>>>);

impl NativeAuthorAdmissions {
    fn verify(proof: &CoreSelfSignedClientProof) -> napi::Result<CoreAuthorSubject> {
        let author = identity::verify_client_runtime_author(
            &proof.token,
            &proof.app_id,
            &proof.claimed_author,
        )
        .map_err(napi::Error::from_reason)?;
        if author.principal_parts().0 != identity::LOCAL_FIRST_ISSUER
            || author.account_id().is_none()
        {
            return Err(napi::Error::from_reason(
                "session admission requires an account-bound local-first proof",
            ));
        }
        Ok(author)
    }

    fn admit(&self, proof: CoreSelfSignedClientProof) -> napi::Result<()> {
        Self::verify(&proof)?;
        self.0
            .borrow_mut()
            .insert(proof.claimed_author.clone(), proof);
        Ok(())
    }

    fn resolve(&self, bytes: &[u8]) -> napi::Result<CoreAuthorSubject> {
        match core_author_id_from_bytes(bytes) {
            Ok(author) => Ok(author),
            Err(error) => {
                let canonical = std::str::from_utf8(bytes).map_err(|_| {
                    napi::Error::from_reason("author subject must be canonical UTF-8 JSON")
                })?;
                let proofs = self.0.borrow();
                let Some(proof) = proofs.get(canonical) else {
                    return Err(error);
                };
                // Recheck expiry and all cryptographic bindings for every ingress.
                Self::verify(proof)
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize)]
struct CoreOpenDbIdentity {
    node: CoreNodeUuid,
    #[serde(deserialize_with = "CoreAuthorSubject::deserialize_untrusted")]
    author: CoreAuthorSubject,
}

#[napi(object)]
pub struct InsertOptions {
    pub row_id: Option<Uint8Array>,
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub branch: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct UpdateOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub head: Option<JsonValue>,
    pub base: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct UpsertOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub head: Option<JsonValue>,
    pub base: Option<JsonValue>,
    /// Parsed only to reject the removed JavaScript `{ branch }` upsert shape.
    ///
    /// This is deliberately omitted from the public TypeScript declaration:
    /// callers must use `head` (and optionally `base`) for a branch view.
    #[napi(skip_typescript)]
    pub branch: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

/// The runtime representation of JavaScript upsert options.
///
/// `#[napi(object)]` intentionally maps an absent, `undefined`, and `null`
/// optional field to the same Rust `None`.  That is appropriate for supported
/// optional fields, but not for the removed `branch` option: its *presence*
/// must be rejected so an untyped caller cannot silently fall back to Root.
/// Keep the generated [`UpsertOptions`] interface for TypeScript consumers,
/// and parse this private representation from the raw JS object instead.
struct ParsedUpsertOptions {
    author: Option<Uint8Array>,
    attribution: Option<Uint8Array>,
    head: Option<JsonValue>,
    base: Option<JsonValue>,
    branch_present: bool,
    updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct DeleteOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub head: Option<JsonValue>,
    pub base: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

#[napi(object)]
pub struct RestoreOptions {
    pub author: Option<Uint8Array>,
    pub attribution: Option<Uint8Array>,
    pub branch: Option<JsonValue>,
    pub updated_at_ms: Option<f64>,
}

impl From<CoreOpenDbIdentity> for CoreDbIdentity {
    fn from(identity: CoreOpenDbIdentity) -> Self {
        Self {
            node: identity.node,
            author: identity.author,
        }
    }
}

#[derive(Clone, Debug, serde::Serialize)]
struct WriteResult {
    row_id: CoreRowUuid,
    tx_id: TxId,
}

type NapiDbInner = Rc<RefCell<Option<NapiDbInnerStorage>>>;

#[derive(Clone)]
enum NapiDbInnerStorage {
    Memory(Rc<CoreDb<CoreMemoryStorage>>),
    Persistent(Rc<CoreDb<CoreRocksDbStorage>>),
}

enum NapiWrite {
    Memory {
        db: Rc<CoreDb<CoreMemoryStorage>>,
        write: WriteHandle<CoreMemoryStorage>,
    },
    Persistent {
        db: Rc<CoreDb<CoreRocksDbStorage>>,
        write: WriteHandle<CoreRocksDbStorage>,
    },
}

#[derive(Clone, Default)]
struct WireQueues {
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
}

struct NapiWireTransport {
    queues: WireQueues,
}

struct NapiTickScheduler {
    callback: std::sync::Arc<ThreadsafeFunction<String, ()>>,
}

struct NapiQueryRuntimeWake {
    callback: std::sync::Arc<ThreadsafeFunction<String, ()>>,
}

impl ArcWake for NapiQueryRuntimeWake {
    fn wake_by_ref(arc_self: &std::sync::Arc<Self>) {
        let _ = arc_self.callback.call(
            Ok("immediate".to_owned()),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }
}

impl CoreTickScheduler for NapiTickScheduler {
    fn schedule_tick(&self, urgency: CoreTickUrgency) {
        let urgency = match urgency {
            CoreTickUrgency::Immediate => "immediate",
            CoreTickUrgency::Deferred => "deferred",
            CoreTickUrgency::AfterCurrentTurn => "after-current-turn",
        };
        let _ = self.callback.call(
            Ok(urgency.to_string()),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }

    fn schedule_tick_after(&self, delay_ms: u64) {
        let _ = self.callback.call(
            Ok(format!("after:{delay_ms}")),
            ThreadsafeFunctionCallMode::NonBlocking,
        );
    }

    fn query_runtime_waker(&self) -> Option<Waker> {
        Some(waker(std::sync::Arc::new(NapiQueryRuntimeWake {
            callback: self.callback.clone(),
        })))
    }
}

impl CoreWireTransport for NapiWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> std::result::Result<(), TransportError> {
        self.queues.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.inbound.borrow_mut().pop_front()
    }
}

#[napi(js_name = "Write")]
pub struct Write {
    payload: Vec<u8>,
    row_id: CoreRowUuid,
    tx_id: TransactionId,
    inner: Option<NapiWrite>,
}

type NativeReadCleanup = Rc<RefCell<Option<Box<dyn FnOnce()>>>>;

/// A JavaScript-thread-owned binding read waiting for query coverage or
/// asynchronous large-value storage. NAPI promises execute on a Send worker
/// pool, whereas a Jazz runtime is deliberately `Rc`/thread-affine. The
/// adapter drives this object after its peer transport makes progress instead
/// of blocking Node.
#[napi]
pub struct PendingNativeRead {
    future: Rc<RefCell<Option<LocalBoxFuture<'static, napi::Result<Uint8Array>>>>>,
    cleanup: NativeReadCleanup,
}

/// Thread-affine subscription opening waiting for the core owner.
#[napi]
pub struct PendingNativeSubscription {
    future: RefCell<Option<LocalBoxFuture<'static, napi::Result<Subscription>>>>,
    wake: RefCell<Option<Waker>>,
}

#[napi]
impl PendingNativeSubscription {
    #[napi(js_name = "setWake")]
    pub fn set_wake(&self, callback: ThreadsafeFunction<String, ()>) {
        *self.wake.borrow_mut() = Some(waker(std::sync::Arc::new(NapiQueryRuntimeWake {
            callback: std::sync::Arc::new(callback),
        })));
    }

    #[napi]
    pub fn poll(&self) -> napi::Result<Option<Subscription>> {
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "subscription opening is complete or cancelled",
            ));
        };
        let wake = self
            .wake
            .borrow()
            .clone()
            .unwrap_or_else(|| Waker::noop().clone());
        let mut context = Context::from_waker(&wake);
        match future.as_mut().poll(&mut context) {
            Poll::Ready(result) => result.map(Some),
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }

    #[napi]
    pub fn cancel(&self) {
        self.future.borrow_mut().take();
        self.wake.borrow_mut().take();
    }
}

/// A JavaScript-thread-owned permission preflight which is waiting for an
/// authenticated upstream authority.  Like pending reads, this remains on the
/// owning JavaScript thread: NAPI's worker-pool promises require `Send`, while
/// the request future deliberately owns thread-affine connection state.
#[napi]
pub struct PendingNativePermissionAdvice {
    future: Rc<RefCell<Option<LocalBoxFuture<'static, napi::Result<String>>>>>,
}

struct PendingSubscriptionBatchCompletion {
    events: Vec<SubscriptionEvent>,
}

/// A retryable chunk miss is not a subscription failure: retain the exact raw
/// batch so the next host turn can re-run hydration after the peer pump has
/// supplied the requested content.
enum PendingSubscriptionBatchOutcome {
    Complete(PendingSubscriptionBatchCompletion),
    Retryable {
        events: Vec<CoreSubscriptionEvent>,
        retry_after_ms: u32,
    },
}

enum PendingSubscriptionBatchState {
    Future(LocalBoxFuture<'static, napi::Result<PendingSubscriptionBatchOutcome>>),
    /// The raw events have been returned to the subscription queue. Keep this
    /// marker visible to JavaScript for one turn so it can honor the retry
    /// delay rather than tight-polling an immediately retryable resolver.
    Retryable {
        retry_after_ms: u32,
    },
}

enum PendingSubscriptionBatchPoll {
    Pending,
    Complete(PendingSubscriptionBatchCompletion),
    /// `Some` is the first observation of the retryable future completion and
    /// owns events which must be restored to the front of the queue. `None`
    /// is the following host turn, which clears the marker and retries them.
    Retryable {
        events: Option<Vec<CoreSubscriptionEvent>>,
    },
}

/// Opaque marker returned while the next bounded native subscription batch is
/// waiting for chunk I/O. Call `readAll` again after transport progress.
#[napi]
pub struct PendingNativeSubscriptionBatch {
    state: Rc<RefCell<Option<PendingSubscriptionBatchState>>>,
}

impl Clone for PendingNativeSubscriptionBatch {
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
        }
    }
}

impl PendingNativeSubscriptionBatch {
    fn new(future: LocalBoxFuture<'static, napi::Result<PendingSubscriptionBatchOutcome>>) -> Self {
        Self {
            state: Rc::new(RefCell::new(Some(PendingSubscriptionBatchState::Future(
                future,
            )))),
        }
    }

    fn poll_once(&self) -> napi::Result<PendingSubscriptionBatchPoll> {
        let Some(state) = self.state.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native pending subscription batch is already complete",
            ));
        };
        let PendingSubscriptionBatchState::Future(mut future) = state else {
            let PendingSubscriptionBatchState::Retryable { retry_after_ms } = state else {
                unreachable!("subscription batch state is exhaustive");
            };
            *self.state.borrow_mut() =
                Some(PendingSubscriptionBatchState::Retryable { retry_after_ms });
            return Ok(PendingSubscriptionBatchPoll::Retryable { events: None });
        };
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match Pin::new(&mut future).poll(&mut context) {
            Poll::Ready(result) => result.map(|outcome| match outcome {
                PendingSubscriptionBatchOutcome::Complete(completion) => {
                    PendingSubscriptionBatchPoll::Complete(completion)
                }
                PendingSubscriptionBatchOutcome::Retryable {
                    events,
                    retry_after_ms,
                } => {
                    *self.state.borrow_mut() =
                        Some(PendingSubscriptionBatchState::Retryable { retry_after_ms });
                    PendingSubscriptionBatchPoll::Retryable {
                        events: Some(events),
                    }
                }
            }),
            Poll::Pending => {
                *self.state.borrow_mut() = Some(PendingSubscriptionBatchState::Future(future));
                Ok(PendingSubscriptionBatchPoll::Pending)
            }
        }
    }

    fn retry_after_ms(&self) -> Option<u32> {
        match self.state.borrow().as_ref() {
            Some(PendingSubscriptionBatchState::Retryable { retry_after_ms }) => {
                Some(*retry_after_ms)
            }
            _ => None,
        }
    }
}

impl PendingNativeRead {
    fn new(future: LocalBoxFuture<'static, napi::Result<Uint8Array>>) -> Self {
        Self {
            future: Rc::new(RefCell::new(Some(future))),
            cleanup: Rc::new(RefCell::new(None)),
        }
    }

    fn with_cleanup(
        future: LocalBoxFuture<'static, napi::Result<Uint8Array>>,
        cleanup: Box<dyn FnOnce()>,
    ) -> Self {
        Self {
            future: Rc::new(RefCell::new(Some(future))),
            cleanup: Rc::new(RefCell::new(Some(cleanup))),
        }
    }

    fn poll_once(&self) -> napi::Result<Option<Uint8Array>> {
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native pending read is already complete",
            ));
        };
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match Pin::new(&mut future).poll(&mut context) {
            Poll::Ready(result) => {
                if let Some(cleanup) = self.cleanup.borrow_mut().take() {
                    cleanup();
                }
                result.map(Some)
            }
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }
}

impl Drop for PendingNativeRead {
    fn drop(&mut self) {
        self.future.borrow_mut().take();
        if let Some(cleanup) = self.cleanup.borrow_mut().take() {
            cleanup();
        }
    }
}

impl PendingNativePermissionAdvice {
    fn new(future: LocalBoxFuture<'static, napi::Result<String>>) -> Self {
        Self {
            future: Rc::new(RefCell::new(Some(future))),
        }
    }

    fn poll_once(&self) -> napi::Result<Option<String>> {
        let Some(mut future) = self.future.borrow_mut().take() else {
            return Err(napi::Error::from_reason(
                "native pending permission advice is already complete",
            ));
        };
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        match Pin::new(&mut future).poll(&mut context) {
            Poll::Ready(result) => result.map(Some),
            Poll::Pending => {
                *self.future.borrow_mut() = Some(future);
                Ok(None)
            }
        }
    }

    fn cancel_inner(&self) {
        // Dropping PermissionAdviceFuture removes only its opaque waiter and
        // makes late/replayed authority responses inert.
        self.future.borrow_mut().take();
    }
}

#[napi]
impl PendingNativeSubscriptionBatch {
    /// The host should wait this bounded delay before asking the subscription
    /// to retry a retained chunk-hydration batch.
    #[napi(js_name = "retryAfterMs")]
    pub fn retry_after_ms_for_js(&self) -> Option<u32> {
        self.retry_after_ms()
    }
}

#[napi]
impl PendingNativeRead {
    #[napi]
    pub fn poll(&self) -> napi::Result<Option<Uint8Array>> {
        self.poll_once()
    }
}

#[napi]
impl PendingNativePermissionAdvice {
    #[napi]
    pub fn poll(&self) -> napi::Result<Option<String>> {
        self.poll_once()
    }

    #[napi]
    pub fn cancel(&self) {
        self.cancel_inner();
    }
}

fn native_read_or_pending(
    future: LocalBoxFuture<'static, napi::Result<Uint8Array>>,
) -> napi::Result<Either<Uint8Array, PendingNativeRead>> {
    let pending = PendingNativeRead::new(future);
    match pending.poll_once()? {
        Some(bytes) => Ok(Either::A(bytes)),
        None => Ok(Either::B(pending)),
    }
}

fn native_covered_read_or_pending(
    future: LocalBoxFuture<'static, napi::Result<Uint8Array>>,
    cleanup: Box<dyn FnOnce()>,
) -> napi::Result<Either<Uint8Array, PendingNativeRead>> {
    let pending = PendingNativeRead::with_cleanup(future, cleanup);
    match pending.poll_once()? {
        Some(bytes) => Ok(Either::A(bytes)),
        None => Ok(Either::B(pending)),
    }
}

fn native_permission_advice_or_pending(
    future: LocalBoxFuture<'static, napi::Result<String>>,
) -> napi::Result<Either<String, PendingNativePermissionAdvice>> {
    let pending = PendingNativePermissionAdvice::new(future);
    match pending.poll_once()? {
        Some(advice) => Ok(Either::A(advice)),
        None => Ok(Either::B(pending)),
    }
}

fn permission_advice_for_js(advice: CorePermissionAdvice) -> String {
    match advice {
        CorePermissionAdvice::Allowed => "allowed",
        CorePermissionAdvice::Denied => "denied",
        CorePermissionAdvice::Unknown => "unknown",
    }
    .to_owned()
}

fn napi_error(error: impl std::fmt::Display) -> napi::Error {
    napi::Error::from_reason(error.to_string())
}

#[napi(js_name = "Transport")]
pub struct Transport {
    inner: NapiTransportInner,
    queues: WireQueues,
    auxiliary_pump: jazz::db::PeerIoPump,
}

#[napi(js_name = "Subscription")]
pub struct Subscription {
    inner: Option<NapiSubscription>,
}

#[napi(object)]
pub struct SubscriptionDeltaEvent {
    #[napi(js_name = "type", ts_type = "'delta'")]
    pub event_type: String,
    pub reset: bool,
    pub delta: Uint8Array,
    #[napi(js_name = "terminalOperations")]
    pub terminal_operations: Vec<SubscriptionTerminalOperation>,
    pub settled: bool,
    #[napi(ts_type = "'None' | 'Local' | 'Edge' | 'Global'")]
    pub tier: String,
}

#[napi(object)]
pub struct SubscriptionRejectedEvent {
    #[napi(js_name = "type", ts_type = "'rejected'")]
    pub event_type: String,
    pub reason: SubscriptionRejectionReason,
}

#[napi(object)]
pub struct SubscriptionClosedEvent {
    #[napi(js_name = "type", ts_type = "'closed'")]
    pub event_type: String,
}

#[napi(object)]
pub struct SubscriptionUnsupportedShapeCapabilityReason {
    #[napi(js_name = "type", ts_type = "'UnsupportedShapeCapability'")]
    pub reason_type: String,
    pub detail: String,
}

#[napi(object)]
pub struct SubscriptionShapeRegistrationPendingReason {
    #[napi(
        js_name = "type",
        ts_type = "'ShapeRegistrationPendingCatalogueAdmission'"
    )]
    pub reason_type: String,
}

#[napi(object)]
pub struct SubscriptionServerFailureReason {
    #[napi(js_name = "type", ts_type = "'ServerFailure'")]
    pub reason_type: String,
    #[napi(
        ts_type = "'TableNotFound' | 'SchemaResolution' | 'QueryValidation' | 'QueryLowering' | 'PolicyEvaluation' | 'Internal'"
    )]
    pub code: String,
}

#[napi(object)]
pub struct SubscriptionInvalidAuthoritySourceClosureReason {
    #[napi(js_name = "type", ts_type = "'InvalidAuthoritySourceClosure'")]
    pub reason_type: String,
    pub transition: String,
}

#[napi(object)]
pub struct SubscriptionTerminalOperation {
    #[napi(js_name = "root_key")]
    pub root_key: Vec<u32>,
    pub path: Vec<SubscriptionTerminalPathSegment>,
    pub edit: SubscriptionTerminalEdit,
}

#[napi(object)]
pub struct SubscriptionTerminalCollectionPathSegment {
    #[napi(js_name = "Collection")]
    pub collection: String,
}

#[napi(object)]
pub struct SubscriptionTerminalKeyPathSegment {
    #[napi(js_name = "Key")]
    pub key: Vec<u32>,
}

#[napi(object)]
pub struct SubscriptionTerminalInsertEdit {
    #[napi(js_name = "Insert")]
    pub insert: SubscriptionTerminalInsert,
}

#[napi(object)]
pub struct SubscriptionTerminalInsert {
    pub index: f64,
    pub key: Vec<u32>,
    pub value: Vec<u32>,
}

#[napi(object)]
pub struct SubscriptionTerminalUpdateEdit {
    #[napi(js_name = "Update")]
    pub update: SubscriptionTerminalUpdate,
}

#[napi(object)]
pub struct SubscriptionTerminalUpdate {
    pub key: Vec<u32>,
    pub value: Vec<u32>,
}

#[napi(object)]
pub struct SubscriptionTerminalRemoveEdit {
    #[napi(js_name = "Remove")]
    pub remove: SubscriptionTerminalRemove,
}

#[napi(object)]
pub struct SubscriptionTerminalRemove {
    pub key: Vec<u32>,
}

#[napi(object)]
pub struct SubscriptionTerminalMoveEdit {
    #[napi(js_name = "Move")]
    pub move_edit: SubscriptionTerminalMove,
}

#[napi(object)]
pub struct SubscriptionTerminalMove {
    pub key: Vec<u32>,
    pub index: f64,
}

#[napi]
pub type SubscriptionEvent =
    Either3<SubscriptionDeltaEvent, SubscriptionRejectedEvent, SubscriptionClosedEvent>;

#[napi]
pub type SubscriptionRejectionReason = Either4<
    SubscriptionUnsupportedShapeCapabilityReason,
    SubscriptionShapeRegistrationPendingReason,
    SubscriptionServerFailureReason,
    SubscriptionInvalidAuthoritySourceClosureReason,
>;

#[napi]
pub type SubscriptionTerminalPathSegment =
    Either<SubscriptionTerminalCollectionPathSegment, SubscriptionTerminalKeyPathSegment>;

#[napi]
pub type SubscriptionTerminalEdit = Either4<
    SubscriptionTerminalInsertEdit,
    SubscriptionTerminalUpdateEdit,
    SubscriptionTerminalRemoveEdit,
    SubscriptionTerminalMoveEdit,
>;

enum NapiTransportInner {
    Memory {
        db: Rc<CoreDb<CoreMemoryStorage>>,
        connection: Option<Rc<LocalMutex<CorePeerConnection<CoreMemoryStorage>>>>,
    },
    Persistent {
        db: Rc<CoreDb<CoreRocksDbStorage>>,
        connection: Option<Rc<LocalMutex<CorePeerConnection<CoreRocksDbStorage>>>>,
    },
    Closed,
}

impl NapiTransportInner {
    fn auxiliary_pump(&self) -> jazz::db::PeerIoPump {
        match self {
            Self::Memory { connection, .. } => core_block_on(async {
                connection
                    .as_ref()
                    .expect("new transport has a connection")
                    .lock()
                    .await
                    .io_pump()
            }),
            Self::Persistent { connection, .. } => core_block_on(async {
                connection
                    .as_ref()
                    .expect("new transport has a connection")
                    .lock()
                    .await
                    .io_pump()
            }),
            Self::Closed => panic!("closed transport has no auxiliary pump"),
        }
    }
}

enum NapiSubscription {
    Memory {
        db: Rc<CoreDb<CoreMemoryStorage>>,
        stream: SubscriptionStream,
        pending_events: VecDeque<CoreSubscriptionEvent>,
        pending_batch: Option<PendingNativeSubscriptionBatch>,
    },
    Persistent {
        db: Rc<CoreDb<CoreRocksDbStorage>>,
        stream: SubscriptionStream,
        pending_events: VecDeque<CoreSubscriptionEvent>,
        pending_batch: Option<PendingNativeSubscriptionBatch>,
    },
}

impl Write {
    fn wait_promise(
        &self,
        env: Env,
        tier: CoreDurabilityTier,
    ) -> napi::Result<PromiseRaw<'static, ()>> {
        let Some(write) = &self.inner else {
            return Err(napi::Error::from_reason("write state is unavailable"));
        };
        let mut deferred = std::ptr::null_mut();
        let mut promise = std::ptr::null_mut();
        let env = env.raw();
        let status = unsafe { sys::napi_create_promise(env, &mut deferred, &mut promise) };
        if status != sys::Status::napi_ok {
            return Err(napi::Error::from_reason(
                "failed to create transaction wait promise",
            ));
        }
        let callback = move |result: std::result::Result<TxId, jazz::db::Error>| {
            finish_wait_promise(env, deferred, result);
        };
        match write {
            NapiWrite::Memory { db, write } => {
                db.wait_for_write_with(write, tier, callback);
            }
            NapiWrite::Persistent { db, write } => {
                db.wait_for_write_with(write, tier, callback);
            }
        }
        Ok(PromiseRaw::new(env, promise))
    }
}

#[napi]
impl Write {
    #[napi(getter, js_name = "txId")]
    pub fn tx_id(&self) -> String {
        self.tx_id.to_string()
    }

    #[napi(getter)]
    pub fn payload(&self) -> Uint8Array {
        Uint8Array::new(self.payload.clone())
    }

    #[napi(getter, js_name = "rowId")]
    pub fn row_id(&self) -> Uint8Array {
        Uint8Array::new(self.row_id.to_bytes())
    }

    #[napi(js_name = "writeState")]
    pub fn write_state(&self) -> napi::Result<serde_json::Value> {
        let Some(write) = &self.inner else {
            return Err(napi::Error::from_reason("write state is unavailable"));
        };
        let state = match write {
            NapiWrite::Memory { write, .. } => core_block_on(write.write_state()),
            NapiWrite::Persistent { write, .. } => core_block_on(write.write_state()),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(core_write_state_to_json(&state))
    }

    #[napi]
    pub fn wait(&self, env: Env, tier: String) -> napi::Result<PromiseRaw<'static, ()>> {
        self.wait_promise(env, core_durability_tier_from_str(&tier)?)
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }
}

#[napi]
impl Transport {
    #[napi(js_name = "routeAuxiliaryWireFrame")]
    pub fn route_auxiliary_wire_frame(
        &self,
        frame: Uint8Array,
    ) -> napi::Result<Option<Uint8Array>> {
        core_block_on(
            self.auxiliary_pump
                .route_incoming_wire_frame(frame.to_vec()),
        )
        .map(|frame| frame.map(Uint8Array::new))
        .map_err(napi::Error::from_reason)
    }

    #[napi(js_name = "recvAuxiliaryWireFrames")]
    pub fn recv_auxiliary_wire_frames(&self) -> napi::Result<Vec<Uint8Array>> {
        let mut frames = Vec::new();
        while let Some(frame) = self
            .auxiliary_pump
            .take_outbound_wire_frame()
            .map_err(napi::Error::from_reason)?
        {
            frames.push(Uint8Array::new(frame));
        }
        Ok(frames)
    }

    #[napi(js_name = "auxiliaryOutboundReady")]
    pub fn auxiliary_outbound_ready(&self) -> bool {
        self.auxiliary_pump.outbound_is_ready()
    }

    #[napi(js_name = "sendWireFrame")]
    pub fn send_wire_frame(&self, frame: Uint8Array) {
        self.queues.inbound.borrow_mut().push_back(frame.to_vec());
    }

    #[napi(js_name = "sendWireFrames")]
    pub fn send_wire_frames(&self, frames: Vec<Uint8Array>) {
        let mut inbound = self.queues.inbound.borrow_mut();
        for frame in frames {
            inbound.push_back(frame.to_vec());
        }
    }

    #[napi(js_name = "recvWireFrames")]
    pub fn recv_wire_frames(&self) -> Vec<Uint8Array> {
        let mut frames = Vec::new();
        let mut outbound = self.queues.outbound.borrow_mut();
        while let Some(frame) = outbound.pop_front() {
            frames.push(Uint8Array::new(frame));
        }
        frames
    }

    #[napi]
    pub fn tick(&self) -> napi::Result<u32> {
        match &self.inner {
            NapiTransportInner::Memory { connection, .. } => core_tick_connection(connection),
            NapiTransportInner::Persistent { connection, .. } => core_tick_connection(connection),
            NapiTransportInner::Closed => Ok(0),
        }
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.auxiliary_pump.disconnect();
        match std::mem::replace(&mut self.inner, NapiTransportInner::Closed) {
            NapiTransportInner::Memory { db, connection } => {
                let Some(connection) = connection else {
                    return false;
                };
                db.detach_connection(&connection)
            }
            NapiTransportInner::Persistent { db, connection } => {
                let Some(connection) = connection else {
                    return false;
                };
                db.detach_connection(&connection)
            }
            NapiTransportInner::Closed => false,
        }
    }
}

#[napi]
impl Subscription {
    #[napi(js_name = "readAll")]
    pub fn read_all(
        &mut self,
    ) -> napi::Result<Either<Vec<SubscriptionEvent>, PendingNativeSubscriptionBatch>> {
        let subscription = self
            .inner
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("subscription is closed"))?;
        let result = match subscription {
            NapiSubscription::Memory {
                db,
                stream,
                pending_events,
                pending_batch,
            } => read_or_start_subscription_batch(db, stream, pending_events, pending_batch),
            NapiSubscription::Persistent {
                db,
                stream,
                pending_events,
                pending_batch,
            } => read_or_start_subscription_batch(db, stream, pending_events, pending_batch),
        };
        match result {
            Ok(Some(events)) => Ok(Either::A(events)),
            Ok(None) => match subscription {
                NapiSubscription::Memory { pending_batch, .. }
                | NapiSubscription::Persistent { pending_batch, .. } => Ok(Either::B(
                    pending_batch
                        .as_ref()
                        .expect("suspended batch is retained")
                        .clone(),
                )),
            },
            Err(error) => {
                self.inner.take();
                Err(error)
            }
        }
    }

    #[napi]
    pub fn drain(
        &mut self,
    ) -> napi::Result<Either<Vec<SubscriptionEvent>, PendingNativeSubscriptionBatch>> {
        self.read_all()
    }

    #[napi]
    pub fn close(&mut self) -> bool {
        self.inner.take().is_some()
    }
}

const MAX_RETAINED_SUBSCRIPTION_BATCH_EVENTS: usize = 256;

fn requeue_retryable_subscription_batch(
    pending_events: &mut VecDeque<CoreSubscriptionEvent>,
    events: Vec<CoreSubscriptionEvent>,
) {
    for event in events.into_iter().rev() {
        pending_events.push_front(event);
    }
}

fn read_or_start_subscription_batch<S>(
    db: &Rc<CoreDb<S>>,
    stream: &mut SubscriptionStream,
    pending_events: &mut VecDeque<CoreSubscriptionEvent>,
    pending_batch: &mut Option<PendingNativeSubscriptionBatch>,
) -> napi::Result<Option<Vec<SubscriptionEvent>>>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    if let Some(batch) = pending_batch.as_ref() {
        match batch.poll_once()? {
            PendingSubscriptionBatchPoll::Complete(completion) => {
                *pending_batch = None;
                return Ok(Some(completion.events));
            }
            PendingSubscriptionBatchPoll::Pending => return Ok(None),
            PendingSubscriptionBatchPoll::Retryable {
                events: Some(events),
                ..
            } => {
                requeue_retryable_subscription_batch(pending_events, events);
                return Ok(None);
            }
            PendingSubscriptionBatchPoll::Retryable { events: None, .. } => {
                *pending_batch = None;
            }
        }
    }
    let mut raw_events = Vec::with_capacity(MAX_RETAINED_SUBSCRIPTION_BATCH_EVENTS);
    while raw_events.len() < MAX_RETAINED_SUBSCRIPTION_BATCH_EVENTS {
        let Some(event) = pending_events
            .pop_front()
            .or_else(|| stream.try_next_event())
        else {
            break;
        };
        raw_events.push(event);
    }
    if raw_events.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let db = Rc::clone(db);
    let batch = PendingNativeSubscriptionBatch::new(Box::pin(async move {
        let mut raw_events = raw_events;
        for event in &mut raw_events {
            match db
                .hydrate_subscription_event_for_binding_outcome(event)
                .await
            {
                Ok(()) => {}
                Err(jazz::db::BindingHydrationError::RetryableChunkUnavailable {
                    retry_after_ms,
                }) => {
                    // Preserve the whole batch below rather than making a
                    // retryable absence terminal at the NAPI boundary.
                    return Ok(PendingSubscriptionBatchOutcome::Retryable {
                        events: raw_events,
                        retry_after_ms,
                    });
                }
                Err(jazz::db::BindingHydrationError::Error(error)) => {
                    return Err(napi_error(error));
                }
            }
        }
        let events = raw_events
            .iter()
            .map(core_subscription_event_to_napi)
            .collect::<napi::Result<Vec<_>>>()?;
        Ok(PendingSubscriptionBatchOutcome::Complete(
            PendingSubscriptionBatchCompletion { events },
        ))
    }));
    match batch.poll_once()? {
        PendingSubscriptionBatchPoll::Complete(completion) => Ok(Some(completion.events)),
        PendingSubscriptionBatchPoll::Pending
        | PendingSubscriptionBatchPoll::Retryable { events: None } => {
            *pending_batch = Some(batch);
            Ok(None)
        }
        PendingSubscriptionBatchPoll::Retryable {
            events: Some(events),
        } => {
            requeue_retryable_subscription_batch(pending_events, events);
            *pending_batch = Some(batch);
            Ok(None)
        }
    }
}

#[napi(js_name = "NapiDb")]
pub struct NapiDb {
    inner: NapiDbInner,
    owns_runtime: bool,
    non_durable_client: Rc<Cell<bool>>,
    // Only explicit backend opens mint this in-process capability. It is
    // independent of the SYSTEM author value.
    trusted_backend: bool,
    author_admissions: NativeAuthorAdmissions,
}

/// Native bounded-memory sink used by the TypeScript async streaming-mutation
/// adapter. Each push incrementally prepares and stages bounded Groove nodes,
/// using the same ingress policy and resumable construction as WASM.
#[napi(js_name = "StreamingMutation")]
pub struct StreamingMutation {
    db: NapiDbInner,
    table: String,
    row_id: CoreRowUuid,
    cells: Option<CoreRowCells>,
    column: String,
    mutation: CoreStreamingMutationKind,
    identity: jazz::db::WriteIdentity,
    updated_at_ms: Option<u64>,
    head: Option<CoreBranchSelector>,
    base: Option<CoreBranchViewBase>,
    upload: Option<CoreStreamingValueUpload>,
}

#[napi]
impl StreamingMutation {
    #[napi]
    pub fn push(&mut self, chunk: Uint8Array) -> napi::Result<()> {
        let upload = self
            .upload
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("streaming insert is closed"))?;
        let db = self.db.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let result = match db {
            NapiDbInnerStorage::Memory(db) => {
                core_block_on(db.push_streaming_value_upload(upload, chunk.as_ref()))
            }
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.push_streaming_value_upload(upload, chunk.as_ref()))
            }
        };
        result.map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi]
    pub fn finish(&mut self) -> napi::Result<Write> {
        let upload = self
            .upload
            .take()
            .ok_or_else(|| napi::Error::from_reason("streaming insert is closed"))?;
        let cells = self
            .cells
            .take()
            .ok_or_else(|| napi::Error::from_reason("streaming insert is closed"))?;
        let db = self.db.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => core_write_memory(
                Rc::clone(db),
                core_block_on(db.finish_streaming_value_upload(
                    upload,
                    self.mutation,
                    &self.table,
                    self.row_id,
                    cells,
                    &self.column,
                    self.identity,
                    self.updated_at_ms,
                    self.head.clone(),
                    self.base.clone(),
                ))
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
            NapiDbInnerStorage::Persistent(db) => core_write_persistent(
                Rc::clone(db),
                core_block_on(db.finish_streaming_value_upload(
                    upload,
                    self.mutation,
                    &self.table,
                    self.row_id,
                    cells,
                    &self.column,
                    self.identity,
                    self.updated_at_ms,
                    self.head.clone(),
                    self.base.clone(),
                ))
                .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            ),
        }
    }

    #[napi]
    pub fn abort(&mut self) -> napi::Result<bool> {
        self.cells.take();
        let Some(upload) = self.upload.take() else {
            return Ok(false);
        };
        let db = self.db.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                core_block_on(db.abort_streaming_value_upload(upload))
            }
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.abort_streaming_value_upload(upload))
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(true)
    }
}

#[napi]
impl NapiDb {
    /// Exact wire capabilities compiled into this native binding.
    ///
    /// The TypeScript WebSocket carrier uses this for its Hello instead of an
    /// independent feature list, so a package cannot advertise a codec this
    /// native artifact cannot decode.
    #[napi(js_name = "wireFeatures")]
    pub fn wire_features(&self) -> u32 {
        jazz::wire::current_wire_features() as u32
    }

    fn request_permission_advice(
        &self,
        action: CorePermissionAdviceAction,
        delegated_session: Option<CoreDelegatedSessionBinding>,
    ) -> napi::Result<Either<String, PendingNativePermissionAdvice>> {
        if delegated_session.is_some() {
            self.require_trusted_backend()?;
        }
        let advice = {
            let db = self.inner.borrow();
            let db = db
                .as_ref()
                .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
            match db {
                NapiDbInnerStorage::Memory(db) => delegated_session.map_or_else(
                    || db.request_permission_advice(action.clone()),
                    |session| {
                        db.request_permission_advice_with_delegated_session(action.clone(), session)
                    },
                ),
                NapiDbInnerStorage::Persistent(db) => delegated_session.map_or_else(
                    || db.request_permission_advice(action.clone()),
                    |session| {
                        db.request_permission_advice_with_delegated_session(action.clone(), session)
                    },
                ),
            }
        };
        native_permission_advice_or_pending(Box::pin(async move {
            Ok(permission_advice_for_js(advice.await))
        }))
    }

    #[napi(js_name = "requestInsertPermissionAdvice")]
    pub fn request_insert_permission_advice(
        &self,
        table: String,
        cells: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> napi::Result<Either<String, PendingNativePermissionAdvice>> {
        let delegated_session =
            core_delegated_session_from_napi(author, claims, &self.author_admissions)?;
        self.request_permission_advice(
            CorePermissionAdviceAction::Insert {
                table,
                cells: decode_core_cells(&cells)?,
            },
            delegated_session,
        )
    }

    #[napi(js_name = "requestReadPermissionAdvice")]
    pub fn request_read_permission_advice(
        &self,
        table: String,
        row_id: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> napi::Result<Either<String, PendingNativePermissionAdvice>> {
        let delegated_session =
            core_delegated_session_from_napi(author, claims, &self.author_admissions)?;
        self.request_permission_advice(
            CorePermissionAdviceAction::Read {
                table,
                row: core_row_uuid_from_bytes(&row_id)?,
            },
            delegated_session,
        )
    }

    #[napi(js_name = "requestUpdatePermissionAdvice")]
    pub fn request_update_permission_advice(
        &self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> napi::Result<Either<String, PendingNativePermissionAdvice>> {
        let delegated_session =
            core_delegated_session_from_napi(author, claims, &self.author_admissions)?;
        self.request_permission_advice(
            CorePermissionAdviceAction::Update {
                table,
                row: core_row_uuid_from_bytes(&row_id)?,
                patch: decode_core_cells(&patch)?,
            },
            delegated_session,
        )
    }

    #[napi(js_name = "requestDeletePermissionAdvice")]
    pub fn request_delete_permission_advice(
        &self,
        table: String,
        row_id: Uint8Array,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> napi::Result<Either<String, PendingNativePermissionAdvice>> {
        let delegated_session =
            core_delegated_session_from_napi(author, claims, &self.author_admissions)?;
        self.request_permission_advice(
            CorePermissionAdviceAction::Delete {
                table,
                row: core_row_uuid_from_bytes(&row_id)?,
            },
            delegated_session,
        )
    }

    /// Admit a verified local-first account for this backend runtime only.
    #[napi]
    pub fn admit_local_first_session(
        &self,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> napi::Result<()> {
        self.require_trusted_backend()?;
        if self.inner.borrow().is_none() {
            return Err(napi::Error::from_reason("database is closed"));
        }
        self.author_admissions.admit(CoreSelfSignedClientProof {
            token,
            app_id,
            claimed_author,
        })
    }

    fn require_trusted_backend(&self) -> napi::Result<()> {
        self.trusted_backend.then_some(()).ok_or_else(|| {
            napi::Error::from_reason("backend attribution requires an explicit backend runtime")
        })
    }
    #[napi(js_name = "insert")]
    pub fn insert_with_options(
        &self,
        table: String,
        cells: Uint8Array,
        options: Option<InsertOptions>,
    ) -> napi::Result<Write> {
        let cells = decode_core_cells(&cells)?;
        let options = core_insert_options_with_admissions(
            options,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                let write = db
                    .enqueue_insert(table, cells, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_memory(Rc::clone(db), write)
            }
            NapiDbInnerStorage::Persistent(db) => {
                let write = db
                    .enqueue_insert(table, cells, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_persistent(Rc::clone(db), write)
            }
        }
    }

    #[napi(js_name = "insertInTransaction")]
    pub fn insert_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        cells: Uint8Array,
        options: Option<InsertOptions>,
    ) -> napi::Result<Uint8Array> {
        let open_transaction_id = open_transaction_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let cells = decode_core_cells(&cells)?;
        let options = core_insert_options_with_admissions(
            options,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let row = match db {
            NapiDbInnerStorage::Memory(db) => {
                let row = db
                    .enqueue_transaction_insert(open_transaction_id, table, cells, options)
                    .map_err(napi_error)?;
                db.drive_queued_mutation_once();
                row
            }
            NapiDbInnerStorage::Persistent(db) => db
                .enqueue_transaction_insert(open_transaction_id, table, cells, options)
                .map_err(napi_error)?,
        };
        Ok(Uint8Array::new(row.to_bytes()))
    }

    #[napi(js_name = "update")]
    pub fn update_with_options(
        &self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        options: Option<UpdateOptions>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let patch = decode_core_cells(&patch)?;
        let options = core_update_options_with_admissions(
            options,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                let write = db
                    .enqueue_update(table, row_id, patch, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_memory(Rc::clone(db), write)
            }
            NapiDbInnerStorage::Persistent(db) => {
                let write = db
                    .enqueue_update(table, row_id, patch, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_persistent(Rc::clone(db), write)
            }
        }
    }

    #[napi(js_name = "updateInTransaction")]
    pub fn update_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        options: Option<UpdateOptions>,
    ) -> napi::Result<()> {
        let open_transaction_id = open_transaction_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let patch = decode_core_cells(&patch)?;
        let options = core_update_options_with_admissions(
            options,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                db.enqueue_transaction_update(open_transaction_id, table, row_id, patch, options)
                    .map_err(napi_error)?;
                db.drive_queued_mutation_once();
            }
            NapiDbInnerStorage::Persistent(db) => {
                db.enqueue_transaction_update(open_transaction_id, table, row_id, patch, options)
                    .map_err(napi_error)?;
            }
        }
        Ok(())
    }

    /// Binding-only entrypoint for typed partial-value updates. The public
    /// TypeScript API validates column-kind-specific descriptors before they
    /// reach this encoded boundary.
    #[napi(js_name = "updateLargeValues")]
    pub fn update_large_values(
        &self,
        table: String,
        row_id: Uint8Array,
        patch: Uint8Array,
        mutations: JsonValue,
        updated_at_ms: Option<f64>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let patch = decode_core_cells(&patch)?;
        let mutations: Vec<CoreLargeValueUpdate> =
            serde_json::from_value(mutations).map_err(|error| {
                napi::Error::from_reason(format!(
                    "invalid partial-value update descriptor: {error}"
                ))
            })?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        // This binding-only ABI must retain the ordinary write-option
        // timestamp contract. A direct `as u64` would turn NaN, fractions,
        // negatives, and unsafe JavaScript numbers into unrelated HLC input.
        let updated_at_ms = updated_at_ms
            .map(|value| checked_u64(value, "updatedAtMs"))
            .transpose()?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                let write = db
                    .enqueue_large_value_update(table, row_id, patch, mutations, updated_at_ms)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_memory(Rc::clone(db), write)
            }
            NapiDbInnerStorage::Persistent(db) => {
                let write = db
                    .enqueue_large_value_update(table, row_id, patch, mutations, updated_at_ms)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_persistent(Rc::clone(db), write)
            }
        }
    }

    #[napi(js_name = "upsert")]
    pub fn upsert_with_options(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        #[napi(ts_arg_type = "UpsertOptions | undefined | null")] options: Option<Unknown<'_>>,
    ) -> napi::Result<Write> {
        // Reject an obsolete JavaScript shape before inspecting mutation bytes:
        // callers should get the actionable API error, and no malformed row
        // payload can mask a Root-target compatibility violation.
        let options = core_upsert_options_with_admissions(
            parse_upsert_options(options)?,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                let write = db
                    .enqueue_upsert(table, row_id, cells, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_memory(Rc::clone(db), write)
            }
            NapiDbInnerStorage::Persistent(db) => {
                let write = db
                    .enqueue_upsert(table, row_id, cells, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_persistent(Rc::clone(db), write)
            }
        }
    }

    #[napi(js_name = "upsertInTransaction")]
    pub fn upsert_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        #[napi(ts_arg_type = "UpsertOptions | undefined | null")] options: Option<Unknown<'_>>,
    ) -> napi::Result<()> {
        let open_transaction_id = open_transaction_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let options = parse_upsert_options(options)?;
        let options = core_upsert_options_with_admissions(
            options,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                db.enqueue_transaction_upsert(open_transaction_id, table, row_id, cells, options)
                    .map_err(napi_error)?;
                db.drive_queued_mutation_once();
            }
            NapiDbInnerStorage::Persistent(db) => {
                db.enqueue_transaction_upsert(open_transaction_id, table, row_id, cells, options)
                    .map_err(napi_error)?;
            }
        }
        Ok(())
    }

    #[napi(js_name = "delete")]
    pub fn delete_with_options(
        &self,
        table: String,
        row_id: Uint8Array,
        options: Option<DeleteOptions>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let options = core_delete_options_with_admissions(
            options,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                let write = db
                    .enqueue_delete(table, row_id, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_memory(Rc::clone(db), write)
            }
            NapiDbInnerStorage::Persistent(db) => {
                let write = db
                    .enqueue_delete(table, row_id, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_persistent(Rc::clone(db), write)
            }
        }
    }

    #[napi(js_name = "deleteInTransaction")]
    pub fn delete_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        options: Option<DeleteOptions>,
    ) -> napi::Result<()> {
        let open_transaction_id = open_transaction_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let options = core_delete_options_with_admissions(
            options,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                db.enqueue_transaction_delete(open_transaction_id, table, row_id, options)
                    .map_err(napi_error)?;
                db.drive_queued_mutation_once();
            }
            NapiDbInnerStorage::Persistent(db) => {
                db.enqueue_transaction_delete(open_transaction_id, table, row_id, options)
                    .map_err(napi_error)?;
            }
        }
        Ok(())
    }

    #[napi(js_name = "restore")]
    pub fn restore_with_options(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Option<Uint8Array>,
        options: Option<RestoreOptions>,
    ) -> napi::Result<Write> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = cells.map(|cells| decode_core_cells(&cells)).transpose()?;
        let options = core_restore_options_with_admissions(
            options,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                let write = db
                    .enqueue_restore(table, row_id, cells, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_memory(Rc::clone(db), write)
            }
            NapiDbInnerStorage::Persistent(db) => {
                let write = db
                    .enqueue_restore(table, row_id, cells, options)
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?;
                core_drive_direct_mutation_once(db, &write)?;
                core_write_persistent(Rc::clone(db), write)
            }
        }
    }

    #[napi(js_name = "restoreInTransaction")]
    pub fn restore_in_transaction(
        &self,
        open_transaction_id: String,
        table: String,
        row_id: Uint8Array,
        cells: Option<Uint8Array>,
        options: Option<RestoreOptions>,
    ) -> napi::Result<()> {
        let open_transaction_id = open_transaction_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = cells.map(|cells| decode_core_cells(&cells)).transpose()?;
        let options = core_restore_options_with_admissions(
            options,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                db.enqueue_transaction_restore(open_transaction_id, table, row_id, cells, options)
                    .map_err(napi_error)?;
                db.drive_queued_mutation_once();
            }
            NapiDbInnerStorage::Persistent(db) => {
                db.enqueue_transaction_restore(open_transaction_id, table, row_id, cells, options)
                    .map_err(napi_error)?;
            }
        }
        Ok(())
    }

    #[napi(js_name = "beginStreamingMutation")]
    #[allow(clippy::too_many_arguments)] // Flat arguments are the generated NAPI ABI.
    pub fn begin_streaming_mutation(
        &self,
        table: String,
        row_id: Uint8Array,
        cells: Uint8Array,
        column: String,
        mutation: Option<String>,
        author: Option<Uint8Array>,
        attribution: Option<Uint8Array>,
        updated_at_ms: Option<f64>,
        head: Option<JsonValue>,
        base: Option<JsonValue>,
    ) -> napi::Result<StreamingMutation> {
        if self.inner.borrow().is_none() {
            return Err(napi::Error::from_reason("database is closed"));
        }
        let mutation = match mutation.as_deref().unwrap_or("insert") {
            "insert" => CoreStreamingMutationKind::Insert,
            "update" => CoreStreamingMutationKind::Update,
            "upsert" => CoreStreamingMutationKind::Upsert,
            _ => {
                return Err(napi::Error::from_reason(
                    "streaming mutation must be insert, update, or upsert",
                ));
            }
        };
        if author.is_some() && attribution.is_some() {
            return Err(napi::Error::from_reason(
                "streaming mutation identity cannot contain both author and attribution",
            ));
        }
        if attribution.is_some() {
            self.require_trusted_backend()?;
            if head.is_some() || base.is_some() {
                return Err(napi::Error::from_reason(
                    "backend-attributed streaming mutations do not support branch writes",
                ));
            }
        }
        let identity = core_write_identity(
            author,
            attribution,
            &self.author_admissions,
            self.trusted_backend,
        )?;
        let head = head.map(core_branch_selector_from_json).transpose()?;
        let base = core_branch_base_from_json(base)?;
        if base.is_some() && head.is_none() {
            return Err(napi::Error::from_reason(
                "a streaming mutation branch base requires a branch head",
            ));
        }
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let cells = decode_core_cells(&cells)?;
        let upload = {
            let db = self.inner.borrow();
            let db = db
                .as_ref()
                .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
            match db {
                NapiDbInnerStorage::Memory(db) => {
                    db.begin_streaming_value_upload(&table, &cells, &column)
                }
                NapiDbInnerStorage::Persistent(db) => {
                    db.begin_streaming_value_upload(&table, &cells, &column)
                }
            }
            .map_err(|error| napi::Error::from_reason(error.to_string()))?
        };
        Ok(StreamingMutation {
            db: Rc::clone(&self.inner),
            table,
            row_id,
            cells: Some(cells),
            column,
            mutation,
            identity,
            updated_at_ms: updated_at_ms
                .map(|value| checked_u64(value, "updatedAtMs"))
                .transpose()?,
            head,
            base,
            upload: Some(upload),
        })
    }

    #[napi(factory, js_name = "openMemory")]
    pub fn open_memory(schema: Uint8Array, config: Uint8Array) -> napi::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let identity = core_open_identity(&config, None)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = open_core_db(
            schema,
            CoreMemoryStorage::new(&refs).expect("valid memory storage families"),
            config,
            identity,
            false,
        )?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Memory(Rc::new(db))))),
            owns_runtime: true,
            non_durable_client: Rc::new(Cell::new(false)),
            trusted_backend: false,
            author_admissions: NativeAuthorAdmissions::default(),
        })
    }

    /// Open a deliberate backend runtime. Unlike the public raw-open entrypoint,
    /// this explicit ABI derives the canonical system author.
    #[napi(factory, js_name = "openMemoryAsBackend")]
    pub fn open_memory_as_backend(schema: Uint8Array, config: Uint8Array) -> napi::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let identity = core_open_backend_identity(&config)?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = open_core_db(
            schema,
            CoreMemoryStorage::new(&refs).expect("valid memory storage families"),
            config,
            identity,
            true,
        )?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Memory(Rc::new(db))))),
            owns_runtime: true,
            non_durable_client: Rc::new(Cell::new(false)),
            trusted_backend: true,
            author_admissions: NativeAuthorAdmissions::default(),
        })
    }

    /// Open with a verified Jazz self-signed client identity. This is a
    /// separate ABI entrypoint deliberately: a new client cannot accidentally
    /// hand proof bytes to an old constructor, and an old client cannot enter
    /// the proof-bearing path.
    #[napi(factory, js_name = "openMemoryWithSelfSignedProof")]
    pub fn open_memory_with_self_signed_proof(
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> napi::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let proof = CoreSelfSignedClientProof {
            token,
            app_id,
            claimed_author,
        };
        let identity = core_open_identity(&config, Some(&proof))?;
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let db = open_core_db(
            schema,
            CoreMemoryStorage::new(&refs).expect("valid memory storage families"),
            config,
            identity,
            false,
        )?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Memory(Rc::new(db))))),
            owns_runtime: true,
            non_durable_client: Rc::new(Cell::new(false)),
            trusted_backend: false,
            author_admissions: NativeAuthorAdmissions::default(),
        })
    }

    #[napi(factory, js_name = "openPersistent")]
    pub fn open_persistent(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> napi::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let identity = core_open_identity(&config, None)?;
        let storage = open_persistent_core_storage(data_path, &schema)?;
        let db = open_core_db(schema, storage, config, identity, false)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Persistent(Rc::new(
                db,
            ))))),
            owns_runtime: true,
            non_durable_client: Rc::new(Cell::new(false)),
            trusted_backend: false,
            author_admissions: NativeAuthorAdmissions::default(),
        })
    }

    /// Open a deliberate persistent backend runtime. This is intentionally a
    /// distinct ABI from the public raw-open entrypoint.
    #[napi(factory, js_name = "openPersistentAsBackend")]
    pub fn open_persistent_as_backend(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
    ) -> napi::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let identity = core_open_backend_identity(&config)?;
        let storage = open_persistent_core_storage(data_path, &schema)?;
        let db = open_core_db(schema, storage, config, identity, true)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Persistent(Rc::new(
                db,
            ))))),
            owns_runtime: true,
            non_durable_client: Rc::new(Cell::new(false)),
            trusted_backend: true,
            author_admissions: NativeAuthorAdmissions::default(),
        })
    }

    #[napi(factory, js_name = "openPersistentWithSelfSignedProof")]
    pub fn open_persistent_with_self_signed_proof(
        data_path: String,
        schema: Uint8Array,
        config: Uint8Array,
        token: String,
        app_id: String,
        claimed_author: String,
    ) -> napi::Result<Self> {
        let (schema, config) = decode_core_open_args(&schema, &config)?;
        let proof = CoreSelfSignedClientProof {
            token,
            app_id,
            claimed_author,
        };
        let identity = core_open_identity(&config, Some(&proof))?;
        let storage = open_persistent_core_storage(data_path, &schema)?;
        let db = open_core_db(schema, storage, config, identity, false)?;
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Persistent(Rc::new(
                db,
            ))))),
            owns_runtime: true,
            non_durable_client: Rc::new(Cell::new(false)),
            trusted_backend: false,
            author_admissions: NativeAuthorAdmissions::default(),
        })
    }

    /// Register and return a typed view backed by this same runtime owner.
    #[napi(js_name = "registerSchema")]
    pub fn register_schema(&self, schema: Uint8Array) -> napi::Result<Self> {
        let schema = decode_public_schema(&schema)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let view = match db {
            NapiDbInnerStorage::Memory(db) => NapiDbInnerStorage::Memory(Rc::new(
                core_block_on(db.register_schema_view(schema))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            )),
            NapiDbInnerStorage::Persistent(db) => NapiDbInnerStorage::Persistent(Rc::new(
                core_block_on(db.register_schema_view(schema))
                    .map_err(|error| napi::Error::from_reason(error.to_string()))?,
            )),
        };
        Ok(Self {
            inner: Rc::new(RefCell::new(Some(view))),
            owns_runtime: false,
            non_durable_client: Rc::clone(&self.non_durable_client),
            trusted_backend: self.trusted_backend,
            author_admissions: self.author_admissions.clone(),
        })
    }

    /// Begin one owner-wide transaction.
    #[napi(js_name = "beginTransaction")]
    pub fn begin_transaction(
        &self,
        open_transaction_id: String,
        kind: String,
        author: Option<Uint8Array>,
        attribution: Option<Uint8Array>,
    ) -> napi::Result<()> {
        let open_transaction_id = open_transaction_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let author = author
            .as_deref()
            .map(|author| self.author_admissions.resolve(author))
            .transpose()?;
        let attribution = attribution
            .as_deref()
            .map(|author| self.author_admissions.resolve(author))
            .transpose()?;
        if attribution.is_some() {
            self.require_trusted_backend()?;
            if author.is_some() {
                return Err(napi::Error::from_reason(
                    "backend-attributed transactions cannot override backend admission identity",
                ));
            }
        }
        if kind != "mergeable" && kind != "exclusive" {
            return Err(napi::Error::from_reason(unknown_transaction_kind_message(
                &kind,
            )));
        }
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        macro_rules! begin {
            ($db:expr, $drive:expr) => {{
                let result = if kind == "mergeable" {
                    $db.enqueue_begin_mergeable(open_transaction_id, author, attribution)
                } else {
                    $db.enqueue_begin_exclusive(open_transaction_id, author, attribution)
                };
                if result.is_ok() && $drive {
                    $db.drive_queued_mutation_once();
                }
                result
            }};
        }
        let result = match db {
            NapiDbInnerStorage::Memory(db) => begin!(db, true),
            NapiDbInnerStorage::Persistent(db) => begin!(db, false),
        };
        result.map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(())
    }

    /// Commit an owner-wide transaction by id and optional kind.
    #[napi(js_name = "commitTransaction")]
    pub fn commit_transaction(
        &self,
        open_transaction_id: String,
        kind: Option<String>,
    ) -> napi::Result<Write> {
        let open_transaction_id = open_transaction_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match (db, kind.as_deref().unwrap_or("mergeable")) {
            (NapiDbInnerStorage::Memory(db), "mergeable") => {
                core_commit_tx_memory(db, open_transaction_id)
            }
            (NapiDbInnerStorage::Persistent(db), "mergeable") => {
                core_commit_tx_persistent(db, open_transaction_id)
            }
            (NapiDbInnerStorage::Memory(db), "exclusive") => {
                core_commit_exclusive_tx_memory(db, open_transaction_id)
            }
            (NapiDbInnerStorage::Persistent(db), "exclusive") => {
                core_commit_exclusive_tx_persistent(db, open_transaction_id)
            }
            (_, kind) => Err(napi::Error::from_reason(unknown_transaction_kind_message(
                kind,
            ))),
        }
    }

    /// Roll back an owner-wide open transaction by id.
    #[napi(js_name = "rollbackTransaction")]
    pub fn rollback_transaction(&self, open_transaction_id: String) -> napi::Result<()> {
        let open_transaction_id = open_transaction_id
            .parse::<CoreOpenTransactionId>()
            .map_err(napi::Error::from_reason)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.abandon_transaction_handle(open_transaction_id),
            NapiDbInnerStorage::Persistent(db) => {
                db.abandon_transaction_handle(open_transaction_id)
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(js_name = "setTickScheduler")]
    pub fn set_tick_scheduler(&self, callback: ThreadsafeFunction<String, ()>) -> napi::Result<()> {
        let scheduler = Rc::new(NapiTickScheduler {
            callback: std::sync::Arc::new(callback),
        });
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.set_tick_scheduler(Some(scheduler)),
            NapiDbInnerStorage::Persistent(db) => db.set_tick_scheduler(Some(scheduler)),
        }
        Ok(())
    }

    #[napi(
        js_name = "onMutationError",
        ts_args_type = "callback: (event: any) => void"
    )]
    pub fn on_mutation_error(
        &self,
        callback: ThreadsafeFunction<JsonValue, (), JsonValue, napi::Status, false>,
    ) -> napi::Result<()> {
        let callback: CoreMutationErrorCallback = Rc::new(move |event| {
            let Ok(event) = serde_json::to_value(event) else {
                return;
            };
            let _ = callback.call(event, ThreadsafeFunctionCallMode::NonBlocking);
        });
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.on_mutation_error(Rc::clone(&callback)),
            NapiDbInnerStorage::Persistent(db) => db.on_mutation_error(Rc::clone(&callback)),
        }
        Ok(())
    }

    /// Decode, prepare, and execute one read inside Rust. Query-plan ownership
    /// never crosses the language boundary.
    #[napi]
    pub fn all(
        &self,
        query: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean; sync?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
        open_transaction_id: Option<String>,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> napi::Result<Either<Uint8Array, PendingNativeRead>> {
        let synchronous = opts
            .as_ref()
            .map(|opts| optional_json_bool_prop(opts, "sync"))
            .transpose()?
            .flatten()
            .unwrap_or(false);
        let opts = core_read_opts_from_json(opts)?;
        let open_tx = open_transaction_id
            .map(|id| id.parse::<CoreOpenTransactionId>())
            .transpose()
            .map_err(napi::Error::from_reason)?;
        let explicit_author = author
            .map(|author| self.author_admissions.resolve(&author))
            .transpose()?;
        let admission = explicit_author
            .map(|author| Ok::<_, napi::Error>((author, core_claims_from_json(author, claims)?)))
            .transpose()?;
        let author = match explicit_author {
            Some(author) => Some(author),
            None if self.trusted_backend => Some(CoreAuthorSubject::SYSTEM),
            None => None,
        };
        let non_durable_client = self.non_durable_client.get();
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        macro_rules! read {
            ($db:expr) => {{
                let db = Rc::clone($db);
                let release_db = Rc::clone(&db);
                let preceding_writes =
                    (!synchronous && open_tx.is_none()).then(|| db.queued_mutation_barrier());
                let future = Box::pin(async move {
                    if let Some(preceding_writes) = preceding_writes {
                        preceding_writes
                            .await
                            .map_err(|_| {
                                napi::Error::from_reason(
                                    "local write ordering barrier was cancelled",
                                )
                            })?
                            .map_err(napi_error)?;
                    }
                    let requires_coverage = non_durable_client
                        || (opts.tier >= jazz::tx::DurabilityTier::Edge
                            && opts.propagation == CorePropagation::Full);
                    let coverage_deadline = Instant::now() + Duration::from_secs(15);
                    let result = db
                        .all_serialized_query(
                            &query,
                            opts,
                            open_tx,
                            admission,
                            author,
                            !synchronous && requires_coverage,
                            || Instant::now() >= coverage_deadline,
                            move |attachment| release_db.detach_query(attachment),
                        )
                        .await
                        .map_err(napi_error)?;
                    match result {
                        CoreSerializedReadResult::Rows(rows) => encode_core_rows(&rows),
                        CoreSerializedReadResult::Relation(snapshot) => {
                            encode_core_relation_snapshot(&snapshot)
                        }
                    }
                    .map(Uint8Array::new)
                    .map_err(napi_error)
                });
                native_covered_read_or_pending(future, Box::new(|| {}))
            }};
        }
        match db {
            NapiDbInnerStorage::Memory(db) => read!(db),
            NapiDbInnerStorage::Persistent(db) => read!(db),
        }
    }

    /// Bind receipt-correlation claims to this client's own admitted identity.
    #[napi(js_name = "setSessionClaims")]
    pub fn set_session_claims(
        &self,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined | null")] claims: Option<
            JsonValue,
        >,
    ) -> napi::Result<()> {
        let inner = self.inner.borrow();
        let db = inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let author = match db {
            NapiDbInnerStorage::Memory(db) => db.identity().author,
            NapiDbInnerStorage::Persistent(db) => db.identity().author,
        };
        let claims = core_claims_from_json(author, claims)?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.set_identity_claims(author, claims),
            NapiDbInnerStorage::Persistent(db) => db.set_identity_claims(author, claims),
        }
        Ok(())
    }

    /// Set ambient claims for mutation and other explicitly serialized
    /// identity operations. Prepared queries capture scoped identity and
    /// claims at preparation time, so concurrent reads do not consult this
    /// shared map.
    #[napi(js_name = "setIdentityClaims")]
    pub fn set_identity_claims(
        &self,
        author: Uint8Array,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined | null")] claims: Option<
            JsonValue,
        >,
    ) -> napi::Result<()> {
        let author = self.author_admissions.resolve(&author)?;
        let claims = core_claims_from_json(author, claims)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.set_identity_claims(author, claims),
            NapiDbInnerStorage::Persistent(db) => db.set_identity_claims(author, claims),
        }
        Ok(())
    }

    #[napi(js_name = "localCurrentRow")]
    pub fn local_current_row(&self, table: String, row_id: Uint8Array) -> napi::Result<Uint8Array> {
        let row_id = core_row_uuid_from_bytes(&row_id)?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let row = match db {
            NapiDbInnerStorage::Memory(db) => core_block_on(db.local_current_row(&table, row_id)),
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.local_current_row(&table, row_id))
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        let rows = row.into_iter().collect::<Vec<_>>();
        encode_core_rows(&rows)
            .map(Uint8Array::new)
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    #[napi(ts_return_type = "Subscription | PendingNativeSubscription")]
    pub fn subscribe(
        &self,
        query: Uint8Array,
        #[napi(
            ts_arg_type = "{ tier?: string; local_updates?: string; propagation?: string; include_deleted?: boolean } | undefined | null"
        )]
        opts: Option<JsonValue>,
        author: Option<Uint8Array>,
        claims: Option<JsonValue>,
    ) -> napi::Result<Either<Subscription, PendingNativeSubscription>> {
        let opts = core_read_opts_from_json(opts)?;
        let trusted_client = self.trusted_backend;
        let explicit_author = author
            .map(|author| self.author_admissions.resolve(&author))
            .transpose()?;
        let admission = explicit_author
            .map(|author| Ok::<_, napi::Error>((author, core_claims_from_json(author, claims)?)))
            .transpose()?;
        let author = match explicit_author {
            Some(author) => Some(author),
            None if self.trusted_backend => Some(CoreAuthorSubject::SYSTEM),
            None => None,
        };
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        macro_rules! subscribe {
            ($db:expr, $variant:ident) => {{
                let db = Rc::clone($db);
                let pending = PendingNativeSubscription {
                    wake: RefCell::new(None),
                    future: RefCell::new(Some(Box::pin(async move {
                        let authorization = match author {
                            Some(author) if trusted_client => {
                                CoreSerializedSubscriptionAuthorization::TrustedClient(author)
                            }
                            Some(author) => {
                                CoreSerializedSubscriptionAuthorization::TrustedServing(author)
                            }
                            None => CoreSerializedSubscriptionAuthorization::ClientLocal,
                        };
                        let stream = db
                            .subscribe_serialized_query(&query, opts, admission, authorization)
                            .await
                            .map_err(napi_error)?;
                        Ok(Subscription {
                            inner: Some(NapiSubscription::$variant {
                                db,
                                stream,
                                pending_events: VecDeque::new(),
                                pending_batch: None,
                            }),
                        })
                    }))),
                };
                match pending.poll()? {
                    Some(subscription) => Ok(Either::A(subscription)),
                    None => Ok(Either::B(pending)),
                }
            }};
        }
        match db {
            NapiDbInnerStorage::Memory(db) => subscribe!(db, Memory),
            NapiDbInnerStorage::Persistent(db) => subscribe!(db, Persistent),
        }
    }

    #[napi]
    pub fn tick(&self) -> napi::Result<()> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let completed = match db {
            NapiDbInnerStorage::Memory(db) => core_poll_once(db.tick()),
            NapiDbInnerStorage::Persistent(db) => core_poll_once(db.tick()),
        };
        completed
            .unwrap_or(Ok(()))
            .map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Configure Jazz-owned upload ingress and unpublished-tree expiry limits.
    #[napi(js_name = "setLargeValueStagingPolicy")]
    pub fn set_large_value_staging_policy(
        &self,
        incoming_bytes_per_window: f64,
        window_ms: f64,
        max_age_ms: Option<f64>,
    ) -> napi::Result<()> {
        let incoming_bytes_per_window =
            checked_u64(incoming_bytes_per_window, "incomingBytesPerWindow")?;
        let window_ms = checked_u64(window_ms, "windowMs")?;
        if window_ms < 1 {
            return Err(napi::Error::from_reason("windowMs must be at least 1"));
        }
        let max_age_ms = max_age_ms
            .map(|value| checked_u64(value, "maxAgeMs"))
            .transpose()?
            .unwrap_or(jazz::node::LargeValueStagingPolicy::default().max_age_ms);
        let policy = jazz::node::LargeValueStagingPolicy {
            incoming_bytes_per_window,
            window_ms,
            max_age_ms,
        };
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.set_large_value_staging_policy(policy),
            NapiDbInnerStorage::Persistent(db) => db.set_large_value_staging_policy(policy),
        }
        Ok(())
    }

    /// Run one idempotent expiry pass; native hosts normally call this on a timer.
    #[napi(js_name = "evictExpiredStagedLargeValues")]
    pub fn evict_expired_staged_large_values(&self) -> napi::Result<u32> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let evicted = match db {
            NapiDbInnerStorage::Memory(db) => core_block_on(db.evict_expired_staged_large_values()),
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.evict_expired_staged_large_values())
            }
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(evicted.try_into().unwrap_or(u32::MAX))
    }

    #[napi(js_name = "setNonDurableClient")]
    pub fn set_non_durable_client(&self) -> napi::Result<()> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => db.set_non_durable_client(),
            NapiDbInnerStorage::Persistent(db) => db.set_non_durable_client(),
        }
        self.non_durable_client.set(true);
        Ok(())
    }

    #[napi(js_name = "connectUpstream")]
    pub fn connect_upstream(&self) -> napi::Result<Transport> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let queues = WireQueues::default();
        // The JS WebSocket carrier has no authenticated endpoint context for
        // scoped receipt/view frames. Keep this upstream transport aligned
        // with its authority-unbound Hello until such a context is plumbed.
        let transport = Box::new(
            CoreWireTransportAdapter::new_with_session_context_and_delegated_sessions(
                NapiWireTransport {
                    queues: queues.clone(),
                },
                jazz::wire::WIRE_PROTOCOL_VERSION,
                jazz::wire::current_wire_features()
                    & !(jazz::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
                        | jazz::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS),
                None,
                None,
                self.trusted_backend,
            ),
        );
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => NapiTransportInner::Memory {
                db: Rc::clone(db),
                connection: Some(jazz::db::block_on(db.connect_upstream(transport))),
            },
            NapiDbInnerStorage::Persistent(db) => NapiTransportInner::Persistent {
                db: Rc::clone(db),
                connection: Some(jazz::db::block_on(db.connect_upstream(transport))),
            },
        };
        let auxiliary_pump = inner.auxiliary_pump();
        Ok(Transport {
            inner,
            queues,
            auxiliary_pump,
        })
    }

    #[napi(js_name = "connectUpstreamWithSession")]
    pub fn connect_upstream_with_session(
        &self,
        protocol_version: u16,
        features: u32,
        remote_node: Buffer,
        remote_epoch: BigInt,
        local_node: Buffer,
        local_epoch: BigInt,
    ) -> napi::Result<Transport> {
        let local_features = jazz::wire::current_wire_features();
        if protocol_version != jazz::wire::WIRE_PROTOCOL_VERSION {
            return Err(napi::Error::from_reason(format!(
                "server negotiated wire protocol {protocol_version}, but this native binding supports only {}",
                jazz::wire::WIRE_PROTOCOL_VERSION
            )));
        }
        let features = features as u64;
        let unsupported = features & !local_features;
        if unsupported != 0 {
            return Err(napi::Error::from_reason(format!(
                "server negotiated wire features {features:#x}, but this native binding was not compiled with {unsupported:#x}"
            )));
        }
        if features & jazz::wire::FEATURE_SYNC_MESSAGE_PAYLOAD == 0 {
            return Err(napi::Error::from_reason(
                "server did not negotiate required sync message payload frames",
            ));
        }
        let remote_node: [u8; 16] = remote_node.as_ref().try_into().map_err(|_| {
            napi::Error::from_reason("server hello authority node must be 16 bytes")
        })?;
        let local_node: [u8; 16] = local_node
            .as_ref()
            .try_into()
            .map_err(|_| napi::Error::from_reason("local peer identity must be 16 bytes"))?;
        let remote_epoch =
            authority_epoch_from_bigint(remote_epoch, "server hello authority epoch")?;
        let local_epoch = authority_epoch_from_bigint(local_epoch, "local connection epoch")?;
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let queues = WireQueues::default();
        let session_context = CoreConnectionSessionContext {
            local: CoreWireAuthorityEndpoint {
                node: CoreNodeUuid::from_bytes(local_node),
                epoch: local_epoch,
            },
            remote: Some(CoreWireAuthorityEndpoint {
                node: CoreNodeUuid::from_bytes(remote_node),
                epoch: remote_epoch,
            }),
            link_identity: match db {
                NapiDbInnerStorage::Memory(db) => db.identity().author,
                NapiDbInnerStorage::Persistent(db) => db.identity().author,
            },
            negotiated_features: features,
        };
        let transport = Box::new(
            CoreWireTransportAdapter::new_with_session_context_and_delegated_sessions(
                NapiWireTransport {
                    queues: queues.clone(),
                },
                protocol_version,
                features,
                None,
                Some(session_context),
                self.trusted_backend,
            ),
        );
        let inner = match db {
            NapiDbInnerStorage::Memory(db) => NapiTransportInner::Memory {
                db: Rc::clone(db),
                connection: Some(jazz::db::block_on(db.connect_upstream(transport))),
            },
            NapiDbInnerStorage::Persistent(db) => NapiTransportInner::Persistent {
                db: Rc::clone(db),
                connection: Some(jazz::db::block_on(db.connect_upstream(transport))),
            },
        };
        let auxiliary_pump = inner.auxiliary_pump();
        Ok(Transport {
            inner,
            queues,
            auxiliary_pump,
        })
    }

    /// Return the originating node clock before a host releases its memory runtime.
    #[napi(js_name = "foregroundTxTimeHighWater")]
    pub fn foreground_tx_time_high_water(&self) -> napi::Result<BigInt> {
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        let value = match db {
            NapiDbInnerStorage::Memory(db) => core_block_on(db.foreground_tx_time_high_water()).0,
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.foreground_tx_time_high_water()).0
            }
        };
        Ok(BigInt::from(value))
    }

    /// Merge a checked host-retained node clock before opening new local writes.
    #[napi(js_name = "seedForegroundTxTimeHighWater")]
    pub fn seed_foreground_tx_time_high_water(&self, high_water: BigInt) -> napi::Result<()> {
        let high_water = jazz::time::TxTime(authority_epoch_from_bigint(
            high_water,
            "node clock high-water",
        )?);
        let db = self.inner.borrow();
        let db = db
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("database is closed"))?;
        match db {
            NapiDbInnerStorage::Memory(db) => {
                core_block_on(db.seed_foreground_tx_time_high_water(high_water))
            }
            NapiDbInnerStorage::Persistent(db) => {
                core_block_on(db.seed_foreground_tx_time_high_water(high_water))
            }
        }
        Ok(())
    }

    #[napi(js_name = "waitForPendingWrites")]
    pub fn wait_for_pending_writes(
        &self,
        tier: String,
    ) -> napi::Result<Either<Uint8Array, PendingNativeRead>> {
        let tier = core_durability_tier_from_str(&tier)?;
        let inner = self
            .inner
            .borrow()
            .as_ref()
            .cloned()
            .ok_or_else(|| napi::Error::from_reason("database closed"))?;
        native_read_or_pending(Box::pin(async move {
            match inner {
                NapiDbInnerStorage::Memory(db) => {
                    db.wait_for_pending_writes(tier).await.map_err(napi_error)?
                }
                NapiDbInnerStorage::Persistent(db) => {
                    db.wait_for_pending_writes(tier).await.map_err(napi_error)?
                }
            }
            Ok(Uint8Array::new(Vec::new()))
        }))
    }
    #[napi(js_name = "__closePollable", skip_typescript)]
    pub fn close(&self) -> napi::Result<Either<Uint8Array, PendingNativeRead>> {
        let inner = self.inner.borrow_mut().take();
        let owns_runtime = self.owns_runtime;
        native_read_or_pending(Box::pin(async move {
            if owns_runtime && let Some(inner) = inner {
                match inner {
                    NapiDbInnerStorage::Memory(db) => {
                        close_owned_napi_runtime(db).await?;
                    }
                    NapiDbInnerStorage::Persistent(db) => {
                        close_owned_napi_runtime(db).await?;
                    }
                }
            }
            Ok(Uint8Array::new(Vec::new()))
        }))
    }
}

async fn close_owned_napi_runtime<S>(db: Rc<CoreDb<S>>) -> napi::Result<()>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    let cleanup_db = Rc::clone(&db);
    close_after_cleanup(
        move || {
            cleanup_db.set_tick_scheduler(None);
            cleanup_db.clear_mutation_error_callback();
        },
        async move { db.close().await.map_err(napi_error) },
    )
    .await
}

async fn close_after_cleanup<F>(cleanup: impl FnOnce(), close: F) -> napi::Result<()>
where
    F: Future<Output = napi::Result<()>>,
{
    // JS resources must not remain retained merely because durable close
    // fails. Detach them before entering the fallible storage lifecycle.
    cleanup();
    close.await
}

fn unknown_transaction_kind_message(kind: &str) -> String {
    format!("unknown transaction kind {kind}")
}

fn authority_epoch_from_bigint(value: BigInt, label: &str) -> napi::Result<u64> {
    let (negative, epoch, lossless) = value.get_u64();
    if negative || !lossless {
        return Err(napi::Error::from_reason(format!(
            "{label} must be an unsigned 64-bit integer"
        )));
    }
    Ok(epoch)
}

fn decode_core_open_args(
    schema: &[u8],
    config: &[u8],
) -> napi::Result<(JazzSchema, CoreOpenDbConfig)> {
    let schema = decode_public_schema(schema)?;
    let config: CoreOpenDbConfig = postcard::from_bytes(config)
        .map_err(|error| napi::Error::from_reason(format!("decode open config: {error}")))?;
    Ok((schema, config))
}

fn decode_public_schema(schema: &[u8]) -> napi::Result<JazzSchema> {
    jazz::tools::public_schema_convert::decode_public_schema_json(schema)
        .map_err(napi::Error::from_reason)
}

/// Open the one durable store owned by a public NAPI runtime.
///
/// This is deliberately the Jazz profile rather than the adapter's generic
/// Groove-only convenience open: the runtime can persist every Jazz codec
/// family in `epoch_1_storage_codec_profile`, so its root must declare all of
/// them before any bytes are admitted.
fn open_persistent_core_storage(
    data_path: String,
    schema: &JazzSchema,
) -> napi::Result<CoreRocksDbStorage> {
    let refs = schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let codec_profile = epoch_1_storage_codec_profile()
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    CoreRocksDbStorage::open_with_durability_and_codec_profile(
        data_path,
        &refs,
        CoreRocksDbDurability::WalNoSync,
        &codec_profile,
    )
    .map_err(|error| napi::Error::from_reason(error.to_string()))
}

fn open_core_db<S>(
    schema: JazzSchema,
    storage: S,
    config: CoreOpenDbConfig,
    identity: CoreDbIdentity,
    backend_attribution: bool,
) -> napi::Result<CoreDb<S>>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    let mut db_config = CoreDbConfig::new(schema, storage, identity);
    if let Some(seed) = config.row_id_seed {
        db_config = db_config.with_id_source(CoreSeededRowIdSource::new(seed));
    }
    let initial_sync_flush_every = config.initial_sync_flush_every;
    if config.history_complete {
        let db = if backend_attribution {
            // SAFETY: only explicit NAPI backend-open constructors pass true.
            core_block_on(unsafe {
                CoreDb::open_history_complete_with_backend_attribution(db_config)
            })
        } else {
            core_block_on(CoreDb::open_history_complete(db_config))
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        configure_initial_sync_flush_cadence(&db, initial_sync_flush_every)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(db)
    } else {
        let db = if backend_attribution {
            // SAFETY: only explicit NAPI backend-open constructors pass true.
            core_block_on(unsafe { CoreDb::open_with_backend_attribution(db_config) })
        } else {
            core_block_on(CoreDb::open(db_config))
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        configure_initial_sync_flush_cadence(&db, initial_sync_flush_every)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(db)
    }
}

fn core_open_identity(
    config: &CoreOpenDbConfig,
    self_signed_client_proof: Option<&CoreSelfSignedClientProof>,
) -> napi::Result<CoreDbIdentity> {
    if let Some(proof) = self_signed_client_proof {
        let author = identity::verify_client_runtime_author(
            &proof.token,
            &proof.app_id,
            &proof.claimed_author,
        )
        .map_err(napi::Error::from_reason)?;
        return Ok(CoreDbIdentity {
            node: config.identity.node,
            author,
        });
    }
    if config.backend_credential.is_some() {
        return Err(napi::Error::from_reason(
            "ordinary NapiDb open configuration cannot carry a backend credential; use a verified identity entrypoint",
        ));
    }
    Ok(CoreDbIdentity {
        node: config.identity.node,
        author: config.identity.author,
    })
}

fn core_open_backend_identity(config: &CoreOpenDbConfig) -> napi::Result<CoreDbIdentity> {
    // Validate every caller-controlled raw field through the ordinary
    // fail-closed ingress before this separate, intentional backend ABI picks
    // the privileged system author.
    core_open_identity(config, None)?;
    Ok(CoreDbIdentity {
        node: config.identity.node,
        author: CoreAuthorSubject::SYSTEM,
    })
}

fn configure_initial_sync_flush_cadence<S>(
    db: &CoreDb<S>,
    every: Option<u32>,
) -> std::result::Result<(), jazz::db::Error>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    let Some(every) = every else {
        return Ok(());
    };
    let Some(every) = std::num::NonZeroUsize::new(every as usize) else {
        return Ok(());
    };
    db.set_initial_sync_flush_cadence(CoreInitialSyncFlushCadence::every(every))
}

fn decode_core_cells(bytes: &[u8]) -> napi::Result<CoreRowCells> {
    jazz::binding_codec::decode_named_cells(bytes)
        .map_err(|error| napi::Error::from_reason(format!("decode cells: {error}")))
}

fn core_row_uuid_from_bytes(bytes: &[u8]) -> napi::Result<CoreRowUuid> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("row id must be 16 bytes"))?;
    Ok(CoreRowUuid::from_bytes(bytes))
}

fn checked_u64(value: f64, name: &str) -> napi::Result<u64> {
    if !value.is_finite()
        || value < 0.0
        || value.fract() != 0.0
        || value > jazz::tools::policy_claims::MAX_SAFE_JS_INTEGER as f64
    {
        return Err(napi::Error::from_reason(format!(
            "{name} must be a nonnegative safe integer"
        )));
    }
    Ok(value as u64)
}

fn core_author_id_from_bytes(bytes: &[u8]) -> napi::Result<CoreAuthorSubject> {
    let canonical = std::str::from_utf8(bytes)
        .map_err(|_| napi::Error::from_reason("author subject must be canonical UTF-8 JSON"))?;
    CoreAuthorSubject::from_untrusted_canonical(canonical)
        .map_err(|error| napi::Error::from_reason(error.to_string()))
}

fn core_delegated_session_from_napi(
    author: Option<Uint8Array>,
    claims: Option<JsonValue>,
    admissions: &NativeAuthorAdmissions,
) -> napi::Result<Option<CoreDelegatedSessionBinding>> {
    author
        .map(|author| {
            let identity = admissions.resolve(&author)?;
            Ok(CoreDelegatedSessionBinding {
                claims: core_claims_from_json(identity, claims)?,
                identity,
            })
        })
        .transpose()
}

fn core_write_memory(
    db: Rc<CoreDb<CoreMemoryStorage>>,
    write: WriteHandle<CoreMemoryStorage>,
) -> napi::Result<Write> {
    let tx_id = write.mergeable_tx_id();
    let result = WriteResult {
        row_id: write.row_uuid(),
        tx_id,
    };
    Ok(Write {
        payload: postcard::to_allocvec(&result)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?,
        row_id: result.row_id,
        tx_id: TransactionId::from_committed_tx(tx_id),
        inner: Some(NapiWrite::Memory { db, write }),
    })
}

fn core_write_persistent(
    db: Rc<CoreDb<CoreRocksDbStorage>>,
    write: WriteHandle<CoreRocksDbStorage>,
) -> napi::Result<Write> {
    let tx_id = write.mergeable_tx_id();
    let result = WriteResult {
        row_id: write.row_uuid(),
        tx_id,
    };
    Ok(Write {
        payload: postcard::to_allocvec(&result)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?,
        row_id: result.row_id,
        tx_id: TransactionId::from_committed_tx(tx_id),
        inner: Some(NapiWrite::Persistent { db, write }),
    })
}

/// Let a direct mutation complete one bounded resident turn before it crosses
/// the synchronous NAPI boundary. This surfaces immediate admission failures
/// (notably a trusted-serving session lacking read access to an UPDATE/UPSERT
/// target) without waiting for storage: a genuinely async operation remains
/// queued and is observed through the returned write's normal `wait()` path.
fn core_drive_direct_mutation_once<S>(db: &CoreDb<S>, write: &WriteHandle<S>) -> napi::Result<()>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    db.drive_queued_mutation_once();
    if let Some(error) = db.take_queued_mutation_failure(write.mergeable_tx_id()) {
        return Err(napi_error(error));
    }
    Ok(())
}

fn core_claims_from_json(
    author: CoreAuthorSubject,
    claims: Option<JsonValue>,
) -> napi::Result<BTreeMap<String, CoreValue>> {
    let claims = match claims {
        None | Some(JsonValue::Null) => BTreeMap::new(),
        Some(JsonValue::Object(map)) => {
            let mut projected = BTreeMap::new();
            for (key, value) in map {
                if let Some(value) = core_claim_value_from_json(value)? {
                    projected.insert(key, value);
                }
            }
            projected
        }
        Some(_) => {
            return Err(napi::Error::from_reason(
                "identity claims must be an object",
            ));
        }
    };
    // This public NAPI ingress receives either an external canonical subject
    // or one already verified by a distinct first-party proof ABI. The shared
    // constructor namespaces raw provider values and derives reserved fields.
    Ok(jazz::tools::policy_claims::canonical_policy_binding_claims(
        &author, claims,
    ))
}

fn core_claim_value_from_json(value: JsonValue) -> napi::Result<Option<CoreValue>> {
    jazz::tools::policy_claims::json_value_to_policy_claim(
        value,
        jazz::tools::policy_claims::NumericClaimOrigin::JavaScript,
    )
    .map_err(napi::Error::from_reason)
}

fn core_tick_connection<S>(
    connection: &Option<Rc<LocalMutex<CorePeerConnection<S>>>>,
) -> napi::Result<u32>
where
    S: CoreOrderedKvStorage + CoreReopenableStorage + 'static,
{
    let Some(connection) = connection else {
        return Ok(0);
    };
    let mut connection = core_block_on(connection.lock());
    let Some(stats) = core_poll_once(connection.tick()) else {
        return Ok(0);
    };
    let stats = stats.map_err(|error| napi::Error::from_reason(error.to_string()))?;
    Ok(stats.subscription_events as u32)
}

fn core_poll_once<F: Future>(future: F) -> Option<F::Output> {
    let mut future = Box::pin(future);
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

fn core_write_state_to_json(state: &jazz::db::WriteState) -> serde_json::Value {
    serde_json::to_value(state).unwrap_or_else(|_| serde_json::json!({}))
}

fn resolve_raw_promise(env: sys::napi_env, deferred: sys::napi_deferred) {
    let mut undefined = std::ptr::null_mut();
    let status = unsafe { sys::napi_get_undefined(env, &mut undefined) };
    if status == sys::Status::napi_ok {
        let _ = unsafe { sys::napi_resolve_deferred(env, deferred, undefined) };
    }
}

fn finish_wait_promise(
    env: sys::napi_env,
    deferred: sys::napi_deferred,
    result: std::result::Result<TxId, jazz::db::Error>,
) {
    finish_immediate_promise(env, deferred, result.map(|_| ()))
}

fn finish_immediate_promise(
    env: sys::napi_env,
    deferred: sys::napi_deferred,
    result: std::result::Result<(), jazz::db::Error>,
) {
    let Err(error) = result else {
        resolve_raw_promise(env, deferred);
        return;
    };
    let message = error.to_string();
    let mut js_message = std::ptr::null_mut();
    let status = unsafe {
        sys::napi_create_string_utf8(
            env,
            message.as_ptr().cast(),
            message.len() as isize,
            &mut js_message,
        )
    };
    if status != sys::Status::napi_ok {
        return;
    }
    let mut js_error = std::ptr::null_mut();
    let status =
        unsafe { sys::napi_create_error(env, std::ptr::null_mut(), js_message, &mut js_error) };
    let rejection = if status == sys::Status::napi_ok {
        js_error
    } else {
        js_message
    };
    let _ = unsafe { sys::napi_reject_deferred(env, deferred, rejection) };
}

fn commit_timestamp_ms() -> napi::Result<u64> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|_| napi::Error::from_reason("commit clock precedes Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| napi::Error::from_reason("commit clock exceeds u64 milliseconds"))
}

fn core_commit_tx_memory(
    db: &Rc<CoreDb<CoreMemoryStorage>>,
    open_tx: CoreOpenTransactionId,
) -> napi::Result<Write> {
    let write = db
        .enqueue_commit_mergeable_handle_at_ms(open_tx, commit_timestamp_ms()?)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    db.drive_queued_mutation_once();
    core_write_memory(Rc::clone(db), write)
}

fn core_commit_tx_persistent(
    db: &Rc<CoreDb<CoreRocksDbStorage>>,
    open_tx: CoreOpenTransactionId,
) -> napi::Result<Write> {
    let write = db
        .enqueue_commit_mergeable_handle_at_ms(open_tx, commit_timestamp_ms()?)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    core_write_persistent(Rc::clone(db), write)
}

fn core_commit_exclusive_tx_memory(
    db: &Rc<CoreDb<CoreMemoryStorage>>,
    open_tx: CoreOpenTransactionId,
) -> napi::Result<Write> {
    let write = db
        .enqueue_commit_exclusive_handle_at_ms(open_tx, commit_timestamp_ms()?)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    db.drive_queued_mutation_once();
    core_write_memory(Rc::clone(db), write)
}

fn core_commit_exclusive_tx_persistent(
    db: &Rc<CoreDb<CoreRocksDbStorage>>,
    open_tx: CoreOpenTransactionId,
) -> napi::Result<Write> {
    let write = db
        .enqueue_commit_exclusive_handle_at_ms(open_tx, commit_timestamp_ms()?)
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
    core_write_persistent(Rc::clone(db), write)
}

fn core_read_opts_from_json(value: Option<JsonValue>) -> napi::Result<CoreReadOpts> {
    let mut opts = CoreReadOpts::default();
    let Some(value) = value else {
        return Ok(opts);
    };
    if value.is_null() {
        return Ok(opts);
    }
    if let Some(tier) = optional_json_string_prop(&value, "tier")? {
        opts.tier = core_read_tier_from_str(&tier)?;
    }
    if let Some(local_updates) = optional_json_string_prop(&value, "local_updates")? {
        opts.local_updates = match local_updates.as_str() {
            "Immediate" | "immediate" => CoreLocalUpdates::Immediate,
            "Deferred" | "deferred" => CoreLocalUpdates::Deferred,
            other => {
                return Err(napi::Error::from_reason(format!(
                    "unknown local_updates {other}"
                )));
            }
        };
    }
    if let Some(propagation) = optional_json_string_prop(&value, "propagation")? {
        opts.propagation = match propagation.as_str() {
            "Full" | "full" => CorePropagation::Full,
            "LocalOnly" | "local_only" | "localOnly" | "local-only" => CorePropagation::LocalOnly,
            other => {
                return Err(napi::Error::from_reason(format!(
                    "unknown propagation {other}"
                )));
            }
        };
    }
    if let Some(include_deleted) = optional_json_bool_prop(&value, "include_deleted")? {
        opts.include_deleted = include_deleted;
    }
    if let Some(read_view) = value
        .get("read_view")
        .or_else(|| value.get("readView"))
        .filter(|read_view| !read_view.is_null())
    {
        opts.read_view = serde_json::from_value::<CoreReadViewSpec>(read_view.clone())
            .map_err(|error| napi::Error::from_reason(format!("invalid read_view: {error}")))?;
    }
    Ok(opts)
}

fn core_branch_selector_from_json(value: JsonValue) -> napi::Result<CoreBranchSelector> {
    serde_json::from_value(value)
        .map_err(|error| napi::Error::from_reason(format!("invalid branch selector: {error}")))
}

fn core_branch_base_from_json(
    value: Option<JsonValue>,
) -> napi::Result<Option<CoreBranchViewBase>> {
    value
        .filter(|value| !value.is_null())
        .map(|value| {
            serde_json::from_value(value).map_err(|error| {
                napi::Error::from_reason(format!("invalid branch view base: {error}"))
            })
        })
        .transpose()
}

fn core_write_identity(
    author: Option<Uint8Array>,
    attribution: Option<Uint8Array>,
    admissions: &NativeAuthorAdmissions,
    trusted_backend: bool,
) -> napi::Result<jazz::db::WriteIdentity> {
    match (author, attribution) {
        (Some(_), Some(_)) => Err(napi::Error::from_reason(
            "write identity cannot contain both author and attribution",
        )),
        (Some(author), None) => admissions
            .resolve(&author)
            .map(jazz::db::WriteIdentity::Session),
        (None, Some(_)) if !trusted_backend => Err(napi::Error::from_reason(
            "backend attribution requires an explicit backend runtime",
        )),
        (None, Some(attribution)) => admissions
            .resolve(&attribution)
            .map(jazz::db::WriteIdentity::Attribution),
        (None, None) => Ok(jazz::db::WriteIdentity::Database),
    }
}

fn core_insert_options_with_admissions(
    options: Option<InsertOptions>,
    admissions: &NativeAuthorAdmissions,
    trusted_backend: bool,
) -> napi::Result<jazz::db::InsertOptions> {
    let Some(options) = options else {
        return Ok(Default::default());
    };
    Ok(jazz::db::InsertOptions {
        row_id: options
            .row_id
            .map(|row_id| core_row_uuid_from_bytes(&row_id))
            .transpose()?,
        identity: core_write_identity(
            options.author,
            options.attribution,
            admissions,
            trusted_backend,
        )?,
        target: options
            .branch
            .map(core_branch_selector_from_json)
            .transpose()?
            .map(jazz::db::ExactWriteTarget::Branch)
            .unwrap_or_default(),
        updated_at_ms: options
            .updated_at_ms
            .map(|value| checked_u64(value, "updatedAtMs"))
            .transpose()?,
    })
}

#[cfg(test)]
fn core_insert_options(options: Option<InsertOptions>) -> napi::Result<jazz::db::InsertOptions> {
    core_insert_options_with_admissions(options, &NativeAuthorAdmissions::default(), false)
}

fn core_update_options_with_admissions(
    options: Option<UpdateOptions>,
    admissions: &NativeAuthorAdmissions,
    trusted_backend: bool,
) -> napi::Result<jazz::db::UpdateOptions> {
    let Some(options) = options else {
        return Ok(Default::default());
    };
    let target = match options.head {
        Some(head) => jazz::db::WriteTarget::BranchView {
            head: core_branch_selector_from_json(head)?,
            base: core_branch_base_from_json(options.base)?,
        },
        None if options.base.is_none() => Default::default(),
        None => {
            return Err(napi::Error::from_reason(
                "branch view base requires a head selector",
            ));
        }
    };
    Ok(jazz::db::UpdateOptions {
        identity: core_write_identity(
            options.author,
            options.attribution,
            admissions,
            trusted_backend,
        )?,
        target,
        updated_at_ms: options
            .updated_at_ms
            .map(|value| checked_u64(value, "updatedAtMs"))
            .transpose()?,
    })
}

#[cfg(test)]
fn core_update_options(options: Option<UpdateOptions>) -> napi::Result<jazz::db::UpdateOptions> {
    core_update_options_with_admissions(options, &NativeAuthorAdmissions::default(), false)
}

/// Parse upsert options without erasing whether the removed `branch` key was
/// supplied. `has_named_property` follows JavaScript's normal prototype and
/// Proxy `has` semantics, while deliberately avoiding a getter for the
/// removed property. A throwing Proxy trap remains a binding error rather than
/// selecting Root.
fn parse_upsert_options(options: Option<Unknown<'_>>) -> napi::Result<Option<ParsedUpsertOptions>> {
    let Some(options) = options else {
        return Ok(None);
    };
    if options.get_type()? != ValueType::Object {
        return Err(napi::Error::from_reason("upsert options must be an object"));
    }
    let object = Object::from_raw(options.value().env, options.value().value);
    Ok(Some(ParsedUpsertOptions {
        author: object.get_named_property_unchecked("author")?,
        attribution: object.get_named_property_unchecked("attribution")?,
        head: object.get_named_property_unchecked("head")?,
        base: object.get_named_property_unchecked("base")?,
        branch_present: object.has_named_property("branch")?,
        updated_at_ms: object.get_named_property_unchecked("updatedAtMs")?,
    }))
}

fn core_upsert_options_with_admissions(
    options: Option<ParsedUpsertOptions>,
    admissions: &NativeAuthorAdmissions,
    trusted_backend: bool,
) -> napi::Result<jazz::db::UpsertOptions> {
    let Some(options) = options else {
        return Ok(Default::default());
    };
    if options.branch_present {
        return Err(napi::Error::from_reason(
            "upsert option `branch` is not supported; use `head` (and optional `base`) for a branch view",
        ));
    }
    let target = match (options.head, options.base) {
        (Some(head), base) => jazz::db::WriteTarget::BranchView {
            head: core_branch_selector_from_json(head)?,
            base: core_branch_base_from_json(base)?,
        },
        (None, None) => Default::default(),
        (None, Some(_)) => {
            return Err(napi::Error::from_reason(
                "branch view base requires a head selector",
            ));
        }
    };
    Ok(jazz::db::UpsertOptions {
        identity: core_write_identity(
            options.author,
            options.attribution,
            admissions,
            trusted_backend,
        )?,
        target,
        updated_at_ms: options
            .updated_at_ms
            .map(|value| checked_u64(value, "updatedAtMs"))
            .transpose()?,
    })
}

#[cfg(test)]
fn core_upsert_options(
    options: Option<ParsedUpsertOptions>,
) -> napi::Result<jazz::db::UpsertOptions> {
    core_upsert_options_with_admissions(options, &NativeAuthorAdmissions::default(), false)
}

fn core_delete_options_with_admissions(
    options: Option<DeleteOptions>,
    admissions: &NativeAuthorAdmissions,
    trusted_backend: bool,
) -> napi::Result<jazz::db::DeleteOptions> {
    let options = options.map(|options| UpdateOptions {
        author: options.author,
        attribution: options.attribution,
        head: options.head,
        base: options.base,
        updated_at_ms: options.updated_at_ms,
    });
    let options = core_update_options_with_admissions(options, admissions, trusted_backend)?;
    Ok(jazz::db::DeleteOptions {
        identity: options.identity,
        target: options.target,
        updated_at_ms: options.updated_at_ms,
    })
}

fn core_restore_options_with_admissions(
    options: Option<RestoreOptions>,
    admissions: &NativeAuthorAdmissions,
    trusted_backend: bool,
) -> napi::Result<jazz::db::RestoreOptions> {
    let Some(options) = options else {
        return Ok(Default::default());
    };
    Ok(jazz::db::RestoreOptions {
        identity: core_write_identity(
            options.author,
            options.attribution,
            admissions,
            trusted_backend,
        )?,
        target: options
            .branch
            .map(core_branch_selector_from_json)
            .transpose()?
            .map(jazz::db::ExactWriteTarget::Branch)
            .unwrap_or_default(),
        updated_at_ms: options
            .updated_at_ms
            .map(|value| checked_u64(value, "updatedAtMs"))
            .transpose()?,
    })
}

#[cfg(test)]
fn core_restore_options(options: Option<RestoreOptions>) -> napi::Result<jazz::db::RestoreOptions> {
    core_restore_options_with_admissions(options, &NativeAuthorAdmissions::default(), false)
}

fn core_durability_tier_from_str(tier: &str) -> napi::Result<CoreDurabilityTier> {
    match tier {
        "None" | "none" => Ok(CoreDurabilityTier::None),
        "Local" | "local" => Ok(CoreDurabilityTier::Local),
        "Edge" | "edge" => Ok(CoreDurabilityTier::Edge),
        "Global" | "global" => Ok(CoreDurabilityTier::Global),
        other => Err(napi::Error::from_reason(format!(
            "unknown durability tier {other}"
        ))),
    }
}

/// Read-only binding lowering. Write waits keep the durability-tier parser so
/// `remote` names cannot accidentally become a write settlement tier.
fn core_read_tier_from_str(tier: &str) -> napi::Result<CoreDurabilityTier> {
    match tier {
        "local-first" | "LocalFirst" => Ok(CoreDurabilityTier::Local),
        // NAPI has no explicit-offline state of its own. The TypeScript
        // connection manager resolves RemoteIfPossible before the ABI call;
        // direct NAPI callers therefore retain strict remote behavior.
        "remote" | "Remote" | "remote-if-possible" | "RemoteIfPossible" => {
            Ok(CoreDurabilityTier::Edge)
        }
        _ => core_durability_tier_from_str(tier),
    }
}

fn optional_json_string_prop(value: &JsonValue, name: &str) -> napi::Result<Option<String>> {
    match value.get(name) {
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(JsonValue::Null) | None => Ok(None),
        Some(_) => Err(napi::Error::from_reason(format!("{name} must be a string"))),
    }
}

fn optional_json_bool_prop(value: &JsonValue, name: &str) -> napi::Result<Option<bool>> {
    match value.get(name) {
        Some(JsonValue::Bool(value)) => Ok(Some(*value)),
        Some(JsonValue::Null) | None => Ok(None),
        Some(_) => Err(napi::Error::from_reason(format!(
            "{name} must be a boolean"
        ))),
    }
}

fn encode_core_rows(
    rows: &[jazz::node::CurrentRow],
) -> std::result::Result<Vec<u8>, postcard::Error> {
    jazz::binding_codec::encode_rows(rows)
}

fn encode_core_relation_snapshot(
    snapshot: &jazz::node::RelationSnapshot,
) -> std::result::Result<Vec<u8>, postcard::Error> {
    jazz::binding_codec::encode_relation_snapshot(snapshot)
}

fn encode_core_subscription_delta<'a>(
    added: &'a [jazz::db::SubscriptionOutputRow],
    updated: &'a [jazz::db::SubscriptionOutputRow],
    removed: &[jazz::db::RemovedRow],
) -> std::result::Result<Vec<u8>, postcard::Error> {
    jazz::binding_codec::encode_subscription_delta(added, updated, removed)
}

fn core_subscription_event_to_napi(
    event: &CoreSubscriptionEvent,
) -> napi::Result<SubscriptionEvent> {
    match event {
        CoreSubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            terminal_operations,
            settled,
            tier,
            ..
        } => {
            let delta = encode_core_subscription_delta(added, updated, removed)
                .map_err(|error| napi::Error::from_reason(error.to_string()))?;
            let terminal_operations = terminal_operations
                .iter()
                .map(core_terminal_operation_to_napi)
                .collect::<std::result::Result<_, _>>()?;
            Ok(Either3::A(SubscriptionDeltaEvent {
                event_type: "delta".to_string(),
                reset: *reset,
                delta: Uint8Array::new(delta),
                terminal_operations,
                settled: *settled,
                tier: format!("{tier:?}"),
            }))
        }
        CoreSubscriptionEvent::Rejected { reason } => {
            let reason = match reason {
                jazz::protocol::SubscribeRejectReason::UnsupportedShapeCapability { detail } => {
                    Either4::A(SubscriptionUnsupportedShapeCapabilityReason {
                        reason_type: "UnsupportedShapeCapability".to_string(),
                        detail: detail.clone(),
                    })
                }
                // Transient: the shape is awaiting catalogue admission and may
                // yet be served. Surfaced distinctly so a caller cannot mistake
                // it for an unsupported capability, which is permanent — that
                // conflation is the bug this variant was introduced to fix.
                jazz::protocol::SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission => {
                    Either4::B(SubscriptionShapeRegistrationPendingReason {
                        reason_type: "ShapeRegistrationPendingCatalogueAdmission".to_string(),
                    })
                }
                jazz::protocol::SubscribeRejectReason::ServerFailure { code } => {
                    Either4::C(SubscriptionServerFailureReason {
                        reason_type: "ServerFailure".to_string(),
                        code: format!("{code:?}"),
                    })
                }
                jazz::protocol::SubscribeRejectReason::InvalidAuthoritySourceClosure {
                    transition,
                } => Either4::D(SubscriptionInvalidAuthoritySourceClosureReason {
                    reason_type: "InvalidAuthoritySourceClosure".to_string(),
                    transition: transition.clone(),
                }),
            };
            Ok(Either3::B(SubscriptionRejectedEvent {
                event_type: "rejected".to_string(),
                reason,
            }))
        }
        CoreSubscriptionEvent::Closed => Ok(Either3::C(SubscriptionClosedEvent {
            event_type: "closed".to_string(),
        })),
    }
}

mod test_fixture_export {
    #[cfg(debug_assertions)]
    #[allow(dead_code)]
    #[napi_derive::napi(js_name = "__testSubscriptionEvents", skip_typescript)]
    pub fn subscription_events() -> napi::Result<Vec<super::SubscriptionEvent>> {
        use jazz::db::SubscriptionEvent;
        use jazz::protocol::{SubscribeRejectReason, SubscribeServerFailureCode};

        [
            SubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::UnsupportedShapeCapability {
                    detail: "fixture unsupported shape".to_owned(),
                },
            },
            SubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
            },
            SubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::ServerFailure {
                    code: SubscribeServerFailureCode::QueryValidation,
                },
            },
            SubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::InvalidAuthoritySourceClosure {
                    transition: "fixture invalid transition".to_owned(),
                },
            },
            SubscriptionEvent::Closed,
        ]
        .iter()
        .map(super::core_subscription_event_to_napi)
        .collect()
    }
}

/// Convert terminal edits without serde_json so binary subscription deltas keep
/// their typed-array representation. Root descriptors retain the upstream
/// postcard encoding; ordered keys and edit payloads retain their number-array
/// representation for the existing TypeScript terminal consumer.
fn core_terminal_operation_to_napi(
    operation: &jazz::groove::ivm::TerminalOperation,
) -> napi::Result<SubscriptionTerminalOperation> {
    if operation.path.is_empty() {
        return Err(napi::Error::from_reason(
            "native producer emitted a root terminal operation".to_owned(),
        ));
    }
    use jazz::groove::ivm::{TerminalEdit, TerminalPathSegment};

    let path = operation
        .path
        .iter()
        .map(|segment| match segment {
            TerminalPathSegment::Collection(collection) => {
                Either::A(SubscriptionTerminalCollectionPathSegment {
                    collection: collection.clone(),
                })
            }
            TerminalPathSegment::Key(key) => Either::B(SubscriptionTerminalKeyPathSegment {
                key: terminal_bytes_to_numbers(key),
            }),
        })
        .collect();
    let edit = match &operation.edit {
        TerminalEdit::Insert { index, key, value } => Either4::A(SubscriptionTerminalInsertEdit {
            insert: SubscriptionTerminalInsert {
                index: *index as f64,
                key: terminal_bytes_to_numbers(key),
                value: terminal_bytes_to_numbers(value),
            },
        }),
        TerminalEdit::Update { key, value } => Either4::B(SubscriptionTerminalUpdateEdit {
            update: SubscriptionTerminalUpdate {
                key: terminal_bytes_to_numbers(key),
                value: terminal_bytes_to_numbers(value),
            },
        }),
        TerminalEdit::Remove { key } => Either4::C(SubscriptionTerminalRemoveEdit {
            remove: SubscriptionTerminalRemove {
                key: terminal_bytes_to_numbers(key),
            },
        }),
        TerminalEdit::Move { key, index } => Either4::D(SubscriptionTerminalMoveEdit {
            move_edit: SubscriptionTerminalMove {
                key: terminal_bytes_to_numbers(key),
                index: *index as f64,
            },
        }),
    };

    Ok(SubscriptionTerminalOperation {
        root_key: terminal_bytes_to_numbers(&operation.root_key),
        path,
        edit,
    })
}

fn terminal_bytes_to_numbers(bytes: &[u8]) -> Vec<u32> {
    bytes.iter().copied().map(u32::from).collect()
}

// ============================================================================
// TestJwtIssuer
// ============================================================================

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JazzServerStartOptions {
    app_id: String,
    port: Option<u16>,
    data_dir: Option<String>,
    in_memory: Option<bool>,
    jwks_url: Option<String>,
    jwt_issuer: Option<String>,
    jwt_audience: Option<String>,
    backend_secret: String,
    admin_secret: String,
    upstream_url: Option<String>,
    allow_local_first_auth: Option<bool>,
    telemetry_collector_url: Option<String>,
    schema: Option<Vec<u8>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestJwtForUserOptions {
    expires_in_seconds: Option<u64>,
    issuer: Option<String>,
}

fn parse_jazz_server_start_options(options: JsonValue) -> napi::Result<JazzServerStartOptions> {
    serde_json::from_value(options)
        .map_err(|error| napi::Error::from_reason(format!("Invalid JazzServer options: {error}")))
}

fn init_jazz_server_telemetry(collector_url: Option<&str>) {
    let Some(collector_url) = collector_url else {
        return;
    };
    jazz_otel::init_process_tracing_with_endpoint_once(
        "jazz-server",
        collector_url,
        &["jazz_tools=trace", "tower_http=debug"],
    );
}

#[napi]
pub struct TestJwtIssuer {
    inner: Mutex<Option<JazzTestJwtIssuer>>,
    jwks_url: String,
}

#[napi]
impl TestJwtIssuer {
    #[napi(factory, ts_return_type = "Promise<TestJwtIssuer>")]
    pub async fn start() -> napi::Result<Self> {
        let issuer = JazzTestJwtIssuer::start().await;
        let jwks_url = issuer.endpoint();
        Ok(Self {
            inner: Mutex::new(Some(issuer)),
            jwks_url,
        })
    }

    #[napi(getter, js_name = "jwksUrl")]
    pub fn jwks_url(&self) -> String {
        self.jwks_url.clone()
    }

    #[napi(getter)]
    pub fn issuer(&self) -> String {
        TEST_JWT_ISSUER.to_owned()
    }

    #[napi(getter)]
    pub fn audience(&self) -> String {
        TEST_JWT_AUDIENCE.to_owned()
    }

    #[napi(js_name = "jwtForUser")]
    pub fn jwt_for_user(
        &self,
        user_id: String,
        #[napi(ts_arg_type = "Record<string, unknown> | undefined")] claims: Option<JsonValue>,
        #[napi(ts_arg_type = "{ expiresInSeconds?: number; issuer?: string } | undefined")]
        options: Option<JsonValue>,
    ) -> napi::Result<String> {
        let claims = claims.unwrap_or_else(|| serde_json::json!({ "role": "user" }));
        let options = match options {
            None | Some(JsonValue::Null) => TestJwtForUserOptions::default(),
            Some(value) => {
                serde_json::from_value::<TestJwtForUserOptions>(value).map_err(|error| {
                    napi::Error::from_reason(format!("Invalid JWT options: {error}"))
                })?
            }
        };
        let expires_in_seconds = options.expires_in_seconds.unwrap_or(3600);

        Ok(JazzTestJwtIssuer::jwt_for_user_with_options(
            &user_id,
            claims,
            TestJwtOptions {
                expires_in: Duration::from_secs(expires_in_seconds),
                // Keep the server test helper's ordinary external-session
                // default. `None` is reserved for tests that explicitly
                // exercise an issuer-less bearer, not the NAPI omission case.
                issuer: options.issuer.or_else(|| Some(TEST_JWT_ISSUER.to_owned())),
            },
        ))
    }

    #[napi]
    pub async fn stop(&self) -> napi::Result<()> {
        self.inner
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?
            .take();
        Ok(())
    }
}

// ============================================================================
// JazzServer
// ============================================================================

#[napi]
pub struct JazzServer {
    lifecycle: Arc<JazzServerLifecycle>,
}

struct JazzServerLifecycle {
    state: Mutex<JazzServerState>,
    changed: tokio::sync::watch::Sender<()>,
}

enum JazzServerState {
    Running(Box<JazzServerInner>),
    Stopping,
    Stopped(std::result::Result<(), String>),
}

enum JazzServerInner {
    Core(CoreJazzServer),
}

#[napi]
impl JazzServer {
    #[napi(factory, ts_return_type = "Promise<JazzServer>")]
    pub async fn start(
        #[napi(
            ts_arg_type = "{ appId: string; backendSecret: string; adminSecret: string; port?: number; dataDir?: string; inMemory?: boolean; jwksUrl?: string; jwtIssuer?: string; jwtAudience?: string; allowLocalFirstAuth?: boolean; upstreamUrl?: string; telemetryCollectorUrl?: string; schema?: Buffer | Uint8Array | number[] }"
        )]
        options: JsonValue,
    ) -> napi::Result<Self> {
        let mut opts = parse_jazz_server_start_options(options)?;
        init_jazz_server_telemetry(opts.telemetry_collector_url.as_deref());

        let core_server_shell_schema = opts
            .schema
            .take()
            .map(|schema_bytes| decode_public_schema(&schema_bytes))
            .transpose()?;

        let app_id =
            AppId::from_string(&opts.app_id).unwrap_or_else(|_| AppId::from_name(&opts.app_id));

        let auth_config = AuthConfig {
            jwks_url: opts.jwks_url,
            jwt_issuer: opts.jwt_issuer,
            jwt_audience: opts.jwt_audience,
            allow_local_first_auth: opts.allow_local_first_auth.unwrap_or(true),
            backend_secret: Some(opts.backend_secret.clone()),
            admin_secret: Some(opts.admin_secret.clone()),
            ..Default::default()
        };

        let in_memory = opts.in_memory.unwrap_or(false);
        let data_dir = if in_memory {
            String::new()
        } else {
            opts.data_dir.unwrap_or_else(|| "./data".to_string())
        };

        let mut server_builder = ServerBuilder::new(app_id)
            .with_auth_config(auth_config)
            .with_native_transport_connector(std::sync::Arc::new(
                jazz_native_transport::NativeWebSocketConnector,
            ));
        if let Some(schema) = core_server_shell_schema {
            server_builder = server_builder.with_core_server_shell_schema(schema);
        }
        if let Some(upstream_url) = opts.upstream_url.clone() {
            server_builder = server_builder.with_upstream_url(upstream_url);
        }

        if in_memory {
            server_builder = server_builder.with_storage(StorageBackend::InMemory);
        } else {
            #[cfg(feature = "rocksdb")]
            {
                server_builder = server_builder
                    .with_storage_factory(std::sync::Arc::new(
                        jazz_storage_rocksdb::RocksDbStorageFactory,
                    ))
                    .with_storage(StorageBackend::Persistent {
                        path: data_dir.clone().into(),
                    });
            }
            #[cfg(not(feature = "rocksdb"))]
            {
                return Err(napi::Error::from_reason(
                    "persistent JazzServer storage requires the rocksdb feature; use inMemory for ephemeral servers"
                        .to_string(),
                ));
            }
        }

        let built = server_builder
            .build()
            .await
            .map_err(napi::Error::from_reason)?;

        let data_dir_path = std::path::PathBuf::from(&data_dir);

        let server = CoreJazzServer::from_built(
            built,
            opts.port,
            app_id,
            ServerDataDir::from_path(data_dir_path),
            opts.admin_secret.clone(),
            opts.backend_secret.clone(),
        )
        .await;

        Ok(Self::from_inner(JazzServerInner::Core(server)))
    }

    #[napi(getter, js_name = "appId")]
    pub fn app_id(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.app_id().to_string(),
        })
    }

    #[napi(getter)]
    pub fn url(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.base_url(),
        })
    }

    #[napi(getter)]
    pub fn port(&self) -> napi::Result<u16> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.port(),
        })
    }

    #[napi(getter, js_name = "dataDir")]
    pub fn data_dir(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.data_dir().to_string_lossy().into_owned(),
        })
    }

    #[napi(getter, js_name = "backendSecret")]
    pub fn backend_secret(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.backend_secret().to_string(),
        })
    }

    #[napi(getter, js_name = "adminSecret")]
    pub fn admin_secret(&self) -> napi::Result<String> {
        self.with_server(|server| match server {
            JazzServerInner::Core(server) => server.admin_secret().to_string(),
        })
    }

    #[napi]
    pub async fn stop(&self) -> napi::Result<()> {
        let mut changes = self.lifecycle.changed.subscribe();
        let shutdown_owner = {
            let mut state = self
                .lifecycle
                .state
                .lock()
                .map_err(|_| napi::Error::from_reason("lock"))?;
            match &*state {
                JazzServerState::Running(_) => {
                    let JazzServerState::Running(server) =
                        std::mem::replace(&mut *state, JazzServerState::Stopping)
                    else {
                        unreachable!("running state changed while locked")
                    };
                    Some(server)
                }
                JazzServerState::Stopping | JazzServerState::Stopped(_) => None,
            }
        };

        if let Some(server) = shutdown_owner {
            let lifecycle = Arc::clone(&self.lifecycle);
            tokio::spawn(async move {
                let result = AssertUnwindSafe(shutdown_jazz_server(*server))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err("JazzServer shutdown task panicked".to_owned()));
                let mut state = lifecycle
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *state = JazzServerState::Stopped(result);
                drop(state);
                lifecycle.changed.send_replace(());
            });
        }

        loop {
            let result = {
                let state = self
                    .lifecycle
                    .state
                    .lock()
                    .map_err(|_| napi::Error::from_reason("lock"))?;
                match &*state {
                    JazzServerState::Running(_) => {
                        return Err(napi::Error::from_reason(
                            "JazzServer shutdown state returned to running",
                        ));
                    }
                    JazzServerState::Stopping => None,
                    JazzServerState::Stopped(result) => Some(result.clone()),
                }
            };
            if let Some(result) = result {
                return result.map_err(napi::Error::from_reason);
            }
            changes.changed().await.map_err(|_| {
                napi::Error::from_reason("JazzServer shutdown state closed unexpectedly")
            })?;
        }
    }

    fn from_inner(inner: JazzServerInner) -> Self {
        let (changed, _) = tokio::sync::watch::channel(());
        Self {
            lifecycle: Arc::new(JazzServerLifecycle {
                state: Mutex::new(JazzServerState::Running(Box::new(inner))),
                changed,
            }),
        }
    }

    fn with_server<T>(&self, f: impl FnOnce(&JazzServerInner) -> T) -> napi::Result<T> {
        let state = self
            .lifecycle
            .state
            .lock()
            .map_err(|_| napi::Error::from_reason("lock"))?;
        match &*state {
            JazzServerState::Running(server) => Ok(f(server.as_ref())),
            JazzServerState::Stopping => Err(napi::Error::from_reason("JazzServer is stopping")),
            JazzServerState::Stopped(_) => {
                Err(napi::Error::from_reason("JazzServer has been stopped"))
            }
        }
    }
}

async fn shutdown_jazz_server(server: JazzServerInner) -> std::result::Result<(), String> {
    let phase = match server {
        JazzServerInner::Core(server) => server.shutdown().await,
    };
    if phase == jazz_server::ShutdownPhase::StorageClosed {
        Ok(())
    } else {
        Err(format!("JazzServer shutdown failed during phase {phase:?}"))
    }
}

// ============================================================================
// Module-level utility functions
// ============================================================================

// ============================================================================
// Identity crypto utilities
// ============================================================================

fn decode_seed_napi(seed_b64: &str) -> napi::Result<[u8; 32]> {
    let bytes = URL_SAFE_NO_PAD
        .decode(seed_b64)
        .map_err(|e| napi::Error::from_reason(format!("seed base64 decode error: {e}")))?;
    bytes
        .try_into()
        .map_err(|_| napi::Error::from_reason("seed must be exactly 32 bytes"))
}

#[napi(js_name = "mintLocalFirstToken")]
pub fn mint_local_first_token(
    seed_b64: String,
    audience: String,
    ttl_seconds: u32,
) -> napi::Result<String> {
    let seed = decode_seed_napi(&seed_b64)?;
    identity::mint_jazz_self_signed_token(
        &seed,
        identity::LOCAL_FIRST_ISSUER,
        &audience,
        ttl_seconds as u64,
    )
    .map_err(napi::Error::from_reason)
}

#[napi(object)]
pub struct VerifyTokenResult {
    pub ok: bool,
    pub id: String,
    pub error: Option<String>,
}

#[napi(js_name = "verifyLocalFirstIdentityProof")]
pub fn verify_local_first_identity_proof_napi(
    token: Option<String>,
    expected_audience: String,
) -> VerifyTokenResult {
    let token = match token {
        Some(t) if !t.is_empty() => t,
        _ => {
            return VerifyTokenResult {
                ok: false,
                id: String::new(),
                error: Some("proofToken is required".to_string()),
            };
        }
    };
    match identity::verify_jazz_self_signed_proof(&token, &expected_audience) {
        Ok(verified) => VerifyTokenResult {
            ok: true,
            id: verified.user_id,
            error: None,
        },
        Err(e) => VerifyTokenResult {
            ok: false,
            id: String::new(),
            error: Some(e),
        },
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use std::cell::Cell;
    use std::collections::{BTreeMap, VecDeque};
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};
    use std::time::Duration;

    use futures::channel::oneshot;

    use crate::{
        CoreOpenDbConfig, CoreSelfSignedClientProof, InsertOptions, JazzServer, JazzServerInner,
        NapiDb, NapiDbInnerStorage, NapiWrite, NativeAuthorAdmissions, ParsedUpsertOptions,
        PendingNativeRead, PendingNativeSubscriptionBatch, PendingSubscriptionBatchOutcome,
        PendingSubscriptionBatchPoll, RestoreOptions, UpdateOptions, authority_epoch_from_bigint,
        close_after_cleanup, core_author_id_from_bytes, core_block_on, core_claim_value_from_json,
        core_drive_direct_mutation_once, core_insert_options, core_open_backend_identity,
        core_open_identity, core_read_opts_from_json, core_read_tier_from_str,
        core_restore_options, core_subscription_event_to_napi, core_update_options,
        core_upsert_options, core_write_memory, core_write_state_to_json,
        encode_core_subscription_delta, requeue_retryable_subscription_batch,
        unknown_transaction_kind_message,
    };

    #[test]
    fn failing_close_releases_scheduler_and_mutation_callback() {
        let scheduler = Rc::new(());
        let scheduler_weak = Rc::downgrade(&scheduler);
        let callback = Rc::new(());
        let callback_weak = Rc::downgrade(&callback);
        let result = core_block_on(close_after_cleanup(
            move || {
                drop(scheduler);
                drop(callback);
            },
            async { Err(napi::Error::from_reason("injected close failure")) },
        ));
        assert!(result.is_err());
        assert!(
            scheduler_weak.upgrade().is_none(),
            "a failed storage close must not retain the JS scheduler"
        );
        assert!(
            callback_weak.upgrade().is_none(),
            "a failed storage close must not retain the JS mutation callback"
        );
    }

    fn encode_persistent_open_config(author: CoreAuthorSubject) -> Vec<u8> {
        #[derive(serde::Serialize)]
        struct EncodedIdentity {
            node: CoreNodeUuid,
            author: CoreAuthorSubject,
        }
        #[derive(serde::Serialize)]
        struct EncodedConfig {
            identity: EncodedIdentity,
            row_id_seed: Option<u64>,
            history_complete: bool,
            initial_sync_flush_every: Option<u32>,
            backend_credential: Option<String>,
        }
        postcard::to_allocvec(&EncodedConfig {
            identity: EncodedIdentity {
                node: CoreNodeUuid::from_bytes([0x71; 16]),
                author,
            },
            row_id_seed: None,
            history_complete: false,
            initial_sync_flush_every: None,
            backend_credential: None,
        })
        .expect("valid NAPI open fixture")
    }

    /// Plant the adapter's generic profile first. Each public persistent NAPI
    /// opener must reject it rather than silently treating a Jazz root as a
    /// Groove-only one. This is intentionally an internal manifest-admission
    /// receipt: a user-visible API cannot create the invalid physical root.
    fn plant_groove_only_persistent_root(path: &std::path::Path, schema_bytes: &[u8]) {
        let schema = crate::decode_public_schema(schema_bytes).expect("valid public schema");
        let column_families = schema.column_families();
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        drop(
            jazz_storage_rocksdb::RocksDbStorage::open(path, &refs)
                .expect("plant generic Groove manifest"),
        );
    }

    #[test]
    fn every_public_persistent_open_rejects_a_planted_groove_only_manifest() {
        let schema = br#"{"tables":{}}"#;
        let external_author =
            CoreAuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let config = encode_persistent_open_config(external_author);

        let ordinary_dir = tempfile::tempdir().unwrap();
        let ordinary_path = ordinary_dir.path().join("ordinary.rocksdb");
        plant_groove_only_persistent_root(&ordinary_path, schema);
        assert!(
            NapiDb::open_persistent(
                ordinary_path.to_string_lossy().into_owned(),
                Uint8Array::from(schema.to_vec()),
                Uint8Array::from(config.clone()),
            )
            .is_err(),
            "openPersistent must reject a manifest that omits Jazz codecs"
        );

        let backend_dir = tempfile::tempdir().unwrap();
        let backend_path = backend_dir.path().join("backend.rocksdb");
        plant_groove_only_persistent_root(&backend_path, schema);
        assert!(
            NapiDb::open_persistent_as_backend(
                backend_path.to_string_lossy().into_owned(),
                Uint8Array::from(schema.to_vec()),
                Uint8Array::from(config.clone()),
            )
            .is_err(),
            "openPersistentAsBackend must reject a manifest that omits Jazz codecs"
        );

        let proof_dir = tempfile::tempdir().unwrap();
        let proof_path = proof_dir.path().join("proof.rocksdb");
        plant_groove_only_persistent_root(&proof_path, schema);
        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &[0x72; 32],
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            "codec-profile-test",
            60,
        )
        .unwrap();
        let verified =
            jazz::tools::identity::verify_jazz_self_signed_proof(&token, "codec-profile-test")
                .unwrap();
        let claimed_author =
            serde_json::to_string(&(jazz::tools::identity::LOCAL_FIRST_ISSUER, verified.user_id))
                .unwrap();
        assert!(
            NapiDb::open_persistent_with_self_signed_proof(
                proof_path.to_string_lossy().into_owned(),
                Uint8Array::from(schema.to_vec()),
                Uint8Array::from(config),
                token,
                "codec-profile-test".to_owned(),
                claimed_author,
            )
            .is_err(),
            "openPersistentWithSelfSignedProof must reject a manifest that omits Jazz codecs"
        );
    }

    #[test]
    fn pending_native_read_retains_a_suspended_future_until_the_next_js_turn() {
        let (sender, receiver) = oneshot::channel::<Uint8Array>();
        let pending = PendingNativeRead::new(Box::pin(async move {
            receiver
                .await
                .map_err(|_| napi::Error::from_reason("planned sender drop"))
        }));

        assert!(
            pending.poll_once().unwrap().is_none(),
            "planted suspension is retained"
        );
        assert!(
            sender.send(Uint8Array::new(vec![7, 11])).is_ok(),
            "complete the retained future"
        );
        assert_eq!(pending.poll_once().unwrap().unwrap().to_vec(), vec![7, 11]);
        assert!(
            pending.poll_once().is_err(),
            "completed reads cannot be replayed or double-encoded"
        );
    }

    // Internal because this asserts the NAPI object's one-turn retry marker;
    // public transport topology tests separately prove that routed chunks are
    // eventually delivered. The marker is what prevents the binding from
    // dropping an otherwise retryable subscription batch between those turns.
    #[test]
    fn pending_subscription_batch_retains_retryable_marker_for_the_next_js_turn() {
        let batch = PendingNativeSubscriptionBatch::new(Box::pin(async {
            Ok(PendingSubscriptionBatchOutcome::Retryable {
                events: Vec::new(),
                retry_after_ms: 37,
            })
        }));

        assert!(matches!(
            batch.poll_once().unwrap(),
            PendingSubscriptionBatchPoll::Retryable {
                events: Some(events),
            } if events.is_empty()
        ));
        assert_eq!(batch.retry_after_ms(), Some(37));
        assert!(matches!(
            batch.poll_once().unwrap(),
            PendingSubscriptionBatchPoll::Retryable { events: None }
        ));
    }

    #[test]
    fn retryable_subscription_batch_returns_raw_events_to_the_front_in_fifo_order() {
        let mut queued = VecDeque::from([CoreSubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
        }]);
        requeue_retryable_subscription_batch(
            &mut queued,
            vec![CoreSubscriptionEvent::Closed, CoreSubscriptionEvent::Closed],
        );

        assert!(matches!(
            queued.pop_front(),
            Some(CoreSubscriptionEvent::Closed)
        ));
        assert!(matches!(
            queued.pop_front(),
            Some(CoreSubscriptionEvent::Closed)
        ));
        assert!(matches!(
            queued.pop_front(),
            Some(CoreSubscriptionEvent::Rejected {
                reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
            })
        ));
        assert!(queued.is_empty());
    }

    /// Binding read choices lower without widening the write durability parser.
    #[test]
    fn read_tier_names_lower_to_existing_core_tiers() {
        assert_eq!(
            core_read_tier_from_str("local-first").expect("local-first read tier"),
            jazz::tx::DurabilityTier::Local
        );
        assert_eq!(
            core_read_tier_from_str("remote-if-possible").expect("strict remote read tier"),
            jazz::tx::DurabilityTier::Edge
        );
        assert!(
            super::core_durability_tier_from_str("remote").is_err(),
            "write waits must not accept read-only tier names"
        );
    }

    #[test]
    fn transaction_binding_diagnostics_use_transaction_vocabulary() {
        assert_eq!(
            unknown_transaction_kind_message("invalid"),
            "unknown transaction kind invalid"
        );
    }

    async fn jazz_server_binding(
        built: jazz_server::BuiltServer,
        app_id: jazz::tools::AppId,
    ) -> JazzServer {
        let server = jazz_server::JazzServer::from_built(
            built,
            None,
            app_id,
            jazz_server::ServerDataDir::in_memory(),
            "napi-stop-test-admin".to_owned(),
            "napi-stop-test-backend".to_owned(),
        )
        .await;
        JazzServer::from_inner(JazzServerInner::Core(server))
    }

    #[tokio::test]
    async fn jazz_server_stop_shares_success_with_concurrent_and_later_callers() {
        let app_id = jazz::tools::AppId::from_name("napi-stop-success");
        let built = jazz_server::ServerBuilder::new(app_id)
            .with_storage(jazz_server::StorageBackend::InMemory)
            .build()
            .await
            .expect("build NAPI test server");
        let server = jazz_server_binding(built, app_id).await;

        let (first, second) = tokio::join!(server.stop(), server.stop());
        assert!(first.is_ok(), "shutdown owner succeeds: {first:?}");
        assert!(
            second.is_ok(),
            "concurrent waiter shares success: {second:?}"
        );
        assert!(
            server.stop().await.is_ok(),
            "later caller replays terminal success"
        );
    }

    #[tokio::test]
    async fn jazz_server_stop_finalization_outlives_the_initiating_future() {
        let app_id = jazz::tools::AppId::from_name("napi-stop-cancelled-initiator");
        let built = jazz_server::ServerBuilder::new(app_id)
            .with_storage(jazz_server::StorageBackend::InMemory)
            .build()
            .await
            .expect("build NAPI test server");
        let server = jazz_server_binding(built, app_id).await;

        // The first poll synchronously transfers Running -> Stopping and
        // launches the independently owned finalizer. Dropping this future
        // models cancellation of the Promise which initiated `stop`.
        let mut initiator = Box::pin(server.stop());
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(
            initiator.as_mut().poll(&mut context),
            Poll::Pending
        ));
        drop(initiator);

        assert!(
            server.stop().await.is_ok(),
            "waiter completes the finalization detached from its initiator"
        );
        assert!(
            server.stop().await.is_ok(),
            "later caller replays the same completed outcome"
        );
    }

    #[tokio::test]
    async fn jazz_server_stop_shares_failure_with_concurrent_and_later_callers() {
        let app_id = jazz::tools::AppId::from_name("napi-stop-failure");
        let built = jazz_server::ServerBuilder::new(app_id)
            .with_storage(jazz_server::StorageBackend::InMemory)
            .with_shutdown_timeout(Duration::from_millis(1))
            .build()
            .await
            .expect("build NAPI test server");
        let active_request = built
            .state
            .shutdown
            .try_enter_app_request()
            .expect("hold request through shutdown timeout");
        let server = jazz_server_binding(built, app_id).await;

        let (first, second) = tokio::join!(server.stop(), server.stop());
        let first = first
            .expect_err("shutdown owner reports failure")
            .reason
            .clone();
        let second = second
            .expect_err("concurrent waiter reports failure")
            .reason
            .clone();
        assert_eq!(second, first);
        assert_eq!(
            first, "JazzServer shutdown failed during phase Failed",
            "binding preserves the embedded host's terminal failure"
        );
        assert_eq!(
            server
                .stop()
                .await
                .expect_err("later caller replays failure")
                .reason
                .clone(),
            first
        );
        drop(active_request);
    }
    use groove::storage::TestStorage;
    use jazz::account_registry::AccountId;
    use jazz::db::{
        Db as CoreDb, DbConfig as CoreDbConfig, DbIdentity as CoreDbIdentity, ExclusiveTxOps,
        MergeableTxOps, Propagation as CorePropagation, SubscriptionEvent as CoreSubscriptionEvent,
    };
    use jazz::groove::ivm::{TerminalEdit, TerminalOperation, TerminalPathSegment};
    use jazz::groove::records::Value as CoreValue;
    use jazz::groove::records::{RecordDescriptor, ValueType};
    use jazz::groove::storage::MemoryStorage as CoreMemoryStorage;
    use jazz::ids::{
        AuthorSubject as CoreAuthorSubject, NodeUuid as CoreNodeUuid, RowAuthor as CoreRowAuthor,
        RowUuid as CoreRowUuid,
    };
    use jazz::protocol::{ReadViewSpec as CoreReadViewSpec, SubscribeRejectReason};
    use jazz::tools::OpenTransactionId as CoreOpenTransactionId;
    use jazz::tools::{
        ColumnType, PolicyExpr, Schema, SchemaBuilder, TableName, TablePolicies, TableSchema, Value,
    };
    use jazz::tx::DurabilityTier;
    use napi::bindgen_prelude::Uint8Array;
    use napi::bindgen_prelude::{BigInt, Either, Either3, Either4};
    use serde_json::json;
    use std::cell::RefCell;

    /// The direct NAPI mutation surface is intentionally synchronous only for
    /// its first resident admission turn. This internal receipt needs a
    /// controlled storage future because neither public NAPI storage adapter
    /// can be made to yield at one precise write boundary.
    #[test]
    fn direct_mutation_admission_polls_yielding_storage_once_and_keeps_fifo() {
        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build();
        let schema = jazz::schema::JazzSchema::new(&source)
            .expect("controlled NAPI mutation fixture schema compiles");
        let column_families = schema.column_families();
        let column_families = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let (storage, control) = TestStorage::controlled(&column_families);
        let author = CoreAuthorSubject::for_test_bytes([0xa7; 16]);
        let db = Rc::new(
            core_block_on(CoreDb::open(CoreDbConfig::new(
                schema,
                storage,
                CoreDbIdentity {
                    node: CoreNodeUuid::from_bytes([0x47; 16]),
                    author,
                },
            )))
            .expect("controlled NAPI mutation fixture opens"),
        );
        let row = CoreRowUuid::from_bytes([0x57; 16]);
        db.seed_settled_mergeable_for_bootstrap(
            "items",
            row,
            author,
            BTreeMap::from([("label".to_owned(), CoreValue::String("before".to_owned()))]),
        )
        .expect("seed existing row");

        let first = db
            .enqueue_update(
                "items".to_owned(),
                row,
                BTreeMap::from([("label".to_owned(), CoreValue::String("first".to_owned()))]),
                Default::default(),
            )
            .expect("queue first update");
        let second = db
            .enqueue_update(
                "items".to_owned(),
                row,
                BTreeMap::from([("label".to_owned(), CoreValue::String("second".to_owned()))]),
                Default::default(),
            )
            .expect("queue second update");

        let polls_before = control.total_poll_count();
        core_drive_direct_mutation_once(&db, &first)
            .expect("a yielding local write stays queued for its normal wait path");
        assert!(
            control.total_poll_count() <= polls_before + 1,
            "one admission poll must not spin on yielding storage; source preparation may yield before storage is reached"
        );
        assert_eq!(
            core_block_on(first.write_state())
                .expect("first queued write state")
                .durability,
            DurabilityTier::None,
            "a yielding first write was not completed synchronously"
        );
        assert_eq!(
            core_block_on(second.write_state())
                .expect("second queued write state")
                .durability,
            DurabilityTier::None,
            "the later queued write did not leapfrog the pending first write"
        );

        for _ in 0..32 {
            db.drive_queued_mutation_once();
            if core_block_on(first.write_state()).unwrap().durability == DurabilityTier::Local {
                break;
            }
        }
        assert_eq!(
            core_block_on(first.wait(DurabilityTier::Local)).expect("first wait resolves later"),
            first.mergeable_tx_id(),
            "the original pending operation resolves exactly once through its write handle"
        );
        assert_eq!(
            core_block_on(second.write_state())
                .expect("second remains queued after first completion")
                .durability,
            DurabilityTier::None,
            "completing the first operation still leaves the FIFO successor untouched"
        );

        for _ in 0..32 {
            db.drive_queued_mutation_once();
            if core_block_on(second.write_state()).unwrap().durability == DurabilityTier::Local {
                break;
            }
        }
        assert_eq!(
            core_block_on(second.wait(DurabilityTier::Local)).expect("second wait resolves"),
            second.mergeable_tx_id(),
            "the retained FIFO successor eventually completes normally"
        );
    }

    /// A public NAPI write owns the bounded completion target for a queued
    /// no-op. Its request id is intentionally not a durable transaction, so
    /// resolving `writeState` or `wait` through `Db::write_state(request)`
    /// would incorrectly report `NotObserved` after admission.
    #[test]
    fn napi_write_retains_queued_noop_completion_target() {
        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build();
        let schema = jazz::schema::JazzSchema::new(&source)
            .expect("NAPI no-op write fixture schema compiles");
        let families = schema.column_families();
        let families = families.iter().map(String::as_str).collect::<Vec<_>>();
        let author = CoreAuthorSubject::for_test_bytes([0xc4; 16]);
        let db = Rc::new(
            core_block_on(CoreDb::open(CoreDbConfig::new(
                schema,
                CoreMemoryStorage::new(&families).expect("valid memory storage families"),
                CoreDbIdentity {
                    node: CoreNodeUuid::from_bytes([0x54; 16]),
                    author,
                },
            )))
            .expect("NAPI no-op write fixture opens"),
        );
        let row = CoreRowUuid::from_bytes([0x64; 16]);
        let existing_tx_id = db
            .seed_settled_mergeable_for_bootstrap(
                "items",
                row,
                author,
                BTreeMap::from([("label".to_owned(), CoreValue::String("before".to_owned()))]),
            )
            .expect("seed current row");
        let queued = db
            .enqueue_update("items".to_owned(), row, BTreeMap::new(), Default::default())
            .expect("queue empty update");
        let reserved_tx_id = queued.mergeable_tx_id();
        let napi_write = core_write_memory(Rc::clone(&db), queued).expect("wrap queued write");

        let waited = Rc::new(RefCell::new(None));
        let waited_for_callback = Rc::clone(&waited);
        match napi_write
            .inner
            .as_ref()
            .expect("public write keeps inner handle")
        {
            NapiWrite::Memory { db, write } => {
                db.wait_for_write_with(write, DurabilityTier::Local, move |outcome| {
                    *waited_for_callback.borrow_mut() = Some(outcome)
                })
            }
            NapiWrite::Persistent { .. } => panic!("memory write retained wrong backend"),
        }
        db.drive_queued_mutation_once();
        core_block_on(db.tick()).expect("the scheduled binding observer receives no-op completion");

        assert_eq!(
            napi_write
                .write_state()
                .expect("NAPI write state follows target"),
            core_write_state_to_json(
                &db.write_state(existing_tx_id)
                    .expect("existing transaction remains observable"),
            ),
            "the public write follows its bounded completion target rather than a global alias"
        );
        assert_eq!(
            waited
                .borrow_mut()
                .take()
                .expect("NAPI wait callback resolves")
                .expect("no-op target is locally durable"),
            reserved_tx_id,
            "the public wait retains its synchronous request identity"
        );
        assert!(
            db.write_state(reserved_tx_id).is_err(),
            "NAPI did not reintroduce a runtime-global no-op alias"
        );
    }

    /// The sync NAPI mutation ABI makes one resident admission turn before it
    /// returns a `Write`. An operation that fails in that turn must not hand
    /// callers a seemingly committed receipt, and ordinary/partial-value
    /// entrypoints must preserve the same boundary.
    #[test]
    fn direct_insert_and_large_value_update_surface_memory_first_turn_failures() {
        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                    ),
            )
            .build();
        let schema = serde_json::to_vec(&source).expect("serialize NAPI policy fixture");
        let author = CoreAuthorSubject::for_test_bytes([0xc4; 16]);
        let db = NapiDb::open_memory(
            Uint8Array::from(schema),
            Uint8Array::from(encode_persistent_open_config(author)),
        )
        .expect("open memory NAPI fixture");
        let label_cells = |value: &str| {
            let descriptor = RecordDescriptor::new([("label", ValueType::String)]);
            let raw = descriptor
                .create(&[CoreValue::String(value.to_owned())])
                .expect("encode label fixture cell");
            Uint8Array::from(
                jazz::binding_codec::encode_named_cells(&jazz::groove::records::OwnedRecord::new(
                    raw, descriptor,
                ))
                .expect("encode named NAPI cells"),
            )
        };

        let insert_error =
            match db.insert_with_options("missing_table".to_owned(), label_cells("before"), None) {
                Ok(_) => panic!("ordinary insert must surface its first-turn failure"),
                Err(error) => error,
            };
        assert!(
            insert_error.reason.contains("missing_table"),
            "ordinary insert retains its core failure diagnostic: {}",
            insert_error.reason
        );

        let large_update_error = match db.update_large_values(
            "missing_table".to_owned(),
            Uint8Array::from(vec![0xc5; 16]),
            label_cells("after"),
            json!([]),
            None,
        ) {
            Ok(_) => panic!("partial-value entrypoint must surface its first-turn failure"),
            Err(error) => error,
        };
        assert!(
            large_update_error.reason.contains("missing_table"),
            "partial-value update retains its core failure diagnostic: {}",
            large_update_error.reason
        );
    }

    #[test]
    fn identity_claim_ingress_namespaces_provider_values_and_derives_reserved_fields() {
        let author = CoreAuthorSubject::authenticated("https://issuer.example", "alice")
            .unwrap()
            .with_account(AccountId(uuid::Uuid::from_bytes([0xa2; 16])));
        let claims = crate::core_claims_from_json(
            author,
            Some(json!({
                "user": "forged-user",
                "iss": "forged-issuer",
                "sub": "provider-subject",
                "custom": "provider-value",
                "authMode": "local-first",
            })),
        )
        .expect("NAPI claims are scalar provider data");

        assert_eq!(
            claims.get("user"),
            Some(
                &CoreRowAuthor::from_persisted_subject(author)
                    .expect("admitted row author")
                    .to_value()
            ),
            "session.user must come from the supplied structured author"
        );
        assert_eq!(
            claims.get("authMode"),
            Some(&CoreValue::String("external".to_owned())),
            "the public NAPI ingress derives external auth mode"
        );
        for (name, value) in [
            ("user", "forged-user"),
            ("custom", "provider-value"),
            ("authMode", "local-first"),
        ] {
            assert_eq!(
                claims.get(&jazz::query::provider_claim_key(name)),
                Some(&CoreValue::String(value.to_owned())),
                "raw provider {name} stays below session.claims"
            );
        }
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("iss")),
            Some(&CoreValue::String("https://issuer.example".to_owned())),
            "session.claims.iss must agree with the admitted author rather than a supplied claim"
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("sub")),
            Some(&CoreValue::String("alice".to_owned())),
            "session.claims.sub must agree with the admitted author rather than a supplied claim"
        );
    }

    #[test]
    fn identity_claim_ingress_omits_recursive_json_but_keeps_scalar_prototype_names() {
        let author = CoreAuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let claims = crate::core_claims_from_json(
            author,
            Some(json!({
                "profile": { "handler_only": true },
                "mixed": ["editor", { "nested": true }],
                "__proto__": "safe",
                "constructor": "also-safe",
            })),
        )
        .expect("recursive metadata must not reject NAPI admission");

        assert!(!claims.contains_key(&jazz::query::provider_claim_key("profile")));
        assert!(!claims.contains_key(&jazz::query::provider_claim_key("mixed")));
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("__proto__")),
            Some(&CoreValue::String("safe".to_owned()))
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("constructor")),
            Some(&CoreValue::String("also-safe".to_owned()))
        );
    }

    #[test]
    fn identity_claim_ingress_derives_first_party_auth_mode_from_verified_author() {
        let account = AccountId(uuid::Uuid::from_bytes([0xa3; 16]));
        let author = CoreAuthorSubject::from_canonical(
            &serde_json::to_string(&(
                account.0.to_string(),
                CoreAuthorSubject::LOCAL_FIRST_ISSUER,
                "alice",
            ))
            .expect("serialize canonical admitted first-party author"),
        )
        .expect("parse canonical admitted first-party author");
        let claims = crate::core_claims_from_json(author, Some(json!({ "authMode": "external" })))
            .expect("NAPI claims are scalar provider data");

        assert_eq!(
            claims.get("user"),
            Some(
                &CoreRowAuthor::from_persisted_subject(author)
                    .expect("admitted row author")
                    .to_value()
            )
        );
        assert_eq!(
            claims.get("authMode"),
            Some(&CoreValue::String("local-first".to_owned())),
            "a provider claim must not override the mode verified by the native open ABI"
        );
        assert_eq!(
            claims.get(&jazz::query::provider_claim_key("authMode")),
            Some(&CoreValue::String("external".to_owned()))
        );
    }

    #[test]
    fn public_author_ingress_requires_a_verified_self_signed_open_proof() {
        // This exercises the binding's raw postcard configuration boundary
        // directly. A normal DB operation cannot construct an invalid native
        // open envelope, so this internal receipt is required to prove that
        // reserved identity selection stays closed at that ingress.
        let external = br#"["https://issuer.example","alice"]"#;
        assert_eq!(
            core_author_id_from_bytes(external).unwrap().canonical(),
            std::str::from_utf8(external).unwrap()
        );
        for issuer in [
            CoreAuthorSubject::SYSTEM_ISSUER,
            CoreAuthorSubject::LOCAL_FIRST_ISSUER,
            CoreAuthorSubject::STATIC_BEARER_ISSUER,
            CoreAuthorSubject::ANONYMOUS_ISSUER,
        ] {
            let canonical = serde_json::to_vec(&(issuer, "caller")).unwrap();
            assert!(
                core_author_id_from_bytes(&canonical).is_err(),
                "issuer {issuer}"
            );
        }

        #[derive(serde::Serialize)]
        struct EncodedIdentity {
            node: CoreNodeUuid,
            author: CoreAuthorSubject,
        }
        #[derive(serde::Serialize)]
        struct EncodedConfig {
            identity: EncodedIdentity,
            row_id_seed: Option<u64>,
            history_complete: bool,
            initial_sync_flush_every: Option<u32>,
            backend_credential: Option<String>,
        }
        let encode_config = |author, backend_credential| {
            postcard::to_allocvec(&EncodedConfig {
                identity: EncodedIdentity {
                    node: CoreNodeUuid::from_bytes([7; 16]),
                    author,
                },
                row_id_seed: None,
                history_complete: false,
                initial_sync_flush_every: None,
                backend_credential,
            })
            .unwrap()
        };
        for issuer in [
            CoreAuthorSubject::SYSTEM_ISSUER,
            CoreAuthorSubject::LOCAL_FIRST_ISSUER,
            CoreAuthorSubject::STATIC_BEARER_ISSUER,
            CoreAuthorSubject::ANONYMOUS_ISSUER,
        ] {
            let author = if issuer == CoreAuthorSubject::SYSTEM_ISSUER {
                CoreAuthorSubject::SYSTEM
            } else {
                CoreAuthorSubject::from_canonical(
                    &serde_json::to_string(&(issuer, "caller")).unwrap(),
                )
                .unwrap()
            };
            let bytes = encode_config(author, None);
            assert!(
                postcard::from_bytes::<CoreOpenDbConfig>(&bytes).is_err(),
                "raw open author must reject {issuer} without a proof"
            );
        }

        let external_author =
            CoreAuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
        let bytes = encode_config(external_author, None);
        let external_config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        assert_eq!(
            core_open_identity(&external_config, None).unwrap().author,
            external_author,
            "an old TS caller uses only this ordinary raw-open path"
        );
        assert_ne!(
            core_open_identity(&external_config, None).unwrap().author,
            CoreAuthorSubject::SYSTEM,
            "the ordinary raw-open path cannot become SYSTEM"
        );
        assert_eq!(
            core_open_backend_identity(&external_config).unwrap().author,
            CoreAuthorSubject::SYSTEM,
            "only the separate backend-open path may intentionally derive SYSTEM"
        );
        NapiDb::open_memory_as_backend(
            Uint8Array::from(br#"{"tables":{}}"#.to_vec()),
            Uint8Array::from(encode_config(external_author, None)),
        )
        .expect(
            "an explicit local backend open derives SYSTEM without requiring upstream verification",
        );

        for credential in ["arbitrary-backend-secret", "malformed.backend.credential"] {
            let bytes = encode_config(external_author, Some(credential.to_owned()));
            let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
            assert!(
                core_open_identity(&config, None).is_err(),
                "ordinary raw config must reject {credential:?}, never promote it to SYSTEM"
            );
            assert!(
                core_open_backend_identity(&config).is_err(),
                "the explicit backend ABI must still reject legacy raw credential input"
            );
        }

        let memory_credential =
            encode_config(external_author, Some("arbitrary-backend-secret".to_owned()));
        assert!(
            NapiDb::open_memory(
                Uint8Array::from(b"{}".to_vec()),
                Uint8Array::from(memory_credential),
            )
            .is_err(),
            "openMemory must reject an unverified backend credential before opening the DB"
        );

        let persistent_path = std::env::temp_dir().join(format!(
            "jazz-unverified-backend-open-{}",
            std::process::id()
        ));
        assert!(
            !persistent_path.exists(),
            "test path must be fresh: {}",
            persistent_path.display()
        );
        let persistent_credential = encode_config(
            external_author,
            Some("malformed.backend.credential".to_owned()),
        );
        let result = NapiDb::open_persistent(
            persistent_path.to_string_lossy().into_owned(),
            Uint8Array::from(b"{}".to_vec()),
            Uint8Array::from(persistent_credential),
        );
        let created_storage = persistent_path.exists();
        if created_storage {
            std::fs::remove_dir_all(&persistent_path).unwrap();
        }
        assert!(
            result.is_err(),
            "openPersistent must reject an unverified backend credential"
        );
        assert!(
            !created_storage,
            "openPersistent must reject an unverified backend credential before creating storage"
        );

        for issuer in [
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            jazz::tools::identity::ANONYMOUS_ISSUER,
        ] {
            let token = jazz::tools::identity::mint_jazz_self_signed_token(
                &[issuer.len() as u8; 32],
                issuer,
                "proof-app",
                60,
            )
            .unwrap();
            let verified =
                jazz::tools::identity::verify_jazz_self_signed_proof(&token, "proof-app").unwrap();
            let claimed_author = serde_json::to_string(&(issuer, verified.user_id)).unwrap();
            let bytes = encode_config(external_author, None);
            let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
            let proof = CoreSelfSignedClientProof {
                token,
                app_id: "proof-app".to_owned(),
                claimed_author: claimed_author.clone(),
            };
            assert_eq!(
                core_open_identity(&config, Some(&proof))
                    .unwrap()
                    .author
                    .canonical(),
                claimed_author,
                "the separate proof constructor derives {issuer}, never backend/SYSTEM identity"
            );
            let bytes = encode_config(external_author, Some("misdecode-me".to_owned()));
            let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
            assert_eq!(
                core_open_identity(&config, Some(&proof))
                    .unwrap()
                    .author
                    .canonical(),
                claimed_author,
                "a proof-bearing constructor never treats a config field as backend authority"
            );
        }

        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &[9; 32],
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            "proof-app",
            60,
        )
        .unwrap();
        let verified =
            jazz::tools::identity::verify_jazz_self_signed_proof(&token, "proof-app").unwrap();
        let claimed_author =
            serde_json::to_string(&(jazz::tools::identity::LOCAL_FIRST_ISSUER, verified.user_id))
                .unwrap();
        let bytes = encode_config(external_author, None);
        let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        let (signing_input, signature_b64) = token.rsplit_once('.').unwrap();
        let mut signature = URL_SAFE_NO_PAD.decode(signature_b64).unwrap();
        assert_eq!(signature.len(), 64);
        signature[0] ^= 1;
        let proof = CoreSelfSignedClientProof {
            token: format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signature)),
            app_id: "proof-app".to_owned(),
            claimed_author: claimed_author.clone(),
        };
        assert!(
            core_open_identity(&config, Some(&proof)).is_err(),
            "same-length, valid-base64 signature tampering must fail"
        );
        let persistent_path = std::env::temp_dir().join(format!(
            "jazz-invalid-self-signed-open-{}",
            std::process::id()
        ));
        assert!(
            !persistent_path.exists(),
            "test path must be fresh: {}",
            persistent_path.display()
        );
        let result = NapiDb::open_persistent_with_self_signed_proof(
            persistent_path.to_string_lossy().into_owned(),
            Uint8Array::from(b"{}".to_vec()),
            Uint8Array::from(encode_config(external_author, None)),
            proof.token.clone(),
            proof.app_id.clone(),
            proof.claimed_author.clone(),
        );
        let created_storage = persistent_path.exists();
        if created_storage {
            std::fs::remove_dir_all(&persistent_path).unwrap();
        }
        assert!(
            result.is_err(),
            "invalid proof must reject the persistent open"
        );
        assert!(
            !created_storage,
            "invalid proof must be rejected before RocksDB creates its data directory"
        );

        let bytes = encode_config(external_author, None);
        let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        let proof = CoreSelfSignedClientProof {
            token: token.clone(),
            app_id: "wrong-app".to_owned(),
            claimed_author: claimed_author.clone(),
        };
        assert!(
            core_open_identity(&config, Some(&proof)).is_err(),
            "wrong audience must fail"
        );

        let bytes = encode_config(external_author, None);
        let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        let proof = CoreSelfSignedClientProof {
            token,
            app_id: "proof-app".to_owned(),
            claimed_author: r#"["urn:jazz:local-first","another-subject"]"#.to_owned(),
        };
        assert!(
            core_open_identity(&config, Some(&proof)).is_err(),
            "claimed author must match the signed issuer and subject"
        );

        let expired = jazz::tools::identity::mint_jazz_self_signed_token_at(
            &[10; 32],
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            "proof-app",
            1,
            0,
        )
        .unwrap();
        let bytes = encode_config(external_author, None);
        let config = postcard::from_bytes::<CoreOpenDbConfig>(&bytes).unwrap();
        let proof = CoreSelfSignedClientProof {
            token: expired,
            app_id: "proof-app".to_owned(),
            claimed_author,
        };
        assert!(
            core_open_identity(&config, Some(&proof)).is_err(),
            "expired proof must fail"
        );
    }

    /// The native binding mints backend attribution only from its distinct
    /// backend-open constructor. A backend can credit Alice while retaining
    /// SYSTEM admission; raw opens, transaction identity overrides, and branch
    /// streaming combinations must all fail before a write is staged.
    ///
    /// ```text
    /// openMemoryAsBackend ──attribution=alice──► accepted NAPI write
    /// openMemory / raw session ──attribution=alice──► rejected at binding edge
    /// ```
    #[test]
    fn native_backend_attribution_capability_is_explicit_and_non_mixable() {
        #[derive(serde::Serialize)]
        struct EncodedIdentity {
            node: CoreNodeUuid,
            author: CoreAuthorSubject,
        }
        #[derive(serde::Serialize)]
        struct EncodedConfig {
            identity: EncodedIdentity,
            row_id_seed: Option<u64>,
            history_complete: bool,
            initial_sync_flush_every: Option<u32>,
            backend_credential: Option<String>,
        }

        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build();
        let schema = serde_json::to_vec(&source).unwrap();
        let config = postcard::to_allocvec(&EncodedConfig {
            identity: EncodedIdentity {
                node: CoreNodeUuid::from_bytes([0xb3; 16]),
                author: CoreAuthorSubject::for_test_bytes([0xb3; 16]),
            },
            row_id_seed: Some(0xb3),
            history_complete: false,
            initial_sync_flush_every: None,
            backend_credential: None,
        })
        .unwrap();
        let alice = CoreAuthorSubject::for_test_bytes([0xa1; 16]);
        let alice_bytes = alice.canonical().as_bytes().to_vec();
        let descriptor = RecordDescriptor::new([("label", ValueType::String)]);
        let raw = descriptor
            .create(&[CoreValue::String("credited to alice".to_owned())])
            .unwrap();
        let cells = jazz::binding_codec::encode_named_cells(
            &jazz::groove::records::OwnedRecord::new(raw, descriptor),
        )
        .unwrap();

        let backend = NapiDb::open_memory_as_backend(
            Uint8Array::from(schema.clone()),
            Uint8Array::from(config.clone()),
        )
        .unwrap();
        let write = backend
            .insert_with_options(
                "items".to_owned(),
                Uint8Array::from(cells.clone()),
                Some(InsertOptions {
                    row_id: Some(Uint8Array::from(vec![0xb4; 16])),
                    author: None,
                    attribution: Some(Uint8Array::from(alice_bytes.clone())),
                    branch: None,
                    updated_at_ms: None,
                }),
            )
            .expect("the explicit backend constructor mints attribution capability");
        assert_eq!(write.row_id, CoreRowUuid::from_bytes([0xb4; 16]));

        let ordinary =
            NapiDb::open_memory(Uint8Array::from(schema), Uint8Array::from(config)).unwrap();
        let err = match ordinary.insert_with_options(
            "items".to_owned(),
            Uint8Array::from(cells),
            Some(InsertOptions {
                row_id: Some(Uint8Array::from(vec![0xb4; 16])),
                author: None,
                attribution: Some(Uint8Array::from(alice_bytes.clone())),
                branch: None,
                updated_at_ms: None,
            }),
        ) {
            Ok(_) => panic!("a raw NAPI open must never gain attribution authority"),
            Err(err) => err,
        };
        assert!(err.reason.contains("explicit backend runtime"));

        let err = ordinary
            .begin_transaction(
                CoreOpenTransactionId::new().to_string(),
                "mergeable".to_owned(),
                None,
                Some(Uint8Array::from(alice_bytes.clone())),
            )
            .expect_err("ordinary transactions cannot claim backend attribution");
        assert!(err.reason.contains("explicit backend runtime"));

        let err = match backend.begin_streaming_mutation(
            "items".to_owned(),
            Uint8Array::from(vec![0xb5; 16]),
            Uint8Array::from(Vec::new()),
            "label".to_owned(),
            None,
            Some(Uint8Array::from(alice_bytes)),
            Some(Uint8Array::from(alice.canonical().as_bytes().to_vec())),
            None,
            None,
            None,
        ) {
            Ok(_) => panic!("streaming attribution cannot override SYSTEM admission"),
            Err(err) => err,
        };
        assert!(
            err.reason
                .contains("cannot contain both author and attribution")
        );

        let err = match backend.begin_streaming_mutation(
            "items".to_owned(),
            Uint8Array::from(vec![0xb6; 16]),
            Uint8Array::from(Vec::new()),
            "label".to_owned(),
            None,
            None,
            Some(Uint8Array::from(alice.canonical().as_bytes().to_vec())),
            None,
            Some(json!({})),
            None,
        ) {
            Ok(_) => panic!("attributed streaming branch writes must fail closed"),
            Err(err) => err,
        };
        assert!(err.reason.contains("do not support branch writes"));
    }

    #[test]
    fn javascript_numeric_claims_preserve_safe_integers_and_fail_closed_when_lossy() {
        assert_eq!(
            core_claim_value_from_json(json!(7)).unwrap().unwrap(),
            CoreValue::U64(7)
        );
        assert_eq!(
            core_claim_value_from_json(json!(-7)).unwrap().unwrap(),
            CoreValue::I64(-7)
        );
        assert_eq!(
            core_claim_value_from_json(serde_json::Value::Number(
                serde_json::Number::from_f64(7.0).unwrap()
            ))
            .unwrap()
            .unwrap(),
            CoreValue::U64(7),
            "JS-number deserialization must agree with integer JSON"
        );
        assert_eq!(
            core_claim_value_from_json(json!(7.5)).unwrap().unwrap(),
            CoreValue::F64(7.5)
        );
        assert_eq!(
            core_claim_value_from_json(json!(9_007_199_254_740_992_u64))
                .unwrap()
                .unwrap(),
            CoreValue::F64(9_007_199_254_740_992.0),
            "integers beyond Number.MAX_SAFE_INTEGER must not participate in integer policy matching"
        );
        assert_eq!(
            core_claim_value_from_json(json!(-9_007_199_254_740_992_i64))
                .unwrap()
                .unwrap(),
            CoreValue::F64(-9_007_199_254_740_992.0)
        );
    }

    #[test]
    fn javascript_u64_boundaries_reject_lossy_or_invalid_numbers() {
        assert_eq!(super::checked_u64(42.0, "value").unwrap(), 42);
        for value in [
            -1.0,
            f64::NAN,
            f64::INFINITY,
            1.5,
            (jazz::tools::policy_claims::MAX_SAFE_JS_INTEGER + 1) as f64,
        ] {
            assert!(super::checked_u64(value, "value").is_err(), "{value:?}");
        }
    }

    #[test]
    fn write_option_timestamps_reject_lossy_javascript_numbers() {
        assert!(
            core_insert_options(Some(InsertOptions {
                row_id: None,
                author: None,
                attribution: None,
                branch: None,
                updated_at_ms: Some(1.5),
            }))
            .is_err()
        );
        assert!(
            core_update_options(Some(UpdateOptions {
                author: None,
                attribution: None,
                head: None,
                base: None,
                updated_at_ms: Some(f64::NAN),
            }))
            .is_err()
        );
        assert!(
            core_upsert_options(Some(ParsedUpsertOptions {
                author: None,
                attribution: None,
                head: None,
                base: None,
                branch_present: false,
                updated_at_ms: Some(-1.0),
            }))
            .is_err()
        );
        assert!(
            core_restore_options(Some(RestoreOptions {
                author: None,
                attribution: None,
                branch: None,
                updated_at_ms: Some((jazz::tools::policy_claims::MAX_SAFE_JS_INTEGER + 1) as f64),
            }))
            .is_err()
        );
    }

    #[test]
    fn javascript_upsert_rejects_removed_branch_property_by_presence() {
        let error = core_upsert_options(Some(ParsedUpsertOptions {
            author: None,
            attribution: None,
            head: None,
            base: None,
            branch_present: true,
            updated_at_ms: None,
        }))
        .expect_err("the removed branch selector must not be reinterpreted as a head");
        assert!(
            error
                .reason
                .contains("option `branch` is not supported; use `head`")
        );

        let canonical_head = serde_json::to_value(jazz::protocol::BranchSelector::new([(
            "branch",
            CoreValue::String("draft".to_owned()),
        )]))
        .expect("branch selector serializes for the binding boundary");
        let parsed = core_upsert_options(Some(ParsedUpsertOptions {
            author: None,
            attribution: None,
            head: Some(canonical_head),
            base: None,
            branch_present: false,
            updated_at_ms: None,
        }))
        .expect("the canonical head selector remains accepted");
        assert!(matches!(
            parsed.target,
            jazz::db::WriteTarget::BranchView { .. }
        ));
    }

    // The capability is binding-local, so exercise native ABI ingress directly.
    #[test]
    fn native_local_first_session_admission_is_exact_and_runtime_scoped() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("items").column("label", ColumnType::Text))
            .build();
        let schema_bytes = || Uint8Array::from(serde_json::to_vec(&schema).unwrap());
        let config = || {
            Uint8Array::from(encode_persistent_open_config(
                CoreAuthorSubject::for_test_bytes([0x92; 16]),
            ))
        };
        let backend = NapiDb::open_memory_as_backend(schema_bytes(), config()).unwrap();
        let other = NapiDb::open_memory_as_backend(schema_bytes(), config()).unwrap();
        let ordinary = NapiDb::open_memory(schema_bytes(), config()).unwrap();
        let app_id = "native-session-admission";
        let token = jazz::tools::identity::mint_jazz_self_signed_token(
            &[0x93; 32],
            jazz::tools::identity::LOCAL_FIRST_ISSUER,
            app_id,
            60,
        )
        .unwrap();
        let verified =
            jazz::tools::identity::verify_jazz_self_signed_proof(&token, app_id).unwrap();
        let app = jazz::tools::AppId::from_name(app_id);
        let account =
            jazz::account_registry::local_first_account_id(*app.uuid(), &verified.user_id);
        let author = CoreAuthorSubject::from_canonical(
            &serde_json::to_string(&(jazz::tools::identity::LOCAL_FIRST_ISSUER, &verified.user_id))
                .unwrap(),
        )
        .unwrap()
        .with_account(account);
        let canonical = author.canonical().to_owned();
        let bytes = || Uint8Array::from(canonical.as_bytes().to_vec());
        assert!(backend.set_identity_claims(bytes(), None).is_err());
        assert!(
            ordinary
                .admit_local_first_session(token.clone(), app_id.into(), canonical.clone())
                .is_err()
        );
        assert!(
            backend
                .admit_local_first_session(token.clone(), "wrong-app".into(), canonical.clone())
                .is_err()
        );
        assert!(backend.set_identity_claims(bytes(), None).is_err());
        let wrong_account = author.with_account(jazz::account_registry::local_first_account_id(
            *app.uuid(),
            "wrong-user",
        ));
        assert!(
            backend
                .admit_local_first_session(
                    token.clone(),
                    app_id.into(),
                    wrong_account.canonical().into()
                )
                .is_err()
        );
        for claimed in [
            serde_json::to_string(&(jazz::tools::identity::LOCAL_FIRST_ISSUER, &verified.user_id))
                .unwrap(),
            CoreAuthorSubject::SYSTEM.canonical().to_owned(),
            serde_json::to_string(&(jazz::tools::identity::ANONYMOUS_ISSUER, &verified.user_id))
                .unwrap(),
        ] {
            assert!(
                backend
                    .admit_local_first_session(token.clone(), app_id.into(), claimed)
                    .is_err()
            );
        }
        backend
            .admit_local_first_session(token.clone(), app_id.into(), canonical.clone())
            .unwrap();
        backend.set_identity_claims(bytes(), None).unwrap();
        assert!(other.set_identity_claims(bytes(), None).is_err());
        assert!(ordinary.set_identity_claims(bytes(), None).is_err());
        assert!(
            backend
                .admit_local_first_session(
                    "forged.token.proof".into(),
                    app_id.into(),
                    canonical.clone()
                )
                .is_err()
        );
        backend.set_identity_claims(bytes(), None).unwrap();
        let batch = CoreOpenTransactionId::new().to_string();
        backend
            .begin_transaction(batch.clone(), "mergeable".into(), None, Some(bytes()))
            .unwrap();
        let descriptor = RecordDescriptor::new([("label", ValueType::String)]);
        let raw = descriptor
            .create(&[CoreValue::String("admitted Tx".to_owned())])
            .unwrap();
        let cells = jazz::binding_codec::encode_named_cells(
            &jazz::groove::records::OwnedRecord::new(raw, descriptor),
        )
        .unwrap();
        backend
            .insert_in_transaction(
                batch.clone(),
                "items".into(),
                Uint8Array::from(cells),
                Some(InsertOptions {
                    row_id: None,
                    author: None,
                    attribution: None,
                    branch: None,
                    updated_at_ms: None,
                }),
            )
            .unwrap();
        backend
            .commit_transaction(batch, Some("mergeable".into()))
            .unwrap();
        // A schema view shares the exact owner's admission map.
        let view = backend.register_schema(schema_bytes()).unwrap();
        view.set_identity_claims(bytes(), None).unwrap();
        view.close().unwrap();
        backend.close().unwrap();
        assert!(
            backend
                .admit_local_first_session(token, app_id.into(), canonical)
                .is_err()
        );
        other.close().unwrap();
        ordinary.close().unwrap();
    }

    #[test]
    fn native_node_clock_handoff_preserves_u64_and_rejects_lossy_inputs() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("items").column("label", ColumnType::Text))
            .build();
        let open = || {
            NapiDb::open_memory_as_backend(
                Uint8Array::from(serde_json::to_vec(&schema).unwrap()),
                Uint8Array::from(encode_persistent_open_config(
                    CoreAuthorSubject::for_test_bytes([0x91; 16]),
                )),
            )
            .unwrap()
        };
        let first = open();
        first
            .seed_foreground_tx_time_high_water(BigInt::from(u64::MAX - 1))
            .unwrap();
        let high_water = first.foreground_tx_time_high_water().unwrap();
        assert_eq!(high_water.get_u64(), (false, u64::MAX - 1, true));
        first.close().unwrap();
        let second = open();
        second
            .seed_foreground_tx_time_high_water(high_water)
            .unwrap();
        second
            .seed_foreground_tx_time_high_water(BigInt::from(1_u64))
            .unwrap();
        assert_eq!(
            second.foreground_tx_time_high_water().unwrap().get_u64(),
            (false, u64::MAX - 1, true)
        );
        for invalid in [
            BigInt {
                sign_bit: true,
                words: vec![1],
            },
            BigInt {
                sign_bit: false,
                words: vec![0, 1],
            },
        ] {
            assert!(second.seed_foreground_tx_time_high_water(invalid).is_err());
        }
        assert_eq!(
            second.foreground_tx_time_high_water().unwrap().get_u64(),
            (false, u64::MAX - 1, true)
        );
        second.close().unwrap();
    }

    #[test]
    fn authority_epoch_bigint_rejects_lossy_values_and_preserves_u64() {
        let above_u32 = BigInt {
            sign_bit: false,
            words: vec![u32::MAX as u64 + 1],
        };
        let near_safe_integer = BigInt {
            sign_bit: false,
            words: vec![9_007_199_254_740_993],
        };
        let maximum_u64 = BigInt {
            sign_bit: false,
            words: vec![u64::MAX],
        };
        assert_eq!(
            authority_epoch_from_bigint(above_u32, "authority").unwrap(),
            u32::MAX as u64 + 1
        );
        assert_eq!(
            authority_epoch_from_bigint(near_safe_integer, "authority").unwrap(),
            9_007_199_254_740_993
        );
        assert_eq!(
            authority_epoch_from_bigint(maximum_u64, "authority").unwrap(),
            u64::MAX
        );
        assert!(
            authority_epoch_from_bigint(
                BigInt {
                    sign_bit: true,
                    words: vec![1],
                },
                "authority",
            )
            .is_err()
        );
        assert!(
            authority_epoch_from_bigint(
                BigInt {
                    sign_bit: false,
                    words: vec![0, 1],
                },
                "authority",
            )
            .is_err()
        );
    }

    #[test]
    fn native_delta_preserves_typed_union_occurrence_keys() {
        #[derive(serde::Deserialize)]
        struct DecodedRemoved {
            #[allow(dead_code)]
            table: String,
            #[allow(dead_code)]
            row_id: CoreRowUuid,
        }
        #[derive(serde::Deserialize)]
        struct DecodedDelta {
            added: Vec<serde::de::IgnoredAny>,
            updated: Vec<serde::de::IgnoredAny>,
            removed: Vec<DecodedRemoved>,
            added_occurrence_keys: Vec<jazz::tools::ResultKey>,
            updated_occurrence_keys: Vec<jazz::tools::ResultKey>,
            removed_occurrence_keys: Vec<jazz::tools::ResultKey>,
        }
        let root = jazz::tools::ObjectId::from_uuid(uuid::Uuid::from_bytes([1; 16]));
        let joined = jazz::tools::ObjectId::from_uuid(uuid::Uuid::from_bytes([2; 16]));
        let occurrence = |label: &str| {
            jazz::tools::ResultKey::from_union_occurrence(root, [joined], [(0, label.to_owned())])
                .unwrap()
        };
        let removed = ["direct", "inherited"].map(|label| {
            jazz::db::RemovedRow::from_result_key(
                "todos".to_owned(),
                CoreRowUuid::from_bytes([1; 16]),
                occurrence(label),
            )
        });
        let bytes = encode_core_subscription_delta(&[], &[], &removed).unwrap();
        let decoded: DecodedDelta = postcard::from_bytes(&bytes).unwrap();
        assert!(decoded.added.is_empty() && decoded.updated.is_empty());
        assert_eq!(decoded.removed.len(), 2);
        assert!(decoded.added_occurrence_keys.is_empty());
        assert!(decoded.updated_occurrence_keys.is_empty());
        assert_ne!(
            decoded.removed_occurrence_keys[0],
            decoded.removed_occurrence_keys[1]
        );
    }

    #[test]
    fn schema_json_roundtrip_preserves_enum_fk_and_defaults() {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("files").column("name", ColumnType::Text))
            .table(
                TableSchema::builder("todos")
                    .column_with_default("done", ColumnType::Boolean, Value::Boolean(false))
                    .column(
                        "status",
                        ColumnType::Enum {
                            variants: vec!["done".to_string(), "todo".to_string()],
                        },
                    )
                    .fk_column("image", "files"),
            )
            .build();

        let encoded = serde_json::to_string(&schema).expect("serialize schema");
        let decoded: Schema = serde_json::from_str(&encoded).expect("deserialize schema");

        let status = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("status")
            .unwrap();
        assert_eq!(
            status.column_type,
            ColumnType::Enum {
                variants: vec!["done".to_string(), "todo".to_string()]
            }
        );

        let image = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("image")
            .unwrap();
        assert_eq!(image.references, Some(TableName::new("files")));

        let done = decoded
            .get(&TableName::new("todos"))
            .unwrap()
            .columns
            .column("done")
            .unwrap();
        assert_eq!(done.default, Some(Value::Boolean(false)));
    }

    #[test]
    fn core_read_opts_accept_public_local_only_spelling() {
        let opts = core_read_opts_from_json(Some(json!({ "propagation": "local-only" })))
            .expect("parse read opts");

        assert_eq!(opts.propagation, CorePropagation::LocalOnly);
    }

    #[test]
    fn core_read_opts_ignore_removed_propagate_field() {
        let opts =
            core_read_opts_from_json(Some(json!({ "propagate": false }))).expect("parse read opts");

        assert_eq!(opts.propagation, CorePropagation::Full);
    }

    #[test]
    fn core_read_opts_accept_branch_view() {
        let expected = CoreReadViewSpec::branch_view(
            jazz::protocol::BranchSelector::new([(
                "branch",
                CoreValue::Uuid(uuid::Uuid::from_bytes([0x42; 16])),
            )]),
            None,
        );
        let opts = core_read_opts_from_json(Some(json!({
            "read_view": serde_json::to_value(&expected).unwrap()
        })))
        .expect("parse branch read view");

        assert_eq!(opts.read_view, expected);
    }

    #[test]
    fn subscription_payload_exposes_only_terminal_rows() {
        let payload = core_subscription_event_to_napi(&CoreSubscriptionEvent::Delta {
            reset: false,
            publishable: true,
            added: Vec::new(),
            updated: Vec::new(),
            removed: Vec::new(),
            terminal_operations: Vec::new(),
            settled: true,
            tier: DurabilityTier::Local,
        })
        .expect("encode terminal delta");

        let Either3::A(payload) = payload else {
            panic!("expected delta payload");
        };
        assert!(!payload.delta.is_empty());
        assert!(payload.terminal_operations.is_empty());
        assert_eq!(payload.tier, "Local");
    }

    #[test]
    fn subscription_payload_preserves_descendant_terminal_operations() {
        let descriptor = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            (
                "user_title",
                ValueType::Nullable(Box::new(ValueType::String)),
            ),
        ]);
        let child_path = vec![TerminalPathSegment::Collection("children".to_owned())];
        let operations = vec![
            TerminalOperation {
                root_descriptor: descriptor,
                root_key: vec![0, 255],
                path: vec![
                    TerminalPathSegment::Collection("children".to_owned()),
                    TerminalPathSegment::Key(vec![1, 254]),
                ],
                edit: TerminalEdit::Insert {
                    index: 3,
                    key: vec![2, 253],
                    value: (0_u8..=u8::MAX).collect(),
                },
            },
            TerminalOperation {
                root_descriptor: descriptor,
                root_key: vec![4],
                path: child_path.clone(),
                edit: TerminalEdit::Update {
                    key: vec![5],
                    value: vec![6],
                },
            },
            TerminalOperation {
                root_descriptor: descriptor,
                root_key: vec![7],
                path: child_path.clone(),
                edit: TerminalEdit::Remove { key: vec![8] },
            },
            TerminalOperation {
                root_descriptor: descriptor,
                root_key: vec![9],
                path: child_path,
                edit: TerminalEdit::Move {
                    key: vec![10],
                    index: 11,
                },
            },
        ];
        let payload = core_subscription_event_to_napi(&CoreSubscriptionEvent::Delta {
            reset: false,
            publishable: true,
            added: Vec::new(),
            updated: Vec::new(),
            removed: Vec::new(),
            terminal_operations: operations,
            settled: false,
            tier: DurabilityTier::Edge,
        })
        .expect("encode terminal operations");

        let Either3::A(payload) = payload else {
            panic!("expected delta payload");
        };
        assert_eq!(payload.tier, "Edge");
        assert_eq!(payload.terminal_operations.len(), 4);
        let insert = &payload.terminal_operations[0];
        assert_eq!(insert.root_key, vec![0, 255]);
        assert!(matches!(
            insert.path.as_slice(),
            [Either::A(collection), Either::B(key)]
                if collection.collection == "children" && key.key == vec![1, 254]
        ));
        assert!(matches!(
            &insert.edit,
            Either4::A(edit)
                if edit.insert.index == 3.0
                    && edit.insert.key == vec![2, 253]
                    && edit.insert.value == (0_u32..=u8::MAX.into()).collect::<Vec<_>>()
        ));
        assert!(matches!(
            &payload.terminal_operations[1].edit,
            Either4::B(edit) if edit.update.key == vec![5] && edit.update.value == vec![6]
        ));
        assert!(matches!(
            &payload.terminal_operations[2].edit,
            Either4::C(edit) if edit.remove.key == vec![8]
        ));
        assert!(matches!(
            &payload.terminal_operations[3].edit,
            Either4::D(edit) if edit.move_edit.key == vec![10] && edit.move_edit.index == 11.0
        ));
    }

    #[test]
    fn subscription_payload_preserves_rejection_and_closed_variants() {
        use jazz::protocol::{SubscribeRejectReason, SubscribeServerFailureCode};

        let unsupported = core_subscription_event_to_napi(&CoreSubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::UnsupportedShapeCapability {
                detail: "unsupported maintained shape".to_owned(),
            },
        })
        .expect("encode unsupported rejection");
        assert!(matches!(
            unsupported,
            Either3::B(crate::SubscriptionRejectedEvent {
                event_type,
                reason: Either4::A(crate::SubscriptionUnsupportedShapeCapabilityReason {
                    reason_type,
                    detail,
                }),
            }) if event_type == "rejected"
                && reason_type == "UnsupportedShapeCapability"
                && detail == "unsupported maintained shape"
        ));

        let pending = core_subscription_event_to_napi(&CoreSubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
        })
        .expect("encode pending rejection");
        assert!(matches!(
            pending,
            Either3::B(crate::SubscriptionRejectedEvent {
                event_type,
                reason: Either4::B(crate::SubscriptionShapeRegistrationPendingReason {
                    reason_type,
                }),
            }) if event_type == "rejected"
                && reason_type == "ShapeRegistrationPendingCatalogueAdmission"
        ));

        let server_failure = core_subscription_event_to_napi(&CoreSubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::ServerFailure {
                code: SubscribeServerFailureCode::QueryValidation,
            },
        })
        .expect("encode server rejection");
        assert!(matches!(
            server_failure,
            Either3::B(crate::SubscriptionRejectedEvent {
                event_type,
                reason: Either4::C(crate::SubscriptionServerFailureReason {
                    reason_type,
                    code,
                }),
            }) if event_type == "rejected"
                && reason_type == "ServerFailure"
                && code == "QueryValidation"
        ));

        let invalid_authority = core_subscription_event_to_napi(&CoreSubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::InvalidAuthoritySourceClosure {
                transition: "authority predecessor is not a source".to_owned(),
            },
        })
        .expect("encode invalid authority rejection");
        assert!(matches!(
            invalid_authority,
            Either3::B(crate::SubscriptionRejectedEvent {
                event_type,
                reason: Either4::D(crate::SubscriptionInvalidAuthoritySourceClosureReason {
                    reason_type,
                    transition,
                }),
            }) if event_type == "rejected"
                && reason_type == "InvalidAuthoritySourceClosure"
                && transition == "authority predecessor is not a source"
        ));

        let closed = core_subscription_event_to_napi(&CoreSubscriptionEvent::Closed)
            .expect("encode closed event");
        assert!(matches!(
            closed,
            Either3::C(crate::SubscriptionClosedEvent { event_type }) if event_type == "closed"
        ));
    }

    #[test]
    #[cfg(debug_assertions)]
    fn debug_subscription_event_fixture_covers_rejection_and_closed_variants() {
        let events = crate::test_fixture_export::subscription_events()
            .expect("encode debug subscription event fixture");

        assert_eq!(events.len(), 5);
        assert!(matches!(events[0], Either3::B(_)));
        assert!(matches!(events[1], Either3::B(_)));
        assert!(matches!(events[2], Either3::B(_)));
        assert!(matches!(events[3], Either3::B(_)));
        assert!(matches!(events[4], Either3::C(_)));
    }
    /// A NAPI schema view can address its owner's open transaction by id.
    #[test]
    fn schema_view_uses_owner_transaction_id() {
        let source = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("label", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(PolicyExpr::True)
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                            .with_delete(PolicyExpr::True),
                    ),
            )
            .build();
        let schema = jazz::schema::JazzSchema::new(&source)
            .expect("NAPI transaction fixture public schema compiles");
        let refs = schema.column_families();
        let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
        let owner = Rc::new(
            core_block_on(CoreDb::open(CoreDbConfig::new(
                schema.clone(),
                CoreMemoryStorage::new(&refs).expect("valid memory storage families"),
                CoreDbIdentity {
                    node: CoreNodeUuid::from_bytes([0x44; 16]),
                    author: CoreAuthorSubject::for_test_bytes([0xa4; 16]),
                },
            )))
            .unwrap(),
        );
        let view = Rc::new(core_block_on(owner.register_schema_view(schema.clone())).unwrap());
        let batch = CoreOpenTransactionId::new();
        core_block_on(owner.begin_mergeable(batch)).unwrap();
        core_block_on(view.mergeable_tx_ref(batch).insert(
            "items",
            BTreeMap::from([("label".to_owned(), CoreValue::String("kept".to_owned()))]),
            jazz::db::InsertOptions {
                row_id: Some(CoreRowUuid::from_bytes([1; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
        core_block_on(owner.commit_mergeable_handle(batch)).unwrap();

        let exclusive = CoreOpenTransactionId::new();
        core_block_on(owner.begin_exclusive(exclusive)).unwrap();
        core_block_on(view.exclusive_tx_ref(exclusive).insert(
            "items",
            BTreeMap::from([(
                "label".to_owned(),
                CoreValue::String("exclusive-kept".to_owned()),
            )]),
            jazz::db::InsertOptions {
                row_id: Some(CoreRowUuid::from_bytes([2; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
        core_block_on(owner.commit_exclusive_handle(exclusive)).unwrap();

        // The public NAPI transaction surface binds Alice at begin and addresses
        // subsequent reads through the owner-wide open transaction id.
        let binding = NapiDb {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Memory(Rc::clone(
                &owner,
            ))))),
            owns_runtime: false,
            non_durable_client: Rc::new(Cell::new(false)),
            trusted_backend: false,
            author_admissions: NativeAuthorAdmissions::default(),
        };
        let alice = CoreAuthorSubject::for_test_bytes([0xa6; 16]);
        let bound = CoreOpenTransactionId::new();
        binding
            .begin_transaction(
                bound.to_string(),
                "exclusive".to_owned(),
                Some(Uint8Array::new(alice.canonical().as_bytes().to_vec())),
                None,
            )
            .unwrap();
        let query =
            Uint8Array::new(postcard::to_allocvec(&owner.table("items")).expect("encode query"));
        assert!(
            binding
                .all(query, None, Some(bound.to_string()), None, None)
                .is_ok(),
            "planted positive: the bound transaction reads successfully"
        );
        let view_binding = NapiDb {
            inner: Rc::new(RefCell::new(Some(NapiDbInnerStorage::Memory(Rc::clone(
                &view,
            ))))),
            owns_runtime: false,
            non_durable_client: Rc::new(Cell::new(false)),
            trusted_backend: false,
            author_admissions: NativeAuthorAdmissions::default(),
        };
        let view_query =
            Uint8Array::new(postcard::to_allocvec(&view.table("items")).expect("encode query"));
        assert!(
            view_binding
                .all(view_query, None, Some(bound.to_string()), None, None)
                .is_ok(),
            "a registered schema facade shares its owner's transaction runtime"
        );
        binding
            .commit_transaction(bound.to_string(), Some("exclusive".to_owned()))
            .unwrap();
    }
}
