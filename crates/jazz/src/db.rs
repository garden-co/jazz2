//! High-level thread-affine database facade described by `jazz/API.md`. This
//! module owns application-facing handles, read/write options, and facade-level
//! sync plumbing; durable version storage, validation, policy checks, and view
//! construction live in [`crate::node`], while link-local shipped state lives in
//! [`crate::peer`]. In the layer map this is the top `Db` facade over the node.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::{Pin, pin};
use std::rc::{Rc, Weak};
#[cfg(feature = "sync-autopsy")]
use std::sync::{
    LazyLock, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll, Waker};

use futures::lock::Mutex as LocalMutex;
use futures_channel::mpsc::{UnboundedReceiver, UnboundedSender, unbounded};
use futures_channel::oneshot;
use futures_core::Stream;
use groove::records::{BorrowedRecord, OwnedRecord, RecordDescriptor, Value};
use groove::schema::ColumnType as GrooveColumnType;
use groove::storage::{OrderedKvStorage, ReopenableStorage};
use thiserror::Error;
#[cfg(feature = "cold-settle-attribution")]
use web_time::Instant;

use crate::authorization_scope::{
    AuthorityContext, AuthorityScopeAggregate, AuthorizationScopeAcquisition,
    AuthorizationScopeInstall, AuthorizationScopeLease, AuthorizationScopeOwnerToken,
    AuthorizationScopeReadiness, AuthorizationScopeRegistry, MAX_AUTHORIZATION_SCOPES,
};
use crate::ids::{AuthorSubject, NodeUuid, RowUuid, SchemaVersionId};
pub use crate::node::CommitUnitTrust;
#[cfg(test)]
use crate::node::CurrentRowBindingRole;
#[cfg(feature = "testing")]
pub use crate::node::NodeOpenReceipt as DbOpenReceipt;
use crate::node::query_engine::QueryAuthorizationMode;
use crate::node::{
    CommitUnitIngestContext, CurrentRow, EdgeCacheBudget, LocalMaintainedViewSubscription,
    LocalMaintainedViewSubscriptionUpdate, MergeableCommit, NodeState, PreparedQueryPlanHandle,
    PublicationOutcome, PublishedTransaction, QueryReadProfile, RelationEdge, RelationSnapshot,
    RowProvenance, TransactionBranchRowState, ViewUpdateParts,
};
use crate::peer::{PeerRole, PeerState};
pub use crate::protocol::PermissionAdvice;
use crate::protocol::{
    AuthorizationScopeReceipt, BindingSource, BindingViewKey, BranchSelector, BranchViewBase,
    ChunkRequestBatch, ChunkRequestEntry, ChunkResponse, ChunkResponseBatch, ChunkResponseEntry,
    CoverageKey, CurrentWriteSchema, LensOp, MigrationLens, PermissionAdviceAction,
    PermissionAdviceRequestId, ReadViewKey, ReadViewSourceSpec, ReadViewSpec, RegisterShapeOptions,
    SchemaLineagePublication, SchemaVersion, ShapeAst, Subscribe, SubscribeRejectReason,
    SubscribeServerFailureCode, SubscriptionKey, SyncMessage, TableLens, VersionRecord,
};
use crate::protocol_limits::{
    MAX_SHAPE_REGISTRATIONS_PER_PEER, validate_fetch_row_versions,
    validate_known_state_declaration, validate_shape_registration_size,
};
use crate::query::{
    Binding, BindingId, Operand, Predicate, Query, QueryError, RelationQuery, ShapeId,
    ValidatedQuery, relation_query_to_query,
};
#[cfg(test)]
use crate::query::{
    RelationCmpOp, RelationColumnRef, RelationExpr, RelationJoinKind, RelationPredicate,
    RelationProjectExpr, RelationRowIdRef, RelationValueRef,
};
pub use crate::result_tree::{ResultNode, ResultRelation, ResultTree, ResultTreeReplacement};
use crate::schema::{JazzSchema, TableSchema};
use crate::time::{GlobalTime, TxTime};
use crate::tools::OpenTransactionId;
use crate::tools::{ObjectId, OutputOccurrenceId, ResultKey, TransactionId};
use crate::tx::{DeletionEvent, DurabilityTier, Fate, RejectionReason, Transaction, TxId, TxKind};
use crate::wire::{TransportError, WireAuthorityEndpoint, WireFeatures, encode_sync_message};

mod wire_transport;
pub use wire_transport::WireTransportAdapter;
#[cfg(test)]
use wire_transport::{LogicalMessageReassembler, RECENT_COMPLETED_LOGICAL_MESSAGES};

/// Pragmatic single-threaded serialization boundary for canonical Jazz state.
///
/// Storage-facing operations may suspend, so a `RefCell` borrow cannot safely
/// represent exclusive ownership for their full lifetime. This local mutex is
/// intentionally part of the existing `Node` owner rather than a parallel
/// async facade. A future operation scheduler may replace it with finer-grained
/// owned sessions once the async lifecycle has settled.
pub(crate) type SharedNodeState<S> = Rc<LocalMutex<NodeState<S>>>;

const DEFAULT_CHUNK_FORWARD_HOPS: u8 = 8;
const MAX_PENDING_CHUNK_DEMANDS: usize = 4096;
// Downstream relay waiters and their response hand-offs share one budget. A
// response remains reserved while a binding is deciding whether its send
// succeeded, so a failed send can be restored without racing fresh admission.
const MAX_RELAY_CHUNK_OBLIGATIONS: usize = MAX_PENDING_CHUNK_DEMANDS;
// Auxiliary chunk routing crosses several independent transports in browser
// persistent-worker mode. Retain only a small, redacted flight recorder so a
// binding can explain a miss without ever exposing a capability locator.
const MAX_CHUNK_RELAY_TRACE_EVENTS_PER_CONNECTION: usize = 64;

/// A bounded, redacted auxiliary-routing event for binding diagnostics.
///
/// Request ids and connection ids are hop-local. `object_hash` and
/// `locator_fingerprint` are short BLAKE3-derived fingerprints rather than
/// the actual immutable-content hash or retrieval capability.
#[derive(Clone, Debug)]
pub struct PeerIoTraceEntry {
    /// Routing transition observed at this transport hop.
    pub event: &'static str,
    /// Whether this link points upstream or to a downstream subscriber.
    pub role: &'static str,
    /// Opaque, process-local connection epoch.
    pub connection: u64,
    /// Hop-local request identifier.
    pub request_id: u64,
    /// Forwarding budget associated with the request at this hop.
    pub remaining_hops: u8,
    /// Short fingerprint of the immutable object hash.
    pub object_hash: String,
    /// Short fingerprint of the opaque retrieval capability.
    pub locator_fingerprint: String,
    /// Result kind, for response transitions only.
    pub response: Option<&'static str>,
    /// Redacted local-storage failure class, when a relay cannot serve a chunk.
    pub storage_error: Option<&'static str>,
}

fn short_fingerprint(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex()[..12].to_owned()
}

fn response_kind(response: &ChunkResponse) -> &'static str {
    match response {
        ChunkResponse::Found(_) => "found",
        ChunkResponse::Unavailable => "unavailable",
        ChunkResponse::Retryable { .. } => "retryable",
    }
}

fn storage_error_kind(error: &groove::chunks::ChunkStorageError) -> &'static str {
    match error {
        groove::chunks::ChunkStorageError::Unavailable => "unavailable",
        groove::chunks::ChunkStorageError::LocatorConflict => "locator-conflict",
        groove::chunks::ChunkStorageError::Integrity => "integrity",
        groove::chunks::ChunkStorageError::Backend(_) => "backend",
    }
}

enum ChunkDemandWaiter {
    Local {
        waiter_id: u64,
        sender: oneshot::Sender<Result<bytes::Bytes, groove::chunks::ChunkError>>,
    },
    Relay {
        connection: u64,
        request_id: u64,
    },
}

struct PendingChunkDemand {
    upstream_id: u64,
    remaining_hops: u8,
    waiters: Vec<ChunkDemandWaiter>,
}

#[derive(Default)]
struct ChunkDemandState {
    next_request_id: u64,
    next_waiter_id: u64,
    relay_chunk_obligations: usize,
    pending_by_chunk: BTreeMap<groove::chunks::ChunkRequest, PendingChunkDemand>,
    chunk_by_upstream_id: BTreeMap<u64, groove::chunks::ChunkRequest>,
    outbound: VecDeque<ChunkRequestEntry>,
    relay_responses: BTreeMap<u64, Vec<ChunkResponseEntry>>,
    // Batches removed by `take_relay_responses` but not yet either restored
    // after a failed send or acknowledged as handed to the transport.
    // They remain part of `relay_chunk_obligations`.
    inflight_relay_responses: BTreeMap<u64, VecDeque<Vec<ChunkResponseEntry>>>,
    outbound_wakers: BTreeMap<u64, Waker>,
    disconnected_connections: BTreeSet<u64>,
    upstream_connection: Option<u64>,
    upstream_connections: BTreeSet<u64>,
    completion_generation: u64,
    trace_by_connection: BTreeMap<u64, VecDeque<PeerIoTraceEntry>>,
    traced_connections: BTreeSet<u64>,
}

#[derive(Clone, Default)]
struct PeerChunkResolver {
    state: Rc<RefCell<ChunkDemandState>>,
}

impl PeerChunkResolver {
    fn record_request(
        &self,
        connection: u64,
        role: PeerIoPumpRole,
        event: &'static str,
        request: &ChunkRequestEntry,
        response: Option<&ChunkResponse>,
        storage_error: Option<&groove::chunks::ChunkStorageError>,
    ) {
        let mut state = self.state.borrow_mut();
        if !state.traced_connections.contains(&connection) {
            return;
        }
        let trace = state.trace_by_connection.entry(connection).or_default();
        if trace.len() >= MAX_CHUNK_RELAY_TRACE_EVENTS_PER_CONNECTION {
            trace.pop_front();
        }
        trace.push_back(PeerIoTraceEntry {
            event,
            role: role.as_str(),
            connection,
            request_id: request.request_id,
            remaining_hops: request.remaining_hops,
            object_hash: short_fingerprint(&request.expected_hash),
            locator_fingerprint: short_fingerprint(request.locator.as_bytes()),
            response: response.map(response_kind),
            storage_error: storage_error.map(storage_error_kind),
        });
    }

    fn take_trace(&self, connection: u64) -> Vec<PeerIoTraceEntry> {
        self.state
            .borrow_mut()
            .trace_by_connection
            .remove(&connection)
            .map(VecDeque::into_iter)
            .map(Iterator::collect)
            .unwrap_or_default()
    }

    fn set_trace_enabled(&self, connection: u64, enabled: bool) {
        let mut state = self.state.borrow_mut();
        if enabled {
            state.traced_connections.insert(connection);
        } else {
            state.traced_connections.remove(&connection);
            state.trace_by_connection.remove(&connection);
        }
    }

    fn has_pending_local_demand(&self) -> bool {
        self.state
            .borrow()
            .pending_by_chunk
            .values()
            .any(|pending| {
                pending
                    .waiters
                    .iter()
                    .any(|waiter| matches!(waiter, ChunkDemandWaiter::Local { .. }))
            })
    }

    fn completion_generation(&self) -> u64 {
        self.state.borrow().completion_generation
    }

    fn register_connection(&self, connection: u64, upstream: bool) {
        let mut state = self.state.borrow_mut();
        state.disconnected_connections.remove(&connection);
        if upstream {
            state.upstream_connections.insert(connection);
            if state.upstream_connection.is_none() {
                state.upstream_connection = Some(connection);
                // A predecessor may have already drained its request batch
                // before disconnecting. Requeue that in-flight demand when a
                // replacement registers, preserving its hop-local id so the
                // usual late-frame guard still rejects the old link.
                Self::requeue_pending_demands(&mut state);
                Self::wake_connection(&mut state, connection);
            }
        }
    }

    fn wake_connection(state: &mut ChunkDemandState, connection: u64) {
        if let Some(waker) = state.outbound_wakers.remove(&connection) {
            waker.wake();
        }
    }

    /// Put every demand absent from the outbound queue back on the active
    /// upstream. Requests retain their allocated ids so a superseded link
    /// cannot complete the replacement link's demand with a late response.
    fn requeue_pending_demands(state: &mut ChunkDemandState) {
        let queued = state
            .outbound
            .iter()
            .map(|entry| entry.request_id)
            .collect::<BTreeSet<_>>();
        let retries = state
            .pending_by_chunk
            .iter()
            .filter(|(_, pending)| !queued.contains(&pending.upstream_id))
            .map(|(request, pending)| ChunkRequestEntry {
                request_id: pending.upstream_id,
                locator: request.locator.clone(),
                expected_hash: request.object_hash,
                remaining_hops: pending.remaining_hops,
            })
            .collect::<Vec<_>>();
        state.outbound.extend(retries);
    }

    fn reserve_relay_obligation(state: &mut ChunkDemandState) -> bool {
        if state.relay_chunk_obligations >= MAX_RELAY_CHUNK_OBLIGATIONS {
            return false;
        }
        state.relay_chunk_obligations += 1;
        true
    }

    fn queue_relay_response(
        state: &mut ChunkDemandState,
        connection: u64,
        response: ChunkResponseEntry,
        reservation_transferred: bool,
    ) -> bool {
        if !reservation_transferred && !Self::reserve_relay_obligation(state) {
            return false;
        }
        state
            .relay_responses
            .entry(connection)
            .or_default()
            .push(response);
        Self::wake_connection(state, connection);
        true
    }

    fn enqueue_relay_responses(
        &self,
        connection: u64,
        responses: impl IntoIterator<Item = ChunkResponseEntry>,
    ) {
        let mut state = self.state.borrow_mut();
        for response in responses {
            if !Self::queue_relay_response(&mut state, connection, response, false) {
                break;
            }
        }
    }

    fn enqueue(
        &self,
        request: groove::chunks::ChunkRequest,
        remaining_hops: u8,
        waiter: ChunkDemandWaiter,
    ) -> Result<(), ChunkDemandWaiter> {
        let mut state = self.state.borrow_mut();
        let relay_waiter = matches!(&waiter, ChunkDemandWaiter::Relay { .. });
        if relay_waiter && !Self::reserve_relay_obligation(&mut state) {
            return Err(waiter);
        }
        if let Some(pending) = state.pending_by_chunk.get_mut(&request) {
            pending.waiters.push(waiter);
            return Ok(());
        }
        if state.pending_by_chunk.len() >= MAX_PENDING_CHUNK_DEMANDS {
            state.relay_chunk_obligations -= usize::from(relay_waiter);
            return Err(waiter);
        }
        state.next_request_id = state.next_request_id.wrapping_add(1).max(1);
        let upstream_id = state.next_request_id;
        state.outbound.push_back(ChunkRequestEntry {
            request_id: upstream_id,
            locator: request.locator.clone(),
            expected_hash: request.object_hash,
            remaining_hops,
        });
        state
            .chunk_by_upstream_id
            .insert(upstream_id, request.clone());
        state.pending_by_chunk.insert(
            request,
            PendingChunkDemand {
                upstream_id,
                remaining_hops,
                waiters: vec![waiter],
            },
        );
        if let Some(connection) = state.upstream_connection {
            Self::wake_connection(&mut state, connection);
        }
        Ok(())
    }

    fn enqueue_relay(&self, connection: u64, request: ChunkRequestEntry) {
        if request.remaining_hops == 0 {
            let mut state = self.state.borrow_mut();
            Self::queue_relay_response(
                &mut state,
                connection,
                ChunkResponseEntry {
                    request_id: request.request_id,
                    result: ChunkResponse::Unavailable,
                },
                false,
            );
            return;
        }
        if let Err(ChunkDemandWaiter::Relay {
            connection,
            request_id,
        }) = self.enqueue(
            groove::chunks::ChunkRequest {
                object_hash: request.expected_hash,
                locator: request.locator,
            },
            request.remaining_hops - 1,
            ChunkDemandWaiter::Relay {
                connection,
                request_id: request.request_id,
            },
        ) {
            let mut state = self.state.borrow_mut();
            Self::queue_relay_response(
                &mut state,
                connection,
                ChunkResponseEntry {
                    request_id,
                    result: ChunkResponse::Retryable { retry_after_ms: 25 },
                },
                false,
            );
        }
    }

    fn take_outbound(&self, limit: usize) -> Vec<ChunkRequestEntry> {
        let mut state = self.state.borrow_mut();
        // The wire decoder rejects batches above this cardinality, so never let
        // an eager host-side drain construct a message that another Jazz peer
        // would reject.
        let count = limit
            .min(crate::protocol_limits::MAX_CHUNK_REQUEST_BATCH_ENTRIES)
            .min(state.outbound.len());
        state.outbound.drain(..count).collect()
    }

    fn take_relay_responses(&self, connection: u64, limit: usize) -> Vec<ChunkResponseEntry> {
        let mut state = self.state.borrow_mut();
        let (responses, exhausted) = match state.relay_responses.get_mut(&connection) {
            Some(queued) => {
                let count = limit.min(queued.len());
                (
                    queued.drain(..count).collect::<Vec<ChunkResponseEntry>>(),
                    queued.is_empty(),
                )
            }
            None => return Vec::new(),
        };
        if exhausted {
            state.relay_responses.remove(&connection);
        }
        state
            .inflight_relay_responses
            .entry(connection)
            .or_default()
            .push_back(responses.clone());
        responses
    }

    fn restore_relay_responses(&self, connection: u64, responses: Vec<ChunkResponseEntry>) {
        let mut state = self.state.borrow_mut();
        let inflight = state
            .inflight_relay_responses
            .get_mut(&connection)
            .expect("restored relay response batch was handed out");
        let position = inflight
            .iter()
            .position(|batch| batch == &responses)
            .expect("restored relay response batch is still in flight");
        inflight.remove(position);
        if inflight.is_empty() {
            state.inflight_relay_responses.remove(&connection);
        }
        state
            .relay_responses
            .entry(connection)
            .or_default()
            .splice(0..0, responses);
    }

    fn acknowledge_relay_response_send(&self, connection: u64, responses: &[ChunkResponseEntry]) {
        let mut state = self.state.borrow_mut();
        let inflight = state
            .inflight_relay_responses
            .get_mut(&connection)
            .expect("acknowledged relay response batch was handed out");
        let position = inflight
            .iter()
            .position(|batch| batch == responses)
            .expect("acknowledged relay response batch is still in flight");
        let released = inflight
            .remove(position)
            .expect("located relay response batch remains removable");
        if inflight.is_empty() {
            state.inflight_relay_responses.remove(&connection);
        }
        state.relay_chunk_obligations = state
            .relay_chunk_obligations
            .checked_sub(released.len())
            .expect("acknowledged relay responses are accounted");
    }

    fn is_active_upstream(&self, connection: u64) -> bool {
        self.state.borrow().upstream_connection == Some(connection)
    }

    fn complete(&self, response: ChunkResponseEntry) {
        let mut state = self.state.borrow_mut();
        let Some(request) = state.chunk_by_upstream_id.remove(&response.request_id) else {
            return;
        };
        let Some(pending) = state.pending_by_chunk.remove(&request) else {
            return;
        };
        state.completion_generation = state.completion_generation.wrapping_add(1);
        debug_assert_eq!(pending.upstream_id, response.request_id);
        for waiter in pending.waiters {
            match waiter {
                ChunkDemandWaiter::Local { sender, .. } => {
                    let result = match &response.result {
                        ChunkResponse::Found(bytes) => Ok(bytes::Bytes::copy_from_slice(bytes)),
                        ChunkResponse::Unavailable => Err(groove::chunks::ChunkError::Unavailable),
                        ChunkResponse::Retryable { retry_after_ms } => {
                            Err(groove::chunks::ChunkError::Retryable {
                                retry_after_ms: *retry_after_ms,
                            })
                        }
                    };
                    let _ = sender.send(result);
                }
                ChunkDemandWaiter::Relay {
                    connection,
                    request_id,
                } => {
                    Self::queue_relay_response(
                        &mut state,
                        connection,
                        ChunkResponseEntry {
                            request_id,
                            result: response.result.clone(),
                        },
                        true,
                    );
                }
            }
        }
        debug_assert!(state.relay_chunk_obligations <= MAX_RELAY_CHUNK_OBLIGATIONS);
    }

    fn cancel_local(&self, request: &groove::chunks::ChunkRequest, waiter_id: u64) {
        let mut state = self.state.borrow_mut();
        let Some(pending) = state.pending_by_chunk.get_mut(request) else {
            return;
        };
        pending.waiters.retain(|waiter| {
            !matches!(waiter, ChunkDemandWaiter::Local { waiter_id: id, .. } if *id == waiter_id)
        });
        if pending.waiters.is_empty() {
            let upstream_id = pending.upstream_id;
            state.pending_by_chunk.remove(request);
            state.chunk_by_upstream_id.remove(&upstream_id);
            state
                .outbound
                .retain(|outbound| outbound.request_id != upstream_id);
        }
    }

    fn disconnect(&self, connection: u64, upstream: bool) {
        let mut state = self.state.borrow_mut();
        state.disconnected_connections.insert(connection);
        state.trace_by_connection.remove(&connection);
        state.traced_connections.remove(&connection);
        let disconnected_waker = state.outbound_wakers.remove(&connection);
        let queued_responses = state
            .relay_responses
            .remove(&connection)
            .map_or(0, |responses| responses.len());
        let inflight_responses = state
            .inflight_relay_responses
            .remove(&connection)
            .map_or(0, |batches| {
                batches.into_iter().map(|batch| batch.len()).sum()
            });
        state.relay_chunk_obligations = state
            .relay_chunk_obligations
            .checked_sub(queued_responses + inflight_responses)
            .expect("disconnected relay responses are accounted");
        if upstream {
            state.upstream_connections.remove(&connection);
            if state.upstream_connection == Some(connection) {
                state.upstream_connection = state.upstream_connections.iter().next().copied();
                if let Some(successor) = state.upstream_connection {
                    Self::requeue_pending_demands(&mut state);
                    Self::wake_connection(&mut state, successor);
                }
            }
            drop(state);
            if let Some(waker) = disconnected_waker {
                waker.wake();
            }
            return;
        }
        let requests = state.pending_by_chunk.keys().cloned().collect::<Vec<_>>();
        for request in requests {
            let (removed_relay_waiters, emptied_upstream_id) = {
                let Some(pending) = state.pending_by_chunk.get_mut(&request) else {
                    continue;
                };
                let mut removed_relay_waiters = 0;
                pending.waiters.retain(|waiter| {
                    let remove = matches!(waiter, ChunkDemandWaiter::Relay { connection: relay, .. } if *relay == connection);
                    removed_relay_waiters += usize::from(remove);
                    !remove
                });
                (
                    removed_relay_waiters,
                    pending.waiters.is_empty().then_some(pending.upstream_id),
                )
            };
            state.relay_chunk_obligations = state
                .relay_chunk_obligations
                .checked_sub(removed_relay_waiters)
                .expect("disconnected relay waiters are accounted");
            if let Some(upstream_id) = emptied_upstream_id {
                state.pending_by_chunk.remove(&request);
                state.chunk_by_upstream_id.remove(&upstream_id);
                state
                    .outbound
                    .retain(|entry| entry.request_id != upstream_id);
            }
        }
        drop(state);
        if let Some(waker) = disconnected_waker {
            waker.wake();
        }
    }
}

struct ChunkResolutionFuture {
    resolver: PeerChunkResolver,
    request: groove::chunks::ChunkRequest,
    waiter_id: u64,
    receiver: oneshot::Receiver<Result<bytes::Bytes, groove::chunks::ChunkError>>,
    completed: bool,
}

impl Future for ChunkResolutionFuture {
    type Output = Result<bytes::Bytes, groove::chunks::ChunkError>;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(context) {
            Poll::Ready(result) => {
                self.completed = true;
                Poll::Ready(result.unwrap_or(Err(groove::chunks::ChunkError::Unavailable)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ChunkResolutionFuture {
    fn drop(&mut self) {
        if !self.completed {
            self.resolver.cancel_local(&self.request, self.waiter_id);
        }
    }
}

#[derive(Clone, Copy)]
enum PeerIoPumpRole {
    Upstream,
    Subscriber,
}

impl PeerIoPumpRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Upstream => "upstream",
            Self::Subscriber => "subscriber",
        }
    }
}

/// Executor-neutral auxiliary peer-I/O endpoint.
///
/// Bindings retain this clone beside their socket. It never acquires Jazz's
/// semantic node lock, so chunk traffic can progress while a Groove evaluation
/// is suspended inside `Node::tick`.
#[derive(Clone)]
pub struct PeerIoPump {
    resolver: PeerChunkResolver,
    local_chunks: groove::chunks::LocalChunkReader,
    connection: u64,
    role: PeerIoPumpRole,
    wire_inbound_context: Option<Rc<crate::wire::WireInboundContext>>,
    wire_outbound_frame: Option<Rc<RefCell<crate::wire::WireFrame>>>,
}

/// One encoded auxiliary frame whose source obligation remains owned by this
/// pump until the binding commits the handoff. Dropping the reservation restores
/// the exact request/response batch to the front of its lane.
pub(crate) struct ReservedOutboundWireFrame {
    pump: PeerIoPump,
    message: Option<SyncMessage>,
    frame: Option<Vec<u8>>,
}

impl ReservedOutboundWireFrame {
    pub(crate) fn take_frame(&mut self) -> Vec<u8> {
        self.frame
            .take()
            .expect("reserved auxiliary wire frame is handed to the transport once")
    }

    pub(crate) fn commit(mut self) {
        let message = self
            .message
            .take()
            .expect("reserved auxiliary outbound batch is committed once");
        self.pump.acknowledge_outbound(&message);
    }
}

impl Drop for ReservedOutboundWireFrame {
    fn drop(&mut self) {
        if let Some(message) = self.message.take() {
            self.pump.restore_outbound(message);
        }
    }
}

impl PeerIoPump {
    fn new(
        resolver: PeerChunkResolver,
        local_chunks: groove::chunks::LocalChunkReader,
        connection: u64,
        role: PeerIoPumpRole,
        wire_inbound_context: Option<Rc<crate::wire::WireInboundContext>>,
    ) -> Self {
        resolver.register_connection(connection, matches!(role, PeerIoPumpRole::Upstream));
        let wire_outbound_frame = wire_inbound_context.as_ref().map(|context| {
            let mut envelope = crate::wire::WireEnvelope::new(
                context.expected_protocol_version(),
                crate::wire::FEATURE_NONE,
                Vec::new(),
            );
            if let Some(session) = context.expected_session().cloned() {
                envelope = envelope.with_session(session);
            }
            Rc::new(RefCell::new(crate::wire::WireFrame::Message(envelope)))
        });
        Self {
            resolver,
            local_chunks,
            connection,
            role,
            wire_inbound_context,
            wire_outbound_frame,
        }
    }

    fn wire_inbound_context(&self) -> Result<&crate::wire::WireInboundContext, String> {
        self.wire_inbound_context.as_deref().ok_or_else(|| {
            "auxiliary wire framing requires a paired wire transport adapter".to_owned()
        })
    }

    /// Route an auxiliary message received by the binding. Returns `false` for
    /// canonical Jazz messages, which the binding must enqueue on its ordinary
    /// transport before scheduling a semantic tick.
    pub async fn route_incoming(&self, message: SyncMessage) -> Result<(), SyncMessage> {
        match (self.role, message) {
            (PeerIoPumpRole::Upstream, SyncMessage::ChunkResponseBatch(batch)) => {
                // A disconnected or superseded upstream can still have a late
                // frame in its binding's receive queue. Demand has already
                // moved to the successor, so only that link may complete it.
                if !self.resolver.is_active_upstream(self.connection) {
                    return Ok(());
                }
                for response in batch.responses {
                    if let Some((request, remaining_hops)) = {
                        let state = self.resolver.state.borrow();
                        state
                            .chunk_by_upstream_id
                            .get(&response.request_id)
                            .and_then(|request| {
                                state
                                    .pending_by_chunk
                                    .get(request)
                                    .map(|pending| (request.clone(), pending.remaining_hops))
                            })
                    } {
                        self.resolver.record_request(
                            self.connection,
                            self.role,
                            "inbound-response",
                            &ChunkRequestEntry {
                                request_id: response.request_id,
                                locator: request.locator,
                                expected_hash: request.object_hash,
                                remaining_hops,
                            },
                            Some(&response.result),
                            None,
                        );
                    }
                    self.resolver.complete(response);
                }
                Ok(())
            }
            (PeerIoPumpRole::Subscriber, SyncMessage::ChunkRequestBatch(batch)) => {
                let mut responses = Vec::new();
                for request in batch.requests {
                    if self.is_disconnected() {
                        break;
                    }
                    self.resolver.record_request(
                        self.connection,
                        self.role,
                        "inbound-request",
                        &request,
                        None,
                        None,
                    );
                    let result = self
                        .local_chunks
                        .get(
                            request.locator.clone(),
                            groove::large_values::ContentHash(request.expected_hash),
                        )
                        .await;
                    if self.is_disconnected() {
                        break;
                    }
                    match result {
                        Ok(bytes) => {
                            self.resolver.record_request(
                                self.connection,
                                self.role,
                                "local-response",
                                &request,
                                Some(&ChunkResponse::Found(Vec::new())),
                                None,
                            );
                            responses.push(ChunkResponseEntry {
                                request_id: request.request_id,
                                result: ChunkResponse::Found(bytes.to_vec()),
                            });
                        }
                        Err(groove::chunks::ChunkStorageError::Unavailable) => {
                            self.resolver.record_request(
                                self.connection,
                                self.role,
                                "relay-request",
                                &request,
                                None,
                                None,
                            );
                            self.resolver.enqueue_relay(self.connection, request)
                        }
                        Err(error) => {
                            self.resolver.record_request(
                                self.connection,
                                self.role,
                                "local-response",
                                &request,
                                Some(&ChunkResponse::Unavailable),
                                Some(&error),
                            );
                            responses.push(ChunkResponseEntry {
                                request_id: request.request_id,
                                result: ChunkResponse::Unavailable,
                            });
                        }
                    }
                }
                if !responses.is_empty() && !self.is_disconnected() {
                    self.resolver
                        .enqueue_relay_responses(self.connection, responses);
                }
                Ok(())
            }
            (_, SyncMessage::ChunkRequestBatch(_) | SyncMessage::ChunkResponseBatch(_)) => Ok(()),
            (_, message) => Err(message),
        }
    }

    /// Decode and route one payload from the dedicated auxiliary protocol
    /// channel. Canonical payloads are returned unchanged for the ordinary wire
    /// adapter, allowing bindings to demultiplex without taking a peer lock.
    pub async fn route_incoming_payload(
        &self,
        payload: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, String> {
        let message = crate::wire::decode_sync_message(&payload)
            .map_err(|error| format!("malformed auxiliary chunk payload: {error}"))?;
        match self.route_incoming(message).await {
            Ok(()) => Ok(None),
            Err(_) => Ok(Some(payload)),
        }
    }

    /// Demultiplex one complete wire frame without taking the Jazz node lock.
    /// Auxiliary messages are consumed; canonical and fragmented frames are
    /// returned byte-for-byte for the ordinary semantic transport.
    pub async fn route_incoming_wire_frame(
        &self,
        frame: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, String> {
        let decoded = crate::wire::decode_frame(&frame)
            .map_err(|error| format!("malformed auxiliary wire frame: {error}"))?;
        let crate::wire::WireFrame::Message(envelope) = decoded else {
            return Ok(Some(frame));
        };
        let context = self.wire_inbound_context()?;
        let mut decoder = crate::wire::WireStreamDecoder::new(context.negotiated_features())
            .map_err(|error| format!("invalid auxiliary wire context: {error}"))?;
        let message = crate::wire::admit_complete_envelope(context, &mut decoder, envelope)
            .map_err(|error| format!("malformed auxiliary wire envelope: {error:?}"))?;
        match self.route_incoming(message).await {
            Ok(()) => Ok(None),
            Err(_) => Ok(Some(frame)),
        }
    }

    /// Encode one bounded auxiliary batch as an ordinary complete wire frame.
    /// Bindings request one chunk per frame, keeping the maximum 256 KiB node
    /// response below the non-fragmented wire-frame bound.
    pub fn take_outbound_wire_frame(&self) -> Result<Option<Vec<u8>>, String> {
        let Some(mut reservation) = self.reserve_outbound_wire_frame()? else {
            return Ok(None);
        };
        let frame = reservation.take_frame();
        reservation.commit();
        Ok(Some(frame))
    }

    /// Reserve one complete auxiliary wire frame for a binding-owned send.
    /// The caller must commit after its transport accepted the frame or restore
    /// it after a rejected send; dropping it also restores the original batch.
    pub(crate) fn reserve_outbound_wire_frame(
        &self,
    ) -> Result<Option<ReservedOutboundWireFrame>, String> {
        let context = self.wire_inbound_context()?;
        let Some(message) = self.take_outbound(1) else {
            return Ok(None);
        };
        let frame = match self.encode_outbound_wire_frame(message.clone(), context) {
            Ok(frame) => frame,
            Err(error) => {
                self.restore_outbound(message);
                return Err(error);
            }
        };
        Ok(Some(ReservedOutboundWireFrame {
            pump: self.clone(),
            message: Some(message),
            frame: Some(frame),
        }))
    }

    /// Drain a bounded FIFO prefix of the auxiliary lane into complete wire
    /// frames. If the next complete frame would exceed `max_bytes`, it remains
    /// queued for a later drain; no response is dropped merely because a host
    /// transport chooses a smaller batch boundary.
    pub fn take_outbound_wire_frames(
        &self,
        max_frames: usize,
        max_bytes: usize,
    ) -> Result<Vec<Vec<u8>>, String> {
        if max_frames == 0 || max_bytes == 0 {
            return Ok(Vec::new());
        }
        let context = self.wire_inbound_context()?;
        let mut frames = Vec::new();
        let mut total_bytes: usize = 0;
        while frames.len() < max_frames {
            let Some(message) = self.take_outbound(1) else {
                break;
            };
            let frame = self.encode_outbound_wire_frame(message.clone(), context);
            let frame = match frame {
                Ok(frame) => frame,
                Err(error) => {
                    self.restore_outbound(message);
                    return Err(error);
                }
            };
            let Some(next_total) = total_bytes.checked_add(frame.len()) else {
                self.restore_outbound(message);
                break;
            };
            if next_total > max_bytes {
                self.restore_outbound(message);
                if frames.is_empty() {
                    return Err(format!(
                        "auxiliary wire frame exceeds bounded drain budget: frame={} budget={max_bytes}",
                        frame.len()
                    ));
                }
                break;
            }
            total_bytes = next_total;
            self.acknowledge_outbound(&message);
            frames.push(frame);
        }
        Ok(frames)
    }

    fn encode_outbound_wire_frame(
        &self,
        message: SyncMessage,
        context: &crate::wire::WireInboundContext,
    ) -> Result<Vec<u8>, String> {
        let negotiated_features = context.negotiated_features();
        let payload = crate::wire::encode_sync_message_for_features(&message, negotiated_features)
            .map_err(|error| format!("cannot encode auxiliary sync payload: {error:?}"))?;
        let active_features = negotiated_features
            & !(crate::wire::FEATURE_PAYLOAD_LZ4 | crate::wire::FEATURE_PAYLOAD_ZSTD);
        let wire_outbound_frame = self.wire_outbound_frame.as_ref().ok_or_else(|| {
            "auxiliary wire framing requires a paired wire transport adapter".to_owned()
        })?;
        let mut frame = wire_outbound_frame.borrow_mut();
        {
            let crate::wire::WireFrame::Message(envelope) = &mut *frame else {
                unreachable!("auxiliary outbound template is always a message frame");
            };
            envelope.protocol_version = context.expected_protocol_version();
            envelope.features = active_features;
            envelope.payload = payload;
        }
        let encoded = crate::wire::encode_frame(&frame)
            .map_err(|error| format!("cannot encode auxiliary wire frame: {error}"));
        let crate::wire::WireFrame::Message(envelope) = &mut *frame else {
            unreachable!("auxiliary outbound template is always a message frame");
        };
        envelope.payload = Vec::new();
        encoded
    }

    /// Drain one bounded auxiliary batch for immediate transmission.
    pub fn take_outbound(&self, limit: usize) -> Option<SyncMessage> {
        match self.role {
            PeerIoPumpRole::Upstream => {
                let requests = self.resolver.take_outbound(limit);
                for request in &requests {
                    self.resolver.record_request(
                        self.connection,
                        self.role,
                        "outbound-request",
                        request,
                        None,
                        None,
                    );
                }
                (!requests.is_empty()).then_some(SyncMessage::ChunkRequestBatch(
                    ChunkRequestBatch { requests },
                ))
            }
            PeerIoPumpRole::Subscriber => {
                let responses = self.resolver.take_relay_responses(self.connection, limit);
                (!responses.is_empty()).then_some(SyncMessage::ChunkResponseBatch(
                    ChunkResponseBatch { responses },
                ))
            }
        }
    }

    /// Hand one bounded auxiliary batch to an in-process transport. Preserve
    /// its source obligation when the transport rejects the send, just as the
    /// wire-frame reservation does for socket bindings.
    pub fn send_outbound(
        &self,
        transport: &mut dyn Transport,
        limit: usize,
    ) -> Result<bool, TransportError> {
        let Some(message) = self.take_outbound(limit) else {
            return Ok(false);
        };
        if let Err(error) = transport.send(message.clone()) {
            self.restore_outbound(message);
            return Err(error);
        }
        self.acknowledge_outbound(&message);
        Ok(true)
    }

    fn restore_outbound(&self, message: SyncMessage) {
        match (self.role, message) {
            (PeerIoPumpRole::Upstream, SyncMessage::ChunkRequestBatch(batch)) => {
                let mut state = self.resolver.state.borrow_mut();
                for request in batch.requests.into_iter().rev() {
                    state.outbound.push_front(request);
                }
            }
            (PeerIoPumpRole::Subscriber, SyncMessage::ChunkResponseBatch(batch)) => {
                self.resolver
                    .restore_relay_responses(self.connection, batch.responses);
            }
            _ => {}
        }
    }

    /// Confirm that an auxiliary batch has been handed to its binding. A
    /// failed transport send must call `restore_outbound` instead, retaining
    /// the reservation across that decision boundary.
    pub(crate) fn acknowledge_outbound(&self, message: &SyncMessage) {
        if let (PeerIoPumpRole::Subscriber, SyncMessage::ChunkResponseBatch(batch)) =
            (self.role, message)
        {
            self.resolver
                .acknowledge_relay_response_send(self.connection, &batch.responses);
        }
    }

    /// Encode one bounded auxiliary payload for a binding-owned socket channel.
    pub fn take_outbound_payload(&self, limit: usize) -> Result<Option<Vec<u8>>, String> {
        let Some(message) = self.take_outbound(limit) else {
            return Ok(None);
        };
        match crate::wire::encode_sync_message(&message) {
            Ok(payload) => {
                self.acknowledge_outbound(&message);
                Ok(Some(payload))
            }
            Err(error) => {
                self.restore_outbound(message);
                Err(format!("cannot encode auxiliary chunk payload: {error}"))
            }
        }
    }

    /// Wait until auxiliary output is ready without polling or driving a Jazz
    /// semantic tick. Browser microtasks, NAPI async notifications, and native
    /// socket tasks can all await this same executor-independent future.
    pub fn outbound_ready(&self) -> PeerIoOutboundReady {
        PeerIoOutboundReady { pump: self.clone() }
    }

    /// Non-blocking readiness probe for hosts that drive local futures by
    /// repeated callbacks rather than awaiting a Rust future directly.
    pub fn outbound_is_ready(&self) -> bool {
        self.has_outbound()
    }

    /// Detach this link's hop-local routing state. Bindings call this when the
    /// socket closes; another registered upstream inherits unsent demand.
    pub fn disconnect(&self) {
        self.resolver.disconnect(
            self.connection,
            matches!(self.role, PeerIoPumpRole::Upstream),
        );
    }

    fn has_outbound(&self) -> bool {
        let state = self.resolver.state.borrow();
        match self.role {
            PeerIoPumpRole::Upstream => !state.outbound.is_empty(),
            PeerIoPumpRole::Subscriber => state
                .relay_responses
                .get(&self.connection)
                .is_some_and(|responses| !responses.is_empty()),
        }
    }

    pub(crate) fn is_disconnected(&self) -> bool {
        self.resolver
            .state
            .borrow()
            .disconnected_connections
            .contains(&self.connection)
    }

    /// Drain this link's bounded redacted auxiliary-routing trace.
    pub fn take_trace(&self) -> Vec<PeerIoTraceEntry> {
        self.resolver.take_trace(self.connection)
    }

    /// Enable or disable this link's bounded diagnostic flight recorder.
    pub fn set_trace_enabled(&self, enabled: bool) {
        self.resolver.set_trace_enabled(self.connection, enabled);
    }
}

/// Future completed when a [`PeerIoPump`] has outbound auxiliary traffic.
pub struct PeerIoOutboundReady {
    pump: PeerIoPump,
}

impl Future for PeerIoOutboundReady {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.pump.has_outbound() || self.pump.is_disconnected() {
            Poll::Ready(())
        } else {
            self.pump
                .resolver
                .state
                .borrow_mut()
                .outbound_wakers
                .insert(self.pump.connection, context.waker().clone());
            Poll::Pending
        }
    }
}

impl groove::chunks::MissingChunkResolver for PeerChunkResolver {
    fn resolve(
        &self,
        request: groove::chunks::ChunkRequest,
    ) -> groove::chunks::ChunkFuture<'_, Result<bytes::Bytes, groove::chunks::ChunkError>> {
        let (sender, receiver) = oneshot::channel();
        let waiter_id = {
            let mut state = self.state.borrow_mut();
            state.next_waiter_id = state.next_waiter_id.wrapping_add(1).max(1);
            state.next_waiter_id
        };
        if let Err(ChunkDemandWaiter::Local { sender, .. }) = self.enqueue(
            request.clone(),
            DEFAULT_CHUNK_FORWARD_HOPS,
            ChunkDemandWaiter::Local { waiter_id, sender },
        ) {
            let _ = sender.send(Err(groove::chunks::ChunkError::Backend(
                "chunk request backpressure".to_owned(),
            )));
        }
        Box::pin(ChunkResolutionFuture {
            resolver: self.clone(),
            request,
            waiter_id,
            receiver,
            completed: false,
        })
    }
}
pub(crate) type WeakNodeState<S> = Weak<LocalMutex<NodeState<S>>>;

/// Temporary source-compatibility for node operations that are still wholly
/// synchronous. Storage-facing call sites must use `lock().await` instead.
/// Remove this trait as the remaining domains become suspendable.
trait LocalMutexBorrow<T> {
    #[track_caller]
    fn borrow(&self) -> futures::lock::MutexGuard<'_, T>;
    #[track_caller]
    fn borrow_mut(&self) -> futures::lock::MutexGuard<'_, T>;
}

impl<T> LocalMutexBorrow<T> for Rc<LocalMutex<T>> {
    #[track_caller]
    fn borrow(&self) -> futures::lock::MutexGuard<'_, T> {
        self.try_lock().unwrap_or_else(|| {
            panic!(
                "synchronous node operation at {} reentered a suspended operation",
                std::panic::Location::caller()
            )
        })
    }

    #[track_caller]
    fn borrow_mut(&self) -> futures::lock::MutexGuard<'_, T> {
        self.try_lock().unwrap_or_else(|| {
            panic!(
                "synchronous node operation at {} reentered a suspended operation",
                std::panic::Location::caller()
            )
        })
    }
}

/// How urgently a runtime should service pending peer-connection work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TickUrgency {
    /// Run as soon as the runtime can do so without re-entering the current
    /// mutable operation. Used when a query/subscription/transport event needs
    /// prompt coverage or inbound draining.
    Immediate,
    /// Coalesce bursty local work before ticking. Used for uploads created by
    /// local writes.
    Deferred,
    /// Service work in a later host turn, rather than recursively entering a
    /// second database tick from the current transport/query owner turn.
    ///
    /// This is for work that may start cold I/O (notably subscriber view
    /// hydration). It preserves prompt eventual progress without allowing a
    /// just-admitted subscription to monopolize the owner before later inbound
    /// frames and durability receipts are observed.
    AfterCurrentTurn,
}

/// Runtime-neutral wake hook for thread-affine [`Node`] sync work.
pub trait TickScheduler {
    /// Schedule a future [`Db::tick`] for pending peer-connection work.
    fn schedule_tick(&self, urgency: TickUrgency);

    /// Schedule one future tick no earlier than the supplied delay.
    ///
    /// This is deliberately distinct from [`TickUrgency::Deferred`]: callers
    /// use it for protocol admission windows, where turning a deadline into a
    /// microtask would create a resend hot loop. Every host therefore supplies
    /// a real timer implementation.
    fn schedule_tick_after(&self, delay_ms: u64);

    /// A waker for cold query-runtime storage progress.
    ///
    /// A non-blocking database tick deliberately returns while a query waits
    /// for storage. Hosts that can arrange a later owner turn provide a waker
    /// which schedules precisely that turn when storage becomes ready. The
    /// default keeps manually-driven hosts source-compatible.
    fn query_runtime_waker(&self) -> Option<Waker> {
        None
    }
}

/// A locally-originated transaction rejection that was not consumed by an
/// active write waiter.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MutationErrorEvent {
    /// Stable machine-readable rejection code.
    pub code: String,
    /// Human-readable rejection reason.
    pub reason: String,
    /// The rejected local transaction.
    pub transaction: LocalTransactionRecord,
}

/// Binding-facing record for one locally committed transaction.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LocalTransactionRecord {
    /// Stable public identity derived from the core transaction id.
    pub transaction_id: TransactionId,
    /// Transaction semantics used by the commit.
    pub kind: TransactionKind,
    /// Committed transaction records are immutable.
    pub sealed: bool,
    /// Latest authority settlement observed for the transaction.
    pub latest_settlement: TransactionFate,
}

/// Binding-facing transaction kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionKind {
    /// CRDT-style mergeable transaction.
    Mergeable,
    /// Authority-validated exclusive transaction.
    Exclusive,
}

impl From<TxKind> for TransactionKind {
    fn from(kind: TxKind) -> Self {
        match kind {
            TxKind::Mergeable => Self::Mergeable,
            TxKind::Exclusive => Self::Exclusive,
        }
    }
}

/// Binding-facing authority fate for a rejected transaction.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum TransactionFate {
    /// The authority rejected the transaction.
    Rejected {
        /// Stable public identity derived from the core transaction id.
        #[serde(rename = "transactionId")]
        transaction_id: TransactionId,
        /// Stable machine-readable rejection code.
        code: String,
        /// Human-readable rejection reason.
        reason: String,
    },
}

/// Thread-affine callback used by bindings to surface unhandled rejections.
pub type MutationErrorCallback = Rc<dyn Fn(&MutationErrorEvent) + 'static>;

#[cfg(feature = "sync-autopsy")]
/// Debug-build sync trace buffer used by integration-test timeout autopsies.
pub mod sync_autopsy {
    use super::*;

    const MAX_EVENTS: usize = 512;

    static ENABLED: AtomicBool = AtomicBool::new(false);
    static EVENTS: LazyLock<Mutex<VecDeque<String>>> =
        LazyLock::new(|| Mutex::new(VecDeque::with_capacity(MAX_EVENTS)));

    /// Enable passive event capture for the current process.
    pub fn enable() {
        ENABLED.store(true, Ordering::Relaxed);
    }

    /// Clear buffered events.
    pub fn clear() {
        if let Ok(mut events) = EVENTS.lock() {
            events.clear();
        }
    }

    /// Return the current buffered event log.
    pub fn dump() -> String {
        let events = EVENTS.lock().ok();
        let mut out = String::from("sync autopsy events:\n");
        if let Some(events) = events {
            for event in events.iter() {
                out.push_str("  ");
                out.push_str(event);
                out.push('\n');
            }
        } else {
            out.push_str("  <event buffer poisoned>\n");
        }
        out
    }

    /// Append one event to the ring buffer when capture is enabled.
    pub fn record(event: impl Into<String>) {
        if !ENABLED.load(Ordering::Relaxed) {
            return;
        }
        let Ok(mut events) = EVENTS.lock() else {
            return;
        };
        if events.len() == MAX_EVENTS {
            events.pop_front();
        }
        events.push_back(event.into());
    }
}

#[cfg(not(feature = "sync-autopsy"))]
/// No-op sync trace buffer when sync autopsy capture is not compiled in.
pub mod sync_autopsy {
    /// Enable passive event capture for the current process.
    pub fn enable() {}
    /// Clear buffered events.
    pub fn clear() {}
    /// Return the current buffered event log.
    pub fn dump() -> String {
        String::new()
    }
}

/// Poll a ready-immediate thread-affine database future to completion.
///
/// This helper is intentionally tiny: it drives local-lane futures that are
/// expected to complete without an async runtime by using a no-op waker and
/// yielding the current thread when a future reports `Pending`.
pub fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);

    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(value) => return value,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

/// Poll a thread-affine database operation on an auxiliary stack segment when
/// the caller's current stack is nearly exhausted.
///
/// A single owner turn can synchronously poll through storage, validation,
/// maintained-view, and transport layers before an async storage operation
/// yields. Keep that implementation detail from making the public `Db` API
/// depend on a host executor's task-stack size.
pub(crate) struct StackSafeFuture<F> {
    inner: Pin<Box<F>>,
}

impl<F> StackSafeFuture<F> {
    pub(crate) fn new(inner: F) -> Self {
        Self {
            inner: Box::pin(inner),
        }
    }
}

impl<F: Future> Future for StackSafeFuture<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        // Keep a generous red zone: the stack consumed by a poll is not known
        // until it reaches the deepest storage/ingest path. The 8 MiB segment
        // is temporary and released after the poll returns or yields.
        stacker::maybe_grow(4 * 1024 * 1024, 8 * 1024 * 1024, || {
            self.inner.as_mut().poll(context)
        })
    }
}

/// Thread-affine high-level database handle.
pub struct Db<S>
where
    S: OrderedKvStorage,
{
    schema: JazzSchema,
    schema_version_id: SchemaVersionId,
    schema_view_is_fixed: bool,
    schema_views: Rc<RefCell<BTreeMap<SchemaViewId, JazzSchema>>>,
    identity: DbIdentity,
    node: Rc<Node<S>>,
    row_id_source: Rc<RefCell<Box<dyn RowIdSource>>>,
    row_id_source_guarantees_fresh: bool,
    next_now_ms: Rc<Cell<u64>>,
    /// Set only on the private clone owned by one queued mutation operation.
    reserved_tx_id: Option<TxId>,
    /// True only for a future accepted while the shared owner was Open. Such
    /// futures must remain executable while close drains the accepted FIFO.
    owner_operation_admitted: bool,
    // Minted only by the explicitly unsafe trusted-backend open path. SYSTEM
    // itself is an admission identity, not proof that a Db may forge external
    // row provenance.
    backend_attribution: bool,
    #[cfg(test)]
    fail_next_subscription_refresh: Rc<Cell<bool>>,
    #[cfg(test)]
    stall_next_subscription_refresh: Rc<Cell<bool>>,
}

/// Process-local, content-addressed identity for an exact typed schema view.
///
/// Unlike [`SchemaVersionId`], which identifies structural migration lineage,
/// this includes defaults and policy metadata used by a typed client facade.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SchemaViewId([u8; 32]);

impl SchemaViewId {
    /// Stable bytes suitable for binding-level map keys.
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    fn for_schema(schema: &JazzSchema) -> Self {
        let bytes =
            serde_json::to_vec(schema.public_schema()).expect("public schema always serializes");
        Self(blake3::derive_key("jazz typed schema view id v1", &bytes))
    }
}

/// Shared list of live subscriptions. Held by both the `Node` and any
/// [`PeerConnection`], so an inbound sync update can push subscription events
/// through the same path a local write does.
type SubscriptionList = Rc<RefCell<Vec<Weak<RefCell<SubscriptionState>>>>>;
type PendingUpstreamCommands = Rc<RefCell<Vec<PendingUpstreamCommand>>>;
type PendingSubscriptionFinalizations = Rc<RefCell<VecDeque<PendingSubscriptionFinalization>>>;
type LatestCoverageSubscriptions = Rc<RefCell<BTreeMap<CoverageKey, SubscriptionKey>>>;
type UpstreamCoverageRefCounts = Rc<RefCell<BTreeMap<CoverageKey, usize>>>;
type AwaitingInitialAuthorityCoverage = Rc<RefCell<BTreeSet<CoverageKey>>>;
type CoverageRefreshGenerations = Rc<RefCell<BTreeMap<CoverageKey, u64>>>;
type QueryCoverageRegistrations = Rc<RefCell<BTreeMap<SubscriptionKey, QueryCoverageRegistration>>>;
/// Ephemeral evidence that the current authority connection has confirmed a
/// canonical binding view. It is deliberately separate from durable
/// `settled_through`: cursors retain payload possession for repair/dedup, not
/// authority settlement.
type ActiveAuthorityViewReceipts = Rc<RefCell<Option<AuthorityViewReceipts>>>;
type UpstreamSubscriptionOwners =
    Rc<RefCell<BTreeMap<SubscriptionKey, Vec<Weak<RefCell<SubscriptionState>>>>>>;
/// Relay-owned upstream usage sites are distinct from public `SubscriptionStream`
/// owners. A served connection can disappear without dropping a public stream,
/// so each connection retains its own pin on a possibly shared upstream handle.
/// The tuple key separates upstream wire identity, connection lifetime, and
/// downstream read semantics. Local and remote readers on the same connection
/// can pin one authority stream without sharing their local evaluator.
type RelayUpstreamSubscriptionOwners =
    Rc<RefCell<BTreeMap<(SubscriptionKey, u64, ReadViewKey), RelayUpstreamSubscriptionOwner>>>;
type PendingRelaySubscriptionRejections =
    Rc<RefCell<BTreeMap<u64, VecDeque<RelaySubscriptionRejection>>>>;
type SharedTickScheduler = Rc<RefCell<Option<Rc<dyn TickScheduler>>>>;
type QueuedMutationFuture = Pin<Box<dyn Future<Output = Result<(), Error>> + 'static>>;
type QueuedMutationCompletion = Box<dyn FnOnce(Result<(), Error>) + 'static>;
type TransactionWaitObserver = Pin<Box<dyn Future<Output = ()> + 'static>>;
type QueuedMutationAlias = Rc<RefCell<Option<TxId>>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MutationOwnerLifecycle {
    Open,
    Closing,
}

struct QueuedMutationOperation {
    tx_id: Option<TxId>,
    open_tx_id: Option<OpenTransactionId>,
    future: QueuedMutationFuture,
    status: Option<Rc<RefCell<QueuedMutationStatus>>>,
    completion: Option<QueuedMutationCompletion>,
}

enum QueuedMutationStatus {
    Pending,
    Published,
    Failed(Error),
}

/// Authenticated logical destination for an upstream upload retry.
///
/// A transport epoch may change during reconnect, but replaying a receiver's
/// missing-node frontier is only sound to the same remote authority under the
/// same authenticated link identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct UpstreamUploadDestination {
    remote_node: [u8; 16],
    link_identity: AuthorSubject,
}

pub(crate) trait UploadRetryClock {
    fn now_ms(&self) -> u64;
}

struct MonotonicUploadRetryClock {
    started: web_time::Instant,
}

impl MonotonicUploadRetryClock {
    fn new() -> Self {
        Self {
            started: web_time::Instant::now(),
        }
    }
}

impl UploadRetryClock for MonotonicUploadRetryClock {
    fn now_ms(&self) -> u64 {
        web_time::Instant::now()
            .duration_since(self.started)
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}

type SharedUploadRetryClock = Rc<RefCell<Rc<dyn UploadRetryClock>>>;
type WriteStateWaiters = Rc<RefCell<BTreeMap<TxId, Vec<WriteStateWaiter>>>>;
type TransactionAbandonmentTombstones = Rc<RefCell<BTreeSet<OpenTransactionId>>>;
type PermissionAdviceWaiters =
    Rc<RefCell<BTreeMap<PermissionAdviceRequestId, oneshot::Sender<PermissionAdvice>>>>;
type PendingDownstreamFates = Rc<RefCell<Vec<SyncMessage>>>;
struct PendingLocalPublication {
    published: Rc<PublishedTransaction>,
    upload_unit: Option<SyncMessage>,
    settlement: Pin<Box<dyn Future<Output = Result<TxId, Error>> + 'static>>,
}

type PendingLocalPublications = Rc<RefCell<VecDeque<PendingLocalPublication>>>;
type AdmittedUpstreamAuthorities = Rc<RefCell<Vec<AuthorityContext>>>;
const MAX_EDGE_FATE_ROUTES: usize = 1024;
const MAX_EDGE_FATE_ROUTES_PER_TX: usize = 8;

#[derive(Default)]
struct AuthorityViewReceipts {
    connection_epoch: u64,
    confirmation_floor: GlobalTime,
    /// Exact live usage subscriptions confirmed on this authority link.
    ///
    /// Binding-view generations are shared by equal query shapes, so they
    /// cannot distinguish a late update for a detached predecessor.
    subscriptions: BTreeSet<SubscriptionKey>,
    binding_views: BTreeSet<BindingViewKey>,
}

/// An inbound message held across authority selection. It preserves ordinary
/// sync delivery while ensuring traffic staged before selection cannot become
/// the selected connection's fresh view receipt.
struct StagedInboundMessage {
    message: SyncMessage,
    authority_receipt_eligible: bool,
}

struct PendingAuthorityViewUpdate {
    parts: ViewUpdateParts,
    authority_receipt_eligible: bool,
}

struct EdgeFateRoute {
    authority: Option<AuthorityContext>,
    queue: Weak<RefCell<Vec<SyncMessage>>>,
    /// The edge-local acceptance has already been emitted to this exact
    /// downstream session.  The later Core terminal fate remains separately
    /// routable through the same retained obligation.
    edge_acknowledged: bool,
}

/// The immutable identity of a client commit while its edge fate obligation is
/// live.  An edge intentionally keeps a pre-proof upload out of durable
/// transaction history, but it must still enforce history's one-payload-per-id
/// rule across all client connections.  Normalize version order here because
/// transport ordering is not semantically meaningful.
#[derive(Clone, Debug)]
struct EdgeFateCommitIdentity {
    tx: Transaction,
    versions: Vec<VersionRecord>,
}

impl EdgeFateCommitIdentity {
    fn new(tx: &Transaction, versions: &[VersionRecord]) -> Self {
        let mut versions = versions.to_vec();
        versions.sort();
        let mut tx = tx.clone();
        // An edge route compares durable commit identity across a local staged
        // write and redacted carrier retransmissions. Its local policy hint is
        // deliberately excluded from that identity.
        tx.permission_subject = None;
        Self { tx, versions }
    }

    fn matches(&self, other: &Self) -> bool {
        self.tx == other.tx && self.versions == other.versions
    }
}

/// The shared edge obligation for one transaction.
struct EdgeFateObligation {
    identity: EdgeFateCommitIdentity,
    routes: Vec<EdgeFateRoute>,
}

type EdgeFateRoutes = Rc<RefCell<BTreeMap<TxId, EdgeFateObligation>>>;

struct LocalFateRoute {
    queue: Weak<RefCell<Vec<SyncMessage>>>,
    local_acknowledged: bool,
}
type LocalFateRoutes = Rc<RefCell<BTreeMap<TxId, Vec<LocalFateRoute>>>>;

fn register_local_fate_route(
    routes: &LocalFateRoutes,
    tx_id: TxId,
    queue: &PendingDownstreamFates,
) {
    register_local_fate_route_with_acknowledgement(routes, tx_id, queue, false);
}

fn register_local_fate_observer(
    routes: &LocalFateRoutes,
    tx_id: TxId,
    queue: &PendingDownstreamFates,
) {
    register_local_fate_route_with_acknowledgement(routes, tx_id, queue, true);
}

fn register_local_fate_route_with_acknowledgement(
    routes: &LocalFateRoutes,
    tx_id: TxId,
    queue: &PendingDownstreamFates,
    local_acknowledged: bool,
) {
    let mut routes = routes.borrow_mut();
    routes.retain(|_, pending| {
        pending.retain(|candidate| candidate.queue.upgrade().is_some());
        !pending.is_empty()
    });
    if routes.get(&tx_id).is_some_and(|pending| {
        pending.iter().any(|candidate| {
            candidate
                .queue
                .upgrade()
                .is_some_and(|candidate| Rc::ptr_eq(&candidate, queue))
        })
    }) {
        return;
    }
    routes.entry(tx_id).or_default().push(LocalFateRoute {
        queue: Rc::downgrade(queue),
        local_acknowledged,
    });
}

async fn queue_local_acknowledgements<S>(routes: &LocalFateRoutes, node: &SharedNodeState<S>)
where
    S: OrderedKvStorage,
{
    let tx_ids = routes.borrow().keys().copied().collect::<Vec<_>>();
    let mut durable = BTreeSet::new();
    let mut node = node.lock().await;
    for tx_id in tx_ids {
        if node
            .transaction_state(tx_id)
            .await
            .is_some_and(|(_, _, durability)| durability >= DurabilityTier::Local)
        {
            durable.insert(tx_id);
        }
    }
    drop(node);
    let mut routes = routes.borrow_mut();
    routes.retain(|tx_id, pending| {
        let locally_durable = durable.contains(tx_id);
        pending.retain_mut(|route| {
            let Some(queue) = route.queue.upgrade() else {
                return false;
            };
            if locally_durable && !route.local_acknowledged {
                queue.borrow_mut().push(SyncMessage::FateUpdate {
                    tx_id: *tx_id,
                    fate: Fate::Pending,
                    global_time: None,
                    durability: Some(DurabilityTier::Local),
                });
                route.local_acknowledged = true;
            }
            true
        });
        !pending.is_empty()
    });
}

fn route_local_fate(routes: &LocalFateRoutes, tx_id: TxId, fate: &SyncMessage) {
    let terminal = matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(_),
            ..
        } | SyncMessage::FateUpdate {
            durability: Some(DurabilityTier::Global),
            ..
        }
    );
    let mut routes = routes.borrow_mut();
    let Some(pending) = routes.get_mut(&tx_id) else {
        return;
    };
    pending.retain(|candidate| {
        let Some(queue) = candidate.queue.upgrade() else {
            return false;
        };
        queue.borrow_mut().push(fate.clone());
        !terminal
    });
    if pending.is_empty() {
        routes.remove(&tx_id);
    }
}

/// Deliver the edge's own admission fate through the same route registry that
/// later carries the selected Core fate.  This avoids a direct-response path
/// that would acknowledge only the tick currently handling the upload (and
/// would duplicate a retransmitted upload), while a rejection retires the
/// obligation because there is no admitted unit for Core to settle.
fn route_edge_admission_fate(routes: &EdgeFateRoutes, tx_id: TxId, fate: &SyncMessage) {
    let terminal = matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(_),
            ..
        }
    );
    let mut routes = routes.borrow_mut();
    let Some(obligation) = routes.get_mut(&tx_id) else {
        return;
    };
    obligation.routes.retain_mut(|route| {
        let Some(queue) = route.queue.upgrade() else {
            return false;
        };
        if terminal || !route.edge_acknowledged {
            queue.borrow_mut().push(fate.clone());
            route.edge_acknowledged = true;
        }
        !terminal
    });
    if obligation.routes.is_empty() {
        routes.remove(&tx_id);
    }
}

async fn collect_local_replay_commit_units<S>(
    node: &mut NodeState<S>,
    tx_id: TxId,
    visited: &mut BTreeSet<TxId>,
    units: &mut Vec<(TxId, SyncMessage)>,
) -> Result<(), Error>
where
    S: OrderedKvStorage,
{
    if !visited.insert(tx_id) {
        return Ok(());
    }
    let unit = node.commit_unit_for(tx_id).await?;
    let SyncMessage::CommitUnit { versions, .. } = &unit else {
        unreachable!("commit_unit_for always returns a commit unit")
    };
    let parents = versions
        .iter()
        .flat_map(crate::protocol::VersionRecord::parents)
        .collect::<BTreeSet<_>>();
    for parent in parents {
        Box::pin(collect_local_replay_commit_units(
            node, parent, visited, units,
        ))
        .await?;
    }
    units.push((tx_id, unit));
    Ok(())
}

/// A parked fate either awaits its first admitted upstream or belongs to one
/// admitted upstream epoch. Drop routes for departed/replaced sessions (and
/// dead subscriber queues) eagerly: retaining a weak queue alone would let
/// arbitrary uploads grow this registry forever.
fn prune_edge_fate_routes(
    routes: &mut BTreeMap<TxId, EdgeFateObligation>,
    admitted: Option<AuthorityContext>,
) {
    routes.retain(|_, obligation| {
        obligation.routes.retain(|route| {
            route.queue.upgrade().is_some()
                && match (route.authority, admitted) {
                    (None, _) => true,
                    (Some(route), Some(admitted)) => admitted.same_admitted_link(route),
                    (Some(_), None) => false,
                }
        });
        !obligation.routes.is_empty()
    });
}
type SharedMutationErrors = Rc<RefCell<MutationErrorState>>;
type ShapeRegistrationKey = (ShapeId, ReadViewKey);

/// Per-subscriber state for a shape/read-view registration.
///
/// A missing runtime shape is ambiguous: catalogue admission is temporary,
/// whereas a capability rejection is permanent for this connection. Keep the
/// distinction at the protocol boundary instead of inferring either from the
/// node's registered-shape map.
#[derive(Clone)]
enum SubscriberShapeRegistration {
    Registered(RegisterShapeOptions),
    PendingCatalogueAdmission(RegisterShapeOptions),
    RejectedUnsupportedCapability(String),
}
impl SubscriberShapeRegistration {
    fn owns_node_shape(&self) -> bool {
        matches!(
            self,
            Self::Registered(_) | Self::PendingCatalogueAdmission(_)
        )
    }
}

fn default_cell_for_column_type(column_type: &GrooveColumnType, default: &Value) -> Value {
    match (column_type, default) {
        (GrooveColumnType::Nullable(_), Value::Nullable(_)) => default.clone(),
        (GrooveColumnType::Nullable(_), default) => {
            Value::Nullable(Some(Box::new(default.clone())))
        }
        _ => default.clone(),
    }
}

fn schema_view_column_default(column: &crate::schema::ColumnSchema) -> Result<Value, Error> {
    if let Some(default) = &column.default {
        return Ok(default.clone());
    }
    if matches!(column.column_type, GrooveColumnType::Nullable(_)) {
        return Ok(Value::Nullable(None));
    }
    Err(Error::new(
        ErrorCode::Schema,
        format!(
            "schema view column {} requires a migration default",
            column.name
        ),
    ))
}

fn direct_schema_view_lens(
    source: &JazzSchema,
    target: &JazzSchema,
) -> Result<(MigrationLens, Vec<String>, Vec<String>), Error> {
    let source_id = source.version_id();
    let target_id = target.version_id();
    let mut table_lenses = Vec::new();
    for source_table in &source.tables {
        let Some(target_table) = target
            .tables
            .iter()
            .find(|table| table.name == source_table.name)
        else {
            continue;
        };
        if source_table.references != target_table.references {
            return Err(Error::new(
                ErrorCode::Schema,
                format!(
                    "schema view changes references on {} without an explicit lens",
                    target_table.name
                ),
            ));
        }
        if source_table.merge_strategies != target_table.merge_strategies {
            return Err(Error::new(
                ErrorCode::Schema,
                format!(
                    "schema view changes merge strategies on {} without an explicit lens",
                    target_table.name
                ),
            ));
        }
        if source_table.indexed_columns != target_table.indexed_columns {
            return Err(Error::new(
                ErrorCode::Schema,
                format!(
                    "schema view changes indices on {} without explicit index admission",
                    target_table.name
                ),
            ));
        }
        let mut ops = Vec::new();
        for target_column in &target_table.columns {
            match source_table
                .columns
                .iter()
                .find(|column| column.name == target_column.name)
            {
                Some(source_column) if source_column.column_type == target_column.column_type => {}
                Some(_) => {
                    return Err(Error::new(
                        ErrorCode::Schema,
                        format!(
                            "schema view changes type of {}.{} without an explicit lens",
                            target_table.name, target_column.name
                        ),
                    ));
                }
                None => ops.push(LensOp::AddColumn {
                    column: target_column.name.clone(),
                    default: schema_view_column_default(target_column)?,
                }),
            }
        }
        for source_column in &source_table.columns {
            if target_table
                .columns
                .iter()
                .all(|column| column.name != source_column.name)
            {
                ops.push(LensOp::DropColumn {
                    column: source_column.name.clone(),
                    backwards_default: schema_view_column_default(source_column)?,
                });
            }
        }
        table_lenses.push(TableLens {
            source_table: source_table.name.clone(),
            target_table: target_table.name.clone(),
            ops,
        });
    }
    let new_tables = target
        .tables
        .iter()
        .filter(|table| source.tables.iter().all(|source| source.name != table.name))
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    let dropped_tables = source
        .tables
        .iter()
        .filter(|table| target.tables.iter().all(|target| target.name != table.name))
        .map(|table| table.name.clone())
        .collect::<Vec<_>>();
    Ok((
        MigrationLens::new(source_id, target_id, table_lenses).expect("valid migration lens"),
        new_tables,
        dropped_tables,
    ))
}

struct WriteStateWaiter {
    id: u64,
    notify: WriteStateWaiterNotify,
}

enum WriteStateWaiterNotify {
    Future(oneshot::Sender<()>),
}

#[derive(Default)]
struct MutationErrorState {
    callback: Option<MutationErrorCallback>,
    pending: BTreeMap<TxId, MutationErrorEvent>,
}

#[derive(Clone)]
enum PendingUpstreamCommand {
    Subscribe(PendingUpstreamSubscription),
    Unsubscribe(SubscriptionKey),
    AuthorizationScopeIntent {
        request_id: PermissionAdviceRequestId,
        action: PermissionAdviceAction,
        /// Present only when an existing request crosses an upstream boundary.
        /// A fresh request binds its claims when its selected authority admits
        /// it; a reconnect must preserve the original immutable binding.
        session_claim_binding: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        /// A backend-selected snapshot which must cross the upstream boundary.
        /// This remains separate from the locally captured lease binding: direct
        /// sessions authenticate at transport admission and never self-delegate.
        delegated_session: Option<crate::protocol::DelegatedSessionBinding>,
    },
}

#[derive(Clone)]
struct PendingUpstreamSubscription {
    subscription: SubscriptionKey,
    shape: ValidatedQuery,
    binding: Binding,
    opts: RegisterShapeOptions,
    identity: AuthorSubject,
    /// Immutable logical session context for a relay-multiplexed request.
    /// Direct upstream consumers use `None` and authenticate at transport.
    policy_binding: Option<(AuthorSubject, BTreeMap<String, Value>)>,
}

struct QueryCoverageRegistration {
    coverage: CoverageKey,
    subscription: PendingUpstreamSubscription,
    ref_count: usize,
}

#[derive(Clone)]
struct UpstreamCoverageHandle {
    coverage: CoverageKey,
    subscription: SubscriptionKey,
}

/// A drop-safe request to retire one public subscription. It carries only
/// stable local/runtime identities and outer ownership bookkeeping; the node
/// runtime drains it under its ordinary async mutex before touching Groove.
struct PendingSubscriptionFinalization {
    /// Keep the state alive until the node has retired the *current* runtime
    /// handles.  Capturing an ID at drop time is racy with catalogue/runtime
    /// replacement: refresh can install a successor before the queued command
    /// reaches the node.
    state: Option<Rc<RefCell<SubscriptionState>>>,
    /// The fallible opening guard can run before a public stream state exists.
    /// It is never subject to runtime replacement, so this narrowly scoped
    /// fallback may carry its just-created Groove handle directly.
    opening_upstream: Vec<UpstreamCoverageHandle>,
    opening_local: Option<(u64, groove::ivm::SubscriptionId)>,
    acknowledgement: Option<oneshot::Sender<()>>,
}

struct OpenedUpstreamCoverage {
    handles: Vec<UpstreamCoverageHandle>,
    awaits_initial_authority_response: bool,
}

struct CoverageGroup {
    shape: ValidatedQuery,
    binding: Binding,
    /// Immutable context represented by this relay-only coverage key.
    policy_binding: (AuthorSubject, BTreeMap<String, Value>),
    /// Where `policy_binding` came from at admission.
    ///
    /// A direct subscriber's binding is the trusted connection's current
    /// authenticated snapshot, so claim refresh replaces it before the
    /// maintained view is reopened. A delegated binding was asserted by the
    /// trusted relay for this exact usage site and must remain immutable even
    /// when the relay transport refreshes its own session. In particular, do
    /// not infer this from the identity: SYSTEM is a valid delegated subject
    /// in internal paths.
    policy_binding_origin: CoveragePolicyBindingOrigin,
    subscribers: BTreeSet<SubscriptionKey>,
    pending_initial_subscribers: BTreeSet<SubscriptionKey>,
    /// #2653: one generated opening awaiting semantic transport acceptance.
    /// Retry its exact receipt before advancing this group again.
    pending_initial_update: Option<(SubscriptionKey, SyncMessage)>,
    /// Remaining recipients of one generated incremental publication.
    pending_incremental_updates: VecDeque<(SubscriptionKey, SyncMessage)>,
    /// Query runtime token governing the unaccepted publications above.
    /// Unlike catalogue lineage sequence, it changes for same-version policy edits.
    publication_runtime_token: Option<u64>,
    /// Claim revision whose replacement opening reset is currently being
    /// delivered. A retry of that same revision resumes this per-subscriber
    /// cursor; a newer admission revision starts every live usage over.
    pending_claim_refresh_revision: Option<u64>,
    initialized: bool,
    /// The usage-site subscription whose authority result supplies this
    /// group's membership. An authoritative server evaluates the incoming
    /// downstream subscription directly; a relay that must ask its upstream
    /// instead uses its separately allocated upstream usage handle.
    authority_result_subscription: SubscriptionKey,
    upstream_subscription: SubscriptionKey,
    upstream_opts: RegisterShapeOptions,
    awaiting_upstream_settlement: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CoveragePolicyBindingOrigin {
    DirectAdmitted,
    Delegated,
}

/// One downstream connection's pin on a propagated coverage stream.
///
/// `upstream_subscription` is a usage-site wire handle, while `coverage`
/// identifies this connection's local evaluator. Only releasing the final pin
/// retires the upstream handle; local evaluator cleanup remains per connection.
struct RelayUpstreamSubscriptionOwner {
    request: PendingUpstreamSubscription,
    scalar_authority_revision: u64,
    scalar_reconciliation: ScalarReconciliation,
    downstream_connection_epoch: u64,
    coverage: CoverageKey,
    policy_binding: (AuthorSubject, BTreeMap<String, Value>),
    downstream_subscriptions: BTreeSet<SubscriptionKey>,
}

/// An authority rejection that must be delivered on the originating downstream
/// connection before that connection's served coverage is retired.
struct RelaySubscriptionRejection {
    coverage: CoverageKey,
    /// Exact authenticated policy scope that admitted the downstream usages.
    /// Teardown must use this scope rather than a bare wire key, which is
    /// ambiguous when one relay multiplexes the same shape across readers.
    policy_binding: (AuthorSubject, BTreeMap<String, Value>),
    downstream_subscriptions: BTreeSet<SubscriptionKey>,
    reason: SubscribeRejectReason,
}

/// Authority-derived scope identity retained for a support subscription.
/// Never constructed from the caller's wire payload.
#[derive(Clone, Debug, PartialEq)]
struct AuthorizedScopePurpose {
    key: crate::protocol::AuthorizationSupportScopeKey,
    operation: crate::protocol::AuthorizationOperationKey,
    action: PermissionAdviceAction,
    expected_support: BTreeSet<(ShapeId, BindingId)>,
}

// Compatibility spelling retained for module-local tests while the actual
// implementation is the shared authority proof primitive.
#[cfg(test)]
type ScopeAggregate = AuthorityScopeAggregate;

/// One receipt-bound authorization operation owned by one admitted upstream.
///
/// This state deliberately lives on `ConnectionLink::Upstream`: a receipt is
/// only meaningful for the authenticated authority epoch that delivered it.
/// The manager additionally records every support subscription, since an
/// aggregate receipt names only the final completing subscription.
struct AuthorizationScopeLeaseRequest {
    action: PermissionAdviceAction,
    /// Immutable requesting-session claims captured when this upstream advice
    /// operation is allocated. Receipts are evaluated on an Upstream link,
    /// which has no subscriber-side ambient claims to consult.
    session_claim_binding: (AuthorSubject, BTreeMap<String, Value>),
    /// Present only for a host-admitted backend or scope-isolated relay request.
    delegated_session: Option<crate::protocol::DelegatedSessionBinding>,
    /// Every local caller sharing this authority hydration.  The first id is
    /// the wire correlation id; later ids never cause another support view.
    waiters: BTreeSet<PermissionAdviceRequestId>,
    /// The authority has accepted the one-shot wire intent. Until then the
    /// upstream command remains pending and must retry after backpressure;
    /// merely allocating this local lease must not make a later tick mistake
    /// it for an already-sent hydration request.
    intent_sent: bool,
    key: Option<crate::protocol::AuthorizationSupportScopeKey>,
    lease: Option<AuthorizationScopeLease>,
    owner: Option<AuthorizationScopeOwnerToken>,
    clause_count: Option<u16>,
    applied_clauses: BTreeMap<u16, (SubscriptionKey, crate::time::GlobalTime, u64)>,
}

/// Per-upstream admission manager for scope receipts and their retained leases.
#[derive(Default)]
struct AuthorizationScopeLeaseManager {
    registry: AuthorizationScopeRegistry,
    requests: BTreeMap<PermissionAdviceRequestId, AuthorizationScopeLeaseRequest>,
}

/// One authority-compiled support hydration retained only while every
/// authority revision and global cut it represents remains current.  It is
/// keyed by the support scope rather than the candidate operation, so distinct
/// rows/patches can reuse hydration but still evaluate their own action.
#[derive(Clone)]
struct ServedAuthorizationScopeHydration {
    clauses: Vec<ServedAuthorizationScopeClause>,
    receipt: AuthorizationScopeReceipt,
}

#[derive(Clone)]
struct ServedAuthorizationScopeClause {
    subscription: SubscriptionKey,
    register: SyncMessage,
    subscribe: SyncMessage,
    view: SyncMessage,
}

/// Locally-authored transactions awaiting upload, oldest first. Shared with
/// upstream [`PeerConnection`]s, each of which tracks how far it has shipped.
type Outbox = Rc<RefCell<UploadOutbox>>;

#[derive(Default)]
struct UploadOutbox {
    entries: VecDeque<PendingUpload>,
    tx_ids: HashSet<TxId>,
    authority_members: HashSet<TxId>,
    authority_receipts: HashSet<TxId>,
}

impl UploadOutbox {
    fn push(&mut self, pending: PendingUpload) -> bool {
        if !self.tx_ids.insert(pending.tx_id) {
            return false;
        }
        if let Some(SyncMessage::AuthorityPublication(publication)) = &pending.unit {
            self.authority_members
                .extend(publication.commits.iter().map(|unit| unit.tx.tx_id));
        }
        self.entries.push_back(pending);
        true
    }

    fn iter(&self) -> impl DoubleEndedIterator<Item = &PendingUpload> + ExactSizeIterator {
        self.entries.iter()
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn retain(&mut self, mut keep: impl FnMut(&PendingUpload) -> bool) {
        self.entries.retain(|pending| keep(pending));
        self.tx_ids.clear();
        self.tx_ids
            .extend(self.entries.iter().map(|pending| pending.tx_id));
        self.reindex_authority_members();
    }

    fn reindex_authority_members(&mut self) {
        self.authority_members.clear();
        for pending in &self.entries {
            if let Some(SyncMessage::AuthorityPublication(publication)) = &pending.unit {
                self.authority_members
                    .extend(publication.commits.iter().map(|unit| unit.tx.tx_id));
            }
        }
        self.authority_receipts
            .retain(|tx_id| self.authority_members.contains(tx_id));
    }

    fn remove_released(&mut self, released: &mut HashSet<TxId>) -> HashSet<TxId> {
        if !self.authority_members.is_empty() {
            self.authority_receipts.extend(
                released
                    .iter()
                    .filter(|tx_id| self.authority_members.contains(tx_id))
                    .copied(),
            );
            let completed = self
                .entries
                .iter()
                .filter(|pending| match &pending.unit {
                    Some(SyncMessage::AuthorityPublication(publication)) => publication
                        .commits
                        .iter()
                        .all(|unit| self.authority_receipts.contains(&unit.tx.tx_id)),
                    _ => released.contains(&pending.tx_id),
                })
                .map(|pending| pending.tx_id)
                .collect::<HashSet<_>>();
            self.retain(|pending| !completed.contains(&pending.tx_id));
            return completed;
        }
        // Ordinary single-transaction uploads retain their prefix fast path.
        let completed = released.clone();
        while self
            .entries
            .front()
            .is_some_and(|pending| released.remove(&pending.tx_id))
        {
            let pending = self
                .entries
                .pop_front()
                .expect("released outbox front remains present");
            self.tx_ids.remove(&pending.tx_id);
        }
        if released.is_empty() {
            return completed;
        }
        self.retain(|pending| !released.contains(&pending.tx_id));
        completed
    }
}

#[derive(Clone)]
struct PendingUpload {
    tx_id: TxId,
    unit: Option<SyncMessage>,
}

/// Queue an upload, letting an exact canonical unit supersede either a tx-id
/// recovery marker or an earlier same-transaction reconstruction.
fn queue_pending_upload_in(outbox: &Outbox, tx_id: TxId, unit: Option<SyncMessage>) -> bool {
    let mut outbox = outbox.borrow_mut();
    if let Some(pending) = outbox
        .entries
        .iter_mut()
        .find(|pending| pending.tx_id == tx_id)
    {
        let Some(unit) = unit else {
            return false;
        };
        if pending.unit.as_ref() == Some(&unit) {
            return false;
        }
        // A reconnect can reconstruct an upload after its route is registered
        // but before subscriber ingest queues the exact inbound unit. The
        // canonical payload must win even when both entries have a body.
        pending.unit = Some(unit);
        outbox.reindex_authority_members();
        return true;
    }
    outbox.push(PendingUpload { tx_id, unit });
    true
}

/// Whether a transaction has reached the requested application-visible wait
/// boundary. Local persistence precedes authority fate assignment, while
/// remote durability is successful only after an Accepted fate.
pub(crate) fn transaction_satisfies_wait(
    fate: &Fate,
    global_time: Option<GlobalTime>,
    durability: DurabilityTier,
    tier: DurabilityTier,
) -> bool {
    durability >= tier
        && (tier <= DurabilityTier::Local || matches!(fate, Fate::Accepted))
        && (tier < DurabilityTier::Global || global_time.is_some())
}

/// Application-visible fate and durability for a local write transaction.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct WriteState {
    /// Latest authority fate observed by this `Db`.
    pub fate: Fate,
    /// Authority-assigned Global timestamp, once observed.
    ///
    /// Global durability is not a completed Global wait without this receipt.
    pub global_time: Option<GlobalTime>,
    /// Highest durability tier observed by this `Db`.
    pub durability: DurabilityTier,
}

/// Explicit client durability cadence while the first server snapshot is
/// loading.
///
/// A crash can lose up to `M - 1` writes since the last completed boundary,
/// where `M` is this value. Older completed boundaries recover from the
/// storage WAL. The final partial initial-sync batch is always flushed before
/// the client returns to per-write durability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InitialSyncFlushCadence(NonZeroUsize);

impl InitialSyncFlushCadence {
    /// The client default: a durability boundary every 512 initial-sync writes.
    pub const DEFAULT: Self = Self(NonZeroUsize::new(512).expect("non-zero"));

    /// Create a cadence with one durability boundary per `writes` initial-sync
    /// writes.
    pub const fn every(writes: NonZeroUsize) -> Self {
        Self(writes)
    }

    /// Number of writes between completed durability boundaries.
    pub const fn writes(self) -> usize {
        self.0.get()
    }
}

/// Usage-site query coverage attachment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryAttachment {
    subscriptions: Vec<SubscriptionKey>,
    required_after: Vec<(BindingViewKey, u64)>,
    /// A memory-only foreground reads local state from its durable owner.
    /// That delivery is required independently of any remote authority receipt.
    requires_delivery_receipt: bool,
    /// Edge/Global coverage is live authority evidence, not merely a newer
    /// durable view generation.
    requires_current_authority_receipt: bool,
    registrations: Vec<SubscriptionKey>,
    refreshes: Vec<(CoverageKey, u64)>,
}

impl QueryAttachment {
    /// Unique usage id; LocalOnly attachments retain it without sending a wire subscription.
    pub fn subscription(&self) -> SubscriptionKey {
        self.subscriptions[0]
    }
}

/// Future that resolves when a database observes a write-state change.
///
/// This is a wake primitive: callers should read [`Db::write_state`] before
/// registering it, read again after registration, and then re-read after it
/// resolves.
pub struct WriteStateChange {
    waiters: WriteStateWaiters,
    tx_id: TxId,
    waiter_id: u64,
    receiver: oneshot::Receiver<()>,
}

impl Future for WriteStateChange {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for WriteStateChange {
    fn drop(&mut self) {
        let mut waiters = self.waiters.borrow_mut();
        let Some(tx_waiters) = waiters.get_mut(&self.tx_id) else {
            return;
        };
        tx_waiters.retain(|waiter| waiter.id != self.waiter_id);
        let empty = tx_waiters.is_empty();
        if empty {
            waiters.remove(&self.tx_id);
        }
    }
}

/// Cancel-safe future for one authoritative permission preflight.
pub struct PermissionAdviceFuture {
    waiters: PermissionAdviceWaiters,
    request_id: PermissionAdviceRequestId,
    receiver: oneshot::Receiver<PermissionAdvice>,
}

impl Future for PermissionAdviceFuture {
    type Output = PermissionAdvice;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.receiver).poll(cx) {
            Poll::Ready(Ok(advice)) => Poll::Ready(advice),
            Poll::Ready(Err(_)) => Poll::Ready(PermissionAdvice::Unknown),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl PermissionAdviceFuture {
    /// Opaque id used to cancel only this request.
    pub fn request_id(&self) -> PermissionAdviceRequestId {
        self.request_id
    }
}

impl Drop for PermissionAdviceFuture {
    fn drop(&mut self) {
        self.waiters.borrow_mut().remove(&self.request_id);
    }
}

mod catalogue;
mod lifecycle;
mod mutations;
pub use mutations::{
    JsonSetEdit, LargeValueUpdate, LargeValueUpdatePage, LargeValueUpdateSplice,
    StreamingMutationKind, StreamingValueUpload,
};
mod reads;
#[doc(hidden)]
pub use reads::BindingHydrationError;
mod subscriptions;
pub(crate) mod terminal_record;
mod transactions;

/// Counts produced while servicing non-blocking database connection work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DbTickStats {
    /// Number of live subscriptions that received a queued event.
    pub subscription_events: usize,
    /// Number of connection ticks that applied remote sync state locally.
    pub remote_sync_applied: usize,
}

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Reserve local transaction-clock positions through `high_water` before a
    /// trusted native foreground host reuses a node identity.
    pub async fn reserve_minted_tx_time_after(&self, high_water: TxTime) -> Result<(), Error> {
        self.node
            .node()
            .lock()
            .await
            .reserve_tx_time_after(high_water)?;
        Ok(())
    }

    /// Return the HLC high-water mark for locally minted transactions.
    pub async fn minted_tx_time_high_water(&self) -> TxTime {
        self.node.node().lock().await.tx_time_high_water()
    }
}

mod node_runtime;
use node_runtime::register_upstream_subscription_owner;
pub use node_runtime::{ConnectionSessionContext, Node, Transport};
mod peer_connection;
mod row_availability;
mod row_version_repairs;
use peer_connection::{ConnectionLink, schedule_tick_in};
pub use peer_connection::{PeerConnection, ResumeCursor};
mod config;
pub use config::{
    ClientRelayScope, DbConfig, DbIdentity, ProductionRowIdSource, RowIdSource, SeededRowIdSource,
};

/// One-shot read options.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ReadOpts {
    /// Durability tier that gates the first result.
    pub tier: DurabilityTier,
    /// Whether own local updates are visible immediately.
    pub local_updates: LocalUpdates,
    /// Whether evaluation may propagate upstream.
    pub propagation: Propagation,
    /// Include current rows whose deletion winner is `Deleted`.
    pub include_deleted: bool,
    /// Semantic read view to evaluate against.
    pub read_view: ReadViewSpec,
}

impl Default for ReadOpts {
    fn default() -> Self {
        Self {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Immediate,
            propagation: Propagation::Full,
            include_deleted: false,
            read_view: ReadViewSpec::default(),
        }
    }
}

impl ReadOpts {
    /// Evaluate the query as a live head branch composed over an optional base.
    pub fn branch_view(mut self, head: BranchSelector, base: Option<BranchViewBase>) -> Self {
        self.read_view = ReadViewSpec::branch_view(head, base);
        self
    }
}

/// Own-write overlay policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum LocalUpdates {
    /// Include local writes immediately.
    Immediate,
    /// Defer local writes until the requested tier observes them.
    Deferred,
}

/// Read propagation policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum Propagation {
    /// Full propagation may be used by future remote paths.
    Full,
    /// Evaluate only against local knowledge.
    LocalOnly,
}

/// Public API error with stable machine-readable codes.
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Error {
    /// Stable error code.
    pub code: ErrorCode,
    /// Human-readable detail.
    pub message: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code {
            ErrorCode::TransactionConflict => {
                write!(formatter, "(transaction_conflict): {}", self.message)
            }
            _ => write!(formatter, "{:?}: {}", self.code, self.message),
        }
    }
}

impl std::error::Error for Error {}

impl Error {
    fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

fn transaction_abandoned(open_tx_id: OpenTransactionId) -> Error {
    Error::new(
        ErrorCode::Protocol,
        format!("transaction handle was abandoned: {open_tx_id}"),
    )
}

fn row_already_deleted(row: RowUuid) -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        format!("row already deleted: {}", row.0),
    )
}

fn read_for_write_denied(operation: &str, table: &str) -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        format!(
            "read policy denied {operation} on table {table}: the operation requires read permission on the target row"
        ),
    )
}

/// Stable API error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ErrorCode {
    /// Schema validation failed.
    Schema,
    /// Query validation or binding failed.
    Query,
    /// Write was rejected.
    WriteRejected,
    /// An exclusive transaction's fixed snapshot was invalidated locally.
    TransactionConflict,
    /// Storage failed.
    Storage,
    /// Protocol or local node operation failed.
    Protocol,
    /// Local transport queue is full and the operation should be retried later.
    Backpressure,
    /// Requested observation is not locally available in this slice.
    NotObserved,
    /// Historical read must be evaluated by a complete-history server.
    HistoricalReadRequiresServer,
}

impl From<crate::node::Error> for Error {
    fn from(error: crate::node::Error) -> Self {
        let code = match &error {
            crate::node::Error::HistoricalReadRequiresServer => {
                ErrorCode::HistoricalReadRequiresServer
            }
            crate::node::Error::Storage(_) | crate::node::Error::Groove(_) => ErrorCode::Storage,
            crate::node::Error::Query(_) => ErrorCode::Query,
            crate::node::Error::TransactionConflict => ErrorCode::TransactionConflict,
            crate::node::Error::TableNotFound(_)
            | crate::node::Error::UnsupportedColumnType(_)
            | crate::node::Error::InvalidMergeableCommit(_) => ErrorCode::Schema,
            _ => ErrorCode::Protocol,
        };
        Self::new(code, error.to_string())
    }
}

impl From<QueryError> for Error {
    fn from(error: QueryError) -> Self {
        Self::new(ErrorCode::Query, error.to_string())
    }
}

#[doc(hidden)]
pub mod doctest_support {
    use std::collections::BTreeMap;
    use std::future::Future;

    use groove::records::Value;
    pub use groove::storage::MemoryStorage;

    use crate::db::{Db, DbConfig, DbIdentity, Error, RowCells, SeededRowIdSource};
    use crate::ids::{AuthorSubject, NodeUuid};
    use crate::schema::JazzSchema;
    use crate::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

    /// Poll a ready-immediate Db future in examples.
    pub fn block_on<F: Future>(future: F) -> F::Output {
        crate::db::block_on(future)
    }

    /// Example schema used by Db doctests.
    pub fn schema() -> JazzSchema {
        let source = SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean),
            )
            .build();
        crate::schema::JazzSchema::new(&source).expect("Db doctest public schema compiles")
    }

    /// Open a fresh Db over in-memory storage.
    pub async fn open_todos_db() -> Result<Db<MemoryStorage>, Error> {
        let schema = schema();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        Db::open(DbConfig {
            schema,
            storage: MemoryStorage::new(&refs).expect("valid memory storage families"),
            identity: DbIdentity {
                node: NodeUuid::from_bytes([0x11; 16]),
                author: AuthorSubject::for_test_bytes([0xa1; 16]),
            },
            id_source: Some(Box::new(SeededRowIdSource::new(0x1111))),
        })
        .await
    }

    /// Todo row payload for examples.
    pub fn todo_cells(title: &str, done: bool) -> RowCells {
        BTreeMap::from([
            ("title".to_owned(), Value::String(title.to_owned())),
            ("done".to_owned(), Value::Bool(done)),
        ])
    }
}

fn effective_read_tier(opts: &ReadOpts) -> DurabilityTier {
    if opts.local_updates == LocalUpdates::Immediate {
        opts.tier.max(DurabilityTier::Local)
    } else {
        opts.tier
    }
}

fn upstream_register_shape_options(
    tier: DurabilityTier,
    read_view: ReadViewSpec,
    upstream_durability_floor: DurabilityTier,
) -> RegisterShapeOptions {
    RegisterShapeOptions {
        tier: remote_subscription_tier(tier, upstream_durability_floor),
        read_view,
        // LocalOnly controls whether the caller attaches a remote usage.
        // Every usage that crosses a node boundary propagates normally.
        propagate_upstream: true,
        ..RegisterShapeOptions::default()
    }
}

fn remote_subscription_tier(
    tier: DurabilityTier,
    upstream_durability_floor: DurabilityTier,
) -> DurabilityTier {
    tier.max(upstream_durability_floor)
}

fn ensure_default_read_view(opts: &ReadOpts) -> Result<(), Error> {
    if opts.read_view.is_default() {
        return Ok(());
    }
    Err(Error::new(
        ErrorCode::Query,
        "non-default read_view is not supported yet; reads currently execute against the current/default view",
    ))
}

fn ensure_supported_read_view(opts: &ReadOpts) -> Result<(), Error> {
    if matches!(
        opts.read_view.source,
        ReadViewSourceSpec::Current
            | ReadViewSourceSpec::BranchView { .. }
            | ReadViewSourceSpec::Snapshot { .. }
    ) {
        return Ok(());
    }
    Err(Error::new(
        ErrorCode::Query,
        "this read_view combination is not supported yet",
    ))
}

fn ensure_supported_subscription_read_opts(opts: &ReadOpts) -> Result<(), Error> {
    if opts.include_deleted {
        return Err(Error::new(
            ErrorCode::Query,
            "live subscriptions do not support include_deleted yet",
        ));
    }
    ensure_supported_read_view(opts)
}

fn ensure_supported_register_shape_read_view(opts: &RegisterShapeOptions) -> Result<(), Error> {
    let read_opts = ReadOpts {
        read_view: opts.read_view.clone(),
        ..ReadOpts::default()
    };
    ensure_supported_read_view(&read_opts)
}

fn ensure_supported_register_shape_options(
    opts: &RegisterShapeOptions,
    local_receiver: bool,
    peer_role: PeerRole,
    delegated_session_capability: bool,
) -> Result<(), Error> {
    ensure_supported_register_shape_read_view(opts)?;
    if !opts.propagate_upstream {
        return Err(Error::new(
            ErrorCode::Query,
            "remote subscriptions cannot disable upstream propagation; LocalOnly is a local read setting",
        ));
    }
    if opts.binding_source == BindingSource::RelayAuthoritySession && !delegated_session_capability
    {
        return Err(Error::new(
            ErrorCode::Query,
            "relay authority-session bindings require a live server-admitted scope-isolated relay capability",
        ));
    }
    let supported = match (local_receiver, peer_role) {
        (true, _) | (false, PeerRole::Relay) => opts.tier >= DurabilityTier::Local,
        (false, PeerRole::ClientLink { .. }) => opts.tier == DurabilityTier::Global,
    };
    if !supported {
        let message = match (local_receiver, peer_role) {
            (true, _) => {
                "Local sync subscription serving requires at least local-tier registration"
            }
            (false, PeerRole::Relay) => {
                "relay sync subscription serving requires at least local-tier registration"
            }
            (false, PeerRole::ClientLink { .. }) => {
                "sync subscription serving supports only global-tier registration"
            }
        };
        return Err(Error::new(ErrorCode::Query, message));
    }
    Ok(())
}

fn validate_shape_ast_for_registration<S>(
    node: &NodeState<S>,
    shape_id: ShapeId,
    ast: &ShapeAst,
) -> Result<Option<ValidatedQuery>, crate::node::Error>
where
    S: OrderedKvStorage,
{
    node.validate_shape_ast_for_registration(shape_id, ast)
}

fn unsupported_shape_capability_rejection_message(
    subscription: SubscriptionKey,
    detail: String,
) -> SyncMessage {
    subscription_rejection_message(
        subscription,
        SubscribeRejectReason::UnsupportedShapeCapability { detail },
    )
}

fn server_subscription_failure_rejection_message(
    subscription: SubscriptionKey,
    error: &crate::node::Error,
) -> SyncMessage {
    // Keep the complete error on the serving process only. Subscription keys
    // provide a correlation handle without disclosing schema, policy, or
    // storage details to the peer.
    eprintln!(
        "jazz subscription rejected: shape={} binding={} read_view={} server_error={error}",
        subscription.shape_id.0, subscription.binding_id.0, subscription.read_view.id,
    );
    subscription_rejection_message(
        subscription,
        SubscribeRejectReason::ServerFailure {
            code: server_failure_code(error),
        },
    )
}

fn subscription_rejection_message(
    subscription: SubscriptionKey,
    reason: SubscribeRejectReason,
) -> SyncMessage {
    SyncMessage::SubscribeRejected {
        subscription,
        reason,
    }
}

fn server_failure_code(error: &crate::node::Error) -> SubscribeServerFailureCode {
    match error {
        crate::node::Error::TableNotFound(_) => SubscribeServerFailureCode::TableNotFound,
        crate::node::Error::Query(error) if matches!(**error, QueryError::UnknownTable(_)) => {
            SubscribeServerFailureCode::TableNotFound
        }
        crate::node::Error::Query(_) => SubscribeServerFailureCode::QueryValidation,
        crate::node::Error::QueryLowering(_) => SubscribeServerFailureCode::QueryLowering,
        crate::node::Error::AuthorizationDenied => SubscribeServerFailureCode::PolicyEvaluation,
        crate::node::Error::InvalidStoredValue(_)
        | crate::node::Error::InvalidCatalogueUpdate(_) => {
            SubscribeServerFailureCode::SchemaResolution
        }
        _ => SubscribeServerFailureCode::Internal,
    }
}

fn is_server_shape_validation_failure(error: &crate::node::Error) -> bool {
    matches!(
        error,
        crate::node::Error::TableNotFound(_) | crate::node::Error::Query(_)
    )
}

fn register_shape_rejection_subscription(
    shape_id: ShapeId,
    read_view: ReadViewKey,
) -> SubscriptionKey {
    SubscriptionKey {
        shape_id,
        binding_id: BindingId(uuid::Uuid::nil()),
        read_view,
    }
}

fn coverage_key(
    shape: &ValidatedQuery,
    binding: &Binding,
    opts: RegisterShapeOptions,
) -> CoverageKey {
    CoverageKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        opts,
        policy_binding: None,
    }
}

fn request_coverage_key(
    shape: &ValidatedQuery,
    binding: &Binding,
    opts: RegisterShapeOptions,
    policy_binding: &Option<(AuthorSubject, BTreeMap<String, Value>)>,
) -> CoverageKey {
    let mut key = coverage_key(shape, binding, opts);
    key.policy_binding = policy_binding.as_ref().map(|(identity, claims)| {
        crate::protocol::PolicyBindingKey::from_canonical_parts(*identity, claims.clone())
    });
    key
}

fn subscriber_permissions_ready(permissions_ready: bool, trust: CommitUnitTrust) -> bool {
    trust.is_trusted() || permissions_ready
}

/// Messages whose semantics assert downstream authority state must never be
/// accepted from a subscriber transport. Keep this admission check ahead of
/// `NodeState::apply_sync_message`: validation inside the node cannot recover
/// the direction or authenticated link role after dispatch.
fn subscriber_inbound_message_is_authority_only(
    message: &SyncMessage,
    ingest: CommitUnitIngestContext,
    peer: &crate::peer::PeerState,
) -> bool {
    matches!(
        message,
        SyncMessage::FateUpdate { .. }
            | SyncMessage::SubscribeRejected { .. }
            | SyncMessage::CatalogueAck(_)
            | SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { .. })
            | SyncMessage::RowVersionPayloads { .. }
            | SyncMessage::CatalogueSnapshot(_)
            | SyncMessage::PermissionAdviceResponse { .. }
            | SyncMessage::AuthorizationScopeReceipt { .. }
            | SyncMessage::AuthorizationScopeView { .. }
            | SyncMessage::AuthorizationScopeAggregateReceipt { .. }
            | SyncMessage::AuthorizationScopeUnavailable { .. }
            | SyncMessage::AuthorizationScopeDecision { .. }
    ) || (matches!(message, SyncMessage::SessionClaims { .. })
        && (peer.rejects_raw_session_claims()
            // Host-admitted trusted links may maintain the compatibility
            // claims map (including backend user-attributed writes). This
            // legacy map authority is distinct from request delegation below:
            // only a backend ClientLink can assert an arbitrary query scope.
            || (!ingest.trust.is_trusted()
                && !delegated_session_capability(ingest, peer.role()))))
}

/// Only the host-admitted core-facing relay may carry another session's
/// immutable policy binding.  This is a connection capability, not a field a
/// wire caller can grant itself by choosing a registration option or message
/// variant.
fn delegated_session_capability(ingest: CommitUnitIngestContext, peer_role: PeerRole) -> bool {
    ingest.trust == CommitUnitTrust::Relay && peer_role == PeerRole::Relay
}

/// Select the immutable session snapshot permitted for one request. Direct
/// links use their host-admitted session. A trusted backend may assert a
/// request snapshot; a scope-isolated relay needs an exact server-issued binding. A generic
/// multiplexed relay has no per-binding capability yet, so it must forward
/// rather than select a user policy subject. Keeping Subscribe and repair on
/// this one admission rule prevents one path from accidentally treating a
/// relay's transport identity as a permission subject.
fn admitted_request_policy_binding(
    ingest: CommitUnitIngestContext,
    peer: &crate::peer::PeerState,
    direct: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    delegated: Option<crate::protocol::DelegatedSessionBinding>,
) -> Option<(AuthorSubject, BTreeMap<String, Value>)> {
    match delegated {
        // A relay transport is deliberately unbound. Its connection context
        // may contain an opaque host identity for lifecycle purposes, but it
        // is never a fallback permission subject for an application query or
        // repair.
        None if peer.role() == PeerRole::Relay => None,
        None => direct,
        Some(delegated) if delegated_session_capability(ingest, peer.role()) => {
            let binding = (delegated.identity, delegated.claims);
            peer.admits_relay_binding(&binding).then_some(binding)
        }
        Some(delegated)
            if ingest.trust == CommitUnitTrust::TrustedBackend
                && matches!(peer.role(), PeerRole::ClientLink { .. }) =>
        {
            Some((delegated.identity, delegated.claims))
        }
        Some(delegated)
            if peer.authority_query_delegate
                && ingest.trust == CommitUnitTrust::TrustedAuthority
                && ingest.identity == AuthorSubject::SYSTEM
                && matches!(peer.role(), PeerRole::ClientLink { .. }) =>
        {
            Some((delegated.identity, delegated.claims))
        }
        Some(_) => None,
    }
}

fn subscriber_permission_subject(ingest: CommitUnitIngestContext) -> Option<AuthorSubject> {
    match ingest.trust {
        CommitUnitTrust::Session => Some(ingest.identity),
        CommitUnitTrust::Relay => None,
        CommitUnitTrust::TrustedBackend
        | CommitUnitTrust::TrustedAuthority
        | CommitUnitTrust::TrustedAdmin => Some(AuthorSubject::SYSTEM),
    }
}

/// Row cells supplied to write methods.
pub type RowCells = BTreeMap<String, Value>;

/// Identity used to author and authorize a standalone write.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum WriteIdentity {
    /// Use the identity that opened the database.
    #[default]
    Database,
    /// Author and authorize the write as this trusted session identity.
    Session(AuthorSubject),
    /// Attribute provenance while retaining the database identity as policy subject.
    /// Only a Db opened through the trusted-backend capability may attribute
    /// to a different author; its database identity remains the policy subject.
    Attribution(AuthorSubject),
}

/// Resolved authorship and authorization roles for one write operation.
///
/// `made_by` is durable provenance. `permission_subject` is the identity
/// explicitly carried for policy evaluation; `None` retains the ordinary
/// database/link identity path.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ResolvedWriteIdentity {
    pub(super) made_by: AuthorSubject,
    pub(super) permission_subject: Option<AuthorSubject>,
}

/// Exact branch selected by an insert or restore.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum ExactWriteTarget {
    /// Write to the database's current root branch.
    #[default]
    Root,
    /// Write to one exact branch key.
    Branch(BranchSelector),
}

impl ExactWriteTarget {
    fn branch(&self) -> BranchSelector {
        match self {
            Self::Root => BranchSelector::default(),
            Self::Branch(branch) => branch.clone(),
        }
    }
}

/// Root or head-over-base view selected by an update, upsert, or delete.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum WriteTarget {
    /// Write to the database's current root branch.
    #[default]
    Root,
    /// Write through a branch view, materializing inherited state in `head`.
    BranchView {
        /// Exact branch receiving the local write.
        head: BranchSelector,
        /// Optional inherited base view visible below `head`.
        base: Option<BranchViewBase>,
    },
}

/// Options for [`Db::insert`] and mergeable transaction inserts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InsertOptions {
    /// Caller-supplied row id, or a generated UUIDv7 row id when omitted.
    pub row_id: Option<RowUuid>,
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Exact branch receiving the row.
    pub target: ExactWriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

/// Options for [`Db::update`] and mergeable transaction updates.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpdateOptions {
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Root or branch view through which the patch is applied.
    pub target: WriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

/// Options for [`Db::upsert`] and mergeable transaction upserts.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct UpsertOptions {
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Root or branch view through which the upsert is applied.
    pub target: WriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

/// Options for [`Db::delete`] and mergeable transaction deletes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DeleteOptions {
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Root or branch view through which the row is deleted.
    pub target: WriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

/// Options for [`Db::restore`] and mergeable transaction restores.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RestoreOptions {
    /// Standalone-write identity. Transactions use the identity chosen when opened.
    pub identity: WriteIdentity,
    /// Exact branch whose deletion register is restored.
    pub target: ExactWriteTarget,
    /// Explicit provenance timestamp, or the database clock when omitted.
    pub updated_at_ms: Option<u64>,
}

fn ensure_transaction_identity(identity: WriteIdentity) -> Result<(), Error> {
    if identity == WriteIdentity::Database {
        return Ok(());
    }
    Err(Error::new(
        ErrorCode::Schema,
        "transaction identity is selected when the transaction is opened",
    ))
}

fn ensure_exclusive_target(target: &ExactWriteTarget) -> Result<(), Error> {
    if *target != ExactWriteTarget::Root {
        return Err(Error::new(
            ErrorCode::Schema,
            "exclusive transactions do not support branch writes",
        ));
    }
    Ok(())
}

fn ensure_exclusive_view_target(target: &WriteTarget) -> Result<(), Error> {
    if *target != WriteTarget::Root {
        return Err(Error::new(
            ErrorCode::Schema,
            "exclusive transactions do not support branch writes",
        ));
    }
    Ok(())
}

/// Build [`RowCells`] with bare identifier column names.
///
/// Keys are converted to column names with `stringify!`, and values are
/// converted with `Into<Value>`. Column and type validation remains lazy at
/// write/query validation time.
///
/// ```rust
/// # use jazz::db::doctest_support::{block_on, open_todos_db};
/// # use jazz::tx::DurabilityTier;
/// let db = block_on(open_todos_db())?;
/// let write = block_on(db.insert(
///     "todos",
///     jazz::row! {
///         title: "Ship it",
///         done: false,
///     },
///     Default::default(),
/// ))?;
/// block_on(write.wait(DurabilityTier::Local))?;
///
/// let todos = db.prepare_query(&db.table("todos"))?;
/// assert_eq!(db.read(&todos)?.len(), 1);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[macro_export]
macro_rules! row {
    () => {
        $crate::db::RowCells::new()
    };
    ($($key:ident : $value:expr),+ $(,)?) => {{
        let mut cells = $crate::db::RowCells::new();
        $(
            cells.insert(::std::string::String::from(stringify!($key)), ($value).into());
        )+
        cells
    }};
}

/// CRUD operations for an open mergeable transaction.
///
/// [`MergeableTx`] and [`MergeableTxRef`] implement this trait, so mergeable
/// CRUD has one definition regardless of who owns the transaction lifetime.
/// Import this trait to call its methods.
pub trait MergeableTxOps<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// The database that owns the open transaction.
    fn db(&self) -> &Db<S>;

    /// The id of the already-open transaction.
    fn tx_id(&self) -> OpenTransactionId;

    /// Stage one insert. Transaction identity is fixed when the transaction opens.
    async fn insert(
        &self,
        table: &str,
        cells: RowCells,
        options: InsertOptions,
    ) -> Result<RowUuid, Error> {
        ensure_transaction_identity(options.identity)?;
        let known_fresh_row = options.row_id.is_none() && self.db().row_id_source_guarantees_fresh;
        let row = options
            .row_id
            .unwrap_or_else(|| self.db().row_id_source.borrow_mut().next_row_id());
        match options.target {
            ExactWriteTarget::Root => {
                self.db()
                    .stage_mergeable_insert(
                        self.tx_id(),
                        table,
                        row,
                        cells,
                        options.updated_at_ms,
                        known_fresh_row,
                    )
                    .await?;
            }
            ExactWriteTarget::Branch(branch) => {
                self.db()
                    .stage_mergeable_insert_in_branch(
                        self.tx_id(),
                        table,
                        branch,
                        row,
                        cells,
                        options.updated_at_ms,
                        known_fresh_row,
                    )
                    .await?;
            }
        }
        Ok(row)
    }

    /// Stage one update; omitted fields keep the transaction-local value.
    async fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        options: UpdateOptions,
    ) -> Result<(), Error> {
        ensure_transaction_identity(options.identity)?;
        match options.target {
            WriteTarget::Root => {
                self.db()
                    .stage_mergeable_update(self.tx_id(), table, row, patch, options.updated_at_ms)
                    .await
            }
            WriteTarget::BranchView { head, base } => {
                self.db()
                    .stage_mergeable_update_in_branch_view(
                        self.tx_id(),
                        table,
                        head,
                        base,
                        row,
                        patch,
                        options.updated_at_ms,
                    )
                    .await
            }
        }
    }

    /// Stage one upsert.
    async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        options: UpsertOptions,
    ) -> Result<(), Error> {
        ensure_transaction_identity(options.identity)?;
        match options.target {
            WriteTarget::Root => {
                let exists = self
                    .db()
                    .mergeable_transaction_upsert_exists(self.tx_id(), table, row)
                    .await?;
                if exists {
                    self.db()
                        .stage_mergeable_update(
                            self.tx_id(),
                            table,
                            row,
                            cells,
                            options.updated_at_ms,
                        )
                        .await
                } else {
                    self.db()
                        .stage_mergeable_insert(
                            self.tx_id(),
                            table,
                            row,
                            cells,
                            options.updated_at_ms,
                            false,
                        )
                        .await
                }
            }
            WriteTarget::BranchView { head, base } => {
                self.db()
                    .stage_mergeable_upsert_in_branch_view(
                        self.tx_id(),
                        table,
                        head,
                        base,
                        row,
                        cells,
                        options.updated_at_ms,
                    )
                    .await
            }
        }
    }

    /// Stage one soft delete.
    async fn delete(&self, table: &str, row: RowUuid, options: DeleteOptions) -> Result<(), Error> {
        ensure_transaction_identity(options.identity)?;
        match options.target {
            WriteTarget::Root => {
                self.db()
                    .stage_mergeable_delete(self.tx_id(), table, row, options.updated_at_ms)
                    .await
            }
            WriteTarget::BranchView { head, base } => {
                self.db()
                    .stage_mergeable_delete_in_branch_view(
                        self.tx_id(),
                        table,
                        head,
                        base,
                        row,
                        options.updated_at_ms,
                    )
                    .await
            }
        }
    }

    /// Stage one restore, optionally replacing row content.
    async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        cells: Option<RowCells>,
        options: RestoreOptions,
    ) -> Result<(), Error> {
        ensure_transaction_identity(options.identity)?;
        let cells = cells.ok_or_else(|| {
            Error::new(
                ErrorCode::Schema,
                "transaction restores currently require replacement cells",
            )
        })?;
        match options.target {
            ExactWriteTarget::Root => {
                self.db()
                    .stage_mergeable_restore(self.tx_id(), table, row, cells, options.updated_at_ms)
                    .await
            }
            ExactWriteTarget::Branch(branch) => {
                self.db()
                    .stage_mergeable_restore_in_branch(
                        self.tx_id(),
                        table,
                        branch,
                        row,
                        cells,
                        options.updated_at_ms,
                    )
                    .await
            }
        }
    }

    /// Stage an atomic move of one object branch-local row between exact branch
    /// keys. The destination receives an explicit content write and restore,
    /// while the source receives an explicit deletion in the same transaction.
    async fn move_between_branches(
        &self,
        table: &str,
        source: BranchSelector,
        target: BranchSelector,
        row: RowUuid,
    ) -> Result<(), Error> {
        if source == target {
            return Err(Error::new(
                ErrorCode::Schema,
                "branch move requires distinct source and target selectors",
            ));
        }
        let mut cells = self
            .db()
            .node
            .node
            .lock()
            .await
            .visible_current_cells_in_branch(table, &source, row)
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::NotObserved,
                    format!("source branch-local row is not visible: {}", row.0),
                )
            })?;
        let table_schema = self
            .db()
            .schema
            .tables
            .iter()
            .find(|candidate| candidate.name == table)
            .ok_or_else(|| Error::new(ErrorCode::Schema, format!("unknown table {table}")))?;
        let (_, target_cells) = self
            .db()
            .schema
            .project_branch_selector(table_schema, &target)
            .map_err(|message| Error::new(ErrorCode::Schema, message))?;
        cells.extend(target_cells);
        self.restore(
            table,
            row,
            Some(cells),
            RestoreOptions {
                target: ExactWriteTarget::Branch(target),
                ..Default::default()
            },
        )
        .await?;
        self.delete(
            table,
            row,
            DeleteOptions {
                target: WriteTarget::BranchView {
                    head: source,
                    base: None,
                },
                ..Default::default()
            },
        )
        .await
    }

    /// Read one row with this transaction's pending writes overlaid.
    async fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.db().transaction_read(self.tx_id(), table, row).await
    }

    /// Read a prepared query with this transaction's pending writes overlaid.
    async fn all_prepared(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_with_opts(prepared, ReadOpts::default())
            .await
    }

    /// Read a prepared query with transaction-local writes and explicit read semantics.
    async fn all_prepared_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .transaction_all(self.tx_id(), prepared, opts)
            .await
    }

    /// Read a prepared query inside this transaction as `author`.
    async fn all_prepared_for_identity(
        &self,
        prepared: &PreparedQuery,
        author: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_for_identity_with_opts(prepared, author, ReadOpts::default())
            .await
    }

    /// Read a prepared query as `author` with explicit read semantics.
    async fn all_prepared_for_identity_with_opts(
        &self,
        prepared: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .transaction_all_for_identity(self.tx_id(), prepared, author, opts)
            .await
    }

    /// Read a relation snapshot with this transaction's pending writes overlaid.
    async fn relation_snapshot_prepared_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.db()
            .transaction_relation_snapshot(self.tx_id(), prepared, opts)
            .await
    }

    /// Read a relation snapshot as `author` with this transaction's pending writes overlaid.
    async fn relation_snapshot_prepared_for_identity_with_opts(
        &self,
        prepared: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.db()
            .transaction_relation_snapshot_for_identity(self.tx_id(), prepared, author, opts)
            .await
    }
}

/// Owning, Rust-facing handle for a group of mergeable writes.
///
/// This handle owns the transaction lifetime and abandons an uncommitted
/// transaction on drop. Use [`MergeableTxRef`] when a caller retains an
/// [`OpenTransactionId`] between calls and must not close the transaction on return.
pub struct MergeableTx<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenTransactionId,
    /// Set once the transaction has been committed, so `Drop` does not submit
    /// redundant abandonment maintenance for an already-terminal id.
    ///
    /// Maintenance is idempotent, but successful commit owns the terminal
    /// transition and should not enqueue cleanup behind unrelated node work.
    committed: bool,
}

impl<S> MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Commit all staged writes as one mergeable transaction.
    ///
    /// Once the commit succeeds, dropping this handle does not abandon the
    /// already-committed transaction. If it fails, dropping the handle attempts
    /// to abandon any transaction that remains open.
    pub async fn commit(mut self) -> Result<TxId, Error> {
        let result = self.db.commit_mergeable_handle(self.tx_id).await;
        if result.is_ok() {
            self.committed = true;
        }
        result
    }
}

impl<S> MergeableTxOps<S> for MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenTransactionId {
        self.tx_id
    }
}

/// Non-owning operations handle for an already-open mergeable transaction.
///
/// Construct this with [`Db::mergeable_tx_ref`] when another layer owns the
/// [`OpenTransactionId`] lifetime. Dropping this ref never abandons the transaction.
pub struct MergeableTxRef<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenTransactionId,
}

impl<S> MergeableTxOps<S> for MergeableTxRef<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenTransactionId {
        self.tx_id
    }
}

impl<S> Drop for MergeableTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.db.node.abandon_or_enqueue_transaction(self.tx_id);
    }
}

/// CRUD and read operations for an open exclusive transaction.
///
/// [`ExclusiveTx`] and [`ExclusiveTxRef`] implement this trait, so exclusive
/// operations have one definition regardless of who owns the transaction
/// lifetime. Import this trait to call its methods.
pub trait ExclusiveTxOps<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// The database that owns the open transaction.
    fn db(&self) -> &Db<S>;

    /// The id of the already-open transaction.
    fn tx_id(&self) -> OpenTransactionId;

    /// Read one row inside the exclusive transaction.
    async fn read(&self, table: &str, row: RowUuid) -> Result<Option<RowCells>, Error> {
        self.db().transaction_read(self.tx_id(), table, row).await
    }

    /// Read all current rows in a table inside the exclusive transaction.
    async fn all(&self, table: &str) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .transaction_current_rows(self.tx_id(), table)
            .await
    }

    /// Read a prepared query inside the exclusive transaction.
    async fn all_prepared(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_with_opts(prepared, ReadOpts::default())
            .await
    }

    /// Read a prepared query with transaction-local writes and explicit read semantics.
    async fn all_prepared_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .transaction_all(self.tx_id(), prepared, opts)
            .await
    }

    /// Read a prepared query inside the exclusive transaction as `author`.
    async fn all_prepared_for_identity(
        &self,
        prepared: &PreparedQuery,
        author: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_prepared_for_identity_with_opts(prepared, author, ReadOpts::default())
            .await
    }

    /// Read a prepared query as `author` with explicit read semantics.
    async fn all_prepared_for_identity_with_opts(
        &self,
        prepared: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.db()
            .transaction_all_for_identity(self.tx_id(), prepared, author, opts)
            .await
    }

    /// Read a relation snapshot with this transaction's pending writes overlaid.
    async fn relation_snapshot_prepared_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.db()
            .transaction_relation_snapshot(self.tx_id(), prepared, opts)
            .await
    }

    /// Read a relation snapshot as `author` with this transaction's pending writes overlaid.
    async fn relation_snapshot_prepared_for_identity_with_opts(
        &self,
        prepared: &PreparedQuery,
        author: AuthorSubject,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        self.db()
            .transaction_relation_snapshot_for_identity(self.tx_id(), prepared, author, opts)
            .await
    }

    /// Stage one insert.
    async fn insert(
        &self,
        table: &str,
        cells: RowCells,
        options: InsertOptions,
    ) -> Result<RowUuid, Error> {
        ensure_transaction_identity(options.identity)?;
        ensure_exclusive_target(&options.target)?;
        let row = options
            .row_id
            .unwrap_or_else(|| self.db().row_id_source.borrow_mut().next_row_id());
        self.db()
            .stage_exclusive_insert(self.tx_id(), table, row, cells, options.updated_at_ms)
            .await?;
        Ok(row)
    }

    /// Stage an update; omitted fields keep the transaction-local value.
    async fn update(
        &self,
        table: &str,
        row: RowUuid,
        patch: RowCells,
        options: UpdateOptions,
    ) -> Result<(), Error> {
        ensure_transaction_identity(options.identity)?;
        ensure_exclusive_view_target(&options.target)?;
        self.db()
            .stage_exclusive_update(self.tx_id(), table, row, patch, options.updated_at_ms)
            .await
    }

    /// Stage one upsert.
    async fn upsert(
        &self,
        table: &str,
        row: RowUuid,
        cells: RowCells,
        options: UpsertOptions,
    ) -> Result<(), Error> {
        ensure_transaction_identity(options.identity)?;
        ensure_exclusive_view_target(&options.target)?;
        self.db()
            .stage_exclusive_upsert(self.tx_id(), table, row, cells, options.updated_at_ms)
            .await
    }

    /// Stage a soft delete.
    async fn delete(&self, table: &str, row: RowUuid, options: DeleteOptions) -> Result<(), Error> {
        ensure_transaction_identity(options.identity)?;
        ensure_exclusive_view_target(&options.target)?;
        self.db()
            .stage_exclusive_delete(self.tx_id(), table, row, options.updated_at_ms)
            .await
    }

    /// Stage a restore, applying defaults for omitted columns.
    async fn restore(
        &self,
        table: &str,
        row: RowUuid,
        cells: Option<RowCells>,
        options: RestoreOptions,
    ) -> Result<(), Error> {
        ensure_transaction_identity(options.identity)?;
        ensure_exclusive_target(&options.target)?;
        let cells = cells.ok_or_else(|| {
            Error::new(
                ErrorCode::Schema,
                "exclusive transaction restores require replacement cells",
            )
        })?;
        self.db()
            .stage_exclusive_restore(self.tx_id(), table, row, cells, options.updated_at_ms)
            .await
    }
}

/// Owning, Rust-facing handle for an exclusive transaction over a stable snapshot.
///
/// This handle owns the transaction lifetime and abandons an uncommitted
/// transaction on drop. Use [`ExclusiveTxRef`] when a caller retains an
/// [`OpenTransactionId`] between calls and must not close the transaction on return.
pub struct ExclusiveTx<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenTransactionId,
    committed: bool,
}

impl<S> ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Commit the exclusive transaction.
    ///
    /// Once the commit succeeds, dropping this handle does not abandon the
    /// already-committed transaction. If it fails, dropping the handle attempts
    /// to abandon any transaction that remains open.
    pub async fn commit(mut self) -> Result<TxId, Error> {
        let result = self.db.commit_exclusive_handle(self.tx_id).await;
        if result.is_ok() {
            self.committed = true;
        }
        result
    }
}

impl<S> ExclusiveTxOps<S> for ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenTransactionId {
        self.tx_id
    }
}

impl<S> Drop for ExclusiveTx<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        self.db.node.abandon_or_enqueue_transaction(self.tx_id);
    }
}

/// Non-owning operations handle for an already-open exclusive transaction.
///
/// Construct this with [`Db::exclusive_tx_ref`] when another layer owns the
/// [`OpenTransactionId`] lifetime. Dropping this ref never abandons the transaction.
pub struct ExclusiveTxRef<'a, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    db: &'a Db<S>,
    tx_id: OpenTransactionId,
}

impl<S> ExclusiveTxOps<S> for ExclusiveTxRef<'_, S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    fn db(&self) -> &Db<S> {
        self.db
    }

    fn tx_id(&self) -> OpenTransactionId {
        self.tx_id
    }
}

/// Handle for an applied local write.
pub struct WriteHandle<S>
where
    S: OrderedKvStorage,
{
    node: WeakNodeState<S>,
    row_uuid: RowUuid,
    tx_id: TxId,
    local_tier: DurabilityTier,
    queued_status: Option<Rc<RefCell<QueuedMutationStatus>>>,
    queued_alias: Option<QueuedMutationAlias>,
}

impl<S> WriteHandle<S>
where
    S: OrderedKvStorage,
{
    /// Generated or caller-supplied row id affected by this write.
    pub fn row_uuid(&self) -> RowUuid {
        self.row_uuid
    }

    /// Mergeable transaction id backing this write.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let write = block_on(db.insert(
    ///     "todos",
    ///     todo_cells("has id", false),
    ///     Default::default(),
    /// ))?;
    ///
    /// let _row_id = write.row_uuid();
    /// let _tx_id = write.mergeable_tx_id();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn mergeable_tx_id(&self) -> TxId {
        self.tx_id
    }

    /// Wait until this write has reached the requested tier.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let write = block_on(db.insert(
    ///     "todos",
    ///     todo_cells("wait locally", false),
    ///     Default::default(),
    /// ))?;
    ///
    /// let tx_id = block_on(write.wait(DurabilityTier::Local))?;
    /// assert_eq!(tx_id, write.mergeable_tx_id());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn wait(&self, tier: DurabilityTier) -> Result<TxId, Error> {
        let reservation_is_pending = self
            .queued_status
            .as_ref()
            .is_some_and(|status| matches!(*status.borrow(), QueuedMutationStatus::Pending));
        if tier <= self.local_tier && !reservation_is_pending {
            return Ok(self.tx_id);
        }
        let state = self.write_state().await?;
        match state.fate {
            Fate::Rejected(reason) => Err(write_rejected(self.tx_id, reason)),
            Fate::Pending if tier >= DurabilityTier::Edge => Err(Error::new(
                ErrorCode::NotObserved,
                format!("write has not been accepted at requested tier {tier:?}"),
            )),
            Fate::Pending | Fate::Accepted
                if !transaction_satisfies_wait(
                    &state.fate,
                    state.global_time,
                    state.durability,
                    tier,
                ) =>
            {
                Err(Error::new(
                    ErrorCode::NotObserved,
                    format!("write has not reached requested tier {tier:?}"),
                ))
            }
            Fate::Pending | Fate::Accepted => Ok(self.tx_id),
        }
    }

    /// Return the locally observed fate and durability for this write.
    pub async fn write_state(&self) -> Result<WriteState, Error> {
        if let Some(status) = &self.queued_status {
            match &*status.borrow() {
                QueuedMutationStatus::Pending => {
                    return Ok(WriteState {
                        fate: Fate::Pending,
                        global_time: None,
                        durability: DurabilityTier::None,
                    });
                }
                QueuedMutationStatus::Failed(error) => return Err(error.clone()),
                QueuedMutationStatus::Published => {}
            }
        }
        let Some(node) = self.node.upgrade() else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                "database handle was dropped",
            ));
        };
        let resolved_tx_id = self
            .queued_alias
            .as_ref()
            .and_then(|alias| *alias.borrow())
            .unwrap_or(self.tx_id);
        let Some((fate, global_time, durability)) =
            node.lock().await.transaction_state(resolved_tx_id).await
        else {
            return Err(Error::new(
                ErrorCode::NotObserved,
                format!("transaction {:?} is not known locally", self.tx_id),
            ));
        };
        Ok(WriteState {
            fate,
            global_time,
            durability,
        })
    }
}

fn write_rejected(transaction_id: impl std::fmt::Debug, reason: RejectionReason) -> Error {
    Error::new(
        ErrorCode::WriteRejected,
        format!("transaction {transaction_id:?} was rejected: {reason:?}"),
    )
}

/// Decode Groove's recursively assembled terminal value into Jazz's typed
/// structured-result view. This separates relation fields from scalar row
/// fields, but does not join or assemble relation facts.
fn materialize_result_tree(query: &Query, snapshot: RelationSnapshot) -> Result<ResultTree, Error> {
    fn node_from_record(
        table: &str,
        record: OwnedRecord,
        arrays: &[crate::query::ArraySubquery],
    ) -> Result<ResultNode, Error> {
        let descriptor = *record.descriptor();
        let values = record.to_values().map_err(|error| {
            Error::new(
                ErrorCode::Protocol,
                format!("invalid Groove terminal record: {error}"),
            )
        })?;
        let array_by_name = arrays
            .iter()
            .map(|array| (array.column_name.as_str(), array))
            .collect::<BTreeMap<_, _>>();
        let mut scalar_fields = Vec::new();
        let mut scalar_values = Vec::new();
        let mut relations = BTreeMap::new();
        for (field, value) in descriptor.fields().iter().zip(values) {
            let Some(name) = field.name.as_deref() else {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "Groove terminal emitted an unnamed field",
                ));
            };
            let Some(array) = array_by_name.get(name) else {
                scalar_fields.push((name.to_owned(), field.value_type.clone()));
                scalar_values.push(value);
                continue;
            };
            let Value::Array(children) = value else {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    format!("Groove terminal relation {name} was not an array"),
                ));
            };
            let children = children
                .into_iter()
                .map(|value| {
                    let Value::Record(record) = value else {
                        return Err(Error::new(
                            ErrorCode::Protocol,
                            format!("Groove terminal relation {name} contained a non-record"),
                        ));
                    };
                    node_from_record(&array.table, record, &array.nested_arrays)
                })
                .collect::<Result<Vec<_>, _>>()?;
            relations.insert(name.to_owned(), ResultRelation::Array(children));
        }
        let scalar_descriptor = RecordDescriptor::new(scalar_fields);
        let raw = scalar_descriptor.create(&scalar_values).map_err(|error| {
            Error::new(
                ErrorCode::Protocol,
                format!("invalid scalar projection from Groove terminal: {error}"),
            )
        })?;
        let row = CurrentRow::new(table, OwnedRecord::new(raw, scalar_descriptor));
        let occurrence = OutputOccurrenceId::single_source(ObjectId::from_uuid(row.row_uuid().0));
        Ok(ResultNode {
            occurrence,
            row,
            relations,
        })
    }

    if !snapshot.edges.is_empty() {
        return Err(Error::new(
            ErrorCode::Protocol,
            "structured result unexpectedly contained relation-fact edges",
        ));
    }
    let roots = snapshot
        .rows
        .into_iter()
        .take(snapshot.root_count)
        .map(|row| {
            let (descriptor, raw) = row.encoded_record();
            node_from_record(
                row.table(),
                OwnedRecord::new(raw.to_vec(), *descriptor),
                &query.array_subqueries,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(ResultTree { roots })
}

struct ScalarProbe {
    deadline: web_time::Instant,
    rows: Vec<RowUuid>,
    future: Pin<Box<dyn Future<Output = row_availability::CurrentRowsResult>>>,
}

#[derive(Default)]
struct ScalarReconciliation {
    generation: Option<(u64, SubscriptionKey, u64)>,
    pending: VecDeque<RowUuid>,
    active: Option<ScalarProbe>,
    retry_at: Option<web_time::Instant>,
    retry_delay_ms: u64,
}

struct SubscriptionState {
    /// Set synchronously by stream finalization, before its async cleanup is
    /// drained. Refresh observes this independently owned cell before it can
    /// install a replacement maintained subscription.
    closed: Rc<Cell<bool>>,
    terminal_rows: bool,
    scalar_reconciliation_enabled: bool,
    scalar_authority_revision: u64,
    scalar_reconciliation: ScalarReconciliation,
    kind: SubscriptionKind,
    groove_runtime_token: u64,
    /// The maintained subscription currently owned by this public stream.
    /// Rehydration replaces the Groove ID, and drop must clean up that new ID.
    local_subscription_cleanup: Rc<Cell<Option<(u64, groove::ivm::SubscriptionId)>>>,
    /// Coverage ownership belongs to the stream state rather than its initial
    /// closure, so finalization always retires the currently live state.
    upstream_subscription_handles: Vec<UpstreamCoverageHandle>,
    propagates_upstream: bool,
    request_identity_claims: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    author: AuthorSubject,
    authorization_mode: QueryAuthorizationMode,
    read_tier: DurabilityTier,
    /// Online remote-if-possible overlays pending changes on scoped inputs.
    pending_overlay: bool,
    remote_read_tier: Option<DurabilityTier>,
    /// Once this stream has an upstream, cached durable state needs a receipt
    /// from each replacement connection before it can be settled again.
    requires_authority_receipt: bool,
    /// Routing intent sent with this subscription's remote registration.
    remote_propagate_upstream: bool,
    read_view: ReadViewSpec,
    snapshot: RelationSnapshot,
    snapshot_index: RelationSnapshotIndex,
    snapshot_source: SubscriptionSnapshotSource,
    settled: bool,
    /// A replacement graph opened cold. Keep the last complete facade only
    /// until the replacement has its first local terminal batch, then publish
    /// one complete reset from that retained baseline.
    cold_runtime_replacement: bool,
    sender: SubscriptionSender,
}

/// One application publication boundary shared by every refresh path. Retain
/// a baseline only while withholding changes, not a second live result tree.
#[derive(Clone)]
struct SubscriptionSender {
    sender: UnboundedSender<SubscriptionEvent>,
    publication: Rc<RefCell<SubscriptionPublication>>,
    requested_tier: DurabilityTier,
}

#[derive(Default)]
struct SubscriptionPublication {
    opened: bool,
    deferred: Option<SubscriptionPublicationSnapshot>,
    reset: bool,
    unresolved: BTreeSet<OutputOccurrenceId>,
}

struct SubscriptionPublicationSnapshot {
    snapshot: RelationSnapshot,
    occurrences: Vec<OutputOccurrenceId>,
}

impl SubscriptionPublicationSnapshot {
    fn capture(snapshot: &RelationSnapshot, index: &RelationSnapshotIndex) -> Result<Self, Error> {
        Ok(Self {
            snapshot: materialized_subscription_snapshot(snapshot, index)?,
            occurrences: snapshot_root_occurrences(snapshot, index)?,
        })
    }
}

impl SubscriptionSender {
    /// Validate only changed roots. Descendant edits do not change a root's
    /// required scalar cells; unchanged roots must not be rescanned here.
    fn materialized<S: OrderedKvStorage>(
        &self,
        node: &NodeState<S>,
        query: &Query,
        event: &SubscriptionEvent,
    ) -> Result<bool, Error> {
        let mut publication = self.publication.borrow_mut();
        if let SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            ..
        } = event
        {
            if *reset {
                publication.unresolved.clear();
            }
            for row in removed {
                publication.unresolved.remove(&row.occurrence_id);
            }
            for row in added.iter().chain(updated) {
                let changed = RelationSnapshot {
                    root_count: 1,
                    rows: vec![row.row.clone()],
                    edges: Vec::new(),
                };
                if node.relation_snapshot_has_materialized_required_cells(query, &changed)? {
                    publication.unresolved.remove(&row.occurrence_id);
                } else {
                    publication.unresolved.insert(row.occurrence_id.clone());
                }
            }
        }
        Ok(publication.unresolved.is_empty())
    }

    fn checkpoint(
        &self,
        tier: DurabilityTier,
        settled: bool,
        snapshot: &RelationSnapshot,
        index: &RelationSnapshotIndex,
    ) -> Result<Option<SubscriptionPublicationSnapshot>, Error> {
        let publication = self.publication.borrow();
        if tier >= DurabilityTier::Edge
            && !settled
            && publication.opened
            && publication.deferred.is_none()
        {
            SubscriptionPublicationSnapshot::capture(snapshot, index).map(Some)
        } else {
            Ok(None)
        }
    }

    fn publish(
        &self,
        mut event: SubscriptionEvent,
        before: Option<SubscriptionPublicationSnapshot>,
        snapshot: &RelationSnapshot,
        index: &RelationSnapshotIndex,
        materialized: bool,
    ) -> Result<bool, Error> {
        let SubscriptionEvent::Delta {
            reset,
            publishable,
            settled,
            tier,
            ..
        } = &event
        else {
            return Ok(self.unbounded_send(event).is_ok());
        };
        let (reset, publishable, settled, tier) = (*reset, *publishable, *settled, *tier);
        if !publishable
            && matches!(&event, SubscriptionEvent::Delta {
            reset: false, added, updated, removed, terminal_operations, ..
        } if added.is_empty() && updated.is_empty() && removed.is_empty() && terminal_operations.is_empty())
        {
            return Ok(false);
        }
        if settled && !materialized {
            return Err(Error::new(
                ErrorCode::Protocol,
                "settled subscription retained unresolved required cells",
            ));
        }
        let mut publication = self.publication.borrow_mut();
        if !publishable
            || !materialized
            || (self.requested_tier >= DurabilityTier::Edge && !settled)
        {
            if publication.opened
                && publication.deferred.is_none()
                && self.requested_tier >= DurabilityTier::Edge
            {
                publication.deferred = Some(before.ok_or_else(|| {
                    Error::new(
                        ErrorCode::Protocol,
                        "withheld subscription change has no publication baseline",
                    )
                })?);
            }
            // Local coverage can be unsettled during perfectly ordinary
            // immediate delivery. Do not checkpoint those updates. If local
            // required cells are actually missing, resume with a canonical
            // reset when the maintained result becomes materialized again.
            publication.reset |= reset || self.requested_tier < DurabilityTier::Edge;
            return Ok(false);
        }
        if !publication.opened || publication.reset || (reset && publication.deferred.is_some()) {
            let current = SubscriptionPublicationSnapshot::capture(snapshot, index)?;
            event = SubscriptionEvent::Delta {
                reset: true,
                publishable: true,
                added: subscription_outputs_with_occurrence_sidecar(
                    &current.snapshot,
                    &current.occurrences,
                )?,
                updated: Vec::new(),
                removed: Vec::new(),
                terminal_operations: Vec::new(),
                settled,
                tier,
            };
        } else if let Some(previous) = &publication.deferred {
            let current = SubscriptionPublicationSnapshot::capture(snapshot, index)?;
            event = subscription_terminal_delta_event(
                tier,
                settled,
                &previous.snapshot,
                &previous.occurrences,
                &current.snapshot,
                &current.occurrences,
            )?;
        }
        publication.opened = true;
        publication.deferred = None;
        publication.reset = false;
        Ok(self.sender.unbounded_send(event).is_ok())
    }

    fn unbounded_send(
        &self,
        event: SubscriptionEvent,
    ) -> Result<(), futures_channel::mpsc::TrySendError<SubscriptionEvent>> {
        if matches!(&event, SubscriptionEvent::Closed)
            || matches!(&event, SubscriptionEvent::Rejected { reason }
                if !matches!(reason, SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission))
        {
            self.publication.borrow_mut().deferred = None;
        }
        self.sender.unbounded_send(event)
    }
}

#[derive(Clone, Default)]
struct RelationSnapshotIndex {
    roots: BTreeMap<OutputOccurrenceId, usize>,
    related: BTreeMap<(String, RowUuid), usize>,
    edges: BTreeSet<RelationEdge>,
    /// Decoded descendants supersede the encoded root seed until a complete
    /// snapshot is requested. Ordinary delta delivery must not re-encode it.
    terminal_records: BTreeMap<OutputOccurrenceId, terminal_record::TerminalRecordState>,
}

impl RelationSnapshotIndex {
    fn from_snapshot(snapshot: &RelationSnapshot) -> Self {
        let mut index = Self::default();
        for (position, row) in snapshot.rows.iter().take(snapshot.root_count).enumerate() {
            index
                .roots
                .insert(subscription_row_occurrence_id(row), position);
        }
        for (offset, row) in snapshot.rows.iter().skip(snapshot.root_count).enumerate() {
            index.related.insert(
                (row.table().to_owned(), row.row_uuid()),
                snapshot.root_count + offset,
            );
        }
        index.edges = snapshot.edges.iter().cloned().collect();
        index
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SubscriptionSnapshotSource {
    LocalMaintained,
    LinkSnapshot,
}

enum SubscriptionKind {
    Prepared {
        shape: ValidatedQuery,
        binding: Binding,
        maintained_subscription: Option<LocalMaintainedViewSubscription>,
    },
}

/// Row identity removed from a materialized subscription result.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct RemovedRow {
    /// Logical table that contained the removed row.
    pub table: String,
    /// Stable row identity.
    pub row_uuid: RowUuid,
    /// Stable identity of the removed output occurrence.
    pub occurrence_id: OutputOccurrenceId,
    /// Position occupied by this occurrence before the frame was applied.
    pub index: usize,
}

impl RemovedRow {
    #[doc(hidden)]
    pub fn from_result_key(table: String, row_uuid: RowUuid, key: ResultKey) -> Self {
        Self {
            table,
            row_uuid,
            occurrence_id: key.as_occurrence().clone(),
            index: 0,
        }
    }
}

/// One row addressed by its maintained output occurrence identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionOutputRow {
    /// Stable output occurrence identity.
    pub occurrence_id: OutputOccurrenceId,
    /// Materialized row body for this output occurrence.
    pub row: CurrentRow,
    /// Position occupied by this occurrence before the frame, when it existed.
    pub previous_index: Option<usize>,
    /// Authoritative position occupied by this occurrence after the frame.
    pub index: usize,
}

/// Immutable producer-owned decoding contract for a structured terminal root.
///
/// The maintained query compiler creates this alongside its app-row terminal.
/// Consumers install it before applying operations which name `id`; the
/// descriptor remains the source of truth for encoded types while these slots
/// map public fields to their physical record positions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRootLayout {
    /// Stable hash of the descriptor, slots, identities and carrier.
    pub id: String,
    /// Exact physical root descriptor used to decode packed bytes.
    pub root_descriptor: RecordDescriptor,
    /// Descriptor slot containing the stable root UUID.
    pub root_key_slot: usize,
    /// Exact descriptor identity of the root UUID slot.
    pub root_key_field_name: String,
    /// Public field-to-descriptor slot mappings, in public output order.
    pub public_fields: Vec<TerminalRootPublicField>,
    /// Physical representation used for public cells.
    pub carrier: TerminalRootCarrier,
    /// The collector key contains an arm discriminator immediately after its
    /// physical root UUID, which denotes source position zero.
    pub root_union_arm: bool,
}

/// One public root field's immutable physical slot identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TerminalRootPublicField {
    /// Authoritative publication binding supplied by the compiler.
    pub publication: crate::node::CurrentRowPublicationField,
    /// Public column name.
    pub name: String,
    /// Physical descriptor field name at `slot`.
    pub descriptor_field_name: String,
    /// Physical descriptor slot.
    pub slot: usize,
    /// Encoded representation of this individual slot.
    pub carrier: TerminalRootCarrier,
}

/// The producer representation applied around declared public column types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalRootCarrier {
    /// A physical `CurrentRow`: each application cell has one extra nullable
    /// carrier around its declared storage type.
    CurrentRow,
    /// A logical collector/projection record with declared storage types.
    Logical,
}

impl std::ops::Deref for SubscriptionOutputRow {
    type Target = CurrentRow;

    fn deref(&self) -> &Self::Target {
        &self.row
    }
}

/// Delta event emitted by a database subscription stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SubscriptionEvent {
    /// Incremental or reset result change.
    Delta {
        /// Whether this delta replaces all previously observed rows and edges.
        ///
        /// Fresh subscriptions start with a reset delta from the empty result.
        reset: bool,
        /// Whether this event represents an authority-backed observation.
        /// A locally constructed opening frame is provisional until an
        /// authority publishes the corresponding reset.
        publishable: bool,
        /// Rows newly visible to the subscription.
        added: Vec<SubscriptionOutputRow>,
        /// Rows still visible with changed projected cells.
        updated: Vec<SubscriptionOutputRow>,
        /// Rows no longer visible to the subscription.
        removed: Vec<RemovedRow>,
        /// Typed structural edits to already hydrated terminal rows.
        terminal_operations: Vec<groove::ivm::TerminalOperation>,
        /// Whether the result is complete at the requested read tier.
        /// Public Edge/Global streams emit only settled results. Local streams
        /// can publish materialized local rows before remote coverage settles.
        settled: bool,
        /// Read tier used to materialize the rows.
        tier: DurabilityTier,
    },
    /// The serving peer rejected the propagated upstream subscription.
    Rejected {
        /// Stable rejection class plus diagnostic detail from the serving peer.
        reason: SubscribeRejectReason,
    },
    /// The subscription stream was closed by the producer.
    Closed,
}

type SubscriptionFinalizationFuture = Pin<Box<dyn Future<Output = Result<(), Error>>>>;
type SubscriptionCleanup =
    Box<dyn FnOnce(Option<oneshot::Sender<()>>) -> Option<SubscriptionFinalizationFuture>>;

enum SubscriptionFinalization {
    Pending(SubscriptionFinalizationFuture),
    Failed { code: ErrorCode, message: String },
}

/// Stream of application-ready subscription events.
///
/// Local results publish as soon as required cells are materialized. Edge and
/// Global results also wait for settlement at the requested tier. Withheld
/// changes are coalesced relative to the last emitted result, so consumers do
/// not maintain provisional snapshots or replay hidden terminal history.
pub struct SubscriptionStream {
    receiver: UnboundedReceiver<SubscriptionEvent>,
    _state: Rc<RefCell<SubscriptionState>>,
    cleanup: Option<SubscriptionCleanup>,
    finalization: Option<SubscriptionFinalization>,
    terminated: bool,
}

struct CleanupGuard {
    cleanup: Option<Box<dyn FnOnce()>>,
}

impl CleanupGuard {
    fn new(cleanup: Box<dyn FnOnce()>) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

    fn take(&mut self) -> Box<dyn FnOnce()> {
        self.cleanup
            .take()
            .expect("cleanup guard must only be disarmed once")
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            cleanup();
        }
    }
}

impl SubscriptionStream {
    /// Queue cancellation, drive finalization under the node owner, and wait
    /// until the local maintained subscription and any upstream coverage
    /// ownership have been retired. The stream owns the in-flight completion,
    /// so cancelling this caller future leaves a later `close` able to resume
    /// and await the same finalization command.
    pub async fn close(&mut self) -> Result<(), Error> {
        if self.finalization.is_none() {
            let Some(cleanup) = self.cleanup.take() else {
                return Ok(());
            };
            // `close` is a terminal stream operation. Do this before awaiting so
            // callers cannot observe an old buffered delta while finalization is
            // suspended behind storage or the node mutex.
            self.terminated = true;
            self._state
                .borrow()
                .sender
                .publication
                .borrow_mut()
                .deferred = None;
            self.receiver.close();
            let (sender, receiver) = oneshot::channel();
            let drain = cleanup(Some(sender));
            self.finalization = Some(SubscriptionFinalization::Pending(Box::pin(async move {
                if let Some(finalization) = drain {
                    finalization.await?;
                }
                receiver.await.map_err(|_| {
                    Error::new(
                        ErrorCode::Protocol,
                        "subscription finalization acknowledgement was dropped",
                    )
                })
            })));
        }

        let result = match self
            .finalization
            .as_mut()
            .expect("close completion must exist after finalization starts")
        {
            SubscriptionFinalization::Pending(completion) => completion.as_mut().await,
            SubscriptionFinalization::Failed { code, message } => {
                return Err(Error::new(*code, message.clone()));
            }
        };
        match result {
            Ok(()) => {
                self.finalization = None;
                Ok(())
            }
            Err(error) => {
                self.finalization = Some(SubscriptionFinalization::Failed {
                    code: error.code,
                    message: error.message.clone(),
                });
                Err(error)
            }
        }
    }

    #[cfg(test)]
    async fn next_raw(&mut self) -> Option<SubscriptionEvent> {
        std::future::poll_fn(|cx| Pin::new(&mut self.receiver).poll_next(cx)).await
    }

    /// Await the next materialized subscription event.
    pub async fn next_event(&mut self) -> Option<SubscriptionEvent> {
        if self.terminated {
            return None;
        }
        loop {
            let event =
                std::future::poll_fn(|cx| Pin::new(&mut self.receiver).poll_next(cx)).await?;
            if subscription_event_is_publishable(&event) {
                return Some(event);
            }
        }
    }

    /// Return the receiver-local maintained snapshot after a settled event.
    ///
    /// This is intentionally crate-private: it is the one-shot counterpart to
    /// consuming a public subscription stream, not another query evaluator.
    /// A remote one-shot first owns a transient subscription, waits for its
    /// exact authority-covered inputs to drive the local maintained graph to
    /// settlement, then takes this snapshot before finalizing that owner.
    ///
    /// The authority may never provide a link/result snapshot here.  Its role
    /// is limited to admitting the covered source closure; the receiver's
    /// maintained graph remains the sole producer of application output.
    #[allow(dead_code)] // The native public facade is feature-gated in the core-only build.
    pub(crate) fn settled_receiver_local_snapshot(&self) -> Result<RelationSnapshot, Error> {
        let state = self._state.borrow();
        if !state.settled {
            return Err(Error::new(
                ErrorCode::Protocol,
                "remote one-shot attempted to materialize before subscription settlement",
            ));
        }
        if state.snapshot_source != SubscriptionSnapshotSource::LocalMaintained {
            return Err(Error::new(
                ErrorCode::Protocol,
                "remote one-shot attempted to materialize a non-local maintained snapshot",
            ));
        }
        materialized_subscription_snapshot(&state.snapshot, &state.snapshot_index)
    }

    /// Return the next queued materialized subscription event without waiting.
    pub fn try_next_event(&mut self) -> Option<SubscriptionEvent> {
        if self.terminated {
            return None;
        }
        loop {
            let event = self.receiver.try_recv().ok()?;
            if subscription_event_is_publishable(&event) {
                return Some(event);
            }
        }
    }

    #[cfg(test)]
    fn retained_plan_authorization_mode(&self) -> Option<QueryAuthorizationMode> {
        let state = self._state.borrow();
        let SubscriptionKind::Prepared {
            maintained_subscription,
            ..
        } = &state.kind;
        maintained_subscription
            .as_ref()
            .and_then(LocalMaintainedViewSubscription::retained_plan_authorization_mode)
    }
}

fn subscription_event_is_publishable(event: &SubscriptionEvent) -> bool {
    !matches!(
        event,
        SubscriptionEvent::Delta {
            publishable: false,
            ..
        }
    )
}

impl Stream for SubscriptionStream {
    type Item = SubscriptionEvent;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.terminated {
            return Poll::Ready(None);
        }
        loop {
            match Pin::new(&mut this.receiver).poll_next(cx) {
                Poll::Ready(Some(event)) if subscription_event_is_publishable(&event) => {
                    return Poll::Ready(Some(event));
                }
                Poll::Ready(Some(_)) => continue,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl Drop for SubscriptionStream {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            drop(cleanup(None));
        }
    }
}

/// Materialized result of a serialized host read.
#[doc(hidden)]
pub enum SerializedReadResult {
    Rows(Vec<CurrentRow>),
    Relation(RelationSnapshot),
}

/// Authorization route used when a host opens a serialized subscription.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[doc(hidden)]
pub enum SerializedSubscriptionAuthorization {
    ClientLocal,
    TrustedServing(AuthorSubject),
    TrustedClient(AuthorSubject),
}

/// Validated and bound query plan used by all `Db` reads and subscriptions.
#[derive(Clone, Debug)]
pub struct PreparedQuery {
    request_identity_claims: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    shape: ValidatedQuery,
    binding: Binding,
    local_plan: Option<PreparedQueryPlanHandle>,
    global_plan: Option<PreparedQueryPlanHandle>,
    /// Plans contain IDs from one live Groove graph registry. A catalogue
    /// change can replace that registry while this public handle survives.
    groove_runtime_token: u64,
}

impl PreparedQuery {
    /// Identity captured by a trusted request scope, if one was attached.
    pub fn request_identity(&self) -> Option<AuthorSubject> {
        self.request_identity_claims
            .as_ref()
            .map(|(author, _)| *author)
    }
    /// Capture the admitted claims for this trusted-serving request.
    /// Clones retain the same immutable scope across asynchronous owner waits.
    pub fn with_identity_claims(
        mut self,
        author: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Self {
        self.request_identity_claims = Some((author, claims));
        // Plans prepared before admission may embed SYSTEM or another scope.
        // Resolve them again under this immutable request's authorization.
        self.local_plan = None;
        self.global_plan = None;
        self
    }

    fn request_policy_binding(
        &self,
        author: AuthorSubject,
    ) -> Result<Option<(AuthorSubject, BTreeMap<String, Value>)>, Error> {
        if self
            .request_identity_claims
            .as_ref()
            .is_some_and(|(admitted, _)| *admitted != author)
        {
            return Err(Error::new(
                ErrorCode::Protocol,
                "prepared request identity does not match read identity",
            ));
        }
        Ok(self.request_identity_claims.clone())
    }

    fn scoped_node<'a, S: OrderedKvStorage>(
        &self,
        node: &'a mut NodeState<S>,
        author: AuthorSubject,
    ) -> Result<crate::node::ActiveSessionClaimsScope<'a, S>, Error> {
        self.request_policy_binding(author)?;
        Ok(node.scoped_optional_session_claims(
            author,
            self.request_identity_claims
                .as_ref()
                .map(|(_, claims)| claims.clone()),
        ))
    }

    /// Validated query shape.
    pub fn shape(&self) -> &ValidatedQuery {
        &self.shape
    }

    /// Bound parameter values.
    pub fn binding(&self) -> &Binding {
        &self.binding
    }

    fn plan_for_tier(
        &self,
        tier: DurabilityTier,
        groove_runtime_token: u64,
    ) -> Option<&PreparedQueryPlanHandle> {
        if self.groove_runtime_token != groove_runtime_token {
            return None;
        }
        match tier {
            DurabilityTier::Local => self.local_plan.as_ref(),
            DurabilityTier::Global => self.global_plan.as_ref(),
            DurabilityTier::None | DurabilityTier::Edge => None,
        }
    }

    #[cfg(test)]
    fn has_plan_for_tier(&self, tier: DurabilityTier) -> bool {
        self.plan_for_tier(tier, self.groove_runtime_token)
            .is_some()
    }
}

fn should_install_prepared_plan(shape: &ValidatedQuery) -> bool {
    !shape.query().joins.is_empty() || !shape.query().reachable.is_empty()
}

fn subscription_delta_event(
    tier: DurabilityTier,
    settled: bool,
    previous: &RelationSnapshot,
    current: &RelationSnapshot,
    terminal_rows: bool,
) -> SubscriptionEvent {
    subscription_delta_event_with_reset(tier, settled, previous, current, false, terminal_rows)
}

/// Retire authority settlement without disturbing the receiver-local terminal
/// snapshot. A stale/nonselected authority update invalidates only the receipt:
/// the same local collector continues to own the visible rows until a fresh
/// exact closure arrives.
pub(in crate::db) fn demote_authority_receipt_subscriptions(
    subscriptions: &SubscriptionList,
    publishing_subscriptions: &BTreeSet<SubscriptionKey>,
) {
    let mut retained = Vec::new();
    for weak in subscriptions.borrow().iter() {
        let Some(state) = weak.upgrade() else {
            continue;
        };
        {
            let mut state_ref = state.borrow_mut();
            if state_ref.propagates_upstream {
                state_ref.requires_authority_receipt = true;
                if state_ref.settled {
                    state_ref.settled = false;
                    // The named receiver will publish the same demotion while
                    // applying its frame. Other subscriptions share the
                    // authority receipt but have no terminal frame of their
                    // own, so publish only their receipt-only transition.
                    let frame_will_publish = state_ref
                        .upstream_subscription_handles
                        .iter()
                        .any(|handle| publishing_subscriptions.contains(&handle.subscription));
                    if !frame_will_publish && state_ref.read_tier < DurabilityTier::Edge {
                        let event = subscription_delta_event(
                            state_ref.read_tier,
                            false,
                            &state_ref.snapshot,
                            &state_ref.snapshot,
                            state_ref.terminal_rows,
                        );
                        let _ = state_ref.sender.unbounded_send(event);
                    }
                }
            }
        }
        retained.push(Rc::downgrade(&state));
    }
    *subscriptions.borrow_mut() = retained;
}

/// Publishes an ordered terminal as explicit root placements.
///
/// Every changed occurrence carries its previous and final position, so
/// consumers never reconstruct ordering from a suffix convention.
fn subscription_terminal_delta_event(
    tier: DurabilityTier,
    settled: bool,
    previous: &RelationSnapshot,
    previous_occurrences: &[OutputOccurrenceId],
    current: &RelationSnapshot,
    current_occurrences: &[OutputOccurrenceId],
) -> Result<SubscriptionEvent, Error> {
    let previous_roots = &previous.rows[..previous.root_count];
    let current_roots = &current.rows[..current.root_count];
    if previous_roots.len() != previous_occurrences.len()
        || current_roots.len() != current_occurrences.len()
    {
        return Err(Error::new(
            ErrorCode::Protocol,
            "maintained terminal occurrence sidecar length does not match root rows",
        ));
    }
    let common_prefix = previous_roots
        .iter()
        .zip(previous_occurrences)
        .zip(current_roots.iter().zip(current_occurrences))
        .take_while(|((_, previous), (_, current))| previous == current)
        .count();

    let mut updated = previous_roots[..common_prefix]
        .iter()
        .zip(&current_roots[..common_prefix])
        .zip(&current_occurrences[..common_prefix])
        .enumerate()
        .filter(|(_, ((previous, current), _))| !previous.subscription_equivalent(current))
        .map(
            |(index, ((_, current), occurrence_id))| SubscriptionOutputRow {
                occurrence_id: occurrence_id.clone(),
                row: current.clone(),
                previous_index: Some(index),
                index,
            },
        )
        .collect::<Vec<_>>();
    let previous_suffix_positions = previous_occurrences[common_prefix..]
        .iter()
        .cloned()
        .enumerate()
        .map(|(offset, occurrence)| (occurrence, common_prefix + offset))
        .collect::<BTreeMap<_, _>>();
    let current_suffix = current_occurrences[common_prefix..]
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    let removed = previous_roots[common_prefix..]
        .iter()
        .zip(&previous_occurrences[common_prefix..])
        .enumerate()
        .filter(|(_, (_, occurrence_id))| !current_suffix.contains(*occurrence_id))
        .map(|(offset, (row, occurrence_id))| RemovedRow {
            table: row.table().to_owned(),
            row_uuid: row.row_uuid(),
            occurrence_id: occurrence_id.clone(),
            index: common_prefix + offset,
        })
        .collect::<Vec<_>>();
    let mut added = Vec::new();
    for (offset, (row, occurrence_id)) in current_roots[common_prefix..]
        .iter()
        .zip(&current_occurrences[common_prefix..])
        .enumerate()
    {
        let index = common_prefix + offset;
        if let Some(previous_index) = previous_suffix_positions.get(occurrence_id).copied() {
            updated.push(SubscriptionOutputRow {
                occurrence_id: occurrence_id.clone(),
                row: row.clone(),
                previous_index: Some(previous_index),
                index,
            });
        } else {
            added.push(SubscriptionOutputRow {
                occurrence_id: occurrence_id.clone(),
                row: row.clone(),
                previous_index: None,
                index,
            });
        }
    }

    Ok(SubscriptionEvent::Delta {
        reset: false,
        publishable: true,
        added,
        updated,
        removed,
        terminal_operations: Vec::new(),
        settled,
        tier,
    })
}

fn subscription_delta_event_with_reset(
    tier: DurabilityTier,
    settled: bool,
    previous: &RelationSnapshot,
    current: &RelationSnapshot,
    reset: bool,
    _terminal_rows: bool,
) -> SubscriptionEvent {
    // A reset is a complete ordered snapshot.  Re-keying it through the
    // occurrence BTreeMap below would sort by identity and silently discard
    // the maintained query order (for example `order_by rank`).
    if reset {
        return SubscriptionEvent::Delta {
            reset: true,
            publishable: true,
            added: current
                .rows
                .iter()
                .cloned()
                .enumerate()
                .map(|(index, row)| subscription_output_row(row, None, index))
                .collect(),
            updated: Vec::new(),
            removed: Vec::new(),
            terminal_operations: Vec::new(),
            settled,
            tier,
        };
    }
    let mut previous_by_id = BTreeMap::new();
    for (index, row) in previous.rows.iter().enumerate() {
        previous_by_id.insert(subscription_row_key(row), (index, row));
    }

    let mut current_by_id = BTreeMap::new();
    for (index, row) in current.rows.iter().enumerate() {
        current_by_id.insert(subscription_row_key(row), (index, row));
    }

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();
    for (key, (index, row)) in &current_by_id {
        match previous_by_id.get(key) {
            None => added.push(subscription_output_row((*row).clone(), None, *index)),
            Some((previous_index, previous_row))
                if !previous_row.subscription_equivalent(row) || *previous_index != *index =>
            {
                updated.push(subscription_output_row(
                    (*row).clone(),
                    Some(*previous_index),
                    *index,
                ))
            }
            Some(_) => {}
        }
    }

    for (key, (index, _)) in &previous_by_id {
        if !current_by_id.contains_key(key) {
            let row = previous_by_id[key].1;
            removed.push(RemovedRow {
                table: row.table().to_owned(),
                row_uuid: row.row_uuid(),
                occurrence_id: key.clone(),
                index: *index,
            });
        }
    }

    SubscriptionEvent::Delta {
        reset,
        publishable: true,
        added,
        updated,
        removed,
        terminal_operations: Vec::new(),
        settled,
        tier,
    }
}

fn apply_maintained_update_to_snapshot(
    snapshot: &mut RelationSnapshot,
    snapshot_index: &mut RelationSnapshotIndex,
    update: LocalMaintainedViewSubscriptionUpdate,
    table: &str,
    tier: DurabilityTier,
    settled: bool,
    terminal_layout: Option<&TerminalRootLayout>,
) -> Result<SubscriptionEvent, Error> {
    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
        let update_kind = match &update {
            LocalMaintainedViewSubscriptionUpdate::Structured {
                terminal_operations,
            } => format!("structured:{}", terminal_operations.len()),
            LocalMaintainedViewSubscriptionUpdate::Flat { added, removed, .. } => {
                format!("flat:add={} remove={}", added.len(), removed.len())
            }
            LocalMaintainedViewSubscriptionUpdate::AggregateWindow {
                snapshot,
                occurrence_ids,
            } => format!(
                "aggregate-window:roots={} occurrences={}",
                snapshot.root_count,
                occurrence_ids.len()
            ),
        };
        eprintln!(
            "JAZZ_COVERED_INPUT_TRACE stage=apply_maintained_snapshot roots={} update={update_kind}",
            snapshot.root_count,
        );
    }
    match update {
        LocalMaintainedViewSubscriptionUpdate::AggregateWindow {
            snapshot: current,
            occurrence_ids,
        } => {
            // Validate the entire replacement facade before touching the
            // currently published snapshot. In particular, a BTreeMap would
            // otherwise silently collapse duplicate occurrence identities.
            let current_index =
                relation_snapshot_index_with_root_occurrences(&current, &occurrence_ids)?;
            let event = subscription_terminal_delta_event(
                tier,
                settled,
                snapshot,
                &snapshot_root_occurrences(snapshot, snapshot_index)?,
                &current,
                &occurrence_ids,
            )?;
            *snapshot = current;
            *snapshot_index = current_index;
            Ok(event)
        }
        LocalMaintainedViewSubscriptionUpdate::Flat { added, removed } => {
            let mut event = apply_maintained_membership_update_to_snapshot(
                snapshot,
                snapshot_index,
                added,
                removed,
                tier,
                settled,
            );
            let SubscriptionEvent::Delta { added, updated, .. } = &mut event else {
                unreachable!("maintained updates always emit deltas")
            };
            for output in added.iter_mut().chain(updated.iter_mut()) {
                let Some(index) = snapshot_index.roots.get(&output.occurrence_id).copied() else {
                    continue;
                };
                output.row = snapshot.rows[index].clone();
                output.index = index;
            }
            Ok(event)
        }
        LocalMaintainedViewSubscriptionUpdate::Structured {
            terminal_operations,
        } => {
            if terminal_operations.is_empty() {
                return Ok(SubscriptionEvent::Delta {
                    reset: false,
                    publishable: true,
                    added: Vec::new(),
                    updated: Vec::new(),
                    removed: Vec::new(),
                    terminal_operations: Vec::new(),
                    settled,
                    tier,
                });
            }
            let layout = terminal_layout.ok_or_else(|| {
                Error::new(
                    ErrorCode::Protocol,
                    "structured terminal operation arrived without a prepared root layout",
                )
            })?;
            let known_occurrences = snapshot_index.roots.keys().cloned().collect::<Vec<_>>();
            let mut occurrence_overrides = BTreeMap::new();
            for operation in terminal_operations
                .iter()
                .filter(|operation| operation.path.is_empty())
            {
                if terminal_root_occurrence_id_with_root_union(
                    &operation.root_key,
                    layout.root_union_arm,
                )
                .is_ok()
                {
                    continue;
                }
                let root_bytes = operation
                    .root_key
                    .get(1..17)
                    .filter(|_| operation.root_key.first().copied() == Some(10));
                let candidates = known_occurrences
                    .iter()
                    .filter(|candidate| candidate.canonical_bytes().get(..16) == root_bytes)
                    .cloned()
                    .collect::<Vec<_>>();
                match candidates.as_slice() {
                    [candidate] => {
                        occurrence_overrides.insert(operation.root_key.clone(), candidate.clone());
                    }
                    [] => {
                        terminal_root_occurrence_id_with_root_union(
                            &operation.root_key,
                            layout.root_union_arm,
                        )?;
                    }
                    _ => {
                        return Err(Error::new(
                            ErrorCode::Protocol,
                            "terminal root key is ambiguous after hiding internal join identities",
                        ));
                    }
                }
            }
            apply_terminal_operations_to_subscription_snapshot(
                snapshot,
                snapshot_index,
                terminal_operations,
                Some(&occurrence_overrides),
                layout,
                table,
                tier,
                settled,
            )
        }
    }
}

fn apply_maintained_membership_update_to_snapshot(
    snapshot: &mut RelationSnapshot,
    snapshot_index: &mut RelationSnapshotIndex,
    update_added: Vec<(OutputOccurrenceId, CurrentRow)>,
    update_removed: Vec<OutputOccurrenceId>,
    tier: DurabilityTier,
    settled: bool,
) -> SubscriptionEvent {
    if snapshot.rows.is_empty()
        && snapshot.edges.is_empty()
        && snapshot.root_count == 0
        && update_removed.is_empty()
    {
        snapshot.root_count = update_added.len();
        snapshot.rows.reserve(update_added.len());
        snapshot
            .rows
            .extend(update_added.iter().map(|(_, row)| row.clone()));
        snapshot_index.roots = update_added
            .iter()
            .enumerate()
            .map(|(index, (occurrence, _))| (occurrence.clone(), index))
            .collect();
        return SubscriptionEvent::Delta {
            reset: false,
            publishable: true,
            added: update_added
                .into_iter()
                .enumerate()
                .map(|(index, (occurrence_id, row))| SubscriptionOutputRow {
                    occurrence_id,
                    row,
                    previous_index: None,
                    index,
                })
                .collect(),
            updated: Vec::new(),
            removed: Vec::new(),
            terminal_operations: Vec::new(),
            settled,
            tier,
        };
    }

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();

    for (key, row) in &update_added {
        if let Some(position) = snapshot_index.roots.get(&key).copied() {
            let equivalent = snapshot.rows[position].subscription_equivalent(row);
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=flat_snapshot_replace occurrence={key:?} position={position} equivalent={equivalent} old={:?} new={:?}",
                    snapshot.rows[position], row,
                );
            }
            if !equivalent {
                snapshot.rows[position] = row.clone();
                updated.push(SubscriptionOutputRow {
                    occurrence_id: key.clone(),
                    row: row.clone(),
                    previous_index: Some(position),
                    index: position,
                });
            }
        } else {
            let index = snapshot.root_count;
            snapshot.rows.insert(index, row.clone());
            for position in snapshot_index.related.values_mut() {
                *position += 1;
            }
            snapshot_index.roots.insert(key.clone(), index);
            snapshot.root_count += 1;
            added.push(SubscriptionOutputRow {
                occurrence_id: key.clone(),
                row: row.clone(),
                previous_index: None,
                index,
            });
        }
    }

    if !update_removed.is_empty() {
        let replaced = update_added
            .iter()
            .map(|(occurrence, _)| occurrence)
            .collect::<BTreeSet<_>>();
        let requested_removals = update_removed.iter().collect::<BTreeSet<_>>();
        let mut removals = requested_removals
            .iter()
            .filter(|occurrence| !replaced.contains(*occurrence))
            .filter_map(|occurrence| {
                snapshot_index
                    .roots
                    .get(*occurrence)
                    .copied()
                    .map(|position| ((*occurrence).clone(), position))
            })
            .collect::<Vec<_>>();
        removals.sort_by_key(|(_, position)| *position);
        if !removals.is_empty() {
            // A removed index is part of the public delta contract: it is the
            // position in the complete pre-frame result, not the position after
            // a preceding removal has already shifted the snapshot.  Remove the
            // whole batch together so both that contract and the index repair are
            // linear in the result size rather than quadratic in removed roots.
            let removed_positions = removals
                .iter()
                .map(|(_, position)| *position)
                .collect::<Vec<_>>();
            let removal_ids = removals
                .iter()
                .map(|(occurrence, _)| occurrence)
                .collect::<BTreeSet<_>>();
            removed.extend(removals.iter().map(|(occurrence_id, index)| {
                let row = &snapshot.rows[*index];
                RemovedRow {
                    table: row.table().to_owned(),
                    row_uuid: row.row_uuid(),
                    occurrence_id: occurrence_id.clone(),
                    index: *index,
                }
            }));
            let root_count_before_removals = snapshot.root_count;
            let mut position = 0;
            snapshot.rows.retain(|_| {
                let keep = position >= root_count_before_removals
                    || removed_positions.binary_search(&position).is_err();
                position += 1;
                keep
            });
            snapshot.root_count -= removed_positions.len();
            snapshot_index
                .roots
                .retain(|occurrence, _| !removal_ids.contains(occurrence));
            for position in snapshot_index.roots.values_mut() {
                *position -= removed_positions.partition_point(|removed| removed < position);
            }
            for position in snapshot_index.related.values_mut() {
                *position -= removed_positions.len();
            }
        }
    }

    SubscriptionEvent::Delta {
        reset: false,
        publishable: true,
        added,
        updated,
        removed,
        terminal_operations: Vec::new(),
        settled,
        tier,
    }
}

/// Applies only top-level structured-terminal edits to the producer-owned
/// subscription snapshot. Descendant edits remain in the returned event for
/// binding object reducers, which own nested object materialization.
fn apply_terminal_operations_to_subscription_snapshot(
    snapshot: &mut RelationSnapshot,
    snapshot_index: &mut RelationSnapshotIndex,
    operations: Vec<groove::ivm::TerminalOperation>,
    occurrence_overrides: Option<&BTreeMap<Vec<u8>, OutputOccurrenceId>>,
    layout: &TerminalRootLayout,
    table: &str,
    tier: DurabilityTier,
    settled: bool,
) -> Result<SubscriptionEvent, Error> {
    let mut root_operations = Vec::new();
    let mut descendant_operations = Vec::new();
    for operation in operations {
        if operation.root_descriptor != layout.root_descriptor {
            return Err(Error::new(
                ErrorCode::Protocol,
                "terminal operation descriptor disagrees with its prepared root layout",
            ));
        }
        if operation.path.is_empty() {
            let edit_key = match &operation.edit {
                groove::ivm::TerminalEdit::Insert { key, .. }
                | groove::ivm::TerminalEdit::Update { key, .. }
                | groove::ivm::TerminalEdit::Remove { key }
                | groove::ivm::TerminalEdit::Move { key, .. } => key,
            };
            if edit_key != &operation.root_key {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "terminal root edit key does not match its addressed root key",
                ));
            }
            let occurrence_id = occurrence_overrides
                .and_then(|overrides| overrides.get(operation.root_key.as_slice()))
                .cloned()
                .map(Ok)
                .unwrap_or_else(|| {
                    terminal_root_occurrence_id_with_root_union(
                        &operation.root_key,
                        layout.root_union_arm,
                    )
                })?;
            root_operations.push((occurrence_id, operation));
        } else {
            descendant_operations.push(operation);
        }
    }

    let mut occurrences = snapshot_root_occurrences(snapshot, snapshot_index)?;
    let affected = root_operations
        .iter()
        .map(|(occurrence_id, _)| occurrence_id.clone())
        .collect::<BTreeSet<_>>();
    for occurrence in &affected {
        materialize_subscription_terminal_record(snapshot, snapshot_index, occurrence)?;
    }
    let before = affected
        .iter()
        .filter_map(|occurrence_id| {
            let index = occurrences
                .iter()
                .position(|current| current == occurrence_id)?;
            Some((occurrence_id.clone(), (index, snapshot.rows[index].clone())))
        })
        .collect::<BTreeMap<_, _>>();

    let inserted = root_operations
        .iter()
        .filter_map(|(occurrence, operation)| match operation.edit {
            groove::ivm::TerminalEdit::Insert { .. } => Some((occurrence.clone(), true)),
            groove::ivm::TerminalEdit::Remove { .. } => Some((occurrence.clone(), false)),
            _ => None,
        })
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .filter_map(|(key, present)| present.then_some(key))
        .collect::<BTreeSet<_>>();
    let replaced = root_operations
        .iter()
        .filter_map(|(occurrence, operation)| {
            (matches!(operation.edit, groove::ivm::TerminalEdit::Remove { .. })
                && inserted.contains(occurrence))
            .then_some(occurrence.clone())
        })
        .collect::<BTreeSet<_>>();
    for (occurrence, operation) in &root_operations {
        if replaced.contains(occurrence) {
            if let groove::ivm::TerminalEdit::Insert { value, .. } = &operation.edit {
                if let Some(state) = snapshot_index.terminal_records.get_mut(occurrence) {
                    state.update_record(OwnedRecord::new(
                        value.clone(),
                        operation.root_descriptor,
                    ))?;
                }
            }
            continue;
        }
        if let groove::ivm::TerminalEdit::Update { value, .. } = &operation.edit {
            if let Some(state) = snapshot_index.terminal_records.get_mut(occurrence) {
                state.update_record(OwnedRecord::new(value.clone(), operation.root_descriptor))?;
            }
        } else if !matches!(operation.edit, groove::ivm::TerminalEdit::Move { .. }) {
            snapshot_index.terminal_records.remove(occurrence);
        }
    }

    for (occurrence_id, operation) in root_operations {
        match operation.edit {
            groove::ivm::TerminalEdit::Insert { index, value, .. } => {
                if let Some(existing) = occurrences
                    .iter()
                    .position(|current| current == &occurrence_id)
                {
                    occurrences.remove(existing);
                    snapshot.rows.remove(existing);
                    snapshot.root_count -= 1;
                }
                let index = index.min(snapshot.root_count);
                let row = terminal_subscription_output_row(
                    table,
                    occurrence_id.clone(),
                    &value,
                    layout,
                    None,
                    index,
                )?
                .row;
                occurrences.insert(index, occurrence_id);
                snapshot.rows.insert(index, row);
                snapshot.root_count += 1;
            }
            groove::ivm::TerminalEdit::Update { value, .. } => {
                let Some(index) = occurrences
                    .iter()
                    .position(|current| current == &occurrence_id)
                else {
                    return Err(Error::new(
                        ErrorCode::Protocol,
                        "terminal root update addressed a missing result",
                    ));
                };
                snapshot.rows[index] = terminal_subscription_output_row(
                    table,
                    occurrence_id,
                    &value,
                    layout,
                    Some(index),
                    index,
                )?
                .row;
            }
            groove::ivm::TerminalEdit::Remove { .. } => {
                let Some(index) = occurrences
                    .iter()
                    .position(|current| current == &occurrence_id)
                else {
                    return Err(Error::new(
                        ErrorCode::Protocol,
                        "terminal root removal addressed a missing result",
                    ));
                };
                occurrences.remove(index);
                snapshot.rows.remove(index);
                snapshot.root_count -= 1;
            }
            groove::ivm::TerminalEdit::Move { index, .. } => {
                let Some(previous_index) = occurrences
                    .iter()
                    .position(|current| current == &occurrence_id)
                else {
                    return Err(Error::new(
                        ErrorCode::Protocol,
                        "terminal root move addressed a missing result",
                    ));
                };
                let occurrence_id = occurrences.remove(previous_index);
                let row = snapshot.rows.remove(previous_index);
                let index = index.min(snapshot.root_count.saturating_sub(1));
                occurrences.insert(index, occurrence_id);
                snapshot.rows.insert(index, row);
            }
        }
    }

    // The facade retains the exact collector tree by folding the same
    // root/descendant terminal stream it exposes to consumers.  This is not
    // a second materializer: a later reset is simply a snapshot of this
    // receiver-local reducer after the ordered operations below have been
    // applied.  In particular, an authority-covered receiver must never
    // reconstruct nested children from result membership or authority facts.
    apply_descendant_terminal_operations_to_snapshot(
        snapshot,
        snapshot_index,
        &occurrences,
        &affected,
        &descendant_operations,
        layout.root_union_arm,
    )?;

    let terminal_records = std::mem::take(&mut snapshot_index.terminal_records);
    *snapshot_index = RelationSnapshotIndex::from_snapshot(snapshot);
    snapshot_index.terminal_records = terminal_records;
    snapshot_index.roots = root_occurrence_positions(&occurrences);

    for occurrence in &affected {
        materialize_subscription_terminal_record(snapshot, snapshot_index, occurrence)?;
    }

    let mut added = Vec::new();
    let mut updated = Vec::new();
    let mut removed = Vec::new();
    for occurrence_id in affected {
        let previous = before.get(&occurrence_id);
        let current = occurrences
            .iter()
            .position(|current| current == &occurrence_id)
            .map(|index| (index, &snapshot.rows[index]));
        match (previous, current) {
            (None, Some((index, row))) => added.push(SubscriptionOutputRow {
                occurrence_id,
                row: row.clone(),
                previous_index: None,
                index,
            }),
            (Some((previous_index, row)), None) => removed.push(RemovedRow {
                table: row.table().to_owned(),
                row_uuid: row.row_uuid(),
                occurrence_id,
                index: *previous_index,
            }),
            (Some((previous_index, previous_row)), Some((index, row)))
                if previous_row != row || *previous_index != index =>
            {
                updated.push(SubscriptionOutputRow {
                    occurrence_id,
                    row: row.clone(),
                    previous_index: Some(*previous_index),
                    index,
                });
            }
            _ => {}
        }
    }

    Ok(SubscriptionEvent::Delta {
        reset: false,
        publishable: true,
        added,
        updated,
        removed,
        terminal_operations: descendant_operations,
        settled,
        tier,
    })
}

/// Fold descendant edits into the private receiver snapshot while preserving
/// the original operations for public incremental delivery.  A terminal path
/// names only compiler-owned collection fields and source-row UUID keys, so a
/// malformed path or non-canonical child identity is a protocol error rather
/// than a reason to search a table/result relation for a plausible match.
fn apply_descendant_terminal_operations_to_snapshot(
    snapshot: &mut RelationSnapshot,
    snapshot_index: &mut RelationSnapshotIndex,
    occurrences: &[OutputOccurrenceId],
    roots_changed_in_batch: &BTreeSet<OutputOccurrenceId>,
    operations: &[groove::ivm::TerminalOperation],
    root_union_arm: bool,
) -> Result<(), Error> {
    for operation in operations {
        if operation.path.is_empty() {
            continue;
        }
        let occurrence =
            terminal_root_occurrence_id_with_root_union(&operation.root_key, root_union_arm)?;
        let Some(root_index) = occurrences
            .iter()
            .position(|candidate| candidate == &occurrence)
        else {
            // A collector can emit the child retractions belonging to a root
            // it retracts in the same terminal batch.  The public operation
            // remains useful to a consumer which folds its child state before
            // the root removal, but the receiver snapshot has already
            // dropped that root in the root-edit phase above.
            if roots_changed_in_batch.contains(&occurrence) {
                continue;
            }
            return Err(Error::new(
                ErrorCode::Protocol,
                "terminal descendant operation addressed a missing root",
            ));
        };
        let root = snapshot.rows.get_mut(root_index).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "terminal root position is outside the receiver snapshot",
            )
        })?;
        if !snapshot_index.terminal_records.contains_key(&occurrence) {
            let (descriptor, raw) = root.encoded_record();
            snapshot_index.terminal_records.insert(
                occurrence.clone(),
                terminal_record::TerminalRecordState::new(OwnedRecord::new(
                    raw.to_vec(),
                    *descriptor,
                ))?,
            );
        }
        snapshot_index
            .terminal_records
            .get_mut(&occurrence)
            .expect("initialized above")
            .apply(operation)?;
    }
    Ok(())
}

/// Materialize the retained terminal state only at a full-snapshot boundary.
/// Delta consumers receive the original child operations, not a rebuilt root.
fn materialized_subscription_snapshot(
    snapshot: &RelationSnapshot,
    index: &RelationSnapshotIndex,
) -> Result<RelationSnapshot, Error> {
    let mut snapshot = snapshot.clone();
    materialize_subscription_terminal_records(&mut snapshot, index)?;
    Ok(snapshot)
}

fn materialize_subscription_terminal_records(
    snapshot: &mut RelationSnapshot,
    index: &RelationSnapshotIndex,
) -> Result<(), Error> {
    for occurrence in index.terminal_records.keys() {
        materialize_subscription_terminal_record(snapshot, index, occurrence)?;
    }
    Ok(())
}

fn materialize_subscription_terminal_record(
    snapshot: &mut RelationSnapshot,
    index: &RelationSnapshotIndex,
    occurrence: &OutputOccurrenceId,
) -> Result<(), Error> {
    if let Some(record) = index.terminal_records.get(occurrence) {
        let position = index.roots.get(occurrence).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "retained terminal record has no root occurrence",
            )
        })?;
        let root = snapshot.rows.get_mut(*position).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "retained terminal root position is outside snapshot",
            )
        })?;
        let table = root.table().to_owned();
        let publication_fields = root.publication_fields().to_vec();
        *root =
            CurrentRow::new_with_publication_fields(table, record.record()?, publication_fields);
    }
    Ok(())
}

fn terminal_child_key(value: &Value) -> Result<Vec<u8>, Error> {
    let Value::Record(record) = value else {
        return Err(Error::new(
            ErrorCode::Protocol,
            "terminal descendant collection contains a non-record child",
        ));
    };
    let Value::Uuid(row_uuid) = record.get_idx(0).map_err(|error| {
        Error::new(
            ErrorCode::Protocol,
            format!("cannot decode terminal child key: {error}"),
        )
    })?
    else {
        return Err(Error::new(
            ErrorCode::Protocol,
            "terminal descendant child key must be its physical row UUID",
        ));
    };
    let mut key = Vec::with_capacity(17);
    key.push(10);
    key.extend_from_slice(row_uuid.as_bytes());
    Ok(key)
}

fn terminal_subscription_output_row(
    table: &str,
    occurrence_id: OutputOccurrenceId,
    raw: &[u8],
    layout: &TerminalRootLayout,
    previous_index: Option<usize>,
    index: usize,
) -> Result<SubscriptionOutputRow, Error> {
    let fields = layout.root_descriptor.fields();
    let Some(root_field) = fields.get(layout.root_key_slot) else {
        return Err(Error::new(
            ErrorCode::Protocol,
            "terminal root layout key slot is out of bounds",
        ));
    };
    if root_field.name.as_deref() != Some(layout.root_key_field_name.as_str()) {
        return Err(Error::new(
            ErrorCode::Protocol,
            "terminal root layout key slot does not match its descriptor",
        ));
    }
    if layout.root_key_slot != 0 {
        return Err(Error::new(
            ErrorCode::Protocol,
            "terminal root layout cannot be represented as a current row",
        ));
    }
    let mut occupied_slots = BTreeSet::from([layout.root_key_slot]);
    for field in &layout.public_fields {
        let Some(descriptor_field) = fields.get(field.slot) else {
            return Err(Error::new(
                ErrorCode::Protocol,
                format!("terminal root layout field {} is out of bounds", field.name),
            ));
        };
        if descriptor_field.name.as_deref() != Some(field.descriptor_field_name.as_str())
            || !occupied_slots.insert(field.slot)
        {
            return Err(Error::new(
                ErrorCode::Protocol,
                format!(
                    "terminal root layout field {} does not match its descriptor",
                    field.name
                ),
            ));
        }
    }

    let borrowed = BorrowedRecord::new(raw, &layout.root_descriptor);
    borrowed.to_values().map_err(|error| {
        Error::new(
            ErrorCode::Protocol,
            format!("invalid terminal root payload: {error}"),
        )
    })?;
    let row_uuid = borrowed.get_uuid(layout.root_key_slot).map_err(|error| {
        Error::new(
            ErrorCode::Protocol,
            format!("invalid terminal root key: {error}"),
        )
    })?;
    if occurrence_id.canonical_bytes().get(..16) != Some(row_uuid.as_bytes()) {
        return Err(Error::new(
            ErrorCode::Protocol,
            "terminal root payload key does not match its addressed result",
        ));
    }

    Ok(SubscriptionOutputRow {
        occurrence_id,
        row: CurrentRow::new_with_publication_fields(
            table.to_owned(),
            OwnedRecord::new(raw.to_vec(), layout.root_descriptor.clone()),
            terminal_root_publication_fields(layout),
        ),
        previous_index,
        index,
    })
}

/// Derive the explicit producer provenance for every terminal descriptor slot.
///
/// Both terminal-delta decoding and local maintained-view reset snapshots use
/// this exact mapping; treating a hybrid collector record as wholly logical
/// loses the distinction between a physical `user_{column}` and a logical
/// field with that same name.
pub(crate) fn terminal_root_publication_fields(
    layout: &TerminalRootLayout,
) -> Vec<crate::node::CurrentRowPublicationField> {
    use crate::node::CurrentRowPublicationField;
    let mut fields = layout
        .root_descriptor
        .fields()
        .iter()
        .map(|field| CurrentRowPublicationField::ResultField {
            name: field.name.clone().expect("terminal fields are named"),
            visibility: crate::node::CurrentRowResultVisibility::HiddenMetadata,
        })
        .collect::<Vec<_>>();
    for field in &layout.public_fields {
        assert_eq!(
            field.publication.public_name(),
            Some(field.name.as_str()),
            "terminal publication name must match its public slot mapping"
        );
        fields[field.slot] = field.publication.clone();
    }
    fields
}

#[cfg(test)]
pub(crate) fn terminal_root_binding_fields(
    layout: &TerminalRootLayout,
) -> Vec<CurrentRowBindingRole> {
    let binding_for_carrier = |carrier| match carrier {
        TerminalRootCarrier::CurrentRow => CurrentRowBindingRole::PhysicalColumn,
        TerminalRootCarrier::Logical => CurrentRowBindingRole::LogicalField,
    };
    let mut fields =
        vec![binding_for_carrier(layout.carrier); layout.root_descriptor.fields().len()];
    for field in &layout.public_fields {
        fields[field.slot] = binding_for_carrier(field.carrier);
    }
    fields
}

/// Public logical names for the same terminal descriptor slots.  A terminal
/// projection can retain its source's `user_{column}` carrier name while its
/// public output is simply `{column}`.  Native hosts must receive the latter
/// without guessing from a prefix, while truly logical `user_*` fields remain
/// untouched.
#[cfg(test)]
pub(crate) fn terminal_root_binding_field_names(
    layout: &TerminalRootLayout,
) -> Vec<Option<String>> {
    let mut names = vec![None; layout.root_descriptor.fields().len()];
    for field in &layout.public_fields {
        names[field.slot] = Some(field.name.clone());
    }
    names
}

/// Decode the Groove ordered key used to address one root output occurrence.
/// Plain joins are UUID sequences; joined-source discriminators precede their
/// UUIDs at source positions one and above. Root-union collector keys retain
/// their physical UUID first and need the prepared layout to identify the
/// following `(label, actual-root-row)` pair as source position zero.
pub(crate) fn terminal_root_occurrence_id_with_root_union(
    encoded: &[u8],
    root_union_arm: bool,
) -> Result<OutputOccurrenceId, Error> {
    fn uuid_at(encoded: &[u8], cursor: &mut usize) -> Option<ObjectId> {
        if encoded.get(*cursor).copied() != Some(10) {
            return None;
        }
        let start = *cursor + 1;
        let end = start + 16;
        let uuid = uuid::Uuid::from_slice(encoded.get(start..end)?).ok()?;
        *cursor = end;
        Some(ObjectId::from_uuid(uuid))
    }

    fn ordered_string_at(encoded: &[u8], cursor: &mut usize) -> Option<String> {
        if encoded.get(*cursor).copied() != Some(6) {
            return None;
        }
        *cursor += 1;
        let mut decoded = Vec::new();
        loop {
            let byte = *encoded.get(*cursor)?;
            *cursor += 1;
            if byte != 0 {
                decoded.push(byte);
                continue;
            }
            match encoded.get(*cursor).copied()? {
                0 => {
                    *cursor += 1;
                    break;
                }
                0xff => {
                    *cursor += 1;
                    decoded.push(0);
                }
                _ => return None,
            }
        }
        String::from_utf8(decoded).ok()
    }

    let mut cursor = 0;
    let root = uuid_at(encoded, &mut cursor).ok_or_else(|| {
        Error::new(
            ErrorCode::Protocol,
            "terminal root key must begin with a UUID",
        )
    })?;
    let mut joined = Vec::new();
    let mut union_arms = Vec::new();
    if root_union_arm {
        let label = ordered_string_at(encoded, &mut cursor).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "terminal root key contains an invalid root union discriminator",
            )
        })?;
        let actual_root = uuid_at(encoded, &mut cursor).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "terminal root key contains no root union contributor",
            )
        })?;
        if actual_root != root {
            return Err(Error::new(
                ErrorCode::Protocol,
                "terminal root key root union contributor disagrees with root UUID",
            ));
        }
        union_arms.push((0, label));
    }
    while cursor < encoded.len() {
        let discriminator = if encoded[cursor] == 6 {
            Some(ordered_string_at(encoded, &mut cursor).ok_or_else(|| {
                Error::new(
                    ErrorCode::Protocol,
                    "terminal root key contains an invalid union discriminator",
                )
            })?)
        } else {
            None
        };
        let joined_id = uuid_at(encoded, &mut cursor).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "terminal root key contains an unsupported component",
            )
        })?;
        if let Some(discriminator) = discriminator {
            union_arms.push((joined.len() + 1, discriminator));
        }
        joined.push(joined_id);
    }

    if union_arms.is_empty() {
        Ok(OutputOccurrenceId::new(root, joined))
    } else {
        OutputOccurrenceId::with_union_arms(root, joined, union_arms).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "terminal root key contains invalid union discriminators",
            )
        })
    }
}

fn snapshot_root_occurrences(
    snapshot: &RelationSnapshot,
    snapshot_index: &RelationSnapshotIndex,
) -> Result<Vec<OutputOccurrenceId>, Error> {
    let mut occurrences = vec![None; snapshot.root_count];
    for (occurrence, position) in &snapshot_index.roots {
        let slot = occurrences.get_mut(*position).ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "maintained root occurrence index exceeds snapshot roots",
            )
        })?;
        *slot = Some(occurrence.clone());
    }
    occurrences
        .into_iter()
        .collect::<Option<Vec<_>>>()
        .ok_or_else(|| {
            Error::new(
                ErrorCode::Protocol,
                "maintained snapshot root is missing an occurrence identity",
            )
        })
}

fn root_occurrence_positions(
    occurrences: &[OutputOccurrenceId],
) -> BTreeMap<OutputOccurrenceId, usize> {
    occurrences
        .iter()
        .cloned()
        .enumerate()
        .map(|(position, occurrence)| (occurrence, position))
        .collect()
}

fn relation_snapshot_index_with_root_occurrences(
    snapshot: &RelationSnapshot,
    occurrences: &[OutputOccurrenceId],
) -> Result<RelationSnapshotIndex, Error> {
    if snapshot.root_count != occurrences.len() {
        return Err(Error::new(
            ErrorCode::Protocol,
            "maintained terminal occurrence sidecar length does not match root rows",
        ));
    }
    let mut index = RelationSnapshotIndex::from_snapshot(snapshot);
    index.roots = root_occurrence_positions(occurrences);
    if index.roots.len() != occurrences.len() {
        return Err(Error::new(
            ErrorCode::Protocol,
            "maintained terminal occurrence sidecar contains duplicate identity",
        ));
    }
    Ok(index)
}

/// Flat tuple identity is carried by the maintained terminal sidecar, not the
/// public row: an external reset may omit hidden joined-row fields. Preserve
/// that sidecar whenever its materialized snapshot is the replacement being
/// installed; ordinary link-only snapshots retain row-derived indexing.
fn maintained_snapshot_index_or_row_index<S>(
    node: &mut NodeState<S>,
    kind: &SubscriptionKind,
    snapshot: &RelationSnapshot,
) -> Result<RelationSnapshotIndex, Error>
where
    S: OrderedKvStorage,
{
    let SubscriptionKind::Prepared {
        shape,
        maintained_subscription: Some(maintained),
        ..
    } = kind
    else {
        return Ok(RelationSnapshotIndex::from_snapshot(snapshot));
    };
    if shape.query().flat_join.is_none() {
        return Ok(RelationSnapshotIndex::from_snapshot(snapshot));
    }
    let materialized = crate::db::block_on(
        node.materialize_local_maintained_relation_snapshot_with_occurrences(maintained),
    )?;
    if materialized.snapshot.root_count == snapshot.root_count {
        return relation_snapshot_index_with_root_occurrences(
            snapshot,
            &materialized.root_occurrence_ids,
        );
    }
    Ok(RelationSnapshotIndex::from_snapshot(snapshot))
}

fn relation_snapshot_with_delta_slack(snapshot: &RelationSnapshot) -> RelationSnapshot {
    let mut snapshot = snapshot.clone();
    reserve_relation_snapshot_delta_slack(&mut snapshot);
    snapshot
}

fn reserve_relation_snapshot_delta_slack(snapshot: &mut RelationSnapshot) {
    fn slack(len: usize) -> usize {
        (len / 8).max(64)
    }

    snapshot.rows.reserve(slack(snapshot.rows.len()));
    snapshot.edges.reserve(slack(snapshot.edges.len()));
}

fn subscription_is_settled<S>(
    node: &NodeState<S>,
    active_authority_view_receipts: &ActiveAuthorityViewReceipts,
    shape: &ValidatedQuery,
    binding: &Binding,
    tier: DurabilityTier,
    read_view: ReadViewSpec,
    propagate_upstream: bool,
    requires_authority_receipt: bool,
    authority_result_key: Option<&crate::protocol::AuthorityResultKey>,
) -> bool
where
    S: OrderedKvStorage,
{
    if tier <= DurabilityTier::Local {
        return true;
    }
    let binding_view_key = BindingViewKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions {
            tier,
            read_view,
            propagate_upstream,
            ..RegisterShapeOptions::default()
        }
        .read_view_key(),
    };
    // Callers without a registered usage-site key are direct/unscoped paths;
    // they may inspect only that explicit unscoped receipt. They must never
    // select a unique scoped receipt by binding view.
    let fallback_unscoped = crate::protocol::AuthorityResultKey::unscoped(binding_view_key);
    let authority_result_key = authority_result_key.unwrap_or(&fallback_unscoped);
    node.has_settled_authority_result(authority_result_key)
        && !node.opening_pending_for_authority_result(authority_result_key)
        && (!requires_authority_receipt
            || active_authority_view_receipts
                .borrow()
                .as_ref()
                .is_some_and(|receipts| receipts.binding_views.contains(&binding_view_key)))
}

pub(crate) fn subscription_row_occurrence_id(row: &CurrentRow) -> OutputOccurrenceId {
    let root = ObjectId::from_uuid(row.row_uuid().0);
    let mut joined = Vec::new();
    for position in 1.. {
        let Some(Value::Uuid(row_id)) = row.raw_field(&format!("__flat_join_row_{position}"))
        else {
            break;
        };
        joined.push(ObjectId::from_uuid(row_id));
    }
    OutputOccurrenceId::new(root, joined)
}

fn subscription_outputs_with_occurrence_sidecar(
    snapshot: &RelationSnapshot,
    occurrence_ids: &[OutputOccurrenceId],
) -> Result<Vec<SubscriptionOutputRow>, Error> {
    if occurrence_ids.len() != snapshot.root_count {
        return Err(Error::new(
            ErrorCode::Protocol,
            "maintained root occurrence sidecar length does not match root rows",
        ));
    }
    let mut unique = BTreeSet::new();
    snapshot
        .rows
        .iter()
        .take(snapshot.root_count)
        .zip(occurrence_ids)
        .enumerate()
        .map(|(index, (row, occurrence_id))| {
            let bytes = occurrence_id.canonical_bytes();
            if bytes.get(..16) != Some(row.row_uuid().0.as_bytes()) {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "maintained root occurrence sidecar root does not match row",
                ));
            }
            if !unique.insert(occurrence_id.clone()) {
                return Err(Error::new(
                    ErrorCode::Protocol,
                    "maintained root occurrence sidecar contains duplicate identity",
                ));
            }
            Ok(SubscriptionOutputRow {
                occurrence_id: occurrence_id.clone(),
                row: row.clone(),
                previous_index: None,
                index,
            })
        })
        .collect()
}

fn reset_removed_roots(
    previous: &RelationSnapshot,
    previous_index: &RelationSnapshotIndex,
    current_occurrences: &[OutputOccurrenceId],
) -> Vec<RemovedRow> {
    let current = current_occurrences.iter().collect::<BTreeSet<_>>();
    previous_index
        .roots
        .iter()
        .filter(|(occurrence, _)| !current.contains(occurrence))
        .map(|(occurrence_id, position)| {
            let row = &previous.rows[*position];
            RemovedRow {
                table: row.table().to_owned(),
                row_uuid: row.row_uuid(),
                occurrence_id: occurrence_id.clone(),
                index: *position,
            }
        })
        .collect()
}

fn subscription_output_row(
    row: CurrentRow,
    previous_index: Option<usize>,
    index: usize,
) -> SubscriptionOutputRow {
    SubscriptionOutputRow {
        occurrence_id: subscription_row_occurrence_id(&row),
        row,
        previous_index,
        index,
    }
}

fn subscription_row_key(row: &CurrentRow) -> OutputOccurrenceId {
    subscription_row_occurrence_id(row)
}

#[cfg(test)]
mod tests;
