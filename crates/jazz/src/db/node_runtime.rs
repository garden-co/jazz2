//! Node-owned orchestration for upstream and subscriber connections.
//!
//! The node runtime owns shared connection state, scheduling, pending uploads,
//! subscription refresh, write-state notification, and connection lifecycle.

use super::peer_connection::{
    ConnectionLink, PeerConnection, SubscriberConnectionState, UpstreamConnectionState,
    coverage_group_subscription_key, mutation_error_event, take_pending_mutation_error_delivery,
};
use super::*;

type PeerOwnerGuards<'a, S> = BTreeMap<usize, futures::lock::MutexGuard<'a, PeerConnection<S>>>;
use crate::time::TxTime;

/// Test-only rendezvous after refresh has detached a public stream's local
/// maintained subscription. It makes the cancellation/finalization handoff
/// deterministic without changing production scheduling.
#[cfg(test)]
#[derive(Clone)]
pub(super) struct SubscriptionRefreshDetachPause {
    token: Rc<()>,
    entered: Rc<Cell<bool>>,
    released: Rc<Cell<bool>>,
    waker: Rc<RefCell<Option<Waker>>>,
}

#[cfg(test)]
impl Drop for SubscriptionRefreshDetachPause {
    fn drop(&mut self) {
        let removed = SUBSCRIPTION_REFRESH_DETACH_PAUSE.with(|slot| {
            if slot
                .borrow()
                .as_ref()
                .is_some_and(|current| Rc::ptr_eq(&current.token, &self.token))
            {
                slot.borrow_mut().take()
            } else {
                None
            }
        });
        drop(removed);
    }
}

#[cfg(test)]
impl SubscriptionRefreshDetachPause {
    pub(super) fn entered(&self) -> bool {
        self.entered.get()
    }

    pub(super) fn release(&self) {
        self.released.set(true);
        if let Some(waker) = self.waker.borrow_mut().take() {
            waker.wake();
        }
    }
}

#[cfg(test)]
thread_local! {
    static SUBSCRIPTION_REFRESH_DETACH_PAUSE: RefCell<Option<SubscriptionRefreshDetachPause>> = const { RefCell::new(None) };
}

#[cfg(test)]
pub(super) fn pause_subscription_refresh_after_detach_for_test() -> SubscriptionRefreshDetachPause {
    let pause = SubscriptionRefreshDetachPause {
        token: Rc::new(()),
        entered: Rc::new(Cell::new(false)),
        released: Rc::new(Cell::new(false)),
        waker: Rc::new(RefCell::new(None)),
    };
    SUBSCRIPTION_REFRESH_DETACH_PAUSE.with(|slot| *slot.borrow_mut() = Some(pause.clone()));
    pause
}

#[cfg(test)]
async fn wait_for_subscription_refresh_detach_for_test() {
    let pause = SUBSCRIPTION_REFRESH_DETACH_PAUSE.with(|slot| slot.borrow().clone());
    let Some(pause) = pause else {
        return;
    };
    pause.entered.set(true);
    std::future::poll_fn(|context| {
        if pause.released.get() {
            Poll::Ready(())
        } else {
            *pause.waker.borrow_mut() = Some(context.waker().clone());
            Poll::Pending
        }
    })
    .await;
    let removed = SUBSCRIPTION_REFRESH_DETACH_PAUSE.with(|slot| slot.borrow_mut().take());
    drop(removed);
}

/// Retain a FIFO owner operation while `Db::close` polls it. Dropping the
/// close future drops the lease, rather than the accepted operation.
struct QueuedMutationLease<'a> {
    queue: &'a RefCell<VecDeque<QueuedMutationOperation>>,
    active_leases: &'a Cell<usize>,
    operation: Option<QueuedMutationOperation>,
}

impl<'a> QueuedMutationLease<'a> {
    fn new(
        queue: &'a RefCell<VecDeque<QueuedMutationOperation>>,
        active_leases: &'a Cell<usize>,
        operation: QueuedMutationOperation,
    ) -> Self {
        active_leases.set(
            active_leases
                .get()
                .checked_add(1)
                .expect("queued mutation lease count overflow"),
        );
        Self {
            queue,
            active_leases,
            operation: Some(operation),
        }
    }

    fn operation(&self) -> &QueuedMutationOperation {
        self.operation
            .as_ref()
            .expect("queued mutation lease must retain its operation")
    }

    fn operation_mut(&mut self) -> &mut QueuedMutationOperation {
        self.operation
            .as_mut()
            .expect("queued mutation lease must retain its operation")
    }

    fn take(&mut self) -> QueuedMutationOperation {
        self.operation
            .take()
            .expect("queued mutation lease must retain its operation")
    }
}

impl Drop for QueuedMutationLease<'_> {
    fn drop(&mut self) {
        if let Some(operation) = self.operation.take() {
            self.queue.borrow_mut().push_front(operation);
        }
        self.active_leases.set(
            self.active_leases
                .get()
                .checked_sub(1)
                .expect("queued mutation lease count underflow"),
        );
    }
}

/// Node-owned participant surface for upstream and subscriber connections.
pub struct Node<S>
where
    S: OrderedKvStorage,
{
    pub(super) node: SharedNodeState<S>,
    mutation_owner_lifecycle: Cell<MutationOwnerLifecycle>,
    close_owner: futures::lock::Mutex<()>,
    tx_time_reservation_clock: Rc<Cell<TxTime>>,
    node_uuid: NodeUuid,
    receives_commits_as_local: bool,
    pub(super) subscriptions: SubscriptionList,
    pub(super) outbox: Outbox,
    pub(super) pending_local_publications: PendingLocalPublications,
    queued_mutations: RefCell<VecDeque<QueuedMutationOperation>>,
    /// Operations temporarily popped by an owner future. They remain part of
    /// FIFO shutdown work even while a cold await leaves the queue empty.
    queued_mutation_active_leases: Cell<usize>,
    transaction_wait_observers: RefCell<Vec<TransactionWaitObserver>>,
    queued_mutation_failures: RefCell<BTreeMap<TxId, Error>>,
    /// Rejections claimed by a waiter during the current owner turn. They
    /// stay observable until every waiter that was woken by the same fate has
    /// had one polling opportunity, then are discarded before the turn ends.
    deferred_rejection_discards: RefCell<BTreeSet<TxId>>,
    queued_open_transaction_failures: RefCell<BTreeMap<OpenTransactionId, Error>>,
    reserved_mutations: RefCell<BTreeSet<TxId>>,
    pub(super) pending_transaction_abandonments: TransactionAbandonmentTombstones,
    transaction_abandonments_closed: Cell<bool>,
    transaction_abandonment_shutdown_pending: Cell<bool>,
    pub(super) upstream_subscriptions: PendingUpstreamCommands,
    pub(super) pending_subscription_finalizations: PendingSubscriptionFinalizations,
    subscription_finalizations_closed: Cell<bool>,
    subscription_runtime_retired: Cell<bool>,
    pub(super) latest_coverage_subscriptions: LatestCoverageSubscriptions,
    pub(super) upstream_coverage_refcounts: UpstreamCoverageRefCounts,
    pub(super) awaiting_initial_authority_coverage: AwaitingInitialAuthorityCoverage,
    pub(super) active_authority_view_receipts: ActiveAuthorityViewReceipts,
    pub(super) coverage_refresh_generations: CoverageRefreshGenerations,
    pub(super) query_coverage_registrations: QueryCoverageRegistrations,
    pub(super) upstream_subscription_owners: UpstreamSubscriptionOwners,
    pub(super) relay_upstream_subscription_owners: RelayUpstreamSubscriptionOwners,
    pub(super) pending_relay_subscription_rejections: PendingRelaySubscriptionRejections,
    pub(super) connections: RefCell<Vec<Rc<LocalMutex<PeerConnection<S>>>>>,
    pub(super) scheduler: SharedTickScheduler,
    pub(super) upload_retry_clock: SharedUploadRetryClock,
    pub(super) detached_large_value_uploads:
        Rc<RefCell<BTreeMap<UpstreamUploadDestination, peer_connection::LargeValueUploadQueues>>>,
    pub(super) large_value_upload_retry_deadlines: Rc<RefCell<BTreeMap<TxId, u64>>>,
    pub(super) write_state_waiters: WriteStateWaiters,
    pub(super) permission_advice_waiters: PermissionAdviceWaiters,
    pub(super) current_rows: row_availability::SharedCurrentRows,
    pub(super) edge_fate_routes: EdgeFateRoutes,
    pub(super) local_fate_routes: LocalFateRoutes,
    pub(super) admitted_upstream_authorities: AdmittedUpstreamAuthorities,
    pub(super) admitted_upstream_authority: Rc<RefCell<Option<AuthorityContext>>>,
    pub(super) mutation_errors: SharedMutationErrors,
    /// Transaction IDs restored from durable browser-relay storage after a
    /// worker restart. These IDs were authored by an ephemeral foreground
    /// node, so their rejection payload must never become worker-owned
    /// durable state. The set exists only until its terminal authority fate is
    /// observed in this runtime.
    pub(super) browser_relay_recovered_tx_ids: Rc<RefCell<BTreeSet<TxId>>>,
    pub(super) next_write_state_waiter_id: Cell<u64>,
    pub(super) next_subscription_nonce: Cell<u64>,
    pub(super) subscriber_dirty_epoch: Rc<Cell<u64>>,
    pub(super) edge_cache_budget: Cell<Option<EdgeCacheBudget>>,
    pub(super) upstream_durability_floor: Cell<DurabilityTier>,
    pub(super) defer_local_persistence: Cell<bool>,
    pub(super) chunk_resolver: PeerChunkResolver,
    pub(super) local_chunk_reader: groove::chunks::LocalChunkReader,
    pub(super) observed_chunk_completion_generation: Cell<u64>,
    local_availability_dirty: Cell<bool>,
}

impl<S> Node<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Test-only staging through the ordinary upstream inbound queue.
    ///
    /// A connection epoch is opaque to test callers and must identify exactly
    /// one still-attached upstream. Refusing ambiguity makes a test fixture
    /// state its intended authority link instead of accidentally exercising a
    /// different parallel upstream.
    #[cfg(feature = "testing")]
    pub async fn stage_upstream_message_for_test(
        &self,
        connection_epoch: u64,
        message: SyncMessage,
    ) -> Result<bool, String> {
        let connections = self.connections.borrow().clone();
        let mut candidates = Vec::new();
        for candidate in connections {
            let connection = candidate.lock().await;
            let selected = connection.connection_epoch == connection_epoch
                && matches!(connection.link, ConnectionLink::Upstream(_));
            drop(connection);
            if selected {
                candidates.push(candidate);
            }
        }
        let [connection] = candidates.as_slice() else {
            return Err(format!(
                "test upstream handle selected {} live upstream connections for epoch {connection_epoch}",
                candidates.len()
            ));
        };
        // A test frame has the same receipt eligibility as a physical arrival
        // on this link. In particular, a still-attached predecessor can be
        // useful for exercising stale application, but must not manufacture
        // durable authority disclosure after a successor replaced it.
        let authority_receipt_eligible = self
            .active_authority_view_receipts
            .borrow()
            .as_ref()
            .is_some_and(|receipts| receipts.connection_epoch == connection_epoch);
        let mut connection = connection.lock().await;
        connection.staged_inbound.push_back(StagedInboundMessage {
            message,
            authority_receipt_eligible,
        });
        self.schedule_tick(TickUrgency::Immediate);
        Ok(authority_receipt_eligible)
    }

    /// Wrap a node for serving subscriber links.
    pub fn new(mut node: NodeState<S>) -> Self {
        // History completeness is a structural property of the opened node,
        // not evaluator state. Cache it so connection attachment never needs
        // to synchronously borrow storage-owning state during evaluation.
        let receives_commits_as_local = !node.is_history_complete();
        let chunk_resolver = PeerChunkResolver::default();
        let local_chunk_reader = node.local_chunk_reader_handle();
        let tx_time_reservation_clock = node.tx_time_reservation_clock();
        let node_uuid = node.node_uuid();
        node.set_missing_chunk_resolver(Rc::new(chunk_resolver.clone()));
        let pending_mutation_errors = node
            .rejected_transactions()
            .into_iter()
            .filter_map(|tx_id| {
                node.rejected_transaction(tx_id)
                    .map(|rejected| (tx_id, mutation_error_event(rejected)))
            })
            .collect();
        Self {
            node: Rc::new(futures::lock::Mutex::new(node)),
            mutation_owner_lifecycle: Cell::new(MutationOwnerLifecycle::Open),
            close_owner: futures::lock::Mutex::new(()),
            tx_time_reservation_clock,
            node_uuid,
            receives_commits_as_local,
            subscriptions: Rc::new(RefCell::new(Vec::new())),
            outbox: Rc::new(RefCell::new(UploadOutbox::default())),
            pending_local_publications: Rc::new(RefCell::new(VecDeque::new())),
            queued_mutations: RefCell::new(VecDeque::new()),
            queued_mutation_active_leases: Cell::new(0),
            transaction_wait_observers: RefCell::new(Vec::new()),
            queued_mutation_failures: RefCell::new(BTreeMap::new()),
            deferred_rejection_discards: RefCell::new(BTreeSet::new()),
            queued_open_transaction_failures: RefCell::new(BTreeMap::new()),
            reserved_mutations: RefCell::new(BTreeSet::new()),
            pending_transaction_abandonments: Rc::new(RefCell::new(BTreeSet::new())),
            transaction_abandonments_closed: Cell::new(false),
            transaction_abandonment_shutdown_pending: Cell::new(false),
            upstream_subscriptions: Rc::new(RefCell::new(Vec::new())),
            pending_subscription_finalizations: Rc::new(RefCell::new(VecDeque::new())),
            subscription_finalizations_closed: Cell::new(false),
            subscription_runtime_retired: Cell::new(false),
            latest_coverage_subscriptions: Rc::new(RefCell::new(BTreeMap::new())),
            upstream_coverage_refcounts: Rc::new(RefCell::new(BTreeMap::new())),
            awaiting_initial_authority_coverage: Rc::new(RefCell::new(BTreeSet::new())),
            active_authority_view_receipts: Rc::new(RefCell::new(None)),
            coverage_refresh_generations: Rc::new(RefCell::new(BTreeMap::new())),
            query_coverage_registrations: Rc::new(RefCell::new(BTreeMap::new())),
            upstream_subscription_owners: Rc::new(RefCell::new(BTreeMap::new())),
            relay_upstream_subscription_owners: Rc::new(RefCell::new(BTreeMap::new())),
            pending_relay_subscription_rejections: Rc::new(RefCell::new(BTreeMap::new())),
            connections: RefCell::new(Vec::new()),
            scheduler: Rc::new(RefCell::new(None)),
            upload_retry_clock: Rc::new(RefCell::new(Rc::new(MonotonicUploadRetryClock::new()))),
            detached_large_value_uploads: Rc::new(RefCell::new(BTreeMap::new())),
            large_value_upload_retry_deadlines: Rc::new(RefCell::new(BTreeMap::new())),
            write_state_waiters: Rc::new(RefCell::new(BTreeMap::new())),
            mutation_errors: Rc::new(RefCell::new(MutationErrorState {
                callback: None,
                pending: pending_mutation_errors,
            })),
            browser_relay_recovered_tx_ids: Rc::new(RefCell::new(BTreeSet::new())),
            next_write_state_waiter_id: Cell::new(1),
            next_subscription_nonce: Cell::new(1),
            permission_advice_waiters: Rc::new(RefCell::new(BTreeMap::new())),
            current_rows: Rc::new(RefCell::new(row_availability::CurrentRowsRouter::default())),
            edge_fate_routes: Rc::new(RefCell::new(BTreeMap::new())),
            local_fate_routes: Rc::new(RefCell::new(BTreeMap::new())),
            admitted_upstream_authorities: Rc::new(RefCell::new(Vec::new())),
            admitted_upstream_authority: Rc::new(RefCell::new(None)),
            subscriber_dirty_epoch: Rc::new(Cell::new(0)),
            edge_cache_budget: Cell::new(None),
            upstream_durability_floor: Cell::new(DurabilityTier::Global),
            defer_local_persistence: Cell::new(false),
            chunk_resolver,
            local_chunk_reader,
            observed_chunk_completion_generation: Cell::new(0),
            local_availability_dirty: Cell::new(false),
        }
    }

    /// Reserve a definitive local transaction identity without borrowing the
    /// async storage-owning node. The shared high-water mirror is advanced by
    /// every ordinary mint and remote observation as well.
    pub(super) fn reserve_transaction_id(&self, now_ms: u64) -> Result<TxId, Error> {
        self.ensure_mutation_admission_open()?;
        let made_at = TxTime::tick(self.tx_time_reservation_clock.get(), now_ms)
            .map_err(crate::node::Error::from)
            .map_err(Error::from)?;
        self.tx_time_reservation_clock.set(made_at);
        Ok(TxId::new(made_at, self.node_uuid))
    }

    pub(super) fn begin_mutation_shutdown(&self) {
        self.mutation_owner_lifecycle
            .set(MutationOwnerLifecycle::Closing);
        // Wait observers may be parked on a state-change channel even when
        // there is no queued mutation left to wake them. Closing is itself a
        // terminal observation boundary, so wake every waiter to let it
        // either observe its requested tier or report that the runtime closed
        // first.
        let waiters = std::mem::take(&mut *self.write_state_waiters.borrow_mut());
        for (_, waiters) in waiters {
            for waiter in waiters {
                let WriteStateWaiterNotify::Future(sender) = waiter.notify;
                let _ = sender.send(());
            }
        }
    }

    /// Serialize shutdown drains while keeping admission closure outside the
    /// await. Cancellation releases ownership so another close caller can
    /// resume the same idempotent drain and finalization sequence.
    pub(super) async fn lock_close_owner(&self) -> futures::lock::MutexGuard<'_, ()> {
        self.close_owner.lock().await
    }

    pub(super) fn ensure_mutation_admission_open(&self) -> Result<(), Error> {
        if self.mutation_owner_lifecycle.get() == MutationOwnerLifecycle::Open {
            Ok(())
        } else {
            Err(Error::new(
                ErrorCode::WriteRejected,
                "database mutation owner is closing",
            ))
        }
    }

    pub(super) fn ensure_subscription_finalization_open(&self) -> Result<(), Error> {
        if self.subscription_finalizations_closed.get() {
            return Err(Error::new(
                ErrorCode::Protocol,
                "database subscription admission is closed",
            ));
        }
        Ok(())
    }

    pub(super) fn enqueue_mutation(
        &self,
        tx_id: TxId,
        future: QueuedMutationFuture,
    ) -> Rc<RefCell<QueuedMutationStatus>> {
        let status = Rc::new(RefCell::new(QueuedMutationStatus::Pending));
        self.reserved_mutations.borrow_mut().insert(tx_id);
        self.queued_mutations
            .borrow_mut()
            .push_back(QueuedMutationOperation {
                tx_id: Some(tx_id),
                open_tx_id: None,
                future,
                status: Some(Rc::clone(&status)),
                completion: None,
            });
        self.schedule_tick(TickUrgency::Immediate);
        status
    }

    pub(super) fn enqueue_transaction_operation(
        &self,
        open_tx_id: OpenTransactionId,
        future: QueuedMutationFuture,
    ) -> Result<(), Error> {
        self.ensure_mutation_admission_open()?;
        self.queued_mutations
            .borrow_mut()
            .push_back(QueuedMutationOperation {
                tx_id: None,
                open_tx_id: Some(open_tx_id),
                future,
                status: None,
                completion: None,
            });
        self.schedule_tick(TickUrgency::Immediate);
        Ok(())
    }

    /// Put a transaction-local read behind every already-admitted operation
    /// for that transaction. The receiver owns the eventual result, while the
    /// owner queue retains the cold storage future and its wake route.
    pub(super) fn enqueue_transaction_read<T: 'static>(
        &self,
        open_tx_id: OpenTransactionId,
        read: impl Future<Output = Result<T, Error>> + 'static,
    ) -> futures::channel::oneshot::Receiver<Result<T, Error>> {
        self.enqueue_read_after_mutations(Some(open_tx_id), read)
    }

    pub(super) fn queued_mutation_barrier(
        &self,
    ) -> futures::channel::oneshot::Receiver<Result<(), Error>> {
        if self.queued_mutations.borrow().is_empty()
            && self.queued_mutation_active_leases.get() == 0
        {
            let (sender, receiver) = futures::channel::oneshot::channel();
            let _ = sender.send(Ok(()));
            return receiver;
        }
        self.enqueue_read_after_mutations(None, std::future::ready(Ok(())))
    }

    fn enqueue_read_after_mutations<T: 'static>(
        &self,
        open_tx_id: Option<OpenTransactionId>,
        read: impl Future<Output = Result<T, Error>> + 'static,
    ) -> futures::channel::oneshot::Receiver<Result<T, Error>> {
        let (sender, receiver) = futures::channel::oneshot::channel();
        let sender = Rc::new(RefCell::new(Some(sender)));
        let read_sender = Rc::clone(&sender);
        let completion_sender = Rc::clone(&sender);
        self.queued_mutations
            .borrow_mut()
            .push_back(QueuedMutationOperation {
                tx_id: None,
                open_tx_id,
                future: Box::pin(async move {
                    let result = read.await;
                    if let Some(sender) = read_sender.borrow_mut().take() {
                        let _ = sender.send(result);
                    }
                    Ok(())
                }),
                status: None,
                completion: Some(Box::new(move |outcome| {
                    if let Err(error) = outcome
                        && let Some(sender) = completion_sender.borrow_mut().take()
                    {
                        let _ = sender.send(Err(error));
                    }
                })),
            });
        self.schedule_tick(TickUrgency::Immediate);
        receiver
    }

    pub(super) fn enqueue_transaction_commit(
        &self,
        open_tx_id: OpenTransactionId,
        tx_id: TxId,
        future: QueuedMutationFuture,
    ) -> Rc<RefCell<QueuedMutationStatus>> {
        let status = Rc::new(RefCell::new(QueuedMutationStatus::Pending));
        self.reserved_mutations.borrow_mut().insert(tx_id);
        self.queued_mutations
            .borrow_mut()
            .push_back(QueuedMutationOperation {
                tx_id: Some(tx_id),
                open_tx_id: Some(open_tx_id),
                future,
                status: Some(Rc::clone(&status)),
                completion: None,
            });
        self.schedule_tick(TickUrgency::Immediate);
        status
    }

    pub(super) fn enqueue_transaction_cleanup(&self, future: QueuedMutationFuture) {
        self.queued_mutations
            .borrow_mut()
            .push_back(QueuedMutationOperation {
                tx_id: None,
                open_tx_id: None,
                future,
                status: None,
                completion: None,
            });
        self.schedule_tick(TickUrgency::Immediate);
    }

    /// Whether owner-free queue waiting can coexist with semantic maintenance.
    pub(super) fn owner_is_available(&self) -> bool {
        // Drop the probe guard before maintenance can acquire the owner.
        self.node.try_lock().is_some()
    }

    pub(super) fn queued_transaction_error(&self, id: OpenTransactionId) -> Option<Error> {
        self.queued_open_transaction_failures
            .borrow()
            .get(&id)
            .cloned()
    }

    pub(super) fn retire_queued_transaction_error(&self, id: OpenTransactionId) {
        self.queued_open_transaction_failures
            .borrow_mut()
            .remove(&id);
    }

    /// Poll one FIFO owner-queue entry, retaining a pending continuation.
    pub(super) fn poll_queued_mutation_once(&self) -> bool {
        use std::task::{Context, Poll, Waker};

        let Some(mut operation) = self.queued_mutations.borrow_mut().pop_front() else {
            return false;
        };
        let owned_waker = self.query_runtime_waker();
        let waker = owned_waker.as_ref().unwrap_or_else(|| Waker::noop());
        let mut context = Context::from_waker(waker);
        let poisoned = operation.open_tx_id.and_then(|open_tx_id| {
            self.queued_open_transaction_failures
                .borrow()
                .get(&open_tx_id)
                .cloned()
        });
        let outcome = match poisoned {
            Some(error) => Poll::Ready(Err(error)),
            None => operation.future.as_mut().poll(&mut context),
        };
        match outcome {
            Poll::Pending => {
                self.queued_mutations.borrow_mut().push_front(operation);
                true
            }
            Poll::Ready(result) => {
                self.finish_queued_mutation(operation, result);
                false
            }
        }
    }

    fn finish_queued_mutation(
        &self,
        mut operation: QueuedMutationOperation,
        result: Result<(), Error>,
    ) {
        if let Some(completion) = operation.completion.take() {
            completion(result.clone());
        }
        if let Some(tx_id) = operation.tx_id {
            self.reserved_mutations.borrow_mut().remove(&tx_id);
        }
        if let Err(error) = &result
            && let Some(open_tx_id) = operation.open_tx_id
        {
            self.queued_open_transaction_failures
                .borrow_mut()
                .entry(open_tx_id)
                .or_insert_with(|| error.clone());
        }
        if let (Some(tx_id), Some(status)) = (operation.tx_id, operation.status) {
            let terminal_failed = result.is_err();
            match result {
                Ok(()) => *status.borrow_mut() = QueuedMutationStatus::Published,
                Err(error) => {
                    *status.borrow_mut() = QueuedMutationStatus::Failed(error.clone());
                    self.queued_mutation_failures
                        .borrow_mut()
                        .insert(tx_id, error);
                }
            }
            if let Some(waiters) = self.write_state_waiters.borrow_mut().remove(&tx_id) {
                for waiter in waiters {
                    let WriteStateWaiterNotify::Future(sender) = waiter.notify;
                    let _ = sender.send(());
                }
            }
            if terminal_failed && let Some(open_tx_id) = operation.open_tx_id {
                let node = Rc::clone(&self.node);
                self.enqueue_transaction_cleanup(Box::pin(async move {
                    let mut node = node.lock().await;
                    match node.abandon_tx(open_tx_id) {
                        Ok(()) | Err(crate::node::Error::MissingOpenBatch(_)) => Ok(()),
                        Err(error) => Err(error.into()),
                    }
                }));
            }
        }
        if operation.tx_id.is_some()
            && let Some(open_tx_id) = operation.open_tx_id
        {
            self.queued_open_transaction_failures
                .borrow_mut()
                .remove(&open_tx_id);
        }
        if !self.queued_mutations.borrow().is_empty() {
            self.schedule_tick(TickUrgency::Immediate);
        }
    }

    pub(super) async fn drain_queued_mutations(&self) {
        loop {
            let Some(operation) = self.queued_mutations.borrow_mut().pop_front() else {
                return;
            };
            let mut lease = QueuedMutationLease::new(
                &self.queued_mutations,
                &self.queued_mutation_active_leases,
                operation,
            );
            let poisoned = lease.operation().open_tx_id.and_then(|open_tx_id| {
                self.queued_open_transaction_failures
                    .borrow()
                    .get(&open_tx_id)
                    .cloned()
            });
            let result = match poisoned {
                Some(error) => Err(error),
                None => lease.operation_mut().future.as_mut().await,
            };
            self.finish_queued_mutation(lease.take(), result);
        }
    }

    pub(super) fn upstream_register_shape_options(
        &self,
        tier: DurabilityTier,
        read_view: ReadViewSpec,
    ) -> RegisterShapeOptions {
        upstream_register_shape_options(tier, read_view, self.upstream_durability_floor.get())
    }

    /// Ordinary `Db::open` nodes are Local receivers. Only the structurally
    /// separate history-complete path acts as the Core fate authority.
    fn receives_commits_as_local(&self) -> bool {
        self.receives_commits_as_local
    }

    /// Borrow the served node.
    pub fn node(&self) -> SharedNodeState<S> {
        Rc::clone(&self.node)
    }

    /// Test-only inspection of retry ownership. Rejected foreign transactions
    /// must never acquire an originating node's retained payload.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn has_retained_rejection_for_test(&self, tx_id: TxId) -> bool {
        self.node.borrow().rejected_transaction(tx_id).is_some()
    }

    /// Test-only inspection of the process-local browser-relay recovery
    /// marker. It is not durable retry ownership: every terminal fate must
    /// consume it, including an accepted Global fate.
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn has_recovered_browser_relay_tx_for_test(&self, tx_id: TxId) -> bool {
        self.browser_relay_recovered_tx_ids
            .borrow()
            .contains(&tx_id)
    }

    #[cfg(feature = "runtime")]
    pub(crate) fn enable_authoritative_scalar_exit_refresh(&self) {
        self.node
            .borrow_mut()
            .enable_authoritative_scalar_exit_refresh();
    }

    /// Configure Jazz-owned ingress and expiry policy for unpublished large
    /// values. Groove persists timestamps and performs eviction, but does not
    /// choose these product limits.
    pub fn set_large_value_staging_policy(&self, policy: crate::node::LargeValueStagingPolicy) {
        self.node
            .borrow_mut()
            .set_large_value_staging_policy(policy);
    }

    /// Run one host-driven staging-expiry maintenance pass.
    ///
    /// Browser, NAPI, and server hosts call this from their own timer cadence;
    /// it is idempotent and does not make Groove own an executor or clock.
    pub async fn evict_expired_staged_large_values(&self) -> Result<usize, Error> {
        self.node
            .borrow()
            .evict_expired_staged_large_values()
            .await
            .map_err(Into::into)
    }

    pub(super) fn set_non_durable_client(&self) {
        self.node.borrow_mut().set_non_durable_client();
        self.upstream_durability_floor.set(DurabilityTier::Local);
    }

    pub(super) fn configure_scope_isolated_client_relay(
        &self,
        scope: crate::db::ClientRelayScope,
    ) -> Result<(), Error> {
        Ok(self
            .node
            .borrow_mut()
            .configure_scope_isolated_client_relay(scope)?)
    }

    pub(super) fn set_deferred_local_persistence(&self, deferred: bool) {
        self.defer_local_persistence.set(deferred);
    }

    /// Change whether subscriber links may serve their registered views.
    /// Publishing a permissions head always rehydrates every live view, so a
    /// tighter head retracts rows without requiring a reconnect.
    pub fn set_permissions_ready(&self, ready: bool) -> Result<(), Error> {
        self.node.borrow_mut().set_permissions_ready(ready);
        if ready {
            for connection in self.connections.borrow().iter() {
                crate::db::block_on(connection.borrow_mut().rehydrate_subscriber_views())?;
            }
        }
        Ok(())
    }

    pub(super) fn queue_pending_upload(&self, tx_id: TxId, unit: Option<SyncMessage>) {
        if queue_pending_upload_in(&self.outbox, tx_id, unit) {
            self.mark_subscriber_connections_dirty();
            self.schedule_tick(TickUrgency::Deferred);
        }
    }

    pub(super) fn queue_local_publication(
        &self,
        published: PublishedTransaction,
        upload_unit: Option<SyncMessage>,
    ) {
        let published = Rc::new(published);
        let settlement_publication = Rc::clone(&published);
        let settlement_node = Rc::clone(&self.node);
        let settlement = Box::pin(async move {
            let tx_id = settlement_publication.tx_id();
            let persistence = settlement_publication.persist().await;
            settlement_node
                .lock()
                .await
                .settle_published_transaction(tx_id, persistence)?;
            Ok(tx_id)
        });
        self.pending_local_publications
            .borrow_mut()
            .push_back(PendingLocalPublication {
                published,
                upload_unit,
                settlement,
            });
        self.schedule_tick(TickUrgency::Immediate);
    }

    /// Whether a resident publication still needs its ordered durability turn.
    /// This check is deliberately synchronous so a host's one bounded tick
    /// does not spend its sole poll acquiring the publication-settler mutex
    /// when there is no publication to advance.
    pub(super) fn has_pending_local_publications(&self) -> bool {
        !self.pending_local_publications.borrow().is_empty()
    }

    fn poll_local_publication_settlement(
        &self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Error>> {
        use std::task::Poll;

        loop {
            let Some(mut pending) = self.pending_local_publications.borrow_mut().pop_front() else {
                return Poll::Ready(Ok(()));
            };
            match pending.settlement.as_mut().poll(cx) {
                Poll::Pending => {
                    self.pending_local_publications
                        .borrow_mut()
                        .push_front(pending);
                    return Poll::Pending;
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Ready(Ok(tx_id)) => {
                    debug_assert_eq!(pending.published.tx_id(), tx_id);
                    self.queue_pending_upload(tx_id, pending.upload_unit);
                }
            }
        }
    }

    /// Advance ordered local durability without keeping the host tick pending.
    /// A retained mutation may own the node lock needed to finish settlement;
    /// the tick must still poll that mutation. The runtime-owned waker requests
    /// another owner turn when this retained settlement can make progress.
    pub(super) fn poll_local_publication_settlement_once(&self) -> Result<bool, Error> {
        use std::task::{Context, Poll, Waker};

        let owned_waker = self.query_runtime_waker();
        let waker = owned_waker.as_ref().unwrap_or_else(|| Waker::noop());
        let mut context = Context::from_waker(waker);
        match self.poll_local_publication_settlement(&mut context) {
            Poll::Pending => Ok(true),
            Poll::Ready(result) => result.map(|()| false),
        }
    }

    pub(super) async fn settle_local_publications(&self) -> Result<(), Error> {
        futures::future::poll_fn(|cx| self.poll_local_publication_settlement(cx)).await
    }

    /// Restore locally originated, unsettled durable writes into the
    /// process-local upload queue after reopening client storage.
    pub(super) fn restore_pending_uploads(&self, identity: DbIdentity) -> Result<(), Error> {
        let mut node = self.node.borrow_mut();
        let pending = node.pending_transaction_ids_for(identity.node, identity.author);
        let pending = crate::db::block_on(pending)?;
        drop(node);
        let mut restored = HashSet::new();
        for tx_id in pending {
            if restored.insert(tx_id) {
                self.queue_pending_upload(tx_id, None);
            }
        }
        Ok(())
    }

    pub(super) async fn restore_backend_pending_uploads(
        &self,
        node_id: NodeUuid,
    ) -> Result<(), Error> {
        let pending = self
            .node
            .lock()
            .await
            .synchronizing_transaction_ids_for_node(node_id)
            .await?;
        for tx_id in pending {
            self.queue_pending_upload(tx_id, None);
        }
        Ok(())
    }

    pub(super) fn restore_browser_relay_pending_uploads(
        &self,
        author: AuthorSubject,
    ) -> Result<(), Error> {
        let mut node = self.node.borrow_mut();
        let pending = node.pending_transaction_ids_for_author(author);
        let pending = crate::db::block_on(pending)?;
        drop(node);
        self.browser_relay_recovered_tx_ids
            .borrow_mut()
            .extend(pending.iter().copied());
        for tx_id in pending {
            self.queue_pending_upload(tx_id, None);
        }
        Ok(())
    }

    /// Restore complete accepted authority publications, without waiting for
    /// the originating clients to reconnect. Only an edge host calls this.
    #[cfg(any(test, feature = "runtime"))]
    pub(super) async fn restore_edge_authority_uploads(&self) -> Result<(), Error> {
        let mut node = self.node.lock().await;
        let pending = node.pending_edge_authority_transaction_ids().await?;
        let mut covered = BTreeSet::new();
        // Newer frontier publications already include their unsettled parents.
        // Do not reconstruct one growing ancestry prefix per historical edit.
        for tx_id in pending.into_iter().rev() {
            if covered.contains(&tx_id) {
                continue;
            }
            let publication = node.edge_authority_publication_for(tx_id).await?;
            covered.extend(publication.commits.iter().map(|unit| unit.tx.tx_id));
            self.queue_pending_upload(tx_id, Some(SyncMessage::AuthorityPublication(publication)));
        }
        Ok(())
    }

    async fn restore_local_subscriber(
        &self,
        author: AuthorSubject,
        downstream_fates: &PendingDownstreamFates,
    ) -> Result<(), Error> {
        let mut node = self.node.lock().await;
        let pending = node.pending_transaction_ids_for_author(author).await?;
        let pending_set = pending.iter().copied().collect::<BTreeSet<_>>();
        let mut replay_units = Vec::new();
        let mut visited = BTreeSet::new();
        for tx_id in &pending {
            collect_local_replay_commit_units(&mut node, *tx_id, &mut visited, &mut replay_units)
                .await?;
        }
        drop(node);
        for (tx_id, unit) in replay_units {
            // A reopened main-thread runtime has no transaction history. Send
            // accepted causal ancestors before each pending unit so the latter
            // can be ingested before its Local ack or later authority fate.
            downstream_fates.borrow_mut().push(unit.clone());
            if pending_set.contains(&tx_id) {
                // Durable recovery omits exclusive snapshot/read evidence. A
                // live sibling may already retain the exact authored unit;
                // never replace that unit with its redacted history replay.
                let retained_unit = self
                    .outbox
                    .borrow()
                    .iter()
                    .any(|pending| pending.tx_id == tx_id && pending.unit.is_some());
                if !retained_unit {
                    self.queue_pending_upload(tx_id, Some(unit));
                }
            }
        }
        for tx_id in pending {
            register_local_fate_route(&self.local_fate_routes, tx_id, downstream_fates);
        }
        queue_local_acknowledgements(&self.local_fate_routes, &self.node).await;
        Ok(())
    }

    pub(super) fn mark_subscriber_connections_dirty(&self) {
        let next = self.subscriber_dirty_epoch.get().wrapping_add(1);
        self.subscriber_dirty_epoch.set(next);
        for connection in self.connections.borrow().iter() {
            // A suspended peer observes the shared epoch on its next tick.
            // Local publication must not synchronously reenter that owner.
            let Some(mut connection) = connection.try_lock() else {
                continue;
            };
            if let ConnectionLink::Subscriber(SubscriberConnectionState { serve_dirty, .. }) =
                &mut connection.link
            {
                *serve_dirty = true;
                connection.observed_subscriber_dirty_epoch.set(next);
            }
        }
    }

    #[cfg(feature = "testing")]
    /// Test/bench harnesses that mutate the served [`NodeState`] directly must
    /// mark subscriber links dirty. Production writes go through `Db`/sync
    /// boundaries that call this as a boundary effect.
    pub fn mark_subscriber_connections_dirty_for_test(&self) {
        self.mark_subscriber_connections_dirty();
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only encoded storage byte estimate across Jazz physical
    /// classes.
    pub async fn encoded_storage_bytes_for_test(&self) -> Result<u64, Error> {
        Ok(self
            .node
            .lock()
            .await
            .encoded_storage_bytes_for_test()
            .await?)
    }

    #[cfg(feature = "testing")]
    /// Test/bench-only runtime diagnostics used by performance receipts.
    pub async fn runtime_stats_for_test(&self) -> groove::ivm::RuntimeStats {
        self.node.lock().await.runtime_stats_for_test()
    }

    pub(super) fn next_subscription_key(
        &self,
        shape: &ValidatedQuery,
        read_view: crate::protocol::ReadViewKey,
    ) -> SubscriptionKey {
        let nonce = self.next_subscription_nonce.get();
        self.next_subscription_nonce.set(nonce.saturating_add(1));
        SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: crate::query::BindingId(uuid::Uuid::new_v5(
                &crate::query::QUERY_NAMESPACE,
                &nonce.to_be_bytes(),
            )),
            read_view,
        }
    }

    pub(super) fn set_scheduler(&self, scheduler: Option<Rc<dyn TickScheduler>>) {
        *self.scheduler.borrow_mut() = scheduler;
    }

    #[cfg(test)]
    pub(super) fn set_upload_retry_clock_for_test(&self, clock: Rc<dyn UploadRetryClock>) {
        *self.upload_retry_clock.borrow_mut() = clock;
    }

    pub(super) fn set_edge_cache_budget(&self, budget: Option<EdgeCacheBudget>) {
        self.edge_cache_budget.set(budget);
    }

    pub(super) fn schedule_tick(&self, urgency: TickUrgency) {
        schedule_tick_in(&self.scheduler, urgency);
    }

    /// Obtain the host-owned waker that survives this short owner turn.
    ///
    /// It is passed only into Groove's non-blocking query-progress poll. A
    /// later cold-storage completion therefore asks the host for one new tick
    /// instead of making this runtime poll while the storage is still cold.
    pub(super) fn query_runtime_waker(&self) -> Option<Waker> {
        self.scheduler
            .borrow()
            .as_ref()
            .and_then(|scheduler| scheduler.query_runtime_waker())
    }
    /// Lock the node for opening a transaction while the database still owns
    /// transaction admission.
    ///
    /// `owner_operation_admitted` is true only for an operation already
    /// retained by the binding owner before shutdown. Those operations must
    /// drain in FIFO order before the final sweep. Every direct caller checks
    /// the gate both before and after waiting for the node mutex.
    ///
    /// The tombstone check closes the corresponding race with an
    /// already-linearized handle abandonment.
    pub(super) async fn lock_for_transaction_open(
        &self,
        open_tx_id: OpenTransactionId,
        owner_operation_admitted: bool,
    ) -> Result<futures::lock::MutexGuard<'_, NodeState<S>>, Error> {
        if !owner_operation_admitted {
            self.ensure_transaction_admission_open()?;
        }
        let mut node = self.node.lock().await;
        if !owner_operation_admitted && let Err(error) = self.ensure_transaction_admission_open() {
            self.finish_transaction_abandonment_shutdown_in(&mut node)?;
            return Err(error);
        }
        self.reject_tombstoned_transaction_in(&mut node, open_tx_id)?;
        Ok(node)
    }

    /// Lock the node for an operation on an existing transaction.
    ///
    /// A handle drop records its tombstone without waiting for this mutex.
    /// Therefore even an operation already queued ahead of the maintenance tick
    /// observes and retires the tombstone before it can stage, read, or commit.
    /// Binding-owner operations accepted before shutdown may finish draining;
    /// direct callers observe the closed gate after acquiring ownership.
    pub(super) async fn lock_for_transaction_operation(
        &self,
        open_tx_id: OpenTransactionId,
        owner_operation_admitted: bool,
    ) -> Result<futures::lock::MutexGuard<'_, NodeState<S>>, Error> {
        let mut node = self.node.lock().await;
        self.reject_tombstoned_transaction_in(&mut node, open_tx_id)?;
        if !owner_operation_admitted && let Err(error) = self.ensure_transaction_admission_open() {
            self.finish_transaction_abandonment_shutdown_in(&mut node)?;
            return Err(error);
        }
        Ok(node)
    }

    fn ensure_transaction_admission_open(&self) -> Result<(), Error> {
        if self.transaction_abandonments_closed.get() {
            return Err(Error::new(
                ErrorCode::Protocol,
                "database transaction admission is closed",
            ));
        }
        Ok(())
    }

    pub(super) fn mark_transaction_abandoned(&self, open_tx_id: OpenTransactionId) -> bool {
        self.pending_transaction_abandonments
            .borrow_mut()
            .insert(open_tx_id)
    }

    pub(super) fn clear_transaction_abandonment(&self, open_tx_id: OpenTransactionId) {
        self.pending_transaction_abandonments
            .borrow_mut()
            .remove(&open_tx_id);
    }

    fn reject_tombstoned_transaction_in(
        &self,
        node: &mut NodeState<S>,
        open_tx_id: OpenTransactionId,
    ) -> Result<(), Error> {
        if !self
            .pending_transaction_abandonments
            .borrow()
            .contains(&open_tx_id)
        {
            return Ok(());
        }
        let retirement = Self::abandon_transaction_for_maintenance(node, open_tx_id);
        if retirement.is_ok() {
            self.clear_transaction_abandonment(open_tx_id);
        }
        retirement?;
        Err(transaction_abandoned(open_tx_id))
    }

    /// Abandon an RAII-owned transaction immediately when the node is
    /// uncontended, otherwise leave its deduplicated tombstone for the next
    /// transaction operation or asynchronous owner turn.
    ///
    /// This is synchronous and never waits for the node mutex.
    pub(super) fn abandon_or_enqueue_transaction(&self, open_tx_id: OpenTransactionId) {
        if self.transaction_abandonments_closed.get() {
            // Shutdown owns every still-open transaction after closing this
            // gate, so a late Drop has no separate maintenance to admit.
            return;
        }
        if !self.mark_transaction_abandoned(open_tx_id) {
            return;
        }
        if let Some(mut node) = self.node.try_lock() {
            if Self::abandon_transaction_for_maintenance(&mut node, open_tx_id).is_ok() {
                self.clear_transaction_abandonment(open_tx_id);
            } else {
                self.schedule_tick(TickUrgency::Immediate);
            }
            return;
        }
        self.schedule_tick(TickUrgency::Immediate);
    }

    fn abandon_transaction_for_maintenance(
        node: &mut NodeState<S>,
        open_tx_id: OpenTransactionId,
    ) -> Result<(), Error> {
        match node.abandon_tx(open_tx_id) {
            Ok(()) | Err(crate::node::Error::MissingOpenBatch(_)) => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    fn drain_transaction_abandonments_in(&self, node: &mut NodeState<S>) -> Result<usize, Error> {
        let abandonments = std::mem::take(&mut *self.pending_transaction_abandonments.borrow_mut());
        let count = abandonments.len();
        let mut first_error = None;
        for open_tx_id in abandonments {
            if let Err(error) = Self::abandon_transaction_for_maintenance(node, open_tx_id) {
                self.pending_transaction_abandonments
                    .borrow_mut()
                    .insert(open_tx_id);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(count),
        }
    }

    fn finish_transaction_abandonment_shutdown_in(
        &self,
        node: &mut NodeState<S>,
    ) -> Result<usize, Error> {
        let drain_result = self.drain_transaction_abandonments_in(node);
        // Shutdown must not retire every open transaction until the FIFO owner
        // has reached quiescence. A queued begin/stage/commit was admitted
        // before close closed the gate, but may not have acquired the node
        // lock yet. Sweeping it from an intervening ordinary tick would turn
        // that accepted operation into a tombstoned transaction. Keep the
        // shutdown sweep pending until its own queue has completely drained;
        // a later owner turn or close will run this same idempotent sweep.
        if self.transaction_abandonment_shutdown_pending.get()
            && self.queued_mutations.borrow().is_empty()
            && self.queued_mutation_active_leases.get() == 0
        {
            self.transaction_abandonment_shutdown_pending.set(false);
            node.abandon_all_open_transactions();
        }
        drain_result
    }

    async fn drain_transaction_abandonments(&self) -> Result<usize, Error> {
        let mut node = self.node.lock().await;
        self.finish_transaction_abandonment_shutdown_in(&mut node)
    }

    /// Close transaction admission and transfer the final open-transaction
    /// sweep to the node tick owner before the close future can suspend.
    ///
    /// If the caller cancels `Db::close` while it is waiting for the node
    /// mutex, the scheduled tick still drains tombstones and terminalizes every
    /// open transaction. Repeated close attempts and tick passes are benign.
    pub(super) fn begin_transaction_abandonment_shutdown(&self) {
        if self.transaction_abandonments_closed.replace(true) {
            return;
        }
        self.transaction_abandonment_shutdown_pending.set(true);
        self.schedule_tick(TickUrgency::Immediate);
    }

    pub(super) async fn finish_transaction_abandonment_shutdown(&self) -> Result<usize, Error> {
        let mut node = self.node.lock().await;
        self.finish_transaction_abandonment_shutdown_in(&mut node)
    }

    pub(super) fn transaction_abandonment_shutdown_is_pending(&self) -> bool {
        self.transaction_abandonment_shutdown_pending.get()
    }

    /// Enqueue a stream-finalization command without touching the async node
    /// mutex. This is the only operation a stream's `Drop` implementation may
    /// perform. A closed node has already retired its runtime, so later
    /// commands are safely invalidated and acknowledged immediately.
    pub(super) fn enqueue_subscription_finalization(
        &self,
        mut command: PendingSubscriptionFinalization,
    ) {
        if self.subscription_finalizations_closed.get() {
            if self.subscription_runtime_retired.get() {
                // Terminal retirement already drained every maintained view
                // and connection, so a late finalizer owns no resident work.
                if let Some(acknowledgement) = command.acknowledgement.take() {
                    let _ = acknowledgement.send(());
                }
                return;
            }
        }
        self.pending_subscription_finalizations
            .borrow_mut()
            .push_back(command);
        self.schedule_tick(TickUrgency::Immediate);
    }

    /// Drain queued stream cleanup under the ordinary async node owner. There
    /// is no await after the queue is taken, so cancellation while waiting for
    /// the mutex leaves the command queued for a later owner turn.
    pub(super) async fn drain_subscription_finalizations(&self) -> Result<usize, Error> {
        let mut node = self.node.lock().await;
        let commands = std::mem::take(&mut *self.pending_subscription_finalizations.borrow_mut());
        let mut drained = 0;
        let mut changed_upstream = false;
        for mut command in commands {
            let (local, upstream, owner) = if let Some(state) = command.state.take() {
                let owner = Rc::downgrade(&state);
                let mut state = state.borrow_mut();
                state.scalar_reconciliation = ScalarReconciliation::default();
                (
                    state.local_subscription_cleanup.take(),
                    std::mem::take(&mut state.upstream_subscription_handles),
                    owner,
                )
            } else {
                (
                    command.opening_local.take(),
                    std::mem::take(&mut command.opening_upstream),
                    Weak::new(),
                )
            };
            if let Some((runtime_token, subscription_id)) = local
                && node.groove_runtime_token() == runtime_token
            {
                node.unsubscribe_groove_subscription(subscription_id);
            }
            for handle in upstream {
                unregister_upstream_subscription_owner(
                    &self.upstream_subscription_owners,
                    handle.subscription,
                    &owner,
                );
                let mut refcounts = self.upstream_coverage_refcounts.borrow_mut();
                let Some(count) = refcounts.get_mut(&handle.coverage) else {
                    continue;
                };
                *count = count.saturating_sub(1);
                if *count > 0 {
                    continue;
                }
                refcounts.remove(&handle.coverage);
                self.awaiting_initial_authority_coverage
                    .borrow_mut()
                    .remove(&handle.coverage);
                drop(refcounts);
                node.apply_unsubscribe(handle.subscription);
                self.coverage_refresh_generations
                    .borrow_mut()
                    .remove(&handle.coverage);
                if let Some(receipts) = self.active_authority_view_receipts.borrow_mut().as_mut() {
                    receipts.subscriptions.remove(&handle.subscription);
                }
                self.latest_coverage_subscriptions
                    .borrow_mut()
                    .retain(|coverage, subscription| {
                        coverage != &handle.coverage && *subscription != handle.subscription
                    });
                self.upstream_subscriptions
                    .borrow_mut()
                    .push(PendingUpstreamCommand::Unsubscribe(handle.subscription));
                changed_upstream = true;
            }
            if let Some(acknowledgement) = command.acknowledgement.take() {
                let _ = acknowledgement.send(());
            }
            drained += 1;
        }
        drop(node);
        if changed_upstream {
            self.schedule_tick(TickUrgency::Immediate);
        }
        Ok(drained)
    }

    /// Atomically close the admission gate and enqueue every live stream for
    /// retirement.  `Db::close` calls this before its first await, so a drop
    /// racing storage shutdown cannot land between a drain and the durable
    /// close. Late finalizers may be acknowledged only after this method has
    /// made their state part of the terminal retirement set.
    pub(super) fn begin_subscription_finalization_shutdown(&self) {
        if self.subscription_finalizations_closed.replace(true) {
            return;
        }
        let live = self
            .subscriptions
            .borrow()
            .iter()
            .filter_map(Weak::upgrade)
            .collect::<Vec<_>>();
        let mut pending = self.pending_subscription_finalizations.borrow_mut();
        for state in live {
            state.borrow().closed.set(true);
            pending.push_back(PendingSubscriptionFinalization {
                state: Some(state),
                opening_upstream: Vec::new(),
                opening_local: None,
                acknowledgement: None,
            });
        }
        // Once admission closes, an abandoned close future must not be the
        // sole owner capable of retiring the captured streams.
        self.schedule_tick(TickUrgency::Immediate);
    }

    /// Release all connection and subscription bookkeeping after its backing
    /// storage has closed.  No later finalizer can leave a live local Groove
    /// view or upstream ownership behind: the retirement pass drained them
    /// before close, and this removes the now-unusable runtime shell.
    pub(super) fn retire_subscription_runtime_after_close(&self) {
        self.subscription_runtime_retired.set(true);
        self.subscriptions.borrow_mut().clear();
        self.connections.borrow_mut().clear();
        self.upstream_subscriptions.borrow_mut().clear();
        self.pending_subscription_finalizations.borrow_mut().clear();
        self.latest_coverage_subscriptions.borrow_mut().clear();
        self.upstream_coverage_refcounts.borrow_mut().clear();
        self.awaiting_initial_authority_coverage
            .borrow_mut()
            .clear();
        self.query_coverage_registrations.borrow_mut().clear();
        self.upstream_subscription_owners.borrow_mut().clear();
        self.relay_upstream_subscription_owners.borrow_mut().clear();
        self.pending_relay_subscription_rejections
            .borrow_mut()
            .clear();
    }

    pub(super) fn subscription_finalization_shutdown_is_pending(&self) -> bool {
        self.subscription_finalizations_closed.get() && !self.subscription_runtime_retired.get()
    }

    pub(super) fn set_mutation_error_callback(&self, callback: Option<MutationErrorCallback>) {
        let should_schedule = {
            let mut state = self.mutation_errors.borrow_mut();
            state.callback = callback;
            state.callback.is_some() && !state.pending.is_empty()
        };
        if should_schedule {
            self.schedule_tick(TickUrgency::Immediate);
        }
    }

    async fn consume_mutation_error(&self, tx_id: TxId) -> Result<bool, Error> {
        let pending = self.mutation_errors.borrow_mut().pending.remove(&tx_id);
        let retained = self.node.borrow().rejected_transaction(tx_id).is_some();
        if retained {
            self.deferred_rejection_discards.borrow_mut().insert(tx_id);
            self.schedule_tick(TickUrgency::Immediate);
        }
        Ok(pending.is_some() || retained)
    }

    #[cfg(test)]
    pub(super) fn defer_rejection_discard_for_test(&self, tx_id: TxId) {
        self.deferred_rejection_discards.borrow_mut().insert(tx_id);
    }

    /// Finish rejection acknowledgement after all wait observers woken by one
    /// owner turn have had a chance to inspect the shared terminal state.
    pub(super) async fn flush_deferred_rejection_discards(&self) -> Result<(), Error> {
        let tx_ids = std::mem::take(&mut *self.deferred_rejection_discards.borrow_mut());
        let mut first_error = None;
        for tx_id in tx_ids {
            if let Err(error) = self.node.lock().await.discard_rejection(tx_id).await {
                self.deferred_rejection_discards.borrow_mut().insert(tx_id);
                if first_error.is_none() {
                    first_error = Some(error);
                }
            }
        }
        if let Some(error) = first_error {
            // A failed atomic Groove commit poisons this opened database. Do
            // not convert its retained acknowledgement into an immediate tick
            // loop: the caller must observe the error and reopen before this
            // runtime can make durable progress again. Other errors are also
            // retained, but have no generic retry-safety contract here, so a
            // host may explicitly service a later owner turn without us
            // spinning on an unknown storage failure.
            if matches!(
                error,
                crate::node::Error::Groove(groove::db::Error::DatabasePoisoned)
            ) {
                tracing::error!(
                    %error,
                    "deferred mutation-error acknowledgement requires reopening the database"
                );
            } else {
                tracing::warn!(
                    %error,
                    "deferred mutation-error acknowledgement retained for an explicit later retry"
                );
            }
            return Err(error.into());
        }
        Ok(())
    }

    pub(super) async fn transaction_wait_outcome(
        &self,
        tx_id: TxId,
        tier: DurabilityTier,
    ) -> Option<Result<TxId, Error>> {
        if let Some(error) = self.queued_mutation_failures.borrow().get(&tx_id) {
            return Some(Err(error.clone()));
        }
        if self.reserved_mutations.borrow().contains(&tx_id) {
            return None;
        }
        let state = self.node.lock().await.transaction_state(tx_id).await;
        let Some((fate, global_time, durability)) = state else {
            return Some(Err(Error::new(
                ErrorCode::NotObserved,
                format!("transaction {tx_id:?} is not known locally"),
            )));
        };
        let satisfied = transaction_satisfies_wait(&fate, global_time, durability, tier);
        match fate {
            Fate::Rejected(reason) => {
                if let Err(error) = self.consume_mutation_error(tx_id).await {
                    tracing::warn!(?tx_id, %error, "failed to consume waited mutation error");
                }
                Some(Err(write_rejected(tx_id, reason)))
            }
            Fate::Pending | Fate::Accepted if satisfied => Some(Ok(tx_id)),
            Fate::Pending | Fate::Accepted => None,
        }
    }

    /// Read a transaction's wait predicate without claiming its rejection.
    ///
    /// A queued empty update may use an existing transaction as its bounded
    /// completion target. That public request must observe the target's state,
    /// but it is not an additional owner of the target's mutation-error
    /// delivery: the target's own waiter or mutation-error callback retains
    /// that responsibility.
    async fn completion_target_wait_outcome(
        &self,
        public_tx_id: TxId,
        target_tx_id: TxId,
        tier: DurabilityTier,
    ) -> Option<Result<TxId, Error>> {
        let state = self.node.lock().await.transaction_state(target_tx_id).await;
        let Some((fate, global_time, durability)) = state else {
            return Some(Err(Error::new(
                ErrorCode::NotObserved,
                format!("completion target for transaction {public_tx_id:?} is not known locally"),
            )));
        };
        let satisfied = transaction_satisfies_wait(&fate, global_time, durability, tier);
        match fate {
            Fate::Rejected(reason) => Some(Err(write_rejected(public_tx_id, reason))),
            Fate::Pending | Fate::Accepted if satisfied => Some(Ok(public_tx_id)),
            Fate::Pending | Fate::Accepted => None,
        }
    }

    pub(super) fn queued_mutation_write_state(
        &self,
        tx_id: TxId,
    ) -> Option<Result<WriteState, Error>> {
        if let Some(error) = self.queued_mutation_failures.borrow().get(&tx_id) {
            return Some(Err(error.clone()));
        }
        self.reserved_mutations
            .borrow()
            .contains(&tx_id)
            .then_some(Ok(WriteState {
                fate: Fate::Pending,
                global_time: None,
                durability: DurabilityTier::None,
            }))
    }

    pub(super) fn take_queued_mutation_failure(&self, tx_id: TxId) -> Option<Error> {
        self.queued_mutation_failures.borrow_mut().remove(&tx_id)
    }

    pub(super) fn wait_for_transaction_with(
        self: &Rc<Self>,
        tx_id: TxId,
        tier: DurabilityTier,
        callback: Box<dyn FnOnce(Result<TxId, Error>)>,
    ) {
        self.wait_for_write_with(tx_id, None, tier, callback);
    }

    /// Callback wait for a binding-owned write handle. A queued empty update
    /// reserves a request id before it can asynchronously discover that the
    /// update is a no-op; its handle-local alias then points at the existing
    /// transaction whose state satisfies this wait. The alias is retained by
    /// the caller, never by this runtime.
    pub(super) fn wait_for_write_with(
        self: &Rc<Self>,
        tx_id: TxId,
        alias: Option<QueuedMutationAlias>,
        tier: DurabilityTier,
        callback: Box<dyn FnOnce(Result<TxId, Error>)>,
    ) {
        if self.mutation_owner_lifecycle.get() == MutationOwnerLifecycle::Closing {
            callback(Err(Error::new(
                ErrorCode::NotObserved,
                format!("database is closed; transaction {tx_id:?} cannot be observed"),
            )));
            return;
        }
        let node = Rc::clone(self);
        self.transaction_wait_observers
            .borrow_mut()
            .push(Box::pin(async move {
                callback(node.wait_for_write(tx_id, alias, tier).await);
            }));
        self.schedule_tick(TickUrgency::Immediate);
    }

    async fn wait_for_write(
        &self,
        tx_id: TxId,
        alias: Option<QueuedMutationAlias>,
        tier: DurabilityTier,
    ) -> Result<TxId, Error> {
        let Some(alias) = alias else {
            return self.wait_for_transaction(tx_id, tier).await;
        };
        loop {
            let target_tx_id = { *alias.borrow() };
            if let Some(target_tx_id) = target_tx_id {
                return self
                    .wait_for_completion_target(tx_id, target_tx_id, tier)
                    .await;
            }
            if let Some(outcome) = self.transaction_wait_outcome(tx_id, tier).await {
                return outcome;
            }
            if self.mutation_owner_lifecycle.get() == MutationOwnerLifecycle::Closing
                && !self.reserved_mutations.borrow().contains(&tx_id)
            {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    format!("database closed before transaction {tx_id:?} reached {tier:?}"),
                ));
            }
            let state_change = self.register_write_state_waiter(tx_id);
            let target_tx_id = { *alias.borrow() };
            if let Some(target_tx_id) = target_tx_id {
                drop(state_change);
                return self
                    .wait_for_completion_target(tx_id, target_tx_id, tier)
                    .await;
            }
            if let Some(outcome) = self.transaction_wait_outcome(tx_id, tier).await {
                drop(state_change);
                return outcome;
            }
            state_change.await;
        }
    }

    async fn wait_for_completion_target(
        &self,
        public_tx_id: TxId,
        target_tx_id: TxId,
        tier: DurabilityTier,
    ) -> Result<TxId, Error> {
        loop {
            if let Some(outcome) = self
                .completion_target_wait_outcome(public_tx_id, target_tx_id, tier)
                .await
            {
                return outcome;
            }
            let state_change = self.register_write_state_waiter(target_tx_id);
            if let Some(outcome) = self
                .completion_target_wait_outcome(public_tx_id, target_tx_id, tier)
                .await
            {
                drop(state_change);
                return outcome;
            }
            state_change.await;
        }
    }

    pub(super) async fn wait_for_transaction(
        &self,
        tx_id: TxId,
        tier: DurabilityTier,
    ) -> Result<TxId, Error> {
        loop {
            if let Some(outcome) = self.transaction_wait_outcome(tx_id, tier).await {
                return outcome;
            }
            if self.mutation_owner_lifecycle.get() == MutationOwnerLifecycle::Closing
                && !self.reserved_mutations.borrow().contains(&tx_id)
            {
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    format!("database closed before transaction {tx_id:?} reached {tier:?}"),
                ));
            }
            let state_change = self.register_write_state_waiter(tx_id);
            if let Some(outcome) = self.transaction_wait_outcome(tx_id, tier).await {
                drop(state_change);
                return outcome;
            }
            if self.mutation_owner_lifecycle.get() == MutationOwnerLifecycle::Closing
                && !self.reserved_mutations.borrow().contains(&tx_id)
            {
                drop(state_change);
                return Err(Error::new(
                    ErrorCode::NotObserved,
                    format!("database closed before transaction {tx_id:?} reached {tier:?}"),
                ));
            }
            state_change.await;
        }
    }

    pub(super) fn poll_transaction_wait_observers(&self) {
        use std::task::{Context, Poll, Waker};

        let owned_waker = self.query_runtime_waker();
        let waker = owned_waker.as_ref().unwrap_or_else(|| Waker::noop());
        let mut context = Context::from_waker(waker);
        let mut observers = std::mem::take(&mut *self.transaction_wait_observers.borrow_mut());
        observers.retain_mut(|observer| observer.as_mut().poll(&mut context) == Poll::Pending);
        self.transaction_wait_observers
            .borrow_mut()
            .splice(0..0, observers);
    }

    #[cfg(test)]
    pub(super) fn enqueue_transaction_wait_observer_for_test(
        &self,
        observer: TransactionWaitObserver,
    ) {
        self.transaction_wait_observers.borrow_mut().push(observer);
    }

    pub(super) async fn drain_transaction_wait_observers(&self) {
        std::future::poll_fn(|context| {
            let mut observers = std::mem::take(&mut *self.transaction_wait_observers.borrow_mut());
            observers.retain_mut(|observer| observer.as_mut().poll(context) == Poll::Pending);
            let empty = observers.is_empty();
            self.transaction_wait_observers
                .borrow_mut()
                .splice(0..0, observers);
            if empty {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await
    }

    fn deliver_pending_mutation_errors(&self) {
        let Some((callback, events)) = take_pending_mutation_error_delivery(&self.mutation_errors)
        else {
            return;
        };
        for (tx_id, event) in events {
            if let Err(error) = crate::db::block_on(self.node.borrow_mut().discard_rejection(tx_id))
            {
                tracing::warn!(?tx_id, %error, "failed to acknowledge delivered mutation error");
            }
            callback(&event);
        }
    }

    pub(super) fn request_permission_advice(
        &self,
        action: PermissionAdviceAction,
    ) -> PermissionAdviceFuture {
        let request_id = PermissionAdviceRequestId(*uuid::Uuid::new_v4().as_bytes());
        let (sender, receiver) = oneshot::channel();
        self.permission_advice_waiters
            .borrow_mut()
            .insert(request_id, sender);
        self.upstream_subscriptions.borrow_mut().push(
            PendingUpstreamCommand::AuthorizationScopeIntent {
                request_id,
                action,
                session_claim_binding: None,
                delegated_session: None,
            },
        );
        self.schedule_tick(TickUrgency::Immediate);
        PermissionAdviceFuture {
            waiters: Rc::clone(&self.permission_advice_waiters),
            request_id,
            receiver,
        }
    }

    pub(super) fn request_permission_advice_with_delegated_session(
        &self,
        action: PermissionAdviceAction,
        session: crate::protocol::DelegatedSessionBinding,
    ) -> PermissionAdviceFuture {
        let request_id = PermissionAdviceRequestId(*uuid::Uuid::new_v4().as_bytes());
        let (sender, receiver) = oneshot::channel();
        self.permission_advice_waiters
            .borrow_mut()
            .insert(request_id, sender);
        self.upstream_subscriptions.borrow_mut().push(
            PendingUpstreamCommand::AuthorizationScopeIntent {
                request_id,
                action,
                session_claim_binding: Some((session.identity, session.claims.clone())),
                delegated_session: Some(session),
            },
        );
        self.schedule_tick(TickUrgency::Immediate);
        PermissionAdviceFuture {
            waiters: Rc::clone(&self.permission_advice_waiters),
            request_id,
            receiver,
        }
    }

    pub(super) fn cancel_permission_advice_request(&self, request_id: PermissionAdviceRequestId) {
        self.permission_advice_waiters
            .borrow_mut()
            .remove(&request_id);
        for connection in self.connections.borrow().iter() {
            let mut connection = connection.borrow_mut();
            if let ConnectionLink::Upstream(UpstreamConnectionState {
                scope_lease_manager,
                ..
            }) = &mut connection.link
            {
                let mut empty = false;
                for request in scope_lease_manager.requests.values_mut() {
                    if request.waiters.remove(&request_id) {
                        empty |= request.waiters.is_empty();
                    }
                }
                if empty {
                    scope_lease_manager
                        .requests
                        .retain(|_, request| !request.waiters.is_empty());
                }
            }
        }
    }

    pub(super) fn register_write_state_waiter(&self, tx_id: TxId) -> WriteStateChange {
        let waiter_id = self.next_write_state_waiter_id.get();
        self.next_write_state_waiter_id
            .set(waiter_id.wrapping_add(1).max(1));
        let (sender, receiver) = oneshot::channel();
        self.write_state_waiters
            .borrow_mut()
            .entry(tx_id)
            .or_default()
            .push(WriteStateWaiter {
                id: waiter_id,
                notify: WriteStateWaiterNotify::Future(sender),
            });
        WriteStateChange {
            waiters: Rc::clone(&self.write_state_waiters),
            tx_id,
            waiter_id,
            receiver,
        }
    }

    #[allow(dead_code)]
    pub(super) async fn refresh_subscriptions(&self) -> Result<usize, Error> {
        let progress_waker = self.query_runtime_waker();
        refresh_subscriptions_in(
            &self.node,
            &self.subscriptions,
            &self.active_authority_view_receipts,
            progress_waker.as_ref(),
        )
        .await
    }

    /// Retire authority settlement synchronously without re-evaluating data.
    ///
    /// Connection replacement changes Jazz status, not the Groove terminal
    /// baseline. Keeping this path non-suspending ensures consumers observe
    /// the demotion before the replacement transport can publish anything.
    fn demote_authority_receipt_subscriptions(&self) {
        demote_authority_receipt_subscriptions(&self.subscriptions, &BTreeSet::new());
    }

    /// Attach using the historical replay-error fallback (an empty upload).
    pub async fn connect_upstream(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.connect_upstream_inner(transport, false)
            .await
            .expect("legacy replay fallback is infallible")
    }

    /// Attach only after every parked replay unit can be loaded.
    pub async fn try_connect_upstream(
        &self,
        transport: Box<dyn Transport>,
    ) -> Result<Rc<LocalMutex<PeerConnection<S>>>, Error> {
        self.connect_upstream_inner(transport, true).await
    }

    async fn connect_upstream_inner(
        &self,
        transport: Box<dyn Transport>,
        strict_replay: bool,
    ) -> Result<Rc<LocalMutex<PeerConnection<S>>>, Error> {
        loop {
            // Connection installation mutates runtime metadata synchronously, but
            // first needs a coherent view of storage-owning node state. Evaluation
            // may temporarily own that state across an async hydration boundary,
            // so wait for it instead of using the synchronous borrow escape hatch.
            let peers = self.connections.borrow().clone();
            let peer_guards = Self::acquire_peer_inventory(&peers).await;
            let mut node = self.node.lock().await;
            if !self.peer_inventory_matches(&peers) {
                drop(node);
                drop(peer_guards);
                continue;
            }
            let session_context = transport.connection_session_context();
            let mut replay_units = BTreeMap::new();
            if self.admitted_upstream_authority.borrow().is_none()
                && session_context.is_some_and(|context| {
                    context.negotiated_features & crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS
                        != 0
                })
            {
                let transactions: Vec<_> = self.edge_fate_routes.borrow().keys().copied().collect();
                for tx_id in transactions {
                    let unit = node.commit_unit_for(tx_id).await;
                    // The legacy entry point retained an empty upload on read
                    // failure. The strict binding entry fails before admission.
                    let unit = if strict_replay {
                        Some(unit?)
                    } else {
                        unit.ok()
                    };
                    replay_units.insert(tx_id, unit);
                }
            }
            let local_receiver = !node.is_history_complete();
            let confirmation_floor = node.committed_global_time();
            drop(node);
            let wire_inbound_context = transport.wire_inbound_context().map(Rc::new);
            let upstream_upload_destination = session_context.and_then(|context| {
                context.remote.map(|remote| UpstreamUploadDestination {
                    remote_node: *remote.node.as_bytes(),
                    link_identity: context.link_identity,
                })
            });
            let transferred_large_value_uploads = upstream_upload_destination
                .and_then(|destination| {
                    self.detached_large_value_uploads
                        .borrow_mut()
                        .remove(&destination)
                })
                .unwrap_or_default();
            let connection_epoch = session_context
                .map(|context| context.local.epoch)
                .unwrap_or_else(|| uuid::Uuid::new_v4().as_u128() as u64);
            // Durable settled-view state remains available for known-state
            // payload repair, but a new upstream (including an edge switch) owns
            // no settlement receipts until it sends a fresh ViewUpdate.
            *self.active_authority_view_receipts.borrow_mut() = Some(AuthorityViewReceipts {
                connection_epoch,
                confirmation_floor,
                subscriptions: BTreeSet::new(),
                binding_views: BTreeSet::new(),
            });
            // A replacement link invalidates the prior link's receipt before the
            // new transport can deliver anything. Publish that demotion now rather
            // than letting cached rows remain settled until the next tick.
            self.demote_authority_receipt_subscriptions();
            let expected_scope_authority = session_context
                .filter(|context| {
                    context.negotiated_features & crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS
                        != 0
                })
                .and_then(|context| {
                    context.remote.map(|remote| AuthorityContext {
                        authority: *remote.node.as_bytes(),
                        link: context.link_identity,
                        connection_id: connection_epoch,
                        connection_epoch: remote.epoch,
                        claims_revision: 0,
                        policy_epoch: 0,
                        authorization_progress: 0,
                        settled_through: 0,
                    })
                });
            // Keep every admitted link eligible, but bind each downstream route
            // to one stable selected owner. A newly connected parallel upstream
            // must not silently replace the owner (or settle its parked writes).
            if let Some(context) = expected_scope_authority {
                let mut eligible = self.admitted_upstream_authorities.borrow_mut();
                if !eligible.contains(&context) {
                    eligible.push(context);
                }
                if self.admitted_upstream_authority.borrow().is_none() {
                    *self.admitted_upstream_authority.borrow_mut() = Some(context);
                    // Routes parked while no authority was connected retain their
                    // downstream obligation. Bind them to this first successor
                    // and restore each commit to the shared outbox: the former
                    // authority may already have suppressed its upload, while
                    // this newly connected successor has not seen it.
                    let mut routes = self.edge_fate_routes.borrow_mut();
                    let routed_txs = routes.keys().copied().collect::<Vec<_>>();
                    for obligation in routes.values_mut() {
                        for route in obligation.routes.iter_mut() {
                            route.authority = Some(context);
                        }
                    }
                    drop(routes);
                    let mut outbox = self.outbox.borrow_mut();
                    for tx_id in routed_txs {
                        outbox.push(PendingUpload {
                            tx_id,
                            unit: replay_units
                                .remove(&tx_id)
                                .expect("preloaded upstream replay"),
                        });
                    }
                }
            }
            // Carry queued and already-registered subscriptions upstream immediately.
            let mut pending = self
                .upstream_subscriptions
                .borrow_mut()
                .drain(..)
                .collect::<Vec<_>>();
            let mut pending_subscriptions = pending
                .iter()
                .filter_map(|command| {
                    let PendingUpstreamCommand::Subscribe(subscription) = command else {
                        return None;
                    };
                    Some(subscription.subscription)
                })
                .collect::<BTreeSet<_>>();
            for registration in self.query_coverage_registrations.borrow().values() {
                if pending_subscriptions.insert(registration.subscription.subscription) {
                    pending.push(PendingUpstreamCommand::Subscribe(
                        registration.subscription.clone(),
                    ));
                }
            }
            for state_rc in self.subscriptions.borrow().iter().filter_map(Weak::upgrade) {
                {
                    let state = state_rc.borrow();
                    if !state.propagates_upstream {
                        continue;
                    }
                    let SubscriptionKind::Prepared { shape, binding, .. } = &state.kind;
                    let opts = self
                        .upstream_register_shape_options(state.read_tier, state.read_view.clone());
                    let coverage = request_coverage_key(
                        shape,
                        binding,
                        opts.clone(),
                        &state.request_identity_claims,
                    );
                    let subscription = self
                        .upstream_subscription_owners
                        .borrow()
                        .iter()
                        .find_map(|(subscription, owners)| {
                            owners
                                .iter()
                                .any(|owner| {
                                    owner
                                        .upgrade()
                                        .is_some_and(|owner| Rc::ptr_eq(&owner, &state_rc))
                                })
                                .then_some(*subscription)
                        })
                        .unwrap_or_else(|| self.next_subscription_key(shape, opts.read_view_key()));
                    self.latest_coverage_subscriptions
                        .borrow_mut()
                        .insert(coverage, subscription);
                    if pending_subscriptions.insert(subscription) {
                        pending.push(PendingUpstreamCommand::Subscribe(
                            PendingUpstreamSubscription {
                                subscription,
                                shape: shape.clone(),
                                binding: binding.clone(),
                                opts,
                                identity: state.author,
                                policy_binding: state.request_identity_claims.clone(),
                            },
                        ));
                    }
                }
            }
            // Relay-owned coverage is not represented by the public subscription
            // list above: it is retained by the downstream connection that it
            // serves. Replacing an upstream transport drops that transport's wire
            // subscriptions, while the downstream browser is still connected and
            // therefore will not send a fresh Subscribe. Replay every live relay
            // owner onto the successor authority, using its stable usage-site key.
            //
            // The owner map is the lifecycle authority here. A rejected coverage
            // group can briefly remain on its downstream link while its rejection
            // waits to be delivered; replaying that orphan would resurrect a
            // subscription that is already being retired.
            let relay_subscriptions = {
                let owners = self.relay_upstream_subscription_owners.borrow();
                self.connections
                    .borrow()
                    .iter()
                    .flat_map(|connection| {
                        let connection = peer_guards
                            .get(&(Rc::as_ptr(connection) as usize))
                            .expect("stable upstream peer inventory");
                        let ConnectionLink::Subscriber(subscriber) = &connection.link else {
                            return Vec::new();
                        };
                        subscriber
                            .coverage_groups
                            .iter()
                            .filter_map(|(coverage, group)| {
                                let owner = owners.get(&(
                                    group.upstream_subscription,
                                    connection.connection_epoch,
                                    coverage.opts.read_view_key(),
                                ))?;
                                (group.upstream_opts.propagate_upstream
                                    && owner.downstream_connection_epoch
                                        == connection.connection_epoch
                                    && owner.coverage == *coverage)
                                    .then(|| PendingUpstreamSubscription {
                                        subscription: group.upstream_subscription,
                                        shape: group.shape.clone(),
                                        binding: group.binding.clone(),
                                        opts: group.upstream_opts.clone(),
                                        // This usage is forwarded under the
                                        // subscription's topology-admitted
                                        // policy binding, never the relay
                                        // transport. A relay has no link
                                        // permission subject to fall back to.
                                        identity: group.policy_binding.0,
                                        policy_binding: Some(group.policy_binding.clone()),
                                    })
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<Vec<_>>()
            };
            for subscription in relay_subscriptions {
                if pending_subscriptions.insert(subscription.subscription) {
                    pending.push(PendingUpstreamCommand::Subscribe(subscription));
                }
            }
            let connection = Rc::new(LocalMutex::new(PeerConnection {
                transport,
                staged_inbound: VecDeque::new(),
                node: Rc::clone(&self.node),
                subscriptions: Rc::clone(&self.subscriptions),
                upstream_subscription_owners: Rc::clone(&self.upstream_subscription_owners),
                relay_upstream_subscription_owners: Rc::clone(
                    &self.relay_upstream_subscription_owners,
                ),
                pending_relay_subscription_rejections: Rc::clone(
                    &self.pending_relay_subscription_rejections,
                ),
                latest_coverage_subscriptions: Rc::clone(&self.latest_coverage_subscriptions),
                awaiting_initial_authority_coverage: Rc::clone(
                    &self.awaiting_initial_authority_coverage,
                ),
                query_coverage_registrations: Rc::clone(&self.query_coverage_registrations),
                active_authority_view_receipts: Rc::clone(&self.active_authority_view_receipts),
                coverage_refresh_generations: Rc::clone(&self.coverage_refresh_generations),
                scheduler: Rc::clone(&self.scheduler),
                upload_retry_clock: Rc::clone(&self.upload_retry_clock),
                upstream_upload_destination,
                large_value_upload_retry_deadlines: Rc::clone(
                    &self.large_value_upload_retry_deadlines,
                ),
                write_state_waiters: Rc::clone(&self.write_state_waiters),
                permission_advice_waiters: Rc::clone(&self.permission_advice_waiters),
                current_rows: Rc::clone(&self.current_rows),
                edge_fate_routes: Rc::clone(&self.edge_fate_routes),
                local_fate_routes: Rc::clone(&self.local_fate_routes),
                admitted_upstream_authority: Rc::clone(&self.admitted_upstream_authority),
                downstream_fates: Rc::new(RefCell::new(Vec::new())),
                mutation_errors: Rc::clone(&self.mutation_errors),
                browser_relay_recovered_tx_ids: Rc::clone(&self.browser_relay_recovered_tx_ids),
                subscriber_dirty_epoch: Rc::clone(&self.subscriber_dirty_epoch),
                #[cfg(any(test, feature = "testing"))]
                fail_next_subscription_refresh: Cell::new(false),
                observed_subscriber_dirty_epoch: Cell::new(self.subscriber_dirty_epoch.get()),
                observed_session_claim_revision: Cell::new(0),
                connection_epoch,
                startup_error: None,
                released_outbox_tx_ids: Vec::new(),
                pending_chunk_response: None,
                pending_control_responses: VecDeque::new(),
                link: ConnectionLink::Upstream(UpstreamConnectionState {
                    local_receiver,
                    pending,
                    upstream_subscriptions: Rc::clone(&self.upstream_subscriptions),
                    announced_shapes: BTreeSet::new(),
                    sent_session_claim_revisions: BTreeMap::new(),
                    outbox: Rc::clone(&self.outbox),
                    uploaded: BTreeSet::new(),
                    large_value_uploads: transferred_large_value_uploads,
                    awaiting_large_value_uploads: BTreeMap::new(),
                    failed_large_value_uploads: BTreeSet::new(),
                    pending_row_version_fetches: VecDeque::new(),
                    pending_row_version_repairs: VecDeque::new(),
                    scope_view_cuts: BTreeMap::new(),
                    scope_receipts: BTreeMap::new(),
                    expected_scope_authority,
                    scope_lease_manager: AuthorizationScopeLeaseManager::default(),
                }),
                last_resume_bytes: None,
                auxiliary_pump: PeerIoPump::new(
                    self.chunk_resolver.clone(),
                    self.local_chunk_reader.clone(),
                    connection_epoch,
                    PeerIoPumpRole::Upstream,
                    wire_inbound_context,
                ),
            }));
            self.connections.borrow_mut().push(Rc::clone(&connection));
            self.schedule_tick(TickUrgency::Immediate);
            return Ok(connection);
        }
    }

    /// Accept a subscriber connection served under `identity`.
    ///
    /// Local-vs-authority behavior is derived from this receiving node, not
    /// selected by the connecting client.
    pub fn accept_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_trust(transport, identity, CommitUnitTrust::Session)
    }

    /// Accept a subscriber connection with explicit auth claims.
    pub fn accept_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_claims_and_trust(
            transport,
            identity,
            claims,
            CommitUnitTrust::Session,
        )
    }

    #[cfg(test)]
    pub(crate) fn accept_test_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        let admitted = self
            .node
            .borrow_mut()
            .set_test_provider_claims(identity, claims.clone());
        self.accept_subscriber_with_claims(transport, identity, admitted)
    }

    /// Accept a subscriber connection with an explicit commit-upload trust mode.
    pub fn accept_subscriber_with_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        trust: CommitUnitTrust,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_resume_and_trust(
            transport,
            identity,
            trust,
            BTreeMap::new(),
            None,
        )
    }

    /// Accept an authenticated relay transport. A relay has no policy subject;
    /// requests needing policy composition must carry a separately admitted
    /// delegated session binding.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn accept_relay_subscriber(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_relay_subscriber_internal(transport)
    }

    #[cfg(not(feature = "testing"))]
    #[allow(dead_code)]
    pub(crate) fn accept_relay_subscriber(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_relay_subscriber_internal(transport)
    }

    fn accept_relay_subscriber_internal(
        &self,
        transport: Box<dyn Transport>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_peer(
            transport,
            AuthorSubject::SYSTEM,
            CommitUnitTrust::Relay,
            BTreeMap::new(),
            None,
            PeerState::relay(),
            false,
        )
    }

    /// Admit the one immutable session selected during a scope-isolated relay
    /// handshake. This is crate-private so a host must not turn application
    /// claims or raw frames into a relay capability.
    // The public serving shell reaches this from a runtime-selected backend;
    // it is intentionally not a general Node API.
    #[allow(dead_code)]
    pub(crate) fn accept_scope_isolated_relay_subscriber(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        admission_epoch: u64,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_peer(
            transport,
            identity,
            CommitUnitTrust::Relay,
            BTreeMap::new(),
            None,
            PeerState::scope_isolated_relay(identity, claims, admission_epoch),
            false,
        )
    }

    /// Accept a subscriber connection with explicit auth claims and upload trust mode.
    pub fn accept_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_resume_and_trust(transport, identity, trust, claims, None)
    }

    /// Accept an edge-terminated subscriber with explicit auth claims.
    pub fn accept_edge_subscriber_with_claims(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_peer(
            transport,
            identity,
            CommitUnitTrust::Session,
            claims,
            None,
            PeerState::edge_client(identity),
            false,
        )
    }

    /// Accept a subscriber whose host shell is wired as an edge fate authority.
    pub fn accept_edge_authority_subscriber_with_claims_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
        trust: CommitUnitTrust,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        let peer = match trust {
            CommitUnitTrust::TrustedBackend
            | CommitUnitTrust::TrustedAuthority
            | CommitUnitTrust::TrustedAdmin => {
                PeerState::edge_client_with_permission_identity(identity, AuthorSubject::SYSTEM)
            }
            CommitUnitTrust::Session => PeerState::edge_client(identity),
            CommitUnitTrust::Relay => PeerState::relay(),
        };
        self.accept_subscriber_with_peer(transport, identity, trust, claims, None, peer, true)
    }

    /// Accept a reconnecting subscriber, resuming from a previous cursor.
    pub fn accept_subscriber_with_resume(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        cursor: ResumeCursor,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        self.accept_subscriber_with_resume_and_trust(
            transport,
            identity,
            CommitUnitTrust::Session,
            BTreeMap::new(),
            Some(cursor),
        )
    }

    fn accept_subscriber_with_resume_and_trust(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        trust: CommitUnitTrust,
        claims: BTreeMap<String, Value>,
        cursor: Option<ResumeCursor>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        // Local/pending ingestion describes durability, not authorization.
        // A scope-isolated worker receives foreground commits as local, but
        // that downstream link still terminates one authenticated foreground
        // session. Only the worker's *upstream* Relay transport is subjectless
        // and therefore requires a per-request delegated binding.
        let peer = match trust {
            CommitUnitTrust::TrustedBackend
            | CommitUnitTrust::TrustedAuthority
            | CommitUnitTrust::TrustedAdmin => {
                PeerState::edge_client_with_permission_identity(identity, AuthorSubject::SYSTEM)
            }
            CommitUnitTrust::Session => PeerState::client_link(identity),
            CommitUnitTrust::Relay => PeerState::relay(),
        };
        self.accept_subscriber_with_peer(transport, identity, trust, claims, cursor, peer, false)
    }

    fn accept_subscriber_with_peer(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        trust: CommitUnitTrust,
        claims: BTreeMap<String, Value>,
        cursor: Option<ResumeCursor>,
        peer: PeerState,
        edge_authority: bool,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        if let Some(cursor) = &cursor {
            assert_eq!(
                cursor.ingest_context.identity, identity,
                "a resume cursor may only be used by its authenticated identity"
            );
        }
        let (downstream_fates, startup_error) =
            crate::db::block_on(self.subscriber_startup(identity, trust, edge_authority));
        self.accept_subscriber_with_peer_and_startup(
            transport,
            identity,
            trust,
            claims,
            cursor,
            peer,
            edge_authority,
            downstream_fates,
            startup_error,
        )
    }

    /// Admit a normal session subscriber without waiting on its semantic owner.
    pub async fn accept_subscriber_with_claims_async(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        claims: BTreeMap<String, Value>,
    ) -> Result<Rc<LocalMutex<PeerConnection<S>>>, Error> {
        let (downstream_fates, startup_error) = self
            .subscriber_startup(identity, CommitUnitTrust::Session, false)
            .await;
        if let Some(error) = startup_error {
            return Err(error);
        }
        Ok(self.accept_subscriber_with_peer_and_startup(
            transport,
            identity,
            CommitUnitTrust::Session,
            claims,
            None,
            PeerState::client_link(identity),
            false,
            downstream_fates,
            None,
        ))
    }

    async fn subscriber_startup(
        &self,
        identity: AuthorSubject,
        trust: CommitUnitTrust,
        edge_authority: bool,
    ) -> (PendingDownstreamFates, Option<Error>) {
        let downstream_fates = Rc::new(RefCell::new(Vec::new()));
        let scope_mismatch = self
            .node
            .lock()
            .await
            .client_relay_scope()
            .is_some_and(|scope| {
                trust == CommitUnitTrust::Session && !scope.admits_session(identity)
            })
            .then(|| {
                Error::new(
                    ErrorCode::Protocol,
                    "foreground session is outside this scope-isolated relay ownership scope",
                )
            });
        let startup_error =
            if scope_mismatch.is_none() && self.receives_commits_as_local() && !edge_authority {
                self.restore_local_subscriber(identity, &downstream_fates)
                    .await
                    .err()
            } else {
                scope_mismatch
            };
        (downstream_fates, startup_error)
    }

    fn accept_subscriber_with_peer_and_startup(
        &self,
        transport: Box<dyn Transport>,
        identity: AuthorSubject,
        trust: CommitUnitTrust,
        claims: BTreeMap<String, Value>,
        cursor: Option<ResumeCursor>,
        peer: PeerState,
        edge_authority: bool,
        downstream_fates: PendingDownstreamFates,
        startup_error: Option<Error>,
    ) -> Rc<LocalMutex<PeerConnection<S>>> {
        if edge_authority {
            self.node.borrow_mut().enable_edge_query_serving();
        }
        let local_receiver = self.receives_commits_as_local() && !edge_authority;
        let (peer, ingest_context, session_claims, session_claim_revision) = match cursor {
            Some(cursor) => {
                assert_eq!(
                    cursor.ingest_context.identity, identity,
                    "a resume cursor may only be used by its authenticated identity"
                );
                (
                    cursor.peer,
                    cursor.ingest_context,
                    cursor.session_claims,
                    cursor.session_claim_revision,
                )
            }
            None => (
                peer,
                CommitUnitIngestContext {
                    identity,
                    trust,
                    edge_authority,
                    admitted_write_authorization: false,
                },
                claims,
                0,
            ),
        };
        let connection_epoch = transport
            .connection_session_context()
            .map(|context| context.local.epoch)
            .unwrap_or_else(|| uuid::Uuid::new_v4().as_u128() as u64);
        let wire_inbound_context = transport.wire_inbound_context().map(Rc::new);
        let connection = Rc::new(LocalMutex::new(PeerConnection {
            transport,
            staged_inbound: VecDeque::new(),
            node: Rc::clone(&self.node),
            subscriptions: Rc::clone(&self.subscriptions),
            upstream_subscription_owners: Rc::clone(&self.upstream_subscription_owners),
            relay_upstream_subscription_owners: Rc::clone(&self.relay_upstream_subscription_owners),
            pending_relay_subscription_rejections: Rc::clone(
                &self.pending_relay_subscription_rejections,
            ),
            latest_coverage_subscriptions: Rc::clone(&self.latest_coverage_subscriptions),
            awaiting_initial_authority_coverage: Rc::clone(
                &self.awaiting_initial_authority_coverage,
            ),
            query_coverage_registrations: Rc::clone(&self.query_coverage_registrations),
            active_authority_view_receipts: Rc::clone(&self.active_authority_view_receipts),
            coverage_refresh_generations: Rc::clone(&self.coverage_refresh_generations),
            scheduler: Rc::clone(&self.scheduler),
            upload_retry_clock: Rc::clone(&self.upload_retry_clock),
            upstream_upload_destination: None,
            large_value_upload_retry_deadlines: Rc::clone(&self.large_value_upload_retry_deadlines),
            write_state_waiters: Rc::clone(&self.write_state_waiters),
            permission_advice_waiters: Rc::clone(&self.permission_advice_waiters),
            current_rows: Rc::clone(&self.current_rows),
            edge_fate_routes: Rc::clone(&self.edge_fate_routes),
            local_fate_routes: Rc::clone(&self.local_fate_routes),
            admitted_upstream_authority: Rc::clone(&self.admitted_upstream_authority),
            downstream_fates,
            mutation_errors: Rc::clone(&self.mutation_errors),
            browser_relay_recovered_tx_ids: Rc::clone(&self.browser_relay_recovered_tx_ids),
            subscriber_dirty_epoch: Rc::clone(&self.subscriber_dirty_epoch),
            #[cfg(any(test, feature = "testing"))]
            fail_next_subscription_refresh: Cell::new(false),
            observed_subscriber_dirty_epoch: Cell::new(self.subscriber_dirty_epoch.get()),
            observed_session_claim_revision: Cell::new(session_claim_revision),
            connection_epoch,
            startup_error,
            released_outbox_tx_ids: Vec::new(),
            pending_chunk_response: None,
            pending_control_responses: VecDeque::new(),
            link: ConnectionLink::Subscriber(SubscriberConnectionState {
                pending_authority_repairs: VecDeque::new(),
                peer,
                ingest_context,
                session_claims,
                session_claim_revision,
                local_receiver,
                partial_edge_query_host: edge_authority,
                outbox: Rc::clone(&self.outbox),
                upstream_subscriptions: Rc::clone(&self.upstream_subscriptions),
                served: BTreeMap::new(),
                coverage_groups: BTreeMap::new(),
                shape_registrations: BTreeMap::new(),
                deferred_subscribe_rejections: VecDeque::new(),
                pending_catalogue_subscriptions: BTreeMap::new(),
                scope_purposes: BTreeMap::new(),
                scope_aggregates: BTreeMap::new(),
                authority_scope_hydrations: BTreeMap::new(),
                authority_scope_hydration_count: 0,
                serve_dirty: true,
            }),
            last_resume_bytes: None,
            auxiliary_pump: PeerIoPump::new(
                self.chunk_resolver.clone(),
                self.local_chunk_reader.clone(),
                connection_epoch,
                PeerIoPumpRole::Subscriber,
                wire_inbound_context,
            ),
        }));
        self.connections.borrow_mut().push(Rc::clone(&connection));
        self.schedule_tick(TickUrgency::Immediate);
        connection
    }

    /// Detach a previously attached peer connection from this node.
    pub fn detach_connection(&self, connection: &Rc<LocalMutex<PeerConnection<S>>>) -> bool {
        if !self
            .connections
            .borrow()
            .iter()
            .any(|candidate| Rc::ptr_eq(candidate, connection))
        {
            return false;
        }
        let connection_ref = connection.borrow_mut();
        let node = self.node.borrow_mut();
        self.detach_connection_with_guards(connection, connection_ref, node, None, None)
    }

    async fn acquire_peer_inventory(
        peers: &[Rc<LocalMutex<PeerConnection<S>>>],
    ) -> PeerOwnerGuards<'_, S> {
        let mut guards = BTreeMap::new();
        for peer in peers {
            guards.insert(Rc::as_ptr(peer) as usize, peer.lock().await);
        }
        guards
    }

    fn peer_inventory_matches(&self, peers: &[Rc<LocalMutex<PeerConnection<S>>>]) -> bool {
        let current = self.connections.borrow();
        current.len() == peers.len() && current.iter().zip(peers).all(|(a, b)| Rc::ptr_eq(a, b))
    }

    /// Await a stable peer inventory and preload replay before detach mutation.
    pub async fn detach_connection_async(
        &self,
        connection: &Rc<LocalMutex<PeerConnection<S>>>,
    ) -> Result<bool, Error> {
        loop {
            let peers = self.connections.borrow().clone();
            if !peers.iter().any(|peer| Rc::ptr_eq(peer, connection)) {
                return Ok(false);
            }
            // Every concurrent detach uses registry order, never target-first.
            // No node guard is held while waiting for a peer owner.
            let mut guards = Self::acquire_peer_inventory(&peers).await;
            let mut node = self.node.lock().await;
            let unchanged = self.peer_inventory_matches(&peers);
            if !unchanged {
                drop(node);
                drop(guards);
                continue;
            }
            let mut replay = BTreeMap::new();
            let target = guards.get(&(Rc::as_ptr(connection) as usize)).unwrap();
            let handoff = match &target.link {
                ConnectionLink::Upstream(state) => {
                    state.expected_scope_authority.is_some_and(|authority| {
                        *self.admitted_upstream_authority.borrow() == Some(authority)
                            && self
                                .admitted_upstream_authorities
                                .borrow()
                                .iter()
                                .any(|other| *other != authority)
                    })
                }
                _ => false,
            };
            if handoff {
                let transactions: Vec<_> = self
                    .edge_fate_routes
                    .borrow()
                    .iter()
                    .filter_map(|(tx_id, obligation)| {
                        obligation
                            .routes
                            .iter()
                            .any(|route| route.queue.upgrade().is_some())
                            .then_some(*tx_id)
                    })
                    .collect();
                for tx_id in transactions {
                    // Finish cold storage work before any detach mutation. A
                    // failed load leaves peers, receipts and outbox unchanged.
                    replay.insert(tx_id, node.commit_unit_for(tx_id).await?);
                }
            }
            let target = guards.remove(&(Rc::as_ptr(connection) as usize)).unwrap();
            return Ok(self.detach_connection_with_guards(
                connection,
                target,
                node,
                Some(guards),
                Some(replay),
            ));
        }
    }

    fn with_detach_peer<T>(
        peer: &Rc<LocalMutex<PeerConnection<S>>>,
        guards: &mut Option<PeerOwnerGuards<'_, S>>,
        run: impl FnOnce(&mut PeerConnection<S>) -> T,
    ) -> T {
        match guards {
            Some(guards) => run(guards
                .get_mut(&(Rc::as_ptr(peer) as usize))
                .expect("stable detach peer inventory")),
            None => run(&mut peer.borrow_mut()),
        }
    }

    fn detach_connection_with_guards(
        &self,
        connection: &Rc<LocalMutex<PeerConnection<S>>>,
        mut connection_ref: futures::lock::MutexGuard<'_, PeerConnection<S>>,
        mut node: futures::lock::MutexGuard<'_, NodeState<S>>,
        mut sibling_guards: Option<PeerOwnerGuards<'_, S>>,
        replay_units: Option<BTreeMap<TxId, SyncMessage>>,
    ) -> bool {
        if !self
            .connections
            .borrow()
            .iter()
            .any(|candidate| Rc::ptr_eq(candidate, connection))
        {
            return false;
        }
        let connection_epoch = connection_ref.connection_epoch;
        if let ConnectionLink::Subscriber(state) = &mut connection_ref.link {
            state.pending_authority_repairs.clear();
        }
        self.current_rows.borrow_mut().disconnect(connection_epoch);
        let upstream_upload_destination = connection_ref.upstream_upload_destination;
        let mut reconnect_permission_advice = Vec::new();
        let mut terminal_permission_advice = Vec::new();
        let current_session_claims = node.session_claims_with_revisions();
        let (authority, upstream_epoch, transferable_uploads, retired_relay_subscriptions) =
            match &mut connection_ref.link {
                ConnectionLink::Upstream(UpstreamConnectionState {
                    expected_scope_authority,
                    large_value_uploads,
                    awaiting_large_value_uploads,
                    pending,
                    scope_lease_manager,
                    ..
                }) => {
                    // Permission-advice futures outlive one transport. Rebuild
                    // their link-local scope bookkeeping on the successor,
                    // one command per live waiter so identical actions can
                    // coalesce under its fresh authority context and wire id.
                    let live_waiters = self.permission_advice_waiters.borrow();
                    let mut queued = BTreeSet::new();
                    // The lease manager becomes authoritative as soon as it
                    // captures a session binding, even if the matching
                    // command is still retained after a backpressured send.
                    // Visit it first so a command's initial `None` cannot
                    // erase the request-owned snapshot during detach.
                    for request in scope_lease_manager.requests.values() {
                        for request_id in &request.waiters {
                            if live_waiters.contains_key(request_id) && queued.insert(*request_id) {
                                let (identity, claims) = &request.session_claim_binding;
                                let current_claims = current_session_claims
                                    .iter()
                                    .find_map(|(current_identity, current_claims, _)| {
                                        (current_identity == identity).then_some(current_claims)
                                    })
                                    .cloned()
                                    .unwrap_or_default();
                                if request.delegated_session.is_some() || current_claims == *claims
                                {
                                    reconnect_permission_advice.push((
                                        *request_id,
                                        request.action.clone(),
                                        Some(request.session_claim_binding.clone()),
                                        request.delegated_session.clone(),
                                    ));
                                } else {
                                    // A successor can only prove its currently
                                    // admitted session. Never send a B-scoped
                                    // hydration to settle this A-owned request.
                                    terminal_permission_advice.push(*request_id);
                                }
                            }
                        }
                    }
                    for command in pending.iter() {
                        let PendingUpstreamCommand::AuthorizationScopeIntent {
                            request_id,
                            action,
                            session_claim_binding,
                            delegated_session,
                        } = command
                        else {
                            continue;
                        };
                        if live_waiters.contains_key(request_id) && queued.insert(*request_id) {
                            if delegated_session.is_some()
                                || session_claim_binding.as_ref().is_none_or(
                                    |(identity, claims)| {
                                        current_session_claims
                                            .iter()
                                            .find_map(|(current_identity, current_claims, _)| {
                                                (current_identity == identity)
                                                    .then_some(current_claims)
                                            })
                                            .cloned()
                                            .unwrap_or_default()
                                            == *claims
                                    },
                                )
                            {
                                reconnect_permission_advice.push((
                                    *request_id,
                                    action.clone(),
                                    session_claim_binding.clone(),
                                    delegated_session.clone(),
                                ));
                            } else {
                                terminal_permission_advice.push(*request_id);
                            }
                        }
                    }
                    (
                        *expected_scope_authority,
                        Some(connection_epoch),
                        Some(peer_connection::take_reconnectable_large_value_uploads(
                            large_value_uploads,
                            awaiting_large_value_uploads,
                        )),
                        Vec::new(),
                    )
                }
                ConnectionLink::Subscriber(SubscriberConnectionState {
                    peer,
                    served,
                    coverage_groups,
                    shape_registrations,
                    scope_purposes,
                    scope_aggregates,
                    authority_scope_hydrations,
                    ..
                }) => {
                    let retired = retire_relay_upstream_subscriptions_for_connection(
                        &self.relay_upstream_subscription_owners,
                        connection_epoch,
                    );
                    // A detached subscriber cannot later send a normal
                    // Unsubscribe. Retire its concrete served usage sites and the
                    // one maintained receiver per coverage group now; groups on
                    // every other downstream connection remain untouched.
                    let groups = std::mem::take(coverage_groups);
                    for (coverage, group) in groups {
                        for subscription in group.subscribers {
                            node.apply_unsubscribe(subscription);
                            served.remove(&subscription);
                            scope_purposes.remove(&subscription);
                        }
                        peer.forget_subscription_with_node(
                            &mut node,
                            coverage_group_subscription_key(&coverage),
                        );
                    }
                    node.release_shapes_for_peer(connection_epoch);
                    shape_registrations.clear();
                    scope_aggregates.clear();
                    authority_scope_hydrations.clear();
                    (None, None, None, retired)
                }
            };
        drop(node);
        // The auxiliary lane is independent of semantic ticks. Retire it for
        // both upstream and subscriber links before releasing this connection
        // so an in-flight local lookup cannot recreate relay state afterward.
        connection_ref.auxiliary_pump.disconnect();
        drop(connection_ref);
        // A rejection can be queued by the upstream turn immediately before
        // this abrupt detach. Its downstream transport is gone, so retaining
        // that queue entry would be an unbounded stale-epoch leak.
        self.pending_relay_subscription_rejections
            .borrow_mut()
            .remove(&connection_epoch);
        let mut connections = self.connections.borrow_mut();
        connections.retain(|candidate| !Rc::ptr_eq(candidate, connection));
        drop(connections);
        let detached = true;
        for request_id in terminal_permission_advice {
            if let Some(waiter) = self
                .permission_advice_waiters
                .borrow_mut()
                .remove(&request_id)
            {
                let _ = waiter.send(PermissionAdvice::Unknown);
            }
        }
        if !retired_relay_subscriptions.is_empty() {
            self.upstream_subscriptions.borrow_mut().extend(
                retired_relay_subscriptions
                    .into_iter()
                    .map(|(subscription, _)| PendingUpstreamCommand::Unsubscribe(subscription)),
            );
            self.schedule_tick(TickUrgency::Immediate);
        }
        if !reconnect_permission_advice.is_empty() {
            self.upstream_subscriptions.borrow_mut().extend(
                reconnect_permission_advice.into_iter().map(
                    |(request_id, action, session_claim_binding, delegated_session)| {
                        PendingUpstreamCommand::AuthorizationScopeIntent {
                            request_id,
                            action,
                            session_claim_binding,
                            delegated_session,
                        }
                    },
                ),
            );
            self.schedule_tick(TickUrgency::Immediate);
        }
        if let (Some(destination), Some(uploads)) =
            (upstream_upload_destination, transferable_uploads)
            && !uploads.is_empty()
        {
            let mut detached_uploads = self.detached_large_value_uploads.borrow_mut();
            let destination_uploads = detached_uploads.entry(destination).or_default();
            peer_connection::merge_reconnectable_large_value_uploads(destination_uploads, uploads);
        }
        if detached
            && let Some(epoch) = upstream_epoch
            && self
                .active_authority_view_receipts
                .borrow()
                .as_ref()
                .is_some_and(|receipts| receipts.connection_epoch == epoch)
        {
            // A parallel upstream that survived the selected link is a new
            // active authority epoch for settlement purposes. Its older
            // receipt was retired by the switch, so it must confirm the view
            // again before cached rows can become settled.
            // Retire B before staging A's queued frames: otherwise applying a
            // row-changing A update during the handoff could briefly publish
            // it under B's now-dead receipt.
            let retired_subscriptions = self
                .active_authority_view_receipts
                .borrow()
                .as_ref()
                .map(|receipts| receipts.subscriptions.clone())
                .unwrap_or_default();
            {
                let mut node = self.node.borrow_mut();
                for subscription in retired_subscriptions {
                    if let Ok(key) = node.authority_result_key_for_subscription(subscription) {
                        node.invalidate_authority_result_settlement(&key);
                    }
                }
            }
            *self.active_authority_view_receipts.borrow_mut() = None;
            let fallback_connection =
                self.connections
                    .borrow()
                    .iter()
                    .rev()
                    .find_map(|connection| {
                        Self::with_detach_peer(connection, &mut sibling_guards, |peer| {
                            matches!(&peer.link, ConnectionLink::Upstream(_))
                        })
                        .then(|| Rc::clone(connection))
                    });
            if let Some(connection) = &fallback_connection {
                Self::with_detach_peer(connection, &mut sibling_guards, |peer| {
                    peer.stage_inbound_without_authority_receipt()
                });
            }
            *self.active_authority_view_receipts.borrow_mut() =
                fallback_connection.map(|connection| AuthorityViewReceipts {
                    connection_epoch: Self::with_detach_peer(
                        &connection,
                        &mut sibling_guards,
                        |peer| peer.connection_epoch,
                    ),
                    confirmation_floor: self.node.borrow().committed_global_time(),
                    subscriptions: BTreeSet::new(),
                    binding_views: BTreeSet::new(),
                });
            // Cached rows remain readable as stale/local state, but their
            // settled receipt died with this authority connection.
            self.demote_authority_receipt_subscriptions();
        }
        if detached && let Some(authority) = authority {
            let mut eligible = self.admitted_upstream_authorities.borrow_mut();
            eligible.retain(|candidate| *candidate != authority);
            if *self.admitted_upstream_authority.borrow() == Some(authority) {
                // Old routes cannot migrate to a replacement authority: they
                // must be rebound explicitly to the deterministic handoff
                // owner. Both upstreams share the upload outbox, so B may
                // already have the unit; clearing this route would strand an
                // Edge-Accepted caller forever.
                let handoff = eligible.first().copied();
                *self.admitted_upstream_authority.borrow_mut() = handoff;
                let mut routes = self.edge_fate_routes.borrow_mut();
                if let Some(handoff) = handoff {
                    routes.retain(|_, obligation| {
                        obligation
                            .routes
                            .retain(|route| route.queue.upgrade().is_some());
                        for route in obligation.routes.iter_mut() {
                            if route.authority == Some(authority) {
                                route.authority = Some(handoff);
                            }
                        }
                        !obligation.routes.is_empty()
                    });
                    let routed_txs = routes.keys().copied().collect::<Vec<_>>();
                    drop(routes);
                    // Re-drive through the successor even when it had sent
                    // the unit before becoming owner. Its per-link uploaded
                    // set is an optimization, never a fate authority token.
                    for candidate in self.connections.borrow().iter() {
                        Self::with_detach_peer(candidate, &mut sibling_guards, |candidate| {
                            let ConnectionLink::Upstream(UpstreamConnectionState {
                                expected_scope_authority,
                                uploaded,
                                outbox,
                                ..
                            }) = &mut candidate.link
                            else {
                                return;
                            };
                            if *expected_scope_authority != Some(handoff) {
                                return;
                            }
                            for tx_id in &routed_txs {
                                uploaded.remove(tx_id);
                                let mut outbox = outbox.borrow_mut();
                                outbox.push(PendingUpload {
                                    tx_id: *tx_id,
                                    unit: match &replay_units {
                                        Some(units) => Some(
                                            units
                                                .get(tx_id)
                                                .expect("preloaded detach replay")
                                                .clone(),
                                        ),
                                        None => crate::db::block_on(
                                            self.node.borrow_mut().commit_unit_for(*tx_id),
                                        )
                                        .ok(),
                                    },
                                });
                            }
                        });
                    }
                    self.schedule_tick(TickUrgency::Immediate);
                } else {
                    // No successor yet: preserve bounded live downstream
                    // routes for a later admitted authority.  Clearing them
                    // after an Edge acceptance would strand the caller.
                    routes.retain(|_, obligation| {
                        obligation
                            .routes
                            .retain(|route| route.queue.upgrade().is_some());
                        for route in obligation.routes.iter_mut() {
                            if route.authority == Some(authority) {
                                route.authority = None;
                            }
                        }
                        !obligation.routes.is_empty()
                    });
                    self.schedule_tick(TickUrgency::Immediate);
                }
            }
        }
        detached
    }

    /// Service every accepted subscriber connection once.
    pub async fn tick(&self) -> Result<DbTickStats, Error> {
        self.drain_transaction_abandonments().await?;
        self.drain_subscription_finalizations().await?;
        self.deliver_pending_mutation_errors();
        let mut stats = DbTickStats::default();
        let progress_waker = self.query_runtime_waker();
        let chunk_completion_generation = self.chunk_resolver.completion_generation();
        if self.local_availability_dirty.replace(false)
            || self.chunk_resolver.has_pending_local_demand()
            || chunk_completion_generation != self.observed_chunk_completion_generation.get()
            || self.node.lock().await.has_pending_query_runtime()
        {
            stats.subscription_events += Box::pin(refresh_subscriptions_in(
                &self.node,
                &self.subscriptions,
                &self.active_authority_view_receipts,
                progress_waker.as_ref(),
            ))
            .await?;
            self.observed_chunk_completion_generation
                .set(chunk_completion_generation);
        }
        let mut remote_sync_applied = false;
        let mut released_outbox_tx_ids = HashSet::new();
        // A later connection can mutate Core state after an earlier subscriber
        // has already had its turn in this pass. Remember that generation so
        // every subscriber is served on a fresh owner turn below. In
        // particular, do not recursively re-enter a just-admitted subscriber:
        // its initial view can suspend on cold storage, while later inbound
        // commit frames and their local fates must still get a turn.
        let subscriber_dirty_epoch_before = self.subscriber_dirty_epoch.get();
        let connections = self.connections.borrow().clone();
        for connection in &connections {
            let mut connection = connection.lock().await;
            // `PeerConnection::tick` contains the subscriber admission state
            // machine. Keep that future off the enclosing Db tick frame so a
            // normal host/test thread cannot accumulate it across connection
            // passes.
            let next = Box::pin(connection.tick()).await?;
            released_outbox_tx_ids.extend(connection.take_released_outbox_tx_ids());
            stats.subscription_events += next.subscription_events;
            stats.remote_sync_applied += next.remote_sync_applied;
            remote_sync_applied |= next.remote_sync_applied > 0;
        }
        let subscriber_state_changed =
            self.subscriber_dirty_epoch.get() != subscriber_dirty_epoch_before;
        if remote_sync_applied || subscriber_state_changed {
            // A binding with a host scheduler can service this newly dirty
            // subscriber on the next owner turn.  A bare `Db` intentionally
            // permits manual driving without installing such a scheduler,
            // though: dropping the wake in that mode would leave the
            // subscriber dirty until unrelated inbound traffic happened to
            // arrive. Preserve the former bounded second serve pass there.
            if self.scheduler.borrow().is_some() {
                for connection in &connections {
                    connection.lock().await.mark_subscriber_dirty();
                }
                self.schedule_tick(TickUrgency::AfterCurrentTurn);
            } else {
                for connection in &connections {
                    let should_tick = {
                        let mut connection = connection.lock().await;
                        connection.mark_subscriber_dirty() || subscriber_state_changed
                    };
                    if should_tick {
                        let mut connection = connection.lock().await;
                        let next = Box::pin(connection.tick()).await?;
                        released_outbox_tx_ids.extend(connection.take_released_outbox_tx_ids());
                        stats.subscription_events += next.subscription_events;
                        stats.remote_sync_applied += next.remote_sync_applied;
                    }
                }
            }
        }
        Box::pin(self.reconcile_scalar_query_inputs()).await?;
        if let Some(budget) = self.edge_cache_budget.get() {
            let mut pins = crate::peer::PeerEvictionPins::default();
            for connection in &connections {
                pins.extend(connection.lock().await.eviction_pins());
            }
            self.node
                .lock()
                .await
                .enforce_edge_cache_budget(&pins, budget)
                .await?;
        }
        if !released_outbox_tx_ids.is_empty() {
            self.release_outbox_uploads(released_outbox_tx_ids);
        }
        Ok(stats)
    }

    /// Both public local-first queries and relay-owned upstream scopes use
    /// ordinary policy-bound point queries to refresh extra scalar inputs.
    async fn reconcile_scalar_query_inputs(&self) -> Result<(), Error> {
        let live = self.subscriptions.borrow().clone();
        for weak in live {
            let Some(owner) = weak.upgrade() else {
                continue;
            };
            let (request, revision, mut reconciliation) = {
                let mut state = owner.borrow_mut();
                if state.closed.get() || !state.scalar_reconciliation_enabled {
                    continue;
                }
                let Some(handle) = state.upstream_subscription_handles.first() else {
                    continue;
                };
                let SubscriptionKind::Prepared { shape, binding, .. } = &state.kind;
                let request = PendingUpstreamSubscription {
                    subscription: handle.subscription,
                    shape: shape.clone(),
                    binding: binding.clone(),
                    opts: handle.coverage.opts.clone(),
                    identity: state.author,
                    policy_binding: Some(state.request_identity_claims.clone().unwrap_or_else(
                        || {
                            (
                                state.author,
                                self.node.borrow().session_claims_for(state.author),
                            )
                        },
                    )),
                };
                (
                    request,
                    state.scalar_authority_revision,
                    std::mem::take(&mut state.scalar_reconciliation),
                )
            };
            Box::pin(self.advance_scalar_reconciliation(
                &request,
                revision,
                &mut reconciliation,
                Some(&owner),
            ))
            .await?;
            let mut state = owner.borrow_mut();
            if !state.closed.get() {
                state.scalar_reconciliation = reconciliation;
            }
        }
        let owners = self
            .relay_upstream_subscription_owners
            .borrow()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        for key in owners {
            let Some((request, revision, mut reconciliation)) = self
                .relay_upstream_subscription_owners
                .borrow_mut()
                .get_mut(&key)
                .map(|owner| {
                    (
                        owner.request.clone(),
                        owner.scalar_authority_revision,
                        std::mem::take(&mut owner.scalar_reconciliation),
                    )
                })
            else {
                continue;
            };
            Box::pin(self.advance_scalar_reconciliation(
                &request,
                revision,
                &mut reconciliation,
                None,
            ))
            .await?;
            if let Some(owner) = self
                .relay_upstream_subscription_owners
                .borrow_mut()
                .get_mut(&key)
            {
                owner.scalar_reconciliation = reconciliation;
            }
        }
        Ok(())
    }

    async fn advance_scalar_reconciliation(
        &self,
        request: &PendingUpstreamSubscription,
        revision: u64,
        state: &mut ScalarReconciliation,
        local_owner: Option<&Rc<RefCell<SubscriptionState>>>,
    ) -> Result<(), Error> {
        if !request.opts.propagate_upstream
            || !request.opts.read_view.is_default()
            || !crate::node::simple_scalar_exit_query(request.shape.query())
        {
            return Ok(());
        }
        let generation = self
            .active_authority_view_receipts
            .borrow()
            .as_ref()
            .filter(|receipt| receipt.subscriptions.contains(&request.subscription))
            .map(|receipt| (receipt.connection_epoch, request.subscription, revision));
        let Some(generation) = generation else {
            *state = ScalarReconciliation::default();
            return Ok(());
        };
        // A new source receipt cannot starve the current bounded batch. It
        // invalidates queued candidates only after the active ordinary probe
        // completes. An authority connection replacement cancels immediately.
        if state
            .generation
            .as_ref()
            .is_some_and(|old| old.0 != generation.0 || old.1 != generation.1)
        {
            *state = ScalarReconciliation::default();
        }
        if let Some(active) = &mut state.active {
            let waker = self.query_runtime_waker();
            let mut cx =
                std::task::Context::from_waker(waker.as_ref().unwrap_or(std::task::Waker::noop()));
            let result = match active.future.as_mut().poll(&mut cx) {
                std::task::Poll::Ready(result) => result,
                std::task::Poll::Pending if web_time::Instant::now() >= active.deadline => {
                    row_availability::CurrentRowsResult::Unknown
                }
                std::task::Poll::Pending => return Ok(()),
            };
            let mut retry_rows = match &result {
                row_availability::CurrentRowsResult::Unknown => active.rows.clone(),
                row_availability::CurrentRowsResult::Applied(receipt) => receipt
                    .rows
                    .iter()
                    .zip(&receipt.outcomes)
                    .filter_map(|(row, outcome)| {
                        (*outcome == crate::protocol::CurrentRowOutcome::Unknown).then_some(row.row)
                    })
                    .collect(),
            };
            state.active = None;
            if let row_availability::CurrentRowsResult::Applied(receipt) = result {
                // The router has correlated this receipt and ingested native
                // carriers. This live original owner additionally gates marker
                // application; raw wire claims are not the derived local key.
                let (identity, claims) = request
                    .policy_binding
                    .clone()
                    .unwrap_or_else(|| (request.identity, BTreeMap::new()));
                if receipt.context
                    != crate::protocol::PolicyBindingKey::from_canonical_parts(identity, claims)
                {
                    *state = ScalarReconciliation::default();
                    return Ok(());
                }
                let mut owner = self.node.lock().await;
                if local_owner.is_some_and(|local| {
                    let local = local.borrow();
                    local.closed.get()
                        || !local
                            .upstream_subscription_handles
                            .iter()
                            .any(|handle| handle.subscription == request.subscription)
                }) || (local_owner.is_none()
                    && !self
                        .relay_upstream_subscription_owners
                        .borrow()
                        .values()
                        .any(|owner| {
                            owner.request.subscription == request.subscription
                                && owner.request.policy_binding == request.policy_binding
                        }))
                    || !self
                        .active_authority_view_receipts
                        .borrow()
                        .as_ref()
                        .is_some_and(|active| {
                            active.connection_epoch == generation.0
                                && active.subscriptions.contains(&request.subscription)
                        })
                {
                    *state = ScalarReconciliation::default();
                    return Ok(());
                }
                let mut node = owner.scoped_active_session_claims(
                    receipt.context.identity,
                    receipt.context.claims().clone(),
                );
                if let Some(scope) = node.local_read_policy_binding(receipt.context.identity) {
                    let mut outcomes = Vec::new();
                    for (row, outcome) in receipt.rows.iter().zip(&receipt.outcomes) {
                        let status = match outcome {
                            crate::protocol::CurrentRowOutcome::Readable => {
                                crate::node::LocalRowAvailability::Readable
                            }
                            crate::protocol::CurrentRowOutcome::CurrentUnavailable => {
                                // Availability constrains settled input. The normal
                                // pending source remains visible without postponing
                                // this receipt or scheduling edit-specific retries.
                                crate::node::LocalRowAvailability::CurrentUnavailable
                            }
                            crate::protocol::CurrentRowOutcome::Unknown => continue,
                        };
                        outcomes.push((row.physical_table, row.row, status));
                    }
                    if !outcomes.is_empty() {
                        node.activate_local_availability_authority(
                            scope.clone(),
                            receipt.core,
                            receipt.core_epoch,
                        )?;
                        let changed = node
                            .apply_verified_local_row_availability(
                                &scope,
                                crate::node::LocalAvailabilityWatermark {
                                    core: receipt.core,
                                    core_epoch: receipt.core_epoch,
                                    claims_revision: receipt.claims_revision,
                                    policy_epoch: receipt.policy_epoch,
                                    settled_through: receipt.settled_through,
                                    authorization_progress: receipt.authorization_progress,
                                },
                                &outcomes,
                            )
                            .await?;
                        if changed {
                            drop(node);
                            drop(owner);
                            self.subscriber_dirty_epoch
                                .set(self.subscriber_dirty_epoch.get().wrapping_add(1));
                            self.local_availability_dirty.set(true);
                            self.schedule_tick(TickUrgency::Immediate);
                        }
                    }
                }
            }
            if retry_rows.is_empty() {
                if state.pending.is_empty() {
                    state.retry_delay_ms = 0;
                }
            } else {
                state.pending.extend(retry_rows.drain(..));
                state.retry_delay_ms = if state.retry_delay_ms == 0 {
                    100
                } else {
                    (state.retry_delay_ms * 2).min(2_000)
                };
                state.retry_at = Some(
                    web_time::Instant::now()
                        + std::time::Duration::from_millis(state.retry_delay_ms),
                );
                if let Some(scheduler) = self.scheduler.borrow().as_ref() {
                    scheduler.schedule_tick_after(state.retry_delay_ms);
                }
            }
        }
        if let Some(at) = state.retry_at {
            if web_time::Instant::now() < at {
                return Ok(());
            }
            state.retry_at = None;
        }
        if state.generation.as_ref() != Some(&generation) && state.pending.is_empty() {
            let mut owner = self.node.lock().await;
            let table = &request.shape.query().table;
            let key = owner.authority_result_key_for_subscription(request.subscription)?;
            if !owner.has_settled_authority_result(&key)
                || owner.opening_pending_for_authority_result(&key)
                || owner.publication_deferred_for_authority_result(&key)
                || owner.authority_source_closure_generation(&key).is_none()
            {
                return Ok(());
            }
            let authoritative = owner.scalar_authority_input_rows(&key, table);
            let claims = request
                .policy_binding
                .as_ref()
                .map(|(_, claims)| claims.clone());
            let mut node = owner.scoped_optional_session_claims(request.identity, claims);
            // Public local-first output is the candidate inventory, even when
            // cached policy proofs no longer admit those retained inputs.
            let local = if let Some(local_owner) = local_owner {
                let local = local_owner.borrow();
                local
                    .snapshot
                    .rows
                    .iter()
                    .take(local.snapshot.root_count)
                    .map(|row| row.row_uuid())
                    .collect::<Vec<_>>()
            } else {
                // This is a local candidate inventory, not a serving answer.
                // Re-evaluating cached permission rules here can both omit
                // stale rows needing revalidation and wait on query work that
                // this same owner pass must drive. Core authorizes the later
                // CurrentRows request; discovery uses ordinary client-local reads.
                node.query_rows_for_client(
                    &request.shape,
                    &request.binding,
                    DurabilityTier::Local,
                    request.identity,
                )
                .await?
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<Vec<_>>()
            };
            let mut candidates = local
                .into_iter()
                .filter(|row| !authoritative.contains(row))
                .collect::<BTreeSet<_>>();
            if let Some(scope) = node.local_read_policy_binding(request.identity) {
                if let Ok(table_id) =
                    node.local_availability_table_id(request.shape.schema_version(), table)
                {
                    // A fresh inclusion must revalidate a hidden row as well;
                    // otherwise the local exclusion would prevent readmission.
                    candidates.extend(
                        authoritative
                            .into_iter()
                            .filter(|row| node.is_local_row_unavailable(&scope, table_id, *row)),
                    );
                }
            }
            state.pending = candidates.into_iter().collect();
            state.generation = Some(generation);
        }
        let count = state.pending.len().min(64);
        let batch = state.pending.drain(..count).collect::<Vec<_>>();
        if batch.is_empty() {
            return Ok(());
        }
        let mut node = self.node.lock().await;
        let table = &request.shape.query().table;
        let mut candidates = Vec::new();
        for row in batch {
            let Some(tx) = node.local_content_winner_tx_id(table, row).await? else {
                continue;
            };
            if node.transaction_record(tx).await.is_some_and(|record| {
                matches!(record.fate, crate::tx::Fate::Accepted)
                    && record.durability >= DurabilityTier::Edge
            }) {
                if let Ok(coordinate) = node.current_row_coordinate(table, row) {
                    candidates.push(coordinate);
                }
            }
        }
        drop(node);
        if !candidates.is_empty() {
            let (identity, claims) = request
                .policy_binding
                .clone()
                .unwrap_or_else(|| (request.identity, BTreeMap::new()));
            let context = crate::protocol::PolicyBindingKey {
                identity,
                canonical_claims: crate::protocol::CanonicalPolicyClaims::new(claims),
            };
            state.active = Some(ScalarProbe {
                deadline: web_time::Instant::now() + std::time::Duration::from_secs(5),
                rows: candidates.iter().map(|row| row.row).collect(),
                future: Box::pin(self.request_current_rows(candidates, context)),
            });
            if let Some(scheduler) = self.scheduler.borrow().as_ref() {
                scheduler.schedule_tick_after(5_000);
            }
            self.schedule_tick(TickUrgency::Immediate);
        } else if !state.pending.is_empty() {
            self.schedule_tick(TickUrgency::AfterCurrentTurn);
        }
        Ok(())
    }

    fn release_outbox_uploads(&self, released_tx_ids: HashSet<TxId>) {
        let mut outbox = self.outbox.borrow_mut();
        let mut remaining = released_tx_ids.clone();
        let released_tx_ids = outbox.remove_released(&mut remaining);
        drop(outbox);
        for connection in self.connections.borrow().iter() {
            connection
                .borrow_mut()
                .forget_released_outbox_tx_ids(&released_tx_ids);
        }
        self.large_value_upload_retry_deadlines
            .borrow_mut()
            .retain(|tx_id, _| !released_tx_ids.contains(tx_id));
        self.detached_large_value_uploads
            .borrow_mut()
            .retain(|_, uploads| {
                uploads.retain(|tx_id, _| !released_tx_ids.contains(tx_id));
                !uploads.is_empty()
            });
    }
}

/// Temporarily owns a maintained Groove handle while subscription refresh does
/// asynchronous work. Restoring on drop keeps every `?`/`continue` exit safe
/// without lending the subscription's `RefCell` contents across an await.
struct DetachedSubscriptionRefresh {
    state: Rc<RefCell<SubscriptionState>>,
    maintained: Option<LocalMaintainedViewSubscription>,
    snapshot: RelationSnapshot,
    snapshot_index: RelationSnapshotIndex,
    snapshot_source: SubscriptionSnapshotSource,
    settled: bool,
    sender: SubscriptionSender,
    local_subscription_cleanup: Rc<Cell<Option<(u64, groove::ivm::SubscriptionId)>>>,
}

impl DetachedSubscriptionRefresh {
    fn new(state: &Rc<RefCell<SubscriptionState>>) -> Self {
        let mut state_ref = state.borrow_mut();
        let SubscriptionKind::Prepared {
            maintained_subscription,
            ..
        } = &mut state_ref.kind;
        let maintained = maintained_subscription.take();
        let snapshot = std::mem::take(&mut state_ref.snapshot);
        let snapshot_index = std::mem::take(&mut state_ref.snapshot_index);
        Self {
            state: Rc::clone(state),
            maintained,
            snapshot,
            snapshot_index,
            snapshot_source: state_ref.snapshot_source,
            settled: state_ref.settled,
            sender: state_ref.sender.clone(),
            local_subscription_cleanup: Rc::clone(&state_ref.local_subscription_cleanup),
        }
    }
}

impl Drop for DetachedSubscriptionRefresh {
    fn drop(&mut self) {
        let mut state_ref = self.state.borrow_mut();
        let SubscriptionKind::Prepared {
            maintained_subscription,
            ..
        } = &mut state_ref.kind;
        debug_assert!(maintained_subscription.is_none());
        *maintained_subscription = self.maintained.take();
        state_ref.snapshot = std::mem::take(&mut self.snapshot);
        state_ref.snapshot_index = std::mem::take(&mut self.snapshot_index);
        state_ref.snapshot_source = self.snapshot_source;
        state_ref.settled = self.settled;
    }
}

/// Resolve a public stream's authority lifecycle from its own registered wire
/// usage site. A canonical binding view is not sufficient here: two delegated
/// sessions may legitimately share it while receiving independent resets.
fn authority_result_key_for_stream<S>(
    node: &NodeState<S>,
    handles: &[UpstreamCoverageHandle],
    binding_view: BindingViewKey,
) -> Option<crate::protocol::AuthorityResultKey>
where
    S: OrderedKvStorage,
{
    let mut keys = handles
        .iter()
        .filter_map(|handle| {
            node.authority_result_key_for_subscription(handle.subscription)
                .ok()
        })
        .filter(|key| key.binding_view == binding_view);
    let key = keys.next()?;
    keys.all(|candidate| candidate == key).then_some(key)
}

/// Re-evaluate every live subscription against the node and push a delta event
/// for any whose rows changed. Shared by local writes
/// ([`Db::refresh_subscriptions`]) and by inbound sync application
/// ([`PeerConnection::tick`]).
pub(super) async fn refresh_subscriptions_in<S>(
    node: &SharedNodeState<S>,
    subscriptions: &SubscriptionList,
    active_authority_view_receipts: &ActiveAuthorityViewReceipts,
    progress_waker: Option<&Waker>,
) -> Result<usize, Error>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut retained = Vec::new();
    let mut changed = 0;
    let pending_authoritative_resets = node.lock().await.take_pending_authoritative_resets();
    let mut consumed_authoritative_resets = BTreeSet::new();
    node.lock()
        .await
        .drive_ready_query_runtime_with_waker(progress_waker)
        .await?;
    let live_subscriptions = subscriptions.borrow().clone();
    for weak in &live_subscriptions {
        let Some(state) = weak.upgrade() else {
            continue;
        };
        // Finalization flips this synchronously, before awaiting the node
        // mutex. Never rehydrate a stream whose cleanup is merely waiting for
        // this tick to acquire that mutex.
        if state.borrow().closed.get() {
            continue;
        }
        let (
            read_tier,
            pending_overlay,
            remote_read_tier,
            requires_authority_receipt,
            remote_propagate_upstream,
            read_view,
            previous_source,
            previous_settled,
            author,
            authorization_mode,
            terminal_rows,
            upstream_subscription_handles,
        ) = {
            let state = state.borrow();
            (
                state.read_tier,
                state.pending_overlay,
                state.remote_read_tier,
                state.requires_authority_receipt,
                state.remote_propagate_upstream,
                state.read_view.clone(),
                state.snapshot_source,
                state.settled,
                state.author,
                state.authorization_mode,
                state.terminal_rows,
                state.upstream_subscription_handles.clone(),
            )
        };
        let request_claims = state
            .borrow()
            .request_identity_claims
            .as_ref()
            .map(|(_, claims)| claims.clone());
        let groove_runtime_token = node.lock().await.groove_runtime_token();
        if state.borrow().groove_runtime_token != groove_runtime_token {
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=reopen_runtime stale={} current={}",
                    state.borrow().groove_runtime_token,
                    groove_runtime_token,
                );
            }
            let (shape, binding) = {
                let state = state.borrow();
                match &state.kind {
                    SubscriptionKind::Prepared { shape, binding, .. } => {
                        (shape.clone(), binding.clone())
                    }
                }
            };
            let stale_subscription_id = {
                let state = state.borrow();
                match &state.kind {
                    SubscriptionKind::Prepared {
                        maintained_subscription,
                        ..
                    } => maintained_subscription
                        .as_ref()
                        .map(LocalMaintainedViewSubscription::subscription_id),
                }
            };
            // The Jazz runtime token invalidates prepared plans, while the
            // Groove runtime itself remains alive. Retire the old maintained
            // handle before installing its replacement so two descriptor
            // generations cannot consume the next physical delta.
            if let Some(subscription_id) = stale_subscription_id {
                node.lock()
                    .await
                    .unsubscribe_groove_subscription(subscription_id);
            }
            let (shape, binding, prepared_plan) = {
                let mut owner = node.lock().await;
                let mut scoped =
                    owner.scoped_optional_session_claims(author, request_claims.clone());
                scoped
                    .prepare_query_binding_for_link_in_authorization_mode(
                        &shape,
                        &binding,
                        read_tier,
                        author,
                        authorization_mode,
                    )
                    .await?
            };
            let (previous_snapshot, previous_snapshot_index) = {
                let state_ref = state.borrow();
                (
                    materialized_subscription_snapshot(
                        &state_ref.snapshot,
                        &state_ref.snapshot_index,
                    )?,
                    state_ref.snapshot_index.clone(),
                )
            };
            let (maintained, mut snapshot) = {
                let mut owner = node.lock().await;
                let mut scoped =
                    owner.scoped_optional_session_claims(author, request_claims.clone());
                scoped
                    .open_maintained_view_subscription_in_authorization_mode_with_waker(
                        &shape,
                        &binding,
                        author,
                        read_tier,
                        &read_view,
                        Some(prepared_plan),
                        authorization_mode,
                        pending_overlay,
                        progress_waker,
                    )
                    .await?
            };
            let replacement_is_cold = !maintained.initial_snapshot_received();
            if state.borrow().closed.get() {
                node.lock()
                    .await
                    .unsubscribe_groove_subscription(maintained.subscription_id());
                continue;
            }
            let delivered_binding_view = BindingViewKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions {
                    tier: read_tier,
                    read_view: read_view.clone(),
                    ..RegisterShapeOptions::default()
                }
                .read_view_key(),
            };
            // From this point, cleanup must target the replacement runtime
            // subscription even if a later fallible refresh step aborts.
            {
                let mut state_ref = state.borrow_mut();
                let subscription_id = maintained.subscription_id();
                match &mut state_ref.kind {
                    SubscriptionKind::Prepared {
                        maintained_subscription,
                        ..
                    } => *maintained_subscription = Some(maintained),
                }
                state_ref
                    .local_subscription_cleanup
                    .set(Some((groove_runtime_token, subscription_id)));
                state_ref.cold_runtime_replacement = replacement_is_cold;
                if replacement_is_cold {
                    // Own the replacement before yielding its cold initial batch;
                    // otherwise the next owner turn would retire and reopen it.
                    state_ref.groove_runtime_token = groove_runtime_token;
                }
            }
            if replacement_is_cold {
                // Opening a new maintained graph is intentionally nonblocking:
                // an empty cold materialization is pending local IVM work, not
                // a replacement result. The first terminal batch below emits
                // one reset from the previously published facade.
                retained.push(Rc::downgrade(&state));
                continue;
            }
            let settled_tier = remote_read_tier.unwrap_or(read_tier);
            let settled_binding_view = BindingViewKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions {
                    tier: settled_tier,
                    read_view: read_view.clone(),
                    propagate_upstream: remote_propagate_upstream,
                    ..RegisterShapeOptions::default()
                }
                .read_view_key(),
            };
            let delivered_authority_result = authority_result_key_for_stream(
                &node.borrow(),
                &upstream_subscription_handles,
                delivered_binding_view,
            );
            let settled_authority_result = authority_result_key_for_stream(
                &node.borrow(),
                &upstream_subscription_handles,
                settled_binding_view,
            );
            let pending_authority_result = delivered_authority_result
                .as_ref()
                .filter(|key| pending_authoritative_resets.contains(*key))
                .cloned()
                .or_else(|| {
                    settled_authority_result
                        .as_ref()
                        .filter(|key| pending_authoritative_resets.contains(*key))
                        .cloned()
                });
            let local_overlay_row_keys = BTreeSet::new();
            if let Some(authority_result_key) = pending_authority_result {
                // A client receiver only ever advances from the exact
                // CoveredInput closure into its own maintained terminal. A
                // receipt with no compiler-owned source registry remains
                // pending; authority result members and payloads are not an
                // alternate result representation.
                let mut maintained = {
                    let mut state_ref = state.borrow_mut();
                    let SubscriptionKind::Prepared {
                        maintained_subscription,
                        ..
                    } = &mut state_ref.kind;
                    maintained_subscription
                        .take()
                        .expect("replacement maintained subscription installed")
                };
                let mut node_ref = node.lock().await;
                // A stream may be cancelled while this refresh waits for the
                // node owner. Its queued finalizer can retire the exact local
                // subscription before this drain acquires that owner.
                if state.borrow().closed.get() {
                    drop(node_ref);
                    let mut state_ref = state.borrow_mut();
                    let SubscriptionKind::Prepared {
                        maintained_subscription,
                        ..
                    } = &mut state_ref.kind;
                    *maintained_subscription = Some(maintained);
                    continue;
                }
                let drained = node_ref
                    .drain_local_maintained_view_subscription_preserving_rows_with_waker(
                        &mut maintained,
                        Some(authority_result_key.clone()),
                        &local_overlay_row_keys,
                        progress_waker,
                    )
                    .await;
                {
                    let mut state_ref = state.borrow_mut();
                    let SubscriptionKind::Prepared {
                        maintained_subscription,
                        ..
                    } = &mut state_ref.kind;
                    *maintained_subscription = Some(maintained);
                }
                let (update, _) = drained?;
                if let Some(update) = update {
                    let state_ref = state.borrow();
                    let SubscriptionKind::Prepared {
                        maintained_subscription,
                        ..
                    } = &state_ref.kind;
                    let terminal_layout = maintained_subscription
                        .as_ref()
                        .and_then(LocalMaintainedViewSubscription::terminal_root_layout);
                    let mut snapshot_index = RelationSnapshotIndex::from_snapshot(&snapshot);
                    let _ = apply_maintained_update_to_snapshot(
                        &mut snapshot,
                        &mut snapshot_index,
                        update,
                        shape.query().table.as_str(),
                        read_tier,
                        previous_settled,
                        terminal_layout,
                    )?;
                    materialize_subscription_terminal_records(&mut snapshot, &snapshot_index)?;
                    consumed_authoritative_resets.insert(authority_result_key);
                }
            }
            let root_occurrence_ids = if shape.query().aggregate.is_some() || terminal_rows {
                // A fresh compiler-owned root collector has already produced
                // this reset snapshot. Pair its roots directly rather than
                // reconstructing positions from membership state.
                snapshot
                    .rows
                    .iter()
                    .take(snapshot.root_count)
                    .map(|row| {
                        crate::tools::OutputOccurrenceId::single_source(
                            crate::tools::ObjectId::from_uuid(row.row_uuid().0),
                        )
                    })
                    .collect()
            } else {
                let state_ref = state.borrow();
                let SubscriptionKind::Prepared {
                    maintained_subscription,
                    ..
                } = &state_ref.kind;
                maintained_subscription
                    .as_ref()
                    .expect("replacement maintained subscription installed")
                    .root_occurrence_ids()
                    .to_vec()
            };
            let settled = subscription_is_settled(
                &node.borrow(),
                active_authority_view_receipts,
                &shape,
                &binding,
                settled_tier,
                read_view.clone(),
                remote_propagate_upstream,
                requires_authority_receipt,
                settled_authority_result.as_ref(),
            );
            let mut event = subscription_delta_event_with_reset(
                read_tier,
                settled,
                &previous_snapshot,
                &snapshot,
                false,
                terminal_rows,
            );
            if let SubscriptionEvent::Delta {
                reset,
                publishable,
                added,
                updated,
                removed,
                ..
            } = &mut event
            {
                *reset = true;
                *added =
                    subscription_outputs_with_occurrence_sidecar(&snapshot, &root_occurrence_ids)?;
                updated.clear();
                *removed = reset_removed_roots(
                    &previous_snapshot,
                    &previous_snapshot_index,
                    &root_occurrence_ids,
                );
                *publishable = settled || !added.is_empty() || !removed.is_empty();
            }
            let mut state_ref = state.borrow_mut();
            let publication_before = state_ref.sender.checkpoint(
                read_tier,
                settled,
                &state_ref.snapshot,
                &state_ref.snapshot_index,
            )?;
            state_ref.groove_runtime_token = groove_runtime_token;
            state_ref.snapshot = relation_snapshot_with_delta_slack(&snapshot);
            state_ref.snapshot_index = RelationSnapshotIndex::from_snapshot(&state_ref.snapshot);
            state_ref.snapshot_index.roots = root_occurrence_ids
                .into_iter()
                .enumerate()
                .map(|(index, occurrence)| (occurrence, index))
                .collect();
            let SubscriptionKind::Prepared {
                maintained_subscription,
                ..
            } = &state_ref.kind;
            state_ref.snapshot_index.terminal_records = maintained_subscription
                .as_ref()
                .map(LocalMaintainedViewSubscription::decoded_terminal_records)
                .transpose()?
                .unwrap_or_default();
            state_ref.snapshot_source = SubscriptionSnapshotSource::LocalMaintained;
            state_ref.settled = settled;
            // A strict remote opening is intentionally absent until its
            // exact covered closure has reached the receiver-local terminal.
            // Do not enqueue that provisional frame merely for the stream
            // facade to discard later: raw/poll consumers must not observe a
            // stale empty opening before the authoritative reset.
            let materialized =
                state_ref
                    .sender
                    .materialized(&node.borrow(), shape.query(), &event)?;
            if state_ref.sender.publish(
                event,
                publication_before,
                &state_ref.snapshot,
                &state_ref.snapshot_index,
                materialized,
            )? {
                changed += 1;
            }
            drop(state_ref);
            retained.push(Rc::downgrade(&state));
            continue;
        }
        let cold_runtime_replacement_pending = state.borrow().cold_runtime_replacement;
        let (mut snapshot, mut snapshot_source, settled, snapshot_tier, force_reset_event) = {
            let mut refresh = DetachedSubscriptionRefresh::new(&state);
            #[cfg(test)]
            wait_for_subscription_refresh_detach_for_test().await;
            let (shape, binding) = {
                let state_ref = state.borrow();
                let SubscriptionKind::Prepared { shape, binding, .. } = &state_ref.kind;
                (shape.clone(), binding.clone())
            };
            let has_maintained_subscription = refresh.maintained.is_some();
            let settled_tier = remote_read_tier.unwrap_or(read_tier);
            let settled_binding_view = BindingViewKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions {
                    tier: settled_tier,
                    read_view: read_view.clone(),
                    propagate_upstream: remote_propagate_upstream,
                    ..RegisterShapeOptions::default()
                }
                .read_view_key(),
            };
            let delivered_binding_view = BindingViewKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions {
                    tier: read_tier,
                    read_view: read_view.clone(),
                    ..RegisterShapeOptions::default()
                }
                .read_view_key(),
            };
            let delivered_authority_result = authority_result_key_for_stream(
                &node.borrow(),
                &upstream_subscription_handles,
                delivered_binding_view,
            );
            let settled_authority_result = authority_result_key_for_stream(
                &node.borrow(),
                &upstream_subscription_handles,
                settled_binding_view,
            );
            let remote_settled_tier = remote_read_tier.filter(|_| {
                settled_authority_result
                    .as_ref()
                    .is_some_and(|key| node.borrow().has_settled_authority_result(key))
            });
            let authoritative_reset_result = delivered_authority_result
                .as_ref()
                .filter(|key| pending_authoritative_resets.contains(*key))
                .cloned()
                .or_else(|| {
                    settled_authority_result
                        .as_ref()
                        .filter(|key| pending_authoritative_resets.contains(*key))
                        .cloned()
                });
            // A propagation receipt is not a replacement input frontier for
            // local-first. Its ordinary local graph observes synced storage
            // changes, and must keep draining even when remote scope changes.
            // Observation tier, not the caller's authorization host, decides
            // whether the remote closure is this stream's input frontier. A
            // trusted backend may still request Local: it evaluates its own
            // storage-backed graph and merely propagates upstream, exactly
            // like any other Local reader.
            let authority_scoped = read_tier >= DurabilityTier::Edge;
            let authoritative_reset_pending =
                authority_scoped && authoritative_reset_result.is_some();
            // Optimistic local writes are represented as eligible local input
            // records in the receiver graph. They are not reconciled against
            // authority output membership at the facade boundary.
            let local_overlay_row_keys = BTreeSet::new();
            if settled_authority_result
                .as_ref()
                .is_some_and(|key| node.borrow().publication_deferred_for_authority_result(key))
            {
                if let Some(key) = authoritative_reset_result.as_ref() {
                    node.borrow_mut().defer_authoritative_reset(key);
                }
                retained.push(Rc::downgrade(&state));
                continue;
            }
            let snapshot_tier = remote_settled_tier.unwrap_or(read_tier);
            let authoritative_reset = authoritative_reset_pending;
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=refresh terminal_rows={terminal_rows} covered={} reset_pending={authoritative_reset_pending} authoritative_reset={authoritative_reset} delivered={delivered_authority_result:?} settled={settled_authority_result:?}",
                    refresh
                        .maintained
                        .as_ref()
                        .is_some_and(LocalMaintainedViewSubscription::has_covered_input_sources),
                );
            }
            if authoritative_reset
                && terminal_rows
                && refresh
                    .maintained
                    .as_ref()
                    .is_none_or(|maintained| !maintained.has_covered_input_sources())
            {
                // A client receiver may only reset after it has installed the
                // exact scoped covered-input closure. Reopening a fresh local
                // terminal here would make the authority reset appear settled
                // without that receipt, reviving the retired result-snapshot
                // path indirectly.
                if authorization_mode == QueryAuthorizationMode::ClientLocal
                    && remote_read_tier.is_some()
                {
                    if let Some(key) = authoritative_reset_result.as_ref() {
                        node.borrow_mut().defer_authoritative_reset(key);
                    }
                    retained.push(Rc::downgrade(&state));
                    continue;
                }
                let Some(maintained) = refresh.maintained.as_mut() else {
                    return Err(Error::new(
                        ErrorCode::Protocol,
                        "structured subscription lost its Groove terminal",
                    ));
                };
                let stale_subscription_id = maintained.subscription_id();
                // A structural-patch stream deliberately does not keep
                // facade-level replacement rows current. Re-open the
                // Groove terminal at an authoritative boundary so the
                // reset is a fresh complete value and subsequent FIFO
                // patches are relative to exactly that value.
                let (replacement, snapshot) = node
                    .lock()
                    .await
                    .open_maintained_view_subscription_in_authorization_mode_with_waker(
                        &shape,
                        &binding,
                        author,
                        read_tier,
                        &read_view,
                        None,
                        authorization_mode,
                        pending_overlay,
                        progress_waker,
                    )
                    .await?;
                let replacement_subscription_id = replacement.subscription_id();
                node.lock()
                    .await
                    .unsubscribe_groove_subscription(stale_subscription_id);
                *maintained = replacement;
                refresh
                    .local_subscription_cleanup
                    .set(Some((groove_runtime_token, replacement_subscription_id)));
                let settled = subscription_is_settled(
                    &node.borrow(),
                    active_authority_view_receipts,
                    &shape,
                    &binding,
                    settled_tier,
                    read_view,
                    remote_propagate_upstream,
                    requires_authority_receipt,
                    settled_authority_result.as_ref(),
                );
                (
                    snapshot,
                    SubscriptionSnapshotSource::LocalMaintained,
                    settled,
                    snapshot_tier,
                    true,
                )
            } else {
                let (maintained_update, suppressed_authoritative_change) = if let Some(maintained) =
                    refresh.maintained.as_mut()
                {
                    let mut node_ref = node.lock().await;
                    // Finalization may have run while this refresh waited for
                    // the owner. Do not drain the detached receiver after its
                    // stream synchronously marked itself closed.
                    if state.borrow().closed.get() {
                        continue;
                    }
                    // Only a scope-bound source consumes authority inputs.
                    // Local-first remains an ordinary storage-backed graph.
                    let authoritative_result_key = (authorization_mode
                        == QueryAuthorizationMode::ClientLocal
                        && maintained.has_covered_input_sources())
                    .then(|| settled_authority_result.clone())
                    .flatten();
                    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                        eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=runtime_drain sources={} authority={authoritative_result_key:?}",
                            maintained.has_covered_input_sources(),
                        );
                    }
                    match node_ref
                        .drain_local_maintained_view_subscription_preserving_rows_with_waker(
                            maintained,
                            authoritative_result_key,
                            &local_overlay_row_keys,
                            progress_waker,
                        )
                        .await
                    {
                        Ok(update) => update,
                        Err(crate::node::Error::MissingTransaction(_)) => {
                            node_ref.record_authoritative_reset_missing_payload_fallback();
                            if let Some(key) = authoritative_reset_result.as_ref() {
                                node_ref.defer_authoritative_reset(key);
                            }
                            retained.push(Rc::downgrade(&state));
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    }
                } else {
                    (None, false)
                };
                // A reset claims an authority successor, but it is publishable
                // only after the receiver has installed that exact closure
                // and drained the same local graph. `Pending` is not empty:
                // retain the receipt until its ProgramSourceCoverage manifest
                // is available, rather than emitting the graph's opening
                // state as a strict remote answer.
                if authoritative_reset
                    && authorization_mode == QueryAuthorizationMode::ClientLocal
                    && remote_read_tier.is_some()
                    && let Some(authority_result_key) = authoritative_reset_result.as_ref()
                {
                    let closure_installed = refresh.maintained.as_ref().is_some_and(|maintained| {
                        maintained.has_covered_input_sources()
                            && node
                                .borrow()
                                .authority_source_closure_generation(authority_result_key)
                                .is_some_and(|generation| {
                                    maintained.has_installed_covered_closure(
                                        authority_result_key,
                                        generation,
                                    )
                                })
                    });
                    if !closure_installed {
                        // Missing source descriptors and incomplete coverage
                        // are both pending protocol state, never permission to
                        // reuse the authority's result set or payload.
                        node.borrow_mut()
                            .defer_authoritative_reset(authority_result_key);
                        retained.push(Rc::downgrade(&state));
                        continue;
                    }
                }
                if let Some(key) = authoritative_reset_result.as_ref() {
                    consumed_authoritative_resets.insert(key.clone());
                }
                if cold_runtime_replacement_pending {
                    let replacement_ready = refresh
                        .maintained
                        .as_ref()
                        .is_some_and(LocalMaintainedViewSubscription::initial_snapshot_received);
                    if !replacement_ready {
                        retained.push(Rc::downgrade(&state));
                        continue;
                    }
                    let previous = materialized_subscription_snapshot(
                        &refresh.snapshot,
                        &refresh.snapshot_index,
                    )?;
                    let previous_snapshot_index = refresh.snapshot_index.clone();
                    let maintained = refresh
                        .maintained
                        .take()
                        .expect("cold runtime replacement kept its maintained subscription");
                    let materialized = node
                        .lock()
                        .await
                        .materialize_local_maintained_relation_snapshot_with_occurrences(
                            &maintained,
                        )
                        .await;
                    refresh.maintained = Some(maintained);
                    let materialized = materialized?;
                    let publication_before = refresh.sender.checkpoint(
                        read_tier,
                        false,
                        &refresh.snapshot,
                        &refresh.snapshot_index,
                    )?;
                    let replacement = materialized.snapshot;
                    refresh.snapshot = relation_snapshot_with_delta_slack(&replacement);
                    refresh.snapshot_index = relation_snapshot_index_with_root_occurrences(
                        &refresh.snapshot,
                        &materialized.root_occurrence_ids,
                    )?;
                    refresh.snapshot_index.terminal_records = refresh
                        .maintained
                        .as_ref()
                        .expect("cold runtime replacement restored maintained subscription")
                        .decoded_terminal_records()?;
                    let settled = subscription_is_settled(
                        &node.borrow(),
                        active_authority_view_receipts,
                        &shape,
                        &binding,
                        settled_tier,
                        read_view,
                        remote_propagate_upstream,
                        requires_authority_receipt,
                        settled_authority_result.as_ref(),
                    );
                    refresh.snapshot_source = SubscriptionSnapshotSource::LocalMaintained;
                    refresh.settled = settled;
                    state.borrow_mut().cold_runtime_replacement = false;
                    let mut event = subscription_delta_event_with_reset(
                        snapshot_tier,
                        settled,
                        &previous,
                        &replacement,
                        true,
                        terminal_rows,
                    );
                    if let SubscriptionEvent::Delta {
                        added,
                        updated,
                        removed,
                        ..
                    } = &mut event
                    {
                        *added = subscription_outputs_with_occurrence_sidecar(
                            &replacement,
                            &materialized.root_occurrence_ids,
                        )?;
                        updated.clear();
                        *removed = reset_removed_roots(
                            &previous,
                            &previous_snapshot_index,
                            &materialized.root_occurrence_ids,
                        );
                    }
                    retained.push(Rc::downgrade(&state));
                    let materialized =
                        refresh
                            .sender
                            .materialized(&node.borrow(), shape.query(), &event)?;
                    if refresh.sender.publish(
                        event,
                        publication_before,
                        &refresh.snapshot,
                        &refresh.snapshot_index,
                        materialized,
                    )? {
                        changed += 1;
                    }
                    continue;
                }
                if let Some(update) = maintained_update {
                    match update {
                        LocalMaintainedViewSubscriptionUpdate::Structured {
                            terminal_operations,
                        } => {
                            if !terminal_operations.is_empty() {
                                let settled = subscription_is_settled(
                                    &node.borrow(),
                                    active_authority_view_receipts,
                                    &shape,
                                    &binding,
                                    settled_tier,
                                    read_view,
                                    remote_propagate_upstream,
                                    requires_authority_receipt,
                                    settled_authority_result.as_ref(),
                                );
                                let terminal_layout = refresh
                                    .maintained
                                    .as_ref()
                                    .and_then(LocalMaintainedViewSubscription::terminal_root_layout)
                                    .ok_or_else(|| {
                                        Error::new(
                                            ErrorCode::Protocol,
                                            "terminal operation arrived without a prepared root layout",
                                        )
                                    })?;
                                let terminal_operation_count = terminal_operations.len();
                                if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                                    eprintln!(
                                        "JAZZ_COVERED_INPUT_TRACE stage=terminal_ops count={terminal_operation_count} reset={authoritative_reset}"
                                    );
                                }
                                let previous_snapshot = authoritative_reset
                                    .then(|| {
                                        materialized_subscription_snapshot(
                                            &refresh.snapshot,
                                            &refresh.snapshot_index,
                                        )
                                    })
                                    .transpose()?;
                                let publication_before = refresh.sender.checkpoint(
                                    read_tier,
                                    settled,
                                    &refresh.snapshot,
                                    &refresh.snapshot_index,
                                )?;
                                let event = apply_terminal_operations_to_subscription_snapshot(
                                    &mut refresh.snapshot,
                                    &mut refresh.snapshot_index,
                                    terminal_operations,
                                    None,
                                    terminal_layout,
                                    shape.query().table.as_str(),
                                    snapshot_tier,
                                    settled,
                                )?;
                                // A newly received authority closure is a
                                // reset boundary.  Its complete value comes
                                // from the same receiver-local terminal
                                // reducer just advanced above, never from an
                                // authority result snapshot or a re-run
                                // query.  Later local/covered updates remain
                                // incremental terminal operations.
                                let event = if authoritative_reset {
                                    materialize_subscription_terminal_records(
                                        &mut refresh.snapshot,
                                        &refresh.snapshot_index,
                                    )?;
                                    subscription_delta_event_with_reset(
                                        snapshot_tier,
                                        settled,
                                        previous_snapshot
                                            .as_ref()
                                            .expect("reset snapshot captured above"),
                                        &refresh.snapshot,
                                        true,
                                        terminal_rows,
                                    )
                                } else {
                                    event
                                };
                                if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                                    eprintln!(
                                        "JAZZ_COVERED_INPUT_TRACE stage=publish_terminal ops={} roots={}",
                                        terminal_operation_count, refresh.snapshot.root_count,
                                    );
                                }
                                refresh.settled = settled;
                                retained.push(Rc::downgrade(&state));
                                let materialized = refresh.sender.materialized(
                                    &node.borrow(),
                                    shape.query(),
                                    &event,
                                )?;
                                if refresh.sender.publish(
                                    event,
                                    publication_before,
                                    &refresh.snapshot,
                                    &refresh.snapshot_index,
                                    materialized,
                                )? {
                                    changed += 1;
                                }
                                continue;
                            }
                            // No terminal delta means the receiver-local
                            // collector state did not change. In particular,
                            // do not materialize a replacement from Jazz
                            // storage here: the only authority input is the
                            // covered source closure above. The common reset
                            // publication below handles an unchanged but
                            // newly complete closure.
                        }
                        update @ (LocalMaintainedViewSubscriptionUpdate::Flat { .. }
                        | LocalMaintainedViewSubscriptionUpdate::AggregateWindow {
                            ..
                        }) => {
                            let state_ref = &mut refresh;
                            let publication_before = state_ref.sender.checkpoint(
                                read_tier,
                                false,
                                &state_ref.snapshot,
                                &state_ref.snapshot_index,
                            )?;
                            let previous_snapshot = materialized_subscription_snapshot(
                                &state_ref.snapshot,
                                &state_ref.snapshot_index,
                            )?;
                            let mut event = apply_maintained_update_to_snapshot(
                                &mut state_ref.snapshot,
                                &mut state_ref.snapshot_index,
                                update,
                                shape.query().table.as_str(),
                                snapshot_tier,
                                previous_settled,
                                None,
                            )?;
                            state_ref.snapshot_source = SubscriptionSnapshotSource::LocalMaintained;
                            let settled = subscription_is_settled(
                                &node.borrow(),
                                active_authority_view_receipts,
                                &shape,
                                &binding,
                                settled_tier,
                                read_view,
                                remote_propagate_upstream,
                                requires_authority_receipt,
                                settled_authority_result.as_ref(),
                            ) && node
                                .borrow()
                                .relation_snapshot_has_materialized_required_cells(
                                    shape.query(),
                                    &state_ref.snapshot,
                                )?;
                            if authoritative_reset {
                                event = subscription_delta_event_with_reset(
                                    snapshot_tier,
                                    settled,
                                    &previous_snapshot,
                                    &state_ref.snapshot,
                                    true,
                                    terminal_rows,
                                );
                            }
                            if let SubscriptionEvent::Delta {
                                reset,
                                publishable,
                                added,
                                updated,
                                removed,
                                terminal_operations: _,
                                settled: event_settled,
                                ..
                            } = &mut event
                            {
                                *publishable = previous_settled != settled
                                    || *reset
                                    || !added.is_empty()
                                    || !updated.is_empty()
                                    || !removed.is_empty();
                                *event_settled = settled;
                            }
                            state_ref.settled = settled;
                            retained.push(Rc::downgrade(&state));
                            let materialized = state_ref.sender.materialized(
                                &node.borrow(),
                                shape.query(),
                                &event,
                            )?;
                            if state_ref.sender.publish(
                                event,
                                publication_before,
                                &state_ref.snapshot,
                                &state_ref.snapshot_index,
                                materialized,
                            )? {
                                changed += 1;
                            }
                            continue;
                        }
                    }
                }
                if authoritative_reset
                    && refresh
                        .maintained
                        .as_ref()
                        .is_some_and(LocalMaintainedViewSubscription::has_covered_input_sources)
                {
                    let publication_before = refresh.sender.checkpoint(
                        read_tier,
                        false,
                        &refresh.snapshot,
                        &refresh.snapshot_index,
                    )?;
                    // Local-first may already have published the exact same
                    // collector state from its provisional local input. The
                    // first authority closure still becomes installed below,
                    // but a no-op source handoff is not a user-visible reset.
                    // Strict remote remains unsettled here and therefore
                    // still publishes its first complete (including empty)
                    // receiver-local reset.
                    if refresh.settled {
                        refresh.snapshot_source = SubscriptionSnapshotSource::LocalMaintained;
                        retained.push(Rc::downgrade(&state));
                        continue;
                    }
                    // Aggregate fact terminals have no structural app-row
                    // collector: their self-contained payload is the local
                    // terminal state. Refresh the facade from that state
                    // after the exact covered closure has quiesced, rather
                    // than retaining the opening snapshot or consulting an
                    // authority result payload.
                    if shape.query().aggregate.is_some() {
                        let materialized = {
                            let maintained = refresh
                                .maintained
                                .as_ref()
                                .expect("covered reset retained its maintained graph");
                            node.lock()
                                .await
                                .materialize_local_maintained_relation_snapshot_with_occurrences(
                                    maintained,
                                )
                                .await?
                        };
                        refresh.snapshot = materialized.snapshot;
                    }
                    let settled = subscription_is_settled(
                        &node.borrow(),
                        active_authority_view_receipts,
                        &shape,
                        &binding,
                        settled_tier,
                        read_view,
                        remote_propagate_upstream,
                        requires_authority_receipt,
                        settled_authority_result.as_ref(),
                    );
                    // A complete closure can be observably unchanged (for
                    // example an empty grouped aggregate). It is still the
                    // exact authority boundary for this receiver and must
                    // publish a reset from the collector state, not from a
                    // result cache or a re-run query.
                    let snapshot = materialized_subscription_snapshot(
                        &refresh.snapshot,
                        &refresh.snapshot_index,
                    )?;
                    let event = subscription_delta_event_with_reset(
                        snapshot_tier,
                        settled,
                        &snapshot,
                        &snapshot,
                        true,
                        terminal_rows,
                    );
                    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                        eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=publish_covered_reset roots={} settled={settled}",
                            snapshot.root_count,
                        );
                    }
                    refresh.snapshot_source = SubscriptionSnapshotSource::LocalMaintained;
                    refresh.settled = settled;
                    retained.push(Rc::downgrade(&state));
                    let materialized =
                        refresh
                            .sender
                            .materialized(&node.borrow(), shape.query(), &event)?;
                    let delivered = refresh.sender.publish(
                        event,
                        publication_before,
                        &refresh.snapshot,
                        &refresh.snapshot_index,
                        materialized,
                    )?;
                    if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                        eprintln!(
                            "JAZZ_COVERED_INPUT_TRACE stage=covered_reset_delivery delivered={delivered}"
                        );
                    }
                    if delivered {
                        changed += 1;
                    }
                    continue;
                }
                let preserve_local_overlay = suppressed_authoritative_change;
                let (snapshot, snapshot_source) = if terminal_rows {
                    (
                        materialized_subscription_snapshot(
                            &refresh.snapshot,
                            &refresh.snapshot_index,
                        )?,
                        SubscriptionSnapshotSource::LocalMaintained,
                    )
                } else if preserve_local_overlay {
                    (
                        materialized_subscription_snapshot(
                            &refresh.snapshot,
                            &refresh.snapshot_index,
                        )?,
                        previous_source,
                    )
                } else if remote_settled_tier.is_some() {
                    // A remote receipt has already been staged through the
                    // local collector above. Never rebuild a relation facade
                    // from authority output or a second storage read here.
                    (
                        materialized_subscription_snapshot(
                            &refresh.snapshot,
                            &refresh.snapshot_index,
                        )?,
                        SubscriptionSnapshotSource::LocalMaintained,
                    )
                } else if has_maintained_subscription {
                    let previous = materialized_subscription_snapshot(
                        &refresh.snapshot,
                        &refresh.snapshot_index,
                    )?;
                    (previous.clone(), previous_source)
                } else {
                    (
                        node.lock()
                            .await
                            .subscription_snapshot_in_authorization_mode(
                                &shape,
                                &binding,
                                read_tier,
                                author,
                                &read_view,
                                authorization_mode,
                            )
                            .await?,
                        SubscriptionSnapshotSource::LinkSnapshot,
                    )
                };
                let settled = subscription_is_settled(
                    &node.borrow(),
                    active_authority_view_receipts,
                    &shape,
                    &binding,
                    settled_tier,
                    read_view,
                    remote_propagate_upstream,
                    requires_authority_receipt,
                    settled_authority_result.as_ref(),
                );
                (
                    snapshot,
                    snapshot_source,
                    settled,
                    snapshot_tier,
                    preserve_local_overlay,
                )
            }
        };
        let (previous, previous_source, has_maintained_flat_tuple_subscription) = {
            let state = state.borrow();
            (
                materialized_subscription_snapshot(&state.snapshot, &state.snapshot_index)?,
                state.snapshot_source,
                matches!(
                    &state.kind,
                    SubscriptionKind::Prepared {
                        shape,
                        maintained_subscription: Some(_),
                        ..
                    } if shape.query().flat_join.is_some()
                ),
            )
        };
        // A link snapshot is a relation facade without the terminal's flat
        // tuple occurrence sidecar. Once a maintained terminal owns this
        // stream, replace it from the terminal rather than installing an
        // unaddressable facade snapshot.
        if !force_reset_event
            && has_maintained_flat_tuple_subscription
            && previous_source == SubscriptionSnapshotSource::LocalMaintained
            && snapshot_source == SubscriptionSnapshotSource::LinkSnapshot
        {
            let maintained = {
                let mut state = state.borrow_mut();
                let SubscriptionKind::Prepared {
                    maintained_subscription,
                    ..
                } = &mut state.kind;
                maintained_subscription
                    .take()
                    .expect("checked maintained subscription above")
            };
            let materialized = node
                .lock()
                .await
                .materialize_local_maintained_relation_snapshot_with_occurrences(&maintained)
                .await;
            {
                let mut state = state.borrow_mut();
                let SubscriptionKind::Prepared {
                    maintained_subscription,
                    ..
                } = &mut state.kind;
                *maintained_subscription = Some(maintained);
            }
            let materialized = materialized?;
            snapshot = materialized.snapshot;
            snapshot_source = SubscriptionSnapshotSource::LocalMaintained;
        }
        if force_reset_event || snapshot != previous || settled != previous_settled {
            let mut state = state.borrow_mut();
            let publication_before = state.sender.checkpoint(
                read_tier,
                settled,
                &state.snapshot,
                &state.snapshot_index,
            )?;
            let event = if force_reset_event {
                subscription_delta_event_with_reset(
                    snapshot_tier,
                    settled,
                    &previous,
                    &snapshot,
                    true,
                    terminal_rows,
                )
            } else {
                subscription_delta_event(
                    snapshot_tier,
                    settled,
                    &previous,
                    &snapshot,
                    terminal_rows,
                )
            };
            state.snapshot = relation_snapshot_with_delta_slack(&snapshot);
            state.snapshot_index = maintained_snapshot_index_or_row_index(
                &mut node.borrow_mut(),
                &state.kind,
                &state.snapshot,
            )?;
            let SubscriptionKind::Prepared {
                maintained_subscription,
                ..
            } = &state.kind;
            state.snapshot_index.terminal_records = maintained_subscription
                .as_ref()
                .map(LocalMaintainedViewSubscription::decoded_terminal_records)
                .transpose()?
                .unwrap_or_default();
            state.snapshot_source = snapshot_source;
            state.settled = settled;
            let SubscriptionKind::Prepared { shape, .. } = &state.kind;
            let materialized = state
                .sender
                .materialized(&node.borrow(), shape.query(), &event)?;
            if state.sender.publish(
                event,
                publication_before,
                &state.snapshot,
                &state.snapshot_index,
                materialized,
            )? {
                changed += 1;
            }
        }
        retained.push(Rc::downgrade(&state));
    }
    for pending in pending_authoritative_resets.difference(&consumed_authoritative_resets) {
        node.borrow_mut().defer_authoritative_reset(pending);
    }
    *subscriptions.borrow_mut() = retained;
    Ok(changed)
}

pub(super) fn register_upstream_subscription_owner(
    owners: &UpstreamSubscriptionOwners,
    handles: &[UpstreamCoverageHandle],
    state: &Rc<RefCell<SubscriptionState>>,
) {
    let weak = Rc::downgrade(state);
    let mut owners = owners.borrow_mut();
    for handle in handles {
        owners
            .entry(handle.subscription)
            .or_default()
            .push(weak.clone());
    }
}

pub(super) fn unregister_upstream_subscription_owner(
    owners: &UpstreamSubscriptionOwners,
    subscription: SubscriptionKey,
    state: &Weak<RefCell<SubscriptionState>>,
) {
    let mut owners = owners.borrow_mut();
    let Some(entries) = owners.get_mut(&subscription) else {
        return;
    };
    entries.retain(|entry| !entry.ptr_eq(state) && entry.strong_count() > 0);
    if entries.is_empty() {
        owners.remove(&subscription);
    }
}

/// Release the matching connection pin. Return an owner only when this was
/// the final pin and the caller must retire the upstream stream itself.
pub(super) fn retire_relay_upstream_subscription(
    owners: &RelayUpstreamSubscriptionOwners,
    subscription: SubscriptionKey,
    downstream_connection_epoch: u64,
    coverage: &CoverageKey,
) -> Option<RelayUpstreamSubscriptionOwner> {
    let mut owners = owners.borrow_mut();
    let key = (
        subscription,
        downstream_connection_epoch,
        coverage.opts.read_view_key(),
    );
    let owner = owners.get(&key)?;
    if owner.downstream_connection_epoch != downstream_connection_epoch
        || owner.coverage != *coverage
    {
        return None;
    }
    let removed = owners.remove(&key)?;
    // The departing connection releases its pin, not another connection's
    // shared stream. Only the last pin authorizes wire/local retirement.
    (!owners
        .keys()
        .any(|(candidate, _, _)| *candidate == subscription))
    .then_some(removed)
}

/// Retire a relay owner after its authority has terminally rejected the wire
/// subscription. Unlike an ordinary unsubscribe there is no downstream link
/// assertion here: the opaque upstream handle itself is the unforgeable owner
/// token, and all returned pins identify downstream recipients of the rejection.
/// A RegisterShape rejection uses a nil binding marker and retires every pin
/// for that exact shape/read-view registration. Callers admit the authority
/// connection generation before using this registration-wide form.
pub(super) fn take_relay_upstream_subscription_owner(
    owners: &RelayUpstreamSubscriptionOwners,
    subscription: SubscriptionKey,
) -> Vec<(SubscriptionKey, RelayUpstreamSubscriptionOwner)> {
    let mut owners = owners.borrow_mut();
    let keys = owners
        .keys()
        .filter(|(candidate, _, _)| {
            *candidate == subscription
                || (subscription.binding_id == BindingId(uuid::Uuid::nil())
                    && candidate.shape_id == subscription.shape_id
                    && candidate.read_view == subscription.read_view)
        })
        .copied()
        .collect::<Vec<_>>();
    keys.into_iter()
        .filter_map(|key| owners.remove(&key).map(|owner| (key.0, owner)))
        .collect()
}

/// Take every propagated upstream owner for a disconnected downstream link.
/// Taking the records before enqueuing wire retirement makes repeated detach
/// calls and late rejections harmless no-ops.
pub(super) fn retire_relay_upstream_subscriptions_for_connection(
    owners: &RelayUpstreamSubscriptionOwners,
    downstream_connection_epoch: u64,
) -> Vec<(SubscriptionKey, RelayUpstreamSubscriptionOwner)> {
    let mut owners = owners.borrow_mut();
    let subscriptions = owners
        .iter()
        .filter_map(|(subscription, owner)| {
            (owner.downstream_connection_epoch == downstream_connection_epoch)
                .then_some(*subscription)
        })
        .collect::<Vec<_>>();
    subscriptions
        .into_iter()
        .filter_map(|key| {
            let owner = owners.remove(&key)?;
            (!owners.keys().any(|(candidate, _, _)| *candidate == key.0)).then_some((key.0, owner))
        })
        .collect()
}

pub(super) fn register_shape_rejection_matches(
    subscription: SubscriptionKey,
    shape: &ValidatedQuery,
    opts: &RegisterShapeOptions,
) -> bool {
    shape.shape_id() == subscription.shape_id && opts.read_view_key() == subscription.read_view
}

pub(super) fn route_upstream_subscription_rejection(
    subscriptions: &SubscriptionList,
    owners: &UpstreamSubscriptionOwners,
    subscription: SubscriptionKey,
    reason: SubscribeRejectReason,
) -> usize {
    let mut delivered = 0;
    if let Some(entries) = owners.borrow_mut().get_mut(&subscription) {
        entries.retain(|entry| entry.strong_count() > 0);
        for state in entries.iter().filter_map(Weak::upgrade) {
            let event = SubscriptionEvent::Rejected {
                reason: reason.clone(),
            };
            if state.borrow().sender.unbounded_send(event).is_ok() {
                delivered += 1;
            }
        }
        if delivered > 0 {
            return delivered;
        }
    }

    for state in subscriptions.borrow().iter().filter_map(Weak::upgrade) {
        let state_ref = state.borrow();
        if !state_ref.propagates_upstream {
            continue;
        }
        let SubscriptionKind::Prepared { shape, binding, .. } = &state_ref.kind;
        let opts = RegisterShapeOptions {
            tier: state_ref.remote_read_tier.unwrap_or(state_ref.read_tier),
            read_view: state_ref.read_view.clone(),
            propagate_upstream: state_ref.remote_propagate_upstream,
            ..RegisterShapeOptions::default()
        };
        if !register_shape_rejection_matches(subscription, shape, &opts) {
            continue;
        }
        if subscription.binding_id != BindingId(uuid::Uuid::nil())
            && binding.binding_id() != subscription.binding_id
        {
            continue;
        }
        let event = SubscriptionEvent::Rejected {
            reason: reason.clone(),
        };
        if state_ref.sender.unbounded_send(event).is_ok() {
            delivered += 1;
        }
    }
    delivered
}

/// Binding-supplied transport for one peer link.
///
/// The `Db` writes outbound messages with [`Transport::send`] and pulls inbound
/// ones with [`Transport::try_recv`]; the binding owns the actual socket and
/// scheduling and bridges these to real I/O on its own runtime. Both methods are
/// non-blocking — `try_recv` returning `None` means "nothing staged right now,"
/// not "closed" (a disconnect surface lands with a later B slice). This is the
/// single seam that keeps the async boundary *between* nodes, never inside `Db`.
pub trait Transport {
    /// Hand an outbound message to the binding's wire.
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError>;
    /// Pull the next inbound message the binding has staged, if any.
    fn try_recv(&mut self) -> Option<SyncMessage>;

    /// Return the immutable wire admission context paired with this transport.
    #[doc(hidden)]
    fn wire_inbound_context(&self) -> Option<crate::wire::WireInboundContext> {
        None
    }

    /// Immutable endpoint facts accepted by authenticated session admission.
    /// Semantic messages never self-assert this context.
    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        None
    }

    /// Whether this upstream is an authenticated SYSTEM backend link that may
    /// carry another session's policy binding. Ordinary session links must
    /// always let the remote peer use the identity it authenticated itself.
    fn permits_delegated_sessions(&self) -> bool {
        false
    }
}

/// Handshake/session facts for one accepted connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ConnectionSessionContext {
    /// This endpoint's authority identity and fresh epoch.
    pub local: WireAuthorityEndpoint,
    /// Remote authority endpoint, absent for ordinary clients. Its absence
    /// does not remove the authenticated session or our local receipt epoch.
    pub remote: Option<WireAuthorityEndpoint>,
    /// Authenticated session identity terminated by this link.
    pub link_identity: AuthorSubject,
    /// Features accepted for this connection.
    pub negotiated_features: WireFeatures,
}
