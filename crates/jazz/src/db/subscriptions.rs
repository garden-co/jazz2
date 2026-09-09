//! Subscription opening, query attachment, coverage, and cleanup.

use super::*;
use crate::protocol::AuthorityResultKey;

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Decode, prepare, scope, and open a serialized host subscription.
    #[doc(hidden)]
    pub async fn subscribe_serialized_query(
        &self,
        query: &[u8],
        opts: ReadOpts,
        request_scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        authorization: SerializedSubscriptionAuthorization,
    ) -> Result<SubscriptionStream, Error> {
        let prepared = self
            .prepare_serialized_query_async(query, request_scope)
            .await?;
        match authorization {
            SerializedSubscriptionAuthorization::ClientLocal => {
                self.subscribe(&prepared, opts).await
            }
            SerializedSubscriptionAuthorization::TrustedServing(author) => {
                self.subscribe_for_identity(&prepared, opts, author).await
            }
            SerializedSubscriptionAuthorization::TrustedClient(author) => {
                if author != AuthorSubject::SYSTEM && prepared.request_identity() != Some(author) {
                    return Err(Error::new(
                        ErrorCode::Protocol,
                        "trusted client subscription requires immutable request claims",
                    ));
                }
                self.subscribe_client_for_identity(&prepared, opts, author)
                    .await
            }
        }
    }

    /// Subscribe to a query and return a stream of materialized subscription events.
    ///
    /// ```rust
    /// # use jazz::db::{LocalUpdates, Propagation, ReadOpts, SubscriptionEvent};
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// let query = db.prepare_query(&db.table("todos"))?;
    /// let mut subscription = block_on(db.subscribe(
    ///     &query,
    ///     ReadOpts {
    ///         tier: DurabilityTier::Local,
    ///         local_updates: LocalUpdates::Immediate,
    ///         propagation: Propagation::LocalOnly,
    ///         include_deleted: false,
    ///         ..ReadOpts::default()
    ///     },
    /// ))?;
    /// let opened = block_on(subscription.next_event()).unwrap();
    /// let SubscriptionEvent::Delta { reset, added, .. } = opened else {
    ///     panic!("expected reset delta");
    /// };
    /// assert!(reset);
    /// assert!(added.is_empty());
    ///
    /// block_on(db.insert(
    ///     "todos",
    ///     todo_cells("notify subscribers", false),
    ///     Default::default(),
    /// ))?;
    /// let changed = block_on(subscription.next_event()).unwrap();
    /// let SubscriptionEvent::Delta { added, updated, removed, .. } = changed else {
    ///     panic!("expected subscription delta");
    /// };
    /// assert_eq!(added.len(), 1);
    /// assert!(updated.is_empty());
    /// assert!(removed.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn subscribe(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(
            prepared,
            opts,
            self.identity.author,
            QueryAuthorizationMode::ClientLocal,
            true,
        )
        .await
    }

    /// Subscribe to a query evaluated as `author`.
    pub async fn subscribe_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<SubscriptionStream, Error> {
        self.open_subscription(
            prepared,
            opts,
            author,
            QueryAuthorizationMode::TrustedServing,
            false,
        )
        .await
    }

    /// Subscribe from a trusted backend client on behalf of an admitted author.
    /// Local reads enforce policy against the backend's shared cache. Remote
    /// reads consume the upstream's exact policy-scoped input closure, without
    /// overlaying the backend's unscoped pending writes.
    pub async fn subscribe_client_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<SubscriptionStream, Error> {
        let mode = if effective_read_tier(&opts) >= DurabilityTier::Edge {
            QueryAuthorizationMode::ClientLocal
        } else {
            QueryAuthorizationMode::TrustedServing
        };
        self.open_subscription(prepared, opts, author, mode, false)
            .await
    }

    /// Subscribe to an output-changing relation query.
    pub async fn subscribe_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<SubscriptionStream, Error> {
        self.open_relation_subscription(
            query,
            opts,
            self.identity.author,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Subscribe to an output-changing relation query evaluated as `author`.
    pub async fn subscribe_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<SubscriptionStream, Error> {
        self.open_relation_subscription(query, opts, author, QueryAuthorizationMode::TrustedServing)
            .await
    }

    /// Attach a one-shot usage-site query coverage request.
    ///
    /// Binding read operations own this attachment internally, drive
    /// [`Db::tick`] until [`Db::query_attachment_is_covered`] is true, execute
    /// the read, and then call [`Db::detach_query`].
    pub fn attach_query_with_opts(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        if opts.propagation == Propagation::LocalOnly {
            return Ok(self.local_query_attachment(prepared, &opts));
        }
        let upstream_opts = self
            .node
            .upstream_register_shape_options(effective_read_tier(&opts), opts.read_view.clone());
        self.attach_or_refresh_query_coverage(
            &prepared.shape,
            &prepared.binding,
            upstream_opts,
            self.identity.author,
            prepared.request_policy_binding(self.identity.author)?,
            effective_read_tier(&opts) >= DurabilityTier::Edge,
        )
    }

    /// Attach one-shot coverage for the immutable base of an open transaction.
    /// Pending writes remain local overlays and are never sent upstream.
    #[doc(hidden)]
    pub fn attach_query_in_transaction_with_opts(
        &self,
        prepared: &PreparedQuery,
        open_tx_id: OpenTransactionId,
        mut opts: ReadOpts,
    ) -> Result<QueryAttachment, Error> {
        let snapshot = self
            .node
            .node
            .borrow()
            .open_transaction_snapshot(open_tx_id)?;
        opts.read_view = ReadViewSpec {
            source: ReadViewSourceSpec::Snapshot {
                snapshot: snapshot.into(),
            },
        };
        self.attach_query_with_opts(prepared, opts)
    }

    /// Attach a one-shot usage-site query coverage request evaluated as `author`.
    pub fn attach_query_with_opts_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        if opts.propagation == Propagation::LocalOnly {
            return Ok(self.local_query_attachment(prepared, &opts));
        }
        let upstream_opts = self
            .node
            .upstream_register_shape_options(effective_read_tier(&opts), opts.read_view.clone());
        let mut owner = self.node.node.borrow_mut();
        let mut node = prepared.scoped_node(&mut owner, author)?;
        let (shape, binding, _) = super::block_on(node.prepare_query_binding_for_link(
            &prepared.shape,
            &prepared.binding,
            upstream_opts.tier,
            author,
        ))?;
        drop(node);
        drop(owner);
        self.attach_or_refresh_query_coverage(
            &shape,
            &binding,
            upstream_opts,
            author,
            prepared.request_policy_binding(author)?,
            effective_read_tier(&opts) >= DurabilityTier::Edge,
        )
    }

    /// Identity-aware counterpart of [`Db::attach_query_in_transaction_with_opts`].
    #[doc(hidden)]
    pub fn attach_query_in_transaction_with_opts_for_identity(
        &self,
        prepared: &PreparedQuery,
        open_tx_id: OpenTransactionId,
        mut opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<QueryAttachment, Error> {
        let snapshot = self
            .node
            .node
            .borrow()
            .open_transaction_snapshot(open_tx_id)?;
        opts.read_view = ReadViewSpec {
            source: ReadViewSourceSpec::Snapshot {
                snapshot: snapshot.into(),
            },
        };
        self.attach_query_with_opts_for_identity(prepared, opts, author)
    }

    /// LocalOnly owns no remote coverage, even for a memory-only foreground.
    fn local_query_attachment(&self, prepared: &PreparedQuery, opts: &ReadOpts) -> QueryAttachment {
        QueryAttachment {
            subscriptions: vec![
                self.node.next_subscription_key(
                    &prepared.shape,
                    RegisterShapeOptions {
                        read_view: opts.read_view.clone(),
                        ..RegisterShapeOptions::default()
                    }
                    .read_view_key(),
                ),
            ],
            required_after: Vec::new(),
            requires_delivery_receipt: false,
            requires_current_authority_receipt: false,
            registrations: Vec::new(),
            refreshes: Vec::new(),
        }
    }

    fn attach_or_refresh_query_coverage(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        upstream_opts: RegisterShapeOptions,
        identity: AuthorSubject,
        policy_binding: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        requires_current_authority_receipt: bool,
    ) -> Result<QueryAttachment, Error> {
        let node = self.node.node.borrow();
        self.attach_or_refresh_query_coverage_with_node(
            &node,
            shape,
            binding,
            upstream_opts,
            identity,
            policy_binding,
            requires_current_authority_receipt,
        )
    }

    /// Wait for the node owner before opening ordinary or identity-aware coverage.
    pub async fn attach_query_with_opts_async(
        &self,
        prepared: &PreparedQuery,
        mut opts: ReadOpts,
        open_tx_id: Option<OpenTransactionId>,
        author: Option<AuthorSubject>,
    ) -> Result<QueryAttachment, Error> {
        ensure_supported_read_view(&opts)?;
        let mut node = match open_tx_id {
            Some(open_tx_id) => {
                self.node
                    .lock_for_transaction_operation(open_tx_id, self.owner_operation_admitted)
                    .await?
            }
            None => self.node.node.lock().await,
        };
        let mut node = prepared.scoped_node(&mut node, author.unwrap_or(self.identity.author))?;
        if let Some(open_tx_id) = open_tx_id {
            opts.read_view = ReadViewSpec {
                source: ReadViewSourceSpec::Snapshot {
                    snapshot: node.open_transaction_snapshot(open_tx_id)?.into(),
                },
            };
        }
        if opts.propagation == Propagation::LocalOnly {
            return Ok(self.local_query_attachment(prepared, &opts));
        }
        let upstream_opts = self
            .node
            .upstream_register_shape_options(effective_read_tier(&opts), opts.read_view.clone());
        let (shape, binding) = if let Some(author) = author {
            let (shape, binding, _) = node
                .prepare_query_binding_for_link(
                    &prepared.shape,
                    &prepared.binding,
                    upstream_opts.tier,
                    author,
                )
                .await?;
            (shape, binding)
        } else {
            (prepared.shape.clone(), prepared.binding.clone())
        };
        self.attach_or_refresh_query_coverage_with_node(
            &node,
            &shape,
            &binding,
            upstream_opts,
            author.unwrap_or(self.identity.author),
            prepared.request_policy_binding(author.unwrap_or(self.identity.author))?,
            effective_read_tier(&opts) >= DurabilityTier::Edge,
        )
    }

    fn attach_or_refresh_query_coverage_with_node(
        &self,
        node: &NodeState<S>,
        shape: &ValidatedQuery,
        binding: &Binding,
        upstream_opts: RegisterShapeOptions,
        identity: AuthorSubject,
        policy_binding: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        requires_current_authority_receipt: bool,
    ) -> Result<QueryAttachment, Error> {
        let requires_delivery_receipt = requires_current_authority_receipt
            || self.node.upstream_durability_floor.get() == DurabilityTier::Local;
        let binding_view = BindingViewKey::new(
            shape.shape_id(),
            binding.binding_id(),
            upstream_opts.read_view_key(),
        );
        let coverage = request_coverage_key(shape, binding, upstream_opts.clone(), &policy_binding);
        let authority_key = match coverage.policy_binding.clone() {
            Some(binding) => AuthorityResultKey::policy_scoped(binding_view, binding),
            None => AuthorityResultKey::unscoped(binding_view),
        };
        let required_after = node.applied_authority_result_generation(&authority_key);
        // All live usages pin one stream. One-shot freshness is a newer
        // receipt on that stream, not another stream with the same inputs.
        if self
            .node
            .upstream_coverage_refcounts
            .borrow()
            .contains_key(&coverage)
            && let Some(subscription) = self
                .node
                .latest_coverage_subscriptions
                .borrow()
                .get(&coverage)
                .copied()
        {
            *self
                .node
                .upstream_coverage_refcounts
                .borrow_mut()
                .entry(coverage.clone())
                .or_insert(0) += 1;
            let pending_subscription = PendingUpstreamSubscription {
                subscription,
                shape: shape.clone(),
                binding: binding.clone(),
                opts: upstream_opts.clone(),
                identity,
                policy_binding: policy_binding.clone(),
            };
            self.register_query_coverage(coverage.clone(), pending_subscription.clone());
            let mut refreshes = self.node.coverage_refresh_generations.borrow_mut();
            if refreshes.get(&coverage).copied() != Some(required_after) {
                refreshes.insert(coverage.clone(), required_after);
                self.node
                    .upstream_subscriptions
                    .borrow_mut()
                    .push(PendingUpstreamCommand::Subscribe(pending_subscription));
                self.node.schedule_tick(TickUrgency::Immediate);
            }
            return Ok(QueryAttachment {
                subscriptions: vec![subscription],
                required_after: vec![(binding_view, required_after)],
                requires_delivery_receipt,
                requires_current_authority_receipt,
                registrations: vec![subscription],
                refreshes: vec![(coverage, required_after)],
            });
        }
        let subscription = self.attach_query_shape_binding_with_opts(
            shape,
            binding,
            upstream_opts.clone(),
            identity,
            policy_binding.clone(),
        )?;
        *self
            .node
            .upstream_coverage_refcounts
            .borrow_mut()
            .entry(coverage.clone())
            .or_insert(0) += 1;
        self.register_query_coverage(
            coverage.clone(),
            PendingUpstreamSubscription {
                subscription,
                shape: shape.clone(),
                binding: binding.clone(),
                opts: upstream_opts,
                identity,
                policy_binding: policy_binding.clone(),
            },
        );
        Ok(QueryAttachment {
            subscriptions: vec![subscription],
            required_after: vec![(binding_view, required_after)],
            requires_delivery_receipt,
            requires_current_authority_receipt,
            registrations: vec![subscription],
            refreshes: Vec::new(),
        })
    }

    fn register_query_coverage(
        &self,
        coverage: CoverageKey,
        subscription: PendingUpstreamSubscription,
    ) {
        let mut registrations = self.node.query_coverage_registrations.borrow_mut();
        registrations
            .entry(subscription.subscription)
            .and_modify(|registration| registration.ref_count += 1)
            .or_insert(QueryCoverageRegistration {
                coverage,
                subscription,
                ref_count: 1,
            });
    }

    fn attach_query_shape_binding_with_opts(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        identity: AuthorSubject,
        policy_binding: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    ) -> Result<SubscriptionKey, Error> {
        let subscription = self.node.next_subscription_key(shape, opts.read_view_key());
        self.node
            .upstream_subscriptions
            .borrow_mut()
            .push(PendingUpstreamCommand::Subscribe(
                PendingUpstreamSubscription {
                    subscription,
                    shape: shape.clone(),
                    binding: binding.clone(),
                    opts: opts.clone(),
                    identity,
                    policy_binding: policy_binding.clone(),
                },
            ));
        self.node.latest_coverage_subscriptions.borrow_mut().insert(
            request_coverage_key(shape, binding, opts, &policy_binding),
            subscription,
        );
        self.node.schedule_tick(TickUrgency::Immediate);
        Ok(subscription)
    }

    /// Attach a one-shot usage-site query coverage request at the default tier.
    pub fn attach_query(&self, prepared: &PreparedQuery) -> Result<QueryAttachment, Error> {
        self.attach_query_with_opts(prepared, ReadOpts::default())
    }

    /// LocalOnly attachments are immediately ready against this node's data.
    /// With Full propagation, memory-only foregrounds first receive their
    /// owner's local query answer; remote reads also require an authority receipt.
    pub fn query_attachment_is_covered(&self, attachment: &QueryAttachment) -> bool {
        // Local propagation does not gate a durable node's local knowledge.
        // A foreground's empty memory is not yet its durable owner's answer.
        if !attachment.requires_delivery_receipt {
            return true;
        }
        let Some(node) = self.node.node.try_lock() else {
            return false;
        };
        let active_receipts = self.node.active_authority_view_receipts.borrow();
        let has_current_authority_receipt = active_receipts.as_ref().is_some_and(|receipts| {
            attachment
                .required_after
                .iter()
                .all(|(binding_view, _)| receipts.binding_views.contains(binding_view))
                && attachment
                    .subscriptions
                    .iter()
                    .all(|subscription| receipts.subscriptions.contains(subscription))
        });
        let covered = attachment
            .required_after
            .iter()
            .all(|(binding_view, required_after)| {
                let mut receipts = attachment
                    .subscriptions
                    .iter()
                    .filter_map(|subscription| {
                        node.authority_result_key_for_subscription(*subscription)
                            .ok()
                    })
                    .filter(|key| key.binding_view == *binding_view);
                let Some(receipt) = receipts.next() else {
                    return false;
                };
                receipts.all(|candidate| candidate == receipt)
                    && node.applied_authority_result_generation(&receipt) > *required_after
                    && !node.opening_pending_for_authority_result(&receipt)
            })
            && (!attachment.requires_current_authority_receipt || has_current_authority_receipt);
        drop(node);
        drop(active_receipts);
        if covered {
            let mut refreshes = self.node.coverage_refresh_generations.borrow_mut();
            for (coverage, generation) in &attachment.refreshes {
                if refreshes.get(coverage).copied() == Some(*generation) {
                    refreshes.remove(coverage);
                }
            }
        }
        covered
    }

    #[cfg(any(test, feature = "testing"))]
    /// Test-only counts of live coverage groups and usage-site registrations.
    pub fn query_coverage_attachment_counts_for_test(&self) -> (usize, usize) {
        (
            self.node.upstream_coverage_refcounts.borrow().len(),
            self.node.query_coverage_registrations.borrow().len(),
        )
    }

    #[cfg(any(test, feature = "testing"))]
    /// Internal receipt-lifetime coverage needs to inspect state that has no
    /// public equivalent: detached Local overlays are intentionally best-effort.
    pub fn settled_authoritative_receipt_counts_for_test(&self) -> (usize, usize) {
        self.node
            .node
            .borrow()
            .settled_authoritative_receipt_counts_for_test()
    }

    #[cfg(any(test, feature = "testing"))]
    /// Hold the async node owner until this future is cancelled.
    ///
    /// This is a test-only suspension point for cancellation and contention
    /// contracts that cannot be reproduced by borrowing the private node.
    pub async fn hold_node_owner_for_test(&self) {
        let _node = self.node.node.lock().await;
        std::future::pending::<()>().await;
    }

    #[cfg(any(test, feature = "testing"))]
    /// Count unfinished core upload journals for lifecycle cancellation receipts.
    pub async fn pending_upload_count_for_test(&self) -> Result<usize, Error> {
        self.node
            .node
            .lock()
            .await
            .pending_upload_count_for_test()
            .await
            .map_err(Into::into)
    }

    /// Detach a one-shot query coverage request.
    pub fn detach_query(&self, attachment: QueryAttachment) {
        self.detach_query_using(attachment, |subscription| {
            self.node.node.borrow_mut().apply_unsubscribe(subscription);
        });
    }

    /// Release binding-owned coverage after asynchronously acquiring its owner.
    /// A cancelled binding read may share the node with another suspended read.
    #[doc(hidden)]
    pub async fn detach_query_async(&self, attachment: QueryAttachment) {
        let mut owner = self.node.node.lock().await;
        self.detach_query_using(attachment, |subscription| {
            owner.apply_unsubscribe(subscription);
        });
    }

    fn detach_query_using(
        &self,
        attachment: QueryAttachment,
        mut unsubscribe: impl FnMut(SubscriptionKey),
    ) {
        let mut removed_subscriptions = Vec::new();
        let mut registrations = self.node.query_coverage_registrations.borrow_mut();
        for subscription in attachment.registrations {
            let Some(registration) = registrations.get_mut(&subscription) else {
                continue;
            };
            let coverage = registration.coverage.clone();
            registration.ref_count = registration.ref_count.saturating_sub(1);
            let last_registration = registration.ref_count == 0;
            if last_registration {
                registrations.remove(&subscription);
            }
            let mut coverage_refcounts = self.node.upstream_coverage_refcounts.borrow_mut();
            let Some(count) = coverage_refcounts.get_mut(&coverage) else {
                continue;
            };
            *count = count.saturating_sub(1);
            let last_coverage_pin = *count == 0;
            if last_coverage_pin {
                coverage_refcounts.remove(&coverage);
                self.node
                    .awaiting_initial_authority_coverage
                    .borrow_mut()
                    .remove(&coverage);
            }
            if last_coverage_pin {
                removed_subscriptions.push((subscription, coverage));
            }
        }
        drop(registrations);
        for (subscription, coverage) in removed_subscriptions {
            if let Some(receipts) = self
                .node
                .active_authority_view_receipts
                .borrow_mut()
                .as_mut()
            {
                receipts.subscriptions.remove(&subscription);
            }
            self.node
                .coverage_refresh_generations
                .borrow_mut()
                .remove(&coverage);
            unsubscribe(subscription);
            let mut latest = self.node.latest_coverage_subscriptions.borrow_mut();
            if latest.get(&coverage) == Some(&subscription) {
                latest.remove(&coverage);
            }
            drop(latest);
            self.node
                .upstream_subscriptions
                .borrow_mut()
                .push(PendingUpstreamCommand::Unsubscribe(subscription));
        }
        self.node.schedule_tick(TickUrgency::Immediate);
    }

    async fn open_subscription(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
        allow_pending_overlay: bool,
    ) -> Result<SubscriptionStream, Error> {
        ensure_supported_subscription_read_opts(&opts)?;
        self.validate_prepared_shape_for_registration(prepared)
            .await?;
        let requested_read_tier = effective_read_tier(&opts);
        let read_tier = requested_read_tier;
        let pending_overlay = allow_pending_overlay
            && authorization_mode == QueryAuthorizationMode::ClientLocal
            && requested_read_tier >= DurabilityTier::Edge
            && opts.local_updates == LocalUpdates::Immediate;
        let mut owner = self.node.node.lock().await;
        let mut node = prepared.scoped_node(&mut owner, author)?;
        node.ensure_peer_maintained_subscription_view_supported(
            &prepared.shape,
            &prepared.binding,
            read_tier,
            author,
            &opts.read_view,
            authorization_mode,
        )
        .await?;
        let (local_shape, local_binding, _local_plan) = node
            .prepare_query_binding_for_link_in_authorization_mode(
                &prepared.shape,
                &prepared.binding,
                read_tier,
                author,
                authorization_mode,
            )
            .await?;
        // The subscription opener performs one bounded IVM poll. Keep the
        // current host scheduler as the cold-storage continuation owner,
        // rather than the short-lived foreground future opening this stream.
        let progress_waker = self.node.query_runtime_waker();
        let (mut subscription, mut snapshot) = node
            .open_maintained_view_subscription_in_authorization_mode_with_waker(
                &local_shape,
                &local_binding,
                author,
                read_tier,
                &opts.read_view,
                Some(_local_plan),
                authorization_mode,
                pending_overlay,
                progress_waker.as_ref(),
            )
            .await?;
        let local_runtime_token = node.groove_runtime_token();
        drop(node);
        drop(owner);
        let local_subscription_id = subscription.subscription_id();
        let local_subscription_cleanup = Rc::new(Cell::new(Some((
            local_runtime_token,
            local_subscription_id,
        ))));
        let local_cleanup_handle = Rc::clone(&local_subscription_cleanup);
        let local_cleanup_node = Rc::clone(&self.node);
        let opening_upstream = Rc::new(RefCell::new(Vec::new()));
        let cleanup_upstream = Rc::clone(&opening_upstream);
        let mut local_cleanup = CleanupGuard::new(Box::new(move || {
            // Opening failed before a public state existed; this is the one
            // intentionally ID-based cleanup path.
            local_cleanup_node.enqueue_subscription_finalization(PendingSubscriptionFinalization {
                state: None,
                opening_upstream: std::mem::take(&mut *cleanup_upstream.borrow_mut()),
                opening_local: local_cleanup_handle.take(),
                acknowledgement: None,
            });
        }));
        // This opener may have been waiting for the node mutex while a close
        // attempt closed finalization admission. Its local maintained view
        // was absent from that close's snapshot, so reject it and let this
        // guard transfer cleanup to the still-live node owner.
        self.node.ensure_subscription_finalization_open()?;
        // Compiler-owned root collectors, not surface query syntax, own the
        // terminal snapshot and positional edits.
        let terminal_rows = subscription.has_root_collector();
        let mut state_shape = local_shape;
        let mut state_binding = local_binding;
        let mut remote_read_tier = None;
        let mut requires_authority_receipt = false;
        let mut upstream_subscription_handles = Vec::new();
        let mut suppress_provisional_opening = false;
        let remote_propagate_upstream = opts.propagation == Propagation::Full;
        // LocalOnly never sends a query to another node, including a durable
        // browser worker. Full Local reads may still consume its cache.
        let propagates_upstream = remote_propagate_upstream;
        if propagates_upstream {
            let upstream_opts = self
                .node
                .upstream_register_shape_options(requested_read_tier, opts.read_view.clone());
            let (shape, binding) = if upstream_opts.tier == read_tier {
                (state_shape.clone(), state_binding.clone())
            } else {
                let mut owner = self.node.node.lock().await;
                let mut node = prepared.scoped_node(&mut owner, author)?;
                let (shape, binding, _) = node
                    .prepare_query_binding_for_link_in_authorization_mode(
                        &prepared.shape,
                        &prepared.binding,
                        upstream_opts.tier,
                        author,
                        authorization_mode,
                    )
                    .await?;
                (shape, binding)
            };
            state_shape = shape.clone();
            state_binding = binding.clone();
            remote_read_tier = Some(upstream_opts.tier);
            // Edge/Global cache possession is never a settlement receipt,
            // even when this subscription opens before an upstream exists.
            // The eventual connection must send its own ViewUpdate.
            requires_authority_receipt = upstream_opts.tier >= DurabilityTier::Edge;
            let opened = self
                .open_subscription_upstream_coverage(
                    prepared,
                    &shape,
                    &binding,
                    upstream_opts,
                    author,
                    authorization_mode,
                )
                .await?;
            upstream_subscription_handles = opened.handles;
            *opening_upstream.borrow_mut() = upstream_subscription_handles.clone();
            suppress_provisional_opening = authorization_mode
                == QueryAuthorizationMode::ClientLocal
                && requested_read_tier >= DurabilityTier::Edge
                && opened.awaits_initial_authority_response
                && snapshot.root_count == 0
                && snapshot.edges.is_empty();
        }
        let settled_tier = remote_read_tier.unwrap_or(read_tier);
        let settled_authority_result = if remote_read_tier.is_some() {
            let binding_view_key = BindingViewKey {
                shape_id: state_shape.shape_id(),
                binding_id: state_binding.binding_id(),
                read_view: RegisterShapeOptions {
                    tier: settled_tier,
                    read_view: opts.read_view.clone(),
                    propagate_upstream: remote_propagate_upstream,
                    ..RegisterShapeOptions::default()
                }
                .read_view_key(),
            };
            let node = self.node.node.lock().await;
            let mut keys = upstream_subscription_handles
                .iter()
                .filter_map(|handle| {
                    node.authority_result_key_for_subscription(handle.subscription)
                        .ok()
                })
                .filter(|key| key.binding_view == binding_view_key);
            let key = keys.next();
            key.filter(|key| keys.all(|candidate| candidate == *key))
        } else {
            None
        };
        // `open_maintained...` creates a receiver-local graph but deliberately
        // does not install a previously received authority closure: that
        // installation must be folded into the public stream owner's snapshot
        // with the same terminal reducer used for every later update.  In
        // particular, do not materialize the authority result set here.
        let mut snapshot_index = RelationSnapshotIndex::from_snapshot(&snapshot);
        snapshot_index.roots = subscription
            .root_occurrence_ids()
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, occurrence)| (occurrence, index))
            .collect();
        if authorization_mode == QueryAuthorizationMode::ClientLocal
            && let Some(authority_result_key) = settled_authority_result.as_ref()
        {
            let (update, _) = self
                .node
                .node
                .lock()
                .await
                .drain_local_maintained_view_subscription_preserving_rows_with_waker(
                    &mut subscription,
                    Some(authority_result_key.clone()),
                    &BTreeSet::new(),
                    progress_waker.as_ref(),
                )
                .await?;
            if let Some(update) = update {
                let terminal_layout = subscription.terminal_root_layout();
                let _ = apply_maintained_update_to_snapshot(
                    &mut snapshot,
                    &mut snapshot_index,
                    update,
                    prepared.shape.query().table.as_str(),
                    read_tier,
                    false,
                    terminal_layout,
                )?;
            }
        }
        let covered_closure_installed = {
            let node = self.node.node.lock().await;
            settled_authority_result.as_ref().is_none_or(|key| {
                subscription.has_installed_covered_closure(
                    key,
                    node.applied_authority_result_generation(key),
                )
            })
        };
        let settled = {
            let node = self.node.node.lock().await;
            subscription_is_settled(
                &node,
                &self.node.active_authority_view_receipts,
                &state_shape,
                &state_binding,
                settled_tier,
                opts.read_view.clone(),
                remote_propagate_upstream,
                requires_authority_receipt,
                settled_authority_result.as_ref(),
            ) && (!subscription.has_covered_input_sources() || covered_closure_installed)
        };
        // An empty local opening carries no observable result information at
        // an Edge/Global request.  Until the authority replies, publishing it
        // would let a public subscription report a provisional empty view as
        // its first delivery.  `awaits_initial_authority_response` is only
        // known while opening a fresh upstream handle, but an already-open
        // link has the same receipt requirement.
        suppress_provisional_opening |= authorization_mode == QueryAuthorizationMode::ClientLocal
            && requested_read_tier >= DurabilityTier::Edge
            && remote_read_tier.is_some()
            && !settled
            && snapshot.root_count == 0
            && snapshot.edges.is_empty();
        let (sender, receiver) = unbounded();
        let sender = SubscriptionSender {
            sender,
            publication: Rc::new(RefCell::new(SubscriptionPublication::default())),
            requested_tier: read_tier,
        };
        let mut root_occurrence_ids = snapshot_index
            .roots
            .iter()
            .map(|(occurrence, index)| (*index, occurrence.clone()))
            .collect::<Vec<_>>();
        root_occurrence_ids.sort_by_key(|(index, _)| *index);
        let root_occurrence_ids = root_occurrence_ids
            .into_iter()
            .map(|(_, occurrence)| occurrence)
            .collect::<Vec<_>>();
        let initial_outputs = {
            materialize_subscription_terminal_records(&mut snapshot, &snapshot_index)?;
            subscription_outputs_with_occurrence_sidecar(&snapshot, &root_occurrence_ids)?
        };
        let state_snapshot = relation_snapshot_with_delta_slack(&snapshot);
        snapshot_index = RelationSnapshotIndex::from_snapshot(&state_snapshot);
        snapshot_index.roots = root_occurrence_ids
            .iter()
            .cloned()
            .enumerate()
            .map(|(index, occurrence)| (occurrence, index))
            .collect();
        snapshot_index.terminal_records = subscription.decoded_terminal_records()?;
        let maintained_subscription = Some(subscription);
        let closed = Rc::new(Cell::new(false));
        let scalar_reconciliation_enabled = read_tier < DurabilityTier::Edge
            && remote_read_tier.is_some()
            && remote_propagate_upstream
            && opts.read_view.is_default()
            && crate::node::simple_scalar_exit_query(state_shape.query());
        let state = Rc::new(RefCell::new(SubscriptionState {
            closed: Rc::clone(&closed),
            terminal_rows,
            scalar_reconciliation_enabled,
            scalar_authority_revision: 0,
            scalar_reconciliation: ScalarReconciliation::default(),
            kind: SubscriptionKind::Prepared {
                shape: state_shape,
                binding: state_binding,
                maintained_subscription,
            },
            groove_runtime_token: local_runtime_token,
            local_subscription_cleanup: Rc::clone(&local_subscription_cleanup),
            upstream_subscription_handles,
            propagates_upstream,
            request_identity_claims: prepared.request_identity_claims.clone(),
            author,
            authorization_mode,
            read_tier,
            pending_overlay,
            remote_read_tier,
            requires_authority_receipt,
            remote_propagate_upstream,
            read_view: opts.read_view.clone(),
            snapshot: state_snapshot,
            snapshot_index,
            snapshot_source: SubscriptionSnapshotSource::LocalMaintained,
            settled,
            cold_runtime_replacement: false,
            sender,
        }));
        {
            let node = self.node.node.lock().await;
            let state = state.borrow();
            let event = SubscriptionEvent::Delta {
                reset: true,
                publishable: !suppress_provisional_opening,
                added: initial_outputs,
                updated: Vec::new(),
                removed: Vec::new(),
                terminal_operations: Vec::new(),
                settled,
                tier: read_tier,
            };
            let materialized = state
                .sender
                .materialized(&node, prepared.shape.query(), &event)?;
            state.sender.publish(
                event,
                None,
                &state.snapshot,
                &state.snapshot_index,
                materialized,
            )?;
        }
        self.node
            .subscriptions
            .borrow_mut()
            .push(Rc::downgrade(&state));
        // The guard covers fallible opening after the local maintained view
        // exists. On success, replace it with one command carrying local and
        // upstream cleanup so Drop never touches the async node mutex.
        drop(local_cleanup.take());
        let cleanup: SubscriptionCleanup = {
            register_upstream_subscription_owner(
                &self.node.upstream_subscription_owners,
                &state.borrow().upstream_subscription_handles,
                &state,
            );
            let node = Rc::clone(&self.node);
            let state = Rc::clone(&state);
            Box::new(move |acknowledgement| {
                closed.set(true);
                let finalization_node = acknowledgement.as_ref().map(|_| Rc::clone(&node));
                node.enqueue_subscription_finalization(PendingSubscriptionFinalization {
                    state: Some(state),
                    opening_upstream: Vec::new(),
                    opening_local: None,
                    acknowledgement,
                });
                finalization_node.map(|node| {
                    Box::pin(async move {
                        node.drain_subscription_finalizations().await?;
                        Ok(())
                    }) as SubscriptionFinalizationFuture
                })
            })
        };
        Ok(SubscriptionStream {
            receiver,
            _state: state,
            cleanup: Some(cleanup),
            finalization: None,
            terminated: false,
        })
    }

    async fn validate_prepared_shape_for_registration(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<(), Error> {
        let ast = ShapeAst::from_validated(&prepared.shape);
        let validation = {
            let node = self.node.node.lock().await;
            validate_shape_ast_for_registration(&node, prepared.shape.shape_id(), &ast)
        };
        validation.map(|_| ()).map_err(Error::from)
    }

    async fn open_relation_subscription(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<SubscriptionStream, Error> {
        ensure_supported_subscription_read_opts(&opts)?;
        let query = relation_query_to_query(query)?;
        let prepared = self.prepare_query(&query)?;
        self.open_subscription(&prepared, opts, author, authorization_mode, true)
            .await
    }

    async fn open_subscription_upstream_coverage(
        &self,
        prepared: &PreparedQuery,
        shape: &ValidatedQuery,
        binding: &Binding,
        opts: RegisterShapeOptions,
        identity: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<OpenedUpstreamCoverage, Error> {
        let mut owner = self.node.node.lock().await;
        let mut node = prepared.scoped_node(&mut owner, identity)?;
        node.ensure_peer_maintained_subscription_view_supported(
            shape,
            binding,
            opts.tier,
            identity,
            &opts.read_view,
            authorization_mode,
        )
        .await?;
        drop(node);
        drop(owner);
        let policy_binding = prepared.request_policy_binding(identity)?;
        let coverage = request_coverage_key(shape, binding, opts.clone(), &policy_binding);
        if self
            .node
            .upstream_coverage_refcounts
            .borrow()
            .contains_key(&coverage)
        {
            if let Some(subscription) = self
                .node
                .latest_coverage_subscriptions
                .borrow()
                .get(&coverage)
                .copied()
            {
                *self
                    .node
                    .upstream_coverage_refcounts
                    .borrow_mut()
                    .entry(coverage.clone())
                    .or_insert(0) += 1;
                let awaits_initial_authority_response = self
                    .node
                    .awaiting_initial_authority_coverage
                    .borrow()
                    .contains(&coverage);
                return Ok(OpenedUpstreamCoverage {
                    handles: vec![UpstreamCoverageHandle {
                        coverage,
                        subscription,
                    }],
                    awaits_initial_authority_response,
                });
            }
        }
        let subscription = self.attach_query_shape_binding_with_opts(
            shape,
            binding,
            opts,
            identity,
            policy_binding,
        )?;
        *self
            .node
            .upstream_coverage_refcounts
            .borrow_mut()
            .entry(coverage.clone())
            .or_insert(0) += 1;
        let has_live_upstream = self
            .node
            .connections
            .borrow()
            .iter()
            .any(|connection| matches!(&connection.borrow().link, ConnectionLink::Upstream(_)));
        if has_live_upstream {
            self.node
                .awaiting_initial_authority_coverage
                .borrow_mut()
                .insert(coverage.clone());
        }
        Ok(OpenedUpstreamCoverage {
            handles: vec![UpstreamCoverageHandle {
                coverage,
                subscription,
            }],
            awaits_initial_authority_response: has_live_upstream,
        })
    }
}
