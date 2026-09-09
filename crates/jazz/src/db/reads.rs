//! Prepared, one-shot, relation, and result-tree read APIs.

use super::*;

/// Binding-only classification for a value whose referenced immutable content
/// has not arrived locally yet. This deliberately preserves the distinction
/// between an ordinary, retryable chunk absence and corrupted/permanent
/// storage errors before they are flattened into a public [`Error`].
#[doc(hidden)]
pub enum BindingHydrationError {
    RetryableChunkUnavailable { retry_after_ms: u32 },
    Error(Error),
}

struct SerializedReadCoverage<F>
where
    F: FnOnce(QueryAttachment),
{
    attachment: Option<QueryAttachment>,
    release: Option<F>,
}

impl<F> Drop for SerializedReadCoverage<F>
where
    F: FnOnce(QueryAttachment),
{
    fn drop(&mut self) {
        if let (Some(attachment), Some(release)) = (self.attachment.take(), self.release.take()) {
            release(attachment);
        }
    }
}

fn binding_hydration_error(error: crate::node::Error) -> BindingHydrationError {
    use groove::chunks::ChunkError;
    use groove::ivm::runtime::IvmRuntimeError;

    let retry_after_ms = match &error {
        crate::node::Error::LargeValueReachability(
            groove::large_values::ReachabilityError::Chunk(ChunkError::Retryable {
                retry_after_ms,
            }),
        )
        | crate::node::Error::Groove(groove::db::Error::IvmRuntime(IvmRuntimeError::Chunk(
            ChunkError::Retryable { retry_after_ms },
        ))) => Some(*retry_after_ms),
        _ => None,
    };
    if let Some(retry_after_ms) = retry_after_ms {
        return BindingHydrationError::RetryableChunkUnavailable { retry_after_ms };
    }
    let unavailable = matches!(
        &error,
        crate::node::Error::ChunkStorage(groove::chunks::ChunkStorageError::Unavailable)
            | crate::node::Error::LargeValueReachability(
                groove::large_values::ReachabilityError::Chunk(ChunkError::Unavailable)
            )
            | crate::node::Error::Groove(groove::db::Error::IvmRuntime(IvmRuntimeError::Chunk(
                ChunkError::Unavailable
            )))
    );
    if unavailable {
        BindingHydrationError::Error(Error::new(
            ErrorCode::NotObserved,
            "large-value chunk is permanently unavailable",
        ))
    } else {
        BindingHydrationError::Error(error.into())
    }
}

impl<S> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    /// Start a query rooted at `table`.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db};
    /// # use jazz::query::{col, eq, lit};
    /// let db = block_on(open_todos_db())?;
    /// let open_todos = db
    ///     .table("todos")
    ///     .filter(eq(col("done"), lit(false)))
    ///     .select(["title", "done"]);
    ///
    /// let open_todos = db.prepare_query(&open_todos)?;
    /// assert!(db.read(&open_todos)?.is_empty());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn table(&self, table: impl Into<String>) -> Query {
        Query::from(table)
    }

    /// Prepare a query for repeated reads or subscriptions.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let write = block_on(db.insert(
    ///     "todos",
    ///     todo_cells("write docs", false),
    ///     Default::default(),
    /// ))?;
    /// let todo = write.row_uuid();
    ///
    /// let query = db.prepare_query(&db.table("todos"))?;
    /// let rows = db.read(&query)?;
    /// assert_eq!(rows.len(), 1);
    /// assert_eq!(rows[0].row_uuid(), todo);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn prepare_query(&self, query: &Query) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound(query, BTreeMap::new())
    }

    /// Normalize relation syntax into the same prepared-query representation
    /// used by every other read path.
    #[doc(hidden)]
    pub fn prepare_relation_query(&self, query: &RelationQuery) -> Result<PreparedQuery, Error> {
        self.prepare_query(&relation_query_to_query(query)?)
    }

    /// Prepare a query after asynchronously acquiring the node owner.
    /// Runtime callers must use this entry point when another operation may
    /// be suspended on storage. The synchronous API requires an idle owner.
    pub async fn prepare_query_async(&self, query: &Query) -> Result<PreparedQuery, Error> {
        let mut node = self.node.node.lock().await;
        let (schema, schema_version) = if self.schema_view_is_fixed {
            (self.schema.clone(), self.schema_version_id)
        } else {
            let current = node.current_write_schema()?;
            let schema = if current.schema == self.schema_version_id {
                self.schema.clone()
            } else {
                node.catalogue_schemas()
                    .get(&current.schema)
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::Schema,
                            "current write schema is missing from catalogue",
                        )
                    })?
                    .schema
                    .clone()
            };
            (schema, current.schema)
        };
        let shape = query.validate_with_schema_version(&schema, schema_version)?;
        let binding = shape.bind(BTreeMap::new())?;
        let (local_plan, global_plan) =
            if should_install_prepared_plan(&shape) && !node.uses_schema_projected_read(&shape) {
                (
                    Some(
                        node.prepared_query_plan(
                            &shape,
                            &binding,
                            DurabilityTier::Local,
                            AuthorSubject::SYSTEM,
                        )
                        .await?,
                    ),
                    Some(
                        node.prepared_query_plan(
                            &shape,
                            &binding,
                            DurabilityTier::Global,
                            AuthorSubject::SYSTEM,
                        )
                        .await?,
                    ),
                )
            } else {
                (None, None)
            };
        Ok(PreparedQuery {
            request_identity_claims: None,
            shape,
            binding,
            local_plan,
            global_plan,
            groove_runtime_token: node.groove_runtime_token(),
        })
    }

    /// Normalize relation syntax and prepare it after asynchronously acquiring
    /// the node owner.
    #[doc(hidden)]
    pub async fn prepare_relation_query_async(
        &self,
        query: &RelationQuery,
    ) -> Result<PreparedQuery, Error> {
        self.prepare_query_async(&relation_query_to_query(query)?)
            .await
    }

    /// Decode and prepare the canonical serialized query accepted by host
    /// bindings. Keeping this here gives every binding the same validation,
    /// normalization, plan ownership, and immutable request scope.
    pub(crate) async fn prepare_serialized_query_async(
        &self,
        query: &[u8],
        request_scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
    ) -> Result<PreparedQuery, Error> {
        let query: Query = crate::wire::decode_postcard_exact(query)
            .map_err(|error| Error::new(ErrorCode::Query, format!("decode query: {error}")))?;
        let prepared = self.prepare_query_async(&query).await?;
        Ok(match request_scope {
            Some((author, claims)) => prepared.with_identity_claims(author, claims),
            None => prepared,
        })
    }

    /// Execute the complete serialized-query path for a host binding.
    ///
    /// Decoding, normalization, preparation, request-scope binding, coverage,
    /// execution, and binding hydration all remain owned by the core. The
    /// release callback lets a host defer attachment cleanup when dropping a
    /// pending operation while its runtime owner is already borrowed.
    #[doc(hidden)]
    #[allow(clippy::too_many_arguments)]
    pub async fn all_serialized_query<F, E>(
        &self,
        query: &[u8],
        opts: ReadOpts,
        open_tx: Option<OpenTransactionId>,
        request_scope: Option<(AuthorSubject, BTreeMap<String, Value>)>,
        author: Option<AuthorSubject>,
        require_coverage: bool,
        coverage_expired: E,
        release_coverage: F,
    ) -> Result<SerializedReadResult, Error>
    where
        F: FnOnce(QueryAttachment),
        E: Fn() -> bool,
    {
        let decoded: Query = crate::wire::decode_postcard_exact(query)
            .map_err(|error| Error::new(ErrorCode::Query, format!("decode query: {error}")))?;
        let is_relation = decoded.relation.is_some();
        if open_tx.is_some() && is_relation {
            return Err(Error::new(
                ErrorCode::Query,
                "relation reads inside a transaction are not supported",
            ));
        }
        let prepared = self.prepare_query_async(&decoded).await?;
        let prepared = match request_scope {
            Some((author, claims)) => prepared.with_identity_claims(author, claims),
            None => prepared,
        };
        // Read the maintained result itself, not just its coverage signal.
        // Dropping the subscription and re-evaluating below would retire its
        // exact request-scoped inputs before the one-shot consumes them.
        // Remote publication waits for settlement. A local foreground also
        // needs a fresh delivery from its durable owner, while the maintained
        // subscription drives the complete multi-hop input closure.
        if require_coverage && is_relation {
            let local_coverage = if effective_read_tier(&opts) == DurabilityTier::Local {
                Some(SerializedReadCoverage {
                    attachment: Some(
                        self.attach_query_with_opts_async(&prepared, opts.clone(), None, author)
                            .await?,
                    ),
                    release: Some(release_coverage),
                })
            } else {
                None
            };
            let mut stream = match author {
                Some(author) => {
                    self.subscribe_client_for_identity(&prepared, opts.clone(), author)
                        .await?
                }
                None => self.subscribe(&prepared, opts.clone()).await?,
            };
            let outcome = async {
                let mut next = Box::pin(stream.next_event());
                let event = std::future::poll_fn(|cx| {
                    if coverage_expired() {
                        return Poll::Ready(Err(Error::new(
                            ErrorCode::NotObserved,
                            "Timed out waiting for query coverage",
                        )));
                    }
                    if local_coverage.as_ref().is_some_and(|coverage| {
                        !self.query_attachment_is_covered(
                            coverage
                                .attachment
                                .as_ref()
                                .expect("live local read coverage"),
                        )
                    }) {
                        return Poll::Pending;
                    }
                    std::future::Future::poll(next.as_mut(), cx).map(Ok)
                })
                .await?;
                drop(next);
                match event {
                    Some(SubscriptionEvent::Delta { reset: true, .. }) => {
                        let mut snapshot = stream.settled_receiver_local_snapshot()?;
                        self.hydrate_relation_snapshot_for_binding(&mut snapshot)
                            .await?;
                        if prepared.shape().query().array_subqueries.is_empty() {
                            Ok(SerializedReadResult::Rows(
                                snapshot
                                    .rows
                                    .into_iter()
                                    .take(snapshot.root_count)
                                    .collect(),
                            ))
                        } else {
                            Ok(SerializedReadResult::Relation(snapshot))
                        }
                    }
                    Some(SubscriptionEvent::Rejected { reason }) => Err(Error::new(
                        ErrorCode::Query,
                        format!("query subscription rejected: {reason:?}"),
                    )),
                    Some(SubscriptionEvent::Closed) | None => Err(Error::new(
                        ErrorCode::NotObserved,
                        "query subscription ended before its published result",
                    )),
                    Some(SubscriptionEvent::Delta { .. }) => Err(Error::new(
                        ErrorCode::Protocol,
                        "query subscription did not open with a reset",
                    )),
                }
            }
            .await;
            let finalization = stream.close().await;
            return match (outcome, finalization) {
                (Ok(result), Ok(())) => Ok(result),
                (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            };
        }
        let coverage = if require_coverage {
            let attachment = self
                .attach_query_with_opts_async(&prepared, opts.clone(), open_tx, author)
                .await?;
            Some(SerializedReadCoverage {
                attachment: Some(attachment),
                release: Some(release_coverage),
            })
        } else {
            None
        };
        if let Some(coverage) = coverage.as_ref() {
            std::future::poll_fn(|_| {
                if self.query_attachment_is_covered(
                    coverage
                        .attachment
                        .as_ref()
                        .expect("live serialized read coverage"),
                ) {
                    Poll::Ready(Ok(()))
                } else if coverage_expired() {
                    Poll::Ready(Err(Error::new(
                        ErrorCode::NotObserved,
                        "Timed out waiting for query coverage",
                    )))
                } else {
                    Poll::Pending
                }
            })
            .await?;
        }

        if !prepared.shape().query().array_subqueries.is_empty() {
            let in_transaction = open_tx.is_some();
            let mut snapshot = match open_tx {
                Some(open_tx) => {
                    self.relation_snapshot_in_open_transaction(open_tx, &prepared, opts, author)
                        .await
                }
                None => match author {
                    Some(author) => {
                        self.all_relation_snapshot_for_identity(&prepared, opts, author)
                            .await
                    }
                    None => self.all_relation_snapshot(&prepared, opts).await,
                },
            }?;
            if !in_transaction {
                self.hydrate_relation_snapshot_for_binding(&mut snapshot)
                    .await?;
            }
            return Ok(SerializedReadResult::Relation(snapshot));
        }

        let mut rows = match open_tx {
            Some(open_tx) => {
                self.all_in_open_transaction(open_tx, &prepared, opts, author)
                    .await
            }
            None => match author {
                Some(author) => self.all_for_identity(&prepared, opts, author).await,
                None => self.all(&prepared, opts).await,
            },
        }?;
        self.hydrate_rows_for_binding(&mut rows).await?;
        Ok(SerializedReadResult::Rows(rows))
    }

    /// Prepare a query with explicit parameter bindings.
    pub fn prepare_query_bound(
        &self,
        query: &Query,
        params: BTreeMap<String, Value>,
    ) -> Result<PreparedQuery, Error> {
        let (schema, schema_version) = self.current_write_schema_for_query()?;
        self.prepare_query_bound_for_schema(query, params, &schema, schema_version)
    }

    /// Prepare a query against the schema this database handle was opened with.
    ///
    /// Typed client facades are pinned to that schema even when a catalogue
    /// snapshot advances or rolls back the separate current-write pointer.
    #[cfg(feature = "runtime")]
    pub(crate) fn prepare_query_for_open_schema(
        &self,
        query: &Query,
    ) -> Result<PreparedQuery, Error> {
        self.prepare_query_bound_for_schema(
            query,
            BTreeMap::new(),
            &self.schema,
            self.schema_version_id,
        )
    }

    fn prepare_query_bound_for_schema(
        &self,
        query: &Query,
        params: BTreeMap<String, Value>,
        schema: &JazzSchema,
        schema_version: SchemaVersionId,
    ) -> Result<PreparedQuery, Error> {
        let shape = query.validate_with_schema_version(schema, schema_version)?;
        let binding = shape.bind(params)?;
        let (local_plan, global_plan) = if should_install_prepared_plan(&shape)
            && !self.node.node.borrow().uses_schema_projected_read(&shape)
        {
            let mut node = self.node.node.borrow_mut();
            (
                Some(super::block_on(node.prepared_query_plan(
                    &shape,
                    &binding,
                    DurabilityTier::Local,
                    AuthorSubject::SYSTEM,
                ))?),
                Some(super::block_on(node.prepared_query_plan(
                    &shape,
                    &binding,
                    DurabilityTier::Global,
                    AuthorSubject::SYSTEM,
                ))?),
            )
        } else {
            (None, None)
        };
        let groove_runtime_token = self.node.node.borrow().groove_runtime_token();
        Ok(PreparedQuery {
            request_identity_claims: None,
            shape,
            binding,
            local_plan,
            global_plan,
            groove_runtime_token,
        })
    }

    /// Synchronously read rows for a prepared query.
    ///
    /// This is a synchronous local-preview read. Upstream/server settled
    /// coverage is tracked separately by query attachments and durability-aware
    /// subscription reads.
    pub fn read(&self, prepared: &PreparedQuery) -> Result<Vec<CurrentRow>, Error> {
        let mut node = self.node.node.borrow_mut();
        let groove_runtime_token = node.groove_runtime_token();
        super::block_on(node.query_rows_local_preview(
            &prepared.shape,
            &prepared.binding,
            prepared.plan_for_tier(DurabilityTier::Local, groove_runtime_token),
        ))
        .map_err(Into::into)
    }

    #[cfg(any(test, feature = "testing"))]
    /// Test-only count of live Groove maintained subscriptions.
    pub fn active_groove_subscriptions_for_test(&self) -> usize {
        self.node
            .node
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions
    }

    /// Synchronously read rows and attribute work inside the node query path.
    ///
    /// The returned rows are identical to [`Self::read`]. This diagnostic
    /// variant exists so persisted-read benchmarks can locate first-read cost
    /// without adding clocks to the ordinary read path.
    pub fn read_profiled(
        &self,
        prepared: &PreparedQuery,
    ) -> Result<(Vec<CurrentRow>, QueryReadProfile), Error> {
        let mut node = self.node.node.borrow_mut();
        let groove_runtime_token = node.groove_runtime_token();
        super::block_on(node.query_rows_local_preview_profiled(
            &prepared.shape,
            &prepared.binding,
            prepared.plan_for_tier(DurabilityTier::Local, groove_runtime_token),
        ))
        .map_err(Into::into)
    }

    /// Synchronously read exactly one local row if present.
    ///
    /// ```rust
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// let db = block_on(open_todos_db())?;
    /// let todo = block_on(db.insert(
    ///     "todos",
    ///     todo_cells("first item", false),
    ///     Default::default(),
    /// ))?
    /// .row_uuid();
    ///
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// let found = db.one(&todos)?;
    /// assert_eq!(found.map(|row| row.row_uuid()), Some(todo));
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn one(&self, prepared: &PreparedQuery) -> Result<Option<CurrentRow>, Error> {
        Ok(self.read(prepared)?.into_iter().next())
    }

    /// Resolve creator/updater provenance for a row returned by this database.
    pub fn row_provenance(&self, row: &CurrentRow) -> Result<Option<RowProvenance>, Error> {
        self.node
            .node
            .borrow_mut()
            .row_provenance(row)
            .map_err(Into::into)
    }

    /// Resolve provenance after a storage-suspended node turn releases its
    /// owner mutex.
    pub async fn row_provenance_async(
        &self,
        row: &CurrentRow,
    ) -> Result<Option<RowProvenance>, Error> {
        self.node
            .node
            .lock()
            .await
            .row_provenance(row)
            .map_err(Into::into)
    }

    /// Read local settled history at an exact global timestamp cut.
    ///
    /// History-incomplete facades return `HistoricalReadRequiresServer` from
    /// the node layer instead of answering from a partial local prefix
    /// (ch11/INV-BRANCH-4).
    pub fn at(
        &self,
        position: GlobalTime,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.at_prepared(position, prepared)
    }

    fn at_prepared(
        &self,
        position: GlobalTime,
        prepared: &PreparedQuery,
    ) -> Result<Vec<CurrentRow>, Error> {
        super::block_on(
            self.node
                .node
                .borrow_mut()
                .at(position)
                .read(&prepared.shape, &prepared.binding),
        )
        .map_err(Into::into)
    }

    /// Tier-gated one-shot read.
    ///
    /// ```rust
    /// # use jazz::db::{ReadOpts, LocalUpdates, Propagation};
    /// # use jazz::db::doctest_support::{block_on, open_todos_db, todo_cells};
    /// # use jazz::tx::DurabilityTier;
    /// let db = block_on(open_todos_db())?;
    /// block_on(db.insert(
    ///     "todos",
    ///     todo_cells("visible locally", false),
    ///     Default::default(),
    /// ))?;
    ///
    /// let opts = ReadOpts {
    ///     tier: DurabilityTier::Local,
    ///     local_updates: LocalUpdates::Immediate,
    ///     propagation: Propagation::LocalOnly,
    ///     include_deleted: false,
    ///     ..ReadOpts::default()
    /// };
    /// let todos = db.prepare_query(&db.table("todos"))?;
    /// let rows = block_on(db.all(&todos, opts))?;
    /// assert_eq!(rows.len(), 1);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub async fn all(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_for_identity_in_authorization_mode(
            prepared,
            opts,
            self.identity.author,
            QueryAuthorizationMode::ClientLocal,
        )
        .await
    }

    /// Tier-gated one-shot read evaluated by a trusted host as `author`.
    ///
    /// Ordinary client reads use [`Db::all`] and never re-run read policy over
    /// locally received data. This explicit identity entry point is reserved
    /// for serving/request hosts that own policy enforcement before emission.
    pub async fn all_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<Vec<CurrentRow>, Error> {
        self.all_for_identity_in_authorization_mode(
            prepared,
            opts,
            author,
            QueryAuthorizationMode::TrustedServing,
        )
        .await
    }

    async fn all_for_identity_in_authorization_mode(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
        authorization_mode: QueryAuthorizationMode,
    ) -> Result<Vec<CurrentRow>, Error> {
        let tier = effective_read_tier(&opts);
        // Follow the same host-selected authority route as subscription
        // registration. Storage durability alone cannot identify that route:
        // a direct core connection may raise Edge to Global, while a relay
        // connection retains Edge. Local knowledge stays local in either case.
        let tier = if authorization_mode == QueryAuthorizationMode::ClientLocal {
            self.client_authority_read_tier(tier)
        } else {
            tier
        };
        let mut node = self.node.node.lock().await;
        let mut node = prepared.scoped_node(&mut node, author)?;
        if !matches!(opts.read_view.source, ReadViewSourceSpec::Current) {
            ensure_supported_read_view(&opts)?;
            if opts.include_deleted {
                return Err(Error::new(
                    ErrorCode::Query,
                    "branch views do not support include_deleted yet",
                ));
            }
            let snapshot = match authorization_mode {
                QueryAuthorizationMode::TrustedServing | QueryAuthorizationMode::EdgeServing => {
                    node.query_relation_snapshot_for_serving_in_read_view(
                        &prepared.shape,
                        &prepared.binding,
                        tier,
                        author,
                        &opts.read_view,
                    )
                    .await
                }
                QueryAuthorizationMode::ClientLocal => {
                    node.query_relation_snapshot_for_client(
                        &prepared.shape,
                        &prepared.binding,
                        tier,
                        author,
                        &opts.read_view,
                    )
                    .await
                }
            }?;
            return Ok(snapshot
                .rows
                .into_iter()
                .take(snapshot.root_count)
                .collect());
        }
        match (opts.include_deleted, authorization_mode) {
            (true, mode) => {
                node.query_rows_including_deleted_in_authorization_mode(
                    &prepared.shape,
                    &prepared.binding,
                    tier,
                    None,
                    author,
                    mode,
                )
                .await
            }
            (
                false,
                QueryAuthorizationMode::TrustedServing | QueryAuthorizationMode::EdgeServing,
            ) => {
                node.query_rows_with_prepared_plan_for_identity(
                    &prepared.shape,
                    &prepared.binding,
                    tier,
                    None,
                    author,
                )
                .await
            }
            (false, QueryAuthorizationMode::ClientLocal) => {
                // A client consumes identity-scoped rows emitted by its
                // trusted upstream; local reads must not apply policy again.
                node.query_rows_for_client(&prepared.shape, &prepared.binding, tier, author)
                    .await
            }
        }
        .map_err(Into::into)
    }

    /// Resolve physical indirect scalars before a subscription event crosses
    /// a language binding boundary.
    #[doc(hidden)]
    pub async fn hydrate_subscription_event_for_binding(
        &self,
        event: &mut SubscriptionEvent,
    ) -> Result<(), Error> {
        self.hydrate_subscription_event_for_binding_outcome(event)
            .await
            .map_err(|error| match error {
                BindingHydrationError::RetryableChunkUnavailable { .. } => Error::new(
                    ErrorCode::NotObserved,
                    "large-value chunk is temporarily unavailable",
                ),
                BindingHydrationError::Error(error) => error,
            })
    }

    /// Like [`Self::hydrate_subscription_event_for_binding`], but retains the
    /// sole retryable cause for host bindings that can await chunk delivery.
    #[doc(hidden)]
    pub async fn hydrate_subscription_event_for_binding_outcome(
        &self,
        event: &mut SubscriptionEvent,
    ) -> Result<(), BindingHydrationError> {
        let SubscriptionEvent::Delta {
            added,
            updated,
            terminal_operations,
            ..
        } = event
        else {
            return Ok(());
        };
        let node = self.node.node.lock().await;
        if terminal_operations.is_empty() {
            for output in added.iter_mut().chain(updated.iter_mut()) {
                node.hydrate_current_rows(std::slice::from_mut(&mut output.row))
                    .await
                    .map_err(binding_hydration_error)?;
            }
        } else {
            // Structured terminal edits replace the row batches at this
            // binding. Do not fetch discarded rows just because the internal
            // event happened to retain them for its own reconciliation.
            for operation in terminal_operations {
                if matches!(
                    &operation.edit,
                    groove::ivm::TerminalEdit::Remove { .. }
                        | groove::ivm::TerminalEdit::Move { .. }
                ) {
                    continue;
                }
                let descriptor = terminal_operation_value_descriptor(operation)?;
                let value = match &mut operation.edit {
                    groove::ivm::TerminalEdit::Insert { value, .. }
                    | groove::ivm::TerminalEdit::Update { value, .. } => value,
                    groove::ivm::TerminalEdit::Remove { .. }
                    | groove::ivm::TerminalEdit::Move { .. } => unreachable!(
                        "terminal operation payload shape changed after classification"
                    ),
                };
                node.hydrate_encoded_record(&descriptor, value)
                    .await
                    .map_err(binding_hydration_error)?;
            }
        }
        Ok(())
    }

    /// Resolve physical indirect scalars in ordinary row output immediately
    /// before a language binding encodes it.
    #[doc(hidden)]
    pub async fn hydrate_rows_for_binding(&self, rows: &mut [CurrentRow]) -> Result<(), Error> {
        self.node
            .node
            .lock()
            .await
            .hydrate_current_rows(rows)
            .await?;
        Ok(())
    }

    /// Resolve physical indirect scalars in a relation snapshot immediately
    /// before a language binding encodes it.
    #[doc(hidden)]
    pub async fn hydrate_relation_snapshot_for_binding(
        &self,
        snapshot: &mut RelationSnapshot,
    ) -> Result<(), Error> {
        self.hydrate_rows_for_binding(&mut snapshot.rows).await
    }

    /// Use the host-selected authority tier, matching subscription registration.
    /// Local reads remain local regardless of the upstream durability floor.
    fn client_authority_read_tier(&self, tier: DurabilityTier) -> DurabilityTier {
        if tier >= DurabilityTier::Edge {
            remote_subscription_tier(tier, self.node.upstream_durability_floor.get())
        } else {
            tier
        }
    }

    /// Tier-gated one-shot relation read evaluated as the database identity.
    pub async fn all_relation_snapshot(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        ensure_supported_read_view(&opts)?;
        if opts.include_deleted {
            return Err(Error::new(
                ErrorCode::Query,
                "relation snapshots do not support include_deleted yet",
            ));
        }
        let tier = self.client_authority_read_tier(effective_read_tier(&opts));
        let mut owner = self.node.node.lock().await;
        let mut node = prepared.scoped_node(&mut owner, self.identity.author)?;
        node.query_relation_snapshot_for_client(
            &prepared.shape,
            &prepared.binding,
            tier,
            self.identity.author,
            &opts.read_view,
        )
        .await
        .map_err(Into::into)
    }

    /// Tier-gated one-shot relation read evaluated as `author`.
    pub async fn all_relation_snapshot_for_identity(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<RelationSnapshot, Error> {
        ensure_supported_read_view(&opts)?;
        if opts.include_deleted {
            return Err(Error::new(
                ErrorCode::Query,
                "relation snapshots do not support include_deleted yet",
            ));
        }
        let tier = effective_read_tier(&opts);
        let mut owner = self.node.node.lock().await;
        let mut node = prepared.scoped_node(&mut owner, author)?;
        node.query_relation_snapshot_for_serving_in_read_view(
            &prepared.shape,
            &prepared.binding,
            tier,
            author,
            &opts.read_view,
        )
        .await
        .map_err(Into::into)
    }

    /// Tier-gated canonical structured result read.
    ///
    /// This is the sole Jazz-boundary materialization of relation facts into
    /// recursive output. Wire delivery deliberately remains on its v3 carrier
    /// until the structured delivery migration.
    pub async fn all_result_tree(
        &self,
        prepared: &PreparedQuery,
        opts: ReadOpts,
    ) -> Result<ResultTree, Error> {
        let snapshot = self.all_relation_snapshot(prepared, opts).await?;
        materialize_result_tree(prepared.shape.query(), snapshot)
    }

    /// Tier-gated one-shot output-changing relation read evaluated as the database identity.
    pub async fn all_relation_query(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
    ) -> Result<RelationSnapshot, Error> {
        ensure_default_read_view(&opts)?;
        let prepared = self.prepare_relation_query_async(query).await?;
        // Output-changing relation queries currently normalize to a single
        // root row set. They have no array payload edges, so request ordinary
        // app rows instead of the relation-snapshot fact output (which is
        // reserved for correlated array/path materialization).
        let rows = self.all(&prepared, opts).await?;
        Ok(RelationSnapshot {
            root_count: rows.len(),
            rows,
            edges: Vec::new(),
        })
    }

    /// Tier-gated one-shot output-changing relation read evaluated as `author`.
    pub async fn all_relation_query_for_identity(
        &self,
        query: &RelationQuery,
        opts: ReadOpts,
        author: AuthorSubject,
    ) -> Result<RelationSnapshot, Error> {
        ensure_default_read_view(&opts)?;
        let prepared = self.prepare_relation_query_async(query).await?;
        // Output-changing relation queries currently normalize to a single
        // root row set.  They have no array payload edges, so request ordinary
        // app rows instead of the relation-snapshot fact output (which is
        // reserved for correlated array/path materialization).
        let rows = self.all_for_identity(&prepared, opts, author).await?;
        Ok(RelationSnapshot {
            root_count: rows.len(),
            rows,
            edges: Vec::new(),
        })
    }
}

/// The descriptor on a terminal operation describes its root record. Nested
/// insertions and updates carry only their child record bytes, so follow the
/// operation path to find the descriptor which actually owns `edit.value`.
///
/// Terminal paths deliberately omit a child key for insertions: the edit owns
/// that key. A key segment therefore validates the path shape but does not
/// change the descriptor.
fn terminal_operation_value_descriptor(
    operation: &groove::ivm::TerminalOperation,
) -> Result<RecordDescriptor, BindingHydrationError> {
    use groove::ivm::TerminalPathSegment;
    use groove::records::ValueType;

    let mut descriptor = operation.root_descriptor;
    let mut expect_collection = true;
    for segment in &operation.path {
        match (expect_collection, segment) {
            (true, TerminalPathSegment::Collection(name)) => {
                let Some(field) = descriptor
                    .fields()
                    .iter()
                    .find(|field| field.name.as_deref() == Some(name))
                else {
                    return Err(BindingHydrationError::Error(Error::new(
                        ErrorCode::Protocol,
                        "terminal operation references an unknown collection field",
                    )));
                };
                let ValueType::Array(element) = &field.value_type else {
                    return Err(BindingHydrationError::Error(Error::new(
                        ErrorCode::Protocol,
                        "terminal operation collection field is not an array",
                    )));
                };
                let ValueType::Record(child) = element.as_ref() else {
                    return Err(BindingHydrationError::Error(Error::new(
                        ErrorCode::Protocol,
                        "terminal operation collection does not contain records",
                    )));
                };
                descriptor = **child;
                expect_collection = false;
            }
            (false, TerminalPathSegment::Key(_)) => expect_collection = true,
            (true, TerminalPathSegment::Key(_)) => {
                return Err(BindingHydrationError::Error(Error::new(
                    ErrorCode::Protocol,
                    "terminal operation path starts with a key",
                )));
            }
            (false, TerminalPathSegment::Collection(_)) => {
                return Err(BindingHydrationError::Error(Error::new(
                    ErrorCode::Protocol,
                    "terminal operation path is missing a child key",
                )));
            }
        }
    }
    Ok(descriptor)
}
