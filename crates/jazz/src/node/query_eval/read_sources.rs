//! Turn logical query read requirements into concrete Groove source graphs.
//!
//! This stage chooses physical, inline, historical, branch, or settled-view
//! inputs and applies durability, schema projection, and access-path decisions.
//! It does not normalize query syntax or materialize engine output into public
//! rows.

use super::*;
use crate::node::query_engine::{BranchViewSourceBase, current_row_field_names};
use std::{future::Future, pin::Pin};

fn current_row_column_field(
    column: &crate::schema::ColumnSchema,
    value_type: ValueType,
) -> records::DescriptorField {
    records::DescriptorField::new(user_column_field(&column.name), value_type)
        .with_identity(records::FieldIdentity::Name(column.name.clone()))
}
pub(super) struct JazzSourceGraphPreparer<'a, S> {
    /// Exact app context survives ClientLocal's policy-free source lowering.
    pub(super) local_unavailable_scope: Option<crate::protocol::PolicyBindingKey>,
    pub(super) node: &'a mut NodeState<S>,
    pub(super) read_view: &'a ReadView<RequestedSourceStage>,
    pub(super) inline_sources: BTreeMap<SourceId, Vec<CurrentRow>>,
    /// Receiver-local mutable inputs for one exact authority-covered program
    /// closure. The mapping is keyed by normalized source identity, never by
    /// a table name, sink, collector, or storage prefix.
    pub(super) covered_input_sources: BTreeMap<SourceId, groove::ivm::InputSourceId>,
    /// The exact compiler-owned descriptor registered for each receiver
    /// source. Input lowering must reuse it rather than reconstructing an
    /// authored descriptor after the physical catalogue has selected a
    /// canonical enum/schema boundary.
    pub(super) covered_input_descriptors: BTreeMap<SourceId, RecordDescriptor>,
    pub(super) access_paths: BTreeMap<SourceId, CurrentAccessPath>,
    /// Whether access-path metrics should account for this logical graph
    /// fragment. A policy proof specialized from its outer source reuses the
    /// same deduplicated physical source node, so only the outer fragment owns
    /// that source-plan receipt.
    pub(super) count_access_path_metrics: bool,
    /// Query-local enum boundary targets, keyed by logical source.  Defining
    /// a variant target invalidates table inputs, so reuse it across the main
    /// source, access path, and metadata sidecars of one compiled program.
    pub(super) current_projection_targets: BTreeMap<SourceId, String>,
}

pub(super) struct CurrentSourceGraph {
    pub(super) graph: GraphBuilder,
    pub(super) descriptor: RecordDescriptor,
    pub(super) metadata: BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
}

#[derive(Clone, Debug)]
pub(super) enum CurrentAccessPath {
    PrimaryKey(Vec<Value>),
    Index {
        column: String,
        prefix: Vec<Value>,
        intersections: Vec<(String, Vec<Value>)>,
        /// A maintained source keeps every equality probe as an ordinary IVM
        /// source and intersects them in the graph. The fused storage request
        /// is snapshot-only and cannot observe later transitions through a
        /// secondary equality index.
        maintained: bool,
        /// A proved physical source cap for an ordinary one-shot read. This is
        /// never selected by policy compilation or subscriptions.
        source_limit: Option<usize>,
    },
}

impl<S> JazzSourceGraphPreparer<'_, S>
where
    S: OrderedKvStorage,
{
    fn local_exclusion_scope(
        &self,
        request: &SourceRequest,
    ) -> Option<crate::protocol::PolicyBindingKey> {
        (!matches!(
            request.authorization,
            SourceAuthorizationRequest::PolicyProof { .. }
        ) && self
            .read_view
            .sources
            .get(&request.source)
            .is_some_and(unavailable_inputs::is_current_app_source))
        .then(|| self.local_unavailable_scope.clone())
        .flatten()
    }

    fn excludes_below_pending(&self, request: &SourceRequest) -> bool {
        self.local_exclusion_scope(request).is_some()
            && !self.inline_sources.contains_key(&request.source)
            && match self.read_view.sources.get(&request.source) {
                Some(SourceExpr::VisibleCurrent {
                    tier: DurabilityTier::Local,
                    ..
                }) => true,
                Some(SourceExpr::WithOverlays { overlays, .. }) => {
                    overlays.entries == [OverlayRef::PendingLocal]
                }
                _ => false,
            }
    }

    async fn exclude_settled_arm(
        &mut self,
        request: &SourceRequest,
        graph: GraphBuilder,
        pending_ahead: bool,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        if !self.excludes_below_pending(request) {
            return Ok(graph);
        }
        let scope = self.local_exclusion_scope(request).expect("checked scope");
        self.node
            .exclude_local_unavailable_graph(
                &scope,
                self.read_view.read_schema,
                request,
                graph,
                pending_ahead,
            )
            .await
            .map_err(|_| source_resolution_error(request, SourceGap::LocalAvailabilityInput))
    }

    fn stored_column_ids_for_read_table(
        &self,
        request: &SourceRequest,
        table: &TableSchema,
    ) -> Result<BTreeMap<String, PhysicalColumnId>, SourceResolutionError> {
        let mapping = self
            .node
            .catalogue
            .physical_mappings
            .get(&self.read_view.read_schema)
            .ok_or_else(|| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let table_mapping = mapping
            .tables
            .get(&table.name)
            .ok_or_else(|| source_resolution_error(request, SourceGap::SchemaProjection))?;
        table
            .columns
            .iter()
            .map(|column| {
                table_mapping
                    .columns
                    .get(&column.name)
                    .copied()
                    .map(|id| (column.name.clone(), id))
                    .ok_or_else(|| source_resolution_error(request, SourceGap::SchemaProjection))
            })
            .collect()
    }

    /// The complete source-family dispatcher. Keep uncommon historical and
    /// branch paths out of the ordinary inline policy-evaluation frame.
    async fn prepare_source_graph_dispatch(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, SourceResolutionError> {
        let Some(source) = self.read_view.sources.get(&request.source) else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        // The overlay is explicit in the source expression; Local-first is
        // ordinary local storage, never an implicit authority-scoped overlay.
        let pending_current = match source {
            SourceExpr::WithOverlays { input, overlays }
                if overlays.entries == [OverlayRef::PendingLocal] =>
            {
                match input.as_ref() {
                    SourceExpr::SettledBindingView { projection, .. } => {
                        Some(SourceExpr::VisibleCurrent {
                            projection: projection.clone(),
                            data: DataSource::Current,
                            tier: DurabilityTier::Local,
                        })
                    }
                    _ => return Err(source_resolution_error(request, SourceGap::Coverage)),
                }
            }
            _ => None,
        };
        let pending_overlay = pending_current.is_some();
        let source = pending_current.as_ref().unwrap_or(source);
        let (covered_input_source, covered_input_descriptor) =
            if request.visibility == RowVisibility::Visible {
                (
                    self.covered_input_sources.get(&request.source).copied(),
                    self.covered_input_descriptors.get(&request.source).cloned(),
                )
            } else {
                (None, None)
            };
        if covered_input_source.is_some() != covered_input_descriptor.is_some() {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        }

        // A receiver-local Local source has exactly two possible inputs: the
        // authority-covered frontier and this node's still-pending Ahead
        // overlay.  It must never reopen the ordinary Local source here:
        // that source also contains Global rows previously received from the
        // authority, which would keep a retracted covered row alive.
        let receiver_local_overlay = covered_input_source.is_some() && pending_overlay;
        if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some()
            && !self.covered_input_sources.is_empty()
        {
            eprintln!(
                "JAZZ_COVERED_INPUT_TRACE stage=covered_input_lookup request={:?} matched={} candidates={:?}",
                request.source,
                covered_input_source.is_some(),
                self.covered_input_sources.keys().collect::<Vec<_>>(),
            );
        }
        if let Some(input_source) = covered_input_source
            && (matches!(source, SourceExpr::SettledBindingView { .. })
                // Strict remote source occurrences have no eligible local
                // alternative. Aggregate output is synthetic, so its raw
                // remote contributor source likewise has to bottom out at
                // the covered input rather than storage.
                || matches!(
                    source,
                    SourceExpr::VisibleCurrent { tier, .. } if *tier >= DurabilityTier::Edge
                ))
        {
            // The compiler created this source map only for an exact
            // authority-covered occurrence. The explicit map, rather than
            // the source expression's spelling, is the capability.
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(request, SourceGap::Coverage));
            }
            let table = self
                .node
                .table_in_schema(&request.source.table, self.read_view.read_schema)
                .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
            let metadata = covered_input_source_metadata(&request.requirements, &table);
            let descriptor = covered_input_descriptor
                .clone()
                .expect("checked alongside compiler-owned covered input source");
            return Ok(ResolvedSource {
                stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?,
                table_schema: table,
                graph: GraphBuilder::input_source(input_source, descriptor.clone()),
                row_shape: SourceRowShape {
                    source: request.source.clone(),
                    descriptor,
                    row_uuid_field: "row_uuid".to_owned(),
                    metadata,
                },
                routing_fields: BTreeSet::new(),
                requires_result_payload: false,
                content_version: None,
                deletion_register: None,
                authorized_deletion_preimage: None,
            });
        }
        let (projection, graph_tier, history_position, snapshot, open_tx_overlay, branch_view) =
            match source {
                SourceExpr::VisibleCurrent {
                    projection,
                    data: DataSource::Current,
                    tier,
                } => (projection, Some(*tier), None, None, None, None),
                SourceExpr::BranchView {
                    projection,
                    head,
                    base,
                    tier,
                } => (
                    projection,
                    Some(*tier),
                    None,
                    None,
                    None,
                    Some((head, base.as_ref())),
                ),
                SourceExpr::HistoryCut {
                    projection,
                    data: DataSource::Current,
                    position,
                } => (projection, None, Some(*position), None, None, None),
                SourceExpr::SnapshotRef {
                    projection,
                    data: DataSource::Current,
                    snapshot,
                } => (projection, None, None, Some(snapshot.clone()), None, None),
                // A settled authority binding is not a source of application
                // rows. The caller must provide an exact, descriptor-bound
                // CoveredInput source for every compiled occurrence before
                // this resolver runs. Falling back to result members,
                // ResultPayload, or retained Global storage would create a
                // second evaluator and could resurrect revoked rows.
                SourceExpr::SettledBindingView { .. } => {
                    return Err(source_resolution_error(request, SourceGap::Coverage));
                }
                SourceExpr::WithOverlays { input, overlays } => {
                    let (projection, tier) = match input.as_ref() {
                        SourceExpr::VisibleCurrent {
                            projection,
                            data: DataSource::Current,
                            tier,
                        } => (projection, Some(*tier)),
                        SourceExpr::SnapshotRef {
                            projection,
                            data: DataSource::Current,
                            snapshot: _,
                        } => (projection, None),
                        _ => {
                            return Err(source_resolution_error(
                                request,
                                SourceGap::TransactionReadOverlay,
                            ));
                        }
                    };
                    let [OverlayRef::OpenTransaction(tx_id)] = overlays.entries.as_slice() else {
                        return Err(source_resolution_error(
                            request,
                            SourceGap::TransactionReadOverlay,
                        ));
                    };
                    (projection, tier, None, None, Some(*tx_id), None)
                }
                _ => {
                    return Err(source_resolution_error(
                        request,
                        SourceGap::HistoricalStorageCut,
                    ));
                }
            };
        if !matches!(projection.schema_family, SchemaFamilySelection::Current)
            || !matches!(
                projection.storage,
                StorageSchemaSelection::Single(_) | StorageSchemaSelection::CompatiblePartitions
            )
            || !matches!(projection.lens, LensSelection::Canonical)
        {
            return Err(source_resolution_error(
                request,
                SourceGap::SchemaProjection,
            ));
        }
        let table = self
            .node
            .table_in_schema(&request.source.table, self.read_view.read_schema)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        // Policy-proof dependencies are raw evidence for the outer policy.
        // Re-applying their own read policy recursively both changes the
        // outer predicate's meaning and can manufacture a proof cycle.
        let authorization = if matches!(
            request.authorization,
            SourceAuthorizationRequest::PolicyProof { .. }
        ) {
            SourceAuthorizationRequest::System
        } else {
            request.authorization.clone()
        };
        if let Some(rows) = self.inline_sources.get(&request.source) {
            if request.visibility != RowVisibility::Visible
                || !matches!(authorization, SourceAuthorizationRequest::System)
            {
                return Err(source_resolution_error(request, SourceGap::Coverage));
            }
            let schema_version_alias = self
                .node
                .ensure_schema_version_alias(self.read_view.read_schema)
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let (graph, descriptor, metadata) = inline_current_graph_with_source_metadata(
                &table,
                rows.clone(),
                schema_version_alias,
                "inline-current",
                &request.requirements,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            return Ok(ResolvedSource {
                stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?,
                table_schema: table,
                graph,
                row_shape: SourceRowShape {
                    source: request.source.clone(),
                    descriptor,
                    row_uuid_field: "row_uuid".to_owned(),
                    metadata,
                },
                routing_fields: BTreeSet::new(),
                requires_result_payload: false,
                content_version: None,
                deletion_register: None,
                authorized_deletion_preimage: None,
            });
        }
        let snapshot_source = snapshot.is_some();
        let mut snapshot_content_version = None;
        let (graph, descriptor, metadata, routing_fields) = if let Some((head, base)) = branch_view
        {
            if request.visibility == RowVisibility::IncludeDeleted
                && base.is_none_or(|base| matches!(base, BranchViewSourceBase::Current(_)))
            {
                let tier = graph_tier.expect("branch view has a current tier");
                let head_keys = self
                    .node
                    .equivalent_stored_branch_keys(
                        &request.source.table,
                        self.read_view.read_schema,
                        head,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let head_content = self
                    .projected_branch_content_source_graph(request, &table, tier, &head_keys)
                    .await?;
                let head_deletions = self
                    .projected_branch_deletion_source_graph(request, tier, &head_keys)
                    .await?;
                // A head-over-current BranchView selects its winner from the
                // head when present and otherwise from the base.  The
                // IncludeDeleted sibling must use that exact same selection
                // before policy evaluation, or an authorized base preimage
                // could accidentally attest to a distinct head deletion.
                let (content, deletions) = match base {
                    Some(BranchViewSourceBase::Current(base)) if base != head => {
                        let base_keys = self
                            .node
                            .equivalent_stored_branch_keys(
                                &request.source.table,
                                self.read_view.read_schema,
                                base,
                            )
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                        let base_content = self
                            .projected_branch_content_source_graph(
                                request, &table, tier, &base_keys,
                            )
                            .await?;
                        let base_deletions = self
                            .projected_branch_deletion_source_graph(request, tier, &base_keys)
                            .await?;
                        (
                            GraphBuilder::union([
                                head_content.clone(),
                                GraphBuilder::anti_join(
                                    base_content,
                                    head_content,
                                    ["row_uuid"],
                                    ["row_uuid"],
                                ),
                            ]),
                            GraphBuilder::union([
                                head_deletions.clone(),
                                GraphBuilder::anti_join(
                                    base_deletions,
                                    head_deletions,
                                    ["row_uuid"],
                                    ["row_uuid"],
                                ),
                            ]),
                        )
                    }
                    _ => (head_content, head_deletions),
                };
                let base = include_deleted_branch_graph(&table, head, content, deletions)
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let graph = match &authorization {
                    SourceAuthorizationRequest::System => base,
                    SourceAuthorizationRequest::PolicyFiltered {
                        permission_subject,
                        plan,
                    }
                    | SourceAuthorizationRequest::PolicyProof {
                        permission_subject,
                        plan,
                    } => {
                        let policy_request = self
                            .node
                            .table_read_policy_authorization_request_for_include_deleted(
                                self.read_view.policy_schema,
                                &table.name,
                                *permission_subject,
                                tier,
                                plan.binding_source_shape.clone(),
                                plan.binding_user_params.clone(),
                                plan.binding_claim_params.clone(),
                            );
                        let policy_request = policy_request.map(|mut request| {
                            request.reads.primary = policy_read_view_projected_through(
                                &request.reads.primary,
                                self.read_view,
                            );
                            request
                        });
                        let mut output_fields = current_row_fields(&table);
                        output_fields.push("__jazz_deleted".to_owned());
                        self.node
                            .compose_policy_filtered_current_source_graph(
                                policy_request,
                                base,
                                &output_fields,
                            )
                            .map_err(|error| {
                                source_resolution_error_from_policy_proof(request, error)
                            })?
                            .graph
                    }
                };
                let (graph, descriptor, metadata) = with_requested_author_paths(
                    graph,
                    include_deleted_current_row_descriptor(&table),
                    BTreeMap::new(),
                    &request.requirements,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                return Ok(ResolvedSource {
                    stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?,
                    table_schema: table.clone(),
                    graph,
                    row_shape: SourceRowShape {
                        source: request.source.clone(),
                        descriptor,
                        row_uuid_field: "row_uuid".to_owned(),
                        metadata,
                    },
                    routing_fields: BTreeSet::new(),
                    requires_result_payload: false,
                    content_version: None,
                    deletion_register: None,
                    authorized_deletion_preimage: None,
                });
            }
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(request, SourceGap::Coverage));
            }
            if base.is_none_or(|base| matches!(base, BranchViewSourceBase::Current(_))) {
                let branch_witness_field =
                    (!table.branch_by.is_empty()).then_some("supplying_branch_key");
                let head_keys = self
                    .node
                    .equivalent_stored_branch_keys(
                        &request.source.table,
                        self.read_view.read_schema,
                        head,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let head_content = self
                    .projected_branch_content_source_graph(
                        request,
                        &table,
                        graph_tier.expect("branch view has a current tier"),
                        &head_keys,
                    )
                    .await?;
                let head_deletions = self
                    .projected_branch_deletion_source_graph(
                        request,
                        graph_tier.expect("branch view has a current tier"),
                        &head_keys,
                    )
                    .await?;
                let (content, deletions) = match base {
                    Some(BranchViewSourceBase::Current(base)) if base != head => {
                        let base_keys = self
                            .node
                            .equivalent_stored_branch_keys(
                                &request.source.table,
                                self.read_view.read_schema,
                                base,
                            )
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                        let base_content = self
                            .projected_branch_content_source_graph(
                                request,
                                &table,
                                graph_tier.expect("branch view has a current tier"),
                                &base_keys,
                            )
                            .await?;
                        let base_deletions = self
                            .projected_branch_deletion_source_graph(
                                request,
                                graph_tier.expect("branch view has a current tier"),
                                &base_keys,
                            )
                            .await?;
                        (
                            GraphBuilder::union([
                                head_content.clone(),
                                GraphBuilder::anti_join(
                                    base_content,
                                    head_content,
                                    ["row_uuid"],
                                    ["row_uuid"],
                                ),
                            ]),
                            GraphBuilder::union([
                                head_deletions.clone(),
                                GraphBuilder::anti_join(
                                    base_deletions,
                                    head_deletions,
                                    ["row_uuid"],
                                    ["row_uuid"],
                                ),
                            ]),
                        )
                    }
                    _ => (head_content, head_deletions),
                };
                let content_version = request
                    .requirements
                    .metadata
                    .iter()
                    .any(|requirement| {
                        matches!(
                            requirement,
                            SourceMetadataRequirement::VersionPayloads
                                | SourceMetadataRequirement::VersionWitnesses
                                | SourceMetadataRequirement::Provenance(_)
                        )
                    })
                    .then(|| ContentVersionSource {
                        graph: content.clone().project_fields(
                            maintained_view_history_storage_field_names(&table)
                                .into_iter()
                                .map(ProjectField::named)
                                .chain(std::iter::once(ProjectField::renamed(
                                    "branch_key",
                                    "supplying_branch_key",
                                ))),
                        ),
                        row_uuid_field: "row_uuid".to_owned(),
                    });
                let deletion_register = request
                    .requirements
                    .metadata
                    .contains(&SourceMetadataRequirement::DeletionMarkers)
                    .then(|| DeletionRegisterSource {
                        graph: deletions.clone().project_fields(
                            register_storage_field_names()
                                .into_iter()
                                .map(ProjectField::named)
                                .chain(std::iter::once(ProjectField::renamed(
                                    "branch_key",
                                    "supplying_branch_key",
                                ))),
                        ),
                        row_uuid_field: "row_uuid".to_owned(),
                    });
                let deleted = deletions
                    .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                    .project(["row_uuid"]);
                let selected_base =
                    GraphBuilder::anti_join(content, deleted, ["row_uuid"], ["row_uuid"])
                        .project_fields(branch_view_storage_source_fields(&table, head).map_err(
                            |_| source_resolution_error(request, SourceGap::SchemaProjection),
                        )?);
                let (graph, descriptor, metadata, routing_fields) = resolved_current_source_graph(
                    self.node,
                    &table,
                    graph_tier.expect("branch view has a current tier"),
                    &request.requirements,
                    &authorization,
                    self.read_view.policy_schema,
                    Some(&self.read_view),
                    branch_witness_field,
                    Some(selected_base),
                )
                .map_err(|error| source_resolution_error_from_policy_proof(request, error))?;
                return Ok(ResolvedSource {
                    stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?,
                    table_schema: table.clone(),
                    graph,
                    row_shape: SourceRowShape {
                        source: request.source.clone(),
                        descriptor,
                        row_uuid_field: "row_uuid".to_owned(),
                        metadata,
                    },
                    routing_fields,
                    requires_result_payload: true,
                    content_version,
                    deletion_register,
                    authorized_deletion_preimage: None,
                });
            }
            if matches!(base, Some(BranchViewSourceBase::Snapshot(_, _))) {
                let branch_witness_field =
                    (!table.branch_by.is_empty()).then_some("supplying_branch_key");
                let tier = graph_tier.expect("branch view has a current tier");
                let (frozen_base_key, frozen_snapshot) = match base {
                    Some(BranchViewSourceBase::Snapshot(key, snapshot)) => (key, snapshot),
                    _ => unreachable!("guarded frozen branch base"),
                };
                let head_keys = self
                    .node
                    .equivalent_stored_branch_keys(
                        &request.source.table,
                        self.read_view.read_schema,
                        head,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let head_content = self
                    .projected_branch_content_source_graph(request, &table, tier, &head_keys)
                    .await?;
                let head_deletions = self
                    .projected_branch_deletion_source_graph(request, tier, &head_keys)
                    .await?;
                let head_content_presence = head_content.clone().project(["row_uuid"]);
                let head_deletion_presence = head_deletions.clone().project(["row_uuid"]);
                let deleted = head_deletions
                    .clone()
                    .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                    .project(["row_uuid"]);
                let restored = head_deletions
                    .filter(PredicateExpr::eq("_deletion", Value::EnumTag(1)))
                    .project(["row_uuid"]);
                let selected_head = GraphBuilder::anti_join(
                    head_content.clone(),
                    deleted.clone(),
                    ["row_uuid"],
                    ["row_uuid"],
                )
                .project_fields(
                    branch_view_storage_source_fields(&table, head).map_err(|_| {
                        source_resolution_error(request, SourceGap::SchemaProjection)
                    })?,
                );
                let system_authorization = SourceAuthorizationRequest::System;
                let (live_head, _, metadata, routing_fields) = resolved_current_source_graph(
                    self.node,
                    &table,
                    tier,
                    &request.requirements,
                    &system_authorization,
                    self.read_view.policy_schema,
                    Some(&self.read_view),
                    branch_witness_field,
                    Some(selected_head),
                )
                .map_err(|error| source_resolution_error_from_policy_proof(request, error))?;
                let descriptor = current_row_descriptor_with_hidden_source_fields_for_branch(
                    &table,
                    &metadata,
                    branch_witness_field.is_some(),
                );
                // Capture only the base snapshot once. The live head is kept
                // entirely in maintained table inputs so pending rejection,
                // replacement, deletion and restoration cannot leak into the
                // frozen relation.
                let (frozen_base_rows, frozen_base_deleted_rows) = self
                    .node
                    .branch_snapshot_rows_for_schema(
                        &request.source.table,
                        self.read_view.read_schema,
                        head,
                        frozen_base_key,
                        frozen_snapshot,
                    )
                    .await
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let schema_version_alias = self
                    .node
                    .ensure_schema_version_alias(self.read_view.read_schema)
                    .await
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                let (opening, opening_descriptor, opening_metadata) =
                    inline_current_graph_with_source_metadata_and_branch_witness(
                        &table,
                        frozen_base_rows,
                        schema_version_alias,
                        "frozen-branch-base",
                        &request.requirements,
                        branch_witness_field.map(|field| (field, frozen_base_key)),
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                if descriptor != opening_descriptor || metadata != opening_metadata {
                    return Err(source_resolution_error(
                        request,
                        SourceGap::SchemaProjection,
                    ));
                }
                let inherited = GraphBuilder::anti_join(
                    opening,
                    head_content_presence,
                    ["row_uuid"],
                    ["row_uuid"],
                );
                // A deletion-register winner is explicit for both states. Use
                // its positive Restored row as a positive maintained input;
                // relying only on retraction from a filtered Deleted relation
                // would not publish a deletion-only restore transition.
                let inherited = if frozen_base_deleted_rows.is_empty() {
                    // Do not add an empty static anti-join to the maintained
                    // path: its static witness cannot publish the inherited
                    // row when a pending head content winner retracts.
                    GraphBuilder::union([
                        GraphBuilder::anti_join(
                            inherited.clone(),
                            head_deletion_presence,
                            ["row_uuid"],
                            ["row_uuid"],
                        ),
                        GraphBuilder::semi_join(inherited, restored, ["row_uuid"], ["row_uuid"]),
                    ])
                } else {
                    let (frozen_base_deletions, deletion_descriptor, deletion_metadata) =
                        inline_current_graph_with_source_metadata_and_branch_witness(
                            &table,
                            frozen_base_deleted_rows,
                            schema_version_alias,
                            "frozen-branch-base-deletions",
                            &request.requirements,
                            branch_witness_field.map(|field| (field, frozen_base_key)),
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                    if descriptor != deletion_descriptor || metadata != deletion_metadata {
                        return Err(source_resolution_error(
                            request,
                            SourceGap::SchemaProjection,
                        ));
                    }
                    let inherited_without_frozen_deletions = GraphBuilder::anti_join(
                        inherited.clone(),
                        frozen_base_deletions.project(["row_uuid"]),
                        ["row_uuid"],
                        ["row_uuid"],
                    );
                    GraphBuilder::union([
                        GraphBuilder::anti_join(
                            inherited_without_frozen_deletions,
                            head_deletion_presence,
                            ["row_uuid"],
                            ["row_uuid"],
                        ),
                        // A head Restored winner deliberately also reveals a
                        // base row whose frozen deletion winner was Deleted.
                        GraphBuilder::semi_join(inherited, restored, ["row_uuid"], ["row_uuid"]),
                    ])
                };
                let unfiltered = GraphBuilder::union([live_head, inherited]);
                let graph = match &authorization {
                    SourceAuthorizationRequest::System => unfiltered,
                    SourceAuthorizationRequest::PolicyFiltered {
                        permission_subject,
                        plan,
                    }
                    | SourceAuthorizationRequest::PolicyProof {
                        permission_subject,
                        plan,
                    } => {
                        let param_binding_mode = if plan.binding_source_shape.is_some() {
                            ParamBindingMode::RetainAllParams
                        } else {
                            ParamBindingMode::InlineAllReachableSeeds
                        };
                        let policy_request = self.node.table_read_policy_authorization_request(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            param_binding_mode,
                            tier,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                        let policy_request = policy_request.map(|mut request| {
                            request.reads.primary = policy_read_view_projected_through(
                                &request.reads.primary,
                                self.read_view,
                            );
                            request
                        });
                        let mut output_fields = current_row_fields(&table);
                        output_fields.extend(branch_witness_field.map(str::to_owned));
                        self.node
                            .compose_policy_filtered_current_source_graph(
                                policy_request,
                                unfiltered,
                                &output_fields,
                            )
                            .map_err(|error| {
                                source_resolution_error_from_policy_proof(request, error)
                            })?
                            .graph
                    }
                };
                return Ok(ResolvedSource {
                    stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?,
                    table_schema: table.clone(),
                    graph,
                    row_shape: SourceRowShape {
                        source: request.source.clone(),
                        descriptor,
                        row_uuid_field: "row_uuid".to_owned(),
                        metadata,
                    },
                    routing_fields,
                    requires_result_payload: true,
                    content_version: request
                        .requirements
                        .metadata
                        .iter()
                        .any(|requirement| {
                            matches!(
                                requirement,
                                SourceMetadataRequirement::VersionPayloads
                                    | SourceMetadataRequirement::VersionWitnesses
                                    | SourceMetadataRequirement::Provenance(_)
                            )
                        })
                        .then(|| ContentVersionSource {
                            graph: head_content.project_fields(
                                maintained_view_history_storage_field_names(&table)
                                    .into_iter()
                                    .map(ProjectField::named)
                                    .chain(std::iter::once(ProjectField::renamed(
                                        "branch_key",
                                        "supplying_branch_key",
                                    ))),
                            ),
                            row_uuid_field: "row_uuid".to_owned(),
                        }),
                    deletion_register: None,
                    authorized_deletion_preimage: None,
                });
            }
            let rows = self
                .node
                .branch_view_rows_for_schema(
                    &request.source.table,
                    self.read_view.read_schema,
                    graph_tier.expect("branch view has a current tier"),
                    head,
                    base,
                )
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
            let (base_graph, descriptor, metadata) = if request.requirements.metadata.is_empty() {
                (
                    inline_current_graph(&table, rows)
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?,
                    current_row_descriptor(&table),
                    BTreeMap::new(),
                )
            } else {
                let schema_version_alias = self
                    .node
                    .ensure_schema_version_alias(self.read_view.read_schema)
                    .await
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                inline_current_graph_with_source_metadata(
                    &table,
                    rows,
                    schema_version_alias,
                    "branch-view",
                    &request.requirements,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            };
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base_graph,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    let param_binding_mode = if plan.binding_source_shape.is_some() {
                        ParamBindingMode::RetainAllParams
                    } else {
                        ParamBindingMode::InlineAllReachableSeeds
                    };
                    let policy_request = self.node.table_read_policy_authorization_request(
                        self.read_view.policy_schema,
                        &table.name,
                        *permission_subject,
                        param_binding_mode,
                        graph_tier.expect("branch view has a current tier"),
                        plan.binding_source_shape.clone(),
                        plan.binding_user_params.clone(),
                        plan.binding_claim_params.clone(),
                    );
                    let policy_request = policy_request.map(|mut request| {
                        request.reads.primary = policy_read_view_projected_through(
                            &request.reads.primary,
                            self.read_view,
                        );
                        request
                    });
                    self.node
                        .compose_policy_filtered_current_source_graph(
                            policy_request,
                            base_graph,
                            &current_row_fields(&table),
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (graph, descriptor, metadata, BTreeSet::new())
        } else if let Some(position) = history_position {
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(
                    request,
                    SourceGap::HistoricalStorageCut,
                ));
            }
            let needs_settle_position = request
                .requirements
                .metadata
                .contains(&SourceMetadataRequirement::SettlePosition);
            let mut metadata = BTreeMap::new();
            if needs_settle_position {
                metadata.insert(
                    SourceMetadataRequirement::SettlePosition,
                    SourceMetadataFields::SettlePosition {
                        settle_position_field: "settle_position".to_owned(),
                    },
                );
            }
            let descriptor = current_row_descriptor_with_hidden_source_fields(&table, &metadata);
            let base = self
                .projected_historical_source_graph(request, &table, position)
                .await?;
            let base = if needs_settle_position {
                base.project_fields(
                    current_row_fields(&table)
                        .into_iter()
                        .map(ProjectField::named)
                        .chain([ProjectField::null_typed(
                            "settle_position",
                            ValueType::Nullable(Box::new(ValueType::U64)),
                        )])
                        .collect::<Vec<_>>(),
                )
            } else {
                base
            };
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(
                            request,
                            SourceGap::HistoricalStorageCut,
                        ));
                    }
                    let policy_request = self.node.table_read_policy_authorization_request_at(
                        self.read_view.policy_schema,
                        &table.name,
                        *permission_subject,
                        ParamBindingMode::InlineAllReachableSeeds,
                        position,
                        plan.binding_source_shape.clone(),
                        plan.binding_user_params.clone(),
                        plan.binding_claim_params.clone(),
                    );
                    self.node
                        .compose_policy_filtered_current_source_graph(
                            policy_request,
                            base,
                            &descriptor_field_names(&descriptor).map_err(|_| {
                                source_resolution_error(request, SourceGap::HistoricalStorageCut)
                            })?,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (graph, descriptor, metadata, BTreeSet::new())
        } else if let Some(snapshot) = snapshot {
            if request.visibility != RowVisibility::Visible {
                return Err(source_resolution_error(
                    request,
                    SourceGap::HistoricalStorageCut,
                ));
            }
            let rows = self
                .node
                .projected_snapshot_current_rows(
                    &request.source.table,
                    self.read_view.read_schema,
                    &snapshot,
                )
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
            let schema_version_alias = self
                .node
                .ensure_schema_version_alias(self.read_view.read_schema)
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
            let (base, descriptor, metadata) = inline_current_graph_with_source_metadata(
                &table,
                rows,
                schema_version_alias,
                "snapshot",
                &request.requirements,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
            snapshot_content_version = request
                .requirements
                .metadata
                .contains(&SourceMetadataRequirement::VersionPayloads)
                .then(|| ContentVersionSource {
                    graph: base
                        .clone()
                        .project(maintained_view_history_storage_field_names(&table)),
                    row_uuid_field: "row_uuid".to_owned(),
                });
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(
                            request,
                            SourceGap::HistoricalStorageCut,
                        ));
                    }
                    let param_binding_mode = if plan.binding_source_shape.is_some() {
                        ParamBindingMode::RetainAllParams
                    } else {
                        ParamBindingMode::InlineAllReachableSeeds
                    };
                    let policy_request = self.node.table_read_policy_authorization_request(
                        self.read_view.policy_schema,
                        &table.name,
                        *permission_subject,
                        param_binding_mode,
                        DurabilityTier::Global,
                        plan.binding_source_shape.clone(),
                        plan.binding_user_params.clone(),
                        plan.binding_claim_params.clone(),
                    );
                    let policy_request = policy_request.map(|mut request| {
                        request.reads.primary = policy_read_view_projected_through(
                            &request.reads.primary,
                            self.read_view,
                        );
                        request
                    });
                    let output_fields = descriptor_field_names(&descriptor).map_err(|_| {
                        source_resolution_error(request, SourceGap::HistoricalStorageCut)
                    })?;
                    self.node
                        .compose_policy_filtered_current_source_graph(
                            policy_request,
                            base,
                            &output_fields,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (graph, descriptor, metadata, BTreeSet::new())
        } else if let Some(tx_id) = open_tx_overlay {
            let include_deleted = request.visibility == RowVisibility::IncludeDeleted;
            let rows = self
                .node
                .tx_current_rows_in_schema_with_options(
                    tx_id,
                    self.read_view.read_schema,
                    &request.source.table,
                    include_deleted,
                )
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::TransactionReadOverlay))?;
            let (base, descriptor, metadata) = if include_deleted {
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        let deleted = row.is_deleted();
                        (row, deleted)
                    })
                    .collect();
                let schema_version_alias = self
                    .node
                    .ensure_schema_version_alias(self.read_view.read_schema)
                    .await
                    .map_err(|_| {
                        source_resolution_error(request, SourceGap::TransactionReadOverlay)
                    })?;
                inline_snapshot_include_deleted_current_graph_with_source_metadata(
                    &table,
                    rows,
                    schema_version_alias,
                    "open-transaction",
                    &request.requirements,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::TransactionReadOverlay))?
            } else {
                let schema_version_alias = self
                    .node
                    .ensure_schema_version_alias(self.read_view.read_schema)
                    .await
                    .map_err(|_| {
                        source_resolution_error(request, SourceGap::TransactionReadOverlay)
                    })?;
                inline_current_graph_with_source_metadata(
                    &table,
                    rows,
                    schema_version_alias,
                    "open-transaction",
                    &request.requirements,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::TransactionReadOverlay))?
            };
            // An open transaction is a snapshot plus its staged overlay, not
            // an authorization result. Filter the effective rows through the
            // same identity-bound read policy as an ordinary trusted-serving
            // source. The matching policy dependency is compiled against this
            // overlay below, so staged rows are subject to the opening
            // transaction identity as well.
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    let policy_request = if include_deleted {
                        self.node
                            .table_read_policy_authorization_request_for_include_deleted(
                                self.read_view.policy_schema,
                                &table.name,
                                *permission_subject,
                                DurabilityTier::Global,
                                plan.binding_source_shape.clone(),
                                plan.binding_user_params.clone(),
                                plan.binding_claim_params.clone(),
                            )
                    } else {
                        let param_binding_mode = if plan.binding_source_shape.is_some() {
                            ParamBindingMode::RetainAllParams
                        } else {
                            ParamBindingMode::InlineAllReachableSeeds
                        };
                        self.node.table_read_policy_authorization_request(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            param_binding_mode,
                            DurabilityTier::Global,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        )
                    };
                    let policy_request = policy_request.map(|mut request| {
                        request.reads.primary = policy_read_view_projected_through(
                            &request.reads.primary,
                            self.read_view,
                        );
                        request
                    });
                    let mut output_fields = descriptor_field_names(&descriptor).map_err(|_| {
                        source_resolution_error(request, SourceGap::TransactionReadOverlay)
                    })?;
                    if include_deleted {
                        output_fields.push("__jazz_deleted".to_owned());
                    }
                    self.node
                        .compose_policy_filtered_current_source_graph(
                            policy_request,
                            base,
                            &output_fields,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (graph, descriptor, metadata, BTreeSet::new())
        } else if request.visibility == RowVisibility::Visible
            && (self.needs_projected_current_source(&request.source.table)
                || self.excludes_below_pending(request))
            && !receiver_local_overlay
        {
            if !request.requirements.metadata.is_empty() {
                let source = self
                    .projected_maintained_visible_current_source_graph(
                        request,
                        &table,
                        graph_tier.expect("visible current source has a tier"),
                    )
                    .await?;
                resolved_current_source_graph(
                    self.node,
                    &table,
                    graph_tier.expect("visible current source has a tier"),
                    &request.requirements,
                    &authorization,
                    self.read_view.policy_schema,
                    Some(self.read_view),
                    None,
                    Some(source.graph),
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            } else {
                let tier = graph_tier.expect("visible current source has a tier");
                let selected_policy_base = if self.access_paths.contains_key(&request.source)
                    || self.excludes_below_pending(request)
                {
                    None
                } else if let Some(access_path) =
                    self.cached_policy_authorization_access_path(request)?
                {
                    self.selected_global_current_source_graph_for_access_path(
                        request,
                        &table,
                        tier,
                        access_path,
                    )
                    .await?
                } else {
                    None
                };
                let source = if let Some(graph) = selected_policy_base {
                    CurrentSourceGraph {
                        graph: graph.project_fields(storage_to_canonical_current_source_fields(
                            &table, true, false,
                        )),
                        descriptor: current_row_descriptor(&table),
                        metadata: BTreeMap::new(),
                    }
                } else {
                    self.projected_visible_current_source_graph(request, &table, tier)
                        .await?
                };
                let graph = match &authorization {
                    SourceAuthorizationRequest::System => source.graph,
                    SourceAuthorizationRequest::PolicyFiltered {
                        permission_subject,
                        plan,
                    }
                    | SourceAuthorizationRequest::PolicyProof {
                        permission_subject,
                        plan,
                    } => {
                        if plan.protected_source.table != table.name
                            || plan.role != PolicyDecisionRole::Read
                            || plan.protected_row_field != "row_uuid"
                        {
                            return Err(source_resolution_error(request, SourceGap::Coverage));
                        }
                        let param_binding_mode = if plan.binding_source_shape.is_some() {
                            ParamBindingMode::RetainAllParams
                        } else {
                            ParamBindingMode::InlineAllReachableSeeds
                        };
                        let policy_request = self.node.table_read_policy_authorization_request(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            param_binding_mode,
                            graph_tier.expect("visible current source has a tier"),
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                        let policy_request = policy_request.map(|mut request| {
                            request.reads.primary = policy_read_view_projected_through(
                                &request.reads.primary,
                                self.read_view,
                            );
                            request
                        });
                        self.node
                            .compose_policy_filtered_current_source_graph(
                                policy_request,
                                source.graph,
                                &current_row_fields(&table),
                            )
                            .map_err(|error| {
                                source_resolution_error_from_policy_proof(request, error)
                            })?
                            .graph
                    }
                };
                (graph, source.descriptor, source.metadata, BTreeSet::new())
            }
        } else if request.visibility == RowVisibility::IncludeDeleted && pending_overlay {
            // A receiver's own pending deletion needs no read-policy proof.
            // Do not reopen persisted Global data for this internal sibling.
            let graph = self
                .pending_deletion_winner_graph(request)?
                .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                .project_fields([
                    ProjectField::named("row_uuid"),
                    ProjectField::named("tx_time"),
                    ProjectField::named("tx_node_id"),
                    ProjectField::literal("__jazz_deleted", Value::Bool(true)),
                ]);
            (
                graph,
                include_deleted_current_row_descriptor(&table),
                BTreeMap::new(),
                BTreeSet::new(),
            )
        } else if request.visibility == RowVisibility::IncludeDeleted
            && (self.needs_projected_current_source(&request.source.table)
                || self.excludes_below_pending(request))
        {
            let tier = graph_tier.expect("visible current source has a tier");
            let base = self
                .projected_include_deleted_current_source_graph(request, &table, tier)
                .await?;
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base.clone(),
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    let policy_request = self
                        .node
                        .table_read_policy_authorization_request_for_include_deleted(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            tier,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                    // Use the same projected read view as dependency preparation:
                    // an old-schema reader still evaluates the current policy.
                    let policy_request = policy_request.map(|mut request| {
                        request.reads.primary = policy_read_view_projected_through(
                            &request.reads.primary,
                            self.read_view,
                        );
                        request
                    });
                    let mut output_fields = current_row_fields(&table);
                    output_fields.push("__jazz_deleted".to_owned());
                    self.node
                        .compose_policy_filtered_current_source_graph(
                            policy_request,
                            base.clone(),
                            &output_fields,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (
                graph,
                include_deleted_current_row_descriptor(&table),
                BTreeMap::new(),
                BTreeSet::new(),
            )
        } else if request.visibility == RowVisibility::IncludeDeleted {
            let tier = graph_tier.expect("visible current source has a tier");
            let base = include_deleted_current_graph(&table, tier);
            let graph = match &authorization {
                SourceAuthorizationRequest::System => base,
                SourceAuthorizationRequest::PolicyFiltered {
                    permission_subject,
                    plan,
                }
                | SourceAuthorizationRequest::PolicyProof {
                    permission_subject,
                    plan,
                } => {
                    if plan.protected_source.table != table.name
                        || plan.role != PolicyDecisionRole::Read
                        || plan.protected_row_field != "row_uuid"
                    {
                        return Err(source_resolution_error(request, SourceGap::Coverage));
                    }
                    let policy_request = self
                        .node
                        .table_read_policy_authorization_request_for_include_deleted(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            tier,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                    let policy_request = policy_request.map(|mut request| {
                        request.reads.primary = policy_read_view_projected_through(
                            &request.reads.primary,
                            self.read_view,
                        );
                        request
                    });
                    let mut output_fields = current_row_fields(&table);
                    output_fields.push("__jazz_deleted".to_owned());
                    self.node
                        .compose_policy_filtered_current_source_graph(
                            policy_request,
                            base,
                            &output_fields,
                        )
                        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
                        .graph
                }
            };
            (
                graph,
                include_deleted_current_row_descriptor(&table),
                BTreeMap::new(),
                BTreeSet::new(),
            )
        } else {
            let tier = graph_tier.expect("visible current source has a tier");
            let selected_base = if receiver_local_overlay {
                // This is deliberately an Ahead-only source.  The companion
                // CoveredInput source is the complete, policy-scoped remote
                // frontier.  A normal Local read is Global ∪ Ahead, but using
                // it here would accidentally turn durable authority storage
                // into a second receiver input path.
                self.node.query_engine_read_metrics.source_full_scans += 1;
                Some(
                    self.pending_local_current_source_graph(request, &table)
                        .await?,
                )
            } else {
                let selected_base = self
                    .selected_global_current_source_graph(request, &table, tier)
                    .await?;
                match selected_base {
                    Some(selected_base) => Some(selected_base),
                    None => match self.cached_policy_authorization_access_path(request)? {
                        Some(access_path) => {
                            self.selected_global_current_source_graph_for_access_path(
                                request,
                                &table,
                                tier,
                                access_path,
                            )
                            .await?
                        }
                        None => None,
                    },
                }
            };
            if selected_base.is_none() {
                self.node.query_engine_read_metrics.source_full_scans += 1;
            }
            resolved_current_source_graph(
                self.node,
                &table,
                tier,
                &request.requirements,
                &authorization,
                self.read_view.policy_schema,
                Some(self.read_view),
                None,
                selected_base,
            )
            .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
        };
        // Own pending deletions carry only winner coordinates and need no
        // read-policy proof. Never manufacture author data for that overlay.
        let (graph, descriptor, metadata) =
            if request.visibility == RowVisibility::IncludeDeleted && !pending_overlay {
                with_requested_author_paths(graph, descriptor, metadata, &request.requirements)
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            } else {
                (graph, descriptor, metadata)
            };
        let deletion_register = if snapshot_source {
            // The snapshot is immutable and already excludes rows whose
            // deletion winner is visible at its cut. It cannot later need a
            // replacement/deletion transition sidecar.
            None
        } else {
            self.deletion_register_source_for_request(
                request,
                &table,
                graph_tier,
                history_position,
                open_tx_overlay,
            )?
        };
        let content_version = if snapshot_source {
            snapshot_content_version
        } else {
            self.content_version_source_for_request(
                request,
                &table,
                graph_tier,
                history_position,
                open_tx_overlay,
            )
            .await?
        };
        // The descriptor attached to the receiver input is the compiler's
        // canonical current-storage descriptor. The physical current arm
        // reaches that same boundary through its variant projection; retain
        // the allocated descriptor for the row shape rather than reconstruct
        // the authored enum occurrence above.
        let descriptor = covered_input_descriptor.clone().unwrap_or(descriptor);
        let graph = if let Some(input_source) = covered_input_source {
            // Online remote-if-possible composes the authority closure with the
            // eligible local-current overlay before it enters the same
            // maintained program. The union is per normalized source
            // occurrence; table-level union would conflate aliases/self-joins.
            // Both sides can carry the same already-admitted version (the
            // local store retains received authority data), so select their
            // single current winner by the normal version identity before
            // the graph can observe it. A locally pending successor wins;
            // rejection retracts that ahead record and deterministically
            // reveals the covered authority version again.
            if std::env::var_os("JAZZ_COVERED_INPUT_TRACE").is_some() {
                eprintln!(
                    "JAZZ_COVERED_INPUT_TRACE stage=union_covered_input request={:?} descriptor={descriptor:?}",
                    request.source,
                );
            }
            let covered_descriptor = covered_input_descriptor
                .expect("checked alongside compiler-owned covered input source");
            let covered = GraphBuilder::input_source(input_source, covered_descriptor.clone());
            let graph = if receiver_local_overlay {
                // Existing cached rows outside this exact source occurrence
                // cannot enter merely because they have a pending edit.
                // Pending inserts have no accepted content predecessor.
                let projection_target = self.current_projection_target(request, &table)?;
                let accepted = self
                    .node
                    .physical_current_source_graph_with_projection_target(
                        self.read_view.read_schema,
                        &request.source.table,
                        PhysicalCurrentClass::Global,
                        projection_target,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?
                    .project(["row_uuid"]);
                let out_of_scope = GraphBuilder::anti_join(
                    accepted,
                    covered.clone().project(["row_uuid"]),
                    ["row_uuid"],
                    ["row_uuid"],
                );
                GraphBuilder::anti_join(graph, out_of_scope, ["row_uuid"], ["row_uuid"])
            } else {
                graph
            };
            let covered = self.exclude_settled_arm(request, covered, false).await?;
            let inputs = vec![covered, graph];

            let winner = GraphBuilder::arg_max_by(
                GraphBuilder::union(inputs),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            );
            if receiver_local_overlay {
                let deleted = self
                    .pending_deletion_winner_graph(request)?
                    .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                    .project(["row_uuid"]);
                GraphBuilder::anti_join(winner, deleted, ["row_uuid"], ["row_uuid"])
            } else {
                winner
            }
        } else {
            graph
        };
        Ok(ResolvedSource {
            stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?,
            table_schema: table,
            graph,
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor,
                row_uuid_field: "row_uuid".to_owned(),
                metadata,
            },
            routing_fields,
            requires_result_payload: false,
            content_version,
            deletion_register,
            authorized_deletion_preimage: None,
        })
    }

    /// Prepare the ordinary current-row source without constructing the
    /// historical/branch source-state machine.  This is the hot path for
    /// regular reads and policy admission, where keeping the async frame
    /// bounded matters because it can run beneath a peer tick.
    async fn prepare_unprojected_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        tier: DurabilityTier,
    ) -> Result<ResolvedSource, SourceResolutionError> {
        let projection = match self.read_view.sources.get(&request.source) {
            Some(SourceExpr::VisibleCurrent {
                projection,
                data: DataSource::Current,
                ..
            }) => projection.clone(),
            _ => return Err(source_resolution_error(request, SourceGap::Coverage)),
        };
        if !matches!(projection.schema_family, SchemaFamilySelection::Current)
            || !matches!(
                projection.storage,
                StorageSchemaSelection::Single(_) | StorageSchemaSelection::CompatiblePartitions
            )
            || !matches!(projection.lens, LensSelection::Canonical)
        {
            return Err(source_resolution_error(
                request,
                SourceGap::SchemaProjection,
            ));
        }
        let table = self
            .node
            .table_in_schema(&request.source.table, self.read_view.read_schema)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        // Policy-proof dependencies are raw evidence for the outer policy.
        // Re-applying their own read policy recursively both changes the
        // outer predicate's meaning and can manufacture a proof cycle.
        let authorization = if matches!(
            request.authorization,
            SourceAuthorizationRequest::PolicyProof { .. }
        ) {
            SourceAuthorizationRequest::System
        } else {
            request.authorization.clone()
        };
        let selected_base = self
            .selected_global_current_source_graph(request, &table, tier)
            .await?;
        let selected_base = match selected_base {
            Some(selected_base) => Some(selected_base),
            None => match self.cached_policy_authorization_access_path(request)? {
                Some(access_path) => {
                    self.selected_global_current_source_graph_for_access_path(
                        request,
                        &table,
                        tier,
                        access_path,
                    )
                    .await?
                }
                None => None,
            },
        };
        if selected_base.is_none() {
            self.node.query_engine_read_metrics.source_full_scans += 1;
        }
        let (graph, descriptor, metadata, routing_fields) = resolved_current_source_graph(
            self.node,
            &table,
            tier,
            &request.requirements,
            &authorization,
            self.read_view.policy_schema,
            Some(self.read_view),
            None,
            selected_base,
        )
        .map_err(|error| source_resolution_error_from_policy_proof(request, error))?;
        let deletion_register =
            self.deletion_register_source_for_request(request, &table, Some(tier), None, None)?;
        let content_version = self
            .content_version_source_for_request(request, &table, Some(tier), None, None)
            .await?;
        Ok(ResolvedSource {
            stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?,
            table_schema: table,
            graph,
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor,
                row_uuid_field: "row_uuid".to_owned(),
                metadata,
            },
            routing_fields,
            requires_result_payload: false,
            content_version,
            deletion_register,
            authorized_deletion_preimage: None,
        })
    }

    /// Prepare a current-row source whose storage layout needs the projected
    /// source path.  Kept separate from historical and branch preparation so
    /// peer admission does not inherit their async frame.
    async fn prepare_projected_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        tier: DurabilityTier,
    ) -> Result<ResolvedSource, SourceResolutionError> {
        let projection = match self.read_view.sources.get(&request.source) {
            Some(SourceExpr::VisibleCurrent {
                projection,
                data: DataSource::Current,
                ..
            }) => projection.clone(),
            _ => return Err(source_resolution_error(request, SourceGap::Coverage)),
        };
        if !matches!(projection.schema_family, SchemaFamilySelection::Current)
            || !matches!(
                projection.storage,
                StorageSchemaSelection::Single(_) | StorageSchemaSelection::CompatiblePartitions
            )
            || !matches!(projection.lens, LensSelection::Canonical)
        {
            return Err(source_resolution_error(
                request,
                SourceGap::SchemaProjection,
            ));
        }
        let table = self
            .node
            .table_in_schema(&request.source.table, self.read_view.read_schema)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let authorization = if matches!(
            request.authorization,
            SourceAuthorizationRequest::PolicyProof { .. }
        ) {
            SourceAuthorizationRequest::System
        } else {
            request.authorization.clone()
        };
        let (graph, descriptor, metadata, routing_fields) =
            if !request.requirements.metadata.is_empty() {
                let source = self
                    .projected_maintained_visible_current_source_graph(request, &table, tier)
                    .await?;
                resolved_current_source_graph(
                    self.node,
                    &table,
                    tier,
                    &request.requirements,
                    &authorization,
                    self.read_view.policy_schema,
                    Some(self.read_view),
                    None,
                    Some(source.graph),
                )
                .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
            } else {
                let selected_policy_base = if self.access_paths.contains_key(&request.source)
                    || self.excludes_below_pending(request)
                {
                    None
                } else if let Some(access_path) =
                    self.cached_policy_authorization_access_path(request)?
                {
                    self.selected_global_current_source_graph_for_access_path(
                        request,
                        &table,
                        tier,
                        access_path,
                    )
                    .await?
                } else {
                    None
                };
                let source = if let Some(graph) = selected_policy_base {
                    CurrentSourceGraph {
                        graph: graph.project_fields(storage_to_canonical_current_source_fields(
                            &table, true, false,
                        )),
                        descriptor: current_row_descriptor(&table),
                        metadata: BTreeMap::new(),
                    }
                } else {
                    self.projected_visible_current_source_graph(request, &table, tier)
                        .await?
                };
                let graph = match &authorization {
                    SourceAuthorizationRequest::System => source.graph,
                    SourceAuthorizationRequest::PolicyFiltered {
                        permission_subject,
                        plan,
                    }
                    | SourceAuthorizationRequest::PolicyProof {
                        permission_subject,
                        plan,
                    } => {
                        if plan.protected_source.table != table.name
                            || plan.role != PolicyDecisionRole::Read
                            || plan.protected_row_field != "row_uuid"
                        {
                            return Err(source_resolution_error(request, SourceGap::Coverage));
                        }
                        let param_binding_mode = if plan.binding_source_shape.is_some() {
                            ParamBindingMode::RetainAllParams
                        } else {
                            ParamBindingMode::InlineAllReachableSeeds
                        };
                        let policy_request = self.node.table_read_policy_authorization_request(
                            self.read_view.policy_schema,
                            &table.name,
                            *permission_subject,
                            param_binding_mode,
                            tier,
                            plan.binding_source_shape.clone(),
                            plan.binding_user_params.clone(),
                            plan.binding_claim_params.clone(),
                        );
                        let policy_request = policy_request.map(|mut request| {
                            request.reads.primary = policy_read_view_projected_through(
                                &request.reads.primary,
                                self.read_view,
                            );
                            request
                        });
                        self.node
                            .compose_policy_filtered_current_source_graph(
                                policy_request,
                                source.graph,
                                &current_row_fields(&table),
                            )
                            .map_err(|error| {
                                source_resolution_error_from_policy_proof(request, error)
                            })?
                            .graph
                    }
                };
                (graph, source.descriptor, source.metadata, BTreeSet::new())
            };
        let deletion_register =
            self.deletion_register_source_for_request(request, &table, Some(tier), None, None)?;
        let content_version = self
            .content_version_source_for_request(request, &table, Some(tier), None, None)
            .await?;
        Ok(ResolvedSource {
            stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?,
            table_schema: table,
            graph,
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor,
                row_uuid_field: "row_uuid".to_owned(),
                metadata,
            },
            routing_fields,
            requires_result_payload: false,
            content_version,
            deletion_register,
            authorized_deletion_preimage: None,
        })
    }
}

impl<S> JazzSourceGraphPreparer<'_, S>
where
    S: OrderedKvStorage,
{
    async fn prepare_inline_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
    ) -> Result<ResolvedSource, SourceResolutionError> {
        let Some(SourceExpr::VisibleCurrent {
            projection,
            data: DataSource::Current,
            ..
        }) = self.read_view.sources.get(&request.source)
        else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        let Some(rows) = self.inline_sources.get(&request.source) else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        if !matches!(projection.schema_family, SchemaFamilySelection::Current)
            || !matches!(
                projection.storage,
                StorageSchemaSelection::Single(_) | StorageSchemaSelection::CompatiblePartitions
            )
            || !matches!(projection.lens, LensSelection::Canonical)
        {
            return Err(source_resolution_error(
                request,
                SourceGap::SchemaProjection,
            ));
        }
        let authorization = if matches!(
            request.authorization,
            SourceAuthorizationRequest::PolicyProof { .. }
        ) {
            SourceAuthorizationRequest::System
        } else {
            request.authorization.clone()
        };
        if request.visibility != RowVisibility::Visible
            || !matches!(authorization, SourceAuthorizationRequest::System)
        {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        }
        let table = self
            .node
            .table_in_schema(&request.source.table, self.read_view.read_schema)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let schema_version_alias = self
            .node
            .ensure_schema_version_alias(self.read_view.read_schema)
            .await
            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
        let (graph, descriptor, metadata) = inline_current_graph_with_source_metadata(
            &table,
            rows.clone(),
            schema_version_alias,
            "inline-current",
            &request.requirements,
        )
        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
        Ok(ResolvedSource {
            stored_column_ids: self.stored_column_ids_for_read_table(request, &table)?,
            table_schema: table,
            graph,
            row_shape: SourceRowShape {
                source: request.source.clone(),
                descriptor,
                row_uuid_field: "row_uuid".to_owned(),
                metadata,
            },
            routing_fields: BTreeSet::new(),
            requires_result_payload: false,
            content_version: None,
            deletion_register: None,
            authorized_deletion_preimage: None,
        })
    }
}

impl<S> SourceGraphPreparer for JazzSourceGraphPreparer<'_, S>
where
    S: OrderedKvStorage,
{
    fn prepare_source_graph<'a>(
        &'a mut self,
        request: &'a SourceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedSource, SourceResolutionError>> + 'a>> {
        Box::pin(async move {
            let exclusion_scope = self.local_exclusion_scope(request);
            // A physical limit is valid only when no later source filter can
            // remove candidates. Unavailability is a mutable anti-join, so
            // retain the logical query limit after it instead.
            if exclusion_scope.is_some()
                && let Some(CurrentAccessPath::Index { source_limit, .. }) =
                    self.access_paths.get_mut(&request.source)
            {
                *source_limit = None;
            }
            let mut resolved = self
                .prepare_source_graph_without_local_exclusions(request)
                .await?;
            if let Some(scope) = exclusion_scope
                && !self.excludes_below_pending(request)
            {
                resolved.graph = self
                    .node
                    .exclude_local_unavailable_graph(
                        &scope,
                        self.read_view.read_schema,
                        request,
                        resolved.graph,
                        false,
                    )
                    .await
                    .map_err(|_| {
                        source_resolution_error(request, SourceGap::LocalAvailabilityInput)
                    })?;
            }
            Ok(resolved)
        })
    }
}

impl<S: OrderedKvStorage> JazzSourceGraphPreparer<'_, S> {
    fn prepare_source_graph_without_local_exclusions<'a>(
        &'a mut self,
        request: &'a SourceRequest,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedSource, SourceResolutionError>> + 'a>> {
        // A receiver-owned covered source must take the single resolver path
        // that either uses the strict authority frontier directly or unions
        // it with the eligible local frontier.  The visible-current fast
        // paths only know how to lower storage-backed input, which would
        // silently leave the allocated runtime source disconnected from the
        // graph on a Local query.
        if self.covered_input_sources.contains_key(&request.source) {
            return Box::pin(self.prepare_source_graph_dispatch(request));
        }
        let visible_current = match self.read_view.sources.get(&request.source) {
            Some(SourceExpr::VisibleCurrent {
                projection,
                data: DataSource::Current,
                tier,
            }) => Some((projection.clone(), *tier)),
            _ => None,
        };
        if let Some((_projection, tier)) = visible_current {
            if self.inline_sources.contains_key(&request.source) {
                return Box::pin(self.prepare_inline_visible_current_source_graph(request));
            }
            if request.visibility == RowVisibility::Visible {
                if self.needs_projected_current_source(&request.source.table) {
                    return Box::pin(
                        self.prepare_projected_visible_current_source_graph(request, tier),
                    );
                }
                return Box::pin(
                    self.prepare_unprojected_visible_current_source_graph(request, tier),
                );
            }
        }
        Box::pin(self.prepare_source_graph_dispatch(request))
    }
}

impl<S> JazzSourceGraphPreparer<'_, S>
where
    S: OrderedKvStorage,
{
    pub(super) fn policy_dependency_request(
        &mut self,
        request: &SourceRequest,
    ) -> Result<Option<QueryProgramRequest>, Error> {
        let SourceAuthorizationRequest::PolicyFiltered {
            permission_subject,
            plan,
        } = &request.authorization
        else {
            return Ok(None);
        };
        if plan.protected_source.table != request.source.table
            || plan.role != PolicyDecisionRole::Read
            || plan.protected_row_field != "row_uuid"
        {
            return Err(Error::QueryCapability(
                "policy authorization plan does not match source dependency".to_owned(),
            ));
        }
        let source = self
            .read_view
            .sources
            .get(&request.source)
            .ok_or(Error::InvalidStoredValue("query source dependency missing"))?;
        let binding_source_shape = plan.binding_source_shape.clone();
        let binding_user_params = plan.binding_user_params.clone();
        let binding_claim_params = plan.binding_claim_params.clone();
        let dependency = match source {
            SourceExpr::HistoryCut {
                data: DataSource::Current,
                position,
                ..
            } => self.node.table_read_policy_authorization_request_at(
                self.read_view.policy_schema,
                &request.source.table,
                *permission_subject,
                ParamBindingMode::InlineAllReachableSeeds,
                *position,
                binding_source_shape,
                binding_user_params,
                binding_claim_params,
            ),
            SourceExpr::SnapshotRef {
                data: DataSource::Current,
                ..
            } => {
                let param_binding_mode = if binding_source_shape.is_some() {
                    ParamBindingMode::RetainAllParams
                } else {
                    ParamBindingMode::InlineAllReachableSeeds
                };
                self.node.table_read_policy_authorization_request(
                    self.read_view.policy_schema,
                    &request.source.table,
                    *permission_subject,
                    param_binding_mode,
                    DurabilityTier::Global,
                    binding_source_shape,
                    binding_user_params,
                    binding_claim_params,
                )
            }
            SourceExpr::VisibleCurrent { .. }
            | SourceExpr::BranchView { .. }
            | SourceExpr::SettledBindingView { .. }
            | SourceExpr::WithOverlays { .. } => {
                let tier = source.current_tier().unwrap_or(DurabilityTier::Global);
                if request.visibility == RowVisibility::IncludeDeleted {
                    self.node
                        .table_read_policy_authorization_request_for_include_deleted(
                            self.read_view.policy_schema,
                            &request.source.table,
                            *permission_subject,
                            tier,
                            binding_source_shape,
                            binding_user_params,
                            binding_claim_params,
                        )
                } else {
                    let param_binding_mode = if binding_source_shape.is_some() {
                        ParamBindingMode::RetainAllParams
                    } else {
                        ParamBindingMode::InlineAllReachableSeeds
                    };
                    self.node.table_read_policy_authorization_request(
                        self.read_view.policy_schema,
                        &request.source.table,
                        *permission_subject,
                        param_binding_mode,
                        tier,
                        binding_source_shape,
                        binding_user_params,
                        binding_claim_params,
                    )
                }
            }
            _ => return Ok(None),
        };
        match dependency {
            Ok(mut dependency) => {
                dependency.reads.primary =
                    policy_read_view_projected_through(&dependency.reads.primary, self.read_view);
                Ok(Some(dependency))
            }
            Err(Error::QueryCapability(error)) if error.contains("PolicyProofCycle") => {
                Err(Error::QueryCapability(error))
            }
            Err(Error::QueryCapability(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(crate) fn current_projection_target(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
    ) -> Result<String, SourceResolutionError> {
        if let Some(target) = self.current_projection_targets.get(&request.source) {
            return Ok(target.clone());
        }
        let required_fields = self.current_projection_required_fields(request, table);
        let target = self
            .node
            .ensure_physical_current_projection_for_enum_columns(
                self.read_view.read_schema,
                &table.name,
                &required_fields,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        self.current_projection_targets
            .insert(request.source.clone(), target.clone());
        Ok(target)
    }

    pub(crate) fn current_projection_required_fields(
        &self,
        request: &SourceRequest,
        table: &TableSchema,
    ) -> BTreeSet<String> {
        // A maintained version witness identifies the immutable stored version
        // and is normalized back to that canonical history record before it is
        // serialized. It therefore must not widen this *current-query* source
        // to every user column: doing so turns an otherwise unselected enum
        // case into a source-level incompatibility and drops the whole row.
        // The app/query requirement remains the compatibility boundary here;
        // `canonical_history_version_for_maintained_witness` supplies the
        // complete authored record at the wire boundary.
        match &request.requirements.app_fields {
            // `All` still goes through the query-local boundary. The durable
            // all-fields projection predates compatibility-sensitive reads and
            // reports a non-total enum remap as an execution error; a read
            // must instead omit precisely that row before its query graph.
            FieldRequirement::All => table
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect(),
            FieldRequirement::None => BTreeSet::new(),
            FieldRequirement::Fields(fields) => fields.clone(),
        }
    }

    pub(crate) async fn selected_global_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<Option<GraphBuilder>, SourceResolutionError> {
        let Some(access_path) = self.access_paths.get(&request.source).cloned() else {
            return Ok(None);
        };
        self.selected_global_current_source_graph_for_access_path(request, table, tier, access_path)
            .await
    }

    /// The only storage-backed half of a Local-first covered receiver input.
    ///
    /// `Ahead` contains versions this node has not yet retired into the
    /// durable Global winner (pending local work, including a pending local
    /// deletion).  The matching complete Global frontier is supplied only by
    /// the authority's exact `CoveredInput` receipt.  Keeping these sources
    /// distinct is what makes an authority retraction observable even though
    /// the client may retain an old received Global row in its database.
    async fn pending_local_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let graph = self
            .visible_current_physical_source_graph(
                request,
                table,
                PhysicalCurrentClass::Ahead,
                DurabilityTier::Local,
            )
            .await?;
        self.exclude_settled_arm(request, graph, true).await
    }

    /// Read a physical current arm with the ordinary deletion fence. The
    /// authority input remains independent of this storage-backed Ahead arm.
    async fn visible_current_physical_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        current_class: PhysicalCurrentClass,
        deletion_tier: DurabilityTier,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let projection_target = self.current_projection_target(request, table)?;
        let content = self
            .node
            .physical_current_source_graph_with_projection_target(
                self.read_view.read_schema,
                &request.source.table,
                current_class,
                projection_target,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let deleted_winners = self
            .projected_deletion_register_current_source_graph(request, deletion_tier)?
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]);
        Ok(GraphBuilder::anti_join(
            content,
            deleted_winners,
            ["row_uuid"],
            ["row_uuid"],
        ))
    }

    /// Reuse the access paths chosen while compiling the already-prepared
    /// policy dependency. Source resolution does not inspect policy syntax or
    /// select a policy-specific path; it only consumes this generic planner
    /// receipt to narrow the protected source before the same proof graph is
    /// joined back in below.
    fn cached_policy_authorization_access_path(
        &mut self,
        request: &SourceRequest,
    ) -> Result<Option<CurrentAccessPath>, SourceResolutionError> {
        let Some(policy_request) = self
            .policy_dependency_request(request)
            .map_err(|error| source_resolution_error_from_policy_proof(request, error))?
        else {
            return Ok(None);
        };
        let cache_key = policy_authorization_graph_cache_key(&policy_request);
        let Some(authorization) = self.node.policy_authorization_graph_cache_get(&cache_key) else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        Ok(authorization.access_paths.get(&request.source).cloned())
    }

    async fn selected_global_current_source_graph_for_access_path(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
        access_path: CurrentAccessPath,
    ) -> Result<Option<GraphBuilder>, SourceResolutionError> {
        match access_path {
            CurrentAccessPath::PrimaryKey(prefix) => {
                if self.count_access_path_metrics {
                    self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                }
                Ok(Some(selected_visible_current_primary_key_graph(
                    table, tier, prefix,
                )))
            }
            CurrentAccessPath::Index {
                column,
                prefix,
                intersections,
                source_limit,
                ..
            } => {
                if tier != DurabilityTier::Global {
                    return Ok(None);
                }
                let source_limit = (request.visibility == RowVisibility::IncludeDeleted)
                    .then_some(source_limit)
                    .flatten();
                let projection_target = self.current_projection_target(request, table)?;
                let rows = self
                    .node
                    .physical_global_current_source_for_index_scan(
                        table,
                        self.read_view.read_schema,
                        &column,
                        &prefix,
                        &intersections,
                        false,
                        source_limit,
                        &projection_target,
                    )
                    .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?;
                self.node.query_engine_read_metrics.source_index_probes +=
                    1 + intersections.len() as u64;
                Ok(Some(rows))
            }
        }
    }

    fn pending_deletion_winner_graph(
        &mut self,
        request: &SourceRequest,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let ahead = if self.needs_projected_current_source(&request.source.table) {
            let table_id = self
                .node
                .physical_table_id_for_schema(self.read_view.read_schema, &request.source.table)
                .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
            GraphBuilder::table(physical_register_ahead_current_table_name(table_id))
        } else {
            GraphBuilder::table(register_ahead_current_table_name(&request.source.table))
        };
        let fields = register_storage_fields_for_query_engine("");
        // Ahead can contain several unconfirmed operations for the same row.
        // A pending restore must supersede an earlier pending deletion.
        Ok(GraphBuilder::arg_max_by(
            ahead.project_fields(fields.clone()),
            ["row_uuid"],
            ["tx_time", "tx_node_id"],
        )
        .project_fields(fields))
    }

    pub(crate) fn deletion_register_source_for_request(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        graph_tier: Option<DurabilityTier>,
        history_position: Option<GlobalTime>,
        open_tx_overlay: Option<OpenTransactionId>,
    ) -> Result<Option<DeletionRegisterSource>, SourceResolutionError> {
        if !request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::DeletionMarkers)
        {
            return Ok(None);
        }
        let Some(tier) = graph_tier else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        if request.visibility != RowVisibility::Visible
            || history_position.is_some()
            || open_tx_overlay.is_some()
        {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        }
        if matches!(self.read_view.sources.get(&request.source),
            Some(SourceExpr::WithOverlays { overlays, .. })
                if overlays.entries == [OverlayRef::PendingLocal])
        {
            return Ok(Some(DeletionRegisterSource {
                graph: self.pending_deletion_winner_graph(request)?,
                row_uuid_field: "row_uuid".to_owned(),
            }));
        }
        if self.needs_projected_current_source(&request.source.table) {
            return Ok(Some(DeletionRegisterSource {
                graph: self.projected_deletion_register_current_source_graph(request, tier)?,
                row_uuid_field: "row_uuid".to_owned(),
            }));
        }
        let register_table = self
            .node
            .physical_register_table_for_schema(
                self.node.catalogue.current_schema_version_id,
                &table.name,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        Ok(Some(DeletionRegisterSource {
            graph: deletion_register_current_source_graph(&table.name, &register_table, tier),
            row_uuid_field: "row_uuid".to_owned(),
        }))
    }

    pub(crate) async fn content_version_source_for_request(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        graph_tier: Option<DurabilityTier>,
        history_position: Option<GlobalTime>,
        open_tx_overlay: Option<OpenTransactionId>,
    ) -> Result<Option<ContentVersionSource>, SourceResolutionError> {
        if !request
            .requirements
            .metadata
            .contains(&SourceMetadataRequirement::VersionPayloads)
        {
            return Ok(None);
        }
        let Some(tier) = graph_tier else {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        };
        if request.visibility != RowVisibility::Visible
            || history_position.is_some()
            || open_tx_overlay.is_some()
        {
            return Err(source_resolution_error(request, SourceGap::Coverage));
        }
        if self.needs_projected_current_source(&request.source.table) {
            return Ok(Some(ContentVersionSource {
                graph: self
                    .projected_content_current_source_graph(request, table, tier, false, false)
                    .await?,
                row_uuid_field: "row_uuid".to_owned(),
            }));
        }
        Ok(Some(ContentVersionSource {
            graph: content_version_current_source_graph(table, tier, false),
            row_uuid_field: "row_uuid".to_owned(),
        }))
    }

    pub(crate) async fn projected_historical_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        position: GlobalTime,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        if self.can_use_bounded_historical_source(&request.source.table) {
            self.node
                .query_engine_read_metrics
                .source_global_time_range_scans += 1;
            let rows = self
                .node
                .bounded_historical_current_rows(&request.source.table, position)
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
            return inline_current_graph(table, rows)
                .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut));
        }
        self.node.query_engine_read_metrics.source_full_scans += 1;
        let rows = self
            .node
            .projected_historical_current_rows(
                &request.source.table,
                self.read_view.read_schema,
                position,
            )
            .await
            .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))?;
        inline_current_graph(table, rows)
            .map_err(|_| source_resolution_error(request, SourceGap::HistoricalStorageCut))
    }

    pub(crate) async fn projected_maintained_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<CurrentSourceGraph, SourceResolutionError> {
        // The schema-aware projection already retains the complete current
        // version witness tuple.  Joining it to the generic physical witness
        // graph would independently decode every enum cell, defeating a
        // title-only old-schema subscription before its narrowed source has a
        // chance to replace unused enum values with typed nulls.
        let projected = self
            .projected_content_current_source_graph(request, table, tier, true, true)
            .await?;
        Ok(CurrentSourceGraph {
            graph: projected,
            descriptor: current_row_descriptor(table),
            metadata: BTreeMap::new(),
        })
    }

    /// Build the maintained content-winner relation for one logical branch
    /// key. `stored_keys` includes historical short spellings produced before
    /// monotone branch-column additions; they compete as one branch-local row.
    pub(crate) async fn projected_branch_content_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
        stored_keys: &BTreeSet<BranchKey>,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let required_fields = self.current_projection_required_fields(request, table);
        let (projection_target, physical_fields) = self
            .node
            .ensure_physical_current_winner_projection(
                self.read_view.read_schema,
                &request.source.table,
            )
            .await
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let post_winner_fields = self
            .node
            .physical_current_post_winner_projection_fields(
                self.read_view.read_schema,
                &request.source.table,
                &required_fields,
            )
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let branch_sources = |class, target: String| {
            stored_keys
                .iter()
                .map(|branch_key| {
                    self.node
                        .physical_current_branch_source_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            class,
                            target.clone(),
                            branch_key,
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))
                })
                .collect::<Result<Vec<_>, _>>()
                .map(GraphBuilder::union)
        };
        let global = branch_sources(PhysicalCurrentClass::Global, projection_target.clone())?
            .project(physical_fields.clone());
        let content = if tier == DurabilityTier::Global {
            global
        } else {
            let ahead = branch_sources(PhysicalCurrentClass::Ahead, projection_target)?;
            let ahead = if tier == DurabilityTier::Edge {
                edge_visible_ahead_current_source_graph(ahead, physical_fields.clone())
            } else {
                ahead.project(physical_fields.clone())
            };
            GraphBuilder::arg_max_by(
                GraphBuilder::union([global, ahead]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(physical_fields)
        };
        Ok(content.project_fields(post_winner_fields))
    }

    /// Build the maintained deletion-register winner relation for one logical
    /// branch key, normalizing historical short key spellings before winner
    /// selection.
    pub(crate) async fn projected_branch_deletion_source_graph(
        &mut self,
        request: &SourceRequest,
        tier: DurabilityTier,
        stored_keys: &BTreeSet<BranchKey>,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let table_id = self
            .node
            .physical_table_id_for_schema(self.read_view.read_schema, &request.source.table)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let fields = std::iter::once("branch_key".to_owned())
            .chain(register_storage_field_names())
            .collect::<Vec<_>>();
        let branch_sources = |table_name: String| {
            GraphBuilder::union(stored_keys.iter().map(|branch_key| {
                GraphBuilder::table_scan(
                    table_name.clone(),
                    groove::ivm::StaticScanSpec::Prefix(vec![groove::ivm::LiteralValue::from(
                        Value::Bytes(branch_key.canonical_bytes()),
                    )]),
                )
            }))
        };
        let global = branch_sources(physical_register_global_current_table_name(table_id))
            .project(fields.clone());
        if tier == DurabilityTier::Global {
            return Ok(global);
        }
        let ahead = branch_sources(physical_register_ahead_current_table_name(table_id));
        let ahead = if tier == DurabilityTier::Edge {
            edge_visible_ahead_current_source_graph(ahead, fields.clone())
        } else {
            ahead.project(fields.clone())
        };
        Ok(GraphBuilder::arg_max_by(
            GraphBuilder::union([global, ahead]),
            ["row_uuid"],
            ["tx_time", "tx_node_id"],
        )
        .project(fields))
    }

    pub(crate) fn projected_content_current_source_graph<'a>(
        &'a mut self,
        request: &'a SourceRequest,
        read_table: &'a TableSchema,
        tier: DurabilityTier,
        include_global_time: bool,
        exclude_deleted: bool,
    ) -> Pin<Box<dyn Future<Output = Result<GraphBuilder, SourceResolutionError>> + 'a>> {
        Box::pin(async move {
            let fields = global_current_storage_fields(read_table, true, include_global_time);
            // Global current storage has already selected the physical winner.  Apply
            // the ordinary lens-aware projection directly so added-column defaults
            // survive instead of being replaced with physical nulls by the raw
            // winner projection.  Local and Edge reads still need to choose between
            // Global and Ahead candidates before their compatibility boundary.
            if tier == DurabilityTier::Global {
                let projection_target = self.current_projection_target(request, read_table)?;
                let content = match self.access_paths.get(&request.source).cloned() {
                    Some(CurrentAccessPath::PrimaryKey(prefix)) => {
                        if self.count_access_path_metrics {
                            self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                        }
                        self.node
                            .physical_current_source_scan_graph_with_projection_target(
                                self.read_view.read_schema,
                                &request.source.table,
                                PhysicalCurrentClass::Global,
                                projection_target,
                                static_scan_for_prefix(prefix, 1),
                            )
                            .map_err(|_| {
                                source_resolution_error(request, SourceGap::SchemaProjection)
                            })?
                    }
                    Some(CurrentAccessPath::Index {
                        column,
                        prefix,
                        intersections,
                        source_limit,
                        maintained,
                    }) => {
                        let source_limit = (!exclude_deleted).then_some(source_limit).flatten();
                        self.node.query_engine_read_metrics.source_index_probes +=
                            1 + intersections.len() as u64;
                        self.node
                            .physical_global_current_source_for_index_scan(
                                read_table,
                                self.read_view.read_schema,
                                &column,
                                &prefix,
                                &intersections,
                                maintained,
                                source_limit,
                                &projection_target,
                            )
                            .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
                    }
                    None => {
                        self.node.query_engine_read_metrics.source_full_scans += 1;
                        self.node
                            .physical_current_source_graph_with_projection_target(
                                self.read_view.read_schema,
                                &request.source.table,
                                PhysicalCurrentClass::Global,
                                projection_target,
                            )
                            .map_err(|_| {
                                source_resolution_error(request, SourceGap::SchemaProjection)
                            })?
                    }
                };
                if !exclude_deleted {
                    return Ok(content.project(fields));
                }
                let deleted_winners = self
                    .projected_deletion_register_current_source_graph(request, tier)?
                    .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                    .project(["row_uuid"]);
                return Ok(GraphBuilder::anti_join(
                    content,
                    deleted_winners,
                    ["row_uuid"],
                    ["row_uuid"],
                ));
            }
            let required_fields = self.current_projection_required_fields(request, read_table);
            let (projection_target, physical_fields) = self
                .node
                .ensure_physical_current_winner_projection(
                    self.read_view.read_schema,
                    &request.source.table,
                )
                .await
                .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
            let post_winner_fields = self
                .node
                .physical_current_post_winner_projection_fields(
                    self.read_view.read_schema,
                    &request.source.table,
                    &required_fields,
                )
                .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
            let raw_global_output = self
                .node
                .physical_table_id_for_schema(self.read_view.read_schema, &request.source.table)
                .and_then(|table_id| {
                    self.node
                        .database
                        .table_schema(&physical_global_current_table_name(table_id))
                        .map(|schema| schema.record_schema())
                        .map_err(Error::Groove)
                })
                .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
            let access_path = self.access_paths.get(&request.source).cloned();
            let global = match &access_path {
                Some(CurrentAccessPath::PrimaryKey(prefix)) => {
                    if self.count_access_path_metrics {
                        self.node.query_engine_read_metrics.source_primary_key_scans += 1;
                    }
                    self.node
                        .physical_current_source_scan_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            PhysicalCurrentClass::Global,
                            projection_target.clone(),
                            static_scan_for_prefix(prefix.clone(), 1),
                        )
                        .map_err(|_| {
                            source_resolution_error(request, SourceGap::SchemaProjection)
                        })?
                }
                Some(CurrentAccessPath::Index {
                    column,
                    prefix,
                    intersections,
                    source_limit,
                    maintained,
                }) => {
                    // Select settled candidates before combining them with the
                    // corresponding Local ahead candidates below.
                    let source_limit = (!exclude_deleted).then_some(*source_limit).flatten();
                    self.node.query_engine_read_metrics.source_index_probes +=
                        1 + intersections.len() as u64;
                    self.node
                        .physical_global_current_source_for_index_scan_with_output(
                            read_table,
                            self.read_view.read_schema,
                            column,
                            prefix,
                            intersections,
                            *maintained,
                            source_limit,
                            &projection_target,
                            raw_global_output.clone(),
                        )
                        .map_err(|_| source_resolution_error(request, SourceGap::Coverage))?
                }
                _ => {
                    self.node.query_engine_read_metrics.source_full_scans += 1;
                    self.node
                        .physical_current_source_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            PhysicalCurrentClass::Global,
                            projection_target.clone(),
                        )
                        .map_err(|_| {
                            source_resolution_error(request, SourceGap::SchemaProjection)
                        })?
                }
            };
            let global = self.exclude_settled_arm(request, global, false).await?;
            let content = if tier == DurabilityTier::Global {
                global
            } else {
                let global = global.project(physical_fields.clone());
                let ahead = match &access_path {
                    Some(CurrentAccessPath::PrimaryKey(prefix)) => self
                        .node
                        .physical_current_source_scan_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            PhysicalCurrentClass::Ahead,
                            projection_target.clone(),
                            static_scan_for_prefix(prefix.clone(), 3),
                        ),
                    Some(CurrentAccessPath::Index { .. }) => {
                        // A Local-ahead winner can change the indexed column and
                        // therefore no longer be present under the settled
                        // candidate's prefix.  Scan the ahead current table in
                        // full before arg-max so that every possible dominating
                        // row participates.  The settled side remains safely
                        // index-bounded through this same access-path mechanism.
                        self.node.query_engine_read_metrics.source_full_scans += 1;
                        self.node
                            .physical_current_source_graph_with_projection_target(
                                self.read_view.read_schema,
                                &request.source.table,
                                PhysicalCurrentClass::Ahead,
                                projection_target,
                            )
                    }
                    _ => self
                        .node
                        .physical_current_source_graph_with_projection_target(
                            self.read_view.read_schema,
                            &request.source.table,
                            PhysicalCurrentClass::Ahead,
                            projection_target,
                        ),
                }
                .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
                let ahead = self.exclude_settled_arm(request, ahead, true).await?;
                let ahead = if tier == DurabilityTier::Edge {
                    edge_visible_ahead_current_source_graph(ahead, physical_fields.clone())
                } else {
                    ahead.project(physical_fields.clone())
                };
                GraphBuilder::arg_max_by(
                    GraphBuilder::union([global, ahead]),
                    ["row_uuid"],
                    ["tx_time", "tx_node_id"],
                )
                .project(physical_fields)
            };
            let content = content.project_fields(post_winner_fields);
            if !exclude_deleted {
                return Ok(content.project(fields));
            }
            let deleted_winners = self
                .projected_deletion_register_current_source_graph(request, tier)?
                .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
                .project(["row_uuid"]);
            Ok(GraphBuilder::anti_join(
                content,
                deleted_winners,
                ["row_uuid"],
                ["row_uuid"],
            ))
        })
    }

    pub(crate) fn projected_deletion_register_current_source_graph(
        &mut self,
        request: &SourceRequest,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let table_id = self
            .node
            .physical_table_id_for_schema(self.read_view.read_schema, &request.source.table)
            .map_err(|_| source_resolution_error(request, SourceGap::SchemaProjection))?;
        let fields = register_storage_fields_for_query_engine("");
        let global = GraphBuilder::table(physical_register_global_current_table_name(table_id));
        if tier == DurabilityTier::Global {
            return Ok(global);
        }
        let global = global.project_fields(fields.clone());
        let ahead = GraphBuilder::table(physical_register_ahead_current_table_name(table_id));
        let ahead = if tier == DurabilityTier::Edge {
            edge_visible_ahead_current_source_graph(ahead, register_storage_field_names())
        } else {
            ahead.project_fields(fields.clone())
        };
        Ok(GraphBuilder::arg_max_by(
            GraphBuilder::union([global, ahead]),
            ["row_uuid"],
            ["tx_time", "tx_node_id"],
        )
        .project_fields(fields))
    }

    pub(crate) async fn projected_visible_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<CurrentSourceGraph, SourceResolutionError> {
        Ok(CurrentSourceGraph {
            // Project heterogeneous physical rows at the source boundary so the
            // rest of the query graph retains the requested logical descriptor.
            graph: self
                .projected_content_current_source_graph(request, table, tier, false, true)
                .await?
                .project_fields(storage_to_canonical_current_source_fields(
                    table, true, false,
                )),
            descriptor: current_row_descriptor(table),
            metadata: BTreeMap::new(),
        })
    }

    async fn projected_include_deleted_current_source_graph(
        &mut self,
        request: &SourceRequest,
        table: &TableSchema,
        tier: DurabilityTier,
    ) -> Result<GraphBuilder, SourceResolutionError> {
        let content = self
            .projected_content_current_source_graph(request, table, tier, false, false)
            .await?
            .project_fields(storage_to_canonical_current_source_fields(
                table, true, false,
            ));
        let deleted_winners = self
            .projected_deletion_register_current_source_graph(request, tier)?
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project_fields([
                ProjectField::named("row_uuid"),
                ProjectField::named("tx_time"),
                ProjectField::named("tx_node_id"),
                ProjectField::renamed("updated_by", "$updatedBy"),
                ProjectField::renamed("updated_at", "$updatedAt"),
            ]);
        let undeleted = GraphBuilder::anti_join(
            content.clone(),
            deleted_winners.clone(),
            ["row_uuid"],
            ["row_uuid"],
        )
        .project_fields(
            current_row_fields(table)
                .into_iter()
                .map(ProjectField::named)
                .chain([ProjectField::literal("__jazz_deleted", Value::Bool(false))]),
        );
        let deleted = GraphBuilder::join(content, deleted_winners, ["row_uuid"], ["row_uuid"])
            .project_fields(
                current_row_fields(table)
                    .into_iter()
                    .map(|field| {
                        let source = match field.as_str() {
                            "$updatedBy" | "$updatedAt" | "tx_time" | "tx_node_id" => {
                                right_field(&field)
                            }
                            _ => left_field(&field),
                        };
                        ProjectField::renamed(source, field)
                    })
                    .chain([ProjectField::literal("__jazz_deleted", Value::Bool(true))]),
            );
        Ok(GraphBuilder::union([undeleted, deleted]))
    }

    pub(crate) fn can_use_bounded_historical_source(&self, table: &str) -> bool {
        if self.read_view.read_schema != self.node.catalogue.current_schema_version_id {
            return false;
        }
        self.node
            .physical_table_id_for_schema(self.read_view.read_schema, table)
            .is_ok()
    }

    pub(crate) fn needs_projected_current_source(&mut self, table: &str) -> bool {
        self.node
            .physical_table_id_for_schema(self.read_view.read_schema, table)
            .is_ok()
    }
}

fn deletion_register_current_source_graph(
    table: &str,
    physical_register_table: &str,
    tier: DurabilityTier,
) -> GraphBuilder {
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(register_global_current_table_name(table))
            .project_fields(register_storage_fields_for_query_engine(""));
    }
    let current_keys = deletion_register_current_keys_graph(table, tier);
    GraphBuilder::join(
        GraphBuilder::table(physical_register_table),
        current_keys,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(register_storage_fields_for_query_engine("left."))
}

pub(super) fn edge_accepted_transaction_source_graph() -> GraphBuilder {
    GraphBuilder::table("jazz_transactions")
        .filter(
            PredicateExpr::And(vec![
                PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
                PredicateExpr::Or(vec![
                    PredicateExpr::eq("durability", Value::EnumTag(2)),
                    PredicateExpr::eq("durability", Value::EnumTag(3)),
                ])
                .canonicalize(),
            ])
            .canonicalize(),
        )
        .project(["time", "node_id"])
}

pub(super) fn edge_visible_ahead_current_source_graph(
    source: GraphBuilder,
    fields: Vec<String>,
) -> GraphBuilder {
    GraphBuilder::join(
        source.project(fields.clone()),
        edge_accepted_transaction_source_graph(),
        ["tx_time", "tx_node_id"],
        ["time", "node_id"],
    )
    .project_fields(
        fields
            .into_iter()
            .map(|field| ProjectField::renamed(left_field(&field), field)),
    )
}

fn content_version_current_source_graph(
    table: &TableSchema,
    tier: DurabilityTier,
    include_global_time: bool,
) -> GraphBuilder {
    let mut fields = maintained_view_history_storage_field_names(table);
    if include_global_time {
        fields.push("global_time".to_owned());
    }
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(global_current_table_name(&table.name)).project(fields);
    }
    let ahead = if tier == DurabilityTier::Edge {
        GraphBuilder::join(
            GraphBuilder::table(ahead_current_table_name(&table.name)).project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::Or(vec![
                        PredicateExpr::eq("durability", Value::EnumTag(2)),
                        PredicateExpr::eq("durability", Value::EnumTag(3)),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            fields
                .iter()
                .cloned()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    } else {
        GraphBuilder::table(ahead_current_table_name(&table.name)).project(fields.clone())
    };
    GraphBuilder::arg_max_by(
        GraphBuilder::union([
            GraphBuilder::table(global_current_table_name(&table.name)).project(fields.clone()),
            ahead,
        ]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    )
    .project(fields)
}

fn deletion_register_current_keys_graph(table: &str, tier: DurabilityTier) -> GraphBuilder {
    let key_fields = ["row_uuid", "tx_time", "tx_node_id"];
    if tier == DurabilityTier::Global {
        return GraphBuilder::table(register_global_current_table_name(table)).project(key_fields);
    }
    let ahead = if tier == DurabilityTier::Edge {
        GraphBuilder::join(
            GraphBuilder::table(register_ahead_current_table_name(table)).project(key_fields),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::Or(vec![
                        PredicateExpr::eq("durability", Value::EnumTag(2)),
                        PredicateExpr::eq("durability", Value::EnumTag(3)),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            key_fields
                .into_iter()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    } else {
        GraphBuilder::table(register_ahead_current_table_name(table)).project(key_fields)
    };
    GraphBuilder::arg_max_by(
        GraphBuilder::union([
            GraphBuilder::table(register_global_current_table_name(table)).project(key_fields),
            ahead,
        ]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    )
    .project(key_fields)
}

fn selected_visible_current_primary_key_graph(
    table: &TableSchema,
    tier: DurabilityTier,
    prefix: Vec<Value>,
) -> GraphBuilder {
    let user_fields = table
        .columns
        .iter()
        .map(|column| user_column_field(&column.name))
        .collect::<Vec<_>>();
    let mut content_fields = vec![
        "row_uuid".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
        "authored_columns".to_owned(),
    ];
    content_fields.extend(user_fields.iter().cloned());
    content_fields.extend([
        "created_by".to_owned(),
        "created_at".to_owned(),
        "updated_by".to_owned(),
        "updated_at".to_owned(),
        "tx_time".to_owned(),
        "tx_node_id".to_owned(),
    ]);
    let content_scan = static_scan_for_prefix(prefix.clone(), 1);
    let deletion_scan = static_scan_for_prefix(prefix, 1);
    let edge_visible_ahead = |table_name: String, fields: Vec<String>, scan: StaticScanSpec| {
        GraphBuilder::join(
            GraphBuilder::table_scan(table_name, scan).project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::And(vec![
                        PredicateExpr::eq("fate", Value::EnumTag(FateTag::Accepted as u8)),
                        PredicateExpr::Or(vec![
                            PredicateExpr::eq("durability", Value::EnumTag(2)),
                            PredicateExpr::eq("durability", Value::EnumTag(3)),
                        ])
                        .canonicalize(),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            fields
                .into_iter()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    };
    let (content_current, deleted_winners) = if tier == DurabilityTier::Global {
        (
            GraphBuilder::table_scan(global_current_table_name(&table.name), content_scan)
                .project(content_fields.clone()),
            GraphBuilder::table_scan(
                register_global_current_table_name(&table.name),
                deletion_scan,
            )
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]),
        )
    } else {
        let ahead_content = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                ahead_current_table_name(&table.name),
                content_fields.clone(),
                content_scan.clone(),
            )
        } else {
            GraphBuilder::table_scan(ahead_current_table_name(&table.name), content_scan.clone())
                .project(content_fields.clone())
        };
        let deletion_fields = vec![
            "row_uuid".to_owned(),
            "tx_time".to_owned(),
            "tx_node_id".to_owned(),
            "created_by".to_owned(),
            "created_at".to_owned(),
            "updated_by".to_owned(),
            "updated_at".to_owned(),
            "_deletion".to_owned(),
        ];
        let ahead_deleted = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                register_ahead_current_table_name(&table.name),
                deletion_fields.clone(),
                deletion_scan.clone(),
            )
        } else {
            GraphBuilder::table_scan(
                register_ahead_current_table_name(&table.name),
                deletion_scan.clone(),
            )
            .project(deletion_fields.clone())
        };
        (
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    GraphBuilder::table_scan(global_current_table_name(&table.name), content_scan)
                        .project(content_fields.clone()),
                    ahead_content,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(content_fields.clone()),
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    GraphBuilder::table_scan(
                        register_global_current_table_name(&table.name),
                        deletion_scan,
                    )
                    .project(deletion_fields),
                    ahead_deleted,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
            .project(["row_uuid"]),
        )
    };
    GraphBuilder::anti_join(content_current, deleted_winners, ["row_uuid"], ["row_uuid"])
        .project(content_fields)
}

pub(super) fn register_storage_fields_for_query_engine(prefix: &str) -> Vec<ProjectField> {
    register_storage_field_names()
        .into_iter()
        .map(|field| ProjectField::renamed(format!("{prefix}{field}"), field))
        .collect()
}

pub(super) fn register_storage_field_names() -> Vec<String> {
    [
        "row_uuid",
        "tx_time",
        "tx_node_id",
        "schema_version",
        "parents",
        "created_by",
        "created_at",
        "updated_by",
        "updated_at",
        "_deletion",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn source_resolution_error(request: &SourceRequest, gap: SourceGap) -> SourceResolutionError {
    SourceResolutionError {
        request: Box::new(request.clone()),
        gap,
    }
}

fn source_resolution_error_from_policy_proof(
    request: &SourceRequest,
    error: Error,
) -> SourceResolutionError {
    match error {
        Error::PolicyProofCycle { table, depth } => {
            source_resolution_error(request, SourceGap::PolicyProofCycle { table, depth })
        }
        Error::QueryCapability(message) => {
            let Some((table, depth)) = policy_proof_cycle_from_capability(&message) else {
                return source_resolution_error(request, SourceGap::Coverage);
            };
            source_resolution_error(request, SourceGap::PolicyProofCycle { table, depth })
        }
        _ => source_resolution_error(request, SourceGap::Coverage),
    }
}

fn policy_proof_cycle_from_capability(message: &str) -> Option<(String, usize)> {
    let (_, suffix) = message.rsplit_once("PolicyProofCycle { table: \"")?;
    let (table, suffix) = suffix.split_once('"')?;
    let (_, suffix) = suffix.split_once(", depth: ")?;
    let depth = suffix
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    Some((table.to_owned(), depth))
}

fn policy_read_view_projected_through(
    policy_view: &ReadView<RequestedSourceStage>,
    enclosing_view: &ReadView<RequestedSourceStage>,
) -> ReadView<RequestedSourceStage> {
    let mut projected = policy_view.clone();
    let enclosing_frozen_view = enclosing_view
        .sources
        .values()
        .find_map(|source| match source {
            SourceExpr::SnapshotRef { snapshot, .. } => Some((snapshot.clone(), None)),
            SourceExpr::WithOverlays { input, overlays }
                if matches!(input.as_ref(), SourceExpr::SnapshotRef { .. }) =>
            {
                let SourceExpr::SnapshotRef { snapshot, .. } = input.as_ref() else {
                    unreachable!("guarded above")
                };
                Some((snapshot.clone(), Some(overlays.clone())))
            }
            _ => None,
        });
    // The outer normalized query and the independently normalized policy proof
    // may use different aliases for one logical table. Preserve every policy
    // source, including policy-only recursive/access tables. A snapshot is a
    // program-wide cut, so policy-only sources must use it too; otherwise a
    // protected row and the relation granting access could be read at
    // different points in history.
    for (policy_source, projected_source) in &mut projected.sources {
        if let Some(source) = enclosing_view
            .sources
            .iter()
            .find_map(|(source_id, source)| {
                (source_id.table == policy_source.table).then(|| source.clone())
            })
        {
            *projected_source = source;
        } else if let Some((snapshot, overlays)) = &enclosing_frozen_view
            && let SourceExpr::VisibleCurrent {
                projection,
                data: DataSource::Current,
                ..
            } = projected_source
        {
            let snapshot_source = SourceExpr::SnapshotRef {
                projection: projection.clone(),
                data: DataSource::Current,
                snapshot: snapshot.clone(),
            };
            *projected_source = match overlays {
                Some(overlays) => SourceExpr::WithOverlays {
                    input: Box::new(snapshot_source),
                    overlays: overlays.clone(),
                },
                None => snapshot_source,
            };
        }
    }
    projected
}

pub(super) fn capability_trace_enabled() -> bool {
    std::env::var_os("JAZZ_CAPABILITY_TRACE").is_some()
        || std::env::var_os("JAZZ_CAPABILITY_TRACE_FILE").is_some()
}

pub(super) fn trace_capability_compile(
    node_uuid: NodeUuid,
    node_alias: Option<NodeAlias>,
    request: &QueryProgramRequest,
    result: Result<&QueryProgram, &CapabilityReport>,
) {
    let Some(path) = std::env::var_os("JAZZ_CAPABILITY_TRACE_FILE") else {
        if std::env::var_os("JAZZ_CAPABILITY_TRACE").is_some() {
            eprintln!(
                "JAZZ_CAPABILITY_TRACE set without JAZZ_CAPABILITY_TRACE_FILE; capability trace skipped"
            );
        }
        return;
    };
    let mut file = match std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
    {
        Ok(file) => file,
        Err(err) => {
            eprintln!(
                "failed to open JAZZ_CAPABILITY_TRACE_FILE {:?}: {err}",
                path
            );
            return;
        }
    };
    let event = match result {
        Ok(_) => "compile_success",
        Err(_) => "compile_failure",
    };
    let _ = writeln!(
        file,
        "\n=== JAZZ_CAPABILITY_TRACE {event} pid={} node_uuid={:?} node_alias={:?} ===",
        std::process::id(),
        node_uuid,
        node_alias
    );
    let _ = writeln!(file, "request:\n{request:#?}");
    match result {
        Ok(program) => {
            let _ = writeln!(file, "explain:\n{:#?}", program.explain);
            let _ = writeln!(file, "output:\n{:#?}", program.lowered.output);
        }
        Err(report) => {
            let _ = writeln!(file, "report:\n{report:#?}");
        }
    }
    let _ = writeln!(
        file,
        "backtrace:\n{}",
        std::backtrace::Backtrace::force_capture()
    );
}

/// Expose requested nested-author fields on policy preimages that retain
/// complete author records, including head-over-current branch preimages.
fn with_requested_author_paths(
    graph: GraphBuilder,
    descriptor: RecordDescriptor,
    mut metadata: BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    requirements: &SourceRequirements,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    let missing = requirements
        .metadata
        .iter()
        .filter_map(|requirement| match requirement {
            SourceMetadataRequirement::AuthorPath(name) if !metadata.contains_key(requirement) => {
                Some(name.clone())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok((graph, descriptor, metadata));
    }
    let mut fields = descriptor.fields().to_vec();
    let mut projection = descriptor_field_names(&descriptor)?
        .into_iter()
        .map(ProjectField::named)
        .collect::<Vec<_>>();
    for name in missing {
        let (root, path) = AuthorSubject::metadata_path(&name).expect("validated author path");
        projection.push(ProjectField::record_field(
            root,
            path.iter().copied(),
            name.clone(),
        ));
        fields.push(records::DescriptorField::new(
            name.clone(),
            AuthorSubject::metadata_type(&name)
                .expect("validated author path")
                .clone(),
        ));
        metadata.insert(
            SourceMetadataRequirement::AuthorPath(name.clone()),
            SourceMetadataFields::Provenance { field: name },
        );
    }
    Ok((
        graph.project_fields(projection),
        RecordDescriptor::new_with_fields(fields),
        metadata,
    ))
}

fn resolved_current_source_graph<S>(
    node: &mut NodeState<S>,
    table: &TableSchema,
    tier: DurabilityTier,
    requirements: &SourceRequirements,
    authorization: &SourceAuthorizationRequest,
    policy_schema: SchemaVersionId,
    policy_read_view: Option<&ReadView<RequestedSourceStage>>,
    branch_witness_field: Option<&str>,
    selected_base: Option<GraphBuilder>,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
        BTreeSet<String>,
    ),
    Error,
>
where
    S: OrderedKvStorage,
{
    let mut fields = canonical_current_source_fields(table, false);
    for requirement in &requirements.metadata {
        if let SourceMetadataRequirement::AuthorPath(name) = requirement {
            let (root, path) = AuthorSubject::metadata_path(name).expect("validated author path");
            fields.push(ProjectField::record_field(
                root,
                path.iter().copied(),
                name.clone(),
            ));
        }
    }
    let mut metadata = BTreeMap::new();
    let needs_version_witnesses = requirements
        .metadata
        .contains(&SourceMetadataRequirement::VersionWitnesses)
        || requirements
            .metadata
            .iter()
            .any(|requirement| matches!(requirement, SourceMetadataRequirement::Provenance(_)));
    let needs_settle_position = requirements
        .metadata
        .contains(&SourceMetadataRequirement::SettlePosition);

    if needs_version_witnesses {
        fields.extend([
            ProjectField::literal("table", Value::String(table.name.clone())),
            ProjectField::literal("layer", Value::String("content".to_owned())),
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
            ProjectField::named("authored_columns"),
            ProjectField::renamed("$createdBy", "created_by"),
            ProjectField::renamed("$createdAt", "created_at"),
            ProjectField::renamed("$updatedBy", "updated_by"),
            ProjectField::renamed("$updatedAt", "updated_at"),
        ]);
        if let Some(branch_witness_field) = branch_witness_field {
            fields.push(ProjectField::named(branch_witness_field));
        }
        metadata.insert(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: branch_witness_field.map(str::to_owned),
            },
        );
    }
    if needs_settle_position {
        fields.push(ProjectField::named("settle_position"));
        metadata.insert(
            SourceMetadataRequirement::SettlePosition,
            SourceMetadataFields::SettlePosition {
                settle_position_field: "settle_position".to_owned(),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::Coverage)
    {
        fields.push(ProjectField::literal(
            "coverage",
            Value::String("visible-current".to_owned()),
        ));
        metadata.insert(
            SourceMetadataRequirement::Coverage,
            SourceMetadataFields::Coverage {
                coverage_field: "coverage".to_owned(),
            },
        );
    }
    for requirement in &requirements.metadata {
        if let SourceMetadataRequirement::AuthorPath(name) = requirement {
            metadata.insert(
                requirement.clone(),
                SourceMetadataFields::Provenance {
                    field: name.clone(),
                },
            );
        }
        if let SourceMetadataRequirement::Provenance(field) = requirement {
            metadata.insert(
                SourceMetadataRequirement::Provenance(*field),
                SourceMetadataFields::Provenance {
                    // Every resolver branch normalizes storage names before it
                    // exposes this source. Metadata therefore names the
                    // canonical field on the resolved graph, never the
                    // physical storage column used below that boundary.
                    field: source_provenance_field(*field).to_owned(),
                },
            );
        }
    }

    // The current graph below carries storage cells, not materialized public
    // values. In particular JSON has a distinct internal cell descriptor.
    // Bind the prepared layout to that same source contract; materialization
    // belongs at the public read boundary, not in terminal-layout repair.
    let descriptor = if branch_witness_field.is_none() {
        current_row_descriptor_with_hidden_source_fields_for_current_storage(table, &metadata)
    } else {
        current_row_descriptor_with_hidden_source_fields_for_branch(table, &metadata, true)
    };
    let (base, routing_fields) = match authorization {
        SourceAuthorizationRequest::System => {
            let graph = if let Some(selected_base) = selected_base.clone() {
                let mut selected_fields = storage_to_canonical_current_source_fields(
                    table,
                    needs_version_witnesses,
                    needs_settle_position,
                );
                if let Some(branch_witness_field) = branch_witness_field {
                    selected_fields.push(ProjectField::named(branch_witness_field));
                }
                selected_base.project_fields(selected_fields)
            } else if needs_version_witnesses {
                node.maintained_view_content_current_with_version(table, tier)?
                    .project_fields(storage_to_canonical_current_source_fields(
                        table,
                        true,
                        needs_settle_position,
                    ))
            } else {
                visible_current_graph(table, tier)
                    .project_fields(canonical_current_source_fields(table, false))
            };
            (graph, BTreeSet::new())
        }
        SourceAuthorizationRequest::PolicyFiltered {
            permission_subject,
            plan,
        }
        | SourceAuthorizationRequest::PolicyProof {
            permission_subject,
            plan,
        } => {
            if plan.protected_source.table != table.name
                || plan.role != PolicyDecisionRole::Read
                || plan.protected_row_field != "row_uuid"
            {
                return Err(Error::QueryCapability(
                    "policy authorization plan does not match resolved source".to_owned(),
                ));
            }
            let binding_source_shape = plan.binding_source_shape.clone();
            let binding_user_params = plan.binding_user_params.clone();
            let binding_claim_params = plan.binding_claim_params.clone();
            let param_binding_mode = if binding_source_shape.is_some() {
                ParamBindingMode::RetainAllParams
            } else {
                ParamBindingMode::InlineAllReachableSeeds
            };
            let policy_request = node.table_read_policy_authorization_request(
                policy_schema,
                &table.name,
                *permission_subject,
                param_binding_mode,
                tier,
                binding_source_shape.clone(),
                binding_user_params.clone(),
                binding_claim_params,
            );
            let policy_request = policy_request.map(|mut request| {
                if let Some(read_view) = policy_read_view {
                    request.reads.primary =
                        policy_read_view_projected_through(&request.reads.primary, read_view);
                }
                request
            });
            let mut output_fields = global_current_storage_fields(
                table,
                needs_version_witnesses,
                needs_settle_position,
            );
            if let Some(branch_witness_field) = branch_witness_field {
                output_fields.push(branch_witness_field.to_owned());
            }
            let base = match selected_base {
                Some(selected_base) => selected_base,
                None => node.maintained_view_content_current_with_version(table, tier)?,
            };
            let storage_graph = node.compose_policy_filtered_current_source_graph(
                policy_request,
                base.clone(),
                &output_fields,
            )?;
            let mut canonical_fields = storage_to_canonical_current_source_fields(
                table,
                needs_version_witnesses,
                needs_settle_position,
            );
            if let Some(branch_witness_field) = branch_witness_field {
                canonical_fields.push(ProjectField::named(branch_witness_field));
            }
            canonical_fields.extend(
                storage_graph
                    .route_fields
                    .iter()
                    .map(|field| ProjectField::named(field.clone())),
            );
            (
                storage_graph.graph.project_fields(canonical_fields),
                storage_graph.route_fields,
            )
        }
    };
    fields.extend(
        routing_fields
            .iter()
            .map(|field| ProjectField::named(field.clone())),
    );
    let graph = if metadata.is_empty() {
        base
    } else {
        base.project_fields(fields)
    };
    Ok((graph, descriptor, metadata, routing_fields))
}

fn canonical_current_source_fields(
    table: &TableSchema,
    include_version: bool,
) -> Vec<ProjectField> {
    let mut fields = std::iter::once(ProjectField::named("row_uuid"))
        .chain(table.columns.iter().map(|column| {
            ProjectField::named_with_identity(
                user_column_field(&column.name),
                records::FieldIdentity::Name(column.name.clone()),
            )
        }))
        .chain([
            ProjectField::named("$createdBy"),
            ProjectField::named("$createdAt"),
            ProjectField::named("$updatedBy"),
            ProjectField::named("$updatedAt"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
        ])
        .collect::<Vec<_>>();
    if include_version {
        fields.extend([
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
            ProjectField::named("authored_columns"),
        ]);
    }
    fields
}

fn source_provenance_field(field: ProvenanceField) -> &'static str {
    match field {
        ProvenanceField::CreatedAt => "$createdAt",
        ProvenanceField::CreatedBy => "$createdBy",
        ProvenanceField::UpdatedAt => "$updatedAt",
        ProvenanceField::UpdatedBy => "$updatedBy",
    }
}

fn storage_to_canonical_current_source_fields(
    table: &TableSchema,
    include_version: bool,
    include_settle_position: bool,
) -> Vec<ProjectField> {
    let mut fields = std::iter::once(ProjectField::named("row_uuid"))
        .chain(table.columns.iter().map(|column| {
            ProjectField::named_with_identity(
                user_column_field(&column.name),
                records::FieldIdentity::Name(column.name.clone()),
            )
        }))
        .chain([
            ProjectField::renamed("created_by", "$createdBy"),
            ProjectField::renamed("created_at", "$createdAt"),
            ProjectField::renamed("updated_by", "$updatedBy"),
            ProjectField::renamed("updated_at", "$updatedAt"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
        ])
        .collect::<Vec<_>>();
    if include_version {
        fields.extend([
            ProjectField::named("schema_version"),
            ProjectField::named("parents"),
            ProjectField::named("authored_columns"),
        ]);
    }
    if include_settle_position {
        fields.push(ProjectField::renamed("global_time", "settle_position"));
    }
    fields
}

fn branch_view_storage_source_fields(
    table: &TableSchema,
    head: &BranchKey,
) -> Result<Vec<ProjectField>, Error> {
    let head_values = head.values.iter().cloned().collect::<BTreeMap<_, _>>();
    let mut fields = vec![
        ProjectField::renamed("branch_key", "supplying_branch_key"),
        ProjectField::named("row_uuid"),
        ProjectField::named("schema_version"),
        ProjectField::named("parents"),
        ProjectField::named("authored_columns"),
    ];
    for column in &table.columns {
        if table.branch_by.contains(&column.name) {
            let value = head_values
                .get(&column.name)
                .ok_or(Error::InvalidBranchKey(
                    "head branch key missing projected table column".to_owned(),
                ))?
                .decode()
                .map_err(|_| {
                    Error::InvalidBranchKey("invalid head branch value encoding".to_owned())
                })?;
            fields.push(ProjectField::literal_with_identity(
                user_column_field(&column.name),
                value,
                records::FieldIdentity::Name(column.name.clone()),
            ));
        } else {
            fields.push(ProjectField::named_with_identity(
                user_column_field(&column.name),
                records::FieldIdentity::Name(column.name.clone()),
            ));
        }
    }
    fields.extend([
        ProjectField::named("created_by"),
        ProjectField::named("created_at"),
        ProjectField::named("updated_by"),
        ProjectField::named("updated_at"),
        ProjectField::named("tx_time"),
        ProjectField::named("tx_node_id"),
        ProjectField::named("global_time"),
    ]);
    Ok(fields)
}

pub(super) fn current_row_descriptor_with_hidden_source_fields(
    table: &TableSchema,
    metadata: &BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
) -> RecordDescriptor {
    current_row_descriptor_with_hidden_source_fields_for_branch(table, metadata, false)
}

/// The current-source compatibility boundary is the schema's canonical
/// current-row representation, not the authored column spelling.  Physical
/// catalogue registration assigns stable enum identities there; a
/// receiver-owned input that later unions with a current arm must retain those
/// identities exactly.
pub(super) fn current_row_descriptor_with_hidden_source_fields_for_current_storage(
    table: &TableSchema,
    metadata: &BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
) -> RecordDescriptor {
    let logical = current_row_descriptor_with_hidden_source_fields(table, metadata);
    let current = table.global_current_storage_tables()[0].record_schema();
    let current_types = table
        .columns
        .iter()
        .filter_map(|column| {
            let storage_name = crate::schema::app_storage_column_name(&column.name);
            current
                .field_index(&storage_name)
                .and_then(|index| current.fields().get(index))
                .map(|field| {
                    (
                        user_column_field(&column.name),
                        (field.identity.clone(), field.value_type.clone()),
                    )
                })
        })
        .collect::<BTreeMap<_, _>>();
    RecordDescriptor::new_with_fields(logical.fields().iter().map(|field| {
        let name = field
            .name
            .clone()
            .expect("compiler-owned current source descriptor fields are named");
        let (identity, value_type) = current_types
            .get(&name)
            .map(|(identity, value_type)| {
                let merged_identity = match (field.identity.clone(), identity.clone()) {
                    (
                        Some(records::FieldIdentity::Name(logical_name)),
                        Some(records::FieldIdentity::Slot(slot)),
                    ) => Some(records::FieldIdentity::NamedSlot {
                        name: logical_name,
                        slot,
                    }),
                    (
                        Some(records::FieldIdentity::Name(logical_name)),
                        Some(records::FieldIdentity::NamedSlot { slot, .. }),
                    ) => Some(records::FieldIdentity::NamedSlot {
                        name: logical_name,
                        slot,
                    }),
                    (
                        Some(records::FieldIdentity::Name(logical_name)),
                        Some(records::FieldIdentity::Name(storage_name)),
                    ) if storage_name == name && logical_name != storage_name => {
                        Some(records::FieldIdentity::Name(logical_name))
                    }
                    (_, Some(storage_identity)) => Some(storage_identity),
                    (logical_identity, None) => logical_identity,
                };
                (merged_identity, value_type.clone())
            })
            .unwrap_or_else(|| (field.identity.clone(), field.value_type.clone()));
        records::DescriptorField {
            name: Some(name),
            identity,
            value_type,
        }
    }))
}

fn current_row_descriptor_with_hidden_source_fields_for_branch(
    table: &TableSchema,
    metadata: &BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    branch_columns_nonnullable: bool,
) -> RecordDescriptor {
    current_row_descriptor_with_hidden_source_fields_for_branch_and_deletion(
        table,
        metadata,
        branch_columns_nonnullable,
        false,
    )
}

fn current_row_descriptor_with_hidden_source_fields_for_branch_and_deletion(
    table: &TableSchema,
    metadata: &BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    branch_columns_nonnullable: bool,
    include_deletion_marker: bool,
) -> RecordDescriptor {
    let mut fields = std::iter::once(records::DescriptorField::new("row_uuid", ValueType::Uuid))
        .chain(table.columns.iter().map(|column| {
            let value_type = if branch_columns_nonnullable && table.branch_by.contains(&column.name)
            {
                column.column_type.clone()
            } else {
                ValueType::Nullable(Box::new(column.column_type.clone()))
            };
            current_row_column_field(column, value_type)
        }))
        .chain([
            records::DescriptorField::new("$createdBy", RowAuthor::value_type()),
            records::DescriptorField::new("$createdAt", ValueType::U64),
            records::DescriptorField::new("$updatedBy", RowAuthor::value_type()),
            records::DescriptorField::new("$updatedAt", ValueType::U64),
            records::DescriptorField::new("tx_time", ValueType::U64),
            records::DescriptorField::new("tx_node_id", ValueType::U64),
        ])
        .collect::<Vec<_>>();
    for requirement in metadata.keys() {
        if let SourceMetadataRequirement::AuthorPath(name) = requirement {
            fields.push(records::DescriptorField::new(
                name.clone(),
                AuthorSubject::metadata_type(name)
                    .expect("author path")
                    .clone(),
            ));
        }
    }
    if metadata.contains_key(&SourceMetadataRequirement::VersionWitnesses) {
        fields.extend([
            records::DescriptorField::new("table", ValueType::String),
            records::DescriptorField::new("layer", ValueType::String),
            records::DescriptorField::new("schema_version", ValueType::U64),
            records::DescriptorField::new(
                "parents",
                ValueType::Array(Box::new(ValueType::Tuple(vec![
                    ValueType::U64,
                    ValueType::Uuid,
                ]))),
            ),
            records::DescriptorField::new(
                "authored_columns",
                ValueType::Nullable(Box::new(ValueType::Array(Box::new(ValueType::U64)))),
            ),
            records::DescriptorField::new("created_by", RowAuthor::value_type()),
            records::DescriptorField::new("created_at", ValueType::U64),
            records::DescriptorField::new("updated_by", RowAuthor::value_type()),
            records::DescriptorField::new("updated_at", ValueType::U64),
        ]);
        if let Some(SourceMetadataFields::VersionWitnesses {
            branch_or_prefix_field: Some(field),
            ..
        }) = metadata.get(&SourceMetadataRequirement::VersionWitnesses)
        {
            fields.push(records::DescriptorField::new(
                field.clone(),
                ValueType::Bytes,
            ));
        }
    }
    if branch_columns_nonnullable
        && !metadata.contains_key(&SourceMetadataRequirement::VersionWitnesses)
    {
        fields.push(records::DescriptorField::new(
            "supplying_branch_key",
            ValueType::Bytes,
        ));
    }
    if metadata.contains_key(&SourceMetadataRequirement::SettlePosition) {
        fields.push(records::DescriptorField::new(
            "settle_position",
            ValueType::Nullable(Box::new(ValueType::U64)),
        ));
    }
    if metadata.contains_key(&SourceMetadataRequirement::Coverage) {
        fields.push(records::DescriptorField::new("coverage", ValueType::String));
    }
    if include_deletion_marker {
        fields.push(records::DescriptorField::new(
            "__jazz_deleted",
            ValueType::Bool,
        ));
    }
    RecordDescriptor::new_with_fields(fields)
}

impl<S> NodeState<S>
where
    S: OrderedKvStorage,
{
    /// Select physical current-source access paths from the normalized program
    /// itself. `Access path` is standard query-planner terminology, but this
    /// deliberately trivial, rule-based planner only recognizes supported
    /// indexed equality constraints and otherwise falls back to a full scan;
    /// it makes no cardinality/selectivity estimates or cost comparisons.
    /// This intentionally has no policy branch: an authorization proof is just
    /// another normalized program with a different policy context. Every
    /// selected path merely narrows candidate rows; the lowered predicate graph
    /// still decides membership.
    pub(super) fn query_program_access_paths(
        &self,
        request: &QueryProgramRequest,
        allow_secondary_indexes: bool,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        // A policy proof may contain both an owner arm and a correlated
        // membership arm for the same protected source. Walking its nested
        // nodes independently would see the owner's claim equality and apply
        // that index path to the entire union, incorrectly excluding rows
        // that only the membership arm authorizes. The small planner does not
        // prove per-arm coverage, so any alternative or relational proof
        // retains full source scans.
        if request.input.shape.nodes.values().any(|node| {
            matches!(
                node,
                RowSetExpr::Union { .. }
                    | RowSetExpr::Join { .. }
                    | RowSetExpr::RecursiveRelation { .. }
            )
        }) {
            return Ok(BTreeMap::new());
        }
        self.normalized_program_access_paths(
            &request.input,
            &request.reads.primary,
            &request.policy,
            false,
            allow_secondary_indexes,
        )
    }

    fn normalized_program_access_paths(
        &self,
        input: &RowSetProgramInput,
        read_view: &ReadView<RequestedSourceStage>,
        policy: &PolicyContext,
        allow_local: bool,
        allow_secondary_indexes: bool,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        let mut equalities_by_source = BTreeMap::new();
        // This deliberately small access-path selector only recognizes a
        // source's own Filter -> Source pipeline.  Inspecting every normalized
        // node lets independent recursive seed/step pipelines participate,
        // while still refusing to infer restrictions through joins, unions,
        // or other relational operators.
        for node_id in input.shape.nodes.keys() {
            let Some(equalities) =
                normalized_program_equalities(&input.shape, node_id, &input.binding, policy)?
            else {
                continue;
            };
            for (source, equalities) in equalities {
                equalities_by_source
                    .entry(source)
                    .or_insert_with(BTreeMap::new)
                    .extend(equalities);
            }
        }

        let mut paths = BTreeMap::new();
        for (source, equalities) in equalities_by_source {
            let Some(tier) = read_view.source_current_tier(&source) else {
                continue;
            };
            let table = self.table_in_schema(&source.table, read_view.read_schema)?;
            let Some(mut path) = select_current_access_path(&table, &equalities) else {
                continue;
            };
            // Authorization dependencies are cached by policy shape and claim
            // schema, not by a resolved claim value. A secondary-index prefix
            // derived from this request could therefore make a later identity
            // reuse another identity's candidate set. Keep those reusable
            // graphs identity-neutral; maintained root views are compiled for
            // this concrete request and may safely select their own index.
            if !allow_secondary_indexes && matches!(path, CurrentAccessPath::Index { .. }) {
                continue;
            }
            if !allow_local && let CurrentAccessPath::Index { maintained, .. } = &mut path {
                *maintained = true;
            }
            // Local/Edge sources still combine the selected settled candidates
            // with the complete ahead overlay before choosing a winner, so a
            // newer row which leaves an equality prefix cannot leave behind a
            // stale settled match.
            if matches!(
                tier,
                DurabilityTier::Global | DurabilityTier::Local | DurabilityTier::Edge
            ) {
                paths.insert(source, path);
            }
        }
        Ok(paths)
    }

    pub(super) fn one_shot_access_paths(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
        tier: DurabilityTier,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        if !matches!(tier, DurabilityTier::Local | DurabilityTier::Global) {
            return Ok(BTreeMap::new());
        }
        let normalized = self.normalized_row_set_shape(shape, binding)?;
        let input = RowSetProgramInput {
            binding: self.program_binding_for_shape(
                shape,
                binding,
                None,
                BTreeMap::new(),
                BTreeMap::new(),
            ),
            shape: normalized,
        };
        let reads = current_query_read_set(
            &input.shape,
            shape.schema_version(),
            shape.schema_version(),
            tier,
            None,
            None,
            false,
        );
        let mut paths = self.normalized_program_access_paths(
            &input,
            &reads.primary,
            &PolicyContext::System,
            true,
            true,
        )?;

        // A source cap is stronger than an index access path: it is only safe
        // when the source is itself the final result prefix. In particular,
        // ordinary visible reads retain their unbounded path because the
        // deletion anti-join can discard sparse candidates after this source.
        // `projected_content_current_source_graph` drops this cap whenever that
        // anti-join is present, leaving the narrow IncludeDeleted one-shot
        // shape below as the initial receipt.
        let query = shape.query();
        let Some(limit) = query.limit else {
            return Ok(paths);
        };
        if query.offset != 0
            || !query.order_by.is_empty()
            || !query.joins.is_empty()
            || query.flat_join.is_some()
            || !query.policy_branches.is_empty()
            || !query.reachable.is_empty()
            || !query.inherits.is_empty()
            || !query.includes.is_empty()
            || !query.array_subqueries.is_empty()
            || query.aggregate.is_some()
        {
            return Ok(paths);
        }
        let root = root_source_id(&query.table);
        let table = self.table_in_schema(&query.table, shape.schema_version())?;
        if table.has_any_policy() {
            return Ok(paths);
        }
        // A Local ahead winner can remove an otherwise matching settled row,
        // making a prefix cap miss a later settled candidate that must fill
        // the public limit after arg-max. Only the Global physical winner is
        // stable enough for this source bound.
        if tier == DurabilityTier::Global
            && let Some(CurrentAccessPath::Index { source_limit, .. }) = paths.get_mut(&root)
        {
            *source_limit = Some(limit);
        }
        Ok(paths)
    }

    pub(super) fn current_query_primary_key_access_paths(
        &self,
        shape: &ValidatedQuery,
        binding: &Binding,
    ) -> Result<BTreeMap<SourceId, CurrentAccessPath>, Error> {
        let query = shape.query();
        let mut access_paths = BTreeMap::new();
        let equalities = root_literal_equalities(query, binding)?;
        let table = self.table_in_schema(&query.table, shape.schema_version())?;
        // A maintained authorization scope reacts to both the content winner
        // and its deletion register. The point source is only incrementally
        // complete for an unscoped row: inside a policy graph, its content cap
        // can strand the deletion-driven membership transition.
        let has_declared_id = table.columns.iter().any(|column| column.name == "id");
        if !table.has_any_policy()
            && !has_declared_id
            && let Some(value) = equalities.get("id").cloned()
        {
            access_paths.insert(
                root_source_id(&query.table),
                CurrentAccessPath::PrimaryKey(vec![value]),
            );
        }
        self.add_reachable_access_paths(query, shape.schema_version(), binding, &mut access_paths)?;
        Ok(access_paths)
    }

    fn add_reachable_access_paths(
        &self,
        query: &JazzQuery,
        schema_version: SchemaVersionId,
        binding: &Binding,
        access_paths: &mut BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<(), Error> {
        for (index, reachable) in query.reachable.iter().enumerate() {
            let reachable_id = format!("reachable:{index}");
            if let Some(seed) = &reachable.seed {
                let source = reachable_seed_source_id(seed, &reachable_id);
                self.add_primary_key_access_path_for_filters(
                    &source,
                    &seed.table,
                    schema_version,
                    &seed.filters,
                    binding,
                    access_paths,
                )?;
            }
            let edge_source = reachable_edge_source_id(reachable, &reachable_id);
            self.add_primary_key_access_path_for_filters(
                &edge_source,
                &reachable.edge_table,
                schema_version,
                &reachable.edge_filters,
                binding,
                access_paths,
            )?;
            let access_source = reachable_access_source_id(reachable, &reachable_id);
            self.add_primary_key_access_path_for_filters(
                &access_source,
                &reachable.access_table,
                schema_version,
                &reachable.access_filters,
                binding,
                access_paths,
            )?;
        }
        Ok(())
    }

    fn add_primary_key_access_path_for_filters(
        &self,
        source: &SourceId,
        table_name: &str,
        schema_version: SchemaVersionId,
        filters: &[Predicate],
        binding: &Binding,
        access_paths: &mut BTreeMap<SourceId, CurrentAccessPath>,
    ) -> Result<(), Error> {
        let table = self.table_in_schema(table_name, schema_version)?;
        let equalities = literal_equalities_for_filters(filters, binding)?;
        if let Some(access_path) = select_current_access_path(&table, &equalities)
            && matches!(access_path, CurrentAccessPath::PrimaryKey(_))
        {
            access_paths.insert(source.clone(), access_path);
        }
        Ok(())
    }

    fn physical_global_current_source_for_index_scan(
        &self,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        column: &str,
        prefix: &[Value],
        intersections: &[(String, Vec<Value>)],
        maintained: bool,
        source_limit: Option<usize>,
        projection_target: &str,
    ) -> Result<GraphBuilder, Error> {
        self.physical_global_current_source_for_index_scan_with_output(
            table,
            schema_version,
            column,
            prefix,
            intersections,
            maintained,
            source_limit,
            projection_target,
            table.global_current_storage_tables()[0].record_schema(),
        )
    }

    fn physical_global_current_source_for_index_scan_with_output(
        &self,
        table: &TableSchema,
        schema_version: SchemaVersionId,
        column: &str,
        prefix: &[Value],
        intersections: &[(String, Vec<Value>)],
        maintained: bool,
        source_limit: Option<usize>,
        projection_target: &str,
        _output: RecordDescriptor,
    ) -> Result<GraphBuilder, Error> {
        let mapping = self
            .catalogue
            .physical_mappings
            .get(&schema_version)
            .and_then(|mapping| mapping.tables.get(&table.name))
            .ok_or(Error::InvalidStoredValue(
                "physical current index table mapping missing",
            ))?;
        let column_id = mapping
            .columns
            .get(column)
            .copied()
            .ok_or(Error::InvalidStoredValue(
                "physical current index column mapping missing",
            ))?;
        let storage_table = physical_global_current_table_name(mapping.table_id);
        // Root reads address the shared (empty) branch coordinate. Physical
        // current indexes include that coordinate first so identical user keys
        // from branch-local rows cannot alias the shared index domain.
        let index_prefix = |prefix: &[Value]| {
            std::iter::once(Value::Bytes(BranchKey::default().canonical_bytes()))
                .chain(prefix.iter().cloned())
                .collect::<Vec<_>>()
        };
        let scan_prefix = index_prefix(prefix);
        let scan = match source_limit {
            Some(max_items) => StaticScanSpec::PrefixLimit {
                prefix: scan_prefix
                    .iter()
                    .cloned()
                    .map(LiteralValue::from)
                    .collect(),
                max_items,
            },
            None => StaticScanSpec::Prefix(
                scan_prefix
                    .iter()
                    .cloned()
                    .map(LiteralValue::from)
                    .collect(),
            ),
        };
        let intersections = intersections
            .iter()
            .map(|(column, prefix)| {
                let column_id =
                    mapping
                        .columns
                        .get(column)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical current intersected index column mapping missing",
                        ))?;
                Ok((
                    physical_current_index_name(column_id),
                    StaticScanSpec::Prefix(
                        index_prefix(prefix)
                            .into_iter()
                            .map(LiteralValue::from)
                            .collect(),
                    ),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let primary_index = physical_current_index_name(column_id);
        if maintained {
            // `IndexedRowsIntersection` is a hydration request, not a live
            // source.  Model each equality as an index source and express the
            // intersection in IVM so table deltas which enter or leave either
            // prefix drive ordinary semi-join updates.
            let mut graph = GraphBuilder::variant_index_scan(
                storage_table.clone(),
                primary_index,
                projection_target,
                scan,
            );
            for (index, scan) in intersections {
                let right = GraphBuilder::variant_index_scan(
                    storage_table.clone(),
                    index,
                    projection_target,
                    scan,
                );
                graph = GraphBuilder::semi_join(graph, right, ["row_uuid"], ["row_uuid"]);
            }
            Ok(graph)
        } else {
            Ok(GraphBuilder::variant_index_intersection_scan(
                storage_table,
                primary_index,
                scan,
                intersections,
                projection_target,
            ))
        }
    }
}

/// Return only restrictions that are plainly safe to push into a physical
/// source.  The result is intentionally conservative: this planner does not
/// reason through relational operators or alternative predicate branches. It
/// may retain equality restrictions from an enclosing conjunction. A full
/// normalized program still runs after the scan, so this is a physical
/// candidate-selection optimization, never a second evaluator.
fn normalized_program_equalities(
    shape: &NormalizedRowSetShape,
    node_id: &RowSetNodeId,
    binding: &ProgramBinding,
    policy: &PolicyContext,
) -> Result<Option<BTreeMap<SourceId, BTreeMap<String, Value>>>, Error> {
    let node = shape.nodes.get(node_id).ok_or(Error::InvalidStoredValue(
        "normalized access-path node missing",
    ))?;
    match node {
        RowSetExpr::Filter { input, predicate } => {
            let Some(mut equalities) =
                normalized_program_equalities(shape, input, binding, policy)?
            else {
                return Ok(None);
            };
            collect_normalized_program_equalities(predicate, binding, policy, &mut equalities)?;
            Ok(Some(equalities))
        }
        RowSetExpr::Distinct { input, .. }
        | RowSetExpr::OrderBy { input, .. }
        | RowSetExpr::Project { input, .. }
        | RowSetExpr::Slice { input, .. } => {
            normalized_program_equalities(shape, input, binding, policy)
        }
        RowSetExpr::Source { source, .. } => {
            Ok(Some(BTreeMap::from([(source.clone(), BTreeMap::new())])))
        }
        RowSetExpr::Aggregate { .. }
        | RowSetExpr::CorrelatedPathProjection { .. }
        | RowSetExpr::FrontierSource { .. }
        | RowSetExpr::Join { .. }
        | RowSetExpr::RecursiveRelation { .. }
        | RowSetExpr::Union { .. }
        | RowSetExpr::ValueSource { .. } => Ok(None),
    }
}

fn collect_normalized_program_equalities(
    predicate: &NormalizedPredicateExpr,
    binding: &ProgramBinding,
    policy: &PolicyContext,
    equalities: &mut BTreeMap<SourceId, BTreeMap<String, Value>>,
) -> Result<(), Error> {
    match predicate {
        NormalizedPredicateExpr::And(predicates) => {
            for predicate in predicates {
                collect_normalized_program_equalities(predicate, binding, policy, equalities)?;
            }
        }
        NormalizedPredicateExpr::Compare {
            left,
            op: NormalizedComparisonOp::Eq,
            right,
        } => {
            if let Some((source, field, value)) =
                normalized_program_equality(left, right, binding, policy)?
            {
                equalities
                    .entry(source)
                    .or_default()
                    .entry(field)
                    .or_insert(value);
            } else if let Some((source, field, value)) =
                normalized_program_equality(right, left, binding, policy)?
            {
                equalities
                    .entry(source)
                    .or_default()
                    .entry(field)
                    .or_insert(value);
            }
        }
        NormalizedPredicateExpr::ArrayContains { .. }
        | NormalizedPredicateExpr::Compare { .. }
        | NormalizedPredicateExpr::EnumMatch { .. }
        | NormalizedPredicateExpr::False
        | NormalizedPredicateExpr::In { .. }
        | NormalizedPredicateExpr::IsNotNull(_)
        | NormalizedPredicateExpr::IsNull(_)
        | NormalizedPredicateExpr::Not(_)
        | NormalizedPredicateExpr::TextContains { .. } => {}
        // Do not descend into a disjunction: an equality within only one
        // branch is not a restriction on the other branch. An enclosing AND
        // may still contribute an independent safe equality.
        NormalizedPredicateExpr::Or(_) => {}
        NormalizedPredicateExpr::True => {}
    }
    Ok(())
}

fn normalized_program_equality(
    field: &NormalizedValueRef,
    value: &NormalizedValueRef,
    binding: &ProgramBinding,
    policy: &PolicyContext,
) -> Result<Option<(SourceId, String, Value)>, Error> {
    let Some((source, field)) = normalized_program_source_field(field) else {
        return Ok(None);
    };
    let Some(value) = normalized_bound_value(value, binding, policy)? else {
        return Ok(None);
    };
    if matches!(value, Value::Nullable(_)) {
        // The physical current index stores its own nullable envelope.  Do
        // not manufacture a nested nullable prefix for a nullable/missing
        // claim; retain the ordinary scan and predicate instead.
        return Ok(None);
    }
    Ok(Some((source, field, value)))
}

fn normalized_program_source_field(value: &NormalizedValueRef) -> Option<(SourceId, String)> {
    match value {
        NormalizedValueRef::SourceField { source, field } => Some((source.clone(), field.clone())),
        NormalizedValueRef::RowId(RowIdRef::Source(source)) => {
            Some((source.clone(), "id".to_owned()))
        }
        _ => None,
    }
}

fn normalized_bound_value(
    value: &NormalizedValueRef,
    binding: &ProgramBinding,
    policy: &PolicyContext,
) -> Result<Option<Value>, Error> {
    match value {
        NormalizedValueRef::Param(name) => Ok(binding.values.get(name).cloned()),
        NormalizedValueRef::Literal(bytes) => postcard::from_bytes::<Value>(bytes)
            .map(Some)
            .map_err(|err| Error::QueryLowering(format!("literal decoding failed: {err}"))),
        // Access-path selection is optional.  System reads have no identity
        // from which to bind a claim, so they retain the ordinary scan rather
        // than failing while considering this candidate optimization.
        NormalizedValueRef::Claim(_) if matches!(policy, PolicyContext::System) => Ok(None),
        NormalizedValueRef::Claim(path) => prepared_claim_value(path, policy),
        _ => Ok(None),
    }
}

pub(super) fn current_row_fields(table: &TableSchema) -> Vec<String> {
    current_row_field_names(table)
}

pub(super) fn global_current_storage_fields(
    table: &TableSchema,
    include_version: bool,
    include_settle_position: bool,
) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    if include_version {
        fields.extend([
            "schema_version".to_owned(),
            "parents".to_owned(),
            "authored_columns".to_owned(),
        ]);
    }
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| user_column_field(&column.name)),
    );
    fields.push("created_by".to_owned());
    fields.push("created_at".to_owned());
    fields.push("updated_by".to_owned());
    fields.push("updated_at".to_owned());
    fields.push("tx_time".to_owned());
    fields.push("tx_node_id".to_owned());
    if include_settle_position {
        fields.push("global_time".to_owned());
    }
    fields
}

fn current_row_descriptor(table: &TableSchema) -> RecordDescriptor {
    // Source metadata and the executable canonical-row projection must carry
    // the same logical identities before subsequent joins restore this shape.
    current_row_descriptor_with_hidden_source_fields(table, &BTreeMap::new())
}

pub(super) fn empty_authorized_row_id_graph() -> GraphBuilder {
    GraphBuilder::inline_records(
        RecordDescriptor::new([("row_uuid", ValueType::Uuid)]),
        Vec::<Vec<u8>>::new(),
    )
}

fn inline_current_record(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
) -> Result<Vec<u8>, Error> {
    let mut values = Vec::with_capacity(table.columns.len() + 7);
    values.push(Value::Uuid(row.row_uuid().0));
    for column in &table.columns {
        values.push(Value::Nullable(row.cell(table, &column.name).map(Box::new)));
    }
    let provenance = row.provenance()?.ok_or(Error::InvalidStoredValue(
        "physical current source row is missing provenance",
    ))?;
    values.push(row_author_value(provenance.created_by)?);
    values.push(Value::U64(provenance.created_at));
    values.push(row_author_value(provenance.updated_by)?);
    values.push(Value::U64(provenance.updated_at));
    let (tx_time, tx_node_alias) = row
        .projected_tx_alias()
        .unwrap_or((TxTime(0), NodeAlias(0)));
    values.push(Value::U64(tx_time.0));
    values.push(Value::U64(tx_node_alias.0));
    append_author_projection_values(
        &mut values,
        descriptor,
        provenance.created_by,
        provenance.updated_by,
    )?;
    Ok(descriptor.create(&values)?)
}

fn inline_current_graph(table: &TableSchema, rows: Vec<CurrentRow>) -> Result<GraphBuilder, Error> {
    let descriptor = current_row_descriptor(table);
    let records = rows
        .iter()
        .map(|row| inline_current_record(table, &descriptor, row))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(GraphBuilder::inline_records(descriptor, records))
}

fn inline_current_graph_with_source_metadata(
    table: &TableSchema,
    rows: Vec<CurrentRow>,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
    requirements: &SourceRequirements,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    inline_current_graph_with_source_metadata_and_branch_witness(
        table,
        rows,
        schema_version_alias,
        coverage,
        requirements,
        None,
    )
}

#[cfg(test)]
pub(super) fn inline_current_graph_with_source_metadata_for_test(
    table: &TableSchema,
    rows: Vec<CurrentRow>,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
    requirements: &SourceRequirements,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    inline_current_graph_with_source_metadata(
        table,
        rows,
        schema_version_alias,
        coverage,
        requirements,
    )
}

fn inline_current_graph_with_source_metadata_and_branch_witness(
    table: &TableSchema,
    rows: Vec<CurrentRow>,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
    requirements: &SourceRequirements,
    branch_witness: Option<(&str, &BranchKey)>,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    let metadata = inline_source_metadata(requirements, branch_witness.map(|(field, _)| field));
    let descriptor = current_row_descriptor_with_hidden_source_fields_for_branch(
        table,
        &metadata,
        branch_witness.is_some(),
    );
    let records = rows
        .iter()
        .map(|row| {
            inline_current_record_with_source_metadata(
                table,
                &descriptor,
                row,
                schema_version_alias,
                coverage,
                branch_witness,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        GraphBuilder::inline_records(descriptor.clone(), records),
        descriptor,
        metadata,
    ))
}

pub(super) fn inline_source_metadata(
    requirements: &SourceRequirements,
    branch_witness_field: Option<&str>,
) -> BTreeMap<SourceMetadataRequirement, SourceMetadataFields> {
    let mut metadata = BTreeMap::new();
    // Provenance is carried by the same content-version witness as ordinary
    // table sources.  Inline candidates must expose that full capability too:
    // a provenance-only requirement still needs the hidden version fields the
    // query program uses to prove and evaluate the source.
    let needs_version_witnesses = requirements
        .metadata
        .contains(&SourceMetadataRequirement::VersionWitnesses)
        || requirements
            .metadata
            .iter()
            .any(|requirement| matches!(requirement, SourceMetadataRequirement::Provenance(_)));
    if needs_version_witnesses {
        metadata.insert(
            SourceMetadataRequirement::VersionWitnesses,
            SourceMetadataFields::VersionWitnesses {
                schema_version_field: "schema_version".to_owned(),
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node_id".to_owned(),
                branch_or_prefix_field: branch_witness_field.map(str::to_owned),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::Coverage)
    {
        metadata.insert(
            SourceMetadataRequirement::Coverage,
            SourceMetadataFields::Coverage {
                coverage_field: "coverage".to_owned(),
            },
        );
    }
    if requirements
        .metadata
        .contains(&SourceMetadataRequirement::SettlePosition)
    {
        metadata.insert(
            SourceMetadataRequirement::SettlePosition,
            SourceMetadataFields::SettlePosition {
                settle_position_field: "settle_position".to_owned(),
            },
        );
    }
    for requirement in &requirements.metadata {
        if let SourceMetadataRequirement::AuthorPath(name) = requirement {
            metadata.insert(
                requirement.clone(),
                SourceMetadataFields::Provenance {
                    field: name.clone(),
                },
            );
        }
        if let SourceMetadataRequirement::Provenance(field) = requirement {
            metadata.insert(
                SourceMetadataRequirement::Provenance(*field),
                SourceMetadataFields::Provenance {
                    field: source_provenance_field(*field).to_owned(),
                },
            );
        }
    }
    metadata
}

fn inline_current_record_with_source_metadata(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
    branch_witness: Option<(&str, &BranchKey)>,
) -> Result<Vec<u8>, Error> {
    inline_current_record_with_source_metadata_and_deletion(
        table,
        descriptor,
        row,
        schema_version_alias,
        coverage,
        branch_witness,
        None,
    )
}

/// Preserve the supplying physical branch in covered version witnesses, even
/// when the query projects that version into a different logical branch.
pub(super) fn covered_input_source_metadata(
    requirements: &SourceRequirements,
    table: &TableSchema,
) -> BTreeMap<SourceMetadataRequirement, SourceMetadataFields> {
    inline_source_metadata(
        requirements,
        (!table.branch_by.is_empty()).then_some("supplying_branch_key"),
    )
}

/// Encode one already-authorized current row for a receiver-owned covered
/// input. The descriptor comes from the exact compiled source occurrence;
/// callers must never synthesize it from a table or result collector.
pub(super) fn covered_input_record(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
    schema_version_alias: SchemaVersionAlias,
    source_branch: &BranchKey,
) -> Result<Vec<u8>, Error> {
    inline_current_record_with_source_metadata(
        table,
        descriptor,
        row,
        schema_version_alias,
        "authority-covered-input",
        descriptor
            .field_index("supplying_branch_key")
            .map(|_| ("supplying_branch_key", source_branch)),
    )
}

fn inline_current_record_with_source_metadata_and_deletion(
    table: &TableSchema,
    descriptor: &RecordDescriptor,
    row: &CurrentRow,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
    branch_witness: Option<(&str, &BranchKey)>,
    deletion_marker: Option<bool>,
) -> Result<Vec<u8>, Error> {
    let mut values = Vec::new();
    values.push(Value::Uuid(row.row_uuid().0));
    for (column_index, column) in table.columns.iter().enumerate() {
        let value = row.cell(table, &column.name);
        if branch_witness.is_some()
            && table.branch_by.contains(&column.name)
            && !matches!(
                &descriptor.fields()[column_index + 1].value_type,
                ValueType::Nullable(_)
            )
        {
            values.push(value.ok_or(Error::InvalidStoredValue(
                "frozen branch row is missing a branch column value",
            ))?);
        } else {
            values.push(Value::Nullable(value.map(Box::new)));
        }
    }
    let provenance = row.provenance()?.ok_or(Error::InvalidStoredValue(
        "physical current source row is missing provenance",
    ))?;
    values.extend([
        row_author_value(provenance.created_by)?,
        Value::U64(provenance.created_at),
        row_author_value(provenance.updated_by)?,
        Value::U64(provenance.updated_at),
    ]);
    let (tx_time, tx_node_alias) = row
        .projected_tx_alias()
        .unwrap_or((TxTime(0), NodeAlias(0)));
    values.extend([Value::U64(tx_time.0), Value::U64(tx_node_alias.0)]);
    append_author_projection_values(
        &mut values,
        descriptor,
        provenance.created_by,
        provenance.updated_by,
    )?;
    if descriptor.field_index("table").is_some() {
        values.extend([
            Value::String(table.name.clone()),
            Value::String("content".to_owned()),
            Value::U64(schema_version_alias.0),
            Value::Array(Vec::new()),
            Value::Nullable(None),
            row_author_value(provenance.created_by)?,
            Value::U64(provenance.created_at),
            row_author_value(provenance.updated_by)?,
            Value::U64(provenance.updated_at),
        ]);
    }
    if let Some((_, branch_key)) = branch_witness {
        values.push(Value::Bytes(branch_key.canonical_bytes()));
    }
    if descriptor.field_index("settle_position").is_some() {
        values.push(Value::Nullable(None));
    }
    if descriptor.field_index("coverage").is_some() {
        values.push(Value::String(coverage.to_owned()));
    }
    if let Some(deleted) = deletion_marker {
        values.push(Value::Bool(deleted));
    }
    Ok(descriptor.create(&values)?)
}

fn inline_snapshot_include_deleted_current_graph_with_source_metadata(
    table: &TableSchema,
    rows: Vec<(CurrentRow, bool)>,
    schema_version_alias: SchemaVersionAlias,
    coverage: &str,
    requirements: &SourceRequirements,
) -> Result<
    (
        GraphBuilder,
        RecordDescriptor,
        BTreeMap<SourceMetadataRequirement, SourceMetadataFields>,
    ),
    Error,
> {
    let metadata = inline_source_metadata(requirements, None);
    let descriptor = current_row_descriptor_with_hidden_source_fields_for_branch_and_deletion(
        table, &metadata, false, true,
    );
    let records = rows
        .iter()
        .map(|(row, deleted)| {
            inline_current_record_with_source_metadata_and_deletion(
                table,
                &descriptor,
                row,
                schema_version_alias,
                coverage,
                None,
                Some(*deleted),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        GraphBuilder::inline_records(descriptor.clone(), records),
        descriptor,
        metadata,
    ))
}

#[cfg(test)]
pub(super) fn historical_current_graph_full_scan(
    table: &TableSchema,
    table_id: PhysicalTableId,
    position: GlobalTime,
    history_rows: GraphBuilder,
) -> GraphBuilder {
    let cut_predicate = PredicateExpr::And(vec![
        PredicateExpr::eq("physical_table_id", Value::U64(table_id.0)),
        PredicateExpr::LtEq {
            field: "global_time".to_owned(),
            value: Value::U64(position.0).into(),
        },
    ])
    .canonicalize();
    let changes_for_layer = |layer: &'static str| {
        GraphBuilder::table("jazz_global_changes").filter(
            PredicateExpr::And(vec![
                cut_predicate.clone(),
                PredicateExpr::eq("layer", Value::Bytes(layer.as_bytes().to_vec())),
            ])
            .canonicalize(),
        )
    };
    let nullable_deletion_type = ValueType::Nullable(Box::new(ValueType::EnumTag(
        groove::records::ScalarEnumSchema::new("jazz_deletion", ["deleted", "restored"])
            .expect("valid deletion enum")
            .with_system_registry(groove::records::SystemVariantRegistry::deletion_state()),
    )));
    let content_events = changes_for_layer("content").project_fields([
        ProjectField::named("row_uuid"),
        ProjectField::named("tx_time"),
        ProjectField::named("tx_node_id"),
        ProjectField::literal("event_layer", Value::String("content".to_owned())),
        ProjectField::null_typed("deletion", nullable_deletion_type.clone()),
    ]);
    let register_events = changes_for_layer("deletion").project_fields([
        ProjectField::named("row_uuid"),
        ProjectField::named("tx_time"),
        ProjectField::named("tx_node_id"),
        ProjectField::literal("event_layer", Value::String("deletion".to_owned())),
        ProjectField::renamed("_deletion", "deletion"),
    ]);
    let latest_event = GraphBuilder::arg_max_by(
        GraphBuilder::union([content_events.clone(), register_events]),
        ["row_uuid"],
        ["tx_time", "tx_node_id"],
    );
    let content_winners =
        GraphBuilder::arg_max_by(content_events, ["row_uuid"], ["tx_time", "tx_node_id"]);
    let history_rows = history_rows.project(maintained_view_history_storage_field_names(table));
    let content_current = GraphBuilder::join(
        history_rows,
        content_winners,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(
        ["row_uuid".to_owned()]
            .into_iter()
            .chain(
                table
                    .columns
                    .iter()
                    .map(|column| user_column_field(&column.name)),
            )
            .map(|field| ProjectField::renamed(left_field(&field), field))
            .chain([
                ProjectField::renamed("left.created_by", "$createdBy"),
                ProjectField::renamed("left.created_at", "$createdAt"),
                ProjectField::renamed("left.updated_by", "$updatedBy"),
                ProjectField::renamed("left.updated_at", "$updatedAt"),
                ProjectField::renamed("left.tx_time", "tx_time"),
                ProjectField::renamed("left.tx_node_id", "tx_node_id"),
            ]),
    );
    let latest_content = latest_event.clone().filter(PredicateExpr::eq(
        "event_layer",
        Value::String("content".to_owned()),
    ));
    let content_is_latest = GraphBuilder::join(
        content_current.clone(),
        latest_content,
        ["row_uuid", "tx_time", "tx_node_id"],
        ["row_uuid", "tx_time", "tx_node_id"],
    )
    .project_fields(
        current_row_fields(table)
            .into_iter()
            .map(|field| ProjectField::renamed(left_field(&field), field)),
    );
    let latest_restore = latest_event.filter(
        PredicateExpr::And(vec![
            PredicateExpr::eq("event_layer", Value::String("deletion".to_owned())),
            PredicateExpr::eq(
                "deletion",
                Value::Nullable(Some(Box::new(Value::EnumTag(1)))),
            ),
        ])
        .canonicalize(),
    );
    let restored_content =
        GraphBuilder::join(content_current, latest_restore, ["row_uuid"], ["row_uuid"])
            .project_fields(
                current_row_fields(table)
                    .into_iter()
                    .map(|field| ProjectField::renamed(left_field(&field), field)),
            );
    GraphBuilder::union([content_is_latest, restored_content])
}

fn include_deleted_current_row_descriptor(table: &TableSchema) -> RecordDescriptor {
    RecordDescriptor::new(
        std::iter::once(("row_uuid".to_owned(), ValueType::Uuid))
            .chain(table.columns.iter().map(|column| {
                (
                    user_column_field(&column.name),
                    ValueType::Nullable(Box::new(column.column_type.clone())),
                )
            }))
            .chain([
                ("$createdBy".to_owned(), RowAuthor::value_type()),
                ("$createdAt".to_owned(), ValueType::U64),
                ("$updatedBy".to_owned(), RowAuthor::value_type()),
                ("$updatedAt".to_owned(), ValueType::U64),
                ("tx_time".to_owned(), ValueType::U64),
                ("tx_node_id".to_owned(), ValueType::U64),
            ])
            .chain([("__jazz_deleted".to_owned(), ValueType::Bool)]),
    )
}

fn include_deleted_branch_graph(
    table: &TableSchema,
    head: &BranchKey,
    content: GraphBuilder,
    deletions: GraphBuilder,
) -> Result<GraphBuilder, Error> {
    let content = content
        .project_fields(branch_view_storage_source_fields(table, head)?)
        .project_fields(storage_to_canonical_current_source_fields(
            table, false, false,
        ));
    let deleted_winners = deletions
        .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
        .project_fields([
            ProjectField::named("row_uuid"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
            ProjectField::renamed("updated_by", "$updatedBy"),
            ProjectField::renamed("updated_at", "$updatedAt"),
        ]);
    let undeleted = GraphBuilder::anti_join(
        content.clone(),
        deleted_winners.clone(),
        ["row_uuid"],
        ["row_uuid"],
    )
    .project_fields(
        current_row_fields(table)
            .into_iter()
            .map(ProjectField::named)
            .chain([ProjectField::literal("__jazz_deleted", Value::Bool(false))]),
    );
    let deleted = GraphBuilder::join(content, deleted_winners, ["row_uuid"], ["row_uuid"])
        .project_fields(
            current_row_fields(table)
                .into_iter()
                .map(|field| {
                    let source = match field.as_str() {
                        "$updatedBy" | "$updatedAt" | "tx_time" | "tx_node_id" => {
                            right_field(&field)
                        }
                        _ => left_field(&field),
                    };
                    ProjectField::renamed(source, field)
                })
                .chain([ProjectField::literal("__jazz_deleted", Value::Bool(true))]),
        );
    Ok(GraphBuilder::union([undeleted, deleted]))
}

fn include_deleted_current_graph(table: &TableSchema, tier: DurabilityTier) -> GraphBuilder {
    let user_fields = table
        .columns
        .iter()
        .map(|column| user_column_field(&column.name))
        .collect::<Vec<_>>();
    let mut content_storage_fields = vec![
        "row_uuid".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
        "authored_columns".to_owned(),
    ];
    content_storage_fields.extend(user_fields.iter().cloned());
    content_storage_fields.push("created_by".to_owned());
    content_storage_fields.push("created_at".to_owned());
    content_storage_fields.push("updated_by".to_owned());
    content_storage_fields.push("updated_at".to_owned());
    content_storage_fields.push("tx_time".to_owned());
    content_storage_fields.push("tx_node_id".to_owned());
    let normalize_content_fields = |graph: GraphBuilder| {
        graph.project_fields(
            ["row_uuid".to_owned()]
                .into_iter()
                .chain(user_fields.iter().cloned())
                .map(ProjectField::named)
                .chain([
                    ProjectField::renamed("created_by", "$createdBy"),
                    ProjectField::renamed("created_at", "$createdAt"),
                    ProjectField::renamed("updated_by", "$updatedBy"),
                    ProjectField::renamed("updated_at", "$updatedAt"),
                    ProjectField::named("tx_time"),
                    ProjectField::named("tx_node_id"),
                ]),
        )
    };
    let edge_visible_ahead = |table_name: String, fields: Vec<String>| {
        GraphBuilder::join(
            GraphBuilder::table(table_name).project(fields.clone()),
            GraphBuilder::table("jazz_transactions")
                .filter(
                    PredicateExpr::Or(vec![
                        PredicateExpr::eq("durability", Value::EnumTag(2)),
                        PredicateExpr::eq("durability", Value::EnumTag(3)),
                    ])
                    .canonicalize(),
                )
                .project(["time", "node_id"]),
            ["tx_time", "tx_node_id"],
            ["time", "node_id"],
        )
        .project_fields(
            fields
                .into_iter()
                .map(|field| ProjectField::renamed(left_field(&field), field)),
        )
    };
    let (content_current, deletion_current) = if tier == DurabilityTier::Global {
        (
            normalize_content_fields(
                GraphBuilder::table(global_current_table_name(&table.name))
                    .project(content_storage_fields.clone()),
            ),
            GraphBuilder::table(register_global_current_table_name(&table.name)),
        )
    } else {
        let ahead_content = if tier == DurabilityTier::Edge {
            normalize_content_fields(edge_visible_ahead(
                ahead_current_table_name(&table.name),
                content_storage_fields.clone(),
            ))
        } else {
            normalize_content_fields(
                GraphBuilder::table(ahead_current_table_name(&table.name))
                    .project(content_storage_fields.clone()),
            )
        };
        let deletion_fields = vec![
            "row_uuid".to_owned(),
            "tx_time".to_owned(),
            "tx_node_id".to_owned(),
            "created_by".to_owned(),
            "created_at".to_owned(),
            "updated_by".to_owned(),
            "updated_at".to_owned(),
            "_deletion".to_owned(),
        ];
        let ahead_deletion = if tier == DurabilityTier::Edge {
            edge_visible_ahead(
                register_ahead_current_table_name(&table.name),
                deletion_fields.clone(),
            )
        } else {
            GraphBuilder::table(register_ahead_current_table_name(&table.name))
                .project(deletion_fields.clone())
        };
        (
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    normalize_content_fields(
                        GraphBuilder::table(global_current_table_name(&table.name))
                            .project(content_storage_fields.clone()),
                    ),
                    ahead_content,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            )
            .project(current_row_fields(table)),
            GraphBuilder::arg_max_by(
                GraphBuilder::union([
                    GraphBuilder::table(register_global_current_table_name(&table.name))
                        .project(deletion_fields),
                    ahead_deletion,
                ]),
                ["row_uuid"],
                ["tx_time", "tx_node_id"],
            ),
        )
    };
    let deleted_winners = deletion_current
        .filter(PredicateExpr::eq("_deletion", Value::EnumTag(0)))
        .project_fields([
            ProjectField::named("row_uuid"),
            ProjectField::named("tx_time"),
            ProjectField::named("tx_node_id"),
            ProjectField::renamed("updated_by", "$updatedBy"),
            ProjectField::renamed("updated_at", "$updatedAt"),
        ]);
    let undeleted = GraphBuilder::anti_join(
        content_current.clone(),
        deleted_winners.clone(),
        ["row_uuid"],
        ["row_uuid"],
    )
    .project_fields(
        current_row_fields(table)
            .into_iter()
            .map(ProjectField::named)
            .chain([ProjectField::literal("__jazz_deleted", Value::Bool(false))]),
    );
    let deleted = GraphBuilder::join(content_current, deleted_winners, ["row_uuid"], ["row_uuid"])
        .project_fields(
            current_row_fields(table)
                .into_iter()
                .map(|field| {
                    let source = match field.as_str() {
                        "$updatedBy" | "$updatedAt" | "tx_time" | "tx_node_id" => {
                            right_field(&field)
                        }
                        _ => left_field(&field),
                    };
                    ProjectField::renamed(source, field)
                })
                .chain([ProjectField::literal("__jazz_deleted", Value::Bool(true))]),
        );
    GraphBuilder::union([undeleted, deleted])
}

pub(super) fn maintained_view_history_storage_field_names(table: &TableSchema) -> Vec<String> {
    let mut fields = vec![
        "row_uuid".to_owned(),
        "tx_time".to_owned(),
        "tx_node_id".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
        "created_by".to_owned(),
        "created_at".to_owned(),
        "updated_by".to_owned(),
        "updated_at".to_owned(),
    ];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| user_column_field(&column.name)),
    );
    fields.push("authored_columns".to_owned());
    fields
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This is an internal planner assertion because the selected physical
    /// access path is not observable through the public query result. A
    /// declared `id` must never be mistaken for the storage row UUID.
    #[test]
    fn declared_id_filter_does_not_select_the_physical_primary_key() {
        let table = TableSchema::new("things", [ColumnSchema::new("id", ColumnType::Uuid)]);
        let declared_id = uuid::Uuid::from_u128(0x99);
        let equalities = BTreeMap::from([("id".to_owned(), Value::Uuid(declared_id))]);

        assert!(select_current_access_path(&table, &equalities).is_none());
    }

    /// A table without a declared `id` retains the legacy physical row-id
    /// primary-key access path.
    #[test]
    fn missing_declared_id_filter_selects_the_physical_primary_key() {
        let table = TableSchema::new("things", [ColumnSchema::new("label", ColumnType::String)]);
        let row_id = uuid::Uuid::from_u128(0x9a);
        let equalities = BTreeMap::from([("id".to_owned(), Value::Uuid(row_id))]);

        assert!(matches!(
            select_current_access_path(&table, &equalities),
            Some(CurrentAccessPath::PrimaryKey(values)) if values == vec![Value::Uuid(row_id)]
        ));
    }

    /// This is an internal planner assertion because the fallback is only
    /// observable as the absence of an optional physical access path. A
    /// system query has no identity claims, so considering a claim predicate
    /// must retain its ordinary scan rather than fail the query.
    #[test]
    fn system_context_claim_declines_an_access_path_without_failing() {
        let binding = ProgramBinding {
            id: BindingId(uuid::Uuid::nil()),
            source_shape: None,
            extra_user_params: BTreeMap::new(),
            param_types: BTreeMap::new(),
            claim_params: BTreeMap::new(),
            values: BTreeMap::new(),
        };

        assert_eq!(
            normalized_bound_value(
                &NormalizedValueRef::Claim(ClaimPath(vec!["sub".to_owned()])),
                &binding,
                &PolicyContext::System,
            )
            .unwrap(),
            None,
        );
    }
}

fn row_author_value(author: AuthorSubject) -> Result<Value, Error> {
    Ok(RowAuthor::from_persisted_subject(author)
        .map_err(|_| Error::UnadmittedWriteAuthor)?
        .to_value())
}

fn append_author_projection_values(
    values: &mut Vec<Value>,
    descriptor: &RecordDescriptor,
    created_by: AuthorSubject,
    updated_by: AuthorSubject,
) -> Result<(), Error> {
    for field in descriptor.fields() {
        let Some(name) = field.name.as_deref() else {
            continue;
        };
        let Some((root, path)) = AuthorSubject::metadata_path(name) else {
            continue;
        };
        let author = if root == "$createdBy" {
            created_by
        } else {
            updated_by
        };
        let mut value = row_author_value(author)?;
        for member in path {
            let Value::Record(record) = value else {
                unreachable!("validated author record")
            };
            value = record.get(member).expect("validated author field");
        }
        values.push(value);
    }
    Ok(())
}
