//! Translation from analyzed plans to executable Groove operators.
//!
//! This module lowers joins, projections, predicates, windows, aggregates,
//! recursive relations, and parameter value sources. Public output-terminal
//! construction remains in [`super::terminals`].

use super::*;

pub(super) fn lower_plan_steps(
    graph: GraphBuilder,
    plan: &AnalyzedQueryPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    match plan {
        AnalyzedQueryPlan::Linear(linear) => {
            lower_linear_plan_steps(graph, linear, root_source, resolved_sources, request)
        }
        AnalyzedQueryPlan::Union(union) => {
            lower_union_plan(union, Some(graph), root_source, resolved_sources, request)
        }
        AnalyzedQueryPlan::CorrelatedPath(path) => {
            lower_correlated_path_plan(graph, path, root_source, resolved_sources, request)
        }
        AnalyzedQueryPlan::RecursiveRelation(relation) => lower_recursive_relation(
            Some(graph),
            relation,
            root_source,
            resolved_sources,
            request,
        ),
    }
}

fn lower_correlated_path_plan(
    graph: GraphBuilder,
    path: &CorrelatedPathPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let parent =
        lower_linear_plan_steps(graph, &path.parent, root_source, resolved_sources, request)?;
    let child_root = path
        .child
        .root
        .source()
        .ok_or_else(|| UnsupportedReason::Operator("path child must be a source".to_owned()))?;
    let child_source = resolved_sources.get(child_root).ok_or_else(|| {
        UnsupportedReason::Runtime(format!(
            "path child source {:?} was not resolved",
            child_root
        ))
    })?;
    let child_relation_steps = path
        .child
        .steps
        .iter()
        // The path graph carries physical parent/child witnesses. Public
        // child projections are applied by the collector after the path, so
        // they must not rename or discard the row/version fields used here.
        .filter(|step| {
            !matches!(
                step,
                LinearStep::OrderBy(_) | LinearStep::Slice { .. } | LinearStep::Project(_)
            )
        })
        .cloned()
        .collect::<Vec<_>>();
    let child_relation_plan = LinearCurrentRoot {
        root: path.child.root.clone(),
        steps: child_relation_steps,
    };
    let child = lower_linear_plan_steps(
        child_source.graph.clone(),
        &child_relation_plan,
        child_source,
        resolved_sources,
        request,
    )?;
    // A correlated child can be maintained once for several provenance
    // routes. Route is therefore part of existence identity whenever both
    // sides carry it; otherwise a qualifying child on one route can keep a
    // different route's parent spuriously present.
    let shared_route_fields = root_source
        .routing_fields
        .intersection(&child.fields)
        .cloned()
        .collect::<BTreeSet<_>>();
    let child_graph = lower_required_nested_parent_graph(
        child.graph,
        &path.nested,
        child_source,
        resolved_sources,
        request,
    )?;
    let (parent_key, child_key) = lower_path_key_pair(
        &path.correlation,
        path.parent.root.source().ok_or_else(|| {
            UnsupportedReason::Operator("path parent must be a source".to_owned())
        })?,
        root_source,
        child_root,
        child_source,
        request,
    )?;
    let parent_key_nullable_depth = source_field_nullable_depth(root_source, &parent_key);
    let child_key_nullable_depth = source_field_nullable_depth(child_source, &child_key);
    let child_graph =
        unwrap_join_key_if_nullable(child_graph, child_key.clone(), child_key_nullable_depth);

    let lowered = match path.requirement {
        CorrelationRequirement::Optional => Ok(LoweredRelationInput {
            graph: parent.graph,
            root_source: Some(root_source.clone()),
            fields: source_fields(root_source).collect(),
            nullable_fields: source_nullable_fields(root_source),
            nullable_field_depths: source_nullable_field_depths(root_source),
            union_occurrence_carrier: None,
        }),
        CorrelationRequirement::AtLeastOne => {
            let parent = unwrap_join_key_if_nullable(
                parent.graph,
                parent_key.clone(),
                parent_key_nullable_depth,
            );
            let (parent_keys, child_keys) =
                correlation_keys_with_routes(parent_key.clone(), child_key, &shared_route_fields);
            Ok(LoweredRelationInput {
                graph: restore_correlation_parent_shape(
                    GraphBuilder::semi_join(parent, child_graph, parent_keys, child_keys),
                    root_source,
                    &parent_key,
                    parent_key_nullable_depth,
                ),
                root_source: Some(root_source.clone()),
                fields: source_fields(root_source).collect(),
                nullable_fields: source_nullable_fields(root_source),
                nullable_field_depths: source_nullable_field_depths(root_source),
                union_occurrence_carrier: None,
            })
        }
        CorrelationRequirement::MatchCorrelationCardinality => {
            let parent = unwrap_join_key_if_nullable(
                parent.graph,
                parent_key.clone(),
                parent_key_nullable_depth,
            );
            lower_cardinality_complete_parent_graph(
                parent,
                child_graph,
                root_source,
                parent_key.clone(),
                child_key,
                &shared_route_fields,
            )
            .map(|graph| LoweredRelationInput {
                graph: restore_correlation_parent_shape(
                    graph,
                    root_source,
                    &parent_key,
                    parent_key_nullable_depth,
                ),
                root_source: Some(root_source.clone()),
                fields: source_fields(root_source).collect(),
                nullable_fields: source_nullable_fields(root_source),
                nullable_field_depths: source_nullable_field_depths(root_source),
                union_occurrence_carrier: None,
            })
        }
    }
    .and_then(|lowered| {
        if path.output_steps.is_empty() {
            Ok(lowered)
        } else {
            let tail = LinearCurrentRoot {
                root: path.parent.root.clone(),
                steps: path.output_steps.clone(),
            };
            lower_linear_plan_steps(lowered.graph, &tail, root_source, resolved_sources, request)
        }
    })?;
    Ok(lowered)
}

/// Apply requirements declared by nested relation builders to the rows of
/// their immediate parent. Optional nested relations never gate that parent;
/// their own descendants are handled when the optional relation is collected.
fn lower_required_nested_parent_graph(
    mut parent: GraphBuilder,
    nested: &[CorrelatedPathPlan],
    parent_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<GraphBuilder, UnsupportedReason> {
    for path in nested {
        if path.requirement == CorrelationRequirement::Optional {
            continue;
        }
        parent =
            lower_correlated_path_plan(parent, path, parent_source, resolved_sources, request)?
                .graph;
    }
    Ok(parent)
}

/// Required-match joins unwrap nullable keys to reject null correlations. Their
/// output still represents the original parent row, including its field identity,
/// routing carriers, and every nullable wrapper expected by collector union arms.
fn restore_correlation_parent_shape(
    mut graph: GraphBuilder,
    source: &ResolvedSource,
    key: &str,
    nullable_depth: usize,
) -> GraphBuilder {
    if nullable_depth == 0 {
        return graph.project_fields(project_source_fields_with_routes(
            source,
            &source.routing_fields,
        ));
    }
    for _ in 0..nullable_depth {
        let mut fields = project_source_fields_with_routes(source, &source.routing_fields);
        for field in &mut fields {
            if field.output_name == key {
                field.expression = groove::ivm::ProjectExpr::Nullable(FieldRef::stored_name(key));
            }
        }
        graph = graph.project_fields(fields);
    }
    graph
}

fn correlation_keys_with_routes(
    parent_key: String,
    child_key: String,
    shared_route_fields: &BTreeSet<String>,
) -> (Vec<String>, Vec<String>) {
    let mut parent_keys = vec![parent_key];
    let mut child_keys = vec![child_key];
    for field in shared_route_fields {
        if !parent_keys
            .iter()
            .zip(&child_keys)
            .any(|(parent, child)| parent == field && child == field)
        {
            parent_keys.push(field.clone());
            child_keys.push(field.clone());
        }
    }
    (parent_keys, child_keys)
}

fn lower_cardinality_complete_parent_graph(
    parent: GraphBuilder,
    child: GraphBuilder,
    root_source: &ResolvedSource,
    parent_key: String,
    child_key: String,
    shared_route_fields: &BTreeSet<String>,
) -> Result<GraphBuilder, UnsupportedReason> {
    let Some(parent_key_type) = source_field_type(root_source, &parent_key) else {
        return Err(UnsupportedReason::Operator(format!(
            "match-correlation-cardinality parent key {parent_key:?} is not projected"
        )));
    };
    let is_array_key = match parent_key_type {
        ValueType::Array(_) => true,
        ValueType::Nullable(inner) => matches!(inner.as_ref(), ValueType::Array(_)),
        _ => false,
    };
    if !is_array_key {
        let (parent_keys, child_keys) =
            correlation_keys_with_routes(parent_key, child_key, shared_route_fields);
        return Ok(
            GraphBuilder::semi_join(parent, child, parent_keys, child_keys).project_fields(
                project_source_fields_with_routes(root_source, &root_source.routing_fields),
            ),
        );
    }

    let required_element_field = "__jazz_required_correlation_element";
    let required = parent
        .clone()
        .unnest(parent_key.clone(), required_element_field);
    let mut covered_fields = project_source_fields_from_prefix(root_source, LEFT_JOIN_PREFIX);
    covered_fields.push(ProjectField::renamed(
        left_field(required_element_field),
        required_element_field,
    ));
    let covered = GraphBuilder::join(
        required.clone(),
        child,
        [required_element_field],
        [child_key],
    )
    .project_fields(covered_fields);
    let missing = GraphBuilder::anti_join(
        required,
        covered,
        [
            root_source.row_shape.row_uuid_field.clone(),
            required_element_field.to_owned(),
        ],
        [
            root_source.row_shape.row_uuid_field.clone(),
            required_element_field.to_owned(),
        ],
    )
    .project_fields(project_source_fields_from_prefix(root_source, ""));
    Ok(GraphBuilder::anti_join(
        parent,
        missing,
        [root_source.row_shape.row_uuid_field.clone()],
        [root_source.row_shape.row_uuid_field.clone()],
    ))
}

pub(super) fn lower_correlated_path_relation_graph(
    path: &CorrelatedPathPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let parent = lower_linear_plan_steps(
        root_source.graph.clone(),
        &path.parent,
        root_source,
        resolved_sources,
        request,
    )?;
    lower_correlated_path_relation_graph_from_parent(
        path,
        parent.graph,
        root_source,
        resolved_sources,
        request,
        true,
    )
}

pub(super) fn lower_correlated_path_relation_graph_from_parent(
    path: &CorrelatedPathPlan,
    parent: GraphBuilder,
    parent_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    retain_child_window: bool,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let child_root = path
        .child
        .root
        .source()
        .ok_or_else(|| UnsupportedReason::Operator("path child must be a source".to_owned()))?;
    let child_source = resolved_sources.get(child_root).ok_or_else(|| {
        UnsupportedReason::Runtime(format!(
            "path child source {:?} was not resolved",
            child_root
        ))
    })?;
    let child_plan = LinearCurrentRoot {
        root: path.child.root.clone(),
        steps: if retain_child_window {
            child_steps_for_relation_edges(&path.child.steps)
        } else {
            path.child
                .steps
                .iter()
                // Correlated paths are a source-level relationship boundary.
                // The collector projects the public child record after that
                // boundary, while relationship edges and nested paths still
                // need the child's physical row/version fields. In
                // particular, a final public projection can rename or omit
                // `row_uuid`, which must not shape the path graph.
                .filter(|step| {
                    !matches!(
                        step,
                        LinearStep::OrderBy(_) | LinearStep::Slice { .. } | LinearStep::Project(_)
                    )
                })
                .cloned()
                .collect()
        },
    };
    let child = lower_linear_plan_steps(
        child_source.graph.clone(),
        &child_plan,
        child_source,
        resolved_sources,
        request,
    )?;
    let child_graph = lower_required_nested_parent_graph(
        child.graph,
        &path.nested,
        child_source,
        resolved_sources,
        request,
    )?;
    let (parent_key, child_key) = lower_path_key_pair(
        &path.correlation,
        path.parent.root.source().ok_or_else(|| {
            UnsupportedReason::Operator("path parent must be a source".to_owned())
        })?,
        parent_source,
        child_root,
        child_source,
        request,
    )?;
    let parent_key_nullable_depth = source_field_nullable_depth(parent_source, &parent_key);
    let child_key_nullable_depth = source_field_nullable_depth(child_source, &child_key);
    let parent = unwrap_join_key_if_nullable(parent, parent_key.clone(), parent_key_nullable_depth);
    let child =
        unwrap_join_key_if_nullable(child_graph, child_key.clone(), child_key_nullable_depth);
    Ok(LoweredRelationInput {
        graph: GraphBuilder::join(parent, child, [parent_key], [child_key]),
        root_source: None,
        fields: BTreeSet::new(),
        nullable_fields: BTreeSet::new(),
        nullable_field_depths: BTreeMap::new(),
        union_occurrence_carrier: None,
    })
}

fn child_steps_for_relation_edges(steps: &[LinearStep]) -> Vec<LinearStep> {
    let mut previous_was_order_by = false;
    let mut filtered = Vec::with_capacity(steps.len());
    for step in steps {
        match step {
            // Relation-edge terminals describe the physical parent/child
            // witnesses, not the child query's rendered row. A final public
            // projection can rename `row_uuid` to `id` and discard the
            // version fields the edge needs; retain the source layout here
            // and let the public collector perform that projection on its
            // own path.
            LinearStep::Project(_) => {
                previous_was_order_by = false;
            }
            LinearStep::Slice { .. } if !previous_was_order_by => {
                previous_was_order_by = false;
            }
            _ => {
                previous_was_order_by = matches!(step, LinearStep::OrderBy(_));
                filtered.push(step.clone());
            }
        }
    }
    filtered
}

fn unwrap_join_key_if_nullable(
    mut graph: GraphBuilder,
    field: String,
    nullable_depth: usize,
) -> GraphBuilder {
    for _ in 0..nullable_depth {
        graph = graph.unwrap_nullable(field.clone());
    }
    graph
}

pub(super) fn unwrap_nullable_join_key(
    graph: GraphBuilder,
    field: String,
    nullable_depth: usize,
) -> GraphBuilder {
    unwrap_join_key_if_nullable(graph, field, nullable_depth)
}

#[derive(Clone, Debug)]
pub(super) struct LoweredRelationInput {
    pub(super) graph: GraphBuilder,
    pub(super) root_source: Option<ResolvedSource>,
    pub(super) fields: BTreeSet<String>,
    pub(super) nullable_fields: BTreeSet<String>,
    pub(super) nullable_field_depths: BTreeMap<String, usize>,
    /// Hidden `(stable union-arm label, source row UUID)` identity retained by
    /// UNION ALL relation inputs for occurrence-addressed public output.
    pub(super) union_occurrence_carrier: Option<(String, String)>,
}

pub(super) fn lower_relation_input(
    plan: &RelationInputPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    lower_relation_input_with_retained_final_fields(plan, resolved_sources, request, false)
}

fn lower_relation_input_with_retained_final_fields(
    plan: &RelationInputPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    retain_final_project_input_fields: bool,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let mut lowered = BTreeMap::new();
    let final_plan_key = relation_input_key(plan);
    for input in relation_input_postorder(plan) {
        let output = lower_relation_input_cached(
            input,
            resolved_sources,
            request,
            &lowered,
            retain_final_project_input_fields && relation_input_key(input) == final_plan_key,
        )?;
        lowered.insert(relation_input_key(input), output);
    }
    lowered
        .remove(&relation_input_key(plan))
        .ok_or_else(|| UnsupportedReason::Runtime("relation input was not lowered".to_owned()))
}

/// Lower a relation as an input to a source-closure contributor terminal.
///
/// A relation facade's final `Project` renders its public output.  Contributor
/// terminals instead freeze the source occurrence that supplied that output,
/// including its row/version witness fields. Retain those physical fields
/// alongside, rather than instead of, the facade projection: membership may
/// deliberately name a public bridge alias such as `bridge_root`.
pub(super) fn lower_relation_input_for_contributor(
    plan: &RelationInputPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    lower_relation_input_with_retained_final_fields(plan, resolved_sources, request, true)
}

/// Relation inputs are nested by policy joins.  Lower them in postorder so a
/// recursive policy graph is compiled iteratively rather than consuming the
/// server shell stack through nested `lower_relation_input` calls.
fn relation_input_postorder(root: &RelationInputPlan) -> Vec<&RelationInputPlan> {
    let mut pending = vec![(root, false)];
    let mut ordered = Vec::new();
    while let Some((plan, visited)) = pending.pop() {
        if visited {
            ordered.push(plan);
            continue;
        }
        pending.push((plan, true));
        match plan {
            RelationInputPlan::Linear(linear) => {
                for step in linear.steps.iter().rev() {
                    if let LinearStep::Join { right, .. } = step {
                        pending.push((right, false));
                    }
                }
            }
            RelationInputPlan::Union(union) => {
                for branch in union.branches.iter().rev() {
                    pending.push((&branch.plan, false));
                }
            }
            RelationInputPlan::Recursive(relation) => {
                for step in relation
                    .step
                    .steps
                    .iter()
                    .rev()
                    .chain(relation.seed.steps.iter().rev())
                {
                    if let LinearStep::Join { right, .. } = step {
                        pending.push((right, false));
                    }
                }
            }
        }
    }
    ordered
}

fn relation_input_key(plan: &RelationInputPlan) -> usize {
    std::ptr::from_ref(plan).addr()
}

fn lower_relation_input_cached(
    plan: &RelationInputPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    lowered: &BTreeMap<usize, LoweredRelationInput>,
    retain_final_project_input_fields: bool,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    match plan {
        RelationInputPlan::Linear(linear) => {
            let source_id = linear.root.source().ok_or_else(|| {
                UnsupportedReason::Operator("linear join input must have a source".to_owned())
            })?;
            let source = resolved_sources.get(source_id).cloned().ok_or_else(|| {
                UnsupportedReason::Runtime(format!("join source {:?} was not resolved", source_id))
            })?;
            lower_linear_plan_steps_cached(
                source.graph.clone(),
                linear,
                &source,
                resolved_sources,
                request,
                Some(lowered),
                Some(linear),
                retain_final_project_input_fields,
            )
        }
        RelationInputPlan::Union(union) => {
            lower_union_relation_input_cached(union, resolved_sources, request, lowered)
        }
        RelationInputPlan::Recursive(relation) => {
            let source_id = relation.root_source().ok_or_else(|| {
                UnsupportedReason::Operator(
                    "recursive join input must include a table source".to_owned(),
                )
            })?;
            let source = resolved_sources.get(source_id).cloned().ok_or_else(|| {
                UnsupportedReason::Runtime(format!(
                    "recursive join source {:?} was not resolved",
                    source_id
                ))
            })?;
            lower_recursive_relation_cached(
                None,
                relation,
                &source,
                resolved_sources,
                request,
                Some(lowered),
            )
        }
    }
}

fn lower_union_relation_input(
    union: &UnionPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    lower_union_relation_input_with_prefix(union, resolved_sources, request, None)
}

fn lower_union_relation_input_cached(
    union: &UnionPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    lowered: &BTreeMap<usize, LoweredRelationInput>,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    lower_union_relation_input_with_prefix_cached(
        union,
        resolved_sources,
        request,
        None,
        Some(lowered),
    )
}

fn lower_union_relation_input_with_prefix(
    union: &UnionPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    prefix: Option<&str>,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    lower_union_relation_input_with_prefix_cached(union, resolved_sources, request, prefix, None)
}

fn lower_union_relation_input_with_prefix_cached(
    union: &UnionPlan,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    prefix: Option<&str>,
    lowered: Option<&BTreeMap<usize, LoweredRelationInput>>,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let mut outputs = Vec::new();
    for (plan, label) in union_branch_plans_with_labels(union, prefix) {
        let linear = match plan {
            RelationInputPlan::Linear(linear) => linear,
            RelationInputPlan::Union(_) => unreachable!("nested unions are flattened above"),
            RelationInputPlan::Recursive(_) => {
                return Err(UnsupportedReason::Operator(
                    "recursive UNION ALL join arms lack a stable finite occurrence carrier"
                        .to_owned(),
                ));
            }
        };
        let source_id = linear.root.source().ok_or_else(|| {
            UnsupportedReason::Operator(
                "UNION ALL join occurrence identity requires a stable source row".to_owned(),
            )
        })?;
        let source = resolved_sources.get(source_id).ok_or_else(|| {
            UnsupportedReason::Runtime(format!("union source {source_id:?} was not resolved"))
        })?;
        let mut retained = linear.clone();
        if let Some(LinearStep::Project(columns)) = retained.steps.last_mut() {
            columns.push(RowProjection {
                output: TypedOutputField {
                    name: "__union_occurrence_row".to_owned(),
                    ty: ColumnType::Uuid,
                },
                value: NormalizedValueRef::RowId(RowIdRef::Source(source_id.clone())),
            });
        }
        let mut output = lower_linear_plan_steps_cached(
            source.graph.clone(),
            &retained,
            source,
            resolved_sources,
            request,
            lowered,
            Some(linear),
            false,
        )?;
        if !output.fields.contains("__union_occurrence_row") {
            let row_field = &source.row_shape.row_uuid_field;
            if !output.fields.contains(row_field) {
                return Err(UnsupportedReason::Operator(
                    "UNION ALL arm projection discarded its stable source row identity".to_owned(),
                ));
            }
            output.graph =
                output
                    .graph
                    .project_fields(output.fields.iter().map(ProjectField::named).chain(
                        std::iter::once(ProjectField::renamed(row_field, "__union_occurrence_row")),
                    ));
            output.fields.insert("__union_occurrence_row".to_owned());
        }
        output.graph =
            output
                .graph
                .project_fields(output.fields.iter().map(ProjectField::named).chain(
                    std::iter::once(ProjectField::literal(
                        "__union_occurrence_arm",
                        Value::String(label),
                    )),
                ));
        output.fields.insert("__union_occurrence_arm".to_owned());
        output.union_occurrence_carrier = Some((
            "__union_occurrence_arm".to_owned(),
            "__union_occurrence_row".to_owned(),
        ));
        outputs.push(output);
    }
    lower_union_inputs(outputs, request)
}

/// Flatten nested UNION ALL inputs without using the call stack.  The complete
/// path label remains the occurrence carrier for each leaf arm.
fn union_branch_plans_with_labels<'a>(
    union: &'a UnionPlan,
    prefix: Option<&str>,
) -> Vec<(&'a RelationInputPlan, String)> {
    let mut leaves = Vec::new();
    let mut pending = union
        .branches
        .iter()
        .rev()
        .map(|branch| {
            let label = prefix.map_or_else(
                || branch.label.clone(),
                |prefix| compose_union_arm_path(prefix, &branch.label),
            );
            (&branch.plan, label)
        })
        .collect::<Vec<_>>();
    while let Some((plan, label)) = pending.pop() {
        match plan {
            RelationInputPlan::Union(nested) => {
                pending.extend(
                    nested.branches.iter().rev().map(|branch| {
                        (&branch.plan, compose_union_arm_path(&label, &branch.label))
                    }),
                )
            }
            RelationInputPlan::Linear(_) | RelationInputPlan::Recursive(_) => {
                leaves.push((plan, label))
            }
        }
    }
    leaves
}

/// Encode a nested semantic-arm path without relying on a separator that a
/// user label could contain. The opaque carrier remains stable when siblings
/// are inserted or reordered and is never derived from traversal position.
fn compose_union_arm_path(prefix: &str, label: &str) -> String {
    format!("{}:{prefix}{}:{label}", prefix.len(), label.len())
}

fn lower_union_plan(
    union: &UnionPlan,
    root_graph: Option<GraphBuilder>,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let mut lowered = Vec::new();
    for (branch_plan, label) in union_branch_plans_with_labels(union, None) {
        let mut input = match branch_plan {
            RelationInputPlan::Linear(linear) => {
                let source_id = linear.root.source().ok_or_else(|| {
                    UnsupportedReason::Operator("union branch must have a source".to_owned())
                })?;
                if source_id != &root_source.row_shape.source {
                    return Err(UnsupportedReason::Operator(
                        "root union branches must share the query result source".to_owned(),
                    ));
                }
                let graph = if let Some(root_graph) = &root_graph {
                    root_graph.clone()
                } else {
                    resolved_sources
                        .get(source_id)
                        .ok_or_else(|| {
                            UnsupportedReason::Runtime(format!(
                                "union branch source {:?} was not resolved",
                                source_id
                            ))
                        })?
                        .graph
                        .clone()
                };
                lower_linear_plan_steps(graph, linear, root_source, resolved_sources, request)?
            }
            RelationInputPlan::Union(_) | RelationInputPlan::Recursive(_) => {
                lower_relation_input(branch_plan, resolved_sources, request)?
            }
        };
        // A public root UNION ALL has no join-side occurrence slot. Retain an
        // explicit typed `(arm, source-row)` carrier beside the real root so
        // the terminal can address two derivations of one physical row.
        let source_id = branch_plan.root_source().ok_or_else(|| {
            UnsupportedReason::Operator("UNION ALL root arm lacks a stable source row".to_owned())
        })?;
        let source = resolved_sources.get(source_id).ok_or_else(|| {
            UnsupportedReason::Runtime(format!("union source {source_id:?} was not resolved"))
        })?;
        let row_field = &source.row_shape.row_uuid_field;
        if !input.fields.contains(row_field) {
            return Err(UnsupportedReason::Operator(
                "UNION ALL root arm projection discarded its stable source row identity".to_owned(),
            ));
        }
        input.graph =
            input
                .graph
                .project_fields(input.fields.iter().map(ProjectField::named).chain([
                    ProjectField::renamed(row_field, "__root_union_row".to_owned()),
                    ProjectField::literal("__root_union_arm", Value::String(label)),
                ]));
        input.fields.insert("__root_union_row".to_owned());
        input.fields.insert("__root_union_arm".to_owned());
        input.union_occurrence_carrier =
            Some(("__root_union_arm".to_owned(), "__root_union_row".to_owned()));
        lowered.push(input);
    }
    let lowered = lower_union_inputs(lowered, request)?;
    lower_union_terminal_steps(lowered, &union.terminal_steps, root_source, request)
}

fn lower_union_terminal_steps(
    mut input: LoweredRelationInput,
    steps: &[LinearStep],
    root_source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    if steps.is_empty() {
        return Ok(input);
    }
    let root = input.root_source.as_ref().unwrap_or(root_source);
    let plan = LinearCurrentRoot {
        root: LinearRoot::Source {
            source: root.row_shape.source.clone(),
            visibility: RowVisibility::Visible,
        },
        steps: Vec::new(),
    };
    let mut pending_order = None;
    for step in steps {
        match step {
            LinearStep::OrderBy(keys) => pending_order = Some(keys.clone()),
            LinearStep::Slice {
                partition_by,
                limit,
                offset,
                tie_breaker,
                ..
            } => {
                let order = pending_order.take().unwrap_or_default();
                input.graph = lower_window(
                    input.graph,
                    &order,
                    partition_by,
                    &root.routing_fields,
                    *limit,
                    *offset,
                    tie_breaker,
                    &plan,
                    root,
                    request,
                )?;
            }
            LinearStep::Filter(_)
            | LinearStep::Join { .. }
            | LinearStep::Project(_)
            | LinearStep::Aggregate { .. } => {
                return Err(UnsupportedReason::Operator(
                    "only global order/window operators may wrap a public UNION ALL".to_owned(),
                ));
            }
        }
    }
    if let Some(order) = pending_order {
        input.graph = lower_window(
            input.graph,
            &order,
            &[],
            &root.routing_fields,
            None,
            0,
            &[NormalizedValueRef::RowId(RowIdRef::Source(
                root.row_shape.source.clone(),
            ))],
            &plan,
            root,
            request,
        )?;
    }
    Ok(input)
}

fn lower_union_inputs(
    lowered: Vec<LoweredRelationInput>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let union_fields = lowered_union_fields(&lowered);
    let needs_alignment = lowered.iter().any(|branch| branch.fields != union_fields);
    let lowered = if needs_alignment {
        lowered
            .into_iter()
            .map(|branch| align_union_route_fields(branch, &union_fields, request))
            .collect::<Result<Vec<_>, _>>()?
    } else {
        lowered
    };
    let mut lowered = lowered.into_iter();
    let first = lowered.next().ok_or_else(|| {
        UnsupportedReason::Operator("union row-set nodes require at least one input".to_owned())
    })?;
    let union_occurrence_carrier = first.union_occurrence_carrier.clone();
    let mut graphs = vec![first.graph];
    let mut root_source = first.root_source;
    let fields = first.fields;
    let mut nullable_fields = first.nullable_fields;
    let mut nullable_field_depths = first.nullable_field_depths;
    for branch in lowered {
        if branch.fields != fields {
            return Err(UnsupportedReason::Operator(
                "union branches must output the same fields".to_owned(),
            ));
        }
        if branch.union_occurrence_carrier != union_occurrence_carrier {
            return Err(UnsupportedReason::Operator(
                "union branches must use the same typed occurrence carrier".to_owned(),
            ));
        }
        nullable_fields.extend(branch.nullable_fields);
        for (field, depth) in branch.nullable_field_depths {
            nullable_field_depths
                .entry(field)
                .and_modify(|existing| *existing = (*existing).max(depth))
                .or_insert(depth);
        }
        if root_source.as_ref().map(|source| &source.row_shape.source)
            != branch
                .root_source
                .as_ref()
                .map(|source| &source.row_shape.source)
        {
            root_source = None;
        }
        graphs.push(branch.graph);
    }
    Ok(LoweredRelationInput {
        graph: GraphBuilder::union(graphs),
        root_source,
        fields,
        nullable_fields,
        nullable_field_depths,
        union_occurrence_carrier,
    })
}

fn lowered_union_fields(lowered: &[LoweredRelationInput]) -> BTreeSet<String> {
    lowered
        .iter()
        .flat_map(|branch| branch.fields.iter().cloned())
        .collect()
}

fn align_union_route_fields(
    mut branch: LoweredRelationInput,
    fields: &BTreeSet<String>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let route_fields = parameter_domain_for_request(request)?.routing_params;
    let missing = fields
        .difference(&branch.fields)
        .cloned()
        .collect::<BTreeSet<_>>();
    if missing.iter().any(|field| !route_fields.contains(field)) {
        return Err(UnsupportedReason::Operator(
            "union branches must output the same fields".to_owned(),
        ));
    }

    if let Some(binding_source_shape) = &request.input.binding.source_shape {
        let binding = GraphBuilder::binding_source(
            binding_source_shape.clone(),
            binding_source_descriptor_with_user_params(request, [])?,
        );
        let project_fields = fields
            .iter()
            .map(|field| {
                if branch.fields.contains(field) {
                    ProjectField::renamed(left_field(field), field.clone())
                } else {
                    let binding_field = route_param_from_field(field).unwrap_or(field);
                    ProjectField::renamed(right_field(binding_field), field.clone())
                }
            })
            .collect::<Vec<_>>();
        let existing_route_fields = branch
            .fields
            .intersection(&route_fields)
            .cloned()
            .collect::<Vec<_>>();
        let binding_route_fields = existing_route_fields
            .iter()
            .map(|field| route_param_from_field(field).unwrap_or(field).to_owned())
            .collect::<Vec<_>>();
        branch.graph = policy_join_if_needed(
            branch.graph,
            binding,
            existing_route_fields,
            binding_route_fields,
            request,
        )
        .project_fields(project_fields);
    } else {
        let project_fields = fields
            .iter()
            .map(|field| {
                if branch.fields.contains(field) {
                    Ok(ProjectField::named(field.clone()))
                } else {
                    route_literal_project_field(field, request)
                }
            })
            .collect::<Result<Vec<_>, UnsupportedReason>>()?;
        branch.graph = branch.graph.project_fields(project_fields);
    }
    branch.fields = fields.clone();
    Ok(branch)
}

fn linear_root_fields(root: &LinearRoot) -> BTreeSet<String> {
    match root {
        LinearRoot::Source { .. } => BTreeSet::new(),
        LinearRoot::Value { columns, .. } | LinearRoot::Frontier { columns, .. } => {
            columns.iter().map(|column| column.name.clone()).collect()
        }
    }
}

fn source_fields(source: &ResolvedSource) -> impl Iterator<Item = String> + '_ {
    source
        .row_shape
        .descriptor
        .fields()
        .iter()
        .filter_map(|field| field.name.clone())
        .chain(source.routing_fields.iter().cloned())
}

fn source_nullable_fields(source: &ResolvedSource) -> BTreeSet<String> {
    source_nullable_field_depths(source).into_keys().collect()
}

fn source_nullable_field_depths(source: &ResolvedSource) -> BTreeMap<String, usize> {
    let mut depths = BTreeMap::new();
    for name in source_fields(source) {
        if let Some((field, depth)) = {
            let depth = source_field_nullable_depth(source, &name);
            (depth > 0).then_some((name, depth))
        } {
            if let Some(logical) = field.strip_prefix(USER_COLUMN_PREFIX) {
                depths.insert(logical.to_owned(), depth);
            }
            depths.insert(field, depth);
        }
    }
    depths
}

fn lower_recursive_relation(
    root_graph: Option<GraphBuilder>,
    relation: &RecursiveRelationPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    lower_recursive_relation_cached(
        root_graph,
        relation,
        root_source,
        resolved_sources,
        request,
        None,
    )
}

pub(super) fn lower_recursive_relation_cached(
    root_graph: Option<GraphBuilder>,
    relation: &RecursiveRelationPlan,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    lowered: Option<&BTreeMap<usize, LoweredRelationInput>>,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    // A recursive relation is ordinarily reached through relation-input
    // postorder and receives its lowered join children. The recursion-owned
    // witness is also lowered directly by closure terminals; build the same
    // immediate child cache there instead of taking an alternate lowering
    // path.
    let mut owned_children = lowered.cloned().unwrap_or_default();
    for linear in [&relation.seed, &relation.step] {
        for step in &linear.steps {
            if let LinearStep::Join { right, .. } = step {
                owned_children
                    .entry(relation_input_key(right))
                    .or_insert(lower_relation_input(right, resolved_sources, request)?);
            }
        }
    }
    let lowered = Some(&owned_children);
    let seed_root_source = relation
        .seed_source()
        .and_then(|source| resolved_sources.get(source));
    let seed_root = seed_root_source.map(|resolved| resolved.graph.clone());
    let seed_graph = seed_root
        .or(root_graph)
        .unwrap_or_else(|| root_source.graph.clone());
    let seed = lower_linear_plan_steps_cached(
        seed_graph,
        &relation.seed,
        seed_root_source.unwrap_or(root_source),
        resolved_sources,
        request,
        lowered,
        None,
        false,
    )?;
    let step_source_id = relation.step_source().ok_or_else(|| {
        UnsupportedReason::Operator("recursive step must include a table source".to_owned())
    })?;
    let step_source = resolved_sources.get(step_source_id).ok_or_else(|| {
        UnsupportedReason::Runtime(format!(
            "recursive step source {:?} was not resolved",
            step_source_id
        ))
    })?;
    let step = lower_linear_plan_steps_cached(
        step_source.graph.clone(),
        &relation.step,
        step_source,
        resolved_sources,
        request,
        lowered,
        None,
        false,
    )?;
    // Preserve a generic recursion-owned view of the physical step rows
    // before the normalized recursive projection discards them. The runtime
    // executes this side graph with the exact same frontier/depth as `step`;
    // it is not a second reachability traversal.
    let step_witness = {
        let mut witness_step_plan = relation.step.clone();
        let Some(LinearStep::Project(_)) = witness_step_plan.steps.last() else {
            return Err(UnsupportedReason::Operator(
                "recursive step witness requires a final projection".to_owned(),
            ));
        };
        witness_step_plan.steps.pop();
        // The side output is still authority-local until its terminal is routed
        // to an admitted session. Preserve the same route carriers as the main
        // step; the wire encoder, not this graph, removes private claim fields.
        let witness_step_source = step_source;
        let mut witness_children = BTreeMap::new();
        for step in &witness_step_plan.steps {
            if let LinearStep::Join { right, .. } = step {
                witness_children.insert(
                    relation_input_key(right),
                    lower_relation_input(right, resolved_sources, request)?,
                );
            }
        }
        lower_linear_plan_steps_cached(
            witness_step_source.graph.clone(),
            &witness_step_plan,
            witness_step_source,
            resolved_sources,
            request,
            Some(&witness_children),
            Some(&witness_step_plan),
            false,
        )?
    };
    let truncate_at_max_iters = matches!(relation.bound, RecursionBound::MaxDepth(_));
    let max_iters = match relation.bound {
        RecursionBound::Fixpoint => FIXPOINT_MAX_ITERS,
        RecursionBound::MaxDepth(max_depth) => max_depth,
    };
    if seed.fields != step.fields {
        return Err(UnsupportedReason::Operator(
            "recursive seed and step outputs must have the same fields".to_owned(),
        ));
    }
    let fields = seed.fields.clone();
    Ok(LoweredRelationInput {
        graph: GraphBuilder::recursive_with_step_witness(
            seed.graph,
            step.graph,
            step_witness.graph,
            relation.frontier.0.clone(),
            max_iters,
            truncate_at_max_iters,
        ),
        root_source: Some(root_source.clone()),
        fields,
        nullable_fields: BTreeSet::new(),
        nullable_field_depths: BTreeMap::new(),
        union_occurrence_carrier: None,
    })
}

pub(super) fn lower_linear_plan_steps(
    graph: GraphBuilder,
    plan: &LinearCurrentRoot,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    lower_linear_plan_steps_cached(
        graph,
        plan,
        root_source,
        resolved_sources,
        request,
        None,
        None,
        false,
    )
}

/// Lower a recursive seed for a covered receiver input. The seed's rendered
/// relation establishes the initial frontier, while the retained source fields
/// encode the exact admitted source occurrence for the receiver closure.
pub(super) fn lower_recursive_seed_membership(
    relation: &RecursiveRelationPlan,
    seed_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    lower_linear_plan_steps_cached(
        seed_source.graph.clone(),
        &relation.seed,
        seed_source,
        resolved_sources,
        request,
        None,
        None,
        true,
    )
}

fn lower_linear_plan_steps_cached(
    graph: GraphBuilder,
    plan: &LinearCurrentRoot,
    root_source: &ResolvedSource,
    resolved_sources: &BTreeMap<SourceId, ResolvedSource>,
    request: &QueryProgramRequest,
    lowered: Option<&BTreeMap<usize, LoweredRelationInput>>,
    cache_plan: Option<&LinearCurrentRoot>,
    retain_final_project_input_fields: bool,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let mut graph = match &plan.root {
        LinearRoot::Source { .. } => graph,
        LinearRoot::Value {
            shape,
            columns,
            mode,
        } => lower_value_source(shape, columns, mode, request)?,
        LinearRoot::Frontier { frontier, columns } => {
            GraphBuilder::frontier_source(frontier.0.clone(), value_source_descriptor(columns))
        }
    };
    let mut fields: BTreeSet<String> = match &plan.root {
        LinearRoot::Source { .. } => source_fields(root_source).collect(),
        LinearRoot::Value { columns, .. } | LinearRoot::Frontier { columns, .. } => {
            columns.iter().map(|column| column.name.clone()).collect()
        }
    };
    let mut nullable_fields = if matches!(plan.root, LinearRoot::Source { .. }) {
        source_nullable_fields(root_source)
    } else {
        BTreeSet::new()
    };
    let mut nullable_field_depths = if matches!(plan.root, LinearRoot::Source { .. }) {
        source_nullable_field_depths(root_source)
    } else {
        BTreeMap::new()
    };
    let mut pending_order: Option<Vec<OrderKey>> = None;
    let mut last_join_right: Option<(
        RelationInputPlan,
        BTreeSet<String>,
        BTreeMap<String, usize>,
        BTreeSet<String>,
    )> = None;
    // Earlier inner-join inputs are flattened into the left record before a
    // subsequent join. Keep their source-qualified field addresses so the
    // final flat-join projection can still name every contributing source.
    let mut accumulated_join_fields = BTreeMap::<(SourceId, String), (String, usize)>::new();
    // UNION inputs have an occurrence identity which is not source-qualified:
    // it is the complete `(arm, row)` pair. Keep its public terminal names
    // while flattening a consecutive inner join so the final projection can
    // retain the pair for result-membership.
    let mut accumulated_union_occurrence_fields = BTreeSet::<String>::new();
    let mut available_route_fields = if matches!(plan.root, LinearRoot::Source { .. }) {
        root_source.routing_fields.clone()
    } else {
        BTreeSet::new()
    };
    let route_fields = parameter_domain_for_request(request)?.routing_params;

    for (step_index, step) in plan.steps.iter().enumerate() {
        match step {
            LinearStep::Filter(predicate) => {
                last_join_right = None;
                let source = plan.root.source().ok_or_else(|| {
                    UnsupportedReason::Operator(
                        "filters on value/frontier sources are not lowered yet".to_owned(),
                    )
                })?;
                let (joined, residual, introduced_route_fields) =
                    lower_equality_param_filter_joins(
                        graph,
                        predicate,
                        source,
                        root_source,
                        &available_route_fields,
                        request,
                    )?;
                graph = joined;
                fields.extend(introduced_route_fields.iter().cloned());
                available_route_fields.extend(introduced_route_fields);
                if !matches!(residual, PredicateExpr::True) {
                    let predicate = lower_predicate(&residual, source, root_source, request)?;
                    graph = if uses_policy_value_comparison(request) {
                        graph.policy_filter(predicate)
                    } else {
                        graph.filter(predicate)
                    };
                }
            }
            LinearStep::Join { right, mode, on } => {
                if !matches!(mode, JoinMode::Inner | JoinMode::Semi) {
                    return Err(UnsupportedReason::Operator(
                        "join_via only lowers inner/semi joins".to_owned(),
                    ));
                }
                let lowered_right = match lowered {
                    Some(lowered) => lowered
                        .get(&relation_input_key(
                            cache_plan
                                .and_then(|plan| plan.steps.get(step_index))
                                .and_then(|step| match step {
                                    LinearStep::Join { right, .. } => Some(right.as_ref()),
                                    _ => None,
                                })
                                .unwrap_or(right),
                        ))
                        .cloned()
                        .ok_or_else(|| {
                            UnsupportedReason::Runtime(format!(
                                "join relation input was not lowered at step {step_index}: right={right:#?}"
                            ))
                        })?,
                    None => lower_relation_input(right, resolved_sources, request)?,
                };
                let (left_keys, right_keys) = lower_linear_join_key_pairs(
                    on,
                    &plan.root,
                    root_source,
                    right,
                    &lowered_right,
                    &accumulated_join_fields,
                    request,
                )?;
                let mut unwrapped_left_keys = BTreeSet::new();
                for left_key in &left_keys {
                    if nullable_fields.contains(left_key)
                        && unwrapped_left_keys.insert(left_key.clone())
                    {
                        graph = unwrap_nullable_join_key(
                            graph,
                            left_key.clone(),
                            nullable_field_depths.get(left_key).copied().unwrap_or(1),
                        );
                    }
                }
                for key in &unwrapped_left_keys {
                    nullable_fields.remove(key);
                    nullable_field_depths.remove(key);
                }
                let mut right_nullable_fields = lowered_right.nullable_fields.clone();
                let mut right_nullable_field_depths = lowered_right.nullable_field_depths.clone();
                let mut right_graph = lowered_right.graph;
                let mut unwrapped_right_keys = BTreeSet::new();
                for right_key in &right_keys {
                    if lowered_right.nullable_fields.contains(right_key)
                        && unwrapped_right_keys.insert(right_key.clone())
                    {
                        right_graph = unwrap_nullable_join_key(
                            right_graph,
                            right_key.clone(),
                            lowered_right
                                .nullable_field_depths
                                .get(right_key)
                                .copied()
                                .unwrap_or(1),
                        );
                    }
                }
                for key in &unwrapped_right_keys {
                    right_nullable_fields.remove(key);
                    right_nullable_field_depths.remove(key);
                }
                if *mode == JoinMode::Semi {
                    // Route each left occurrence before applying the existence
                    // gate. A Groove semi-join preserves left multiplicity and
                    // collapses every matching right derivation, while carrying
                    // the route on the left keeps prepared identities distinct.
                    let right_route_fields = route_fields
                        .iter()
                        .filter(|field| lowered_right.fields.contains(*field))
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    let missing_route_fields = right_route_fields
                        .difference(&available_route_fields)
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    if !missing_route_fields.is_empty() {
                        if let Some(binding_source_shape) = &request.input.binding.source_shape {
                            let binding = GraphBuilder::binding_source(
                                binding_source_shape.clone(),
                                binding_source_descriptor_with_user_params(request, [])?,
                            );
                            let existing_route_fields = available_route_fields
                                .intersection(&right_route_fields)
                                .cloned()
                                .collect::<Vec<_>>();
                            let binding_route_fields = existing_route_fields
                                .iter()
                                .map(|field| {
                                    route_param_from_field(field).unwrap_or(field).to_owned()
                                })
                                .collect::<Vec<_>>();
                            let mut projection = fields
                                .iter()
                                .map(|field| {
                                    ProjectField::renamed(left_field(field), field.clone())
                                })
                                .collect::<Vec<_>>();
                            projection.extend(missing_route_fields.iter().map(|field| {
                                let binding_field = route_param_from_field(field).unwrap_or(field);
                                ProjectField::renamed(right_field(binding_field), field.clone())
                            }));
                            graph = policy_join_if_needed(
                                graph,
                                binding,
                                existing_route_fields,
                                binding_route_fields,
                                request,
                            )
                            .project_fields(projection);
                        } else {
                            let mut projection = fields
                                .iter()
                                .map(|field| ProjectField::named(field.clone()))
                                .collect::<Vec<_>>();
                            projection.extend(
                                missing_route_fields
                                    .iter()
                                    .map(|field| route_literal_project_field(field, request))
                                    .collect::<Result<Vec<_>, _>>()?,
                            );
                            graph = graph.project_fields(projection);
                        }
                        fields.extend(missing_route_fields.iter().cloned());
                        available_route_fields.extend(missing_route_fields);
                    }

                    let mut right_fields = right_keys.clone();
                    let mut semi_left_keys = left_keys;
                    let mut semi_right_keys = right_keys;
                    for field in right_route_fields {
                        if !semi_right_keys.contains(&field) {
                            right_fields.push(field.clone());
                            semi_left_keys.push(field.clone());
                            semi_right_keys.push(field);
                        }
                    }
                    let right = right_graph.project_fields(
                        right_fields
                            .into_iter()
                            .map(ProjectField::named)
                            .collect::<Vec<_>>(),
                    );
                    graph =
                        semi_join_if_needed(graph, right, semi_left_keys, semi_right_keys, request);
                    last_join_right = None;
                } else {
                    graph =
                        policy_join_if_needed(graph, right_graph, left_keys, right_keys, request);
                    let right_fields = lowered_right.fields.clone();
                    last_join_right = Some((
                        (**right).clone(),
                        right_nullable_fields,
                        right_nullable_field_depths,
                        right_fields,
                    ));
                }
                let next_is_project =
                    matches!(plan.steps.get(step_index + 1), Some(LinearStep::Project(_)));
                let next_is_join = matches!(
                    plan.steps.get(step_index + 1),
                    Some(LinearStep::Join { .. })
                );
                if *mode == JoinMode::Inner && next_is_join {
                    // Consecutive joins must retain the right input for the
                    // next predicate. Public queries also use those values as
                    // occurrence carriers; authorization subplans keep the
                    // same single lowering path, but mark them internal so a
                    // decision terminal never asks for public row identity.
                    let policy_subplan = omits_public_occurrence_carriers(request);
                    let (_, right_nullable, right_depths, right_fields) = last_join_right
                        .take()
                        .expect("inner join records its right input");
                    let right_source = right.root_source().ok_or_else(|| {
                        UnsupportedReason::Operator(
                            "multi-source flat join requires a source-rooted right input"
                                .to_owned(),
                        )
                    })?;
                    let mut union_occurrence_outputs = BTreeMap::new();
                    if !policy_subplan && matches!(right.as_ref(), RelationInputPlan::Union(_)) {
                        if let Some((arm_field, row_field)) =
                            &lowered_right.union_occurrence_carrier
                        {
                            union_occurrence_outputs
                                .insert(arm_field.clone(), format!("__root_join_arm_{step_index}"));
                            union_occurrence_outputs
                                .insert(row_field.clone(), format!("__root_join_row_{step_index}"));
                        }
                    }
                    let mut projection = fields
                        .iter()
                        .map(|field| ProjectField::renamed(left_field(field), field.clone()))
                        .collect::<Vec<_>>();
                    let mut next_fields = fields.clone();
                    let mut next_nullable = nullable_fields.clone();
                    let mut next_depths = nullable_field_depths.clone();
                    for field in right_fields {
                        let output = union_occurrence_outputs
                            .get(&field)
                            .cloned()
                            .unwrap_or_else(|| {
                                if policy_subplan {
                                    format!("__policy_join_source_{step_index}_{field}")
                                } else {
                                    format!("__flat_join_source_{step_index}_{field}")
                                }
                            });
                        projection.push(ProjectField::renamed(right_field(&field), output.clone()));
                        if union_occurrence_outputs.contains_key(&field) {
                            accumulated_union_occurrence_fields.insert(output.clone());
                        }
                        let nullable_depth = right_depths.get(&field).copied().unwrap_or(0);
                        accumulated_join_fields.insert(
                            (right_source.clone(), field.clone()),
                            (output.clone(), nullable_depth),
                        );
                        next_fields.insert(output.clone());
                        if right_nullable.contains(&field) {
                            next_nullable.insert(output.clone());
                            next_depths.insert(output, nullable_depth.max(1));
                        }
                    }
                    graph = graph.project_fields(projection);
                    fields = next_fields;
                    nullable_fields = next_nullable;
                    nullable_field_depths = next_depths;
                }
                if matches!(mode, JoinMode::Inner | JoinMode::Semi)
                    && matches!(&plan.root, LinearRoot::Source { .. })
                    && !next_is_project
                    && !next_is_join
                {
                    let introduced_route_fields = route_fields
                        .iter()
                        .filter(|field| lowered_right.fields.contains(*field))
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    let retained_route_fields = available_route_fields
                        .union(&introduced_route_fields)
                        .cloned()
                        .collect::<BTreeSet<_>>();
                    let mut projection = if *mode == JoinMode::Semi {
                        project_source_fields_with_routes(root_source, &retained_route_fields)
                    } else {
                        project_left_source_fields_with_join_routes(
                            root_source,
                            &available_route_fields,
                            &introduced_route_fields,
                        )
                    };
                    let retained_left_field = |field: &str| {
                        if *mode == JoinMode::Semi {
                            field.to_owned()
                        } else {
                            left_field(field)
                        }
                    };
                    let mut occurrence_fields = BTreeSet::new();
                    if !omits_public_occurrence_carriers(request) {
                        // Earlier consecutive UNION joins already flattened
                        // their complete occurrence pair into the left input.
                        // Retain both fields under their terminal names; row
                        // identity without its union arm is ambiguous.
                        for output in &accumulated_union_occurrence_fields {
                            projection.push(ProjectField::renamed(
                                retained_left_field(output),
                                output.clone(),
                            ));
                            occurrence_fields.insert(output.clone());
                        }
                        // A trailing semi-join filters the complete public
                        // tuple but contributes no occurrence of its own.
                        // Preserve the earlier inner-join row IDs from the
                        // left input: terminals still use them to distinguish
                        // public root occurrences.
                        for ((source_id, field), (output, _)) in &accumulated_join_fields {
                            let is_row_id = resolved_sources
                                .get(source_id)
                                .is_some_and(|source| source.row_shape.row_uuid_field == *field);
                            if is_row_id {
                                projection.push(ProjectField::renamed(
                                    retained_left_field(output),
                                    output.clone(),
                                ));
                                occurrence_fields.insert(output.clone());
                            }
                        }
                    }
                    if *mode == JoinMode::Inner && !omits_public_occurrence_carriers(request) {
                        // A union's root source is one of its arms, but its
                        // public occurrence is the `(arm, row)` pair. Check
                        // the relation shape before inspecting that source so
                        // an aliased common arm cannot make us drop the union
                        // carrier that result-membership terminals require.
                        if matches!(right.as_ref(), RelationInputPlan::Union(_)) {
                            if let Some((arm_field, row_field)) =
                                &lowered_right.union_occurrence_carrier
                            {
                                let arm_output = format!("__root_join_arm_{step_index}");
                                let row_output = format!("__root_join_row_{step_index}");
                                projection.push(ProjectField::renamed(
                                    right_field(arm_field),
                                    arm_output.clone(),
                                ));
                                projection.push(ProjectField::renamed(
                                    right_field(row_field),
                                    row_output.clone(),
                                ));
                                occurrence_fields.insert(arm_output);
                                occurrence_fields.insert(row_output);
                            }
                        } else if let Some(right_source_id) = right.root_source() {
                            let right_source = resolved_sources.get(right_source_id).ok_or_else(|| {
                                UnsupportedReason::Operator(format!(
                                    "inner join occurrence source {right_source_id:?} was not resolved"
                                ))
                            })?;
                            let app_join_source = matches!(
                                right_source_id.path.components.last(),
                                Some(SourceRole::Alias(_))
                            );
                            let right_row = &right_source.row_shape.row_uuid_field;
                            if app_join_source
                                && last_join_right
                                    .as_ref()
                                    .is_some_and(|(_, _, _, fields)| fields.contains(right_row))
                            {
                                let output = format!("__root_join_row_{step_index}");
                                projection.push(ProjectField::renamed(
                                    right_field(right_row),
                                    output.clone(),
                                ));
                                occurrence_fields.insert(output);
                            }
                        }
                    }
                    graph = graph.project_fields(projection);
                    fields = source_fields(root_source).collect();
                    fields.extend(available_route_fields.iter().cloned());
                    fields.extend(introduced_route_fields.iter().cloned());
                    fields.extend(occurrence_fields);
                    nullable_fields = source_nullable_fields(root_source);
                    nullable_field_depths = source_nullable_field_depths(root_source);
                    available_route_fields.extend(introduced_route_fields);
                    last_join_right = None;
                }
            }
            LinearStep::Project(columns) => {
                // A contributor freezes the same rendered relation that
                // membership predicates name, but it must also retain the
                // pre-projection physical row/version fields needed to encode
                // the admitted source occurrence. Capture the exact fields
                // available at this boundary before the facade aliases them.
                let retained_contributor_fields = (retain_final_project_input_fields
                    && step_index + 1 == plan.steps.len())
                .then(|| fields.clone());
                let retained_contributor_nullable_depths = retained_contributor_fields
                    .as_ref()
                    .map(|_| nullable_field_depths.clone());
                let mut unwrap_fields = BTreeMap::<String, usize>::new();
                let mut projected_nullable_field_depths = BTreeMap::<String, usize>::new();
                let mut wrap_after_project = BTreeMap::<String, usize>::new();
                let field_plans = columns
                    .iter()
                    .map(|column| {
                        lower_projection_field(
                            column,
                            plan,
                            root_source,
                            &fields,
                            &nullable_field_depths,
                            last_join_right.as_ref(),
                            &accumulated_join_fields,
                            request,
                        )
                    })
                    .collect::<Result<Vec<_>, UnsupportedReason>>()?;
                for field in &field_plans {
                    for (source, depth) in &field.unwrap_before_project {
                        unwrap_fields
                            .entry(source.clone())
                            .and_modify(|existing| *existing = (*existing).max(*depth))
                            .or_insert(*depth);
                    }
                }
                let project_fields = field_plans
                    .into_iter()
                    .map(|mut field| {
                        let output_depth = field
                            .nullable_after_project
                            .as_ref()
                            .map_or(0, |(_, depth)| *depth);
                        if let Some((source, source_depth)) = &field.source_nullability {
                            // Every alias sees the same transformed input. Plan its
                            // wrappers only after all aliases agree on source unwraps.
                            let remaining = source_depth
                                .saturating_sub(unwrap_fields.get(source).copied().unwrap_or(0));
                            let wrappers = output_depth.saturating_sub(remaining);
                            if wrappers > 0 {
                                field.project =
                                    ProjectField::nullable(source, &field.project.output_name);
                                if wrappers > 1 {
                                    wrap_after_project
                                        .insert(field.project.output_name.clone(), wrappers - 1);
                                }
                            }
                        }
                        if let Some((output, depth)) = field.nullable_after_project {
                            projected_nullable_field_depths.insert(output, depth);
                        }
                        field.project
                    })
                    .collect::<Vec<_>>();
                for (field, depth) in &unwrap_fields {
                    for _ in 0..*depth {
                        graph = graph.unwrap_nullable(field.clone());
                    }
                }
                let mut project_fields = project_fields;
                let mut retained_route_fields = BTreeSet::new();
                let mut projected_outputs = project_fields
                    .iter()
                    .map(|field| field.output_name.clone())
                    .collect::<BTreeSet<_>>();
                match last_join_right.as_ref() {
                    Some((_, _, _, right_fields)) => {
                        for field in &available_route_fields {
                            if !projected_outputs.contains(field) {
                                project_fields
                                    .push(ProjectField::renamed(left_field(field), field.clone()));
                            }
                            retained_route_fields.insert(field.clone());
                        }
                        for field in route_fields
                            .iter()
                            .filter(|field| right_fields.contains(*field))
                        {
                            if !projected_outputs.contains(field) {
                                project_fields
                                    .push(ProjectField::renamed(right_field(field), field.clone()));
                            }
                            retained_route_fields.insert(field.clone());
                        }
                    }
                    None => {
                        for field in &available_route_fields {
                            if !projected_outputs.contains(field) {
                                project_fields.push(ProjectField::named(field.clone()));
                            }
                            retained_route_fields.insert(field.clone());
                        }
                    }
                }
                if let Some(retained_fields) = &retained_contributor_fields {
                    // A final relation projection follows a joined graph, so
                    // every retained root descriptor field—including magic
                    // provenance and version metadata—still lives on the
                    // left side. Keeping user fields qualified but appending
                    // metadata unqualified made nested covered-input joins
                    // fail while preparing their receiver-local terminal.
                    let retained_prefix = last_join_right
                        .as_ref()
                        .map(|_| LEFT_JOIN_PREFIX)
                        .unwrap_or("");
                    for field in retained_fields {
                        if projected_outputs.insert(field.clone()) {
                            let source_field = format!("{retained_prefix}{field}");
                            let declared_depth = retained_contributor_nullable_depths
                                .as_ref()
                                .and_then(|depths| depths.get(field))
                                .copied()
                                .unwrap_or(0);
                            let removed_layers = unwrap_fields
                                .get(&source_field)
                                .copied()
                                .unwrap_or(0)
                                .min(declared_depth);
                            // Contributor carriers promise the pre-projection
                            // source layout, independently of public aliases.
                            // Restore wrappers removed by shared source unwraps.
                            if removed_layers > 0 {
                                project_fields.push(ProjectField::nullable(source_field, field));
                                if removed_layers > 1 {
                                    wrap_after_project.insert(field.clone(), removed_layers - 1);
                                }
                            } else {
                                project_fields.push(ProjectField::renamed(source_field, field));
                            }
                        }
                    }
                }
                graph = graph.project_fields(project_fields);
                fields = columns
                    .iter()
                    .map(|column| column.output.name.clone())
                    .collect();
                fields.extend(retained_route_fields.iter().cloned());
                if let Some(retained_fields) = retained_contributor_fields {
                    fields.extend(retained_fields);
                }
                for layer in 0..wrap_after_project.values().copied().max().unwrap_or(0) {
                    graph = graph.project_fields(
                        fields
                            .iter()
                            .map(|name| {
                                if wrap_after_project.get(name).copied().unwrap_or(0) > layer {
                                    ProjectField::nullable(name, name)
                                } else {
                                    ProjectField::named(name)
                                }
                            })
                            .collect::<Vec<_>>(),
                    );
                }
                nullable_fields = projected_nullable_field_depths.keys().cloned().collect();
                nullable_field_depths = projected_nullable_field_depths;
                if let Some(retained_depths) = retained_contributor_nullable_depths {
                    for (field, depth) in retained_depths {
                        if fields.contains(&field) {
                            nullable_fields.insert(field.clone());
                            nullable_field_depths
                                .entry(field)
                                .and_modify(|existing| *existing = (*existing).max(depth))
                                .or_insert(depth);
                        }
                    }
                }
                available_route_fields = retained_route_fields;
                last_join_right = None;
            }
            LinearStep::OrderBy(keys) => {
                last_join_right = None;
                pending_order = Some(keys.clone());
            }
            LinearStep::Slice {
                partition_by,
                limit,
                offset,
                tie_breaker,
                ..
            } => {
                last_join_right = None;
                let order = pending_order.take().unwrap_or_default();
                graph = lower_window(
                    graph,
                    &order,
                    partition_by,
                    &available_route_fields,
                    *limit,
                    *offset,
                    tie_breaker,
                    plan,
                    root_source,
                    request,
                )?;
            }
            LinearStep::Aggregate { group_by, outputs } => {
                last_join_right = None;
                if pending_order.take().is_some() {
                    return Err(UnsupportedReason::Operator(
                        "order-by before aggregate is not lowered yet".to_owned(),
                    ));
                }
                let lowered =
                    lower_aggregate(graph, group_by, outputs, plan, root_source, request)?;
                graph = lowered.graph;
                fields = lowered.fields;
                nullable_fields = BTreeSet::new();
                nullable_field_depths = BTreeMap::new();
                available_route_fields = BTreeSet::new();
            }
        }
    }

    if let Some(order) = pending_order {
        graph = lower_window(
            graph,
            &order,
            &[],
            &available_route_fields,
            None,
            0,
            &[NormalizedValueRef::RowId(RowIdRef::Source(
                plan.root
                    .source()
                    .ok_or_else(|| {
                        UnsupportedReason::Operator("order fallback must be a source".to_owned())
                    })?
                    .clone(),
            ))],
            plan,
            root_source,
            request,
        )?;
    }

    Ok(LoweredRelationInput {
        graph,
        root_source: Some(root_source.clone()),
        fields,
        nullable_fields,
        nullable_field_depths,
        union_occurrence_carrier: None,
    })
}

fn uses_policy_value_comparison(request: &QueryProgramRequest) -> bool {
    matches!(request.policy, PolicyContext::AuthorizationSubplan { .. })
}

/// Policy-predicate programs reuse relation lowering but do not publish rows.
/// Their intermediate join values remain available to later predicates, never
/// becoming occurrence identity that a public terminal must retain.
fn omits_public_occurrence_carriers(request: &QueryProgramRequest) -> bool {
    uses_policy_value_comparison(request)
        || request
            .output
            .app_rows
            .as_ref()
            .is_some_and(|output| !output.public_terminal)
}

fn policy_join_if_needed(
    left: GraphBuilder,
    right: GraphBuilder,
    left_on: impl IntoIterator<Item = impl Into<String>>,
    right_on: impl IntoIterator<Item = impl Into<String>>,
    request: &QueryProgramRequest,
) -> GraphBuilder {
    if uses_policy_value_comparison(request) {
        GraphBuilder::policy_join(left, right, left_on, right_on)
    } else {
        GraphBuilder::join(left, right, left_on, right_on)
    }
}

fn semi_join_if_needed(
    left: GraphBuilder,
    right: GraphBuilder,
    left_on: impl IntoIterator<Item = impl Into<String>>,
    right_on: impl IntoIterator<Item = impl Into<String>>,
    request: &QueryProgramRequest,
) -> GraphBuilder {
    GraphBuilder::SemiJoin {
        left: std::sync::Arc::new(left),
        right: std::sync::Arc::new(right),
        left_on: left_on.into_iter().map(FieldRef::name).collect(),
        right_on: right_on.into_iter().map(FieldRef::name).collect(),
        comparison: if uses_policy_value_comparison(request) {
            groove::ivm::ValueComparison::Policy
        } else {
            groove::ivm::ValueComparison::Exact
        },
    }
}

fn value_source_descriptor(columns: &[ValueSourceColumn]) -> RecordDescriptor {
    RecordDescriptor::new(
        columns
            .iter()
            .map(|column| (column.name.clone(), column.ty.clone())),
    )
}

fn binding_descriptor_params_with_user_params(
    request: &QueryProgramRequest,
    additional_user_params: impl IntoIterator<Item = (String, ColumnType)>,
) -> Result<Vec<(String, ColumnType)>, UnsupportedReason> {
    let domain = parameter_domain_for_request(request)?;
    let mut user_params = request.input.binding.extra_user_params.clone();
    user_params.extend(domain.user_params.clone());
    user_params.extend(additional_user_params);
    Ok(user_params
        .into_iter()
        .chain(
            domain
                .claim_params
                .into_iter()
                .map(|(name, param)| (name, param.ty)),
        )
        .collect())
}

fn binding_descriptor_params(
    request: &QueryProgramRequest,
) -> Result<Vec<(String, ColumnType)>, UnsupportedReason> {
    binding_descriptor_params_with_user_params(request, [])
}

fn binding_source_descriptor_with_user_params(
    request: &QueryProgramRequest,
    additional_user_params: impl IntoIterator<Item = (String, ColumnType)>,
) -> Result<RecordDescriptor, UnsupportedReason> {
    Ok(RecordDescriptor::new(
        binding_descriptor_params_with_user_params(request, additional_user_params)?
            .into_iter()
            .map(|(name, column_type)| (name, column_type.clone())),
    ))
}

fn lower_value_source(
    shape: &str,
    columns: &[ValueSourceColumn],
    mode: &ValueSourceMode,
    request: &QueryProgramRequest,
) -> Result<GraphBuilder, UnsupportedReason> {
    let descriptor = value_source_descriptor(columns);
    match mode {
        ValueSourceMode::Binding => {
            let domain = parameter_domain_for_request(request)?;
            let params = binding_descriptor_params(request)?;
            for column in columns {
                match &column.value {
                    NormalizedValueRef::Param(param) => {
                        let Some((_, existing)) = params.iter().find(|(name, _)| name == param)
                        else {
                            return Err(UnsupportedReason::Operator(format!(
                                "binding parameter '{param}' is not part of the program parameter domain"
                            )));
                        };
                        if *existing != column.ty {
                            return Err(UnsupportedReason::Operator(format!(
                                "binding parameter '{param}' has conflicting value-source types"
                            )));
                        }
                    }
                    NormalizedValueRef::Claim(path) => {
                        let param = claim_param_field(path);
                        let Some(existing) = domain.claim_params.get(&param) else {
                            return Err(UnsupportedReason::Operator(format!(
                                "claim parameter '{param}' is not part of the program parameter domain"
                            )));
                        };
                        if existing.ty != column.ty {
                            return Err(UnsupportedReason::Operator(format!(
                                "claim parameter '{param}' has conflicting value-source types"
                            )));
                        }
                    }
                    NormalizedValueRef::Literal(_) => {}
                    _ => {
                        return Err(UnsupportedReason::Operator(
                            "binding value source columns must reference binding params, claims, or literals"
                                .to_owned(),
                        ));
                    }
                }
            }
            if params.is_empty() {
                let row = columns
                    .iter()
                    .map(|column| lower_value_source_column(column, request))
                    .collect::<Result<Vec<_>, _>>()?;
                return GraphBuilder::values(descriptor, [row]).map_err(|err| {
                    UnsupportedReason::Operator(format!(
                        "binding value source could not encode constant row: {err}"
                    ))
                });
            }
            let input_descriptor = RecordDescriptor::new(
                params
                    .iter()
                    .map(|(name, column_type)| (name.clone(), column_type.clone())),
            );
            let projected = columns
                .iter()
                .map(|column| column.name.clone())
                .collect::<BTreeSet<_>>();
            let source_user_params = columns
                .iter()
                .filter_map(|column| match &column.value {
                    NormalizedValueRef::Param(param) if domain.user_params.contains_key(param) => {
                        Some(param.clone())
                    }
                    NormalizedValueRef::Claim(path) => {
                        let param = claim_param_field(path);
                        domain.claim_params.contains_key(&param).then_some(param)
                    }
                    _ => None,
                })
                .collect::<BTreeSet<_>>();
            let retained_routes = domain
                .user_params
                .keys()
                .filter(|param| source_user_params.contains(*param))
                .filter_map(|param| {
                    let route_field = route_param_field(param);
                    (!projected.contains(&route_field))
                        .then(|| ProjectField::renamed(param.clone(), route_field))
                })
                // A nested policy graph can consume an enclosing claim only
                // in a sibling/ancestor branch. The shared binding descriptor
                // nevertheless needs that slot to survive this value-source
                // projection so downstream authorization joins can route it.
                .chain(
                    domain
                        .claim_params
                        .keys()
                        .filter(|param| !projected.contains(*param))
                        .map(ProjectField::named),
                )
                .collect::<Vec<_>>();
            Ok(
                GraphBuilder::binding_source(shape.to_owned(), input_descriptor).project_fields(
                    columns
                        .iter()
                        .map(|column| {
                            Ok(match &column.value {
                                NormalizedValueRef::Param(param) => {
                                    ProjectField::renamed(param.clone(), column.name.clone())
                                }
                                NormalizedValueRef::Claim(path) => ProjectField::renamed(
                                    claim_param_field(path),
                                    column.name.clone(),
                                ),
                                NormalizedValueRef::Literal(bytes) => {
                                    let value =
                                        postcard::from_bytes::<Value>(bytes).map_err(|err| {
                                            UnsupportedReason::Operator(format!(
                                                "literal value could not be decoded: {err}"
                                            ))
                                        })?;
                                    ProjectField::literal(column.name.clone(), value)
                                }
                                _ => unreachable!("checked above"),
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .into_iter()
                        .chain(retained_routes),
                ),
            )
        }
        ValueSourceMode::Inline => {
            let row = columns
                .iter()
                .map(|column| lower_value_source_column(column, request))
                .collect::<Result<Vec<_>, _>>()?;
            GraphBuilder::values(descriptor, [row]).map_err(|err| {
                UnsupportedReason::Operator(format!("inline value source could not encode: {err}"))
            })
        }
    }
}

#[cfg(test)]
pub(crate) fn binding_value_source_projection_fields_for_test(
    request: &QueryProgramRequest,
    columns: &[ValueSourceColumn],
) -> Result<BTreeSet<String>, UnsupportedReason> {
    let graph = lower_value_source(
        "test-binding-source",
        columns,
        &ValueSourceMode::Binding,
        request,
    )?;
    graph_declared_output_fields(&graph).ok_or_else(|| {
        UnsupportedReason::Runtime(
            "binding value-source projection must have a named descriptor".to_owned(),
        )
    })
}

fn lower_value_source_column(
    column: &ValueSourceColumn,
    request: &QueryProgramRequest,
) -> Result<Value, UnsupportedReason> {
    match &column.value {
        NormalizedValueRef::Param(name) => request
            .input
            .binding
            .values
            .get(name)
            .cloned()
            .ok_or_else(|| {
                UnsupportedReason::Operator(format!("binding parameter '{name}' is not bound"))
            }),
        NormalizedValueRef::Literal(bytes) => postcard::from_bytes::<Value>(bytes).map_err(|err| {
            UnsupportedReason::Operator(format!("literal value could not be decoded: {err}"))
        }),
        NormalizedValueRef::Claim(path) => claim_value(path, &request.policy),
        _ => Err(UnsupportedReason::Operator(
            "value source columns must be binding params, literals, or claims".to_owned(),
        )),
    }
}

pub(super) fn lower_path_key_pair(
    predicate: &PredicateExpr,
    parent_source_id: &SourceId,
    parent_source: &ResolvedSource,
    child_source_id: &SourceId,
    child_source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    lower_bidirectional_key_pair(
        predicate,
        "correlated path projection only lowers equality correlations",
        "correlated path projection correlation must compare parent and child fields",
        |value| lower_join_key_ref(value, parent_source_id, parent_source, request),
        |value| lower_join_key_ref(value, child_source_id, child_source, request),
    )
}

fn lower_join_key_pair(
    predicate: &PredicateExpr,
    left_source_id: &SourceId,
    left_source: &ResolvedSource,
    right_source_id: &SourceId,
    right_source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    lower_bidirectional_key_pair(
        predicate,
        "join_via only lowers equality join predicates",
        "join_via join predicate must compare the root row id to one join source field",
        |value| lower_join_key_ref(value, left_source_id, left_source, request),
        |value| lower_join_key_ref(value, right_source_id, right_source, request),
    )
}

fn lower_join_key_pairs(
    predicate: &PredicateExpr,
    left_source_id: &SourceId,
    left_source: &ResolvedSource,
    right_source_id: &SourceId,
    right_source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<(Vec<String>, Vec<String>), UnsupportedReason> {
    let pairs = match predicate {
        PredicateExpr::And(predicates) => predicates
            .iter()
            .map(|predicate| {
                lower_join_key_pair(
                    predicate,
                    left_source_id,
                    left_source,
                    right_source_id,
                    right_source,
                    request,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => vec![lower_join_key_pair(
            predicate,
            left_source_id,
            left_source,
            right_source_id,
            right_source,
            request,
        )?],
    };
    if pairs.is_empty() {
        return Err(UnsupportedReason::Operator(
            "join_via requires at least one equality join predicate".to_owned(),
        ));
    }
    Ok(pairs.into_iter().unzip())
}

fn lower_linear_join_key_pair(
    predicate: &PredicateExpr,
    left_root: &LinearRoot,
    left_source: &ResolvedSource,
    right_plan: &RelationInputPlan,
    right_output: &LoweredRelationInput,
    accumulated_join_fields: &BTreeMap<(SourceId, String), (String, usize)>,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    lower_bidirectional_key_pair(
        predicate,
        "join_via only lowers equality join predicates",
        "join_via join predicate must compare left root and right relation fields",
        |value| {
            lower_linear_root_key_ref(value, left_root, left_source, request).or_else(|_| {
                accumulated_join_field(value, accumulated_join_fields)
                    .map(|(field, _)| field)
                    .ok_or_else(|| {
                        UnsupportedReason::Operator(
                            "join left key must reference the root or an accumulated join source"
                                .to_owned(),
                        )
                    })
            })
        },
        |value| lower_relation_key_ref(value, right_plan, right_output, request),
    )
}

fn lower_linear_join_key_pairs(
    predicate: &PredicateExpr,
    left_root: &LinearRoot,
    left_source: &ResolvedSource,
    right_plan: &RelationInputPlan,
    right_output: &LoweredRelationInput,
    accumulated_join_fields: &BTreeMap<(SourceId, String), (String, usize)>,
    request: &QueryProgramRequest,
) -> Result<(Vec<String>, Vec<String>), UnsupportedReason> {
    let pairs = match predicate {
        PredicateExpr::And(predicates) => predicates
            .iter()
            .map(|predicate| {
                lower_linear_join_key_pair(
                    predicate,
                    left_root,
                    left_source,
                    right_plan,
                    right_output,
                    accumulated_join_fields,
                    request,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => vec![lower_linear_join_key_pair(
            predicate,
            left_root,
            left_source,
            right_plan,
            right_output,
            accumulated_join_fields,
            request,
        )?],
    };
    if pairs.is_empty() {
        return Err(UnsupportedReason::Operator(
            "join_via requires at least one equality join predicate".to_owned(),
        ));
    }
    Ok(pairs.into_iter().unzip())
}

fn lower_root_to_relation_key_pair(
    predicate: &PredicateExpr,
    root_source: &ResolvedSource,
    right_plan: &RelationInputPlan,
    right_output: &LoweredRelationInput,
    request: &QueryProgramRequest,
) -> Result<(String, String), UnsupportedReason> {
    lower_bidirectional_key_pair(
        predicate,
        "join contribution membership only lowers equality predicates",
        "join contribution membership must compare root fields to relation output fields",
        |value| lower_join_key_ref(value, &root_source.row_shape.source, root_source, request),
        |value| lower_relation_key_ref(value, right_plan, right_output, request),
    )
}

pub(super) fn lower_root_to_relation_key_pairs(
    predicate: &PredicateExpr,
    root_source: &ResolvedSource,
    right_plan: &RelationInputPlan,
    right_output: &LoweredRelationInput,
    request: &QueryProgramRequest,
) -> Result<(Vec<String>, Vec<String>), UnsupportedReason> {
    let pairs = match predicate {
        PredicateExpr::And(predicates) => predicates
            .iter()
            .map(|predicate| {
                lower_root_to_relation_key_pair(
                    predicate,
                    root_source,
                    right_plan,
                    right_output,
                    request,
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => vec![lower_root_to_relation_key_pair(
            predicate,
            root_source,
            right_plan,
            right_output,
            request,
        )?],
    };
    if pairs.is_empty() {
        return Err(UnsupportedReason::Operator(
            "join contribution membership requires at least one equality predicate".to_owned(),
        ));
    }
    Ok(pairs.into_iter().unzip())
}

fn lower_bidirectional_key_pair(
    predicate: &PredicateExpr,
    non_equality_message: &str,
    mismatch_message: &str,
    left_resolver: impl Fn(&NormalizedValueRef) -> Result<String, UnsupportedReason>,
    right_resolver: impl Fn(&NormalizedValueRef) -> Result<String, UnsupportedReason>,
) -> Result<(String, String), UnsupportedReason> {
    let PredicateExpr::Compare {
        left,
        op: ComparisonOp::Eq,
        right,
    } = predicate
    else {
        return Err(UnsupportedReason::Operator(non_equality_message.to_owned()));
    };

    match (left_resolver(left), right_resolver(right)) {
        (Ok(left_key), Ok(right_key)) => Ok((left_key, right_key)),
        (direct_left, direct_right) => match (left_resolver(right), right_resolver(left)) {
            (Ok(left_key), Ok(right_key)) => Ok((left_key, right_key)),
            (swapped_left, swapped_right) => Err(UnsupportedReason::Operator(format!(
                "{mismatch_message}; direct errors: {}, {}; swapped errors: {}, {}",
                key_pair_error(direct_left),
                key_pair_error(direct_right),
                key_pair_error(swapped_left),
                key_pair_error(swapped_right),
            ))),
        },
    }
}

fn key_pair_error(result: Result<String, UnsupportedReason>) -> String {
    match result {
        Ok(field) => format!("accepted {field:?}"),
        Err(reason) => format!("{reason:?}"),
    }
}

pub(super) fn lower_relation_key_ref(
    value: &NormalizedValueRef,
    plan: &RelationInputPlan,
    output: &LoweredRelationInput,
    request: &QueryProgramRequest,
) -> Result<String, UnsupportedReason> {
    match plan {
        RelationInputPlan::Linear(linear) => {
            if linear_ends_in_projection(linear)
                && let Ok(field) = lower_named_relation_field(value, &output.fields)
            {
                return Ok(field);
            }
            if let Some(LinearStep::Project(columns)) = linear.steps.last()
                && let NormalizedValueRef::RowId(RowIdRef::Source(value_source)) = value
                && linear.root.source() == Some(value_source)
                && let Some(column) = columns.iter().find(|column| {
                    matches!(
                        &column.value,
                        NormalizedValueRef::RowId(RowIdRef::Source(column_source))
                            if column_source == value_source
                    )
                })
            {
                // Relation-backed policy expressions commonly project their
                // source row ID as public `id`. Subsequent outer joins must
                // consume that projected private proof field, rather than
                // asking the graph for the pre-projection `row_uuid` name.
                return Ok(column.output.name.clone());
            }
            if let Some(source) = &output.root_source {
                if let Some(source_id) = linear.root.source() {
                    if let Ok(key) = lower_join_key_ref(value, source_id, source, request) {
                        return Ok(key);
                    }
                }
            }
            lower_named_relation_field(value, &output.fields)
        }
        RelationInputPlan::Union(_) => lower_named_relation_field(value, &output.fields),
        RelationInputPlan::Recursive(_) => lower_named_relation_field(value, &output.fields),
    }
}

fn linear_ends_in_projection(linear: &LinearCurrentRoot) -> bool {
    matches!(linear.steps.last(), Some(LinearStep::Project(_)))
}

fn lower_named_relation_field(
    value: &NormalizedValueRef,
    fields: &BTreeSet<String>,
) -> Result<String, UnsupportedReason> {
    let field = match value {
        NormalizedValueRef::FrontierColumn { field, .. } => field,
        NormalizedValueRef::Param(param) => param,
        NormalizedValueRef::SourceField { field, .. } => field,
        NormalizedValueRef::RowId(RowIdRef::Frontier(_)) => "row_uuid",
        NormalizedValueRef::RowId(RowIdRef::Source(_))
        | NormalizedValueRef::Claim(_)
        | NormalizedValueRef::Provenance { .. }
        | NormalizedValueRef::Literal(_) => {
            return Err(UnsupportedReason::Operator(
                "join relation key must be an output field".to_owned(),
            ));
        }
    };
    if fields.contains(field) {
        Ok(field.to_owned())
    } else {
        Err(UnsupportedReason::Operator(format!(
            "join relation does not output field '{field}'"
        )))
    }
}

fn lower_linear_root_key_ref(
    value: &NormalizedValueRef,
    root: &LinearRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<String, UnsupportedReason> {
    match root {
        LinearRoot::Source {
            source: source_id, ..
        } => lower_join_key_ref(value, source_id, source, request),
        LinearRoot::Frontier { frontier, columns } => match value {
            NormalizedValueRef::FrontierColumn {
                frontier: value_frontier,
                field,
            } if value_frontier == frontier
                && columns.iter().any(|column| column.name == *field) =>
            {
                Ok(field.clone())
            }
            NormalizedValueRef::RowId(RowIdRef::Frontier(value_frontier))
                if value_frontier == frontier
                    && columns.iter().any(|column| column.name == "row_uuid") =>
            {
                Ok("row_uuid".to_owned())
            }
            _ => Err(UnsupportedReason::Operator(
                "join left key must be a frontier column".to_owned(),
            )),
        },
        LinearRoot::Value { columns, .. } => match value {
            NormalizedValueRef::Param(name)
            | NormalizedValueRef::FrontierColumn { field: name, .. }
                if columns.iter().any(|column| column.name == *name) =>
            {
                Ok(name.clone())
            }
            _ => Err(UnsupportedReason::Operator(
                "join left key must be a value-source column".to_owned(),
            )),
        },
    }
}

fn accumulated_join_field(
    value: &NormalizedValueRef,
    fields: &BTreeMap<(SourceId, String), (String, usize)>,
) -> Option<(String, usize)> {
    let (source, field) = match value {
        NormalizedValueRef::SourceField { source, field } => (source, user_column_field(field)),
        NormalizedValueRef::RowId(RowIdRef::Source(source)) => (source, "row_uuid".to_owned()),
        _ => return None,
    };
    fields.get(&(source.clone(), field)).cloned()
}

fn lower_projection_field(
    column: &RowProjection,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    fields: &BTreeSet<String>,
    field_nullable_depths: &BTreeMap<String, usize>,
    last_join_right: Option<&(
        RelationInputPlan,
        BTreeSet<String>,
        BTreeMap<String, usize>,
        BTreeSet<String>,
    )>,
    accumulated_join_fields: &BTreeMap<(SourceId, String), (String, usize)>,
    request: &QueryProgramRequest,
) -> Result<ProjectionFieldPlan, UnsupportedReason> {
    let mut unwrap_before_project = BTreeMap::new();
    let mut nullable_after_project = None;
    let mut source_nullability = None;
    let project = match lower_projection_source(
        &column.value,
        plan,
        source,
        fields,
        field_nullable_depths,
        last_join_right,
        accumulated_join_fields,
        request,
    )? {
        ProjectionSource::Field {
            field,
            nullable_depth,
        } => {
            let output_depth = value_type_nullable_depth(&column.output.ty);
            let unwrap_depth = nullable_depth.saturating_sub(output_depth);
            if unwrap_depth > 0 {
                unwrap_before_project.insert(field.clone(), unwrap_depth);
            }
            if output_depth > 0 {
                nullable_after_project = Some((column.output.name.clone(), output_depth));
            }
            source_nullability = Some((field.clone(), nullable_depth));
            ProjectField::renamed(field, column.output.name.clone())
        }
        ProjectionSource::Literal(value) => {
            ProjectField::literal(column.output.name.clone(), value)
        }
    };
    Ok(ProjectionFieldPlan {
        project,
        unwrap_before_project,
        nullable_after_project,
        source_nullability,
    })
}

#[derive(Clone, Debug)]
enum ProjectionSource {
    Field {
        field: String,
        nullable_depth: usize,
    },
    Literal(LiteralValue),
}

#[derive(Clone, Debug)]
struct ProjectionFieldPlan {
    pub(super) project: ProjectField,
    pub(super) unwrap_before_project: BTreeMap<String, usize>,
    pub(super) nullable_after_project: Option<(String, usize)>,
    pub(super) source_nullability: Option<(String, usize)>,
}

fn lower_projection_source(
    value: &NormalizedValueRef,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    fields: &BTreeSet<String>,
    field_nullable_depths: &BTreeMap<String, usize>,
    last_join_right: Option<&(
        RelationInputPlan,
        BTreeSet<String>,
        BTreeMap<String, usize>,
        BTreeSet<String>,
    )>,
    accumulated_join_fields: &BTreeMap<(SourceId, String), (String, usize)>,
    request: &QueryProgramRequest,
) -> Result<ProjectionSource, UnsupportedReason> {
    if let Ok(field) = lower_linear_root_key_ref(value, &plan.root, source, request) {
        let nullable_depth = field_nullable_depths.get(&field).copied().unwrap_or(0);
        return Ok(ProjectionSource::Field {
            field: match last_join_right {
                Some(_) => left_field(&field),
                None => field,
            },
            nullable_depth,
        });
    }

    if let NormalizedValueRef::Param(param) = value
        && fields.contains(param)
    {
        return Ok(ProjectionSource::Field {
            field: match last_join_right {
                Some(_) => left_field(param),
                None => param.clone(),
            },
            nullable_depth: 0,
        });
    }
    if let Some((field, _)) = accumulated_join_field(value, accumulated_join_fields) {
        let nullable_depth = field_nullable_depths.get(&field).copied().unwrap_or(0);
        return Ok(ProjectionSource::Field {
            field: match last_join_right {
                Some(_) => left_field(&field),
                None => field,
            },
            nullable_depth,
        });
    }
    if let Some((right, nullable_fields, nullable_field_depths, _)) = last_join_right {
        if let Some(field) = lower_relation_projection_ref(value, right, request)? {
            let nullable_depth = if nullable_fields.contains(&field) {
                nullable_field_depths.get(&field).copied().unwrap_or(1)
            } else {
                0
            };
            return Ok(ProjectionSource::Field {
                field: right_field(&field),
                nullable_depth,
            });
        }
    }

    match lower_literal_projection_value(value, request)? {
        Some(value) => Ok(ProjectionSource::Literal(value)),
        None => Err(UnsupportedReason::Operator(
            "project value must reference the current root, last join input, or a literal"
                .to_owned(),
        )),
    }
}

fn lower_relation_projection_ref(
    value: &NormalizedValueRef,
    plan: &RelationInputPlan,
    _request: &QueryProgramRequest,
) -> Result<Option<String>, UnsupportedReason> {
    match plan {
        RelationInputPlan::Linear(linear) => {
            if matches!(linear.root, LinearRoot::Source { .. }) {
                if let Some(source_id) = linear.root.source() {
                    match value {
                        NormalizedValueRef::SourceField {
                            source: value_source,
                            field,
                        } if value_source == source_id => {
                            return Ok(Some(user_column_field(field)));
                        }
                        NormalizedValueRef::RowId(RowIdRef::Source(value_source))
                            if value_source == source_id =>
                        {
                            return Ok(Some("row_uuid".to_owned()));
                        }
                        _ => {}
                    }
                }
            }
            match value {
                NormalizedValueRef::Param(param)
                | NormalizedValueRef::FrontierColumn { field: param, .. } => {
                    Ok(Some(param.clone()))
                }
                NormalizedValueRef::Literal(_) => Ok(None),
                NormalizedValueRef::Claim(_)
                | NormalizedValueRef::SourceField { .. }
                | NormalizedValueRef::RowId(_)
                | NormalizedValueRef::Provenance { .. } => Ok(None),
            }
        }
        RelationInputPlan::Recursive(relation) => match value {
            NormalizedValueRef::FrontierColumn { frontier, field }
                if frontier == &relation.frontier =>
            {
                Ok(Some(field.clone()))
            }
            NormalizedValueRef::Param(param) => Ok(Some(param.clone())),
            NormalizedValueRef::Literal(_) => Ok(None),
            NormalizedValueRef::Claim(_)
            | NormalizedValueRef::SourceField { .. }
            | NormalizedValueRef::RowId(_)
            | NormalizedValueRef::Provenance { .. }
            | NormalizedValueRef::FrontierColumn { .. } => Ok(None),
        },
        RelationInputPlan::Union(_) => match value {
            NormalizedValueRef::Param(param)
            | NormalizedValueRef::FrontierColumn { field: param, .. }
            | NormalizedValueRef::SourceField { field: param, .. } => Ok(Some(param.clone())),
            NormalizedValueRef::RowId(_) => Ok(Some("row_uuid".to_owned())),
            NormalizedValueRef::Literal(_)
            | NormalizedValueRef::Claim(_)
            | NormalizedValueRef::Provenance { .. } => Ok(None),
        },
    }
}

fn lower_equality_param_filter_joins(
    mut graph: GraphBuilder,
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    available_route_fields: &BTreeSet<String>,
    request: &QueryProgramRequest,
) -> Result<(GraphBuilder, PredicateExpr, BTreeSet<String>), UnsupportedReason> {
    let predicates = match predicate {
        PredicateExpr::And(predicates) => predicates.as_slice(),
        _ => std::slice::from_ref(predicate),
    };
    let mut residual = Vec::new();
    let mut retained_route_fields = available_route_fields.clone();
    for predicate in predicates {
        let Some(join) = equality_param_join(predicate, source_id, source)? else {
            residual.push(predicate.clone());
            continue;
        };
        let Some(binding_source_shape) = &request.input.binding.source_shape else {
            residual.push(predicate.clone());
            continue;
        };
        let domain = parameter_domain_for_request(request)?;
        let is_claim_param = domain.claim_params.contains_key(&join.param);
        let binding_descriptor = if is_claim_param {
            binding_source_descriptor_with_user_params(request, [])?
        } else {
            binding_source_descriptor_with_user_params(
                request,
                [(join.param.clone(), join.value_type.clone())],
            )?
        };
        let binding_fields = binding_descriptor
            .fields()
            .iter()
            .map(|field| {
                field.name.clone().ok_or_else(|| {
                    UnsupportedReason::Operator("binding fields must be named".to_owned())
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let mut binding =
            GraphBuilder::binding_source(binding_source_shape.clone(), binding_descriptor);
        let route_field = if is_claim_param {
            join.param.clone()
        } else {
            route_param_field(&join.param)
        };
        let route_is_nullable = domain
            .claim_params
            .get(&join.param)
            .map(|claim| matches!(claim.ty, ColumnType::Nullable(_)))
            .or_else(|| {
                domain
                    .user_params
                    .get(&join.param)
                    .map(|ty| matches!(ty, ColumnType::Nullable(_)))
            })
            .unwrap_or(false);
        // Equality unwrapping must not become the route carrier's value
        // conversion. Keep a second, untouched copy of a nullable binding for
        // the policy-arm union and for the downstream result-membership
        // window, then unwrap only the copy used as the equality key.
        let route_carrier = (join.nullable && route_is_nullable)
            .then(|| format!("__jazz_route_carrier:{}", join.param));
        if let Some(route_carrier) = &route_carrier {
            let mut fields = binding_fields
                .iter()
                .cloned()
                .map(ProjectField::named)
                .collect::<Vec<_>>();
            fields.push(ProjectField::renamed(join.param.clone(), route_carrier));
            binding = binding.project_fields(fields);
        }
        let mut projection = project_source_fields_from_prefix_rewrapping_nullable(
            source,
            LEFT_JOIN_PREFIX,
            join.nullable.then_some(join.field.as_str()),
        );
        projection.extend(
            retained_route_fields
                .iter()
                .map(|field| ProjectField::renamed(left_field(&field), field.clone())),
        );
        projection.push(ProjectField::renamed(
            right_field(route_carrier.as_deref().unwrap_or(&join.param)),
            route_field.clone(),
        ));
        if join.nullable {
            graph = graph.unwrap_nullable(join.field.clone());
            binding = binding.unwrap_nullable(join.param.clone());
        }
        graph = policy_join_if_needed(graph, binding, [join.field], [join.param], request)
            .project_fields(projection);
        retained_route_fields.insert(route_field);
    }
    let residual = match residual.len() {
        0 => PredicateExpr::True,
        1 => residual.pop().expect("one residual predicate"),
        _ => PredicateExpr::And(residual),
    };
    Ok((graph, residual, retained_route_fields))
}

struct EqualityParamJoin {
    pub(super) field: String,
    pub(super) param: String,
    pub(super) value_type: ValueType,
    pub(super) nullable: bool,
}

fn equality_param_join(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
) -> Result<Option<EqualityParamJoin>, UnsupportedReason> {
    let PredicateExpr::Compare {
        left,
        op: ComparisonOp::Eq,
        right,
    } = predicate
    else {
        return Ok(None);
    };
    if let (Some((field, value_type, nullable)), NormalizedValueRef::Param(param)) =
        (source_join_field(left, source_id, source)?, right)
    {
        return Ok(Some(EqualityParamJoin {
            field,
            param: param.clone(),
            value_type,
            nullable,
        }));
    }
    match (left, source_join_field(right, source_id, source)?) {
        (NormalizedValueRef::Param(param), Some((field, value_type, nullable))) => {
            Ok(Some(EqualityParamJoin {
                field,
                param: param.clone(),
                value_type,
                nullable,
            }))
        }
        _ => Ok(None),
    }
}

fn source_join_field(
    value: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
) -> Result<Option<(String, ValueType, bool)>, UnsupportedReason> {
    let field = match value {
        NormalizedValueRef::SourceField {
            source: value_source,
            field,
        } if value_source == source_id => {
            let resolved = require_source_field(source, &user_column_field(field))
                .or_else(|_| require_source_field(source, field));
            resolved?
        }
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) if value_source == source_id => {
            require_source_field(source, &source.row_shape.row_uuid_field)?
        }
        _ => return Ok(None),
    };
    let Some(value_type) = source_field_type(source, &field).cloned() else {
        return Err(UnsupportedReason::Runtime(format!(
            "source field {field:?} is missing from resolved descriptor"
        )));
    };
    let (value_type, nullable) = match value_type {
        ValueType::Nullable(inner) => ((*inner).clone(), true),
        value_type => (value_type, false),
    };
    Ok(Some((field, value_type, nullable)))
}

fn lower_literal_projection_value(
    value: &NormalizedValueRef,
    request: &QueryProgramRequest,
) -> Result<Option<LiteralValue>, UnsupportedReason> {
    match value {
        NormalizedValueRef::Literal(bytes) => {
            let value = postcard::from_bytes::<Value>(bytes).map_err(|err| {
                UnsupportedReason::Operator(format!("literal value could not be decoded: {err}"))
            })?;
            Ok(Some(value.into()))
        }
        NormalizedValueRef::Param(name) => {
            let value = request.input.binding.values.get(name).ok_or_else(|| {
                UnsupportedReason::Operator(format!("binding parameter '{name}' is not bound"))
            })?;
            Ok(Some(value.clone().into()))
        }
        _ => Ok(None),
    }
}

fn lower_join_key_ref(
    value: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<String, UnsupportedReason> {
    if let NormalizedValueRef::SourceField {
        source: value_source,
        field,
    } = value
        && value_source == source_id
        && field == "id"
    {
        let declared_id = user_column_field(field);
        if source
            .row_shape
            .descriptor
            .fields()
            .iter()
            .any(|candidate| candidate.name.as_deref() == Some(declared_id.as_str()))
        {
            return Ok(declared_id);
        }
        return require_source_field(source, &source.row_shape.row_uuid_field);
    }
    match lower_value_ref(value, source_id, source, request)? {
        LoweredValueRef::Field(field) => Ok(field),
        LoweredValueRef::Literal(_) => Err(UnsupportedReason::Operator(
            "join_via join keys must be source fields".to_owned(),
        )),
    }
}

fn source_field_is_nullable(source: &ResolvedSource, field: &str) -> bool {
    source_field_nullable_depth(source, field) > 0
}

fn value_type_nullable_depth(mut value_type: &ValueType) -> usize {
    let mut depth = 0;
    while let ValueType::Nullable(inner) = value_type {
        depth += 1;
        value_type = inner;
    }
    depth
}

fn source_field_nullable_depth(source: &ResolvedSource, field: &str) -> usize {
    source_field_type(source, field).map_or(0, value_type_nullable_depth)
}

pub(super) fn resolved_source_descriptor_index(
    source: &ResolvedSource,
    field: &str,
) -> Option<usize> {
    source.row_shape.descriptor.field_index(field).or_else(|| {
        source
            .row_shape
            .descriptor
            .fields()
            .iter()
            .position(|candidate| candidate.name.as_deref() == Some(field))
    })
}

pub(super) fn source_field_type<'a>(
    source: &'a ResolvedSource,
    field: &str,
) -> Option<&'a ValueType> {
    resolved_source_descriptor_index(source, field)
        .and_then(|index| source.row_shape.descriptor.fields().get(index))
        .map(|field| &field.value_type)
}

fn project_left_source_fields_with_join_routes(
    source: &ResolvedSource,
    existing_route_fields: &BTreeSet<String>,
    introduced_route_fields: &BTreeSet<String>,
) -> Vec<ProjectField> {
    let mut fields = project_source_fields_from_prefix(source, LEFT_JOIN_PREFIX);
    fields.extend(
        existing_route_fields
            .iter()
            .map(|field| ProjectField::renamed(left_field(&field), field.clone())),
    );
    fields.extend(
        introduced_route_fields
            .iter()
            .map(|field| ProjectField::renamed(right_field(&field), field.clone())),
    );
    fields
}

fn lower_window(
    graph: GraphBuilder,
    order: &[OrderKey],
    partition_by: &[NormalizedValueRef],
    route_fields: &BTreeSet<String>,
    limit: Option<u32>,
    offset: u32,
    tie_breaker: &[NormalizedValueRef],
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GraphBuilder, UnsupportedReason> {
    let mut group_cols = partition_by
        .iter()
        .map(|value| lower_field_ref(value, plan, source, request, "slice partition key"))
        .collect::<Result<Vec<_>, _>>()?;
    // One maintained graph serves every active binding. A window must therefore
    // be independent for each route tuple before the runtime filters its sinks.
    for route_field in route_fields {
        if !group_cols.contains(route_field) {
            group_cols.push(route_field.clone());
        }
    }
    let order_cols = order
        .iter()
        .map(|key| lower_order_key(key, plan, source, request))
        .collect::<Result<Vec<_>, _>>()?;
    let tie_cols = if tie_breaker.is_empty() {
        vec![source.row_shape.row_uuid_field.clone()]
    } else {
        tie_breaker
            .iter()
            .map(|value| lower_field_ref(value, plan, source, request, "slice tie-breaker"))
            .collect::<Result<Vec<_>, _>>()?
    }
    .into_iter()
    .filter(|tie| {
        !order_cols
            .iter()
            .any(|order| order.field == FieldRef::name(tie.clone()))
    })
    .collect::<Vec<_>>();
    let top_by_limit = match limit {
        Some(limit) => TopByLimit::Finite(u64::from(limit)),
        None => TopByLimit::Unbounded,
    };

    if order.is_empty() {
        if offset == 0 && limit == Some(1) {
            return Ok(GraphBuilder::arg_min_by(graph, group_cols, tie_cols));
        }
        return Ok(GraphBuilder::top_by(
            graph,
            group_cols,
            Vec::new(),
            tie_cols,
            u64::from(offset),
            top_by_limit,
        ));
    }

    Ok(GraphBuilder::top_by(
        graph,
        group_cols,
        order_cols,
        tie_cols,
        u64::from(offset),
        top_by_limit,
    ))
}

fn lower_aggregate(
    mut graph: GraphBuilder,
    group_by: &[NormalizedValueRef],
    outputs: &[AggregateExpr],
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<LoweredRelationInput, UnsupportedReason> {
    let group_cols = group_by
        .iter()
        .map(|value| lower_field_ref(value, plan, source, request, "aggregate group key"))
        .collect::<Result<Vec<_>, _>>()?;
    let aggregates = outputs
        .iter()
        .map(|aggregate| lower_aggregate_expr(aggregate, plan, source, request))
        .collect::<Result<Vec<_>, _>>()?;
    let mut unwrap_fields = BTreeSet::new();
    for field in &group_cols {
        if source_field_is_nullable(source, field) {
            unwrap_fields.insert(field.clone());
        }
    }
    for aggregate in outputs {
        let Some(input) = &aggregate.input else {
            continue;
        };
        let field = lower_field_ref(input, plan, source, request, "aggregate input")?;
        if source_field_is_nullable(source, &field) {
            unwrap_fields.insert(field);
        }
    }
    for field in unwrap_fields {
        graph = GraphBuilder::UnwrapNullable {
            input: std::sync::Arc::new(graph),
            field: FieldRef::stored_name(field),
        };
    }
    let mut fields = group_cols.iter().cloned().collect::<BTreeSet<_>>();
    fields.extend(
        outputs
            .iter()
            .map(|aggregate| aggregate_output_field(&aggregate.output.name)),
    );
    Ok(LoweredRelationInput {
        graph: GraphBuilder::Aggregate {
            input: std::sync::Arc::new(graph),
            group_cols: group_cols.into_iter().map(FieldRef::stored_name).collect(),
            aggregates,
        },
        root_source: Some(source.clone()),
        fields,
        nullable_fields: BTreeSet::new(),
        nullable_field_depths: BTreeMap::new(),
        union_occurrence_carrier: None,
    })
}

fn lower_aggregate_expr(
    aggregate: &AggregateExpr,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GrooveAggregateExpr, UnsupportedReason> {
    let expression = aggregate
        .input
        .as_ref()
        .map(|value| {
            let carrier = lower_field_ref(value, plan, source, request, "aggregate input")?;
            let descriptor = &source.row_shape.descriptor;
            let index = descriptor
                .fields()
                .iter()
                .position(|field| field.name.as_deref() == Some(carrier.as_str()))
                .ok_or_else(|| {
                    UnsupportedReason::Operator(format!(
                        "aggregate input carrier {carrier:?} is missing from resolved source"
                    ))
                })?;
            let field = &descriptor.fields()[index];
            // PlanExpr carries a name. Preserve the selected source slot by
            // choosing a spelling that its logical-then-carrier resolver will
            // bind back to that exact slot, including user-prefix collisions.
            let name = field
                .name
                .as_deref()
                .into_iter()
                .chain(field.logical_name())
                .find(|name| resolved_source_descriptor_index(source, name) == Some(index))
                .ok_or_else(|| {
                    UnsupportedReason::Operator(format!(
                        "aggregate input carrier {carrier:?} has no unambiguous expression name"
                    ))
                })?;
            Ok(GroovePlanExpr::field(name))
        })
        .transpose()?;
    Ok(GrooveAggregateExpr {
        function: match aggregate.function {
            AggregateFunction::Count => GrooveAggregateFunction::Count,
            AggregateFunction::Sum => GrooveAggregateFunction::Sum,
            AggregateFunction::Avg => GrooveAggregateFunction::Avg,
            AggregateFunction::Min => GrooveAggregateFunction::Min,
            AggregateFunction::Max => GrooveAggregateFunction::Max,
        },
        expression,
        distinct: false,
        output_name: Some(aggregate_output_field(&aggregate.output.name)),
        output_identity: Some(groove::records::FieldIdentity::Name(
            aggregate_output_field(&aggregate.output.name),
        )),
    })
}

fn lower_order_key(
    key: &OrderKey,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<TopByOrder, UnsupportedReason> {
    // Preserve source qualification validation before binding the exact carrier.
    let lowered = lower_field_ref(&key.value, plan, source, request, "order key")?;
    let field = match collect_window_source_field(source, &key.value) {
        Some(field) => FieldRef::stored_name(field.name.clone().expect("window fields are named")),
        None => FieldRef::name(lowered),
    };
    Ok(TopByOrder {
        field,
        direction: match key.direction {
            SortDirection::Asc => groove::ivm::TopByDirection::Asc,
            SortDirection::Desc => groove::ivm::TopByDirection::Desc,
        },
    })
}

fn lower_predicate(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    let lowered = match lower_predicate_inner(predicate, source_id, source, request) {
        Err(reason) if is_unbound_claim_reason(&reason) => constant_predicate(false),
        other => other?,
    };
    Ok(lowered.canonicalize())
}

fn lower_predicate_inner(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    Ok(match predicate {
        PredicateExpr::True => GroovePredicateExpr::And(Vec::new()),
        PredicateExpr::False => GroovePredicateExpr::Or(Vec::new()),
        PredicateExpr::Compare { left, op, right } => {
            lower_compare(left, *op, right, source_id, source, request)?
        }
        PredicateExpr::In { value, options } => {
            let predicates = options
                .iter()
                .map(|option| {
                    lower_compare(value, ComparisonOp::Eq, option, source_id, source, request)
                })
                .collect::<Result<Vec<_>, _>>()?;
            GroovePredicateExpr::Or(predicates)
        }
        PredicateExpr::ArrayContains { value, needle } => {
            lower_contains(value, needle, source_id, source, request)?
        }
        PredicateExpr::TextContains { .. } => {
            return Err(UnsupportedReason::Operator(
                "text containment predicates are not lowered yet".to_owned(),
            ));
        }
        PredicateExpr::IsNull(value) => lower_null_test(value, true, source_id, source, request)?,
        PredicateExpr::IsNotNull(value) => {
            lower_null_test(value, false, source_id, source, request)?
        }
        PredicateExpr::EnumMatch {
            value,
            case_tag,
            payload,
        } => {
            let LoweredValueRef::Field(field) = lower_value_ref(value, source_id, source, request)?
            else {
                return Err(UnsupportedReason::Operator(
                    "enum match requires a source field".to_owned(),
                ));
            };
            GroovePredicateExpr::EnumMatch {
                field,
                case_tag: *case_tag,
                payload: Box::new(lower_enum_payload_predicate(payload)?),
            }
        }
        PredicateExpr::And(predicates) => GroovePredicateExpr::And(
            predicates
                .iter()
                .map(|predicate| lower_predicate(predicate, source_id, source, request))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::Or(predicates) => GroovePredicateExpr::Or(
            predicates
                .iter()
                .map(|predicate| lower_predicate(predicate, source_id, source, request))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::Not(predicate) => {
            lower_not_predicate(predicate, source_id, source, request)?
        }
    })
}

fn lower_enum_payload_predicate(
    predicate: &PredicateExpr,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    Ok(match predicate {
        PredicateExpr::True => GroovePredicateExpr::And(Vec::new()),
        PredicateExpr::False => GroovePredicateExpr::Or(Vec::new()),
        PredicateExpr::Compare { left, op, right } => {
            let (field, value, op) = match (left, right) {
                (
                    NormalizedValueRef::SourceField { field, .. },
                    NormalizedValueRef::Literal(bytes),
                ) => {
                    let value = postcard::from_bytes::<Value>(bytes).map_err(|err| {
                        UnsupportedReason::Operator(format!(
                            "payload enum literal could not be decoded: {err}"
                        ))
                    })?;
                    (field.clone(), LiteralValue::from(value), *op)
                }
                (
                    NormalizedValueRef::Literal(bytes),
                    NormalizedValueRef::SourceField { field, .. },
                ) => {
                    let value = postcard::from_bytes::<Value>(bytes).map_err(|err| {
                        UnsupportedReason::Operator(format!(
                            "payload enum literal could not be decoded: {err}"
                        ))
                    })?;
                    (
                        field.clone(),
                        LiteralValue::from(value),
                        invert_comparison(*op),
                    )
                }
                _ => {
                    return Err(UnsupportedReason::Operator(
                        "payload enum predicates require field/literal comparisons".to_owned(),
                    ));
                }
            };
            GroovePredicateExpr::from_field_literal(predicate_kind(op), field, value)
        }
        PredicateExpr::IsNull(NormalizedValueRef::SourceField { field, .. }) => {
            GroovePredicateExpr::IsNull {
                field: field.clone(),
            }
        }
        PredicateExpr::IsNotNull(NormalizedValueRef::SourceField { field, .. }) => {
            GroovePredicateExpr::IsNotNull {
                field: field.clone(),
            }
        }
        PredicateExpr::And(children) => GroovePredicateExpr::And(
            children
                .iter()
                .map(lower_enum_payload_predicate)
                .collect::<Result<_, _>>()?,
        ),
        PredicateExpr::Or(children) => GroovePredicateExpr::Or(
            children
                .iter()
                .map(lower_enum_payload_predicate)
                .collect::<Result<_, _>>()?,
        ),
        PredicateExpr::Not(_child) => {
            return Err(UnsupportedReason::Operator(
                "negated payload enum predicates are not lowered yet".to_owned(),
            ));
        }
        _ => {
            return Err(UnsupportedReason::Operator(
                "unsupported payload enum predicate".to_owned(),
            ));
        }
    })
}

fn lower_not_predicate(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    let lowered = match lower_not_predicate_inner(predicate, source_id, source, request) {
        Err(reason) if is_unbound_claim_reason(&reason) => constant_predicate(false),
        other => other?,
    };
    Ok(lowered.canonicalize())
}

fn lower_not_predicate_inner(
    predicate: &PredicateExpr,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    Ok(match predicate {
        PredicateExpr::True => GroovePredicateExpr::Or(Vec::new()),
        PredicateExpr::False => GroovePredicateExpr::And(Vec::new()),
        PredicateExpr::Compare { left, op, right } => lower_compare(
            left,
            invert_comparison(*op),
            right,
            source_id,
            source,
            request,
        )?,
        PredicateExpr::In { value, options } => GroovePredicateExpr::And(
            options
                .iter()
                .map(|option| lower_two_valued_ne(value, option, source_id, source, request))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::ArrayContains { .. } | PredicateExpr::TextContains { .. } => {
            return Err(UnsupportedReason::Operator(
                "negated containment predicates are not lowered yet".to_owned(),
            ));
        }
        PredicateExpr::IsNull(value) => lower_null_test(value, false, source_id, source, request)?,
        PredicateExpr::IsNotNull(value) => {
            lower_null_test(value, true, source_id, source, request)?
        }
        PredicateExpr::And(predicates) => GroovePredicateExpr::Or(
            predicates
                .iter()
                .map(|predicate| lower_not_predicate(predicate, source_id, source, request))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::Or(predicates) => GroovePredicateExpr::And(
            predicates
                .iter()
                .map(|predicate| lower_not_predicate(predicate, source_id, source, request))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        PredicateExpr::Not(predicate) => lower_predicate(predicate, source_id, source, request)?,
        PredicateExpr::EnumMatch { .. } => {
            return Err(UnsupportedReason::Operator(
                "negated enum match predicates are not lowered yet".to_owned(),
            ));
        }
    })
}

fn lower_two_valued_ne(
    left: &NormalizedValueRef,
    right: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    // Groove comparisons deliberately use SQL-null semantics. Jazz comparison
    // predicates are two-valued, so unequal means either exactly one operand is
    // null or both are non-null and Groove reports inequality.
    Ok(GroovePredicateExpr::Or(vec![
        GroovePredicateExpr::And(vec![
            lower_null_test(left, true, source_id, source, request)?,
            lower_null_test(right, false, source_id, source, request)?,
        ]),
        GroovePredicateExpr::And(vec![
            lower_null_test(left, false, source_id, source, request)?,
            lower_null_test(right, true, source_id, source, request)?,
        ]),
        lower_compare(left, ComparisonOp::Ne, right, source_id, source, request)?,
    ])
    .canonicalize())
}

fn invert_comparison(op: ComparisonOp) -> ComparisonOp {
    match op {
        ComparisonOp::Eq => ComparisonOp::Ne,
        ComparisonOp::Ne => ComparisonOp::Eq,
        ComparisonOp::Lt => ComparisonOp::Gte,
        ComparisonOp::Lte => ComparisonOp::Gt,
        ComparisonOp::Gt => ComparisonOp::Lte,
        ComparisonOp::Gte => ComparisonOp::Lt,
    }
}

fn lower_compare(
    left: &NormalizedValueRef,
    op: ComparisonOp,
    right: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    let left = lower_value_ref(left, source_id, source, request)?;
    let right = lower_value_ref(right, source_id, source, request)?;
    let kind = predicate_kind(op);

    match (left, right) {
        (LoweredValueRef::Field(field), LoweredValueRef::Literal(value)) => {
            let value = coerce_literal_for_source_field(value, source, &field);
            Ok(GroovePredicateExpr::from_field_literal(kind, field, value))
        }
        (LoweredValueRef::Literal(value), LoweredValueRef::Field(field)) => {
            Ok(GroovePredicateExpr::from_field_literal(
                kind.reversed(),
                field.clone(),
                coerce_literal_for_source_field(value, source, &field),
            ))
        }
        (LoweredValueRef::Field(field), LoweredValueRef::Field(value_field)) => match op {
            ComparisonOp::Eq => Ok(GroovePredicateExpr::EqField { field, value_field }),
            ComparisonOp::Ne => Ok(GroovePredicateExpr::NeqField { field, value_field }),
            _ => Err(UnsupportedReason::Operator(format!(
                "field-to-field comparison {:?} is not lowered yet",
                op
            ))),
        },
        (LoweredValueRef::Literal(left), LoweredValueRef::Literal(right)) => {
            Ok(constant_predicate(compare_literals(&left, op, &right)))
        }
    }
}

fn lower_contains(
    value: &NormalizedValueRef,
    needle: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    let value = lower_value_ref(value, source_id, source, request)?;
    let needle = lower_value_ref(needle, source_id, source, request)?;
    match (value, needle) {
        (LoweredValueRef::Field(field), LoweredValueRef::Literal(value)) => {
            let value = coerce_literal_for_source_array_element(value, source, &field);
            Ok(GroovePredicateExpr::Contains { field, value })
        }
        (LoweredValueRef::Field(field), LoweredValueRef::Field(needle_field)) => {
            Ok(GroovePredicateExpr::ContainsField {
                field,
                needle_field,
            })
        }
        (LoweredValueRef::Literal(LiteralValue::Array(values)), LoweredValueRef::Field(field)) => {
            if values.is_empty() {
                return Ok(constant_predicate(false));
            }
            Ok(GroovePredicateExpr::Or(
                values
                    .into_iter()
                    .map(|value| GroovePredicateExpr::Eq {
                        field: field.clone(),
                        value: coerce_literal_for_source_field(value, source, &field),
                    })
                    .collect(),
            ))
        }
        _ => Err(UnsupportedReason::Operator(
            "array contains requires a source field haystack".to_owned(),
        )),
    }
}

fn coerce_literal_for_source_array_element(
    value: LiteralValue,
    source: &ResolvedSource,
    field: &str,
) -> LiteralValue {
    let Some(value_type) = source_field_type(source, field) else {
        return value;
    };
    match non_null_value_type(value_type) {
        ValueType::Array(member) => coerce_literal_for_value_type(value, member),
        _ => value,
    }
}

fn coerce_literal_for_source_field(
    value: LiteralValue,
    source: &ResolvedSource,
    field: &str,
) -> LiteralValue {
    let Some(value_type) = source_field_type(source, field) else {
        return value;
    };
    coerce_literal_for_value_type(value, value_type)
}

fn non_null_value_type(mut value_type: &ValueType) -> &ValueType {
    while let ValueType::Nullable(inner) = value_type {
        value_type = inner.as_ref();
    }
    value_type
}

pub(super) fn coerce_literal_for_value_type(
    value: LiteralValue,
    value_type: &ValueType,
) -> LiteralValue {
    match (value, value_type) {
        (LiteralValue::String(value), ValueType::Uuid) => uuid::Uuid::parse_str(&value)
            .map(LiteralValue::Uuid)
            .unwrap_or(LiteralValue::String(value)),
        (LiteralValue::Uuid(value), ValueType::String) => LiteralValue::String(value.to_string()),
        (LiteralValue::String(value), ValueType::EnumTag(schema)) => schema
            .discriminant(&value)
            .map(LiteralValue::EnumTag)
            .unwrap_or(LiteralValue::String(value)),
        (LiteralValue::Nullable(Some(value)), value_type) => LiteralValue::Nullable(Some(
            Box::new(coerce_literal_for_value_type(*value, value_type)),
        )),
        (value, ValueType::Nullable(inner)) => {
            LiteralValue::Nullable(Some(Box::new(coerce_literal_for_value_type(value, inner))))
        }
        (LiteralValue::Array(values), ValueType::Array(inner)) => LiteralValue::Array(
            values
                .into_iter()
                .map(|value| coerce_literal_for_value_type(value, inner))
                .collect(),
        ),
        (LiteralValue::Tuple(values), ValueType::Tuple(types)) if values.len() == types.len() => {
            LiteralValue::Tuple(
                values
                    .into_iter()
                    .zip(types)
                    .map(|(value, value_type)| coerce_literal_for_value_type(value, value_type))
                    .collect(),
            )
        }
        (value, _) => value,
    }
}

fn lower_null_test(
    value: &NormalizedValueRef,
    is_null: bool,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<GroovePredicateExpr, UnsupportedReason> {
    match lower_value_ref(value, source_id, source, request)? {
        LoweredValueRef::Field(field) if is_null => Ok(GroovePredicateExpr::IsNull { field }),
        LoweredValueRef::Field(field) => Ok(GroovePredicateExpr::IsNotNull { field }),
        LoweredValueRef::Literal(LiteralValue::Nullable(None)) => Ok(constant_predicate(is_null)),
        LoweredValueRef::Literal(_) => Ok(constant_predicate(!is_null)),
    }
}

fn predicate_kind(op: ComparisonOp) -> PredicateKind {
    match op {
        ComparisonOp::Eq => PredicateKind::Eq,
        ComparisonOp::Ne => PredicateKind::Neq,
        ComparisonOp::Lt => PredicateKind::Lt,
        ComparisonOp::Lte => PredicateKind::LtEq,
        ComparisonOp::Gt => PredicateKind::Gt,
        ComparisonOp::Gte => PredicateKind::GtEq,
    }
}

fn compare_literals(left: &LiteralValue, op: ComparisonOp, right: &LiteralValue) -> bool {
    match op {
        ComparisonOp::Eq => left == right,
        ComparisonOp::Ne => left != right,
        ComparisonOp::Lt => left < right,
        ComparisonOp::Lte => left <= right,
        ComparisonOp::Gt => left > right,
        ComparisonOp::Gte => left >= right,
    }
}

fn constant_predicate(value: bool) -> GroovePredicateExpr {
    if value {
        GroovePredicateExpr::And(Vec::new())
    } else {
        GroovePredicateExpr::Or(Vec::new())
    }
}

#[derive(Clone, Debug)]
enum LoweredValueRef {
    Field(String),
    Literal(LiteralValue),
}

fn lower_field_ref(
    value: &NormalizedValueRef,
    plan: &LinearCurrentRoot,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
    context: &str,
) -> Result<String, UnsupportedReason> {
    let source_id = plan.root.source().ok_or_else(|| {
        UnsupportedReason::Operator(format!("{context} must be a root source field"))
    })?;
    match lower_value_ref(value, source_id, source, request)? {
        LoweredValueRef::Field(field) => Ok(field),
        LoweredValueRef::Literal(_) => Err(UnsupportedReason::Operator(format!(
            "{context} must be a root source field"
        ))),
    }
}

fn lower_value_ref(
    value: &NormalizedValueRef,
    source_id: &SourceId,
    source: &ResolvedSource,
    request: &QueryProgramRequest,
) -> Result<LoweredValueRef, UnsupportedReason> {
    match value {
        NormalizedValueRef::SourceField {
            source: value_source,
            field,
        } if value_source == source_id && field == "__root_union_arm" => {
            Ok(LoweredValueRef::Field(field.clone()))
        }
        NormalizedValueRef::SourceField {
            source: value_source,
            field,
        } if value_source == source_id => Ok(LoweredValueRef::Field(
            require_source_field(source, field)
                .or_else(|_| require_source_field(source, &user_column_field(field)))?,
        )),
        NormalizedValueRef::SourceField { source, .. } => Err(UnsupportedReason::Operator(
            format!("predicate references unsupported source {:?}", source),
        )),
        NormalizedValueRef::Param(name) => {
            let Some(value) = request.input.binding.values.get(name) else {
                return Err(UnsupportedReason::Operator(format!(
                    "binding parameter '{name}' is not bound"
                )));
            };
            Ok(LoweredValueRef::Literal(value.clone().into()))
        }
        NormalizedValueRef::Claim(path) => {
            let value = claim_value(path, &request.policy)?;
            Ok(LoweredValueRef::Literal(value.into()))
        }
        NormalizedValueRef::FrontierColumn { .. } => Err(UnsupportedReason::Operator(
            "frontier values are not valid in root source predicates".to_owned(),
        )),
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) if value_source == source_id => {
            Ok(LoweredValueRef::Field(require_source_field(
                source,
                &source.row_shape.row_uuid_field,
            )?))
        }
        NormalizedValueRef::RowId(RowIdRef::Source(value_source)) => {
            Err(UnsupportedReason::Operator(format!(
                "predicate references unsupported row id source {:?}",
                value_source
            )))
        }
        NormalizedValueRef::RowId(RowIdRef::Frontier(_)) => Err(UnsupportedReason::Operator(
            "frontier row ids are not valid in root source predicates".to_owned(),
        )),
        NormalizedValueRef::Provenance {
            source: value_source,
            field,
        } if value_source == source_id => Ok(LoweredValueRef::Field(require_source_field(
            source,
            provenance_source_field(*field),
        )?)),
        NormalizedValueRef::Provenance { source, .. } => Err(UnsupportedReason::Operator(format!(
            "predicate references unsupported provenance source {:?}",
            source
        ))),
        NormalizedValueRef::Literal(bytes) => {
            let value = postcard::from_bytes::<Value>(bytes).map_err(|err| {
                UnsupportedReason::Operator(format!("literal value could not be decoded: {err}"))
            })?;
            Ok(LoweredValueRef::Literal(value.into()))
        }
    }
}

pub(super) fn claim_value(
    path: &ClaimPath,
    policy: &PolicyContext,
) -> Result<Value, UnsupportedReason> {
    let (permission_subject, claims) = match policy {
        PolicyContext::Identity {
            permission_subject,
            claims,
            ..
        }
        | PolicyContext::AuthorizationSubplan {
            permission_subject,
            claims,
            ..
        } => (permission_subject, claims),
        PolicyContext::System => {
            return Err(UnsupportedReason::Operator(
                "claim values require an identity policy context".to_owned(),
            ));
        }
    };
    if let Some(name) = crate::query::author_claim_path_key(&path.0) {
        return crate::tools::policy_claims::author_policy_claims(*permission_subject)
            .remove(&name)
            .ok_or_else(|| UnsupportedReason::UnboundClaim(path.clone()));
    }
    let name = match path.0.as_slice() {
        [name] => name.clone(),
        [claims, name] if claims == "claims" => crate::query::provider_claim_key(name),
        _ => {
            return Err(UnsupportedReason::Operator(
                "unsupported session claim path".to_owned(),
            ));
        }
    };
    if let Some(value) = claims.get(&name) {
        return Ok(value.clone());
    }
    match name.as_str() {
        "user" => Ok(permission_subject.to_value()),
        _ => Err(UnsupportedReason::UnboundClaim(path.clone())),
    }
}

fn is_unbound_claim_reason(reason: &UnsupportedReason) -> bool {
    matches!(reason, UnsupportedReason::UnboundClaim(_))
}

pub(super) fn require_source_field(
    source: &ResolvedSource,
    field: &str,
) -> Result<String, UnsupportedReason> {
    let Some(index) = resolved_source_descriptor_index(source, field) else {
        return Err(UnsupportedReason::Operator(format!(
            "resolved source {:?} does not provide field '{field}'",
            source.row_shape.source
        )));
    };
    source
        .row_shape
        .descriptor
        .fields()
        .get(index)
        .and_then(|field| field.name.clone())
        .ok_or_else(|| {
            UnsupportedReason::Operator(format!(
                "resolved source {:?} field '{field}' has no carrier name",
                source.row_shape.source
            ))
        })
}

fn provenance_source_field(field: ProvenanceField) -> &'static str {
    match field {
        ProvenanceField::CreatedAt => "$createdAt",
        ProvenanceField::CreatedBy => "$createdBy",
        ProvenanceField::UpdatedAt => "$updatedAt",
        ProvenanceField::UpdatedBy => "$updatedBy",
    }
}

pub(super) fn has_explicit_closure_path(shape: &NormalizedRowSetShape) -> bool {
    shape
        .closure_paths
        .iter()
        .any(|path| matches!(path, ClosurePath::ExplicitInclude { .. }))
}
