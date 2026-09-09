/// Validated query shape with inferred parameter types.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ValidatedQuery {
    query: Query,
    schema_version: SchemaVersionId,
    params: BTreeMap<String, ColumnType>,
    canonical: Vec<u8>,
    shape_id: ShapeId,
}

impl ValidatedQuery {
    /// Shape id derived from canonical AST bytes.
    pub fn shape_id(&self) -> ShapeId {
        self.shape_id
    }

    /// Canonical AST bytes.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }

    /// Schema version this query was authored and validated against.
    pub fn schema_version(&self) -> SchemaVersionId {
        self.schema_version
    }

    /// Inferred parameter types by name.
    pub fn params(&self) -> &BTreeMap<String, ColumnType> {
        &self.params
    }

    /// Original AST normalized into canonical order.
    pub fn query(&self) -> &Query {
        &self.query
    }

    /// Validate a binding against this shape.
    pub fn bind(&self, values: BTreeMap<String, Value>) -> Result<Binding, QueryError> {
        validate_binding_values(&self.params, values)
    }
}

fn validate_binding_values(
    params: &BTreeMap<String, ColumnType>,
    values: BTreeMap<String, Value>,
) -> Result<Binding, QueryError> {
    for required in params.keys() {
        if !values.contains_key(required) {
            return Err(QueryError::MissingParam(required.clone()));
        }
    }
    for (name, value) in &values {
        let Some(expected) = params.get(name) else {
            return Err(QueryError::UnknownParam(name.clone()));
        };
        if !value_matches_type(value, expected) {
            return Err(QueryError::ParamTypeMismatch {
                param: name.clone(),
                expected: expected.clone(),
            });
        }
    }
    let canonical = canonical_binding_bytes(&values);
    Ok(Binding { values, canonical })
}

/// Validated binding values for a query shape.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Binding {
    values: BTreeMap<String, Value>,
    canonical: Vec<u8>,
}

impl Binding {
    /// Binding id derived from canonical binding bytes.
    pub fn binding_id(&self) -> BindingId {
        BindingId(uuid::Uuid::new_v5(&QUERY_NAMESPACE, &self.canonical))
    }

    /// Canonical binding bytes.
    pub fn canonical_bytes(&self) -> &[u8] {
        &self.canonical
    }

    /// Bound values by parameter name.
    pub fn values(&self) -> &BTreeMap<String, Value> {
        &self.values
    }
}

pub(crate) fn binding_id_for_values(values: &BTreeMap<String, Value>) -> BindingId {
    BindingId(uuid::Uuid::new_v5(
        &QUERY_NAMESPACE,
        &canonical_binding_bytes(values),
    ))
}

/// Query validation error.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum QueryError {
    /// Aggregate aliases cannot occupy compiler-owned output names.
    #[error("aggregate alias {0} uses a reserved compiler namespace")]
    ReservedAggregateAlias(String),
    /// Flat tuple output currently has a deliberately narrow executable envelope.
    #[error("flat join cannot be combined with {feature}")]
    UnsupportedFlatJoinCombination {
        /// Unsupported feature present on the same query.
        feature: String,
    },
    /// Referenced table does not exist.
    #[error("unknown table {0}")]
    UnknownTable(String),
    /// Referenced column does not exist.
    #[error("unknown column {table}.{column}")]
    UnknownColumn {
        /// Table name.
        table: String,
        /// Column name.
        column: String,
    },
    /// Operand types do not match.
    #[error("operand type mismatch")]
    OperandTypeMismatch,
    /// An `in` candidate does not match its column's whole-value type.
    #[error(
        "in candidate for column {column} has type {candidate_type:?}, but the column has type {column_type:?}"
    )]
    InCandidateTypeMismatch {
        /// Column on the left side of the `in` predicate.
        column: String,
        /// Declared type of that column.
        column_type: Box<ColumnType>,
        /// Type of the mismatched candidate.
        candidate_type: Box<ColumnType>,
    },
    /// Claim and column operand types do not match.
    #[error(
        "claim {claim_path} has type {claim_type:?}, but column {column} has type {column_type:?}"
    )]
    ClaimTypeMismatch {
        /// Claim path.
        claim_path: String,
        /// Column name.
        column: String,
        /// Claim type.
        claim_type: String,
        /// Column type.
        column_type: String,
    },
    /// Relation facade syntax is not yet covered by unified lowering.
    #[error("unsupported relation query: {0}")]
    UnsupportedRelationQuery(String),
    /// Parameter was inferred with incompatible types.
    #[error("parameter {param} inferred with incompatible type")]
    ParamTypeConflict {
        /// Parameter name.
        param: String,
    },
    /// Binding omitted a required parameter.
    #[error("missing parameter {0}")]
    MissingParam(String),
    /// Binding supplied an unknown parameter.
    #[error("unknown parameter {0}")]
    UnknownParam(String),
    /// Binding value had the wrong type.
    #[error("parameter {param} has wrong type")]
    ParamTypeMismatch {
        /// Parameter name.
        param: String,
        /// Expected type.
        expected: ColumnType,
    },
    /// Join column is not a reference to the current table.
    #[error("join column {join_table}.{column} does not reference {target_table}")]
    JoinNotRefCompatible {
        /// Junction table.
        join_table: String,
        /// Column name.
        column: String,
        /// Expected target table.
        target_table: String,
    },
    /// Include path did not resolve through reference metadata.
    #[error("bad include path {path}")]
    BadIncludePath {
        /// Include path.
        path: String,
    },
    /// Permission-introspection columns are not executable yet.
    #[error(
        "unsupported query magic column {column}: permission introspection columns are not executable yet"
    )]
    UnsupportedMagicColumn {
        /// Column name.
        column: String,
    },
    /// Portable author identities deliberately have no public ordering.
    #[error("ordering by author provenance column {column} is unsupported")]
    UnsupportedAuthorOrdering {
        /// Author provenance column named as an ordering key.
        column: String,
    },
}

fn validate_query(query: &Query, schema: &RuntimeSchema) -> Result<ValidatedQuery, QueryError> {
    let schema_version = schema.version_id();
    validate_query_with_schema_version(query, schema, schema_version)
}

fn validate_query_with_schema_version(
    query: &Query,
    schema: &RuntimeSchema,
    schema_version: SchemaVersionId,
) -> Result<ValidatedQuery, QueryError> {
    let (normalized, params, canonical) = validate_query_canonical_parts(query, schema)?;
    let mut shape_identity = canonical.clone();
    shape_identity.extend_from_slice(schema_version.as_bytes());
    let shape_id = ShapeId(uuid::Uuid::new_v5(&QUERY_NAMESPACE, &shape_identity));
    Ok(ValidatedQuery {
        query: normalized,
        schema_version,
        params,
        canonical,
        shape_id,
    })
}

type ValidatedQueryCanonicalParts = (Query, BTreeMap<String, ColumnType>, Vec<u8>);

fn validate_query_canonical_parts(
    query: &Query,
    schema: &RuntimeSchema,
) -> Result<ValidatedQueryCanonicalParts, QueryError> {
    let root = schema_table(schema, &query.table)?;
    let mut resolved_query = query.clone();
    let mut params = BTreeMap::new();
    if let Some(relation) = &query.relation {
        validate_retained_relation_outer_query(query)?;
        if relation_union_parts(&relation.rel).is_none() {
            let mut resolved = relation_query_to_query(relation)?;
            if resolved.table != query.table {
                return Err(QueryError::UnsupportedRelationQuery(
                    "relation query output table does not match its Query envelope".to_owned(),
                ));
            }
            resolved.array_subqueries = query.array_subqueries.clone();
            resolved.select = query.select.clone();
            return validate_query_canonical_parts(&resolved, schema);
        }
        validate_retained_relation_union(relation, &query.table, schema, &mut params)?;
        validate_array_subqueries(
            schema,
            &root,
            &mut resolved_query.array_subqueries,
            &mut params,
        )?;
        if let Some(select) = &query.select {
            for column in select {
                validate_select_column(&root, column)?;
            }
        }
        let normalized = normalize_query(&resolved_query);
        let canonical = canonical_query_bytes_for_schema(&normalized, schema)?;
        return Ok((normalized, params, canonical));
    }
    for join in &mut resolved_query.joins {
        validate_join(schema, &root, &query.table, join, &mut params)?;
    }
    if let Some(flat_join) = &query.flat_join {
        let unsupported = [
            (flat_join.sources.is_empty(), "zero joined sources"),
            (!query.joins.is_empty(), "existential joins"),
            (!query.policy_branches.is_empty(), "policy branches"),
            (!query.reachable.is_empty(), "reachability"),
            (!query.inherits.is_empty(), "inherited-policy traversals"),
            (!query.includes.is_empty(), "includes"),
            (!query.array_subqueries.is_empty(), "array subqueries"),
            (query.select.is_some(), "select projections"),
            (!query.order_by.is_empty(), "ordering"),
            (query.aggregate.is_some(), "aggregates"),
            (query.limit.is_some(), "limits"),
            (query.offset != 0, "offsets"),
        ]
        .into_iter()
        .find_map(|(present, feature)| present.then_some(feature));
        if let Some(feature) = unsupported {
            return Err(QueryError::UnsupportedFlatJoinCombination {
                feature: feature.to_owned(),
            });
        }
        validate_flat_join(schema, &query.table, flat_join)?;
        let sources = flat_join_source_tables(schema, &query.table, flat_join)?;
        for predicate in &mut resolved_query.filters {
            qualify_flat_join_predicate(predicate, &sources)?;
            validate_flat_join_filter_routing(predicate)?;
        }
        let filter_schema = flat_join_filter_schema(&sources)?;
        for predicate in &mut resolved_query.filters {
            validate_predicate(&filter_schema, predicate, &mut params)?;
        }
    } else {
        for predicate in &mut resolved_query.filters {
            validate_predicate(&root, predicate, &mut params)?;
        }
    }
    for reachable in &mut resolved_query.reachable {
        validate_reachable(schema, &root, reachable, &mut params)?;
    }
    for inherits in &query.inherits {
        validate_inherits(&root, inherits)?;
    }
    for branch in &mut resolved_query.policy_branches {
        for predicate in &mut branch.filters {
            validate_predicate(&root, predicate, &mut params)?;
        }
        for join in &mut branch.joins {
            validate_join(schema, &root, &query.table, join, &mut params)?;
        }
        for reachable in &mut branch.reachable {
            validate_reachable(schema, &root, reachable, &mut params)?;
        }
        for inherits in &branch.inherits {
            validate_inherits(&root, inherits)?;
        }
    }
    for include in &query.includes {
        validate_include(schema, &root, &include.path)?;
    }
    validate_array_subqueries(
        schema,
        &root,
        &mut resolved_query.array_subqueries,
        &mut params,
    )?;
    if let Some(select) = &query.select {
        for column in select {
            validate_select_column(&root, column)?;
        }
    }
    reject_author_ordering(&query.order_by)?;
    if let Some(aggregate) = &query.aggregate {
        validate_aggregate(&root, aggregate)?;
        validate_aggregate_order_by(&query.table, aggregate, &query.order_by)?;
    } else {
        for order in &query.order_by {
            planner_column_type(&root, &order.column)?;
        }
    }
    let normalized = normalize_query(&resolved_query);
    let canonical = canonical_query_bytes_for_schema(&normalized, schema)?;
    Ok((normalized, params, canonical))
}

/// Relation syntax owns row membership and its terminal ordering/window. The
/// outer Query envelope may still describe the returned row shape through
/// select projections and array subqueries; reject other clauses because they
/// would compete with or disappear behind the relation row-set syntax.
fn validate_retained_relation_outer_query(query: &Query) -> Result<(), QueryError> {
    let has_outer_clause = !query.filters.is_empty()
        || !query.joins.is_empty()
        || query.flat_join.is_some()
        || !query.policy_branches.is_empty()
        || !query.reachable.is_empty()
        || !query.inherits.is_empty()
        || !query.includes.is_empty()
        || !query.order_by.is_empty()
        || query.aggregate.is_some()
        || query.limit.is_some()
        || query.offset != 0;
    if has_outer_clause {
        return Err(QueryError::UnsupportedRelationQuery(
            "relation query cannot be combined with ordinary query clauses".to_owned(),
        ));
    }
    Ok(())
}

/// Validate every relation UNION arm through the established single-relation
/// facade, then merge its inferred parameter domain. The outer union itself
/// stays retained for row-set normalization so its labels remain observable.
fn validate_retained_relation_union(
    relation: &RelationQuery,
    output_table: &str,
    schema: &RuntimeSchema,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let Some(parts) = relation_union_parts(&relation.rel) else {
        return Err(QueryError::UnsupportedRelationQuery(
            "retained relation query must be a union with global terminal operators in builder order"
                .to_owned(),
        ));
    };
    let inputs = parts.inputs;
    if inputs.is_empty() {
        return Err(QueryError::UnsupportedRelationQuery(
            "union requires at least one input".to_owned(),
        ));
    }
    if let Some(terms) = parts.order_by {
        for term in terms {
            if term
                .column
                .scope
                .as_deref()
                .is_some_and(|scope| scope != output_table)
            {
                return Err(QueryError::UnsupportedRelationQuery(
                    "union order_by must be scoped to the union output table".to_owned(),
                ));
            }
            let table = schema_table(schema, output_table)?;
            planner_column_type(&table, &term.column.column)?;
            reject_author_ordering(&[OrderBy {
                column: term.column.column,
                direction: term.direction,
            }])?;
        }
    }
    let mut labels = BTreeSet::new();
    for arm in inputs {
        if arm.label.is_empty()
            || arm.label.len() > 4096
            || arm.label.contains('\0')
            || !labels.insert(arm.label.clone())
        {
            return Err(QueryError::UnsupportedRelationQuery(
                "union arm labels must be 1..=4096 bytes, NUL-free, and unique".to_owned(),
            ));
        }
        let arm_query = relation_query_to_query(&RelationQuery {
            rel: arm.input.clone(),
        })?;
        if arm_query.table != output_table {
            return Err(QueryError::UnsupportedRelationQuery(
                "union inputs must output the same table".to_owned(),
            ));
        }
        let (_, arm_params, _) = validate_query_canonical_parts(&arm_query, schema)?;
        for (name, ty) in arm_params {
            match params.entry(name) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(ty);
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get() == &ty => {}
                std::collections::btree_map::Entry::Occupied(entry) => {
                    return Err(QueryError::ParamTypeConflict {
                        param: entry.key().clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

fn reject_author_ordering(order_by: &[OrderBy]) -> Result<(), QueryError> {
    if let Some(order) = order_by
        .iter()
        .find(|order| matches!(order.column.as_str(), "$createdBy" | "$updatedBy"))
    {
        return Err(QueryError::UnsupportedAuthorOrdering {
            column: order.column.clone(),
        });
    }
    Ok(())
}

fn flat_join_source_tables(
    schema: &RuntimeSchema,
    root_table: &str,
    flat_join: &FlatJoin,
) -> Result<BTreeMap<String, TableSchema>, QueryError> {
    let root_name = flat_join_source_name(root_table, &flat_join.root_alias);
    let mut sources = BTreeMap::from([(root_name, schema_table(schema, root_table)?)]);
    for source in &flat_join.sources {
        let name = flat_join_source_name(&source.table, &source.alias);
        if sources
            .insert(name.clone(), schema_table(schema, &source.table)?)
            .is_some()
        {
            return Err(QueryError::UnknownColumn {
                table: "flat join duplicate source".to_owned(),
                column: name,
            });
        }
    }
    Ok(sources)
}

fn flat_join_filter_schema(
    sources: &BTreeMap<String, TableSchema>,
) -> Result<TableSchema, QueryError> {
    let mut columns = Vec::new();
    for (scope, table) in sources {
        // `id` retains its normal effective-column semantics: a declared
        // field wins, and only legacy tables expose their physical row UUID
        // through that spelling. `_id` is the explicit physical-row alias.
        columns.push(JazzColumnSchema::new(
            format!("{scope}._id"),
            ColumnType::Uuid,
        ));
        if !has_declared_id(table) {
            columns.push(JazzColumnSchema::new(
                format!("{scope}.id"),
                ColumnType::Uuid,
            ));
        }
        for magic in ["$createdBy", "$updatedBy", "$createdAt", "$updatedAt"] {
            columns.push(JazzColumnSchema::new(
                format!("{scope}.{magic}"),
                executable_magic_column_type(magic)?
                    .expect("listed flat-join magic columns are executable")
                    .clone(),
            ));
        }
        columns.extend(table.columns.iter().map(|column| {
            JazzColumnSchema::new(
                format!("{scope}.{}", column.name),
                column.column_type.clone(),
            )
        }));
    }
    Ok(TableSchema::new("flat join", columns))
}

fn qualify_flat_join_predicate(
    predicate: &mut Predicate,
    sources: &BTreeMap<String, TableSchema>,
) -> Result<(), QueryError> {
    rewrite_flat_join_predicate_columns(predicate, &mut |column| {
        qualify_flat_join_column(column, sources)
    })
}

fn rewrite_flat_join_predicate_columns(
    predicate: &mut Predicate,
    rewrite: &mut impl FnMut(&str) -> Result<String, QueryError>,
) -> Result<(), QueryError> {
    let mut rewrite_operand = |operand: &mut Operand| -> Result<(), QueryError> {
        let Operand::Column(column) = operand else {
            return Ok(());
        };
        *column = rewrite(column)?;
        Ok(())
    };
    match predicate {
        Predicate::All(predicates) | Predicate::Any(predicates) => predicates
            .iter_mut()
            .try_for_each(|predicate| rewrite_flat_join_predicate_columns(predicate, rewrite)),
        Predicate::Not(predicate) => rewrite_flat_join_predicate_columns(predicate, rewrite),
        Predicate::Eq(left, right)
        | Predicate::Ne(left, right)
        | Predicate::Gt(left, right)
        | Predicate::Gte(left, right)
        | Predicate::Lt(left, right)
        | Predicate::Lte(left, right)
        | Predicate::Contains(left, right) => {
            rewrite_operand(left)?;
            rewrite_operand(right)
        }
        Predicate::In(value, options) => {
            rewrite_operand(value)?;
            options.iter_mut().try_for_each(rewrite_operand)
        }
        Predicate::EnumMatch { column, .. } => {
            *column = rewrite(column)?;
            Ok(())
        }
        Predicate::IsNull(operand) => rewrite_operand(operand),
    }
}

pub(crate) fn flat_join_predicate_sources(
    predicate: &Predicate,
) -> Result<BTreeSet<String>, QueryError> {
    let mut predicate = predicate.clone();
    let mut sources = BTreeSet::new();
    rewrite_flat_join_predicate_columns(&mut predicate, &mut |column| {
        let (scope, _) = flat_join_qualified_field(column)?;
        sources.insert(scope.to_owned());
        Ok(column.to_owned())
    })?;
    Ok(sources)
}

pub(crate) fn unqualify_flat_join_predicate(
    predicate: &Predicate,
    expected_scope: &str,
) -> Result<Predicate, QueryError> {
    let mut predicate = predicate.clone();
    rewrite_flat_join_predicate_columns(&mut predicate, &mut |column| {
        let (scope, field) = flat_join_qualified_field(column)?;
        if scope != expected_scope {
            return Err(QueryError::UnsupportedFlatJoinCombination {
                feature: "filters spanning multiple sources".to_owned(),
            });
        }
        Ok(field.to_owned())
    })?;
    Ok(predicate)
}

pub(crate) fn qualify_flat_join_source_predicate(
    mut predicate: Predicate,
    scope: &str,
) -> Result<Predicate, QueryError> {
    rewrite_flat_join_predicate_columns(&mut predicate, &mut |column| {
        Ok(format!("{scope}.{column}"))
    })?;
    Ok(predicate)
}

fn validate_flat_join_filter_routing(predicate: &Predicate) -> Result<(), QueryError> {
    if let Predicate::All(predicates) = predicate {
        return predicates
            .iter()
            .try_for_each(validate_flat_join_filter_routing);
    }
    if flat_join_predicate_sources(predicate)?.len() > 1 {
        return Err(QueryError::UnsupportedFlatJoinCombination {
            feature: "filters spanning multiple sources".to_owned(),
        });
    }
    Ok(())
}

fn qualify_flat_join_column(
    column: &str,
    sources: &BTreeMap<String, TableSchema>,
) -> Result<String, QueryError> {
    if let Ok((scope, field)) = flat_join_qualified_field(column) {
        let table = sources
            .get(scope)
            .ok_or_else(|| QueryError::UnknownColumn {
                table: "flat join source".to_owned(),
                column: scope.to_owned(),
            })?;
        flat_join_column_type(table, field)?;
        return Ok(column.to_owned());
    }

    let mut matches = sources
        .iter()
        .filter(|(_, table)| flat_join_column_type(table, column).is_ok())
        .map(|(scope, _)| scope.clone());
    let Some(scope) = matches.next() else {
        return Err(QueryError::UnknownColumn {
            table: "flat join".to_owned(),
            column: column.to_owned(),
        });
    };
    if matches.next().is_some() {
        return Err(QueryError::UnknownColumn {
            table: "ambiguous flat join column; qualify its source".to_owned(),
            column: column.to_owned(),
        });
    }
    Ok(format!("{scope}.{column}"))
}

fn flat_join_source_name(table: &str, alias: &Option<String>) -> String {
    alias.clone().unwrap_or_else(|| table.to_owned())
}

fn flat_join_qualified_field(field: &str) -> Result<(&str, &str), QueryError> {
    if let Some(index) = field.find(".$") {
        return Ok((&field[..index], &field[index + 1..]));
    }
    field
        .rsplit_once('.')
        .ok_or_else(|| QueryError::UnknownColumn {
            table: "flat join qualified source".to_owned(),
            column: field.to_owned(),
        })
}

fn flat_join_column_type<'a>(
    table: &'a TableSchema,
    column: &str,
) -> Result<&'a ColumnType, QueryError> {
    if column == "_id" {
        Ok(&ColumnType::Uuid)
    } else {
        planner_column_type(table, column)
    }
}

fn validate_flat_join(
    schema: &RuntimeSchema,
    root_table: &str,
    flat_join: &FlatJoin,
) -> Result<(), QueryError> {
    let mut sources = BTreeMap::new();
    let root_name = flat_join_source_name(root_table, &flat_join.root_alias);
    sources.insert(root_name, root_table.to_owned());

    for source in &flat_join.sources {
        let name = flat_join_source_name(&source.table, &source.alias);
        if sources.contains_key(&name) {
            return Err(QueryError::UnknownColumn {
                table: "flat join duplicate source".to_owned(),
                column: name,
            });
        }
        schema_table(schema, &source.table)?;
        let (left_source, left_column) = flat_join_qualified_field(&source.on.left)?;
        let (right_source, right_column) = flat_join_qualified_field(&source.on.right)?;
        let left_table = sources
            .get(left_source)
            .ok_or_else(|| QueryError::UnknownColumn {
                table: "flat join accumulated source".to_owned(),
                column: left_source.to_owned(),
            })?;
        if right_source != name {
            return Err(QueryError::UnknownColumn {
                table: "flat join source".to_owned(),
                column: right_source.to_owned(),
            });
        }
        let left_schema = schema_table(schema, left_table)?;
        let right_schema = schema_table(schema, &source.table)?;
        if !flat_join_key_types_compatible(
            flat_join_column_type(&left_schema, left_column)?,
            flat_join_column_type(&right_schema, right_column)?,
        ) {
            return Err(QueryError::OperandTypeMismatch);
        }
        sources.insert(name, source.table.clone());
    }
    Ok(())
}

fn validate_join(
    schema: &RuntimeSchema,
    root: &TableSchema,
    root_table: &str,
    join: &mut JoinVia,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let join_table = schema_table(schema, &join.table)?;
    match join.target {
        JoinTarget::Column => {
            planner_column_type(&join_table, &join.on_column)?;
        }
        JoinTarget::RowId => {
            if join.on_column != "id" {
                return Err(QueryError::UnknownColumn {
                    table: join.table.clone(),
                    column: join.on_column.clone(),
                });
            }
        }
    }
    let target_table = if let Some(lookup) = &join.source_lookup {
        planner_column_type(root, &lookup.row_id_source_column)?;
        let lookup_table = schema_table(schema, &lookup.table)?;
        match root.references.get(&lookup.row_id_source_column) {
            Some(target) if target == &lookup.table => {}
            _ => {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: root_table.to_owned(),
                    column: lookup.row_id_source_column.clone(),
                    target_table: lookup.table.clone(),
                });
            }
        }
        planner_column_type(&lookup_table, &lookup.value_column)?;
        if lookup.value_column == "id"
            && has_declared_id(&lookup_table)
            && planner_column_type(&lookup_table, &lookup.value_column)?
                != planner_column_type(&join_table, &join.on_column)?
        {
            return Err(QueryError::OperandTypeMismatch);
        }
        if join.source_column.as_deref() != Some(lookup.value_column.as_str()) {
            return Err(QueryError::JoinNotRefCompatible {
                join_table: lookup.table.clone(),
                column: lookup.value_column.clone(),
                target_table: "join source column".to_owned(),
            });
        }
        if lookup.value_column == "id" {
            lookup.table.clone()
        } else {
            lookup_table
                .references
                .get(&lookup.value_column)
                .cloned()
                .ok_or_else(|| QueryError::JoinNotRefCompatible {
                    join_table: lookup.table.clone(),
                    column: lookup.value_column.clone(),
                    target_table: "referenced table".to_owned(),
                })?
        }
    } else if let Some(source_column) = &join.source_column {
        if source_column == "id" {
            if has_declared_id(root)
                && planner_column_type(root, source_column)?
                    != planner_column_type(&join_table, &join.on_column)?
            {
                return Err(QueryError::OperandTypeMismatch);
            }
            root_table.to_owned()
        } else {
            planner_column_type(root, source_column)?;
            root.references.get(source_column).cloned().ok_or_else(|| {
                QueryError::JoinNotRefCompatible {
                    join_table: root_table.to_owned(),
                    column: source_column.clone(),
                    target_table: "referenced table".to_owned(),
                }
            })?
        }
    } else {
        root_table.to_owned()
    };
    for correlation in &join.correlated_filters {
        let source_type = planner_column_type(root, &correlation.source_column)?;
        let join_type = planner_column_type(&join_table, &correlation.join_column)?;
        if source_type != join_type {
            return Err(QueryError::OperandTypeMismatch);
        }
    }
    match join.target {
        JoinTarget::Column => match join_table.references.get(&join.on_column) {
            Some(target) if target == &target_table => {}
            None if join.on_column == "id" && join.table == target_table => {}
            _ => {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: join.table.clone(),
                    column: join.on_column.clone(),
                    target_table: target_table.to_owned(),
                });
            }
        },
        JoinTarget::RowId => {
            if join.table != target_table {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: join.table.clone(),
                    column: join.on_column.clone(),
                    target_table: target_table.to_owned(),
                });
            }
        }
    }
    for predicate in &mut join.filters {
        validate_predicate(&join_table, predicate, params)?;
    }
    for nested in &mut join.nested_joins {
        validate_join(schema, &join_table, &join.table, nested, params)?;
    }
    Ok(())
}

fn validate_aggregate(table: &TableSchema, aggregate: &AggregateQuery) -> Result<(), QueryError> {
    if let Some(group_by) = &aggregate.group_by {
        planner_column_type(table, group_by)?;
    }
    for aggregate in &aggregate.aggregates {
        if aggregate.alias.starts_with("__jazz_aggregate_") {
            return Err(QueryError::ReservedAggregateAlias(aggregate.alias.clone()));
        }
        match aggregate.function {
            AggregateFunction::Count => {
                if let Some(column) = &aggregate.column {
                    column_type(table, column)?;
                }
            }
            AggregateFunction::Sum | AggregateFunction::Avg => {
                let Some(column) = &aggregate.column else {
                    return Err(QueryError::OperandTypeMismatch);
                };
                if !is_numeric(column_type(table, column)?) {
                    return Err(QueryError::OperandTypeMismatch);
                }
            }
            AggregateFunction::Min | AggregateFunction::Max => {
                let Some(column) = &aggregate.column else {
                    return Err(QueryError::OperandTypeMismatch);
                };
                if !is_orderable(column_type(table, column)?) {
                    return Err(QueryError::OperandTypeMismatch);
                }
            }
        }
    }
    Ok(())
}

fn validate_aggregate_order_by(
    table: &str,
    aggregate: &AggregateQuery,
    order_by: &[OrderBy],
) -> Result<(), QueryError> {
    for order in order_by {
        let is_group_by = aggregate.group_by.as_deref() == Some(order.column.as_str());
        let is_aggregate = aggregate
            .aggregates
            .iter()
            .any(|aggregate| aggregate.alias == order.column);
        if !is_group_by && !is_aggregate {
            return Err(QueryError::UnknownColumn {
                table: format!("{table}_aggregate"),
                column: order.column.clone(),
            });
        }
    }
    Ok(())
}

fn validate_select_column(table: &TableSchema, column: &str) -> Result<(), QueryError> {
    match column {
        "id" => Ok(()),
        name if executable_magic_column_type(name)?.is_some() => Ok(()),
        name if name.starts_with('$') => Err(QueryError::UnknownColumn {
            table: table.name.clone(),
            column: name.to_owned(),
        }),
        name => column_type(table, name).map(|_| ()),
    }
}

fn schema_table(schema: &RuntimeSchema, name: &str) -> Result<TableSchema, QueryError> {
    schema
        .tables
        .iter()
        .find(|table| table.name == name)
        .cloned()
        .ok_or_else(|| QueryError::UnknownTable(name.to_owned()))
}

fn column_type<'a>(table: &'a TableSchema, column: &str) -> Result<&'a ColumnType, QueryError> {
    column_schema(table, column).map(|column| &column.column_type)
}

fn column_schema<'a>(
    table: &'a TableSchema,
    column: &str,
) -> Result<&'a JazzColumnSchema, QueryError> {
    table
        .columns
        .iter()
        .find(|candidate| candidate.name == column)
        .ok_or_else(|| QueryError::UnknownColumn {
            table: table.name.clone(),
            column: column.to_owned(),
        })
}

fn planner_column_type<'a>(
    table: &'a TableSchema,
    column: &str,
) -> Result<&'a ColumnType, QueryError> {
    if let Ok(column_schema) = column_schema(table, column) {
        return Ok(&column_schema.column_type);
    }
    if column == "id" {
        return Ok(&ColumnType::Uuid);
    }
    if let Some(column_type) = executable_magic_column_type(column)? {
        return Ok(column_type);
    }
    Ok(&column_schema(table, column)?.column_type)
}

fn has_declared_id(table: &TableSchema) -> bool {
    table.columns.iter().any(|column| column.name == "id")
}

fn executable_magic_column_type(column: &str) -> Result<Option<&'static ColumnType>, QueryError> {
    if let Some(ty) = crate::ids::AuthorSubject::metadata_type(column) {
        return Ok(Some(ty));
    }
    if is_permission_introspection_magic_column(column) {
        return Err(QueryError::UnsupportedMagicColumn {
            column: column.to_owned(),
        });
    }
    match column {
        "$createdAt" | "$updatedAt" => Ok(Some(&ColumnType::U64)),
        _ => Ok(None),
    }
}

fn is_permission_introspection_magic_column(column: &str) -> bool {
    matches!(column, "$canRead")
}

fn validate_include(
    schema: &RuntimeSchema,
    root: &TableSchema,
    path: &str,
) -> Result<(), QueryError> {
    let mut current = root.clone();
    for segment in path.split('.') {
        column_type(&current, segment)?;
        let Some(target) = current.references.get(segment) else {
            return Err(QueryError::BadIncludePath {
                path: path.to_owned(),
            });
        };
        current = schema_table(schema, target)?;
    }
    Ok(())
}

fn validate_array_subqueries(
    schema: &RuntimeSchema,
    parent: &TableSchema,
    subqueries: &mut [ArraySubquery],
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let mut names = std::collections::BTreeSet::new();
    for subquery in subqueries.iter() {
        if subquery.column_name.is_empty() || !names.insert(subquery.column_name.clone()) {
            return Err(QueryError::BadIncludePath {
                path: subquery.column_name.clone(),
            });
        }
    }
    for subquery in subqueries {
        let relation_path = subquery.column_name.clone();
        validate_array_subquery(schema, parent, subquery, params, &relation_path)?;
    }
    Ok(())
}

fn validate_array_subquery(
    schema: &RuntimeSchema,
    parent: &TableSchema,
    subquery: &mut ArraySubquery,
    params: &mut BTreeMap<String, ColumnType>,
    relation_path: &str,
) -> Result<(), QueryError> {
    let child = schema_table(schema, &subquery.table)?;
    let parent_type = planner_column_type(parent, &subquery.outer_column)?;
    let child_type = planner_column_type(&child, &subquery.inner_column)?;
    if !array_correlation_types_compatible(parent_type, child_type) {
        return Err(QueryError::OperandTypeMismatch);
    }
    for predicate in &mut subquery.filters {
        validate_predicate(&child, predicate, params)?;
    }
    if let Some(select) = &subquery.select {
        for column in select {
            validate_select_column(&child, column)?;
        }
    }
    reject_author_ordering(&subquery.order_by)?;
    for order in &subquery.order_by {
        planner_column_type(&child, &order.column)?;
    }
    let mut names = std::collections::BTreeSet::new();
    for nested in &mut subquery.nested_arrays {
        if nested.column_name.is_empty() || !names.insert(nested.column_name.clone()) {
            return Err(QueryError::BadIncludePath {
                path: nested.column_name.clone(),
            });
        }
        let nested_path = format!("{relation_path}.{}", nested.column_name);
        validate_array_subquery(schema, &child, nested, params, &nested_path)?;
    }
    Ok(())
}

fn validate_reachable(
    schema: &RuntimeSchema,
    root: &TableSchema,
    reachable: &mut ReachableVia,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let access = schema_table(schema, &reachable.access_table)?;
    planner_column_type(&access, &reachable.access_row_column)?;
    planner_column_type(&access, &reachable.access_team_column)?;
    let root_key_type = if has_declared_id(root) {
        planner_column_type(root, "id")?
    } else {
        &ColumnType::Uuid
    };
    if reachable.access_row_column == "id" && !has_declared_id(&access) {
        if access.name != root.name {
            return Err(QueryError::JoinNotRefCompatible {
                join_table: reachable.access_table.clone(),
                column: reachable.access_row_column.clone(),
                target_table: root.name.clone(),
            });
        }
    } else if access.name == root.name {
        let access_column_type = planner_column_type(&access, &reachable.access_row_column)?;
        if !column_types_comparable(access_column_type, root_key_type) {
            return Err(QueryError::JoinNotRefCompatible {
                join_table: reachable.access_table.clone(),
                column: reachable.access_row_column.clone(),
                target_table: root.name.clone(),
            });
        }
    } else {
        match access.references.get(&reachable.access_row_column) {
            Some(target) if target == &root.name => {}
            _ => {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: reachable.access_table.clone(),
                    column: reachable.access_row_column.clone(),
                    target_table: root.name.clone(),
                });
            }
        }
        if !column_types_comparable(
            planner_column_type(&access, &reachable.access_row_column)?,
            root_key_type,
        ) {
            return Err(QueryError::OperandTypeMismatch);
        }
    }
    let team_table = match reachable.access_team_target {
        JoinTarget::Column => access
            .references
            .get(&reachable.access_team_column)
            .ok_or_else(|| QueryError::JoinNotRefCompatible {
                join_table: reachable.access_table.clone(),
                column: reachable.access_team_column.clone(),
                target_table: "referenced table".to_owned(),
            })?,
        JoinTarget::RowId => {
            if reachable.access_team_column != "id" {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: reachable.access_table.clone(),
                    column: reachable.access_team_column.clone(),
                    target_table: reachable.access_table.clone(),
                });
            }
            &access.name
        }
    };
    let edge = schema_table(schema, &reachable.edge_table)?;
    for column in [&reachable.edge_member_column, &reachable.edge_parent_column] {
        planner_column_type(&edge, column)?;
        if *column == "id" && !has_declared_id(&edge) && edge.name == *team_table {
            continue;
        }
        match edge.references.get(column) {
            Some(target) if target == team_table => {}
            _ => {
                return Err(QueryError::JoinNotRefCompatible {
                    join_table: reachable.edge_table.clone(),
                    column: column.clone(),
                    target_table: team_table.clone(),
                });
            }
        }
    }
    if let Some(seed) = &mut reachable.seed {
        let seed_table = schema_table(schema, &seed.table)?;
        if planner_column_type(&seed_table, &seed.team_column)? != &ColumnType::Uuid {
            return Err(QueryError::OperandTypeMismatch);
        }
        if let Some(user_column) = &seed.user_column {
            planner_column_type(&seed_table, user_column)?;
        }
        let seed_projects_team = if seed.team_column == "id" {
            seed_table.name == *team_table
        } else {
            matches!(
                seed_table.references.get(&seed.team_column),
                Some(target) if target == team_table
            )
        };
        if !seed_projects_team {
            return Err(QueryError::JoinNotRefCompatible {
                join_table: seed.table.clone(),
                column: seed.team_column.clone(),
                target_table: team_table.clone(),
            });
        }
        for predicate in &mut seed.filters {
            validate_predicate(&seed_table, predicate, params)?;
        }
    } else {
        match operand_type(root, &reachable.from, params)? {
            Some(ColumnType::Uuid) => {}
            None => infer_param(&reachable.from, ColumnType::Uuid, params)?,
            Some(_) => return Err(QueryError::OperandTypeMismatch),
        }
    }
    for predicate in &mut reachable.access_filters {
        validate_predicate(&access, predicate, params)?;
    }
    for predicate in &mut reachable.edge_filters {
        validate_predicate(&edge, predicate, params)?;
    }
    Ok(())
}

fn validate_inherits(root: &TableSchema, inherits: &InheritsVia) -> Result<(), QueryError> {
    planner_column_type(root, &inherits.parent_column)?;
    root.references
        .get(&inherits.parent_column)
        .ok_or_else(|| QueryError::JoinNotRefCompatible {
            join_table: root.name.clone(),
            column: inherits.parent_column.clone(),
            target_table: "referenced table".to_owned(),
        })?;
    Ok(())
}

fn validate_predicate(
    table: &TableSchema,
    predicate: &mut Predicate,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    match predicate {
        Predicate::All(predicates) | Predicate::Any(predicates) => predicates
            .iter_mut()
            .try_for_each(|predicate| validate_predicate(table, predicate, params)),
        Predicate::Not(predicate) => validate_predicate(table, predicate, params),
        Predicate::Eq(left, right) | Predicate::Ne(left, right) => {
            validate_comparable_operands(table, left, right, params).map(|_| ())
        }
        Predicate::In(left, values) => {
            let left_type = operand_type(table, left, params)?;
            for value in values {
                let mut value_type = operand_type(table, value, params)?;
                if let (Some(left_type), Some(candidate_type)) = (&left_type, &value_type)
                    && !in_operand_types_compatible(left_type, candidate_type)
                    && !matches!(&*left, Operand::Literal(_))
                    && coerce_integer_literal_for_type(value, left_type)
                {
                    value_type = operand_type(table, value, params)?;
                }
                match (left_type.clone(), value_type) {
                    (Some(left_type), Some(value_type))
                        if !in_operand_types_compatible(&left_type, &value_type)
                            && !in_literal_value_coercible(&left_type, value) =>
                    {
                        return Err(in_candidate_type_mismatch_error(
                            left, left_type, value_type,
                        ));
                    }
                    (Some(left_type), None) => {
                        infer_param(value, left_type, params)?;
                    }
                    (None, Some(value_type)) => infer_param(left, value_type, params)?,
                    (Some(_), Some(_)) => {}
                    (None, None) => return Err(QueryError::OperandTypeMismatch),
                }
            }
            Ok(())
        }
        Predicate::Gt(left, right)
        | Predicate::Gte(left, right)
        | Predicate::Lt(left, right)
        | Predicate::Lte(left, right) => {
            let column_type = validate_comparable_operands(table, left, right, params)?;
            if is_orderable(&column_type) {
                Ok(())
            } else {
                Err(QueryError::OperandTypeMismatch)
            }
        }
        Predicate::Contains(left, right) => {
            let left_type = operand_type(table, left, params)?;
            let right_type = operand_type(table, right, params)?;
            match (
                left_type.map(|column_type| non_null_column_type(&column_type)),
                right_type,
            ) {
                (Some(ColumnType::String), _) => {
                    validate_operand_against_type(table, right, ColumnType::String, params)
                }
                (Some(ColumnType::Array(member)), _) => {
                    validate_operand_against_type(table, right, *member, params)
                }
                (Some(_), _) => Err(QueryError::OperandTypeMismatch),
                (None, Some(right_type)) => {
                    infer_param(left, ColumnType::Array(Box::new(right_type)), params)
                }
                (None, None) => Err(QueryError::OperandTypeMismatch),
            }
        }
        Predicate::EnumMatch {
            column,
            case,
            payload,
        } => {
            let column_type = planner_column_type(table, column)?;
            let ColumnType::Enum(schema) = non_null_column_type(column_type) else {
                return Err(QueryError::OperandTypeMismatch);
            };
            let enum_case = schema
                .case(
                    schema
                        .tag(case)
                        .map_err(|_| QueryError::OperandTypeMismatch)?,
                )
                .map_err(|_| QueryError::OperandTypeMismatch)?;
            let payload_table = TableSchema::new(
                "__enum_payload",
                enum_case.payload.fields().iter().map(|field| {
                    crate::schema::ColumnSchema::new(
                        field.name.clone().unwrap_or_default(),
                        field.value_type.clone(),
                    )
                }),
            );
            validate_predicate(&payload_table, payload, params)
        }
        Predicate::IsNull(operand) => match operand_type(table, operand, params)? {
            Some(ColumnType::Nullable(_)) => Ok(()),
            Some(_) => Err(QueryError::OperandTypeMismatch),
            None => Err(QueryError::OperandTypeMismatch),
        },
    }
}

fn validate_comparable_operands(
    table: &TableSchema,
    left: &mut Operand,
    right: &mut Operand,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<ColumnType, QueryError> {
    let mut left_type = operand_type(table, left, params)?;
    let mut right_type = operand_type(table, right, params)?;
    if let (Some(left_known), Some(right_known)) = (&left_type, &right_type)
        && !column_types_comparable(left_known, right_known)
    {
        let coerced = if matches!(&*left, Operand::Literal(_))
            && !matches!(&*right, Operand::Literal(_))
        {
            coerce_integer_literal_for_type(left, right_known)
        } else if matches!(&*right, Operand::Literal(_)) && !matches!(&*left, Operand::Literal(_)) {
            coerce_integer_literal_for_type(right, left_known)
        } else {
            false
        };
        if coerced {
            left_type = operand_type(table, left, params)?;
            right_type = operand_type(table, right, params)?;
        }
    }
    match (left_type, right_type) {
        (Some(left_type), Some(right_type))
            if !column_types_comparable(&left_type, &right_type) =>
        {
            if let Some(error) = claim_type_mismatch_error(left, &left_type, right, &right_type) {
                return Err(error);
            }
            if let Some(error) = claim_type_mismatch_error(right, &right_type, left, &left_type) {
                return Err(error);
            }
            Err(QueryError::OperandTypeMismatch)
        }
        (Some(left_type), None) => {
            infer_param(right, left_type.clone(), params)?;
            Ok(left_type)
        }
        (None, Some(right_type)) => {
            infer_param(left, right_type.clone(), params)?;
            Ok(right_type)
        }
        (Some(left_type), Some(_)) => Ok(left_type),
        (None, None) => Err(QueryError::OperandTypeMismatch),
    }
}

fn validate_operand_against_type(
    table: &TableSchema,
    operand: &Operand,
    expected: ColumnType,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    match operand_type(table, operand, params)? {
        Some(actual) if actual == expected => Ok(()),
        Some(_) => Err(QueryError::OperandTypeMismatch),
        None => infer_param(operand, expected, params),
    }
}

fn is_orderable(column_type: &ColumnType) -> bool {
    let column_type = non_null_column_type(column_type);
    matches!(
        &column_type,
        ColumnType::U8
            | ColumnType::U16
            | ColumnType::U32
            | ColumnType::U64
            | ColumnType::I32
            | ColumnType::I64
            | ColumnType::F64
            | ColumnType::Uuid
            | ColumnType::String
    )
}

fn column_types_comparable(left: &ColumnType, right: &ColumnType) -> bool {
    let left = non_null_column_type(left);
    let right = non_null_column_type(right);
    left == right
        || matches!(
            (&left, &right),
            (ColumnType::EnumTag(_), ColumnType::U8) | (ColumnType::U8, ColumnType::EnumTag(_))
        )
}

fn flat_join_key_types_compatible(left: &ColumnType, right: &ColumnType) -> bool {
    column_types_comparable(left, right)
        || array_element_type_compatible(left, right)
        || array_element_type_compatible(right, left)
}

fn array_element_type_compatible(array: &ColumnType, scalar: &ColumnType) -> bool {
    match non_null_column_type(array) {
        ColumnType::Array(member) => {
            !matches!(non_null_column_type(scalar), ColumnType::Array(_))
                && column_types_comparable(&member, scalar)
        }
        _ => false,
    }
}

fn in_operand_types_compatible(left: &ColumnType, right: &ColumnType) -> bool {
    if column_types_comparable(left, right) {
        return true;
    }
    let left = non_null_column_type(left);
    let right = non_null_column_type(right);
    if matches!(left, ColumnType::EnumTag(_))
        && matches!(right, ColumnType::String | ColumnType::Uuid)
    {
        return true;
    }
    false
}

fn array_correlation_types_compatible(parent: &ColumnType, child: &ColumnType) -> bool {
    if in_operand_types_compatible(parent, child) {
        return true;
    }
    // Array-subquery correlation expands the parent array into child lookup
    // keys; it is distinct from whole-value `Predicate::In` membership.
    match non_null_column_type(parent) {
        ColumnType::Array(member) => column_types_comparable(&member, child),
        _ => false,
    }
}

fn in_literal_value_coercible(left: &ColumnType, value: &Operand) -> bool {
    let Operand::Literal(value) = value else {
        return false;
    };
    match non_null_column_type(left) {
        ColumnType::String => matches!(value, Value::Uuid(_)),
        ColumnType::EnumTag(_) => matches!(value, Value::String(_) | Value::Uuid(_)),
        ColumnType::Array(member) => matches!(value, Value::Array(values)
        if values.iter().all(|value| {
            in_literal_value_coercible(&member, &Operand::Literal(value.clone()))
        })),
        _ => false,
    }
}

fn in_candidate_type_mismatch_error(
    left: &Operand,
    column_type: ColumnType,
    candidate_type: ColumnType,
) -> QueryError {
    match left {
        Operand::Column(column) => QueryError::InCandidateTypeMismatch {
            column: column.clone(),
            column_type: Box::new(column_type),
            candidate_type: Box::new(candidate_type),
        },
        _ => QueryError::OperandTypeMismatch,
    }
}

fn coerce_integer_literal_for_type(operand: &mut Operand, target: &ColumnType) -> bool {
    let target = non_null_column_type(target);
    let coerced = match (&*operand, target) {
        (Operand::Literal(Value::I32(value)), ColumnType::I64) => {
            Some(Value::I64(i64::from(*value)))
        }
        (Operand::Literal(Value::I64(value)), ColumnType::I32) => {
            i32::try_from(*value).ok().map(Value::I32)
        }
        _ => None,
    };
    let Some(value) = coerced else {
        return false;
    };
    *operand = Operand::Literal(value);
    true
}

fn non_null_column_type(column_type: &ColumnType) -> ColumnType {
    match column_type {
        ColumnType::Nullable(inner) => inner.as_ref().clone(),
        other => other.clone(),
    }
}

fn is_numeric(column_type: &ColumnType) -> bool {
    // Unwrap nullability like is_orderable does: SUM/AVG over a nullable column
    // is the canonical SQL case (NULLs are skipped), so rejecting it here would
    // make the all-NULL and empty-input semantics unreachable for exactly the
    // column type where NULLs occur.
    let column_type = non_null_column_type(column_type);
    matches!(
        &column_type,
        ColumnType::U8
            | ColumnType::U16
            | ColumnType::U32
            | ColumnType::U64
            | ColumnType::I32
            | ColumnType::I64
            | ColumnType::F64
    )
}

fn operand_type(
    table: &TableSchema,
    operand: &Operand,
    params: &BTreeMap<String, ColumnType>,
) -> Result<Option<ColumnType>, QueryError> {
    match operand {
        Operand::Column(column) => Ok(Some(planner_column_type(table, column)?.clone())),
        Operand::Literal(value) => Ok(Some(value_type(value))),
        Operand::Param(name) => Ok(params.get(name).cloned()),
        Operand::Claim(name) => claim_type(name),
    }
}

fn claim_type(name: &str) -> Result<Option<ColumnType>, QueryError> {
    if let Some(suffix) = name.strip_prefix("user") {
        if let Some(ty) = crate::ids::AuthorSubject::metadata_type(&format!("$createdBy{suffix}")) {
            return Ok(Some(ty.clone()));
        }
    }
    match name {
        "team" => Ok(Some(ColumnType::Uuid)),
        "isAdmin" => Ok(Some(ColumnType::Bool)),
        _ => Ok(None),
    }
}

fn claim_type_mismatch_error(
    claim: &Operand,
    claim_type: &ColumnType,
    other: &Operand,
    other_type: &ColumnType,
) -> Option<QueryError> {
    let Operand::Claim(claim_path) = claim else {
        return None;
    };
    let Operand::Column(column) = other else {
        return None;
    };
    Some(QueryError::ClaimTypeMismatch {
        claim_path: claim_path.clone(),
        column: column.clone(),
        claim_type: column_type_name(claim_type),
        column_type: column_type_name(other_type),
    })
}

fn column_type_name(column_type: &ColumnType) -> String {
    format!("{column_type:?}")
}

fn infer_param(
    operand: &Operand,
    expected: ColumnType,
    params: &mut BTreeMap<String, ColumnType>,
) -> Result<(), QueryError> {
    let Operand::Param(name) = operand else {
        return Ok(());
    };
    match params.get(name) {
        Some(existing) if existing != &expected => Err(QueryError::ParamTypeConflict {
            param: name.clone(),
        }),
        Some(_) => Ok(()),
        None => {
            params.insert(name.clone(), expected);
            Ok(())
        }
    }
}
