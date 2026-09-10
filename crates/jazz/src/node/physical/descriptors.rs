fn widened_projection_descriptor(
    logical: &records::RecordDescriptor,
    physical_names: &[String],
    physical_table: &GrooveTableSchema,
) -> Result<records::RecordDescriptor, Error> {
    if logical.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical projection descriptor width mismatch",
        ));
    }
    Ok(records::RecordDescriptor::new(
        logical
            .fields()
            .iter()
            .zip(physical_names)
            .map(|(field, name)| {
                let physical = physical_table
                    .columns
                    .iter()
                    .find(|column| column.name == *name)
                    .ok_or(Error::InvalidStoredValue(
                        "physical projection column missing",
                    ))?;
                Ok((
                    field.name.clone().ok_or(Error::InvalidStoredValue(
                        "physical projection logical field unnamed",
                    ))?,
                    widen_projection_value_type(&field.value_type, &physical.column_type),
                ))
            })
            .collect::<Result<Vec<_>, Error>>()?,
    ))
}

/// The write-side counterpart of `widened_projection_descriptor`: a physical
/// variant record must use the table's physical field names as well as its
/// widened value types. Keeping logical names here makes Groove correctly
/// reject the record as a descriptor mismatch, but too late to explain the
/// authored-to-physical enum boundary.
pub(super) fn physical_write_descriptor(
    logical: &records::RecordDescriptor,
    physical_names: &[String],
    physical_table: &GrooveTableSchema,
) -> Result<records::RecordDescriptor, Error> {
    if logical.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical write descriptor width mismatch",
        ));
    }
    Ok(records::RecordDescriptor::new(
        logical
            .fields()
            .iter()
            .zip(physical_names)
            .map(|(logical, name)| {
                let physical = physical_table
                    .columns
                    .iter()
                    .find(|column| column.name == *name)
                    .ok_or(Error::InvalidStoredValue("physical write column missing"))?;
                // Writes target the physical table itself. Unlike read-side
                // widening, its descriptor must retain the physical enum
                // registry identities so Groove accepts the variant record;
                // values are explicitly re-encoded before this point.
                let _ = logical;
                Ok((name.clone(), physical.column_type.clone()))
            })
            .collect::<Result<Vec<_>, Error>>()?,
    ))
}

#[cfg(test)]
fn remap_authored_scalar_enum_value(
    value: Value,
    authored_cases: &[GlobalScalarEnumCaseId],
    physical_cases: &[GlobalScalarEnumCaseId],
) -> Result<Value, Error> {
    match value {
        Value::EnumTag(authored_tag) => {
            let identity =
                authored_cases
                    .get(usize::from(authored_tag))
                    .ok_or(Error::InvalidStoredValue(
                        "authored scalar enum tag outside identity mapping",
                    ))?;
            let physical_tag = physical_cases
                .iter()
                .position(|candidate| candidate == identity)
                .ok_or(Error::InvalidStoredValue(
                    "authored scalar enum identity absent from physical registry",
                ))?;
            Ok(Value::EnumTag(u8::try_from(physical_tag).map_err(
                |_| Error::InvalidStoredValue("physical scalar enum tag exhausted"),
            )?))
        }
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        Value::Nullable(Some(value)) => Ok(Value::Nullable(Some(Box::new(
            remap_authored_scalar_enum_value(*value, authored_cases, physical_cases)?,
        )))),
        _ => Err(Error::InvalidStoredValue(
            "authored scalar enum value has non-enum representation",
        )),
    }
}

#[cfg(test)]
fn remap_authored_payload_enum_value(
    value: Value,
    authored_schema: &records::EnumSchema,
    authored_cases: &[GlobalEnumCaseId],
    physical_cases: &[GlobalEnumCaseId],
) -> Result<Value, Error> {
    match value {
        Value::Enum(value) => {
            authored_schema.case(value.tag())?;
            let identity = authored_cases
                .get(usize::try_from(value.tag()).map_err(|_| {
                    Error::InvalidStoredValue("authored payload enum tag exhausted")
                })?)
                .ok_or(Error::InvalidStoredValue(
                    "authored payload enum tag outside identity mapping",
                ))?;
            let physical_tag = physical_cases
                .iter()
                .position(|case| case == identity)
                .ok_or(Error::InvalidStoredValue(
                    "authored payload enum identity absent from physical registry",
                ))?;
            // Payload descriptors are checked again by the physical record
            // encoder.  A same-name sibling with a different layout therefore
            // fails rather than being silently reinterpreted.
            Ok(Value::Enum(records::EnumValue::new(
                u32::try_from(physical_tag).map_err(|_| {
                    Error::InvalidStoredValue("physical payload enum tag exhausted")
                })?,
                value.into_record(),
            )))
        }
        Value::Nullable(None) => Ok(Value::Nullable(None)),
        Value::Nullable(Some(value)) => Ok(Value::Nullable(Some(Box::new(
            remap_authored_payload_enum_value(
                *value,
                authored_schema,
                authored_cases,
                physical_cases,
            )?,
        )))),
        _ => Err(Error::InvalidStoredValue(
            "authored payload enum value has non-enum representation",
        )),
    }
}

fn remap_nested_enum_value(
    value: Value,
    authored: &records::ValueType,
    physical: &records::ValueType,
    remaps: &EnumOccurrenceRemaps,
    path: &str,
) -> Result<Value, Error> {
    use records::ValueType;
    match (value, authored, physical) {
        (Value::EnumTag(tag), ValueType::EnumTag(_), ValueType::EnumTag(_)) => remaps
            .scalar
            .get(path)
            .and_then(|tags| tags.get(usize::from(tag)))
            .and_then(|tag| *tag)
            .map(Value::EnumTag)
            .ok_or(Error::InvalidStoredValue(
                "nested scalar enum tag absent from physical mapping",
            )),
        (Value::Nullable(None), ValueType::Nullable(_), ValueType::Nullable(_)) => {
            Ok(Value::Nullable(None))
        }
        (
            Value::Nullable(Some(value)),
            ValueType::Nullable(authored),
            ValueType::Nullable(physical),
        ) => Ok(Value::Nullable(Some(Box::new(remap_nested_enum_value(
            *value,
            authored,
            physical,
            remaps,
            &format!("{path}/nullable"),
        )?)))),
        (Value::Array(values), ValueType::Array(authored), ValueType::Array(physical)) => {
            Ok(Value::Array(
                values
                    .into_iter()
                    .map(|value| {
                        remap_nested_enum_value(
                            value,
                            authored,
                            physical,
                            remaps,
                            &format!("{path}/array"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (Value::Tuple(values), ValueType::Tuple(authored), ValueType::Tuple(physical))
            if authored.len() == physical.len() && values.len() == authored.len() =>
        {
            Ok(Value::Tuple(
                values
                    .into_iter()
                    .zip(authored.iter().zip(physical))
                    .enumerate()
                    .map(|(index, (value, (authored, physical)))| {
                        remap_nested_enum_value(
                            value,
                            authored,
                            physical,
                            remaps,
                            &format!("{path}/tuple/{index}"),
                        )
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (Value::Record(record), ValueType::Record(authored), ValueType::Record(physical))
            if authored.fields().len() == physical.fields().len() =>
        {
            let values = record.to_values()?;
            let values = values
                .into_iter()
                .zip(authored.fields().iter().zip(physical.fields()))
                .map(|(value, (authored, physical))| {
                    let name = authored.name.as_deref().ok_or(Error::InvalidStoredValue(
                        "nested record enum field unnamed",
                    ))?;
                    remap_nested_enum_value(
                        value,
                        &authored.value_type,
                        &physical.value_type,
                        remaps,
                        &format!("{path}/record/{name}"),
                    )
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(Value::Record(OwnedRecord::new(
                physical.create(&values)?,
                (**physical).clone(),
            )))
        }
        (Value::Enum(value), ValueType::Enum(authored), ValueType::Enum(physical)) => {
            let authored_tag = value.tag();
            let physical_tag = remaps
                .payload
                .get(path)
                .and_then(|tags| tags.get(usize::try_from(authored_tag).ok()?))
                .and_then(|tag| *tag)
                .ok_or(Error::InvalidStoredValue(
                    "nested payload enum tag absent from physical mapping",
                ))?;
            let authored_case = authored.case(authored_tag)?;
            let physical_case = physical.case(physical_tag)?;
            if authored_case.payload.fields().len() != physical_case.payload.fields().len() {
                return Err(Error::InvalidStoredValue(
                    "nested payload enum payload width changed",
                ));
            }
            let semantic_child_root = remaps
                .payload_children
                .get(path)
                .and_then(|paths| paths.get(usize::try_from(authored_tag).ok()?))
                .and_then(|path| path.as_deref());
            let child_root = semantic_child_root
                .map(str::to_owned)
                .unwrap_or_else(|| format!("{path}/case/{authored_tag}"));
            let values = value.record().to_values()?;
            let values = values
                .into_iter()
                .zip(
                    authored_case
                        .payload
                        .fields()
                        .iter()
                        .zip(physical_case.payload.fields()),
                )
                .map(|(value, (authored, physical))| {
                    let name = authored.name.as_deref().ok_or(Error::InvalidStoredValue(
                        "nested payload enum field unnamed",
                    ))?;
                    remap_nested_enum_value(
                        value,
                        &authored.value_type,
                        &physical.value_type,
                        remaps,
                        &if semantic_child_root.is_some() {
                            format!("{child_root}/record/{name}")
                        } else {
                            format!("{child_root}/{name}")
                        },
                    )
                })
                .collect::<Result<Vec<_>, Error>>()?;
            Ok(Value::Enum(records::EnumValue::new(
                physical_tag,
                OwnedRecord::new(
                    physical_case.payload.create(&values)?,
                    physical_case.payload.clone(),
                ),
            )))
        }
        (value, authored, physical) if authored == physical => Ok(value),
        _ => Err(Error::InvalidStoredValue(
            "nested enum remap descriptor mismatch",
        )),
    }
}

fn widen_projection_value_type(
    logical: &records::ValueType,
    physical: &records::ValueType,
) -> records::ValueType {
    use records::ValueType;
    match (logical, physical) {
        // Large JSON is logical String-shaped but its durable cell is the
        // internal stored-scalar enum. Physical-to-physical projections must
        // retain that descriptor; logical materialization happens only at the
        // Jazz boundary, not in Groove's raw variant projector.
        (_, physical) if physical.is_internal_storage_type() => physical.clone(),
        // Projection crosses the physical interning boundary.  It must expose
        // the target schema's declaration-local enum descriptor after the
        // explicit tag remap above, not leak the physical descriptor/tag space.
        (ValueType::EnumTag(logical), ValueType::EnumTag(_)) => ValueType::EnumTag(logical.clone()),
        (ValueType::Enum(logical), ValueType::Enum(_)) => ValueType::Enum(logical.clone()),
        (logical, ValueType::Nullable(physical)) if !matches!(logical, ValueType::Nullable(_)) => {
            widen_projection_value_type(logical, physical)
        }
        (ValueType::Nullable(logical), ValueType::Nullable(physical)) => {
            ValueType::Nullable(Box::new(widen_projection_value_type(logical, physical)))
        }
        (ValueType::Array(logical), ValueType::Array(physical)) => {
            ValueType::Array(Box::new(widen_projection_value_type(logical, physical)))
        }
        (ValueType::Tuple(logical), ValueType::Tuple(physical))
            if logical.len() == physical.len() =>
        {
            ValueType::Tuple(
                logical
                    .iter()
                    .zip(physical)
                    .map(|(logical, physical)| widen_projection_value_type(logical, physical))
                    .collect(),
            )
        }
        _ => logical.clone(),
    }
}

pub(super) fn physical_version_storage_tables(
    catalogue_schemas: &BTreeMap<SchemaVersionId, SchemaVersion>,
    schema_version_aliases: &BTreeMap<SchemaVersionId, SchemaVersionAlias>,
    physical_mappings: &BTreeMap<SchemaVersionId, SchemaPhysicalMapping>,
) -> Result<Vec<GrooveTableSchema>, Error> {
    let mut lineages = BTreeMap::<
        PhysicalTableId,
        Vec<(SchemaVersionId, &TableSchema, &TablePhysicalMapping, &PhysicalTableIdentity)>,
    >::new();
    for (schema_version, mapping) in physical_mappings {
        let schema = catalogue_schemas
            .get(schema_version)
            .ok_or(Error::InvalidStoredValue(
                "physical mapping schema payload missing",
            ))?;
        for (logical_table, table_mapping) in &mapping.tables {
            let table_identity = mapping.identities.tables.get(logical_table).ok_or(
                Error::InvalidStoredValue("physical table identity missing"),
            )?;
            let table = schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == *logical_table)
                .ok_or(Error::InvalidStoredValue(
                    "physical mapping logical table missing",
                ))?;
            lineages.entry(table_mapping.table_id).or_default().push((
                *schema_version,
                table,
                table_mapping,
                table_identity,
            ));
        }
    }

    let mut tables = Vec::with_capacity(lineages.len() * 7);
    for (table_id, variants) in lineages {
        let (_, template_table, _, _) = variants
            .first()
            .ok_or(Error::InvalidStoredValue("physical history lineage empty"))?;
        let template = template_table.history_storage_table();
        let system_columns = template
            .columns
            .iter()
            .take(HistoryRowRecord::USER_CELLS)
            .cloned()
            .collect::<Vec<_>>();
        let trailing_history_columns = template
            .columns
            .iter()
            .skip(HistoryRowRecord::USER_CELLS + template_table.columns.len())
            .cloned()
            .collect::<Vec<_>>();
        // First form the persistent registry for every scalar enum occurrence.
        // Concurrent schemas may use the same authored ordinal for distinct
        // cases; their permanent UUID identities must therefore be unioned
        // before any descriptor assigns compact local tags.
        let mut scalar_enum_registries =
            BTreeMap::<PhysicalColumnId, BTreeSet<GlobalScalarEnumCaseId>>::new();
        for (schema_version, logical_table, mapping, table_identity) in &variants {
            for column in &logical_table.columns {
                let records::ValueType::EnumTag(enum_schema) = &column.column_type else {
                    continue;
                };
                let column_id =
                    mapping
                        .columns
                        .get(&column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical scalar enum column mapping missing",
                        ))?;
                let cases = if let Some(cases) = mapping.scalar_enum_cases.get(&column_id) {
                    cases.clone()
                } else {
                    // Provisional bootstrap schemas acquire their durable
                    // mapping immediately after table construction. Until
                    // then this deterministic spelling is the same mapping
                    // hydration will persist; it is not receipt-order state.
                    enum_schema
                        .variants
                        .iter()
                        .zip(&table_identity.columns[&column.name].enum_variants["root"])
                        .enumerate()
                        .map(|(ordinal, (_, id))| {
                            Ok(GlobalScalarEnumCaseId {
                                id: *id,
                                introducing_schema: *schema_version,
                                introducing_ordinal: u8::try_from(ordinal).map_err(|_| {
                                    Error::InvalidStoredValue("scalar enum tag exhausted")
                                })?,
                            })
                        })
                        .collect::<Result<Vec<_>, Error>>()?
                };
                scalar_enum_registries
                    .entry(column_id)
                    .or_default()
                    .extend(cases);
            }
        }
        let scalar_enum_registries = scalar_enum_registries
            .into_iter()
            .map(|(column_id, cases)| {
                let mut cases = cases.into_iter().collect::<Vec<_>>();
                cases.sort_by(|left, right| {
                    compare_scalar_enum_cases(schema_version_aliases, left, right)
                });
                Ok((column_id, cases))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        let mut nested_scalar_enum_registries =
            BTreeMap::<(PhysicalColumnId, String), BTreeSet<GlobalScalarEnumCaseId>>::new();
        for (schema_version, logical_table, mapping, table_identity) in &variants {
            // Bootstrap constructs physical tables before the freshly
            // introduced mapping is hydrated into the catalogue. Seed nested
            // occurrences from the authored descriptor in that one state;
            // otherwise the first table gets a generic registry id and the
            // next synchronization sees an incompatible field definition.
            let mut bootstrap_paths = mapping.nested_scalar_enum_cases.clone();
            if bootstrap_paths.is_empty() {
                for column in &logical_table.columns {
                    if matches!(column.column_type, records::ValueType::EnumTag(_)) {
                        continue;
                    }
                    hydrate_nested_scalar_enum_cases(
                        &column.column_type,
                        &table_identity.columns[&column.name].enum_variants,
                        *schema_version,
                        "root",
                        "root",
                        bootstrap_paths
                            .entry(mapping.columns.get(&column.name).copied().ok_or(
                                Error::InvalidStoredValue(
                                    "physical nested scalar enum column mapping missing",
                                ),
                            )?)
                            .or_default(),
                    )?;
                }
            }
            for (column_id, paths) in &bootstrap_paths {
                for (path, cases) in paths {
                    nested_scalar_enum_registries
                        .entry((*column_id, path.clone()))
                        .or_default()
                        .extend(cases.iter().cloned());
                }
            }
        }
        let nested_scalar_enum_registries = nested_scalar_enum_registries
            .into_iter()
            .map(|((column_id, path), cases)| {
                let mut cases = cases.into_iter().collect::<Vec<_>>();
                cases.sort_by(|left, right| {
                    compare_scalar_enum_cases(schema_version_aliases, left, right)
                });
                Ok(((column_id, path), cases))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        // Payload enum occurrences, including those inside another payload,
        // need the same lineage-wide union.  The path beneath a payload case
        // is rooted by that case's global UUID, so concurrent siblings which
        // both authored ordinal `n` never share a descendant registry.
        let mut nested_payload_enum_registries =
            BTreeMap::<(PhysicalColumnId, String), BTreeSet<GlobalEnumCaseId>>::new();
        let mut nested_payload_enum_layouts = BTreeMap::<
            (PhysicalColumnId, String, GlobalEnumCaseId),
            records::RecordDescriptor,
        >::new();
        for (schema_version, logical_table, mapping, table_identity) in &variants {
            for column in &logical_table.columns {
                let column_id =
                    mapping
                        .columns
                        .get(&column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical nested payload enum column mapping missing",
                        ))?;
                // As for nested scalar cases above, the first physical-table
                // construction precedes catalogue hydration. Seed payload
                // identities from the authored descriptor so reopening does
                // not try to replace a generic nested registry.
                let mut bootstrap_paths = mapping.nested_payload_enum_cases.clone();
                if bootstrap_paths.is_empty() {
                    hydrate_nested_payload_enum_cases(
                        &column.column_type,
                        &table_identity.columns[&column.name].enum_variants,
                        *schema_version,
                        "root",
                        "root",
                        bootstrap_paths.entry(column_id).or_default(),
                    )?;
                }
                let Some(paths) = bootstrap_paths.get(&column_id) else {
                    continue;
                };
                for (path, cases) in paths {
                    nested_payload_enum_registries
                        .entry((column_id, path.clone()))
                        .or_default()
                        .extend(cases.iter().cloned());
                }
                let mut layouts = BTreeMap::new();
                collect_nested_payload_enum_layouts(
                    &column.column_type,
                    "root",
                    paths,
                    &mut layouts,
                )?;
                for ((path, identity), layout) in layouts {
                    let key = (column_id, path, identity);
                    match nested_payload_enum_layouts.entry(key) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(layout);
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get() == &layout => {}
                        std::collections::btree_map::Entry::Occupied(_) => {
                            return Err(Error::InvalidStoredValue(
                                "same nested payload enum identity has incompatible layout",
                            ));
                        }
                    }
                }
            }
        }
        let nested_payload_enum_registries = nested_payload_enum_registries
            .into_iter()
            .map(|((column_id, path), cases)| {
                let mut cases = cases.into_iter().collect::<Vec<_>>();
                cases.sort_by(|left, right| {
                    compare_global_enum_cases(schema_version_aliases, left, right)
                });
                Ok(((column_id, path), cases))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        let mut payload_enum_registries =
            BTreeMap::<PhysicalColumnId, BTreeSet<GlobalEnumCaseId>>::new();
        let mut payload_enum_layouts =
            BTreeMap::<(PhysicalColumnId, GlobalEnumCaseId), records::RecordDescriptor>::new();
        for (schema_version, logical_table, mapping, table_identity) in &variants {
            for column in &logical_table.columns {
                let records::ValueType::Enum(enum_schema) = &column.column_type else {
                    continue;
                };
                let column_id =
                    mapping
                        .columns
                        .get(&column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical payload enum column mapping missing",
                        ))?;
                let identities = if let Some(cases) = mapping.payload_enum_cases.get(&column_id) {
                    cases.clone()
                } else {
                    enum_schema
                        .cases
                        .iter()
                        .zip(&table_identity.columns[&column.name].enum_variants["root"])
                        .enumerate()
                        .map(|(ordinal, (_, id))| {
                            Ok(GlobalEnumCaseId {
                                id: *id,
                                introducing_schema: *schema_version,
                                introducing_ordinal: u32::try_from(ordinal).map_err(|_| {
                                    Error::InvalidStoredValue("payload enum tag exhausted")
                                })?,
                            })
                        })
                        .collect::<Result<Vec<_>, Error>>()?
                };
                if identities.len() != enum_schema.cases.len() {
                    return Err(Error::InvalidStoredValue(
                        "payload enum identity mapping width mismatch",
                    ));
                }
                if let Some(nested_root) = mapping
                    .nested_payload_enum_cases
                    .get(&column_id)
                    .and_then(|paths| paths.get("root"))
                    && nested_root != &identities
                {
                    return Err(Error::InvalidStoredValue(
                        "direct and nested payload enum identity mappings diverged",
                    ));
                }
                for (identity, case) in identities.iter().zip(&enum_schema.cases) {
                    let key = (column_id, identity.clone());
                    match payload_enum_layouts.entry(key) {
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            entry.insert(case.payload.clone());
                        }
                        std::collections::btree_map::Entry::Occupied(entry)
                            if entry.get() == &case.payload => {}
                        std::collections::btree_map::Entry::Occupied(_) => {
                            return Err(Error::InvalidStoredValue(
                                "same payload enum identity has incompatible layout",
                            ));
                        }
                    }
                }
                payload_enum_registries
                    .entry(column_id)
                    .or_default()
                    .extend(identities.iter().cloned());
            }
        }
        let payload_enum_registries = payload_enum_registries
            .into_iter()
            .map(|(column_id, cases)| {
                let mut cases = cases.into_iter().collect::<Vec<_>>();
                cases.sort_by(|left, right| {
                    compare_global_enum_cases(schema_version_aliases, left, right)
                });
                Ok((column_id, cases))
            })
            .collect::<Result<BTreeMap<_, _>, Error>>()?;

        let mut physical_columns = BTreeMap::new();
        for (_, logical_table, mapping, _) in &variants {
            for column in &logical_table.columns {
                let column_id =
                    mapping
                        .columns
                        .get(&column.name)
                        .copied()
                        .ok_or(Error::InvalidStoredValue(
                            "physical history column mapping missing",
                        ))?;
                let storage_type = match &column.column_type {
                    records::ValueType::EnumTag(_) => {
                        records::ValueType::EnumTag(physical_scalar_enum_schema(
                            column_id,
                            scalar_enum_registries.get(&column_id).ok_or(
                                Error::InvalidStoredValue("physical scalar enum registry missing"),
                            )?,
                        )?)
                        .nullable()
                    }
                    records::ValueType::Enum(_) => {
                        let cases = payload_enum_registries.get(&column_id).ok_or(
                            Error::InvalidStoredValue("physical payload enum registry missing"),
                        )?;
                        let cases = cases
                            .iter()
                            .map(|identity| {
                                let payload = payload_enum_layouts
                                    .get(&(column_id, identity.clone()))
                                    .ok_or(Error::InvalidStoredValue(
                                        "physical payload enum layout missing",
                                    ))?;
                                let scalar_registries = nested_scalar_enum_registries
                                    .iter()
                                    .filter(|((id, _), _)| *id == column_id)
                                    .map(|((_, path), cases)| (path.clone(), cases.clone()))
                                    .collect::<BTreeMap<_, _>>();
                                let payload_registries = nested_payload_enum_registries
                                    .iter()
                                    .filter(|((id, _), _)| *id == column_id)
                                    .map(|((_, path), cases)| (path.clone(), cases.clone()))
                                    .collect::<BTreeMap<_, _>>();
                                let payload_layouts = nested_payload_enum_layouts
                                    .iter()
                                    .filter(|((id, _, _), _)| *id == column_id)
                                    .map(|((_, path, identity), layout)| {
                                        ((path.clone(), identity.clone()), layout.clone())
                                    })
                                    .collect::<BTreeMap<_, _>>();
                                let records::ValueType::Record(payload) =
                                    physical_nested_enum_value_type(
                                        &records::ValueType::Record(Box::new(payload.clone())),
                                        &global_case_path("root", identity),
                                        &scalar_registries,
                                        &payload_registries,
                                        &payload_layouts,
                                        column_id,
                                    )?
                                else {
                                    unreachable!("record lowering preserves payload shape");
                                };
                                Ok(records::EnumCase::new(
                                    physical_enum_case_name(identity),
                                    *payload,
                                ))
                            })
                            .collect::<Result<Vec<_>, Error>>()?;
                        records::ValueType::Enum(Box::new(
                            records::EnumSchema::new(
                                format!("physical-column-{}", column_id.0),
                                cases,
                            )
                            .map_err(|_| {
                                Error::InvalidStoredValue("invalid physical payload enum registry")
                            })?
                            .with_registry_id(
                                records::variant_registry_id_for_path(&format!(
                                    "physical-column/{}",
                                    column_id.0
                                )),
                            ),
                        ))
                        .nullable()
                    }
                    _ if nested_scalar_enum_registries
                        .keys()
                        .any(|(id, _)| *id == column_id)
                        || nested_payload_enum_registries
                            .keys()
                            .any(|(id, _)| *id == column_id) =>
                    {
                        let scalar_registries = nested_scalar_enum_registries
                            .iter()
                            .filter(|((id, _), _)| *id == column_id)
                            .map(|((_, path), cases)| (path.clone(), cases.clone()))
                            .collect::<BTreeMap<_, _>>();
                        let payload_registries = nested_payload_enum_registries
                            .iter()
                            .filter(|((id, _), _)| *id == column_id)
                            .map(|((_, path), cases)| (path.clone(), cases.clone()))
                            .collect::<BTreeMap<_, _>>();
                        let payload_layouts = nested_payload_enum_layouts
                            .iter()
                            .filter(|((id, _, _), _)| *id == column_id)
                            .map(|((_, path, identity), layout)| {
                                ((path.clone(), identity.clone()), layout.clone())
                            })
                            .collect::<BTreeMap<_, _>>();
                        physical_nested_enum_value_type(
                            &column.column_type,
                            "root",
                            &scalar_registries,
                            &payload_registries,
                            &payload_layouts,
                            column_id,
                        )?
                        .nullable()
                    }
                    _ => physical_storage_value_type(column)
                        .nullable()
                        .rebind_variant_registries(&format!("physical-column/{}", column_id.0)),
                };
                if let Some(existing) = physical_columns.get_mut(&column_id) {
                    *existing = merge_physical_value_type(existing, &storage_type)?;
                } else {
                    physical_columns.insert(column_id, storage_type);
                }
            }
        }
        let columns = system_columns
            .into_iter()
            .chain(physical_columns.iter().map(|(column_id, column_type)| {
                GrooveColumnSchema::new(physical_user_column_field(*column_id), column_type.clone())
            }))
            .chain(trailing_history_columns);
        let mut physical = GrooveTableSchema::new_with_bound_registries(
            physical_history_table_name(table_id),
            columns,
        );
        physical.primary_key = template.primary_key.clone();
        physical.indices = template.indices.clone();
        let mut register = template_table.register_storage_table();
        register.name = physical_register_table_name(table_id);

        let logical_global_tables = template_table.global_current_storage_tables();
        let current_system_columns = logical_global_tables[0]
            .columns
            .iter()
            .take(GlobalCurrentRowRecord::USER_CELLS)
            .cloned()
            .collect::<Vec<_>>();
        let current_trailing_columns = logical_global_tables[0]
            .columns
            .iter()
            .skip(GlobalCurrentRowRecord::USER_CELLS + template_table.columns.len())
            .cloned()
            .collect::<Vec<_>>();
        let current_columns = || {
            current_system_columns
                .iter()
                .cloned()
                .chain(physical_columns.iter().map(|(column_id, column_type)| {
                    GrooveColumnSchema::new(
                        physical_user_column_field(*column_id),
                        column_type.clone(),
                    )
                }))
                .chain(current_trailing_columns.iter().cloned())
        };
        let mut physical_global = GrooveTableSchema::new_with_bound_registries(
            physical_global_current_table_name(table_id),
            current_columns(),
        );
        physical_global.primary_key = logical_global_tables[0].primary_key.clone();
        let indexed_columns = variants
            .iter()
            .flat_map(|(_, logical_table, mapping, _)| {
                logical_table
                    .global_current_indexed_columns()
                    .into_iter()
                    .filter_map(|column| mapping.columns.get(&column).copied())
            })
            .collect::<BTreeSet<_>>();
        for &column_id in &indexed_columns {
            physical_global = physical_global.with_index(GrooveIndexSchema::new(
                physical_current_index_name(column_id),
                vec!["branch_key".to_owned(), physical_user_column_field(column_id)],
            ));
        }
        let mut register_global = logical_global_tables[1].clone();
        register_global.name = physical_register_global_current_table_name(table_id);

        let logical_ahead_tables = template_table.ahead_current_storage_tables();
        let mut physical_ahead = GrooveTableSchema::new_with_bound_registries(
            physical_ahead_current_table_name(table_id),
            current_columns(),
        );
        physical_ahead.primary_key = logical_ahead_tables[0].primary_key.clone();
        physical_ahead.indices = logical_ahead_tables[0].indices.clone();
        for column_id in indexed_columns {
            physical_ahead = physical_ahead.with_index(GrooveIndexSchema::new(
                physical_current_index_name(column_id),
                vec!["branch_key".to_owned(), physical_user_column_field(column_id)],
            ));
        }
        let mut register_ahead = logical_ahead_tables[1].clone();
        register_ahead.name = physical_register_ahead_current_table_name(table_id);

        let rejected_template = template_table.rejected_versions_storage_table();
        let rejected_system_columns = rejected_template
            .columns
            .iter()
            .take(RejectedVersionRowRecord::USER_CELLS)
            .cloned();
        let rejected_columns = rejected_system_columns.chain(physical_columns.iter().map(
            |(column_id, column_type)| {
                GrooveColumnSchema::new(physical_user_column_field(*column_id), column_type.clone())
            },
        ));
        let mut rejected = GrooveTableSchema::new_with_bound_registries(
            physical_rejected_versions_table_name(table_id),
            rejected_columns,
        );
        rejected.primary_key = rejected_template.primary_key.clone();

        let mut layouts_by_tag = BTreeMap::new();
        let mut current_layouts_by_tag = BTreeMap::new();
        let mut rejected_layouts_by_tag = BTreeMap::new();
        for (schema_version, logical_table, mapping, _) in &variants {
            let alias = schema_version_aliases.get(&schema_version).copied().ok_or(
                Error::InvalidStoredValue("physical history schema alias missing"),
            )?;
            let cases = if mapping.variant_cases.is_empty() {
                vec![(groove_variant_tag(alias)?, None)]
            } else {
                mapping
                    .variant_cases
                    .iter()
                    .map(|case| (case.tag, Some(&case.fields)))
                    .collect()
            };
            for (tag, fields) in cases {
                let history =
                    physical_history_field_names_for_case(logical_table, mapping, fields)?;
                if layouts_by_tag.insert(tag, history).is_some() {
                    return Err(Error::InvalidStoredValue(
                        "physical table variant tag collision",
                    ));
                }
                let current =
                    physical_current_field_names_for_case(logical_table, mapping, fields)?;
                if current_layouts_by_tag.insert(tag, current).is_some() {
                    return Err(Error::InvalidStoredValue(
                        "physical table variant tag collision",
                    ));
                }
                let rejected =
                    physical_rejected_version_field_names_for_case(logical_table, mapping, fields)?;
                if rejected_layouts_by_tag.insert(tag, rejected).is_some() {
                    return Err(Error::InvalidStoredValue(
                        "physical table variant tag collision",
                    ));
                }
            }
        }
        for (tag, fields) in layouts_by_tag {
            let payload = variant_payload_fields_for_names(&physical, &fields)?;
            physical = physical.with_variant_payload(tag, payload);
        }
        for (tag, fields) in current_layouts_by_tag {
            let global_payload = variant_payload_fields_for_names(&physical_global, &fields)?;
            physical_global = physical_global.with_variant_payload(tag, global_payload);
            let ahead_payload = variant_payload_fields_for_names(&physical_ahead, &fields)?;
            physical_ahead = physical_ahead.with_variant_payload(tag, ahead_payload);
        }
        for (tag, fields) in rejected_layouts_by_tag {
            let payload = variant_payload_fields_for_names(&rejected, &fields)?;
            rejected = rejected.with_variant_payload(tag, payload);
        }
        tables.push(physical);
        tables.push(register);
        tables.push(physical_global);
        tables.push(register_global);
        tables.push(physical_ahead);
        tables.push(register_ahead);
        tables.push(rejected);
    }
    Ok(tables)
}

/// Physical history/current rows carry an engine-owned stored-scalar context
/// rather than the logical String/Bytes type. The semantic kind is frozen in
/// `ColumnSchema` by schema lowering; raw cells and public query bindings never
/// choose it.
fn physical_storage_value_type(column: &ColumnSchema) -> records::ValueType {
    crate::schema::storage_column_type(column)
}

fn variant_payload_fields_for_names(
    table: &GrooveTableSchema,
    names: &[String],
) -> Result<Vec<GrooveTableVariantField>, Error> {
    names
        .iter()
        .map(|name| {
            let column = table
                .columns
                .iter()
                .find(|column| column.name == *name)
                .ok_or(Error::InvalidStoredValue(
                    "physical variant shared column missing",
                ))?;
            Ok(GrooveTableVariantField::shared(
                name.clone(),
                column.column_type.clone(),
                name.clone(),
            ))
        })
        .collect()
}

fn merge_physical_record_descriptor(
    existing: &records::RecordDescriptor,
    incoming: &records::RecordDescriptor,
) -> Result<records::RecordDescriptor, Error> {
    if existing.fields().len() != incoming.fields().len() {
        return Err(Error::InvalidStoredValue(
            "physical variant payload descriptor width changed",
        ));
    }
    let mut fields = Vec::with_capacity(existing.fields().len());
    for (left, right) in existing.fields().iter().zip(incoming.fields()) {
        if left.name != right.name {
            return Err(Error::InvalidStoredValue(
                "physical variant payload field identity changed",
            ));
        }
        fields.push((
            left.name.clone().ok_or(Error::InvalidStoredValue(
                "physical variant payload field unnamed",
            ))?,
            merge_physical_value_type(&left.value_type, &right.value_type)?,
        ));
    }
    Ok(records::RecordDescriptor::new(fields))
}

/// Merge two snapshots of one physical value occurrence. Registry identity,
/// rather than structural descriptor equality, is authoritative for enums and
/// enums; the older declaration must be an exact prefix of the newer one.
fn merge_physical_value_type(
    existing: &records::ValueType,
    incoming: &records::ValueType,
) -> Result<records::ValueType, Error> {
    use records::ValueType;
    match (existing, incoming) {
        (ValueType::EnumTag(left), ValueType::EnumTag(right))
            if left.registry_id() == right.registry_id() =>
        {
            // This helper is also used while combining independently authored
            // snapshots.  A shared authored registry id does not make ordinal
            // `n` globally meaningful: concurrent siblings can legitimately
            // introduce distinct cases at that ordinal.  The physical-table
            // path supplies permanent global identities and replaces these
            // names with its durable registry; retain a deterministic union
            // here so descriptor construction never aliases sibling cases.
            // This is a physical registry, so its declaration order is the
            // stored tag order. Preserve the established prefix and append
            // only newly observed opaque case names; a sorted set would
            // silently retag existing values.
            let mut variants = left.variants.clone();
            let appended = right
                .variants
                .iter()
                .filter(|variant| !variants.contains(variant))
                .cloned()
                .collect::<Vec<_>>();
            variants.extend(appended);
            Ok(ValueType::EnumTag(
                records::ScalarEnumSchema::new(left.name.clone(), variants)
                    .map_err(|_| Error::InvalidStoredValue("invalid physical enum registry"))?
                    .with_registry_id(left.registry_id()),
            ))
        }
        (ValueType::Enum(left), ValueType::Enum(right))
            if left.registry_id == right.registry_id =>
        {
            // Preserve the established physical tag prefix. These names are
            // opaque spellings of catalogue identities; sorting them would
            // silently retag stored values merely because two schema IDs sort
            // differently. New identities append in the incoming descriptor's
            // already-canonical catalogue order.
            let mut cases = left.cases.clone();
            for incoming_case in &right.cases {
                if let Some(existing_case) = cases
                    .iter_mut()
                    .find(|existing| existing.name == incoming_case.name)
                {
                    existing_case.payload = merge_physical_record_descriptor(
                        &existing_case.payload,
                        &incoming_case.payload,
                    )?;
                } else {
                    cases.push(incoming_case.clone());
                }
            }
            Ok(ValueType::Enum(Box::new(
                records::EnumSchema::new(right.name.clone(), cases)
                    .map_err(|_| Error::InvalidStoredValue("invalid physical enum registry"))?
                    .with_registry_id(left.registry_id),
            )))
        }
        (ValueType::Tuple(left), ValueType::Tuple(right)) if left.len() == right.len() => {
            Ok(ValueType::Tuple(
                left.iter()
                    .zip(right)
                    .map(|(a, b)| merge_physical_value_type(a, b))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        (ValueType::Array(left), ValueType::Array(right)) => Ok(ValueType::Array(Box::new(
            merge_physical_value_type(left, right)?,
        ))),
        (ValueType::Nullable(left), ValueType::Nullable(right)) => Ok(ValueType::Nullable(
            Box::new(merge_physical_value_type(left, right)?),
        )),
        (ValueType::Record(left), ValueType::Record(right)) => Ok(ValueType::Record(Box::new(
            merge_physical_record_descriptor(left, right)?,
        ))),
        _ if existing == incoming => Ok(existing.clone()),
        _ => Err(Error::InvalidStoredValue(
            "physical history column type mismatch",
        )),
    }
}

fn physical_history_descriptor(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
    _alias: SchemaVersionAlias,
) -> Result<records::RecordDescriptor, Error> {
    let logical_descriptor = table.history_storage_table().record_schema();
    let physical_names = physical_history_field_names(table, mapping)?;
    if logical_descriptor.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical history descriptor width mismatch",
        ));
    }
    physical_descriptor_with_enum_registries(table, logical_descriptor, physical_names, mapping)
}

fn physical_current_descriptor(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<records::RecordDescriptor, Error> {
    let logical_descriptor = table.global_current_storage_tables()[0].record_schema();
    let physical_names = physical_current_field_names(table, mapping)?;
    if logical_descriptor.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical current descriptor width mismatch",
        ));
    }
    physical_descriptor_with_enum_registries(table, logical_descriptor, physical_names, mapping)
}

fn physical_rejected_version_descriptor(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<records::RecordDescriptor, Error> {
    let logical_descriptor = table.rejected_versions_storage_table().record_schema();
    let physical_names = physical_rejected_version_field_names(table, mapping)?;
    if logical_descriptor.fields().len() != physical_names.len() {
        return Err(Error::InvalidStoredValue(
            "physical rejected-version descriptor width mismatch",
        ));
    }
    physical_descriptor_with_enum_registries(table, logical_descriptor, physical_names, mapping)
}

fn physical_descriptor_with_enum_registries(
    table: &TableSchema,
    logical: records::RecordDescriptor,
    physical_names: Vec<String>,
    mapping: &TablePhysicalMapping,
) -> Result<records::RecordDescriptor, Error> {
    fn physical_user_slot(name: &str) -> Option<PhysicalColumnId> {
        name.strip_prefix("_app_")
            .and_then(|id| id.parse::<u64>().ok())
            .map(PhysicalColumnId)
    }

    Ok(records::RecordDescriptor::new_with_fields(
        physical_names
            .into_iter()
            .zip(logical.fields())
            .map(|(name, field)| {
                let identity = if let Some(id) = physical_user_slot(&name) {
                    Some(match field.logical_name() {
                        Some(logical) if logical != name => records::FieldIdentity::NamedSlot {
                            name: logical.to_owned(),
                            slot: id.0,
                        },
                        _ => records::FieldIdentity::Slot(id.0),
                    })
                } else {
                    field.identity.clone()
                };
                let value_type = if let Some(id) = physical_user_slot(&name) {
                    if let Some(cases) = mapping.scalar_enum_cases.get(&id) {
                        physical_scalar_enum_schema(id, cases)
                            .map(|schema| records::ValueType::EnumTag(schema).nullable())?
                    } else if let Some(column) = mapping
                        .columns
                        .iter()
                        .find_map(|(name, candidate)| (*candidate == id).then_some(name))
                        .and_then(|name| table.columns.iter().find(|column| column.name == *name))
                    {
                        // A physical history/current projection must use the
                        // same schema-derived scalar descriptor as the
                        // physical table. In particular JSON is logically a
                        // String but physically an internal stored scalar.
                        physical_storage_value_type(column).nullable()
                    } else {
                        match &field.value_type {
                            // Physical user cells are nullable for absence, but their
                            // direct enum registry belongs to the column occurrence—not
                            // to the nullable wrapper. Match the storage descriptor's
                            // `physical_scalar_enum_schema(column_id, ...)` identity.
                            records::ValueType::Nullable(inner) => records::ValueType::Nullable(
                                Box::new(inner.as_ref().clone().rebind_variant_registries(
                                    &format!("physical-column/{}", id.0),
                                )),
                            ),
                            value_type => value_type
                                .clone()
                                .rebind_variant_registries(&format!("physical-column/{}", id.0)),
                        }
                    }
                } else {
                    field.value_type.clone()
                };
                Ok(records::DescriptorField {
                    name: Some(name),
                    identity,
                    value_type,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?,
    ))
}


fn physical_history_field_names(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<Vec<String>, Error> {
    physical_history_field_names_for_case(table, mapping, None)
}

fn physical_history_field_names_for_case(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
    present: Option<&BTreeSet<String>>,
) -> Result<Vec<String>, Error> {
    let logical_descriptor = table.history_storage_table().record_schema();
    let mut fields = logical_descriptor
        .fields()
        .iter()
        .take(HistoryRowRecord::USER_CELLS)
        .map(|field| {
            field.name.clone().ok_or(Error::InvalidStoredValue(
                "physical history system field unnamed",
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for column in &table.columns {
        if present.is_some_and(|present| !present.contains(&column.name)) {
            continue;
        }
        let column_id =
            mapping
                .columns
                .get(&column.name)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical history column mapping missing",
                ))?;
        fields.push(physical_user_column_field(column_id));
    }
    fields.extend(
        logical_descriptor
            .fields()
            .iter()
            .skip(HistoryRowRecord::USER_CELLS + table.columns.len())
            .map(|field| {
                field.name.clone().ok_or(Error::InvalidStoredValue(
                    "physical history trailing field unnamed",
                ))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(fields)
}

pub(super) fn physical_current_field_names(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<Vec<String>, Error> {
    physical_current_field_names_for_case(table, mapping, None)
}

fn physical_current_field_names_for_case(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
    present: Option<&BTreeSet<String>>,
) -> Result<Vec<String>, Error> {
    let logical_descriptor = table.global_current_storage_tables()[0].record_schema();
    let mut fields = logical_descriptor
        .fields()
        .iter()
        .take(GlobalCurrentRowRecord::USER_CELLS)
        .map(|field| {
            field.name.clone().ok_or(Error::InvalidStoredValue(
                "physical current system field unnamed",
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for column in &table.columns {
        if present.is_some_and(|present| !present.contains(&column.name)) {
            continue;
        }
        let column_id =
            mapping
                .columns
                .get(&column.name)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical current column mapping missing",
                ))?;
        fields.push(physical_user_column_field(column_id));
    }
    fields.extend(
        logical_descriptor
            .fields()
            .iter()
            .skip(GlobalCurrentRowRecord::USER_CELLS + table.columns.len())
            .map(|field| {
                field.name.clone().ok_or(Error::InvalidStoredValue(
                    "physical current trailing field unnamed",
                ))
            })
            .collect::<Result<Vec<_>, _>>()?,
    );
    Ok(fields)
}

fn physical_rejected_version_field_names(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
) -> Result<Vec<String>, Error> {
    physical_rejected_version_field_names_for_case(table, mapping, None)
}

fn physical_rejected_version_field_names_for_case(
    table: &TableSchema,
    mapping: &TablePhysicalMapping,
    present: Option<&BTreeSet<String>>,
) -> Result<Vec<String>, Error> {
    let logical_descriptor = table.rejected_versions_storage_table().record_schema();
    let mut fields = logical_descriptor
        .fields()
        .iter()
        .take(RejectedVersionRowRecord::USER_CELLS)
        .map(|field| {
            field.name.clone().ok_or(Error::InvalidStoredValue(
                "physical rejected-version system field unnamed",
            ))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for column in &table.columns {
        if present.is_some_and(|present| !present.contains(&column.name)) {
            continue;
        }
        let column_id =
            mapping
                .columns
                .get(&column.name)
                .copied()
                .ok_or(Error::InvalidStoredValue(
                    "physical rejected-version column mapping missing",
                ))?;
        fields.push(physical_user_column_field(column_id));
    }
    Ok(fields)
}

pub(crate) fn physical_column_epoch_is_compatible(
    source_table: &TableSchema,
    source_column_name: &str,
    target_table: &TableSchema,
    target_column_name: &str,
) -> bool {
    let Some(source_column) = source_table
        .columns
        .iter()
        .find(|column| column.name == source_column_name)
    else {
        return false;
    };
    let Some(target_column) = target_table
        .columns
        .iter()
        .find(|column| column.name == target_column_name)
    else {
        return false;
    };

    physical_value_epoch_is_compatible(&source_column.column_type, &target_column.column_type)
        && source_table.merge_strategy(source_column_name)
            == target_table.merge_strategy(target_column_name)
}

pub(crate) fn physical_value_epoch_is_compatible(
    source: &records::ValueType,
    target: &records::ValueType,
) -> bool {
    use records::ValueType;
    match (source, target) {
        (ValueType::EnumTag(left), ValueType::EnumTag(right)) => {
            right.variants.starts_with(&left.variants)
        }
        (ValueType::Enum(left), ValueType::Enum(right)) => {
            right.cases.len() >= left.cases.len()
                && left.cases.iter().zip(&right.cases).all(|(a, b)| {
                    a.name == b.name && physical_record_epoch_is_compatible(&a.payload, &b.payload)
                })
        }
        (ValueType::Tuple(left), ValueType::Tuple(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(a, b)| physical_value_epoch_is_compatible(a, b))
        }
        (ValueType::Array(left), ValueType::Array(right))
        | (ValueType::Nullable(left), ValueType::Nullable(right)) => {
            physical_value_epoch_is_compatible(left, right)
        }
        (ValueType::Record(left), ValueType::Record(right)) => {
            physical_record_epoch_is_compatible(left, right)
        }
        _ => source == target,
    }
}

fn physical_record_epoch_is_compatible(
    source: &records::RecordDescriptor,
    target: &records::RecordDescriptor,
) -> bool {
    source.fields().len() == target.fields().len()
        && source.fields().iter().zip(target.fields()).all(|(a, b)| {
            a.name == b.name && physical_value_epoch_is_compatible(&a.value_type, &b.value_type)
        })
}
