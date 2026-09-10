//! Jazz schema metadata and lowering to groove storage schemas. This module
//! owns table/column declarations, merge-strategy declarations, policy metadata,
//! storage-table naming, and migration-lens schema surfaces from
//! `jazz/SPEC/10_lenses_migrations.md`;
//! policy evaluation lives in [`crate::node::policy`], query shapes in
//! [`crate::query`], and runtime catalogue ingestion in [`crate::node::ingest`].
//! In the layer map it is the schema bridge from Jazz concepts to groove tables.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use groove::records::{
    EnumCase, EnumSchema, RecordDescriptor, ScalarEnumSchema, SystemVariantRegistry, Value,
    ValueType,
};
use groove::schema::{
    ColumnType as GrooveColumnType, DatabaseSchema as GrooveDatabaseSchema,
    DirectRecordStoreSchema, IndexSchema as GrooveIndexSchema, IntegerKeyType, PrimaryKey,
    PrimaryKeyColumn, TableSchema as GrooveTableSchema,
};
use groove::storage::StorageLayout;

use crate::ids::SchemaVersionId;
use crate::protocol::{BranchColumnValue, BranchKey, BranchSelector};
use crate::query::Query;
#[cfg(test)]
use crate::query::{claim, col, eq};
use crate::tools::public_schema::Schema as PublicSchema;

pub use crate::tools::public_schema_convert::SchemaConversionError;

/// Namespace used for schema-version UUIDv5 ids.
pub const SCHEMA_VERSION_NAMESPACE: uuid::Uuid =
    uuid::uuid!("61b9ef21-3195-50e8-87fc-2aa83a6f74e3");

/// Direct groove record store used for persisted fast known-state facts.
pub const KNOWN_STATE_FACTS_STORE: &str = "jazz_known_state_facts";
/// Direct groove record store used for persisted settled result memberships.
/// Direct groove record store used for persisted settled program facts.
pub const SETTLED_PROGRAM_FACTS_STORE: &str = "jazz_settled_program_facts";
/// Collision-checked directory from a fixed policy-binding digest to its full
/// authority identity. Result-store keys remain bounded without reducing the
/// runtime policy boundary to a hash-only identity.
pub const AUTHORITY_POLICY_BINDINGS_STORE: &str = "jazz_authority_policy_bindings";
/// Versioned local current-row availability and ordering receipts.
pub const LOCAL_ROW_AVAILABILITY_STORE: &str = "jazz_local_row_availability_v1";
/// Append-only proof that a scope-isolated relay actually received a row
/// version from its upstream authority. This is distinct from live result
/// membership, whose later removals only govern future disclosure.
pub const SCOPE_RELAY_REPAIR_LEDGER_STORE: &str = "jazz_scope_relay_repair_ledger";
/// Direct groove record store used to distinguish clean shutdown from crash
/// recovery windows for bounded startup repair.
pub const CLEAN_CLOSE_MARKERS_STORE: &str = "jazz_clean_close_markers";
/// Direct groove record store used to bound crash recovery work when a process
/// dies after a durable consistency boundary but before clean close runs.
pub const STORAGE_CONSISTENCY_MARKERS_STORE: &str = "jazz_storage_consistency_markers";
/// Node-local derived content-head table used to avoid row-history scans on
/// ordinary accepted writes. It is storage metadata, never wire or app data.
pub const MERGE_HEADS_TABLE: &str = "jazz_merge_heads";

/// Source-backed Jazz schema accepted by public database APIs.
///
/// The developer-authored source is retained for durable catalogue
/// publication while [`RuntimeSchema`] is the derived, in-memory engine form.
#[derive(Clone, Debug)]
pub struct JazzSchema {
    public_schema: PublicSchema,
    runtime: RuntimeSchema,
}

impl PartialEq for JazzSchema {
    fn eq(&self, other: &Self) -> bool {
        self.runtime == other.runtime
    }
}

impl JazzSchema {
    /// Construct an empty source-backed schema.
    pub fn empty() -> Self {
        Self::new(&PublicSchema::new()).expect("empty public schema always compiles")
    }

    /// Compile a developer-authored public schema and retain its durable source.
    pub fn new(schema: &PublicSchema) -> Result<Self, SchemaConversionError> {
        crate::tools::public_schema_convert::convert_public_schema(schema)
    }

    /// Return the developer-authored public schema retained for persistence.
    pub fn public_schema(&self) -> &PublicSchema {
        &self.public_schema
    }

    /// Return the compiled application tables used by the runtime.
    pub fn tables(&self) -> &[TableSchema] {
        &self.runtime.tables
    }

    pub(crate) fn from_runtime(public_schema: PublicSchema, runtime: RuntimeSchema) -> Self {
        Self {
            public_schema,
            runtime,
        }
    }

    pub(crate) fn runtime(&self) -> &RuntimeSchema {
        &self.runtime
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn runtime_mut_for_testing(&mut self) -> &mut RuntimeSchema {
        &mut self.runtime
    }

    #[cfg(test)]
    pub(crate) fn into_runtime(self) -> RuntimeSchema {
        self.runtime
    }

    /// Construct a compiled schema for internal engine tests whose tables
    /// already declare their branch columns.
    #[cfg(test)]
    pub(crate) fn new_with_branch_columns(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self::from_runtime(PublicSchema::new(), RuntimeSchema::new(tables))
    }
}

impl Deref for JazzSchema {
    type Target = RuntimeSchema;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

/// Compiled logical schema used internally by the Jazz engine.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct RuntimeSchema {
    /// Application tables in the schema.
    pub tables: Vec<TableSchema>,
}

impl PartialEq for RuntimeSchema {
    fn eq(&self, other: &Self) -> bool {
        self.tables == other.tables
    }
}

impl RuntimeSchema {
    /// Construct a compiled schema from already-lowered application tables.
    #[cfg(test)]
    pub(crate) fn new(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
        }
        .validated()
    }

    /// Project a named selector onto one table and validate its exact branch key.
    pub fn project_branch_selector(
        &self,
        table: &TableSchema,
        selector: &BranchSelector,
    ) -> Result<(BranchKey, BTreeMap<String, Value>), String> {
        if selector.values.len() != table.branch_by.len() {
            return Err(format!(
                "branch selector for {} must provide exactly {} values",
                table.name,
                table.branch_by.len()
            ));
        }
        let mut values = Vec::with_capacity(table.branch_by.len());
        let mut cells = BTreeMap::new();
        for column_name in &table.branch_by {
            let column = table
                .columns
                .iter()
                .find(|column| &column.name == column_name)
                .ok_or_else(|| {
                    format!("unknown branch column {column_name:?} on {}", table.name)
                })?;
            let encoded = selector
                .values
                .get(column_name)
                .ok_or_else(|| format!("missing branch column {column_name}"))?;
            let value = encoded
                .decode()
                .map_err(|_| format!("invalid branch column {column_name} encoding"))?;
            RecordDescriptor::new([("value", column.column_type.clone())])
                .create(std::slice::from_ref(&value))
                .map_err(|_| format!("invalid branch column {column_name} value"))?;
            let encoded = BranchColumnValue::encode_typed(&value, &column.column_type)
                .map_err(|_| format!("invalid branch column {column_name} value"))?;
            values.push((column_name.clone(), encoded));
            cells.insert(column_name.clone(), value);
        }
        values.sort_by(|left, right| left.0.cmp(&right.0));
        Ok((BranchKey { values }, cells))
    }

    /// Validate a schema-wide view selector, then project it onto one table's subset.
    pub fn project_branch_view_selector(
        &self,
        table: &TableSchema,
        selector: &BranchSelector,
    ) -> Result<(BranchKey, BTreeMap<String, Value>), String> {
        let schema_branch_columns = self
            .tables
            .iter()
            .flat_map(|table| table.branch_by.iter().cloned())
            .collect::<BTreeSet<_>>();
        if selector.values.keys().cloned().collect::<BTreeSet<_>>() != schema_branch_columns {
            return Err(
                "branch view selector must provide every schema branch column exactly once"
                    .to_owned(),
            );
        }
        let projected = BranchSelector {
            values: table
                .branch_by
                .iter()
                .map(|column| (column.clone(), selector.values[column].clone()))
                .collect(),
        };
        self.project_branch_selector(table, &projected)
    }

    /// Compare a stored key with a selector in this schema, interpreting
    /// branch columns absent from older monotone schemas at their defaults.
    pub(crate) fn branch_key_matches(
        &self,
        table: &TableSchema,
        stored: &BranchKey,
        selected: &BranchKey,
    ) -> bool {
        table.branch_by.iter().all(|column_name| {
            let column = table
                .columns
                .iter()
                .find(|column| column.name == *column_name)
                .expect("validated branch column");
            let selected = selected
                .values
                .iter()
                .find(|(name, _)| name == column_name)
                .map(|(_, value)| value);
            let stored = stored
                .values
                .iter()
                .find(|(name, _)| name == column_name)
                .map(|(_, value)| value)
                .cloned()
                .or_else(|| {
                    column.default.as_ref().map(|value| {
                        BranchColumnValue::encode_typed(value, &column.column_type)
                            .expect("validated branch default")
                    })
                });
            selected == stored.as_ref()
        }) && stored
            .values
            .iter()
            .all(|(name, _)| table.branch_by.contains(name))
    }

    /// Expand an older table-local key with immutable defaults from the
    /// current monotone branch-column declaration.
    pub(crate) fn normalize_branch_key(
        &self,
        table: &TableSchema,
        stored: &BranchKey,
    ) -> Result<BranchKey, String> {
        if stored
            .values
            .iter()
            .any(|(name, _)| !table.branch_by.contains(name))
        {
            return Err(format!(
                "stored branch key has an unknown column on {}",
                table.name
            ));
        }
        let mut values = table
            .branch_by
            .iter()
            .map(|column_name| {
                let column = table
                    .columns
                    .iter()
                    .find(|column| column.name == *column_name)
                    .ok_or_else(|| format!("unknown branch column on {}", table.name))?;
                let value = stored
                    .values
                    .iter()
                    .find(|(name, _)| name == column_name)
                    .map(|(_, value)| value.clone())
                    .or_else(|| {
                        column.default.as_ref().map(|value| {
                            BranchColumnValue::encode_typed(value, &column.column_type)
                                .expect("validated branch default")
                        })
                    })
                    .ok_or_else(|| {
                        format!(
                            "older branch key is missing {column_name} without a migration default"
                        )
                    })?;
                Ok((column_name.clone(), value))
            })
            .collect::<Result<Vec<_>, String>>()?;
        values.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(BranchKey { values })
    }

    /// Validate an authored table-local branch key without applying migration
    /// defaults. Wire versions must carry the exact key declared by their own
    /// schema: accepting a short or non-canonical spelling here would let a
    /// peer change the physical branch coordinate independently of row cells.
    pub(crate) fn validate_authored_branch_key(
        &self,
        table: &TableSchema,
        key: &BranchKey,
    ) -> Result<BTreeMap<String, Value>, String> {
        Self::validate_branch_key_for_table(table, key)
    }

    /// Decode one persisted branch coordinate using the table that owns it.
    ///
    /// The binary envelope is only self-describing enough to reject malformed
    /// bytes. The table's branch declaration is the authority for component
    /// names, scalar types, and stable-enum domains, so recovery must perform
    /// this second validation before a physical coordinate can influence a
    /// query or cache.
    pub(crate) fn decode_persisted_branch_key(
        table: &TableSchema,
        bytes: &[u8],
    ) -> Result<BranchKey, String> {
        let key = BranchKey::from_canonical_bytes(bytes)
            .map_err(|_| format!("invalid persisted branch key for {}", table.name))?;
        Self::validate_branch_key_for_table(table, &key)?;
        Ok(key)
    }

    /// Validate an exact branch coordinate against its owning table
    /// declaration. Shared by admission and persisted-state recovery so their
    /// type/domain rules cannot drift.
    pub(crate) fn validate_branch_key_for_table(
        table: &TableSchema,
        key: &BranchKey,
    ) -> Result<BTreeMap<String, Value>, String> {
        let mut bindings = table.branch_by.iter().collect::<Vec<_>>();
        bindings.sort();
        if key.values.len() != bindings.len() {
            return Err(format!(
                "branch key for {} must contain exactly {} values",
                table.name,
                bindings.len()
            ));
        }

        let mut cells = BTreeMap::new();
        for (column_name, (actual_name, encoded)) in bindings.into_iter().zip(&key.values) {
            if actual_name != column_name {
                return Err(format!(
                    "branch key for {} is not in canonical column order",
                    table.name
                ));
            }
            let column = table
                .columns
                .iter()
                .find(|column| column.name == *column_name)
                .ok_or_else(|| format!("unknown branch column on {}", table.name))?;
            let value = encoded
                .decode_as(&column.column_type)
                .map_err(|_| format!("invalid branch column {column_name} encoding"))?;
            if BranchColumnValue::encode_typed(&value, &column.column_type)
                .map_err(|_| format!("invalid branch column {column_name} value"))?
                != *encoded
            {
                return Err(format!(
                    "non-canonical branch column {column_name} encoding"
                ));
            }
            RecordDescriptor::new([("value", column.column_type.clone())])
                .create(std::slice::from_ref(&value))
                .map_err(|_| format!("invalid branch column {column_name} value"))?;
            cells.insert(column_name.clone(), value);
        }
        Ok(cells)
    }

    /// Reconstruct the table-local named selector for an exact stored key.
    pub(crate) fn branch_selector_for_key(
        &self,
        table: &TableSchema,
        key: &BranchKey,
    ) -> Result<BranchSelector, String> {
        let mut values = BTreeMap::new();
        for column_name in &table.branch_by {
            let column = table
                .columns
                .iter()
                .find(|column| column.name == *column_name)
                .ok_or_else(|| format!("unknown branch column on {}", table.name))?;
            let value = key
                .values
                .iter()
                .find(|(name, _)| name == column_name)
                .map(|(_, value)| value.clone())
                .or_else(|| {
                    column.default.as_ref().map(|value| {
                        BranchColumnValue::encode_typed(value, &column.column_type)
                            .expect("validated branch default")
                    })
                })
                .ok_or_else(|| {
                    format!("branch key is missing {column_name} without a migration default")
                })?;
            values.insert(column_name.clone(), value);
        }
        Ok(BranchSelector { values })
    }

    #[cfg(test)]
    fn validated(self) -> Self {
        let mut branch_column_types = BTreeMap::new();
        for table in &self.tables {
            let mut bound_columns = BTreeSet::new();
            for column_name in &table.branch_by {
                let column = table
                    .columns
                    .iter()
                    .find(|column| column.name == *column_name)
                    .unwrap_or_else(|| {
                        panic!("unknown branch column {}.{column_name}", table.name)
                    });
                assert!(
                    bound_columns.insert(column_name),
                    "duplicate table branch column"
                );
                assert!(
                    !matches!(column.column_type, GrooveColumnType::Nullable(_)),
                    "branch columns must be non-nullable"
                );
                assert!(
                    matches!(
                        column.column_type,
                        GrooveColumnType::String
                            | GrooveColumnType::Uuid
                            | GrooveColumnType::U8
                            | GrooveColumnType::U16
                            | GrooveColumnType::U32
                            | GrooveColumnType::U64
                            | GrooveColumnType::I32
                            | GrooveColumnType::I64
                            | GrooveColumnType::EnumTag(_)
                    ),
                    "branch columns require string, UUID, stable enum, or fixed-width integer values"
                );
                if let Some(existing) =
                    branch_column_types.insert(column_name.clone(), column.column_type.clone())
                {
                    assert_eq!(
                        existing, column.column_type,
                        "same-named branch columns must have the same type"
                    );
                }
            }
            for (column_name, strategy) in &table.merge_strategies {
                let column = table
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == *column_name)
                    .unwrap_or_else(|| {
                        panic!(
                            "merge strategy declared for unknown column {}.{}",
                            table.name, column_name
                        )
                    });
                match strategy {
                    MergeStrategy::Lww => {}
                    MergeStrategy::Counter => {
                        assert!(
                            is_counter_column_type(&column.column_type),
                            "counter merge strategy requires a non-nullable integer column: {}.{}",
                            table.name,
                            column.name
                        );
                    }
                    MergeStrategy::GSet => {
                        assert!(
                            is_gset_column_type(&column.column_type),
                            "g-set merge strategy requires a non-nullable array column: {}.{}",
                            table.name,
                            column.name
                        );
                    }
                }
            }
            if let Some(policy) = &table.read_policy {
                assert_eq!(policy.table, table.name, "read policy table must match");
                policy.validate_runtime(&self).unwrap_or_else(|error| {
                    panic!("valid read policy shape for {}: {error:?}", table.name)
                });
            }
            for (label, policy) in table.write_policies.iter() {
                assert_eq!(
                    policy.table, table.name,
                    "{label} write policy table must match"
                );
                policy
                    .validate_runtime(&self)
                    .unwrap_or_else(|_| panic!("valid {label} write policy shape"));
            }
        }
        self
    }

    /// Lower the Jazz schema into groove storage tables.
    pub fn lower_to_groove(&self) -> GrooveDatabaseSchema {
        self.with_jazz_direct_record_stores(GrooveDatabaseSchema::new(self.storage_tables()))
    }

    /// Lower only the fixed metadata tables needed for the first open stage.
    pub fn lower_catalogue_meta_to_groove(&self) -> GrooveDatabaseSchema {
        self.with_jazz_direct_record_stores(GrooveDatabaseSchema::new(
            self.catalogue_meta_storage_tables(),
        ))
    }

    /// Return the required RocksDB column-family names.
    pub fn column_families(&self) -> Vec<String> {
        let lowered = self.lower_to_groove();
        StorageLayout::jazz_class_v1().physical_column_families(
            lowered
                .column_families()
                .into_iter()
                .chain(std::iter::once("indices"))
                // A schema-independent runtime may open before its first typed
                // application view is registered. Keep the shared physical
                // row classes available even for the empty bootstrap schema.
                .chain(std::iter::once("jazz_physical_history"))
                .chain(std::iter::once("jazz_physical_register"))
                .chain(std::iter::once("jazz_physical_global_current"))
                .chain(std::iter::once("jazz_physical_ahead_current")),
        )
    }

    /// Return physical storage column-family names for Jazz's class-CF layout.
    pub fn physical_column_families(&self) -> Vec<String> {
        self.column_families()
    }

    /// Return all storage tables used by Jazz.
    pub fn storage_tables(&self) -> Vec<GrooveTableSchema> {
        let mut tables = vec![
            nodes_table(),
            schema_versions_table(),
            catalogue_table(),
            catalogue_pointer_table(),
            transactions_table(),
            rejected_transactions_table(),
            pending_edges_table(),
            merge_heads_table(),
        ];
        tables.push(global_changes_table());
        tables.push(shared_deletion_history_table());
        tables
    }

    /// Return the version-independent metadata tables used by staged catalogue open.
    pub fn catalogue_meta_storage_tables(&self) -> Vec<GrooveTableSchema> {
        vec![
            nodes_table(),
            schema_versions_table(),
            catalogue_table(),
            catalogue_pointer_table(),
        ]
    }

    /// Return the canonical byte encoding used to address this schema version.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        canonical_schema_bytes(self)
    }

    /// Return the content-addressed id for this schema.
    pub fn version_id(&self) -> SchemaVersionId {
        SchemaVersionId(uuid::Uuid::new_v5(
            &SCHEMA_VERSION_NAMESPACE,
            &self.canonical_bytes(),
        ))
    }

    fn with_jazz_direct_record_stores(&self, schema: GrooveDatabaseSchema) -> GrooveDatabaseSchema {
        // Ordered direct-store key audit (storage format V1): every authority
        // result is addressed by three UUIDs, a scope discriminator and a
        // fixed 256-bit policy-binding directory ID. Application-shaped
        // claims remain in the collision-checked directory value, never in
        // an ordered B-tree key.
        schema
            .with_direct_record_store(DirectRecordStoreSchema::new(
                AUTHORITY_POLICY_BINDINGS_STORE,
                RecordDescriptor::new([("policy_binding_digest", ValueType::Bytes)]),
                RecordDescriptor::new([
                    ("subject", ValueType::String),
                    (
                        "claims_v1",
                        crate::protocol::policy_binding_directory_claims_value_type(),
                    ),
                ]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                KNOWN_STATE_FACTS_STORE,
                RecordDescriptor::new([
                    ("shape_id", ValueType::Uuid),
                    ("binding_id", ValueType::Uuid),
                    ("read_view_id", ValueType::Uuid),
                    ("policy_scope", ValueType::U8),
                    ("policy_binding_digest", ValueType::Bytes),
                ]),
                RecordDescriptor::new([
                    ("settled_through", ValueType::U64),
                    ("authorization_progress", ValueType::U64),
                    // A nonzero generation proves the persisted program
                    // facts are an exact CoveredInput closure. It is
                    // deliberately distinct from the live authority receipt.
                    ("source_closure_generation", ValueType::U64),
                ]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                SETTLED_PROGRAM_FACTS_STORE,
                RecordDescriptor::new([
                    ("shape_id", ValueType::Uuid),
                    ("binding_id", ValueType::Uuid),
                    ("read_view_id", ValueType::Uuid),
                    ("policy_scope", ValueType::U8),
                    ("policy_binding_digest", ValueType::Bytes),
                    // Canonical source closure facts are addressed by digest.
                    // Their exact source roles and version references remain
                    // in the validated record body.
                    ("fact_digest", ValueType::Bytes),
                ]),
                RecordDescriptor::new([("fact", ValueType::Bytes)]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                LOCAL_ROW_AVAILABILITY_STORE,
                RecordDescriptor::new([
                    ("policy_binding_digest", ValueType::Bytes),
                    ("global_table", ValueType::Uuid),
                    ("row", ValueType::Uuid),
                ]),
                crate::node::local_availability_record_descriptor(),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                SCOPE_RELAY_REPAIR_LEDGER_STORE,
                RecordDescriptor::new([
                    ("scope_digest", ValueType::Bytes),
                    ("physical_table_id", ValueType::U64),
                    ("row_id", ValueType::Uuid),
                    ("tx_time", ValueType::U64),
                    ("tx_node", ValueType::Uuid),
                ]),
                // Full scope components are values so host supplied strings
                // never enter a backend's bounded ordered-key space.
                RecordDescriptor::new([
                    ("format_version", ValueType::U64),
                    ("storage_owner", ValueType::String),
                    (
                        "admitted_subject",
                        ValueType::Nullable(Box::new(ValueType::String)),
                    ),
                ]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                CLEAN_CLOSE_MARKERS_STORE,
                RecordDescriptor::new([("marker", ValueType::String)]),
                RecordDescriptor::new([("version", ValueType::U64), ("node", ValueType::Uuid)]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                STORAGE_CONSISTENCY_MARKERS_STORE,
                RecordDescriptor::new([("marker", ValueType::String)]),
                RecordDescriptor::new([
                    ("version", ValueType::U64),
                    ("node", ValueType::Uuid),
                    ("tx_time", ValueType::U64),
                ]),
            ))
    }
}

/// Per-column strategy used when upstream nodes merge concurrent content heads.
///
/// Counter deltas are represented on the wire as an absolute user cell plus the
/// version's parents. The observed base is reconstructed from the parent set;
/// merge computes `merged(parent union) + sum(version_value - merged(parents))`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum MergeStrategy {
    /// Highest HLC/TxId value wins for this column.
    #[default]
    Lww,
    /// Concurrent integer deltas from observed bases are summed.
    Counter,
    /// Array elements grow monotonically by union over the version DAG.
    GSet,
}

#[cfg(test)]
fn is_counter_column_type(column_type: &GrooveColumnType) -> bool {
    matches!(
        column_type,
        GrooveColumnType::U8
            | GrooveColumnType::U16
            | GrooveColumnType::U32
            | GrooveColumnType::U64
            | GrooveColumnType::I32
            | GrooveColumnType::I64
    )
}

#[cfg(test)]
fn is_gset_column_type(column_type: &GrooveColumnType) -> bool {
    matches!(column_type, GrooveColumnType::Array(_))
}

/// Semantics declared for a built-in column transform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ColumnTransformSemantics {
    /// The transform has a total inverse over its declared value domain.
    pub(crate) bijective: bool,
    /// Equal canonical source values remain equal after transform and inverse.
    pub(crate) canonical_equality_preserving: bool,
}

/// Return the semantics for a registered built-in column transform.
pub(crate) fn registered_column_transform(key: &str) -> Option<ColumnTransformSemantics> {
    match key {
        "identity" | "jazz.identity" => Some(ColumnTransformSemantics {
            bijective: true,
            canonical_equality_preserving: true,
        }),
        _ => None,
    }
}

/// Internal schema-derived physical interpretation for a scalar column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum LargeValueSemanticKind {
    /// The column never uses the hidden large-scalar representation.
    NotLarge,
    /// Byte scalar semantics.
    Bytes,
    /// UTF-8 text scalar semantics.
    String,
    /// Canonical JSON-in-string scalar semantics.
    Json,
}

/// Application column declaration.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ColumnSchema {
    /// Logical column name.
    pub(crate) name: String,
    /// Groove storage type used for this column's cell value.
    pub(crate) column_type: GrooveColumnType,
    /// Immutable, schema-derived semantic kind used only by the hidden
    /// physical large-scalar envelope. JSON remains logically string-shaped in
    /// Groove, but cannot be decoded or staged as text at this boundary.
    pub(crate) large_value_kind: LargeValueSemanticKind,
    /// Literal value used when an insert omits this column.
    #[serde(default)]
    pub(crate) default: Option<Value>,
}

#[derive(serde::Deserialize)]
struct SerializedColumnSchema {
    name: String,
    column_type: GrooveColumnType,
    #[serde(default)]
    large_value_kind: Option<LargeValueSemanticKind>,
    #[serde(default)]
    default: Option<Value>,
}

impl<'de> serde::Deserialize<'de> for ColumnSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedColumnSchema::deserialize(deserializer)?;
        if contains_internal_storage_type(&serialized.column_type) {
            return Err(serde::de::Error::custom(
                "internal Groove storage types cannot appear in a Jazz schema",
            ));
        }
        let derived = large_value_kind_for_type(&serialized.column_type);
        let large_value_kind = serialized.large_value_kind.unwrap_or(derived);
        let kind_is_valid = large_value_kind == derived
            || (large_value_kind == LargeValueSemanticKind::Json
                && derived == LargeValueSemanticKind::String);
        if !kind_is_valid {
            return Err(serde::de::Error::custom(
                "large-value semantic kind does not match the declared column type",
            ));
        }
        Ok(Self {
            name: serialized.name,
            column_type: serialized.column_type,
            large_value_kind,
            default: serialized.default,
        })
    }
}

impl ColumnSchema {
    /// Logical column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Public logical Groove type. Physical large-scalar backing types remain sealed.
    pub fn column_type(&self) -> &GrooveColumnType {
        &self.column_type
    }

    /// Construct an ordinary column from a groove storage type.
    pub fn new(name: impl Into<String>, column_type: GrooveColumnType) -> Self {
        assert!(
            !contains_internal_storage_type(&column_type),
            "raw and stored-scalar value types are physical-only and cannot be declared in a Jazz schema"
        );
        Self {
            name: name.into(),
            large_value_kind: large_value_kind_for_type(&column_type),
            column_type,
            default: None,
        }
    }

    /// Attach a literal insert default to this column.
    pub fn with_default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }
}

fn contains_internal_storage_type(column_type: &GrooveColumnType) -> bool {
    if column_type.is_internal_storage_type() {
        return true;
    }
    match column_type {
        GrooveColumnType::Nullable(inner) | GrooveColumnType::Array(inner) => {
            contains_internal_storage_type(inner)
        }
        GrooveColumnType::Tuple(members) => members.iter().any(contains_internal_storage_type),
        GrooveColumnType::Record(descriptor) => descriptor
            .fields()
            .iter()
            .any(|field| contains_internal_storage_type(&field.value_type)),
        GrooveColumnType::Enum(schema) => schema.cases.iter().any(|case| {
            case.payload
                .fields()
                .iter()
                .any(|field| contains_internal_storage_type(&field.value_type))
        }),
        _ => false,
    }
}

fn large_value_kind_for_type(column_type: &GrooveColumnType) -> LargeValueSemanticKind {
    match column_type {
        GrooveColumnType::String => LargeValueSemanticKind::String,
        GrooveColumnType::Bytes => LargeValueSemanticKind::Bytes,
        GrooveColumnType::Nullable(inner) => large_value_kind_for_type(inner),
        _ => LargeValueSemanticKind::NotLarge,
    }
}

/// Storage descriptors are schema-derived. JSON remains string-shaped to
/// callers and operators, but its internal cell codec is distinct so a large
/// JSON descriptor cannot be mistaken for text.
pub(crate) fn storage_column_type(column: &ColumnSchema) -> GrooveColumnType {
    match column.large_value_kind {
        LargeValueSemanticKind::Json => {
            let physical = groove::large_values::physical_storage_value_type(
                groove::large_values::LargeValueKind::Json,
            );
            // Column nullability is distinct from the enclosing version record's
            // "this column was authored" nullable slot. Keep both wrappers.
            if matches!(column.column_type, GrooveColumnType::Nullable(_)) {
                physical.nullable()
            } else {
                physical
            }
        }
        _ => column.column_type.clone(),
    }
}

impl From<groove::schema::ColumnSchema> for ColumnSchema {
    fn from(column: groove::schema::ColumnSchema) -> Self {
        Self {
            name: column.name,
            large_value_kind: large_value_kind_for_type(&column.column_type),
            column_type: column.column_type,
            default: None,
        }
    }
}

/// Operation-specific write policy clauses for an application table.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WritePolicies {
    /// Policy evaluated against the inserted row.
    #[serde(default)]
    pub insert_check: Option<Query>,
    /// Policy evaluated against the row before an update.
    #[serde(default)]
    pub update_using: Option<Query>,
    /// Policy evaluated against the row after an update.
    #[serde(default)]
    pub update_check: Option<Query>,
    /// Policy evaluated against the row being deleted.
    #[serde(default)]
    pub delete_using: Option<Query>,
}

impl WritePolicies {
    /// Build operation-specific clauses from the legacy single write policy.
    pub fn legacy(policy: Option<Query>) -> Self {
        Self {
            insert_check: policy.clone(),
            update_using: policy.clone(),
            update_check: policy.clone(),
            delete_using: policy,
        }
    }

    /// Iterate over every present operation-specific clause.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &Query)> {
        [
            ("insert_check", self.insert_check.as_ref()),
            ("update_using", self.update_using.as_ref()),
            ("update_check", self.update_check.as_ref()),
            ("delete_using", self.delete_using.as_ref()),
        ]
        .into_iter()
        .filter_map(|(label, policy)| policy.map(|policy| (label, policy)))
    }

    /// Return one representative policy for coarse subscription scoping.
    pub fn any(&self) -> Option<Query> {
        self.insert_check
            .clone()
            .or_else(|| self.update_check.clone())
            .or_else(|| self.update_using.clone())
            .or_else(|| self.delete_using.clone())
    }

    /// Whether this table declares any write authorization clause.
    pub fn has_any(&self) -> bool {
        self.insert_check.is_some()
            || self.update_using.is_some()
            || self.update_check.is_some()
            || self.delete_using.is_some()
    }
}

/// Application table whose rows are stored as immutable history versions.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TableSchema {
    /// Logical table name.
    pub name: String,
    /// User columns.
    pub columns: Vec<ColumnSchema>,
    /// Ordinary columns that form this table's exact branch key.
    #[serde(default)]
    pub branch_by: Vec<String>,
    /// Jazz-level reference metadata by source column name.
    pub references: BTreeMap<String, String>,
    /// Read policy used when serving views.
    pub read_policy: Option<Query>,
    /// Write policies used by fate authority.
    #[serde(default)]
    pub write_policies: WritePolicies,
    /// User columns materialized and indexed on the global-current content table.
    #[serde(default)]
    pub indexed_columns: BTreeSet<String>,
    /// Per-column merge strategy. Columns omitted here use [`MergeStrategy::Lww`].
    #[serde(default)]
    pub merge_strategies: BTreeMap<String, MergeStrategy>,
}

impl TableSchema {
    /// Construct a public/read-anyone table.
    pub(crate) fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<ColumnSchema>>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            branch_by: Vec::new(),
            references: BTreeMap::new(),
            read_policy: None,
            write_policies: WritePolicies::default(),
            indexed_columns: BTreeSet::new(),
            merge_strategies: BTreeMap::new(),
        }
    }

    /// Whether this table has opted into authorization.
    ///
    /// A completely policy-free table is intentionally open so a new app can
    /// use its data before it has introduced permissions. Once a table
    /// declares any read or write clause, its policy set is closed: an
    /// omitted operation has no grant and therefore denies at the authority.
    pub fn has_any_policy(&self) -> bool {
        self.read_policy.is_some() || self.write_policies.has_any()
    }

    /// Add an ordinary application column to this table's branch key.
    pub fn with_branch_column(mut self, column: impl Into<String>) -> Self {
        self.branch_by.push(column.into());
        self
    }

    /// Mark a user column as referencing another Jazz table.
    #[cfg(test)]
    pub(crate) fn with_reference(
        mut self,
        column: impl Into<String>,
        target_table: impl Into<String>,
    ) -> Self {
        self.references.insert(column.into(), target_table.into());
        self
    }

    /// Set a user column's merge strategy.
    #[cfg(test)]
    pub(crate) fn with_column_merge_strategy(
        mut self,
        column: impl Into<String>,
        strategy: MergeStrategy,
    ) -> Self {
        self.merge_strategies.insert(column.into(), strategy);
        self
    }

    /// Return the merge strategy for a user column.
    pub fn merge_strategy(&self, column: &str) -> MergeStrategy {
        self.merge_strategies
            .get(column)
            .copied()
            .unwrap_or_default()
    }

    /// Set the table read policy.
    #[cfg(test)]
    pub(crate) fn with_read_policy(mut self, read_policy: impl Into<Option<Query>>) -> Self {
        self.read_policy = read_policy.into();
        self
    }

    /// Set the table write policy.
    #[cfg(test)]
    pub(crate) fn with_write_policy(mut self, write_policy: impl Into<Option<Query>>) -> Self {
        self.write_policies = WritePolicies::legacy(write_policy.into());
        self
    }

    /// Set operation-specific write policies.
    #[cfg(test)]
    pub(crate) fn with_write_policies(mut self, write_policies: WritePolicies) -> Self {
        self.write_policies = write_policies;
        self
    }

    /// Return the storage table for rejected versions of this application table.
    pub fn rejected_versions_storage_table(&self) -> GrooveTableSchema {
        let mut columns = vec![
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("row_uuid", GrooveColumnType::Uuid),
            column("layer", GrooveColumnType::Bytes),
            column("parents", tx_id_column().array_of()),
            column("_deletion", deletion_column().nullable()),
        ];
        columns.extend(self.columns.iter().map(|user_column| {
            column(
                app_storage_column_name(&user_column.name),
                storage_column_type(user_column).nullable(),
            )
        }));
        GrooveTableSchema::new(format!("jazz_{}_rejected_versions", self.name), columns)
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::bytes("layer"),
            ]))
    }

    /// Return the storage history table for this application table.
    pub fn history_storage_table(&self) -> GrooveTableSchema {
        self.history_storage_table_named(format!("jazz_{}_history", self.name))
    }

    /// Alias kept at the codec boundary to make the physical interpretation
    /// explicit at call sites.
    pub(crate) fn authored_history_storage_table(&self) -> GrooveTableSchema {
        self.history_storage_table()
    }

    fn history_storage_table_named(&self, name: String) -> GrooveTableSchema {
        let mut columns = vec![
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", crate::ids::RowAuthor::value_type()),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", crate::ids::RowAuthor::value_type()),
            column("updated_at", GrooveColumnType::U64),
        ];
        columns.extend(self.columns.iter().map(|user_column| {
            column(
                app_storage_column_name(&user_column.name),
                storage_column_type(user_column).nullable(),
            )
        }));
        // Absent on legacy records. When present, this is a strictly ordered
        // set of node-local physical column ids explicitly authored by this
        // version. Logical names remain a wire/schema-boundary concern.
        columns.push(column(
            "authored_columns",
            GrooveColumnType::U64.array_of().nullable(),
        ));

        GrooveTableSchema::new(name, columns)
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("branch_key"),
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
            ]))
            .with_index(GrooveIndexSchema::new(
                "by_tx",
                ["tx_time", "tx_node_id", "branch_key", "row_uuid"],
            ))
    }

    /// Return the storage table for deletion-register versions.
    pub fn register_storage_table(&self) -> GrooveTableSchema {
        self.register_storage_table_named(format!("jazz_{}_register", self.name))
    }

    fn register_storage_table_named(&self, name: String) -> GrooveTableSchema {
        GrooveTableSchema::new(
            name,
            [
                column("branch_key", GrooveColumnType::Bytes),
                column("row_uuid", GrooveColumnType::Uuid),
                column("tx_time", GrooveColumnType::U64),
                column("tx_node_id", GrooveColumnType::U64),
                column("schema_version", GrooveColumnType::U64),
                column("parents", tx_id_column().array_of()),
                column("created_by", crate::ids::RowAuthor::value_type()),
                column("created_at", GrooveColumnType::U64),
                column("updated_by", crate::ids::RowAuthor::value_type()),
                column("updated_at", GrooveColumnType::U64),
                column("_deletion", deletion_column()),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::bytes("branch_key"),
            PrimaryKeyColumn::uuid("row_uuid"),
            PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
        ]))
        .with_index(GrooveIndexSchema::new(
            "by_tx",
            ["tx_time", "tx_node_id", "branch_key", "row_uuid"],
        ))
    }

    /// Return per-layer global-current tables for content and register winners.
    pub fn global_current_storage_tables(&self) -> Vec<GrooveTableSchema> {
        let indexed_columns = self.global_current_indexed_columns();
        let mut content_columns = vec![
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", crate::ids::RowAuthor::value_type()),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", crate::ids::RowAuthor::value_type()),
            column("updated_at", GrooveColumnType::U64),
            column("global_time", GrooveColumnType::U64.nullable()),
        ];
        // Carry every user column (not only indexed ones) so the global-current
        // table is a self-sufficient current-row index: whole-table current
        // reads and subscriptions resolve cells here in O(current rows) without
        // joining the full history table. Secondary indexes are still built only
        // on the indexed subset below.
        content_columns.extend(self.columns.iter().map(|user_column| {
            column(
                app_storage_column_name(&user_column.name),
                storage_column_type(user_column).nullable(),
            )
        }));
        content_columns.push(column(
            "authored_columns",
            GrooveColumnType::U64.array_of().nullable(),
        ));
        let mut content_table = GrooveTableSchema::new(
            format!("jazz_{}_global_current", self.name),
            content_columns,
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::bytes("branch_key"),
            PrimaryKeyColumn::uuid("row_uuid"),
        ]));
        for indexed in &indexed_columns {
            content_table = content_table.with_index(GrooveIndexSchema::new(
                global_current_index_name(indexed),
                ["branch_key".to_owned(), app_storage_column_name(indexed)],
            ));
        }
        vec![
            content_table,
            GrooveTableSchema::new(
                format!("jazz_{}_register_global_current", self.name),
                [
                    column("branch_key", GrooveColumnType::Bytes),
                    column("row_uuid", GrooveColumnType::Uuid),
                    column("tx_time", GrooveColumnType::U64),
                    column("tx_node_id", GrooveColumnType::U64),
                    column("schema_version", GrooveColumnType::U64),
                    column("parents", tx_id_column().array_of()),
                    column("created_by", crate::ids::RowAuthor::value_type()),
                    column("created_at", GrooveColumnType::U64),
                    column("updated_by", crate::ids::RowAuthor::value_type()),
                    column("updated_at", GrooveColumnType::U64),
                    column("global_time", GrooveColumnType::U64.nullable()),
                    column("_deletion", deletion_column()),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("branch_key"),
                PrimaryKeyColumn::uuid("row_uuid"),
            ])),
        ]
    }

    /// Return per-layer ahead-of-global candidate tables.
    pub fn ahead_current_storage_tables(&self) -> Vec<GrooveTableSchema> {
        let mut content_columns = vec![
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", crate::ids::RowAuthor::value_type()),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", crate::ids::RowAuthor::value_type()),
            column("updated_at", GrooveColumnType::U64),
            column("global_time", GrooveColumnType::U64.nullable()),
        ];
        content_columns.extend(self.columns.iter().map(|user_column| {
            column(
                app_storage_column_name(&user_column.name),
                storage_column_type(user_column).nullable(),
            )
        }));
        content_columns.push(column(
            "authored_columns",
            GrooveColumnType::U64.array_of().nullable(),
        ));
        vec![
            GrooveTableSchema::new(format!("jazz_{}_ahead_current", self.name), content_columns)
                .with_primary_key(PrimaryKey::composite([
                    PrimaryKeyColumn::bytes("branch_key"),
                    PrimaryKeyColumn::uuid("row_uuid"),
                    PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                    PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
                ]))
                .with_index(GrooveIndexSchema::new(
                    "by_tx",
                    ["tx_time", "tx_node_id", "branch_key", "row_uuid"],
                )),
            GrooveTableSchema::new(
                format!("jazz_{}_register_ahead_current", self.name),
                [
                    column("branch_key", GrooveColumnType::Bytes),
                    column("row_uuid", GrooveColumnType::Uuid),
                    column("tx_time", GrooveColumnType::U64),
                    column("tx_node_id", GrooveColumnType::U64),
                    column("schema_version", GrooveColumnType::U64),
                    column("parents", tx_id_column().array_of()),
                    column("created_by", crate::ids::RowAuthor::value_type()),
                    column("created_at", GrooveColumnType::U64),
                    column("updated_by", crate::ids::RowAuthor::value_type()),
                    column("updated_at", GrooveColumnType::U64),
                    column("global_time", GrooveColumnType::U64.nullable()),
                    column("_deletion", deletion_column()),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("branch_key"),
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
            ]))
            .with_index(GrooveIndexSchema::new(
                "by_tx",
                ["tx_time", "tx_node_id", "branch_key", "row_uuid"],
            )),
        ]
    }

    /// Columns available for constrained global-current reads.
    pub fn global_current_indexed_columns(&self) -> BTreeSet<String> {
        self.references
            .keys()
            .cloned()
            .chain(self.indexed_columns.iter().cloned())
            .collect()
    }

    /// Return the wire descriptor for replicated immutable row payloads.
    ///
    /// Wire records contain row payload data and immutable row provenance:
    /// `row_uuid`, `parents`, provenance, `_deletion`, and nullable user cells.
    /// Receiver-local currentness and authority-state columns are deliberately
    /// excluded. Schema changes change this descriptor; v1 requires identical
    /// descriptors at sender and receiver. JSON cells retain their schema-derived
    /// stored-scalar kind so indirect JSON is never encoded as ordinary text.
    pub fn wire_record_descriptor(&self) -> RecordDescriptor {
        RecordDescriptor::new(
            [
                ("row_uuid".to_owned(), ValueType::Uuid),
                (
                    "parents".to_owned(),
                    ValueType::Array(Box::new(tx_id_column().clone())),
                ),
                ("created_by".to_owned(), crate::ids::RowAuthor::value_type()),
                ("created_at".to_owned(), ValueType::U64),
                ("updated_by".to_owned(), crate::ids::RowAuthor::value_type()),
                ("updated_at".to_owned(), ValueType::U64),
                (
                    "_deletion".to_owned(),
                    ValueType::Nullable(Box::new(deletion_column().clone())),
                ),
            ]
            .into_iter()
            .chain(self.columns.iter().map(|column| {
                (
                    app_storage_column_name(&column.name),
                    ValueType::Nullable(Box::new(storage_column_type(column))),
                )
            })),
        )
    }
}

fn schema_versions_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_schema_versions",
        [
            // node-local-derived: allocated by schema-version alias interning.
            column("id", GrooveColumnType::U64),
            column("uuid", GrooveColumnType::Uuid),
            // node-local: mapping from a schema version's logical table & column names to stable
            // physical storage identities. Stored as serialized JSON bytes containing `SchemaPhysicalMapping`:
            // {
            //   tables: {
            //     "todos": {
            //       table_id: 7,
            //       columns: {
            //         "title": 12,
            //         "body": 19
            //       }
            //     }
            //   }
            // }
            column("physical_mapping", GrooveColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
}

fn catalogue_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_catalogue",
        [
            // The only hard-coded catalogue schema is a small, epoch-pinned
            // kernel. Its record kind is a permanent numeric discriminator;
            // all ordinary Jazz system and application descriptors live in
            // the catalogue itself.
            column("kind", GrooveColumnType::U64),
            column("id", GrooveColumnType::Uuid),
            column("payload", GrooveColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("kind", IntegerKeyType::U64),
        PrimaryKeyColumn::uuid("id"),
    ]))
}

fn catalogue_pointer_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_catalogue_pointer",
        [
            column("revision", GrooveColumnType::U64),
            column("schema", GrooveColumnType::Uuid),
        ],
    )
    .with_primary_key(PrimaryKey::new("revision", IntegerKeyType::U64).user_supplied())
}

fn global_changes_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_global_changes",
        [
            column("physical_table_id", GrooveColumnType::U64),
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("layer", GrooveColumnType::Bytes),
            column("global_time", GrooveColumnType::U64),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("_deletion", deletion_column().nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("physical_table_id", IntegerKeyType::U64),
        PrimaryKeyColumn::bytes("branch_key"),
        PrimaryKeyColumn::uuid("row_uuid"),
        PrimaryKeyColumn::bytes("layer"),
        PrimaryKeyColumn::integer("global_time", IntegerKeyType::U64),
    ]))
    .with_index(GrooveIndexSchema::new(
        "by_global_time",
        [
            "global_time",
            "physical_table_id",
            "branch_key",
            "row_uuid",
            "layer",
        ],
    ))
    .with_index(GrooveIndexSchema::new(
        "by_table_global_time",
        [
            "physical_table_id",
            "branch_key",
            "global_time",
            "row_uuid",
            "layer",
        ],
    ))
}

/// Immutable sparse deletion/restore history for every physical table lineage.
///
/// This is intentionally a fixed system table rather than a schema-variant
/// table: deletion payload has no user cells. Branch-key routing is added by
/// the physical branch-local row layer rather than transaction metadata.
pub(crate) fn shared_deletion_history_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_deletion_history",
        [
            column("branch_key", GrooveColumnType::Bytes),
            column("physical_table_id", GrooveColumnType::U64),
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", crate::ids::RowAuthor::value_type()),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", crate::ids::RowAuthor::value_type()),
            column("updated_at", GrooveColumnType::U64),
            column("_deletion", deletion_column()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("branch_key"),
        PrimaryKeyColumn::integer("physical_table_id", IntegerKeyType::U64),
        PrimaryKeyColumn::uuid("row_uuid"),
        PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
    ]))
    .with_index(GrooveIndexSchema::new(
        "by_tx",
        [
            "tx_time",
            "tx_node_id",
            "branch_key",
            "physical_table_id",
            "row_uuid",
        ],
    ))
}

fn merge_heads_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        MERGE_HEADS_TABLE,
        [
            column("physical_table_id", GrooveColumnType::U64),
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("heads", tx_id_column().array_of()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("physical_table_id", IntegerKeyType::U64),
        PrimaryKeyColumn::bytes("branch_key"),
        PrimaryKeyColumn::uuid("row_uuid"),
    ]))
}

fn pending_edges_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_pending_edges",
        [
            column("child_time", GrooveColumnType::U64),
            column("child_node_id", GrooveColumnType::U64),
            column("parent_time", GrooveColumnType::U64),
            column("parent_node_id", GrooveColumnType::U64),
            column("physical_table_id", GrooveColumnType::U64),
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("layer", GrooveColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("child_time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("child_node_id", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("parent_time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("parent_node_id", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("physical_table_id", IntegerKeyType::U64),
        PrimaryKeyColumn::bytes("branch_key"),
        PrimaryKeyColumn::uuid("row_uuid"),
        PrimaryKeyColumn::bytes("layer"),
    ]))
}

/// Policy-shape constructors.
#[cfg(test)]
pub(crate) struct Policy;

#[cfg(test)]
impl Policy {
    /// Owner-only policy equivalent to `column == claim("user")`.
    pub(crate) fn owner_only(table: impl Into<String>, column: impl Into<String>) -> Option<Query> {
        Some(Query::from(table).filter(eq(col(column), claim("user"))))
    }
}

fn storage_enum(name: &str, variants: &[&str]) -> GrooveColumnType {
    GrooveColumnType::EnumTag(
        ScalarEnumSchema::new(name, variants.iter().copied()).expect("valid enum schema"),
    )
}

fn tx_kind_column() -> GrooveColumnType {
    storage_enum("jazz_tx_kind", &["mergeable", "exclusive"])
}

fn fate_column() -> GrooveColumnType {
    storage_enum("jazz_fate", &["pending", "accepted", "rejected"])
}

fn deletion_column() -> GrooveColumnType {
    GrooveColumnType::EnumTag(
        ScalarEnumSchema::new("jazz_deletion", ["deleted", "restored"])
            .expect("valid deletion enum")
            .with_system_registry(SystemVariantRegistry::deletion_state()),
    )
}

fn rejection_reason_column() -> GrooveColumnType {
    storage_enum(
        "jazz_rejection_reason",
        &[
            "client_clock_too_far_ahead",
            "authorization_denied",
            "exclusive_conflict",
            "causality_violation",
            "cascade",
            "malformed_commit",
        ],
    )
}

fn durability_column() -> GrooveColumnType {
    storage_enum("jazz_durability", &["none", "local", "edge", "global"])
}

fn tx_id_column() -> GrooveColumnType {
    GrooveColumnType::Tuple(vec![GrooveColumnType::U64, GrooveColumnType::Uuid])
}

fn column(name: impl Into<String>, column_type: GrooveColumnType) -> groove::schema::ColumnSchema {
    groove::schema::ColumnSchema::new(name, column_type)
}

/// Frozen carrier namespace used by schema-owned stored/current/wire fields.
/// Application names are encoded by this constructor, never parsed as identity.
pub(crate) const APP_COLUMN_PREFIX: &str = "_app_";

pub(crate) fn app_storage_column_name(column: &str) -> String {
    format!("{APP_COLUMN_PREFIX}{column}")
}

pub(crate) fn global_current_index_name(column: &str) -> String {
    format!("by_app_{column}")
}

fn nodes_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_nodes",
        [
            // node-local-derived: allocated by node alias interning.
            column("id", GrooveColumnType::U64),
            column("uuid", GrooveColumnType::Uuid),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
}

fn transactions_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_transactions",
        [
            column("time", GrooveColumnType::U64),
            column("node_id", GrooveColumnType::U64),
            column("kind", tx_kind_column()),
            column("n_total_writes", GrooveColumnType::U32),
            column("made_by", crate::ids::RowAuthor::value_type()),
            column("base_snapshot", GrooveColumnType::Bytes.nullable()),
            column("row_read_set", GrooveColumnType::Bytes.nullable()),
            column("absent_read_set", GrooveColumnType::Bytes.nullable()),
            column("predicate_read_set", GrooveColumnType::Bytes.nullable()),
            column("user_metadata", GrooveColumnType::String.nullable()),
            column("contribution_merge", contribution_merge_column()),
            column(
                "permission_subject",
                crate::ids::AuthorSubject::value_type().nullable(),
            ),
            column("merge_strategy", GrooveColumnType::String.nullable()),
            // upstream-decided: written only by fate/state application.
            column("fate", fate_column()),
            // upstream-decided: written only by fate/state application.
            column("global_time", GrooveColumnType::U64.nullable()),
            // upstream-decided: written only by rejection/state application.
            column("rejection_reason", rejection_reason_column().nullable()),
            // upstream-decided: written only by rejection/state application.
            column("cascade_root", tx_id_column().nullable()),
            // upstream-decided: written only by rejection/state application.
            column("reason_detail", GrooveColumnType::String.nullable()),
            // node-local-derived: updated when the node learns stronger durability.
            column("durability", durability_column()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("node_id", IntegerKeyType::U64),
    ]))
    .with_index(GrooveIndexSchema::new("by_global_time", ["global_time"]))
}

fn contribution_component_column() -> GrooveColumnType {
    GrooveColumnType::Enum(Box::new(
        EnumSchema::new(
            "jazz_contribution_component",
            [
                EnumCase::new(
                    "column",
                    // The public/wire contribution coordinate names a logical
                    // column.  Its durable payload is instead this node's
                    // stable physical-column identity; names are resolved at
                    // the storage boundary through the retained catalogue.
                    RecordDescriptor::new([("physical_column_id", ValueType::U64)]),
                ),
                EnumCase::new(
                    "operation",
                    RecordDescriptor::new([
                        ("physical_column_id", ValueType::U64),
                        ("identity", ValueType::Bytes),
                    ]),
                ),
                EnumCase::new(
                    "register",
                    RecordDescriptor::new(std::iter::empty::<(String, ValueType)>()),
                ),
            ],
        )
        .expect("valid contribution component enum"),
    ))
}

fn contribution_coordinate_column() -> GrooveColumnType {
    GrooveColumnType::Record(Box::new(RecordDescriptor::new([
        ("branch_key", ValueType::Bytes),
        ("physical_table_id", ValueType::U64),
        ("row_uuid", ValueType::Uuid),
        (
            "layer",
            storage_enum("jazz_contribution_layer", &["content", "deletion"]),
        ),
        ("component", contribution_component_column()),
    ])))
}

fn contribution_dot_column() -> GrooveColumnType {
    GrooveColumnType::Record(Box::new(RecordDescriptor::new([
        ("tx_time", ValueType::U64),
        ("tx_node", ValueType::Uuid),
        ("coordinate", contribution_coordinate_column()),
    ])))
}

fn contribution_merge_column() -> GrooveColumnType {
    GrooveColumnType::Record(Box::new(RecordDescriptor::new([
        ("source", ValueType::Bytes),
        ("target", ValueType::Bytes),
        (
            "substitutions",
            GrooveColumnType::Record(Box::new(RecordDescriptor::new([
                ("target", contribution_coordinate_column()),
                ("sources", contribution_dot_column().array_of()),
            ])))
            .array_of(),
        ),
        // Versioned non-causal authorization evidence for a first head
        // overlay. This is a normal Groove record, not an opaque postcard
        // payload: every field remains inspectable by authority admission.
        (
            "branch_view_copy_v1",
            GrooveColumnType::Record(Box::new(RecordDescriptor::new([
                ("version", ValueType::U8),
                ("head", ValueType::Bytes),
                (
                    "base",
                    GrooveColumnType::Enum(Box::new(
                        EnumSchema::new(
                            "jazz_branch_view_copy_base_v1",
                            [
                                EnumCase::new(
                                    "current",
                                    RecordDescriptor::new([("branch", ValueType::Bytes)]),
                                ),
                                EnumCase::new(
                                    "snapshot",
                                    RecordDescriptor::new([
                                        ("branch", ValueType::Bytes),
                                        ("owner", ValueType::Uuid),
                                        ("global_base", ValueType::U64),
                                        ("local_base", ValueType::U64),
                                        (
                                            "dots",
                                            ValueType::Record(Box::new(RecordDescriptor::new([
                                                ("time", ValueType::U64),
                                                ("node", ValueType::Uuid),
                                            ])))
                                            .array_of(),
                                        ),
                                    ]),
                                ),
                            ],
                        )
                        .expect("valid branch-view copy base enum"),
                    )),
                ),
                ("table", ValueType::String),
                ("row_uuid", ValueType::Uuid),
                ("source_time", ValueType::U64),
                ("source_node", ValueType::Uuid),
            ])))
            .array_of(),
        ),
        // Every non-root mergeable branch write carries one canonical intent.
        // Its table identity is physical; the authored schema disambiguates
        // historical logical names when admission resolves it.
        (
            "branch_write_intent_v1",
            GrooveColumnType::Record(Box::new(RecordDescriptor::new([
                ("version", ValueType::U8),
                ("physical_table_id", ValueType::U64),
                ("authored_schema", ValueType::Uuid),
                ("row_uuid", ValueType::Uuid),
                ("head", ValueType::Bytes),
                (
                    "operation",
                    GrooveColumnType::Enum(
                        Box::new(
                            EnumSchema::new(
                                "jazz_branch_write_operation_v1",
                                [
                                    EnumCase::new(
                                        "exact_head_insert",
                                        RecordDescriptor::new(std::iter::empty::<(
                                            String,
                                            ValueType,
                                        )>(
                                        )),
                                    ),
                                    EnumCase::new(
                                        "exact_head_update",
                                        RecordDescriptor::new(std::iter::empty::<(
                                            String,
                                            ValueType,
                                        )>(
                                        )),
                                    ),
                                    EnumCase::new(
                                        "view_update_copy",
                                        RecordDescriptor::new([("evidence_index", ValueType::U32)]),
                                    ),
                                ],
                            )
                            .expect("valid branch write operation enum"),
                        ),
                    ),
                ),
            ])))
            .array_of(),
        ),
    ])))
    .nullable()
}

/// Bound physical type used by the transaction codec. Constructing the
/// single-column table applies the same durable registry paths as the real
/// `jazz_transactions.contribution_merge` column.
pub(crate) fn contribution_merge_storage_type() -> GrooveColumnType {
    GrooveTableSchema::new(
        "jazz_transactions",
        [column("contribution_merge", contribution_merge_column())],
    )
    .columns
    .into_iter()
    .next()
    .expect("contribution merge storage column")
    .column_type
}

fn canonical_schema_bytes(schema: &RuntimeSchema) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, "jazz-schema-v1-large-value-kinds");
    let mut tables = schema.tables.iter().collect::<Vec<_>>();
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    put_u64(&mut bytes, tables.len() as u64);
    for table in tables {
        put_str(&mut bytes, &table.name);
        put_u64(&mut bytes, table.columns.len() as u64);
        for column in &table.columns {
            put_str(&mut bytes, &column.name);
            put_column_type(&mut bytes, &column.column_type);
            bytes.push(match column.large_value_kind {
                LargeValueSemanticKind::NotLarge => 0,
                LargeValueSemanticKind::Bytes => 1,
                LargeValueSemanticKind::String => 2,
                LargeValueSemanticKind::Json => 3,
            });
            put_merge_strategy(&mut bytes, table.merge_strategy(&column.name));
        }
        put_u64(&mut bytes, table.references.len() as u64);
        for (column, target) in &table.references {
            put_str(&mut bytes, column);
            put_str(&mut bytes, target);
        }
        let mut branch_by = table.branch_by.iter().collect::<Vec<_>>();
        branch_by.sort();
        put_u64(&mut bytes, branch_by.len() as u64);
        for column in branch_by {
            put_str(&mut bytes, column);
        }
    }
    bytes
}

fn put_merge_strategy(bytes: &mut Vec<u8>, strategy: MergeStrategy) {
    bytes.push(match strategy {
        MergeStrategy::Lww => 0,
        MergeStrategy::Counter => 1,
        MergeStrategy::GSet => 2,
    });
}

fn put_column_type(bytes: &mut Vec<u8>, column_type: &GrooveColumnType) {
    match column_type {
        GrooveColumnType::U8 => bytes.push(0),
        GrooveColumnType::U16 => bytes.push(1),
        GrooveColumnType::U32 => bytes.push(2),
        GrooveColumnType::U64 => bytes.push(3),
        GrooveColumnType::I32 => bytes.push(4),
        GrooveColumnType::I64 => bytes.push(5),
        GrooveColumnType::F64 => bytes.push(6),
        GrooveColumnType::Bool => bytes.push(7),
        GrooveColumnType::String => bytes.push(8),
        GrooveColumnType::Bytes => bytes.push(9),
        GrooveColumnType::Uuid => bytes.push(10),
        GrooveColumnType::EnumTag(schema) => {
            bytes.push(11);
            put_str(bytes, &schema.name);
            put_u64(bytes, schema.variants.len() as u64);
            for variant in &schema.variants {
                put_str(bytes, variant);
            }
        }
        GrooveColumnType::Tuple(members) => {
            bytes.push(12);
            put_u64(bytes, members.len() as u64);
            for member in members {
                put_column_type(bytes, member);
            }
        }
        GrooveColumnType::Array(member) => {
            bytes.push(13);
            put_column_type(bytes, member);
        }
        GrooveColumnType::Nullable(member) => {
            bytes.push(14);
            put_column_type(bytes, member);
        }
        GrooveColumnType::Record(descriptor) => {
            bytes.push(15);
            put_u64(bytes, descriptor.fields().len() as u64);
            for field in descriptor.fields() {
                match &field.name {
                    Some(name) => {
                        bytes.push(1);
                        put_str(bytes, name);
                    }
                    None => bytes.push(0),
                }
                put_column_type(bytes, &field.value_type);
            }
        }
        GrooveColumnType::Enum(schema) => {
            bytes.push(16);
            put_str(bytes, &schema.name);
            put_u64(bytes, schema.cases.len() as u64);
            for case in &schema.cases {
                put_str(bytes, &case.name);
                put_u64(bytes, case.payload.fields().len() as u64);
                for field in case.payload.fields() {
                    match &field.name {
                        Some(name) => {
                            bytes.push(1);
                            put_str(bytes, name);
                        }
                        None => bytes.push(0),
                    }
                    put_column_type(bytes, &field.value_type);
                }
            }
        }
        _ => {
            panic!("raw stored-scalar backing types cannot appear in a Jazz schema")
        }
    }
}

fn put_str(bytes: &mut Vec<u8>, value: &str) {
    put_bytes(bytes, value.as_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_u64(bytes, value.len() as u64);
    bytes.extend_from_slice(value);
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn rejected_transactions_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_rejected_transactions",
        [
            column("time", GrooveColumnType::U64),
            column("node_id", GrooveColumnType::U64),
            column("kind", tx_kind_column()),
            column("made_by", crate::ids::RowAuthor::value_type()),
            column("rejection_reason", rejection_reason_column()),
            column("cascade_root", tx_id_column().nullable()),
            column("reason_detail", GrooveColumnType::String.nullable()),
            column("user_metadata", GrooveColumnType::String.nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("node_id", IntegerKeyType::U64),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::schema::ColumnType;

    #[test]
    fn string_branch_columns_are_accepted() {
        let _ = JazzSchema::new_with_branch_columns([TableSchema::new(
            "todos",
            [ColumnSchema::new("branch", ColumnType::String)],
        )
        .with_branch_column("branch")]);
    }

    #[test]
    fn branch_selector_projection_uses_the_declared_enum_codec() {
        let phase = ScalarEnumSchema::new("phase", ["draft", "ready"]).unwrap();
        let schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new(
                "phase",
                ColumnType::EnumTag(phase.clone()),
            )],
        )
        .with_branch_column("phase")]);
        let table = &schema.tables[0];

        let (key, cells) = schema
            .project_branch_selector(
                table,
                &BranchSelector::new([("phase", Value::String("ready".to_owned()))]),
            )
            .unwrap();

        assert_eq!(key.values[0].1.0, [1, 8, 1]);
        assert_eq!(cells["phase"], Value::String("ready".to_owned()));
        assert_eq!(
            schema.validate_authored_branch_key(table, &key).unwrap()["phase"],
            Value::EnumTag(1)
        );
    }

    #[test]
    fn added_branch_column_reads_older_keys_at_its_default() {
        let schema = JazzSchema::new_with_branch_columns([TableSchema::new(
            "todos",
            [ColumnSchema::new("workspace_id", ColumnType::Uuid)
                .with_default(Value::Uuid(uuid::Uuid::nil()))],
        )
        .with_branch_column("workspace_id")]);
        let table = &schema.tables[0];
        let default = schema
            .project_branch_selector(
                table,
                &BranchSelector::new([("workspace_id", Value::Uuid(uuid::Uuid::nil()))]),
            )
            .unwrap()
            .0;
        let other = schema
            .project_branch_selector(
                table,
                &BranchSelector::new([(
                    "workspace_id",
                    Value::Uuid(uuid::Uuid::from_bytes([0x32; 16])),
                )]),
            )
            .unwrap()
            .0;

        assert!(schema.branch_key_matches(table, &BranchKey::default(), &default));
        assert!(!schema.branch_key_matches(table, &BranchKey::default(), &other));
    }

    #[test]
    fn logical_history_descriptor_has_composite_primary_key() {
        let schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);

        let groove = schema.lower_to_groove();
        assert!(groove.table("jazz_nodes").is_some());
        assert!(groove.table("jazz_schema_versions").is_some());
        assert!(groove.table("jazz_transactions").is_some());
        assert!(groove.table("jazz_todos_history").is_none());
        let table = schema.tables[0].history_storage_table();
        let primary_key = table.primary_key.as_ref().unwrap();

        assert_eq!(primary_key.columns.len(), 4);
        assert_eq!(primary_key.columns[0].column, "branch_key");
        assert_eq!(primary_key.columns[1].column, "row_uuid");
        assert_eq!(primary_key.columns[2].column, "tx_time");
        assert_eq!(primary_key.columns[3].column, "tx_node_id");
        assert!(
            table
                .columns
                .iter()
                .any(|column| column.name == "_app_title")
        );
        assert!(
            table
                .columns
                .iter()
                .any(|column| column.name == "schema_version")
        );
    }

    #[test]
    fn schema_version_id_is_stable_and_content_addressed() {
        let schema_a = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let schema_b = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let schema_c = RuntimeSchema::new([TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("done", ColumnType::Bool),
            ],
        )]);

        assert_eq!(schema_a.version_id(), schema_b.version_id());
        assert_eq!(schema_a.canonical_bytes(), schema_b.canonical_bytes());
        assert_ne!(schema_a.version_id(), schema_c.version_id());
    }

    #[test]
    fn counter_merge_strategy_changes_schema_identity() {
        let lww = RuntimeSchema::new([TableSchema::new(
            "counters",
            [ColumnSchema::new("count", ColumnType::U64)],
        )]);
        let counter = RuntimeSchema::new([TableSchema::new(
            "counters",
            [ColumnSchema::new("count", ColumnType::U64)],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter)]);

        assert_ne!(lww.version_id(), counter.version_id());
    }

    #[test]
    #[should_panic(expected = "counter merge strategy requires a non-nullable integer column")]
    fn counter_merge_strategy_rejects_string_columns() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )
        .with_column_merge_strategy("title", MergeStrategy::Counter)]);
    }

    #[test]
    #[should_panic(expected = "merge strategy declared for unknown column todos.missing")]
    fn merge_strategy_rejects_unknown_user_column() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )
        .with_column_merge_strategy("missing", MergeStrategy::Lww)]);
    }

    #[test]
    #[should_panic(expected = "counter merge strategy requires a non-nullable integer column")]
    fn counter_merge_strategy_rejects_nullable_integer_columns() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("count", ColumnType::U64.nullable())],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter)]);
    }

    #[test]
    #[should_panic(expected = "read policy table must match")]
    fn read_policy_must_name_attached_table() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_read_policy(Policy::owner_only("other", "owner"))]);
    }

    #[test]
    #[should_panic(expected = "write policy table must match")]
    fn write_policy_must_name_attached_table() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_write_policy(Policy::owner_only("other", "owner"))]);
    }

    #[test]
    #[should_panic(expected = "valid read policy shape")]
    fn read_policy_validates_against_complete_schema() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_read_policy(Policy::owner_only("todos", "missing"))]);
    }

    #[test]
    fn global_changes_table_key_and_index_match_sync_contract() {
        let table = global_changes_table();
        let primary_key = table.primary_key.as_ref().unwrap();
        assert_eq!(table.name, "jazz_global_changes");
        assert_eq!(
            primary_key
                .columns
                .iter()
                .map(|column| column.column.as_str())
                .collect::<Vec<_>>(),
            vec![
                "physical_table_id",
                "branch_key",
                "row_uuid",
                "layer",
                "global_time"
            ]
        );

        let index = table
            .indices
            .iter()
            .find(|index| index.name == "by_global_time")
            .unwrap();
        assert_eq!(
            index.columns,
            vec![
                "global_time",
                "physical_table_id",
                "branch_key",
                "row_uuid",
                "layer"
            ]
        );
        let table_index = table
            .indices
            .iter()
            .find(|index| index.name == "by_table_global_time")
            .unwrap();
        assert_eq!(
            table_index.columns,
            vec![
                "physical_table_id",
                "branch_key",
                "global_time",
                "row_uuid",
                "layer"
            ]
        );
    }

    // This is intentionally an internal schema test: the physical encoding of
    // a derived index is not exposed by the public API. The declared type is
    // the durable contract that keeps Rust/serde layout out of stored rows.
    #[test]
    fn merge_heads_use_the_native_transaction_id_array_type() {
        let table = merge_heads_table();
        assert_eq!(
            table
                .columns
                .iter()
                .find(|column| column.name == "heads")
                .map(|column| &column.column_type),
            Some(&tx_id_column().array_of())
        );
    }

    // This is intentionally an internal schema test: physical-key boundedness
    // is not observable through the public API until deletion ingestion routes
    // through the shared table. It guards the storage contract that makes the
    // later black-box collision and branch-isolation tests meaningful.
    #[test]
    fn shared_deletion_history_is_prefix_bounded_by_lineage_table_and_row() {
        let table = shared_deletion_history_table();
        assert_eq!(table.name, "jazz_deletion_history");
        assert_eq!(
            table
                .primary_key
                .as_ref()
                .expect("shared deletion history has a primary key")
                .columns
                .iter()
                .map(|column| column.column.as_str())
                .collect::<Vec<_>>(),
            vec![
                "branch_key",
                "physical_table_id",
                "row_uuid",
                "tx_time",
                "tx_node_id",
            ]
        );
        assert_eq!(
            table
                .indices
                .iter()
                .find(|index| index.name == "by_tx")
                .expect("shared deletion history has tx lookup")
                .columns,
            vec![
                "tx_time",
                "tx_node_id",
                "branch_key",
                "physical_table_id",
                "row_uuid",
            ]
        );
    }

    #[test]
    fn storage_lowering_declares_system_columns_by_shape() {
        let schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let tables = schema.storage_tables();
        let transactions = tables
            .iter()
            .find(|table| table.name == "jazz_transactions")
            .unwrap();
        assert!(
            tables
                .iter()
                .all(|table| table.name != "jazz_todos_history"
                    && table.name != "jazz_todos_register")
        );
        assert!(
            tables
                .iter()
                .any(|table| table.name == "jazz_deletion_history")
        );
        let history = schema.tables[0].history_storage_table();
        let register = schema.tables[0].register_storage_table();
        assert!(tables.iter().all(|table| {
            table.name != "jazz_todos_global_current"
                && table.name != "jazz_todos_register_global_current"
                && table.name != "jazz_todos_ahead_current"
                && table.name != "jazz_todos_register_ahead_current"
        }));
        let current_tables = schema.tables[0].global_current_storage_tables();
        let global_current = &current_tables[0];
        let register_global_current = &current_tables[1];
        let ahead_tables = schema.tables[0].ahead_current_storage_tables();
        let ahead_current = &ahead_tables[0];

        for table in [&history, &register, global_current, register_global_current] {
            for name in ["created_by", "updated_by"] {
                assert_eq!(
                    table
                        .columns
                        .iter()
                        .find(|column| column.name == name)
                        .map(|column| &column.column_type),
                    Some(&crate::ids::RowAuthor::value_type()),
                    "{name} must use the structured author record in {}",
                    table.name
                );
            }
        }

        for table in [&history, global_current, ahead_current] {
            assert_eq!(
                table
                    .columns
                    .iter()
                    .find(|column| column.name == "authored_columns")
                    .map(|column| &column.column_type),
                Some(&GrooveColumnType::U64.array_of().nullable()),
                "authored_columns must remain a native physical-id array in {}",
                table.name
            );
        }

        assert!(
            transactions
                .columns
                .iter()
                .any(|column| column.name == "fate")
        );
        assert!(
            transactions
                .columns
                .iter()
                .any(|column| column.name == "durability")
        );
        assert!(
            history
                .columns
                .iter()
                .any(|column| column.name == "parents")
        );
        assert!(
            register
                .columns
                .iter()
                .any(|column| column.name == "_deletion")
        );
        assert_eq!(
            global_current
                .primary_key
                .as_ref()
                .unwrap()
                .columns
                .iter()
                .map(|column| column.column.as_str())
                .collect::<Vec<_>>(),
            vec!["branch_key", "row_uuid"]
        );
        assert_eq!(
            register_global_current
                .primary_key
                .as_ref()
                .unwrap()
                .columns
                .iter()
                .map(|column| column.column.as_str())
                .collect::<Vec<_>>(),
            vec!["branch_key", "row_uuid"]
        );
    }

    #[test]
    fn contribution_merge_uses_native_nested_records_and_groove_enums() {
        // Internal storage-shape coverage is necessary here because the public
        // transaction API intentionally hides Groove's durable descriptor.
        let transactions = transactions_table();
        let column = transactions
            .columns
            .iter()
            .find(|column| column.name == "contribution_merge")
            .unwrap();
        let GrooveColumnType::Nullable(provenance) = &column.column_type else {
            panic!("contribution provenance must be nullable");
        };
        let GrooveColumnType::Record(provenance) = provenance.as_ref() else {
            panic!("contribution provenance must be a native record");
        };
        assert_eq!(
            provenance
                .fields()
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            [
                "source",
                "target",
                "substitutions",
                "branch_view_copy_v1",
                "branch_write_intent_v1",
            ]
        );

        let GrooveColumnType::Array(substitution) = &provenance.fields()[2].value_type else {
            panic!("contribution substitutions must be an array");
        };
        let GrooveColumnType::Record(substitution) = substitution.as_ref() else {
            panic!("contribution substitutions must contain records");
        };
        let GrooveColumnType::Record(coordinate) = &substitution.fields()[0].value_type else {
            panic!("contribution target must be a coordinate record");
        };
        let GrooveColumnType::EnumTag(layer) = &coordinate.fields()[3].value_type else {
            panic!("contribution layer must be a Groove enum");
        };
        assert_eq!(layer.variants, ["content", "deletion"]);
        let GrooveColumnType::Enum(component) = &coordinate.fields()[4].value_type else {
            panic!("contribution component must be a Groove payload enum");
        };
        assert_eq!(
            component
                .cases
                .iter()
                .map(|case| case.name.as_str())
                .collect::<Vec<_>>(),
            ["column", "operation", "register"]
        );
        assert_eq!(component.tag("column").unwrap(), 0);
        assert_eq!(component.tag("operation").unwrap(), 1);
        assert_eq!(component.tag("register").unwrap(), 2);
        assert_eq!(
            component.cases[1]
                .payload
                .fields()
                .iter()
                .map(|field| field.name.as_deref().unwrap())
                .collect::<Vec<_>>(),
            ["physical_column_id", "identity"]
        );
    }

    #[test]
    fn system_deletion_registry_survives_storage_rebinding_but_user_enums_do_not() {
        let state = ScalarEnumSchema::new("state", ["open", "done"]).unwrap();
        let left = TableSchema::new(
            "left",
            [ColumnSchema::new(
                "state",
                ColumnType::EnumTag(state.clone()),
            )],
        );
        let right = TableSchema::new(
            "right",
            [ColumnSchema::new("state", ColumnType::EnumTag(state))],
        );
        let registry = |table: GrooveTableSchema, name: &str| match &table
            .columns
            .iter()
            .find(|column| column.name == name)
            .unwrap()
            .column_type
        {
            GrooveColumnType::EnumTag(schema) => schema.registry_id(),
            GrooveColumnType::Nullable(inner) => match inner.as_ref() {
                GrooveColumnType::EnumTag(schema) => schema.registry_id(),
                other => panic!("expected enum field, got {other:?}"),
            },
            other => panic!("expected enum field, got {other:?}"),
        };

        assert_eq!(
            registry(left.register_storage_table(), "_deletion"),
            registry(right.register_storage_table(), "_deletion")
        );
        assert_ne!(
            registry(left.history_storage_table(), "_app_state"),
            registry(right.history_storage_table(), "_app_state"),
            "structurally identical user enums must retain independent registries"
        );
    }

    #[test]
    fn deserializing_a_schema_rejects_internal_storage_types_and_kind_spoofing() {
        let internal = groove::large_values::physical_storage_value_type(
            groove::large_values::LargeValueKind::Json,
        );
        let internal_column = serde_json::json!({
            "name": "body",
            "column_type": internal,
            "large_value_kind": "Json",
            "default": null,
        });
        assert!(
            serde_json::from_value::<ColumnSchema>(internal_column).is_err(),
            "serde must not bypass the public schema constructor with an internal type"
        );

        let spoofed_kind = serde_json::json!({
            "name": "body",
            "column_type": GrooveColumnType::Bytes,
            "large_value_kind": "Json",
            "default": null,
        });
        assert!(
            serde_json::from_value::<ColumnSchema>(spoofed_kind).is_err(),
            "semantic kind must be structurally compatible with its public column type"
        );
    }

    #[test]
    fn persisted_branch_keys_require_the_owning_table_type_and_enum_domain() {
        let uuid_schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("branch", ColumnType::Uuid)],
        )
        .with_branch_column("branch")]);
        let uuid_table = &uuid_schema.tables[0];
        let (valid, _) = uuid_schema
            .project_branch_selector(
                uuid_table,
                &BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([0x42; 16])))]),
            )
            .unwrap();
        assert_eq!(
            RuntimeSchema::decode_persisted_branch_key(uuid_table, &valid.canonical_bytes())
                .unwrap(),
            valid
        );

        let wrong_scalar = BranchKey {
            values: vec![(
                "branch".to_owned(),
                BranchColumnValue::from(Value::String("not-a-uuid".to_owned())),
            )],
        };
        assert!(
            RuntimeSchema::decode_persisted_branch_key(uuid_table, &wrong_scalar.canonical_bytes())
                .is_err()
        );

        let phase = ScalarEnumSchema::new("phase", ["draft", "ready"]).unwrap();
        let enum_schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("phase", ColumnType::EnumTag(phase))],
        )
        .with_branch_column("phase")]);
        let invalid_enum = BranchKey {
            values: vec![("phase".to_owned(), BranchColumnValue(vec![1, 8, u8::MAX]))],
        };
        assert!(
            RuntimeSchema::decode_persisted_branch_key(
                &enum_schema.tables[0],
                &invalid_enum.canonical_bytes()
            )
            .is_err()
        );
    }
}
