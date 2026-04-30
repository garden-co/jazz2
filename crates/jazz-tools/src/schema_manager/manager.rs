//! SchemaManager - Coordinates schema evolution with query execution.
//!
//! This provides the top-level API for schema-aware queries, combining:
//! - SchemaContext for tracking current/live schema versions
//! - Lens management for migrations
//! - Schema-aware branch naming
//! - Integrated QueryManager for query/insert/update/delete operations
//! - Catalogue persistence for schema/lens discovery via sync

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};

use blake3::Hasher;

use crate::catalogue::CatalogueEntry;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::manager::{DeleteHandle, InsertResult, QueryError, QueryManager};
use crate::query_manager::query::{Query, QueryBuilder};
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{
    ComposedBranchName, PermissionPreflightDecision, RowDescriptor, RowPolicyMode, Schema,
    SchemaHash, TableName, TablePolicies, Value,
};
use crate::query_manager::writes::{RowBranchDelete, RowBranchWrite, SchemaUpdateRowLoad};
use crate::row_format::decode_row;
use crate::schema_manager::rehydrate::latest_catalogue_content;
use crate::storage::Storage;
use crate::sync_manager::{ConnectionSchemaDiagnostics, SyncManager};
use uuid::Uuid;

use super::auto_lens::generate_lens;
use super::context::{QuerySchemaContext, SchemaContext, SchemaError};
use super::encoding::{
    decode_lens_transform, decode_permissions, decode_permissions_bundle, decode_permissions_head,
    decode_schema, encode_lens_transform, encode_permissions, encode_permissions_bundle,
    encode_permissions_head, encode_schema,
};
use super::lens::Lens;
use super::types::AppId;

#[derive(Clone, Debug, PartialEq)]
struct PermissionsBundleState {
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    permissions: HashMap<TableName, TablePolicies>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct PermissionsHeadState {
    schema_hash: SchemaHash,
    version: u64,
    parent_bundle_object_id: Option<ObjectId>,
    bundle_object_id: ObjectId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PermissionsHeadSummary {
    pub schema_hash: SchemaHash,
    pub version: u64,
    pub parent_bundle_object_id: Option<ObjectId>,
    pub bundle_object_id: ObjectId,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CurrentPermissionsSummary {
    pub head: PermissionsHeadSummary,
    pub permissions: HashMap<TableName, TablePolicies>,
}

/// SchemaManager coordinates schema evolution with query execution.
///
/// It manages:
/// - Current schema and environment
/// - Live schema versions reachable via lenses
/// - Lens registration and auto-generation
/// - Schema-aware branch naming
/// - Query execution with automatic lens transforms
/// - Catalogue persistence for schema/lens discovery via sync
///
/// # Example
///
/// ```ignore
/// let app_id = AppId::from_name("my-app");
/// let mut manager = SchemaManager::new(
///     SyncManager::new(),
///     schema,
///     app_id,
///     "dev",
///     "main",
/// )?;
///
/// // Add a previous schema version as "live"
/// manager.add_live_schema(old_schema)?;
///
/// // Persist schema and lens to catalogue for other clients
/// manager.persist_schema();
/// manager.persist_lens(&lens);
///
/// // Insert data
/// let handle = manager.insert(
///     "users",
///     std::collections::HashMap::from([
///         ("id".to_string(), id),
///         ("name".to_string(), name),
///     ]),
/// )?;
///
/// // Query across all schema versions via subscription
/// let sub_id = manager.query_manager_mut().subscribe(manager.query("users").build())?;
/// manager.process();
/// let results = manager.query_manager_mut().get_subscription_results(sub_id);
/// manager.query_manager_mut().unsubscribe_with_sync(sub_id);
/// ```
pub struct SchemaManager {
    context: SchemaContext,
    query_manager: QueryManager,
    app_id: AppId,
    catalogue_publish_timestamps: HashMap<ObjectId, u64>,
    current_permissions_head: Option<PermissionsHeadState>,
    known_permissions_bundles: HashMap<ObjectId, PermissionsBundleState>,
    pending_permissions_head: Option<PermissionsHeadState>,
    /// Schemas known to this manager (for server mode).
    /// Server adds schemas here when received via catalogue sync.
    /// These are stored without requiring a lens path to current.
    known_schemas: Arc<HashMap<SchemaHash, Schema>>,
    known_schemas_dirty: bool,
    persisted_current_schema_in_storage: HashSet<(usize, SchemaHash)>,
}

impl SchemaManager {
    /// Create a new SchemaManager with integrated QueryManager.
    ///
    /// # Arguments
    ///
    /// * `sync_manager` - SyncManager for object persistence
    /// * `schema` - Current schema for this client
    /// * `app_id` - Application identifier for catalogue queries
    /// * `env` - Environment (e.g., "dev", "prod")
    /// * `user_branch` - User-facing branch name (e.g., "main")
    pub fn new(
        sync_manager: SyncManager,
        schema: Schema,
        app_id: AppId,
        env: &str,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        let row_policy_mode = if QueryManager::schema_has_any_explicit_policies(&schema) {
            RowPolicyMode::Enforcing
        } else {
            RowPolicyMode::PermissiveLocal
        };
        Self::new_with_policy_mode(
            sync_manager,
            schema,
            app_id,
            env,
            user_branch,
            row_policy_mode,
        )
    }

    pub fn new_with_policy_mode(
        sync_manager: SyncManager,
        schema: Schema,
        app_id: AppId,
        env: &str,
        user_branch: &str,
        row_policy_mode: RowPolicyMode,
    ) -> Result<Self, SchemaError> {
        let structural_schema = strip_schema_policies(&schema);

        let context = SchemaContext::new(schema.clone(), env, user_branch);
        let current_hash = SchemaHash::compute(&schema);

        // Create QueryManager with empty context, then set current schema
        let mut query_manager = QueryManager::new(sync_manager);
        query_manager.set_catalogue_app_id(app_id.uuid().to_string());
        query_manager.set_current_schema_with_policy_mode(
            schema.clone(),
            env,
            user_branch,
            row_policy_mode,
        );

        // Initialize known_schemas with current schema
        let mut known_schemas = HashMap::new();
        known_schemas.insert(current_hash, structural_schema);

        Ok(Self {
            context,
            query_manager,
            app_id,
            catalogue_publish_timestamps: HashMap::new(),
            current_permissions_head: None,
            known_permissions_bundles: HashMap::new(),
            pending_permissions_head: None,
            known_schemas: Arc::new(known_schemas),
            known_schemas_dirty: true,
            persisted_current_schema_in_storage: HashSet::new(),
        })
    }

    /// Create with default environment ("dev").
    pub fn with_defaults(
        sync_manager: SyncManager,
        schema: Schema,
        app_id: AppId,
        user_branch: &str,
    ) -> Result<Self, SchemaError> {
        Self::new(sync_manager, schema, app_id, "dev", user_branch)
    }

    /// Create a server-mode SchemaManager with no fixed current schema.
    ///
    /// Servers don't have a "current" schema - they serve multiple clients
    /// with different schema versions. Schemas are added via `add_known_schema()`
    /// when received from clients via catalogue sync.
    ///
    /// Queries are executed with explicit `QuerySchemaContext` rather than
    /// using implicit current schema context.
    pub fn new_server(sync_manager: SyncManager, app_id: AppId, _env: &str) -> Self {
        let mut query_manager = QueryManager::new(sync_manager);
        query_manager.set_catalogue_app_id(app_id.uuid().to_string());
        Self {
            context: SchemaContext::empty(),
            query_manager,
            app_id,
            catalogue_publish_timestamps: HashMap::new(),
            current_permissions_head: None,
            known_permissions_bundles: HashMap::new(),
            pending_permissions_head: None,
            known_schemas: Arc::new(HashMap::new()),
            known_schemas_dirty: false,
            persisted_current_schema_in_storage: HashSet::new(),
        }
    }

    /// Check if this manager has a current schema set.
    ///
    /// Returns false for server-mode managers created with `new_server()`.
    pub fn has_current_schema(&self) -> bool {
        self.context.is_initialized()
    }

    /// Add a schema to known_schemas without requiring a lens path to current.
    ///
    /// Used by servers when receiving client schemas via catalogue sync.
    /// The schema becomes available for use in explicit-context queries.
    ///
    /// Also creates indices for all env/user_branch combinations if known.
    pub fn add_known_schema(&mut self, schema: Schema) {
        let schema = strip_schema_policies(&schema);
        let hash = SchemaHash::compute(&schema);

        // Skip if already known
        if self.known_schemas.contains_key(&hash) {
            return;
        }

        Arc::make_mut(&mut self.known_schemas).insert(hash, schema.clone());
        self.known_schemas_dirty = true;

        // If we have a current schema context, also try the lens-path activation
        if self.context.is_initialized() {
            self.context.add_pending_schema(schema.clone());
            self.activate_pending_and_sync_to_query_manager();
        }

        self.try_apply_pending_permissions_head();
    }

    /// Get a known schema by hash.
    pub fn get_known_schema(&self, hash: &SchemaHash) -> Option<&Schema> {
        self.known_schemas.get(hash)
    }

    /// Check if a schema is known (either current, live, or in known_schemas).
    pub fn is_schema_known(&self, hash: &SchemaHash) -> bool {
        self.context.is_live(hash) || self.known_schemas.contains_key(hash)
    }

    /// Get the application ID.
    pub fn app_id(&self) -> AppId {
        self.app_id
    }

    /// Get the current schema.
    pub fn current_schema(&self) -> &Schema {
        &self.context.current_schema
    }

    /// Get the current schema hash.
    pub fn current_hash(&self) -> SchemaHash {
        self.context.current_hash
    }

    /// Get the composed branch name for the current schema.
    pub fn branch_name(&self) -> BranchName {
        self.context.branch_name()
    }

    /// Get branch names for all live schemas (current + live).
    pub fn all_branches(&self) -> Vec<BranchName> {
        self.context.all_branch_names()
    }

    /// Get the environment.
    pub fn env(&self) -> &str {
        &self.context.env
    }

    /// Get the user branch.
    pub fn user_branch(&self) -> &str {
        &self.context.user_branch
    }

    fn schema_for_hash(&self, schema_hash: SchemaHash) -> Option<&Schema> {
        if schema_hash == self.context.current_hash {
            return Some(&self.context.current_schema);
        }
        self.context.live_schemas.get(&schema_hash)
    }

    fn resolve_target_branch(
        &self,
        write_context: Option<&WriteContext>,
    ) -> Result<(String, SchemaHash), QueryError> {
        let current_branch = self.context.branch_name().as_str().to_string();
        let current_hash = self.context.current_hash;
        let Some(target_branch_name) = write_context.and_then(WriteContext::target_branch_name)
        else {
            return Ok((current_branch, current_hash));
        };

        let parsed =
            ComposedBranchName::parse(&BranchName::new(target_branch_name)).ok_or_else(|| {
                QueryError::EncodingError(format!(
                    "invalid target_branch_name `{target_branch_name}`"
                ))
            })?;

        if !parsed.matches_env_and_branch(&self.context.env, &self.context.user_branch) {
            return Err(QueryError::EncodingError(format!(
                "target_branch_name `{target_branch_name}` is outside the current schema family {}/*/{}",
                self.context.env, self.context.user_branch
            )));
        }

        if parsed.schema_hash.short() == current_hash.short() {
            return Ok((current_branch, current_hash));
        }

        if let Some(hash) = self
            .context
            .live_schemas
            .keys()
            .copied()
            .find(|hash| hash.short() == parsed.schema_hash.short())
        {
            let canonical =
                ComposedBranchName::new(&self.context.env, hash, &self.context.user_branch)
                    .to_branch_name();
            return Ok((canonical.as_str().to_string(), hash));
        }

        Err(QueryError::UnknownSchema(parsed.schema_hash))
    }

    fn schema_context_for_hash(
        &self,
        schema_hash: SchemaHash,
    ) -> Result<SchemaContext, QueryError> {
        let target_schema = self
            .schema_for_hash(schema_hash)
            .ok_or(QueryError::UnknownSchema(schema_hash))?
            .clone();
        let mut temp_context = SchemaContext::new(
            target_schema.clone(),
            &self.context.env,
            &self.context.user_branch,
        );

        for lens in self.context.lenses.values() {
            temp_context.register_lens(lens.clone());
        }

        if self.context.current_hash != schema_hash {
            temp_context.add_pending_schema(self.context.current_schema.clone());
        }

        for (hash, schema) in &self.context.live_schemas {
            if *hash != schema_hash {
                temp_context.add_pending_schema(schema.clone());
            }
        }

        temp_context.try_activate_pending();
        Ok(temp_context)
    }

    fn get_insert_values_with_defaults_for_schema(
        table: &str,
        schema: &Schema,
        mut values_by_column: HashMap<String, Value>,
    ) -> Result<Vec<Value>, QueryError> {
        let table_name = TableName::new(table);
        let table_schema = schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?;

        for column in values_by_column.keys() {
            if table_schema.columns.column(column.as_str()).is_none() {
                return Err(QueryError::EncodingError(format!(
                    "unknown column `{column}` on table `{table}`"
                )));
            }
        }

        let mut aligned_values = Vec::with_capacity(table_schema.columns.columns.len());
        for column in &table_schema.columns.columns {
            if let Some(value) = values_by_column.remove(column.name.as_str()) {
                if value == Value::Null && !column.nullable {
                    return Err(QueryError::EncodingError(format!(
                        "cannot set required field `{}` to null",
                        column.name
                    )));
                }
                aligned_values.push(value);
                continue;
            }

            if let Some(default) = &column.default {
                aligned_values.push(default.clone());
            } else if column.nullable {
                aligned_values.push(Value::Null);
            } else {
                return Err(QueryError::EncodingError(format!(
                    "missing required field `{}` on table `{table}`",
                    column.name
                )));
            }
        }

        Ok(aligned_values)
    }

    /// Add a live schema version with auto-generated lens.
    ///
    /// The lens is automatically generated from the schema diff.
    /// Returns error if the generated lens is a draft (needs manual review).
    ///
    /// Automatically updates QueryManager indices and marks subscriptions for recompile.
    pub fn add_live_schema(&mut self, old_schema: Schema) -> Result<&Lens, SchemaError> {
        let old_schema = strip_schema_policies(&old_schema);
        let lens = generate_lens(&old_schema, &self.context.current_schema);

        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }

        let source_hash = lens.source_hash;

        // Update context
        self.context
            .add_live_schema(old_schema.clone(), lens.clone());

        // Update QueryManager (indices, branch map, subscriptions)
        self.query_manager.add_live_schema(old_schema);
        self.query_manager.register_lens(lens);

        // Return reference to the registered lens
        self.context
            .get_lens(&source_hash, &self.context.current_hash)
            .ok_or(SchemaError::LensNotFound {
                source: source_hash,
                target: self.context.current_hash,
            })
    }

    /// Add a live schema version with explicit lens.
    ///
    /// Use this when auto-generated lens needs customization or
    /// when adding a schema with a manual migration.
    ///
    /// Automatically updates QueryManager indices and marks subscriptions for recompile.
    pub fn add_live_schema_with_lens(
        &mut self,
        old_schema: Schema,
        lens: Lens,
    ) -> Result<(), SchemaError> {
        let old_schema = strip_schema_policies(&old_schema);
        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }

        // Update context
        self.context
            .add_live_schema(old_schema.clone(), lens.clone());

        // Update QueryManager
        self.query_manager.add_live_schema(old_schema);
        self.query_manager.register_lens(lens);

        Ok(())
    }

    /// Register a lens between two schemas.
    ///
    /// Also registers the lens in QueryManager and tries to activate pending schemas.
    pub fn register_lens(&mut self, lens: Lens) -> Result<(), SchemaError> {
        if lens.is_draft() {
            return Err(SchemaError::DraftLensInPath {
                source: lens.source_hash,
                target: lens.target_hash,
            });
        }

        // Update context
        self.context.register_lens(lens.clone());

        // Update QueryManager
        self.query_manager.register_lens(lens);

        Ok(())
    }

    /// Get lens between two schemas if it exists.
    pub fn get_lens(&self, source: &SchemaHash, target: &SchemaHash) -> Option<&Lens> {
        self.context.get_lens(source, target)
    }

    /// Generate a lens between two schemas (may be draft).
    ///
    /// This doesn't register the lens - use `register_lens` after review.
    pub fn generate_lens(&self, old_schema: &Schema, new_schema: &Schema) -> Lens {
        generate_lens(old_schema, new_schema)
    }

    /// Get the lens path from a live schema to the current schema.
    ///
    /// Returns pairs of (lens, direction) indicating which transform to use.
    pub fn lens_path(
        &self,
        from: &SchemaHash,
    ) -> Result<Vec<(&Lens, super::lens::Direction)>, SchemaError> {
        self.context.lens_path(from)
    }

    /// Validate that all live schemas are reachable via non-draft lenses.
    pub fn validate(&self) -> Result<(), SchemaError> {
        self.context.validate()
    }

    /// Check if a schema hash is live (current or in live_schemas).
    pub fn is_live(&self, hash: &SchemaHash) -> bool {
        self.context.is_live(hash)
    }

    /// Get all live schema hashes.
    pub fn all_live_hashes(&self) -> Vec<SchemaHash> {
        self.context.all_live_hashes()
    }

    /// Get all known schema hashes (current + any learned via catalogue).
    pub fn known_schema_hashes(&self) -> Vec<SchemaHash> {
        self.known_schemas.keys().copied().collect()
    }

    /// Get all pending schema hashes awaiting lens-path activation.
    pub fn pending_schema_hashes(&self) -> Vec<SchemaHash> {
        self.context.pending_schemas.keys().copied().collect()
    }

    /// Get all registered lens edges as (source, target) hash pairs.
    pub fn lens_edges(&self) -> Vec<(SchemaHash, SchemaHash)> {
        self.context.lenses.keys().copied().collect()
    }

    /// Compute a canonical digest of the catalogue state known to this manager.
    pub fn catalogue_state_hash(&self) -> String {
        let mut hasher = Hasher::new();
        hasher.update(b"jazz-catalogue-state-v1");

        let mut schemas: Vec<_> = self.known_schemas.iter().collect();
        schemas.sort_by(|(left_hash, _), (right_hash, _)| {
            left_hash.as_bytes().cmp(right_hash.as_bytes())
        });
        hasher.update(&(schemas.len() as u64).to_le_bytes());
        for (hash, schema) in schemas {
            hasher.update(b"schema");
            hasher.update(hash.as_bytes());
            let encoded = encode_schema(schema);
            hash_len_prefixed(&mut hasher, &encoded);
        }

        let mut lenses: Vec<_> = self.context.lenses.values().collect();
        lenses.sort_by(|left, right| {
            left.source_hash
                .as_bytes()
                .cmp(right.source_hash.as_bytes())
                .then_with(|| {
                    left.target_hash
                        .as_bytes()
                        .cmp(right.target_hash.as_bytes())
                })
        });
        hasher.update(&(lenses.len() as u64).to_le_bytes());
        for lens in lenses {
            hasher.update(b"lens");
            hasher.update(lens.source_hash.as_bytes());
            hasher.update(lens.target_hash.as_bytes());
            let encoded = encode_lens_transform(&lens.forward);
            hash_len_prefixed(&mut hasher, &encoded);
        }

        if let Some(head) = self.current_permissions_head
            && let Some(bundle) = self.known_permissions_bundles.get(&head.bundle_object_id)
        {
            hasher.update(b"permissions");
            hasher.update(head.schema_hash.as_bytes());
            hasher.update(&head.version.to_le_bytes());
            if let Some(parent_bundle_object_id) = head.parent_bundle_object_id {
                hasher.update(parent_bundle_object_id.uuid().as_bytes());
            }
            let encoded = encode_permissions(&bundle.permissions);
            hash_len_prefixed(&mut hasher, &encoded);
        }

        hasher.finalize().to_hex().to_string()
    }

    /// Get access to the underlying context.
    pub fn context(&self) -> &SchemaContext {
        &self.context
    }

    /// Get mutable access to the underlying context.
    pub fn context_mut(&mut self) -> &mut SchemaContext {
        &mut self.context
    }

    /// Get reference to the internal QueryManager.
    pub fn query_manager(&self) -> &QueryManager {
        &self.query_manager
    }

    /// Get mutable reference to the internal QueryManager.
    pub fn query_manager_mut(&mut self) -> &mut QueryManager {
        &mut self.query_manager
    }

    pub fn current_permissions_head(&self) -> Option<PermissionsHeadSummary> {
        self.current_permissions_head
            .map(|head| PermissionsHeadSummary {
                schema_hash: head.schema_hash,
                version: head.version,
                parent_bundle_object_id: head.parent_bundle_object_id,
                bundle_object_id: head.bundle_object_id,
            })
    }

    pub fn current_permissions(&self) -> Option<CurrentPermissionsSummary> {
        let head = self.current_permissions_head()?;
        let bundle = self.known_permissions_bundles.get(&head.bundle_object_id)?;
        Some(CurrentPermissionsSummary {
            head,
            permissions: bundle.permissions.clone(),
        })
    }

    pub fn connection_schema_diagnostics(
        &self,
        client_schema_hash: SchemaHash,
    ) -> ConnectionSchemaDiagnostics {
        let active_permissions_hash = self
            .current_permissions_head
            .map(|head| head.schema_hash)
            .or_else(|| self.has_current_schema().then_some(self.current_hash()));
        let reachable_hashes = self.non_draft_reachable_hashes(client_schema_hash);
        let disconnected_permissions_schema_hash = active_permissions_hash
            .filter(|permissions_hash| !reachable_hashes.contains(permissions_hash));

        let mut unreachable_schema_hashes: Vec<_> = self
            .known_schema_hashes()
            .into_iter()
            .filter(|hash| *hash != client_schema_hash)
            .filter(|hash| !reachable_hashes.contains(hash))
            .filter(|hash| Some(*hash) != disconnected_permissions_schema_hash)
            .collect();
        unreachable_schema_hashes.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));

        ConnectionSchemaDiagnostics {
            client_schema_hash,
            disconnected_permissions_schema_hash,
            unreachable_schema_hashes,
        }
    }

    pub fn are_schema_hashes_connected(&self, from_hash: SchemaHash, to_hash: SchemaHash) -> bool {
        self.non_draft_reachable_hashes(from_hash)
            .contains(&to_hash)
    }

    fn non_draft_reachable_hashes(&self, start_hash: SchemaHash) -> HashSet<SchemaHash> {
        if !self.is_schema_known(&start_hash) {
            return HashSet::new();
        }

        let mut reachable = HashSet::from([start_hash]);
        let mut queue = VecDeque::from([start_hash]);

        while let Some(current) = queue.pop_front() {
            for (&(source_hash, target_hash), lens) in &self.context.lenses {
                if lens.is_draft() {
                    continue;
                }

                let next_hash = if source_hash == current {
                    Some(target_hash)
                } else if target_hash == current {
                    Some(source_hash)
                } else {
                    None
                };

                if let Some(next_hash) = next_hash
                    && self.is_schema_known(&next_hash)
                    && reachable.insert(next_hash)
                {
                    queue.push_back(next_hash);
                }
            }
        }

        reachable
    }

    // =========================================================================
    // Multi-Schema Query Support
    // =========================================================================

    /// Get branch names as strings for use with QueryBuilder.
    pub fn all_branch_strings(&self) -> Vec<String> {
        self.context
            .all_branch_names()
            .into_iter()
            .map(|b| b.as_str().to_string())
            .collect()
    }

    /// Build a mapping from branch name to schema hash.
    pub fn branch_schema_map(&self) -> std::collections::HashMap<String, SchemaHash> {
        let mut map = std::collections::HashMap::new();

        // Current schema branch
        map.insert(
            self.context.branch_name().as_str().to_string(),
            self.context.current_hash,
        );

        // Live schema branches
        for hash in self.context.live_schemas.keys() {
            let branch =
                ComposedBranchName::new(&self.context.env, *hash, &self.context.user_branch)
                    .to_branch_name();
            map.insert(branch.as_str().to_string(), *hash);
        }

        map
    }

    /// Create a LensTransformer for a specific table.
    pub fn transformer(&self, table: &str) -> super::transformer::LensTransformer<'_> {
        super::transformer::LensTransformer::new(&self.context, table)
    }

    /// Translate a column name for index lookup on a specific schema version.
    pub fn translate_column_for_schema(
        &self,
        table: &str,
        column: &str,
        target_hash: &SchemaHash,
    ) -> Option<String> {
        super::transformer::translate_column_for_index(&self.context, table, column, target_hash)
    }

    /// Get the descriptor for a table in a specific schema version.
    pub fn get_table_descriptor(
        &self,
        table: &str,
        schema_hash: &SchemaHash,
    ) -> Option<&crate::query_manager::types::RowDescriptor> {
        let schema = self.context.get_schema(schema_hash)?;
        let table_schema = schema.get(&crate::query_manager::types::TableName::new(table))?;
        Some(&table_schema.columns)
    }

    /// Return the timestamp when a schema was published.
    ///
    /// Tracks the most recent publish timestamp observed by this manager.
    pub fn schema_published_at(&self, schema_hash: &SchemaHash) -> Option<u64> {
        let object_id = schema_hash.to_object_id();
        self.catalogue_publish_timestamps.get(&object_id).copied()
    }

    // =========================================================================
    // Catalogue Persistence
    // =========================================================================

    fn persist_catalogue_object_if_changed<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
        content: Vec<u8>,
    ) -> bool {
        if latest_catalogue_content_matches(storage, object_id, &content) {
            return false;
        }

        let timestamp = self.query_manager.sync_manager_mut().reserve_timestamp();
        self.catalogue_publish_timestamps
            .insert(object_id, timestamp);
        self.query_manager
            .sync_manager_mut()
            .upsert_catalogue_entry(
                storage,
                CatalogueEntry {
                    object_id,
                    metadata,
                    content,
                },
            );
        true
    }

    pub fn ensure_current_schema_persisted<H: Storage>(&mut self, storage: &mut H) -> bool {
        if !self.context.is_initialized() {
            return false;
        }
        let schema_hash = self.context.current_hash;
        let storage_key = (storage.storage_cache_namespace(), schema_hash);
        if self
            .persisted_current_schema_in_storage
            .contains(&storage_key)
        {
            return false;
        }
        let object_id = schema_hash.to_object_id();
        let metadata = self.schema_metadata(&schema_hash);
        let content = encode_schema(&strip_schema_policies(&self.context.current_schema));

        let changed =
            self.persist_catalogue_object_if_changed(storage, object_id, metadata, content);
        self.persisted_current_schema_in_storage.insert(storage_key);
        changed
    }

    /// Persist the current schema to the catalogue as an Object.
    ///
    /// The schema is stored on the "main" branch with metadata identifying it
    /// as a catalogue schema for this app. Other clients with the same app_id
    /// will receive this via catalogue sync.
    ///
    /// Returns the ObjectId of the stored schema object.
    pub fn persist_schema<H: Storage>(&mut self, storage: &mut H) -> ObjectId {
        let schema_hash = self.context.current_hash;
        let object_id = schema_hash.to_object_id();
        let content = encode_schema(&strip_schema_policies(&self.context.current_schema));

        let metadata = self.schema_metadata(&schema_hash);
        let timestamp = self.query_manager.sync_manager_mut().reserve_timestamp();
        self.catalogue_publish_timestamps
            .insert(object_id, timestamp);
        self.query_manager
            .sync_manager_mut()
            .upsert_catalogue_entry(
                storage,
                CatalogueEntry {
                    object_id,
                    metadata,
                    content,
                },
            );

        object_id
    }

    /// Persist any schema to the catalogue as an Object.
    ///
    /// Used when seeding or syncing historical schema versions.
    pub fn persist_schema_object<H: Storage>(
        &mut self,
        storage: &mut H,
        schema: &Schema,
    ) -> ObjectId {
        let schema = strip_schema_policies(schema);
        let schema_hash = SchemaHash::compute(&schema);
        let object_id = schema_hash.to_object_id();
        let content = encode_schema(&schema);

        let metadata = self.schema_metadata(&schema_hash);
        let timestamp = self.query_manager.sync_manager_mut().reserve_timestamp();
        self.catalogue_publish_timestamps
            .insert(object_id, timestamp);
        self.query_manager
            .sync_manager_mut()
            .upsert_catalogue_entry(
                storage,
                CatalogueEntry {
                    object_id,
                    metadata,
                    content,
                },
            );

        object_id
    }

    /// Persist a lens to the catalogue as an Object.
    ///
    /// The lens is stored on the "main" branch with metadata identifying it
    /// as a catalogue lens for this app. Other clients with the same app_id
    /// will receive this via catalogue sync.
    ///
    /// Returns the ObjectId of the stored lens object.
    pub fn persist_lens<H: Storage>(&mut self, storage: &mut H, lens: &Lens) -> ObjectId {
        let object_id = lens.object_id();
        let content = encode_lens_transform(&lens.forward);

        let metadata = self.lens_metadata(lens);
        let timestamp = self.query_manager.sync_manager_mut().reserve_timestamp();
        self.catalogue_publish_timestamps
            .insert(object_id, timestamp);
        self.query_manager
            .sync_manager_mut()
            .upsert_catalogue_entry(
                storage,
                CatalogueEntry {
                    object_id,
                    metadata,
                    content,
                },
            );

        object_id
    }

    pub fn persist_current_permissions<H: Storage>(&mut self, storage: &mut H) -> Option<ObjectId> {
        let head = self.current_permissions_head?;
        let bundle = self.known_permissions_bundles.get(&head.bundle_object_id)?;

        let bundle_metadata = self.permissions_bundle_metadata();
        let head_object_id = self.permissions_head_object_id();
        let head_metadata = self.permissions_head_metadata();
        let bundle_content = encode_permissions_bundle(
            bundle.schema_hash,
            bundle.version,
            bundle.parent_bundle_object_id,
            &bundle.permissions,
        );
        let bundle_timestamp = self.query_manager.sync_manager_mut().reserve_timestamp();
        self.catalogue_publish_timestamps
            .insert(head.bundle_object_id, bundle_timestamp);
        self.query_manager
            .sync_manager_mut()
            .upsert_catalogue_entry(
                storage,
                CatalogueEntry {
                    object_id: head.bundle_object_id,
                    metadata: bundle_metadata,
                    content: bundle_content,
                },
            );

        let head_content = encode_permissions_head(
            head.schema_hash,
            head.version,
            head.parent_bundle_object_id,
            head.bundle_object_id,
        );
        self.persist_catalogue_object_if_changed(
            storage,
            head_object_id,
            head_metadata,
            head_content,
        );

        Some(head_object_id)
    }

    pub fn publish_permissions_bundle<H: Storage>(
        &mut self,
        storage: &mut H,
        schema_hash: SchemaHash,
        permissions: HashMap<TableName, TablePolicies>,
        expected_parent_bundle_object_id: Option<ObjectId>,
    ) -> Result<Option<ObjectId>, SchemaError> {
        let current_parent_bundle_object_id = self
            .current_permissions_head
            .map(|head| head.bundle_object_id);
        if current_parent_bundle_object_id != expected_parent_bundle_object_id {
            return Err(SchemaError::StalePermissionsParent {
                expected: expected_parent_bundle_object_id,
                current: current_parent_bundle_object_id,
            });
        }

        if let Some(head) = self.current_permissions_head
            && head.schema_hash == schema_hash
            && let Some(existing) = self.known_permissions_bundles.get(&head.bundle_object_id)
            && existing.permissions == permissions
        {
            return Ok(Some(self.permissions_head_object_id()));
        }

        let version = self
            .current_permissions_head
            .map(|head| head.version + 1)
            .unwrap_or(1);
        let bundle_state = PermissionsBundleState {
            schema_hash,
            version,
            parent_bundle_object_id: current_parent_bundle_object_id,
            permissions,
        };
        let bundle_object_id = self.permissions_bundle_object_id(&bundle_state);
        self.known_permissions_bundles
            .insert(bundle_object_id, bundle_state);
        let head = PermissionsHeadState {
            schema_hash,
            version,
            parent_bundle_object_id: current_parent_bundle_object_id,
            bundle_object_id,
        };
        self.current_permissions_head = Some(head);
        if self.apply_permissions_head(head) {
            self.pending_permissions_head = None;
        } else {
            self.pending_permissions_head = Some(head);
        }
        Ok(self.persist_current_permissions(storage))
    }

    /// Register a reviewed lens in memory, activate any newly reachable schemas,
    /// and persist the corresponding catalogue object for sync.
    pub fn publish_lens<H: Storage>(
        &mut self,
        storage: &mut H,
        lens: &Lens,
    ) -> Result<ObjectId, SchemaError> {
        self.register_lens(lens.clone())?;
        self.activate_pending_and_sync_to_query_manager();
        Ok(self.persist_lens(storage, lens))
    }

    /// Build metadata for a schema catalogue object.
    fn schema_metadata(&self, schema_hash: &SchemaHash) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CatalogueSchema.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            self.app_id.uuid().to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::SchemaHash.to_string(),
            schema_hash.to_string(),
        );
        metadata
    }

    pub(crate) fn permissions_head_object_id_for(app_id: AppId) -> ObjectId {
        ObjectId::from_uuid(Uuid::new_v5(
            &Uuid::NAMESPACE_DNS,
            format!("jazz-catalogue-permissions-head:{}", app_id.uuid()).as_bytes(),
        ))
    }

    fn permissions_head_object_id(&self) -> ObjectId {
        Self::permissions_head_object_id_for(self.app_id)
    }

    fn permissions_bundle_object_id(&self, bundle: &PermissionsBundleState) -> ObjectId {
        let mut identity =
            format!("jazz-catalogue-permissions-bundle:{}:", self.app_id.uuid()).into_bytes();
        identity.extend_from_slice(&encode_permissions_bundle(
            bundle.schema_hash,
            bundle.version,
            bundle.parent_bundle_object_id,
            &bundle.permissions,
        ));
        ObjectId::from_uuid(Uuid::new_v5(&Uuid::NAMESPACE_DNS, &identity))
    }

    fn permissions_bundle_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CataloguePermissionsBundle.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            self.app_id.uuid().to_string(),
        );
        metadata
    }

    fn permissions_head_metadata(&self) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CataloguePermissionsHead.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            self.app_id.uuid().to_string(),
        );
        metadata
    }

    /// Build metadata for a lens catalogue object.
    fn lens_metadata(&self, lens: &Lens) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CatalogueLens.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            self.app_id.uuid().to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::SourceHash.to_string(),
            lens.source_hash.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::TargetHash.to_string(),
            lens.target_hash.to_string(),
        );
        metadata
    }

    /// Process a catalogue update received via sync.
    ///
    /// Called when QueryManager receives an object with catalogue metadata
    /// matching this app_id.
    ///
    /// For schemas: stored as pending until a lens path exists.
    /// For lenses: registered immediately, then pending schemas are checked.
    pub fn process_catalogue_update(
        &mut self,
        object_id: ObjectId,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        let Some(type_str) = metadata.get(crate::metadata::MetadataKey::Type.as_str()) else {
            return Ok(()); // Not a catalogue object
        };

        match type_str.as_str() {
            t if t == crate::metadata::ObjectType::CatalogueSchema.as_str() => {
                self.process_catalogue_schema(metadata, content)
            }
            t if t == crate::metadata::ObjectType::CataloguePermissionsBundle.as_str() => {
                self.process_catalogue_permissions_bundle(object_id, metadata, content)
            }
            t if t == crate::metadata::ObjectType::CataloguePermissionsHead.as_str() => {
                self.process_catalogue_permissions_head(metadata, content)
            }
            t if t == crate::metadata::ObjectType::CataloguePermissions.as_str() => {
                self.process_catalogue_permissions_legacy(object_id, metadata, content)
            }
            t if t == crate::metadata::ObjectType::CatalogueLens.as_str() => {
                self.process_catalogue_lens(metadata, content)
            }
            _ => Ok(()), // Unknown type, ignore
        }
    }

    fn process_catalogue_schema(
        &mut self,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        // Verify app_id matches
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(()); // Different app, ignore
        }

        // Decode schema
        let schema = decode_schema(content)
            .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;

        // An empty schema (zero tables) carries no structural information and
        // can only appear from legacy bugs that persisted the uninitialized
        // server context. Ignore it so stale entries don't surface as
        // "unreachable" hashes in connection diagnostics.
        if schema.is_empty() {
            return Ok(());
        }

        let hash = SchemaHash::compute(&schema);

        // Always add to known_schemas (server or client)
        // This allows server-mode query execution even without lens paths
        if !self.known_schemas.contains_key(&hash) {
            Arc::make_mut(&mut self.known_schemas).insert(hash, schema.clone());
            self.known_schemas_dirty = true;
        }

        // Skip if already live or is current
        if self.context.is_live(&hash) {
            return Ok(());
        }

        // If we have a current schema, also try lens-path activation
        if self.context.is_initialized() {
            // Add to pending - will be activated when lens path becomes available
            self.context.add_pending_schema(schema);

            // Try to activate in case we already have the lens path
            self.activate_pending_and_sync_to_query_manager();
        }

        self.try_apply_pending_permissions_head();

        Ok(())
    }

    fn process_catalogue_permissions_bundle(
        &mut self,
        object_id: ObjectId,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(());
        }

        let (schema_hash, version, parent_bundle_object_id, permissions) =
            decode_permissions_bundle(content)
                .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;
        self.known_permissions_bundles.insert(
            object_id,
            PermissionsBundleState {
                schema_hash,
                version,
                parent_bundle_object_id,
                permissions,
            },
        );

        self.try_apply_pending_permissions_head();

        Ok(())
    }

    fn process_catalogue_permissions_head(
        &mut self,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(());
        }

        let (schema_hash, version, parent_bundle_object_id, bundle_object_id) =
            decode_permissions_head(content)
                .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;
        let head = PermissionsHeadState {
            schema_hash,
            version,
            parent_bundle_object_id,
            bundle_object_id,
        };
        self.query_manager.require_authorization_schema();
        if let Some(current_head) = self.current_permissions_head
            && current_head.version > head.version
        {
            return Ok(());
        }
        self.current_permissions_head = Some(head);
        if self.apply_permissions_head(head) {
            self.pending_permissions_head = None;
        } else {
            self.pending_permissions_head = Some(head);
        }

        Ok(())
    }

    fn process_catalogue_permissions_legacy(
        &mut self,
        object_id: ObjectId,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(());
        }

        let schema_hash = metadata
            .get(crate::metadata::MetadataKey::SchemaHash.as_str())
            .ok_or_else(|| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))
            .and_then(|value| parse_schema_hash(value))?;
        let permissions = decode_permissions(content)
            .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;
        self.known_permissions_bundles.insert(
            object_id,
            PermissionsBundleState {
                schema_hash,
                version: 1,
                parent_bundle_object_id: None,
                permissions,
            },
        );
        let head = PermissionsHeadState {
            schema_hash,
            version: 1,
            parent_bundle_object_id: None,
            bundle_object_id: object_id,
        };
        self.query_manager.require_authorization_schema();
        self.current_permissions_head = Some(head);
        if self.apply_permissions_head(head) {
            self.pending_permissions_head = None;
        } else {
            self.pending_permissions_head = Some(head);
        }

        Ok(())
    }

    fn process_catalogue_lens(
        &mut self,
        metadata: &HashMap<String, String>,
        content: &[u8],
    ) -> Result<(), SchemaError> {
        // Verify app_id matches
        let app_id_str = metadata
            .get(crate::metadata::MetadataKey::AppId.as_str())
            .map(|s| s.as_str())
            .unwrap_or("");
        if app_id_str != self.app_id.uuid().to_string() {
            return Ok(()); // Different app, ignore
        }

        // Parse source/target hashes from metadata
        let source_hex = metadata
            .get(crate::metadata::MetadataKey::SourceHash.as_str())
            .ok_or_else(|| SchemaError::LensNotFound {
                source: SchemaHash::from_bytes([0; 32]),
                target: SchemaHash::from_bytes([0; 32]),
            })?;
        let target_hex = metadata
            .get(crate::metadata::MetadataKey::TargetHash.as_str())
            .ok_or_else(|| SchemaError::LensNotFound {
                source: SchemaHash::from_bytes([0; 32]),
                target: SchemaHash::from_bytes([0; 32]),
            })?;

        let source_hash = parse_schema_hash(source_hex)?;
        let target_hash = parse_schema_hash(target_hex)?;

        // Skip if we already have this lens (handles duplicate syncs)
        // Since ObjectId is deterministic from hashes and encoding is deterministic,
        // the same source/target should always produce identical content.
        if self.context.get_lens(&source_hash, &target_hash).is_some() {
            return Ok(());
        }

        // Decode lens transform
        let transform = decode_lens_transform(content).map_err(|_| SchemaError::LensNotFound {
            source: source_hash,
            target: target_hash,
        })?;

        // Reconstruct lens (backward is computed from forward)
        let lens = Lens::new(source_hash, target_hash, transform);

        // Log warning if draft, but still store it
        // Note: Draft lenses can still be registered but won't be used for activation
        // unless they're the only path available (which will fail validation)
        if lens.is_draft() {
            // TODO: proper logging
            // Draft lens received via catalogue - storing but not activating schemas through it
        }

        // Register the lens in both context and QueryManager
        self.context.register_lens(lens.clone());
        self.query_manager.register_lens(lens);

        // Try to activate pending schemas that may now be reachable
        self.activate_pending_and_sync_to_query_manager();
        self.try_apply_pending_permissions_head();

        Ok(())
    }

    fn schema_for_permissions_hash(&self, schema_hash: SchemaHash) -> Option<Schema> {
        if self.context.is_initialized() && self.context.current_hash == schema_hash {
            return Some(strip_schema_policies(&self.context.current_schema));
        }

        self.context
            .get_schema(&schema_hash)
            .map(strip_schema_policies)
            .or_else(|| self.known_schemas.get(&schema_hash).cloned())
    }

    fn apply_permissions_head(&mut self, head: PermissionsHeadState) -> bool {
        let Some(bundle) = self.known_permissions_bundles.get(&head.bundle_object_id) else {
            return false;
        };
        if bundle.schema_hash != head.schema_hash {
            return false;
        }
        if bundle.version != head.version {
            return false;
        }
        if bundle.parent_bundle_object_id != head.parent_bundle_object_id {
            return false;
        }
        let Some(schema) = self.schema_for_permissions_hash(head.schema_hash) else {
            return false;
        };

        let authorization_schema = merge_permissions_into_schema(&schema, &bundle.permissions);
        self.query_manager
            .set_authorization_schema(authorization_schema);
        true
    }

    fn try_apply_pending_permissions_head(&mut self) {
        let Some(head) = self.pending_permissions_head else {
            return;
        };

        if self.apply_permissions_head(head) {
            self.pending_permissions_head = None;
        }
    }

    /// Try to activate pending schemas that now have lens paths.
    ///
    /// Called after registering new lenses. Returns hashes of newly activated schemas.
    pub fn try_activate_pending_schemas(&mut self) -> Vec<SchemaHash> {
        self.context.try_activate_pending()
    }

    /// Activate pending schemas and sync them to QueryManager.
    ///
    /// This is the incremental replacement for sync_context().
    fn activate_pending_and_sync_to_query_manager(&mut self) {
        let activated = self.context.try_activate_pending();
        if activated.is_empty() {
            return;
        }

        // For each newly activated schema, add it to QueryManager
        for hash in &activated {
            if let Some(schema) = self.context.live_schemas.get(hash).cloned() {
                self.query_manager.add_live_schema(schema);
            }
        }

        // Pending row updates will be retried in the next process() call,
        // which has access to Storage needed for index updates.
    }

    // =========================================================================
    // Query/Write Operations (delegated to QueryManager)
    // =========================================================================

    /// Create a query builder for a table.
    pub fn query(&self, table: &str) -> QueryBuilder {
        QueryBuilder::new(table)
    }

    /// Subscribe to a query with explicit schema context (for server use).
    ///
    /// Servers don't have a fixed "current" schema - they serve multiple clients
    /// with different schema versions. This method allows subscribing to a query
    /// using the client's schema as the "current" for that subscription.
    ///
    /// The schema must be in `known_schemas` (received via catalogue sync).
    /// Returns `UnknownSchema` error if the schema is not known.
    ///
    /// # Arguments
    ///
    /// * `query` - The query to subscribe to
    /// * `ctx` - Schema context from the client (env, schema_hash, user_branch)
    /// * `session` - Optional session for policy evaluation
    pub fn subscribe_with_schema_context(
        &mut self,
        query: Query,
        ctx: &QuerySchemaContext,
        session: Option<Session>,
    ) -> Result<crate::query_manager::QuerySubscriptionId, QueryError> {
        // Look up the target schema in known_schemas
        let target_schema = self
            .known_schemas
            .get(&ctx.schema_hash)
            .ok_or(QueryError::UnknownSchema(ctx.schema_hash))?
            .clone();

        // Build a SchemaContext with target as current
        let mut temp_context =
            SchemaContext::new(target_schema.clone(), &ctx.env, &ctx.user_branch);

        // Copy lenses from our main context for multi-schema queries
        for ((_source, _target), lens) in &self.context.lenses {
            temp_context.register_lens(lens.clone());
        }

        // Add other known schemas as potential live schemas
        for (hash, schema) in self.known_schemas.iter() {
            if *hash != ctx.schema_hash {
                // Add to pending - will activate if lens path exists to target
                temp_context.add_pending_schema(schema.clone());
            }
        }

        // Try to activate any pending schemas that now have lens paths
        temp_context.try_activate_pending();

        // Ensure the client's branch is registered for indexing (server-mode)
        let client_branch =
            ComposedBranchName::new(&ctx.env, ctx.schema_hash, &ctx.user_branch).to_branch_name();
        self.query_manager
            .add_schema_branch(client_branch.as_str(), ctx.schema_hash);

        // Ensure indices exist for all branches in the temp context
        for branch_name in temp_context.all_branch_names() {
            let branch_str = branch_name.as_str();
            if let Some(composed) = ComposedBranchName::parse(&branch_name)
                && let Some(schema) = self.known_schemas.get(&composed.schema_hash)
            {
                for (table_name, table_schema) in schema {
                    self.query_manager.ensure_indices_for_branch(
                        table_name.as_str(),
                        branch_str,
                        table_schema,
                    );
                }
            }
        }

        // Subscribe using the temporary context
        self.query_manager.subscribe_with_explicit_context(
            query,
            &target_schema,
            &temp_context,
            session,
        )
    }

    /// Insert a row into the current schema's branch.
    pub fn insert<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: HashMap<String, Value>,
    ) -> Result<InsertResult, QueryError> {
        let _span =
            tracing::debug_span!("SM::insert", table, schema_hash = %self.context.current_hash)
                .entered();
        self.insert_with_write_context(storage, table, values, None)
    }

    /// Insert with session-based policy checking.
    ///
    /// Omitted fields are filled from schema defaults or nullable nulls.
    pub fn insert_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
    ) -> Result<InsertResult, QueryError> {
        self.insert_with_write_context_and_id(storage, table, values, None, write_context)
    }

    /// Insert with session-based policy checking and an optional caller-supplied row id.
    pub fn insert_with_write_context_and_id<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: HashMap<String, Value>,
        object_id: Option<ObjectId>,
        write_context: Option<&WriteContext>,
    ) -> Result<InsertResult, QueryError> {
        let _ = self.ensure_current_schema_persisted(storage);
        let (target_branch, target_hash) = self.resolve_target_branch(write_context)?;
        let target_schema = self
            .schema_for_hash(target_hash)
            .ok_or(QueryError::UnknownSchema(target_hash))?
            .clone();
        let aligned_values =
            Self::get_insert_values_with_defaults_for_schema(table, &target_schema, values)?;
        self.query_manager
            .insert_on_branch_with_schema_and_write_context_and_id(
                storage,
                table,
                &target_branch,
                &aligned_values,
                object_id,
                &target_schema,
                write_context,
            )
    }

    pub fn insert_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: HashMap<String, Value>,
        session: Option<&Session>,
    ) -> Result<InsertResult, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.insert_with_write_context(storage, table, values, owned.as_ref())
    }

    pub fn can_insert_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
    ) -> Result<PermissionPreflightDecision, QueryError> {
        let (target_branch, target_hash) = match self.resolve_target_branch(write_context) {
            Ok(target) => target,
            Err(QueryError::UnknownSchema(_)) => {
                return Ok(PermissionPreflightDecision::Unknown);
            }
            Err(error) => return Err(error),
        };
        let Some(target_schema) = self.schema_for_hash(target_hash).cloned() else {
            return Ok(PermissionPreflightDecision::Unknown);
        };
        let aligned_values =
            Self::get_insert_values_with_defaults_for_schema(table, &target_schema, values)?;
        self.query_manager
            .can_insert_on_branch_with_schema_and_write_context(
                storage,
                crate::query_manager::writes::RowBranchInsert {
                    table,
                    branch: &target_branch,
                    values: &aligned_values,
                },
                &target_schema,
                write_context,
            )
    }

    pub fn can_insert_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        values: HashMap<String, Value>,
        session: Option<&Session>,
    ) -> Result<PermissionPreflightDecision, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.can_insert_with_write_context(storage, table, values, owned.as_ref())
    }

    /// Create or update a row with a caller-supplied UUID.
    ///
    /// If a visible row already exists for `object_id`, only the supplied
    /// columns are updated. Otherwise a new row is inserted with that id.
    pub fn upsert_with_write_context_and_id<H: Storage>(
        &mut self,
        storage: &mut H,
        table: &str,
        object_id: ObjectId,
        values: HashMap<String, Value>,
        write_context: Option<&WriteContext>,
    ) -> Result<crate::row_histories::BatchId, QueryError> {
        let _ = self.ensure_current_schema_persisted(storage);
        let (target_branch, target_hash) = self.resolve_target_branch(write_context)?;
        let target_context = self.schema_context_for_hash(target_hash)?;
        let branches = target_context
            .all_branch_names()
            .into_iter()
            .map(|branch_name| branch_name.as_str().to_string())
            .collect::<Vec<_>>();

        if let Some(existing_table) = write_context
            .filter(|ctx| ctx.batch_mode() == crate::batch_fate::BatchMode::Transactional)
            .and_then(WriteContext::batch_id)
            .and_then(|batch_id| {
                self.query_manager
                    .load_latest_transactional_staged_row_on_branch(
                        storage,
                        object_id,
                        &target_branch,
                        batch_id,
                    )
                    .map(|(table, _row)| table)
            })
            .or_else(|| {
                self.query_manager
                    .load_row_for_schema_update_in_context(
                        storage,
                        object_id,
                        &branches,
                        &target_context,
                    )
                    .map(|(table, ..)| table)
            })
        {
            if existing_table != table {
                return Err(QueryError::EncodingError(format!(
                    "object {object_id} already exists in table {existing_table}, cannot upsert into {table}"
                )));
            }

            let updates = values.into_iter().collect::<Vec<_>>();
            return self.update_with_write_context(storage, object_id, &updates, write_context);
        }

        let inserted = self.insert_with_write_context_and_id(
            storage,
            table,
            values,
            Some(object_id),
            write_context,
        )?;
        Ok(inserted.batch_id)
    }

    /// Update a row using current-schema column names, performing copy-on-write
    /// when the latest visible row batch entry still lives on an older schema branch.
    pub fn update_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        values: &[(String, Value)],
        write_context: Option<&WriteContext>,
    ) -> Result<crate::row_histories::BatchId, QueryError> {
        let _ = self.ensure_current_schema_persisted(storage);
        let (target_branch, target_hash) = self.resolve_target_branch(write_context)?;
        let target_schema = self
            .schema_for_hash(target_hash)
            .ok_or(QueryError::UnknownSchema(target_hash))?
            .clone();
        let target_context = self.schema_context_for_hash(target_hash)?;
        let branches = target_context
            .all_branch_names()
            .into_iter()
            .map(|branch_name| branch_name.as_str().to_string())
            .collect::<Vec<_>>();
        let (table, source_branch, old_current_data, _source_commit_id, old_current_provenance) =
            write_context
                .filter(|ctx| ctx.batch_mode() == crate::batch_fate::BatchMode::Transactional)
                .and_then(WriteContext::batch_id)
                .and_then(|batch_id| {
                    self.query_manager
                        .load_latest_transactional_staged_row_on_branch(
                            storage,
                            object_id,
                            &target_branch,
                            batch_id,
                        )
                        .map(|(table, row)| {
                            (
                                table,
                                target_branch.clone(),
                                row.data.to_vec(),
                                row.batch_id(),
                                row.row_provenance(),
                            )
                        })
                })
                .or_else(|| {
                    self.query_manager.load_row_for_schema_update_in_context(
                        storage,
                        object_id,
                        &branches,
                        &target_context,
                    )
                })
                .ok_or(QueryError::ObjectNotFound(object_id))?;

        let table_name = TableName::new(&table);
        let descriptor = target_schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?
            .columns
            .clone();

        let mut current_values = decode_row(&descriptor, &old_current_data)
            .map_err(|err| QueryError::EncodingError(format!("{err:?}")))?;

        for (column_name, new_value) in values {
            let Some(index) = descriptor.column_index(column_name) else {
                return Err(QueryError::EncodingError(format!(
                    "column '{column_name}' not found"
                )));
            };
            current_values[index] = new_value.clone();
        }

        let _ = source_branch;
        let batch_id = self
            .query_manager
            .write_existing_row_on_branch_with_schema_and_write_context(
                storage,
                RowBranchWrite {
                    table: &table,
                    branch: &target_branch,
                    id: object_id,
                    values: &current_values,
                    old_data_for_policy: &old_current_data,
                    old_provenance_for_policy: &old_current_provenance,
                },
                &target_schema,
                write_context,
            )?;

        Ok(batch_id)
    }

    pub fn update_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        values: &[(String, Value)],
        session: Option<&Session>,
    ) -> Result<crate::row_histories::BatchId, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.update_with_write_context(storage, object_id, values, owned.as_ref())
    }

    pub fn can_update_with_write_context<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        values: &[(String, Value)],
        write_context: Option<&WriteContext>,
    ) -> Result<PermissionPreflightDecision, QueryError> {
        let (target_branch, target_hash) = match self.resolve_target_branch(write_context) {
            Ok(target) => target,
            Err(QueryError::UnknownSchema(_)) => {
                return Ok(PermissionPreflightDecision::Unknown);
            }
            Err(error) => return Err(error),
        };
        let Some(target_schema) = self.schema_for_hash(target_hash).cloned() else {
            return Ok(PermissionPreflightDecision::Unknown);
        };
        let target_context = match self.schema_context_for_hash(target_hash) {
            Ok(context) => context,
            Err(QueryError::UnknownSchema(_)) => {
                return Ok(PermissionPreflightDecision::Unknown);
            }
            Err(error) => return Err(error),
        };
        let branches = target_context
            .all_branch_names()
            .into_iter()
            .map(|branch_name| branch_name.as_str().to_string())
            .collect::<Vec<_>>();
        let loaded_row = write_context
            .filter(|ctx| ctx.batch_mode() == crate::batch_fate::BatchMode::Transactional)
            .and_then(WriteContext::batch_id)
            .and_then(|batch_id| {
                self.query_manager
                    .load_latest_transactional_staged_row_on_branch(
                        storage,
                        object_id,
                        &target_branch,
                        batch_id,
                    )
                    .map(|(table, row)| {
                        if row.is_hard_deleted() {
                            SchemaUpdateRowLoad::HardDeleted
                        } else {
                            SchemaUpdateRowLoad::Found {
                                table,
                                branch: target_branch.clone(),
                                data: row.data.to_vec(),
                                batch_id: row.batch_id(),
                                provenance: row.row_provenance(),
                            }
                        }
                    })
            })
            .or_else(|| {
                self.query_manager
                    .load_schema_update_row_in_context_for_tier(
                        storage,
                        object_id,
                        &branches,
                        &target_context,
                        None,
                    )
            });
        let Some(loaded_row) = loaded_row else {
            return Ok(PermissionPreflightDecision::Unknown);
        };
        let (table, _source_branch, old_current_data, _source_commit_id, old_current_provenance) =
            match loaded_row {
                SchemaUpdateRowLoad::Found {
                    table,
                    branch,
                    data,
                    batch_id,
                    provenance,
                } => (table, branch, data, batch_id, provenance),
                SchemaUpdateRowLoad::HardDeleted => {
                    return Ok(PermissionPreflightDecision::Deny);
                }
            };

        let table_name = TableName::new(&table);
        let descriptor = target_schema
            .get(&table_name)
            .ok_or(QueryError::TableNotFound(table_name))?
            .columns
            .clone();

        let mut current_values = decode_row(&descriptor, &old_current_data)
            .map_err(|err| QueryError::EncodingError(format!("{err:?}")))?;

        for (column_name, new_value) in values {
            let Some(index) = descriptor.column_index(column_name) else {
                return Err(QueryError::EncodingError(format!(
                    "column '{column_name}' not found"
                )));
            };
            current_values[index] = new_value.clone();
        }

        self.query_manager
            .can_update_existing_row_on_branch_with_schema_and_write_context(
                storage,
                RowBranchWrite {
                    table: &table,
                    branch: &target_branch,
                    id: object_id,
                    values: &current_values,
                    old_data_for_policy: &old_current_data,
                    old_provenance_for_policy: &old_current_provenance,
                },
                &target_schema,
                write_context,
            )
    }

    pub fn can_update_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        values: &[(String, Value)],
        session: Option<&Session>,
    ) -> Result<PermissionPreflightDecision, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.can_update_with_write_context(storage, object_id, values, owned.as_ref())
    }

    /// Delete a row (soft delete), performing copy-on-write when the latest
    /// visible row batch entry still lives on an older schema branch.
    pub fn delete<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        write_context: Option<&WriteContext>,
    ) -> Result<DeleteHandle, QueryError> {
        let _ = self.ensure_current_schema_persisted(storage);
        let (target_branch, target_hash) = self.resolve_target_branch(write_context)?;
        let target_schema = self
            .schema_for_hash(target_hash)
            .ok_or(QueryError::UnknownSchema(target_hash))?
            .clone();
        let target_context = self.schema_context_for_hash(target_hash)?;
        let branches = target_context
            .all_branch_names()
            .into_iter()
            .map(|branch_name| branch_name.as_str().to_string())
            .collect::<Vec<_>>();
        let (table, source_branch, old_current_data, _source_commit_id, old_current_provenance) =
            write_context
                .filter(|ctx| ctx.batch_mode() == crate::batch_fate::BatchMode::Transactional)
                .and_then(WriteContext::batch_id)
                .and_then(|batch_id| {
                    self.query_manager
                        .load_latest_transactional_staged_row_on_branch(
                            storage,
                            object_id,
                            &target_branch,
                            batch_id,
                        )
                        .map(|(table, row)| {
                            (
                                table,
                                target_branch.clone(),
                                row.data.to_vec(),
                                row.batch_id(),
                                row.row_provenance(),
                            )
                        })
                })
                .or_else(|| {
                    self.query_manager.load_row_for_schema_update_in_context(
                        storage,
                        object_id,
                        &branches,
                        &target_context,
                    )
                })
                .ok_or(QueryError::ObjectNotFound(object_id))?;

        let _span = tracing::debug_span!("SM::delete", table, %object_id, schema_hash = %self.context.current_hash).entered();
        let _ = source_branch;
        self.query_manager
            .delete_existing_row_on_branch_with_schema_and_write_context(
                storage,
                RowBranchDelete {
                    table: &table,
                    branch: &target_branch,
                    id: object_id,
                    old_data_for_policy: &old_current_data,
                    old_provenance_for_policy: &old_current_provenance,
                },
                &target_schema,
                write_context,
            )
    }

    pub fn delete_with_session<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        session: Option<&Session>,
    ) -> Result<DeleteHandle, QueryError> {
        let owned = session.cloned().map(WriteContext::from_session);
        self.delete(storage, object_id, owned.as_ref())
    }

    /// Process pending operations (drives SyncManager).
    ///
    /// This also processes any pending catalogue updates (schemas/lenses) that
    /// were received via sync. Catalogue schemas are stored as pending until
    /// a lens path exists, then activated.
    ///
    /// When schemas activate, QueryManager is updated incrementally and
    /// buffered row updates are retried.
    pub fn process<H: Storage>(&mut self, storage: &mut H) {
        let _span = tracing::debug_span!("SM::process").entered();
        self.query_manager.process(storage);

        // Process any catalogue updates queued by QueryManager
        let updates = self.query_manager.take_pending_catalogue_updates();
        for update in updates {
            // Ignore errors from individual catalogue updates - they're non-critical
            let _ =
                self.process_catalogue_update(update.object_id, &update.metadata, &update.content);
        }

        // Sync known schemas to QueryManager for server-mode lazy activation
        // This enables QueryManager to activate branches when rows arrive
        if self.known_schemas_dirty {
            self.query_manager
                .set_known_schemas(Arc::clone(&self.known_schemas));
            self.known_schemas_dirty = false;
        }

        // Final attempt to activate any remaining pending schemas
        self.activate_pending_and_sync_to_query_manager();

        // Retry any pending row updates that might now be processable
        self.query_manager
            .retry_pending_row_visibility_changes(storage);
    }
}

fn latest_catalogue_content_matches<H: Storage + ?Sized>(
    storage: &H,
    object_id: ObjectId,
    expected: &[u8],
) -> bool {
    let latest_content = if let Ok(Some(content)) = latest_catalogue_content(storage, object_id) {
        content
    } else {
        return false;
    };
    latest_content == expected
}

#[allow(dead_code)]
fn reorder_values_by_column_name(
    source_descriptor: &RowDescriptor,
    target_descriptor: &RowDescriptor,
    values: &[Value],
) -> Option<Vec<Value>> {
    if values.len() != source_descriptor.columns.len()
        || source_descriptor.columns.len() != target_descriptor.columns.len()
    {
        return None;
    }

    let mut values_by_column = HashMap::with_capacity(values.len());
    for (column, value) in source_descriptor.columns.iter().zip(values.iter()) {
        values_by_column.insert(column.name, value.clone());
    }

    let mut reordered_values = Vec::with_capacity(values.len());
    for column in &target_descriptor.columns {
        reordered_values.push(values_by_column.remove(&column.name)?);
    }

    Some(reordered_values)
}

fn merge_permissions_into_schema(
    schema: &Schema,
    permissions: &HashMap<TableName, TablePolicies>,
) -> Schema {
    schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut merged = table_schema.clone();
            if let Some(table_policies) = permissions.get(table_name) {
                merged.policies = table_policies.clone();
            } else {
                merged.policies = TablePolicies::default();
            }
            (*table_name, merged)
        })
        .collect()
}

fn strip_schema_policies(schema: &Schema) -> Schema {
    schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut structural = table_schema.clone();
            structural.policies = TablePolicies::default();
            (*table_name, structural)
        })
        .collect()
}
fn hash_len_prefixed(hasher: &mut Hasher, bytes: &[u8]) {
    hasher.update(&(bytes.len() as u64).to_le_bytes());
    hasher.update(bytes);
}

/// Parse a hex-encoded SchemaHash string.
fn parse_schema_hash(hex_str: &str) -> Result<SchemaHash, SchemaError> {
    let bytes = hex::decode(hex_str)
        .map_err(|_| SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])))?;
    if bytes.len() != 32 {
        return Err(SchemaError::SchemaNotFound(SchemaHash::from_bytes([0; 32])));
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&bytes);
    Ok(SchemaHash::from_bytes(arr))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_manager::policy::PolicyExpr;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, SchemaBuilder, SchemaHash, TableName,
        TablePolicies, TableSchema,
    };

    fn test_app_id() -> AppId {
        AppId::from_name("test-app")
    }

    fn make_schema_v1() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build()
    }

    fn make_schema_v2() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text),
            )
            .build()
    }

    fn make_schema_with_insert_defaults() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("todos"),
            TableSchema::new(RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("done", ColumnType::Boolean).default(Value::Boolean(false)),
                ColumnDescriptor::new("note", ColumnType::Text)
                    .nullable()
                    .default(Value::Text("from default".into())),
            ])),
        );
        schema
    }

    #[test]
    fn schema_manager_new() {
        let schema = make_schema_v1();
        let manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        assert_eq!(manager.env(), "dev");
        assert_eq!(manager.user_branch(), "main");
        assert_eq!(manager.app_id(), test_app_id());
    }

    #[test]
    fn schema_manager_new_preserves_declared_table_column_order() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("name", ColumnType::Text)
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        let descriptor = manager.current_schema().get(&"users".into()).unwrap();
        let column_names: Vec<_> = descriptor
            .columns
            .columns
            .iter()
            .map(|column| column.name_str())
            .collect();

        assert_eq!(column_names, vec!["name", "id", "email"]);
    }

    #[test]
    fn schema_manager_new_hashes_reordered_schemas_differently() {
        let schema_a = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("name", ColumnType::Text)
                    .column("id", ColumnType::Uuid)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();
        let schema_b = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .nullable_column("email", ColumnType::Text)
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let manager_a =
            SchemaManager::new(SyncManager::new(), schema_a, test_app_id(), "dev", "main").unwrap();
        let manager_b =
            SchemaManager::new(SyncManager::new(), schema_b, test_app_id(), "dev", "main").unwrap();

        assert_ne!(manager_a.current_hash(), manager_b.current_hash());
    }

    #[test]
    fn schema_manager_branch_name() {
        let schema = make_schema_v1();
        let manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "prod", "feature")
                .unwrap();

        let branch = manager.branch_name();
        let s = branch.as_str();

        assert!(s.starts_with("prod-"));
        assert!(s.ends_with("-feature"));
    }

    #[test]
    fn schema_manager_add_live_schema() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        let lens = manager.add_live_schema(v1).unwrap();

        assert!(!lens.is_draft());
        assert_eq!(manager.all_branches().len(), 2);
    }

    #[test]
    fn schema_manager_add_live_schema_draft_fails() {
        let v1 = make_schema_v1();
        // Add non-nullable UUID column - creates draft lens
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("org_id", ColumnType::Uuid), // non-nullable UUID = draft
            )
            .build();

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        let result = manager.add_live_schema(v1);

        assert!(matches!(result, Err(SchemaError::DraftLensInPath { .. })));
    }

    #[test]
    fn schema_manager_explicit_lens() {
        use crate::schema_manager::lens::{LensOp, LensTransform};

        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // Create explicit lens
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::AddColumn {
                table: "users".into(),
                column: "email".into(),
                column_type: ColumnType::Text,
                default: crate::query_manager::types::Value::Null,
            },
            false, // not draft
        );
        let lens = Lens::new(v1_hash, v2_hash, transform);

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema_with_lens(v1, lens).unwrap();

        assert_eq!(manager.all_branches().len(), 2);
    }

    #[test]
    fn schema_manager_validate() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        // Should pass - no draft lenses
        assert!(manager.validate().is_ok());
    }

    #[test]
    fn schema_manager_lens_path() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        let path = manager.lens_path(&v1_hash).unwrap();
        assert_eq!(path.len(), 1);
    }

    #[test]
    fn schema_manager_generate_lens_without_register() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let manager =
            SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main")
                .unwrap();
        let lens = manager.generate_lens(&v1, &v2);

        // Generated but not registered
        assert!(!lens.is_draft());
        assert_eq!(manager.all_branches().len(), 1); // Only current
    }

    #[test]
    fn schema_manager_branch_schema_map() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        let map = manager.branch_schema_map();
        assert_eq!(map.len(), 2);

        // Should contain both schema hashes
        let hashes: std::collections::HashSet<_> = map.values().collect();
        assert!(hashes.contains(&v1_hash));
        assert!(hashes.contains(&v2_hash));
    }

    #[test]
    fn schema_manager_schema_published_at_uses_latest_catalogue_commit_timestamp() {
        let schema = make_schema_v1();
        let schema_hash = SchemaHash::compute(&schema);
        let mut storage = crate::storage::MemoryStorage::new();
        let mut manager = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();

        assert_eq!(manager.schema_published_at(&schema_hash), None);

        manager.persist_schema_object(&mut storage, &schema);
        let first_timestamp = manager
            .schema_published_at(&schema_hash)
            .expect("schema should expose publish timestamp after first persist");

        manager.persist_schema_object(&mut storage, &schema);
        let second_timestamp = manager
            .schema_published_at(&schema_hash)
            .expect("schema should expose publish timestamp after republish");

        assert!(
            second_timestamp > first_timestamp,
            "republishing the same schema should advance the visible publish timestamp"
        );
    }

    #[test]
    fn schema_manager_all_branch_strings() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        let branches = manager.all_branch_strings();
        assert_eq!(branches.len(), 2);

        // All should have correct format
        for branch in &branches {
            assert!(branch.starts_with("dev-"));
            assert!(branch.ends_with("-main"));
        }
    }

    #[test]
    fn schema_manager_get_table_descriptor() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        // V1 has 2 columns (id, name)
        let v1_desc = manager.get_table_descriptor("users", &v1_hash).unwrap();
        assert_eq!(v1_desc.columns.len(), 2);

        // V2 has 3 columns (id, name, email)
        let v2_desc = manager.get_table_descriptor("users", &v2_hash).unwrap();
        assert_eq!(v2_desc.columns.len(), 3);
    }

    #[test]
    fn server_dynamic_mode_does_not_persist_empty_placeholder_schema() {
        // Regression: RuntimeCore::new calls ensure_current_schema_persisted on
        // every runtime construction. On a dynamic-schema server built with
        // SchemaManager::new_server(...), the context has an uninitialized
        // sentinel hash ([0; 32]) and an empty Schema. Persisting that writes
        // a bogus catalogue_schema row whose content hashes to BLAKE3("") =
        // af1349b9f5f9..., which later appears as an "unreachable schema hash"
        // in every connection diagnostics call.
        let mut storage = crate::storage::MemoryStorage::new();
        let mut manager = SchemaManager::new_server(SyncManager::new(), test_app_id(), "prod");
        let wrote = manager.ensure_current_schema_persisted(&mut storage);
        assert!(
            !wrote,
            "dynamic server with no current schema must not persist a placeholder entry"
        );
        let entries = storage.scan_catalogue_entries().unwrap();
        assert!(
            entries.is_empty(),
            "no catalogue entries should be written for an uninitialized schema context, got: {:?}",
            entries.iter().map(|e| &e.metadata).collect::<Vec<_>>()
        );
    }

    #[test]
    fn process_catalogue_schema_ignores_empty_schema_rows_from_legacy_bug() {
        // Defensive: sqlite files written by a pre-fix server may contain a
        // bogus catalogue_schema row whose content encodes an empty Schema
        // (the uninitialized sentinel). On rehydrate that empty schema would
        // hash to BLAKE3("") = af1349b9f5f9... and surface as an unreachable
        // hash in every client's diagnostics. Ignore empty schemas.
        let schema = make_schema_v1();
        let real_hash = SchemaHash::compute(&schema);
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        let empty_content = crate::schema_manager::encoding::encode_schema(&Schema::new());
        let mut metadata = HashMap::new();
        metadata.insert(
            crate::metadata::MetadataKey::Type.to_string(),
            crate::metadata::ObjectType::CatalogueSchema.to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::AppId.to_string(),
            test_app_id().uuid().to_string(),
        );
        metadata.insert(
            crate::metadata::MetadataKey::SchemaHash.to_string(),
            SchemaHash::from_bytes([0; 32]).to_string(),
        );
        let sentinel_object_id = SchemaHash::from_bytes([0; 32]).to_object_id();

        manager
            .process_catalogue_update(sentinel_object_id, &metadata, &empty_content)
            .unwrap();

        let empty_hash = SchemaHash::compute(&Schema::new());
        let known: std::collections::HashSet<_> =
            manager.known_schema_hashes().into_iter().collect();
        assert!(
            !known.contains(&empty_hash),
            "empty-schema hash must not be registered in known_schemas"
        );
        assert!(known.contains(&real_hash));
    }

    #[test]
    fn connection_schema_diagnostics_treat_unknown_client_schema_as_disconnected() {
        let schema = make_schema_v2();
        let current_hash = SchemaHash::compute(&schema);
        let manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        let diagnostics = manager.connection_schema_diagnostics(SchemaHash::from_bytes([7; 32]));

        assert_eq!(
            diagnostics.client_schema_hash,
            SchemaHash::from_bytes([7; 32])
        );
        assert_eq!(
            diagnostics.disconnected_permissions_schema_hash,
            Some(current_hash)
        );
        assert!(diagnostics.unreachable_schema_hashes.is_empty());
    }

    #[test]
    fn connection_schema_diagnostics_reports_other_unreachable_server_schemas() {
        let v1 = make_schema_v1();
        let v2 = make_schema_v2();
        let v3 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text)
                    .nullable_column("nickname", ColumnType::Text),
            )
            .build();
        let v1_hash = SchemaHash::compute(&v1);
        let v3_hash = SchemaHash::compute(&v3);

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();
        manager.add_known_schema(v3);

        let diagnostics = manager.connection_schema_diagnostics(v1_hash);

        assert_eq!(diagnostics.disconnected_permissions_schema_hash, None);
        assert_eq!(diagnostics.unreachable_schema_hashes, vec![v3_hash]);
    }

    #[test]
    fn connection_schema_diagnostics_ignore_draft_lenses() {
        let v1 = make_schema_v1();
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("org_id", ColumnType::Uuid),
            )
            .build();
        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);
        let draft_lens = generate_lens(&v1, &v2);

        assert!(draft_lens.is_draft());

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_known_schema(v1);
        manager.context.register_lens(draft_lens);

        let diagnostics = manager.connection_schema_diagnostics(v1_hash);

        assert_eq!(
            diagnostics.disconnected_permissions_schema_hash,
            Some(v2_hash)
        );
        assert!(diagnostics.unreachable_schema_hashes.is_empty());
    }

    #[test]
    fn schema_hash_connectivity_requires_non_draft_uploaded_lenses() {
        let v1 = make_schema_v1();
        let v1_hash = SchemaHash::compute(&v1);
        let draft_target = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .column("org_id", ColumnType::Uuid),
            )
            .build();
        let draft_target_hash = SchemaHash::compute(&draft_target);
        let draft_lens = generate_lens(&v1, &draft_target);

        assert!(draft_lens.is_draft());

        let mut disconnected = SchemaManager::new(
            SyncManager::new(),
            draft_target,
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();
        disconnected.add_known_schema(v1.clone());
        disconnected.context.register_lens(draft_lens);
        assert!(!disconnected.are_schema_hashes_connected(v1_hash, draft_target_hash));

        let live_target = make_schema_v2();
        let live_target_hash = SchemaHash::compute(&live_target);
        let mut connected = SchemaManager::new(
            SyncManager::new(),
            live_target,
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();
        connected.add_live_schema(v1).unwrap();
        assert!(connected.are_schema_hashes_connected(v1_hash, live_target_hash));
    }

    #[test]
    fn permissions_head_waits_for_bundle_then_applies() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
        let permissions = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);
        let bundle = PermissionsBundleState {
            schema_hash,
            version: 3,
            parent_bundle_object_id: Some(ObjectId::new()),
            permissions: permissions.clone(),
        };
        let bundle_object_id = manager.permissions_bundle_object_id(&bundle);
        let head = PermissionsHeadState {
            schema_hash,
            version: bundle.version,
            parent_bundle_object_id: bundle.parent_bundle_object_id,
            bundle_object_id,
        };

        manager
            .process_catalogue_update(
                manager.permissions_head_object_id(),
                &manager.permissions_head_metadata(),
                &encode_permissions_head(
                    schema_hash,
                    bundle.version,
                    bundle.parent_bundle_object_id,
                    bundle_object_id,
                ),
            )
            .expect("head should process");
        assert_eq!(manager.current_permissions_head, Some(head));
        assert_eq!(manager.pending_permissions_head, Some(head));

        manager
            .process_catalogue_update(
                bundle_object_id,
                &manager.permissions_bundle_metadata(),
                &encode_permissions_bundle(
                    schema_hash,
                    bundle.version,
                    bundle.parent_bundle_object_id,
                    &permissions,
                ),
            )
            .expect("bundle should process");

        assert_eq!(manager.current_permissions_head, Some(head));
        assert_eq!(manager.pending_permissions_head, None);
        assert_eq!(
            manager
                .known_permissions_bundles
                .get(&bundle_object_id)
                .map(|state| state.permissions.clone()),
            Some(permissions)
        );
    }

    #[test]
    fn repersisting_rehydrated_permissions_keeps_unchanged_entries_stable() {
        let app_id = test_app_id();
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let permissions = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);

        let mut storage = crate::storage::MemoryStorage::new();
        let mut previous_run =
            SchemaManager::new(SyncManager::new(), schema.clone(), app_id, "dev", "main").unwrap();
        previous_run.persist_schema(&mut storage);
        previous_run
            .publish_permissions_bundle(&mut storage, schema_hash, permissions, None)
            .expect("previous run should publish permissions");
        previous_run.process(&mut storage);

        let head_object_id = SchemaManager::permissions_head_object_id_for(app_id);
        let head_entry_before = storage
            .load_catalogue_entry(head_object_id)
            .expect("head entry should load")
            .expect("head entry should exist");
        let bundle_entry_before = storage
            .load_catalogue_entry(
                previous_run
                    .current_permissions_head
                    .expect("permissions head should exist")
                    .bundle_object_id,
            )
            .expect("bundle entry should load")
            .expect("bundle entry should exist");

        let mut restarted =
            SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
        crate::schema_manager::rehydrate_schema_manager_from_catalogue(
            &mut restarted,
            &storage,
            app_id,
        )
        .expect("rehydrate should succeed");

        let republished_head_object_id = restarted
            .persist_current_permissions(&mut storage)
            .expect("rehydrated permissions should republish");
        assert_eq!(republished_head_object_id, head_object_id);

        let head_entry_after = storage
            .load_catalogue_entry(head_object_id)
            .expect("head entry should load after materialize")
            .expect("head entry should still exist");
        let bundle_entry_after = storage
            .load_catalogue_entry(
                restarted
                    .current_permissions_head
                    .expect("rehydrated permissions head should exist")
                    .bundle_object_id,
            )
            .expect("bundle entry should load after materialize")
            .expect("bundle entry should still exist");
        assert_eq!(
            head_entry_after, head_entry_before,
            "materializing unchanged permissions head should not rewrite the stored head entry"
        );
        assert_eq!(
            bundle_entry_after, bundle_entry_before,
            "materializing unchanged permissions head should not rewrite the stored bundle entry"
        );
    }

    #[test]
    fn publish_permissions_bundle_rejects_stale_parent() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
        let mut storage = crate::storage::MemoryStorage::new();
        let permissions = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);

        manager
            .publish_permissions_bundle(&mut storage, schema_hash, permissions.clone(), None)
            .expect("initial permissions publish should succeed");

        let stale =
            manager.publish_permissions_bundle(&mut storage, schema_hash, permissions, None);
        assert!(matches!(
            stale,
            Err(SchemaError::StalePermissionsParent {
                expected: None,
                current: Some(_),
            })
        ));
    }

    #[test]
    fn republishing_identical_permissions_is_a_no_op() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
        let mut storage = crate::storage::MemoryStorage::new();
        let permissions = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);

        manager
            .publish_permissions_bundle(&mut storage, schema_hash, permissions.clone(), None)
            .expect("initial permissions publish should succeed");
        manager.process(&mut storage);

        let head_before = manager
            .current_permissions_head()
            .expect("head should exist after first publish");
        let bundle_entry_before = storage
            .load_catalogue_entry(head_before.bundle_object_id)
            .expect("bundle entry should load")
            .expect("bundle entry should exist");
        let head_object_id = SchemaManager::permissions_head_object_id_for(test_app_id());
        let head_entry_before = storage
            .load_catalogue_entry(head_object_id)
            .expect("head entry should load")
            .expect("head entry should exist");

        let republished = manager
            .publish_permissions_bundle(
                &mut storage,
                schema_hash,
                permissions,
                Some(head_before.bundle_object_id),
            )
            .expect("republishing identical permissions should succeed");

        assert_eq!(
            republished,
            Some(head_object_id),
            "republish should return the existing head object id"
        );

        let head_after = manager
            .current_permissions_head()
            .expect("head should still exist after no-op publish");
        assert_eq!(
            head_after.version, head_before.version,
            "no-op publish must not bump the version"
        );
        assert_eq!(
            head_after.bundle_object_id, head_before.bundle_object_id,
            "no-op publish must not produce a new bundle object id"
        );
        assert_eq!(
            head_after.parent_bundle_object_id, head_before.parent_bundle_object_id,
            "no-op publish must not change the parent link"
        );

        let bundle_entry_after = storage
            .load_catalogue_entry(head_after.bundle_object_id)
            .expect("bundle entry should load after no-op publish")
            .expect("bundle entry should still exist");
        let head_entry_after = storage
            .load_catalogue_entry(head_object_id)
            .expect("head entry should load after no-op publish")
            .expect("head entry should still exist");
        assert_eq!(
            bundle_entry_after, bundle_entry_before,
            "no-op publish must not rewrite the stored bundle entry"
        );
        assert_eq!(
            head_entry_after, head_entry_before,
            "no-op publish must not rewrite the stored head entry"
        );
    }

    #[test]
    fn republishing_changed_permissions_bumps_version() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
        let mut storage = crate::storage::MemoryStorage::new();
        let permissive = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);
        let restrictive = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::False),
        )]);

        manager
            .publish_permissions_bundle(&mut storage, schema_hash, permissive, None)
            .expect("initial permissions publish should succeed");
        let head_before = manager
            .current_permissions_head()
            .expect("head should exist after first publish");

        manager
            .publish_permissions_bundle(
                &mut storage,
                schema_hash,
                restrictive,
                Some(head_before.bundle_object_id),
            )
            .expect("changed publish should succeed");
        let head_after = manager
            .current_permissions_head()
            .expect("head should exist after changed publish");

        assert_eq!(
            head_after.version,
            head_before.version + 1,
            "changed permissions must bump the version"
        );
        assert_ne!(
            head_after.bundle_object_id, head_before.bundle_object_id,
            "changed permissions must produce a new bundle object id"
        );
        assert_eq!(
            head_after.parent_bundle_object_id,
            Some(head_before.bundle_object_id),
            "changed permissions must chain off the previous bundle"
        );
    }

    #[test]
    fn dedup_preserves_parent_chain_for_subsequent_change() {
        let schema = make_schema_v2();
        let schema_hash = SchemaHash::compute(&schema);
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();
        let mut storage = crate::storage::MemoryStorage::new();
        let permissive = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::True),
        )]);
        let restrictive = HashMap::from([(
            TableName::new("users"),
            TablePolicies::new().with_select(PolicyExpr::False),
        )]);

        manager
            .publish_permissions_bundle(&mut storage, schema_hash, permissive.clone(), None)
            .expect("initial publish should succeed");
        let head_a = manager
            .current_permissions_head()
            .expect("head should exist after publish A");

        manager
            .publish_permissions_bundle(
                &mut storage,
                schema_hash,
                permissive,
                Some(head_a.bundle_object_id),
            )
            .expect("identical republish should succeed");
        let head_a_again = manager
            .current_permissions_head()
            .expect("head should exist after no-op republish");
        assert_eq!(
            head_a_again, head_a,
            "no-op republish must leave the head untouched"
        );

        manager
            .publish_permissions_bundle(
                &mut storage,
                schema_hash,
                restrictive,
                Some(head_a.bundle_object_id),
            )
            .expect("subsequent changed publish should succeed");
        let head_b = manager
            .current_permissions_head()
            .expect("head should exist after publish B");

        assert_eq!(
            head_b.version,
            head_a.version + 1,
            "version must advance by one across no-op + change, not two"
        );
        assert_eq!(
            head_b.parent_bundle_object_id,
            Some(head_a.bundle_object_id),
            "B must chain off A, not off the skipped no-op"
        );
    }

    #[test]
    fn schema_manager_translate_column() {
        use crate::schema_manager::lens::{LensOp, LensTransform};

        // Create schemas where a column was renamed
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // Create explicit rename lens
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
            false,
        );
        let lens = Lens::new(v1_hash, v2_hash, transform);

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema_with_lens(v1, lens).unwrap();

        // Current schema uses "email_address"
        // For v1 index, we need "email"
        let translated = manager
            .translate_column_for_schema("users", "email_address", &v1_hash)
            .unwrap();
        assert_eq!(translated, "email");

        // For v2 (current), no translation needed
        let current = manager
            .translate_column_for_schema("users", "email_address", &v2_hash)
            .unwrap();
        assert_eq!(current, "email_address");
    }

    #[test]
    fn schema_manager_insert_and_query() {
        use crate::object::ObjectId;
        use crate::storage::MemoryStorage;

        let schema = make_schema_v2();
        let mut storage = MemoryStorage::new();
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        // Insert a row
        let id = ObjectId::new();
        let id_val = Value::Uuid(id);
        let name = Value::Text("Alice".into());
        let email = Value::Text("alice@example.com".into());

        let descriptor = manager
            .current_schema()
            .get(&"users".into())
            .unwrap()
            .columns
            .clone();
        let values = HashMap::from([
            ("id".to_string(), id_val.clone()),
            ("name".to_string(), name.clone()),
            ("email".to_string(), email.clone()),
        ]);

        let _handle = manager.insert(&mut storage, "users", values).unwrap();
        manager.process(&mut storage);

        // Query via subscribe/process/unsubscribe pattern
        let query = manager.query("users").build();
        let qm = manager.query_manager_mut();
        let sub_id = qm.subscribe(query).unwrap();
        qm.process(&mut storage);
        let results = qm.get_subscription_results(sub_id);
        qm.unsubscribe_with_sync(sub_id);

        assert_eq!(results.len(), 1);
        let id_idx = descriptor.column_index("id").unwrap();
        assert_eq!(results[0].1[id_idx], id_val);
    }

    #[test]
    fn schema_manager_insert_materializes_schema_defaults() {
        use crate::storage::MemoryStorage;

        let schema = make_schema_with_insert_defaults();
        let mut storage = MemoryStorage::new();
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        let handle = manager
            .insert(
                &mut storage,
                "todos",
                HashMap::from([("title".to_string(), Value::Text("ship default".into()))]),
            )
            .unwrap();

        manager.process(&mut storage);
        let stored = manager
            .query_manager_mut()
            .get_row(&storage, handle.row_id)
            .expect("inserted row should be readable")
            .1;

        let descriptor = &manager.current_schema()[&TableName::new("todos")].columns;
        let done_idx = descriptor.column_index("done").unwrap();
        let note_idx = descriptor.column_index("note").unwrap();
        let title_idx = descriptor.column_index("title").unwrap();

        assert_eq!(stored[title_idx], Value::Text("ship default".into()));
        assert_eq!(stored[done_idx], Value::Boolean(false));
        assert_eq!(stored[note_idx], Value::Text("from default".into()));
    }

    #[test]
    fn schema_manager_insert_explicit_null_bypasses_default_for_nullable_column() {
        use crate::storage::MemoryStorage;

        let schema = make_schema_with_insert_defaults();
        let mut storage = MemoryStorage::new();
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        let handle = manager
            .insert(
                &mut storage,
                "todos",
                HashMap::from([
                    ("title".to_string(), Value::Text("keep null".into())),
                    ("note".to_string(), Value::Null),
                ]),
            )
            .unwrap();

        let descriptor = &manager.current_schema()[&TableName::new("todos")].columns;
        let note_idx = descriptor.column_index("note").unwrap();
        let done_idx = descriptor.column_index("done").unwrap();

        assert_eq!(handle.row_values[note_idx], Value::Null);
        assert_eq!(handle.row_values[done_idx], Value::Boolean(false));
    }

    #[test]
    fn schema_manager_insert_missing_required_non_defaulted_field_errors() {
        use crate::storage::MemoryStorage;

        let schema = make_schema_with_insert_defaults();
        let mut storage = MemoryStorage::new();
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        let err = manager
            .insert(&mut storage, "todos", HashMap::new())
            .unwrap_err();
        assert!(
            matches!(err, QueryError::EncodingError(ref msg) if msg.contains("missing required field `title`")),
            "expected missing required field error, got {err:?}"
        );
    }

    #[test]
    fn schema_manager_insert_explicit_null_for_required_column_errors() {
        use crate::storage::MemoryStorage;

        let schema = make_schema_with_insert_defaults();
        let mut storage = MemoryStorage::new();
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        let err = manager
            .insert(
                &mut storage,
                "todos",
                HashMap::from([("title".to_string(), Value::Null)]),
            )
            .unwrap_err();
        assert!(
            matches!(err, QueryError::EncodingError(ref msg) if msg.contains("cannot set required field `title` to null")),
            "expected nullability error, got {err:?}"
        );
    }

    #[test]
    fn schema_manager_insert_unknown_column_errors() {
        use crate::storage::MemoryStorage;

        let schema = make_schema_with_insert_defaults();
        let mut storage = MemoryStorage::new();
        let mut manager =
            SchemaManager::new(SyncManager::new(), schema, test_app_id(), "dev", "main").unwrap();

        let err = manager
            .insert(
                &mut storage,
                "todos",
                HashMap::from([
                    ("title".to_string(), Value::Text("first".into())),
                    ("bogus".to_string(), Value::Text("second".into())),
                ]),
            )
            .unwrap_err();
        assert!(
            matches!(err, QueryError::EncodingError(ref msg) if msg.contains("unknown column `bogus`")),
            "expected unknown column error, got {err:?}"
        );
    }
}
