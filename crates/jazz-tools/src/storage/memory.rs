//! In-memory `Storage` implementation used by tests and the main thread.
//!
//! The trailing `mod tests` block primarily exercises `MemoryStorage` against
//! the trait surface, so the unit tests live alongside the implementation.

use super::*;

// ============================================================================
// MemoryStorage - In-memory implementation for testing and main thread
// ============================================================================

/// Ordered raw-table rows keyed by their local storage key.
type RawTableEntries = BTreeMap<String, Vec<u8>>;

#[derive(Debug, Clone, Default)]
struct TableRowHistories {
    visible: BTreeMap<SharedString, BTreeMap<ObjectId, VisibleRowEntry>>,
    history: BTreeMap<ObjectId, BTreeMap<(SharedString, BatchId), StoredRowBatch>>,
}

impl TableRowHistories {
    fn history_rows_for(&self, branch: &str, row_id: ObjectId) -> Vec<StoredRowBatch> {
        let mut rows: Vec<_> = self
            .history
            .get(&row_id)
            .into_iter()
            .flat_map(|inner| inner.iter())
            .filter(|((history_branch, _), _)| history_branch.as_str() == branch)
            .map(|(_, row)| row.clone())
            .collect();
        rows.sort_by_key(|row| (row.updated_at, row.batch_id()));
        rows
    }
}

/// In-memory Storage for testing and main-thread use.
///
/// Stores objects and raw tables in HashMaps/BTreeMaps. No persistence.
/// This is sufficient for:
/// - All jazz unit tests
/// - All jazz integration tests
/// - Main thread in browser (acts as cache of worker state)
pub struct MemoryStorage {
    cache_namespace: usize,
    /// Ordered raw-table storage.
    raw_tables: HashMap<String, RawTableEntries>,
    /// Raw table headers already validated/inserted in this storage instance.
    ensured_raw_table_headers: HashSet<String>,
    /// Decoded row locators keyed by logical row id.
    row_locators: HashMap<ObjectId, RowLocator>,
    /// Row-history storage keyed by table.
    row_histories: HashMap<String, TableRowHistories>,
    /// Raw encoded row-history bytes keyed by table, row id, branch, and batch id.
    row_history_bytes: HashMap<String, EncodedTableRowHistories>,
}

impl MemoryStorage {
    /// Create a new empty MemoryStorage.
    pub fn new() -> Self {
        Self::default()
    }

    fn ensure_cached_raw_table_header(
        &mut self,
        raw_table: &str,
        expected_header: &RawTableHeader,
    ) -> Result<(), StorageError> {
        if self.ensured_raw_table_headers.contains(raw_table) {
            return Ok(());
        }
        ensure_raw_table_header(self, raw_table, expected_header)?;
        self.ensured_raw_table_headers.insert(raw_table.to_string());
        Ok(())
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self {
            cache_namespace: next_storage_cache_namespace(),
            raw_tables: HashMap::new(),
            ensured_raw_table_headers: HashSet::new(),
            row_locators: HashMap::new(),
            row_histories: HashMap::new(),
            row_history_bytes: HashMap::new(),
        }
    }
}

impl Storage for MemoryStorage {
    fn storage_cache_namespace(&self) -> usize {
        self.cache_namespace
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowBatch],
        visible_entries: &[VisibleRowEntry],
        encoded_history_rows: &[OwnedHistoryRowBytes],
        encoded_visible_rows: &[OwnedVisibleRowBytes],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        if history_rows.len() != encoded_history_rows.len() {
            return Err(StorageError::IoError(format!(
                "prepared history row count mismatch: {} decoded vs {} encoded",
                history_rows.len(),
                encoded_history_rows.len()
            )));
        }
        if visible_entries.len() != encoded_visible_rows.len() {
            return Err(StorageError::IoError(format!(
                "prepared visible row count mismatch: {} decoded vs {} encoded",
                visible_entries.len(),
                encoded_visible_rows.len()
            )));
        }

        let table = table.to_string();

        for (row, encoded) in history_rows.iter().zip(encoded_history_rows) {
            self.ensure_cached_raw_table_header(
                encoded.row_raw_table.as_str(),
                &row_raw_table_header(&encoded.row_raw_table_id, &encoded.user_descriptor),
            )?;
            if encoded.needs_exact_locator {
                self.put_history_row_batch_table_locator(
                    encoded.branch.as_str(),
                    encoded.row_id,
                    encoded.batch_id,
                    Some(&ExactRowTableLocator {
                        row_raw_table: encoded.row_raw_table.clone().into(),
                        table_name: encoded.row_raw_table_id.table_name.clone(),
                        schema_hash: encoded.row_raw_table_id.schema_hash,
                    }),
                )?;
            }
            self.row_histories
                .entry(table.clone())
                .or_default()
                .history
                .entry(row.row_id)
                .or_default()
                .insert((row.branch.clone(), row.batch_id()), row.clone());
            self.row_history_bytes
                .entry(table.clone())
                .or_default()
                .entry(encoded.row_id)
                .or_default()
                .insert(
                    (encoded.branch.clone().into(), encoded.batch_id),
                    encoded.bytes.clone(),
                );
        }

        for (entry, encoded) in visible_entries.iter().zip(encoded_visible_rows) {
            self.ensure_cached_raw_table_header(
                encoded.row_raw_table.as_str(),
                &row_raw_table_header(&encoded.row_raw_table_id, &encoded.user_descriptor),
            )?;
            if encoded.needs_exact_locator {
                self.put_visible_row_table_locator(
                    encoded.branch.as_str(),
                    encoded.row_id,
                    Some(&ExactRowTableLocator {
                        row_raw_table: encoded.row_raw_table.clone().into(),
                        table_name: encoded.row_raw_table_id.table_name.clone(),
                        schema_hash: encoded.row_raw_table_id.schema_hash,
                    }),
                )?;
            }
            self.row_histories
                .entry(table.clone())
                .or_default()
                .visible
                .entry(entry.current_row.branch.clone())
                .or_default()
                .insert(entry.current_row.row_id, entry.clone());
        }

        if !index_mutations.is_empty() {
            self.apply_index_mutations(index_mutations)?;
        }
        Ok(())
    }

    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[OwnedHistoryRowBytes],
        visible_rows: &[OwnedVisibleRowBytes],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        let table = table.to_string();

        for row in history_rows {
            self.ensure_cached_raw_table_header(
                row.row_raw_table.as_str(),
                &row_raw_table_header(&row.row_raw_table_id, &row.user_descriptor),
            )?;
            if row.needs_exact_locator {
                self.put_history_row_batch_table_locator(
                    row.branch.as_str(),
                    row.row_id,
                    row.batch_id,
                    Some(&ExactRowTableLocator {
                        row_raw_table: row.row_raw_table.clone().into(),
                        table_name: row.row_raw_table_id.table_name.clone(),
                        schema_hash: row.row_raw_table_id.schema_hash,
                    }),
                )?;
            }
            let decoded = decode_history_row_bytes_in_table(
                &ResolvedRowTable {
                    row_raw_table: row.row_raw_table.clone(),
                    user_descriptor: row.user_descriptor.clone(),
                    row_codecs: crate::row_histories::flat_row_codecs(row.user_descriptor.as_ref()),
                },
                row.row_id,
                row.branch.as_str(),
                row.batch_id,
                &row.bytes,
            )?;
            self.row_histories
                .entry(table.clone())
                .or_default()
                .history
                .entry(row.row_id)
                .or_default()
                .insert((row.branch.clone().into(), row.batch_id), decoded);
            self.row_history_bytes
                .entry(table.clone())
                .or_default()
                .entry(row.row_id)
                .or_default()
                .insert((row.branch.clone().into(), row.batch_id), row.bytes.clone());
        }

        for row in visible_rows {
            self.ensure_cached_raw_table_header(
                row.row_raw_table.as_str(),
                &row_raw_table_header(&row.row_raw_table_id, &row.user_descriptor),
            )?;
            if row.needs_exact_locator {
                self.put_visible_row_table_locator(
                    row.branch.as_str(),
                    row.row_id,
                    Some(&ExactRowTableLocator {
                        row_raw_table: row.row_raw_table.clone().into(),
                        table_name: row.row_raw_table_id.table_name.clone(),
                        schema_hash: row.row_raw_table_id.schema_hash,
                    }),
                )?;
            }
            let decoded = decode_visible_row_entry_bytes_in_table(
                &ResolvedRowTable {
                    row_raw_table: row.row_raw_table.clone(),
                    user_descriptor: row.user_descriptor.clone(),
                    row_codecs: crate::row_histories::flat_row_codecs(row.user_descriptor.as_ref()),
                },
                row.row_id,
                row.branch.as_str(),
                &row.bytes,
            )?;
            self.row_histories
                .entry(table.clone())
                .or_default()
                .visible
                .entry(row.branch.clone().into())
                .or_default()
                .insert(row.row_id, decoded);
        }

        if !index_mutations.is_empty() {
            self.apply_index_mutations(index_mutations)?;
        }
        Ok(())
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        _schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        self.patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&RowLocator>,
    ) -> Result<(), StorageError> {
        if let Some(locator) = locator {
            self.ensure_cached_raw_table_header(
                ROW_LOCATOR_TABLE,
                &RawTableHeader::system(STORAGE_KIND_ROW_LOCATOR, ROW_LOCATOR_STORAGE_FORMAT_V1),
            )?;
            let locator_bytes = encode_row_locator(locator)?;
            self.raw_tables
                .entry(ROW_LOCATOR_TABLE.to_string())
                .or_default()
                .insert(metadata_raw_key(id), locator_bytes);
            self.row_locators.insert(id, locator.clone());
        } else {
            if let Some(rows) = self.raw_tables.get_mut(ROW_LOCATOR_TABLE) {
                rows.remove(&metadata_raw_key(id));
            }
            self.row_locators.remove(&id);
        }

        Ok(())
    }

    fn load_row_locator(&self, id: ObjectId) -> Result<Option<RowLocator>, StorageError> {
        Ok(self.row_locators.get(&id).cloned())
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        self.raw_tables
            .entry(table.to_string())
            .or_default()
            .insert(key.to_string(), value.to_vec());
        Ok(())
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        if let Some(rows) = self.raw_tables.get_mut(table) {
            rows.remove(key);
        }
        Ok(())
    }

    fn apply_raw_table_mutations(
        &mut self,
        mutations: &[RawTableMutation<'_>],
    ) -> Result<(), StorageError> {
        let mut raw_tables = self.raw_tables.clone();
        for mutation in mutations {
            match mutation {
                RawTableMutation::Put { table, key, value } => {
                    raw_tables
                        .entry((*table).to_string())
                        .or_default()
                        .insert((*key).to_string(), (*value).to_vec());
                }
                RawTableMutation::Delete { table, key } => {
                    if let Some(rows) = raw_tables.get_mut(*table) {
                        rows.remove(*key);
                    }
                }
            }
        }
        self.raw_tables = raw_tables;
        Ok(())
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self
            .raw_tables
            .get(table)
            .and_then(|rows| rows.get(key))
            .cloned())
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        let Some(rows) = self.raw_tables.get(table) else {
            return Ok(Vec::new());
        };
        Ok(rows
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        let Some(rows) = self.raw_tables.get(table) else {
            return Ok(Vec::new());
        };
        Ok(rows
            .range(prefix.to_string()..)
            .take_while(|(key, _)| key.starts_with(prefix))
            .map(|(key, _)| key.clone())
            .collect())
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        let Some(rows) = self.raw_tables.get(table) else {
            return Ok(Vec::new());
        };

        let start = start.map(str::to_string);
        let end = end.map(str::to_string);

        Ok(match (start.as_ref(), end.as_ref()) {
            (Some(start), Some(end)) => rows
                .range(start.clone()..end.clone())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            (Some(start), None) => rows
                .range(start.clone()..)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            (None, Some(end)) => rows
                .range(..end.clone())
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            (None, None) => rows
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        })
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        let Some(rows) = self.raw_tables.get(table) else {
            return Ok(Vec::new());
        };

        let start = start.map(str::to_string);
        let end = end.map(str::to_string);

        Ok(match (start.as_ref(), end.as_ref()) {
            (Some(start), Some(end)) => rows
                .range(start.clone()..end.clone())
                .map(|(key, _)| key.clone())
                .collect(),
            (Some(start), None) => rows
                .range(start.clone()..)
                .map(|(key, _)| key.clone())
                .collect(),
            (None, Some(end)) => rows
                .range(..end.clone())
                .map(|(key, _)| key.clone())
                .collect(),
            (None, None) => rows.keys().cloned().collect(),
        })
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowBatch],
    ) -> Result<(), StorageError> {
        let encoded_rows = encode_history_row_bytes_for_storage(self, table, rows)?;
        {
            let regions = self.row_histories.entry(table.to_string()).or_default();
            for row in rows {
                regions
                    .history
                    .entry(row.row_id)
                    .or_default()
                    .insert((row.branch.clone(), row.batch_id()), row.clone());
            }
        }
        for row in &encoded_rows {
            self.ensure_cached_raw_table_header(
                row.row_raw_table.as_str(),
                &row_raw_table_header(&row.row_raw_table_id, &row.user_descriptor),
            )?;
            if row.needs_exact_locator {
                self.put_history_row_batch_table_locator(
                    row.branch.as_str(),
                    row.row_id,
                    row.batch_id,
                    Some(&ExactRowTableLocator {
                        row_raw_table: row.row_raw_table.clone().into(),
                        table_name: row.row_raw_table_id.table_name.clone(),
                        schema_hash: row.row_raw_table_id.schema_hash,
                    }),
                )?;
            }
        }
        let raw_regions = self.row_history_bytes.entry(table.to_string()).or_default();
        for row in encoded_rows {
            raw_regions
                .entry(row.row_id)
                .or_default()
                .insert((row.branch.clone().into(), row.batch_id), row.bytes);
        }
        Ok(())
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        let regions = self.row_history_bytes.entry(table.to_string()).or_default();
        for row in rows {
            regions.entry(row.row_id).or_default().insert(
                (row.branch.to_string().into(), row.batch_id),
                row.bytes.to_vec(),
            );
        }
        Ok(())
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        let encoded_rows = encode_visible_row_bytes_for_storage(self, table, entries)?;
        {
            let regions = self.row_histories.entry(table.to_string()).or_default();
            for entry in entries {
                regions
                    .visible
                    .entry(entry.current_row.branch.clone())
                    .or_default()
                    .insert(entry.current_row.row_id, entry.clone());
            }
        }
        for row in encoded_rows {
            self.ensure_cached_raw_table_header(
                row.row_raw_table.as_str(),
                &row_raw_table_header(&row.row_raw_table_id, &row.user_descriptor),
            )?;
            if row.needs_exact_locator {
                self.put_visible_row_table_locator(
                    row.branch.as_str(),
                    row.row_id,
                    Some(&ExactRowTableLocator {
                        row_raw_table: row.row_raw_table.clone().into(),
                        table_name: row.row_raw_table_id.table_name.clone(),
                        schema_hash: row.row_raw_table_id.schema_hash,
                    }),
                )?;
            }
        }
        Ok(())
    }

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let Some(regions) = self.row_histories.get_mut(table) else {
            return Ok(());
        };
        if let Some(rows) = regions.visible.get_mut(branch) {
            rows.remove(&row_id);
            if rows.is_empty() {
                regions.visible.remove(branch);
            }
        }
        self.put_visible_row_table_locator(branch, row_id, None)?;
        Ok(())
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        let mut rebuild_inputs = Vec::new();

        {
            let Some(regions) = self.row_histories.get_mut(table) else {
                return Ok(());
            };

            let mut affected_visible_rows = HashSet::new();
            for row in regions
                .history
                .values_mut()
                .flat_map(|inner| inner.values_mut())
            {
                if row.batch_id == batch_id {
                    if let Some(state) = state {
                        row.state = state;
                    }
                    row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
                        (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                        (Some(existing), None) => Some(existing),
                        (None, incoming) => incoming,
                    };
                    affected_visible_rows.insert((row.branch.clone(), row.row_id));
                }
            }

            for (branch, row_id) in affected_visible_rows {
                let history_rows = regions.history_rows_for(&branch, row_id);
                let context_row = regions
                    .visible
                    .get(branch.as_str())
                    .and_then(|rows| rows.get(&row_id))
                    .map(|entry| entry.current_row.clone())
                    .or_else(|| {
                        history_rows
                            .iter()
                            .rev()
                            .find(|row| row.state.is_visible())
                            .cloned()
                    })
                    .or_else(|| history_rows.last().cloned());
                if let Some(context_row) = context_row {
                    rebuild_inputs.push((branch, row_id, context_row, history_rows));
                }
            }
        }

        let mut rebuilt_visible_entries = Vec::new();
        let mut rows_without_visible_head = Vec::new();
        for (branch, row_id, context_row, history_rows) in rebuild_inputs {
            let context = resolve_history_row_write_context(self, table, &context_row)?;
            if let Some(entry) = VisibleRowEntry::rebuild_with_descriptor(
                context.user_descriptor.as_ref(),
                &history_rows,
            )
            .map_err(|err| StorageError::IoError(format!("rebuild visible entry: {err}")))?
            {
                rebuilt_visible_entries.push(entry);
            } else {
                rows_without_visible_head.push((branch, row_id));
            }
        }

        if !rebuilt_visible_entries.is_empty() {
            self.upsert_visible_region_rows(table, &rebuilt_visible_entries)?;
        }
        for (branch, row_id) in rows_without_visible_head {
            self.delete_visible_region_row(table, branch.as_str(), row_id)?;
        }

        Ok(())
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(Vec::new());
        };

        Ok(regions
            .visible
            .get(branch)
            .map(|rows| {
                rows.values()
                    .map(|entry| entry.current_row.clone())
                    .collect()
            })
            .unwrap_or_default())
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        Ok(self.row_histories.get(table).and_then(|regions| {
            regions
                .visible
                .get(branch)
                .and_then(|rows| rows.get(&row_id))
                .map(|entry| entry.current_row.clone())
        }))
    }

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self.row_histories.get(table).and_then(|regions| {
            regions
                .visible
                .get(branch)
                .and_then(|rows| rows.get(&row_id))
                .map(|entry| QueryRowBatch::from(&entry.current_row))
        }))
    }

    fn load_visible_region_entry(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<VisibleRowEntry>, StorageError> {
        Ok(self
            .row_histories
            .get(table)
            .and_then(|regions| regions.visible.get(branch))
            .and_then(|rows| rows.get(&row_id).cloned()))
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<BatchId>>, StorageError> {
        Ok(self.row_histories.get(table).and_then(|regions| {
            regions
                .visible
                .get(branch)
                .and_then(|rows| rows.get(&row_id))
                .map(|entry| entry.branch_frontier.clone())
        }))
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: BranchName,
    ) -> Result<Vec<CapturedFrontierMember>, StorageError> {
        let family_branches: BTreeSet<_> = self
            .row_histories
            .values()
            .flat_map(|regions| regions.visible.keys())
            .filter_map(|branch_name| {
                let branch_name = BranchName::new(branch_name.as_str());
                branch_matches_transaction_family(branch_name, target_branch_name)
                    .then_some(branch_name.as_str().to_string())
            })
            .collect();
        if family_branches.is_empty() {
            return Ok(Vec::new());
        }

        let mut frontier = Vec::new();
        for regions in self.row_histories.values() {
            for branch_name in &family_branches {
                let Some(rows) = regions.visible.get(branch_name.as_str()) else {
                    continue;
                };
                for entry in rows.values() {
                    frontier.push(CapturedFrontierMember {
                        object_id: entry.current_row.row_id,
                        branch_name: BranchName::new(branch_name),
                        batch_id: entry.current_row.batch_id(),
                    });
                }
            }
        }

        frontier.sort_by(|left, right| {
            left.object_id
                .uuid()
                .as_bytes()
                .cmp(right.object_id.uuid().as_bytes())
                .then_with(|| left.branch_name.as_str().cmp(right.branch_name.as_str()))
                .then_with(|| left.batch_id.0.cmp(&right.batch_id.0))
        });
        frontier.dedup();
        Ok(frontier)
    }

    fn load_visible_region_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(None);
        };
        let Some(entry) = regions
            .visible
            .get(branch)
            .and_then(|rows| rows.get(&row_id))
        else {
            return Ok(None);
        };
        let context = resolve_history_row_write_context(self, table, &entry.current_row)?;
        let current_tier = row_confirmed_tier_with_batch_settlement(self, &entry.current_row)?;
        if current_tier.is_some_and(|tier| tier >= required_tier) {
            let mut current_row = entry.current_row.clone();
            current_row.confirmed_tier = current_tier;
            return Ok(Some(current_row));
        }
        let mut history_rows = regions.history_rows_for(branch, row_id);
        apply_batch_settlement_tiers_to_rows(self, &mut history_rows)?;
        crate::row_histories::visible_row_preview_from_history_rows(
            context.user_descriptor.as_ref(),
            &history_rows,
            Some(required_tier),
        )
        .map_err(|err| StorageError::IoError(format!("load tiered visible preview: {err}")))
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self
            .load_visible_region_row_for_tier(table, branch, row_id, required_tier)?
            .as_ref()
            .map(QueryRowBatch::from))
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(Vec::new());
        };

        let mut rows: Vec<_> = regions
            .visible
            .values()
            .filter_map(|branch_rows| branch_rows.get(&row_id))
            .map(|entry| entry.current_row.clone())
            .collect();
        rows.sort_by_key(|row| row.branch.clone());
        Ok(rows)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(Vec::new());
        };

        let mut rows: Vec<_> = regions
            .history
            .get(&row_id)
            .into_iter()
            .flat_map(|inner| inner.values())
            .cloned()
            .collect();
        rows.sort_by_key(|row| (row.branch.clone(), row.updated_at, row.batch_id()));
        Ok(rows)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(self.row_history_bytes.get(table).and_then(|regions| {
            regions
                .get(&row_id)
                .and_then(|inner| inner.get(&(branch.to_string().into(), batch_id)))
                .cloned()
        }))
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        let Some(regions) = self.row_history_bytes.get(table) else {
            return Ok(Vec::new());
        };

        Ok(match scan {
            HistoryScan::Branch | HistoryScan::AsOf { .. } => regions
                .values()
                .flat_map(|inner| inner.values())
                .cloned()
                .collect::<Vec<_>>(),
            HistoryScan::Row { row_id } => regions
                .get(&row_id)
                .map(|inner| inner.values().cloned().collect::<Vec<_>>())
                .unwrap_or_default(),
        })
    }

    fn load_visible_region_row_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        let Some(entry) = self
            .row_histories
            .get(table)
            .and_then(|regions| regions.visible.get(branch))
            .and_then(|rows| rows.get(&row_id))
            .cloned()
        else {
            return Ok(None);
        };

        let encoded =
            encode_visible_row_bytes_for_storage(self, table, std::slice::from_ref(&entry))?;
        Ok(encoded.into_iter().next().map(|row| row.bytes))
    }

    fn scan_visible_region_bytes(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        let Some(entries) = self
            .row_histories
            .get(table)
            .and_then(|regions| regions.visible.get(branch))
        else {
            return Ok(Vec::new());
        };

        let entries = entries.values().cloned().collect::<Vec<_>>();
        Ok(encode_visible_row_bytes_for_storage(self, table, &entries)?
            .into_iter()
            .map(|row| row.bytes)
            .collect())
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        Ok(self.row_histories.get(table).and_then(|regions| {
            regions
                .history
                .get(&row_id)
                .and_then(|inner| inner.get(&(branch.to_string().into(), batch_id)))
                .cloned()
        }))
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        _schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        self.load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self
            .row_histories
            .get(table)
            .and_then(|regions| {
                regions
                    .history
                    .get(&row_id)
                    .and_then(|inner| inner.get(&(branch.to_string().into(), batch_id)))
            })
            .map(QueryRowBatch::from))
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let Some(regions) = self.row_histories.get(table) else {
            return Ok(Vec::new());
        };

        let mut rows: Vec<StoredRowBatch> = match scan {
            HistoryScan::Branch => regions
                .history
                .values()
                .flat_map(|inner| inner.values())
                .filter(|row| row.branch == branch)
                .cloned()
                .collect(),
            HistoryScan::Row { row_id } => regions
                .history
                .get(&row_id)
                .map(|inner| {
                    inner
                        .values()
                        .filter(|row| row.branch == branch)
                        .cloned()
                        .collect()
                })
                .unwrap_or_default(),
            HistoryScan::AsOf { ts } => {
                let mut latest_per_row: BTreeMap<ObjectId, StoredRowBatch> = BTreeMap::new();
                for (row_id, inner) in &regions.history {
                    for row in inner.values() {
                        if row.branch != branch || row.updated_at > ts || !row.state.is_visible() {
                            continue;
                        }
                        match latest_per_row.get(row_id) {
                            Some(existing)
                                if (existing.updated_at, existing.batch_id())
                                    >= (row.updated_at, row.batch_id()) => {}
                            _ => {
                                latest_per_row.insert(*row_id, row.clone());
                            }
                        }
                    }
                }
                latest_per_row.into_values().collect()
            }
        };

        rows.sort_by_key(|row| {
            (
                row.branch.clone(),
                row.row_id,
                row.updated_at,
                row.batch_id(),
            )
        });
        Ok(rows)
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use super::*;
    use crate::metadata::RowProvenance;
    use crate::object::BranchName;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, SchemaBuilder, SchemaHash, TableSchema,
    };
    use crate::row_format::encode_row;
    use crate::row_histories::{decode_flat_history_row, encode_flat_history_row};
    use crate::test_support::persist_test_schema;

    fn users_test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![ColumnDescriptor::new("value", ColumnType::Text)])
    }

    fn users_test_schema() -> crate::query_manager::types::Schema {
        SchemaBuilder::new()
            .table(TableSchema::builder("users").column("value", ColumnType::Text))
            .build()
    }

    fn seed_users_schema(storage: &mut MemoryStorage) -> SchemaHash {
        persist_test_schema(storage, &users_test_schema())
    }

    fn seed_users_row(storage: &mut MemoryStorage, row_id: ObjectId, schema_hash: SchemaHash) {
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();
    }

    #[test]
    fn row_locator_persistence_does_not_create_metadata_table() {
        let mut storage = MemoryStorage::new();
        let row_id = ObjectId::new();
        let schema_hash = seed_users_schema(&mut storage);

        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();

        assert!(!storage.raw_tables.contains_key("__metadata"));
    }

    fn make_users_row_batch(
        row_id: ObjectId,
        branch: &str,
        value: &str,
        provenance: RowProvenance,
        state: crate::row_histories::RowState,
        durability: Option<DurabilityTier>,
        parents: Vec<BatchId>,
    ) -> crate::row_histories::StoredRowBatch {
        crate::row_histories::StoredRowBatch::new(
            row_id,
            branch,
            parents,
            encode_row(&users_test_descriptor(), &[Value::Text(value.to_string())]).unwrap(),
            provenance,
            HashMap::new(),
            state,
            durability,
        )
    }

    struct RawTableOnlyMemoryStorage {
        inner: MemoryStorage,
    }

    impl RawTableOnlyMemoryStorage {
        fn new() -> Self {
            Self {
                inner: MemoryStorage::new(),
            }
        }
    }

    impl Storage for RawTableOnlyMemoryStorage {
        fn raw_table_put(
            &mut self,
            table: &str,
            key: &str,
            value: &[u8],
        ) -> Result<(), StorageError> {
            self.inner.raw_table_put(table, key, value)
        }

        fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
            self.inner.raw_table_delete(table, key)
        }

        fn apply_raw_table_mutations(
            &mut self,
            mutations: &[RawTableMutation<'_>],
        ) -> Result<(), StorageError> {
            self.inner.apply_raw_table_mutations(mutations)
        }

        fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
            self.inner.raw_table_get(table, key)
        }

        fn raw_table_scan_prefix(
            &self,
            table: &str,
            prefix: &str,
        ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
            self.inner.raw_table_scan_prefix(table, prefix)
        }

        fn append_history_region_row_bytes(
            &mut self,
            _table: &str,
            rows: &[HistoryRowBytes<'_>],
        ) -> Result<(), StorageError> {
            for row in rows {
                self.inner.raw_table_put(
                    row.row_raw_table,
                    &key_codec::history_row_raw_table_key(row.row_id, row.branch, row.batch_id),
                    row.bytes,
                )?;
            }
            Ok(())
        }

        fn upsert_visible_region_row_bytes(
            &mut self,
            _table: &str,
            rows: &[VisibleRowBytes<'_>],
        ) -> Result<(), StorageError> {
            for row in rows {
                self.inner.raw_table_put(
                    row.row_raw_table,
                    &key_codec::visible_row_raw_table_key(row.branch, row.row_id),
                    row.bytes,
                )?;
            }
            Ok(())
        }

        fn apply_index_mutations(
            &mut self,
            _index_mutations: &[IndexMutation<'_>],
        ) -> Result<(), StorageError> {
            Ok(())
        }
    }

    struct FailOnNthRawPutStorage {
        inner: MemoryStorage,
        fail_on_put_number: usize,
        raw_puts_seen: usize,
    }

    impl FailOnNthRawPutStorage {
        fn new(fail_on_put_number: usize) -> Self {
            Self {
                inner: MemoryStorage::new(),
                fail_on_put_number,
                raw_puts_seen: 0,
            }
        }
    }

    impl Storage for FailOnNthRawPutStorage {
        fn raw_table_put(
            &mut self,
            table: &str,
            key: &str,
            value: &[u8],
        ) -> Result<(), StorageError> {
            self.raw_puts_seen += 1;
            if self.raw_puts_seen == self.fail_on_put_number {
                return Err(StorageError::IoError(format!(
                    "simulated raw_table_put failure #{:?} for {table}:{key}",
                    self.fail_on_put_number
                )));
            }
            self.inner.raw_table_put(table, key, value)
        }

        fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
            self.inner.raw_table_delete(table, key)
        }

        fn apply_raw_table_mutations(
            &mut self,
            mutations: &[RawTableMutation<'_>],
        ) -> Result<(), StorageError> {
            let mut raw_tables = self.inner.raw_tables.clone();
            for mutation in mutations {
                self.raw_puts_seen += 1;
                if self.raw_puts_seen == self.fail_on_put_number {
                    return Err(StorageError::IoError(format!(
                        "simulated raw_table_mutation failure #{:?}",
                        self.fail_on_put_number
                    )));
                }
                match mutation {
                    RawTableMutation::Put { table, key, value } => {
                        raw_tables
                            .entry((*table).to_string())
                            .or_default()
                            .insert((*key).to_string(), (*value).to_vec());
                    }
                    RawTableMutation::Delete { table, key } => {
                        if let Some(rows) = raw_tables.get_mut(*table) {
                            rows.remove(*key);
                        }
                    }
                }
            }
            self.inner.raw_tables = raw_tables;
            Ok(())
        }

        fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
            self.inner.raw_table_get(table, key)
        }

        fn raw_table_scan_prefix(
            &self,
            table: &str,
            prefix: &str,
        ) -> Result<RawTableRows, StorageError> {
            self.inner.raw_table_scan_prefix(table, prefix)
        }

        fn raw_table_scan_range(
            &self,
            table: &str,
            start: Option<&str>,
            end: Option<&str>,
        ) -> Result<RawTableRows, StorageError> {
            self.inner.raw_table_scan_range(table, start, end)
        }
    }

    struct CountingCatalogueLoadsStorage {
        inner: MemoryStorage,
        catalogue_loads: std::cell::Cell<usize>,
        raw_table_header_scans: std::cell::Cell<usize>,
    }

    impl CountingCatalogueLoadsStorage {
        fn new() -> Self {
            Self {
                inner: MemoryStorage::new(),
                catalogue_loads: std::cell::Cell::new(0),
                raw_table_header_scans: std::cell::Cell::new(0),
            }
        }

        fn catalogue_loads(&self) -> usize {
            self.catalogue_loads.get()
        }

        fn raw_table_header_scans(&self) -> usize {
            self.raw_table_header_scans.get()
        }
    }

    impl Storage for CountingCatalogueLoadsStorage {
        fn raw_table_put(
            &mut self,
            table: &str,
            key: &str,
            value: &[u8],
        ) -> Result<(), StorageError> {
            self.inner.raw_table_put(table, key, value)
        }

        fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
            self.inner.raw_table_delete(table, key)
        }

        fn apply_raw_table_mutations(
            &mut self,
            mutations: &[RawTableMutation<'_>],
        ) -> Result<(), StorageError> {
            self.inner.apply_raw_table_mutations(mutations)
        }

        fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
            self.inner.raw_table_get(table, key)
        }

        fn raw_table_scan_prefix(
            &self,
            table: &str,
            prefix: &str,
        ) -> Result<RawTableRows, StorageError> {
            self.inner.raw_table_scan_prefix(table, prefix)
        }

        fn raw_table_scan_range(
            &self,
            table: &str,
            start: Option<&str>,
            end: Option<&str>,
        ) -> Result<RawTableRows, StorageError> {
            self.inner.raw_table_scan_range(table, start, end)
        }

        fn append_history_region_row_bytes(
            &mut self,
            table: &str,
            rows: &[HistoryRowBytes<'_>],
        ) -> Result<(), StorageError> {
            let _ = table;
            for row in rows {
                self.inner.raw_table_put(
                    row.row_raw_table,
                    &key_codec::history_row_raw_table_key(row.row_id, row.branch, row.batch_id),
                    row.bytes,
                )?;
            }
            Ok(())
        }

        fn upsert_visible_region_row_bytes(
            &mut self,
            table: &str,
            rows: &[VisibleRowBytes<'_>],
        ) -> Result<(), StorageError> {
            let _ = table;
            for row in rows {
                self.inner.raw_table_put(
                    row.row_raw_table,
                    &key_codec::visible_row_raw_table_key(row.branch, row.row_id),
                    row.bytes,
                )?;
            }
            Ok(())
        }

        fn load_catalogue_entry(
            &self,
            object_id: ObjectId,
        ) -> Result<Option<CatalogueEntry>, StorageError> {
            self.catalogue_loads.set(self.catalogue_loads.get() + 1);
            self.inner.load_catalogue_entry(object_id)
        }

        fn scan_raw_table_headers(&self) -> Result<Vec<(String, RawTableHeader)>, StorageError> {
            self.raw_table_header_scans
                .set(self.raw_table_header_scans.get() + 1);
            self.inner.scan_raw_table_headers()
        }
    }

    #[test]
    fn encode_value_ordering() {
        // Null < Boolean < Integer < BigInt < Timestamp < Text < Uuid

        let null = encode_value(&Value::Null);
        let bool_false = encode_value(&Value::Boolean(false));
        let bool_true = encode_value(&Value::Boolean(true));
        let int_neg = encode_value(&Value::Integer(-100));
        let int_zero = encode_value(&Value::Integer(0));
        let int_pos = encode_value(&Value::Integer(100));

        assert!(null < bool_false);
        assert!(bool_false < bool_true);
        assert!(bool_true < int_neg);
        assert!(int_neg < int_zero);
        assert!(int_zero < int_pos);
    }

    #[test]
    fn real_encode_value_ordering() {
        let neg_inf = encode_value(&Value::Double(f64::NEG_INFINITY));
        let neg_big = encode_value(&Value::Double(-1000.0));
        let neg_small = encode_value(&Value::Double(-0.001));
        let neg_zero = encode_value(&Value::Double(-0.0));
        let pos_zero = encode_value(&Value::Double(0.0));
        let pos_small = encode_value(&Value::Double(0.001));
        let pos_big = encode_value(&Value::Double(1000.0));
        let pos_inf = encode_value(&Value::Double(f64::INFINITY));

        assert!(neg_inf < neg_big);
        assert!(neg_big < neg_small);
        assert!(neg_small < neg_zero);
        assert!(neg_zero < pos_zero);
        assert!(pos_zero < pos_small);
        assert!(pos_small < pos_big);
        assert!(pos_big < pos_inf);
    }

    #[test]
    fn real_cross_type_ordering() {
        // Double should sort after all existing types (tag 0x09 > 0x08)
        let row = encode_value(&Value::Row {
            id: None,
            values: vec![],
        });
        let double = encode_value(&Value::Double(0.0));

        assert!(row < double);
    }

    // ----------------------------------------------------------------
    // Negative zero IEEE 754 semantics: -0.0 and 0.0 are equal per the
    // standard, so index lookups and range queries must treat them as
    // the same value even though they have distinct bit patterns.
    // ----------------------------------------------------------------

    #[test]
    fn real_negative_zero_exact_lookup() {
        // Store a value as -0.0, look it up with 0.0 (and vice versa).
        let mut storage = MemoryStorage::new();

        let row_neg = ObjectId::new();
        let row_pos = ObjectId::new();

        storage
            .index_insert("prices", "amount", "main", &Value::Double(-0.0), row_neg)
            .unwrap();
        storage
            .index_insert("prices", "amount", "main", &Value::Double(0.0), row_pos)
            .unwrap();

        // Looking up 0.0 should find both (IEEE 754: -0.0 == 0.0)
        let results = storage.index_lookup("prices", "amount", "main", &Value::Double(0.0));
        assert_eq!(results.len(), 2, "lookup 0.0 should match both zeros");
        assert!(results.contains(&row_neg));
        assert!(results.contains(&row_pos));

        // Looking up -0.0 should also find both
        let results = storage.index_lookup("prices", "amount", "main", &Value::Double(-0.0));
        assert_eq!(results.len(), 2, "lookup -0.0 should match both zeros");
        assert!(results.contains(&row_neg));
        assert!(results.contains(&row_pos));
    }

    #[test]
    fn branch_ord_allocation_commits_atomically_across_forward_reverse_and_meta_rows() {
        let mut storage = FailOnNthRawPutStorage::new(2);
        let main = BranchName::new("dev-aaaaaaaaaaaa-main");
        storage.fail_on_put_number = usize::MAX;
        for (table, header) in [
            (
                BRANCH_ORD_BY_NAME_TABLE,
                RawTableHeader::system(
                    STORAGE_KIND_BRANCH_ORD_BY_NAME,
                    BRANCH_ORD_BY_NAME_FORMAT_V1,
                ),
            ),
            (
                BRANCH_NAME_BY_ORD_TABLE,
                RawTableHeader::system(
                    STORAGE_KIND_BRANCH_NAME_BY_ORD,
                    BRANCH_NAME_BY_ORD_FORMAT_V1,
                ),
            ),
            (
                BRANCH_ORD_META_TABLE,
                RawTableHeader::system(STORAGE_KIND_BRANCH_ORD_META, BRANCH_ORD_META_FORMAT_V1),
            ),
        ] {
            storage
                .upsert_raw_table_header(table, &header)
                .expect("branch ord headers should pre-exist");
        }
        storage.fail_on_put_number = 2;
        storage.raw_puts_seen = 0;

        let err = storage.resolve_or_alloc_branch_ord(main).unwrap_err();
        assert!(
            err.to_string()
                .contains("simulated raw_table_mutation failure"),
            "unexpected error: {err}"
        );
        assert_eq!(storage.raw_puts_seen, 2);
        assert_eq!(storage.load_branch_ord(main).unwrap(), None);
        assert_eq!(storage.load_branch_name_by_ord(1).unwrap(), None);
        assert_eq!(
            storage
                .raw_table_get(BRANCH_ORD_META_TABLE, BRANCH_ORD_NEXT_ORD_KEY)
                .unwrap(),
            None
        );
    }

    #[test]
    fn branch_ord_tables_use_header_owned_format_version() {
        let mut storage = MemoryStorage::new();
        let branch_name = BranchName::new("dev-aaaaaaaaaaaa-main");

        storage.resolve_or_alloc_branch_ord(branch_name).unwrap();

        let by_name_header = storage
            .load_raw_table_header(BRANCH_ORD_BY_NAME_TABLE)
            .unwrap()
            .expect("branch ord by name header");
        assert_eq!(
            by_name_header,
            RawTableHeader::system(
                STORAGE_KIND_BRANCH_ORD_BY_NAME,
                BRANCH_ORD_BY_NAME_FORMAT_V1,
            )
        );

        let by_ord_header = storage
            .load_raw_table_header(BRANCH_NAME_BY_ORD_TABLE)
            .unwrap()
            .expect("branch name by ord header");
        assert_eq!(
            by_ord_header,
            RawTableHeader::system(
                STORAGE_KIND_BRANCH_NAME_BY_ORD,
                BRANCH_NAME_BY_ORD_FORMAT_V1,
            )
        );

        let meta_header = storage
            .load_raw_table_header(BRANCH_ORD_META_TABLE)
            .unwrap()
            .expect("branch ord meta header");
        assert_eq!(
            meta_header,
            RawTableHeader::system(STORAGE_KIND_BRANCH_ORD_META, BRANCH_ORD_META_FORMAT_V1)
        );

        let ord_bytes = storage
            .raw_table_get(BRANCH_ORD_BY_NAME_TABLE, branch_name.as_str())
            .unwrap()
            .expect("branch ord row");
        assert_eq!(decode_branch_ord_value(&ord_bytes).unwrap(), 1);

        let name_bytes = storage
            .raw_table_get(
                BRANCH_NAME_BY_ORD_TABLE,
                &branch_name_by_ord_key(1).unwrap(),
            )
            .unwrap()
            .expect("branch name row");
        assert_eq!(decode_branch_name_value(&name_bytes).unwrap(), branch_name);

        let meta_bytes = storage
            .raw_table_get(BRANCH_ORD_META_TABLE, BRANCH_ORD_NEXT_ORD_KEY)
            .unwrap()
            .expect("branch ord meta row");
        assert_eq!(decode_branch_ord_meta(&meta_bytes).unwrap(), 2);
    }

    #[test]
    fn real_negative_zero_range_gte() {
        // WHERE amount >= 0.0 should include -0.0 (equal per IEEE 754)
        let mut storage = MemoryStorage::new();

        let row_neg_zero = ObjectId::new();
        let row_pos_zero = ObjectId::new();
        let row_negative = ObjectId::new();

        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-0.0),
                row_neg_zero,
            )
            .unwrap();
        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(0.0),
                row_pos_zero,
            )
            .unwrap();
        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-1.0),
                row_negative,
            )
            .unwrap();

        // >= 0.0 should include -0.0 and 0.0, but not -1.0
        let results = storage.index_range(
            "prices",
            "amount",
            "main",
            Bound::Included(&Value::Double(0.0)),
            Bound::Unbounded,
        );
        assert!(
            results.contains(&row_neg_zero),
            ">= 0.0 should include -0.0"
        );
        assert!(results.contains(&row_pos_zero), ">= 0.0 should include 0.0");
        assert!(
            !results.contains(&row_negative),
            ">= 0.0 should exclude -1.0"
        );
    }

    #[test]
    fn real_negative_zero_range_lt() {
        // WHERE amount < 0.0 should exclude -0.0 (equal per IEEE 754, not strictly less)
        let mut storage = MemoryStorage::new();

        let row_neg_zero = ObjectId::new();
        let row_negative = ObjectId::new();

        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-0.0),
                row_neg_zero,
            )
            .unwrap();
        storage
            .index_insert(
                "prices",
                "amount",
                "main",
                &Value::Double(-1.0),
                row_negative,
            )
            .unwrap();

        // < 0.0 should exclude -0.0 but include -1.0
        let results = storage.index_range(
            "prices",
            "amount",
            "main",
            Bound::Unbounded,
            Bound::Excluded(&Value::Double(0.0)),
        );
        assert!(
            !results.contains(&row_neg_zero),
            "< 0.0 should exclude -0.0"
        );
        assert!(results.contains(&row_negative), "< 0.0 should include -1.0");
    }

    #[test]
    fn memory_storage_catalogue_entry_upsert_overwrites_existing() {
        let mut storage = MemoryStorage::new();
        let object_id = ObjectId::new();
        let initial = CatalogueEntry {
            object_id,
            metadata: HashMap::from([(
                crate::metadata::MetadataKey::Type.to_string(),
                crate::metadata::ObjectType::CatalogueSchema.to_string(),
            )]),
            content: b"v1".to_vec(),
        };
        let updated = CatalogueEntry {
            object_id,
            metadata: HashMap::from([(
                crate::metadata::MetadataKey::Type.to_string(),
                crate::metadata::ObjectType::CatalogueSchema.to_string(),
            )]),
            content: b"v2".to_vec(),
        };

        storage.upsert_catalogue_entry(&initial).unwrap();
        storage.upsert_catalogue_entry(&updated).unwrap();

        let loaded = storage.load_catalogue_entry(object_id).unwrap();
        assert_eq!(loaded, Some(updated.clone()));
        assert_eq!(storage.scan_catalogue_entries().unwrap(), vec![updated]);
    }

    #[test]
    fn memory_storage_row_histories_visible_and_history_round_trip() {
        use crate::row_histories::{HistoryScan, RowState, VisibleRowEntry};

        let mut storage = MemoryStorage::new();
        let schema_hash = seed_users_schema(&mut storage);
        let row_id = ObjectId::new();
        seed_users_row(&mut storage, row_id, schema_hash);
        let version = make_users_row_batch(
            row_id,
            "dev/main",
            "alice",
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", &[version.clone()])
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                &[VisibleRowEntry::rebuild(
                    version.clone(),
                    std::slice::from_ref(&version),
                )],
            )
            .unwrap();

        let visible = storage.scan_visible_region("users", "dev/main").unwrap();
        let history_by_row = storage.scan_history_row_batches("users", row_id).unwrap();
        let history = storage
            .scan_history_region("users", "dev/main", HistoryScan::Row { row_id })
            .unwrap();

        assert_eq!(visible, vec![version.clone()]);
        assert_eq!(history_by_row, vec![version.clone()]);
        assert_eq!(history, vec![version]);
    }

    #[test]
    fn exact_history_row_load_uses_row_locator_schema_hash_not_branch_short_hash() {
        let mut storage = MemoryStorage::new();
        let schema_hash = seed_users_schema(&mut storage);
        let row_id = ObjectId::new();
        seed_users_row(&mut storage, row_id, schema_hash);

        let branch = "dev-deadbeefcafe-main";
        assert_ne!(schema_hash.short(), "deadbeefcafe");

        let row = make_users_row_batch(
            row_id,
            branch,
            "alice",
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", std::slice::from_ref(&row))
            .unwrap();

        let loaded =
            Storage::load_history_row_batch(&storage, "users", branch, row_id, row.batch_id())
                .unwrap();

        assert_eq!(loaded, Some(row));
    }

    #[test]
    fn visible_branch_scan_unions_row_raw_tables_without_branch_hash_matching() {
        use crate::row_histories::VisibleRowEntry;

        let mut storage = MemoryStorage::new();
        let schema_hash = seed_users_schema(&mut storage);
        let row_id = ObjectId::new();
        seed_users_row(&mut storage, row_id, schema_hash);

        let branch = "dev-deadbeefcafe-main";
        assert_ne!(schema_hash.short(), "deadbeefcafe");

        let row = make_users_row_batch(
            row_id,
            branch,
            "alice",
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", std::slice::from_ref(&row))
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                &[VisibleRowEntry::rebuild(
                    row.clone(),
                    std::slice::from_ref(&row),
                )],
            )
            .unwrap();

        let loaded = storage.scan_visible_region("users", branch).unwrap();
        assert_eq!(loaded, vec![row.clone()]);

        let point_loaded = storage
            .load_visible_region_row("users", branch, row_id)
            .unwrap();
        assert_eq!(point_loaded, Some(row));
    }

    #[test]
    fn exact_visible_row_load_avoids_raw_table_header_scans() {
        use crate::row_histories::VisibleRowEntry;

        let mut storage = CountingCatalogueLoadsStorage::new();
        let schema_hash = persist_test_schema(&mut storage, &users_test_schema());
        let row_id = ObjectId::new();
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();

        let row = make_users_row_batch(
            row_id,
            "main",
            "alpha",
            RowProvenance::for_insert("alice".to_string(), 10),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", std::slice::from_ref(&row))
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                std::slice::from_ref(&VisibleRowEntry::rebuild(
                    row.clone(),
                    std::slice::from_ref(&row),
                )),
            )
            .unwrap();

        storage.raw_table_header_scans.set(0);
        let loaded = storage
            .load_visible_region_row("users", "main", row_id)
            .unwrap();

        assert_eq!(loaded, Some(row));
        assert_eq!(storage.raw_table_header_scans(), 0);
    }

    #[test]
    fn exact_history_row_load_avoids_raw_table_header_scans() {
        let mut storage = CountingCatalogueLoadsStorage::new();
        let schema_hash = persist_test_schema(&mut storage, &users_test_schema());
        let row_id = ObjectId::new();
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();

        let row = make_users_row_batch(
            row_id,
            "main",
            "alpha",
            RowProvenance::for_insert("alice".to_string(), 10),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", std::slice::from_ref(&row))
            .unwrap();

        storage.raw_table_header_scans.set(0);
        let loaded = storage
            .load_history_row_batch("users", "main", row_id, row.batch_id())
            .unwrap();

        assert_eq!(loaded, Some(row));
        assert_eq!(storage.raw_table_header_scans(), 0);
    }

    #[test]
    fn capture_family_visible_frontier_reads_synced_branches_without_branch_ords() {
        use crate::row_histories::VisibleRowEntry;

        let mut storage = CountingCatalogueLoadsStorage::new();
        let schema_hash = persist_test_schema(&mut storage, &users_test_schema());
        let target_row_id = ObjectId::new();
        let sibling_row_id = ObjectId::new();
        for row_id in [target_row_id, sibling_row_id] {
            storage
                .put_row_locator(
                    row_id,
                    Some(&RowLocator {
                        table: "users".into(),
                        origin_schema_hash: Some(schema_hash),
                    }),
                )
                .unwrap();
        }

        let target_branch = "dev-aaaaaaaaaaaa-main";
        let sibling_branch = "dev-bbbbbbbbbbbb-main";
        let target_row = make_users_row_batch(
            target_row_id,
            target_branch,
            "alpha",
            RowProvenance::for_insert("alice".to_string(), 10),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );
        let sibling_row = make_users_row_batch(
            sibling_row_id,
            sibling_branch,
            "beta",
            RowProvenance::for_insert("bob".to_string(), 20),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", &[target_row.clone(), sibling_row.clone()])
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                &[
                    VisibleRowEntry::rebuild(target_row.clone(), std::slice::from_ref(&target_row)),
                    VisibleRowEntry::rebuild(
                        sibling_row.clone(),
                        std::slice::from_ref(&sibling_row),
                    ),
                ],
            )
            .unwrap();

        assert_eq!(
            storage
                .load_branch_ord(BranchName::new(target_branch))
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .load_branch_ord(BranchName::new(sibling_branch))
                .unwrap(),
            None
        );

        let frontier = storage
            .capture_family_visible_frontier(BranchName::new(target_branch))
            .unwrap();

        assert_eq!(
            frontier,
            vec![
                CapturedFrontierMember {
                    object_id: target_row_id,
                    branch_name: BranchName::new(target_branch),
                    batch_id: target_row.batch_id(),
                },
                CapturedFrontierMember {
                    object_id: sibling_row_id,
                    branch_name: BranchName::new(sibling_branch),
                    batch_id: sibling_row.batch_id(),
                },
            ]
        );
    }

    #[test]
    fn visible_scan_loads_catalogue_descriptor_at_most_once_per_raw_table_instance() {
        use crate::row_histories::VisibleRowEntry;

        let mut storage = CountingCatalogueLoadsStorage::new();
        let schema_hash = persist_test_schema(&mut storage, &users_test_schema());
        let first_row_id = ObjectId::new();
        let second_row_id = ObjectId::new();
        for row_id in [first_row_id, second_row_id] {
            storage
                .put_row_locator(
                    row_id,
                    Some(&RowLocator {
                        table: "users".into(),
                        origin_schema_hash: Some(schema_hash),
                    }),
                )
                .unwrap();
        }

        let first_row = make_users_row_batch(
            first_row_id,
            "main",
            "alpha",
            RowProvenance::for_insert("alice".to_string(), 10),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );
        let second_row = make_users_row_batch(
            second_row_id,
            "main",
            "beta",
            RowProvenance::for_insert("alice".to_string(), 20),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", &[first_row.clone(), second_row.clone()])
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                &[
                    VisibleRowEntry::rebuild(first_row.clone(), std::slice::from_ref(&first_row)),
                    VisibleRowEntry::rebuild(second_row.clone(), std::slice::from_ref(&second_row)),
                ],
            )
            .unwrap();

        storage.catalogue_loads.set(0);
        let loaded = Storage::scan_visible_region(&storage, "users", "main").unwrap();

        assert_eq!(loaded, vec![first_row, second_row]);
        assert!(
            storage.catalogue_loads() <= 1,
            "visible scan should not reload the descriptor more than once per raw table instance"
        );
    }

    #[test]
    fn history_row_scan_loads_catalogue_descriptor_at_most_once_per_raw_table_instance() {
        let mut storage = CountingCatalogueLoadsStorage::new();
        let schema_hash = persist_test_schema(&mut storage, &users_test_schema());
        let first_row_id = ObjectId::new();
        let second_row_id = ObjectId::new();
        for row_id in [first_row_id, second_row_id] {
            storage
                .put_row_locator(
                    row_id,
                    Some(&RowLocator {
                        table: "users".into(),
                        origin_schema_hash: Some(schema_hash),
                    }),
                )
                .unwrap();
        }

        let first_row = make_users_row_batch(
            first_row_id,
            "main",
            "alpha",
            RowProvenance::for_insert("alice".to_string(), 10),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );
        let second_row = make_users_row_batch(
            second_row_id,
            "main",
            "beta",
            RowProvenance::for_insert("alice".to_string(), 20),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            Vec::new(),
        );

        storage
            .append_history_region_rows("users", &[first_row.clone(), second_row.clone()])
            .unwrap();

        storage.catalogue_loads.set(0);
        let loaded =
            Storage::scan_history_region(&storage, "users", "main", HistoryScan::Branch).unwrap();

        assert_eq!(loaded, vec![first_row, second_row]);
        assert!(
            storage.catalogue_loads() <= 1,
            "history scan should not reload the descriptor more than once per raw table instance"
        );
    }

    #[test]
    fn memory_storage_visible_entries_track_older_tier_winners() {
        use crate::row_histories::{RowState, VisibleRowEntry};

        let mut storage = MemoryStorage::new();
        let schema_hash = seed_users_schema(&mut storage);
        let row_id = ObjectId::new();
        seed_users_row(&mut storage, row_id, schema_hash);

        let globally_confirmed = make_users_row_batch(
            row_id,
            "dev/main",
            "v1",
            crate::metadata::RowProvenance::for_insert("alice".to_string(), 10),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
            Vec::new(),
        );
        let current_worker = make_users_row_batch(
            row_id,
            "dev/main",
            "v2",
            crate::metadata::RowProvenance {
                created_by: "alice".to_string(),
                created_at: 10,
                updated_by: "alice".to_string(),
                updated_at: 20,
            },
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
            vec![globally_confirmed.batch_id()],
        );

        storage
            .append_history_region_rows(
                "users",
                &[globally_confirmed.clone(), current_worker.clone()],
            )
            .unwrap();
        storage
            .upsert_visible_region_rows(
                "users",
                std::slice::from_ref(&VisibleRowEntry::rebuild(
                    current_worker.clone(),
                    &[globally_confirmed.clone(), current_worker.clone()],
                )),
            )
            .unwrap();

        let visible = storage
            .load_visible_region_row("users", "dev/main", row_id)
            .unwrap();
        let entry = storage
            .row_histories
            .get("users")
            .and_then(|regions| regions.visible.get("dev/main"))
            .and_then(|rows| rows.get(&row_id))
            .cloned()
            .expect("visible entry");

        assert_eq!(visible, Some(current_worker.clone()));
        assert_eq!(entry.current_row, current_worker);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, Some(globally_confirmed.batch_id()));
        assert_eq!(entry.global_batch_id, Some(globally_confirmed.batch_id()));
    }

    #[test]
    fn raw_history_bytes_roundtrip_flat_rows_outside_storage() {
        let mut storage = MemoryStorage::new();
        let user_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
        ]);
        let row_id = ObjectId::new();
        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("Ship flat rows".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );
        let encoded = encode_flat_history_row(&user_descriptor, &row).unwrap();

        storage
            .append_history_region_row_bytes(
                "tasks",
                &[HistoryRowBytes {
                    row_raw_table: "rowtable:history:tasks:test",
                    branch: row.branch.as_str(),
                    row_id,
                    batch_id: row.batch_id(),
                    bytes: &encoded,
                }],
            )
            .unwrap();

        let loaded = storage
            .load_history_row_batch_bytes("tasks", row.branch.as_str(), row_id, row.batch_id())
            .unwrap()
            .expect("history bytes should load");
        assert_eq!(
            decode_flat_history_row(
                &user_descriptor,
                row_id,
                row.branch.as_str(),
                row.batch_id(),
                &loaded,
            )
            .unwrap(),
            row
        );

        let scanned = storage
            .scan_history_region_bytes("tasks", HistoryScan::Row { row_id })
            .unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(
            decode_flat_history_row(
                &user_descriptor,
                row_id,
                row.branch.as_str(),
                row.batch_id(),
                &scanned[0],
            )
            .unwrap(),
            row
        );
    }

    #[test]
    fn typed_history_appends_use_flat_rows_when_schema_is_known() {
        use crate::catalogue::CatalogueEntry;
        use crate::metadata::{MetadataKey, ObjectType};
        use crate::query_manager::types::{SchemaBuilder, SchemaHash, TableSchema, Value};
        use crate::schema_manager::encoding::encode_schema;

        let mut storage = MemoryStorage::new();
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("tasks")
                    .column("title", ColumnType::Text)
                    .nullable_column("done", ColumnType::Boolean),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let row_id = ObjectId::new();
        let row_locator = RowLocator {
            table: "tasks".into(),
            origin_schema_hash: Some(schema_hash),
        };

        storage
            .upsert_catalogue_entry(&CatalogueEntry {
                object_id: schema_hash.to_object_id(),
                metadata: HashMap::from([(
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                )]),
                content: encode_schema(&schema),
            })
            .unwrap();
        storage.put_row_locator(row_id, Some(&row_locator)).unwrap();

        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("Ship flat rows".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );

        storage
            .append_history_region_rows("tasks", std::slice::from_ref(&row))
            .unwrap();

        let encoded = storage
            .load_history_row_batch_bytes("tasks", row.branch.as_str(), row_id, row.batch_id())
            .unwrap()
            .expect("history bytes should load");
        assert_eq!(
            decode_flat_history_row(
                &user_descriptor,
                row_id,
                row.branch.as_str(),
                row.batch_id(),
                &encoded,
            )
            .unwrap(),
            row
        );
    }

    #[test]
    fn typed_history_appends_reuse_catalogue_descriptor_for_same_schema() {
        use crate::catalogue::CatalogueEntry;
        use crate::metadata::{MetadataKey, ObjectType};
        use crate::query_manager::types::{SchemaBuilder, SchemaHash, TableSchema, Value};
        use crate::schema_manager::encoding::encode_schema;

        let mut storage = CountingCatalogueLoadsStorage::new();
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("tasks")
                    .column("title", ColumnType::Text)
                    .nullable_column("done", ColumnType::Boolean),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let user_descriptor = schema[&"tasks".into()].columns.clone();

        storage
            .upsert_catalogue_entry(&CatalogueEntry {
                object_id: schema_hash.to_object_id(),
                metadata: HashMap::from([(
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                )]),
                content: encode_schema(&schema),
            })
            .unwrap();

        let alice_task_id = ObjectId::new();
        let bob_task_id = ObjectId::new();
        let row_locator = RowLocator {
            table: "tasks".into(),
            origin_schema_hash: Some(schema_hash),
        };
        storage
            .put_row_locator(alice_task_id, Some(&row_locator))
            .unwrap();
        storage
            .put_row_locator(bob_task_id, Some(&row_locator))
            .unwrap();

        let alice_task = crate::row_histories::StoredRowBatch::new(
            alice_task_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[
                    Value::Text("Prepare dropdown seeds".into()),
                    Value::Boolean(false),
                ],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );
        let bob_task = crate::row_histories::StoredRowBatch::new(
            bob_task_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[
                    Value::Text("Verify seeded rows".into()),
                    Value::Boolean(true),
                ],
            )
            .unwrap(),
            RowProvenance::for_insert("bob".to_string(), 101),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );

        storage
            .append_history_region_rows("tasks", &[alice_task, bob_task])
            .unwrap();

        assert_eq!(storage.catalogue_loads(), 1);
    }

    #[test]
    fn typed_history_appends_use_row_locator_schema_before_catalogue_scans() {
        use crate::catalogue::CatalogueEntry;
        use crate::metadata::{MetadataKey, ObjectType};
        use crate::query_manager::types::{SchemaBuilder, SchemaHash, TableSchema, Value};
        use crate::schema_manager::encoding::encode_schema;

        struct PanicOnCatalogueScanStorage {
            inner: MemoryStorage,
        }

        impl Storage for PanicOnCatalogueScanStorage {
            fn raw_table_put(
                &mut self,
                table: &str,
                key: &str,
                value: &[u8],
            ) -> Result<(), StorageError> {
                self.inner.raw_table_put(table, key, value)
            }

            fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
                self.inner.raw_table_delete(table, key)
            }

            fn raw_table_get(
                &self,
                table: &str,
                key: &str,
            ) -> Result<Option<Vec<u8>>, StorageError> {
                self.inner.raw_table_get(table, key)
            }

            fn raw_table_scan_prefix(
                &self,
                table: &str,
                prefix: &str,
            ) -> Result<RawTableRows, StorageError> {
                self.inner.raw_table_scan_prefix(table, prefix)
            }

            fn raw_table_scan_range(
                &self,
                table: &str,
                start: Option<&str>,
                end: Option<&str>,
            ) -> Result<RawTableRows, StorageError> {
                self.inner.raw_table_scan_range(table, start, end)
            }

            fn append_history_region_row_bytes(
                &mut self,
                table: &str,
                rows: &[HistoryRowBytes<'_>],
            ) -> Result<(), StorageError> {
                self.inner.append_history_region_row_bytes(table, rows)
            }

            fn upsert_visible_region_row_bytes(
                &mut self,
                table: &str,
                rows: &[VisibleRowBytes<'_>],
            ) -> Result<(), StorageError> {
                self.inner.upsert_visible_region_row_bytes(table, rows)
            }

            fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError> {
                panic!(
                    "append path should not need catalogue scans when row locator has exact schema"
                );
            }
        }

        let mut storage = PanicOnCatalogueScanStorage {
            inner: MemoryStorage::new(),
        };
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("tasks")
                    .column("title", ColumnType::Text)
                    .nullable_column("done", ColumnType::Boolean),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let row_id = ObjectId::new();

        storage
            .upsert_catalogue_entry(&CatalogueEntry {
                object_id: schema_hash.to_object_id(),
                metadata: HashMap::from([(
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                )]),
                content: encode_schema(&schema),
            })
            .unwrap();
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "tasks".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();

        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[
                    Value::Text("Ship locator fast path".into()),
                    Value::Boolean(false),
                ],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );

        storage
            .append_history_region_rows("tasks", std::slice::from_ref(&row))
            .unwrap();
    }

    #[test]
    fn exact_row_table_locator_round_trips_raw_table_name() {
        let schema_hash = SchemaHash::from_bytes([0x11; 32]);
        let locator = ExactRowTableLocator {
            row_raw_table: "rowtable:visible:tasks:1111111111111111111111111111111111111111111111111111111111111111"
                .into(),
            table_name: "tasks".into(),
            schema_hash,
        };

        let encoded = encode_exact_row_table_locator(&locator).expect("encode locator");
        let decoded = decode_exact_row_table_locator(&encoded).expect("decode locator");

        assert_eq!(decoded, locator);
    }

    fn seed_common_case_visible_task_row<H: Storage>(
        storage: &mut H,
    ) -> (SchemaHash, ObjectId, crate::row_histories::StoredRowBatch) {
        use crate::catalogue::CatalogueEntry;
        use crate::metadata::{MetadataKey, ObjectType};
        use crate::query_manager::types::{SchemaBuilder, TableSchema, Value};
        use crate::schema_manager::encoding::encode_schema;

        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let row_id = ObjectId::new();
        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("Ship common-case locators".into())],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        storage
            .upsert_catalogue_entry(&CatalogueEntry {
                object_id: schema_hash.to_object_id(),
                metadata: HashMap::from([(
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                )]),
                content: encode_schema(&schema),
            })
            .unwrap();
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "tasks".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();
        storage
            .append_history_region_rows("tasks", std::slice::from_ref(&row))
            .unwrap();
        storage
            .upsert_visible_region_rows("tasks", &[VisibleRowEntry::new(row.clone())])
            .unwrap();

        (schema_hash, row_id, row)
    }

    #[test]
    fn common_case_rows_skip_exact_locator_tables_and_still_load_exactly() {
        let mut storage = RawTableOnlyMemoryStorage::new();
        let (_schema_hash, row_id, row) = seed_common_case_visible_task_row(&mut storage);

        assert_eq!(
            storage
                .raw_table_scan_prefix(HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE, "")
                .unwrap(),
            Vec::new()
        );
        assert_eq!(
            storage
                .raw_table_scan_prefix(VISIBLE_ROW_TABLE_LOCATOR_TABLE, "")
                .unwrap(),
            Vec::new()
        );
        assert_eq!(
            storage
                .load_history_row_batch("tasks", "main", row_id, row.batch_id())
                .unwrap(),
            Some(row.clone())
        );
        assert_eq!(
            storage
                .load_visible_region_row("tasks", "main", row_id)
                .unwrap(),
            Some(row)
        );
    }

    #[test]
    fn common_case_visible_delete_removes_visible_row_without_exact_locator_row() {
        let mut storage = RawTableOnlyMemoryStorage::new();
        let (schema_hash, row_id, row) = seed_common_case_visible_task_row(&mut storage);

        storage
            .delete_visible_region_row("tasks", row.branch.as_str(), row_id)
            .unwrap();

        assert_eq!(
            storage
                .load_visible_region_row("tasks", row.branch.as_str(), row_id)
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .raw_table_get(
                    &visible_row_raw_table_id("tasks", schema_hash).raw_table_name,
                    &key_codec::visible_row_raw_table_key(row.branch.as_str(), row_id),
                )
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .raw_table_scan_prefix(VISIBLE_ROW_TABLE_LOCATOR_TABLE, "")
                .unwrap(),
            Vec::new()
        );
    }

    #[test]
    fn rows_without_row_locator_still_persist_exact_locators() {
        use crate::catalogue::CatalogueEntry;
        use crate::metadata::{MetadataKey, ObjectType};
        use crate::query_manager::types::{SchemaBuilder, TableSchema, Value};
        use crate::schema_manager::encoding::encode_schema;

        let mut storage = MemoryStorage::new();
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("tasks")
                    .column("title", ColumnType::Text)
                    .nullable_column("done", ColumnType::Boolean),
            )
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let row_id = ObjectId::new();
        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[
                    Value::Text("Persist exact locator".into()),
                    Value::Boolean(false),
                ],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let entry = VisibleRowEntry::new(row.clone());

        storage
            .upsert_catalogue_entry(&CatalogueEntry {
                object_id: schema_hash.to_object_id(),
                metadata: HashMap::from([(
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                )]),
                content: encode_schema(&schema),
            })
            .unwrap();

        storage
            .append_history_region_rows("tasks", std::slice::from_ref(&row))
            .unwrap();
        storage
            .upsert_visible_region_rows("tasks", std::slice::from_ref(&entry))
            .unwrap();

        let history_locators = storage
            .raw_table_scan_prefix(HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE, "")
            .unwrap();
        let visible_locators = storage
            .raw_table_scan_prefix(VISIBLE_ROW_TABLE_LOCATOR_TABLE, "")
            .unwrap();

        assert_eq!(history_locators.len(), 1);
        assert_eq!(visible_locators.len(), 1);
        assert_eq!(
            decode_exact_row_table_locator(&history_locators[0].1).unwrap(),
            ExactRowTableLocator {
                row_raw_table: history_row_raw_table_id("tasks", schema_hash).raw_table_name,
                table_name: "tasks".into(),
                schema_hash,
            }
        );
        assert_eq!(
            decode_exact_row_table_locator(&visible_locators[0].1).unwrap(),
            ExactRowTableLocator {
                row_raw_table: visible_row_raw_table_id("tasks", schema_hash).raw_table_name,
                table_name: "tasks".into(),
                schema_hash,
            }
        );
    }

    #[test]
    fn branch_ord_tables_reject_header_version_mismatch_on_read() {
        let mut storage = MemoryStorage::new();
        let branch_name = BranchName::new("dev-aaaaaaaaaaaa-main");

        storage.resolve_or_alloc_branch_ord(branch_name).unwrap();

        let mut header = storage
            .load_raw_table_header(BRANCH_ORD_BY_NAME_TABLE)
            .unwrap()
            .expect("branch ord by name header");
        header.storage_format_version += 1;
        storage
            .upsert_raw_table_header(BRANCH_ORD_BY_NAME_TABLE, &header)
            .unwrap();

        let err = storage.load_branch_ord(branch_name).unwrap_err();
        assert!(
            err.to_string().contains("storage_format_version mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn nonempty_headerless_raw_table_is_rejected_instead_of_backfilled() {
        let mut storage = MemoryStorage::new();
        storage
            .raw_table_put("legacy_users", "alice", b"hello")
            .expect("seed legacy row without header");

        let err = ensure_raw_table_header(
            &mut storage,
            "legacy_users",
            &RawTableHeader::system(STORAGE_KIND_CATALOGUE, CATALOGUE_STORAGE_FORMAT_V1),
        )
        .expect_err("non-empty headerless raw table should fail closed");

        assert!(
            err.to_string()
                .contains("missing raw table header for non-empty table legacy_users"),
            "unexpected error: {err}"
        );
        assert_eq!(
            storage.load_raw_table_header("legacy_users").unwrap(),
            None,
            "legacy raw table should not be relabeled in place"
        );
    }

    #[test]
    fn exact_visible_row_load_rejects_row_raw_table_header_version_mismatch() {
        let mut storage = RawTableOnlyMemoryStorage::new();
        let (schema_hash, row_id, row) = seed_common_case_visible_task_row(&mut storage);

        let raw_table = visible_row_raw_table_id("tasks", schema_hash).raw_table_name;
        let mut header = storage
            .load_raw_table_header(&raw_table)
            .unwrap()
            .expect("visible raw table header");
        header.storage_format_version += 1;
        storage
            .upsert_raw_table_header(&raw_table, &header)
            .unwrap();

        let err = storage
            .load_visible_region_row("tasks", row.branch.as_str(), row_id)
            .unwrap_err();
        assert!(
            err.to_string().contains("storage_format_version mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn local_batch_records_store_artifacts_in_dedicated_tables_only() {
        use crate::batch_fate::{
            BatchMode, LocalBatchMember, SealedBatchMember, VisibleBatchMember,
        };

        let mut storage = MemoryStorage::new();
        let batch_id = BatchId::new();
        let object_id = ObjectId::new();
        let branch_name = BranchName::new("main");
        let schema_hash = SchemaHash::from_bytes([0x33; 32]);
        let row_digest = Digest32([0x44; 32]);
        let mut record = LocalBatchRecord::new(batch_id, BatchMode::Transactional, false, None);
        record.upsert_member(LocalBatchMember {
            object_id,
            table_name: "tasks".to_string(),
            branch_name,
            schema_hash,
            row_digest,
        });
        let submission = SealedBatchSubmission::new(
            batch_id,
            BranchName::new("main"),
            vec![SealedBatchMember {
                object_id,
                row_digest,
            }],
            Vec::new(),
        );
        record.mark_sealed(submission.clone());
        let settlement = BatchSettlement::AcceptedTransaction {
            batch_id,
            confirmed_tier: DurabilityTier::EdgeServer,
            visible_members: vec![VisibleBatchMember {
                object_id,
                branch_name: BranchName::new("main"),
                batch_id,
            }],
        };
        record.apply_settlement(settlement.clone());

        storage.upsert_local_batch_record(&record).unwrap();

        let bytes = storage
            .raw_table_get(LOCAL_BATCH_RECORD_TABLE, &local_batch_record_key(batch_id))
            .unwrap()
            .expect("local batch record row");
        let values = decode_row(
            &local_batch_record_storage_descriptor_with_branch_ords(),
            &bytes,
        )
        .expect("decode local batch record row");
        assert_eq!(values.len(), 4);
        assert_eq!(
            storage
                .load_sealed_batch_submission(batch_id)
                .unwrap()
                .expect("sealed submission"),
            submission
        );
        assert_eq!(
            storage
                .load_authoritative_batch_settlement(batch_id)
                .unwrap()
                .expect("authoritative settlement"),
            settlement
        );
        assert_eq!(
            storage
                .load_local_batch_record(batch_id)
                .unwrap()
                .expect("record"),
            record
        );
    }

    #[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
    #[test]
    fn sqlite_common_case_visible_delete_after_restart_removes_durable_row() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("storage.sqlite");

        let (schema_hash, row_id, row) = {
            let mut storage = SqliteStorage::open(&path).unwrap();
            let seeded = seed_common_case_visible_task_row(&mut storage);
            storage.flush();
            storage.close().unwrap();
            seeded
        };

        let mut storage = SqliteStorage::open(&path).unwrap();
        storage
            .delete_visible_region_row("tasks", row.branch.as_str(), row_id)
            .unwrap();

        assert_eq!(
            storage
                .load_visible_region_row("tasks", row.branch.as_str(), row_id)
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .raw_table_get(
                    &visible_row_raw_table_id("tasks", schema_hash).raw_table_name,
                    &key_codec::visible_row_raw_table_key(row.branch.as_str(), row_id),
                )
                .unwrap(),
            None
        );
    }

    #[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
    #[test]
    fn rocksdb_common_case_visible_delete_after_restart_removes_durable_row() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("rocksdb");

        let (schema_hash, row_id, row) = {
            let mut storage = RocksDBStorage::open(&path, 8 * 1024 * 1024).unwrap();
            let seeded = seed_common_case_visible_task_row(&mut storage);
            storage.flush();
            storage.close().unwrap();
            seeded
        };

        let mut storage = RocksDBStorage::open(&path, 8 * 1024 * 1024).unwrap();
        storage
            .delete_visible_region_row("tasks", row.branch.as_str(), row_id)
            .unwrap();

        assert_eq!(
            storage
                .load_visible_region_row("tasks", row.branch.as_str(), row_id)
                .unwrap(),
            None
        );
        assert_eq!(
            storage
                .raw_table_get(
                    &visible_row_raw_table_id("tasks", schema_hash).raw_table_name,
                    &key_codec::visible_row_raw_table_key(row.branch.as_str(), row_id),
                )
                .unwrap(),
            None
        );
    }

    #[test]
    fn visible_region_scan_ignores_unrelated_malformed_raw_table_headers() {
        use crate::catalogue::CatalogueEntry;
        use crate::metadata::{MetadataKey, ObjectType};
        use crate::query_manager::types::{SchemaBuilder, TableSchema, Value};
        use crate::schema_manager::encoding::encode_schema;

        let mut storage = MemoryStorage::new();
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let row_id = ObjectId::new();

        storage
            .upsert_catalogue_entry(&CatalogueEntry {
                object_id: schema_hash.to_object_id(),
                metadata: HashMap::from([(
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                )]),
                content: encode_schema(&schema),
            })
            .unwrap();
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "tasks".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();

        let current_row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(&user_descriptor, &[Value::Text("Ship it".into())]).unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );
        storage
            .upsert_visible_region_rows("tasks", &[VisibleRowEntry::new(current_row.clone())])
            .unwrap();

        storage
            .upsert_raw_table_header(
                "rowtable:visible:users:2222222222222222222222222222222222222222222222222222222222222222",
                &RawTableHeader::system(STORAGE_KIND_BRANCH_ORD_BY_NAME, 1),
            )
            .unwrap();

        let scanned = scan_visible_row_bytes_with_storage(&storage, "tasks", "main");
        assert!(
            scanned.is_ok(),
            "scan should ignore unrelated malformed headers"
        );
    }

    #[test]
    fn visible_region_load_uses_catalogue_descriptor_without_raw_table_header_row_descriptor() {
        use crate::catalogue::CatalogueEntry;
        use crate::metadata::{MetadataKey, ObjectType};
        use crate::query_manager::types::{SchemaBuilder, TableSchema, Value};
        use crate::schema_manager::encoding::encode_schema;

        let mut storage = MemoryStorage::new();
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
            .build();
        let schema_hash = SchemaHash::compute(&schema);
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let row_id = ObjectId::new();

        storage
            .upsert_catalogue_entry(&CatalogueEntry {
                object_id: schema_hash.to_object_id(),
                metadata: HashMap::from([(
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                )]),
                content: encode_schema(&schema),
            })
            .unwrap();
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "tasks".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();

        let current_row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(&user_descriptor, &[Value::Text("Ship it".into())]).unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );
        storage
            .upsert_visible_region_rows("tasks", &[VisibleRowEntry::new(current_row.clone())])
            .unwrap();

        storage
            .upsert_raw_table_header(
                &visible_row_raw_table_id("tasks", schema_hash).raw_table_name,
                &RawTableHeader {
                    row_descriptor_bytes: None,
                    ..row_raw_table_header(
                        &visible_row_raw_table_id("tasks", schema_hash),
                        &user_descriptor,
                    )
                },
            )
            .unwrap();

        let loaded = storage
            .load_visible_region_row("tasks", "main", row_id)
            .unwrap()
            .expect("visible row should still load from the catalogue-backed descriptor");
        assert_eq!(loaded, current_row);
    }

    #[test]
    fn typed_history_appends_require_catalogue_backed_descriptor() {
        use crate::query_manager::types::{SchemaBuilder, TableSchema, Value};

        let mut storage = MemoryStorage::new();
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("tasks").column("title", ColumnType::Text))
            .build();
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let row_id = ObjectId::new();
        let row = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(&user_descriptor, &[Value::Text("Needs schema".into())]).unwrap(),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            crate::row_histories::RowState::VisibleDirect,
            None,
        );

        let error = storage
            .append_history_region_rows("tasks", std::slice::from_ref(&row))
            .expect_err("typed history writes should require a catalogue-backed descriptor");

        assert!(
            matches!(error, StorageError::IoError(ref message) if message.contains("missing catalogue-backed row descriptor")),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn exact_visible_row_tier_load_uses_persisted_winner_sidecar() {
        use crate::batch_fate::VisibleBatchMember;
        use crate::query_manager::types::{SchemaBuilder, TableSchema, Value};
        use crate::row_format::decode_row;
        use crate::row_histories::{RowState, VisibleRowEntry};

        let mut storage = RawTableOnlyMemoryStorage::new();
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("tasks")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean),
            )
            .build();
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let schema_hash = persist_test_schema(&mut storage, &schema);
        let row_id = ObjectId::new();
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "tasks".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();

        let base = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let edge_title = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &user_descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let worker_done = crate::row_histories::StoredRowBatch::new(
            row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &user_descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &user_descriptor,
            &[base.clone(), edge_title.clone(), worker_done.clone()],
        )
        .unwrap()
        .expect("merged visible entry");

        storage
            .append_history_region_rows(
                "tasks",
                &[base.clone(), edge_title.clone(), worker_done.clone()],
            )
            .unwrap();
        storage
            .upsert_visible_region_rows("tasks", std::slice::from_ref(&entry))
            .unwrap();
        for (row, confirmed_tier) in [
            (&base, DurabilityTier::GlobalServer),
            (&edge_title, DurabilityTier::EdgeServer),
            (&worker_done, DurabilityTier::Local),
        ] {
            storage
                .upsert_authoritative_batch_settlement(&BatchSettlement::DurableDirect {
                    batch_id: row.batch_id,
                    confirmed_tier,
                    visible_members: vec![VisibleBatchMember {
                        object_id: row_id,
                        branch_name: BranchName::new("main"),
                        batch_id: row.batch_id,
                    }],
                })
                .unwrap();
        }

        let worker_preview = Storage::load_visible_region_row_for_tier(
            &storage,
            "tasks",
            "main",
            row_id,
            DurabilityTier::Local,
        )
        .unwrap()
        .expect("worker preview");
        let edge_preview = Storage::load_visible_region_row_for_tier(
            &storage,
            "tasks",
            "main",
            row_id,
            DurabilityTier::EdgeServer,
        )
        .unwrap()
        .expect("edge preview");
        let global_preview = Storage::load_visible_region_row_for_tier(
            &storage,
            "tasks",
            "main",
            row_id,
            DurabilityTier::GlobalServer,
        )
        .unwrap()
        .expect("global preview");

        assert_eq!(
            decode_row(&user_descriptor, &worker_preview.data).unwrap(),
            vec![Value::Text("edge-title".into()), Value::Boolean(true)]
        );
        assert_eq!(
            decode_row(&user_descriptor, &edge_preview.data).unwrap(),
            vec![Value::Text("edge-title".into()), Value::Boolean(false)]
        );
        assert_eq!(
            decode_row(&user_descriptor, &global_preview.data).unwrap(),
            vec![Value::Text("task".into()), Value::Boolean(false)]
        );
    }

    #[test]
    fn exact_visible_row_tier_load_uses_authoritative_batch_settlement() {
        use crate::batch_fate::VisibleBatchMember;
        use crate::query_manager::types::{SchemaBuilder, TableSchema, Value};
        use crate::row_format::decode_row;
        use crate::row_histories::{RowState, VisibleRowEntry};

        let mut storage = MemoryStorage::new();
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("tasks")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean),
            )
            .build();
        let user_descriptor = schema[&"tasks".into()].columns.clone();
        let schema_hash = persist_test_schema(&mut storage, &schema);
        let row_id = ObjectId::new();
        storage
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "tasks".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();

        let row = StoredRowBatch::new(
            row_id,
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("settled".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            None,
        );
        let entry =
            VisibleRowEntry::rebuild_with_descriptor(&user_descriptor, std::slice::from_ref(&row))
                .unwrap()
                .expect("visible entry");
        storage
            .append_history_region_rows("tasks", std::slice::from_ref(&row))
            .unwrap();
        storage
            .upsert_visible_region_rows("tasks", std::slice::from_ref(&entry))
            .unwrap();
        storage
            .upsert_authoritative_batch_settlement(&BatchSettlement::DurableDirect {
                batch_id: row.batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
                visible_members: vec![VisibleBatchMember {
                    object_id: row_id,
                    branch_name: BranchName::new("main"),
                    batch_id: row.batch_id,
                }],
            })
            .unwrap();

        let global_preview = Storage::load_visible_region_row_for_tier(
            &storage,
            "tasks",
            "main",
            row_id,
            DurabilityTier::GlobalServer,
        )
        .unwrap()
        .expect("global preview");

        assert_eq!(
            global_preview.confirmed_tier,
            Some(DurabilityTier::GlobalServer)
        );
        assert_eq!(
            decode_row(&user_descriptor, &global_preview.data).unwrap(),
            vec![Value::Text("settled".into()), Value::Boolean(false)]
        );
    }

    mod memory_conformance {
        use crate::storage::MemoryStorage;
        use crate::storage::Storage;

        crate::storage_conformance_tests!(memory, || {
            Box::new(MemoryStorage::new()) as Box<dyn Storage>
        });
    }
}
