//! The Storage trait plus its blanket forwarding impl for `Box<T: Storage>`.
//!
//! Trait-adjacent types and codecs live in `super` (the storage module root);
//! per-backend implementations (sqlite, rocksdb, opfs_btree, memory) live in
//! their own sibling modules.

use std::ops::Bound;

use super::*;

// ============================================================================
// Storage Trait
// ============================================================================

/// Synchronous storage for metadata, row histories, raw tables, and indices.
///
/// All operations are **synchronous** - they return immediately with results.
/// This eliminates the async response/callback pattern that permeated the
/// old architecture.
///
/// # Single-threaded
///
/// No `Send + Sync` bounds. Each thread has its own Storage instance.
/// Cross-thread communication uses the sync protocol, not shared state.
pub trait Storage {
    fn storage_cache_namespace(&self) -> usize {
        std::ptr::from_ref(self).cast::<()>() as usize
    }

    // ================================================================
    // Logical row locator storage (sync - returns immediately with result)
    // ================================================================

    fn scan_row_locators(&self) -> Result<RowLocatorRows, StorageError> {
        let mut rows = Vec::new();
        for (key, bytes) in self.raw_table_scan_prefix(ROW_LOCATOR_TABLE, "")? {
            ensure_system_raw_table_header_validated_once(
                self,
                ROW_LOCATOR_TABLE,
                STORAGE_KIND_ROW_LOCATOR,
                ROW_LOCATOR_STORAGE_FORMAT_V1,
            )?;
            rows.push((decode_metadata_raw_key(&key)?, decode_row_locator(&bytes)?));
        }
        rows.sort_by_key(|(object_id, _)| *object_id);
        Ok(rows)
    }

    fn load_row_locator(&self, id: ObjectId) -> Result<Option<RowLocator>, StorageError> {
        self.raw_table_get(ROW_LOCATOR_TABLE, &metadata_raw_key(id))?
            .map(|bytes| {
                ensure_system_raw_table_header_validated_once(
                    self,
                    ROW_LOCATOR_TABLE,
                    STORAGE_KIND_ROW_LOCATOR,
                    ROW_LOCATOR_STORAGE_FORMAT_V1,
                )?;
                decode_row_locator(&bytes)
            })
            .transpose()
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&RowLocator>,
    ) -> Result<(), StorageError> {
        if let Some(locator) = locator {
            ensure_raw_table_header(
                self,
                ROW_LOCATOR_TABLE,
                &RawTableHeader::system(STORAGE_KIND_ROW_LOCATOR, ROW_LOCATOR_STORAGE_FORMAT_V1),
            )?;
            let locator_bytes = encode_row_locator(locator)?;
            self.raw_table_put(ROW_LOCATOR_TABLE, &metadata_raw_key(id), &locator_bytes)
        } else {
            self.raw_table_delete(ROW_LOCATOR_TABLE, &metadata_raw_key(id))
        }
    }

    fn load_visible_row_table_locator(
        &self,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<ExactRowTableLocator>, StorageError> {
        self.raw_table_get(
            VISIBLE_ROW_TABLE_LOCATOR_TABLE,
            &visible_row_table_locator_key(branch, row_id),
        )?
        .map(|bytes| {
            ensure_system_raw_table_header_validated_once(
                self,
                VISIBLE_ROW_TABLE_LOCATOR_TABLE,
                STORAGE_KIND_VISIBLE_ROW_TABLE_LOCATOR,
                EXACT_ROW_TABLE_LOCATOR_STORAGE_FORMAT_V1,
            )?;
            decode_exact_row_table_locator(&bytes)
        })
        .transpose()
    }

    fn put_visible_row_table_locator(
        &mut self,
        branch: &str,
        row_id: ObjectId,
        locator: Option<&ExactRowTableLocator>,
    ) -> Result<(), StorageError> {
        let key = visible_row_table_locator_key(branch, row_id);
        if let Some(locator) = locator {
            ensure_raw_table_header(
                self,
                VISIBLE_ROW_TABLE_LOCATOR_TABLE,
                &RawTableHeader::system(
                    STORAGE_KIND_VISIBLE_ROW_TABLE_LOCATOR,
                    EXACT_ROW_TABLE_LOCATOR_STORAGE_FORMAT_V1,
                ),
            )?;
            let bytes = encode_exact_row_table_locator(locator)?;
            self.raw_table_put(VISIBLE_ROW_TABLE_LOCATOR_TABLE, &key, &bytes)
        } else {
            self.raw_table_delete(VISIBLE_ROW_TABLE_LOCATOR_TABLE, &key)
        }
    }

    fn load_history_row_batch_table_locator(
        &self,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<ExactRowTableLocator>, StorageError> {
        self.raw_table_get(
            HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE,
            &history_row_batch_table_locator_key(row_id, branch, batch_id),
        )?
        .map(|bytes| {
            ensure_system_raw_table_header_validated_once(
                self,
                HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE,
                STORAGE_KIND_HISTORY_ROW_BATCH_TABLE_LOCATOR,
                EXACT_ROW_TABLE_LOCATOR_STORAGE_FORMAT_V1,
            )?;
            decode_exact_row_table_locator(&bytes)
        })
        .transpose()
    }

    fn put_history_row_batch_table_locator(
        &mut self,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
        locator: Option<&ExactRowTableLocator>,
    ) -> Result<(), StorageError> {
        let key = history_row_batch_table_locator_key(row_id, branch, batch_id);
        if let Some(locator) = locator {
            ensure_raw_table_header(
                self,
                HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE,
                &RawTableHeader::system(
                    STORAGE_KIND_HISTORY_ROW_BATCH_TABLE_LOCATOR,
                    EXACT_ROW_TABLE_LOCATOR_STORAGE_FORMAT_V1,
                ),
            )?;
            let bytes = encode_exact_row_table_locator(locator)?;
            self.raw_table_put(HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE, &key, &bytes)
        } else {
            self.raw_table_delete(HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE, &key)
        }
    }

    // ================================================================
    // Ordered raw-table storage
    // ================================================================

    fn raw_table_put(
        &mut self,
        _table: &str,
        _key: &str,
        _value: &[u8],
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "raw table puts are not implemented for this backend yet".to_string(),
        ))
    }

    fn raw_table_delete(&mut self, _table: &str, _key: &str) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "raw table deletes are not implemented for this backend yet".to_string(),
        ))
    }

    fn apply_raw_table_mutations(
        &mut self,
        mutations: &[RawTableMutation<'_>],
    ) -> Result<(), StorageError> {
        for mutation in mutations {
            match mutation {
                RawTableMutation::Put { table, key, value } => {
                    self.raw_table_put(table, key, value)?;
                }
                RawTableMutation::Delete { table, key } => {
                    self.raw_table_delete(table, key)?;
                }
            }
        }
        Ok(())
    }

    fn raw_table_get(&self, _table: &str, _key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Err(StorageError::IoError(
            "raw table lookups are not implemented for this backend yet".to_string(),
        ))
    }

    fn raw_table_scan_prefix(
        &self,
        _table: &str,
        _prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        Err(StorageError::IoError(
            "raw table prefix scans are not implemented for this backend yet".to_string(),
        ))
    }

    fn raw_table_scan_range(
        &self,
        _table: &str,
        _start: Option<&str>,
        _end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        Err(StorageError::IoError(
            "raw table range scans are not implemented for this backend yet".to_string(),
        ))
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        self.raw_table_scan_prefix(table, prefix)
            .map(|rows| rows.into_iter().map(|(key, _)| key).collect())
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        self.raw_table_scan_range(table, start, end)
            .map(|rows| rows.into_iter().map(|(key, _)| key).collect())
    }

    fn load_branch_ord(&self, branch_name: BranchName) -> Result<Option<BranchOrd>, StorageError> {
        self.raw_table_get(
            BRANCH_ORD_BY_NAME_TABLE,
            &branch_ord_by_name_key(branch_name),
        )?
        .map(|bytes| {
            ensure_system_raw_table_header_validated_once(
                self,
                BRANCH_ORD_BY_NAME_TABLE,
                STORAGE_KIND_BRANCH_ORD_BY_NAME,
                BRANCH_ORD_BY_NAME_FORMAT_V1,
            )?;
            decode_branch_ord_value(&bytes)
        })
        .transpose()
    }

    fn load_branch_name_by_ord(
        &self,
        branch_ord: BranchOrd,
    ) -> Result<Option<BranchName>, StorageError> {
        let key = branch_name_by_ord_key(branch_ord)?;
        self.raw_table_get(BRANCH_NAME_BY_ORD_TABLE, &key)?
            .map(|bytes| {
                ensure_system_raw_table_header_validated_once(
                    self,
                    BRANCH_NAME_BY_ORD_TABLE,
                    STORAGE_KIND_BRANCH_NAME_BY_ORD,
                    BRANCH_NAME_BY_ORD_FORMAT_V1,
                )?;
                decode_branch_name_value(&bytes)
            })
            .transpose()
    }

    fn resolve_or_alloc_branch_ord(
        &mut self,
        branch_name: BranchName,
    ) -> Result<BranchOrd, StorageError> {
        if let Some(existing_ord) = self.load_branch_ord(branch_name)? {
            return Ok(existing_ord);
        }

        ensure_raw_table_header(
            self,
            BRANCH_ORD_BY_NAME_TABLE,
            &RawTableHeader::system(
                STORAGE_KIND_BRANCH_ORD_BY_NAME,
                BRANCH_ORD_BY_NAME_FORMAT_V1,
            ),
        )?;
        ensure_raw_table_header(
            self,
            BRANCH_NAME_BY_ORD_TABLE,
            &RawTableHeader::system(
                STORAGE_KIND_BRANCH_NAME_BY_ORD,
                BRANCH_NAME_BY_ORD_FORMAT_V1,
            ),
        )?;
        ensure_raw_table_header(
            self,
            BRANCH_ORD_META_TABLE,
            &RawTableHeader::system(STORAGE_KIND_BRANCH_ORD_META, BRANCH_ORD_META_FORMAT_V1),
        )?;

        let mut next_ord = load_next_branch_ord(self)?.max(1);
        while self.load_branch_name_by_ord(next_ord)?.is_some() {
            next_ord = next_ord.saturating_add(1);
        }

        let branch_name_key = branch_ord_by_name_key(branch_name);
        let branch_ord_key = branch_name_by_ord_key(next_ord)?;
        let branch_ord_bytes = encode_branch_ord_value(next_ord)?;
        let branch_name_bytes = encode_branch_name_value(branch_name)?;
        let next_ord_bytes = encode_branch_ord_meta(next_ord.saturating_add(1))?;
        let mutations = [
            RawTableMutation::Put {
                table: BRANCH_ORD_BY_NAME_TABLE,
                key: branch_name_key.as_str(),
                value: &branch_ord_bytes,
            },
            RawTableMutation::Put {
                table: BRANCH_NAME_BY_ORD_TABLE,
                key: branch_ord_key.as_str(),
                value: &branch_name_bytes,
            },
            RawTableMutation::Put {
                table: BRANCH_ORD_META_TABLE,
                key: BRANCH_ORD_NEXT_ORD_KEY,
                value: &next_ord_bytes,
            },
        ];
        self.apply_raw_table_mutations(&mutations)?;
        Ok(next_ord)
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> Result<(), StorageError> {
        ensure_raw_table_header(
            self,
            "catalogue",
            &RawTableHeader::system(STORAGE_KIND_CATALOGUE, CATALOGUE_STORAGE_FORMAT_V1),
        )?;
        let bytes = entry
            .encode_storage_row()
            .map_err(|err| StorageError::IoError(format!("encode catalogue entry: {err}")))?;
        self.raw_table_put(
            "catalogue",
            &key_codec::catalogue_entry_key(entry.object_id),
            &bytes,
        )
    }

    fn load_catalogue_entry(
        &self,
        object_id: ObjectId,
    ) -> Result<Option<CatalogueEntry>, StorageError> {
        match self.raw_table_get("catalogue", &key_codec::catalogue_entry_key(object_id))? {
            Some(bytes) => {
                ensure_system_raw_table_header_validated_once(
                    self,
                    "catalogue",
                    STORAGE_KIND_CATALOGUE,
                    CATALOGUE_STORAGE_FORMAT_V1,
                )?;
                CatalogueEntry::decode_storage_row(object_id, &bytes)
                    .map(Some)
                    .map_err(|err| StorageError::IoError(format!("decode catalogue entry: {err}")))
            }
            None => Ok(None),
        }
    }

    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError> {
        let mut entries = Vec::new();
        for (key, bytes) in
            self.raw_table_scan_prefix("catalogue", key_codec::catalogue_entry_prefix())?
        {
            ensure_system_raw_table_header_validated_once(
                self,
                "catalogue",
                STORAGE_KIND_CATALOGUE,
                CATALOGUE_STORAGE_FORMAT_V1,
            )?;
            let Some(hex_id) = key.strip_prefix(key_codec::catalogue_entry_prefix()) else {
                continue;
            };
            let bytes_id = hex::decode(hex_id).map_err(|err| {
                StorageError::IoError(format!("invalid catalogue entry key '{key}': {err}"))
            })?;
            let uuid = uuid::Uuid::from_slice(&bytes_id).map_err(|err| {
                StorageError::IoError(format!("invalid catalogue entry uuid '{key}': {err}"))
            })?;
            let object_id = ObjectId::from_uuid(uuid);
            let entry = CatalogueEntry::decode_storage_row(object_id, &bytes)
                .map_err(|err| StorageError::IoError(format!("decode catalogue entry: {err}")))?;
            entries.push(entry);
        }
        entries.sort_by_key(|entry| entry.object_id);
        Ok(entries)
    }

    fn upsert_raw_table_header(
        &mut self,
        raw_table: &str,
        header: &RawTableHeader,
    ) -> Result<(), StorageError> {
        let bytes = encode_raw_table_header(header)?;
        self.raw_table_put(RAW_TABLE_HEADER_TABLE, raw_table, &bytes)?;
        cache_raw_table_header_with_storage(self, raw_table, header.clone());
        invalidate_validated_raw_table_with_storage(self, raw_table);
        Ok(())
    }

    fn load_raw_table_header(
        &self,
        raw_table: &str,
    ) -> Result<Option<RawTableHeader>, StorageError> {
        if let Some(header) = cached_raw_table_header_with_storage(self, raw_table) {
            if !raw_table_validated_with_storage(self, raw_table) {
                validate_raw_table_header_storage_format(raw_table, &header)?;
            }
            return Ok(Some(header));
        }

        let header = self
            .raw_table_get(RAW_TABLE_HEADER_TABLE, raw_table)?
            .map(|bytes| {
                let header = decode_raw_table_header(&bytes)?;
                validate_raw_table_header_storage_format(raw_table, &header)?;
                Ok(header)
            })
            .transpose()?;
        if let Some(header) = header.as_ref() {
            cache_raw_table_header_with_storage(self, raw_table, header.clone());
        }
        Ok(header)
    }

    fn scan_raw_table_headers(&self) -> Result<Vec<(String, RawTableHeader)>, StorageError> {
        let mut rows = Vec::new();
        for (key, bytes) in self.raw_table_scan_prefix(RAW_TABLE_HEADER_TABLE, "")? {
            let header = decode_raw_table_header(&bytes)?;
            validate_raw_table_header_storage_format(&key, &header)?;
            cache_raw_table_header_with_storage(self, &key, header.clone());
            rows.push((key, header));
        }
        rows.sort_by(|(left, _), (right, _)| left.cmp(right));
        Ok(rows)
    }

    fn upsert_local_batch_record(&mut self, record: &LocalBatchRecord) -> Result<(), StorageError> {
        if let Some(submission) = record.sealed_submission.as_ref() {
            self.upsert_sealed_batch_submission(submission)?;
        }
        if let Some(settlement) = record.latest_settlement.as_ref() {
            self.upsert_authoritative_batch_settlement(settlement)?;
        }
        ensure_raw_table_header(
            self,
            LOCAL_BATCH_RECORD_TABLE,
            &RawTableHeader::system(
                STORAGE_KIND_LOCAL_BATCH_RECORD,
                LOCAL_BATCH_RECORD_FORMAT_V3,
            ),
        )?;
        let bytes = encode_local_batch_record_with_branch_ords(self, record)?;
        self.raw_table_put(
            LOCAL_BATCH_RECORD_TABLE,
            &local_batch_record_key(record.batch_id),
            &bytes,
        )
    }

    fn load_local_batch_record(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<LocalBatchRecord>, StorageError> {
        match self.raw_table_get(LOCAL_BATCH_RECORD_TABLE, &local_batch_record_key(batch_id))? {
            Some(bytes) => {
                ensure_system_raw_table_header_validated_once(
                    self,
                    LOCAL_BATCH_RECORD_TABLE,
                    STORAGE_KIND_LOCAL_BATCH_RECORD,
                    LOCAL_BATCH_RECORD_FORMAT_V3,
                )?;
                decode_local_batch_record_with_branch_ords(self, &bytes).map(Some)
            }
            None => Ok(None),
        }
    }

    fn delete_local_batch_record(&mut self, batch_id: BatchId) -> Result<(), StorageError> {
        self.raw_table_delete(LOCAL_BATCH_RECORD_TABLE, &local_batch_record_key(batch_id))
    }

    fn scan_local_batch_records(&self) -> Result<Vec<LocalBatchRecord>, StorageError> {
        let mut records = Vec::new();
        for (key, bytes) in self.raw_table_scan_prefix(LOCAL_BATCH_RECORD_TABLE, "batch:")? {
            ensure_system_raw_table_header_validated_once(
                self,
                LOCAL_BATCH_RECORD_TABLE,
                STORAGE_KIND_LOCAL_BATCH_RECORD,
                LOCAL_BATCH_RECORD_FORMAT_V3,
            )?;
            let batch_id = decode_local_batch_record_key(&key)?;
            let record = decode_local_batch_record_with_branch_ords(self, &bytes)?;
            if record.batch_id != batch_id {
                return Err(StorageError::IoError(format!(
                    "local batch record key/row mismatch for {key}"
                )));
            }
            records.push(record);
        }
        records.sort_by_key(|record| record.batch_id);
        Ok(records)
    }

    fn upsert_sealed_batch_submission(
        &mut self,
        submission: &SealedBatchSubmission,
    ) -> Result<(), StorageError> {
        ensure_raw_table_header(
            self,
            SEALED_BATCH_SUBMISSION_TABLE,
            &RawTableHeader::system(
                STORAGE_KIND_SEALED_BATCH_SUBMISSION,
                SEALED_BATCH_SUBMISSION_FORMAT_V2,
            ),
        )?;
        let bytes = encode_sealed_batch_submission_with_branch_ords(self, submission)?;
        self.raw_table_put(
            SEALED_BATCH_SUBMISSION_TABLE,
            &local_batch_record_key(submission.batch_id),
            &bytes,
        )
    }

    fn load_sealed_batch_submission(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<SealedBatchSubmission>, StorageError> {
        match self.raw_table_get(
            SEALED_BATCH_SUBMISSION_TABLE,
            &local_batch_record_key(batch_id),
        )? {
            Some(bytes) => {
                ensure_system_raw_table_header_validated_once(
                    self,
                    SEALED_BATCH_SUBMISSION_TABLE,
                    STORAGE_KIND_SEALED_BATCH_SUBMISSION,
                    SEALED_BATCH_SUBMISSION_FORMAT_V2,
                )?;
                decode_sealed_batch_submission_with_branch_ords(self, &bytes).map(Some)
            }
            None => Ok(None),
        }
    }

    fn delete_sealed_batch_submission(&mut self, batch_id: BatchId) -> Result<(), StorageError> {
        self.raw_table_delete(
            SEALED_BATCH_SUBMISSION_TABLE,
            &local_batch_record_key(batch_id),
        )
    }

    fn scan_sealed_batch_submissions(&self) -> Result<Vec<SealedBatchSubmission>, StorageError> {
        let mut submissions = Vec::new();
        for (key, bytes) in self.raw_table_scan_prefix(SEALED_BATCH_SUBMISSION_TABLE, "batch:")? {
            ensure_system_raw_table_header_validated_once(
                self,
                SEALED_BATCH_SUBMISSION_TABLE,
                STORAGE_KIND_SEALED_BATCH_SUBMISSION,
                SEALED_BATCH_SUBMISSION_FORMAT_V2,
            )?;
            let batch_id = decode_local_batch_record_key(&key)?;
            let submission = decode_sealed_batch_submission_with_branch_ords(self, &bytes)?;
            if submission.batch_id != batch_id {
                return Err(StorageError::IoError(format!(
                    "sealed batch submission key/row mismatch for {key}"
                )));
            }
            submissions.push(submission);
        }
        submissions.sort_by_key(|submission| submission.batch_id);
        Ok(submissions)
    }

    fn upsert_authoritative_batch_settlement(
        &mut self,
        settlement: &BatchSettlement,
    ) -> Result<(), StorageError> {
        let settlement = match self.load_authoritative_batch_settlement(settlement.batch_id())? {
            Some(existing) => existing.merged_with(settlement),
            None => settlement.clone(),
        };
        ensure_raw_table_header(
            self,
            AUTHORITATIVE_BATCH_SETTLEMENT_TABLE,
            &RawTableHeader::system(
                STORAGE_KIND_AUTHORITATIVE_BATCH_SETTLEMENT,
                AUTHORITATIVE_BATCH_SETTLEMENT_FORMAT_V2,
            ),
        )?;
        let bytes = settlement.encode_storage_row().map_err(|err| {
            StorageError::IoError(format!("encode authoritative batch settlement: {err}"))
        })?;
        self.raw_table_put(
            AUTHORITATIVE_BATCH_SETTLEMENT_TABLE,
            &local_batch_record_key(settlement.batch_id()),
            &bytes,
        )
    }

    fn load_authoritative_batch_settlement(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchSettlement>, StorageError> {
        match self.raw_table_get(
            AUTHORITATIVE_BATCH_SETTLEMENT_TABLE,
            &local_batch_record_key(batch_id),
        )? {
            Some(bytes) => {
                ensure_system_raw_table_header_validated_once(
                    self,
                    AUTHORITATIVE_BATCH_SETTLEMENT_TABLE,
                    STORAGE_KIND_AUTHORITATIVE_BATCH_SETTLEMENT,
                    AUTHORITATIVE_BATCH_SETTLEMENT_FORMAT_V2,
                )?;
                BatchSettlement::decode_storage_row(&bytes)
                    .map(Some)
                    .map_err(|err| {
                        StorageError::IoError(format!(
                            "decode authoritative batch settlement: {err}"
                        ))
                    })
            }
            None => Ok(None),
        }
    }

    fn scan_authoritative_batch_settlements(&self) -> Result<Vec<BatchSettlement>, StorageError> {
        let mut settlements = Vec::new();
        for (key, bytes) in
            self.raw_table_scan_prefix(AUTHORITATIVE_BATCH_SETTLEMENT_TABLE, "batch:")?
        {
            ensure_system_raw_table_header_validated_once(
                self,
                AUTHORITATIVE_BATCH_SETTLEMENT_TABLE,
                STORAGE_KIND_AUTHORITATIVE_BATCH_SETTLEMENT,
                AUTHORITATIVE_BATCH_SETTLEMENT_FORMAT_V2,
            )?;
            let batch_id = decode_local_batch_record_key(&key)?;
            let settlement = BatchSettlement::decode_storage_row(&bytes).map_err(|err| {
                StorageError::IoError(format!("decode authoritative batch settlement: {err}"))
            })?;
            if settlement.batch_id() != batch_id {
                return Err(StorageError::IoError(format!(
                    "authoritative batch settlement key/row mismatch for {key}"
                )));
            }
            settlements.push(settlement);
        }
        settlements.sort_by_key(|settlement| settlement.batch_id().0);
        Ok(settlements)
    }

    // ================================================================
    // Row-history storage
    // ================================================================

    fn append_history_region_row_bytes(
        &mut self,
        _table: &str,
        _rows: &[HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "raw row-history appends are not implemented for this backend yet".to_string(),
        ))
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(
            load_history_row_batch_row_bytes_with_storage(self, table, branch, row_id, batch_id)?
                .map(|row| row.bytes),
        )
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        Ok(scan_history_row_bytes_with_storage(self, table, scan)?
            .into_iter()
            .map(|row| row.bytes)
            .collect())
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowBatch],
    ) -> Result<(), StorageError> {
        let encoded_rows = encode_history_row_bytes_for_storage(self, table, rows)?;
        self.apply_encoded_row_mutation(table, &encoded_rows, &[], &[])
    }

    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[OwnedHistoryRowBytes],
        visible_rows: &[OwnedVisibleRowBytes],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        let mut seen_row_raw_tables = HashSet::new();
        for row in history_rows {
            if seen_row_raw_tables.insert(row.row_raw_table.clone()) {
                ensure_raw_table_header(
                    self,
                    row.row_raw_table.as_str(),
                    &row_raw_table_header(&row.row_raw_table_id, &row.user_descriptor),
                )?;
            }
        }
        for row in visible_rows {
            if seen_row_raw_tables.insert(row.row_raw_table.clone()) {
                ensure_raw_table_header(
                    self,
                    row.row_raw_table.as_str(),
                    &row_raw_table_header(&row.row_raw_table_id, &row.user_descriptor),
                )?;
            }
        }
        if history_rows.iter().any(|row| row.needs_exact_locator) {
            ensure_raw_table_header(
                self,
                HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE,
                &RawTableHeader::system(STORAGE_KIND_HISTORY_ROW_BATCH_TABLE_LOCATOR, 1),
            )?;
            for row in history_rows {
                if !row.needs_exact_locator {
                    continue;
                }
                let bytes = encode_exact_row_table_locator(&ExactRowTableLocator {
                    row_raw_table: row.row_raw_table.clone().into(),
                    table_name: row.row_raw_table_id.table_name.clone(),
                    schema_hash: row.row_raw_table_id.schema_hash,
                })?;
                self.raw_table_put(
                    HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE,
                    &history_row_batch_table_locator_key(
                        row.row_id,
                        row.branch.as_str(),
                        row.batch_id,
                    ),
                    &bytes,
                )?;
            }
        }
        if visible_rows.iter().any(|row| row.needs_exact_locator) {
            ensure_raw_table_header(
                self,
                VISIBLE_ROW_TABLE_LOCATOR_TABLE,
                &RawTableHeader::system(STORAGE_KIND_VISIBLE_ROW_TABLE_LOCATOR, 1),
            )?;
            for row in visible_rows {
                if !row.needs_exact_locator {
                    continue;
                }
                let bytes = encode_exact_row_table_locator(&ExactRowTableLocator {
                    row_raw_table: row.row_raw_table.clone().into(),
                    table_name: row.row_raw_table_id.table_name.clone(),
                    schema_hash: row.row_raw_table_id.schema_hash,
                })?;
                self.raw_table_put(
                    VISIBLE_ROW_TABLE_LOCATOR_TABLE,
                    &visible_row_table_locator_key(row.branch.as_str(), row.row_id),
                    &bytes,
                )?;
            }
        }
        if !history_rows.is_empty() {
            let borrowed_rows = history_rows
                .iter()
                .map(|row| HistoryRowBytes {
                    row_raw_table: row.row_raw_table.as_str(),
                    branch: row.branch.as_str(),
                    row_id: row.row_id,
                    batch_id: row.batch_id,
                    bytes: &row.bytes,
                })
                .collect::<Vec<_>>();
            self.append_history_region_row_bytes(table, &borrowed_rows)?;
        }
        if !visible_rows.is_empty() {
            let borrowed_rows = visible_rows
                .iter()
                .map(|row| VisibleRowBytes {
                    row_raw_table: row.row_raw_table.as_str(),
                    branch: row.branch.as_str(),
                    row_id: row.row_id,
                    bytes: &row.bytes,
                })
                .collect::<Vec<_>>();
            self.upsert_visible_region_row_bytes(table, &borrowed_rows)?;
        }
        if !index_mutations.is_empty() {
            self.apply_index_mutations(index_mutations)?;
        }
        Ok(())
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
        let _ = history_rows;
        let _ = visible_entries;
        self.apply_encoded_row_mutation(
            table,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
    }

    fn upsert_visible_region_row_bytes(
        &mut self,
        _table: &str,
        _rows: &[VisibleRowBytes<'_>],
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "raw visible-row upserts are not implemented for this backend yet".to_string(),
        ))
    }

    fn load_visible_region_row_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        Ok(
            load_visible_region_row_bytes_with_storage(self, table, branch, row_id)?
                .map(|row| row.bytes),
        )
    }

    fn scan_visible_region_bytes(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        Ok(scan_visible_row_bytes_with_storage(self, table, branch)?
            .into_iter()
            .map(|row| row.bytes)
            .collect())
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        let encoded_rows = encode_visible_row_bytes_for_storage(self, table, entries)?;
        self.apply_encoded_row_mutation(table, &[], &encoded_rows, &[])
    }

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = key_codec::visible_row_raw_table_key(branch, row_id);
        if let Some(locator) =
            exact_visible_row_table_locator_for_delete(self, table, branch, row_id)?
        {
            self.raw_table_delete(locator.row_raw_table.as_str(), &key)?;
        }
        self.put_visible_row_table_locator(branch, row_id, None)?;
        Ok(())
    }

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        patch_exact_row_batch_with_storage(
            self,
            table,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        _table: &str,
        _batch_id: crate::row_histories::BatchId,
        _state: Option<RowState>,
        _confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        Err(StorageError::IoError(
            "row-history patching is not implemented for this backend yet".to_string(),
        ))
    }

    fn apply_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowBatch],
        visible_entries: &[VisibleRowEntry],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        let encoded_history_rows = encode_history_row_bytes_for_storage(self, table, history_rows)?;
        let encoded_visible_rows =
            encode_visible_row_bytes_for_storage(self, table, visible_entries)?;
        self.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            &encoded_history_rows,
            &encoded_visible_rows,
            index_mutations,
        )
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let resolved_tables = resolved_row_tables_for_table(self, RowRawTableKind::Visible, table)?;
        let prefix = key_codec::visible_row_raw_table_prefix(branch);
        let mut rows = Vec::new();
        for resolved in &resolved_tables {
            for (key, bytes) in self.raw_table_scan_prefix(&resolved.row_raw_table, &prefix)? {
                let (decoded_branch, row_id) = key_codec::decode_visible_row_raw_table_key(&key)?;
                if decoded_branch != branch {
                    return Err(StorageError::IoError(format!(
                        "visible row raw table key '{key}' decoded unexpected branch '{decoded_branch}'"
                    )));
                }
                rows.push(
                    decode_visible_row_entry_bytes_in_table(
                        resolved,
                        row_id,
                        decoded_branch.as_str(),
                        &bytes,
                    )?
                    .current_row,
                );
            }
        }
        rows.sort_by_key(|row| (row.branch.clone(), row.row_id));
        Ok(rows)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        Ok(self
            .load_visible_region_entry(table, branch, row_id)?
            .map(|entry| entry.current_row))
    }

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self
            .load_visible_region_row(table, branch, row_id)?
            .as_ref()
            .map(QueryRowBatch::from))
    }

    fn load_visible_region_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        let Some(row) = load_visible_region_row_bytes_with_storage(self, table, branch, row_id)?
        else {
            return Ok(None);
        };
        let entry = crate::row_histories::decode_flat_visible_row_entry(
            row.user_descriptor.as_ref(),
            row_id,
            branch,
            &row.bytes,
        )
        .map_err(|err| StorageError::IoError(format!("decode flat visible row: {err}")))?;
        let current_tier = row_confirmed_tier_with_batch_settlement(self, &entry.current_row)?;
        if current_tier.is_some_and(|tier| tier >= required_tier) {
            let mut current_row = entry.current_row.clone();
            current_row.confirmed_tier = current_tier;
            return Ok(Some(current_row));
        }
        let mut history_rows =
            self.scan_history_region(table, branch, HistoryScan::Row { row_id })?;
        apply_batch_settlement_tiers_to_rows(self, &mut history_rows)?;
        crate::row_histories::visible_row_preview_from_history_rows(
            row.user_descriptor.as_ref(),
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

    fn load_visible_region_entry(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<VisibleRowEntry>, StorageError> {
        load_visible_region_row_bytes_with_storage(self, table, branch, row_id)?
            .map(|row| {
                let user_descriptor = row.user_descriptor;
                let row_codecs = crate::row_histories::flat_row_codecs(&user_descriptor);
                let resolved = ResolvedRowTable {
                    row_raw_table: row.row_raw_table,
                    user_descriptor,
                    row_codecs,
                };
                decode_visible_row_entry_bytes_in_table(&resolved, row_id, branch, &row.bytes)
            })
            .transpose()
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<BatchId>>, StorageError> {
        Ok(self
            .load_visible_region_entry(table, branch, row_id)?
            .map(|entry| entry.branch_frontier))
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: BranchName,
    ) -> Result<Vec<CapturedFrontierMember>, StorageError> {
        let mut frontier = Vec::new();
        let visible_tables = scan_row_raw_table_headers_with_storage(self)?
            .into_iter()
            .filter(|(row_raw_table_id, _)| row_raw_table_id.kind == RowRawTableKind::Visible)
            .map(|(row_raw_table_id, header)| {
                resolved_row_table_from_header(self, row_raw_table_id, header)
            })
            .collect::<Result<Vec<_>, _>>()?;
        for resolved in visible_tables {
            for (key, bytes) in self.raw_table_scan_prefix(&resolved.row_raw_table, "")? {
                let (branch, row_id) = key_codec::decode_visible_row_raw_table_key(&key)?;
                let branch_name = BranchName::new(&branch);
                if !branch_matches_transaction_family(branch_name, target_branch_name) {
                    continue;
                }
                let entry = decode_visible_row_entry_bytes_in_table(
                    &resolved,
                    row_id,
                    branch.as_str(),
                    &bytes,
                )?;
                frontier.push(CapturedFrontierMember {
                    object_id: entry.current_row.row_id,
                    branch_name,
                    batch_id: entry.current_row.batch_id(),
                });
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

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        load_history_row_batch_row_bytes_with_storage(self, table, branch, row_id, batch_id)?
            .map(|row| {
                let user_descriptor = row.user_descriptor;
                let row_codecs = crate::row_histories::flat_row_codecs(&user_descriptor);
                let resolved = ResolvedRowTable {
                    row_raw_table: row.row_raw_table,
                    user_descriptor,
                    row_codecs,
                };
                decode_history_row_bytes_in_table(&resolved, row_id, branch, batch_id, &row.bytes)
            })
            .transpose()
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        let row_raw_table_id = history_row_raw_table_id(table, schema_hash);
        let Some(resolved) = resolved_row_table_from_id(self, row_raw_table_id)? else {
            return Ok(None);
        };
        let key = key_codec::history_row_raw_table_key(row_id, branch, batch_id);
        self.raw_table_get(&resolved.row_raw_table, &key)?
            .map(|bytes| {
                decode_history_row_bytes_in_table(&resolved, row_id, branch, batch_id, &bytes)
            })
            .transpose()
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self
            .load_history_row_batch(table, branch, row_id, batch_id)?
            .as_ref()
            .map(QueryRowBatch::from))
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        let mut matches = self
            .scan_history_row_batches(table, row_id)?
            .into_iter()
            .filter(|row| row.batch_id() == batch_id);
        let Some(first_match) = matches.next() else {
            return Ok(None);
        };
        if let Some(second_match) = matches.next() {
            return Err(StorageError::IoError(format!(
                "ambiguous row history version {batch_id:?} for row {row_id}: found branches {} and {}",
                first_match.branch, second_match.branch
            )));
        }
        Ok(Some(first_match))
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        Ok(self
            .load_history_row_batch_any_branch(table, row_id, batch_id)?
            .as_ref()
            .map(QueryRowBatch::from))
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<bool, StorageError> {
        Ok(self
            .load_history_row_batch(table, branch, row_id, batch_id)?
            .is_some())
    }

    #[allow(clippy::too_many_arguments)]
    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        let Some(mut current_row) = self.load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )?
        else {
            return Ok(false);
        };

        if let Some(state) = state {
            current_row.state = state;
        }
        if let Some(confirmed_tier) = confirmed_tier {
            current_row.confirmed_tier = Some(match current_row.confirmed_tier {
                Some(existing) => existing.max(confirmed_tier),
                None => confirmed_tier,
            });
        }

        let history_rows =
            scan_history_row_batches_for_schema_hash(self, table, schema_hash, row_id)?;
        let mut patched_history = history_rows.clone();
        if let Some(existing) = patched_history
            .iter_mut()
            .find(|row| row.branch == branch && row.row_id == row_id && row.batch_id() == batch_id)
        {
            *existing = current_row.clone();
        }
        let context = prepared_row_write_context_for_schema_hash(
            self,
            table,
            schema_hash,
            current_row.row_id,
        )?;
        let visible_entries = VisibleRowEntry::rebuild_with_descriptor(
            context.user_descriptor.as_ref(),
            &patched_history,
        )
        .map_err(|err| StorageError::IoError(format!("rebuild visible entry: {err}")))?
        .into_iter()
        .collect::<Vec<_>>();
        let encoded_history_rows = vec![encode_history_row_bytes_with_context(
            &context,
            &current_row,
        )?];
        let encoded_visible_rows = visible_entries
            .iter()
            .map(|entry| encode_visible_row_bytes_with_context(&context, entry))
            .collect::<Result<Vec<_>, _>>()?;

        self.apply_prepared_row_mutation(
            table,
            std::slice::from_ref(&current_row),
            &visible_entries,
            &encoded_history_rows,
            &encoded_visible_rows,
            &[],
        )?;
        if visible_entries.is_empty() {
            self.delete_visible_region_row(table, branch, row_id)?;
        }

        Ok(true)
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<BatchId>, StorageError> {
        if let Some(frontier) = self.load_visible_region_frontier(table, branch, row_id)? {
            return Ok(frontier);
        }

        let branch_rows = self
            .scan_history_row_batches(table, row_id)?
            .into_iter()
            .filter(|row| row.branch == branch)
            .collect::<Vec<_>>();

        let mut non_tips = SmolSet::<[BatchId; 2]>::new();
        for row in &branch_rows {
            for parent in &row.parents {
                non_tips.insert(*parent);
            }
        }

        let mut tips: Vec<_> = branch_rows
            .into_iter()
            .map(|row| row.batch_id())
            .filter(|batch_id| !non_tips.contains(batch_id))
            .collect();
        tips.sort();
        tips.dedup();
        Ok(tips)
    }

    fn scan_visible_region_row_batches(
        &self,
        _table: &str,
        _row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        Err(StorageError::IoError(
            "visible-row history scans are not implemented for this backend yet".to_string(),
        ))
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let resolved_tables = resolved_row_tables_for_table(self, RowRawTableKind::History, table)?;
        let prefix = key_codec::history_row_raw_table_prefix(Some(row_id));
        let mut rows = Vec::new();
        for resolved in &resolved_tables {
            for (key, bytes) in self.raw_table_scan_prefix(&resolved.row_raw_table, &prefix)? {
                let (decoded_row_id, branch, batch_id) =
                    key_codec::decode_history_row_raw_table_key(&key)?;
                rows.push(decode_history_row_bytes_in_table(
                    resolved,
                    decoded_row_id,
                    branch.as_str(),
                    batch_id,
                    &bytes,
                )?);
            }
        }
        rows.sort_by_key(|row| (row.branch.clone(), row.updated_at, row.batch_id()));
        Ok(rows)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        let resolved_tables = resolved_row_tables_for_table(self, RowRawTableKind::History, table)?;
        let prefix = match scan {
            HistoryScan::Branch | HistoryScan::AsOf { .. } => {
                key_codec::history_row_raw_table_prefix(None)
            }
            HistoryScan::Row { row_id } => key_codec::history_row_raw_table_prefix(Some(row_id)),
        };
        let mut scanned = Vec::new();
        for resolved in &resolved_tables {
            for (key, bytes) in self.raw_table_scan_prefix(&resolved.row_raw_table, &prefix)? {
                let (row_id, decoded_branch, batch_id) =
                    key_codec::decode_history_row_raw_table_key(&key)?;
                scanned.push(decode_history_row_bytes_in_table(
                    resolved,
                    row_id,
                    decoded_branch.as_str(),
                    batch_id,
                    &bytes,
                )?);
            }
        }

        let mut rows: Vec<StoredRowBatch> = match scan {
            HistoryScan::Branch | HistoryScan::Row { .. } => scanned
                .into_iter()
                .filter(|row| row.branch == branch)
                .collect(),
            HistoryScan::AsOf { ts } => {
                let mut latest_per_row: BTreeMap<ObjectId, StoredRowBatch> = BTreeMap::new();
                for row in scanned {
                    if row.branch != branch || row.updated_at > ts || !row.state.is_visible() {
                        continue;
                    }
                    match latest_per_row.get(&row.row_id) {
                        Some(existing)
                            if (existing.updated_at, existing.batch_id())
                                >= (row.updated_at, row.batch_id()) => {}
                        _ => {
                            latest_per_row.insert(row.row_id, row);
                        }
                    }
                }
                latest_per_row.into_values().collect()
            }
        };
        rows.sort_by_key(|row| (row.branch.clone(), row.updated_at, row.batch_id()));
        Ok(rows)
    }

    // ================================================================
    // Index operations (built on ordered raw tables)
    // ================================================================

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let raw_table = key_codec::index_raw_table(table, column, branch);
        let key = key_codec::index_entry_key(table, column, branch, value, row_id)?;
        self.raw_table_put(&raw_table, &key, &[0x01])
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        let key = match key_codec::index_entry_key(table, column, branch, value, row_id) {
            Ok(key) => key,
            Err(StorageError::IndexKeyTooLarge { .. }) => return Ok(()),
            Err(error) => return Err(error),
        };
        let raw_table = key_codec::index_raw_table(table, column, branch);
        self.raw_table_delete(&raw_table, &key)
    }

    fn apply_index_mutations(
        &mut self,
        mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        for mutation in mutations {
            match mutation {
                IndexMutation::Insert {
                    table,
                    column,
                    branch,
                    value,
                    row_id,
                } => self.index_insert(table, column, branch, value, *row_id)?,
                IndexMutation::Remove {
                    table,
                    column,
                    branch,
                    value,
                    row_id,
                } => self.index_remove(table, column, branch, value, *row_id)?,
            }
        }
        Ok(())
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        let raw_table = key_codec::index_raw_table(table, column, branch);
        if is_double_zero(value) {
            let mut result = HashSet::new();
            for zero in &[Value::Double(0.0), Value::Double(-0.0)] {
                let Ok(prefix) = key_codec::index_value_prefix(table, column, branch, zero) else {
                    continue;
                };
                if let Ok(keys) = self.raw_table_scan_prefix_keys(&raw_table, &prefix) {
                    for key in keys {
                        if let Some(id) = key_codec::parse_uuid_from_index_key(&key) {
                            result.insert(id);
                        }
                    }
                }
            }
            return result.into_iter().collect();
        }

        let Ok(prefix) = key_codec::index_value_prefix(table, column, branch, value) else {
            return Vec::new();
        };
        self.raw_table_scan_prefix_keys(&raw_table, &prefix)
            .map(|keys| {
                keys.into_iter()
                    .filter_map(|key| key_codec::parse_uuid_from_index_key(&key))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        let raw_table = key_codec::index_raw_table(table, column, branch);
        let Some((start_key, end_key)) =
            key_codec::index_range_scan_bounds(table, column, branch, start, end)
        else {
            return Vec::new();
        };

        self.raw_table_scan_range_keys(&raw_table, start_key.as_deref(), end_key.as_deref())
            .map(|keys| {
                keys.into_iter()
                    .filter_map(|key| key_codec::parse_uuid_from_index_key(&key))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        let raw_table = key_codec::index_raw_table(table, column, branch);
        self.raw_table_scan_prefix_keys(&raw_table, "")
            .map(|keys| {
                keys.into_iter()
                    .filter_map(|key| key_codec::parse_uuid_from_index_key(&key))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Flush buffered data to persistent storage. No-op for in-memory storage.
    fn flush(&self) {}

    /// Flush only the WAL buffer (not the snapshot). No-op for storage without WAL.
    fn flush_wal(&self) {}

    /// Close and release storage resources (e.g. file locks). No-op by default.
    fn close(&self) -> Result<(), StorageError> {
        Ok(())
    }
}

// Box<Storage> is used to allow for dynamic dispatch of the Storage trait.
impl<T: Storage + ?Sized> Storage for Box<T> {
    fn storage_cache_namespace(&self) -> usize {
        (**self).storage_cache_namespace()
    }

    fn scan_row_locators(&self) -> Result<RowLocatorRows, StorageError> {
        (**self).scan_row_locators()
    }

    fn load_row_locator(&self, id: ObjectId) -> Result<Option<RowLocator>, StorageError> {
        (**self).load_row_locator(id)
    }

    fn put_row_locator(
        &mut self,
        id: ObjectId,
        locator: Option<&RowLocator>,
    ) -> Result<(), StorageError> {
        (**self).put_row_locator(id, locator)
    }

    fn raw_table_put(&mut self, table: &str, key: &str, value: &[u8]) -> Result<(), StorageError> {
        (**self).raw_table_put(table, key, value)
    }

    fn raw_table_delete(&mut self, table: &str, key: &str) -> Result<(), StorageError> {
        (**self).raw_table_delete(table, key)
    }

    fn apply_raw_table_mutations(
        &mut self,
        mutations: &[RawTableMutation<'_>],
    ) -> Result<(), StorageError> {
        (**self).apply_raw_table_mutations(mutations)
    }

    fn raw_table_get(&self, table: &str, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        (**self).raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableRows, StorageError> {
        (**self).raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_prefix_keys(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<RawTableKeys, StorageError> {
        (**self).raw_table_scan_prefix_keys(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableRows, StorageError> {
        (**self).raw_table_scan_range(table, start, end)
    }

    fn raw_table_scan_range_keys(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<RawTableKeys, StorageError> {
        (**self).raw_table_scan_range_keys(table, start, end)
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> Result<(), StorageError> {
        (**self).upsert_catalogue_entry(entry)
    }

    fn load_catalogue_entry(
        &self,
        object_id: ObjectId,
    ) -> Result<Option<CatalogueEntry>, StorageError> {
        (**self).load_catalogue_entry(object_id)
    }

    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError> {
        (**self).scan_catalogue_entries()
    }

    fn upsert_local_batch_record(&mut self, record: &LocalBatchRecord) -> Result<(), StorageError> {
        (**self).upsert_local_batch_record(record)
    }

    fn load_local_batch_record(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<LocalBatchRecord>, StorageError> {
        (**self).load_local_batch_record(batch_id)
    }

    fn delete_local_batch_record(&mut self, batch_id: BatchId) -> Result<(), StorageError> {
        (**self).delete_local_batch_record(batch_id)
    }

    fn scan_local_batch_records(&self) -> Result<Vec<LocalBatchRecord>, StorageError> {
        (**self).scan_local_batch_records()
    }

    fn upsert_sealed_batch_submission(
        &mut self,
        submission: &SealedBatchSubmission,
    ) -> Result<(), StorageError> {
        (**self).upsert_sealed_batch_submission(submission)
    }

    fn load_sealed_batch_submission(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<SealedBatchSubmission>, StorageError> {
        (**self).load_sealed_batch_submission(batch_id)
    }

    fn delete_sealed_batch_submission(&mut self, batch_id: BatchId) -> Result<(), StorageError> {
        (**self).delete_sealed_batch_submission(batch_id)
    }

    fn scan_sealed_batch_submissions(&self) -> Result<Vec<SealedBatchSubmission>, StorageError> {
        (**self).scan_sealed_batch_submissions()
    }

    fn upsert_authoritative_batch_settlement(
        &mut self,
        settlement: &BatchSettlement,
    ) -> Result<(), StorageError> {
        (**self).upsert_authoritative_batch_settlement(settlement)
    }

    fn load_authoritative_batch_settlement(
        &self,
        batch_id: BatchId,
    ) -> Result<Option<BatchSettlement>, StorageError> {
        (**self).load_authoritative_batch_settlement(batch_id)
    }

    fn scan_authoritative_batch_settlements(&self) -> Result<Vec<BatchSettlement>, StorageError> {
        (**self).scan_authoritative_batch_settlements()
    }

    fn append_history_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[HistoryRowBytes<'_>],
    ) -> Result<(), StorageError> {
        (**self).append_history_region_row_bytes(table, rows)
    }

    fn load_history_row_batch_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        (**self).load_history_row_batch_bytes(table, branch, row_id, batch_id)
    }

    fn scan_history_region_bytes(
        &self,
        table: &str,
        scan: HistoryScan,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        (**self).scan_history_region_bytes(table, scan)
    }

    fn append_history_region_rows(
        &mut self,
        table: &str,
        rows: &[StoredRowBatch],
    ) -> Result<(), StorageError> {
        (**self).append_history_region_rows(table, rows)
    }

    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[OwnedHistoryRowBytes],
        visible_rows: &[OwnedVisibleRowBytes],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        (**self).apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
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
        (**self).apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
    }

    fn upsert_visible_region_rows(
        &mut self,
        table: &str,
        entries: &[VisibleRowEntry],
    ) -> Result<(), StorageError> {
        (**self).upsert_visible_region_rows(table, entries)
    }

    fn delete_visible_region_row(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).delete_visible_region_row(table, branch, row_id)
    }

    fn upsert_visible_region_row_bytes(
        &mut self,
        table: &str,
        rows: &[VisibleRowBytes<'_>],
    ) -> Result<(), StorageError> {
        (**self).upsert_visible_region_row_bytes(table, rows)
    }

    fn load_visible_region_row_bytes(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<u8>>, StorageError> {
        (**self).load_visible_region_row_bytes(table, branch, row_id)
    }

    fn scan_visible_region_bytes(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<Vec<u8>>, StorageError> {
        (**self).scan_visible_region_bytes(table, branch)
    }

    fn patch_row_region_rows_by_batch(
        &mut self,
        table: &str,
        batch_id: crate::row_histories::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<(), StorageError> {
        (**self).patch_row_region_rows_by_batch(table, batch_id, state, confirmed_tier)
    }

    fn patch_exact_row_batch(
        &mut self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: crate::row_histories::BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        (**self).patch_exact_row_batch(table, branch, row_id, batch_id, state, confirmed_tier)
    }

    fn apply_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowBatch],
        visible_entries: &[VisibleRowEntry],
        index_mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        (**self).apply_row_mutation(table, history_rows, visible_entries, index_mutations)
    }

    fn scan_visible_region(
        &self,
        table: &str,
        branch: &str,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        (**self).scan_visible_region(table, branch)
    }

    fn load_visible_region_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_visible_region_row(table, branch, row_id)
    }

    fn load_visible_query_row(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        (**self).load_visible_query_row(table, branch, row_id)
    }

    fn load_visible_region_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_visible_region_row_for_tier(table, branch, row_id, required_tier)
    }

    fn load_visible_query_row_for_tier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        required_tier: DurabilityTier,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        (**self).load_visible_query_row_for_tier(table, branch, row_id, required_tier)
    }

    fn load_visible_region_frontier(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Option<Vec<BatchId>>, StorageError> {
        (**self).load_visible_region_frontier(table, branch, row_id)
    }

    fn capture_family_visible_frontier(
        &self,
        target_branch_name: BranchName,
    ) -> Result<Vec<CapturedFrontierMember>, StorageError> {
        (**self).capture_family_visible_frontier(target_branch_name)
    }

    fn scan_visible_region_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        (**self).scan_visible_region_row_batches(table, row_id)
    }

    fn scan_history_row_batches(
        &self,
        table: &str,
        row_id: ObjectId,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        (**self).scan_history_row_batches(table, row_id)
    }

    fn load_history_query_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        (**self).load_history_query_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn load_history_row_batch_for_schema_hash(
        &self,
        table: &str,
        schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_history_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
        )
    }

    fn load_history_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        (**self).load_history_row_batch_any_branch(table, row_id, batch_id)
    }

    fn load_history_query_row_batch_any_branch(
        &self,
        table: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<QueryRowBatch>, StorageError> {
        (**self).load_history_query_row_batch_any_branch(table, row_id, batch_id)
    }

    fn row_batch_exists(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<bool, StorageError> {
        (**self).row_batch_exists(table, branch, row_id, batch_id)
    }

    fn patch_exact_row_batch_for_schema_hash(
        &mut self,
        table: &str,
        schema_hash: SchemaHash,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Result<bool, StorageError> {
        (**self).patch_exact_row_batch_for_schema_hash(
            table,
            schema_hash,
            branch,
            row_id,
            batch_id,
            state,
            confirmed_tier,
        )
    }

    fn scan_row_branch_tip_ids(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
    ) -> Result<Vec<BatchId>, StorageError> {
        (**self).scan_row_branch_tip_ids(table, branch, row_id)
    }

    fn scan_history_region(
        &self,
        table: &str,
        branch: &str,
        scan: HistoryScan,
    ) -> Result<Vec<StoredRowBatch>, StorageError> {
        (**self).scan_history_region(table, branch, scan)
    }

    fn index_insert(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).index_insert(table, column, branch, value, row_id)
    }

    fn index_remove(
        &mut self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
        row_id: ObjectId,
    ) -> Result<(), StorageError> {
        (**self).index_remove(table, column, branch, value, row_id)
    }

    fn apply_index_mutations(
        &mut self,
        mutations: &[IndexMutation<'_>],
    ) -> Result<(), StorageError> {
        (**self).apply_index_mutations(mutations)
    }

    fn index_lookup(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        value: &Value,
    ) -> Vec<ObjectId> {
        (**self).index_lookup(table, column, branch, value)
    }

    fn index_range(
        &self,
        table: &str,
        column: &str,
        branch: &str,
        start: Bound<&Value>,
        end: Bound<&Value>,
    ) -> Vec<ObjectId> {
        (**self).index_range(table, column, branch, start, end)
    }

    fn index_scan_all(&self, table: &str, column: &str, branch: &str) -> Vec<ObjectId> {
        (**self).index_scan_all(table, column, branch)
    }

    fn flush(&self) {
        (**self).flush();
    }

    fn flush_wal(&self) {
        (**self).flush_wal();
    }

    fn close(&self) -> Result<(), StorageError> {
        (**self).close()
    }
}
