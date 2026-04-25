use super::*;
use crate::batch_fate::{BatchSettlement, SealedBatchSubmission, VisibleBatchMember};
use crate::metadata::MetadataKey;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::policy::Operation;
use crate::row_histories::{
    BatchId, RowState, RowVisibilityChange, StoredRowBatch, apply_row_batch, patch_row_batch_state,
};
use crate::storage::{Storage, metadata_from_row_locator};
use std::collections::{HashMap, HashSet};

struct AppliedRowBatch {
    metadata: HashMap<String, String>,
    row: StoredRowBatch,
    visibility_change: Option<RowVisibilityChange>,
}
impl SyncManager {
    fn validate_sealed_batch_submission(
        &self,
        submission: &SealedBatchSubmission,
    ) -> Result<BranchName, BatchSettlement> {
        if submission.members.is_empty() {
            return Err(BatchSettlement::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: "sealed transactional batch must declare at least one member".to_string(),
            });
        }

        if submission.batch_digest
            != SealedBatchSubmission::compute_batch_digest(&submission.members)
        {
            return Err(BatchSettlement::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: "sealed transactional batch digest does not match declared members"
                    .to_string(),
            });
        }

        Ok(submission.target_branch_name)
    }

    fn frontier_conflict_settlement(
        &self,
        batch_id: crate::row_histories::BatchId,
    ) -> BatchSettlement {
        BatchSettlement::Rejected {
            batch_id,
            code: "transaction_conflict".to_string(),
            reason: "family-visible frontier changed since batch was sealed".to_string(),
        }
    }

    fn validate_batch_rows_target_branch(
        &self,
        submission: &SealedBatchSubmission,
        batch_rows: &[(String, StoredRowBatch)],
    ) -> Result<(), BatchSettlement> {
        if batch_rows.iter().any(|(_, row)| {
            row.batch_id == submission.batch_id
                && row.branch.as_str() != submission.target_branch_name.as_str()
        }) {
            return Err(BatchSettlement::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: "sealed transactional batch rows must belong to the declared target branch"
                    .to_string(),
            });
        }

        Ok(())
    }

    fn validate_captured_frontier<H: Storage>(
        &self,
        storage: &H,
        submission: &SealedBatchSubmission,
    ) -> Result<(), BatchSettlement> {
        let current_frontier = storage
            .capture_family_visible_frontier(submission.target_branch_name)
            .map_err(|error| BatchSettlement::Rejected {
                batch_id: submission.batch_id,
                code: "invalid_batch_submission".to_string(),
                reason: format!("failed to capture family-visible frontier: {error}"),
            })?;
        if current_frontier != submission.captured_frontier {
            return Err(self.frontier_conflict_settlement(submission.batch_id));
        }

        Ok(())
    }

    fn persist_authoritative_batch_settlement<H: Storage>(
        &self,
        storage: &mut H,
        settlement: &BatchSettlement,
    ) -> Result<(), crate::storage::StorageError> {
        storage
            .upsert_authoritative_batch_settlement(settlement)
            .map_err(|error| {
                tracing::warn!(
                    batch_id = ?settlement.batch_id(),
                    %error,
                    "failed to persist authoritative batch settlement"
                );
                error
            })
    }

    fn persist_sealed_batch_submission<H: Storage>(
        &self,
        storage: &mut H,
        submission: &SealedBatchSubmission,
    ) -> Result<(), crate::storage::StorageError> {
        storage
            .upsert_sealed_batch_submission(submission)
            .map_err(|error| {
                tracing::warn!(
                    batch_id = ?submission.batch_id,
                    %error,
                    "failed to persist sealed batch submission"
                );
                error
            })
    }

    fn ensure_object_metadata<H: Storage>(
        &mut self,
        storage: &mut H,
        object_id: ObjectId,
        metadata: HashMap<String, String>,
    ) {
        let existing_row_locator = storage.load_row_locator(object_id).ok().flatten();
        if existing_row_locator.is_none()
            && let Some(row_locator) = crate::storage::row_locator_from_metadata(&metadata)
        {
            let _ = storage.put_row_locator(object_id, Some(&row_locator));
        }
    }

    fn row_metadata_from_payload<H: Storage>(
        &self,
        storage: &H,
        row: &StoredRowBatch,
        metadata: Option<&RowMetadata>,
    ) -> Option<HashMap<String, String>> {
        if let Some(metadata) = metadata {
            return Some(metadata.metadata.clone());
        }

        storage
            .load_row_locator(row.row_id)
            .ok()
            .flatten()
            .map(|locator| metadata_from_row_locator(&locator))
    }

    fn matches_replayed_row_batch(existing: &StoredRowBatch, incoming: &StoredRowBatch) -> bool {
        existing.row_id == incoming.row_id
            && existing.batch_id == incoming.batch_id
            && existing.branch == incoming.branch
            && existing.parents == incoming.parents
            && existing.updated_at == incoming.updated_at
            && existing.created_by == incoming.created_by
            && existing.created_at == incoming.created_at
            && existing.updated_by == incoming.updated_by
            && existing.state == incoming.state
            && existing.delete_kind == incoming.delete_kind
            && existing.is_deleted == incoming.is_deleted
            && existing.data == incoming.data
            && existing.metadata == incoming.metadata
    }

    fn pre_batch_visible_row<H: Storage>(
        &self,
        storage: &H,
        table: &str,
        row: &StoredRowBatch,
    ) -> Option<StoredRowBatch> {
        if row.parents.is_empty() {
            return None;
        }

        let context =
            crate::storage::resolve_history_row_write_context(storage, table, row).ok()?;
        let history_rows = storage.scan_history_row_batches(table, row.row_id).ok()?;
        let visible_rows = history_rows
            .into_iter()
            .filter(|candidate| {
                candidate.branch.as_str() == row.branch.as_str()
                    && candidate.batch_id != row.batch_id
                    && candidate.state.is_visible()
            })
            .collect::<Vec<_>>();
        let visible_rows_by_batch = visible_rows
            .iter()
            .cloned()
            .map(|candidate| (candidate.batch_id(), candidate))
            .collect::<HashMap<_, _>>();

        let mut included_batch_ids = HashSet::new();
        let mut frontier = row.parents.iter().copied().collect::<Vec<_>>();
        while let Some(batch_id) = frontier.pop() {
            if !included_batch_ids.insert(batch_id) {
                continue;
            }
            if let Some(parent_row) = visible_rows_by_batch.get(&batch_id) {
                frontier.extend(parent_row.parents.iter().copied());
            }
        }

        let pre_batch_rows = visible_rows
            .into_iter()
            .filter(|candidate| included_batch_ids.contains(&candidate.batch_id()))
            .collect::<Vec<_>>();
        crate::row_histories::visible_row_preview_from_history_rows(
            context.user_descriptor.as_ref(),
            &pre_batch_rows,
            None,
        )
        .ok()
        .flatten()
    }

    fn apply_row_updated<H: Storage>(
        &mut self,
        storage: &mut H,
        metadata: Option<RowMetadata>,
        mut row: StoredRowBatch,
    ) -> Option<AppliedRowBatch> {
        if let Some(local_tier) = self.max_local_durability_tier() {
            row.confirmed_tier = Some(match row.confirmed_tier {
                Some(existing) => existing.max(local_tier),
                None => local_tier,
            });
        }

        let metadata = self.row_metadata_from_payload(storage, &row, metadata.as_ref())?;
        self.ensure_object_metadata(storage, row.row_id, metadata.clone());
        let branch_name = BranchName::new(&row.branch);
        let visibility_change =
            match apply_row_batch(storage, row.row_id, &branch_name, row.clone(), &[]) {
                Ok(applied) => applied.visibility_change,
                Err(err) => {
                    tracing::warn!(
                        row_id = %row.row_id,
                        %branch_name,
                        ?err,
                        "failed to apply synced row batch"
                    );
                    return None;
                }
            };

        Some(AppliedRowBatch {
            metadata,
            row,
            visibility_change,
        })
    }

    fn apply_row_batch_state_changed<H: Storage>(
        &mut self,
        storage: &mut H,
        row_id: ObjectId,
        branch_name: BranchName,
        batch_id: BatchId,
        state: Option<RowState>,
        confirmed_tier: Option<DurabilityTier>,
    ) {
        if confirmed_tier.is_none() && state.is_none() {
            return;
        }

        let row_update = match patch_row_batch_state(
            storage,
            row_id,
            &branch_name,
            batch_id,
            state,
            confirmed_tier,
        ) {
            Ok(update) => update,
            Err(err) => {
                tracing::error!(
                    %row_id,
                    %branch_name,
                    ?batch_id,
                    ?state,
                    ?confirmed_tier,
                    ?err,
                    "failed to apply row batch state change"
                );
                return;
            }
        };

        if let Some(tier) = confirmed_tier {
            self.received_row_batch_acks
                .push((RowBatchKey::new(row_id, branch_name, batch_id), tier));
        }

        if let Some(update) = row_update {
            self.pending_row_visibility_changes.push(update);
        }
    }

    fn replayable_visible_batch_settlement<H: Storage>(
        &self,
        storage: &H,
        row_id: ObjectId,
        branch_name: BranchName,
    ) -> Option<BatchSettlement> {
        let row_locator = storage.load_row_locator(row_id).ok().flatten()?;
        self.load_current_batch_settlement_from_storage(storage, row_id, &branch_name, &row_locator)
    }

    fn settlement_for_row_batch_state_change<H: Storage>(
        &self,
        storage: &H,
        row_id: ObjectId,
        branch_name: BranchName,
        batch_id: BatchId,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Option<BatchSettlement> {
        self.replayable_visible_batch_settlement(storage, row_id, branch_name)
            .map(|settlement| match (settlement, confirmed_tier) {
                (
                    BatchSettlement::DurableDirect {
                        batch_id,
                        confirmed_tier: existing_tier,
                        visible_members,
                    },
                    Some(incoming_tier),
                ) if incoming_tier > existing_tier => BatchSettlement::DurableDirect {
                    batch_id,
                    confirmed_tier: incoming_tier,
                    visible_members,
                },
                (
                    BatchSettlement::AcceptedTransaction {
                        batch_id,
                        confirmed_tier: existing_tier,
                        visible_members,
                    },
                    Some(incoming_tier),
                ) if incoming_tier > existing_tier => BatchSettlement::AcceptedTransaction {
                    batch_id,
                    confirmed_tier: incoming_tier,
                    visible_members,
                },
                (settlement, _) => settlement,
            })
            .or_else(|| {
                let confirmed_tier = confirmed_tier?;
                let record = storage.load_local_batch_record(batch_id).ok().flatten()?;
                if !record
                    .members
                    .iter()
                    .any(|member| member.object_id == row_id && member.branch_name == branch_name)
                {
                    return None;
                }
                let visible_members = record
                    .members
                    .iter()
                    .map(|member| VisibleBatchMember {
                        object_id: member.object_id,
                        branch_name: member.branch_name,
                        batch_id,
                    })
                    .collect::<Vec<_>>();
                match record.mode {
                    crate::batch_fate::BatchMode::Direct => Some(BatchSettlement::DurableDirect {
                        batch_id,
                        confirmed_tier,
                        visible_members,
                    }),
                    crate::batch_fate::BatchMode::Transactional => {
                        Some(BatchSettlement::AcceptedTransaction {
                            batch_id,
                            confirmed_tier,
                            visible_members,
                        })
                    }
                }
            })
    }

    fn respond_to_batch_settlement_request<H: Storage>(
        &mut self,
        storage: &H,
        destination: Destination,
        batch_ids: Vec<crate::row_histories::BatchId>,
    ) {
        for batch_id in batch_ids {
            let settlement = self
                .load_batch_settlement_by_batch_id_from_storage(storage, batch_id)
                .unwrap_or(BatchSettlement::Missing { batch_id });
            self.outbox.push(OutboxEntry {
                destination: destination.clone(),
                payload: SyncPayload::BatchSettlement { settlement },
            });
        }
    }

    fn transactional_batch_rows<H: Storage>(
        &self,
        storage: &H,
        batch_id: crate::row_histories::BatchId,
        object_ids: &[ObjectId],
    ) -> Vec<(String, StoredRowBatch)> {
        let mut rows = Vec::new();
        for row_id in object_ids {
            let Ok(Some(row_locator)) = storage.load_row_locator(*row_id) else {
                continue;
            };
            let Ok(history_rows) =
                storage.scan_history_row_batches(row_locator.table.as_str(), *row_id)
            else {
                continue;
            };

            for row in history_rows {
                if row.batch_id == batch_id {
                    rows.push((row_locator.table.to_string(), row));
                }
            }
        }

        rows.sort_by(|(_, left), (_, right)| {
            left.row_id
                .uuid()
                .as_bytes()
                .cmp(right.row_id.uuid().as_bytes())
                .then_with(|| left.branch.as_str().cmp(right.branch.as_str()))
                .then_with(|| left.batch_id.0.cmp(&right.batch_id.0))
        });
        rows
    }

    fn apply_transactional_batch_settlement_to_rows<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: Option<ClientId>,
        settlement: &BatchSettlement,
        batch_rows: &[(String, StoredRowBatch)],
    ) {
        let server_ids: Vec<_> = self.servers.keys().copied().collect();
        match settlement {
            BatchSettlement::AcceptedTransaction { confirmed_tier, .. } => {
                for (_table, row) in batch_rows {
                    let row_id = row.row_id;
                    let branch_name = BranchName::new(&row.branch);
                    let accepted_row = row.accepted_transaction_output(*confirmed_tier);
                    let accepted_batch_id = accepted_row.batch_id;
                    let applied =
                        apply_row_batch(storage, row_id, &branch_name, accepted_row.clone(), &[])
                            .ok();

                    let metadata = storage
                        .load_row_locator(row_id)
                        .ok()
                        .flatten()
                        .map(|locator| metadata_from_row_locator(&locator));

                    if let Some(client_id) = origin_client_id {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::RowBatchStateChanged {
                                row_id,
                                branch_name,
                                batch_id: accepted_batch_id,
                                state: Some(RowState::VisibleTransactional),
                                confirmed_tier: Some(*confirmed_tier),
                            },
                        });
                    }

                    if let Some(metadata) = metadata {
                        for server_id in &server_ids {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Server(*server_id),
                                payload: SyncPayload::RowBatchNeeded {
                                    metadata: Some(RowMetadata {
                                        id: row_id,
                                        metadata: metadata.clone(),
                                    }),
                                    row: accepted_row.clone(),
                                },
                            });
                        }
                    }

                    if let Some(applied) = applied
                        && let Some(update) = applied.visibility_change
                    {
                        self.pending_row_visibility_changes.push(update);
                        if let Some(client_id) = origin_client_id {
                            self.forward_update_to_clients_except_with_storage(
                                storage,
                                row_id,
                                branch_name,
                                client_id,
                            );
                        } else {
                            self.forward_update_to_clients_with_storage(
                                storage,
                                row_id,
                                branch_name,
                            );
                        }
                    }
                }

                for server_id in &server_ids {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Server(*server_id),
                        payload: SyncPayload::BatchSettlement {
                            settlement: settlement.clone(),
                        },
                    });
                }
            }
            BatchSettlement::Rejected { .. } => {
                for (_, row) in batch_rows {
                    let row_id = row.row_id;
                    let branch_name = BranchName::new(&row.branch);
                    let batch_id = row.batch_id;
                    let row_batch_id = row.batch_id();

                    let visibility_change = patch_row_batch_state(
                        storage,
                        row_id,
                        &branch_name,
                        row_batch_id,
                        Some(RowState::Rejected),
                        None,
                    )
                    .ok()
                    .flatten();

                    if let Some(client_id) = origin_client_id {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::RowBatchStateChanged {
                                row_id,
                                branch_name,
                                batch_id,
                                state: Some(RowState::Rejected),
                                confirmed_tier: None,
                            },
                        });
                    }

                    if let Some(update) = visibility_change {
                        self.pending_row_visibility_changes.push(update);
                        if let Some(client_id) = origin_client_id {
                            self.forward_update_to_clients_except_with_storage(
                                storage,
                                row_id,
                                branch_name,
                                client_id,
                            );
                        } else {
                            self.forward_update_to_clients_with_storage(
                                storage,
                                row_id,
                                branch_name,
                            );
                        }
                    }
                }
            }
            BatchSettlement::DurableDirect { .. } | BatchSettlement::Missing { .. } => return,
        }

        if let Some(client_id) = origin_client_id {
            self.outbox.push(OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::BatchSettlement {
                    settlement: settlement.clone(),
                },
            });
        }
    }

    fn reject_sealed_transactional_batch<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: Option<ClientId>,
        settlement: BatchSettlement,
        batch_rows: &[(String, StoredRowBatch)],
    ) {
        if self
            .persist_authoritative_batch_settlement(storage, &settlement)
            .is_err()
        {
            return;
        }
        self.pending_batch_settlements.push(settlement.clone());
        if let Err(error) = storage.delete_sealed_batch_submission(settlement.batch_id()) {
            tracing::warn!(
                batch_id = ?settlement.batch_id(),
                %error,
                "failed to delete rejected sealed batch submission"
            );
        }
        self.apply_transactional_batch_settlement_to_rows(
            storage,
            origin_client_id,
            &settlement,
            batch_rows,
        );
    }

    fn accept_sealed_transactional_batch<H: Storage>(
        &mut self,
        storage: &mut H,
        origin_client_id: Option<ClientId>,
        submission: SealedBatchSubmission,
        batch_rows: Vec<(String, StoredRowBatch)>,
    ) {
        let batch_id = submission.batch_id;
        let declared_rows: Vec<_> = submission
            .members
            .iter()
            .filter_map(|member| {
                batch_rows
                    .iter()
                    .find(|(_, row)| {
                        row.row_id == member.object_id
                            && row.branch.as_str() == submission.target_branch_name.as_str()
                            && row.content_digest() == member.row_digest
                    })
                    .cloned()
            })
            .collect();
        let settlement = match storage.load_authoritative_batch_settlement(batch_id) {
            Ok(Some(existing_settlement)) => existing_settlement,
            Ok(None) => {
                if batch_rows.is_empty() {
                    BatchSettlement::Missing { batch_id }
                } else {
                    let Some(confirmed_tier) = self.my_tiers.iter().copied().max() else {
                        return;
                    };
                    let visible_members = submission
                        .members
                        .iter()
                        .map(|member| VisibleBatchMember {
                            object_id: member.object_id,
                            branch_name: submission.target_branch_name,
                            batch_id,
                        })
                        .collect();
                    let settlement = BatchSettlement::AcceptedTransaction {
                        batch_id,
                        confirmed_tier,
                        visible_members,
                    };
                    if self
                        .persist_authoritative_batch_settlement(storage, &settlement)
                        .is_err()
                    {
                        return;
                    }
                    settlement
                }
            }
            Err(error) => {
                tracing::warn!(?batch_id, %error, "failed to load authoritative batch settlement");
                return;
            }
        };

        if !matches!(settlement, BatchSettlement::Missing { .. }) {
            self.pending_batch_settlements.push(settlement.clone());
            if let Err(error) = storage.delete_sealed_batch_submission(batch_id) {
                tracing::warn!(?batch_id, %error, "failed to delete sealed batch submission");
            }
        }
        let rows_to_patch: &[(String, StoredRowBatch)] = match settlement {
            BatchSettlement::AcceptedTransaction { .. } => &declared_rows,
            BatchSettlement::Rejected { .. } => &batch_rows,
            BatchSettlement::DurableDirect { .. } | BatchSettlement::Missing { .. } => &[],
        };
        self.apply_transactional_batch_settlement_to_rows(
            storage,
            origin_client_id,
            &settlement,
            rows_to_patch,
        );
    }

    fn try_accept_completed_sealed_batch_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        batch_id: crate::row_histories::BatchId,
    ) {
        let submission = match storage.load_sealed_batch_submission(batch_id) {
            Ok(Some(submission)) => submission,
            Ok(None) => return,
            Err(error) => {
                tracing::warn!(?batch_id, %error, "failed to load sealed batch submission");
                return;
            }
        };

        let batch_rows = self.transactional_batch_rows(
            storage,
            batch_id,
            &submission
                .members
                .iter()
                .map(|member| member.object_id)
                .collect::<Vec<_>>(),
        );
        if let Err(rejection) = self.validate_sealed_batch_submission(&submission) {
            self.reject_sealed_transactional_batch(
                storage,
                Some(client_id),
                rejection,
                &batch_rows,
            );
            return;
        }
        if let Err(rejection) = self.validate_batch_rows_target_branch(&submission, &batch_rows) {
            self.reject_sealed_transactional_batch(
                storage,
                Some(client_id),
                rejection,
                &batch_rows,
            );
            return;
        }
        if !submission.members.iter().all(|member| {
            batch_rows.iter().any(|(_, row)| {
                row.row_id == member.object_id
                    && row.branch.as_str() == submission.target_branch_name.as_str()
                    && row.content_digest() == member.row_digest
            })
        }) {
            return;
        }
        if let Err(rejection) = self.validate_captured_frontier(storage, &submission) {
            self.reject_sealed_transactional_batch(
                storage,
                Some(client_id),
                rejection,
                &batch_rows,
            );
            return;
        }

        self.accept_sealed_transactional_batch(storage, Some(client_id), submission, batch_rows);
    }

    pub(crate) fn recover_completed_sealed_batches_with_storage<H: Storage>(
        &mut self,
        storage: &mut H,
    ) -> bool {
        if self.my_tiers.is_empty() {
            return false;
        }

        let submissions = match storage.scan_sealed_batch_submissions() {
            Ok(submissions) => submissions,
            Err(error) => {
                tracing::warn!(%error, "failed to scan sealed batch submissions for recovery");
                return false;
            }
        };

        let mut recovered_any = false;
        for submission in submissions {
            let batch_rows = self.transactional_batch_rows(
                storage,
                submission.batch_id,
                &submission
                    .members
                    .iter()
                    .map(|member| member.object_id)
                    .collect::<Vec<_>>(),
            );
            if let Err(rejection) = self.validate_sealed_batch_submission(&submission) {
                self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                recovered_any = true;
                continue;
            }
            if let Err(rejection) = self.validate_batch_rows_target_branch(&submission, &batch_rows)
            {
                self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                recovered_any = true;
                continue;
            }
            if !submission.members.iter().all(|member| {
                batch_rows.iter().any(|(_, row)| {
                    row.row_id == member.object_id
                        && row.branch.as_str() == submission.target_branch_name.as_str()
                        && row.content_digest() == member.row_digest
                })
            }) {
                continue;
            }
            if let Err(rejection) = self.validate_captured_frontier(storage, &submission) {
                self.reject_sealed_transactional_batch(storage, None, rejection, &batch_rows);
                recovered_any = true;
                continue;
            }

            self.accept_sealed_transactional_batch(storage, None, submission, batch_rows);
            recovered_any = true;
        }

        recovered_any
    }

    /// Process a single inbox entry.
    pub(super) fn process_inbox_entry<H: Storage>(&mut self, storage: &mut H, entry: InboxEntry) {
        tracing::trace!(source = ?entry.source, payload = entry.payload.variant_name(), "processing inbox entry");
        match entry.source {
            Source::Server(server_id) => {
                self.process_from_server(storage, server_id, entry.payload)
            }
            Source::Client(client_id) => {
                self.process_from_client(storage, client_id, entry.payload)
            }
        }
    }

    /// Process a payload from a server.
    pub(super) fn process_from_server<H: Storage>(
        &mut self,
        storage: &mut H,
        server_id: ServerId,
        payload: SyncPayload,
    ) {
        let _span = tracing::debug_span!("process_from_server", %server_id, payload = payload.variant_name()).entered();
        match payload {
            SyncPayload::CatalogueEntryUpdated { entry } => {
                tracing::debug!(
                    object_id = %entry.object_id,
                    object_type = ?entry.object_type(),
                    "server→CatalogueEntryUpdated"
                );
                if self.persist_catalogue_entry(storage, entry.clone()) {
                    self.pending_catalogue_updates.push(entry.clone());
                    self.forward_catalogue_entry_to_clients(entry, None);
                }
            }
            SyncPayload::RowBatchCreated { metadata, row }
            | SyncPayload::RowBatchNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                tracing::debug!(
                    %object_id,
                    %branch_name,
                    "server→row-batch payload"
                );
                if let Some(applied) = self.apply_row_updated(storage, metadata, row.clone()) {
                    let batch_id = applied.row.batch_id;

                    for tier in self.my_tiers.iter().copied() {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Server(server_id),
                            payload: SyncPayload::RowBatchStateChanged {
                                row_id: object_id,
                                branch_name,
                                batch_id,
                                state: None,
                                confirmed_tier: Some(tier),
                            },
                        });
                    }

                    if let Some(update) = applied.visibility_change {
                        self.pending_row_visibility_changes.push(update);
                        self.forward_update_to_clients_with_storage(
                            storage,
                            object_id,
                            branch_name,
                        );
                    }
                }
            }
            SyncPayload::RowBatchStateChanged {
                row_id,
                branch_name,
                batch_id,
                state,
                confirmed_tier,
            } => {
                tracing::debug!(
                    %row_id,
                    %branch_name,
                    ?batch_id,
                    ?state,
                    ?confirmed_tier,
                    "server→RowBatchStateChanged"
                );
                self.apply_row_batch_state_changed(
                    storage,
                    row_id,
                    branch_name,
                    batch_id,
                    state,
                    confirmed_tier,
                );

                let key = RowBatchKey::new(row_id, branch_name, batch_id);
                let mut interested = HashSet::new();
                if let Some(clients) = self.row_batch_interest.get(&key) {
                    interested.extend(clients);
                }
                let settlement = self.settlement_for_row_batch_state_change(
                    storage,
                    row_id,
                    branch_name,
                    batch_id,
                    confirmed_tier,
                );
                let persisted_settlement = settlement.clone().filter(|settlement| {
                    self.persist_authoritative_batch_settlement(storage, settlement)
                        .is_ok()
                });
                if let Some(settlement) = persisted_settlement.clone() {
                    self.pending_batch_settlements.push(settlement);
                }
                for cid in interested {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(cid),
                        payload: SyncPayload::RowBatchStateChanged {
                            row_id,
                            branch_name,
                            batch_id,
                            state,
                            confirmed_tier,
                        },
                    });
                    if let Some(settlement) = persisted_settlement.clone() {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::BatchSettlement { settlement },
                        });
                    }
                }
            }
            SyncPayload::BatchSettlement { settlement } => {
                if self
                    .persist_authoritative_batch_settlement(storage, &settlement)
                    .is_err()
                {
                    return;
                }
                self.pending_batch_settlements.push(settlement.clone());
                let interested: HashSet<ClientId> = match &settlement {
                    BatchSettlement::DurableDirect {
                        visible_members, ..
                    }
                    | BatchSettlement::AcceptedTransaction {
                        visible_members, ..
                    } => visible_members
                        .iter()
                        .flat_map(|member| {
                            self.clients.iter().filter_map(move |(client_id, client)| {
                                client
                                    .is_in_scope(member.object_id, &member.branch_name)
                                    .then_some(*client_id)
                            })
                        })
                        .collect(),
                    BatchSettlement::Missing { .. } | BatchSettlement::Rejected { .. } => {
                        HashSet::new()
                    }
                };
                for cid in interested {
                    self.outbox.push(OutboxEntry {
                        destination: Destination::Client(cid),
                        payload: SyncPayload::BatchSettlement {
                            settlement: settlement.clone(),
                        },
                    });
                }
            }
            SyncPayload::BatchSettlementNeeded { batch_ids } => {
                self.respond_to_batch_settlement_request(
                    storage,
                    Destination::Server(server_id),
                    batch_ids,
                );
            }
            SyncPayload::QueryScopeSnapshot { query_id, scope } => {
                let scope_set: HashSet<(ObjectId, BranchName)> = scope.iter().copied().collect();
                self.remote_query_scopes
                    .insert((server_id, query_id), scope_set);

                if let Some(clients) = self.query_origin.get(&query_id) {
                    for &cid in clients {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::QueryScopeSnapshot {
                                query_id,
                                scope: scope.clone(),
                            },
                        });
                    }
                }
            }
            SyncPayload::QuerySettled {
                query_id,
                tier,
                through_seq,
            } => {
                tracing::debug!(?query_id, "server→QuerySettled");
                // Queue for local QueryManager to process
                self.pending_query_settled.push(PendingQuerySettled {
                    server_id: Some(server_id),
                    query_id,
                    tier,
                    through_seq,
                });

                // Relay to interested clients
                if let Some(clients) = self.query_origin.get(&query_id) {
                    for &cid in clients {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::QuerySettled {
                                query_id,
                                tier,
                                through_seq,
                            },
                        });
                    }
                }
            }
            SyncPayload::SchemaWarning(warning) => {
                super::log_schema_warning(&warning, Some("server"), None);

                if let Some(clients) = self.query_origin.get(&warning.query_id) {
                    for &cid in clients {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(cid),
                            payload: SyncPayload::SchemaWarning(warning.clone()),
                        });
                    }
                }
            }
            SyncPayload::ConnectionSchemaDiagnostics(diagnostics) => {
                super::log_connection_schema_diagnostics(&diagnostics, Some("server"));
            }
            SyncPayload::Error(err) => match err {
                SyncError::QuerySubscriptionRejected {
                    query_id,
                    code,
                    reason,
                } => {
                    tracing::warn!(
                        ?server_id,
                        query_id = query_id.0,
                        code = %code,
                        error = %reason,
                        "server rejected query subscription"
                    );
                    self.pending_query_rejections.push(PendingQueryRejection {
                        query_id,
                        code: code.clone(),
                        reason: reason.clone(),
                    });
                }
                _ => {
                    tracing::warn!(?server_id, error = ?err, "error from server");
                }
            },
            // Servers shouldn't send these to us
            SyncPayload::QuerySubscription { .. }
            | SyncPayload::QueryUnsubscription { .. }
            | SyncPayload::SealBatch { .. } => {}
        }
    }

    /// Process a payload from a client.
    pub(super) fn process_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
    ) {
        let _span = tracing::debug_span!("process_from_client", %client_id, payload = payload.variant_name()).entered();
        let Some(client) = self.clients.get(&client_id) else {
            tracing::warn!(%client_id, "message from unknown client, ignoring");
            return;
        };
        tracing::trace!(%client_id, role = ?client.role, payload = payload.variant_name(), "client→payload");

        match &payload {
            SyncPayload::CatalogueEntryUpdated { entry } => {
                let object_id = entry.object_id;
                let branch_name = BranchName::new("main");
                match client.role {
                    ClientRole::Peer | ClientRole::Admin => {
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::Backend => {
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                object_id,
                                branch_name,
                            }),
                        });
                    }
                    ClientRole::User => {
                        let Some(_session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        if self.allow_unprivileged_schema_catalogue_writes
                            && entry.is_structural_schema_catalogue()
                        {
                            self.apply_payload_from_client(storage, client_id, payload, false);
                            return;
                        }
                        self.outbox.push(OutboxEntry {
                            destination: Destination::Client(client_id),
                            payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                object_id,
                                branch_name,
                            }),
                        });
                    }
                }
            }
            SyncPayload::RowBatchCreated { metadata, row }
            | SyncPayload::RowBatchNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                match client.role {
                    ClientRole::Peer | ClientRole::Admin => {
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::Backend => {
                        if payload.is_catalogue() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }
                        self.apply_payload_from_client(storage, client_id, payload, false);
                    }
                    ClientRole::User => {
                        let Some(session) = &client.session else {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::SessionRequired {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        };
                        if payload.is_catalogue() {
                            if self.allow_unprivileged_schema_catalogue_writes
                                && payload.is_structural_schema_catalogue()
                            {
                                self.apply_payload_from_client(storage, client_id, payload, false);
                                return;
                            }
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied {
                                    object_id,
                                    branch_name,
                                }),
                            });
                            return;
                        }

                        let payload_metadata = metadata
                            .as_ref()
                            .map(|meta| meta.metadata.clone())
                            .unwrap_or_default();
                        let (stored_metadata, existing_history_row, pre_batch_visible_row) = self
                            .row_metadata_from_payload(storage, row, metadata.as_ref())
                            .and_then(|stored_metadata| {
                                let table =
                                    stored_metadata.get(MetadataKey::Table.as_str())?.clone();
                                let existing_history_row = storage
                                    .load_history_row_batch(
                                        &table,
                                        &row.branch,
                                        row.row_id,
                                        row.batch_id,
                                    )
                                    .ok()
                                    .flatten();
                                let pre_batch_visible_row =
                                    self.pre_batch_visible_row(storage, &table, row);
                                Some((stored_metadata, existing_history_row, pre_batch_visible_row))
                            })
                            .unwrap_or_else(|| (HashMap::new(), None, None));

                        // Idempotent replay short-circuit: reconnect replays row
                        // history, so only an exact stored row-batch match counts as
                        // a true no-op. Same-batch corrections must still flow
                        // through permission evaluation.
                        if let Some(existing_history_row) = existing_history_row.as_ref()
                            && Self::matches_replayed_row_batch(existing_history_row, row)
                        {
                            if let Some(settlement) = self
                                .load_batch_settlement_by_batch_id_from_storage(
                                    storage,
                                    row.batch_id,
                                )
                            {
                                self.queue_batch_settlement_to_client(client_id, settlement);
                            }
                            return;
                        }

                        let old_content = pre_batch_visible_row
                            .as_ref()
                            .map(|previous| previous.data.clone());
                        let metadata = if old_content.is_none() && stored_metadata.is_empty() {
                            payload_metadata
                        } else {
                            stored_metadata
                        };
                        let new_content = (!row.is_deleted).then_some(row.data.clone());
                        let operation = if row.is_deleted {
                            Operation::Delete
                        } else if old_content.is_some() || !row.parents.is_empty() {
                            Operation::Update
                        } else {
                            Operation::Insert
                        };
                        self.queue_for_permission_check(
                            client_id,
                            payload,
                            session.clone(),
                            metadata,
                            old_content.map(|content| content.to_vec()),
                            new_content.map(|content| content.to_vec()),
                            operation,
                        );
                    }
                }
            }
            SyncPayload::SealBatch { .. } => {
                self.apply_payload_from_client(storage, client_id, payload, false);
            }
            // Handle query subscription with full Query struct
            // Queue for QueryManager to process (SyncManager doesn't know about QueryGraph)
            SyncPayload::QuerySubscription {
                query_id,
                query,
                session,
                propagation,
                policy_context_tables,
            } => {
                // Build effective session: identity (user_id) comes from the
                // server-established session (set during the SSE auth handshake) and
                // cannot be overridden by the payload. However, ephemeral per-subscription
                // claims supplied in the payload — such as a join_code for invite flows —
                // are merged in when the user_id matches, so that policy conditions like
                // `claims.join_code` evaluate correctly for this subscription.
                let effective_session = match (&client.session, session) {
                    (Some(client_session), Some(payload_session)) => {
                        if client_session.user_id != payload_session.user_id {
                            tracing::warn!(
                                %client_id,
                                "QuerySubscription payload session user_id does not match client session; ignoring payload session"
                            );
                            Some(client_session.clone())
                        } else {
                            // Same user: merge claims. Payload provides ephemeral claims
                            // (e.g. join_code); client session claims take precedence so
                            // auth-established values cannot be spoofed.
                            let merged_claims = if let (
                                serde_json::Value::Object(client_map),
                                serde_json::Value::Object(payload_map),
                            ) =
                                (&client_session.claims, &payload_session.claims)
                            {
                                let mut merged = payload_map.clone();
                                merged.extend(client_map.clone());
                                serde_json::Value::Object(merged)
                            } else {
                                client_session.claims.clone()
                            };
                            Some(Session {
                                user_id: client_session.user_id.clone(),
                                claims: merged_claims,
                                auth_mode: client_session.auth_mode,
                            })
                        }
                    }
                    (Some(client_session), None) => Some(client_session.clone()),
                    (None, payload_session) => payload_session.clone(),
                };
                // Track origin for QuerySettled relay
                self.query_origin
                    .entry(*query_id)
                    .or_default()
                    .insert(client_id);
                self.pending_query_subscriptions
                    .push(PendingQuerySubscription {
                        client_id,
                        query_id: *query_id,
                        query: query.as_ref().clone(),
                        session: effective_session,
                        propagation: *propagation,
                        policy_context_tables: policy_context_tables.clone(),
                    });
            }
            // Handle query unsubscription
            // Queue for QueryManager to process (remove server-side QueryGraph, forward upstream)
            SyncPayload::QueryUnsubscription { query_id } => {
                // Clean up query origin
                if let Some(clients) = self.query_origin.get_mut(query_id) {
                    clients.remove(&client_id);
                    if clients.is_empty() {
                        self.query_origin.remove(query_id);
                    }
                }
                self.pending_query_unsubscriptions
                    .push(PendingQueryUnsubscription {
                        client_id,
                        query_id: *query_id,
                    });
            }
            SyncPayload::RowBatchStateChanged {
                row_id,
                branch_name,
                batch_id,
                state,
                confirmed_tier,
            } => {
                self.apply_row_batch_state_changed(
                    storage,
                    *row_id,
                    *branch_name,
                    *batch_id,
                    *state,
                    *confirmed_tier,
                );
            }
            SyncPayload::BatchSettlement { settlement } => {
                self.pending_batch_settlements.push(settlement.clone());
            }
            SyncPayload::BatchSettlementNeeded { batch_ids } => {
                self.respond_to_batch_settlement_request(
                    storage,
                    Destination::Client(client_id),
                    batch_ids.clone(),
                );
            }
            SyncPayload::QuerySettled {
                query_id,
                tier,
                through_seq,
            } => {
                // Client relaying a QuerySettled from downstream
                self.pending_query_settled.push(PendingQuerySettled {
                    server_id: None,
                    query_id: *query_id,
                    tier: *tier,
                    through_seq: *through_seq,
                });
            }
            SyncPayload::SchemaWarning(warning) => {
                tracing::warn!(
                    %client_id,
                    query_id = warning.query_id.0,
                    "client attempted to send SchemaWarning payload; ignoring"
                );
            }
            SyncPayload::QueryScopeSnapshot { query_id, .. } => {
                tracing::warn!(
                    %client_id,
                    query_id = query_id.0,
                    "client attempted to send QueryScopeSnapshot payload; ignoring"
                );
            }
            SyncPayload::ConnectionSchemaDiagnostics(_) => {
                tracing::warn!(
                    %client_id,
                    "client attempted to send ConnectionSchemaDiagnostics payload; ignoring"
                );
            }
            // Clients shouldn't send these
            SyncPayload::Error(_) => {}
        }
    }

    /// Apply a payload from a client (either directly or after approval).
    pub(super) fn apply_payload_from_client<H: Storage>(
        &mut self,
        storage: &mut H,
        client_id: ClientId,
        payload: SyncPayload,
        _was_pending: bool,
    ) {
        match payload {
            SyncPayload::CatalogueEntryUpdated { entry } => {
                if self.persist_catalogue_entry(storage, entry.clone()) {
                    self.pending_catalogue_updates.push(entry.clone());
                    self.forward_catalogue_entry_to_servers(entry.clone());
                    self.forward_catalogue_entry_to_clients(entry, Some(client_id));
                }
            }
            SyncPayload::RowBatchCreated { metadata, row }
            | SyncPayload::RowBatchNeeded { metadata, row } => {
                let object_id = row.row_id;
                let branch_name = BranchName::new(&row.branch);
                let batch_id = row.batch_id;
                self.row_batch_interest
                    .entry(RowBatchKey::new(object_id, branch_name, batch_id))
                    .or_default()
                    .insert(client_id);

                if let Some(applied) = self.apply_row_updated(storage, metadata, row.clone()) {
                    self.forward_row_batch_to_servers(object_id, applied.metadata.clone(), row);
                    if !matches!(
                        applied.row.state,
                        RowState::StagingPending | RowState::Superseded
                    ) {
                        let persisted_direct_settlement = self
                            .my_tiers
                            .iter()
                            .copied()
                            .max()
                            .and_then(|confirmed_tier| {
                                let settlement = BatchSettlement::DurableDirect {
                                    batch_id,
                                    confirmed_tier,
                                    visible_members: vec![VisibleBatchMember {
                                        object_id,
                                        branch_name,
                                        batch_id,
                                    }],
                                };
                                self.persist_authoritative_batch_settlement(storage, &settlement)
                                    .ok()
                                    .map(|_| settlement)
                            });
                        if let Some(settlement) = persisted_direct_settlement {
                            self.pending_batch_settlements.push(settlement.clone());
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::BatchSettlement { settlement },
                            });
                        }

                        for tier in self.my_tiers.iter().copied() {
                            self.outbox.push(OutboxEntry {
                                destination: Destination::Client(client_id),
                                payload: SyncPayload::RowBatchStateChanged {
                                    row_id: object_id,
                                    branch_name,
                                    batch_id,
                                    state: None,
                                    confirmed_tier: Some(tier),
                                },
                            });
                        }

                        if let Some(update) = applied.visibility_change {
                            self.pending_row_visibility_changes.push(update);
                            self.forward_update_to_clients_except_with_storage(
                                storage,
                                object_id,
                                branch_name,
                                client_id,
                            );
                        }
                    } else {
                        self.try_accept_completed_sealed_batch_from_client(
                            storage,
                            client_id,
                            applied.row.batch_id,
                        );
                    }
                }
            }
            SyncPayload::SealBatch { submission } => {
                if submission.members.is_empty() {
                    tracing::warn!(batch_id = ?submission.batch_id, "ignoring SealBatch with no declared members");
                    return;
                }
                if let Err(rejection) = self.validate_sealed_batch_submission(&submission) {
                    let batch_rows = self.transactional_batch_rows(
                        storage,
                        submission.batch_id,
                        &submission
                            .members
                            .iter()
                            .map(|member| member.object_id)
                            .collect::<Vec<_>>(),
                    );
                    self.reject_sealed_transactional_batch(
                        storage,
                        Some(client_id),
                        rejection,
                        &batch_rows,
                    );
                    return;
                }
                if self
                    .persist_sealed_batch_submission(storage, &submission)
                    .is_err()
                {
                    return;
                }
                self.try_accept_completed_sealed_batch_from_client(
                    storage,
                    client_id,
                    submission.batch_id,
                );
            }
            SyncPayload::BatchSettlement { settlement } => {
                self.pending_batch_settlements.push(settlement);
            }
            SyncPayload::BatchSettlementNeeded { batch_ids } => {
                self.respond_to_batch_settlement_request(
                    storage,
                    Destination::Client(client_id),
                    batch_ids,
                );
            }
            _ => {}
        }
    }
}
