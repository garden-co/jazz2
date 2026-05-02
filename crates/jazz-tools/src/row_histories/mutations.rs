//! Storage-mutating row-history operations.
//!
//! Owns the public verbs that change row-history state and keep the derived
//! visible region consistent afterward:
//! - [`apply_row_batch`] — insert or update a history batch, then recompute and
//!   persist visibility (and supersede stale staging siblings if needed)
//! - [`patch_row_batch_state`] — flip an existing batch's state and
//!   recompute visibility
//!
//! Each mutation captures the visible row before/after the change as an
//! [`AppliedRowBatch`], from which a [`RowVisibilityChange`] is derived for
//! downstream observers (sync, indexers, query subscribers).
//!
//! Pure visibility/merge math lives in [`super::resolution`]; this module
//! only orchestrates: load → mutate → recompute → write.

use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{RowDescriptor, SharedString};
use crate::storage::{IndexMutation, PreparedRowWriteContext, RowLocator, Storage, StorageError};
use crate::sync_manager::DurabilityTier;

use super::resolution::visible_entry_from_history_rows;
use super::types::{
    ApplyRowBatchResult, BatchId, HistoryScan, RowHistoryError, RowState, RowVisibilityChange,
    StoredRowBatch, VisibleRowEntry,
};

#[derive(Debug, Clone)]
pub(super) struct AppliedRowBatch {
    row_locator: RowLocator,
    previous_visible: Option<StoredRowBatch>,
    current_visible: Option<StoredRowBatch>,
    is_new_object: bool,
    visible_changed: bool,
}

pub(crate) struct ApplyRowBatchWithContext<'a> {
    pub(crate) object_id: ObjectId,
    pub(crate) branch_name: &'a BranchName,
    pub(crate) row: StoredRowBatch,
    pub(crate) index_mutations: &'a [IndexMutation<'a>],
    pub(crate) row_locator: RowLocator,
    pub(crate) table: String,
    pub(crate) branch: SharedString,
    pub(crate) context: PreparedRowWriteContext,
    pub(crate) is_known_new_object: bool,
}

pub(super) fn row_locator_from_storage<H: Storage>(
    io: &H,
    object_id: ObjectId,
) -> Result<RowLocator, RowHistoryError> {
    io.load_row_locator(object_id)
        .map_err(RowHistoryError::StorageError)?
        .ok_or(RowHistoryError::ObjectNotFound(object_id))
}

pub(super) fn load_branch_history<H: Storage>(
    io: &H,
    table: &str,
    object_id: ObjectId,
    branch_name: &SharedString,
) -> Result<Vec<StoredRowBatch>, RowHistoryError> {
    io.scan_history_region(
        table,
        branch_name.as_str(),
        HistoryScan::Row { row_id: object_id },
    )
    .map_err(RowHistoryError::StorageError)
}

pub(super) fn rebuild_visible_entry_from_history<H: Storage>(
    io: &H,
    table: &str,
    object_id: ObjectId,
    branch_name: &SharedString,
    user_descriptor: &RowDescriptor,
) -> Result<Option<VisibleRowEntry>, RowHistoryError> {
    let history_rows = load_branch_history(io, table, object_id, branch_name)?;
    visible_entry_from_history_rows(user_descriptor, &history_rows).map_err(|err| {
        RowHistoryError::StorageError(StorageError::IoError(format!(
            "rebuild visible entry: {err}"
        )))
    })
}

pub(super) fn load_previous_visible_entry<H: Storage>(
    io: &H,
    table: &str,
    object_id: ObjectId,
    branch_name: &SharedString,
    user_descriptor: &RowDescriptor,
) -> Result<Option<VisibleRowEntry>, RowHistoryError> {
    match io.load_visible_region_entry(table, branch_name.as_str(), object_id) {
        Ok(Some(entry)) => Ok(Some(entry)),
        Ok(None) => {
            rebuild_visible_entry_from_history(io, table, object_id, branch_name, user_descriptor)
        }
        Err(_) => {
            rebuild_visible_entry_from_history(io, table, object_id, branch_name, user_descriptor)
        }
    }
}

pub(super) fn visibility_change_from_applied(
    object_id: ObjectId,
    applied: AppliedRowBatch,
) -> Option<RowVisibilityChange> {
    if !applied.visible_changed {
        return None;
    }

    let current_visible = applied.current_visible?;
    Some(RowVisibilityChange {
        object_id,
        row_locator: applied.row_locator,
        row: current_visible,
        previous_row: applied.previous_visible,
        is_new_object: applied.is_new_object,
    })
}

pub(super) fn supersede_older_staging_rows_for_batch<H: Storage>(
    io: &mut H,
    table: &str,
    object_id: ObjectId,
    branch_name: &BranchName,
    batch_id: BatchId,
) -> Result<(), RowHistoryError> {
    let branch = SharedString::from(branch_name.as_str().to_string());
    let history_rows = load_branch_history(io, table, object_id, &branch)?;
    let mut pending_rows = history_rows
        .into_iter()
        .filter(|row| row.batch_id == batch_id && matches!(row.state, RowState::StagingPending))
        .collect::<Vec<_>>();

    if pending_rows.len() <= 1 {
        return Ok(());
    }

    pending_rows.sort_by_key(|row| (row.updated_at, row.batch_id()));
    pending_rows.pop();

    for row in pending_rows {
        let _ = patch_row_batch_state(
            io,
            object_id,
            branch_name,
            row.batch_id(),
            Some(RowState::Superseded),
            None,
        )?;
    }

    Ok(())
}

pub fn apply_row_batch<H: Storage>(
    io: &mut H,
    object_id: ObjectId,
    branch_name: &BranchName,
    row: StoredRowBatch,
    index_mutations: &[IndexMutation<'_>],
) -> Result<ApplyRowBatchResult, RowHistoryError> {
    let row_locator = row_locator_from_storage(io, object_id)?;
    let table = row_locator.table.to_string();
    let branch = SharedString::from(branch_name.as_str().to_string());
    let context = crate::storage::resolve_history_row_write_context(io, &table, &row)
        .map_err(RowHistoryError::StorageError)?;
    apply_row_batch_with_context(
        io,
        ApplyRowBatchWithContext {
            object_id,
            branch_name,
            row,
            index_mutations,
            row_locator,
            table,
            branch,
            context,
            is_known_new_object: false,
        },
    )
}

pub(crate) fn apply_row_batch_with_context<H: Storage>(
    io: &mut H,
    request: ApplyRowBatchWithContext<'_>,
) -> Result<ApplyRowBatchResult, RowHistoryError> {
    let ApplyRowBatchWithContext {
        object_id,
        branch_name,
        row,
        index_mutations,
        row_locator,
        table,
        branch,
        context,
        is_known_new_object,
    } = request;
    let batch_id = row.batch_id();
    debug_assert!(
        !is_known_new_object || row.parents.is_empty(),
        "known-new row apply skips parent lookups"
    );
    let previous_entry = if is_known_new_object {
        None
    } else {
        load_previous_visible_entry(
            io,
            &table,
            object_id,
            &branch,
            context.user_descriptor.as_ref(),
        )?
    };
    let previous_visible = previous_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());

    if !is_known_new_object {
        for parent in &row.parents {
            if io
                .load_history_row_batch(&table, branch_name.as_str(), object_id, *parent)
                .map_err(RowHistoryError::StorageError)?
                .is_none()
            {
                return Err(RowHistoryError::ParentNotFound(*parent));
            }
        }
    }

    let mut patched_history = if is_known_new_object {
        Vec::new()
    } else {
        load_branch_history(io, &table, object_id, &branch)?
    };

    if !is_known_new_object
        && let Some(existing_row) = io
            .load_history_row_batch(&table, branch_name.as_str(), object_id, batch_id)
            .map_err(RowHistoryError::StorageError)?
        && existing_row == row
    {
        return Ok(ApplyRowBatchResult {
            batch_id,
            row_locator,
            visibility_change: None,
        });
    }
    if let Some(existing) = patched_history
        .iter_mut()
        .find(|candidate| candidate.batch_id() == batch_id)
    {
        *existing = row.clone();
    } else {
        patched_history.push(row.clone());
    }
    let current_entry =
        visible_entry_from_history_rows(context.user_descriptor.as_ref(), &patched_history)
            .map_err(|err| {
                RowHistoryError::StorageError(StorageError::IoError(format!(
                    "rebuild visible entry after append: {err}"
                )))
            })?;
    let current_visible = current_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());
    let visible_entry_changed = current_entry.as_ref() != previous_entry.as_ref();
    let visible_entries: &[VisibleRowEntry] = match (visible_entry_changed, current_entry.as_ref())
    {
        (true, Some(entry)) => std::slice::from_ref(entry),
        _ => &[],
    };
    let visible_changed = previous_visible != current_visible;
    let can_encode_visible_with_row_context = visible_entries.len() == 1
        && visible_entries[0].current_row.row_id == row.row_id
        && visible_entries[0].current_row.branch == row.branch
        && visible_entries[0].current_row.batch_id() == row.batch_id();

    if visible_entries.is_empty() || can_encode_visible_with_row_context {
        let encoded_history = crate::storage::encode_history_row_bytes_with_context(&context, &row)
            .map_err(RowHistoryError::StorageError)?;
        let encoded_visible = if let Some(entry) = visible_entries.first() {
            vec![
                crate::storage::encode_visible_row_bytes_with_context(&context, entry)
                    .map_err(RowHistoryError::StorageError)?,
            ]
        } else {
            Vec::new()
        };
        <H as Storage>::apply_prepared_row_mutation(
            io,
            &table,
            std::slice::from_ref(&row),
            visible_entries,
            std::slice::from_ref(&encoded_history),
            &encoded_visible,
            index_mutations,
        )
        .map_err(RowHistoryError::StorageError)?;
    } else {
        <H as Storage>::apply_row_mutation(
            io,
            &table,
            std::slice::from_ref(&row),
            visible_entries,
            index_mutations,
        )
        .map_err(RowHistoryError::StorageError)?;
    }

    if matches!(row.state, RowState::StagingPending) {
        supersede_older_staging_rows_for_batch(io, &table, object_id, branch_name, row.batch_id)?;
    }

    let applied = AppliedRowBatch {
        row_locator: row_locator.clone(),
        previous_visible: previous_visible.clone(),
        current_visible,
        is_new_object: previous_visible.is_none(),
        visible_changed,
    };

    Ok(ApplyRowBatchResult {
        batch_id,
        row_locator,
        visibility_change: visibility_change_from_applied(object_id, applied),
    })
}

pub fn patch_row_batch_state<H: Storage>(
    io: &mut H,
    object_id: ObjectId,
    branch_name: &BranchName,
    batch_id: BatchId,
    state: Option<RowState>,
    confirmed_tier: Option<DurabilityTier>,
) -> Result<Option<RowVisibilityChange>, RowHistoryError> {
    let row_locator = row_locator_from_storage(io, object_id)?;
    let table = row_locator.table.to_string();
    let branch = SharedString::from(branch_name.as_str().to_string());
    let mut patched_row = io
        .load_history_row_batch(&table, branch_name.as_str(), object_id, batch_id)
        .map_err(RowHistoryError::StorageError)?
        .ok_or(RowHistoryError::ObjectNotFound(object_id))?;
    if patched_row.branch.as_str() != branch_name.as_str() {
        return Ok(None);
    }
    let context = crate::storage::resolve_history_row_write_context(io, &table, &patched_row)
        .map_err(RowHistoryError::StorageError)?;
    let previous_entry = load_previous_visible_entry(
        io,
        &table,
        object_id,
        &branch,
        context.user_descriptor.as_ref(),
    )?;
    let previous_visible = previous_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());

    if let Some(state) = state {
        patched_row.state = state;
    }
    patched_row.confirmed_tier = match (patched_row.confirmed_tier, confirmed_tier) {
        (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
        (Some(existing), None) => Some(existing),
        (None, incoming) => incoming,
    };

    let mut history_rows = load_branch_history(io, &table, object_id, &branch)?;
    let Some(existing) = history_rows
        .iter_mut()
        .find(|candidate| candidate.batch_id() == batch_id)
    else {
        return Err(RowHistoryError::ObjectNotFound(object_id));
    };
    *existing = patched_row.clone();
    let patched_entry =
        visible_entry_from_history_rows(context.user_descriptor.as_ref(), &history_rows).map_err(
            |err| {
                RowHistoryError::StorageError(StorageError::IoError(format!(
                    "rebuild visible entry after patch: {err}"
                )))
            },
        )?;
    let visible_entries: Vec<_> = patched_entry.iter().cloned().collect();
    if patched_entry.is_some() {
        io.apply_row_mutation(
            &table,
            std::slice::from_ref(&patched_row),
            &visible_entries,
            &[],
        )
        .map_err(RowHistoryError::StorageError)?;
    } else {
        io.append_history_region_rows(&table, std::slice::from_ref(&patched_row))
            .map_err(RowHistoryError::StorageError)?;
        io.delete_visible_region_row(&table, branch_name.as_str(), object_id)
            .map_err(RowHistoryError::StorageError)?;
    }

    let current_visible = patched_entry
        .as_ref()
        .map(|entry| entry.current_row.clone());
    if previous_visible == current_visible {
        return Ok(None);
    }

    let Some(current_visible) = current_visible else {
        return Ok(None);
    };

    Ok(Some(RowVisibilityChange {
        object_id,
        row_locator,
        row: current_visible,
        previous_row: previous_visible.clone(),
        is_new_object: previous_visible.is_none(),
    }))
}
