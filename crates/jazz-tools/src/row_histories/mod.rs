use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::sync::{Arc, Mutex, OnceLock};

use blake3::Hasher;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;
use uuid::Uuid;

use crate::digest::Digest32;
use crate::metadata::{DeleteKind, MetadataKey, RowProvenance};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, RowBytes, RowDescriptor, SharedString, Value,
};
use crate::row_format::{
    CompiledRowLayout, EncodingError, column_bytes_with_layout, column_is_null_with_layout,
    compiled_row_layout, decode_row, encode_row, project_row_with_layout,
};
use crate::storage::{IndexMutation, RowLocator, Storage, StorageError};
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BatchId(pub [u8; 16]);

impl BatchId {
    pub fn new() -> Self {
        Self::from_uuid(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(*uuid.as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Default for BatchId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl FromStr for BatchId {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(raw).map_err(|err| format!("invalid batch id hex: {err}"))?;
        let len = bytes.len();
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| format!("expected 16-byte batch id, got {len}"))?;
        Ok(Self(bytes))
    }
}

impl From<BatchId> for Digest32 {
    fn from(value: BatchId) -> Self {
        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(&value.0);
        Digest32(bytes)
    }
}

impl From<Digest32> for BatchId {
    fn from(value: Digest32) -> Self {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&value.0[..16]);
        Self(bytes)
    }
}

impl Serialize for BatchId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for BatchId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = String::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowState {
    StagingPending,
    Superseded,
    Rejected,
    VisibleDirect,
    VisibleTransactional,
}

impl RowState {
    pub fn is_visible(self) -> bool {
        matches!(self, Self::VisibleDirect | Self::VisibleTransactional)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryScan {
    Branch,
    Row { row_id: ObjectId },
    AsOf { ts: u64 },
}

/// Visibility change emitted when a row object's winning version changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowVisibilityChange {
    pub object_id: ObjectId,
    pub row_locator: RowLocator,
    pub row: StoredRowBatch,
    pub previous_row: Option<StoredRowBatch>,
    pub is_new_object: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRowBatchResult {
    pub batch_id: BatchId,
    pub row_locator: RowLocator,
    pub visibility_change: Option<RowVisibilityChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowHistoryError {
    ObjectNotFound(ObjectId),
    ParentNotFound(BatchId),
    StorageError(StorageError),
}

fn tier_satisfies(confirmed_tier: Option<DurabilityTier>, required_tier: DurabilityTier) -> bool {
    confirmed_tier.is_some_and(|confirmed| confirmed >= required_tier)
}

fn malformed(message: impl Into<String>) -> EncodingError {
    EncodingError::MalformedData {
        message: message.into(),
    }
}

pub fn compute_row_digest(
    branch: &str,
    parents: &[BatchId],
    data: &[u8],
    updated_at: u64,
    updated_by: &str,
    metadata: Option<&RowMetadata>,
) -> Digest32 {
    let mut hasher = Hasher::new();

    hasher.update(b"row-batch-v1");
    hasher.update(&(branch.len() as u64).to_le_bytes());
    hasher.update(branch.as_bytes());

    hasher.update(&(parents.len() as u64).to_le_bytes());
    for parent in parents {
        hasher.update(parent.as_bytes());
    }

    hasher.update(&(data.len() as u64).to_le_bytes());
    hasher.update(data);

    hasher.update(&updated_at.to_le_bytes());
    hasher.update(updated_by.as_bytes());

    if let Some(metadata) = metadata {
        hasher.update(&[1u8]);
        hasher.update(&(metadata.len() as u64).to_le_bytes());
        for (key, value) in metadata.iter() {
            hasher.update(&(key.len() as u64).to_le_bytes());
            hasher.update(key.as_bytes());
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value.as_bytes());
        }
    } else {
        hasher.update(&[0u8]);
    }

    Digest32(*hasher.finalize().as_bytes())
}

fn metadata_entry_descriptor() -> &'static RowDescriptor {
    static DESCRIPTOR: OnceLock<RowDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("key", ColumnType::Text),
            ColumnDescriptor::new("value", ColumnType::Text),
        ])
    })
}

fn metadata_entry_layout() -> &'static Arc<CompiledRowLayout> {
    static LAYOUT: OnceLock<Arc<CompiledRowLayout>> = OnceLock::new();
    LAYOUT.get_or_init(|| compiled_row_layout(metadata_entry_descriptor()))
}

fn row_state_column_type() -> ColumnType {
    ColumnType::Enum {
        variants: vec![
            "staging_pending".to_string(),
            "superseded".to_string(),
            "rejected".to_string(),
            "visible_direct".to_string(),
            "visible_transactional".to_string(),
        ],
    }
}

fn confirmed_tier_column_type() -> ColumnType {
    ColumnType::Enum {
        variants: vec![
            "local".to_string(),
            "edge".to_string(),
            "global".to_string(),
        ],
    }
}

fn delete_kind_column_type() -> ColumnType {
    ColumnType::Enum {
        variants: vec!["soft".to_string(), "hard".to_string()],
    }
}

fn history_row_system_columns() -> Vec<ColumnDescriptor> {
    vec![
        ColumnDescriptor::new(
            "_jazz_parents",
            ColumnType::Array {
                element: Box::new(ColumnType::BatchId),
            },
        )
        .nullable(),
        ColumnDescriptor::new("_jazz_updated_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_created_by", ColumnType::Text),
        ColumnDescriptor::new("_jazz_created_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_updated_by", ColumnType::Text),
        ColumnDescriptor::new("_jazz_state", row_state_column_type()),
        ColumnDescriptor::new("_jazz_confirmed_tier", confirmed_tier_column_type()).nullable(),
        ColumnDescriptor::new("_jazz_delete_kind", delete_kind_column_type()).nullable(),
        ColumnDescriptor::new("_jazz_is_deleted", ColumnType::Boolean),
        ColumnDescriptor::new(
            "_jazz_metadata",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(metadata_entry_descriptor().clone()),
                }),
            },
        )
        .nullable(),
    ]
}

fn history_row_system_values(row: &StoredRowBatch) -> Vec<Value> {
    vec![
        Value::Array(row.parents.iter().copied().map(batch_id_to_value).collect()),
        Value::Timestamp(row.updated_at),
        Value::Text(row.created_by.to_string()),
        Value::Timestamp(row.created_at),
        Value::Text(row.updated_by.to_string()),
        row_state_to_value(row.state),
        row.confirmed_tier
            .map(durability_tier_to_value)
            .unwrap_or(Value::Null),
        row.delete_kind
            .map(delete_kind_to_value)
            .unwrap_or(Value::Null),
        Value::Boolean(row.is_deleted),
        metadata_to_value(&row.metadata),
    ]
}

fn history_row_system_column_count() -> usize {
    history_row_system_columns().len()
}

fn visible_row_system_columns() -> Vec<ColumnDescriptor> {
    let mut columns = vec![
        ColumnDescriptor::new("_jazz_batch_id", ColumnType::BatchId),
        ColumnDescriptor::new("_jazz_updated_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_created_by", ColumnType::Text),
        ColumnDescriptor::new("_jazz_created_at", ColumnType::Timestamp),
        ColumnDescriptor::new("_jazz_updated_by", ColumnType::Text),
        ColumnDescriptor::new("_jazz_state", row_state_column_type()),
        ColumnDescriptor::new("_jazz_confirmed_tier", confirmed_tier_column_type()).nullable(),
        ColumnDescriptor::new("_jazz_delete_kind", delete_kind_column_type()).nullable(),
    ];
    columns.extend([
        ColumnDescriptor::new(
            "_jazz_branch_frontier",
            ColumnType::Array {
                element: Box::new(ColumnType::BatchId),
            },
        )
        .nullable(),
        ColumnDescriptor::new("_jazz_worker_batch_id", ColumnType::BatchId).nullable(),
        ColumnDescriptor::new("_jazz_edge_batch_id", ColumnType::BatchId).nullable(),
        ColumnDescriptor::new("_jazz_global_batch_id", ColumnType::BatchId).nullable(),
        ColumnDescriptor::new(
            "_jazz_winner_batch_pool",
            ColumnType::Array {
                element: Box::new(ColumnType::BatchId),
            },
        )
        .nullable(),
        ColumnDescriptor::new("_jazz_current_winner_ordinals", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_worker_winner_ordinals", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_edge_winner_ordinals", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_global_winner_ordinals", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("_jazz_merge_artifacts", ColumnType::Bytea).nullable(),
    ]);
    columns
}

fn visible_row_system_values(entry: &VisibleRowEntry) -> Vec<Value> {
    let mut values = vec![
        batch_id_to_value(entry.current_row.batch_id),
        Value::Timestamp(entry.current_row.updated_at),
        Value::Text(entry.current_row.created_by.to_string()),
        Value::Timestamp(entry.current_row.created_at),
        Value::Text(entry.current_row.updated_by.to_string()),
        row_state_to_value(entry.current_row.state),
        entry
            .current_row
            .confirmed_tier
            .map(durability_tier_to_value)
            .unwrap_or(Value::Null),
        entry
            .current_row
            .delete_kind
            .map(delete_kind_to_value)
            .unwrap_or(Value::Null),
    ];
    values.extend([
        visible_frontier_to_value(entry),
        optional_batch_id_to_value(entry.worker_batch_id),
        optional_batch_id_to_value(entry.edge_batch_id),
        optional_batch_id_to_value(entry.global_batch_id),
        winner_batch_pool_to_value(&entry.winner_batch_pool),
        optional_winner_ordinals_to_value(entry.current_winner_ordinals.as_deref()),
        optional_winner_ordinals_to_value(entry.worker_winner_ordinals.as_deref()),
        optional_winner_ordinals_to_value(entry.edge_winner_ordinals.as_deref()),
        optional_winner_ordinals_to_value(entry.global_winner_ordinals.as_deref()),
        entry
            .merge_artifacts
            .as_ref()
            .map(|bytes| Value::Bytea(bytes.clone()))
            .unwrap_or(Value::Null),
    ]);
    values
}

fn visible_row_system_column_count() -> usize {
    visible_row_system_columns().len()
}

#[derive(Debug, Clone)]
pub(crate) struct FlatRowCodecs {
    user_descriptor: Arc<RowDescriptor>,
    history_descriptor: Arc<RowDescriptor>,
    history_layout: Arc<CompiledRowLayout>,
    history_user_projection: Vec<(usize, usize)>,
    visible_descriptor: Arc<RowDescriptor>,
    visible_layout: Arc<CompiledRowLayout>,
    visible_user_projection: Vec<(usize, usize)>,
}

fn flat_row_codecs_cache() -> &'static Mutex<HashMap<[u8; 32], Arc<FlatRowCodecs>>> {
    static CACHE: OnceLock<Mutex<HashMap<[u8; 32], Arc<FlatRowCodecs>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn flat_row_codecs(user_descriptor: &RowDescriptor) -> Arc<FlatRowCodecs> {
    let key = user_descriptor.content_hash();
    {
        let guard = flat_row_codecs_cache()
            .lock()
            .expect("flat row codec cache poisoned");
        if let Some(codecs) = guard.get(&key) {
            return codecs.clone();
        }
    }

    let user_descriptor = Arc::new(user_descriptor.clone());
    let history_descriptor = Arc::new(history_row_physical_descriptor(user_descriptor.as_ref()));
    let visible_descriptor = Arc::new(visible_row_physical_descriptor(user_descriptor.as_ref()));
    let history_system_count = history_row_system_column_count();
    let visible_system_count = visible_row_system_column_count();
    let user_projection_len = user_descriptor.columns.len();
    let codecs = Arc::new(FlatRowCodecs {
        user_descriptor: user_descriptor.clone(),
        history_layout: compiled_row_layout(history_descriptor.as_ref()),
        history_descriptor,
        history_user_projection: (0..user_projection_len)
            .map(|index| (history_system_count + index, index))
            .collect(),
        visible_layout: compiled_row_layout(visible_descriptor.as_ref()),
        visible_descriptor,
        visible_user_projection: (0..user_projection_len)
            .map(|index| (visible_system_count + index, index))
            .collect(),
    });

    flat_row_codecs_cache()
        .lock()
        .expect("flat row codec cache poisoned")
        .insert(key, codecs.clone());
    codecs
}

/// Build the physical row descriptor used when row-history state is stored as a
/// single flat row: reserved Jazz columns first, followed by the table's user
/// columns as nullable storage columns.
pub fn history_row_physical_descriptor(user_descriptor: &RowDescriptor) -> RowDescriptor {
    let mut columns = history_row_system_columns();
    columns.extend(user_descriptor.columns.iter().cloned().map(|mut column| {
        column.nullable = true;
        column
    }));
    RowDescriptor::new(columns)
}

pub fn visible_row_physical_descriptor(user_descriptor: &RowDescriptor) -> RowDescriptor {
    let mut columns = visible_row_system_columns();
    columns.extend(user_descriptor.columns.iter().cloned().map(|mut column| {
        column.nullable = true;
        column
    }));
    RowDescriptor::new(columns)
}

fn flat_user_values(
    user_descriptor: &RowDescriptor,
    data: &RowBytes,
) -> Result<Vec<Value>, EncodingError> {
    if data.is_empty() {
        Ok(user_descriptor
            .columns
            .iter()
            .map(|_| Value::Null)
            .collect::<Vec<_>>())
    } else {
        decode_row(user_descriptor, data)
    }
}

/// Encode a row-history version into a single flat physical row.
pub fn encode_flat_history_row(
    user_descriptor: &RowDescriptor,
    row: &StoredRowBatch,
) -> Result<Vec<u8>, EncodingError> {
    let codecs = flat_row_codecs(user_descriptor);
    let mut values = history_row_system_values(row);
    values.extend(flat_user_values(user_descriptor, &row.data)?);

    encode_row(codecs.history_descriptor.as_ref(), &values)
}

/// Decode a flat physical row back into the current `StoredRowBatch` shape.
pub fn decode_flat_history_row(
    user_descriptor: &RowDescriptor,
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
    data: &[u8],
) -> Result<StoredRowBatch, EncodingError> {
    let codecs = flat_row_codecs(user_descriptor);
    decode_flat_history_row_with_codecs(codecs.as_ref(), row_id, branch, batch_id, data)
}

pub fn encode_flat_visible_row_entry(
    user_descriptor: &RowDescriptor,
    entry: &VisibleRowEntry,
) -> Result<Vec<u8>, EncodingError> {
    let codecs = flat_row_codecs(user_descriptor);
    let mut values = visible_row_system_values(entry);
    values.extend(flat_user_values(user_descriptor, &entry.current_row.data)?);
    encode_row(codecs.visible_descriptor.as_ref(), &values)
}

pub fn decode_flat_visible_row_entry(
    user_descriptor: &RowDescriptor,
    row_id: ObjectId,
    branch: &str,
    data: &[u8],
) -> Result<VisibleRowEntry, EncodingError> {
    let codecs = flat_row_codecs(user_descriptor);
    decode_flat_visible_row_entry_with_codecs(codecs.as_ref(), row_id, branch, data)
}

fn visible_frontier_to_value(entry: &VisibleRowEntry) -> Value {
    if entry.branch_frontier.len() == 1 && entry.branch_frontier[0] == entry.current_row.batch_id()
    {
        Value::Null
    } else {
        batch_ids_to_value(&entry.branch_frontier)
    }
}

fn row_state_to_value(state: RowState) -> Value {
    Value::Text(
        match state {
            RowState::StagingPending => "staging_pending",
            RowState::Superseded => "superseded",
            RowState::Rejected => "rejected",
            RowState::VisibleDirect => "visible_direct",
            RowState::VisibleTransactional => "visible_transactional",
        }
        .to_string(),
    )
}

fn durability_tier_to_value(tier: DurabilityTier) -> Value {
    Value::Text(
        match tier {
            DurabilityTier::Local => "local",
            DurabilityTier::EdgeServer => "edge",
            DurabilityTier::GlobalServer => "global",
        }
        .to_string(),
    )
}

fn delete_kind_to_value(kind: DeleteKind) -> Value {
    Value::Text(kind.as_str().to_string())
}

fn batch_id_to_value(batch_id: BatchId) -> Value {
    Value::BatchId(*batch_id.as_bytes())
}

fn optional_batch_id_to_value(batch_id: Option<BatchId>) -> Value {
    batch_id.map(batch_id_to_value).unwrap_or(Value::Null)
}

fn batch_ids_to_value(batch_ids: &[BatchId]) -> Value {
    Value::Array(batch_ids.iter().copied().map(batch_id_to_value).collect())
}

fn winner_batch_pool_to_value(batch_ids: &[BatchId]) -> Value {
    if batch_ids.is_empty() {
        Value::Null
    } else {
        batch_ids_to_value(batch_ids)
    }
}

fn encode_winner_ordinals(ordinals: &[u16]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(ordinals.len() * 2);
    for ordinal in ordinals {
        bytes.extend_from_slice(&ordinal.to_le_bytes());
    }
    bytes
}

fn optional_winner_ordinals_to_value(ordinals: Option<&[u16]>) -> Value {
    ordinals
        .map(|ordinals| Value::Bytea(encode_winner_ordinals(ordinals)))
        .unwrap_or(Value::Null)
}

fn decode_batch_ids_array_bytes(data: &[u8], label: &str) -> Result<Vec<BatchId>, EncodingError> {
    if data.len() < 4 {
        return Err(malformed(format!("{label} array too short for count")));
    }

    let count = u32::from_le_bytes(data[..4].try_into().unwrap()) as usize;
    let expected_len = 4 + count * 16;
    if data.len() != expected_len {
        return Err(malformed(format!(
            "{label} batch-id array expected {expected_len} bytes, got {}",
            data.len()
        )));
    }

    let mut batch_ids = Vec::with_capacity(count);
    for index in 0..count {
        let start = 4 + index * 16;
        let end = start + 16;
        batch_ids.push(BatchId(data[start..end].try_into().unwrap()));
    }
    Ok(batch_ids)
}

fn metadata_to_value(metadata: &RowMetadata) -> Value {
    Value::Array(
        metadata
            .iter()
            .map(|(key, value)| Value::Row {
                id: None,
                values: vec![Value::Text(key.to_string()), Value::Text(value.to_string())],
            })
            .collect(),
    )
}

fn decode_required_column_bytes<'a>(
    descriptor: &RowDescriptor,
    layout: &CompiledRowLayout,
    data: &'a [u8],
    column_index: usize,
    label: &str,
) -> Result<&'a [u8], EncodingError> {
    column_bytes_with_layout(descriptor, layout, data, column_index)?.ok_or_else(|| {
        malformed(format!(
            "expected {label} column '{}' to be non-null",
            descriptor.columns[column_index].name
        ))
    })
}

fn decode_text_bytes(bytes: &[u8], label: &str) -> Result<String, EncodingError> {
    std::str::from_utf8(bytes)
        .map(|raw| raw.to_string())
        .map_err(|err| malformed(format!("expected {label} utf8 text: {err}")))
}

fn decode_timestamp_bytes(bytes: &[u8], label: &str) -> Result<u64, EncodingError> {
    if bytes.len() != 8 {
        return Err(malformed(format!(
            "expected {label} timestamp to be 8 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
}

fn decode_bool_bytes(bytes: &[u8], label: &str) -> Result<bool, EncodingError> {
    if bytes.len() != 1 {
        return Err(malformed(format!(
            "expected {label} boolean to be 1 byte, got {}",
            bytes.len()
        )));
    }
    Ok(bytes[0] != 0)
}

fn decode_row_state_bytes(bytes: &[u8]) -> Result<RowState, EncodingError> {
    match bytes {
        [0] => Ok(RowState::StagingPending),
        [1] => Ok(RowState::Superseded),
        [2] => Ok(RowState::Rejected),
        [3] => Ok(RowState::VisibleDirect),
        [4] => Ok(RowState::VisibleTransactional),
        b"staging_pending" => Ok(RowState::StagingPending),
        b"superseded" => Ok(RowState::Superseded),
        b"rejected" => Ok(RowState::Rejected),
        b"visible_direct" => Ok(RowState::VisibleDirect),
        b"visible_transactional" => Ok(RowState::VisibleTransactional),
        _ => Err(malformed(format!(
            "invalid row state bytes '{}'",
            String::from_utf8_lossy(bytes)
        ))),
    }
}

fn decode_optional_durability_tier_bytes(
    bytes: Option<&[u8]>,
) -> Result<Option<DurabilityTier>, EncodingError> {
    match bytes {
        None => Ok(None),
        Some([0]) => Ok(Some(DurabilityTier::Local)),
        Some([1]) => Ok(Some(DurabilityTier::EdgeServer)),
        Some([2]) => Ok(Some(DurabilityTier::GlobalServer)),
        Some(b"local") => Ok(Some(DurabilityTier::Local)),
        Some(b"edge") => Ok(Some(DurabilityTier::EdgeServer)),
        Some(b"global") => Ok(Some(DurabilityTier::GlobalServer)),
        Some(bytes) => Err(malformed(format!(
            "invalid durability tier bytes '{}'",
            String::from_utf8_lossy(bytes)
        ))),
    }
}

fn decode_optional_delete_kind_bytes(
    bytes: Option<&[u8]>,
) -> Result<Option<DeleteKind>, EncodingError> {
    match bytes {
        None => Ok(None),
        Some([0]) => Ok(Some(DeleteKind::Soft)),
        Some([1]) => Ok(Some(DeleteKind::Hard)),
        Some(b"soft") => Ok(Some(DeleteKind::Soft)),
        Some(b"hard") => Ok(Some(DeleteKind::Hard)),
        Some(bytes) => Err(malformed(format!(
            "invalid delete kind bytes '{}'",
            String::from_utf8_lossy(bytes)
        ))),
    }
}

fn decode_required_batch_id_bytes(bytes: &[u8], label: &str) -> Result<BatchId, EncodingError> {
    if bytes.len() != 16 {
        return Err(malformed(format!(
            "expected {label} batch id to be 16 bytes, got {}",
            bytes.len()
        )));
    }
    Ok(BatchId(bytes.try_into().unwrap()))
}

fn decode_optional_batch_id_bytes(bytes: Option<&[u8]>) -> Result<Option<BatchId>, EncodingError> {
    bytes
        .map(|bytes| decode_required_batch_id_bytes(bytes, "optional"))
        .transpose()
}

fn decode_optional_winner_ordinals_bytes(
    bytes: Option<&[u8]>,
    label: &str,
    expected_len: usize,
) -> Result<Option<Vec<u16>>, EncodingError> {
    let Some(bytes) = bytes else {
        return Ok(None);
    };
    if bytes.len() != expected_len * 2 {
        return Err(malformed(format!(
            "{label} expected {} bytes for {expected_len} columns, got {}",
            expected_len * 2,
            bytes.len()
        )));
    }
    let mut ordinals = Vec::with_capacity(expected_len);
    for chunk in bytes.chunks_exact(2) {
        ordinals.push(u16::from_le_bytes([chunk[0], chunk[1]]));
    }
    Ok(Some(ordinals))
}

fn decode_metadata_entry_row_bytes(bytes: &[u8]) -> Result<(String, String), EncodingError> {
    if bytes.is_empty() {
        return Err(malformed("metadata row id flag missing"));
    }
    let row_bytes = match bytes[0] {
        0 => &bytes[1..],
        1 => {
            if bytes.len() < 17 {
                return Err(malformed("metadata row id too short"));
            }
            &bytes[17..]
        }
        other => {
            return Err(malformed(format!(
                "metadata row id flag must be 0 or 1, got {other}"
            )));
        }
    };

    let descriptor = metadata_entry_descriptor();
    let layout = metadata_entry_layout().as_ref();
    let key = decode_text_bytes(
        decode_required_column_bytes(descriptor, layout, row_bytes, 0, "metadata key")?,
        "metadata key",
    )?;
    let value = decode_text_bytes(
        decode_required_column_bytes(descriptor, layout, row_bytes, 1, "metadata value")?,
        "metadata value",
    )?;
    Ok((key, value))
}

fn decode_metadata_entries_array_bytes(
    bytes: &[u8],
) -> Result<Vec<(String, String)>, EncodingError> {
    if bytes.len() < 4 {
        return Err(malformed("metadata array too short for count"));
    }

    let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
    if count == 0 {
        return Ok(Vec::new());
    }

    let offset_table_start = 4;
    let offset_table_size = (count - 1) * 4;
    let data_start = offset_table_start + offset_table_size;
    if data_start > bytes.len() {
        return Err(malformed("metadata array offset table truncated"));
    }

    let mut entries = Vec::with_capacity(count);
    for index in 0..count {
        let start = if index == 0 {
            data_start
        } else {
            let offset_pos = offset_table_start + (index - 1) * 4;
            u32::from_le_bytes(bytes[offset_pos..offset_pos + 4].try_into().unwrap()) as usize
                + data_start
        };
        let end = if index + 1 < count {
            let offset_pos = offset_table_start + index * 4;
            u32::from_le_bytes(bytes[offset_pos..offset_pos + 4].try_into().unwrap()) as usize
                + data_start
        } else {
            bytes.len()
        };

        if end > bytes.len() || start > end {
            return Err(malformed("metadata array element bounds invalid"));
        }

        entries.push(decode_metadata_entry_row_bytes(&bytes[start..end])?);
    }

    Ok(entries)
}

fn decode_metadata_bytes(bytes: Option<&[u8]>) -> Result<RowMetadata, EncodingError> {
    let Some(bytes) = bytes else {
        return Ok(RowMetadata::default());
    };
    Ok(RowMetadata::from_entries(
        decode_metadata_entries_array_bytes(bytes)?,
    ))
}

fn project_user_row_data_from_physical(
    physical_descriptor: &RowDescriptor,
    physical_layout: &CompiledRowLayout,
    user_descriptor: &RowDescriptor,
    projection: &[(usize, usize)],
    data: &[u8],
    delete_kind: Option<DeleteKind>,
    is_deleted: bool,
) -> Result<Vec<u8>, EncodingError> {
    if delete_kind == Some(DeleteKind::Hard) {
        return Ok(Vec::new());
    }

    let all_user_columns_null = projection
        .iter()
        .try_fold(true, |all_null, (src_index, _)| {
            if !all_null {
                return Ok(false);
            }
            column_is_null_with_layout(physical_descriptor, physical_layout, data, *src_index)
        })?;
    if is_deleted && all_user_columns_null {
        return Ok(Vec::new());
    }

    project_row_with_layout(
        physical_descriptor,
        physical_layout,
        data,
        user_descriptor,
        projection,
    )
}

pub(crate) fn decode_flat_history_row_with_codecs(
    codecs: &FlatRowCodecs,
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
    data: &[u8],
) -> Result<StoredRowBatch, EncodingError> {
    let descriptor = codecs.history_descriptor.as_ref();
    let layout = codecs.history_layout.as_ref();
    let delete_kind =
        decode_optional_delete_kind_bytes(column_bytes_with_layout(descriptor, layout, data, 7)?)?;
    let is_deleted = decode_bool_bytes(
        decode_required_column_bytes(descriptor, layout, data, 8, "is_deleted")?,
        "is_deleted",
    )?;
    let user_data = project_user_row_data_from_physical(
        descriptor,
        layout,
        codecs.user_descriptor.as_ref(),
        &codecs.history_user_projection,
        data,
        delete_kind,
        is_deleted,
    )?;

    let parents = match column_bytes_with_layout(descriptor, layout, data, 0)? {
        None => SmallVec::new(),
        Some(bytes) => SmallVec::from_vec(decode_batch_ids_array_bytes(bytes, "parents")?),
    };

    Ok(StoredRowBatch {
        row_id,
        batch_id,
        branch: branch.into(),
        parents,
        updated_at: decode_timestamp_bytes(
            decode_required_column_bytes(descriptor, layout, data, 1, "updated_at")?,
            "updated_at",
        )?,
        created_by: decode_text_bytes(
            decode_required_column_bytes(descriptor, layout, data, 2, "created_by")?,
            "created_by",
        )?
        .into(),
        created_at: decode_timestamp_bytes(
            decode_required_column_bytes(descriptor, layout, data, 3, "created_at")?,
            "created_at",
        )?,
        updated_by: decode_text_bytes(
            decode_required_column_bytes(descriptor, layout, data, 4, "updated_by")?,
            "updated_by",
        )?
        .into(),
        state: decode_row_state_bytes(decode_required_column_bytes(
            descriptor, layout, data, 5, "state",
        )?)?,
        confirmed_tier: decode_optional_durability_tier_bytes(column_bytes_with_layout(
            descriptor, layout, data, 6,
        )?)?,
        delete_kind,
        is_deleted,
        data: user_data.into(),
        metadata: decode_metadata_bytes(column_bytes_with_layout(descriptor, layout, data, 9)?)?,
    })
}

pub(crate) fn decode_flat_visible_row_entry_with_codecs(
    codecs: &FlatRowCodecs,
    row_id: ObjectId,
    branch: &str,
    data: &[u8],
) -> Result<VisibleRowEntry, EncodingError> {
    let descriptor = codecs.visible_descriptor.as_ref();
    let layout = codecs.visible_layout.as_ref();
    let batch_id = decode_required_batch_id_bytes(
        decode_required_column_bytes(descriptor, layout, data, 0, "batch_id")?,
        "batch_id",
    )?;
    let delete_kind =
        decode_optional_delete_kind_bytes(column_bytes_with_layout(descriptor, layout, data, 7)?)?;
    let is_deleted = delete_kind.is_some();
    let current_row = StoredRowBatch {
        row_id,
        batch_id,
        branch: branch.into(),
        parents: SmallVec::new(),
        updated_at: decode_timestamp_bytes(
            decode_required_column_bytes(descriptor, layout, data, 1, "updated_at")?,
            "updated_at",
        )?,
        created_by: decode_text_bytes(
            decode_required_column_bytes(descriptor, layout, data, 2, "created_by")?,
            "created_by",
        )?
        .into(),
        created_at: decode_timestamp_bytes(
            decode_required_column_bytes(descriptor, layout, data, 3, "created_at")?,
            "created_at",
        )?,
        updated_by: decode_text_bytes(
            decode_required_column_bytes(descriptor, layout, data, 4, "updated_by")?,
            "updated_by",
        )?
        .into(),
        state: decode_row_state_bytes(decode_required_column_bytes(
            descriptor, layout, data, 5, "state",
        )?)?,
        confirmed_tier: decode_optional_durability_tier_bytes(column_bytes_with_layout(
            descriptor, layout, data, 6,
        )?)?,
        delete_kind,
        is_deleted,
        data: project_user_row_data_from_physical(
            descriptor,
            layout,
            codecs.user_descriptor.as_ref(),
            &codecs.visible_user_projection,
            data,
            delete_kind,
            is_deleted,
        )?
        .into(),
        metadata: RowMetadata::default(),
    };
    let current_batch_id = current_row.batch_id();

    Ok(VisibleRowEntry {
        current_row,
        branch_frontier: match column_bytes_with_layout(descriptor, layout, data, 8)? {
            None => vec![current_batch_id],
            Some(bytes) => decode_batch_ids_array_bytes(bytes, "branch_frontier")?,
        },
        worker_batch_id: decode_optional_batch_id_bytes(column_bytes_with_layout(
            descriptor, layout, data, 9,
        )?)?,
        edge_batch_id: decode_optional_batch_id_bytes(column_bytes_with_layout(
            descriptor, layout, data, 10,
        )?)?,
        global_batch_id: decode_optional_batch_id_bytes(column_bytes_with_layout(
            descriptor, layout, data, 11,
        )?)?,
        winner_batch_pool: match column_bytes_with_layout(descriptor, layout, data, 12)? {
            None => Vec::new(),
            Some(bytes) => decode_batch_ids_array_bytes(bytes, "winner_batch_pool")?,
        },
        current_winner_ordinals: decode_optional_winner_ordinals_bytes(
            column_bytes_with_layout(descriptor, layout, data, 13)?,
            "current_winner_ordinals",
            codecs.user_descriptor.columns.len(),
        )?,
        worker_winner_ordinals: decode_optional_winner_ordinals_bytes(
            column_bytes_with_layout(descriptor, layout, data, 14)?,
            "worker_winner_ordinals",
            codecs.user_descriptor.columns.len(),
        )?,
        edge_winner_ordinals: decode_optional_winner_ordinals_bytes(
            column_bytes_with_layout(descriptor, layout, data, 15)?,
            "edge_winner_ordinals",
            codecs.user_descriptor.columns.len(),
        )?,
        global_winner_ordinals: decode_optional_winner_ordinals_bytes(
            column_bytes_with_layout(descriptor, layout, data, 16)?,
            "global_winner_ordinals",
            codecs.user_descriptor.columns.len(),
        )?,
        merge_artifacts: column_bytes_with_layout(descriptor, layout, data, 17)?
            .map(|bytes| bytes.to_vec()),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRowBatch {
    pub batch_id: BatchId,
    pub branch: SharedString,
    pub updated_at: u64,
    pub created_by: SharedString,
    pub created_at: u64,
    pub updated_by: SharedString,
    pub state: RowState,
    pub delete_kind: Option<DeleteKind>,
    pub data: RowBytes,
}

impl QueryRowBatch {
    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    pub fn row_provenance(&self) -> RowProvenance {
        RowProvenance {
            created_by: self.created_by.to_string(),
            created_at: self.created_at,
            updated_by: self.updated_by.to_string(),
            updated_at: self.updated_at,
        }
    }

    pub fn is_soft_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Soft)
    }

    pub fn is_hard_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Hard)
    }
}

impl From<&StoredRowBatch> for QueryRowBatch {
    fn from(row: &StoredRowBatch) -> Self {
        Self {
            batch_id: row.batch_id,
            branch: row.branch.clone(),
            updated_at: row.updated_at,
            created_by: row.created_by.clone(),
            created_at: row.created_at,
            updated_by: row.updated_by.clone(),
            state: row.state,
            delete_kind: row.delete_kind,
            data: row.data.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRowBatch {
    pub row_id: ObjectId,
    pub batch_id: BatchId,
    pub branch: SharedString,
    pub parents: SmallVec<[BatchId; 2]>,
    pub updated_at: u64,
    pub created_by: SharedString,
    pub created_at: u64,
    pub updated_by: SharedString,
    pub state: RowState,
    pub confirmed_tier: Option<DurabilityTier>,
    pub delete_kind: Option<DeleteKind>,
    pub is_deleted: bool,
    pub data: RowBytes,
    pub metadata: RowMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RowMetadata(SmallVec<[(String, String); 4]>);

impl RowMetadata {
    pub fn from_entries(mut entries: Vec<(String, String)>) -> Self {
        entries.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
        entries.dedup_by(|(left_key, _), (right_key, _)| left_key == right_key);
        Self(SmallVec::from_vec(entries))
    }

    pub fn from_hash_map(metadata: HashMap<String, String>) -> Self {
        Self::from_entries(metadata.into_iter().collect())
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0
            .iter()
            .find_map(|(entry_key, value)| (entry_key == key).then_some(value))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

fn delete_kind_from_metadata(metadata: &HashMap<String, String>) -> Option<DeleteKind> {
    match metadata
        .get(MetadataKey::Delete.as_str())
        .map(String::as_str)
    {
        Some("soft") => Some(DeleteKind::Soft),
        Some("hard") => Some(DeleteKind::Hard),
        _ => None,
    }
}

impl StoredRowBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        row_id: ObjectId,
        branch: impl Into<String>,
        parents: impl IntoIterator<Item = BatchId>,
        data: Vec<u8>,
        provenance: RowProvenance,
        metadata: HashMap<String, String>,
        state: RowState,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Self {
        Self::new_with_batch_id(
            BatchId::new(),
            row_id,
            branch,
            parents,
            data,
            provenance,
            metadata,
            state,
            confirmed_tier,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_batch_id(
        batch_id: BatchId,
        row_id: ObjectId,
        branch: impl Into<String>,
        parents: impl IntoIterator<Item = BatchId>,
        data: Vec<u8>,
        provenance: RowProvenance,
        metadata: HashMap<String, String>,
        state: RowState,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Self {
        let delete_kind = delete_kind_from_metadata(&metadata);
        let is_deleted = delete_kind.is_some();
        let metadata = RowMetadata::from_hash_map(
            metadata
                .into_iter()
                .filter(|(key, _)| key != MetadataKey::Delete.as_str())
                .collect(),
        );
        let branch = SharedString::from(branch.into());
        let parents = parents.into_iter().collect::<SmallVec<[BatchId; 2]>>();

        Self {
            row_id,
            batch_id,
            branch,
            parents,
            updated_at: provenance.updated_at,
            created_by: provenance.created_by.into(),
            created_at: provenance.created_at,
            updated_by: provenance.updated_by.into(),
            state,
            confirmed_tier,
            delete_kind,
            is_deleted,
            data: data.into(),
            metadata,
        }
    }

    pub fn row_provenance(&self) -> RowProvenance {
        RowProvenance {
            created_by: self.created_by.to_string(),
            created_at: self.created_at,
            updated_by: self.updated_by.to_string(),
            updated_at: self.updated_at,
        }
    }

    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    pub fn content_digest(&self) -> Digest32 {
        compute_row_digest(
            &self.branch,
            &self.parents,
            &self.data,
            self.updated_at,
            &self.updated_by,
            (!self.metadata.is_empty()).then_some(&self.metadata),
        )
    }

    pub fn accepted_transaction_output(&self, confirmed_tier: DurabilityTier) -> Self {
        let mut row = self.clone();
        row.parents = self.parents.clone();
        row.state = RowState::VisibleTransactional;
        row.confirmed_tier = Some(confirmed_tier);
        row
    }

    pub fn is_soft_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Soft)
    }

    pub fn is_hard_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Hard) || (self.is_deleted && self.data.is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleRowEntry {
    pub current_row: StoredRowBatch,
    pub branch_frontier: Vec<BatchId>,
    pub worker_batch_id: Option<BatchId>,
    pub edge_batch_id: Option<BatchId>,
    pub global_batch_id: Option<BatchId>,
    pub winner_batch_pool: Vec<BatchId>,
    pub current_winner_ordinals: Option<Vec<u16>>,
    pub worker_winner_ordinals: Option<Vec<u16>>,
    pub edge_winner_ordinals: Option<Vec<u16>>,
    pub global_winner_ordinals: Option<Vec<u16>>,
    pub merge_artifacts: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ComputedVisiblePreview {
    row: StoredRowBatch,
    winner_batch_ids: Option<Vec<BatchId>>,
}

impl VisibleRowEntry {
    pub fn new(current_row: StoredRowBatch) -> Self {
        Self {
            branch_frontier: vec![current_row.batch_id()],
            current_row,
            worker_batch_id: None,
            edge_batch_id: None,
            global_batch_id: None,
            winner_batch_pool: Vec::new(),
            current_winner_ordinals: None,
            worker_winner_ordinals: None,
            edge_winner_ordinals: None,
            global_winner_ordinals: None,
            merge_artifacts: None,
        }
    }

    pub fn rebuild(current_row: StoredRowBatch, history_rows: &[StoredRowBatch]) -> Self {
        let current_batch_id = current_row.batch_id();
        let branch_frontier = branch_frontier(history_rows);
        let worker = latest_visible_version_for_tier(history_rows, DurabilityTier::Local);
        let worker_batch_id = worker.filter(|batch_id| *batch_id != current_batch_id);

        let edge = latest_visible_version_for_tier(history_rows, DurabilityTier::EdgeServer);
        let edge_batch_id = edge.filter(|batch_id| *batch_id != current_batch_id);

        let global = latest_visible_version_for_tier(history_rows, DurabilityTier::GlobalServer);
        let global_batch_id = global.filter(|batch_id| *batch_id != current_batch_id);

        Self {
            current_row,
            branch_frontier,
            worker_batch_id,
            edge_batch_id,
            global_batch_id,
            winner_batch_pool: Vec::new(),
            current_winner_ordinals: None,
            worker_winner_ordinals: None,
            edge_winner_ordinals: None,
            global_winner_ordinals: None,
            merge_artifacts: None,
        }
    }

    pub fn rebuild_with_descriptor(
        user_descriptor: &RowDescriptor,
        history_rows: &[StoredRowBatch],
    ) -> Result<Option<Self>, EncodingError> {
        let Some(current_preview) =
            build_computed_visible_preview(user_descriptor, history_rows, None)?
        else {
            return Ok(None);
        };
        let current_row = current_preview.row.clone();
        let branch_frontier = branch_frontier(history_rows);
        let worker_preview = build_computed_visible_preview(
            user_descriptor,
            history_rows,
            Some(DurabilityTier::Local),
        )?;
        let edge_preview = build_computed_visible_preview(
            user_descriptor,
            history_rows,
            Some(DurabilityTier::EdgeServer),
        )?;
        let global_preview = build_computed_visible_preview(
            user_descriptor,
            history_rows,
            Some(DurabilityTier::GlobalServer),
        )?;

        let mut winner_batch_pool = Vec::new();
        let mut pool_ordinals = HashMap::new();
        let current_winner_ordinals = assign_winner_ordinals(
            current_preview.winner_batch_ids.as_deref(),
            &mut winner_batch_pool,
            &mut pool_ordinals,
        )?;
        let (worker_batch_id, worker_winner_ordinals) = preview_override_sidecar(
            &current_preview,
            worker_preview.as_ref(),
            &mut winner_batch_pool,
            &mut pool_ordinals,
        )?;
        let (edge_batch_id, edge_winner_ordinals) = preview_override_sidecar(
            &current_preview,
            edge_preview.as_ref(),
            &mut winner_batch_pool,
            &mut pool_ordinals,
        )?;
        let (global_batch_id, global_winner_ordinals) = preview_override_sidecar(
            &current_preview,
            global_preview.as_ref(),
            &mut winner_batch_pool,
            &mut pool_ordinals,
        )?;

        Ok(Some(Self {
            current_row,
            branch_frontier,
            worker_batch_id,
            edge_batch_id,
            global_batch_id,
            winner_batch_pool,
            current_winner_ordinals,
            worker_winner_ordinals,
            edge_winner_ordinals,
            global_winner_ordinals,
            merge_artifacts: None,
        }))
    }

    pub fn current_batch_id(&self) -> BatchId {
        self.current_row.batch_id()
    }

    pub fn batch_id_for_tier(&self, tier: DurabilityTier) -> Option<BatchId> {
        let current = self.current_batch_id();
        if tier_satisfies(self.current_row.confirmed_tier, tier) {
            return Some(current);
        }

        match tier {
            DurabilityTier::Local => self.worker_batch_id,
            DurabilityTier::EdgeServer => self.edge_batch_id,
            DurabilityTier::GlobalServer => self.global_batch_id,
        }
    }

    fn winner_ordinals_for_tier(&self, tier: DurabilityTier) -> Option<&[u16]> {
        match tier {
            DurabilityTier::Local => self.worker_winner_ordinals.as_deref(),
            DurabilityTier::EdgeServer => self.edge_winner_ordinals.as_deref(),
            DurabilityTier::GlobalServer => self.global_winner_ordinals.as_deref(),
        }
    }

    pub fn materialize_preview_for_tier_from_loaded_rows(
        &self,
        user_descriptor: &RowDescriptor,
        tier: DurabilityTier,
        row_by_batch_id: &HashMap<BatchId, StoredRowBatch>,
    ) -> Result<Option<StoredRowBatch>, EncodingError> {
        if tier_satisfies(self.current_row.confirmed_tier, tier) {
            return Ok(Some(self.current_row.clone()));
        }

        let Some(preview_batch_id) = self.batch_id_for_tier(tier) else {
            return Ok(None);
        };
        let Some(metadata_row) = row_by_batch_id.get(&preview_batch_id).cloned() else {
            return Err(malformed(format!(
                "missing preview metadata row for batch {preview_batch_id}"
            )));
        };
        let Some(ordinals) = self.winner_ordinals_for_tier(tier) else {
            return Ok(Some(metadata_row));
        };

        let mut decoded_rows = HashMap::<BatchId, Vec<Value>>::new();
        let mut merged_values = Vec::with_capacity(ordinals.len());
        let mut contributing_rows = Vec::new();
        for (column_index, ordinal) in ordinals.iter().enumerate() {
            let pool_index = usize::from(*ordinal);
            let Some(batch_id) = self.winner_batch_pool.get(pool_index).copied() else {
                return Err(malformed(format!(
                    "winner ordinal {pool_index} out of range for pool size {}",
                    self.winner_batch_pool.len()
                )));
            };
            let Some(row) = row_by_batch_id.get(&batch_id) else {
                return Err(malformed(format!(
                    "missing winner row for batch {batch_id} in preview reconstruction"
                )));
            };
            let values = if let Some(values) = decoded_rows.get(&batch_id) {
                values
            } else {
                let values = flat_user_values(user_descriptor, &row.data)?;
                decoded_rows.insert(batch_id, values);
                decoded_rows
                    .get(&batch_id)
                    .expect("decoded row values should be cached")
            };
            merged_values.push(values[column_index].clone());
            contributing_rows.push(row);
        }

        let mut confirmed_tier = metadata_row.confirmed_tier;
        for row in contributing_rows {
            confirmed_tier = match (confirmed_tier, row.confirmed_tier) {
                (Some(existing), Some(incoming)) => Some(existing.min(incoming)),
                _ => None,
            };
            if confirmed_tier.is_none() {
                break;
            }
        }

        let data = match metadata_row.delete_kind {
            Some(DeleteKind::Hard) => Vec::new(),
            _ => encode_row(user_descriptor, &merged_values)?,
        };

        Ok(Some(StoredRowBatch {
            confirmed_tier,
            data: data.into(),
            is_deleted: metadata_row.delete_kind.is_some(),
            ..metadata_row
        }))
    }

    pub fn materialize_preview_for_tier_with_storage<H: Storage + ?Sized>(
        &self,
        io: &H,
        table: &str,
        user_descriptor: &RowDescriptor,
        tier: DurabilityTier,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        if tier_satisfies(self.current_row.confirmed_tier, tier) {
            return Ok(Some(self.current_row.clone()));
        }

        let Some(preview_batch_id) = self.batch_id_for_tier(tier) else {
            return Ok(None);
        };

        let mut row_by_batch_id = HashMap::new();
        let Some(metadata_row) = io.load_history_row_batch(
            table,
            self.current_row.branch.as_str(),
            self.current_row.row_id,
            preview_batch_id,
        )?
        else {
            return Err(StorageError::IoError(format!(
                "missing history row for preview batch {preview_batch_id}"
            )));
        };
        row_by_batch_id.insert(preview_batch_id, metadata_row);

        if let Some(ordinals) = self.winner_ordinals_for_tier(tier) {
            for ordinal in ordinals {
                let pool_index = usize::from(*ordinal);
                let Some(batch_id) = self.winner_batch_pool.get(pool_index).copied() else {
                    return Err(StorageError::IoError(format!(
                        "winner ordinal {pool_index} out of range for pool size {}",
                        self.winner_batch_pool.len()
                    )));
                };
                if row_by_batch_id.contains_key(&batch_id) {
                    continue;
                }
                let Some(row) = io.load_history_row_batch(
                    table,
                    self.current_row.branch.as_str(),
                    self.current_row.row_id,
                    batch_id,
                )?
                else {
                    return Err(StorageError::IoError(format!(
                        "missing history row for winner batch {batch_id}"
                    )));
                };
                row_by_batch_id.insert(batch_id, row);
            }
        }

        self.materialize_preview_for_tier_from_loaded_rows(user_descriptor, tier, &row_by_batch_id)
            .map_err(|err| StorageError::IoError(format!("materialize tier preview: {err}")))
    }
}

fn visible_rows_for_tier(
    history_rows: &[StoredRowBatch],
    required_tier: Option<DurabilityTier>,
) -> Vec<&StoredRowBatch> {
    history_rows
        .iter()
        .filter(|row| {
            row.state.is_visible()
                && required_tier
                    .map(|tier| tier_satisfies(row.confirmed_tier, tier))
                    .unwrap_or(true)
        })
        .collect()
}

fn latest_common_ancestor<'a>(
    frontier: &[&'a StoredRowBatch],
    row_by_batch_id: &HashMap<BatchId, &'a StoredRowBatch>,
) -> Option<&'a StoredRowBatch> {
    let mut common_ancestors: Option<std::collections::HashSet<BatchId>> = None;

    for tip in frontier {
        let mut stack = vec![tip.batch_id()];
        let mut ancestors = std::collections::HashSet::new();
        while let Some(batch_id) = stack.pop() {
            if !ancestors.insert(batch_id) {
                continue;
            }
            if let Some(row) = row_by_batch_id.get(&batch_id) {
                stack.extend(row.parents.iter().copied());
            }
        }

        common_ancestors = Some(match common_ancestors {
            None => ancestors,
            Some(mut existing) => {
                existing.retain(|batch_id| ancestors.contains(batch_id));
                existing
            }
        });
    }

    common_ancestors?
        .into_iter()
        .filter_map(|batch_id| row_by_batch_id.get(&batch_id).copied())
        .max_by_key(|row| (row.updated_at, row.batch_id()))
}

fn delete_winner<'a>(frontier: &[&'a StoredRowBatch]) -> Option<&'a StoredRowBatch> {
    frontier
        .iter()
        .copied()
        .filter(|row| row.delete_kind.is_some())
        .max_by(|left, right| {
            let left_rank = match left.delete_kind {
                Some(DeleteKind::Hard) => 2u8,
                Some(DeleteKind::Soft) => 1u8,
                None => 0u8,
            };
            let right_rank = match right.delete_kind {
                Some(DeleteKind::Hard) => 2u8,
                Some(DeleteKind::Soft) => 1u8,
                None => 0u8,
            };
            (left_rank, left.updated_at, left.batch_id()).cmp(&(
                right_rank,
                right.updated_at,
                right.batch_id(),
            ))
        })
}

fn computed_visible_preview_matches(
    current: &ComputedVisiblePreview,
    candidate: &ComputedVisiblePreview,
) -> bool {
    current.row == candidate.row && current.winner_batch_ids == candidate.winner_batch_ids
}

fn current_winner_batch_id(
    column_winner: Option<&StoredRowBatch>,
    fallback: &StoredRowBatch,
) -> BatchId {
    column_winner
        .map(StoredRowBatch::batch_id)
        .unwrap_or_else(|| fallback.batch_id())
}

#[derive(Clone, Copy)]
struct ColumnContender<'a> {
    row: &'a StoredRowBatch,
    value: &'a Value,
}

fn merge_column_with_strategy<'a>(
    column: &ColumnDescriptor,
    ancestor_value: &Value,
    contenders: &[ColumnContender<'a>],
) -> Result<(Value, Option<&'a StoredRowBatch>), EncodingError> {
    match column.merge_strategy {
        Some(ColumnMergeStrategy::Counter) => {
            let ancestor = match ancestor_value {
                Value::Integer(value) => *value,
                Value::Null => 0,
                other => {
                    return Err(malformed(format!(
                        "counter merge expected INTEGER ancestor for column '{}', got {:?}",
                        column.name_str(),
                        other
                    )));
                }
            };

            let mut delta_sum = 0i32;
            let mut latest_contributor: Option<&StoredRowBatch> = None;
            for contender in contenders {
                let contender_value = match contender.value {
                    Value::Integer(value) => *value,
                    other => {
                        return Err(malformed(format!(
                            "counter merge expected INTEGER contender for column '{}', got {:?}",
                            column.name_str(),
                            other
                        )));
                    }
                };
                let delta = contender_value.checked_sub(ancestor).ok_or_else(|| {
                    malformed(format!(
                        "counter merge delta overflow for column '{}'",
                        column.name_str()
                    ))
                })?;
                delta_sum = delta_sum.checked_add(delta).ok_or_else(|| {
                    malformed(format!(
                        "counter merge overflow for column '{}'",
                        column.name_str()
                    ))
                })?;
                if delta != 0
                    && latest_contributor
                        .map(|current| {
                            (contender.row.updated_at, contender.row.batch_id())
                                > (current.updated_at, current.batch_id())
                        })
                        .unwrap_or(true)
                {
                    latest_contributor = Some(contender.row);
                }
            }

            let merged = ancestor.checked_add(delta_sum).ok_or_else(|| {
                malformed(format!(
                    "counter merge overflow for column '{}'",
                    column.name_str()
                ))
            })?;

            Ok((Value::Integer(merged), latest_contributor))
        }
        None => {
            let mut latest_changed: Option<&StoredRowBatch> = None;
            let mut merged_value = ancestor_value.clone();

            for contender in contenders {
                if latest_changed
                    .map(|current| {
                        (contender.row.updated_at, contender.row.batch_id())
                            > (current.updated_at, current.batch_id())
                    })
                    .unwrap_or(true)
                {
                    latest_changed = Some(contender.row);
                    merged_value = contender.value.clone();
                }
            }

            Ok((merged_value, latest_changed))
        }
    }
}

fn assign_winner_ordinals(
    winner_batch_ids: Option<&[BatchId]>,
    winner_batch_pool: &mut Vec<BatchId>,
    pool_ordinals: &mut HashMap<BatchId, u16>,
) -> Result<Option<Vec<u16>>, EncodingError> {
    let Some(winner_batch_ids) = winner_batch_ids else {
        return Ok(None);
    };

    let mut ordinals = Vec::with_capacity(winner_batch_ids.len());
    for batch_id in winner_batch_ids {
        let ordinal = if let Some(existing) = pool_ordinals.get(batch_id) {
            *existing
        } else {
            let ordinal = u16::try_from(winner_batch_pool.len())
                .map_err(|_| malformed("winner batch pool exceeds u16 ordinal capacity"))?;
            winner_batch_pool.push(*batch_id);
            pool_ordinals.insert(*batch_id, ordinal);
            ordinal
        };
        ordinals.push(ordinal);
    }

    Ok(Some(ordinals))
}

fn preview_override_sidecar(
    current_preview: &ComputedVisiblePreview,
    candidate_preview: Option<&ComputedVisiblePreview>,
    winner_batch_pool: &mut Vec<BatchId>,
    pool_ordinals: &mut HashMap<BatchId, u16>,
) -> Result<(Option<BatchId>, Option<Vec<u16>>), EncodingError> {
    let Some(candidate_preview) = candidate_preview else {
        return Ok((None, None));
    };
    if computed_visible_preview_matches(current_preview, candidate_preview) {
        return Ok((None, None));
    }

    Ok((
        Some(candidate_preview.row.batch_id()),
        assign_winner_ordinals(
            candidate_preview.winner_batch_ids.as_deref(),
            winner_batch_pool,
            pool_ordinals,
        )?,
    ))
}

fn build_computed_visible_preview(
    user_descriptor: &RowDescriptor,
    history_rows: &[StoredRowBatch],
    required_tier: Option<DurabilityTier>,
) -> Result<Option<ComputedVisiblePreview>, EncodingError> {
    let visible_rows = visible_rows_for_tier(history_rows, required_tier);
    if visible_rows.is_empty() {
        return Ok(None);
    }

    let mut non_tips = std::collections::BTreeSet::new();
    for row in &visible_rows {
        for parent in &row.parents {
            non_tips.insert(*parent);
        }
    }
    let mut frontier: Vec<_> = visible_rows
        .iter()
        .copied()
        .filter(|row| !non_tips.contains(&row.batch_id()))
        .collect();
    frontier.sort_by_key(|row| (row.updated_at, row.batch_id()));
    frontier.dedup_by_key(|row| row.batch_id());
    let Some(latest_tip) = frontier.last().copied() else {
        return Ok(None);
    };
    if frontier.len() == 1 {
        return Ok(Some(ComputedVisiblePreview {
            row: latest_tip.clone(),
            winner_batch_ids: None,
        }));
    }

    let row_by_batch_id = visible_rows
        .iter()
        .copied()
        .map(|row| (row.batch_id(), row))
        .collect::<HashMap<_, _>>();
    let ancestor = latest_common_ancestor(&frontier, &row_by_batch_id);

    let ancestor_values = match ancestor {
        Some(row) => flat_user_values(user_descriptor, &row.data)?,
        None => user_descriptor
            .columns
            .iter()
            .map(|_| Value::Null)
            .collect(),
    };
    let frontier_values = frontier
        .iter()
        .map(|row| flat_user_values(user_descriptor, &row.data))
        .collect::<Result<Vec<_>, _>>()?;

    let mut merged_values = Vec::with_capacity(user_descriptor.columns.len());
    let mut contributing_rows: Vec<&StoredRowBatch> = Vec::new();
    let mut winner_batch_ids = Vec::with_capacity(user_descriptor.columns.len());

    for column_index in 0..user_descriptor.columns.len() {
        let ancestor_value = ancestor_values[column_index].clone();
        let changed_contenders = frontier
            .iter()
            .zip(frontier_values.iter())
            .filter_map(|(row, row_values)| {
                let candidate_value = &row_values[column_index];
                (candidate_value != &ancestor_value).then_some(ColumnContender {
                    row,
                    value: candidate_value,
                })
            })
            .collect::<Vec<_>>();
        let (best_value, best_changed) = merge_column_with_strategy(
            &user_descriptor.columns[column_index],
            &ancestor_value,
            &changed_contenders,
        )?;

        merged_values.push(best_value);
        let winner_row = best_changed.or(ancestor).unwrap_or(latest_tip);
        winner_batch_ids.push(current_winner_batch_id(Some(winner_row), latest_tip));
        contributing_rows.push(winner_row);
    }

    let delete_winner = delete_winner(&frontier);
    let metadata_row = delete_winner.unwrap_or_else(|| {
        contributing_rows
            .iter()
            .copied()
            .max_by_key(|row| (row.updated_at, row.batch_id()))
            .unwrap_or(latest_tip)
    });

    let mut confirmed_tier: Option<DurabilityTier> = None;
    for tier in contributing_rows
        .iter()
        .copied()
        .chain(delete_winner)
        .map(|row| row.confirmed_tier)
    {
        let Some(tier) = tier else {
            confirmed_tier = None;
            break;
        };
        confirmed_tier = Some(match confirmed_tier {
            Some(existing) => existing.min(tier),
            None => tier,
        });
    }

    let data = match delete_winner.and_then(|row| row.delete_kind) {
        Some(DeleteKind::Hard) => Vec::new(),
        _ => encode_row(user_descriptor, &merged_values)?,
    };

    let row = StoredRowBatch {
        row_id: metadata_row.row_id,
        batch_id: metadata_row.batch_id,
        branch: metadata_row.branch.clone(),
        parents: metadata_row.parents.clone(),
        updated_at: metadata_row.updated_at,
        created_by: metadata_row.created_by.clone(),
        created_at: metadata_row.created_at,
        updated_by: metadata_row.updated_by.clone(),
        state: metadata_row.state,
        confirmed_tier,
        delete_kind: delete_winner.and_then(|row| row.delete_kind),
        is_deleted: delete_winner.is_some(),
        data: data.into(),
        metadata: metadata_row.metadata.clone(),
    };

    let winner_batch_ids = if winner_batch_ids
        .iter()
        .all(|batch_id| *batch_id == metadata_row.batch_id())
        && row.data == metadata_row.data
        && row.confirmed_tier == metadata_row.confirmed_tier
        && row.delete_kind == metadata_row.delete_kind
        && row.is_deleted == metadata_row.is_deleted
    {
        None
    } else {
        Some(winner_batch_ids)
    };

    Ok(Some(ComputedVisiblePreview {
        row,
        winner_batch_ids,
    }))
}

pub(crate) fn visible_row_preview_from_history_rows(
    user_descriptor: &RowDescriptor,
    history_rows: &[StoredRowBatch],
    required_tier: Option<DurabilityTier>,
) -> Result<Option<StoredRowBatch>, EncodingError> {
    Ok(
        build_computed_visible_preview(user_descriptor, history_rows, required_tier)?
            .map(|preview| preview.row),
    )
}

#[derive(Debug, Clone)]
struct RowBatchApply {
    row_locator: RowLocator,
    previous_visible: Option<StoredRowBatch>,
    current_visible: Option<StoredRowBatch>,
    is_new_object: bool,
    visible_changed: bool,
}

fn row_locator_from_storage<H: Storage>(
    io: &H,
    object_id: ObjectId,
) -> Result<RowLocator, RowHistoryError> {
    io.load_row_locator(object_id)
        .map_err(RowHistoryError::StorageError)?
        .ok_or(RowHistoryError::ObjectNotFound(object_id))
}

fn load_branch_history<H: Storage>(
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

fn rebuild_visible_entry_from_history<H: Storage>(
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

fn visible_entry_from_history_rows(
    user_descriptor: &RowDescriptor,
    history_rows: &[StoredRowBatch],
) -> Result<Option<VisibleRowEntry>, EncodingError> {
    VisibleRowEntry::rebuild_with_descriptor(user_descriptor, history_rows)
}

fn load_previous_visible_entry<H: Storage>(
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

fn visibility_change_from_applied(
    object_id: ObjectId,
    applied: RowBatchApply,
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

fn supersede_older_staging_rows_for_batch<H: Storage>(
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
    let batch_id = row.batch_id();
    let branch = SharedString::from(branch_name.as_str().to_string());
    let context = crate::storage::resolve_history_row_write_context(io, &table, &row)
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

    for parent in &row.parents {
        if io
            .load_history_row_batch(&table, branch_name.as_str(), object_id, *parent)
            .map_err(RowHistoryError::StorageError)?
            .is_none()
        {
            return Err(RowHistoryError::ParentNotFound(*parent));
        }
    }

    let mut patched_history = load_branch_history(io, &table, object_id, &branch)?;

    if let Some(existing_row) = io
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

    let applied = RowBatchApply {
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

    if state.is_none()
        && let Some(previous_entry) = previous_entry.as_ref()
        && previous_entry.current_row.batch_id() == batch_id
        && previous_entry.current_row.branch.as_str() == branch_name.as_str()
    {
        let encoded_history =
            crate::storage::encode_history_row_bytes_with_context(&context, &patched_row)
                .map_err(RowHistoryError::StorageError)?;
        let mut patched_entry = previous_entry.clone();
        patched_entry.current_row.confirmed_tier = patched_row.confirmed_tier;
        let encoded_visible =
            crate::storage::encode_visible_row_bytes_with_context(&context, &patched_entry)
                .map_err(RowHistoryError::StorageError)?;
        io.apply_prepared_row_mutation(
            &table,
            std::slice::from_ref(&patched_row),
            std::slice::from_ref(&patched_entry),
            std::slice::from_ref(&encoded_history),
            std::slice::from_ref(&encoded_visible),
            &[],
        )
        .map_err(RowHistoryError::StorageError)?;

        let current_visible = patched_entry.current_row.clone();
        if previous_visible.as_ref() == Some(&current_visible) {
            return Ok(None);
        }

        return Ok(Some(RowVisibilityChange {
            object_id,
            row_locator,
            row: current_visible,
            previous_row: previous_visible.clone(),
            is_new_object: previous_visible.is_none(),
        }));
    }

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

fn latest_visible_version_for_tier(
    history_rows: &[StoredRowBatch],
    required_tier: DurabilityTier,
) -> Option<BatchId> {
    history_rows
        .iter()
        .filter(|row| row.state.is_visible() && tier_satisfies(row.confirmed_tier, required_tier))
        .max_by_key(|row| (row.updated_at, row.batch_id()))
        .map(StoredRowBatch::batch_id)
}

fn branch_frontier(history_rows: &[StoredRowBatch]) -> Vec<BatchId> {
    let mut non_tips = std::collections::BTreeSet::new();
    for row in history_rows.iter().filter(|row| row.state.is_visible()) {
        for parent in &row.parents {
            non_tips.insert(*parent);
        }
    }

    let mut tips: Vec<_> = history_rows
        .iter()
        .filter(|row| row.state.is_visible())
        .map(StoredRowBatch::batch_id)
        .filter(|batch_id| !non_tips.contains(batch_id))
        .collect();
    tips.sort();
    tips.dedup();
    tips
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::RowProvenance;
    use crate::row_format::decode_row;

    fn visible_row(updated_at: u64, confirmed_tier: Option<DurabilityTier>) -> StoredRowBatch {
        StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            vec![updated_at as u8],
            RowProvenance::for_insert("alice".to_string(), updated_at),
            HashMap::new(),
            RowState::VisibleDirect,
            confirmed_tier,
        )
    }

    #[test]
    fn flat_visible_row_binary_roundtrips_retained_visible_columns() {
        let user_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean).nullable(),
        ]);
        let global = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(21)),
            "main",
            Vec::new(),
            encode_row(
                &user_descriptor,
                &[Value::Text("ship it".into()), Value::Boolean(true)],
            )
            .expect("encode global row"),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::from([("source".to_string(), "global".to_string())]),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let current = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![global.batch_id()],
            encode_row(
                &user_descriptor,
                &[Value::Text("ship it".into()), Value::Boolean(false)],
            )
            .expect("encode current row"),
            RowProvenance::for_update(&global.row_provenance(), "bob".to_string(), 30),
            HashMap::from([("source".to_string(), "local".to_string())]),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let entry = VisibleRowEntry {
            current_row: current,
            branch_frontier: vec![global.batch_id()],
            worker_batch_id: None,
            edge_batch_id: Some(global.batch_id()),
            global_batch_id: Some(global.batch_id()),
            winner_batch_pool: Vec::new(),
            current_winner_ordinals: None,
            worker_winner_ordinals: None,
            edge_winner_ordinals: None,
            global_winner_ordinals: None,
            merge_artifacts: Some(vec![1, 2, 3, 4]),
        };

        let encoded =
            encode_flat_visible_row_entry(&user_descriptor, &entry).expect("encode flat visible");
        let decoded = decode_flat_visible_row_entry(
            &user_descriptor,
            entry.current_row.row_id,
            entry.current_row.branch.as_str(),
            &encoded,
        )
        .expect("decode flat visible");

        assert_eq!(decoded.current_row.row_id, entry.current_row.row_id);
        assert_eq!(decoded.current_row.batch_id(), entry.current_row.batch_id());
        assert_eq!(decoded.current_row.branch, entry.current_row.branch);
        assert!(decoded.current_row.parents.is_empty());
        assert_eq!(decoded.current_row.updated_at, entry.current_row.updated_at);
        assert_eq!(decoded.current_row.created_by, entry.current_row.created_by);
        assert_eq!(decoded.current_row.created_at, entry.current_row.created_at);
        assert_eq!(decoded.current_row.updated_by, entry.current_row.updated_by);
        assert_eq!(decoded.current_row.state, entry.current_row.state);
        assert_eq!(
            decoded.current_row.confirmed_tier,
            entry.current_row.confirmed_tier
        );
        assert_eq!(
            decoded.current_row.delete_kind,
            entry.current_row.delete_kind
        );
        assert!(decoded.current_row.metadata.is_empty());
        assert_eq!(decoded.current_row.data, entry.current_row.data);
        assert_eq!(decoded.branch_frontier, entry.branch_frontier);
        assert_eq!(decoded.worker_batch_id, entry.worker_batch_id);
        assert_eq!(decoded.edge_batch_id, entry.edge_batch_id);
        assert_eq!(decoded.global_batch_id, entry.global_batch_id);
        assert_eq!(decoded.merge_artifacts, entry.merge_artifacts);
    }

    #[test]
    fn visible_row_entry_omits_tier_pointers_when_current_is_globally_confirmed() {
        let current = visible_row(30, Some(DurabilityTier::GlobalServer));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, None);
        assert_eq!(entry.global_batch_id, None);
        assert_eq!(entry.merge_artifacts, None);
    }

    #[test]
    fn visible_row_entry_resolves_tier_fallback_chain() {
        let global = visible_row(10, Some(DurabilityTier::GlobalServer));
        let edge = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![global.batch_id()],
            vec![2],
            RowProvenance::for_update(&global.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let current = StoredRowBatch::new(
            global.row_id,
            "main",
            vec![edge.batch_id()],
            vec![3],
            RowProvenance::for_update(&edge.row_provenance(), "alice".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history = vec![global.clone(), edge.clone(), current.clone()];

        let entry = VisibleRowEntry::rebuild(current.clone(), &history);

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.worker_batch_id, None);
        assert_eq!(entry.edge_batch_id, Some(edge.batch_id()));
        assert_eq!(entry.global_batch_id, Some(global.batch_id()));
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::Local),
            Some(current.batch_id())
        );
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::EdgeServer),
            Some(edge.batch_id())
        );
        assert_eq!(
            entry.batch_id_for_tier(DurabilityTier::GlobalServer),
            Some(global.batch_id())
        );
    }

    #[test]
    fn visible_row_entry_returns_none_when_no_version_meets_required_tier() {
        let current = visible_row(30, Some(DurabilityTier::Local));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.branch_frontier, vec![current.batch_id()]);
        assert_eq!(entry.batch_id_for_tier(DurabilityTier::EdgeServer), None);
        assert_eq!(entry.batch_id_for_tier(DurabilityTier::GlobalServer), None);
    }

    #[test]
    fn visible_row_entry_preserves_multiple_branch_tips() {
        let base = visible_row(10, Some(DurabilityTier::Local));
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            vec![1],
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            vec![2],
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild(right.clone(), &[base, left.clone(), right.clone()]);

        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
    }

    #[test]
    fn visible_row_entry_merges_conflicting_field_updates() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("alice-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[base, left.clone(), right.clone()],
        )
        .unwrap()
        .expect("merged visible entry");

        assert_eq!(
            decode_row(&descriptor, &entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Boolean(true)]
        );
        assert_eq!(entry.current_row.batch_id(), right.batch_id());
        assert_eq!(entry.current_row.updated_by.as_str(), "bob");
        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
    }

    #[test]
    fn visible_row_entry_applies_counter_merge_strategy_per_column() {
        let descriptor = counter_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Integer(5)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("alice-title".into()), Value::Integer(7)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Integer(4)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[base, left.clone(), right.clone()],
        )
        .unwrap()
        .expect("merged visible entry");

        assert_eq!(
            decode_row(&descriptor, &entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Integer(6)]
        );
        assert_eq!(entry.current_row.batch_id(), right.batch_id());
        assert_eq!(entry.current_row.updated_by.as_str(), "bob");
        assert_eq!(
            entry.branch_frontier,
            vec![left.batch_id(), right.batch_id()]
        );
        assert_eq!(
            entry.winner_batch_pool,
            vec![left.batch_id(), right.batch_id()]
        );
        assert_eq!(entry.current_winner_ordinals, Some(vec![0, 1]));
    }

    #[test]
    fn visible_row_entry_uses_consumer_schema_merge_strategy() {
        let counter_descriptor = counter_descriptor();
        let lww_descriptor = lww_integer_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &counter_descriptor,
                &[Value::Text("task".into()), Value::Integer(5)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &counter_descriptor,
                &[Value::Text("alice-title".into()), Value::Integer(7)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &counter_descriptor,
                &[Value::Text("task".into()), Value::Integer(4)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history = vec![base, left, right];

        let counter_entry = VisibleRowEntry::rebuild_with_descriptor(&counter_descriptor, &history)
            .unwrap()
            .expect("counter merged visible entry");
        let lww_entry = VisibleRowEntry::rebuild_with_descriptor(&lww_descriptor, &history)
            .unwrap()
            .expect("lww merged visible entry");

        assert_eq!(
            decode_row(&counter_descriptor, &counter_entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Integer(6)]
        );
        assert_eq!(
            decode_row(&lww_descriptor, &lww_entry.current_row.data).unwrap(),
            vec![Value::Text("alice-title".into()), Value::Integer(4)]
        );
    }

    #[test]
    fn visible_row_entry_errors_when_counter_merge_overflows() {
        let descriptor = counter_only_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(&descriptor, &[Value::Integer(i32::MAX - 1)]).unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(&descriptor, &[Value::Integer(i32::MAX)]).unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(&descriptor, &[Value::Integer(i32::MAX)]).unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let error = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &[base, left, right])
            .expect_err("counter overflow should fail");

        assert!(
            error.to_string().contains("overflow"),
            "expected overflow error, got {error}"
        );
    }

    #[test]
    fn visible_row_entry_merges_accepted_transactional_rows_but_ignores_staging_and_rejected_rows()
    {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let accepted_transaction = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("txn-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleTransactional,
            Some(DurabilityTier::Local),
        );
        let direct = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let staging = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[
                    Value::Text("staging-should-not-win".into()),
                    Value::Boolean(false),
                ],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "mallory".to_string(), 30),
            HashMap::new(),
            RowState::StagingPending,
            None,
        );
        let rejected = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[
                    Value::Text("rejected-should-not-win".into()),
                    Value::Boolean(false),
                ],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "mallory".to_string(), 31),
            HashMap::new(),
            RowState::Rejected,
            None,
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[
                base,
                accepted_transaction,
                direct.clone(),
                staging,
                rejected,
            ],
        )
        .unwrap()
        .expect("merged visible entry");

        assert_eq!(
            decode_row(&descriptor, &entry.current_row.data).unwrap(),
            vec![Value::Text("txn-title".into()), Value::Boolean(true)]
        );
        assert_eq!(entry.current_row.batch_id(), direct.batch_id());
        assert_eq!(entry.current_row.updated_by.as_str(), "bob");
    }

    #[test]
    fn visible_row_entry_roundtrips_current_winner_ordinals() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let left = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("alice-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let right = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let entry = VisibleRowEntry::rebuild_with_descriptor(
            &descriptor,
            &[base, left.clone(), right.clone()],
        )
        .unwrap()
        .expect("merged visible entry");
        assert_eq!(
            entry.winner_batch_pool,
            vec![left.batch_id(), right.batch_id()]
        );
        assert_eq!(entry.current_winner_ordinals, Some(vec![0, 1]));

        let encoded =
            encode_flat_visible_row_entry(&descriptor, &entry).expect("encode merged visible row");
        let decoded = decode_flat_visible_row_entry(
            &descriptor,
            entry.current_row.row_id,
            entry.current_row.branch.as_str(),
            &encoded,
        )
        .expect("decode merged visible row");

        assert_eq!(decoded.winner_batch_pool, entry.winner_batch_pool);
        assert_eq!(
            decoded.current_winner_ordinals,
            entry.current_winner_ordinals
        );
        assert_eq!(decoded.edge_winner_ordinals, None);
        assert_eq!(decoded.global_winner_ordinals, None);
    }

    #[test]
    fn visible_row_entry_materializes_tier_preview_when_batch_id_matches_current() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let worker_done = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let edge_title = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let history_rows = vec![base.clone(), worker_done.clone(), edge_title.clone()];
        let row_by_batch_id = history_rows
            .iter()
            .cloned()
            .map(|row| (row.batch_id(), row))
            .collect::<HashMap<_, _>>();

        let entry = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &history_rows)
            .unwrap()
            .expect("visible entry");

        assert_eq!(entry.current_row.batch_id(), edge_title.batch_id());
        assert_eq!(entry.edge_batch_id, Some(edge_title.batch_id()));
        assert_eq!(entry.edge_winner_ordinals, None);

        let edge_preview = entry
            .materialize_preview_for_tier_from_loaded_rows(
                &descriptor,
                DurabilityTier::EdgeServer,
                &row_by_batch_id,
            )
            .unwrap()
            .expect("edge preview");
        assert_eq!(
            decode_row(&descriptor, &edge_preview.data).unwrap(),
            vec![Value::Text("edge-title".into()), Value::Boolean(false)]
        );
    }

    #[test]
    fn visible_row_entry_persists_merged_tier_override_ordinals() {
        let descriptor = user_descriptor();
        let base = StoredRowBatch::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_insert("alice".to_string(), 10),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::GlobalServer),
        );
        let edge_title = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(false)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "alice".to_string(), 20),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let edge_done = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![base.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("task".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&base.row_provenance(), "bob".to_string(), 21),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::EdgeServer),
        );
        let worker_current = StoredRowBatch::new(
            base.row_id,
            "main",
            vec![edge_title.batch_id(), edge_done.batch_id()],
            encode_row(
                &descriptor,
                &[Value::Text("edge-title".into()), Value::Boolean(true)],
            )
            .unwrap(),
            RowProvenance::for_update(&edge_done.row_provenance(), "charlie".to_string(), 30),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );
        let history_rows = vec![
            base.clone(),
            edge_title.clone(),
            edge_done.clone(),
            worker_current.clone(),
        ];
        let row_by_batch_id = history_rows
            .iter()
            .cloned()
            .map(|row| (row.batch_id(), row))
            .collect::<HashMap<_, _>>();

        let entry = VisibleRowEntry::rebuild_with_descriptor(&descriptor, &history_rows)
            .unwrap()
            .expect("visible entry");

        assert_eq!(entry.current_row.batch_id(), worker_current.batch_id());
        assert_eq!(entry.current_winner_ordinals, None);
        assert_eq!(entry.edge_batch_id, Some(edge_done.batch_id()));
        assert_eq!(
            entry.winner_batch_pool,
            vec![edge_title.batch_id(), edge_done.batch_id()]
        );
        assert_eq!(entry.edge_winner_ordinals, Some(vec![0, 1]));

        let edge_preview = entry
            .materialize_preview_for_tier_from_loaded_rows(
                &descriptor,
                DurabilityTier::EdgeServer,
                &row_by_batch_id,
            )
            .unwrap()
            .expect("edge preview");
        assert_eq!(edge_preview.batch_id(), edge_done.batch_id());
        assert_eq!(
            decode_row(&descriptor, &edge_preview.data).unwrap(),
            vec![Value::Text("edge-title".into()), Value::Boolean(true)]
        );
    }

    fn user_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
        ])
    }

    fn lww_integer_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("count", ColumnType::Integer),
        ])
    }

    fn counter_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("count", ColumnType::Integer)
                .merge_strategy(ColumnMergeStrategy::Counter),
        ])
    }

    fn counter_only_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("count", ColumnType::Integer)
                .merge_strategy(ColumnMergeStrategy::Counter),
        ])
    }

    #[test]
    fn history_row_physical_descriptor_appends_nullable_user_columns() {
        let descriptor = history_row_physical_descriptor(&user_descriptor());

        let title = descriptor
            .column("title")
            .expect("physical descriptor should contain title");
        assert!(title.nullable, "physical user columns should be nullable");

        let done = descriptor
            .column("done")
            .expect("physical descriptor should contain done");
        assert!(done.nullable, "physical user columns should be nullable");
    }

    #[test]
    fn history_row_physical_descriptor_omits_key_derived_and_marker_columns() {
        let descriptor = history_row_physical_descriptor(&user_descriptor());

        assert_eq!(
            descriptor
                .columns
                .iter()
                .filter(|column| column.name == "_jazz_batch_id")
                .count(),
            0,
            "flat history rows should not store batch identity from the key in the payload"
        );
        assert!(
            descriptor.column("_jazz_format_id").is_none(),
            "flat history rows should not need an in-payload format marker once decoding is key-aware"
        );
        assert!(
            descriptor.column("_jazz_row_id").is_none(),
            "flat history rows should not store row id from the key in the payload"
        );
        assert!(
            descriptor.column("_jazz_branch").is_none(),
            "flat history rows should not store branch from the key in the payload"
        );
    }

    #[test]
    fn visible_row_physical_descriptor_keeps_current_batch_id_but_omits_marker() {
        let descriptor = visible_row_physical_descriptor(&user_descriptor());

        assert!(
            descriptor.column("_jazz_format_id").is_none(),
            "visible rows should not need an in-payload format marker once keyed decoding is available"
        );
        assert_eq!(
            descriptor
                .columns
                .iter()
                .filter(|column| column.name == "_jazz_batch_id")
                .count(),
            1,
            "visible rows should keep the current visible batch id in the flat payload"
        );
        assert!(
            descriptor.column("_jazz_row_id").is_none(),
            "visible rows should derive row id from the storage key"
        );
        assert!(
            descriptor.column("_jazz_branch").is_none(),
            "visible rows should derive branch from the storage key"
        );
        assert!(
            descriptor.column("_jazz_parents").is_none(),
            "visible rows should not duplicate history parents in the hot visible payload"
        );
        assert!(
            descriptor.column("_jazz_metadata").is_none(),
            "visible rows should not duplicate history metadata in the hot visible payload"
        );
        assert!(
            descriptor.column("_jazz_is_deleted").is_none(),
            "visible rows should derive deletion state from delete_kind in the hot payload"
        );
    }

    #[test]
    fn flat_visible_row_common_case_omits_empty_arrays_and_metadata() {
        let descriptor = user_descriptor();
        let current = visible_row(10, Some(DurabilityTier::Local));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        let encoded =
            encode_flat_visible_row_entry(&descriptor, &entry).expect("encode visible row");
        let values = decode_row(&visible_row_physical_descriptor(&descriptor), &encoded)
            .expect("decode visible row");

        assert_eq!(
            values[8],
            Value::Null,
            "singleton frontier matching current batch should be implicit"
        );
    }

    #[test]
    fn flat_history_row_binary_roundtrips_user_and_system_columns() {
        let user_descriptor = user_descriptor();
        let user_values = vec![Value::Text("Write docs".into()), Value::Boolean(false)];
        let user_data = crate::row_format::encode_row(&user_descriptor, &user_values).unwrap();
        let row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(42)),
            "main",
            vec![BatchId([9; 16])],
            user_data.clone(),
            RowProvenance {
                created_by: "alice".to_string(),
                created_at: 100,
                updated_by: "bob".to_string(),
                updated_at: 123,
            },
            HashMap::from([("source".to_string(), "test".to_string())]),
            RowState::VisibleTransactional,
            Some(DurabilityTier::EdgeServer),
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let decoded = decode_flat_history_row(
            &user_descriptor,
            row.row_id,
            row.branch.as_str(),
            row.batch_id(),
            &encoded,
        )
        .expect("decode flat history row");

        assert_eq!(decoded, row);

        let physical_descriptor = history_row_physical_descriptor(&user_descriptor);
        let physical_values = decode_row(&physical_descriptor, &encoded).expect("decode values");
        assert_eq!(
            physical_values[physical_descriptor.column_index("title").unwrap()],
            Value::Text("Write docs".into())
        );
        assert_eq!(
            physical_values[physical_descriptor.column_index("done").unwrap()],
            Value::Boolean(false)
        );
    }

    #[test]
    fn flat_history_row_binary_roundtrips_nonempty_metadata() {
        let user_descriptor = user_descriptor();
        let row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(44)),
            "main",
            vec![BatchId([3; 16])],
            encode_row(
                &user_descriptor,
                &[Value::Text("Ship".into()), Value::Boolean(true)],
            )
            .expect("encode user row"),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::from([
                ("source".to_string(), "local".to_string()),
                ("kind".to_string(), "task".to_string()),
            ]),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let decoded = decode_flat_history_row(
            &user_descriptor,
            row.row_id,
            row.branch.as_str(),
            row.batch_id(),
            &encoded,
        )
        .expect("decode flat history row");

        assert_eq!(decoded.metadata, row.metadata);
    }

    #[test]
    fn flat_history_row_hard_delete_uses_null_user_columns() {
        let user_descriptor = user_descriptor();
        let deleted = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(43)),
            "main",
            vec![BatchId([7; 16])],
            vec![],
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::from([(
                crate::metadata::MetadataKey::Delete.to_string(),
                "hard".to_string(),
            )]),
            RowState::VisibleDirect,
            None,
        );

        let encoded =
            encode_flat_history_row(&user_descriptor, &deleted).expect("encode hard delete");
        let physical_descriptor = history_row_physical_descriptor(&user_descriptor);
        let physical_values = decode_row(&physical_descriptor, &encoded).expect("decode values");

        assert_eq!(
            physical_values[physical_descriptor.column_index("title").unwrap()],
            Value::Null
        );
        assert_eq!(
            physical_values[physical_descriptor.column_index("done").unwrap()],
            Value::Null
        );

        let decoded = decode_flat_history_row(
            &user_descriptor,
            deleted.row_id,
            deleted.branch.as_str(),
            deleted.batch_id(),
            &encoded,
        )
        .expect("decode hard delete");
        assert_eq!(decoded.data.as_ref(), &[] as &[u8]);
        assert!(decoded.is_hard_deleted());
    }

    #[test]
    fn flat_history_row_binary_compacts_hot_enums_to_single_bytes() {
        let user_descriptor = user_descriptor();
        let mut row = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(45)),
            "main",
            vec![BatchId([4; 16])],
            encode_row(
                &user_descriptor,
                &[Value::Text("Compact".into()), Value::Boolean(false)],
            )
            .expect("encode user row"),
            RowProvenance::for_insert("alice".to_string(), 100),
            HashMap::new(),
            RowState::VisibleTransactional,
            Some(DurabilityTier::EdgeServer),
        );
        row.delete_kind = Some(DeleteKind::Hard);

        let encoded =
            encode_flat_history_row(&user_descriptor, &row).expect("encode flat history row");
        let descriptor = history_row_physical_descriptor(&user_descriptor);
        let layout = crate::row_format::compiled_row_layout(&descriptor);

        let state = crate::row_format::column_bytes_with_layout(
            &descriptor,
            layout.as_ref(),
            &encoded,
            descriptor.column_index("_jazz_state").unwrap(),
        )
        .expect("read state bytes")
        .expect("state should be present");
        let tier = crate::row_format::column_bytes_with_layout(
            &descriptor,
            layout.as_ref(),
            &encoded,
            descriptor.column_index("_jazz_confirmed_tier").unwrap(),
        )
        .expect("read tier bytes")
        .expect("tier should be present");
        let delete_kind = crate::row_format::column_bytes_with_layout(
            &descriptor,
            layout.as_ref(),
            &encoded,
            descriptor.column_index("_jazz_delete_kind").unwrap(),
        )
        .expect("read delete kind bytes")
        .expect("delete kind should be present");

        assert_eq!(state.len(), 1);
        assert_eq!(tier.len(), 1);
        assert_eq!(delete_kind.len(), 1);
    }

    #[test]
    fn direct_row_writes_use_batch_identity() {
        let provenance = RowProvenance::for_insert("alice".to_string(), 100);
        let first = StoredRowBatch::new(
            ObjectId::from_uuid(Uuid::from_u128(101)),
            "main",
            Vec::new(),
            vec![1, 2, 3],
            provenance.clone(),
            HashMap::new(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        );

        assert_eq!(
            first.batch_id(),
            first.batch_id,
            "direct visible rows should publish under their batch identity"
        );
    }
}
