//! Synchronous Storage trait and implementations.
//!
//! This is the foundation of the sync storage architecture. All storage
//! and index operations are synchronous - they return immediately with results.
//!
//! # Design: Single-threaded
//!
//! No `Send + Sync` bounds on Storage. Each thread (main, worker) has its own
//! Storage instance. Cross-thread communication uses the sync protocol over
//! postMessage, not shared mutable state.

#[cfg(test)]
pub mod conformance;
mod key_codec;
mod memory;
mod opfs_btree;
mod storage_core;
mod storage_trait;
pub use memory::MemoryStorage;
pub use opfs_btree::OpfsBTreeStorage;
pub use storage_trait::Storage;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
mod rocksdb;
#[cfg(all(feature = "rocksdb", not(target_arch = "wasm32")))]
pub use rocksdb::RocksDBStorage;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
mod sqlite;
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
pub use sqlite::SqliteStorage;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use smolset::SmolSet;

use crate::batch_fate::{
    BatchSettlement, CapturedFrontierMember, LocalBatchRecord, SealedBatchSubmission,
};
use crate::catalogue::CatalogueEntry;
use crate::digest::Digest32;
use crate::metadata::MetadataKey;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, ComposedBranchName, RowDescriptor, SchemaHash, SharedString,
    Value,
};
use crate::row_format::{decode_row, encode_row};
use crate::row_histories::{
    BatchId, FlatRowCodecs, HistoryScan, QueryRowBatch, RowState, StoredRowBatch, VisibleRowEntry,
    decode_flat_history_row_with_codecs, decode_flat_visible_row_entry_with_codecs,
    flat_row_codecs,
};
use crate::sync_manager::DurabilityTier;

// ============================================================================
// Storage Types
// ============================================================================

type EncodedTableRowHistories = BTreeMap<ObjectId, BTreeMap<(SharedString, BatchId), Vec<u8>>>;

pub(super) fn batch_settlement_confirmed_tier_for_row(
    settlement: &BatchSettlement,
    row: &StoredRowBatch,
) -> Option<DurabilityTier> {
    let confirmed_tier = settlement.confirmed_tier()?;
    let is_visible_member = match settlement {
        BatchSettlement::DurableDirect {
            visible_members, ..
        }
        | BatchSettlement::AcceptedTransaction {
            visible_members, ..
        } => visible_members.iter().any(|member| {
            member.object_id == row.row_id
                && member.branch_name.as_str() == row.branch.as_str()
                && member.batch_id == row.batch_id
        }),
        BatchSettlement::Missing { .. } | BatchSettlement::Rejected { .. } => false,
    };
    is_visible_member.then_some(confirmed_tier)
}

pub(super) fn row_confirmed_tier_with_batch_settlement<H: Storage + ?Sized>(
    storage: &H,
    row: &StoredRowBatch,
) -> Result<Option<DurabilityTier>, StorageError> {
    Ok(storage
        .load_authoritative_batch_settlement(row.batch_id)?
        .as_ref()
        .and_then(|settlement| batch_settlement_confirmed_tier_for_row(settlement, row)))
}

pub(super) fn apply_batch_settlement_tiers_to_rows<H: Storage + ?Sized>(
    storage: &H,
    rows: &mut [StoredRowBatch],
) -> Result<(), StorageError> {
    let mut settlement_cache = HashMap::<BatchId, Option<BatchSettlement>>::new();
    for row in rows {
        let settlement = if let Some(settlement) = settlement_cache.get(&row.batch_id) {
            settlement
        } else {
            let settlement = storage.load_authoritative_batch_settlement(row.batch_id)?;
            settlement_cache.insert(row.batch_id, settlement);
            settlement_cache
                .get(&row.batch_id)
                .expect("settlement cache should contain inserted batch")
        };

        row.confirmed_tier = settlement
            .as_ref()
            .and_then(|settlement| batch_settlement_confirmed_tier_for_row(settlement, row));
    }
    Ok(())
}

/// Errors from storage operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageError {
    NotFound,
    IoError(String),
    IndexKeyTooLarge {
        table: String,
        column: String,
        branch: String,
        key_bytes: usize,
        max_key_bytes: usize,
    },
    SecurityError(String),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::NotFound => write!(f, "not found"),
            StorageError::IoError(message) => write!(f, "{message}"),
            StorageError::IndexKeyTooLarge {
                table,
                column,
                branch,
                key_bytes,
                max_key_bytes,
            } => write!(
                f,
                "indexed value too large for {table}.{column} on branch {branch}: index key would be {key_bytes} bytes (max {max_key_bytes})"
            ),
            StorageError::SecurityError(message) => write!(f, "security error: {message}"),
        }
    }
}

impl std::error::Error for StorageError {}

pub(crate) fn validate_index_value_size(
    table: &str,
    column: &str,
    branch: &str,
    value: &Value,
) -> Result<(), StorageError> {
    key_codec::validate_index_entry_size(table, column, branch, value)
}

pub type RowLocatorRows = Vec<(ObjectId, RowLocator)>;
pub type RawTableRows = Vec<(String, Vec<u8>)>;
pub type RawTableKeys = Vec<String>;

const ROW_LOCATOR_TABLE: &str = "__row_locator";
const VISIBLE_ROW_TABLE_LOCATOR_TABLE: &str = "__visible_row_table_locator";
const HISTORY_ROW_BATCH_TABLE_LOCATOR_TABLE: &str = "__history_row_batch_table_locator";
const LOCAL_BATCH_RECORD_TABLE: &str = "__local_batch_record";
const AUTHORITATIVE_BATCH_SETTLEMENT_TABLE: &str = "__authoritative_batch_settlement";
const SEALED_BATCH_SUBMISSION_TABLE: &str = "__sealed_batch_submission";
const RAW_TABLE_HEADER_TABLE: &str = "__raw_table_header";
const BRANCH_ORD_BY_NAME_TABLE: &str = "__branch_ord_by_name";
const BRANCH_NAME_BY_ORD_TABLE: &str = "__branch_name_by_ord";
const BRANCH_ORD_META_TABLE: &str = "__branch_ord_meta";
const BRANCH_ORD_NEXT_ORD_KEY: &str = "next_ord";
pub(crate) const STORE_MANIFEST_KEY: &str = "__jazz_store_manifest";
const STORE_MANIFEST_MAGIC: &[u8; 10] = b"JAZZSTORE1";
const STORE_FORMAT_V3: i32 = 3;
const ROW_STORAGE_FORMAT_V3: i32 = 3;
const ROW_LOCATOR_STORAGE_FORMAT_V1: i32 = 1;
const EXACT_ROW_TABLE_LOCATOR_STORAGE_FORMAT_V1: i32 = 1;
const CATALOGUE_STORAGE_FORMAT_V1: i32 = 1;
const BRANCH_ORD_BY_NAME_FORMAT_V1: i32 = 1;
const BRANCH_NAME_BY_ORD_FORMAT_V1: i32 = 1;
const BRANCH_ORD_META_FORMAT_V1: i32 = 1;
const SEALED_BATCH_SUBMISSION_FORMAT_V2: i32 = 2;
const AUTHORITATIVE_BATCH_SETTLEMENT_FORMAT_V2: i32 = 2;
const LOCAL_BATCH_RECORD_FORMAT_V3: i32 = 3;

pub type BranchOrd = i32;

const STORAGE_KIND_ROW_LOCATOR: &str = "row_locator";
const STORAGE_KIND_VISIBLE_ROW_TABLE_LOCATOR: &str = "visible_row_table_locator";
const STORAGE_KIND_HISTORY_ROW_BATCH_TABLE_LOCATOR: &str = "history_row_batch_table_locator";
const STORAGE_KIND_BRANCH_ORD_BY_NAME: &str = "branch_ord_by_name";
const STORAGE_KIND_BRANCH_NAME_BY_ORD: &str = "branch_name_by_ord";
const STORAGE_KIND_BRANCH_ORD_META: &str = "branch_ord_meta";
const STORAGE_KIND_LOCAL_BATCH_RECORD: &str = "local_batch_record";
const STORAGE_KIND_AUTHORITATIVE_BATCH_SETTLEMENT: &str = "authoritative_batch_settlement";
const STORAGE_KIND_SEALED_BATCH_SUBMISSION: &str = "sealed_batch_submission";
const STORAGE_KIND_CATALOGUE: &str = "catalogue";
#[cfg(feature = "sqlite")]
pub(crate) const SQLITE_STORE_KIND: &str = "sqlite";
#[cfg(feature = "rocksdb")]
pub(crate) const ROCKSDB_STORE_KIND: &str = "rocksdb";
pub(crate) const OPFS_BTREE_STORE_KIND: &str = "opfs_btree";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoreManifest {
    pub store_kind: String,
    pub store_format_version: i32,
}

pub(crate) fn expected_store_manifest(store_kind: &str) -> StoreManifest {
    StoreManifest {
        store_kind: store_kind.to_string(),
        store_format_version: STORE_FORMAT_V3,
    }
}

pub(crate) fn encode_store_manifest(manifest: &StoreManifest) -> Result<Vec<u8>, StorageError> {
    let kind_bytes = manifest.store_kind.as_bytes();
    if kind_bytes.len() > u8::MAX as usize {
        return Err(StorageError::IoError(format!(
            "store manifest kind too long: {} bytes",
            kind_bytes.len()
        )));
    }
    let mut bytes = Vec::with_capacity(
        STORE_MANIFEST_MAGIC.len() + std::mem::size_of::<i32>() + 1 + kind_bytes.len(),
    );
    bytes.extend_from_slice(STORE_MANIFEST_MAGIC);
    bytes.extend_from_slice(&manifest.store_format_version.to_le_bytes());
    bytes.push(kind_bytes.len() as u8);
    bytes.extend_from_slice(kind_bytes);
    Ok(bytes)
}

pub(crate) fn decode_store_manifest(bytes: &[u8]) -> Result<StoreManifest, StorageError> {
    let min_len = STORE_MANIFEST_MAGIC.len() + std::mem::size_of::<i32>() + 1;
    if bytes.len() < min_len {
        return Err(StorageError::IoError(
            "store manifest too short".to_string(),
        ));
    }
    if &bytes[..STORE_MANIFEST_MAGIC.len()] != STORE_MANIFEST_MAGIC {
        return Err(StorageError::IoError(
            "store manifest magic mismatch".to_string(),
        ));
    }
    let mut version_bytes = [0u8; 4];
    version_bytes
        .copy_from_slice(&bytes[STORE_MANIFEST_MAGIC.len()..STORE_MANIFEST_MAGIC.len() + 4]);
    let store_format_version = i32::from_le_bytes(version_bytes);
    let kind_len = bytes[STORE_MANIFEST_MAGIC.len() + 4] as usize;
    let kind_start = STORE_MANIFEST_MAGIC.len() + 5;
    let kind_end = kind_start + kind_len;
    if bytes.len() != kind_end {
        return Err(StorageError::IoError(
            "store manifest trailing bytes mismatch".to_string(),
        ));
    }
    let store_kind = String::from_utf8(bytes[kind_start..kind_end].to_vec())
        .map_err(|err| StorageError::IoError(format!("invalid store manifest kind utf8: {err}")))?;
    Ok(StoreManifest {
        store_kind,
        store_format_version,
    })
}

pub(crate) fn validate_store_manifest(
    actual: &StoreManifest,
    expected: &StoreManifest,
) -> Result<(), StorageError> {
    if actual.store_kind != expected.store_kind {
        return Err(StorageError::IoError(format!(
            "store manifest kind mismatch: expected {}, got {}",
            expected.store_kind, actual.store_kind
        )));
    }
    if actual.store_format_version != expected.store_format_version {
        return Err(StorageError::IoError(format!(
            "store manifest version mismatch for {}: expected {}, got {}",
            expected.store_kind, expected.store_format_version, actual.store_format_version
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawTableHeader {
    pub storage_kind: SharedString,
    pub storage_format_version: i32,
    pub logical_table_name: Option<SharedString>,
    pub schema_hash: Option<SchemaHash>,
    pub row_descriptor_bytes: Option<Vec<u8>>,
}

impl RawTableHeader {
    pub fn new(storage_kind: impl Into<String>, storage_format_version: i32) -> Self {
        Self {
            storage_kind: storage_kind.into().into(),
            storage_format_version,
            logical_table_name: None,
            schema_hash: None,
            row_descriptor_bytes: None,
        }
    }

    pub fn row_raw_table(
        kind: RowRawTableKind,
        table_name: impl Into<String>,
        schema_hash: SchemaHash,
        user_descriptor: &RowDescriptor,
    ) -> Self {
        let table_name: SharedString = table_name.into().into();
        Self {
            storage_kind: kind.storage_kind().into(),
            storage_format_version: ROW_STORAGE_FORMAT_V3,
            logical_table_name: Some(table_name),
            schema_hash: Some(schema_hash),
            row_descriptor_bytes: Some(
                crate::schema_manager::encoding::encode_row_descriptor_bytes(user_descriptor),
            ),
        }
    }

    pub fn system(storage_kind: impl Into<String>, storage_format_version: i32) -> Self {
        Self::new(storage_kind, storage_format_version)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowLocator {
    pub table: SharedString,
    pub origin_schema_hash: Option<SchemaHash>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExactRowTableLocator {
    pub row_raw_table: SharedString,
    pub table_name: SharedString,
    pub schema_hash: SchemaHash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RowRawTableKind {
    Visible,
    History,
}

impl RowRawTableKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Visible => "visible",
            Self::History => "history",
        }
    }

    fn storage_kind(self) -> &'static str {
        match self {
            Self::Visible => "visible_rows",
            Self::History => "row_history",
        }
    }

    fn from_str(raw: &str) -> Result<Self, StorageError> {
        match raw {
            "visible" => Ok(Self::Visible),
            "history" => Ok(Self::History),
            other => Err(StorageError::IoError(format!(
                "unknown row raw table kind '{other}'"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RowRawTableId {
    pub kind: RowRawTableKind,
    pub table_name: SharedString,
    pub schema_hash: SchemaHash,
    pub raw_table_name: SharedString,
}

impl RowRawTableId {
    pub fn new(
        kind: RowRawTableKind,
        table_name: impl Into<String>,
        schema_hash: SchemaHash,
    ) -> Self {
        let table_name: SharedString = table_name.into().into();
        Self {
            kind,
            raw_table_name: format!("rowtable:{}:{}:{}", kind.as_str(), table_name, schema_hash)
                .into(),
            table_name,
            schema_hash,
        }
    }

    pub fn raw_table_name(&self) -> &str {
        self.raw_table_name.as_str()
    }

    fn parse_raw_table_name(raw: &str) -> Result<Self, StorageError> {
        let Some(rest) = raw.strip_prefix("rowtable:") else {
            return Err(StorageError::IoError(format!(
                "invalid row raw table id '{raw}'"
            )));
        };
        let mut parts = rest.splitn(3, ':');
        let kind =
            RowRawTableKind::from_str(parts.next().ok_or_else(|| {
                StorageError::IoError(format!("invalid row raw table id '{raw}'"))
            })?)?;
        let table_name = parts
            .next()
            .ok_or_else(|| StorageError::IoError(format!("invalid row raw table id '{raw}'")))?;
        let schema_hash = parts
            .next()
            .and_then(SchemaHash::from_hex)
            .ok_or_else(|| StorageError::IoError(format!("invalid row raw table id '{raw}'")))?;
        Ok(Self {
            kind,
            table_name: table_name.into(),
            schema_hash,
            raw_table_name: raw.into(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexMutation<'a> {
    Insert {
        table: &'a str,
        column: &'a str,
        branch: &'a str,
        value: Value,
        row_id: ObjectId,
    },
    Remove {
        table: &'a str,
        column: &'a str,
        branch: &'a str,
        value: Value,
        row_id: ObjectId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawTableMutation<'a> {
    Put {
        table: &'a str,
        key: &'a str,
        value: &'a [u8],
    },
    Delete {
        table: &'a str,
        key: &'a str,
    },
}

pub struct HistoryRowBytes<'a> {
    pub row_raw_table: &'a str,
    pub branch: &'a str,
    pub row_id: ObjectId,
    pub batch_id: BatchId,
    pub bytes: &'a [u8],
}

#[doc(hidden)]
pub struct OwnedHistoryRowBytes {
    pub row_raw_table_id: RowRawTableId,
    pub row_raw_table: String,
    pub user_descriptor: Arc<RowDescriptor>,
    pub branch: String,
    pub row_id: ObjectId,
    pub batch_id: BatchId,
    pub needs_exact_locator: bool,
    pub bytes: Vec<u8>,
}

pub struct VisibleRowBytes<'a> {
    pub row_raw_table: &'a str,
    pub branch: &'a str,
    pub row_id: ObjectId,
    pub bytes: &'a [u8],
}

#[doc(hidden)]
pub struct OwnedVisibleRowBytes {
    pub row_raw_table_id: RowRawTableId,
    pub row_raw_table: String,
    pub user_descriptor: Arc<RowDescriptor>,
    pub branch: String,
    pub row_id: ObjectId,
    pub needs_exact_locator: bool,
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone)]
struct ResolvedRowTable {
    row_raw_table: String,
    user_descriptor: Arc<RowDescriptor>,
    row_codecs: Arc<FlatRowCodecs>,
}

#[derive(Clone)]
pub(crate) struct PreparedRowWriteContext {
    pub history_row_raw_table_id: RowRawTableId,
    pub visible_row_raw_table_id: RowRawTableId,
    pub user_descriptor: Arc<RowDescriptor>,
    pub needs_exact_locator: bool,
}

type RowRawTableIdCache = HashMap<(RowRawTableKind, String, SchemaHash), RowRawTableId>;
type CatalogueUserDescriptorCache = HashMap<(usize, String, SchemaHash), Arc<RowDescriptor>>;

fn row_raw_table_id_cache() -> &'static Mutex<RowRawTableIdCache> {
    static CACHE: OnceLock<Mutex<RowRawTableIdCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cached_row_raw_table_id(
    kind: RowRawTableKind,
    table: &str,
    schema_hash: SchemaHash,
) -> RowRawTableId {
    let cache_key = (kind, table.to_string(), schema_hash);
    if let Some(cached) = row_raw_table_id_cache()
        .lock()
        .expect("row raw table id cache poisoned")
        .get(&cache_key)
        .cloned()
    {
        return cached;
    }

    let created = RowRawTableId::new(kind, table, schema_hash);
    row_raw_table_id_cache()
        .lock()
        .expect("row raw table id cache poisoned")
        .insert(cache_key, created.clone());
    created
}

fn row_raw_table_descriptor_cache() -> &'static Mutex<HashMap<String, Arc<RowDescriptor>>> {
    static CACHE: OnceLock<Mutex<HashMap<String, Arc<RowDescriptor>>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

pub(crate) fn next_storage_cache_namespace() -> usize {
    static NEXT_STORAGE_CACHE_NAMESPACE: AtomicUsize = AtomicUsize::new(1);
    NEXT_STORAGE_CACHE_NAMESPACE.fetch_add(1, Ordering::Relaxed)
}

fn raw_table_header_cache() -> &'static Mutex<HashMap<(usize, String), RawTableHeader>> {
    static CACHE: OnceLock<Mutex<HashMap<(usize, String), RawTableHeader>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cached_raw_table_header_with_storage<H: Storage + ?Sized>(
    storage: &H,
    raw_table: &str,
) -> Option<RawTableHeader> {
    raw_table_header_cache()
        .lock()
        .expect("raw table header cache poisoned")
        .get(&(storage.storage_cache_namespace(), raw_table.to_string()))
        .cloned()
}

fn cache_raw_table_header_with_storage<H: Storage + ?Sized>(
    storage: &H,
    raw_table: &str,
    header: RawTableHeader,
) {
    raw_table_header_cache()
        .lock()
        .expect("raw table header cache poisoned")
        .insert(
            (storage.storage_cache_namespace(), raw_table.to_string()),
            header,
        );
}

fn validated_raw_table_cache() -> &'static Mutex<HashSet<(usize, String)>> {
    static CACHE: OnceLock<Mutex<HashSet<(usize, String)>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashSet::new()))
}

fn raw_table_validated_with_storage<H: Storage + ?Sized>(storage: &H, raw_table: &str) -> bool {
    validated_raw_table_cache()
        .lock()
        .expect("validated raw table cache poisoned")
        .contains(&(storage.storage_cache_namespace(), raw_table.to_string()))
}

fn cache_validated_raw_table_with_storage<H: Storage + ?Sized>(storage: &H, raw_table: &str) {
    validated_raw_table_cache()
        .lock()
        .expect("validated raw table cache poisoned")
        .insert((storage.storage_cache_namespace(), raw_table.to_string()));
}

fn invalidate_validated_raw_table_with_storage<H: Storage + ?Sized>(storage: &H, raw_table: &str) {
    validated_raw_table_cache()
        .lock()
        .expect("validated raw table cache poisoned")
        .remove(&(storage.storage_cache_namespace(), raw_table.to_string()));
}

fn cached_row_descriptor(raw_table: &str) -> Option<Arc<RowDescriptor>> {
    row_raw_table_descriptor_cache()
        .lock()
        .expect("row raw table descriptor cache poisoned")
        .get(raw_table)
        .cloned()
}

fn cache_row_descriptor(raw_table: &str, descriptor: Arc<RowDescriptor>) {
    row_raw_table_descriptor_cache()
        .lock()
        .expect("row raw table descriptor cache poisoned")
        .insert(raw_table.to_string(), descriptor);
}

fn catalogue_user_descriptor_cache() -> &'static Mutex<CatalogueUserDescriptorCache> {
    static CACHE: OnceLock<Mutex<CatalogueUserDescriptorCache>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cached_catalogue_user_descriptor_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table_name: &str,
    schema_hash: SchemaHash,
) -> Option<Arc<RowDescriptor>> {
    catalogue_user_descriptor_cache()
        .lock()
        .expect("catalogue user descriptor cache poisoned")
        .get(&(
            storage.storage_cache_namespace(),
            table_name.to_string(),
            schema_hash,
        ))
        .cloned()
}

fn cache_catalogue_user_descriptor_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table_name: &str,
    schema_hash: SchemaHash,
    descriptor: Arc<RowDescriptor>,
) {
    catalogue_user_descriptor_cache()
        .lock()
        .expect("catalogue user descriptor cache poisoned")
        .insert(
            (
                storage.storage_cache_namespace(),
                table_name.to_string(),
                schema_hash,
            ),
            descriptor,
        );
}

fn metadata_raw_key(id: ObjectId) -> String {
    hex::encode(id.uuid().as_bytes())
}

fn decode_metadata_raw_key(key: &str) -> Result<ObjectId, StorageError> {
    let bytes = hex::decode(key)
        .map_err(|err| StorageError::IoError(format!("invalid metadata key '{key}': {err}")))?;
    let uuid = uuid::Uuid::from_slice(&bytes)
        .map_err(|err| StorageError::IoError(format!("invalid metadata uuid '{key}': {err}")))?;
    Ok(ObjectId::from_uuid(uuid))
}

pub(crate) fn row_locator_from_metadata(metadata: &HashMap<String, String>) -> Option<RowLocator> {
    Some(RowLocator {
        table: metadata.get(MetadataKey::Table.as_str())?.clone().into(),
        origin_schema_hash: metadata
            .get(MetadataKey::OriginSchemaHash.as_str())
            .and_then(|raw_hash| SchemaHash::from_hex(raw_hash)),
    })
}

pub(crate) fn metadata_from_row_locator(locator: &RowLocator) -> HashMap<String, String> {
    let mut metadata = HashMap::from([(MetadataKey::Table.to_string(), locator.table.to_string())]);
    if let Some(origin_schema_hash) = locator.origin_schema_hash {
        metadata.insert(
            MetadataKey::OriginSchemaHash.to_string(),
            origin_schema_hash.to_string(),
        );
    }
    metadata
}

fn encode_row_locator(locator: &RowLocator) -> Result<Vec<u8>, StorageError> {
    postcard::to_allocvec(locator)
        .map_err(|err| StorageError::IoError(format!("serialize row locator: {err}")))
}

fn decode_row_locator(bytes: &[u8]) -> Result<RowLocator, StorageError> {
    postcard::from_bytes(bytes)
        .map_err(|err| StorageError::IoError(format!("deserialize row locator: {err}")))
}

fn exact_row_table_locator_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("row_raw_table", ColumnType::Text),
        ColumnDescriptor::new("table_name", ColumnType::Text),
        ColumnDescriptor::new("schema_hash", ColumnType::Bytea),
    ])
}

fn encode_exact_row_table_locator(locator: &ExactRowTableLocator) -> Result<Vec<u8>, StorageError> {
    encode_row(
        &exact_row_table_locator_storage_descriptor(),
        &[
            Value::Text(locator.row_raw_table.to_string()),
            Value::Text(locator.table_name.to_string()),
            Value::Bytea(locator.schema_hash.as_bytes().to_vec()),
        ],
    )
    .map_err(|err| StorageError::IoError(format!("encode exact row table locator: {err}")))
}

fn decode_exact_row_table_locator(bytes: &[u8]) -> Result<ExactRowTableLocator, StorageError> {
    let values = decode_row(&exact_row_table_locator_storage_descriptor(), bytes)
        .map_err(|err| StorageError::IoError(format!("decode exact row table locator: {err}")))?;
    let [row_raw_table, table_name, schema_hash] = values.as_slice() else {
        return Err(StorageError::IoError(
            "malformed exact row table locator".to_string(),
        ));
    };
    let row_raw_table = match row_raw_table {
        Value::Text(raw) => SharedString::from(raw.clone()),
        other => {
            return Err(StorageError::IoError(format!(
                "exact row table locator row_raw_table was {other:?}"
            )));
        }
    };
    let table_name = match table_name {
        Value::Text(raw) => SharedString::from(raw.clone()),
        other => {
            return Err(StorageError::IoError(format!(
                "exact row table locator table_name was {other:?}"
            )));
        }
    };
    let schema_hash = match schema_hash {
        Value::Bytea(bytes) => SchemaHash::from_bytes(bytes.clone().try_into().map_err(|_| {
            StorageError::IoError(
                "exact row table locator schema_hash must be 32 bytes".to_string(),
            )
        })?),
        other => {
            return Err(StorageError::IoError(format!(
                "exact row table locator schema_hash was {other:?}"
            )));
        }
    };
    Ok(ExactRowTableLocator {
        row_raw_table,
        table_name,
        schema_hash,
    })
}

fn visible_row_table_locator_key(branch: &str, row_id: ObjectId) -> String {
    key_codec::visible_row_raw_table_key(branch, row_id)
}

fn history_row_batch_table_locator_key(
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
) -> String {
    key_codec::history_row_raw_table_key(row_id, branch, batch_id)
}

fn local_batch_record_key(batch_id: BatchId) -> String {
    format!("batch:{}", hex::encode(batch_id.as_bytes()))
}

fn decode_local_batch_record_key(key: &str) -> Result<BatchId, StorageError> {
    let Some(hex_id) = key.strip_prefix("batch:") else {
        return Err(StorageError::IoError(format!(
            "invalid local batch record key '{key}'"
        )));
    };
    let bytes = hex::decode(hex_id).map_err(|err| {
        StorageError::IoError(format!("invalid local batch record key '{key}': {err}"))
    })?;
    let bytes: [u8; 16] = bytes.try_into().map_err(|_| {
        StorageError::IoError(format!(
            "invalid local batch record batch id '{key}': expected 16 bytes, got {}",
            hex_id.len() / 2
        ))
    })?;
    Ok(BatchId(bytes))
}

fn branch_ord_by_name_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![ColumnDescriptor::new(
        "branch_ord",
        ColumnType::Integer,
    )])
}

fn branch_name_by_ord_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![ColumnDescriptor::new("branch_name", ColumnType::Text)])
}

fn branch_ord_meta_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![ColumnDescriptor::new("next_ord", ColumnType::Integer)])
}

fn branch_ord_by_name_key(branch_name: BranchName) -> String {
    branch_name.as_str().to_string()
}

fn branch_name_by_ord_key(branch_ord: BranchOrd) -> Result<String, StorageError> {
    if branch_ord < 1 {
        return Err(StorageError::IoError(format!(
            "branch ord must be >= 1, got {branch_ord}"
        )));
    }
    Ok(format!("{branch_ord:010}"))
}

fn encode_branch_ord_value(branch_ord: BranchOrd) -> Result<Vec<u8>, StorageError> {
    if branch_ord < 1 {
        return Err(StorageError::IoError(format!(
            "branch ord must be >= 1, got {branch_ord}"
        )));
    }
    encode_row(
        &branch_ord_by_name_storage_descriptor(),
        &[Value::Integer(branch_ord)],
    )
    .map_err(|err| StorageError::IoError(format!("encode branch ord value: {err}")))
}

fn decode_branch_ord_value(bytes: &[u8]) -> Result<BranchOrd, StorageError> {
    let values = decode_row(&branch_ord_by_name_storage_descriptor(), bytes)
        .map_err(|err| StorageError::IoError(format!("decode branch ord value: {err}")))?;
    let [branch_ord] = values.as_slice() else {
        return Err(StorageError::IoError(
            "unexpected branch ord row shape".to_string(),
        ));
    };
    match branch_ord {
        Value::Integer(branch_ord) if *branch_ord >= 1 => Ok(*branch_ord),
        Value::Integer(branch_ord) => Err(StorageError::IoError(format!(
            "branch ord must be >= 1, got {branch_ord}"
        ))),
        other => Err(StorageError::IoError(format!(
            "branch ord row must contain Integer, got {other:?}"
        ))),
    }
}

fn encode_branch_name_value(branch_name: BranchName) -> Result<Vec<u8>, StorageError> {
    encode_row(
        &branch_name_by_ord_storage_descriptor(),
        &[Value::Text(branch_name.as_str().to_string())],
    )
    .map_err(|err| StorageError::IoError(format!("encode branch name value: {err}")))
}

fn decode_branch_name_value(bytes: &[u8]) -> Result<BranchName, StorageError> {
    let values = decode_row(&branch_name_by_ord_storage_descriptor(), bytes)
        .map_err(|err| StorageError::IoError(format!("decode branch name value: {err}")))?;
    let [branch_name] = values.as_slice() else {
        return Err(StorageError::IoError(
            "unexpected branch name row shape".to_string(),
        ));
    };
    match branch_name {
        Value::Text(branch_name) => Ok(BranchName::new(branch_name.clone())),
        other => Err(StorageError::IoError(format!(
            "branch name row must contain Text, got {other:?}"
        ))),
    }
}

fn encode_branch_ord_meta(next_ord: BranchOrd) -> Result<Vec<u8>, StorageError> {
    if next_ord < 1 {
        return Err(StorageError::IoError(format!(
            "next branch ord must be >= 1, got {next_ord}"
        )));
    }
    encode_row(
        &branch_ord_meta_storage_descriptor(),
        &[Value::Integer(next_ord)],
    )
    .map_err(|err| StorageError::IoError(format!("encode branch ord meta: {err}")))
}

fn decode_branch_ord_meta(bytes: &[u8]) -> Result<BranchOrd, StorageError> {
    let values = decode_row(&branch_ord_meta_storage_descriptor(), bytes)
        .map_err(|err| StorageError::IoError(format!("decode branch ord meta: {err}")))?;
    let [next_ord] = values.as_slice() else {
        return Err(StorageError::IoError(
            "unexpected branch ord meta row shape".to_string(),
        ));
    };
    match next_ord {
        Value::Integer(next_ord) if *next_ord >= 1 => Ok(*next_ord),
        Value::Integer(next_ord) => Err(StorageError::IoError(format!(
            "next branch ord must be >= 1, got {next_ord}"
        ))),
        other => Err(StorageError::IoError(format!(
            "branch ord meta row must contain Integer, got {other:?}"
        ))),
    }
}

fn load_next_branch_ord<H: Storage + ?Sized>(storage: &H) -> Result<BranchOrd, StorageError> {
    match storage.raw_table_get(BRANCH_ORD_META_TABLE, BRANCH_ORD_NEXT_ORD_KEY)? {
        Some(bytes) => {
            ensure_system_raw_table_header_validated_once(
                storage,
                BRANCH_ORD_META_TABLE,
                STORAGE_KIND_BRANCH_ORD_META,
                BRANCH_ORD_META_FORMAT_V1,
            )?;
            decode_branch_ord_meta(&bytes)
        }
        None => Ok(1),
    }
}

fn encode_raw_table_header(header: &RawTableHeader) -> Result<Vec<u8>, StorageError> {
    encode_row(
        &raw_table_header_storage_descriptor(),
        &[
            Value::Text(header.storage_kind.to_string()),
            Value::Integer(header.storage_format_version),
            header
                .logical_table_name
                .as_ref()
                .map(|name| Value::Text(name.to_string()))
                .unwrap_or(Value::Null),
            header
                .schema_hash
                .map(|schema_hash| Value::Bytea(schema_hash.as_bytes().to_vec()))
                .unwrap_or(Value::Null),
            header
                .row_descriptor_bytes
                .as_ref()
                .map(|bytes| Value::Bytea(bytes.clone()))
                .unwrap_or(Value::Null),
        ],
    )
    .map_err(|err| StorageError::IoError(format!("encode raw table header: {err}")))
}

fn decode_raw_table_header(bytes: &[u8]) -> Result<RawTableHeader, StorageError> {
    let values = decode_row(&raw_table_header_storage_descriptor(), bytes)
        .map_err(|err| StorageError::IoError(format!("decode raw table header: {err}")))?;
    let [
        storage_kind,
        storage_format_version,
        logical_table_name,
        schema_hash,
        row_descriptor_bytes,
    ] = values.as_slice()
    else {
        return Err(StorageError::IoError(
            "unexpected raw table header shape".to_string(),
        ));
    };

    let storage_kind = match storage_kind {
        Value::Text(value) => SharedString::from(value.clone()),
        other => {
            return Err(StorageError::IoError(format!(
                "raw table header storage_kind was {other:?}"
            )));
        }
    };
    let storage_format_version = match storage_format_version {
        Value::Integer(value) => *value,
        other => {
            return Err(StorageError::IoError(format!(
                "raw table header storage_format_version was {other:?}"
            )));
        }
    };
    let logical_table_name = match logical_table_name {
        Value::Null => None,
        Value::Text(value) => Some(SharedString::from(value.clone())),
        other => {
            return Err(StorageError::IoError(format!(
                "raw table header logical_table_name was {other:?}"
            )));
        }
    };
    let schema_hash = match schema_hash {
        Value::Null => None,
        Value::Bytea(bytes) => Some(SchemaHash::from_bytes(bytes.clone().try_into().map_err(
            |_| StorageError::IoError("raw table header schema_hash must be 32 bytes".to_string()),
        )?)),
        other => {
            return Err(StorageError::IoError(format!(
                "raw table header schema_hash was {other:?}"
            )));
        }
    };
    let row_descriptor_bytes = match row_descriptor_bytes {
        Value::Null => None,
        Value::Bytea(bytes) => Some(bytes.clone()),
        other => {
            return Err(StorageError::IoError(format!(
                "raw table header row_descriptor_bytes was {other:?}"
            )));
        }
    };

    Ok(RawTableHeader {
        storage_kind,
        storage_format_version,
        logical_table_name,
        schema_hash,
        row_descriptor_bytes,
    })
}

fn raw_table_header_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("storage_kind", ColumnType::Text),
        ColumnDescriptor::new("storage_format_version", ColumnType::Integer),
        ColumnDescriptor::new("logical_table_name", ColumnType::Text).nullable(),
        ColumnDescriptor::new("schema_hash", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("row_descriptor_bytes", ColumnType::Bytea).nullable(),
    ])
}

fn supported_storage_format_version(storage_kind: &str) -> Result<i32, StorageError> {
    match storage_kind {
        STORAGE_KIND_ROW_LOCATOR => Ok(ROW_LOCATOR_STORAGE_FORMAT_V1),
        STORAGE_KIND_VISIBLE_ROW_TABLE_LOCATOR => Ok(EXACT_ROW_TABLE_LOCATOR_STORAGE_FORMAT_V1),
        STORAGE_KIND_HISTORY_ROW_BATCH_TABLE_LOCATOR => {
            Ok(EXACT_ROW_TABLE_LOCATOR_STORAGE_FORMAT_V1)
        }
        STORAGE_KIND_BRANCH_ORD_BY_NAME => Ok(BRANCH_ORD_BY_NAME_FORMAT_V1),
        STORAGE_KIND_BRANCH_NAME_BY_ORD => Ok(BRANCH_NAME_BY_ORD_FORMAT_V1),
        STORAGE_KIND_BRANCH_ORD_META => Ok(BRANCH_ORD_META_FORMAT_V1),
        STORAGE_KIND_LOCAL_BATCH_RECORD => Ok(LOCAL_BATCH_RECORD_FORMAT_V3),
        STORAGE_KIND_SEALED_BATCH_SUBMISSION => Ok(SEALED_BATCH_SUBMISSION_FORMAT_V2),
        STORAGE_KIND_AUTHORITATIVE_BATCH_SETTLEMENT => Ok(AUTHORITATIVE_BATCH_SETTLEMENT_FORMAT_V2),
        STORAGE_KIND_CATALOGUE => Ok(CATALOGUE_STORAGE_FORMAT_V1),
        "visible_rows" | "row_history" => Ok(ROW_STORAGE_FORMAT_V3),
        other => Err(StorageError::IoError(format!(
            "unknown raw table header storage_kind '{other}'"
        ))),
    }
}

fn validate_raw_table_header_storage_format(
    raw_table: &str,
    header: &RawTableHeader,
) -> Result<(), StorageError> {
    let expected_version = supported_storage_format_version(header.storage_kind.as_str())?;
    if header.storage_format_version != expected_version {
        return Err(StorageError::IoError(format!(
            "raw table header storage_format_version mismatch for {raw_table}: expected {expected_version}, got {}",
            header.storage_format_version
        )));
    }
    Ok(())
}

fn validate_system_raw_table_header(
    raw_table: &str,
    header: RawTableHeader,
    expected_storage_kind: &str,
    expected_storage_format_version: i32,
) -> Result<RawTableHeader, StorageError> {
    validate_raw_table_header_storage_format(raw_table, &header)?;
    if header.storage_kind.as_str() != expected_storage_kind {
        return Err(StorageError::IoError(format!(
            "raw table header storage_kind mismatch for {raw_table}: expected {expected_storage_kind}, got {}",
            header.storage_kind
        )));
    }
    if header.storage_format_version != expected_storage_format_version {
        return Err(StorageError::IoError(format!(
            "raw table header storage_format_version mismatch for {raw_table}: expected {expected_storage_format_version}, got {}",
            header.storage_format_version
        )));
    }
    Ok(header)
}

fn ensure_system_raw_table_header_validated_once<H: Storage + ?Sized>(
    storage: &H,
    raw_table: &str,
    expected_storage_kind: &str,
    expected_storage_format_version: i32,
) -> Result<(), StorageError> {
    if raw_table_validated_with_storage(storage, raw_table) {
        return Ok(());
    }
    let header = storage.load_raw_table_header(raw_table)?.ok_or_else(|| {
        StorageError::IoError(format!("missing raw table header for {raw_table}"))
    })?;
    validate_system_raw_table_header(
        raw_table,
        header,
        expected_storage_kind,
        expected_storage_format_version,
    )?;
    cache_validated_raw_table_with_storage(storage, raw_table);
    Ok(())
}

fn ensure_raw_table_header<H: Storage + ?Sized>(
    storage: &mut H,
    raw_table: &str,
    expected_header: &RawTableHeader,
) -> Result<(), StorageError> {
    match storage.load_raw_table_header(raw_table)? {
        Some(existing) => {
            if existing == *expected_header {
                return Ok(());
            }
            if existing.storage_kind == expected_header.storage_kind
                && existing.storage_format_version == expected_header.storage_format_version
                && existing.logical_table_name == expected_header.logical_table_name
                && existing.schema_hash == expected_header.schema_hash
            {
                return storage.upsert_raw_table_header(raw_table, expected_header);
            }
            Err(StorageError::IoError(format!(
                "raw table header mismatch for {raw_table}"
            )))
        }
        None => {
            if !storage
                .raw_table_scan_prefix_keys(raw_table, "")?
                .is_empty()
            {
                return Err(StorageError::IoError(format!(
                    "missing raw table header for non-empty table {raw_table}"
                )));
            }
            storage.upsert_raw_table_header(raw_table, expected_header)
        }
    }
}

fn history_row_raw_table_id(table: &str, schema_hash: SchemaHash) -> RowRawTableId {
    cached_row_raw_table_id(RowRawTableKind::History, table, schema_hash)
}

fn visible_row_raw_table_id(table: &str, schema_hash: SchemaHash) -> RowRawTableId {
    cached_row_raw_table_id(RowRawTableKind::Visible, table, schema_hash)
}

fn load_user_descriptor_for_schema_hash<H: Storage + ?Sized>(
    storage: &H,
    table_name: &str,
    schema_hash: SchemaHash,
) -> Result<Arc<RowDescriptor>, StorageError> {
    load_history_user_descriptor_for_schema_hash(storage, table_name, schema_hash)?.ok_or_else(
        || {
            StorageError::IoError(format!(
                "missing catalogue descriptor for table {table_name} at schema {schema_hash}"
            ))
        },
    )
}

fn prepared_row_write_context_for_descriptor(
    table_name: &str,
    schema_hash: SchemaHash,
    user_descriptor: Arc<RowDescriptor>,
    needs_exact_locator: bool,
) -> Result<PreparedRowWriteContext, StorageError> {
    Ok(PreparedRowWriteContext {
        history_row_raw_table_id: history_row_raw_table_id(table_name, schema_hash),
        visible_row_raw_table_id: visible_row_raw_table_id(table_name, schema_hash),
        user_descriptor,
        needs_exact_locator,
    })
}

pub(crate) fn prepared_row_write_context_for_known_exact_locator(
    table_name: &str,
    schema_hash: SchemaHash,
    user_descriptor: Arc<RowDescriptor>,
) -> Result<PreparedRowWriteContext, StorageError> {
    prepared_row_write_context_for_descriptor(table_name, schema_hash, user_descriptor, false)
}

pub(crate) fn prepared_row_write_context_for_schema_hash_and_descriptor<H: Storage + ?Sized>(
    storage: &H,
    table_name: &str,
    schema_hash: SchemaHash,
    row_id: ObjectId,
    user_descriptor: Arc<RowDescriptor>,
) -> Result<PreparedRowWriteContext, StorageError> {
    let needs_exact_locator = storage
        .load_row_locator(row_id)?
        .and_then(|locator| locator.origin_schema_hash)
        != Some(schema_hash);
    prepared_row_write_context_for_descriptor(
        table_name,
        schema_hash,
        user_descriptor,
        needs_exact_locator,
    )
}

fn prepared_row_write_context_for_schema_hash<H: Storage + ?Sized>(
    storage: &H,
    table_name: &str,
    schema_hash: SchemaHash,
    row_id: ObjectId,
) -> Result<PreparedRowWriteContext, StorageError> {
    let user_descriptor = load_user_descriptor_for_schema_hash(storage, table_name, schema_hash)?;
    prepared_row_write_context_for_schema_hash_and_descriptor(
        storage,
        table_name,
        schema_hash,
        row_id,
        user_descriptor,
    )
}

fn load_user_descriptor_from_raw_table_header(
    header: &RawTableHeader,
) -> Result<Option<RowDescriptor>, StorageError> {
    let Some(raw) = header.row_descriptor_bytes.as_ref() else {
        return Ok(None);
    };
    crate::schema_manager::encoding::decode_row_descriptor_bytes(raw)
        .map(Some)
        .map_err(|err| {
            StorageError::IoError(format!(
                "decode row descriptor from raw table header: {err}"
            ))
        })
}

fn resolved_user_descriptor_for_raw_table<H: Storage + ?Sized>(
    storage: &H,
    raw_table_name: &str,
    table_name: &str,
    schema_hash: SchemaHash,
    header: &RawTableHeader,
) -> Result<Arc<RowDescriptor>, StorageError> {
    if let Some(descriptor) = cached_row_descriptor(raw_table_name) {
        return Ok(descriptor);
    }

    let descriptor = match load_user_descriptor_from_raw_table_header(header)? {
        Some(descriptor) => Arc::new(descriptor),
        None => load_user_descriptor_for_schema_hash(storage, table_name, schema_hash)?,
    };
    cache_row_descriptor(raw_table_name, descriptor.clone());
    Ok(descriptor)
}

fn row_raw_table_header(id: &RowRawTableId, user_descriptor: &RowDescriptor) -> RawTableHeader {
    RawTableHeader::row_raw_table(
        id.kind,
        id.table_name.to_string(),
        id.schema_hash,
        user_descriptor,
    )
}

fn row_raw_table_header_prefix(kind: RowRawTableKind, table: &str) -> String {
    format!("rowtable:{}:{}:", kind.as_str(), table)
}

fn validate_row_raw_table_header(
    id: &RowRawTableId,
    header: RawTableHeader,
) -> Result<RawTableHeader, StorageError> {
    validate_row_raw_table_header_fields(
        id.kind,
        id.table_name.as_str(),
        id.schema_hash,
        id.raw_table_name(),
        header,
    )
}

fn validate_row_raw_table_header_fields(
    kind: RowRawTableKind,
    table_name: &str,
    schema_hash: SchemaHash,
    raw_table_name: &str,
    header: RawTableHeader,
) -> Result<RawTableHeader, StorageError> {
    validate_raw_table_header_storage_format(raw_table_name, &header)?;
    if header.storage_kind.as_str() != kind.storage_kind() {
        return Err(StorageError::IoError(format!(
            "raw table header storage_kind '{}' did not match expected row raw table kind '{}'",
            header.storage_kind,
            kind.storage_kind()
        )));
    }
    if header.logical_table_name.as_ref().map(|name| name.as_str()) != Some(table_name) {
        return Err(StorageError::IoError(format!(
            "row raw table header logical_table_name mismatch for {}",
            raw_table_name
        )));
    }
    if header.schema_hash != Some(schema_hash) {
        return Err(StorageError::IoError(format!(
            "row raw table header schema_hash mismatch for {}",
            raw_table_name
        )));
    }
    Ok(header)
}

fn ensure_row_raw_table_header_validated_once<H: Storage + ?Sized>(
    storage: &H,
    row_raw_table_id: &RowRawTableId,
) -> Result<(), StorageError> {
    if raw_table_validated_with_storage(storage, row_raw_table_id.raw_table_name()) {
        return Ok(());
    }
    let header = storage
        .load_raw_table_header(row_raw_table_id.raw_table_name())?
        .ok_or_else(|| {
            StorageError::IoError(format!(
                "missing raw table header for {}",
                row_raw_table_id.raw_table_name()
            ))
        })?;
    validate_row_raw_table_header(row_raw_table_id, header)?;
    cache_validated_raw_table_with_storage(storage, row_raw_table_id.raw_table_name());
    Ok(())
}

pub(crate) fn load_row_raw_table_header_with_storage<H: Storage + ?Sized>(
    storage: &H,
    id: &RowRawTableId,
) -> Result<Option<RawTableHeader>, StorageError> {
    let header = storage
        .load_raw_table_header(id.raw_table_name())?
        .map(|header| validate_row_raw_table_header(id, header))
        .transpose()?;
    if header.is_some() {
        cache_validated_raw_table_with_storage(storage, id.raw_table_name());
    }
    Ok(header)
}

pub(crate) fn scan_row_raw_table_headers_with_storage<H: Storage + ?Sized>(
    storage: &H,
) -> Result<Vec<(RowRawTableId, RawTableHeader)>, StorageError> {
    let mut rows = Vec::new();
    for (raw_table_name, header) in storage.scan_raw_table_headers()? {
        if !raw_table_name.starts_with("rowtable:") {
            continue;
        }
        let row_raw_table_id = RowRawTableId::parse_raw_table_name(&raw_table_name)?;
        cache_validated_raw_table_with_storage(storage, &raw_table_name);
        rows.push((
            row_raw_table_id.clone(),
            validate_row_raw_table_header(&row_raw_table_id, header)?,
        ));
    }
    rows.sort_by_key(|(row_raw_table_id, _)| row_raw_table_id.raw_table_name().to_string());
    Ok(rows)
}

fn row_raw_table_ids_for_table<H: Storage + ?Sized>(
    storage: &H,
    kind: RowRawTableKind,
    table: &str,
) -> Result<Vec<RowRawTableId>, StorageError> {
    let prefix = row_raw_table_header_prefix(kind, table);
    let mut ids = Vec::new();
    for (raw_table_name, bytes) in storage.raw_table_scan_prefix(RAW_TABLE_HEADER_TABLE, &prefix)? {
        let row_raw_table_id = RowRawTableId::parse_raw_table_name(&raw_table_name)?;
        let header = decode_raw_table_header(&bytes)?;
        let header = validate_row_raw_table_header(&row_raw_table_id, header)?;
        cache_raw_table_header_with_storage(storage, &raw_table_name, header);
        cache_validated_raw_table_with_storage(storage, &raw_table_name);
        ids.push(row_raw_table_id);
    }
    ids.sort_by_key(|row_raw_table_id| row_raw_table_id.raw_table_name().to_string());
    Ok(ids)
}

fn resolved_row_table_from_header<H: Storage + ?Sized>(
    storage: &H,
    row_raw_table_id: RowRawTableId,
    header: RawTableHeader,
) -> Result<ResolvedRowTable, StorageError> {
    let table_name = header
        .logical_table_name
        .as_ref()
        .expect("validated row raw table header must have logical_table_name")
        .as_str();
    let schema_hash = header
        .schema_hash
        .expect("validated row raw table header must have schema_hash");
    let user_descriptor = resolved_user_descriptor_for_raw_table(
        storage,
        row_raw_table_id.raw_table_name(),
        table_name,
        schema_hash,
        &header,
    )?;
    let row_codecs = flat_row_codecs(user_descriptor.as_ref());
    Ok(ResolvedRowTable {
        row_raw_table: row_raw_table_id.raw_table_name().to_string(),
        user_descriptor,
        row_codecs,
    })
}

fn resolved_row_table_from_id<H: Storage + ?Sized>(
    storage: &H,
    row_raw_table_id: RowRawTableId,
) -> Result<Option<ResolvedRowTable>, StorageError> {
    let Some(header) = load_row_raw_table_header_with_storage(storage, &row_raw_table_id)? else {
        return Ok(None);
    };
    resolved_row_table_from_header(storage, row_raw_table_id, header).map(Some)
}

fn resolved_row_table_from_locator<H: Storage + ?Sized>(
    storage: &H,
    locator: &ExactRowTableLocator,
) -> Result<Option<ResolvedRowTable>, StorageError> {
    let row_raw_table_id = RowRawTableId {
        kind: RowRawTableId::parse_raw_table_name(locator.row_raw_table.as_str())?.kind,
        table_name: locator.table_name.clone(),
        schema_hash: locator.schema_hash,
        raw_table_name: locator.row_raw_table.clone(),
    };
    ensure_row_raw_table_header_validated_once(storage, &row_raw_table_id)?;
    let user_descriptor =
        if let Some(descriptor) = cached_row_descriptor(locator.row_raw_table.as_str()) {
            descriptor
        } else {
            let descriptor = load_user_descriptor_for_schema_hash(
                storage,
                locator.table_name.as_str(),
                locator.schema_hash,
            )?;
            cache_row_descriptor(locator.row_raw_table.as_str(), descriptor.clone());
            descriptor
        };
    let row_codecs = flat_row_codecs(user_descriptor.as_ref());
    Ok(Some(ResolvedRowTable {
        row_raw_table: locator.row_raw_table.to_string(),
        user_descriptor,
        row_codecs,
    }))
}

fn resolved_row_tables_for_table<H: Storage + ?Sized>(
    storage: &H,
    kind: RowRawTableKind,
    table: &str,
) -> Result<Vec<ResolvedRowTable>, StorageError> {
    let prefix = row_raw_table_header_prefix(kind, table);
    let mut tables = Vec::new();
    for (raw_table_name, bytes) in storage.raw_table_scan_prefix(RAW_TABLE_HEADER_TABLE, &prefix)? {
        let row_raw_table_id = RowRawTableId::parse_raw_table_name(&raw_table_name)?;
        let schema_hash = row_raw_table_id.schema_hash;
        let header = decode_raw_table_header(&bytes)?;
        let header = validate_row_raw_table_header_fields(
            kind,
            table,
            schema_hash,
            &raw_table_name,
            header,
        )?;
        cache_raw_table_header_with_storage(storage, &raw_table_name, header.clone());
        cache_validated_raw_table_with_storage(storage, &raw_table_name);
        tables.push(resolved_row_table_from_header(
            storage,
            row_raw_table_id,
            header,
        )?);
    }
    tables.sort_by(|left, right| left.row_raw_table.cmp(&right.row_raw_table));
    Ok(tables)
}

fn common_case_exact_row_table_locator<H: Storage + ?Sized>(
    storage: &H,
    row_id: ObjectId,
) -> Result<Option<RowLocator>, StorageError> {
    let Some(locator) = storage.load_row_locator(row_id)? else {
        return Ok(None);
    };
    if locator.origin_schema_hash.is_none() {
        return Ok(None);
    }
    Ok(Some(locator))
}

fn common_case_exact_history_row_table_locator<H: Storage + ?Sized>(
    storage: &H,
    row_id: ObjectId,
) -> Result<Option<ExactRowTableLocator>, StorageError> {
    let Some(locator) = common_case_exact_row_table_locator(storage, row_id)? else {
        return Ok(None);
    };
    let Some(schema_hash) = locator.origin_schema_hash else {
        return Ok(None);
    };
    let row_raw_table_id = history_row_raw_table_id(locator.table.as_str(), schema_hash);
    Ok(Some(ExactRowTableLocator {
        row_raw_table: row_raw_table_id.raw_table_name.clone(),
        table_name: row_raw_table_id.table_name.clone(),
        schema_hash,
    }))
}

fn common_case_exact_visible_row_table_locator<H: Storage + ?Sized>(
    storage: &H,
    row_id: ObjectId,
) -> Result<Option<ExactRowTableLocator>, StorageError> {
    let Some(locator) = common_case_exact_row_table_locator(storage, row_id)? else {
        return Ok(None);
    };
    let Some(schema_hash) = locator.origin_schema_hash else {
        return Ok(None);
    };
    let row_raw_table_id = visible_row_raw_table_id(locator.table.as_str(), schema_hash);
    Ok(Some(ExactRowTableLocator {
        row_raw_table: row_raw_table_id.raw_table_name.clone(),
        table_name: row_raw_table_id.table_name.clone(),
        schema_hash,
    }))
}

fn exact_visible_row_table_locator_for_delete<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    branch: &str,
    row_id: ObjectId,
) -> Result<Option<ExactRowTableLocator>, StorageError> {
    if let Some(locator) = storage.load_visible_row_table_locator(branch, row_id)? {
        if locator.table_name.as_str() != table {
            return Err(StorageError::IoError(format!(
                "visible row locator table mismatch for {branch}/{row_id:?}: expected {table}, got {}",
                locator.table_name
            )));
        }
        return Ok(Some(locator));
    }

    let Some(locator) = common_case_exact_visible_row_table_locator(storage, row_id)? else {
        return Ok(None);
    };
    if locator.table_name.as_str() == table {
        Ok(Some(locator))
    } else {
        Ok(None)
    }
}

fn sealed_batch_submission_storage_descriptor_with_branch_ords() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("batch_id", ColumnType::BatchId),
        ColumnDescriptor::new("target_branch_ord", ColumnType::Integer),
        ColumnDescriptor::new("batch_digest", ColumnType::Bytea),
        ColumnDescriptor::new(
            "members",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("row_digest", ColumnType::Bytea),
                    ])),
                }),
            },
        ),
        ColumnDescriptor::new(
            "captured_frontier",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("branch_ord", ColumnType::Integer),
                        ColumnDescriptor::new("batch_id", ColumnType::BatchId),
                    ])),
                }),
            },
        ),
    ])
}

fn local_batch_record_storage_descriptor_with_branch_ords() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("batch_id", ColumnType::BatchId),
        ColumnDescriptor::new("mode", ColumnType::Text),
        ColumnDescriptor::new("sealed", ColumnType::Boolean),
        ColumnDescriptor::new(
            "members",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("table_name", ColumnType::Text),
                        ColumnDescriptor::new("branch_ord", ColumnType::Integer),
                        ColumnDescriptor::new("schema_hash", ColumnType::Bytea),
                        ColumnDescriptor::new("row_digest", ColumnType::Bytea),
                    ])),
                }),
            },
        ),
    ])
}

fn encode_batch_mode(mode: crate::batch_fate::BatchMode) -> &'static str {
    match mode {
        crate::batch_fate::BatchMode::Direct => "direct",
        crate::batch_fate::BatchMode::Transactional => "transactional",
    }
}

fn decode_batch_mode(raw: &str) -> Result<crate::batch_fate::BatchMode, StorageError> {
    match raw {
        "direct" => Ok(crate::batch_fate::BatchMode::Direct),
        "transactional" => Ok(crate::batch_fate::BatchMode::Transactional),
        other => Err(StorageError::IoError(format!(
            "unknown batch mode '{other}'"
        ))),
    }
}

fn load_history_user_descriptor_for_schema_hash<H: Storage + ?Sized>(
    storage: &H,
    table_hint: &str,
    schema_hash: SchemaHash,
) -> Result<Option<Arc<RowDescriptor>>, StorageError> {
    if let Some(descriptor) =
        cached_catalogue_user_descriptor_with_storage(storage, table_hint, schema_hash)
    {
        return Ok(Some(descriptor));
    }

    let Some(entry) = storage.load_catalogue_entry(schema_hash.to_object_id())? else {
        return Ok(None);
    };
    let schema = crate::schema_manager::encoding::decode_schema(&entry.content)
        .map_err(|err| StorageError::IoError(format!("decode schema for row history: {err}")))?;

    let hinted_table_name = crate::query_manager::types::TableName::new(table_hint);
    let Some(table_schema) = schema.get(&hinted_table_name) else {
        return Ok(None);
    };
    let descriptor = Arc::new(table_schema.columns.clone());
    cache_catalogue_user_descriptor_with_storage(
        storage,
        table_hint,
        schema_hash,
        descriptor.clone(),
    );
    Ok(Some(descriptor))
}

fn catalogue_schema_hash(entry: &CatalogueEntry) -> Result<SchemaHash, StorageError> {
    if let Some(schema_hash) = entry
        .metadata
        .get(MetadataKey::SchemaHash.as_str())
        .and_then(|raw| SchemaHash::from_hex(raw))
    {
        return Ok(schema_hash);
    }

    let schema = crate::schema_manager::encoding::decode_schema(&entry.content)
        .map_err(|err| StorageError::IoError(format!("decode schema for row history: {err}")))?;
    Ok(SchemaHash::compute(&schema))
}

fn schema_hashes_matching_branch<H: Storage + ?Sized>(
    storage: &H,
    branch: &str,
) -> Result<Vec<SchemaHash>, StorageError> {
    let Some(composed) = ComposedBranchName::parse(&BranchName::new(branch)) else {
        return Ok(Vec::new());
    };

    let mut hashes = storage
        .scan_catalogue_entries()?
        .into_iter()
        .filter_map(|entry| catalogue_schema_hash(&entry).ok())
        .filter(|schema_hash| schema_hash.short() == composed.schema_hash.short())
        .collect::<Vec<_>>();
    hashes.sort_by_key(|schema_hash| schema_hash.to_string());
    hashes.dedup();
    Ok(hashes)
}

fn catalogue_row_descriptors_for_table<H: Storage + ?Sized>(
    storage: &H,
    table_hint: &str,
) -> Result<Vec<(SchemaHash, crate::query_manager::types::RowDescriptor)>, StorageError> {
    let table_name = crate::query_manager::types::TableName::new(table_hint);
    let mut candidates = Vec::new();

    for entry in storage.scan_catalogue_entries()? {
        let schema =
            crate::schema_manager::encoding::decode_schema(&entry.content).map_err(|err| {
                StorageError::IoError(format!("decode schema for row history: {err}"))
            })?;
        let Some(table_schema) = schema.get(&table_name) else {
            continue;
        };
        candidates.push((SchemaHash::compute(&schema), table_schema.columns.clone()));
    }

    candidates.sort_by_key(|(schema_hash, _)| schema_hash.to_string());
    candidates.dedup_by(|(left_hash, _), (right_hash, _)| left_hash == right_hash);
    Ok(candidates)
}

fn required_history_user_descriptor_and_schema_hash_for_row<H: Storage + ?Sized>(
    storage: &H,
    table_hint: &str,
    row: &StoredRowBatch,
) -> Result<(SchemaHash, Arc<RowDescriptor>), StorageError> {
    let row_data_matches = |descriptor: &crate::query_manager::types::RowDescriptor| {
        row.data.is_empty() || crate::row_format::decode_row(descriptor, &row.data).is_ok()
    };
    let try_schema_hash = |candidates: &mut Vec<(SchemaHash, Arc<RowDescriptor>)>,
                           table_name: &str,
                           schema_hash: SchemaHash|
     -> Result<(), StorageError> {
        if let Some(descriptor) =
            load_history_user_descriptor_for_schema_hash(storage, table_name, schema_hash)?
            && row_data_matches(descriptor.as_ref())
        {
            candidates.push((schema_hash, descriptor));
        }
        Ok(())
    };

    let row_locator = storage.load_row_locator(row.row_id)?;
    let mut locator_candidates = Vec::new();
    if let Some(row_locator) = row_locator.as_ref()
        && let Some(origin_schema_hash) = row_locator.origin_schema_hash
    {
        try_schema_hash(
            &mut locator_candidates,
            row_locator.table.as_str(),
            origin_schema_hash,
        )?;
        if row_locator.table.as_str() != table_hint {
            try_schema_hash(&mut locator_candidates, table_hint, origin_schema_hash)?;
        }
    }
    locator_candidates.sort_by_key(|(schema_hash, _)| schema_hash.to_string());
    locator_candidates.dedup_by(|(left_hash, _), (right_hash, _)| left_hash == right_hash);
    if let [(schema_hash, descriptor)] = locator_candidates.as_slice() {
        return Ok((*schema_hash, descriptor.clone()));
    }

    let mut branch_candidates = Vec::new();
    for schema_hash in schema_hashes_matching_branch(storage, row.branch.as_str())? {
        try_schema_hash(&mut branch_candidates, table_hint, schema_hash)?;
    }
    branch_candidates.sort_by_key(|(schema_hash, _)| schema_hash.to_string());
    branch_candidates.dedup_by(|(left_hash, _), (right_hash, _)| left_hash == right_hash);
    if let [(schema_hash, descriptor)] = branch_candidates.as_slice() {
        return Ok((*schema_hash, descriptor.clone()));
    }

    let mut table_candidates = catalogue_row_descriptors_for_table(storage, table_hint)?
        .into_iter()
        .filter(|(_, descriptor)| row_data_matches(descriptor))
        .map(|(schema_hash, descriptor)| (schema_hash, Arc::new(descriptor)))
        .collect::<Vec<_>>();
    table_candidates.sort_by_key(|(schema_hash, _)| schema_hash.to_string());
    table_candidates.dedup_by(|(left_hash, _), (right_hash, _)| left_hash == right_hash);
    if let [(schema_hash, descriptor)] = table_candidates.as_slice() {
        return Ok((*schema_hash, descriptor.clone()));
    }

    let row_locator_debug = row_locator.or(storage.load_row_locator(row.row_id)?);
    let candidate_hashes = branch_candidates
        .iter()
        .chain(locator_candidates.iter())
        .chain(table_candidates.iter())
        .map(|(schema_hash, _)| schema_hash.to_string())
        .collect::<BTreeSet<_>>();

    if candidate_hashes.is_empty() {
        return Err(StorageError::IoError(format!(
            "missing catalogue-backed row descriptor for history row {} in table {} on branch {} (row_locator={row_locator_debug:?})",
            row.row_id, table_hint, row.branch,
        )));
    }

    Err(StorageError::IoError(format!(
        "ambiguous exact schema hash for history row {} in table {} on branch {}: {:?}",
        row.row_id, table_hint, row.branch, candidate_hashes
    )))
}

pub(crate) fn resolve_history_row_write_context<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    row: &StoredRowBatch,
) -> Result<PreparedRowWriteContext, StorageError> {
    let (schema_hash, user_descriptor) =
        required_history_user_descriptor_and_schema_hash_for_row(storage, table, row)?;
    prepared_row_write_context_for_schema_hash_and_descriptor(
        storage,
        table,
        schema_hash,
        row.row_id,
        user_descriptor,
    )
}

pub(crate) fn encode_history_row_bytes_with_context(
    context: &PreparedRowWriteContext,
    row: &StoredRowBatch,
) -> Result<OwnedHistoryRowBytes, StorageError> {
    let bytes =
        crate::row_histories::encode_flat_history_row(context.user_descriptor.as_ref(), row)
            .map_err(|err| StorageError::IoError(format!("encode flat history row: {err}")))?;

    Ok(OwnedHistoryRowBytes {
        row_raw_table: context
            .history_row_raw_table_id
            .raw_table_name()
            .to_string(),
        row_raw_table_id: context.history_row_raw_table_id.clone(),
        user_descriptor: context.user_descriptor.clone(),
        branch: row.branch.to_string(),
        row_id: row.row_id,
        batch_id: row.batch_id(),
        needs_exact_locator: context.needs_exact_locator,
        bytes,
    })
}

pub(crate) fn encode_visible_row_bytes_with_context(
    context: &PreparedRowWriteContext,
    entry: &VisibleRowEntry,
) -> Result<OwnedVisibleRowBytes, StorageError> {
    let bytes = crate::row_histories::encode_flat_visible_row_entry(
        context.user_descriptor.as_ref(),
        entry,
    )
    .map_err(|err| StorageError::IoError(format!("encode flat visible row: {err}")))?;

    Ok(OwnedVisibleRowBytes {
        row_raw_table: context
            .visible_row_raw_table_id
            .raw_table_name()
            .to_string(),
        row_raw_table_id: context.visible_row_raw_table_id.clone(),
        user_descriptor: context.user_descriptor.clone(),
        branch: entry.current_row.branch.to_string(),
        row_id: entry.current_row.row_id,
        needs_exact_locator: context.needs_exact_locator,
        bytes,
    })
}

pub(crate) fn encode_history_row_bytes_for_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    rows: &[StoredRowBatch],
) -> Result<Vec<OwnedHistoryRowBytes>, StorageError> {
    rows.iter()
        .map(|row| {
            let context = resolve_history_row_write_context(storage, table, row)?;
            encode_history_row_bytes_with_context(&context, row)
        })
        .collect()
}

pub(crate) fn encode_visible_row_bytes_for_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    entries: &[VisibleRowEntry],
) -> Result<Vec<OwnedVisibleRowBytes>, StorageError> {
    entries
        .iter()
        .map(|entry| {
            let context = resolve_history_row_write_context(storage, table, &entry.current_row)?;
            encode_visible_row_bytes_with_context(&context, entry)
        })
        .collect()
}

fn decode_history_row_bytes_in_table(
    resolved: &ResolvedRowTable,
    row_id: ObjectId,
    branch: &str,
    batch_id: BatchId,
    bytes: &[u8],
) -> Result<StoredRowBatch, StorageError> {
    decode_flat_history_row_with_codecs(
        resolved.row_codecs.as_ref(),
        row_id,
        branch,
        batch_id,
        bytes,
    )
    .map_err(|err| StorageError::IoError(format!("decode flat history row: {err}")))
}

fn decode_visible_row_entry_bytes_in_table(
    resolved: &ResolvedRowTable,
    row_id: ObjectId,
    branch: &str,
    bytes: &[u8],
) -> Result<VisibleRowEntry, StorageError> {
    decode_flat_visible_row_entry_with_codecs(resolved.row_codecs.as_ref(), row_id, branch, bytes)
        .map_err(|err| StorageError::IoError(format!("decode flat visible row: {err}")))
}

pub(super) fn scan_history_row_bytes_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    scan: HistoryScan,
) -> Result<Vec<OwnedHistoryRowBytes>, StorageError> {
    let row_raw_table_ids = row_raw_table_ids_for_table(storage, RowRawTableKind::History, table)?;
    let prefix = match scan {
        HistoryScan::Branch | HistoryScan::AsOf { .. } => {
            key_codec::history_row_raw_table_prefix(None)
        }
        HistoryScan::Row { row_id } => key_codec::history_row_raw_table_prefix(Some(row_id)),
    };
    let mut rows = Vec::new();
    for row_raw_table_id in row_raw_table_ids {
        let resolved = resolved_row_table_from_id(storage, row_raw_table_id.clone())?
            .expect("row raw table id from header scan must resolve");
        let row_raw_table = resolved.row_raw_table.clone();
        for (key, bytes) in storage.raw_table_scan_prefix(&row_raw_table, &prefix)? {
            let (row_id, branch, batch_id) = key_codec::decode_history_row_raw_table_key(&key)?;
            rows.push(OwnedHistoryRowBytes {
                row_raw_table_id: row_raw_table_id.clone(),
                row_raw_table: row_raw_table.clone(),
                user_descriptor: resolved.user_descriptor.clone(),
                branch,
                row_id,
                batch_id,
                needs_exact_locator: true,
                bytes,
            });
        }
    }
    Ok(rows)
}

pub(super) fn scan_visible_row_bytes_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    branch: &str,
) -> Result<Vec<OwnedVisibleRowBytes>, StorageError> {
    let row_raw_table_ids = row_raw_table_ids_for_table(storage, RowRawTableKind::Visible, table)?;
    let prefix = key_codec::visible_row_raw_table_prefix(branch);
    let mut rows = Vec::new();
    for row_raw_table_id in row_raw_table_ids {
        let resolved = resolved_row_table_from_id(storage, row_raw_table_id.clone())?
            .expect("row raw table id from header scan must resolve");
        let row_raw_table = resolved.row_raw_table.clone();
        for (key, bytes) in storage.raw_table_scan_prefix(&row_raw_table, &prefix)? {
            let (decoded_branch, row_id) = key_codec::decode_visible_row_raw_table_key(&key)?;
            if decoded_branch != branch {
                return Err(StorageError::IoError(format!(
                    "visible row raw table key '{key}' decoded unexpected branch '{decoded_branch}'"
                )));
            }
            rows.push(OwnedVisibleRowBytes {
                row_raw_table_id: row_raw_table_id.clone(),
                row_raw_table: row_raw_table.clone(),
                user_descriptor: resolved.user_descriptor.clone(),
                branch: decoded_branch,
                row_id,
                needs_exact_locator: true,
                bytes,
            });
        }
    }
    Ok(rows)
}

pub(super) fn load_history_row_batch_row_bytes_with_storage<H: Storage + ?Sized>(
    storage: &H,
    _table: &str,
    branch: &str,
    row_id: ObjectId,
    batch_id: BatchId,
) -> Result<Option<OwnedHistoryRowBytes>, StorageError> {
    let key = key_codec::history_row_raw_table_key(row_id, branch, batch_id);
    if let Some(locator) = common_case_exact_history_row_table_locator(storage, row_id)? {
        let row_raw_table = locator.row_raw_table.to_string();
        if let Some(bytes) = storage.raw_table_get(&row_raw_table, &key)? {
            let resolved = resolved_row_table_from_locator(storage, &locator)?
                .expect("common-case locator-resolved history row table must exist");
            return Ok(Some(OwnedHistoryRowBytes {
                row_raw_table_id: RowRawTableId {
                    kind: RowRawTableKind::History,
                    table_name: locator.table_name.clone(),
                    schema_hash: locator.schema_hash,
                    raw_table_name: locator.row_raw_table.clone(),
                },
                row_raw_table,
                user_descriptor: resolved.user_descriptor,
                branch: branch.to_string(),
                row_id,
                batch_id,
                needs_exact_locator: false,
                bytes,
            }));
        }
    }

    let Some(locator) = storage.load_history_row_batch_table_locator(branch, row_id, batch_id)?
    else {
        return Ok(None);
    };
    let resolved = resolved_row_table_from_locator(storage, &locator)?
        .expect("locator-resolved row table must exist");
    let row_raw_table = locator.row_raw_table.to_string();
    Ok(storage
        .raw_table_get(&row_raw_table, &key)?
        .map(|bytes| OwnedHistoryRowBytes {
            row_raw_table_id: RowRawTableId {
                kind: RowRawTableKind::History,
                table_name: locator.table_name.clone(),
                schema_hash: locator.schema_hash,
                raw_table_name: locator.row_raw_table.clone(),
            },
            row_raw_table,
            user_descriptor: resolved.user_descriptor,
            branch: branch.to_string(),
            row_id,
            batch_id,
            needs_exact_locator: true,
            bytes,
        }))
}

pub(super) fn load_visible_region_row_bytes_with_storage<H: Storage + ?Sized>(
    storage: &H,
    _table: &str,
    branch: &str,
    row_id: ObjectId,
) -> Result<Option<OwnedVisibleRowBytes>, StorageError> {
    let key = key_codec::visible_row_raw_table_key(branch, row_id);
    if let Some(locator) = common_case_exact_visible_row_table_locator(storage, row_id)? {
        let row_raw_table = locator.row_raw_table.to_string();
        if let Some(bytes) = storage.raw_table_get(&row_raw_table, &key)? {
            let resolved = resolved_row_table_from_locator(storage, &locator)?
                .expect("common-case locator-resolved visible row table must exist");
            return Ok(Some(OwnedVisibleRowBytes {
                row_raw_table_id: RowRawTableId {
                    kind: RowRawTableKind::Visible,
                    table_name: locator.table_name.clone(),
                    schema_hash: locator.schema_hash,
                    raw_table_name: locator.row_raw_table.clone(),
                },
                row_raw_table,
                user_descriptor: resolved.user_descriptor,
                branch: branch.to_string(),
                row_id,
                needs_exact_locator: false,
                bytes,
            }));
        }
    }

    let Some(locator) = storage.load_visible_row_table_locator(branch, row_id)? else {
        return Ok(None);
    };
    let resolved = resolved_row_table_from_locator(storage, &locator)?
        .expect("locator-resolved row table must exist");
    let row_raw_table = locator.row_raw_table.to_string();
    Ok(storage
        .raw_table_get(&row_raw_table, &key)?
        .map(|bytes| OwnedVisibleRowBytes {
            row_raw_table_id: RowRawTableId {
                kind: RowRawTableKind::Visible,
                table_name: locator.table_name.clone(),
                schema_hash: locator.schema_hash,
                raw_table_name: locator.row_raw_table.clone(),
            },
            row_raw_table,
            user_descriptor: resolved.user_descriptor,
            branch: branch.to_string(),
            row_id,
            needs_exact_locator: true,
            bytes,
        }))
}

fn scan_history_row_batches_for_schema_hash<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    schema_hash: SchemaHash,
    row_id: ObjectId,
) -> Result<Vec<StoredRowBatch>, StorageError> {
    let row_raw_table_id = history_row_raw_table_id(table, schema_hash);
    let Some(resolved) = resolved_row_table_from_id(storage, row_raw_table_id.clone())? else {
        return Ok(Vec::new());
    };

    let prefix = key_codec::history_row_raw_table_prefix(Some(row_id));
    let mut rows = Vec::new();
    for (key, bytes) in storage.raw_table_scan_prefix(row_raw_table_id.raw_table_name(), &prefix)? {
        let (decoded_row_id, branch, batch_id) = key_codec::decode_history_row_raw_table_key(&key)?;
        rows.push(decode_history_row_bytes_in_table(
            &resolved,
            decoded_row_id,
            branch.as_str(),
            batch_id,
            &bytes,
        )?);
    }
    rows.sort_by_key(|row| (row.branch.clone(), row.updated_at, row.batch_id()));
    Ok(rows)
}

pub(super) fn scan_visible_region_row_batch_branches_with_storage<H: Storage + ?Sized>(
    storage: &H,
    table: &str,
    row_id: ObjectId,
) -> Result<Vec<String>, StorageError> {
    let row_raw_table_ids = row_raw_table_ids_for_table(storage, RowRawTableKind::History, table)?;
    let prefix = key_codec::history_row_raw_table_prefix(Some(row_id));
    let mut branches = Vec::new();
    for row_raw_table_id in row_raw_table_ids {
        for key in storage.raw_table_scan_prefix_keys(row_raw_table_id.raw_table_name(), &prefix)? {
            let (_decoded_row_id, branch, _batch_id) =
                key_codec::decode_history_row_raw_table_key(&key)?;
            branches.push(branch);
        }
    }
    branches.sort();
    branches.dedup();
    Ok(branches)
}

pub(crate) fn patch_row_region_rows_by_batch_with_storage<H: Storage + ?Sized>(
    storage: &mut H,
    table: &str,
    batch_id: crate::row_histories::BatchId,
    state: Option<RowState>,
    confirmed_tier: Option<DurabilityTier>,
) -> Result<(), StorageError> {
    let history_rows = {
        let resolved_tables =
            resolved_row_tables_for_table(storage, RowRawTableKind::History, table)?;
        let mut rows = Vec::new();
        for resolved in &resolved_tables {
            for (key, bytes) in storage.raw_table_scan_prefix(
                &resolved.row_raw_table,
                &key_codec::history_row_raw_table_prefix(None),
            )? {
                let (row_id, branch, batch_id) = key_codec::decode_history_row_raw_table_key(&key)?;
                rows.push(decode_history_row_bytes_in_table(
                    resolved,
                    row_id,
                    branch.as_str(),
                    batch_id,
                    &bytes,
                )?);
            }
        }
        rows
    };

    let mut patched_history = Vec::new();
    let mut history_by_visible_row = HashMap::<(String, ObjectId), Vec<StoredRowBatch>>::new();
    let mut affected_visible_rows = HashSet::<(String, ObjectId)>::new();

    for mut row in history_rows {
        if row.batch_id == batch_id {
            if let Some(state) = state {
                row.state = state;
            }
            row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
                (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                (Some(existing), None) => Some(existing),
                (None, incoming) => incoming,
            };
            affected_visible_rows.insert((row.branch.to_string(), row.row_id));
            patched_history.push(row.clone());
        }

        history_by_visible_row
            .entry((row.branch.to_string(), row.row_id))
            .or_default()
            .push(row);
    }

    if !patched_history.is_empty() {
        storage.append_history_region_rows(table, &patched_history)?;
    }

    let mut rebuilt_visible_entries = Vec::new();
    let mut rows_without_visible_head = Vec::new();
    for (branch, row_id) in &affected_visible_rows {
        let Some(existing_entry) = storage.load_visible_region_entry(table, branch, *row_id)?
        else {
            continue;
        };

        let history_rows = history_by_visible_row
            .remove(&(branch.clone(), *row_id))
            .unwrap_or_default();
        let context = if let Some(current_row) = history_rows.first() {
            resolve_history_row_write_context(storage, table, current_row)?
        } else {
            continue;
        };
        if let Some(entry) = VisibleRowEntry::rebuild_with_descriptor(
            context.user_descriptor.as_ref(),
            &history_rows,
        )
        .map_err(|err| StorageError::IoError(format!("rebuild visible entry: {err}")))?
        {
            rebuilt_visible_entries.push(entry);
            continue;
        }

        let mut current = existing_entry.current_row.clone();
        if current.batch_id == batch_id {
            if let Some(state) = state {
                current.state = state;
            }
            current.confirmed_tier = match (current.confirmed_tier, confirmed_tier) {
                (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
                (Some(existing), None) => Some(existing),
                (None, incoming) => incoming,
            };
        }

        if current.state.is_visible() {
            if let Some(entry) = VisibleRowEntry::rebuild_with_descriptor(
                context.user_descriptor.as_ref(),
                &history_rows,
            )
            .map_err(|err| StorageError::IoError(format!("rebuild visible entry: {err}")))?
            {
                rebuilt_visible_entries.push(entry);
            }
        } else {
            rows_without_visible_head.push((branch.clone(), *row_id));
        }
    }

    if !rebuilt_visible_entries.is_empty() {
        storage.upsert_visible_region_rows(table, &rebuilt_visible_entries)?;
    }
    for (branch, row_id) in rows_without_visible_head {
        storage.delete_visible_region_row(table, &branch, row_id)?;
    }

    Ok(())
}

pub(crate) fn patch_exact_row_batch_with_storage<H: Storage + ?Sized>(
    storage: &mut H,
    table: &str,
    branch: &str,
    row_id: ObjectId,
    batch_id: crate::row_histories::BatchId,
    state: Option<RowState>,
    confirmed_tier: Option<DurabilityTier>,
) -> Result<bool, StorageError> {
    let Some(mut row) = storage.load_history_row_batch(table, branch, row_id, batch_id)? else {
        return Ok(false);
    };

    if let Some(state) = state {
        row.state = state;
    }
    row.confirmed_tier = match (row.confirmed_tier, confirmed_tier) {
        (Some(existing), Some(incoming)) => Some(existing.max(incoming)),
        (Some(existing), None) => Some(existing),
        (None, incoming) => incoming,
    };
    let history_rows = storage.scan_history_row_batches(table, row_id)?;
    let mut patched_history = history_rows.clone();
    if let Some(existing) = patched_history
        .iter_mut()
        .find(|candidate| candidate.branch == branch && candidate.batch_id() == batch_id)
    {
        *existing = row.clone();
    }
    let context = resolve_history_row_write_context(storage, table, &row)?;

    let visible_entries = VisibleRowEntry::rebuild_with_descriptor(
        context.user_descriptor.as_ref(),
        &patched_history,
    )
    .map_err(|err| StorageError::IoError(format!("rebuild visible entry: {err}")))?
    .into_iter()
    .collect::<Vec<_>>();

    storage.apply_row_mutation(table, std::slice::from_ref(&row), &visible_entries, &[])?;
    if visible_entries.is_empty() {
        storage.delete_visible_region_row(table, branch, row_id)?;
    }

    Ok(true)
}

fn branch_matches_transaction_family(
    branch_name: BranchName,
    target_branch_name: BranchName,
) -> bool {
    match (
        ComposedBranchName::parse(&branch_name),
        ComposedBranchName::parse(&target_branch_name),
    ) {
        (Some(branch), Some(target)) => {
            branch.matches_env_and_branch(&target.env, &target.user_branch)
        }
        _ => branch_name == target_branch_name,
    }
}

fn decode_storage_batch_id_value(value: &Value, context: &str) -> Result<BatchId, StorageError> {
    match value {
        Value::BatchId(bytes) => Ok(BatchId(*bytes)),
        Value::Bytea(bytes) => {
            let bytes: [u8; 16] = bytes.as_slice().try_into().map_err(|_| {
                StorageError::IoError(format!("{context}: expected 16 bytes, got {}", bytes.len()))
            })?;
            Ok(BatchId(bytes))
        }
        other => Err(StorageError::IoError(format!(
            "{context}: expected batch id bytes, got {other:?}"
        ))),
    }
}

fn encode_sealed_batch_submission_with_branch_ords<H: Storage + ?Sized>(
    storage: &mut H,
    submission: &SealedBatchSubmission,
) -> Result<Vec<u8>, StorageError> {
    let target_branch_ord = storage.resolve_or_alloc_branch_ord(submission.target_branch_name)?;
    let member_values = submission
        .members
        .iter()
        .map(|member| {
            Ok(Value::Row {
                id: None,
                values: vec![
                    Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                    Value::Bytea(member.row_digest.0.to_vec()),
                ],
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    let frontier_values = submission
        .captured_frontier
        .iter()
        .map(|member| {
            let branch_ord = storage.resolve_or_alloc_branch_ord(member.branch_name)?;
            Ok(Value::Row {
                id: None,
                values: vec![
                    Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                    Value::Integer(branch_ord),
                    Value::BatchId(member.batch_id.0),
                ],
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    encode_row(
        &sealed_batch_submission_storage_descriptor_with_branch_ords(),
        &[
            Value::BatchId(*submission.batch_id.as_bytes()),
            Value::Integer(target_branch_ord),
            Value::Bytea(submission.batch_digest.0.to_vec()),
            Value::Array(member_values),
            Value::Array(frontier_values),
        ],
    )
    .map_err(|err| StorageError::IoError(format!("encode sealed batch submission: {err}")))
}

fn decode_sealed_batch_submission_with_branch_ords<H: Storage + ?Sized>(
    storage: &H,
    bytes: &[u8],
) -> Result<SealedBatchSubmission, StorageError> {
    let values = decode_row(
        &sealed_batch_submission_storage_descriptor_with_branch_ords(),
        bytes,
    )
    .map_err(|err| StorageError::IoError(format!("decode sealed batch submission: {err}")))?;
    let [
        batch_id,
        target_branch_ord,
        batch_digest,
        members,
        captured_frontier,
    ] = values.as_slice()
    else {
        return Err(StorageError::IoError(
            "unexpected sealed batch submission shape".to_string(),
        ));
    };

    let batch_id = decode_storage_batch_id_value(batch_id, "decode sealed batch id")?;
    let target_branch_ord = match target_branch_ord {
        Value::Integer(raw) => *raw,
        other => {
            return Err(StorageError::IoError(format!(
                "expected target branch ord integer, got {other:?}"
            )));
        }
    };
    let target_branch_name = storage
        .load_branch_name_by_ord(target_branch_ord)?
        .ok_or_else(|| {
            StorageError::IoError(format!(
                "missing branch name for target branch ord {target_branch_ord}"
            ))
        })?;
    let batch_digest = match batch_digest {
        Value::Bytea(bytes) => Digest32(bytes.as_slice().try_into().map_err(|_| {
            StorageError::IoError(format!(
                "expected sealed batch digest to be 32 bytes, got {}",
                bytes.len()
            ))
        })?),
        other => {
            return Err(StorageError::IoError(format!(
                "expected sealed batch digest bytes, got {other:?}"
            )));
        }
    };

    let members = match members {
        Value::Array(elements) => elements
            .iter()
            .map(|element| match element {
                Value::Row { values, .. } => {
                    let [object_id, row_digest] = values.as_slice() else {
                        return Err(StorageError::IoError(
                            "expected sealed batch member row to have two values".to_string(),
                        ));
                    };
                    let object_id = match object_id {
                        Value::Bytea(bytes) => uuid::Uuid::from_slice(bytes)
                            .map(ObjectId::from_uuid)
                            .map_err(|err| {
                                StorageError::IoError(format!(
                                    "decode sealed batch member object id uuid: {err}"
                                ))
                            })?,
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected sealed batch member object id bytes, got {other:?}"
                            )));
                        }
                    };
                    let row_digest = match row_digest {
                        Value::Bytea(bytes) => Digest32(bytes.as_slice().try_into().map_err(
                            |_| {
                                StorageError::IoError(format!(
                                    "expected sealed batch member row digest to be 32 bytes, got {}",
                                    bytes.len()
                                ))
                            },
                        )?),
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected sealed batch member row digest bytes, got {other:?}"
                            )));
                        }
                    };
                    Ok(crate::batch_fate::SealedBatchMember {
                        object_id,
                        row_digest,
                    })
                }
                other => Err(StorageError::IoError(format!(
                    "expected sealed batch member row, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>, StorageError>>()?,
        other => {
            return Err(StorageError::IoError(format!(
                "expected sealed batch members array, got {other:?}"
            )));
        }
    };

    let captured_frontier = match captured_frontier {
        Value::Array(elements) => elements
            .iter()
            .map(|element| match element {
                Value::Row { values, .. } => {
                    let [object_id, branch_ord, batch_id] = values.as_slice() else {
                        return Err(StorageError::IoError(
                            "expected captured frontier row to have three values".to_string(),
                        ));
                    };
                    let object_id = match object_id {
                        Value::Bytea(bytes) => uuid::Uuid::from_slice(bytes)
                            .map(ObjectId::from_uuid)
                            .map_err(|err| {
                                StorageError::IoError(format!(
                                    "decode captured frontier object id uuid: {err}"
                                ))
                            })?,
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected captured frontier object id bytes, got {other:?}"
                            )));
                        }
                    };
                    let branch_ord = match branch_ord {
                        Value::Integer(raw) => *raw,
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected captured frontier branch ord integer, got {other:?}"
                            )));
                        }
                    };
                    let branch_name =
                        storage
                            .load_branch_name_by_ord(branch_ord)?
                            .ok_or_else(|| {
                                StorageError::IoError(format!(
                                    "missing branch name for captured frontier ord {branch_ord}"
                                ))
                            })?;
                    let batch_id = decode_storage_batch_id_value(
                        batch_id,
                        "decode captured frontier batch id",
                    )?;
                    Ok(CapturedFrontierMember {
                        object_id,
                        branch_name,
                        batch_id,
                    })
                }
                other => Err(StorageError::IoError(format!(
                    "expected captured frontier row, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>, StorageError>>()?,
        other => {
            return Err(StorageError::IoError(format!(
                "expected captured frontier array, got {other:?}"
            )));
        }
    };

    let submission =
        SealedBatchSubmission::new(batch_id, target_branch_name, members, captured_frontier);
    if submission.batch_digest != batch_digest {
        return Err(StorageError::IoError(format!(
            "sealed batch digest mismatch: expected {batch_digest:?}, computed {:?}",
            submission.batch_digest
        )));
    }
    Ok(submission)
}

fn encode_local_batch_record_with_branch_ords<H: Storage + ?Sized>(
    storage: &mut H,
    record: &LocalBatchRecord,
) -> Result<Vec<u8>, StorageError> {
    encode_row(
        &local_batch_record_storage_descriptor_with_branch_ords(),
        &[
            Value::BatchId(*record.batch_id.as_bytes()),
            Value::Text(encode_batch_mode(record.mode).to_string()),
            Value::Boolean(record.sealed),
            Value::Array(
                record
                    .members
                    .iter()
                    .map(|member| {
                        let branch_ord = storage.resolve_or_alloc_branch_ord(member.branch_name)?;
                        Ok(Value::Row {
                            id: None,
                            values: vec![
                                Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                                Value::Text(member.table_name.clone()),
                                Value::Integer(branch_ord),
                                Value::Bytea(member.schema_hash.as_bytes().to_vec()),
                                Value::Bytea(member.row_digest.0.to_vec()),
                            ],
                        })
                    })
                    .collect::<Result<Vec<_>, StorageError>>()?,
            ),
        ],
    )
    .map_err(|err| StorageError::IoError(format!("encode local batch record: {err}")))
}

fn decode_local_batch_record_with_branch_ords<H: Storage + ?Sized>(
    storage: &H,
    bytes: &[u8],
) -> Result<LocalBatchRecord, StorageError> {
    let values = decode_row(
        &local_batch_record_storage_descriptor_with_branch_ords(),
        bytes,
    )
    .map_err(|err| StorageError::IoError(format!("decode local batch record: {err}")))?;
    let [batch_id, mode, sealed, members] = values.as_slice() else {
        return Err(StorageError::IoError(
            "unexpected local batch record shape".to_string(),
        ));
    };

    let batch_id = decode_storage_batch_id_value(batch_id, "decode local batch record batch id")?;
    let mode = match mode {
        Value::Text(raw) => decode_batch_mode(raw)?,
        other => {
            return Err(StorageError::IoError(format!(
                "expected batch mode text, got {other:?}"
            )));
        }
    };
    let sealed = match sealed {
        Value::Boolean(value) => *value,
        other => {
            return Err(StorageError::IoError(format!(
                "expected sealed boolean, got {other:?}"
            )));
        }
    };
    let members = match members {
        Value::Array(values) => values
                .iter()
                .map(|value| match value {
                    Value::Row { values, .. } => {
                    let [object_id, table_name, branch_ord, schema_hash, row_digest] =
                        values.as_slice()
                    else {
                        return Err(StorageError::IoError(
                            "expected local batch member row to have five values".to_string(),
                        ));
                    };
                    let object_id = match object_id {
                        Value::Bytea(bytes) => {
                            let uuid = uuid::Uuid::from_slice(bytes).map_err(|err| {
                                StorageError::IoError(format!(
                                    "decode local batch member object id: expected uuid bytes: {err}"
                                ))
                            })?;
                            ObjectId::from_uuid(uuid)
                        }
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member object id bytes, got {other:?}"
                            )));
                        }
                    };
                    let table_name = match table_name {
                        Value::Text(raw) => raw.clone(),
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member table name text, got {other:?}"
                            )));
                        }
                    };
                    let branch_ord = match branch_ord {
                        Value::Integer(raw) => *raw,
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member branch ord integer, got {other:?}"
                            )));
                        }
                    };
                    let branch_name = storage
                        .load_branch_name_by_ord(branch_ord)?
                        .ok_or_else(|| {
                            StorageError::IoError(format!(
                                "missing branch name for local batch member branch ord {branch_ord}"
                            ))
                        })?;
                    let schema_hash = match schema_hash {
                        Value::Bytea(bytes) => {
                            let bytes: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                                StorageError::IoError(format!(
                                    "expected local batch member schema hash to be 32 bytes, got {}",
                                    bytes.len()
                                ))
                            })?;
                            SchemaHash::from_bytes(bytes)
                        }
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member schema hash bytes, got {other:?}"
                            )));
                        }
                    };
                    let row_digest = match row_digest {
                        Value::Bytea(bytes) => Digest32(bytes.as_slice().try_into().map_err(
                            |_| {
                                StorageError::IoError(format!(
                                    "expected local batch member row digest to be 32 bytes, got {}",
                                    bytes.len()
                                ))
                            },
                        )?),
                        other => {
                            return Err(StorageError::IoError(format!(
                                "expected local batch member row digest bytes, got {other:?}"
                            )));
                        }
                    };
                    Ok(crate::batch_fate::LocalBatchMember {
                        object_id,
                        table_name,
                        branch_name,
                        schema_hash,
                        row_digest,
                    })
                }
                other => Err(StorageError::IoError(format!(
                    "expected local batch member row, got {other:?}"
                ))),
            })
            .collect::<Result<Vec<_>, StorageError>>()?,
        other => {
            return Err(StorageError::IoError(format!(
                "expected local batch members array, got {other:?}"
            )));
        }
    };

    Ok(LocalBatchRecord {
        batch_id,
        mode,
        sealed,
        members,
        sealed_submission: storage.load_sealed_batch_submission(batch_id)?,
        latest_settlement: storage.load_authoritative_batch_settlement(batch_id)?,
    })
}
// ============================================================================
// Value Encoding for Index Keys
// ============================================================================
//
// Values must be encoded so lexicographic byte ordering equals semantic ordering.
// This enables range queries via BTreeMap::range().

/// Returns true if the value is Double(0.0) or Double(-0.0).
///
/// IEEE 754 defines -0.0 == 0.0, but they have distinct bit patterns and
/// therefore distinct index encodings. Query operations must check both.
pub(crate) fn is_double_zero(value: &Value) -> bool {
    matches!(value, Value::Double(f) if *f == 0.0)
}

/// Encode a Value into bytes that sort correctly for range queries.
pub(crate) fn encode_value(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0x00], // Null sorts first

        Value::Boolean(b) => {
            // false (0x01) < true (0x02)
            vec![0x01, if *b { 0x02 } else { 0x01 }]
        }

        Value::Integer(n) => {
            // Flip sign bit so negative < positive, big-endian for correct ordering
            let mut bytes = vec![0x02];
            bytes.extend_from_slice(&((*n as i64) ^ i64::MIN).to_be_bytes());
            bytes
        }

        Value::BigInt(n) => {
            // Flip sign bit so negative < positive, big-endian for correct ordering
            let mut bytes = vec![0x03];
            bytes.extend_from_slice(&(*n ^ i64::MIN).to_be_bytes());
            bytes
        }

        Value::Double(f) => {
            let mut bytes = vec![0x09];
            let bits = f.to_bits();
            // Flip for lexicographic ordering: if sign bit set, flip all bits;
            // otherwise flip only the sign bit.
            let ordered = if bits & (1u64 << 63) != 0 {
                !bits
            } else {
                bits ^ (1u64 << 63)
            };
            bytes.extend_from_slice(&ordered.to_be_bytes());
            bytes
        }

        Value::Timestamp(ts) => {
            // Unsigned, big-endian (already sorts correctly)
            let mut bytes = vec![0x04];
            bytes.extend_from_slice(&ts.to_be_bytes());
            bytes
        }

        Value::Text(s) => {
            // UTF-8 bytes sort correctly for ASCII; good enough for now
            let mut bytes = vec![0x05];
            bytes.extend_from_slice(s.as_bytes());
            bytes
        }

        Value::Uuid(id) => {
            // UUID bytes compare lexicographically by raw value.
            let mut bytes = vec![0x06];
            bytes.extend_from_slice(id.uuid().as_bytes());
            bytes
        }

        Value::BatchId(batch_id) => {
            let mut bytes = vec![0x0A];
            bytes.extend_from_slice(batch_id);
            bytes
        }

        Value::Bytea(bytes_value) => {
            // Raw bytes for exact-match index semantics.
            let mut bytes = vec![0x09];
            bytes.extend_from_slice(bytes_value);
            bytes
        }

        Value::Array(_) => {
            // Arrays use serialized bytes for equality semantics.
            // The durable key codec hashes oversized segments if needed.
            let mut bytes = vec![0x07];
            let json = serde_json::to_string(value).unwrap_or_default();
            bytes.extend_from_slice(json.as_bytes());
            bytes
        }

        Value::Row { .. } => {
            // Rows use serialized bytes for equality semantics.
            // The durable key codec hashes oversized segments if needed.
            let mut bytes = vec![0x08];
            let json = serde_json::to_string(value).unwrap_or_default();
            bytes.extend_from_slice(json.as_bytes());
            bytes
        }
    }
}
