use crate::digest::Digest32;
use blake3::Hasher;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::SchemaHash;
use crate::query_manager::types::{ColumnDescriptor, ColumnType, RowDescriptor, Value};
use crate::row_format::{decode_row, encode_row};
use crate::row_histories::BatchId;
use crate::sync_manager::DurabilityTier;

pub const BATCH_FATE_STORAGE_FORMAT_V2: i32 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BatchMode {
    Direct,
    Transactional,
}

impl BatchMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Direct => "direct",
            Self::Transactional => "transactional",
        }
    }

    fn parse(raw: &str) -> Result<Self, String> {
        match raw {
            "direct" => Ok(Self::Direct),
            "transactional" => Ok(Self::Transactional),
            other => Err(format!("unknown batch mode '{other}'")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleBatchMember {
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    pub batch_id: BatchId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalBatchMember {
    pub object_id: ObjectId,
    pub table_name: String,
    pub branch_name: BranchName,
    pub schema_hash: SchemaHash,
    pub row_digest: Digest32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchSettlement {
    Missing {
        batch_id: BatchId,
    },
    Rejected {
        batch_id: BatchId,
        code: String,
        reason: String,
    },
    DurableDirect {
        batch_id: BatchId,
        confirmed_tier: DurabilityTier,
        visible_members: Vec<VisibleBatchMember>,
    },
    AcceptedTransaction {
        batch_id: BatchId,
        confirmed_tier: DurabilityTier,
        visible_members: Vec<VisibleBatchMember>,
    },
}

impl BatchSettlement {
    pub fn batch_id(&self) -> BatchId {
        match self {
            Self::Missing { batch_id }
            | Self::Rejected { batch_id, .. }
            | Self::DurableDirect { batch_id, .. }
            | Self::AcceptedTransaction { batch_id, .. } => *batch_id,
        }
    }

    pub fn confirmed_tier(&self) -> Option<DurabilityTier> {
        match self {
            Self::DurableDirect { confirmed_tier, .. }
            | Self::AcceptedTransaction { confirmed_tier, .. } => Some(*confirmed_tier),
            Self::Missing { .. } | Self::Rejected { .. } => None,
        }
    }

    pub fn visible_members(&self) -> &[VisibleBatchMember] {
        match self {
            Self::DurableDirect {
                visible_members, ..
            }
            | Self::AcceptedTransaction {
                visible_members, ..
            } => visible_members,
            Self::Missing { .. } | Self::Rejected { .. } => &[],
        }
    }

    pub fn merged_with(&self, incoming: &BatchSettlement) -> BatchSettlement {
        match (self, incoming) {
            (
                Self::DurableDirect {
                    batch_id,
                    confirmed_tier: existing_tier,
                    visible_members: existing_members,
                },
                Self::DurableDirect {
                    confirmed_tier: incoming_tier,
                    visible_members: incoming_members,
                    ..
                },
            ) => {
                let mut visible_members = existing_members.clone();
                for member in incoming_members {
                    if !visible_members.contains(member) {
                        visible_members.push(member.clone());
                    }
                }
                Self::DurableDirect {
                    batch_id: *batch_id,
                    confirmed_tier: (*existing_tier).max(*incoming_tier),
                    visible_members,
                }
            }
            (
                Self::AcceptedTransaction {
                    batch_id,
                    confirmed_tier: existing_tier,
                    visible_members: existing_members,
                },
                Self::AcceptedTransaction {
                    confirmed_tier: incoming_tier,
                    visible_members: incoming_members,
                    ..
                },
            ) => {
                let mut visible_members = existing_members.clone();
                for member in incoming_members {
                    if !visible_members.contains(member) {
                        visible_members.push(member.clone());
                    }
                }
                Self::AcceptedTransaction {
                    batch_id: *batch_id,
                    confirmed_tier: (*existing_tier).max(*incoming_tier),
                    visible_members,
                }
            }
            _ => incoming.clone(),
        }
    }

    pub fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        let (kind, code, reason, confirmed_tier, visible_members) = match self {
            Self::Missing { .. } => (
                "missing",
                Value::Null,
                Value::Null,
                Value::Null,
                Value::Array(Vec::new()),
            ),
            Self::Rejected { code, reason, .. } => (
                "rejected",
                Value::Text(code.clone()),
                Value::Text(reason.clone()),
                Value::Null,
                Value::Array(Vec::new()),
            ),
            Self::DurableDirect {
                confirmed_tier,
                visible_members,
                ..
            } => (
                "durable_direct",
                Value::Null,
                Value::Null,
                Value::Text(durability_tier_to_str(*confirmed_tier).to_string()),
                encode_visible_batch_members_value(self.batch_id(), visible_members),
            ),
            Self::AcceptedTransaction {
                confirmed_tier,
                visible_members,
                ..
            } => (
                "accepted_transaction",
                Value::Null,
                Value::Null,
                Value::Text(durability_tier_to_str(*confirmed_tier).to_string()),
                encode_visible_batch_members_value(self.batch_id(), visible_members),
            ),
        };

        encode_row(
            &batch_settlement_storage_descriptor(),
            &[
                Value::Text(kind.to_string()),
                Value::BatchId(*self.batch_id().as_bytes()),
                code,
                reason,
                confirmed_tier,
                visible_members,
            ],
        )
        .map_err(|err| format!("encode batch settlement row: {err}"))
    }

    pub fn decode_storage_row(bytes: &[u8]) -> Result<Self, String> {
        let values = decode_row(&batch_settlement_storage_descriptor(), bytes)
            .map_err(|err| format!("decode batch settlement row: {err}"))?;
        let [
            kind,
            batch_id,
            code,
            reason,
            confirmed_tier,
            visible_members,
        ] = values.as_slice()
        else {
            return Err("unexpected batch settlement shape".to_string());
        };

        let kind = match kind {
            Value::Text(value) => value.as_str(),
            other => {
                return Err(format!(
                    "expected batch settlement kind text, got {other:?}"
                ));
            }
        };
        let batch_id = decode_batch_id_value(batch_id, "expected batch id to be 16 bytes")?;

        match kind {
            "missing" => Ok(Self::Missing { batch_id }),
            "rejected" => Ok(Self::Rejected {
                batch_id,
                code: decode_nullable_text(code, "rejected code")?
                    .ok_or_else(|| "rejected settlement missing code".to_string())?,
                reason: decode_nullable_text(reason, "rejected reason")?
                    .ok_or_else(|| "rejected settlement missing reason".to_string())?,
            }),
            "durable_direct" => Ok(Self::DurableDirect {
                batch_id,
                confirmed_tier: decode_nullable_durability_tier(confirmed_tier)?.ok_or_else(
                    || "durable direct settlement missing confirmed tier".to_string(),
                )?,
                visible_members: decode_visible_batch_members_value(batch_id, visible_members)?,
            }),
            "accepted_transaction" => Ok(Self::AcceptedTransaction {
                batch_id,
                confirmed_tier: decode_nullable_durability_tier(confirmed_tier)?.ok_or_else(
                    || "accepted transaction settlement missing confirmed tier".to_string(),
                )?,
                visible_members: decode_visible_batch_members_value(batch_id, visible_members)?,
            }),
            other => Err(format!("unknown batch settlement kind '{other}'")),
        }
    }
}

fn merged_visible_batch_members(
    current: &[VisibleBatchMember],
    incoming: &[VisibleBatchMember],
) -> Vec<VisibleBatchMember> {
    let mut merged = current.to_vec();
    for member in incoming {
        if !merged.iter().any(|existing| existing == member) {
            merged.push(member.clone());
        }
    }
    merged.sort_by(|left, right| {
        left.object_id
            .uuid()
            .as_bytes()
            .cmp(right.object_id.uuid().as_bytes())
            .then_with(|| left.branch_name.as_str().cmp(right.branch_name.as_str()))
            .then_with(|| left.batch_id.as_bytes().cmp(right.batch_id.as_bytes()))
    });
    merged
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalBatchRecord {
    pub batch_id: BatchId,
    pub mode: BatchMode,
    pub sealed: bool,
    pub members: Vec<LocalBatchMember>,
    pub sealed_submission: Option<SealedBatchSubmission>,
    pub latest_settlement: Option<BatchSettlement>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedBatchSubmission {
    pub batch_id: BatchId,
    pub target_branch_name: BranchName,
    pub batch_digest: Digest32,
    pub members: Vec<SealedBatchMember>,
    pub captured_frontier: Vec<CapturedFrontierMember>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SealedBatchMember {
    pub object_id: ObjectId,
    pub row_digest: Digest32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedFrontierMember {
    pub object_id: ObjectId,
    pub branch_name: BranchName,
    pub batch_id: BatchId,
}

impl LocalBatchRecord {
    pub fn new(
        batch_id: BatchId,
        mode: BatchMode,
        sealed: bool,
        latest_settlement: Option<BatchSettlement>,
    ) -> Self {
        Self {
            batch_id,
            mode,
            sealed,
            members: Vec::new(),
            sealed_submission: None,
            latest_settlement,
        }
    }

    pub fn upsert_member(&mut self, member: LocalBatchMember) {
        match self.members.binary_search_by(|existing| {
            Self::compare_member_identity(existing, &member)
                .then_with(|| Self::compare_member_version(existing, &member))
        }) {
            Ok(index) => {
                self.members[index] = member;
            }
            Err(index) => {
                if index > 0
                    && Self::compare_member_identity(&self.members[index - 1], &member)
                        == Ordering::Equal
                {
                    self.members[index - 1] = member;
                } else if index < self.members.len()
                    && Self::compare_member_identity(&self.members[index], &member)
                        == Ordering::Equal
                {
                    self.members[index] = member;
                } else {
                    self.members.insert(index, member);
                }
            }
        }
    }

    fn compare_member_identity(left: &LocalBatchMember, right: &LocalBatchMember) -> Ordering {
        left.object_id
            .uuid()
            .as_bytes()
            .cmp(right.object_id.uuid().as_bytes())
            .then_with(|| left.table_name.cmp(&right.table_name))
            .then_with(|| left.branch_name.as_str().cmp(right.branch_name.as_str()))
    }

    fn compare_member_version(left: &LocalBatchMember, right: &LocalBatchMember) -> Ordering {
        left.schema_hash
            .as_bytes()
            .cmp(right.schema_hash.as_bytes())
            .then_with(|| left.row_digest.0.cmp(&right.row_digest.0))
    }

    pub fn mark_sealed(&mut self, submission: SealedBatchSubmission) {
        self.sealed = true;
        self.sealed_submission = Some(submission);
    }

    pub fn apply_settlement(&mut self, settlement: BatchSettlement) {
        assert_eq!(
            settlement.batch_id(),
            self.batch_id,
            "settlement batch id should match record batch id"
        );

        match (&self.latest_settlement, settlement) {
            (Some(BatchSettlement::Rejected { .. }), _) => {}
            (_, rejected @ BatchSettlement::Rejected { .. }) => {
                self.latest_settlement = Some(rejected);
            }
            (
                Some(BatchSettlement::DurableDirect {
                    confirmed_tier: current_tier,
                    visible_members: current_members,
                    ..
                }),
                BatchSettlement::DurableDirect {
                    batch_id,
                    confirmed_tier,
                    visible_members,
                },
            ) => {
                if confirmed_tier >= *current_tier {
                    self.latest_settlement = Some(BatchSettlement::DurableDirect {
                        batch_id,
                        confirmed_tier,
                        visible_members: merged_visible_batch_members(
                            current_members,
                            &visible_members,
                        ),
                    });
                }
            }
            (
                Some(BatchSettlement::AcceptedTransaction {
                    confirmed_tier: current_tier,
                    visible_members: current_members,
                    ..
                }),
                BatchSettlement::AcceptedTransaction {
                    batch_id,
                    confirmed_tier,
                    visible_members,
                },
            ) => {
                if confirmed_tier >= *current_tier {
                    self.latest_settlement = Some(BatchSettlement::AcceptedTransaction {
                        batch_id,
                        confirmed_tier,
                        visible_members: merged_visible_batch_members(
                            current_members,
                            &visible_members,
                        ),
                    });
                }
            }
            (
                Some(BatchSettlement::DurableDirect { .. })
                | Some(BatchSettlement::AcceptedTransaction { .. }),
                BatchSettlement::Missing { .. },
            ) => {}
            (_, settlement) => {
                self.latest_settlement = Some(settlement);
            }
        }
    }

    pub fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        let latest_settlement = self
            .latest_settlement
            .as_ref()
            .map(BatchSettlement::encode_storage_row)
            .transpose()
            .map_err(|err| format!("encode settlement: {err}"))?;
        let sealed_submission = self
            .sealed_submission
            .as_ref()
            .map(SealedBatchSubmission::encode_storage_row)
            .transpose()
            .map_err(|err| format!("encode sealed submission: {err}"))?;
        let values = vec![
            Value::BatchId(*self.batch_id.as_bytes()),
            Value::Text(self.mode.as_str().to_string()),
            Value::Boolean(self.sealed),
            Value::Array(
                self.members
                    .iter()
                    .map(|member| Value::Row {
                        id: None,
                        values: vec![
                            Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                            Value::Text(member.table_name.clone()),
                            Value::Text(member.branch_name.to_string()),
                            Value::Bytea(member.schema_hash.as_bytes().to_vec()),
                            Value::Bytea(member.row_digest.0.to_vec()),
                        ],
                    })
                    .collect(),
            ),
            sealed_submission.map(Value::Bytea).unwrap_or(Value::Null),
            latest_settlement.map(Value::Bytea).unwrap_or(Value::Null),
        ];
        encode_row(&storage_descriptor(), &values).map_err(|err| format!("encode batch row: {err}"))
    }

    pub fn decode_storage_row(bytes: &[u8]) -> Result<Self, String> {
        let values = decode_row(&storage_descriptor(), bytes)
            .map_err(|err| format!("decode batch row: {err}"))?;
        let [
            batch_id,
            mode,
            sealed,
            members,
            sealed_submission,
            latest_settlement,
        ] = values.as_slice()
        else {
            return Err("unexpected local batch record shape".to_string());
        };

        let batch_id = decode_batch_id_value(batch_id, "decode batch id")?;
        let mode = match mode {
            Value::Text(raw) => BatchMode::parse(raw)?,
            other => return Err(format!("expected batch mode text, got {other:?}")),
        };
        let sealed = match sealed {
            Value::Boolean(value) => *value,
            other => return Err(format!("expected sealed boolean, got {other:?}")),
        };
        let members = match members {
            Value::Array(values) => values
                .iter()
                .map(|value| match value {
                    Value::Row { values, .. } => {
                        let [object_id, table_name, branch_name, schema_hash, row_digest] =
                            values.as_slice()
                        else {
                            return Err(
                                "expected local batch member row to have five values".to_string(),
                            );
                        };
                        let object_id = match object_id {
                            Value::Bytea(bytes) => {
                                let uuid = uuid::Uuid::from_slice(bytes).map_err(|err| {
                                    format!(
                                        "decode local batch member object id: expected uuid bytes: {err}"
                                    )
                                })?;
                                ObjectId::from_uuid(uuid)
                            }
                            other => {
                                return Err(format!(
                                    "expected local batch member object id bytes, got {other:?}"
                                ));
                            }
                        };
                        let table_name = match table_name {
                            Value::Text(raw) => raw.clone(),
                            other => {
                                return Err(format!(
                                    "expected local batch member table name text, got {other:?}"
                                ));
                            }
                        };
                        let branch_name = match branch_name {
                            Value::Text(raw) => BranchName::new(raw),
                            other => {
                                return Err(format!(
                                    "expected local batch member branch name text, got {other:?}"
                                ));
                            }
                        };
                        let schema_hash = match schema_hash {
                            Value::Bytea(bytes) => {
                                let bytes: [u8; 32] = bytes.as_slice().try_into().map_err(|_| {
                                    format!(
                                        "expected local batch member schema hash to be 32 bytes, got {}",
                                        bytes.len()
                                    )
                                })?;
                                SchemaHash::from_bytes(bytes)
                            }
                            other => {
                                return Err(format!(
                                    "expected local batch member schema hash bytes, got {other:?}"
                                ));
                            }
                        };
                        let row_digest = match row_digest {
                            Value::Bytea(bytes) => Digest32(bytes.as_slice().try_into().map_err(
                                |_| {
                                    format!(
                                        "expected local batch member row digest to be 32 bytes, got {}",
                                        bytes.len()
                                    )
                                },
                            )?),
                            other => {
                                return Err(format!(
                                    "expected local batch member row digest bytes, got {other:?}"
                                ));
                            }
                        };
                        Ok(LocalBatchMember {
                            object_id,
                            table_name,
                            branch_name,
                            schema_hash,
                            row_digest,
                        })
                    }
                    other => Err(format!("expected local batch member row, got {other:?}")),
                })
                .collect::<Result<Vec<_>, String>>()?,
            other => return Err(format!("expected local batch members array, got {other:?}")),
        };
        let sealed_submission = match sealed_submission {
            Value::Null => None,
            Value::Bytea(bytes) => Some(
                SealedBatchSubmission::decode_storage_row(bytes)
                    .map_err(|err| format!("decode sealed batch submission: {err}"))?,
            ),
            other => {
                return Err(format!(
                    "expected sealed submission bytes or null, got {other:?}"
                ));
            }
        };
        let latest_settlement = match latest_settlement {
            Value::Null => None,
            Value::Bytea(bytes) => Some(
                BatchSettlement::decode_storage_row(bytes)
                    .map_err(|err| format!("decode latest settlement: {err}"))?,
            ),
            other => {
                return Err(format!(
                    "expected latest settlement bytes or null, got {other:?}"
                ));
            }
        };

        Ok(Self {
            batch_id,
            mode,
            sealed,
            members,
            sealed_submission,
            latest_settlement,
        })
    }
}

impl SealedBatchSubmission {
    pub fn compute_batch_digest(members: &[SealedBatchMember]) -> Digest32 {
        let mut hasher = Hasher::new();
        hasher.update(b"sealed-batch-manifest-v1");
        hasher.update(&(members.len() as u64).to_le_bytes());
        for member in members {
            hasher.update(member.object_id.uuid().as_bytes());
            hasher.update(&member.row_digest.0);
        }
        Digest32(*hasher.finalize().as_bytes())
    }

    pub fn new(
        batch_id: BatchId,
        target_branch_name: BranchName,
        mut members: Vec<SealedBatchMember>,
        mut captured_frontier: Vec<CapturedFrontierMember>,
    ) -> Self {
        members.sort_by(|left, right| {
            left.object_id
                .uuid()
                .as_bytes()
                .cmp(right.object_id.uuid().as_bytes())
                .then_with(|| left.row_digest.0.cmp(&right.row_digest.0))
        });
        members.dedup();
        let batch_digest = Self::compute_batch_digest(&members);
        captured_frontier.sort_by(|left, right| {
            left.object_id
                .uuid()
                .as_bytes()
                .cmp(right.object_id.uuid().as_bytes())
                .then_with(|| left.branch_name.as_str().cmp(right.branch_name.as_str()))
                .then_with(|| left.batch_id.0.cmp(&right.batch_id.0))
        });
        captured_frontier.dedup();
        Self {
            batch_id,
            target_branch_name,
            batch_digest,
            members,
            captured_frontier,
        }
    }

    pub fn encode_storage_row(&self) -> Result<Vec<u8>, String> {
        let values = vec![
            Value::Bytea(self.batch_id.as_bytes().to_vec()),
            Value::Text(self.target_branch_name.as_str().to_string()),
            Value::Bytea(self.batch_digest.0.to_vec()),
            Value::Array(
                self.members
                    .iter()
                    .map(|member| Value::Row {
                        id: None,
                        values: vec![
                            Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                            Value::Bytea(member.row_digest.0.to_vec()),
                        ],
                    })
                    .collect(),
            ),
            Value::Array(
                self.captured_frontier
                    .iter()
                    .map(|member| Value::Row {
                        id: None,
                        values: vec![
                            Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                            Value::Text(member.branch_name.as_str().to_string()),
                            Value::Bytea(member.batch_id.as_bytes().to_vec()),
                        ],
                    })
                    .collect(),
            ),
        ];
        encode_row(&sealed_batch_submission_storage_descriptor(), &values)
            .map_err(|err| format!("encode sealed batch submission row: {err}"))
    }

    pub fn decode_storage_row(bytes: &[u8]) -> Result<Self, String> {
        let values = decode_row(&sealed_batch_submission_storage_descriptor(), bytes)
            .map_err(|err| format!("decode sealed batch submission row: {err}"))?;
        let [
            batch_id,
            target_branch_name,
            batch_digest,
            members,
            captured_frontier,
        ] = values.as_slice()
        else {
            return Err("unexpected sealed batch submission shape".to_string());
        };

        let batch_id = decode_batch_id_value(batch_id, "decode sealed batch submission batch id")?;
        let target_branch_name = match target_branch_name {
            Value::Text(raw) => BranchName::new(raw),
            other => return Err(format!("expected target branch text, got {other:?}")),
        };
        let batch_digest = match batch_digest {
            Value::Bytea(bytes) => Digest32(bytes.as_slice().try_into().map_err(|_| {
                format!(
                    "expected sealed batch submission batch digest to be 32 bytes, got {}",
                    bytes.len()
                )
            })?),
            other => return Err(format!("expected batch digest bytes, got {other:?}")),
        };

        let members = match members {
            Value::Array(elements) => elements
                .iter()
                .map(|element| match element {
                    Value::Row { values, .. } => {
                        let [object_id, row_digest] = values.as_slice() else {
                            return Err("expected sealed batch member row to have two values".to_string());
                        };
                        let object_id = match object_id {
                            Value::Bytea(bytes) => uuid::Uuid::from_slice(bytes)
                                .map(ObjectId::from_uuid)
                                .map_err(|err| {
                                    format!("decode sealed batch object id uuid: {err}")
                                })?,
                            other => {
                                return Err(format!(
                                    "expected sealed batch member object id bytes, got {other:?}"
                                ));
                            }
                        };
                        let row_digest = match row_digest {
                            Value::Bytea(bytes) => Digest32(bytes.as_slice().try_into().map_err(
                                |_| {
                                    format!(
                                        "expected sealed batch member row digest to be 32 bytes, got {}",
                                        bytes.len()
                                    )
                                },
                            )?),
                            other => {
                                return Err(format!(
                                    "expected sealed batch member row digest bytes, got {other:?}"
                                ));
                            }
                        };
                        Ok(SealedBatchMember {
                            object_id,
                            row_digest,
                        })
                    }
                    other => Err(format!("expected sealed batch member row, got {other:?}")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            other => return Err(format!("expected sealed batch member array, got {other:?}")),
        };

        let captured_frontier = match captured_frontier {
            Value::Array(elements) => elements
                .iter()
                .map(|element| match element {
                    Value::Row { values, .. } => {
                        let [object_id, branch_name, batch_id] = values.as_slice() else {
                            return Err(
                                "expected captured frontier row to have three values".to_string()
                            );
                        };
                        let object_id = match object_id {
                            Value::Bytea(bytes) => uuid::Uuid::from_slice(bytes)
                                .map(ObjectId::from_uuid)
                                .map_err(|err| {
                                    format!("decode captured frontier object id uuid: {err}")
                                })?,
                            other => {
                                return Err(format!(
                                    "expected captured frontier object id bytes, got {other:?}"
                                ));
                            }
                        };
                        let branch_name = match branch_name {
                            Value::Text(raw) => BranchName::new(raw),
                            other => {
                                return Err(format!(
                                    "expected captured frontier branch text, got {other:?}"
                                ));
                            }
                        };
                        let batch_id = decode_batch_id_value(
                            batch_id,
                            "expected captured frontier batch id to be 16 bytes",
                        )?;
                        Ok(CapturedFrontierMember {
                            object_id,
                            branch_name,
                            batch_id,
                        })
                    }
                    other => Err(format!("expected captured frontier row, got {other:?}")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            other => return Err(format!("expected captured frontier array, got {other:?}")),
        };

        let submission = Self::new(batch_id, target_branch_name, members, captured_frontier);
        if submission.batch_digest != batch_digest {
            return Err(format!(
                "sealed batch submission batch digest mismatch: expected {batch_digest:?}, computed {:?}",
                submission.batch_digest
            ));
        }
        Ok(submission)
    }
}

fn durability_tier_to_str(tier: DurabilityTier) -> &'static str {
    match tier {
        DurabilityTier::Local => "local",
        DurabilityTier::EdgeServer => "edge",
        DurabilityTier::GlobalServer => "global",
    }
}

fn durability_tier_from_str(raw: &str) -> Result<DurabilityTier, String> {
    match raw {
        "local" => Ok(DurabilityTier::Local),
        "edge" => Ok(DurabilityTier::EdgeServer),
        "global" => Ok(DurabilityTier::GlobalServer),
        other => Err(format!("unknown durability tier '{other}'")),
    }
}

fn decode_batch_id_value(value: &Value, context: &str) -> Result<BatchId, String> {
    match value {
        Value::BatchId(bytes) => Ok(BatchId(*bytes)),
        Value::Bytea(bytes) => {
            let bytes: [u8; 16] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| format!("{context}: expected 16 bytes, got {}", bytes.len()))?;
            Ok(BatchId(bytes))
        }
        other => Err(format!("expected batch id bytes, got {other:?}")),
    }
}

fn encode_visible_batch_members_value(
    batch_id: BatchId,
    visible_members: &[VisibleBatchMember],
) -> Value {
    Value::Array(
        visible_members
            .iter()
            .map(|member| {
                assert_eq!(
                    member.batch_id, batch_id,
                    "visible batch member batch ids should match enclosing settlement batch id"
                );
                Value::Row {
                    id: None,
                    values: vec![
                        Value::Bytea(member.object_id.uuid().as_bytes().to_vec()),
                        Value::Text(member.branch_name.as_str().to_string()),
                    ],
                }
            })
            .collect(),
    )
}

fn decode_visible_batch_members_value(
    batch_id: BatchId,
    value: &Value,
) -> Result<Vec<VisibleBatchMember>, String> {
    match value {
        Value::Array(elements) => elements
            .iter()
            .map(|element| match element {
                Value::Row { values, .. } => {
                    let [object_id, branch_name] = values.as_slice() else {
                        return Err(
                            "expected visible batch member row to have two values".to_string()
                        );
                    };
                    let object_id = match object_id {
                        Value::Bytea(bytes) => uuid::Uuid::from_slice(bytes)
                            .map(ObjectId::from_uuid)
                            .map_err(|err| {
                                format!("decode visible batch member object id uuid: {err}")
                            })?,
                        other => {
                            return Err(format!(
                                "expected visible batch member object id bytes, got {other:?}"
                            ));
                        }
                    };
                    let branch_name = match branch_name {
                        Value::Text(raw) => BranchName::new(raw),
                        other => {
                            return Err(format!(
                                "expected visible batch member branch text, got {other:?}"
                            ));
                        }
                    };
                    Ok(VisibleBatchMember {
                        object_id,
                        branch_name,
                        batch_id,
                    })
                }
                other => Err(format!("expected visible batch member row, got {other:?}")),
            })
            .collect(),
        other => Err(format!(
            "expected visible batch members array, got {other:?}"
        )),
    }
}

fn decode_nullable_text(value: &Value, label: &str) -> Result<Option<String>, String> {
    match value {
        Value::Null => Ok(None),
        Value::Text(value) => Ok(Some(value.clone())),
        other => Err(format!("expected {label} text or null, got {other:?}")),
    }
}

fn decode_nullable_durability_tier(value: &Value) -> Result<Option<DurabilityTier>, String> {
    match value {
        Value::Null => Ok(None),
        Value::Text(raw) => durability_tier_from_str(raw).map(Some),
        other => Err(format!(
            "expected confirmed tier text or null, got {other:?}"
        )),
    }
}

fn storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("batch_id", ColumnType::BatchId),
        ColumnDescriptor::new(
            "mode",
            ColumnType::Enum {
                variants: vec!["direct".to_string(), "transactional".to_string()],
            },
        ),
        ColumnDescriptor::new("sealed", ColumnType::Boolean),
        ColumnDescriptor::new(
            "members",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("table_name", ColumnType::Text),
                        ColumnDescriptor::new("branch_name", ColumnType::Text),
                        ColumnDescriptor::new("schema_hash", ColumnType::Bytea),
                        ColumnDescriptor::new("row_digest", ColumnType::Bytea),
                    ])),
                }),
            },
        ),
        ColumnDescriptor::new("sealed_submission", ColumnType::Bytea).nullable(),
        ColumnDescriptor::new("latest_settlement", ColumnType::Bytea).nullable(),
    ])
}

fn sealed_batch_submission_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new("batch_id", ColumnType::BatchId),
        ColumnDescriptor::new("target_branch_name", ColumnType::Text),
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
                        ColumnDescriptor::new("branch_name", ColumnType::Text),
                        ColumnDescriptor::new("batch_id", ColumnType::BatchId),
                    ])),
                }),
            },
        ),
    ])
}

fn batch_settlement_storage_descriptor() -> RowDescriptor {
    RowDescriptor::new(vec![
        ColumnDescriptor::new(
            "kind",
            ColumnType::Enum {
                variants: vec![
                    "missing".to_string(),
                    "rejected".to_string(),
                    "durable_direct".to_string(),
                    "accepted_transaction".to_string(),
                ],
            },
        ),
        ColumnDescriptor::new("batch_id", ColumnType::BatchId),
        ColumnDescriptor::new("code", ColumnType::Text).nullable(),
        ColumnDescriptor::new("reason", ColumnType::Text).nullable(),
        ColumnDescriptor::new(
            "confirmed_tier",
            ColumnType::Enum {
                variants: vec![
                    "local".to_string(),
                    "edge".to_string(),
                    "global".to_string(),
                ],
            },
        )
        .nullable(),
        ColumnDescriptor::new(
            "visible_members",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(RowDescriptor::new(vec![
                        ColumnDescriptor::new("object_id", ColumnType::Bytea),
                        ColumnDescriptor::new("branch_name", ColumnType::Text),
                    ])),
                }),
            },
        ),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_batch_record_storage_row_roundtrips() {
        let batch_id = BatchId::new();
        let mut record = LocalBatchRecord::new(
            batch_id,
            BatchMode::Direct,
            true,
            Some(BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::Local,
                visible_members: vec![VisibleBatchMember {
                    object_id: ObjectId::from_uuid(uuid::Uuid::from_u128(7)),
                    branch_name: BranchName::new("main"),
                    batch_id,
                }],
            }),
        );
        record.upsert_member(LocalBatchMember {
            object_id: ObjectId::from_uuid(uuid::Uuid::from_u128(8)),
            table_name: "users".to_string(),
            branch_name: BranchName::new("main"),
            schema_hash: SchemaHash::from_bytes([4; 32]),
            row_digest: Digest32([3; 32]),
        });

        let bytes = record.encode_storage_row().expect("encode record");
        let decoded = LocalBatchRecord::decode_storage_row(&bytes).expect("decode record");

        assert_eq!(decoded, record);
    }

    #[test]
    fn local_batch_record_upsert_member_keeps_members_sorted_and_replaces_row() {
        let batch_id = BatchId::new();
        let mut record = LocalBatchRecord::new(batch_id, BatchMode::Direct, false, None);
        let alice_id = ObjectId::from_uuid(uuid::Uuid::from_u128(1));
        let bob_id = ObjectId::from_uuid(uuid::Uuid::from_u128(2));

        record.upsert_member(LocalBatchMember {
            object_id: bob_id,
            table_name: "tasks".to_string(),
            branch_name: BranchName::new("main"),
            schema_hash: SchemaHash::from_bytes([2; 32]),
            row_digest: Digest32([2; 32]),
        });
        record.upsert_member(LocalBatchMember {
            object_id: alice_id,
            table_name: "tasks".to_string(),
            branch_name: BranchName::new("main"),
            schema_hash: SchemaHash::from_bytes([1; 32]),
            row_digest: Digest32([1; 32]),
        });
        record.upsert_member(LocalBatchMember {
            object_id: bob_id,
            table_name: "tasks".to_string(),
            branch_name: BranchName::new("main"),
            schema_hash: SchemaHash::from_bytes([3; 32]),
            row_digest: Digest32([3; 32]),
        });

        assert_eq!(
            record
                .members
                .iter()
                .map(|member| (member.object_id, member.row_digest))
                .collect::<Vec<_>>(),
            vec![(alice_id, Digest32([1; 32])), (bob_id, Digest32([3; 32]))]
        );
    }

    #[test]
    fn local_batch_record_keeps_highest_durable_direct_tier() {
        let batch_id = BatchId::new();
        let mut record = LocalBatchRecord::new(
            batch_id,
            BatchMode::Direct,
            true,
            Some(BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::EdgeServer,
                visible_members: Vec::new(),
            }),
        );

        record.apply_settlement(BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::Local,
            visible_members: Vec::new(),
        });

        assert_eq!(
            record.latest_settlement,
            Some(BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::EdgeServer,
                visible_members: Vec::new(),
            })
        );
    }

    #[test]
    fn local_batch_record_merges_visible_members_for_shared_direct_batches() {
        let batch_id = BatchId::new();
        let first_row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(1));
        let second_row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(2));
        let mut record = LocalBatchRecord::new(
            batch_id,
            BatchMode::Direct,
            true,
            Some(BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::Local,
                visible_members: vec![VisibleBatchMember {
                    object_id: first_row_id,
                    branch_name: BranchName::new("main"),
                    batch_id,
                }],
            }),
        );

        record.apply_settlement(BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::Local,
            visible_members: vec![VisibleBatchMember {
                object_id: second_row_id,
                branch_name: BranchName::new("main"),
                batch_id,
            }],
        });

        assert_eq!(
            record.latest_settlement,
            Some(BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::Local,
                visible_members: vec![
                    VisibleBatchMember {
                        object_id: first_row_id,
                        branch_name: BranchName::new("main"),
                        batch_id,
                    },
                    VisibleBatchMember {
                        object_id: second_row_id,
                        branch_name: BranchName::new("main"),
                        batch_id,
                    },
                ],
            })
        );
    }

    #[test]
    fn local_batch_record_storage_row_roundtrips_with_sealed_submission() {
        let batch_id = BatchId::new();
        let mut record = LocalBatchRecord::new(batch_id, BatchMode::Transactional, false, None);
        record.mark_sealed(SealedBatchSubmission::new(
            batch_id,
            BranchName::new("dev-aaaaaaaaaaaa-main"),
            vec![SealedBatchMember {
                object_id: ObjectId::from_uuid(uuid::Uuid::from_u128(42)),
                row_digest: Digest32([4; 32]),
            }],
            vec![CapturedFrontierMember {
                object_id: ObjectId::from_uuid(uuid::Uuid::from_u128(7)),
                branch_name: BranchName::new("dev-bbbbbbbbbbbb-main"),
                batch_id: BatchId([8; 16]),
            }],
        ));

        let bytes = record.encode_storage_row().expect("encode record");
        let decoded = LocalBatchRecord::decode_storage_row(&bytes).expect("decode record");

        assert_eq!(decoded, record);
    }

    #[test]
    fn sealed_batch_submission_storage_row_roundtrips() {
        let batch_id = BatchId::new();
        let object_id = ObjectId::new();
        let row_digest = Digest32([7; 32]);
        let submission = SealedBatchSubmission::new(
            batch_id,
            BranchName::new("main"),
            vec![
                SealedBatchMember {
                    object_id,
                    row_digest,
                },
                SealedBatchMember {
                    object_id,
                    row_digest,
                },
            ],
            vec![CapturedFrontierMember {
                object_id,
                branch_name: BranchName::new("dev-aaaaaaaaaaaa-main"),
                batch_id: BatchId([9; 16]),
            }],
        );

        let bytes = submission
            .encode_storage_row()
            .expect("encode sealed batch submission");
        let decoded = SealedBatchSubmission::decode_storage_row(&bytes)
            .expect("decode sealed batch submission");

        assert_eq!(
            decoded,
            SealedBatchSubmission {
                batch_id,
                target_branch_name: BranchName::new("main"),
                batch_digest: submission.batch_digest,
                members: vec![SealedBatchMember {
                    object_id,
                    row_digest,
                }],
                captured_frontier: vec![CapturedFrontierMember {
                    object_id,
                    branch_name: BranchName::new("dev-aaaaaaaaaaaa-main"),
                    batch_id: BatchId([9; 16]),
                }],
            }
        );
    }

    #[test]
    fn sealed_batch_submission_batch_digest_tracks_current_member_manifest() {
        let object_id = ObjectId::from_uuid(uuid::Uuid::from_u128(11));
        let first = SealedBatchSubmission::new(
            BatchId::new(),
            BranchName::new("main"),
            vec![SealedBatchMember {
                object_id,
                row_digest: Digest32([1; 32]),
            }],
            Vec::new(),
        );
        let second = SealedBatchSubmission::new(
            BatchId::new(),
            BranchName::new("main"),
            vec![SealedBatchMember {
                object_id,
                row_digest: Digest32([2; 32]),
            }],
            Vec::new(),
        );

        assert_ne!(first.batch_digest, second.batch_digest);
    }

    #[test]
    fn batch_settlement_storage_row_uses_structured_row_format() {
        let batch_id = BatchId::new();
        let settlement = BatchSettlement::Rejected {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "alice cannot write here".to_string(),
        };

        let bytes = settlement.encode_storage_row().expect("encode settlement");
        let values = decode_row(&batch_settlement_storage_descriptor(), &bytes)
            .expect("decode settlement as row-format");

        assert_eq!(values[0], Value::Text("rejected".to_string()));
        assert_eq!(values[1], Value::BatchId(*batch_id.as_bytes()));
    }

    #[test]
    fn batch_settlement_storage_row_compacts_enums_and_member_batch_id() {
        let batch_id = BatchId::new();
        let settlement = BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::EdgeServer,
            visible_members: vec![VisibleBatchMember {
                object_id: ObjectId::from_uuid(uuid::Uuid::from_u128(9)),
                branch_name: BranchName::new("main"),
                batch_id,
            }],
        };

        let bytes = settlement.encode_storage_row().expect("encode settlement");
        let descriptor = batch_settlement_storage_descriptor();
        let layout = crate::row_format::compiled_row_layout(&descriptor);

        let kind =
            crate::row_format::column_bytes_with_layout(&descriptor, layout.as_ref(), &bytes, 0)
                .expect("read kind bytes")
                .expect("kind should be present");
        let tier =
            crate::row_format::column_bytes_with_layout(&descriptor, layout.as_ref(), &bytes, 4)
                .expect("read tier bytes")
                .expect("tier should be present");

        assert_eq!(kind.len(), 1);
        assert_eq!(tier.len(), 1);

        let values = decode_row(&descriptor, &bytes).expect("decode settlement");
        let Value::Array(members) = &values[5] else {
            panic!("expected visible member array");
        };
        let Value::Row { values, .. } = &members[0] else {
            panic!("expected visible member row");
        };
        assert_eq!(values.len(), 2);
    }
}
