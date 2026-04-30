//! Shared helpers used by native bindings.
//!
//! These utilities keep wrapper crates thin by centralizing JSON parsing,
//! schema-alignment, and subscription payload shaping in the core crate.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

use crate::batch_fate::{BatchMode, BatchSettlement, LocalBatchRecord, VisibleBatchMember};
use crate::object::ObjectId;
use crate::query_manager::manager::LocalUpdates;
use crate::query_manager::parse_query_json;
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{
    PermissionPreflightDecision, RowDescriptor, Schema, TableName, Value,
};
use crate::row_format::decode_row;
use crate::row_histories::BatchId;
use crate::runtime_core::{ReadDurabilityOptions, SubscriptionDelta};
use crate::sync_manager::{DurabilityTier, QueryPropagation};

#[derive(Debug, Clone, Deserialize, Default)]
struct QueryExecutionOptionsWire {
    propagation: Option<String>,
    local_updates: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSchemaInput {
    pub schema: Schema,
    pub loaded_policy_bundle: bool,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeSchemaEnvelopeWire {
    #[serde(rename = "__jazzRuntimeSchema")]
    version: u8,
    schema: Schema,
    #[serde(default)]
    loaded_policy_bundle: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RuntimeSchemaWire {
    Envelope(RuntimeSchemaEnvelopeWire),
    Schema(Schema),
}

pub fn query_rows_can_be_schema_aligned(query: &Query) -> bool {
    query.joins.is_empty()
        && query.array_subqueries.is_empty()
        && query.recursive.is_none()
        && query.select_columns.is_none()
        && query.result_element_index.is_none()
}

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

pub fn align_values_to_declared_schema(
    declared_schema: &Schema,
    table: &TableName,
    source_descriptor: &RowDescriptor,
    values: Vec<Value>,
) -> Vec<Value> {
    let Some(declared_table) = declared_schema.get(table) else {
        return values;
    };

    reorder_values_by_column_name(source_descriptor, &declared_table.columns, &values)
        .unwrap_or(values)
}

pub fn align_row_values_to_declared_schema(
    declared_schema: &Schema,
    runtime_schema: &Schema,
    table: &TableName,
    values: Vec<Value>,
) -> Vec<Value> {
    let Some(runtime_table) = runtime_schema.get(table) else {
        return values;
    };

    align_values_to_declared_schema(declared_schema, table, &runtime_table.columns, values)
}

pub fn align_query_rows_to_declared_schema(
    declared_schema: &Schema,
    runtime_schema: &Schema,
    query: &Query,
    rows: Vec<(ObjectId, Vec<Value>)>,
) -> Vec<(ObjectId, Vec<Value>)> {
    if !query_rows_can_be_schema_aligned(query) {
        return rows;
    }

    let Some(declared_table) = declared_schema.get(&query.table) else {
        return rows;
    };
    let Some(runtime_table) = runtime_schema.get(&query.table) else {
        return rows;
    };

    rows.into_iter()
        .map(|(id, values)| {
            let values = reorder_values_by_column_name(
                &runtime_table.columns,
                &declared_table.columns,
                &values,
            )
            .unwrap_or(values);
            (id, values)
        })
        .collect()
}

pub fn parse_query_input(query_json: &str) -> Result<Query, String> {
    parse_query_json(query_json)
}

pub fn parse_runtime_schema_input(schema_json: &str) -> Result<RuntimeSchemaInput, String> {
    match serde_json::from_str::<RuntimeSchemaWire>(schema_json).map_err(|err| err.to_string())? {
        RuntimeSchemaWire::Envelope(envelope) => {
            if envelope.version != 1 {
                return Err(format!(
                    "unsupported runtime schema envelope version {}",
                    envelope.version
                ));
            }
            Ok(RuntimeSchemaInput {
                schema: envelope.schema,
                loaded_policy_bundle: envelope.loaded_policy_bundle,
            })
        }
        RuntimeSchemaWire::Schema(schema) => Ok(RuntimeSchemaInput {
            schema,
            loaded_policy_bundle: false,
        }),
    }
}

pub fn parse_session_input(session_json: Option<&str>) -> Result<Option<Session>, String> {
    match session_json {
        Some(json) => serde_json::from_str(json)
            .map(Some)
            .map_err(|err| err.to_string()),
        None => Ok(None),
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WriteContextWire {
    Session(Session),
    Context(WriteContextPayloadWire),
}

#[derive(Debug, Deserialize)]
struct WriteContextPayloadWire {
    #[serde(default)]
    session: Option<Session>,
    #[serde(default)]
    attribution: Option<String>,
    #[serde(default)]
    updated_at: Option<u64>,
    #[serde(default)]
    batch_mode: Option<String>,
    #[serde(default)]
    batch_id: Option<String>,
    #[serde(default)]
    target_branch_name: Option<String>,
}

impl TryFrom<WriteContextPayloadWire> for WriteContext {
    type Error = String;

    fn try_from(value: WriteContextPayloadWire) -> Result<Self, Self::Error> {
        let batch_mode = match value.batch_mode.as_deref() {
            None => None,
            Some("direct") | Some("Direct") => Some(BatchMode::Direct),
            Some("transactional") | Some("Transactional") => Some(BatchMode::Transactional),
            Some(other) => {
                return Err(format!(
                    "Invalid batch mode '{other}'. Must be 'direct' or 'transactional'."
                ));
            }
        };
        let batch_id = value
            .batch_id
            .as_deref()
            .map(parse_batch_id_input)
            .transpose()?;

        Ok(WriteContext {
            session: value.session,
            attribution: value.attribution,
            updated_at: value.updated_at,
            batch_mode,
            batch_id,
            target_branch_name: value.target_branch_name,
        })
    }
}

pub fn parse_write_context_input(
    write_context_json: Option<&str>,
) -> Result<Option<WriteContext>, String> {
    match write_context_json {
        Some(json) => match serde_json::from_str::<WriteContextWire>(json) {
            Ok(WriteContextWire::Session(session)) => Ok(Some(WriteContext::from_session(session))),
            Ok(WriteContextWire::Context(context)) => context.try_into().map(Some),
            Err(err) => Err(err.to_string()),
        },
        None => Ok(None),
    }
}

pub fn parse_durability_tier(tier: &str) -> Result<DurabilityTier, String> {
    match tier {
        "local" => Ok(DurabilityTier::Local),
        "edge" => Ok(DurabilityTier::EdgeServer),
        "global" => Ok(DurabilityTier::GlobalServer),
        _ => Err(format!(
            "Invalid tier '{}'. Must be 'local', 'edge', or 'global'.",
            tier
        )),
    }
}

pub fn parse_batch_id_input(batch_id: &str) -> Result<BatchId, String> {
    batch_id
        .parse()
        .map_err(|err: String| format!("Invalid BatchId: {err}"))
}

pub fn serialize_durability_tier(tier: DurabilityTier) -> &'static str {
    match tier {
        DurabilityTier::Local => "local",
        DurabilityTier::EdgeServer => "edge",
        DurabilityTier::GlobalServer => "global",
    }
}

pub fn serialize_permission_preflight_decision(decision: PermissionPreflightDecision) -> JsonValue {
    match decision {
        PermissionPreflightDecision::Allow => JsonValue::Bool(true),
        PermissionPreflightDecision::Deny => JsonValue::Bool(false),
        PermissionPreflightDecision::Unknown => JsonValue::String("unknown".to_string()),
    }
}

fn serialize_batch_mode(mode: BatchMode) -> &'static str {
    match mode {
        BatchMode::Direct => "direct",
        BatchMode::Transactional => "transactional",
    }
}

fn serialize_visible_batch_member(member: &VisibleBatchMember) -> JsonValue {
    json!({
        "objectId": member.object_id.uuid().to_string(),
        "branchName": member.branch_name.to_string(),
        "batchId": member.batch_id.to_string(),
    })
}

fn serialize_batch_settlement(settlement: &BatchSettlement) -> JsonValue {
    match settlement {
        BatchSettlement::Rejected {
            batch_id,
            code,
            reason,
        } => json!({
            "kind": "rejected",
            "batchId": batch_id.to_string(),
            "code": code,
            "reason": reason,
        }),
        BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier,
            visible_members,
        } => json!({
            "kind": "durableDirect",
            "batchId": batch_id.to_string(),
            "confirmedTier": serialize_durability_tier(*confirmed_tier),
            "visibleMembers": visible_members
                .iter()
                .map(serialize_visible_batch_member)
                .collect::<Vec<_>>(),
        }),
        BatchSettlement::AcceptedTransaction {
            batch_id,
            confirmed_tier,
            visible_members,
        } => json!({
            "kind": "acceptedTransaction",
            "batchId": batch_id.to_string(),
            "confirmedTier": serialize_durability_tier(*confirmed_tier),
            "visibleMembers": visible_members
                .iter()
                .map(serialize_visible_batch_member)
                .collect::<Vec<_>>(),
        }),
        BatchSettlement::Missing { batch_id } => json!({
            "kind": "missing",
            "batchId": batch_id.to_string(),
        }),
    }
}

pub fn serialize_local_batch_record(record: &LocalBatchRecord) -> JsonValue {
    json!({
        "batchId": record.batch_id.to_string(),
        "mode": serialize_batch_mode(record.mode),
        "sealed": record.sealed,
        "latestSettlement": record.latest_settlement.as_ref().map(serialize_batch_settlement),
    })
}

pub fn serialize_local_batch_records(records: &[LocalBatchRecord]) -> JsonValue {
    JsonValue::Array(records.iter().map(serialize_local_batch_record).collect())
}

pub fn default_read_durability_options(tier: Option<DurabilityTier>) -> ReadDurabilityOptions {
    ReadDurabilityOptions {
        tier,
        local_updates: LocalUpdates::Immediate,
    }
}

pub fn parse_read_durability_options(
    tier: Option<&str>,
    options_json: Option<&str>,
) -> Result<(ReadDurabilityOptions, QueryPropagation), String> {
    let parsed_tier = tier.map(parse_durability_tier).transpose()?;
    let Some(raw) = options_json else {
        return Ok((
            default_read_durability_options(parsed_tier),
            QueryPropagation::Full,
        ));
    };

    let options: QueryExecutionOptionsWire =
        serde_json::from_str(raw).map_err(|err| format!("Invalid query options JSON: {}", err))?;

    let propagation = match options.propagation.as_deref() {
        None | Some("full") => Ok(QueryPropagation::Full),
        Some("local-only") => Ok(QueryPropagation::LocalOnly),
        Some(other) => Err(format!(
            "Invalid propagation '{}'. Must be 'full' or 'local-only'.",
            other
        )),
    }?;

    let local_updates = match options.local_updates.as_deref() {
        None | Some("immediate") => Ok(LocalUpdates::Immediate),
        Some("deferred") => Ok(LocalUpdates::Deferred),
        Some(other) => Err(format!(
            "Invalid localUpdates '{}'. Must be 'immediate' or 'deferred'.",
            other
        )),
    }?;

    Ok((
        ReadDurabilityOptions {
            tier: parsed_tier,
            local_updates,
        },
        propagation,
    ))
}

pub fn subscription_delta_to_json(
    delta: &SubscriptionDelta,
    declared_schema: Option<&Schema>,
    table: Option<&TableName>,
) -> serde_json::Value {
    let row_to_json = |row: &crate::query_manager::types::Row,
                       descriptor: &crate::query_manager::types::RowDescriptor|
     -> serde_json::Value {
        let values = decode_row(descriptor, &row.data)
            .map(|vals| vals.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let values = match (declared_schema, table) {
            (Some(schema), Some(table)) => {
                align_values_to_declared_schema(schema, table, descriptor, values)
            }
            _ => values,
        };
        serde_json::json!({
            "id": row.id.uuid().to_string(),
            "values": values,
        })
    };

    let descriptor = &delta.descriptor;
    let delta_obj = delta
        .ordered_delta
        .removed
        .iter()
        .map(|change| {
            serde_json::json!({
                "kind": 1,
                "id": change.id.uuid().to_string(),
                "index": change.index
            })
        })
        .chain(delta.ordered_delta.updated.iter().map(|change| {
            serde_json::json!({
                "kind": 2,
                "id": change.id.uuid().to_string(),
                "index": change.new_index,
                "row": change.row.as_ref().map(|row| row_to_json(row, descriptor))
            })
        }))
        .chain(delta.ordered_delta.added.iter().map(|change| {
            serde_json::json!({
                "kind": 0,
                "id": change.id.uuid().to_string(),
                "index": change.index,
                "row": row_to_json(&change.row, descriptor)
            })
        }))
        .collect::<Vec<_>>();

    serde_json::Value::Array(delta_obj)
}

pub fn generate_id() -> String {
    ObjectId::new().uuid().to_string()
}

pub fn parse_external_object_id(object_id: Option<&str>) -> Result<Option<ObjectId>, String> {
    let Some(object_id) = object_id else {
        return Ok(None);
    };

    let uuid = Uuid::parse_str(object_id).map_err(|err| format!("Invalid ObjectId: {err}"))?;
    Ok(Some(ObjectId::from_uuid(uuid)))
}

pub fn current_timestamp_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::{
        align_query_rows_to_declared_schema, align_values_to_declared_schema,
        parse_read_durability_options, parse_runtime_schema_input, parse_write_context_input,
        query_rows_can_be_schema_aligned,
    };
    use crate::batch_fate::BatchMode;
    use crate::object::ObjectId;
    use crate::query_manager::query::Query;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, Schema, SchemaBuilder, TableName, TableSchema,
        Value,
    };

    fn declared_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("description", ColumnType::Text),
            )
            .build()
    }

    fn runtime_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("description", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("title", ColumnType::Text),
            )
            .build()
    }

    #[test]
    fn query_rows_are_reordered_back_to_declared_schema() {
        let rows = vec![(
            ObjectId::new(),
            vec![
                Value::Text("note".to_string()),
                Value::Boolean(false),
                Value::Text("buy milk".to_string()),
            ],
        )];
        let query = Query::new("todos");

        let aligned = align_query_rows_to_declared_schema(
            &declared_todo_schema(),
            &runtime_todo_schema(),
            &query,
            rows,
        );

        assert_eq!(
            aligned[0].1,
            vec![
                Value::Text("buy milk".to_string()),
                Value::Boolean(false),
                Value::Text("note".to_string()),
            ]
        );
    }

    #[test]
    fn parse_external_object_id_accepts_any_valid_uuid() {
        let parsed = super::parse_external_object_id(Some("550e8400-e29b-41d4-a716-446655440000"))
            .expect("parse valid uuid");

        assert_eq!(
            parsed.expect("object id").uuid().to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn descriptor_values_are_reordered_back_to_declared_schema() {
        let runtime_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("description", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
            ColumnDescriptor::new("title", ColumnType::Text),
        ]);

        let aligned = align_values_to_declared_schema(
            &declared_todo_schema(),
            &TableName::new("todos"),
            &runtime_descriptor,
            vec![
                Value::Text("note".to_string()),
                Value::Boolean(true),
                Value::Text("ship fix".to_string()),
            ],
        );

        assert_eq!(
            aligned,
            vec![
                Value::Text("ship fix".to_string()),
                Value::Boolean(true),
                Value::Text("note".to_string()),
            ]
        );
    }

    #[test]
    fn simple_queries_are_schema_alignable() {
        assert!(query_rows_can_be_schema_aligned(&Query::new("todos")));
    }

    #[test]
    fn read_durability_options_default_to_full_and_immediate() {
        let (durability, propagation) =
            parse_read_durability_options(Some("local"), None).expect("parse options");

        assert_eq!(
            durability.tier,
            Some(crate::sync_manager::DurabilityTier::Local)
        );
        assert_eq!(
            durability.local_updates,
            crate::query_manager::manager::LocalUpdates::Immediate
        );
        assert_eq!(propagation, crate::sync_manager::QueryPropagation::Full);
    }

    #[test]
    fn runtime_schema_envelope_reads_ts_policy_bundle_flag() {
        let schema_json = r#"{
            "__jazzRuntimeSchema": 1,
            "schema": {
                "todos": {
                    "columns": [
                        {
                            "name": "title",
                            "column_type": { "type": "Text" },
                            "nullable": false
                        },
                        {
                            "name": "done",
                            "column_type": { "type": "Boolean" },
                            "nullable": false
                        }
                    ]
                }
            },
            "loadedPolicyBundle": true
        }"#;

        let input = parse_runtime_schema_input(schema_json).expect("parse runtime schema");

        assert!(input.loaded_policy_bundle);
        assert!(input.schema.contains_key(&TableName::new("todos")));
    }

    #[test]
    fn runtime_schema_envelope_defaults_missing_policy_bundle_flag_to_permissive_local() {
        let schema_json = r#"{
            "__jazzRuntimeSchema": 1,
            "schema": {
                "todos": {
                    "columns": [
                        {
                            "name": "title",
                            "column_type": { "type": "Text" },
                            "nullable": false
                        }
                    ]
                }
            }
        }"#;

        let input = parse_runtime_schema_input(schema_json).expect("parse runtime schema");

        assert!(!input.loaded_policy_bundle);
        assert!(input.schema.contains_key(&TableName::new("todos")));
    }

    #[test]
    fn parse_write_context_accepts_ts_batch_id_strings() {
        let batch_id = "0123456789abcdef0123456789abcdef";
        let input = format!(
            r#"{{
                "session": {{
                    "user_id": "alice",
                    "claims": {{}},
                    "authMode": "external"
                }},
                "batch_mode": "transactional",
                "batch_id": "{batch_id}",
                "target_branch_name": "dev-123456789abc-main"
            }}"#
        );

        let context = parse_write_context_input(Some(&input))
            .expect("parse write context")
            .expect("write context");

        assert_eq!(
            context
                .batch_id()
                .map(|parsed| parsed.to_string())
                .as_deref(),
            Some(batch_id)
        );
        assert_eq!(context.target_branch_name(), Some("dev-123456789abc-main"));
    }

    #[test]
    fn parse_write_context_rejects_legacy_batch_id_arrays() {
        let input = r#"{
            "session": {
                "user_id": "alice",
                "claims": {},
                "authMode": "external"
            },
            "batch_mode": "transactional",
            "batch_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            "target_branch_name": "dev-123456789abc-main"
        }"#;

        let error =
            parse_write_context_input(Some(input)).expect_err("legacy batch_id should fail");
        assert!(error.contains("WriteContextWire"));
    }

    #[test]
    fn write_context_accepts_lowercase_transactional_batch_mode() {
        let context = parse_write_context_input(Some(
            r#"{
                "batch_mode": "transactional",
                "batch_id": "0196721ac2617f10a4bebbc7f7ffdb3f",
                "target_branch_name": "dev-111111111111-main"
            }"#,
        ))
        .expect("parse write context")
        .expect("write context present");

        assert_eq!(context.batch_mode(), BatchMode::Transactional);
        assert_eq!(context.target_branch_name(), Some("dev-111111111111-main"));
        assert!(context.batch_id().is_some());
    }
}
