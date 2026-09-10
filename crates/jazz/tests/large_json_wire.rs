//! Wire descriptors need an internal receipt: visible JSON alone cannot prove
//! that an inline payload and an indirect reference use the same semantic kind.
use jazz::groove::large_values::{LargeValueKind, prepare_with_fixture_locators};
use jazz::groove::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use jazz::ids::{AuthorSubject, RowUuid, SchemaVersionId};
use jazz::protocol::VersionRecord;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn unhex(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
/// Alice's inline and indirect JSON version records retain their semantic kind
/// at Bob's wire decoder. This codec receipt inspects descriptors because visible
/// JSON alone cannot distinguish a JSON kind from an ordinary String carrier.
fn json_version_records_freeze_inline_and_indirect_semantics() {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .column("payload", ColumnType::Json { schema: None }),
            )
            .build(),
    )
    .unwrap();
    let table = &schema.tables[0];
    let author = AuthorSubject::for_test_bytes([0x33; 16]);
    let make = |value: Value| {
        VersionRecord::encode(
            table,
            SchemaVersionId::from_bytes([0x22; 16]),
            RowUuid::from_bytes([0x44; 16]),
            vec![],
            author,
            7,
            author,
            8,
            &[Some(value)],
            None,
        )
        .unwrap()
    };
    let inline_value = Value::String("{\"answer\":42}".into());
    let inline = make(inline_value.clone());
    let json = format!("{{\"padding\":\"{}\"}}", "a".repeat(70000));
    let prepared =
        prepare_with_fixture_locators(LargeValueKind::Json, json.as_bytes(), b"jazz-json-wire-v1")
            .unwrap();
    let indirect_value = Value::Large(prepared.value_ref);
    let indirect = make(indirect_value.clone());
    let legacy_descriptor =
        RecordDescriptor::new(inline.record().descriptor().fields().iter().map(|field| {
            (
                field.name.clone().unwrap(),
                if field.name.as_deref() == Some("_app_payload") {
                    ValueType::Nullable(Box::new(ValueType::String))
                } else {
                    field.value_type.clone()
                },
            )
        }));
    let inline_values = inline.record().to_values().unwrap();
    let legacy_raw = legacy_descriptor.create(&inline_values).unwrap();
    assert_eq!(
        legacy_raw,
        inline.record().raw(),
        "inline scalar bytes remain unchanged; the serialized descriptor changes"
    );
    assert!(
        legacy_descriptor
            .create(&indirect.record().to_values().unwrap())
            .is_err(),
        "legacy String descriptors never supported indirect JSON"
    );
    let legacy = VersionRecord::new(
        "documents",
        SchemaVersionId::from_bytes([0x22; 16]),
        OwnedRecord::new(legacy_raw, legacy_descriptor),
    );
    let corpus = serde_json::json!({ "inline": hex(&postcard::to_allocvec(&inline).unwrap()), "indirect": hex(&postcard::to_allocvec(&indirect).unwrap()), "legacy_inline": hex(&postcard::to_allocvec(&legacy).unwrap()) });
    if std::env::var_os("JAZZ_UPDATE_WIRE_FIXTURES").is_some() {
        std::fs::write(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/fixtures/large_json_wire_v1.json"
            ),
            serde_json::to_string_pretty(&corpus).unwrap() + "\n",
        )
        .unwrap();
        return;
    }
    let expected: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/large_json_wire_v1.json")).unwrap();
    assert_eq!(corpus, expected);
    for (name, value) in [("inline", inline_value), ("indirect", indirect_value)] {
        let bytes = unhex(expected[name].as_str().unwrap());
        assert!(bytes.windows(5).any(|bytes| bytes == b"JVRR\x01"));
        let decoded: VersionRecord = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(
            decoded.record().descriptor(),
            &table.wire_record_descriptor()
        );
        assert_eq!(
            decoded.record().borrowed().get("_app_payload").unwrap(),
            Value::Nullable(Some(Box::new(value)))
        );
        assert_eq!(postcard::to_allocvec(&decoded).unwrap(), bytes);
    }
    let legacy: VersionRecord =
        postcard::from_bytes(&unhex(expected["legacy_inline"].as_str().unwrap())).unwrap();
    assert_ne!(
        legacy.record().descriptor(),
        &table.wire_record_descriptor(),
        "schema admission rejects the old inline JSON descriptor"
    );
}

/// Alice's explicit SQL null, JSON literal null, and omitted update have distinct
/// version-record bytes for Bob. The descriptor names the existing v1 nullable
/// and stored-JSON codecs; no new scalar tag or implicit serializer is introduced.
#[test]
fn nullable_json_version_records_preserve_presence_and_nullability() {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("documents")
                    .nullable_column("payload", ColumnType::Json { schema: None }),
            )
            .build(),
    )
    .unwrap();
    let table = &schema.tables[0];
    let author = AuthorSubject::for_test_bytes([0x33; 16]);
    let values = [
        ("omitted", None),
        ("sql_null", Some(Value::Nullable(None))),
        (
            "json_null",
            Some(Value::Nullable(Some(Box::new(Value::String(
                "null".into(),
            ))))),
        ),
        (
            "object",
            Some(Value::Nullable(Some(Box::new(Value::String(
                "{\"answer\":42}".into(),
            ))))),
        ),
    ];
    let mut corpus = serde_json::Map::new();
    for (name, value) in values {
        let record = VersionRecord::encode(
            table,
            SchemaVersionId::from_bytes([0x22; 16]),
            RowUuid::from_bytes([0x44; 16]),
            vec![],
            author,
            7,
            author,
            8,
            &[value.clone()],
            None,
        )
        .unwrap();
        let bytes = postcard::to_allocvec(&record).unwrap();
        let decoded: VersionRecord = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(
            decoded.record().descriptor(),
            &table.wire_record_descriptor()
        );
        assert_eq!(
            decoded.record().borrowed().get("_app_payload").unwrap(),
            Value::Nullable(value.map(Box::new)),
        );
        assert_eq!(postcard::to_allocvec(&decoded).unwrap(), bytes);
        corpus.insert(name.into(), hex(&bytes).into());
    }
    if std::env::var_os("JAZZ_UPDATE_WIRE_FIXTURES").is_some() {
        std::fs::write(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/fixtures/nullable_json_wire_v1.json"
            ),
            serde_json::to_string_pretty(&corpus).unwrap() + "\n",
        )
        .unwrap();
    } else {
        let expected: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/nullable_json_wire_v1.json")).unwrap();
        assert_eq!(serde_json::Value::Object(corpus), expected);
    }
}
