//! R1 plan Task 7: `loams.live.v1` values and documents survive the binary
//! protobuf encoding and the proto3 JSON mapping (Connect's JSON codec),
//! with 64-bit integers as JSON strings so they stay lossless in
//! JavaScript.

use std::collections::BTreeMap;

use buffa::Message;
use loams_live_proto::loams::live::v1::__buffa::oneof::value::Kind;
use loams_live_proto::loams::live::v1::{Array, DocumentRecord, Object, Value};

fn value(kind: Kind) -> Value {
    Value {
        kind: Some(kind),
        ..Default::default()
    }
}

fn null() -> Value {
    value(Kind::NullValue(Box::default()))
}

fn int(v: i64) -> Value {
    value(Kind::Int64Value(v))
}

fn double(v: f64) -> Value {
    value(Kind::DoubleValue(v))
}

fn string(v: &str) -> Value {
    value(Kind::StringValue(v.to_string()))
}

fn array(values: Vec<Value>) -> Value {
    value(Kind::ArrayValue(Box::new(Array {
        values,
        ..Default::default()
    })))
}

fn object(fields: &[(&str, Value)]) -> Value {
    value(Kind::ObjectValue(Box::new(Object {
        fields: fields
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
        ..Default::default()
    })))
}

/// Every variant, with the edge cases of each.
fn every_variant() -> Vec<(&'static str, Value)> {
    vec![
        ("null", null()),
        ("int64 zero", int(0)),
        ("int64 min", int(i64::MIN)),
        ("int64 max", int(i64::MAX)),
        ("int64 above 2^53", int((1 << 53) + 1)),
        ("double zero", double(0.0)),
        ("double negative zero", double(-0.0)),
        ("double fraction", double(-1.5)),
        ("double smallest positive", double(f64::MIN_POSITIVE)),
        ("double max", double(f64::MAX)),
        ("double infinity", double(f64::INFINITY)),
        ("double negative infinity", double(f64::NEG_INFINITY)),
        ("double NaN", double(f64::NAN)),
        ("bool false", value(Kind::BoolValue(false))),
        ("bool true", value(Kind::BoolValue(true))),
        ("string empty", string("")),
        (
            "string unicode and NUL",
            string("h\u{e9}llo \u{1F980} \0 end"),
        ),
        ("bytes empty", value(Kind::BytesValue(Vec::new()))),
        (
            "bytes every byte",
            value(Kind::BytesValue((0..=255).collect())),
        ),
        ("array empty", array(Vec::new())),
        (
            "array nested",
            array(vec![int(1), double(1.0), array(vec![null(), string("x")])]),
        ),
        ("object empty", object(&[])),
        (
            "object nested",
            object(&[
                ("a", int(-7)),
                (
                    "b",
                    object(&[("c", array(vec![value(Kind::BoolValue(true))]))]),
                ),
                ("", string("empty key")),
            ]),
        ),
    ]
}

/// Equality that treats NaN as equal to itself and tells `-0.0` from `0.0`,
/// so a roundtrip must keep the exact double.
fn same(a: &Value, b: &Value) -> bool {
    match (&a.kind, &b.kind) {
        (Some(Kind::DoubleValue(x)), Some(Kind::DoubleValue(y))) => {
            x.to_bits() == y.to_bits() || (x.is_nan() && y.is_nan())
        }
        (Some(Kind::ArrayValue(x)), Some(Kind::ArrayValue(y))) => {
            x.values.len() == y.values.len()
                && x.values.iter().zip(&y.values).all(|(x, y)| same(x, y))
        }
        (Some(Kind::ObjectValue(x)), Some(Kind::ObjectValue(y))) => {
            same_fields(&x.fields, &y.fields)
        }
        _ => a == b,
    }
}

/// Map fields compared by key (the generated map type is buffa's own).
fn same_fields<'a>(
    a: impl IntoIterator<Item = (&'a String, &'a Value)>,
    b: impl IntoIterator<Item = (&'a String, &'a Value)>,
) -> bool {
    let a: BTreeMap<_, _> = a.into_iter().collect();
    let b: BTreeMap<_, _> = b.into_iter().collect();
    a.len() == b.len() && a.iter().all(|(k, v)| b.get(k).is_some_and(|w| same(v, w)))
}

#[test]
fn value_binary_and_json_roundtrip() {
    for (name, v) in every_variant() {
        let bytes = v.encode_to_vec();
        let back = Value::decode_from_slice(&bytes).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert!(
            same(&v, &back),
            "{name}: binary {v:?} came back as {back:?}"
        );

        let json = serde_json::to_string(&v).unwrap_or_else(|e| panic!("{name}: {e}"));
        let back: Value =
            serde_json::from_str(&json).unwrap_or_else(|e| panic!("{name}: {json}: {e}"));
        assert!(same(&v, &back), "{name}: JSON {json} came back as {back:?}");
    }
}

#[test]
fn int64_extremes_are_json_strings() {
    for (v, text) in [
        (i64::MIN, "-9223372036854775808"),
        (i64::MAX, "9223372036854775807"),
        ((1 << 53) + 1, "9007199254740993"),
    ] {
        let json = serde_json::to_value(int(v)).expect("to JSON");
        assert_eq!(json, serde_json::json!({ "int64Value": text }), "{v}");
        let back: Value = serde_json::from_value(json).expect("from JSON");
        assert_eq!(back, int(v));
    }
    // The other JSON forms clients see.
    assert_eq!(
        serde_json::to_value(null()).expect("to JSON"),
        serde_json::json!({ "nullValue": {} })
    );
    assert_eq!(
        serde_json::to_value(double(f64::NAN)).expect("to JSON"),
        serde_json::json!({ "doubleValue": "NaN" })
    );
    assert_eq!(
        serde_json::to_value(value(Kind::BytesValue(vec![0xff, 0x00]))).expect("to JSON"),
        serde_json::json!({ "bytesValue": "/wA=" })
    );
}

#[test]
fn document_record_roundtrip() {
    let fields = every_variant();
    for creation_ms in [0, 1_790_000_000_000, u64::MAX] {
        let doc = DocumentRecord {
            format: 1,
            creation_ms,
            fields: fields
                .iter()
                .map(|(name, v)| ((*name).to_string(), v.clone()))
                .collect(),
            ..Default::default()
        };

        let back = DocumentRecord::decode_from_slice(&doc.encode_to_vec()).expect("binary");
        assert_eq!((back.format, back.creation_ms), (1, creation_ms));
        assert!(
            same_fields(&doc.fields, &back.fields),
            "binary fields differ"
        );

        let json = serde_json::to_value(&doc).expect("to JSON");
        assert_eq!(json["format"], 1);
        if creation_ms == 0 {
            // proto3 JSON leaves out a field at its default.
            assert!(json.get("creationMs").is_none(), "{json}");
        } else {
            assert_eq!(
                json["creationMs"],
                serde_json::json!(creation_ms.to_string())
            );
        }
        let back: DocumentRecord = serde_json::from_value(json).expect("from JSON");
        assert_eq!((back.format, back.creation_ms), (1, creation_ms));
        assert!(same_fields(&doc.fields, &back.fields), "JSON fields differ");
    }
}
