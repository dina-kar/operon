//! JSON values and Qdrant's protobuf `Value` (`json_with_int.proto`): an
//! integer that fits `i64` is `integer_value`, any other number
//! `double_value`.

use std::collections::HashMap;

use serde_json::{Map, Number, Value as Json};

use crate::proto::qdrant::value::Kind;
use crate::proto::qdrant::{ListValue, NullValue, Struct, Value};

pub fn json_to_value(v: &Json) -> Value {
    let kind = match v {
        Json::Null => Kind::NullValue(NullValue::NullValue as i32),
        Json::Bool(b) => Kind::BoolValue(*b),
        Json::Number(n) => match n.as_i64() {
            Some(i) => Kind::IntegerValue(i),
            None => Kind::DoubleValue(n.as_f64().unwrap_or(f64::NAN)),
        },
        Json::String(s) => Kind::StringValue(s.clone()),
        Json::Array(items) => Kind::ListValue(ListValue {
            values: items.iter().map(json_to_value).collect(),
        }),
        Json::Object(map) => Kind::StructValue(Struct {
            fields: map_to_payload(map),
        }),
    };
    Value { kind: Some(kind) }
}

/// A non-finite double (which JSON cannot hold) and an unset kind become
/// `null`.
pub fn value_to_json(v: &Value) -> Json {
    match &v.kind {
        None | Some(Kind::NullValue(_)) => Json::Null,
        Some(Kind::BoolValue(b)) => Json::Bool(*b),
        Some(Kind::IntegerValue(i)) => Json::from(*i),
        Some(Kind::DoubleValue(d)) => Number::from_f64(*d).map_or(Json::Null, Json::Number),
        Some(Kind::StringValue(s)) => Json::String(s.clone()),
        Some(Kind::ListValue(list)) => Json::Array(list.values.iter().map(value_to_json).collect()),
        Some(Kind::StructValue(s)) => Json::Object(payload_to_map(&s.fields)),
    }
}

pub fn map_to_payload(m: &Map<String, Json>) -> HashMap<String, Value> {
    m.iter()
        .map(|(k, v)| (k.clone(), json_to_value(v)))
        .collect()
}

/// A protobuf map has no order, so the keys come out sorted.
pub fn payload_to_map(p: &HashMap<String, Value>) -> Map<String, Json> {
    let mut keys: Vec<&String> = p.keys().collect();
    keys.sort();
    keys.into_iter()
        .map(|k| (k.clone(), value_to_json(&p[k])))
        .collect()
}
