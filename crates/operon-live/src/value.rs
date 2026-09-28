//! [`LiveValue`], a Loam Live value (design §20 §4.1), and its two
//! encodings: the `loam.live.v1.Value` protobuf (storage and the wire) and
//! the order-preserving tuple element of index keys (§20 §4.3).

use std::borrow::Cow;
use std::collections::BTreeMap;

use operon_tikv::tuple::Elem;

use crate::LiveError;
use crate::pb;
use crate::pb::__buffa::oneof::value::Kind;

/// A Loam Live value. `I64` and `F64` are distinct types, so `1` and `1.0`
/// are different values (as in Convex).
#[derive(Debug, Clone)]
pub enum LiveValue {
    Null,
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(String),
    Bytes(Vec<u8>),
    Array(Vec<LiveValue>),
    Object(BTreeMap<String, LiveValue>),
}

/// Value identity: doubles compare by their bits, except that every NaN
/// equals every NaN, so `-0.0 != 0.0` here (they are equal as index keys).
impl PartialEq for LiveValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LiveValue::Null, LiveValue::Null) => true,
            (LiveValue::I64(a), LiveValue::I64(b)) => a == b,
            (LiveValue::F64(a), LiveValue::F64(b)) => {
                a.to_bits() == b.to_bits() || (a.is_nan() && b.is_nan())
            }
            (LiveValue::Bool(a), LiveValue::Bool(b)) => a == b,
            (LiveValue::Str(a), LiveValue::Str(b)) => a == b,
            (LiveValue::Bytes(a), LiveValue::Bytes(b)) => a == b,
            (LiveValue::Array(a), LiveValue::Array(b)) => a == b,
            (LiveValue::Object(a), LiveValue::Object(b)) => a == b,
            _ => false,
        }
    }
}

impl LiveValue {
    /// The value of a protobuf `Value`. A `Value` (at any depth) with no kind
    /// set is [`LiveError::InvalidArgument`].
    pub fn from_proto(v: pb::Value) -> Result<Self, LiveError> {
        Ok(match v.kind {
            None => return Err(LiveError::invalid("a Value has no kind set")),
            Some(Kind::NullValue(_)) => LiveValue::Null,
            Some(Kind::Int64Value(i)) => LiveValue::I64(i),
            Some(Kind::DoubleValue(f)) => LiveValue::F64(f),
            Some(Kind::BoolValue(b)) => LiveValue::Bool(b),
            Some(Kind::StringValue(s)) => LiveValue::Str(s),
            Some(Kind::BytesValue(b)) => LiveValue::Bytes(b),
            Some(Kind::ArrayValue(a)) => LiveValue::Array(
                a.values
                    .into_iter()
                    .map(LiveValue::from_proto)
                    .collect::<Result<_, _>>()?,
            ),
            Some(Kind::ObjectValue(o)) => LiveValue::Object(fields_from_proto(o.fields)?),
        })
    }

    /// The protobuf form of this value.
    pub fn to_proto(&self) -> pb::Value {
        let kind = match self {
            LiveValue::Null => Kind::NullValue(Box::default()),
            LiveValue::I64(i) => Kind::Int64Value(*i),
            LiveValue::F64(f) => Kind::DoubleValue(*f),
            LiveValue::Bool(b) => Kind::BoolValue(*b),
            LiveValue::Str(s) => Kind::StringValue(s.clone()),
            LiveValue::Bytes(b) => Kind::BytesValue(b.clone()),
            LiveValue::Array(a) => Kind::ArrayValue(Box::new(pb::Array {
                values: a.iter().map(LiveValue::to_proto).collect(),
                ..Default::default()
            })),
            LiveValue::Object(o) => Kind::ObjectValue(Box::new(pb::Object {
                fields: fields_to_proto(o),
                ..Default::default()
            })),
        };
        pb::Value {
            kind: Some(kind),
            ..Default::default()
        }
    }

    /// The tuple element of this value in an index key. Objects, and arrays
    /// holding one at any depth, are not indexable in R1
    /// ([`LiveError::InvalidArgument`]).
    pub fn index_elem(&self) -> Result<Elem<'_>, LiveError> {
        Ok(match self {
            LiveValue::Null => Elem::Null,
            LiveValue::I64(i) => Elem::I64(*i),
            LiveValue::F64(f) => Elem::F64(*f),
            LiveValue::Bool(b) => Elem::Bool(*b),
            LiveValue::Str(s) => Elem::Str(Cow::Borrowed(s)),
            LiveValue::Bytes(b) => Elem::Bytes(Cow::Borrowed(b)),
            LiveValue::Array(a) => Elem::Array(
                a.iter()
                    .map(LiveValue::index_elem)
                    .collect::<Result<_, _>>()?,
            ),
            LiveValue::Object(_) => {
                return Err(LiveError::invalid("objects are not indexable in R1"));
            }
        })
    }

    /// The type's name, for error messages.
    pub fn type_name(&self) -> &'static str {
        match self {
            LiveValue::Null => "null",
            LiveValue::I64(_) => "int64",
            LiveValue::F64(_) => "float64",
            LiveValue::Bool(_) => "bool",
            LiveValue::Str(_) => "string",
            LiveValue::Bytes(_) => "bytes",
            LiveValue::Array(_) => "array",
            LiveValue::Object(_) => "object",
        }
    }
}

/// Document or object fields from their protobuf map.
pub fn fields_from_proto(
    fields: impl IntoIterator<Item = (String, pb::Value)>,
) -> Result<BTreeMap<String, LiveValue>, LiveError> {
    fields
        .into_iter()
        .map(|(k, v)| Ok((k, LiveValue::from_proto(v)?)))
        .collect()
}

/// The protobuf map of document or object fields.
pub fn fields_to_proto<C>(fields: &BTreeMap<String, LiveValue>) -> C
where
    C: FromIterator<(String, pb::Value)>,
{
    fields
        .iter()
        .map(|(k, v)| (k.clone(), v.to_proto()))
        .collect()
}
