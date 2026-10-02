//! gRPC filters and payload-index requests to the REST model (Task 4), so
//! both surfaces compile one `Filter`.

use serde_json::{Map, Value, json};

use crate::error::GatewayError;
use crate::ids::{pk_to_json, point_id_from_grpc};
use crate::model::collections::{CreateFieldIndex, PayloadFieldSchema};
use crate::model::filter::{
    AnyVariants, Condition, FieldCondition, Filter, Match, MatchValue, MinShould, OneOrMany,
    PayloadField, Range, RangeInterface, ValuesCount,
};
use crate::proto::qdrant as pb;

/// A gRPC `Filter`; empty lists stay empty (they match everything).
pub fn filter_from_grpc(f: &pb::Filter) -> Result<Filter, GatewayError> {
    let list =
        |conditions: &[pb::Condition]| -> Result<Option<OneOrMany<Condition>>, GatewayError> {
            Ok(Some(OneOrMany::Many(
                conditions
                    .iter()
                    .map(condition_from_grpc)
                    .collect::<Result<_, _>>()?,
            )))
        };
    Ok(Filter {
        must: list(&f.must)?,
        should: list(&f.should)?,
        must_not: list(&f.must_not)?,
        min_should: f
            .min_should
            .as_ref()
            .map(|m| -> Result<MinShould, GatewayError> {
                Ok(MinShould {
                    conditions: m
                        .conditions
                        .iter()
                        .map(condition_from_grpc)
                        .collect::<Result<_, _>>()?,
                    min_count: usize::try_from(m.min_count).unwrap_or(usize::MAX),
                })
            })
            .transpose()?,
    })
}

/// One gRPC condition; an unset one is a `BadRequest`.
fn condition_from_grpc(c: &pb::Condition) -> Result<Condition, GatewayError> {
    use pb::condition::ConditionOneOf;
    let Some(one) = &c.condition_one_of else {
        return Err(GatewayError::BadRequest(
            "Condition must have a condition".to_string(),
        ));
    };
    Ok(match one {
        ConditionOneOf::Field(fc) => Condition::Field(field_from_grpc(fc)?),
        ConditionOneOf::IsEmpty(e) => Condition::IsEmpty {
            is_empty: PayloadField { key: e.key.clone() },
        },
        ConditionOneOf::IsNull(n) => Condition::IsNull {
            is_null: PayloadField { key: n.key.clone() },
        },
        ConditionOneOf::HasId(h) => Condition::HasId {
            has_id: h
                .has_id
                .iter()
                .map(|id| point_id_from_grpc(id).map(|id| pk_to_json(&id.to_pk())))
                .collect::<Result<_, _>>()?,
        },
        ConditionOneOf::Filter(f) => Condition::Filter(Box::new(filter_from_grpc(f)?)),
        ConditionOneOf::Nested(_) => Condition::Nested {
            nested: Value::Null,
        },
        ConditionOneOf::HasVector(h) => Condition::HasVector {
            has_vector: h.has_vector.clone(),
        },
        ConditionOneOf::Slice(_) => Condition::Slice { slice: Value::Null },
    })
}

/// A gRPC field condition, its `match` and ranges converted to the model.
fn field_from_grpc(fc: &pb::FieldCondition) -> Result<FieldCondition, GatewayError> {
    let present = |on: bool| on.then_some(Value::Null);
    let range = match (&fc.range, &fc.datetime_range) {
        (Some(r), _) => Some(RangeInterface::Number(Range {
            lt: r.lt,
            gt: r.gt,
            gte: r.gte,
            lte: r.lte,
        })),
        (None, Some(r)) => {
            let ts =
                |t: &Option<prost_types::Timestamp>| t.as_ref().map(timestamp_rfc3339).transpose();
            Some(RangeInterface::Datetime(Range {
                lt: ts(&r.lt)?,
                gt: ts(&r.gt)?,
                gte: ts(&r.gte)?,
                lte: ts(&r.lte)?,
            }))
        }
        (None, None) => None,
    };
    Ok(FieldCondition {
        key: fc.key.clone(),
        r#match: fc.r#match.as_ref().and_then(match_from_grpc),
        range,
        values_count: fc.values_count.as_ref().map(|v| ValuesCount {
            lt: v.lt,
            gt: v.gt,
            gte: v.gte,
            lte: v.lte,
        }),
        is_empty: fc.is_empty,
        is_null: fc.is_null,
        geo_bounding_box: present(fc.geo_bounding_box.is_some()),
        geo_radius: present(fc.geo_radius.is_some()),
        geo_polygon: present(fc.geo_polygon.is_some()),
    })
}

/// `None` for a `Match` without a value (then the condition has no
/// `match`).
fn match_from_grpc(m: &pb::Match) -> Option<Match> {
    use pb::r#match::MatchValue as M;
    Some(match m.match_value.as_ref()? {
        M::Keyword(s) => Match::Value {
            value: MatchValue::Str(s.clone()),
        },
        M::Integer(n) => Match::Value {
            value: MatchValue::Int(*n),
        },
        M::Boolean(b) => Match::Value {
            value: MatchValue::Bool(*b),
        },
        M::Text(t) => Match::Text { text: t.clone() },
        M::TextAny(t) => Match::TextAny {
            text_any: t.clone(),
        },
        M::Phrase(p) => Match::Phrase { phrase: p.clone() },
        M::Prefix(p) => Match::Prefix { prefix: p.clone() },
        M::Keywords(k) => Match::Any {
            any: strs_or_empty(&k.strings),
        },
        M::Integers(i) => Match::Any {
            any: AnyVariants::Ints(i.integers.clone()),
        },
        M::ExceptKeywords(k) => Match::Except {
            except: strs_or_empty(&k.strings),
        },
        M::ExceptIntegers(i) => Match::Except {
            except: AnyVariants::Ints(i.integers.clone()),
        },
    })
}

/// An empty list is `Ints([])`, as REST reads `[]`.
fn strs_or_empty(strings: &[String]) -> AnyVariants {
    if strings.is_empty() {
        AnyVariants::Ints(Vec::new())
    } else {
        AnyVariants::Strs(strings.to_vec())
    }
}

/// A protobuf timestamp as RFC 3339 (UTC).
fn timestamp_rfc3339(t: &prost_types::Timestamp) -> Result<String, GatewayError> {
    let nanos = i128::from(t.seconds) * 1_000_000_000 + i128::from(t.nanos);
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()
        .and_then(|at| {
            at.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .ok_or_else(|| GatewayError::BadRequest(format!("Unable to parse datetime {t}")))
}

/// `CreateFieldIndex` as the REST body: the params' type when given, else
/// `field_type`; neither is `field_schema is required`.
pub fn field_index_from_grpc(r: &pb::CreateFieldIndexCollection) -> CreateFieldIndex {
    use pb::payload_index_params::IndexParams;
    let from_type = r.field_type.map(|t| {
        match pb::FieldType::try_from(t) {
            Ok(pb::FieldType::Keyword) => "keyword",
            Ok(pb::FieldType::Integer) => "integer",
            Ok(pb::FieldType::Float) => "float",
            Ok(pb::FieldType::Geo) => "geo",
            Ok(pb::FieldType::Text) => "text",
            Ok(pb::FieldType::Bool) => "bool",
            Ok(pb::FieldType::Datetime) => "datetime",
            Ok(pb::FieldType::Uuid) => "uuid",
            Err(_) => "unknown",
        }
        .to_string()
    });
    let params = r
        .field_index_params
        .as_ref()
        .and_then(|p| p.index_params.as_ref());
    let field_schema = match params {
        Some(IndexParams::TextIndexParams(t)) => Some(PayloadFieldSchema::Params(text_params(t))),
        Some(other) => {
            let name = match other {
                IndexParams::KeywordIndexParams(_) => "keyword",
                IndexParams::IntegerIndexParams(_) => "integer",
                IndexParams::FloatIndexParams(_) => "float",
                IndexParams::GeoIndexParams(_) => "geo",
                IndexParams::BoolIndexParams(_) => "bool",
                IndexParams::DatetimeIndexParams(_) => "datetime",
                IndexParams::UuidIndexParams(_) => "uuid",
                IndexParams::TextIndexParams(_) => "text",
            };
            Some(PayloadFieldSchema::Name(name.to_string()))
        }
        None => from_type.map(PayloadFieldSchema::Name),
    };
    CreateFieldIndex {
        field_name: r.field_name.clone(),
        field_schema,
    }
}

/// gRPC text params as REST's; an unset tokenizer is `word` (row T4-5).
fn text_params(t: &pb::TextIndexParams) -> Map<String, Value> {
    let tokenizer = match pb::TokenizerType::try_from(t.tokenizer) {
        Ok(pb::TokenizerType::Prefix) => "prefix",
        Ok(pb::TokenizerType::Whitespace) => "whitespace",
        Ok(pb::TokenizerType::Multilingual) => "multilingual",
        Ok(pb::TokenizerType::Word | pb::TokenizerType::Unknown) | Err(_) => "word",
    };
    let mut out = Map::new();
    out.insert("type".into(), json!("text"));
    out.insert("tokenizer".into(), json!(tokenizer));
    let mut put = |key: &str, v: Option<Value>| {
        if let Some(v) = v {
            out.insert(key.into(), v);
        }
    };
    put("lowercase", t.lowercase.map(Value::Bool));
    put("min_token_len", t.min_token_len.map(Value::from));
    put("max_token_len", t.max_token_len.map(Value::from));
    put("stopwords", t.stopwords.as_ref().map(|_| json!(true)));
    put("stemmer", t.stemmer.as_ref().map(|_| json!(true)));
    put("phrase_matching", t.phrase_matching.map(Value::Bool));
    put("ascii_folding", t.ascii_folding.map(Value::Bool));
    out
}
