//! Qdrant filters compiled to the search IR's `Query` ("Filter semantics";
//! Task 4), plus the reference evaluator the differential tests compare
//! the gateway with (feature `test-util`).
//!
//! Every condition on key `k` reads the catch-all `payload` field at
//! `payload.<normalized k>` (Ruling 5): text conditions its `standard`
//! companion and datetime ranges its date companion (Ruling 6), so no
//! result depends on whether or when a payload index was created.

use loams_collection::{CollectionSchema, FieldKind, PrimaryKey};
use loams_query::{BoolOperator, FieldValue, Query};

use crate::PAYLOAD_FIELD;
use crate::error::GatewayError;
use crate::ids::PointId;
use crate::jsonpath::JsonPath;
use crate::model::filter::{
    AnyVariants, Condition, FieldCondition, Filter, Match, MatchValue, RangeInterface,
};

/// `filter` as an IR query over `schema`, whose `payload` field it reads.
/// A collection without that field (made through another API) cannot be
/// filtered through the gateway (row T4-3).
pub fn compile_filter(filter: &Filter, schema: &CollectionSchema) -> Result<Query, GatewayError> {
    let has_payload = schema
        .fields
        .iter()
        .any(|f| f.name == PAYLOAD_FIELD && f.kind == FieldKind::Json);
    if !has_payload {
        return Err(GatewayError::Unsupported(
            "filters on a collection not created through the Qdrant API".to_string(),
        ));
    }
    compile(filter)
}

/// Both queries (`Bool { filter: [a, b] }`); either alone when the other is
/// absent.
pub fn and(a: Option<Query>, b: Option<Query>) -> Option<Query> {
    match (a, b) {
        (Some(a), Some(b)) => Some(Query::Bool {
            must: Vec::new(),
            should: Vec::new(),
            must_not: Vec::new(),
            filter: vec![a, b],
            minimum_should_match: None,
        }),
        (a, None) => a,
        (None, b) => b,
    }
}

/// `q` without the points `ids` (`Bool { filter: [q], must_not: [Ids] }`;
/// every point when `q` is absent). No ids leaves `q` as it is.
pub fn exclude_ids(q: Option<Query>, ids: &[PrimaryKey]) -> Option<Query> {
    if ids.is_empty() {
        return q;
    }
    Some(Query::Bool {
        must: Vec::new(),
        should: Vec::new(),
        must_not: vec![Query::Ids(ids.to_vec())],
        filter: vec![q.unwrap_or(Query::MatchAll)],
        minimum_should_match: None,
    })
}

/// A `Bool` query of the given clauses; the `filter` clauses do not
/// score.
fn bool_query(filter: Vec<Query>, should: Vec<Query>, must_not: Vec<Query>) -> Query {
    Query::Bool {
        must: Vec::new(),
        should,
        must_not,
        filter,
        minimum_should_match: None,
    }
}

/// Rule 1.1.
fn compile(filter: &Filter) -> Result<Query, GatewayError> {
    let list = |l: &Option<crate::model::filter::OneOrMany<Condition>>| {
        l.as_ref()
            .map_or(&[][..], |l| l.as_slice())
            .iter()
            .map(condition)
            .collect::<Result<Vec<_>, _>>()
    };
    let mut must = list(&filter.must)?;
    let should = list(&filter.should)?;
    let must_not = list(&filter.must_not)?;
    if !should.is_empty() {
        must.push(bool_query(Vec::new(), should, Vec::new()));
    }
    if let Some(min) = &filter.min_should {
        if min.min_count == 0 {
            return Err(GatewayError::BadRequest(
                "min_count must be greater than 0".to_string(),
            ));
        }
        let conditions = min
            .conditions
            .iter()
            .map(condition)
            .collect::<Result<Vec<_>, _>>()?;
        // The IR clamps `minimum_should_match` to the clause count, where
        // Qdrant never matches (row T4-4).
        must.push(if min.min_count > conditions.len() {
            Query::MatchNone
        } else {
            Query::Bool {
                must: Vec::new(),
                should: conditions,
                must_not: Vec::new(),
                filter: Vec::new(),
                minimum_should_match: Some(min.min_count.to_string()),
            }
        });
    }
    if must.is_empty() && must_not.is_empty() {
        return Ok(Query::MatchAll);
    }
    Ok(bool_query(must, Vec::new(), must_not))
}

/// The IR field of payload key `key`.
fn payload_field(key: &str) -> Result<String, GatewayError> {
    let path: JsonPath = key.parse()?;
    Ok(format!("{PAYLOAD_FIELD}.{}", path.normalized()?))
}

/// One condition of a filter; the conditions Ruling 15 leaves out are
/// `Unsupported`.
fn condition(c: &Condition) -> Result<Query, GatewayError> {
    let unsupported = |name: &str| Err(GatewayError::Unsupported(format!("{name} condition")));
    match c {
        Condition::Field(fc) => field_condition(fc),
        Condition::IsEmpty { is_empty } => Ok(Query::IsEmpty {
            field: payload_field(&is_empty.key)?,
        }),
        Condition::IsNull { is_null } => Ok(Query::IsNull {
            field: payload_field(&is_null.key)?,
        }),
        Condition::HasId { has_id } => Ok(Query::Ids(parse_ids(has_id)?)),
        Condition::HasVector { .. } => unsupported("has_vector"),
        Condition::Nested { .. } => unsupported("nested"),
        Condition::Slice { .. } => unsupported("slice"),
        Condition::Filter(f) => compile(f),
    }
}

/// `has_id` ids as keys (Ruling 18).
fn parse_ids(ids: &[serde_json::Value]) -> Result<Vec<PrimaryKey>, GatewayError> {
    ids.iter()
        .map(|v| PointId::from_json(v).map(PointId::to_pk))
        .collect()
}

/// A `FieldCondition` with at most the geo sub-conditions refused.
fn field_condition(fc: &FieldCondition) -> Result<Query, GatewayError> {
    for (name, present) in [
        ("geo_bounding_box", fc.geo_bounding_box.is_some()),
        ("geo_radius", fc.geo_radius.is_some()),
        ("geo_polygon", fc.geo_polygon.is_some()),
    ] {
        if present {
            return Err(GatewayError::Unsupported(format!("{name} condition")));
        }
    }
    let f = payload_field(&fc.key)?;
    let mut parts = Vec::new();
    if let Some(m) = &fc.r#match {
        parts.push(match_query(&f, m));
    }
    if let Some(r) = &fc.range {
        parts.push(range_query(&f, r)?);
    }
    if let Some(vc) = &fc.values_count {
        let (gt, gte, lt, lte) = tighten(vc.gt, vc.gte, vc.lt, vc.lte);
        parts.push(Query::ValuesCount {
            field: f.clone(),
            gt,
            gte,
            lt,
            lte,
        });
    }
    if let Some(is_empty) = fc.is_empty {
        parts.push(flag(Query::IsEmpty { field: f.clone() }, is_empty));
    }
    if let Some(is_null) = fc.is_null {
        parts.push(flag(Query::IsNull { field: f.clone() }, is_null));
    }
    match parts.len() {
        0 => Err(GatewayError::BadRequest(
            "At least one field condition must be specified".to_string(),
        )),
        1 => Ok(parts.remove(0)),
        _ => Ok(bool_query(Vec::new(), parts, Vec::new())),
    }
}

/// `q` for `true`, everything but `q` for `false`.
fn flag(q: Query, on: bool) -> Query {
    if on {
        q
    } else {
        bool_query(vec![Query::MatchAll], Vec::new(), vec![q])
    }
}

/// The values of `match.any` or `match.except` as IR terms.
fn any_values(list: &AnyVariants) -> Vec<FieldValue> {
    match list {
        AnyVariants::Ints(v) => v.iter().map(|n| FieldValue::I64(*n)).collect(),
        AnyVariants::Strs(v) => v.iter().map(|s| FieldValue::Str(s.clone())).collect(),
    }
}

/// Rule 1.3.
fn match_query(f: &str, m: &Match) -> Query {
    let field = f.to_string();
    let text = |text: &str, operator| Query::Match {
        field: field.clone(),
        text: text.to_string(),
        operator,
        minimum_should_match: None,
        fuzziness: None,
        analyzer: None,
    };
    match m {
        Match::Value { value } => Query::Term {
            field: field.clone(),
            value: match value {
                MatchValue::Bool(b) => FieldValue::Bool(*b),
                MatchValue::Int(n) => FieldValue::I64(*n),
                MatchValue::Str(s) => FieldValue::Str(s.clone()),
            },
        },
        Match::Any { any } if any.is_empty() => Query::MatchNone,
        Match::Any { any } => Query::Terms {
            field: field.clone(),
            values: any_values(any),
        },
        Match::Except { except } if except.is_empty() => Query::Exists {
            field: field.clone(),
        },
        Match::Except { except } => bool_query(
            vec![Query::Exists {
                field: field.clone(),
            }],
            Vec::new(),
            vec![Query::Terms {
                field: field.clone(),
                values: any_values(except),
            }],
        ),
        Match::Prefix { prefix } => Query::Prefix {
            field: field.clone(),
            value: prefix.clone(),
        },
        Match::Text { text: t } => text(t, BoolOperator::And),
        Match::TextAny { text_any } => text(text_any, BoolOperator::Or),
        Match::Phrase { phrase } => Query::MatchPhrase {
            field: field.clone(),
            text: phrase.clone(),
            slop: 0,
        },
    }
}

/// The bounds with `gt` and `gte` (or `lt` and `lte`) both set reduced to
/// the stricter one: Qdrant applies both, the IR takes one (row T4-6).
fn tighten<T: PartialOrd + Copy>(
    gt: Option<T>,
    gte: Option<T>,
    lt: Option<T>,
    lte: Option<T>,
) -> (Option<T>, Option<T>, Option<T>, Option<T>) {
    let (gt, gte) = match (gt, gte) {
        (Some(a), Some(b)) if a >= b => (Some(a), None),
        (Some(_), Some(b)) => (None, Some(b)),
        other => other,
    };
    let (lt, lte) = match (lt, lte) {
        (Some(a), Some(b)) if a <= b => (Some(a), None),
        (Some(_), Some(b)) => (None, Some(b)),
        other => other,
    };
    (gt, gte, lt, lte)
}

/// Rule 1.4.
fn range_query(f: &str, r: &RangeInterface) -> Result<Query, GatewayError> {
    let (gt, gte, lt, lte) = match r {
        RangeInterface::Number(r) => {
            let (gt, gte, lt, lte) = tighten(r.gt, r.gte, r.lt, r.lte);
            let v = |b: Option<f64>| b.map(FieldValue::F64);
            (v(gt), v(gte), v(lt), v(lte))
        }
        RangeInterface::Datetime(r) => {
            let v = |b: &Option<String>| b.as_deref().map(parse_datetime).transpose();
            let (gt, gte, lt, lte) = tighten(v(&r.gt)?, v(&r.gte)?, v(&r.lt)?, v(&r.lte)?);
            let v = |b: Option<i64>| b.map(FieldValue::Date);
            (v(gt), v(gte), v(lt), v(lte))
        }
    };
    Ok(Query::Range {
        field: f.to_string(),
        gt,
        gte,
        lt,
        lte,
    })
}

/// A datetime bound in Qdrant's formats (`qdrant:lib/segment/src/types.rs`,
/// `DateTimePayloadType::from_str`), as µs since the epoch, UTC: RFC 3339,
/// `YYYY-MM-DD[T| ]HH:MM:SS[.f]` with an offset `Z`, `±HH`, `±HHMM` or
/// `±HH:MM` or none (UTC), `YYYY-MM-DD[T| ]HH:MM`, and `YYYY-MM-DD`. Finer
/// digits than µs are truncated toward the past.
pub fn parse_datetime(s: &str) -> Result<i64, GatewayError> {
    parse_datetime_opt(s)
        .ok_or_else(|| GatewayError::BadRequest(format!("Unable to parse datetime {s}")))
}

/// [`parse_datetime`]'s parser: epoch milliseconds, or `None` for a string
/// in none of Qdrant's formats (row T4-9).
fn parse_datetime_opt(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    let num = |from: usize, len: usize| -> Option<i64> {
        let part = b.get(from..from + len)?;
        part.iter()
            .all(u8::is_ascii_digit)
            .then(|| std::str::from_utf8(part).ok()?.parse().ok())
            .flatten()
    };
    let year = i32::try_from(num(0, 4)?).ok()?;
    if b.get(4) != Some(&b'-') || b.get(7) != Some(&b'-') {
        return None;
    }
    let month = time::Month::try_from(u8::try_from(num(5, 2)?).ok()?).ok()?;
    let day = u8::try_from(num(8, 2)?).ok()?;
    let date = time::Date::from_calendar_date(year, month, day).ok()?;
    if b.len() == 10 {
        return Some(date.midnight().assume_utc().unix_timestamp() * 1_000_000);
    }
    if !matches!(b.get(10), Some(b'T' | b't' | b' ')) || b.get(13) != Some(&b':') {
        return None;
    }
    let hour = u8::try_from(num(11, 2)?).ok()?;
    let minute = u8::try_from(num(14, 2)?).ok()?;
    let mut at = 16;
    let (mut second, mut nanos) = (0u8, 0u32);
    let with_seconds = b.get(at) == Some(&b':');
    if with_seconds {
        second = u8::try_from(num(at + 1, 2)?).ok()?;
        at += 3;
        if b.get(at) == Some(&b'.') {
            let digits = b[at + 1..]
                .iter()
                .take_while(|c| c.is_ascii_digit())
                .count();
            if digits == 0 {
                return None;
            }
            let frac = &s[at + 1..at + 1 + digits];
            let padded: String = frac.chars().chain(std::iter::repeat('0')).take(9).collect();
            nanos = padded.parse().ok()?;
            at += 1 + digits;
        }
    }
    let time = time::Time::from_hms_nano(hour, minute, second, nanos).ok()?;
    let local = time::PrimitiveDateTime::new(date, time);
    let offset_secs: i64 = match &b[at..] {
        [] => 0,
        // An offset needs the seconds (Qdrant's formats).
        _ if !with_seconds => return None,
        [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), rest @ ..] => {
            let (h, m) = match rest.len() {
                2 => (num(at + 1, 2)?, 0),
                4 => (num(at + 1, 2)?, num(at + 3, 2)?),
                5 if rest[2] == b':' => (num(at + 1, 2)?, num(at + 4, 2)?),
                _ => return None,
            };
            if h > 23 || m > 59 {
                return None;
            }
            let secs = h * 3600 + m * 60;
            if *sign == b'-' { -secs } else { secs }
        }
        _ => return None,
    };
    let nanos = local.assume_utc().unix_timestamp_nanos() - i128::from(offset_secs) * 1_000_000_000;
    i64::try_from(nanos.div_euclid(1000)).ok()
}

#[cfg(feature = "test-util")]
pub use reference::reference_eval;

/// The reference evaluator: the semantics `compile_filter` gives, computed
/// directly over the payload (Task 4 rule 2).
#[cfg(feature = "test-util")]
mod reference {
    use serde_json::{Map, Value};

    use super::*;
    use crate::model::filter::{Range, ValuesCount};

    /// Whether point `id` with `payload` matches `filter`, as Loams
    /// evaluates the compiled query: Qdrant's condition checker, with the
    /// values at a key taken after Loams's array flattening, an integer
    /// matching an integral float (E8), text over the `standard` analyzer's
    /// tokens, and datetime ranges over the string leaves M1.1 reads as
    /// dates (Ruling 6). Errors exactly where `compile_filter` errs.
    pub fn reference_eval(
        filter: &Filter,
        id: &PrimaryKey,
        payload: &Map<String, Value>,
    ) -> Result<bool, GatewayError> {
        eval_filter(filter, id, payload)
    }

    /// A filter against one point, with Qdrant's clause rules.
    fn eval_filter(
        filter: &Filter,
        id: &PrimaryKey,
        payload: &Map<String, Value>,
    ) -> Result<bool, GatewayError> {
        let list = |l: &Option<crate::model::filter::OneOrMany<Condition>>| {
            l.as_ref()
                .map_or(&[][..], |l| l.as_slice())
                .iter()
                .map(|c| eval_condition(c, id, payload))
                .collect::<Result<Vec<bool>, _>>()
        };
        // Evaluate every list first, so errors surface as in `compile`.
        let must = list(&filter.must)?;
        let should = list(&filter.should)?;
        let must_not = list(&filter.must_not)?;
        let min = match &filter.min_should {
            None => true,
            Some(min) => {
                if min.min_count == 0 {
                    return Err(GatewayError::BadRequest(
                        "min_count must be greater than 0".to_string(),
                    ));
                }
                let hits = min
                    .conditions
                    .iter()
                    .map(|c| eval_condition(c, id, payload))
                    .collect::<Result<Vec<bool>, _>>()?;
                hits.iter().filter(|h| **h).count() >= min.min_count
            }
        };
        Ok(must.iter().all(|m| *m)
            && (should.is_empty() || should.iter().any(|s| *s))
            && min
            && !must_not.iter().any(|m| *m))
    }

    /// One condition against one point.
    fn eval_condition(
        c: &Condition,
        id: &PrimaryKey,
        payload: &Map<String, Value>,
    ) -> Result<bool, GatewayError> {
        let unsupported = |name: &str| Err(GatewayError::Unsupported(format!("{name} condition")));
        match c {
            Condition::Field(fc) => eval_field(fc, payload),
            Condition::IsEmpty { is_empty } => {
                Ok(!exists(&values(&key_path(&is_empty.key)?, payload)))
            }
            Condition::IsNull { is_null } => Ok(values(&key_path(&is_null.key)?, payload)
                .iter()
                .any(|v| v.is_null())),
            Condition::HasId { has_id } => Ok(parse_ids(has_id)?.contains(id)),
            Condition::HasVector { .. } => unsupported("has_vector"),
            Condition::Nested { .. } => unsupported("nested"),
            Condition::Slice { .. } => unsupported("slice"),
            Condition::Filter(f) => eval_filter(f, id, payload),
        }
    }

    /// The normalized keys of a payload key.
    fn key_path(key: &str) -> Result<Vec<String>, GatewayError> {
        let path: JsonPath = key.parse()?;
        Ok(path.normalized()?.split('.').map(str::to_string).collect())
    }

    /// Every non-array value at `path`, arrays flattened at every step
    /// (Loams's Json field).
    fn values<'a>(path: &[String], payload: &'a Map<String, Value>) -> Vec<&'a Value> {
        /// Collects the leaves at `path`, flattening arrays at every level.
        fn walk<'a>(path: &[String], value: &'a Value, out: &mut Vec<&'a Value>) {
            match value {
                Value::Array(items) => items.iter().for_each(|v| walk(path, v, out)),
                _ => match path.split_first() {
                    None => out.push(value),
                    Some((key, rest)) => {
                        if let Some(v) = value.as_object().and_then(|o| o.get(key)) {
                            walk(rest, v, out);
                        }
                    }
                },
            }
        }
        let mut out = Vec::new();
        if let Some((first, rest)) = path.split_first()
            && let Some(v) = payload.get(first)
        {
            walk(rest, v, &mut out);
        }
        out
    }

    /// Whether some non-null scalar lies at or below `value`.
    fn has_leaf(value: &Value) -> bool {
        match value {
            Value::Null => false,
            Value::Array(items) => items.iter().any(has_leaf),
            Value::Object(map) => map.values().any(has_leaf),
            _ => true,
        }
    }

    /// `Exists`: the key holds some non-null leaf.
    fn exists(values: &[&Value]) -> bool {
        values.iter().any(|v| has_leaf(v))
    }

    /// An integer condition matches an integral float too (E8).
    fn int_eq(v: &Value, n: i64) -> bool {
        match v {
            Value::Number(x) => {
                x.as_i64() == Some(n)
                    || (x.is_f64()
                        && x.as_f64()
                            .is_some_and(|f| f.fract() == 0.0 && f == n as f64))
            }
            _ => false,
        }
    }

    /// Whether `v` is in the list, integers by value (E8).
    fn in_list(v: &Value, list: &AnyVariants) -> bool {
        match list {
            AnyVariants::Ints(ns) => ns.iter().any(|n| int_eq(v, *n)),
            AnyVariants::Strs(ss) => v.as_str().is_some_and(|s| ss.iter().any(|x| x == s)),
        }
    }

    /// A field condition: true when any of its sub-conditions is (row T4-8).
    fn eval_field(fc: &FieldCondition, payload: &Map<String, Value>) -> Result<bool, GatewayError> {
        for (name, present) in [
            ("geo_bounding_box", fc.geo_bounding_box.is_some()),
            ("geo_radius", fc.geo_radius.is_some()),
            ("geo_polygon", fc.geo_polygon.is_some()),
        ] {
            if present {
                return Err(GatewayError::Unsupported(format!("{name} condition")));
            }
        }
        let vals = values(&key_path(&fc.key)?, payload);
        let mut parts = Vec::new();
        if let Some(m) = &fc.r#match {
            parts.push(eval_match(m, &vals));
        }
        if let Some(r) = &fc.range {
            parts.push(eval_range(r, &vals)?);
        }
        if let Some(vc) = &fc.values_count {
            parts.push(eval_count(vc, &vals));
        }
        if let Some(is_empty) = fc.is_empty {
            parts.push(exists(&vals) != is_empty);
        }
        if let Some(is_null) = fc.is_null {
            parts.push(vals.iter().any(|v| v.is_null()) == is_null);
        }
        if parts.is_empty() {
            return Err(GatewayError::BadRequest(
                "At least one field condition must be specified".to_string(),
            ));
        }
        Ok(parts.into_iter().any(|p| p))
    }

    /// `match` against the values at the key: true when any value matches.
    fn eval_match(m: &Match, vals: &[&Value]) -> bool {
        match m {
            Match::Value { value } => vals.iter().any(|v| match value {
                MatchValue::Bool(b) => v.as_bool() == Some(*b),
                MatchValue::Int(n) => int_eq(v, *n),
                MatchValue::Str(s) => v.as_str() == Some(s),
            }),
            Match::Any { any } => vals.iter().any(|v| in_list(v, any)),
            Match::Except { except } => exists(vals) && !vals.iter().any(|v| in_list(v, except)),
            Match::Prefix { prefix } => vals
                .iter()
                .any(|v| v.as_str().is_some_and(|s| s.starts_with(prefix.as_str()))),
            Match::Text { text } => text_match(vals, text, true),
            Match::TextAny { text_any } => text_match(vals, text_any, false),
            Match::Phrase { phrase } => {
                let query = tokens(phrase);
                !query.is_empty()
                    && vals
                        .iter()
                        .filter_map(|v| v.as_str())
                        .any(|s| phrase_in(&tokens(s), &query))
            }
        }
    }

    /// The `standard` analyzer's tokens with their positions.
    fn tokens(text: &str) -> Vec<(usize, String)> {
        use tantivy::tokenizer::TokenStream as _;
        let Some(mut analyzer) = loams_text::tokenizer_manager().get(loams_text::STANDARD) else {
            return Vec::new();
        };
        let mut out = Vec::new();
        analyzer
            .token_stream(text)
            .process(&mut |t| out.push((t.position, t.text.clone())));
        out
    }

    /// `Match` over the union of the tokens of every string at the key
    /// (one Tantivy field): all query tokens (`and`) or any.
    fn text_match(vals: &[&Value], text: &str, and: bool) -> bool {
        let query = tokens(text);
        if query.is_empty() {
            return false;
        }
        let doc: Vec<String> = vals
            .iter()
            .filter_map(|v| v.as_str())
            .flat_map(|s| tokens(s).into_iter().map(|(_, t)| t))
            .collect();
        let has = |t: &String| doc.contains(t);
        if and {
            query.iter().all(|(_, t)| has(t))
        } else {
            query.iter().any(|(_, t)| has(t))
        }
    }

    /// Whether the query tokens appear at their relative positions in one
    /// string (a phrase never spans two values).
    fn phrase_in(doc: &[(usize, String)], query: &[(usize, String)]) -> bool {
        let base = query[0].0;
        doc.iter().any(|(start, first)| {
            *first == query[0].1
                && query.iter().all(|(p, t)| {
                    doc.iter()
                        .any(|(dp, dt)| *dp == start + (p - base) && dt == t)
                })
        })
    }

    /// Whether `x` satisfies every bound of `r`.
    fn check<T: PartialOrd + Copy>(x: T, r: &Range<T>) -> bool {
        r.gt.is_none_or(|b| x > b)
            && r.gte.is_none_or(|b| x >= b)
            && r.lt.is_none_or(|b| x < b)
            && r.lte.is_none_or(|b| x <= b)
    }

    /// `range` against the values at the key: numbers, or date strings for
    /// datetime bounds.
    fn eval_range(r: &RangeInterface, vals: &[&Value]) -> Result<bool, GatewayError> {
        match r {
            RangeInterface::Number(r) => {
                if r.gt.is_none() && r.gte.is_none() && r.lt.is_none() && r.lte.is_none() {
                    // A range without bounds is `Exists` in the IR.
                    return Ok(exists(vals));
                }
                Ok(vals.iter().any(|v| v.as_f64().is_some_and(|x| check(x, r))))
            }
            RangeInterface::Datetime(r) => {
                let b = |s: &Option<String>| s.as_deref().map(parse_datetime).transpose();
                let r = Range {
                    gt: b(&r.gt)?,
                    gte: b(&r.gte)?,
                    lt: b(&r.lt)?,
                    lte: b(&r.lte)?,
                };
                Ok(vals.iter().any(|v| {
                    // The date companion holds the string leaves that
                    // M1.1's `parse_date` reads, digit strings aside, at ms
                    // precision.
                    v.as_str().is_some_and(|s| {
                        !s.bytes().all(|c| c.is_ascii_digit())
                            && loams_collection::parse_date(v).is_ok_and(|ms| check(ms * 1000, &r))
                    })
                }))
            }
        }
    }

    /// The IR's `ValuesCount`: the non-null values at the key (an object
    /// counts once), or-ed with `IsEmpty` when the bounds admit 0.
    fn eval_count(vc: &ValuesCount, vals: &[&Value]) -> bool {
        let r = Range {
            gt: vc.gt,
            gte: vc.gte,
            lt: vc.lt,
            lte: vc.lte,
        };
        let n = vals.iter().filter(|v| !v.is_null()).count() as u64;
        (n > 0 && check(n, &r)) || (check(0, &r) && !exists(vals))
    }
}
