//! The reference evaluator of the differential DSL tests (behind
//! `test-util`): Elasticsearch's semantics for the filter leaves over the
//! non-text field types, computed directly over a document's `_source`,
//! independently of the parser and the engine (the approach of
//! loams-qdrant's `filter::reference_eval`).
//!
//! Covered: `match_all`, `match_none`, `bool`, `constant_score`, `term`,
//! `terms`, `ids`, `range`, `exists`, `prefix` and `wildcard` over
//! `keyword`, `long`, `double`, `boolean`, `date` and `flattened` paths
//! (with Loams's numeric flattened ranges, row E11), and `_id`. Anything
//! else is an `Err` with kind `unsupported_by_reference`, so a corpus
//! never relies on it.

use loams_collection::FieldKind;
use serde_json::{Map, Value};

use super::datemath::{Rounding, epoch_millis, parse_date, parse_date_math};
use super::value::{FieldRef, resolve, scalar_text};
use crate::doc::glob_match;
use crate::error::EsError;
use crate::mapping::IndexView;

fn unsupported(what: &str) -> EsError {
    EsError::new(400, "unsupported_by_reference", what.to_string())
}

fn input_error(v: &Value) -> EsError {
    EsError::new(
        400,
        "query_shard_exception",
        format!(
            "failed to create query: For input string: \"{}\"",
            scalar_text(v).unwrap_or_default()
        ),
    )
}

/// Whether document `id` with `source` matches `query`, as Elasticsearch
/// evaluates it (module docs). Errors where the query is invalid.
pub fn reference_eval(
    query: &Value,
    view: &IndexView,
    id: &str,
    source: &Map<String, Value>,
    now_ms: i64,
) -> Result<bool, EsError> {
    Eval {
        view,
        id,
        source,
        now_ms,
    }
    .query(query)
}

struct Eval<'a> {
    view: &'a IndexView,
    id: &'a str,
    source: &'a Map<String, Value>,
    now_ms: i64,
}

/// The one `(key, value)` of an object.
fn single(v: &Value) -> Result<(&str, &Value), EsError> {
    let map = v.as_object().ok_or_else(|| unsupported("not an object"))?;
    let mut it = map.iter().filter(|(k, _)| *k != "boost" && *k != "_name");
    let (k, v) = it.next().ok_or_else(|| unsupported("empty"))?;
    if it.next().is_some() {
        return Err(unsupported("several keys"));
    }
    Ok((k.as_str(), v))
}

/// The leaves at the dot path `path`, arrays flattened.
fn leaves<'v>(source: &'v Map<String, Value>, path: &str) -> Vec<&'v Value> {
    fn walk<'v>(v: &'v Value, rest: &[&str], out: &mut Vec<&'v Value>) {
        match v {
            Value::Array(items) => items.iter().for_each(|i| walk(i, rest, out)),
            Value::Null => {}
            Value::Object(map) => {
                if let Some((first, tail)) = rest.split_first()
                    && let Some(next) = map.get(*first)
                {
                    walk(next, tail, out);
                }
            }
            scalar => {
                if rest.is_empty() {
                    out.push(scalar);
                }
            }
        }
    }
    let segments: Vec<&str> = path.split('.').collect();
    let mut out = Vec::new();
    if let Some((first, tail)) = segments.split_first()
        && let Some(v) = source.get(*first)
    {
        walk(v, tail, &mut out);
    }
    out
}

/// Whether a non-null scalar lies at or below the dot path `path`.
fn present(source: &Map<String, Value>, path: &str) -> bool {
    fn below(v: &Value) -> bool {
        match v {
            Value::Null => false,
            Value::Array(items) => items.iter().any(below),
            Value::Object(map) => map.values().any(below),
            _ => true,
        }
    }
    fn walk(v: &Value, rest: &[&str]) -> bool {
        match (v, rest.split_first()) {
            (Value::Array(items), _) => items.iter().any(|i| walk(i, rest)),
            (v, None) => below(v),
            (Value::Object(map), Some((first, tail))) => {
                map.get(*first).is_some_and(|n| walk(n, tail))
            }
            _ => false,
        }
    }
    let segments: Vec<&str> = path.split('.').collect();
    segments
        .split_first()
        .is_some_and(|(first, tail)| source.get(*first).is_some_and(|v| walk(v, tail)))
}

/// A stored leaf of a `date` field in µs.
fn stored_date(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().map(|ms| ms * 1000),
        Value::String(s) => parse_date(s, Rounding::Down)
            .ok()
            .or_else(|| epoch_millis(s)),
        _ => None,
    }
}

/// A stored leaf of a numeric field.
fn stored_number(kind: &FieldKind, v: &Value) -> Option<f64> {
    let x = match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }?;
    Some(if *kind == FieldKind::I64 {
        x.trunc()
    } else {
        x
    })
}

/// A query value of a numeric field.
fn query_number(v: &Value) -> Result<f64, EsError> {
    let x = match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    };
    x.filter(|x| x.is_finite()).ok_or_else(|| input_error(v))
}

fn stored_bool(v: &Value) -> Option<bool> {
    match v {
        Value::Bool(b) => Some(*b),
        Value::String(s) if s == "true" => Some(true),
        Value::String(s) if s == "false" || s.is_empty() => Some(false),
        _ => None,
    }
}

impl Eval<'_> {
    fn query(&self, q: &Value) -> Result<bool, EsError> {
        let (name, body) = single(q)?;
        match name {
            "match_all" => Ok(true),
            "match_none" => Ok(false),
            "constant_score" => {
                self.query(body.get("filter").ok_or_else(|| unsupported("no filter"))?)
            }
            "bool" => self.bool_query(body),
            "ids" => {
                let values = body["values"]
                    .as_array()
                    .ok_or_else(|| unsupported("ids"))?;
                Ok(values
                    .iter()
                    .any(|v| scalar_text(v).as_deref() == Some(self.id)))
            }
            "term" => {
                let (field, v) = single(body)?;
                let v = v.get("value").unwrap_or(v);
                self.term(field, v)
            }
            "terms" => {
                let (field, values) = single(body)?;
                let values = values.as_array().ok_or_else(|| unsupported("terms"))?;
                let mut any = false;
                for v in values {
                    any |= self.term(field, v)?;
                }
                Ok(any)
            }
            "range" => {
                let (field, params) = single(body)?;
                self.range(field, params)
            }
            "exists" => {
                let field = body["field"]
                    .as_str()
                    .ok_or_else(|| unsupported("exists"))?;
                self.exists(field)
            }
            "prefix" | "wildcard" => {
                let (field, v) = single(body)?;
                let v = v.get("value").or_else(|| v.get("wildcard")).unwrap_or(v);
                let pattern = scalar_text(v).ok_or_else(|| unsupported("pattern"))?;
                let texts = self.keyword_texts(field)?;
                Ok(texts.iter().any(|t| {
                    if name == "prefix" {
                        t.starts_with(&pattern)
                    } else {
                        wildcard(&pattern, t)
                    }
                }))
            }
            other => Err(unsupported(other)),
        }
    }

    fn bool_query(&self, body: &Value) -> Result<bool, EsError> {
        let list = |key: &str| -> Result<Vec<bool>, EsError> {
            match body.get(key) {
                None => Ok(Vec::new()),
                Some(Value::Array(items)) => items.iter().map(|q| self.query(q)).collect(),
                Some(q) => Ok(vec![self.query(q)?]),
            }
        };
        let must = list("must")?;
        let filter = list("filter")?;
        let should = list("should")?;
        let must_not = list("must_not")?;
        let hits = should.iter().filter(|s| **s).count();
        let required = match body.get("minimum_should_match") {
            Some(v) => {
                let n: i64 = scalar_text(v)
                    .and_then(|t| t.parse().ok())
                    .ok_or_else(|| unsupported("minimum_should_match"))?;
                // ES never requires more than the optional clauses.
                let n = if n < 0 { should.len() as i64 + n } else { n };
                (n.max(0) as usize).min(should.len())
            }
            None if must.is_empty() && filter.is_empty() => usize::from(!should.is_empty()),
            None => 0,
        };
        Ok(must.iter().all(|m| *m)
            && filter.iter().all(|f| *f)
            && !must_not.iter().any(|m| *m)
            && hits >= required)
    }

    /// The texts a keyword-like field holds.
    fn keyword_texts(&self, field: &str) -> Result<Vec<String>, EsError> {
        match resolve(self.view, field) {
            FieldRef::Field(spec) if spec.kind == FieldKind::Keyword => {
                Ok(leaves(self.source, &spec.source_path)
                    .into_iter()
                    .filter_map(scalar_text)
                    .collect())
            }
            FieldRef::Json { spec, path } => {
                Ok(leaves(self.source, &format!("{}.{path}", spec.source_path))
                    .into_iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect())
            }
            FieldRef::Unmapped => Ok(Vec::new()),
            _ => Err(unsupported("pattern on this field")),
        }
    }

    fn term(&self, field: &str, v: &Value) -> Result<bool, EsError> {
        match resolve(self.view, field) {
            FieldRef::Unmapped => Ok(false),
            FieldRef::Id => Ok(scalar_text(v).as_deref() == Some(self.id)),
            FieldRef::Vector(_) => Err(unsupported("vector")),
            FieldRef::Json { spec, path } => {
                let stored = leaves(self.source, &format!("{}.{path}", spec.source_path));
                Ok(stored.iter().any(|s| match (v, s) {
                    (Value::Number(a), Value::Number(b)) => a.as_f64() == b.as_f64(),
                    (Value::String(a), Value::String(b)) => a == b,
                    (Value::Bool(a), Value::Bool(b)) => a == b,
                    _ => false,
                }))
            }
            FieldRef::Field(spec) => {
                let stored = leaves(self.source, &spec.source_path);
                match &spec.kind {
                    FieldKind::Keyword => {
                        let want = scalar_text(v);
                        Ok(stored.iter().any(|s| scalar_text(s) == want))
                    }
                    FieldKind::I64 | FieldKind::F64 => {
                        let want = query_number(v)?;
                        Ok(stored
                            .iter()
                            .filter_map(|s| stored_number(&spec.kind, s))
                            .any(|x| x == want))
                    }
                    FieldKind::Bool => {
                        let want = stored_bool(v).ok_or_else(|| {
                            EsError::new(400, "query_shard_exception", "bad boolean")
                        })?;
                        Ok(stored.iter().any(|s| stored_bool(s) == Some(want)))
                    }
                    FieldKind::Date => {
                        let (low, high) = self.date_span(v)?;
                        Ok(stored
                            .iter()
                            .filter_map(|s| stored_date(s))
                            .any(|x| low <= x && x <= high))
                    }
                    _ => Err(unsupported("term on this field type")),
                }
            }
        }
    }

    /// The µs a date query value covers.
    fn date_span(&self, v: &Value) -> Result<(i64, i64), EsError> {
        match v {
            Value::Number(n) => {
                let ms = n.as_i64().ok_or_else(|| input_error(v))?;
                Ok((ms * 1000, ms * 1000))
            }
            Value::String(s) => Ok((
                parse_date_math(s, self.now_ms, Rounding::Down)?,
                parse_date_math(s, self.now_ms, Rounding::Up)?,
            )),
            _ => Err(input_error(v)),
        }
    }

    fn date_bound(&self, v: &Value, round: Rounding) -> Result<i64, EsError> {
        match v {
            Value::Number(n) => n.as_i64().map(|ms| ms * 1000).ok_or_else(|| input_error(v)),
            Value::String(s) => parse_date_math(s, self.now_ms, round),
            _ => Err(input_error(v)),
        }
    }

    fn range(&self, field: &str, params: &Value) -> Result<bool, EsError> {
        let get = |k: &str| params.get(k).filter(|v| !v.is_null());
        let (gt, gte, lt, lte) = (get("gt"), get("gte"), get("lt"), get("lte"));
        if gt.is_none() && gte.is_none() && lt.is_none() && lte.is_none() {
            // A range without bounds is `exists`.
            return self.exists(field);
        }
        match resolve(self.view, field) {
            FieldRef::Unmapped => Ok(false),
            FieldRef::Json { spec, path } => {
                let stored = leaves(self.source, &format!("{}.{path}", spec.source_path));
                let bounds = [gt, gte, lt, lte];
                let numeric = bounds.iter().flatten().all(|b| b.is_number());
                let textual = bounds.iter().flatten().all(|b| b.is_string());
                if numeric {
                    let b = |v: Option<&Value>| v.and_then(Value::as_f64);
                    let (gt, gte, lt, lte) = (b(gt), b(gte), b(lt), b(lte));
                    Ok(stored.iter().filter_map(|s| s.as_f64()).any(|x| {
                        gt.is_none_or(|b| x > b)
                            && gte.is_none_or(|b| x >= b)
                            && lt.is_none_or(|b| x < b)
                            && lte.is_none_or(|b| x <= b)
                    }))
                } else if textual {
                    fn b(v: Option<&Value>) -> Option<&str> {
                        v.and_then(Value::as_str)
                    }
                    let (gt, gte, lt, lte) = (b(gt), b(gte), b(lt), b(lte));
                    Ok(stored.iter().filter_map(|s| s.as_str()).any(|x| {
                        gt.is_none_or(|b| x > b)
                            && gte.is_none_or(|b| x >= b)
                            && lt.is_none_or(|b| x < b)
                            && lte.is_none_or(|b| x <= b)
                    }))
                } else {
                    Err(unsupported("mixed flattened bounds"))
                }
            }
            FieldRef::Field(spec) => {
                let stored = leaves(self.source, &spec.source_path);
                match &spec.kind {
                    FieldKind::I64 | FieldKind::F64 => {
                        let b = |v: Option<&Value>| v.map(query_number).transpose();
                        let (gt, gte, lt, lte) = (b(gt)?, b(gte)?, b(lt)?, b(lte)?);
                        Ok(stored
                            .iter()
                            .filter_map(|s| stored_number(&spec.kind, s))
                            .any(|x| {
                                gt.is_none_or(|b| x > b)
                                    && gte.is_none_or(|b| x >= b)
                                    && lt.is_none_or(|b| x < b)
                                    && lte.is_none_or(|b| x <= b)
                            }))
                    }
                    FieldKind::Date => {
                        let b = |v: Option<&Value>, r| v.map(|v| self.date_bound(v, r)).transpose();
                        let (gt, gte, lt, lte) = (
                            b(gt, Rounding::Up)?,
                            b(gte, Rounding::Down)?,
                            b(lt, Rounding::Down)?,
                            b(lte, Rounding::Up)?,
                        );
                        Ok(stored.iter().filter_map(|s| stored_date(s)).any(|x| {
                            gt.is_none_or(|b| x > b)
                                && gte.is_none_or(|b| x >= b)
                                && lt.is_none_or(|b| x < b)
                                && lte.is_none_or(|b| x <= b)
                        }))
                    }
                    FieldKind::Keyword => {
                        let b = |v: Option<&Value>| v.and_then(scalar_text);
                        let (gt, gte, lt, lte) = (b(gt), b(gte), b(lt), b(lte));
                        Ok(stored.iter().filter_map(|s| scalar_text(s)).any(|x| {
                            gt.as_ref().is_none_or(|b| x > *b)
                                && gte.as_ref().is_none_or(|b| x >= *b)
                                && lt.as_ref().is_none_or(|b| x < *b)
                                && lte.as_ref().is_none_or(|b| x <= *b)
                        }))
                    }
                    _ => Err(unsupported("range on this field type")),
                }
            }
            _ => Err(unsupported("range on this field")),
        }
    }

    fn exists(&self, field: &str) -> Result<bool, EsError> {
        match resolve(self.view, field) {
            FieldRef::Id => Ok(true),
            FieldRef::Field(spec) => Ok(present(self.source, &spec.source_path)),
            FieldRef::Json { spec, path } => Ok(present(
                self.source,
                &format!("{}.{path}", spec.source_path),
            )),
            FieldRef::Vector(_) => Err(unsupported("exists on a vector")),
            FieldRef::Unmapped => {
                let prefix = format!("{field}.");
                Ok(self
                    .view
                    .info
                    .schema
                    .fields
                    .iter()
                    .filter(|spec| spec.name.starts_with(&prefix))
                    .any(|spec| present(self.source, &spec.source_path)))
            }
        }
    }
}

/// ES wildcard: `*` any run, `?` one character.
fn wildcard(pattern: &str, text: &str) -> bool {
    if !pattern.contains('?') {
        return glob_match(pattern, text);
    }
    fn go(p: &[char], t: &[char]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some(('*', rest)) => (0..=t.len()).any(|i| go(rest, &t[i..])),
            Some(('?', rest)) => !t.is_empty() && go(rest, &t[1..]),
            Some((c, rest)) => t.first() == Some(c) && go(rest, &t[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    go(&p, &t)
}
