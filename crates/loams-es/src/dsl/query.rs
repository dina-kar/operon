//! The query parser (plan M1.5 Task 7 items 1–5).

use loams_collection::{FieldKind, KNOWN_ANALYZERS, PrimaryKey};
use loams_query::{BoolOperator, FieldValue, Fuzziness, MultiMatchKind, Query};
use serde_json::{Map, Value};

use super::datemath::Rounding;
use super::value::{
    FieldRef, check_searchable, coerce_rounded, es_type, is_searchable, is_text_like, json_class,
    resolve, scalar_text, shard_error,
};
use super::{
    KnnSpec, MAX_DEPTH, MAX_NUM_CANDIDATES, MAX_TERMS, ParsedQuery, QueryContext,
    check_query_vector,
};
use crate::doc::glob_match;
use crate::error::EsError;
use crate::mapping::IndexView;

/// Phase B queries (D48): refused with one reason.
const PHASE_B: &[&str] = &[
    "match_phrase_prefix",
    "match_bool_prefix",
    "regexp",
    "nested",
    "has_child",
    "has_parent",
    "parent_id",
    "function_score",
    "more_like_this",
    "simple_query_string",
    "dis_max",
    "boosting",
    "geo_bounding_box",
    "geo_distance",
    "geo_grid",
    "geo_polygon",
    "geo_shape",
    "shape",
    "percolate",
    "span_term",
    "span_near",
    "span_or",
    "span_not",
    "span_first",
    "span_multi",
    "span_containing",
    "span_within",
    "field_masking_span",
    "intervals",
    "distance_feature",
    "rank_feature",
    "terms_set",
    "combined_fields",
    "wrapper",
];

/// Queries that need a script engine or an ML model (Ruling 13).
const NO_SCRIPT_OR_ML: &[&str] = &[
    "script",
    "sparse_vector",
    "text_expansion",
    "semantic",
    "weighted_tokens",
    "rule",
    "pinned",
];

/// The error of a `knn` clause outside the places it may stand.
fn knn_placement() -> EsError {
    EsError::illegal_argument(
        "[knn] queries are only supported as the top-level query or a top-level [bool] should \
         clause (Loams Phase A)",
    )
}

/// ES's name of a JSON value's first token.
pub(crate) fn token_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "VALUE_NULL",
        Value::Bool(_) => "VALUE_BOOLEAN",
        Value::Number(_) => "VALUE_NUMBER",
        Value::String(_) => "VALUE_STRING",
        Value::Array(_) => "START_ARRAY",
        Value::Object(_) => "START_OBJECT",
    }
}

/// A number (or a numeric string) as an f32.
pub(crate) fn number_f32(query: &str, key: &str, v: &Value) -> Result<f32, EsError> {
    let x = match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    };
    x.filter(|x| x.is_finite())
        .map(|x| x as f32)
        .ok_or_else(|| EsError::parsing(format!("[{query}] [{key}] must be a number, found [{v}]")))
}

/// An integer (or an integer string, or an integral float).
pub(crate) fn number_int(query: &str, key: &str, v: &Value) -> Result<i64, EsError> {
    let n = match v {
        Value::Number(n) => n
            .as_i64()
            .or_else(|| n.as_f64().filter(|x| x.fract() == 0.0).map(|x| x as i64)),
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    };
    n.ok_or_else(|| EsError::parsing(format!("[{query}] [{key}] must be an integer, found [{v}]")))
}

fn boolean(query: &str, key: &str, v: &Value) -> Result<bool, EsError> {
    match v {
        Value::Bool(b) => Ok(*b),
        Value::String(s) if s == "true" => Ok(true),
        Value::String(s) if s == "false" => Ok(false),
        _ => Err(EsError::parsing(format!(
            "[{query}] [{key}] must be a boolean, found [{v}]"
        ))),
    }
}

/// `or` / `and`, in any case.
pub(crate) fn operator(v: &Value) -> Result<BoolOperator, EsError> {
    match v.as_str().map(str::to_ascii_lowercase).as_deref() {
        Some("or") => Ok(BoolOperator::Or),
        Some("and") => Ok(BoolOperator::And),
        _ => Err(EsError::illegal_argument(format!(
            "No enum constant org.elasticsearch.index.query.Operator.{}",
            scalar_text(v).unwrap_or_default().to_ascii_uppercase()
        ))),
    }
}

/// `minimum_should_match`: an integer, a negative integer, `N%` or `-N%`.
fn minimum_should_match(v: &Value) -> Result<String, EsError> {
    let text = scalar_text(v).unwrap_or_default();
    let digits = text.strip_prefix('-').unwrap_or(&text);
    let digits = digits.strip_suffix('%').unwrap_or(digits);
    if matches!(v, Value::Number(_) | Value::String(_))
        && !digits.is_empty()
        && digits.bytes().all(|b| b.is_ascii_digit())
    {
        return Ok(text);
    }
    Err(EsError::unsupported(&format!(
        "minimum_should_match: {text}"
    )))
}

/// `fuzziness`: `AUTO`, `AUTO:3,6` or 0–2.
fn fuzziness(v: &Value) -> Result<Fuzziness, EsError> {
    let text = scalar_text(v).unwrap_or_default();
    match text.to_ascii_uppercase().as_str() {
        "AUTO" | "AUTO:3,6" => Ok(Fuzziness::Auto),
        "0" => Ok(Fuzziness::Edits(0)),
        "1" => Ok(Fuzziness::Edits(1)),
        "2" => Ok(Fuzziness::Edits(2)),
        _ => Err(EsError::unsupported(&format!("fuzziness: {text}"))),
    }
}

fn and_queries(mut parts: Vec<Query>) -> Option<Query> {
    match parts.len() {
        0 => None,
        1 => parts.pop(),
        _ => Some(Query::Bool {
            must: Vec::new(),
            should: Vec::new(),
            must_not: Vec::new(),
            filter: parts,
            minimum_should_match: None,
        }),
    }
}

/// `query` with `boost` applied (item 1): wrapped unless it is 1.0.
fn boosted(query: Query, boost: f32) -> Query {
    if boost == 1.0 {
        query
    } else {
        Query::Boost {
            query: Box::new(query),
            boost,
        }
    }
}

/// The parser of one query, over one index.
pub(crate) struct Parser<'a> {
    ctx: QueryContext<'a>,
}

impl<'a> Parser<'a> {
    pub(crate) fn new(ctx: &QueryContext<'a>) -> Self {
        Self { ctx: *ctx }
    }

    pub(super) fn view(&self) -> &'a IndexView {
        self.ctx.view
    }

    pub(super) fn object<'v>(
        &self,
        query: &str,
        v: &'v Value,
    ) -> Result<&'v Map<String, Value>, EsError> {
        v.as_object().ok_or_else(|| {
            EsError::parsing(format!(
                "[{query}] query malformed, no start_object after query name"
            ))
        })
    }

    pub(super) fn does_not_support(&self, query: &str, param: &str) -> EsError {
        EsError::parsing(format!("[{query}] query does not support [{param}]"))
    }

    pub(super) fn boost_value(&self, query: &str, v: &Value) -> Result<f32, EsError> {
        let boost = number_f32(query, "boost", v)?;
        if boost < 0.0 {
            return Err(EsError::illegal_argument(
                "negative [boost] are not allowed.",
            ));
        }
        Ok(boost)
    }

    /// The one key of a query object (item 1).
    fn clause<'v>(&self, v: &'v Value, depth: u32) -> Result<(&'v str, &'v Value), EsError> {
        if depth > MAX_DEPTH {
            return Err(EsError::illegal_argument(
                "The nested depth of the query exceeds the maximum nested depth for queries set \
                 in [indices.query.bool.max_nested_depth]",
            ));
        }
        let Value::Object(map) = v else {
            return Err(EsError::parsing(format!(
                "[_na] query malformed, must start with start_object, found [{}]",
                token_name(v)
            )));
        };
        let mut keys = map.iter();
        let Some((name, body)) = keys.next() else {
            return Err(EsError::parsing("query malformed, empty clause found"));
        };
        if keys.next().is_some() {
            return Err(EsError::parsing(format!(
                "[{name}] malformed query, expected [END_OBJECT] but found [FIELD_NAME]"
            )));
        }
        Ok((name.as_str(), body))
    }

    /// A query where knn and script_score are not allowed.
    pub(super) fn leaf(&self, v: &Value, depth: u32) -> Result<Query, EsError> {
        let (name, body) = self.clause(v, depth)?;
        match name {
            "knn" => Err(knn_placement()),
            "script_score" => Err(EsError::illegal_argument(
                "[script_score] queries are only supported as the top-level query (Loams \
                 Phase A)",
            )),
            _ => self.named(name, body, depth),
        }
    }

    /// The top-level query (items 5 and 6).
    fn top(&self, v: &Value) -> Result<ParsedQuery, EsError> {
        let depth = self.ctx.depth + 1;
        let (name, body) = self.clause(v, depth)?;
        match name {
            "knn" => Ok(ParsedQuery {
                query: None,
                knn: vec![self.knn_spec(body, depth, self.ctx.default_k)?],
                script: None,
            }),
            "script_score" => Ok(ParsedQuery {
                query: None,
                knn: Vec::new(),
                script: Some(self.script_score(body, depth)?),
            }),
            "bool" => {
                let mut knn = Vec::new();
                let query = self.bool_query(body, depth, Some(&mut knn))?;
                Ok(ParsedQuery {
                    query,
                    knn,
                    script: None,
                })
            }
            _ => Ok(ParsedQuery {
                query: Some(self.named(name, body, depth)?),
                knn: Vec::new(),
                script: None,
            }),
        }
    }

    fn named(&self, name: &str, body: &Value, depth: u32) -> Result<Query, EsError> {
        match name {
            "match_all" => self.match_all(body),
            "match_none" => {
                self.only_name("match_none", body)?;
                Ok(Query::MatchNone)
            }
            "match" => self.match_query(body, false),
            "match_phrase" => self.match_query(body, true),
            "multi_match" => self.multi_match(body),
            "term" => self.term(body),
            "terms" => self.terms(body),
            "ids" => self.ids(body),
            "range" => self.range(body),
            "exists" => self.exists(body),
            "prefix" | "wildcard" | "fuzzy" => self.pattern(name, body),
            "query_string" => self.query_string(body),
            "constant_score" => self.constant_score(body, depth),
            "bool" => Ok(self
                .bool_query(body, depth, None)?
                .unwrap_or_else(empty_bool)),
            _ if PHASE_B.contains(&name) => Err(EsError::unsupported(name)),
            _ if NO_SCRIPT_OR_ML.contains(&name) => Err(EsError::illegal_argument(format!(
                "Loams does not support [{name}] queries (no scripting or ML inference)"
            ))),
            _ => Err(EsError::parsing(format!("unknown query [{name}]"))),
        }
    }

    fn only_name(&self, query: &str, body: &Value) -> Result<(), EsError> {
        for key in self.object(query, body)?.keys() {
            if key != "_name" {
                return Err(self.does_not_support(query, key));
            }
        }
        Ok(())
    }

    fn match_all(&self, body: &Value) -> Result<Query, EsError> {
        let mut boost = 1.0;
        for (key, value) in self.object("match_all", body)? {
            match key.as_str() {
                "boost" => boost = self.boost_value("match_all", value)?,
                "_name" => {}
                _ => return Err(self.does_not_support("match_all", key)),
            }
        }
        Ok(boosted(Query::MatchAll, boost))
    }

    /// `{"<query>": {"<field>": <body>}}`: the field and its body.
    fn field_body<'v>(
        &self,
        query: &str,
        body: &'v Value,
    ) -> Result<(&'v str, &'v Value), EsError> {
        let map = self.object(query, body)?;
        let mut keys = map.iter();
        let Some((field, value)) = keys.next() else {
            return Err(EsError::parsing(format!(
                "[{query}] query malformed, no field"
            )));
        };
        if let Some((other, _)) = keys.next() {
            return Err(EsError::parsing(format!(
                "[{query}] query doesn't support multiple fields, found [{field}] and [{other}]"
            )));
        }
        Ok((field.as_str(), value))
    }

    /// A term on `field` (item 3 `term`; `match` on a non-text field).
    fn term_on(&self, field: &str, value: &Value) -> Result<Query, EsError> {
        match resolve(self.view(), field) {
            FieldRef::Unmapped => Ok(Query::MatchNone),
            FieldRef::Id => Ok(Query::Ids(vec![id_key(value)?])),
            FieldRef::Vector(_) => Err(shard_error(format!(
                "Field [{field}] of type [dense_vector] does not support term queries"
            ))),
            FieldRef::Json { .. } => Ok(Query::Term {
                field: field.to_string(),
                value: json_class(value),
            }),
            FieldRef::Field(spec) => {
                check_searchable(self.view(), field, spec)?;
                if spec.kind == FieldKind::Date {
                    return self.date_term(field, value);
                }
                let coerced =
                    coerce_rounded(self.view(), field, value, self.ctx.now_ms, Rounding::Down)?
                        .unwrap_or(FieldValue::Str(String::new()));
                Ok(Query::Term {
                    field: field.to_string(),
                    value: coerced,
                })
            }
        }
    }

    /// A term on a date field: the instants the date names, as ES's
    /// `DateFieldType.termQuery` does (`2026-09-24` is the whole day).
    fn date_term(&self, field: &str, value: &Value) -> Result<Query, EsError> {
        let at = |round| coerce_rounded(self.view(), field, value, self.ctx.now_ms, round);
        let (low, high) = (at(Rounding::Down)?, at(Rounding::Up)?);
        Ok(match (low, high) {
            (Some(low), Some(high)) if low != high => Query::Range {
                field: field.to_string(),
                gt: None,
                gte: Some(low),
                lt: None,
                lte: Some(high),
            },
            (Some(low), _) => Query::Term {
                field: field.to_string(),
                value: low,
            },
            _ => Query::MatchNone,
        })
    }

    fn match_query(&self, body: &Value, phrase: bool) -> Result<Query, EsError> {
        let q = if phrase { "match_phrase" } else { "match" };
        let (field, v) = self.field_body(q, body)?;
        let mut text = None;
        let mut operator_ = BoolOperator::Or;
        let mut msm = None;
        let mut fuzz = None;
        let mut analyzer = None;
        let mut lenient = false;
        let mut boost = 1.0;
        let mut slop = 0u32;
        match v {
            Value::Object(params) => {
                for (key, value) in params {
                    match (key.as_str(), phrase) {
                        ("query", _) => text = Some(value),
                        ("boost", _) => boost = self.boost_value(q, value)?,
                        ("_name", _) => {}
                        ("analyzer", _) => {
                            let name = value.as_str().unwrap_or_default();
                            if !KNOWN_ANALYZERS.contains(&name) {
                                return Err(EsError::illegal_argument(format!(
                                    "[{q}] analyzer [{name}] not found"
                                )));
                            }
                            analyzer = Some(name.to_string());
                        }
                        ("zero_terms_query", _) => {
                            if !value
                                .as_str()
                                .is_some_and(|s| s.eq_ignore_ascii_case("none"))
                            {
                                return Err(EsError::unsupported(&format!(
                                    "{q}.zero_terms_query: {}",
                                    scalar_text(value).unwrap_or_default()
                                )));
                            }
                        }
                        ("slop", true) => {
                            slop = u32::try_from(number_int(q, key, value)?).map_err(|_| {
                                EsError::illegal_argument("No negative slop allowed.")
                            })?;
                        }
                        ("operator", false) => operator_ = operator(value)?,
                        ("minimum_should_match", false) => msm = Some(minimum_should_match(value)?),
                        ("fuzziness", false) => fuzz = Some(fuzziness(value)?),
                        ("lenient", false) => lenient = boolean(q, key, value)?,
                        ("prefix_length", false) => {
                            if number_int(q, key, value)? != 0 {
                                return Err(EsError::unsupported("match.prefix_length"));
                            }
                        }
                        ("max_expansions", false) => {
                            if number_int(q, key, value)? != 50 {
                                return Err(EsError::unsupported("match.max_expansions"));
                            }
                        }
                        ("fuzzy_transpositions", false) => {
                            if !boolean(q, key, value)? {
                                return Err(EsError::unsupported("match.fuzzy_transpositions"));
                            }
                        }
                        _ => return Err(self.does_not_support(q, key)),
                    }
                }
            }
            other => text = Some(other),
        }
        let Some(text) = text.filter(|t| !t.is_null()) else {
            return Err(EsError::parsing(format!("[{q}] requires query value")));
        };
        let Some(query_text) = scalar_text(text) else {
            return Err(EsError::parsing(format!(
                "[{q}] unknown token [{}] after [query]",
                token_name(text)
            )));
        };
        let query = match resolve(self.view(), field) {
            FieldRef::Unmapped => Query::MatchNone,
            FieldRef::Field(spec) if matches!(spec.kind, FieldKind::Text { .. }) => {
                check_searchable(self.view(), field, spec)?;
                if phrase {
                    Query::MatchPhrase {
                        field: field.to_string(),
                        text: query_text,
                        slop,
                    }
                } else {
                    Query::Match {
                        field: field.to_string(),
                        text: query_text,
                        operator: operator_,
                        minimum_should_match: msm,
                        fuzziness: fuzz,
                        analyzer,
                    }
                }
            }
            FieldRef::Field(spec)
                if fuzz.is_some() && matches!(spec.kind, FieldKind::Keyword | FieldKind::Uuid) =>
            {
                check_searchable(self.view(), field, spec)?;
                Query::Fuzzy {
                    field: field.to_string(),
                    value: query_text,
                    fuzziness: fuzz.unwrap_or(Fuzziness::Auto),
                }
            }
            FieldRef::Field(spec) if fuzz.is_some() => {
                return Err(shard_error(format!(
                    "Can only use fuzzy queries on keyword and text fields - not on [{field}] \
                     which is of type [{}]",
                    es_type(self.view(), &spec.name)
                )));
            }
            _ => match self.term_on(field, text) {
                Ok(query) => query,
                Err(err) if lenient && err.kind == "query_shard_exception" => Query::MatchNone,
                Err(err) if lenient && err.kind == "parse_exception" => Query::MatchNone,
                Err(err) => return Err(err),
            },
        };
        Ok(boosted(query, boost))
    }

    /// Fields named by a `fields` list: `f` or `f^boost`, wildcards
    /// expanded over the searchable text and keyword fields.
    fn expand_fields(&self, query: &str, list: &[String]) -> Result<Vec<(String, f32)>, EsError> {
        let mut out: Vec<(String, f32)> = Vec::new();
        for entry in list {
            let (name, boost) = match entry.rsplit_once('^') {
                Some((name, boost)) => (
                    name,
                    boost.parse::<f32>().map_err(|_| {
                        EsError::parsing(format!("[{query}] invalid field boost [{entry}]"))
                    })?,
                ),
                None => (entry.as_str(), 1.0),
            };
            if name.contains('*') {
                for spec in &self.view().info.schema.fields {
                    if glob_match(name, &spec.name)
                        && is_text_like(self.view(), spec)
                        && !out.iter().any(|(n, _)| *n == spec.name)
                    {
                        out.push((spec.name.clone(), boost));
                    }
                }
                continue;
            }
            match resolve(self.view(), name) {
                FieldRef::Unmapped => {}
                FieldRef::Json { .. } => out.push((name.to_string(), boost)),
                FieldRef::Field(spec)
                    if matches!(spec.kind, FieldKind::Text { .. } | FieldKind::Keyword) =>
                {
                    check_searchable(self.view(), name, spec)?;
                    out.push((name.to_string(), boost));
                }
                _ => {
                    return Err(EsError::unsupported(&format!(
                        "{query} over the non-text field [{name}]"
                    )));
                }
            }
        }
        Ok(out)
    }

    /// Every searchable text field (the default fields of `multi_match`
    /// adds keywords).
    fn default_fields(&self, keywords: bool) -> Vec<(String, f32)> {
        self.view()
            .info
            .schema
            .fields
            .iter()
            .filter(|spec| is_text_like(self.view(), spec))
            .filter(|spec| keywords || matches!(spec.kind, FieldKind::Text { .. }))
            .map(|spec| (spec.name.clone(), 1.0))
            .collect()
    }

    fn string_list(&self, query: &str, key: &str, v: &Value) -> Result<Vec<String>, EsError> {
        match v {
            Value::String(s) => Ok(vec![s.clone()]),
            Value::Array(items) => items
                .iter()
                .map(|item| {
                    item.as_str().map(str::to_string).ok_or_else(|| {
                        EsError::parsing(format!("[{query}] [{key}] must hold strings"))
                    })
                })
                .collect(),
            _ => Err(EsError::parsing(format!(
                "[{query}] [{key}] must be a string or an array of strings"
            ))),
        }
    }

    fn multi_match(&self, body: &Value) -> Result<Query, EsError> {
        let q = "multi_match";
        let mut text = None;
        let mut fields = None;
        let mut kind = MultiMatchKind::BestFields;
        let mut operator_ = BoolOperator::Or;
        let mut tie_breaker = None;
        let mut boost = 1.0;
        let mut slop = 0;
        for (key, value) in self.object(q, body)? {
            match key.as_str() {
                "query" => text = Some(value),
                "fields" => fields = Some(self.string_list(q, key, value)?),
                "type" => {
                    kind = match value.as_str().unwrap_or_default() {
                        "best_fields" | "BEST_FIELDS" => MultiMatchKind::BestFields,
                        "most_fields" | "MOST_FIELDS" => MultiMatchKind::MostFields,
                        "phrase" | "PHRASE" => MultiMatchKind::Phrase,
                        other @ ("cross_fields" | "phrase_prefix" | "bool_prefix") => {
                            return Err(EsError::unsupported(&format!(
                                "multi_match.type: {other}"
                            )));
                        }
                        other => {
                            return Err(EsError::parsing(format!(
                                "[{q}] query does not allow type [{other}]"
                            )));
                        }
                    }
                }
                "operator" => operator_ = operator(value)?,
                "tie_breaker" => {
                    let t = number_f32(q, key, value)?;
                    if !(0.0..=1.0).contains(&t) {
                        return Err(EsError::parsing(format!(
                            "[{q}] [tie_breaker] must be in [0, 1], found [{t}]"
                        )));
                    }
                    tie_breaker = Some(t);
                }
                "slop" => slop = number_int(q, key, value)?,
                "boost" => boost = self.boost_value(q, value)?,
                "_name" => {}
                "fuzziness"
                | "minimum_should_match"
                | "analyzer"
                | "prefix_length"
                | "max_expansions"
                | "fuzzy_transpositions"
                | "cutoff_frequency"
                | "zero_terms_query"
                | "auto_generate_synonyms_phrase_query" => {
                    return Err(EsError::unsupported(&format!("multi_match.{key}")));
                }
                _ => return Err(self.does_not_support(q, key)),
            }
        }
        if slop != 0 {
            return Err(EsError::unsupported("multi_match.slop"));
        }
        let Some(text) = text.and_then(scalar_text) else {
            return Err(EsError::parsing("No text specified for multi_match query"));
        };
        let fields = match fields {
            Some(list) => self.expand_fields(q, &list)?,
            None => self.default_fields(true),
        };
        if fields.is_empty() {
            return Ok(Query::MatchNone);
        }
        Ok(boosted(
            Query::MultiMatch {
                fields,
                text,
                kind,
                operator: operator_,
                tie_breaker,
            },
            boost,
        ))
    }

    fn term(&self, body: &Value) -> Result<Query, EsError> {
        let (field, v) = self.field_body("term", body)?;
        let mut value = None;
        let mut boost = 1.0;
        match v {
            Value::Object(params) => {
                for (key, param) in params {
                    match key.as_str() {
                        "value" => value = Some(param),
                        "boost" => boost = self.boost_value("term", param)?,
                        "_name" => {}
                        "case_insensitive" => {
                            if boolean("term", key, param)? {
                                return Err(EsError::unsupported("term.case_insensitive"));
                            }
                        }
                        _ => return Err(self.does_not_support("term", key)),
                    }
                }
            }
            other => value = Some(other),
        }
        let Some(value) = value.filter(|v| !v.is_null()) else {
            return Err(EsError::parsing("[term] query requires a non-null [value]"));
        };
        if !matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_)) {
            return Err(EsError::parsing(format!(
                "[term] query does not support [{}] values",
                token_name(value)
            )));
        }
        Ok(boosted(self.term_on(field, value)?, boost))
    }

    fn terms(&self, body: &Value) -> Result<Query, EsError> {
        let mut boost = 1.0;
        let mut target = None;
        for (key, value) in self.object("terms", body)? {
            match key.as_str() {
                "boost" => boost = self.boost_value("terms", value)?,
                "_name" => {}
                _ => {
                    if let Some((first, _)) = target {
                        return Err(EsError::parsing(format!(
                            "[terms] query does not support multiple fields, found [{first}] \
                             and [{key}]"
                        )));
                    }
                    target = Some((key.as_str(), value));
                }
            }
        }
        let Some((field, values)) = target else {
            return Err(EsError::parsing("[terms] query requires a field"));
        };
        let values = match values {
            Value::Array(values) => values,
            Value::Object(_) => return Err(EsError::unsupported("terms lookup")),
            other => {
                return Err(EsError::parsing(format!(
                    "[terms] query does not support [{field}] as [{}]",
                    token_name(other)
                )));
            }
        };
        if values.len() > MAX_TERMS {
            return Err(EsError::illegal_argument(format!(
                "The number of terms [{}] used in the Terms Query request has exceeded the \
                 allowed maximum of [{MAX_TERMS}]. This maximum can be set by changing the \
                 [index.max_terms_count] index level setting.",
                values.len()
            )));
        }
        let query = match resolve(self.view(), field) {
            FieldRef::Unmapped => Query::MatchNone,
            FieldRef::Id => Query::Ids(values.iter().map(id_key).collect::<Result<_, _>>()?),
            FieldRef::Vector(_) => {
                return Err(shard_error(format!(
                    "Field [{field}] of type [dense_vector] does not support term queries"
                )));
            }
            FieldRef::Json { .. } => Query::Terms {
                field: field.to_string(),
                values: values.iter().map(json_class).collect(),
            },
            FieldRef::Field(spec) => {
                check_searchable(self.view(), field, spec)?;
                let mut exact = Vec::new();
                let mut ranges = Vec::new();
                for value in values {
                    if value.is_null() || value.is_array() || value.is_object() {
                        return Err(EsError::parsing(format!(
                            "[terms] query does not support [{}] values",
                            token_name(value)
                        )));
                    }
                    match self.term_on(field, value)? {
                        Query::Term { value, .. } => exact.push(value),
                        other => ranges.push(other),
                    }
                }
                if ranges.is_empty() {
                    Query::Terms {
                        field: field.to_string(),
                        values: exact,
                    }
                } else {
                    let mut should = ranges;
                    if !exact.is_empty() {
                        should.push(Query::Terms {
                            field: field.to_string(),
                            values: exact,
                        });
                    }
                    Query::Bool {
                        must: Vec::new(),
                        should,
                        must_not: Vec::new(),
                        filter: Vec::new(),
                        minimum_should_match: None,
                    }
                }
            }
        };
        Ok(boosted(query, boost))
    }

    fn ids(&self, body: &Value) -> Result<Query, EsError> {
        let mut values = Vec::new();
        let mut boost = 1.0;
        for (key, value) in self.object("ids", body)? {
            match key.as_str() {
                "values" => {
                    let Value::Array(list) = value else {
                        return Err(EsError::parsing("[ids] [values] must be an array"));
                    };
                    values = list.iter().map(id_key).collect::<Result<_, _>>()?;
                }
                "boost" => boost = self.boost_value("ids", value)?,
                "_name" => {}
                _ => return Err(self.does_not_support("ids", key)),
            }
        }
        Ok(boosted(Query::Ids(values), boost))
    }

    fn range(&self, body: &Value) -> Result<Query, EsError> {
        let (field, v) = self.field_body("range", body)?;
        let Value::Object(params) = v else {
            return Err(EsError::parsing(format!(
                "[range] query malformed, no start_object after field name [{field}]"
            )));
        };
        // As ES's `RangeQueryBuilder`: `gt`/`gte`/`from` set the lower bound
        // and `lt`/`lte`/`to` the upper one, the last key winning (row
        // T11-3); `gt` and `lt` make the bound exclusive.
        let mut lower: Option<&Value> = None;
        let mut upper: Option<&Value> = None;
        let mut include_lower = true;
        let mut include_upper = true;
        let mut boost = 1.0;
        for (key, value) in params {
            let bound = (!value.is_null()).then_some(value);
            match key.as_str() {
                "gt" => (lower, include_lower) = (bound, false),
                "gte" => (lower, include_lower) = (bound, true),
                "lt" => (upper, include_upper) = (bound, false),
                "lte" => (upper, include_upper) = (bound, true),
                "from" => lower = bound,
                "to" => upper = bound,
                "include_lower" => include_lower = boolean("range", key, value)?,
                "include_upper" => include_upper = boolean("range", key, value)?,
                "boost" => boost = self.boost_value("range", value)?,
                "_name" => {}
                "format" => check_date_format(value)?,
                "time_zone" => {
                    let zone = value.as_str().unwrap_or_default();
                    if !matches!(zone, "UTC" | "Z" | "+00:00" | "Etc/UTC") {
                        return Err(EsError::unsupported(&format!("range.time_zone: {zone}")));
                    }
                }
                "relation" => {
                    if !value
                        .as_str()
                        .is_some_and(|r| r.eq_ignore_ascii_case("intersects"))
                    {
                        return Err(EsError::unsupported(&format!(
                            "range.relation: {}",
                            scalar_text(value).unwrap_or_default()
                        )));
                    }
                }
                _ => return Err(self.does_not_support("range", key)),
            }
        }
        let (gt, gte) = if include_lower {
            (None, lower)
        } else {
            (lower, None)
        };
        let (lt, lte) = if include_upper {
            (None, upper)
        } else {
            (upper, None)
        };
        let query = match resolve(self.view(), field) {
            FieldRef::Unmapped => Query::MatchNone,
            FieldRef::Id => return Err(EsError::unsupported("range on [_id]")),
            FieldRef::Vector(_) => {
                return Err(shard_error(format!(
                    "Field [{field}] of type [dense_vector] does not support range queries"
                )));
            }
            FieldRef::Json { .. } => Query::Range {
                field: field.to_string(),
                gt: gt.map(json_class),
                gte: gte.map(json_class),
                lt: lt.map(json_class),
                lte: lte.map(json_class),
            },
            FieldRef::Field(spec) => {
                check_searchable(self.view(), field, spec)?;
                let bound = |v: Option<&Value>, round| -> Result<Option<FieldValue>, EsError> {
                    match v {
                        None => Ok(None),
                        Some(v) => {
                            if v.is_array() || v.is_object() {
                                return Err(EsError::parsing(format!(
                                    "[range] query does not support [{}] bounds",
                                    token_name(v)
                                )));
                            }
                            coerce_rounded(self.view(), field, v, self.ctx.now_ms, round)
                        }
                    }
                };
                Query::Range {
                    field: field.to_string(),
                    gt: bound(gt, Rounding::Up)?,
                    gte: bound(gte, Rounding::Down)?,
                    lt: bound(lt, Rounding::Down)?,
                    lte: bound(lte, Rounding::Up)?,
                }
            }
        };
        Ok(boosted(query, boost))
    }

    fn exists(&self, body: &Value) -> Result<Query, EsError> {
        let mut field = None;
        let mut boost = 1.0;
        for (key, value) in self.object("exists", body)? {
            match key.as_str() {
                "field" => field = value.as_str(),
                "boost" => boost = self.boost_value("exists", value)?,
                "_name" => {}
                _ => return Err(self.does_not_support("exists", key)),
            }
        }
        let Some(field) = field.filter(|f| !f.is_empty()) else {
            return Err(EsError::illegal_argument("field name is null or empty"));
        };
        let query = match resolve(self.view(), field) {
            FieldRef::Id => Query::MatchAll,
            // ES keeps neither norms nor doc values for an unindexed `text`
            // or a `binary`, so `exists` finds nothing there (row T11-3).
            FieldRef::Field(spec) if !is_searchable(self.view(), field, spec) => Query::MatchNone,
            FieldRef::Field(_) | FieldRef::Json { .. } => Query::Exists {
                field: field.to_string(),
            },
            FieldRef::Vector(_) => {
                return Err(EsError::unsupported("exists on a dense_vector field"));
            }
            FieldRef::Unmapped => {
                let prefix = format!("{field}.");
                let should: Vec<Query> = self
                    .view()
                    .info
                    .schema
                    .fields
                    .iter()
                    .filter(|spec| spec.name.starts_with(&prefix))
                    .map(|spec| Query::Exists {
                        field: spec.name.clone(),
                    })
                    .collect();
                if should.is_empty() {
                    Query::MatchNone
                } else {
                    Query::Bool {
                        must: Vec::new(),
                        should,
                        must_not: Vec::new(),
                        filter: Vec::new(),
                        minimum_should_match: None,
                    }
                }
            }
        };
        Ok(boosted(query, boost))
    }

    /// `prefix`, `wildcard` and `fuzzy`.
    fn pattern(&self, q: &str, body: &Value) -> Result<Query, EsError> {
        let (field, v) = self.field_body(q, body)?;
        let mut value = None;
        let mut boost = 1.0;
        let mut fuzz = Fuzziness::Auto;
        match v {
            Value::Object(params) => {
                for (key, param) in params {
                    match (key.as_str(), q) {
                        ("value", _) | ("wildcard", "wildcard") => value = Some(param),
                        ("boost", _) => boost = self.boost_value(q, param)?,
                        ("_name" | "rewrite", _) => {}
                        ("case_insensitive", "prefix" | "wildcard") => {
                            if boolean(q, key, param)? {
                                return Err(EsError::unsupported(&format!("{q}.case_insensitive")));
                            }
                        }
                        ("fuzziness", "fuzzy") => fuzz = fuzziness(param)?,
                        ("prefix_length", "fuzzy") => {
                            if number_int(q, key, param)? != 0 {
                                return Err(EsError::unsupported("fuzzy.prefix_length"));
                            }
                        }
                        ("max_expansions", "fuzzy") => {
                            if number_int(q, key, param)? != 50 {
                                return Err(EsError::unsupported("fuzzy.max_expansions"));
                            }
                        }
                        ("transpositions", "fuzzy") => {
                            if !boolean(q, key, param)? {
                                return Err(EsError::unsupported("fuzzy.transpositions"));
                            }
                        }
                        _ => return Err(self.does_not_support(q, key)),
                    }
                }
            }
            other => value = Some(other),
        }
        let Some(text) = value.and_then(scalar_text) else {
            return Err(EsError::parsing(format!("[{q}] requires a [value]")));
        };
        let query = match resolve(self.view(), field) {
            FieldRef::Unmapped => Query::MatchNone,
            FieldRef::Id => return Err(EsError::unsupported(&format!("{q} on [_id]"))),
            FieldRef::Field(spec)
                if !matches!(
                    spec.kind,
                    FieldKind::Text { .. } | FieldKind::Keyword | FieldKind::Uuid
                ) =>
            {
                return Err(shard_error(format!(
                    "Can only use {q} queries on keyword and text fields - not on [{field}] \
                     which is of type [{}]",
                    es_type(self.view(), field)
                )));
            }
            FieldRef::Vector(_) => {
                return Err(shard_error(format!(
                    "Can only use {q} queries on keyword and text fields - not on [{field}] \
                     which is of type [dense_vector]"
                )));
            }
            FieldRef::Field(spec) => {
                check_searchable(self.view(), field, spec)?;
                pattern_query(q, field, text, fuzz)
            }
            FieldRef::Json { .. } => pattern_query(q, field, text, fuzz),
        };
        Ok(boosted(query, boost))
    }

    fn query_string(&self, body: &Value) -> Result<Query, EsError> {
        let q = "query_string";
        let mut text = None;
        let mut default_field = None;
        let mut fields = None;
        let mut default_operator = BoolOperator::Or;
        let mut boost = 1.0;
        for (key, value) in self.object(q, body)? {
            match key.as_str() {
                "query" => text = value.as_str(),
                "default_field" => default_field = value.as_str().map(str::to_string),
                "fields" => fields = Some(self.string_list(q, key, value)?),
                "default_operator" => default_operator = operator(value)?,
                "lenient" => {
                    boolean(q, key, value)?;
                }
                "boost" => boost = self.boost_value(q, value)?,
                "_name" => {}
                _ => return Err(EsError::unsupported(&format!("query_string.{key}"))),
            }
        }
        let Some(text) = text else {
            return Err(EsError::parsing(
                "[query_string] must be provided with a [query]",
            ));
        };
        let named = match (fields, default_field) {
            (Some(list), _) => Some(list),
            (None, Some(field)) if field != "*" => Some(vec![field]),
            _ => None,
        };
        let default_fields = match named {
            Some(list) => self.expand_fields(q, &list)?,
            None => self.default_fields(false),
        };
        if default_fields.iter().any(|(_, boost)| *boost != 1.0) {
            return Err(EsError::unsupported("query_string field boosts"));
        }
        Ok(boosted(
            Query::QueryString {
                query: text.to_string(),
                default_fields: default_fields.into_iter().map(|(f, _)| f).collect(),
                default_operator,
            },
            boost,
        ))
    }

    fn constant_score(&self, body: &Value, depth: u32) -> Result<Query, EsError> {
        let mut filter = None;
        let mut boost = 1.0;
        for (key, value) in self.object("constant_score", body)? {
            match key.as_str() {
                "filter" => filter = Some(self.leaf(value, depth + 1)?),
                "boost" => boost = self.boost_value("constant_score", value)?,
                "_name" => {}
                _ => return Err(self.does_not_support("constant_score", key)),
            }
        }
        let Some(filter) = filter else {
            return Err(EsError::parsing(
                "[constant_score] requires a 'filter' element",
            ));
        };
        Ok(Query::ConstantScore {
            query: Box::new(filter),
            score: boost,
        })
    }

    /// `bool`; with `lift`, the knn clauses of `should` are taken out, and
    /// the bool's `filter` clauses become their pre-filters (item 5). `None`
    /// when lifting leaves no clause at all.
    fn bool_query(
        &self,
        body: &Value,
        depth: u32,
        mut lift: Option<&mut Vec<KnnSpec>>,
    ) -> Result<Option<Query>, EsError> {
        let mut must = Vec::new();
        let mut should = Vec::new();
        let mut must_not = Vec::new();
        let mut filter = Vec::new();
        let mut msm = None;
        let mut boost = 1.0;
        let mut lifted = Vec::new();
        for (key, value) in self.object("bool", body)? {
            let clauses = || -> Vec<&Value> {
                match value {
                    Value::Array(items) => items.iter().collect(),
                    other => vec![other],
                }
            };
            match key.as_str() {
                "must" => {
                    for c in clauses() {
                        must.push(self.leaf(c, depth + 1)?);
                    }
                }
                "should" => {
                    for c in clauses() {
                        let knn = c
                            .as_object()
                            .filter(|m| m.len() == 1)
                            .and_then(|m| m.get("knn"));
                        match (knn, lift.is_some()) {
                            (Some(knn), true) => {
                                lifted.push(self.knn_spec(knn, depth + 1, self.ctx.default_k)?);
                            }
                            _ => should.push(self.leaf(c, depth + 1)?),
                        }
                    }
                }
                "must_not" | "mustNot" => {
                    for c in clauses() {
                        must_not.push(self.leaf(c, depth + 1)?);
                    }
                }
                "filter" => {
                    for c in clauses() {
                        filter.push(self.leaf(c, depth + 1)?);
                    }
                }
                "minimum_should_match" => msm = Some(minimum_should_match(value)?),
                "boost" => boost = self.boost_value("bool", value)?,
                "adjust_pure_negative" => {
                    if !boolean("bool", key, value)? {
                        return Err(EsError::unsupported("bool.adjust_pure_negative: false"));
                    }
                }
                "_name" => {}
                _ => return Err(self.does_not_support("bool", key)),
            }
        }
        if !lifted.is_empty() {
            if msm.is_some() {
                return Err(EsError::unsupported(
                    "minimum_should_match with a knn should clause",
                ));
            }
            for mut knn in lifted {
                let mut parts: Vec<Query> = knn.filter.take().into_iter().collect();
                parts.extend(filter.iter().cloned());
                knn.filter = and_queries(parts);
                knn.boost *= boost;
                if let Some(out) = lift.as_deref_mut() {
                    out.push(knn);
                }
            }
            if must.is_empty() && should.is_empty() && must_not.is_empty() && filter.is_empty() {
                return Ok(None);
            }
        }
        Ok(Some(boosted(
            Query::Bool {
                must,
                should,
                must_not,
                filter,
                minimum_should_match: msm,
            },
            boost,
        )))
    }

    /// One knn clause (item 5).
    pub(super) fn knn_spec(
        &self,
        body: &Value,
        depth: u32,
        default_k: usize,
    ) -> Result<KnnSpec, EsError> {
        let q = "knn";
        let map = self.object(q, body)?;
        let mut field = None;
        let mut query_vector = None;
        let mut k = None;
        let mut num_candidates = None;
        let mut filter = None;
        let mut similarity = None;
        let mut boost = 1.0;
        for (key, value) in map {
            match key.as_str() {
                "field" => field = value.as_str(),
                "query_vector" => query_vector = Some(value),
                "query_vector_builder" => {
                    return Err(EsError::illegal_argument(
                        "Loams does not support [query_vector_builder] (no scripting or ML \
                         inference)",
                    ));
                }
                "k" => k = Some(number_int(q, key, value)?),
                "num_candidates" => num_candidates = Some(number_int(q, key, value)?),
                "filter" => filter = parse_filter_list_at(self, value, depth + 1)?,
                "similarity" => similarity = Some(number_f32(q, key, value)?),
                "boost" => boost = self.boost_value(q, value)?,
                "_name" => {}
                _ => return Err(EsError::parsing(format!("[knn] unknown field [{key}]"))),
            }
        }
        let Some(field) = field else {
            return Err(EsError::parsing("[knn] requires [field]"));
        };
        let k = k.unwrap_or(default_k as i64);
        if k < 1 {
            return Err(EsError::illegal_argument("[k] must be greater than 0"));
        }
        let k = k as usize;
        let num_candidates = match num_candidates {
            Some(n) => usize::try_from(n).unwrap_or(0),
            None => (k.saturating_mul(3).div_ceil(2)).min(MAX_NUM_CANDIDATES),
        };
        if num_candidates > MAX_NUM_CANDIDATES {
            return Err(EsError::illegal_argument(format!(
                "[num_candidates] cannot exceed [{MAX_NUM_CANDIDATES}]"
            )));
        }
        if num_candidates < k {
            return Err(EsError::illegal_argument(
                "[num_candidates] cannot be less than [k]",
            ));
        }
        let Some(vector) = self.view().es.vectors.get(field) else {
            return Err(EsError::illegal_argument(format!(
                "field [{field}] does not exist in the mapping"
            )));
        };
        if !vector.indexed {
            return Err(EsError::illegal_argument(format!(
                "to perform knn search on field [{field}], its mapping must have [index] set to \
                 [true]"
            )));
        }
        let Some(query_vector) = query_vector else {
            return Err(EsError::illegal_argument(
                "either [query_vector_builder] or [query_vector] must be provided",
            ));
        };
        Ok(KnnSpec {
            field: field.to_string(),
            query_vector: check_query_vector(query_vector, vector, true)?,
            k,
            num_candidates,
            filter,
            similarity,
            boost,
        })
    }
}

fn empty_bool() -> Query {
    Query::Bool {
        must: Vec::new(),
        should: Vec::new(),
        must_not: Vec::new(),
        filter: Vec::new(),
        minimum_should_match: None,
    }
}

fn pattern_query(q: &str, field: &str, text: String, fuzziness: Fuzziness) -> Query {
    match q {
        "prefix" => Query::Prefix {
            field: field.to_string(),
            value: text,
        },
        "wildcard" => Query::Wildcard {
            field: field.to_string(),
            pattern: text,
        },
        _ => Query::Fuzzy {
            field: field.to_string(),
            value: text,
            fuzziness,
        },
    }
}

/// An `_id` value: a string, or a number taken as its text.
fn id_key(v: &Value) -> Result<PrimaryKey, EsError> {
    match v {
        Value::String(s) => Ok(PrimaryKey::Str(s.clone())),
        Value::Number(n) => Ok(PrimaryKey::Str(n.to_string())),
        other => Err(EsError::parsing(format!(
            "[ids] values must be strings, found [{}]",
            token_name(other)
        ))),
    }
}

/// A `range.format`: the Ruling 17 formats joined by `||`.
fn check_date_format(v: &Value) -> Result<(), EsError> {
    let text = v.as_str().unwrap_or_default();
    for part in text.split("||") {
        if !matches!(
            part,
            "strict_date_optional_time"
                | "date_optional_time"
                | "strict_date_optional_time_nanos"
                | "epoch_millis"
        ) {
            return Err(EsError::unsupported(&format!("range.format: {part}")));
        }
    }
    Ok(())
}

fn parse_filter_list_at(p: &Parser<'_>, v: &Value, depth: u32) -> Result<Option<Query>, EsError> {
    let queries = match v {
        Value::Array(items) => items
            .iter()
            .map(|item| p.leaf(item, depth))
            .collect::<Result<Vec<_>, _>>()?,
        Value::Object(_) => vec![p.leaf(v, depth)?],
        other => {
            return Err(EsError::parsing(format!(
                "[filter] must be an object or an array, found [{}]",
                token_name(other)
            )));
        }
    };
    Ok(and_queries(queries))
}

/// A search's `query` (items 1–6).
pub fn parse_query(v: &Value, ctx: &QueryContext) -> Result<ParsedQuery, EsError> {
    Parser::new(ctx).top(v)
}

/// A query in which knn and script_score are refused.
pub fn parse_leaf(v: &Value, ctx: &QueryContext) -> Result<Query, EsError> {
    Parser::new(ctx).leaf(v, ctx.depth + 1)
}

/// A filter: an object or an array of queries, all of which must match.
/// An empty array is `None`; one query is itself.
pub fn parse_filter_list(v: &Value, ctx: &QueryContext) -> Result<Option<Query>, EsError> {
    parse_filter_list_at(&Parser::new(ctx), v, ctx.depth + 1)
}

/// A search body's `knn`: an object or an array of them.
pub fn parse_knn(v: &Value, ctx: &QueryContext, default_k: usize) -> Result<Vec<KnnSpec>, EsError> {
    let p = Parser::new(ctx);
    match v {
        Value::Array(items) => items
            .iter()
            .map(|item| p.knn_spec(item, ctx.depth + 1, default_k))
            .collect(),
        Value::Object(_) => Ok(vec![p.knn_spec(v, ctx.depth + 1, default_k)?]),
        other => Err(EsError::parsing(format!(
            "[knn] must be an object or an array, found [{}]",
            token_name(other)
        ))),
    }
}
