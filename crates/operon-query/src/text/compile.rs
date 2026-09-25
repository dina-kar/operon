//! The IR [`Query`] compiled to a Tantivy query for one split's schema (plan
//! M1.2 Task 2 rules 1–3 and 6–9).
//!
//! Fields resolve by name against the collection schema (for the kind) and
//! the split's own Tantivy schema (for the `Field`; M1.1 names Tantivy
//! fields after `FieldSpec.name`). A field the split lacks matches nothing
//! there, as an unmapped field does in ES.
//!
//! Exact values of a date field, and of a field that is fast but not
//! indexed, are fast-field ranges `[v, v]`: Tantivy's postings hold dates at
//! second precision, while the fast column holds the field's milliseconds.

use std::collections::{BTreeMap, HashSet};
use std::ops::Bound;
use std::sync::Arc;

use operon_collection::{
    CollectionSchema, FieldKind, FieldSpec, PK_FIELD, PrimaryKey, ROWID_FIELD, count_companion,
    date_companion, null_companion, text_companion,
};
use operon_quickwit::doc_mapper::{
    Automaton, FastFieldWarmupInfo, TermRange, WarmupInfo, build_query,
};
use operon_quickwit::query::query_ast::{
    AutomatonQuery, BuildTantivyAstContext, FieldPresenceQuery, JsonPathPrefix, QueryAst,
};
use operon_quickwit::query::tokenizers::TokenizerManager;
use operon_text::STANDARD;
use tantivy::columnar::NumericalValue;
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, ConstScoreQuery, DisjunctionMaxQuery, EmptyQuery,
    ExistsQuery, FuzzyTermQuery, Occur, PhrasePrefixQuery, PhraseQuery, Query as TantivyQuery,
    QueryParser, RangeQuery, RegexQuery, TermQuery, TermSetQuery,
};
use tantivy::schema::{Field, FieldType, IndexRecordOption, Schema};
use tantivy::{DateTime, Term};

use crate::error::ServiceError;
use crate::ir::{BoolOperator, FieldValue, Fuzziness, MultiMatchKind, Query};
use crate::text::coerce::{Coerced, RangeOp, coerce_bound, coerce_term};
use crate::text::fields::{ResolvedField, resolve_field};

type Boxed = Box<dyn TantivyQuery>;
/// An analyzed token: its position, its text and its term.
type Token = (usize, String, Term);

/// The reserved name of the primary key.
const ID_FIELD: &str = "_id";
/// At most this many terms a phrase prefix expands its last token to.
const PHRASE_PREFIX_EXPANSIONS: u32 = 50;

/// How the compiled query is run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompileMode {
    /// Scores per rule 2.
    Scoring,
    /// Every leaf scores 0, and scoring is disabled when the query runs
    /// (Task 5).
    Filter,
}

/// A compiled query and what it reads.
#[derive(Debug)]
pub struct CompiledQuery {
    pub query: Box<dyn TantivyQuery>,
    /// Everything the query reads, for the vendored `warmup` (rule 8).
    pub warmup: WarmupInfo,
    /// No leaf computes BM25.
    pub constant_score: bool,
}

/// Compiles queries of one collection schema for one split.
#[derive(Debug)]
pub struct QueryCompiler<'a> {
    schema: &'a CollectionSchema,
    split: &'a Schema,
    tokenizers: &'a TokenizerManager,
}

/// A field of a query, resolved for the split.
enum Target<'a> {
    Plain {
        spec: &'a FieldSpec,
        field: Field,
    },
    Json {
        spec: &'a FieldSpec,
        path: String,
    },
    /// An unknown field, or one the split lacks.
    Missing,
}

impl<'a> QueryCompiler<'a> {
    pub fn new(
        schema: &'a CollectionSchema,
        split_schema: &'a Schema,
        tokenizers: &'a TokenizerManager,
    ) -> Self {
        Self {
            schema,
            split: split_schema,
            tokenizers,
        }
    }

    pub fn compile(&self, query: &Query, mode: CompileMode) -> Result<CompiledQuery, ServiceError> {
        let mut build = Build::new(self, mode);
        let query = build.query(query)?;
        Ok(build.finish(query))
    }

    /// `Bool { must: [query], filter: [filter] }` without allocating the IR.
    pub fn compile_with_filter(
        &self,
        query: &Query,
        filter: Option<&Query>,
    ) -> Result<CompiledQuery, ServiceError> {
        let mut build = Build::new(self, CompileMode::Scoring);
        let mut compiled = build.query(query)?;
        if let Some(filter) = filter {
            let filter = build.query(filter)?;
            compiled = Box::new(BooleanQuery::new(vec![
                (Occur::Must, compiled),
                (Occur::Must, Box::new(ConstScoreQuery::new(filter, 0.0))),
            ]));
        }
        Ok(build.finish(compiled))
    }

    fn field(&self, name: &str) -> Option<Field> {
        self.split.get_field(name).ok()
    }

    fn resolve(&self, name: &str) -> Target<'a> {
        match resolve_field(self.schema, name) {
            ResolvedField::Plain { spec } => match self.field(&spec.name) {
                Some(field) => Target::Plain { spec, field },
                None => Target::Missing,
            },
            ResolvedField::JsonPath { spec, path } => Target::Json { spec, path },
            ResolvedField::Unknown => Target::Missing,
        }
    }

    /// The tokens of `text` under `analyzer`, with their positions.
    fn analyze(&self, analyzer: &str, text: &str) -> Result<Vec<(usize, String)>, ServiceError> {
        analyze(self.tokenizers, analyzer, text)
    }
}

fn analyze(
    tokenizers: &TokenizerManager,
    analyzer: &str,
    text: &str,
) -> Result<Vec<(usize, String)>, ServiceError> {
    let mut analyzer = tokenizers
        .get_tokenizer(analyzer)
        .ok_or_else(|| ServiceError::InvalidArgument(format!("unknown analyzer {analyzer:?}")))?;
    let mut tokens = Vec::new();
    analyzer
        .token_stream(text)
        .process(&mut |token| tokens.push((token.position, token.text.clone())));
    Ok(tokens)
}

fn invalid(message: impl Into<String>) -> ServiceError {
    ServiceError::InvalidArgument(message.into())
}

fn json_whole(field: &str) -> ServiceError {
    invalid(format!("use a path inside JSON field {field}"))
}

fn empty() -> Boxed {
    Box::new(EmptyQuery)
}

/// A JSON term of `field` at `path`, with no value yet.
fn json_term(field: Field, path: &str) -> Term {
    Term::from_field_json_path(field, path, true)
}

fn json_str(field: Field, path: &str, value: &str) -> Term {
    let mut term = json_term(field, path);
    term.append_type_and_str(value);
    term
}

/// The bytes a term of `field` at `path` starts with in the term
/// dictionary: the path, then the string type.
fn json_path_prefix(field: Field, path: &str) -> Vec<u8> {
    json_str(field, path, "").serialized_value_bytes().to_vec()
}

/// A JSON term holding `value`, type-strict between strings, numbers and
/// bools (S4): `I64` (and `U64` that fits) as i64, larger `U64` as u64,
/// `F64` as f64. Tantivy indexes an integral JSON float as the integer it
/// equals (`NumericalValue::normalize`), so an integral `F64` is looked up
/// the same way. `None` for a date.
fn json_value_term(field: Field, path: &str, value: &FieldValue) -> Option<Term> {
    let mut term = json_term(field, path);
    match value {
        FieldValue::Str(s) => term.append_type_and_str(s),
        FieldValue::I64(n) => term.append_type_and_fast_value(*n),
        FieldValue::U64(n) => match i64::try_from(*n) {
            Ok(n) => term.append_type_and_fast_value(n),
            Err(_) => term.append_type_and_fast_value(*n),
        },
        FieldValue::F64(x) => match NumericalValue::F64(*x).normalize() {
            NumericalValue::I64(n) => term.append_type_and_fast_value(n),
            NumericalValue::U64(n) => term.append_type_and_fast_value(n),
            NumericalValue::F64(x) => term.append_type_and_fast_value(x),
        },
        FieldValue::Bool(b) => term.append_type_and_fast_value(*b),
        FieldValue::Date(_) => return None,
    }
    Some(term)
}

fn date(ms: i64) -> DateTime {
    DateTime::from_timestamp_millis(ms)
}

/// The term of a coerced value on a plain field; `None` for `Never`.
fn plain_term(field: Field, value: &Coerced) -> Option<Term> {
    Some(match value {
        Coerced::Str(s) => Term::from_field_text(field, s),
        Coerced::I64(n) => Term::from_field_i64(field, *n),
        Coerced::F64(x) => Term::from_field_f64(field, *x),
        Coerced::Bool(b) => Term::from_field_bool(field, *b),
        Coerced::DateMs(ms) => Term::from_field_date(field, date(*ms)),
        Coerced::Never => return None,
    })
}

fn is_never(bound: &Bound<Coerced>) -> bool {
    matches!(
        bound,
        Bound::Included(Coerced::Never) | Bound::Excluded(Coerced::Never)
    )
}

/// The first byte string above every string starting with `prefix`.
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < u8::MAX {
            end.push(last + 1);
            return Some(end);
        }
    }
    None
}

/// Backslash-escapes every regex metacharacter `\.+*?()|[]{}^$#&-~"`.
fn escape_regex(text: &str, out: &mut String) {
    for c in text.chars() {
        if "\\.+*?()|[]{}^$#&-~\"".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
}

fn prefix_regex(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    escape_regex(value, &mut out);
    out.push_str(".*");
    out
}

/// A wildcard pattern as a regex: `*` is `.*`, `?` is `.`, everything else
/// literal.
fn wildcard_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 8);
    for c in pattern.chars() {
        match c {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            c => escape_regex(c.encode_utf8(&mut [0; 4]), &mut out),
        }
    }
    out
}

/// The primary key a `Term` value on `_id` names.
fn id_of(value: &FieldValue) -> Option<PrimaryKey> {
    match value {
        FieldValue::I64(n) => u64::try_from(*n).ok().map(PrimaryKey::U64),
        FieldValue::U64(n) => Some(PrimaryKey::U64(*n)),
        FieldValue::Str(s) => Some(PrimaryKey::Str(s.clone())),
        FieldValue::F64(_) | FieldValue::Bool(_) | FieldValue::Date(_) => None,
    }
}

/// The ES `minimum_should_match` of `optional` optional clauses: `"3"`,
/// `"-1"`, `"75%"` or `"-25%"`, clamped to `0..=optional`.
pub fn parse_minimum_should_match(spec: &str, optional: usize) -> Result<usize, ServiceError> {
    let unsupported = || invalid(format!("unsupported minimum_should_match {spec}"));
    let text = spec.trim();
    let (number, percent) = match text.strip_suffix('%') {
        Some(number) => (number, true),
        None => (text, false),
    };
    let value: i64 = number.parse().map_err(|_| unsupported())?;
    let n = optional as i64;
    let count = match (percent, value < 0) {
        (false, false) => value.min(n),
        (false, true) => n.saturating_add(value),
        (true, false) => n.saturating_mul(value) / 100,
        (true, true) => n - n.saturating_mul(value.saturating_neg()) / 100,
    };
    Ok(count.clamp(0, n) as usize)
}

/// The edit distance of a fuzzy match on `term`: `Edits(e)` is at most 2;
/// `Auto` is 0 below 3 chars, 1 for 3–5 and 2 above (ES `AUTO:3,6`).
pub fn fuzziness_edits(fuzziness: Fuzziness, term: &str) -> u8 {
    match fuzziness {
        Fuzziness::Edits(edits) => edits.min(2),
        Fuzziness::Auto => match term.chars().count() {
            0..=2 => 0,
            3..=5 => 1,
            _ => 2,
        },
    }
}

/// (field name, analyzed term text) of every scoring text leaf, for
/// highlighting (Task 8): `Match`, `MatchPhrase`, `MultiMatch` and `Term`
/// on a `Text` field or a JSON path (through `_text.<n>`). `must_not`
/// subtrees are skipped; each pair appears once, first occurrence first.
pub fn highlight_terms(
    schema: &CollectionSchema,
    query: &Query,
    tokenizers: &TokenizerManager,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    highlight_walk(schema, query, tokenizers, &mut out);
    let mut seen = HashSet::new();
    out.retain(|pair| seen.insert(pair.clone()));
    out
}

fn highlight_walk(
    schema: &CollectionSchema,
    query: &Query,
    tokenizers: &TokenizerManager,
    out: &mut Vec<(String, String)>,
) {
    let mut analyzed = |field: &str, text: &str, analyzer: Option<&str>| {
        let analyzer = match resolve_field(schema, field) {
            ResolvedField::Plain { spec } => match &spec.kind {
                FieldKind::Text { analyzer: own, .. } => analyzer.unwrap_or(own).to_string(),
                _ => return,
            },
            ResolvedField::JsonPath { .. } => analyzer.unwrap_or(STANDARD).to_string(),
            ResolvedField::Unknown => return,
        };
        if let Ok(tokens) = analyze(tokenizers, &analyzer, text) {
            out.extend(tokens.into_iter().map(|(_, t)| (field.to_string(), t)));
        }
    };
    match query {
        Query::Match {
            field,
            text,
            analyzer,
            ..
        } => analyzed(field, text, analyzer.as_deref()),
        Query::MatchPhrase { field, text, .. } => analyzed(field, text, None),
        Query::MultiMatch { fields, text, .. } => {
            for (field, _) in fields {
                analyzed(field, text, None);
            }
        }
        Query::Term {
            field,
            value: FieldValue::Str(value),
        } => {
            let text_field = match resolve_field(schema, field) {
                ResolvedField::Plain { spec } => matches!(spec.kind, FieldKind::Text { .. }),
                ResolvedField::JsonPath { .. } => true,
                ResolvedField::Unknown => false,
            };
            if text_field {
                out.push((field.clone(), value.clone()));
            }
        }
        Query::Bool {
            must,
            should,
            filter,
            ..
        } => {
            for query in must.iter().chain(should).chain(filter) {
                highlight_walk(schema, query, tokenizers, out);
            }
        }
        Query::Boost { query, .. } | Query::ConstantScore { query, .. } => {
            highlight_walk(schema, query, tokenizers, out);
        }
        _ => {}
    }
}

/// One compilation: the mode, what the query reads, and whether some leaf
/// computes BM25.
struct Build<'c, 'a> {
    c: &'c QueryCompiler<'a>,
    mode: CompileMode,
    warmup: WarmupInfo,
    bm25: bool,
}

impl<'c, 'a> Build<'c, 'a> {
    fn new(c: &'c QueryCompiler<'a>, mode: CompileMode) -> Self {
        Self {
            c,
            mode,
            warmup: WarmupInfo::default(),
            bm25: false,
        }
    }

    fn finish(mut self, query: Boxed) -> CompiledQuery {
        for name in [ROWID_FIELD, PK_FIELD] {
            if self.c.field(name).is_some() {
                self.warm_fast(name.to_string(), false);
            }
        }
        let constant_score = self.mode == CompileMode::Filter || !self.bm25;
        self.warmup.field_norms = !constant_score;
        // A required term lets the vendored warmup stop early and leave the
        // split cold (row 0.46).
        self.warmup.required_terms.clear();
        CompiledQuery {
            query,
            warmup: self.warmup,
            constant_score,
        }
    }

    // ---- scores (rule 2) ----

    /// A leaf that scores BM25.
    fn scored(&mut self, query: Boxed) -> Boxed {
        match self.mode {
            CompileMode::Scoring => {
                self.bm25 = true;
                query
            }
            CompileMode::Filter => Box::new(ConstScoreQuery::new(query, 0.0)),
        }
    }

    /// A leaf that scores a constant 1.0.
    fn constant(&mut self, query: Boxed) -> Boxed {
        let score = match self.mode {
            CompileMode::Scoring => 1.0,
            CompileMode::Filter => 0.0,
        };
        Box::new(ConstScoreQuery::new(query, score))
    }

    /// A BM25 term leaf (frequencies only when scoring).
    fn scored_term(&mut self, term: Term) -> Boxed {
        self.warm_term(&term, false);
        let record = match self.mode {
            CompileMode::Scoring => IndexRecordOption::WithFreqs,
            CompileMode::Filter => IndexRecordOption::Basic,
        };
        self.scored(Box::new(TermQuery::new(term, record)))
    }

    // ---- warmup (rule 8) ----

    fn warm_term(&mut self, term: &Term, positions: bool) {
        *self
            .warmup
            .terms_grouped_by_field
            .entry(term.field())
            .or_default()
            .entry(term.clone())
            .or_default() |= positions;
    }

    fn warm_range(
        &mut self,
        field: Field,
        start: Bound<Term>,
        end: Bound<Term>,
        limit: Option<u64>,
        positions: bool,
    ) {
        *self
            .warmup
            .term_ranges_grouped_by_field
            .entry(field)
            .or_default()
            .entry(TermRange { start, end, limit })
            .or_default() |= positions;
    }

    fn warm_fast(&mut self, name: String, with_subfields: bool) {
        self.warmup.merge(WarmupInfo {
            fast_fields: HashSet::from([FastFieldWarmupInfo {
                name,
                with_subfields,
            }]),
            ..WarmupInfo::default()
        });
    }

    fn warm_automaton(&mut self, field: Field, path: Option<Vec<u8>>, pattern: String) {
        self.warmup
            .automatons_grouped_by_field
            .entry(field)
            .or_default()
            .insert(Automaton::Regex(path, pattern));
    }

    fn warm_dict(&mut self, field: Field) {
        self.warmup.term_dict_fields.insert(field);
    }

    // ---- fields ----

    fn entry_name(&self, field: Field) -> String {
        self.c.split.get_field_entry(field).name().to_string()
    }

    /// The split's Json companion (`_text.n`, …) of Json field `spec`.
    fn companion(&self, spec: &FieldSpec, name: fn(&str) -> String) -> Option<Field> {
        self.c.field(&name(&spec.name))
    }

    /// Whether an exact value of `field` is answered by its fast column
    /// (a date, or a field that is fast but not indexed).
    fn exact_is_fast(&self, field: Field) -> bool {
        let entry = self.c.split.get_field_entry(field);
        entry.is_fast() && (matches!(entry.field_type(), FieldType::Date(_)) || !entry.is_indexed())
    }

    /// The unscored leaf matching `term` of a plain field exactly.
    fn exact_raw(&mut self, field: Field, term: Term) -> Boxed {
        if self.exact_is_fast(field) {
            let name = self.entry_name(field);
            self.warm_fast(name, false);
            Box::new(RangeQuery::new(
                Bound::Included(term.clone()),
                Bound::Included(term),
            ))
        } else {
            self.warm_term(&term, false);
            Box::new(TermQuery::new(term, IndexRecordOption::Basic))
        }
    }

    /// An exact-value leaf, scored BM25 when `bm25` and the postings answer
    /// it.
    fn exact(&mut self, field: Field, term: Term, bm25: bool) -> Boxed {
        if bm25 && !self.exact_is_fast(field) {
            return self.scored_term(term);
        }
        let raw = self.exact_raw(field, term);
        self.constant(raw)
    }

    /// A fast-field range over JSON dates of `_date.n` at `path`, in ms.
    fn json_date_range(
        &mut self,
        date_field: Field,
        path: &str,
        lo: Bound<i64>,
        hi: Bound<i64>,
    ) -> Boxed {
        let term = |ms: i64| {
            let mut term = json_term(date_field, path);
            term.append_type_and_fast_value(date(ms));
            term
        };
        self.range_raw(date_field, path, lo.map(term), hi.map(term))
    }

    /// An unscored range leaf on `field` (at a JSON `path` when not empty).
    fn range_raw(&mut self, field: Field, path: &str, lo: Bound<Term>, hi: Bound<Term>) -> Boxed {
        if self.c.split.get_field_entry(field).is_fast() {
            let mut name = self.entry_name(field);
            if !path.is_empty() {
                name = format!("{name}.{path}");
            }
            self.warm_fast(name, false);
        } else {
            self.warm_range(field, lo.clone(), hi.clone(), None, false);
        }
        Box::new(RangeQuery::new(lo, hi))
    }

    // ---- the variants (rule 3) ----

    fn query(&mut self, query: &Query) -> Result<Boxed, ServiceError> {
        match query {
            Query::MatchAll => Ok(self.constant(Box::new(AllQuery))),
            Query::MatchNone => Ok(empty()),
            Query::Match {
                field,
                text,
                operator,
                minimum_should_match,
                fuzziness,
                analyzer,
            } => self.match_query(
                field,
                text,
                *operator,
                minimum_should_match.as_deref(),
                *fuzziness,
                analyzer.as_deref(),
            ),
            Query::MatchPhrase { field, text, slop } => self.phrase(field, text, *slop),
            Query::MultiMatch {
                fields,
                text,
                kind,
                operator,
                tie_breaker,
            } => self.multi_match(fields, text, *kind, *operator, tie_breaker.unwrap_or(0.0)),
            Query::Term { field, value } => self.term(field, value),
            Query::Terms { field, values } => self.terms(field, values),
            Query::Range {
                field,
                gt,
                gte,
                lt,
                lte,
            } => self.range(field, gt, gte, lt, lte),
            Query::Exists { field } => self.exists(field),
            Query::IsNull { field } => self.is_null(field),
            Query::IsEmpty { field } => {
                let is_empty = self.is_empty_raw(field)?;
                Ok(self.constant(is_empty))
            }
            Query::ValuesCount {
                field,
                gt,
                gte,
                lt,
                lte,
            } => self.values_count(field, *gt, *gte, *lt, *lte),
            Query::Prefix { field, value } => self.pattern(field, value, prefix_regex),
            Query::Wildcard { field, pattern } => self.pattern(field, pattern, wildcard_regex),
            Query::Fuzzy {
                field,
                value,
                fuzziness,
            } => self.fuzzy(field, value, *fuzziness),
            Query::Ids(pks) => Ok(self.ids(pks.iter().cloned())),
            Query::QueryString {
                query,
                default_fields,
                default_operator,
            } => self.query_string(query, default_fields, *default_operator),
            Query::Bool {
                must,
                should,
                must_not,
                filter,
                minimum_should_match,
            } => self.bool_query(
                must,
                should,
                must_not,
                filter,
                minimum_should_match.as_deref(),
            ),
            Query::Boost { query, boost } => {
                let inner = self.query(query)?;
                Ok(Box::new(BoostQuery::new(inner, *boost)))
            }
            Query::ConstantScore { query, score } => {
                let inner = self.query(query)?;
                let score = match self.mode {
                    CompileMode::Scoring => *score,
                    CompileMode::Filter => 0.0,
                };
                Ok(Box::new(ConstScoreQuery::new(inner, score)))
            }
        }
    }

    /// The analyzed tokens of `text` for text field `name`, as terms: a
    /// `Text` field with `analyzer` (or its own), a JSON path through
    /// `_text.<n>` with `analyzer` (or `standard`). `Ok(None)` for another
    /// kind; an empty list for a field the split lacks.
    fn text_terms(
        &self,
        name: &str,
        text: &str,
        analyzer: Option<&str>,
        need_positions: bool,
    ) -> Result<Option<Vec<Token>>, ServiceError> {
        let (field, path, analyzer) = match self.c.resolve(name) {
            Target::Missing => return Ok(Some(Vec::new())),
            Target::Plain { spec, field } => match &spec.kind {
                FieldKind::Text {
                    analyzer: own,
                    positions,
                } => {
                    if need_positions && !positions {
                        return Err(invalid(format!(
                            "field {name} was indexed without positions"
                        )));
                    }
                    (field, None, analyzer.unwrap_or(own).to_string())
                }
                FieldKind::Json => return Err(json_whole(name)),
                _ => return Ok(None),
            },
            Target::Json { spec, path } => match self.companion(spec, text_companion) {
                Some(field) => (field, Some(path), analyzer.unwrap_or(STANDARD).to_string()),
                None => return Ok(Some(Vec::new())),
            },
        };
        let tokens = self.c.analyze(&analyzer, text)?;
        Ok(Some(
            tokens
                .into_iter()
                .map(|(position, token)| {
                    let term = match &path {
                        Some(path) => json_str(field, path, &token),
                        None => Term::from_field_text(field, &token),
                    };
                    (position, token, term)
                })
                .collect(),
        ))
    }

    fn match_query(
        &mut self,
        name: &str,
        text: &str,
        operator: BoolOperator,
        minimum_should_match: Option<&str>,
        fuzziness: Option<Fuzziness>,
        analyzer: Option<&str>,
    ) -> Result<Boxed, ServiceError> {
        let Some(terms) = self.text_terms(name, text, analyzer, false)? else {
            // Keyword and Uuid: the exact text; numeric, bool, date: a term.
            return self.term(name, &FieldValue::Str(text.to_string()));
        };
        let n = terms.len();
        let minimum = minimum_should_match
            .map(|spec| parse_minimum_should_match(spec, n))
            .transpose()?;
        let mut leaves: Vec<Boxed> = terms
            .into_iter()
            .map(|(_, token, term)| match fuzziness {
                Some(fuzziness) => {
                    self.warm_dict(term.field());
                    let edits = fuzziness_edits(fuzziness, &token);
                    self.constant(Box::new(FuzzyTermQuery::new(term, edits, true)))
                }
                None => self.scored_term(term),
            })
            .collect();
        if leaves.is_empty() {
            return Ok(empty());
        }
        if leaves.len() == 1 {
            return Ok(leaves.pop().expect("one leaf"));
        }
        let occur = match operator {
            BoolOperator::Or => Occur::Should,
            BoolOperator::And => Occur::Must,
        };
        let mut query = BooleanQuery::new(leaves.into_iter().map(|leaf| (occur, leaf)).collect());
        if let (BoolOperator::Or, Some(minimum)) = (operator, minimum) {
            query.set_minimum_number_should_match(minimum);
        }
        Ok(Box::new(query))
    }

    fn phrase(&mut self, name: &str, text: &str, slop: u32) -> Result<Boxed, ServiceError> {
        let Some(terms) = self.text_terms(name, text, None, true)? else {
            return self.match_query(name, text, BoolOperator::Or, None, None, None);
        };
        let mut terms: Vec<(usize, Term)> = terms.into_iter().map(|(p, _, t)| (p, t)).collect();
        Ok(match terms.len() {
            0 => empty(),
            1 => {
                let (_, term) = terms.pop().expect("one term");
                self.scored_term(term)
            }
            _ => {
                for (_, term) in &terms {
                    self.warm_term(term, true);
                }
                self.scored(Box::new(PhraseQuery::new_with_offset_and_slop(terms, slop)))
            }
        })
    }

    /// A phrase whose last token is a prefix (at most 50 expansions); one
    /// token is a prefix query.
    fn phrase_prefix(&mut self, name: &str, text: &str) -> Result<Boxed, ServiceError> {
        let Some(terms) = self.text_terms(name, text, None, false)? else {
            return self.pattern(name, text, prefix_regex);
        };
        if terms.len() == 1 {
            let (_, token, term) = &terms[0];
            let pattern = prefix_regex(token);
            let leaf: Boxed = match term.get_json_path() {
                None => Box::new(
                    RegexQuery::from_pattern(&pattern, term.field())
                        .map_err(|err| invalid(format!("prefix {token:?}: {err}")))?,
                ),
                Some(path) => self.json_automaton(term.field(), &path, &pattern)?,
            };
            if term.get_json_path().is_none() {
                self.warm_automaton(term.field(), None, pattern);
            }
            return Ok(self.constant(leaf));
        }
        // Several tokens need positions.
        let Some(terms) = self.text_terms(name, text, None, true)? else {
            unreachable!("the field's kind did not change");
        };
        let Some((_, _, last)) = terms.last() else {
            return Ok(empty());
        };
        let end = prefix_end(last.serialized_value_bytes()).map(|bytes| {
            let mut end = last.clone();
            end.set_bytes(&bytes);
            end
        });
        self.warm_range(
            last.field(),
            Bound::Included(last.clone()),
            end.map_or(Bound::Unbounded, Bound::Excluded),
            Some(u64::from(PHRASE_PREFIX_EXPANSIONS)),
            true,
        );
        let terms: Vec<(usize, Term)> = terms.into_iter().map(|(p, _, t)| (p, t)).collect();
        for (_, term) in &terms[..terms.len() - 1] {
            self.warm_term(term, true);
        }
        let mut query = PhrasePrefixQuery::new_with_offset(terms);
        query.set_max_expansions(PHRASE_PREFIX_EXPANSIONS);
        Ok(self.scored(Box::new(query)))
    }

    fn multi_match(
        &mut self,
        fields: &[(String, f32)],
        text: &str,
        kind: MultiMatchKind,
        operator: BoolOperator,
        tie_breaker: f32,
    ) -> Result<Boxed, ServiceError> {
        if kind == MultiMatchKind::CrossFields {
            return self.cross_fields(fields, text, operator, tie_breaker);
        }
        let mut per_field = Vec::with_capacity(fields.len());
        for (name, boost) in fields {
            let query = match kind {
                MultiMatchKind::Phrase => self.phrase(name, text, 0)?,
                MultiMatchKind::PhrasePrefix => self.phrase_prefix(name, text)?,
                _ => self.match_query(name, text, operator, None, None, None)?,
            };
            per_field.push(Box::new(BoostQuery::new(query, *boost)) as Boxed);
        }
        if per_field.is_empty() {
            return Ok(empty());
        }
        Ok(match kind {
            MultiMatchKind::MostFields => Box::new(BooleanQuery::new(
                per_field.into_iter().map(|q| (Occur::Should, q)).collect(),
            )),
            _ => Box::new(DisjunctionMaxQuery::with_tie_breaker(
                per_field,
                tie_breaker,
            )),
        })
    }

    /// Term-centric: per distinct token, the best field (plus the tie
    /// breaker times the others); tokens combine by `operator`. IDF is not
    /// blended across fields. Text, Keyword and Uuid fields and JSON paths
    /// take part.
    fn cross_fields(
        &mut self,
        fields: &[(String, f32)],
        text: &str,
        operator: BoolOperator,
        tie_breaker: f32,
    ) -> Result<Boxed, ServiceError> {
        // Token → (boost, term) per field that produced it, in first-seen order.
        let mut tokens: Vec<(String, Vec<(f32, Term)>)> = Vec::new();
        for (name, boost) in fields {
            let terms: Vec<(String, Term)> = match self.text_terms(name, text, None, false)? {
                Some(terms) => terms.into_iter().map(|(_, s, t)| (s, t)).collect(),
                None => match self.c.resolve(name) {
                    Target::Plain { spec, field }
                        if matches!(spec.kind, FieldKind::Keyword | FieldKind::Uuid) =>
                    {
                        match coerce_term(&spec.kind, name, &FieldValue::Str(text.to_string()))? {
                            Coerced::Str(s) => vec![(s.clone(), Term::from_field_text(field, &s))],
                            _ => Vec::new(),
                        }
                    }
                    _ => Vec::new(),
                },
            };
            for (token, term) in terms {
                let at = match tokens.iter().position(|(t, _)| *t == token) {
                    Some(at) => at,
                    None => {
                        tokens.push((token, Vec::new()));
                        tokens.len() - 1
                    }
                };
                let entry = &mut tokens[at].1;
                if !entry.iter().any(|(_, t)| *t == term) {
                    entry.push((*boost, term));
                }
            }
        }
        if tokens.is_empty() {
            return Ok(empty());
        }
        let occur = match operator {
            BoolOperator::Or => Occur::Should,
            BoolOperator::And => Occur::Must,
        };
        let mut clauses = Vec::with_capacity(tokens.len());
        for (_, terms) in tokens {
            let disjuncts: Vec<Boxed> = terms
                .into_iter()
                .map(|(boost, term)| {
                    let leaf = self.scored_term(term);
                    Box::new(BoostQuery::new(leaf, boost)) as Boxed
                })
                .collect();
            let per_token: Boxed = Box::new(DisjunctionMaxQuery::with_tie_breaker(
                disjuncts,
                tie_breaker,
            ));
            clauses.push((occur, per_token));
        }
        Ok(Box::new(BooleanQuery::new(clauses)))
    }

    fn ids(&mut self, pks: impl IntoIterator<Item = PrimaryKey>) -> Boxed {
        let Some(pk_field) = self.c.field(PK_FIELD) else {
            return empty();
        };
        let terms: Vec<Term> = pks
            .into_iter()
            .map(|pk| Term::from_field_bytes(pk_field, &pk.canonical()))
            .collect();
        if terms.is_empty() {
            return empty();
        }
        for term in &terms {
            self.warm_term(term, false);
        }
        self.constant(Box::new(TermSetQuery::new(terms)))
    }

    fn term(&mut self, name: &str, value: &FieldValue) -> Result<Boxed, ServiceError> {
        if name == ID_FIELD {
            return Ok(self.ids(id_of(value)));
        }
        match self.c.resolve(name) {
            Target::Missing => Ok(empty()),
            Target::Plain { spec, field } => {
                let coerced = coerce_term(&spec.kind, name, value)?;
                let Some(term) = plain_term(field, &coerced) else {
                    return Ok(empty());
                };
                let bm25 = matches!(spec.kind, FieldKind::Text { .. } | FieldKind::Keyword);
                Ok(self.exact(field, term, bm25))
            }
            Target::Json { spec, path } => match value {
                FieldValue::Date(_) => {
                    let leaf = self.json_date_term(spec, &path, value);
                    Ok(self.constant(leaf))
                }
                _ => {
                    let Some(main) = self.c.field(&spec.name) else {
                        return Ok(empty());
                    };
                    let term = json_value_term(main, &path, value).expect("not a date");
                    if matches!(value, FieldValue::Str(_)) {
                        Ok(self.scored_term(term))
                    } else {
                        self.warm_term(&term, false);
                        Ok(self.constant(Box::new(TermQuery::new(term, IndexRecordOption::Basic))))
                    }
                }
            },
        }
    }

    /// The unscored leaf of a JSON date at `path`: `_date.<n>` at ms
    /// precision, empty unless the µs are whole ms.
    fn json_date_term(&mut self, spec: &FieldSpec, path: &str, value: &FieldValue) -> Boxed {
        let Some(date_field) = self.companion(spec, date_companion) else {
            return empty();
        };
        match coerce_term(&FieldKind::Date, &spec.name, value) {
            Ok(Coerced::DateMs(ms)) => {
                self.json_date_range(date_field, path, Bound::Included(ms), Bound::Included(ms))
            }
            _ => empty(),
        }
    }

    fn terms(&mut self, name: &str, values: &[FieldValue]) -> Result<Boxed, ServiceError> {
        if name == ID_FIELD {
            return Ok(self.ids(values.iter().filter_map(id_of)));
        }
        let mut set = Vec::new();
        let mut others: Vec<Boxed> = Vec::new();
        match self.c.resolve(name) {
            Target::Missing => return Ok(empty()),
            Target::Plain { spec, field } => {
                let fast = self.exact_is_fast(field);
                for value in values {
                    let Some(term) = plain_term(field, &coerce_term(&spec.kind, name, value)?)
                    else {
                        continue;
                    };
                    if fast {
                        others.push(self.exact_raw(field, term));
                    } else {
                        set.push(term);
                    }
                }
            }
            Target::Json { spec, path } => {
                let main = self.c.field(&spec.name);
                for value in values {
                    match (value, main) {
                        (FieldValue::Date(_), _) => {
                            others.push(self.json_date_term(spec, &path, value));
                        }
                        (_, Some(main)) => {
                            set.push(json_value_term(main, &path, value).expect("not a date"));
                        }
                        (_, None) => {}
                    }
                }
            }
        }
        if !set.is_empty() {
            for term in &set {
                self.warm_term(term, false);
            }
            others.push(Box::new(TermSetQuery::new(set)));
        }
        Ok(match others.len() {
            0 => empty(),
            1 => {
                let leaf = others.pop().expect("one leaf");
                self.constant(leaf)
            }
            _ => {
                let any =
                    BooleanQuery::new(others.into_iter().map(|q| (Occur::Should, q)).collect());
                self.constant(Box::new(any))
            }
        })
    }

    fn range(
        &mut self,
        name: &str,
        gt: &Option<FieldValue>,
        gte: &Option<FieldValue>,
        lt: &Option<FieldValue>,
        lte: &Option<FieldValue>,
    ) -> Result<Boxed, ServiceError> {
        if gt.is_some() && gte.is_some() {
            return Err(invalid(format!("range on {name} sets both gt and gte")));
        }
        if lt.is_some() && lte.is_some() {
            return Err(invalid(format!("range on {name} sets both lt and lte")));
        }
        let lower = gt
            .as_ref()
            .map(|v| (RangeOp::Gt, v))
            .or(gte.as_ref().map(|v| (RangeOp::Gte, v)));
        let upper = lt
            .as_ref()
            .map(|v| (RangeOp::Lt, v))
            .or(lte.as_ref().map(|v| (RangeOp::Lte, v)));
        match self.c.resolve(name) {
            Target::Missing => Ok(empty()),
            Target::Plain { spec, field } => {
                if spec.kind == FieldKind::Json {
                    return Err(json_whole(name));
                }
                let bound = |side: Option<(RangeOp, &FieldValue)>| match side {
                    Some((op, value)) => coerce_bound(&spec.kind, name, op, value),
                    None => Ok(Bound::Unbounded),
                };
                let (lo, hi) = (bound(lower)?, bound(upper)?);
                if is_never(&lo) || is_never(&hi) {
                    return Ok(empty());
                }
                if matches!((&lo, &hi), (Bound::Unbounded, Bound::Unbounded)) {
                    return self.exists(name);
                }
                let term = |c: Coerced| plain_term(field, &c).expect("not Never");
                let leaf = self.range_raw(field, "", lo.map(term), hi.map(term));
                Ok(self.constant(leaf))
            }
            Target::Json { spec, path } => self.json_range(name, spec, &path, lower, upper),
        }
    }

    fn json_range(
        &mut self,
        name: &str,
        spec: &FieldSpec,
        path: &str,
        lower: Option<(RangeOp, &FieldValue)>,
        upper: Option<(RangeOp, &FieldValue)>,
    ) -> Result<Boxed, ServiceError> {
        #[derive(PartialEq)]
        enum Class {
            Number,
            Date,
            Str,
            Bool,
        }
        let class = |value: &FieldValue| match value {
            FieldValue::I64(_) | FieldValue::U64(_) | FieldValue::F64(_) => Class::Number,
            FieldValue::Date(_) => Class::Date,
            FieldValue::Str(_) => Class::Str,
            FieldValue::Bool(_) => Class::Bool,
        };
        let classes: Vec<Class> = [lower, upper]
            .into_iter()
            .flatten()
            .map(|(_, v)| class(v))
            .collect();
        let Some(first) = classes.first() else {
            return self.exists(name);
        };
        if classes.iter().any(|c| c != first) {
            return Err(invalid(format!(
                "the range bounds on {name} mix value types"
            )));
        }
        let parts: Vec<Boxed> = match first {
            Class::Bool => {
                return Err(invalid(format!(
                    "cannot range over boolean values on {name}"
                )));
            }
            Class::Str => {
                let Some(main) = self.c.field(&spec.name) else {
                    return Ok(empty());
                };
                let bound = |side: Option<(RangeOp, &FieldValue)>| match side {
                    Some((op, FieldValue::Str(s))) => {
                        let term = json_str(main, path, s);
                        match op {
                            RangeOp::Gte | RangeOp::Lte => Bound::Included(term),
                            RangeOp::Gt | RangeOp::Lt => Bound::Excluded(term),
                        }
                    }
                    _ => Bound::Unbounded,
                };
                vec![self.range_raw(main, path, bound(lower), bound(upper))]
            }
            Class::Date => {
                let Some(date_field) = self.companion(spec, date_companion) else {
                    return Ok(empty());
                };
                let bound = |side: Option<(RangeOp, &FieldValue)>| match side {
                    Some((op, value)) => coerce_bound(&FieldKind::Date, name, op, value),
                    None => Ok(Bound::Unbounded),
                };
                let (lo, hi) = (bound(lower)?, bound(upper)?);
                if is_never(&lo) || is_never(&hi) {
                    return Ok(empty());
                }
                let ms = |c: Coerced| match c {
                    Coerced::DateMs(ms) => ms,
                    other => unreachable!("a date bound is DateMs, not {other:?}"),
                };
                match (lo.map(ms), hi.map(ms)) {
                    (Bound::Unbounded, Bound::Unbounded) => {
                        let name = format!("{}.{path}", date_companion(&spec.name));
                        self.warm_fast(name.clone(), false);
                        vec![Box::new(ExistsQuery::new(name, false)) as Boxed]
                    }
                    (lo, hi) => vec![self.json_date_range(date_field, path, lo, hi)],
                }
            }
            Class::Number => {
                let Some(main) = self.c.field(&spec.name) else {
                    return Ok(empty());
                };
                self.json_number_ranges(name, main, path, lower, upper)?
            }
        };
        Ok(match parts.len() {
            0 => empty(),
            1 => {
                let mut parts = parts;
                let leaf = parts.pop().expect("one part");
                self.constant(leaf)
            }
            _ => {
                let any =
                    BooleanQuery::new(parts.into_iter().map(|q| (Occur::Should, q)).collect());
                self.constant(Box::new(any))
            }
        })
    }

    /// `Should[i64 range (bounds rounded as for I64 fields), f64 range, u64
    /// range when a bound exceeds i64::MAX]` over the JSON numbers at
    /// `path` of `main`.
    fn json_number_ranges(
        &mut self,
        name: &str,
        main: Field,
        path: &str,
        lower: Option<(RangeOp, &FieldValue)>,
        upper: Option<(RangeOp, &FieldValue)>,
    ) -> Result<Vec<Boxed>, ServiceError> {
        let mut parts = Vec::new();
        let json = |value: Coerced| {
            let mut term = json_term(main, path);
            match value {
                Coerced::I64(n) => term.append_type_and_fast_value(n),
                Coerced::F64(x) => term.append_type_and_fast_value(x),
                other => unreachable!("a numeric bound, not {other:?}"),
            }
            term
        };
        for kind in [FieldKind::I64, FieldKind::F64] {
            let bound = |side: Option<(RangeOp, &FieldValue)>| match side {
                Some((op, value)) => coerce_bound(&kind, name, op, value),
                None => Ok(Bound::Unbounded),
            };
            let (lo, hi) = (bound(lower)?, bound(upper)?);
            if is_never(&lo)
                || is_never(&hi)
                || matches!((&lo, &hi), (Bound::Unbounded, Bound::Unbounded))
            {
                continue;
            }
            parts.push(self.range_raw(main, path, lo.map(json), hi.map(json)));
        }
        let huge = |side: Option<(RangeOp, &FieldValue)>| match side {
            Some((_, FieldValue::U64(n))) => i64::try_from(*n).is_err(),
            Some((_, FieldValue::F64(x))) => *x >= 9_223_372_036_854_775_808.0,
            _ => false,
        };
        if huge(lower) || huge(upper) {
            let bound = |side: Option<(RangeOp, &FieldValue)>| -> Option<Bound<u64>> {
                let Some((op, value)) = side else {
                    return Some(Bound::Unbounded);
                };
                let lower_side = matches!(op, RangeOp::Gt | RangeOp::Gte);
                let x = match value {
                    FieldValue::U64(n) => *n as f64,
                    FieldValue::I64(n) => *n as f64,
                    FieldValue::F64(x) => *x,
                    _ => return Some(Bound::Unbounded),
                };
                let rounded = match op {
                    RangeOp::Gte | RangeOp::Lt => x.ceil(),
                    RangeOp::Gt | RangeOp::Lte => x.floor(),
                };
                if rounded < 0.0 {
                    // Below every u64: no lower bound, or an upper bound
                    // that matches nothing.
                    return lower_side.then_some(Bound::Unbounded);
                }
                if rounded >= 18_446_744_073_709_551_616.0 {
                    return (!lower_side).then_some(Bound::Unbounded);
                }
                let v = match value {
                    FieldValue::U64(n) => *n,
                    _ => rounded as u64,
                };
                Some(match op {
                    RangeOp::Gte | RangeOp::Lte => Bound::Included(v),
                    RangeOp::Gt | RangeOp::Lt => Bound::Excluded(v),
                })
            };
            if let (Some(lo), Some(hi)) = (bound(lower), bound(upper))
                && !matches!((&lo, &hi), (Bound::Unbounded, Bound::Unbounded))
            {
                let term = |n: u64| {
                    let mut term = json_term(main, path);
                    term.append_type_and_fast_value(n);
                    term
                };
                parts.push(self.range_raw(main, path, lo.map(term), hi.map(term)));
            }
        }
        Ok(parts)
    }

    /// The unscored leaf of `Exists`.
    fn exists_raw(&mut self, name: &str) -> Result<Boxed, ServiceError> {
        if name == ID_FIELD {
            return Ok(Box::new(AllQuery));
        }
        Ok(match self.c.resolve(name) {
            Target::Missing => empty(),
            Target::Plain { spec, .. } if spec.kind == FieldKind::Json => {
                self.warm_fast(spec.name.clone(), true);
                Box::new(ExistsQuery::new(spec.name.clone(), true))
            }
            Target::Plain { spec, .. } => {
                let context = BuildTantivyAstContext {
                    schema: self.c.split,
                    tokenizer_manager: self.c.tokenizers,
                    search_fields: &[],
                    with_validation: true,
                };
                let ast = QueryAst::FieldPresence(FieldPresenceQuery {
                    field: spec.name.clone(),
                });
                let (query, mut warmup) = build_query(ast, &context, None)
                    .map_err(|err| ServiceError::Internal(format!("exists on {name}: {err}")))?;
                warmup.required_terms.clear();
                self.warmup.merge(warmup);
                query
            }
            Target::Json { spec, path } => {
                if self.c.field(&spec.name).is_none() {
                    return Ok(empty());
                }
                let full = format!("{}.{path}", spec.name);
                self.warm_fast(full.clone(), true);
                Box::new(ExistsQuery::new(full, true))
            }
        })
    }

    fn exists(&mut self, name: &str) -> Result<Boxed, ServiceError> {
        let leaf = self.exists_raw(name)?;
        Ok(self.constant(leaf))
    }

    /// `Bool { must: [AllQuery], must_not: [Exists] }`, unscored.
    fn is_empty_raw(&mut self, name: &str) -> Result<Boxed, ServiceError> {
        let exists = self.exists_raw(name)?;
        Ok(Box::new(BooleanQuery::new(vec![
            (Occur::Must, Box::new(AllQuery) as Boxed),
            (Occur::MustNot, exists),
        ])))
    }

    fn is_null(&mut self, name: &str) -> Result<Boxed, ServiceError> {
        match self.c.resolve(name) {
            Target::Missing => Ok(empty()),
            Target::Plain { .. } => Err(invalid("is_null is supported on JSON field paths only")),
            Target::Json { spec, path } => {
                let Some(nulls) = self.companion(spec, null_companion) else {
                    return Ok(empty());
                };
                let leaf = self.exact_raw(nulls, Term::from_field_text(nulls, &path));
                Ok(self.constant(leaf))
            }
        }
    }

    fn values_count(
        &mut self,
        name: &str,
        gt: Option<u64>,
        gte: Option<u64>,
        lt: Option<u64>,
        lte: Option<u64>,
    ) -> Result<Boxed, ServiceError> {
        if gt.is_some() && gte.is_some() {
            return Err(invalid(format!(
                "values_count on {name} sets both gt and gte"
            )));
        }
        if lt.is_some() && lte.is_some() {
            return Err(invalid(format!(
                "values_count on {name} sets both lt and lte"
            )));
        }
        let lo = gt
            .map(Bound::Excluded)
            .or(gte.map(Bound::Included))
            .unwrap_or(Bound::Unbounded);
        let hi = lt
            .map(Bound::Excluded)
            .or(lte.map(Bound::Included))
            .unwrap_or(Bound::Unbounded);
        let admits_zero = matches!(lo, Bound::Unbounded | Bound::Included(0))
            && !matches!(hi, Bound::Excluded(0));
        let unbounded = matches!((&lo, &hi), (Bound::Unbounded, Bound::Unbounded));
        let (spec, path) = match self.c.resolve(name) {
            Target::Plain { .. } => {
                return Err(invalid(
                    "values_count is supported on JSON field paths only",
                ));
            }
            // An unknown field has no values anywhere.
            Target::Missing => {
                return Ok(if admits_zero {
                    self.constant(Box::new(AllQuery))
                } else {
                    empty()
                });
            }
            Target::Json { spec, path } => (spec, path),
        };
        if unbounded {
            return Ok(self.constant(Box::new(AllQuery)));
        }
        let mut parts: Vec<Boxed> = Vec::new();
        if let Some(counts) = self.companion(spec, count_companion) {
            let term = |n: u64| {
                let mut term = json_term(counts, &path);
                term.append_type_and_fast_value(n);
                term
            };
            parts.push(self.range_raw(counts, &path, lo.map(term), hi.map(term)));
        }
        if admits_zero {
            parts.push(self.is_empty_raw(name)?);
        }
        Ok(match parts.len() {
            0 => empty(),
            _ => {
                let any =
                    BooleanQuery::new(parts.into_iter().map(|q| (Occur::Should, q)).collect());
                self.constant(Box::new(any))
            }
        })
    }

    /// An automaton over the JSON strings of `field` at `path`.
    fn json_automaton(
        &mut self,
        field: Field,
        path: &str,
        pattern: &str,
    ) -> Result<Boxed, ServiceError> {
        let regex = tantivy_fst::Regex::new(pattern)
            .map_err(|err| invalid(format!("pattern {pattern:?}: {err}")))?;
        let prefix = json_path_prefix(field, path);
        self.warm_automaton(field, Some(prefix.clone()), pattern.to_string());
        Ok(Box::new(AutomatonQuery {
            field,
            automaton: Arc::new(JsonPathPrefix {
                prefix,
                automaton: Arc::new(regex),
            }),
        }))
    }

    /// `Prefix` and `Wildcard`: `regex(value)` over a text, keyword or uuid
    /// field (lowercased first on a lowercasing `Text` analyzer), or over
    /// the raw strings of a JSON path.
    fn pattern(
        &mut self,
        name: &str,
        value: &str,
        regex: fn(&str) -> String,
    ) -> Result<Boxed, ServiceError> {
        match self.c.resolve(name) {
            Target::Missing => Ok(empty()),
            Target::Plain { spec, field } => {
                let pattern = match &spec.kind {
                    FieldKind::Text { analyzer, .. } => {
                        if self.c.tokenizers.tokenizer_does_lowercasing(analyzer) == Some(true) {
                            regex(&value.to_lowercase())
                        } else {
                            regex(value)
                        }
                    }
                    FieldKind::Keyword | FieldKind::Uuid => regex(value),
                    FieldKind::Json => return Err(json_whole(name)),
                    _ => {
                        return Err(invalid(format!(
                            "prefix and wildcard queries need a text, keyword or JSON field, not {name}"
                        )));
                    }
                };
                let query = RegexQuery::from_pattern(&pattern, field)
                    .map_err(|err| invalid(format!("pattern {value:?}: {err}")))?;
                self.warm_automaton(field, None, pattern);
                Ok(self.constant(Box::new(query)))
            }
            Target::Json { spec, path } => {
                let Some(main) = self.c.field(&spec.name) else {
                    return Ok(empty());
                };
                let leaf = self.json_automaton(main, &path, &regex(value))?;
                Ok(self.constant(leaf))
            }
        }
    }

    fn fuzzy(
        &mut self,
        name: &str,
        value: &str,
        fuzziness: Fuzziness,
    ) -> Result<Boxed, ServiceError> {
        let term = match self.c.resolve(name) {
            Target::Missing => return Ok(empty()),
            Target::Plain { spec, field } => match spec.kind {
                FieldKind::Text { .. } | FieldKind::Keyword | FieldKind::Uuid => {
                    Term::from_field_text(field, value)
                }
                FieldKind::Json => return Err(json_whole(name)),
                _ => {
                    return Err(invalid(format!(
                        "fuzzy queries need a text, keyword or JSON field, not {name}"
                    )));
                }
            },
            Target::Json { spec, path } => match self.c.field(&spec.name) {
                Some(main) => json_str(main, &path, value),
                None => return Ok(empty()),
            },
        };
        self.warm_dict(term.field());
        let edits = fuzziness_edits(fuzziness, value);
        Ok(self.constant(Box::new(FuzzyTermQuery::new(term, edits, true))))
    }

    fn query_string(
        &mut self,
        query: &str,
        default_fields: &[String],
        operator: BoolOperator,
    ) -> Result<Boxed, ServiceError> {
        let fields: Vec<Field> = default_fields
            .iter()
            .filter_map(|name| match self.c.resolve(name) {
                Target::Plain { field, .. } => Some(field),
                Target::Json { .. } | Target::Missing => None,
            })
            .collect();
        if fields.is_empty() {
            return Ok(empty());
        }
        let mut parser = QueryParser::new(
            self.c.split.clone(),
            fields.clone(),
            self.c.tokenizers.tantivy_manager().clone(),
        );
        if operator == BoolOperator::And {
            parser.set_conjunction_by_default();
        }
        let parsed = parser
            .parse_query(query)
            .map_err(|err| invalid(format!("query_string: {err}")))?;
        for field in fields {
            self.warm_dict(field);
        }
        let mut terms: BTreeMap<Term, bool> = BTreeMap::new();
        parsed.query_terms(&mut |term, positions| {
            *terms.entry(term.clone()).or_default() |= positions;
        });
        for (term, positions) in terms {
            self.warm_dict(term.field());
            self.warm_term(&term, positions);
        }
        Ok(self.scored(parsed))
    }

    fn bool_query(
        &mut self,
        must: &[Query],
        should: &[Query],
        must_not: &[Query],
        filter: &[Query],
        minimum_should_match: Option<&str>,
    ) -> Result<Boxed, ServiceError> {
        let mut clauses: Vec<(Occur, Boxed)> = Vec::new();
        for query in must {
            clauses.push((Occur::Must, self.query(query)?));
        }
        for query in should {
            clauses.push((Occur::Should, self.query(query)?));
        }
        for query in must_not {
            clauses.push((Occur::MustNot, self.query(query)?));
        }
        for query in filter {
            let inner = self.query(query)?;
            clauses.push((Occur::Must, Box::new(ConstScoreQuery::new(inner, 0.0))));
        }
        if clauses.is_empty() {
            return Ok(self.constant(Box::new(AllQuery)));
        }
        if must.is_empty() && should.is_empty() && filter.is_empty() {
            // A pure must_not matches the rest with score 0.
            clauses.push((
                Occur::Must,
                Box::new(ConstScoreQuery::new(Box::new(AllQuery), 0.0)),
            ));
        }
        let minimum = match minimum_should_match {
            Some(spec) => parse_minimum_should_match(spec, should.len())?,
            None if must.is_empty() && filter.is_empty() && !should.is_empty() => 1,
            None => 0,
        };
        let mut query = BooleanQuery::new(clauses);
        if minimum > 0 {
            query.set_minimum_number_should_match(minimum);
        }
        Ok(Box::new(query))
    }
}
