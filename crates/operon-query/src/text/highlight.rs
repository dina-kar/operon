//! Highlighting (plan M1.2 Task 8 rule 6): snippets of the requested text
//! fields of each hit, with the query terms marked by the field's tags.
//!
//! Terms are weighted by `1 / (1 + df)` with the request's global
//! statistics (Ruling 2), so fragment choice is identical on every
//! placement. Tags are inserted without HTML escaping (ES's default
//! encoder).

use std::collections::{BTreeMap, BTreeSet};

use operon_collection::{CollectionSchema, FieldKind, coerce, extract};
use operon_quickwit::doc_mapper::WarmupInfo;
use tantivy::schema::Field;
use tantivy::snippet::{SnippetGenerator, collapse_overlapped_ranges};
use tantivy::{Score, Term};

use crate::error::ServiceError;
use crate::exec::doc_fetch::FetchedRow;
use crate::ir::{Highlight, HighlightField, Retriever, SearchRequest};
use crate::read::ReadView;
use crate::text::compile::highlight_terms;
use crate::text::fields::{ResolvedField, resolve_field};
use crate::text::query_tokenizers;
use crate::text::splits::open_splits_with;
use crate::text::stats::{GlobalStats, StatsCache, StatsTerm};

/// The analyzer of a highlighted field; any field that is not `Text` is
/// `InvalidArgument`.
fn analyzer_of<'a>(schema: &'a CollectionSchema, field: &str) -> Result<&'a str, ServiceError> {
    match resolve_field(schema, field) {
        ResolvedField::Plain { spec } => match &spec.kind {
            FieldKind::Text { analyzer, .. } => Ok(analyzer),
            _ => Err(not_text(field)),
        },
        _ => Err(not_text(field)),
    }
}

fn not_text(field: &str) -> ServiceError {
    ServiceError::InvalidArgument(format!("highlighting needs a text field: {field}"))
}

/// Checks that every highlighted field is a `Text` field.
pub fn check_highlight(
    schema: &CollectionSchema,
    highlight: &Highlight,
) -> Result<(), ServiceError> {
    for field in &highlight.fields {
        analyzer_of(schema, &field.field)?;
    }
    Ok(())
}

/// Every `Text` retriever's query, nested ones included.
fn text_queries(retrievers: &[Retriever]) -> Vec<&crate::ir::Query> {
    let mut out = Vec::new();
    let mut stack: Vec<&Retriever> = retrievers.iter().rev().collect();
    while let Some(retriever) = stack.pop() {
        match retriever {
            Retriever::Text { query, .. } => out.push(query),
            Retriever::Fused { inputs, .. } => stack.extend(inputs.iter().rev()),
            Retriever::Rescore { input, .. } => stack.push(input),
            Retriever::Vector { .. } | Retriever::Sparse { .. } => {}
        }
    }
    out
}

/// The analyzed query terms of `field` over every `Text` retriever, each
/// once, first occurrence first (rule 6.1).
fn field_terms(schema: &CollectionSchema, request: &SearchRequest, field: &str) -> Vec<String> {
    let tokenizers = query_tokenizers();
    let mut out: Vec<String> = Vec::new();
    for query in text_queries(&request.retrievers) {
        for (name, text) in highlight_terms(schema, query, &tokenizers) {
            if name == field && !out.contains(&text) {
                out.push(text);
            }
        }
    }
    out
}

/// The statistics terms of every highlighted field's query terms.
fn stats_terms(schema: &CollectionSchema, request: &SearchRequest) -> BTreeSet<StatsTerm> {
    let Some(highlight) = &request.highlight else {
        return BTreeSet::new();
    };
    highlight
        .fields
        .iter()
        .flat_map(|field| {
            field_terms(schema, request, &field.field)
                .into_iter()
                .map(|text| (field.field.clone(), text.into_bytes()))
        })
        .collect()
}

/// The global document frequencies of the highlighted terms (rule 6.2):
/// the view's splits opened warm for those terms, then
/// [`GlobalStats::compute`].
pub async fn highlight_stats(
    view: &ReadView,
    request: &SearchRequest,
    cache: &StatsCache,
    parallelism: usize,
) -> Result<GlobalStats, ServiceError> {
    let schema = &view.collection.schema;
    let terms = stats_terms(schema, request);
    if terms.is_empty() {
        return Ok(GlobalStats::default());
    }
    let warm = |split: &tantivy::schema::Schema| {
        let mut warmup = WarmupInfo::default();
        for (name, bytes) in &terms {
            let Ok(field) = split.get_field(name) else {
                continue;
            };
            let text = String::from_utf8_lossy(bytes);
            warmup
                .terms_grouped_by_field
                .entry(field)
                .or_default()
                .insert(Term::from_field_text(field, &text), false);
        }
        Ok(warmup)
    };
    let splits = open_splits_with(view, &warm, parallelism).await?;
    GlobalStats::compute(view, &splits, &terms, &BTreeSet::new(), cache, parallelism).await
}

/// `fragment` with `pre`/`post` around each highlighted range.
fn render(fragment: &str, ranges: &[std::ops::Range<usize>], pre: &str, post: &str) -> String {
    let mut out = String::with_capacity(fragment.len() + ranges.len() * (pre.len() + post.len()));
    let mut at = 0;
    for range in collapse_overlapped_ranges(ranges) {
        out.push_str(&fragment[at..range.start]);
        out.push_str(pre);
        out.push_str(&fragment[range.start..range.end]);
        out.push_str(post);
        at = range.end;
    }
    out.push_str(&fragment[at..]);
    out
}

/// The snippets of one field of one doc.
fn field_snippets(
    schema: &CollectionSchema,
    field: &HighlightField,
    terms: &BTreeMap<String, Score>,
    row: &FetchedRow,
) -> Result<Vec<String>, ServiceError> {
    let Some(source) = &row.source else {
        return Ok(Vec::new());
    };
    let ResolvedField::Plain { spec } = resolve_field(schema, &field.field) else {
        return Err(not_text(&field.field));
    };
    let analyzer = analyzer_of(schema, &field.field)?;
    let tokenizer = operon_text::tokenizer_manager()
        .get(analyzer)
        .ok_or_else(|| ServiceError::Internal(format!("no analyzer {analyzer}")))?;
    let mut out = Vec::new();
    for value in extract(source, &spec.source_path) {
        let Ok(Some(operon_collection::IndexValue::Text(text))) =
            coerce(&spec.kind, value.as_ref())
        else {
            continue;
        };
        let whole = field.number_of_fragments == 0;
        let max_num_chars = if whole {
            text.len().max(1)
        } else {
            field.fragment_size.max(1)
        };
        let generator = SnippetGenerator::new(
            terms.clone(),
            tokenizer.clone(),
            Field::from_field_id(0),
            max_num_chars,
        );
        let snippet = generator.snippet(&text);
        if snippet.highlighted().is_empty() {
            continue;
        }
        let rendered = if whole {
            // The snippet starts at offset 0: its ranges index the value.
            render(
                &text,
                snippet.highlighted(),
                &field.pre_tag,
                &field.post_tag,
            )
        } else {
            render(
                snippet.fragment(),
                snippet.highlighted(),
                &field.pre_tag,
                &field.post_tag,
            )
        };
        out.push(rendered);
        if !whole && out.len() >= field.number_of_fragments {
            break;
        }
    }
    Ok(out)
}

/// The highlights of `hits` (rule 6): one map per hit, from each requested
/// field that has a highlighted value to its rendered snippets.
pub fn highlight(
    schema: &CollectionSchema,
    _view: &ReadView,
    request: &SearchRequest,
    stats: &GlobalStats,
    hits: &[FetchedRow],
) -> Result<Vec<BTreeMap<String, Vec<String>>>, ServiceError> {
    let mut out = vec![BTreeMap::new(); hits.len()];
    let Some(highlight) = &request.highlight else {
        return Ok(out);
    };
    check_highlight(schema, highlight)?;
    for field in &highlight.fields {
        let terms: BTreeMap<String, Score> = field_terms(schema, request, &field.field)
            .into_iter()
            .map(|text| {
                let df = stats
                    .doc_freq
                    .get(&(field.field.clone(), text.as_bytes().to_vec()))
                    .copied()
                    .unwrap_or(0);
                (text, 1.0 / (1.0 + df as Score))
            })
            .collect();
        if terms.is_empty() {
            continue;
        }
        for (row, map) in hits.iter().zip(out.iter_mut()) {
            let snippets = field_snippets(schema, field, &terms, row)?;
            if !snippets.is_empty() {
                map.insert(field.field.clone(), snippets);
            }
        }
    }
    Ok(out)
}
