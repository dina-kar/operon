// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// Vendored from quickwit-oss/quickwit af0591a3 (quickwit/quickwit-search/src/leaf.rs lines 281-600 and test 2933-3018); modified for Operon: warm_up_automatons uses tokio spawn_blocking; the Priority and on_absent parameters removed; warmup made pub; imports added; test adapted.

use std::collections::{HashMap, HashSet};

use anyhow::Context;
use futures::future::try_join_all;
use tantivy::fastfield::FastFieldReaders;
use tantivy::schema::Field;
use tantivy::{Searcher, Term};
use tokio_util::sync::CancellationToken;
use tracing::*;

use crate::doc_mapper::{Automaton, FastFieldWarmupInfo, TermRange, WarmupInfo};

/// Runs `fut`, racing it against `cancel`. If cancellation fires first, the
/// (possibly in-flight) future is dropped — aborting its downloads — and
/// `Ok(())` is returned. With no token, `fut` simply runs to completion.
async fn run_cancellable(
    cancel: Option<&CancellationToken>,
    fut: impl std::future::Future<Output = anyhow::Result<()>>,
) -> anyhow::Result<()> {
    let Some(cancel) = cancel else {
        return fut.await;
    };
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Ok(()),
        result = fut => result,
    }
}

/// Tantivy search does not make it possible to fetch data asynchronously during
/// search.
///
/// It is required to download all required information in advance.
/// This is the role of the `warmup` function.
///
/// The downloaded data depends on the query (which term's posting list is required,
/// are position required too), and the collector.
///
/// * `query` - query is used to extract the terms and their fields which will be loaded from the
///   inverted_index.
///
/// * `term_dict_field_names` - A list of fields, where the whole dictionary needs to be loaded.
///   This is e.g. required for term aggregation, since we don't know in advance which terms are
///   going to be hit.
///
/// A required term found to have an empty posting list proves the query empty in this split,
/// so the remaining warmup downloads are then cancelled. This only happens for a
/// single-segment split, where "absent in the split" is sound.
///
/// The CPU-intensive part of warmup (resolving automatons walks the term dictionary) runs on
/// tokio's blocking thread pool.
///
/// Returns whether the query is provably empty in this split (i.e. a required term was absent
/// and warmup was short-circuited).
pub async fn warmup(searcher: &Searcher, warmup_info: &WarmupInfo) -> anyhow::Result<bool> {
    debug!(warmup_info=?warmup_info);

    // Early-abort optimization: the split's downloads can be cancelled as soon as
    // a *required* term is found to have an empty posting list, which proves the
    // query matches nothing here. This conclusion is only sound for the whole
    // split when there is a single segment (the common case in Quickwit), so we
    // only arm the token then. `warm_up_terms` fires the token; every other
    // warmup task observes it through `run_cancellable` and bails.
    let abort_token: Option<CancellationToken> =
        if searcher.segment_readers().len() == 1 && !warmup_info.required_terms.is_empty() {
            Some(CancellationToken::new())
        } else {
            None
        };

    let warm_up_terms_future = warm_up_terms(
        searcher,
        &warmup_info.terms_grouped_by_field,
        &warmup_info.required_terms,
        abort_token.as_ref(),
    )
    .instrument(debug_span!("warm_up_terms"));
    let warm_up_term_ranges_future = run_cancellable(
        abort_token.as_ref(),
        warm_up_term_ranges(searcher, &warmup_info.term_ranges_grouped_by_field),
    )
    .instrument(debug_span!("warm_up_term_ranges"));
    let warm_up_term_dict_future = run_cancellable(
        abort_token.as_ref(),
        warm_up_term_dict_fields(searcher, &warmup_info.term_dict_fields),
    )
    .instrument(debug_span!("warm_up_term_dicts"));
    let warm_up_fastfields_future = run_cancellable(
        abort_token.as_ref(),
        warm_up_fastfields(searcher, &warmup_info.fast_fields),
    )
    .instrument(debug_span!("warm_up_fastfields"));
    let warm_up_fieldnorms_future = run_cancellable(
        abort_token.as_ref(),
        warm_up_fieldnorms(searcher, warmup_info.field_norms),
    )
    .instrument(debug_span!("warm_up_fieldnorms"));
    // TODO merge warm_up_postings into warm_up_term_dict_fields
    let warm_up_postings_future = run_cancellable(
        abort_token.as_ref(),
        warm_up_postings(searcher, &warmup_info.term_dict_fields),
    )
    .instrument(debug_span!("warm_up_postings"));
    let warm_up_automatons_future = run_cancellable(
        abort_token.as_ref(),
        warm_up_automatons(searcher, &warmup_info.automatons_grouped_by_field),
    )
    .instrument(debug_span!("warm_up_automatons"));

    tokio::try_join!(
        warm_up_terms_future,
        warm_up_term_ranges_future,
        warm_up_fastfields_future,
        warm_up_term_dict_future,
        warm_up_fieldnorms_future,
        warm_up_postings_future,
        warm_up_automatons_future,
    )?;

    let provably_empty = match &abort_token {
        Some(abort_token) => abort_token.is_cancelled(),
        None => false,
    };
    Ok(provably_empty)
}

async fn warm_up_term_dict_fields(
    searcher: &Searcher,
    term_dict_fields: &HashSet<Field>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for field in term_dict_fields {
        for segment_reader in searcher.segment_readers() {
            let inverted_index = segment_reader.inverted_index(*field)?.clone();
            warm_up_futures.push(async move {
                let dict = inverted_index.terms();
                dict.warm_up_dictionary().await
            });
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_postings(searcher: &Searcher, fields: &HashSet<Field>) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for field in fields {
        for segment_reader in searcher.segment_readers() {
            let inverted_index = segment_reader.inverted_index(*field)?.clone();
            warm_up_futures.push(async move { inverted_index.warm_postings_full(false).await });
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_fastfield(
    fast_field_reader: &FastFieldReaders,
    fast_field: &FastFieldWarmupInfo,
) -> anyhow::Result<()> {
    let mut columns = fast_field_reader
        .list_dynamic_column_handles(&fast_field.name)
        .await?;
    if fast_field.with_subfields {
        let subpath_columns = fast_field_reader
            .list_subpath_dynamic_column_handles(&fast_field.name)
            .await?;
        columns.extend(subpath_columns);
    }
    futures::future::try_join_all(
        columns
            .into_iter()
            .map(|col| async move { col.file_slice().read_bytes_async().await }),
    )
    .await?;
    Ok(())
}

/// Populates the short-lived cache with the data for
/// all of the fast fields passed as argument.
async fn warm_up_fastfields(
    searcher: &Searcher,
    fast_fields: &HashSet<FastFieldWarmupInfo>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for segment_reader in searcher.segment_readers() {
        let fast_field_reader = segment_reader.fast_fields();
        for fast_field in fast_fields {
            let warm_up_fut = warm_up_fastfield(fast_field_reader, fast_field);
            warm_up_futures.push(Box::pin(warm_up_fut));
        }
    }
    futures::future::try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_terms(
    searcher: &Searcher,
    terms_grouped_by_field: &HashMap<Field, HashMap<Term, bool>>,
    required_terms: &HashSet<Term>,
    abort_token: Option<&CancellationToken>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for (field, terms) in terms_grouped_by_field {
        for segment_reader in searcher.segment_readers() {
            let inv_idx = segment_reader.inverted_index(*field)?;
            for (term, position_needed) in terms.iter() {
                let inv_idx_clone = inv_idx.clone();
                // Only a required term can prove the query empty. When such a
                // term turns out to have an empty posting list, fire the token so
                // the rest of the warmup is cancelled.
                let cancel_on_empty = match abort_token {
                    Some(abort_token) if required_terms.contains(term) => Some(abort_token),
                    _ => None,
                };
                warm_up_futures.push(async move {
                    let found = inv_idx_clone.warm_postings(term, *position_needed).await?;
                    if !found && let Some(abort_token) = cancel_on_empty {
                        // Fire the abort token. This is synchronous, so it runs before any
                        // cancellation can drop us.
                        abort_token.cancel();
                    }
                    anyhow::Ok(())
                });
            }
        }
    }
    // Race against the token so we also stop loading the *other* terms' postings
    // once a required term has proven the query empty.
    run_cancellable(abort_token, async move {
        try_join_all(warm_up_futures).await?;
        anyhow::Ok(())
    })
    .await
}

async fn warm_up_term_ranges(
    searcher: &Searcher,
    terms_grouped_by_field: &HashMap<Field, HashMap<TermRange, bool>>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for (field, terms) in terms_grouped_by_field {
        for segment_reader in searcher.segment_readers() {
            let inv_idx = segment_reader.inverted_index(*field)?;
            for (term_range, position_needed) in terms.iter() {
                let inv_idx_clone = inv_idx.clone();
                let range = (term_range.start.as_ref(), term_range.end.as_ref());
                warm_up_futures.push(async move {
                    inv_idx_clone
                        .warm_postings_range(range, term_range.limit, *position_needed)
                        .await
                });
            }
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_automatons(
    searcher: &Searcher,
    terms_grouped_by_field: &HashMap<Field, HashSet<Automaton>>,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    let cpu_intensive_executor = |task| async move {
        tokio::task::spawn_blocking(task)
            .await
            .map_err(|_| std::io::Error::other("task panicked"))?
    };
    for (field, automatons) in terms_grouped_by_field {
        for segment_reader in searcher.segment_readers() {
            let inv_idx = segment_reader.inverted_index(*field)?;
            for automaton in automatons {
                let inv_idx_clone = inv_idx.clone();
                warm_up_futures.push(async move {
                    match automaton {
                        Automaton::Regex(path, regex_str) => {
                            let regex = tantivy_fst::Regex::new(regex_str)
                                .context("failed to parse regex during warmup")?;
                            inv_idx_clone
                                .warm_postings_automaton(
                                    crate::query::query_ast::JsonPathPrefix {
                                        automaton: regex.into(),
                                        prefix: path.clone().unwrap_or_default(),
                                    },
                                    cpu_intensive_executor,
                                )
                                .await
                                .context("failed to load automaton")
                        }
                    }
                });
            }
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

async fn warm_up_fieldnorms(searcher: &Searcher, requires_scoring: bool) -> anyhow::Result<()> {
    if !requires_scoring {
        return Ok(());
    }
    let mut warm_up_futures = Vec::new();
    for field in searcher.schema().fields() {
        for segment_reader in searcher.segment_readers() {
            let fieldnorm_readers = segment_reader.fieldnorms_readers();
            let file_handle_opt = fieldnorm_readers.get_inner_file().open_read(field.0);
            if let Some(file_handle) = file_handle_opt {
                warm_up_futures.push(async move { file_handle.read_bytes_async().await })
            }
        }
    }
    try_join_all(warm_up_futures).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use tantivy::schema::Schema;
    use tantivy::{Index, ReloadPolicy, TantivyDocument};

    use super::*;

    /// Builds a single-segment in-RAM searcher with one text field, one document
    /// per provided value.
    fn ram_searcher_with_text(field_name: &str, docs: &[&str]) -> (Searcher, Field) {
        let mut schema_builder = Schema::builder();
        let field = schema_builder.add_text_field(field_name, tantivy::schema::TEXT);
        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        let mut index_writer = index.writer(15_000_000).unwrap();
        for doc_text in docs {
            let mut doc = TantivyDocument::default();
            doc.add_text(field, doc_text);
            index_writer.add_document(doc).unwrap();
        }
        index_writer.commit().unwrap();
        let searcher = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .unwrap()
            .searcher();
        (searcher, field)
    }

    /// Builds a `WarmupInfo` warming `terms`, with `required` as the set of required
    /// terms (each must be present for the query to match).
    fn warmup_info_with_required(terms: &[&Term], required: &[&Term]) -> WarmupInfo {
        let mut terms_grouped_by_field: HashMap<Field, HashMap<Term, bool>> = HashMap::new();
        for term in terms {
            terms_grouped_by_field
                .entry(term.field())
                .or_default()
                .insert((*term).clone(), false);
        }
        WarmupInfo {
            terms_grouped_by_field,
            required_terms: required.iter().map(|term| (*term).clone()).collect(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_warmup_reports_absent_required_terms() {
        let (searcher, body) = ram_searcher_with_text("body", &["hello world"]);
        // Single segment: the early-abort optimization is armed.
        assert_eq!(searcher.segment_readers().len(), 1);

        let present = Term::from_field_text(body, "hello");
        let missing = Term::from_field_text(body, "missing");

        // Runs warmup, returning whether the split is provably empty.
        async fn run(searcher: &Searcher, warmup_info: &WarmupInfo) -> bool {
            warmup(searcher, warmup_info).await.unwrap()
        }

        // An absent required term proves the split empty.
        let warmup_info = warmup_info_with_required(&[&present, &missing], &[&present, &missing]);
        assert!(run(&searcher, &warmup_info).await);

        // All required terms present: the split must be searched.
        let warmup_info = warmup_info_with_required(&[&present], &[&present]);
        assert!(!run(&searcher, &warmup_info).await);

        // A missing term that is not required proves nothing, so the split must be searched.
        let warmup_info = warmup_info_with_required(&[&present, &missing], &[]);
        assert!(!run(&searcher, &warmup_info).await);
    }
}
