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
/// `on_absent` is invoked once for every required term found to have an empty posting list,
/// with the segment it was missing from. Such a term proves the query empty in this split,
/// so the remaining warmup downloads are then cancelled; the callback lets the caller record
/// the (immutable, query-independent) absence — see [`term_absence_cache_key`]. It only ever
/// fires for a single-segment split, where "absent in the split" is sound.
///
/// `priority` schedules the CPU-intensive part of warmup. Warmup is mostly IO-bound, but
/// resolving automatons walks the term dictionary on the search thread pool, so the originating
/// request's priority has to be forwarded for that work to be scheduled against the rest of the
/// queue. Callers without a request priority to forward pass [`Priority::default`].
///
/// Returns whether the query is provably empty in this split (i.e. `on_absent` fired and
/// warmup was short-circuited).
pub(crate) async fn warmup(
    searcher: &Searcher,
    warmup_info: &WarmupInfo,
    priority: Priority,
    on_absent: &(dyn Fn(&Term, SegmentId) + Sync),
) -> anyhow::Result<bool> {
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
        on_absent,
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
        warm_up_automatons(searcher, &warmup_info.automatons_grouped_by_field, priority),
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
    on_absent: &(dyn Fn(&Term, SegmentId) + Sync),
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    for (field, terms) in terms_grouped_by_field {
        for segment_reader in searcher.segment_readers() {
            let inv_idx = segment_reader.inverted_index(*field)?;
            let segment_id = segment_reader.segment_id();
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
                        // Report the absence and fire the abort token. Both are synchronous, so
                        // they run before any cancellation can drop us.
                        on_absent(term, segment_id);
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
    priority: Priority,
) -> anyhow::Result<()> {
    let mut warm_up_futures = Vec::new();
    let cpu_intensive_executor = |task| async move {
        crate::search_thread_pool()
            .run_cpu_intensive_with_priority(priority, task)
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
                                    quickwit_query::query_ast::JsonPathPrefix {
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
        // Single segment: the early-abort optimization is armed, so absence is recorded.
        assert_eq!(searcher.segment_readers().len(), 1);

        let present = Term::from_field_text(body, "hello");
        let missing = Term::from_field_text(body, "missing");

        // Runs warmup, returning whether the split is provably empty and the terms that
        // `on_absent` was invoked with.
        async fn run(searcher: &Searcher, warmup_info: &WarmupInfo) -> (bool, Vec<Term>) {
            let reported = std::sync::Mutex::new(Vec::new());
            let provably_empty = warmup(
                searcher,
                warmup_info,
                Priority::default(),
                &|term: &Term, _segment_id| {
                    reported.lock().unwrap().push(term.clone());
                },
            )
            .await
            .unwrap();
            (provably_empty, reported.into_inner().unwrap())
        }

        // An absent required term is reported (so the caller can cache it) and proves the
        // split empty; the present required term is not reported.
        let warmup_info = warmup_info_with_required(&[&present, &missing], &[&present, &missing]);
        let (provably_empty, reported) = run(&searcher, &warmup_info).await;
        assert!(provably_empty);
        assert_eq!(reported, vec![missing.clone()]);

        // All required terms present: nothing reported, so the split must be searched.
        let warmup_info = warmup_info_with_required(&[&present], &[&present]);
        let (provably_empty, reported) = run(&searcher, &warmup_info).await;
        assert!(!provably_empty);
        assert!(reported.is_empty());

        // A missing term that is not required is never reported (recording would be unsound
        // without a required-term proof), so the split must be searched.
        let warmup_info = warmup_info_with_required(&[&present, &missing], &[]);
        let (provably_empty, reported) = run(&searcher, &warmup_info).await;
        assert!(!provably_empty);
        assert!(reported.is_empty());
    }
