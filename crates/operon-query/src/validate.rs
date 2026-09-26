//! Request validation that needs no schema (plan M1.2 Task 1 rule 4).
//! Vector dimensions, field existence and kinds are checked at execution
//! time against the schema.

use crate::error::ServiceError;
use crate::ir::{FieldValue, Fusion, Query, Retriever, SearchRequest, SortKey};

/// Size limits of a search.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchLimits {
    /// Largest `k`, and largest `offset + limit`.
    pub max_window: usize,
    /// Retrievers per request, nested inputs included.
    pub max_retrievers: usize,
    /// How deep fused retrievers nest.
    pub max_fused_depth: usize,
    /// `Query` nodes per query.
    pub max_query_clauses: usize,
}

impl Default for SearchLimits {
    fn default() -> Self {
        Self {
            max_window: 100_000,
            max_retrievers: 16,
            max_fused_depth: 4,
            max_query_clauses: 1_024,
        }
    }
}

fn invalid(message: String) -> Result<(), ServiceError> {
    Err(ServiceError::InvalidArgument(message))
}

/// Every retriever of the request, nested inputs included, depth first.
fn all_retrievers(retrievers: &[Retriever]) -> Vec<&Retriever> {
    let mut out = Vec::new();
    let mut stack: Vec<&Retriever> = retrievers.iter().rev().collect();
    while let Some(retriever) = stack.pop() {
        out.push(retriever);
        match retriever {
            Retriever::Fused { inputs, .. } => stack.extend(inputs.iter().rev()),
            Retriever::Rescore { input, .. } => stack.push(input),
            _ => {}
        }
    }
    out
}

/// How deep fused retrievers nest under `retriever` (a fused one counts 1).
fn fused_depth(retriever: &Retriever) -> usize {
    match retriever {
        Retriever::Fused { inputs, .. } => 1 + inputs.iter().map(fused_depth).max().unwrap_or(0),
        Retriever::Rescore { input, .. } => fused_depth(input),
        _ => 0,
    }
}

/// `Query` nodes in `query`.
fn clauses(query: &Query) -> usize {
    1 + match query {
        Query::Bool {
            must,
            should,
            must_not,
            filter,
            ..
        } => must
            .iter()
            .chain(should)
            .chain(must_not)
            .chain(filter)
            .map(clauses)
            .sum(),
        Query::Boost { query, .. } | Query::ConstantScore { query, .. } => clauses(query),
        _ => 0,
    }
}

/// Checks `request` against Task 1 rule 4, in order; the first failure wins.
pub fn validate_request(
    request: &SearchRequest,
    limits: &SearchLimits,
) -> Result<(), ServiceError> {
    // 1.
    if request.collection.is_empty() {
        return invalid("collection must not be empty".to_string());
    }
    let every = all_retrievers(&request.retrievers);
    // 2.
    if every.len() > limits.max_retrievers {
        return invalid(format!("at most {} retrievers", limits.max_retrievers));
    }
    // 3.
    if request
        .retrievers
        .iter()
        .map(fused_depth)
        .max()
        .unwrap_or(0)
        > limits.max_fused_depth
    {
        return invalid(format!(
            "fused retrievers nest at most {} deep",
            limits.max_fused_depth
        ));
    }
    // 4.
    if every
        .iter()
        .any(|r| matches!(r, Retriever::Fused { inputs, .. } if inputs.is_empty()))
    {
        return invalid("a fused retriever needs at least one input".to_string());
    }
    // 5.
    if request.retrievers.len() > 1 && request.fusion.is_none() {
        return invalid("fusion is required with several retrievers".to_string());
    }
    // 6. The lists of a level are its retrievers.
    let mut levels: Vec<(&Option<Fusion>, usize)> =
        vec![(&request.fusion, request.retrievers.len())];
    let nested: Vec<(Option<Fusion>, usize)> = every
        .iter()
        .filter_map(|r| match r {
            Retriever::Fused { inputs, fusion, .. } => Some((Some(fusion.clone()), inputs.len())),
            _ => None,
        })
        .collect();
    levels.extend(nested.iter().map(|(fusion, n)| (fusion, *n)));
    for (fusion, n) in levels {
        if let Some(Fusion::WeightedSum { weights }) = fusion
            && weights.len() != n
        {
            return invalid(format!(
                "weighted_sum needs one weight per list, got {} for {n}",
                weights.len()
            ));
        }
    }
    // 7.
    let k_of = |r: &Retriever| match r {
        Retriever::Vector { k, .. }
        | Retriever::Text { k, .. }
        | Retriever::Fused { k, .. }
        | Retriever::Rescore { k, .. }
        | Retriever::Sparse { k, .. } => *k,
    };
    if every
        .iter()
        .any(|r| !(1..=limits.max_window).contains(&k_of(r)))
    {
        return invalid(format!("k must be between 1 and {}", limits.max_window));
    }
    // 8.
    if request
        .offset
        .checked_add(request.limit)
        .is_none_or(|window| window > limits.max_window)
    {
        return invalid(format!(
            "offset + limit must be at most {}",
            limits.max_window
        ));
    }
    // 9.
    if every.iter().any(|r| {
        matches!(r, Retriever::Vector { params, .. } if params.distance.is_some() && !params.exact)
    }) {
        return invalid("a distance override needs exact search".to_string());
    }
    // 10.
    if request.retrievers.is_empty()
        && request
            .sort
            .iter()
            .any(|key| matches!(key, SortKey::Score { .. }))
    {
        return invalid("a score sort needs a retriever".to_string());
    }
    // 11. Field mode (Ruling 6): at most one retriever, and it is text.
    let field_mode = matches!(
        request.sort.first(),
        Some(SortKey::Field { .. } | SortKey::Pk { .. })
    );
    if field_mode
        && (request.retrievers.len() > 1
            || request
                .retrievers
                .iter()
                .any(|r| !matches!(r, Retriever::Text { .. })))
    {
        return invalid(
            "a field sort allows at most one retriever, and it must be text".to_string(),
        );
    }
    // 12. The effective keys: the sort, or the score when it is empty and
    // there is a retriever; then the implicit PK key unless one is named.
    if let Some(after) = &request.search_after {
        let explicit = if request.sort.is_empty() {
            usize::from(!request.retrievers.is_empty())
        } else {
            request.sort.len()
        };
        let has_pk = request
            .sort
            .iter()
            .any(|key| matches!(key, SortKey::Pk { .. }));
        let n = explicit + usize::from(!has_pk);
        if after.len() > n {
            return invalid(format!(
                "search_after has {} values but the sort has {n} keys",
                after.len()
            ));
        }
    }
    // 13.
    if let Some(group) = &request.group_by
        && (group.group_size == 0 || group.limit == 0)
    {
        return invalid("group_size and limit of group_by must be at least 1".to_string());
    }
    // 14.
    if let Some(aggregations) = &request.aggregations
        && let Err(err) = serde_json::from_value::<tantivy::aggregation::agg_req::Aggregations>(
            aggregations.clone(),
        )
    {
        return invalid(format!(
            "aggregations are not a valid aggregation request: {err}"
        ));
    }
    // 15.
    if let Some(path) = non_finite(request) {
        return invalid(format!("non-finite number in {path}"));
    }
    // 16.
    let mut queries: Vec<&Query> = request.filter.iter().collect();
    for retriever in &every {
        match retriever {
            Retriever::Vector { filter, .. } => queries.extend(filter),
            Retriever::Text { query, .. } => queries.push(query),
            Retriever::Sparse { filter, params, .. } => {
                queries.extend(filter);
                queries.extend(&params.idf_corpus);
            }
            Retriever::Fused { .. } | Retriever::Rescore { .. } => {}
        }
    }
    if queries
        .iter()
        .any(|query| clauses(query) > limits.max_query_clauses)
    {
        return invalid(format!(
            "a query has more than {} clauses",
            limits.max_query_clauses
        ));
    }
    Ok(())
}

/// The JSON path of the first non-finite number of the request.
fn non_finite(request: &SearchRequest) -> Option<String> {
    for (i, retriever) in request.retrievers.iter().enumerate() {
        if let Some(path) = retriever_non_finite(retriever, &format!("retrievers[{i}]")) {
            return Some(path);
        }
    }
    if let Some(Fusion::WeightedSum { weights }) = &request.fusion
        && let Some(path) = floats(weights, "fusion.weighted_sum.weights")
    {
        return Some(path);
    }
    if let Some(filter) = &request.filter
        && let Some(path) = query_non_finite(filter, "filter")
    {
        return Some(path);
    }
    if let Some(threshold) = request.score_threshold
        && !threshold.is_finite()
    {
        return Some("score_threshold".to_string());
    }
    if let Some(after) = &request.search_after {
        for (i, value) in after.iter().enumerate() {
            if let crate::ir::SortValue::F64(x) = value
                && !x.is_finite()
            {
                return Some(format!("search_after[{i}]"));
            }
        }
    }
    None
}

fn floats(values: &[f32], path: &str) -> Option<String> {
    values
        .iter()
        .position(|x| !x.is_finite())
        .map(|i| format!("{path}[{i}]"))
}

fn float(value: Option<f32>, path: String) -> Option<String> {
    value.filter(|x| !x.is_finite()).map(|_| path)
}

fn retriever_non_finite(retriever: &Retriever, path: &str) -> Option<String> {
    match retriever {
        Retriever::Vector {
            query,
            params,
            filter,
            ..
        } => floats(query, &format!("{path}.vector.query"))
            .or_else(|| {
                float(
                    params.oversampling,
                    format!("{path}.vector.params.oversampling"),
                )
            })
            .or_else(|| {
                filter
                    .as_ref()
                    .and_then(|f| query_non_finite(f, &format!("{path}.vector.filter")))
            }),
        Retriever::Text { query, .. } => query_non_finite(query, &format!("{path}.text.query")),
        Retriever::Fused { inputs, fusion, .. } => inputs
            .iter()
            .enumerate()
            .find_map(|(i, input)| {
                retriever_non_finite(input, &format!("{path}.fused.inputs[{i}]"))
            })
            .or_else(|| match fusion {
                Fusion::WeightedSum { weights } => floats(
                    weights,
                    &format!("{path}.fused.fusion.weighted_sum.weights"),
                ),
                _ => None,
            }),
        Retriever::Rescore { input, query, .. } => {
            retriever_non_finite(input, &format!("{path}.rescore.input"))
                .or_else(|| floats(query, &format!("{path}.rescore.query")))
        }
        Retriever::Sparse {
            query,
            filter,
            params,
            ..
        } => floats(query.values(), &format!("{path}.sparse.query.values"))
            .or_else(|| {
                filter
                    .as_ref()
                    .and_then(|f| query_non_finite(f, &format!("{path}.sparse.filter")))
            })
            .or_else(|| {
                params
                    .idf_corpus
                    .as_ref()
                    .and_then(|f| query_non_finite(f, &format!("{path}.sparse.params.idf_corpus")))
            }),
    }
}

fn value_non_finite(value: &FieldValue, path: String) -> Option<String> {
    match value {
        FieldValue::F64(x) if !x.is_finite() => Some(path),
        _ => None,
    }
}

fn query_non_finite(query: &Query, path: &str) -> Option<String> {
    let list = |queries: &[Query], key: &str| {
        queries
            .iter()
            .enumerate()
            .find_map(|(i, q)| query_non_finite(q, &format!("{path}.bool.{key}[{i}]")))
    };
    match query {
        Query::MultiMatch {
            fields,
            tie_breaker,
            ..
        } => fields
            .iter()
            .position(|(_, boost)| !boost.is_finite())
            .map(|i| format!("{path}.multi_match.fields[{i}]"))
            .or_else(|| float(*tie_breaker, format!("{path}.multi_match.tie_breaker"))),
        Query::Term { value, .. } => value_non_finite(value, format!("{path}.term.value")),
        Query::Terms { values, .. } => values
            .iter()
            .enumerate()
            .find_map(|(i, v)| value_non_finite(v, format!("{path}.terms.values[{i}]"))),
        Query::Range {
            gt, gte, lt, lte, ..
        } => [("gt", gt), ("gte", gte), ("lt", lt), ("lte", lte)]
            .into_iter()
            .find_map(|(key, bound)| {
                bound
                    .as_ref()
                    .and_then(|v| value_non_finite(v, format!("{path}.range.{key}")))
            }),
        Query::Bool {
            must,
            should,
            must_not,
            filter,
            ..
        } => list(must, "must")
            .or_else(|| list(should, "should"))
            .or_else(|| list(must_not, "must_not"))
            .or_else(|| list(filter, "filter")),
        Query::Boost { query, boost } => float(Some(*boost), format!("{path}.boost.boost"))
            .or_else(|| query_non_finite(query, &format!("{path}.boost.query"))),
        Query::ConstantScore { query, score } => {
            float(Some(*score), format!("{path}.constant_score.score"))
                .or_else(|| query_non_finite(query, &format!("{path}.constant_score.query")))
        }
        _ => None,
    }
}
