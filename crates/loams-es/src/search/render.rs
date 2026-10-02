//! ES scores and the rendering of search answers (plan M1.5 Task 9 items
//! 1 and 5).

use loams_collection::PrimaryKey;
use loams_query::{Hit, SortKey, SortValue, TotalHits, TotalRelation, TrackTotalHits};
use serde_json::{Map, Value, json};

use super::{EsScore, RenderSpec};
use crate::doc::{f32_json, id_of, restore_vectors};
use crate::dsl::ScriptFunction;
use crate::mapping::EsSimilarity;

/// The ES score of an engine score `s` (item 1; the engine conventions of
/// overview §6.6: cosine and dot are the similarity, Euclid is `−d`).
/// Boosts are applied by the caller.
pub fn to_es_score(score: EsScore, s: f32) -> f32 {
    match score {
        EsScore::Bm25 | EsScore::Rrf | EsScore::Sum => s,
        EsScore::Knn(EsSimilarity::Cosine | EsSimilarity::DotProduct) => (1.0 + s) / 2.0,
        EsScore::Knn(EsSimilarity::L2Norm) => 1.0 / (1.0 + s * s),
        EsScore::Knn(EsSimilarity::MaxInnerProduct) => {
            if s < 0.0 {
                1.0 / (1.0 - s)
            } else {
                s + 1.0
            }
        }
        EsScore::Knn(EsSimilarity::L1Norm) => 1.0 / (1.0 - s),
        EsScore::Script(ScriptFunction::CosinePlusOne) => s + 1.0,
        EsScore::Script(ScriptFunction::InverseOnePlusL2) => 1.0 / (1.0 - s),
        EsScore::Script(ScriptFunction::SigmoidDot) => 1.0 / (1.0 + (-s).exp()),
    }
}

/// A sort value as ES prints it: dates are already epoch milliseconds, a
/// UUID is its hyphenated text.
pub fn sort_value_json(value: &SortValue) -> Value {
    match value {
        SortValue::Null => Value::Null,
        SortValue::Bool(b) => json!(b),
        SortValue::I64(n) => json!(n),
        SortValue::U64(n) => json!(n),
        SortValue::F64(x) => serde_json::Number::from_f64(*x).map_or(Value::Null, Value::Number),
        SortValue::Str(s) => json!(s),
        SortValue::Uuid(bytes) => json!(id_of(&PrimaryKey::Uuid(*bytes))),
    }
}

/// A hit's `sort` values: one per user sort key, which lead the request's
/// sort. A `_score` key prints the ES score, the others the engine's
/// values.
fn sort_values(hit: &Hit, spec: &RenderSpec, es_score: Option<f32>) -> Vec<Value> {
    spec.sort_keys
        .iter()
        .take(spec.user_sort_len)
        .enumerate()
        .map(|(i, key)| match key {
            SortKey::Score { .. } => es_score.map_or(Value::Null, f32_json),
            _ => hit.sort_values.get(i).map_or(Value::Null, sort_value_json),
        })
        .collect()
}

/// One hit (item 5), in ES's key order: `_index`, `_id`, `_score`,
/// `_version`/`_seq_no`/`_primary_term` when asked for, `_source`
/// (vectors restored, then filtered), `sort`.
pub fn render_hit(
    index: &str,
    hit: &Hit,
    spec: &RenderSpec,
    es_score: Option<f32>,
    seq_no: Option<u64>,
) -> Value {
    let mut out = Map::new();
    out.insert("_index".to_string(), json!(index));
    out.insert("_id".to_string(), json!(id_of(&hit.pk)));
    let score = if spec.scores_visible { es_score } else { None };
    out.insert("_score".to_string(), score.map_or(Value::Null, f32_json));
    if let Some(seq_no) = seq_no {
        if spec.version {
            out.insert("_version".to_string(), json!(seq_no + 1));
        }
        if spec.seq_no_primary_term {
            out.insert("_seq_no".to_string(), json!(seq_no));
            out.insert("_primary_term".to_string(), json!(1));
        }
    }
    if spec.source.enabled {
        let mut source = hit.source.clone().unwrap_or_default();
        restore_vectors(&mut source, &hit.vectors);
        out.insert(
            "_source".to_string(),
            Value::Object(spec.source.apply(source)),
        );
    }
    if spec.user_sort_len > 0 {
        out.insert(
            "sort".to_string(),
            Value::Array(sort_values(hit, spec, es_score)),
        );
    }
    Value::Object(out)
}

/// `hits.total`, or `None` when it is not tracked.
pub fn render_total(total: Option<TotalHits>, spec: &RenderSpec) -> Option<Value> {
    if spec.track == TrackTotalHits::None {
        return None;
    }
    let total = total.unwrap_or(TotalHits {
        value: 0,
        relation: TotalRelation::Eq,
    });
    Some(if spec.rest_total_hits_as_int {
        json!(total.value)
    } else {
        let relation = match total.relation {
            TotalRelation::Eq => "eq",
            TotalRelation::Gte => "gte",
        };
        json!({"value": total.value, "relation": relation})
    })
}

/// `total` capped by `track` as ES reports it: past an `UpTo(n)` limit the
/// value is `n` and the relation `gte`.
pub fn cap_total(total: TotalHits, track: TrackTotalHits) -> Option<TotalHits> {
    match track {
        TrackTotalHits::None => None,
        TrackTotalHits::Exact => Some(total),
        TrackTotalHits::UpTo(n) if total.value > n => Some(TotalHits {
            value: n,
            relation: TotalRelation::Gte,
        }),
        TrackTotalHits::UpTo(_) => Some(total),
    }
}

/// `_shards` of a request over `n` indices (one shard each).
pub fn shards(n: usize) -> Value {
    json!({"total": n, "successful": n, "skipped": 0, "failed": 0})
}

/// The whole search answer (item 5). `hits` are the page's rendered hits
/// and `max_score` the page's largest visible ES score.
pub fn render_response(
    took_ms: u128,
    n_indices: usize,
    total: Option<TotalHits>,
    max_score: Option<f32>,
    hits: Vec<Value>,
    spec: &RenderSpec,
) -> Value {
    let mut body = Map::new();
    if let Some(total) = render_total(total, spec) {
        body.insert("total".to_string(), total);
    }
    let max_score = if spec.scores_visible { max_score } else { None };
    body.insert(
        "max_score".to_string(),
        max_score.map_or(Value::Null, f32_json),
    );
    body.insert("hits".to_string(), Value::Array(hits));
    json!({
        "took": u64::try_from(took_ms).unwrap_or(u64::MAX),
        "timed_out": false,
        "_shards": shards(n_indices),
        "hits": Value::Object(body),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f32, b: f32) -> bool {
        (a - b).abs() < 1e-6
    }

    #[test]
    fn es_scores_follow_the_formula_per_similarity() {
        let knn = |sim| move |s| to_es_score(EsScore::Knn(sim), s);
        assert!(close(knn(EsSimilarity::Cosine)(1.0), 1.0));
        assert!(close(knn(EsSimilarity::Cosine)(0.0), 0.5));
        assert!(close(knn(EsSimilarity::DotProduct)(-1.0), 0.0));
        assert!(close(knn(EsSimilarity::L2Norm)(0.0), 1.0));
        assert!(close(knn(EsSimilarity::L2Norm)(-0.2), 1.0 / 1.04));
        assert!(close(knn(EsSimilarity::MaxInnerProduct)(-3.0), 0.25));
        assert!(close(knn(EsSimilarity::MaxInnerProduct)(2.0), 3.0));
        assert!(close(knn(EsSimilarity::L1Norm)(-1.0), 0.5));
        let script = |f| move |s| to_es_score(EsScore::Script(f), s);
        assert!(close(script(ScriptFunction::CosinePlusOne)(1.0), 2.0));
        assert!(close(script(ScriptFunction::InverseOnePlusL2)(-3.0), 0.25));
        assert!(close(script(ScriptFunction::SigmoidDot)(0.0), 0.5));
        for score in [EsScore::Bm25, EsScore::Rrf, EsScore::Sum] {
            assert_eq!(to_es_score(score, 1.25), 1.25);
        }
    }

    #[test]
    fn totals_are_capped_by_the_tracking_limit() {
        let eq = |value| TotalHits {
            value,
            relation: TotalRelation::Eq,
        };
        assert_eq!(cap_total(eq(30), TrackTotalHits::None), None);
        assert_eq!(cap_total(eq(30), TrackTotalHits::Exact), Some(eq(30)));
        assert_eq!(cap_total(eq(10), TrackTotalHits::UpTo(10)), Some(eq(10)));
        assert_eq!(
            cap_total(eq(30), TrackTotalHits::UpTo(10)),
            Some(TotalHits {
                value: 10,
                relation: TotalRelation::Gte
            })
        );
    }

    #[test]
    fn sort_values_print_as_es_does() {
        assert_eq!(
            sort_value_json(&SortValue::I64(1_727_172_672_000)),
            json!(1_727_172_672_000_i64)
        );
        assert_eq!(sort_value_json(&SortValue::Null), Value::Null);
        assert_eq!(sort_value_json(&SortValue::Str("a".into())), json!("a"));
        assert_eq!(
            sort_value_json(&SortValue::Uuid([0x11; 16])),
            json!("11111111-1111-1111-1111-111111111111")
        );
    }
}
