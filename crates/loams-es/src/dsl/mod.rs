//! The Elasticsearch Query DSL → the search IR (plan M1.5 Task 7, Ruling 1).
//!
//! Loams's own parser: every Phase A leaf and compound query becomes a
//! [`Query`]; `knn` clauses at the top (or in the top-level `bool`'s
//! `should`) are lifted into [`KnnSpec`]s, and a top-level `script_score`
//! with a recognised vector script into a [`ScriptVectorSpec`]. Field names
//! resolve against the index's mapping ([`IndexView`]); values are coerced
//! to the field's type ([`coerce`]). Quickwit's `elastic_query_dsl` is the
//! reference for shapes only: the parser works on `serde_json::Value` so its
//! errors carry ES's texts (row T7-1).

pub mod datemath;
pub mod query;
#[cfg(feature = "test-util")]
pub mod reference;
pub mod script;
pub mod value;

use loams_query::Query;
use serde_json::Value;

pub use datemath::{Rounding, parse_date_math};
pub use query::{parse_filter_list, parse_knn, parse_leaf, parse_query};
#[cfg(feature = "test-util")]
pub use reference::reference_eval;
pub use script::recognise_script;
pub use value::coerce;

use crate::error::EsError;
use crate::mapping::{EsSimilarity, IndexView, VectorView};

/// The deepest a query may nest (ES `indices.query.bool.max_nested_depth`).
pub const MAX_DEPTH: u32 = 30;

/// The most values a `terms` query takes (ES `index.max_terms_count`).
pub const MAX_TERMS: usize = 65_536;

/// The largest `num_candidates` of a knn search.
pub const MAX_NUM_CANDIDATES: usize = 10_000;

/// What a query is parsed against.
#[derive(Clone, Copy, Debug)]
pub struct QueryContext<'a> {
    pub view: &'a IndexView,
    /// `now` of date math, in epoch milliseconds.
    pub now_ms: i64,
    /// The nesting depth of the query being parsed (0 at the top; at most
    /// [`MAX_DEPTH`]).
    pub depth: u32,
    /// The `k` of a knn clause that sets none: the request's `size`.
    pub default_k: usize,
}

impl<'a> QueryContext<'a> {
    /// A context at the top, with `default_k` 10.
    pub fn new(view: &'a IndexView, now_ms: i64) -> Self {
        Self {
            view,
            now_ms,
            depth: 0,
            default_k: 10,
        }
    }
}

/// A parsed `query`.
#[derive(Clone, Debug, PartialEq)]
pub struct ParsedQuery {
    /// The scoring part (`None` when the query is exactly a knn or
    /// script_score).
    pub query: Option<Query>,
    /// knn clauses lifted from the query (the top level, or the top-level
    /// `bool`'s `should`).
    pub knn: Vec<KnnSpec>,
    pub script: Option<ScriptVectorSpec>,
}

/// One knn search.
#[derive(Clone, Debug, PartialEq)]
pub struct KnnSpec {
    pub field: String,
    pub query_vector: Vec<f32>,
    pub k: usize,
    pub num_candidates: usize,
    pub filter: Option<Query>,
    /// The raw similarity threshold (`knn.similarity`).
    pub similarity: Option<f32>,
    pub boost: f32,
}

/// A recognised `script_score` vector script.
#[derive(Clone, Debug, PartialEq)]
pub struct ScriptVectorSpec {
    pub field: String,
    pub query_vector: Vec<f32>,
    pub function: ScriptFunction,
    /// The candidates: the script's `query`.
    pub filter: Query,
    pub min_score: Option<f32>,
    pub boost: f32,
}

/// The vector scripts `script_score` recognises (C18, C20, C21).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptFunction {
    /// `cosineSimilarity(params.query_vector, '<f>') + 1.0`.
    CosinePlusOne,
    /// `1 / (1 + l2norm(params.query_vector, '<f>'))`.
    InverseOnePlusL2,
    /// `sigmoid(1, Math.E, -dotProduct(params.query_vector, '<f>'))`.
    SigmoidDot,
}

/// `x` as Java prints a `float`: an integral value keeps `.0`.
pub(crate) fn java_float(x: f32) -> String {
    if x.fract() == 0.0 && x.abs() < 1e7 {
        format!("{x:.1}")
    } else {
        x.to_string()
    }
}

/// A `query_vector` checked against `vector` (item 5): finite numbers, the
/// vector's dimension, no zero vector on cosine and, for knn (`knn ==
/// true`), unit length on dot_product.
pub(crate) fn check_query_vector(
    v: &Value,
    vector: &VectorView,
    knn: bool,
) -> Result<Vec<f32>, EsError> {
    let Value::Array(items) = v else {
        return Err(EsError::parsing(
            "[query_vector] must be an array of numbers",
        ));
    };
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let x = item
            .as_f64()
            .map(|x| x as f32)
            .filter(|x| x.is_finite())
            .ok_or_else(|| EsError::parsing("[query_vector] must be an array of finite numbers"))?;
        out.push(x);
    }
    if out.len() != vector.dim as usize {
        return Err(EsError::illegal_argument(format!(
            "the query vector has a different number of dimensions [{}] than the document \
             vectors [{}]",
            out.len(),
            vector.dim
        )));
    }
    let norm2: f64 = out.iter().map(|&x| f64::from(x) * f64::from(x)).sum();
    match vector.similarity {
        EsSimilarity::Cosine if norm2 == 0.0 => Err(EsError::illegal_argument(
            "The [cosine] similarity does not support vectors with zero magnitude.",
        )),
        EsSimilarity::DotProduct if knn && (norm2 - 1.0).abs() > 1e-4 => {
            // ES's `createKnnQuery` text, a shard failure (row T11-3).
            let preview: Vec<String> = out.iter().map(|x| java_float(*x)).collect();
            Err(value::shard_error(format!(
                "The [dot_product] similarity can only be used with unit-length vectors. \
                 Preview of invalid vector: [{}]",
                preview.join(", ")
            )))
        }
        _ => Ok(out),
    }
}
