//! `script_score` with the fixed vector scripts of elasticsearch-py's
//! `DenseVectorScriptScoreStrategy` (plan M1.5 Task 7 item 6, Ruling 12,
//! C18–C22). No script is ever run: a recognised source becomes an exact
//! vector search with a metric override.

use serde_json::{Value, json};

use super::query::{Parser, number_f32};
use super::{ScriptFunction, ScriptVectorSpec, check_query_vector};
use crate::error::EsError;

/// The templates `recognise_script` matches, `<f>` standing for the field.
const TEMPLATES: &[(&str, ScriptFunction)] = &[
    (
        "cosineSimilarity(params.query_vector, '<f>') + 1.0",
        ScriptFunction::CosinePlusOne,
    ),
    (
        "cosineSimilarity(params.query_vector, doc['<f>']) + 1.0",
        ScriptFunction::CosinePlusOne,
    ),
    (
        "1 / (1 + l2norm(params.query_vector, '<f>'))",
        ScriptFunction::InverseOnePlusL2,
    ),
    (
        "1 / (1 + l2norm(params.query_vector, doc['<f>']))",
        ScriptFunction::InverseOnePlusL2,
    ),
    (
        "double value = dotProduct(params.query_vector, '<f>'); return sigmoid(1, Math.E, -value);",
        ScriptFunction::SigmoidDot,
    ),
];

/// The `MAX_INNER_PRODUCT` source of elasticsearch-py, which Painless cannot
/// compile (it uses `dotProduct` as a variable, C22).
const BROKEN_MIP: &str = "double value = dotProduct(params.query_vector, '<f>'); if (dotProduct \
                          < 0) { return 1 / (1 + -1 * dotProduct); } return dotProduct + 1;";

/// `source` with every run of whitespace collapsed to one space, trimmed.
fn normalize(source: &str) -> String {
    source.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// The field when `normalized` is `template` with `<f>` replaced by a name
/// of `[^'\]]+`.
fn matches_template(normalized: &str, template: &str) -> Option<String> {
    let (head, tail) = template.split_once("<f>")?;
    let field = normalized.strip_prefix(head)?.strip_suffix(tail)?;
    (!field.is_empty() && !field.contains(['\'', ']'])).then(|| field.to_string())
}

/// 400 for a script outside the recognised set.
fn arbitrary() -> EsError {
    EsError::illegal_argument(
        "Loams does not support arbitrary scripts; script_score supports only the vector \
         sources cosineSimilarity(params.query_vector, '<field>') + 1.0, 1 / (1 + \
         l2norm(params.query_vector, '<field>')) and the sigmoid(dotProduct) form",
    )
}

/// The function and field of a recognised vector script source (item 6).
pub fn recognise_script(source: &str) -> Result<(ScriptFunction, String), EsError> {
    let normalized = normalize(source);
    for (template, function) in TEMPLATES {
        if let Some(field) = matches_template(&normalized, template) {
            return Ok((*function, field));
        }
    }
    if matches_template(&normalized, BROKEN_MIP).is_some() {
        let mut error = EsError::new(400, "script_exception", "compile error")
            .with(
                "script_stack",
                json!(["if (dotProduct < 0) {", "    ^---- HERE"]),
            )
            .with("script", source)
            .with("lang", "painless");
        error.extra.insert(
            "caused_by".to_string(),
            json!({
                "type": "illegal_argument_exception",
                "reason": "cannot resolve symbol [dotProduct]",
            }),
        );
        return Err(error);
    }
    Err(arbitrary())
}

impl Parser<'_> {
    /// `script_score {query, script {source, params, lang}, min_score,
    /// boost}`.
    pub(super) fn script_score(
        &self,
        body: &Value,
        depth: u32,
    ) -> Result<ScriptVectorSpec, EsError> {
        let map = self.object("script_score", body)?;
        let mut query = None;
        let mut script = None;
        let mut min_score = None;
        let mut boost = 1.0;
        for (key, value) in map {
            match key.as_str() {
                "query" => query = Some(value),
                "script" => script = Some(value),
                "min_score" => min_score = Some(number_f32("script_score", key, value)?),
                "boost" => boost = self.boost_value("script_score", value)?,
                "_name" => {}
                _ => return Err(self.does_not_support("script_score", key)),
            }
        }
        let Some(query) = query else {
            return Err(EsError::parsing("[script_score] requires 'query' field"));
        };
        let Some(script) = script else {
            return Err(EsError::parsing("[script_score] requires 'script' field"));
        };
        let (source, params) = match script {
            Value::String(source) => (source.as_str(), None),
            Value::Object(script) => {
                let mut source = None;
                let mut params = None;
                for (key, value) in script {
                    match key.as_str() {
                        "source" | "inline" => source = value.as_str(),
                        "params" => params = Some(value),
                        "lang" => {
                            if value.as_str() != Some("painless") {
                                return Err(EsError::illegal_argument(format!(
                                    "script_lang not supported [{}]",
                                    value.as_str().unwrap_or_default()
                                )));
                            }
                        }
                        "id" | "options" => return Err(arbitrary()),
                        _ => {
                            return Err(EsError::parsing(format!(
                                "[script] unknown field [{key}]"
                            )));
                        }
                    }
                }
                let Some(source) = source else {
                    return Err(arbitrary());
                };
                (source, params)
            }
            _ => return Err(arbitrary()),
        };
        let (function, field) = recognise_script(source)?;
        let query_vector = match params {
            Some(Value::Object(params)) if params.len() == 1 => match params.get("query_vector") {
                Some(v) => v,
                None => return Err(arbitrary()),
            },
            _ => return Err(arbitrary()),
        };
        let Some(vector) = self.view().es.vectors.get(&field) else {
            return Err(EsError::illegal_argument(format!(
                "field [{field}] does not exist in the mapping"
            )));
        };
        let query_vector = check_query_vector(query_vector, vector, false)?;
        let filter = self.leaf(query, depth + 1)?;
        Ok(ScriptVectorSpec {
            field,
            query_vector,
            function,
            filter,
            min_score,
            boost,
        })
    }
}
