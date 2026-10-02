//! `POST /{index}/_update_by_query` (plan M1.5 Task 9a rule 8, D87): the
//! recognised Painless scripts compile to one patch, which the native
//! `patch_by_filter` applies to every match at one pin per index.
//!
//! A recognised script is one or more statements separated by `;`, each
//! `ctx._source.<path> = params.<name>` or
//! `ctx._source[.<path>].remove('<key>')`. Assignments become a `MergeDeep`
//! source, so a missing or non-object parent on the way is created as an
//! object (Painless would fail on it); removes become `delete_keys`.

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::Uri;
use axum::response::Response;
use loams_query::PatchSpec;
use serde_json::{Map, Value, json};

use crate::EsGateway;
use crate::dbq::{self, BY_QUERY_PARAMS, ByFilter, DbqParams};
use crate::error::EsError;
use crate::http::{Params, RequestCtx, fail, json_body, respond};
use crate::mapping::IndexView;
use crate::search::exec::{expr_of, resolve_options, views};

/// The refusal of every script outside the recognised forms.
const SCRIPT_REFUSAL: &str = "Loams supports only params assignments and remove() in [_update_by_query] scripts (Elasticsearch API Phase A)";
/// The refusal of a request without a script.
const NO_SCRIPT: &str = "re-indexing without a script needs online index changes (D97); not supported in Elasticsearch API Phase A";

fn refused() -> EsError {
    EsError::illegal_argument(SCRIPT_REFUSAL)
}

/// Removes the whitespace outside quotes and splits on the `;` outside
/// quotes, dropping empty statements (a trailing `;`).
fn statements(source: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quote = None;
    for c in source.chars() {
        match quote {
            Some(q) => {
                current.push(c);
                if c == q {
                    quote = None;
                }
            }
            None if c == '\'' || c == '"' => {
                quote = Some(c);
                current.push(c);
            }
            None if c == ';' => out.push(std::mem::take(&mut current)),
            None if c.is_whitespace() => {}
            None => current.push(c),
        }
    }
    out.push(current);
    out.retain(|s| !s.is_empty());
    out
}

fn is_ident(s: &str) -> bool {
    let mut chars = s.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `a.b.c` as its segments, each an identifier.
fn path(s: &str) -> Option<Vec<String>> {
    let segments: Vec<String> = s.split('.').map(str::to_string).collect();
    segments.iter().all(|s| is_ident(s)).then_some(segments)
}

/// One recognised statement.
enum Statement {
    Assign(Vec<String>, String),
    Remove(Vec<String>),
}

fn statement(s: &str) -> Option<Statement> {
    let rest = s.strip_prefix("ctx._source")?;
    if let Some(call) = rest.strip_suffix(')')
        && let Some(at) = call.rfind(".remove(")
    {
        let parent = &call[..at];
        let arg = &call[at + ".remove(".len()..];
        let mut segments = match parent {
            "" => Vec::new(),
            p => path(p.strip_prefix('.')?)?,
        };
        let quote = arg.chars().next().filter(|c| *c == '\'' || *c == '"')?;
        let key = arg.strip_prefix(quote)?.strip_suffix(quote)?;
        if key.is_empty() || key.contains(['.', '\'', '"']) {
            return None;
        }
        segments.push(key.to_string());
        return Some(Statement::Remove(segments));
    }
    let (lhs, rhs) = rest.strip_prefix('.')?.split_once('=')?;
    let name = rhs.strip_prefix("params.")?;
    if !is_ident(name) {
        return None;
    }
    Some(Statement::Assign(path(lhs)?, name.to_string()))
}

/// Whether one path is the other or lies under it.
fn overlaps(a: &[String], b: &[String]) -> bool {
    a.iter().zip(b).all(|(x, y)| x == y)
}

fn insert(source: &mut Map<String, Value>, path: &[String], value: Value) {
    let Some((last, parents)) = path.split_last() else {
        return;
    };
    let mut map = source;
    for segment in parents {
        let entry = map
            .entry(segment.clone())
            .or_insert_with(|| Value::Object(Map::new()));
        if !entry.is_object() {
            *entry = Value::Object(Map::new());
        }
        let Value::Object(next) = entry else {
            return;
        };
        map = next;
    }
    map.insert(last.clone(), value);
}

/// The recognised `_update_by_query` scripts (rule 8), compiled to one
/// patch.
pub fn compile_update_script(script: &Value) -> Result<PatchSpec, EsError> {
    let (source, params) = match script {
        Value::String(source) => (source.as_str(), None),
        Value::Object(map) => {
            if map
                .keys()
                .any(|key| !matches!(key.as_str(), "source" | "lang" | "params"))
            {
                return Err(refused());
            }
            match map.get("lang") {
                None => {}
                Some(Value::String(lang)) if lang == "painless" => {}
                Some(_) => return Err(refused()),
            }
            let source = map
                .get("source")
                .and_then(Value::as_str)
                .ok_or_else(refused)?;
            let params = match map.get("params") {
                None | Some(Value::Null) => None,
                Some(Value::Object(params)) => Some(params),
                Some(_) => return Err(refused()),
            };
            (source, params)
        }
        _ => return Err(refused()),
    };
    let mut assigned: Vec<(Vec<String>, Value)> = Vec::new();
    let mut removed: Vec<Vec<String>> = Vec::new();
    let texts = statements(source);
    if texts.is_empty() {
        return Err(refused());
    }
    for text in texts {
        match statement(&text).ok_or_else(refused)? {
            Statement::Assign(path, name) => {
                let value = params
                    .and_then(|params| params.get(&name))
                    .ok_or_else(refused)?;
                if value.is_object() {
                    return Err(refused());
                }
                // A later assignment of the same path wins, as in Painless.
                assigned.retain(|(p, _)| *p != path);
                assigned.push((path, value.clone()));
            }
            Statement::Remove(path) => removed.push(path),
        }
    }
    for (i, (a, _)) in assigned.iter().enumerate() {
        let nested = assigned[i + 1..].iter().any(|(b, _)| overlaps(a, b));
        let also_removed = removed.iter().any(|r| overlaps(a, r));
        if nested || also_removed {
            return Err(refused());
        }
    }
    let mut out = PatchSpec::merge_deep(Map::new());
    for (path, value) in assigned {
        insert(&mut out.source, &path, value);
    }
    // Every duplicate goes, adjacent or not, in first-appearance order.
    let mut seen = std::collections::HashSet::new();
    out.delete_keys = removed
        .iter()
        .map(|path| path.join("."))
        .filter(|key| seen.insert(key.clone()))
        .collect();
    Ok(out)
}

/// `_update_by_query` over `indices` (rule 8): the body's `query` (or `q`),
/// `max_docs` and `script`, executed per index by `patch_by_filter`.
pub async fn update_by_query(
    gw: &EsGateway,
    ctx: &RequestCtx,
    indices: &[IndexView],
    body: &Value,
    params: &DbqParams,
) -> Result<Value, EsError> {
    let parsed = dbq::parse_body(body, true)?;
    let script = parsed
        .script
        .ok_or_else(|| EsError::illegal_argument(NO_SCRIPT))?;
    let patch = compile_update_script(script)?;
    let max_docs = parsed.max_docs.unwrap_or(params.max_docs);
    let mut indices = indices.to_vec();
    indices.sort_by(|a, b| a.name.cmp(&b.name));
    // Without a query every document matches, as in ES (row T11-3).
    let queries = dbq::compile_queries(&indices, parsed.query, &params.search, true)?;
    let targets: Vec<(IndexView, _)> = indices.into_iter().zip(queries).collect();
    let totals = dbq::run(
        gw,
        ctx,
        &targets,
        &ByFilter::Patch(patch),
        max_docs,
        params.timeout,
        "_update_by_query",
    )
    .await?;
    let fields = [
        ("took", json!(ctx.started.elapsed().as_millis() as u64)),
        ("timed_out", json!(false)),
        ("total", json!(totals.total)),
        ("updated", json!(totals.affected)),
        ("deleted", json!(0)),
        ("batches", json!(totals.batches)),
        ("version_conflicts", json!(0)),
        ("noops", json!(totals.total.saturating_sub(totals.affected))),
        ("retries", json!({"bulk": 0, "search": 0})),
        ("throttled_millis", json!(0)),
        ("requests_per_second", json!(-1.0)),
        ("throttled_until_millis", json!(0)),
        ("failures", json!([])),
    ];
    Ok(Value::Object(
        fields
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect(),
    ))
}

async fn update_by_query_request(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: &str,
    body: &[u8],
) -> Result<Value, EsError> {
    let params = Params::parse(uri.query(), uri.path(), BY_QUERY_PARAMS)?;
    let dbq_params = DbqParams::parse(&params)?;
    let body = json_body(body)?.unwrap_or(Value::Null);
    // The body is read before resolution, so a bad script writes nothing
    // and names itself before a missing index does.
    let parsed = dbq::parse_body(&body, true)?;
    let script = parsed
        .script
        .ok_or_else(|| EsError::illegal_argument(NO_SCRIPT))?;
    compile_update_script(script)?;
    let indices = views(
        gw,
        ctx,
        &expr_of(Some(index)),
        resolve_options(&params, &dbq_params.search),
    )
    .await?;
    update_by_query(gw, ctx, &indices, &body, &dbq_params).await
}

/// `POST /{index}/_update_by_query`.
pub(crate) async fn update_by_query_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    match update_by_query_request(&gw, &ctx, &uri, &index, &body).await {
        Ok(body) => respond(&ctx, 200, &body),
        Err(err) => fail(&ctx, &err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn compiled(source: &str, params: Value) -> Result<PatchSpec, EsError> {
        compile_update_script(&json!({"source": source, "lang": "painless", "params": params}))
    }

    #[test]
    fn assignments_and_removes_compile_to_one_patch() {
        let patch = compiled(
            " ctx._source.a = params.x ;ctx._source.m.n=params.y;\n ctx._source.m.remove('old'); ctx._source.remove(\"gone\");",
            json!({"x": 1, "y": [1, 2]}),
        )
        .expect("recognised");
        assert_eq!(
            Value::Object(patch.source.clone()),
            json!({"a": 1, "m": {"n": [1, 2]}})
        );
        assert_eq!(patch.delete_keys, vec!["m.old", "gone"]);
        assert_eq!(patch.mode, loams_collection::PatchMode::MergeDeep);
        // A later assignment of the same path wins.
        let patch = compiled(
            "ctx._source.a = params.x; ctx._source.a = params.y",
            json!({"x": 1, "y": 2}),
        )
        .expect("recognised");
        assert_eq!(Value::Object(patch.source), json!({"a": 2}));
        // A key removed twice, not adjacently, is deleted once.
        let patch = compile_update_script(&json!(
            "ctx._source.remove('a'); ctx._source.remove('b'); ctx._source.remove('a')"
        ))
        .expect("recognised");
        assert_eq!(patch.delete_keys, vec!["a", "b"]);
        // The short string form, without params.
        let patch = compile_update_script(&json!("ctx._source.remove('k')")).expect("recognised");
        assert_eq!(patch.delete_keys, vec!["k"]);
    }

    #[test]
    fn everything_else_is_refused() {
        let params = json!({"x": 1, "o": {"a": 1}});
        for source in [
            "ctx._source.a += params.x",
            "ctx._source.a == params.x",
            "ctx._source.a = 1",
            "ctx._source.a = params.missing",
            "ctx._source.a = params.o",
            "ctx.op = 'noop'",
            "ctx._source.a = params.x; ctx._source.a.b = params.x",
            "ctx._source.a.b = params.x; ctx._source.a = params.x",
            "ctx._source.a = params.x; ctx._source.remove('a')",
            "ctx._source.a.b = params.x; ctx._source.remove('a')",
            "ctx._source.remove('a.b')",
            "ctx._source.remove(a)",
            "ctx._source['a'] = params.x",
            "ctx._source.1a = params.x",
            "",
            " ; ",
        ] {
            let err = compiled(source, params.clone()).expect_err(source);
            assert_eq!(err.reason, SCRIPT_REFUSAL, "{source}");
        }
        for script in [
            json!({"source": "ctx._source.a = params.x", "lang": "expression", "params": {"x": 1}}),
            json!({"id": "stored"}),
            json!({"source": "ctx._source.a = params.x", "params": [1]}),
            json!(1),
        ] {
            let err = compile_update_script(&script).expect_err("refused");
            assert_eq!(err.status, 400);
            assert_eq!(err.kind, "illegal_argument_exception");
        }
    }
}
