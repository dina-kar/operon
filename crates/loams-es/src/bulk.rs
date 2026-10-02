//! `_bulk` (plan M1.5 Task 5): NDJSON parsing, and one write-engine call
//! per index, in order of first appearance, with the per-item results put
//! back at their positions.

use std::collections::HashMap;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, Uri, header};
use axum::response::Response;
use loams_collection::ConsistencyToken;
use serde_json::{Map, Value, json};

use crate::doc::SourceFilter;
use crate::error::EsError;
use crate::http::{Params, RequestCtx, fail, respond};
use crate::write::{
    ItemOutcome, WriteCall, WriteItem, check_occ, execute, occ_unsupported, refresh_param,
};
use crate::{EsGateway, TOKEN_HEADER};

/// A `_bulk` action.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BulkAction {
    Index,
    Create,
    Update,
    Delete,
}

impl BulkAction {
    /// The name the response reports it under.
    pub fn as_str(self) -> &'static str {
        match self {
            BulkAction::Index => "index",
            BulkAction::Create => "create",
            BulkAction::Update => "update",
            BulkAction::Delete => "delete",
        }
    }

    fn parse(name: &str) -> Option<BulkAction> {
        Some(match name {
            "index" => BulkAction::Index,
            "create" => BulkAction::Create,
            "update" => BulkAction::Update,
            "delete" => BulkAction::Delete,
            _ => return None,
        })
    }
}

/// One item of a `_bulk` body: its action line's metadata and its source
/// line (`None` for `delete`; `Err` with the parse error for a source that
/// is not JSON, which fails only that item).
#[derive(Clone, Debug, PartialEq)]
pub struct BulkLine {
    pub action: BulkAction,
    pub index: Option<String>,
    pub id: Option<String>,
    pub require_alias: Option<bool>,
    pub pipeline: Option<String>,
    pub source: Option<Result<Value, String>>,
    /// The action line's number, from 1.
    pub line_no: usize,
    /// The action line asked for optimistic concurrency control (the item
    /// fails, Phase B).
    pub occ: bool,
}

/// The metadata keys an action line may hold.
const METADATA_KEYS: [&str; 9] = [
    "_index",
    "_id",
    "require_alias",
    "pipeline",
    "routing",
    "op_type",
    "retry_on_conflict",
    "_source",
    "dynamic_templates",
];

fn validation(reason: &str) -> EsError {
    EsError::new(
        400,
        "action_request_validation_exception",
        format!("Validation Failed: 1: {reason};"),
    )
}

/// The token ES's parser would name for `value`.
fn token_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "VALUE_NULL",
        Value::Bool(_) => "VALUE_BOOLEAN",
        Value::Number(_) => "VALUE_NUMBER",
        Value::String(_) => "VALUE_STRING",
        Value::Array(_) => "START_ARRAY",
        Value::Object(_) => "START_OBJECT",
    }
}

fn malformed(line_no: usize, what: String) -> EsError {
    EsError::illegal_argument(format!(
        "Malformed action/metadata line [{line_no}], {what}"
    ))
}

/// Parses a `_bulk` body (rule 1). Errors that fail the whole request are
/// `Err`; a source that is not JSON is kept per item.
pub fn parse_ndjson(body: &[u8]) -> Result<Vec<BulkLine>, EsError> {
    if body.is_empty() {
        return Err(validation("no requests added"));
    }
    if body.last() != Some(&b'\n') {
        return Err(EsError::illegal_argument(
            "The bulk request must be terminated by a newline [\\n]",
        ));
    }
    let mut lines = body
        .split(|&b| b == b'\n')
        .enumerate()
        .map(|(i, line)| (i + 1, line.strip_suffix(b"\r").unwrap_or(line)))
        .filter(|(_, line)| !line.iter().all(u8::is_ascii_whitespace));
    let mut out = Vec::new();
    while let Some((line_no, line)) = lines.next() {
        let value: Value = serde_json::from_slice(line).map_err(|err| {
            EsError::new(
                400,
                "x_content_parse_exception",
                format!("[{line_no}:{}] {err}", err.column()),
            )
        })?;
        let Value::Object(object) = value else {
            return Err(malformed(
                line_no,
                format!("expected START_OBJECT but found [{}]", token_name(&value)),
            ));
        };
        // ES reads the first key and ignores any other (row T11-3).
        let mut entries = object.into_iter();
        let Some((name, meta)) = entries.next() else {
            return Err(malformed(
                line_no,
                "expected FIELD_NAME but found [END_OBJECT]".to_string(),
            ));
        };
        let Some(action) = BulkAction::parse(&name) else {
            return Err(malformed(
                line_no,
                format!(
                    "expected field [create], [delete], [index] or [update] but found [{name}]"
                ),
            ));
        };
        let Value::Object(meta) = meta else {
            return Err(malformed(
                line_no,
                format!("expected START_OBJECT but found [{}]", token_name(&meta)),
            ));
        };
        let mut item = BulkLine {
            action,
            index: None,
            id: None,
            require_alias: None,
            pipeline: None,
            source: None,
            line_no,
            occ: false,
        };
        for (key, value) in &meta {
            let text = || match value {
                Value::String(s) => Some(s.clone()),
                Value::Number(n) => Some(n.to_string()),
                _ => None,
            };
            match key.as_str() {
                "_index" => item.index = text(),
                "_id" => item.id = text(),
                "require_alias" => {
                    item.require_alias = match value {
                        Value::Bool(b) => Some(*b),
                        Value::String(s) if s == "true" || s == "false" => Some(s == "true"),
                        _ => {
                            return Err(EsError::illegal_argument(format!(
                                "Action/metadata line [{line_no}]: [require_alias] must be a \
                                 boolean"
                            )));
                        }
                    }
                }
                "pipeline" => item.pipeline = text(),
                "op_type" => match (action, text().as_deref()) {
                    (BulkAction::Index, Some("create")) => item.action = BulkAction::Create,
                    (BulkAction::Index, Some("index")) => {}
                    (_, other) => {
                        return Err(EsError::illegal_argument(format!(
                            "Action/metadata line [{line_no}]: opType must be 'create' or \
                             'index', found: [{}]",
                            other.unwrap_or_default()
                        )));
                    }
                },
                "if_seq_no" | "if_primary_term" | "version" | "version_type" => item.occ = true,
                k if METADATA_KEYS.contains(&k) => {}
                other => {
                    return Err(EsError::illegal_argument(format!(
                        "Action/metadata line [{line_no}] contains an unknown parameter [{other}]"
                    )));
                }
            }
        }
        if action != BulkAction::Delete {
            let Some((_, source)) = lines.next() else {
                return Err(validation("source is missing"));
            };
            item.source = Some(serde_json::from_slice(source).map_err(|err| {
                format!("[{}:{}] failed to parse: {err}", err.line(), err.column())
            }));
        }
        out.push(item);
    }
    if out.is_empty() {
        return Err(validation("no requests added"));
    }
    Ok(out)
}

const BULK_PARAMS: &[&str] = &[
    "refresh",
    "routing",
    "timeout",
    "wait_for_active_shards",
    "require_alias",
    "require_data_stream",
    "pipeline",
    "_source",
    "_source_includes",
    "_source_excludes",
    "include_source_on_error",
    "list_executed_pipelines",
];

/// `POST|PUT /_bulk`.
pub(crate) async fn bulk(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    body: Bytes,
) -> Response {
    run(&gw, &ctx, &uri, None, &body).await
}

/// `POST|PUT /{index}/_bulk`.
pub(crate) async fn bulk_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    run(&gw, &ctx, &uri, Some(&index), &body).await
}

/// The items of one `execute` call: one index, one `require_alias` and one
/// `pipeline` (row T5-2).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct GroupKey {
    index: String,
    require_alias: bool,
    pipeline: Option<String>,
}

async fn run(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    path_index: Option<&str>,
    body: &[u8],
) -> Response {
    let parsed = (|| {
        let params = Params::parse(uri.query(), uri.path(), BULK_PARAMS)?;
        check_occ(&params)?;
        let refresh = refresh_param(&params)?;
        let require_alias = params.bool("require_alias")?.unwrap_or(false);
        let source_on_update = SourceFilter::from_params(&params)?;
        let lines = parse_ndjson(body)?;
        if lines.iter().any(|l| l.index.is_none()) && path_index.is_none() {
            return Err(validation("index is missing"));
        }
        // An update or delete without `_id` fails the whole request, as
        // ES's `BulkRequest.validate` does (row T11-3).
        if lines
            .iter()
            .any(|l| matches!(l.action, BulkAction::Update | BulkAction::Delete) && l.id.is_none())
        {
            return Err(validation("id is missing"));
        }
        Ok::<_, EsError>((params, refresh, require_alias, source_on_update, lines))
    })();
    let (params, refresh, require_alias, source_on_update, lines) = match parsed {
        Ok(parsed) => parsed,
        Err(err) => return fail(ctx, &err),
    };
    let default_pipeline = params.str("pipeline").map(str::to_string);
    let mut outcomes: Vec<Option<ItemOutcome>> = vec![None; lines.len()];
    let mut groups: Vec<(GroupKey, Vec<(usize, WriteItem)>)> = Vec::new();
    let mut group_of: HashMap<GroupKey, usize> = HashMap::new();
    for (i, line) in lines.iter().enumerate() {
        let index = line
            .index
            .clone()
            .or_else(|| path_index.map(str::to_string))
            .unwrap_or_default();
        let item_error = |error: EsError| {
            let mut body = Map::new();
            body.insert("_index".to_string(), json!(index));
            body.insert("_id".to_string(), json!(line.id));
            body.insert("error".to_string(), error.cause_value());
            ItemOutcome {
                status: error.status,
                body,
                error: Some(error),
            }
        };
        if line.occ {
            outcomes[i] = Some(item_error(occ_unsupported()));
            continue;
        }
        let source = match &line.source {
            Some(Err(message)) => {
                let error = EsError::new(400, "document_parsing_exception", message.clone());
                outcomes[i] = Some(item_error(error));
                continue;
            }
            Some(Ok(source)) => Some(source.clone()),
            None => None,
        };
        let item = match (line.action, line.id.clone()) {
            (BulkAction::Index | BulkAction::Create, id) => WriteItem::Index {
                id,
                source: source.unwrap_or(Value::Null),
                create: line.action == BulkAction::Create,
            },
            // Both have an `_id`: checked before the groups.
            (BulkAction::Update, id) => WriteItem::Update {
                id: id.unwrap_or_default(),
                body: source.unwrap_or(Value::Null),
            },
            (BulkAction::Delete, id) => WriteItem::Delete {
                id: id.unwrap_or_default(),
            },
        };
        let key = GroupKey {
            index,
            require_alias: line.require_alias.unwrap_or(require_alias),
            pipeline: line.pipeline.clone().or_else(|| default_pipeline.clone()),
        };
        let at = *group_of.entry(key.clone()).or_insert_with(|| {
            groups.push((key, Vec::new()));
            groups.len() - 1
        });
        groups[at].1.push((i, item));
    }
    let mut token: Option<ConsistencyToken> = None;
    for (key, items) in groups {
        let (positions, items): (Vec<usize>, Vec<WriteItem>) = items.into_iter().unzip();
        let call = WriteCall {
            gateway: gw,
            ctx,
            index: &key.index,
            require_alias: key.require_alias,
            pipeline: key.pipeline.clone(),
            refresh,
            source_on_update: source_on_update.clone(),
        };
        let (answers, written) = execute(call, items).await;
        for (at, outcome) in positions.into_iter().zip(answers) {
            outcomes[at] = Some(outcome);
        }
        if let Some(written) = written {
            match &mut token {
                Some(token) => token.merge(&written),
                None => token = Some(written),
            }
        }
    }
    let mut errors = false;
    let mut retry_after: Option<u64> = None;
    let mut items = Vec::with_capacity(lines.len());
    for (line, outcome) in lines.iter().zip(outcomes) {
        let outcome = outcome.unwrap_or_else(|| ItemOutcome {
            status: 500,
            body: Map::new(),
            error: None,
        });
        errors |= !(200..300).contains(&outcome.status);
        if let Some(secs) = outcome.error.as_ref().and_then(|e| e.retry_after_secs) {
            retry_after = Some(retry_after.map_or(secs, |r| r.max(secs)));
        }
        let mut body = Map::new();
        let mut status = Some(outcome.status);
        for (key, value) in outcome.body {
            if key == "error"
                && let Some(s) = status.take()
            {
                body.insert("status".to_string(), json!(s));
            }
            body.insert(key, value);
        }
        if let Some(s) = status {
            body.insert("status".to_string(), json!(s));
        }
        items.push(json!({line.action.as_str(): Value::Object(body)}));
    }
    let took = u64::try_from(ctx.started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let body = json!({"took": took, "errors": errors, "items": items});
    let mut response = respond(ctx, 200, &body);
    if let Some(token) = token
        && let Ok(value) = HeaderValue::from_str(&token.to_string())
    {
        response.headers_mut().insert(TOKEN_HEADER, value);
    }
    if let Some(secs) = retry_after {
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(secs));
    }
    response
}
