//! Document reads (plan M1.5 Task 6): `GET`/`HEAD /{index}/_doc/{id}`,
//! `GET`/`HEAD /{index}/_source/{id}` and `_mget`, with `_source`
//! filtering ([`SourceFilter`]).
//!
//! A read resolves its index with `resolve_single` (Ruling 9), fetches the
//! whole source and only the vectors the filter keeps (Review Focus 2),
//! puts the vectors back with `restore_vectors` (Ruling 2) and then
//! filters. `_version` is `_seq_no + 1` (Ruling 4).

use std::collections::{BTreeSet, HashMap};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{Method, Uri};
use axum::response::Response;
use loams_collection::PrimaryKey;
use loams_query::{Projection, SourceFilter as IrSourceFilter, StoredDoc};
use serde_json::{Map, Value, json};

use crate::EsGateway;
use crate::doc::{SourceFilter, restore_vectors};
use crate::error::{ErrorContext, EsError};
use crate::http::{Params, RequestCtx, fail, json_body, respond, respond_head};
use crate::mapping::IndexView;
use crate::names::resolve_single;

/// `GET /{index}/_doc/{id}`'s parameters.
const GET_PARAMS: &[&str] = &[
    "_source",
    "_source_includes",
    "_source_excludes",
    "realtime",
    "refresh",
    "routing",
    "preference",
    "stored_fields",
    "version",
    "version_type",
];

/// `GET /{index}/_source/{id}`'s parameters: [`GET_PARAMS`] minus
/// `stored_fields`.
const SOURCE_PARAMS: &[&str] = &[
    "_source",
    "_source_includes",
    "_source_excludes",
    "realtime",
    "refresh",
    "routing",
    "preference",
    "version",
    "version_type",
];

/// `_mget`'s parameters.
const MGET_PARAMS: &[&str] = &[
    "_source",
    "_source_includes",
    "_source_excludes",
    "realtime",
    "refresh",
    "routing",
    "preference",
    "stored_fields",
];

/// The keys an `_mget` `docs` entry takes.
const MGET_DOC_KEYS: &[&str] = &["_index", "_id", "_source", "routing", "stored_fields"];

/// 400 `action_request_validation_exception` with ES's one-error text.
fn validation(reason: &str) -> EsError {
    EsError::new(
        400,
        "action_request_validation_exception",
        format!("Validation Failed: 1: {reason};"),
    )
}

/// `stored_fields`: `_none_` leaves the source out unless `_source` asks
/// for it (ES's `normalizeFetchSourceContent`); stored fields are Phase B.
fn stored_fields_none(value: Option<&str>) -> Result<bool, EsError> {
    match value {
        None => Ok(false),
        Some("_none_") => Ok(true),
        Some(_) => Err(EsError::unsupported("stored_fields")),
    }
}

/// The parameters every read shares: `realtime` and `refresh` must be
/// booleans (reads are `Strong` anyway), `routing` and `preference` are
/// ignored, and versioned reads are Phase B (Ruling 4).
fn check_read_params(params: &Params) -> Result<(), EsError> {
    params.bool("realtime")?;
    params.bool("refresh")?;
    match ["version", "version_type"]
        .into_iter()
        .find(|p| params.str(p).is_some())
    {
        Some(p) => Err(EsError::unsupported(p)),
        None => Ok(()),
    }
}

/// The filter a read applies: the request's, or the whole source; none at
/// all under `stored_fields=_none_` without a `_source` parameter.
fn effective(filter: Option<SourceFilter>, stored_none: bool) -> SourceFilter {
    filter.unwrap_or(SourceFilter {
        enabled: !stored_none,
        ..SourceFilter::default()
    })
}

/// A fetched document's `_source` through `filter`: vectors restored at
/// their paths, then filtered; `None` when the filter turns it off.
fn render_source(doc: &mut StoredDoc, filter: &SourceFilter) -> Option<Map<String, Value>> {
    if !filter.enabled {
        return None;
    }
    let mut source = doc.source.take().unwrap_or_default();
    restore_vectors(&mut source, &doc.vectors);
    Some(filter.apply(source))
}

/// A found document's body (rule 4).
fn found_body(index: &str, id: &str, doc: &mut StoredDoc, filter: &SourceFilter) -> Value {
    let mut body = Map::new();
    body.insert("_index".to_string(), json!(index));
    body.insert("_id".to_string(), json!(id));
    body.insert("_version".to_string(), json!(doc.seq_no + 1));
    body.insert("_seq_no".to_string(), json!(doc.seq_no));
    body.insert("_primary_term".to_string(), json!(1));
    body.insert("found".to_string(), json!(true));
    if let Some(source) = render_source(doc, filter) {
        body.insert("_source".to_string(), Value::Object(source));
    }
    Value::Object(body)
}

/// A missing document's body (rule 4: not an error envelope).
fn missing_body(index: &str, id: &str) -> Value {
    json!({"_index": index, "_id": id, "found": false})
}

/// Fetches `ids` from `index` (a concrete collection): the whole source
/// when any filter wants it, and each vector some filter keeps. The
/// answer is keyed by id; a missing document is `None`.
async fn fetch(
    gw: &EsGateway,
    ctx: &RequestCtx,
    index: &str,
    ids: &[&str],
    filters: &[&SourceFilter],
) -> Result<HashMap<String, Option<StoredDoc>>, EsError> {
    let service = gw.service();
    let read_error = |err| EsError::from_service(err, ErrorContext::Read);
    let wants_source = filters.iter().any(|f| f.enabled);
    let vectors = if wants_source {
        let info = service
            .get_collection(&ctx.namespace, index)
            .await
            .map_err(read_error)?;
        let view = IndexView::new(info);
        view.es
            .vectors
            .keys()
            .filter(|path| filters.iter().any(|f| f.keeps_path(path)))
            .cloned()
            .collect()
    } else {
        Vec::new()
    };
    let select = Projection {
        source: if wants_source {
            IrSourceFilter::All
        } else {
            IrSourceFilter::None
        },
        vectors,
        fields: Vec::new(),
    };
    let unique: Vec<&str> = ids
        .iter()
        .copied()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let mut found = HashMap::new();
    for chunk in unique.chunks(service.config().max_get_keys.max(1)) {
        let pks: Vec<PrimaryKey> = chunk
            .iter()
            .map(|id| PrimaryKey::Str((*id).to_string()))
            .collect();
        let docs = service
            .get(
                &ctx.namespace,
                index,
                &pks,
                &select,
                ctx.consistency.clone(),
            )
            .await
            .map_err(read_error)?;
        for (id, doc) in chunk.iter().zip(docs) {
            found.insert((*id).to_string(), doc);
        }
    }
    Ok(found)
}

/// One document of a single-document read, with the concrete index.
async fn read_one(
    gw: &EsGateway,
    ctx: &RequestCtx,
    index: &str,
    id: &str,
    filter: &SourceFilter,
) -> Result<(String, Option<StoredDoc>), EsError> {
    let target = resolve_single(gw.service(), &ctx.namespace, index).await?;
    let mut docs = fetch(gw, ctx, &target.name, &[id], &[filter]).await?;
    Ok((target.name, docs.remove(id).flatten()))
}

/// `GET`/`HEAD /{index}/_doc/{id}` (rule 4).
pub(crate) async fn get_doc(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    method: Method,
    uri: Uri,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    let head = method == Method::HEAD;
    let filter = (|| {
        let params = Params::parse(uri.query(), uri.path(), GET_PARAMS)?;
        check_read_params(&params)?;
        let stored_none = stored_fields_none(params.str("stored_fields"))?;
        let filter = effective(SourceFilter::from_params(&params)?, stored_none);
        Ok::<_, EsError>(filter)
    })();
    let filter = match filter {
        // `HEAD` needs no source.
        Ok(filter) if head => SourceFilter {
            enabled: false,
            ..filter
        },
        Ok(filter) => filter,
        Err(err) if head => return respond_head(err.status),
        Err(err) => return fail(&ctx, &err),
    };
    match read_one(&gw, &ctx, &index, &id, &filter).await {
        Ok((_, Some(_))) if head => respond_head(200),
        Ok((_, None)) if head => respond_head(404),
        Ok((index, Some(mut doc))) => {
            respond(&ctx, 200, &found_body(&index, &id, &mut doc, &filter))
        }
        Ok((index, None)) => respond(&ctx, 404, &missing_body(&index, &id)),
        Err(err) if head => respond_head(err.status),
        Err(err) => fail(&ctx, &err),
    }
}

/// `GET`/`HEAD /{index}/_source/{id}` (rule 5).
pub(crate) async fn get_source(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    method: Method,
    uri: Uri,
    Path((index, id)): Path<(String, String)>,
) -> Response {
    let head = method == Method::HEAD;
    let filter = (|| {
        let params = Params::parse(uri.query(), uri.path(), SOURCE_PARAMS)?;
        check_read_params(&params)?;
        let filter = SourceFilter::from_params(&params)?.unwrap_or_default();
        if !filter.enabled {
            return Err(validation("fetching source can not be disabled"));
        }
        Ok::<_, EsError>(filter)
    })();
    let filter = match filter {
        Ok(filter) if head => SourceFilter {
            enabled: false,
            ..filter
        },
        Ok(filter) => filter,
        Err(err) if head => return respond_head(err.status),
        Err(err) => return fail(&ctx, &err),
    };
    match read_one(&gw, &ctx, &index, &id, &filter).await {
        Ok((_, Some(_))) if head => respond_head(200),
        Ok((_, None)) if head => respond_head(404),
        Ok((_, Some(mut doc))) => {
            let source = render_source(&mut doc, &filter).unwrap_or_default();
            respond(&ctx, 200, &Value::Object(source))
        }
        Ok((index, None)) => fail(
            &ctx,
            &EsError::new(
                404,
                "resource_not_found_exception",
                format!("Document not found [{index}]/_doc/[{id}]"),
            ),
        ),
        Err(err) if head => respond_head(err.status),
        Err(err) => fail(&ctx, &err),
    }
}

/// One `_mget` entry as requested.
#[derive(Debug)]
struct MgetItem {
    index: Option<String>,
    id: String,
    filter: SourceFilter,
}

/// An `_id` in an `_mget` body: a string, or a number taken as its text.
fn mget_id(value: &Value) -> Result<String, EsError> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        other => Err(EsError::new(
            400,
            "parse_exception",
            format!("failed to parse multi get request. unexpected value for [_id]: [{other}]"),
        )),
    }
}

/// The `_mget` body (rule 6): `{"docs": [...]}` or `{"ids": [...]}`, each
/// entry with the URL's filter unless it carries its own `_source`.
fn parse_mget(
    body: Option<Value>,
    path_index: Option<&str>,
    url_filter: &SourceFilter,
    stored_none: bool,
) -> Result<Vec<MgetItem>, EsError> {
    let Some(body) = body else {
        return Err(validation("no documents to get"));
    };
    let Value::Object(body) = body else {
        return Err(EsError::new(
            400,
            "parse_exception",
            "failed to parse multi get request: the body is not an object",
        ));
    };
    let mut items = Vec::new();
    // ES's `MultiGetRequest.validate`: every entry without an index or an
    // id, numbered, fails the request (row T11-3).
    let mut invalid: Vec<String> = Vec::new();
    // Each entry's position in the request, as ES numbers them.
    let mut position = 0usize;
    for (key, value) in &body {
        match (key.as_str(), value) {
            ("docs", Value::Array(docs)) => {
                for doc in docs {
                    let Value::Object(doc) = doc else {
                        return Err(EsError::new(
                            400,
                            "parse_exception",
                            "failed to parse multi get request. docs array element should \
                             include an object",
                        ));
                    };
                    if let Some(key) = doc.keys().find(|k| !MGET_DOC_KEYS.contains(&k.as_str())) {
                        return Err(EsError::new(
                            400,
                            "parse_exception",
                            format!("failed to parse multi get request. unknown field [{key}]"),
                        ));
                    }
                    let index = match doc.get("_index") {
                        None | Some(Value::Null) => path_index.map(str::to_string),
                        Some(Value::String(s)) => Some(s.clone()),
                        Some(other) => Some(other.to_string()),
                    };
                    let n = position;
                    position += 1;
                    if index.is_none() {
                        invalid.push(format!("index is missing for doc {n}"));
                    }
                    let Some(id) = doc.get("_id").filter(|v| !v.is_null()) else {
                        invalid.push(format!("id is missing for doc {n}"));
                        continue;
                    };
                    let doc_stored_none = match doc.get("stored_fields") {
                        None => stored_none,
                        Some(Value::String(s)) => stored_fields_none(Some(s))?,
                        Some(Value::Array(fields)) if fields.is_empty() => stored_none,
                        Some(Value::Array(fields)) => match fields.as_slice() {
                            [Value::String(s)] => stored_fields_none(Some(s))?,
                            _ => return Err(EsError::unsupported("stored_fields")),
                        },
                        Some(_) => return Err(EsError::unsupported("stored_fields")),
                    };
                    let filter = match doc.get("_source") {
                        Some(source) => SourceFilter::from_body(source)?,
                        None if doc_stored_none != stored_none => effective(None, doc_stored_none),
                        None => url_filter.clone(),
                    };
                    items.push(MgetItem {
                        index,
                        id: mget_id(id)?,
                        filter,
                    });
                }
            }
            ("ids", Value::Array(ids)) => {
                for id in ids {
                    if path_index.is_none() {
                        invalid.push(format!("index is missing for doc {position}"));
                    }
                    position += 1;
                    items.push(MgetItem {
                        index: path_index.map(str::to_string),
                        id: mget_id(id)?,
                        filter: url_filter.clone(),
                    });
                }
            }
            (key, value) => {
                let token = match value {
                    Value::Array(_) => "START_ARRAY",
                    Value::Object(_) => "START_OBJECT",
                    _ => "VALUE",
                };
                return Err(EsError::parsing(format!(
                    "unknown key [{key}] for a {token}, expected [docs] or [ids]"
                )));
            }
        }
    }
    if !invalid.is_empty() {
        let reasons: String = invalid
            .iter()
            .enumerate()
            .map(|(i, e)| format!("{}: {e};", i + 1))
            .collect();
        return Err(EsError::new(
            400,
            "action_request_validation_exception",
            format!("Validation Failed: {reasons}"),
        ));
    }
    if items.is_empty() {
        return Err(validation("no documents to get"));
    }
    Ok(items)
}

/// An `_mget` entry that failed.
fn error_entry(index: Option<&str>, id: &str, error: &EsError) -> Value {
    json!({"_index": index, "_id": id, "error": error.to_body()["error"]})
}

async fn mget(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    path_index: Option<&str>,
    body: &[u8],
) -> Response {
    let parsed = (|| {
        let params = Params::parse(uri.query(), uri.path(), MGET_PARAMS)?;
        check_read_params(&params)?;
        let stored_none = stored_fields_none(params.str("stored_fields"))?;
        let url_filter = effective(SourceFilter::from_params(&params)?, stored_none);
        parse_mget(json_body(body)?, path_index, &url_filter, stored_none)
    })();
    let items = match parsed {
        Ok(items) => items,
        Err(err) => return fail(ctx, &err),
    };
    let mut entries: Vec<Option<Value>> = vec![None; items.len()];
    // Each name resolves once; entries are grouped per concrete index.
    let mut resolved: HashMap<&str, Result<String, EsError>> = HashMap::new();
    let mut groups: Vec<(String, Vec<usize>)> = Vec::new();
    let mut group_of: HashMap<String, usize> = HashMap::new();
    for (i, item) in items.iter().enumerate() {
        let Some(name) = item.index.as_deref() else {
            entries[i] = Some(error_entry(None, &item.id, &validation("index is missing")));
            continue;
        };
        if !resolved.contains_key(name) {
            let target = resolve_single(gw.service(), &ctx.namespace, name)
                .await
                .map(|t| t.name);
            resolved.insert(name, target);
        }
        match &resolved[name] {
            Ok(concrete) => match group_of.get(concrete) {
                Some(&g) => groups[g].1.push(i),
                None => {
                    group_of.insert(concrete.clone(), groups.len());
                    groups.push((concrete.clone(), vec![i]));
                }
            },
            Err(err) => entries[i] = Some(error_entry(Some(name), &item.id, err)),
        }
    }
    for (index, members) in groups {
        let ids: Vec<&str> = members.iter().map(|&i| items[i].id.as_str()).collect();
        let filters: Vec<&SourceFilter> = members.iter().map(|&i| &items[i].filter).collect();
        match fetch(gw, ctx, &index, &ids, &filters).await {
            Ok(found) => {
                for &i in &members {
                    let item = &items[i];
                    entries[i] = Some(match found.get(&item.id).cloned().flatten() {
                        Some(mut doc) => found_body(&index, &item.id, &mut doc, &item.filter),
                        None => missing_body(&index, &item.id),
                    });
                }
            }
            Err(err) => {
                for &i in &members {
                    entries[i] = Some(error_entry(Some(&index), &items[i].id, &err));
                }
            }
        }
    }
    let docs: Vec<Value> = entries.into_iter().map(Option::unwrap_or_default).collect();
    respond(ctx, 200, &json!({ "docs": docs }))
}

/// `GET`/`POST /_mget` (rule 6).
pub(crate) async fn mget_all(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    body: Bytes,
) -> Response {
    mget(&gw, &ctx, &uri, None, &body).await
}

/// `GET`/`POST /{index}/_mget` (rule 6): the path index is the default of
/// entries without `_index`.
pub(crate) async fn mget_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    mget(&gw, &ctx, &uri, Some(&index), &body).await
}
