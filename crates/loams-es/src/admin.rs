//! Index administration (plan M1.5 Task 3): create, delete, `HEAD` and
//! `GET` of indices, mappings, aliases (Ruling 9, D57) and `_refresh`.
//!
//! Every alias request is one `update_alias_targets` call, so it applies
//! atomically; the gateway first applies it to the aliases it reads, to
//! answer ES's own errors.

use std::collections::{BTreeMap, BTreeSet};

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::Uri;
use axum::response::Response;
use loams_query::{AliasTargetAction, CollectionService, NameInfo, ServiceError};
use serde_json::{Map, Value, json};

use crate::EsGateway;
use crate::error::{ErrorContext, EsError};
use crate::http::{Params, RequestCtx, fail, json_body, respond, respond_head};
use crate::mapping::{index_uuid, plan_create, plan_put_mapping, render_mappings, render_settings};
use crate::names::{IndexExpr, ResolveOptions, glob, resolve, resolve_write, validate_index_name};

const CREATE_PARAMS: &[&str] = &["wait_for_active_shards", "timeout", "master_timeout"];
const DELETE_PARAMS: &[&str] = &[
    "timeout",
    "master_timeout",
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
];
const HEAD_PARAMS: &[&str] = &[
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
    "local",
    "flat_settings",
    "include_defaults",
];
const GET_PARAMS: &[&str] = &[
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
    "local",
    "flat_settings",
    "include_defaults",
    "master_timeout",
    "features",
];
const GET_MAPPING_PARAMS: &[&str] = &[
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
    "local",
    "master_timeout",
];
const PUT_MAPPING_PARAMS: &[&str] = &[
    "timeout",
    "master_timeout",
    "write_index_only",
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
];
const ALIAS_WRITE_PARAMS: &[&str] = &["timeout", "master_timeout"];
const ALIAS_GET_PARAMS: &[&str] = &[
    "ignore_unavailable",
    "allow_no_indices",
    "expand_wildcards",
    "local",
];
const REFRESH_PARAMS: &[&str] = &["ignore_unavailable", "allow_no_indices", "expand_wildcards"];

/// Alias settings refused as Phase B (Ruling 9).
const PHASE_B_ALIAS_KEYS: [&str; 6] = [
    "filter",
    "routing",
    "index_routing",
    "search_routing",
    "is_hidden",
    "must_exist",
];

fn answer(ctx: &RequestCtx, result: Result<(u16, Value), EsError>) -> Response {
    match result {
        Ok((status, body)) => respond(ctx, status, &body),
        Err(err) => fail(ctx, &err),
    }
}

fn ack() -> (u16, Value) {
    (200, json!({"acknowledged": true}))
}

fn admin_error(error: ServiceError) -> EsError {
    EsError::from_service(error, ErrorContext::Admin)
}

fn is_missing(error: &ServiceError) -> bool {
    matches!(
        error,
        ServiceError::NotFound {
            kind: "collection" | "alias",
            ..
        }
    )
}

fn parse_exception(reason: impl Into<String>) -> EsError {
    EsError::new(400, "parse_exception", reason)
}

fn validation(reason: &str) -> EsError {
    EsError::new(
        400,
        "action_request_validation_exception",
        format!("Validation Failed: 1: {reason};"),
    )
}

fn matches_alias(expression: &str) -> EsError {
    EsError::illegal_argument(format!(
        "The provided expression [{expression}] matches an alias, specify the corresponding \
         concrete indices instead."
    ))
}

/// `ignore_unavailable`, `allow_no_indices` and `expand_wildcards` (`none`
/// turns wildcards off; the other values are accepted).
fn resolve_options(params: &Params) -> Result<ResolveOptions, EsError> {
    let defaults = ResolveOptions::default();
    Ok(ResolveOptions {
        ignore_unavailable: params
            .bool("ignore_unavailable")?
            .unwrap_or(defaults.ignore_unavailable),
        allow_no_indices: params
            .bool("allow_no_indices")?
            .unwrap_or(defaults.allow_no_indices),
        allow_wildcards: params
            .list("expand_wildcards")
            .is_none_or(|values| !values.iter().all(|v| v == "none")),
    })
}

/// ES's alias-name rules: those of index names except the lowercase rule.
fn validate_alias_name(alias: &str) -> Result<(), EsError> {
    match validate_index_name(alias) {
        Ok(()) => Ok(()),
        Err(err) if err.reason.ends_with("must be lowercase") => Ok(()),
        Err(err) => {
            let why = err
                .reason
                .split_once("], ")
                .map_or(err.reason.as_str(), |(_, why)| why);
            Err(EsError::invalid_alias_name(alias, why))
        }
    }
}

fn alias_clashes_with_index(alias: &str) -> EsError {
    EsError::invalid_alias_name(
        alias,
        "an index or data stream exists with the same name as the alias",
    )
}

/// `{alias: {} | {"is_write_index": b}}` of one member.
fn alias_setting(is_write_index: Option<bool>) -> Value {
    match is_write_index {
        Some(b) => json!({"is_write_index": b}),
        None => json!({}),
    }
}

// ----- create -----

/// `PUT /{index}`.
pub(crate) async fn create_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    answer(&ctx, create(&gw, &ctx, &uri, &index, &body).await)
}

/// Err with ES's answer when `index` names an index or an alias.
async fn check_free(service: &CollectionService, ns: &str, index: &str) -> Result<(), EsError> {
    match service.resolve_name(ns, index).await {
        Ok(NameInfo::Collection(name)) => {
            let uuid = match service.get_collection(ns, &name).await {
                Ok(info) => index_uuid(info.id),
                Err(_) => "_na_".to_string(),
            };
            Err(EsError::already_exists(&name, &uuid))
        }
        Ok(NameInfo::Alias(_)) => Err(EsError::new(
            400,
            "invalid_index_name_exception",
            format!("Invalid index name [{index}], already exists as alias"),
        )
        .with("index_uuid", "_na_")
        .with("index", index)),
        Err(err) if is_missing(&err) => Ok(()),
        Err(err) => Err(admin_error(err)),
    }
}

/// The `is_write_index` of an alias body, refusing Phase B settings.
fn alias_body(body: &Map<String, Value>) -> Result<Option<bool>, EsError> {
    let mut is_write_index = None;
    for (key, value) in body {
        match key.as_str() {
            "is_write_index" => {
                is_write_index = match value {
                    Value::Null => None,
                    Value::Bool(b) => Some(*b),
                    other => {
                        return Err(parse_exception(format!(
                            "[is_write_index] must be a boolean, got [{other}]"
                        )));
                    }
                }
            }
            k if PHASE_B_ALIAS_KEYS.contains(&k) => return Err(EsError::unsupported(k)),
            other => {
                return Err(parse_exception(format!("[alias] unknown field [{other}]")));
            }
        }
    }
    Ok(is_write_index)
}

async fn create(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: &str,
    body: &[u8],
) -> Result<(u16, Value), EsError> {
    Params::parse(uri.query(), uri.path(), CREATE_PARAMS)?;
    validate_index_name(index)?;
    let body = json_body(body)?;
    let empty = Map::new();
    let body = match &body {
        None => &empty,
        Some(Value::Object(map)) => map,
        Some(_) => return Err(parse_exception("request body must be an object")),
    };
    if let Some(key) = body
        .keys()
        .find(|k| !matches!(k.as_str(), "mappings" | "settings" | "aliases"))
    {
        return Err(parse_exception(format!(
            "unknown key [{key}] for create index"
        )));
    }
    let plan = plan_create(body.get("mappings"), body.get("settings"))?;
    let mut adds = Vec::new();
    match body.get("aliases") {
        None | Some(Value::Null) => {}
        Some(Value::Object(aliases)) => {
            for (alias, setting) in aliases {
                validate_alias_name(alias)?;
                let is_write_index = match setting {
                    Value::Object(map) => alias_body(map)?,
                    Value::Null => None,
                    other => {
                        return Err(parse_exception(format!(
                            "alias [{alias}] must be an object, got [{other}]"
                        )));
                    }
                };
                adds.push(AliasTargetAction::Add {
                    alias: alias.clone(),
                    collection: index.to_string(),
                    is_write_index,
                });
            }
        }
        Some(_) => return Err(parse_exception("[aliases] must be an object")),
    }
    let service = gw.service();
    let ns = ctx.namespace.as_str();
    check_free(service, ns, index).await?;
    for action in &adds {
        if let AliasTargetAction::Add { alias, .. } = action {
            let clash = alias == index
                || matches!(
                    service.resolve_name(ns, alias).await,
                    Ok(NameInfo::Collection(_))
                );
            if clash {
                return Err(alias_clashes_with_index(alias));
            }
        }
    }
    let created = match service
        .create_collection_owned(ns, index, plan.schema(), plan.partitions)
        .await
    {
        Ok((_, created)) => created,
        Err(ServiceError::AlreadyExists(_)) => {
            // Created concurrently: answer as for an existing index.
            check_free(service, ns, index).await?;
            return Err(EsError::already_exists(index, "_na_"));
        }
        Err(err) => return Err(admin_error(err)),
    };
    if !adds.is_empty()
        && let Err(err) = service.update_alias_targets(ns, adds).await
    {
        // ES creates the index and its aliases in one step, so the index
        // is dropped again, but only if this request created it (an
        // identical concurrent create may own it, row T3-4).
        if created && let Err(drop) = service.drop_collection(ns, index).await {
            tracing::warn!(index, error = %drop, "could not drop an index whose aliases were refused");
        }
        return Err(alias_service_error(err));
    }
    Ok((
        200,
        json!({"acknowledged": true, "shards_acknowledged": true, "index": index}),
    ))
}

// ----- delete, HEAD, GET -----

/// `DELETE /{index}` (Ruling 21).
pub(crate) async fn delete_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
) -> Response {
    answer(&ctx, delete(&gw, &ctx, &uri, &index).await)
}

async fn delete(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: &str,
) -> Result<(u16, Value), EsError> {
    let params = Params::parse(uri.query(), uri.path(), DELETE_PARAMS)?;
    let ignore_unavailable = params.bool("ignore_unavailable")?.unwrap_or(false);
    let items: Vec<&str> = index.split(',').filter(|i| !i.is_empty()).collect();
    if items.iter().any(|i| *i == "_all" || i.contains('*')) || items.is_empty() {
        return Err(EsError::illegal_argument(
            "Wildcard expressions or all indices are not allowed",
        ));
    }
    let service = gw.service();
    let ns = ctx.namespace.as_str();
    // Every name resolves before anything is deleted: a missing one fails
    // the request with nothing deleted, as in ES (row T11-3; was T3-2).
    let mut targets = Vec::new();
    for item in items {
        match service.resolve_name(ns, item).await {
            Ok(NameInfo::Collection(name)) => targets.push((item, name)),
            Ok(NameInfo::Alias(_)) => return Err(matches_alias(item)),
            Err(err) if is_missing(&err) => {
                if !ignore_unavailable {
                    return Err(EsError::index_not_found(item));
                }
            }
            Err(err) => return Err(admin_error(err)),
        }
    }
    for (item, name) in targets {
        let dropped = service
            .drop_collection(ns, &name)
            .await
            .map_err(admin_error)?;
        if !dropped && !ignore_unavailable {
            return Err(EsError::index_not_found(item));
        }
    }
    Ok(ack())
}

/// `HEAD /{index}`: 200 when the expression covers an index, else 404.
pub(crate) async fn head_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
) -> Response {
    let found = async {
        let params = Params::parse(uri.query(), uri.path(), HEAD_PARAMS)?;
        let opts = resolve_options(&params)?;
        let expr = IndexExpr::parse(&index);
        resolve(gw.service(), &ctx.namespace, &expr, opts)
            .await
            .map(|resolved| !resolved.is_empty())
    };
    match found.await {
        Ok(true) => respond_head(200),
        Ok(false) => respond_head(404),
        Err(err) => fail(&ctx, &err),
    }
}

/// `GET /{index}`.
pub(crate) async fn get_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
) -> Response {
    answer(&ctx, get(&gw, &ctx, &uri, &index).await)
}

/// Settings as flat `index.*` keys (`flat_settings=true`).
fn flatten(prefix: &str, value: &Value, out: &mut Map<String, Value>) {
    match value {
        Value::Object(map) => {
            for (key, inner) in map {
                let key = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                flatten(&key, inner, out);
            }
        }
        other => {
            out.insert(prefix.to_string(), other.clone());
        }
    }
}

/// The aliases of `index` in ES's rendering.
fn aliases_of(aliases: &[loams_query::AliasInfo], index: &str) -> Map<String, Value> {
    let mut out = Map::new();
    for alias in aliases {
        if let Some(member) = alias.members.iter().find(|m| m.collection == index) {
            out.insert(alias.alias.clone(), alias_setting(member.is_write_index));
        }
    }
    out
}

async fn get(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: &str,
) -> Result<(u16, Value), EsError> {
    let params = Params::parse(uri.query(), uri.path(), GET_PARAMS)?;
    let opts = resolve_options(&params)?;
    let flat = params.bool("flat_settings")?.unwrap_or(false);
    let mut features = BTreeSet::from(["aliases", "mappings", "settings"]);
    if let Some(list) = params.list("features") {
        features.clear();
        for feature in &list {
            match feature.as_str() {
                "aliases" => features.insert("aliases"),
                "mappings" => features.insert("mappings"),
                "settings" => features.insert("settings"),
                other => {
                    return Err(EsError::illegal_argument(format!(
                        "Invalid features specified [{other}]"
                    )));
                }
            };
        }
    }
    let service = gw.service();
    let ns = ctx.namespace.as_str();
    let resolved = resolve(service, ns, &IndexExpr::parse(index), opts).await?;
    let aliases = service.list_aliases(ns).await.map_err(admin_error)?;
    let mut out = Map::new();
    for r in resolved {
        let info = match service.get_collection(ns, &r.name).await {
            Ok(info) => info,
            Err(err) if is_missing(&err) => continue,
            Err(err) => return Err(admin_error(err)),
        };
        let mut entry = Map::new();
        if features.contains("aliases") {
            entry.insert(
                "aliases".to_string(),
                Value::Object(aliases_of(&aliases, &r.name)),
            );
        }
        if features.contains("mappings") {
            entry.insert("mappings".to_string(), render_mappings(&info.schema));
        }
        if features.contains("settings") {
            let mut settings = render_settings(&info);
            if flat {
                let mut flat_map = Map::new();
                flatten("", &settings, &mut flat_map);
                settings = Value::Object(flat_map);
            }
            entry.insert("settings".to_string(), settings);
        }
        out.insert(r.name, Value::Object(entry));
    }
    Ok((200, Value::Object(out)))
}

// ----- mappings -----

/// `GET /_mapping`.
pub(crate) async fn get_mapping_all(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
) -> Response {
    answer(&ctx, mappings(&gw, &ctx, &uri, IndexExpr::All).await)
}

/// `GET /{index}/_mapping`.
pub(crate) async fn get_mapping(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
) -> Response {
    answer(
        &ctx,
        mappings(&gw, &ctx, &uri, IndexExpr::parse(&index)).await,
    )
}

async fn mappings(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    expr: IndexExpr,
) -> Result<(u16, Value), EsError> {
    let params = Params::parse(uri.query(), uri.path(), GET_MAPPING_PARAMS)?;
    let opts = resolve_options(&params)?;
    let service = gw.service();
    let ns = ctx.namespace.as_str();
    let mut out = Map::new();
    for r in resolve(service, ns, &expr, opts).await? {
        match service.get_collection(ns, &r.name).await {
            Ok(info) => {
                out.insert(r.name, json!({"mappings": render_mappings(&info.schema)}));
            }
            Err(err) if is_missing(&err) => {}
            Err(err) => return Err(admin_error(err)),
        }
    }
    Ok((200, Value::Object(out)))
}

/// `PUT|POST /{index}/_mapping` (rule 3).
pub(crate) async fn put_mapping(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
    body: Bytes,
) -> Response {
    answer(&ctx, put_mapping_of(&gw, &ctx, &uri, &index, &body).await)
}

/// A failure while updating `index`, naming it.
fn on_index(error: EsError, index: &str) -> EsError {
    if error.extra.contains_key("index") {
        error
    } else {
        error.with("index", index)
    }
}

async fn put_mapping_of(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: &str,
    body: &[u8],
) -> Result<(u16, Value), EsError> {
    let params = Params::parse(uri.query(), uri.path(), PUT_MAPPING_PARAMS)?;
    let opts = resolve_options(&params)?;
    let write_index_only = params.bool("write_index_only")?.unwrap_or(false);
    let Some(mapping) = json_body(body)? else {
        return Err(EsError::body_required());
    };
    let service = gw.service();
    let ns = ctx.namespace.as_str();
    let resolved = resolve(service, ns, &IndexExpr::parse(index), opts).await?;
    let mut targets: Vec<String> = Vec::new();
    for r in resolved {
        match (&r.via_alias, write_index_only) {
            (Some(alias), true) => {
                if let Some(target) = resolve_write(service, ns, alias).await?
                    && target.name == r.name
                {
                    targets.push(r.name);
                }
            }
            _ => targets.push(r.name),
        }
    }
    // Plan every index first: a planning error changes nothing.
    let mut plans = Vec::with_capacity(targets.len());
    for name in &targets {
        let info = service
            .get_collection(ns, name)
            .await
            .map_err(admin_error)?;
        let plan = plan_put_mapping(&info.schema, &mapping).map_err(|e| on_index(e, name))?;
        plans.push((name.clone(), plan));
    }
    for (name, plan) in plans {
        if plan.fields.is_empty() && plan.vectors.is_empty() && plan.annotations.is_empty() {
            continue;
        }
        let first = service
            .add_fields(ns, &name, plan.fields, plan.vectors, plan.annotations)
            .await;
        match first {
            Ok(_) => {}
            Err(
                ServiceError::InvalidArgument(_)
                | ServiceError::SchemaViolation { .. }
                | ServiceError::AlreadyExists(_),
            ) => {
                // A concurrent schema change: re-read and re-plan once.
                let info = service
                    .get_collection(ns, &name)
                    .await
                    .map_err(admin_error)?;
                let plan =
                    plan_put_mapping(&info.schema, &mapping).map_err(|e| on_index(e, &name))?;
                service
                    .add_fields(ns, &name, plan.fields, plan.vectors, plan.annotations)
                    .await
                    .map_err(|err| match err {
                        ServiceError::AlreadyExists(what) => {
                            on_index(EsError::illegal_argument(format!("{what} exists")), &name)
                        }
                        other => on_index(admin_error(other), &name),
                    })?;
            }
            Err(err) => return Err(on_index(admin_error(err), &name)),
        }
    }
    Ok(ack())
}

// ----- aliases -----

/// An alias request applied to the aliases as read, before it is sent as
/// one `update_alias_targets` (rule 4).
struct AliasEdit {
    collections: BTreeSet<String>,
    /// Alias → member → `is_write_index`.
    state: BTreeMap<String, BTreeMap<String, Option<bool>>>,
    actions: Vec<AliasTargetAction>,
}

impl AliasEdit {
    async fn load(service: &CollectionService, ns: &str) -> Result<AliasEdit, EsError> {
        let collections = service
            .collection_records(ns)
            .await
            .map_err(admin_error)?
            .into_iter()
            .map(|c| c.name)
            .collect();
        let state = service
            .list_aliases(ns)
            .await
            .map_err(admin_error)?
            .into_iter()
            .map(|alias| {
                let members = alias
                    .members
                    .into_iter()
                    .map(|m| (m.collection, m.is_write_index))
                    .collect();
                (alias.alias, members)
            })
            .collect();
        Ok(AliasEdit {
            collections,
            state,
            actions: Vec::new(),
        })
    }

    /// The concrete indices of `items`: names of indices, or wildcards over
    /// them; an alias is refused, a missing name is 404.
    fn indices(&self, items: &[String]) -> Result<Vec<String>, EsError> {
        let mut out = Vec::new();
        for item in items {
            if item == "_all" || item.contains('*') {
                let pattern = if item == "_all" { "*" } else { item.as_str() };
                let matched: Vec<&String> = self
                    .collections
                    .iter()
                    .filter(|name| glob(pattern, name))
                    .collect();
                if matched.is_empty() {
                    return Err(EsError::index_not_found(item));
                }
                out.extend(matched.into_iter().cloned());
            } else if self.collections.contains(item) {
                out.push(item.clone());
            } else if self.state.contains_key(item) {
                return Err(matches_alias(item));
            } else {
                return Err(EsError::index_not_found(item));
            }
        }
        out.dedup();
        Ok(out)
    }

    fn add(
        &mut self,
        index: &str,
        alias: &str,
        is_write_index: Option<bool>,
    ) -> Result<(), EsError> {
        validate_alias_name(alias)?;
        if self.collections.contains(alias) {
            return Err(alias_clashes_with_index(alias));
        }
        self.state
            .entry(alias.to_string())
            .or_default()
            .insert(index.to_string(), is_write_index);
        self.actions.push(AliasTargetAction::Add {
            alias: alias.to_string(),
            collection: index.to_string(),
            is_write_index,
        });
        Ok(())
    }

    /// Removes `index` from every alias matching `pattern`; how many.
    fn remove_matching(&mut self, index: &str, pattern: &str) -> usize {
        let pattern = if pattern == "_all" { "*" } else { pattern };
        let matched: Vec<String> = self
            .state
            .iter()
            .filter(|(alias, members)| glob(pattern, alias) && members.contains_key(index))
            .map(|(alias, _)| alias.clone())
            .collect();
        for alias in &matched {
            if let Some(members) = self.state.get_mut(alias) {
                members.remove(index);
                if members.is_empty() {
                    self.state.remove(alias);
                }
            }
            self.actions.push(AliasTargetAction::Remove {
                alias: alias.clone(),
                collection: index.to_string(),
            });
        }
        matched.len()
    }

    /// Checks the result and sends it.
    async fn apply(self, service: &CollectionService, ns: &str) -> Result<(), EsError> {
        for (alias, members) in &self.state {
            let writers: Vec<&str> = members
                .iter()
                .filter(|(_, w)| **w == Some(true))
                .map(|(name, _)| name.as_str())
                .collect();
            if writers.len() > 1 {
                // ES 8.19 answers 500 (verify against the oracle, Task 11).
                return Err(EsError::new(
                    500,
                    "illegal_state_exception",
                    format!(
                        "alias [{alias}] has more than one write index [{}]",
                        writers.join(",")
                    ),
                ));
            }
        }
        if self.actions.is_empty() {
            return Ok(());
        }
        service
            .update_alias_targets(ns, self.actions)
            .await
            .map_err(alias_service_error)
    }
}

/// A catalog refusal of an alias change that the gateway accepted (a
/// concurrent change).
fn alias_service_error(error: ServiceError) -> EsError {
    match error {
        ServiceError::AlreadyExists(name) => alias_clashes_with_index(&name),
        other => admin_error(other),
    }
}

/// Strings of `key` (a string or an array of strings) of an alias action.
fn strings(action: &Map<String, Value>, key: &str) -> Result<Vec<String>, EsError> {
    match action.get(key) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(s)) => Ok(vec![s.clone()]),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| {
                item.as_str().map(str::to_string).ok_or_else(|| {
                    parse_exception(format!("[{key}] must hold strings, got [{item}]"))
                })
            })
            .collect(),
        Some(other) => Err(parse_exception(format!(
            "[{key}] must be a string or an array, got [{other}]"
        ))),
    }
}

/// `POST /_aliases`.
pub(crate) async fn update_aliases(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    body: Bytes,
) -> Response {
    answer(&ctx, aliases_request(&gw, &ctx, &uri, &body).await)
}

async fn aliases_request(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    body: &[u8],
) -> Result<(u16, Value), EsError> {
    Params::parse(uri.query(), uri.path(), ALIAS_WRITE_PARAMS)?;
    let body = json_body(body)?;
    let Some(Value::Object(body)) = body else {
        return Err(validation("no action specified"));
    };
    if let Some(key) = body.keys().find(|k| *k != "actions") {
        return Err(parse_exception(format!("[aliases] unknown field [{key}]")));
    }
    let actions = match body.get("actions") {
        Some(Value::Array(actions)) if !actions.is_empty() => actions,
        _ => return Err(validation("no action specified")),
    };
    let service = gw.service();
    let ns = ctx.namespace.as_str();
    let mut edit = AliasEdit::load(service, ns).await?;
    for action in actions {
        let Some((kind, inner)) = action
            .as_object()
            .filter(|map| map.len() == 1)
            .and_then(|map| map.iter().next())
        else {
            return Err(parse_exception(
                "an alias action must be an object with one of [add], [remove] or \
                 [remove_index]",
            ));
        };
        let add = match kind.as_str() {
            "add" => true,
            "remove" => false,
            "remove_index" => return Err(EsError::unsupported("remove_index")),
            other => return Err(parse_exception(format!("Unsupported action [{other}]"))),
        };
        let Value::Object(inner) = inner else {
            return Err(parse_exception(format!("[{kind}] must be an object")));
        };
        let mut is_write_index = None;
        for (key, value) in inner {
            match key.as_str() {
                "index" | "indices" | "alias" | "aliases" => {}
                "is_write_index" if add => {
                    is_write_index = match value {
                        Value::Null => None,
                        Value::Bool(b) => Some(*b),
                        other => {
                            return Err(parse_exception(format!(
                                "[is_write_index] must be a boolean, got [{other}]"
                            )));
                        }
                    }
                }
                k if PHASE_B_ALIAS_KEYS.contains(&k) => return Err(EsError::unsupported(k)),
                other => {
                    return Err(parse_exception(format!(
                        "[alias_action] unknown field [{other}]"
                    )));
                }
            }
        }
        let mut index_items = strings(inner, "index")?;
        index_items.extend(strings(inner, "indices")?);
        let mut alias_items = strings(inner, "alias")?;
        alias_items.extend(strings(inner, "aliases")?);
        if index_items.is_empty() {
            return Err(validation("One of [index] or [indices] is required"));
        }
        if alias_items.is_empty() {
            return Err(validation("One of [alias] or [aliases] is required"));
        }
        let indices = edit.indices(&index_items)?;
        if add {
            for index in &indices {
                for alias in &alias_items {
                    edit.add(index, alias, is_write_index)?;
                }
            }
        } else {
            for alias in &alias_items {
                let removed: usize = indices
                    .iter()
                    .map(|index| edit.remove_matching(index, alias))
                    .sum();
                if removed == 0 {
                    return Err(EsError::aliases_not_found(alias));
                }
            }
        }
    }
    edit.apply(service, ns).await?;
    Ok((200, json!({"acknowledged": true, "errors": false})))
}

/// `PUT|POST /{index}/_alias/{name}` (and `_aliases`).
pub(crate) async fn put_alias(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path((index, name)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let result = async {
        Params::parse(uri.query(), uri.path(), ALIAS_WRITE_PARAMS)?;
        let is_write_index = match json_body(&body)? {
            None => None,
            Some(Value::Object(map)) => alias_body(&map)?,
            Some(_) => return Err(parse_exception("request body must be an object")),
        };
        let service = gw.service();
        let ns = ctx.namespace.as_str();
        let mut edit = AliasEdit::load(service, ns).await?;
        let items: Vec<String> = index.split(',').map(str::to_string).collect();
        let indices = edit.indices(&items)?;
        for index in &indices {
            for alias in name.split(',').filter(|a| !a.is_empty()) {
                edit.add(index, alias, is_write_index)?;
            }
        }
        edit.apply(service, ns).await?;
        Ok(ack())
    };
    answer(&ctx, result.await)
}

/// `DELETE /{index}/_alias/{name}` (and `_aliases`).
pub(crate) async fn delete_alias(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path((index, name)): Path<(String, String)>,
) -> Response {
    let result = async {
        Params::parse(uri.query(), uri.path(), ALIAS_WRITE_PARAMS)?;
        let service = gw.service();
        let ns = ctx.namespace.as_str();
        let mut edit = AliasEdit::load(service, ns).await?;
        let items: Vec<String> = index.split(',').map(str::to_string).collect();
        let indices = edit.indices(&items)?;
        let mut removed = 0;
        for index in &indices {
            for pattern in name.split(',').filter(|a| !a.is_empty()) {
                removed += edit.remove_matching(index, pattern);
            }
        }
        if removed == 0 {
            return Err(EsError::aliases_not_found(&name));
        }
        edit.apply(service, ns).await?;
        Ok(ack())
    };
    answer(&ctx, result.await)
}

/// `GET /_alias` and `GET /_aliases` (and their `HEAD`).
pub(crate) async fn get_aliases_all(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
) -> Response {
    answer(&ctx, get_aliases(&gw, &ctx, &uri, None, None).await)
}

/// `GET /_alias/{name}`.
pub(crate) async fn get_aliases_named(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(name): Path<String>,
) -> Response {
    answer(&ctx, get_aliases(&gw, &ctx, &uri, None, Some(&name)).await)
}

/// `GET /{index}/_alias`.
pub(crate) async fn get_aliases_of_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
) -> Response {
    answer(&ctx, get_aliases(&gw, &ctx, &uri, Some(&index), None).await)
}

/// `GET /{index}/_alias/{name}`.
pub(crate) async fn get_aliases_of_index_named(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path((index, name)): Path<(String, String)>,
) -> Response {
    answer(
        &ctx,
        get_aliases(&gw, &ctx, &uri, Some(&index), Some(&name)).await,
    )
}

/// `{index: {"aliases": {alias: setting}}}`, one entry per (member, alias)
/// pair; without a name every index of the expression is listed, with no
/// aliases if it has none. A name that matches nothing is ES's 404.
async fn get_aliases(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    index: Option<&str>,
    names: Option<&str>,
) -> Result<(u16, Value), EsError> {
    let params = Params::parse(uri.query(), uri.path(), ALIAS_GET_PARAMS)?;
    let opts = resolve_options(&params)?;
    let service = gw.service();
    let ns = ctx.namespace.as_str();
    let expr = index.map_or(IndexExpr::All, IndexExpr::parse);
    let indices: BTreeSet<String> = resolve(service, ns, &expr, opts)
        .await?
        .into_iter()
        .map(|r| r.name)
        .collect();
    let patterns: Vec<&str> = match names {
        None => vec!["*"],
        Some(names) => names
            .split(',')
            .filter(|n| !n.is_empty())
            .map(|n| if n == "_all" { "*" } else { n })
            .collect(),
    };
    let mut out: BTreeMap<String, Map<String, Value>> = BTreeMap::new();
    if names.is_none() {
        for name in &indices {
            out.insert(name.clone(), Map::new());
        }
    }
    let mut matched: BTreeSet<&str> = BTreeSet::new();
    let aliases = service.list_aliases(ns).await.map_err(admin_error)?;
    for alias in &aliases {
        let Some(pattern) = patterns.iter().find(|p| glob(p, &alias.alias)) else {
            continue;
        };
        for member in alias
            .members
            .iter()
            .filter(|m| indices.contains(&m.collection))
        {
            matched.insert(pattern);
            out.entry(member.collection.clone())
                .or_default()
                .insert(alias.alias.clone(), alias_setting(member.is_write_index));
        }
    }
    let missing: Vec<&str> = patterns
        .iter()
        .filter(|p| !p.contains('*') && !matched.contains(*p))
        .copied()
        .collect();
    let mut body: Map<String, Value> = out
        .into_iter()
        .map(|(name, aliases)| (name, json!({"aliases": aliases})))
        .collect();
    if missing.is_empty() {
        return Ok((200, Value::Object(body)));
    }
    let reason = match missing.as_slice() {
        [one] => format!("alias [{one}] missing"),
        many => format!("aliases [{}] missing", many.join(",")),
    };
    if body.is_empty() {
        return Err(EsError::plain(404, reason));
    }
    body.insert("error".to_string(), json!(reason));
    body.insert("status".to_string(), json!(404));
    Ok((404, Value::Object(body)))
}

// ----- refresh -----

/// `GET|POST /_refresh`.
pub(crate) async fn refresh_all(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
) -> Response {
    answer(&ctx, refresh(&gw, &ctx, &uri, IndexExpr::All).await)
}

/// `GET|POST /{index}/_refresh`.
pub(crate) async fn refresh_index(
    State(gw): State<EsGateway>,
    ctx: RequestCtx,
    uri: Uri,
    Path(index): Path<String>,
) -> Response {
    answer(
        &ctx,
        refresh(&gw, &ctx, &uri, IndexExpr::parse(&index)).await,
    )
}

/// Resolves and writes nothing: reads are strong (R11, rule 5).
async fn refresh(
    gw: &EsGateway,
    ctx: &RequestCtx,
    uri: &Uri,
    expr: IndexExpr,
) -> Result<(u16, Value), EsError> {
    let params = Params::parse(uri.query(), uri.path(), REFRESH_PARAMS)?;
    let opts = resolve_options(&params)?;
    let n = resolve(gw.service(), &ctx.namespace, &expr, opts)
        .await?
        .len();
    Ok((
        200,
        json!({"_shards": {"total": n, "successful": n, "failed": 0}}),
    ))
}
