//! Point reads (Task 6): payload and vector selectors, retrieve, scroll and
//! count, the executors REST and gRPC share.

use std::collections::BTreeMap;

use loams_collection::{CollectionSchema, PrimaryKey, SparseVector};
use loams_query::{Projection, Query, ReadConsistency, SourceFilter, StoredDoc};
use serde_json::{Map, Value};

use crate::QdrantGateway;
use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::filter::compile_filter;
use crate::ids::{PointId, pk_predecessor, pk_to_json};
use crate::jsonpath::{JsonPath, select_exclude, select_include};
use crate::model::common::{
    NamedVectorOutput, PayloadSelector, Record, VectorOutput, WithPayload, WithVector,
};
use crate::model::filter::Filter;
use crate::model::points::{CountRequest, CountResult, PointRequest, ScrollRequest, ScrollResult};

/// What a read returns of each point, resolved against the schema.
#[derive(Clone, Debug, PartialEq)]
pub struct Selectors {
    pub payload: PayloadOut,
    /// Dense and sparse vector names.
    pub vectors: Vec<String>,
}

/// Which part of the payload a read returns.
#[derive(Clone, Debug, PartialEq)]
pub enum PayloadOut {
    None,
    All,
    Include(Vec<JsonPath>),
    Exclude(Vec<JsonPath>),
}

/// Parsed JsonPath keys.
fn paths(keys: &[String]) -> Result<Vec<JsonPath>, GatewayError> {
    keys.iter().map(|k| k.parse()).collect()
}

/// Step 1: `with_payload` (absent → `default_payload`) and `with_vector`
/// (`true` → every schema vector; a list of names as given, each known).
pub(crate) fn resolve_selectors(
    schema: &CollectionSchema,
    with_payload: Option<&WithPayload>,
    default_payload: bool,
    with_vector: Option<&WithVector>,
) -> Result<Selectors, GatewayError> {
    let payload = match with_payload {
        None if default_payload => PayloadOut::All,
        None => PayloadOut::None,
        Some(WithPayload::Bool(true)) => PayloadOut::All,
        Some(WithPayload::Bool(false)) => PayloadOut::None,
        Some(
            WithPayload::Include(keys)
            | WithPayload::Selector(PayloadSelector::Include { include: keys }),
        ) => PayloadOut::Include(paths(keys)?),
        Some(WithPayload::Selector(PayloadSelector::Exclude { exclude })) => {
            PayloadOut::Exclude(paths(exclude)?)
        }
    };
    let vectors = match with_vector {
        None | Some(WithVector::Bool(false)) => Vec::new(),
        Some(WithVector::Bool(true)) => schema
            .vectors
            .iter()
            .map(|s| s.name.clone())
            .chain(schema.sparse_vectors.iter().map(|s| s.name.clone()))
            .collect(),
        Some(WithVector::Names(names)) => {
            for name in names {
                let known = schema.vectors.iter().any(|s| &s.name == name)
                    || schema.sparse_vectors.iter().any(|s| &s.name == name);
                if !known {
                    return Err(GatewayError::BadRequest(format!(
                        "Not existing vector name error: {name}"
                    )));
                }
            }
            names.clone()
        }
    };
    Ok(Selectors { payload, vectors })
}

/// The whole source when any payload is returned (the selectors run in the
/// gateway, with Qdrant's JsonPath semantics), and the selected vectors.
pub(crate) fn projection(sel: &Selectors) -> Projection {
    Projection {
        source: if sel.payload == PayloadOut::None {
            SourceFilter::None
        } else {
            SourceFilter::All
        },
        vectors: sel.vectors.clone(),
        fields: Vec::new(),
    }
}

/// The payload a point returns; `None` when no payload is selected.
pub(crate) fn render_payload(
    sel: &Selectors,
    source: Option<Map<String, Value>>,
) -> Option<Map<String, Value>> {
    match &sel.payload {
        PayloadOut::None => None,
        PayloadOut::All => Some(source.unwrap_or_default()),
        PayloadOut::Include(paths) => Some(select_include(&source.unwrap_or_default(), paths)),
        PayloadOut::Exclude(paths) => Some(select_exclude(&source.unwrap_or_default(), paths)),
    }
}

/// The vectors a point returns: a bare list iff exactly `[""]` is selected
/// (and present), else a map of the selected vectors the point has.
pub(crate) fn render_vectors(
    sel: &Selectors,
    mut vectors: BTreeMap<String, Vec<f32>>,
    mut sparse: BTreeMap<String, SparseVector>,
) -> Option<VectorOutput> {
    if sel.vectors.is_empty() {
        return None;
    }
    if let [only] = sel.vectors.as_slice()
        && only.is_empty()
        && let Some(v) = vectors.remove("")
    {
        return Some(VectorOutput::Single(v));
    }
    let mut out = BTreeMap::new();
    for name in &sel.vectors {
        if let Some(v) = vectors.remove(name) {
            out.insert(name.clone(), NamedVectorOutput::Dense(v));
        } else if let Some(v) = sparse.remove(name) {
            out.insert(name.clone(), NamedVectorOutput::from(&v));
        }
    }
    Some(VectorOutput::Named(out))
}

/// A stored document as Qdrant's `Record`.
pub(crate) fn record(sel: &Selectors, doc: StoredDoc) -> Record {
    Record {
        id: pk_to_json(&doc.pk),
        payload: render_payload(sel, doc.source),
        vector: render_vectors(sel, doc.vectors, doc.sparse_vectors),
    }
}

/// A request's filter compiled against the collection's schema; `None`
/// without one.
pub(crate) async fn compiled(
    gw: &QdrantGateway,
    ctx: &RequestCtx,
    collection: &str,
    filter: Option<&Filter>,
) -> Result<Option<Query>, GatewayError> {
    let Some(filter) = filter else {
        return Ok(None);
    };
    let info = gw.service().get_collection(&ctx.ns, collection).await?;
    compile_filter(filter, &info.schema).map(Some)
}

/// Step 4: the number of points in the collection that match the filter;
/// always exact.
pub(crate) async fn count(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    request: CountRequest,
) -> Result<CountResult, GatewayError> {
    let query = compiled(&gw, &ctx, &collection, request.filter.as_ref()).await?;
    let (count, _) = gw
        .service()
        .count_with_token(&ctx.ns, &collection, query, ctx.consistency.clone())
        .await?;
    Ok(CountResult { count })
}

/// Step 2: the found points in request order, each id once (the first
/// occurrence), missing ids skipped.
pub(crate) async fn retrieve(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    request: PointRequest,
) -> Result<Vec<Record>, GatewayError> {
    let max = gw.retrieve_id_limit();
    retrieve_bounded(gw, ctx, collection, request, max).await
}

async fn retrieve_bounded(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    request: PointRequest,
    max: usize,
) -> Result<Vec<Record>, GatewayError> {
    let len = request.ids.len();
    crate::check_request_len("The id list", len, max)?;
    let info = gw.service().get_collection(&ctx.ns, &collection).await?;
    let sel = resolve_selectors(
        &info.schema,
        request.with_payload.as_ref(),
        true,
        request.with_vector.as_ref(),
    )?;
    let mut pks: Vec<PrimaryKey> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for id in &request.ids {
        let pk = PointId::from_json(id)?.to_pk();
        if seen.insert(pk.clone()) {
            pks.push(pk);
        }
    }
    let docs = gw
        .service()
        .get(
            &ctx.ns,
            &info.name,
            &pks,
            &projection(&sel),
            ctx.consistency.clone(),
        )
        .await?;
    Ok(docs
        .into_iter()
        .flatten()
        .map(|d| record(&sel, d))
        .collect())
}

/// `GET /points/{id}`: payload and every vector
/// (`qdrant:src/actix/api/retrieve_api.rs:55-56`); a missing point is
/// `PointNotFound`.
pub(crate) async fn get_point(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    id: PointId,
) -> Result<Record, GatewayError> {
    let json = pk_to_json(&id.to_pk());
    let shown = match &json {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let request = PointRequest {
        ids: vec![json],
        with_payload: Some(WithPayload::Bool(true)),
        with_vector: Some(WithVector::Bool(true)),
    };
    retrieve_bounded(gw, ctx, collection, request, 1)
        .await?
        .pop()
        .ok_or(GatewayError::PointNotFound(shown))
}

/// Step 3: one page from the inclusive `offset`, and the id the next page
/// starts at (`null` on the last page). A `limit` over the service's
/// `max_scroll_limit` is read in consecutive pages; one over the search
/// window (`max_window`) is refused.
pub(crate) async fn scroll(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    request: ScrollRequest,
) -> Result<ScrollResult, GatewayError> {
    if request.order_by.is_some() {
        return Err(GatewayError::Unsupported("order_by".to_string()));
    }
    let limit = request.limit.unwrap_or(10);
    if limit == 0 {
        return Err(GatewayError::BadRequest(
            "limit must be at least 1".to_string(),
        ));
    }
    // The pages below are buffered, so the total is bounded like a
    // query's window (PR #50 review).
    let max_window = gw.service().config().search.limits.max_window;
    if limit > max_window {
        return Err(GatewayError::BadRequest(format!(
            "limit must be at most {max_window}"
        )));
    }
    let info = gw.service().get_collection(&ctx.ns, &collection).await?;
    let sel = resolve_selectors(
        &info.schema,
        request.with_payload.as_ref(),
        true,
        request.with_vector.as_ref(),
    )?;
    let query = request
        .filter
        .as_ref()
        .map(|f| compile_filter(f, &info.schema))
        .transpose()?;
    let mut after = match &request.offset {
        None => None,
        Some(offset) => pk_predecessor(&PointId::from_json(offset)?.to_pk()),
    };
    let select = projection(&sel);
    let max_page = gw.service().config().max_scroll_limit.max(1);
    let want = limit.saturating_add(1);
    let mut consistency = ctx.consistency.clone();
    let mut first = true;
    let mut docs: Vec<StoredDoc> = Vec::new();
    loop {
        let page = (want - docs.len()).min(max_page);
        let ((got, next), token) = gw
            .service()
            .scroll_with_token(
                &ctx.ns,
                &info.name,
                query.clone(),
                after.take(),
                page,
                &select,
                consistency.clone(),
            )
            .await?;
        if first {
            consistency = ReadConsistency::AtLeast(token);
            first = false;
        }
        docs.extend(got);
        match next {
            Some(key) if docs.len() < want => after = Some(key),
            _ => break,
        }
    }
    let next_page_offset = if docs.len() > limit {
        docs.truncate(want);
        docs.pop().map(|d| pk_to_json(&d.pk))
    } else {
        None
    };
    Ok(ScrollResult {
        points: docs.into_iter().map(|d| record(&sel, d)).collect(),
        next_page_offset,
    })
}
