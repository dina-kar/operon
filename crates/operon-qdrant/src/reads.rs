//! Point reads: the executors REST and gRPC share.

use operon_query::Query;

use crate::QdrantGateway;
use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::filter::compile_filter;
use crate::model::filter::Filter;
use crate::model::points::{CountRequest, CountResult};

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

/// The number of points in the collection that match the filter; always
/// exact.
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
