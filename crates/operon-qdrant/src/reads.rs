//! Point reads: the executors REST and gRPC share.

use crate::QdrantGateway;
use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::model::points::{CountRequest, CountResult};

/// The number of points in the collection; always exact.
pub(crate) async fn count(
    gw: QdrantGateway,
    ctx: RequestCtx,
    collection: String,
    request: CountRequest,
) -> Result<CountResult, GatewayError> {
    if request.filter.as_ref().is_some_and(|f| !f.is_null()) {
        return Err(GatewayError::Unsupported(
            "filters (plan M1.4 Task 4)".to_string(),
        ));
    }
    let (count, _) = gw
        .service()
        .count_with_token(&ctx.ns, &collection, None, ctx.consistency.clone())
        .await?;
    Ok(CountResult { count })
}
