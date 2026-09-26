//! Collections: the executors REST and gRPC share.

use crate::QdrantGateway;
use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::model::collections::{CollectionDescription, CollectionsResponse};

/// The collections of the request's namespace, by name (aliases are not
/// listed).
pub(crate) async fn list_collections(
    gw: QdrantGateway,
    ctx: RequestCtx,
) -> Result<CollectionsResponse, GatewayError> {
    let infos = gw.service().list_collections(&ctx.ns).await?;
    Ok(CollectionsResponse {
        collections: infos
            .into_iter()
            .map(|info| CollectionDescription { name: info.name })
            .collect(),
    })
}
