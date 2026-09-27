//! The gRPC listener: tonic services for `Qdrant`, `Collections`, `Points`,
//! `Snapshots` and `grpc.health.v1.Health` ("Qdrant protocol facts", gRPC
//! methods). Methods no task serves yet answer `UNIMPLEMENTED`.

use axum::extract::Request as HttpRequest;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response as HttpResponse};
use operon_collection::ConsistencyToken;
use operon_query::hot::{HOT_HEADER, HotLayer, parse_hot_header};
use serde_json::{Map, Value};
use tonic::codec::CompressionEncoding;
use tonic::service::Routes;
use tonic::{Request, Response, Status};

use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::model::points::CountRequest;
use crate::proto::health as hpb;
use crate::proto::qdrant as pb;
use crate::{QDRANT_TITLE, QdrantGateway, TOKEN_HEADER, reads, schema};

/// Every gRPC service, gzip on both ways and messages of up to
/// `max_request_bytes`, inside `HotLayer` and the gateway's own
/// `operon-hot` check (step 5a).
pub(crate) fn routes(gw: &QdrantGateway) -> Routes {
    let max = gw.config().max_request_bytes;
    let svc = GrpcService { gw: gw.clone() };
    macro_rules! server {
        ($server:expr) => {
            $server
                .accept_compressed(CompressionEncoding::Gzip)
                .send_compressed(CompressionEncoding::Gzip)
                .max_decoding_message_size(max)
        };
    }
    let router = Routes::new(server!(pb::qdrant_server::QdrantServer::new(svc.clone())))
        .add_service(server!(pb::collections_server::CollectionsServer::new(
            svc.clone()
        )))
        .add_service(server!(pb::points_server::PointsServer::new(svc.clone())))
        .add_service(server!(pb::snapshots_server::SnapshotsServer::new(
            svc.clone()
        )))
        .add_service(server!(hpb::health_server::HealthServer::new(svc)))
        .into_axum_router()
        .layer(HotLayer::new(gw.service().config().hot_default))
        .layer(middleware::from_fn(check_hot_metadata));
    Routes::from(router)
}

/// Answers an invalid `operon-hot` with Qdrant's `INVALID_ARGUMENT`, before
/// `HotLayer` sees it (step 5a).
async fn check_hot_metadata(request: HttpRequest, next: Next) -> HttpResponse {
    if let Some(value) = request.headers().get(HOT_HEADER)
        && let Err(err) = parse_hot_header(&String::from_utf8_lossy(value.as_bytes()))
    {
        return GatewayError::from(err)
            .grpc_status()
            .into_http::<axum::body::Body>()
            .into_response();
    }
    next.run(request).await
}

/// Adds a write's `operon-consistency-token` response metadata.
#[allow(dead_code)] // The write methods arrive with Task 5.
pub(crate) fn with_token<T>(mut response: Response<T>, token: &ConsistencyToken) -> Response<T> {
    if let Ok(value) = token.to_string().parse() {
        response.metadata_mut().insert(TOKEN_HEADER, value);
    }
    response
}

/// The one value behind every service.
#[derive(Clone, Debug)]
struct GrpcService {
    gw: QdrantGateway,
}

impl GrpcService {
    fn ctx(
        &self,
        meta: &tonic::metadata::MetadataMap,
        timeout: Option<u64>,
    ) -> Result<RequestCtx, Status> {
        RequestCtx::from_grpc(meta, timeout, self.gw.config()).map_err(|e| e.grpc_status())
    }
}

fn unsupported(method: &str) -> Status {
    GatewayError::Unsupported(format!("gRPC {method}")).grpc_status()
}

/// Emits a whole service impl (so `async_trait` sees every method): the
/// given methods, plus one `UNIMPLEMENTED` method per `unsupported` entry.
macro_rules! service {
    (
        impl $trait:ident for GrpcService as $label:literal { $($items:tt)* }
        unsupported { $($name:ident($req:ident) -> $resp:ident;)* }
    ) => {
        #[tonic::async_trait]
        impl $trait for GrpcService {
            $($items)*
            $(
                async fn $name(
                    &self,
                    _request: Request<pb::$req>,
                ) -> Result<Response<pb::$resp>, Status> {
                    Err(unsupported(concat!($label, "/", stringify!($name))))
                }
            )*
        }
    };
}

use hpb::health_server::Health;
use pb::collections_server::Collections;
use pb::points_server::Points;
use pb::qdrant_server::Qdrant;
use pb::snapshots_server::Snapshots;

#[tonic::async_trait]
impl Qdrant for GrpcService {
    /// Equals REST `GET /`, with no `commit`.
    async fn health_check(
        &self,
        _request: Request<pb::HealthCheckRequest>,
    ) -> Result<Response<pb::HealthCheckReply>, Status> {
        Ok(Response::new(pb::HealthCheckReply {
            title: QDRANT_TITLE.to_string(),
            version: self.gw.config().reported_version.clone(),
            commit: None,
        }))
    }
}

#[tonic::async_trait]
impl Health for GrpcService {
    /// `SERVING` for any service name.
    async fn check(
        &self,
        _request: Request<hpb::HealthCheckRequest>,
    ) -> Result<Response<hpb::HealthCheckResponse>, Status> {
        Ok(Response::new(hpb::HealthCheckResponse {
            status: hpb::health_check_response::ServingStatus::Serving as i32,
        }))
    }
}

service! {
    impl Collections for GrpcService as "Collections" {
        async fn list(
            &self,
            request: Request<pb::ListCollectionsRequest>,
        ) -> Result<Response<pb::ListCollectionsResponse>, Status> {
            let ctx = self.ctx(request.metadata(), None)?;
            let listed = ctx
                .run(schema::list_collections(self.gw.clone(), ctx.clone()))
                .await
                .map_err(|e| e.grpc_status())?;
            Ok(Response::new(pb::ListCollectionsResponse {
                collections: listed
                    .collections
                    .into_iter()
                    .map(|c| pb::CollectionDescription { name: c.name })
                    .collect(),
                time: ctx.elapsed_secs(),
            }))
        }
    }
    unsupported {
        get(GetCollectionInfoRequest) -> GetCollectionInfoResponse;
        create(CreateCollection) -> CollectionOperationResponse;
        update(UpdateCollection) -> CollectionOperationResponse;
        delete(DeleteCollection) -> CollectionOperationResponse;
        update_aliases(ChangeAliases) -> CollectionOperationResponse;
        list_collection_aliases(ListCollectionAliasesRequest) -> ListAliasesResponse;
        list_aliases(ListAliasesRequest) -> ListAliasesResponse;
        collection_cluster_info(CollectionClusterInfoRequest) -> CollectionClusterInfoResponse;
        collection_exists(CollectionExistsRequest) -> CollectionExistsResponse;
        update_collection_cluster_setup(UpdateCollectionClusterSetupRequest) -> UpdateCollectionClusterSetupResponse;
        create_shard_key(CreateShardKeyRequest) -> CreateShardKeyResponse;
        delete_shard_key(DeleteShardKeyRequest) -> DeleteShardKeyResponse;
        list_shard_keys(ListShardKeysRequest) -> ListShardKeysResponse;
    }
}

service! {
    impl Points for GrpcService as "Points" {
        async fn count(
            &self,
            request: Request<pb::CountPoints>,
        ) -> Result<Response<pb::CountResponse>, Status> {
            let ctx = self.ctx(request.metadata(), request.get_ref().timeout)?;
            let request = request.into_inner();
            // Filters are converted by Task 4; until then a present one is
            // refused by the executor.
            let count = CountRequest {
                filter: request.filter.map(|_| Value::Object(Map::new())),
                exact: request.exact,
            };
            let result = ctx
                .run(reads::count(
                    self.gw.clone(),
                    ctx.clone(),
                    request.collection_name,
                    count,
                ))
                .await
                .map_err(|e| e.grpc_status())?;
            Ok(Response::new(pb::CountResponse {
                result: Some(pb::CountResult {
                    count: result.count,
                }),
                time: ctx.elapsed_secs(),
                usage: None,
            }))
        }
    }
    unsupported {
        upsert(UpsertPoints) -> PointsOperationResponse;
        delete(DeletePoints) -> PointsOperationResponse;
        get(GetPoints) -> GetResponse;
        update_vectors(UpdatePointVectors) -> PointsOperationResponse;
        delete_vectors(DeletePointVectors) -> PointsOperationResponse;
        set_payload(SetPayloadPoints) -> PointsOperationResponse;
        overwrite_payload(SetPayloadPoints) -> PointsOperationResponse;
        delete_payload(DeletePayloadPoints) -> PointsOperationResponse;
        clear_payload(ClearPayloadPoints) -> PointsOperationResponse;
        create_field_index(CreateFieldIndexCollection) -> PointsOperationResponse;
        delete_field_index(DeleteFieldIndexCollection) -> PointsOperationResponse;
        create_vector_name(CreateVectorNameRequest) -> PointsOperationResponse;
        delete_vector_name(DeleteVectorNameRequest) -> PointsOperationResponse;
        search(SearchPoints) -> SearchResponse;
        search_batch(SearchBatchPoints) -> SearchBatchResponse;
        search_groups(SearchPointGroups) -> SearchGroupsResponse;
        scroll(ScrollPoints) -> ScrollResponse;
        recommend(RecommendPoints) -> RecommendResponse;
        recommend_batch(RecommendBatchPoints) -> RecommendBatchResponse;
        recommend_groups(RecommendPointGroups) -> RecommendGroupsResponse;
        discover(DiscoverPoints) -> DiscoverResponse;
        discover_batch(DiscoverBatchPoints) -> DiscoverBatchResponse;
        update_batch(UpdateBatchPoints) -> UpdateBatchResponse;
        query(QueryPoints) -> QueryResponse;
        query_batch(QueryBatchPoints) -> QueryBatchResponse;
        query_groups(QueryPointGroups) -> QueryGroupsResponse;
        facet(FacetCounts) -> FacetResponse;
        search_matrix_pairs(SearchMatrixPoints) -> SearchMatrixPairsResponse;
        search_matrix_offsets(SearchMatrixPoints) -> SearchMatrixOffsetsResponse;
    }
}

service! {
    impl Snapshots for GrpcService as "Snapshots" {}
    unsupported {
        create(CreateSnapshotRequest) -> CreateSnapshotResponse;
        list(ListSnapshotsRequest) -> ListSnapshotsResponse;
        delete(DeleteSnapshotRequest) -> DeleteSnapshotResponse;
        create_full(CreateFullSnapshotRequest) -> CreateSnapshotResponse;
        list_full(ListFullSnapshotsRequest) -> ListSnapshotsResponse;
        delete_full(DeleteFullSnapshotRequest) -> DeleteSnapshotResponse;
    }
}
