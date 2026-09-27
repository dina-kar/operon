//! The gRPC listener: tonic services for `Qdrant`, `Collections`, `Points`,
//! `Snapshots` and `grpc.health.v1.Health` ("Qdrant protocol facts", gRPC
//! methods). Methods no task serves yet answer `UNIMPLEMENTED`.

use axum::extract::Request as HttpRequest;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response as HttpResponse};
use operon_collection::ConsistencyToken;
use operon_query::hot::{HOT_HEADER, HotLayer, parse_hot_header};
use tonic::codec::CompressionEncoding;
use tonic::service::Routes;
use tonic::{Request, Response, Status};

use crate::convert::collections as conv;
use crate::convert::common::{with_payload_from_grpc, with_vector_from_grpc};
use crate::convert::filter::{field_index_from_grpc, filter_from_grpc};
use crate::convert::points as pconv;
use crate::ctx::RequestCtx;
use crate::error::GatewayError;
use crate::model::common::UpdateResult;
use crate::model::points::{CountRequest, PointRequest, ScrollRequest, UpdateOperation};
use crate::proto::health as hpb;
use crate::proto::qdrant as pb;
use crate::schema::NewVector;
use crate::{QDRANT_TITLE, QdrantGateway, TOKEN_HEADER, reads, schema, snapshots, writes};

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
    /// Builds the context and runs `op` under its timeout; the result with
    /// the context (for `time`).
    async fn run<T, F>(
        &self,
        meta: &tonic::metadata::MetadataMap,
        timeout: Option<u64>,
        op: impl FnOnce(QdrantGateway, RequestCtx) -> F,
    ) -> Result<(T, RequestCtx), Status>
    where
        F: Future<Output = Result<T, GatewayError>>,
    {
        let ctx = self.ctx(meta, timeout)?;
        let result = ctx
            .run(op(self.gw.clone(), ctx.clone()))
            .await
            .map_err(|e| e.grpc_status())?;
        Ok((result, ctx))
    }

    fn ctx(
        &self,
        meta: &tonic::metadata::MetadataMap,
        timeout: Option<u64>,
    ) -> Result<RequestCtx, Status> {
        RequestCtx::from_grpc(meta, timeout, self.gw.config()).map_err(|e| e.grpc_status())
    }

    /// A write method of one operation (Task 5 step 8): `wait` defaults to
    /// `false`; the answer carries the consistency token.
    async fn write_one(
        &self,
        meta: &tonic::metadata::MetadataMap,
        timeout: Option<u64>,
        collection: String,
        wait: Option<bool>,
        op: Result<UpdateOperation, GatewayError>,
    ) -> Result<Response<pb::PointsOperationResponse>, Status> {
        let op = op.map_err(|e| e.grpc_status())?;
        let wait = wait.unwrap_or(false);
        let ((result, token), ctx) = self
            .run(meta, timeout, |gw, ctx| {
                writes::update_one(gw, ctx, collection, op, wait)
            })
            .await?;
        Ok(with_token(
            Response::new(pb::PointsOperationResponse {
                result: Some(pconv::update_result_to_grpc(&result)),
                time: ctx.elapsed_secs(),
                usage: None,
            }),
            &token,
        ))
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

        async fn get(
            &self,
            request: Request<pb::GetCollectionInfoRequest>,
        ) -> Result<Response<pb::GetCollectionInfoResponse>, Status> {
            let name = request.get_ref().collection_name.clone();
            let (info, ctx) = self
                .run(request.metadata(), None, |gw, ctx| {
                    schema::collection_info(gw, ctx, name)
                })
                .await?;
            Ok(Response::new(pb::GetCollectionInfoResponse {
                result: Some(conv::info_to_grpc(&info)),
                time: ctx.elapsed_secs(),
            }))
        }

        async fn create(
            &self,
            request: Request<pb::CreateCollection>,
        ) -> Result<Response<pb::CollectionOperationResponse>, Status> {
            let body = conv::create_to_json(request.get_ref());
            let name = request.get_ref().collection_name.clone();
            let (result, ctx) = self
                .run(request.metadata(), request.get_ref().timeout, |gw, ctx| {
                    schema::create_collection(gw, ctx, name, body)
                })
                .await?;
            Ok(operation(result, &ctx))
        }

        async fn update(
            &self,
            request: Request<pb::UpdateCollection>,
        ) -> Result<Response<pb::CollectionOperationResponse>, Status> {
            let body = conv::update_to_json(request.get_ref());
            let name = request.get_ref().collection_name.clone();
            let (result, ctx) = self
                .run(request.metadata(), request.get_ref().timeout, |gw, ctx| {
                    schema::update_collection(gw, ctx, name, body)
                })
                .await?;
            Ok(operation(result, &ctx))
        }

        async fn delete(
            &self,
            request: Request<pb::DeleteCollection>,
        ) -> Result<Response<pb::CollectionOperationResponse>, Status> {
            let name = request.get_ref().collection_name.clone();
            let (result, ctx) = self
                .run(request.metadata(), request.get_ref().timeout, |gw, ctx| {
                    schema::delete_collection(gw, ctx, name)
                })
                .await?;
            Ok(operation(result, &ctx))
        }

        async fn update_aliases(
            &self,
            request: Request<pb::ChangeAliases>,
        ) -> Result<Response<pb::CollectionOperationResponse>, Status> {
            let ops = conv::alias_ops_from_grpc(&request.get_ref().actions)
                .map_err(|e| e.grpc_status())?;
            let (result, ctx) = self
                .run(request.metadata(), request.get_ref().timeout, |gw, ctx| {
                    schema::update_aliases(gw, ctx, ops)
                })
                .await?;
            Ok(operation(result, &ctx))
        }

        async fn list_collection_aliases(
            &self,
            request: Request<pb::ListCollectionAliasesRequest>,
        ) -> Result<Response<pb::ListAliasesResponse>, Status> {
            let name = request.get_ref().collection_name.clone();
            let (aliases, ctx) = self
                .run(request.metadata(), None, |gw, ctx| {
                    schema::collection_aliases(gw, ctx, name)
                })
                .await?;
            Ok(Response::new(pb::ListAliasesResponse {
                aliases: conv::aliases_to_grpc(aliases),
                time: ctx.elapsed_secs(),
            }))
        }

        async fn list_aliases(
            &self,
            request: Request<pb::ListAliasesRequest>,
        ) -> Result<Response<pb::ListAliasesResponse>, Status> {
            let (aliases, ctx) = self
                .run(request.metadata(), None, schema::list_aliases)
                .await?;
            Ok(Response::new(pb::ListAliasesResponse {
                aliases: conv::aliases_to_grpc(aliases),
                time: ctx.elapsed_secs(),
            }))
        }

        async fn collection_cluster_info(
            &self,
            request: Request<pb::CollectionClusterInfoRequest>,
        ) -> Result<Response<pb::CollectionClusterInfoResponse>, Status> {
            let name = request.get_ref().collection_name.clone();
            let (points, ctx) = self
                .run(request.metadata(), None, |gw, ctx| {
                    schema::cluster_points(gw, ctx, name)
                })
                .await?;
            Ok(Response::new(conv::cluster_to_grpc(points, ctx.elapsed_secs())))
        }

        async fn collection_exists(
            &self,
            request: Request<pb::CollectionExistsRequest>,
        ) -> Result<Response<pb::CollectionExistsResponse>, Status> {
            let name = request.get_ref().collection_name.clone();
            let (result, ctx) = self
                .run(request.metadata(), None, |gw, ctx| {
                    schema::collection_exists(gw, ctx, name)
                })
                .await?;
            Ok(Response::new(pb::CollectionExistsResponse {
                result: Some(pb::CollectionExists {
                    exists: result.exists,
                }),
                time: ctx.elapsed_secs(),
            }))
        }
    }
    unsupported {
        update_collection_cluster_setup(UpdateCollectionClusterSetupRequest) -> UpdateCollectionClusterSetupResponse;
        create_shard_key(CreateShardKeyRequest) -> CreateShardKeyResponse;
        delete_shard_key(DeleteShardKeyRequest) -> DeleteShardKeyResponse;
        list_shard_keys(ListShardKeysRequest) -> ListShardKeysResponse;
    }
}

service! {
    impl Points for GrpcService as "Points" {
        async fn upsert(
            &self,
            request: Request<pb::UpsertPoints>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let r = request.get_ref();
            let op = pconv::upsert_from_grpc(r);
            self.write_one(request.metadata(), r.timeout, r.collection_name.clone(), r.wait, op)
                .await
        }

        async fn delete(
            &self,
            request: Request<pb::DeletePoints>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let r = request.get_ref();
            let op = pconv::delete_from_grpc(r);
            self.write_one(request.metadata(), r.timeout, r.collection_name.clone(), r.wait, op)
                .await
        }

        async fn set_payload(
            &self,
            request: Request<pb::SetPayloadPoints>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let r = request.get_ref();
            let op = pconv::set_payload_from_grpc(r, false);
            self.write_one(request.metadata(), r.timeout, r.collection_name.clone(), r.wait, op)
                .await
        }

        async fn overwrite_payload(
            &self,
            request: Request<pb::SetPayloadPoints>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let r = request.get_ref();
            let op = pconv::set_payload_from_grpc(r, true);
            self.write_one(request.metadata(), r.timeout, r.collection_name.clone(), r.wait, op)
                .await
        }

        async fn delete_payload(
            &self,
            request: Request<pb::DeletePayloadPoints>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let r = request.get_ref();
            let op = pconv::delete_payload_from_grpc(r);
            self.write_one(request.metadata(), r.timeout, r.collection_name.clone(), r.wait, op)
                .await
        }

        async fn clear_payload(
            &self,
            request: Request<pb::ClearPayloadPoints>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let r = request.get_ref();
            let op = pconv::clear_payload_from_grpc(r);
            self.write_one(request.metadata(), r.timeout, r.collection_name.clone(), r.wait, op)
                .await
        }

        async fn update_vectors(
            &self,
            request: Request<pb::UpdatePointVectors>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let r = request.get_ref();
            let op = pconv::update_vectors_from_grpc(r);
            self.write_one(request.metadata(), r.timeout, r.collection_name.clone(), r.wait, op)
                .await
        }

        async fn delete_vectors(
            &self,
            request: Request<pb::DeletePointVectors>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let r = request.get_ref();
            let op = pconv::delete_vectors_from_grpc(r);
            self.write_one(request.metadata(), r.timeout, r.collection_name.clone(), r.wait, op)
                .await
        }

        async fn update_batch(
            &self,
            request: Request<pb::UpdateBatchPoints>,
        ) -> Result<Response<pb::UpdateBatchResponse>, Status> {
            let r = request.get_ref();
            let ops = r
                .operations
                .iter()
                .map(pconv::batch_operation_from_grpc)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.grpc_status())?;
            let (collection, wait) = (r.collection_name.clone(), r.wait.unwrap_or(false));
            let ((results, token), ctx): ((Vec<UpdateResult>, _), _) = self
                .run(request.metadata(), r.timeout, |gw, ctx| {
                    writes::update(gw, ctx, collection, ops, wait)
                })
                .await?;
            Ok(with_token(
                Response::new(pb::UpdateBatchResponse {
                    result: results.iter().map(pconv::update_result_to_grpc).collect(),
                    time: ctx.elapsed_secs(),
                    usage: None,
                }),
                &token,
            ))
        }

        async fn get(
            &self,
            request: Request<pb::GetPoints>,
        ) -> Result<Response<pb::GetResponse>, Status> {
            let r = request.get_ref();
            let ids = r
                .ids
                .iter()
                .map(|id| pconv::id_from_grpc(Some(id)))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| e.grpc_status())?;
            let point_request = PointRequest {
                ids,
                with_payload: Some(with_payload_from_grpc(r.with_payload.as_ref(), true)),
                with_vector: Some(with_vector_from_grpc(r.with_vectors.as_ref(), false)),
            };
            let collection = r.collection_name.clone();
            let (records, ctx) = self
                .run(request.metadata(), r.timeout, |gw, ctx| {
                    reads::retrieve(gw, ctx, collection, point_request)
                })
                .await?;
            Ok(Response::new(pb::GetResponse {
                result: records.iter().map(pconv::record_to_grpc).collect(),
                time: ctx.elapsed_secs(),
                usage: None,
            }))
        }

        async fn scroll(
            &self,
            request: Request<pb::ScrollPoints>,
        ) -> Result<Response<pb::ScrollResponse>, Status> {
            let r = request.get_ref();
            let parsed = (|| {
                Ok::<_, GatewayError>(ScrollRequest {
                    offset: r
                        .offset
                        .as_ref()
                        .map(|id| pconv::id_from_grpc(Some(id)))
                        .transpose()?,
                    limit: r.limit.map(|l| l as usize),
                    filter: r.filter.as_ref().map(filter_from_grpc).transpose()?,
                    with_payload: Some(with_payload_from_grpc(r.with_payload.as_ref(), true)),
                    with_vector: Some(with_vector_from_grpc(r.with_vectors.as_ref(), false)),
                    order_by: r.order_by.as_ref().map(|_| serde_json::Value::Bool(true)),
                })
            })()
            .map_err(|e| e.grpc_status())?;
            let collection = r.collection_name.clone();
            let (page, ctx) = self
                .run(request.metadata(), r.timeout, |gw, ctx| {
                    reads::scroll(gw, ctx, collection, parsed)
                })
                .await?;
            Ok(Response::new(pb::ScrollResponse {
                next_page_offset: page.next_page_offset.as_ref().map(pconv::id_to_grpc),
                result: page.points.iter().map(pconv::record_to_grpc).collect(),
                time: ctx.elapsed_secs(),
                usage: None,
            }))
        }

        async fn count(
            &self,
            request: Request<pb::CountPoints>,
        ) -> Result<Response<pb::CountResponse>, Status> {
            let ctx = self.ctx(request.metadata(), request.get_ref().timeout)?;
            let request = request.into_inner();
            let count = CountRequest {
                filter: request
                    .filter
                    .as_ref()
                    .map(filter_from_grpc)
                    .transpose()
                    .map_err(|e| e.grpc_status())?,
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

        async fn create_field_index(
            &self,
            request: Request<pb::CreateFieldIndexCollection>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let req = request.get_ref();
            let body = field_index_from_grpc(req);
            let (collection, wait) = (req.collection_name.clone(), req.wait.unwrap_or(false));
            let (result, ctx) = self
                .run(request.metadata(), req.timeout, |gw, ctx| {
                    schema::create_field_index(gw, ctx, collection, body, wait)
                })
                .await?;
            let status = match result.status {
                crate::model::common::UpdateStatus::Completed => pb::UpdateStatus::Completed,
                crate::model::common::UpdateStatus::Acknowledged => pb::UpdateStatus::Acknowledged,
            };
            Ok(Response::new(pb::PointsOperationResponse {
                result: Some(pb::UpdateResult {
                    operation_id: result.operation_id,
                    status: status as i32,
                }),
                time: ctx.elapsed_secs(),
                usage: None,
            }))
        }

        async fn create_vector_name(
            &self,
            request: Request<pb::CreateVectorNameRequest>,
        ) -> Result<Response<pb::PointsOperationResponse>, Status> {
            let req = request.get_ref();
            let body = match (&req.vector_config, conv::dense_creation_to_json(req)) {
                (None, _) => {
                    return Err(GatewayError::json("vector_config is required").grpc_status());
                }
                (_, Some(dense)) => NewVector::Dense(dense),
                (_, None) => NewVector::Sparse,
            };
            let (collection, vector) = (req.collection_name.clone(), req.vector_name.clone());
            let (result, ctx) = self
                .run(request.metadata(), req.timeout, |gw, ctx| {
                    schema::create_vector_name(gw, ctx, collection, vector, body)
                })
                .await?;
            Ok(Response::new(pb::PointsOperationResponse {
                result: Some(pb::UpdateResult {
                    operation_id: result.operation_id,
                    status: pb::UpdateStatus::Completed as i32,
                }),
                time: ctx.elapsed_secs(),
                usage: None,
            }))
        }
    }
    unsupported {
        delete_field_index(DeleteFieldIndexCollection) -> PointsOperationResponse;
        delete_vector_name(DeleteVectorNameRequest) -> PointsOperationResponse;
        search(SearchPoints) -> SearchResponse;
        search_batch(SearchBatchPoints) -> SearchBatchResponse;
        search_groups(SearchPointGroups) -> SearchGroupsResponse;
        recommend(RecommendPoints) -> RecommendResponse;
        recommend_batch(RecommendBatchPoints) -> RecommendBatchResponse;
        recommend_groups(RecommendPointGroups) -> RecommendGroupsResponse;
        discover(DiscoverPoints) -> DiscoverResponse;
        discover_batch(DiscoverBatchPoints) -> DiscoverBatchResponse;
        query(QueryPoints) -> QueryResponse;
        query_batch(QueryBatchPoints) -> QueryBatchResponse;
        query_groups(QueryPointGroups) -> QueryGroupsResponse;
        facet(FacetCounts) -> FacetResponse;
        search_matrix_pairs(SearchMatrixPoints) -> SearchMatrixPairsResponse;
        search_matrix_offsets(SearchMatrixPoints) -> SearchMatrixOffsetsResponse;
    }
}

service! {
    impl Snapshots for GrpcService as "Snapshots" {
        async fn create(
            &self,
            request: Request<pb::CreateSnapshotRequest>,
        ) -> Result<Response<pb::CreateSnapshotResponse>, Status> {
            let name = request.get_ref().collection_name.clone();
            let (m, ctx) = self
                .run(request.metadata(), None, |gw, ctx| {
                    snapshots::create(gw, ctx, name.clone())
                })
                .await?;
            Ok(Response::new(pb::CreateSnapshotResponse {
                snapshot_description: Some(conv::snapshot_to_grpc(&name, &m)),
                time: ctx.elapsed_secs(),
            }))
        }

        async fn list(
            &self,
            request: Request<pb::ListSnapshotsRequest>,
        ) -> Result<Response<pb::ListSnapshotsResponse>, Status> {
            let name = request.get_ref().collection_name.clone();
            let (versions, ctx) = self
                .run(request.metadata(), None, |gw, ctx| {
                    snapshots::list(gw, ctx, name.clone())
                })
                .await?;
            Ok(Response::new(pb::ListSnapshotsResponse {
                snapshot_descriptions: versions
                    .iter()
                    .map(|m| conv::snapshot_to_grpc(&name, m))
                    .collect(),
                time: ctx.elapsed_secs(),
            }))
        }
    }
    unsupported {
        delete(DeleteSnapshotRequest) -> DeleteSnapshotResponse;
        create_full(CreateFullSnapshotRequest) -> CreateSnapshotResponse;
        list_full(ListFullSnapshotsRequest) -> ListSnapshotsResponse;
        delete_full(DeleteFullSnapshotRequest) -> DeleteSnapshotResponse;
    }
}

fn operation(result: bool, ctx: &RequestCtx) -> Response<pb::CollectionOperationResponse> {
    Response::new(pb::CollectionOperationResponse {
        result,
        time: ctx.elapsed_secs(),
    })
}
