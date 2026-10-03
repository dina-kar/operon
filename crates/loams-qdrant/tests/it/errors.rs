//! Error mapping and the request context (plan M1.4 Task 1; E2).

use std::time::Duration;

use http::{HeaderMap, HeaderValue, StatusCode};
use loams_qdrant::{GatewayError, QdrantConfig, RequestCtx};
use loams_query::{ReadConsistency, ServiceError};
use tonic::Code;

/// Every `ServiceError` variant, listed through an exhaustive match so a
/// new variant fails to compile here too.
fn every_service_error() -> Vec<(ServiceError, StatusCode, Code, &'static str)> {
    let cases = vec![
        (
            ServiceError::NotFound {
                kind: "collection",
                name: "docs".into(),
            },
            StatusCode::NOT_FOUND,
            Code::NotFound,
            "Not found: Collection `docs` doesn't exist!",
        ),
        (
            ServiceError::NotFound {
                kind: "alias",
                name: "a1".into(),
            },
            StatusCode::NOT_FOUND,
            Code::NotFound,
            "Not found: Alias a1 does not exists!",
        ),
        (
            ServiceError::NotFound {
                kind: "field",
                name: "f".into(),
            },
            StatusCode::NOT_FOUND,
            Code::NotFound,
            "Not found: field `f` doesn't exist!",
        ),
        (
            ServiceError::AlreadyExists("docs".into()),
            StatusCode::CONFLICT,
            Code::AlreadyExists,
            "Wrong input: Collection `docs` already exists!",
        ),
        (
            ServiceError::InvalidArgument("bad".into()),
            StatusCode::BAD_REQUEST,
            Code::InvalidArgument,
            "Wrong input: bad",
        ),
        (
            ServiceError::SchemaViolation {
                field: "n".into(),
                message: "not a number".into(),
            },
            StatusCode::BAD_REQUEST,
            Code::InvalidArgument,
            "Wrong input: n: not a number",
        ),
        (
            ServiceError::Unavailable("later".into()),
            StatusCode::SERVICE_UNAVAILABLE,
            Code::Unavailable,
            "Service unavailable: later",
        ),
        (
            ServiceError::Timeout,
            StatusCode::REQUEST_TIMEOUT,
            Code::DeadlineExceeded,
            "Timeout: request timed out",
        ),
        (
            ServiceError::Internal("boom".into()),
            StatusCode::INTERNAL_SERVER_ERROR,
            Code::Internal,
            "Service internal error: boom",
        ),
        (
            ServiceError::ResourceExhausted {
                message: "over budget".into(),
                retry_after_ms: 1500,
            },
            StatusCode::TOO_MANY_REQUESTS,
            Code::ResourceExhausted,
            "Rate limiting exceeded: over budget",
        ),
    ];
    for (err, ..) in &cases {
        match err {
            ServiceError::NotFound { .. }
            | ServiceError::AlreadyExists(_)
            | ServiceError::InvalidArgument(_)
            | ServiceError::SchemaViolation { .. }
            | ServiceError::Unavailable(_)
            | ServiceError::Timeout
            | ServiceError::Internal(_)
            | ServiceError::ResourceExhausted { .. } => {}
        }
    }
    cases
}

fn check(err: &GatewayError, http: StatusCode, code: Code, message: &str) {
    assert_eq!(err.http_status(), http, "{err:?}");
    assert_eq!(err.to_string(), message, "{err:?}");
    let status = err.grpc_status();
    assert_eq!(status.code(), code, "{err:?}");
    assert_eq!(status.message(), message, "{err:?}");
}

#[test]
fn service_errors_map_to_qdrant_status_and_text() {
    for (err, http, code, message) in every_service_error() {
        let err = GatewayError::from(err);
        check(&err, http, code, message);
        let retry = err.retry_after_secs();
        assert_eq!(
            retry.is_some(),
            http == StatusCode::TOO_MANY_REQUESTS,
            "{err:?}"
        );
        assert_eq!(
            err.grpc_status().metadata().contains_key("retry-after"),
            retry.is_some()
        );
    }
    let gateway = [
        (
            GatewayError::BadRequest("x".into()),
            StatusCode::BAD_REQUEST,
            Code::InvalidArgument,
            "Wrong input: x",
        ),
        (
            GatewayError::Format {
                what: "query parameters",
                message: "bad wait".into(),
            },
            StatusCode::BAD_REQUEST,
            Code::InvalidArgument,
            "Format error in query parameters: bad wait",
        ),
        (
            GatewayError::PointNotFound("7".into()),
            StatusCode::NOT_FOUND,
            Code::NotFound,
            "Not found: Point with id 7 does not exists!",
        ),
        (
            GatewayError::PointsNotFound("7".into()),
            StatusCode::NOT_FOUND,
            Code::NotFound,
            "Not found: No point with id 7 found",
        ),
        (
            GatewayError::Unsupported("facet".into()),
            StatusCode::NOT_IMPLEMENTED,
            Code::Unimplemented,
            "Unsupported in Loams: facet",
        ),
        (
            GatewayError::Timeout(Duration::from_secs(3)),
            StatusCode::REQUEST_TIMEOUT,
            Code::DeadlineExceeded,
            "Timeout: request timed out after 3s",
        ),
        (
            GatewayError::CollectionExists("docs".into()),
            StatusCode::CONFLICT,
            Code::AlreadyExists,
            "Wrong input: Collection `docs` already exists!",
        ),
        (
            GatewayError::TooLarge,
            StatusCode::PAYLOAD_TOO_LARGE,
            Code::ResourceExhausted,
            "Format error in JSON body: payload too large",
        ),
    ];
    for (err, http, code, message) in gateway {
        check(&err, http, code, message);
        assert_eq!(err.retry_after_secs(), None, "{err:?}");
    }
    for (ms, secs) in [(0, 1), (1, 1), (1000, 1), (1001, 2), (2500, 3)] {
        let err = GatewayError::from(ServiceError::ResourceExhausted {
            message: "m".into(),
            retry_after_ms: ms,
        });
        assert_eq!(err.retry_after_secs(), Some(secs), "{ms} ms");
        let status = err.grpc_status();
        let value = status.metadata().get("retry-after").expect("retry-after");
        assert_eq!(value.to_str().unwrap(), secs.to_string(), "{ms} ms");
    }
}

#[test]
fn ctx_reads_namespace_and_token() {
    let config = QdrantConfig::default();
    let mut headers = HeaderMap::new();
    headers.insert("Loams-Namespace", HeaderValue::from_static("acme"));
    headers.insert(
        "Loams-Consistency-Token",
        HeaderValue::from_static("v1:s7/p3@918274"),
    );
    let ctx = RequestCtx::from_http(&headers, Some(5), &config).expect("ctx");
    assert_eq!(ctx.ns, "acme");
    assert_eq!(
        ctx.consistency,
        ReadConsistency::AtLeast("v1:s7/p3@918274".parse().unwrap())
    );
    assert_eq!(ctx.timeout, Some(Duration::from_secs(5)));

    let ctx = RequestCtx::from_http(&HeaderMap::new(), None, &config).expect("ctx");
    assert_eq!(ctx.ns, "default");
    assert_eq!(ctx.consistency, ReadConsistency::Strong);
    assert_eq!(ctx.timeout, None);

    let mut empty = HeaderMap::new();
    empty.insert("loams-namespace", HeaderValue::from_static(""));
    assert_eq!(
        RequestCtx::from_http(&empty, None, &config)
            .expect("ctx")
            .ns,
        "default"
    );

    let mut bad = HeaderMap::new();
    bad.insert("loams-consistency-token", HeaderValue::from_static("nope"));
    let err = RequestCtx::from_http(&bad, None, &config).expect_err("bad token");
    assert_eq!(err.http_status(), StatusCode::BAD_REQUEST);
    assert!(
        err.to_string()
            .starts_with("Wrong input: invalid Loams-Consistency-Token: "),
        "{err}"
    );

    let mut latin1 = HeaderMap::new();
    latin1.insert(
        "loams-namespace",
        HeaderValue::from_bytes(&[0xe9]).expect("opaque"),
    );
    let err = RequestCtx::from_http(&latin1, None, &config).expect_err("not UTF-8");
    assert_eq!(err.http_status(), StatusCode::BAD_REQUEST);

    let mut meta = tonic::metadata::MetadataMap::new();
    meta.insert("loams-namespace", "grpc-ns".parse().unwrap());
    let ctx = RequestCtx::from_grpc(&meta, Some(2), &config).expect("ctx");
    assert_eq!(ctx.ns, "grpc-ns");
    assert_eq!(ctx.consistency, ReadConsistency::Strong);
    assert_eq!(ctx.timeout, Some(Duration::from_secs(2)));
}

#[tokio::test(start_paused = true)]
async fn ctx_timeout_fires() {
    let config = QdrantConfig::default();
    let ctx = RequestCtx::from_http(&HeaderMap::new(), Some(1), &config).expect("ctx");
    let started = tokio::time::Instant::now();
    let result = ctx
        .run(async {
            tokio::time::sleep(Duration::from_secs(2)).await;
            Ok::<_, GatewayError>(())
        })
        .await;
    let waited = started.elapsed();
    assert!(
        matches!(result, Err(GatewayError::Timeout(d)) if d == Duration::from_secs(1)),
        "{result:?}"
    );
    assert!(
        waited >= Duration::from_secs(1) && waited < Duration::from_secs(2),
        "{waited:?}"
    );
    let fast = ctx.run(async { Ok::<_, GatewayError>(7) }).await;
    assert_eq!(fast.expect("in time"), 7);
}

#[test]
fn check_request_len_refuses_huge_lengths() {
    // Issue #298: client-sized lists are checked before allocation.
    let config = QdrantConfig::default();
    assert_eq!(config.max_batch_queries, 1_000);
    assert_eq!(config.max_point_ids, 10_000);
    loams_qdrant::check_request_len("The id list", 0, 10).expect("empty");
    loams_qdrant::check_request_len("The id list", 10, 10).expect("at the limit");
    loams_qdrant::check_request_len("The id list", 0, 0).expect("zero limit accepts empty");
    assert!(loams_qdrant::check_request_len("The id list", 1, 0).is_err());
    for len in [11, usize::MAX] {
        let err = loams_qdrant::check_request_len("The id list", len, 10).expect_err("over");
        assert_eq!(err.http_status(), StatusCode::BAD_REQUEST);
        assert_eq!(err.grpc_code(), Code::InvalidArgument);
        assert_eq!(
            err.to_string(),
            format!("Wrong input: The id list holds {len} entries, more than the limit of 10")
        );
    }
}
