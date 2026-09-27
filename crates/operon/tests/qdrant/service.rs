//! Serving: listeners, service endpoints and request plumbing (plan M1.4
//! Task 2).

use operon_qdrant::proto::health::HealthCheckRequest as StdHealthRequest;
use operon_qdrant::proto::health::health_check_response::ServingStatus;
use operon_qdrant::proto::health::health_client::HealthClient;
use operon_qdrant::proto::qdrant::{CountPoints, HealthCheckRequest, ListCollectionsRequest};
use reqwest::{Method, StatusCode};
use serde_json::{Value, json};
use tonic::codec::CompressionEncoding;

use crate::harness::Qd;

/// A Qdrant error envelope: `status.error` (returned) and `time`.
fn envelope_error(body: &Value) -> &str {
    assert!(body["time"].is_f64(), "{body}");
    assert!(body.get("result").is_none(), "{body}");
    body["status"]["error"]
        .as_str()
        .unwrap_or_else(|| panic!("no status.error: {body}"))
}

#[tokio::test]
async fn root_reports_qdrant_title_and_version() {
    let qd = Qd::start().await;
    let response = qd
        .http
        .get(format!("{}/", qd.rest))
        .send()
        .await
        .expect("GET /");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"].to_str().unwrap(),
        "application/json"
    );
    let text = response.text().await.expect("body");
    assert_eq!(
        text,
        r#"{"title":"qdrant - vector search engine","version":"1.19.1"}"#
    );
}

#[tokio::test]
async fn grpc_health_check_equals_rest_root() {
    let qd = Qd::start().await;
    let (_, root) = qd.get("/", None).await;
    let reply = qd
        .qdrant()
        .await
        .health_check(HealthCheckRequest {})
        .await
        .expect("HealthCheck")
        .into_inner();
    assert_eq!(json!(reply.title), root["title"]);
    assert_eq!(json!(reply.version), root["version"]);
    assert_eq!(reply.commit, None);
}

#[tokio::test]
async fn grpc_standard_health_is_serving() {
    let qd = Qd::start().await;
    let mut health = HealthClient::new(qd.channel().await);
    for service in ["", "qdrant.Points", "anything"] {
        let reply = health
            .check(StdHealthRequest {
                service: service.to_string(),
            })
            .await
            .expect("Check")
            .into_inner();
        assert_eq!(reply.status, ServingStatus::Serving as i32, "{service:?}");
    }
}

#[tokio::test]
async fn health_probes_answer_text() {
    let qd = Qd::start().await;
    for (path, text) in [
        ("/healthz", "healthz check passed"),
        ("/livez", "livez check passed"),
        ("/readyz", "all shards are ready"),
    ] {
        let (status, body) = qd.get(path, None).await;
        assert_eq!(status, StatusCode::OK, "{path}");
        assert_eq!(body, json!(text), "{path}");
    }
}

#[tokio::test]
async fn unknown_route_is_404_with_envelope() {
    let qd = Qd::start().await;
    let (status, body) = qd.get("/nope", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(envelope_error(&body), "Not found: route GET /nope");
}

#[tokio::test]
async fn method_not_allowed_is_405_with_envelope() {
    let qd = Qd::start().await;
    let (status, body) = qd.delete("/collections", None).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    assert!(
        envelope_error(&body).contains("DELETE /collections"),
        "{body}"
    );
}

#[tokio::test]
async fn phase_b_route_is_501() {
    let qd = Qd::start().await;
    let (status, body) = qd
        .post("/collections/x/facet", Some(json!({"key": "k"})))
        .await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(
        envelope_error(&body),
        "Unsupported in Operon: POST /collections/{collection_name}/facet"
    );
}

#[tokio::test]
async fn malformed_json_is_400_format_error() {
    let qd = Qd::start().await;
    qd.create_raw("default", "docs").await;
    let (status, body) = qd
        .post_raw("/collections/docs/points/count", b"{".to_vec())
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(
        envelope_error(&body).starts_with("Format error in JSON body:"),
        "{body}"
    );
}

#[tokio::test]
async fn oversized_body_is_413() {
    let qd = Qd::start().await;
    qd.create_raw("default", "docs").await;
    let mut body = b"{\"filter\": null, \"pad\": \"".to_vec();
    body.resize(33 << 20, b'x');
    body.extend_from_slice(b"\"}");
    let (status, body) = qd.post_raw("/collections/docs/points/count", body).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        envelope_error(&body),
        "Format error in JSON body: payload too large"
    );
}

#[tokio::test]
async fn namespace_header_isolates_collections() {
    let qd = Qd::start().await;
    qd.create_raw("a", "in_a").await;
    let names = |body: &Value| -> Vec<String> {
        body["result"]["collections"]
            .as_array()
            .unwrap_or_else(|| panic!("{body}"))
            .iter()
            .map(|c| c["name"].as_str().expect("name").to_string())
            .collect()
    };
    let (status, body) = qd.get("/collections", None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "ok");
    assert!(names(&body).is_empty(), "{body}");
    let (status, body) = qd
        .send(
            Method::GET,
            "/collections",
            None,
            &[("Operon-Namespace", "a")],
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(names(&body), ["in_a"]);

    // The same over gRPC metadata.
    let mut collections = qd.collections().await;
    let listed = collections
        .list(ListCollectionsRequest {})
        .await
        .expect("List")
        .into_inner();
    assert!(listed.collections.is_empty());
    let mut request = tonic::Request::new(ListCollectionsRequest {});
    request
        .metadata_mut()
        .insert("operon-namespace", "a".parse().unwrap());
    let listed = collections.list(request).await.expect("List").into_inner();
    let names: Vec<_> = listed.collections.into_iter().map(|c| c.name).collect();
    assert_eq!(names, ["in_a"]);
}

fn count_request(collection: &str) -> CountPoints {
    CountPoints {
        collection_name: collection.to_string(),
        exact: Some(true),
        ..Default::default()
    }
}

#[tokio::test]
async fn grpc_accepts_gzip() {
    let qd = Qd::start().await;
    qd.create_raw("default", "docs").await;
    let mut points = qd
        .points()
        .await
        .send_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Gzip);
    let reply = points
        .count(count_request("docs"))
        .await
        .expect("Count")
        .into_inner();
    assert_eq!(reply.result.expect("result").count, 0);
    assert!(reply.time >= 0.0);
}

#[test]
fn dev_binary_prints_qdrant_listeners() {
    let dev = Qd::dev(true);
    let rest_prefix = "operon qdrant REST listening on http://";
    let grpc_prefix = "operon qdrant gRPC listening on grpc://";
    let position = |prefix: &str| dev.lines.iter().position(|l| l.starts_with(prefix));
    let rest_at = position(rest_prefix).expect("REST line");
    let grpc_at = position(grpc_prefix).expect("gRPC line");
    let http_at = position("operon listening on ").expect("HTTP line");
    assert!(rest_at < grpc_at && grpc_at < http_at, "{:?}", dev.lines);
    let rest = dev.addr(rest_prefix).expect("REST addr");
    let grpc: std::net::SocketAddr = dev.addr(grpc_prefix).expect("gRPC").parse().expect("addr");
    assert_ne!(grpc.port(), 0, "the bound port");
    let runtime = tokio::runtime::Runtime::new().expect("runtime");
    let body: Value = runtime.block_on(async {
        reqwest::get(format!("http://{rest}/"))
            .await
            .expect("GET /")
            .json()
            .await
            .expect("JSON")
    });
    assert_eq!(body["title"], "qdrant - vector search engine");
}

#[test]
fn no_qdrant_flag_disables_listeners() {
    let dev = Qd::dev(false);
    assert!(
        !dev.lines.iter().any(|l| l.contains("qdrant")),
        "{:?}",
        dev.lines
    );
}

#[tokio::test]
async fn api_key_is_ignored() {
    let qd = Qd::start().await;
    for header in [("api-key", "secret"), ("authorization", "Bearer secret")] {
        let (status, body) = qd.send(Method::GET, "/collections", None, &[header]).await;
        assert_eq!(status, StatusCode::OK, "{header:?}: {body}");
    }
    let mut request = tonic::Request::new(ListCollectionsRequest {});
    request
        .metadata_mut()
        .insert("api-key", "secret".parse().unwrap());
    qd.collections()
        .await
        .list(request)
        .await
        .expect("List with api-key");
}

#[tokio::test]
async fn the_hot_header_is_honoured_on_rest_and_grpc() {
    let qd = Qd::start().await;
    qd.create_raw("default", "docs").await;
    let response = qd
        .http
        .post(format!("{}/collections/docs/points/count", qd.rest))
        .header("Operon-Hot", "off")
        .json(&json!({"exact": true}))
        .send()
        .await
        .expect("count");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["operon-hot-used"], "none");
    let body: Value = response.json().await.expect("JSON");
    assert_eq!(body["result"]["count"], 0, "{body}");

    let (status, body) = qd
        .send(
            Method::POST,
            "/collections/docs/points/count",
            Some(json!({})),
            &[("Operon-Hot", "maybe")],
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        envelope_error(&body),
        "Wrong input: invalid Operon-Hot header: maybe (expected on or off)"
    );

    let mut points = qd.points().await;
    let mut request = tonic::Request::new(count_request("docs"));
    request
        .metadata_mut()
        .insert("operon-hot", "off".parse().unwrap());
    let reply = points.count(request).await.expect("Count");
    assert_eq!(reply.metadata().get("operon-hot-used").unwrap(), "none");

    let mut request = tonic::Request::new(count_request("docs"));
    request
        .metadata_mut()
        .insert("operon-hot", "maybe".parse().unwrap());
    let status = points.count(request).await.expect_err("invalid");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "Wrong input: invalid Operon-Hot header: maybe (expected on or off)"
    );
}

#[tokio::test]
async fn service_errors_use_the_qdrant_envelope_and_status() {
    let qd = Qd::start().await;
    let (status, body) = qd
        .post("/collections/missing/points/count", Some(json!({})))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        envelope_error(&body),
        "Not found: Collection `missing` doesn't exist!"
    );
    let status = qd
        .points()
        .await
        .count(count_request("missing"))
        .await
        .expect_err("missing");
    assert_eq!(status.code(), tonic::Code::NotFound);
}
