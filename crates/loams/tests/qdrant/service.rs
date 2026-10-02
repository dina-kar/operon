//! Serving: listeners, service endpoints and request plumbing (plan M1.4
//! Task 2).

use loams_qdrant::proto::health::HealthCheckRequest as StdHealthRequest;
use loams_qdrant::proto::health::health_check_response::ServingStatus;
use loams_qdrant::proto::health::health_client::HealthClient;
use loams_qdrant::proto::qdrant::{CountPoints, HealthCheckRequest, ListCollectionsRequest};
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
        "Unsupported in Loams: POST /collections/{collection_name}/facet"
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

/// Sends `head` and then `chunks` on a raw connection, and reads the
/// response's status and body. Write errors are ignored: the server may
/// answer and stop reading before the request is complete.
async fn raw_exchange(qd: &Qd, head: String, chunks: Vec<Vec<u8>>) -> (u16, Value) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let addr = qd.rest.trim_start_matches("http://");
    let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let (mut read, mut write) = stream.into_split();
    let writer = tokio::spawn(async move {
        if write.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        for chunk in chunks {
            if write.write_all(&chunk).await.is_err() {
                return;
            }
        }
    });
    let mut buf = Vec::new();
    let response = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        let mut piece = [0u8; 4096];
        loop {
            let n = read.read(&mut piece).await.expect("read");
            assert!(n > 0, "closed before a full response: {buf:?}");
            buf.extend_from_slice(&piece[..n]);
            let text = String::from_utf8_lossy(&buf).to_string();
            let Some(end) = text.find("\r\n\r\n") else {
                continue;
            };
            let length = text[..end]
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    k.eq_ignore_ascii_case("content-length")
                        .then(|| v.trim().parse::<usize>().ok())?
                })
                .expect("content-length");
            if buf.len() >= end + 4 + length {
                let status = text[9..12].parse::<u16>().expect("status");
                let body = serde_json::from_slice(&buf[end + 4..end + 4 + length]).expect("JSON");
                return (status, body);
            }
        }
    })
    .await
    .expect("a response in time");
    writer.abort();
    response
}

#[tokio::test]
async fn oversized_body_is_413() {
    let qd = Qd::start().await;
    qd.create_raw("default", "docs").await;
    // A declared length over the limit is refused before the body is read,
    // so only a few bytes are sent.
    let head = format!(
        "POST /collections/docs/points/count HTTP/1.1\r\nhost: x\r\n\
         content-type: application/json\r\ncontent-length: {}\r\n\r\n",
        33 << 20
    );
    let (status, body) = raw_exchange(&qd, head, vec![b"{".to_vec()]).await;
    assert_eq!(status, 413);
    assert_eq!(
        envelope_error(&body),
        "Format error in JSON body: payload too large"
    );
}

#[tokio::test]
async fn oversized_chunked_body_is_413() {
    let qd = Qd::start().await;
    qd.create_raw("default", "docs").await;
    // No declared length: the body is read up to the limit.
    let head = "POST /collections/docs/points/count HTTP/1.1\r\nhost: x\r\n\
                content-type: application/json\r\ntransfer-encoding: chunked\r\n\r\n"
        .to_string();
    let mut chunk = format!("{:x}\r\n", 1 << 20).into_bytes();
    chunk.extend(std::iter::repeat_n(b' ', 1 << 20));
    chunk.extend_from_slice(b"\r\n");
    let (status, body) = raw_exchange(&qd, head, vec![chunk; 33]).await;
    assert_eq!(status, 413);
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
            &[("Loams-Namespace", "a")],
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
        .insert("loams-namespace", "a".parse().unwrap());
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
    let rest_prefix = "loams qdrant REST listening on http://";
    let grpc_prefix = "loams qdrant gRPC listening on grpc://";
    let position = |prefix: &str| dev.lines.iter().position(|l| l.starts_with(prefix));
    let rest_at = position(rest_prefix).expect("REST line");
    let grpc_at = position(grpc_prefix).expect("gRPC line");
    let http_at = position("loams listening on ").expect("HTTP line");
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
        .header("Loams-Hot", "off")
        .json(&json!({"exact": true}))
        .send()
        .await
        .expect("count");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["loams-hot-used"], "none");
    let body: Value = response.json().await.expect("JSON");
    assert_eq!(body["result"]["count"], 0, "{body}");

    let (status, body) = qd
        .send(
            Method::POST,
            "/collections/docs/points/count",
            Some(json!({})),
            &[("Loams-Hot", "maybe")],
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        envelope_error(&body),
        "Wrong input: invalid Loams-Hot header: maybe (expected on or off)"
    );

    let mut points = qd.points().await;
    let mut request = tonic::Request::new(count_request("docs"));
    request
        .metadata_mut()
        .insert("loams-hot", "off".parse().unwrap());
    let reply = points.count(request).await.expect("Count");
    assert_eq!(reply.metadata().get("loams-hot-used").unwrap(), "none");

    let mut request = tonic::Request::new(count_request("docs"));
    request
        .metadata_mut()
        .insert("loams-hot", "maybe".parse().unwrap());
    let status = points.count(request).await.expect_err("invalid");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert_eq!(
        status.message(),
        "Wrong input: invalid Loams-Hot header: maybe (expected on or off)"
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
