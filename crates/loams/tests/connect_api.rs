//! The Connect API on the main port (design §44 §4, D600; API1 plan Task 1),
//! against the real `loams` server over its `--listen` address.
//!
//! These are the five tests the plan names for Task 1. Everything else about
//! the API is Task 2 onwards: no application RPC is served yet, so the only
//! service that answers is `loams.instance.v1.InstanceService`, and the only
//! one that refuses is `loams.live.v1.LiveService`.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use buffa::Message as _;
use loams::api::connect::VARIANT;
use loams::{Server, ServerConfig};
use loams_proto::loams::errors::v1::ErrorInfo;
use serde_json::Value;
use tempfile::TempDir;

/// An in-process server on an ephemeral port, with fast background work.
struct Running {
    server: Server,
    base: String,
    _dir: TempDir,
}

impl Running {
    async fn start() -> Self {
        Self::start_with(|_| {}).await
    }

    /// [`Self::start`], with `edit` applied to the config.
    async fn start_with(edit: impl FnOnce(&mut ServerConfig)) -> Self {
        let dir = TempDir::new().expect("temp dir");
        let mut config = ServerConfig::new(dir.path());
        config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
        config.log.flush_interval = Duration::from_millis(20);
        config.worker_poll_interval = Duration::from_millis(50);
        edit(&mut config);
        let server = Server::start(config).await.expect("start");
        let base = format!("http://{}", server.local_addr());
        Self {
            server,
            base,
            _dir: dir,
        }
    }

    /// A Connect unary call: `POST` with a JSON body, which is what `curl`
    /// sends (design §44 §4).
    async fn connect(&self, rpc: &str, body: &str) -> (reqwest::StatusCode, Value) {
        let response = reqwest::Client::new()
            .post(format!("{}{rpc}", self.base))
            .header("content-type", "application/json")
            .body(body.to_owned())
            .send()
            .await
            .expect("connect call");
        let status = response.status();
        let body = response.json().await.expect("a JSON body");
        (status, body)
    }
}

/// Design §44 §4: a Connect unary call is an HTTP `POST` with a JSON body, so
/// `curl` reaches every RPC of the API without a bespoke REST layer. The
/// shape is the point: the path is `/<package>.<Service>/<Method>`, the request
/// is `content-type: application/json` and the answer is a JSON object.
#[tokio::test]
async fn connect_json_unary_via_curl_shape() {
    let running = Running::start().await;
    let (status, info) = running
        .connect("/loams.instance.v1.InstanceService/GetInstance", "{}")
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{info}");

    // proto3 JSON: `api_versions` is `apiVersions`, an enum is its proto name.
    assert_eq!(info["edition"], "EDITION_OSS", "{info}");
    assert!(
        info["apiVersions"]
            .as_array()
            .is_some_and(|list| list.iter().any(|p| p == "loams.instance.v1")),
        "{info}"
    );
    // `services[]` is the long form: what is served *and* what is not, so an
    // SDK feature-detects instead of calling and reading the error.
    let services = info["services"].as_array().expect("services[]");
    let entry = |package: &str| {
        services
            .iter()
            .find(|entry| entry["package"] == package)
            .unwrap_or_else(|| panic!("{package} is missing from {info}"))
    };
    let served = entry("loams.instance.v1");
    assert_eq!(served["available"], true, "{served}");
    assert_eq!(served["version"], "v1", "{served}");
    assert_eq!(
        served["services"],
        serde_json::json!(["loams.instance.v1.InstanceService"]),
        "{served}"
    );
    let absent = entry("loams.live.v1");
    // proto3 omits a false bool, so "not served" is absent-or-false.
    assert!(
        absent
            .get("available")
            .is_none_or(|v| v == &Value::Bool(false)),
        "{absent}"
    );
    // A package this binary does not serve is not in `apiVersions`.
    assert!(
        !info["apiVersions"]
            .as_array()
            .is_some_and(|list| list.iter().any(|p| p == "loams.live.v1")),
        "{info}"
    );

    running.server.shutdown().await.expect("shutdown");
}

/// Design §44 §4: connect-rust answers the Connect protocol, gRPC and
/// gRPC-Web from one port, so `grpcurl`, a browser and `curl` all reach the
/// same RPC on the same address. gRPC needs HTTP/2 (prior knowledge here: the
/// listener is plaintext, so there is no ALPN to negotiate), gRPC-Web is
/// HTTP/1.1, and both frame the message the same way.
#[tokio::test]
async fn grpc_and_grpc_web_on_same_port() {
    const RPC: &str = "/loams.instance.v1.InstanceService/GetInstance";
    let running = Running::start().await;

    // An empty `GetInstanceRequest`: one length-prefixed frame, compression
    // flag 0 and a zero length.
    let frame = [0u8, 0, 0, 0, 0];
    for (content_type, prior_knowledge) in [
        ("application/grpc+proto", true),
        ("application/grpc-web+proto", false),
    ] {
        let mut builder = reqwest::Client::builder();
        if prior_knowledge {
            builder = builder.http2_prior_knowledge();
        }
        let response = builder
            .build()
            .expect("client")
            .post(format!("{}{RPC}", running.base))
            .header("content-type", content_type)
            .header("te", "trailers")
            .body(frame.to_vec())
            .send()
            .await
            .unwrap_or_else(|err| panic!("{content_type}: {err}"));
        assert_eq!(response.status(), reqwest::StatusCode::OK, "{content_type}");
        assert!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with(content_type)),
            "{content_type}: {:?}",
            response.headers()
        );
        let body = response.bytes().await.expect("a framed body");
        let frames = frames(&body);
        assert!(!frames.is_empty(), "{content_type}: {body:?}");
        assert_eq!(frames[0].0, 0, "{content_type}: the frame is uncompressed");
        assert!(!frames[0].1.is_empty(), "{content_type}: a framed answer");
        if prior_knowledge {
            // gRPC carries `grpc-status` in HTTP/2 trailers.
            assert_eq!(frames.len(), 1, "{content_type}: {body:?}");
        } else {
            // gRPC-Web has no trailers to put it in, so it appends a frame
            // whose flag byte has 0x80 set.
            assert_eq!(
                frames.last().map(|frame| frame.0 & 0x80),
                Some(0x80),
                "{content_type}: no trailer frame in {body:?}"
            );
        }
    }

    running.server.shutdown().await.expect("shutdown");
}

/// The frames of a gRPC or gRPC-Web body: each is a compression flag byte, a
/// big-endian message length and that many bytes. Panics on a partial one.
fn frames(body: &[u8]) -> Vec<(u8, &[u8])> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 5 <= body.len() {
        let len =
            u32::from_be_bytes([body[at + 1], body[at + 2], body[at + 3], body[at + 4]]) as usize;
        let end = at + 5 + len;
        assert!(end <= body.len(), "a frame runs past the body: {body:?}");
        out.push((body[at], &body[at + 5..end]));
        at = end;
    }
    assert_eq!(at, body.len(), "a trailing partial frame: {body:?}");
    out
}

/// Design §44 §4: `grpc.health.v1.Health` is served on the main port, so
/// `grpc_health_probe`, Kubernetes' gRPC liveness probe and a service mesh
/// can check this node without a bespoke endpoint. An empty service name is
/// the whole process.
#[tokio::test]
async fn health_rpc_ok() {
    let running = Running::start().await;
    let (status, health) = running.connect("/grpc.health.v1.Health/Check", "{}").await;
    assert_eq!(status, reqwest::StatusCode::OK, "{health}");
    assert_eq!(health["status"], "SERVING", "{health}");

    // The catalogue's served services are registered too, so a probe that
    // names one gets an answer instead of `not_found`.
    let (status, named) = running
        .connect(
            "/grpc.health.v1.Health/Check",
            r#"{"service":"loams.instance.v1.InstanceService"}"#,
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::OK, "{named}");
    assert_eq!(named["status"], "SERVING", "{named}");

    running.server.shutdown().await.expect("shutdown");
}

/// Design §44 §4, D600: a service whose engine is not in the build variant
/// answers `unimplemented` with `ErrorInfo.reason = feature_not_in_variant`
/// and the variant in `metadata`, so a caller can tell "not in this build"
/// from "not written yet". `loams.live.v1` is a `full`-variant engine
/// (§30 §8.2) and no variant this binary is has it.
#[tokio::test]
async fn unavailable_service_reports_reason() {
    let running = Running::start().await;
    let (status, error) = running
        .connect("/loams.live.v1.LiveService/Query", "{}")
        .await;
    assert_eq!(status, reqwest::StatusCode::NOT_IMPLEMENTED, "{error}");
    assert_eq!(error["code"], "unimplemented", "{error}");

    let details = error["details"].as_array().expect("details[]");
    let detail = details
        .first()
        .unwrap_or_else(|| panic!("no ErrorInfo detail in {error}"));
    assert_eq!(detail["type"], "loams.errors.v1.ErrorInfo", "{error}");
    let bytes = STANDARD_NO_PAD
        .decode(
            detail["value"]
                .as_str()
                .unwrap_or_else(|| panic!("an encoded ErrorInfo in {error}")),
        )
        .expect("the Connect protocol base64: unpadded standard");
    let info = ErrorInfo::decode_from_slice(&bytes).expect("an ErrorInfo");
    assert_eq!(info.reason, "feature_not_in_variant");
    assert_eq!(
        info.metadata.get("variant").map(String::as_str),
        Some(VARIANT)
    );

    running.server.shutdown().await.expect("shutdown");
}

/// Design §44 §4, Q603: reflection publishes the schema of the API to anyone
/// who can reach the port, so it is off by default and `loams dev` turns it
/// on (`ServerConfig.reflection`, which `main.rs`'s `Dev` arm sets).
///
/// What is pinned here is the whole round trip: with reflection on, a client
/// that speaks the reflection protocol — what `grpcurl` does — gets the list
/// of services the server was built from, off `loams-proto`'s descriptor set;
/// with it off, the same path falls through to the native `404` like any other
/// unknown path.
///
/// The request is gRPC-Web because reflection is a bidirectional stream, which
/// the Connect unary protocol cannot carry.
#[tokio::test]
async fn reflection_lists_the_served_services_where_it_is_asked_for() {
    use connectrpc_reflection::wire::v1::ServerReflectionRequest;
    use connectrpc_reflection::wire::v1::ServerReflectionResponse;
    use connectrpc_reflection::wire::v1::server_reflection_request::MessageRequest;
    use connectrpc_reflection::wire::v1::server_reflection_response::MessageResponse;

    let request = ServerReflectionRequest {
        message_request: Some(MessageRequest::ListServices(String::new())),
        ..Default::default()
    };
    let payload = ServerReflectionRequest::encode_to_vec(&request);
    let mut frame = vec![0u8];
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);

    let running = Running::start_with(|config| config.reflection = true).await;
    let response = reqwest::Client::new()
        .post(format!(
            "{}/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            running.base
        ))
        .header("content-type", "application/grpc-web+proto")
        .body(frame)
        .send()
        .await
        .expect("reflection request");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    let answer = response.bytes().await.expect("a framed answer");
    let frames = frames(&answer);
    let answer =
        ServerReflectionResponse::decode_from_slice(frames[0].1).expect("a reflection answer");
    let Some(response) = answer.message_response else {
        panic!("{answer:?}");
    };
    let MessageResponse::ListServicesResponse(list) = response else {
        panic!("{response:?}");
    };
    assert!(
        list.service
            .iter()
            .any(|service| service.name == "loams.instance.v1.InstanceService"),
        "{list:?}"
    );
    running.server.shutdown().await.expect("shutdown");

    let running = Running::start().await;
    let (status, body) = running
        .connect(
            "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo",
            "{}",
        )
        .await;
    assert_eq!(status, reqwest::StatusCode::NOT_FOUND, "{body}");
    assert_eq!(body["error"], "not_found", "{body}");
    running.server.shutdown().await.expect("shutdown");
}

/// Design §44 §7.4, D611: `docs/api/reasons.md` is the registry of every
/// `reason` a caller may branch on. Within an API major version a reason is
/// never renamed and never removed, so the page may only grow: the test fails
/// on a duplicate, on a name outside `snake_case`, and on a registry that has
/// lost one of the reasons it promised.
#[test]
fn reasons_are_snake_case_and_unique() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/api/reasons.md");
    let text = std::fs::read_to_string(&path).expect("read docs/api/reasons.md");
    let mut reasons = BTreeSet::new();
    for line in text.lines() {
        // A registry row: `| `reason` | code | raised by | … |`.
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() < 4 {
            continue;
        }
        let Some(reason) = cells[1].strip_prefix('`').and_then(|r| r.strip_suffix('`')) else {
            continue;
        };
        assert!(
            reason.starts_with(|c: char| c.is_ascii_lowercase()),
            "a reason is lower_snake_case: {reason}"
        );
        assert!(
            reason
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'),
            "a reason is [a-z0-9_] only: {reason}"
        );
        assert!(
            reasons.insert(reason.to_owned()),
            "duplicate reason: {reason}"
        );
    }
    // The reasons the API promises today. Removing one of these is a breaking
    // change, and this line is what says so.
    for promised in [
        // AP0's, before this task.
        "approval_expired",
        "device_revoked",
        "not_implemented",
        // This task's.
        "feature_not_in_variant",
        // The code-to-class mapping every RPC error goes through (D611).
        "invalid_argument",
        "not_found",
        "unavailable",
    ] {
        assert!(
            reasons.contains(promised),
            "{promised} is missing from {path:?}"
        );
    }
}
