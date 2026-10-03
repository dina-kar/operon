//! API1 Task 1, against the main listener of the real Loams server.
use std::collections::BTreeSet;
use buffa::Message as _;
use base64::Engine as _;
use loams::{Server, ServerConfig};
use serde_json::Value;
use tempfile::TempDir;

async fn start(dir: &TempDir) -> Server {
    let mut config = ServerConfig::new(dir.path());
    config.listen = "127.0.0.1:0".parse().unwrap();
    Server::start(config).await.unwrap()
}

#[tokio::test]
async fn connect_json_unary_via_curl_shape() {
    let dir = TempDir::new().unwrap();
    let server = start(&dir).await;
    let response = reqwest::Client::new()
        .post(format!("http://{}/loams.instance.v1.InstanceService/GetInstance", server.local_addr()))
        .header("content-type", "application/json").body("{}")
        .send().await.unwrap();
    assert_eq!(response.status(), 200);
    let info: Value = response.json().await.unwrap();
    assert_eq!(info["edition"], "EDITION_OSS");
    assert!(info["apiVersions"].as_array().unwrap().iter().any(|p| p == "loams.instance.v1"));
    assert!(info["services"].as_array().unwrap().iter().any(|s| s["package"] == "loams.instance.v1" && s["available"] == true));
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn grpc_and_grpc_web_on_same_port() {
    let dir = TempDir::new().unwrap();
    let server = start(&dir).await;
    for content_type in ["application/grpc+proto", "application/grpc-web+proto"] {
        let client = if content_type == "application/grpc+proto" {
            reqwest::Client::builder().http2_prior_knowledge().build().unwrap()
        } else { reqwest::Client::new() };
        let response = client.post(format!("http://{}/loams.instance.v1.InstanceService/GetInstance", server.local_addr()))
            .header("content-type", content_type).header("te", "trailers")
            .body(vec![0_u8; 5]).send().await.unwrap();
        assert_eq!(response.status(), 200);
        assert!(response.headers()["content-type"].to_str().unwrap().starts_with(content_type));
        let body = response.bytes().await.unwrap();
        assert!(body.len() > 5, "a framed instance message must be returned");
        assert_eq!(body[0], 0);
    }
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn health_rpc_ok() {
    let dir = TempDir::new().unwrap();
    let server = start(&dir).await;
    let response = reqwest::Client::new().post(format!("http://{}/grpc.health.v1.Health/Check", server.local_addr()))
        .header("content-type", "application/json").body("{}").send().await.unwrap();
    assert_eq!(response.status(), 200);
    let health: Value = response.json().await.unwrap();
    assert_eq!(health["status"], "SERVING");
    server.shutdown().await.unwrap();
}

#[tokio::test]
async fn unavailable_service_reports_reason() {
    let dir = TempDir::new().unwrap();
    let server = start(&dir).await;
    let response = reqwest::Client::new().post(format!("http://{}/loams.live.v1.LiveService/Query", server.local_addr()))
        .header("content-type", "application/json").body("{}").send().await.unwrap();
    assert_eq!(response.status(), 501);
    let error: Value = response.json().await.unwrap();
    assert_eq!(error["code"], "unimplemented");
    let detail = &error["details"][0];
    assert_eq!(detail["type"], "loams.errors.v1.ErrorInfo");
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD.decode(detail["value"].as_str().unwrap()).unwrap();
    let info = loams_proto::loams::errors::v1::ErrorInfo::decode_from_slice(&bytes).unwrap();
    assert_eq!(info.reason, "feature_not_in_variant");
    assert_eq!(info.metadata["variant"], "standard");
    server.shutdown().await.unwrap();
}

#[test]
fn reasons_are_snake_case_and_unique() {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/api/reasons.md");
    let text = std::fs::read_to_string(path).unwrap();
    let mut reasons = BTreeSet::new();
    for line in text.lines() {
        let cells: Vec<_> = line.split('|').map(str::trim).collect();
        if cells.len() < 4 { continue; }
        let Some(reason) = cells[1].strip_prefix('`').and_then(|s| s.strip_suffix('`')) else { continue; };
        assert!(reason.starts_with(|c: char| c.is_ascii_lowercase()));
        assert!(reason.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_'));
        assert!(reasons.insert(reason.to_owned()), "duplicate reason: {reason}");
    }
    assert!(reasons.contains("feature_not_in_variant"));
    assert!(reasons.contains("approval_expired"));
}
