use std::{env, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{DefaultBodyLimit, Path, State},
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tonic::{Request, transport::Channel};

mod stream {
    tonic::include_proto!("loam.stream.v1");
}
use stream::{Header, ProduceRequest, Record, stream_service_client::StreamServiceClient};

#[derive(Clone)]
struct AppState {
    stream_app_id: String,
    namespace: String,
    trigger_stream: String,
    grpc_endpoint: String,
}

fn env_or(key: &str, default: &str) -> String {
    env::var(key).unwrap_or_else(|_| default.into())
}

async fn kube_get(path: &str) -> Result<Value, Box<dyn std::error::Error>> {
    let account = "/var/run/secrets/kubernetes.io/serviceaccount";
    let token = tokio::fs::read_to_string(format!("{account}/token")).await?;
    let ca = tokio::fs::read(format!("{account}/ca.crt")).await?;
    let client = reqwest::Client::builder()
        .add_root_certificate(reqwest::Certificate::from_pem(&ca)?)
        .timeout(Duration::from_secs(5))
        .build()?;
    let host = env::var("KUBERNETES_SERVICE_HOST")?;
    let port = env_or("KUBERNETES_SERVICE_PORT", "443");
    Ok(client
        .get(format!("https://{host}:{port}{path}"))
        .bearer_auth(token.trim())
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?)
}

async fn guard_workflow() -> Result<(), Box<dyn std::error::Error>> {
    if env_or("DAPR_WORKFLOW_ENABLED", "false") != "false" {
        return Err("Dapr Workflow must remain disabled; Resonate owns durable execution".into());
    }
    let namespace = env_or("POD_NAMESPACE", "operon");
    let config_name = env_or("DAPR_CONFIG_NAME", "operon-no-workflow");
    let config = kube_get(&format!(
        "/apis/dapr.io/v1alpha1/namespaces/{namespace}/configurations/{config_name}"
    ))
    .await?;
    if config["spec"].get("workflow").is_some() {
        return Err("Dapr Workflow settings are configured".into());
    }
    let denied = config["spec"]["api"]["denied"]
        .as_array()
        .ok_or("Dapr Configuration has no API denylist")?;
    for (version, protocol) in [("v1.0", "http"), ("v1", "grpc")] {
        if !denied.iter().any(|item| {
            item["name"] == "workflows"
                && item["version"] == version
                && item["protocol"] == protocol
        }) {
            return Err(format!("Dapr Workflow {protocol} API is not denied").into());
        }
    }
    let components = kube_get(&format!(
        "/apis/dapr.io/v1alpha1/namespaces/{namespace}/components"
    ))
    .await?;
    for component in components["items"]
        .as_array()
        .ok_or("cannot list Dapr Components")?
    {
        if component["spec"]["type"]
            .as_str()
            .is_some_and(|kind| kind == "workflow" || kind.starts_with("workflow."))
        {
            return Err(format!(
                "Dapr Workflow component configured: {}",
                component["metadata"]["name"]
            )
            .into());
        }
    }
    Ok(())
}

fn normalize(source: &str, message: &Value) -> Result<(String, Vec<u8>, Vec<u8>), String> {
    let data = if source == "webhook" {
        message
    } else {
        message.get("data").unwrap_or(message)
    };
    let id = data["event_id"]
        .as_str()
        .filter(|id| !id.trim().is_empty() && id.len() <= 512)
        .ok_or("event_id must be a stable, nonempty string of at most 512 bytes")?;
    let payload = data["payload"]
        .as_object()
        .ok_or("payload must be a JSON object")?;
    let mut hash = Sha256::new();
    hash.update(source.as_bytes());
    hash.update([0u8]);
    hash.update(id.as_bytes());
    let invocation_id = hex::encode(hash.finalize());
    let canonical = json!({
        "source": source,
        "event_id": id,
        "invocation_id": invocation_id,
        "payload": payload,
    });
    let value = serde_json::to_vec(&canonical).map_err(|error| error.to_string())?;
    Ok((invocation_id, id.as_bytes().to_vec(), value))
}

async fn deliver(state: &AppState, source: &str, message: &Value) -> Result<Value, String> {
    let (invocation_id, event_id, value) = normalize(source, message)?;
    let endpoint = format!("http://{}", state.grpc_endpoint);
    let channel = Channel::from_shared(endpoint)
        .map_err(|error| error.to_string())?
        .connect()
        .await
        .map_err(|error| error.to_string())?;
    let mut client = StreamServiceClient::new(channel);
    let mut request = Request::new(ProduceRequest {
        namespace: state.namespace.clone(),
        stream: state.trigger_stream.clone(),
        partition: 0,
        records: vec![Record {
            key: Some(invocation_id.as_bytes().to_vec()),
            value: Some(value),
            headers: vec![
                Header {
                    key: "source-event-id".into(),
                    value: Some(event_id),
                },
                Header {
                    key: "resonate-invocation-id".into(),
                    value: Some(invocation_id.as_bytes().to_vec()),
                },
            ],
            timestamp_ms: -1,
        }],
    });
    request.metadata_mut().insert(
        "dapr-app-id",
        state
            .stream_app_id
            .parse()
            .map_err(|error: tonic::metadata::errors::InvalidMetadataValue| error.to_string())?,
    );
    let result = tokio::time::timeout(Duration::from_secs(15), client.produce(request))
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?
        .into_inner();
    Ok(
        json!({"invocation_id": invocation_id, "stream_id": result.stream_id, "offset": result.base_offset}),
    )
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true}))
}

async fn event(
    Path(source): Path<String>,
    State(state): State<Arc<AppState>>,
    Json(message): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if !matches!(source.as_str(), "kafka" | "agent" | "webhook") {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "unknown source"})),
        );
    }
    let is_pubsub = source != "webhook";
    match deliver(&state, &source, &message).await {
        Ok(_) if is_pubsub => (StatusCode::OK, Json(json!({"status": "SUCCESS"}))),
        Ok(value) => (StatusCode::ACCEPTED, Json(value)),
        Err(error) if error.starts_with("event_id") || error.starts_with("payload") => {
            if is_pubsub {
                (
                    StatusCode::OK,
                    Json(json!({"status": "DROP", "error": error})),
                )
            } else {
                (StatusCode::BAD_REQUEST, Json(json!({"error": error})))
            }
        }
        Err(error) if is_pubsub => (
            StatusCode::OK,
            Json(json!({"status": "RETRY", "error": error})),
        ),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": error})),
        ),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    guard_workflow().await?;
    if env::args().any(|arg| arg == "--check-config") {
        return Ok(());
    }
    // The SDK is built without its default Workflow feature. Metadata verifies
    // that the local Dapr sidecar is available before the adapter starts.
    let mut dapr = dapr::Client::new().await?;
    dapr.get_metadata().await?;
    let state = Arc::new(AppState {
        stream_app_id: env_or("OPERON_STREAM_APP_ID", "operon-stream"),
        namespace: env_or("OPERON_NAMESPACE", "default"),
        trigger_stream: env_or("OPERON_TRIGGER_STREAM", "workflow-triggers"),
        grpc_endpoint: env_or("DAPR_GRPC_PROXY_ENDPOINT", "127.0.0.1:50001"),
    });
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/events/{source}", post(event))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(state);
    let addr: SocketAddr = env_or("EDGE_LISTEN", "0.0.0.0:8080").parse()?;
    axum::serve(tokio::net::TcpListener::bind(addr).await?, app).await?;
    Ok(())
}
