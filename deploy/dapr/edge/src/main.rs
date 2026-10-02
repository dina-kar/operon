use std::{env, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::{ConnectInfo, DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    routing::{get, post},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tonic::{Request, transport::Channel};

// The generated code names the CloudEvents package by relative paths, so
// both packages live at their own module paths, as prost expects.
#[allow(dead_code, clippy::all)]
mod generated {
    pub mod io {
        pub mod cloudevents {
            pub mod v1 {
                tonic::include_proto!("io.cloudevents.v1");
            }
        }
    }
    pub mod loams {
        pub mod stream {
            pub mod v1 {
                tonic::include_proto!("loams.stream.v1");
            }
        }
    }
}
use generated::loams::stream::v1 as stream;
use stream::{Header, ProduceRequest, Record, stream_service_client::StreamServiceClient};

#[derive(Clone)]
struct AppState {
    stream_app_id: String,
    namespace: String,
    trigger_stream: String,
    /// One lazily connected channel to the Dapr gRPC proxy, shared by every
    /// event instead of a new connection per event.
    stream_client: StreamServiceClient<Channel>,
    /// SHA-256 of the webhook bearer token; the webhook route is off without it.
    webhook_token: Option<[u8; 32]>,
}

fn digest(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

/// Whether the webhook request carries the configured bearer token. The
/// digests are compared, so the comparison time does not depend on how much
/// of the token matches.
fn webhook_authorized(state: &AppState, headers: &HeaderMap) -> bool {
    let Some(expected) = state.webhook_token else {
        return false;
    };
    headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .is_some_and(|token| digest(token.as_bytes()) == expected)
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

/// Every Workflow API version a supported Dapr runtime serves (the alpha and
/// beta HTTP names of Dapr 1.10-1.14 and the stable ones of 1.15+). Each must
/// be denied, so an older runtime cannot keep a Workflow endpoint open.
const WORKFLOW_APIS: [(&str, &str); 5] = [
    ("v1.0", "http"),
    ("v1.0-beta1", "http"),
    ("v1.0-alpha1", "http"),
    ("v1", "grpc"),
    ("v1alpha1", "grpc"),
];

async fn guard_workflow() -> Result<(), Box<dyn std::error::Error>> {
    if env_or("DAPR_WORKFLOW_ENABLED", "false") != "false" {
        return Err("Dapr Workflow must remain disabled; Resonate owns durable execution".into());
    }
    let namespace = env_or("POD_NAMESPACE", "loams");
    let config_name = env_or("DAPR_CONFIG_NAME", "loams-no-workflow");
    let config = kube_get(&format!(
        "/apis/dapr.io/v1alpha1/namespaces/{namespace}/configurations/{config_name}"
    ))
    .await?;
    if config["spec"].get("workflow").is_some() {
        return Err("Dapr Workflow settings are configured".into());
    }
    // Service invocation must be deny-by-default: the sidecar forwards an
    // invocation over loopback, so the app alone cannot tell it apart from a
    // pub/sub delivery.
    if config["spec"]["accessControl"]["defaultAction"] != "deny" {
        return Err("Dapr Configuration must deny service invocation by default".into());
    }
    let denied = config["spec"]["api"]["denied"]
        .as_array()
        .ok_or("Dapr Configuration has no API denylist")?;
    for (version, protocol) in WORKFLOW_APIS {
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
    let mut client = state.stream_client.clone();
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
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(message): Json<Value>,
) -> (StatusCode, Json<Value>) {
    if !matches!(source.as_str(), "kafka" | "agent" | "webhook") {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "unknown source"})),
        );
    }
    let is_pubsub = source != "webhook";
    // Pub/sub deliveries come from the Dapr sidecar in this pod, over
    // loopback; the Service can reach only the webhook route.
    if is_pubsub && !peer.ip().is_loopback() {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "pub/sub routes accept only the local Dapr sidecar"})),
        );
    }
    if !is_pubsub && !webhook_authorized(&state, &headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "a valid webhook bearer token is required"})),
        );
    }
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
    let endpoint = format!(
        "http://{}",
        env_or("DAPR_GRPC_PROXY_ENDPOINT", "127.0.0.1:50001")
    );
    let channel = Channel::from_shared(endpoint)?.connect_lazy();
    let state = Arc::new(AppState {
        stream_app_id: env_or("LOAMS_STREAM_APP_ID", "loams-stream"),
        namespace: env_or("LOAMS_NAMESPACE", "default"),
        trigger_stream: env_or("LOAMS_TRIGGER_STREAM", "workflow-triggers"),
        stream_client: StreamServiceClient::new(channel),
        webhook_token: env::var("EDGE_WEBHOOK_TOKEN")
            .ok()
            .filter(|token| !token.is_empty())
            .map(|token| digest(token.as_bytes())),
    });
    let app = Router::new()
        .route("/healthz", get(health))
        .route("/events/{source}", post(event))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(state);
    let addr: SocketAddr = env_or("EDGE_LISTEN", "0.0.0.0:8080").parse()?;
    axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .await?;
    Ok(())
}
