use std::{env, net::SocketAddr, sync::Arc, time::Duration};

use axum::{
    Json, Router,
    body::Bytes,
    extract::{ConnectInfo, DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode, header},
    routing::{get, post},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tonic::{Request, transport::Channel};

// Both packages at their own module paths, as prost's relative paths expect.
#[allow(clippy::enum_variant_names)] // the CloudEvents schema's own names
mod generated {
    pub mod io {
        pub mod cloudevents {
            pub mod v1 {
                tonic::include_proto!("io.cloudevents.v1");
            }
        }
    }
    pub mod loam {
        pub mod stream {
            pub mod v1 {
                tonic::include_proto!("loam.stream.v1");
            }
        }
    }
}
use generated::io::cloudevents::v1::CloudEvent;
use generated::io::cloudevents::v1::cloud_event::cloud_event_attribute_value::Attr;
use generated::io::cloudevents::v1::cloud_event::{CloudEventAttributeValue, Data};
use generated::loam::stream::v1 as stream;
use stream::produce_cloud_events_request::Events;
use stream::{EventStatus, ProduceCloudEventsRequest, stream_service_client::StreamServiceClient};

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
        .get(header::AUTHORIZATION)
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
    let namespace = env_or("POD_NAMESPACE", "operon");
    let config_name = env_or("DAPR_CONFIG_NAME", "operon-no-workflow");
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

/// What the stream service said about one event, as the edge acts on it.
#[derive(Debug, PartialEq, Eq)]
enum Delivery {
    /// Appended, or appended before (a redelivery): acknowledge.
    Stored {
        duplicate: bool,
        partition: u32,
        offset: u64,
    },
    /// Another request is appending the same `source` + `id`: retry later.
    InFlight { retry_after_ms: u64 },
    /// The event is not a valid CloudEvent: redelivery cannot fix it.
    Invalid(String),
    /// The stream service or the path to it failed: retry.
    Failed(String),
}

fn content_type(headers: &HeaderMap) -> String {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// Decodes the HTTP binding's percent-encoding of a `ce-*` header value.
fn percent_decode(value: &[u8]) -> Result<String, String> {
    let mut out = Vec::with_capacity(value.len());
    let mut i = 0;
    while i < value.len() {
        if value[i] == b'%' {
            let hex = value
                .get(i + 1..i + 3)
                .and_then(|h| std::str::from_utf8(h).ok())
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or("bad percent-encoding in a ce- header")?;
            out.push(hex);
            i += 3;
        } else {
            out.push(value[i]);
            i += 1;
        }
    }
    String::from_utf8(out).map_err(|_| "a ce- header is not UTF-8".to_string())
}

/// A binary-mode HTTP event (`ce-*` headers, the body as the data) as a
/// protobuf event. The stream service validates it: required attributes, the
/// `specversion`, `time`.
fn binary_event(headers: &HeaderMap, body: Bytes) -> Result<CloudEvent, String> {
    let mut event = CloudEvent::default();
    for (name, value) in headers {
        let Some(attr) = name.as_str().strip_prefix("ce-") else {
            continue;
        };
        let value = percent_decode(value.as_bytes())?;
        match attr {
            "id" => event.id = value,
            "source" => event.source = value,
            "specversion" => event.spec_version = value,
            "type" => event.r#type = value,
            // Headers carry strings only; the stream service reads `time`
            // from its string form like any other attribute.
            _ => {
                event.attributes.insert(
                    attr.to_string(),
                    CloudEventAttributeValue {
                        attr: Some(Attr::CeString(value)),
                    },
                );
            }
        }
    }
    if let Some(content_type) = headers.get(header::CONTENT_TYPE) {
        let content_type = content_type
            .to_str()
            .map_err(|_| "the Content-Type header is not visible ASCII".to_string())?;
        event.attributes.insert(
            "datacontenttype".into(),
            CloudEventAttributeValue {
                attr: Some(Attr::CeString(content_type.into())),
            },
        );
    }
    if !body.is_empty() {
        event.data = Some(Data::BinaryData(body.to_vec()));
    }
    Ok(event)
}

/// The request for one delivery: Dapr's pub/sub CloudEvent and a structured
/// webhook event go through as the JSON they arrived as (the stream service
/// reads it with the same codec as its HTTP route), a binary-mode webhook
/// event as a protobuf event. Nothing is renamed or rewritten, so the
/// deduplication key is the publisher's own `source` + `id`.
fn events_of(headers: &HeaderMap, body: Bytes) -> Result<Events, String> {
    match content_type(headers).as_str() {
        "application/cloudevents+json" => {
            let mut batch = Vec::with_capacity(body.len() + 2);
            batch.push(b'[');
            batch.extend_from_slice(&body);
            batch.push(b']');
            Ok(Events::JsonBatch(batch))
        }
        "application/cloudevents-batch+json" => Ok(Events::JsonBatch(body.to_vec())),
        _ if headers.contains_key("ce-specversion") => Ok(Events::Batch(
            generated::io::cloudevents::v1::CloudEventBatch {
                events: vec![binary_event(headers, body)?],
            },
        )),
        _ => Err(
            "send a CloudEvent: a ce-specversion header, or Content-Type \
                  application/cloudevents+json"
                .to_string(),
        ),
    }
}

async fn deliver(state: &AppState, headers: &HeaderMap, body: Bytes) -> Delivery {
    let events = match events_of(headers, body) {
        Ok(events) => events,
        Err(error) => return Delivery::Invalid(error),
    };
    let mut client = state.stream_client.clone();
    let mut request = Request::new(ProduceCloudEventsRequest {
        namespace: state.namespace.clone(),
        stream: state.trigger_stream.clone(),
        partition: None,
        events: Some(events),
    });
    match state.stream_app_id.parse() {
        Ok(app_id) => {
            request.metadata_mut().insert("dapr-app-id", app_id);
        }
        Err(error) => return Delivery::Failed(format!("{error}")),
    }
    let answer = match tokio::time::timeout(
        Duration::from_secs(15),
        client.produce_cloud_events(request),
    )
    .await
    {
        Err(error) => return Delivery::Failed(error.to_string()),
        Ok(Err(status)) if status.code() == tonic::Code::InvalidArgument => {
            return Delivery::Invalid(status.message().to_string());
        }
        Ok(Err(status)) => return Delivery::Failed(status.to_string()),
        Ok(Ok(answer)) => answer.into_inner(),
    };
    match answer.results.as_slice() {
        [result] => match EventStatus::try_from(result.status) {
            Ok(EventStatus::Appended) => Delivery::Stored {
                duplicate: false,
                partition: result.partition,
                offset: result.offset,
            },
            Ok(EventStatus::Duplicate) => Delivery::Stored {
                duplicate: true,
                partition: result.partition,
                offset: result.offset,
            },
            Ok(EventStatus::InFlight) => Delivery::InFlight {
                retry_after_ms: result.retry_after_ms,
            },
            _ => Delivery::Failed("the stream service sent an unknown status".to_string()),
        },
        other => Delivery::Failed(format!(
            "the stream service answered {} results for one event",
            other.len()
        )),
    }
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true}))
}

async fn event(
    Path(source): Path<String>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> (StatusCode, HeaderMap, Json<Value>) {
    let reply = |status, body: Value| (status, HeaderMap::new(), Json(body));
    if !matches!(source.as_str(), "kafka" | "agent" | "webhook") {
        return reply(StatusCode::NOT_FOUND, json!({"error": "unknown source"}));
    }
    let is_pubsub = source != "webhook";
    // Pub/sub deliveries come from the Dapr sidecar in this pod, over
    // loopback; the Service can reach only the webhook route.
    if is_pubsub && !peer.ip().is_loopback() {
        return reply(
            StatusCode::FORBIDDEN,
            json!({"error": "pub/sub routes accept only the local Dapr sidecar"}),
        );
    }
    if !is_pubsub && !webhook_authorized(&state, &headers) {
        return reply(
            StatusCode::UNAUTHORIZED,
            json!({"error": "a valid webhook bearer token is required"}),
        );
    }
    answer(is_pubsub, deliver(&state, &headers, body).await)
}

/// How a delivery is answered: Dapr's `SUCCESS`, `RETRY` and `DROP` for
/// pub/sub; status codes for a webhook.
fn answer(is_pubsub: bool, delivery: Delivery) -> (StatusCode, HeaderMap, Json<Value>) {
    let reply = |status, body: Value| (status, HeaderMap::new(), Json(body));
    match delivery {
        Delivery::Stored { .. } if is_pubsub => reply(StatusCode::OK, json!({"status": "SUCCESS"})),
        Delivery::Stored {
            duplicate,
            partition,
            offset,
        } => reply(
            StatusCode::ACCEPTED,
            json!({"duplicate": duplicate, "partition": partition, "offset": offset}),
        ),
        Delivery::Invalid(error) if is_pubsub => {
            reply(StatusCode::OK, json!({"status": "DROP", "error": error}))
        }
        Delivery::Invalid(error) if error.starts_with("send a CloudEvent") => {
            reply(StatusCode::UNSUPPORTED_MEDIA_TYPE, json!({"error": error}))
        }
        Delivery::Invalid(error) => reply(StatusCode::BAD_REQUEST, json!({"error": error})),
        Delivery::InFlight { .. } if is_pubsub => reply(
            StatusCode::OK,
            json!({"status": "RETRY", "error": "the event is being appended by another request"}),
        ),
        Delivery::InFlight { retry_after_ms } => {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::RETRY_AFTER,
                retry_after_ms.div_ceil(1000).max(1).into(),
            );
            (
                StatusCode::CONFLICT,
                headers,
                Json(json!({"error": "the event is being appended by another request"})),
            )
        }
        Delivery::Failed(error) if is_pubsub => {
            reply(StatusCode::OK, json!({"status": "RETRY", "error": error}))
        }
        Delivery::Failed(error) => reply(StatusCode::SERVICE_UNAVAILABLE, json!({"error": error})),
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
        stream_app_id: env_or("OPERON_STREAM_APP_ID", "operon-stream"),
        namespace: env_or("OPERON_NAMESPACE", "default"),
        trigger_stream: env_or("OPERON_TRIGGER_STREAM", "workflow-triggers"),
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

#[cfg(test)]
mod tests {
    use axum::http::HeaderValue;

    use super::*;

    fn headers(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.insert(*name, HeaderValue::from_static(value));
        }
        map
    }

    #[test]
    fn a_structured_event_goes_through_as_the_json_it_arrived_as() {
        let body =
            Bytes::from_static(br#"{"specversion":"1.0","id":"1","source":"/s","type":"t"}"#);
        let map = headers(&[(
            "content-type",
            "application/cloudevents+json; charset=utf-8",
        )]);
        let Ok(Events::JsonBatch(batch)) = events_of(&map, body.clone()) else {
            panic!("expected a JSON batch");
        };
        assert_eq!(batch, [b"[", &body[..], b"]"].concat());
    }

    #[test]
    fn a_binary_mode_event_becomes_a_protobuf_event() {
        let map = headers(&[
            ("ce-specversion", "1.0"),
            ("ce-id", "a%20b"),
            ("ce-source", "/s"),
            ("ce-type", "t"),
            ("ce-partitionkey", "k"),
            ("content-type", "text/plain"),
        ]);
        let Ok(Events::Batch(batch)) = events_of(&map, Bytes::from_static(b"hi")) else {
            panic!("expected a protobuf batch");
        };
        let event = &batch.events[0];
        assert_eq!(
            (event.id.as_str(), event.spec_version.as_str()),
            ("a b", "1.0")
        );
        assert_eq!(
            event.attributes.len(),
            2,
            "partitionkey and datacontenttype"
        );
        assert_eq!(event.data, Some(Data::BinaryData(b"hi".to_vec())));
    }

    #[test]
    fn a_request_that_is_not_a_cloudevent_is_refused() {
        let map = headers(&[("content-type", "application/json")]);
        assert!(events_of(&map, Bytes::from_static(b"{}")).is_err());
        assert!(percent_decode(b"%zz").is_err());
    }

    #[test]
    fn deliveries_are_answered_as_dapr_and_webhooks_expect() {
        let stored = Delivery::Stored {
            duplicate: true,
            partition: 0,
            offset: 3,
        };
        assert_eq!(answer(true, stored).2.0, json!({"status": "SUCCESS"}));
        let (status, headers, _) = answer(
            false,
            Delivery::InFlight {
                retry_after_ms: 1500,
            },
        );
        assert_eq!(status, StatusCode::CONFLICT);
        assert_eq!(headers[header::RETRY_AFTER], "2");
        let failed = answer(true, Delivery::Failed("x".into()));
        assert_eq!(failed.2.0["status"], "RETRY");
        let invalid = answer(true, Delivery::Invalid("x".into()));
        assert_eq!(invalid.2.0["status"], "DROP");
        assert_eq!(
            answer(false, Delivery::Invalid("send a CloudEvent: x".into())).0,
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
    }
}
