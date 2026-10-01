//! CloudEvents on the native HTTP API (design §02 §7.4, D270): ingest in
//! binary, structured and batched mode, deduplication by `source` + `id`,
//! and consume as events.

use std::net::SocketAddr;
use std::time::Duration;

use loams::{Server, ServerConfig};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tempfile::TempDir;

const STRUCTURED: &str = "application/cloudevents+json";
const BATCH: &str = "application/cloudevents-batch+json";

fn config(dir: &TempDir) -> ServerConfig {
    let mut config = ServerConfig::new(dir.path());
    config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.log.flush_interval = Duration::from_millis(20);
    config.worker_poll_interval = Duration::from_millis(50);
    config
}

struct Api {
    base: String,
    http: reqwest::Client,
}

impl Api {
    async fn start(dir: &TempDir, partitions: u32) -> (Server, Self) {
        let server = Server::start(config(dir)).await.expect("start");
        let api = Self {
            base: format!("http://{}", server.local_addr()),
            http: reqwest::Client::new(),
        };
        let created = api
            .http
            .post(format!("{}/v1/namespaces", api.base))
            .json(&json!({"name": "acme"}))
            .send()
            .await
            .expect("send");
        assert_eq!(created.status(), StatusCode::CREATED);
        let created = api
            .http
            .post(format!("{}/v1/namespaces/acme/streams", api.base))
            .json(&json!({"name": "events", "partitions": partitions}))
            .send()
            .await
            .expect("send");
        assert_eq!(created.status(), StatusCode::CREATED);
        (server, api)
    }

    fn events_url(&self, query: &str) -> String {
        format!(
            "{}/v1/namespaces/acme/streams/events/events{query}",
            self.base
        )
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
    ) -> (StatusCode, reqwest::header::HeaderMap, Vec<u8>) {
        let response = request.send().await.expect("send");
        let (status, headers) = (response.status(), response.headers().clone());
        (
            status,
            headers,
            response.bytes().await.expect("body").to_vec(),
        )
    }

    async fn post_structured(&self, query: &str, event: &Value) -> (StatusCode, Value) {
        let (status, _, body) = self
            .send(
                self.http
                    .post(self.events_url(query))
                    .header("content-type", STRUCTURED)
                    .body(event.to_string()),
            )
            .await;
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }

    async fn post_batch(&self, query: &str, events: &Value) -> (StatusCode, Value) {
        let (status, _, body) = self
            .send(
                self.http
                    .post(self.events_url(query))
                    .header("content-type", BATCH)
                    .body(events.to_string()),
            )
            .await;
        (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
    }

    async fn consume(
        &self,
        partition: u32,
        offset: u64,
        mode: &str,
    ) -> (StatusCode, reqwest::header::HeaderMap, Vec<u8>) {
        self.send(self.http.get(format!(
            "{}/v1/namespaces/acme/streams/events/partitions/{partition}/events?offset={offset}&mode={mode}",
            self.base
        )))
        .await
    }
}

fn event(id: &str) -> Value {
    json!({
        "specversion": "1.0",
        "id": id,
        "source": "/orders",
        "type": "order.created",
        "subject": "order-7",
        "time": "2026-09-30T10:00:00.250Z",
        "datacontenttype": "application/json",
        "data": {"total": 12},
    })
}

#[tokio::test]
async fn a_structured_event_is_appended_once_and_read_back_byte_for_byte() {
    let dir = TempDir::new().unwrap();
    let (_server, api) = Api::start(&dir, 3).await;

    let (status, body) = api.post_structured("", &event("a-1")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"][0]["status"], "appended");
    let partition = body["events"][0]["partition"].as_u64().unwrap() as u32;
    let offset = body["events"][0]["offset"].as_u64().unwrap();

    // A retry answers with the first write's location and appends nothing.
    let (status, again) = api.post_structured("", &event("a-1")).await;
    assert_eq!(status, StatusCode::OK, "{again}");
    assert_eq!(again["events"][0]["status"], "duplicate");
    assert_eq!(again["events"][0]["partition"], partition);
    assert_eq!(again["events"][0]["offset"], offset);

    let (status, headers, bytes) = api.consume(partition, offset, "structured").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], BATCH);
    assert_eq!(
        headers["loams-next-offset"],
        (offset + 1).to_string().as_str()
    );
    let events: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(events, vec![event("a-1")], "one record, one event");
}

#[tokio::test]
async fn binary_mode_maps_headers_and_serves_binary_mode_back() {
    let dir = TempDir::new().unwrap();
    let (_server, api) = Api::start(&dir, 1).await;
    let send = || {
        api.http
            .post(api.events_url(""))
            .header("ce-specversion", "1.0")
            .header("ce-id", "b-1")
            .header("ce-source", "/sensors/a%20b")
            .header("ce-type", "reading")
            .header("ce-partitionkey", "sensor-1")
            .header("ce-time", "2026-09-30T10:00:00Z")
            .header("content-type", "text/plain")
            .body("21.5")
    };
    let (status, _, body) = api.send(send()).await;
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"][0]["status"], "appended");
    assert_eq!(body["events"][0]["offset"], 0);

    let (_, _, body) = api.send(send()).await;
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["events"][0]["status"], "duplicate");

    // The record is stored as the Kafka binding's binary mode.
    let (status, headers, bytes) = api.consume(0, 0, "binary").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, b"21.5");
    assert_eq!(headers["ce-id"], "b-1");
    assert_eq!(headers["ce-source"], "/sensors/a%20b");
    assert_eq!(headers["ce-partitionkey"], "sensor-1");
    assert_eq!(headers["content-type"], "text/plain");
    assert_eq!(headers["loams-offset"], "0");
    assert_eq!(headers["loams-next-offset"], "1");

    let (status, _, _) = api.consume(0, 1, "binary").await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn a_batch_is_deduplicated_within_and_across_requests() {
    let dir = TempDir::new().unwrap();
    let (_server, api) = Api::start(&dir, 1).await;
    let batch = json!([event("c-1"), event("c-2"), event("c-1")]);
    let (status, body) = api.post_batch("", &batch).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let statuses: Vec<&str> = body["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["status"].as_str().unwrap())
        .collect();
    assert_eq!(statuses, ["appended", "appended", "duplicate"]);
    // The repeat answers with its first occurrence's offset.
    assert_eq!(body["events"][2]["offset"], body["events"][0]["offset"]);

    let (_, body) = api
        .post_batch("", &json!([event("c-2"), event("c-3")]))
        .await;
    assert_eq!(body["events"][0]["status"], "duplicate");
    assert_eq!(body["events"][1]["status"], "appended");

    let (_, _, bytes) = api.consume(0, 0, "structured").await;
    let events: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
    let ids: Vec<&str> = events.iter().map(|e| e["id"].as_str().unwrap()).collect();
    assert_eq!(ids, ["c-1", "c-2", "c-3"]);
}

#[tokio::test]
async fn an_invalid_event_refuses_the_whole_batch_with_its_index() {
    let dir = TempDir::new().unwrap();
    let (_server, api) = Api::start(&dir, 1).await;
    let mut bad = event("d-2");
    bad["specversion"] = json!("0.3");
    let (status, body) = api.post_batch("", &json!([event("d-1"), bad])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["message"].as_str().unwrap().contains("event 1"),
        "{body}"
    );
    // Nothing was written, and the good event's key was never claimed.
    let (status, body) = api.post_batch("", &json!([event("d-1")])).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"][0]["offset"], 0);
    assert_eq!(body["events"][0]["status"], "appended");
}

#[tokio::test]
async fn a_request_that_is_not_a_cloudevent_is_415() {
    let dir = TempDir::new().unwrap();
    let (_server, api) = Api::start(&dir, 1).await;
    let (status, _, _) = api
        .send(
            api.http
                .post(api.events_url(""))
                .header("content-type", "application/json")
                .body("{}"),
        )
        .await;
    assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
}

#[tokio::test]
async fn the_partition_parameter_and_a_keyless_event_land_deterministically() {
    let dir = TempDir::new().unwrap();
    let (_server, api) = Api::start(&dir, 4).await;
    let (status, body) = api.post_structured("?partition=2", &event("e-1")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["events"][0]["partition"], 2);
    let (status, _) = api.post_structured("?partition=9", &event("e-2")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // No subject or partitionkey: the idempotency key picks the partition,
    // so a retry of an unacknowledged request would land in the same place.
    let mut keyless = event("e-3");
    keyless.as_object_mut().unwrap().remove("subject");
    let (_, first) = api.post_structured("", &keyless).await;
    assert_eq!(first["events"][0]["status"], "appended");
    let (_, second) = api.post_structured("", &keyless).await;
    assert_eq!(second["events"][0]["status"], "duplicate");
    assert_eq!(
        second["events"][0]["partition"],
        first["events"][0]["partition"]
    );
}

#[tokio::test]
async fn a_record_that_is_not_an_event_is_read_as_a_synthesized_envelope() {
    let dir = TempDir::new().unwrap();
    let (_server, api) = Api::start(&dir, 1).await;
    let produced = api
        .http
        .post(format!(
            "{}/v1/namespaces/acme/streams/events/partitions/0/records",
            api.base
        ))
        .json(&json!({"records": [{"value": "eyJhIjoxfQ=="}]}))
        .send()
        .await
        .unwrap();
    assert_eq!(produced.status(), StatusCode::OK);
    let (_, _, bytes) = api.consume(0, 0, "structured").await;
    let events: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["type"], "io.loams.dev.stream.record");
    assert_eq!(
        events[0]["source"],
        "/namespaces/acme/streams/events/partitions/0"
    );
    assert_eq!(events[0]["data"], json!({"a": 1}));
}

#[tokio::test]
async fn a_dedup_window_over_24_hours_is_refused() {
    let dir = TempDir::new().unwrap();
    let mut config = config(&dir);
    config.cloudevents.dedup_window = Duration::from_secs(86_401);
    assert!(Server::start(config).await.is_err());
}

#[tokio::test]
async fn an_event_another_request_is_appending_is_409_with_retry_after() {
    use loams_common::meta::{Consistency, IdempotencyClaim};
    let dir = TempDir::new().unwrap();
    let (server, api) = Api::start(&dir, 1).await;
    let meta = server.meta_store();
    let ns = meta
        .namespace_by_name(Consistency::Local, "acme")
        .await
        .unwrap()
        .unwrap();
    let stream = meta
        .stream_by_name(Consistency::Local, ns.id, "events")
        .await
        .unwrap()
        .unwrap();
    let parsed = loams_cloudevents::json::parse_event(event("f-1").to_string().as_bytes()).unwrap();
    meta.claim_idempotency_keys(IdempotencyClaim {
        stream: stream.id,
        owner: "another-request".to_string(),
        keys: vec![parsed.dedup_key()],
        ttl_ms: 60_000,
    })
    .await
    .unwrap();

    let (status, headers, body) = api
        .send(
            api.http
                .post(api.events_url(""))
                .header("content-type", STRUCTURED)
                .body(event("f-1").to_string()),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    assert!(headers.contains_key("retry-after"));
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["events"][0]["status"], "in_flight");
    // The rest of the request is not held up by it.
    let (status, _, body) = api
        .send(
            api.http
                .post(api.events_url(""))
                .header("content-type", BATCH)
                .body(json!([event("f-1"), event("f-2")]).to_string()),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["events"][0]["status"], "in_flight");
    assert_eq!(body["events"][1]["status"], "appended");
}

#[tokio::test]
async fn plain_produce_with_ce_headers_is_stored_as_sent_and_read_back_as_an_event() {
    let dir = TempDir::new().unwrap();
    let (_server, api) = Api::start(&dir, 1).await;
    let record = json!({"records": [{
        "key": "b3JkZXItNw==",
        "value": "eyJ0b3RhbCI6MTJ9",
        "headers": [
            {"key": "ce_specversion", "value": "MS4w"},
            {"key": "ce_id", "value": "Zy0x"},
            {"key": "ce_source", "value": "L2thZmth"},
            {"key": "ce_type", "value": "dGVzdA=="},
        ],
    }]});
    for _ in 0..2 {
        let produced = api
            .http
            .post(format!(
                "{}/v1/namespaces/acme/streams/events/partitions/0/records",
                api.base
            ))
            .json(&record)
            .send()
            .await
            .unwrap();
        assert_eq!(produced.status(), StatusCode::OK);
    }
    let (_, _, bytes) = api.consume(0, 0, "structured").await;
    let events: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
    // Not deduplicated: both records are there.
    assert_eq!(events.len(), 2);
    assert_eq!(events[0]["id"], "g-1");
    assert_eq!(events[0]["source"], "/kafka");
    assert_eq!(events[0]["data"], json!({"total": 12}));
}
