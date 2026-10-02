//! `StreamService.ProduceCloudEvents` against a server started in-process
//! (design §02 §7.4, D270): the protobuf format, the JSON pass-through,
//! and the ledger shared with the HTTP route.
#![cfg(feature = "stream-grpc")]

use std::net::SocketAddr;
use std::time::Duration;

use loams::{Server, ServerConfig};
use loams_stream_grpc::events::cloudevents::cloud_event::cloud_event_attribute_value::Attr;
use loams_stream_grpc::events::cloudevents::cloud_event::{CloudEventAttributeValue, Data};
use loams_stream_grpc::events::cloudevents::{CloudEvent, CloudEventBatch};
use loams_stream_grpc::proto::produce_cloud_events_request::Events;
use loams_stream_grpc::proto::stream_service_client::StreamServiceClient;
use loams_stream_grpc::proto::{EventStatus, ProduceCloudEventsRequest};
use serde_json::{Value, json};
use tempfile::TempDir;

fn event(id: &str) -> CloudEvent {
    let string = |s: &str| CloudEventAttributeValue {
        attr: Some(Attr::CeString(s.into())),
    };
    CloudEvent {
        id: id.into(),
        source: "/orders".into(),
        spec_version: "1.0".into(),
        r#type: "order.created".into(),
        attributes: [
            ("subject".to_string(), string("order-7")),
            (
                "retries".to_string(),
                CloudEventAttributeValue {
                    attr: Some(Attr::CeInteger(3)),
                },
            ),
        ]
        .into_iter()
        .collect(),
        data: Some(Data::BinaryData(vec![1, 2, 3])),
    }
}

fn request(events: Events) -> ProduceCloudEventsRequest {
    ProduceCloudEventsRequest {
        namespace: "acme".into(),
        stream: "events".into(),
        partition: None,
        events: Some(events),
    }
}

#[tokio::test]
async fn cloudevents_over_grpc_share_the_ledger_with_http() {
    let dir = TempDir::new().unwrap();
    let mut config = ServerConfig::new(dir.path());
    config.listen = SocketAddr::from(([127, 0, 0, 1], 0));
    config.stream_grpc = Some(SocketAddr::from(([127, 0, 0, 1], 0)));
    config.log.flush_interval = Duration::from_millis(20);
    let server = Server::start(config).await.unwrap();
    let http = reqwest::Client::new();
    let base = format!("http://{}", server.local_addr());
    for (path, body) in [
        ("/v1/namespaces", json!({"name": "acme"})),
        (
            "/v1/namespaces/acme/streams",
            json!({"name": "events", "partitions": 1}),
        ),
    ] {
        let created = http
            .post(format!("{base}{path}"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(created.status().is_success());
    }
    let mut client = StreamServiceClient::connect(format!(
        "http://{}",
        server.stream_grpc_addr().expect("stream gRPC address")
    ))
    .await
    .unwrap();

    let batch = CloudEventBatch {
        events: vec![event("g-1"), event("g-2"), event("g-1")],
    };
    let answer = client
        .produce_cloud_events(request(Events::Batch(batch.clone())))
        .await
        .unwrap()
        .into_inner();
    let statuses: Vec<i32> = answer.results.iter().map(|r| r.status).collect();
    assert_eq!(
        statuses,
        [
            EventStatus::Appended as i32,
            EventStatus::Appended as i32,
            EventStatus::Duplicate as i32
        ]
    );
    assert_eq!(answer.results[2].offset, answer.results[0].offset);

    // A retry of the whole request appends nothing.
    let again = client
        .produce_cloud_events(request(Events::Batch(batch)))
        .await
        .unwrap()
        .into_inner();
    assert!(
        again
            .results
            .iter()
            .all(|r| r.status == EventStatus::Duplicate as i32)
    );

    // The JSON pass-through uses the same ledger: g-2 is a duplicate, j-1 new.
    let json_batch = br#"[{"specversion":"1.0","id":"g-2","source":"/orders","type":"order.created"},{"specversion":"1.0","id":"j-1","source":"/orders","type":"order.created"}]"#;
    let answer = client
        .produce_cloud_events(request(Events::JsonBatch(json_batch.to_vec())))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(answer.results[0].status, EventStatus::Duplicate as i32);
    assert_eq!(answer.results[1].status, EventStatus::Appended as i32);

    // HTTP sees the same events: g-1 is a duplicate there too.
    let posted = http
        .post(format!("{base}/v1/namespaces/acme/streams/events/events"))
        .header("content-type", "application/cloudevents+json")
        .body(
            json!({"specversion": "1.0", "id": "g-1", "source": "/orders", "type": "order.created"})
                .to_string(),
        )
        .send()
        .await
        .unwrap();
    let body: Value = posted.json().await.unwrap();
    assert_eq!(body["events"][0]["status"], "duplicate");

    // The typed extension and binary data survive the protobuf path.
    let fetched = http
        .get(format!(
            "{base}/v1/namespaces/acme/streams/events/partitions/0/events?offset=0"
        ))
        .send()
        .await
        .unwrap();
    let events: Vec<Value> = fetched.json().await.unwrap();
    assert_eq!(events[0]["retries"], 3);
    assert_eq!(events[0]["data_base64"], "AQID");

    let invalid = client
        .produce_cloud_events(request(Events::JsonBatch(b"[{}]".to_vec())))
        .await
        .unwrap_err();
    assert_eq!(invalid.code(), tonic::Code::InvalidArgument);
}
