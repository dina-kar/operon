//! The native stream gRPC endpoint. Protocol adapters send through Dapr's
//! gRPC service invocation; this service reuses the HTTP/Flight produce path.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use loams_log::Record;
use loams_query::ServiceError;
use loams_query::flight_ingest::StreamProducer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

pub mod events;

// The generated code names the CloudEvents package by relative paths, so
// both packages live at their own module paths, as prost expects.
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

/// `loams.stream.v1`.
pub use generated::loams::stream::v1 as proto;

pub use events::EventProducer;
use proto::produce_cloud_events_request::Events;
use proto::stream_service_server::{StreamService, StreamServiceServer};

#[derive(Clone)]
struct NativeStreams {
    producer: Arc<dyn StreamProducer>,
    events: Arc<dyn EventProducer>,
}

#[tonic::async_trait]
impl StreamService for NativeStreams {
    async fn produce(
        &self,
        request: Request<proto::ProduceRequest>,
    ) -> Result<Response<proto::ProduceResponse>, Status> {
        let request = request.into_inner();
        if request.namespace.is_empty() || request.stream.is_empty() {
            return Err(Status::invalid_argument(
                "namespace and stream are required",
            ));
        }
        if request.records.is_empty() {
            return Err(Status::invalid_argument("records must not be empty"));
        }
        let records = request
            .records
            .into_iter()
            .map(|record| Record {
                key: record.key.map(Bytes::from),
                value: record.value.map(Bytes::from),
                headers: record
                    .headers
                    .into_iter()
                    .map(|header| (header.key, header.value.map(Bytes::from)))
                    .collect(),
                timestamp_ms: record.timestamp_ms,
            })
            .collect();
        let acks = self
            .producer
            .produce(
                &request.namespace,
                &request.stream,
                vec![(request.partition, records)],
            )
            .await
            .map_err(status)?;
        let ack = acks
            .into_iter()
            .next()
            .ok_or_else(|| Status::internal("stream writer returned no acknowledgement"))?;
        Ok(Response::new(proto::ProduceResponse {
            stream_id: ack.stream.0,
            base_offset: ack.base_offset,
            last_offset: ack.last_offset,
        }))
    }

    async fn produce_cloud_events(
        &self,
        request: Request<proto::ProduceCloudEventsRequest>,
    ) -> Result<Response<proto::ProduceCloudEventsResponse>, Status> {
        let request = request.into_inner();
        if request.namespace.is_empty() || request.stream.is_empty() {
            return Err(Status::invalid_argument(
                "namespace and stream are required",
            ));
        }
        let events = match &request.events {
            Some(Events::Batch(batch)) => events::from_proto_batch(batch),
            Some(Events::JsonBatch(body)) => events::from_json_batch(body),
            None => Err("a batch of events is required".to_string()),
        }
        .map_err(|message| Status::invalid_argument(format!("invalid CloudEvent: {message}")))?;
        if events.is_empty() {
            return Err(Status::invalid_argument("events must not be empty"));
        }
        let results = self
            .events
            .produce_events(
                &request.namespace,
                &request.stream,
                request.partition,
                events,
            )
            .await
            .map_err(status)?;
        Ok(Response::new(proto::ProduceCloudEventsResponse { results }))
    }
}

fn status(error: ServiceError) -> Status {
    match error {
        ServiceError::NotFound { .. } => Status::not_found(error.to_string()),
        ServiceError::AlreadyExists(_) => Status::already_exists(error.to_string()),
        ServiceError::InvalidArgument(_) | ServiceError::SchemaViolation { .. } => {
            Status::invalid_argument(error.to_string())
        }
        ServiceError::Unavailable(_) => Status::unavailable(error.to_string()),
        ServiceError::Timeout => Status::deadline_exceeded(error.to_string()),
        ServiceError::Internal(_) => Status::internal(error.to_string()),
        ServiceError::ResourceExhausted { .. } => Status::resource_exhausted(error.to_string()),
    }
}

/// Serve the native stream API on an already-bound listener until shutdown.
pub async fn serve(
    listener: TcpListener,
    producer: Arc<dyn StreamProducer>,
    events: Arc<dyn EventProducer>,
    stop: CancellationToken,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(StreamServiceServer::new(NativeStreams { producer, events }))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), stop.cancelled_owned())
        .await
}

/// Binds the stream listener. It has no authentication yet, so only
/// loopback addresses are served (D111): an adapter reaches it through a
/// sidecar in the same pod, such as Dapr's gRPC service invocation.
pub async fn bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    if !addr.ip().is_loopback() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "only loopback addresses are served until the unified auth plan (D111)",
        ));
    }
    TcpListener::bind(addr).await
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use loams_common::StreamId;
    use loams_log::AppendAck;

    use super::proto::stream_service_client::StreamServiceClient;
    use super::*;

    /// One `produce` call: namespace, stream and the partitions' records.
    type Call = (String, String, Vec<(u32, Vec<Record>)>);

    /// Records what it was asked to produce and acknowledges every record.
    #[derive(Debug, Default)]
    struct Recorder {
        calls: Mutex<Vec<Call>>,
    }

    #[async_trait::async_trait]
    impl StreamProducer for Recorder {
        async fn partitions(&self, _ns: &str, _stream: &str) -> Result<u32, ServiceError> {
            Ok(1)
        }

        async fn produce(
            &self,
            ns: &str,
            stream: &str,
            records: Vec<(u32, Vec<Record>)>,
        ) -> Result<Vec<AppendAck>, ServiceError> {
            if stream == "missing" {
                return Err(ServiceError::NotFound {
                    kind: "stream",
                    name: stream.to_string(),
                });
            }
            let acks = records
                .iter()
                .map(|(partition, batch)| AppendAck {
                    stream: StreamId(7),
                    partition: *partition,
                    base_offset: 10,
                    last_offset: 10 + batch.len() as u64 - 1,
                })
                .collect();
            self.calls
                .lock()
                .unwrap()
                .push((ns.to_string(), stream.to_string(), records));
            Ok(acks)
        }
    }

    #[async_trait::async_trait]
    impl EventProducer for Recorder {
        async fn produce_events(
            &self,
            _ns: &str,
            stream: &str,
            partition: Option<u32>,
            events: Vec<loams_cloudevents::CloudEvent>,
        ) -> Result<Vec<proto::EventResult>, ServiceError> {
            if stream == "missing" {
                return Err(ServiceError::NotFound {
                    kind: "stream",
                    name: stream.to_string(),
                });
            }
            Ok(events
                .iter()
                .enumerate()
                .map(|(i, _)| proto::EventResult {
                    status: proto::EventStatus::Appended.into(),
                    partition: partition.unwrap_or(0),
                    offset: i as u64,
                    retry_after_ms: 0,
                })
                .collect())
        }
    }

    fn request(stream: &str, records: usize) -> proto::ProduceRequest {
        proto::ProduceRequest {
            namespace: "default".into(),
            stream: stream.into(),
            partition: 0,
            records: (0..records)
                .map(|i| proto::Record {
                    key: Some(vec![i as u8]),
                    value: Some(b"v".to_vec()),
                    headers: vec![proto::Header {
                        key: "h".into(),
                        value: None,
                    }],
                    timestamp_ms: -1,
                })
                .collect(),
        }
    }

    #[tokio::test]
    async fn listener_refuses_non_loopback() {
        let err = bind("0.0.0.0:0".parse().unwrap()).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[tokio::test]
    async fn produce_appends_through_the_producer() {
        let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let recorder = Arc::new(Recorder::default());
        let stop = CancellationToken::new();
        let server = tokio::spawn(serve(
            listener,
            recorder.clone(),
            recorder.clone(),
            stop.clone(),
        ));
        let mut client = StreamServiceClient::connect(format!("http://{addr}"))
            .await
            .unwrap();

        let ack = client
            .produce(request("events", 2))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            (ack.stream_id, ack.base_offset, ack.last_offset),
            (7, 10, 11)
        );
        {
            let calls = recorder.calls.lock().unwrap();
            let (ns, stream, batches) = &calls[0];
            assert_eq!((ns.as_str(), stream.as_str()), ("default", "events"));
            assert_eq!(batches[0].1.len(), 2);
            assert_eq!(batches[0].1[0].timestamp_ms, -1);
            assert_eq!(batches[0].1[0].headers, vec![("h".to_string(), None)]);
        }

        let empty = client.produce(request("events", 0)).await.unwrap_err();
        assert_eq!(empty.code(), tonic::Code::InvalidArgument);
        let unnamed = client.produce(request("", 1)).await.unwrap_err();
        assert_eq!(unnamed.code(), tonic::Code::InvalidArgument);
        let missing = client.produce(request("missing", 1)).await.unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);

        stop.cancel();
        server.await.unwrap().unwrap();
    }

    fn cloud_event(id: &str) -> events::cloudevents::CloudEvent {
        use events::cloudevents::cloud_event::cloud_event_attribute_value::Attr;
        use events::cloudevents::cloud_event::{CloudEventAttributeValue, Data};
        let attr = |attr| CloudEventAttributeValue { attr: Some(attr) };
        events::cloudevents::CloudEvent {
            id: id.into(),
            source: "/orders".into(),
            spec_version: "1.0".into(),
            r#type: "order.created".into(),
            attributes: [
                (
                    "subject".to_string(),
                    attr(Attr::CeString("order-7".into())),
                ),
                (
                    "time".to_string(),
                    attr(Attr::CeTimestamp(prost_types::Timestamp {
                        seconds: 1_790_762_400,
                        nanos: 250_000_000,
                    })),
                ),
                ("retries".to_string(), attr(Attr::CeInteger(3))),
                ("urgent".to_string(), attr(Attr::CeBoolean(true))),
                ("blob".to_string(), attr(Attr::CeBytes(vec![1, 2, 3]))),
                (
                    "datacontenttype".to_string(),
                    attr(Attr::CeString("application/json".into())),
                ),
            ]
            .into_iter()
            .collect(),
            data: Some(Data::TextData("{\"total\":12}".into())),
        }
    }

    #[test]
    fn a_protobuf_event_round_trips_through_the_codec() {
        let proto = cloud_event("p-1");
        let event = events::from_proto(&proto).unwrap();
        assert_eq!(event.attr("time"), Some("2026-09-30T10:00:00.25Z"));
        assert_eq!(event.attr("retries"), Some("3"));
        assert_eq!(event.attr("blob"), Some("AQID"));
        assert_eq!(event.data().map(|d| &d[..]), Some(&b"{\"total\":12}"[..]));
        assert_eq!(events::to_proto(&event), proto);
        // The same event through the JSON format keeps its extension types.
        let json = loams_cloudevents::json::write_event(&event);
        let again = loams_cloudevents::json::parse_event(&json).unwrap();
        assert_eq!(again.attr("retries"), Some("3"));
    }

    #[test]
    fn proto_data_and_untyped_attributes_are_refused() {
        use events::cloudevents::cloud_event::{CloudEventAttributeValue, Data};
        let mut any = cloud_event("p-2");
        any.data = Some(Data::ProtoData(prost_types::Any::default()));
        assert!(events::from_proto(&any).is_err());
        let mut empty = cloud_event("p-3");
        empty
            .attributes
            .insert("x".into(), CloudEventAttributeValue { attr: None });
        assert!(events::from_proto(&empty).is_err());
        let mut bad = cloud_event("p-4");
        bad.spec_version = "0.3".into();
        assert!(events::from_proto(&bad).is_err());
    }

    #[tokio::test]
    async fn produce_cloud_events_answers_per_event() {
        let listener = bind("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let recorder = Arc::new(Recorder::default());
        let stop = CancellationToken::new();
        let server = tokio::spawn(serve(
            listener,
            recorder.clone(),
            recorder.clone(),
            stop.clone(),
        ));
        let mut client = StreamServiceClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let request =
            |stream: &str, events: Option<proto::produce_cloud_events_request::Events>| {
                proto::ProduceCloudEventsRequest {
                    namespace: "default".into(),
                    stream: stream.into(),
                    partition: Some(1),
                    events,
                }
            };
        let batch = events::cloudevents::CloudEventBatch {
            events: vec![cloud_event("g-1"), cloud_event("g-2")],
        };
        let answer = client
            .produce_cloud_events(request(
                "events",
                Some(proto::produce_cloud_events_request::Events::Batch(batch)),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(answer.results.len(), 2);
        assert_eq!(answer.results[1].offset, 1);
        assert_eq!(answer.results[0].partition, 1);

        // The JSON batch is read by the same codec as HTTP's.
        let json = br#"[{"specversion":"1.0","id":"j-1","source":"/s","type":"t"}]"#;
        let answer = client
            .produce_cloud_events(request(
                "events",
                Some(proto::produce_cloud_events_request::Events::JsonBatch(
                    json.to_vec(),
                )),
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(answer.results.len(), 1);

        let invalid = br#"[{"specversion":"1.0","id":"j-1","source":"/s","type":"t"},{"id":"x"}]"#;
        let err = client
            .produce_cloud_events(request(
                "events",
                Some(proto::produce_cloud_events_request::Events::JsonBatch(
                    invalid.to_vec(),
                )),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("event 1"), "{err}");
        let none = client
            .produce_cloud_events(request("events", None))
            .await
            .unwrap_err();
        assert_eq!(none.code(), tonic::Code::InvalidArgument);
        let missing = client
            .produce_cloud_events(request(
                "missing",
                Some(proto::produce_cloud_events_request::Events::JsonBatch(
                    json.to_vec(),
                )),
            ))
            .await
            .unwrap_err();
        assert_eq!(missing.code(), tonic::Code::NotFound);

        stop.cancel();
        server.await.unwrap().unwrap();
    }
}
