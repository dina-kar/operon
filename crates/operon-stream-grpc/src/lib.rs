//! The native stream gRPC endpoint. Protocol adapters send through Dapr's
//! gRPC service invocation; this service reuses the HTTP/Flight produce path.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use operon_log::Record;
use operon_query::ServiceError;
use operon_query::flight_ingest::StreamProducer;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};

pub mod proto {
    tonic::include_proto!("loam.stream.v1");
}

use proto::stream_service_server::{StreamService, StreamServiceServer};

#[derive(Clone)]
struct NativeStreams {
    producer: Arc<dyn StreamProducer>,
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
    stop: CancellationToken,
) -> Result<(), tonic::transport::Error> {
    tonic::transport::Server::builder()
        .add_service(StreamServiceServer::new(NativeStreams { producer }))
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

    use operon_common::StreamId;
    use operon_log::AppendAck;

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
        let server = tokio::spawn(serve(listener, recorder.clone(), stop.clone()));
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
}
