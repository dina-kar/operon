//! Arrow Flight SQL over collections (plan M1.2 Task 12): read-only
//! statements through the SQL catalog of Task 10, the catalog metadata
//! commands, and the self-contained statement tickets any node can serve.
//!
//! - The namespace of a request is its `operon-namespace` metadata
//!   ([`NAMESPACE_METADATA`], `default` when absent).
//! - Its consistency is `AtLeast(token)` when `operon-consistency-token`
//!   parses as a token, else `Strong`.
//! - `operon-hot` goes through [`HotLayer`] on the tonic server.
//! - Statements plan through [`plan_read_only`], which refreshes the
//!   namespace's catalog first, so a collection created just before a
//!   statement is visible to it.

use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{SqlInfoData, SqlInfoDataBuilder};
use arrow_flight::sql::server::{DoPutError, FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    Any, CommandGetCatalogs, CommandGetDbSchemas, CommandGetSqlInfo, CommandGetTableTypes,
    CommandGetTables, CommandStatementIngest, CommandStatementQuery, ProstMessageExt, SqlInfo,
    SqlSupportedTransaction, TicketStatementQuery,
};
use arrow_flight::{
    FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    Ticket,
};
use bytes::Bytes;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use futures::{Stream, StreamExt, TryStreamExt};
use operon_collection::{ConsistencyToken, MAX_WRITE_OPS};
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use tonic::{Request, Response, Status, Streaming};

use crate::error::ServiceError;
use crate::flight_ingest::{
    Acks, ID_TYPE_METADATA, IngestAction, Put, PutTarget, Sink, StreamProducer, ingest_action,
    put_target_from_ingest, put_target_from_path,
};
use crate::hot::HotLayer;
use crate::ir::ReadConsistency;
use crate::scan::PIN_MANIFEST_METADATA;
use crate::service::CollectionService;
use crate::sql::{COLLECTIONS_SCHEMA, collection_arrow_schema, execute_read_only, plan_read_only};

/// The metadata key naming a request's namespace.
pub const NAMESPACE_METADATA: &str = "operon-namespace";
/// The namespace of a request without [`NAMESPACE_METADATA`].
pub const DEFAULT_NAMESPACE: &str = "default";
/// The metadata key of a request's consistency token (the REST header's
/// name).
pub const CONSISTENCY_METADATA: &str = "operon-consistency-token";
/// The first bytes of every statement ticket.
pub const TICKET_MAGIC: &[u8; 4] = b"OPFS";
/// The statement ticket format version.
pub const TICKET_VERSION: u16 = 1;
/// The one table type.
pub const TABLE_TYPE: &str = "TABLE";
/// The Arrow version the server declares.
pub const ARROW_VERSION: &str = "58.4";

/// What a `DoGet` needs to run a statement: the ticket is self-contained,
/// so any node can serve it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatementTicket {
    pub namespace: String,
    pub query: String,
    /// The consistency token of the `GetFlightInfo` (Display form); `None`
    /// reads `Strong`.
    pub token: Option<String>,
    /// The `operon-pin-manifest` of the `GetFlightInfo`: with `token`, the
    /// statement reads `Pinned` (Task 14 rule 6).
    pub pinned_manifest: Option<u64>,
}

/// The read of a statement ticket: `Pinned` with a pinned manifest (which
/// needs a token), `AtLeast` with a token alone, else `Strong`. A token that
/// does not parse, or a pinned manifest without a token, is an invalid
/// ticket.
pub fn ticket_consistency(ticket: &StatementTicket) -> Result<ReadConsistency, ServiceError> {
    let invalid = || ServiceError::InvalidArgument("invalid statement ticket".to_string());
    let token = match &ticket.token {
        Some(text) => Some(ConsistencyToken::from_str(text).map_err(|_| invalid())?),
        None => None,
    };
    match (ticket.pinned_manifest, token) {
        (Some(manifest_version), Some(token)) => Ok(ReadConsistency::Pinned {
            manifest_version,
            token,
        }),
        (Some(_), None) => Err(invalid()),
        (None, token) => Ok(consistency_of(token)),
    }
}

/// The read a statement's request metadata asks for (rule 2, Task 14 rule
/// 6): `operon-pin-manifest` with `operon-consistency-token` reads
/// `Pinned`, the token alone `AtLeast` and neither `Strong`. A token that
/// does not parse reads `Strong`; a pinned manifest without a (parsable)
/// token, or one that is not a canonical decimal u64, is `InvalidArgument`.
pub fn metadata_consistency(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<ReadConsistency, ServiceError> {
    let (token, pinned_manifest) = statement_read(metadata)?;
    Ok(match (pinned_manifest, token) {
        (Some(manifest_version), Some(token)) => ReadConsistency::Pinned {
            manifest_version,
            token,
        },
        (_, token) => consistency_of(token),
    })
}

/// A canonical decimal u64: digits only, without a leading zero (but `0`).
fn canonical_u64(text: &str) -> Option<u64> {
    let canonical = !text.is_empty()
        && text.bytes().all(|b| b.is_ascii_digit())
        && (text == "0" || !text.starts_with('0'));
    canonical.then(|| text.parse().ok()).flatten()
}

/// The token and pinned manifest of a statement's metadata.
fn statement_read(
    metadata: &tonic::metadata::MetadataMap,
) -> Result<(Option<ConsistencyToken>, Option<u64>), ServiceError> {
    let token = metadata
        .get(CONSISTENCY_METADATA)
        .and_then(|value| value.to_str().ok())
        .and_then(|text| ConsistencyToken::from_str(text).ok());
    let Some(pin) = metadata.get(PIN_MANIFEST_METADATA) else {
        return Ok((token, None));
    };
    let pinned = pin.to_str().ok().and_then(canonical_u64);
    match (pinned, token) {
        (Some(version), Some(token)) => Ok((Some(token), Some(version))),
        _ => Err(ServiceError::InvalidArgument(format!(
            "{PIN_MANIFEST_METADATA} needs a canonical manifest version and {CONSISTENCY_METADATA}"
        ))),
    }
}

/// `TICKET_MAGIC | u16 LE version | postcard(ticket) | crc32c u32 LE` over
/// every preceding byte (rule 1).
pub fn encode_ticket(ticket: &StatementTicket) -> Bytes {
    let mut out = Vec::with_capacity(64 + ticket.query.len());
    out.extend_from_slice(TICKET_MAGIC);
    out.extend_from_slice(&TICKET_VERSION.to_le_bytes());
    let body = postcard::to_stdvec(ticket).expect("a ticket always serializes");
    out.extend_from_slice(&body);
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    Bytes::from(out)
}

/// Decodes an [`encode_ticket`] ticket; a wrong magic, an unknown version,
/// a bad checksum or trailing bytes are `InvalidArgument("invalid statement
/// ticket")`.
pub fn decode_ticket(bytes: &[u8]) -> Result<StatementTicket, ServiceError> {
    let invalid = || ServiceError::InvalidArgument("invalid statement ticket".to_string());
    let header = TICKET_MAGIC.len() + 2;
    if bytes.len() < header + 4 {
        return Err(invalid());
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if &body[..TICKET_MAGIC.len()] != TICKET_MAGIC {
        return Err(invalid());
    }
    if u16::from_le_bytes([body[4], body[5]]) != TICKET_VERSION {
        return Err(invalid());
    }
    let crc = u32::from_le_bytes(crc.try_into().map_err(|_| invalid())?);
    if crc32c::crc32c(body) != crc {
        return Err(invalid());
    }
    let (ticket, rest) =
        postcard::take_from_bytes::<StatementTicket>(&body[header..]).map_err(|_| invalid())?;
    if !rest.is_empty() {
        return Err(invalid());
    }
    Ok(ticket)
}

/// How Flight SQL bounds its work.
#[derive(Clone, Debug, PartialEq)]
pub struct FlightConfig {
    /// 10 min: how long one `DoGet` statement may plan and stream (a put
    /// has no deadline).
    pub max_duration: Duration,
    /// 64 MiB: the largest gRPC message the server decodes (Task 13 rule 5).
    pub max_message_bytes: usize,
    /// 10 000 rows per ingest chunk; 1..=`MAX_WRITE_OPS`.
    pub put_chunk_rows: usize,
}

impl Default for FlightConfig {
    fn default() -> Self {
        Self {
            max_duration: Duration::from_secs(600),
            max_message_bytes: 64 << 20,
            put_chunk_rows: 10_000,
        }
    }
}

impl FlightConfig {
    /// Refuses a `put_chunk_rows` of 0 or above `MAX_WRITE_OPS`, and a
    /// `max_message_bytes` of 0.
    pub fn validate(&self) -> Result<(), String> {
        if !(1..=MAX_WRITE_OPS).contains(&self.put_chunk_rows) {
            return Err(format!(
                "flight.put_chunk_rows must be 1..={MAX_WRITE_OPS}, got {}",
                self.put_chunk_rows
            ));
        }
        if self.max_message_bytes == 0 {
            return Err("flight.max_message_bytes must be at least 1".to_string());
        }
        Ok(())
    }
}

/// The status of a service error (rule 5); the message is the `Display`
/// text.
pub fn status_of(err: &ServiceError) -> Status {
    let message = err.to_string();
    match err {
        ServiceError::NotFound { .. } => Status::not_found(message),
        ServiceError::AlreadyExists(_) => Status::already_exists(message),
        ServiceError::InvalidArgument(_) | ServiceError::SchemaViolation { .. } => {
            Status::invalid_argument(message)
        }
        ServiceError::Unavailable(_) => Status::unavailable(message),
        ServiceError::Timeout => Status::deadline_exceeded(message),
        ServiceError::Internal(_) => Status::internal(message),
    }
}

impl From<ServiceError> for Status {
    fn from(err: ServiceError) -> Self {
        status_of(&err)
    }
}

/// The SQL info every `GetSqlInfo` answers (rule 4).
pub fn sql_info_data() -> SqlInfoData {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "operon");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    builder.append(SqlInfo::FlightSqlServerArrowVersion, ARROW_VERSION);
    // SQL statements stay read-only; ingest is not a SQL statement (Task 13
    // rule 8).
    builder.append(SqlInfo::FlightSqlServerReadOnly, true);
    builder.append(SqlInfo::FlightSqlServerBulkIngestion, true);
    builder.append(SqlInfo::FlightSqlServerIngestTransactionsSupported, false);
    builder.append(
        SqlInfo::FlightSqlServerTransaction,
        SqlSupportedTransaction::None as i32,
    );
    builder.build().expect("the SQL info is well formed")
}

type DoGetStream = <OperonFlightSql as FlightService>::DoGetStream;
type DoPutStream = <OperonFlightSql as FlightService>::DoPutStream;

/// The Flight SQL service of one node.
#[derive(Clone)]
pub struct OperonFlightSql {
    service: Arc<CollectionService>,
    streams: Option<Arc<dyn StreamProducer>>,
    config: FlightConfig,
    sql_info: Arc<SqlInfoData>,
    /// The put tasks, so a shutdown can wait for them.
    puts: TaskTracker,
    /// Cancelled when the server shuts down; stops the puts.
    stop: CancellationToken,
}

impl std::fmt::Debug for OperonFlightSql {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperonFlightSql")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

/// The namespace of `request` (rule 2).
fn namespace_of<T>(request: &Request<T>) -> String {
    request
        .metadata()
        .get(NAMESPACE_METADATA)
        .and_then(|value| value.to_str().ok())
        .filter(|ns| !ns.is_empty())
        .unwrap_or(DEFAULT_NAMESPACE)
        .to_string()
}

/// The request's [`ID_TYPE_METADATA`], for a string `_id` column whose field
/// has none.
fn id_type_of<T>(request: &Request<T>) -> Option<String> {
    request
        .metadata()
        .get(ID_TYPE_METADATA)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

fn consistency_of(token: Option<ConsistencyToken>) -> ReadConsistency {
    match token {
        Some(token) => ReadConsistency::AtLeast(token),
        None => ReadConsistency::Strong,
    }
}

fn flight_error(err: ServiceError) -> FlightError {
    FlightError::Tonic(Box::new(status_of(&err)))
}

/// A metadata command's `FlightInfo`: its schema and one endpoint whose
/// ticket is the command itself.
fn command_info(
    schema: &Schema,
    ticket: Vec<u8>,
    descriptor: FlightDescriptor,
) -> Result<Response<FlightInfo>, Status> {
    let info = FlightInfo::new()
        .try_with_schema(schema)
        .map_err(|err| Status::internal(format!("encoding a schema: {err}")))?
        .with_endpoint(FlightEndpoint::new().with_ticket(Ticket::new(ticket)))
        .with_descriptor(descriptor);
    Ok(Response::new(info))
}

/// One batch as a `DoGet` stream.
fn batch_stream(schema: SchemaRef, batch: Result<RecordBatch, Status>) -> DoGetStream {
    let batch = batch.map_err(|status| FlightError::Tonic(Box::new(status)));
    Box::pin(
        FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async move { batch }))
            .map_err(Status::from),
    )
}

/// `stream`, ended with `Timeout` at `deadline`.
fn bounded(
    stream: SendableRecordBatchStream,
    deadline: tokio::time::Instant,
) -> impl Stream<Item = Result<RecordBatch, FlightError>> + Send {
    futures::stream::unfold(Some(stream), move |state| async move {
        let mut stream = state?;
        match tokio::time::timeout_at(deadline, stream.next()).await {
            Ok(Some(Ok(batch))) => Some((Ok(batch), Some(stream))),
            Ok(Some(Err(err))) => Some((Err(flight_error(crate::sql::execution(err))), None)),
            Ok(None) => None,
            Err(_) => Some((Err(flight_error(ServiceError::Timeout)), None)),
        }
    })
}

impl OperonFlightSql {
    pub fn new(service: Arc<CollectionService>, config: FlightConfig) -> Self {
        Self {
            service,
            streams: None,
            config,
            sql_info: Arc::new(sql_info_data()),
            puts: TaskTracker::new(),
            stop: CancellationToken::new(),
        }
    }

    /// Tracks the put tasks in `puts` and stops them once `stop` is
    /// cancelled: a put waiting for a message stops at once, one writing
    /// stops before its next chunk.
    pub fn with_put_tasks(self, puts: TaskTracker, stop: CancellationToken) -> Self {
        Self { puts, stop, ..self }
    }

    /// Serves stream ingest through `streams` (Task 13 rule 7).
    pub fn with_streams(self, streams: Arc<dyn StreamProducer>) -> Self {
        Self {
            streams: Some(streams),
            ..self
        }
    }

    /// The producer of stream puts, or `Unimplemented`.
    fn producer(&self) -> Result<Arc<dyn StreamProducer>, Status> {
        self.streams
            .clone()
            .ok_or_else(|| Status::unimplemented("stream ingest is not configured"))
    }

    /// Rule 2.1 for streams: the partition count, `None` when the stream is
    /// missing.
    async fn stream_partitions(
        producer: &Arc<dyn StreamProducer>,
        ns: &str,
        name: &str,
    ) -> Result<Option<u32>, Status> {
        match producer.partitions(ns, name).await {
            Ok(partitions) => Ok(Some(partitions)),
            Err(ServiceError::NotFound { .. }) => Ok(None),
            Err(err) => Err(err.into()),
        }
    }

    /// A put into `sink`, stopped by the server's shutdown.
    fn put(&self, sink: Sink) -> Put {
        Put::new(
            self.service.clone(),
            sink,
            self.config.put_chunk_rows,
            self.stop.clone(),
        )
    }

    /// Runs `put` over `data` in a tracked task of its own, so a client that
    /// leaves stops it only after the chunk in flight (Task 13 rule 5).
    fn spawn_put(
        &self,
        mut put: Put,
        data: PeekableFlightDataStream,
        acks: Option<Acks>,
    ) -> tokio::task::JoinHandle<Result<u64, Status>> {
        self.puts.spawn(async move {
            let result = put.run(data, acks.as_ref()).await;
            match result {
                Ok(()) => Ok(put.written),
                Err(err) => {
                    let status = err.into_status(put.written);
                    if let Some(acks) = &acks {
                        let _ = acks.send(Err(status.clone())).await;
                    }
                    Err(status)
                }
            }
        })
    }

    /// The namespaces a metadata command lists: `catalog` when it names
    /// one, else every namespace.
    async fn namespaces(&self, catalog: Option<&str>) -> Result<Vec<String>, Status> {
        let names = self.service.namespace_names().await?;
        Ok(match catalog {
            Some(catalog) => names.into_iter().filter(|ns| ns == catalog).collect(),
            None => names,
        })
    }
}

#[tonic::async_trait]
impl FlightSqlService for OperonFlightSql {
    type FlightService = Self;

    /// No authentication in M1 (§6.9): an empty answer.
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let response = HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::new(),
        };
        Ok(Response::new(Box::pin(futures::stream::once(async {
            Ok(response)
        }))))
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let namespace = namespace_of(&request);
        let (token, pinned_manifest) = statement_read(request.metadata())?;
        let ticket = StatementTicket {
            namespace,
            query: query.query,
            token: token.as_ref().map(ToString::to_string),
            pinned_manifest,
        };
        let ctx = self
            .service
            .sql_context_with(&ticket.namespace, ticket_consistency(&ticket)?);
        let frame = plan_read_only(&ctx, &ticket.query).await?;
        let schema: Schema = frame.schema().as_arrow().clone();
        let handle = TicketStatementQuery {
            statement_handle: encode_ticket(&ticket),
        };
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|err| Status::internal(format!("encoding a schema: {err}")))?
            // No location: the client reads from this connection.
            .with_endpoint(
                FlightEndpoint::new().with_ticket(Ticket::new(handle.as_any().encode_to_vec())),
            )
            .with_descriptor(request.into_inner())
            .with_total_records(-1)
            .with_total_bytes(-1);
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let ticket = decode_ticket(&ticket.statement_handle)?;
        let consistency = ticket_consistency(&ticket)?;
        let deadline = tokio::time::Instant::now() + self.config.max_duration;
        let ctx = self
            .service
            .sql_context_with(&ticket.namespace, consistency);
        let stream = tokio::time::timeout_at(deadline, async {
            let frame = plan_read_only(&ctx, &ticket.query).await?;
            execute_read_only(frame).await
        })
        .await
        .map_err(|_| ServiceError::Timeout)??;
        let schema = stream.schema();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(bounded(stream, deadline))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        command_info(&query.into_builder().schema(), ticket, request.into_inner())
    }

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut builder = query.into_builder();
        for ns in self.namespaces(None).await? {
            builder.append(ns);
        }
        let schema = builder.schema();
        Ok(Response::new(batch_stream(
            schema,
            builder.build().map_err(Status::from),
        )))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        command_info(&query.into_builder().schema(), ticket, request.into_inner())
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let namespaces = self.namespaces(query.catalog.as_deref()).await?;
        // The builder applies `catalog` and the `LIKE` pattern.
        let mut builder = query.into_builder();
        for ns in namespaces {
            builder.append(ns, COLLECTIONS_SCHEMA);
        }
        let schema = builder.schema();
        Ok(Response::new(batch_stream(
            schema,
            builder.build().map_err(Status::from),
        )))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        command_info(&query.into_builder().schema(), ticket, request.into_inner())
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let namespaces = self.namespaces(query.catalog.as_deref()).await?;
        let include_schema = query.include_schema;
        // The builder applies `catalog`, the patterns and the table types.
        let mut builder = query.into_builder();
        let empty = Schema::empty();
        for ns in namespaces {
            for collection in self.service.collection_records(&ns).await? {
                let schema = if include_schema {
                    collection_arrow_schema(&collection.schema)
                } else {
                    Arc::new(empty.clone())
                };
                builder
                    .append(
                        &ns,
                        COLLECTIONS_SCHEMA,
                        &collection.name,
                        TABLE_TYPE,
                        &schema,
                    )
                    .map_err(Status::from)?;
            }
        }
        let schema = builder.schema();
        Ok(Response::new(batch_stream(
            schema,
            builder.build().map_err(Status::from),
        )))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        command_info(&query.into_builder().schema(), ticket, request.into_inner())
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let mut builder = query.into_builder();
        builder.append(TABLE_TYPE);
        let schema = builder.schema();
        Ok(Response::new(batch_stream(
            schema,
            builder.build().map_err(Status::from),
        )))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let ticket = query.as_any().encode_to_vec();
        let schema = query.into_builder(&self.sql_info).schema();
        command_info(&schema, ticket, request.into_inner())
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<DoGetStream>, Status> {
        let builder = query.into_builder(&self.sql_info);
        let schema = builder.schema();
        Ok(Response::new(batch_stream(
            schema,
            builder.build().map_err(Status::from),
        )))
    }

    /// Rule 1.1 (Task 13): a PATH descriptor (`Any` type URL `""`); every
    /// other unknown command stays `Unimplemented`.
    async fn do_put_fallback(
        &self,
        mut request: Request<PeekableFlightDataStream>,
        message: Any,
    ) -> Result<Response<DoPutStream>, Status> {
        if !message.type_url.is_empty() {
            return Err(Status::unimplemented(format!(
                "do_put: The defined request is invalid: {}",
                message.type_url
            )));
        }
        let ns = namespace_of(&request);
        let id_type = id_type_of(&request);
        let path = match request.get_mut().peek().await {
            Some(Ok(first)) => first
                .flight_descriptor
                .as_ref()
                .map(|descriptor| descriptor.path.clone())
                .unwrap_or_default(),
            Some(Err(status)) => return Err(status.clone()),
            None => Vec::new(),
        };
        let sink = match put_target_from_path(&path)? {
            PutTarget::Collection { name } => {
                let info = self.service.get_collection(&ns, &name).await?;
                Sink::Collection {
                    ns,
                    name,
                    schema: Some(info.schema),
                    id_type,
                }
            }
            PutTarget::Stream { name, partition } => {
                let producer = self.producer()?;
                let partitions = Self::stream_partitions(&producer, &ns, &name)
                    .await?
                    .ok_or_else(|| {
                        status_of(&ServiceError::NotFound {
                            kind: "stream",
                            name: name.clone(),
                        })
                    })?;
                Sink::Stream {
                    ns,
                    name,
                    partitions,
                    partition,
                    producer,
                }
            }
        };
        let put = self.put(sink);
        let (acks, received) = tokio::sync::mpsc::channel(4);
        self.spawn_put(put, request.into_inner(), Some(acks));
        let stream = futures::stream::unfold(received, |mut received| async move {
            let item = received.recv().await?.map(|ack| PutResult {
                app_metadata: Bytes::from(
                    serde_json::to_vec(&ack).expect("an acknowledgement serializes"),
                ),
            });
            Some((item, received))
        });
        Ok(Response::new(Box::pin(stream)))
    }

    /// Rule 1.3 (Task 13).
    async fn do_put_error_callback(
        &self,
        _request: Request<PeekableFlightDataStream>,
        _error: DoPutError,
    ) -> Result<Response<DoPutStream>, Status> {
        Err(Status::invalid_argument(
            "DoPut needs a flight descriptor in its first message",
        ))
    }

    /// Rule 1.2 (Task 13): ADBC's `adbc_ingest`; answers the rows written.
    async fn do_put_statement_ingest(
        &self,
        cmd: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let default_ns = namespace_of(&request);
        let id_type = id_type_of(&request);
        let (ns, target) = put_target_from_ingest(&cmd, &default_ns)?;
        let options = cmd.table_definition_options.as_ref();
        let sink = match target {
            PutTarget::Collection { name } => {
                let existing = match self.service.get_collection(&ns, &name).await {
                    Ok(info) => Some(info.schema),
                    Err(ServiceError::NotFound { .. }) => None,
                    Err(err) => return Err(err.into()),
                };
                let target = PutTarget::Collection { name: name.clone() };
                let schema = match ingest_action(options, &target, existing.is_some())? {
                    IngestAction::Append => existing,
                    IngestAction::Create => None,
                };
                Sink::Collection {
                    ns,
                    name,
                    schema,
                    id_type,
                }
            }
            PutTarget::Stream { name, partition } => {
                let producer = self.producer()?;
                let partitions = Self::stream_partitions(&producer, &ns, &name).await?;
                let target = PutTarget::Stream {
                    name: name.clone(),
                    partition,
                };
                ingest_action(options, &target, partitions.is_some())?;
                Sink::Stream {
                    ns,
                    name,
                    partitions: partitions.unwrap_or(1),
                    partition,
                    producer,
                }
            }
        };
        let put = self.put(sink);
        let written = self
            .spawn_put(put, request.into_inner(), None)
            .await
            .map_err(|err| Status::internal(format!("the put task failed: {err}")))??;
        Ok(i64::try_from(written).unwrap_or(i64::MAX))
    }

    /// The SQL info is fixed ([`sql_info_data`]).
    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Serves Flight SQL on `listener` until `shutdown` is cancelled, with the
/// hot switch of `operon-hot` (rule 2), stream ingest through `streams`
/// and messages of up to `config.max_message_bytes`.
pub async fn serve_flight_sql(
    listener: tokio::net::TcpListener,
    service: Arc<CollectionService>,
    streams: Option<Arc<dyn StreamProducer>>,
    config: FlightConfig,
    shutdown: CancellationToken,
) -> Result<(), tonic::transport::Error> {
    let puts = TaskTracker::new();
    serve_flight_sql_tracked(listener, service, streams, config, shutdown, puts).await
}

/// [`serve_flight_sql`], with the put tasks in `puts`. Once `shutdown` is
/// cancelled the puts stop (a put waiting for a message at once, one
/// writing before its next chunk), and the server waits for them after the
/// calls in flight. A caller that gives up waiting for the server can still
/// wait for `puts` before it stops what the puts write through.
pub async fn serve_flight_sql_tracked(
    listener: tokio::net::TcpListener,
    service: Arc<CollectionService>,
    streams: Option<Arc<dyn StreamProducer>>,
    config: FlightConfig,
    shutdown: CancellationToken,
    puts: TaskTracker,
) -> Result<(), tonic::transport::Error> {
    let hot = HotLayer::new(service.config().hot_default);
    let max_message_bytes = config.max_message_bytes;
    let mut flight_sql =
        OperonFlightSql::new(service, config).with_put_tasks(puts.clone(), shutdown.clone());
    if let Some(streams) = streams {
        flight_sql = flight_sql.with_streams(streams);
    }
    let flight = FlightServiceServer::new(flight_sql).max_decoding_message_size(max_message_bytes);
    let result = tonic::transport::Server::builder()
        .layer(hot)
        .add_service(flight)
        .serve_with_incoming_shutdown(
            pace_accept_errors(
                tonic::transport::server::TcpIncoming::from(listener),
                ACCEPT_ERROR_PAUSE,
            ),
            shutdown.cancelled_owned(),
        )
        .await;
    puts.close();
    puts.wait().await;
    result
}

/// How long the Flight SQL listener waits after a failed accept.
pub const ACCEPT_ERROR_PAUSE: Duration = Duration::from_millis(100);

/// `incoming`, with a `pause` before each failed accept is handed on.
/// tonic's accept loop retries a failed accept at once, so a persistent
/// error (`EMFILE` while file descriptors run out) would otherwise spin a
/// runtime worker.
pub fn pace_accept_errors<T, S>(
    incoming: S,
    pause: Duration,
) -> impl Stream<Item = std::io::Result<T>> + Send
where
    S: Stream<Item = std::io::Result<T>> + Send,
    T: Send,
{
    incoming.then(move |accepted| async move {
        if let Err(err) = &accepted {
            tracing::debug!(%err, "Flight SQL accept failed");
            tokio::time::sleep(pause).await;
        }
        accepted
    })
}
