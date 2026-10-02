//! CloudEvents on streams (design §02 §7.4, D270): ingest with
//! deduplication by `source` + `id` through the metastore ledger, and
//! consume as events. [`ingest`] is shared by the HTTP route and the gRPC
//! `ProduceCloudEvents` call.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::rejection::{BytesRejection, PathRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::IntoResponse;
use bytes::Bytes;
use loams_cloudevents::record::{RecordContext, from_record, to_record};
use loams_cloudevents::{CloudEvent, http as ce_http, json as ce_json, partition};
use loams_collection::ConsistencyToken;
use loams_common::StreamId;
use loams_common::meta::{
    Consistency, IdempotencyClaim, IdempotencyCompletion, IdempotencyKey, IdempotencyState,
    MAX_IDEMPOTENCY_KEYS, MAX_IDEMPOTENCY_TTL_MS, MetaStore,
};
use loams_log::{FetchRequest, LogError, LogWriter, Record};
use serde_json::json;

use super::streams::checked_stream;
use super::{
    ApiError, ApiResult, AppState, DEFAULT_MAX_BYTES, MAX_FETCH_BYTES, MAX_WAIT, namespace_id,
    parse_partition, query_number, stream_id, with_token,
};

/// How long a request's claims stay pending: longer than a writer's 60 s
/// commit budget (§3), so a live request never loses them.
pub const CLAIM_TTL_MS: u64 = 120_000;
/// The default dedup window: 1 hour.
pub const DEFAULT_DEDUP_WINDOW: Duration = Duration::from_secs(3600);
/// The most events in one request: the ledger's claim size.
pub const MAX_EVENTS: usize = MAX_IDEMPOTENCY_KEYS;

/// CloudEvents ingest settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventsConfig {
    /// How long a done `source` + `id` is remembered
    /// (`--cloudevents-dedup-window`, at most 24 hours).
    pub dedup_window: Duration,
}

impl Default for EventsConfig {
    fn default() -> Self {
        Self {
            dedup_window: DEFAULT_DEDUP_WINDOW,
        }
    }
}

impl EventsConfig {
    /// Rejects a window of 0 or over 24 hours.
    pub fn validate(&self) -> Result<(), String> {
        let ms = self.dedup_window.as_millis();
        if ms == 0 || ms > u128::from(MAX_IDEMPOTENCY_TTL_MS) {
            return Err(format!(
                "--cloudevents-dedup-window must be above 0 and at most 24 hours, got {:?}",
                self.dedup_window
            ));
        }
        Ok(())
    }

    fn window_ms(&self) -> u64 {
        u64::try_from(self.dedup_window.as_millis()).unwrap_or(MAX_IDEMPOTENCY_TTL_MS)
    }
}

/// What ingest did with one event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventStatus {
    /// Appended by this request.
    Appended,
    /// Appended before (or earlier in this request); `partition` and
    /// `offset` are the first write's.
    Duplicate,
    /// Another request is appending it; retry after `retry_after_ms`.
    InFlight { retry_after_ms: u64 },
}

impl EventStatus {
    pub fn name(self) -> &'static str {
        match self {
            EventStatus::Appended => "appended",
            EventStatus::Duplicate => "duplicate",
            EventStatus::InFlight { .. } => "in_flight",
        }
    }
}

/// The answer for one event, in request order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EventOutcome {
    pub status: EventStatus,
    /// `None` while in flight.
    pub location: Option<(u32, u64)>,
}

/// The answer to an ingest request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IngestReport {
    pub stream: StreamId,
    pub outcomes: Vec<EventOutcome>,
    /// The consistency token of the appended events.
    pub token: ConsistencyToken,
}

impl IngestReport {
    /// The longest wait any in-flight event asks for.
    pub fn retry_after_ms(&self) -> Option<u64> {
        self.outcomes
            .iter()
            .filter_map(|o| match o.status {
                EventStatus::InFlight { retry_after_ms } => Some(retry_after_ms),
                _ => None,
            })
            .max()
    }
}

/// What ingest runs against.
#[derive(Clone, Copy, Debug)]
pub struct IngestContext<'a> {
    pub meta: &'a Arc<dyn MetaStore>,
    pub writer: &'a LogWriter,
    pub config: &'a EventsConfig,
    pub node_id: u64,
}

/// A unique owner per request, so a retry of the same request is not
/// mistaken for another one's claim.
fn new_owner(node_id: u64) -> String {
    format!("ce-{node_id}-{}", ulid::Ulid::generate())
}

/// Appends `events` to `ns/stream`, once per `source` + `id` within the
/// dedup window (design §02 §7.4): claim the keys, append what was
/// claimed in one `append_many`, complete the claims with the offsets.
///
/// `partition` is the caller's choice; without it an event goes to its
/// key's Kafka partition, or, keyless, to the partition its idempotency key
/// hashes to, so a retry lands where the first attempt did.
pub async fn ingest(
    ctx: &IngestContext<'_>,
    ns: &str,
    stream: &str,
    partition: Option<u32>,
    events: &[CloudEvent],
) -> Result<IngestReport, ApiError> {
    let IngestContext {
        meta,
        writer,
        config,
        node_id,
    } = *ctx;
    if events.is_empty() {
        return Err(ApiError::invalid("a request carries at least one event"));
    }
    if events.len() > MAX_EVENTS {
        return Err(ApiError::invalid(format!(
            "a request carries at most {MAX_EVENTS} events, got {}",
            events.len()
        )));
    }
    let namespace = namespace_id(&**meta, ns).await?;
    let found = meta
        .stream_by_name(Consistency::Local, namespace, stream)
        .await?
        .ok_or_else(|| ApiError::not_found(format!("stream {ns}/{stream} not found")))?;
    if let Some(p) = partition
        && p >= found.partitions
    {
        return Err(ApiError::invalid(format!(
            "partition {p} is out of range: {ns}/{stream} has {} partitions",
            found.partitions
        )));
    }

    // Coalesce repeats: one claim and one record per distinct key.
    let keys: Vec<IdempotencyKey> = events.iter().map(CloudEvent::dedup_key).collect();
    let mut first: HashMap<IdempotencyKey, usize> = HashMap::new();
    let mut distinct: Vec<usize> = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        first.entry(*key).or_insert_with(|| {
            distinct.push(i);
            i
        });
    }

    let owner = new_owner(node_id);
    let states = meta
        .claim_idempotency_keys(IdempotencyClaim {
            stream: found.id,
            owner: owner.clone(),
            keys: distinct.iter().map(|&i| keys[i]).collect(),
            ttl_ms: CLAIM_TTL_MS,
        })
        .await?;
    if states.len() != distinct.len() {
        return Err(super::errors::internal(
            "the ledger answered a different number of keys than it was asked",
        ));
    }

    let now = meta.now_ms();
    let mut outcomes: HashMap<usize, EventOutcome> = HashMap::new();
    let mut claimed: Vec<usize> = Vec::new();
    for (&i, state) in distinct.iter().zip(&states) {
        let outcome = match *state {
            IdempotencyState::Claimed => {
                claimed.push(i);
                continue;
            }
            IdempotencyState::Done { partition, offset } => EventOutcome {
                status: EventStatus::Duplicate,
                location: Some((partition, offset)),
            },
            IdempotencyState::InFlight { until_ms } => EventOutcome {
                status: EventStatus::InFlight {
                    retry_after_ms: until_ms.saturating_sub(now).max(1000),
                },
                location: None,
            },
        };
        outcomes.insert(i, outcome);
    }

    let mut token = ConsistencyToken(Vec::new());
    if !claimed.is_empty() {
        // Group the claimed events by partition, keeping request order.
        let mut groups: BTreeMap<u32, Vec<(usize, Record)>> = BTreeMap::new();
        for &i in &claimed {
            let p =
                partition.unwrap_or_else(|| partition::partition_of(&events[i], found.partitions));
            groups
                .entry(p)
                .or_default()
                .push((i, to_record(&events[i])));
        }
        let batches: Vec<(u32, Vec<Record>)> = groups
            .iter()
            .map(|(p, group)| (*p, group.iter().map(|(_, r)| r.clone()).collect()))
            .collect();
        let claimed_keys: Vec<IdempotencyKey> = claimed.iter().map(|&i| keys[i]).collect();
        let appended = append(meta, writer, ns, stream, batches).await;
        let acks = match appended {
            Ok(acks) => acks,
            Err(err) => {
                // A definite failure releases the claims so a retry need not
                // wait for them to lapse; an unknown outcome keeps them (the
                // append may have committed).
                if !err.commit_unknown
                    && let Err(release) = meta
                        .release_idempotency_keys(found.id, &owner, claimed_keys)
                        .await
                {
                    tracing::warn!(%release, "releasing CloudEvents claims failed");
                }
                return Err(err.error);
            }
        };
        let mut done: Vec<(IdempotencyKey, u32, u64)> = Vec::with_capacity(claimed.len());
        for ack in &acks {
            let Some(group) = groups.get(&ack.partition) else {
                continue;
            };
            for (n, (i, _)) in group.iter().enumerate() {
                let offset = ack.base_offset + n as u64;
                done.push((keys[*i], ack.partition, offset));
                outcomes.insert(
                    *i,
                    EventOutcome {
                        status: EventStatus::Appended,
                        location: Some((ack.partition, offset)),
                    },
                );
            }
            token
                .0
                .push((ack.stream, ack.partition, ack.last_offset + 1));
        }
        // The events are in the log whatever this returns; a claim that is
        // not completed lapses, which is the documented duplicate case.
        if let Err(err) = meta
            .complete_idempotency_keys(IdempotencyCompletion {
                stream: found.id,
                owner,
                done,
                window_ms: config.window_ms(),
            })
            .await
        {
            tracing::warn!(%err, "completing CloudEvents claims failed");
        }
    }

    let outcomes = (0..events.len())
        .map(|i| {
            let source = first[&keys[i]];
            let base = outcomes
                .get(&source)
                .copied()
                .ok_or_else(|| super::errors::internal("an event has no outcome after ingest"))?;
            // A repeat within the request is answered as a duplicate of its
            // first occurrence, unless that one is in flight.
            Ok(if i != source && base.status == EventStatus::Appended {
                EventOutcome {
                    status: EventStatus::Duplicate,
                    ..base
                }
            } else {
                base
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;
    Ok(IngestReport {
        stream: found.id,
        outcomes,
        token,
    })
}

/// An append error, and whether the append may nonetheless have committed.
struct AppendError {
    error: ApiError,
    commit_unknown: bool,
}

async fn append(
    meta: &Arc<dyn MetaStore>,
    writer: &LogWriter,
    ns: &str,
    stream: &str,
    batches: Vec<(u32, Vec<Record>)>,
) -> Result<Vec<loams_log::AppendAck>, AppendError> {
    let id = checked_stream(meta, ns, stream, &batches)
        .await
        .map_err(|error| AppendError {
            error,
            commit_unknown: false,
        })?;
    writer.append_many(id, batches).await.map_err(|err| {
        let commit_unknown = matches!(err, LogError::CommitUnknown(_));
        AppendError {
            error: err.into(),
            commit_unknown,
        }
    })
}

// ----- HTTP -----

const BATCH: &str = ce_json::BATCH_CONTENT_TYPE;
const STRUCTURED: &str = ce_json::CONTENT_TYPE;

fn ce_error(err: impl std::fmt::Display) -> ApiError {
    ApiError::invalid(format!("invalid CloudEvent: {err}"))
}

/// The events of a request in whichever mode it uses; `None` when it uses
/// none (415).
fn parse_request(headers: &HeaderMap, body: Bytes) -> Result<Option<Vec<CloudEvent>>, ApiError> {
    let media = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(loams_cloudevents::media_type);
    match media.as_deref() {
        Some(BATCH) => ce_json::parse_batch(&body).map(Some).map_err(ce_error),
        Some(STRUCTURED) => ce_json::parse_event(&body)
            .map(|event| Some(vec![event]))
            .map_err(ce_error),
        _ if ce_http::is_binary(headers) => ce_http::parse_binary(headers, body)
            .map(|event| Some(vec![event]))
            .map_err(ce_error),
        _ => Ok(None),
    }
}

/// `POST /v1/namespaces/{ns}/streams/{stream}/events`.
pub(super) async fn produce_events(
    State(state): State<AppState>,
    path: Result<Path<(String, String)>, PathRejection>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult {
    let (Path((ns, stream)), Query(query), body) = (path?, query?, body?);
    let partition = query
        .get("partition")
        .map(|p| parse_partition(p))
        .transpose()?;
    let Some(events) = parse_request(&headers, body)? else {
        return Err(ApiError::new(
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "unsupported_media_type",
            format!(
                "send a CloudEvent: a ce-specversion header (binary mode), or Content-Type \
                 {STRUCTURED} or {BATCH}"
            ),
        ));
    };
    let ctx = IngestContext {
        meta: &state.meta,
        writer: &state.writer,
        config: &state.cloudevents,
        node_id: state.node_id,
    };
    let report = ingest(&ctx, &ns, &stream, partition, &events).await?;
    let listed: Vec<_> = report
        .outcomes
        .iter()
        .map(|o| {
            let mut event = json!({ "status": o.status.name() });
            if let Some((partition, offset)) = o.location {
                event["partition"] = partition.into();
                event["offset"] = offset.into();
            }
            event
        })
        .collect();
    let body = json!({
        "events": listed,
        "token": report
            .token
            .0
            .iter()
            .map(|(stream, partition, offset)| json!({
                "stream": stream.0,
                "partition": partition,
                "offset": offset.saturating_sub(1),
            }))
            .collect::<Vec<_>>(),
    });
    let mut response = match report.retry_after_ms() {
        Some(ms) => {
            let mut response = (StatusCode::CONFLICT, axum::Json(body)).into_response();
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from(ms.div_ceil(1000).max(1)),
            );
            response
        }
        None => axum::Json(body).into_response(),
    };
    if !report.token.0.is_empty() {
        response = with_token(response, &report.token);
    }
    Ok(response)
}

/// `GET /v1/namespaces/{ns}/streams/{stream}/partitions/{p}/events`.
pub(super) async fn fetch_events(
    State(state): State<AppState>,
    path: Result<Path<(String, String, String)>, PathRejection>,
    query: Result<Query<HashMap<String, String>>, QueryRejection>,
) -> ApiResult {
    let (Path((ns, stream, partition)), Query(query)) = (path?, query?);
    let partition = parse_partition(&partition)?;
    let binary = match query.get("mode").map(String::as_str) {
        None | Some("structured") => false,
        Some("binary") => true,
        Some(other) => {
            return Err(ApiError::invalid(format!(
                "bad mode {other:?}: structured or binary"
            )));
        }
    };
    let offset = query_number::<u64>(&query, "offset")?
        .ok_or_else(|| ApiError::invalid("the offset query parameter is required"))?;
    let max_bytes = query_number::<usize>(&query, "max_bytes")?
        .unwrap_or(DEFAULT_MAX_BYTES)
        .min(MAX_FETCH_BYTES);
    let max_wait = query_number::<u64>(&query, "max_wait_ms")?
        .map_or(Duration::ZERO, Duration::from_millis)
        .min(MAX_WAIT);
    let id = stream_id(&*state.meta, &ns, &stream).await?;
    let response = state
        .reader
        .fetch(FetchRequest {
            stream: id,
            partition,
            offset,
            max_bytes,
            max_wait,
        })
        .await?;
    let event_of = |offset: u64, record: &Record| {
        from_record(
            record,
            RecordContext {
                namespace: &ns,
                stream: &stream,
                stream_id: id.0,
                partition,
                offset,
            },
        )
    };
    let number = |n: u64| HeaderValue::from(n);
    if binary {
        let Some(first) = response.records.first() else {
            return Ok(StatusCode::NO_CONTENT.into_response());
        };
        let event = event_of(first.offset, &first.record);
        let (mut headers, body) = ce_http::write_binary(&event).map_err(|err| {
            super::errors::internal(format!(
                "a stored event cannot be sent in binary mode: {err}"
            ))
        })?;
        headers.insert("loams-offset", number(first.offset));
        headers.insert("loams-next-offset", number(first.offset + 1));
        headers.insert("loams-high-watermark", number(response.high_watermark));
        return Ok((StatusCode::OK, headers, body).into_response());
    }
    let events: Vec<CloudEvent> = response
        .records
        .iter()
        .map(|r| event_of(r.offset, &r.record))
        .collect();
    let mut out = (
        StatusCode::OK,
        [(header::CONTENT_TYPE, HeaderValue::from_static(BATCH))],
        ce_json::write_batch(&events),
    )
        .into_response();
    out.headers_mut()
        .insert("loams-next-offset", number(response.next_offset));
    out.headers_mut()
        .insert("loams-high-watermark", number(response.high_watermark));
    Ok(out)
}

/// The gRPC `ProduceCloudEvents` ingest: the same path as the HTTP route.
#[cfg(feature = "stream-grpc")]
#[derive(Clone, Debug)]
pub struct NativeEventProducer {
    pub meta: Arc<dyn MetaStore>,
    pub writer: LogWriter,
    pub config: EventsConfig,
    pub node_id: u64,
}

#[cfg(feature = "stream-grpc")]
#[async_trait::async_trait]
impl loams_stream_grpc::EventProducer for NativeEventProducer {
    async fn produce_events(
        &self,
        ns: &str,
        stream: &str,
        partition: Option<u32>,
        events: Vec<CloudEvent>,
    ) -> Result<Vec<loams_stream_grpc::proto::EventResult>, loams_query::ServiceError> {
        use loams_stream_grpc::proto::{EventResult, EventStatus as Status};
        let ctx = IngestContext {
            meta: &self.meta,
            writer: &self.writer,
            config: &self.config,
            node_id: self.node_id,
        };
        let report = ingest(&ctx, ns, stream, partition, &events)
            .await
            .map_err(|err| super::streams::service_error(err, stream))?;
        Ok(report
            .outcomes
            .iter()
            .map(|outcome| {
                let (partition, offset) = outcome.location.unwrap_or_default();
                EventResult {
                    status: match outcome.status {
                        EventStatus::Appended => Status::Appended,
                        EventStatus::Duplicate => Status::Duplicate,
                        EventStatus::InFlight { .. } => Status::InFlight,
                    }
                    .into(),
                    partition,
                    offset,
                    retry_after_ms: match outcome.status {
                        EventStatus::InFlight { retry_after_ms } => retry_after_ms,
                        _ => 0,
                    },
                }
            })
            .collect())
    }
}
