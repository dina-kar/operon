//! The Loam Live sync service (design §20 §7, D121; R1 plan Task 12):
//! [`LiveServer`] serves `loam.live.v1.LiveService` over connect-rust and
//! axum on its own loopback listener, with one [`Subscriptions`] manager, one
//! journal janitor and the [`Sessions`] of the app.
//!
//! `Watch` opens a session and streams its Transitions; `ModifyQuerySet`
//! changes a session's query set; `Query` and `Mutate` run through the
//! app's [`Runner`]; `Deploy` answers `UNIMPLEMENTED` until Task 13. Connect,
//! gRPC and gRPC-Web, JSON and binary, over HTTP/1.1 and HTTP/2 (cleartext,
//! loopback only).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use connectrpc::{
    ConnectError, ConnectRpcService, RequestContext, Response, ServiceRequest, ServiceResult,
    ServiceStream,
};
use futures::StreamExt;
use operon_tikv::{Tikv, TimestampExt};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::pb::{self, LiveService, LiveServiceServer};
use crate::session::{Outbox, SESSION_HEADER, Sessions, Start, args_of, chunks, ts_of};
use crate::subs::{SubsConfig, SubsStats, Subscriptions};
use crate::{Janitor, LiveConfig, LiveError, Runner, check_listen, deploy};

/// How long stopping waits for in-flight requests before aborting them.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// Starts Loam Live servers.
#[derive(Debug)]
pub struct LiveServer;

/// A running Live server.
#[derive(Debug)]
pub struct LiveHandle {
    /// The address the sync API listens on.
    pub addr: SocketAddr,
    tikv: Tikv,
    subs: Arc<Subscriptions>,
    sessions: Sessions,
    stop: CancellationToken,
    serve: JoinHandle<()>,
    janitor: JoinHandle<()>,
}

impl LiveServer {
    /// Checks that `config.listen` is loopback, connects to the app's
    /// keyspace, opens its runner, starts the subscription manager (its
    /// consumer is `config.node` unless set) and the janitor, binds the
    /// listener and serves until `shutdown` is cancelled or
    /// [`LiveHandle::stop`]. Logs once that the API is unauthenticated.
    pub async fn start(
        config: LiveConfig,
        shutdown: CancellationToken,
    ) -> Result<LiveHandle, LiveError> {
        check_listen(config.listen)?;
        let tikv = Tikv::connect(config.tikv.clone()).await.map_err(|e| {
            LiveError::Internal(format!(
                "connecting to the Live keyspace {}: {e}",
                config.tikv.keyspace
            ))
        })?;
        let runner = Runner::open(tikv.clone(), &config).await?;
        let listener = tokio::net::TcpListener::bind(config.listen)
            .await
            .map_err(|e| LiveError::Internal(format!("live listen on {}: {e}", config.listen)))?;
        let addr = listener
            .local_addr()
            .map_err(|e| LiveError::Internal(format!("live listen on {}: {e}", config.listen)))?;
        let stop = shutdown.child_token();
        let subs_config = SubsConfig {
            consumer: config
                .subs
                .consumer
                .clone()
                .or_else(|| Some(config.node.clone())),
            ..config.subs.clone()
        };
        let subs = Arc::new(Subscriptions::spawn(
            runner.clone(),
            subs_config,
            stop.clone(),
        ));
        let sessions = Sessions::new(
            subs.clone(),
            Arc::new(deploy::resolve),
            config.session.clone(),
            config.node.clone(),
            stop.clone(),
        );
        let janitor = tokio::spawn(janitor_loop(
            runner.clone(),
            config.janitor_interval,
            stop.clone(),
        ));
        let service = Live {
            runner,
            subs: subs.clone(),
            sessions: sessions.clone(),
            max_transition_bytes: config.session.max_transition_bytes,
        };
        let app = axum::Router::new()
            .fallback_service(ConnectRpcService::new(LiveServiceServer::new(service)));
        let until = stop.clone();
        let serve = tokio::spawn(async move {
            let served = axum::serve(listener, app)
                .with_graceful_shutdown(until.cancelled_owned())
                .await;
            if let Err(e) = served {
                tracing::error!(error = %e, "the Live sync API failed");
            }
        });
        tracing::warn!(
            %addr,
            app = %config.app,
            "Loam Live API is unauthenticated (D111): Query, Mutate and Deploy are open to \
             every local process; it listens on loopback only"
        );
        Ok(LiveHandle {
            addr,
            tikv,
            subs,
            sessions,
            stop,
            serve,
            janitor,
        })
    }
}

impl LiveHandle {
    /// The app's TiKV handle (the cluster GC loop sweeps its commit tokens).
    pub fn tikv(&self) -> &Tikv {
        &self.tikv
    }

    /// The subscription manager's counters. `missed_invalidations` is the
    /// `live_missed_invalidation_total` counter (row T12-8).
    pub fn stats(&self) -> SubsStats {
        self.subs.stats()
    }

    /// Open sessions.
    pub fn sessions(&self) -> usize {
        self.sessions.len()
    }

    /// Stops serving: ends every session's stream, stops the manager and the
    /// janitor, and waits up to 10 s for in-flight requests.
    pub async fn stop(self) {
        self.stop.cancel();
        let mut serve = self.serve;
        if tokio::time::timeout(STOP_GRACE, &mut serve).await.is_err() {
            tracing::warn!("Live requests did not finish; aborting them");
            serve.abort();
        }
        // Review of #88: a janitor pass already running may retry against a
        // slow cluster; it gets the same grace as the requests.
        let mut janitor = self.janitor;
        if tokio::time::timeout(STOP_GRACE, &mut janitor)
            .await
            .is_err()
        {
            tracing::warn!("the Live journal janitor did not stop; aborting it");
            janitor.abort();
        }
    }
}

/// Runs the journal janitor every `interval` (the first run one interval
/// after start).
async fn janitor_loop(runner: Runner, interval: Duration, stop: CancellationToken) {
    let mut every = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            () = stop.cancelled() => return,
            _ = every.tick() => {}
        }
        let run = async {
            let journal = runner.journal().await?;
            Janitor::new(runner.tikv().clone(), journal)
                .run_once()
                .await
        };
        match run.await {
            Ok(report) => tracing::debug!(?report, "the Live journal janitor ran"),
            Err(e) => tracing::warn!(error = %e, "the Live journal janitor failed; retrying"),
        }
    }
}

/// The `LiveService` implementation.
struct Live {
    runner: Runner,
    subs: Arc<Subscriptions>,
    sessions: Sessions,
    max_transition_bytes: usize,
}

/// The Connect error of `e` (the codes of §20 §7.1).
pub fn connect_error(e: &LiveError) -> ConnectError {
    let message = e.to_string();
    match e.code() {
        pb::ErrorCode::ERROR_CODE_INVALID_ARGUMENT => ConnectError::invalid_argument(message),
        pb::ErrorCode::ERROR_CODE_NOT_FOUND => ConnectError::not_found(message),
        pb::ErrorCode::ERROR_CODE_FAILED_PRECONDITION => ConnectError::failed_precondition(message),
        pb::ErrorCode::ERROR_CODE_RESOURCE_EXHAUSTED => ConnectError::resource_exhausted(message),
        pb::ErrorCode::ERROR_CODE_UNAVAILABLE => ConnectError::unavailable(message),
        _ => ConnectError::internal(message),
    }
}

/// The stream of one session's Transitions: pops its outbox, chunks what is
/// too large, and tells the session when the client goes away.
struct Stream {
    outbox: Arc<Outbox>,
    chunks: std::collections::VecDeque<pb::Transition>,
    max_bytes: usize,
}

impl Drop for Stream {
    fn drop(&mut self) {
        self.outbox.client_gone();
    }
}

fn transitions(outbox: Arc<Outbox>, max_bytes: usize) -> ServiceStream<pb::Transition> {
    let state = Stream {
        outbox,
        chunks: std::collections::VecDeque::new(),
        max_bytes,
    };
    futures::stream::unfold(state, |mut s| async move {
        if let Some(c) = s.chunks.pop_front() {
            return Some((Ok(c), s));
        }
        match s.outbox.pop().await? {
            Err(e) => Some((Err(connect_error(&e)), s)),
            Ok(t) => {
                s.chunks = chunks(t, s.max_bytes).into();
                let first = s.chunks.pop_front()?;
                Some((Ok(first), s))
            }
        }
    })
    .boxed()
}

// The generated trait returns `impl Encodable`; plain `async fn`s with the
// owned messages are how connect-rust documents implementing it, and `Live`
// is private, so the refinement is not API.
#[allow(refining_impl_trait)]
impl LiveService for Live {
    async fn watch(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::WatchRequest>,
    ) -> ServiceResult<ServiceStream<pb::Transition>> {
        let start =
            Start::from_request(request.to_owned_message()).map_err(|e| connect_error(&e))?;
        let session = self.sessions.open(start).map_err(|e| connect_error(&e))?;
        Ok(Response::new(transitions(
            session.outbox,
            self.max_transition_bytes,
        )))
    }

    async fn modify_query_set(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::ModifyQuerySetRequest>,
    ) -> ServiceResult<pb::ModifyQuerySetResponse> {
        self.sessions
            .modify(request.to_owned_message())
            .await
            .map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::ModifyQuerySetResponse::default()))
    }

    async fn query(
        &self,
        _ctx: RequestContext,
        request: ServiceRequest<'_, pb::QueryRequest>,
    ) -> ServiceResult<pb::QueryResponse> {
        let req = request.to_owned_message();
        let run = async {
            let f = deploy::resolve(&req.function)?;
            let args = args_of(req.args.into_option())?;
            let at = match req.ts {
                Some(ts) => ts_of(ts),
                None => match self.subs.current() {
                    Some(at) => at,
                    None => self
                        .runner
                        .tikv()
                        .now()
                        .await
                        .map_err(|e| LiveError::Internal(format!("a timestamp: {e}")))?,
                },
            };
            self.runner.query(&*f, args, at).await
        };
        let queried = run.await.map_err(|e| connect_error(&e))?;
        Ok(Response::new(pb::QueryResponse {
            ts: queried.ts.version(),
            result: buffa::MessageField::some(queried.result.to_proto()),
            ..Default::default()
        }))
    }

    async fn mutate(
        &self,
        ctx: RequestContext,
        request: ServiceRequest<'_, pb::MutateRequest>,
    ) -> ServiceResult<pb::MutateResponse> {
        let req = request.to_owned_message();
        let run = async {
            let f = deploy::resolve(&req.function)?;
            let args = args_of(req.args.into_option())?;
            self.runner.mutate(f, args, req.idempotency_key).await
        };
        let mutated = run.await.map_err(|e| connect_error(&e))?;
        let commit_ts = mutated.commit_ts.version();
        if let Some(session) = ctx.header(SESSION_HEADER).and_then(|v| v.to_str().ok()) {
            self.sessions.mutation_committed(session, commit_ts);
        }
        Ok(Response::new(pb::MutateResponse {
            commit_ts,
            result: buffa::MessageField::some(mutated.result.to_proto()),
            ..Default::default()
        }))
    }

    async fn deploy(
        &self,
        _ctx: RequestContext,
        _request: ServiceRequest<'_, pb::DeployRequest>,
    ) -> ServiceResult<pb::DeployResponse> {
        Err(ConnectError::unimplemented(deploy::DEPLOY_UNIMPLEMENTED))
    }
}
