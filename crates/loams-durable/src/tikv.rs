//! Native TiKV storage for Resonate's conditional object port.
//!
//! Each object is one TiKV key containing a random revision and its body. A
//! pessimistic transaction locks that key before comparing revisions and
//! writing, so concurrent Resonate instances cannot both win a CAS. Revisions
//! are random instead of counters: deleting and recreating a key cannot make
//! an old ETag valid again.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use loams_tikv::{Mode, Tikv, TxnError, TxnOptions, token, tuple};
use resonate_core::router::ResonateRouter;
use resonate_core::types::{RequestEnvelope, ResponseEnvelope};
use resonate_core::{ResonateServer, Unavailable};
use resonate_plugin::{ConfigError, ServerDependencies, ServerPlugin, Settings};
use resonate_server_blob::applier::{ApplierCfg, KeySpace};
use resonate_server_blob::kernel::state::KernelCfg;
use resonate_server_blob::server::{Server, ServerCfg};
use resonate_server_blob::store::{Etag, Store, StoreError};
use uuid::Uuid;

const PREFIX: &[u8] = b"resonate/blob/";
const REVISION_BYTES: usize = 16;
const TOKEN_SWEEP_PAGE: usize = 256;
const TOKEN_SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// How long `stop` waits for each background task before aborting it.
const STOP_GRACE: Duration = Duration::from_secs(10);

/// The Resonate server plugin backed by native TiKV transactions.
pub static PLUGIN: ServerPlugin = ServerPlugin::new("resonate-server-tikv", configure);

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    pd: Vec<String>,
    keyspace: String,
    root: Vec<u8>,
    #[serde(default)]
    server_url: String,
    #[serde(default = "default_retry_timeout")]
    retry_timeout: i64,
}

fn default_retry_timeout() -> i64 {
    30_000
}

fn configure(
    settings: &Settings<'_>,
    deps: ServerDependencies,
) -> Result<Arc<dyn ResonateServer>, ConfigError> {
    let config: Config = settings.extract()?;
    if config.pd.is_empty() {
        return Err(settings.reject("pd", "at least one PD endpoint is required"));
    }
    if config.keyspace.is_empty() {
        return Err(settings.reject("keyspace", "a TiKV API v2 keyspace is required"));
    }
    Ok(Arc::new(TikvServer {
        config,
        router: deps.router,
        inner: OnceLock::new(),
        shutdown: tokio::sync::watch::channel(false).0,
        timer: Mutex::new(None),
        token_sweeper: Mutex::new(None),
    }))
}

struct TikvServer {
    config: Config,
    router: Arc<dyn ResonateRouter>,
    inner: OnceLock<Arc<Server>>,
    shutdown: tokio::sync::watch::Sender<bool>,
    timer: Mutex<Option<tokio::task::JoinHandle<()>>>,
    token_sweeper: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[async_trait]
impl ResonateServer for TikvServer {
    async fn init(&self, debug: bool) -> Result<(), Unavailable> {
        let mut config =
            loams_tikv::TikvConfig::new(self.config.pd.clone(), self.config.keyspace.clone());
        config.root = self.config.root.clone();
        let tikv = Tikv::connect(config)
            .await
            .map_err(|e| Unavailable::new(format!("cannot open the TiKV durable store: {e}")))?;
        let store: Arc<dyn Store> = Arc::new(TikvStore::new(tikv.clone()));
        let server = Server::build(
            store,
            Arc::clone(&self.router),
            ServerCfg {
                keys: KeySpace::new("", 4),
                applier: ApplierCfg {
                    kernel: KernelCfg {
                        retry_timeout: self.config.retry_timeout,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                debug,
                server_url: self.config.server_url.clone(),
                ..Default::default()
            },
        );
        self.inner
            .set(Arc::clone(&server))
            .map_err(|_| Unavailable::new("TiKV durable server was initialized twice"))?;
        if !debug {
            let timer = Arc::clone(server.timerd()).spawn(self.shutdown.subscribe());
            *self.timer.lock().expect("TiKV timer mutex") = Some(timer);
            let mut shutdown = self.shutdown.subscribe();
            let token_sweeper = tokio::spawn(async move {
                let mut interval = tokio::time::interval(TOKEN_SWEEP_INTERVAL);
                loop {
                    tokio::select! {
                        _ = shutdown.changed() => break,
                        _ = interval.tick() => {
                            tokio::select! {
                                _ = shutdown.changed() => break,
                                result = sweep_expired_tokens(&tikv) => {
                                    if let Err(error) = result {
                                        tracing::warn!(%error, "durable TiKV token sweep failed");
                                    }
                                }
                            }
                        }
                    }
                }
            });
            *self.token_sweeper.lock().expect("TiKV sweeper mutex") = Some(token_sweeper);
        }
        Ok(())
    }

    async fn stop(&self) -> Result<(), Unavailable> {
        let _ = self.shutdown.send(true);
        let timer = self.timer.lock().expect("TiKV timer mutex").take();
        if let Some(timer) = timer {
            join_within(timer, "timer").await;
        }
        let token_sweeper = self
            .token_sweeper
            .lock()
            .expect("TiKV sweeper mutex")
            .take();
        if let Some(token_sweeper) = token_sweeper {
            join_within(token_sweeper, "token sweeper").await;
        }
        Ok(())
    }

    async fn process(&self, request: &RequestEnvelope) -> Result<ResponseEnvelope, Unavailable> {
        self.inner
            .get()
            .ok_or_else(|| Unavailable::new("TiKV durable server has not started"))?
            .process(request)
            .await
    }

    async fn ready(&self) -> bool {
        match self.inner.get() {
            Some(server) => server.ready().await,
            None => false,
        }
    }
}

/// Waits up to [`STOP_GRACE`] for a background task that was told to stop,
/// then aborts it, so a stalled TiKV call cannot hold up shutdown.
async fn join_within(task: tokio::task::JoinHandle<()>, name: &str) {
    let abort = task.abort_handle();
    if tokio::time::timeout(STOP_GRACE, task).await.is_err() {
        tracing::warn!(
            task = name,
            "the durable TiKV task did not stop in time; aborting it"
        );
        abort.abort();
    }
}

/// Sweep only this durable handle's commit tokens. The cluster-level GC loop
/// is owned by the metastore; starting another one here would give two loops
/// independent leases over the same PD safe point. The token format and
/// expiry are supplied by `loams-tikv` so both users apply the same rule.
async fn sweep_expired_tokens(tikv: &Tikv) -> Result<u64, String> {
    let end = tuple::successor(token::TOKEN_PREFIX);
    let mut start = token::TOKEN_PREFIX.to_vec();
    let mut swept = 0;
    loop {
        let now = tikv.now().await.map_err(|e| e.to_string())?;
        let now_ms = Tikv::physical_ms(&now);
        let mut snapshot = tikv.snapshot(now).await.map_err(|e| e.to_string())?;
        let page = snapshot
            .scan(&start, Some(&end), TOKEN_SWEEP_PAGE)
            .await
            .map_err(|e| e.to_string())?;
        let expired: Vec<Vec<u8>> = page
            .iter()
            .filter(|(_, value)| token::token_expiry(value).is_some_and(|until| until < now_ms))
            .map(|(key, _)| key.clone())
            .collect();
        if !expired.is_empty() {
            let result = tikv
                .run(TxnOptions::new("durable.sweep_tokens"), move |txn| {
                    let expired = expired.clone();
                    Box::pin(async move {
                        let now_ms = Tikv::physical_ms(&txn.start_ts());
                        let mut removed = 0;
                        for (key, value) in txn.batch_get(expired.iter()).await? {
                            if token::token_expiry(&value).is_some_and(|until| until < now_ms) {
                                txn.delete(&key).await?;
                                removed += 1;
                            }
                        }
                        Ok::<u64, TxnError>(removed)
                    })
                })
                .await
                .map_err(|e| e.to_string())?;
            swept += result.value;
        }
        match page.last() {
            Some((last, _)) if page.len() == TOKEN_SWEEP_PAGE => {
                start = last.clone();
                start.push(0);
            }
            _ => return Ok(swept),
        }
    }
}

/// Resonate's conditional object store on the existing, keyspace-scoped TiKV
/// client. The caller should give the durable backend its own root prefix.
#[derive(Debug, Clone)]
pub struct TikvStore {
    tikv: Tikv,
}

impl TikvStore {
    pub fn new(tikv: Tikv) -> Self {
        Self { tikv }
    }

    fn key(name: &str) -> Vec<u8> {
        let mut key = Vec::with_capacity(PREFIX.len() + name.len());
        key.extend_from_slice(PREFIX);
        key.extend_from_slice(name.as_bytes());
        key
    }

    async fn write(
        &self,
        name: &str,
        body: Vec<u8>,
        condition: Condition,
    ) -> Result<Etag, StoreError> {
        let key = Self::key(name);
        let revision = Uuid::new_v4();
        let mut value = Vec::with_capacity(REVISION_BYTES + body.len());
        value.extend_from_slice(revision.as_bytes());
        value.extend_from_slice(&body);
        let result = self
            .tikv
            .run(
                TxnOptions::pessimistic("resonate_blob_write").with_token(),
                |txn| {
                    let key = key.clone();
                    let value = value.clone();
                    let condition = condition.clone();
                    Box::pin(async move {
                        let current = txn.get_for_update(&key).await?;
                        let allowed = match condition {
                            Condition::Overwrite => true,
                            Condition::Absent => current.is_none(),
                            Condition::Match(expected) => current
                                .as_deref()
                                .and_then(|bytes| bytes.get(..REVISION_BYTES))
                                .is_some_and(|actual| actual == expected.as_bytes()),
                        };
                        if !allowed {
                            return Ok(Err(StoreError::PreconditionFailed));
                        }
                        txn.put(&key, value).await?;
                        Ok(Ok(()))
                    })
                },
            )
            .await
            .map_err(map_txn)?;
        result.value?;
        Ok(Etag(revision.to_string()))
    }
}

#[derive(Clone)]
enum Condition {
    Overwrite,
    Absent,
    Match(Uuid),
}

fn map_txn(error: TxnError) -> StoreError {
    match error {
        TxnError::Conflict | TxnError::AlreadyExists(_) => StoreError::Conflict,
        other => StoreError::Unavailable(other.to_string()),
    }
}

fn decode(value: Vec<u8>) -> Result<(Vec<u8>, Etag), StoreError> {
    let revision = value.get(..REVISION_BYTES).ok_or_else(|| {
        StoreError::Unavailable("TiKV returned a truncated Resonate object".into())
    })?;
    let revision = Uuid::from_slice(revision)
        .map_err(|e| StoreError::Unavailable(format!("invalid Resonate object revision: {e}")))?;
    Ok((value[REVISION_BYTES..].to_vec(), Etag(revision.to_string())))
}

#[async_trait]
impl Store for TikvStore {
    async fn get(&self, name: &str) -> Result<Option<(Vec<u8>, Etag)>, StoreError> {
        let key = Self::key(name);
        let result = self
            .tikv
            .run(TxnOptions::new("resonate_blob_get"), |txn| {
                let key = key.clone();
                Box::pin(async move { txn.get(&key).await })
            })
            .await
            .map_err(map_txn)?;
        result.value.map(decode).transpose()
    }

    async fn put_if_match(
        &self,
        name: &str,
        body: Vec<u8>,
        etag: &Etag,
    ) -> Result<Etag, StoreError> {
        let expected = Uuid::parse_str(&etag.0).map_err(|_| StoreError::PreconditionFailed)?;
        self.write(name, body, Condition::Match(expected)).await
    }

    async fn put_if_none_match(&self, name: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.write(name, body, Condition::Absent).await
    }

    async fn put(&self, name: &str, body: Vec<u8>) -> Result<Etag, StoreError> {
        self.write(name, body, Condition::Overwrite).await
    }

    async fn delete(&self, name: &str) -> Result<(), StoreError> {
        let key = Self::key(name);
        self.tikv
            .run(
                TxnOptions::pessimistic("resonate_blob_delete").with_token(),
                |txn| {
                    let key = key.clone();
                    Box::pin(async move { txn.delete(&key).await })
                },
            )
            .await
            .map_err(map_txn)?;
        Ok(())
    }

    async fn list(&self, prefix: &str, max_keys: usize) -> Result<Vec<String>, StoreError> {
        if max_keys == 0 {
            return Ok(Vec::new());
        }
        let start = Self::key(prefix);
        let end = tuple::successor(&start);
        let result = self
            .tikv
            .run(
                TxnOptions {
                    mode: Mode::Pessimistic,
                    ..TxnOptions::new("resonate_blob_list")
                },
                |txn| {
                    let start = start.clone();
                    let end = end.clone();
                    Box::pin(async move { txn.scan(&start, Some(&end), max_keys).await })
                },
            )
            .await
            .map_err(map_txn)?;
        result
            .value
            .into_iter()
            .map(|(key, _)| {
                let name = key.strip_prefix(PREFIX).ok_or_else(|| {
                    StoreError::Unavailable("TiKV returned a key outside the Resonate root".into())
                })?;
                String::from_utf8(name.to_vec()).map_err(|e| {
                    StoreError::Unavailable(format!("invalid Resonate object key: {e}"))
                })
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_revisions_do_not_expose_payloads() {
        let id = Uuid::new_v4();
        let mut value = id.as_bytes().to_vec();
        value.extend_from_slice(b"payload");
        assert_eq!(
            decode(value),
            Ok((b"payload".to_vec(), Etag(id.to_string())))
        );
        assert!(matches!(
            decode(vec![0; 15]),
            Err(StoreError::Unavailable(_))
        ));
    }

    #[test]
    fn object_key_prefix_is_stable() {
        assert_eq!(TikvStore::key("timer/1"), b"resonate/blob/timer/1");
    }

    #[tokio::test]
    async fn conditional_writes_are_atomic_and_old_revisions_do_not_revive() {
        let Some(cluster) = loams_tikv::testing::cluster().await else {
            return;
        };
        let tikv = cluster.connect(loams_tikv::testing::TEST_META).await;
        let store = TikvStore::new(tikv.clone());
        let first = store
            .put_if_none_match("promise/one", b"one".to_vec())
            .await
            .unwrap();
        assert_eq!(
            store.get("promise/one").await.unwrap(),
            Some((b"one".to_vec(), first.clone()))
        );
        assert_eq!(
            store
                .put_if_none_match("promise/one", b"two".to_vec())
                .await,
            Err(StoreError::PreconditionFailed)
        );

        let (left, right) = tokio::join!(
            store.put_if_match("promise/one", b"left".to_vec(), &first),
            store.put_if_match("promise/one", b"right".to_vec(), &first),
        );
        assert_eq!(left.is_ok() as u8 + right.is_ok() as u8, 1);
        let winner = store.get("promise/one").await.unwrap().unwrap().0;
        assert!(winner == b"left" || winner == b"right");

        store.delete("promise/one").await.unwrap();
        let recreated = store
            .put_if_none_match("promise/one", b"new".to_vec())
            .await
            .unwrap();
        assert_ne!(first, recreated);
        assert_eq!(
            store
                .put_if_match("promise/one", b"stale".to_vec(), &first)
                .await,
            Err(StoreError::PreconditionFailed)
        );

        let token_key = token::token_key(&token::new_token());
        tikv.run(TxnOptions::new("test.expired_token"), |txn| {
            let token_key = token_key.clone();
            Box::pin(async move { txn.put(&token_key, 0_u64.to_be_bytes()).await })
        })
        .await
        .unwrap();
        assert_eq!(sweep_expired_tokens(&tikv).await.unwrap(), 1);
        let token_value = tikv
            .run(TxnOptions::new("test.token_after_sweep"), |txn| {
                let token_key = token_key.clone();
                Box::pin(async move { txn.get(&token_key).await })
            })
            .await
            .unwrap();
        assert_eq!(token_value.value, None);
    }
}
