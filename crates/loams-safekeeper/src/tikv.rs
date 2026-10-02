//! The TiKV [`WalStore`]: the quorum hot tier of §28 §6.4–§6.5 (D237, D238).
//!
//! Every mutating call is one optimistic transaction that reads the
//! timeline's head, applies the shared rule from [`crate::store`], and writes
//! the head back together with any WAL chunks. Writing the head makes a
//! concurrent term bump (a vote handled by another pool instance) a
//! write-write conflict in either order, so a deposed proposer's WAL can never
//! be acknowledged: the fence needs no timing assumption. Transactions commit
//! with async commit and one-phase commit (the handle's default), which is
//! 1PC whenever the head and the tail chunks share a region.
//!
//! Keys, under the handle's root in keyspace `loams_pgwal`:
//!
//! - `H ‖ tenant ‖ timeline`: the head, `0x01 ‖ postcard(AcceptorState)`;
//! - `W ‖ tenant ‖ timeline ‖ begin_lsn (u64 BE)`: WAL bytes
//!   `[begin_lsn, begin_lsn + len)`, at most one `AppendRequest` (128 KiB).
//!
//! An append whose commit outcome is unknown is simply retried: the rules
//! skip WAL already stored, so the retry is idempotent.

use async_trait::async_trait;
use bytes::Bytes;
use loams_tikv::{Tikv, TxnError, TxnOptions};

use crate::Error;
use crate::proto::ProposerElected;
use crate::store::{
    AppendBatch, Deposed, WalStore, apply_append, apply_commit_lsn, apply_elected, apply_vote,
    read_chunks, remaining_chunks, trim_bound,
};
use crate::types::{AcceptorState, Configuration, Lsn, ServerInfo, Term, TimelineId};

/// The production keyspace (pre-allocated in `deploy/tikv/pd.toml`).
pub const KEYSPACE: &str = "loams_pgwal";

const HEAD_VERSION: u8 = 1;
/// Undetermined commits retried per call (each retry is idempotent).
const UNDETERMINED_RETRIES: u32 = 3;
/// Chunks deleted per trim transaction.
const TRIM_BATCH: usize = 1024;
/// Chunks deleted per truncation transaction on election. Scans return values,
/// so this bounds memory (64 × 128 KiB) and the transaction's size.
const TRUNCATE_BATCH: usize = 64;

fn head_key(tl: &TimelineId) -> Vec<u8> {
    let mut k = Vec::with_capacity(33);
    k.push(b'H');
    k.extend_from_slice(&tl.to_bytes());
    k
}

fn wal_key(tl: &TimelineId, lsn: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(41);
    k.push(b'W');
    k.extend_from_slice(&tl.to_bytes());
    k.extend_from_slice(&lsn.to_be_bytes());
    k
}

fn wal_lsn(key: &[u8]) -> Result<u64, TxnError> {
    key.get(33..41)
        .and_then(|b| <[u8; 8]>::try_from(b).ok())
        .map(u64::from_be_bytes)
        .ok_or_else(|| TxnError::Fatal("malformed WAL key".into()))
}

fn encode_head(st: &AcceptorState) -> Result<Vec<u8>, TxnError> {
    let mut out = vec![HEAD_VERSION];
    out.extend(postcard::to_stdvec(st).map_err(|e| TxnError::Fatal(format!("encode head: {e}")))?);
    Ok(out)
}

fn decode_head(v: &[u8]) -> Result<AcceptorState, TxnError> {
    match v.split_first() {
        Some((&HEAD_VERSION, rest)) => {
            postcard::from_bytes(rest).map_err(|e| TxnError::Fatal(format!("decode head: {e}")))
        }
        _ => Err(TxnError::Fatal("unknown head version".into())),
    }
}

async fn get_head(
    txn: &mut loams_tikv::Txn,
    tl: &TimelineId,
) -> Result<Option<AcceptorState>, TxnError> {
    txn.get(&head_key(tl))
        .await?
        .map(|v| decode_head(&v))
        .transpose()
}

/// The chunk that contains `lsn`, if any: `(begin, bytes)`.
async fn chunk_at(
    txn: &mut loams_tikv::Txn,
    tl: &TimelineId,
    lsn: u64,
) -> Result<Option<(u64, Vec<u8>)>, TxnError> {
    let lo = wal_key(tl, 0);
    let hi = wal_key(tl, lsn.saturating_add(1));
    let mut last = txn.scan_reverse(&lo, Some(&hi), 1).await?;
    match last.pop() {
        Some((k, v)) => {
            let begin = wal_lsn(&k)?;
            Ok((begin + v.len() as u64 > lsn).then_some((begin, v)))
        }
        None => Ok(None),
    }
}

fn store_err(e: TxnError) -> Error {
    Error::Store(e.to_string())
}

/// The TiKV-backed [`WalStore`].
#[derive(Debug, Clone)]
pub struct TikvWalStore {
    tikv: Tikv,
}

impl TikvWalStore {
    /// A store on a handle bound to the WAL keyspace ([`KEYSPACE`] in
    /// production; tests use a test keyspace under a random root).
    pub fn new(tikv: Tikv) -> Self {
        Self { tikv }
    }

    /// The handle the store runs its transactions on.
    pub fn tikv(&self) -> &Tikv {
        &self.tikv
    }

    /// Runs `op` (one transaction via the runner), retrying an undetermined
    /// commit: every body here is idempotent against its own earlier commit.
    async fn run<T, F>(&self, name: &'static str, mut body: F) -> Result<T, Error>
    where
        T: Send,
        F: for<'t> FnMut(
                &'t mut loams_tikv::Txn,
            )
                -> futures::future::BoxFuture<'t, Result<Result<T, Error>, TxnError>>
            + Send,
    {
        let mut tries = 0;
        loop {
            match self.tikv.run(TxnOptions::new(name), &mut body).await {
                Ok(c) => return c.value,
                Err(TxnError::Undetermined { .. }) if tries < UNDETERMINED_RETRIES => {
                    tries += 1;
                    tracing::warn!(op = name, tries, "commit outcome undetermined; retrying");
                }
                Err(e) => return Err(store_err(e)),
            }
        }
    }
}

#[async_trait]
impl WalStore for TikvWalStore {
    async fn load(&self, tl: &TimelineId) -> Result<Option<AcceptorState>, Error> {
        let tl = *tl;
        self.run("pgwal.load", move |txn| {
            Box::pin(async move { Ok(Ok(get_head(txn, &tl).await?)) })
        })
        .await
    }

    async fn create(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        start_lsn: Lsn,
    ) -> Result<AcceptorState, Error> {
        let tl = *tl;
        self.run("pgwal.create", move |txn| {
            Box::pin(async move {
                if let Some(st) = get_head(txn, &tl).await? {
                    return Ok(Ok(st));
                }
                let st = AcceptorState::new(server, start_lsn);
                txn.put(&head_key(&tl), encode_head(&st)?).await?;
                Ok(Ok(st))
            })
        })
        .await
    }

    async fn update_meta(
        &self,
        tl: &TimelineId,
        server: ServerInfo,
        mconf: Option<Configuration>,
    ) -> Result<AcceptorState, Error> {
        let tl = *tl;
        self.run("pgwal.update_meta", move |txn| {
            let mconf = mconf.clone();
            Box::pin(async move {
                let Some(mut st) = get_head(txn, &tl).await? else {
                    return Ok(Err(Error::NotFound(tl)));
                };
                st.server = server;
                if let Some(m) = mconf {
                    st.mconf = m;
                }
                txn.put(&head_key(&tl), encode_head(&st)?).await?;
                Ok(Ok(st))
            })
        })
        .await
    }

    async fn vote(&self, tl: &TimelineId, term: Term) -> Result<(bool, AcceptorState), Error> {
        let tl = *tl;
        self.run("pgwal.vote", move |txn| {
            Box::pin(async move {
                let Some(mut st) = get_head(txn, &tl).await? else {
                    return Ok(Err(Error::NotFound(tl)));
                };
                let given = apply_vote(&mut st, term);
                // Written either way: a refused vote must still conflict with
                // a concurrent grant, so it never reports a stale term.
                txn.put(&head_key(&tl), encode_head(&st)?).await?;
                Ok(Ok((given, st)))
            })
        })
        .await
    }

    async fn elected(
        &self,
        tl: &TimelineId,
        msg: &ProposerElected,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        let tl = *tl;
        let msg = msg.clone();
        // Chunks above the truncation point go in bounded batches. The WAL
        // they hold is from a deposed term: the vote that elected this
        // proposer already fences its writer, so no new chunks appear there.
        // Only the last transaction, which finds at most a batch left, writes
        // the head; until then every rule is re-checked.
        loop {
            let msg = msg.clone();
            let done = self
                .run("pgwal.elected", move |txn| {
                    let msg = msg.clone();
                    Box::pin(async move {
                        let Some(mut st) = get_head(txn, &tl).await? else {
                            return Ok(Err(Error::NotFound(tl)));
                        };
                        let at = match apply_elected(&mut st, &msg) {
                            Err(e) => return Ok(Err(e)),
                            Ok(Err(d)) => return Ok(Ok(Some(Err(d)))),
                            Ok(Ok(at)) => at.0,
                        };
                        let above = txn
                            .scan(
                                &wal_key(&tl, at),
                                Some(&wal_key(&tl, u64::MAX)),
                                TRUNCATE_BATCH + 1,
                            )
                            .await?;
                        let more = above.len() > TRUNCATE_BATCH;
                        for (k, _) in above.into_iter().take(TRUNCATE_BATCH) {
                            txn.delete(&k).await?;
                        }
                        if more {
                            return Ok(Ok(None));
                        }
                        // Cut the chunk that straddles the truncation point.
                        if let Some((begin, bytes)) = chunk_at(txn, &tl, at).await?
                            && begin < at
                        {
                            let keep = (at - begin) as usize;
                            txn.put(&wal_key(&tl, begin), bytes[..keep].to_vec())
                                .await?;
                        }
                        txn.put(&head_key(&tl), encode_head(&st)?).await?;
                        Ok(Ok(Some(Ok(st))))
                    })
                })
                .await?;
            if let Some(out) = done {
                return Ok(out);
            }
        }
    }

    async fn append(
        &self,
        tl: &TimelineId,
        batch: &AppendBatch,
    ) -> Result<Result<AcceptorState, Deposed>, Error> {
        let tl = *tl;
        let batch = batch.clone();
        self.run("pgwal.append", move |txn| {
            let batch = batch.clone();
            Box::pin(async move {
                let Some(mut st) = get_head(txn, &tl).await? else {
                    return Ok(Err(Error::NotFound(tl)));
                };
                let plan = match apply_append(&mut st, &batch) {
                    Err(e) => return Ok(Err(e)),
                    Ok(Err(d)) => return Ok(Ok(Err(d))),
                    Ok(Ok(p)) => p,
                };
                let mut at = plan.write_from.0;
                for c in remaining_chunks(&batch, &plan) {
                    if c.is_empty() {
                        continue;
                    }
                    let len = c.len() as u64;
                    txn.put(&wal_key(&tl, at), c.to_vec()).await?;
                    at += len;
                }
                txn.put(&head_key(&tl), encode_head(&st)?).await?;
                Ok(Ok(Ok(st)))
            })
        })
        .await
    }

    async fn record_commit_lsn(
        &self,
        tl: &TimelineId,
        term: Term,
        commit_lsn: Lsn,
    ) -> Result<Result<(), Deposed>, Error> {
        let tl = *tl;
        self.run("pgwal.commit_lsn", move |txn| {
            Box::pin(async move {
                let Some(mut st) = get_head(txn, &tl).await? else {
                    return Ok(Err(Error::NotFound(tl)));
                };
                let before = st.commit_lsn;
                match apply_commit_lsn(&mut st, term, commit_lsn) {
                    Err(e) => return Ok(Err(e)),
                    Ok(Err(d)) => return Ok(Ok(Err(d))),
                    Ok(Ok(())) => {}
                }
                if st.commit_lsn != before {
                    txn.put(&head_key(&tl), encode_head(&st)?).await?;
                }
                Ok(Ok(Ok(())))
            })
        })
        .await
    }

    async fn record_backup_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error> {
        let tl = *tl;
        self.run("pgwal.backup_lsn", move |txn| {
            Box::pin(async move {
                let Some(mut st) = get_head(txn, &tl).await? else {
                    return Ok(Err(Error::NotFound(tl)));
                };
                let b = st.backup_lsn.max(lsn.min(st.wal_end()));
                if b != st.backup_lsn {
                    st.backup_lsn = b;
                    txn.put(&head_key(&tl), encode_head(&st)?).await?;
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    async fn record_remote_consistent_lsn(&self, tl: &TimelineId, lsn: Lsn) -> Result<(), Error> {
        let tl = *tl;
        self.run("pgwal.remote_consistent_lsn", move |txn| {
            Box::pin(async move {
                let Some(mut st) = get_head(txn, &tl).await? else {
                    return Ok(Err(Error::NotFound(tl)));
                };
                if lsn > st.remote_consistent_lsn {
                    st.remote_consistent_lsn = lsn;
                    txn.put(&head_key(&tl), encode_head(&st)?).await?;
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    async fn read(
        &self,
        tl: &TimelineId,
        from: Lsn,
        max_bytes: usize,
    ) -> Result<Vec<(Lsn, Bytes)>, Error> {
        let tl = *tl;
        self.run("pgwal.read", move |txn| {
            Box::pin(async move {
                let Some(st) = get_head(txn, &tl).await? else {
                    return Ok(Err(Error::NotFound(tl)));
                };
                if from >= st.flush_lsn {
                    return Ok(read_chunks(&st, Vec::new(), from, max_bytes));
                }
                let begin = chunk_at(txn, &tl, from.0).await?.map_or(from.0, |(b, _)| b);
                // Chunks are at most 128 KiB; read enough of them for the budget.
                let limit = (max_bytes / (64 * 1024)).clamp(2, 64);
                let pairs = txn
                    .scan(
                        &wal_key(&tl, begin),
                        Some(&wal_key(&tl, st.flush_lsn.0)),
                        limit,
                    )
                    .await?;
                let mut chunks = Vec::with_capacity(pairs.len());
                for (k, v) in pairs {
                    chunks.push((wal_lsn(&k)?, Bytes::from(v)));
                }
                Ok(read_chunks(&st, chunks, from, max_bytes))
            })
        })
        .await
    }

    async fn trim(&self, tl: &TimelineId, lsn: Lsn) -> Result<Lsn, Error> {
        let tl = *tl;
        loop {
            let (bound, more) = self
                .run("pgwal.trim", move |txn| {
                    Box::pin(async move {
                        let Some(mut st) = get_head(txn, &tl).await? else {
                            return Ok(Err(Error::NotFound(tl)));
                        };
                        let bound = trim_bound(&st, lsn);
                        let keep_from = chunk_at(txn, &tl, bound.0)
                            .await?
                            .map_or(bound.0, |(b, _)| b);
                        let below = txn
                            .scan(&wal_key(&tl, 0), Some(&wal_key(&tl, keep_from)), TRIM_BATCH)
                            .await?;
                        let more = below.len() == TRIM_BATCH;
                        for (k, _) in below {
                            txn.delete(&k).await?;
                        }
                        if bound != st.trimmed_lsn {
                            st.trimmed_lsn = bound;
                            txn.put(&head_key(&tl), encode_head(&st)?).await?;
                        }
                        Ok(Ok((bound, more)))
                    })
                })
                .await?;
            if !more {
                return Ok(bound);
            }
        }
    }
}
