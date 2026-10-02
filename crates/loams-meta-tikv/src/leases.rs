//! Leases with epochs (design §20 §11.3: a read-modify-write of `e/…`
//! comparing owner, epoch and deadline). Deadlines are judged against the
//! transaction's start timestamp, the metastore clock of that transaction
//! (row R2); the rules are `loams-meta/src/state/leases.rs`'s.

use std::time::Duration;

use loams_common::meta::{ApplyError, Fence, Lease, LeaseGrant, MAX_LEASE_TTL_MS, MetaResult};
use loams_tikv::{Tikv, Txn, TxnError};

use crate::catalog::validate_key;
use crate::keys;
use crate::{Reader, TikvMeta, decode_all, load};

/// Which lease write this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Acquire,
    Renew,
    Reacquire,
}

fn validate_lease_args(key: &str, owner: &str, ttl_ms: u64) -> Result<(), ApplyError> {
    validate_key("lease key", key)?;
    validate_key("lease owner", owner)?;
    if !(1..=MAX_LEASE_TTL_MS).contains(&ttl_ms) {
        return Err(ApplyError::InvalidArgument(format!(
            "lease ttl must be 1..={MAX_LEASE_TTL_MS} ms, got {ttl_ms}"
        )));
    }
    Ok(())
}

fn ttl_ms(ttl: Duration) -> u64 {
    u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
}

/// Checks, inside `txn`, that the fence's lease is still at its epoch and not
/// released, and locks the lease record so a takeover or release committed
/// concurrently conflicts with the fenced write. `get_for_update` reads the
/// latest lease in a pessimistic transaction (whose start-timestamp reads
/// may predate its locks) and the start-timestamp one in an optimistic
/// transaction, which then conflicts at commit if the lease moved.
pub(crate) async fn check_fence(
    txn: &mut Txn,
    fence: &Fence,
) -> Result<Result<(), ApplyError>, TxnError> {
    let key = keys::lease(&fence.lease);
    let lease: Option<Lease> = match txn.get_for_update(&key).await? {
        Some(v) => Some(keys::decode("lease", &v).map_err(crate::fatal)?),
        None => None,
    };
    match lease {
        Some(lease) if lease.epoch == fence.epoch && lease.owner.is_some() => Ok(Ok(())),
        _ => Ok(Err(ApplyError::Fenced {
            lease: fence.lease.clone(),
        })),
    }
}

impl TikvMeta {
    async fn lease_write(
        &self,
        kind: Kind,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant> {
        let ttl_ms = ttl_ms(ttl);
        validate_lease_args(key, owner, ttl_ms)?;
        let op = match kind {
            Kind::Acquire => "meta.acquire_lease",
            Kind::Renew => "meta.renew_lease",
            Kind::Reacquire => "meta.reacquire_lease",
        };
        let (key, owner) = (key.to_string(), owner.to_string());
        self.write_plain(op, move |txn| {
            let (key, owner) = (key.clone(), owner.clone());
            Box::pin(async move {
                let now = Tikv::physical_ms(&txn.start_ts());
                let record_key = keys::lease(&key);
                let current: Option<Lease> = load(txn, "lease", &record_key).await?;
                let deadline_ms = now.saturating_add(ttl_ms);
                let lost = || ApplyError::LeaseLost { key: key.clone() };
                let epoch = match (kind, current) {
                    (Kind::Acquire, None) => 1,
                    (Kind::Acquire, Some(current)) if current.is_held_at(now) => {
                        if current.owner.as_deref() != Some(owner.as_str()) {
                            return Ok(Err(ApplyError::LeaseHeld {
                                owner: current.owner.unwrap_or_default(),
                                deadline_ms: current.deadline_ms,
                            }));
                        }
                        current.epoch
                    }
                    (Kind::Acquire, Some(current)) => current.epoch + 1,
                    (_, None) => return Ok(Err(lost())),
                    (Kind::Renew, Some(current)) => {
                        if current.epoch != epoch
                            || current.owner.as_deref() != Some(owner.as_str())
                            || !current.is_held_at(now)
                        {
                            return Ok(Err(lost()));
                        }
                        epoch
                    }
                    (Kind::Reacquire, Some(current)) => {
                        if current.epoch != epoch
                            || current.owner.as_deref() != Some(owner.as_str())
                        {
                            return Ok(Err(lost()));
                        }
                        epoch
                    }
                };
                let lease = Lease {
                    epoch,
                    owner: Some(owner),
                    deadline_ms,
                };
                txn.put(&record_key, keys::encode(&lease)).await?;
                Ok(Ok(LeaseGrant { epoch, deadline_ms }))
            })
        })
        .await
    }

    pub(crate) async fn acquire_lease_impl(
        &self,
        key: &str,
        owner: &str,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant> {
        self.lease_write(Kind::Acquire, key, owner, 0, ttl).await
    }

    pub(crate) async fn renew_lease_impl(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant> {
        self.lease_write(Kind::Renew, key, owner, epoch, ttl).await
    }

    pub(crate) async fn reacquire_lease_impl(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
        ttl: Duration,
    ) -> MetaResult<LeaseGrant> {
        self.lease_write(Kind::Reacquire, key, owner, epoch, ttl)
            .await
    }

    pub(crate) async fn release_lease_impl(
        &self,
        key: &str,
        owner: &str,
        epoch: u64,
    ) -> MetaResult<()> {
        let (key, owner) = (key.to_string(), owner.to_string());
        self.write_plain("meta.release_lease", move |txn| {
            let (key, owner) = (key.clone(), owner.clone());
            Box::pin(async move {
                let record_key = keys::lease(&key);
                let lost = || ApplyError::LeaseLost { key: key.clone() };
                let Some(mut lease) = load::<Lease>(txn, "lease", &record_key).await? else {
                    return Ok(Err(lost()));
                };
                if lease.epoch != epoch {
                    return Ok(Err(lost()));
                }
                match lease.owner.as_deref() {
                    None => Ok(Ok(())),
                    Some(current) if current == owner => {
                        lease.owner = None;
                        txn.put(&record_key, keys::encode(&lease)).await?;
                        Ok(Ok(()))
                    }
                    Some(_) => Ok(Err(lost())),
                }
            })
        })
        .await
    }

    pub(crate) async fn lease_impl(&self, key: &str) -> MetaResult<Option<Lease>> {
        let key = keys::lease(key);
        self.read(move |snap| {
            let key = key.clone();
            Box::pin(async move { Ok(Ok(load(snap, "lease", &key).await?)) })
        })
        .await
    }

    pub(crate) async fn leases_with_prefix_impl(
        &self,
        prefix: &str,
    ) -> MetaResult<Vec<(String, Lease)>> {
        let prefix = keys::lease(prefix);
        self.read(move |snap| {
            let prefix = prefix.clone();
            Box::pin(async move {
                let pairs = snap.scan_prefix(&prefix).await?;
                let leases: Vec<Lease> = decode_all("lease", &pairs)?;
                Ok(Ok(pairs
                    .into_iter()
                    .zip(leases)
                    .filter_map(|((key, _), lease)| keys::lease_key(&key).map(|k| (k, lease)))
                    .collect()))
            })
        })
        .await
    }
}
