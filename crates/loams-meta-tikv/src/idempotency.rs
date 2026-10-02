//! The stream ingest ledger (design §02 §7.4, D270): claim, complete,
//! release and prune, with the rules of
//! `loams-meta/src/state/idempotency.rs`. Time is the transaction's start
//! timestamp, the metastore clock of that transaction (row R2).

use std::collections::BTreeSet;

use loams_common::StreamId;
use loams_common::meta::{
    ApplyError, Consistency, Fence, IdempotencyClaim, IdempotencyCompletion, IdempotencyEntry,
    IdempotencyKey, IdempotencyState, MAX_IDEMPOTENCY_KEYS, MAX_IDEMPOTENCY_TTL_MS, MetaResult,
};
use loams_tikv::{Tikv, TxnError};

use crate::catalog::validate_key;
use crate::keys;
use crate::leases::check_fence;
use crate::{TikvMeta, fatal};

/// Ledger entries a prune transaction scans.
const PRUNE_BATCH: usize = 256;

/// The checks every ledger write shares: an owner, 1..=MAX keys, a TTL in
/// 1..=MAX, no repeated key.
fn validate_args<'a>(
    owner: &str,
    keys: impl ExactSizeIterator<Item = &'a IdempotencyKey>,
    ttl_ms: Option<u64>,
) -> Result<(), ApplyError> {
    validate_key("idempotency owner", owner)?;
    let count = keys.len();
    if count == 0 || count > MAX_IDEMPOTENCY_KEYS {
        return Err(ApplyError::InvalidArgument(format!(
            "an idempotency request carries 1..={MAX_IDEMPOTENCY_KEYS} keys, got {count}"
        )));
    }
    if let Some(ttl_ms) = ttl_ms
        && (ttl_ms == 0 || ttl_ms > MAX_IDEMPOTENCY_TTL_MS)
    {
        return Err(ApplyError::InvalidArgument(format!(
            "an idempotency TTL is 1..={MAX_IDEMPOTENCY_TTL_MS} ms, got {ttl_ms}"
        )));
    }
    Ok(())
}

fn refuse_repeats<'a>(
    keys: impl IntoIterator<Item = &'a IdempotencyKey>,
) -> Result<(), ApplyError> {
    let mut seen = BTreeSet::new();
    for key in keys {
        if !seen.insert(*key) {
            return Err(ApplyError::InvalidArgument(
                "an idempotency request repeats a key".to_string(),
            ));
        }
    }
    Ok(())
}

fn is_live(entry: &IdempotencyEntry, now_ms: u64) -> bool {
    now_ms < entry.until_ms()
}

impl TikvMeta {
    pub(crate) async fn claim_idempotency_keys_impl(
        &self,
        claim: IdempotencyClaim,
    ) -> MetaResult<Vec<IdempotencyState>> {
        validate_args(&claim.owner, claim.keys.iter(), Some(claim.ttl_ms))?;
        refuse_repeats(&claim.keys)?;
        self.reach_now().await;
        self.write_plain("meta.claim_idempotency_keys", move |txn| {
            let claim = claim.clone();
            Box::pin(async move {
                let now = Tikv::physical_ms(&txn.start_ts());
                if txn
                    .get_for_update(&keys::stream(claim.stream))
                    .await?
                    .is_none()
                {
                    return Ok(Err(ApplyError::StreamNotFound(claim.stream)));
                }
                let slots: Vec<Vec<u8>> = claim
                    .keys
                    .iter()
                    .map(|key| keys::idempotency(claim.stream, key))
                    .collect();
                let found = txn.batch_get_for_update(slots.iter()).await?;
                let until_ms = now.saturating_add(claim.ttl_ms);
                let mut states = Vec::with_capacity(slots.len());
                for slot in &slots {
                    let entry: Option<IdempotencyEntry> =
                        match found.iter().find(|(key, _)| key == slot) {
                            Some((_, value)) => {
                                Some(keys::decode("idempotency entry", value).map_err(fatal)?)
                            }
                            None => None,
                        };
                    let state = match entry {
                        Some(IdempotencyEntry::Done {
                            partition,
                            offset,
                            until_ms,
                        }) if now < until_ms => IdempotencyState::Done { partition, offset },
                        Some(IdempotencyEntry::Pending { owner, until_ms })
                            if now < until_ms && owner != claim.owner =>
                        {
                            IdempotencyState::InFlight { until_ms }
                        }
                        _ => IdempotencyState::Claimed,
                    };
                    if state == IdempotencyState::Claimed {
                        let pending = IdempotencyEntry::Pending {
                            owner: claim.owner.clone(),
                            until_ms,
                        };
                        txn.put(slot, keys::encode(&pending)).await?;
                    }
                    states.push(state);
                }
                Ok(Ok(states))
            })
        })
        .await
    }

    pub(crate) async fn complete_idempotency_keys_impl(
        &self,
        completion: IdempotencyCompletion,
    ) -> MetaResult<()> {
        validate_args(
            &completion.owner,
            completion.done.iter().map(|(key, _, _)| key),
            Some(completion.window_ms),
        )?;
        refuse_repeats(completion.done.iter().map(|(key, _, _)| key))?;
        self.reach_now().await;
        self.write_plain("meta.complete_idempotency_keys", move |txn| {
            let completion = completion.clone();
            Box::pin(async move {
                let now = Tikv::physical_ms(&txn.start_ts());
                let slots: Vec<Vec<u8>> = completion
                    .done
                    .iter()
                    .map(|(key, _, _)| keys::idempotency(completion.stream, key))
                    .collect();
                let found = txn.batch_get_for_update(slots.iter()).await?;
                for (slot, (_, partition, offset)) in slots.iter().zip(&completion.done) {
                    let Some((_, value)) = found.iter().find(|(key, _)| key == slot) else {
                        continue;
                    };
                    let entry: IdempotencyEntry =
                        keys::decode("idempotency entry", value).map_err(fatal)?;
                    if matches!(&entry, IdempotencyEntry::Pending { owner, .. } if *owner == completion.owner)
                    {
                        let done = IdempotencyEntry::Done {
                            partition: *partition,
                            offset: *offset,
                            until_ms: now.saturating_add(completion.window_ms),
                        };
                        txn.put(slot, keys::encode(&done)).await?;
                    }
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(crate) async fn release_idempotency_keys_impl(
        &self,
        stream: StreamId,
        owner: &str,
        ledger_keys: Vec<IdempotencyKey>,
    ) -> MetaResult<()> {
        validate_args(owner, ledger_keys.iter(), None)?;
        let owner = owner.to_string();
        self.write_plain("meta.release_idempotency_keys", move |txn| {
            let (owner, ledger_keys) = (owner.clone(), ledger_keys.clone());
            Box::pin(async move {
                let slots: Vec<Vec<u8>> = ledger_keys
                    .iter()
                    .map(|key| keys::idempotency(stream, key))
                    .collect();
                for (slot, value) in txn.batch_get_for_update(slots.iter()).await? {
                    let entry: IdempotencyEntry =
                        keys::decode("idempotency entry", &value).map_err(fatal)?;
                    if matches!(&entry, IdempotencyEntry::Pending { owner: holder, .. } if *holder == owner)
                    {
                        txn.delete(&slot).await?;
                    }
                }
                Ok(Ok(()))
            })
        })
        .await
    }

    pub(crate) async fn idempotency_key_impl(
        &self,
        _consistency: Consistency,
        stream: StreamId,
        key: IdempotencyKey,
    ) -> MetaResult<Option<IdempotencyEntry>> {
        let slot = keys::idempotency(stream, &key);
        self.read(move |snap| {
            let slot = slot.clone();
            Box::pin(async move {
                Ok(Ok(match snap.get(&slot).await? {
                    Some(value) => Some(keys::decode("idempotency entry", &value).map_err(fatal)?),
                    None => None,
                }))
            })
        })
        .await
    }

    /// Deletes every ledger entry of `stream`, [`PRUNE_BATCH`] per
    /// transaction.
    pub(crate) async fn purge_idempotency_stream(&self, stream: StreamId) -> MetaResult<()> {
        let (lo, hi) = keys::prefix_range(&keys::idempotency_entries(stream));
        loop {
            let (from, hi) = (lo.clone(), hi.clone());
            let emptied = self
                .write_plain("meta.purge_idempotency_stream", move |txn| {
                    let (from, hi) = (from.clone(), hi.clone());
                    Box::pin(async move {
                        let page = txn.scan(&from, hi.as_deref(), PRUNE_BATCH).await?;
                        let last = page.len() < PRUNE_BATCH;
                        for (key, _) in page {
                            txn.delete(&key).await?;
                        }
                        Ok::<_, TxnError>(Ok(last))
                    })
                })
                .await?;
            if emptied {
                return Ok(());
            }
        }
    }

    /// One transaction per page of [`PRUNE_BATCH`] entries, each checking
    /// the fence and stamped with its start timestamp.
    pub(crate) async fn prune_idempotency_keys_impl(
        &self,
        fence: Option<Fence>,
    ) -> MetaResult<u32> {
        self.reach_now().await;
        let (lo, hi) = keys::prefix_range(keys::IDEMPOTENCY);
        let mut start = lo;
        let mut removed = 0u32;
        loop {
            let (fence, from, hi) = (fence.clone(), start.clone(), hi.clone());
            let (count, next) = self
                .write("meta.prune_idempotency_keys", move |txn| {
                    let (fence, from, hi) = (fence.clone(), from.clone(), hi.clone());
                    Box::pin(async move {
                        if let Some(fence) = &fence
                            && let Err(e) = check_fence(txn, fence).await?
                        {
                            return Ok(Err(e));
                        }
                        let now = Tikv::physical_ms(&txn.start_ts());
                        let page = txn.scan(&from, hi.as_deref(), PRUNE_BATCH).await?;
                        let next = (page.len() == PRUNE_BATCH)
                            .then(|| page.last().map(|(k, _)| after(k)))
                            .flatten();
                        let mut count = 0u32;
                        for (key, value) in page {
                            let entry: IdempotencyEntry =
                                keys::decode("idempotency entry", &value).map_err(fatal)?;
                            if !is_live(&entry, now) {
                                txn.delete(&key).await?;
                                count = count.saturating_add(1);
                            }
                        }
                        Ok::<_, TxnError>(Ok((count, next)))
                    })
                })
                .await
                .0?;
            removed = removed.saturating_add(count);
            match next {
                Some(next) => start = next,
                None => return Ok(removed),
            }
        }
    }
}

/// The first key after `key`.
fn after(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}
