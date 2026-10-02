//! The stream ingest ledger of idempotency keys (design §02 §7.4, D270):
//! claim, complete, release and prune.

use std::collections::BTreeMap;

use loams_common::StreamId;
use loams_common::meta::{
    ApplyError, Fence, IdempotencyEntry, IdempotencyKey, IdempotencyState, MAX_IDEMPOTENCY_KEYS,
    MAX_IDEMPOTENCY_TTL_MS,
};

use super::{MetaState, validate_key};
use crate::command::Reply;

/// The checks every ledger write shares: an owner, 1..=MAX keys, a TTL in
/// 1..=MAX.
pub(crate) fn validate_idempotency_args(
    owner: &str,
    keys: usize,
    ttl_ms: Option<u64>,
) -> Result<(), ApplyError> {
    validate_key("idempotency owner", owner)?;
    if keys == 0 || keys > MAX_IDEMPOTENCY_KEYS {
        return Err(ApplyError::InvalidArgument(format!(
            "an idempotency request carries 1..={MAX_IDEMPOTENCY_KEYS} keys, got {keys}"
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

/// Refuses a key that appears twice in one request.
pub(crate) fn refuse_repeats<'a>(
    keys: impl IntoIterator<Item = &'a IdempotencyKey>,
) -> Result<(), ApplyError> {
    let mut seen = std::collections::BTreeSet::new();
    for key in keys {
        if !seen.insert(*key) {
            return Err(ApplyError::InvalidArgument(
                "an idempotency request repeats a key".to_string(),
            ));
        }
    }
    Ok(())
}

impl MetaState {
    pub(super) fn claim_idempotency_keys(
        &mut self,
        stream: StreamId,
        owner: String,
        keys: Vec<IdempotencyKey>,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<Reply, ApplyError> {
        validate_idempotency_args(&owner, keys.len(), Some(ttl_ms))?;
        refuse_repeats(&keys)?;
        if !self.streams.contains_key(&stream) {
            return Err(ApplyError::StreamNotFound(stream));
        }
        let now = self.clock_ms.max(now_ms);
        self.clock_ms = now;
        let until_ms = now.saturating_add(ttl_ms);
        let states = keys
            .into_iter()
            .map(|key| {
                let slot = (stream, key);
                let state = match self.idempotency.get(&slot) {
                    Some(IdempotencyEntry::Done {
                        partition,
                        offset,
                        until_ms,
                    }) if now < *until_ms => IdempotencyState::Done {
                        partition: *partition,
                        offset: *offset,
                    },
                    Some(IdempotencyEntry::Pending {
                        owner: holder,
                        until_ms,
                    }) if now < *until_ms && *holder != owner => IdempotencyState::InFlight {
                        until_ms: *until_ms,
                    },
                    _ => IdempotencyState::Claimed,
                };
                if state == IdempotencyState::Claimed {
                    self.idempotency.insert(
                        slot,
                        IdempotencyEntry::Pending {
                            owner: owner.clone(),
                            until_ms,
                        },
                    );
                }
                state
            })
            .collect();
        Ok(Reply::IdempotencyClaimed { states })
    }

    pub(super) fn complete_idempotency_keys(
        &mut self,
        stream: StreamId,
        owner: String,
        done: Vec<(IdempotencyKey, u32, u64)>,
        window_ms: u64,
        now_ms: u64,
    ) -> Result<Reply, ApplyError> {
        validate_idempotency_args(&owner, done.len(), Some(window_ms))?;
        refuse_repeats(done.iter().map(|(key, _, _)| key))?;
        let now = self.clock_ms.max(now_ms);
        self.clock_ms = now;
        for (key, partition, offset) in done {
            let entry = self.idempotency.get_mut(&(stream, key));
            if let Some(entry) = entry
                && matches!(entry, IdempotencyEntry::Pending { owner: holder, .. } if *holder == owner)
            {
                *entry = IdempotencyEntry::Done {
                    partition,
                    offset,
                    until_ms: now.saturating_add(window_ms),
                };
            }
        }
        Ok(Reply::IdempotencyKeysUpdated)
    }

    pub(super) fn release_idempotency_keys(
        &mut self,
        stream: StreamId,
        owner: String,
        keys: Vec<IdempotencyKey>,
    ) -> Result<Reply, ApplyError> {
        validate_idempotency_args(&owner, keys.len(), None)?;
        for key in keys {
            let slot = (stream, key);
            if matches!(
                self.idempotency.get(&slot),
                Some(IdempotencyEntry::Pending { owner: holder, .. }) if *holder == owner
            ) {
                self.idempotency.remove(&slot);
            }
        }
        Ok(Reply::IdempotencyKeysUpdated)
    }

    pub(super) fn prune_idempotency_keys(
        &mut self,
        fence: Option<Fence>,
        now_ms: u64,
    ) -> Result<Reply, ApplyError> {
        if let Some(fence) = &fence {
            self.check_fence(fence)?;
        }
        self.clock_ms = self.clock_ms.max(now_ms);
        let clock_ms = self.clock_ms;
        let before = self.idempotency.len();
        self.idempotency
            .retain(|_, entry| entry.is_live_at(clock_ms));
        let removed = before - self.idempotency.len();
        Ok(Reply::Pruned {
            removed: u32::try_from(removed).unwrap_or(u32::MAX),
        })
    }

    /// The ledger entry of `key` in `stream`, lapsed or not.
    pub fn idempotency_entry(
        &self,
        stream: StreamId,
        key: &IdempotencyKey,
    ) -> Option<&IdempotencyEntry> {
        self.idempotency.get(&(stream, *key))
    }

    /// Forgets every ledger entry of a dropped stream.
    pub(super) fn drop_stream_idempotency(&mut self, stream: StreamId) {
        self.idempotency.retain(|(id, _), _| *id != stream);
    }

    /// The whole ledger (the snapshot codec's encode).
    pub(crate) fn idempotency_map(
        &self,
    ) -> &BTreeMap<(StreamId, IdempotencyKey), IdempotencyEntry> {
        &self.idempotency
    }

    /// Replaces the ledger (the snapshot codec's decode).
    pub(crate) fn set_idempotency_map(
        &mut self,
        map: BTreeMap<(StreamId, IdempotencyKey), IdempotencyEntry>,
    ) {
        self.idempotency = map;
    }
}
