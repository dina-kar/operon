//! Leases with epochs, for worker tasks and fencing (design §09 §3, §6).

use super::{MAX_LEASE_TTL_MS, MetaState, validate_key};
use crate::command::{ApplyError, Reply};
use crate::types::{Fence, Lease, LeaseGrant};

impl MetaState {
    pub(super) fn acquire_lease(
        &mut self,
        key: String,
        owner: String,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<Reply, ApplyError> {
        validate_lease_args(&key, &owner, ttl_ms)?;
        let now = self.clock_ms.max(now_ms);
        let deadline_ms = now.saturating_add(ttl_ms);
        let lease = match self.leases.get(&key) {
            None => Lease {
                epoch: 1,
                owner: Some(owner),
                deadline_ms,
            },
            Some(current) if current.is_held_at(now) => {
                if current.owner.as_deref() != Some(owner.as_str()) {
                    return Err(ApplyError::LeaseHeld {
                        owner: current.owner.clone().unwrap_or_default(),
                        deadline_ms: current.deadline_ms,
                    });
                }
                Lease {
                    epoch: current.epoch,
                    owner: Some(owner),
                    deadline_ms,
                }
            }
            Some(current) => Lease {
                epoch: current.epoch + 1,
                owner: Some(owner),
                deadline_ms,
            },
        };
        let grant = LeaseGrant {
            epoch: lease.epoch,
            deadline_ms,
        };
        self.clock_ms = now;
        self.leases.insert(key, lease);
        Ok(Reply::Lease(grant))
    }

    pub(super) fn renew_lease(
        &mut self,
        key: String,
        owner: String,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<Reply, ApplyError> {
        validate_lease_args(&key, &owner, ttl_ms)?;
        let now = self.clock_ms.max(now_ms);
        let Some(lease) = self.leases.get_mut(&key) else {
            return Err(ApplyError::LeaseLost { key });
        };
        if lease.epoch != epoch
            || lease.owner.as_deref() != Some(owner.as_str())
            || !lease.is_held_at(now)
        {
            return Err(ApplyError::LeaseLost { key });
        }
        lease.deadline_ms = now.saturating_add(ttl_ms);
        let grant = LeaseGrant {
            epoch,
            deadline_ms: lease.deadline_ms,
        };
        self.clock_ms = now;
        Ok(Reply::Lease(grant))
    }

    pub(super) fn reacquire_lease(
        &mut self,
        key: String,
        owner: String,
        epoch: u64,
        ttl_ms: u64,
        now_ms: u64,
    ) -> Result<Reply, ApplyError> {
        validate_lease_args(&key, &owner, ttl_ms)?;
        let now = self.clock_ms.max(now_ms);
        let Some(lease) = self.leases.get_mut(&key) else {
            return Err(ApplyError::LeaseLost { key });
        };
        if lease.epoch != epoch || lease.owner.as_deref() != Some(owner.as_str()) {
            return Err(ApplyError::LeaseLost { key });
        }
        lease.deadline_ms = now.saturating_add(ttl_ms);
        let grant = LeaseGrant {
            epoch,
            deadline_ms: lease.deadline_ms,
        };
        self.clock_ms = now;
        Ok(Reply::Lease(grant))
    }

    pub(super) fn release_lease(
        &mut self,
        key: String,
        owner: String,
        epoch: u64,
    ) -> Result<Reply, ApplyError> {
        let Some(lease) = self.leases.get_mut(&key) else {
            return Err(ApplyError::LeaseLost { key });
        };
        if lease.epoch != epoch {
            return Err(ApplyError::LeaseLost { key });
        }
        match lease.owner.as_deref() {
            None => Ok(Reply::LeaseReleased),
            Some(current) if current == owner => {
                lease.owner = None;
                Ok(Reply::LeaseReleased)
            }
            Some(_) => Err(ApplyError::LeaseLost { key }),
        }
    }

    /// Checks that the fence's lease is still at the fence's epoch and not released.
    pub(super) fn check_fence(&self, fence: &Fence) -> Result<(), ApplyError> {
        match self.leases.get(&fence.lease) {
            Some(lease) if lease.epoch == fence.epoch && lease.owner.is_some() => Ok(()),
            _ => Err(ApplyError::Fenced {
                lease: fence.lease.clone(),
            }),
        }
    }

    /// The lease on `key`, if it was ever acquired.
    pub fn lease(&self, key: &str) -> Option<&Lease> {
        self.leases.get(key)
    }
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
