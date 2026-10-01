//! The mock's in-memory state and the approval change log that watch
//! streams resume from (AP0 Ruling 3).

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Duration;

use tokio::sync::broadcast;

use crate::proto::loams::approvals::v1::{Approval, DecideApprovalResponse};
use crate::proto::loams::devices::v1::Device;
use crate::proto::loams::notifications::v1::Notification;
use crate::proto::loams::operations::v1::Operation;
use crate::seed::Seed;

/// How many approval changes a cursor can resume across; older cursors get
/// a fresh snapshot with `snapshot_reset`.
pub(crate) const LOG_CAPACITY: usize = 256;

/// One change to the approvals, numbered by `seq`.
#[derive(Debug, Clone)]
pub(crate) struct ApprovalChange {
    pub(crate) seq: u64,
    pub(crate) approval: Approval,
}

#[derive(Debug, Default)]
pub(crate) struct State {
    pub(crate) approvals: BTreeMap<String, Approval>,
    pub(crate) operations: BTreeMap<String, Operation>,
    pub(crate) notifications: BTreeMap<String, Notification>,
    /// Device id → (owner principal id, device).
    pub(crate) devices: BTreeMap<String, (String, Device)>,
    /// Idempotency key → the response it produced (AP0 Ruling 5).
    pub(crate) decided: HashMap<String, DecideApprovalResponse>,
    /// The approval change log, oldest first, at most `LOG_CAPACITY` long.
    pub(crate) log: VecDeque<ApprovalChange>,
    pub(crate) seq: u64,
}

#[derive(Debug)]
pub(crate) struct Store {
    pub(crate) seed: Seed,
    pub(crate) heartbeat: Duration,
    pub(crate) state: Mutex<State>,
    pub(crate) changes: broadcast::Sender<ApprovalChange>,
}

impl Store {
    pub(crate) fn new(seed: Seed, heartbeat: Duration) -> Self {
        let state = State {
            approvals: seed
                .approvals
                .iter()
                .map(|a| (a.id.clone(), a.clone()))
                .collect(),
            operations: seed
                .operations
                .iter()
                .map(|o| (o.id.clone(), o.clone()))
                .collect(),
            notifications: seed
                .notifications
                .iter()
                .map(|n| (n.id.clone(), n.clone()))
                .collect(),
            devices: seed
                .devices
                .iter()
                .map(|(owner, d)| (d.id.clone(), (owner.clone(), d.clone())))
                .collect(),
            ..State::default()
        };
        let (changes, _) = broadcast::channel(LOG_CAPACITY);
        Self {
            seed,
            heartbeat,
            state: Mutex::new(state),
            changes,
        }
    }

    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl State {
    /// Stores a new revision of an approval and logs the change; the caller
    /// broadcasts the returned change after releasing the lock.
    pub(crate) fn record(&mut self, approval: Approval) -> ApprovalChange {
        self.seq += 1;
        let change = ApprovalChange {
            seq: self.seq,
            approval: approval.clone(),
        };
        self.approvals.insert(approval.id.clone(), approval);
        self.log.push_back(change.clone());
        while self.log.len() > LOG_CAPACITY {
            self.log.pop_front();
        }
        change
    }

    /// The changes after `cursor`, or `None` when the cursor is older than
    /// the log (the stream must reset).
    pub(crate) fn since(&self, cursor: u64) -> Option<Vec<ApprovalChange>> {
        if cursor > self.seq {
            return None;
        }
        let oldest = self.log.front().map_or(self.seq + 1, |c| c.seq);
        if cursor + 1 < oldest {
            return None;
        }
        Some(
            self.log
                .iter()
                .filter(|c| c.seq > cursor)
                .cloned()
                .collect(),
        )
    }
}

/// Cursors are opaque to clients; the mock writes `c<seq>`.
pub(crate) fn cursor(seq: u64) -> String {
    format!("c{seq}")
}

pub(crate) fn parse_cursor(cursor: &str) -> Option<u64> {
    cursor.strip_prefix('c')?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn since_replays_or_asks_for_a_reset() {
        let store = Store::new(Seed::demo(), Duration::from_secs(15));
        let mut state = store.lock();
        let approval = state.approvals.values().next().cloned().unwrap();
        for _ in 0..3 {
            state.record(approval.clone());
        }
        assert_eq!(state.since(1).unwrap().len(), 2);
        assert_eq!(state.since(3).unwrap().len(), 0);
        assert!(state.since(4).is_none(), "a cursor from the future resets");
        for _ in 0..LOG_CAPACITY {
            state.record(approval.clone());
        }
        assert!(state.since(1).is_none(), "an evicted cursor resets");
        assert_eq!(parse_cursor(&cursor(42)), Some(42));
        assert_eq!(parse_cursor("garbage"), None);
    }
}
