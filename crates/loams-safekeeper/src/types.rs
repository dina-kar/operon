//! The acceptor's data types: LSNs, terms, timeline ids, term histories, the
//! membership configuration and the durable acceptor state.
//!
//! The shapes follow Neon's `safekeeper/src/safekeeper.rs`, `libs/utils/src/lsn.rs`
//! and `libs/safekeeper_api` (Apache-2.0), so that the wire codecs in
//! [`crate::proto`] map one to one.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

use crate::Error;

/// A proposer term (walproposer's Paxos ballot).
pub type Term = u64;

/// A safekeeper node id, as walproposer sees it.
pub type NodeId = u64;

/// A Postgres WAL position.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Lsn(pub u64);

impl Lsn {
    /// The invalid LSN, as `InvalidXLogRecPtr`.
    pub const INVALID: Lsn = Lsn(0);

    /// `self + n`, or an error on overflow.
    pub fn checked_add(self, n: u64) -> Result<Lsn, Error> {
        self.0
            .checked_add(n)
            .map(Lsn)
            .ok_or_else(|| Error::Protocol(format!("LSN overflow: {self} + {n}")))
    }
}

impl fmt::Display for Lsn {
    /// Postgres' `X/Y` notation.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:X}/{:X}", self.0 >> 32, self.0 & 0xffff_ffff)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for Lsn {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let bad = || Error::Protocol(format!("invalid LSN {s:?}"));
        let (hi, lo) = s.split_once('/').ok_or_else(bad)?;
        let hi = u32::from_str_radix(hi, 16).map_err(|_| bad())?;
        let lo = u32::from_str_radix(lo, 16).map_err(|_| bad())?;
        Ok(Lsn((u64::from(hi) << 32) | u64::from(lo)))
    }
}

/// A 16-byte Neon id (tenant or timeline), hex-encoded on the wire.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Id(pub [u8; 16]);

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(self.0))
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for Id {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut out = [0u8; 16];
        hex::decode_to_slice(s, &mut out)
            .map_err(|_| Error::Protocol(format!("invalid id {s:?}: want 32 hex digits")))?;
        Ok(Id(out))
    }
}

/// A timeline: `(tenant_id, timeline_id)` in Neon's ids.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TimelineId {
    pub tenant: Id,
    pub timeline: Id,
}

impl TimelineId {
    pub fn new(tenant: Id, timeline: Id) -> Self {
        Self { tenant, timeline }
    }

    /// The 32-byte key prefix: tenant, then timeline.
    pub fn to_bytes(&self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[..16].copy_from_slice(&self.tenant.0);
        out[16..].copy_from_slice(&self.timeline.0);
        out
    }
}

impl fmt::Display for TimelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.tenant, self.timeline)
    }
}

impl fmt::Debug for TimelineId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// The start of a term in the WAL.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TermLsn {
    pub term: Term,
    pub lsn: Lsn,
}

impl From<(Term, Lsn)> for TermLsn {
    fn from((term, lsn): (Term, Lsn)) -> Self {
        Self { term, lsn }
    }
}

/// The history of term switches: `(term, start_lsn)`, ascending.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TermHistory(pub Vec<TermLsn>);

impl TermHistory {
    /// The entries that start at or below `up_to`.
    pub fn up_to(&self, up_to: Lsn) -> TermHistory {
        TermHistory(
            self.0
                .iter()
                .take_while(|e| e.lsn <= up_to)
                .copied()
                .collect(),
        )
    }

    /// The point where the proposer's and the acceptor's histories diverge.
    ///
    /// The proposer's history ends at +infinity and the acceptor's at
    /// `sk_wal_end`. Ported from Neon's `TermHistory::find_highest_common_point`
    /// (and walproposer's `SendProposerElected`); the invariant violations that
    /// Neon asserts are errors here.
    pub fn find_highest_common_point(
        prop: &TermHistory,
        sk: &TermHistory,
        sk_wal_end: Lsn,
    ) -> Result<Option<TermLsn>, Error> {
        let (prop, sk) = (&prop.0, &sk.0);
        if let Some(last) = sk.last()
            && last.lsn > sk_wal_end
        {
            return Err(Error::Protocol(format!(
                "acceptor term history ends at {:?}, above its WAL end {sk_wal_end}",
                last
            )));
        }
        let mut last_common = None;
        for i in 0..prop.len().min(sk.len()) {
            if prop[i].term != sk[i].term {
                break;
            }
            if prop[i].lsn != sk[i].lsn {
                return Err(Error::Protocol(format!(
                    "term {} starts at {} for the proposer and at {} for the acceptor",
                    prop[i].term, prop[i].lsn, sk[i].lsn
                )));
            }
            last_common = Some(i);
        }
        let Some(i) = last_common else {
            return Ok(None);
        };
        let lsn = if i == prop.len() - 1 {
            sk_wal_end
        } else {
            let prop_end = prop[i + 1].lsn;
            let sk_end = sk.get(i + 1).map_or(sk_wal_end, |e| e.lsn);
            prop_end.min(sk_end)
        };
        Ok(Some(TermLsn {
            term: prop[i].term,
            lsn,
        }))
    }
}

/// A member of a safekeeper configuration.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SafekeeperId {
    pub id: NodeId,
    pub host: String,
    pub pg_port: u16,
}

/// Neon's membership configuration. Loams serves one logical acceptor per
/// timeline (§28 §6.3), so it only echoes this back, and switches to a higher
/// generation that names it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Configuration {
    pub generation: u32,
    pub members: Vec<SafekeeperId>,
    pub new_members: Option<Vec<SafekeeperId>>,
}

impl Configuration {
    /// Whether `node` is in the configuration (either member set).
    pub fn contains(&self, node: NodeId) -> bool {
        self.members.iter().any(|m| m.id == node)
            || self
                .new_members
                .as_ref()
                .is_some_and(|n| n.iter().any(|m| m.id == node))
    }
}

/// The Postgres server a timeline belongs to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerInfo {
    /// The full version, e.g. 160009 (`PG_VERSION_NUM`); 0 when unknown.
    pub pg_version: u32,
    pub system_id: u64,
    pub wal_seg_size: u32,
}

impl ServerInfo {
    /// The major version (16 for 160009); `None` when unknown.
    pub fn pg_major(&self) -> Option<u32> {
        (self.pg_version != 0).then_some(self.pg_version / 10_000)
    }
}

/// Hot-standby feedback relayed to the proposer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HotStandbyFeedback {
    pub ts: i64,
    pub xmin: u64,
    pub catalog_xmin: u64,
}

/// The pageserver's progress, as Neon's `PageserverFeedback`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageserverFeedback {
    pub current_timeline_size: u64,
    pub last_received_lsn: Lsn,
    pub disk_consistent_lsn: Lsn,
    pub remote_consistent_lsn: Lsn,
    /// Microseconds since the Postgres epoch (2000-01-01).
    pub replytime_us: i64,
    pub shard_number: u32,
    pub corruption_detected: bool,
}

/// The durable state of one timeline's acceptor: what a safekeeper keeps in
/// its control file, plus the WAL end (the WAL itself lives in the store).
///
/// This is the head record (`H/<tl>`) of §28 §6.5.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcceptorState {
    /// The highest term voted for.
    pub term: Term,
    /// The term switches of the stored WAL. It may run past `flush_lsn`,
    /// because the proposer's history is adopted before its WAL arrives.
    pub term_history: TermHistory,
    pub mconf: Configuration,
    pub server: ServerInfo,
    /// End of the durable WAL.
    pub flush_lsn: Lsn,
    /// The highest LSN known committed (never above `flush_lsn`).
    pub commit_lsn: Lsn,
    /// The proposer's `truncate_lsn`: the lowest LSN it may still need.
    pub peer_horizon_lsn: Lsn,
    /// End of the WAL copied to the bucket.
    pub backup_lsn: Lsn,
    /// The pageserver's `remote_consistent_lsn`.
    pub remote_consistent_lsn: Lsn,
    /// Where the timeline's WAL begins.
    pub timeline_start_lsn: Lsn,
    /// Where this acceptor's WAL begins.
    pub local_start_lsn: Lsn,
    /// The lowest LSN still held in the store (trimmed below it).
    pub trimmed_lsn: Lsn,
}

impl AcceptorState {
    /// A fresh timeline.
    pub fn new(server: ServerInfo, start_lsn: Lsn) -> Self {
        Self {
            server,
            timeline_start_lsn: start_lsn,
            local_start_lsn: start_lsn,
            flush_lsn: start_lsn,
            commit_lsn: start_lsn,
            peer_horizon_lsn: start_lsn,
            backup_lsn: start_lsn,
            remote_consistent_lsn: start_lsn,
            trimmed_lsn: start_lsn,
            ..Self::default()
        }
    }

    /// The WAL end, never below the timeline start (Neon's `flush_lsn()`).
    pub fn wal_end(&self) -> Lsn {
        self.flush_lsn.max(self.timeline_start_lsn)
    }

    /// The term switches of the WAL actually stored.
    pub fn stored_term_history(&self) -> TermHistory {
        self.term_history.up_to(self.wal_end())
    }

    /// The term of the last stored WAL record (0 before any).
    pub fn last_log_term(&self) -> Term {
        self.stored_term_history().0.last().map_or(0, |e| e.term)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lsn_display_parse_round_trip() {
        let lsn = Lsn(0x1_06B5_9C00);
        assert_eq!(lsn.to_string(), "1/6B59C00");
        assert_eq!("1/6B59C00".parse::<Lsn>().unwrap(), lsn);
        assert_eq!("0/0".parse::<Lsn>().unwrap(), Lsn::INVALID);
        assert!("16B59C00".parse::<Lsn>().is_err());
    }

    #[test]
    fn id_parse() {
        let id: Id = "cf0480929707ee75372337efaa5ecf96".parse().unwrap();
        assert_eq!(id.to_string(), "cf0480929707ee75372337efaa5ecf96");
        assert!("cf04".parse::<Id>().is_err());
    }

    // The four cases of Neon's safekeeper.rs tests.
    #[test]
    fn highest_common_point_none() {
        let prop = TermHistory(vec![(0, Lsn(1)).into()]);
        let sk = TermHistory(vec![(1, Lsn(1)).into(), (2, Lsn(2)).into()]);
        assert_eq!(
            TermHistory::find_highest_common_point(&prop, &sk, Lsn(3)).unwrap(),
            None
        );
    }

    #[test]
    fn highest_common_point_middle() {
        let prop = TermHistory(vec![
            (1, Lsn(10)).into(),
            (2, Lsn(20)).into(),
            (4, Lsn(40)).into(),
        ]);
        let sk = TermHistory(vec![
            (1, Lsn(10)).into(),
            (2, Lsn(20)).into(),
            (3, Lsn(30)).into(),
        ]);
        assert_eq!(
            TermHistory::find_highest_common_point(&prop, &sk, Lsn(40)).unwrap(),
            Some((2, Lsn(30)).into())
        );
    }

    #[test]
    fn highest_common_point_sk_end() {
        let prop = TermHistory(vec![
            (1, Lsn(10)).into(),
            (2, Lsn(20)).into(),
            (4, Lsn(40)).into(),
        ]);
        let sk = TermHistory(vec![(1, Lsn(10)).into(), (2, Lsn(20)).into()]);
        assert_eq!(
            TermHistory::find_highest_common_point(&prop, &sk, Lsn(32)).unwrap(),
            Some((2, Lsn(32)).into())
        );
    }

    #[test]
    fn highest_common_point_walprop() {
        let prop = TermHistory(vec![(1, Lsn(10)).into(), (2, Lsn(20)).into()]);
        let sk = TermHistory(vec![(1, Lsn(10)).into(), (2, Lsn(20)).into()]);
        assert_eq!(
            TermHistory::find_highest_common_point(&prop, &sk, Lsn(32)).unwrap(),
            Some((2, Lsn(32)).into())
        );
    }

    #[test]
    fn highest_common_point_rejects_mismatched_start() {
        let prop = TermHistory(vec![(1, Lsn(10)).into()]);
        let sk = TermHistory(vec![(1, Lsn(11)).into()]);
        assert!(TermHistory::find_highest_common_point(&prop, &sk, Lsn(20)).is_err());
    }

    #[test]
    fn last_log_term_follows_flush_lsn() {
        let mut st = AcceptorState::new(ServerInfo::default(), Lsn(1));
        st.term_history = TermHistory(vec![(1, Lsn(1)).into(), (2, Lsn(3)).into()]);
        st.flush_lsn = Lsn(2);
        assert_eq!(st.last_log_term(), 1);
        st.flush_lsn = Lsn(3);
        assert_eq!(st.last_log_term(), 2);
    }
}
