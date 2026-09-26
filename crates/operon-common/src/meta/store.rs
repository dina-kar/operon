/// How fresh a metastore read must be.
///
/// `Linearizable` reflects every write acknowledged before the read began.
/// `Local` returns one consistent state that may be stale; successive
/// `Local` reads through one handle never go backwards. A backend may serve
/// `Local` as `Linearizable`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Consistency {
    /// Reflects every write acknowledged before the call began. Served only by
    /// the leader, after it confirms its leadership with a quorum.
    Linearizable,
    /// Whatever this node has applied so far; may be stale on a follower or on
    /// a leader that has been cut off.
    Local,
}
