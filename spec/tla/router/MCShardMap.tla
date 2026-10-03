---------------------------- MODULE MCShardMap ----------------------------
(* Model of ShardMap for TLC and Apalache. Constants are strings so one    *)
(* model serves both checkers; each .cfg picks the bounds.                *)
EXTENDS ShardMap

\* Small (PR) bounds.
SmallKeys == {"k1", "k2"}
SmallShards == {"s1", "s2", "s3"}
SmallInstances == {"i1", "i2"}

\* Nightly bounds.
NightlyKeys == {"k1", "k2", "k3"}
NightlyInstances == {"i1", "i2", "i3"}

\* Apalache: constant initialisation for the Small bounds.
CInitSmall ==
    /\ Keys = SmallKeys
    /\ Shards = SmallShards
    /\ Instances = SmallInstances
    /\ MaxGen = 2
    /\ MaxWrites = 3
    /\ UnsafeConfigMapFirst = FALSE

=============================================================================
