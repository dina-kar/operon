------------------------- MODULE MCReshardCutover -------------------------
(* Model of ReshardCutover for TLC and Apalache; each .cfg picks bounds.   *)
EXTENDS ReshardCutover

SmallKeys == {"k1"}
SmallInstances == {"d", "i2"}
NightlyKeys == {"k1", "k2"}
NightlyInstances == {"d", "i2", "i3"}

\* Apalache: constant initialisation for the Small bounds.
CInitSmall ==
    /\ Keys = SmallKeys
    /\ Instances = SmallInstances
    /\ Designated = "d"
    /\ MaxWrites = 3
    /\ FenceEnabled = TRUE
    /\ SagaCrashes = 1

=============================================================================
