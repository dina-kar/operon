--------------------------- MODULE ReshardCutover ---------------------------
(***************************************************************************)
(* The multi-instance resharding cutover of the Loams router (design §31   *)
(* §6.4, D305).                                                            *)
(*                                                                         *)
(* PgDog's open-source RESHARD switches traffic on ONE PgDog instance. A   *)
(* production deployment runs several, so Loams drives the cutover as a    *)
(* saga across all of them, with a fence on the source cluster's backends  *)
(* (ALTER ROLE ... NOLOGIN) for the instances it cannot reach:             *)
(*   StartCopy -> CatchUp -> PauseAll -> FenceSource -> CutOverDesignated  *)
(*   -> PublishAndReload -> ResumeAll -> Finalize                          *)
(* RollBack is allowed from the cutover until Finalize; it fences the      *)
(* destination, drains the reverse stream and routes back to the source.   *)
(*                                                                         *)
(* The safety argument is per key, so each cluster is collapsed to one     *)
(* logical store and the keys only tag writes. Writes are upserts with a   *)
(* unique id, so redelivery by a stream is idempotent (set union).         *)
(*                                                                         *)
(* PauseAll pauses the instances the saga can reach; after its timeout it  *)
(* proceeds past the others, which keep routing to the source. Only the    *)
(* fence stops them, which MCReshardCutover_NoFence shows.                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    \* @type: Set(Str);
    Keys,
    \* @type: Set(Str);
    Instances,
    \* The instance whose PgDog runs the RESHARD switch.
    \* @type: Str;
    Designated,
    \* @type: Int;
    MaxWrites,
    \* @type: Bool;
    FenceEnabled,
    \* @type: Int;
    SagaCrashes

ASSUME Designated \in Instances

Phases == {"Copy", "CatchUp", "Paused", "Fenced", "CutOver", "Published",
           "Resumed", "Finalized", "RollingBack", "RolledBack"}

VARIABLES
    \* @type: Str;
    phase,
    \* @type: Str -> Bool;
    paused,
    \* Whether the saga can reach each instance.
    \* @type: Str -> Bool;
    reachable,
    \* "Src" or "Dst".
    \* @type: Str -> Str;
    routesTo,
    \* @type: Bool;
    srcFenced,
    \* @type: Bool;
    dstFenced,
    \* @type: Set({ id: Int, key: Str });
    srcData,
    \* @type: Set({ id: Int, key: Str });
    dstData,
    \* @type: Set({ id: Int, key: Str });
    acked,
    \* @type: Int;
    nextWrite,
    \* Whether the saga process is down (its step is durable in `phase`).
    \* @type: Bool;
    crashed,
    \* @type: Int;
    crashes

vars == <<phase, paused, reachable, routesTo, srcFenced, dstFenced, srcData,
          dstData, acked, nextWrite, crashed, crashes>>

TypeOK ==
    /\ phase \in Phases
    /\ paused \in [Instances -> BOOLEAN]
    /\ reachable \in [Instances -> BOOLEAN]
    /\ routesTo \in [Instances -> {"Src", "Dst"}]
    /\ srcFenced \in BOOLEAN
    /\ dstFenced \in BOOLEAN
    /\ nextWrite \in 1..(MaxWrites + 1)
    /\ crashes \in 0..SagaCrashes

\* Phases in which the saga has switched (some) traffic to the destination.
AfterCutOver == {"CutOver", "Published", "Resumed"}

-----------------------------------------------------------------------------
Init ==
    /\ phase = "Copy"
    /\ paused = [i \in Instances |-> FALSE]
    /\ reachable = [i \in Instances |-> TRUE]
    /\ routesTo = [i \in Instances |-> "Src"]
    /\ srcFenced = FALSE
    /\ dstFenced = FALSE
    /\ srcData = {}
    /\ dstData = {}
    /\ acked = {}
    /\ nextWrite = 1
    /\ crashed = FALSE
    /\ crashes = 0

\* Every saga step needs the saga to be up.
Saga(A) == ~crashed /\ A

StartCopy == Saga(
    /\ phase = "Copy"
    /\ dstData' = dstData \cup srcData
    /\ phase' = "CatchUp"
    /\ UNCHANGED <<paused, reachable, routesTo, srcFenced, dstFenced, srcData,
                   acked, nextWrite, crashed, crashes>>)

\* Pause the reachable instances; unreachable ones are passed over after
\* the pause timeout and keep running.
PauseAll == Saga(
    /\ phase = "CatchUp"
    /\ paused' = [i \in Instances |-> reachable[i]]
    /\ phase' = "Paused"
    /\ UNCHANGED <<reachable, routesTo, srcFenced, dstFenced, srcData, dstData,
                   acked, nextWrite, crashed, crashes>>)

FenceSource == Saga(
    /\ phase = "Paused"
    /\ srcFenced' = FenceEnabled
    /\ phase' = "Fenced"
    /\ UNCHANGED <<paused, reachable, routesTo, dstFenced, srcData, dstData,
                   acked, nextWrite, crashed, crashes>>)

\* RESHARD's switch on the designated instance, once the forward stream has
\* drained. The saga must reach the designated instance, which is paused.
CutOverDesignated == Saga(
    /\ phase = "Fenced"
    /\ reachable[Designated]
    /\ paused[Designated]
    /\ srcData \subseteq dstData
    /\ routesTo' = [routesTo EXCEPT ![Designated] = "Dst"]
    /\ phase' = "CutOver"
    /\ UNCHANGED <<paused, reachable, srcFenced, dstFenced, srcData, dstData,
                   acked, nextWrite, crashed, crashes>>)

\* Publish the new map and reload every reachable instance.
PublishAndReload == Saga(
    /\ phase = "CutOver"
    /\ routesTo' = [i \in Instances |-> IF reachable[i] THEN "Dst" ELSE routesTo[i]]
    /\ phase' = "Published"
    /\ UNCHANGED <<paused, reachable, srcFenced, dstFenced, srcData, dstData,
                   acked, nextWrite, crashed, crashes>>)

ResumeAll == Saga(
    /\ phase = "Published"
    /\ paused' = [i \in Instances |-> IF reachable[i] THEN FALSE ELSE paused[i]]
    /\ phase' = "Resumed"
    /\ UNCHANGED <<reachable, routesTo, srcFenced, dstFenced, srcData, dstData,
                   acked, nextWrite, crashed, crashes>>)

\* Drop the reverse stream and the source, once every instance routes to
\* the destination.
Finalize == Saga(
    /\ phase = "Resumed"
    /\ \A i \in Instances : routesTo[i] = "Dst" /\ ~paused[i]
    /\ phase' = "Finalized"
    /\ UNCHANGED <<paused, reachable, routesTo, srcFenced, dstFenced, srcData,
                   dstData, acked, nextWrite, crashed, crashes>>)

\* Start the reverse path: pause what can be reached and fence the
\* destination.
RollBack == Saga(
    /\ phase \in AfterCutOver
    /\ paused' = [i \in Instances |-> IF reachable[i] THEN TRUE ELSE paused[i]]
    /\ dstFenced' = FenceEnabled
    /\ phase' = "RollingBack"
    /\ UNCHANGED <<reachable, routesTo, srcFenced, srcData, dstData, acked,
                   nextWrite, crashed, crashes>>)

\* Once the reverse stream has drained, route reachable instances back to
\* the source, unfence it and resume.
RolledBack == Saga(
    /\ phase = "RollingBack"
    /\ dstData \subseteq srcData
    /\ routesTo' = [i \in Instances |-> IF reachable[i] THEN "Src" ELSE routesTo[i]]
    /\ paused' = [i \in Instances |-> IF reachable[i] THEN FALSE ELSE paused[i]]
    /\ srcFenced' = FALSE
    /\ phase' = "RolledBack"
    /\ UNCHANGED <<reachable, dstFenced, srcData, dstData, acked, nextWrite,
                   crashed, crashes>>)

\* The forward stream (PgDog's logical replication) moves one write from
\* the source to the destination, from the copy until finalize or rollback.
StreamForward ==
    /\ phase \in {"CatchUp", "Paused", "Fenced"} \cup AfterCutOver
    /\ \E w \in srcData \ dstData : dstData' = dstData \cup {w}
    /\ UNCHANGED <<phase, paused, reachable, routesTo, srcFenced, dstFenced,
                   srcData, acked, nextWrite, crashed, crashes>>

\* The reverse stream moves a write accepted on the destination back to the
\* source, from the cutover until finalize.
StreamReverse ==
    /\ phase \in AfterCutOver \cup {"RollingBack"}
    /\ \E w \in dstData \ srcData : srcData' = srcData \cup {w}
    /\ UNCHANGED <<phase, paused, reachable, routesTo, srcFenced, dstFenced,
                   dstData, acked, nextWrite, crashed, crashes>>

Accepting(store) == IF store = "Src" THEN ~srcFenced ELSE ~dstFenced

\* A client write through instance i. A paused instance queues it (the
\* action is not enabled); a fenced store refuses it.
ClientWrite(i, k) ==
    LET w == [id |-> nextWrite, key |-> k] IN
    /\ nextWrite <= MaxWrites
    /\ ~paused[i]
    /\ nextWrite' = nextWrite + 1
    /\ IF Accepting(routesTo[i])
         THEN /\ acked' = acked \cup {w}
              /\ IF routesTo[i] = "Src"
                   THEN srcData' = srcData \cup {w} /\ UNCHANGED dstData
                   ELSE dstData' = dstData \cup {w} /\ UNCHANGED srcData
         ELSE UNCHANGED <<acked, srcData, dstData>>
    /\ UNCHANGED <<phase, paused, reachable, routesTo, srcFenced, dstFenced,
                   crashed, crashes>>

Partition(i) ==
    /\ reachable[i]
    /\ reachable' = [reachable EXCEPT ![i] = FALSE]
    /\ UNCHANGED <<phase, paused, routesTo, srcFenced, dstFenced, srcData,
                   dstData, acked, nextWrite, crashed, crashes>>

\* A healed instance catches up with the saga's current intent.
Heal(i) ==
    /\ ~reachable[i]
    /\ reachable' = [reachable EXCEPT ![i] = TRUE]
    /\ paused' = [paused EXCEPT ![i] =
                    phase \in {"Paused", "Fenced", "CutOver", "RollingBack"}]
    /\ routesTo' = [routesTo EXCEPT ![i] =
                      IF phase \in {"Published", "Resumed", "Finalized"} THEN "Dst"
                      ELSE IF phase = "RolledBack" THEN "Src"
                      ELSE routesTo[i]]
    /\ UNCHANGED <<phase, srcFenced, dstFenced, srcData, dstData, acked,
                   nextWrite, crashed, crashes>>

SagaCrash ==
    /\ ~crashed
    /\ crashes < SagaCrashes
    /\ phase \notin {"Finalized", "RolledBack"}
    /\ crashed' = TRUE
    /\ crashes' = crashes + 1
    /\ UNCHANGED <<phase, paused, reachable, routesTo, srcFenced, dstFenced,
                   srcData, dstData, acked, nextWrite>>

\* The saga restarts from its durable step.
SagaRestart ==
    /\ crashed
    /\ crashed' = FALSE
    /\ UNCHANGED <<phase, paused, reachable, routesTo, srcFenced, dstFenced,
                   srcData, dstData, acked, nextWrite, crashes>>

SagaStep ==
    \/ StartCopy \/ PauseAll \/ FenceSource \/ CutOverDesignated
    \/ PublishAndReload \/ ResumeAll \/ Finalize \/ RollBack \/ RolledBack

Next ==
    \/ SagaStep
    \/ StreamForward \/ StreamReverse
    \/ \E i \in Instances, k \in Keys : ClientWrite(i, k)
    \/ \E i \in Instances : Partition(i) \/ Heal(i)
    \/ SagaCrash \/ SagaRestart

\* Strong fairness on the saga: a flapping partition of the designated
\* instance enables CutOverDesignated only intermittently, and the saga must
\* still get through (the network is eventually stable enough).
Fairness ==
    /\ SF_vars(SagaStep)
    /\ WF_vars(SagaRestart)
    /\ WF_vars(StreamForward)
    /\ WF_vars(StreamReverse)
    /\ \A i \in Instances : WF_vars(Heal(i))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety *)

\* A store can take client writes when it is unfenced and some running
\* instance routes to it.
CanWrite(store) ==
    /\ Accepting(store)
    /\ \E i \in Instances : routesTo[i] = store /\ ~paused[i]

\* Never both clusters writable for the range at once.
SingleWriterRange == ~(CanWrite("Src") /\ CanWrite("Dst"))

\* After finalize, the destination holds every acknowledged write.
NoLostWrite == phase = "Finalized" => acked \subseteq dstData

\* Each write id occurs at most once per store (redelivery is a union).
NoDuplicateEffect ==
    \A store \in {srcData, dstData} :
        \A w1, w2 \in store : w1.id = w2.id => w1 = w2

\* After a rollback, the source holds every acknowledged write, including
\* those acknowledged on the destination after the cutover.
ReverseSafe == phase = "RolledBack" => acked \subseteq srcData

(* Liveness *)

Terminates == <>(phase \in {"Finalized", "RolledBack"})

=============================================================================
