--------------------------- MODULE CrossShardCommit ---------------------------
(***************************************************************************)
(* SKELETON (RT0 Task 4). Model-checked from RT2. Design §31 §11.2.         *)
(*                                                                         *)
(* PgDog's two-phase commit as Loams runs it: the coordinator (a PgDog     *)
(* instance) logs Phase 1, sends PREPARE TRANSACTION to each participant,  *)
(* logs Phase 2, sends COMMIT PREPARED, then forgets the transaction.      *)
(* Recovery rolls back a transaction found in Phase 1 and commits one in   *)
(* Phase 2. Loams adds an in-doubt monitor that alerts and never decides   *)
(* (D307). Written from PgDog's documentation and observed behaviour only; *)
(* no PgDog code is used (D318).                                           *)
(*                                                                         *)
(* Action-to-code (filled in by RT2):                                      *)
(*   LogPhase1, Prepare(p), LogPhase2, CommitPrepared(p), Done -> PgDog    *)
(*     (observed by the simulator's PgDog model)              code = "RT2" *)
(*   CoordinatorCrash(keepLog), Recover   -> nemesis, PgDog restart  "RT2" *)
(*   ParticipantCrash(p)                  -> nemesis                 "RT2" *)
(*   MonitorScan                          -> loams_sqlrouter::monitor "RT2"*)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Participants, KeepLog

VARIABLES
    coordLog,    \* "none" | "phase1" | "phase2" | "done", lost on a crash unless KeepLog
    coordUp,     \* whether the coordinator runs
    partState,   \* [Participants -> {"working", "prepared", "committed", "aborted"}]
    alerts       \* in-doubt transactions the monitor has reported

vars == <<coordLog, coordUp, partState, alerts>>

TypeOK ==
    /\ coordLog \in {"none", "phase1", "phase2", "done"}
    /\ coordUp \in BOOLEAN
    /\ partState \in [Participants -> {"working", "prepared", "committed", "aborted"}]
    /\ alerts \in BOOLEAN

Init ==
    /\ coordLog = "none"
    /\ coordUp = TRUE
    /\ partState = [p \in Participants |-> "working"]
    /\ alerts = FALSE

\* Enabling conditions are final; effects on the log follow PgDog's documented phases.
LogPhase1 == coordUp /\ coordLog = "none" /\ coordLog' = "phase1" /\ UNCHANGED <<coordUp, partState, alerts>>
Prepare(p) == coordUp /\ coordLog = "phase1" /\ partState[p] = "working"
              /\ partState' = [partState EXCEPT ![p] = "prepared"] /\ UNCHANGED <<coordLog, coordUp, alerts>>
LogPhase2 == coordUp /\ coordLog = "phase1" /\ \A p \in Participants : partState[p] = "prepared"
             /\ coordLog' = "phase2" /\ UNCHANGED <<coordUp, partState, alerts>>
CommitPrepared(p) == coordUp /\ coordLog = "phase2" /\ partState[p] = "prepared"
                     /\ partState' = [partState EXCEPT ![p] = "committed"] /\ UNCHANGED <<coordLog, coordUp, alerts>>
Done == coordUp /\ coordLog = "phase2" /\ \A p \in Participants : partState[p] = "committed"
        /\ coordLog' = "done" /\ UNCHANGED <<coordUp, partState, alerts>>
\* RT2: a crash keeps or loses the log; the participant states survive (prepared transactions are durable).
CoordinatorCrash(keep) == coordUp /\ coordUp' = FALSE
        /\ coordLog' = (IF keep THEN coordLog ELSE "none")
        /\ UNCHANGED <<partState, alerts>>
\* RT2: recovery rolls back Phase 1 and commits Phase 2 (one participant per step, modelled in RT2).
Recover == ~coordUp /\ coordUp' = TRUE /\ UNCHANGED <<coordLog, partState, alerts>>
\* RT2: a participant restart keeps prepared transactions and aborts working ones.
ParticipantCrash(p) == partState[p] = "working" /\ partState' = [partState EXCEPT ![p] = "aborted"]
        /\ UNCHANGED <<coordLog, coordUp, alerts>>
\* The monitor reports prepared transactions older than a bound and never resolves them (D307).
MonitorScan == \E p \in Participants : partState[p] = "prepared"
        /\ alerts' = TRUE /\ UNCHANGED <<coordLog, coordUp, partState>>

Next ==
    \/ LogPhase1 \/ LogPhase2 \/ Done \/ Recover \/ MonitorScan
    \/ \E p \in Participants : Prepare(p) \/ CommitPrepared(p) \/ ParticipantCrash(p)
    \/ \E keep \in {KeepLog} : CoordinatorCrash(keep)

Spec == Init /\ [][Next]_vars

\* No participant commits while another rolls back.
Atomicity == ~(\E p, q \in Participants : partState[p] = "committed" /\ partState[q] = "aborted")
\* With a kept log, a logged Phase 2 decision is never forgotten (RT2 states this over histories).
DecisionDurable == KeepLog => TRUE
\* The monitor never changes a participant's state (structural: MonitorScan leaves partState unchanged).
MonitorNeverDecides == TRUE
\* Liveness (RT2): with a kept log and fair recovery, no participant stays prepared.
NoStuckPrepared == <>[](\A p \in Participants : partState[p] # "prepared")
=============================================================================
