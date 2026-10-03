--------------------------- MODULE PrimaryFailover ---------------------------
(***************************************************************************)
(* SKELETON (RT0 Task 4). Model-checked from RT4. Design §31 §11.2.         *)
(*                                                                         *)
(* Per shard: proposer terms over acceptors (the Loams WAL's Arm A, D264,  *)
(* and WeSQL's W-phases, D276–D278), a lease of length T with a clock     *)
(* drift bound Eps, the primary record's CAS in the metastore, and the    *)
(* routers' repoint (§6.3, §9.1). The acceptor half follows walproposer's *)
(* Paxos as modelled in neon/safekeeper/spec/ProposerAcceptorStatic.tla   *)
(* (Apache-2.0), cited and not copied.                                     *)
(*                                                                         *)
(* Action-to-code (filled in by RT4): Elect(p), Append(p), Ack, LeaseTick, *)
(* RecordCas(p), Repoint(i), Crash(p) -> code = "RT4".                     *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Proposers, Acceptors, Instances, MaxTerm, T, Eps

VARIABLES
    term,        \* [Proposers -> 0..MaxTerm]
    primary,     \* the proposer the metastore record names, or "none"
    leaseUntil,  \* [Proposers -> Nat], in a shared logical clock
    clock,       \* the logical clock
    logs,        \* [Acceptors -> Seq([term: Nat, val: Nat])]
    acked,       \* acknowledged commits
    routedTo     \* [Instances -> Proposers \cup {"none"}]

vars == <<term, primary, leaseUntil, clock, logs, acked, routedTo>>

Init ==
    /\ term = [p \in Proposers |-> 0]
    /\ primary = "none"
    /\ leaseUntil = [p \in Proposers |-> 0]
    /\ clock = 0
    /\ logs = [a \in Acceptors |-> <<>>]
    /\ acked = {}
    /\ routedTo = [i \in Instances |-> "none"]

\* RT4: a proposer wins a majority vote for a higher term and syncs the log.
Elect(p) == UNCHANGED vars
\* RT4: the primary appends to a majority and acknowledges.
AppendEntry(p) == UNCHANGED vars
\* RT4: the record CAS names the new primary, after the old lease plus Eps has expired.
RecordCas(p) == UNCHANGED vars
\* RT4: a router instance repoints to the record's primary.
Repoint(i) == UNCHANGED vars
Tick == clock' = clock + 1 /\ UNCHANGED <<term, primary, leaseUntil, logs, acked, routedTo>>

Next == Tick \/ \E p \in Proposers : Elect(p) \/ AppendEntry(p) \/ RecordCas(p)
             \/ \E i \in Instances : Repoint(i)
Spec == Init /\ [][Next]_vars

\* At most one proposer acknowledges writes in any term.
OneWriterPerTerm == TRUE
\* An acknowledged commit is in every later primary's log.
AckedSurvives == TRUE
\* A deposed primary serves no read after its lease plus Eps.
NoStaleServe == TRUE
\* Liveness (RT4): after the old primary stops, a new primary becomes writable.
EventuallyWritable == TRUE
=============================================================================
