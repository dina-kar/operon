---------------------------- MODULE RouterSession ----------------------------
(***************************************************************************)
(* CONTRACT ONLY (RT0 Task 4; Q312). Model-checked from RT4. §31 §11.2.     *)
(*                                                                         *)
(* The routers' observable session contract, which Loams relies on and    *)
(* tests from outside (PgDog and vtgate run unmodified):                  *)
(*   PinnedTxn:      a transaction's statements all go to the shard set   *)
(*                   chosen by its first statement, even if the map's     *)
(*                   generation changes mid-transaction.                  *)
(*   NoSilentReplan: after a generation change, a prepared statement is   *)
(*                   re-prepared against the new map or fails; it never   *)
(*                   runs a plan built for the old map.                   *)
(* Action-to-code (RT4): Begin, Statement, Commit, GenerationChange,       *)
(* Execute(stmt) -> code = "RT4".                                          *)
(***************************************************************************)
EXTENDS Naturals

VARIABLES gen, txnShards, stmtGen
vars == <<gen, txnShards, stmtGen>>

Init == gen = 0 /\ txnShards = {} /\ stmtGen = 0
GenerationChange == gen' = gen + 1 /\ UNCHANGED <<txnShards, stmtGen>>
Next == GenerationChange
Spec == Init /\ [][Next]_vars

PinnedTxn == TRUE
NoSilentReplan == TRUE
=============================================================================
