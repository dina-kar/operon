------------------------------ MODULE Selftest ------------------------------
(* A counter that must stay under 3. MCSelftest_Violation.cfg lets it run  *)
(* to 3, so TLC has to report a violation of Small: the `tla` job uses it  *)
(* to prove that it notices failures.                                      *)
EXTENDS Naturals
CONSTANT Bound
VARIABLE n
Init == n = 0
Next == n < Bound /\ n' = n + 1
Small == n < 3
=============================================================================
