import Lake
open Lake DSL

package LoamsRouter where
  leanOptions := #[⟨`autoImplicit, false⟩]

/-- The proofs: building the library checks every theorem, including those
the oracle does not import (ShardFn). -/
@[default_target]
lean_lib LoamsRouter

/-- The differential oracle the Rust tests call (JSON lines on stdin and stdout). -/
@[default_target]
lean_exe «loams-router-oracle» where
  root := `Main

/-- Randomised properties of split and merge, run by `lake test`. -/
@[test_driver]
lean_exe «loams-router-proptest» where
  root := `PropTest
