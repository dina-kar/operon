import LoamsRouter.KeyRange

/-!
`lake test`: randomised checks of the executable definitions, beside the proofs.
For random partitions: `validate` accepts them; splitting inside a range then
merging the two halves gives back the original; every split keeps `validate`
happy; lookup finds a range containing the id. A fixed seed keeps it
reproducible; set `LOAMS_PROPTEST_CASES` for more cases.
-/
open LoamsRouter

def isOk : Except PartitionError Unit → Bool
  | .ok () => true
  | .error _ => false

/-- A random partition of `[0, ∞)` with up to `n + 1` ranges, from cut points below 2^64. -/
def randomPartition (g : StdGen) (n : Nat) : List KeyRange × StdGen := Id.run do
  let mut g := g
  let mut cuts : List Nat := []
  let (k, g1) := randNat g 0 n
  g := g1
  for _ in [0:k] do
    let (c, g2) := randNat g 1 (2 ^ 64 - 1)
    g := g2
    cuts := c :: cuts
  let sorted := (cuts.mergeSort (· ≤ ·)).eraseDups
  let rec build (lo : Nat) : List Nat → List KeyRange
    | [] => [⟨lo, none⟩]
    | c :: cs => ⟨lo, some c⟩ :: build c cs
  return (build 0 sorted, g)

def main : IO UInt32 := do
  let cases := ((← IO.getEnv "LOAMS_PROPTEST_CASES").bind String.toNat?).getD 2000
  let mut g := mkStdGen 20261002
  let mut failures := 0
  for _ in [0:cases] do
    let (rs, g1) := randomPartition g 8
    g := g1
    if !isOk (validate rs) then
      failures := failures + 1
      IO.eprintln s!"validate rejected a partition: {repr rs}"
    let (i, g2) := randNat g 0 (rs.length - 1)
    g := g2
    let r := rs[i]!
    let hi := r.hi.getD (2 ^ 64)
    if r.lo + 1 < hi then
      let (at_, g3) := randNat g (r.lo + 1) (hi - 1)
      g := g3
      match split rs i at_ with
      | none =>
        failures := failures + 1
        IO.eprintln s!"split refused an inside point {at_} of range {i}"
      | some rs' =>
        if !isOk (validate rs') then
          failures := failures + 1
          IO.eprintln "split broke the partition"
        if merge rs' i != some rs then
          failures := failures + 1
          IO.eprintln "merge did not undo split"
    let (id, g4) := randNat g 0 (2 ^ 64 - 1)
    g := g4
    match lookup rs id with
    | none =>
      failures := failures + 1
      IO.eprintln s!"lookup found no range for {id}"
    | some j =>
      if !(inRange rs[j]! id) then
        failures := failures + 1
        IO.eprintln s!"lookup returned a range not containing {id}"
  IO.println s!"loams-router-proptest: {cases} cases, {failures} failures"
  return if failures == 0 then 0 else 1
