/-
Key ranges of a sharded keyspace (design §31 §6.1 and §12, D312; RT0 Task 8).

A keyspace is split into ranges `[lo, hi)` of keyspace ids, the last one open
(`hi = none`), as Vitess names shards (`-80`, `80-`). `validate` is the
reference algorithm that `loams_sqlrouter::ranges::validate_partition`
implements in Rust, with the same error vocabulary; the theorems here prove
that `validate` accepts exactly the partitions of `[0, ∞)`, that a partition
maps every id to exactly one range, and that `split` and `merge` keep a
partition a partition. The Rust tests compare both implementations on random
inputs through the `loams-router-oracle` executable.
-/
namespace LoamsRouter

structure KeyRange where
  lo : Nat
  hi : Option Nat
  deriving Repr, DecidableEq, Inhabited

/-- `id` lies in `r = [lo, hi)`. -/
def inRange (r : KeyRange) (id : Nat) : Bool :=
  decide (r.lo ≤ id) && match r.hi with
    | none => true
    | some h => decide (id < h)

/-- `rs` covers `[s, ∞)` with adjacent, non-empty ranges in order. -/
inductive Chain : Nat → List KeyRange → Prop
  | last (s : Nat) : Chain s [⟨s, none⟩]
  | cons {s h : Nat} {rest : List KeyRange} : s < h → Chain h rest → Chain s (⟨s, some h⟩ :: rest)

/-- A partition of the whole id space `[0, ∞)`. -/
def IsPartition (rs : List KeyRange) : Prop := Chain 0 rs

/-- Why a list of ranges is not a partition. `at` is the first uncovered id
(`gap`) or the start of the first range that overlaps its predecessor. -/
inductive PartitionError
  | gap (at_ : Nat)
  | overlap (at_ : Nat)
  | notSorted
  | empty
  deriving Repr, DecidableEq

/-- Check that `rs` covers `[s, ∞)` exactly, scanning left to right. -/
def validateFrom (s : Nat) : List KeyRange → Except PartitionError Unit
  | [] => .error .empty
  | [r] =>
    if s < r.lo then .error (.gap s)
    else if r.lo < s then .error (.overlap r.lo)
    else match r.hi with
      | none => .ok ()
      | some h => if h ≤ r.lo then .error .notSorted else .error (.gap h)
  | r :: r2 :: rest =>
    if s < r.lo then .error (.gap s)
    else if r.lo < s then .error (.overlap r.lo)
    else match r.hi with
      | none => .error (.overlap r2.lo)
      | some h => if h ≤ r.lo then .error .notSorted else validateFrom h (r2 :: rest)

def validate (rs : List KeyRange) : Except PartitionError Unit := validateFrom 0 rs

/-! ### `validate` accepts exactly the partitions -/

theorem validateFrom_sound : ∀ (s : Nat) (rs : List KeyRange),
    validateFrom s rs = .ok () → Chain s rs
  | s, [], h => by simp [validateFrom] at h
  | s, [⟨lo, hi⟩], h => by
    unfold validateFrom at h
    by_cases h1 : s < lo
    · simp [h1] at h
    · by_cases h2 : lo < s
      · simp [h1, h2] at h
      · have : lo = s := by omega
        subst this
        cases hi with
        | none => exact Chain.last lo
        | some x =>
          simp at h
          by_cases h3 : x ≤ lo <;> simp [h3] at h
  | s, ⟨lo, hi⟩ :: r2 :: rest, h => by
    unfold validateFrom at h
    by_cases h1 : s < lo
    · simp [h1] at h
    · by_cases h2 : lo < s
      · simp [h1, h2] at h
      · have : lo = s := by omega
        subst this
        cases hi with
        | none => simp at h
        | some x =>
          simp at h
          by_cases h3 : x ≤ lo
          · simp [h3] at h
          · simp [h3] at h
            exact Chain.cons (by omega) (validateFrom_sound x (r2 :: rest) h)

theorem validateFrom_complete : ∀ {s : Nat} {rs : List KeyRange},
    Chain s rs → validateFrom s rs = .ok ()
  | _, _, Chain.last s => by simp [validateFrom]
  | _, _, @Chain.cons s h rest hlt hrest => by
    cases rest with
    | nil => cases hrest
    | cons r2 rest2 =>
      have ih := validateFrom_complete hrest
      simp only [validateFrom]
      have : ¬ h ≤ s := by omega
      simp [this, ih]

theorem validate_iff (rs : List KeyRange) : validate rs = .ok () ↔ IsPartition rs :=
  ⟨validateFrom_sound 0 rs, validateFrom_complete⟩

/-! ### Every id lies in exactly one range -/

theorem chain_lo_ge : ∀ {s : Nat} {rs : List KeyRange}, Chain s rs → ∀ r ∈ rs, s ≤ r.lo
  | _, _, Chain.last s => by intro r hr; simp at hr; subst hr; simp
  | _, _, @Chain.cons s h rest hlt hrest => by
    intro r hr
    simp at hr
    rcases hr with rfl | hr
    · simp
    · have := chain_lo_ge hrest r hr; omega

theorem not_inRange_of_lt {r : KeyRange} {id : Nat} (h : id < r.lo) : inRange r id = false := by
  simp [inRange]; intro h'; omega

/-- In a chain from `s`, every id `≥ s` lies in exactly one range. -/
theorem lookup_total_unique : ∀ {s : Nat} {rs : List KeyRange}, Chain s rs →
    ∀ id, s ≤ id → (rs.filter (fun r => inRange r id)).length = 1
  | _, _, Chain.last s => by intro id hid; simp [inRange, hid]
  | _, _, @Chain.cons s h rest hlt hrest => by
    intro id hid
    by_cases hlo : id < h
    · have hnone : rest.filter (fun r => inRange r id) = [] := by
        rw [List.filter_eq_nil_iff]
        intro r hr
        have := chain_lo_ge hrest r hr
        simp [not_inRange_of_lt (show id < r.lo by omega)]
      have hhead : inRange ⟨s, some h⟩ id = true := by simp [inRange, hid, hlo]
      simp only [List.filter_cons, hhead, hnone, if_true, List.length_singleton]
    · have ih := lookup_total_unique hrest id (by omega)
      have : inRange ⟨s, some h⟩ id = false := by simp [inRange]; omega
      simp only [List.filter_cons, this]
      simpa using ih

theorem partition_total_unique {rs : List KeyRange} (h : IsPartition rs) (id : Nat) :
    (rs.filter (fun r => inRange r id)).length = 1 :=
  lookup_total_unique h id (Nat.zero_le _)

/-- The index of the first range containing `id`. -/
def lookup : List KeyRange → Nat → Option Nat
  | [], _ => none
  | r :: rest, id => if inRange r id then some 0 else (lookup rest id).map (· + 1)

theorem lookup_spec : ∀ (rs : List KeyRange) (id i : Nat),
    lookup rs id = some i → ∃ r, rs[i]? = some r ∧ inRange r id = true
  | [], _, _, h => by simp [lookup] at h
  | r :: rest, id, i, h => by
    simp only [lookup] at h
    by_cases hr : inRange r id
    · simp [hr] at h; subst h; exact ⟨r, by simp, hr⟩
    · simp [hr] at h
      obtain ⟨j, hj, rfl⟩ := h
      obtain ⟨r', h1, h2⟩ := lookup_spec rest id j hj
      exact ⟨r', by simpa using h1, h2⟩

theorem lookup_isSome : ∀ {s : Nat} {rs : List KeyRange}, Chain s rs →
    ∀ id, s ≤ id → (lookup rs id).isSome
  | _, _, Chain.last s => by intro id hid; simp [lookup, inRange, hid]
  | _, _, @Chain.cons s h rest hlt hrest => by
    intro id hid
    simp only [lookup]
    by_cases hr : inRange ⟨s, some h⟩ id
    · simp [hr]
    · have : h ≤ id := by simp [inRange] at hr; omega
      simp [hr, lookup_isSome hrest id this]

/-! ### Split and merge keep a partition -/

/-- `at_` lies strictly inside `r`, so splitting there leaves two non-empty ranges. -/
def inside (r : KeyRange) (at_ : Nat) : Bool :=
  decide (r.lo < at_) && match r.hi with
    | none => true
    | some h => decide (at_ < h)

/-- Split range `i` at `at_`, which must lie strictly inside it. -/
def split : List KeyRange → Nat → Nat → Option (List KeyRange)
  | [], _, _ => none
  | r :: rest, 0, at_ =>
    if inside r at_ then
      some (⟨r.lo, some at_⟩ :: ⟨at_, r.hi⟩ :: rest)
    else none
  | r :: rest, i + 1, at_ => (split rest i at_).map (r :: ·)

/-- Merge range `i` with range `i + 1`, which must be adjacent. -/
def merge : List KeyRange → Nat → Option (List KeyRange)
  | r1 :: r2 :: rest, 0 => if r1.hi = some r2.lo then some (⟨r1.lo, r2.hi⟩ :: rest) else none
  | r :: rest, i + 1 => (merge rest i).map (r :: ·)
  | _, _ => none

theorem split_preserves : ∀ {s : Nat} {rs : List KeyRange}, Chain s rs →
    ∀ i at_ rs', split rs i at_ = some rs' → Chain s rs'
  | _, _, Chain.last s => by
    intro i at_ rs' h
    cases i with
    | zero =>
      simp [split, inside] at h
      obtain ⟨hlt, rfl⟩ := h
      exact Chain.cons hlt (Chain.last at_)
    | succ k => simp [split] at h
  | _, _, @Chain.cons s h rest hlt hrest => by
    intro i at_ rs' hs
    cases i with
    | zero =>
      simp [split, inside] at hs
      obtain ⟨⟨h1, h2⟩, rfl⟩ := hs
      exact Chain.cons h1 (Chain.cons h2 hrest)
    | succ k =>
      simp [split] at hs
      obtain ⟨rest', hr, rfl⟩ := hs
      exact Chain.cons hlt (split_preserves hrest k at_ rest' hr)

theorem merge_preserves : ∀ {s : Nat} {rs : List KeyRange}, Chain s rs →
    ∀ i rs', merge rs i = some rs' → Chain s rs'
  | _, _, Chain.last s => by
    intro i rs' h
    cases i <;> simp [merge] at h
  | _, _, @Chain.cons s h rest hlt hrest => by
    intro i rs' hm
    cases i with
    | zero =>
      cases hrest with
      | last _ =>
        simp [merge] at hm; subst hm; exact Chain.last s
      | @cons _ h2 rest2 hlt2 hrest2 =>
        simp [merge] at hm; subst hm; exact Chain.cons (by omega) hrest2
    | succ k =>
      cases rest with
      | nil => cases hrest
      | cons r2 rest2 =>
        simp [merge] at hm
        obtain ⟨rest', hr, rfl⟩ := hm
        exact Chain.cons hlt (merge_preserves hrest k rest' hr)

theorem split_partition {rs rs' : List KeyRange} {i at_ : Nat}
    (h : IsPartition rs) (hs : split rs i at_ = some rs') : IsPartition rs' :=
  split_preserves h i at_ rs' hs

theorem merge_partition {rs rs' : List KeyRange} {i : Nat}
    (h : IsPartition rs) (hm : merge rs i = some rs') : IsPartition rs' :=
  merge_preserves h i rs' hm

end LoamsRouter
