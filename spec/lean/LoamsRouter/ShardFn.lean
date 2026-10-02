/-
How a key's hash picks a shard (design §31 §6.1, §12; RT0 Task 8): by modulus
(Postgres hash partitioning, as PgDog routes) or by key range (Vitess keyspace
ids). Both are total functions, so every key has exactly one shard.
-/
import LoamsRouter.KeyRange

namespace LoamsRouter

/-- The shard of hash `h` among `n` shards by modulus (Postgres `PARTITION BY HASH`). -/
def shardOfModulo (n : Nat) (hn : 0 < n) (h : Nat) : Fin n := ⟨h % n, Nat.mod_lt _ hn⟩

/-- The modulus scheme partitions the hashes: each lands in exactly one shard. -/
theorem modulo_partition (n : Nat) (hn : 0 < n) (h : Nat) :
    (∃ i : Fin n, i.val = h % n) ∧ ∀ i j : Fin n, i.val = h % n → j.val = h % n → i = j :=
  ⟨⟨shardOfModulo n hn h, rfl⟩, fun _ _ hi hj => Fin.ext (hi.trans hj.symm)⟩

/-- The shard of keyspace id `id` in a range partition: the range containing it. -/
def shardOfRange (rs : List KeyRange) (id : Nat) : Option Nat := lookup rs id

/-- In a partition, range lookup always finds a shard, and that shard's range contains the id. -/
theorem shardOfRange_total {rs : List KeyRange} (h : IsPartition rs) (id : Nat) :
    ∃ i r, shardOfRange rs id = some i ∧ rs[i]? = some r ∧ inRange r id = true := by
  have hs := lookup_isSome h id (Nat.zero_le _)
  match hl : lookup rs id, hs with
  | some i, _ =>
    obtain ⟨r, h1, h2⟩ := lookup_spec rs id i hl
    exact ⟨i, r, by simp [shardOfRange, hl], h1, h2⟩

/-- Same key, same partition, same shard: the router's choice depends on nothing else. -/
theorem shardOf_deterministic (rs : List KeyRange) (id : Nat) :
    shardOfRange rs id = shardOfRange rs id := rfl

end LoamsRouter
