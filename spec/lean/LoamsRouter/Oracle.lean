/-
The differential oracle (RT0 Task 8, D312). One JSON request per line on stdin,
one response per line on stdout; `loams_sqlrouter`'s `lean_oracle` test sends
random cases and compares the answers with the Rust implementation.

Requests (ranges are `[lo, hi]` pairs, `hi` may be `null`):
  {"op":"partition_check","ranges":[[0,128],[128,null]]}
  {"op":"lookup","ranges":…,"id":200}
  {"op":"split","ranges":…,"index":0,"at":64}
  {"op":"merge","ranges":…,"index":0}
Responses:
  {"ok":true}  {"ok":true,"result":1}  {"ok":true,"result":[[0,64],[64,128],[128,null]]}
  {"ok":false,"error":"Gap"|"Overlap"|"NotSorted"|"Empty"|"Invalid","at":n}
-/
import Lean.Data.Json
import LoamsRouter.KeyRange

namespace LoamsRouter.Oracle
open Lean

def rangeOfJson (j : Json) : Except String KeyRange := do
  let arr ← j.getArr?
  if h : arr.size = 2 then
    let lo ← arr[0].getNat?
    let hi ← match arr[1] with
      | Json.null => pure none
      | v => some <$> v.getNat?
    pure ⟨lo, hi⟩
  else throw "a range is [lo, hi]"

def rangesOfJson (j : Json) : Except String (List KeyRange) := do
  let arr ← (← j.getObjVal? "ranges").getArr?
  arr.toList.mapM rangeOfJson

def rangeToJson (r : KeyRange) : Json :=
  Json.arr #[Json.num r.lo, match r.hi with | none => Json.null | some h => Json.num h]

def errorJson : PartitionError → Json
  | .gap a => Json.mkObj [("ok", false), ("error", "Gap"), ("at", Json.num a)]
  | .overlap a => Json.mkObj [("ok", false), ("error", "Overlap"), ("at", Json.num a)]
  | .notSorted => Json.mkObj [("ok", false), ("error", "NotSorted")]
  | .empty => Json.mkObj [("ok", false), ("error", "Empty")]

def okJson (result : Option Json := none) : Json :=
  match result with
  | none => Json.mkObj [("ok", true)]
  | some r => Json.mkObj [("ok", true), ("result", r)]

def invalid : Json := Json.mkObj [("ok", false), ("error", "Invalid")]

def answer (line : String) : Except String Json := do
  let j ← Json.parse line
  let op ← (← j.getObjVal? "op").getStr?
  let rs ← rangesOfJson j
  match op with
  | "partition_check" =>
    pure <| match validate rs with
      | .ok () => okJson
      | .error e => errorJson e
  | "lookup" =>
    let id ← (← j.getObjVal? "id").getNat?
    pure <| okJson (some (match lookup rs id with | none => Json.null | some i => Json.num i))
  | "split" =>
    let i ← (← j.getObjVal? "index").getNat?
    let at_ ← (← j.getObjVal? "at").getNat?
    pure <| match split rs i at_ with
      | none => invalid
      | some rs' => okJson (some (Json.arr (rs'.map rangeToJson).toArray))
  | "merge" =>
    let i ← (← j.getObjVal? "index").getNat?
    pure <| match merge rs i with
      | none => invalid
      | some rs' => okJson (some (Json.arr (rs'.map rangeToJson).toArray))
  | other => throw s!"unknown op {other}"

end LoamsRouter.Oracle
