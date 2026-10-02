------------------------------ MODULE ShardMap ------------------------------
(***************************************************************************)
(* The shard map of the Loams router (design §31 §6.1–§6.3, D303–D306).    *)
(*                                                                         *)
(* Loams keeps one versioned shard-map record per sharded database in the  *)
(* metastore. Each generation maps every key to exactly one shard. Router  *)
(* instances (PgDog, or vtgate for MySQL) never read the record: they load *)
(* a rendered ConfigMap, and each instance reloads on its own schedule. So *)
(* at any moment several generations can be live across instances.        *)
(*                                                                         *)
(* A shard (a real Postgres or WeSQL server) knows nothing about key       *)
(* ownership: it accepts every write that reaches it unless it is fenced   *)
(* (Loams fences a shard with ALTER ROLE ... NOLOGIN, §31 §6.4). The fence *)
(* is the only guard, so the protocol's ordering is what keeps the map     *)
(* safe:                                                                   *)
(*   1. Publish generation g (CAS on the record, from g - 1 only).         *)
(*   2. Fence every shard that loses keys in g.                            *)
(*   3. Copy each moving key's writes to its new owner (the final          *)
(*      catch-up; the bulk copy and stream before it do not matter for     *)
(*      safety, so they are not modelled).                                 *)
(*   4. Write the ConfigMap for g.                                         *)
(*   5. Instances reload (or restart) onto g.                              *)
(*   6. Once every instance runs g, unfence.                               *)
(* With UnsafeConfigMapFirst the ConfigMap may be written before 2 and 3;  *)
(* the variant MCShardMap_UnsafeConfigMap shows why that is wrong.         *)
(*                                                                         *)
(* Action-to-code mapping: spec/tla/router/README.md.                      *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS
    \* @type: Set(Str);
    Keys,
    \* @type: Set(Str);
    Shards,
    \* @type: Set(Str);
    Instances,
    \* @type: Int;
    MaxGen,
    \* @type: Int;
    MaxWrites,
    \* @type: Bool;
    UnsafeConfigMapFirst

VARIABLES
    \* The metastore record: its generation and that generation's map.
    \* @type: { gen: Int, owner: Str -> Str };
    record,
    \* Every published generation's map; history[g + 1] is generation g.
    \* @type: Seq(Str -> Str);
    history,
    \* The generation rendered into the routers' ConfigMap.
    \* @type: Int;
    configmap,
    \* The generation each router instance has loaded.
    \* @type: Str -> Int;
    applied,
    \* Whether each shard refuses client logins.
    \* @type: Str -> Bool;
    fenced,
    \* The writes each shard holds (accepted there or copied in).
    \* @type: Str -> Set({ id: Int, key: Str, at: Str, fresh: Bool });
    data,
    \* The writes acknowledged to clients.
    \* @type: Set({ id: Int, key: Str, at: Str, fresh: Bool });
    acked,
    \* The id of the next client write.
    \* @type: Int;
    nextWrite

vars == <<record, history, configmap, applied, fenced, data, acked, nextWrite>>

Maps == [Keys -> Shards]

\* The map of generation g.
Map(g) == history[g + 1]

\* Keys whose owner changes between generation g - 1 and g.
Moving(g) == {k \in Keys : Map(g - 1)[k] # Map(g)[k]}

\* Shards that lose at least one key in generation g.
Losing(g) == {Map(g - 1)[k] : k \in Moving(g)}

\* Writes acknowledged for key k.
AckedFor(k) == {w \in acked : w.key = k}

\* Keys for which shard s holds every acknowledged write.
CaughtUp(s) == {k \in Keys : AckedFor(k) \subseteq data[s]}

\* A cutover is in progress until the ConfigMap and every instance are on
\* the record's generation.
InCutover ==
    \/ configmap < record.gen
    \/ \E i \in Instances : applied[i] < record.gen

\* Generations still in use somewhere: by an instance or by the ConfigMap
\* (a restarting instance loads the ConfigMap).
LiveGens == {applied[i] : i \in Instances} \cup {configmap}

TypeOK ==
    /\ record.gen \in 0..MaxGen
    /\ record.owner \in Maps
    /\ Len(history) = record.gen + 1
    /\ configmap \in 0..record.gen
    /\ applied \in [Instances -> 0..record.gen]
    /\ fenced \in [Shards -> BOOLEAN]
    /\ nextWrite \in 1..(MaxWrites + 1)

-----------------------------------------------------------------------------
Init ==
    /\ \E m \in Maps :
        /\ record = [gen |-> 0, owner |-> m]
        /\ history = <<m>>
    /\ configmap = 0
    /\ applied = [i \in Instances |-> 0]
    /\ fenced = [s \in Shards |-> FALSE]
    /\ data = [s \in Shards |-> {}]
    /\ acked = {}
    /\ nextWrite = 1

\* Step 1: the control plane publishes generation gen + 1 by CAS. It starts
\* a new cutover only after the previous one has finished.
Publish(m) ==
    /\ record.gen < MaxGen
    /\ ~InCutover
    /\ \A s \in Shards : ~fenced[s]
    /\ m # record.owner
    /\ record' = [gen |-> record.gen + 1, owner |-> m]
    /\ history' = Append(history, m)
    /\ UNCHANGED <<configmap, applied, fenced, data, acked, nextWrite>>

\* Step 2: fence a shard that loses keys in the record's generation.
Fence(s) ==
    /\ record.gen > 0
    /\ configmap < record.gen
    /\ s \in Losing(record.gen)
    /\ ~fenced[s]
    /\ fenced' = [fenced EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<record, history, configmap, applied, data, acked, nextWrite>>

\* Step 3: the final catch-up of key k into its new owner, once the old
\* owner is fenced (so no write can land there after the copy). The fence
\* guard is belt and braces: WriteConfigMap re-checks both conditions, and a
\* mutation run without this guard finds no violation (README).
CopyKey(k) ==
    LET g == record.gen
        old == Map(g - 1)[k]
        new == Map(g)[k]
    IN  /\ g > 0
        /\ k \in Moving(g)
        /\ fenced[old]
        /\ k \notin CaughtUp(new)
        /\ data' = [data EXCEPT ![new] = @ \cup {w \in data[old] : w.key = k}]
        /\ UNCHANGED <<record, history, configmap, applied, fenced, acked, nextWrite>>

\* Step 4: render the record's generation into the ConfigMap.
WriteConfigMap ==
    LET g == record.gen IN
    /\ configmap < g
    /\ \/ UnsafeConfigMapFirst
       \/ /\ \A s \in Losing(g) : fenced[s]
          /\ \A k \in Moving(g) : k \in CaughtUp(Map(g)[k])
    /\ configmap' = g
    /\ UNCHANGED <<record, history, applied, fenced, data, acked, nextWrite>>

\* Step 5: an instance notices the ConfigMap change and reloads it.
Reload(i) ==
    /\ applied[i] # configmap
    /\ applied' = [applied EXCEPT ![i] = configmap]
    /\ UNCHANGED <<record, history, configmap, fenced, data, acked, nextWrite>>

\* An instance restarts and loads the ConfigMap. The effect equals Reload's;
\* it is a separate action so traces name it (RT1 trace validation).
Restart(i) ==
    /\ applied[i] # configmap
    /\ applied' = [applied EXCEPT ![i] = configmap]
    /\ UNCHANGED <<record, history, configmap, fenced, data, acked, nextWrite>>

\* Step 6: unfence once every instance runs the record's generation.
Unfence(s) ==
    /\ fenced[s]
    /\ ~InCutover
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<record, history, configmap, applied, data, acked, nextWrite>>

\* A client write for key k through instance i, routed by the generation i
\* has loaded. An unfenced shard accepts it whatever it owns. `fresh`
\* records whether the shard already held every earlier acknowledged write
\* for k, i.e. whether it was a legitimate place to write k.
ClientWrite(i, k) ==
    LET s == Map(applied[i])[k]
        w == [id |-> nextWrite, key |-> k, at |-> s, fresh |-> k \in CaughtUp(s)]
    IN  /\ nextWrite <= MaxWrites
        /\ ~fenced[s]
        /\ data' = [data EXCEPT ![s] = @ \cup {w}]
        /\ acked' = acked \cup {w}
        /\ nextWrite' = nextWrite + 1
        /\ UNCHANGED <<record, history, configmap, applied, fenced>>

\* The same write refused by a fenced shard; the client sees an error.
Reject(i, k) ==
    LET s == Map(applied[i])[k] IN
    /\ nextWrite <= MaxWrites
    /\ fenced[s]
    /\ nextWrite' = nextWrite + 1
    /\ UNCHANGED <<record, history, configmap, applied, fenced, data, acked>>

Protocol ==
    \/ \E m \in Maps : Publish(m)
    \/ \E s \in Shards : Fence(s) \/ Unfence(s)
    \/ \E k \in Keys : CopyKey(k)
    \/ WriteConfigMap

Next ==
    \/ Protocol
    \/ \E i \in Instances : Reload(i) \/ Restart(i)
    \/ \E i \in Instances, k \in Keys : ClientWrite(i, k) \/ Reject(i, k)

\* Publish is bounded by MaxGen, so weak fairness on the remaining protocol
\* steps and on each instance's reload drives every cutover to completion.
Fairness ==
    /\ WF_vars(\E s \in Shards : Fence(s) \/ Unfence(s))
    /\ WF_vars(\E k \in Keys : CopyKey(k))
    /\ WF_vars(WriteConfigMap)
    /\ \A i \in Instances : WF_vars(Reload(i))

Spec == Init /\ [][Next]_vars /\ Fairness

-----------------------------------------------------------------------------
(* Safety *)

\* Every generation's map is a total function onto shards.
OneOwner == \A g \in 0..record.gen : Map(g) \in Maps

\* For every key, at most one shard that owns it in a live generation is
\* unfenced. This is what prevents two shards accepting writes for one key.
SingleWriter ==
    \A k \in Keys :
        Cardinality({s \in {Map(g)[k] : g \in LiveGens} : ~fenced[s]}) <= 1

\* No write was accepted by a shard that lagged behind the key's history.
NoStrayWrite == \A w \in acked : w.fresh

\* Outside a cutover, the record's owner of each key holds all its
\* acknowledged writes.
NoLostAck ==
    ~InCutover => \A w \in acked : w \in data[record.owner[w.key]]

\* Whoever loads the ConfigMap routes each key to a shard that holds every
\* acknowledged write for it.
ConfigMapSafe == \A k \in Keys : k \in CaughtUp(Map(configmap)[k])

Safety == TypeOK /\ OneOwner /\ SingleWriter /\ NoStrayWrite /\ NoLostAck /\ ConfigMapSafe

(* Liveness *)

\* Every instance eventually runs the record's generation and stays there.
Converges == <>[](\A i \in Instances : applied[i] = record.gen)

=============================================================================
