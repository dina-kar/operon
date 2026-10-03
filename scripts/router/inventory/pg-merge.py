#!/usr/bin/env python3
"""Merge the static kinds and the dynamic captures into capture.jsonl (RT0 plan Task 5, §31 §15 step 3).

Usage: pg-merge.py <capture-dir> <static.tsv> <pg-static-kinds.tsv>

Reads <capture-dir>/<label>-<port>.pgss.jsonl and <label>-<port>.replcmds.log; writes
<capture-dir>/capture.jsonl (one entry per digest: digest, component, source, example, db) and
<capture-dir>/static-unmapped.tsv (static kinds that have no replayable example, with counts).
PgDog source never enters: static input holds paths, line numbers and kinds only.
"""
import glob, hashlib, json, os, re, sys, collections

cap, static_path, kinds_path = sys.argv[1:4]

def norm(sql):
    return re.sub(r"\s+", " ", sql.strip().rstrip(";").strip()).lower()

def strip_comments(sql):
    """Removes -- and /* */ comments outside string literals: comments in a scenario's SQL are its authors' text."""
    out, i, n, q = [], 0, len(sql), None
    while i < n:
        c = sql[i]
        if q:
            out.append(c)
            if c == q:
                if i + 1 < n and sql[i + 1] == q:
                    out.append(sql[i + 1]); i += 1
                else:
                    q = None
        elif c in "'\"":
            q = c; out.append(c)
        elif sql.startswith("--", i):
            while i < n and sql[i] != "\n":
                i += 1
            continue
        elif sql.startswith("/*", i):
            j = sql.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        else:
            out.append(c)
        i += 1
    return re.sub(r"[ \t]*\n[ \t]*", " ", "".join(out)).strip()

def digest(sql):
    return hashlib.sha256(norm(sql).encode()).hexdigest()[:16]

COMPONENTS = [  # first match wins
    ("2pc", r"prepare transaction|commit prepared|rollback prepared|pg_prepared_xacts"),
    ("replication", r"pg_replication_slots|pg_create_logical|pg_logical_slot|pg_publication|pg_subscription|publication|subscription|pg_current_wal_lsn|pg_last_wal|pg_replication_origin|replica identity|pg_stat_replication|pg_export_snapshot|pg_drop_replication_slot|_replication_slot"),
    ("copy", r"^copy\b"),
    ("schema-sync", r"information_schema|pg_catalog|pg_class|pg_attribute|pg_index|pg_constraint|pg_namespace|pg_get_|pg_sequence|pg_type|^(create|alter|drop) (table|index|unique index|sequence|schema|type|extension|view|function|trigger|materialized view)|^grant|^comment on"),
    ("health", r"^select 1\b|^;|pg_is_in_recovery|pg_stat_activity|^show "),
    ("pool", r"^(set|reset|discard|deallocate|begin|commit|rollback|start transaction|savepoint|release|listen|unlisten|notify|end|abort)\b|pg_advisory|pg_terminate_backend|pg_cancel_backend"),
]

def component(sql):
    n = norm(sql)
    for name, pat in COMPONENTS:
        if re.search(pat, n):
            return name
    return "query"

kinds = {}      # kind -> (component, example)
for line in open(kinds_path):
    if line.startswith("#") or not line.strip():
        continue
    k, c, e = line.rstrip("\n").split("\t")
    kinds[k] = (c, e)

entries = collections.OrderedDict()

def add(sql, comp, source, db=None):
    sql = strip_comments(sql)
    d = digest(sql)
    e = entries.get(d)
    if e is None:
        entries[d] = {"digest": d, "component": comp, "source": source, "example": sql.strip(), "db": db}
    elif source not in e["source"].split(";"):
        e["source"] += ";" + source
    return d

# Static kinds that have a canonical example: one row per kind, source = the first hits.
hits = collections.defaultdict(list)
for line in open(static_path):
    loc, kind = line.rstrip("\n").split("\t")
    hits[kind].append(loc)
unmapped = {}
for kind, locs in sorted(hits.items()):
    if kind in kinds:
        c, ex = kinds[kind]
        src = ";".join(locs[:5]) + (f";+{len(locs) - 5} more" if len(locs) > 5 else "")
        add(ex, c, src)
    else:
        unmapped[kind] = len(locs)

# Dynamic: pg_stat_statements per scenario, DBs named <label>_<db> on the reference.
for f in sorted(glob.glob(os.path.join(cap, "*.pgss.jsonl"))):
    label = os.path.basename(f).split("-")[0]
    seen_here = set()
    for line in open(f):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        q = r["query"]
        if not q or q.startswith("<insufficient privilege>"):
            continue
        if "pg_stat_statements" in q or q.lower().startswith("select pg_terminate_backend(pid) from pg_stat_activity where datname"):
            continue
        add(q, component(q), f"dynamic:{label}", f"{label}_{r['db']}")

# --- Two filters that keep PgDog's own SQL and PgDog's test fixtures out of the table ------------------
# 1. PgDog installs plpgsql functions, triggers and tables in its own `pgdog` schema (AGPL-3.0 SQL text), and the
#    scenarios' fixtures define functions and triggers of their own. All such definitions (function, procedure and
#    trigger bodies, anything with $$, and DDL in the pgdog schema) are dropped; one canonical row written for Loams stands for "the target must accept
#    plpgsql functions, triggers and tables in a schema". Statements that only *call* or *read* those
#    objects (SELECT pgdog.install_trigger(...)) are PgDog's behaviour on the wire and stay.
# 2. The user schema a scenario replicates (schema_sync's DDL, pg_dump output) is the scenario's fixture, not the
#    router's own behaviour. DDL of the replicated schema is collapsed to the first KEEP_PER_KIND examples
#    of each statement kind, in digest order; the source column says how many were seen.
KEEP_PER_KIND = 3
own_ddl = re.compile(r"(?is)^\s*(create|alter|drop|comment on|grant|revoke)\b.*\b(pgdog|__pgdog\w*)\b|^\s*create\s+(or\s+replace\s+)?(function|procedure|trigger)\b|\$\$")
dropped_own = 0
for d in [d for d, e in entries.items() if e["source"].startswith("dynamic:") and own_ddl.match(e["example"])]:
    del entries[d]
    dropped_own += 1
if dropped_own:
    canon_ddl = ("CREATE SCHEMA loams_inv_s ;; CREATE TABLE loams_inv_s.cfg (shard bigint NOT NULL, shards bigint NOT NULL) ;; "
                 "CREATE OR REPLACE FUNCTION loams_inv_s.f() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$ ;; "
                 "CREATE TRIGGER loams_inv_trg BEFORE INSERT ON loams_inv_s.cfg FOR EACH ROW EXECUTE FUNCTION loams_inv_s.f()")
    add(canon_ddl, "schema-sync", f"dynamic:data_sync (function, trigger and PgDog-schema definitions; {dropped_own} statements, text not recorded)")

ddl_kind = re.compile(r"(?is)^\s*(create(?:\s+or\s+replace)?|alter|drop|comment\s+on|grant|revoke)\s+(\w+)(?:\s+(if\s+(?:not\s+)?exists|only|unique))?")
by_kind = collections.defaultdict(list)
for d, e in entries.items():
    low = e["example"].lower()
    if e["source"].startswith("dynamic:") and "catalog" not in low and "information_schema" not in low:
        m = ddl_kind.match(e["example"])
        if m:
            kind = " ".join(x.upper() for x in m.groups() if x)
            kind = re.sub(r"\s+", " ", re.sub(r"IF (NOT )?EXISTS", "", kind)).strip()
            by_kind[(kind, e["source"].split(";")[0])].append(d)
collapsed = 0
for (kind, src), ds in by_kind.items():
    ds.sort()
    for d in ds[KEEP_PER_KIND:]:
        del entries[d]
        collapsed += 1
    if len(ds) > KEEP_PER_KIND:
        for d in ds[:KEEP_PER_KIND]:
            entries[d]["source"] += f" (replicated-schema DDL, kind {kind}: {KEEP_PER_KIND} of {len(ds)} kept)"
# 3. Client workload DML and SELECT (the scenarios' own traffic through the router) is capped per statement
#    kind and scenario; the router-originated statements (catalogs, replication, copy, 2PC, pool) are all kept.
KEEP_QUERIES = 12
by_q = collections.defaultdict(list)
for d, e in entries.items():
    if e["component"] == "query" and e["source"].startswith("dynamic:"):
        w = re.match(r"\s*\(?\s*(\w+)", e["example"])
        by_q[((w.group(1) if w else "?").upper(), e["source"].split(";")[0].split(" ")[0])].append(d)
capped = 0
for (kind, src), ds in by_q.items():
    ds.sort()
    for d in ds[KEEP_QUERIES:]:
        del entries[d]
        capped += 1
    if len(ds) > KEEP_QUERIES:
        for d in ds[:KEEP_QUERIES]:
            entries[d]["source"] += f" (workload {kind}: {KEEP_QUERIES} of {len(ds)} kept)"
print(f"capped {capped} workload statements", file=sys.stderr)
print(f"dropped {dropped_own} PgDog-schema definitions; collapsed {collapsed} replicated-schema DDL statements", file=sys.stderr)

# Replication commands from the server log: one row per normalized shape, replayed with our canonical example.
canon = {k: v[1] for k, v in kinds.items() if k in ("IDENTIFY_SYSTEM", "CREATE_REPLICATION_SLOT", "DROP_REPLICATION_SLOT", "START_REPLICATION")}
shapes = {}
for f in sorted(glob.glob(os.path.join(cap, "*.replcmds.log"))):
    label = os.path.basename(f).split("-")[0]
    for line in open(f, errors="replace"):
        m = re.search(r"received replication command: (.*)$", line)
        if not m:
            continue
        cmd = m.group(1)
        kw = cmd.split()[0].upper() if cmd.split() else ""
        shape = re.sub(r"\d+", "?", re.sub(r"[0-9A-Fa-f]+/[0-9A-Fa-f]+", "?", re.sub(r"'[^']*'", "'?'", re.sub(r'"[^"]*"', '"?"', cmd))))
        shapes.setdefault((kw, shape), (label, cmd))
for (kw, shape), (label, cmd) in sorted(shapes.items()):
    if kw not in canon:
        unmapped["replication command " + kw] = unmapped.get("replication command " + kw, 0) + 1
        continue
    d = hashlib.sha256(("repl:" + shape).encode()).hexdigest()[:16]
    entries[d] = {"digest": d, "component": "replication", "source": f"dynamic:{label} (server log)", "example": canon[kw], "db": None}

with open(os.path.join(cap, "capture.jsonl"), "w") as out:
    for e in entries.values():
        out.write(json.dumps({k: v for k, v in e.items() if v is not None}) + "\n")
with open(os.path.join(cap, "static-unmapped.tsv"), "w") as out:
    out.write("kind\thits\n")
    for k, n in sorted(unmapped.items(), key=lambda kv: (-kv[1], kv[0])):
        out.write(f"{k}\t{n}\n")
print(f"{len(entries)} capture entries; {len(unmapped)} static kinds without a replayable example", file=sys.stderr)
