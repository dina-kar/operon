#!/usr/bin/env python3
"""Merge Vitess's static statements and the dynamic digest dump into capture.jsonl (RT0 plan Task 6, §31 §15 step 3),
and attribute digests to the scenario steps that touched them (the `rows` column of the suites table).

Usage: vt-merge.py <capture-dir> <vitess-static.tsv>

Reads   <capture-dir>/digests.<node>.jsonl     final events_statements_summary_by_digest dump per backend
        <capture-dir>/snap/<step>.<node>.{before,after}   DIGEST<TAB>COUNT_STAR before/after each step
        <capture-dir>/steps.tsv                step<TAB>suite<TAB>test<TAB>result
Writes  <capture-dir>/capture.jsonl            digest, component, source, example, db
        <capture-dir>/step-digests.tsv         step<TAB>digest... (my digests touched by the step)
"""
import glob, hashlib, json, os, re, sys, collections

cap, static_path = sys.argv[1:3]

def norm(sql):
    s = sql.strip().rstrip(";").strip()
    s = re.sub(r"'(?:[^'\\]|\\.|'')*'", "?", s)
    s = re.sub(r'"(?:[^"\\]|\\.|"")*"', "?", s)
    s = re.sub(r"%[a-z]\b", "?", s)           # Go format verbs and Vitess's %a
    s = re.sub(r":[a-z_][a-z0-9_]*", "?", s, flags=re.I)  # bind variables
    s = re.sub(r"\b0x[0-9a-f]+\b", "?", s, flags=re.I)
    s = re.sub(r"\b\d+(\.\d+)?\b", "?", s)
    s = re.sub(r"\s+", " ", s).lower().replace("`", "")
    s = re.sub(r"\s*([.,()=<>!+\-*/])\s*", r"\1", s)   # DIGEST_TEXT spaces out punctuation; Go source does not
    s = re.sub(r"\(\?(,\?)+\)", "(?)", s)
    return s

def digest(sql):
    return hashlib.sha256(norm(sql).encode()).hexdigest()[:16]

REGEXES = [  # first match wins
    ("vdiff", r"_vt\.vdiff|vdiff_"),
    ("2pc", r"dt_state|dt_participant|redo_state|redo_statement|_vt\.dt_|\bxa (start|end|prepare|commit|rollback|recover)\b|prepared_transactions"),
    ("onlineddl", r"schema_migrations|_vt_vrp_|_vt_gho|_vt_hold|_vt_ghc|_vt_ddl|\b_vt_[a-z0-9]{8}_"),
    ("vreplication", r"_vt\.vreplication|vreplication|copy_state|post_copy_action|resharding_journal|_vt\.vreplication_log"),
    ("sidecar", r"\b_vt(_[a-z0-9]+)?\b|sidecar"),
    ("reparent", r"^(change|start|stop|reset) (replication|replica|slave|master|binary)|reparent_journal|gtid_purged|gtid_executed|^show (master|binary|replica|slave)|semi_sync|rpl_semi|read_only|super_read_only|^set global|^flush (binary|logs)"),
    ("schema-engine", r"information_schema|^show (create|full tables|table status|tables|columns|index|databases)|\bmysql\.|performance_schema|^describe|^desc "),
    ("health", r"^select \?$|@@|^show (global|session)? ?(variables|status)|heartbeat|^select version|^show warnings|connection_id|^set (names|session|@@|sql_)|^use "),
]

def component_for_sql(sql):
    n = norm(sql)
    for name, pat in REGEXES:
        if re.search(pat, n):
            return name
    return "query"

def component_for_path(path, sql):
    c = component_for_sql(sql)
    if c != "query":
        return c
    for sub, comp in [("sidecardb", "sidecar"), ("vdiff", "vdiff"), ("vstreamer", "vreplication"), ("vreplication", "vreplication"),
                      ("tabletserver/schema", "schema-engine"), ("onlineddl", "onlineddl"), ("dt_executor", "2pc"),
                      ("tx_engine", "2pc"), ("twopc", "2pc"), ("flavor_mysql", "health"), ("tabletmanager", "reparent")]:
        if sub in path:
            return comp
    return "query"

ISSUES = [  # design §31 §9.2: the C-n item a statement bears on (first match; several joined by commas below)
    ("C-1", r"engine\s*=|\b_vt(_[a-z0-9]+)?\b.*\b(create table|alter table|show create table)|^(create table if not exists|alter table|show create table) `?_vt"),
    ("C-2", r"semi_sync|semisync|\bxa (start|end|prepare|commit|rollback|recover)\b|dt_state|dt_participant|redo_state|redo_statement|prepared"),
    ("C-4", r"isolation|tx_isolation|consistent snapshot|\bautocommit\b|set (session )?transaction|read committed|repeatable read|serializable"),
    ("C-5", r"temporary"),
    ("C-6", r"information_schema|performance_schema|^show (create|full tables|table status|tables|columns|index|databases|variables|global|session|warnings|status)|@@"),
    ("C-7", r"binlog|show (master|binary|replica|slave)|gtid_executed|gtid_purged|show master status|vstream"),
]

def issues_for(sql):
    n = norm(sql)
    return ",".join(tag for tag, pat in ISSUES if re.search(pat, n))

entries = collections.OrderedDict()
def add(sql, comp, source, db=None, key=None):
    d = digest(key or sql)
    e = entries.get(d)
    if e is None:
        entries[d] = {"digest": d, "component": comp, "source": source, "example": sql.strip(), "db": db, "issue": issues_for(sql)}
    else:
        for s in source.split(";"):
            if s not in e["source"].split(";"):
                e["source"] += ";" + s
        if db and not e.get("db"):
            e["db"] = db
    return d

# Static first (statement text with its path: Vitess is Apache-2.0).
static_by_digest = {}
for line in open(static_path):
    loc, stmt = line.rstrip("\n").split("\t", 1)
    path = loc.rsplit(":", 1)[0]
    d = add(stmt, component_for_path(path, stmt), loc)
    static_by_digest[d] = loc

# Dynamic: the example is QUERY_SAMPLE_TEXT (a real statement with its literals).
step_of_digest = collections.defaultdict(set)
mysql_to_mine = {}
for f in sorted(glob.glob(os.path.join(cap, "digests.*.jsonl"))):
    for line in open(f):
        line = line.strip()
        if not line:
            continue
        r = json.loads(line)
        text = r.get("sample") or r.get("text")
        if not text or not r.get("text"):
            continue
        schema = r.get("schema")
        low = r["text"].lower()
        if "performance_schema" in low and "events_statements" in low:
            continue  # our own dump
        d = digest(r["text"])
        mysql_to_mine[r["digest"]] = d
        entries_before = d in entries
        add(text, component_for_sql(text) if entries_before is False else entries[d]["component"], "dynamic:vitess", schema, key=r["text"])
        if entries_before:
            # the example becomes the captured one (it has real literals), the static text stays as source
            entries[d]["example"] = text.strip()
            if schema and not entries[d].get("db"):
                entries[d]["db"] = schema

def load_snap(path):
    m = {}
    if os.path.exists(path):
        for line in open(path):
            parts = line.rstrip("\n").split("\t")
            if len(parts) == 2 and parts[1].isdigit():
                m[parts[0]] = int(parts[1])
    return m

steps = []
sp = os.path.join(cap, "steps.tsv")
if os.path.exists(sp):
    steps = [l.rstrip("\n").split("\t") for l in open(sp) if l.strip()]
with open(os.path.join(cap, "step-digests.tsv"), "w") as out:
    for st in steps:
        step = st[0]
        touched = set()
        for node in ("a", "b", "c"):
            b = load_snap(os.path.join(cap, "snap", f"{step}.{node}.before"))
            a = load_snap(os.path.join(cap, "snap", f"{step}.{node}.after"))
            for k, v in a.items():
                if v > b.get(k, 0) and k in mysql_to_mine:
                    touched.add(mysql_to_mine[k])
        for d in touched:
            if d in entries and entries[d]["source"].find("dynamic:") >= 0:
                tag = "dynamic:" + st[1]
                if tag not in entries[d]["source"].split(";"):
                    entries[d]["source"] += ";" + tag
        out.write(step + "\t" + ",".join(sorted(touched)) + "\n")

with open(os.path.join(cap, "capture.jsonl"), "w") as out:
    for e in entries.values():
        out.write(json.dumps({k: v for k, v in e.items() if v}) + "\n")
print(f"{len(entries)} capture entries ({len(static_by_digest)} static)", file=sys.stderr)
