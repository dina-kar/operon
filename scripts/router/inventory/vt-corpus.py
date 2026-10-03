#!/usr/bin/env python3
"""Build the vtgate DML and SELECT corpus (RT0 plan Task 6) from Vitess's planbuilder test data.

Usage: vt-corpus.py <vitess checkout> <limit> > corpus.sql

Takes the `query` of every case in go/vt/vtgate/planbuilder/testdata/*_cases.json (Apache-2.0) that is a
plain SELECT, INSERT, UPDATE or DELETE, drops duplicates, and prints at most <limit> statements, one per
line. Statements that name a keyspace the capture stack lacks, or that vtgate refuses to plan, never reach
the backend; the digest capture only sees what does.
"""
import glob, json, os, re, sys

src, limit = sys.argv[1], int(sys.argv[2])
files = sorted(glob.glob(os.path.join(src, "go/vt/vtgate/planbuilder/testdata/*_cases.json")))
skip = ("unsupported", "unknown_schema", "vexplain", "wireup", "large", "mirror", "foreignkey", "bypass", "onecase", "show", "other_admin", "ddl", "alterVschema", "migration", "flush", "lock", "call", "set_", "use_", "view", "sysschema", "info_schema")
seen, out = set(), []
per_file = {}
for f in files:
    name = os.path.basename(f)
    if any(name.startswith(s) for s in skip):
        continue
    try:
        cases = json.load(open(f))
    except Exception:
        continue
    for c in cases:
        q = c.get("query") if isinstance(c, dict) else None
        if not q or not re.match(r"^\s*\(?\s*(select|insert|update|delete|with)\b", q, re.I):
            continue
        q = " ".join(q.split())
        key = q.lower()
        if key in seen or ";" in q or "/*vt+" in q:
            continue
        seen.add(key)
        per_file.setdefault(name, []).append(q)
# Round-robin across files so one big file does not take the quota.
while len(out) < limit and any(per_file.values()):
    for name in sorted(per_file):
        if per_file[name] and len(out) < limit:
            out.append(per_file[name].pop(0))
for q in out:
    print(q + ";")
print(f"{len(out)} statements from {len(files)} files", file=sys.stderr)
