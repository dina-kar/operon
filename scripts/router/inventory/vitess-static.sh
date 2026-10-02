#!/usr/bin/env bash
# Static half of the MySQL inventory (RT0 plan Task 6, §31 §15 step 1).
#
# Usage: vitess-static.sh "$VITESS_SRC"   (a Vitess v24.0.4 checkout, e.g. $HOME/.cache/loam/vitess-v24.0.4)
#
# Prints `source_path:line<TAB>statement` for every SQL string literal (double-quoted or raw
# backtick) in the files that talk to MySQL, plus every statement in the sidecar schema files.
# Vitess is Apache-2.0, so the statement text is recorded with its path (§31 §16). Whitespace is
# collapsed; format verbs (%s, %d) and bind variables (:name) stay as written. Test files are skipped.
set -euo pipefail
src="${1:-${VITESS_SRC:-}}"
[[ -n "$src" && -d "$src/go/vt" ]] || { echo "usage: $0 <vitess checkout>" >&2; exit 2; }
cd "$src"
python3 - <<'PY'
import os, re, sys

roots = [  # §31 §15 step 1, plus the two-phase-commit and online-DDL code paths
    "go/mysql/flavor_mysql.go", "go/mysql/flavor_mysql_legacy.go", "go/mysql/flavor_mysqlgr.go",
    "go/vt/vttablet/tabletserver/schema",
    "go/vt/vttablet/tabletmanager",
    "go/vt/vttablet/tabletserver/vstreamer",
    "go/vt/vttablet/tabletmanager/vreplication",
    "go/vt/vttablet/tabletserver/dt_executor.go", "go/vt/vttablet/tabletserver/tx_engine.go",
    "go/vt/vttablet/tabletserver/twopc.go", "go/vt/vttablet/tabletserver/tx_engine_dt.go",
    "go/vt/vttablet/onlineddl",
    "go/vt/sidecardb/schema",
    "go/vt/sidecardb/sidecardb.go",
]
verbs = r"select|insert|update|delete|create|alter|drop|set|show|begin|commit|rollback|start|flush|reset|stop|change|replace|truncate|rename|lock|unlock|xa|savepoint|release|call|use|with|explain|desc|describe|optimize|analyze|checksum|grant|revoke|kill|load|install|uninstall|purge|binlog|repair"
sql_start = re.compile(r"^\s*(?:/\*.*?\*/\s*)?(?:%s)\b" % verbs, re.I | re.S)
dq = re.compile(r'"((?:[^"\\\n]|\\.)*)"')
bt = re.compile(r"`([^`]*)`", re.S)

def files():
    seen = set()
    for r in roots:
        if os.path.isfile(r):
            paths = [r]
        elif os.path.isdir(r):
            paths = [os.path.join(d, f) for d, _, fs in os.walk(r) for f in fs]
        else:
            continue
        for p in sorted(paths):
            if p in seen or p.endswith("_test.go") or "/testdata/" in p or "/endtoend/" in p:
                continue
            if p.endswith(".go") or p.endswith(".sql"):
                seen.add(p)
                yield p

def looks_like_sql(s):
    """Drops log messages and errors that merely start with a SQL verb ("Change to tablet state", "Unlock tables
    failed: %v"): Vitess writes statements in all lower case or all upper case, messages in sentence case."""
    first = s.split()[0]
    if first[0].isupper() and first[1:].islower():
        return False
    return not re.search(r"%\+?v|\bfailed\b|\berror\b", s, re.I)

def collapse(s):
    return re.sub(r"\s+", " ", s.replace('\\"', '"').replace("\\n", " ").replace("\\t", " ")).strip()

out = []
for p in files():
    text = open(p, encoding="utf-8", errors="replace").read()
    if p.endswith(".sql"):
        body = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
        pos = 0
        for stmt in re.split(r";\s*(?:\n|$)", text):
            stmt = stmt.strip()
            if not stmt:
                continue
            line = text.find(stmt.splitlines()[0], pos) if stmt else 0
            pos = max(pos, line)
            ln = text.count("\n", 0, line) + 1
            if sql_start.match(stmt):
                out.append((p, ln, collapse(stmt)))
        continue
    # Go: raw strings first (they span lines), then single-line quoted strings outside them.
    raw_spans = []
    for m in bt.finditer(text):
        raw_spans.append(m.span())
        s = m.group(1)
        if sql_start.match(s) and looks_like_sql(s.strip()):
            out.append((p, text.count("\n", 0, m.start()) + 1, collapse(s)))
    def in_raw(i):
        return any(a <= i < b for a, b in raw_spans)
    for m in dq.finditer(text):
        if in_raw(m.start()):
            continue
        s = m.group(1)
        if sql_start.match(s) and looks_like_sql(s.strip()) and (" " in s.strip() or s.strip().lower() in ("begin", "commit", "rollback")):
            out.append((p, text.count("\n", 0, m.start()) + 1, collapse(s)))
seen = set()
for p, ln, s in sorted(out):
    k = (p, ln, s)
    if k in seen:
        continue
    seen.add(k)
    print(f"{p}:{ln}\t{s}")
PY
