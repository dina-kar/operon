#!/usr/bin/env python3
"""Build a `…-suites.tsv` (columns: suite, test, result, rows) from a capture's steps.tsv (§31 §15 step 5).

Usage: suites.py <steps.tsv> <statements.tsv> <out.tsv> [--target-pending] [--ref-steps <steps.tsv of the reference run>]

steps.tsv rows are either   suite, test, result, detail          (Postgres: PgDog's scenarios)
                       or   step, suite, test, result            (MySQL: the Vitess scenario steps)
`rows` is the number of statement rows in <statements.tsv> whose source names the suite
(`dynamic:<suite>`), the inventory rows the suite touches.

With --target-pending the suite ran only against the reference engine: the row's result is
`pending-target` and the reference result goes into the test name, e.g. `… [reference: pass]`.
With --ref-steps (MySQL), the row carries the target run's result and the reference's result is in
the test name: `… [reference: pass]`.
"""
import csv, sys

args = sys.argv[1:]
steps_path, stmts_path, out_path = args[:3]
pending = "--target-pending" in args
ref_steps = args[args.index("--ref-steps") + 1] if "--ref-steps" in args else None

def read_steps(path):
    out = []
    for line in open(path):
        p = line.rstrip("\n").split("\t")
        if len(p) < 4:
            continue
        if p[0].startswith("s") and p[0][1:].isdigit():      # step, suite, test, result
            out.append((p[1], p[2], p[3]))
        else:                                                  # suite, test, result, detail
            out.append((p[0], p[1] + (f" - {p[3]}" if p[3] and p[2] != "pass" else ""), p[2]))
    return out

counts = {}
with open(stmts_path, newline="") as f:
    r = csv.DictReader(f, delimiter="\t")
    for row in r:
        for src in row["source"].split(";"):
            if src.startswith("dynamic:"):
                name = src[len("dynamic:"):].split(" ")[0]
                counts[name] = counts.get(name, 0) + 1

ref = {}
if ref_steps:
    for s, t, res in read_steps(ref_steps):
        ref[(s, t)] = res

with open(out_path, "w") as out:
    out.write("suite\ttest\tresult\trows\n")
    for suite, test, result in read_steps(steps_path):
        t = test.replace("\t", " ")
        res = result
        if pending:
            t = f"{t} [reference: {result}]"
            res = "pending-target"
        elif ref_steps:
            t = f"{t} [reference: {ref.get((suite, test), 'n/a')}]"
        out.write(f"{suite}\t{t}\t{res}\t{counts.get(suite, 0)}\n")
