#!/usr/bin/env python3
"""Run the TLA+ specs listed in spec/tla/*/specs.toml and compare each result
with its declared `expect` (RT0 Task 1, D310).

Usage: scripts/spec/check.sh <Spec> [<variant cfg>] [--parse-only] [--nightly]
       scripts/spec/check.sh --all [--nightly]
       scripts/spec/check.sh --self-test

Exit codes: 0 every result matched, 1 a mismatch, 2 a tool checksum mismatch
or a missing tool.
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import tomllib
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SPEC_DIRS = [ROOT / "spec/tla/router", ROOT / "spec/tla/selftest"]
LOCK = Path(__file__).with_name("tools.lock")
CACHE = Path(os.environ.get("LOAMS_SPEC_TOOLS", Path.home() / ".cache/loam/spec-tools"))
WORKERS = os.environ.get("TLC_WORKERS", "2")
# TLC spills its state queue to the metadir; keep it off /tmp (often a small RAM tmpfs).
WORK = Path(os.environ.get("LOAMS_SPEC_WORK", Path.home() / ".cache/loam/spec-work"))


class ToolError(Exception):
    pass


def read_lock(lock: Path) -> dict[str, tuple[str, str, str]]:
    tools = {}
    for line in lock.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, version, url, sha = line.split()
        tools[name] = (version, url, sha)
    return tools


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def fetch(name: str, version: str, url: str, sha: str) -> Path:
    CACHE.mkdir(parents=True, exist_ok=True)
    dest = CACHE / f"{name}-{version}{''.join(Path(url).suffixes) or '.bin'}"
    if not dest.exists():
        print(f"downloading {name} {version}", file=sys.stderr)
        tmp = dest.with_suffix(dest.suffix + ".part")
        urllib.request.urlretrieve(url, tmp)
        # Verify before publishing into the cache, so a bad download is never reused.
        if sha256(tmp) != sha:
            tmp.unlink()
            raise ToolError(f"checksum mismatch for {name}")
        tmp.rename(dest)
    if sha256(dest) != sha:
        raise ToolError(f"checksum mismatch for {name}")
    return dest


def tools(lock: Path = LOCK) -> dict[str, Path]:
    found = {}
    for name, (version, url, sha) in read_lock(lock).items():
        path = fetch(name, version, url, sha)
        if name == "apalache":
            unpacked = CACHE / f"apalache-{version}"
            if not unpacked.exists():
                with tarfile.open(path) as tar:
                    tar.extractall(CACHE, filter="data")
                if not unpacked.exists() and (CACHE / "apalache").exists():
                    (CACHE / "apalache").rename(unpacked)
            path = unpacked / "bin/apalache-mc"
        found[name] = path
    return found


def load_specs() -> list[tuple[Path, dict]]:
    specs = []
    for d in SPEC_DIRS:
        f = d / "specs.toml"
        if f.exists():
            for spec in tomllib.loads(f.read_text())["spec"]:
                specs.append((d, spec))
    return specs


VIOLATION = re.compile(r"Error: Invariant (\w+) is violated")


def run_tlc(tla2tools: Path, d: Path, model: str, cfg: str) -> tuple[str, str]:
    WORK.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="tlc-", dir=WORK) as meta:
        cmd = ["java", "-XX:+UseParallelGC", f"-Djava.io.tmpdir={meta}", "-cp", str(tla2tools),
               "tlc2.TLC", "-workers", WORKERS, "-deadlock", "-metadir", meta, "-config", cfg, model]
        out = subprocess.run(cmd, cwd=d, capture_output=True, text=True).stdout
    if "Model checking completed. No error has been found." in out:
        return "ok", out
    if m := VIOLATION.search(out):
        return f"violation:{m.group(1)}", out
    if "Temporal properties were violated" in out:
        return "violation:temporal", out
    if "Deadlock reached" in out:
        return "violation:deadlock", out
    return "error", out


def run_sany(tla2tools: Path, d: Path, model: str) -> tuple[str, str]:
    WORK.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="sany-", dir=WORK) as tmp:
        cmd = ["java", f"-Djava.io.tmpdir={tmp}", "-cp", str(tla2tools), "tla2sany.SANY", model]
        p = subprocess.run(cmd, cwd=d, capture_output=True, text=True)
    out = p.stdout + p.stderr
    ok = p.returncode == 0 and "error" not in out.lower().replace("errors: 0", "")
    return ("ok" if ok else "error"), out


def run_apalache(apalache: Path, d: Path, model: str, cinit: str, inv: str, length: int) -> tuple[str, str]:
    WORK.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="apalache-", dir=WORK) as tmp:
        cmd = [str(apalache), "check", f"--out-dir={tmp}", f"--cinit={cinit}", "--init=Init",
               "--next=Next", f"--inv={inv}", f"--length={length}", model]
        p = subprocess.run(cmd, cwd=d, capture_output=True, text=True)
    out = p.stdout + p.stderr
    if "The outcome is: NoError" in out:
        return "ok", out
    if "The outcome is: Error" in out:
        return f"violation:{inv}", out
    return "error", out


STATES = re.compile(r"(\d[\d,]*) distinct states found")


def check(specs, tl, only=None, variant=None, nightly=False, parse_only=False) -> bool:
    all_ok = True
    ran = 0
    for d, spec in specs:
        if only and spec["name"] != only:
            continue
        if parse_only or spec.get("parse_only"):
            if variant:
                # A variant was named; a parse-only spec has none, so it does not match.
                continue
            ran += 1
            result, out = run_sany(tl["tla2tools"], d, spec["model"])
            report(spec["name"], "(parse)", "ok", result, 0.0, out)
            all_ok &= result == "ok"
            continue
        for v in spec.get("variant", []):
            if variant and v["cfg"] != variant:
                continue
            if not variant and not (v.get("nightly") if nightly else v.get("pr")):
                continue
            ran += 1
            t0 = time.monotonic()
            result, out = run_tlc(tl["tla2tools"], d, spec["model"], v["cfg"])
            report(spec["name"], v["cfg"], v["expect"], result, time.monotonic() - t0, out)
            all_ok &= result == v["expect"]
            for inv in v.get("apalache", []):
                t0 = time.monotonic()
                result, out = run_apalache(tl["apalache"], d, spec["model"], v["apalache_cinit"], inv,
                                           v.get("apalache_length", 8))
                report(spec["name"], f"{v['cfg']} apalache {inv}", "ok", result, time.monotonic() - t0, out)
                all_ok &= result == "ok"
    if ran == 0:
        # An unknown spec or variant name must not pass silently.
        print(f"no spec or variant matched (spec={only!r}, variant={variant!r})", file=sys.stderr)
        return False
    return all_ok


def report(name, variant, expect, actual, secs, out):
    states = STATES.findall(out)
    mark = "PASS" if expect == actual else "FAIL"
    print(f"{mark} {name} {variant}: expected {expect}, got {actual}, "
          f"{states[-1] if states else '-'} states, {secs:.1f}s", flush=True)
    if expect != actual:
        print(out[-4000:], file=sys.stderr)


def self_test() -> int:
    tl = tools()
    if not check([(ROOT / "spec/tla/selftest", s) for _, s in load_specs() if s["name"] == "Selftest"], tl):
        print("self-test: the self-test spec did not behave as declared", file=sys.stderr)
        return 1
    with tempfile.TemporaryDirectory() as tmp:
        bad = Path(tmp) / "tools.lock"
        lines = []
        for line in LOCK.read_text().splitlines():
            parts = line.split()
            if len(parts) == 4 and parts[0] == "tla2tools":
                parts[3] = "0" * 64
            lines.append(" ".join(parts))
        bad.write_text("\n".join(lines) + "\n")
        try:
            tools(bad)
        except ToolError as e:
            print(f"self-test: corrupted lock refused ({e})")
            return 0
    print("self-test: a corrupted tools.lock was accepted", file=sys.stderr)
    return 1


def main() -> int:
    if not shutil.which("java"):
        print("java not found (Java 21+ is required)", file=sys.stderr)
        return 2
    ap = argparse.ArgumentParser()
    ap.add_argument("spec", nargs="?")
    ap.add_argument("variant", nargs="?")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--nightly", action="store_true")
    ap.add_argument("--parse-only", action="store_true")
    ap.add_argument("--self-test", action="store_true")
    a = ap.parse_args()
    try:
        if a.self_test:
            return self_test()
        if not a.all and not a.spec:
            ap.error("give a spec name or --all")
        ok = check(load_specs(), tools(), only=a.spec, variant=a.variant,
                   nightly=a.nightly, parse_only=a.parse_only)
    except ToolError as e:
        print(e, file=sys.stderr)
        return 2
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
