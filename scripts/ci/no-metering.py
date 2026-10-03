#!/usr/bin/env python3
"""Enforce D552 without printing file contents or scanning untracked secrets."""
from fnmatch import fnmatchcase
from pathlib import Path
import subprocess
import sys
import tomllib


# Compose these names so the guard also checks its own source and fixtures.
MARKERS = (b"loams." + b"meter", b"meter." + b"sock", b"Host" + b"Report",
           b"x-loams-" + b"usage", b"loams_" + b"meter_")
ALLOWLIST = ("docs/design/13-decision-log.md", "docs/design/_pending/*.md",
             "docs/design/27-usage-hooks.md", "docs/design/41-*.md",
             "docs/open-core.md", "docs/plans/2026-10-0*-rn1-*.md",
             "docs/plans/2026-10-02-mt4-*.md", "CHANGELOG.md")
PREFIX = "loam-platform"


def private_package(value):
    if isinstance(value, dict):
        for key, child in value.items():
            if key.startswith(PREFIX):
                return True
            if key in ("name", "package") and isinstance(child, str) and child.startswith(PREFIX):
                return True
            if private_package(child):
                return True
    elif isinstance(value, list):
        return any(private_package(child) for child in value)
    return False


def main():
    root = Path(subprocess.check_output(["git", "rev-parse", "--show-toplevel"]).decode().strip())
    names = subprocess.check_output(["git", "-C", str(root), "ls-files", "-z"]).split(b"\0")
    failed = False
    for raw_name in filter(None, names):
        name = raw_name.decode(errors="surrogateescape")
        path = root / name
        if not path.exists() and not path.is_symlink():
            # A tracked deletion is not part of the candidate tree.
            continue
        if path.is_symlink() and path.name in ("Cargo.toml", "Cargo.lock"):
            print(f"{name}: symlinked Cargo manifest/lockfile is forbidden", file=sys.stderr)
            failed = True
            continue
        if path.is_symlink():
            # Scan the committed symlink target, never the file it points to.
            data = path.readlink().as_posix().encode(errors="surrogateescape")
        else:
            data = path.read_bytes()
        if path.name in ("Cargo.toml", "Cargo.lock"):
            try:
                document = tomllib.loads(data.decode())
            except (UnicodeError, tomllib.TOMLDecodeError):
                print(f"{name}: cannot parse Cargo manifest/lockfile", file=sys.stderr)
                failed = True
            else:
                if private_package(document):
                    print(f"{name}: private platform package is forbidden", file=sys.stderr)
                    failed = True
        if any(len(name.split("/")) == len(allowed.split("/"))
               and all(fnmatchcase(part, pattern) for part, pattern in
                       zip(name.split("/"), allowed.split("/")))
               for allowed in ALLOWLIST):
            continue
        for marker in MARKERS:
            start = data.find(marker)
            if start >= 0:
                line = data[:start].count(b"\n") + 1
                print(f"{name}:{line}: forbidden billing-grade name {marker.decode()}", file=sys.stderr)
                failed = True
    if failed:
        return 1
    print("no-metering: tracked files and Cargo packages satisfy the open-core boundary")
    return 0


if __name__ == "__main__":
    sys.exit(main())
