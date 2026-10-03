#!/usr/bin/env python3
"""List the crates a crates.io release may publish, in dependency order.

Issue #254 defines the crates.io release path. The list cannot be derived
silently from the workspace: a crate is publishable only when its manifest does
not say `publish = false` *and* none of its normal dependencies is a crate that
is itself unpublishable (crates.io would resolve them, and they are never
uploaded). Dev-dependencies do not count: cargo strips them from the published
manifest.

So the release workflow carries an explicit, reviewed list, and this script is
what proves the list still matches the manifests. `--expect` fails with the
correct order when it does not, and `--self-test` exercises the exclusion and
the ordering on fixture workspaces.

Usage:
    publishable-crates.py [--root DIR] [--expect name,name,...] [--self-test]
"""
from pathlib import Path
import argparse
import sys
import tomllib

# Dependency tables that reach the published manifest. A `[target.'cfg(...)']`
# table is conditional, and cargo still requires a version for those, so they
# count too.
BUILD_SECTIONS = ("dependencies", "build-dependencies")
SELF_TEST_NOTE = "publishable-crates: self-test passed"


class ManifestError(Exception):
    """A workspace this script cannot read."""


def read_manifest(path):
    try:
        with path.open("rb") as handle:
            return tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise ManifestError(f"{path}: {error}") from error


def resolved(dep, spec, workspace_deps):
    """The spec of `dep`, following one level of workspace inheritance."""
    if isinstance(spec, dict) and spec.get("workspace"):
        return workspace_deps.get(dep, {})
    return spec


def read_workspace(root):
    """Return (workspace_dependencies, {crate: manifest}) for `root`."""
    root = Path(root)
    members = read_manifest(root / "Cargo.toml")
    workspace_deps = members.get("workspace", {}).get("dependencies", {})
    manifests = {}
    for crate in sorted((root / "crates").iterdir()):
        manifest_path = crate / "Cargo.toml"
        if not manifest_path.is_file():
            continue
        manifest = read_manifest(manifest_path)
        manifests[manifest["package"]["name"]] = manifest
    return workspace_deps, manifests


def normal_dependencies(manifest, workspace_deps):
    """The names a published manifest would require, in every table cargo keeps."""
    names = set()
    tables = [manifest.get(section, {}) for section in BUILD_SECTIONS]
    tables += [table.get("dependencies", {}) for table in manifest.get("target", {}).values()]
    for table in tables:
        for dep, spec in table.items():
            if isinstance(spec, str):
                continue
            if "git" in resolved(dep, spec, workspace_deps):
                raise ManifestError(
                    f"{manifest['package']['name']}: {dep} is a git dependency; "
                    "crates.io cannot publish a manifest without a version"
                )
            names.add(dep)
    return names


def publishable(root):
    """The publishable crate names, in an order that respects dependencies."""
    workspace_deps, manifests = read_workspace(root)
    unpublishable = {
        name for name, manifest in manifests.items() if manifest["package"].get("publish") is False
    }
    reachable = {}
    for name, manifest in manifests.items():
        if name in unpublishable:
            continue
        if normal_dependencies(manifest, workspace_deps) & unpublishable:
            unpublishable.add(name)
    order, done = [], set()

    def visit(name, stack):
        if name in done:
            return
        if name in stack:
            raise ManifestError(f"dependency cycle: {' -> '.join(stack + (name,))}")
        manifest = manifests[name]
        for dep in sorted(normal_dependencies(manifest, workspace_deps) & set(manifests)):
            if dep not in unpublishable:
                visit(dep, stack + (name,))
        done.add(name)
        order.append(name)

    for name in sorted(manifests):
        if name not in unpublishable:
            visit(name, ())
    return order


def compare(expected, actual):
    """A diff the reader can act on, or "" when the lists agree."""
    if expected == actual:
        return ""
    lines = []
    for index in range(max(len(expected), len(actual))):
        want = expected[index] if index < len(expected) else None
        have = actual[index] if index < len(actual) else None
        if want != have:
            lines.append(f"  position {index + 1}: declared {want or '-'}, workspace {have or '-'}")
    return "\n".join(lines)


def self_test():
    """Fixtures: an order that respects dependencies, and two exclusions."""
    import tempfile

    def workspace(publish_false, edges):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "Cargo.toml").write_text('[workspace]\nmembers = ["crates/*"]\n')
            for name in ("app", "leaf", "internal", "blocked", "middle", "gitted"):
                crate = root / "crates" / name
                crate.mkdir(parents=True)
                package = f'name = "{name}"\nversion = "0.0.1"\nedition = "2024"\n'
                if name in publish_false:
                    package += "publish = false\n"
                (crate / "Cargo.toml").write_text(
                    f"[package]\n{package}\n[dependencies]\n" + edges.get(name, "")
                )
                (crate / "lib.rs").write_text("")
            try:
                return publishable(root), None
            except ManifestError as error:
                return None, str(error)

    # `leaf` first because `app` depends on it. `internal` is excluded by its
    # own flag, and a dev-dependency on it does not exclude `app`. `blocked` is
    # excluded by its flag, which also excludes `middle`, the crate that needs
    # it. A git dependency stops the run outright, because such a manifest has
    # no version crates.io could resolve.
    edges = {
        "app": 'leaf = { path = "../leaf" }\n'
        '[dev-dependencies]\ninternal = { path = "../internal" }\n',
        "middle": 'blocked = { path = "../blocked" }\n',
    }
    got, error = workspace({"internal", "blocked", "gitted"}, edges)
    if error or got != ["leaf", "app"]:
        return f"self-test: expected ['leaf', 'app'], got {got or error}"
    got, error = workspace({"leaf", "app", "internal", "blocked", "middle", "gitted"}, {})
    if error or got != []:
        return f"self-test: a fully excluded workspace listed {got or error}"
    got, error = workspace({}, {"leaf": 'gitted = { git = "https://example.invalid/p" }'})
    if got is not None or "git dependency" not in (error or ""):
        return f"self-test: a git dependency did not stop the crate ({got or error})"
    print(SELF_TEST_NOTE)
    return None


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", default=None, help="workspace root (default: this repository)")
    parser.add_argument("--expect", default=None, help="comma-separated list the workflow declares")
    parser.add_argument("--self-test", action="store_true", help="run the fixtures and exit")
    args = parser.parse_args(argv)

    if args.self_test:
        failure = self_test()
        if failure:
            print(failure, file=sys.stderr)
            return 1
        return 0

    try:
        actual = publishable(args.root or Path(__file__).resolve().parents[2])
    except ManifestError as error:
        print(f"publishable-crates: {error}", file=sys.stderr)
        return 1

    expected = None
    if args.expect is not None:
        expected = [name for name in args.expect.split(",") if name]
        diff = compare(expected, actual)
        if diff:
            print("publishable-crates: the declared list no longer matches the workspace", file=sys.stderr)
            print(diff, file=sys.stderr)
            print("publishable-crates: the correct order is " + ",".join(actual), file=sys.stderr)
            return 1
        print("publishable-crates: the declared list matches the workspace")
        return 0

    print("\n".join(actual))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
