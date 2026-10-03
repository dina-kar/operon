#!/usr/bin/env python3
"""Decide which files a SignPath signing request may cover: Linux only.

The owner's ruling for issues #253 and #264 (2026-10-03) splits signing by
platform, and the split is not the one the original issue assumed:

    Linux    signed, through SignPath (this script's whole subject)
    Windows  unsigned
    macOS    unsigned

So the signing request is built here rather than in the workflow's shell, and
the rule is one line: a file qualifies only when it is a Linux artifact. A
Windows or macOS artifact submitted "just in case" is rejected by name and by
magic, because a release that appears to sign more than it does is worse than
one that visibly refuses.

What "is a Linux artifact" means here, and why the check is not just the file
name:

    *.rpm          an RPM package, which is the one Linux format SignPath's
                   Open Source Code Signing edition can sign (verified
                   2026-10-03: `<rpm-sign>` is listed as available for
                   "Semantic Code Signing, Open Source Code Signing",
                   while `<debsigs-sign>` is Semantic-only and
                   `<create-gpg-signature>` on a bare file is Semantic-only
                   too). An RPM is a payload format: the ELF binary inside it
                   is signed by the package signature, so no ELF test applies.
    ELF + x86-64  a bare Linux x86-64 ELF executable, which is what a
    ELF + aarch64  cargo-dist-style `.tar.gz` contains. Accepted so the
                   workflow has a working path before RPM packaging exists
                   (D621); a bare ELF is only signed by a policy whose
                   certificate type can carry a detached signature, and the
                   OSS edition cannot, so the policy must be created before
                   this branch can ever succeed.
    anything else rejected, including .exe/.msi/.dll/.appx (Windows),
                   .dmg/.pkg (macOS) and .zip/.tar.gz on their own (a bare
                   archive is not signed; its contents are, so a caller holding
                   an archive must unpack it and point this script at the
                   unpacked directory).

Usage:
    signpath-artifacts.py [--strict] DIR [DIR ...]
    signpath-artifacts.py --self-test

Exits 0 and prints one `method<TAB>path` line per accepted file. Anything that is
not a Linux artifact is reported on stderr with the reason and is never
submitted; the run fails when nothing was accepted, so a request is never empty
and never quietly narrower than the release.

`--strict` additionally fails when anything was rejected. `.github/workflows/
release-sign.yml` deliberately does **not** use it: it downloads every asset of a
release, which by decision includes unsigned Windows and macOS files, and
skipping those is the correct outcome rather than a failure. `--strict` is for
a caller that has already narrowed the directory to the files it means to sign.
"""
from pathlib import Path
import argparse
import struct
import sys

SELF_TEST_NOTE = "signpath-artifacts: self-test passed"

# Rejected by extension before any content is read, so the error names the
# platform rather than "unrecognised format".
WINDOWS_SUFFIXES = (".exe", ".msi", ".msix", ".msixbundle", ".appx", ".appxbundle",
                    ".dll", ".sys", ".efi", ".cab", ".msp", ".msm", ".ps1", ".psm1")
MACOS_SUFFIXES = (".dmg", ".pkg", ".app", ".mpkg")
# An archive is a container, not a signed artifact. The workflow unpacks it and
# passes the ELF inside, so a path ending here is a mistake in the workflow.
ARCHIVE_SUFFIXES = (".zip", ".gz", ".xz", ".bz2", ".zst")

# The first four bytes of every ELF object, on every endianness.
ELF_MAGIC = b"\x7fELF"
# e_ident[EI_CLASS] (offset 4) and e_machine (offset 18, endianness-dependent).
ELF_CLASS_64 = 2
ELF_MACHINE_OFFSET = 18
EM_X86_64 = 62
EM_AARCH64 = 183
EM_386 = 3
# An RPM lead: 0xed 0xab 0xee 0xdb.
RPM_MAGIC = b"\xed\xab\xee\xdb"


class Rejected(Exception):
    """An artifact this script refuses to submit for signing."""


def _suffix(path):
    return path.suffix.lower()


def read_elf_machine(data):
    """Return the e_machine of an ELF header, or raise.

    Only the 64-bit little-endian and big-endian layouts are decoded, because
    the two targets this project builds for (x86-64 and aarch64 Linux) are
    always one of those and the offsets are the only thing that moves.
    """
    if len(data) < ELF_MACHINE_OFFSET + 2:
        raise Rejected("truncated: not a whole ELF header")
    if data[:4] != ELF_MAGIC:
        raise Rejected("not an ELF object (bad magic)")
    if data[4] != ELF_CLASS_64:
        raise Rejected("not a 64-bit ELF object")
    endian = data[5]
    if endian == 1:
        fmt = "<H"
    elif endian == 2:
        fmt = ">H"
    else:
        raise Rejected(f"unknown ELF data encoding {endian}")
    (machine,) = struct.unpack_from(fmt, data, ELF_MACHINE_OFFSET)
    return machine


def classify(path):
    """Return the signing method an artifact needs, or raise Rejected.

    The method is what the workflow asserts the SignPath artifact
    configuration has to match, so a configuration that signs RPMs is never
    quietly pointed at an ELF file.
    """
    suffix = _suffix(path)
    if suffix in WINDOWS_SUFFIXES:
        raise Rejected(
            f"a Windows artifact ({suffix}); the owner's 2026-10-03 ruling leaves "
            "Windows unsigned and documents a local build instead")
    if suffix in MACOS_SUFFIXES:
        raise Rejected(
            f"a macOS artifact ({suffix}); the owner's 2026-10-03 ruling leaves "
            "macOS unsigned and documents a local build instead")
    if suffix in ARCHIVE_SUFFIXES:
        raise Rejected(
            f"a bare archive ({suffix}); unpack it and submit the ELF inside, "
            "because an archive is signed through its contents, not itself")
    if suffix == ".rpm":
        try:
            head = path.read_bytes()[:4]
        except OSError as error:
            raise Rejected(f"unreadable: {error}") from error
        if head != RPM_MAGIC:
            raise Rejected(f"named .rpm but starts with {head.hex(' ')}, not the RPM magic {RPM_MAGIC.hex(' ')}")
        return "rpm-sign"
    if path.name == "loams" or suffix == "":
        try:
            data = path.read_bytes()[:ELF_MACHINE_OFFSET + 2]
        except OSError as error:
            raise Rejected(f"unreadable: {error}") from error
        machine = read_elf_machine(data)
        if machine == EM_X86_64:
            return "linux-elf-x86_64"
        if machine == EM_AARCH64:
            return "linux-elf-aarch64"
        raise Rejected(f"an ELF for e_machine {machine}, which is not a Loams release target")
    raise Rejected(
        f"an unrecognised artifact ({suffix or 'no extension'}); SignPath signs "
        "an .rpm or a Linux ELF executable, and nothing else this repository builds")


def scan(directories):
    """Classify every file under `directories`. Returns (accepted, rejected)."""
    accepted = []
    rejected = []
    for directory in directories:
        root = Path(directory)
        if not root.is_dir():
            rejected.append((root, Rejected("not a directory")))
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file() or path.is_symlink():
                continue
            try:
                method = classify(path)
            except Rejected as reason:
                rejected.append((path, reason))
            else:
                accepted.append((path, method))
    return accepted, rejected


def self_test():
    """Exercise the accept and reject rules on fixture bytes."""
    import contextlib
    import io
    import tempfile

    elf_header = lambda machine: (  # noqa: E731
        ELF_MAGIC
        + bytes([ELF_CLASS_64, 1, 1, 0])
        + b"\0" * 8
        + struct.pack("<H", 3)
        + struct.pack("<H", machine)
    )
    rpm_header = b"\xed\xab\xee\xdb" + b"\0" * 28

    def check(label, name, payload, expect_ok):
        with tempfile.TemporaryDirectory(prefix="signpath-fixture-") as scratch:
            path = Path(scratch) / name
            path.write_bytes(payload)
            try:
                method = classify(path)
            except Rejected:
                if expect_ok:
                    return f"fixture {label}: rejected {name}, expected acceptance"
                return None
            if not expect_ok:
                return f"fixture {label}: accepted {name} as {method}, expected a rejection"
            return None

    failures = []
    cases = [
        ("rpm", "loams-0.0.1-1.x86_64.rpm", rpm_header, True),
        ("elf-x86_64", "loams", elf_header(EM_X86_64), True),
        ("elf-aarch64", "loams", elf_header(EM_AARCH64), True),
        ("elf-other-machine", "loams", elf_header(EM_386), False),
        ("truncated", "loams", ELF_MAGIC[:8], False),
        ("not-elf", "loams", b"\x02\x01\x00\x00" + b"\0" * 32, False),
        ("rpm-without-magic", "loams.rpm", b"not an rpm", False),
        ("windows-exe", "loams.exe", elf_header(EM_X86_64), False),
        ("windows-msi", "setup.msi", b"\x78\x00\x00\x00", False),
        ("macos-dmg", "loams.dmg", b"koly", False),
        ("macos-pkg", "loams.pkg", b"xar!", False),
        ("bare-zip", "loams.tar.gz", b"\x1f\x8b", False),
    ]
    for case in cases:
        failure = check(*case)
        if failure:
            failures.append(failure)

    # The directory-level rules: a mixed directory keeps the Linux files and
    # reports the rest, and `--strict` is what turns a non-Linux file into a
    # failure. Exercised through main() so the argument handling is covered too.
    with tempfile.TemporaryDirectory(prefix="signpath-scan-") as scratch:
        mixed = Path(scratch) / "release-assets"
        mixed.mkdir()
        (mixed / "loams").write_bytes(elf_header(EM_X86_64))
        (mixed / "loams-0.0.1-1.x86_64.rpm").write_bytes(rpm_header)
        (mixed / "loams.exe").write_bytes(b"MZ")
        (mixed / "loams.dmg").write_bytes(b"koly")

        def run_scan(strict, directory):
            argv = ["--strict", str(directory)] if strict else [str(directory)]
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer), contextlib.redirect_stderr(io.StringIO()):
                code = main(argv)
            return code, buffer.getvalue()

        code, out = run_scan(False, mixed)
        if code != 0:
            failures.append("scan: a mixed release directory must still succeed")
        if sorted(line.split("\t")[0] for line in out.splitlines()) != ["linux-elf-x86_64", "rpm-sign"]:
            failures.append(f"scan: mixed directory accepted the wrong set:\n{out}")
        code, _ = run_scan(True, mixed)
        if code == 0:
            failures.append("scan: --strict must fail on a release directory holding Windows and macOS artifacts")

        empty = Path(scratch) / "empty"
        empty.mkdir()
        code, _ = run_scan(False, empty)
        if code == 0:
            failures.append("scan: an empty directory must fail, so no empty request is submitted")
        code, _ = run_scan(False, Path(scratch) / "does-not-exist")
        if code == 0:
            failures.append("scan: a missing directory must fail")

    if failures:
        return "\n".join(failures)
    print(SELF_TEST_NOTE)
    return None


def main(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("directories", nargs="*", help="directories holding the unsigned artifacts")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="also fail when anything was rejected, not only when nothing was accepted",
    )
    parser.add_argument("--self-test", action="store_true", help="run the fixtures and exit")
    args = parser.parse_args(argv)

    if args.self_test:
        failure = self_test()
        if failure:
            print(failure, file=sys.stderr)
            return 1
        return 0

    if not args.directories:
        print("signpath-artifacts: no artifact directory given", file=sys.stderr)
        return 1

    accepted, rejected = scan(args.directories)
    for path, method in accepted:
        print(f"{method}\t{path}")
    for path, reason in rejected:
        print(f"signpath-artifacts: skipping {path}: {reason}", file=sys.stderr)
    if not accepted:
        print(
            "signpath-artifacts: no artifact was accepted, refusing to submit an empty signing request",
            file=sys.stderr,
        )
        return 1
    if rejected and args.strict:
        print(
            f"signpath-artifacts: --strict, and {len(rejected)} artifact(s) are outside the "
            "Linux signing scope",
            file=sys.stderr,
        )
        return 1
    print(
        f"signpath-artifacts: {len(accepted)} Linux artifact(s) accepted for signing"
        + (f", {len(rejected)} outside scope and skipped" if rejected else ""),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]) or 0)