"""Shared fixtures: a spawned `operon dev` (plan M1.6 Task 2 rule 10, rows E13 and E21)."""

from __future__ import annotations

import os
import re
import signal
import subprocess
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

import operon

REPO = Path(__file__).resolve().parents[3]
LISTENING = re.compile(r"operon listening on (http://\S+)")
ANSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
STARTUP_TIMEOUT_S = 60.0


def _binary() -> Path:
    binary = Path(os.environ.get("OPERON_BIN", REPO / "target" / "debug" / "operon"))
    if not binary.is_file():
        pytest.fail(f"no operon binary at {binary}; build it with: cargo build -p operon")
    return binary


def _read_until_listening(proc: subprocess.Popen[str]) -> str:
    """Reads stdout lines until the native listener's line; tracing logs are interleaved."""
    assert proc.stdout is not None
    seen: list[str] = []
    found: list[str] = []

    def read() -> None:
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = ANSI.sub("", raw.rstrip("\n"))
            seen.append(line)
            match = LISTENING.search(line)
            if match:
                found.append(match.group(1))
                return

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    reader.join(STARTUP_TIMEOUT_S)
    if not found:
        proc.kill()
        pytest.fail(f"operon dev did not print its address within 60 s: {seen[-20:]}")
    # Keep draining stdout so the child never blocks on a full pipe.
    threading.Thread(target=lambda: [None for _ in proc.stdout or ()], daemon=True).start()
    return found[0]


@pytest.fixture(scope="session")
def operon_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """The base URL of an `operon dev` child on an ephemeral port."""
    data = tmp_path_factory.mktemp("operon-data")
    proc = subprocess.Popen(
        [
            str(_binary()),
            "dev",
            "--listen",
            "127.0.0.1:0",
            "--flush-interval-ms",
            "20",
            "--data-dir",
            str(data),
            "--no-qdrant",
            "--no-es",
            "--no-flight-sql",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env={**os.environ, "RUST_LOG": "warn"},
    )
    try:
        yield _read_until_listening(proc)
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


@pytest.fixture
def ns_name(operon_url: str) -> str:
    """A fresh namespace, already created."""
    name = "t-" + uuid.uuid4().hex[:12]
    with operon.Client(operon_url) as client:
        client.create_namespace(name)
    return name


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"
