"""Shared fixtures: a spawned `operon dev` (M1.6 Task 2 rule 10, Task 4 rule 6, E13, E21)."""

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
from operon import schema

REPO = Path(__file__).resolve().parents[3]
LISTENING = re.compile(r"operon listening on (http://\S+)")
FLIGHT = re.compile(r"operon flight sql listening on (grpc://\S+)")
ANSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")
STARTUP_TIMEOUT_S = 60.0


def _binary() -> Path:
    binary = Path(os.environ.get("OPERON_BIN", REPO / "target" / "debug" / "operon"))
    if not binary.is_file():
        pytest.fail(f"no operon binary at {binary}; build it with: cargo build -p operon")
    return binary


def _read_until_listening(proc: subprocess.Popen[str]) -> tuple[str, str]:
    """Reads stdout lines until the native and then the Flight SQL listener's line.

    Tracing logs are interleaved; the Flight line comes after the HTTP one (E13).
    """
    assert proc.stdout is not None
    seen: list[str] = []
    found: dict[str, str] = {}

    def read() -> None:
        assert proc.stdout is not None
        for raw in proc.stdout:
            line = ANSI.sub("", raw.rstrip("\n"))
            seen.append(line)
            for key, pattern in (("http", LISTENING), ("flight", FLIGHT)):
                match = pattern.search(line)
                if match:
                    found[key] = match.group(1)
            if len(found) == 2:
                return

    reader = threading.Thread(target=read, daemon=True)
    reader.start()
    reader.join(STARTUP_TIMEOUT_S)
    if len(found) != 2:
        proc.kill()
        pytest.fail(f"operon dev did not print its addresses within 60 s: {seen[-20:]}")
    # Keep draining stdout so the child never blocks on a full pipe.
    threading.Thread(target=lambda: [None for _ in proc.stdout or ()], daemon=True).start()
    return found["http"], found["flight"]


@pytest.fixture(scope="session")
def operon_server(tmp_path_factory: pytest.TempPathFactory) -> Iterator[tuple[str, str]]:
    """An `operon dev` child on ephemeral ports: its REST base URL and Flight SQL URI."""
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
            "--flight-sql-listen",
            "127.0.0.1:0",
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


@pytest.fixture(scope="session")
def operon_url(operon_server: tuple[str, str]) -> str:
    """The base URL of the spawned server's native REST API."""
    return operon_server[0]


@pytest.fixture(scope="session")
def flight_uri(operon_server: tuple[str, str]) -> str:
    """The spawned server's Flight SQL URI, `grpc://127.0.0.1:<port>`."""
    return operon_server[1]


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


@pytest.fixture
def client(operon_url: str) -> Iterator[operon.Client]:
    """A client of the spawned server."""
    with operon.Client(operon_url) as c:
        yield c


@pytest.fixture
def ns(client: operon.Client, ns_name: str) -> operon.Namespace:
    """The fresh namespace as a handle."""
    return client.namespace(ns_name)


def kb_schema() -> operon.Schema:
    """The fixture's `kb` collection (scenario.json step 6)."""
    return operon.Schema(
        fields=[schema.text("body"), schema.keyword("tenant"), schema.i64("n")],
        vectors=[schema.vector("embedding", 3)],
        dynamic="ignore",
    )


def kb_docs() -> list[operon.Document]:
    """The fixture's first three documents (scenario.json step 10)."""
    return [
        operon.Document(
            1, {"body": "refund policy", "tenant": "a", "n": 1}, {"embedding": [1.0, 0.0, 0.0]}
        ),
        operon.Document(
            2, {"body": "shipping times", "tenant": "a", "n": 2}, {"embedding": [0.9, 0.1, 0.0]}
        ),
        operon.Document(
            3, {"body": "refund window", "tenant": "b", "n": 3}, {"embedding": [0.0, 0.0, 1.0]}
        ),
    ]


@pytest.fixture
def kb(ns: operon.Namespace) -> operon.Collection:
    """`kb` with its three documents written."""
    ns.create_collection("kb", kb_schema(), partitions=2)
    collection = ns.collection("kb")
    collection.upsert(kb_docs())
    return collection
