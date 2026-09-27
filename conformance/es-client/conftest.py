"""Fixtures of the elasticsearch-py 8.19 client suite (plan M1.5 Task 11).

The session fixture `operon` starts `operon dev` with the Elasticsearch
gateway on an ephemeral port and yields its URL. With `OPERON_ES_URL` set,
the suite runs against that server instead (for example a running Operon,
or the Elasticsearch 8.19 oracle, to check the expectations themselves);
checks of Operon-only answers are skipped against a real Elasticsearch.

`ES_ORACLE_URL`, when set, names an Elasticsearch 8.19 used by
`test_oracle.py` only as a test oracle (owner ruling O-M15-6).
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import tempfile
import threading
import uuid

import pytest
from elasticsearch import Elasticsearch

LISTENING = re.compile(r"^operon es listening on (http://\S+)$")


def _start_operon():
    binary = os.environ.get("OPERON_BIN", "target/debug/operon")
    data = tempfile.mkdtemp(prefix="operon-es-client-")
    proc = subprocess.Popen(
        [
            binary,
            "dev",
            "--data-dir",
            data,
            "--listen",
            "127.0.0.1:0",
            "--es-listen",
            "127.0.0.1:0",
            "--flush-interval-ms",
            "20",
            "--no-qdrant",
            "--no-flight-sql",
            "--no-mcp",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        env={**os.environ, "RUST_LOG": os.environ.get("RUST_LOG", "warn")},
    )
    url = None
    seen = []
    for line in proc.stdout:
        line = line.rstrip("\n")
        seen.append(line)
        match = LISTENING.match(line)
        if match:
            url = match.group(1)
        if line.startswith("operon listening on "):
            break
    if url is None:
        proc.kill()
        raise RuntimeError(f"operon dev did not print its ES address: {seen}")
    # Keep draining stdout so the process never blocks on a full pipe.
    threading.Thread(target=lambda: [None for _ in proc.stdout], daemon=True).start()
    return proc, data, url


@pytest.fixture(scope="session")
def operon():
    """The URL of the Elasticsearch API under test."""
    url = os.environ.get("OPERON_ES_URL")
    if url:
        yield url
        return
    proc, data, url = _start_operon()
    try:
        yield url
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=20)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        shutil.rmtree(data, ignore_errors=True)


@pytest.fixture(scope="session")
def is_operon(operon):
    """Whether the server under test is Operon (and not a real ES)."""
    client = Elasticsearch(operon, request_timeout=30)
    try:
        return client.info()["name"] == "operon"
    finally:
        client.close()


@pytest.fixture
def es(operon):
    client = Elasticsearch(operon, request_timeout=30)
    yield client
    client.close()


def _drop(client, name):
    client.options(ignore_status=404).indices.delete(index=name)


@pytest.fixture
def index(es):
    """A fresh index name, dropped after the test."""
    name = f"test_{uuid.uuid4().hex}"
    yield name
    _drop(es, name)


@pytest.fixture
def names(es):
    """A factory of fresh index names, every one dropped after the test."""
    made = []

    def make(prefix="test"):
        name = f"{prefix}_{uuid.uuid4().hex}"
        made.append(name)
        return name

    yield make
    for name in made:
        _drop(es, name)
