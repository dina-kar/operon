"""The plan's "verify" items, checked against an Elasticsearch 8.19 oracle
(plan M1.5 Task 11, owner ruling O-M15-6).

Runs only when `ES_ORACLE_URL` names an Elasticsearch 8.19 (single node,
security off), used only as a test oracle. Each probe of `oracle.PROBES`
sends the same requests to Loams and to the oracle and compares what the
plan marks "verify". A probe with a recorded deviation is an expected
failure (strict: once Loams matches, the mark must go).
"""

from __future__ import annotations

import os

import pytest

import oracle

ORACLE = os.environ.get("ES_ORACLE_URL")

pytestmark = pytest.mark.skipif(not ORACLE, reason="ES_ORACLE_URL is not set")


@pytest.mark.parametrize("probe", oracle.PROBES, ids=[p.id for p in oracle.PROBES])
def test_loams_answers_like_the_oracle(loams, is_loams, probe):
    if not is_loams:
        pytest.skip("the server under test is not Loams")
    ours = oracle.run_probe(loams, probe)
    theirs = oracle.run_probe(ORACLE, probe)
    if probe.deviation:
        if ours == theirs:
            pytest.fail(f"{probe.id} now matches the oracle; drop its deviation ({probe.deviation})")
        pytest.xfail(probe.deviation)
    assert ours == theirs, f"{probe.item}\n loams: {ours}\n oracle: {theirs}"
