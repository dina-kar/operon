"""SQL over the native API (W13)."""

from __future__ import annotations

import json

import httpx
import pytest

import operon
from operon import _wire


def test_sql_select_returns_rows(kb: operon.Collection, ns: operon.Namespace) -> None:
    result = ns.sql("SELECT count(*) AS n FROM kb")
    assert result.columns[0].name == "n"
    assert result.columns[0].type == "Int64"
    assert result.rows == [[3]]
    assert result.to_dicts() == [{"n": 3}]
    assert result.truncated is False


def test_sql_error_raises_invalid_argument(ns: operon.Namespace) -> None:
    with pytest.raises(operon.InvalidArgumentError):
        ns.sql("SELEC 1")


def test_eventual_is_sent_in_the_body_and_tokens_in_the_header() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"columns": [], "rows": [], "truncated": False})

    token = "v1:s1/p0@9"
    pin = operon.Pin(3, operon.ConsistencyToken.parse("v1:s1/p0@8"))
    with operon.Client("http://operon.test", transport=httpx.MockTransport(handler)) as client:
        ns = client.namespace("n")
        ns.sql("SELECT 1")
        ns.sql("SELECT 1", consistency="eventual")
        ns.sql("SELECT 1", consistency=token)
        ns.sql("SELECT 1", consistency=operon.ConsistencyToken.parse(token))
        ns.sql("SELECT 1", consistency=pin)
    bodies = [json.loads(r.content) for r in requests]
    headers = [r.headers.get(_wire.TOKEN_HEADER) for r in requests]
    assert bodies == [
        {"query": "SELECT 1"},
        {"query": "SELECT 1", "consistency": "eventual"},
        {"query": "SELECT 1"},
        {"query": "SELECT 1"},
        {
            "query": "SELECT 1",
            "consistency": {"pinned": {"manifest_version": 3, "token": "v1:s1/p0@8"}},
        },
    ]
    assert headers == [None, None, token, token, None]
    assert all(r.url.path == "/v1/namespaces/n/sql" for r in requests)
