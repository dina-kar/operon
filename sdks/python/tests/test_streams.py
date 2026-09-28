"""Namespaces and streams (W1-W5), against a spawned `operon dev` unless noted."""

from __future__ import annotations

import json
import time
import uuid

import httpx
import pytest

import operon
from operon import _wire


def test_create_namespace_and_stream_then_produce_and_fetch(operon_url: str, ns_name: str) -> None:
    with operon.Client(operon_url) as client:
        ns = client.namespace(ns_name)
        stream_id = ns.create_stream("events", 2)
        records = [
            operon.Record(value="first", key=b"\x00k1", headers=[("h", b"x"), ("empty", None)]),
            operon.Record(value=b"second", timestamp_ms=1_700_000_000_000),
            operon.Record(),
        ]
        produced = ns.produce("events", 1, records)
        assert produced.base_offset == 0
        assert produced.last_offset == 2
        assert produced.token.items == ((stream_id, 1, 3),)

        fetched = ns.fetch("events", 1, 0)
        assert fetched.next_offset == 3
        assert fetched.high_watermark == 3
        assert fetched.log_start_offset == 0
        got = fetched.records
        assert [r.offset for r in got] == [0, 1, 2]
        assert got[0].key == b"\x00k1"
        assert got[0].value == b"first"
        assert got[0].headers == [("h", b"x"), ("empty", None)]
        assert got[1].key is None
        assert got[1].value == b"second"
        assert got[1].timestamp_ms == 1_700_000_000_000
        assert got[1].headers == []
        assert (got[2].key, got[2].value) == (None, None)

        assert ns.fetch("events", 0, 0).records == []


def test_create_namespace_twice_raises_already_exists_unless_exist_ok(operon_url: str) -> None:
    name = "t-" + uuid.uuid4().hex[:12]
    with operon.Client(operon_url) as client:
        first = client.create_namespace(name)
        with pytest.raises(operon.AlreadyExistsError) as info:
            client.create_namespace(name)
        assert info.value.id == first
        assert client.create_namespace(name, exist_ok=True) == first


def test_fetch_above_the_high_watermark_raises_offset_out_of_range(
    operon_url: str, ns_name: str
) -> None:
    with operon.Client(operon_url) as client:
        ns = client.namespace(ns_name)
        ns.create_stream("events", 1)
        with pytest.raises(operon.OffsetOutOfRangeError) as info:
            ns.fetch("events", 0, 100)
    assert info.value.offset == 100
    assert info.value.high_watermark == 0
    assert info.value.log_start_offset == 0


def test_long_poll_longer_than_the_client_timeout_does_not_time_out(
    operon_url: str, ns_name: str
) -> None:
    with operon.Client(operon_url, timeout=1.0) as client:
        ns = client.namespace(ns_name)
        ns.create_stream("events", 1)
        started = time.monotonic()
        result = ns.fetch("events", 0, 0, max_wait_ms=2000)
        elapsed = time.monotonic() - started
    assert result.records == []
    assert elapsed >= 1.9


def test_get_stream_describes_partitions(operon_url: str, ns_name: str) -> None:
    with operon.Client(operon_url) as client:
        ns = client.namespace(ns_name)
        stream_id = ns.create_stream("events", 3, max_age_ms=3_600_000, max_bytes=1 << 30)
        ns.produce("events", 2, [operon.Record(value=b"a"), operon.Record(value=b"b")])
        info = ns.get_stream("events")
    assert info.id == stream_id
    assert [p.partition for p in info.partitions] == [0, 1, 2]
    assert [p.high_watermark for p in info.partitions] == [0, 0, 2]
    assert all(p.log_start_offset == 0 for p in info.partitions)
    assert info.max_age_ms == 3_600_000
    assert info.max_bytes == 1 << 30


def test_a_missing_stream_is_not_found(operon_url: str, ns_name: str) -> None:
    with operon.Client(operon_url) as client, pytest.raises(operon.NotFoundError):
        client.namespace(ns_name).get_stream("nope")


def test_bool_is_not_an_id() -> None:
    with pytest.raises(TypeError):
        _wire.encode_id(True)
    with pytest.raises(ValueError, match="2\\*\\*64"):
        _wire.encode_id(2**64)
    with pytest.raises(ValueError, match="2\\*\\*64"):
        _wire.encode_id(-1)
    with pytest.raises(TypeError):
        _wire.encode_id(1.0)


def test_ids_round_trip() -> None:
    u = uuid.UUID("0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e")
    for value, wire in [
        (2**64 - 1, 2**64 - 1),
        (0, 0),
        ("0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e", "0190f5c4-6c1e-7b3a-9d2e-4f5a6b7c8d9e"),
        (u, {"uuid": str(u)}),
    ]:
        assert _wire.encode_id(value) == wire
        assert _wire.decode_id(wire) == value
    with pytest.raises(ValueError, match="bool"):
        _wire.decode_id(False)


def test_requests_are_encoded_as_the_wire_contract_says() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(
            200,
            json={
                "base_offset": 4,
                "last_offset": 4,
                "token": [{"stream": 9, "partition": 0, "offset": 4}],
            },
        )

    with operon.Client(
        "http://operon.test", transport=httpx.MockTransport(handler), headers={"X-Extra": "1"}
    ) as client:
        result = client.namespace("a/b c").produce(
            "é?", 0, [operon.Record(value="v", headers=[("h", None)])]
        )
    request = seen[0]
    assert (
        request.url.raw_path == b"/v1/namespaces/a%2Fb%20c/streams/%C3%A9%3F/partitions/0/records"
    )
    assert request.headers["User-Agent"] == "operon-client-python/0.0.1"
    assert request.headers["X-Extra"] == "1"
    assert request.headers["Content-Type"] == "application/json"
    assert request.content == b'{"records":[{"value":"dg==","headers":[{"key":"h"}]}]}'
    # Without the header the token is built from the body: next offset = last + 1.
    assert str(result.token) == "v1:s9/p0@5"


def test_the_token_header_wins_over_the_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "base_offset": 0,
                "last_offset": 0,
                "token": [{"stream": 1, "partition": 0, "offset": 0}],
            },
            headers={"Operon-Consistency-Token": "v1:s1/p0@7"},
        )

    with operon.Client("http://operon.test", transport=httpx.MockTransport(handler)) as client:
        result = client.namespace("n").produce("s", 0, [operon.Record(value=b"")])
    assert str(result.token) == "v1:s1/p0@7"


def test_a_nan_in_a_body_is_refused_before_sending() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("nothing may be sent")

    with operon.Client("http://operon.test", transport=httpx.MockTransport(handler)) as client:
        request = _wire.Request("POST", "/x", json_body={"v": [float("nan")]})
        with pytest.raises(ValueError, match="JSON"):
            client._transport.send(request)


def test_fetch_extends_the_timeout_by_the_long_poll_wait() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        body = {"records": [], "next_offset": 0, "high_watermark": 0, "log_start_offset": 0}
        return httpx.Response(200, content=json.dumps(body).encode())

    with operon.Client(
        "http://operon.test", timeout=1.0, transport=httpx.MockTransport(handler)
    ) as client:
        client.namespace("n").fetch("s", 0, 3, max_bytes=10, max_wait_ms=2500)
    assert seen[0].url.params == httpx.QueryParams(
        {"offset": "3", "max_bytes": "10", "max_wait_ms": "2500"}
    )
    assert seen[0].extensions["timeout"]["read"] == pytest.approx(3.5)


@pytest.mark.anyio
async def test_async_client_produces_and_fetches(operon_url: str, ns_name: str) -> None:
    async with operon.AsyncClient(operon_url) as client:
        ns = client.namespace(ns_name)
        stream_id = await ns.create_stream("events", 1)
        produced = await ns.produce("events", 0, [operon.Record(value="a", key="k")])
        assert produced.token.items == ((stream_id, 0, 1),)
        fetched = await ns.fetch("events", 0, 0)
        assert [(r.key, r.value) for r in fetched.records] == [(b"k", b"a")]
        info = await ns.get_stream("events")
        assert info.partitions[0].high_watermark == 1
        name = "t-" + uuid.uuid4().hex[:12]
        created = await client.create_namespace(name)
        assert await client.create_namespace(name, exist_ok=True) == created
