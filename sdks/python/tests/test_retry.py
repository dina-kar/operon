"""Ruling 5: which failures are retried, and how long the client waits."""

from __future__ import annotations

import json
from collections.abc import Callable

import httpx
import pytest

import operon
from operon import _wire

Step = httpx.Response | Exception

_STREAM = {
    "id": 1,
    "partitions": [{"partition": 0, "log_start_offset": 0, "high_watermark": 0}],
    "retention": {"max_age_ms": None, "max_bytes": None},
}
_PRODUCED = {
    "base_offset": 0,
    "last_offset": 0,
    "token": [{"stream": 1, "partition": 0, "offset": 0}],
}


def _ok(body: object) -> httpx.Response:
    return httpx.Response(200, json=body)


def _unavailable(retry_after: str | None = None) -> httpx.Response:
    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    return httpx.Response(
        503, json={"error": "unavailable", "message": "try again"}, headers=headers
    )


class Script:
    """A MockTransport handler answering from a list; it records every request."""

    def __init__(self, *steps: Step) -> None:
        self.steps = list(steps)
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        step = self.steps.pop(0)
        if isinstance(step, Exception):
            raise step
        return step


def _client(script: Script, delays: list[float], max_retries: int = 3) -> operon.Client:
    return operon.Client(
        "http://operon.test",
        transport=httpx.MockTransport(script),
        retry_sleep=delays.append,
        retry_random=lambda: 1.0,
        max_retries=max_retries,
    )


def _record() -> list[operon.Record]:
    return [operon.Record(value=b"v")]


def test_a_503_is_retried_then_succeeds() -> None:
    script = Script(_unavailable(), _unavailable(), _ok(_STREAM))
    delays: list[float] = []
    with _client(script, delays) as client:
        info = client.namespace("n").get_stream("s")
    assert info.id == 1
    assert len(script.requests) == 3
    assert delays == [0.1, 0.2]


def test_backoff_is_capped_and_jittered() -> None:
    script = Script(*[_unavailable() for _ in range(6)], _ok(_STREAM))
    delays: list[float] = []
    with operon.Client(
        "http://operon.test",
        transport=httpx.MockTransport(script),
        max_retries=6,
        retry_sleep=delays.append,
        retry_random=lambda: 0.0,
    ) as client:
        client.namespace("n").get_stream("s")
    assert delays == [0.05, 0.1, 0.2, 0.4, 0.8, 1.0]


def test_retries_stop_after_max_retries() -> None:
    script = Script(*[_unavailable() for _ in range(4)])
    delays: list[float] = []
    with (
        _client(script, delays, max_retries=3) as client,
        pytest.raises(operon.UnavailableError),
    ):
        client.namespace("n").get_stream("s")
    assert len(script.requests) == 4
    assert len(delays) == 3


def test_retry_after_is_honoured() -> None:
    script = Script(_unavailable("2"), _ok(_STREAM))
    delays: list[float] = []
    with _client(script, delays) as client:
        client.namespace("n").get_stream("s")
    assert delays == [2.0]


def test_a_retry_after_over_30_seconds_is_not_waited_for() -> None:
    script = Script(_unavailable("31"), _ok(_STREAM))
    delays: list[float] = []
    with _client(script, delays) as client, pytest.raises(operon.UnavailableError):
        client.namespace("n").get_stream("s")
    assert len(script.requests) == 1
    assert delays == []


def test_produce_is_never_retried() -> None:
    script = Script(_unavailable(), _ok(_PRODUCED))
    delays: list[float] = []
    with _client(script, delays) as client, pytest.raises(operon.UnavailableError):
        client.namespace("n").produce("s", 0, _record())
    assert len(script.requests) == 1


def test_a_503_on_a_collection_write_is_retried() -> None:
    written = {"token": "v1:s1/p0@1", "results": ["accepted"], "positions": [None]}
    script = Script(_unavailable(), _ok(written))
    delays: list[float] = []
    request = _wire.Request(
        "POST",
        "/v1/namespaces/n/collections/c/documents",
        json_body={"ops": [{"delete": {"id": 1}}], "report_existence": False},
        idempotent=True,
    )
    with _client(script, delays) as client:
        response = client._transport.send(request)
    assert response.json() == written
    assert len(script.requests) == 2
    assert json.loads(script.requests[1].content) == request.json_body


def test_a_connect_error_is_retried_for_produce_too() -> None:
    script = Script(httpx.ConnectError("refused"), _ok(_PRODUCED))
    delays: list[float] = []
    with _client(script, delays) as client:
        result = client.namespace("n").produce("s", 0, _record())
    assert result.last_offset == 0
    assert len(script.requests) == 2


def test_a_read_timeout_on_produce_is_not_retried() -> None:
    script = Script(httpx.ReadTimeout("slow"), _ok(_PRODUCED))
    delays: list[float] = []
    with _client(script, delays) as client, pytest.raises(operon.TransportError) as info:
        client.namespace("n").produce("s", 0, _record())
    assert len(script.requests) == 1
    assert info.value.code == "transport"
    assert info.value.status == 0
    assert isinstance(info.value.__cause__, httpx.ReadTimeout)


@pytest.mark.parametrize(
    "make",
    [
        lambda: httpx.ReadTimeout("slow"),
        lambda: httpx.ReadError("reset"),
        lambda: httpx.RemoteProtocolError("closed"),
        lambda: httpx.WriteError("broken"),
        lambda: httpx.ConnectTimeout("slow"),
    ],
)
def test_failures_after_sending_are_retried_on_idempotent_requests(
    make: Callable[[], Exception],
) -> None:
    script = Script(make(), _ok(_STREAM))
    delays: list[float] = []
    with _client(script, delays) as client:
        client.namespace("n").get_stream("s")
    assert len(script.requests) == 2


def test_the_last_transport_failure_becomes_a_transport_error() -> None:
    script = Script(*[httpx.ConnectError("refused") for _ in range(4)])
    delays: list[float] = []
    with _client(script, delays) as client, pytest.raises(operon.TransportError) as info:
        client.namespace("n").get_stream("s")
    assert len(script.requests) == 4
    assert isinstance(info.value.__cause__, httpx.ConnectError)


def test_a_4xx_is_not_retried() -> None:
    missing = httpx.Response(404, json={"error": "not_found", "message": "no stream"})
    script = Script(missing, _ok(_STREAM))
    delays: list[float] = []
    with _client(script, delays) as client, pytest.raises(operon.NotFoundError):
        client.namespace("n").get_stream("s")
    assert len(script.requests) == 1


@pytest.mark.anyio
async def test_async_client_retries_the_same_way() -> None:
    script = Script(_unavailable(), _unavailable(), _ok(_STREAM))
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    async with operon.AsyncClient(
        "http://operon.test",
        transport=httpx.MockTransport(script),
        retry_sleep=sleep,
        retry_random=lambda: 1.0,
    ) as client:
        info = await client.namespace("n").get_stream("s")
    assert info.id == 1
    assert len(script.requests) == 3
    assert delays == [0.1, 0.2]


@pytest.mark.anyio
async def test_async_produce_is_never_retried() -> None:
    script = Script(_unavailable(), _ok(_PRODUCED))

    async def sleep(delay: float) -> None:
        raise AssertionError("no retry expected")

    async with operon.AsyncClient(
        "http://operon.test", transport=httpx.MockTransport(script), retry_sleep=sleep
    ) as client:
        with pytest.raises(operon.UnavailableError):
            await client.namespace("n").produce("s", 0, _record())
    assert len(script.requests) == 1
