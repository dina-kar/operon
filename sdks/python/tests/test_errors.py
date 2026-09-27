from __future__ import annotations

import json

import httpx
import pytest

import operon
from operon import (
    AlreadyExistsError,
    ConflictError,
    InternalError,
    InvalidArgumentError,
    NotFoundError,
    OffsetOutOfRangeError,
    OperonError,
    OperonTimeoutError,
    ResourceExhaustedError,
    SchemaViolationError,
    UnavailableError,
)


def _client(status: int, body: object, *, content_type: str = "application/json") -> operon.Client:
    content = body.encode() if isinstance(body, str) else json.dumps(body).encode()

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=content, headers={"Content-Type": content_type})

    # No retries: a 503 must surface on the first answer here.
    return operon.Client(
        "http://operon.test", transport=httpx.MockTransport(handler), max_retries=0
    )


_EXTRAS = {"offset": 5, "log_start_offset": 0, "high_watermark": 1}


@pytest.mark.parametrize(
    ("code", "status", "cls"),
    [
        ("invalid_argument", 400, InvalidArgumentError),
        ("schema_violation", 400, SchemaViolationError),
        ("not_found", 404, NotFoundError),
        ("already_exists", 409, AlreadyExistsError),
        ("conflict", 409, ConflictError),
        ("offset_out_of_range", 416, OffsetOutOfRangeError),
        ("resource_exhausted", 429, ResourceExhaustedError),
        ("internal", 500, InternalError),
        ("unavailable", 503, UnavailableError),
        ("timeout", 504, OperonTimeoutError),
    ],
)
def test_error_codes_map_to_typed_errors(code: str, status: int, cls: type[OperonError]) -> None:
    body = {"error": code, "message": f"the {code} message", **_EXTRAS}
    with _client(status, body) as client, pytest.raises(cls) as info:
        client.namespace("n").get_stream("s")
    err = info.value
    assert type(err) is cls
    assert err.code == code
    assert err.status == status
    assert err.message == f"the {code} message"
    assert err.body["error"] == code
    assert err.retryable is (status == 503)


def test_error_extras_are_attributes() -> None:
    body = {"error": "offset_out_of_range", "message": "m", **_EXTRAS}
    with _client(416, body) as client, pytest.raises(OffsetOutOfRangeError) as range_info:
        client.namespace("n").fetch("s", 0, 5)
    assert (range_info.value.offset, range_info.value.log_start_offset) == (5, 0)
    assert range_info.value.high_watermark == 1

    body = {"error": "schema_violation", "message": "m", "field": "n"}
    with _client(400, body) as client, pytest.raises(SchemaViolationError) as schema_info:
        client.namespace("n").get_stream("s")
    assert schema_info.value.field == "n"
    assert isinstance(schema_info.value, InvalidArgumentError)

    body = {"error": "already_exists", "message": "m", "id": 42}
    with _client(409, body) as client, pytest.raises(AlreadyExistsError) as exists_info:
        client.create_namespace("n")
    assert exists_info.value.id == 42

    body = {"error": "already_exists", "message": "m"}
    with _client(409, body) as client, pytest.raises(AlreadyExistsError) as bare_info:
        client.create_namespace("n", exist_ok=True)
    assert bare_info.value.id is None

    body = {"error": "resource_exhausted", "message": "m", "retry_after_ms": 1500}
    with _client(429, body) as client, pytest.raises(ResourceExhaustedError) as busy_info:
        client.namespace("n").get_stream("s")
    assert busy_info.value.retry_after_ms == 1500


def test_unknown_error_code_keeps_its_code_and_maps_by_status() -> None:
    with (
        _client(503, {"error": "busy", "message": "m"}) as client,
        pytest.raises(UnavailableError) as info,
    ):
        client.namespace("n").get_stream("s")
    assert info.value.code == "busy"
    assert info.value.status == 503


@pytest.mark.parametrize(
    ("status", "cls"),
    [
        (400, InvalidArgumentError),
        (404, NotFoundError),
        (409, ConflictError),
        (416, OperonError),
        (504, OperonTimeoutError),
        (502, InternalError),
        (418, OperonError),
    ],
)
def test_unknown_codes_map_by_status(status: int, cls: type[OperonError]) -> None:
    with (
        _client(status, {"error": "odd", "message": "m"}) as client,
        pytest.raises(OperonError) as info,
    ):
        client.namespace("n").get_stream("s")
    assert type(info.value) is cls
    assert info.value.code == "odd"


def test_non_json_error_body_gives_a_generic_error() -> None:
    page = "<html>" + "x" * 500 + "</html>"
    with (
        _client(502, page, content_type="text/html") as client,
        pytest.raises(OperonError) as info,
    ):
        client.namespace("n").get_stream("s")
    assert type(info.value) is OperonError
    assert info.value.code == "http_502"
    assert info.value.status == 502
    assert info.value.message == page[:200]
