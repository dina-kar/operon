"""The one module that knows the native REST API's wire JSON (plan M1.6, rows W1-W5).

Everything here is pure: request builders return a `Request`, parsers take a
`Response`. `Client` and `AsyncClient` share it, so their behaviour cannot drift.
"""

from __future__ import annotations

import base64
import json
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, TypeAlias
from urllib.parse import quote

from ._version import __version__
from .errors import OperonError, error_from_body
from .token import ConsistencyToken
from .types import FetchedRecord, FetchResult, PartitionInfo, ProduceResult, Record, StreamInfo

TOKEN_HEADER = "Operon-Consistency-Token"
USER_AGENT = f"operon-client-python/{__version__}"

_U64_LIMIT = 2**64

Json: TypeAlias = Any
"""A decoded JSON value."""


@dataclass(frozen=True, slots=True)
class Request:
    """One API call, before it is sent."""

    method: str
    path: str
    query: Mapping[str, str | int] = field(default_factory=dict)
    headers: Mapping[str, str] = field(default_factory=dict)
    json_body: Json = None
    idempotent: bool = True
    extra_timeout: float = 0.0
    """Seconds added to the client's timeout for this request (a long poll's wait)."""


@dataclass(frozen=True, slots=True)
class Response:
    """A successful answer: status, headers (lower-cased names) and the raw body."""

    status: int
    headers: Mapping[str, str]
    content: bytes

    def json(self) -> Json:
        return json.loads(self.content)

    def header(self, name: str) -> str | None:
        return self.headers.get(name.lower())


def encode_body(body: Json) -> bytes:
    """Compact JSON; a non-finite float raises `ValueError` before anything is sent."""
    return json.dumps(body, allow_nan=False, separators=(",", ":")).encode("utf-8")


def seg(name: str) -> str:
    """A path segment: every reserved character percent-encoded."""
    return quote(name, safe="")


# ---------------------------------------------------------------- ids


def encode_id(value: object) -> int | str | dict[str, str]:
    """A document id's JSON form: an int in `0..2**64`, a string, or `{"uuid": …}`."""
    if isinstance(value, bool):
        raise TypeError("a bool is not a document id")
    if isinstance(value, int):
        if 0 <= value < _U64_LIMIT:
            return value
        raise ValueError(f"an integer id must be in 0..2**64, got {value}")
    if isinstance(value, str):
        return value
    if isinstance(value, uuid.UUID):
        return {"uuid": str(value)}
    raise TypeError(f"a document id is an int, str or uuid.UUID, not {type(value).__name__}")


def decode_id(value: object) -> int | str | uuid.UUID:
    """The inverse of `encode_id`."""
    if isinstance(value, bool):
        raise ValueError("a bool is not a document id")
    if isinstance(value, int | str):
        return value
    if isinstance(value, Mapping) and set(value) == {"uuid"} and isinstance(value["uuid"], str):
        return uuid.UUID(value["uuid"])
    raise ValueError(f"not a document id: {value!r}")


# ---------------------------------------------------------------- errors


def retry_after_seconds(headers: Mapping[str, str]) -> int | None:
    """`Retry-After` as an integer number of seconds, if it is one."""
    raw = headers.get("retry-after")
    if raw is None:
        return None
    raw = raw.strip()
    if not raw.isdigit():
        return None
    return int(raw)


def error_from_response(status: int, content: bytes) -> OperonError:
    """Maps a non-2xx answer to its typed error."""
    try:
        body = json.loads(content)
    except (ValueError, UnicodeDecodeError):
        body = None
    if isinstance(body, dict) and isinstance(body.get("error"), str):
        return error_from_body(status, body)
    text = content[:800].decode("utf-8", errors="replace")[:200]
    return OperonError(text, code=f"http_{status}", status=status, body={})


# ---------------------------------------------------------------- bytes


def _to_bytes(value: bytes | str) -> bytes:
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bytes | bytearray | memoryview):
        return bytes(value)
    raise TypeError(f"expected bytes or str, not {type(value).__name__}")


def _b64(value: bytes | str) -> str:
    return base64.b64encode(_to_bytes(value)).decode("ascii")


def _unb64(value: object) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"expected a base64 string, got {value!r}")
    return base64.b64decode(value, validate=True)


# ---------------------------------------------------------------- namespaces (W1)


def create_namespace(name: str) -> Request:
    return Request("POST", "/v1/namespaces", json_body={"name": name})


def parse_created_id(response: Response) -> int:
    return int(response.json()["id"])


# ---------------------------------------------------------------- streams (W2-W5)


def _stream_path(ns: str, stream: str) -> str:
    return f"/v1/namespaces/{seg(ns)}/streams/{seg(stream)}"


def create_stream(
    ns: str,
    name: str,
    partitions: int,
    max_age_ms: int | None,
    max_bytes: int | None,
) -> Request:
    body: dict[str, Any] = {"name": name, "partitions": partitions}
    if max_age_ms is not None or max_bytes is not None:
        retention: dict[str, int] = {}
        if max_age_ms is not None:
            retention["max_age_ms"] = max_age_ms
        if max_bytes is not None:
            retention["max_bytes"] = max_bytes
        body["retention"] = retention
    return Request("POST", f"/v1/namespaces/{seg(ns)}/streams", json_body=body)


def get_stream(ns: str, stream: str) -> Request:
    return Request("GET", _stream_path(ns, stream))


def parse_stream_info(response: Response) -> StreamInfo:
    body = response.json()
    retention = body.get("retention") or {}
    return StreamInfo(
        id=int(body["id"]),
        partitions=[
            PartitionInfo(
                partition=int(p["partition"]),
                log_start_offset=int(p["log_start_offset"]),
                high_watermark=int(p["high_watermark"]),
            )
            for p in body.get("partitions", [])
        ],
        max_age_ms=retention.get("max_age_ms"),
        max_bytes=retention.get("max_bytes"),
    )


def _encode_record(record: Record) -> dict[str, Any]:
    if not isinstance(record, Record):
        raise TypeError(f"expected operon.Record, not {type(record).__name__}")
    out: dict[str, Any] = {}
    if record.key is not None:
        out["key"] = _b64(record.key)
    if record.value is not None:
        out["value"] = _b64(record.value)
    if record.headers:
        headers: list[dict[str, str]] = []
        for key, value in record.headers:
            header: dict[str, str] = {"key": key}
            if value is not None:
                header["value"] = _b64(value)
            headers.append(header)
        out["headers"] = headers
    if record.timestamp_ms is not None:
        out["timestamp_ms"] = record.timestamp_ms
    return out


def produce(ns: str, stream: str, partition: int, records: Sequence[Record]) -> Request:
    """W4. Never retried automatically: a 503 may already have been committed (Ruling 5)."""
    return Request(
        "POST",
        f"{_stream_path(ns, stream)}/partitions/{int(partition)}/records",
        json_body={"records": [_encode_record(r) for r in records]},
        idempotent=False,
    )


def token_from_header(response: Response) -> ConsistencyToken | None:
    raw = response.header(TOKEN_HEADER)
    return ConsistencyToken.parse(raw) if raw is not None else None


def parse_produce(response: Response) -> ProduceResult:
    body = response.json()
    token = token_from_header(response)
    if token is None:
        # The body's offsets are the last written; a token names the next one.
        token = ConsistencyToken(
            tuple(
                sorted(
                    (int(t["stream"]), int(t["partition"]), int(t["offset"]) + 1)
                    for t in body.get("token", [])
                )
            )
        )
    return ProduceResult(
        base_offset=int(body["base_offset"]),
        last_offset=int(body["last_offset"]),
        token=token,
    )


def fetch(
    ns: str,
    stream: str,
    partition: int,
    offset: int,
    max_bytes: int | None,
    max_wait_ms: int | None,
) -> Request:
    query: dict[str, str | int] = {"offset": int(offset)}
    if max_bytes is not None:
        query["max_bytes"] = int(max_bytes)
    if max_wait_ms is not None:
        query["max_wait_ms"] = int(max_wait_ms)
    return Request(
        "GET",
        f"{_stream_path(ns, stream)}/partitions/{int(partition)}/records",
        query=query,
        extra_timeout=(max_wait_ms or 0) / 1000,
    )


def parse_fetch(response: Response) -> FetchResult:
    body = response.json()
    return FetchResult(
        records=[
            FetchedRecord(
                offset=int(r["offset"]),
                key=_unb64(r.get("key")),
                value=_unb64(r.get("value")),
                headers=[(str(h["key"]), _unb64(h.get("value"))) for h in r.get("headers", [])],
                timestamp_ms=int(r["timestamp_ms"]),
            )
            for r in body.get("records", [])
        ],
        next_offset=int(body["next_offset"]),
        high_watermark=int(body["high_watermark"]),
        log_start_offset=int(body["log_start_offset"]),
    )
