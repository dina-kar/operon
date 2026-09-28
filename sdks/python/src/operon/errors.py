"""Typed errors mapped from the native API's error body `{"error": code, "message": text, …}`."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

__all__ = [
    "AlreadyExistsError",
    "ConflictError",
    "InternalError",
    "InvalidArgumentError",
    "NotFoundError",
    "OffsetOutOfRangeError",
    "OperonError",
    "OperonTimeoutError",
    "ResourceExhaustedError",
    "SchemaViolationError",
    "TransportError",
    "UnavailableError",
]


def _int_or_none(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


class OperonError(Exception):
    """An error answered by the server (or, for `TransportError`, no answer at all)."""

    code: str
    status: int
    message: str
    body: Mapping[str, Any]

    def __init__(
        self,
        message: str,
        *,
        code: str,
        status: int,
        body: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status
        self.message = message
        self.body = dict(body) if body is not None else {}

    @property
    def retryable(self) -> bool:
        """Whether the same request may succeed when sent again (a 503)."""
        return self.status == 503

    def __str__(self) -> str:
        return f"{self.code} ({self.status}): {self.message}"


class InvalidArgumentError(OperonError):
    """400 `invalid_argument`."""


class SchemaViolationError(InvalidArgumentError):
    """400 `schema_violation`; `field` names the offending field when the server says."""

    field: str | None

    def __init__(
        self,
        message: str,
        *,
        code: str,
        status: int,
        body: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code=code, status=status, body=body)
        field = self.body.get("field")
        self.field = field if isinstance(field, str) else None


class NotFoundError(OperonError):
    """404 `not_found`."""


class AlreadyExistsError(OperonError):
    """409 `already_exists`; `id` is set on namespace, stream and link creates only."""

    id: int | None

    def __init__(
        self,
        message: str,
        *,
        code: str,
        status: int,
        body: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code=code, status=status, body=body)
        self.id = _int_or_none(self.body.get("id"))


class ConflictError(OperonError):
    """409 `conflict`."""


class OffsetOutOfRangeError(OperonError):
    """416 `offset_out_of_range`, with the partition's bounds."""

    offset: int
    log_start_offset: int
    high_watermark: int

    def __init__(
        self,
        message: str,
        *,
        code: str,
        status: int,
        body: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code=code, status=status, body=body)
        extras = _offset_extras(self.body)
        if extras is None:
            raise ValueError("offset_out_of_range needs offset, log_start_offset, high_watermark")
        self.offset, self.log_start_offset, self.high_watermark = extras


def _offset_extras(body: Mapping[str, Any]) -> tuple[int, int, int] | None:
    values = [_int_or_none(body.get(k)) for k in ("offset", "log_start_offset", "high_watermark")]
    if any(v is None for v in values):
        return None
    offset, start, high = values
    assert offset is not None
    assert start is not None
    assert high is not None
    return offset, start, high


class ResourceExhaustedError(OperonError):
    """429 `resource_exhausted`; `retry_after_ms` is the server's suggested wait."""

    retry_after_ms: int | None

    def __init__(
        self,
        message: str,
        *,
        code: str,
        status: int,
        body: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message, code=code, status=status, body=body)
        self.retry_after_ms = _int_or_none(self.body.get("retry_after_ms"))


class UnavailableError(OperonError):
    """503 `unavailable`: retryable (the outcome of a write may be unknown)."""


class OperonTimeoutError(OperonError):
    """504 `timeout`."""


class InternalError(OperonError):
    """500 `internal` and other 5xx answers."""


class TransportError(OperonError):
    """No usable answer: `code` is `"transport"`, `status` 0, `__cause__` the httpx error."""

    def __init__(self, message: str) -> None:
        super().__init__(message, code="transport", status=0, body={})


_BY_CODE: dict[str, type[OperonError]] = {
    "invalid_argument": InvalidArgumentError,
    "schema_violation": SchemaViolationError,
    "not_found": NotFoundError,
    "already_exists": AlreadyExistsError,
    "conflict": ConflictError,
    "offset_out_of_range": OffsetOutOfRangeError,
    "resource_exhausted": ResourceExhaustedError,
    "internal": InternalError,
    "unavailable": UnavailableError,
    "timeout": OperonTimeoutError,
}


def _by_status(status: int, body: Mapping[str, Any]) -> type[OperonError]:
    if status == 400:
        return InvalidArgumentError
    if status == 404:
        return NotFoundError
    if status == 409:
        return ConflictError
    if status == 416:
        return OffsetOutOfRangeError if _offset_extras(body) is not None else OperonError
    if status == 429:
        return ResourceExhaustedError
    if status == 503:
        return UnavailableError
    if status == 504:
        return OperonTimeoutError
    if 500 <= status <= 599:
        return InternalError
    return OperonError


def error_from_body(status: int, body: Mapping[str, Any]) -> OperonError:
    """Maps a JSON error body (with a string `error`) to its typed error."""
    code = body["error"]
    message = body.get("message")
    text = message if isinstance(message, str) else code
    cls = _BY_CODE.get(code) or _by_status(status, body)
    if cls is OffsetOutOfRangeError and _offset_extras(body) is None:
        cls = OperonError
    return cls(text, code=code, status=status, body=body)
