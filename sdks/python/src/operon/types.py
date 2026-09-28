"""The SDK's value types: frozen dataclasses, independent of the wire JSON."""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal, TypeAlias

from .token import ConsistencyToken

__all__ = [
    "Consistency",
    "FetchResult",
    "FetchedRecord",
    "Id",
    "PartitionInfo",
    "ProduceResult",
    "Record",
    "StreamInfo",
]

Consistency: TypeAlias = Literal["strong", "eventual"] | ConsistencyToken | str
"""A read's consistency: `"strong"` (the default), `"eventual"`, or a token (a `str` is parsed)."""

Id: TypeAlias = int | str | uuid.UUID
"""A document id: an int in `0..2**64`, a string, or a UUID."""


@dataclass(frozen=True, slots=True)
class Record:
    """A record to produce. `str` keys, values and header values are sent as UTF-8."""

    value: bytes | str | None = None
    key: bytes | str | None = None
    headers: Sequence[tuple[str, bytes | str | None]] = ()
    timestamp_ms: int | None = None


@dataclass(frozen=True, slots=True)
class FetchedRecord:
    """A record read from a partition."""

    offset: int
    key: bytes | None
    value: bytes | None
    headers: list[tuple[str, bytes | None]]
    timestamp_ms: int


@dataclass(frozen=True, slots=True)
class ProduceResult:
    """The offsets a produce was assigned and the token that makes them visible to reads."""

    base_offset: int
    last_offset: int
    token: ConsistencyToken


@dataclass(frozen=True, slots=True)
class FetchResult:
    """One fetch: the records and the partition's bounds."""

    records: list[FetchedRecord]
    next_offset: int
    high_watermark: int
    log_start_offset: int


@dataclass(frozen=True, slots=True)
class PartitionInfo:
    """A partition's bounds."""

    partition: int
    log_start_offset: int
    high_watermark: int


@dataclass(frozen=True, slots=True)
class StreamInfo:
    """A stream's id, partitions and retention."""

    id: int
    partitions: list[PartitionInfo]
    max_age_ms: int | None
    max_bytes: int | None
