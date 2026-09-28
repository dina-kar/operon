"""Operon's Python client for the native REST API (sync `Client` and async `AsyncClient`)."""

from ._async import AsyncClient, AsyncNamespace
from ._sync import Client, Namespace
from ._version import __version__
from .errors import (
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
    TransportError,
    UnavailableError,
)
from .token import ConsistencyToken
from .types import (
    Consistency,
    FetchedRecord,
    FetchResult,
    Id,
    PartitionInfo,
    ProduceResult,
    Record,
    StreamInfo,
)

__all__ = [
    "AlreadyExistsError",
    "AsyncClient",
    "AsyncNamespace",
    "Client",
    "ConflictError",
    "Consistency",
    "ConsistencyToken",
    "FetchResult",
    "FetchedRecord",
    "Id",
    "InternalError",
    "InvalidArgumentError",
    "Namespace",
    "NotFoundError",
    "OffsetOutOfRangeError",
    "OperonError",
    "OperonTimeoutError",
    "PartitionInfo",
    "ProduceResult",
    "Record",
    "ResourceExhaustedError",
    "SchemaViolationError",
    "StreamInfo",
    "TransportError",
    "UnavailableError",
    "__version__",
]
