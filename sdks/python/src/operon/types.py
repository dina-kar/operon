"""The SDK's value types: frozen dataclasses, independent of the wire JSON."""

from __future__ import annotations

import math
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeAlias, runtime_checkable

from .schema import Schema
from .token import ConsistencyToken

if TYPE_CHECKING:
    import polars
    import pyarrow

__all__ = [
    "CollectionInfo",
    "Column",
    "Consistency",
    "Delete",
    "Document",
    "FetchResult",
    "FetchedRecord",
    "Hit",
    "Id",
    "LanceVersion",
    "Op",
    "PartitionInfo",
    "Patch",
    "PatchMode",
    "Pin",
    "ProduceResult",
    "Record",
    "ScanAt",
    "ScanColumn",
    "ScanFragment",
    "ScanPlan",
    "SearchResponse",
    "SparseVector",
    "SqlResult",
    "StoredDoc",
    "StreamInfo",
    "TotalHits",
    "Upsert",
    "VectorLike",
    "WriteResult",
]

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


# ---------------------------------------------------------------- collections (Task 3)


@runtime_checkable
class _HasToList(Protocol):
    def tolist(self) -> object: ...


VectorLike: TypeAlias = Sequence[float] | _HasToList
"""A dense vector: a sequence of numbers, or any object with `.tolist()` (e.g. a numpy array)."""

_U32_LIMIT = 2**32


@dataclass(frozen=True, slots=True)
class SparseVector:
    """A sparse vector (overview A27), checked on construction.

    Indices are unique and in `0..2**32`, the two sequences have equal
    lengths, and every value is finite. The order is kept: the server sorts.
    """

    indices: Sequence[int]
    values: Sequence[float]

    def __post_init__(self) -> None:
        indices = tuple(self.indices)
        values = tuple(self.values)
        if len(indices) != len(values):
            raise ValueError(f"a sparse vector has {len(indices)} indices but {len(values)} values")
        for index in indices:
            if isinstance(index, bool) or not isinstance(index, int):
                raise ValueError(f"a sparse vector index is an int, got {index!r}")
            if not 0 <= index < _U32_LIMIT:
                raise ValueError(f"a sparse vector index must be in 0..2**32, got {index}")
        if len(set(indices)) != len(indices):
            raise ValueError(f"a sparse vector's indices must be unique: {list(indices)}")
        for value in values:
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise ValueError(f"a sparse vector value is a number, got {value!r}")
            if not math.isfinite(value):
                raise ValueError(f"a sparse vector value must be finite, got {value!r}")
        object.__setattr__(self, "indices", indices)
        object.__setattr__(self, "values", tuple(float(v) for v in values))


@dataclass(frozen=True, slots=True)
class Document:
    """A document to upsert: its id, JSON source and vectors by field name."""

    id: Id
    source: Mapping[str, Any] = field(default_factory=dict)
    vectors: Mapping[str, VectorLike] = field(default_factory=dict)
    sparse_vectors: Mapping[str, SparseVector] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Upsert:
    """Writes a whole document, replacing any with the same id."""

    doc: Document


@dataclass(frozen=True, slots=True)
class Delete:
    """Deletes a document by id."""

    id: Id


PatchMode: TypeAlias = Literal["merge_deep", "merge_top", "replace"]


@dataclass(frozen=True, slots=True)
class Patch:
    """Changes part of a document; a `None` vector removes it. `upsert` is written if absent."""

    id: Id
    source: Mapping[str, Any] = field(default_factory=dict)
    mode: PatchMode = "merge_deep"
    delete_keys: Sequence[str] = ()
    vectors: Mapping[str, VectorLike | None] = field(default_factory=dict)
    sparse_vectors: Mapping[str, SparseVector | None] = field(default_factory=dict)
    upsert: Document | None = None


Op: TypeAlias = Upsert | Delete | Patch


@dataclass(frozen=True, slots=True)
class WriteResult:
    """A write's token (pass it as `consistency=` to read the write) and per-op results."""

    token: ConsistencyToken
    results: list[str]


@dataclass(frozen=True, slots=True)
class StoredDoc:
    """A stored document; `source` is empty when the projection asked for no source."""

    id: Id
    source: dict[str, Any]
    vectors: dict[str, list[float]]
    sparse_vectors: dict[str, SparseVector]


@dataclass(frozen=True, slots=True)
class CollectionInfo:
    """A collection: id, name and schema; every other key the server sent is in `raw`."""

    id: int
    name: str
    schema: Schema
    partitions: int | None
    live_doc_count: int | None
    raw: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class TotalHits:
    value: int
    relation: Literal["eq", "gte"]


@dataclass(frozen=True, slots=True)
class Hit:
    """One search hit. `sort_values` are as returned (pass them to `search_after`)."""

    id: Id
    score: float
    sort_values: list[Any]
    source: dict[str, Any] | None
    vectors: dict[str, list[float]]
    sparse_vectors: dict[str, SparseVector]
    highlight: dict[str, list[str]]


@dataclass(frozen=True, slots=True)
class SearchResponse:
    """A search answer. `to_arrow()`/`to_polars()` need the `arrow`/`polars` extras.

    Any Arrow PyCapsule consumer takes it directly: `pyarrow.table(response)`,
    `polars.DataFrame(response)`.
    """

    hits: list[Hit]
    total: TotalHits | None
    aggregations: dict[str, Any] | None
    groups: list[dict[str, Any]] | None
    read_token: ConsistencyToken

    def to_arrow(self, *, source: Literal["json", "columns"] = "json") -> pyarrow.Table:
        """The hits as a table: `_id`, `_score`, the source, then vectors (`operon.arrow`)."""
        from .arrow import search_response_to_arrow

        return search_response_to_arrow(self, source=source)

    def to_polars(self) -> polars.DataFrame:
        """`to_arrow()` as a Polars DataFrame, through the Arrow C stream."""
        from .arrow import to_polars

        return to_polars(self.to_arrow())

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """The Arrow PyCapsule stream of `to_arrow()`."""
        return self.to_arrow().__arrow_c_stream__(requested_schema)


@dataclass(frozen=True, slots=True)
class Column:
    """A SQL result column; `type` is Arrow's `DataType` display form."""

    name: str
    type: str


@dataclass(frozen=True, slots=True)
class SqlResult:
    """A SQL answer: columns and rows in column order; `truncated` when the row cap cut it."""

    columns: list[Column]
    rows: list[list[Any]]
    truncated: bool = False

    def to_dicts(self) -> list[dict[str, Any]]:
        """Each row as a dict keyed by column name."""
        names = [c.name for c in self.columns]
        return [dict(zip(names, row, strict=True)) for row in self.rows]

    def to_arrow(self) -> pyarrow.Table:
        """The rows as a table, typed from each column's Arrow type name (`operon.arrow`)."""
        from .arrow import sql_result_to_arrow

        return sql_result_to_arrow(self)

    def to_polars(self) -> polars.DataFrame:
        """`to_arrow()` as a Polars DataFrame, through the Arrow C stream."""
        from .arrow import to_polars

        return to_polars(self.to_arrow())

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object:
        """The Arrow PyCapsule stream of `to_arrow()`."""
        return self.to_arrow().__arrow_c_stream__(requested_schema)


@dataclass(frozen=True, slots=True)
class Pin:
    """A pinned read (a scan plan's `pin`): exactly one state, tail included."""

    manifest_version: int
    token: ConsistencyToken


Consistency: TypeAlias = Literal["strong", "eventual"] | ConsistencyToken | Pin | str
"""A read's consistency: `"strong"` (the default), `"eventual"`, a token (a `str` is parsed),
or a scan plan's `Pin` (a pinned read)."""


@dataclass(frozen=True, slots=True)
class LanceVersion:
    """The Lance version a scan plan names; `version` may exceed 2**63 (a detached id)."""

    uri: str | None
    version: int
    manifest_path: str


@dataclass(frozen=True, slots=True)
class ScanFragment:
    """A Lance fragment: data file paths, its deletion file's path, and Lance's own JSON."""

    id: int
    physical_rows: int
    deleted_rows: int
    live_rows: int
    files: list[str]
    deletion_file: str | None
    lance: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ScanColumn:
    name: str
    data_type: str
    role: str
    vector: str | None
    dim: int | None


@dataclass(frozen=True, slots=True)
class ScanPlan:
    """What an external reader needs to read one state of a collection (D53).

    Read the same state over REST or Flight SQL with `consistency=plan.pin`.
    Every key the server sent is in `raw`.
    """

    collection: str
    collection_id: int
    manifest_version: int
    lance: LanceVersion | None
    fragments: list[ScanFragment]
    live_rows: int
    columns: list[ScanColumn]
    tail: bool
    tail_records: int
    durable_token: ConsistencyToken
    pin: Pin
    planned_at_ms: int
    expires_at_ms: int | None
    raw: Mapping[str, Any]


ScanAt: TypeAlias = Literal["current"] | int | ConsistencyToken | str
"""A scan point: `"current"`, a manifest version, or a token (a `str` is parsed)."""
