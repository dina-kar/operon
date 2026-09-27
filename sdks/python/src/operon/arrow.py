"""Arrow and Polars conversions of REST results (plan M1.6 Task 4 rules 9-12, Ruling 17).

Importing this module imports `pyarrow`; `import operon` never does. The
methods `to_arrow()`, `to_polars()` and `__arrow_c_stream__` on
`SearchResponse` and `SqlResult` import it when called.
"""

from __future__ import annotations

import base64
import datetime
import json
import re
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

try:
    import pyarrow as pa
except ImportError as err:  # pragma: no cover - exercised by blocking the import
    raise ImportError(
        "Arrow results need pyarrow: install operon-client[arrow] (or pip install pyarrow)"
    ) from err

from .types import Hit, SearchResponse, SqlResult

if TYPE_CHECKING:
    import polars

__all__ = ["READ_TOKEN_METADATA", "search_response_to_arrow", "sql_result_to_arrow", "to_polars"]

READ_TOKEN_METADATA = "operon.read_token"
"""The schema metadata key of a search table's read token."""

_SPARSE_TYPE = pa.struct(
    [pa.field("indices", pa.list_(pa.uint32())), pa.field("values", pa.list_(pa.float32()))]
)


def _display_id(value: int | str | uuid.UUID) -> str:
    """The SQL display form of an id: a u64 in decimal, a string as itself, a UUID hyphenated."""
    return str(value)


def _dense_type(values: list[list[float] | None]) -> pa.DataType:
    lengths = {len(v) for v in values if v is not None}
    if len(lengths) == 1:
        return pa.list_(pa.float32(), lengths.pop())
    return pa.list_(pa.float32())


def _source_columns(hits: list[Hit]) -> tuple[list[str], list[pa.Array]]:
    keys: dict[str, None] = {}
    for hit in hits:
        for key in hit.source or {}:
            keys.setdefault(key, None)
    names = list(keys)
    arrays = [pa.array([(hit.source or {}).get(key) for hit in hits]) for key in names]
    return names, arrays


def search_response_to_arrow(
    response: SearchResponse, *, source: Literal["json", "columns"] = "json"
) -> pa.Table:
    """One row per hit, in rank order: `_id`, `_score`, the source, dense then sparse vectors.

    With `source="json"` the source is one `_source` JSON string column, and
    the table has the collection column shape Flight SQL ingest takes
    (`_score` is ignored there). With `source="columns"` each top-level
    source key is a column, its type inferred by pyarrow.
    """
    if source not in ("json", "columns"):
        raise ValueError(f"source is 'json' or 'columns', got {source!r}")
    hits = response.hits
    names: list[str] = ["_id", "_score"]
    arrays: list[Any] = [
        pa.array([_display_id(h.id) for h in hits], type=pa.string()),
        pa.array([h.score for h in hits], type=pa.float32()),
    ]
    if source == "json":
        names.append("_source")
        arrays.append(
            pa.array(
                [
                    None
                    if h.source is None
                    else json.dumps(h.source, separators=(",", ":"), ensure_ascii=False)
                    for h in hits
                ],
                type=pa.string(),
            )
        )
    else:
        source_names, source_arrays = _source_columns(hits)
        names.extend(source_names)
        arrays.extend(source_arrays)
    for name in sorted({n for h in hits for n in h.vectors}):
        dense = [h.vectors.get(name) for h in hits]
        names.append(name)
        arrays.append(pa.array(dense, type=_dense_type(dense)))
    for name in sorted({n for h in hits for n in h.sparse_vectors}):
        sparse = [h.sparse_vectors.get(name) for h in hits]
        names.append(name)
        arrays.append(
            pa.array(
                [
                    None if s is None else {"indices": list(s.indices), "values": list(s.values)}
                    for s in sparse
                ],
                type=_SPARSE_TYPE,
            )
        )
    schema = pa.schema(
        [pa.field(n, a.type) for n, a in zip(names, arrays, strict=True)],
        metadata={READ_TOKEN_METADATA: str(response.read_token)},
    )
    return pa.Table.from_arrays(arrays, schema=schema)


# ---------------------------------------------------------------- SQL results (rule 11)

_PRIMITIVES: dict[str, Callable[[], Any]] = {
    "Boolean": pa.bool_,
    "Int8": pa.int8,
    "Int16": pa.int16,
    "Int32": pa.int32,
    "Int64": pa.int64,
    "UInt8": pa.uint8,
    "UInt16": pa.uint16,
    "UInt32": pa.uint32,
    "UInt64": pa.uint64,
    "Float32": pa.float32,
    "Float64": pa.float64,
    "Utf8": pa.string,
    "Utf8View": pa.string,
    "LargeUtf8": pa.large_string,
}
_BINARIES: dict[str, Callable[[], Any]] = {
    "Binary": pa.binary,
    "LargeBinary": pa.large_binary,
    "BinaryView": pa.binary,
}
_DATES = frozenset({"Date32", "Date64"})
# Arrow 58's Display: `Timestamp(µs, "UTC")`, the zone part absent without a zone (row E12).
_TIMESTAMP = re.compile(r'^Timestamp\((s|ms|µs|ns)(?:, "([^"]*)")?\)$')
_FIXED_SIZE_LIST = re.compile(r"^FixedSizeList\((\d+) x (\w+)\)$")
_UNITS = {"s": "s", "ms": "ms", "µs": "us", "ns": "ns"}


def _date(value: object) -> datetime.date | None:
    if value is None:
        return None
    return datetime.date.fromisoformat(str(value))


def _column(type_text: str, values: list[Any]) -> pa.Array:
    """One SQL column as an Arrow array, typed from arrow's `DataType` display string."""
    if type_text in _PRIMITIVES:
        return pa.array(values, type=_PRIMITIVES[type_text]())
    if type_text in _BINARIES:
        decoded = [None if v is None else base64.b64decode(v, validate=True) for v in values]
        return pa.array(decoded, type=_BINARIES[type_text]())
    if type_text in _DATES:
        return pa.array([_date(v) for v in values], type=pa.date32())
    timestamp = _TIMESTAMP.match(type_text)
    if timestamp:
        unit = _UNITS[timestamp.group(1)]
        zone = timestamp.group(2)
        # Values are always UTC with `Z` at the unit's precision; the zone is only in the type.
        utc = pa.array(values, type=pa.string()).cast(pa.timestamp(unit, "UTC"))
        return utc.cast(pa.timestamp(unit, zone))
    fixed = _FIXED_SIZE_LIST.match(type_text)
    if fixed and fixed.group(2) in _PRIMITIVES:
        item = _PRIMITIVES[fixed.group(2)]()
        return pa.array(values, type=pa.list_(item, int(fixed.group(1))))
    # Lists, structs, decimals (strings as sent) and anything else: pyarrow infers.
    return pa.array(values)


def sql_result_to_arrow(result: SqlResult) -> pa.Table:
    """One column per SQL column, in order, typed from its arrow `DataType` display string."""
    arrays = [
        _column(column.type, [row[i] for row in result.rows])
        for i, column in enumerate(result.columns)
    ]
    schema = pa.schema(
        [pa.field(c.name, a.type) for c, a in zip(result.columns, arrays, strict=True)]
    )
    return pa.Table.from_arrays(arrays, schema=schema)


def to_polars(table: pa.Table) -> polars.DataFrame:
    """The table as a Polars DataFrame, handed over through the Arrow C stream."""
    try:
        import polars as pl
    except ImportError as err:
        raise ImportError(
            "Polars results need polars: install operon-client[polars] (or pip install polars)"
        ) from err
    return pl.DataFrame(table)
