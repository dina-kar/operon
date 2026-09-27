"""Arrow Flight SQL through ADBC: queries as Arrow tables, and bulk ingest (M1.6 Task 4, W14).

Needs the `flight` extra (`adbc-driver-flightsql`, `adbc-driver-manager`,
`pyarrow`); `import operon` never imports this module.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from typing import Any, Literal

try:
    import adbc_driver_flightsql
    import adbc_driver_flightsql.dbapi as flightsql_dbapi
    import adbc_driver_manager
    import pyarrow as pa
except ImportError as err:  # pragma: no cover - exercised without the extra
    raise ImportError("Flight SQL needs the flight extra: install operon-client[flight]") from err

from .errors import (
    InvalidArgumentError,
    NotFoundError,
    OperonError,
    OperonTimeoutError,
    UnavailableError,
)
from .token import ConsistencyToken
from .types import Consistency, Pin

__all__ = [
    "CONSISTENCY_HEADER",
    "ID_TYPE_HEADER",
    "NAMESPACE_HEADER",
    "PIN_MANIFEST_HEADER",
    "FlightSqlClient",
]

NAMESPACE_HEADER = "operon-namespace"
CONSISTENCY_HEADER = "operon-consistency-token"
PIN_MANIFEST_HEADER = "operon-pin-manifest"
ID_TYPE_HEADER = "operon-id-type"

_CALL_HEADER = adbc_driver_flightsql.DatabaseOptions.RPC_CALL_HEADER_PREFIX.value
# The client timeout bounds every call: planning (query), reading results (fetch), ingest (update).
_TIMEOUTS = tuple(
    option.value
    for option in (
        adbc_driver_flightsql.DatabaseOptions.TIMEOUT_QUERY,
        adbc_driver_flightsql.DatabaseOptions.TIMEOUT_FETCH,
        adbc_driver_flightsql.DatabaseOptions.TIMEOUT_UPDATE,
    )
)
_ID_TYPES = ("str", "u64", "uuid")

_Status = adbc_driver_manager.AdbcStatusCode
# ADBC status → (error class, wire code, the REST status of the same error). ADBC 1.12 has
# no UNAVAILABLE: the driver reports gRPC UNAVAILABLE as IO.
_BY_STATUS: dict[Any, tuple[type[OperonError], str, int]] = {
    _Status.INVALID_ARGUMENT: (InvalidArgumentError, "invalid_argument", 400),
    _Status.NOT_FOUND: (NotFoundError, "not_found", 404),
    _Status.IO: (UnavailableError, "unavailable", 503),
    _Status.TIMEOUT: (OperonTimeoutError, "timeout", 504),
}


def _operon_error(err: adbc_driver_manager.Error) -> OperonError:
    """The typed error of an ADBC error (rule 5); the caller chains `err` as `__cause__`."""
    cls, code, status = _BY_STATUS.get(err.status_code, (OperonError, "flight", 0))
    return cls(str(err), code=code, status=status)


@contextmanager
def _mapped() -> Iterator[None]:
    try:
        yield
    except adbc_driver_manager.Error as err:
        raise _operon_error(err) from err


def _consistency_headers(consistency: Consistency) -> dict[str, str]:
    """The call headers of a read (rule 3): nothing for strong, the token, or a pin."""
    if isinstance(consistency, Pin):
        return {
            CONSISTENCY_HEADER: str(consistency.token),
            PIN_MANIFEST_HEADER: str(consistency.manifest_version),
        }
    if consistency == "strong":
        return {}
    if consistency == "eventual":
        raise ValueError("Flight SQL reads are strong, at-least-token or pinned")
    if isinstance(consistency, str):
        consistency = ConsistencyToken.parse(consistency)
    if isinstance(consistency, ConsistencyToken):
        return {CONSISTENCY_HEADER: str(consistency)}
    raise TypeError(f"not a consistency: {consistency!r}")


class FlightSqlClient:
    """A Flight SQL connection to one namespace.

    Queries return Arrow end to end; `ingest` and `ingest_stream` bulk-load
    Arrow data (no consistency token comes back, and ingest is never
    retried). Not safe to share between threads.
    """

    def __init__(
        self, uri: str = "grpc://127.0.0.1:8082", *, namespace: str, timeout: float = 30.0
    ) -> None:
        self.namespace = namespace
        db_kwargs = {_CALL_HEADER + NAMESPACE_HEADER: namespace}
        db_kwargs.update(dict.fromkeys(_TIMEOUTS, str(timeout)))
        with _mapped():
            # No transactions on the server: autocommit, or ADBC would ask for one.
            self._conn = flightsql_dbapi.connect(uri, db_kwargs=db_kwargs, autocommit=True)

    def _cursor(self, headers: Mapping[str, str]) -> adbc_driver_manager.dbapi.Cursor:
        """A new cursor whose statement sends `headers` on its calls (and only it).

        The call headers are statement options, so they end with the cursor
        and never leak into another call. (With ADBC 1.12, connection
        options set after the cursor was created did not reach its calls.)
        """
        cursor = self._conn.cursor()
        if headers:
            try:
                cursor.adbc_statement.set_options(
                    **{_CALL_HEADER + k: v for k, v in headers.items()}
                )
            except BaseException:
                cursor.close()
                raise
        return cursor

    def sql(self, query: str, *, consistency: Consistency = "strong") -> pa.Table:
        """Runs a read-only statement and returns the whole result as a table."""
        headers = _consistency_headers(consistency)
        with self._cursor(headers) as cursor, _mapped():
            cursor.execute(query)
            return cursor.fetch_arrow_table()

    def sql_batches(
        self, query: str, *, consistency: Consistency = "strong"
    ) -> pa.RecordBatchReader:
        """Runs a read-only statement and streams its record batches."""
        headers = _consistency_headers(consistency)
        cursor = self._cursor(headers)
        try:
            with _mapped():
                cursor.execute(query)
                reader = cursor.fetch_record_batch()
        except BaseException:
            cursor.close()
            raise

        def batches() -> Iterator[pa.RecordBatch]:
            try:
                with _mapped():
                    yield from reader
            finally:
                cursor.close()

        return pa.RecordBatchReader.from_batches(reader.schema, batches())

    def ingest(
        self,
        collection: str,
        data: pa.Table | pa.RecordBatchReader,
        *,
        id_type: Literal["str", "u64", "uuid"] = "str",
    ) -> int:
        """Appends Arrow rows to a collection (the server maps the columns); returns the count."""
        if id_type not in _ID_TYPES:
            raise ValueError(f"id_type is 'str', 'u64' or 'uuid', got {id_type!r}")
        headers = {} if id_type == "str" else {ID_TYPE_HEADER: id_type}
        return self._ingest(collection, data, "collections", headers)

    def ingest_stream(self, stream: str, data: pa.Table | pa.RecordBatchReader) -> int:
        """Appends Arrow rows to a stream as records; returns the count. Never retried."""
        return self._ingest(stream, data, "streams", {})

    def _ingest(self, table: str, data: object, db_schema: str, headers: Mapping[str, str]) -> int:
        with self._cursor(headers) as cursor, _mapped():
            count: int = cursor.adbc_ingest(table, data, mode="append", db_schema_name=db_schema)
            return count

    def close(self) -> None:
        """Closes the connection."""
        self._conn.close()

    def __enter__(self) -> FlightSqlClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
