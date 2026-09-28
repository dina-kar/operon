"""The synchronous client. `_async.py` mirrors it method for method (plan M1.6 Task 2 rule 9)."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

import httpx

from . import _wire
from ._transport import SyncTransport
from .errors import AlreadyExistsError
from .query import BuilderBase, Projection, SearchRequest
from .schema import Schema
from .types import (
    CollectionInfo,
    Consistency,
    Delete,
    Document,
    FetchResult,
    Id,
    Op,
    Patch,
    PatchMode,
    ProduceResult,
    Record,
    ScanAt,
    ScanPlan,
    SearchResponse,
    SparseVector,
    SqlResult,
    StoredDoc,
    StreamInfo,
    Upsert,
    VectorLike,
    WriteResult,
)

__all__ = ["Client", "Collection", "Namespace", "SearchBuilder"]


class Client:
    """A client of the native REST API.

    Requests that fail with 503 or a connection error are retried with capped
    exponential backoff; stream produce is never retried after the request may
    have reached the server.
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8080",
        *,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_base: float = 0.1,
        backoff_max: float = 2.0,
        headers: Mapping[str, str] | None = None,
        transport: httpx.BaseTransport | None = None,
        retry_sleep: Callable[[float], None] | None = None,
        retry_random: Callable[[], float] | None = None,
    ) -> None:
        self._transport = SyncTransport(
            base_url,
            timeout=timeout,
            max_retries=max_retries,
            backoff_base=backoff_base,
            backoff_max=backoff_max,
            headers=headers,
            transport=transport,
            retry_sleep=retry_sleep,
            retry_random=retry_random,
        )

    def close(self) -> None:
        """Closes the connection pool."""
        self._transport.close()

    def __enter__(self) -> Client:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def create_namespace(self, name: str, *, exist_ok: bool = False) -> int:
        """Creates a namespace and returns its id; with `exist_ok`, returns an existing one's."""
        try:
            response = self._transport.send(_wire.create_namespace(name))
        except AlreadyExistsError as err:
            if exist_ok and err.id is not None:
                return err.id
            raise
        return _wire.parse_created_id(response)

    def namespace(self, name: str) -> Namespace:
        """A handle on a namespace (no request is made)."""
        return Namespace(self._transport, name)


class Namespace:
    """A namespace: its streams and collections, search and SQL."""

    name: str

    def __init__(self, transport: SyncTransport, name: str) -> None:
        self._transport = transport
        self.name = name

    def __repr__(self) -> str:
        return f"Namespace({self.name!r})"

    def create_stream(
        self,
        name: str,
        partitions: int,
        *,
        max_age_ms: int | None = None,
        max_bytes: int | None = None,
    ) -> int:
        """Creates a stream and returns its id."""
        request = _wire.create_stream(self.name, name, partitions, max_age_ms, max_bytes)
        return _wire.parse_created_id(self._transport.send(request))

    def get_stream(self, name: str) -> StreamInfo:
        """A stream's partitions and retention."""
        return _wire.parse_stream_info(self._transport.send(_wire.get_stream(self.name, name)))

    def produce(self, stream: str, partition: int, records: Sequence[Record]) -> ProduceResult:
        """Appends records to one partition. Never retried once the request may have been sent."""
        request = _wire.produce(self.name, stream, partition, records)
        return _wire.parse_produce(self._transport.send(request))

    def fetch(
        self,
        stream: str,
        partition: int,
        offset: int,
        *,
        max_bytes: int | None = None,
        max_wait_ms: int | None = None,
    ) -> FetchResult:
        """Reads records from `offset`; `max_wait_ms` long-polls (the timeout is extended by it)."""
        request = _wire.fetch(self.name, stream, partition, offset, max_bytes, max_wait_ms)
        return _wire.parse_fetch(self._transport.send(request))

    # ------------------------------------------------------------ collections (Task 3)

    def create_collection(
        self, name: str, schema: Schema, *, partitions: int | None = None
    ) -> CollectionInfo:
        """Creates a collection; an identical re-create returns the same collection."""
        request = _wire.create_collection(self.name, name, schema, partitions)
        return _wire.parse_collection_info(self._transport.send(request))

    def get_collection(self, name: str) -> CollectionInfo:
        """A collection by name or alias."""
        request = _wire.get_collection(self.name, name)
        return _wire.parse_collection_info(self._transport.send(request))

    def list_collections(self) -> list[CollectionInfo]:
        """Every collection of the namespace."""
        request = _wire.list_collections(self.name)
        return _wire.parse_collection_list(self._transport.send(request))

    def drop_collection(self, name: str) -> bool:
        """Drops a collection; `False` when there was none."""
        request = _wire.drop_collection(self.name, name)
        return _wire.parse_dropped(self._transport.send(request))

    def collection(self, name: str) -> Collection:
        """A handle on a collection (no request is made)."""
        return Collection(self, name)

    def search(self, collection: str) -> SearchBuilder:
        """A search builder over `collection`."""
        return SearchBuilder(self, collection)

    def query(
        self, request: SearchRequest, *, consistency: Consistency = "strong"
    ) -> SearchResponse:
        """Runs a search request (W12)."""
        wire = _wire.query(self.name, request, consistency)
        return _wire.parse_search(self._transport.send(wire))

    def sql(self, query: str, *, consistency: Consistency = "strong") -> SqlResult:
        """Runs a read-only SQL query (W13)."""
        request = _wire.sql(self.name, query, consistency)
        return _wire.parse_sql(self._transport.send(request))


class Collection:
    """A collection: document writes and reads, search and scan plans."""

    name: str

    def __init__(self, namespace: Namespace, name: str) -> None:
        self._namespace = namespace
        self.name = name

    def __repr__(self) -> str:
        return f"Collection({self._namespace.name!r}, {self.name!r})"

    def upsert(self, docs: Sequence[Document]) -> WriteResult:
        """Writes whole documents (one request, atomic)."""
        return self.write([Upsert(doc) for doc in docs])

    def patch(
        self,
        id: Id,
        source: Mapping[str, Any] | None = None,
        *,
        mode: PatchMode = "merge_deep",
        delete_keys: Sequence[str] = (),
        vectors: Mapping[str, VectorLike | None] | None = None,
        sparse_vectors: Mapping[str, SparseVector | None] | None = None,
        upsert: Document | None = None,
    ) -> WriteResult:
        """Changes part of one document; `upsert` is written when it does not exist."""
        op = Patch(
            id,
            source if source is not None else {},
            mode,
            tuple(delete_keys),
            vectors if vectors is not None else {},
            sparse_vectors if sparse_vectors is not None else {},
            upsert,
        )
        return self.write([op])

    def delete(self, ids: Sequence[Id]) -> WriteResult:
        """Deletes documents by id (one request, atomic)."""
        return self.write([Delete(i) for i in ids])

    def write(self, ops: Sequence[Op], *, report_existence: bool = False) -> WriteResult:
        """Sends ops as one W10 request, atomic across partitions; retried on 503 (keyed)."""
        request = _wire.write(self._namespace.name, self.name, ops, report_existence)
        return _wire.parse_write(self._namespace._transport.send(request))

    def get(
        self,
        ids: Sequence[Id],
        *,
        select: Projection | None = None,
        consistency: Consistency = "strong",
    ) -> list[StoredDoc | None]:
        """Documents in request order, `None` for a missing id; no vectors unless selected."""
        request = _wire.get_documents(self._namespace.name, self.name, ids, select, consistency)
        return _wire.parse_documents(self._namespace._transport.send(request))

    def search(self) -> SearchBuilder:
        """A search builder over this collection."""
        return SearchBuilder(self._namespace, self.name)

    def scan_plan(self, *, at: ScanAt = "current") -> ScanPlan:
        """The scan plan of one state (W15, D53): `"current"`, a manifest version or a token."""
        request = _wire.scan_plan(self._namespace.name, self.name, at)
        return _wire.parse_scan_plan(self._namespace._transport.send(request))


class SearchBuilder(BuilderBase):
    """An immutable search builder: every method returns a new builder."""

    __slots__ = ("_namespace",)

    def __init__(self, namespace: Namespace, collection: str) -> None:
        super().__init__(collection)
        self._namespace = namespace

    def execute(self) -> SearchResponse:
        """Builds the request and runs it."""
        return self._namespace.query(self.build(), consistency=self._consistency)
