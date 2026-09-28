"""The asynchronous client: `_sync.py` with `async def` (plan M1.6 Task 2 rule 9)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

import httpx

from . import _wire
from ._transport import AsyncTransport
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

__all__ = ["AsyncClient", "AsyncCollection", "AsyncNamespace", "AsyncSearchBuilder"]


class AsyncClient:
    """The async client of the native REST API; see `Client`."""

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8080",
        *,
        timeout: float = 30.0,
        max_retries: int = 3,
        backoff_base: float = 0.1,
        backoff_max: float = 2.0,
        headers: Mapping[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        retry_sleep: Callable[[float], Awaitable[None]] | None = None,
        retry_random: Callable[[], float] | None = None,
    ) -> None:
        self._transport = AsyncTransport(
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

    async def close(self) -> None:
        """Closes the connection pool."""
        await self._transport.close()

    async def __aenter__(self) -> AsyncClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()

    async def create_namespace(self, name: str, *, exist_ok: bool = False) -> int:
        """Creates a namespace and returns its id; with `exist_ok`, returns an existing one's."""
        try:
            response = await self._transport.send(_wire.create_namespace(name))
        except AlreadyExistsError as err:
            if exist_ok and err.id is not None:
                return err.id
            raise
        return _wire.parse_created_id(response)

    def namespace(self, name: str) -> AsyncNamespace:
        """A handle on a namespace (no request is made)."""
        return AsyncNamespace(self._transport, name)


class AsyncNamespace:
    """A namespace: its streams and collections, search and SQL."""

    name: str

    def __init__(self, transport: AsyncTransport, name: str) -> None:
        self._transport = transport
        self.name = name

    def __repr__(self) -> str:
        return f"AsyncNamespace({self.name!r})"

    async def create_stream(
        self,
        name: str,
        partitions: int,
        *,
        max_age_ms: int | None = None,
        max_bytes: int | None = None,
    ) -> int:
        """Creates a stream and returns its id."""
        request = _wire.create_stream(self.name, name, partitions, max_age_ms, max_bytes)
        return _wire.parse_created_id(await self._transport.send(request))

    async def get_stream(self, name: str) -> StreamInfo:
        """A stream's partitions and retention."""
        response = await self._transport.send(_wire.get_stream(self.name, name))
        return _wire.parse_stream_info(response)

    async def produce(
        self, stream: str, partition: int, records: Sequence[Record]
    ) -> ProduceResult:
        """Appends records to one partition. Never retried once the request may have been sent."""
        request = _wire.produce(self.name, stream, partition, records)
        return _wire.parse_produce(await self._transport.send(request))

    async def fetch(
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
        return _wire.parse_fetch(await self._transport.send(request))

    # ------------------------------------------------------------ collections (Task 3)

    async def create_collection(
        self, name: str, schema: Schema, *, partitions: int | None = None
    ) -> CollectionInfo:
        """Creates a collection; an identical re-create returns the same collection."""
        request = _wire.create_collection(self.name, name, schema, partitions)
        return _wire.parse_collection_info(await self._transport.send(request))

    async def get_collection(self, name: str) -> CollectionInfo:
        """A collection by name or alias."""
        request = _wire.get_collection(self.name, name)
        return _wire.parse_collection_info(await self._transport.send(request))

    async def list_collections(self) -> list[CollectionInfo]:
        """Every collection of the namespace."""
        request = _wire.list_collections(self.name)
        return _wire.parse_collection_list(await self._transport.send(request))

    async def drop_collection(self, name: str) -> bool:
        """Drops a collection; `False` when there was none."""
        request = _wire.drop_collection(self.name, name)
        return _wire.parse_dropped(await self._transport.send(request))

    def collection(self, name: str) -> AsyncCollection:
        """A handle on a collection (no request is made)."""
        return AsyncCollection(self, name)

    def search(self, collection: str) -> AsyncSearchBuilder:
        """A search builder over `collection`."""
        return AsyncSearchBuilder(self, collection)

    async def query(
        self, request: SearchRequest, *, consistency: Consistency = "strong"
    ) -> SearchResponse:
        """Runs a search request (W12)."""
        wire = _wire.query(self.name, request, consistency)
        return _wire.parse_search(await self._transport.send(wire))

    async def sql(self, query: str, *, consistency: Consistency = "strong") -> SqlResult:
        """Runs a read-only SQL query (W13)."""
        request = _wire.sql(self.name, query, consistency)
        return _wire.parse_sql(await self._transport.send(request))


class AsyncCollection:
    """A collection: document writes and reads, search and scan plans."""

    name: str

    def __init__(self, namespace: AsyncNamespace, name: str) -> None:
        self._namespace = namespace
        self.name = name

    def __repr__(self) -> str:
        return f"AsyncCollection({self._namespace.name!r}, {self.name!r})"

    async def upsert(self, docs: Sequence[Document]) -> WriteResult:
        """Writes whole documents (one request, atomic)."""
        return await self.write([Upsert(doc) for doc in docs])

    async def patch(
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
        return await self.write([op])

    async def delete(self, ids: Sequence[Id]) -> WriteResult:
        """Deletes documents by id (one request, atomic)."""
        return await self.write([Delete(i) for i in ids])

    async def write(self, ops: Sequence[Op], *, report_existence: bool = False) -> WriteResult:
        """Sends ops as one W10 request, atomic across partitions; retried on 503 (keyed)."""
        request = _wire.write(self._namespace.name, self.name, ops, report_existence)
        return _wire.parse_write(await self._namespace._transport.send(request))

    async def get(
        self,
        ids: Sequence[Id],
        *,
        select: Projection | None = None,
        consistency: Consistency = "strong",
    ) -> list[StoredDoc | None]:
        """Documents in request order, `None` for a missing id; no vectors unless selected."""
        request = _wire.get_documents(self._namespace.name, self.name, ids, select, consistency)
        return _wire.parse_documents(await self._namespace._transport.send(request))

    def search(self) -> AsyncSearchBuilder:
        """A search builder over this collection."""
        return AsyncSearchBuilder(self._namespace, self.name)

    async def scan_plan(self, *, at: ScanAt = "current") -> ScanPlan:
        """The scan plan of one state (W15, D53): `"current"`, a manifest version or a token."""
        request = _wire.scan_plan(self._namespace.name, self.name, at)
        return _wire.parse_scan_plan(await self._namespace._transport.send(request))


class AsyncSearchBuilder(BuilderBase):
    """An immutable search builder: every method returns a new builder."""

    __slots__ = ("_namespace",)

    def __init__(self, namespace: AsyncNamespace, collection: str) -> None:
        super().__init__(collection)
        self._namespace = namespace

    async def execute(self) -> SearchResponse:
        """Builds the request and runs it."""
        return await self._namespace.query(self.build(), consistency=self._consistency)
