"""The asynchronous client: `_sync.py` with `async def` (plan M1.6 Task 2 rule 9)."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence

import httpx

from . import _wire
from ._transport import AsyncTransport
from .errors import AlreadyExistsError
from .types import FetchResult, ProduceResult, Record, StreamInfo

__all__ = ["AsyncClient", "AsyncNamespace"]


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
    """A namespace: its streams (and, from Task 3, its collections)."""

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
