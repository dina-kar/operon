"""The synchronous client. `_async.py` mirrors it method for method (plan M1.6 Task 2 rule 9)."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence

import httpx

from . import _wire
from ._transport import SyncTransport
from .errors import AlreadyExistsError
from .types import FetchResult, ProduceResult, Record, StreamInfo

__all__ = ["Client", "Namespace"]


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
    """A namespace: its streams (and, from Task 3, its collections)."""

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
