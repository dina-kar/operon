"""Sends a `_wire.Request` over httpx, with the retry policy and error mapping.

The sync and async transports differ only in `def`/`async def`, the httpx
client and the sleep function.
"""

from __future__ import annotations

import random as _random
import time
from collections.abc import Awaitable, Callable, Mapping

import anyio
import httpx

from ._retry import RetryPolicy
from ._wire import USER_AGENT, Request, Response, encode_body, error_from_response
from .errors import TransportError


def _headers(extra: Mapping[str, str] | None) -> dict[str, str]:
    return {"User-Agent": USER_AGENT} | dict(extra or {})


def _policy(
    max_retries: int,
    backoff_base: float,
    backoff_max: float,
    retry_random: Callable[[], float] | None,
) -> RetryPolicy:
    if max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    return RetryPolicy(max_retries, backoff_base, backoff_max, retry_random or _random.random)


def _content(request: Request) -> tuple[bytes | None, dict[str, str]]:
    headers = dict(request.headers)
    if request.json_body is None:
        return None, headers
    headers["Content-Type"] = "application/json"
    return encode_body(request.json_body), headers


def _response(raw: httpx.Response) -> Response:
    return Response(
        status=raw.status_code,
        headers={k.lower(): v for k, v in raw.headers.items()},
        content=raw.content,
    )


def _transport_error(exc: httpx.TransportError) -> TransportError:
    error = TransportError(f"{type(exc).__name__}: {exc}")
    error.__cause__ = exc
    return error


class SyncTransport:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float,
        max_retries: int,
        backoff_base: float,
        backoff_max: float,
        headers: Mapping[str, str] | None,
        transport: httpx.BaseTransport | None,
        retry_sleep: Callable[[float], None] | None,
        retry_random: Callable[[], float] | None,
    ) -> None:
        self._timeout = timeout
        self._policy = _policy(max_retries, backoff_base, backoff_max, retry_random)
        self._sleep = retry_sleep or time.sleep
        self._http = httpx.Client(
            base_url=base_url, timeout=timeout, transport=transport, headers=_headers(headers)
        )

    def close(self) -> None:
        self._http.close()

    def send(self, request: Request) -> Response:
        content, headers = _content(request)
        timeout = self._timeout + request.extra_timeout
        retry = 0
        while True:
            retry += 1
            try:
                raw = self._http.request(
                    request.method,
                    request.path,
                    params=dict(request.query),
                    headers=headers,
                    content=content,
                    timeout=timeout,
                )
            except httpx.TransportError as exc:
                delay = self._policy.after_exception(exc, request.idempotent, retry)
                if delay is None:
                    raise _transport_error(exc) from exc
                self._sleep(delay)
                continue
            response = _response(raw)
            if 200 <= response.status < 300:
                return response
            delay = self._policy.after_status(
                response.status, response.headers, request.idempotent, retry
            )
            if delay is None:
                raise error_from_response(response.status, response.content)
            self._sleep(delay)


class AsyncTransport:
    def __init__(
        self,
        base_url: str,
        *,
        timeout: float,
        max_retries: int,
        backoff_base: float,
        backoff_max: float,
        headers: Mapping[str, str] | None,
        transport: httpx.AsyncBaseTransport | None,
        retry_sleep: Callable[[float], Awaitable[None]] | None,
        retry_random: Callable[[], float] | None,
    ) -> None:
        self._timeout = timeout
        self._policy = _policy(max_retries, backoff_base, backoff_max, retry_random)
        self._sleep = retry_sleep or anyio.sleep
        self._http = httpx.AsyncClient(
            base_url=base_url, timeout=timeout, transport=transport, headers=_headers(headers)
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def send(self, request: Request) -> Response:
        content, headers = _content(request)
        timeout = self._timeout + request.extra_timeout
        retry = 0
        while True:
            retry += 1
            try:
                raw = await self._http.request(
                    request.method,
                    request.path,
                    params=dict(request.query),
                    headers=headers,
                    content=content,
                    timeout=timeout,
                )
            except httpx.TransportError as exc:
                delay = self._policy.after_exception(exc, request.idempotent, retry)
                if delay is None:
                    raise _transport_error(exc) from exc
                await self._sleep(delay)
                continue
            response = _response(raw)
            if 200 <= response.status < 300:
                return response
            delay = self._policy.after_status(
                response.status, response.headers, request.idempotent, retry
            )
            if delay is None:
                raise error_from_response(response.status, response.content)
            await self._sleep(delay)
