"""The retry policy (plan M1.6 Ruling 5), shared by the sync and async transports.

A 503 is retried on idempotent requests; a connection that never reached the
server is retried on every request; a failure after the request may have been
sent is retried only on idempotent requests. Stream produce is the one request
that is not idempotent.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

import httpx

from ._wire import retry_after_seconds

MAX_RETRY_AFTER_S = 30

# Failures before the server saw the request: always safe to resend.
_BEFORE_SEND: tuple[type[Exception], ...] = (httpx.ConnectError, httpx.ConnectTimeout)
# Failures after the request may have reached the server.
_AFTER_SEND: tuple[type[Exception], ...] = (
    httpx.ReadTimeout,
    httpx.ReadError,
    httpx.RemoteProtocolError,
    httpx.WriteError,
)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_retries: int
    backoff_base: float
    backoff_max: float
    random: Callable[[], float]

    def backoff(self, retry: int) -> float:
        """The wait before retry number `retry` (1-based), with jitter."""
        capped = min(self.backoff_max, self.backoff_base * 2.0 ** (retry - 1))
        return float(capped * (0.5 + 0.5 * self.random()))

    def after_exception(self, exc: Exception, idempotent: bool, retry: int) -> float | None:
        """The wait before resending after `exc`, or `None` to give up."""
        if retry > self.max_retries:
            return None
        if isinstance(exc, _BEFORE_SEND) or (idempotent and isinstance(exc, _AFTER_SEND)):
            return self.backoff(retry)
        return None

    def after_status(
        self, status: int, headers: Mapping[str, str], idempotent: bool, retry: int
    ) -> float | None:
        """The wait before resending after a non-2xx answer, or `None` to give up."""
        if status != 503 or not idempotent or retry > self.max_retries:
            return None
        delay = self.backoff(retry)
        retry_after = retry_after_seconds(headers)
        if retry_after is not None:
            if retry_after > MAX_RETRY_AFTER_S:
                return None
            delay = max(delay, float(retry_after))
        return delay
