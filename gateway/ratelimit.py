"""Per-client token bucket rate limiter with burst allowance."""

from __future__ import annotations

import asyncio
import time


class TokenBucket:
    """Async-safe token bucket for a single client key.

    Tokens refill continuously at ``rate_per_second`` up to ``burst``.
    ``try_acquire`` never waits: it either consumes a token or refuses,
    so the gateway can reject immediately with 429.
    """

    def __init__(self, rate_per_second: float, burst: int) -> None:
        self.rate_per_second = rate_per_second
        self.burst = burst
        self._tokens = float(burst)
        self._last = time.monotonic()

    def try_acquire(self, now: float | None = None) -> bool:
        """Attempt to consume one token; True if allowed."""
        now = time.monotonic() if now is None else now
        elapsed = now - self._last
        self._last = now
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate_per_second)
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False


class RateLimiter:
    """Manages one TokenBucket per client key.

    A single asyncio.Lock guards bucket lookup + acquisition so that
    concurrent requests for the same client cannot both consume the
    last token (quota cannot be exceeded under concurrency).
    """

    def __init__(self, rate_per_second: float, burst: int) -> None:
        self.rate_per_second = rate_per_second
        self.burst = burst
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def allow(self, client_key: str) -> bool:
        """Return True if the client may proceed, consuming a token."""
        async with self._lock:
            bucket = self._buckets.get(client_key)
            if bucket is None:
                bucket = TokenBucket(self.rate_per_second, self.burst)
                self._buckets[client_key] = bucket
            return bucket.try_acquire()
