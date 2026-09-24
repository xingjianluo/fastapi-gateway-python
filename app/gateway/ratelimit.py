"""按客户端维度的令牌桶限流。

- 每个客户端独立一个令牌桶，容量 = burst，填充速率 = rate_per_second；
- 所有取令牌操作在同一把锁内完成「补充 + 扣减」，并发下不会超发；
- 空闲桶惰性过期，避免无界增长。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass
class TokenBucket:
    rate_per_second: float
    burst: int
    tokens: float = -1.0  # -1 表示尚未显式赋值，初始化为满桶
    last_refill: float = 0.0

    def __post_init__(self) -> None:
        if self.tokens < 0:
            self.tokens = float(self.burst)
        if self.last_refill == 0.0:
            self.last_refill = time.monotonic()

    def try_acquire(self, now: float, tokens: float = 1.0) -> bool:
        """按经过时间补充令牌后尝试扣减；桶容量上限为 burst。"""
        elapsed = max(0.0, now - self.last_refill)
        self.last_refill = now
        self.tokens = min(float(self.burst), self.tokens + elapsed * self.rate_per_second)
        if self.tokens >= tokens:
            self.tokens -= tokens
            return True
        return False


class ClientRateLimiter:
    def __init__(
        self,
        rate_per_second: float,
        burst: int,
        idle_ttl: float = 120.0,
    ) -> None:
        self._rate = rate_per_second
        self._burst = burst
        self._idle_ttl = idle_ttl
        self._buckets: dict[str, TokenBucket] = {}
        self._lock = threading.Lock()

    def allow(self, client: str, now: float | None = None) -> bool:
        """判断该客户端本次请求是否放行（消耗 1 个令牌）。"""
        if now is None:
            now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(client)
            if bucket is None:
                bucket = TokenBucket(self._rate, self._burst, last_refill=now)
                self._buckets[client] = bucket
            self._evict_idle(now)
            return bucket.try_acquire(now)

    def available(self, client: str, now: float | None = None) -> float:
        if now is None:
            now = time.monotonic()
        with self._lock:
            bucket = self._buckets.get(client)
            if bucket is None:
                return float(self._burst)
            elapsed = max(0.0, now - bucket.last_refill)
            return min(float(self._burst), bucket.tokens + elapsed * self._rate)

    def reset(self, client: str | None = None) -> None:
        with self._lock:
            if client is None:
                self._buckets.clear()
            else:
                self._buckets.pop(client, None)

    def _evict_idle(self, now: float) -> None:
        """调用方需持有锁。"""
        stale = [
            key
            for key, bucket in self._buckets.items()
            if now - bucket.last_refill > self._idle_ttl
        ]
        for key in stale:
            self._buckets.pop(key, None)
