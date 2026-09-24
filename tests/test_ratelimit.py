"""Token bucket / rate limiter behaviour, including concurrent pressure."""

import asyncio
import logging

from gateway.ratelimit import RateLimiter, TokenBucket

logger = logging.getLogger("test.ratelimit")


def test_bucket_burst_then_reject():
    bucket = TokenBucket(rate_per_second=1.0, burst=3)
    assert [bucket.try_acquire() for _ in range(5)] == [True, True, True, False, False]


def test_bucket_refills_over_time():
    bucket = TokenBucket(rate_per_second=100.0, burst=1)
    assert bucket.try_acquire()
    assert not bucket.try_acquire()
    now = 0.0
    bucket2 = TokenBucket(rate_per_second=10.0, burst=2)
    bucket2._last = now  # align injected clock with the bucket's timeline
    assert bucket2.try_acquire(now=now)
    assert bucket2.try_acquire(now=now)
    assert not bucket2.try_acquire(now=now)
    assert bucket2.try_acquire(now=now + 0.5)  # 0.5s * 10/s = 5 tokens, capped at 2


async def test_concurrent_quota_never_exceeded():
    """Under 200 concurrent acquisitions, exactly `burst` may pass."""
    limiter = RateLimiter(rate_per_second=0.0001, burst=10)
    results = await asyncio.gather(*[limiter.allow("client") for _ in range(200)])
    allowed = sum(results)
    logger.info("concurrent acquire: allowed=%d rejected=%d", allowed, 200 - allowed)
    assert allowed == 10


async def test_per_client_isolation():
    limiter = RateLimiter(rate_per_second=1.0, burst=2)
    assert await limiter.allow("a")
    assert await limiter.allow("a")
    assert not await limiter.allow("a")
    assert await limiter.allow("b")  # different client unaffected
