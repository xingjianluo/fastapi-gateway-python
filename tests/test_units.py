"""限流与熔断的纯单元测试（确定性时间，无网络）。"""

from __future__ import annotations

import threading

from app.gateway.breaker import BreakerState, CircuitBreaker
from app.gateway.pool import InflightLimit
from app.gateway.ratelimit import ClientRateLimiter, TokenBucket


class TestTokenBucket:
    def test_burst_capacity(self):
        b = TokenBucket(rate_per_second=5, burst=3, last_refill=0.0)
        assert [b.try_acquire(0.0) for _ in range(4)] == [True, True, True, False]

    def test_refill_at_rate(self):
        b = TokenBucket(rate_per_second=10, burst=5, last_refill=0.0)
        for _ in range(5):
            assert b.try_acquire(0.0)
        assert b.try_acquire(0.0) is False
        # 0.1s 补 1 个令牌
        assert b.try_acquire(0.1) is True
        assert b.try_acquire(0.1) is False

    def test_refill_capped_at_burst(self):
        b = TokenBucket(rate_per_second=100, burst=2, last_refill=0.0)
        assert b.try_acquire(10.0)  # 长时间后仍不超过容量
        assert b.try_acquire(10.0)
        assert b.try_acquire(10.0) is False


class TestClientRateLimiter:
    def test_clients_independent(self):
        lim = ClientRateLimiter(rate_per_second=0.0, burst=2)
        assert lim.allow("a", now=0.0)
        assert lim.allow("a", now=0.0)
        assert lim.allow("a", now=0.0) is False
        assert lim.allow("b", now=0.0)  # b 有独立桶

    def test_concurrent_quota_not_breached(self):
        """N 个线程并发抢 burst 个令牌，放行数必须恰好等于 burst。"""
        burst = 50
        lim = ClientRateLimiter(rate_per_second=0.0, burst=burst)
        allowed = []
        lock = threading.Lock()

        def worker() -> None:
            ok = lim.allow("same-client", now=100.0)
            with lock:
                allowed.append(ok)

        threads = [threading.Thread(target=worker) for _ in range(200)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert sum(allowed) == burst


class TestCircuitBreaker:
    def _cb(self, **kw):
        events = []
        cb = CircuitBreaker(
            name="x",
            on_state_change=lambda n, o, s, t: events.append((o.value, s.value)),
            **kw,
        )
        return cb, events

    def test_opens_after_threshold(self):
        cb, events = self._cb(failure_threshold=2, open_state_ttl=1.0)
        cb.record_failure(0.0)
        assert cb.state is BreakerState.CLOSED
        cb.record_failure(0.0)
        assert cb.state is BreakerState.OPEN
        assert events == [("closed", "open")]

    def test_open_rejects_until_ttl(self):
        cb, _ = self._cb(failure_threshold=1, open_state_ttl=2.0)
        cb.record_failure(0.0)
        assert cb.allow_request(1.0) is False
        assert cb.allow_request(2.0) is True
        assert cb.state is BreakerState.HALF_OPEN

    def test_half_open_single_probe(self):
        cb, events = self._cb(
            failure_threshold=1, recover_threshold=1, open_state_ttl=1.0
        )
        cb.record_failure(0.0)
        assert cb.allow_request(1.0) is True  # 探测
        assert cb.allow_request(1.1) is False  # 半开期间其余请求被拒
        cb.record_success(1.2)
        assert cb.state is BreakerState.CLOSED
        assert ("half_open", "closed") in events

    def test_probe_failure_reopens(self):
        cb, events = self._cb(
            failure_threshold=1, recover_threshold=1, open_state_ttl=1.0
        )
        cb.record_failure(0.0)
        cb.allow_request(1.0)  # -> half_open
        cb.record_failure(1.1)
        assert cb.state is BreakerState.OPEN
        # 重新计时：必须再过一个 TTL 才能探测
        assert cb.allow_request(2.0) is False
        assert cb.allow_request(2.1) is True

    def test_recover_threshold_requires_consecutive_success(self):
        cb, _ = self._cb(
            failure_threshold=1, recover_threshold=2, open_state_ttl=1.0
        )
        cb.record_failure(0.0)
        cb.allow_request(1.0)
        cb.record_success(1.1)
        assert cb.state is BreakerState.HALF_OPEN
        cb.record_success(1.2)
        assert cb.state is BreakerState.CLOSED


class TestInflightLimit:
    def test_cap_and_release(self):
        lim = InflightLimit(2)
        assert lim.acquire()
        assert lim.acquire()
        assert not lim.acquire()
        assert lim.active == 2
        lim.release()
        assert lim.active == 1
        assert lim.acquire()

    def test_double_release_rejected(self):
        lim = InflightLimit(1)
        lim.acquire()
        lim.release()
        try:
            lim.release()
        except RuntimeError:
            return
        raise AssertionError("重复释放必须被拒绝，防止计数泄漏/击穿")

    def test_concurrent_cap_not_breached(self):
        lim = InflightLimit(30)
        acquired = []
        lock = threading.Lock()

        def worker() -> None:
            ok = lim.acquire()
            with lock:
                acquired.append(ok)
                # 拿到后立刻释放，保证其他线程有机会竞争
                if ok:
                    lim.release()

        threads = [threading.Thread(target=worker) for _ in range(300)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(acquired)  # 全部最终成功
