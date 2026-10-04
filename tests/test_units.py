"""限流与熔断的纯单元测试（确定性时间，无网络）。"""

from __future__ import annotations

import threading

import pytest

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


"""以下为按权重调度的纯单元测试：不依赖网络，时间可注入，结果确定。"""

from app.gateway.breaker import CircuitBreaker as _CB
from app.gateway.pool import Instance, UpstreamPool


def _make_pool(
    weights: list[int],
    *,
    warmup: float = 0.0,
    inflight: int = 10,
) -> UpstreamPool:
    insts = [
        Instance(
            id=f"i{idx}",
            url=f"http://127.0.0.1:900{idx}",
            breaker=_CB(),
            inflight=InflightLimit(inflight),
            weight=w,
            healthy=True,
        )
        for idx, w in enumerate(weights)
    ]
    return UpstreamPool("up", insts, weight_warmup=warmup)


def _pick_counts(pool: UpstreamPool, n: int, now: float = 0.0) -> dict[str, int]:
    counts: dict[str, int] = {}
    for _ in range(n):
        inst, reason = pool.acquire_instance(now)
        assert inst is not None, reason
        counts[inst.id] = counts.get(inst.id, 0) + 1
        pool.release_instance(inst)
    return counts


class TestWeightedSelection:
    def test_distribution_matches_weights_exactly(self):
        """平滑加权轮询：1:2:3 在整周期内恰好 100/200/300，无饿死、无漂移。"""
        pool = _make_pool([1, 2, 3])
        counts = _pick_counts(pool, 600)
        assert counts == {"i0": 100, "i1": 200, "i2": 300}

    def test_equal_weights_behave_like_round_robin(self):
        pool = _make_pool([1, 1, 1])
        counts = _pick_counts(pool, 99)
        assert counts == {"i0": 33, "i1": 33, "i2": 33}

    def test_zero_weight_gets_no_traffic(self):
        """权重 0：不接新流量；其他实例按比例分配。"""
        pool = _make_pool([0, 1, 3])
        counts = _pick_counts(pool, 400)
        assert "i0" not in counts
        assert counts == {"i1": 100, "i2": 300}

    def test_all_zero_weight_reports_no_healthy(self):
        pool = _make_pool([0, 0])
        inst, reason = pool.acquire_instance(0.0)
        assert inst is None
        assert reason == "no_healthy"

    def test_weight_change_takes_effect_immediately(self):
        """运行时调权无需重启：1:1 -> 0:1 后流量全部落到后者。"""
        pool = _make_pool([1, 1])
        assert pool.set_weight("i0", 0, now=0.0)
        counts = _pick_counts(pool, 50)
        assert counts == {"i1": 50}
        # 调回大权重后，比例随之翻转
        assert pool.set_weight("i0", 9, now=1.0)
        counts = _pick_counts(pool, 100, now=1.0)
        assert counts == {"i0": 90, "i1": 10}

    def test_removed_instance_excluded_then_reincluded(self):
        """摘除即不再被选中；重新纳入后恢复参与，可反复操作。"""
        pool = _make_pool([1, 1])
        assert pool.remove_instance("i0")
        assert _pick_counts(pool, 20) == {"i1": 20}
        for _ in range(3):  # 反复摘除/纳入，行为一致
            assert pool.reinclude_instance("i0", now=0.0)
            counts = _pick_counts(pool, 20)
            assert counts == {"i0": 10, "i1": 10}
            assert pool.remove_instance("i0")
            assert _pick_counts(pool, 20) == {"i1": 20}

    def test_warmup_ramps_effective_weight(self):
        """爬坡期有效权重从 10% 线性升到 100%，结束后与对等实例均分。"""
        pool = _make_pool([1, 1], warmup=10.0)
        # i0 经历一次 不健康->健康 的恢复，从 now=0 开始爬坡
        pool.set_health("i0", False, now=0.0)
        pool.set_health("i0", True, now=0.0)
        i0 = pool.instances[0]
        # 起点：10%；中点：55%；结束：100%
        assert i0.effective_weight(0.0, 10.0) == pytest.approx(0.1)
        assert i0.effective_weight(5.0, 10.0) == pytest.approx(0.55)
        assert i0.effective_weight(10.0, 10.0) == pytest.approx(1.0)
        # 爬坡早期：i0 分到的流量远少于 i1（约 1:10）
        early = _pick_counts(pool, 110, now=0.0)
        assert early["i0"] < early["i1"] / 4
        # 爬坡结束后：两者均分
        pool2 = _make_pool([1, 1], warmup=10.0)
        late = _pick_counts(pool2, 100, now=100.0)
        assert late == {"i0": 50, "i1": 50}

    def test_recovery_restart_ramp_is_repeatable(self):
        """每次恢复都重新爬坡：反复故障/恢复，早期流量分布一致。"""
        pool = _make_pool([1, 1], warmup=10.0)
        snapshots = []
        for round_no in range(3):
            t = 100.0 * round_no
            pool.set_health("i0", False, now=t)
            pool.set_health("i0", True, now=t)
            counts = _pick_counts(pool, 110, now=t)
            snapshots.append(counts["i0"])
        assert snapshots[0] == snapshots[1] == snapshots[2]
        assert snapshots[0] < 30  # 早期爬坡，远小于均分的 55

    def test_at_capacity_falls_over_within_bounded_attempts(self):
        """权重最高的实例在途占满时，有界次数内换到其他实例，不击穿上限。"""
        pool = _make_pool([100, 1, 1], inflight=1)
        first, _ = pool.acquire_instance(0.0)
        assert first is not None and first.id == "i0"
        # i0 在途已满：换到次优实例，而不是等待或击穿
        second, _ = pool.acquire_instance(0.0)
        third, _ = pool.acquire_instance(0.0)
        assert {second.id, third.id} == {"i1", "i2"}
        # 全部占满：立即报容量不足，不无限尝试
        inst, reason = pool.acquire_instance(0.0)
        assert inst is None and reason == "at_capacity"
        # 计分回滚：释放后选择比例仍按权重
        for inst_ in (first, second, third):
            pool.release_instance(inst_)
        counts = _pick_counts(pool, 102)
        assert counts["i0"] == 100  # 100:1:1

    def test_exclude_set_is_respected_with_weights(self):
        """重试语义不变：已尝试实例优先跳过，全部试过才允许回退。"""
        pool = _make_pool([5, 1, 1])
        seen = set()
        for _ in range(3):
            inst, _ = pool.acquire_instance(0.0, exclude=frozenset(seen))
            assert inst is not None
            assert inst.id not in seen
            seen.add(inst.id)
            pool.release_instance(inst)
        assert seen == {"i0", "i1", "i2"}
        # 全部 exclude 后回退到合格集合
        inst, reason = pool.acquire_instance(0.0, exclude=frozenset(seen))
        assert inst is not None and reason is None
        pool.release_instance(inst)
