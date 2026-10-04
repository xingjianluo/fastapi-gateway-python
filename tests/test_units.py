"""限流、熔断、加权调度与预热的纯单元测试（确定性时间，无网络）。"""

from __future__ import annotations

import threading

from app.gateway.breaker import BreakerState, CircuitBreaker
from app.gateway.pool import InflightLimit, Instance, UpstreamPool
from app.gateway.ratelimit import ClientRateLimiter, TokenBucket


def _pool(weights: dict[str, int], *, warmup: float = 0.0,
          inflight: int = 1000, ratio: float = 0.1) -> UpstreamPool:
    """构造全部健康、熔断器常闭的确定性实例池。"""
    pool = UpstreamPool("u", [], warmup_seconds=warmup,
                        warmup_initial_ratio=ratio)
    for i, w in weights.items():
        inst = Instance(
            id=i,
            url=f"http://{i}",
            breaker=CircuitBreaker(name=i),
            inflight=InflightLimit(inflight),
            weight=w,
            warmup_seconds=warmup,
            warmup_initial_ratio=ratio,
        )
        inst.healthy = True
        if warmup > 0:
            # 初始即视为预热已满（ramp 在过去就已完成）；
            # 恢复场景由 set_health/resume 重新触发爬升
            inst.ramp_started_at = -1e9
        pool.instances.append(inst)
    return pool


def _drain(pool: UpstreamPool, times: int, now: float = 1000.0):
    counts: dict[str, int] = {}
    for _ in range(times):
        inst, reason = pool.acquire_instance(now)
        assert inst is not None, f"无可用实例: {reason}"
        counts[inst.id] = counts.get(inst.id, 0) + 1
        pool.release_instance(inst)
    return counts


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


class TestWeightedScheduler:
    """平滑加权轮询（SWRR）的比例、饿死与热更新语义。"""

    def test_equal_weights_still_balanced(self):
        pool = _pool({"a": 1, "b": 1, "c": 1})
        counts = _drain(pool, 60)
        assert counts == {"a": 20, "b": 20, "c": 20}

    def test_distribution_follows_weights_exactly(self):
        """权重 5:1:1：700 次选择严格落在 500/100/100（无随机性）。"""
        pool = _pool({"a": 5, "b": 1, "c": 1})
        counts = _drain(pool, 700)
        assert counts == {"a": 500, "b": 100, "c": 100}

    def test_distribution_arbitrary_ratio(self):
        """权重 4:3:2:1：1000 次选择偏差为 0（整数权重按比例精确分配）。"""
        pool = _pool({"a": 4, "b": 3, "c": 2, "d": 1})
        counts = _drain(pool, 1000)
        assert counts == {"a": 400, "b": 300, "c": 200, "d": 100}

    def test_low_weight_instance_never_starves(self):
        """低权重实例（1）在高权重实例（100）面前也不会被饿死。"""
        pool = _pool({"a": 100, "b": 1})
        counts = _drain(pool, 1010)
        assert counts["b"] == 10
        assert counts["a"] == 1000
        # 更关键：即便只看任意一个 101 次窗口，b 也至少出现 1 次
        seen_b = False
        window = 101
        picked = []
        for _ in range(1010):
            inst, _ = pool.acquire_instance(1000.0)
            picked.append(inst.id)
            pool.release_instance(inst)
        for start in range(len(picked) - window + 1):
            if picked[start:start + window].count("b") >= 1:
                seen_b = True
                break
        assert seen_b

    def test_weight_zero_gets_no_new_traffic_but_remains_probed(self):
        """weight=0：不再被选中，但实例仍在池中（健康检查继续）。"""
        pool = _pool({"a": 1, "b": 1, "c": 1})
        assert pool.set_weight("b", 0) is not None
        counts = _drain(pool, 60)
        assert "b" not in counts
        # 实例未被删除，健康位仍可读，重新调大权重后立即回到调度
        assert pool.get_instance("b") is not None
        pool.set_weight("b", 1)
        counts = _drain(pool, 60)
        assert "b" in counts

    def test_weight_change_takes_effect_without_restart(self):
        """运行中把 1:1:1 改成 1:1:0 再改回，选择结果立即跟随。"""
        pool = _pool({"a": 1, "b": 1, "c": 1})
        pool.set_weight("c", 0)
        counts = _drain(pool, 40)
        assert counts == {"a": 20, "b": 20}
        pool.set_weight("a", 3)
        pool.set_weight("c", 1)
        counts = _drain(pool, 50)
        total = sum(counts.values())
        assert counts["a"] == 30
        assert counts["b"] == 10
        assert counts["c"] == 10
        assert total == 50

    def test_at_capacity_skips_to_other_instance_boundedly(self):
        """权重选中的实例在途满时，在候选数内换实例，且不击穿在途上限。"""
        pool = _pool({"a": 5, "b": 1}, inflight=1)
        # 占满 a（高权重者），下一次选择必须换到 b
        first, _ = pool.acquire_instance(1000.0)
        assert first.id == "a"
        second, _ = pool.acquire_instance(1000.0)
        assert second.id == "b"
        # 两个都满 -> at_capacity，且 a 的在途仍恰好为 1
        third, reason = pool.acquire_instance(1000.0)
        assert third is None and reason == "at_capacity"
        assert first.inflight.active == 1
        pool.release_instance(first)
        pool.release_instance(second)

    def test_concurrent_weighted_distribution_holds(self):
        """并发压力下比例仍然成立，且无实例被饿死。"""
        pool = _pool({"a": 5, "b": 3, "c": 2}, inflight=100)
        counts: dict[str, int] = {}
        lock = threading.Lock()

        def worker() -> None:
            inst, _ = pool.acquire_instance(1000.0)
            if inst is None:
                return
            with lock:
                counts[inst.id] = counts.get(inst.id, 0) + 1
            pool.release_instance(inst)

        threads = [threading.Thread(target=worker) for _ in range(2000)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        total = sum(counts.values())
        assert total == 2000
        # 期望 1000/600/400，允许极小调度抖动（这里 SWRR 应精确）
        assert counts == {"a": 1000, "b": 600, "c": 400}


class TestWarmupRamp:
    """重新纳入实例时有效权重线性爬升，且过程可重复。"""

    def test_effective_weight_ramps_linearly(self):
        pool = _pool({"a": 1}, warmup=10.0, ratio=0.0)
        inst = pool.get_instance("a")
        # 从 unhealthy -> healthy 触发预热起点
        inst.healthy = False
        pool.set_health("a", True, now=100.0)
        assert pool.effective_weight(inst, 100.0) == 0.0
        assert pool.effective_weight(inst, 102.5) == 0.25
        assert pool.effective_weight(inst, 105.0) == 0.5
        assert pool.effective_weight(inst, 110.0) == 1.0
        # 窗口之后保持满权重，不再回落
        assert pool.effective_weight(inst, 130.0) == 1.0

    def test_warmup_with_initial_ratio(self):
        pool = _pool({"a": 1}, warmup=10.0, ratio=0.2)
        inst = pool.get_instance("a")
        inst.healthy = False
        pool.set_health("a", True, now=0.0)
        assert abs(pool.effective_weight(inst, 0.0) - 0.2) < 1e-9
        assert abs(pool.effective_weight(inst, 5.0) - 0.6) < 1e-9
        assert pool.effective_weight(inst, 10.0) == 1.0

    def test_ramp_restarts_on_each_recovery(self):
        """反复故障/恢复：每次恢复都从初始比例重新爬升，结果一致可重复。"""
        pool = _pool({"a": 1}, warmup=10.0, ratio=0.0)
        inst = pool.get_instance("a")
        samples = []
        for cycle in range(3):
            base = 100.0 + cycle * 100
            pool.set_health("a", False, now=base - 1)
            pool.set_health("a", True, now=base)
            samples.append(
                (
                    pool.effective_weight(inst, base),
                    pool.effective_weight(inst, base + 5.0),
                    pool.effective_weight(inst, base + 10.0),
                )
            )
        assert samples == [(0.0, 0.5, 1.0)] * 3

    def test_weight_zero_to_positive_restarts_ramp(self):
        pool = _pool({"a": 1}, warmup=10.0, ratio=0.0)
        inst = pool.get_instance("a")
        pool.set_weight("a", 0, now=0.0)
        pool.set_weight("a", 1, now=50.0)  # 重新接流量
        assert pool.effective_weight(inst, 50.0) == 0.0
        assert pool.effective_weight(inst, 55.0) == 0.5


class TestDrainResume:
    """摘除：在途跑完、新流量不进；纳入：重新可调度。"""

    def test_drained_instance_gets_no_new_traffic(self):
        pool = _pool({"a": 1, "b": 1, "c": 1})
        assert pool.drain("b") is True
        counts = _drain(pool, 40)
        assert "b" not in counts

    def test_drain_keeps_inflight_request_running(self):
        """摘除时已占有的额度不被强制释放，请求可自然跑完。"""
        pool = _pool({"a": 1, "b": 1}, inflight=1)
        inst, _ = pool.acquire_instance(1000.0)
        assert inst.id == "a"
        pool.drain("a")
        # 在途请求仍持有额度，新请求只落到 b
        other, _ = pool.acquire_instance(1000.0)
        assert other.id == "b"
        assert inst.inflight.active == 1
        # 原请求正常结束并释放；排空状态被标记
        pool.release_instance(inst)
        assert pool.is_drained("a") is True
        pool.release_instance(other)

    def test_remove_blocked_until_drained(self):
        """有在途请求时直接 remove 必须报错，杜绝中途掐断。"""
        pool = _pool({"a": 1}, inflight=1)
        inst, _ = pool.acquire_instance(1000.0)
        try:
            pool.remove_instance("a")
        except RuntimeError:
            pass
        else:
            raise AssertionError("有在途请求时不得移除实例")
        pool.release_instance(inst)
        removed = pool.remove_instance("a")
        assert removed is not None
        assert pool.get_instance("a") is None

    def test_resume_restarts_ramp_and_serves(self):
        pool = _pool({"a": 1, "b": 1}, warmup=10.0, ratio=0.0)
        a = pool.get_instance("a")
        pool.drain("a")
        assert pool.resume("a", now=0.0) is True
        # 预热从 0 开始：t=0 时 a 有效权重为 0，选不到
        assert pool.effective_weight(a, 0.0) == 0.0
        inst, _ = pool.acquire_instance(0.0)
        assert inst.id == "b"
        pool.release_instance(inst)
        # 预热满后 a 重新参与
        inst, _ = pool.acquire_instance(20.0)
        assert inst.id == "a"
        pool.release_instance(inst)
