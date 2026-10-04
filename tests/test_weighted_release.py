"""灰度发布能力集成测试：

场景分组（日志中以 SCENARIO 标记，便于看出覆盖范围与逐个用例结果）：
- 权重分配：等权均衡 / 非等权比例偏差 / weight=0 摘流但仍探测 / 热改权重即时生效
- 摘除与纳入：drain 后在途请求跑完、新请求不进；resume 后流量回归
- 反复故障恢复：多次 fail/recover 后流量分布重新平稳，实例不会「进去一次再回不来」
- 预热爬升：恢复后流量逐步加回，而不是瞬间压满
- 协作：权重选中实例在途满时换到其他实例，且限流/503/504 状态码保持不变
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest


def _hit(gw, path: str = "/api/echo") -> tuple[int, str | None]:
    r = gw.get(path)
    inst = r.json().get("instance") if r.status_code == 200 else None
    return r.status_code, inst


def _wait_healthy(gw, count: int = 3, timeout: float = 3.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        stats = gw.get("/_gateway/stats").json()
        flags = [
            i["healthy"]
            for up in stats["upstreams"]
            for i in up["instances"]
        ]
        if len(flags) >= count and all(flags):
            return
        time.sleep(0.05)
    raise AssertionError("实例未全部转健康")


def _instance_stats(gw, instance_id: str) -> dict:
    stats = gw.get("/_gateway/stats").json()
    for up in stats["upstreams"]:
        for inst in up["instances"]:
            if inst["id"] == instance_id:
                return inst
    raise KeyError(instance_id)


def _instance_url(gw, instance_id: str) -> str:
    return _instance_stats(gw, instance_id)["url"]


# --------------------------------------------------------------------- #
# 场景一：权重分配
# --------------------------------------------------------------------- #


class TestWeightedDistribution:
    SCENARIO = "权重分配"

    def test_equal_weights_balance(self, gateway_factory):
        """等权（默认）：60 次请求三个实例各 20，无偏差。"""
        gw = gateway_factory(health_interval=60.0)
        _wait_healthy(gw)
        counts = Counter(_hit(gw)[1] for _ in range(60))
        assert counts == {"u1": 20, "u2": 20, "u3": 20}

    def test_weighted_ratio_has_small_deviation(self, gateway_factory):
        """权重 5:1:1：140 次请求比例 100/20/20，偏差为 0。"""
        gw = gateway_factory(
            weights={"u1": 5, "u2": 1, "u3": 1},
            health_interval=60.0,
        )
        _wait_healthy(gw)
        counts = Counter(_hit(gw)[1] for _ in range(140))
        assert counts == {"u1": 100, "u2": 20, "u3": 20}, counts

    def test_weighted_distribution_under_concurrency(self, gateway_factory):
        """并发压力（16 线程 x 30）下比例仍然成立，低权重实例不被饿死。"""
        gw = gateway_factory(
            weights={"u1": 6, "u2": 3, "u3": 1},
            health_interval=60.0,
            rate=10000.0,
            burst=1000,
        )
        _wait_healthy(gw)
        counts: Counter = Counter()
        lock = threading.Lock()

        def one(_: int) -> None:
            _, inst = _hit(gw)
            with lock:
                counts[inst] += 1

        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(one, range(480)))
        assert sum(counts.values()) == 480
        # 期望 288/144/48；SWRR 无随机，允许极少量边界调度差异
        assert counts["u1"] == 288, counts
        assert counts["u2"] == 144, counts
        assert counts["u3"] == 48, counts

    def test_weight_zero_takes_no_traffic_but_still_healthy(self, gateway_factory):
        """weight=0：无新流量，但健康检查照常（healthy 仍为 true）。"""
        gw = gateway_factory(
            weights={"u1": 1, "u2": 1, "u3": 1},
            health_interval=0.2,
        )
        _wait_healthy(gw)
        r = gw.post(
            "/_gateway/upstreams/api/instances/u3/weight",
            json={"weight": 0},
        )
        assert r.status_code == 200
        counts = Counter(_hit(gw)[1] for _ in range(40))
        assert "u3" not in counts
        # 健康探测仍在跑：u3 保持健康，随时可调回
        time.sleep(0.4)
        assert _instance_stats(gw, "u3")["healthy"] is True

        r = gw.post(
            "/_gateway/upstreams/api/instances/u3/weight",
            json={"weight": 1},
        )
        assert r.status_code == 200
        counts = Counter(_hit(gw)[1] for _ in range(30))
        assert "u3" in counts

    def test_weight_change_hot_reloads_without_restart(self, gateway_factory):
        """运行中热改权重：1:1:1 -> 3:1:0，立即按新比例分流。"""
        gw = gateway_factory(health_interval=60.0)
        _wait_healthy(gw)
        for inst, w in (("u1", 3), ("u2", 1), ("u3", 0)):
            r = gw.post(
                f"/_gateway/upstreams/api/instances/{inst}/weight",
                json={"weight": w},
            )
            assert r.status_code == 200
        counts = Counter(_hit(gw)[1] for _ in range(40))
        assert counts == {"u1": 30, "u2": 10}, counts


# --------------------------------------------------------------------- #
# 场景二：实例摘除（drain / weight=0 / 配置移除）时在途请求跑完
# --------------------------------------------------------------------- #


class TestDrainInFlight:
    SCENARIO = "摘除与在途保护"

    def test_drain_lets_inflight_finish_and_blocks_new(self, gateway_factory):
        """摘除瞬间在途的慢请求必须正常 200 返回；摘除后的新请求不再进入。"""
        gw = gateway_factory(
            max_inflight=10,
            timeout=5.0,
            max_retries=0,
            health_interval=60.0,
            rate=10000.0,
            burst=1000,
        )
        _wait_healthy(gw)

        results: list[int] = []
        lock = threading.Lock()
        barrier = threading.Event()

        def slow_call() -> None:
            barrier.wait()
            r = gw.get("/api/slow?delay=0.8")
            with lock:
                results.append(r.status_code)

        # 先发起 4 个慢请求（覆盖 3 个实例），等它们在在途中
        threads = [threading.Thread(target=slow_call) for _ in range(4)]
        for t in threads:
            t.start()
        barrier.set()
        time.sleep(0.35)  # 请求已打到上游、正在 sleep

        # 摘除全部三个实例
        for inst in ("u1", "u2", "u3"):
            assert gw.post(
                f"/_gateway/upstreams/api/instances/{inst}/drain"
            ).status_code == 200

        # 摘除后立即的新请求必须 503（无可用实例），不能中途掐断在途请求
        r = gw.get("/api/echo")
        assert r.status_code == 503

        for t in threads:
            t.join(timeout=5)
        # 在途的 4 个请求全部正常完成，没有一个 5xx
        assert results.count(200) == 4, results

        # 在途归零后所有实例标记为排空
        for inst in ("u1", "u2", "u3"):
            assert _instance_stats(gw, inst)["draining"] is True
            assert _instance_stats(gw, inst)["inflight"] == 0

    def test_weight_zero_lets_inflight_finish(self, gateway_factory):
        """weight 置 0 同样不影响已在途请求，仅阻止新流量。"""
        gw = gateway_factory(
            timeout=5.0, max_retries=0, health_interval=60.0,
            rate=10000.0, burst=1000,
        )
        _wait_healthy(gw)
        done = threading.Event()

        def slow() -> None:
            gw.get("/api/slow?delay=0.7")
            done.set()

        t = threading.Thread(target=slow)
        t.start()
        time.sleep(0.3)
        # 把所有实例权重置 0
        for inst in ("u1", "u2", "u3"):
            gw.post(
                f"/_gateway/upstreams/api/instances/{inst}/weight",
                json={"weight": 0},
            )
        assert gw.get("/api/echo").status_code == 503
        t.join(timeout=5)
        assert done.is_set(), "在途慢请求必须跑完"

    def test_resume_brings_instance_back(self, gateway_factory):
        """resume 后实例重新接收新流量。"""
        gw = gateway_factory(health_interval=60.0)
        _wait_healthy(gw)
        gw.post("/_gateway/upstreams/api/instances/u2/drain")
        counts = Counter(_hit(gw)[1] for _ in range(40))
        assert "u2" not in counts
        gw.post("/_gateway/upstreams/api/instances/u2/resume")
        counts = Counter(_hit(gw)[1] for _ in range(30))
        assert "u2" in counts
        assert not _instance_stats(gw, "u2")["draining"]

    def test_reload_removing_instance_with_inflight_returns_409(self, gateway):
        """配置移除仍有在途请求的实例：返回 409，转为摘除；排空后再 reload 删除。"""
        gw = gateway
        _wait_healthy(gw)
        u1_url = _instance_url(gw, "u1")
        u2_url = _instance_url(gw, "u2")

        # 先摘除 u1/u2，让慢请求必然落在 u3 上
        gw.post("/_gateway/upstreams/api/instances/u1/drain")
        gw.post("/_gateway/upstreams/api/instances/u2/drain")
        barrier = threading.Event()

        def slow() -> None:
            barrier.wait()
            gw.get("/api/slow?delay=0.8", timeout=5)

        t = threading.Thread(target=slow)
        t.start()
        barrier.set()
        time.sleep(0.35)
        assert _instance_stats(gw, "u3")["inflight"] == 1

        # 恢复 u1/u2 的 url 供 payload 使用，reload 时它们重新纳入
        payload = {
            "rate_limit": {"rate_per_second": 100.0, "burst": 200},
            "upstreams": {
                "api": {
                    "instances": [
                        {"id": "u1", "url": u1_url},
                        {"id": "u2", "url": u2_url},
                    ],
                    "health_path": "/health",
                    "health_interval": 60.0,
                    "health_timeout": 0.5,
                    "max_inflight": 100,
                }
            },
            "routes": [
                {"id": "api", "prefix": "/api", "upstream": "api",
                 "strip_prefix": True, "timeout": 1.5, "max_retries": 2}
            ],
        }
        r = gw.post("/_gateway/reload", json=payload)
        assert r.status_code == 409
        body = r.json()
        assert "api/u3" in body["summary"]["instances_pending_drain"]
        # u3 仍在运行时池中（摘除状态），在途请求未被掐断
        assert _instance_stats(gw, "u3")["draining"] is True

        t.join(timeout=5)
        # 排空后再次 reload，u3 被真正移除
        r2 = gw.post("/_gateway/reload", json=payload)
        assert r2.status_code == 200
        ids = [i["id"] for i in gw.get("/_gateway/stats").json()["upstreams"][0]["instances"]]
        assert "u3" not in ids


# --------------------------------------------------------------------- #
# 场景三：反复故障注入与恢复后，流量回归平稳（可重复）
# --------------------------------------------------------------------- #


class TestRepeatedFailureRecovery:
    SCENARIO = "反复故障恢复"

    @pytest.mark.parametrize("cycle", range(3))
    def test_traffic_returns_after_each_recovery(self, gateway_factory,
                                                 upstream_calls, cycle):
        """对 u1 连续做 3 轮 fail -> 等待摘除 -> recover -> 等待回归，
        每轮恢复后 u1 都能重新拿到稳定份额的流量（不会进去一次再回不来）。"""
        gw = gateway_factory(
            failure_threshold=1,
            open_state_ttl=0.3,
            health_interval=0.1,
            warmup_seconds=0.0,
            rate=10000.0,
            burst=2000,
        )
        _wait_healthy(gw)

        # 基线：u1 有稳定流量
        base = Counter(_hit(gw)[1] for _ in range(30))
        assert base["u1"] == 10, base

        # 故障：u1 被摘，流量只去 u2/u3
        upstream_calls.set_mode("u1", "fail")
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if not _instance_stats(gw, "u1")["healthy"]:
                break
            time.sleep(0.05)
        during = Counter(_hit(gw)[1] for _ in range(20))
        assert "u1" not in during, during

        # 恢复：等待健康位回正，且有足够探测轮次驱动熔断 closed
        upstream_calls.set_mode("u1", "recover")
        deadline = time.monotonic() + 5
        recovered = False
        while time.monotonic() < deadline:
            stat = _instance_stats(gw, "u1")
            if stat["healthy"] and stat["breaker"]["state"] == "closed":
                recovered = True
                break
            time.sleep(0.05)
        assert recovered, "u1 恢复后健康位/熔断未收敛"

        # 回归：每轮恢复后 u1 都拿回稳定的 1/3 份额
        counts = Counter(_hit(gw)[1] for _ in range(30))
        assert counts == {"u1": 10, "u2": 10, "u3": 10}, (cycle, counts)

    def test_multiple_cycles_same_instance_always_comes_back(
        self, gateway_factory, upstream_calls
    ):
        """3 轮故障/恢复结束后，再跑大量请求，三实例分布依然平稳一致。"""
        gw = gateway_factory(
            failure_threshold=1,
            open_state_ttl=0.2,
            health_interval=0.08,
            warmup_seconds=0.0,
            rate=10000.0,
            burst=5000,
        )
        _wait_healthy(gw)

        def wait_state(inst, predicate, timeout=5.0):
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if predicate(_instance_stats(gw, inst)):
                    return True
                time.sleep(0.04)
            return False

        for cycle in range(3):
            upstream_calls.set_mode("u1", "fail")
            assert wait_state("u1", lambda s: not s["healthy"])
            upstream_calls.set_mode("u1", "recover")
            assert wait_state(
                "u1",
                lambda s: s["healthy"] and s["breaker"]["state"] == "closed",
            )

        counts = Counter(_hit(gw)[1] for _ in range(150))
        assert counts == {"u1": 50, "u2": 50, "u3": 50}, counts


# --------------------------------------------------------------------- #
# 场景四：恢复时预热爬升，流量逐步加回而非瞬间压满
# --------------------------------------------------------------------- #


class TestWarmupGradualReturn:
    SCENARIO = "预热渐进回归"

    def test_effective_weight_climbs_in_stats(self, gateway_factory):
        """resume 后 stats 中的 effective_weight 从初始比例逐步爬满。"""
        gw = gateway_factory(
            warmup_seconds=1.0,
            warmup_initial_ratio=0.2,
            health_interval=60.0,
        )
        _wait_healthy(gw)
        gw.post("/_gateway/upstreams/api/instances/u1/drain")
        gw.post("/_gateway/upstreams/api/instances/u1/resume")
        early = _instance_stats(gw, "u1")["effective_weight"]
        assert 0.19 <= early <= 0.3, early  # 起点约 0.2
        time.sleep(0.5)
        mid = _instance_stats(gw, "u1")["effective_weight"]
        assert 0.55 <= mid <= 0.7, mid
        time.sleep(0.65)
        late = _instance_stats(gw, "u1")["effective_weight"]
        assert late == 1.0, late

    def test_recovered_instance_gets_traffic_gradually(
        self, gateway_factory, upstream_calls
    ):
        """故障恢复后预热窗口内 u1 的份额小于稳态份额，窗口结束后回到稳态。"""
        gw = gateway_factory(
            weights={"u1": 1, "u2": 1, "u3": 1},
            failure_threshold=1,
            open_state_ttl=0.2,
            health_interval=0.08,
            warmup_seconds=2.0,
            warmup_initial_ratio=0.0,
            rate=10000.0,
            burst=5000,
        )
        _wait_healthy(gw)
        upstream_calls.set_mode("u1", "fail")
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            if not _instance_stats(gw, "u1")["healthy"]:
                break
            time.sleep(0.04)
        upstream_calls.set_mode("u1", "recover")
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            s = _instance_stats(gw, "u1")
            if s["healthy"] and s["breaker"]["state"] == "closed":
                break
            time.sleep(0.04)

        # 恢复初期（预热窗口前段）：u1 拿到的份额明显小于 1/3
        early = Counter(_hit(gw)[1] for _ in range(30))
        assert early["u1"] < 10, early
        assert early["u1"] >= 0

        # 预热窗口结束后：回到稳态 1/3
        time.sleep(2.2)
        steady = Counter(_hit(gw)[1] for _ in range(30))
        assert steady["u1"] == 10, steady


# --------------------------------------------------------------------- #
# 场景五：加权调度与限流 / 在途上限 / 熔断超时的协作
# --------------------------------------------------------------------- #


class TestSchedulerCooperation:
    SCENARIO = "限流/在途/熔断协作"

    def test_weighted_pick_at_capacity_skips_to_other(self, gateway_factory):
        """高权重实例在途打满时，请求在有界尝试内换到其他实例（用户无感）。

        u1 权重极高但 max_inflight=2：占满 2 个在途后，后续请求应全部落到
        u2/u3，而不是 503（只要还有别的实例有额度）。
        """
        gw = gateway_factory(
            weights={"u1": 100, "u2": 1, "u3": 1},
            max_inflight=2,
            timeout=5.0,
            max_retries=0,
            health_interval=60.0,
            rate=10000.0,
            burst=2000,
        )
        _wait_healthy(gw)

        picked: list[str] = []
        picked_lock = threading.Lock()
        barrier = threading.Event()

        def call(path: str) -> None:
            barrier.wait()
            r = gw.get(path)
            if r.status_code == 200:
                with picked_lock:
                    picked.append(r.json()["instance"])

        # 前 2 个慢请求必然命中权重最高的 u1
        slow_threads = [
            threading.Thread(target=call, args=("/api/slow?delay=0.8",))
            for _ in range(2)
        ]
        for t in slow_threads:
            t.start()
        barrier.set()
        time.sleep(0.4)
        assert _instance_stats(gw, "u1")["inflight"] == 2

        # u1 满了：新请求必须换到 u2/u3（有界尝试内完成），不会被 503
        fast = [_hit(gw) for _ in range(6)]
        insts = {i for _, i in fast if i}
        assert insts <= {"u2", "u3"}, insts
        assert insts, "u1 满时请求应被换到 u2/u3"

        for t in slow_threads:
            t.join(timeout=5)

    def test_rate_limit_still_distinct_when_weighted(self, gateway_factory):
        """加权配置下限流仍是 429，且不被换实例逻辑绕过。"""
        gw = gateway_factory(
            weights={"u1": 5, "u2": 1, "u3": 1},
            rate=0.0,
            burst=2,
            health_interval=60.0,
        )
        _wait_healthy(gw)
        codes = [gw.get("/api/echo").status_code for _ in range(4)]
        assert codes.count(200) == 2
        assert codes.count(429) == 2

    def test_no_upstream_503_when_all_weight_zero(self, gateway_factory):
        """全部实例权重 0：无可用上游，仍然 503。"""
        gw = gateway_factory(health_interval=60.0)
        _wait_healthy(gw)
        for inst in ("u1", "u2", "u3"):
            gw.post(
                f"/_gateway/upstreams/api/instances/{inst}/weight",
                json={"weight": 0},
            )
        r = gw.get("/api/echo")
        assert r.status_code == 503
        assert r.json()["error"]["reason"] == "no_upstream_available"

    def test_timeout_504_unchanged_with_weighted_pool(self, gateway_factory):
        """加权池中所有实例慢：仍返回 504（超时与失败可区分）。"""
        gw = gateway_factory(
            weights={"u1": 5, "u2": 1, "u3": 1},
            timeout=0.3,
            max_retries=0,
            health_interval=60.0,
        )
        _wait_healthy(gw)
        r = gw.get("/api/slow?delay=3", timeout=8)
        assert r.status_code == 504
        assert r.json()["error"]["reason"] == "upstream_timeout"

    def test_drain_all_then_503_does_not_consume_extra_quota(self, gateway_factory):
        """全部摘除后连续请求都是 503；恢复一个后立即恢复服务。"""
        gw = gateway_factory(
            rate=10000.0, burst=2000, health_interval=60.0
        )
        _wait_healthy(gw)
        for inst in ("u1", "u2", "u3"):
            gw.post(f"/_gateway/upstreams/api/instances/{inst}/drain")
        for _ in range(3):
            assert gw.get("/api/echo").status_code == 503
        gw.post("/_gateway/upstreams/api/instances/u2/resume")
        assert gw.get("/api/echo").status_code == 200
