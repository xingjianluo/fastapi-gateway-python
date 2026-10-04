"""按权重分配流量、实例摘除/重新纳入、爬坡恢复的集成测试。

场景总览：
- TestWeightDistribution      权重分配偏差（含并发压力、无饿死）
- TestZeroWeight              权重 0：不接新流量但仍参与健康检查
- TestRuntimeWeightUpdate     运行时调权/摘除/纳入，无需重启
- TestDrainInflight           摘除时在途请求正常跑完
- TestWarmupRamp              恢复后流量逐步爬坡而非瞬间打满
- TestRepeatedFaultRecovery   反复注入故障再恢复，流量回归一致
- TestWeightWithLimits        权重调度与限流/在途上限的配合
"""

from __future__ import annotations

import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

# 分布统计类用例会快速打几百个请求，限流配额放大到不干扰观测
_BIG_QUOTA = {"rate": 100000.0, "burst": 100000}


def _instances(gw, n: int, path: str = "/api/echo") -> list[str]:
    return [gw.get(path).json()["instance"] for _ in range(n)]


def _counts(gw, n: int, path: str = "/api/echo") -> Counter:
    return Counter(_instances(gw, n, path))


def _stats_by_id(gw) -> dict[str, dict]:
    stats = gw.get("/_gateway/stats").json()
    return {
        inst["id"]: inst
        for up in stats["upstreams"]
        for inst in up["instances"]
    }


def _set_weight(gw, inst: str, weight: int):
    return gw.post(f"/_gateway/upstreams/api/instances/{inst}/weight",
                   json={"weight": weight})


class TestWeightDistribution:
    def test_traffic_split_matches_weights(self, gateway_factory):
        """权重 1:2:3：300 个请求大致按 50/100/150 分布，偏差有界。"""
        gw = gateway_factory(
            weights={"u1": 1, "u2": 2, "u3": 3}, health_interval=60.0,
            **_BIG_QUOTA,
        )
        time.sleep(0.3)
        counts = _counts(gw, 300)
        assert abs(counts["u1"] - 50) <= 10, counts
        assert abs(counts["u2"] - 100) <= 15, counts
        assert abs(counts["u3"] - 150) <= 20, counts

    def test_skewed_weights_no_starvation(self, gateway_factory):
        """权重 1:1:8：小权重实例不被饿死，大权重实例确实拿到大头。"""
        gw = gateway_factory(
            weights={"u1": 1, "u2": 1, "u3": 8}, health_interval=60.0,
            **_BIG_QUOTA,
        )
        time.sleep(0.3)
        counts = _counts(gw, 200)
        assert counts["u1"] >= 10, counts  # 期望 20，绝不为 0
        assert counts["u2"] >= 10, counts
        assert counts["u3"] >= 120, counts  # 期望 160，权重不是摆设

    def test_distribution_holds_under_concurrency(self, gateway_factory):
        """16 线程并发打 300 个请求：分布仍贴近权重且无人被饿死。"""
        gw = gateway_factory(
            weights={"u1": 1, "u2": 2, "u3": 3}, health_interval=60.0,
            **_BIG_QUOTA,
        )
        time.sleep(0.3)
        results: list[str] = []
        lock = threading.Lock()

        def one(_: int) -> None:
            inst = gw.get("/api/echo").json()["instance"]
            with lock:
                results.append(inst)

        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(one, range(300)))
        counts = Counter(results)
        assert abs(counts["u1"] - 50) <= 15, counts
        assert abs(counts["u2"] - 100) <= 20, counts
        assert abs(counts["u3"] - 150) <= 25, counts


class TestZeroWeight:
    def test_zero_weight_no_traffic_but_health_checked(self, gateway_factory):
        """权重 0：新流量为 0，但健康探测仍在跑（stats 中 healthy=True）。"""
        gw = gateway_factory(
            weights={"u1": 1, "u2": 1, "u3": 0}, health_interval=0.2,
            **_BIG_QUOTA,
        )
        time.sleep(0.4)
        counts = _counts(gw, 100)
        assert "u3" not in counts
        snap = _stats_by_id(gw)
        assert snap["u3"]["healthy"] is True
        assert snap["u3"]["weight"] == 0
        assert snap["u3"]["effective_weight"] == 0.0


class TestRuntimeWeightUpdate:
    def test_weight_update_without_restart(self, gateway_factory):
        """运行时调权立即生效：u3 调 0 后无流量，调大后拿走大头。"""
        gw = gateway_factory(health_interval=60.0, **_BIG_QUOTA)
        time.sleep(0.3)
        r = _set_weight(gw, "u3", 0)
        assert r.status_code == 200
        assert r.json()["weight"] == 0
        counts = _counts(gw, 60)
        assert "u3" not in counts

        r = _set_weight(gw, "u3", 8)
        assert r.status_code == 200
        counts = _counts(gw, 100)
        assert counts["u3"] >= 60, counts  # 8:1:1 -> 期望 80

    def test_weight_admin_validation(self, gateway_factory):
        """控制面校验：未知实例 404、非法权重 400。"""
        gw = gateway_factory(health_interval=60.0)
        time.sleep(0.3)
        assert _set_weight(gw, "ghost", 1).status_code == 404
        assert _set_weight(gw, "u1", -1).status_code == 400
        assert gw.post(
            "/_gateway/upstreams/api/instances/u1/weight", json={}
        ).status_code == 400
        assert gw.post(
            "/_gateway/upstreams/nope/instances/u1/weight",
            json={"weight": 1},
        ).status_code == 404

    def test_remove_and_reinclude_via_admin(self, gateway_factory):
        """摘除后新请求不再落入；重新纳入后恢复参与，可反复操作。"""
        gw = gateway_factory(health_interval=60.0, **_BIG_QUOTA)
        time.sleep(0.3)
        for _ in range(2):  # 反复两轮，行为一致
            r = gw.post("/_gateway/upstreams/api/instances/u1/remove")
            assert r.status_code == 200 and r.json()["removed"] is True
            counts = _counts(gw, 40)
            assert "u1" not in counts
            r = gw.post("/_gateway/upstreams/api/instances/u1/reinclude")
            assert r.status_code == 200 and r.json()["removed"] is False
            counts = _counts(gw, 60)
            assert counts["u1"] >= 10, counts  # 等权重，期望约 20


class TestDrainInflight:
    def test_inflight_request_completes_when_instance_removed(
        self, gateway_factory
    ):
        """摘除瞬间在途的慢请求正常跑完，新请求不再落到被摘除实例。"""
        gw = gateway_factory(
            weights={"u1": 1, "u2": 0, "u3": 0},
            timeout=5.0,
            max_retries=0,
            health_interval=60.0,
            **_BIG_QUOTA,
        )
        time.sleep(0.3)
        barrier = threading.Barrier(2)
        outcome: dict[str, object] = {}

        def slow_call() -> None:
            barrier.wait()
            r = gw.get("/api/slow?delay=1.0", timeout=10)
            outcome["status"] = r.status_code
            outcome["instance"] = r.json()["instance"]

        t = threading.Thread(target=slow_call)
        t.start()
        barrier.wait()
        time.sleep(0.3)  # 确认慢请求已在 u1 在途
        assert _stats_by_id(gw)["u1"]["inflight"] == 1

        # 摘除 u1：在途请求必须正常完成
        gw.post("/_gateway/upstreams/api/instances/u1/remove")
        t.join(timeout=8)
        assert outcome["status"] == 200
        assert outcome["instance"] == "u1"

        # 新请求不再落到 u1（其他实例权重 0，暂时无可用上游）
        r = gw.get("/api/echo")
        assert r.status_code == 503
        assert r.json()["error"]["reason"] == "no_upstream_available"

        # 把流量引到 u2/u3 后，u1 仍被排除在外
        _set_weight(gw, "u2", 1)
        _set_weight(gw, "u3", 1)
        counts = _counts(gw, 40)
        assert "u1" not in counts
        assert counts["u2"] > 0 and counts["u3"] > 0


class TestWarmupRamp:
    def test_traffic_ramps_gradually_after_reinclude(self, gateway_factory):
        """重新纳入后流量逐步爬坡：早期份额远低于稳态，稳态回到均分。"""
        gw = gateway_factory(
            weight_warmup=3.0, health_interval=60.0, **_BIG_QUOTA
        )
        time.sleep(0.3)
        gw.post("/_gateway/upstreams/api/instances/u1/remove")
        gw.post("/_gateway/upstreams/api/instances/u1/reinclude")

        # 爬坡早期：u1 有效权重从 10% 起步，份额远小于 1/3
        early = _counts(gw, 60)
        assert early["u1"] < 15, early

        # 爬坡结束后：回到约 1/3
        time.sleep(3.2)
        steady = _counts(gw, 300)
        assert abs(steady["u1"] - 100) <= 25, steady


class TestRepeatedFaultRecovery:
    def test_repeated_faults_converge_to_same_distribution(
        self, gateway_factory, upstream_calls
    ):
        """反复注入故障再恢复 3 轮：每轮恢复并爬坡结束后，分布一致且贴近均分。"""
        gw = gateway_factory(
            weight_warmup=0.5,
            failure_threshold=1,
            open_state_ttl=0.3,
            health_interval=0.15,
            **_BIG_QUOTA,
        )
        time.sleep(0.4)
        shares: list[float] = []
        for round_no in range(3):
            # 注入故障：等待健康探测摘除 u1
            upstream_calls.set_mode("u1", "fail")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                if not _stats_by_id(gw)["u1"]["healthy"]:
                    break
                time.sleep(0.05)
            assert not _stats_by_id(gw)["u1"]["healthy"]
            counts = _counts(gw, 40)
            assert "u1" not in counts

            # 恢复：等待重新纳入并越过爬坡期
            upstream_calls.set_mode("u1", "recover")
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                snap = _stats_by_id(gw)["u1"]
                if snap["healthy"] and snap["breaker"]["state"] == "closed":
                    break
                time.sleep(0.05)
            time.sleep(0.7)  # 越过 weight_warmup=0.5 的爬坡期

            counts = _counts(gw, 150)
            share = counts["u1"] / 150
            shares.append(share)
            assert 0.23 <= share <= 0.43, (round_no, counts)

        # 多轮结果一致：没有「进去一次就回不来」或逐轮衰减
        assert max(shares) - min(shares) <= 0.15, shares


class TestWeightWithLimits:
    def test_weighted_pick_respects_inflight_cap(self, gateway_factory):
        """权重最高的实例在途占满时，有界换到其他实例；单实例在途不越界。"""
        gw = gateway_factory(
            weights={"u1": 100, "u2": 1, "u3": 1},
            max_inflight=1,
            timeout=5.0,
            max_retries=0,
            health_interval=60.0,
            **_BIG_QUOTA,
        )
        time.sleep(0.3)
        barrier = threading.Barrier(4)
        results: list[tuple[int, str]] = []
        lock = threading.Lock()

        def slow_call(_: int) -> None:
            barrier.wait()
            r = gw.get("/api/slow?delay=1.0", timeout=10)
            inst = r.json().get("instance", "-") if r.status_code == 200 else "-"
            with lock:
                results.append((r.status_code, inst))

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(slow_call, range(4)))

        codes = [c for c, _ in results]
        served = [inst for c, inst in results if c == 200]
        # 容量 3：3 个成功且分别落在 3 个不同实例（高权重的 u1 占满后
        # 溢出到 u2/u3），第 4 个立即 503，没有任何实例在途越界
        assert codes.count(200) == 3, results
        assert codes.count(503) == 1, results
        assert sorted(served) == ["u1", "u2", "u3"], results

    def test_fallback_does_not_consume_extra_rate_quota(self, gateway_factory):
        """在途占满触发的实例切换不额外消耗客户端限流配额。"""
        gw = gateway_factory(
            weights={"u1": 100, "u2": 1, "u3": 1},
            max_inflight=1,
            rate=0.0,
            burst=3,
            timeout=5.0,
            max_retries=0,
            health_interval=60.0,
        )
        time.sleep(0.3)
        barrier = threading.Barrier(3)
        codes: list[int] = []
        lock = threading.Lock()

        def slow_call(_: int) -> None:
            barrier.wait()
            r = gw.get("/api/slow?delay=1.0", timeout=10)
            with lock:
                codes.append(r.status_code)

        with ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(slow_call, range(3)))
        # 3 个请求各消耗 1 个令牌且全部成功：故障转移没有多扣配额
        assert codes == [200, 200, 200], codes
        # 配额恰好耗尽：第 4 个请求被限流 429（而非其他错误）
        r = gw.get("/api/echo")
        assert r.status_code == 429
        assert r.json()["error"]["reason"] == "rate_limited"

    def test_all_zero_weight_is_503_not_500(self, gateway_factory):
        """全部权重调 0：对外仍是可区分的 503 no_upstream_available。"""
        gw = gateway_factory(health_interval=60.0, **_BIG_QUOTA)
        time.sleep(0.3)
        for inst in ("u1", "u2", "u3"):
            assert _set_weight(gw, inst, 0).status_code == 200
        r = gw.get("/api/echo")
        assert r.status_code == 503
        assert r.json()["error"]["reason"] == "no_upstream_available"
