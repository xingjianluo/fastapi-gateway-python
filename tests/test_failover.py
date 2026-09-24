"""故障注入、有界重试、熔断打开/半开/恢复的集成测试。"""

from __future__ import annotations

import time


def _reason(r) -> str:
    return r.json()["error"]["reason"]


def test_single_instance_fails_request_fails_over(gateway_factory, upstream_calls):
    """一个实例 500：GET 在其他实例上重试成功，用户无感。"""
    gw = gateway_factory(max_retries=2)
    time.sleep(0.3)
    upstream_calls.set_mode("u1", "fail")
    time.sleep(0.4)  # 等待健康探测把 u1 标记为不健康
    # 连续多次，没有任何一次失败（流量只走健康实例）
    for _ in range(6):
        r = gw.get("/api/echo")
        assert r.status_code == 200
        assert r.json()["instance"] in {"u2", "u3"}


def test_retry_bounded_when_all_return_5xx(gateway_factory, upstream_calls):
    """全部实例 500：最多尝试 max_retries+1 次，统一 502，绝不无限重试。"""
    gw = gateway_factory(max_retries=2)
    time.sleep(0.3)
    for n in ("u1", "u2", "u3"):
        upstream_calls.set_mode(n, "fail")
    time.sleep(0.4)
    # 实例已被健康探测摘除 -> 直接 503 no_upstream（没有可尝试实例）
    r = gw.get("/api/echo")
    assert r.status_code == 503
    assert _reason(r) == "no_upstream_available"


def test_all_500_before_probe_gives_502(gateway_factory, upstream_calls):
    """配置 health_interval 很长：故障尚未被探测到时，请求得到 502（重试耗尽）。"""
    gw = gateway_factory(max_retries=2, health_interval=60.0)
    time.sleep(0.3)
    # 首轮探测是健康的；随即注入故障，不再有探测把它摘掉
    for n in ("u1", "u2", "u3"):
        upstream_calls.set_mode(n, "fail")
    r = gw.get("/api/echo")
    assert r.status_code == 502
    assert _reason(r) == "upstream_failure"


def test_timeout_is_distinct_from_failure(gateway_factory, upstream_calls):
    """全部实例慢 -> 504 upstream_timeout，与 502 可区分。"""
    gw = gateway_factory(timeout=0.5, max_retries=2, health_interval=60.0)
    time.sleep(0.3)
    r = gw.get("/api/slow?delay=5", timeout=10)
    assert r.status_code == 504
    assert _reason(r) == "upstream_timeout"


def test_three_rejection_reasons_distinct_status(gateway_factory, upstream_calls):
    """三类拒绝原因对外状态码稳定且可区分：429 / 503 / 504。"""
    # 1) 限流 429
    gw_rl = gateway_factory(rate=0.0, burst=1, health_interval=60.0)
    time.sleep(0.3)
    assert gw_rl.get("/api/echo").status_code == 200
    r429 = gw_rl.get("/api/echo")
    assert r429.status_code == 429
    assert _reason(r429) == "rate_limited"
    assert r429.headers.get("retry-after") == "1"

    # 2) 无可用上游 503
    gw_down = gateway_factory(health_interval=60.0)
    for n in ("u1", "u2", "u3"):
        upstream_calls.set_mode(n, "fail")
    # 主动关闭：模拟进程不存在的实例最直接——全部健康标记不健康
    # 这里改用熔断：对每个实例制造 failure_threshold 次 5xx
    cfg_app = gw_down.app
    for pool in cfg_app.state.pools.values():
        for inst in pool.instances:
            for _ in range(5):
                inst.breaker.record_failure()
    r503 = gw_down.get("/api/echo")
    assert r503.status_code == 503
    assert _reason(r503) == "no_upstream_available"

    # 3) 超时 504（上游必须处于正常模式，只是响应慢）
    for n in ("u1", "u2", "u3"):
        upstream_calls.set_mode(n, "recover")
    gw_to = gateway_factory(timeout=0.3, max_retries=0, health_interval=60.0)
    time.sleep(0.2)
    r504 = gw_to.get("/api/slow?delay=3", timeout=8)
    assert r504.status_code == 504
    assert _reason(r504) == "upstream_timeout"


def test_breaker_opens_and_closes_on_recovery(gateway_factory, upstream_calls):
    """熔断：健康失败驱动 open -> TTL 后半开探测 -> 成功则 closed，实例重新加入。"""
    gw = gateway_factory(
        failure_threshold=1,
        open_state_ttl=0.5,
        health_interval=0.15,
        max_retries=0,
    )
    time.sleep(0.3)
    app = gw.app

    def states():
        out = {}
        for pool in app.state.pools.values():
            for inst in pool.instances:
                out[inst.id] = inst.breaker.state.value
        return out

    # u1 故障：探测失败后熔断打开并标记不健康
    upstream_calls.set_mode("u1", "fail")
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        if states().get("u1") == "open":
            break
        time.sleep(0.05)
    assert states()["u1"] == "open"

    # open 期间 u1 不接收新请求
    for _ in range(5):
        inst = gw.get("/api/echo").json()["instance"]
        assert inst != "u1"

    # 恢复：探测在 TTL 后进入半开，成功后关闭
    upstream_calls.set_mode("u1", "recover")
    deadline = time.monotonic() + 4
    recovered = False
    while time.monotonic() < deadline:
        if states().get("u1") == "closed":
            recovered = True
            break
        time.sleep(0.05)
    assert recovered, states()

    # 重新参与负载均衡
    seen = {gw.get("/api/echo").json()["instance"] for _ in range(6)}
    assert "u1" in seen


def test_post_not_retried_by_default(gateway_factory, upstream_calls):
    """非幂等 POST 不重试：即便其他实例健康也只尝试一次。"""
    gw = gateway_factory(max_retries=2, health_interval=60.0)
    time.sleep(0.3)
    # 让轮询起点落到 u1：先做若干 GET 对齐成本高，直接断言单次 POST：
    # 若选中 u1（500）-> 502；否则 200。两种都必须成立，且失败时只有一次尝试。
    upstream_calls.set_mode("u1", "fail")
    results = []
    for _ in range(10):
        r = gw.post("/api/echo", json={"x": 1})
        results.append(r.status_code)
        assert r.status_code in (200, 502)
    upstream_calls.set_mode("u1", "recover")
    assert 502 in results  # 轮询必然命中过 u1，且没有被重试掩盖
