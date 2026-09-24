"""End-to-end gateway tests against real local upstream servers."""

import asyncio
import logging

from gateway.circuitbreaker import CircuitState

from .conftest import gateway_client, make_config

logger = logging.getLogger("test.gateway")


async def test_round_robin_and_request_forwarding(upstream_factory, caplog):
    a = upstream_factory("rr-a")
    b = upstream_factory("rr-b")
    config = make_config([a, b])
    with caplog.at_level(logging.INFO, logger="gateway.router"):
        async with gateway_client(config) as (client, _):
            names = []
            for _ in range(4):
                resp = await client.post("/api/items?x=1", content="payload",
                                         headers={"x-custom": "v"})
                assert resp.status_code == 200
                body = resp.json()
                assert body["method"] == "POST"
                assert body["query"] == "x=1"
                assert body["body"] == "payload"
                assert body["headers"].get("x-custom") == "v"
                assert "x-request-id" in resp.headers
                names.append(body["upstream"])
    assert sorted(names) == ["rr-a", "rr-a", "rr-b", "rr-b"]
    assert any("selected upstream" in r.message for r in caplog.records)
    logger.info("round-robin distribution: %s", names)


async def test_fault_injection_failover_and_recovery(upstream_factory, caplog):
    a = upstream_factory("fo-a")
    b = upstream_factory("fo-b")
    config = make_config([a, b], health_interval=0.2)
    with caplog.at_level(logging.INFO, logger="gateway.health"):
        async with gateway_client(config) as (client, _):
            a.set_fault(True)
            await asyncio.sleep(0.6)  # let the prober mark it unhealthy
            for _ in range(4):
                resp = await client.get("/api/data")
                assert resp.status_code == 200
                assert resp.json()["upstream"] == "fo-b"
            a.set_fault(False)
            await asyncio.sleep(0.6)  # prober detects recovery
            seen = {(await client.get("/api/data")).json()["upstream"] for _ in range(4)}
            assert seen == {"fo-a", "fo-b"}
    health_logs = [r.message for r in caplog.records
                   if r.name == "gateway.health"]
    assert any("marked unhealthy" in m for m in health_logs)
    assert any("recovered" in m for m in health_logs)
    logger.info("failover+recovery verified; health log: %s", health_logs)


async def test_concurrent_rate_limit_quota_not_exceeded(upstream_factory):
    a = upstream_factory("rl-a")
    config = make_config([a], rate=0.0001, burst=5)
    async with gateway_client(config) as (client, _):
        responses = await asyncio.gather(*[client.get("/api/x") for _ in range(30)])
    ok = [r for r in responses if r.status_code == 200]
    limited = [r for r in responses if r.status_code == 429]
    assert len(ok) == 5
    assert len(limited) == 25
    for r in limited:
        err = r.json()["error"]
        assert err["reason"] == "rate_limited"
        assert r.headers["cache-control"] == "no-store"
        assert r.headers["x-request-id"] == err["request_id"]
    logger.info("rate limit under concurrency: allowed=%d rejected=%d",
                len(ok), len(limited))


async def test_upstream_timeout_rejection(upstream_factory):
    a = upstream_factory("to-a")
    config = make_config([a], timeout=0.3, max_retries=0)
    async with gateway_client(config) as (client, _):
        resp = await client.get("/api/slow?delay=2")
    assert resp.status_code == 504
    assert resp.json()["error"]["reason"] == "upstream_timeout"
    logger.info("timeout rejection: 504 upstream_timeout")


async def test_no_healthy_upstream_rejection(upstream_factory):
    a = upstream_factory("nh-a")
    b = upstream_factory("nh-b")
    config = make_config([a, b], health_interval=0.2, max_retries=0)
    async with gateway_client(config) as (client, _):
        a.set_fault(True)
        b.set_fault(True)
        await asyncio.sleep(0.6)
        resp = await client.get("/api/data")
    assert resp.status_code == 503
    assert resp.json()["error"]["reason"] == "no_healthy_upstream"
    logger.info("no-upstream rejection: 503 no_healthy_upstream")


async def test_in_flight_cap_rejects_and_does_not_leak(upstream_factory):
    a = upstream_factory("busy-a")
    config = make_config([a], max_in_flight=1, max_retries=0)
    async with gateway_client(config) as (client, app):
        responses = await asyncio.gather(
            *[client.get("/api/slow?delay=0.4") for _ in range(6)])
        ok = [r for r in responses if r.status_code == 200]
        busy = [r for r in responses if r.status_code == 503]
        assert len(ok) == 1
        assert len(busy) == 5
        for r in busy:
            assert r.json()["error"]["reason"] == "upstream_busy"
        # after all requests settle, no in-flight count may leak
        route = app.state.router.routes[0]
        assert route.upstreams[0].in_flight == 0
    logger.info("in-flight cap: ok=%d busy=%d leaked=%d",
                len(ok), len(busy), route.upstreams[0].in_flight)


async def test_circuit_breaker_opens_and_recovers(upstream_factory, caplog):
    a = upstream_factory("cb-a")
    # huge health interval: only the breaker governs availability here
    config = make_config([a], max_retries=0, cb_failure_threshold=2,
                         cb_reset_timeout=0.4, health_interval=60.0)
    async with gateway_client(config) as (client, app):
        route = app.state.router.routes[0]
        await asyncio.sleep(0.2)  # let the initial probe finish (upstream healthy)
        with caplog.at_level(logging.INFO, logger="gateway.circuitbreaker"):
            a.set_fault(True)
            r1 = await client.get("/api/flaky")
            r2 = await client.get("/api/flaky")
            assert (r1.status_code, r2.status_code) == (500, 500)
            assert route.upstreams[0].breaker.state is CircuitState.OPEN
            # open circuit rejects fast, distinguishable from upstream 500
            r3 = await client.get("/api/flaky")
            assert r3.status_code == 503
            assert r3.json()["error"]["reason"] == "no_healthy_upstream"
            a.set_fault(False)
            await asyncio.sleep(0.5)  # reset timeout elapses
            r4 = await client.get("/api/flaky")  # half-open probe
            assert r4.status_code == 200
            assert route.upstreams[0].breaker.state is CircuitState.CLOSED
    transitions = [r.message for r in caplog.records
                   if r.name == "gateway.circuitbreaker"]
    assert any("closed -> open" in m for m in transitions)
    assert any("open -> half_open" in m for m in transitions)
    assert any("half_open -> closed" in m for m in transitions)
    logger.info("breaker transitions: %s", transitions)


async def test_no_route_rejection(upstream_factory):
    a = upstream_factory("nr-a")
    config = make_config([a], prefix="/api")
    async with gateway_client(config) as (client, _):
        resp = await client.get("/other/path")
    assert resp.status_code == 404
    assert resp.json()["error"]["reason"] == "no_route"


async def test_rejection_reasons_are_distinguishable(upstream_factory):
    """429 / 503 / 504 carry stable, distinct reason codes."""
    reasons: set[tuple[int, str]] = set()

    # scenario 1: upstream timeout -> 504 upstream_timeout
    a = upstream_factory("dist-a")
    async with gateway_client(make_config([a], timeout=0.3, max_retries=0)) as (client, _):
        r = await client.get("/api/slow?delay=2")
        reasons.add((r.status_code, r.json()["error"]["reason"]))

    # scenario 2: quota exhausted -> 429 rate_limited
    b = upstream_factory("dist-b")
    async with gateway_client(make_config([b], rate=0.0001, burst=1)) as (client, _):
        await client.get("/api/ok")  # consumes the only token
        r = await client.get("/api/ok")
        reasons.add((r.status_code, r.json()["error"]["reason"]))

    # scenario 3: all upstreams down -> 503 no_healthy_upstream
    c = upstream_factory("dist-c")
    async with gateway_client(make_config([c], health_interval=0.2)) as (client, _):
        c.set_fault(True)
        await asyncio.sleep(0.6)  # prober marks it down
        r = await client.get("/api/ok")
        reasons.add((r.status_code, r.json()["error"]["reason"]))

    assert (504, "upstream_timeout") in reasons
    assert (429, "rate_limited") in reasons
    assert (503, "no_healthy_upstream") in reasons
    logger.info("distinguishable rejections observed: %s", sorted(reasons))
