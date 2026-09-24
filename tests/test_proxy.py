"""Proxy layer: header filtering, bounded retries, stable rejection reasons."""

import logging

import httpx
import pytest
from starlette.datastructures import Headers as StarletteHeaders

from gateway.config import CircuitBreakerConfig, RouteConfig, UpstreamConfig
from gateway.errors import RejectReason
from gateway.proxy import (
    GatewayError,
    build_upstream_url,
    filter_request_headers,
    filter_response_headers,
    forward_with_retries,
)
from gateway.router import Route

logger = logging.getLogger("test.proxy")


def make_route(max_retries: int = 1, n_upstreams: int = 2, timeout: float = 1.0) -> Route:
    return Route(RouteConfig(
        prefix="/api", strip_prefix=True, max_retries=max_retries,
        request_timeout_seconds=timeout,
        circuit_breaker=CircuitBreakerConfig(failure_threshold=10,
                                             reset_timeout_seconds=60),
        upstreams=[UpstreamConfig(name=f"u{i}", base_url=f"http://u{i}")
                   for i in range(n_upstreams)],
    ))


def test_strip_hop_by_hop_and_connection_nominated_headers():
    headers = StarletteHeaders(raw=[
        (b"connection", b"keep-alive, x-secret"),
        (b"x-secret", b"leak"),
        (b"keep-alive", b"timeout=5"),
        (b"transfer-encoding", b"chunked"),
        (b"host", b"gateway"),
        (b"content-length", b"10"),
        (b"x-app", b"keep-me"),
        (b"authorization", b"Bearer t"),
    ])
    filtered = filter_request_headers(headers)
    assert filtered == {"x-app": "keep-me", "authorization": "Bearer t"}


def test_response_headers_strip_hop_by_hop_keep_cache_control():
    headers = httpx.Headers({
        "connection": "close",
        "transfer-encoding": "chunked",
        "cache-control": "no-store",
        "content-type": "application/json",
    })
    filtered = filter_response_headers(headers)
    assert filtered == {"cache-control": "no-store",
                        "content-type": "application/json"}


def test_build_upstream_url_strips_prefix_and_keeps_query():
    route = make_route()
    assert build_upstream_url(route, "/api/users/1", "a=1&b=2") == "/users/1?a=1&b=2"
    assert build_upstream_url(route, "/api", "") == "/"


async def test_failover_to_second_upstream_on_500():
    """First upstream 500s; request must be retried on the next one."""
    calls: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.host)
        if request.url.host == "u0":
            return httpx.Response(500, json={"error": "boom"})
        return httpx.Response(200, json={"ok": True})

    route = make_route(max_retries=1)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    resp = await forward_with_retries(client, route, "GET", "/api/x", "", {}, b"", "r1")
    assert resp.status_code == 200
    assert calls == ["u0", "u1"]
    logger.info("failover: attempts=%s final=%d", calls, resp.status_code)
    await client.aclose()


async def test_retries_are_bounded():
    """Attempts must never exceed 1 + max_retries, even if all fail."""
    calls = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(500)

    route = make_route(max_retries=2, n_upstreams=2)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    resp = await forward_with_retries(client, route, "GET", "/api/x", "", {}, b"", "r2")
    assert resp.status_code == 500  # last 5xx passed through, never as success
    assert calls == 3  # 1 + max_retries, bounded
    logger.info("bounded retries: attempts=%d (max 3)", calls)
    await client.aclose()


async def test_timeout_maps_to_stable_reason():
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow", request=request)

    route = make_route(max_retries=1, n_upstreams=1)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(GatewayError) as exc_info:
        await forward_with_retries(client, route, "GET", "/api/x", "", {}, b"", "r3")
    assert exc_info.value.reason is RejectReason.UPSTREAM_TIMEOUT
    logger.info("timeout rejection reason=%s", exc_info.value.reason.value)
    await client.aclose()


async def test_connect_error_maps_to_upstream_error():
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    route = make_route(max_retries=0, n_upstreams=1)
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    with pytest.raises(GatewayError) as exc_info:
        await forward_with_retries(client, route, "GET", "/api/x", "", {}, b"", "r4")
    assert exc_info.value.reason is RejectReason.UPSTREAM_ERROR
    await client.aclose()


async def test_no_available_upstream():
    route = make_route(max_retries=1, n_upstreams=1)
    route.upstreams[0].healthy = False
    client = httpx.AsyncClient(transport=httpx.MockTransport(
        lambda r: httpx.Response(200)))
    with pytest.raises(GatewayError) as exc_info:
        await forward_with_retries(client, route, "GET", "/api/x", "", {}, b"", "r5")
    assert exc_info.value.reason is RejectReason.NO_HEALTHY_UPSTREAM
    await client.aclose()
