"""Upstream in-flight accounting: bounded, no leaks, cancellation-safe."""

import asyncio
import logging

import httpx
import pytest

from gateway.config import CircuitBreakerConfig, RouteConfig, UpstreamConfig
from gateway.router import Route
from gateway.upstream import Upstream, UpstreamBusyError
from gateway.proxy import send_to_upstream

logger = logging.getLogger("test.upstream")


def make_upstream(max_in_flight: int = 2) -> Upstream:
    return Upstream(
        UpstreamConfig(name="u", base_url="http://x", max_in_flight=max_in_flight),
        CircuitBreakerConfig(failure_threshold=3, reset_timeout_seconds=1.0),
    )


def test_in_flight_cap_and_release():
    u = make_upstream(2)
    u.acquire()
    u.acquire()
    with pytest.raises(UpstreamBusyError):
        u.acquire()
    u.release()
    u.acquire()  # slot freed
    u.release()
    u.release()
    u.release()  # extra release must not drive count negative
    assert u.in_flight == 0


async def test_in_flight_released_on_cancellation():
    """Client disconnect (task cancellation) must not leak in-flight count."""
    u = make_upstream(1)
    route = Route(RouteConfig(
        prefix="/api", request_timeout_seconds=5,
        upstreams=[UpstreamConfig(name="u", base_url="http://x")],
    ))

    async def slow_handler(request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(30)
        return httpx.Response(200)

    client = httpx.AsyncClient(transport=httpx.MockTransport(slow_handler))
    task = asyncio.create_task(send_to_upstream(
        client, u, route, "GET", "/api/slow", "", {}, b"", "req-1"))
    await asyncio.sleep(0.05)
    assert u.in_flight == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert u.in_flight == 0
    logger.info("in-flight count after client disconnect: %d (no leak)", u.in_flight)
    await client.aclose()
