"""Shared fixtures: real local upstream servers + in-process gateway client."""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager

import httpx
import pytest
import uvicorn

from gateway.app import create_app
from gateway.config import (
    CircuitBreakerConfig,
    GatewayConfig,
    HealthCheckConfig,
    RateLimitConfig,
    RouteConfig,
    UpstreamConfig,
)
from upstream_app import create_upstream


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class RunningUpstream:
    """A demo upstream app served by uvicorn in a daemon thread."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.app = create_upstream(name)
        self.port = free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        config = uvicorn.Config(self.app, host="127.0.0.1", port=self.port,
                                log_level="warning")
        self._server = uvicorn.Server(config)
        self._thread = threading.Thread(target=self._server.run, daemon=True)

    def start(self) -> None:
        self._thread.start()
        deadline = time.monotonic() + 10
        while not self._server.started:
            if time.monotonic() > deadline:
                raise RuntimeError(f"upstream {self.name} did not start")
            time.sleep(0.01)

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(timeout=10)

    def set_fault(self, enabled: bool) -> None:
        self.app.state.fault_enabled = enabled


@pytest.fixture()
def upstream_factory() -> Iterator:
    """Creates real upstream servers; tears them all down after the test."""
    created: list[RunningUpstream] = []

    def make(name: str) -> RunningUpstream:
        up = RunningUpstream(name)
        up.start()
        created.append(up)
        return up

    yield make
    for up in created:
        up.stop()


def make_config(upstreams: list[RunningUpstream], *,
                prefix: str = "/api",
                rate: float = 1000.0,
                burst: int = 1000,
                max_in_flight: int = 64,
                max_retries: int = 1,
                timeout: float = 2.0,
                cb_failure_threshold: int = 3,
                cb_reset_timeout: float = 1.0,
                health_interval: float = 0.2) -> GatewayConfig:
    """Build a GatewayConfig pointing at the given running upstreams."""
    return GatewayConfig(
        health_check=HealthCheckConfig(
            path="/healthz",
            interval_seconds=health_interval,
            timeout_seconds=0.5,
        ),
        routes=[RouteConfig(
            prefix=prefix,
            strip_prefix=True,
            request_timeout_seconds=timeout,
            max_retries=max_retries,
            rate_limit=RateLimitConfig(rate_per_second=rate, burst=burst),
            circuit_breaker=CircuitBreakerConfig(
                failure_threshold=cb_failure_threshold,
                reset_timeout_seconds=cb_reset_timeout,
                half_open_max_probes=1,
            ),
            upstreams=[UpstreamConfig(name=u.name, base_url=u.base_url,
                                      max_in_flight=max_in_flight)
                       for u in upstreams],
        )],
    )


@asynccontextmanager
async def gateway_client(config: GatewayConfig) -> AsyncIterator:
    """Yield (httpx client, gateway app) with the lifespan running."""
    app = create_app(config)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://gateway.test") as client:
            yield client, app
