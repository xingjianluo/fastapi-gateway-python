"""测试夹具：进程内启动 3 个模拟上游与网关（无需外部进程）。"""

from __future__ import annotations

import logging
import socket
import threading
import time
from collections.abc import Iterator

import httpx
import pytest
import uvicorn
from starlette.testclient import TestClient

from app.gateway.app import create_app
from app.gateway.config import (
    GatewayConfig,
    RateLimitConfig,
    RouteConfig,
    UpstreamConfig,
    UpstreamInstanceConfig,
)
from app.gateway.logging_utils import configure_logging
from app.upstreams.dummy import create_app as create_dummy

# 单元测试日志中打印路由决策、上游选择与熔断状态变化
configure_logging(logging.DEBUG)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class _ServerHandle:
    def __init__(self, port: int, name: str) -> None:
        self.port = port
        self.name = name
        config = uvicorn.Config(
            create_dummy(name),
            host="127.0.0.1",
            port=port,
            log_level="error",
        )
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(
            target=self.server.run, name=f"up-{name}", daemon=True
        )

    def start(self) -> None:
        self.thread.start()
        # 等待端口就绪
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            try:
                httpx.get(f"http://127.0.0.1:{self.port}/health", timeout=0.5)
                return
            except httpx.HTTPError:
                time.sleep(0.02)
        raise RuntimeError(f"upstream {self.name} 启动超时")

    def stop(self) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=3)


@pytest.fixture(scope="session")
def upstream_servers() -> Iterator[dict[str, _ServerHandle]]:
    handles = {
        "u1": _ServerHandle(_free_port(), "u1"),
        "u2": _ServerHandle(_free_port(), "u2"),
        "u3": _ServerHandle(_free_port(), "u3"),
    }
    for h in handles.values():
        h.start()
    yield handles
    for h in handles.values():
        h.stop()


def _control(handles: dict[str, _ServerHandle], name: str, action: str) -> None:
    httpx.post(
        f"http://127.0.0.1:{handles[name].port}/control/{action}", timeout=2
    )


@pytest.fixture(autouse=True)
def _reset_upstreams(upstream_servers):
    """每个测试前把全部模拟上游恢复为正常模式（测试间隔离）。"""
    for h in upstream_servers.values():
        httpx.post(
            f"http://127.0.0.1:{h.port}/control/recover", timeout=2
        )
    yield


@pytest.fixture
def upstream_calls(upstream_servers):
    """返回辅助函数：通过控制面设置故障模式 / 读取实例被调用次数。"""

    class Ctl:
        def set_mode(self, name: str, mode: str) -> None:
            _control(upstream_servers, name, mode)

        def all_normal(self) -> None:
            for n in upstream_servers:
                _control(upstream_servers, n, "recover")

        def url(self, name: str, path: str = "") -> str:
            return f"http://127.0.0.1:{upstream_servers[name].port}{path}"

    yield Ctl()


def make_config(
    handles: dict[str, _ServerHandle],
    *,
    rate: float = 100.0,
    burst: int = 200,
    max_inflight: int = 100,
    timeout: float = 1.5,
    max_retries: int = 2,
    failure_threshold: int = 3,
    open_state_ttl: float = 1.0,
    health_interval: float = 0.2,
    names: tuple[str, ...] = ("u1", "u2", "u3"),
) -> GatewayConfig:
    instances = tuple(
        UpstreamInstanceConfig(id=n, url=f"http://127.0.0.1:{handles[n].port}")
        for n in names
    )
    upstream = UpstreamConfig(
        id="api",
        instances=instances,
        health_path="/health",
        health_interval=health_interval,
        health_timeout=0.5,
        max_inflight=max_inflight,
        failure_threshold=failure_threshold,
        recover_threshold=1,
        open_state_ttl=open_state_ttl,
    )
    route = RouteConfig(
        id="api",
        prefix="/api",
        upstream="api",
        strip_prefix=True,
        timeout=timeout,
        max_retries=max_retries,
    )
    return GatewayConfig(
        routes=(route,),
        upstreams={"api": upstream},
        rate_limit=RateLimitConfig(rate_per_second=rate, burst=burst),
    )


@pytest.fixture
def gateway_factory(upstream_servers):
    clients: list[TestClient] = []

    def _factory(**kwargs) -> TestClient:
        cfg = make_config(upstream_servers, **kwargs)
        app = create_app(
            cfg, log_level=logging.DEBUG, configure_logging_=False
        )
        client = TestClient(app, base_url="http://gw.local")
        client.__enter__()
        clients.append(client)
        return client

    yield _factory

    for c in clients:
        c.__exit__(None, None, None)


@pytest.fixture
def gateway(gateway_factory, upstream_calls):
    upstream_calls.all_normal()
    gw = gateway_factory()
    # 等待首轮健康探测完成
    time.sleep(0.3)
    return gw
