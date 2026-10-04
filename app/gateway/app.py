"""FastAPI 网关应用组装。"""

from __future__ import annotations

import asyncio
import contextlib
import logging

import httpx
from fastapi import FastAPI, Request
from starlette.responses import JSONResponse, Response

from .admin import AdminController, build_single_pool, register_admin_routes
from .config import GatewayConfig
from .errors import RejectReason
from .health import HealthTracker
from .logging_utils import (
    configure_logging,
    get_logger,
    log_event,
    new_request_id,
    request_id_var,
)
from .pool import UpstreamPool
from .proxy import ProxyHandler, error_response
from .ratelimit import ClientRateLimiter

logger = get_logger("app")


def build_pools(config: GatewayConfig) -> dict[str, UpstreamPool]:
    return {
        up_id: build_single_pool(up_id, up_cfg)
        for up_id, up_cfg in config.upstreams.items()
    }


def create_app(
    config: GatewayConfig,
    log_level: int = logging.INFO,
    health_interval_override: float | None = None,
    configure_logging_: bool = True,
) -> FastAPI:
    if configure_logging_:
        configure_logging(log_level)

    app = FastAPI(title="fastapi-gateway", docs_url=None, redoc_url=None)
    pools = build_pools(config)
    limiter = ClientRateLimiter(
        rate_per_second=config.rate_limit.rate_per_second,
        burst=config.rate_limit.burst,
    )

    # 上游与健康探测使用独立连接池，禁止自动重试（重试由网关有界控制）
    upstream_client = httpx.Client(
        timeout=httpx.Timeout(10.0),
        limits=httpx.Limits(max_connections=200, max_keepalive_connections=50),
        transport=httpx.HTTPTransport(retries=0),
    )
    health_client = httpx.Client(transport=httpx.HTTPTransport(retries=0))
    health_tracker = HealthTracker(health_client)
    for up_id, up_cfg in config.upstreams.items():
        interval = (
            health_interval_override
            if health_interval_override is not None
            else up_cfg.health_interval
        )
        health_tracker.register(
            pools[up_id],
            health_path=up_cfg.health_path,
            interval=interval,
            timeout=up_cfg.health_timeout,
        )

    handler = ProxyHandler(config, pools, upstream_client, limiter)

    admin_controller = AdminController(
        config=config,
        pools=pools,
        health_tracker=health_tracker,
        limiter=limiter,
        handler=handler,
    )
    # 管理面必须在全捕获路由 /{path:path} 之前用 add_api_route 直接注册，
    # 否则会被参数化兜底路由遮蔽（FastAPI 0.141 下 include_router 为懒解析）
    register_admin_routes(app, admin_controller)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        health_tracker.start()
        log_event(
            logger,
            "gateway_started",
            routes=[r.id for r in config.routes],
            upstreams=list(config.upstreams),
        )
        try:
            yield
        finally:
            health_tracker.stop()
            upstream_client.close()
            health_client.close()

    app.router.lifespan_context = lifespan

    @app.exception_handler(Exception)
    async def unhandled_exception(request: Request, exc: Exception):
        request_id = getattr(request.state, "request_id", "-")
        log_event(
            logger, "internal_error", error=type(exc).__name__, detail=str(exc)
        )
        return error_response(
            RejectReason.INTERNAL, "网关内部错误", request_id
        )

    @app.get("/_gateway/stats")
    async def stats() -> JSONResponse:
        return JSONResponse(
            {
                "upstreams": [pool.stats() for pool in pools.values()],
                "rate_limit": {
                    "rate_per_second": config.rate_limit.rate_per_second,
                    "burst": config.rate_limit.burst,
                },
            }
        )

    @app.api_route(
        "/{path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
    )
    async def gateway_entry(request: Request) -> Response:
        request_id = request.headers.get("x-request-id") or new_request_id()
        request.state.request_id = request_id
        token = request_id_var.set(request_id)
        try:
            # 先读完客户端请求体；客户端中途断开时这里抛异常，不会占用任何在途额度
            body = await request.body()
            # 同步转发逻辑放到线程池，避免阻塞事件循环（健康探测在独立线程）
            return await asyncio.to_thread(
                handler.handle, request, body, request_id
            )
        finally:
            request_id_var.reset(token)

    # 暴露给测试/脚本，方便直接驱动一轮探测
    app.state.health_tracker = health_tracker
    app.state.pools = pools
    app.state.limiter = limiter
    app.state.admin = admin_controller
    return app
