"""FastAPI application factory wiring router, limiter, proxy and health."""

from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response

from gateway.config import GatewayConfig
from gateway.errors import STATUS_BY_REASON, RejectReason
from gateway.health import HealthChecker
from gateway.proxy import (
    GatewayError,
    filter_request_headers,
    filter_response_headers,
    forward_with_retries,
)
from gateway.ratelimit import RateLimiter
from gateway.router import Router, Route

logger = logging.getLogger("gateway.app")


def _error_response(reason: RejectReason, detail: str, request_id: str) -> JSONResponse:
    """Unified error body; errors are never cacheable (no-store)."""
    return JSONResponse(
        {
            "error": {
                "reason": reason.value,
                "message": detail,
                "request_id": request_id,
            }
        },
        status_code=STATUS_BY_REASON[reason],
        headers={
            "x-request-id": request_id,
            "cache-control": "no-store",
        },
    )


def create_app(config: GatewayConfig) -> FastAPI:
    """Build the gateway ASGI app from configuration."""
    routes = [Route(rc) for rc in config.routes]
    router = Router(routes)
    health_checker = HealthChecker(router, config.health_check)
    # One limiter per route so different prefixes can have different quotas.
    limiters: dict[str, RateLimiter] = {
        r.prefix: RateLimiter(r.config.rate_limit.rate_per_second,
                              r.config.rate_limit.burst)
        for r in routes
    }

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.http_client = httpx.AsyncClient()
        await health_checker.start()
        yield
        await health_checker.stop()
        await app.state.http_client.aclose()

    app = FastAPI(title="local-gateway", lifespan=lifespan)
    app.state.router = router
    app.state.health_checker = health_checker

    @app.get("/_gateway/status")
    async def gateway_status() -> dict:
        """Introspection: route/upstream/breaker states (for local checks)."""
        return {
            "routes": [
                {
                    "prefix": route.prefix,
                    "upstreams": [
                        {
                            "name": u.name,
                            "healthy": u.healthy,
                            "in_flight": u.in_flight,
                            "circuit": u.breaker.state.value,
                        }
                        for u in route.upstreams
                    ],
                }
                for route in router.routes
            ]
        }

    @app.api_route("/{path:path}",
                   methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"])
    async def gateway(request: Request, path: str) -> Response:
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
        full_path = "/" + path

        route = router.match(full_path)
        if route is None:
            return _error_response(RejectReason.NO_ROUTE,
                                   f"no route matches {full_path}", request_id)

        client_key = request.client.host if request.client else "unknown"
        limiter = limiters[route.prefix]
        if not await limiter.allow(client_key):
            logger.info("req=%s rejected: rate limited (client=%s route=%s)",
                        request_id, client_key, route.prefix)
            return _error_response(RejectReason.RATE_LIMITED,
                                   "client rate limit exceeded", request_id)

        body = await request.body()
        headers = filter_request_headers(request.headers)
        try:
            upstream_resp = await forward_with_retries(
                request.app.state.http_client, route, request.method,
                full_path, str(request.url.query), headers, body, request_id)
        except GatewayError as exc:
            logger.info("req=%s rejected: %s (%s)", request_id, exc.reason.value, exc.detail)
            return _error_response(exc.reason, exc.detail, request_id)

        logger.info("req=%s upstream responded %d", request_id, upstream_resp.status_code)
        resp_headers = filter_response_headers(upstream_resp.headers)
        resp_headers["x-request-id"] = request_id
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
        )

    return app
