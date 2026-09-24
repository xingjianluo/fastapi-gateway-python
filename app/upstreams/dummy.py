"""本地模拟上游服务。

每个实例是一个独立 FastAPI 应用，通过环境变量或工厂参数命名，支持：

- GET  /health                 健康检查，受故障开关控制
- ANY  /echo、/items/{item_id} 回声：回显方法、查询参数、请求体与关键头部
- GET  /slow?delay=秒           慢响应（模拟上游超时）
- POST /control/fail            打开故障开关（/health 与业务路由返回 500）
- POST /control/recover         关闭故障开关
- POST /control/timeout         后续请求进入长睡眠（模拟挂起）
- POST /control/normal          恢复为正常模式
"""

from __future__ import annotations

import os
import threading
import time

from fastapi import FastAPI, Request
from starlette.responses import JSONResponse


def create_app(name: str = "upstream") -> FastAPI:
    app = FastAPI(title=f"dummy-upstream:{name}")
    state = {"mode": "normal", "lock": threading.Lock()}
    # 记录所有被调用的请求，供测试断言负载均衡/重试落到了哪些实例
    app.state.calls: list[dict[str, object]] = []
    app.state.name = name

    def _mode() -> str:
        with state["lock"]:
            return state["mode"]

    def _set_mode(mode: str) -> None:
        with state["lock"]:
            state["mode"] = mode

    def _record(request: Request) -> None:
        app.state.calls.append(
            {
                "ts": time.monotonic(),
                "method": request.method,
                "path": request.url.path,
                "instance": name,
            }
        )

    @app.get("/health")
    async def health() -> JSONResponse:
        if _mode() == "fail":
            return JSONResponse({"status": "unhealthy", "instance": name},
                                status_code=503)
        return JSONResponse({"status": "ok", "instance": name})

    @app.api_route("/echo", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def echo(request: Request) -> JSONResponse:
        return await _business(request, extra_path=None)

    @app.api_route(
        "/items/{item_id}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"]
    )
    async def item(request: Request, item_id: str) -> JSONResponse:
        return await _business(request, extra_path=item_id)

    async def _business(request: Request, extra_path: str | None) -> JSONResponse:
        _record(request)
        mode = _mode()
        if mode == "timeout":
            await asyncio_sleep(60.0)
        if mode == "fail":
            return JSONResponse(
                {"error": "injected failure", "instance": name},
                status_code=500,
            )
        body = await request.body()
        return JSONResponse(
            {
                "instance": name,
                "method": request.method,
                "path": request.url.path,
                "query": dict(request.query_params),
                "body": body.decode("utf-8", "replace"),
                "x_request_id": request.headers.get("x-request-id"),
                "x_forwarded_for": request.headers.get("x-forwarded-for"),
                "via": request.headers.get("via"),
                "received_cache_control": request.headers.get("cache-control"),
            },
            headers={"X-Upstream-Instance": name, "Cache-Control": "max-age=10"},
        )

    @app.get("/slow")
    async def slow(delay: float = 5.0) -> JSONResponse:
        _record_request_get(delay)
        await asyncio_sleep(delay)
        return JSONResponse({"instance": name, "slept": delay})

    def _record_request_get(delay: float) -> None:
        app.state.calls.append(
            {"ts": time.monotonic(), "method": "GET", "path": "/slow",
             "instance": name, "delay": delay}
        )

    @app.post("/control/fail")
    async def control_fail() -> dict[str, str]:
        _set_mode("fail")
        return {"mode": "fail", "instance": name}

    @app.post("/control/recover")
    async def control_recover() -> dict[str, str]:
        _set_mode("normal")
        return {"mode": "normal", "instance": name}

    @app.post("/control/timeout")
    async def control_timeout() -> dict[str, str]:
        _set_mode("timeout")
        return {"mode": "timeout", "instance": name}

    @app.post("/control/normal")
    async def control_normal() -> dict[str, str]:
        _set_mode("normal")
        return {"mode": "normal", "instance": name}

    return app


async def asyncio_sleep(seconds: float) -> None:
    import asyncio

    await asyncio.sleep(seconds)


# 供 uvicorn 以模块路径启动：UPSTREAM_NAME=u1 uvicorn app.upstreams.dummy:app
app = create_app(os.environ.get("UPSTREAM_NAME", "upstream"))
