"""Demo upstream server with fault-injection endpoints for local verification."""

from __future__ import annotations

import asyncio
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


def create_upstream(name: str = "upstream") -> FastAPI:
    """Create a demo upstream app.

    Endpoints:
      GET  /healthz              - health probe target (503 while fault on)
      GET  /flaky                - 500 while fault injection is enabled
      GET  /slow?delay=SECONDS   - sleeps before responding (timeout tests)
      POST /admin/fault          - {"enabled": bool} toggle fault injection
      GET  /admin/state          - current fault state
      ANY  /{path}               - echoes method, query, body, headers
    """
    app = FastAPI(title=name)
    app.state.fault_enabled = False

    @app.get("/healthz")
    async def healthz() -> JSONResponse:
        if app.state.fault_enabled:
            return JSONResponse({"upstream": name, "ok": False}, status_code=503)
        return JSONResponse({"upstream": name, "ok": True})

    @app.get("/flaky")
    async def flaky() -> JSONResponse:
        if app.state.fault_enabled:
            return JSONResponse({"upstream": name, "error": "injected fault"},
                                status_code=500)
        return JSONResponse({"upstream": name, "ok": True})

    @app.get("/slow")
    async def slow(delay: float = 2.0) -> JSONResponse:
        await asyncio.sleep(delay)
        return JSONResponse({"upstream": name, "ok": True, "delay": delay})

    @app.post("/admin/fault")
    async def set_fault(request: Request) -> JSONResponse:
        payload = await request.json()
        app.state.fault_enabled = bool(payload.get("enabled", False))
        return JSONResponse({"upstream": name,
                             "fault_enabled": app.state.fault_enabled})

    @app.get("/admin/state")
    async def get_state() -> JSONResponse:
        return JSONResponse({"upstream": name,
                             "fault_enabled": app.state.fault_enabled})

    # Catch-all echo must be registered LAST so the endpoints above win.
    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
    async def echo(request: Request, path: str) -> JSONResponse:
        body = await request.body()
        return JSONResponse({
            "upstream": name,
            "method": request.method,
            "path": f"/{path}",
            "query": str(request.url.query),
            "headers": dict(request.headers),
            "body": body.decode("utf-8", errors="replace"),
        })

    return app


app = create_upstream(os.environ.get("UPSTREAM_NAME", "upstream"))
