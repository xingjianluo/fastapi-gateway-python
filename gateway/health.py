"""Background health probing of upstream instances."""

from __future__ import annotations

import asyncio
import logging

import httpx

from gateway.config import HealthCheckConfig
from gateway.router import Router

logger = logging.getLogger("gateway.health")


class HealthChecker:
    """Periodically probes upstreams and flips their healthy flags.

    An upstream is healthy iff the probe gets a 2xx/3xx response within
    the configured timeout. State transitions are logged; unhealthy
    instances stop receiving new requests (router skips them) and
    rejoin automatically once probes succeed again.
    """

    def __init__(self, router: Router, config: HealthCheckConfig) -> None:
        self.router = router
        self.config = config
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        self._stopped.clear()
        self._task = asyncio.create_task(self._run(), name="health-checker")

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.probe_once()
            except Exception:  # probing must never kill the loop
                logger.exception("health probe round failed")
            try:
                await asyncio.wait_for(self._stopped.wait(),
                                       timeout=self.config.interval_seconds)
            except asyncio.TimeoutError:
                pass

    async def probe_once(self) -> None:
        async with httpx.AsyncClient(timeout=self.config.timeout_seconds) as client:
            for route in self.router.routes:
                for upstream in route.upstreams:
                    ok = await self._probe(client, upstream.base_url)
                    if ok and not upstream.healthy:
                        logger.info("upstream %s recovered (probe ok)", upstream.name)
                    elif not ok and upstream.healthy:
                        logger.info("upstream %s marked unhealthy (probe failed)",
                                    upstream.name)
                    upstream.healthy = ok

    async def _probe(self, client: httpx.AsyncClient, base_url: str) -> bool:
        try:
            resp = await client.get(base_url + self.config.path)
            return resp.status_code < 400
        except httpx.HTTPError:
            return False
