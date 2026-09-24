"""Prefix-based route matching and upstream selection."""

from __future__ import annotations

import itertools
import logging
import threading

from gateway.config import RouteConfig
from gateway.upstream import Upstream

logger = logging.getLogger("gateway.router")


class Route:
    """A compiled route: config plus its upstream pool."""

    def __init__(self, config: RouteConfig) -> None:
        self.config = config
        self.prefix = config.prefix.rstrip("/") or "/"
        self.upstreams: list[Upstream] = [
            Upstream(u, config.circuit_breaker) for u in config.upstreams
        ]
        self._rr = itertools.count()
        self._rr_lock = threading.Lock()

    def matches(self, path: str) -> bool:
        if self.prefix == "/":
            return True
        return path == self.prefix or path.startswith(self.prefix + "/")

    def select_upstream(self) -> Upstream | None:
        """Round-robin pick among available upstreams; None if none usable."""
        with self._rr_lock:
            start = next(self._rr)
        n = len(self.upstreams)
        for i in range(n):
            candidate = self.upstreams[(start + i) % n]
            if candidate.available:
                logger.info("route %s: selected upstream %s", self.prefix, candidate.name)
                return candidate
        logger.info("route %s: no available upstream", self.prefix)
        return None


class Router:
    """Longest-prefix-match router over configured routes."""

    def __init__(self, routes: list[Route]) -> None:
        # Longest prefix first so the most specific route wins.
        self.routes = sorted(routes, key=lambda r: len(r.prefix),reverse=True)

    def match(self, path: str) -> Route | None:
        for route in self.routes:
            if route.matches(path):
                logger.info("path %s matched route %s", path, route.prefix)
                return route
        logger.info("path %s matched no route", path)
        return None
