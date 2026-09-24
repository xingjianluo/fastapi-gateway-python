"""Upstream instance: health state, in-flight limiting, circuit breaker."""

from __future__ import annotations

import threading

from gateway.circuitbreaker import CircuitBreaker
from gateway.config import CircuitBreakerConfig, UpstreamConfig


class UpstreamBusyError(Exception):
    """Raised when the upstream's in-flight limit is reached."""


class Upstream:
    """One upstream instance with health flag, in-flight cap and breaker.

    The in-flight counter is a plain bounded counter guarded by a lock.
    ``acquire`` never blocks: when the cap is reached it raises
    ``UpstreamBusyError`` so the gateway can try another upstream or
    reject. ``release`` is called in a ``finally`` by the proxy layer, so
    client disconnects (which raise through the handler) cannot leak
    in-flight counts.
    """

    def __init__(self, config: UpstreamConfig, cb_config: CircuitBreakerConfig) -> None:
        self.config = config
        self.name = config.name
        self.base_url = config.base_url.rstrip("/")
        self.healthy: bool = True
        self.breaker = CircuitBreaker(
            failure_threshold=cb_config.failure_threshold,
            reset_timeout_seconds=cb_config.reset_timeout_seconds,
            half_open_max_probes=cb_config.half_open_max_probes,
            name=config.name,
        )
        self._max_in_flight = config.max_in_flight
        self._in_flight = 0
        self._lock = threading.Lock()

    @property
    def in_flight(self) -> int:
        with self._lock:
            return self._in_flight

    @property
    def available(self) -> bool:
        """Healthy according to the prober and circuit not open."""
        return self.healthy and self.breaker.allow_request()

    def acquire(self) -> None:
        """Acquire an in-flight slot or raise UpstreamBusyError."""
        with self._lock:
            if self._in_flight >= self._max_in_flight:
                raise UpstreamBusyError(
                    f"upstream {self.name} at in-flight cap {self._max_in_flight}")
            self._in_flight += 1

    def release(self) -> None:
        """Release an in-flight slot."""
        with self._lock:
            if self._in_flight > 0:
                self._in_flight -= 1
