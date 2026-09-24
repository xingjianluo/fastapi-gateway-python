"""Circuit breaker with closed/open/half-open states."""

from __future__ import annotations

import enum
import logging
import threading
import time

logger = logging.getLogger("gateway.circuitbreaker")


class CircuitState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a call is rejected because the circuit is open."""


class CircuitBreaker:
    """Circuit breaker for one upstream.

    - CLOSED: requests flow; consecutive failures are counted.
    - OPEN: after ``failure_threshold`` consecutive failures, requests are
      rejected for ``reset_timeout_seconds``.
    - HALF_OPEN: after the timeout, up to ``half_open_max_probes`` probe
      requests are let through. A success closes the circuit; a failure
      re-opens it for another timeout window.

    A threading.Lock is used (not asyncio.Lock) because state checks happen
    synchronously in the request path; all critical sections are tiny.
    """

    def __init__(self, failure_threshold: int, reset_timeout_seconds: float,
                 half_open_max_probes: int = 1, name: str = "") -> None:
        self.failure_threshold = failure_threshold
        self.reset_timeout_seconds = reset_timeout_seconds
        self.half_open_max_probes = half_open_max_probes
        self.name = name
        self._state = CircuitState.CLOSED
        self._consecutive_failures = 0
        self._opened_at = 0.0
        self._half_open_in_flight = 0
        self._lock = threading.Lock()

    @property
    def state(self) -> CircuitState:
        with self._lock:
            self._maybe_transition_to_half_open()
            return self._state

    def _maybe_transition_to_half_open(self) -> None:
        if (self._state is CircuitState.OPEN
                and time.monotonic() - self._opened_at >= self.reset_timeout_seconds):
            self._transition(CircuitState.HALF_OPEN)

    def _transition(self, new_state: CircuitState) -> None:
        old = self._state
        if old is new_state:
            return
        self._state = new_state
        if new_state is CircuitState.OPEN:
            self._opened_at = time.monotonic()
        if new_state is CircuitState.HALF_OPEN:
            self._half_open_in_flight = 0
        if new_state is CircuitState.CLOSED:
            self._consecutive_failures = 0
        logger.info("circuit %s: %s -> %s", self.name or "<unnamed>",
                    old.value, new_state.value)

    def allow_request(self) -> bool:
        """True if a request may proceed right now."""
        with self._lock:
            self._maybe_transition_to_half_open()
            if self._state is CircuitState.CLOSED:
                return True
            if self._state is CircuitState.OPEN:
                return False
            # HALF_OPEN: admit a bounded number of concurrent probes.
            if self._half_open_in_flight < self.half_open_max_probes:
                self._half_open_in_flight += 1
                return True
            return False

    def record_success(self) -> None:
        with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
                self._transition(CircuitState.CLOSED)
            else:
                self._consecutive_failures = 0

    def record_failure(self) -> None:
        with self._lock:
            if self._state is CircuitState.HALF_OPEN:
                self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
                self._transition(CircuitState.OPEN)
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.failure_threshold:
                self._transition(CircuitState.OPEN)
