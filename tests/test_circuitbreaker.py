"""Circuit breaker state machine: closed -> open -> half-open -> closed."""

import logging
import time

from gateway.circuitbreaker import CircuitBreaker, CircuitState

logger = logging.getLogger("test.circuitbreaker")


def test_opens_after_threshold_and_rejects(caplog):
    cb = CircuitBreaker(failure_threshold=3, reset_timeout_seconds=60, name="u1")
    with caplog.at_level(logging.INFO, logger="gateway.circuitbreaker"):
        for _ in range(2):
            cb.record_failure()
        assert cb.state is CircuitState.CLOSED
        cb.record_failure()
        assert cb.state is CircuitState.OPEN
    assert not cb.allow_request()
    assert any("closed -> open" in r.message for r in caplog.records)
    logger.info("breaker opened after threshold; transitions=%s",
                [r.message for r in caplog.records])


def test_half_open_probe_closes_on_success():
    cb = CircuitBreaker(failure_threshold=1, reset_timeout_seconds=0.05,
                        half_open_max_probes=1, name="u2")
    cb.record_failure()
    assert cb.state is CircuitState.OPEN
    time.sleep(0.06)
    assert cb.state is CircuitState.HALF_OPEN
    assert cb.allow_request()          # single probe admitted
    assert not cb.allow_request()      # concurrent probes bounded
    cb.record_success()
    assert cb.state is CircuitState.CLOSED
    logger.info("breaker half-open probe succeeded -> closed")


def test_half_open_probe_failure_reopens():
    cb = CircuitBreaker(failure_threshold=1, reset_timeout_seconds=0.05, name="u3")
    cb.record_failure()
    time.sleep(0.06)
    assert cb.allow_request()
    cb.record_failure()
    assert cb.state is CircuitState.OPEN
    assert not cb.allow_request()
    logger.info("breaker half-open probe failed -> reopened")


def test_success_resets_failure_count():
    cb = CircuitBreaker(failure_threshold=2, reset_timeout_seconds=1, name="u4")
    cb.record_failure()
    cb.record_success()
    cb.record_failure()
    assert cb.state is CircuitState.CLOSED
