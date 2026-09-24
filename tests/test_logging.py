"""单测中断言关键决策日志：路由、上游选择、熔断状态变化。"""

from __future__ import annotations

import logging
import time


class _Capture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _events(capture: _Capture) -> list[dict]:
    out = []
    for record in capture.records:
        extra = getattr(record, "extra_fields", {})
        out.append({"event": record.getMessage(), **extra})
    return out


def _attach() -> _Capture:
    capture = _Capture()
    root = logging.getLogger("gateway")
    root.addHandler(capture)
    return capture


def _detach(capture: _Capture) -> None:
    logging.getLogger("gateway").removeHandler(capture)


def test_route_and_upstream_decision_logged(gateway):
    capture = _attach()
    try:
        gateway.get("/api/echo?q=1")
        events = _events(capture)
    finally:
        _detach(capture)

    route_evts = [e for e in events if e["event"] == "route_selected"]
    assert route_evts and route_evts[0]["route"] == "api"
    assert route_evts[0]["upstream"] == "api"

    picks = [e for e in events if e["event"] == "upstream_selected"]
    assert picks and picks[0]["instance"] in {"u1", "u2", "u3"}
    assert picks[0]["attempt"] == 1


def test_breaker_state_transition_logged(gateway_factory, upstream_calls):
    capture = _attach()
    try:
        gateway_factory(
            failure_threshold=1, open_state_ttl=0.3, health_interval=0.1
        )
        time.sleep(0.3)
        upstream_calls.set_mode("u1", "fail")

        deadline = time.monotonic() + 3
        transitions: list[dict] = []
        while time.monotonic() < deadline:
            transitions = [
                e for e in _events(capture)
                if e["event"] == "breaker_state_changed"
                and e.get("instance") == "u1"
            ]
            if any(e["new_state"] == "open" for e in transitions):
                break
            time.sleep(0.05)

        assert any(
            e["new_state"] == "open" and e["old_state"] == "closed"
            for e in transitions
        ), transitions

        upstream_calls.set_mode("u1", "recover")
        deadline = time.monotonic() + 4
        while time.monotonic() < deadline:
            transitions = [
                e for e in _events(capture)
                if e["event"] == "breaker_state_changed"
                and e.get("instance") == "u1"
            ]
            if any(e["new_state"] == "closed" for e in transitions):
                break
            time.sleep(0.05)
        assert any(e["new_state"] == "half_open" for e in transitions)
        assert any(e["new_state"] == "closed" for e in transitions)
    finally:
        _detach(capture)
