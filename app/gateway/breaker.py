"""单上游实例熔断器：closed / open / half_open。

状态迁移（与时间、调用结果相关）::

    closed --连续失败达到 failure_threshold--> open
    open   --距打开超过 open_state_ttl, 有请求探测--> half_open（放行一次探测）
    half_open --连续成功达到 recover_threshold--> closed
    half_open --任意一次失败--> open（重新计时）

所有状态判断与迁移在同一把锁内完成；状态变化通过注入的回调通知（用于日志/健康标记）。
"""

from __future__ import annotations

import enum
import threading
import time
from collections.abc import Callable

# (实例名, 旧状态, 新状态, 当前时间)
StateChangeCallback = Callable[[str, "BreakerState", "BreakerState", float], None]


class BreakerState(str, enum.Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 3,
        recover_threshold: int = 1,
        open_state_ttl: float = 2.0,
        name: str = "",
        on_state_change: StateChangeCallback | None = None,
    ) -> None:
        self.failure_threshold = failure_threshold
        self.recover_threshold = recover_threshold
        self.open_state_ttl = open_state_ttl
        self.name = name
        self._on_change = on_state_change
        self._lock = threading.Lock()
        self._state = BreakerState.CLOSED
        self._consecutive_failures = 0
        self._half_open_successes = 0
        self._opened_at = 0.0

    @property
    def state(self) -> BreakerState:
        with self._lock:
            return self._state

    def snapshot(self, now: float | None = None) -> dict[str, object]:
        with self._lock:
            return {
                "name": self.name,
                "state": self._state.value,
                "consecutive_failures": self._consecutive_failures,
                "opened_for": (
                    max(0.0, (now or time.monotonic()) - self._opened_at)
                    if self._state is BreakerState.OPEN
                    else 0.0
                ),
            }

    def allow_request(self, now: float | None = None) -> bool:
        """该实例现在能否接收流量（OPEN 超时后放行一次探测并进入 HALF_OPEN）。"""
        if now is None:
            now = time.monotonic()
        with self._lock:
            if self._state is BreakerState.CLOSED:
                return True
            if self._state is BreakerState.OPEN:
                if now - self._opened_at >= self.open_state_ttl:
                    self._transition(BreakerState.HALF_OPEN, now)
                    self._half_open_successes = 0
                    return True  # 仅这一次探测请求被放行
                return False
            # HALF_OPEN：探测请求已在途或尚未积累足够成功，拒绝其余请求
            return False

    def record_success(self, now: float | None = None) -> None:
        if now is None:
            now = time.monotonic()
        with self._lock:
            self._consecutive_failures = 0
            if self._state is BreakerState.HALF_OPEN:
                self._half_open_successes += 1
                if self._half_open_successes >= self.recover_threshold:
                    self._transition(BreakerState.CLOSED, now)

    def record_failure(self, now: float | None = None) -> None:
        if now is None:
            now = time.monotonic()
        with self._lock:
            if self._state is BreakerState.HALF_OPEN:
                # 探测失败：立即回到 OPEN 并重新计时
                self._transition(BreakerState.OPEN, now)
                self._opened_at = now
                self._half_open_successes = 0
                return
            self._consecutive_failures += 1
            if (
                self._state is BreakerState.CLOSED
                and self._consecutive_failures >= self.failure_threshold
            ):
                self._transition(BreakerState.OPEN, now)
                self._opened_at = now

    def _transition(self, new_state: BreakerState, now: float) -> None:
        """调用方持锁。"""
        old = self._state
        if old is new_state:
            return
        self._state = new_state
        if self._on_change is not None:
            self._on_change(self.name, old, new_state, now)
