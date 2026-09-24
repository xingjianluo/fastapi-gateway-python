"""统一错误响应与可区分的拒绝原因（骨架）。"""

from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    RATE_LIMITED = "rate_limited"
    NO_UPSTREAM = "no_upstream_available"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_FAILURE = "upstream_failure"
    INTERNAL = "internal_error"


# 拒绝原因 -> 对外稳定状态码
REASON_STATUS: dict[RejectReason, int] = {
    RejectReason.RATE_LIMITED: 429,
    RejectReason.NO_UPSTREAM: 503,
    RejectReason.UPSTREAM_TIMEOUT: 504,
    RejectReason.UPSTREAM_FAILURE: 502,
    RejectReason.INTERNAL: 500,
}


class GatewayError(Exception):
    def __init__(self, reason: RejectReason, message: str = "") -> None:
        self.reason = reason
        super().__init__(message or reason.value)
