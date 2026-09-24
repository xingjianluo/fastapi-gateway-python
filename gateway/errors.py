"""Unified error responses and rejection reasons."""

from __future__ import annotations

import enum


class RejectReason(str, enum.Enum):
    RATE_LIMITED = "rate_limited"
    UPSTREAM_BUSY = "upstream_busy"
    NO_HEALTHY_UPSTREAM = "no_healthy_upstream"
    UPSTREAM_TIMEOUT = "upstream_timeout"
    UPSTREAM_ERROR = "upstream_error"
    CIRCUIT_OPEN = "circuit_open"
    NO_ROUTE = "no_route"


# Stable mapping of rejection reason -> external HTTP status code.
STATUS_BY_REASON: dict[RejectReason, int] = {
    RejectReason.RATE_LIMITED: 429,
    RejectReason.UPSTREAM_BUSY: 503,
    RejectReason.NO_HEALTHY_UPSTREAM: 503,
    RejectReason.UPSTREAM_TIMEOUT: 504,
    RejectReason.UPSTREAM_ERROR: 502,
    RejectReason.CIRCUIT_OPEN: 503,
    RejectReason.NO_ROUTE: 404,
}
