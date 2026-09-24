"""Request forwarding: header handling, bounded retries, error mapping."""

from __future__ import annotations

import logging

import httpx

from gateway.errors import RejectReason
from gateway.router import Route
from gateway.upstream import Upstream, UpstreamBusyError

logger = logging.getLogger("gateway.proxy")

HOP_BY_HOP_HEADERS: frozenset[str] = frozenset({
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
})


def _connection_tokens(headers) -> set[str]:
    """Extra header names nominated by the Connection header."""
    # starlette Headers exposes getlist(); httpx Headers exposes get_list().
    get_all = getattr(headers, "getlist", None) or getattr(headers, "get_list")
    tokens: set[str] = set()
    for value in get_all("connection"):
        tokens.update(t.strip().lower() for t in value.split(",") if t.strip())
    return tokens


def filter_request_headers(headers: httpx.Headers) -> dict[str, str]:
    """Strip hop-by-hop headers (incl. Connection-nominated ones)."""
    drop = HOP_BY_HOP_HEADERS | _connection_tokens(headers) | {"host", "content-length"}
    return {k: v for k, v in headers.items() if k.lower() not in drop}


def filter_response_headers(headers: httpx.Headers) -> dict[str, str]:
    """Strip hop-by-hop headers from an upstream response."""
    drop = HOP_BY_HOP_HEADERS | _connection_tokens(headers) | {"content-length"}
    return {k: v for k, v in headers.items() if k.lower() not in drop}


class GatewayError(Exception):
    """A rejection with a stable, externally distinguishable reason."""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


class UpstreamAttemptError(Exception):
    """A single upstream attempt failed and may be retried elsewhere."""

    def __init__(self, reason: RejectReason, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def build_upstream_url(route: Route, path: str, query: str) -> str:
    """Map an incoming path onto the upstream base URL."""
    suffix = path
    if route.config.strip_prefix and route.prefix != "/" and path.startswith(route.prefix):
        suffix = path[len(route.prefix):] or "/"
    url = route.prefix and suffix or "/"
    return url if not query else f"{url}?{query}"


async def send_to_upstream(client: httpx.AsyncClient, upstream: Upstream,
                           route: Route, method: str, path: str, query: str,
                           headers: dict[str, str], body: bytes,
                           request_id: str) -> httpx.Response:
    """Send one attempt to one upstream.

    Acquires an in-flight slot (released in ``finally`` so client
    disconnects cannot leak the count) and records the outcome on the
    upstream's circuit breaker. Raises UpstreamAttemptError on
    timeout/connection failure; returns the response otherwise
    (including 5xx, which is recorded as a breaker failure but never
    converted into a success).
    """
    upstream.acquire()  # may raise UpstreamBusyError
    try:
        url = upstream.base_url + build_upstream_url(route, path, query)
        logger.info("req=%s forwarding %s %s -> upstream %s (%s)",
                    request_id, method, path, upstream.name, url)
        try:
            resp = await client.request(
                method, url,
                headers={**headers, "x-request-id": request_id},
                content=body if body else None,
                timeout=route.config.request_timeout_seconds,
            )
        except httpx.TimeoutException as exc:
            upstream.breaker.record_failure()
            raise UpstreamAttemptError(
                RejectReason.UPSTREAM_TIMEOUT,
                f"upstream {upstream.name} timed out") from exc
        except httpx.TransportError as exc:
            upstream.breaker.record_failure()
            raise UpstreamAttemptError(
                RejectReason.UPSTREAM_ERROR,
                f"upstream {upstream.name} connect error: {exc}") from exc
        if resp.status_code >= 500:
            upstream.breaker.record_failure()
        else:
            upstream.breaker.record_success()
        return resp
    finally:
        upstream.release()


async def forward_with_retries(client: httpx.AsyncClient, route: Route,
                               method: str, path: str, query: str,
                               headers: dict[str, str], body: bytes,
                               request_id: str) -> httpx.Response:
    """Forward with bounded retries across upstreams of the route.

    At most ``1 + max_retries`` attempts are made; each attempt picks the
    next available upstream via round-robin. Failure precedence for the
    final rejection: timeout > no/busy upstream > transport error.
    """
    attempts = 1 + max(0, route.config.max_retries)
    last_error: UpstreamAttemptError | None = None
    saw_busy = False
    last_5xx: httpx.Response | None = None

    for attempt in range(attempts):
        upstream = route.select_upstream()
        if upstream is None:
            if last_error is not None or last_5xx is not None or saw_busy:
                break  # nothing usable left; report the real cause below
            raise GatewayError(RejectReason.NO_HEALTHY_UPSTREAM,
                               f"route {route.prefix} has no available upstream")
        try:
            resp = await send_to_upstream(
                client, upstream, route, method, path, query, headers, body, request_id)
        except UpstreamBusyError:
            saw_busy = True
            logger.info("req=%s upstream %s busy (attempt %d/%d)",
                        request_id, upstream.name, attempt + 1, attempts)
            continue
        except UpstreamAttemptError as exc:
            last_error = exc
            logger.info("req=%s attempt %d/%d failed: %s",
                        request_id, attempt + 1, attempts, exc.detail)
            continue
        if resp.status_code >= 500 and attempt + 1 < attempts:
            last_5xx = resp
            logger.info("req=%s upstream %s returned %d, failing over (attempt %d/%d)",
                        request_id, upstream.name, resp.status_code, attempt + 1, attempts)
            continue
        return resp

    if last_error is not None and last_error.reason is RejectReason.UPSTREAM_TIMEOUT:
        raise GatewayError(RejectReason.UPSTREAM_TIMEOUT, last_error.detail)
    if last_5xx is not None:
        # All attempts failed and the best answer we have is an upstream
        # 5xx: pass it through with its real status, never as a success.
        return last_5xx
    if saw_busy:
        raise GatewayError(RejectReason.UPSTREAM_BUSY,
                           f"route {route.prefix} upstreams at in-flight cap")
    if last_error is not None:
        raise GatewayError(last_error.reason, last_error.detail)
    raise GatewayError(RejectReason.NO_HEALTHY_UPSTREAM,
                       f"route {route.prefix} has no available upstream")
