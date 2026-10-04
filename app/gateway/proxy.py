"""请求转发、有界重试与故障切换。

流程：路由前缀匹配 -> 客户端令牌桶限流 -> 在实例池中获取健康实例（占在途额度）
-> 转发请求；连接错误/超时/上游 5xx 时在剩余实例上做有界重试（最多 max_retries 次）。
任何退出路径（含客户端断开、上游异常）都在 finally 中释放在途额度。
"""

from __future__ import annotations

import time
from urllib.parse import urlsplit

import httpx
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from .config import GatewayConfig, RouteConfig
from .errors import REASON_STATUS, RejectReason
from .logging_utils import get_logger, log_event
from .pool import UpstreamPool
from .ratelimit import ClientRateLimiter

logger = get_logger("proxy")

# RFC 7230 逐跳头部 + 网关需要自行决定语义的头部，转发前必须剥离
HOP_BY_HOP_HEADERS = frozenset(
    h.lower()
    for h in (
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        # 长度由 httpx 根据转发的 body 重新计算，避免不一致
        "content-length",
        # 网关自身生成请求标识
        "x-request-id",
    )
)

# 默认仅对幂等方法重试；请求体已缓存，是否允许重试非幂等方法由配置决定
DEFAULT_RETRY_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "PUT", "DELETE"})

RETRYABLE_STATUS = frozenset({500, 502, 503, 504})


def match_route(config: GatewayConfig, path: str) -> RouteConfig | None:
    """最长前缀匹配：/ 前缀匹配所有，其余要求前缀后是边界（/ 或结尾）。"""
    for route in config.routes:
        prefix = route.prefix
        if path == prefix or path.startswith(prefix.rstrip("/") + "/"):
            return route
    return None


def build_upstream_headers(request: Request, request_id: str) -> dict[str, str]:
    """保留必要的端到端头部，剥离逐跳头部，追加转发与追踪头部。"""
    headers = {
        k: v
        for k, v in request.headers.items()
        if k.lower() not in HOP_BY_HOP_HEADERS
    }
    client = request.client
    fwd_parts = []
    if client is not None:
        fwd_parts.append(f"for={client.host}")
    fwd_parts.append(f"proto={request.url.scheme}")
    headers["X-Forwarded-Proto"] = request.url.scheme
    if client is not None:
        prior = request.headers.get("x-forwarded-for", "").strip()
        headers["X-Forwarded-For"] = (
            f"{prior}, {client.host}".strip(", ") if prior else client.host
        )
    headers["X-Request-Id"] = request_id
    headers["Via"] = "1.1 fastapi-gateway"
    return headers


def build_upstream_url(
    base_url: str, route: RouteConfig, path: str
) -> str:
    if route.strip_prefix:
        remainder = path[len(route.prefix):]
        if not remainder.startswith("/"):
            remainder = "/" + remainder
    else:
        remainder = path
    return f"{base_url}{remainder}"


def error_response(reason: RejectReason, message: str, request_id: str,
                   status_code: int | None = None) -> JSONResponse:
    """统一错误响应：固定结构、稳定状态码、no-store、携带请求标识。"""
    code = status_code or REASON_STATUS[reason]
    return JSONResponse(
        status_code=code,
        content={
            "error": {
                "reason": reason.value,
                "message": message,
                "request_id": request_id,
            }
        },
        headers={
            "X-Request-Id": request_id,
            "Cache-Control": "no-store",
            "Via": "1.1 fastapi-gateway",
        },
    )



class ProxyHandler:
    def __init__(
        self,
        config: GatewayConfig,
        pools: dict[str, UpstreamPool],
        client: httpx.Client,
        limiter: ClientRateLimiter,
        retry_methods: frozenset[str] = DEFAULT_RETRY_METHODS,
    ) -> None:
        self.config = config
        self.pools = pools
        self._client = client
        self._limiter = limiter
        self._retry_methods = retry_methods

    # ------------------------------------------------------------------ #

    def handle(
        self, request: Request, body: bytes, request_id: str
    ) -> Response:
        """同步转发入口（由端点在线程池中调用，body 已读取完毕）。"""
        route = match_route(self.config, request.url.path)
        if route is None:
            return error_response(
                RejectReason.NO_UPSTREAM,
                f"无匹配路由: {request.url.path}",
                request_id,
                status_code=404,
            )
        log_event(
            logger,
            "route_selected",
            route=route.id,
            prefix=route.prefix,
            upstream=route.upstream,
            path=request.url.path,
        )

        # 1) 按客户端维度限流
        client_key = request.client.host if request.client else "unknown"
        if not self._limiter.allow(client_key):
            log_event(logger, "rate_limited", client=client_key)
            resp = error_response(
                RejectReason.RATE_LIMITED,
                "客户端请求超出配额",
                request_id,
            )
            resp.headers["Retry-After"] = "1"
            return resp

        # 2) 转发（body 已缓存，可用于重试重放）
        return self._forward_with_retry(request, route, body, request_id)

    # ------------------------------------------------------------------ #

    def _forward_with_retry(
        self,
        request: Request,
        route: RouteConfig,
        body: bytes,
        request_id: str,
    ) -> Response:
        pool = self.pools[route.upstream]
        headers = build_upstream_headers(request, request_id)
        # 保留原始查询串（允许重复参数，避免被去重）
        query = urlsplit(str(request.url)).query
        can_retry = request.method.upper() in self._retry_methods
        attempts_left = route.max_retries if can_retry else 0
        tried: set[str] = set()
        now = time.monotonic()

        while True:
            inst, unavailable_reason = pool.acquire_instance(
                now, exclude=frozenset(tried)
            )
            if inst is None:
                # 所有健康实例的在途额度被占满，同样视为暂时无可用上游
                if unavailable_reason == "at_capacity" and not tried:
                    message = "上游实例均已达到在途并发上限"
                else:
                    message = "无可用实例（熔断打开或不健康）"
                log_event(
                    logger,
                    "no_upstream",
                    upstream=route.upstream,
                    reason=unavailable_reason,
                    tried=sorted(tried),
                )
                return error_response(
                    RejectReason.NO_UPSTREAM, message, request_id
                )

            url = build_upstream_url(inst.url, route, request.url.path)
            if query:
                url = f"{url}?{query}"
            log_event(
                logger,
                "upstream_selected",
                instance=inst.id,
                url=url,
                attempt=route.max_retries - attempts_left + 1,
                weight=inst.weight,
                effective_weight=round(pool.effective_weight(inst), 4),
                inflight=inst.inflight.active,
            )

            # terminal 为本轮尝试的终结结论；failure_kind 记录失败类型
            terminal: Response | None = None
            failure_kind: str | None = None
            retryable_failure = False
            try:
                upstream_resp = self._client.request(
                    request.method,
                    url,
                    headers=headers,
                    content=body if body else None,
                    timeout=route.timeout,
                )
                if upstream_resp.status_code in RETRYABLE_STATUS:
                    inst.breaker.record_failure()
                    failure_kind = "status"
                    retryable_failure = can_retry
                    log_event(
                        logger,
                        "upstream_5xx",
                        instance=inst.id,
                        status=upstream_resp.status_code,
                        retryable=can_retry,
                    )
                    if not can_retry:
                        # 非幂等方法不重试，但绝不把上游内部错误直接透传
                        log_event(
                            logger,
                            "non_retryable_5xx",
                            instance=inst.id,
                            method=request.method,
                            status=upstream_resp.status_code,
                        )
                        terminal = error_response(
                            RejectReason.UPSTREAM_FAILURE,
                            f"上游返回 {upstream_resp.status_code}，"
                            f"且 {request.method} 请求不可重试",
                            request_id,
                        )
                else:
                    inst.breaker.record_success()
                    terminal = self._adapt_response(upstream_resp, request_id)
            except httpx.TimeoutException:
                inst.breaker.record_failure()
                failure_kind = "timeout"
                retryable_failure = can_retry
                log_event(
                    logger,
                    "upstream_timeout",
                    instance=inst.id,
                    timeout=route.timeout,
                )
            except httpx.HTTPError as exc:
                inst.breaker.record_failure()
                failure_kind = "connect"
                retryable_failure = can_retry
                log_event(
                    logger,
                    "upstream_connect_error",
                    instance=inst.id,
                    error=type(exc).__name__,
                )
            finally:
                # 唯一释放入口：任何路径（含上游异常、客户端断开）都不泄漏在途计数
                pool.release_instance(inst)
                tried.add(inst.id)

            if terminal is not None:
                return terminal

            if not retryable_failure or attempts_left <= 0:
                reason = (
                    RejectReason.UPSTREAM_TIMEOUT
                    if failure_kind == "timeout"
                    else RejectReason.UPSTREAM_FAILURE
                )
                log_event(
                    logger,
                    "retries_exhausted",
                    upstream=route.upstream,
                    reason=reason.value,
                    failure_kind=failure_kind,
                    attempts=len(tried),
                )
                return error_response(
                    reason,
                    f"上游在 {len(tried)} 次尝试后仍失败（有界重试已耗尽）",
                    request_id,
                )
            attempts_left -= 1
            now = time.monotonic()

    # ------------------------------------------------------------------ #

    def _adapt_response(
        self, upstream_resp: httpx.Response, request_id: str
    ) -> Response:
        """把上游响应转换为对外响应：剥离逐跳头部，强制 no-store 等缓存语义。"""
        resp_headers = {
            k: v
            for k, v in upstream_resp.headers.items()
            if k.lower() not in HOP_BY_HOP_HEADERS
        }
        resp_headers["X-Request-Id"] = request_id
        resp_headers["Via"] = "1.1 fastapi-gateway"
        # 网关默认不缓存任何转发内容；若上游显式给出缓存指令则保留更严格者
        existing_cc = {
            p.strip().lower()
            for p in upstream_resp.headers.get("cache-control", "").split(",")
            if p.strip()
        }
        existing_cc.add("no-store")
        resp_headers["Cache-Control"] = ", ".join(sorted(existing_cc))
        return Response(
            content=upstream_resp.content,
            status_code=upstream_resp.status_code,
            headers=resp_headers,
            media_type=None,
        )
