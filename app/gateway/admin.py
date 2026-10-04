"""网关管理面：权重热更新、实例摘除/纳入与配置热加载。

所有操作直接作用于运行中的 UpstreamPool / HealthTracker，立即生效，无需重启：

- POST /_gateway/upstreams/{up}/instances/{inst}/weight  {"weight": 5}
- POST /_gateway/upstreams/{up}/instances/{inst}/drain     摘流（在途跑完）
- POST /_gateway/upstreams/{up}/instances/{inst}/resume    重新纳入（预热爬升）
- GET  /_gateway/upstreams/{up}/instances/{inst}/status    摘除/在途状态
- POST /_gateway/reload                                    整份配置热加载

摘除语义：drain / weight=0 后新请求不再分入，但在途请求在 finally 中正常跑完，
健康检查照常；reload 中被移除的实例若仍有在途请求，会先转为摘除状态并返回 409，
排空后再次 reload 才真正删除。
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from .breaker import CircuitBreaker
from .config import GatewayConfig, UpstreamConfig, config_from_dict
from .health import HealthTracker
from .logging_utils import get_logger, log_event
from .pool import InflightLimit, Instance, UpstreamPool
from .proxy import ProxyHandler
from .ratelimit import ClientRateLimiter

logger = get_logger("admin")


def build_single_pool(up_id: str, up_cfg: UpstreamConfig) -> UpstreamPool:
    """按配置构造一个 upstream 池（启动与热加载共用）。"""
    instances: list[Instance] = []
    for inst_cfg in up_cfg.instances:

        def _on_change(name: str, old, new, now: float) -> None:
            log_event(
                get_logger("breaker"),
                "breaker_state_changed",
                instance=name,
                old_state=old.value,
                new_state=new.value,
            )

        breaker = CircuitBreaker(
            failure_threshold=up_cfg.failure_threshold,
            recover_threshold=up_cfg.recover_threshold,
            open_state_ttl=up_cfg.open_state_ttl,
            name=inst_cfg.id,
            on_state_change=_on_change,
        )
        instances.append(
            Instance(
                id=inst_cfg.id,
                url=inst_cfg.url,
                breaker=breaker,
                inflight=InflightLimit(up_cfg.max_inflight),
                weight=inst_cfg.weight,
                warmup_seconds=up_cfg.warmup_seconds,
                warmup_initial_ratio=up_cfg.warmup_initial_ratio,
            )
        )
    return UpstreamPool(
        up_id,
        instances,
        warmup_seconds=up_cfg.warmup_seconds,
        warmup_initial_ratio=up_cfg.warmup_initial_ratio,
    )


class AdminController:
    def __init__(
        self,
        config: GatewayConfig,
        pools: dict[str, UpstreamPool],
        health_tracker: HealthTracker,
        limiter: ClientRateLimiter,
        handler: ProxyHandler,
    ) -> None:
        self._config = config
        self._pools = pools
        self._health = health_tracker
        self._limiter = limiter
        self._handler = handler

    # ------------------------------------------------------------------ #
    # 单实例操作
    # ------------------------------------------------------------------ #

    def _pool_or_404(self, upstream_id: str) -> UpstreamPool | None:
        return self._pools.get(upstream_id)

    def set_weight(
        self, upstream_id: str, instance_id: str, weight: int
    ) -> tuple[int, dict]:
        pool = self._pool_or_404(upstream_id)
        if pool is None:
            return 404, {"error": f"未知 upstream: {upstream_id}"}
        if weight < 0:
            return 400, {"error": "weight 必须 >= 0"}
        inst = pool.set_weight(instance_id, weight)
        if inst is None:
            return 404, {"error": f"未知实例: {instance_id}"}
        log_event(
            logger,
            "weight_updated",
            upstream=upstream_id,
            instance=instance_id,
            weight=weight,
        )
        return 200, {
            "upstream": upstream_id,
            "instance": instance_id,
            "weight": weight,
            "effective_weight": pool.effective_weight(inst),
        }

    def drain(self, upstream_id: str, instance_id: str) -> tuple[int, dict]:
        pool = self._pool_or_404(upstream_id)
        if pool is None:
            return 404, {"error": f"未知 upstream: {upstream_id}"}
        inst = pool.get_instance(instance_id)
        if inst is None:
            return 404, {"error": f"未知实例: {instance_id}"}
        pool.drain(instance_id)
        log_event(
            logger,
            "instance_draining",
            upstream=upstream_id,
            instance=instance_id,
            inflight=inst.inflight.active,
        )
        return 200, {
            "upstream": upstream_id,
            "instance": instance_id,
            "draining": True,
            "drained": inst.inflight.active == 0,
            "inflight": inst.inflight.active,
        }

    def resume(self, upstream_id: str, instance_id: str) -> tuple[int, dict]:
        pool = self._pool_or_404(upstream_id)
        if pool is None:
            return 404, {"error": f"未知 upstream: {upstream_id}"}
        inst = pool.get_instance(instance_id)
        if inst is None:
            return 404, {"error": f"未知实例: {instance_id}"}
        pool.resume(instance_id)
        log_event(
            logger,
            "instance_resumed",
            upstream=upstream_id,
            instance=instance_id,
            weight=inst.weight,
        )
        return 200, {
            "upstream": upstream_id,
            "instance": instance_id,
            "draining": False,
            "weight": inst.weight,
            "effective_weight": pool.effective_weight(inst),
        }

    def instance_status(
        self, upstream_id: str, instance_id: str
    ) -> tuple[int, dict]:
        pool = self._pool_or_404(upstream_id)
        if pool is None:
            return 404, {"error": f"未知 upstream: {upstream_id}"}
        inst = pool.get_instance(instance_id)
        if inst is None:
            return 404, {"error": f"未知实例: {instance_id}"}
        return 200, {
            "upstream": upstream_id,
            "instance": instance_id,
            "healthy": inst.healthy,
            "weight": inst.weight,
            "effective_weight": pool.effective_weight(inst),
            "draining": inst.drain_state is not None,
            "drained": pool.is_drained(instance_id),
            "inflight": inst.inflight.active,
            "breaker": inst.breaker.snapshot(),
        }

    # ------------------------------------------------------------------ #
    # 整份配置热加载
    # ------------------------------------------------------------------ #

    def reload(self, new_config: GatewayConfig) -> tuple[int, dict]:
        summary: dict[str, object] = {
            "rate_limit": None,
            "upstreams_added": [],
            "upstreams_removed": [],
            "instances_added": [],
            "instances_removed": [],
            "instances_weight_changed": [],
            "instances_pending_drain": [],
            "routes": len(new_config.routes),
        }

        # 1) 限流参数热更新（桶保留，配额按新参数计算）
        self._limiter.update_config(
            new_config.rate_limit.rate_per_second,
            new_config.rate_limit.burst,
        )
        summary["rate_limit"] = {
            "rate_per_second": new_config.rate_limit.rate_per_second,
            "burst": new_config.rate_limit.burst,
        }

        # 2) upstream 组：新增 / 协调 / 删除
        for up_id, up_cfg in new_config.upstreams.items():
            pool = self._pools.get(up_id)
            if pool is None:
                pool = build_single_pool(up_id, up_cfg)
                self._pools[up_id] = pool
                self._health.register_pool(
                    pool,
                    health_path=up_cfg.health_path,
                    interval=up_cfg.health_interval,
                    timeout=up_cfg.health_timeout,
                )
                summary["upstreams_added"].append(up_id)
                continue
            self._reconcile_pool(pool, up_cfg, summary)

        for up_id in list(self._pools):
            if up_id not in new_config.upstreams:
                pool = self._pools[up_id]
                pending = [
                    i.id for i in pool.instances if i.inflight.active > 0
                ]
                if pending:
                    # 整组摘除：保证在途跑完，再次 reload 后才真正删除
                    for inst in pool.instances:
                        pool.drain(inst.id)
                    summary["instances_pending_drain"].extend(
                        f"{up_id}/{i}" for i in pending
                    )
                    continue
                self._health.unregister_pool(up_id)
                self._pools.pop(up_id)
                summary["upstreams_removed"].append(up_id)

        # 3) 路由配置热替换（最长前缀排序已在加载时完成）
        self._config = new_config
        self._handler.config = new_config

        log_event(logger, "config_reloaded", **summary)
        pending = summary["instances_pending_drain"]
        if pending:
            return 409, {
                "error": "部分实例仍有在途请求，已置为摘除状态；排空后再次 reload",
                "summary": summary,
            }
        return 200, {"summary": summary}

    def _reconcile_pool(
        self, pool: UpstreamPool, up_cfg: UpstreamConfig, summary: dict
    ) -> None:
        """协调单个已有 upstream 池与新配置（调用方：reload）。"""
        pool.warmup_seconds = up_cfg.warmup_seconds
        pool.warmup_initial_ratio = up_cfg.warmup_initial_ratio
        new_ids = {c.id for c in up_cfg.instances}

        # 2.1) 被移除的实例：无在途则删除并注销探测；有在途先摘除，返回 409 待排空
        for inst in list(pool.instances):
            if inst.id in new_ids:
                continue
            if inst.inflight.active > 0:
                pool.drain(inst.id)
                summary["instances_pending_drain"].append(
                    f"{pool.upstream_id}/{inst.id}"
                )
                continue
            self._health.unregister_instance(inst.id)
            pool.remove_instance(inst.id)
            summary["instances_removed"].append(
                f"{pool.upstream_id}/{inst.id}"
            )

        # 2.2) 新增实例：登记运行时状态与健康探测（立即先探一轮）
        existing = {i.id: i for i in pool.instances}
        for inst_cfg in up_cfg.instances:
            if inst_cfg.id in existing:
                inst = existing[inst_cfg.id]
                if inst.weight != inst_cfg.weight:
                    pool.set_weight(inst_cfg.id, inst_cfg.weight)
                    summary["instances_weight_changed"].append(
                        f"{pool.upstream_id}/{inst_cfg.id}:{inst_cfg.weight}"
                    )
                if inst.url != inst_cfg.url:
                    inst.url = inst_cfg.url
                    self._health.unregister_instance(inst.id)
                    self._health.add_instance(
                        pool.upstream_id,
                        inst.id,
                        inst.url,
                        health_path=up_cfg.health_path,
                        interval=up_cfg.health_interval,
                        timeout=up_cfg.health_timeout,
                    )
                inst.warmup_seconds = up_cfg.warmup_seconds
                inst.warmup_initial_ratio = up_cfg.warmup_initial_ratio
                # 熔断阈值等标量参数也热更新
                inst.breaker.failure_threshold = up_cfg.failure_threshold
                inst.breaker.recover_threshold = up_cfg.recover_threshold
                inst.breaker.open_state_ttl = up_cfg.open_state_ttl
                continue
            breaker = CircuitBreaker(
                failure_threshold=up_cfg.failure_threshold,
                recover_threshold=up_cfg.recover_threshold,
                open_state_ttl=up_cfg.open_state_ttl,
                name=inst_cfg.id,
            )
            new_inst = Instance(
                id=inst_cfg.id,
                url=inst_cfg.url,
                breaker=breaker,
                inflight=InflightLimit(up_cfg.max_inflight),
                weight=inst_cfg.weight,
                warmup_seconds=up_cfg.warmup_seconds,
                warmup_initial_ratio=up_cfg.warmup_initial_ratio,
            )
            pool.add_instance(new_inst)
            self._health.add_instance(
                pool.upstream_id,
                new_inst.id,
                new_inst.url,
                health_path=up_cfg.health_path,
                interval=up_cfg.health_interval,
                timeout=up_cfg.health_timeout,
            )
            summary["instances_added"].append(
                f"{pool.upstream_id}/{new_inst.id}"
            )


def register_admin_routes(app: FastAPI, controller: AdminController) -> None:
    """把管理面路由直接注册到网关 app。

    必须在全捕获路由 /{path:path} 之前调用：用 add_api_route 直接注册
    （而非 include_router），避免 FastAPI 0.141 对 include 的懒解析被
    后注册的参数化兜底路由遮蔽。
    """

    def _json(status: int, body: dict) -> JSONResponse:
        return JSONResponse(status_code=status, content=body,
                            headers={"Cache-Control": "no-store"})

    async def set_weight(
        upstream_id: str, instance_id: str, request: Request
    ) -> JSONResponse:
        try:
            payload = await request.json()
            weight = int(payload["weight"])
        except (KeyError, TypeError, ValueError):
            return _json(400, {"error": "请求体必须为 {\"weight\": <非负整数>}"})
        status, body = controller.set_weight(upstream_id, instance_id, weight)
        return _json(status, body)

    async def drain(upstream_id: str, instance_id: str) -> JSONResponse:
        status, body = controller.drain(upstream_id, instance_id)
        return _json(status, body)

    async def resume(upstream_id: str, instance_id: str) -> JSONResponse:
        status, body = controller.resume(upstream_id, instance_id)
        return _json(status, body)

    async def instance_status(upstream_id: str, instance_id: str) -> JSONResponse:
        status, body = controller.instance_status(upstream_id, instance_id)
        return _json(status, body)

    async def reload(request: Request) -> JSONResponse:
        try:
            payload = await request.json()
            new_config = config_from_dict(payload)
        except ValueError as exc:
            return _json(400, {"error": f"配置非法: {exc}"})
        status, body = controller.reload(new_config)
        return _json(status, body)

    base = "/_gateway/upstreams/{upstream_id}/instances/{instance_id}"
    app.add_api_route(
        f"{base}/weight", set_weight, methods=["POST"],
        name="admin_set_weight",
    )
    app.add_api_route(
        f"{base}/drain", drain, methods=["POST"], name="admin_drain"
    )
    app.add_api_route(
        f"{base}/resume", resume, methods=["POST"], name="admin_resume"
    )
    app.add_api_route(
        f"{base}/status", instance_status, methods=["GET"],
        name="admin_instance_status",
    )
    app.add_api_route(
        "/_gateway/reload", reload, methods=["POST"], name="admin_reload"
    )
