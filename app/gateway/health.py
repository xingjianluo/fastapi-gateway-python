"""上游实例健康探测。

- 后台守护线程周期性 GET 每个实例的 health_path，2xx/3xx 视为健康；
- 实例初始为不健康（未探测），首轮探测通过后才允许接收流量；
- 探测结果同时喂给熔断器：恢复探测成功时 half_open -> closed；
  open 状态下探针会触发 open -> half_open，成功再 -> closed（恢复后自动重新加入）；
- 连续探测失败标记不健康，从候选中摘除（不再接收新请求）；
- 权重为 0 / 正在 draining 的实例**仍然参与健康检查**，因此重新纳入时
  健康位是新鲜的；从配置中移除的实例通过 unregister_instance 注销探测；
- 探测目标支持热增删（配置热更新），新增实例登记后立即先探一轮。
"""

from __future__ import annotations

import threading
import time

import httpx

from .logging_utils import get_logger, log_event
from .pool import UpstreamPool

logger = get_logger("health")


class _ProbeTarget:
    __slots__ = (
        "consecutive_failures",
        "instance_id",
        "interval",
        "last_ok",
        "timeout",
        "url",
    )

    def __init__(self, instance_id: str, url: str, interval: float, timeout: float):
        self.instance_id = instance_id
        self.url = url
        self.interval = interval
        self.timeout = timeout
        self.consecutive_failures = 0
        self.last_ok: bool | None = None


class HealthTracker:
    def __init__(self, client: httpx.Client) -> None:
        self._client = client
        self._pools: dict[str, UpstreamPool] = {}
        self._targets: list[_ProbeTarget] = []
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # 注册 / 热增删
    # ------------------------------------------------------------------ #

    def register(self, pool: UpstreamPool, health_path: str,
                 interval: float, timeout: float) -> None:
        """注册一个 upstream 组的全部实例。"""
        self._pools[pool.upstream_id] = pool
        for inst in pool.instances:
            self._add_target(
                inst.id, f"{inst.url}{health_path}", interval, timeout
            )

    def register_pool(self, pool: UpstreamPool, health_path: str,
                      interval: float, timeout: float) -> None:
        """热更新：登记一个全新 upstream 组，并立即探测一轮。"""
        self.register(pool, health_path, interval, timeout)
        for inst in pool.instances:
            self.probe_instance(inst.id)

    def add_instance(
        self,
        upstream_id: str,
        instance_id: str,
        url: str,
        health_path: str,
        interval: float,
        timeout: float,
        probe: bool = True,
    ) -> None:
        """热更新：为新实例登记探测目标，并立即先探一次。"""
        if upstream_id not in self._pools:
            raise KeyError(f"未注册的 upstream 组: {upstream_id}")
        self._add_target(
            instance_id, f"{url}{health_path}", interval, timeout
        )
        if probe:
            self.probe_instance(instance_id)

    def unregister_instance(self, instance_id: str) -> None:
        """实例从配置移除后注销探测目标。"""
        with self._lock:
            self._targets = [
                t for t in self._targets if t.instance_id != instance_id
            ]

    def unregister_pool(self, upstream_id: str) -> None:
        """热更新：整个 upstream 组消失时注销其全部探测目标。"""
        pool = self._pools.pop(upstream_id, None)
        if pool is None:
            return
        with self._lock:
            ids = {i.id for i in pool.instances}
            self._targets = [
                t for t in self._targets if t.instance_id not in ids
            ]

    def _add_target(self, instance_id: str, url: str,
                    interval: float, timeout: float) -> None:
        with self._lock:
            if any(t.instance_id == instance_id for t in self._targets):
                return
            self._targets.append(
                _ProbeTarget(instance_id, url, interval, timeout)
            )

    # ------------------------------------------------------------------ #
    # 生命周期
    # ------------------------------------------------------------------ #

    def start(self, probe_now: bool = True) -> None:
        if probe_now:
            # 启动时先做一轮探测，避免启动瞬间把流量打到死掉的实例
            self._run_round(time.monotonic())
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="health-probe", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def is_healthy(self, instance_id: str) -> bool:
        for pool in self._pools.values():
            for inst in pool.instances:
                if inst.id == instance_id:
                    return inst.healthy
        return False

    def probe_once(self) -> None:
        """供测试使用：同步执行一轮探测。"""
        self._run_round(time.monotonic())

    def probe_instance(self, instance_id: str) -> None:
        """供测试/热更新使用：同步探测单个实例一轮。"""
        with self._lock:
            targets = [
                t for t in self._targets if t.instance_id == instance_id
            ]
        now = time.monotonic()
        for target in targets:
            self._probe_target(target, now)

    # ------------------------------------------------------------------ #

    def _loop(self) -> None:
        while not self._stop.wait(self._min_interval()):
            self._run_round(time.monotonic())

    def _min_interval(self) -> float:
        with self._lock:
            return min((t.interval for t in self._targets), default=1.0)

    def _find_instance(self, instance_id: str):
        for pool in self._pools.values():
            for inst in pool.instances:
                if inst.id == instance_id:
                    return inst
        return None

    def _owner_pool(self, instance_id: str) -> UpstreamPool:
        for pool in self._pools.values():
            for inst in pool.instances:
                if inst.id == instance_id:
                    return pool
        raise KeyError(instance_id)

    def _run_round(self, now: float) -> None:
        # 探测过程中 targets 可能被热增删，先取快照
        with self._lock:
            targets = list(self._targets)
        for target in targets:
            if self._stop.is_set():
                return
            self._probe_target(target, now)

    def _probe_target(self, target: _ProbeTarget, now: float) -> None:
        inst = self._find_instance(target.instance_id)
        if inst is None:
            # 实例已从所有 pool 移除但目标尚未注销（理论上注销是同步的）
            return
        ok = self._probe(target)
        pool = self._owner_pool(target.instance_id)
        if ok:
            target.consecutive_failures = 0
            if target.last_ok is False:
                log_event(
                    logger,
                    "upstream_recovered",
                    instance=target.instance_id,
                    url=target.url,
                )
            target.last_ok = True
            # 重新变健康会在 pool 内重置预热爬升
            pool.set_health(target.instance_id, True, now)
            # open -> half_open（TTL 后）-> closed 的恢复收敛由这里驱动
            if inst.breaker.allow_request(now):
                inst.breaker.record_success(now)
        else:
            target.consecutive_failures += 1
            target.last_ok = False
            pool.set_health(target.instance_id, False, now)
            inst.breaker.record_failure(now)
            log_event(
                logger,
                "upstream_unhealthy",
                instance=target.instance_id,
                url=target.url,
                consecutive_failures=target.consecutive_failures,
            )

    def _probe(self, target: _ProbeTarget) -> bool:
        try:
            resp = self._client.get(target.url, timeout=target.timeout)
        except httpx.HTTPError:
            return False
        return 200 <= resp.status_code < 400
