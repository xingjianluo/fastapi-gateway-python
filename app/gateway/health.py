"""上游实例健康探测。

- 后台守护线程周期性 GET 每个实例的 health_path，2xx/3xx 视为健康；
- 实例初始为不健康（未探测），首轮探测通过后才允许接收流量；
- 探测结果同时喂给熔断器：恢复探测成功时 half_open -> closed；
  open 状态下探针会触发 open -> half_open，成功再 -> closed（恢复后自动重新加入）；
- 连续探测失败标记不健康，从候选中摘除（不再接收新请求）。
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
        "instance_id",
        "url",
        "interval",
        "timeout",
        "consecutive_failures",
        "last_ok",
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

    def register(self, pool: UpstreamPool, health_path: str,
                 interval: float, timeout: float) -> None:
        """注册一个 upstream 组的全部实例。"""
        self._pools[pool.upstream_id] = pool
        for inst in pool.instances:
            self._targets.append(
                _ProbeTarget(
                    inst.id,
                    f"{inst.url}{health_path}",
                    interval,
                    timeout,
                )
            )

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

    # ------------------------------------------------------------------ #

    def _loop(self) -> None:
        while not self._stop.wait(self._min_interval()):
            self._run_round(time.monotonic())

    def _min_interval(self) -> float:
        return min((t.interval for t in self._targets), default=1.0)

    def _find_instance(self, instance_id: str):
        for pool in self._pools.values():
            for inst in pool.instances:
                if inst.id == instance_id:
                    return inst
        return None

    def _run_round(self, now: float) -> None:
        for target in self._targets:
            if self._stop.is_set():
                return
            inst = self._find_instance(target.instance_id)
            if inst is None:
                continue
            ok = self._probe(target)
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
                pool = self._owner_pool(target.instance_id)
                pool.set_health(target.instance_id, True)
                # open -> half_open（TTL 后）-> closed 的恢复收敛由这里驱动
                if inst.breaker.allow_request(now):
                    inst.breaker.record_success(now)
            else:
                target.consecutive_failures += 1
                target.last_ok = False
                pool = self._owner_pool(target.instance_id)
                pool.set_health(target.instance_id, False)
                inst.breaker.record_failure(now)
                log_event(
                    logger,
                    "upstream_unhealthy",
                    instance=target.instance_id,
                    url=target.url,
                    consecutive_failures=target.consecutive_failures,
                )

    def _owner_pool(self, instance_id: str) -> UpstreamPool:
        for pool in self._pools.values():
            for inst in pool.instances:
                if inst.id == instance_id:
                    return pool
        raise KeyError(instance_id)

    def _probe(self, target: _ProbeTarget) -> bool:
        try:
            resp = self._client.get(target.url, timeout=target.timeout)
        except httpx.HTTPError:
            return False
        return 200 <= resp.status_code < 400
