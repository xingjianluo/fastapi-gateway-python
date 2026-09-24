"""实例选择、负载均衡与在途并发控制。

- 每个实例一把有界信号量限制在途请求；acquire/release 严格配对，
  调用方使用 try/finally，客户端断开也会释放。
- 选择策略：轮询起点逐请求推进，只返回「健康探测通过 + 熔断器允许 + 有在途额度」的实例。
- 熔断打开的实例在 open 阶段直接跳过；TTL 后只有一次探测请求能选中它（half_open）。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .breaker import CircuitBreaker


@dataclass
class Instance:
    id: str
    url: str
    breaker: CircuitBreaker
    inflight: "InflightLimit"
    healthy: bool = False  # 初始视为不健康，首轮探测后再决定


class InflightLimit:
    """单实例在途请求计数。

    acquire 非阻塞（timeout=0）：拿不到额度立即返回 False，由选择器跳过该实例。
    release 必须在 finally 中调用，因此客户端断开也不会泄漏计数。
    """

    def __init__(self, limit: int) -> None:
        if limit < 1:
            raise ValueError("inflight limit 必须 >= 1")
        self._limit = limit
        self._sem = threading.BoundedSemaphore(limit)
        self._active = 0
        self._lock = threading.Lock()

    @property
    def active(self) -> int:
        with self._lock:
            return self._active

    @property
    def limit(self) -> int:
        return self._limit

    def acquire(self, blocking: bool = False, timeout: float = 0.0) -> bool:
        kwargs = {"blocking": blocking}
        if blocking:
            kwargs["timeout"] = timeout
        if self._sem.acquire(**kwargs):
            with self._lock:
                self._active += 1
            return True
        return False

    def release(self) -> None:
        with self._lock:
            if self._active <= 0:
                # 防御性：绝不重复释放，避免计数被击穿
                raise RuntimeError("inflight 计数重复释放")
            self._active -= 1
        self._sem.release()


class UpstreamPool:
    """一个 upstream 组内所有实例的运行时状态与选择逻辑。"""

    def __init__(
        self,
        upstream_id: str,
        instances: list[Instance],
    ) -> None:
        self.upstream_id = upstream_id
        self.instances = instances
        self._rr_index = 0
        self._lock = threading.Lock()

    def set_health(self, instance_id: str, healthy: bool) -> None:
        for inst in self.instances:
            if inst.id == instance_id:
                inst.healthy = healthy
                return

    def eligible_instances(self, now: float) -> list[Instance]:
        return [
            inst
            for inst in self.instances
            if inst.healthy and inst.breaker.allow_request(now)
        ]

    def acquire_instance(
        self,
        now: float | None = None,
        exclude: frozenset[str] = frozenset(),
    ) -> tuple[Instance | None, str | None]:
        """选择一个可服务实例并占用其一个在途额度。

        exclude 中的实例（通常是本次请求已经尝试失败的实例）优先跳过；
        只有所有未尝试实例都不可用时，才允许回退到已尝试实例。

        返回 (实例, 跳过原因)。实例为 None 时，原因可能是：
        - "no_healthy"：无健康/熔断允许的实例；
        - "at_capacity"：实例健康但全部达到在途上限。
        """
        if now is None:
            now = time.monotonic()
        with self._lock:
            eligible = self.eligible_instances(now)
            candidates = [i for i in eligible if i.id not in exclude]
            fallback = not candidates
            if fallback:
                candidates = eligible
            if not candidates:
                return None, "no_healthy"
            n = len(self.instances)
            for offset in range(n):
                inst = self.instances[(self._rr_index + offset) % n]
                if inst not in candidates:
                    continue
                if inst.inflight.acquire():
                    self._rr_index = (self._rr_index + offset + 1) % n
                    return inst, None
                # 该实例在途已满，尝试下一个
            # 候选存在但全在容量上限
            return None, "at_capacity"

    def release_instance(self, inst: Instance) -> None:
        inst.inflight.release()

    def stats(self) -> dict[str, object]:
        return {
            "upstream": self.upstream_id,
            "instances": [
                {
                    "id": i.id,
                    "url": i.url,
                    "healthy": i.healthy,
                    "inflight": i.inflight.active,
                    "max_inflight": i.inflight.limit,
                    "breaker": i.breaker.snapshot(),
                }
                for i in self.instances
            ],
        }
