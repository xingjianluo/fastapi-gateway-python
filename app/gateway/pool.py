"""实例选择、按权重负载均衡与在途并发控制。

- 每个实例一把有界信号量限制在途请求；acquire/release 严格配对，
  调用方使用 try/finally，客户端断开也会释放。
- 选择策略：平滑加权轮询（smooth WRR）。每个实例按「有效权重」比例
  获得流量，长期偏差有界，不会饿死权重小的实例，权重为 0 的实例
  不参与选择（但仍被健康探测）。
- 有效权重 = 配置权重 x 爬坡系数。实例从不合格变为合格（恢复健康、
  权重 0->正、摘除后重新纳入）时，有效权重在 weight_warmup 秒内从
  10% 线性爬升到 100%，避免刚恢复的实例被流量打垮；每次恢复都重新
  爬坡，行为可重复。
- 运行时可通过 set_weight / remove_instance / reinclude_instance
  调整，无需重启；被摘除（removed）或权重置 0 的实例不再接收新请求，
  已在途的请求正常跑完（在途额度与实例对象生命周期一致）。
- 熔断打开的实例在 open 阶段直接跳过；TTL 后只有一次探测请求能选中它（half_open）。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field

from .breaker import CircuitBreaker

# 爬坡起点：恢复瞬间有效权重至少为配置权重的 10%，保证新流量能缓慢探过来
_WARMUP_MIN_RATIO = 0.1


@dataclass
class Instance:
    id: str
    url: str
    breaker: CircuitBreaker
    inflight: InflightLimit
    weight: int = 1  # 配置权重，0 = 不接新流量（仍参与健康探测）
    healthy: bool = False  # 初始视为不健康，首轮探测后再决定
    removed: bool = False  # 被运维摘除：不参与选择，在途请求正常完成
    # 平滑加权轮询的当前计分（仅在选择锁内访问）
    current_weight: float = field(default=0.0, repr=False)
    # 爬坡起点（monotonic 秒）；None 表示不在爬坡中（全额有效权重）
    ramp_start: float | None = field(default=None, repr=False)

    def effective_weight(self, now: float, warmup: float) -> float:
        """当前有效权重：配置权重乘以爬坡系数。"""
        if self.weight <= 0 or self.removed:
            return 0.0
        if warmup <= 0 or self.ramp_start is None:
            return float(self.weight)
        ratio = (now - self.ramp_start) / warmup
        if ratio >= 1.0:
            return float(self.weight)
        factor = _WARMUP_MIN_RATIO + (1.0 - _WARMUP_MIN_RATIO) * max(ratio, 0.0)
        return self.weight * factor


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
        weight_warmup: float = 0.0,
    ) -> None:
        self.upstream_id = upstream_id
        self.instances = instances
        self.weight_warmup = weight_warmup
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # 运行时状态变更（健康探测、运维操作）
    # ------------------------------------------------------------------ #

    def set_health(self, instance_id: str, healthy: bool,
                   now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        with self._lock:
            for inst in self.instances:
                if inst.id == instance_id:
                    if healthy and not inst.healthy:
                        # 从不健康恢复：重新开始爬坡，流量逐步加回
                        inst.ramp_start = now
                    inst.healthy = healthy
                    return

    def set_weight(self, instance_id: str, weight: int,
                   now: float | None = None) -> bool:
        """运行时调整权重（无需重启）。0->正 时重新开始爬坡。"""
        if weight < 0:
            raise ValueError("weight 必须 >= 0")
        now = time.monotonic() if now is None else now
        with self._lock:
            for inst in self.instances:
                if inst.id == instance_id:
                    if inst.weight == 0 and weight > 0:
                        inst.ramp_start = now
                    inst.weight = weight
                    return True
            return False

    def remove_instance(self, instance_id: str) -> bool:
        """摘除实例：不再接收新请求；已在途的请求正常跑完。"""
        with self._lock:
            for inst in self.instances:
                if inst.id == instance_id:
                    inst.removed = True
                    return True
            return False

    def reinclude_instance(self, instance_id: str,
                           now: float | None = None) -> bool:
        """重新纳入被摘除的实例：从爬坡起点开始逐步恢复流量。"""
        now = time.monotonic() if now is None else now
        with self._lock:
            for inst in self.instances:
                if inst.id == instance_id:
                    inst.removed = False
                    inst.ramp_start = now
                    return True
            return False

    # ------------------------------------------------------------------ #
    # 实例选择
    # ------------------------------------------------------------------ #

    def eligible_instances(self, now: float) -> list[Instance]:
        return [
            inst
            for inst in self.instances
            if inst.healthy
            and not inst.removed
            and inst.effective_weight(now, self.weight_warmup) > 0.0
            and inst.breaker.allow_request(now)
        ]

    def acquire_instance(
        self,
        now: float | None = None,
        exclude: frozenset[str] = frozenset(),
    ) -> tuple[Instance | None, str | None]:
        """按有效权重选择一个可服务实例并占用其一个在途额度。

        exclude 中的实例（通常是本次请求已经尝试失败的实例）优先跳过；
        只有所有未尝试实例都不可用时，才允许回退到已尝试实例。

        选中的实例若在途已满，在有界次数内（候选数）换到下一个
        按权重排序的实例，绝不突破任何单实例在途上限。

        返回 (实例, 跳过原因)。实例为 None 时，原因可能是：
        - "no_healthy"：无健康/熔断允许/有权重的实例；
        - "at_capacity"：实例可服务但全部达到在途上限。
        """
        if now is None:
            now = time.monotonic()
        with self._lock:
            eligible = self.eligible_instances(now)
            candidates = [i for i in eligible if i.id not in exclude]
            if not candidates:
                candidates = eligible
            if not candidates:
                return None, "no_healthy"

            # 平滑加权轮询：先为所有候选临时加上有效权重，
            # 之后每轮取计分最高者；成功占用额度才提交计分变更，
            # 全部失败则回滚到进入时的计分，避免漂移破坏长期比例。
            weights = {
                i.id: i.effective_weight(now, self.weight_warmup)
                for i in candidates
            }
            total = sum(weights.values())
            saved = {i.id: i.current_weight for i in candidates}
            for i in candidates:
                i.current_weight += weights[i.id]

            remaining = list(candidates)
            picked: Instance | None = None
            while remaining:  # 有界：最多尝试候选数个实例
                best = max(remaining, key=lambda i: i.current_weight)
                remaining.remove(best)
                best.current_weight -= total
                if best.inflight.acquire():
                    picked = best
                    break
                # 该实例在途已满，换下一个计分最高的候选
            if picked is None:
                # 回滚计分，本次选择不产生影响
                for i in candidates:
                    i.current_weight = saved[i.id]
                return None, "at_capacity"
            return picked, None

    def release_instance(self, inst: Instance) -> None:
        inst.inflight.release()

    def stats(self) -> dict[str, object]:
        now = time.monotonic()
        return {
            "upstream": self.upstream_id,
            "instances": [
                {
                    "id": i.id,
                    "url": i.url,
                    "healthy": i.healthy,
                    "removed": i.removed,
                    "weight": i.weight,
                    "effective_weight": round(
                        i.effective_weight(now, self.weight_warmup), 4
                    ),
                    "inflight": i.inflight.active,
                    "max_inflight": i.inflight.limit,
                    "breaker": i.breaker.snapshot(),
                }
                for i in self.instances
            ],
        }
