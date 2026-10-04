"""实例选择、加权负载均衡、预热爬升与在途并发控制。

- 每个实例一把有界信号量限制在途请求；acquire/release 严格配对，
  调用方使用 try/finally，客户端断开也会释放。
- 选择策略为 nginx 式**平滑加权轮询（SWRR）**：每个实例维护 current_weight，
  每轮先加「有效权重」，选 current_weight 最大者，再减去总有效权重。
  权重比例在并发压力下仍然成立，不会饿死低权重实例（权重 > 0 时必然被选中）。
- 实例重新纳入（摘除后恢复健康 / weight 从 0 调回）时，有效权重在
  ``warmup_seconds`` 窗口内从 ``warmup_initial_ratio`` 线性爬升到配置权重，
  避免刚恢复的实例瞬间被打垮；该过程可重复。
- 权重为 0 或被摘除（draining）的实例不接新流量，但仍留在实例列表中，
  健康检查照常进行，在途请求由调用方在 finally 中跑完后自然排空。
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
    inflight: InflightLimit
    weight: int = 1
    # 管理面摘除：不再接新流量；None 表示正常接流量，"drained" 表示已排空
    drain_state: str | None = None
    healthy: bool = False  # 初始视为不健康，首轮探测后再决定
    # SWRR 动态权重
    current_weight: float = 0.0
    # 预热爬升起点（monotonic 时间）；None 表示尚未开始预热
    ramp_started_at: float | None = None
    # 预热窗口内的起始比例与时长（由所属 pool 在构造时注入）
    warmup_seconds: float = 0.0
    warmup_initial_ratio: float = 0.1


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
        warmup_seconds: float = 0.0,
        warmup_initial_ratio: float = 0.1,
    ) -> None:
        self.upstream_id = upstream_id
        self.instances = instances
        self.warmup_seconds = warmup_seconds
        self.warmup_initial_ratio = warmup_initial_ratio
        for inst in self.instances:
            inst.warmup_seconds = warmup_seconds
            inst.warmup_initial_ratio = warmup_initial_ratio
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # 管理面：健康位、权重、摘除/纳入（全部热生效，无需重启）
    # ------------------------------------------------------------------ #

    def get_instance(self, instance_id: str) -> Instance | None:
        for inst in self.instances:
            if inst.id == instance_id:
                return inst
        return None

    def set_health(self, instance_id: str, healthy: bool,
                   now: float | None = None) -> None:
        """更新健康位。

        不健康 -> 健康（重新纳入）会重置预热爬升；持续健康不重置，
        因此反复故障/恢复时预热过程可重复，且不会在正常探测周期内反复打折。
        """
        if now is None:
            now = time.monotonic()
        with self._lock:
            inst = self.get_instance(instance_id)
            if inst is None:
                return
            was_healthy = inst.healthy
            inst.healthy = healthy
            if healthy and not was_healthy:
                self._restart_ramp(inst, now)
            elif not healthy:
                # 离开候选集合：复位爬升起点，下次恢复重新预热
                inst.ramp_started_at = None

    def set_weight(self, instance_id: str, weight: int,
                   now: float | None = None) -> Instance | None:
        """热更新实例权重（>=0）；0 表示只摘流不摘健康检查。

        0 -> 正数视为重新接流量，重置预热；其余调整立即按新权重生效。
        """
        if weight < 0:
            raise ValueError("weight 必须 >= 0")
        if now is None:
            now = time.monotonic()
        with self._lock:
            inst = self.get_instance(instance_id)
            if inst is None:
                return None
            resuming = inst.weight == 0 and weight > 0
            inst.weight = weight
            if resuming:
                self._restart_ramp(inst, now)
            return inst

    def drain(self, instance_id: str) -> bool:
        """摘除实例：在途请求跑完，新请求不再分入；健康检查照常。"""
        with self._lock:
            inst = self.get_instance(instance_id)
            if inst is None:
                return False
            inst.drain_state = "draining"
            inst.ramp_started_at = None
            return True

    def resume(self, instance_id: str, now: float | None = None) -> bool:
        """取消摘除，重新纳入（走预热爬升）。"""
        if now is None:
            now = time.monotonic()
        with self._lock:
            inst = self.get_instance(instance_id)
            if inst is None:
                return False
            if inst.drain_state is not None:
                inst.drain_state = None
                self._restart_ramp(inst, now)
            return True

    def is_drained(self, instance_id: str) -> bool:
        """已摘除（或已摘除且在途归零）。draining 期间在途为 0 即视为排空。"""
        with self._lock:
            inst = self.get_instance(instance_id)
            if inst is None or inst.drain_state is None:
                return False
            return inst.inflight.active == 0

    def add_instance(self, inst: Instance, now: float | None = None) -> None:
        """把新实例加入本组（配置热更新）；预热参数继承 pool 设置。"""
        if now is None:
            now = time.monotonic()
        with self._lock:
            if self.get_instance(inst.id) is not None:
                raise ValueError(f"实例 {inst.id} 已存在")
            inst.warmup_seconds = self.warmup_seconds
            inst.warmup_initial_ratio = self.warmup_initial_ratio
            self.instances.append(inst)
            self._restart_ramp(inst, now)

    def remove_instance(self, instance_id: str) -> Instance | None:
        """从配置中移除实例。仅允许在已排空（无在途）时执行。

        在途请求未跑完时抛 RuntimeError——调用方应先 drain 等待排空，
        避免请求被中途掐断。
        """
        with self._lock:
            inst = self.get_instance(instance_id)
            if inst is None:
                return None
            if inst.inflight.active > 0:
                raise RuntimeError(
                    f"实例 {instance_id} 仍有 {inst.inflight.active} 个在途请求，"
                    "请先 drain 并等待排空"
                )
            self.instances = [i for i in self.instances if i.id != instance_id]
            return inst

    # ------------------------------------------------------------------ #
    # 选择逻辑
    # ------------------------------------------------------------------ #

    def _eligible(self, inst: Instance, now: float) -> bool:
        """调用方持锁。候选：健康 + 未摘除 + 权重 > 0 + 熔断允许。"""
        return (
            inst.healthy
            and inst.drain_state is None
            and inst.weight > 0
            and inst.breaker.allow_request(now)
        )

    def effective_weight(self, inst: Instance, now: float | None = None) -> float:
        """考虑预热爬升后的有效权重（线程安全快照）。"""
        if now is None:
            now = time.monotonic()
        with self._lock:
            return self._effective_weight_locked(inst, now)

    def _effective_weight_locked(self, inst: Instance, now: float) -> float:
        """调用方持锁。首次调用惰性启动预热计时（用于真实选择路径）。"""
        if inst.weight <= 0 or inst.warmup_seconds <= 0:
            return float(inst.weight)
        if inst.ramp_started_at is None:
            # 懒启动：首次进入候选时才开始计时
            inst.ramp_started_at = now
        return self._ramp_weight(inst, now)

    def _peek_effective_weight(self, inst: Instance, now: float) -> float:
        """只读快照（stats/管理面）：不惰性启动预热计时，无副作用。"""
        if inst.weight <= 0 or inst.warmup_seconds <= 0:
            return float(inst.weight)
        if inst.ramp_started_at is None:
            # 预热尚未开始：展示初始比例而不是偷偷启动计时
            return inst.weight * inst.warmup_initial_ratio
        return self._ramp_weight(inst, now)

    def _ramp_weight(self, inst: Instance, now: float) -> float:
        """按预热进度计算有效权重（调用方持锁）。"""
        progress = (now - inst.ramp_started_at) / inst.warmup_seconds
        if progress >= 1.0:
            return float(inst.weight)
        ratio = inst.warmup_initial_ratio + (
            1.0 - inst.warmup_initial_ratio
        ) * max(0.0, progress)
        return inst.weight * ratio

    def _restart_ramp(self, inst: Instance, now: float) -> None:
        """调用方持锁：重新纳入时从初始比例开始爬升。"""
        inst.current_weight = 0.0
        if inst.warmup_seconds > 0:
            inst.ramp_started_at = now
        else:
            inst.ramp_started_at = None

    def acquire_instance(
        self,
        now: float | None = None,
        exclude: frozenset[str] = frozenset(),
    ) -> tuple[Instance | None, str | None]:
        """按权重选择一个可服务实例并占用其一个在途额度。

        选择算法（SWRR，全部在锁内完成，并发下比例不失真）：
        1. 候选 = 健康 + 未摘除 + 权重>0 + 熔断允许；
        2. 每个候选 current_weight += 有效权重，选最大者，
           被选者 current_weight -= 候选有效权重总和；
        3. 被选者在途已满则本轮跳过，继续按更新后的 current_weight 选下一个，
           尝试次数有界（<= 候选数），不会击穿在途上限。

        exclude 中的实例（通常是本次请求已经尝试失败的实例）不参与选择；
        只有所有未尝试实例都不可用时，才允许回退到已尝试实例。

        返回 (实例, 跳过原因)。实例为 None 时，原因可能是：
        - "no_healthy"：无健康/熔断允许/有正权重的实例；
        - "at_capacity"：实例健康但全部达到在途上限。
        """
        if now is None:
            now = time.monotonic()
        with self._lock:
            eligible = [i for i in self.instances if self._eligible(i, now)]
            # 预热爬升中有效权重可能为 0（初始比例=0 且刚恢复）：
            # 此刻暂不接流量，但预热计时已被惰性启动，后续调用权重即变正
            eff = {
                i.id: self._effective_weight_locked(i, now) for i in eligible
            }
            eligible = [i for i in eligible if eff[i.id] > 0]
            candidates = [i for i in eligible if i.id not in exclude]
            if not candidates:
                # 未尝试实例已耗尽：回退到全部合格实例（供重试去重逻辑判定）
                candidates = eligible
            if not candidates:
                return None, "no_healthy"

            # 用 id 做下标：Instance 是 dataclass，相等语义不适合用来定位
            order = {i.id: idx for idx, i in enumerate(self.instances)}
            remaining = list(candidates)
            while remaining:
                total = sum(eff[i.id] for i in remaining)
                for i in remaining:
                    i.current_weight += eff[i.id]
                best = max(
                    remaining,
                    key=lambda i: (i.current_weight, -order[i.id]),
                )
                best.current_weight -= total
                if best.inflight.acquire():
                    return best, None
                # 在途已满：移出本轮尝试集合，继续选下一个（有界）
                remaining.remove(best)
            # 候选存在但全在容量上限
            return None, "at_capacity"

    def release_instance(self, inst: Instance) -> None:
        inst.inflight.release()

    def stats(self) -> dict[str, object]:
        with self._lock:
            now = time.monotonic()
            return {
                "upstream": self.upstream_id,
                "instances": [
                    {
                        "id": i.id,
                        "url": i.url,
                        "healthy": i.healthy,
                        "weight": i.weight,
                        "effective_weight": round(
                            self._peek_effective_weight(i, now), 4
                        ),
                        "draining": i.drain_state is not None,
                        "inflight": i.inflight.active,
                        "max_inflight": i.inflight.limit,
                        "breaker": i.breaker.snapshot(),
                    }
                    for i in self.instances
                ],
            }
