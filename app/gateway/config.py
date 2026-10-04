"""网关配置模型与 JSON 配置加载。"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class UpstreamInstanceConfig:
    id: str
    url: str
    # 流量权重（>=0）；0 表示不接新流量，但仍参与健康检查
    weight: int = 1


@dataclass(frozen=True)
class RouteConfig:
    id: str
    prefix: str
    upstream: str
    strip_prefix: bool = True
    timeout: float = 5.0
    max_retries: int = 2


@dataclass(frozen=True)
class UpstreamConfig:
    id: str
    instances: tuple[UpstreamInstanceConfig, ...]
    health_path: str = "/health"
    health_interval: float = 2.0
    health_timeout: float = 1.0
    max_inflight: int = 100
    failure_threshold: int = 3
    recover_threshold: int = 1
    open_state_ttl: float = 2.0
    # 实例首次可用 / 摘除后重新纳入时的流量预热时长（秒）；
    # 有效权重在该窗口内从 warmup_initial_ratio 线性爬升到配置权重
    warmup_seconds: float = 0.0
    warmup_initial_ratio: float = 0.1


@dataclass(frozen=True)
class RateLimitConfig:
    rate_per_second: float = 10.0
    burst: int = 20
    exempt_headers: tuple[str, ...] = ()


@dataclass(frozen=True)
class GatewayConfig:
    routes: tuple[RouteConfig, ...]
    upstreams: dict[str, UpstreamConfig]
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)


class ConfigError(ValueError):
    """配置文件缺失字段或取值非法。"""


def _require(cond: bool, msg: str) -> None:
    if not cond:
        raise ConfigError(msg)


def config_from_dict(raw: dict) -> GatewayConfig:
    """从已解析的 JSON 字典构造并校验配置（启动加载与热加载共用）。"""

    rl_raw = raw.get("rate_limit", {})
    rate_limit = RateLimitConfig(
        rate_per_second=float(rl_raw.get("rate_per_second", 10.0)),
        burst=int(rl_raw.get("burst", 20)),
        exempt_headers=tuple(
            h.lower() for h in rl_raw.get("exempt_headers", [])
        ),
    )
    _require(rate_limit.rate_per_second > 0, "rate_per_second 必须为正数")
    _require(rate_limit.burst >= 1, "burst 必须 >= 1")

    upstreams: dict[str, UpstreamConfig] = {}
    for up_id, up_raw in raw.get("upstreams", {}).items():
        inst_list = up_raw.get("instances", [])
        _require(inst_list, f"upstream {up_id} 至少需要一个实例")
        instances = tuple(
            UpstreamInstanceConfig(
                id=str(inst["id"]),
                url=str(inst["url"]).rstrip("/"),
                weight=int(inst.get("weight", 1)),
            )
            for inst in inst_list
        )
        ids = [i.id for i in instances]
        _require(len(set(ids)) == len(ids), f"upstream {up_id} 实例 id 重复")
        _require(
            all(i.weight >= 0 for i in instances),
            f"upstream {up_id} 实例 weight 必须 >= 0",
        )
        upstream = UpstreamConfig(
            id=up_id,
            instances=instances,
            health_path=str(up_raw.get("health_path", "/health")),
            health_interval=float(up_raw.get("health_interval", 2.0)),
            health_timeout=float(up_raw.get("health_timeout", 1.0)),
            max_inflight=int(up_raw.get("max_inflight", 100)),
            failure_threshold=int(up_raw.get("failure_threshold", 3)),
            recover_threshold=int(up_raw.get("recover_threshold", 1)),
            open_state_ttl=float(up_raw.get("open_state_ttl", 2.0)),
            warmup_seconds=float(up_raw.get("warmup_seconds", 0.0)),
            warmup_initial_ratio=float(
                up_raw.get("warmup_initial_ratio", 0.1)
            ),
        )
        _require(upstream.max_inflight >= 1, f"upstream {up_id} max_inflight>=1")
        _require(upstream.failure_threshold >= 1, "failure_threshold>=1")
        _require(upstream.recover_threshold >= 1, "recover_threshold>=1")
        _require(upstream.open_state_ttl > 0, "open_state_ttl>0")
        _require(upstream.warmup_seconds >= 0, "warmup_seconds>=0")
        _require(
            0.0 < upstream.warmup_initial_ratio <= 1.0,
            "warmup_initial_ratio 必须在 (0, 1] 区间",
        )
        upstreams[up_id] = upstream

    routes_list = []
    for r_raw in raw.get("routes", []):
        prefix = str(r_raw["prefix"])
        up_id = str(r_raw["upstream"])
        _require(prefix.startswith("/"), f"route prefix 必须以 / 开头: {prefix}")
        _require(up_id in upstreams, f"route {r_raw.get('id')} 引用了未知 upstream {up_id}")
        route = RouteConfig(
            id=str(r_raw["id"]),
            prefix=prefix,
            upstream=up_id,
            strip_prefix=bool(r_raw.get("strip_prefix", True)),
            timeout=float(r_raw.get("timeout", 5.0)),
            max_retries=int(r_raw.get("max_retries", 2)),
        )
        _require(route.max_retries >= 0, "max_retries>=0")
        routes_list.append(route)

    # 更长的前缀优先匹配，避免短前缀遮蔽长前缀
    routes_list.sort(key=lambda r: len(r.prefix), reverse=True)
    return GatewayConfig(
        routes=tuple(routes_list),
        upstreams=upstreams,
        rate_limit=rate_limit,
    )


def load_config(path: str | Path) -> GatewayConfig:
    """从 JSON 文件加载并校验配置。

    结构::

        {
          "rate_limit": {"rate_per_second": 10, "burst": 20},
          "upstreams": {
            "users": {
              "instances": [
                {"id": "u1", "url": "http://127.0.0.1:9001", "weight": 1}
              ],
              "health_path": "/health", "health_interval": 2.0,
              "health_timeout": 1.0, "max_inflight": 100,
              "failure_threshold": 3, "recover_threshold": 1,
              "open_state_ttl": 2.0,
              "warmup_seconds": 5.0, "warmup_initial_ratio": 0.1
            }
          },
          "routes": [
            {"id": "users", "prefix": "/users", "upstream": "users",
             "strip_prefix": true, "timeout": 5.0, "max_retries": 2}
          ]
        }
    """
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return config_from_dict(raw)
