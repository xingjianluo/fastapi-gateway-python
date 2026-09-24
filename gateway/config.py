"""Configuration models for the gateway (routes, upstreams, limits)."""

from __future__ import annotations

from pydantic import BaseModel, Field


class UpstreamConfig(BaseModel):
    """A single upstream instance."""

    name: str
    base_url: str
    max_in_flight: int = 16


class RateLimitConfig(BaseModel):
    """Per-client token bucket settings."""

    rate_per_second: float = 10.0
    burst: int = 20


class CircuitBreakerConfig(BaseModel):
    """Circuit breaker thresholds."""

    failure_threshold: int = 3
    reset_timeout_seconds: float = 5.0
    half_open_max_probes: int = 1


class RouteConfig(BaseModel):
    """A prefix route mapping to a group of upstreams."""

    prefix: str
    upstreams: list[UpstreamConfig] = Field(default_factory=list)
    strip_prefix: bool = False
    request_timeout_seconds: float = 5.0
    max_retries: int = 1
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)


class HealthCheckConfig(BaseModel):
    path: str = "/healthz"
    interval_seconds: float = 2.0
    timeout_seconds: float = 1.0


class GatewayConfig(BaseModel):
    routes: list[RouteConfig] = Field(default_factory=list)
    health_check: HealthCheckConfig = Field(default_factory=HealthCheckConfig)


def load_config(path: str) -> GatewayConfig:
    """Load gateway configuration from a YAML file."""
    import yaml

    with open(path, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return GatewayConfig.model_validate(data)
