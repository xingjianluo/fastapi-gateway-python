"""结构化日志与请求 ID 辅助。

日志格式为单行 JSON，便于在单测中断言路由决策、上游选择与熔断状态变化。
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import uuid

# 当前请求 ID，供日志字段注入
request_id_var: contextvars.ContextVar[str] = contextvars.ContextVar(
    "request_id", default="-"
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "request_id": request_id_var.get(),
            "event": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(level: int = logging.INFO) -> None:
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger("gateway")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name if name.startswith("gateway") else f"gateway.{name}")
    return logger


def new_request_id() -> str:
    return uuid.uuid4().hex[:12]


def log_event(logger: logging.Logger, event: str, **fields: object) -> None:
    """以结构化字段记录一个事件。"""
    logger.info(event, extra={"extra_fields": fields})
