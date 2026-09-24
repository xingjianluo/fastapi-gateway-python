"""网关入口：uvicorn main:app（骨架）。"""

from __future__ import annotations

from pathlib import Path

from app.gateway.app import create_app
from app.gateway.config import load_config

app = create_app(load_config(Path(__file__).parent / "config" / "gateway.json"))
