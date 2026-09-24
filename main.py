"""Entrypoint: run the gateway with `uvicorn main:app` (see README).

Config path defaults to gateway.yaml, override with GATEWAY_CONFIG.
"""

import os

from gateway.app import create_app
from gateway.config import load_config

app = create_app(load_config(os.environ.get("GATEWAY_CONFIG", "gateway.yaml")))
