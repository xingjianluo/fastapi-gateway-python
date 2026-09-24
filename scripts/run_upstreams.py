"""启动多个本地模拟上游实例（9001/9002/9003）。

用法::

    uv run python -m scripts.run_upstreams

按 Ctrl-C 停止。
"""

from __future__ import annotations

import multiprocessing
import os
import time

import uvicorn

from app.upstreams.dummy import create_app

PORTS_AND_NAMES = [
    (9001, "api-1"),
    (9002, "api-2"),
    (9003, "api-3"),
]


def _worker(port: int, name: str) -> None:  # pragma: no cover
    os.environ["UPSTREAM_NAME"] = name
    uvicorn.run(
        create_app(name),
        host="127.0.0.1",
        port=port,
        log_level="warning",
    )


def main() -> None:  # pragma: no cover
    procs = [
        multiprocessing.Process(target=_worker, args=(p, n), daemon=False)
        for p, n in PORTS_AND_NAMES
    ]
    for proc in procs:
        proc.start()
    print(f"upstreams started: {PORTS_AND_NAMES} (Ctrl-C 停止)")
    try:
        while all(p.is_alive() for p in procs):
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            proc.join(timeout=3)


if __name__ == "__main__":
    main()
