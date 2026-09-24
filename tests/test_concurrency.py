"""并发压力下的配额、在途计数与客户端断开测试。"""

from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from app.gateway.app import create_app as create_gateway_app

from .conftest import make_config as make_test_config


def free_tcp_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_port(port: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            httpx.get(f"http://127.0.0.1:{port}/_gateway/stats", timeout=0.5)
            return
        except httpx.HTTPError:
            time.sleep(0.03)
    raise RuntimeError(f"gateway on {port} 未就绪")


def test_rate_limit_under_concurrency(gateway_factory):
    """burst=8、速率≈0：30 个并发请求恰好 8 个 200，其余 429。"""
    gw = gateway_factory(rate=0.0, burst=8, health_interval=60.0)
    time.sleep(0.3)
    statuses: list[int] = []
    lock = threading.Lock()

    def one(_: int) -> None:
        r = gw.get("/api/echo")
        with lock:
            statuses.append(r.status_code)

    with ThreadPoolExecutor(max_workers=16) as pool:
        list(pool.map(one, range(30)))

    assert statuses.count(200) == 8
    assert statuses.count(429) == 22


def test_rate_limit_refill_allows_more_after_wait(gateway_factory):
    """rate=20/s,burst=2：打满后按速率补充，等待后重新放行。"""
    gw = gateway_factory(rate=20.0, burst=2, health_interval=60.0)
    time.sleep(0.3)
    first = [gw.get("/api/echo").status_code for _ in range(4)]
    assert first.count(200) == 2
    assert first.count(429) == 2
    # rate=20/s => 每 0.05s 补 1 个令牌
    time.sleep(0.06)
    assert gw.get("/api/echo").status_code == 200
    assert gw.get("/api/echo").status_code == 429
    # 再等 0.15s 补 ~3 个（被 burst=2 截断），攒满 2 个
    time.sleep(0.15)
    assert gw.get("/api/echo").status_code == 200
    assert gw.get("/api/echo").status_code == 200
    assert gw.get("/api/echo").status_code == 429


def test_inflight_counter_never_exceeds_cap(gateway_factory):
    """每个实例 max_inflight=1：并发慢请求期间，单实例在途绝不超过 1。

    3 个实例同时只能承载 3 个在途请求，其余请求立即 503（无可用上游）。
    """
    gw = gateway_factory(
        max_inflight=1,
        timeout=3.0,
        max_retries=0,
        health_interval=60.0,
    )
    time.sleep(0.3)

    peaks: dict[str, int] = {}
    peaks_lock = threading.Lock()
    stop = threading.Event()

    def monitor() -> None:
        while not stop.is_set():
            stats = gw.get("/_gateway/stats").json()
            for up in stats["upstreams"]:
                for inst in up["instances"]:
                    cur = inst["inflight"]
                    with peaks_lock:
                        peaks[inst["id"]] = max(peaks.get(inst["id"], 0), cur)
                    assert cur <= 1, f"{inst['id']} 在途越界: {cur}"
            time.sleep(0.005)

    mon = threading.Thread(target=monitor, daemon=True)
    mon.start()

    def slow_call(_: int) -> int:
        try:
            r = gw.get("/api/slow?delay=0.8", timeout=5)
            return r.status_code
        except Exception:
            return -1

    with ThreadPoolExecutor(max_workers=12) as pool:
        codes = list(pool.map(slow_call, range(12)))
    stop.set()
    mon.join(timeout=1)

    served = codes.count(200)
    rejected = codes.count(503)
    # 容量为 3：第一批 3 个被处理，其余至少有若干被立即拒绝；
    # 0.8s 的慢窗口内 12 个并发不可能全部被处理
    assert served <= 6, codes
    assert rejected >= 1, codes
    assert all(v <= 1 for v in peaks.values()), peaks

    # 慢请求结束后，在途计数必须全部归零（无泄漏）
    time.sleep(0.2)
    stats = gw.get("/_gateway/stats").json()
    inflight = [
        inst["inflight"]
        for up in stats["upstreams"]
        for inst in up["instances"]
    ]
    assert inflight == [0, 0, 0], inflight


def test_client_disconnect_does_not_leak_inflight(
    gateway_factory, upstream_servers
):
    """客户端在网关等待慢上游时断开 TCP：在途计数必须被回收。"""
    import uvicorn

    cfg = make_test_config(upstream_servers)
    app = create_gateway_app(cfg)
    port = free_tcp_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    wait_port(port)
    try:
        # 1) 发起一个慢请求（上游 sleep 3s），但在 0.3s 后客户端强制断开
        s = socket.create_connection(("127.0.0.1", port), timeout=3)
        s.sendall(b"GET /api/slow?delay=3.0 HTTP/1.1\r\nHost: gw\r\n\r\n")
        time.sleep(0.4)
        s.close()  # 客户端中途断开

        # 2) 轮询观测端点：等待慢上游超时/结束期间，计数可能短暂为 1，
        #    但最终必须归零，证明没有泄漏
        deadline = time.monotonic() + 6
        leaked = []
        while time.monotonic() < deadline:
            stats = httpx.get(
                f"http://127.0.0.1:{port}/_gateway/stats", timeout=2
            ).json()
            inflight = [
                inst["inflight"]
                for up in stats["upstreams"]
                for inst in up["instances"]
            ]
            if sum(inflight) == 0:
                return
            leaked = inflight
            time.sleep(0.1)
        raise AssertionError(f"客户端断开后在途计数未回收: {leaked}")
    finally:
        server.should_exit = True
        thread.join(timeout=3)
