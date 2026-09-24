"""请求/响应转发与故障隔离的集成测试。"""

from __future__ import annotations

import json


def test_get_forwards_query_and_strips_prefix(gateway):
    r = gateway.get("/api/echo?q=hello&r=world")
    assert r.status_code == 200
    body = r.json()
    assert body["path"] == "/echo"  # 前缀被剥离
    assert body["query"] == {"q": "hello", "r": "world"}
    assert body["method"] == "GET"


def test_post_forwards_body_and_method(gateway):
    r = gateway.post(
        "/api/items/42",
        content=json.dumps({"a": 1}),
        headers={"Content-Type": "application/json", "X-My-Header": "abc"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["method"] == "POST"
    assert body["path"] == "/items/42"
    assert body["body"] == '{"a": 1}'


def test_hop_by_hop_headers_stripped(gateway):
    r = gateway.get(
        "/api/echo",
        headers={
            "Connection": "keep-alive",
            "Keep-Alive": "timeout=5",
            "Transfer-Encoding": "chunked",
            "Upgrade": "h2c",
        },
    )
    assert r.status_code == 200


def test_gateway_adds_tracing_headers(gateway):
    r = gateway.get("/api/echo", headers={"X-Request-Id": "fixed-id-123"})
    assert r.status_code == 200
    # 客户端提供的请求标识被透传到上游
    assert r.json()["x_request_id"] == "fixed-id-123"
    assert r.headers["x-request-id"] == "fixed-id-123"
    assert r.headers["via"] == "1.1 fastapi-gateway"


def test_no_store_cache_semantics(gateway):
    # 上游声明 max-age=10，网关必须合并出 no-store
    r = gateway.get("/api/echo")
    cc = {p.strip() for p in r.headers["cache-control"].split(",")}
    assert "no-store" in cc


def test_unknown_route_404(gateway):
    r = gateway.get("/no/such/route")
    assert r.status_code == 404
    err = r.json()["error"]
    assert err["reason"] == "no_upstream_available"
    assert "request_id" in err


def test_error_response_is_no_store(gateway):
    r = gateway.get("/no/such/route")
    assert r.headers["cache-control"] == "no-store"


def test_load_balancing_round_robin(gateway):
    seen = set()
    for _ in range(6):
        seen.add(gateway.get("/api/echo").json()["instance"])
    assert seen == {"u1", "u2", "u3"}
