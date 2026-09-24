# FastAPI 本地反向代理 / API 网关

一个可本地验证的反向代理与 API 网关：配置驱动的路由、多上游负载均衡、
持续健康探测、按客户端限流、上游并发在途限制、有界重试与故障切换、
熔断器（closed/open/half-open），以及统一的错误响应与请求标识。

## 架构

```
client ──> gateway (main:app)
             ├─ Router        最长前缀匹配路由 (gateway/router.py)
             ├─ RateLimiter   每客户端令牌桶，含突发额度 (gateway/ratelimit.py)
             ├─ Upstream      健康标志 + 在途计数上限 (gateway/upstream.py)
             ├─ CircuitBreaker 熔断器 closed/open/half_open (gateway/circuitbreaker.py)
             ├─ HealthChecker 后台周期探测 /healthz (gateway/health.py)
             └─ proxy         转发、逐跳头部剥离、有界重试 (gateway/proxy.py)
                 │
                 ├─> upstream A (upstream_app.py, 故障注入可控)
                 └─> upstream B
```

## 路由规则

- 路由按 `prefix` 做**最长前缀匹配**；`strip_prefix: true` 时转发前剥离前缀。
- 每个路由挂载多个上游，**轮询**分发到「健康且熔断器未打开」的实例。
- 请求失败（连接错误 / 超时 / 上游 5xx / 在途已满）时在路由内**切换上游有界重试**，
  最多 `1 + max_retries` 次，绝不无限重试；最终的上游 5xx 按原状态码透传，
  不会被包装成成功。

## 健康探测与故障切换

- 后台按 `health_check.interval_seconds` 周期探测每个上游的 `health_check.path`。
- 探测失败即标记不健康，不再接收新请求；恢复后自动重新加入轮询。
- 熔断器：连续失败 `failure_threshold` 次后打开，拒绝请求 `reset_timeout_seconds`；
  之后进入半开，放行 `half_open_max_probes` 个探测请求，成功则关闭、失败则重新打开。

## 限流与并发控制

- **按客户端（源 IP）令牌桶**：`rate_per_second` 匀速补充，`burst` 为突发额度；
  单把 asyncio 锁保证并发下配额不被击穿。
- **单上游在途上限** `max_in_flight`：获取不到槽位立即拒绝（不排队）；
  槽位在 `finally` 中释放，客户端中途断开不会泄漏计数。

## 拒绝原因与状态码（稳定、可区分）

| 原因 (`error.reason`)     | 状态码 | 含义                     |
|---------------------------|--------|--------------------------|
| `rate_limited`            | 429    | 客户端限流超限           |
| `upstream_busy`           | 503    | 上游在途请求数达到上限   |
| `no_healthy_upstream`     | 503    | 无可用上游（含熔断打开） |
| `upstream_timeout`        | 504    | 上游超时                 |
| `upstream_error`          | 502    | 上游连接/传输错误        |
| `no_route`                | 404    | 无匹配路由               |

错误响应体统一为：

```json
{"error": {"reason": "rate_limited", "message": "...", "request_id": "<hex>"}}
```

所有错误响应带 `Cache-Control: no-store` 与 `x-request-id`；成功响应透传上游的
缓存语义并附加 `x-request-id`。转发时剥离逐跳头部（Connection 及其提名的头部、
Keep-Alive、TE、Trailer、Transfer-Encoding、Upgrade 等），保留方法、查询参数、
请求体与端到端头部。

## 配置项（gateway.yaml）

```yaml
health_check:
  path: /healthz            # 探测路径
  interval_seconds: 1.0     # 探测周期
  timeout_seconds: 0.5      # 探测超时

routes:
  - prefix: /api/echo       # 前缀匹配
    strip_prefix: true      # 转发前剥离前缀
    request_timeout_seconds: 2.0
    max_retries: 1          # 额外重试次数（总尝试 = 1 + max_retries）
    rate_limit:
      rate_per_second: 50   # 每客户端令牌补充速率
      burst: 100            # 突发额度
    circuit_breaker:
      failure_threshold: 3      # 连续失败多少次后熔断
      reset_timeout_seconds: 3.0
      half_open_max_probes: 1   # 半开时允许多少探测请求
    upstreams:
      - name: echo-a
        base_url: http://127.0.0.1:9001
        max_in_flight: 32       # 单上游并发在途上限
```

## 本地验证

```bash
# 安装依赖
uv sync

# 终端 1/2：启动两个模拟上游（带故障注入管理接口）
UPSTREAM_NAME=echo-a uv run uvicorn upstream_app:app --port 9001
UPSTREAM_NAME=echo-b uv run uvicorn upstream_app:app --port 9002

# 终端 3：启动网关（GATEWAY_CONFIG 可指定其他配置文件）
uv run uvicorn main:app --port 8080

# 正常代理（轮询分发到 echo-a / echo-b）
curl localhost:8080/api/echo/hello?x=1

# 故障注入：echo-a 全部接口开始失败，流量自动切到 echo-b
curl -X POST localhost:9001/admin/fault -H 'Content-Type: application/json' \
     -d '{"enabled": true}'
curl localhost:8080/api/echo/hello        # 只会打到 echo-b
curl localhost:8080/_gateway/status       # 查看健康/在途/熔断状态

# 恢复：echo-a 被探测恢复后重新加入轮询
curl -X POST localhost:9001/admin/fault -H 'Content-Type: application/json' \
     -d '{"enabled": false}'

# 超时场景：上游 /slow 沉睡，超过 request_timeout_seconds 返回 504
curl localhost:8080/api/echo/slow?delay=5
```

模拟上游接口：`GET /healthz`、`GET /flaky`（故障时 500）、
`GET /slow?delay=N`、`POST /admin/fault`、`GET /admin/state`，
其余路径原样回显方法/查询/请求体/头部。

## 测试

```bash
uv run pytest                 # 全部测试
uv run pytest -o log_cli=true -o log_cli_level=INFO   # 观察路由/熔断日志
```

覆盖内容：

- `tests/test_ratelimit.py`：令牌桶突发/补充、200 并发下配额不越界、客户端隔离
- `tests/test_circuitbreaker.py`：closed→open→half_open→closed 状态机与日志
- `tests/test_upstream.py`：在途上限、取消（客户端断开）不泄漏计数
- `tests/test_proxy.py`：逐跳头部剥离、有界重试、超时/连接错误映射、5xx 透传
- `tests/test_gateway.py`：真实上游端到端——轮询与请求转发、故障注入切换与恢复、
  并发限流、在途上限、三类拒绝原因可区分、熔断打开与自动恢复
