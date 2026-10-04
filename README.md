# FastAPI 反向代理 / API 网关（本地可验证）

一个纯 Python、零外部中间件的 API 网关实现：配置化路由前缀 + 多上游负载均衡、
**按权重的流量分配（平滑加权轮询）与灰度控制面（运行时调权/摘除/重新纳入，
恢复后流量爬坡）**、后台健康探测、按客户端令牌桶限流（支持突发）、
单上游在途并发限制、有界重试与故障切换、实例级熔断（closed/open/half_open）、
逐跳头部剥离、统一错误响应与请求标识。所有能力均可在本机用内置模拟上游直接验证。

## 目录结构

```
app/
  gateway/
    config.py          配置模型与 JSON 加载/校验（最长前缀匹配排序）
    app.py             FastAPI 组装：生命周期、全捕获路由、stats、统一异常
    proxy.py           路由匹配、头部处理、转发、有界重试与故障切换
    ratelimit.py       按客户端的令牌桶（突发额度，并发不超发）
    pool.py            按权重实例选择（平滑加权轮询+爬坡）、健康与熔断过滤、在途信号量
    breaker.py         单实例熔断器状态机 closed/open/half_open
    health.py          后台周期性健康探测，驱动摘除与恢复收敛
    errors.py          可区分的拒绝原因 -> 稳定状态码
    logging_utils.py   单行 JSON 结构化日志（含 request_id）
  upstreams/
    dummy.py           模拟后端：正常回声 / 慢响应 / 故障注入控制面
config/gateway.json    网关配置
scripts/run_upstreams.py  一键启动 3 个本地模拟上游（9001-9003）
tests/                 单元 + 并发 + 故障切换/熔断恢复 + 日志断言
```

## 快速开始

需要 Python 3.14+，使用 [uv](https://docs.astral.sh/uv/) 管理依赖。

```bash
# 1) 安装依赖（含 pytest）
uv sync

# 2) 终端 A：启动 3 个模拟上游（127.0.0.1:9001/9002/9003）
uv run python -m scripts.run_upstreams

# 3) 终端 B：启动网关（127.0.0.1:8080）
uv run uvicorn main:app --host 127.0.0.1 --port 8080

# 4) 验证
curl -s 'http://127.0.0.1:8080/api/echo?q=hello'      # 按权重分配到 api-1/2/3
curl -s http://127.0.0.1:8080/_gateway/stats          # 健康/在途/熔断快照
```

## 路由规则

- 路由在 `config.routes` 中定义，按 **最长前缀优先** 排序后匹配；
  `/api` 与 `/api/v2` 可并存，请求会命中更具体的前缀。
- 命中后按 `strip_prefix` 决定是否剥离前缀再转发（默认剥离）：
  `GET /api/items/42 -> http://<instance>/items/42`。
- 查询参数原样保留（允许重复键）；方法、请求体（整体缓存以支持重试重放）、
  端到端头部被保留。
- 无匹配路由返回 `404`，错误体仍是统一结构。

## 负载均衡、健康探测与故障切换

- 每个 upstream 组可有多个实例，选择采用**平滑加权轮询（smooth WRR）**：
  每个实例按 `weight` 比例获得流量，长期偏差有界——权重小的实例不会被
  饿死，权重为 0 的实例完全不参与选择（但仍参与健康探测）。
- 候选实例必须同时满足：最近一次健康探测通过 **且** 熔断器允许
  **且** 有效权重大于 0 **且** 未被摘除，任一不满足即跳过。
- 按权重选中的实例若在途额度已满，会在**有界次数内**（候选实例数）
  换到次优实例；全部占满立即返回 503，不排队、不击穿任何单实例上限。
- **有效权重 = 配置权重 x 爬坡系数**：实例从不合格变为合格（恢复健康、
  权重 0->正、摘除后重新纳入）时，有效权重在 `weight_warmup` 秒内从
  10% 线性爬升到 100%，避免刚恢复的实例被瞬间压垮；每次恢复都重新
  爬坡，反复故障/恢复行为一致。
- 健康探测为后台守护线程，周期性 GET `health_path`：
  - 启动时先探测一轮，未通过的实例不接流量；
  - 失败的实例立即摘除（`healthy=false`），不再接收新请求；
  - 恢复后由探针驱动熔断器 `open -> half_open -> closed`，自动重新加入。
- 请求遇到连接错误 / 超时 / 上游 5xx 时**有界重试**：
  - 最多 `max_retries + 1` 次尝试（默认 2 次重试），绝不无限重试；
  - 重试优先选择本次**尚未尝试过**的实例；
  - 默认仅幂等方法（GET/HEAD/OPTIONS/PUT/DELETE）重试；
    POST 等非幂等方法不重试，且上游 5xx 会被统一映射为 502，
    不会把上游内部异常当成功透传。

## 限流策略

- 按客户端（客户端 IP）维度，每个客户端一个**令牌桶**：
  - 容量 `burst`（突发额度），稳态填充速率 `rate_per_second`；
  - 取令牌在锁内完成「补充 + 扣减」，并发下恰好放行 `burst` 个，不被击穿；
  - 空闲桶惰性过期。
- 超限返回 **429**，带 `Retry-After: 1`。
- 每个实例还有独立的**在途请求上限** `max_inflight`（有界信号量）：
  额度占满时该实例被跳过；全部实例占满则立即返回 503，不排队堆积。
  释放在 `finally` 中完成，客户端中途断开也会回收，杜绝计数泄漏。

## 熔断策略（每实例独立状态机）

```
closed ── 连续失败达到 failure_threshold ──▶ open
open   ── 距打开超过 open_state_ttl，来一个请求 ──▶ half_open（仅放 1 个探测）
half_open ── 任意失败 ──▶ open（重新计时）
half_open ── 连续成功达到 recover_threshold ──▶ closed
```

- open 阶段该实例直接被跳过（快速失败，避免故障扩散）；
- 半开期间只允许一次探测流量，其余请求走其他实例；
- 健康探针与真实请求都会驱动状态迁移，状态变化必有结构化日志。

## 灰度控制面：运行时调权、摘除与重新纳入

无需重启即可调整流量分配（全部通过 HTTP 控制面完成，变更立即生效）：

```bash
# 调整权重（0 = 不再接新流量，但仍参与健康探测）
curl -X POST http://127.0.0.1:8080/_gateway/upstreams/api/instances/api-2/weight \
  -H 'Content-Type: application/json' -d '{"weight": 0}'

# 摘除实例（等价于从配置中移除）：不再接收新请求，
# 已经在它上面的在途请求正常跑完，不会中途失败
curl -X POST http://127.0.0.1:8080/_gateway/upstreams/api/instances/api-2/remove

# 重新纳入：从爬坡起点（10% 有效权重）逐步把流量加回来
curl -X POST http://127.0.0.1:8080/_gateway/upstreams/api/instances/api-2/reinclude
```

- 三类「摘除」语义一致：权重置 0、控制面 `remove`、健康探测失败，
  都只是把实例移出候选集合；在途请求的在途额度与实例对象同生命周期，
  摘除动作不影响已开始的请求。
- 重新纳入一律走爬坡（`weight_warmup`），可以反复摘除/恢复，
  每轮行为一致，不存在「摘掉一次就回不来」的状态残留。
- 与限流/熔断的关系：限流在选实例**之前**按客户端扣一次令牌，
  实例切换（在途占满换下一个、重试换实例）不会重复扣配额；
  权重只在「健康 + 熔断允许 + 未摘除」的候选集合内起作用，
  不会绕过单实例在途上限，也不会向熔断打开的实例导流。

## 三类拒绝原因与稳定状态码

| 场景 | reason | HTTP |
| --- | --- | --- |
| 超出客户端配额 | `rate_limited` | **429** |
| 无可用上游（熔断/不健康/在途全满） | `no_upstream_available` | **503** |
| 上游在超时时间内无响应 | `upstream_timeout` | **504** |
| 有界重试耗尽 / 不可重试的上游 5xx | `upstream_failure` | **502** |
| 未匹配路由 | `no_upstream_available` | 404 |
| 网关内部错误 | `internal_error` | 500 |

统一错误响应：

```json
{"error": {"reason": "upstream_timeout",
           "message": "上游在 3 次尝试后仍失败（有界重试已耗尽）",
           "request_id": "d6f4499fe802"}}
```

所有错误与转发响应都带 `X-Request-Id`、`Via`，且 `Cache-Control`
必含 `no-store`（与上游指令取更严格的并集）。

## 头部处理

- 转发前剥离逐跳头部：`connection`、`keep-alive`、`proxy-authenticate`、
  `proxy-authorization`、`te`、`trailer`、`transfer-encoding`、`upgrade`，
  以及由网关重新生成的 `content-length`、`x-request-id`。
- 追加 `X-Forwarded-For`（在已有值后追加客户端 IP）、`X-Forwarded-Proto`、
  `X-Request-Id`（尊重客户端传入值）与 `Via`。

## 配置项（`config/gateway.json`）

| 路径 | 字段 | 含义 |
| --- | --- | --- |
| `rate_limit` | `rate_per_second` | 每客户端稳态令牌速率 |
| `rate_limit` | `burst` | 突发额度（桶容量） |
| `upstreams.<id>` | `instances[].id/url` | 实例标识与基础 URL |
| | `instances[].weight` | 静态权重（缺省 1；0 = 不接新流量但仍被探测） |
| | `weight_warmup` | 恢复/重新纳入后有效权重从 10% 爬到 100% 的秒数 |
| | `health_path` / `health_interval` / `health_timeout` | 探测路径/周期/超时 |
| | `max_inflight` | 单实例在途请求上限 |
| | `failure_threshold` | 触发熔断的连续失败数 |
| | `recover_threshold` | 半开到关闭所需连续成功数 |
| | `open_state_ttl` | open 持续时间，过后允许探测 |
| `routes[]` | `prefix` / `upstream` | 匹配前缀与目标 upstream 组 |
| | `strip_prefix` | 转发时是否剥离前缀 |
| | `timeout` | 单次上游调用超时（秒） |
| | `max_retries` | 最大**重试**次数（总尝试 = 1 + 该值） |

## 观测

- `GET /_gateway/stats`：各实例健康位、`weight`/`effective_weight`/`removed`、
  在途/上限、熔断器状态快照与限流参数。
- 日志为单行 JSON，关键事件：
  `route_selected`（路由决策）、`upstream_selected`（上游选择）、
  `rate_limited`、`upstream_timeout`、`upstream_5xx`、
  `no_upstream`、`retries_exhausted`、
  `breaker_state_changed`（closed/open/half_open 迁移）、
  `upstream_unhealthy` / `upstream_recovered`、
  `weight_changed` / `instance_removed` / `instance_reincluded`（灰度控制面）。

## 模拟上游的故障注入控制面

对任一实例（9001/9002/9003）调用：

```bash
curl -X POST http://127.0.0.1:9001/control/fail      # 健康与业务均失败(500)
curl -X POST http://127.0.0.1:9001/control/timeout   # 请求挂起（模拟超时）
curl -X POST http://127.0.0.1:9001/control/slow      # 用 /slow?delay= 控制慢响应
curl -X POST http://127.0.0.1:9001/control/recover   # 恢复正常
```

手动验证示例：

```bash
# 限流（burst=10，连续 14 个请求）
for i in $(seq 1 14); do curl -s -o /dev/null -w '%{http_code} ' \
  http://127.0.0.1:8080/api/echo; done; echo   # 10 个 200 后 429

# 全部慢响应 -> 有界重试切换 3 个实例后 504
curl -s -w '\n%{http_code}\n' 'http://127.0.0.1:8080/api/slow?delay=3'

# 单实例故障：业务无感切换；全部故障：503；恢复后自动回到轮询
curl -X POST http://127.0.0.1:9001/control/fail
```

灰度流量本地验证（权重、摘除、爬坡）：

> 默认配置 `rate_limit=5/s, burst=10` 是为限流演示准备的；观察流量分布前
> 请先把 `config/gateway.json` 中的 `rate_limit` 调大（如
> `rate_per_second=100000, burst=100000`）并重启网关，否则大部分请求会
> 被 429 拦掉，看不到真实分布。

```bash
# 1) 基线：等权重时三实例均分
for i in $(seq 1 30); do curl -s http://127.0.0.1:8080/api/echo; echo; done \
  | grep -o '"instance":"[^"]*"' | sort | uniq -c

# 2) 调成 1:1:8，流量大头落到 api-3（100 个请求约 10/10/80）
curl -X POST http://127.0.0.1:8080/_gateway/upstreams/api/instances/api-3/weight \
  -H 'Content-Type: application/json' -d '{"weight": 8}'

# 3) 摘除 api-1：新请求不再落入，stats 中 removed=true；
#    正在执行的慢请求（另开一个终端 curl /api/slow?delay=5）不受影响
curl -X POST http://127.0.0.1:8080/_gateway/upstreams/api/instances/api-1/remove
curl -s http://127.0.0.1:8080/_gateway/stats | python -m json.tool

# 4) 重新纳入：观察 effective_weight 从 10% 逐步爬回 100%（weight_warmup 秒）
curl -X POST http://127.0.0.1:8080/_gateway/upstreams/api/instances/api-1/reinclude
watch -n 1 'curl -s http://127.0.0.1:8080/_gateway/stats | grep effective_weight'
# 爬坡期间的分布示例（前 2 秒）：api-1 明显少于另外两个
#   2 "instance":"api-1"   19 "instance":"api-2"   19 "instance":"api-3"
```

## 测试

测试在进程内启动 3 个模拟上游与网关（线程中跑 uvicorn），无需外部进程：

```bash
uv run pytest                      # 全部用例
uv run pytest tests/test_units.py  # 令牌桶/熔断/在途信号量纯单测
```

覆盖内容：

- **并发限流**：200 线程争抢，放行数恰好等于 burst，配额不越界；
  补充速率随时间正确恢复。
- **在途计数**：`max_inflight=1` + 12 路并发慢请求，监控线程断言单实例
  在途恒不超过 1，超额得到 503，结束后计数归零。
- **客户端断开**：对真实 uvicorn 发原始 HTTP 请求后中途关闭 socket，
  断言在途计数最终全部回收（不泄漏）。
- **故障切换**：单实例 500 时 GET 在其他实例重试成功；全部 500 时
  重试次数有界（恰好 `max_retries+1`）并返回 502；全慢返回 504。
- **熔断恢复**：故障后 `closed->open` 摘除流量，恢复后
  `open->half_open->closed` 并重新参与轮询。
- **权重分配**：1:2:3 / 1:1:8 下分布偏差有界、小权重不饿死、
  并发压力下比例保持；权重 0 不接流量但仍被健康探测。
- **运行时调权**：控制面调权/摘除/重新纳入立即生效，非法输入 400/404。
- **摘除语义**：摘除瞬间在途慢请求正常跑完，新请求不再落入被摘除实例。
- **爬坡恢复**：重新纳入后早期份额显著低于稳态，爬坡结束后回到均分；
  反复注入故障再恢复 3 轮，每轮稳态分布一致。
- **与配额配合**：高权重实例在途占满时有界切换到其他实例（不越界），
  实例切换不重复消耗客户端限流配额；全部权重为 0 时仍是 503。

运行测试时，每个用例会打印一行 `[场景] <用例> -> PASSED/FAILED | 场景说明`，
可直接从输出确认覆盖了哪些场景及各自结果（`uv run pytest` 即可，无需 -v）。
- **可区分性**：429 / 503 / 504 / 502 的状态码与 reason 字段稳定可区分。
- **日志**：断言出现路由决策、上游选择与熔断器状态变化事件。
  加 `-s` 可直接观察 JSON 日志。
