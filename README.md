# 熔断网关（Circuit-Breaker Gateway）

值班止损场景：上游连续出错时两台网关**共享熔断状态**自动停止转发，上游恢复后
仅放行一笔探测，探测成功再接流；熔断拒绝期间用**共享降级副本**继续返回上一次
确认成功的 GET。FastAPI + SQLite（WAL）实现，浏览器即可操作。

## 架构

```
浏览器 ──► gateway1 (容器 8000 → 宿主 8001) ─┐
       └► gateway2 (容器 8000 → 宿主 8002) ─┴──► flaky-upstream (宿主 8000)
                  两台网关挂载同一个 Docker volume，
        熔断状态与降级副本存于 /data/breaker.db（SQLite WAL）
```

- `app/breaker.py`：熔断核心，纯标准库。所有状态迁移在 `BEGIN IMMEDIATE`
  事务内完成，跨进程串行化；WAL + `busy_timeout=10s` 支持两实例并发读写。
- `app/gateway_app.py`：网关，含 `/proxy`（受保护的 GET）、管理 API 与内嵌网页。
- `app/upstream_app.py`：可切换 `normal` / `slow`（5 秒）/ `error`（500）的上游。
  失联场景用 `docker compose stop upstream` 真实模拟。

## 熔断规则（与需求逐条对应）

1. **连续 3 次** 5xx 或超时（按完成顺序计数）→ `OPEN`，两台网关立即 503 拒绝，
   不再访问上游。
2. CLOSED 下任一成功响应把失败计数清零。
3. `OPEN` 冷却 **2 秒**后进入 `HALF_OPEN`，跨实例**只放行一笔恢复探测**，
   探测期间其余请求立即拒绝（`probe_in_flight`）。
4. 探测成功 → `CLOSED` 恢复转发；探测失败 → 重新冷却 2 秒。
5. 探测超过 3 秒无结论（超时、实例被 kill）→ 另一台实例可**回收并重新探测**。
6. **迟到结果防护**：每笔决策带 token（普通请求校验 `epoch+CLOSED`，探测校验
   `probe_token+HALF_OPEN`）；失效探测和旧请求的迟到回包一律忽略，不能改写新结论。

## 共享降级副本（fallback replica）

熔断拒绝不等于直接报错：网关注解里还维护一行两实例共享的降级副本。

1. **只有网关确认有效的上游成功回包才能更新副本**：回包状态码 < 500，且
   `record_success` 校验 token 仍然有效（当前仍处于 CLOSED 同 epoch，或探测
   token 仍匹配 HALF_OPEN）。刷新与熔断状态迁移在**同一个 `BEGIN IMMEDIATE`
   事务**内完成；5xx、超时、连接错误、探测失败、迟到/失效成功一律不写副本。
2. **熔断拒绝时（`circuit_open` / `probe_in_flight`）**：若副本距采集时间
   **不超过 30 秒**（`CB_FALLBACK_TTL_SECONDS`，本地演练默认 4 秒），返回副本里
   保存的**上游状态码与正文**，并显式标明这是降级结果：响应体 `degraded:true`、
   `reject_reason`、`fallback.captured_at(_iso)`、`fallback.age_seconds`、
   `ttl_seconds`、`remaining_seconds`、`version` 与采集实例；响应头带
   `X-Gateway-Degraded: 1`、`X-Replica-Age`、`X-Replica-Captured-At`、
   `X-Replica-Version` 与 RFC 7234 `Warning: 110 ...`。
3. **副本过期或根本没有副本 → 仍返回普通 503**，不伪造成功。
4. **读取不延长有效期**：`/proxy` 降级读和 `/api/replica` 都是纯读，采集时间戳
   与 TTL 不变；年龄随时间单调增长，只有新的确认成功才能把年龄清零。
5. 每轮冷却后仍只放行**一笔真实恢复探测**；探测失败不刷新副本，探测成功才在
   关断熔断的同一事务里刷新副本并恢复转发。
6. 失效探测与旧请求的迟到成功因 token 校验失败直接返回 `None`，**永远无法覆盖
   较新副本**（副本行带单调递增 `version`，单元测试与端到端测试均有覆盖）。

关键参数可经环境变量调整：`CB_FAILURE_THRESHOLD`、`CB_COOLDOWN_SECONDS`、
`CB_PROBE_TIMEOUT_SECONDS`、`CB_FALLBACK_TTL_SECONDS`、`UPSTREAM_TIMEOUT`。

## 启动（Docker Compose）

```bash
docker compose up --build
# 网页控制台（任选一个入口，页面内可切换入口地址）
#   http://localhost:8001/   或   http://localhost:8002/
```

页面功能：切换上游模式、发 1/3/5 笔或并发 10 笔 GET、查看每笔请求结果
（降级回包带橙色“降级”徽章）、熔断状态（状态徽章、失败计数、冷却剩余、
探测属主）、**共享降级副本卡片（上游状态码、正文、采集时间、年龄、TTL、
新鲜/过期徽章）**与两实例共享事件流。两个入口（8001/8002）看到的是同一份
副本与状态，可在页面顶部切换入口地址。

### 无 Docker 环境的本地等效启动

```bash
./scripts/run_local.sh        # 三个 uvicorn 进程，共享 ./data/breaker.db
./scripts/stop_local.sh       # 停止
```

## 自测

```bash
# 单元测试（状态机、并发、迟到结果、探测回收、降级副本语义）
python3 -I tests/test_breaker.py        # 24 项

# 端到端：先起本地三进程，再执行（76 项检查：5xx/超时/慢回包/失联/实例重启/
# 降级副本的共享、30 秒 TTL 过期、读取不续期、探测交错与迟到成功防护）
./scripts/run_local.sh
python3 -I tests/test_integration.py
```

本地演练把 `CB_FALLBACK_TTL_SECONDS` 默认设为 4 秒，便于观察过期；
Docker Compose 中保持需求规定的 30 秒，也可用环境变量覆盖：

```bash
CB_FALLBACK_TTL_SECONDS=2 ./scripts/run_local.sh
```

## 主要 HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/proxy` | 经熔断转发到上游；熔断拒绝且副本新鲜时返回降级副本（原上游状态码，带降级标记与采集时间），过期/无副本时 503，超时 504，失联 502 |
| GET | `/api/state` | 当前熔断状态（含冷却剩余、探测属主、TTL 与副本摘要） |
| GET | `/api/replica` | 共享降级副本全文（正文、采集时间、年龄、新鲜/过期），无副本返回 `null` |
| GET | `/api/events?limit=20` | 共享事件流（含 `replica_refreshed` 事件） |
| POST | `/api/upstream/mode` | 经网关切换上游模式 `{"mode":"normal|slow|error"}` |
| GET | `/health` | 网关健康检查 |
