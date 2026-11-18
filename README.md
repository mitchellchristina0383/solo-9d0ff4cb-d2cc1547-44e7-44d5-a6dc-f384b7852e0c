# 熔断网关（Circuit-Breaker Gateway）

值班止损场景：上游连续出错时两台网关**共享熔断状态**自动停止转发，上游恢复后
仅放行一笔探测，探测成功再接流。FastAPI + SQLite（WAL）实现，浏览器即可操作。

## 架构

```
浏览器 ──► gateway1 (容器 8000 → 宿主 8001) ─┐
       └► gateway2 (容器 8000 → 宿主 8002) ─┴──► flaky-upstream (宿主 8000)
                  两台网关挂载同一个 Docker volume，
                  熔断状态存于 /data/breaker.db（SQLite WAL）
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
   探测期间其余请求立即 503（`probe_in_flight`）。
4. 探测成功 → `CLOSED` 恢复转发；探测失败 → 重新冷却 2 秒。
5. 探测超过 3 秒无结论（超时、实例被 kill）→ 另一台实例可**回收并重新探测**。
6. **迟到结果防护**：每笔决策带 token（普通请求校验 `epoch+CLOSED`，探测校验
   `probe_token+HALF_OPEN`）；失效探测和旧请求的迟到回包一律忽略，不能改写新结论。

关键参数可经环境变量调整：`CB_FAILURE_THRESHOLD`、`CB_COOLDOWN_SECONDS`、
`CB_PROBE_TIMEOUT_SECONDS`、`UPSTREAM_TIMEOUT`。

## 启动（Docker Compose）

```bash
docker compose up --build
# 网页控制台（任选一个入口，页面内可切换入口地址）
#   http://localhost:8001/   或   http://localhost:8002/
```

页面功能：切换上游模式、发 1/3/5 笔或并发 10 笔 GET、查看每笔请求结果、
熔断状态（状态徽章、失败计数、冷却剩余、探测属主）与两实例共享事件流。

### 无 Docker 环境的本地等效启动

```bash
./scripts/run_local.sh        # 三个 uvicorn 进程，共享 ./data/breaker.db
./scripts/stop_local.sh       # 停止
```

## 自测

```bash
# 单元测试（状态机、并发、迟到结果、探测回收）
python3 -I tests/test_breaker.py

# 端到端：先起本地三进程，再执行（53 项检查：5xx/超时/慢回包/失联/实例重启）
./scripts/run_local.sh
python3 -I tests/test_integration.py
```

## 主要 HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/proxy` | 经熔断转发到上游；熔断时 503，超时 504，失联 502 |
| GET | `/api/state` | 当前熔断状态（含冷却剩余、探测属主） |
| GET | `/api/events?limit=20` | 共享事件流 |
| POST | `/api/upstream/mode` | 经网关切换上游模式 `{"mode":"normal|slow|error"}` |
| GET | `/health` | 网关健康检查 |
