"""Circuit-breaker gateway.

Forwards GET requests to the flaky upstream through a shared SQLite-backed
breaker.  Two instances run behind no load balancer: the web UI talks to each
entry point directly (ports 8001 / 8002) so either instance can be driven.

When the breaker rejects a request but a fresh shared fallback replica exists
(age <= CB_FALLBACK_TTL_SECONDS, default 30s), the gateway degrades: it serves
the stored upstream status code and body, clearly marked as a fallback with the
capture timestamp.  Expired or missing replicas still produce a plain 503.
"""

from __future__ import annotations

import os
import socket
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response

from .breaker import Breaker

INSTANCE_ID = os.getenv("INSTANCE_ID") or f"gw-{socket.gethostname()}"
UPSTREAM_URL = os.getenv("UPSTREAM_URL", "http://upstream:8000")
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "2"))
DB_PATH = os.getenv("CB_DB_PATH", "/data/breaker.db")

breaker = Breaker(DB_PATH, INSTANCE_ID)
_client: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _client
    _client = httpx.AsyncClient(timeout=UPSTREAM_TIMEOUT)
    yield
    await _client.aclose()


app = FastAPI(title=f"breaker-gateway ({INSTANCE_ID})", version="1.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE_ID}


@app.get("/", response_class=HTMLResponse)
async def index():
    return INDEX_HTML


@app.get("/api/state")
async def api_state():
    return {
        "instance": INSTANCE_ID,
        "upstream_url": UPSTREAM_URL,
        "upstream_timeout": UPSTREAM_TIMEOUT,
        "breaker": breaker.get_state(),
    }


@app.get("/api/events")
async def api_events(limit: int = 30):
    return {"instance": INSTANCE_ID, "events": breaker.get_events(limit)}


@app.get("/api/replica")
async def api_replica():
    """The shared fallback replica (or null), including freshness and age."""
    return {"instance": INSTANCE_ID, "replica": breaker.get_replica()}


@app.post("/api/upstream/mode")
async def api_set_mode(request: Request):
    """Convenience: switch upstream mode through this gateway entry point."""
    body = await request.body()
    try:
        r = await _client.post(f"{UPSTREAM_URL}/mode", content=body,
                               headers={"content-type": "application/json"})
        return JSONResponse(status_code=r.status_code, content=r.json())
    except httpx.HTTPError as exc:
        return JSONResponse(
            status_code=502,
            content={"error": f"cannot reach upstream to switch mode: {exc!r}"},
        )


@app.get("/api/upstream/mode")
async def api_get_mode():
    try:
        r = await _client.get(f"{UPSTREAM_URL}/mode")
        return r.json()
    except httpx.HTTPError as exc:
        return JSONResponse(
            status_code=502,
            content={"error": f"upstream unreachable: {exc!r}", "reachable": False}
        )


@app.get("/proxy")
async def proxy():
    """The guarded GET path, with shared fallback on breaker rejection."""
    decision = breaker.dispatch()

    if decision["action"] == "reject":
        degraded = _degraded_response(decision["reason"])
        if degraded is not None:
            return degraded
        return JSONResponse(
            status_code=503,
            content={
                "gateway": INSTANCE_ID,
                "rejected": True,
                "reason": decision["reason"],
                "decision": decision,
                "breaker": breaker.get_state(),
            },
        )

    # Forward (normal request or the single recovery probe).
    try:
        r = await _client.get(UPSTREAM_URL + "/")
    except httpx.TimeoutException:
        new_state = breaker.record_failure(decision, "timeout")
        return JSONResponse(
            status_code=504,
            content={
                "gateway": INSTANCE_ID,
                "outcome": "timeout",
                "applied": new_state is not None,
                "breaker": new_state or breaker.get_state(),
            },
        )
    except httpx.HTTPError as exc:
        new_state = breaker.record_failure(decision, f"connection_error:{type(exc).__name__}")
        return JSONResponse(
            status_code=502,
            content={
                "gateway": INSTANCE_ID,
                "outcome": "connection_error",
                "detail": repr(exc),
                "applied": new_state is not None,
                "breaker": new_state or breaker.get_state(),
            },
        )

    if r.status_code >= 500:
        # A failure never refreshes the fallback replica.
        new_state = breaker.record_failure(decision, f"http_{r.status_code}")
        return JSONResponse(
            status_code=r.status_code,
            content={
                "gateway": INSTANCE_ID,
                "outcome": "upstream_5xx",
                "upstream_body": _safe_json(r),
                "applied": new_state is not None,
                "breaker": new_state or breaker.get_state(),
            },
        )

    # Confirmed upstream success: the breaker applies it only when the token
    # still matches; the replica is refreshed inside that same transaction.
    content_type = r.headers.get("content-type", "application/json")
    upstream_packet = {
        "status_code": r.status_code,
        "content_type": content_type,
        "body_text": r.text,
        "body_is_json": "json" in content_type.lower(),
    }
    new_state = breaker.record_success(decision, upstream_packet)
    return JSONResponse(
        status_code=r.status_code,
        content={
            "gateway": INSTANCE_ID,
            "outcome": "success",
            "upstream_body": _safe_json(r),
            "applied": new_state is not None,
            "breaker": new_state or breaker.get_state(),
        },
    )


def _degraded_response(reason: str) -> Response | None:
    """Serve the shared fallback replica while the breaker rejects.

    Returns None when no replica exists or it is past its TTL: the caller then
    issues a normal 503.  Reading the replica never extends its validity.
    """
    replica = breaker.get_replica()
    if replica is None or replica["expired"]:
        return None
    headers = {
        "X-Gateway-Degraded": "1",
        "X-Replica-Age": str(replica["age_seconds"]),
        "X-Replica-Captured-At": str(replica["captured_at"]),
        "X-Replica-Version": str(replica["version"]),
        # RFC 7234 warning: 110 = Response is stale.
        "Warning": f'110 {INSTANCE_ID} "fallback replica; captured_at='
                   f'{_iso_utc(replica["captured_at"])}; age={replica["age_seconds"]}s"',
    }
    payload = {
        "degraded": True,
        "gateway": INSTANCE_ID,
        "reject_reason": reason,
        "fallback": {
            "upstream_status": replica["status_code"],
            "upstream_content_type": replica["content_type"],
            "captured_at": replica["captured_at"],
            "captured_at_iso": _iso_utc(replica["captured_at"]),
            "captured_by": replica["captured_by"],
            "age_seconds": replica["age_seconds"],
            "ttl_seconds": replica["ttl_seconds"],
            "remaining_seconds": replica["remaining_seconds"],
            "version": replica["version"],
        },
        "upstream_body": replica["body"],
    }
    return JSONResponse(status_code=replica["status_code"], content=payload, headers=headers)


def _iso_utc(epoch_seconds: float) -> str:
    return datetime.fromtimestamp(epoch_seconds, tz=timezone.utc).isoformat()


def _safe_json(r: httpx.Response):
    try:
        return r.json()
    except Exception:
        return r.text


INDEX_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>熔断网关控制台</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: system-ui, "Segoe UI", sans-serif; margin: 0; padding: 1.2rem;
         background: #0f1115; color: #e6e6e6; }
  h1 { font-size: 1.25rem; margin: 0 0 .8rem; }
  .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  @media (max-width: 900px) { .grid { grid-template-columns: 1fr; } }
  .card { background: #181b22; border: 1px solid #2a2f3a; border-radius: 10px;
          padding: 1rem; }
  .card h2 { font-size: .95rem; margin: 0 0 .6rem; color: #9fb4ff; }
  button { border: 0; border-radius: 8px; padding: .5rem .8rem; margin: .2rem .2rem .2rem 0;
           cursor: pointer; font-weight: 600; }
  button.go { background: #2f81f7; color: #fff; }
  button.mode { background: #30363d; color: #e6e6e6; }
  button.mode.active { background: #238636; }
  button:disabled { opacity: .45; cursor: default; }
  pre { background: #0d1117; border: 1px solid #2a2f3a; border-radius: 8px;
        padding: .7rem; overflow:auto; max-height: 320px; font-size: .78rem; }
  .pill { display:inline-block; padding: .15rem .55rem; border-radius: 999px;
          font-size: .78rem; font-weight: 700; }
  .CLOSED { background:#1a7f37; color:#fff; }
  .OPEN { background:#da3633; color:#fff; }
  .HALF_OPEN { background:#9e6a03; color:#fff; }
  .DEGRADED { background:#bf6b00; color:#fff; }
  .FRESH { background:#1a7f37; color:#fff; }
  .EXPIRED { background:#6e7681; color:#fff; }
  table { width:100%; border-collapse: collapse; font-size: .78rem; }
  th, td { text-align:left; padding: .25rem .35rem; border-bottom:1px solid #2a2f3a;
           vertical-align: top; }
  .muted { color:#8b949e; font-size:.78rem; }
  .row { display:flex; flex-wrap:wrap; gap:.5rem; align-items:center; }
  input { background:#0d1117; border:1px solid #2a2f3a; color:#e6e6e6;
          border-radius:8px; padding:.45rem .6rem; width: 14rem; }
</style>
</head>
<body>
<h1>熔断网关控制台 <span class="muted" id="who"></span></h1>
<div class="grid">
  <div class="card">
    <h2>1. 选择网关入口</h2>
    <div class="row">
      <label class="muted">入口地址</label>
      <input id="base" value="">
      <button class="go" onclick="saveBase()">保存</button>
    </div>
    <p class="muted">两台网关共享同一份 SQLite 熔断状态，任选其一操作即可；
       也可分别打开两个浏览器标签指向 8001 / 8002 观察联动。</p>
  </div>

  <div class="card">
    <h2>2. 上游模式（任一台网关切换均生效）</h2>
    <div class="row">
      <button class="mode" onclick="setMode('normal')">正常 normal</button>
      <button class="mode" onclick="setMode('slow')">慢响应 slow（5s）</button>
      <button class="mode" onclick="setMode('error')">500 错误 error</button>
    </div>
    <p class="muted" id="upmode"></p>
  </div>

  <div class="card">
    <h2>3. 发送 GET（经熔断转发；熔断时优先返回共享降级副本）</h2>
    <div class="row">
      <button class="go" onclick="send(1)">发 1 笔</button>
      <button class="go" onclick="send(3)">连发 3 笔</button>
      <button class="go" onclick="send(5)">连发 5 笔</button>
      <button class="go" onclick="send(10)">并发 10 笔</button>
    </div>
    <pre id="result">（结果显示在这里；降级回包带 <b>degraded:true</b> 与采集时间）</pre>
  </div>

  <div class="card">
    <h2>4. 共享降级副本（两实例同一份，读取不续期） <span id="reppill"></span></h2>
    <pre id="replica">（尚无副本：需先有一笔被网关确认的上游成功）</pre>
    <p class="muted">仅当熔断拒绝且副本年龄不超过 TTL（默认 30 秒）时才返回降级结果；
       过期或没有副本仍返回 503。副本只由“确认有效”的上游成功回包刷新，
       探测失败与迟到成功都不会覆盖它。</p>
  </div>

  <div class="card">
    <h2>5. 熔断状态 <span id="statepill"></span></h2>
    <pre id="state"></pre>
    <h2 style="margin-top:.8rem">事件流（两台实例共享）</h2>
    <table><thead><tr><th>#</th><th>距今(s)</th><th>实例</th><th>事件</th><th>详情</th></tr></thead>
    <tbody id="events"></tbody></table>
  </div>
</div>

<script>
const $ = (id) => document.getElementById(id);
function defaultBase() {
  return location.origin || `http://${location.hostname}:8001`;
}
$("base").value = localStorage.getItem("gwBase") || defaultBase();
function saveBase() { localStorage.setItem("gwBase", $("base").value); refresh(); }
const base = () => $("base").value.replace(/\\/$/, "");

async function api(path, opts) {
  const r = await fetch(base() + path, opts);
  const text = await r.text();
  let body; try { body = JSON.parse(text); } catch { body = text; }
  return { status: r.status, body };
}

async function setMode(m) {
  const { status, body } = await api("/api/upstream/mode", {
    method: "POST", headers: {"content-type": "application/json"},
    body: JSON.stringify({mode: m})
  });
  $("result").textContent = `切换模式 -> ${m}\\nHTTP ${status}\\n` + JSON.stringify(body, null, 2);
  refresh();
}

async function sendOne() {
  try { return await api("/proxy"); }
  catch (e) { return { status: 0, body: { error: String(e) } }; }
}
async function send(n) {
  const parallel = n >= 10;
  let outcomes;
  if (parallel) outcomes = await Promise.all(Array.from({length: n}, sendOne));
  else {
    outcomes = [];
    for (let i = 0; i < n; i++) outcomes.push(await sendOne());
  }
  $("result").innerHTML = outcomes.map((o, i) => {
    const tag = (o.body && o.body.degraded)
      ? ` <span class="pill DEGRADED">降级 fallback · 采集于 ${o.body.fallback.age_seconds}s 前</span>`
      : "";
    return `#${i+1} HTTP ${o.status}:${tag}\\n` + JSON.stringify(o.body);
  }).join("\\n\\n");
  refresh();
}

function renderReplica(rep) {
  if (!rep) {
    $("reppill").innerHTML = "";
    $("replica").textContent = "（尚无副本）";
    return;
  }
  $("reppill").innerHTML =
    `<span class="pill ${rep.expired ? "EXPIRED" : "FRESH"}">` +
    (rep.expired ? "已过期" : "新鲜可降级") + "</span>";
  $("replica").textContent = JSON.stringify({
    version: rep.version,
    upstream_status: rep.status_code,
    content_type: rep.content_type,
    captured_at_iso: new Date(rep.captured_at * 1000).toISOString(),
    captured_by: rep.captured_by,
    age_seconds: rep.age_seconds,
    ttl_seconds: rep.ttl_seconds,
    remaining_seconds: rep.remaining_seconds,
    epoch: rep.epoch,
    body: rep.body
  }, null, 2);
}

async function refresh() {
  try {
    const [{body: st}, {body: ev}, repResp, up] = await Promise.all([
      api("/api/state"), api("/api/events?limit=20"),
      api("/api/replica"),
      api("/api/upstream/mode").catch(e => ({body: {error: String(e)}}))
    ]);
    renderReplica(repResp.body && repResp.body.replica);
    $("who").textContent = "当前入口实例: " + st.instance;
    const b = st.breaker;
    $("statepill").innerHTML =
      `<span class="pill ${b.state}">${b.state}</span>`;
    $("state").textContent = JSON.stringify({
      state: b.state,
      failure_count: b.failure_count,
      threshold: b.failure_threshold,
      epoch: b.epoch,
      cooldown_remaining_s: b.cooldown_remaining,
      probe_instance: b.probe_instance,
      probe_age_s: b.probe_started_at != null
        ? Number((b.now - b.probe_started_at).toFixed(3)) : null,
      fallback_ttl_s: b.fallback_ttl_seconds,
      replica_summary: b.replica
    }, null, 2);
    $("events").innerHTML = (ev.events || []).map(e =>
      `<tr><td>${e.id}</td><td>${e.age}</td><td>${e.instance}</td>
       <td>${e.kind}</td><td>${e.detail}</td></tr>`).join("");
    document.querySelectorAll(".mode").forEach(btn => btn.classList.remove("active"));
    if (up && up.mode) {
      $("upmode").textContent = "上游当前模式: " + up.mode + "，累计调用: " + up.calls;
      const map = {normal:0, slow:1, error:2};
      const idx = map[up.mode];
      if (idx != null) document.querySelectorAll(".mode")[idx].classList.add("active");
    } else {
      $("upmode").textContent = "上游不可达（失联）: " + JSON.stringify(up);
    }
  } catch (e) {
    $("state").textContent = "刷新失败: " + e;
  }
}
refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
"""
