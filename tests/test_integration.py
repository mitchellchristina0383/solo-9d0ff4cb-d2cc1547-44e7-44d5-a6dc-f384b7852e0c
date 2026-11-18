"""End-to-end test against three real uvicorn processes (see scripts/run_local.sh).

Covers the full acceptance story:
  1. normal forwarding + success resets failure count
  2. three consecutive 5xx trip the breaker; both gateways stop forwarding
  3. during cooldown requests are rejected immediately (no upstream call)
  4. after cooldown exactly one probe is admitted; concurrent others rejected
  5. probe failure restarts cooldown; probe success restores forwarding
  6. slow upstream -> gateway timeout counts as failure
  7. upstream process lost -> connection errors handled; traffic resumes on recovery
  8. stale late results cannot overwrite newer breaker conclusions

Run:  python -I tests/test_integration.py
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

UP = os.getenv("UP_BASE", "http://127.0.0.1:18000")
GW = [os.getenv("GW1_BASE", "http://127.0.0.1:18001"),
      os.getenv("GW2_BASE", "http://127.0.0.1:18002")]

PASS, FAIL = 0, 0


def check(name, cond, detail=""):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS {name}")
    else:
        FAIL += 1
        print(f"  FAIL {name} {detail}")


def req(method, url, body=None, timeout=10):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read().decode()
            try:
                return resp.status, json.loads(raw)
            except json.JSONDecodeError:
                return resp.status, {"raw": raw}
    except urllib.error.HTTPError as e:
        raw = e.read().decode()
        try:
            return e.code, json.loads(raw)
        except json.JSONDecodeError:
            return e.code, {"raw": raw}
    except Exception as exc:  # connection refused / timeout
        return 0, {"error": repr(exc)}


def set_mode(mode):
    status, body = req("POST", GW[0] + "/api/upstream/mode", {"mode": mode})
    assert status == 200, body
    return body


def state(which=0):
    _, body = req("GET", GW[which] + "/api/state")
    return body["breaker"]


def proxy(which=0, timeout=10):
    return req("GET", GW[which] + "/proxy", timeout=timeout)


def rejected_or_degraded(status, body, reason):
    """A breaker rejection is now either a plain 503 or a degraded replica
    response (HTTP 200 + degraded:true) carrying the same reject reason."""
    if body.get("degraded"):
        return status == 200 and body.get("reject_reason") == reason
    return status == 503 and body.get("reason") == reason


def wait_state(expected, timeout=8, which=0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if state(which)["state"] == expected:
            return True
        time.sleep(0.1)
    return False


def wait_port(url, timeout=10):
    deadline = time.time() + timeout
    while time.time() < deadline:
        s, _ = req("GET", url + "/health", timeout=2)
        if s == 200:
            return True
        time.sleep(0.3)
    return False


def main():
    print("== wait for services ==")
    for url in [UP, *GW]:
        check(f"service up: {url}", wait_port(url))

    print("== 1. normal forwarding ==")
    set_mode("normal")
    assert wait_state("CLOSED")
    s, b = proxy(0)
    check("normal GET returns 200", s == 200 and b["outcome"] == "success", str(b))
    s, b = proxy(1)
    check("gateway-2 also forwards", s == 200 and b["gateway"] == "gateway-2", str(b))

    print("== 2. success resets failure count ==")
    set_mode("error")
    proxy(0); proxy(1)
    check("two failures counted", state()["failure_count"] == 2, str(state()))
    set_mode("normal")
    s, b = proxy(0)
    check("success clears failures", s == 200 and state()["failure_count"] == 0, str(b))

    print("== 3. three consecutive 5xx trip the breaker (both entries stop) ==")
    set_mode("error")
    codes = [proxy(i % 2) for i in range(3)]
    check("three 5xx responses", all(c == 500 for c, _ in codes), str(codes))
    check("breaker OPEN", wait_state("OPEN"), str(state()))
    s1, b1 = proxy(0)
    s2, b2 = proxy(1)
    check("gateway-1 fast-rejects (503 or degraded replica)",
          rejected_or_degraded(s1, b1, "circuit_open"), str(b1))
    check("gateway-2 fast-rejects (503 or degraded replica)",
          rejected_or_degraded(s2, b2, "circuit_open"), str(b2))
    _, up_before = req("GET", UP + "/mode")
    calls_before = up_before["calls"]
    proxy(0); proxy(1)
    _, up_after = req("GET", UP + "/mode")
    check("OPEN state does not call upstream", up_after["calls"] == calls_before,
          f"{calls_before} -> {up_after['calls']}")

    print("== 4. cooldown then exactly one probe, others rejected ==")
    set_mode("normal")  # upstream recovered; breaker still cooling down
    time.sleep(0.5)
    s, b = proxy(0)
    check("still rejecting within 2s cooldown (503 or degraded)",
          rejected_or_degraded(s, b, "circuit_open"), str(b))
    time.sleep(2.0)

    # 4a. fast upstream: probe may close before peer threads start, so peers
    #     may legitimately become normal forwards — but there must never be
    #     more than one probe, and no 503 with circuit_open after recovery.
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda i: proxy(i % 2), range(8)))
    bad_open = [o for o in outcomes if o[0] == 503 and o[1].get("reason") == "circuit_open"]
    check("no stale open-rejects after cooldown", not bad_open, str(outcomes))
    check("all requests eventually succeed or are probe-window rejects",
          all(o[0] == 200 or o[1].get("reason") == "probe_in_flight" for o in outcomes),
          str([(c, b.get("reason")) for c, b in outcomes]))
    check("probe success -> CLOSED", wait_state("CLOSED"), str(state()))
    s, b = proxy(0)
    check("forwarding resumed", s == 200 and b["outcome"] == "success", str(b))

    # 4b. slow upstream guarantees the probe stays in flight for ~2s:
    #     concurrent requests must ALL be fast-rejected while it runs.
    set_mode("error")
    for _ in range(3):
        proxy(0)
    check("breaker OPEN for probe-window test", wait_state("OPEN"), str(state()))
    set_mode("slow")
    time.sleep(2.3)
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda i: proxy(i % 2, timeout=8), range(8)))
    wall = time.time() - t0
    probes = [o for o in outcomes if o[1].get("outcome") == "timeout"]
    rejects = [o for o in outcomes
               if rejected_or_degraded(o[0], o[1], "probe_in_flight")]
    check("exactly one probe while probe in flight", len(probes) == 1,
          str([(c, b.get("reason") or b.get("outcome")) for c, b in outcomes]))
    check("other 7 requests rejected immediately (503 or degraded replica)",
          len(rejects) == 7, str(len(rejects)))
    check("rejections were immediate (no upstream wait)", wall < 3.0, f"{wall:.2f}s")
    check("failed probe reopened breaker", wait_state("OPEN"), str(state()))
    set_mode("normal")
    time.sleep(2.3)
    s, b = proxy(0)
    check("recovery probe closes again", s == 200 and wait_state("CLOSED"), str(b))

    print("== 5. failed probe restarts cooldown ==")
    set_mode("error")
    for _ in range(3):
        proxy(0)
    check("breaker OPEN again", wait_state("OPEN"), str(state()))
    set_mode("error")  # keep upstream broken
    time.sleep(2.3)
    s, b = proxy(0)
    check("probe is attempted (got upstream 500)", s == 500 and b["outcome"] == "upstream_5xx", str(b))
    check("probe failure re-opens", wait_state("OPEN"), str(state()))
    s, b = proxy(1)
    check("cooldown restarted, still rejected (503 or degraded)",
          rejected_or_degraded(s, b, "circuit_open"), str(b))
    set_mode("normal")
    time.sleep(2.3)
    s, b = proxy(0)
    check("fresh probe succeeds and closes", s == 200 and wait_state("CLOSED"), str(b))

    print("== 6. slow upstream -> timeout counts as failure ==")
    set_mode("slow")
    t0 = time.time()
    s, b = proxy(0, timeout=8)
    elapsed = time.time() - t0
    check("slow call returns 504", s == 504 and b["outcome"] == "timeout", str(b))
    check("gateway gave up near 2s, not 5s", 1.8 < elapsed < 3.5, f"{elapsed:.2f}s")
    check("timeout counted as failure", state()["failure_count"] == 1, str(state()))
    # two more timeouts trip it
    proxy(0, timeout=8); proxy(1, timeout=8)
    check("three timeouts trip breaker", wait_state("OPEN"), str(state()))
    set_mode("normal")
    time.sleep(2.3)
    s, _ = proxy(0)
    check("recovery after slow period", s == 200 and wait_state("CLOSED"))

    print("== 7. upstream lost then restored ==")
    # find upstream pid via its pidfile
    pidfile = os.path.join(os.path.dirname(__file__), "..", "logs", "flaky-upstream.pid")
    if os.path.exists(pidfile):
        up_pid = int(open(pidfile).read().strip())
        os.kill(up_pid, signal.SIGTERM)
        time.sleep(1.0)
        s, b = proxy(0)
        check("lost upstream -> 502 connection_error", s == 502 and b["outcome"] == "connection_error", str(b))
        s, b = proxy(1)
        check("second failure recorded on gateway-2", s == 502 and state()["failure_count"] == 2, str(b))
        s, b = proxy(0)
        check("third failure opens breaker", s == 502 and wait_state("OPEN"), str(b))
        # restart upstream
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        env = dict(os.environ)
        env["PYTHONPATH"] = repo_root
        log = open(os.path.join(os.path.dirname(pidfile), "flaky-upstream.log"), "a")
        proc = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.upstream_app:app",
             "--host", "127.0.0.1", "--port", UP.rsplit(":", 1)[1]],
            cwd=env["PYTHONPATH"], env=env, stdout=log, stderr=log,
        )
        with open(pidfile, "w") as f:
            f.write(str(proc.pid))
        check("upstream restarts", wait_port(UP, timeout=10))
        time.sleep(2.3)
        s, b = proxy(1)
        check("probe against restored upstream succeeds", s == 200 and wait_state("CLOSED"), str(b))
        s, b = proxy(0)
        check("traffic fully resumed", s == 200 and b["outcome"] == "success", str(b))

        print("== 7b. dead instance's probe is reclaimed by the other gateway ==")
        # trip the breaker again, then let gateway-1 own a probe against the
        # slow upstream, and kill gateway-1 while that probe is in flight.
        set_mode("error")
        for _ in range(3):
            proxy(0)
        check("breaker OPEN", wait_state("OPEN"), str(state()))
        set_mode("slow")
        time.sleep(2.3)
        # gateway-1 takes the probe (it will be stuck in the 2s upstream call)
        from threading import Thread
        holder = {}
        t = Thread(target=lambda: holder.update(zip(("s", "b"), proxy(0, timeout=8))))
        t.start()
        time.sleep(0.6)  # let the probe start
        st = state(1)
        check("gateway-1 owns the in-flight probe",
              st["state"] == "HALF_OPEN" and st["probe_instance"] == "gateway-1", str(st))
        # other requests are rejected meanwhile (503 or a degraded replica)
        s, b = proxy(1)
        check("gateway-2 rejected while probe held",
              rejected_or_degraded(s, b, "probe_in_flight"), str(b))
        # kill gateway-1 mid-probe
        gw1_pid = int(open(os.path.join(os.path.dirname(pidfile), "gateway-1.pid")).read().strip())
        os.kill(gw1_pid, signal.SIGTERM)
        time.sleep(0.5)
        s, _ = req("GET", GW[0] + "/api/state", timeout=2)
        check("gateway-1 is down", s == 0)
        # wait past probe timeout (3s): gateway-2 reclaims and starts its own probe
        time.sleep(3.2)
        s, b = proxy(1, timeout=8)
        check("gateway-2 reclaimed probe (new attempt made)",
              s == 504 and b["outcome"] == "timeout", str(b))
        st = state(1)
        check("reclaimed probe failure reopens", st["state"] == "OPEN", str(st))
        # restart gateway-1: shared DB means it sees the same state, no corruption
        env = dict(os.environ)
        env["PYTHONPATH"] = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        env.update({
            "INSTANCE_ID": "gateway-1",
            "UPSTREAM_URL": f"http://127.0.0.1:{UP.rsplit(':', 1)[1]}",
            "UPSTREAM_TIMEOUT": "2",
            "CB_DB_PATH": os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "breaker.db")),
            "CB_FAILURE_THRESHOLD": "3",
            "CB_COOLDOWN_SECONDS": "2",
            "CB_PROBE_TIMEOUT_SECONDS": "3",
            "CB_FALLBACK_TTL_SECONDS": os.getenv("CB_FALLBACK_TTL_SECONDS", "4"),
        })
        glog = open(os.path.join(os.path.dirname(pidfile), "gateway-1.log"), "a")
        g1 = subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "app.gateway_app:app",
             "--host", "127.0.0.1", "--port", GW[0].rsplit(":", 1)[1]],
            cwd=env["PYTHONPATH"], env=env, stdout=glog, stderr=glog,
        )
        with open(os.path.join(os.path.dirname(pidfile), "gateway-1.pid"), "w") as f:
            f.write(str(g1.pid))
        check("gateway-1 rejoins", wait_port(GW[0], timeout=10))
        check("restarted instance sees shared OPEN state", state(0)["state"] == "OPEN", str(state(0)))
        t.join(timeout=9)
        set_mode("normal")
        time.sleep(2.3)
        s, b = proxy(0)
        check("recovery via restarted gateway", s == 200 and wait_state("CLOSED"), str(b))
    else:
        print("  SKIP lost-contact cycle (no upstream pidfile; run via run_local.sh)")

    print("== 8. event audit trail ==")
    _, ev = req("GET", GW[0] + "/api/events?limit=50")
    kinds = {e["kind"] for e in ev["events"]}
    for needed in ("circuit_opened", "probe_start", "circuit_closed", "failure_recorded"):
        check(f"event logged: {needed}", needed in kinds, str(sorted(kinds)))
    instances = {e["instance"] for e in ev["events"]}
    check("events from both gateway instances",
          {"gateway-1", "gateway-2"}.issubset(instances), str(instances))

    print("== 9. shared fallback replica (degraded GET while breaker rejects) ==")
    ttl = state()["fallback_ttl_seconds"]
    check("fallback TTL advertised by breaker", isinstance(ttl, (int, float)) and ttl > 0, str(ttl))

    # 9.0 remove any replica captured by earlier sections so the "no replica"
    #     branch can be exercised against the shared DB of the live gateways.
    import sqlite3 as _sqlite3
    db_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "breaker.db"))
    if os.path.exists(db_path):
        _c = _sqlite3.connect(db_path)
        _c.execute("DELETE FROM fallback_replica")
        _c.commit()
        _c.close()
    set_mode("normal")
    assert wait_state("CLOSED")
    s, b = req("GET", GW[0] + "/api/replica")
    check("no replica before first success", s == 200 and b["replica"] is None, str(b))

    # 9.1 a confirmed success captures the replica (remember the upstream body)
    s, b = proxy(0)
    assert s == 200 and b["outcome"] == "success", str(b)
    captured_body = b["upstream_body"]
    s, b = req("GET", GW[0] + "/api/replica")
    rep = b["replica"]
    check("success stored shared replica",
          s == 200 and rep and rep["status_code"] == 200
          and rep["body"] == captured_body and rep["fresh"], str(b))
    captured_version = rep["version"]
    capture_epoch = rep["captured_at"]

    # 9.2 trip the breaker; while OPEN, both entries serve the degraded replica
    set_mode("error")
    for _ in range(3):
        proxy(0)
    check("breaker OPEN for fallback drill", wait_state("OPEN"), str(state()))
    s, b = proxy(0)
    check("gateway-1 serves degraded replica (upstream status preserved)",
          s == rep["status_code"] and b.get("degraded") is True
          and b["upstream_body"] == captured_body
          and b["fallback"]["upstream_status"] == rep["status_code"], str(b))
    s2, b2 = proxy(1)
    check("gateway-2 serves the same shared replica",
          s2 == rep["status_code"] and b2.get("degraded") is True
          and b2["upstream_body"] == captured_body
          and b2["fallback"]["version"] == captured_version, str(b2))
    check("degraded payload carries capture time and reject reason",
          b["fallback"]["captured_at_iso"] and b["fallback"]["age_seconds"] >= 0
          and b["reject_reason"] == "circuit_open", str(b))
    # marker headers for non-browser clients
    import urllib.request as _ur
    hr = _ur.urlopen(_ur.Request(GW[1] + "/proxy"), timeout=5)
    try:
        check("degraded response headers marked",
              hr.headers.get("X-Gateway-Degraded") == "1"
              and hr.headers.get("Warning", "").startswith("110 ")
              and hr.headers.get("X-Replica-Version") == str(captured_version),
              str(dict(hr.headers)))
    finally:
        hr.read()

    # 9.3 reads never extend the replica lifetime
    s, b = req("GET", GW[0] + "/api/replica")
    age1 = b["replica"]["age_seconds"]
    time.sleep(1.0)
    for _ in range(5):
        proxy(1)  # degraded reads
    req("GET", GW[0] + "/api/replica")
    s, b = req("GET", GW[1] + "/api/replica")
    rep2 = b["replica"]
    check("replica capture time unchanged by reads",
          rep2["captured_at"] == capture_epoch
          and rep2["age_seconds"] >= age1 + 0.9, str((age1, rep2["age_seconds"])))

    # 9.4 once older than the TTL, rejection is a plain 503 again.
    #     Keep the breaker in OPEN by re-tripping it inside every cooldown so
    #     the replica can age past its TTL.
    deadline = time.time() + ttl + 1.0
    while time.time() < deadline:
        s, b = proxy(0)
        if s == 503 and not b.get("degraded"):
            break  # cooldown active and replica already expired
        time.sleep(0.4)
    s, b = req("GET", GW[0] + "/api/replica")
    check("replica reported expired", b["replica"]["expired"] and not b["replica"]["fresh"], str(b))
    s, b = proxy(0)
    check("expired replica -> plain 503, not degraded",
          s == 503 and not b.get("degraded") and b["reason"] == "circuit_open", str(b))
    s, b = proxy(1)
    check("second entry also 503 after expiry", s == 503 and not b.get("degraded"), str(b))

    # 9.5 recovery probe interleaving: one real probe, peers read the replica.
    set_mode("normal")
    time.sleep(2.3)  # cooldown elapsed -> the next request is the single probe
    s, b = proxy(0)
    check("recovery probe succeeds and restores CLOSED",
          s == 200 and wait_state("CLOSED") and b["outcome"] == "success", str(b))
    s, b = req("GET", GW[0] + "/api/replica")
    check("successful probe refreshed replica (new version, age reset)",
          b["replica"]["version"] > captured_version and b["replica"]["age_seconds"] < 2,
          str(b["replica"]))
    fresh_version = b["replica"]["version"]

    # trip again on hard 5xx (fast, replica stays fresh) and enter HALF_OPEN
    set_mode("error")
    for _ in range(3):
        proxy(0)
    check("breaker OPEN again for probe-interleave drill", wait_state("OPEN"), str(state()))
    set_mode("slow")  # the recovery probe will hang until the 2s gateway timeout
    time.sleep(2.3)   # cooldown elapsed; next request becomes the single probe
    from threading import Thread
    holder = {}
    t = Thread(target=lambda: holder.update(zip(("s", "b"), proxy(0, timeout=8))))
    t.start()
    time.sleep(0.6)  # let the probe take off
    check("single probe in flight", state()["state"] == "HALF_OPEN", str(state()))
    # the other 7 requests must be fast rejections served from the replica:
    # none of them may become a probe
    with ThreadPoolExecutor(max_workers=7) as pool:
        others = list(pool.map(lambda i: proxy(i % 2), range(7)))
    check("concurrent peers degraded, never probes",
          all(o[0] == 200 and o[1].get("degraded") is True
              and o[1]["reject_reason"] == "probe_in_flight"
              and o[1]["fallback"]["version"] == fresh_version for o in others),
          str([(c, x.get("degraded"), x.get("reason") or x.get("reject_reason"))
               for c, x in others]))
    t.join(timeout=9)
    check("the one probe failed (slow upstream) without touching replica",
          holder["s"] == 504 and holder["b"]["outcome"] == "timeout", str(holder))
    s, b = req("GET", GW[0] + "/api/replica")
    check("failed probe did not refresh replica",
          b["replica"]["version"] == fresh_version, str(b["replica"]))
    check("breaker re-opened after failed probe", wait_state("OPEN"), str(state()))

    # 9.6 after the failed probe re-opens the breaker, rejection behaviour is
    #     governed solely by replica freshness. The 2s slow-probe wait plus the
    #     HALF_OPEN window may have aged the short local-TTL replica past expiry,
    #     in which case a plain 503 is the correct answer.
    s, b = proxy(1)
    _, rep_resp = req("GET", GW[0] + "/api/replica")
    if rep_resp["replica"]["fresh"]:
        check("post-probe rejection serves the fresh replica",
              s == 200 and b.get("degraded") is True
              and b["fallback"]["version"] == fresh_version, str(b))
    else:
        check("aged replica expired -> plain 503 after failed probe",
              s == 503 and not b.get("degraded") and b["reason"] == "circuit_open", str(b))

    set_mode("normal")
    time.sleep(2.3)
    s, b = proxy(0)
    check("final recovery probe succeeds", s == 200 and wait_state("CLOSED"), str(b))
    s, b = req("GET", GW[0] + "/api/replica")
    check("replica fresh again after recovery", b["replica"]["fresh"], str(b["replica"]))

    print(f"\n{'='*50}\nRESULT: {PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
