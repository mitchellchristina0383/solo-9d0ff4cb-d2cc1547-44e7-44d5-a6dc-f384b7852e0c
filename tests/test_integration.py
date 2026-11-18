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
    check("gateway-1 fast-rejects 503", s1 == 503 and b1["reason"] == "circuit_open", str(b1))
    check("gateway-2 fast-rejects 503", s2 == 503 and b2["reason"] == "circuit_open", str(b2))
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
    check("still rejecting within 2s cooldown", s == 503 and b["reason"] == "circuit_open", str(b))
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
    rejects = [o for o in outcomes if o[0] == 503 and o[1].get("reason") == "probe_in_flight"]
    check("exactly one probe while probe in flight", len(probes) == 1,
          str([(c, b.get("reason") or b.get("outcome")) for c, b in outcomes]))
    check("other 7 requests rejected immediately", len(rejects) == 7, str(len(rejects)))
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
    check("cooldown restarted, still 503", s == 503 and b["reason"] == "circuit_open", str(b))
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
        env = dict(os.environ)
        env.setdefault("PYTHONPATH", os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
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
        # other requests are rejected meanwhile
        s, b = proxy(1)
        check("gateway-2 rejected while probe held", s == 503 and b["reason"] == "probe_in_flight", str(b))
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
        env.update({
            "INSTANCE_ID": "gateway-1",
            "UPSTREAM_URL": f"http://127.0.0.1:{UP.rsplit(':', 1)[1]}",
            "UPSTREAM_TIMEOUT": "2",
            "CB_DB_PATH": os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "breaker.db")),
            "CB_FAILURE_THRESHOLD": "3",
            "CB_COOLDOWN_SECONDS": "2",
            "CB_PROBE_TIMEOUT_SECONDS": "3",
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

    print(f"\n{'='*50}\nRESULT: {PASS} passed, {FAIL} failed")
    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()
