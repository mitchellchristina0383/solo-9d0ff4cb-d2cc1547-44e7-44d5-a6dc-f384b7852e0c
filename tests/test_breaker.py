"""Unit tests for the SQLite-backed breaker core.

Run:  python -I -m pytest tests/test_breaker.py -q
(also runnable without pytest via: python -I tests/test_breaker.py)
"""

import os
import sqlite3
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.breaker import (  # noqa: E402
    Breaker,
    CLOSED,
    OPEN,
    HALF_OPEN,
    FAILURE_THRESHOLD,
    COOLDOWN_SECONDS,
    PROBE_TIMEOUT_SECONDS,
)


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def make_breaker(tmpdir, name="gw-a", clock=None):
    return Breaker(os.path.join(tmpdir, "breaker.db"), name, monotonic=clock or FakeClock())


def test_opens_after_three_consecutive_failures(tmp_path):
    b = make_breaker(str(tmp_path))
    decisions = []
    for i in range(FAILURE_THRESHOLD - 1):
        d = b.dispatch()
        decisions.append(d)
        st = b.record_failure(d, "http_500")
        assert st["state"] == CLOSED
        assert st["failure_count"] == i + 1
    d = b.dispatch()
    st = b.record_failure(d, "http_500")
    assert st["state"] == OPEN
    assert st["failure_count"] == FAILURE_THRESHOLD

    # once open, everything is rejected immediately
    d = b.dispatch()
    assert d["action"] == "reject" and d["reason"] == "circuit_open"


def test_success_resets_failure_count(tmp_path):
    b = make_breaker(str(tmp_path))
    d1 = b.dispatch(); b.record_failure(d1, "x")
    d2 = b.dispatch(); b.record_failure(d2, "x")
    assert b.get_state()["failure_count"] == 2
    d3 = b.dispatch()
    st = b.record_success(d3)
    assert st["state"] == CLOSED and st["failure_count"] == 0
    # two more failures must not trip it (count restarted)
    d4 = b.dispatch(); b.record_failure(d4, "x")
    d5 = b.dispatch()
    assert b.record_failure(d5, "x")["state"] == CLOSED


def test_cooldown_then_single_probe_others_rejected(tmp_path):
    clock = FakeClock()
    b = make_breaker(str(tmp_path), clock=clock)
    for _ in range(FAILURE_THRESHOLD):
        d = b.dispatch(); b.record_failure(d, "timeout")
    assert b.get_state()["state"] == OPEN

    # still inside cooldown
    clock.advance(COOLDOWN_SECONDS - 0.5)
    assert b.dispatch()["action"] == "reject"

    # cooldown elapsed: exactly one probe admitted
    clock.advance(0.6)
    probe = b.dispatch()
    assert probe["action"] == "probe"
    other = b.dispatch()
    assert other["action"] == "reject" and other["reason"] == "probe_in_flight"
    assert b.get_state()["state"] == HALF_OPEN


def test_probe_success_closes_and_forwards_resume(tmp_path):
    clock = FakeClock()
    b = make_breaker(str(tmp_path), clock=clock)
    for _ in range(FAILURE_THRESHOLD):
        d = b.dispatch(); b.record_failure(d, "x")
    clock.advance(COOLDOWN_SECONDS + 0.1)
    probe = b.dispatch()
    st = b.record_success(probe)
    assert st["state"] == CLOSED and st["failure_count"] == 0
    nxt = b.dispatch()
    assert nxt["action"] == "forward" and nxt["kind"] == "normal"


def test_probe_failure_reenters_cooldown(tmp_path):
    clock = FakeClock()
    b = make_breaker(str(tmp_path), clock=clock)
    for _ in range(FAILURE_THRESHOLD):
        d = b.dispatch(); b.record_failure(d, "x")
    clock.advance(COOLDOWN_SECONDS + 0.1)
    probe = b.dispatch()
    st = b.record_failure(probe, "http_500")
    assert st["state"] == OPEN
    # cooldown restarts: immediate reject
    assert b.dispatch()["reason"] == "circuit_open"
    clock.advance(COOLDOWN_SECONDS + 0.1)
    assert b.dispatch()["action"] == "probe"


def test_stuck_probe_is_reclaimable_after_probe_timeout(tmp_path):
    clock = FakeClock()
    b1 = make_breaker(str(tmp_path), "gw-1", clock)
    for _ in range(FAILURE_THRESHOLD):
        d = b1.dispatch(); b1.record_failure(d, "x")
    clock.advance(COOLDOWN_SECONDS + 0.1)
    dead_probe = b1.dispatch()
    assert dead_probe["action"] == "probe"

    # instance gw-1 died holding the probe; gw-2 shares the same DB
    b2 = Breaker(os.path.join(str(tmp_path), "breaker.db"), "gw-2", monotonic=clock)
    clock.advance(PROBE_TIMEOUT_SECONDS + 0.1)
    new_probe = b2.dispatch()
    assert new_probe["action"] == "probe"
    assert new_probe["token"]["probe_token"] != dead_probe["token"]["probe_token"]

    # old probe's late result must not change the new conclusion
    assert b1.record_success(dead_probe) is None
    assert b1.record_failure(dead_probe, "late") is None
    assert b2.get_state()["state"] == HALF_OPEN

    # new probe succeeds -> closed
    assert b2.record_success(new_probe)["state"] == CLOSED


def test_late_normal_result_does_not_overwrite_newer_state(tmp_path):
    b = make_breaker(str(tmp_path))
    old = b.dispatch()                       # normal request in CLOSED, epoch 0
    for _ in range(FAILURE_THRESHOLD):
        d = b.dispatch(); b.record_failure(d, "x")
    assert b.get_state()["state"] == OPEN
    # the old in-flight request now comes back (both ways): must be ignored
    assert b.record_failure(old, "late failure") is None
    assert b.record_success(old) is None
    assert b.get_state()["state"] == OPEN


def test_late_probe_result_after_success_ignored(tmp_path):
    clock = FakeClock()
    b = make_breaker(str(tmp_path), clock=clock)
    for _ in range(FAILURE_THRESHOLD):
        d = b.dispatch(); b.record_failure(d, "x")
    clock.advance(COOLDOWN_SECONDS + 0.1)
    p1 = b.dispatch()
    assert b.record_success(p1)["state"] == CLOSED
    # p1 duplicated / replayed late
    assert b.record_success(p1) is None
    assert b.record_failure(p1, "late") is None


def test_two_breaker_instances_share_state(tmp_path):
    clock = FakeClock()
    b1 = make_breaker(str(tmp_path), "gw-1", clock)
    b2 = Breaker(os.path.join(str(tmp_path), "breaker.db"), "gw-2", monotonic=clock)
    d = b1.dispatch(); b1.record_failure(d, "x")
    assert b2.get_state()["failure_count"] == 1
    d = b2.dispatch(); b2.record_failure(d, "x")
    assert b1.get_state()["failure_count"] == 2


def test_concurrent_dispatch_only_one_probe_wins(tmp_path):
    clock = FakeClock()
    b1 = make_breaker(str(tmp_path), "gw-1", clock)
    for _ in range(FAILURE_THRESHOLD):
        d = b1.dispatch(); b1.record_failure(d, "x")
    clock.advance(COOLDOWN_SECONDS + 0.1)

    b2 = Breaker(os.path.join(str(tmp_path), "breaker.db"), "gw-2", monotonic=clock)
    results = []
    barrier = threading.Barrier(8)

    def worker(src):
        barrier.wait()
        results.append(src.dispatch())

    threads = [threading.Thread(target=worker, args=(b1 if i % 2 else b2,))
               for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    probes = [r for r in results if r["action"] == "probe"]
    rejects = [r for r in results if r["action"] == "reject"]
    assert len(probes) == 1
    assert len(rejects) == 7
    assert all(r["reason"] == "probe_in_flight" for r in rejects)


def test_concurrent_failures_under_lock_count_exactly(tmp_path):
    b1 = make_breaker(str(tmp_path), "gw-1")
    b2 = Breaker(os.path.join(str(tmp_path), "breaker.db"), "gw-2")
    n = 20
    decisions = []
    lock = threading.Lock()

    def worker(src):
        d = src.dispatch()
        src.record_failure(d, "concurrent")
        with lock:
            decisions.append(d)

    threads = [threading.Thread(target=worker, args=(b1 if i % 2 else b2,))
               for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    st = b1.get_state()
    assert st["state"] == OPEN
    # all 20 failures recorded sequentially; the trip happened at threshold
    assert st["failure_count"] == FAILURE_THRESHOLD


def test_db_is_sqlite_wal_and_persists(tmp_path):
    db = os.path.join(str(tmp_path), "breaker.db")
    b = Breaker(db, "gw-x")
    d = b.dispatch(); b.record_failure(d, "x")
    conn = sqlite3.connect(db)
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0].lower()
    conn.close()
    assert mode == "wal"
    # reopen: state survives
    b2 = Breaker(db, "gw-y")
    assert b2.get_state()["failure_count"] == 1


def _run_all():
    import inspect
    fns = [v for k, v in sorted(globals().items())
           if k.startswith("test_") and inspect.isfunction(v)]
    class Tmp:
        def __init__(self): self.p = tempfile.mkdtemp()
        def __fspath__(self): return self.p
        def __str__(self): return self.p

    passed = 0
    for fn in fns:
        params = inspect.signature(fn).parameters
        fn(Tmp()) if params else fn()
        print(f"PASS {fn.__name__}")
        passed += 1
    print(f"\n{passed} tests passed")


if __name__ == "__main__":
    _run_all()
