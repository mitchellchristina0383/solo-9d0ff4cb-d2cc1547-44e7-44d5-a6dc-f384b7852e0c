"""Circuit breaker core: shared state in SQLite, safe across two gateway processes.

State machine
-------------
CLOSED     : normal forwarding; consecutive failures are counted.
             The `epoch` increments every time the breaker leaves CLOSED.
OPEN       : all requests are fast-rejected until the cooldown elapses.
HALF_OPEN  : exactly one probe request is admitted; every other request is
             fast-rejected. A stuck probe can be reclaimed after PROBE_TIMEOUT.

Stale-result protection
------------------------
Every dispatch decision carries a token.  A result is applied to the breaker
state only when the token still matches:
  * normal request token  -> (epoch, state == CLOSED)
  * probe token           -> (probe_token, state == HALF_OPEN)
Late results of superseded requests (or of a probe whose instance died and was
reclaimed by the other gateway) therefore can never overwrite a newer verdict.
"""

from __future__ import annotations

import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from typing import Any

# --- tunables (overridable via environment, seconds) -------------------------
FAILURE_THRESHOLD = int(os.getenv("CB_FAILURE_THRESHOLD", "3"))
COOLDOWN_SECONDS = float(os.getenv("CB_COOLDOWN_SECONDS", "2"))
# A probe that neither succeeds nor fails within this time may be reclaimed so
# another instance (or the same one) can start a fresh recovery probe.
PROBE_TIMEOUT_SECONDS = float(os.getenv("CB_PROBE_TIMEOUT_SECONDS", "3"))
EVENT_LIMIT = int(os.getenv("CB_EVENT_LIMIT", "100"))

CLOSED, OPEN, HALF_OPEN = "CLOSED", "OPEN", "HALF_OPEN"

SCHEMA = """
CREATE TABLE IF NOT EXISTS breaker_state (
    id               INTEGER PRIMARY KEY CHECK (id = 1),
    state            TEXT NOT NULL,
    failure_count    INTEGER NOT NULL,
    epoch            INTEGER NOT NULL,
    opened_at        REAL,
    probe_token      TEXT,
    probe_instance   TEXT,
    probe_started_at REAL
);
CREATE TABLE IF NOT EXISTS events (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    ts         REAL NOT NULL,
    instance   TEXT NOT NULL,
    kind       TEXT NOT NULL,
    detail     TEXT NOT NULL DEFAULT ''
);
"""


class Breaker:
    def __init__(self, db_path: str, instance_id: str, *, monotonic=time.monotonic):
        self.db_path = db_path
        self.instance_id = instance_id
        self._monotonic = monotonic
        self._init_db()

    # ------------------------------------------------------------------ infra
    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout={10_000}")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(SCHEMA)
            row = conn.execute("SELECT id FROM breaker_state WHERE id = 1").fetchone()
            if row is None:
                conn.execute(
                    "INSERT INTO breaker_state (id, state, failure_count, epoch) "
                    "VALUES (1, ?, 0, 0)",
                    (CLOSED,),
                )

    @contextmanager
    def _txn(self, conn: sqlite3.Connection):
        # Exclusive: all state transitions are serialized across both gateways.
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            conn.execute("ROLLBACK")
            raise

    def _log(self, conn: sqlite3.Connection, kind: str, detail: str = "") -> None:
        conn.execute(
            "INSERT INTO events (ts, instance, kind, detail) VALUES (?, ?, ?, ?)",
            (self._monotonic(), self.instance_id, kind, detail),
        )
        conn.execute(
            "DELETE FROM events WHERE id NOT IN "
            "(SELECT id FROM events ORDER BY id DESC LIMIT ?)",
            (EVENT_LIMIT,),
        )

    @staticmethod
    def _row_to_state(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "state": row["state"],
            "failure_count": row["failure_count"],
            "epoch": row["epoch"],
            "opened_at": row["opened_at"],
            "probe_token": row["probe_token"],
            "probe_instance": row["probe_instance"],
            "probe_started_at": row["probe_started_at"],
        }

    # ------------------------------------------------------------- public API
    def dispatch(self) -> dict[str, Any]:
        """Decide what to do with an incoming request.

        Returns a dict with action:
          {"action": "forward", "kind": "normal", "token": {...}}
          {"action": "probe",   "kind": "probe",  "token": {...}}
          {"action": "reject",  "reason": "..."}
        """
        now = self._monotonic()
        with self._connect() as conn:
            with self._txn(conn):
                row = conn.execute(
                    "SELECT * FROM breaker_state WHERE id = 1"
                ).fetchone()
                state = row["state"]

                if state == CLOSED:
                    return {
                        "action": "forward",
                        "kind": "normal",
                        "token": {"epoch": row["epoch"], "state": CLOSED},
                    }

                if state == OPEN:
                    if now - row["opened_at"] < COOLDOWN_SECONDS:
                        remain = COOLDOWN_SECONDS - (now - row["opened_at"])
                        return {
                            "action": "reject",
                            "kind": "reject",
                            "reason": "circuit_open",
                            "retry_after": round(remain, 3),
                        }
                    # Cooldown elapsed -> admit the single recovery probe.
                    token = uuid.uuid4().hex
                    conn.execute(
                        "UPDATE breaker_state SET state=?, probe_token=?, "
                        "probe_instance=?, probe_started_at=? WHERE id=1",
                        (HALF_OPEN, token, self.instance_id, now),
                    )
                    self._log(conn, "probe_start", f"cooldown elapsed")
                    return {
                        "action": "probe",
                        "kind": "probe",
                        "token": {"probe_token": token, "state": HALF_OPEN},
                    }

                # HALF_OPEN: only the outstanding probe may proceed.
                if (
                    row["probe_started_at"] is not None
                    and now - row["probe_started_at"] > PROBE_TIMEOUT_SECONDS
                ):
                    old = row["probe_instance"]
                    token = uuid.uuid4().hex
                    conn.execute(
                        "UPDATE breaker_state SET probe_token=?, probe_instance=?, "
                        "probe_started_at=? WHERE id=1",
                        (token, self.instance_id, now),
                    )
                    self._log(
                        conn,
                        "probe_reclaimed",
                        f"previous probe by {old} exceeded "
                        f"{PROBE_TIMEOUT_SECONDS}s",
                    )
                    return {
                        "action": "probe",
                        "kind": "probe",
                        "token": {"probe_token": token, "state": HALF_OPEN},
                    }
                return {
                    "action": "reject",
                    "kind": "reject",
                    "reason": "probe_in_flight",
                    "probe_instance": row["probe_instance"],
                }

    def record_success(self, decision: dict[str, Any]) -> dict[str, Any] | None:
        """Apply a successful response. Returns new state dict, or None if stale."""
        now = self._monotonic()
        with self._connect() as conn:
            with self._txn(conn):
                row = conn.execute(
                    "SELECT * FROM breaker_state WHERE id = 1"
                ).fetchone()
                kind = decision["kind"]

                if kind == "probe":
                    if (
                        row["state"] != HALF_OPEN
                        or row["probe_token"] != decision["token"]["probe_token"]
                    ):
                        self._log(conn, "stale_result_ignored", "late probe success")
                        return None
                    conn.execute(
                        "UPDATE breaker_state SET state=?, failure_count=0, "
                        "epoch=epoch+1, probe_token=NULL, probe_instance=NULL, "
                        "probe_started_at=NULL, opened_at=NULL WHERE id=1",
                        (CLOSED,),
                    )
                    self._log(conn, "circuit_closed", "probe succeeded")
                    return self._state_locked(conn, now)

                # normal request
                if (
                    row["state"] != CLOSED
                    or row["epoch"] != decision["token"]["epoch"]
                ):
                    self._log(conn, "stale_result_ignored", "late normal success")
                    return None
                if row["failure_count"] != 0:
                    conn.execute(
                        "UPDATE breaker_state SET failure_count=0 WHERE id=1"
                    )
                    self._log(conn, "failure_count_reset", "success in CLOSED state")
                return self._state_locked(conn, now)

    def record_failure(self, decision: dict[str, Any], detail: str) -> dict[str, Any] | None:
        """Apply a failed response / timeout. Returns new state, or None if stale."""
        now = self._monotonic()
        with self._connect() as conn:
            with self._txn(conn):
                row = conn.execute(
                    "SELECT * FROM breaker_state WHERE id = 1"
                ).fetchone()
                kind = decision["kind"]

                if kind == "probe":
                    if (
                        row["state"] != HALF_OPEN
                        or row["probe_token"] != decision["token"]["probe_token"]
                    ):
                        self._log(conn, "stale_result_ignored", f"late probe failure: {detail}")
                        return None
                    conn.execute(
                        "UPDATE breaker_state SET state=?, opened_at=?, "
                        "epoch=epoch+1, probe_token=NULL, probe_instance=NULL, "
                        "probe_started_at=? WHERE id=1",
                        (OPEN, now, None),
                    )
                    self._log(conn, "probe_failed", f"re-enter cooldown: {detail}")
                    return self._state_locked(conn, now)

                # normal request
                if (
                    row["state"] != CLOSED
                    or row["epoch"] != decision["token"]["epoch"]
                ):
                    self._log(conn, "stale_result_ignored", f"late normal failure: {detail}")
                    return None
                new_count = row["failure_count"] + 1
                if new_count >= FAILURE_THRESHOLD:
                    conn.execute(
                        "UPDATE breaker_state SET state=?, failure_count=?, "
                        "epoch=epoch+1, opened_at=? WHERE id=1",
                        (OPEN, new_count, now),
                    )
                    self._log(
                        conn,
                        "circuit_opened",
                        f"{new_count} consecutive failures: {detail}",
                    )
                else:
                    conn.execute(
                        "UPDATE breaker_state SET failure_count=? WHERE id=1",
                        (new_count,),
                    )
                    self._log(conn, "failure_recorded", f"{new_count}/{FAILURE_THRESHOLD}: {detail}")
                return self._state_locked(conn, now)

    def _state_locked(self, conn: sqlite3.Connection, now: float) -> dict[str, Any]:
        row = conn.execute("SELECT * FROM breaker_state WHERE id = 1").fetchone()
        out = self._row_to_state(row)
        out["now"] = now
        out["cooldown_seconds"] = COOLDOWN_SECONDS
        out["probe_timeout_seconds"] = PROBE_TIMEOUT_SECONDS
        out["failure_threshold"] = FAILURE_THRESHOLD
        if out["state"] == OPEN and out["opened_at"] is not None:
            elapsed = now - out["opened_at"]
            out["cooldown_remaining"] = max(0.0, COOLDOWN_SECONDS - elapsed)
        else:
            out["cooldown_remaining"] = 0.0
        return out

    def get_state(self) -> dict[str, Any]:
        now = self._monotonic()
        with self._connect() as conn:
            return self._state_locked(conn, now)

    def get_events(self, limit: int = 30) -> list[dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT id, ts, instance, kind, detail FROM events "
                "ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
            now = self._monotonic()
            return [
                {
                    "id": r["id"],
                    "age": round(now - r["ts"], 3),
                    "instance": r["instance"],
                    "kind": r["kind"],
                    "detail": r["detail"],
                }
                for r in rows
            ]
