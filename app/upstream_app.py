"""Local upstream with switchable behaviour: normal / slow / error.

Modes (switched at runtime via POST /mode):
  normal : 200 JSON immediately
  slow   : waits UPSTREAM_SLOW_DELAY seconds (the gateway times out first)
  error  : HTTP 500

A fully unreachable upstream ("lost contact") is simulated by stopping this
process / container; the gateway then sees connection errors.
"""

from __future__ import annotations

import asyncio
import os
import time

from fastapi import FastAPI
from fastapi.responses import JSONResponse

SLOW_DELAY = float(os.getenv("UPSTREAM_SLOW_DELAY", "5"))
VALID_MODES = ("normal", "slow", "error")

app = FastAPI(title="flaky-upstream", version="1.0")
_state: dict[str, str] = {"mode": "normal"}
_counter = {"calls": 0}


@app.get("/health")
async def health():
    return {"status": "ok", "mode": _state["mode"]}


@app.post("/mode")
async def set_mode(payload: dict):
    mode = payload.get("mode")
    if mode not in VALID_MODES:
        return JSONResponse(
            status_code=400,
            content={"error": f"mode must be one of {VALID_MODES}"},
        )
    old = _state["mode"]
    _state["mode"] = mode
    return {"ok": True, "previous": old, "mode": mode}


@app.get("/mode")
async def get_mode():
    return {"mode": _state["mode"], "calls": _counter["calls"]}


@app.get("/")
async def root():
    _counter["calls"] += 1
    n = _counter["calls"]
    mode = _state["mode"]
    if mode == "error":
        return JSONResponse(
            status_code=500,
            content={"upstream": "error", "call": n, "ts": time.time()},
        )
    if mode == "slow":
        await asyncio.sleep(SLOW_DELAY)
    return {
        "upstream": "ok",
        "mode": mode,
        "call": n,
        "host": os.getenv("HOSTNAME", os.uname().nodename),
        "ts": time.time(),
    }
