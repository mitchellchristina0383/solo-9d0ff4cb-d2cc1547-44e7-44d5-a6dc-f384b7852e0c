#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
for f in logs/*.pid; do
  [ -e "$f" ] || continue
  pid=$(cat "$f")
  kill "$pid" 2>/dev/null || true
  rm -f "$f"
done
# uvicorn spawns no child processes in this setup, but clean up stragglers.
pkill -f "uvicorn app.gateway_app" 2>/dev/null || true
pkill -f "uvicorn app.upstream_app" 2>/dev/null || true
echo "stopped"
