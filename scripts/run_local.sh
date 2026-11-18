#!/usr/bin/env bash
# Local equivalent of docker-compose for environments without a container runtime.
# Starts one upstream + two gateways sharing one SQLite file under ./data.
set -euo pipefail

cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD"
export PYTHONUSERBASE="${PYTHONUSERBASE:-$PWD/.pylibs}"
mkdir -p data logs

UPSTREAM_PORT=18000
GW1_PORT=18001
GW2_PORT=18002
export UPSTREAM_URL="http://127.0.0.1:${UPSTREAM_PORT}"
export UPSTREAM_TIMEOUT=2
export CB_DB_PATH="$PWD/data/breaker.db"
export CB_FAILURE_THRESHOLD=3
export CB_COOLDOWN_SECONDS=2
export CB_PROBE_TIMEOUT_SECONDS=3
# Local drills use a short replica TTL so expiry can be observed quickly;
# docker-compose keeps the production default of 30 seconds.
export CB_FALLBACK_TTL_SECONDS="${CB_FALLBACK_TTL_SECONDS:-4}"

start() {
  local name="$1" app="$2" port="$3" extra="$4"
  INSTANCE_ID="$name" $extra python3 -m uvicorn "$app" \
    --host 127.0.0.1 --port "$port" >"logs/${name}.log" 2>&1 &
  echo $! > "logs/${name}.pid"
}

start flaky-upstream app.upstream_app:app "$UPSTREAM_PORT" ""
start gateway-1 app.gateway_app:app "$GW1_PORT" ""
start gateway-2 app.gateway_app:app "$GW2_PORT" ""

echo "upstream : http://127.0.0.1:${UPSTREAM_PORT}"
echo "gateway-1: http://127.0.0.1:${GW1_PORT}  (browser UI)"
echo "gateway-2: http://127.0.0.1:${GW2_PORT}  (browser UI)"
echo "stop with: scripts/stop_local.sh"
