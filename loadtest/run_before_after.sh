#!/usr/bin/env bash
# Before/after step test for the query service: old code vs uvicorn[standard] + orjson Response.
# Only the code changes between the two runs; the system runs in its normal configuration
# (1 uvicorn worker, no CPU caps). Same warm-up, rates, seed and recorders for both.
#
# Usage (repo root):  bash loadtest/run_before_after.sh
set -u
cd "$(dirname "$0")/.."
PY=.venv/bin/python
RATES="300 400 500 600 700 800 1000 1200 1500 2000"
BASE=loadtest/results/before-after-$(date +%Y%m%d-%H%M%S)
mkdir -p "$BASE"

run_one() {   # $1 = label
  local R="$BASE/$1"; mkdir -p "$R"
  echo "=== $1: $(date +%H:%M:%S)"
  docker exec api-bsky-api-1 sh -c 'grep -oE "(uvloop|httptools)[^ ]*\.so" /proc/1/maps | sort -u' > "$R/c-extensions-loaded.txt" 2>&1
  docker logs api-bsky-api-1 2>&1 | grep -c "Started server process" > "$R/worker-processes.txt"
  $PY loadtest/monitor.py --out "$R" > "$R/monitor.log" 2>&1 & local MP=$!
  sleep 10
  # warm-up: fill the cache for all windows, not measured
  sed -n '1,6000p' "$BASE/targets-warmup.txt" | vegeta attack -rate=100/s -duration=60s -timeout=5s -max-body=0 | vegeta report > "$R/warmup.txt"
  sleep 10
  $PY loadtest/step_test.py --rates $RATES --out "$R" 2>&1 | tee "$R/step_test.log"
  sleep 20
  kill -TERM $MP; wait $MP
}

# warm-up targets (random queries, seeded)
$PY - <<EOF
import random
r = random.Random(7)
with open("$BASE/targets-warmup.txt", "w") as f:
    for _ in range(6000):
        f.write(f"GET http://localhost:8000/top-liked?minutes={r.randint(1,60)}&limit={r.randint(1,20)}\n")
EOF

echo "--- restore normal setup on the OLD code (no rebuild): 1 worker, no CPU caps"
docker compose -f bluesky/api/docker-compose.yml up -d 2>&1 | tail -1
docker update --cpus 6 redis >/dev/null   # "--cpus 0" does not remove a cap; 6 = the whole VM
sleep 8
run_one before

echo "--- deploy the NEW code"
docker compose -f bluesky/api/docker-compose.yml up -d --build 2>&1 | tail -1
sleep 8
run_one after

echo "=== done $(date +%H:%M:%S) -> $BASE"
