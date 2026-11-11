#!/bin/bash
# B.4 reaper test: a job that always crashes its worker must reach terminal 'failed'
# Run from project root: bash scripts/b4-reaper-test.sh
set -uo pipefail

DB_URL="postgresql://soup:soup@localhost:5432/soup"
PSQL="docker compose exec -T db psql -U soup -d soup -t -A"
LOG=/tmp/b4-logs
mkdir -p $LOG

echo "=== B.4 Reaper Test ==="

echo "[1/6] Stopping workers + wiping DB..."
pkill -f "src.worker" 2>/dev/null || true
docker compose stop worker >/dev/null 2>&1 || true
sleep 0.5
docker compose exec -T db psql -U soup -d soup -c "TRUNCATE side_effects, jobs;" >/dev/null

echo "[2/6] Creating 1 normal + 1 poison job..."
STAMP=$(date +%s)
NORMAL_KEY="b4-normal-$STAMP"
POISON_KEY="b4-poison-$STAMP"
curl -sS -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d "{\"idempotency_key\":\"$NORMAL_KEY\",\"payload\":{\"type\":\"normal\"}}" >/dev/null
curl -sS -X POST localhost:8000/jobs -H 'content-type: application/json' \
  -d "{\"idempotency_key\":\"$POISON_KEY\",\"payload\":{\"poison\":true}}" >/dev/null
echo "  normal_key=$NORMAL_KEY"
echo "  poison_key=$POISON_KEY"

echo "[3/6] Starting 2 supervisor workers with POISON_TEST=1..."
sup_worker() {
  local name=$1
  while true; do
    DATABASE_URL=$DB_URL WORKER_ID=$name POISON_TEST=1 \
      python -m src.worker >> "$LOG/$name.log" 2>&1 || true
    sleep 0.3
  done
}
sup_worker w1 & P1=$!
sup_worker w2 & P2=$!

echo "[4/6] Waiting 70s for 5 attempts + lease expiry + reaper..."
sleep 70

echo "[5/6] Stopping supervisors..."
kill $P1 $P2 2>/dev/null || true
pkill -f "src.worker" 2>/dev/null || true
sleep 0.5

echo "[6/6] Checking invariants..."
NORMAL=$($PSQL -c "SELECT state, attempt FROM jobs WHERE idempotency_key='$NORMAL_KEY';")
POISON=$($PSQL -c "SELECT state, attempt, COALESCE(last_error,'(null)') FROM jobs WHERE idempotency_key='$POISON_KEY';")
EFFECTS=$($PSQL -c "SELECT count(*) FROM side_effects;")
POISON_EFF=$($PSQL -c "SELECT count(*) FROM side_effects s JOIN jobs j ON s.job_id=j.id WHERE j.idempotency_key='$POISON_KEY';")
CLAIMS=$($PSQL -c "SELECT count(*) FROM jobs WHERE idempotency_key='$POISON_KEY' AND attempt=5;")

echo ""
echo "  normal job:    $NORMAL"
echo "  poison job:    $POISON"
echo "  total effects: $EFFECTS"
echo "  poison effects:$POISON_EFF"
echo "  logs in:       $LOG/"
echo ""

PASS=true
if [[ "$NORMAL" != succeeded* ]]; then echo "  FAIL: normal job not succeeded"; PASS=false; fi
if [[ "$POISON" != failed*"max attempts exceeded"* ]]; then echo "  FAIL: poison not 'failed | max attempts exceeded'"; PASS=false; fi
if [[ "$POISON" != *"|5|"* ]]; then echo "  FAIL: poison attempt != 5 (got '$POISON')"; PASS=false; fi
if [[ "$EFFECTS" != "1" ]]; then echo "  FAIL: expected 1 side effect, got $EFFECTS"; PASS=false; fi
if [[ "$POISON_EFF" != "0" ]]; then echo "  FAIL: poison has side effects (should be 0)"; PASS=false; fi

# Also grep the logs — the crash evidence must be visible
CRASHES=$(grep -hc "simulated crash (POISON_TEST)" $LOG/*.log 2>/dev/null | awk '{s+=$1} END {print s+0}')
REAPED=$(grep -hc "reaped job=" $LOG/*.log 2>/dev/null | awk '{s+=$1} END {print s+0}')
echo "  observed crashes in logs: $CRASHES"
echo "  observed reaps in logs:   $REAPED"
if [[ "$CRASHES" -lt 4 ]]; then echo "  WARN: fewer than 4 crashes logged (expected 5)" ; fi
if [[ "$REAPED" -lt 1 ]]; then echo "  FAIL: reaper never logged"; PASS=false; fi

if $PASS; then echo "=== PASS ==="; exit 0; else echo "=== FAIL ==="; exit 1; fi