#!/bin/bash
# Pre-chaos integration: 20 jobs, 2 workers, all four invariants.
# Run from project root: bash scripts/pre-chaos-integration.sh
set -uo pipefail

DB_URL="postgresql://soup:soup@localhost:5432/soup"
PSQL="docker compose exec -T db psql -U soup -d soup -t -A"
LOG=/tmp/int-logs
mkdir -p $LOG

echo "=== Pre-Chaos Integration Test ==="

echo "[1/5] Stopping workers + wiping DB..."
pkill -f "src.worker" 2>/dev/null || true
docker compose stop worker >/dev/null 2>&1 || true
sleep 0.5
docker compose exec -T db psql -U soup -d soup -c "TRUNCATE side_effects, jobs;" >/dev/null

echo "[2/5] Creating 20 jobs..."
STAMP=$(date +%s)
for i in $(seq 1 20); do
  curl -sS -X POST localhost:8000/jobs -H 'content-type: application/json' \
    -d "{\"idempotency_key\":\"int-$STAMP-$i\",\"payload\":{\"n\":$i}}" >/dev/null
done
QUEUED=$($PSQL -c "SELECT count(*) FROM jobs WHERE state='queued';")
echo "  queued: $QUEUED"
if [[ "$QUEUED" != "20" ]]; then echo "  FAIL: expected 20 queued"; exit 1; fi

echo "[3/5] Starting 2 workers (no POISON_TEST)..."
sup_worker() {
  local name=$1
  while true; do
    DATABASE_URL=$DB_URL WORKER_ID=$name \
      python -m src.worker >> "$LOG/$name.log" 2>&1 || true
    sleep 0.3
  done
}
sup_worker w1 & P1=$!
sup_worker w2 & P2=$!

echo "[4/5] Waiting for queue to drain (max 30s)..."
for i in $(seq 1 30); do
  REMAIN=$($PSQL -c "SELECT count(*) FROM jobs WHERE state IN ('queued','running');")
  if [[ "$REMAIN" == "0" ]]; then echo "  drained in ${i}s"; break; fi
  sleep 1
done

echo "[5/5] Stopping workers + checking invariants..."
kill $P1 $P2 2>/dev/null || true
pkill -f "src.worker" 2>/dev/null || true
sleep 0.5

TOTAL=$($PSQL -c "SELECT count(*) FROM jobs;")
SUCC=$($PSQL -c "SELECT count(*) FROM jobs WHERE state='succeeded';")
FAILED=$($PSQL -c "SELECT count(*) FROM jobs WHERE state='failed';")
RUNNING=$($PSQL -c "SELECT count(*) FROM jobs WHERE state='running';")
QUEUED=$($PSQL -c "SELECT count(*) FROM jobs WHERE state='queued';")
CANCELLED=$($PSQL -c "SELECT count(*) FROM jobs WHERE state='cancelled';")
EFFECTS=$($PSQL -c "SELECT count(*) FROM side_effects;")
DUPS=$($PSQL -c "SELECT count(*) FROM (SELECT job_id FROM side_effects GROUP BY job_id HAVING count(*)>1) x;")
ORPHAN=$($PSQL -c "SELECT count(*) FROM jobs j LEFT JOIN side_effects s ON s.job_id=j.id WHERE j.state='succeeded' AND s.id IS NULL;")

echo ""
echo "  total:       $TOTAL"
echo "  succeeded:   $SUCC"
echo "  failed:      $FAILED"
echo "  running:     $RUNNING"
echo "  queued:      $QUEUED"
echo "  cancelled:   $CANCELLED"
echo "  effects:     $EFFECTS"
echo "  duplicates:  $DUPS"
echo "  succeeded w/o effect: $ORPHAN"
echo ""

PASS=true
if [[ "$TOTAL" != "20" ]]; then echo "  FAIL: total != 20"; PASS=false; fi
if [[ "$SUCC" != "20" ]]; then echo "  FAIL: succeeded != 20"; PASS=false; fi
if [[ "$FAILED" != "0" || "$CANCELLED" != "0" ]]; then echo "  FAIL: unexpected failed/cancelled"; PASS=false; fi
if [[ "$RUNNING" != "0" ]]; then echo "  FAIL: stuck in running"; PASS=false; fi
if [[ "$QUEUED" != "0" ]]; then echo "  FAIL: stuck in queued"; PASS=false; fi
if [[ "$EFFECTS" != "20" ]]; then echo "  FAIL: effects != 20"; PASS=false; fi
if [[ "$DUPS" != "0" ]]; then echo "  FAIL: duplicate side effects"; PASS=false; fi
if [[ "$ORPHAN" != "0" ]]; then echo "  FAIL: succeeded job with no effect"; PASS=false; fi

if $PASS; then echo "=== PASS ==="; exit 0; else echo "=== FAIL ==="; exit 1; fi