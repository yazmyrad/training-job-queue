# training-job-queue

A durable job queue with crash-recoverable workers, built for the Soup backend task.

FastAPI HTTP API, Postgres storage, and N worker processes that claim jobs atomically, execute them with at-most-once side effects, and survive `kill -9` at any point.

> **Verdict at 100 rps: DON'T SHIP.** Correctness is solid; operational readiness is not proven. No throughput benchmark was run, no connection pool is implemented, and several silent-failure paths were exercised only in isolation. See [Verdict](#verdict-at-100-rps).

---

| | |
|---|---|
| **Storage** | Postgres 16, `FOR UPDATE SKIP LOCKED` claim |
| **Guarantee** | At-most-once side effect, at-least-once execution attempt |
| **Chaos result** | PASS — 10,000 jobs, 8 workers, 85 `kill -9`s in 173.6 s |
| **Invariants held** | No lost jobs, no duplicate side effects, nothing stuck in `running`, all terminal states consistent |
| **Blind spots** | Clock skew, network partition, DB restart, external side effects, sustained 100 rps throughput |
| **Design note** | [DESIGN.md](DESIGN.md) |

---

## Quickstart

```bash
git clone https://github.com/yazmyrad/training-job-queue.git && cd training-job-queue
docker compose up --build
```

This brings up:

- **db** — Postgres 16, schema loaded from `src/db/init.sql` on first boot
- **api** — FastAPI on `:8000`
- **worker** — 1 worker by default; scale with `docker compose up --scale worker=8`

Health check:

```bash
curl localhost:8000/health
# {"status":"ok","db":"ok"}
```

### Running workers locally (faster iteration)

```bash
pkill -f "src.worker"
DATABASE_URL=postgresql://soup:soup@localhost:5432/soup WORKER_ID=w1 python -m src.worker
DATABASE_URL=postgresql://soup:soup@localhost:5432/soup WORKER_ID=w2 python -m src.worker
```

---

## Repo structure

```
src/
  main.py                   # FastAPI app, /health
  worker.py                 # claim loop, fence, heartbeat, reaper
  config/settings.py        # DATABASE_URL
  db/
    connection.py           # get_conn / check_db
    init.sql                # schema
    models.py               # Pydantic models
  api/routes.py             # POST /jobs, GET, cancel
  scripts/
    reaper-test.sh         # B.4 evidence script
    chaos-integration.sh   # integration evidence script
    chaos_harness.py       # main chaos harness
  docs/chaos/
    b1-proof.txt ... b4-proof.txt
    pre-chaos-integration.txt
    chaos-run-01.log ... chaos-run-03.log

DESIGN.md                   # one-page design note
```

---

## API

| Method | Path | Notes |
|---|---|---|
| `POST` | `/jobs` | Body `{idempotency_key, payload}` → `201` new, `200` dedup |
| `GET` | `/jobs/{id}` | `200` / `404` / `422` on malformed UUID |
| `GET` | `/jobs` | Cursor pagination (`limit`, `before`, `state`) |
| `POST` | `/jobs/{id}/cancel` | `200` cancelled (or already cancelled), `409` terminal, `404` missing |

### States

```
queued ──▶ running ──┬──▶ succeeded
                     ├──▶ failed      (max attempts exceeded)
                     └──▶ cancelled
```

---

## Guarantees

- **At-most-once side effect.** Enforced by two mechanisms:
  1. Completion fence: `UPDATE ... WHERE id = ? AND worker_id = ? AND attempt = ?` — 0 rows means the caller has lost ownership.
  2. `UNIQUE(job_id)` on `side_effects` as a backstop.
- **At-least-once execution attempt.** Lease + reclaim clause + reaper together ensure every job reaches a terminal state in finite time.
- **Bounded retries.** `attempt < MAX_ATTEMPTS` in the claim query; the reaper flips exhausted jobs to `failed`.
- **DB-authoritative clock.** All timestamps use Postgres `now()`, never app-server time, which removes clock skew between workers as a failure mode.
- **Idempotent API.** Client retries of `POST /jobs` return the existing job (`200`), never a second one.

**Not guaranteed: at-most-once on external effects.** Our side effect is a DB row, per the assignment. A real-world effect such as sending an email requires the provider to honor an idempotency key; otherwise at-least-once execution can duplicate the external effect even when our row is written once.

---

## Testing evidence

Every mechanism was tested in isolation before chaos testing. Raw outputs are in `docs/chaos/`.

### B.1 — Claim loop
**Proved:** two workers claim different jobs; `SKIP LOCKED` prevents double-claim. The reclaim clause picks up jobs whose lease expired.
**Output:** `b1-proof.txt` — 5 jobs split w1: 2, w2: 3; `attempt` increments on reclaim.

### B.2 — Completion fence + side effect
**Proved:** 10 jobs → 10 succeeded, 10 side effects, 0 duplicates. Manual fence test: a fake worker with a stale `attempt` got `UPDATE 0` and did not steal the job from the current owner.
**Output:** `b2-proof.txt`

### B.3 — Heartbeat
**Proved:** a 25 s job (2.5× the lease) completed without being reclaimed by another worker. No LOST events, exactly 1 side effect, `attempt = 1`.
**Output:** `b3-proof.txt`

### B.4 — Reaper + max attempts
**Proved:** a poison-pill job crashed its worker 5 times, then stayed unclaimable (`attempt < 5` filter), then the reaper flipped it to `failed` with `last_error = 'max attempts exceeded'`. 0 side effects. A normal job running alongside it succeeded.
**Output:** `b4-proof.txt`

### Pre-chaos integration
**Proved:** 20 jobs, 2 workers, all four invariants (no lost, no duplicate, no stuck-running, consistent final states).
**Output:** `pre-chaos-integration.txt`

### Chaos runs

**Parameters:** 10,000 baseline jobs, 8 workers, `kill -9` every ~2 s (uniform 1.4–2.6 s), concurrent duplicate POSTs at 20/s, cancellations at 5/s.

| Run | Jobs | Kills | Duration | Succeeded | Cancelled | Failed | Running | Queued | Effects | Dups | LOST | Verdict |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 01 | 10,000 | 85 | 173.6 s | 9,616 | 384 | 0 | 0 | 0 | 9,616 | 0 | 1 | **PASS** |
| 02 | 10,000 | 106 | 218.3 s | 9,557 | 443 | 0 | 0 | 0 | 9,557 | 0 | 3 | **PASS** |
| 03 | 10,000 | 102 | 209.5 s | 9,564 | 436 | 0 | 0 | 0 | 9,564 | 0 | 3 | **PASS** |
| **Aggregate** | **30,000** | **293** | — | **28,737** | **1,263** | **0** | **0** | **0** | **28,737** | **0** | **7** | **3/3 PASS** |

**Run 01 client stats:**

- **1,294 duplicate POSTs** — all returned `200`, zero `201`s (idempotency held under concurrent load).
- **386 cancels** — 384× `200`, 2× `409`, 0 errors.
- **1 LOST event** — the fence rejected a stale worker at least once.
- **0 UNIQUE-constraint catches** — the `UNIQUE(job_id)` backstop never had to fire; the fence alone was sufficient.
- **0 reaped jobs** — expected, since no poison jobs were included in the run.

---

## Silent failures — how each was checked

The assignment lists six silent-failure modes. For each: how it would manifest, what prevents it, and how it was checked.

### 1. Double claim
*Two workers claim the same job, both run it, two side effects.*

- **Prevented by:** an atomic `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP LOCKED)` — no window between select and update.
- **Checked:** B.1 (two workers → two distinct claims) and chaos (0 duplicates over 10,000 jobs). During early iterations, removing `SKIP LOCKED` made the test flaky; that state was never committed.

### 2. Zombie worker returning after lease expiry
*Worker A freezes, its lease expires, worker B reclaims (`attempt + 1`) and completes. A wakes up and tries to complete.*

- **Prevented by:** `attempt` acts as a fencing token. A's completion `UPDATE` matches the old `attempt` and affects 0 rows.
- **Checked:** B.2 manual fence test (`UPDATE 0` with a stale attempt), plus 1 LOST event in chaos run 01 — a real stale worker rejected under chaos.
- **Honest note:** 1 LOST in 85 kills is low. The execution window (50 ms) is small compared to the kill interval (~2 s), so most kills don't land mid-execution. Raising `SIMULATED_WORK_SECS` to ~0.3 s would exercise the fence much harder. Not done for the run above.

### 3. Cancel / complete race
*A client cancels while a worker is completing.*

- **Prevented by:** both `UPDATE`s filter on `state = 'running'`; the row lock serializes them. The winner flips the state, the loser sees 0 rows.
- **Possible outcomes:** cancelled + 0 effects, or succeeded + 1 effect. Never both, never neither.
- **Checked:** chaos run 01 shows 384 cancelled jobs with 0 side effects. 2 of 386 cancel requests returned `409` (job already succeeded), showing the worker won the race in those cases.
- **Not directly observed:** a cancel and a completion hitting the exact same commit boundary — statistically rare even at 10,000 jobs.

### 4. Duplicate on request retry
*A client retries `POST` with the same idempotency key and two jobs are created.*

- **Prevented by:** `UNIQUE(idempotency_key)` + `ON CONFLICT DO NOTHING RETURNING`. On conflict, a follow-up `SELECT` returns the existing row with status `200`.
- **Checked:** chaos run 01 issued 1,294 concurrent duplicate POSTs. Every one returned `200`; zero duplicates created.

### 5. Transaction isolation
*A wrong isolation level could let a claim or completion observe inconsistent state.*

- **Prevented by:** Postgres default `READ COMMITTED` plus row-level locks. `FOR UPDATE SKIP LOCKED` is safe at this level, and the fence's `WHERE` clause is re-evaluated against committed state under the row lock, so no lost update is possible.
- **Checked:** argued from the SQL structure (single-statement atomic claim, conditional `UPDATE` fence) rather than observed by a targeted harness. Not tested at other isolation levels.

### 6. Clock skew
*Machines disagree on the time; leases expire early or late.*

- **Prevented by:** all timestamps use Postgres `now()` — the DB is the single time authority. Workers never compute lease expiry locally.
- **Checked:** argued by design only. The chaos harness runs on one host, so it cannot create real skew between workers. This is a blind spot, not a test.

---

## What the harness does NOT catch

Each of these is a failure mode the chaos test would miss if it appeared.

1. **Clock skew** — all workers share the host clock. Mitigated by DB `now()`, but not stress-tested.
2. **Network partitions** — everything runs on localhost. A worker↔DB partition would likely look like a connection drop → worker crash → lease expiry → reclaim, but no network-level fault injection was done.
3. **Postgres failover / restart** — single instance. A DB restart drops every worker connection at once; not simulated.
4. **Sub-millisecond cancel/complete race** — statistically unreachable at 10,000 jobs. Correctness relies on the SQL argument, not observation.
5. **External side effects** — our side effect is a DB row. Real external effects (email, S3, third-party APIs) need their own idempotency keys.
6. **Memory / thread / connection leaks** — a 173 s run surfaces gross leaks, not subtle ones. The heartbeat-thread connection leak found in code review would have taken roughly 30 minutes at high throughput to show up under load.
7. **Connection pool exhaustion** — there is no pool. Every handler and heartbeat opens a new connection; at 100 rps this would exhaust Postgres's default `max_connections` (100) within seconds.
8. **Disk full / OOM** — not simulated.
9. **High worker counts** — 8 workers is fine; 100 workers × 2 connections each = 200, beyond Postgres defaults.
10. **Sustained 100 rps throughput** — see the verdict below.

---

## Verdict at 100 rps

**DON'T SHIP.**

Correctness is proven at the scale tested (10,000 jobs, 8 workers, 85 kills). But surviving chaos is not the same as handling 100 rps, and throughput was never measured. The chaos run drained ~58 jobs/s while absorbing 85 kills — a floor, not a benchmark.

### Fix before shipping at 100 rps

1. **Connection pool.** Every request opens a fresh Postgres connection; at 100 rps the API alone would churn 100 connections/s. Use `psycopg2.pool.ThreadedConnectionPool` (or `asyncpg.Pool`). This is the #1 blocker — without it the system isn't viable above roughly 10 rps sustained.
2. **Throughput benchmark.** Run `wrk` / `k6` / `hey` at 100 rps sustained for at least 60 s to establish:
   - API p99 latency (target < 100 ms)
   - Claim-query contention under `SKIP LOCKED`
   - Queue depth over time (drains or grows unbounded?)
   - Required worker count (with 0.05 s simulated work, 8 workers ≈ 160 jobs/s ceiling; real work is slower)
3. **`MAX_ATTEMPTS` tuning.** Currently 5 with no justification. 5 attempts × 10 s lease ≈ 50 s retry window, likely too short for flaky external dependencies. Should be configurable per job type.
4. **Observability.** Logs are fine for debugging, not for production. Missing:
   - Structured (JSON) logs for aggregation
   - A `/metrics` endpoint: queue depth, claim rate, success rate, LOST count, reap count, worker count
   - Alerting hooks (reap count > 0 is a good "poison job present" signal)
5. **Graceful shutdown of long jobs.** The `SIGTERM` handler sets a stop flag that's checked only between claims. A worker running a 30 s job ignores `docker compose stop`, gets `SIGKILL` after the default 10 s timeout, and the job is reclaimed and re-executed. Either check the flag inside `execute_and_complete`, or set the stop timeout longer than the longest job.

### What works well

- **Correctness under failure is solid.** Zero duplicate side effects across 10,000 jobs and 85 forced crashes is the fence doing its job, not luck.
- **Idempotency holds under concurrent load** — 1,294 duplicate POSTs, all deduplicated correctly.
- **The design is defensible.** Every mechanism targets a specific failure, and each has evidence that it prevented that failure.

"DON'T SHIP" is about operational readiness, not correctness.

---

## What surprised me

**The UNIQUE constraint never fired.** Across 10,000 jobs and 85 kills, the fence caught every stale worker before it could write a side effect, so `UNIQUE(job_id)` was never needed. Good news — the fence is sufficient — but it also means the backstop is untested under chaos. It works in the manual B.2 test, not in live chaos.

**Idempotency was more forgiving than expected.** 1,294 concurrent duplicate POSTs: zero `201`s, zero errors. I had budgeted for one or two `500`s under contention; none occurred.

**Code review caught more bugs than any test.** Four real bugs — operator precedence in the claim query, an uninitialized `empty_polls`, the heartbeat connection leak, and test-code residue in the hot path — none surfaced in isolation testing.

**The LOST count was low — 1 in 85 kills.** The execution window is tiny, so the harness as designed barely exercises the fence. Not a correctness gap, but a testing gap.

---

## What still concerns me

- **The cancel/complete race was never directly observed** at the exact commit boundary. The SQL argument is sound, but the "loser affects 0 rows" path was only proven in the manual B.2 test.
- **The reaper was not chaos-tested.** Run 01 had zero reaps; the reaper is proven only by the B.4 isolation script. A chaos run with ~1% poison jobs would exercise it under load. If asked "how do you know the reaper works under load?" the honest answer is: it was tested in isolation, not under chaos.
- **The heartbeat was barely exercised.** With 50 ms simulated work and a 10 s lease, the heartbeat thread starts and stops almost immediately for every job. The 25 s B.3 test is the only evidence it keeps long jobs alive. It should be tested with realistic job durations (5–30 s) under concurrent load.
- **10,000 jobs is not proof of 100,000.** Some failure modes only appear at higher scale, e.g. autovacuum on `jobs` during `SKIP LOCKED` claims, or pool exhaustion once a pool exists.
- **Silent max-attempt failures.** A bug in `execute_and_complete` (say, a return path that skips the fence) would show up only as `attempt` climbing to `MAX_ATTEMPTS` and the job ending as `failed`. A `failed_due_to_max_attempts` counter should be a first-class metric; today you'd have to grep logs.

---

## AI tool usage

AI assistants were used throughout as pair-programming tools. Per the task's AI-tools policy, an honest accounting:

### Where AI helped

- **Design discussion** — walking through the claim / lease / fence / idempotency design and pressure-testing edge cases (zombie worker, cancel race, external effects).
- **Code review** — line-by-line review of `src/worker.py` and `src/routers/jobs.py` caught the four bugs listed above. Every fix was verified by re-running the relevant test.
- **Test script scaffolding** — `b4-reaper-test.sh`, `pre-chaos-integration.sh` and `chaos_harness.py` were drafted with AI, then adapted to the real schema, ports and log formats. Assertions were reviewed against the design note.
- **Debugging** — interpreting FastAPI / psycopg2 / Docker errors. One "curl shows nothing" issue turned out to be `curl -s` (fully silent) vs `curl -sS` (shows errors).

### Suggestions not accepted

- **async / asyncpg** — rejected. The worker is a synchronous loop on psycopg2; async would add complexity with no benefit at this scale.
- **docker-compose bind mount for hot reload** — not committed. The task requires `docker compose up` to start an unmodified image, and a dev bind mount can hide "forgot to copy a file into the image" bugs. Host-run workers cover fast iteration.
- **`init.sql`** — the AI draft was adopted with additions (partial indexes, `last_error` column), verified via `\d jobs`.

### Verified vs. taken on faith

- Every SQL statement was run against real Postgres and inspected (`\d`, `EXPLAIN`, live updates).
- Every script was run end-to-end; raw output is saved in `docs/chaos/`.
- `SKIP LOCKED` behavior was verified by observing two workers claim different jobs (B.1).
- The fence was verified by a stale worker getting `UPDATE 0` (B.2) and by 1 LOST event under chaos.
- **Not verified under chaos:** the `UNIQUE(job_id)` backstop. It works when triggered manually; the chaos run never triggered it.

### Where AI was wrong or misleading

- The first proposed `docker-compose.yml` omitted the DB healthcheck, which would have caused API/worker startup races on first boot. Caught in the second iteration.
- The first proposed `init.sql` lacked `UNIQUE(job_id)` on `side_effects`; it was added only after the backstop was discussed in design.
- Reusing `get_jobs(id)` inside the cancel handler was suggested as "DRY" and caused a `500` (`JSONResponse` has no `.data`). The fix was an inline query. Lesson: DRY across route handlers is often wrong; handlers are unit-of-work boundaries.

Design decisions — guarantee choice, isolation level, whether to pool — were made by me. The AI proposed options and pointed at precedents.

---

## Known limitations & future work

1. **No connection pool** — add `ThreadedConnectionPool` to `src/db/connection.py`.
2. **No throughput benchmark** — add a `wrk` or `k6` scenario at 100 rps sustained.
3. **Reaper scaling** — the reaper `UPDATE` (`state = 'running' AND lease_until < now() AND attempt >= MAX`) is fine at 10k jobs; at 1M+ it needs a verified partial index on `lease_until WHERE state = 'running'` or batched `SKIP LOCKED` reaping.
4. **No poison-pill chaos** — add ~1% always-crashing jobs and verify the reaper fires under chaos.
5. **Heartbeat barely exercised** — test with `SIMULATED_WORK_SECS` spanning 5–30 s.
6. **Single Postgres, single host** — no failover, partition, or clock-skew testing.
7. **No metrics endpoint** — add `/metrics` (queue depth, claim rate, LOST count, reap count, per-worker status).
8. **Graceful shutdown of long jobs** — `SIGTERM` is only checked between claims.
9. **`MAX_ATTEMPTS` is hardcoded** — should be per job type.

---

## Design note

See [DESIGN.md](DESIGN.md) for the one-page rationale: claim mechanism, dead-worker detection, cancel semantics, guarantee choice, and storage decision.
