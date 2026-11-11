# Design Note

Clients create jobs over HTTP, the API writes them to Postgres, and a pool of workers must execute every job, survive crashes at any point, and never produce a duplicate side effect.

## 1. Storage: why Postgres

Postgres can act as a queue out of the box: row-level locks plus `FOR UPDATE SKIP LOCKED` let many workers pull from one table without coordinating. SQLite locks the whole database on write and has no `SKIP LOCKED`, so concurrent workers would serialize. Postgres also serves as the single clock (`now()`), which removes clock skew between workers.

## 2. Claiming a job

Workers claim with one atomic statement:

```sql

UPDATE jobs SET state='running', worker_id=?, attempt=attempt+1, lease_until=now()+lease

WHERE id = (

  SELECT id FROM jobs

  WHERE (state='queued' OR (state='running' AND lease_until < now()))

    AND attempt < MAX_ATTEMPTS

  ORDER BY created_at

  FOR UPDATE SKIP LOCKED LIMIT 1)

RETURNING id, attempt;;

```

`FOR UPDATE` locks the chosen row; `SKIP LOCKED` makes other workers move to the next one, so no two workers claim the same job. Ordering by `created_at` keeps the queue roughly FIFO.

## 3. Detecting dead workers: lease, heartbeat, reaper

- **Lease.** Each claim sets `lease_until`. If the worker dies, the lease expires and any worker can reclaim the job.

- **Heartbeat.** A job may legitimately take longer than the lease, so a live worker extends `lease_until` periodically. If the process dies, heartbeats stop and the job is reclaimed.

- **Bounded retries.** Every claim increments `attempt`; the claim query skips jobs with `attempt >= MAX_ATTEMPTS`, and a reaper flips expired, exhausted jobs to `failed`. This stops a poison job that crashes every worker from cycling forever.

## 4. Fencing zombie workers

A zombie is a worker that freezes (GC pause, VM suspend, partition) and wakes up after its lease expired. Meanwhile worker B may have reclaimed and completed the job. A plain `UPDATE ... WHERE id=?` would let the zombie overwrite B's result.

`attempt` is the **fencing token**. Completion only succeeds if the caller still owns that exact attempt:

```sql

UPDATE jobs SET state='succeeded'

WHERE id=? AND state='running' AND worker_id=? AND attempt=?;

```

A claimed at `attempt=3`, B reclaimed at `attempt=4` → A's update matches 0 rows, A discards its result and logs `LOST`. As a second line of defense, `side_effects` has `UNIQUE(job_id)` with `ON CONFLICT DO NOTHING`, so even a buggy fence cannot record two effects.

## 5. Cancellation

| State at cancel | Result |

|---|---|

| `queued` | Cancelled; no worker will claim it |

| `running` | Race with completion (below) |

| `succeeded` / `failed` | `409 Conflict` — finished work cannot be undone |

| `(no such id)` | 404 Not Found |

Cancel and completion are both `UPDATE ... WHERE state='running'`. The row lock serializes them: whichever commits first flips the state, the other affects 0 rows. Outcomes are *cancelled + no effect* or *succeeded + one effect*, never both and never neither.

## 6. Guarantee

**At-most-once side effect, at-least-once execution attempt.**

Execution and effect are separate. A worker may run the work, get killed, and another worker runs it again, so execution can repeat. The effect is written zero times (cancelled, exhausted, or fenced out) or once (succeeded), never twice, enforced by the fence plus `UNIQUE(job_id)`. Exactly-once *execution* is not achievable when workers can die mid-job.

**Caveat:** this covers effects stored in our database. An external effect such as sending an email would happen twice on reclaim unless the external service honors an idempotency key (e.g. an `Idempotency-Key` header).

## 7. Two kinds of idempotency

- **API:** `UNIQUE(idempotency_key)` on `jobs` + `ON CONFLICT DO NOTHING RETURNING`. A retried `POST` returns the existing job (`200`) instead of creating a second one, the first creation returns (`201 Created`).

- **Worker:** `UNIQUE(job_id)` on `side_effects`. A repeated execution cannot record a second effect.

## 8. Known gap

A worker that is alive but stuck (e.g. an infinite loop) keeps heartbeating, so its lease never expires, `attempt` never increases, and the reaper never sees it. Bounding this requires a hard per-job execution deadline (stop heartbeating after `MAX_RUNTIME`), which is not implemented.

