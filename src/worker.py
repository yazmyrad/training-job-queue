import logging
import os
import signal
import time
import uuid

from src.db.connection import get_conn 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("worker")

WORKER_ID = os.environ.get("WORKER_ID") or f"w-{uuid.uuid4().hex[:6]}"
LEASE_SECS = 10          
POLL_INTERVAL = 0.5

CLAIM_SQL = """
    UPDATE jobs
    SET state = 'running',
        worker_id = %s,
        lease_until = now() + %s * interval '1 second',
        attempt = attempt + 1,
        updated_at = now()
    WHERE id = (
        SELECT id FROM jobs
        WHERE state = 'queued'
           OR (state = 'running' AND lease_until < now())
        ORDER BY created_at
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    RETURNING id, attempt;
"""

FENCE_SQL="""
    UPDATE jobs
    SET state = 'succeeded', updated_at = now()
    WHERE id = %s AND state = 'running' AND worker_id = %s AND attempt = %s
    RETURNING id;
"""

SIDE_EFFECT_SQL="""
    INSERT INTO side_effects (job_id, attempt)
    VALUES (%s, %s)
    ON CONFLICT (job_id) DO NOTHING;
"""


def claim(conn):
    with conn.cursor() as cur:
        cur.execute(CLAIM_SQL, (WORKER_ID, LEASE_SECS))
        return cur.fetchone()

def execute_and_complete(conn, job_id, attempt):
    time.sleep(0.05)

    with conn:
        with conn.cursor() as cur:
            cur.execute(FENCE_SQL, (str(job_id), WORKER_ID, attempt))
            if cur.fetchone() is None:
                log.warning(
                    "worker %s LOST job=%s attempt=%d (no longer owner)",
                    WORKER_ID, job_id, attempt,
                )
                return "lost"

            cur.execute(SIDE_EFFECT_SQL, (str(job_id), attempt))
            log.info(
                "worker %s succeeded job=%s attempt=%d side_effect_rows=%d",
                WORKER_ID, job_id, attempt, cur.rowcount,
            )
            return "succeeded"

def main():
    log.info("worker %s starting", WORKER_ID)
    stop = {"flag": False}

    def _stop(signum, frame):
        stop["flag"] = True
        log.info("worker %s: signal %s, stopping after current claim", WORKER_ID, signum)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    conn = get_conn()
    try:
        while not stop["flag"]:
            with conn:                       
                row = claim(conn)
            if row:
                job_id, attempt = row
                log.info("worker %s claimed job=%s attempt=%d", WORKER_ID, job_id, attempt)
                result = execute_and_complete(conn, job_id, attempt)
            else:
                time.sleep(POLL_INTERVAL)
    finally:
        conn.close()
        log.info("worker %s stopped", WORKER_ID)


if __name__ == "__main__":
    main()