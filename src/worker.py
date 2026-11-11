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


def claim(conn):
    with conn.cursor() as cur:
        cur.execute(CLAIM_SQL, (WORKER_ID, LEASE_SECS))
        return cur.fetchone()


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
                time.sleep(0.2)
                log.info("worker %s claimed job=%s attempt=%d", WORKER_ID, job_id, attempt)
            else:
                time.sleep(POLL_INTERVAL)
    finally:
        conn.close()
        log.info("worker %s stopped", WORKER_ID)


if __name__ == "__main__":
    main()