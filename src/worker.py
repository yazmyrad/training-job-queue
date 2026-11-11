import logging
import os
import signal
import time
import uuid
import threading

from src.db.connection import get_conn 

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("worker")

WORKER_ID = os.environ.get("WORKER_ID") or f"w-{uuid.uuid4().hex[:6]}"
LEASE_SECS = 10          
POLL_INTERVAL = 0.5
MAX_ATTEMPTS = 5

CLAIM_SQL = """
    UPDATE jobs
    SET state = 'running',
        worker_id = %s,
        lease_until = now() + %s * interval '1 second',
        attempt = attempt + 1,
        updated_at = now()
    WHERE id = (
        SELECT id FROM jobs
        WHERE (state = 'queued'
           OR (state = 'running' AND lease_until < now()))
            AND attempt < %s                              
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
REAP_SQL = """
    UPDATE jobs
    SET state = 'failed',
        last_error = 'max attempts exceeded',
        updated_at = now()
    WHERE state = 'running'
      AND lease_until < now()
      AND attempt >= %s
    RETURNING id;
"""

HEARTBEAT_SQL="""
        UPDATE jobs
        SET lease_until = now() + %s * interval '1 second'
        WHERE id = %s AND worker_id = %s AND attempt = %s
        RETURNING id;
    """


def claim(conn):
    with conn.cursor() as cur:
        cur.execute(CLAIM_SQL, (WORKER_ID, LEASE_SECS, MAX_ATTEMPTS))
        return cur.fetchone()

def reap(conn):
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(REAP_SQL, (MAX_ATTEMPTS,))
                rows = cur.fetchall()
                for (jid,) in rows:
                    log.warning("reaped job=%s (attempt>=%d) -> failed",
                                jid, MAX_ATTEMPTS)
    except Exception as e:
        log.warning("reaper error: %s", e)
    
def _heartbeat(stop_event, job_id, attempt):
    hb_conn = get_conn()
    try:
        while not stop_event.wait(LEASE_SECS / 3):
            try:
                with hb_conn:           
                    with hb_conn.cursor() as cur:
                        cur.execute(HEARTBEAT_SQL,
                                    (LEASE_SECS, str(job_id), WORKER_ID, attempt))
                        if cur.fetchone() is None:
                            log.warning(
                                "worker %s lost lease on %s (attempt=%d)",
                                WORKER_ID, job_id, attempt,
                            )
                            return
            except Exception as e:
                log.warning("heartbeat error: %s", e)
    finally:
        hb_conn.close()

POISON_TEST_ENABLED = os.environ.get("POISON_TEST") == "1"
SIMULATED_WORK_SECS = 0.05      
def execute_and_complete(conn, job_id, attempt):
    stop_hb = threading.Event()
    hb_thread = threading.Thread(
        target=_heartbeat, args=(stop_hb, job_id, attempt), daemon=True,
    )
    hb_thread.start()
    try:
        time.sleep(SIMULATED_WORK_SECS)

        if POISON_TEST_ENABLED:
            with conn.cursor() as cur:
                cur.execute("SELECT payload FROM jobs WHERE id = %s", (str(job_id),))
                row = cur.fetchone()
                if row and "poison" in str(row[0]):
                    raise RuntimeError("simulated crash (POISON_TEST)")

        with conn:
            with conn.cursor() as cur:
                cur.execute(FENCE_SQL, (str(job_id), WORKER_ID, attempt))
                if cur.fetchone() is None:
                    log.warning("worker %s LOST job=%s attempt=%d",
                                WORKER_ID, job_id, attempt)
                    return "lost"
                cur.execute(SIDE_EFFECT_SQL, (str(job_id), attempt))
                log.info("worker %s succeeded job=%s attempt=%d rows=%d",
                         WORKER_ID, job_id, attempt, cur.rowcount)
                return "succeeded"
    finally:
        stop_hb.set()
        hb_thread.join(timeout=1)

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
        last_reap = 0
        empty_polls = 0                
        while not stop["flag"]:
            with conn:
                row = claim(conn)
            if row:
                job_id, attempt = row
                log.info("worker %s claimed job=%s attempt=%d", WORKER_ID, job_id, attempt)
                execute_and_complete(conn, job_id, attempt)
                empty_polls = 0
            else:
                empty_polls += 1
                if time.time() - last_reap > 5:
                    reap(conn)
                    last_reap = time.time()
                if empty_polls % 10 == 0:
                    log.info("worker %s idle (%d empty polls)", WORKER_ID, empty_polls)
                time.sleep(POLL_INTERVAL)
    finally:
        conn.close()
        log.info("worker %s stopped", WORKER_ID)


if __name__ == "__main__":
    main()