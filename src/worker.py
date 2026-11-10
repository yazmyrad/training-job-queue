import logging
import os
import time

from src.db.connection import check_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("worker")

WORKER_ID = os.environ.get("WORKER_ID", "worker-1")


def main():
    log.info("worker %s starting", WORKER_ID)

    for attempt in range(30):
        if check_db():
            log.info("worker %s: db ok", WORKER_ID)
            break
        log.info("worker %s: db not ready (attempt %d)", WORKER_ID, attempt + 1)
        time.sleep(1)
    else:
        log.error("worker %s: db never came up, exiting", WORKER_ID)
        raise SystemExit(1)

    while True:
        time.sleep(5)
        log.info("worker %s alive", WORKER_ID)


if __name__ == "__main__":
    main()