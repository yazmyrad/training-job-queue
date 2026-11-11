#!/usr/bin/env python3
"""
Chaos harness: 10,000 jobs, 8 workers, kill -9 every ~2s,
concurrent duplicate POSTs and cancellations.

Run from project root with venv active:
    python scripts/chaos_harness.py
"""

import os, sys, time, random, signal, subprocess, threading, glob
import urllib.request, urllib.error
import psycopg2

API                = "http://localhost:8000"
DB_URL             = "postgresql://soup:soup@localhost:5432/soup"
NUM_JOBS           = 10_000
NUM_WORKERS        = 8
KILL_INTERVAL_MEAN = 2.0          # seconds
DRAIN_TIMEOUT      = 600          # seconds
DUP_POST_HZ        = 20           # duplicate POSTs per second
CANCEL_HZ          = 5            # cancels per second
DUP_KEY_POOL       = 500          # how many keys the dup-POST thread hammers

LOG_DIR = "/tmp/chaos"
os.makedirs(LOG_DIR, exist_ok=True)


def _http(method, path, body=None, timeout=5):
    data = None if body is None else __import__("json").dumps(body).encode()
    req = urllib.request.Request(
        API + path, data=data, method=method,
        headers={"content-type": "application/json"} if data else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status, __import__("json").loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, {}
    except Exception:
        return 0, {}


def http_post(path, body=None): return _http("POST", path, body)


def db():
    return psycopg2.connect(DB_URL)


def reset_db():
    with db() as conn, conn.cursor() as cur:
        cur.execute("TRUNCATE side_effects, jobs;")


def enqueue_baseline(n):
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO jobs (idempotency_key, payload)
            SELECT 'chaos-' || i, jsonb_build_object('n', i)
            FROM generate_series(1, %s) i;
        """, (n,))


def active_job_ids(limit=200):
    with db() as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT id FROM jobs WHERE state IN ('queued','running')
            ORDER BY random() LIMIT %s;
        """, (limit,))
        return [r[0] for r in cur.fetchall()]


def fetch_keys(n):
    with db() as conn, conn.cursor() as cur:
        cur.execute("SELECT idempotency_key FROM jobs ORDER BY random() LIMIT %s;", (n,))
        return [r[0] for r in cur.fetchall()]


# ---------------------------------------------------------------------------
# Worker fleet with auto-respawn
# ---------------------------------------------------------------------------
class Fleet:
    def __init__(self):
        self.procs = {}
        self.lock = threading.Lock()
        self.kills = 0
        self.spawns = 0

    def spawn(self, idx):
        env = os.environ.copy()
        env["DATABASE_URL"] = DB_URL
        env["WORKER_ID"] = f"cw{idx}"
        f = open(f"{LOG_DIR}/cw{idx}.log", "a")
        p = subprocess.Popen(
            [sys.executable, "-m", "src.worker"],
            env=env, cwd=os.getcwd(), stdout=f, stderr=subprocess.STDOUT,
        )
        with self.lock:
            self.procs[idx] = p
            self.spawns += 1

    def kill(self, idx):
        with self.lock:
            p = self.procs.get(idx)
            if p and p.poll() is None:
                try:
                    os.kill(p.pid, signal.SIGKILL)
                    p.wait()
                    self.kills += 1
                except ProcessLookupError:
                    pass

    def kill_all(self):
        with self.lock:
            for p in self.procs.values():
                if p.poll() is None:
                    try:
                        os.kill(p.pid, signal.SIGKILL)
                        p.wait()
                    except ProcessLookupError:
                        pass


# ---------------------------------------------------------------------------
# Threads
# ---------------------------------------------------------------------------
class Run:
    def __init__(self, keys):
        self.stop = threading.Event()
        self.keys = keys
        self.lock = threading.Lock()
        self.s = {
            "dup_posts": 0, "dup_201": 0, "dup_200": 0, "dup_err": 0,
            "cancels": 0, "cancel_200": 0, "cancel_409": 0, "cancel_404": 0, "cancel_err": 0,
        }

    def bump(self, k):
        with self.lock:
            self.s[k] += 1


def killer_thread(fleet, run):
    while not run.stop.is_set():
        t = random.uniform(KILL_INTERVAL_MEAN * 0.7, KILL_INTERVAL_MEAN * 1.3)
        if run.stop.wait(t):
            break
        idx = random.randint(0, NUM_WORKERS - 1)
        fleet.kill(idx)
        fleet.spawn(idx)


def dup_post_thread(run):
    while not run.stop.is_set():
        key = random.choice(run.keys)
        status, _ = http_post("/jobs", {"idempotency_key": key, "payload": {"dup": True}})
        run.bump("dup_posts")
        if status == 201: run.bump("dup_201")
        elif status == 200: run.bump("dup_200")
        else: run.bump("dup_err")
        time.sleep(1.0 / DUP_POST_HZ)


def cancel_thread(run):
    while not run.stop.is_set():
        ids = active_job_ids(limit=100)
        if ids:
            jid = random.choice(ids)
            status, _ = http_post(f"/jobs/{jid}/cancel")
            run.bump("cancels")
            if status == 200: run.bump("cancel_200")
            elif status == 409: run.bump("cancel_409")
            elif status == 404: run.bump("cancel_404")
            else: run.bump("cancel_err")
        time.sleep(1.0 / CANCEL_HZ)


# ---------------------------------------------------------------------------
# Drain + verify
# ---------------------------------------------------------------------------
def wait_for_drain(timeout):
    start = time.time()
    while time.time() - start < timeout:
        with db() as conn, conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM jobs WHERE state IN ('queued','running');")
            remaining = cur.fetchone()[0]
        if remaining == 0:
            return True, time.time() - start
        print(f"  ... draining: {remaining} remaining", flush=True)
        time.sleep(2)
    return False, time.time() - start


def verify():
    with db() as conn, conn.cursor() as cur:
        def q(sql, *a):
            cur.execute(sql, a)
            return cur.fetchone()[0]
        return {
            "total":              q("SELECT count(*) FROM jobs;"),
            "succeeded":          q("SELECT count(*) FROM jobs WHERE state='succeeded';"),
            "failed":             q("SELECT count(*) FROM jobs WHERE state='failed';"),
            "cancelled":          q("SELECT count(*) FROM jobs WHERE state='cancelled';"),
            "running":            q("SELECT count(*) FROM jobs WHERE state='running';"),
            "queued":             q("SELECT count(*) FROM jobs WHERE state='queued';"),
            "effects":            q("SELECT count(*) FROM side_effects;"),
            "dups":               q("SELECT count(*) FROM (SELECT job_id FROM side_effects GROUP BY job_id HAVING count(*)>1) x;"),
            "success_no_effect":  q("""SELECT count(*) FROM jobs j LEFT JOIN side_effects s
                                       ON s.job_id=j.id WHERE j.state='succeeded' AND s.id IS NULL;"""),
            "cancel_with_effect": q("""SELECT count(*) FROM jobs j JOIN side_effects s
                                       ON s.job_id=j.id WHERE j.state='cancelled';"""),
        }


def check_invariants(st):
    fails = []
    if st["total"] != NUM_JOBS:                                       fails.append(f"total={st['total']} != {NUM_JOBS} (job lost)")
    if st["running"] != 0:                                            fails.append(f"running={st['running']} (stuck in running)")
    if st["queued"] != 0:                                             fails.append(f"queued={st['queued']} (stuck in queued)")
    if st["succeeded"]+st["failed"]+st["cancelled"]+st["running"]+st["queued"] != NUM_JOBS:
        fails.append("terminal-state counts don't sum to total")
    if st["dups"] != 0:                                               fails.append(f"duplicate side effects={st['dups']}")
    if st["success_no_effect"] != 0:                                  fails.append(f"succeeded w/o side effect={st['success_no_effect']}")
    if st["cancel_with_effect"] != 0:                                 fails.append(f"cancelled with side effect={st['cancel_with_effect']}")
    return fails


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print(f"=== Chaos Harness === jobs={NUM_JOBS} workers={NUM_WORKERS} kill_mean={KILL_INTERVAL_MEAN}s\n", flush=True)

    print("[1/6] reset DB...", flush=True)
    reset_db()

    print(f"[2/6] enqueue {NUM_JOBS} baseline jobs...", flush=True)
    t0 = time.time(); enqueue_baseline(NUM_JOBS)
    print(f"  done in {time.time()-t0:.1f}s", flush=True)

    keys = fetch_keys(DUP_KEY_POOL)
    print(f"[3/6] fetched {len(keys)} keys for dup-POST thread\n", flush=True)

    print("[4/6] starting 8 workers...", flush=True)
    fleet = Fleet()
    for i in range(NUM_WORKERS):
        fleet.spawn(i)
    time.sleep(0.5)

    run = Run(keys)
    threads = [
        threading.Thread(target=killer_thread,   args=(fleet, run), daemon=True),
        threading.Thread(target=dup_post_thread, args=(run,),       daemon=True),
        threading.Thread(target=cancel_thread,   args=(run,),       daemon=True),
    ]
    for t in threads: t.start()

    print("[5/6] chaos running, waiting for drain...", flush=True)
    drained, elapsed = wait_for_drain(DRAIN_TIMEOUT)
    run.stop.set()
    for t in threads: t.join(timeout=2)
    fleet.kill_all()
    time.sleep(1)

    print(f"\n[6/6] drained={drained} in {elapsed:.1f}s", flush=True)
    st = verify()
    fails = check_invariants(st)

    print("\n--- FLEET ---")
    print(f"  kills={fleet.kills}  spawns={fleet.spawns}")
    print("\n--- CLIENT ---")
    print(f"  dup_posts={run.s['dup_posts']} (201={run.s['dup_201']} 200={run.s['dup_200']} err={run.s['dup_err']})")
    print(f"  cancels={run.s['cancels']} (200={run.s['cancel_200']} 409={run.s['cancel_409']} 404={run.s['cancel_404']} err={run.s['cancel_err']})")
    print("\n--- DB ---")
    for k, v in st.items():
        print(f"  {k}: {v}")

    lost = reaped = zero_effect = 0
    for f in glob.glob(f"{LOG_DIR}/cw*.log"):
        try:
            txt = open(f).read()
            lost        += txt.count("LOST")
            reaped      += txt.count("reaped job=")
            zero_effect += txt.count("rows=0")
        except Exception:
            pass
    print("\n--- WORKER LOGS ---")
    print(f"  LOST events (fence rejected a stale worker): {lost}")
    print(f"  reaped events: {reaped}")
    print(f"  side_effect_rows=0 (UNIQUE blocked a dup): {zero_effect}")

    print()
    if fails:
        print("=== FAIL ===")
        for f in fails: print("  - " + f)
        sys.exit(1)
    print("=== PASS ===")
    sys.exit(0)


if __name__ == "__main__":
    main()