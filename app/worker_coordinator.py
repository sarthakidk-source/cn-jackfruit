"""
Shared worker coordinator (Demo 2). Exactly ONE of these runs; the servers never
talk to workers.

It is the existing TaskManager (worker registration, heartbeats, scheduling,
retries after worker failure, Ryu notifications) with its task table connected to
the shared SQLite queue by a small sync loop:

  pull   claim PENDING rows from the database (atomic) and hand them to the manager
  push   copy every task's state, worker, result and attempts back to the database

Run from the project root, e.g. on h1:
    python3 -u -m app.worker_coordinator --db results/demo2.db
"""
import argparse
import logging
import signal
import threading
import time

from app import shared_queue as sq
from app import taskmodel as tm
from app.controller_client import DEFAULT_SOCKET, ControllerClient, NullController
from app.scheduler import LEAST_LOADED, POLICIES
from app.task_manager import TaskManager

log = logging.getLogger("coordinator")

# task state inside the manager -> status stored in the database
DB_STATUS = {tm.PENDING: sq.CLAIMED, tm.ASSIGNED: sq.ASSIGNED,
             tm.COMPLETED: sq.COMPLETED, tm.FAILED: sq.FAILED}


def pull_new_tasks(mgr, db):
    """Move tasks from the shared queue into the manager."""
    claimed = sq.claim_pending(db)
    if not claimed:
        return
    ack = mgr._submit({"tasks": [dict(spec, task_id=tid) for tid, spec in claimed]})
    if ack.get("type") != "SUBMIT_ACK":       # whole batch refused: fail all of it
        bad = [tid for tid, _ in claimed]
    else:
        bad = [claimed[r["index"]][0] for r in ack["rejected"]]
    if bad:
        log.warning("manager refused %d task(s)", len(bad))
        sq.update_tasks(db, [(sq.FAILED, None, "rejected by coordinator", 0, time.time(), tid)
                             for tid in bad])


def push_changes(mgr, db, last_written):
    """Write every task whose state changed since the last push to the database."""
    with mgr.lock:
        snapshot = [(t.task_id, t.state, t.worker, t.attempts, t.result,
                     t.completed_at or t.history[-1][0]) for t in mgr.tasks.values()]
    rows = []
    for tid, state, worker, attempts, result, when in snapshot:
        key = (state, worker, attempts)
        if last_written.get(tid) == key:
            continue
        last_written[tid] = key
        finished = state in tm.TERMINAL
        rows.append((DB_STATUS[state], worker, None if result is None else str(result),
                     attempts, when if finished else None, tid))
    if rows:
        sq.update_tasks(db, rows)


def sync_loop(mgr, db_path, stop, interval=0.1):
    db = sq.connect(db_path)
    last_written = {}
    while not stop.wait(interval):
        try:
            pull_new_tasks(mgr, db)
            push_changes(mgr, db, last_written)
        except Exception:        # keep the coordinator alive no matter what
            log.exception("sync failed")
    push_changes(mgr, db, last_written)     # final flush


def main():
    p = argparse.ArgumentParser(description="Shared worker coordinator")
    p.add_argument("--db", required=True, help="shared SQLite file")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--worker-port", type=int, default=9100)
    p.add_argument("--policy", choices=POLICIES, default=LEAST_LOADED)
    p.add_argument("--cap", type=int, default=2)
    p.add_argument("--hb-interval", type=float, default=1.0)
    p.add_argument("--hb-timeout", type=float, default=3.0)
    p.add_argument("--max-attempts", type=int, default=None)
    p.add_argument("--controller", default=DEFAULT_SOCKET,
                   help="Unix socket of the SDN controller, or 'none'")
    p.add_argument("--quarantine", type=float, default=0.0)
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        datefmt="%H:%M:%S",
                        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s")

    ctl = NullController() if args.controller.lower() == "none" \
        else ControllerClient(args.controller)
    mgr = TaskManager(policy=args.policy, cap=args.cap, hb_interval=args.hb_interval,
                      hb_timeout=args.hb_timeout, max_attempts_cap=args.max_attempts,
                      controller=ctl, quarantine_s=args.quarantine)
    sq.connect(args.db).close()            # create the tables if this is the first process
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())

    # the manager's own client port is not used in Demo 2 (port 0 = any free port)
    mgr.start(args.host, args.worker_port, 0)
    syncer = threading.Thread(target=sync_loop, args=(mgr, args.db, stop))
    syncer.start()
    stop.wait()
    syncer.join()
    mgr.stop()


if __name__ == "__main__":
    main()
