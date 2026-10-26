"""
Task submission client (runs on h2).

Submits a batch of tasks to the manager, polls until every task is finished
(COMPLETED or FAILED) or the timeout expires, prints a summary, and optionally
writes everything (including the manager's metrics dump) to a JSON file.

Examples (from the project root, on h2):
    python3 -u -m app.client --manager 10.0.0.1 --tasks 20 --work-ms 200
    python3 -u -m app.client --manager 10.0.0.1 --tasks 40 --payload-kb 100 \
            --wait-workers 4 --out results/run1.json
    python3 -u -m app.client --manager 10.0.0.1 --status     # just show status

Programmatic use: run_batch(manager, port, specs, timeout) -> result dict
"""
import argparse
import json
import socket
import sys
import time

from app.protocol import (ERROR, METRICS, METRICS_REQ, STATUS, STATUS_REQ, SUBMIT,
                          SUBMIT_ACK, MessageChannel, ProtocolError)

CLIENT_PORT = 9200


def connect(manager, port, retries=20, delay=0.5):
    last = None
    for _ in range(retries):
        try:
            return MessageChannel(socket.create_connection((manager, port), timeout=5))
        except OSError as e:
            last = e
            time.sleep(delay)
    raise OSError("cannot reach manager %s:%d (%s)" % (manager, port, last))


def make_specs(count, task_type="sumsq", n=100000, work_ms=100.0, payload_bytes=0,
               uneven=False, deadline_s=10.0, max_attempts=3, prefix="t", idempotent=True):
    """Build a deterministic list of task specs. With uneven=True every 4th task
    takes 4x as long, so total work is not evenly divisible."""
    specs = []
    for i in range(count):
        factor = 4.0 if uneven and i % 4 == 3 else 1.0
        specs.append({"task_id": "%s%04d" % (prefix, i), "task_type": task_type, "n": n,
                      "work_ms": work_ms * factor, "payload_bytes": payload_bytes,
                      "deadline_s": deadline_s, "max_attempts": max_attempts,
                      "idempotent": idempotent})
    return specs


def wait_for_workers(ch, count, timeout):
    end = time.time() + timeout
    while time.time() < end:
        ch.send({"type": STATUS_REQ, "task_ids": [], "workers": True})
        reply = ch.recv()
        if reply and reply["type"] == STATUS and len(reply["workers"]) >= count:
            return True
        time.sleep(0.25)
    return False


def run_batch(manager, port, specs, timeout=120.0, poll=0.2, want_workers=0,
              wait_timeout=30.0, verbose=True):
    ch = connect(manager, port)
    try:
        if want_workers and not wait_for_workers(ch, want_workers, wait_timeout):
            raise RuntimeError("fewer than %d workers registered within %.0fs"
                               % (want_workers, wait_timeout))
        t_submit = time.time()
        ch.send({"type": SUBMIT, "tasks": specs})
        ack = ch.recv()
        if ack is None or ack["type"] == ERROR:
            raise RuntimeError("submit refused: %s" % (ack,))
        if ack["type"] != SUBMIT_ACK:
            raise ProtocolError("unexpected reply %s" % ack["type"])
        ids = ack["accepted"] + ack["duplicates"]
        if verbose:
            print("submitted %d tasks (%d duplicate, %d rejected)"
                  % (len(ack["accepted"]), len(ack["duplicates"]), len(ack["rejected"])))
        deadline = time.time() + timeout
        status = None
        timed_out = False
        while True:
            ch.send({"type": STATUS_REQ, "task_ids": ids, "workers": False})
            status = ch.recv()
            if status is None or status["type"] != STATUS:
                raise ProtocolError("bad status reply: %s" % (status,))
            s = status["summary"]
            if all(t["state"] in ("COMPLETED", "FAILED") for t in status["tasks"]):
                break
            if time.time() > deadline:
                timed_out = True
                break
            time.sleep(poll)
        t_done = time.time()
        ch.send({"type": METRICS_REQ})
        metrics = ch.recv()
        if metrics is None or metrics["type"] != METRICS:
            raise ProtocolError("bad metrics reply")
        metrics.pop("type", None)
    finally:
        ch.close()

    tasks = status["tasks"]
    done = [t for t in tasks if t["state"] == "COMPLETED"]
    failed = [t for t in tasks if t["state"] == "FAILED"]
    starts = [t["submitted_at"] for t in tasks]
    ends = [t["completed_at"] for t in done if t["completed_at"]]
    makespan = (max(ends) - min(starts)) if ends and starts else None
    per_worker = {}
    for t in done:
        per_worker[t["worker"]] = per_worker.get(t["worker"], 0) + 1
    lat = sorted(t["latency_ms"] for t in done if t["latency_ms"] is not None)
    result = {
        "submitted": len(ids), "completed": len(done), "failed": len(failed),
        "unfinished": len(tasks) - len(done) - len(failed), "timed_out": timed_out,
        "client_wall_s": round(t_done - t_submit, 3),
        "makespan_s": None if makespan is None else round(makespan, 3),
        "throughput_tps": None if not makespan else round(len(done) / makespan, 3),
        "latency_ms": {
            "mean": round(sum(lat) / len(lat), 1) if lat else None,
            "median": lat[len(lat) // 2] if lat else None,
            "max": lat[-1] if lat else None},
        "tasks_per_worker": per_worker,
        "requeues": sum(t["requeues"] for t in tasks),
        "t_submit": t_submit, "t_done": t_done,
        "tasks": tasks, "manager_metrics": metrics,
    }
    return result


def print_summary(r):
    print("completed %d / %d   failed %d   unfinished %d   timed_out=%s"
          % (r["completed"], r["submitted"], r["failed"], r["unfinished"], r["timed_out"]))
    print("makespan %s s   throughput %s tasks/s   requeues %d"
          % (r["makespan_s"], r["throughput_tps"], r["requeues"]))
    print("latency ms: %s" % r["latency_ms"])
    print("tasks per worker: %s" % dict(sorted(r["tasks_per_worker"].items())))


def show_status(manager, port):
    ch = connect(manager, port)
    try:
        ch.send({"type": STATUS_REQ, "workers": True})
        reply = ch.recv()
    finally:
        ch.close()
    print(json.dumps({"summary": reply["summary"], "workers": reply["workers"]}, indent=1))


def main():
    p = argparse.ArgumentParser(description="Submit a batch of tasks")
    p.add_argument("--manager", required=True)
    p.add_argument("--port", type=int, default=CLIENT_PORT)
    p.add_argument("--status", action="store_true", help="only print manager status")
    p.add_argument("--tasks", type=int, default=20)
    p.add_argument("--type", dest="task_type", default="sumsq", choices=("sumsq", "primes"))
    p.add_argument("--n", type=int, default=100000)
    p.add_argument("--work-ms", type=float, default=100.0)
    p.add_argument("--payload-kb", type=float, default=0.0)
    p.add_argument("--uneven", action="store_true", help="every 4th task is 4x heavier")
    p.add_argument("--deadline", type=float, default=10.0, help="per-task deadline (s)")
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--non-idempotent", action="store_true",
                   help="mark tasks as not retry-safe (never redistributed)")
    p.add_argument("--prefix", default="t")
    p.add_argument("--timeout", type=float, default=120.0)
    p.add_argument("--wait-workers", type=int, default=0)
    p.add_argument("--out", default=None, help="write full result JSON here")
    args = p.parse_args()

    if args.status:
        show_status(args.manager, args.port)
        return 0
    specs = make_specs(args.tasks, args.task_type, args.n, args.work_ms,
                       int(args.payload_kb * 1024), args.uneven, args.deadline,
                       args.max_attempts, args.prefix, not args.non_idempotent)
    result = run_batch(args.manager, args.port, specs, args.timeout,
                       want_workers=args.wait_workers)
    print_summary(result)
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=1)
        print("full result written to %s" % args.out)
    return 0 if result["failed"] == 0 and result["unfinished"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
