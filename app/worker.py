"""
Stage 4 worker (runs on w1..w4).

Connects to the task manager, registers, then loops: receive TASK, do the work,
send RESULT. --slowdown multiplies the simulated work time, which lets us make
some workers slower than others in later experiments.

Run from the project root:
    python3 -u -m app.worker --id w1 --manager 10.0.0.1
"""
import argparse
import logging
import socket
import sys
import time

from app.protocol import (REGISTER, REGISTER_ACK, RESULT, TASK,
                          MessageChannel, ProtocolError)

log = logging.getLogger("worker")


def connect(ip, port, attempts, delay):
    for n in range(1, attempts + 1):
        try:
            s = socket.create_connection((ip, port), timeout=5)
            s.settimeout(None)
            return s
        except OSError as e:
            log.warning("connect attempt %d/%d failed: %s", n, attempts, e)
            time.sleep(delay)
    return None


def run_task(msg, slowdown):
    """Real CPU work (sum of squares) plus a simulated delay."""
    n = int(msg.get("n", 0))
    work_ms = float(msg.get("work_ms", 0))
    t0 = time.time()
    output = sum(i * i for i in range(n))
    time.sleep(work_ms / 1000.0 * slowdown)
    return {"output": output, "elapsed_ms": round((time.time() - t0) * 1000, 1)}


def main():
    p = argparse.ArgumentParser(description="Worker (Stage 4)")
    p.add_argument("--id", required=True)
    p.add_argument("--manager", required=True, help="manager IP, e.g. 10.0.0.1")
    p.add_argument("--port", type=int, default=9100)
    p.add_argument("--slowdown", type=float, default=1.0)
    p.add_argument("--attempts", type=int, default=10)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, datefmt="%H:%M:%S",
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    sock = connect(args.manager, args.port, args.attempts, 1.0)
    if sock is None:
        log.error("could not reach manager %s:%d", args.manager, args.port)
        return 1
    ch = MessageChannel(sock)
    try:
        ch.send({"type": REGISTER, "worker_id": args.id, "slowdown": args.slowdown})
        ack = ch.recv()
        if ack is None or ack["type"] != REGISTER_ACK:
            log.error("registration failed: %s", ack)
            return 1
        log.info("registered with manager %s:%d as %s", args.manager, args.port, args.id)

        while True:
            msg = ch.recv()
            if msg is None:
                log.info("manager closed the connection")
                break
            if msg["type"] == TASK:
                task_id = msg.get("task_id")
                log.info("received task %s", task_id)
                res = run_task(msg, args.slowdown)
                ch.send({"type": RESULT, "task_id": task_id, "worker_id": args.id,
                         "status": "OK", **res})
                log.info("sent result for %s", task_id)
            else:
                log.warning("ignoring %s", msg["type"])
    except (ProtocolError, OSError) as e:
        log.error("connection problem: %s", e)
        return 1
    finally:
        ch.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
    