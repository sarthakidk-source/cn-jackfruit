"""
Stage 4 task manager (runs on h1).

Listens for TCP connections from workers. Each connection gets its own thread.
Stage 4 supports: REGISTER -> REGISTER_ACK, then a short demo where the manager
sends a few TASK messages one at a time and waits for each RESULT.
(Stage 5 replaces the demo with a real scheduler.)

Run from the project root:
    python3 -u -m app.task_manager --demo-tasks 3
"""
import argparse
import logging
import socket
import threading
import time

from app.protocol import (ERROR, REGISTER, REGISTER_ACK, RESULT, TASK,
                          MessageChannel, ProtocolError)

log = logging.getLogger("manager")


class TaskManager:
    def __init__(self, demo_tasks):
        self.demo_tasks = demo_tasks
        self.workers = {}             # worker_id -> {"addr": (ip, port), "since": time}
        self.lock = threading.Lock()  # protects self.workers

    def serve(self, host, port):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((host, port))
        srv.listen(64)
        log.info("listening for workers on %s:%d", host, port)
        while True:
            conn, addr = srv.accept()
            threading.Thread(target=self.handle_worker, args=(conn, addr),
                             daemon=True).start()

    def handle_worker(self, conn, addr):
        ch = MessageChannel(conn)
        worker_id = None
        registered = False
        try:
            conn.settimeout(10)  # a new connection must register quickly
            msg = ch.recv()
            if msg is None:
                log.info("%s:%d closed before registering", *addr)
                return
            if msg["type"] != REGISTER or not msg.get("worker_id"):
                ch.send({"type": ERROR, "reason": "first message must be REGISTER with worker_id"})
                log.warning("bad first message from %s:%d: %s", addr[0], addr[1], msg["type"])
                return
            worker_id = str(msg["worker_id"])

            with self.lock:
                duplicate = worker_id in self.workers
                if not duplicate:
                    self.workers[worker_id] = {"addr": addr, "since": time.time()}
                    registered = True
                online = len(self.workers)
            if duplicate:
                ch.send({"type": ERROR, "reason": "worker_id already registered"})
                log.warning("rejected duplicate registration for %s", worker_id)
                return

            ch.send({"type": REGISTER_ACK, "worker_id": worker_id})
            log.info("registered worker %s from %s:%d (%d online)",
                     worker_id, addr[0], addr[1], online)

            self.run_demo(ch, conn, worker_id)

            conn.settimeout(None)  # idle wait; heartbeats arrive in Stage 7
            while True:
                msg = ch.recv()
                if msg is None:
                    log.info("worker %s disconnected", worker_id)
                    break
                log.info("ignoring unexpected %s from %s", msg["type"], worker_id)
        except ProtocolError as e:
            log.warning("protocol error with %s: %s", worker_id or addr, e)
        except OSError as e:  # includes socket.timeout and connection resets
            log.warning("socket error with %s: %s", worker_id or addr, e)
        finally:
            if registered:
                with self.lock:
                    self.workers.pop(worker_id, None)
            ch.close()

    def run_demo(self, ch, conn, worker_id):
        conn.settimeout(30)
        for i in range(1, self.demo_tasks + 1):
            task_id = "%s-demo-%d" % (worker_id, i)
            t0 = time.time()
            ch.send({"type": TASK, "task_id": task_id, "n": 100000, "work_ms": 300})
            res = ch.recv()
            if res is None:
                raise ProtocolError("worker closed the connection during a task")
            if res["type"] != RESULT or res.get("task_id") != task_id:
                raise ProtocolError("unexpected reply: %r" % (res,))
            log.info("task %s done by %s: status=%s output=%s round-trip=%.0f ms",
                     task_id, worker_id, res.get("status"), res.get("output"),
                     (time.time() - t0) * 1000)


def main():
    p = argparse.ArgumentParser(description="Task manager (Stage 4)")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--worker-port", type=int, default=9100)
    p.add_argument("--demo-tasks", type=int, default=3)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, datefmt="%H:%M:%S",
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    TaskManager(args.demo_tasks).serve(args.host, args.worker_port)


if __name__ == "__main__":
    main()