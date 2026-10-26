"""
Worker (runs on w1..w4).

Connects to the task manager, registers, then:
  * main thread       reads messages from the manager (TASK, HEARTBEAT_ACK, SHUTDOWN)
  * executor thread   runs queued tasks one at a time and sends RESULTs
  * heartbeat thread  sends HEARTBEAT every heartbeat_interval so the manager can
                      tell "busy" from "dead", even while a long task is running

If the manager is silent for too long (no HEARTBEAT_ACK) or the connection drops,
the worker reconnects (unless --no-reconnect). That is what lets a worker come
back after a network fault has been repaired.

--slowdown multiplies the simulated work time, so some workers can be made
slower than others (used by the scheduling experiment).

Failure injection (for tests and experiments):
  --crash-on-task N   hard-exit the process when the Nth task starts
  --hang-on-task N    freeze (keep the socket open but stop all activity)
                      when the Nth task starts

Run from the project root, e.g. on w1:
    python3 -u -m app.worker --id w1 --manager 10.0.0.1
"""
import argparse
import logging
import os
import queue
import signal
import socket
import sys
import threading
import time

from app import taskmodel as tm
from app.protocol import (ERROR, HEARTBEAT, HEARTBEAT_ACK, PROTOCOL_VERSION, REGISTER,
                          REGISTER_ACK, RESULT, SHUTDOWN, TASK, MessageChannel,
                          ProtocolError)

log = logging.getLogger("worker")


def run_task(msg, slowdown):
    """Execute one TASK message. Returns the fields of a RESULT message.
    Raises ValueError for a malformed task."""
    task_type = msg.get("task_type")
    n = msg.get("n")
    if task_type not in tm.TASK_TYPES:
        raise ValueError("unknown task_type %r" % (task_type,))
    if isinstance(n, bool) or not isinstance(n, int) or not 0 <= n <= tm.MAX_N:
        raise ValueError("bad n %r" % (n,))
    work_ms = msg.get("work_ms", 0)
    if isinstance(work_ms, bool) or not isinstance(work_ms, (int, float)) or work_ms < 0:
        raise ValueError("bad work_ms %r" % (work_ms,))
    payload = msg.get("payload", "")
    if not isinstance(payload, str) or len(payload) != msg.get("payload_bytes", 0):
        raise ValueError("payload length does not match payload_bytes")
    t0 = time.time()
    output = tm.compute(task_type, n)
    if work_ms:
        time.sleep(work_ms / 1000.0 * slowdown)
    return {"output": output, "payload_crc": tm.payload_crc(payload),
            "elapsed_ms": round((time.time() - t0) * 1000, 1)}


class Worker:
    def __init__(self, worker_id, manager, port=9100, slowdown=1.0, reconnect=True,
                 attempts=30, crash_on_task=None, hang_on_task=None):
        self.worker_id = worker_id
        self.manager = manager
        self.port = port
        self.slowdown = slowdown
        self.reconnect = reconnect
        self.attempts = attempts
        self.crash_on_task = crash_on_task
        self.hang_on_task = hang_on_task
        self.stop_event = threading.Event()
        self._frozen = False
        self._started = 0
        self.completed = 0
        self._sock = None
        self.shutdown_received = False

    # ---------------------------------------------------------------- top level
    def run(self):
        failures = 0
        while not self.stop_event.is_set():
            sock = self._connect()
            if sock is None:
                failures += 1
                if failures >= self.attempts or not self.reconnect and failures >= 1:
                    log.error("giving up: cannot reach manager %s:%d", self.manager, self.port)
                    return 1
                self.stop_event.wait(1.0)
                continue
            failures = 0
            outcome = self._session(sock)
            if outcome == "shutdown" or self.stop_event.is_set():
                log.info("worker exiting (%s)", outcome)
                return 0
            log.warning("session ended (%s)", outcome)
            if not self.reconnect:
                return 1
            self.stop_event.wait(1.0)
        return 0

    def stop(self):
        self.stop_event.set()
        s = self._sock
        if s is not None:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _connect(self):
        try:
            s = socket.create_connection((self.manager, self.port), timeout=5)
        except OSError as e:
            log.warning("connect to %s:%d failed: %s", self.manager, self.port, e)
            return None
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        return s

    # ---------------------------------------------------------------- one session
    def _session(self, sock):
        self._sock = sock
        ch = MessageChannel(sock)
        done = threading.Event()          # set when this session is over
        work_q = queue.Queue()
        threads = []
        try:
            sock.settimeout(5)
            ch.send({"type": REGISTER, "worker_id": self.worker_id,
                     "slowdown": self.slowdown, "version": PROTOCOL_VERSION})
            ack = ch.recv()
            if ack is None:
                return "manager closed during registration"
            if ack["type"] != REGISTER_ACK:
                return "registration refused: %s" % ack.get("reason", ack["type"])
            hb = float(ack.get("heartbeat_interval", 1.0))
            log.info("registered with manager %s:%d as %s (heartbeat %.1fs)",
                     self.manager, self.port, self.worker_id, hb)

            for target, name in ((self._heartbeat_loop, "hb"), (self._exec_loop, "exec")):
                th = threading.Thread(target=target, name=name, daemon=True,
                                      args=(ch, done, work_q, hb))
                th.start()
                threads.append(th)

            sock.settimeout(max(3 * hb, 3.0))
            while not done.is_set():
                if self._frozen:
                    time.sleep(0.2)
                    continue
                try:
                    msg = ch.recv()
                except socket.timeout:
                    if self._frozen:
                        continue
                    return "manager silent"
                if msg is None:
                    return "manager closed the connection"
                mtype = msg["type"]
                if mtype == TASK:
                    work_q.put(msg)
                elif mtype == SHUTDOWN:
                    self.shutdown_received = True
                    return "shutdown"
                elif mtype == HEARTBEAT_ACK:
                    pass
                elif mtype == ERROR:
                    log.warning("manager says: %s", msg.get("reason"))
                else:
                    log.warning("ignoring unexpected %s", mtype)
            return "stopped"
        except ProtocolError as e:
            return "protocol error: %s" % e
        except OSError as e:
            return "socket error: %s" % e
        finally:
            done.set()
            ch.close()
            for th in threads:
                th.join(timeout=1.0)
            self._sock = None

    # ---------------------------------------------------------------- helper threads
    def _heartbeat_loop(self, ch, done, work_q, hb):
        while not done.wait(hb):
            if self._frozen:
                continue
            try:
                ch.send({"type": HEARTBEAT, "worker_id": self.worker_id,
                         "inflight": work_q.qsize(), "completed": self.completed})
            except OSError:
                done.set()
                return

    def _exec_loop(self, ch, done, work_q, hb):
        while not done.is_set():
            try:
                msg = work_q.get(timeout=0.2)
            except queue.Empty:
                continue
            self._started += 1
            if self.crash_on_task and self._started == self.crash_on_task:
                log.error("INJECTED CRASH on task %s", msg.get("task_id"))
                os._exit(1)
            if self.hang_on_task and self._started == self.hang_on_task:
                log.error("INJECTED HANG on task %s", msg.get("task_id"))
                self._frozen = True
                while True:
                    time.sleep(1)
            tid = msg.get("task_id")
            reply = {"type": RESULT, "task_id": tid, "attempt": msg.get("attempt"),
                     "worker_id": self.worker_id}
            try:
                reply.update(run_task(msg, self.slowdown), status="OK")
                self.completed += 1
                log.info("task %s done in %.0f ms", tid, reply["elapsed_ms"])
            except Exception as e:   # malformed task or unexpected failure
                log.warning("task %s failed: %s", tid, e)
                reply.update(status="ERROR", reason=str(e))
            try:
                ch.send(reply)
            except OSError:
                done.set()
                return


def main():
    p = argparse.ArgumentParser(description="Worker")
    p.add_argument("--id", required=True)
    p.add_argument("--manager", required=True, help="manager IP, e.g. 10.0.0.1")
    p.add_argument("--port", type=int, default=9100)
    p.add_argument("--slowdown", type=float, default=1.0)
    p.add_argument("--attempts", type=int, default=30,
                   help="consecutive failed connection attempts before giving up")
    p.add_argument("--no-reconnect", action="store_true")
    p.add_argument("--crash-on-task", type=int, default=None)
    p.add_argument("--hang-on-task", type=int, default=None)
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        datefmt="%H:%M:%S",
                        format="%(asctime)s.%(msecs)03d %(levelname)s %(name)s[%(process)d]: %(message)s")
    w = Worker(args.id, args.manager, args.port, args.slowdown,
               reconnect=not args.no_reconnect, attempts=args.attempts,
               crash_on_task=args.crash_on_task, hang_on_task=args.hang_on_task)

    def on_signal(signum, _frame):
        log.info("signal %d received, stopping", signum)
        w.stop()
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)
    sys.exit(w.run())


if __name__ == "__main__":
    main()
