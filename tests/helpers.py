"""Shared test helpers: a manager fixture, scriptable fake workers, a fake controller."""
import json
import os
import socket
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from app import taskmodel as tm  # noqa: E402
from app.client import connect, make_specs, run_batch  # noqa: E402
from app.protocol import (HEARTBEAT, REGISTER, RESULT, STATUS_REQ, SUBMIT,  # noqa: E402
                          MessageChannel)
from app.task_manager import TaskManager  # noqa: E402
from app.worker import Worker  # noqa: E402


def wait_until(cond, timeout=10.0, interval=0.02, msg="condition"):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(interval)
    raise AssertionError("timed out waiting for %s" % msg)


class ManagerFixture:
    """A real TaskManager on ephemeral loopback ports with short timeouts."""

    def __init__(self, **kw):
        kw.setdefault("hb_interval", 0.2)
        kw.setdefault("hb_timeout", 0.8)
        self.mgr = TaskManager(**kw)
        self.worker_port, self.client_port = self.mgr.start("127.0.0.1", 0, 0)
        self.threads = []
        self.workers = []

    def real_worker(self, worker_id, slowdown=1.0, **kw):
        w = Worker(worker_id, "127.0.0.1", self.worker_port, slowdown, **kw)
        t = threading.Thread(target=w.run, daemon=True)
        t.start()
        self.threads.append(t)
        self.workers.append(w)
        return w

    def fake_worker(self, worker_id, **kw):
        fw = FakeWorker(worker_id, self.worker_port, **kw)
        self.workers.append(fw)
        return fw

    def wait_workers(self, n, timeout=5.0):
        wait_until(lambda: len(self.mgr.workers) >= n, timeout, msg="%d workers" % n)

    def client(self):
        return connect("127.0.0.1", self.client_port)

    def submit(self, specs):
        ch = self.client()
        try:
            ch.send({"type": SUBMIT, "tasks": specs})
            return ch.recv()
        finally:
            ch.close()

    def task(self, tid):
        with self.mgr.lock:
            return self.mgr.tasks[tid].to_dict()

    def wait_task(self, tid, states=("COMPLETED", "FAILED"), timeout=10.0):
        wait_until(lambda: self.task(tid)["state"] in states, timeout, msg="task %s" % tid)
        return self.task(tid)

    def wait_all_terminal(self, timeout=15.0):
        def done():
            with self.mgr.lock:
                return all(t.terminal for t in self.mgr.tasks.values())
        wait_until(done, timeout, msg="all tasks terminal")

    def close(self):
        for w in self.workers:
            try:
                w.stop()
            except Exception:
                pass
        self.mgr.stop()


class FakeWorker:
    """A worker we control by hand. Registers, then calls on_task(msg, self) for
    every TASK. heartbeat=True keeps sending heartbeats."""

    def __init__(self, worker_id, port, on_task=None, heartbeat=True, hb_interval=0.2,
                 register=True):
        self.worker_id = worker_id
        self.tasks = []
        self.on_task = on_task
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.ch = MessageChannel(self.sock)
        self.alive = True
        self.replies = []
        if register:
            self.ch.send({"type": REGISTER, "worker_id": worker_id, "slowdown": 1.0})
            self.ack = self.ch.recv()
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()
        if heartbeat:
            threading.Thread(target=self._hb, args=(hb_interval,), daemon=True).start()

    def _hb(self, interval):
        while self.alive:
            time.sleep(interval)
            try:
                self.ch.send({"type": HEARTBEAT, "worker_id": self.worker_id})
            except OSError:
                return

    def _read(self):
        try:
            while self.alive:
                msg = self.ch.recv()
                if msg is None:
                    return
                if msg["type"] == "TASK":
                    self.tasks.append(msg)
                    if self.on_task:
                        self.on_task(msg, self)
                else:
                    self.replies.append(msg)
        except Exception:
            return

    def good_result(self, msg, **override):
        out = tm.compute(msg["task_type"], msg["n"])
        res = {"type": RESULT, "task_id": msg["task_id"], "attempt": msg.get("attempt"),
               "worker_id": self.worker_id, "status": "OK", "output": out,
               "payload_crc": tm.payload_crc(msg.get("payload", "")), "elapsed_ms": 1.0}
        res.update(override)
        return res

    def reply(self, msg, **override):
        self.ch.send(self.good_result(msg, **override))

    def crash(self):
        """Abruptly close the connection (like a killed process)."""
        self.alive = False
        self.ch.close()

    def stop(self):
        self.alive = False
        self.ch.close()


class FakeController:
    """Listens on a Unix socket like the Ryu control channel; records messages."""

    def __init__(self, path=None, reply_type="OK"):
        self.dir = tempfile.mkdtemp(prefix="jf-ctl-")
        self.path = path or os.path.join(self.dir, "ctl.sock")
        self.received = []
        self.reply_type = reply_type
        self.srv = None
        self._stop = False

    def start(self):
        self.srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.srv.bind(self.path)
        self.srv.listen(8)
        self.srv.settimeout(0.2)
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while not self._stop:
            try:
                c, _ = self.srv.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            threading.Thread(target=self._serve, args=(c,), daemon=True).start()

    def _serve(self, c):
        buf = b""
        try:
            while True:
                chunk = c.recv(65536)
                if not chunk:
                    return
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    msg = json.loads(line.decode())
                    self.received.append(msg)
                    c.sendall(json.dumps({"type": self.reply_type}).encode() + b"\n")
        except OSError:
            return
        finally:
            c.close()

    def stop(self):
        self._stop = True
        if self.srv:
            self.srv.close()
        try:
            os.unlink(self.path)
        except OSError:
            pass

    def types(self):
        return [m["type"] for m in self.received]
