"""
Task manager (runs on h1).

Two TCP listeners:
  worker port (default 9100)  workers connect, REGISTER, receive TASKs, return RESULTs
  client port (default 9200)  clients SUBMIT tasks and ask for STATUS / METRICS

Threads
  * one accept thread per listener
  * one reader thread per connection (blocking recv, so concurrency is simple)
  * one writer thread per worker (so a slow or stalled link to one worker can never
    block dispatching to the others)
  * one dispatcher thread: every <=100 ms it detects dead workers (no heartbeat),
    detects task deadlines, and assigns pending tasks using the scheduler

All shared state lives behind one RLock (self.lock). Nothing in the locked
sections blocks on the network: sends are queued to the writer threads.

Failure handling
  * worker connection closes / errors / heartbeat silent for hb_timeout
        -> worker marked dead, its unfinished tasks are requeued (or FAILED if
           they are not retry-safe or out of attempts) and the SDN controller is
           told (WORKER_DOWN)
  * task not completed within its deadline
        -> attempt abandoned, task requeued (excluding that worker)
  * a late result for an abandoned attempt, or a second result for a task
        -> accepted if the task is still open, otherwise counted as a duplicate

Run from the project root, e.g. on h1:
    python3 -u -m app.task_manager --policy least_loaded
"""
import argparse
import collections
import logging
import queue
import re
import signal
import socket
import threading
import time
import json

from app import taskmodel as tm
from app.controller_client import (DEFAULT_SOCKET, ControllerClient, NullController)
from app.protocol import (ERROR, HEARTBEAT, HEARTBEAT_ACK, METRICS, METRICS_REQ,
                          REGISTER, REGISTER_ACK, RESULT, SHUTDOWN, STATUS, STATUS_REQ,
                          SUBMIT, SUBMIT_ACK, TASK, MessageChannel, ProtocolError,
                          require)
from app.scheduler import LEAST_LOADED, POLICIES, make_scheduler

log = logging.getLogger("manager")

WORKER_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,32}$")
MAX_BATCH = 5000
MAX_EVENTS = 20000


class TaskManager:
    def __init__(self, policy=LEAST_LOADED, cap=2, hb_interval=1.0, hb_timeout=3.0,
                 max_attempts_cap=None, controller=None, quarantine_s=0.0,
                 clock=time.time):
        self.policy_name = policy
        self.cap = cap
        self.scheduler = make_scheduler(policy, cap)
        self.hb_interval = hb_interval
        self.hb_timeout = hb_timeout
        self.max_attempts_cap = max_attempts_cap   # None = use each task's own limit
        self.quarantine_s = quarantine_s
        self.clock = clock

        self.lock = threading.RLock()
        self.cv = threading.Condition(self.lock)
        self.tasks = {}
        self.pending = collections.deque()
        self.workers = {}            # alive workers by id
        self.worker_history = []     # every WorkerInfo ever registered
        self.events = []
        self.counters = collections.Counter()
        self._auto_id = 0

        self._stop = threading.Event()
        self._threads = []
        self._listeners = []
        self.worker_port = None
        self.client_port = None
        self.started_at = None

        if controller is None:
            controller = NullController()
        self.ctl = controller
        if isinstance(self.ctl, ControllerClient) and self.ctl.resync is None:
            self.ctl.resync = self._ctl_resync

    # ------------------------------------------------------------------ lifecycle
    def start(self, host="0.0.0.0", worker_port=9100, client_port=9200):
        self.started_at = self.clock()
        self.worker_port = self._listen(host, worker_port, self._handle_worker, "workers")
        self.client_port = self._listen(host, client_port, self._handle_client, "clients")
        self._spawn(self._dispatcher, "dispatcher")
        self.ctl.start()
        log.info("task manager up: policy=%s cap=%s hb_timeout=%.1fs workers:%d clients:%d",
                 self.policy_name, self.cap if self.policy_name == LEAST_LOADED else "-",
                 self.hb_timeout, self.worker_port, self.client_port)
        return self.worker_port, self.client_port

    def _spawn(self, target, name, *args):
        t = threading.Thread(target=target, name=name, args=args, daemon=True)
        t.start()
        self._threads.append(t)
        return t

    def _listen(self, host, port, handler, label):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((host, port))
        srv.listen(64)
        srv.settimeout(0.3)
        self._listeners.append(srv)
        self._spawn(self._accept_loop, "accept-" + label, srv, handler)
        return srv.getsockname()[1]

    def _accept_loop(self, srv, handler):
        while not self._stop.is_set():
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._spawn(handler, "conn-%s:%d" % addr, conn, addr)

    def stop(self, metrics_file=None):
        """Graceful shutdown: tell workers to exit, close listeners, dump metrics."""
        if self._stop.is_set():
            return
        with self.lock:
            self._event("manager_stopping")
            self._stop.set()
            for w in list(self.workers.values()):
                w.out_queue.put({"type": SHUTDOWN})
                w.out_queue.put(None)        # writer closes the channel after SHUTDOWN
            self.cv.notify_all()
        for srv in self._listeners:
            try:
                srv.close()
            except OSError:
                pass
        time.sleep(0.2)   # let writers flush SHUTDOWN
        if metrics_file:
            try:
                with open(metrics_file, "w") as f:
                    json.dump(self.metrics(), f, indent=1)
                log.info("metrics written to %s", metrics_file)
            except OSError as e:
                log.error("could not write metrics file: %s", e)
        self.ctl.stop()

    # ------------------------------------------------------------------ helpers
    def _event(self, kind, **detail):
        ev = dict(detail, t=self.clock(), kind=kind)
        if len(self.events) < MAX_EVENTS:
            self.events.append(ev)

    def _ctl_resync(self):
        with self.lock:
            return [("WORKER_UP", {"ip": w.addr[0], "worker": w.worker_id})
                    for w in self.workers.values() if w.alive]

    def _effective_max_attempts(self, task):
        m = task.spec["max_attempts"]
        return min(m, self.max_attempts_cap) if self.max_attempts_cap else m

    # ------------------------------------------------------------------ workers
    def _handle_worker(self, conn, addr):
        ch = MessageChannel(conn)
        w = None
        reason = "connection closed"
        try:
            conn.settimeout(10)   # a new connection must register quickly
            msg = ch.recv()
            if msg is None:
                ch.close()
                return
            if msg["type"] != REGISTER:
                ch.send({"type": ERROR, "reason": "first message must be REGISTER"})
                ch.close()
                return
            require(msg, "worker_id")
            wid = str(msg["worker_id"])
            if not WORKER_ID_RE.match(wid):
                ch.send({"type": ERROR, "reason": "invalid worker_id"})
                ch.close()
                return
            slowdown = msg.get("slowdown", 1.0)
            if isinstance(slowdown, bool) or not isinstance(slowdown, (int, float)):
                slowdown = 1.0
            rejected = False
            with self.lock:
                now = self.clock()
                old = self.workers.get(wid)
                if old is not None and now - old.last_seen <= max(2 * self.hb_interval, 2.0):
                    # A live worker already uses this id: refuse the newcomer.
                    rejected = True
                    self.counters["duplicate_registrations"] += 1
                    self._event("registration_rejected", worker=wid, addr=addr[0])
                else:
                    if old is not None:   # stale (silent) old connection: newcomer wins
                        self._fail_worker_locked(old, "replaced by a new connection")
                    w = tm.WorkerInfo(wid, addr, float(slowdown), now)
                    w.channel = ch
                    w.out_queue = queue.Queue()
                    self.workers[wid] = w
                    self.worker_history.append(w)
                    self._event("worker_registered", worker=wid, addr=addr[0])
                    w.out_queue.put({"type": REGISTER_ACK, "worker_id": wid,
                                     "heartbeat_interval": self.hb_interval})
                    self.cv.notify_all()
            if rejected:
                log.warning("rejected duplicate registration for %s", wid)
                ch.send({"type": ERROR, "reason": "worker_id already registered"})
                ch.close()
                return
            self._spawn(self._writer, "writer-" + wid, w)
            self.ctl.notify("WORKER_UP", ip=addr[0], worker=wid)
            log.info("registered worker %s from %s:%d (%d online)", wid, addr[0], addr[1],
                     len(self.workers))
            conn.settimeout(None)
            while True:
                msg = ch.recv()
                if msg is None:
                    reason = "connection closed"
                    break
                self._on_worker_message(w, msg)
        except ProtocolError as e:
            reason = "protocol error: %s" % e
            log.warning("worker %s: %s", w.worker_id if w else addr, reason)
            if w is None:   # never registered: tell the peer why, then drop it
                try:
                    ch.send({"type": ERROR, "reason": str(e)})
                except OSError:
                    pass
        except OSError as e:
            reason = "socket error: %s" % e
        finally:
            if w is not None:
                with self.lock:
                    self._fail_worker_locked(w, reason)
            else:
                ch.close()

    def _writer(self, w):
        while True:
            m = w.out_queue.get()
            if m is None:
                w.channel.close()
                return
            try:
                w.channel.send(m)
            except OSError as e:
                with self.lock:
                    self._fail_worker_locked(w, "send failed: %s" % e)
                return

    def _on_worker_message(self, w, msg):
        with self.lock:
            w.last_seen = self.clock()
        mtype = msg["type"]
        if mtype == HEARTBEAT:
            w.out_queue.put({"type": HEARTBEAT_ACK})
        elif mtype == RESULT:
            self._on_result(w, msg)
        else:
            self.counters["unexpected_messages"] += 1
            w.out_queue.put({"type": ERROR, "reason": "unexpected %s from worker" % mtype})

    def _fail_worker_locked(self, w, reason, quarantine=False):
        """Mark a worker dead and requeue its work. Safe to call repeatedly."""
        if not w.alive:
            return
        now = self.clock()
        w.alive = False
        w.failed_at = now
        w.fail_reason = reason
        if self.workers.get(w.worker_id) is w:
            del self.workers[w.worker_id]
        try:
            w.out_queue.put(None)
        except Exception:
            pass
        w.channel.close()
        if self._stop.is_set():
            return
        self.counters["workers_failed"] += 1
        lost = list(w.assigned)
        w.assigned.clear()
        self._event("worker_failed", worker=w.worker_id, reason=reason, tasks_in_flight=len(lost))
        log.warning("worker %s FAILED (%s); %d task(s) in flight", w.worker_id, reason, len(lost))
        for tid in lost:
            t = self.tasks[tid]
            if t.state == tm.ASSIGNED and t.worker == w.worker_id:
                self._requeue_or_fail_locked(t, w.worker_id, "worker failed: %s" % reason)
        q = self.quarantine_s if quarantine else 0.0
        self.ctl.notify("WORKER_DOWN", ip=w.addr[0], worker=w.worker_id, quarantine_s=q)
        self.cv.notify_all()

    # ------------------------------------------------------------------ task flow
    def _requeue_or_fail_locked(self, t, worker_id, reason):
        now = self.clock()
        t.excluded.add(worker_id)
        t.reason = reason
        if not t.spec["idempotent"]:
            self._fail_task_locked(t, "not retry-safe (%s)" % reason)
        elif t.attempts >= self._effective_max_attempts(t):
            self._fail_task_locked(t, "max attempts exceeded (%s)" % reason)
        else:
            t.transition(tm.PENDING, now, reason)
            t.worker = None
            t.requeues += 1
            self.counters["requeues"] += 1
            self.pending.appendleft(t.task_id)
            self._event("task_requeued", task=t.task_id, reason=reason, attempts=t.attempts)
            log.info("task %s requeued (%s)", t.task_id, reason)

    def _fail_task_locked(self, t, reason):
        t.transition(tm.FAILED, self.clock(), reason)
        t.reason = reason
        self.counters["failed"] += 1
        self._event("task_failed", task=t.task_id, reason=reason)
        log.warning("task %s FAILED: %s", t.task_id, reason)

    def _on_result(self, w, msg):
        try:
            require(msg, "task_id")
        except ProtocolError as e:
            w.out_queue.put({"type": ERROR, "reason": str(e)})
            return
        tid = msg["task_id"]
        with self.lock:
            now = self.clock()
            t = self.tasks.get(tid)
            was_current = w.assigned.pop(tid, None) is not None
            if t is None:
                self.counters["unknown_results"] += 1
                log.warning("result for unknown task %r from %s", tid, w.worker_id)
                return
            if t.terminal:
                self.counters["duplicate_results"] += 1
                self._event("duplicate_result", task=tid, worker=w.worker_id)
                log.info("duplicate result for %s from %s ignored", tid, w.worker_id)
                self.cv.notify_all()
                return
            ok = msg.get("status") == "OK"
            if ok:
                if t.expected_output is not None and msg.get("output") != t.expected_output:
                    ok = False
                    reason = "wrong output"
                elif msg.get("payload_crc") != t.expected_crc:
                    ok = False
                    reason = "payload checksum mismatch"
                else:
                    reason = ""
            else:
                reason = "worker error: %s" % msg.get("reason", "unknown")
            if ok:
                self._complete_locked(t, w, msg, now, was_current)
            else:
                self.counters["bad_results"] += 1
                log.warning("bad result for %s from %s: %s", tid, w.worker_id, reason)
                if t.state == tm.ASSIGNED and t.worker == w.worker_id:
                    self._requeue_or_fail_locked(t, w.worker_id, reason)
            self.cv.notify_all()

    def _complete_locked(self, t, w, msg, now, was_current):
        late = not (t.state == tm.ASSIGNED and t.worker == w.worker_id)
        t.transition(tm.COMPLETED, now, "by %s%s" % (w.worker_id, " (late)" if late else ""))
        elapsed = msg.get("elapsed_ms")
        t.worker = w.worker_id
        t.completed_at = now
        t.result = msg.get("output")
        t.elapsed_ms = elapsed if isinstance(elapsed, (int, float)) else None
        w.completed += 1
        if t.elapsed_ms:
            w.busy_ms += t.elapsed_ms
        if was_current and t.assigned_at is not None:
            w.observe((now - t.assigned_at) * 1000.0 / (t.assign_load + 1))
        self.counters["completed"] += 1
        if late:
            self.counters["late_results_accepted"] += 1
            self._event("late_result_accepted", task=t.task_id, worker=w.worker_id)

    def _tick_locked(self):
        now = self.clock()
        # 1. silent workers
        for w in list(self.workers.values()):
            if now - w.last_seen > self.hb_timeout:
                self._fail_worker_locked(w, "heartbeat timeout (%.1fs silent)" % (now - w.last_seen),
                                         quarantine=True)
        # 2. task deadlines
        for w in list(self.workers.values()):
            for tid, since in list(w.assigned.items()):
                t = self.tasks[tid]
                if t.terminal:   # finished elsewhere (late result): nothing to time out
                    del w.assigned[tid]
                    continue
                if now - since > t.spec["deadline_s"]:
                    del w.assigned[tid]
                    w.timeouts += 1
                    self.counters["task_timeouts"] += 1
                    self._event("task_timeout", task=tid, worker=w.worker_id)
                    log.warning("task %s timed out on %s (deadline %.1fs)", tid, w.worker_id,
                                t.spec["deadline_s"])
                    if t.state == tm.ASSIGNED and t.worker == w.worker_id:
                        self._requeue_or_fail_locked(t, w.worker_id, "deadline exceeded")
        # 3. assignments
        if not self.pending:
            return
        alive = list(self.workers.values())
        if not alive:
            return
        still = collections.deque()
        while self.pending:
            tid = self.pending.popleft()
            t = self.tasks[tid]
            if t.state != tm.PENDING:
                continue
            w = self.scheduler.pick(t, alive)
            if w is None:
                still.append(tid)
                continue
            t.transition(tm.ASSIGNED, now, w.worker_id)
            t.attempts += 1
            t.worker = w.worker_id
            t.assigned_at = now
            t.assign_load = w.inflight
            if t.first_assigned_at is None:
                t.first_assigned_at = now
            w.assigned[tid] = now
            w.out_queue.put({
                "type": TASK, "task_id": tid, "attempt": t.attempts,
                "task_type": t.spec["task_type"], "n": t.spec["n"],
                "work_ms": t.spec["work_ms"], "payload_bytes": t.spec["payload_bytes"],
                "payload": tm.make_payload(tid, t.spec["payload_bytes"]),
            })
            self._event("task_assigned", task=tid, worker=w.worker_id, attempt=t.attempts)
        self.pending = still

    def _dispatcher(self):
        while not self._stop.is_set():
            with self.cv:
                self.cv.wait(timeout=0.1)
                if self._stop.is_set():
                    break
                try:
                    self._tick_locked()
                except Exception:   # keep the manager alive no matter what
                    log.exception("dispatcher tick failed")

    # ------------------------------------------------------------------ clients
    def _handle_client(self, conn, addr):
        ch = MessageChannel(conn)
        try:
            while not self._stop.is_set():
                msg = ch.recv()
                if msg is None:
                    break
                mtype = msg["type"]
                if mtype == SUBMIT:
                    ch.send(self._submit(msg))
                elif mtype == STATUS_REQ:
                    ch.send(self._status(msg))
                elif mtype == METRICS_REQ:
                    ch.send(dict(self.metrics(), type=METRICS))
                else:
                    ch.send({"type": ERROR, "reason": "unexpected %s from client" % mtype})
        except ProtocolError as e:
            try:
                ch.send({"type": ERROR, "reason": str(e)})
            except OSError:
                pass
        except OSError:
            pass
        finally:
            ch.close()

    def _submit(self, msg):
        specs = msg.get("tasks")
        if not isinstance(specs, list) or not specs:
            return {"type": ERROR, "reason": "SUBMIT needs a non-empty 'tasks' list"}
        if len(specs) > MAX_BATCH:
            return {"type": ERROR, "reason": "too many tasks in one SUBMIT (max %d)" % MAX_BATCH}
        accepted, duplicates, rejected = [], [], []
        with self.lock:
            now = self.clock()
            for i, raw in enumerate(specs):
                try:
                    spec = tm.normalise_spec(raw)
                except tm.InvalidSpec as e:
                    rejected.append({"index": i, "reason": str(e)})
                    self.counters["rejected_submissions"] += 1
                    continue
                tid = spec["task_id"]
                if tid is None:
                    self._auto_id += 1
                    tid = "t%06d" % self._auto_id
                    while tid in self.tasks:
                        self._auto_id += 1
                        tid = "t%06d" % self._auto_id
                existing = self.tasks.get(tid)
                if existing is not None:
                    if all(existing.spec[k] == spec[k] for k in tm.SPEC_KEYS):
                        duplicates.append(tid)
                        self.counters["duplicate_submits"] += 1
                    else:
                        rejected.append({"index": i, "task_id": tid,
                                         "reason": "task_id already used with a different spec"})
                        self.counters["rejected_submissions"] += 1
                    continue
                self.tasks[tid] = tm.Task(tid, spec, now)
                self.pending.append(tid)
                self.counters["submitted"] += 1
                accepted.append(tid)
            if accepted:
                self._event("tasks_submitted", count=len(accepted))
                self.cv.notify_all()
        log.info("SUBMIT: %d accepted, %d duplicate, %d rejected",
                 len(accepted), len(duplicates), len(rejected))
        return {"type": SUBMIT_ACK, "accepted": accepted, "duplicates": duplicates,
                "rejected": rejected}

    def _summary_locked(self):
        by_state = collections.Counter(t.state for t in self.tasks.values())
        return {"tasks": len(self.tasks),
                "pending": by_state[tm.PENDING], "assigned": by_state[tm.ASSIGNED],
                "completed": by_state[tm.COMPLETED], "failed": by_state[tm.FAILED],
                "workers_alive": len(self.workers),
                "all_done": bool(self.tasks) and by_state[tm.PENDING] == 0
                and by_state[tm.ASSIGNED] == 0}

    def _status(self, msg):
        ids = msg.get("task_ids")
        with self.lock:
            if ids is None:
                tasks = [t.to_dict() for t in self.tasks.values()]
            elif isinstance(ids, list):
                tasks = [self.tasks[i].to_dict() if i in self.tasks
                         else {"task_id": i, "state": "UNKNOWN"} for i in ids]
            else:
                return {"type": ERROR, "reason": "task_ids must be a list"}
            workers = ([w.to_dict() for w in self.worker_history if w.alive]
                       if msg.get("workers", True) else [])
            return {"type": STATUS, "tasks": tasks, "workers": workers,
                    "summary": self._summary_locked()}

    # ------------------------------------------------------------------ metrics
    def metrics(self):
        with self.lock:
            return {
                "policy": self.policy_name, "cap": self.cap,
                "hb_timeout": self.hb_timeout, "max_attempts_cap": self.max_attempts_cap,
                "started_at": self.started_at, "now": self.clock(),
                "counters": dict(self.counters),
                "summary": self._summary_locked(),
                "tasks": [t.to_dict() for t in self.tasks.values()],
                "workers": [w.to_dict() for w in self.worker_history],
                "events": list(self.events),
                "controller": {
                    "enabled": self.ctl.enabled,
                    "notifications": [
                        {"t": ts, "msg": m, "reply": r}
                        for ts, m, r in getattr(self.ctl, "log_snapshot", lambda: [])()],
                },
            }


def main():
    p = argparse.ArgumentParser(description="Task manager")
    p.add_argument("--host", default="0.0.0.0")
    p.add_argument("--worker-port", type=int, default=9100)
    p.add_argument("--client-port", type=int, default=9200)
    p.add_argument("--policy", choices=POLICIES, default=LEAST_LOADED)
    p.add_argument("--cap", type=int, default=2,
                   help="least_loaded: max unfinished tasks per worker")
    p.add_argument("--hb-interval", type=float, default=1.0)
    p.add_argument("--hb-timeout", type=float, default=3.0)
    p.add_argument("--max-attempts", type=int, default=None,
                   help="upper limit on attempts per task (1 = never redistribute)")
    p.add_argument("--controller", default=DEFAULT_SOCKET,
                   help="Unix socket of the SDN controller, or 'none'")
    p.add_argument("--quarantine", type=float, default=0.0,
                   help="seconds to drop traffic of a worker that went silent (0 = off)")
    p.add_argument("--metrics-file", default=None, help="write full metrics JSON on shutdown")
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
    done = threading.Event()

    def on_signal(signum, _frame):
        log.info("signal %d received, shutting down", signum)
        done.set()
    signal.signal(signal.SIGINT, on_signal)
    signal.signal(signal.SIGTERM, on_signal)

    mgr.start(args.host, args.worker_port, args.client_port)
    while not done.wait(0.5):
        pass
    mgr.stop(args.metrics_file)


if __name__ == "__main__":
    main()
