"""
Application -> SDN controller notifications (the "northbound" integration).

The task manager tells the Ryu controller about application events so that the
controller can change forwarding:

    WORKER_UP    {ip}                 a worker registered   -> controller installs
                                      a steerable "application flow" manager<->worker
    WORKER_DOWN  {ip, quarantine_s}   a worker failed       -> controller removes
                                      those flows (and optionally drops the
                                      worker's traffic for quarantine_s seconds)

Transport: newline-delimited JSON over a Unix domain socket (default
/tmp/jackfruit-ctl.sock). This is deliberate: Mininet hosts live in their own
network namespaces and cannot reach the controller's 127.0.0.1, but they DO share
the filesystem, so a Unix socket path is reachable from h1. It is an out-of-band
management channel, as in a real SDN deployment (the data network is not used).

notify() never blocks the caller: events go on a queue and a background thread
delivers them. If the controller is down, the thread keeps retrying and, after
reconnecting, re-sends the current state (WORKER_UP for every live worker) so the
controller never ends up out of sync.
"""
import json
import logging
import queue
import socket
import threading
import time

log = logging.getLogger("ctlclient")

DEFAULT_SOCKET = "/tmp/jackfruit-ctl.sock"


def request(path, msg, timeout=3.0):
    """One-shot request/response to the controller. Returns the reply dict.
    Raises OSError if the controller cannot be reached."""
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try:
        s.connect(path)
        s.sendall(json.dumps(msg, separators=(",", ":")).encode() + b"\n")
        buf = b""
        while b"\n" not in buf:
            chunk = s.recv(65536)
            if not chunk:
                break
            buf += chunk
        if not buf.strip():
            raise OSError("controller closed the connection without replying")
        return json.loads(buf.split(b"\n", 1)[0].decode())
    finally:
        s.close()


class NullController:
    """Used when no SDN controller is configured (e.g. plain unit tests)."""
    enabled = False

    def notify(self, msg_type, **fields):
        pass

    def start(self):
        pass

    def stop(self):
        pass

    def drain(self, timeout=0):
        return True


class ControllerClient:
    enabled = True

    def __init__(self, path=DEFAULT_SOCKET, resync=None, retry_s=1.0, max_queue=1000):
        self.path = path
        self.resync = resync          # callable -> list of (type, fields) to re-send
        self.retry_s = retry_s
        self._q = queue.Queue(maxsize=max_queue)
        self._stop = threading.Event()
        self._thread = None
        self.sent = []                # (time, msg, reply) log, for metrics
        self._sent_lock = threading.Lock()
        self.connected = False
        self._busy = False

    def start(self):
        self._thread = threading.Thread(target=self._run, name="ctl-notify", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._q.put(None)
        if self._thread:
            self._thread.join(timeout=2)

    def notify(self, msg_type, **fields):
        msg = dict(fields, type=msg_type)
        try:
            self._q.put_nowait(msg)
        except queue.Full:
            log.warning("controller notification queue full; dropping %s", msg_type)

    def drain(self, timeout=3.0):
        """Wait until queued notifications have been delivered (used by tests)."""
        end = time.time() + timeout
        while time.time() < end:
            if self._q.empty() and not getattr(self, "_busy", False):
                return True
            time.sleep(0.01)
        return False

    def log_snapshot(self):
        with self._sent_lock:
            return list(self.sent)

    # ---- background delivery --------------------------------------------------
    def _connect(self):
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.settimeout(3.0)
        s.connect(self.path)
        return s

    def _roundtrip(self, sock, msg):
        sock.sendall(json.dumps(msg, separators=(",", ":")).encode() + b"\n")
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(65536)
            if not chunk:
                raise OSError("controller closed connection")
            buf += chunk
        return json.loads(buf.split(b"\n", 1)[0].decode())

    def _run(self):
        sock = None
        pending = None
        while not self._stop.is_set():
            try:
                if sock is None:
                    sock = self._connect()
                    self.connected = True
                    log.info("connected to controller at %s", self.path)
                    if self.resync:
                        for mtype, fields in self.resync():
                            self._roundtrip(sock, dict(fields, type=mtype))
                if pending is None:
                    self._busy = False
                    pending = self._q.get()
                    if pending is None:
                        break
                self._busy = True
                reply = self._roundtrip(sock, pending)
                with self._sent_lock:
                    self.sent.append((time.time(), pending, reply))
                if reply.get("type") == "ERROR":
                    log.warning("controller rejected %s: %s", pending.get("type"),
                                reply.get("reason"))
                pending = None
            except (OSError, ValueError) as e:
                if self.connected:
                    log.warning("controller link lost: %s", e)
                self.connected = False
                if sock is not None:
                    try:
                        sock.close()
                    except OSError:
                        pass
                    sock = None
                self._stop.wait(self.retry_s)
        self._busy = False
        if sock is not None:
            sock.close()
