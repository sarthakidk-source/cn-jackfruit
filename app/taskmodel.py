"""
Task model: task types, explicit task states with a checked transition table,
and the per-worker record used by the task manager.

This module has no sockets and no threads, so it is easy to unit-test.

Task types (all are pure functions of their inputs, so retrying them is safe)
------------------------------------------------------------------------------
sumsq  : sum of i*i for i in range(n)          (manager can verify the answer)
primes : number of primes below n (sieve)      (CPU heavier, not verified)

Every task may also carry:
  work_ms        simulated extra processing time (the worker sleeps)
  payload_bytes  size of an opaque data block sent with the task, so that task
                 dispatch really loads the network (used by the congestion
                 experiment). The worker returns a CRC32 of what it received.
  idempotent     True  -> safe to retry on another worker (default)
                 False -> never retried after a failure; goes to FAILED instead
"""
import zlib

# ---- task states ------------------------------------------------------------
PENDING = "PENDING"        # waiting for a worker
ASSIGNED = "ASSIGNED"      # sent to a worker, result not yet received
COMPLETED = "COMPLETED"    # valid result accepted (terminal)
FAILED = "FAILED"          # gave up (terminal)

TERMINAL = frozenset((COMPLETED, FAILED))

# Allowed transitions. ASSIGNED -> PENDING is the "requeue" used for redistribution.
# PENDING -> COMPLETED covers a late but valid result that arrives from the
# original worker after the task was requeued and before it was reassigned.
TRANSITIONS = {
    PENDING: frozenset((ASSIGNED, COMPLETED, FAILED)),
    ASSIGNED: frozenset((PENDING, COMPLETED, FAILED)),
    COMPLETED: frozenset(),
    FAILED: frozenset(),
}

TASK_TYPES = ("sumsq", "primes")

# Limits applied to client submissions (protects the manager and workers)
MAX_N = 5000000
MAX_PAYLOAD_BYTES = 2 << 20
MAX_WORK_MS = 60000
DEFAULT_DEADLINE_S = 10.0
DEFAULT_MAX_ATTEMPTS = 3


class InvalidTransition(Exception):
    pass


class InvalidSpec(ValueError):
    pass


# ---- task computation (shared by worker and manager) -------------------------
def make_payload(task_id, size):
    """Deterministic payload of exactly `size` bytes (ASCII)."""
    if size <= 0:
        return ""
    base = "%s|" % task_id
    return (base * (size // len(base) + 1))[:size]


def payload_crc(payload):
    return zlib.crc32(payload.encode("ascii")) & 0xffffffff


def compute(task_type, n):
    """Run the actual computation. Raises ValueError for an unknown type."""
    if task_type == "sumsq":
        return sum(i * i for i in range(n))
    if task_type == "primes":
        if n < 3:
            return 0
        sieve = bytearray([1]) * n
        sieve[0:2] = b"\x00\x00"
        for i in range(2, int(n ** 0.5) + 1):
            if sieve[i]:
                sieve[i * i::i] = bytearray(len(range(i * i, n, i)))
        return sum(sieve)
    raise ValueError("unknown task type %r" % (task_type,))


def expected_output(task_type, n):
    """Cheap independent check of a result, or None if not cheaply verifiable."""
    if task_type == "sumsq":
        return (n - 1) * n * (2 * n - 1) // 6 if n > 0 else 0
    return None


# ---- spec validation ---------------------------------------------------------
def _num(spec, key, default, lo, hi, integer=True):
    val = spec.get(key, default)
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        raise InvalidSpec("%s must be a number" % key)
    if integer and int(val) != val:
        raise InvalidSpec("%s must be an integer" % key)
    if not lo <= val <= hi:
        raise InvalidSpec("%s must be between %s and %s" % (key, lo, hi))
    return int(val) if integer else float(val)


def normalise_spec(spec):
    """Validate a client-supplied task spec and return a clean dict.
    Raises InvalidSpec with a human-readable reason."""
    if not isinstance(spec, dict):
        raise InvalidSpec("task spec must be an object")
    task_type = spec.get("task_type", "sumsq")
    if task_type not in TASK_TYPES:
        raise InvalidSpec("unknown task_type %r (allowed: %s)"
                          % (task_type, ", ".join(TASK_TYPES)))
    task_id = spec.get("task_id")
    if task_id is not None and (not isinstance(task_id, str) or not task_id
                                or len(task_id) > 64):
        raise InvalidSpec("task_id must be a non-empty string (max 64 chars)")
    idem = spec.get("idempotent", True)
    if not isinstance(idem, bool):
        raise InvalidSpec("idempotent must be true or false")
    return {
        "task_id": task_id,
        "task_type": task_type,
        "n": _num(spec, "n", 1000, 0, MAX_N),
        "work_ms": _num(spec, "work_ms", 0, 0, MAX_WORK_MS, integer=False),
        "payload_bytes": _num(spec, "payload_bytes", 0, 0, MAX_PAYLOAD_BYTES),
        "idempotent": idem,
        "deadline_s": _num(spec, "deadline_s", DEFAULT_DEADLINE_S, 0.1, 3600,
                           integer=False),
        "max_attempts": _num(spec, "max_attempts", DEFAULT_MAX_ATTEMPTS, 1, 10),
    }


SPEC_KEYS = ("task_type", "n", "work_ms", "payload_bytes", "idempotent",
             "deadline_s", "max_attempts")


# ---- Task ----------------------------------------------------------------------
class Task:
    def __init__(self, task_id, spec, now):
        self.task_id = task_id
        self.spec = {k: spec[k] for k in SPEC_KEYS}
        self.state = PENDING
        self.worker = None           # worker currently (or finally) responsible
        self.attempts = 0            # how many times it has been assigned
        self.requeues = 0
        self.excluded = set()        # workers that failed/timed out on this task
        self.submitted_at = now
        self.first_assigned_at = None
        self.assigned_at = None      # time of the *current* assignment
        self.assign_load = 0         # worker's inflight count when assigned
        self.completed_at = None
        self.result = None
        self.elapsed_ms = None
        self.reason = None           # why it failed / was last requeued
        self.history = [(now, PENDING, "submitted")]
        self.expected_crc = payload_crc(make_payload(task_id, spec["payload_bytes"]))
        self.expected_output = expected_output(spec["task_type"], spec["n"])

    @property
    def terminal(self):
        return self.state in TERMINAL

    def transition(self, new_state, now, note=""):
        if new_state not in TRANSITIONS[self.state]:
            raise InvalidTransition("%s: %s -> %s not allowed"
                                    % (self.task_id, self.state, new_state))
        self.state = new_state
        self.history.append((now, new_state, note))

    def to_dict(self, with_history=False):
        d = {
            "task_id": self.task_id, "state": self.state, "worker": self.worker,
            "attempts": self.attempts, "requeues": self.requeues,
            "task_type": self.spec["task_type"], "n": self.spec["n"],
            "payload_bytes": self.spec["payload_bytes"],
            "submitted_at": self.submitted_at,
            "first_assigned_at": self.first_assigned_at,
            "completed_at": self.completed_at,
            "latency_ms": (None if self.completed_at is None else
                           round((self.completed_at - self.submitted_at) * 1000, 1)),
            "elapsed_ms": self.elapsed_ms, "result": self.result,
            "reason": self.reason,
        }
        if with_history:
            d["history"] = list(self.history)
        return d


# ---- Worker record (manager side) ------------------------------------------------
class WorkerInfo:
    """Manager's view of one registered worker. Mutated only under the manager lock."""

    def __init__(self, worker_id, addr, slowdown, now):
        self.worker_id = worker_id
        self.addr = addr
        self.slowdown = slowdown
        self.registered_at = now
        self.last_seen = now
        self.alive = True
        self.assigned = {}       # task_id -> time assigned (current assignments)
        self.completed = 0
        self.busy_ms = 0.0       # sum of worker-reported processing time
        self.timeouts = 0
        self.est_ms = None       # EWMA of per-task time, learned from results
        self.failed_at = None
        self.fail_reason = None
        # transport objects, set by the manager
        self.channel = None
        self.out_queue = None

    @property
    def inflight(self):
        return len(self.assigned)

    def observe(self, sample_ms, alpha=0.3):
        self.est_ms = sample_ms if self.est_ms is None else \
            (1 - alpha) * self.est_ms + alpha * sample_ms

    def to_dict(self):
        return {
            "worker_id": self.worker_id, "addr": self.addr[0] if self.addr else None,
            "alive": self.alive, "inflight": self.inflight,
            "completed": self.completed, "busy_ms": round(self.busy_ms, 1),
            "est_ms": None if self.est_ms is None else round(self.est_ms, 1),
            "timeouts": self.timeouts, "registered_at": self.registered_at,
            "failed_at": self.failed_at, "fail_reason": self.fail_reason,
            "slowdown": self.slowdown,
        }
