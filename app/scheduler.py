"""
Application-level scheduling policies (this is NOT the SDN layer: it decides
which *worker* gets a task, not which network path packets take).

round_robin   Baseline. Load-oblivious: hands tasks to workers in turn, as soon
              as they arrive, with no limit on how many a worker may hold.
              A slow worker simply accumulates a long queue.

least_loaded  Load-aware. A worker may hold at most `cap` unfinished tasks. The
              next task goes to the worker with the smallest estimated finish
              time  (inflight + 1) * est_ms,  where est_ms is an exponentially
              weighted average of how long that worker took per task so far
              (measured by the manager, so it includes network delay and
              congestion, not only CPU time). Workers with no history yet are
              assumed to be as fast as the average of the others, so they get
              tried.

Both skip workers that are not alive and workers that already failed this task
(unless that would leave no worker at all, in which case the exclusion is
ignored rather than starving the task).
"""

ROUND_ROBIN = "round_robin"
LEAST_LOADED = "least_loaded"
POLICIES = (ROUND_ROBIN, LEAST_LOADED)


def _eligible(task, workers):
    alive = [w for w in workers if w.alive]
    fresh = [w for w in alive if w.worker_id not in task.excluded]
    return fresh or alive


class RoundRobin:
    name = ROUND_ROBIN

    def __init__(self):
        self._last = None  # worker_id handed a task most recently

    def pick(self, task, workers):
        cands = sorted(_eligible(task, workers), key=lambda w: w.worker_id)
        if not cands:
            return None
        chosen = cands[0]
        if self._last is not None:
            for w in cands:
                if w.worker_id > self._last:
                    chosen = w
                    break
        self._last = chosen.worker_id
        return chosen


class LeastLoaded:
    name = LEAST_LOADED

    def __init__(self, cap=2):
        if cap < 1:
            raise ValueError("cap must be >= 1")
        self.cap = cap

    def pick(self, task, workers):
        cands = [w for w in _eligible(task, workers) if w.inflight < self.cap]
        if not cands:
            return None
        known = [w.est_ms for w in workers if w.alive and w.est_ms]
        default = (sum(known) / len(known)) if known else 1.0

        def score(w):
            return ((w.inflight + 1) * (w.est_ms or default), w.worker_id)
        return min(cands, key=score)


def make_scheduler(name, cap=2):
    if name == ROUND_ROBIN:
        return RoundRobin()
    if name == LEAST_LOADED:
        return LeastLoaded(cap)
    raise ValueError("unknown policy %r (choose from %s)" % (name, ", ".join(POLICIES)))
