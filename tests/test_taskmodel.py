"""Unit tests: task creation, status transitions, spec validation, scheduler policies.
No sockets, no root, no Mininet."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from app import taskmodel as tm  # noqa: E402
from app.scheduler import LeastLoaded, RoundRobin, make_scheduler  # noqa: E402


def spec(**kw):
    return tm.normalise_spec(dict(task_type="sumsq", n=10, **kw))


def mk_task(tid="t1", **kw):
    s = spec(**kw)
    return tm.Task(tid, s, 100.0)


def mk_worker(wid, inflight=0, est=None, alive=True):
    w = tm.WorkerInfo(wid, ("10.0.0.1", 1), 1.0, 0.0)
    w.alive = alive
    w.est_ms = est
    for i in range(inflight):
        w.assigned["x%s-%d" % (wid, i)] = 0.0
    return w


class TaskStateTests(unittest.TestCase):
    def test_new_task_is_pending(self):
        t = mk_task()
        self.assertEqual(t.state, tm.PENDING)
        self.assertFalse(t.terminal)
        self.assertEqual(t.attempts, 0)

    def test_normal_lifecycle(self):
        t = mk_task()
        t.transition(tm.ASSIGNED, 101.0, "w1")
        t.transition(tm.COMPLETED, 102.0, "done")
        self.assertTrue(t.terminal)
        self.assertEqual([h[1] for h in t.history], ["PENDING", "ASSIGNED", "COMPLETED"])

    def test_requeue_path(self):
        t = mk_task()
        t.transition(tm.ASSIGNED, 1)
        t.transition(tm.PENDING, 2, "worker failed")
        t.transition(tm.ASSIGNED, 3)
        t.transition(tm.COMPLETED, 4)

    def test_late_result_while_pending_allowed(self):
        t = mk_task()
        t.transition(tm.ASSIGNED, 1)
        t.transition(tm.PENDING, 2)
        t.transition(tm.COMPLETED, 3)

    def test_terminal_states_are_final(self):
        for terminal in (tm.COMPLETED, tm.FAILED):
            t = mk_task()
            t.transition(tm.ASSIGNED, 1)
            t.transition(terminal, 2)
            for nxt in (tm.PENDING, tm.ASSIGNED, tm.COMPLETED, tm.FAILED):
                with self.assertRaises(tm.InvalidTransition):
                    t.transition(nxt, 3)

    def test_cannot_skip_assignment_to_assigned_twice(self):
        t = mk_task()
        t.transition(tm.ASSIGNED, 1)
        with self.assertRaises(tm.InvalidTransition):
            t.transition(tm.ASSIGNED, 2)

    def test_to_dict_latency(self):
        t = mk_task()
        t.transition(tm.ASSIGNED, 100.5)
        t.transition(tm.COMPLETED, 101.0)
        t.completed_at = 101.0
        self.assertEqual(t.to_dict()["latency_ms"], 1000.0)


class SpecTests(unittest.TestCase):
    def test_defaults(self):
        s = tm.normalise_spec({})
        self.assertEqual(s["task_type"], "sumsq")
        self.assertTrue(s["idempotent"])
        self.assertEqual(s["max_attempts"], tm.DEFAULT_MAX_ATTEMPTS)

    def test_rejects_bad_specs(self):
        bad = [
            "not a dict", {"task_type": "rm -rf"}, {"n": -1}, {"n": 1.5},
            {"n": tm.MAX_N + 1}, {"n": True}, {"n": "10"}, {"work_ms": -1},
            {"payload_bytes": tm.MAX_PAYLOAD_BYTES + 1}, {"idempotent": "yes"},
            {"task_id": ""}, {"task_id": 5}, {"task_id": "x" * 65},
            {"deadline_s": 0}, {"max_attempts": 0}, {"max_attempts": 11},
        ]
        for b in bad:
            with self.subTest(spec=b):
                with self.assertRaises(tm.InvalidSpec):
                    tm.normalise_spec(b)


class ComputeTests(unittest.TestCase):
    def test_sumsq_matches_closed_form(self):
        for n in (0, 1, 2, 10, 1000, 12345):
            self.assertEqual(tm.compute("sumsq", n), tm.expected_output("sumsq", n))

    def test_primes_known_values(self):
        self.assertEqual(tm.compute("primes", 10), 4)       # 2 3 5 7
        self.assertEqual(tm.compute("primes", 100), 25)
        self.assertEqual(tm.compute("primes", 1000), 168)
        self.assertEqual(tm.compute("primes", 2), 0)

    def test_unknown_type(self):
        with self.assertRaises(ValueError):
            tm.compute("nope", 1)

    def test_payload_deterministic_and_exact_size(self):
        for size in (0, 1, 7, 100, 4096):
            p = tm.make_payload("task-9", size)
            self.assertEqual(len(p), size)
            self.assertEqual(p, tm.make_payload("task-9", size))
        self.assertNotEqual(tm.payload_crc("abc"), tm.payload_crc("abd"))

    def test_task_expected_values(self):
        t = mk_task("tid", payload_bytes=50)
        self.assertEqual(t.expected_output, tm.expected_output("sumsq", 10))
        self.assertEqual(t.expected_crc, tm.payload_crc(tm.make_payload("tid", 50)))


class SchedulerTests(unittest.TestCase):
    def test_round_robin_cycles_and_ignores_load(self):
        rr = RoundRobin()
        ws = [mk_worker("w1", inflight=9), mk_worker("w2"), mk_worker("w3")]
        picks = [rr.pick(mk_task("t%d" % i), ws).worker_id for i in range(6)]
        self.assertEqual(picks, ["w1", "w2", "w3", "w1", "w2", "w3"])

    def test_round_robin_skips_dead_and_excluded(self):
        rr = RoundRobin()
        ws = [mk_worker("w1"), mk_worker("w2", alive=False), mk_worker("w3")]
        t = mk_task()
        t.excluded.add("w1")
        self.assertEqual(rr.pick(t, ws).worker_id, "w3")

    def test_no_workers(self):
        self.assertIsNone(RoundRobin().pick(mk_task(), []))
        self.assertIsNone(LeastLoaded().pick(mk_task(), [mk_worker("w1", alive=False)]))

    def test_least_loaded_prefers_idle(self):
        ll = LeastLoaded(cap=3)
        ws = [mk_worker("w1", inflight=2, est=100), mk_worker("w2", inflight=0, est=100)]
        self.assertEqual(ll.pick(mk_task(), ws).worker_id, "w2")

    def test_least_loaded_prefers_fast_worker(self):
        ll = LeastLoaded(cap=4)
        fast, slow = mk_worker("w1", inflight=1, est=100), mk_worker("w2", inflight=0, est=1000)
        # fast finishes a new task at (1+1)*100=200ms, slow at (0+1)*1000=1000ms
        self.assertEqual(ll.pick(mk_task(), [slow, fast]).worker_id, "w1")

    def test_least_loaded_respects_cap(self):
        ll = LeastLoaded(cap=2)
        ws = [mk_worker("w1", inflight=2), mk_worker("w2", inflight=2)]
        self.assertIsNone(ll.pick(mk_task(), ws))
        ws[1].assigned.clear()
        self.assertEqual(ll.pick(mk_task(), ws).worker_id, "w2")

    def test_least_loaded_unknown_estimate_uses_average(self):
        ll = LeastLoaded(cap=4)
        known = mk_worker("w1", inflight=1, est=100)   # score 200
        new = mk_worker("w2", inflight=0, est=None)    # default=100 -> score 100
        self.assertEqual(ll.pick(mk_task(), [known, new]).worker_id, "w2")

    def test_exclusion_ignored_if_no_other_worker(self):
        t = mk_task()
        t.excluded.add("w1")
        self.assertEqual(LeastLoaded().pick(t, [mk_worker("w1")]).worker_id, "w1")
        self.assertEqual(RoundRobin().pick(t, [mk_worker("w1")]).worker_id, "w1")

    def test_factory(self):
        self.assertIsInstance(make_scheduler("round_robin"), RoundRobin)
        self.assertIsInstance(make_scheduler("least_loaded", 3), LeastLoaded)
        with self.assertRaises(ValueError):
            make_scheduler("random")
        with self.assertRaises(ValueError):
            LeastLoaded(cap=0)


class WorkerInfoTests(unittest.TestCase):
    def test_ewma(self):
        w = mk_worker("w1")
        w.observe(100)
        self.assertEqual(w.est_ms, 100)
        w.observe(200)
        self.assertAlmostEqual(w.est_ms, 130.0)


if __name__ == "__main__":
    unittest.main()
