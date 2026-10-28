"""Tests for experiments/analyze.py using SYNTHETIC files in a temp dir (never real results)."""
import json
import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "experiments"))
import analyze as a  # noqa: E402


def client(completed=40, submitted=40, failed=0, requeues=0, makespan=10.0, wf=0):
    return {"submitted": submitted, "completed": completed, "failed": failed, "requeues": requeues,
            "makespan_s": makespan, "throughput_tps": completed / makespan,
            "latency_ms": {"mean": 500.0, "median": 400, "max": 900},
            "tasks_per_worker": {"a": 10, "b": 30},
            "manager_metrics": {"counters": {"workers_failed": wf}}}


def put(d, name, obj):
    with open(os.path.join(d, "exp", name), "w") as f:
        if isinstance(obj, str):
            f.write(obj)
        else:
            json.dump(obj, f)


class TestAnalyze(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.d = self.tmp.name
        os.makedirs(os.path.join(self.d, "exp"))

    def tearDown(self):
        self.tmp.cleanup()

    def test_sched_rows_and_summary(self):
        for var, ms in (("round_robin", 6.0), ("least_loaded", 3.0)):
            for r in (1, 2):
                put(self.d, "sched_%s_r%d_client.json" % (var, r), client(makespan=ms + r * 0.1))
        rows, skipped = a.collect(self.d)
        self.assertEqual((len(rows), skipped), (4, []))
        a.write_md(rows, skipped, os.path.join(self.d, "s.md"))
        with open(os.path.join(self.d, "s.md")) as f:
            md = f.read()
        self.assertIn("least_loaded (n=2)", md)
        self.assertIn("better", md)

    def test_linkfail_open_outage_is_flagged(self):
        pre = "linkfail_static_r1_"
        put(self.d, pre + "client.json", client(100, 100, makespan=20))
        put(self.d, pre + "times.txt", "104.0\n112.0\n")
        put(self.d, pre + "ping.log", "\n".join(
            "[%.3f] 64 bytes from 10.0.0.11: icmp_seq=%d ttl=64 time=1 ms" % (100 + .05 * i, i)
            for i in range(40)))                       # replies stop at 102 s, never resume
        put(self.d, pre + "state.json", {"events": [{"kind": "link_down", "t": 104.0}]})
        rows, skipped = a.collect(self.d)
        self.assertEqual(skipped, [])
        self.assertEqual(rows[0]["outage_open"], 1)
        self.assertEqual(rows[0]["outage_s"], 8.0)
        self.assertEqual(rows[0]["ctl_flow_mods"], 0)

    def test_congestion_ignores_stale_steering(self):
        pre = "congestion_static_r1_"
        put(self.d, pre + "client.json", client())
        put(self.d, pre + "times.txt", "200.0\n")
        put(self.d, pre + "state.json", {"util": {"s1->s2": 0.9},
                                         "events": [{"kind": "steered", "t": 50.0}]})
        rows, _ = a.collect(self.d)
        self.assertEqual(rows[0]["steer_events"], 0)

    def test_broken_run_is_excluded_not_invented(self):
        put(self.d, "linkfail_dynamic_r1_client.json", client())     # no times/ping/state
        rows, skipped = a.collect(self.d)
        self.assertEqual(rows, [])
        self.assertEqual(len(skipped), 1)

    def test_mean_sd_and_improvement(self):
        self.assertEqual(a.mean_sd([2.0, 4.0]), (3.0, 2 ** 0.5, 2))
        self.assertEqual(a.mean_sd([]), (None, None, 0))
        self.assertAlmostEqual(a.improvement(10, 5, True), 50.0)
        self.assertAlmostEqual(a.improvement(10, 15, False), 50.0)
        self.assertIsNone(a.improvement(0, 5, True))

    def test_graphs_written(self):
        for var in ("static", "dynamic"):
            for r in (1, 2, 3):
                put(self.d, "congestion_%s_r%d_client.json" % (var, r), client(makespan=10 + r))
                put(self.d, "congestion_%s_r%d_times.txt" % (var, r), "1.0\n")
                put(self.d, "congestion_%s_r%d_state.json" % (var, r), {"events": []})
        rows, _ = a.collect(self.d)
        made = a.write_graphs(rows, os.path.join(self.d, "g"))
        self.assertEqual(len(made), 3)
        with open(made[0]) as f:
            self.assertTrue(f.read().startswith("<svg"))


if __name__ == "__main__":
    unittest.main()
