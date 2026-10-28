"""Unit tests for the Stage 8 checker's parsing/analysis functions (synthetic text only)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import check_s8 as c  # noqa: E402


def ping_text(times_seqs):
    lines = ["PING 10.0.0.11 (10.0.0.11) 56(84) bytes of data."]
    for t, s in times_seqs:
        lines.append("[%.6f] 64 bytes from 10.0.0.11: icmp_seq=%d ttl=64 time=0.5 ms" % (t, s))
    lines.append("--- 10.0.0.11 ping statistics ---")
    return "\n".join(lines)


class TestParse(unittest.TestCase):
    def test_parse_ignores_noise(self):
        r = c.parse_ping(ping_text([(100.0, 1), (100.05, 2)]))
        self.assertEqual(r, [(100.0, 1), (100.05, 2)])

    def test_empty(self):
        self.assertEqual(c.parse_ping("no replies\n"), [])


class TestOutage(unittest.TestCase):
    def test_gap_and_lost(self):
        # replies every 0.05 s, silence 100.5 -> 101.2 (14 probes lost)
        rep = [(100 + 0.05 * i, i) for i in range(10)] + [(101.2, 24), (101.25, 25)]
        gap, lost = c.outage(rep, 100.45, 108.0)
        self.assertAlmostEqual(gap, 101.2 - 100.45, places=3)
        self.assertEqual(lost, 14)

    def test_never_recovered(self):
        rep = [(100 + 0.05 * i, i) for i in range(10)]
        self.assertEqual(c.outage(rep, 105.0, 113.0), (None, None))

    def test_no_replies(self):
        self.assertEqual(c.outage([], 1, 2), (None, None))

    def test_gap_before_fault_ignored(self):
        rep = [(90.0, 1), (95.0, 100), (110.0, 400), (110.05, 401)]
        gap, _ = c.outage(rep, 109.0, 117.0)
        self.assertAlmostEqual(gap, 15.0, places=3)   # the 95->110 silence overlaps the fault


class TestReaction(unittest.TestCase):
    def state(self, mods):
        return {"events": [
            {"kind": "link_down", "t": 100.0, "link": "s1-s2"},
            {"kind": "reconcile", "t": 100.02, "reason": "link down (s1 port 1)",
             "flow_mods": mods, "ms": 12.0}]}

    def test_dynamic(self):
        r = c.reaction(self.state(8), 99.9)
        self.assertEqual(r["flow_mods"], 8)
        self.assertAlmostEqual(r["delay_s"], 0.02, places=3)

    def test_static_logs_link_down_but_no_reconcile(self):
        st = {"events": [{"kind": "link_down", "t": 100.0, "link": "s1-s2"}]}
        r = c.reaction(st, 99.9)
        self.assertEqual(r["flow_mods"], 0)

    def test_no_event(self):
        self.assertIsNone(c.reaction({"events": []}, 1.0))

    def test_earlier_event_ignored(self):
        self.assertIsNone(c.reaction(self.state(8), 200.0))


if __name__ == "__main__":
    unittest.main()


class TestEventsSince(unittest.TestCase):
    def test_old_run_events_are_ignored(self):
        st = {"events": [{"kind": "steered", "t": 50.0}, {"kind": "steered", "t": 51.0},
                         {"kind": "mode_changed", "t": 100.0},
                         {"kind": "steered", "t": 130.0}]}
        self.assertEqual(len(c.events_since(st, "steered", 100.0)), 1)
        self.assertEqual(len(c.events_since(st, "steered", 0)), 3)
        self.assertEqual(c.events_since({}, "steered", 0), [])
