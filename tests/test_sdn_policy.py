"""
Unit tests for the SDN forwarding policy (controller/sdn_policy.py).
Pure Python: no Ryu, no root, no Mininet. Packets are traced through the computed
flow tables with simulate_path(), so a bad reroute shows up as a black hole or loop.
"""
import itertools
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "controller"))
import sdn_policy as pol  # noqa: E402

H1, H2 = "10.0.0.1", "10.0.0.2"
W1, W2, W3, W4 = "10.0.0.11", "10.0.0.12", "10.0.0.13", "10.0.0.14"
ALL = sorted(pol.HOSTS)


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def fail_link(st, a, b):
    """Both switches report their port facing the other as down (what OVS does)."""
    st.set_port(a, pol.LINK_PORTS[(a, b)], True)
    st.set_port(b, pol.LINK_PORTS[(b, a)], True)


def restore_link(st, a, b):
    st.set_port(a, pol.LINK_PORTS[(a, b)], False)
    st.set_port(b, pol.LINK_PORTS[(b, a)], False)


def down_set(*pairs):
    return frozenset(pol.link_key(a, b) for a, b in pairs)


class TestGraph(unittest.TestCase):
    def test_shortest_paths_in_full_triangle(self):
        self.assertEqual(pol.shortest_path(1, 2), [1, 2])
        self.assertEqual(pol.shortest_path(2, 3), [2, 3])
        self.assertEqual(pol.shortest_path(1, 1), [1])

    def test_shortest_path_detours_when_link_down(self):
        self.assertEqual(pol.shortest_path(1, 2, down_set((1, 2))), [1, 3, 2])
        self.assertEqual(pol.shortest_path(2, 3, down_set((2, 3))), [2, 1, 3])

    def test_unreachable(self):
        self.assertIsNone(pol.shortest_path(1, 2, down_set((1, 2), (1, 3))))

    def test_simple_paths_shortest_first(self):
        self.assertEqual(pol.simple_paths(1, 2), [(1, 2), (1, 3, 2)])
        self.assertEqual(pol.simple_paths(1, 2, down_set((1, 2))), [(1, 3, 2)])
        self.assertEqual(pol.simple_paths(1, 2, down_set((1, 2), (1, 3))), [])

    def test_path_ports(self):
        self.assertEqual(pol.path_ports((1, 3, 2), 3), [(1, 2), (3, 2), (2, 3)])
        self.assertEqual(pol.path_ports((2,), 4), [(2, 4)])

    def test_diff_rules(self):
        inst = {"a": 1, "b": 2, "c": 3}
        want = {"a": 1, "b": 9, "d": 4}
        add, rm = pol.diff_rules(inst, want)
        self.assertEqual(add, {"b": 9, "d": 4})
        self.assertEqual(rm, ["c"])
        self.assertEqual(pol.diff_rules(want, want), ({}, []))


class TestBaseRules(unittest.TestCase):
    # The flow table verified on the real Mininet topology in Stage 3.
    EXPECTED = {
        1: {H1: 3, H2: 4, W1: 1, W2: 1, W3: 2, W4: 2},
        2: {H1: 1, H2: 1, W1: 3, W2: 4, W3: 2, W4: 2},
        3: {H1: 1, H2: 1, W1: 2, W2: 2, W3: 3, W4: 4},
    }

    def test_matches_verified_stage3_table(self):
        for mode in pol.MODES:
            rules = pol.PolicyState(mode).compute_rules()
            for sw, per_ip in self.EXPECTED.items():
                got = {k[2]: out for k, out in rules[sw].items() if k[0] == pol.PRIO_BASE}
                self.assertEqual(got, per_ip, (mode, sw))
                self.assertEqual(len(rules[sw]), 6)

    def test_every_pair_reachable_initially(self):
        for mode in pol.MODES:
            rules = pol.PolicyState(mode).compute_rules()
            for a, b in itertools.permutations(ALL, 2):
                self.assertIsNotNone(pol.simulate_path(rules, a, b), (mode, a, b))

    def test_w1_to_w3_goes_directly_s2_s3(self):
        rules = pol.PolicyState().compute_rules()
        self.assertEqual(pol.simulate_path(rules, W1, W3), [2, 3])


class TestLinkFailure(unittest.TestCase):
    def test_dynamic_reroutes_around_failed_link(self):
        st = pol.PolicyState(pol.MODE_DYNAMIC)
        fail_link(st, 1, 2)
        rules = st.compute_rules()
        phys = down_set((1, 2))
        self.assertEqual(pol.simulate_path(rules, H1, W1, phys), [1, 3, 2])
        self.assertEqual(pol.simulate_path(rules, W1, H1, phys), [2, 3, 1])
        for a, b in itertools.permutations(ALL, 2):
            self.assertIsNotNone(pol.simulate_path(rules, a, b, phys), (a, b))

    def test_static_blackholes_traffic_over_failed_link(self):
        st = pol.PolicyState(pol.MODE_STATIC)
        fail_link(st, 1, 2)
        rules = st.compute_rules()
        phys = down_set((1, 2))
        self.assertIsNone(pol.simulate_path(rules, H1, W1, phys))
        self.assertIsNone(pol.simulate_path(rules, W2, H2, phys))
        # traffic that never used the failed link is fine
        self.assertIsNotNone(pol.simulate_path(rules, H1, W3, phys))
        self.assertIsNotNone(pol.simulate_path(rules, W3, W4, phys))

    def test_restoration_returns_to_shortest_paths(self):
        st = pol.PolicyState()
        before = st.compute_rules()
        fail_link(st, 1, 2)
        self.assertNotEqual(st.compute_rules(), before)
        restore_link(st, 1, 2)
        self.assertEqual(st.compute_rules(), before)

    def test_one_end_reporting_down_is_enough(self):
        st = pol.PolicyState()
        self.assertTrue(st.set_port(1, 1, True))
        self.assertEqual(st.down_links(), {pol.link_key(1, 2)})
        self.assertFalse(st.set_port(2, 1, True))      # second report: no new change
        self.assertFalse(st.set_port(1, 1, True))      # repeated report: no change
        st.set_port(1, 1, False)
        self.assertEqual(st.down_links(), {pol.link_key(1, 2)})   # other end still down
        self.assertTrue(st.set_port(2, 1, False))
        self.assertEqual(st.down_links(), set())

    def test_host_facing_ports_are_ignored(self):
        st = pol.PolicyState()
        self.assertFalse(st.set_port(1, 3, True))      # h1's port
        self.assertFalse(st.set_port(2, 4, True))      # w2's port
        self.assertEqual(st.down_links(), set())

    def test_every_single_link_failure_leaves_everyone_reachable(self):
        for a, b in ((1, 2), (1, 3), (2, 3)):
            st = pol.PolicyState()
            fail_link(st, a, b)
            rules = st.compute_rules()
            phys = down_set((a, b))
            for x, y in itertools.permutations(ALL, 2):
                self.assertIsNotNone(pol.simulate_path(rules, x, y, phys), (a, b, x, y))

    def test_double_failure_isolates_s1_without_crashing(self):
        st = pol.PolicyState()
        st.on_worker_up(W1)
        fail_link(st, 1, 2)
        fail_link(st, 1, 3)
        rules = st.compute_rules()
        phys = down_set((1, 2), (1, 3))
        self.assertIsNone(pol.simulate_path(rules, H1, W1, phys))
        self.assertIsNotNone(pol.simulate_path(rules, W1, W3, phys))
        self.assertIsNotNone(pol.simulate_path(rules, H1, H2, phys))
        self.assertIsNone(st.app_paths[W1])
        restore_link(st, 1, 2)
        self.assertEqual(st.app_paths[W1], (1, 2))

    def test_events_are_logged(self):
        st = pol.PolicyState()
        fail_link(st, 1, 2)
        restore_link(st, 1, 2)
        kinds = [e["kind"] for e in st.events]
        self.assertEqual(kinds.count("link_down"), 1)
        self.assertEqual(kinds.count("link_up"), 1)


class TestAppFlows(unittest.TestCase):
    def test_unknown_worker_ip_rejected(self):
        st = pol.PolicyState()
        self.assertFalse(st.on_worker_up("10.9.9.9"))
        self.assertFalse(st.on_worker_up(H2))         # not a worker
        self.assertFalse(st.on_worker_down("10.9.9.9"))

    def test_worker_up_creates_app_rules_in_both_directions(self):
        st = pol.PolicyState()
        self.assertTrue(st.on_worker_up(W1))
        rules = st.compute_rules()
        self.assertEqual(rules[1][(pol.PRIO_APP, H1, W1)], 1)       # s1 -> s2
        self.assertEqual(rules[2][(pol.PRIO_APP, H1, W1)], 3)       # s2 -> w1
        self.assertEqual(rules[2][(pol.PRIO_APP, W1, H1)], 1)       # s2 -> s1
        self.assertEqual(rules[1][(pol.PRIO_APP, W1, H1)], 3)       # s1 -> h1
        self.assertNotIn((pol.PRIO_APP, H1, W1), rules[3])
        self.assertEqual(pol.simulate_path(rules, H1, W1), [1, 2])

    def test_worker_down_removes_app_rules(self):
        st = pol.PolicyState()
        st.on_worker_up(W1)
        base = pol.PolicyState().compute_rules()
        self.assertNotEqual(st.compute_rules(), base)
        st.on_worker_down(W1)
        self.assertEqual(st.compute_rules(), base)

    def test_app_flow_follows_link_failure_and_recovery(self):
        st = pol.PolicyState()
        st.on_worker_up(W2)
        fail_link(st, 1, 2)
        self.assertEqual(st.app_paths[W2], (1, 3, 2))
        phys = down_set((1, 2))
        rules = st.compute_rules()
        self.assertEqual(pol.simulate_path(rules, H1, W2, phys), [1, 3, 2])
        self.assertEqual(pol.simulate_path(rules, W2, H1, phys), [2, 3, 1])
        restore_link(st, 1, 2)
        # a valid path is kept (no flapping back); steering/failure logic decides changes
        self.assertEqual(st.app_paths[W2], (1, 3, 2))
        self.assertIsNotNone(pol.simulate_path(st.compute_rules(), H1, W2))

    def test_static_mode_has_no_app_rules(self):
        st = pol.PolicyState(pol.MODE_STATIC)
        st.on_worker_up(W1)
        self.assertEqual(st.compute_rules(), pol.PolicyState(pol.MODE_STATIC).compute_rules())

    def test_mode_switch(self):
        st = pol.PolicyState(pol.MODE_STATIC)
        st.on_worker_up(W1)
        fail_link(st, 1, 2)
        static_rules = st.compute_rules()
        self.assertIsNone(pol.simulate_path(static_rules, H1, W1, down_set((1, 2))))
        self.assertTrue(st.set_mode(pol.MODE_DYNAMIC))
        self.assertFalse(st.set_mode(pol.MODE_DYNAMIC))
        rules = st.compute_rules()
        self.assertEqual(pol.simulate_path(rules, H1, W1, down_set((1, 2))), [1, 3, 2])
        with self.assertRaises(ValueError):
            st.set_mode("turbo")
        with self.assertRaises(ValueError):
            pol.PolicyState("turbo")


class TestSteering(unittest.TestCase):
    def setUp(self):
        self.clk = Clock()
        self.st = pol.PolicyState(clock=self.clk)
        self.st.on_worker_up(W1)
        self.st.on_worker_up(W2)
        self.clk.advance(10)    # past the cooldown that starts at registration

    def congest(self, a, b, mbps):
        for _ in range(8):                   # converge the EWMA
            self.st.update_util(a, b, mbps)
            self.st.update_util(b, a, mbps)

    def test_no_move_when_not_congested(self):
        self.congest(1, 2, 3.0)
        self.assertIsNone(self.st.steer())

    def test_moves_one_flow_per_call_off_congested_link(self):
        self.congest(1, 2, 9.5)
        self.congest(1, 3, 0.5)
        self.congest(2, 3, 0.5)
        first = self.st.steer()
        self.assertIsNotNone(first)
        ip, old, new = first
        self.assertEqual((old, new), ((1, 2), (1, 3, 2)))
        self.assertEqual(self.st.app_paths[ip], (1, 3, 2))
        other = W2 if ip == W1 else W1
        self.assertEqual(self.st.app_paths[other], (1, 2))
        # the other flow is still congested but the moved flow is in cooldown
        # and the other is allowed: a second call moves the second flow
        second = self.st.steer()
        self.assertIsNotNone(second)
        self.assertEqual(second[0], other)

    def test_cooldown_prevents_flapping(self):
        self.congest(1, 2, 9.5)
        ip, _, _ = self.st.steer()
        # now make the new path look worse and the old one better
        self.congest(1, 2, 0.5)
        self.congest(1, 3, 9.5)
        self.congest(2, 3, 9.5)
        self.clk.advance(1)
        moved = self.st.steer()
        self.assertTrue(moved is None or moved[0] != ip)
        self.clk.advance(pol.STEER_COOLDOWN_S + 1)
        moved = self.st.steer()
        self.assertIsNotNone(moved)
        self.assertEqual(moved[0], ip)
        self.assertEqual(moved[2], (1, 2))

    def test_no_move_if_alternative_is_also_loaded(self):
        self.congest(1, 2, 9.0)
        self.congest(1, 3, 7.5)           # alternative not clearly better (margin 0.3)
        self.congest(2, 3, 7.5)
        self.assertIsNone(self.st.steer())

    def test_no_steering_in_static_mode(self):
        st = pol.PolicyState(pol.MODE_STATIC, clock=self.clk)
        st.on_worker_up(W1)
        for _ in range(8):
            st.update_util(1, 2, 10)
        self.clk.advance(10)
        self.assertIsNone(st.steer())

    def test_new_worker_avoids_congested_link_at_registration(self):
        self.st.on_worker_down(W1)
        self.congest(1, 2, 9.0)
        self.st.on_worker_up(W1)
        self.assertEqual(self.st.app_paths[W1], (1, 3, 2))

    def test_steered_rules_forward_correctly(self):
        self.congest(1, 2, 9.5)
        ip, _, new = self.st.steer()
        rules = self.st.compute_rules()
        self.assertEqual(pol.simulate_path(rules, H1, ip), [1, 3, 2])
        self.assertEqual(pol.simulate_path(rules, ip, H1), [2, 3, 1])
        # unrelated traffic (h2 -> same worker) keeps using the shortest path
        self.assertEqual(pol.simulate_path(rules, H2, ip), [1, 2])

    def test_util_ewma_and_clamp(self):
        st = pol.PolicyState()
        st.update_util(1, 2, 5.0)
        self.assertAlmostEqual(st.util[(1, 2)], 0.5)
        st.update_util(1, 2, 10.0)
        self.assertAlmostEqual(st.util[(1, 2)], 0.75)
        st.update_util(2, 1, 1000.0)
        self.assertEqual(st.util[(2, 1)], 2.0)
        st.update_util(1, 3, -5)
        self.assertEqual(st.util[(1, 3)], 0.0)


class TestQuarantine(unittest.TestCase):
    def test_quarantine_drops_both_directions_then_expires(self):
        clk = Clock()
        st = pol.PolicyState(clock=clk)
        st.on_worker_up(W1)
        st.on_worker_down(W1, quarantine_s=5)
        rules = st.compute_rules()
        self.assertIsNone(pol.simulate_path(rules, H1, W1))
        self.assertIsNone(pol.simulate_path(rules, W1, H1))
        self.assertIsNone(pol.simulate_path(rules, W3, W1))
        self.assertIsNotNone(pol.simulate_path(rules, H1, W2))      # others unaffected
        self.assertIn(W1, st.snapshot()["quarantine"])
        clk.advance(6)
        self.assertTrue(st.expire_quarantine())
        self.assertFalse(st.expire_quarantine())
        self.assertIsNotNone(pol.simulate_path(st.compute_rules(), H1, W1))

    def test_worker_up_cancels_quarantine(self):
        st = pol.PolicyState(clock=Clock())
        st.on_worker_down(W1, quarantine_s=30)
        st.on_worker_up(W1)
        self.assertEqual(st.quarantine, {})
        self.assertIsNotNone(pol.simulate_path(st.compute_rules(), H1, W1))

    def test_no_quarantine_in_static_mode(self):
        st = pol.PolicyState(pol.MODE_STATIC, clock=Clock())
        st.on_worker_down(W1, quarantine_s=30)
        self.assertIsNotNone(pol.simulate_path(st.compute_rules(), H1, W1))


class TestSnapshot(unittest.TestCase):
    def test_snapshot_is_json_serialisable(self):
        import json
        st = pol.PolicyState()
        st.on_worker_up(W1)
        fail_link(st, 2, 3)
        st.update_util(1, 2, 4)
        text = json.dumps(st.snapshot())
        self.assertIn("s2-s3", text)


if __name__ == "__main__":
    unittest.main()
