"""
Controller test over the REAL OpenFlow 1.3 wire protocol, without Mininet or OVS.

It starts the actual Ryu controller (ryu-manager controller/sdn_controller.py) and
connects three fake switches (tests/sdn/fake_switch.py) that speak OpenFlow on a
TCP socket. The tests then check the flow tables the controller really pushes:
initial rules, ARP proxy, application events, link failure and recovery,
congestion steering, quarantine, static/dynamic modes.

This validates the controller code and its Ryu integration. It does NOT validate
Open vSwitch or Mininet behaviour (see tests/mininet/ for that).

Run with the Ryu environment, from the project root, as a normal user:
    ~/ryu-env/bin/python -m unittest discover -s tests/sdn -v
"""
import itertools
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, "controller"))

try:
    import ryu  # noqa: F401
except ImportError:
    raise unittest.SkipTest("Ryu not importable: run with ~/ryu-env/bin/python")

import sdn_policy as pol  # noqa: E402
from fake_switch import FakeSwitch  # noqa: E402

from app.controller_client import ControllerClient, request  # noqa: E402

H1, H2 = "10.0.0.1", "10.0.0.2"
W1, W2, W3, W4 = "10.0.0.11", "10.0.0.12", "10.0.0.13", "10.0.0.14"
ALL = sorted(pol.HOSTS)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def wait_for(cond, timeout=8.0, interval=0.02, msg="condition"):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if cond():
                return True
        except Exception:
            pass
        time.sleep(interval)
    raise AssertionError("timed out waiting for " + msg)


class WireTest(unittest.TestCase):
    STATS_INTERVAL = "0.25"
    MODE = "dynamic"
    DOWN = {}      # {dpid: ports reported down when the switch connects}

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="jf-wire-")
        self.sock_path = os.path.join(self.tmp, "ctl.sock")
        self.of_port = free_port()
        env = dict(os.environ, JACKFRUIT_CTL_SOCKET=self.sock_path,
                   JACKFRUIT_STATS_INTERVAL=self.STATS_INTERVAL, JACKFRUIT_MODE=self.MODE,
                   PYTHONUNBUFFERED="1")
        ryu_bin = shutil.which("ryu-manager") or os.path.join(
            os.path.dirname(sys.executable), "ryu-manager")
        self.log_path = os.path.join(self.tmp, "ryu.log")
        self.logf = open(self.log_path, "w")
        self.proc = subprocess.Popen(
            [ryu_bin, "--ofp-tcp-listen-port", str(self.of_port),
             os.path.join(ROOT, "controller", "sdn_controller.py")],
            env=env, stdout=self.logf, stderr=subprocess.STDOUT, cwd=ROOT)
        self.switches = {}
        try:
            wait_for(lambda: os.path.exists(self.sock_path), 20, msg="control socket")
            self.connect_switches(down=self.DOWN)
        except Exception:
            self.tearDown()
            raise

    def connect_switches(self, down=None):
        down = down or {}
        for dpid in (1, 2, 3):
            sw = FakeSwitch(dpid, self.of_port, down.get(dpid, ()))
            sw.start()
            self.switches[dpid] = sw
        for sw in self.switches.values():
            wait_for(sw.main.is_set, 10, msg="switch %d features" % sw.dpid)
        wait_for(lambda: all(len(s.snapshot()) >= 6 for s in self.switches.values()), 10,
                 msg="initial flows")

    def tearDown(self):
        for sw in self.switches.values():
            sw.stop()
        if getattr(self, "proc", None):
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
        if getattr(self, "logf", None):
            self.logf.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ---- helpers
    def ctl(self, **msg):
        return request(self.sock_path, msg)

    def tables(self):
        return {d: s.snapshot() for d, s in self.switches.items()}

    def wait_tables(self, expected, msg="tables to converge", timeout=8.0):
        wait_for(lambda: self.tables() == expected, timeout, msg=msg)

    def ryu_log(self):
        self.logf.flush()
        with open(self.log_path) as f:
            return f.read()

    def fail_link(self, a, b):
        self.switches[a].send_port_status(pol.LINK_PORTS[(a, b)], True)
        self.switches[b].send_port_status(pol.LINK_PORTS[(b, a)], True)

    def restore_link(self, a, b):
        self.switches[a].send_port_status(pol.LINK_PORTS[(a, b)], False)
        self.switches[b].send_port_status(pol.LINK_PORTS[(b, a)], False)


class TestInitialAndArp(WireTest):
    def test_initial_tables_match_policy_and_everyone_is_reachable(self):
        expected = pol.PolicyState().compute_rules()
        self.assertEqual(self.tables(), expected)
        for sw in self.switches.values():
            self.assertTrue(sw.table_miss, "table-miss rule missing on s%d" % sw.dpid)
        for a, b in itertools.permutations(ALL, 2):
            self.assertIsNotNone(pol.simulate_path(self.tables(), a, b), (a, b))
        st = self.ctl(type="GET_STATE")
        self.assertEqual(st["switches"], [1, 2, 3])
        self.assertEqual(st["mode"], "dynamic")

    def test_arp_proxy_answers_from_controller(self):
        sw = self.switches[1]
        sw.send_arp_request(3, "00:00:00:00:00:01", H1, W1)
        wait_for(lambda: sw.packet_outs, 5, msg="ARP reply packet-out")
        port, pkt = sw.packet_outs[0]
        self.assertEqual(port, 3)
        eth, rep = pkt.protocols[0], pkt.protocols[1]
        self.assertEqual(rep.opcode, 2)
        self.assertEqual(rep.src_ip, W1)
        self.assertEqual(rep.src_mac, "00:00:00:00:00:11")
        self.assertEqual(eth.dst, "00:00:00:00:00:01")

    def test_arp_for_unknown_host_is_ignored(self):
        sw = self.switches[1]
        sw.send_arp_request(3, "00:00:00:00:00:01", H1, "10.0.0.99")
        time.sleep(0.5)
        self.assertEqual(sw.packet_outs, [])


class TestControlChannel(WireTest):
    def test_ping_and_bad_requests(self):
        self.assertEqual(self.ctl(type="PING")["type"], "PONG")
        self.assertEqual(self.ctl(type="NOPE")["type"], "ERROR")
        self.assertEqual(self.ctl(type="WORKER_UP", ip="10.9.9.9")["type"], "ERROR")
        self.assertEqual(self.ctl(type="WORKER_UP", ip=H2)["type"], "ERROR")
        self.assertEqual(self.ctl(type="SET_MODE", mode="warp")["type"], "ERROR")
        self.assertEqual(self.ctl(type="WORKER_DOWN", ip=W1, quarantine_s=-3)["type"], "ERROR")
        s = socket.socket(socket.AF_UNIX)
        s.connect(self.sock_path)
        s.sendall(b"this is not json\n")
        reply = s.recv(4096)
        s.close()
        self.assertIn(b"ERROR", reply)
        self.assertEqual(self.ctl(type="PING")["type"], "PONG")   # still alive

    def test_get_rules_reflects_installed_flows(self):
        rules = self.ctl(type="GET_RULES")["rules"]
        self.assertEqual(len(rules["1"]), 6)


class TestApplicationEvents(WireTest):
    def test_worker_up_installs_app_flows_and_down_removes_them(self):
        base = self.tables()
        reply = self.ctl(type="WORKER_UP", ip=W1, worker="w1")
        self.assertEqual(reply["type"], "OK")
        self.assertEqual(reply["path"], ["s1", "s2"])
        st = pol.PolicyState()
        st.on_worker_up(W1)
        self.wait_tables(st.compute_rules())
        self.assertNotEqual(self.tables(), base)
        self.assertEqual(self.tables()[1][(pol.PRIO_APP, H1, W1)], 1)
        self.assertEqual(self.tables()[2][(pol.PRIO_APP, W1, H1)], 1)
        self.assertEqual(self.ctl(type="WORKER_DOWN", ip=W1)["type"], "OK")
        self.wait_tables(base, "app flows removed")

    def test_real_controller_client_notifies_controller(self):
        cc = ControllerClient(self.sock_path, retry_s=0.1)
        cc.start()
        self.addCleanup(cc.stop)
        cc.notify("WORKER_UP", ip=W3, worker="w3")
        self.assertTrue(cc.drain(5))
        st = pol.PolicyState()
        st.on_worker_up(W3)
        self.wait_tables(st.compute_rules())
        sent = cc.log_snapshot()
        self.assertEqual(sent[0][2]["type"], "OK")

    def test_quarantine_drops_then_expires(self):
        self.ctl(type="WORKER_UP", ip=W1, worker="w1")
        self.assertEqual(self.ctl(type="WORKER_DOWN", ip=W1, quarantine_s=1.5)["type"], "OK")
        wait_for(lambda: pol.simulate_path(self.tables(), H1, W1) is None, 5, msg="drop rules")
        self.assertIsNone(pol.simulate_path(self.tables(), W1, H1))
        self.assertIsNotNone(pol.simulate_path(self.tables(), H1, W2))
        self.assertIn(W1, self.ctl(type="GET_STATE")["quarantine"])
        wait_for(lambda: pol.simulate_path(self.tables(), H1, W1) is not None, 8,
                 msg="quarantine expiry")
        self.wait_tables(pol.PolicyState().compute_rules())


class TestLinkFailure(WireTest):
    def test_reroute_on_link_down_and_restore_on_link_up(self):
        self.ctl(type="WORKER_UP", ip=W1, worker="w1")
        self.ctl(type="WORKER_UP", ip=W2, worker="w2")
        t0 = time.time()
        self.fail_link(1, 2)
        st = pol.PolicyState()
        st.on_worker_up(W1)
        st.on_worker_up(W2)
        st.set_port(1, 1, True)
        st.set_port(2, 1, True)
        self.wait_tables(st.compute_rules(), "reroute after link failure")
        reaction = time.time() - t0
        phys = frozenset([pol.link_key(1, 2)])
        for a, b in itertools.permutations(ALL, 2):
            self.assertIsNotNone(pol.simulate_path(self.tables(), a, b, phys), (a, b))
        self.assertEqual(pol.simulate_path(self.tables(), H1, W1, phys), [1, 3, 2])
        self.assertEqual(pol.simulate_path(self.tables(), W2, H1, phys), [2, 3, 1])
        self.assertLess(reaction, 2.0)
        state = self.ctl(type="GET_STATE")
        self.assertEqual(state["down_links"], ["s1-s2"])
        kinds = [e["kind"] for e in state["events"]]
        self.assertIn("link_down", kinds)
        rec = [e for e in state["events"] if e["kind"] == "reconcile" and "link down" in e["reason"]]
        self.assertTrue(rec and rec[0]["flow_mods"] > 0)
        self.assertLess(rec[0]["ms"], 500)
        self.restore_link(1, 2)
        wait_for(lambda: "link_up" in [e["kind"] for e in self.ctl(type="GET_STATE")["events"]],
                 5, msg="link up event")
        wait_for(lambda: pol.simulate_path(self.tables(), H1, W3) == [1, 3], 5, msg="settle")
        for a, b in itertools.permutations(ALL, 2):
            self.assertIsNotNone(pol.simulate_path(self.tables(), a, b), (a, b))

    def test_one_end_reporting_is_enough_and_flapping_is_stable(self):
        for _ in range(3):
            self.fail_link(2, 3)
            wait_for(lambda: pol.simulate_path(self.tables(), W1, W3,
                                               frozenset([pol.link_key(2, 3)])) == [2, 1, 3],
                     5, msg="detour")
            self.restore_link(2, 3)
            wait_for(lambda: pol.simulate_path(self.tables(), W1, W3) == [2, 3], 5,
                     msg="direct again")
        self.wait_tables(pol.PolicyState().compute_rules())


class TestLinkAlreadyDownAtConnect(WireTest):
    # s1 and s2 both report their ports toward each other as down when they connect
    DOWN = {1: (1,), 2: (1,)}

    def test_initial_tables_avoid_the_pre_failed_link(self):
        st = pol.PolicyState()
        st.set_port(1, 1, True)
        st.set_port(2, 1, True)
        self.wait_tables(st.compute_rules(), "tables avoid the pre-failed link")
        phys = frozenset([pol.link_key(1, 2)])
        self.assertEqual(pol.simulate_path(self.tables(), H1, W1, phys), [1, 3, 2])
        self.assertEqual(self.ctl(type="GET_STATE")["down_links"], ["s1-s2"])


class TestStaticBaseline(WireTest):
    MODE = "static"

    def test_static_mode_ignores_failures_and_app_events(self):
        base = self.tables()
        self.assertEqual(self.ctl(type="GET_STATE")["mode"], "static")
        self.ctl(type="WORKER_UP", ip=W1, worker="w1")
        self.fail_link(1, 2)
        time.sleep(1.0)
        self.assertEqual(self.tables(), base)               # nothing changed
        phys = frozenset([pol.link_key(1, 2)])
        self.assertIsNone(pol.simulate_path(self.tables(), H1, W1, phys))   # blackhole
        # the switch to dynamic mode heals it
        self.assertEqual(self.ctl(type="SET_MODE", mode="dynamic")["type"], "OK")
        wait_for(lambda: pol.simulate_path(self.tables(), H1, W1, phys) == [1, 3, 2], 5,
                 msg="healed after switching to dynamic")
        self.assertEqual(self.ctl(type="SET_MODE", mode="static")["type"], "OK")
        self.wait_tables(base, "back to static base rules")


class TestSteering(WireTest):
    def test_congestion_moves_app_flow_to_alternative_path(self):
        self.ctl(type="WORKER_UP", ip=W1, worker="w1")
        self.ctl(type="WORKER_UP", ip=W2, worker="w2")
        time.sleep(pol.STEER_COOLDOWN_S + 0.5)        # registration cooldown
        # saturate s1->s2 (port 1 of s1) and the reverse direction at s2
        self.switches[1].set_rate_mbps(1, 9.5)
        self.switches[2].set_rate_mbps(1, 9.5)
        wait_for(lambda: any(e["kind"] == "steered" for e in
                             self.ctl(type="GET_STATE")["events"]), 15, msg="steer event")
        paths = self.ctl(type="GET_STATE")["app_paths"]
        moved = [ip for ip, p in paths.items() if p == ["s1", "s3", "s2"]]
        self.assertTrue(moved, paths)
        ip = moved[0]
        wait_for(lambda: pol.simulate_path(self.tables(), H1, ip) == [1, 3, 2], 5,
                 msg="flow table follows steering")
        self.assertEqual(pol.simulate_path(self.tables(), ip, H1), [2, 3, 1])
        # other traffic to that worker (h2 -> worker) still takes the shortest path
        self.assertEqual(pol.simulate_path(self.tables(), H2, ip), [1, 2])

    def test_no_steering_in_static_mode(self):
        self.ctl(type="SET_MODE", mode="static")
        self.ctl(type="WORKER_UP", ip=W1, worker="w1")
        self.switches[1].set_rate_mbps(1, 9.5)
        self.switches[2].set_rate_mbps(1, 9.5)
        time.sleep(2.0)
        kinds = [e["kind"] for e in self.ctl(type="GET_STATE")["events"]]
        self.assertNotIn("steered", kinds)
        util = self.ctl(type="GET_STATE")["util"]
        self.assertGreater(util.get("s1->s2", 0), 0.7)       # utilisation is still measured


class TestSwitchReconnect(WireTest):
    def test_flows_are_reinstalled_when_a_switch_reconnects(self):
        self.ctl(type="WORKER_UP", ip=W1, worker="w1")
        st = pol.PolicyState()
        st.on_worker_up(W1)
        self.wait_tables(st.compute_rules())
        old = self.switches[2]
        old.stop()
        wait_for(lambda: 2 not in self.ctl(type="GET_STATE")["switches"], 8,
                 msg="controller notices disconnect")
        new = FakeSwitch(2, self.of_port)
        new.start()
        self.switches[2] = new
        wait_for(new.main.is_set, 10, msg="features")
        self.wait_tables(st.compute_rules(), "tables restored on reconnect")


if __name__ == "__main__":
    unittest.main()
