"""
Integration tests for the task manager, workers and client over real TCP sockets
on loopback. No root, no Mininet, no Ryu needed.

They cover: dispatch and result collection, registration (incl. duplicates and
malformed input), worker crash, hung worker (heartbeat timeout), task deadline,
retry limits, non-retry-safe tasks, bad/duplicate results, duplicate submissions,
controller notifications, reconnect, graceful shutdown and a concurrency run.
"""
import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from helpers import (FakeController, FakeWorker, ManagerFixture, wait_until)  # noqa: E402

from app import taskmodel as tm  # noqa: E402
from app.client import connect, make_specs, run_batch  # noqa: E402
from app.controller_client import ControllerClient  # noqa: E402
from app.protocol import (ERROR, METRICS, METRICS_REQ, REGISTER, STATUS, STATUS_REQ,  # noqa: E402
                          SUBMIT, SUBMIT_ACK, MessageChannel)


class ManagerTestCase(unittest.TestCase):
    def setUp(self):
        self.fx = None

    def tearDown(self):
        if self.fx:
            self.fx.close()

    def make(self, **kw):
        self.fx = ManagerFixture(**kw)
        return self.fx


class TestNormalOperation(ManagerTestCase):
    def test_batch_completes_with_correct_results(self):
        fx = self.make()
        fx.real_worker("w1")
        fx.real_worker("w2")
        fx.wait_workers(2)
        specs = make_specs(12, n=500, work_ms=20, payload_bytes=2048)
        r = run_batch("127.0.0.1", fx.client_port, specs, timeout=20, verbose=False)
        self.assertEqual((r["completed"], r["failed"], r["unfinished"]), (12, 0, 0))
        self.assertEqual(sum(r["tasks_per_worker"].values()), 12)
        for t in r["tasks"]:
            self.assertEqual(t["result"], tm.expected_output("sumsq", 500))
            self.assertEqual(t["state"], "COMPLETED")
        self.assertIsNotNone(r["makespan_s"])
        self.assertEqual(r["manager_metrics"]["counters"]["completed"], 12)

    def test_primes_task(self):
        fx = self.make()
        fx.real_worker("w1")
        fx.wait_workers(1)
        specs = make_specs(1, task_type="primes", n=1000, work_ms=0)
        r = run_batch("127.0.0.1", fx.client_port, specs, timeout=10, verbose=False)
        self.assertEqual(r["tasks"][0]["result"], 168)

    def test_least_loaded_favours_fast_worker(self):
        fx = self.make(policy="least_loaded", cap=2)
        fx.real_worker("fast", slowdown=1.0)
        fx.real_worker("slow", slowdown=6.0)
        fx.wait_workers(2)
        r = run_batch("127.0.0.1", fx.client_port, make_specs(24, work_ms=50), timeout=30,
                      verbose=False)
        self.assertEqual(r["completed"], 24)
        self.assertGreater(r["tasks_per_worker"]["fast"], r["tasks_per_worker"].get("slow", 0) * 2)

    def test_round_robin_splits_evenly_regardless_of_speed(self):
        fx = self.make(policy="round_robin")
        fx.real_worker("fast", slowdown=1.0)
        fx.real_worker("slow", slowdown=2.0)
        fx.wait_workers(2)
        r = run_batch("127.0.0.1", fx.client_port, make_specs(10, work_ms=20), timeout=30,
                      verbose=False)
        self.assertEqual(r["tasks_per_worker"], {"fast": 5, "slow": 5})

    def test_tasks_wait_until_a_worker_registers(self):
        fx = self.make()
        ack = fx.submit(make_specs(3, work_ms=1))
        self.assertEqual(len(ack["accepted"]), 3)
        time.sleep(0.4)
        self.assertEqual(fx.task("t0000")["state"], "PENDING")
        fx.real_worker("w1")
        fx.wait_all_terminal()
        self.assertEqual(fx.task("t0002")["state"], "COMPLETED")

    def test_status_and_metrics_messages(self):
        fx = self.make()
        fx.real_worker("w1")
        fx.wait_workers(1)
        fx.submit(make_specs(2, work_ms=1))
        fx.wait_all_terminal()
        ch = fx.client()
        try:
            ch.send({"type": STATUS_REQ, "workers": True})
            st = ch.recv()
            self.assertEqual(st["type"], STATUS)
            self.assertEqual(st["summary"]["completed"], 2)
            self.assertTrue(st["summary"]["all_done"])
            self.assertEqual(st["workers"][0]["worker_id"], "w1")
            ch.send({"type": STATUS_REQ, "task_ids": ["t0000", "nope"], "workers": False})
            st = ch.recv()
            self.assertEqual([t["state"] for t in st["tasks"]], ["COMPLETED", "UNKNOWN"])
            ch.send({"type": METRICS_REQ})
            m = ch.recv()
            self.assertEqual(m["type"], METRICS)
            self.assertIn("events", m)
            self.assertEqual(m["workers"][0]["completed"], 2)
        finally:
            ch.close()

    def test_many_workers_many_tasks_exactly_once(self):
        fx = self.make(cap=3)
        for i in range(8):
            fx.real_worker("w%d" % i)
        fx.wait_workers(8)
        r = run_batch("127.0.0.1", fx.client_port, make_specs(200, n=200, work_ms=5),
                      timeout=60, verbose=False)
        self.assertEqual((r["completed"], r["failed"]), (200, 0))
        ids = [t["task_id"] for t in r["tasks"]]
        self.assertEqual(len(set(ids)), 200)
        c = r["manager_metrics"]["counters"]
        self.assertEqual(c.get("duplicate_results", 0), 0)
        self.assertEqual(sum(w["completed"] for w in r["manager_metrics"]["workers"]), 200)


class TestRegistrationAndMalformedInput(ManagerTestCase):
    def test_duplicate_live_worker_id_rejected(self):
        fx = self.make()
        fx.real_worker("w1")
        fx.wait_workers(1)
        fw = FakeWorker("w1", fx.worker_port, register=False, heartbeat=False)
        fw.ch.send({"type": REGISTER, "worker_id": "w1"})
        wait_until(lambda: fw.replies, 5, msg="reply to duplicate REGISTER")
        reply = fw.replies[0]          # (FakeWorker's reader thread collects replies)
        self.assertEqual(reply["type"], ERROR)
        self.assertIn("already registered", reply["reason"])
        self.assertEqual(fx.mgr.counters["duplicate_registrations"], 1)
        self.assertEqual(len(fx.mgr.workers), 1)

    def test_stale_worker_is_replaced_by_new_connection(self):
        fx = self.make()
        old = fx.fake_worker("w1", heartbeat=False)     # goes silent
        fx.wait_workers(1)
        time.sleep(2.1)   # beyond the 2 s "recently heard from" window (and hb_timeout)
        # the silent worker was already declared dead by the heartbeat check
        self.assertEqual(len(fx.mgr.workers), 0)
        new = fx.fake_worker("w1")
        self.assertEqual(new.ack["type"], "REGISTER_ACK")
        old.stop()

    def test_invalid_worker_id_and_wrong_first_message(self):
        fx = self.make()
        for first in ({"type": REGISTER, "worker_id": "bad id!"},
                      {"type": REGISTER},
                      {"type": "TASK"}):
            s = socket.create_connection(("127.0.0.1", fx.worker_port), timeout=3)
            ch = MessageChannel(s)
            ch.send(first)
            reply = ch.recv()
            self.assertEqual(reply["type"], ERROR, first)
            ch.close()
        self.assertEqual(len(fx.mgr.workers), 0)

    def test_garbage_does_not_kill_manager(self):
        fx = self.make()
        for garbage in (b"this is not json\n", b"[1,2,3]\n", b'{"no_type":1}\n', b"\xff\xfe\n"):
            s = socket.create_connection(("127.0.0.1", fx.worker_port), timeout=3)
            s.sendall(garbage)
            s.settimeout(3)
            data = s.recv(4096)       # an ERROR reply or a close, never a hang
            s.close()
            if data:
                self.assertIn(b"ERROR", data)
        fx.real_worker("w1")
        fx.wait_workers(1)
        r = run_batch("127.0.0.1", fx.client_port, make_specs(2, work_ms=1), timeout=10,
                      verbose=False)
        self.assertEqual(r["completed"], 2)

    def test_garbage_after_registration_drops_only_that_worker(self):
        fx = self.make()
        good = fx.real_worker("good")
        bad = fx.fake_worker("bad")
        fx.wait_workers(2)
        bad.sock.sendall(b"%%% not json %%%\n")
        wait_until(lambda: "bad" not in fx.mgr.workers, 5, msg="bad worker dropped")
        self.assertIn("good", fx.mgr.workers)

    def test_client_malformed_and_unknown_messages(self):
        fx = self.make()
        ch = fx.client()
        ch.send({"type": "BOGUS"})
        self.assertEqual(ch.recv()["type"], ERROR)
        ch.send({"type": SUBMIT, "tasks": "nope"})
        self.assertEqual(ch.recv()["type"], ERROR)
        ch.send({"type": SUBMIT, "tasks": []})
        self.assertEqual(ch.recv()["type"], ERROR)
        ch.sock.sendall(b"garbage\n")
        self.assertEqual(ch.recv()["type"], ERROR)
        ch.close()

    def test_invalid_task_specs_rejected_good_ones_accepted(self):
        fx = self.make()
        ack = fx.submit([{"task_id": "ok1", "n": 10}, {"n": -5}, {"task_type": "evil"},
                         {"task_id": "ok2", "n": 20, "payload_bytes": 10 ** 9}])
        self.assertEqual(ack["type"], SUBMIT_ACK)
        self.assertEqual(ack["accepted"], ["ok1"])
        self.assertEqual([r["index"] for r in ack["rejected"]], [1, 2, 3])

    def test_duplicate_submission_is_idempotent(self):
        fx = self.make()
        spec = {"task_id": "same", "n": 10}
        a1 = fx.submit([spec])
        a2 = fx.submit([spec])
        a3 = fx.submit([dict(spec, n=11)])
        self.assertEqual(a1["accepted"], ["same"])
        self.assertEqual(a2["duplicates"], ["same"])
        self.assertEqual(a2["accepted"], [])
        self.assertEqual(len(a3["rejected"]), 1)
        self.assertEqual(len(fx.mgr.tasks), 1)
        self.assertEqual(fx.mgr.counters["duplicate_submits"], 1)


class TestFailureHandling(ManagerTestCase):
    def test_worker_crash_requeues_to_other_worker(self):
        fx = self.make(hb_timeout=5.0)   # make sure we detect via the closed socket
        dying = fx.fake_worker("dying", on_task=lambda m, fw: fw.crash())
        fx.wait_workers(1)
        fx.submit(make_specs(3, work_ms=1, prefix="c"))
        wait_until(lambda: "dying" not in fx.mgr.workers, 5, msg="crash noticed")
        fx.real_worker("good")
        fx.wait_all_terminal()
        for i in range(3):
            t = fx.task("c000%d" % i)
            self.assertEqual(t["state"], "COMPLETED")
            self.assertEqual(t["worker"], "good")
        self.assertGreaterEqual(fx.mgr.counters["requeues"], 1)
        kinds = [e["kind"] for e in fx.mgr.events]
        self.assertIn("worker_failed", kinds)
        self.assertIn("task_requeued", kinds)

    def test_hung_worker_detected_by_heartbeat_timeout(self):
        fx = self.make(hb_timeout=0.8)
        hung = fx.fake_worker("hung", heartbeat=False)   # never heartbeats, never answers
        fx.wait_workers(1)
        fx.submit(make_specs(2, work_ms=1, prefix="h"))
        wait_until(lambda: len(hung.tasks) >= 1, 5, msg="task reached hung worker")
        t0 = time.time()
        wait_until(lambda: "hung" not in fx.mgr.workers, 5, msg="hang detected")
        detect = time.time() - t0
        self.assertGreater(detect, 0.2)
        self.assertLess(detect, 2.5)
        reasons = [e["reason"] for e in fx.mgr.events if e["kind"] == "worker_failed"]
        self.assertTrue(any("heartbeat timeout" in r for r in reasons), reasons)
        fx.real_worker("good")
        fx.wait_all_terminal()
        self.assertEqual(fx.task("h0000")["worker"], "good")
        self.assertEqual(fx.task("h0001")["state"], "COMPLETED")

    def test_task_deadline_requeues_and_late_result_is_duplicate(self):
        fx = self.make(hb_timeout=10.0)
        stuck = fx.fake_worker("stuck")                  # heartbeats, never finishes
        fx.wait_workers(1)
        fx.submit([{"task_id": "slow1", "n": 10, "deadline_s": 0.5}])
        wait_until(lambda: len(stuck.tasks) == 1, 5, msg="task delivered")
        good = fx.real_worker("good")
        fx.wait_workers(2)
        t = fx.wait_task("slow1", timeout=10)
        self.assertEqual(t["state"], "COMPLETED")
        self.assertEqual(t["worker"], "good")
        self.assertGreaterEqual(fx.mgr.counters["task_timeouts"], 1)
        # the stuck worker finally answers: the task is already done -> duplicate
        stuck.reply(stuck.tasks[0])
        wait_until(lambda: fx.mgr.counters["duplicate_results"] >= 1, 5, msg="duplicate counted")
        self.assertEqual(fx.task("slow1")["worker"], "good")
        self.assertEqual(fx.mgr.counters["completed"], 1)

    def test_late_result_accepted_if_task_still_open(self):
        fx = self.make(hb_timeout=10.0)
        stuck = fx.fake_worker("stuck")
        fx.wait_workers(1)
        fx.submit([{"task_id": "late1", "n": 10, "deadline_s": 0.3, "max_attempts": 5}])
        wait_until(lambda: len(stuck.tasks) >= 2, 5, msg="task re-sent after timeout")
        stuck.reply(stuck.tasks[0])        # answer for the FIRST attempt
        t = fx.wait_task("late1")
        self.assertEqual(t["state"], "COMPLETED")
        stuck.reply(stuck.tasks[-1])       # answer for the second attempt: duplicate
        wait_until(lambda: fx.mgr.counters["duplicate_results"] >= 1, 5, msg="dup counted")
        self.assertEqual(fx.mgr.counters["completed"], 1)

    def test_max_attempts_then_failed(self):
        fx = self.make(hb_timeout=10.0)
        bad = fx.fake_worker("bad", on_task=lambda m, fw: fw.reply(m, status="ERROR",
                                                                      reason="boom"))
        fx.wait_workers(1)
        fx.submit([{"task_id": "doomed", "n": 10, "max_attempts": 3}])
        t = fx.wait_task("doomed")
        self.assertEqual(t["state"], "FAILED")
        self.assertEqual(t["attempts"], 3)
        self.assertIn("max attempts", t["reason"])

    def test_manager_attempt_cap_disables_redistribution(self):
        fx = self.make(hb_timeout=5.0, max_attempts_cap=1)
        fx.fake_worker("dying", on_task=lambda m, fw: fw.crash())
        fx.wait_workers(1)
        fx.submit([{"task_id": "once", "n": 10}])
        t = fx.wait_task("once")
        self.assertEqual(t["state"], "FAILED")
        self.assertEqual(t["attempts"], 1)

    def test_non_idempotent_task_is_not_retried(self):
        fx = self.make(hb_timeout=5.0)
        fx.fake_worker("dying", on_task=lambda m, fw: fw.crash())
        fx.wait_workers(1)
        fx.real_worker("good")
        fx.wait_workers(2)
        fx.submit([{"task_id": "once", "n": 10, "idempotent": False},
                   {"task_id": "safe", "n": 10, "idempotent": True}])
        fx.wait_all_terminal()
        # whichever worker got which task, a non-idempotent task is never run twice
        self.assertLessEqual(fx.task("once")["attempts"], 1)
        if fx.task("once")["state"] == "FAILED":
            self.assertIn("not retry-safe", fx.task("once")["reason"])
        self.assertEqual(fx.task("safe")["state"], "COMPLETED")

    def test_non_idempotent_failure_goes_to_failed(self):
        fx = self.make(hb_timeout=5.0)
        fx.fake_worker("dying", on_task=lambda m, fw: fw.crash())
        fx.wait_workers(1)
        fx.submit([{"task_id": "once", "n": 10, "idempotent": False}])
        t = fx.wait_task("once")
        self.assertEqual(t["state"], "FAILED")
        self.assertIn("not retry-safe", t["reason"])

    def test_wrong_output_and_wrong_checksum_are_rejected_and_retried(self):
        fx = self.make(hb_timeout=10.0)
        calls = {"n": 0}

        def liar(msg, fw):
            calls["n"] += 1
            if calls["n"] == 1:
                fw.reply(msg, output=12345)           # wrong answer
            elif calls["n"] == 2:
                fw.reply(msg, payload_crc=1)          # corrupted payload
            else:
                fw.reply(msg)                         # honest
        fx.fake_worker("liar", on_task=liar)
        fx.wait_workers(1)
        fx.submit([{"task_id": "chk", "n": 50, "payload_bytes": 100, "max_attempts": 5}])
        t = fx.wait_task("chk")
        self.assertEqual(t["state"], "COMPLETED")
        self.assertEqual(t["attempts"], 3)
        self.assertEqual(fx.mgr.counters["bad_results"], 2)

    def test_unknown_task_result_is_ignored(self):
        fx = self.make()
        fw = fx.fake_worker("w1")
        fx.wait_workers(1)
        fw.ch.send({"type": "RESULT", "task_id": "ghost", "status": "OK"})
        wait_until(lambda: fx.mgr.counters["unknown_results"] == 1, 5, msg="unknown counted")

    def test_all_workers_dead_tasks_stay_pending_then_finish_on_return(self):
        fx = self.make(hb_timeout=5.0)
        fx.fake_worker("dying", on_task=lambda m, fw: fw.crash())
        fx.wait_workers(1)
        fx.submit(make_specs(2, work_ms=1, prefix="p"))
        wait_until(lambda: not fx.mgr.workers, 5, msg="worker gone")
        self.assertEqual(fx.task("p0001")["state"], "PENDING")
        fx.real_worker("back")
        fx.wait_all_terminal()
        self.assertEqual(fx.task("p0000")["worker"], "back")

    def test_worker_reconnects_after_manager_drops_it(self):
        fx = self.make()
        w = fx.real_worker("w1")
        fx.wait_workers(1)
        first = fx.mgr.workers["w1"]
        with fx.mgr.lock:
            fx.mgr._fail_worker_locked(first, "test: forced disconnect")
        wait_until(lambda: "w1" in fx.mgr.workers and fx.mgr.workers["w1"] is not first,
                   10, msg="worker re-registered")
        r = run_batch("127.0.0.1", fx.client_port, make_specs(3, work_ms=1), timeout=10,
                      verbose=False)
        self.assertEqual(r["completed"], 3)


class TestControllerNotifications(ManagerTestCase):
    def test_worker_up_and_down_are_sent(self):
        fc = FakeController()
        fc.start()
        self.addCleanup(fc.stop)
        fx = self.make(controller=ControllerClient(fc.path, retry_s=0.1), quarantine_s=7.0)
        fw = fx.fake_worker("w1", on_task=lambda m, f: f.crash())
        wait_until(lambda: "WORKER_UP" in fc.types(), 5, msg="WORKER_UP")
        up = [m for m in fc.received if m["type"] == "WORKER_UP"][0]
        self.assertEqual(up["ip"], "127.0.0.1")
        fx.submit([{"task_id": "x", "n": 1}])
        wait_until(lambda: "WORKER_DOWN" in fc.types(), 5, msg="WORKER_DOWN")
        down = [m for m in fc.received if m["type"] == "WORKER_DOWN"][0]
        self.assertEqual(down["worker"], "w1")
        self.assertEqual(down["quarantine_s"], 0.0)   # crash: no quarantine

    def test_quarantine_requested_only_for_silent_worker(self):
        fc = FakeController()
        fc.start()
        self.addCleanup(fc.stop)
        fx = self.make(controller=ControllerClient(fc.path, retry_s=0.1), quarantine_s=7.0,
                       hb_timeout=0.6)
        fx.fake_worker("quiet", heartbeat=False)
        wait_until(lambda: "WORKER_DOWN" in fc.types(), 5, msg="WORKER_DOWN")
        down = [m for m in fc.received if m["type"] == "WORKER_DOWN"][0]
        self.assertEqual(down["quarantine_s"], 7.0)

    def test_state_is_resynced_when_controller_comes_up_late(self):
        d = tempfile.mkdtemp()
        path = os.path.join(d, "late.sock")
        fx = self.make(controller=ControllerClient(path, retry_s=0.1))
        fx.fake_worker("w1")
        fx.wait_workers(1)
        time.sleep(0.4)                       # controller not there yet
        fc = FakeController(path)
        fc.start()
        self.addCleanup(fc.stop)
        wait_until(lambda: "WORKER_UP" in fc.types(), 5, msg="resync WORKER_UP")

    def test_controller_outage_does_not_block_the_manager(self):
        fx = self.make(controller=ControllerClient("/nonexistent/ctl.sock", retry_s=0.1))
        fx.real_worker("w1")
        fx.wait_workers(1)
        r = run_batch("127.0.0.1", fx.client_port, make_specs(5, work_ms=1), timeout=10,
                      verbose=False)
        self.assertEqual(r["completed"], 5)


class TestShutdown(ManagerTestCase):
    def test_graceful_shutdown_tells_workers_and_writes_metrics(self):
        fx = self.make()
        w = fx.real_worker("w1", reconnect=True)
        fx.wait_workers(1)
        run_batch("127.0.0.1", fx.client_port, make_specs(2, work_ms=1), timeout=10,
                  verbose=False)
        path = os.path.join(tempfile.mkdtemp(), "m.json")
        fx.mgr.stop(path)
        wait_until(lambda: w.shutdown_received, 5, msg="worker got SHUTDOWN")
        with open(path) as f:
            m = json.load(f)
        self.assertEqual(m["counters"]["completed"], 2)
        self.assertEqual(len(m["tasks"]), 2)


if __name__ == "__main__":
    unittest.main()
