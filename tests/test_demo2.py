"""
Tests for Demo 2 (multi-client / multi-server). Real TCP on loopback, no Mininet or Ryu.

What is real here: two server.py OS processes, a real TaskManager used as the shared
coordinator, real Worker threads, real client code, one SQLite file in a temp dir.
"""
import multiprocessing
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from helpers import FakeWorker, wait_until  # noqa: E402

from app import metrics  # noqa: E402
from app import shared_queue as sq  # noqa: E402
from app import taskmodel as tm  # noqa: E402
from app.client import make_specs, run_batch  # noqa: E402
from app.task_manager import TaskManager  # noqa: E402
from app.worker import Worker  # noqa: E402
from app.worker_coordinator import sync_loop  # noqa: E402

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def _claim_all(db_path, queue, start_together):
    """Child process: keep claiming until the queue is empty, report what we got."""
    conn = sq.connect(db_path)
    start_together.wait()                 # all children start competing at the same moment
    got = []
    while True:
        batch = sq.claim_pending(conn, limit=3)
        if not batch:
            break
        got += [tid for tid, _ in batch]
        time.sleep(0.005)                 # give the other processes a chance to interleave
    conn.close()
    queue.put(got)


class SharedQueueTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="jf-q-")
        self.db_path = os.path.join(self.dir, "q.db")
        self.db = sq.connect(self.db_path)

    def test_duplicate_submission_is_not_inserted_twice(self):
        spec = {"task_type": "sumsq", "n": 10}
        self.assertEqual(sq.add_tasks(self.db, "a", "s1", [("a-1", spec)]), ["a-1"])
        self.assertEqual(sq.add_tasks(self.db, "a", "s2", [("a-1", spec)]), [])
        self.assertEqual(len(sq.get_tasks(self.db)), 1)

    def test_wal_mode_is_on(self):
        self.assertEqual(self.db.execute("PRAGMA journal_mode").fetchone()[0], "wal")

    def test_processes_never_claim_the_same_task(self):
        sq.add_tasks(self.db, "a", "s1", [("a-%03d" % i, {"n": i}) for i in range(150)])
        ctx = multiprocessing.get_context("spawn")      # fresh interpreters, like real servers
        q = ctx.Queue()
        start_together = ctx.Barrier(4)
        procs = [ctx.Process(target=_claim_all, args=(self.db_path, q, start_together))
                 for _ in range(4)]
        for p in procs:
            p.start()
        results = [q.get(timeout=30) for _ in procs]
        for p in procs:
            p.join()
        everything = [tid for r in results for tid in r]
        self.assertEqual(len(everything), 150)             # every task claimed ...
        self.assertEqual(len(set(everything)), 150)        # ... exactly once
        self.assertGreaterEqual(sum(1 for r in results if r), 2)   # work really was shared


class Demo2System:
    """Coordinator (TaskManager + sync loop) in this process, two server processes,
    and real workers."""

    def __init__(self, workers=4):
        self.dir = tempfile.mkdtemp(prefix="jf-d2-")
        self.db_path = os.path.join(self.dir, "shared.db")
        sq.connect(self.db_path).close()
        self.mgr = TaskManager(hb_interval=0.2, hb_timeout=0.8)
        worker_port, _ = self.mgr.start("127.0.0.1", 0, 0)
        self.worker_port = worker_port
        self.stop = threading.Event()
        self.syncer = threading.Thread(target=sync_loop, args=(self.mgr, self.db_path, self.stop))
        self.syncer.start()
        self.workers = []
        for i in range(1, workers + 1):
            self.add_worker("w%d" % i)
        wait_until(lambda: len(self.mgr.workers) >= workers, 5, msg="workers registered")
        self.servers = {}
        for sid in ("server1", "server2"):
            self.servers[sid] = self.start_server(sid)
        self.db = sq.connect(self.db_path)

    def add_worker(self, wid):
        w = Worker(wid, "127.0.0.1", self.worker_port)
        threading.Thread(target=w.run, daemon=True).start()
        self.workers.append(w)

    def start_server(self, sid):
        proc = subprocess.Popen([sys.executable, "-u", "-m", "app.server", "--id", sid,
                                 "--host", "127.0.0.1", "--port", "0", "--db", self.db_path],
                                cwd=ROOT, stdout=subprocess.PIPE, text=True)
        line = proc.stdout.readline()                      # "server X listening on port N"
        return proc, int(line.split()[-1])

    def port(self, sid):
        return self.servers[sid][1]

    def close(self):
        for proc, _ in self.servers.values():
            proc.terminate()
            proc.wait(timeout=10)
            proc.stdout.close()
        self.stop.set()
        self.syncer.join()
        for w in self.workers:
            w.stop()
        self.mgr.stop()


def run_client(system, server_id, client_id, count, results, **kw):
    specs = make_specs(count, n=1000, work_ms=kw.pop("work_ms", 40), prefix="t", **kw)
    results[client_id] = run_batch("127.0.0.1", system.port(server_id), specs, timeout=30,
                                   verbose=False, client_id=client_id)


class Demo2Tests(unittest.TestCase):
    def setUp(self):
        self.sys = Demo2System()

    def tearDown(self):
        self.sys.close()

    def run_four_clients(self, per_client=15):
        """alice+bob use server1, carol+dave use server2, all at the same time."""
        results = {}
        plan = [("server1", "alice"), ("server1", "bob"), ("server2", "carol"), ("server2", "dave")]
        threads = [threading.Thread(target=run_client, args=(self.sys, s, c, per_client, results))
                   for s, c in plan]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=60)
        return results

    def test_both_servers_start_and_are_active(self):
        stats = metrics.get_stats(self.sys.db)
        self.assertEqual(stats["active_servers"], 2)
        self.assertEqual(set(stats["per_server"]), {"server1", "server2"})
        self.assertNotEqual(self.sys.port("server1"), self.sys.port("server2"))

    def test_clients_on_both_servers_get_their_own_results(self):
        results = self.run_four_clients()
        self.assertEqual(set(results), {"alice", "bob", "carol", "dave"})
        expected = str(tm.expected_output("sumsq", 1000))
        for cid, r in results.items():
            self.assertEqual((r["completed"], r["failed"], r["unfinished"]), (15, 0, 0), cid)
            for t in r["tasks"]:
                self.assertTrue(t["task_id"].startswith(cid + "-"), "result of another client")
                self.assertEqual(t["result"], expected)

    def test_both_servers_feed_the_same_queue_and_the_same_four_workers(self):
        self.run_four_clients()
        rows = self.sys.db.execute("SELECT server_id, worker, status FROM tasks").fetchall()
        self.assertEqual(len(rows), 60)
        self.assertEqual({r["server_id"] for r in rows}, {"server1", "server2"})
        self.assertTrue(all(r["status"] == sq.COMPLETED for r in rows))
        workers_used = {r["worker"] for r in rows}
        self.assertEqual(workers_used, {"w1", "w2", "w3", "w4"})
        # the same worker ran tasks that came in through different servers
        both = [w for w in workers_used
                if {r["server_id"] for r in rows if r["worker"] == w} == {"server1", "server2"}]
        self.assertTrue(both)

    def test_no_task_is_assigned_to_two_workers(self):
        self.run_four_clients()
        with self.sys.mgr.lock:
            assigned = {}
            for ev in self.sys.mgr.events:
                if ev["kind"] == "task_assigned":
                    assigned[ev["task"]] = assigned.get(ev["task"], 0) + 1
            self.assertEqual(len(assigned), 60)
            self.assertTrue(all(n == 1 for n in assigned.values()))   # no failures -> one attempt
            self.assertEqual(self.sys.mgr.counters["duplicate_results"], 0)
        self.assertEqual(self.sys.db.execute("SELECT MAX(attempts) FROM tasks").fetchone()[0], 1)

    def test_statistics_match_what_was_submitted(self):
        self.run_four_clients()
        time.sleep(0.3)
        s = metrics.get_stats(self.sys.db)
        self.assertEqual((s["submitted"], s["completed"], s["failed"], s["pending"]), (60, 60, 0, 0))
        self.assertEqual(sum(s["per_worker"].values()), 60)
        for worker, total in s["per_worker"].items():           # breakdown adds up per worker
            self.assertEqual(sum(s["worker_by_server"][worker].values()), total)
        self.assertEqual(sum(v["tasks_submitted"] for v in s["per_server"].values()), 60)
        self.assertEqual(s["per_server"]["server1"]["tasks_submitted"], 30)
        self.assertEqual(s["per_server"]["server2"]["tasks_submitted"], 30)
        self.assertTrue(all(v["requests"] > 0 and v["up"] for v in s["per_server"].values()))
        self.assertGreater(s["avg_latency_ms"], 40)          # each task sleeps 40 ms
        self.assertGreater(s["throughput_tps"], 0)
        self.assertEqual(s["connected_clients"], 0)          # all clients have finished
        self.assertIn("Completed tasks        : 60", metrics.format_stats(s))

    def test_connected_clients_are_counted_while_connected(self):
        a = threading.Thread(target=run_client, args=(self.sys, "server1", "alice", 5, {}),
                             kwargs={"work_ms": 600})
        b = threading.Thread(target=run_client, args=(self.sys, "server2", "bob", 5, {}),
                             kwargs={"work_ms": 600})
        a.start()
        b.start()
        wait_until(lambda: metrics.get_stats(self.sys.db)["connected_clients"] == 2, 5,
                   msg="two connected clients")
        a.join()
        b.join()
        wait_until(lambda: metrics.get_stats(self.sys.db)["connected_clients"] == 0, 5,
                   msg="clients gone")

    def test_result_can_be_read_through_the_other_server(self):
        results = {}
        run_client(self.sys, "server1", "alice", 5, results)
        ids = [t["task_id"] for t in results["alice"]["tasks"]]
        # ask server2 for alice's tasks
        again = run_batch("127.0.0.1", self.sys.port("server2"),
                          make_specs(5, n=1000, work_ms=40, prefix="t"), timeout=30,
                          verbose=False, client_id="alice")
        self.assertEqual([t["task_id"] for t in again["tasks"]], ids)   # same tasks (duplicates)
        self.assertEqual(again["completed"], 5)
        self.assertEqual(self.sys.db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0], 5)

    def test_submit_without_client_id_is_refused(self):
        with self.assertRaises(RuntimeError):
            run_batch("127.0.0.1", self.sys.port("server1"), make_specs(1), timeout=5, verbose=False)

    def test_worker_failure_tasks_are_redistributed(self):
        # a 5th, fake worker takes tasks and then dies without answering
        fake = FakeWorker("wfake", self.sys.worker_port)
        wait_until(lambda: "wfake" in self.sys.mgr.workers, 5, msg="fake registered")
        results = {}
        t = threading.Thread(target=run_client, args=(self.sys, "server2", "erin", 30, results),
                             kwargs={"work_ms": 80})
        t.start()
        wait_until(lambda: len(fake.tasks) >= 1, 10, msg="fake got a task")
        fake.crash()
        t.join(timeout=60)
        r = results["erin"]
        self.assertEqual((r["completed"], r["failed"]), (30, 0))
        s = metrics.get_stats(self.sys.db)
        self.assertGreaterEqual(s["retried"], 1)
        self.assertNotIn("wfake", s["per_worker"])           # it never finished a task


if __name__ == "__main__":
    unittest.main()
