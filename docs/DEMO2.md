# Demo 2: multi-client / multi-server with statistics

Two independent server processes, one shared task queue, one shared worker coordinator, the same four workers, the
existing Ryu controller and the existing three-switch topology. Demo 1 is untouched.

## Architecture (for the presentation and the viva)

```
 clients (h2)            servers (h1)            shared state            coordinator (h1)        workers
 alice, bob  --9201-->  server1  \                                      worker_coordinator      w1  w2  (s2)
 carol, dave --9202-->  server2  /-- SQLite file results/demo2.db  <--  = TaskManager + sync  -- w3  w4  (s3)
                                      (tasks, servers, clients)           TCP 9100, heartbeats
                                                                          WORKER_UP/DOWN --> Ryu
```

* **Servers** (`app/server.py`) only talk to clients and to the database. They insert tasks and read back status
  and results. They never run tasks and never see a worker. Run as many as you like on different ports.
* **Shared queue** (`app/shared_queue.py`) is one SQLite file in WAL mode. Each task row stores task id, client id,
  server id, payload, status, worker, result, attempts and submission/completion time.
* **Coordinator** (`app/worker_coordinator.py`) is the existing `TaskManager` (worker registration, heartbeats,
  `least_loaded` scheduling, deadlines, requeue after a worker failure, `WORKER_UP`/`WORKER_DOWN` to Ryu) plus one
  loop every 100 ms: **pull** claims `PENDING` rows from SQLite and hands them to the manager, **push** writes each
  task's state, worker and result back. Because only the coordinator talks to workers, there is exactly one worker pool.
* **No double assignment.** A claim is `BEGIN IMMEDIATE`, `SELECT ... PENDING`, `UPDATE ... CLAIMED`, `COMMIT`: SQLite
  allows one writer at a time, so a second process waits and then no longer sees the task as `PENDING`. After that
  the manager's state machine allows a task to be `ASSIGNED` to one worker at a time.
* **Client identity.** Every client passes `--client-id`. Task ids are stored as `<client-id>-<task id>`, so clients
  cannot collide, and a client only sees its own tasks.
* **Statistics** (`app/metrics.py`) are SQL queries over the same file, so they are real measurements.

Task status in the database: `PENDING` (in the queue) -> `CLAIMED` (coordinator has it) -> `ASSIGNED` (a worker runs
it) -> `COMPLETED` / `FAILED`. A task requeued after a worker failure goes back to `CLAIMED`.

## Files

| File | New / changed | Purpose |
|---|---|---|
| `app/shared_queue.py` | new | SQLite queue: tables, atomic claim, insert, update, read |
| `app/worker_coordinator.py` | new | the one shared worker pool; reuses `TaskManager`, adds the sync loop |
| `app/server.py` | new | client-facing server process |
| `app/metrics.py` | new | statistics (`python3 -m app.metrics --db ...`) |
| `app/client.py` | changed | new `--client-id` option (a few lines); Demo 1 behaviour is unchanged |
| `tests/test_demo2.py` | new | 12 tests |
| `scripts/mn/demo2.mn`, `scripts/mn/demo2_failure.mn` | new | Mininet demo scripts |
| `.gitignore`, `README.md` | changed | ignore `results/*.db*`; pointer to this file |

Not changed: `task_manager.py`, `worker.py`, `scheduler.py`, `protocol.py`, `taskmodel.py`, `controller_client.py`,
`controller/`, `topology/`.

## Run it

Terminals 1 and 2 are the same as Demo 1 (`scripts/run_ryu.sh`, then `sudo /usr/bin/python3 topology/topo.py`).
Then at the `mininet>` prompt, either run everything with one script:

```
mininet> source scripts/mn/demo2.mn            # about 20 s; prints statistics during and after the run
mininet> source scripts/mn/demo2_failure.mn    # kills a worker mid-run (optional)
```

or start the pieces by hand (this is what the script does):

```
mininet> sh mkdir -p results
mininet> sh rm -f results/demo2.db results/demo2.db-wal results/demo2.db-shm
mininet> h1 python3 -u -m app.worker_coordinator --db results/demo2.db > results/d2_coordinator.log 2>&1 &
mininet> w1 python3 -u -m app.worker --id w1 --manager 10.0.0.1 > results/d2_w1.log 2>&1 &
mininet> w2 python3 -u -m app.worker --id w2 --manager 10.0.0.1 > results/d2_w2.log 2>&1 &
mininet> w3 python3 -u -m app.worker --id w3 --manager 10.0.0.1 > results/d2_w3.log 2>&1 &
mininet> w4 python3 -u -m app.worker --id w4 --manager 10.0.0.1 > results/d2_w4.log 2>&1 &
mininet> h1 python3 -u -m app.server --id server1 --port 9201 --db results/demo2.db > results/d2_server1.log 2>&1 &
mininet> h1 python3 -u -m app.server --id server2 --port 9202 --db results/demo2.db > results/d2_server2.log 2>&1 &
```

Clients (several at once; two per server):

```
mininet> h2 python3 -u -m app.client --client-id alice --manager 10.0.0.1 --port 9201 --tasks 20 --work-ms 200 --prefix a > results/d2_alice.log 2>&1 &
mininet> h2 python3 -u -m app.client --client-id bob   --manager 10.0.0.1 --port 9202 --tasks 20 --work-ms 200 --prefix b > results/d2_bob.log 2>&1 &
mininet> h2 python3 -u -m app.client --client-id carol --manager 10.0.0.1 --port 9201 --tasks 20 --work-ms 200 --prefix c > results/d2_carol.log 2>&1 &
mininet> h2 python3 -u -m app.client --client-id dave  --manager 10.0.0.1 --port 9202 --tasks 20 --work-ms 200 --prefix d > results/d2_dave.log 2>&1 &
```

Statistics (run it while the clients are working and again afterwards):

```
mininet> sh python3 -m app.metrics --db results/demo2.db
```

Notes:

* Run these inside Mininet with `sh`/`h1`, as above: the files are then created by the same user. Worker ids
  print as IP addresses (`10.0.0.11` is `w1`), see README section 4.4.
* Do not run Demo 1's `app.task_manager` at the same time: it also uses worker port 9100. The scripts stop it first.
* Start with a fresh database file (the scripts delete it). If the coordinator dies, tasks it had claimed stay
  `CLAIMED`; restart everything with a new file.
* `--wait-workers` is a Demo 1 option and does not work against a server (servers do not know the workers).
  Wait a few seconds after starting the workers instead.
* Without a Mininet VM (plain loopback, no SDN), the same commands work with `--manager 127.0.0.1`, plus
  `--controller none --host 127.0.0.1` on the coordinator.

## Example output (EXAMPLE, from a loopback run in a sandbox, not from Mininet)

`python3 -m app.metrics` after four clients each submitted 20 tasks of 200 ms (two via each server):

```
=== Demo 2 statistics ===
Connected clients      : 0
Active servers         : 2
Submitted tasks        : 80
Completed tasks        : 80
Failed tasks           : 0
Pending tasks          : 0   (running now: 0)
Retried tasks          : 0   (re-run after a worker failure)
Average task latency   : 2142.6 ms
Throughput             : 19.43 tasks/s
Completed per worker   :
    w1           20   (tasks submitted via server1: 10, server2: 10)
    w2           20   (tasks submitted via server1: 10, server2: 10)
    w3           20   (tasks submitted via server1: 10, server2: 10)
    w4           20   (tasks submitted via server1: 10, server2: 10)
Requests per server    :
    server1      33 requests, 40 tasks submitted (up)
    server2      33 requests, 40 tasks submitted (up)
```

Worker failure (same setup, one worker process killed one second in; two tasks were in flight): both clients still
finished 20/20, the coordinator log shows `worker w2 FAILED ... 2 task(s) in flight` and two `requeued` lines, and the
statistics showed `Completed tasks : 40`, `Failed tasks : 0`, `Retried tasks : 2`. Your numbers will differ.

Definitions: latency = completion time minus submission time, averaged over completed tasks (it includes time spent
waiting in the queue); throughput = completed tasks / (last completion - first submission); a request is one
message a server handled from a client; connected clients are clients with an open connection to a server that is up;
a server counts as active if its heartbeat is less than 3 s old.

## What was tested

Run in a sandbox (Python 3.13, no Mininet, no Ryu):

* `python3 -m unittest discover -s tests`: **134 tests passed** (the 122 existing tests plus 12 new ones).
* `tests/test_demo2.py` covers: both server processes start and are active; four clients on both servers at the same
  time each get only their own, correct results; both servers write to the same queue and all four workers run tasks
  from both; no task is assigned twice (4 separate processes racing to claim 150 tasks claimed each exactly once);
  statistics match what was submitted; connected clients are counted; results are readable through the other server;
  a submit without a client id is refused; a worker that dies mid-task has its tasks redistributed and every task completes.
* A manual run with every component as its own process (coordinator, 4 workers, 2 servers, 4 clients) using the
  commands above on loopback: 80/80 tasks completed, and the worker-failure run described above.

**Not tested (no Mininet/Ryu in the sandbox):** `scripts/mn/demo2.mn` and `demo2_failure.mn` themselves, the
`WORKER_UP`/`WORKER_DOWN` notifications to Ryu from the coordinator, link-failure recovery with Demo 2 running, and
Demo 1 through Mininet. The coordinator creates its `ControllerClient` exactly as `task_manager.py` does, and the
existing controller tests still pass, but do one dry run in your VM before presenting.

## Limitations

* One coordinator and one SQLite file: the coordinator is a single point of failure, and there is no recovery of
  tasks claimed by a coordinator that died.
* Server and coordinator clocks are the same machine's clock (all on one Mininet VM), so latencies are consistent.
* A duplicate submission (same client and task id) is ignored; its spec is not compared with the original.
