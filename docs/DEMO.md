# Final demo script

Total time about 12 minutes. Every command below is one used elsewhere in this project; timings come from the
`sleep` lines in the scripts plus startup. **The full sequence has not been rehearsed end to end by the
author of this document**, so do one dry run first and write your own real timings next to each step.

## 0. Before the audience arrives (5 minutes)

Normal user, in `~/cn-jackfruit`:

```bash
git status --short                  # note anything already modified
ss -ltn | grep 6653                 # expect nothing: no controller running yet
scripts/run_ryu.sh                  # Terminal 1, leave running
```

Terminal 2 (sudo):

```bash
cd ~/cn-jackfruit
sudo /usr/bin/python3 topology/topo.py
```

Rules for the whole demo: never run `sudo mn -c` (it kills Ryu); never nest `source`; if the Mininet CLI
hangs, `exit` and start `topo.py` again (Ryu can stay up).

Open `results/graphs/` and `results/summary.md` in a second window as the fallback for any step that fails.

## 1. The network (2 minutes)

```
mininet> links
mininet> pingall
mininet> sh ovs-ofctl -O OpenFlow13 dump-flows s1
mininet> sh python3 scripts/ctl.py ping
```

Say: three OVS switches in a triangle, 10 Mbit/s core links, one Ryu controller. `pingall` works with no
flooding: the controller installed priority-10 shortest-path rules and answers ARP itself. Point at the
priority-10 rules in the flow dump.

## 2. The application (2 minutes)

```
mininet> sh mkdir -p results
mininet> h1 python3 -u -m app.task_manager > results/demo_manager.log 2>&1 &
mininet> w1 python3 -u -m app.worker --id w1 --manager 10.0.0.1 > results/demo_w1.log 2>&1 &
mininet> w2 python3 -u -m app.worker --id w2 --manager 10.0.0.1 > results/demo_w2.log 2>&1 &
mininet> w3 python3 -u -m app.worker --id w3 --manager 10.0.0.1 --slowdown 2 > results/demo_w3.log 2>&1 &
mininet> w4 python3 -u -m app.worker --id w4 --manager 10.0.0.1 --slowdown 4 > results/demo_w4.log 2>&1 &
mininet> h2 python3 -u -m app.client --manager 10.0.0.1 --tasks 60 --work-ms 100 --deadline 30 --wait-workers 4 --out results/demo_client.json
mininet> sh ovs-ofctl -O OpenFlow13 dump-flows s1 | grep priority=20
mininet> sh python3 scripts/ctl.py state
```

Say: workers register over TCP and the manager tells the controller (`WORKER_UP`), which installs
priority-20 application flows. Show the client summary: the slow workers (w3 at 2x, w4 at 4x) received fewer tasks
because `least_loaded` measures each worker's speed. Note that worker ids print as IP addresses
(`10.0.0.11` is w1): Mininet rewrites arguments that match a host name.

Compare with the recorded baseline instead of re-running it live:
`round_robin` 6.52 ± 0.64 s vs `least_loaded` 2.87 ± 0.02 s makespan (`results/summary.md`, `sched`).

## 3. Worker failure and redistribution (2 minutes)

```
mininet> source scripts/mn/stage7_redistribute.mn
mininet> source scripts/mn/stage7_baseline.mn
```

Each takes about 20 s. Expected output (matches the committed `results/s7a_*` and `s7c_*`):
redistribution run: 40 completed, 0 failed, 2 requeues; baseline (`--max-attempts 1`): 38 completed,
2 failed, 0 requeues, and `7C PASS`. In the first run, point out that no priority-20 flow for `10.0.0.12`
remains after the worker died.

## 4. Link failure: static vs dynamic SDN (3 minutes)

```
mininet> source scripts/mn/stage8_linkfail_dynamic.mn
```

About 35 s. While it runs, explain: a 100-task batch is running, a ping runs h1 to w1, and 4 s in the script
takes `s1-s2` down for 8 s. Expected final lines: outage about 0.1 s, `workers_failed 0`, 100/100 done,
`LINKFAIL-dynamic PASS`. Then optionally run `stage8_linkfail_static.mn` (about 35 s): the ping is dead for about
8 s, 2 workers are declared failed, tasks are redistributed, and `LINKFAIL-static PASS` means the baseline
correctly did **not** recover. Recorded values (3 runs): 0.10 ± 0.04 s vs 8.07 ± 0.03 s outage.

## 5. Congestion: show the recorded result, including the bad news (1 minute)

Do not run it live (about 70 s per run, and the result is the same one already measured). Show
`results/graphs/congestion_makespan_s.svg` and say plainly: dynamic steering moved w1 and w2 to the path via s3,
and in this testbed that made the batch slower (3.53 s vs 2.41 s static, 3 runs each, ranges not overlapping).
Explain the likely reason (all four workers' traffic ended up on one 10 Mbit/s uplink) and that it is a
hypothesis. Mention the fix ideas from README section 6.

## 6. Close (1 minute)

```
mininet> exit
```
```bash
python3 -m unittest discover -s tests               # 122 tests
~/ryu-env/bin/python -m unittest discover -s tests/sdn   # 15 tests
git status --short
```

Show the GitHub repository and `README.md`.

## After the demo

The demo scripts overwrite tracked files in `results/` (`s7a_*`, `s7c_*`, `s8lf_*`, plus `demo_*`). Check
`git status`; if you do not want to keep the changes, review them with `git diff --stat results/` and restore only
what you intend to discard, yourself.

## If something goes wrong

| Symptom | Action |
|---|---|
| `scripts/run_ryu.sh` says port 6653 is in use | a controller is already running; use it, do not start a second |
| `ctl.py` says it cannot reach the controller | Ryu is not running; restart `scripts/run_ryu.sh`, then restart `topo.py` |
| `pingall` shows drops | wait 5 s after Ryu starts, then retry; check the Ryu terminal for the switch-connected lines |
| A script prints `FAIL` | show the printed numbers, then the committed result in `results/summary.md`; do not rerun repeatedly |
| Mininet CLI hangs | `exit`, rerun `topo.py` |
