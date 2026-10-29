# Rubric checklist (CN Jackfruit, 40 marks)

Status key: **PASS** = evidence exists in the repository and was re-checked; **PASS (caveat)** = works, with a
limitation stated; **NOT VERIFIED** = no evidence available yet. Marks themselves are awarded by the faculty;
this file says only whether the evidence for each item exists.

How the evidence was checked: on 2026-10-29 the repository (commit `ead7f3b`) was freshly cloned, `python3 -m unittest
discover -s tests` printed `Ran 122 tests ... OK`, and `scripts/check_s8.py` was re-run on the committed result files.
Experiments themselves were run on the author's Ubuntu 22.04 VM with Mininet 2.3.0, Open vSwitch and Ryu 4.34.

## Deliverable 1 (15 marks)

| Item | Marks | Status | Evidence |
|---|---|---|---|
| Architecture design | 2 | PASS | `README.md` section 2 (Mermaid diagram, channels, topology); `topology/topo.py` |
| TCP socket implementation | 4 | PASS | `app/protocol.py`, `app/task_manager.py`, `app/worker.py`, `app/client.py`; 40/40 tasks over real TCP in `results/stage5_least_loaded.json`; loopback integration tests `tests/test_manager.py` |
| Mininet + SDN topology | 4 | PASS | 3 OVS switches, 6 hosts, 10 Mbit/s core links (`topology/topo.py`); Ryu proactive flows and ARP proxy; link-failure reroute measured on the real network (`results/exp/linkfail_*`) |
| Socket-SDN integration | 3 | PASS | manager sends `WORKER_UP`/`WORKER_DOWN` to the controller; `worker_up` events with per-worker paths appear in `results/exp/*_state.json` |
| Initial testing / demo | 2 | PASS (caveat) | 122 unit/integration tests OK (re-run on a fresh clone); 15 controller wire tests reported OK by the author on the VM (output not seen by the reviewer). `docs/DEMO.md` written; the live demo itself is NOT VERIFIED |

## Deliverable 2 (25 marks)

| Item | Marks | Status | Evidence |
|---|---|---|---|
| Application functionality | 4 | PASS | registration, assignment, load tracking, heartbeats (1 s / 3 s timeout), deadlines, redistribution, retry-safety; `results/s7a`..`s7d`, `results/exp/wfail_*` |
| Dynamic SDN behaviour | 6 | PASS (caveat) | Link-failure rerouting: outage 0.10 ± 0.04 s vs 8.07 ± 0.03 s static (`results/summary.md`). App-event flows and quarantine: Stage 6 and `state.json`. **Caveat:** congestion steering fires (2 `steered` events in `results/s8cg_dynamic_state.json`) but did not improve performance (section below) |
| Challenging scenario / robustness | 4 | PASS | worker killed (`s7a`: 40/40, 2 requeues), worker hang (`s7b`: 40/40, 2 requeues), link cut mid-batch (`linkfail`: 100/100), non-retry-safe tasks fail cleanly (`s7d`: 38 done, 2 failed with "not retry-safe") |
| Performance evaluation vs baseline | 5 | PASS (caveat) | 4 scenarios, each with a baseline, 3 runs per condition, 24 runs (`results/summary_runs.csv`, `results/summary.md`, `results/graphs/*.svg`). n = 3 is small and the congestion result is negative |
| Documentation, final demo, viva | 6 | PASS (docs); demo and viva NOT VERIFIED | README (558 lines), `docs/RUBRIC_CHECKLIST.md`, `docs/DEMO.md`, `docs/VIVA.md` exist. The demo script has not been rehearsed end to end; the demo and viva happen in person |

## Mandatory features

| Feature | Status | Where / evidence |
|---|---|---|
| Worker registration | PASS | `REGISTER` / `REGISTER_ACK`; `app/task_manager.py`; `tests/test_manager.py` |
| Task assignment | PASS | `app/scheduler.py`; `results/stage5_*.json` |
| Workload tracking | PASS | `inflight` in heartbeats, EWMA per-worker time; `--status` output |
| Timeouts | PASS | heartbeat timeout 3 s, per-task deadline; `results/s7b_*` (hang) |
| Failure detection | PASS | `workers_failed` counter; `results/exp/linkfail_static_*` (2 workers declared failed) |
| Redistribution | PASS | `wfail`: 0 failed with redistribution vs 2 failed without, in all 3 runs |
| Scheduling evaluation | PASS | `sched`: least_loaded 2.87 ± 0.02 s vs round_robin 6.52 ± 0.64 s makespan |
| Real SDN-controlled network | PASS | OVS 1.3 + Ryu; flow-mod counts and reconcile times in `*_state.json` |
| Baseline | PASS | round_robin, `--max-attempts 1`, static SDN mode |
| GitHub repo with docs | PASS | `github.com/sarthakidk-source/cn-jackfruit`, README committed |
| Reproducible experiments | PASS (caveat) | `experiments/reproduce.sh`, generated `exp_*.mn`; the author ran the scenarios from these scripts. Re-running from a fresh clone has not been done |

## Open items (not hidden)

1. **Congestion steering is not beneficial here.** Dynamic mode: 3.53 ± 0.02 s makespan vs 2.41 ± 0.16 s static
   (`results/summary.md`). README section 5.4 explains the data and labels the explanation as a hypothesis.
2. **Stage 8C (congestion dynamic) was last judged by an earlier version of the checker.** The committed
   `results/s8cg_dynamic_*` files lack `times.txt`, so the current `scripts/check_s8.py` cannot re-evaluate them
   (it prints "cannot evaluate"). The state file does contain 2 `steered` events and 40/40 tasks completed.
   Re-running `source scripts/mn/stage8_congestion_dynamic.mn` would regenerate it with the current checker.
3. **Controller wire tests (`tests/sdn`, 15 tests against the real Ryu with fake switches):** the author reports
   `Ran 15 tests ... OK` on the VM (`~/ryu-env/bin/python -m unittest discover -s tests/sdn`). The reviewer did not
   see the output. Status: PASS, author-reported.
4. **Stale controller state after manager shutdown** (no `WORKER_DOWN` on graceful stop): known, documented, not fixed.
5. **Results come from 3 runs per condition on one VM.** No significance tests.
6. **Demo script and viva notes** now exist (`docs/DEMO.md`, `docs/VIVA.md`). The demo script has not been run
   start to finish; do one dry run and record real timings.
