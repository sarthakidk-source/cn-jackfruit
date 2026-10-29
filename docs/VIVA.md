# Viva preparation

Answers use only what the code and the recorded results show. Numbers are from `results/summary.md`
(3 runs per condition) unless stated. If you do not know something, say so; every limitation below is already
written down in `README.md` section 6.

## A. The project in 30 seconds

A task manager, workers and a client talk over TCP. The whole thing runs on a Mininet network of three Open vSwitch
switches controlled by a Ryu OpenFlow 1.3 controller. The manager tells the controller when workers join or die;
the controller reroutes around failed links and can steer traffic off congested links. Every mechanism is compared
with a baseline in repeated experiments.

## B. Architecture and sockets

**Why NDJSON over TCP?** Human-readable, easy to debug, and one line is one message, so framing is simple.
TCP is a byte stream, so `MessageChannel` buffers input and returns a message only when a full line has arrived;
it handles partial reads, several messages in one read, a 4 MiB line limit and bad JSON.

**What happens if a send times out halfway?** The message may be half written, so the channel is closed on any
`OSError`; the worker reconnects and the manager requeues its tasks.

**Why a writer thread and queue per worker?** A stalled worker connection must not block dispatch to the others.
The single lock is never held during network I/O.

**Why does the manager talk to Ryu over a Unix socket?** Mininet hosts share the VM's filesystem but have their own
network namespace, so `h1` cannot reach Ryu on `127.0.0.1`. A Unix socket at `/tmp/jackfruit-ctl.sock` works from any host.

**Is heartbeat needed when TCP already detects closed connections?** Yes. A killed process closes its socket, but a
hung process or a dead link does not; only silence for 3 s (heartbeat every 1 s) reveals it. Stage 7B (hang) and the
link-failure runs show this.

## C. Scheduling and failure recovery

**How do the policies differ?** `round_robin` hands out tasks in turn and ignores load. `least_loaded` caps 2 tasks
in flight per worker and picks the lowest `(inflight + 1) * est_ms`, with `est_ms` an EWMA of the time the manager
measured per task for that worker.

**What did you measure?** With workers slowed 1x, 1x, 2x, 4x and 60 tasks: makespan 6.52 ± 0.64 s vs 2.87 ± 0.02 s
(56 % shorter), throughput 9.26 vs 20.89 tasks/s. Round-robin gives every worker 15 tasks, so the 4x worker sets the pace.

**Caveat to volunteer:** worker speed differences are injected with `--slowdown`; work is `sleep(work_ms * slowdown)`,
not real computation.

**What happens when a worker dies?** The manager notices (closed connection or 3 s of missing heartbeats), re-queues
its in-flight tasks to others, up to `max_attempts` (default 3). Results: with redistribution 40/40 complete (2 requeues);
with `--max-attempts 1`, 38 complete and 2 fail, in all three runs.

**Why can a task be non-idempotent and what do you do?** Re-running a task with side effects could do it twice.
Tasks marked `--non-idempotent` are never retried and fail with "not retry-safe" (Stage 7D: 38 done, 2 failed).

**What if a late or duplicate result arrives?** It is counted; a late valid result is accepted if the task is still
open; a duplicate is ignored. Duplicate submissions are idempotent.

**Single point of failure?** Yes: the manager. There is no replication or failover; this is outside the project scope.
Also no authentication or encryption on the sockets.

## D. SDN

**Why proactive rules instead of a learning switch?** The topology contains a loop (the triangle), so flooding
would storm. The controller computes shortest paths and installs `ipv4_dst` rules in advance, and answers ARP itself.

**Rule priorities?** 10 base shortest-path; 20 per-worker application flows (steerable); 30 quarantine drop.
Priority 0 table-miss sends ARP to the controller.

**How does link-failure recovery work?** The switches send `PORT_STATUS`; the controller marks the link down,
recomputes paths and pushes only the changed rules (reconcile by diff, new rules added before old ones are removed).
Measured: outage 0.10 ± 0.04 s vs 8.07 ± 0.03 s in static mode; 12 flow-mods, 1.1 to 5.6 ms controller time per
failure. Because the reroute is faster than the 3 s heartbeat timeout, no worker was declared failed (0 vs 2).

**What is the difference between static and dynamic mode?** Static installs shortest paths and ignores link state,
load and application events; dynamic reacts to all three. Static is the baseline.

**How does congestion steering work?** The controller polls port statistics every second, keeps an EWMA of link
utilisation, and when an application flow's path is above 70 % it moves that flow to a path at least 30 points less
loaded, one flow per poll, 5 s cooldown against flapping.

**What does the application tell the network?** `WORKER_UP` and `WORKER_DOWN`: the controller installs or removes the
priority-20 flows for that worker and can quarantine a failed one (`--quarantine`).

**Why did all 100 tasks complete even in static mode when the link failed?** The application layer recovered:
the two unreachable workers were declared failed and their tasks went to w3 and w4 (4 requeues per run). The network
failure still cost 8 s of connectivity and made the batch 4.3 s slower (12.03 vs 7.74 s).

## E. The negative result (prepare this one carefully)

**Is dynamic mode better?** For link failure, clearly yes. For congestion, not in our test: makespan 3.53 ± 0.02 s vs
2.41 ± 0.16 s static, throughput 11.33 vs 16.66 tasks/s, latency 1961 vs 1277 ms. All three dynamic runs were slower than
all three static runs. We report it as measured.

**Why?** What the data shows: w1 and w2 ended on the two-hop path via s3, and `s1->s3` utilisation was 0.75 to 0.85
(static: 0.62 to 0.71). Likely cause (a hypothesis, not tested): the steering policy compares only link utilisation,
so it moved two workers' traffic onto the same uplink that already served w3 and w4, and the application-level
`least_loaded` scheduler was already adapting to the congestion in static mode (e.g. 10/5/10/15 tasks per worker).

**Why do the Stage 9 dynamic runs show zero steering events?** The controller log shows a `path_changed` for w1 and w2
at the moment the mode was switched to dynamic, not a `steered` event during the batch. Cause found in code:
`TaskManager.stop()` does not send `WORKER_DOWN`, so the controller keeps old worker state across runs and re-selected
paths for them at the mode switch. The earlier Stage 8 run did record 2 real `steered` events (load 0.76 on `s1-s2`)
and gave the same 3.47 s batch time, so the outcome is the same either way.

**What would you change?** Notify `WORKER_DOWN` on shutdown; make steering account for the candidate path's load and for
other workers sharing it; longer congestion runs with more repetitions; tune thresholds.

**Do not say:** that dynamic mode "wins" on congestion, or that the explanation above is proven.

## F. Experiments and statistics

**How many runs?** 3 per condition, 24 in total (4 scenarios x 2 variants x 3), variants alternated within each repetition.
Standard deviations from n = 3 are rough; no significance tests were run.

**Why is the standard deviation 0 in the worker-failure scenario?** The failure is injected deterministically
(kill w2 at a fixed time), so the outcome is the same each time; it demonstrates the mechanism, not variability.

**What is the outage resolution?** Ping runs every 50 ms, so outage values are accurate to about that.

**Limits of the testbed?** One VirtualBox VM; Mininet shares CPU between all nodes; three switches; 10 Mbit/s links;
one UDP `iperf` flow as background load; simulated work. Relative comparisons are meaningful, absolute seconds are not.

**How are results reproduced?** `experiments/reproduce.sh gen`, then `source experiments/mn/exp_*.mn` in Mininet,
then `experiments/reproduce.sh analyze` (README section 9).

## G. Testing

**What tests exist?** 122 standard-library tests (protocol framing, task model, loopback TCP integration of the manager,
SDN policy, checker, analyzer) and 15 wire tests that run the real Ryu app against fake OpenFlow 1.3 switches.
The author reports the 15 wire tests passing on the VM. Fake switches are not Open vSwitch: real switch behaviour was
validated by the Mininet experiments.

## H. Quick facts

* Ports: manager 9100 (workers), 9200 (clients); controller OpenFlow 6653; Unix socket `/tmp/jackfruit-ctl.sock`.
* Hosts: h1 `10.0.0.1` manager, h2 `10.0.0.2` client, w1 to w4 `10.0.0.11` to `10.0.0.14`.
* Defaults: heartbeat 1 s, timeout 3 s, task deadline 10 s, 3 attempts, in-flight cap 2.
* Steering: poll 1 s, trigger 70 %, margin 30 points, cooldown 5 s, EWMA alpha 0.5.
