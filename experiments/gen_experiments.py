#!/usr/bin/env python3
"""
Generate the Mininet CLI scripts for the Stage 9 experiments (standard library only).

    python3 experiments/gen_experiments.py [--reps 3] [--out experiments/mn]

Writes one script per scenario. Each script is self-contained (no nested `source`,
no comment lines: both upset the Mininet CLI) and writes its raw measurements to
results/exp/<scenario>_<variant>_r<run>_*. Variants alternate inside every repetition
so slow drift of the VM affects both variants equally.

Scenarios (baseline vs the project's mechanism):
  sched      round_robin  vs  least_loaded    workers slowed 1x 1x 2x 4x, 60 tasks
  wfail      max-attempts 1 (no redistribution)  vs  redistribution, worker killed mid-batch
  linkfail   static SDN   vs  dynamic SDN     link s1-s2 cut mid-batch for 8 s
  congestion static SDN   vs  dynamic SDN     9 Mbps UDP hogs s1-s2 while 40 tasks run
Run order inside the Mininet CLI:  source experiments/mn/exp_<scenario>.mn
"""
import argparse
import os

D = "results/exp/"
WORKERS = "w1 w2 w3 w4".split()


def reset(pre):
    return ['sh pkill -f "[a]pp.task_manager"', 'sh pkill -f "[a]pp.worker"',
            'sh pkill -f "[i]perf"', 'sh pkill -f "[p]ing -D"', 'sh sleep 2']


def start(pre, manager_args="", slowdowns=(1, 1, 1, 1)):
    L = ["h1 python3 -u -m app.task_manager %s> %smanager.log 2>&1 &" % (
        (manager_args + " ") if manager_args else "", pre)]
    for w, s in zip(WORKERS, slowdowns):
        L.append("%s python3 -u -m app.worker --id %s --manager 10.0.0.1%s > %s%s.log 2>&1 &" % (
            w, w, (" --slowdown %s" % s) if s != 1 else "", pre, w))
    return L


def sched(rep):
    L = []
    for pol in ("round_robin", "least_loaded"):
        pre = D + "sched_%s_r%d_" % (pol, rep)
        L += ['sh echo "[sched %s run %d]"' % (pol, rep)] + reset(pre)
        L += ["sh python3 scripts/ctl.py mode dynamic"]
        L += start(pre, "--policy " + pol, (1, 1, 2, 4))
        L += ["sh sleep 3",
              "h2 python3 -u -m app.client --manager 10.0.0.1 --tasks 60 --work-ms 100 "
              "--deadline 30 --wait-workers 4 --timeout 120 --out %sclient.json > %sclient.log 2>&1"
              % (pre, pre)]
    return L


def wfail(rep):
    L = []
    for var, margs in (("baseline", "--max-attempts 1"), ("redistribute", "")):
        pre = D + "wfail_%s_r%d_" % (var, rep)
        L += ['sh echo "[wfail %s run %d]"' % (var, rep)] + reset(pre)
        L += ["sh python3 scripts/ctl.py mode dynamic"]
        L += start(pre, margs)
        L += ["sh sleep 3",
              "h2 python3 -u -m app.client --manager 10.0.0.1 --tasks 40 --work-ms 500 "
              "--wait-workers 4 --timeout 90 --out %sclient.json > %sclient.log 2>&1 &" % (pre, pre),
              "sh sleep 3",
              'sh pkill -f "[a]pp.worker --id 10.0.0.12"',
              'sh while pgrep -f "[a]pp.client" > /dev/null; do sleep 1; done']
    return L


def linkfail(rep):
    L = []
    for mode in ("static", "dynamic"):
        pre = D + "linkfail_%s_r%d_" % (mode, rep)
        L += ['sh echo "[linkfail %s run %d]"' % (mode, rep), "link s1 s2 up"] + reset(pre)
        L += ["sh python3 scripts/ctl.py mode " + mode]
        L += start(pre)
        L += ["sh sleep 3",
              "h1 ping -D -i 0.05 -w 22 10.0.0.11 > %sping.log 2>&1 &" % pre,
              "h2 python3 -u -m app.client --manager 10.0.0.1 --tasks 100 --work-ms 300 "
              "--wait-workers 4 --out %sclient.json > %sclient.log 2>&1 &" % (pre, pre),
              "sh sleep 4",
              "sh date +%%s.%%N > %stimes.txt" % pre,
              "link s1 s2 down",
              "sh sleep 8",
              "sh date +%%s.%%N >> %stimes.txt" % pre,
              "link s1 s2 up",
              "sh sleep 13",
              "sh python3 scripts/ctl.py state-json > %sstate.json" % pre]
    return L


def congestion(rep):
    L = []
    for mode in ("static", "dynamic"):
        pre = D + "congestion_%s_r%d_" % (mode, rep)
        L += ['sh echo "[congestion %s run %d]"' % (mode, rep), "link s1 s2 up"] + reset(pre)
        L += ["sh python3 scripts/ctl.py mode " + mode,
              "sh date +%%s.%%N > %stimes.txt" % pre]
        L += start(pre)
        L += ["sh sleep 3",
              "w2 iperf -u -s > %siperf_server.log 2>&1 &" % pre,
              "sh sleep 1",
              "h2 iperf -u -c 10.0.0.12 -b 9M -t 120 -i 1 > %siperf_client.log 2>&1 &" % pre,
              "sh sleep 9",
              "h2 python3 -u -m app.client --manager 10.0.0.1 --tasks 40 --work-ms 50 "
              "--payload-kb 100 --wait-workers 4 --timeout 90 --out %sclient.json > %sclient.log 2>&1"
              % (pre, pre),
              "sh python3 scripts/ctl.py state-json > %sstate.json" % pre,
              'sh pkill -f "[i]perf"']
    return L


SCENARIOS = {"sched": sched, "wfail": wfail, "linkfail": linkfail, "congestion": congestion}


def build(name, reps):
    lines = ["sh mkdir -p results/exp"]
    for r in range(1, reps + 1):
        lines += SCENARIOS[name](r)
    lines += reset("") + ['sh echo "=== %s finished"' % name]
    return lines


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reps", type=int, default=3)
    ap.add_argument("--out", default="experiments/mn")
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    for name in SCENARIOS:
        path = os.path.join(a.out, "exp_%s.mn" % name)
        with open(path, "w") as f:
            f.write("\n".join(build(name, a.reps)) + "\n")
        print("wrote %s (%d lines)" % (path, len(build(name, a.reps))))


if __name__ == "__main__":
    main()
