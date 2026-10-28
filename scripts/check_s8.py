#!/usr/bin/env python3
"""
Stage 8 checker: reads the files a stage8_*.mn script produced under results/ and
prints the measurements plus a PASS/FAIL verdict. Standard library only.

    python3 scripts/check_s8.py linkfail   static|dynamic
    python3 scripts/check_s8.py congestion static|dynamic

All numbers come from the result files; nothing is estimated. A verdict only
says whether the system behaved as DESIGNED for that mode (the baseline is
"PASS" when it fails to recover, because that is what a static baseline does).
"""
import json
import os
import re
import sys

PING_RE = re.compile(r"^\[(\d+\.\d+)\] \d+ bytes from [^:]+: icmp_seq=(\d+) ")
RESULTS = os.environ.get("JACKFRUIT_RESULTS", "results")


def parse_ping(text):
    """[(epoch, seq)] of echo replies from `ping -D` output."""
    out = []
    for line in text.splitlines():
        m = PING_RE.match(line)
        if m:
            out.append((float(m.group(1)), int(m.group(2))))
    return out


def outage(replies, t_down, t_up):
    """Connectivity gap caused by the fault.
    Returns (gap_seconds, lost_probes). The gap is the longest silence between two
    consecutive replies that overlaps [t_down, t_up]; if no reply ever arrived after
    t_down the gap is open-ended and is reported as None (still down)."""
    if not replies:
        return None, None
    best, lost = 0.0, 0
    for (t0, s0), (t1, s1) in zip(replies, replies[1:]):
        if t1 >= t_down and t0 <= t_up + 5:
            if t1 - t0 > best:
                best, lost = t1 - t0, max(0, s1 - s0 - 1)
    if replies[-1][0] < t_down:
        return None, None
    return best, lost


def reaction(state, t_down):
    """Controller-side timing from its event log: (link_down seen at, flow-mods sent,
    ms spent reconciling) for the first reroute after t_down, or None."""
    ev = state.get("events", [])
    downs = [e for e in ev if e["kind"] == "link_down" and e["t"] >= t_down - 1]
    if not downs:
        return None
    d = downs[0]
    recs = [e for e in ev if e["kind"] == "reconcile" and e["t"] >= d["t"] - 0.001
            and "link down" in e.get("reason", "")]
    if not recs:
        return {"link_down_at": d["t"], "flow_mods": 0, "ms": None, "delay_s": None}
    r = recs[0]
    return {"link_down_at": d["t"], "flow_mods": r["flow_mods"], "ms": r["ms"],
            "delay_s": r["t"] - d["t"]}


def events_since(state, kind, t0):
    """Controller events of one kind logged at or after t0. The controller keeps its
    event log across scenarios (one Ryu process serves every run), so a run must only
    look at events from its own time window."""
    return [e for e in state.get("events", []) if e["kind"] == kind and e["t"] >= t0]


def load(name):
    with open(os.path.join(RESULTS, name)) as f:
        return json.load(f)


def read(name):
    with open(os.path.join(RESULTS, name)) as f:
        return f.read()


def verdict(tag, ok, details):
    print("%s %s  %s" % (tag, "PASS" if ok else "FAIL", details))
    return 0 if ok else 1


def check_linkfail(mode):
    pre = "s8lf_%s_" % mode
    client = load(pre + "client.json")
    state = load(pre + "state.json")
    times = [float(x) for x in read(pre + "times.txt").split()]
    t_down, t_up = times[0], times[1]
    replies = parse_ping(read(pre + "ping.log"))
    gap, lost = outage(replies, t_down, t_up)
    react = reaction(state, t_down)
    m = client["manager_metrics"]
    wf = m["counters"].get("workers_failed", 0)
    down_for = t_up - t_down
    print("link s1-s2 was down for %.1f s" % down_for)
    print("ping h1->w1: %d replies, outage %s s, probes lost %s" % (
        len(replies), "n/a" if gap is None else "%.2f" % gap, lost))
    print("controller reaction: %s" % (react,))
    print("batch: completed %d/%d failed %d requeues %d workers_failed %d makespan %s s" % (
        client["completed"], client["submitted"], client["failed"], client["requeues"], wf,
        client["makespan_s"]))
    complete = client["completed"] == client["submitted"]
    if mode == "dynamic":
        ok = (gap is not None and gap < 1.0 and complete and wf == 0
              and react is not None and react["flow_mods"] > 0)
        return verdict("LINKFAIL-dynamic", ok,
                       "needs: outage<1s, no worker lost, all tasks done, controller rerouted")
    rerouted = react is not None and react["flow_mods"] > 0
    ok = (gap is None or gap >= 0.8 * down_for) and not rerouted
    return verdict("LINKFAIL-static", ok,
                   "baseline must NOT recover while the link is down (all tasks done: %s)"
                   % complete)


def check_congestion(mode):
    pre = "s8cg_%s_" % mode
    client = load(pre + "client.json")
    state = load(pre + "state.json")
    t_start = float(read(pre + "times.txt").split()[0])
    steered = events_since(state, "steered", t_start)
    older = len(events_since(state, "steered", 0)) - len(steered)
    util = state.get("util", {}).get("s1->s2")
    paths = state.get("app_paths", {})
    print("utilisation s1->s2 (EWMA at end): %s" % util)
    print("controller mode at end: %s" % state.get("mode"))
    print("steering events in this run: %d (ignored %d from earlier runs)   app paths: %s" % (
        len(steered), older, paths))
    print("batch: completed %d/%d failed %d requeues %d makespan %s s throughput %s tasks/s" % (
        client["completed"], client["submitted"], client["failed"], client["requeues"],
        client["makespan_s"], client["throughput_tps"]))
    print("tasks per worker: %s" % dict(sorted(client["tasks_per_worker"].items())))
    complete = client["completed"] == client["submitted"]
    if mode == "dynamic":
        off = [ip for ip in ("10.0.0.11", "10.0.0.12") if paths.get(ip) == ["s1", "s3", "s2"]]
        ok = state.get("mode") == mode and bool(steered) and bool(off) and complete
        return verdict("CONGESTION-dynamic", ok,
                       "needs: a steering event, w1/w2 flows moved to s1-s3-s2, all tasks done")
    ok = state.get("mode") == mode and not steered and (util or 0) > 0.7 and complete
    return verdict("CONGESTION-static", ok,
                   "baseline needs: no steering, link really congested (>0.7), all tasks done")


def main(argv):
    if len(argv) != 2 or argv[0] not in ("linkfail", "congestion") \
            or argv[1] not in ("static", "dynamic"):
        print(__doc__)
        return 2
    try:
        return (check_linkfail if argv[0] == "linkfail" else check_congestion)(argv[1])
    except (OSError, ValueError, KeyError, IndexError) as e:
        print("cannot evaluate: %s: %s\n(a result file is missing or incomplete)"
              % (type(e).__name__, e))
        return 3


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
