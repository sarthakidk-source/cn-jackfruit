#!/usr/bin/env python3
"""
Turn the raw files in results/exp/ into tables and graphs (standard library only).

    python3 experiments/analyze.py [--results results] [--graphs results/graphs]

Outputs
  results/summary_runs.csv   one row per run, every metric (the raw evidence)
  results/summary.md         mean +/- sample std-dev per scenario/variant (n shown)
  results/graphs/*.svg       bar = mean, whiskers = std-dev, dots = individual runs

Nothing is estimated: every number is read from a result file written by an experiment
run. Runs whose files are missing or unreadable are listed and excluded, never filled in.
"""
import argparse
import csv
import glob
import json
import os
import re
import statistics
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "scripts"))
import check_s8 as c8  # noqa: E402

NAME_RE = re.compile(r"^(?P<scn>[a-z]+)_(?P<var>[a-z_]+)_r(?P<run>\d+)_client\.json$")

# scenario -> (variants in display order, baseline variant, [(metric, label, unit, lower_is_better)])
SPEC = {
    "sched": (["round_robin", "least_loaded"], "round_robin", [
        ("makespan_s", "Batch makespan", "s", True),
        ("throughput_tps", "Throughput", "tasks/s", False),
        ("latency_mean_ms", "Mean task latency", "ms", True)]),
    "wfail": (["baseline", "redistribute"], "baseline", [
        ("completed", "Tasks completed (of 40)", "tasks", False),
        ("failed", "Tasks failed", "tasks", True),
        ("requeues", "Tasks redistributed", "count", False)]),
    "linkfail": (["static", "dynamic"], "static", [
        ("outage_s", "Connectivity outage (ping h1-w1)", "s", True),
        ("completed", "Tasks completed (of 100)", "tasks", False),
        ("workers_failed", "Workers declared failed", "count", True),
        ("makespan_s", "Batch makespan", "s", True)]),
    "congestion": (["static", "dynamic"], "static", [
        ("makespan_s", "Batch makespan", "s", True),
        ("throughput_tps", "Throughput", "tasks/s", False),
        ("latency_mean_ms", "Mean task latency", "ms", True)]),
}


def _load(path):
    with open(path) as f:
        return json.load(f)


def _text(path):
    with open(path) as f:
        return f.read()


def run_metrics(scn, results, pre):
    """pre = path prefix up to and including '_r<k>_'. Returns dict of metrics."""
    cl = _load(pre + "client.json")
    m = {"submitted": cl["submitted"], "completed": cl["completed"], "failed": cl["failed"],
         "requeues": cl["requeues"], "makespan_s": cl["makespan_s"],
         "throughput_tps": cl["throughput_tps"],
         "latency_mean_ms": (cl.get("latency_ms") or {}).get("mean"),
         "latency_max_ms": (cl.get("latency_ms") or {}).get("max"),
         "workers_failed": cl["manager_metrics"]["counters"].get("workers_failed", 0)}
    if scn == "sched":
        tpw = sorted(cl["tasks_per_worker"].values(), reverse=True)
        m["max_tasks_one_worker"] = tpw[0] if tpw else None
    if scn == "linkfail":
        t = [float(x) for x in _text(pre + "times.txt").split()]
        rep = c8.parse_ping(_text(pre + "ping.log"))
        gap, lost = c8.outage(rep, t[0], t[1])
        # a probe stream that never recovered is an open-ended outage: report the
        # whole time the link was down as a lower bound and flag it
        m["outage_s"] = round(gap, 3) if gap is not None else round(t[1] - t[0], 3)
        m["outage_open"] = int(gap is None)
        m["probes_lost"] = lost
        react = c8.reaction(_load(pre + "state.json"), t[0])
        m["ctl_flow_mods"] = react["flow_mods"] if react else 0
        m["ctl_reconcile_ms"] = react["ms"] if react else None
    if scn == "congestion":
        st = _load(pre + "state.json")
        t0 = float(_text(pre + "times.txt").split()[0])
        m["steer_events"] = len(c8.events_since(st, "steered", t0))
        m["util_s1_s2_end"] = st.get("util", {}).get("s1->s2")
    return m


def collect(results):
    rows, skipped = [], []
    base = os.path.join(results, "exp")
    for path in sorted(glob.glob(os.path.join(base, "*_client.json"))):
        mt = NAME_RE.match(os.path.basename(path))
        if not mt:
            continue
        scn, var, run = mt.group("scn"), mt.group("var"), int(mt.group("run"))
        if scn not in SPEC:
            continue
        pre = path[:-len("client.json")]
        try:
            m = run_metrics(scn, results, pre)
        except (OSError, ValueError, KeyError, TypeError) as e:
            skipped.append("%s/%s/r%d: %s: %s" % (scn, var, run, type(e).__name__, e))
            continue
        m.update(scenario=scn, variant=var, run=run)
        rows.append(m)
    return rows, skipped


def mean_sd(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None, None, 0
    return (statistics.mean(vals), statistics.stdev(vals) if len(vals) > 1 else 0.0, len(vals))


def fmt(v, nd=2):
    return "n/a" if v is None else ("%.*f" % (nd, v))


def write_csv(rows, path):
    keys = ["scenario", "variant", "run"]
    extra = sorted({k for r in rows for k in r} - set(keys))
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys + extra)
        w.writeheader()
        for r in sorted(rows, key=lambda r: (r["scenario"], r["variant"], r["run"])):
            w.writerow(r)


def improvement(base, new, lower_better):
    if base in (None, 0) or new is None:
        return None
    ch = (new - base) / base * 100.0
    return -ch if lower_better else ch


def write_md(rows, skipped, path):
    L = ["# Experiment summary", "",
         "Generated by `experiments/analyze.py` from the raw files in `results/exp/`. "
         "Values are mean ± sample standard deviation over n runs. "
         "Every number is read from a result file written by an experiment run; none is estimated.", ""]
    for scn, (variants, basev, metrics) in SPEC.items():
        sub = [r for r in rows if r["scenario"] == scn]
        if not sub:
            L += ["## %s" % scn, "", "_no runs found_", ""]
            continue
        L += ["## %s" % scn, "", "| metric | " + " | ".join(
            "%s (n=%d)" % (v, len([r for r in sub if r["variant"] == v])) for v in variants)
            + " | change vs `%s` |" % basev, "|---|" + "---|" * (len(variants) + 1)]
        for key, label, unit, lower in metrics:
            cells, stats = [], {}
            for v in variants:
                mu, sd, n = mean_sd([r.get(key) for r in sub if r["variant"] == v])
                stats[v] = mu
                cells.append("n/a" if mu is None else "%s ± %s %s" % (fmt(mu), fmt(sd), unit))
            others = [v for v in variants if v != basev]
            imp = improvement(stats.get(basev), stats.get(others[0]), lower) if others else None
            note = "n/a" if imp is None else "%+.1f %% (%s)" % (imp, "better" if imp > 0 else "worse")
            L.append("| %s | %s | %s |" % (label, " | ".join(cells), note))
        if scn == "linkfail":
            opn = [r for r in sub if r.get("outage_open")]
            if opn:
                L += ["", "_%d run(s) never recovered while the link was down; their outage is "
                      "recorded as the full down time (a lower bound)._" % len(opn)]
        L.append("")
    if skipped:
        L += ["## Runs excluded (unreadable or incomplete)", ""] + ["- " + s for s in skipped] + [""]
    with open(path, "w") as f:
        f.write("\n".join(L))


# ---- tiny SVG bar chart (no plotting library needed) ----------------------------------------
COLORS = ["#6b7280", "#2563eb", "#d97706", "#059669"]


def nice_top(v):
    """Round v up to 1, 2, 2.5, 5 or 10 times a power of ten so the axis ticks are tidy."""
    import math
    if v <= 0:
        return 1.0
    e = 10 ** math.floor(math.log10(v))
    for m in (1, 2, 2.5, 5, 10):
        if v <= m * e:
            return m * e
    return 10 * e


def svg_chart(title, ylabel, variants, data):
    """data: {variant: [values]}. Bars = mean, whiskers = std-dev, dots = runs."""
    W, H, ml, mr, mt, mb = 520, 340, 70, 20, 50, 60
    stats = {v: mean_sd(data.get(v, [])) for v in variants}
    top = max([(s[0] or 0) + (s[1] or 0) for s in stats.values()] +
              [x for v in variants for x in data.get(v, [])] + [1e-9]) * 1.08
    top = nice_top(top)
    pw, ph = W - ml - mr, H - mt - mb

    def y(v):
        return mt + ph - (v / top) * ph
    o = ['<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" '
         'font-family="sans-serif" font-size="12">' % (W, H, W, H),
         '<rect width="100%" height="100%" fill="#ffffff"/>',
         '<text x="%d" y="24" font-size="15" font-weight="bold" fill="#111">%s</text>' % (ml, title)]
    for i in range(5):
        v = top * i / 4
        o.append('<line x1="%d" x2="%d" y1="%.1f" y2="%.1f" stroke="#e5e7eb"/>' % (ml, W - mr, y(v), y(v)))
        o.append('<text x="%d" y="%.1f" text-anchor="end" fill="#444">%s</text>' % (ml - 6, y(v) + 4, ("%.3g" % v)))
    o.append('<text x="14" y="%d" transform="rotate(-90 14 %d)" text-anchor="middle" fill="#444">%s</text>'
             % (mt + ph // 2, mt + ph // 2, ylabel))
    slot = pw / max(len(variants), 1)
    for i, v in enumerate(variants):
        mu, sd, n = stats[v]
        cx = ml + slot * (i + 0.5)
        col = COLORS[i % len(COLORS)]
        if mu is not None:
            o.append('<rect x="%.1f" y="%.1f" width="%.1f" height="%.1f" fill="%s" opacity="0.85"/>'
                     % (cx - slot * 0.28, y(mu), slot * 0.56, max(ph - (y(mu) - mt), 0), col))
            o.append('<line x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f" stroke="#111" stroke-width="1.5"/>'
                     % (cx, cx, y(max(mu - sd, 0)), y(mu + sd)))
            o.append('<text x="%.1f" y="%.1f" text-anchor="middle" fill="#111">%s</text>'
                     % (cx, y(mu + sd) - 6, "%.3g" % mu))
            for j, val in enumerate(data[v]):
                dx = (j - (len(data[v]) - 1) / 2) * 9
                o.append('<circle cx="%.1f" cy="%.1f" r="3.5" fill="#fff" stroke="#111"/>' % (cx + dx, y(val)))
        o.append('<text x="%.1f" y="%d" text-anchor="middle" fill="#111">%s</text>' % (cx, H - mb + 20, v))
        o.append('<text x="%.1f" y="%d" text-anchor="middle" fill="#666">n=%d</text>' % (cx, H - mb + 36, n))
    o.append("</svg>")
    return "\n".join(o)


def write_graphs(rows, outdir):
    os.makedirs(outdir, exist_ok=True)
    made = []
    for scn, (variants, _b, metrics) in SPEC.items():
        sub = [r for r in rows if r["scenario"] == scn]
        for key, label, unit, lower in metrics:
            data = {v: [r[key] for r in sub if r["variant"] == v and r.get(key) is not None]
                    for v in variants}
            if not any(data.values()):
                continue
            path = os.path.join(outdir, "%s_%s.svg" % (scn, key))
            with open(path, "w") as f:
                f.write(svg_chart("%s: %s" % (scn, label), "%s (%s)" % (label, unit), variants, data))
            made.append(path)
    return made


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="results")
    ap.add_argument("--graphs", default=None)
    a = ap.parse_args()
    c8.RESULTS = a.results
    rows, skipped = collect(a.results)
    if not rows:
        print("no readable runs found under %s/exp/" % a.results)
        return 1
    write_csv(rows, os.path.join(a.results, "summary_runs.csv"))
    write_md(rows, skipped, os.path.join(a.results, "summary.md"))
    made = write_graphs(rows, a.graphs or os.path.join(a.results, "graphs"))
    print("%d runs, %d excluded, %d graphs" % (len(rows), len(skipped), len(made)))
    for s in skipped:
        print("  excluded:", s)
    print("wrote %s, %s" % (os.path.join(a.results, "summary_runs.csv"),
                            os.path.join(a.results, "summary.md")))
    return 0


if __name__ == "__main__":
    sys.exit(main())
