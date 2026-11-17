"""
Statistics for the multi-client / multi-server system.

Every number is computed from what is stored in the shared SQLite file (task rows,
server heartbeats, client rows). Nothing is hard-coded.

Show the statistics:
    python3 -m app.metrics --db results/demo2.db
"""
import argparse
import time

from app import shared_queue as sq


def _worker_by_server(conn):
    table = {}
    for r in conn.execute("SELECT worker, server_id, COUNT(*) AS n FROM tasks WHERE status = ?"
                          " GROUP BY worker, server_id ORDER BY worker, server_id", (sq.COMPLETED,)):
        table.setdefault(r["worker"], {})[r["server_id"]] = r["n"]
    return table


def get_stats(conn):
    now = time.time()
    one = lambda sql, args=(): conn.execute(sql, args).fetchone()[0]
    fresh = now - sq.SERVER_TIMEOUT_S      # heartbeat newer than this = server is up

    status = dict(conn.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status").fetchall())
    submitted = sum(status.values())
    completed = status.get(sq.COMPLETED, 0)

    avg_latency = one("SELECT AVG(completed_at - submitted_at) FROM tasks WHERE status = ?",
                      (sq.COMPLETED,))
    # throughput = completed tasks / time from first submission to last completion
    first_submit = one("SELECT MIN(submitted_at) FROM tasks")
    last_done = one("SELECT MAX(completed_at) FROM tasks WHERE status = ?", (sq.COMPLETED,))
    elapsed = (last_done - first_submit) if completed and last_done > first_submit else None

    return {
        "connected_clients": one("SELECT COUNT(*) FROM clients c JOIN servers s"
                                 " ON c.server_id = s.server_id"
                                 " WHERE c.connected = 1 AND s.last_seen > ?", (fresh,)),
        "active_servers": one("SELECT COUNT(*) FROM servers WHERE last_seen > ?", (fresh,)),
        "submitted": submitted,
        "completed": completed,
        "failed": status.get(sq.FAILED, 0),
        "pending": status.get(sq.PENDING, 0) + status.get(sq.CLAIMED, 0),
        "running": status.get(sq.ASSIGNED, 0),
        "retried": one("SELECT COUNT(*) FROM tasks WHERE attempts > 1"),
        "avg_latency_ms": None if avg_latency is None else round(avg_latency * 1000, 1),
        "throughput_tps": None if not elapsed else round(completed / elapsed, 2),
        "per_worker": dict(conn.execute(
            "SELECT worker, COUNT(*) FROM tasks WHERE status = ? GROUP BY worker ORDER BY worker",
            (sq.COMPLETED,)).fetchall()),
        # for each worker: how many of its completed tasks came in through each server
        "worker_by_server": _worker_by_server(conn),
        "per_server": {r["server_id"]: {
            "requests": r["requests"],
            "tasks_submitted": one("SELECT COUNT(*) FROM tasks WHERE server_id = ?",
                                   (r["server_id"],)),
            "up": r["last_seen"] > fresh}
            for r in conn.execute("SELECT * FROM servers ORDER BY server_id").fetchall()},
    }


def format_stats(s):
    lat = "-" if s["avg_latency_ms"] is None else "%.1f ms" % s["avg_latency_ms"]
    tps = "-" if s["throughput_tps"] is None else "%.2f tasks/s" % s["throughput_tps"]
    lines = [
        "=== Demo 2 statistics ===",
        "Connected clients      : %d" % s["connected_clients"],
        "Active servers         : %d" % s["active_servers"],
        "Submitted tasks        : %d" % s["submitted"],
        "Completed tasks        : %d" % s["completed"],
        "Failed tasks           : %d" % s["failed"],
        "Pending tasks          : %d   (running now: %d)" % (s["pending"], s["running"]),
        "Retried tasks          : %d   (re-run after a worker failure)" % s["retried"],
        "Average task latency   : %s" % lat,
        "Throughput             : %s" % tps,
        "Completed per worker   :",
    ]
    for w, n in s["per_worker"].items():
        by_server = ", ".join("%s: %d" % kv for kv in s["worker_by_server"].get(w, {}).items())
        lines.append("    %-12s %-4d (tasks submitted via %s)" % (w or "-", n, by_server))
    if not s["per_worker"]:
        lines.append("    (none)")
    lines.append("Requests per server    :")
    for sid, v in s["per_server"].items():
        lines.append("    %-12s %d requests, %d tasks submitted (%s)"
                     % (sid, v["requests"], v["tasks_submitted"], "up" if v["up"] else "down"))
    if not s["per_server"]:
        lines.append("    (none)")
    return "\n".join(lines)


def main():
    p = argparse.ArgumentParser(description="Show Demo 2 statistics")
    p.add_argument("--db", required=True, help="shared SQLite file")
    args = p.parse_args()
    conn = sq.connect(args.db)
    print(format_stats(get_stats(conn)))


if __name__ == "__main__":
    main()
