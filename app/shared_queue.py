"""
Shared task queue stored in one SQLite file (used by the servers, the worker
coordinator and the statistics script, each in its own OS process).

Tables
  tasks    one row per task: who submitted it, through which server, its state,
           which worker ran it, the result and the timestamps
  servers  one row per server process: heartbeat time and request counter
  clients  one row per client id: which server it uses and whether it is connected

Task status in the database
  PENDING    inserted by a server, nobody has taken it yet
  CLAIMED    taken by the coordinator, waiting for a worker (also after a requeue)
  ASSIGNED   a worker is running it
  COMPLETED  finished, result stored
  FAILED     gave up

Concurrency: every process opens its own connection. WAL mode lets readers and one
writer work at the same time. Every write is wrapped in BEGIN IMMEDIATE ... COMMIT,
so two processes can never claim the same task: the second one waits for the lock
and then no longer sees the task as PENDING.
"""
import contextlib
import json
import sqlite3
import time

PENDING, CLAIMED, ASSIGNED, COMPLETED, FAILED = \
    "PENDING", "CLAIMED", "ASSIGNED", "COMPLETED", "FAILED"

SERVER_TIMEOUT_S = 3.0    # a server whose heartbeat is older than this counts as down

SCHEMA = """
CREATE TABLE IF NOT EXISTS tasks (
    task_id      TEXT PRIMARY KEY,
    client_id    TEXT NOT NULL,
    server_id    TEXT NOT NULL,
    payload      TEXT NOT NULL,      -- task spec as JSON
    status       TEXT NOT NULL,
    worker       TEXT,
    result       TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0,
    submitted_at REAL NOT NULL,
    completed_at REAL
);
CREATE INDEX IF NOT EXISTS tasks_status ON tasks(status);
CREATE TABLE IF NOT EXISTS servers (
    server_id  TEXT PRIMARY KEY,
    port       INTEGER,
    started_at REAL,
    last_seen  REAL,
    requests   INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS clients (
    client_id TEXT PRIMARY KEY,
    server_id TEXT,
    connected INTEGER NOT NULL DEFAULT 0,
    last_seen REAL
);
"""


def connect(path):
    """Open a connection for the calling thread (connections are not shared)."""
    conn = sqlite3.connect(path, timeout=10, isolation_level=None)   # we use explicit BEGIN
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript(SCHEMA)
    return conn


@contextlib.contextmanager
def write_transaction(conn):
    """BEGIN IMMEDIATE takes the write lock right away, so reads inside the
    transaction cannot be invalidated by another process."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    conn.execute("COMMIT")


# ---------------------------------------------------------------- tasks
def add_tasks(conn, client_id, server_id, items):
    """items: list of (task_id, spec dict). Returns the ids that were really
    inserted; an id that already exists is a duplicate submission and is skipped."""
    now = time.time()
    inserted = []
    with write_transaction(conn):
        for task_id, spec in items:
            cur = conn.execute(
                "INSERT OR IGNORE INTO tasks (task_id, client_id, server_id, payload, status,"
                " submitted_at) VALUES (?, ?, ?, ?, ?, ?)",
                (task_id, client_id, server_id, json.dumps(spec), PENDING, now))
            if cur.rowcount == 1:
                inserted.append(task_id)
    return inserted


def claim_pending(conn, limit=200):
    """Atomically move up to `limit` PENDING tasks to CLAIMED and return them
    (oldest first) as (task_id, spec dict). A task is returned to one caller only."""
    with write_transaction(conn):
        rows = conn.execute("SELECT task_id, payload FROM tasks WHERE status = ?"
                            " ORDER BY rowid LIMIT ?", (PENDING, limit)).fetchall()
        for r in rows:
            conn.execute("UPDATE tasks SET status = ? WHERE task_id = ?", (CLAIMED, r["task_id"]))
    return [(r["task_id"], json.loads(r["payload"])) for r in rows]


def update_tasks(conn, rows):
    """rows: list of (status, worker, result, attempts, completed_at, task_id)."""
    with write_transaction(conn):
        conn.executemany("UPDATE tasks SET status = ?, worker = ?, result = ?, attempts = ?,"
                         " completed_at = ? WHERE task_id = ?", rows)


def get_tasks(conn, client_id=None, task_ids=None):
    """Tasks as dicts shaped like the task manager's STATUS reply, so the existing
    client code can read them. Optionally only one client's tasks / only some ids."""
    if client_id is None:
        rows = conn.execute("SELECT * FROM tasks ORDER BY rowid").fetchall()
    else:
        rows = conn.execute("SELECT * FROM tasks WHERE client_id = ? ORDER BY rowid",
                            (client_id,)).fetchall()
    wanted = None if task_ids is None else set(task_ids)
    tasks = []
    for r in rows:
        if wanted is not None and r["task_id"] not in wanted:
            continue
        done = r["completed_at"] is not None and r["status"] == COMPLETED
        tasks.append({
            "task_id": r["task_id"], "state": r["status"], "worker": r["worker"],
            "client_id": r["client_id"], "server_id": r["server_id"],
            "attempts": r["attempts"], "requeues": max(r["attempts"] - 1, 0),
            "submitted_at": r["submitted_at"], "completed_at": r["completed_at"],
            "latency_ms": round((r["completed_at"] - r["submitted_at"]) * 1000, 1) if done else None,
            "result": r["result"],
        })
    return tasks


# ---------------------------------------------------------------- servers and clients
def register_server(conn, server_id, port):
    now = time.time()
    with write_transaction(conn):
        conn.execute("INSERT OR REPLACE INTO servers (server_id, port, started_at, last_seen,"
                     " requests) VALUES (?, ?, ?, ?, 0)", (server_id, port, now, now))


def server_heartbeat(conn, server_id, alive=True):
    with write_transaction(conn):
        conn.execute("UPDATE servers SET last_seen = ? WHERE server_id = ?",
                     (time.time() if alive else 0, server_id))


def count_request(conn, server_id):
    with write_transaction(conn):
        conn.execute("UPDATE servers SET requests = requests + 1, last_seen = ?"
                     " WHERE server_id = ?", (time.time(), server_id))


def client_seen(conn, client_id, server_id, connected):
    with write_transaction(conn):
        conn.execute("INSERT INTO clients (client_id, server_id, connected, last_seen)"
                     " VALUES (?, ?, ?, ?) ON CONFLICT(client_id) DO UPDATE SET"
                     " server_id = excluded.server_id, connected = excluded.connected,"
                     " last_seen = excluded.last_seen",
                     (client_id, server_id, 1 if connected else 0, time.time()))
